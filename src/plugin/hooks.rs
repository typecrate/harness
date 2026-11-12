// =============================================================================
// plugin/hooks.rs - the typed lifecycle hook bus (the INPUT side of the harness).
//
// TypeScript origin: `packages/underload/harness/src/plugin/hooks.ts` (the whole file).
// Every rule number cited below is that file's header rule, so a reviewer can diff the two
// implementations rule by rule.
//
// WHY A SEPARATE BUS INSTEAD OF `report.progress` (`hooks.ts:3-14`)
// ----------------------------------------------------------------
// `report.progress` is an OUTPUT mirror: one callback that sees every event frame and must
// not change anything, and a throwing reporter FAILS the run. A plugin hook is an INPUT: it
// can rewrite the request about to leave the harness (`turn.before`) and decide what happens
// to the run (`TurnDecision`), and it runs while the run is parked and waiting. An optional
// extension must never be able to break a conversation, so the failure rules differ, and so
// do the files.
//
// THE RULES, AND HOW EACH ONE IS ENFORCED HERE
// ----------------------------------------------------------------
//  1. SEQUENTIAL. Handlers run one at a time, awaited, in dispatch order: `priority`
//     (higher first, default 0), then registration order (`id`) within one priority.
//     Enforced by [`ordered`] plus the single `.await` per handler in the round tail.
//  2. FAIL-OPEN. A handler that fails (`Err`), outlives its `timeout_ms`, or returns a
//     decision this contract cannot parse is DROPPED; the rest of the round still runs and
//     the round still resolves. Every drop is REPORTED (rule 8). A Rust handler signals a
//     throw with `Err` rather than a panic because this crate's release profile is
//     `panic = "abort"` (`Cargo.toml`), so a panic cannot be caught the way JS catches a
//     `throw`.
//  3. SNAPSHOT ITERATION. [`HookBus::listeners_of`] copies the registration list under the
//     lock and releases it, so a handler that (un)subscribes during a round cannot mutate the
//     loop it is part of (and cannot make a later handler skip).
//  4. SYNC POINTS EXIST. Thread create/delete/stop/pause happen inside synchronous handle
//     methods a caller cannot await, so [`HookBus::fire`] exists.
//  5. EMPTY IS FREE. [`HookBus::has`] lets an emit site skip the emit AND the payload
//     construction. TS's guard idiom is `if (bus.has(event)) await bus.emit(event, factory)`;
//     the harness interleaves a delegated child's frames with its parent's on exactly those
//     microtask boundaries, so an unguarded `await` changes OBSERVABLE event order even with
//     zero plugins installed.
//  6. SUBSCRIBED-YET-INERT IS ALSO FREE - [`HookBus::emit_maybe_sync`] runs the same
//     sequential fail-open round and returns [`MaybeSync::Decided`] while every handler stayed
//     synchronous, switching to [`MaybeSync::Deferred`] only at the FIRST handler registered as
//     async. See "THE RUST MODEL OF THE SYNC FAST PATH" below.
//  7. A WILDCARD IS AN OBSERVER, NOT A PARTY. `on('*')` gets every event
//     [`is_wildcard_deliverable`] accepts, and its result is DISCARDED. `live()` expands it
//     into the exact events it will receive - never into an invented `'*'` key.
//  8. EVERY DROP IS REPORTED - [`HandlerFailureSink`], once per dropped handler, with the kind
//     when the reason was NOT a failure.
//  9. A GATED OWNER IS INERT, NOT UNREGISTERED. A registration may carry a live `enabled`
//     predicate; dispatch, `has()` and `count()` all consult it PER EVENT; `harness.dispose` is
//     exempt; `live()` stays registration-based. See [`Registration::enabled`].
//
// THE RUST MODEL OF THE SYNC FAST PATH (rule 6) - READ THIS BEFORE "SIMPLIFYING" IT
// ---------------------------------------------------------------------------------
// JS decides sync-vs-async by inspecting what the handler RETURNED (`isThenable`,
// `hooks.ts:317`): an `async` function always returns a thenable, so it always defers, while a
// plain function returning an object stays on the caller's tick. Rust has no observable
// "returned a thenable" moment - every boxed future is polled by whoever holds it. Polling
// once with `now_or_never()` would be WRONG: an `async fn` that happens to resolve immediately
// would take the sync path, and TS would have deferred it.
//
// So sync-vs-async is a property of the REGISTRATION: [`HandlerBody::Sync`] is the translation
// target of a TS non-thenable-returning handler, and [`HandlerBody::Async`] the target of a TS
// `async`/thenable-returning one. The bus then reproduces the JS schedule exactly: the `Sync`
// prefix runs inline and at the FIRST `Async` registration the round stops and hands back the
// rest as one future (`hooks.ts:806-870`, "runs the synchronous prefix immediately and defers
// only from the first promise"). Two consequences are pinned by the TS suite and hold here:
//   * a budget timer is armed only for an `Async` handler (`hooks.ts:687-695`) - a `Sync`
//     registration carrying `timeout_ms` changes nothing and arms no timer
//     (`hooks.test.ts:765-785`);
//   * a round whose owners are all gated off is a SYNCHRONOUS empty round (rule 9), never a
//     deferred one (`hooks.test.ts:996-1015`).
//
// NO PANIC PATH: nothing in this module unwinds. A handler's failure is `Err`, a reporter's
// failure is its own business, and a poisoned lock is taken anyway.
// =============================================================================

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures_util::FutureExt as _;
use serde_json::{Map, Value};
use tokio::time::{Duration, timeout};

use super::context::{PluginControls, TurnControls, fallback_controls};
use super::diagnostics::{DiagnosticEvent, DiagnosticKind, DiagnosticReporter};
use super::event::{
    ALWAYS_DELIVERED, Contribution, LifecycleEvent, Payload, SubscriptionTarget,
    is_wildcard_deliverable,
};

/// A boxed async handler body (the `Async` half of the TS handler union).
pub type HandlerFuture = Pin<Box<dyn Future<Output = HandlerResult> + Send>>;

/// A boxed deferred round: the tail of a round that met an async handler (rule 6).
pub type RoundFuture = Pin<Box<dyn Future<Output = Contribution> + Send>>;

// ---------------------------------------------------------------------------
// What a handler is handed, and what it may hand back
// ---------------------------------------------------------------------------

/// Everything ONE handler receives: the payload built for THIS registration, plus the live
/// control surface that registration is entitled to.
///
/// TS hands the same two things in one object: the payload (`payloadFor`, `hooks.ts:661`) and
/// the per-plugin api baked into it (`entry.api`, `hooks.ts:161-166`) so a handler sees ITS OWN
/// scoped settings rather than another plugin's (`hooks.test.ts:189`). The Rust seam is split in
/// two because only `turn.before` has a control surface: [`PluginControls`] is the per-plugin
/// api (always present) and [`TurnControls`] is the round-scoped transcript/park surface
/// (`Some` for `turn.before` only).
pub struct HandlerCall {
    /// The event being delivered. For a `'*'` registration this is the event inside the
    /// envelope (rule 7): the gate is applied with THIS event, which is why a disabled wildcard
    /// subscriber still receives `harness.dispose` (`host.test.ts:498-523`).
    pub event: LifecycleEvent,
    /// True when this call is going to a `'*'` registration. A wildcard handler reads the
    /// event/payload pair, and its result is discarded either way.
    pub wildcard: bool,
    /// The payload. Built once per handler (rule 1's "hook order is only observable if later
    /// handlers can see what earlier ones did"), and envelope-shaped for a wildcard.
    pub payload: Payload,
    /// The plugin-scoped api this registration was made with (`entry.api`).
    pub controls: Arc<dyn PluginControls>,
    /// The `turn.before` control surface, `None` for every other event. The RUN LOOP supplies it
    /// through [`HookBus::emit_with`] - it is the round's state, not the registration's
    /// (`hooks.ts:232-239`'s payload factory, see `context.rs`'s three-life split).
    pub turn: Option<Arc<dyn TurnControls>>,
}

impl HandlerCall {
    /// The round's control surface, when this event has one (`turn.before`).
    #[must_use]
    pub fn turn(&self) -> Option<&dyn TurnControls> {
        self.turn.as_deref()
    }

    /// The payload of a `turn.before` round, which every context-aware handler needs.
    #[must_use]
    pub fn turn_payload(&self) -> Option<&super::event::TurnBeforePayload> {
        match &self.payload {
            Payload::TurnBefore(payload) => Some(payload),
            _ => None,
        }
    }

    /// The plugin this call belongs to (`''` for a host-level subscription).
    #[must_use]
    pub fn owner(&self) -> &str {
        self.controls.owner()
    }
}

/// Manual `Debug`: the two seam fields are trait objects the integrator implements, and the seam
/// must not force `Debug` on a store-backed context. What a reader of a log line needs is the
/// identity and the payload, which is exactly what this prints.
impl std::fmt::Debug for HandlerCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerCall")
            .field("event", &self.event.as_str())
            .field("wildcard", &self.wildcard)
            .field("payload", &self.payload)
            .field("owner", &self.controls.owner())
            .field("has_turn", &self.turn.is_some())
            .finish()
    }
}

/// A handler that failed - the Rust spelling of a JS `throw`/`reject` (`hooks.ts:746-761`).
///
/// `message` is what reaches the operator (`report.diagnostic`), so it must read like the thrown
/// Error's message. `data` is optional structured detail; a framework drop never sets it, a
/// plugin may.
#[derive(Debug, Clone, PartialEq)]
pub struct HandlerFailure {
    pub message: String,
    pub data: Option<Map<String, Value>>,
    /// Which diagnostic kind this failure becomes. A plugin's own failure is `handler-threw` -
    /// TS's `throw` - and only the BUS may produce the other two, because a budget breach and an
    /// unparseable decision are facts about the contract, not about what the handler raised.
    pub kind: DiagnosticKind,
}

impl HandlerFailure {
    /// The common case: one human-readable sentence, the `new Error(text)` shape.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            data: None,
            kind: DiagnosticKind::HandlerThrew,
        }
    }

    /// A failure that also carries structured detail for the diagnostic.
    #[must_use]
    pub fn with_data(message: impl Into<String>, data: Map<String, Value>) -> Self {
        Self {
            message: message.into(),
            data: Some(data),
            kind: DiagnosticKind::HandlerThrew,
        }
    }

    /// The bus's own failure: the handler lost its budget race, so the report says WHICH of the
    /// two contract reasons dropped it (`hooks.ts:119-124` splits the kinds, and an operator
    /// reading "timed out after 250ms" knows to fix the budget rather than the plugin).
    pub fn timed_out(event: LifecycleEvent, owner: &str, timeout_ms: u64) -> Self {
        Self {
            message: budget_error_of(event, owner, timeout_ms),
            data: None,
            kind: DiagnosticKind::HandlerTimedOut,
        }
    }
}

/// What one handler returns: its contribution to the round, or the failure that drops it.
///
/// The `Ok` arm is a [`Contribution`] - TS's `HarnessHookResult` already normalized
/// (`contributionOf`, `hooks.ts:417`): `{ transcript }`, `{ decision }`, both, or nothing. A
/// handler that must report an unparseable decision returns [`Contribution::invalid_decision`],
/// and the bus then keeps the transcript AND reports the decision (rule 2's "the rest of its
/// result still applies").
pub type HandlerResult = Result<Contribution, HandlerFailure>;

/// One handler body, sync or async. See the header for why this is a REGISTRATION property and
/// not a return-value inspection.
#[derive(Clone)]
pub enum HandlerBody {
    /// Runs inline on the caller's task; arms no timer even when `timeout_ms` is set.
    Sync(Arc<dyn Fn(&HandlerCall) -> HandlerResult + Send + Sync>),
    /// Runs on the round's future; the only body a budget timer is armed for.
    Async(Arc<dyn Fn(Arc<HandlerCall>) -> HandlerFuture + Send + Sync>),
}

impl std::fmt::Debug for HandlerBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sync(_) => f.write_str("HandlerBody::Sync"),
            Self::Async(_) => f.write_str("HandlerBody::Async"),
        }
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// One subscription (`HookEntry`, `hooks.ts:164-182`).
pub struct Registration {
    /// The bus's monotonic registration stamp: rule 1's tie-break, and the handle `off()`
    /// removes by.
    pub id: u64,
    /// The plugin name (diagnostics, `live()` rows, and the park owner on `turn.before`). `''`
    /// is a real value here: it means "no identity" (a host-level subscription), and TS spreads
    /// it as an ABSENT owner key rather than as a plugin named `''` (`hooks.ts:671-677`).
    pub owner: String,
    /// What `on(...)` was pointed at: one event, or `'*'` (rule 7).
    pub target: SubscriptionTarget,
    /// Dispatch rank WITHIN one event: higher runs first, default 0, stable within a priority.
    /// It reorders nothing ACROSS events (`types:4774-4777`).
    pub priority: i64,
    /// Per-handler budget; `None` = unbounded (the historical behaviour). Only an `Async` body
    /// can hit it.
    pub timeout_ms: Option<u64>,
    /// True for a `'*'` registration: it receives the envelope and its result is ignored.
    pub wildcard: bool,
    /// The plugin-scoped api handed to this handler (`entry.api`).
    pub controls: Arc<dyn PluginControls>,
    /// THE GLOBAL ENABLEMENT GATE (rule 9) - the live "may this owner be delivered a hook right
    /// now" predicate the plugin host builds from THAT plugin's settings view
    /// (`boolean('enabled', true)`). `None` means "always deliver", which is every host-level
    /// registration and every deployment that never touched the switch.
    ///
    /// It is a FUNCTION and not a bool, and that is the whole feature: an operator flips the
    /// setting over HTTP and the next turn of an ALREADY RUNNING session reads the new answer,
    /// with no re-registration (`host.test.ts:471-496`). Read at dispatch, never cached.
    ///
    /// TS's "a gate that THROWS reads as enabled" clause (`hooks.ts:350-354`) has no Rust
    /// counterpart: a Rust predicate has no failure channel, so the fail-open outcome is the
    /// only one reachable. The OTHER half of that clause is ported exactly: a skipped delivery
    /// is never reported as a handler error (`hooks.test.ts:1016-1042`).
    pub enabled: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    pub body: HandlerBody,
}

/// Manual `Debug`: the gate is a `dyn Fn`, which Rust cannot print, so this reports whether a gate
/// exists rather than pretending to show it.
impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registration")
            .field("id", &self.id)
            .field("owner", &self.owner)
            .field("target", &self.target)
            .field("priority", &self.priority)
            .field("timeout_ms", &self.timeout_ms)
            .field("wildcard", &self.wildcard)
            .field("controls", &self.controls)
            .field("gated", &self.enabled.is_some())
            .field("body", &self.body)
            .finish()
    }
}

impl Clone for Registration {
    /// `Arc`-deep: a snapshot round shares one registration's handler and gate rather than
    /// cloning the closure, which is what lets rule 3 hold without a per-entry rebuild.
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            owner: self.owner.clone(),
            target: self.target,
            priority: self.priority,
            timeout_ms: self.timeout_ms,
            wildcard: self.wildcard,
            controls: Arc::clone(&self.controls),
            enabled: self.enabled.clone(),
            body: self.body.clone(),
        }
    }
}

/// Options for one `on(...)` call (`HarnessHookOptions` + `HarnessHookRegistration`).
#[derive(Clone, Default)]
pub struct HookOptions {
    /// The plugin name this registration belongs to.
    pub owner: String,
    /// Higher runs first (default 0).
    pub priority: i64,
    /// Per-handler budget in ms; `None` = unbounded.
    pub timeout_ms: Option<u64>,
    /// The plugin-scoped api the handler is handed. `None` uses [`fallback_controls`].
    pub controls: Option<Arc<dyn PluginControls>>,
    /// The live enablement gate (rule 9).
    pub enabled: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

impl std::fmt::Debug for HookOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookOptions")
            .field("owner", &self.owner)
            .field("priority", &self.priority)
            .field("timeout_ms", &self.timeout_ms)
            .field("has_controls", &self.controls.is_some())
            .field("gated", &self.enabled.is_some())
            .finish()
    }
}

/// The bus's own refusal of a registration (`on`'s `throw new TypeError`s, `hooks.ts:591-615`).
///
/// The sentences are byte-identical to TS because the TS suite asserts on them
/// (`hooks.test.ts:885-897`) and because an operator-facing error that names the offending thing
/// is the whole value of a strict registration edge.
#[derive(Debug, Clone, PartialEq)]
pub struct RegistrationError(pub String);

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RegistrationError {}

/// The `Unknown lifecycle event: X.` refusal (`hooks.ts:592`). An invented name must not reach a
/// registration the bus would then never visit.
#[must_use]
pub fn unknown_event_error(name: &str) -> RegistrationError {
    RegistrationError(format!("Unknown lifecycle event: {name}."))
}

/// The `A "E" timeoutMs must be a finite number >= 0.` refusal (`hooks.ts:614`).
///
/// Rust's `u64` cannot carry NaN or Infinity, so the only reachable spelling of "not a finite
/// number >= 0" is the caller's own sentinel: `u64::MAX` is a budget no timer can arm, and TS
/// refuses that input rather than running unbounded by accident.
// No `#[must_use]` here: the return is a `Result`, which is already `#[must_use]` in std, and an
// attribute with no message on top of it is the `clippy::double_must_use` redundancy.
fn check_options(name: &str, options: &HookOptions) -> Result<(), RegistrationError> {
    if options.timeout_ms == Some(u64::MAX) {
        return Err(RegistrationError(format!(
            "A \"{name}\" timeoutMs must be a finite number >= 0."
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Failure reporting (rule 8)
// ---------------------------------------------------------------------------

/// Called once per handler that was dropped (`HarnessHookBusOptions.onHandlerError`).
///
/// The bus hands a [`DiagnosticEvent`] already shaped for the durable channel, so an integrator
/// forwards it straight to a [`DiagnosticReporter`] and the store.
pub trait HandlerFailureSink: Send + Sync {
    /// `handler-threw` for a failure; `handler-timed-out` / `invalid-decision` when the bus
    /// itself dropped the handler for a contract reason (`hooks.ts:119-124`).
    fn on_handler_error(&self, event: DiagnosticEvent);
}

/// Bridges rule 8 onto the diagnostic channel (what `harness.ts` does: it installs a reporter and
/// routes every drop to `report.diagnostic`).
#[derive(Clone, Debug)]
pub struct ReporterFailureSink {
    reporter: Arc<dyn DiagnosticReporter>,
}

impl ReporterFailureSink {
    #[must_use]
    pub fn new(reporter: Arc<dyn DiagnosticReporter>) -> Self {
        Self { reporter }
    }
}

impl HandlerFailureSink for ReporterFailureSink {
    fn on_handler_error(&self, event: DiagnosticEvent) {
        // `hooks.ts:526-528` - a diagnostics reporter must never become the failure it reports.
        // A Rust reporter signals failure inside itself (see `DiagnosticReporter`), so this call
        // cannot raise.
        self.reporter.report(event);
    }
}

/// The synthetic message for a dropped handler (`budgetErrorOf`, `hooks.ts:384-387`). Byte-
/// identical to TS because it IS the diagnostic text an operator reads - and the exact string the
/// TS suite pins (`hooks.test.ts:755-762`).
#[must_use]
pub fn budget_error_of(event: LifecycleEvent, owner: &str, timeout_ms: u64) -> String {
    format!(
        "A \"{owner}\" handler for \"{}\" timed out after {timeout_ms}ms and was dropped (fail-open).",
        event.as_str()
    )
}

/// The synthetic message for a decision the contract cannot parse (`decisionErrorOf`,
/// `hooks.ts:391-404`); `received` prints as `JSON.stringify` output, and the pinned shape is
/// `hooks.test.ts:806-813` / `:834`.
#[must_use]
pub fn decision_error_of(event: LifecycleEvent, owner: &str, received: &Value) -> String {
    format!(
        "A \"{owner}\" handler for \"{}\" returned a decision this contract cannot parse and it was ignored: {received}.",
        event.as_str()
    )
}

// ---------------------------------------------------------------------------
// The bus
// ---------------------------------------------------------------------------

/// One round's dispatch list, gate applied and ordered (`listenersOf`, `hooks.ts:541-563`).
type DispatchList = Vec<Arc<Registration>>;

/// What `emit_maybe_sync` hands back (`HarnessMaybeAsync`, `hooks.ts:228-230`).
pub enum MaybeSync {
    /// Every handler stayed synchronous: the value is decided and the caller applies it on the
    /// SAME tick - no `await`, no microtask boundary, no reordering of the run's frames.
    Decided(Contribution),
    /// A handler did real work: the boundary is the point, and the caller awaits this.
    Deferred(RoundFuture),
}

impl MaybeSync {
    /// Awaiting form, for a call site that is going to await regardless.
    pub async fn resolve(self) -> Contribution {
        match self {
            Self::Decided(contribution) => contribution,
            Self::Deferred(future) => future.await,
        }
    }

    /// True when the round is already decided (the `{ sync: true }` discriminant).
    #[must_use]
    pub const fn is_decided(&self) -> bool {
        matches!(self, Self::Decided(_))
    }
}

/// The registrations map plus the sticky dispatch flag.
#[derive(Default)]
struct Registrations {
    /// Keyed by event, with the `'*'` registrations kept in their own list (they are a sibling
    /// key in TS, never an event). A `Vec` keeps registration order, which is rule 1's tie-break.
    by_event: BTreeMap<LifecycleEvent, Vec<Arc<Registration>>>,
    wildcard: Vec<Arc<Registration>>,
    /// Sticky once ANY registration carries a non-zero priority or a wildcard exists
    /// (`hooks.ts:506-511`): it only ever makes dispatch do MORE work than the fast path, never
    /// less, and it cannot change an order - sorting an all-zero list by `(priority, id)`
    /// reproduces registration order exactly, because `id` is monotonic. Sticky also avoids a
    /// rescan on every unsubscribe.
    ordered: bool,
}

/// The typed lifecycle hook bus (`createHookBus`).
///
/// A harness owns exactly one, so subscriptions die with it and a test harness never inherits
/// another test's hooks. Cheap to share: wrap in `Arc`, clone per owner.
pub struct HookBus {
    /// The snapshot lock, held ONLY for the map read/copy - never across an `await`, which is
    /// what makes rule 3 and a handler that subscribes mid-round safe.
    state: Mutex<Registrations>,
    /// Monotonic registration stamp (rule 1's tie-break). Bus-scoped, not process-scoped.
    sequence: AtomicU64,
    /// Rule 8's reporter. `None` = drops stay silent but are still drops.
    on_handler_error: Option<Arc<dyn HandlerFailureSink>>,
}

impl std::fmt::Debug for HookBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();

        f.debug_struct("HookBus")
            .field(
                "events",
                &state
                    .by_event
                    .iter()
                    .map(|(event, list)| (event.as_str(), list.len()))
                    .collect::<Vec<_>>(),
            )
            .field("wildcard", &state.wildcard.len())
            .field("ordered", &state.ordered)
            .finish()
    }
}

impl Default for HookBus {
    fn default() -> Self {
        Self::new()
    }
}

impl HookBus {
    /// A bus with no failure reporter (drops are still drops, just silent).
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(Registrations::default()),
            sequence: AtomicU64::new(0),
            on_handler_error: None,
        }
    }

    /// A bus that reports every drop (`HarnessHookBusOptions.onHandlerError`).
    #[must_use]
    pub fn with_reporter(sink: Arc<dyn HandlerFailureSink>) -> Self {
        Self {
            on_handler_error: Some(sink),
            ..Self::new()
        }
    }

    /// Locks without ever panicking on a poisoned lock: a hook that panicked inside a round must
    /// not take the whole bus down with it (rule 2 is fail-OPEN).
    fn lock(&self) -> MutexGuard<'_, Registrations> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // -- registration --------------------------------------------------------

    /// Subscribes one SYNCHRONOUS handler (`on`, `hooks.ts:586-653`).
    ///
    /// `name` is a dotted event name or `'*'`; an unknown name is the same refusal TS throws.
    /// Returns the registration id, which [`Self::off`] removes by - idempotent, and bound to
    /// THIS registration, so two identical handlers from two plugins are independent.
    pub fn on_sync(
        &self,
        name: &str,
        handler: impl Fn(&HandlerCall) -> HandlerResult + Send + Sync + 'static,
        options: HookOptions,
    ) -> Result<u64, RegistrationError> {
        self.register(name, HandlerBody::Sync(Arc::new(handler)), options)
    }

    /// Subscribes one ASYNC handler (`on`, same registration edge; rule 6 defers from here).
    pub fn on_async(
        &self,
        name: &str,
        handler: impl Fn(Arc<HandlerCall>) -> HandlerFuture + Send + Sync + 'static,
        options: HookOptions,
    ) -> Result<u64, RegistrationError> {
        self.register(name, HandlerBody::Async(Arc::new(handler)), options)
    }

    /// The one registration path. Every refusal happens BEFORE the map is touched
    /// (`hooks.test.ts:885-897`, "nothing was registered on the way to the refusal").
    pub fn register(
        &self,
        name: &str,
        body: HandlerBody,
        mut options: HookOptions,
    ) -> Result<u64, RegistrationError> {
        let target = super::event::subscription_of_name(name)
            .ok_or_else(|| unknown_event_error(name))?;

        check_options(name, &options)?;

        // An unscoped registration still needs an api: a host-level `on` has no settings view,
        // and `fallback_controls` is the read-only nothing that TS's absent `api` corresponds to.
        let controls = options
            .controls
            .get_or_insert_with(fallback_controls);

        let wildcard = matches!(target, SubscriptionTarget::Wildcard);
        let id = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;

        let registration = Arc::new(Registration {
            id,
            owner: std::mem::take(&mut options.owner),
            target,
            priority: options.priority,
            timeout_ms: options.timeout_ms,
            wildcard,
            controls: Arc::clone(controls),
            enabled: options.enabled,
            body,
        });

        let mut state = self.lock();

        if wildcard || registration.priority != 0 {
            // Rule 1's fast-path switch: from here on every dispatch sorts, because a priority
            // or a wildcard exists somewhere on this bus.
            state.ordered = true;
        }

        match target {
            SubscriptionTarget::Event(event) => {
                state.by_event.entry(event).or_default().push(Arc::clone(&registration));
            }
            SubscriptionTarget::Wildcard => state.wildcard.push(Arc::clone(&registration)),
        }

        Ok(id)
    }

    /// Removes one registration (`off`, idempotent: a second call for the same id is a no-op,
    /// `hooks.test.ts:225-249`).
    pub fn off(&self, id: u64) {
        let mut state = self.lock();

        for list in state.by_event.values_mut() {
            if let Some(at) = list.iter().position(|entry| entry.id == id) {
                list.remove(at);
                return;
            }
        }

        if let Some(at) = state.wildcard.iter().position(|entry| entry.id == id) {
            state.wildcard.remove(at);
        }
    }

    /// Drops every subscription (harness teardown / tests, `hooks.test.ts:338-360`).
    pub fn clear(&self) {
        let mut state = self.lock();
        state.by_event.clear();
        state.wildcard.clear();
        state.ordered = false;
    }

    // -- the guards (rule 5 + rule 9) ----------------------------------------

    /// True when at least one handler would RUN for this event right now - the emitter's cheap
    /// skip test (`has`, `hooks.ts:570-584`).
    ///
    /// A wildcard registration makes every DELIVERABLE event true, which is exactly the audit
    /// plugin case (`hooks.test.ts:665-693`); `host.state_change` stays false for a wildcard. A
    /// gated-off owner is NOT counted, so `has()` lets an emit site skip the payload construction
    /// as well as the handler call - the difference between "a disabled compaction costs
    /// nothing" and "it costs an await".
    #[must_use]
    pub fn has(&self, event: LifecycleEvent) -> bool {
        let state = self.lock();

        if deliverable_list(state.by_event.get(&event), event) {
            return true;
        }

        is_wildcard_deliverable(event) && deliverable_list(Some(&state.wildcard), event)
    }

    /// How many handlers this event would REACH right now (`count`, rule 9: "handlers that run
    /// for this event", so a gated-off owner is not counted - `host.test.ts:478-495`).
    #[must_use]
    pub fn count(&self, event: LifecycleEvent) -> usize {
        self.listeners_of(event).len()
    }

    /// Every event with listeners, plus the owners per event, in event-declaration order (`live`,
    /// rules 7 and 9).
    ///
    /// This is what the server reports as the LIVE hook surface of a plugin: declared
    /// `metadata.hooks` is the intent, this is the fact - and it is the source of the REST row's
    /// `hooks` field (see `Temp/fixtures/harness/plugin-catalogue/descriptors-by-plugin.json`,
    /// where `async-ping` answers its six SUBSCRIBED events, not its six DECLARED patterns).
    /// A `'*'` registration is EXPANDED into the events it will actually receive, the owner is
    /// repeated per event, and `host.state_change` is never claimed for it
    /// (`hooks.test.ts:678-687`). `live()` stays REGISTRATION-BASED on purpose: a disabled plugin
    /// did still subscribe, and the Plugins pane's declared-vs-subscribed comparison would
    /// otherwise go blank on a toggle (`host.test.ts:485-487`).
    #[must_use]
    pub fn live(&self) -> Vec<(&'static str, Vec<String>)> {
        let state = self.lock();

        LifecycleEvent::ALL
            .into_iter()
            .filter_map(|event| {
                let mut owners: Vec<String> = Vec::new();

                if let Some(list) = state.by_event.get(&event) {
                    owners.extend(list.iter().map(|entry| entry.owner.clone()));
                }

                if is_wildcard_deliverable(event) {
                    owners.extend(state.wildcard.iter().map(|entry| entry.owner.clone()));
                }

                (!owners.is_empty()).then(|| (event.as_str(), owners))
            })
            .collect()
    }

    /// The events ONE owner is live on, in the order it SUBSCRIBED (`host.ts`'s per-plugin hook
    /// surface, and the REST row's `hooks` field).
    ///
    /// Registration order, not catalogue order: a plugin's own list is the order its setup ran in,
    /// which is what an operator comparing it against `metadata.hooks` expects to read. A `'*'`
    /// registration contributes the events it will actually receive AT ITS OWN POSITION, in the
    /// catalogue's order, and an event already claimed by a named registration is not repeated.
    #[must_use]
    pub fn live_for(&self, owner: &str) -> Vec<&'static str> {
        let state = self.lock();

        // (registration id, events that registration is live on) - `id` is monotonic, so sorting by
        // it reproduces the order `on(...)` ran in, across both the named and the wildcard lists.
        let mut ordered: Vec<(u64, Vec<&'static str>)> = Vec::new();

        for list in state.by_event.values() {
            for entry in list {
                // A named registration is live on exactly the event it was pointed at.
                if entry.owner == owner
                    && let SubscriptionTarget::Event(event) = entry.target
                {
                    ordered.push((entry.id, vec![event.as_str()]));
                }
            }
        }

        for entry in &state.wildcard {
            if entry.owner == owner {
                let events = LifecycleEvent::ALL
                    .iter()
                    .filter(|event| is_wildcard_deliverable(**event))
                    .map(|event| event.as_str())
                    .collect();

                ordered.push((entry.id, events));
            }
        }

        ordered.sort_by_key(|(id, _)| *id);

        let mut events: Vec<&'static str> = Vec::new();

        for (_, list) in ordered {
            for event in list {
                if !events.contains(&event) {
                    events.push(event);
                }
            }
        }

        events
    }

    /// The owners subscribed to one event, registration order, wildcard expansions included.
    /// The REST row's `hooks` list for a plugin is `live()` filtered to that owner.
    #[must_use]
    pub fn live_owners(&self, event: LifecycleEvent) -> Vec<String> {
        self.live()
            .into_iter()
            .find(|(name, _)| *name == event.as_str())
            .map(|(_, owners)| owners)
            .unwrap_or_default()
    }

    // -- dispatch ------------------------------------------------------------

    /// Awaited, sequential, fail-open (`emit`, `hooks.ts:872-875`).
    ///
    /// The shape for a call site that is going to `await` regardless. A hook that is ALWAYS
    /// subscribed and usually inert wants [`Self::emit_maybe_sync`] instead (rule 6).
    ///
    /// A `turn.before` emit needs the round's control surface: use [`Self::emit_with`].
    pub async fn emit(&self, event: LifecycleEvent, payload: Payload) -> Contribution {
        match self.emit_maybe_sync_with(event, payload, None) {
            MaybeSync::Decided(contribution) => contribution,
            MaybeSync::Deferred(future) => future.await,
        }
    }

    /// [`Self::emit`] with the round's control surface (`turn.before`'s live writes).
    pub async fn emit_with(
        &self,
        event: LifecycleEvent,
        payload: Payload,
        turn: Arc<dyn TurnControls>,
    ) -> Contribution {
        match self.emit_maybe_sync_with(event, payload, Some(turn)) {
            MaybeSync::Decided(contribution) => contribution,
            MaybeSync::Deferred(future) => future.await,
        }
    }

    /// The same round, staying on the caller's task while every handler is synchronous (rule 6).
    /// See the module header for the Rust model of the sync fast path.
    #[must_use]
    pub fn emit_maybe_sync(&self, event: LifecycleEvent, payload: Payload) -> MaybeSync {
        self.emit_maybe_sync_with(event, payload, None)
    }

    /// [`Self::emit_maybe_sync`] with the round's control surface - the shape `harness.ts`'s
    /// `emitTurnBefore` uses, and the ONLY way a `turn.before` handler can write.
    #[must_use]
    pub fn emit_maybe_sync_with(
        &self,
        event: LifecycleEvent,
        payload: Payload,
        turn: Option<Arc<dyn TurnControls>>,
    ) -> MaybeSync {
        // Snapshot up front (rule 3): a handler may (un)subscribe during this round, and that
        // must never change which handlers THIS round visits.
        let snapshot = self.listeners_of(event);

        if snapshot.is_empty() {
            // Rules 5/6: no listener means no payload build, no timer, no future, no boundary.
            return MaybeSync::Decided(Contribution::nothing());
        }

        let mut round = Contribution::nothing();

        for (index, registration) in snapshot.iter().enumerate() {
            let call = build_call(event, registration, &payload, &turn);

            let HandlerBody::Async(_) = &registration.body else {
                // The SYNC PREFIX: invoke inline, fold, continue (`hooks.ts:816-840`).
                let HandlerBody::Sync(handler) = &registration.body else {
                    unreachable!("the else arm above is the Async case");
                };

                let outcome = handler(&call);
                self.apply(event, registration, outcome, &mut round);

                continue;
            };

            // The FIRST async handler: everything from here on is one future, in the same order,
            // with the round object carried forward (`finishRound`, `hooks.ts:768-804`).
            let tail = snapshot[index..].to_vec();
            let reporter = self.on_handler_error.clone();

            return MaybeSync::Deferred(
                async move {
                    let mut round = round;
                    let mut call: Option<Arc<HandlerCall>> = Some(call);

                    for registration in &tail {
                        // Handler 0 already RAN; only its result is outstanding, so its call is
                        // reused rather than rebuilt (`hooks.ts:787-789`).
                        let call = call
                            .take()
                            .unwrap_or_else(|| build_call(event, registration, &payload, &turn));

                        let outcome = match &registration.body {
                            HandlerBody::Async(handler) => {
                                // `&**`, not `&`: the field is an `Arc` around a `dyn Fn`, and the
                                // helper wants the bare `&dyn Fn`. Written explicitly so the
                                // deref coercion is not left to inference at a trait-object
                                // boundary (where the return type is itself a boxed future).
                                run_async(&**handler, Arc::clone(&call), registration).await
                            }
                            // A SYNC registration after the first async one still runs inline,
                            // inside the tail: the tail differs from the prefix only in that the
                            // round is already on a microtask.
                            HandlerBody::Sync(handler) => handler(&call),
                        };

                        apply_result(event, registration, outcome, &mut round, reporter.as_ref());
                    }

                    round
                }
                .boxed(),
            );
        }

        MaybeSync::Decided(round)
    }

    /// Synchronous fire-and-forget for a lifecycle point inside a sync handle method (rule 4).
    ///
    /// A `timeout_ms` budget means nothing here: `fire` never waits, so there is no wait to bound
    /// (`hooks.ts:277-283`). Sync handlers run inline; an async handler is spawned onto the
    /// current tokio runtime, which is what "starts the handlers in order and swallows
    /// everything, including a rejected promise a handler returned anyway" means in Rust.
    ///
    /// ONE DOCUMENTED DIFFERENCE: with no tokio runtime in scope an async handler has nowhere to
    /// run and is skipped. Node's event loop always exists; a Rust host that calls `fire` from
    /// outside a runtime does not, and every shipped synchronous emit site (thread
    /// create/delete/stop/pause, `harness.dispose`) registers synchronous handlers.
    pub fn fire(&self, event: LifecycleEvent, payload: Payload) {
        self.fire_with(event, payload, None);
    }

    /// [`Self::fire`] with the round's control surface.
    pub fn fire_with(&self, event: LifecycleEvent, payload: Payload, turn: Option<Arc<dyn TurnControls>>) {
        let snapshot = self.listeners_of(event);

        for registration in &snapshot {
            let call = Arc::new(build_call(event, registration, &payload, &turn));

            match &registration.body {
                HandlerBody::Sync(handler) => {
                    // `fire` ignores the round result (`hooks.ts:280-283`): the sync caller
                    // cannot consume one. A drop is still reported (rule 8).
                    let outcome = handler(&call);
                    apply_result(event, registration, outcome, &mut Contribution::nothing(), self.on_handler_error.as_ref());
                }
                HandlerBody::Async(handler) => {
                    let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
                        continue;
                    };

                    let handler = Arc::clone(handler);
                    let registration = Arc::clone(registration);
                    let reporter = self.on_handler_error.clone();

                    handle.spawn(async move {
                        // `&*`, not `&`: `handler` is the owned `Arc`, and the helper takes the
                        // bare `&dyn Fn` (see the sync-prefix call site for the same cast).
                        let outcome = run_async(&*handler, Arc::clone(&call), &registration).await;
                        apply_result(
                            event,
                            &registration,
                            outcome,
                            &mut Contribution::nothing(),
                            reporter.as_ref(),
                        );
                    });
                }
            }
        }
    }

    // -- internals -----------------------------------------------------------

    /// Whether one registration is DELIVERED this event (`isDeliverable`, `hooks.ts:347-355`).
    /// The gate is a live read, so this is re-decided on every emit - which is what makes an
    /// operator's click reach a running session.
    fn is_deliverable(registration: &Registration, event: LifecycleEvent) -> bool {
        // Teardown is never skipped: a plugin switched off mid-session still has to see the
        // teardown point, or its timers, listeners and handles leak for the life of the process
        // (async-ping's `harness.dispose` handler is exactly such a timer sweep).
        if registration.enabled.is_none() || event == ALWAYS_DELIVERED {
            return true;
        }

        (registration.enabled.as_ref().expect("checked above"))()
    }

    /// The dispatch list for one event: the named registrations plus, when the event is
    /// deliverable to a wildcard, every wildcard handler - gate applied, priority order
    /// (`listenersOf`, `hooks.ts:541-563`).
    fn listeners_of(&self, event: LifecycleEvent) -> DispatchList {
        let state = self.lock();

        let named: DispatchList = state
            .by_event
            .get(&event)
            .map(|list| gate(list, event))
            .unwrap_or_default();

        if !is_wildcard_deliverable(event) {
            // `host.state_change`: an explicit subscriber only, never the wildcard
            // (`hooks.test.ts:695-710`).
            return if state.ordered && named.len() > 1 { ordered(named) } else { named };
        }

        let wild = gate(&state.wildcard, event);

        if named.is_empty() {
            return ordered(wild);
        }

        if wild.is_empty() {
            return if state.ordered && named.len() > 1 { ordered(named) } else { named };
        }

        let mut joined = named;
        joined.extend(wild);

        ordered(joined)
    }

    /// Folds one handler's outcome into the round, reporting every drop (rule 2 + rule 8).
    fn apply(
        &self,
        event: LifecycleEvent,
        registration: &Registration,
        outcome: HandlerResult,
        round: &mut Contribution,
    ) {
        apply_result(event, registration, outcome, round, self.on_handler_error.as_ref());
    }
}

/// Applies rule 9 to a registration list (`gated`, `hooks.ts:361-370`). The fast path is the
/// point: with no gate on this list this is one clone per entry and no predicate call, so a bus
/// that never touched the switch costs exactly what it cost before rule 9.
fn gate(list: &[Arc<Registration>], event: LifecycleEvent) -> DispatchList {
    if event == ALWAYS_DELIVERED {
        return list.to_vec();
    }

    if !list.iter().any(|entry| entry.enabled.is_some()) {
        return list.to_vec();
    }

    list.iter()
        .filter(|entry| HookBus::is_deliverable(entry, event))
        .cloned()
        .collect()
}

/// `has()`'s allocation-free "would a round run?" test (`listDeliverable`, `hooks.ts:575-584`).
fn deliverable_list(list: Option<&Vec<Arc<Registration>>>, event: LifecycleEvent) -> bool {
    let Some(list) = list else { return false };
    if list.is_empty() {
        return false;
    }
    if event == ALWAYS_DELIVERED {
        return true;
    }

    list.iter().any(|entry| HookBus::is_deliverable(entry, event))
}

/// Priority order, then registration order, higher priority FIRST (`byPriority`,
/// `hooks.ts:532-533`; pinned by `hooks.test.ts:712-727`, including a negative priority sorting
/// AFTER the defaults and ties keeping registration order).
fn ordered(mut list: DispatchList) -> DispatchList {
    list.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.id.cmp(&right.id))
    });

    list
}

/// Resolves one handler's call (`payloadFor`, `hooks.ts:661-685`): the payload for THIS
/// registration, the plugin-scoped api it registered with, and - only when the emitter handed one
/// - the round's control surface.
fn build_call(
    event: LifecycleEvent,
    registration: &Registration,
    payload: &Payload,
    turn: &Option<Arc<dyn TurnControls>>,
) -> Arc<HandlerCall> {
    let wildcard = registration.wildcard;

    Arc::new(HandlerCall {
        event,
        wildcard,
        payload: payload.clone(),
        controls: Arc::clone(&registration.controls),
        // The emitter decides whether a round surface exists: `emit_with` is the only entry that
        // carries one, and it is the run loop's job to pass it ONLY on `turn.before` (the payload
        // union is what says so, `types:4224-4255`). A plain `None` - never a panic - is what a
        // `turn.before` emitted without a round means: the handler observes the payload and cannot
        // write, which is the same "no controls" answer a wildcard gets on every event.
        turn: turn.clone(),
    })
}

/// Runs one async handler, arming the budget ONLY here (`withBudget`, `hooks.ts:687-731`).
///
/// A budget bounds the WAIT, not the damage: losing the race drops this handler's contribution
/// and reports it, the round continues with the next handler, and a side effect the handler
/// already made (a flag write, an api call) is NOT undone.
async fn run_async(
    handler: &(dyn Fn(Arc<HandlerCall>) -> HandlerFuture + Send + Sync),
    call: Arc<HandlerCall>,
    registration: &Registration,
) -> HandlerResult {
    let future = handler(Arc::clone(&call));

    match registration.timeout_ms {
        // The unbounded case, exactly as before: no wrapper, no timer.
        None => future.await,
        // A zero budget is legitimate (`timeoutMs: 0` = "must already be resolved"), and tokio's
        // `timeout` answers the same way: the handler loses the race on the first poll.
        Some(ms) => match timeout(Duration::from_millis(ms), future).await {
            Ok(result) => result,
            Err(_elapsed) => Err(HandlerFailure::timed_out(
                call.event,
                &registration.owner,
                ms,
            )),
        },
    }
}

/// The one reducer for a round: report every drop, then merge the survivor's contribution
/// (`finishRound`'s body, `hooks.ts:792-800`, plus `mergeContribution`, `hooks.ts:486-489`).
fn apply_result(
    event: LifecycleEvent,
    registration: &Registration,
    outcome: HandlerResult,
    round: &mut Contribution,
    reporter: Option<&Arc<dyn HandlerFailureSink>>,
) {
    let contribution = match outcome {
        Ok(contribution) => contribution,
        Err(failure) => {
            // `handler-threw`: it threw or rejected, so its result is discarded. A skipped
            // delivery (rule 9) never reaches here - that is deliberate, because "the operator
            // turned this plugin off" is not a handler failure.
            report_drop(
                reporter,
                DiagnosticEvent {
                    // The failure names its own kind: a throw is `handler-threw` and a budget
                    // breach is `handler-timed-out`, and conflating them tells an operator the
                    // plugin is broken when the real fix is a bigger budget.
                    kind: failure.kind,
                    owner: registration.owner.clone(),
                    event: Some(event.as_str().to_owned()),
                    message: failure.message,
                    data: failure.data,
                },
            );

            return;
        }
    };

    if let Some(received) = &contribution.invalid_decision {
        // T2.2 rule 5: the decision is ignored and REPORTED, and the rest of the handler's
        // result still applies - so the transcript it returned merges below
        // (`hooks.test.ts:787-814`).
        report_drop(
            reporter,
            DiagnosticEvent::failure(
                DiagnosticKind::InvalidDecision,
                registration.owner.clone(),
                event.as_str(),
                decision_error_of(event, &registration.owner, received),
            ),
        );
    }

    if registration.wildcard {
        // Rule 7: a wildcard is an OBSERVER, and its result is DISCARDED even on a `turn.before`
        // frame - an audit plugin must not rewrite a request by accident.
        return;
    }

    round.merge(&contribution);
}

/// Rule 8's single reporting point.
fn report_drop(reporter: Option<&Arc<dyn HandlerFailureSink>>, event: DiagnosticEvent) {
    if let Some(sink) = reporter {
        sink.on_handler_error(event);
    }
}
