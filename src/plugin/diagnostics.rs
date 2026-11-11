// =============================================================================
// plugin/diagnostics.rs - the diagnostic plane every shipped plugin reports on.
//
// TypeScript origin, one section per ported surface:
//   * `packages/underload/harness/src/types/index.ts:4297` `HarnessDiagnosticEvent` -
//     the WIRE event (`{ type:'plugin_diagnostic', kind, event?, owner, message, data? }`);
//   * `src/types/index.ts:4322` `HarnessPluginDiagnosticReport` - the NARROW object a
//     plugin may author through `api.report.diagnostic` (kind is pinned to `policy`);
//   * `src/plugin/host.ts:100-107` - the host stamps `owner` with the plugin's own name and
//     hands ONE reporter per plugin;
//   * `src/contract.rs` `SessionPluginDiagnostic` / `src/persistence.rs`
//     `PLUGIN_DIAGNOSTIC_KINDS` + `MAX_PLUGIN_DIAGNOSTICS` - the durable landing spot and
//     the newest-24 cap that already exist in this crate.
//
// WHY THE SINK IS A TRAIT (and not `&mut SessionState`)
// -----------------------------------------------------
// `api.report.diagnostic(...)` in JS writes straight into a live server. Here the durable
// landing spot is `SessionState.pluginDiagnostics`, reached through
// `SessionState::note_plugin_diagnostic(&mut self, entry, at)` - a method the run-loop
// integrator owes this crate. The plugin layer must NOT name `SessionStore`: every other
// edge in the crate points store -> plugin, and a plugin -> store dependency would invert
// it. So the host names this contract and the integrator wires it in about five lines:
//
//     struct StoreSink { state: SessionState }                    // wraps the store
//     impl DiagnosticSink for StoreSink {
//         fn note_plugin_diagnostic(&mut self, entry: SessionPluginDiagnostic, at: u64) {
//             self.state.note_plugin_diagnostic(entry, at);       // the store's newest-24 cap
//         }
//     }
//
// The method NAME and SHAPE are exactly the store method the integrator was already told to
// add: `&mut self` because the document mutates, and `at` passed IN (not read from a clock
// here) so a persistence sink stays deterministic under test - the reporter stamps.
//
// THE FOUR KINDS ARE A CLOSED SET (types/index.ts:4283-4296)
// ----------------------------------------------------------
//   handler-threw      a handler threw or rejected; its result is discarded.
//   handler-timed-out  it outlived its `timeoutMs`; its late result is discarded too.
//   invalid-decision   it returned a `decision` the contract cannot parse (rule 5); the
//                      REST of its result still applies.
//   policy             NOT a failure: the plugin's own report of what it decided, authored
//                      by the plugin rather than by the framework. It is the only kind with
//                      no `event`, because a decision is not tied to one hook.
//
// The first three are produced BY the framework ABOUT a plugin; `policy` is produced BY a
// plugin ABOUT itself. They share one channel because the channel answers "what is this
// plugin doing that I cannot see".
//
// INVARIANTS THAT MUST NOT DRIFT
// ------------------------------
//   * `message` is never empty (`types/index.ts:4310`); a caller with no text has no report.
//   * `owner` is attributed, never guessed: the host stamps it from the REGISTRATION, so a
//     plugin cannot forge a report about another plugin (`host.ts:100`).
//   * `event` is present for every framework failure and absent for a `policy` report -
//     naming a hook on a policy report would claim a failure that did not happen.
//   * A sink failure never travels back into a turn: a diagnostic is an observer, never an
//     authority, so the `Mutex` poison path records rather than raises.
//   * The cap is the STORE's (`MAX_PLUGIN_DIAGNOSTICS`), not this module's: the reporter
//     emits in order and lets the sink trim, which is what keeps a restart holding the
//     newest records exactly as TS does.
// =============================================================================

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde_json::{Map, Value};

use crate::contract::{SessionPluginDiagnostic, SessionPluginDiagnosticKind};

/// Every `kind` the diagnostic channel carries (`HarnessDiagnosticEvent.kind`).
///
/// The wire spellings are the four literals of `types/index.ts:4299`; the persistence layer
/// (`src/persistence.rs` `PLUGIN_DIAGNOSTIC_KINDS`) accepts exactly these and normalises
/// anything else, which is why this is an enum rather than a free string: a Rust plugin
/// cannot invent a fifth kind the durable document would then have to launder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticKind {
    /// A handler threw or rejected, so its result was discarded (fail-open, rule 2).
    HandlerThrew,
    /// A handler outlived its `timeoutMs` budget and was dropped (fail-open, rule 4).
    HandlerTimedOut,
    /// A handler returned a `decision` this contract cannot parse (rule 5).
    InvalidDecision,
    /// The plugin's own account of what it did - the only authorable kind.
    Policy,
}

impl DiagnosticKind {
    /// The wire spelling (`types/index.ts:4299`, and `persistence.rs` `PLUGIN_DIAGNOSTIC_KINDS`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HandlerThrew => "handler-threw",
            Self::HandlerTimedOut => "handler-timed-out",
            Self::InvalidDecision => "invalid-decision",
            Self::Policy => "policy",
        }
    }

    /// True for the three framework-produced failures. A consumer that only handles
    /// failures reads exactly this set (`types/index.ts:4306`).
    #[must_use]
    pub const fn is_failure(self) -> bool {
        !matches!(self, Self::Policy)
    }
}

impl fmt::Display for DiagnosticKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<DiagnosticKind> for SessionPluginDiagnosticKind {
    fn from(kind: DiagnosticKind) -> Self {
        match kind {
            DiagnosticKind::HandlerThrew => Self::HandlerThrew,
            DiagnosticKind::HandlerTimedOut => Self::HandlerTimedOut,
            DiagnosticKind::InvalidDecision => Self::InvalidDecision,
            DiagnosticKind::Policy => Self::Policy,
        }
    }
}

/// One thing the operator is meant to be able to see after the turn is over
/// (`HarnessDiagnosticEvent`, minus `type`, which this crate's transport stamps itself).
#[derive(Debug, Clone, PartialEq)]
pub struct DiagnosticEvent {
    pub kind: DiagnosticKind,
    /// The plugin the report is ABOUT. The host fills this from the registration
    /// (`host.ts:100`), so a handler that leaves it empty is still attributed - the one
    /// case a wrong owner is impossible.
    pub owner: String,
    /// The lifecycle event whose handler failed, in wire spelling (`turn.before`).
    /// `None` for a `policy` report, which names no hook on purpose.
    pub event: Option<String>,
    /// Self-contained human-readable text, never empty.
    pub message: String,
    /// The structured form of a `policy` report (which units, how many tokens, which unit
    /// superseded which). Absent for a framework failure, which has nothing to structure.
    pub data: Option<Map<String, Value>>,
}

impl DiagnosticEvent {
    /// A framework failure about one handler: the shape the bus reports through
    /// `onHandlerError` (`plugin/hooks.ts` `report`). `event` is REQUIRED here because every
    /// framework diagnostic names its event.
    #[must_use]
    pub fn failure(
        kind: DiagnosticKind,
        owner: impl Into<String>,
        event: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        debug_assert!(kind.is_failure(), "failure() is for the three failure kinds");

        Self {
            kind,
            owner: owner.into(),
            event: Some(event.into()),
            message: message.into(),
            data: None,
        }
    }

    /// A plugin's own decision report (`HarnessPluginDiagnosticReport`, kind pinned to
    /// `policy`). `owner` starts EMPTY: the host stamps it, because the scoped api that
    /// authors this cannot see its own plugin name any other way.
    #[must_use]
    pub fn policy(message: impl Into<String>, data: Option<Map<String, Value>>) -> Self {
        Self {
            kind: DiagnosticKind::Policy,
            owner: String::new(),
            event: None,
            message: message.into(),
            data,
        }
    }

    /// The durable record, stamped with `at` (epoch millis).
    #[must_use]
    pub fn into_record(self, at: u64) -> SessionPluginDiagnostic {
        SessionPluginDiagnostic {
            at,
            kind: self.kind.into(),
            owner: self.owner,
            event: self.event,
            message: self.message,
            data: self.data,
        }
    }
}

/// The durable landing spot - the contract the INTEGRATOR implements over `SessionState`.
///
/// See the header for the reasoning and the wiring example.
pub trait DiagnosticSink: Send {
    /// Appends one already-stamped record. The sink owns the trim
    /// (`normalize_plugin_diagnostics`'s newest-N cap).
    fn note_plugin_diagnostic(&mut self, entry: SessionPluginDiagnostic, at: u64);
}

/// The `&self` handle a handler carries, sync or async.
///
/// It is `Send + Sync` on purpose: the bus stores ONE handle per registration inside the
/// handler closure, which is what lets the context seam's `report_diagnostic(&self, ..)` be
/// called from a `&self` context read AND from inside an `async` handler that has already
/// moved out of the borrow.
pub trait DiagnosticReporter: Send + Sync {
    /// Reports one event. Clock stamping lives in the implementations, so a test reporter
    /// can pin `at`.
    fn report(&self, event: DiagnosticEvent);

    /// The owner stamped on reports from this handle. `''` when the handle is shared across
    /// plugins, in which case the caller fills it per call (see [`stamp_owner`]).
    #[must_use]
    fn owner(&self) -> &str {
        ""
    }
}

impl fmt::Debug for dyn DiagnosticReporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiagnosticReporter")
            .field("owner", &self.owner())
            .finish_non_exhaustive()
    }
}

/// A reporter that FORCES an owner onto every event - the host's half of `host.ts:207-209`
/// (`report: { diagnostic: (report) => base.diagnostic({ ...report, owner: plugin.name }) }`).
///
/// It is a wrapper rather than a parameter because the api a plugin holds is scoped at SETUP time
/// and shared by every hook that plugin registers: attribution has to be a property of the handle,
/// so a plugin cannot forge a report about another plugin, and a `policy` report authored with an
/// empty owner still lands attributed.
#[derive(Debug, Clone)]
pub struct AttributedReporter {
    inner: Arc<dyn DiagnosticReporter>,
    owner: String,
}

impl AttributedReporter {
    #[must_use]
    pub fn new(inner: Arc<dyn DiagnosticReporter>, owner: impl Into<String>) -> Self {
        Self {
            inner,
            owner: owner.into(),
        }
    }
}

impl DiagnosticReporter for AttributedReporter {
    fn report(&self, event: DiagnosticEvent) {
        // An OVERRIDE, not a fill: `{ ...report, owner: plugin.name }` puts the host's name last,
        // so a plugin that names itself in a report is still attributed to the plugin the host
        // loaded. `stamp_owner`'s fill-if-empty rule is the OTHER call site's semantics (a shared
        // bus handle), where an unset owner must not be guessed at.
        let mut event = event;
        event.owner.clone_from(&self.owner);

        self.inner.report(event);
    }

    fn owner(&self) -> &str {
        &self.owner
    }
}

/// Bridges a [`DiagnosticSink`] behind one `Arc<Mutex<..>>`.
///
/// This is the whole integration: wrap the sink once, clone the `Arc` into every plugin
/// runtime. The lock is held only for the single append, and a poisoned sink still records
/// rather than failing the turn.
#[derive(Debug)]
pub struct SinkReporter<S: DiagnosticSink> {
    sink: Arc<Mutex<S>>,
    owner: String,
}

impl<S: DiagnosticSink> Clone for SinkReporter<S> {
    fn clone(&self) -> Self {
        Self {
            sink: Arc::clone(&self.sink),
            owner: self.owner.clone(),
        }
    }
}

impl<S: DiagnosticSink> SinkReporter<S> {
    /// A reporter that attributes nothing itself (the bus-level handle).
    #[must_use]
    pub fn new(sink: S) -> Self {
        Self::attributed(sink, "")
    }

    /// One handle per plugin (`host.ts:100`): the owner is the plugin name, so every report
    /// through this handle is stamped with it unless the event already names an owner.
    #[must_use]
    pub fn attributed(sink: S, owner: impl Into<String>) -> Self {
        Self {
            sink: Arc::new(Mutex::new(sink)),
            owner: owner.into(),
        }
    }

    /// The shared handle, for a caller that wants to read the sink back (a test, or the store).
    #[must_use]
    pub fn handle(&self) -> Arc<Mutex<S>> {
        Arc::clone(&self.sink)
    }

    /// Locks without ever panicking on a poisoned sink.
    fn lock(&self) -> MutexGuard<'_, S> {
        self.sink.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S: DiagnosticSink> DiagnosticReporter for SinkReporter<S> {
    fn report(&self, event: DiagnosticEvent) {
        // ONE clock read per report: the record and the sink's trim window must agree on `at`.
        let at = now_ms();
        let event = stamp_owner(event, &self.owner);
        let mut sink = self.lock();
        sink.note_plugin_diagnostic(event.into_record(at), at);
    }

    fn owner(&self) -> &str {
        &self.owner
    }
}

/// A reporter that keeps every record in memory - what the host gate and the plugin tests
/// assert on.
#[derive(Debug, Default)]
pub struct RecordingDiagnosticSink {
    records: Vec<SessionPluginDiagnostic>,
}

impl RecordingDiagnosticSink {
    /// Every record, oldest first.
    #[must_use]
    pub fn records(&self) -> &[SessionPluginDiagnostic] {
        &self.records
    }

    /// The `message` of every record, oldest first - the usual assertion target.
    #[must_use]
    pub fn messages(&self) -> Vec<String> {
        self.records.iter().map(|entry| entry.message.clone()).collect()
    }

    /// The `kind` of every record, wire spelling, oldest first.
    #[must_use]
    pub fn kinds(&self) -> Vec<&'static str> {
        self.records
            .iter()
            .map(|entry| kind_label(&entry.kind))
            .collect()
    }
}

impl DiagnosticSink for RecordingDiagnosticSink {
    fn note_plugin_diagnostic(&mut self, entry: SessionPluginDiagnostic, _at: u64) {
        self.records.push(entry);
    }
}

/// The wire spelling of a durable kind, for test assertions and log lines.
#[must_use]
pub const fn kind_label(kind: &SessionPluginDiagnosticKind) -> &'static str {
    match kind {
        SessionPluginDiagnosticKind::HandlerThrew => "handler-threw",
        SessionPluginDiagnosticKind::HandlerTimedOut => "handler-timed-out",
        SessionPluginDiagnosticKind::InvalidDecision => "invalid-decision",
        SessionPluginDiagnosticKind::Policy => "policy",
    }
}

/// Wall clock in epoch milliseconds, the unit every `at` field in this crate uses.
///
/// A test that needs exact timestamps injects its own reporter; this clock is only the
/// fallback, so production cannot silently run on a frozen time.
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |delta| delta.as_millis().try_into().unwrap_or(u64::MAX))
}

/// Fills an empty owner with the registration's owner (`host.ts:100`'s `plugin.name` closure).
pub(crate) fn stamp_owner(mut event: DiagnosticEvent, owner: &str) -> DiagnosticEvent {
    if event.owner.is_empty() && !owner.is_empty() {
        event.owner = owner.to_owned();
    }

    event
}

/// Builds the `data` object the shipped plugins attach to a settings write.
///
/// `settings_written` is `{ plugin, key, from, to }`; keeping the builder here means the two
/// settings-writing plugins cannot drift apart on key spelling.
#[must_use]
pub fn settings_written_data(
    plugin: &str,
    key: &str,
    from: Option<Value>,
    to: Option<Value>,
) -> Map<String, Value> {
    let mut data = Map::new();
    data.insert("plugin".to_owned(), Value::String(plugin.to_owned()));
    data.insert("key".to_owned(), Value::String(key.to_owned()));
    data.insert("from", from.unwrap_or(Value::Null));
    data.insert("to", to.unwrap_or(Value::Null));

    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_four_wire_spellings_are_the_closed_set() {
        // `persistence.rs PLUGIN_DIAGNOSTIC_KINDS` accepts exactly these four, in this order.
        assert_eq!(DiagnosticKind::HandlerThrew.as_str(), "handler-threw");
        assert_eq!(DiagnosticKind::HandlerTimedOut.as_str(), "handler-timed-out");
        assert_eq!(DiagnosticKind::InvalidDecision.as_str(), "invalid-decision");
        assert_eq!(DiagnosticKind::Policy.as_str(), "policy");
        assert!(!DiagnosticKind::Policy.is_failure());
    }

    #[test]
    fn a_policy_report_names_no_event_and_an_empty_owner_until_the_host_stamps_it() {
        let report = DiagnosticEvent::policy("withheld 2 units".to_owned(), None);

        assert_eq!(report.owner, "");
        assert_eq!(report.event, None);
        assert_eq!(report.kind, DiagnosticKind::Policy);

        // `host.ts:100` - the scoped api cannot see its own name, so attribution is the
        // host's job and happens exactly once, at the handle.
        let stamped = stamp_owner(report, "context-trimmer");
        assert_eq!(stamped.owner, "context-trimmer");

        let record = stamped.into_record(1_700_000_000_000);
        assert_eq!(record.kind, SessionPluginDiagnosticKind::Policy);
        assert_eq!(record.event, None);
        assert_eq!(record.at, 1_700_000_000_000);
    }

    #[test]
    fn a_framework_failure_always_names_its_event() {
        let event = DiagnosticEvent::failure(
            DiagnosticKind::HandlerTimedOut,
            "slow-policy",
            "turn.before",
            "timed out",
        );

        assert_eq!(event.event.as_deref(), Some("turn.before"));
        // An explicit owner is never overwritten by the handle's owner.
        assert_eq!(stamp_owner(event, "other").owner, "slow-policy");
    }

    #[test]
    fn a_recording_sink_keeps_records_in_order() {
        let mut sink = RecordingDiagnosticSink::default();
        sink.note_plugin_diagnostic(
            DiagnosticEvent::policy("first".to_owned(), None).into_record(1),
            1,
        );
        sink.note_plugin_diagnostic(
            DiagnosticEvent::failure(DiagnosticKind::HandlerThrew, "a", "turn.after", "boom")
                .into_record(2),
            2,
        );

        assert_eq!(
            sink.kinds(),
            vec!["policy", "handler-threw"],
            "kinds() is the assertion target the plugin tests use"
        );
        assert_eq!(sink.messages(), vec!["first", "boom"]);
    }
}
