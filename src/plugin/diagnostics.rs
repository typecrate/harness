// =============================================================================
// plugin/diagnostics.rs - the diagnostic plane W2-B2 adds to the harness.
//
// WHY this file exists at all: the shipped plugins report through
// `api.context.reportDiagnostic({ kind, owner, event, message, data })`, and the JS original hands
// that straight to a live `context` object. This crate's context view is a SEAM TRAIT, and the
// durable landing spot for a plugin diagnostic is `SessionState.pluginDiagnostics` - which today
// has NO write path at all (this is the "store-side sink" the integrator owes the host, see
// `SessionPluginDiagnostic` in src/contract.rs and `MAX_PLUGIN_DIAGNOSTICS` in src/persistence.rs).
// So the host cannot name `SessionStore` (layering: plugin -> store would invert every other edge
// in the crate) and instead names THIS contract, which the integrator implements over the store in
// about five lines:
//
//     impl DiagnosticSink for MyStoreSink {                  // MyStoreSink wraps the store
//         fn note_plugin_diagnostic(&mut self, entry: SessionPluginDiagnostic, at: u64) {
//             self.store.note_plugin_diagnostic(entry, at);   // the store method W2-B2 needs
//         }
//     }
//
// The method NAME and SHAPE are chosen to be exactly the store method the integrator was already
// told to add - `&mut self` because the state document mutates, `at` passed IN rather than read
// from a clock because a persistence sink must be deterministic under test (the reporter stamps).
//
// THE THREE PIECES
//   1. `DiagnosticEvent`  - what a handler reports: kind + owner + event + message + optional data.
//      `owner` is NOT part of the JS payload (JS closes over `plugin.name`); it is explicit here so
//      the impl CANNOT mis-stamp it, and the host fills it from the registration when a handler
//      leaves it empty (`Contribution`-style fail-safe, never a silent wrong owner).
//   2. `DiagnosticSink`   - the durable landing spot, above.
//   3. `DiagnosticReporter` - the SHARED, `&self` object every registered handler carries. It is
//      `Clone + Send + Sync`, so it lives inside the handler closure, which is what makes the
//      context seam's `report_diagnostic(&self, ..)` callable from a `&self` context read AND from
//      inside an async handler that has already moved out of the borrow.
//
// BEHAVIOUR THAT MUST NOT DRIFT
//   - One diagnostic per handler round, never a flush loop (`host.ts:105-107`): the ROUND gate here
//     keeps the first event and drops the rest with a loud test-only log. `at` is stamped on the
//     first event of the round, so the record order matches the handler's own order.
//   - Sink failures never travel back into a turn. A sink that panics is contained by the
//     `Mutex` poison recovery (`unwrap_or_else(PoisonError::into_inner)`), and a sink is NEVER
//     called while a lock the plugin may re-enter is held.
//   - The bound is the store's (`MAX_PLUGIN_DIAGNOSTICS`), not the reporter's - the reporter emits
//     in order and lets the sink trim, so a restart keeps the newest records exactly as TS does.
// =============================================================================

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde_json::{Map, Value};

use crate::contract::SessionPluginDiagnostic;

/// The highest `kind` a diagnostic may carry that this crate recognises.
///
/// Mirrors `PLUGIN_DIAGNOSTIC_KINDS` in src/persistence.rs; kept as a local constant (rather than a
/// re-export) so the plugin layer never has to reach into the persistence module for a label.
pub const KNOWN_DIAGNOSTIC_KINDS: [&str; 5] = [
    "settings",
    "guardrail",
    "execution",
    "prune",
    "failure",
];

/// One thing a plugin wants the operator to be able to see after the turn is over.
#[derive(Clone, Debug, PartialEq)]
pub struct DiagnosticEvent {
    /// What happened: `settings` / `guardrail` / `execution` / `prune` / `failure`. A free string on
    /// purpose, exactly as in TS - the persistence layer normalises anything else to `failure`.
    pub kind: String,
    /// The plugin that reported it. A handler may leave this EMPTY and the host fills in the
    /// registered owner; a host-less reporter (a direct test) keeps whatever it was given.
    pub owner: String,
    /// The lifecycle event the report happened on, `''` when the report is not tied to one.
    pub event: String,
    pub message: String,
    /// Provider-visible detail. `None` and `Value::Null` are the same thing to the wire; the store
    /// keeps only plain objects (src/contract.rs `data`).
    pub data: Option<Value>,
}

impl DiagnosticEvent {
    /// A plain `failure` report - the shape every shipped plugin uses for "I could not do it".
    #[must_use]
    pub fn failure(owner: &str, event: &str, message: impl Into<String>) -> Self {
        Self {
            kind: "failure".to_owned(),
            owner: owner.to_owned(),
            event: event.to_owned(),
            message: message.into(),
            data: None,
        }
    }

    /// The durable record, stamped with `at`.
    #[must_use]
    pub fn into_record(self, at: u64) -> SessionPluginDiagnostic {
        SessionPluginDiagnostic {
            at,
            kind: self.kind,
            owner: self.owner,
            event: self.event,
            message: self.message,
            data: self.data,
        }
    }
}

/// The durable landing spot - the contract the INTEGRATOR implements over `SessionState`.
///
/// See the module header for the reasoning and the wiring example.
pub trait DiagnosticSink: Send {
    /// Appends one already-stamped record. The sink owns the trim
    /// (`normalize_plugin_diagnostics`'s newest-N cap).
    fn note_plugin_diagnostic(&mut self, entry: SessionPluginDiagnostic, at: u64);
}

/// The `&self` handle a handler (sync or async) reports through.
pub trait DiagnosticReporter: Send + Sync {
    /// Reports one event. The default clock stamping lives in the implementations, not here, so a
    /// test reporter can pin `at`.
    fn report(&self, event: DiagnosticEvent);

    /// The owner stamped on reports from this handle. `''` when the handle is shared across plugins
    /// (the bus keeps ONE handle for every registration, so an unset owner is filled per call).
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

/// Bridges a [`DiagnosticSink`] behind one `Arc<Mutex<..>>`.
///
/// This is the whole integration: wrap the sink once, clone the `Arc` into every plugin runtime.
/// The lock is taken only for the single append, and a poisoned sink still records rather than
/// failing the turn (a diagnostic is an observer, never an authority).
#[derive(Debug)]
pub struct SinkReporter<S: DiagnosticSink> {
    sink: Arc<Mutex<S>>,
}

impl<S: DiagnosticSink> Clone for SinkReporter<S> {
    fn clone(&self) -> Self {
        Self {
            sink: Arc::clone(&self.sink),
        }
    }
}

impl<S: DiagnosticSink> SinkReporter<S> {
    #[must_use]
    pub fn new(sink: S) -> Self {
        Self {
            sink: Arc::new(Mutex::new(sink)),
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
        let mut sink = self.lock();
        sink.note_plugin_diagnostic(event.into_record(at), at);
    }
}

/// A reporter that keeps every record in memory - what the host gate and the plugin tests assert on.
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

    /// The `kind` of every record, oldest first.
    #[must_use]
    pub fn kinds(&self) -> Vec<String> {
        self.records.iter().map(|entry| entry.kind.clone()).collect()
    }
}

impl DiagnosticSink for RecordingDiagnosticSink {
    fn note_plugin_diagnostic(&mut self, entry: SessionPluginDiagnostic, _at: u64) {
        self.records.push(entry);
    }
}

impl DiagnosticReporter for Arc<Mutex<RecordingDiagnosticSink>> {
    fn report(&self, event: DiagnosticEvent) {
        let at = now_ms();
        let mut sink = self.lock().unwrap_or_else(PoisonError::into_inner);
        sink.records.push(event.into_record(at));
    }

    fn owner(&self) -> &str {
        ""
    }
}

/// Wall clock in epoch milliseconds, the unit every `at` field in this crate uses.
///
/// A test that needs exact timestamps injects its own reporter; this clock is only the fallback so
/// production cannot silently run on a frozen time.
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |delta| delta.as_millis().try_into().unwrap_or(u64::MAX))
}

/// Fills an empty owner with the registration's owner (`host.ts:100`'s `plugin.name` closure).
pub(crate) fn stamp_owner(mut event: DiagnosticEvent, owner: &str) -> DiagnosticEvent {
    if event.owner.is_empty() {
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
) -> Value {
    let mut data = Map::new();
    data.insert("plugin".to_owned(), Value::String(plugin.to_owned()));
    data.insert("key".to_owned(), Value::String(key.to_owned()));
    data.insert("from", from.unwrap_or(Value::Null));
    data.insert("to", to.unwrap_or(Value::Null));

    Value::Object(data)
}
