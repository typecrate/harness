//! The plugin layer: the lifecycle vocabulary, the hook bus, the plugin host, the seam a plugin
//! acts through, and the settings/diagnostic planes both halves share.
//!
//! TypeScript origin: `packages/underload/harness/src/plugin/` - `hooks.ts` (the bus), `host.ts`
//! (the host), `settings.ts` (the plugin-facing settings view), `manifest.ts` (the built-in
//! catalogue, ported as [`crate::plugins`]), plus the `types/index.ts` lifecycle surfaces. One
//! Rust module per TS file, with the module header of each naming the lines it ports.
//!
//! THE LAYERS, AND WHY THEY POINT THIS WAY
//! ---------------------------------------
//! ```text
//!   event.rs        vocabulary - zero dependencies (the payloads are `contract` types)
//!      ^
//!   context.rs      THE SEAM - the traits the run loop implements, the plugins consume
//!      ^
//!   hooks.rs        the bus - rules 1-9, dispatch, budget, gate, reducer
//!      ^
//!   host.rs         the host - resolve/replace-in-place, scoped api, the enablement gate
//!      ^
//!   *.rs (plugins)  the shipped policies - async-ping, context-trimmer, context-compaction
//! ```
//!
//! `diagnostics.rs` and `settings.rs` sit beside that stack: they are the two planes the host wires
//! INTO a plugin (an attributed reporter, a scoped settings view) and the two the seam writes
//! THROUGH (a store sink, a settings provider). Nothing here names `SessionStore` - the durable
//! landing spot of a diagnostic and of a settings write is a trait this layer owns, so the
//! dependency direction never inverts (see each file's header).
//!
//! THE THREE THINGS A CALLER MUST NOT CONFUSE
//! ------------------------------------------
//!   * [`event::LifecycleEvent`] is the CATALOGUE; `'*'` is a subscription FORM
//!     ([`event::SubscriptionTarget`]) and is never an event, so `has('*')` is false by
//!     construction.
//!   * [`hooks::HookBus`] is mechanism: order, gate, budget, fail-open, the round reducer.
//!     [`host::PluginHost`] is policy: which plugin exists, which name replaced which, and which
//!     settings key silences it.
//!   * [`context::TurnControls`] is ROUND state and [`context::PluginControls`] is PLUGIN state.
//!     The distinction is what makes a disabled plugin free (rule 9) while a replaced transcript
//!     stays visible to the next handler (rule 1).
//!
//! THE GATE IS NOT UNREGISTRATION (the one rule most likely to be "fixed" wrongly)
//! -------------------------------------------------------------------------------
//! Flipping the reserved `enabled` key off stops DELIVERY for every gated event, keeps the plugin
//! loaded and listed, keeps [`hooks::HookBus::live`] reporting its subscription, and still delivers
//! `harness.dispose` so the plugin can release its own timers. See
//! [`hooks::Registration::enabled`] and the pins in `tests/plugin_host.rs`.

// The three shipped policies are written against this layer, not inside it; they are declared
// here once their modules compile (each one is a `Plugin` impl over the seam).
pub mod diagnostics;
pub mod event;
pub mod host;
pub mod hooks;
pub mod settings;

pub use context::{
    ContextEntry, ControlError, EditPatch, EntryTarget, FallbackControls, NullRuntime,
    PluginControls, PluginRuntime, SendFuture, TurnControls, declared_keys, fallback_controls,
    resolved_value, written_data,
};
pub use diagnostics::{
    DiagnosticEvent, DiagnosticKind, DiagnosticReporter, DiagnosticSink, RecordingDiagnosticSink,
    SinkReporter, kind_label, now_ms, settings_written_data,
};
pub use event::{
    ALWAYS_DELIVERED, AsyncLifecycle, Contribution, LifecycleEvent, PendingTool, Payload,
    RunLifecycle, SubscriptionTarget, ToolAfterPayload, ToolBeforePayload, TurnAfterPayload,
    TurnBeforePayload, TurnDecision, WILDCARD, decision_of_value, event_of_name,
    is_wildcard_deliverable, subscription_of_name,
};
pub use host::{
    HostError, Plugin, PluginApi, PluginHost, PluginHostReport, ResolvedPlugin, resolve_plugin_list,
};
pub use hooks::{
    HandlerBody, HandlerCall, HandlerFailure, HandlerFuture, HandlerResult, HookBus, HookOptions,
    MaybeSync, ReporterFailureSink, RoundFuture, budget_error_of, decision_error_of,
    // The bus's own registration type, aliased so `plugin::Registration` never shadows
    // `plugins::Registration`-style names in a caller that globs both.
    Registration as HookRegistration, RegistrationError,
};
pub use settings::{MapSettingsProvider, SettingsProvider, SettingsView};
