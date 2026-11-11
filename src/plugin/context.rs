// =============================================================================
// plugin/context.rs - the SEAM: the live surfaces a plugin handler acts through.
//
// TypeScript origin:
//   * `packages/underload/harness/src/types/index.ts:4421-4680` -
//     `HarnessTurnBeforeContext`: `entries`, `setIncluded`, `setReasoningIncluded`, `edit`,
//     `setTranscript`, `replaceHistory`, `pause`, `resume`, `currentMessages`,
//     `currentEstimatedTokens` -> [`TurnControls`];
//   * `src/types/index.ts:4801-4900` - `HarnessPluginApi`: `settings`, `on`, `report.diagnostic`,
//     `threads.*`, `agents.*`, `async.*`, split by OWNERSHIP below into [`PluginControls`] (the
//     per-plugin slice) and [`PluginRuntime`] (the shared slice);
//   * `src/plugin/host.ts:174-230` - `runPlugins`, which is why the api a handler holds is SCOPED
//     to its own plugin: settings reads/writes and diagnostic attribution both name the
//     registration, never the round.
//
// WHY TRAITS AND NOT STRUCTS
// --------------------------
// Everything here is LIVE: `entries` re-reads the transcript, `set_included` writes through the
// host into canonical state (and notifies persistence, which is the ONLY durability mechanism for
// a flag write), `pause` mutates the thread record and emits `thread.paused`, and `api.settings`
// is re-read on every call so an operator's mid-session flip reaches a running session. A plain
// data struct would freeze answers the contract promises to recompute, so the host implements
// these traits and the plugin layer only ever calls through them.
//
// THIS IS THE CONTRACT THE RUN-LOOP INTEGRATOR IMPLEMENTS. Nothing in this crate implements it
// yet: the writer half (canonical inclusion flags, the owned park, the context-lane read) lands
// with the run-loop wave that owns `src/store/*` and `src/api/turns.rs`. The method NAMES and
// SHAPES are chosen to match the surface that store layer already exposes, so the implementation
// is an adapter, not a redesign.
//
// THE THREE-LIFE SPLIT OF ONE TS OBJECT
// -------------------------------------
// TS hands a `turn.before` handler ONE object that is simultaneously the per-plugin api
// (`payload.api`), the shared harness handle (`payload.api.threads`), and the per-round controls
// (`payload.setIncluded`). They have three different lifetimes:
//   * [`PluginRuntime`] - process-scoped, shared by every plugin (the harness handle: threads,
//     agents, the async board).
//   * [`PluginControls`] - process-scoped, per-PLUGIN (scoped settings + attributed diagnostics),
//     so the enablement gate can close over it and a handler can hold it across an `await`.
//   * [`TurnControls`] - ROUND-scoped. The run loop builds it and hands it to
//     `HookBus::emit_with`, which is where TS's payload factory runs
//     ("the factory is called ONCE PER HANDLER, so a handler that runs after another handler has
//     already rewritten something observes the CURRENT value", `hooks.ts:232-239`). Because a Rust
//     `TurnControls` re-reads on every call rather than caching a build, ONE object per round is
//     behaviourally identical to one per handler - a later hook still sees what an earlier one
//     wrote, which is the invariant the factory exists to buy.
//
// WHAT IS DELIBERATELY NOT ON THE SEAM
// ------------------------------------
// `agents.createAgent`, `tools.addTool`, `addCommand`, `threads.create/delete/stop/steer` and the
// provider-anchored token estimator are not here: no shipped plugin uses them, and this crate's
// `src/tools/*` + `src/agents/*` surfaces own those operations today. When a plugin needs one, the
// seam grows a method - it is not a licence to reach around it.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::FutureExt as _;
use serde_json::{Map, Value};

use crate::contract::HarnessMessage;
use crate::plugins::metadata_of;

use super::diagnostics::DiagnosticEvent;
use super::event::PendingTool;
use super::settings::{MapSettingsProvider, SettingsProvider, SettingsView};

// ---------------------------------------------------------------------------
// Targets and patches
// ---------------------------------------------------------------------------

/// What a context write may be pointed at (`setIncluded`'s three accepted targets,
/// `types/index.ts:4520-4529`).
///
/// TS also accepts an ENTRY object captured from `entries`, resolved by message identity; in Rust
/// an entry is a value, so the durable address is spelled [`EntryTarget::MessageId`] and the index
/// [`EntryTarget::Index`]. The cascade rule is identical for both: a target names a UNIT, never one
/// message, so a request never ends up with a half-answered `tool_calls[]` or an orphan result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryTarget {
    /// The canonical index, interpreted against the CURRENT transcript.
    Index(usize),
    /// THE DURABLE ADDRESS (`messageId`): unlike the index it survives a history rewrite, a
    /// snapshot/restore, and a whole run, so a candidate named in one round re-resolves in a later
    /// one. An id no longer in the transcript is refused, not silently skipped.
    MessageId(String),
}

impl EntryTarget {
    /// The address a diagnostic names: the id when there is one, else the index.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Index(index) => index.to_string(),
            Self::MessageId(id) => id.clone(),
        }
    }
}

/// The text fields one entry may change (`HarnessContextEditPatch`, `types/index.ts:4411-4416`).
///
/// Both are optional and a field the patch does not name is left EXACTLY as it was: `edit` never
/// clears a field it is silent about. `reasoning` exists on the assistant variant only, and the
/// runtime refuses it for any other role rather than writing a key the snapshot reader would then
/// refuse to load.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EditPatch {
    /// The model-facing content of every role.
    pub content: Option<String>,
    /// The assistant's thinking text (kept out of the provider payload).
    pub reasoning: Option<String>,
}

impl EditPatch {
    #[must_use]
    pub fn content(text: impl Into<String>) -> Self {
        Self {
            content: Some(text.into()),
            reasoning: None,
        }
    }

    #[must_use]
    pub fn reasoning(text: impl Into<String>) -> Self {
        Self {
            content: None,
            reasoning: Some(text.into()),
        }
    }
}

/// One entry of the CANONICAL transcript, as `turn.before` exposes it for context control
/// (`HarnessContextEntry`, `types/index.ts:4360-4400`).
///
/// Unlike the projected message list, this is the committed history ITSELF: an entry that is
/// disabled is still listed, because context control never DELETES a turn - it hides it from the
/// model, reversibly. A plugin that only saw the projection could not re-enable anything, since the
/// entries it would need are exactly the ones the projection left out.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextEntry {
    /// Index in the canonical transcript at the moment THIS entry list was built. The index is a
    /// snapshot, so a handler that calls `replace_history` invalidates the indices of every entry
    /// captured BEFORE that call (`types/index.ts:4363-4366`).
    pub index: usize,
    /// The canonical message itself - read-only by contract: an inclusion change goes through
    /// `set_included`, which cascades the unit and notifies persistence.
    pub message: HarnessMessage,
    /// EFFECTIVE unit inclusion: `false` when this entry, OR ANY OTHER MEMBER of its unit, is
    /// disabled. A live view of canonical state, not a snapshot.
    pub included: bool,
    /// Whether this entry's THINKING travels in the request (`reasoningIncluded`). The second axis,
    /// and NOT cascaded: a thought belongs to one assistant entry, not to the unit.
    pub reasoning_included: bool,
    /// The estimator's cost of THIS entry alone. The planners sum these to report "tokens saved",
    /// and the estimator does NOT count reasoning - which is exactly why the trimmer measures
    /// thinking savings in characters instead (`context-trimmer.plugin.ts`, `minThoughtChars`).
    pub estimated_tokens: f64,
}

impl ContextEntry {
    /// The durable address, hoisted (`messageId`, `types/index.ts:4372-4379`).
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.message.message_id()
    }

    /// The write target this entry names: the durable address when it has one, else the snapshot
    /// index.
    #[must_use]
    pub fn target(&self) -> EntryTarget {
        self.message_id()
            .map(|id| EntryTarget::MessageId(id.to_owned()))
            .unwrap_or(EntryTarget::Index(self.index))
    }

    /// The view the pure pruning rules read (`crate::core_pruning::ContextEntry`), so the shipped
    /// planners need no adapter of their own.
    #[must_use]
    pub fn as_prune_entry(&self) -> crate::core_pruning::ContextEntry {
        crate::core_pruning::ContextEntry {
            index: self.index,
            message: self.message.clone(),
            included: self.included,
        }
    }
}

/// A refusal from the seam: the target is stale, the value is the wrong type, or the runtime could
/// not do it. The message is operator-facing, so it names the thing.
#[derive(Debug, Clone, PartialEq)]
pub struct ControlError(pub String);

impl ControlError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ControlError {}

/// One host round trip that can suspend (`api.threads.send`, compaction's summarizer, async-ping's
/// wake). Boxed because a trait method cannot be `async` and the caller owns the future.
pub type SendFuture = Pin<Box<dyn Future<Output = Result<HarnessMessage, ControlError>> + Send>>;

// ---------------------------------------------------------------------------
// The round-scoped surface (`HarnessTurnBeforeContext`)
// ---------------------------------------------------------------------------

/// The live control surface a `turn.before` handler acts through.
///
/// Built by the RUN LOOP and handed to [`super::hooks::HookBus::emit_with`]. See the module header
/// for why one object per round is the faithful Rust shape of TS's per-handler payload factory.
pub trait TurnControls: Send + Sync {
    /// The conversation this round belongs to (`threadId`), for the log line and the cooldown key.
    fn thread_id(&self) -> &str;

    /// The FULL canonical transcript, in canonical order, INCLUDING disabled entries
    /// (`types/index.ts:4474-4479`).
    fn entries(&self) -> Vec<ContextEntry>;

    /// The projection as it is NOW - equal to the entry-time `messages` until somebody writes, and
    /// afterwards the list the provider will actually receive (`types/index.ts:4454-4464`).
    fn current_messages(&self) -> Vec<HarnessMessage>;

    /// The entry-time estimate re-evaluated against the CURRENT projection, so a compactor can
    /// measure what ITS OWN prune just saved inside the same handler (`types/index.ts:4465-4473`).
    /// Counted by the same estimator over the same leading instruction blocks, so the two numbers
    /// agree whenever nothing has been written.
    fn current_estimated_tokens(&self) -> f64;

    /// Switch one transcript entry - or, when it belongs to a tool-call block, the WHOLE unit - off
    /// or on for the model (`types/index.ts:4480-4538`).
    ///
    /// `false` stores `included: false`; `true` REMOVES the key, because only `false` is ever
    /// stored: an unflagged transcript stays byte-identical and re-enabling is the exact inverse of
    /// disabling. The write lands on canonical state and is reported as a transcript change - that
    /// notification is the only durability mechanism.
    fn set_included(&self, target: &EntryTarget, included: bool) -> Result<(), ControlError>;

    /// The thinking twin: one per-entry FLAG, never an edit of the text. The entry keeps travelling
    /// with its content and tool calls intact and only its request copy drops `reasoning`, so
    /// provider pairing is untouched and canonical history keeps every thought
    /// (`types/index.ts:4540-4580`). Withhold-only in practice: the shipped trimmer never writes
    /// `true`, so an operator switches it back on from the context view instead.
    fn set_reasoning_included(
        &self,
        target: &EntryTarget,
        included: bool,
    ) -> Result<(), ControlError>;

    /// Change one entry's TEXT (the summarizer's rewrite; `types/index.ts:4580-4609`).
    fn edit(&self, target: &EntryTarget, patch: &EditPatch) -> Result<(), ControlError>;

    /// Replace the transcript for THIS REQUEST ONLY (compaction's main move). Does NOT touch
    /// committed history: the next turn projects the real transcript again. Last non-empty write
    /// wins (`types/index.ts:4661-4665`).
    fn set_transcript(&self, messages: &[HarnessMessage]) -> Result<(), ControlError>;

    /// Rewrite the conversation's COMMITTED transcript (`types/index.ts:4666-4670`). Use when the
    /// history itself is too big, not just this request. Refuses a transcript the snapshot reader
    /// would reject.
    fn replace_history(&self, messages: &[HarnessMessage]) -> Result<(), ControlError>;

    /// Park this conversation at the turn boundary (`types/index.ts:4671-4680`): the thread record
    /// goes to `paused` and `thread.paused` fires.
    ///
    /// THE OWNED PARK (compaction T11/D1): the park is attributed to `owner`, so only the same
    /// owner's `resume` lifts it and another plugin's `resume` in the SAME round cannot. Parking a
    /// conversation somebody else already parked is a no-op that does NOT transfer ownership.
    fn pause(&self, owner: &str) -> Result<(), ControlError>;

    /// Undo [`Self::pause`]: back to `running` when a run is still attached (the normal case), to
    /// `idle` otherwise, emitting `thread.resumed`. A no-op when nothing is parked, and a no-op
    /// when it is parked BY ANOTHER PLUGIN.
    fn resume(&self, owner: &str) -> Result<(), ControlError>;

    /// Whether this round has a durable conversation to write to (`hasPersistentThread`,
    /// `context-compaction.plugin.ts:961-962`): a branch run or a threadless `writer('go')` turn
    /// has an ephemeral transcript, so `replace_history` refuses it and a park cannot be lifted.
    fn has_persistent_thread(&self) -> bool;
}

// ---------------------------------------------------------------------------
// The shared harness handle (`HarnessPluginApi`, the ownerless slice)
// ---------------------------------------------------------------------------

/// The harness operations every plugin shares (`api.threads.*`, `api.agents.*`, `api.async.*`).
///
/// Ownerless by design: these are the SAME handle methods a host calls, so a plugin cannot reach a
/// private path with different rules (`types/index.ts:4838-4840`). The integrator implements this
/// over the store + run loop.
pub trait PluginRuntime: Send + Sync {
    /// The model id one agent would use (`api.agents.get(agent)?.model`), which is how compaction
    /// resolves its per-model limit. `None` = no such agent, which the caller reads as "fall back
    /// to the base limit".
    fn agent_model(&self, agent: &str) -> Option<String>;

    /// The pending background calls right now (`api.async.pending()`), the reason async-ping arms
    /// its interval instead of guessing.
    fn async_pending(&self) -> Vec<PendingTool>;

    /// `api.async.pending(agent)` - the same board read THROUGH an identity filter. async-ping
    /// sweeps by scheduler key, so it needs the filtered answer; the default of "ignore the filter"
    /// is safe because an unfiltered board is a superset, and every caller treats empty as "stop".
    fn async_pending_for(&self, agent: Option<&str>) -> Vec<PendingTool> {
        let _ = agent;
        self.async_pending()
    }

    /// `api.async.wake(agent)`: emit the `wakeup` frame for that identity, which the host routes
    /// through the agent's mailbox (and which also gives the run its inspecting turn). The default
    /// no-op is the honest answer of a host with no mailbox wired: the heartbeat fires, finds the
    /// board still pending, and re-arms, exactly as a wake into a host that discards it.
    fn async_wake(&self, agent: Option<&str>) {
        let _ = agent;
    }

    /// `api.config.wakeIntervalMs`: the host-level spelling of the same background-work heartbeat
    /// policy, consulted only when the plugin's provider stores nothing AND declares nothing.
    fn wake_interval_ms(&self) -> Option<f64> {
        None
    }

    /// The context lane a conversation is on: the durable "the next request starts at this
    /// boundary" stamp compaction writes after a range lands (`api.threads.getContextLane`). `None`
    /// = lane 0, which this crate spells as an absent lane (see `contract.rs:3035`, "absent means
    /// lane 0").
    fn context_lane(&self, thread_id: &str) -> Option<u64>;

    /// Moves a conversation's context lane.
    fn set_context_lane(&self, thread_id: &str, lane: u64);

    /// Whether a conversation exists and is still addressable (a deleted thread answers `false`, so
    /// a cooldown entry for it is dropped rather than replayed).
    fn thread_exists(&self, thread_id: &str) -> bool;

    /// Wakes an idle/parked conversation with a message and runs one turn (`api.threads.send`):
    /// async-ping's wake, and compaction's summarizer round trip.
    fn thread_send(&self, thread_id: &str, input: &str) -> SendFuture;
}

// ---------------------------------------------------------------------------
// The plugin-scoped surface (`HarnessPluginApi`, the per-plugin slice)
// ---------------------------------------------------------------------------

/// The plugin-scoped api a registration carries (`entry.api`, built by `host.ts`'s
/// `createPluginSettingsView` + the attributed `report`).
///
/// It is `Send + Sync` and round-independent, so a handler can hold it across an `await` and the
/// enablement gate can close over it. Every read is live by contract: that is what lets an operator
/// flip a switch from the UI mid-session and have it take effect on the next turn.
pub trait PluginControls: Send + Sync {
    /// The plugin this api belongs to (`plugin.name`). `''` is the fallback api - a host-level
    /// subscription with no plugin behind it.
    fn owner(&self) -> &str;

    /// This plugin's settings, scoped to its own name and declared defaults (`api.settings`).
    fn settings(&self) -> Option<SettingsView<'_>>;

    /// The provider behind [`Self::settings`] - what the enablement gate closes over.
    fn settings_provider(&self) -> Option<Arc<dyn SettingsProvider>>;

    /// `api.report.diagnostic`, ATTRIBUTED: the host stamps the owner with the plugin's own name
    /// (`host.ts:207-209`), so a plugin cannot forge a report about another plugin.
    fn report_diagnostic(&self, event: DiagnosticEvent);

    /// The shared harness handle behind the scoped api.
    fn runtime(&self) -> &dyn PluginRuntime;
}

// ---------------------------------------------------------------------------
// The fallback api (`hooks.ts:165`'s optional `entry.api`)
// ---------------------------------------------------------------------------

/// A harness handle with nothing behind it - what a Rust build with no run loop integrated yet
/// (and every unit test) uses. Every operation answers "no facts", which is exactly how a plugin
/// behaves when the host cannot answer: compaction falls back to the base limit, async-ping sees no
/// pending work, and a send is refused rather than silently dropped.
#[derive(Debug, Clone, Default)]
pub struct NullRuntime;

impl PluginRuntime for NullRuntime {
    fn agent_model(&self, _agent: &str) -> Option<String> {
        None
    }

    fn async_pending(&self) -> Vec<PendingTool> {
        Vec::new()
    }

    fn context_lane(&self, _thread_id: &str) -> Option<u64> {
        None
    }

    fn set_context_lane(&self, _thread_id: &str, _lane: u64) {}

    fn thread_exists(&self, _thread_id: &str) -> bool {
        false
    }

    fn thread_send(&self, thread_id: &str, _input: &str) -> SendFuture {
        let thread_id = thread_id.to_owned();

        async move {
            Err(ControlError::new(format!(
                "This build has no run loop integrated, so conversation \"{thread_id}\" cannot be sent to."
            )))
        }
        .boxed()
    }
}

/// The read-only nothing handed to a registration that named no plugin.
///
/// TS's `entry.api` is OPTIONAL (`hooks.ts:165`): a host `on` has no settings view, and a handler
/// that reads settings through it reads the plugin's declared defaults and nothing stored. This
/// type is that shape: an empty store scoped to `''`, so every read resolves to whatever the caller
/// passes as its own fallback.
#[derive(Debug, Clone)]
pub struct FallbackControls {
    provider: Arc<dyn SettingsProvider>,
    runtime: Arc<dyn PluginRuntime>,
}

impl FallbackControls {
    #[must_use]
    pub fn new() -> Self {
        Self {
            provider: Arc::new(MapSettingsProvider::with_defaults("")),
            runtime: Arc::new(NullRuntime),
        }
    }
}

impl Default for FallbackControls {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginControls for FallbackControls {
    fn owner(&self) -> &str {
        ""
    }

    fn settings(&self) -> Option<SettingsView<'_>> {
        Some(SettingsView::live(self.provider.as_ref()))
    }

    fn settings_provider(&self) -> Option<Arc<dyn SettingsProvider>> {
        Some(Arc::clone(&self.provider))
    }

    fn report_diagnostic(&self, _event: DiagnosticEvent) {
        // A host-level subscription has no attributed sink of its own; the BUS reporter is the one
        // that sees its failures (rule 8), so there is nothing to drop here.
    }

    fn runtime(&self) -> &dyn PluginRuntime {
        self.runtime.as_ref()
    }
}

/// The api for a registration that named no plugin (`hooks.ts:165`'s optional `entry.api`).
#[must_use]
pub fn fallback_controls() -> Arc<dyn PluginControls> {
    Arc::new(FallbackControls::new())
}

// ---------------------------------------------------------------------------
// Debug views for the three erased seams
// ---------------------------------------------------------------------------
//
// The seam traits are implemented OUTSIDE this crate (by the run-loop integrator, over the store),
// so requiring `Debug` on them would push a logging obligation onto types that may hold sockets and
// store handles. Printing the identity each one already carries is enough to debug a dispatch, and
// it keeps `Arc<dyn ..>` usable inside a `#[derive(Debug)]` container - the same shape
// `diagnostics.rs` uses for `dyn DiagnosticReporter`.

impl std::fmt::Debug for dyn PluginControls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginControls")
            .field("owner", &self.owner())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for dyn TurnControls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnControls")
            .field("thread_id", &self.thread_id())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for dyn PluginRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRuntime").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for dyn SettingsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingsProvider")
            .field("owner", &self.owner())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Catalogue helpers the shipped plugins share
// ---------------------------------------------------------------------------

/// Every key one plugin may read: its declared settings, plus the reserved `enabled` key the
/// framework synthesises when the plugin never declared it (`effective_settings_of`).
///
/// The trimmer and the compactor both read this to decide whether a stored value is even
/// meaningful, and the reserved key is what makes `enabled` readable on a plugin that never
/// declared it (`host.test.ts:415-440`).
#[must_use]
pub fn declared_keys(plugin: &str) -> Vec<&'static str> {
    if metadata_of(plugin).is_none() {
        return Vec::new();
    }

    crate::plugins::effective_settings_of(plugin)
        .into_iter()
        .map(|descriptor| descriptor.key)
        .collect()
}

/// One resolved setting as JSON, through the plugin's own view (declared default unless an override
/// is stored). `None` for a plugin with no settings view at all.
#[must_use]
pub fn resolved_value(controls: &dyn PluginControls, key: &str) -> Option<Value> {
    controls.settings().map(|view| view.value(key, None))
}

/// The `{ plugin, key, from, to }` payload a settings write reports
/// (`diagnostics::settings_written_data`), kept here so a plugin's `data` block cannot drift.
#[must_use]
pub fn written_data(
    plugin: &str,
    key: &str,
    from: Option<Value>,
    to: Option<Value>,
) -> Map<String, Value> {
    super::diagnostics::settings_written_data(plugin, key, from, to)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_target_prefers_the_durable_address_over_the_index() {
        let with_id = ContextEntry {
            index: 3,
            message: HarnessMessage::User {
                content: "go".to_owned(),
                attachments: vec![],
                included: None,
                message_id: Some("msg-9".to_owned()),
            },
            included: true,
            reasoning_included: true,
            estimated_tokens: 1.0,
        };

        assert_eq!(with_id.target(), EntryTarget::MessageId("msg-9".to_owned()));
        assert_eq!(with_id.target().label(), "msg-9");

        let without_id = ContextEntry {
            index: 3,
            message: HarnessMessage::System {
                content: "be brief".to_owned(),
                included: None,
                message_id: None,
            },
            included: false,
            reasoning_included: true,
            estimated_tokens: 2.0,
        };

        assert_eq!(without_id.target(), EntryTarget::Index(3));
        assert_eq!(without_id.target().label(), "3");
    }

    #[test]
    fn a_seam_entry_admits_the_pure_pruning_rules_unchanged() {
        let entry = ContextEntry {
            index: 0,
            // A `System` entry stands in for "an entry with no durable address": the seam hands the
            // SAME `contract::HarnessMessage` the store holds, so `core_pruning`'s rules read it
            // without an adapter.
            message: HarnessMessage::System {
                content: "hello".to_owned(),
                included: Some(false),
                message_id: None,
            },
            included: false,
            reasoning_included: true,
            estimated_tokens: 0.5,
        };

        assert!(entry.message.is_withheld());

        let prune = entry.as_prune_entry();

        assert_eq!(prune.index, 0);
        assert!(!prune.included, "the effective unit flag travels, that is the whole point");
    }

    #[test]
    fn the_fallback_api_reads_declared_defaults_and_nothing_else() {
        let controls = FallbackControls::new();

        assert_eq!(controls.owner(), "");
        // An unknown owner has no stored values, so every typed read answers the caller's
        // fallback - which is what a host-level subscription gets in TS.
        let view = controls.settings().expect("the fallback always has a view");
        assert!(view.boolean("enabled", true));
        assert_eq!(view.number("contextLimit", 200_000.0), Some(200_000.0));
        assert!(!controls.runtime().thread_exists("thread-1"));
        assert!(controls.runtime().async_pending().is_empty());
        assert_eq!(controls.runtime().context_lane("thread-1"), None);
    }

    #[test]
    fn declared_keys_include_the_reserved_switch_for_a_plugin_that_never_declared_one() {
        // `async-ping` declares only `intervalMs`; the framework's synthesised `enabled` descriptor
        // is what makes the reserved key readable (host.test.ts:415-440).
        assert_eq!(
            declared_keys("async-ping"),
            vec!["intervalMs", "enabled"],
            "declared order first, the reserved key appended"
        );
        assert_eq!(declared_keys("nope"), Vec::<&str>::new());
    }
}
