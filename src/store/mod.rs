//! The session store: the in-memory state machine plus its durable copy.
//!
//! PORTED FROM
//! -----------
//! * `underload/harness/src/server/sessions.ts` (`HarnessSessionStore`): the session lifecycle, the
//!   pump, the recorder, the projector cache, and every operation an HTTP route performs.
//! * `underload/harness/src/harness.ts`: the parts of the live harness the STORE owns — each
//!   conversation's transcript, the created-tool ledger, the toolbox policy, the session agent
//!   bindings and the owner-question registry.
//!
//! WHAT THIS STAGE DOES NOT DO
//! ---------------------------
//! No agent loop, no provider call, no tool body and no projector algorithm live here: those arrive
//! through [`crate::ports`] (`TurnExecutor`, `ToolRuntime`, `ProjectorFactory`, `ActionHost`,
//! `TaskHost`), so this stage compiles and is tested on its own. There is also no HTTP and no axum
//! type: the transport (T5) maps [`crate::error::ApiError`] onto a status code.
//!
//! SHAPE OF THE STATE
//! ------------------
//! [`SessionState`] owns a live [`HarnessSessionRecord`] — this session's row of `state.json`, kept
//! current the way TS keeps `session.record` (`sessions.ts:3885`) — plus the members a document cannot
//! hold: projector folds, execution lanes, live runs, parked owner questions, the steer mailbox, the
//! label allocator, the toolbox ledger and the provider credential.
//!
//! The split is deliberate. Everything `persistence::validate_document` (T1) understands lives on the
//! record, so a persisted fact and a live fact cannot drift. Everything the pump needs that
//! `protocol.ts` has no field for lives beside it, and is written into `record.core` by
//! [`SessionState::sync_core`] — the one place the two halves meet, run on every session before a save.
//!
//! THE LOCK — read this before adding a method
//! -------------------------------------------
//! `Store` is `Send + Sync` through exactly ONE `std::sync::Mutex<StoreInner>`. The async surface is an
//! accommodation to the transport, not a concurrency strategy.
//!
//!   1. **The guard is never held across an `.await`.** Every method here is synchronous. The only async
//!      work is a turn, which runs on a `tokio::spawn`ed task and reaches the store through the settle
//!      channel — never by calling back in. Two sessions therefore pump concurrently and a long turn
//!      never blocks another session's read.
//!   2. **Persistence happens INSIDE the guard**, with blocking `std::fs`, before a mutation reports
//!      success. Writing outside the guard would let two writers publish interleaved snapshots of each
//!      other's half-states — the race the brief names. The lock is therefore
//!      "session-mutating-AND-persisting", and a failed write rolls the in-memory state back so the
//!      document and the machine never disagree (`sessions.ts:2256-2270`).
//!   3. **A mutation returns a [`CommitOutcome`]; it does not commit one.** A method that mutates a
//!      `SessionState` holds a mutable borrow out of the store guard, so it cannot also read the whole
//!      `StoreInner` to serialize it. It returns the bumps and hub events it wants, and the CALLER —
//!      which owns the guard — runs [`Store::commit`] (sync, save, bump, publish). An `?` before that
//!      call IS the rollback: nothing was written and no counter moved.
//!   4. **A hub listener must not call back into the store synchronously**: the T2 hub fans out on the
//!      publishing thread while this mutex is held (`events.rs:360` `fan_out`). The stream layer hands
//!      frames to its own queue instead.
//!   5. **The provider credential never enters the record.** It is a [`SessionState`] field
//!      (`provider_key`), so no code path can serialize it by accident — the rule `sessions.ts:1618-1634`
//!      enforces by keeping it in a side map.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::{Map, Value};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::agents::AgentCatalogue;
use crate::config::{HarnessRuntimeSettings, ServerConfig};
use crate::contract::{
    AgentResultReference, BindingOrigin, HarnessActionAnswer, HarnessActionRequest, HarnessEvent,
    HarnessUsage, PageMeta, ProviderConfig, SessionStatus, SessionToolOrigin, SessionTurnStatus,
    ThreadLifecycle, ThreadOrigin, TurnOutcome,
};
use crate::error::ApiError;
use crate::events::EventHub;
use crate::journal::HarnessJournal;
use crate::labels::LabelAllocator;
use crate::persistence::{
    HARNESS_DATABASE_VERSION, HarnessDatabase, HarnessDescriptorRecord, HarnessInvalidThreadRecord,
    HarnessPresetRecord, HarnessSessionRecord, HarnessSessionSettingsRecord, HarnessThreadRecord,
    SessionTurn, SteerRecord,
};
use crate::ports::{Projector, ProjectorFactory, ProjectorSeed, Query, ToolRuntime, TurnExecutor};
use crate::registry::HarnessRegistry;
use crate::store::actions::{StoreActionHost, StoreTaskHost, TaskRow};
use crate::store::tools::CreatedTool;

pub mod actions;
pub mod agents;
pub mod commands;
pub mod read;
pub mod session;
pub mod threads;
pub mod tools;
pub mod transcript;
pub mod turns;

/// The store refuses more than this many QUEUED tasks per session (`sessions.ts:84`), and the refusal
/// text interpolates this number exactly as TS does (`:2874`).
pub const MAX_PENDING_TASKS: usize = 100;

/// Turns kept live per session (`harnessService.ts:106 turnLimit = 200`).
pub const DEFAULT_TURN_LIMIT: usize = 200;

/// A terminal turn older than this is dropped from an oversized session (`turnTtl: 30d`).
pub const DEFAULT_TURN_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

/// Tasks per session before completed rows are pruned (`persistence.ts:107 taskCount: 50`).
pub const DEFAULT_TASK_COUNT: usize = 50;

/// Entries kept in one agent's own conversation (`taskTurns: 20`).
pub const DEFAULT_TASK_TURNS: usize = 20;

/// Entries kept in one persistent conversation (`threadTurns: 50`).
pub const DEFAULT_THREAD_TURNS: usize = 50;

/// Milliseconds an idle session survives before it is dropped (`sessionIdleMs: 15 minutes`).
pub const DEFAULT_SESSION_IDLE_MS: u64 = 15 * 60 * 1_000;

/// Sessions the store holds before evicting the oldest idle one (`maxSessions: 64`).
pub const DEFAULT_SESSION_LIMIT: usize = 64;

/// How often the pump checkpoints a RUNNING turn to disk (`sessions.ts CHECKPOINT_INTERVAL_MS = 2500`):
/// an hour-long run must not lose all of its events when the process dies.
pub const CHECKPOINT_INTERVAL_MS: u64 = 2_500;

/// Tool-loop budget (`harnesses/toolbox.ts:103 PRESET_MAX_ITERATIONS = 1000`).
pub const PRESET_MAX_ITERATIONS: u64 = 1_000;

/// The `tool` entry a slash command result rides on (`core/command.ts COMMAND_TOOL_NAME`).
pub const COMMAND_TOOL_NAME: &str = "command";

/// `SessionTurn.extra` key carrying the agent this turn ran as. `SessionTurn` (T1) mirrors
/// `protocol.ts` exactly, so a store-only fact rides in its flattened `extra` map.
pub const EXTRA_AGENT: &str = "agent";

/// `SessionTurn.extra` key carrying the executor this turn ran with.
pub const EXTRA_EXECUTOR: &str = "executor";

/// `SessionTurn.extra` key carrying the final `stopReason` the run reported
/// ([`TurnOutcome::stop_reason`]).
pub const EXTRA_STOP_REASON: &str = "stopReason";

/// `SessionTurn.extra` key carrying the invocation id this turn's root run reported. TS keeps the same
/// fact in `record.turnInvocations`, which this store also writes; the copy on the turn is what a
/// display fold reads without a second lookup.
pub const EXTRA_INVOCATION: &str = "invocationId";

/// `record.core` key carrying the session-scoped agent overrides. TS persists a top-level
/// `record.sessionAgents` NAME LIST (`persistence.ts:241-249`); the store additionally needs each
/// binding's profile, role, instruction override and provenance, and T1's record has no field for
/// those, so they ride inside the snapshot envelope under this key. See knowledge.md "Decisions".
pub const CORE_SESSION_AGENTS: &str = "sessionAgentOverrides";

/// `record.core` key carrying the undelivered steering mailbox. TS keeps the mailbox in memory
/// (`session.steerRecords`); the durable copy of a DELIVERED steer is `turn.steers`, and the mailbox is
/// what survives a restart between accept and delivery.
pub const CORE_STEER_MAILBOX: &str = "steerMailbox";

/// Every deployment value the store reads: the session-related subset of
/// `underload/harness/src/harnessService.ts`'s options.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Root of the durable state tree (`harnessPersistenceRoot`). `<root>/harness/state.json` is the
    /// document and `<root>/harness/sessions/<id>/events.jsonl` is one session's journal
    /// (`persistence.rs:129-149`).
    pub root: PathBuf,
    /// `sessionPersist` (`:105`): `false` makes the store memory-only; the journal is still written, as
    /// TS does for an in-memory deployment.
    pub persist: bool,
    /// `sessionIdleMs` (`:107`).
    pub idle_ms: u64,
    /// `turnLimit` (`:106`).
    pub turn_limit: usize,
    /// Turn TTL in milliseconds.
    pub turn_ttl_ms: u64,
    /// `maxSessions` (`:104`).
    pub session_limit: usize,
    /// `taskCount` (`persistence.ts:107`).
    pub task_count: usize,
    /// `taskTurns` (`:107`).
    pub task_turns: usize,
    /// `threadTurns` (`:108`).
    pub thread_turns: usize,
}

impl StoreConfig {
    /// The production default: `<package_root>/harness`, durable, every cap at the TS default. The root
    /// is package-relative so a test can point the whole store at a fixture directory (`tests/store.rs`)
    /// the way the TS suites point theirs at a temp dir.
    #[must_use]
    pub fn default_config() -> Self {
        Self {
            root: crate::package_root::repo_root().join("harness"),
            persist: true,
            idle_ms: DEFAULT_SESSION_IDLE_MS,
            turn_limit: DEFAULT_TURN_LIMIT,
            turn_ttl_ms: DEFAULT_TURN_TTL_MS,
            session_limit: DEFAULT_SESSION_LIMIT,
            task_count: DEFAULT_TASK_COUNT,
            task_turns: DEFAULT_TASK_TURNS,
            thread_turns: DEFAULT_THREAD_TURNS,
        }
    }

    /// The same defaults under a caller-chosen root, which is what every test uses.
    #[must_use]
    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            ..Self::default_config()
        }
    }

    /// `<root>/harness`, the directory this package owns (`persistence.rs:129 state_dir`).
    #[must_use]
    pub fn state_dir(&self) -> PathBuf {
        crate::persistence::state_dir(&self.root)
    }

    /// `<root>/harness/sessions`, the journal root (`persistence.rs:148 sessions_dir`).
    #[must_use]
    pub fn sessions_dir(&self) -> PathBuf {
        crate::persistence::sessions_dir(&self.root)
    }
}

/// The store's result type. `ApiError` is the crate's one error type (T2), and the transport maps its
/// `status` onto the wire, so a store refusal states its status here.
pub type StoreResult<T> = Result<T, ApiError>;

/// One hub change a mutation wants published, already addressed.
///
/// Targets hold OWNED strings, because a mutation builds them while the caller's guard borrow is
/// mutable; [`Store::commit`] re-borrows them at publish time.
#[derive(Debug, Clone)]
pub struct HubEmit {
    pub kind: String,
    pub targets: Vec<(String, Option<String>)>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub payload: Value,
    pub ops: Vec<Value>,
}

/// What a mutation wants done once its state change is final: the counters to bump, the hub events to
/// publish, and the journal refusals it accumulated.
///
/// The bump list lives INSIDE the outcome (rather than "bump while mutating") for the reason TS states
/// at `api-events.ts:302-306`: a counter must move exactly once per committed write, so a mutation that
/// rolls back must not have advanced anything.
#[derive(Debug, Default, Clone)]
pub struct CommitOutcome {
    /// Resources to bump, in publish order.
    pub bumps: Vec<(String, Option<String>)>,
    /// The hub events to publish, in order.
    pub events: Vec<HubEmit>,
    /// The session revision the caller reports. `0` means "derive it from the bumps".
    pub revision: u64,
    /// Journal write refusals: durable state committed, replay log degraded
    /// (`sessions.ts:1121-1131`), which is never a caller-visible failure.
    pub journal_errors: Vec<String>,
}

impl CommitOutcome {
    /// An outcome that publishes nothing and bumps only the session counter.
    #[must_use]
    pub fn touch(session_id: &str) -> Self {
        Self {
            bumps: vec![("session".to_owned(), Some(session_id.to_owned()))],
            ..Self::default()
        }
    }

    /// Appends one hub event together with the bumps its targets need.
    #[must_use]
    pub fn with_event(mut self, emit: HubEmit) -> Self {
        for target in &emit.targets {
            self.bumps.push(target.clone());
        }
        self.events.push(emit);
        self
    }

    /// Addresses one hub event (kind plus payload) against the given resources.
    #[must_use]
    pub fn event(
        self,
        kind: &str,
        targets: &[(&str, Option<&str>)],
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        payload: Value,
    ) -> Self {
        self.event_with_ops(kind, targets, thread_id, turn_id, payload, Vec::new())
    }

    /// The same, carrying compiled display ops (`display.ops`, `api-events.ts:320`).
    #[must_use]
    pub fn event_with_ops(
        self,
        kind: &str,
        targets: &[(&str, Option<&str>)],
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        payload: Value,
        ops: Vec<Value>,
    ) -> Self {
        self.with_event(HubEmit {
            kind: kind.to_owned(),
            targets: targets
                .iter()
                .map(|(resource, id)| ((*resource).to_owned(), id.map(str::to_owned)))
                .collect(),
            thread_id: thread_id.map(str::to_owned),
            turn_id: turn_id.map(str::to_owned),
            payload,
            ops,
        })
    }

    /// Records one journal refusal without failing the write.
    pub fn journal_error(&mut self, message: impl Into<String>) {
        self.journal_errors.push(message.into());
    }
}

/// A turn the store is projecting while its executor runs.
///
/// The projector lives here rather than in the record because it is per-turn DISPLAY state: TS keeps the
/// same pairing on `session.projections` (`sessions.ts:642`) and rebuilds it at restore by replaying the
/// recorded events (`:3585-3590`).
pub struct TurnState {
    /// The durable row, mutated in place as events arrive (`sessions.ts:6704 recordTurnEvent`).
    pub record: SessionTurn,
    /// How far [`TurnState::projector`] has folded. The recorder only ever applies `events[folded..]`,
    /// which is what makes "live == replay" true by construction.
    pub folded: usize,
    /// The turn's live display fold.
    pub projector: Box<dyn Projector>,
    /// What the projector was opened with ([`ProjectorSeed`]).
    ///
    /// It lives on the runtime because [`TurnState::replay`] has to open a projector with the SAME
    /// seed the live run used: the three non-event segments (system, prompt, steers) are not in the
    /// event stream, so a replay that seeded less would rebuild a bar missing segments and break the
    /// `live == replay` invariant the restore path depends on.
    pub seed: ProjectorSeed,
    /// The system instructions this turn's agents actually ran with, per agent
    /// (`record.turnInstructionSources[turnId]`). Held on the runtime because the fold consumes them and
    /// T1 persists the map as opaque JSON.
    pub instruction_sources: Vec<Value>,
    pub extra: Map<String, Value>,
}

impl std::fmt::Debug for TurnState {
    /// A hand-written impl rather than a derive, because [`Projector`] is a trait object with no `Debug`.
    /// The two fields that matter when a store invariant breaks (the durable row and the fold position)
    /// are both reported.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnState")
            .field("id", &self.record.id)
            .field("thread_id", &self.record.thread_id)
            .field("status", &self.record.status)
            .field("folded", &self.folded)
            .field("events", &self.record.events.len())
            .finish_non_exhaustive()
    }
}

impl TurnState {
    /// A fresh fold for one opened turn (`sessions.ts:2855 appendTurn`).
    ///
    /// The seed is derived from the record alone ([`seed_from_turn_record`]); a caller that also has the
    /// CONVERSATION's context — the section the block ids are prefixed with, the first user entry's id,
    /// the recorded instruction sources — upgrades it with [`TurnState::set_seed`] before folding, which
    /// is what `store::turns` does on the way in and what restore does on the way back.
    #[must_use]
    pub fn new(store: &Store, record: SessionTurn) -> Self {
        let seed = seed_from_turn_record(&record);

        Self::seeded(store, record, seed)
    }

    /// The same fold, opened with a seed the CALLER assembled — normally
    /// [`SessionState::projector_seed`], which adds the conversation's section identity and the turn's
    /// recorded instruction sources.
    #[must_use]
    pub fn seeded(store: &Store, record: SessionTurn, seed: ProjectorSeed) -> Self {
        Self {
            projector: store.projector.create(&seed),
            record,
            folded: 0,
            seed,
            instruction_sources: Vec::new(),
            extra: Map::new(),
        }
    }

    /// A fold rebuilt from a durable record (`sessions.ts:3585-3600 restoreTurn`).
    #[must_use]
    pub fn from_record(store: &Store, record: &SessionTurn) -> Self {
        let seed = seed_from_turn_record(record);
        Self {
            projector: store.projector.create(&seed),
            record: record.clone(),
            folded: 0,
            seed,
            instruction_sources: Vec::new(),
            extra: Map::new(),
        }
    }

    /// Reopens the fold with a fuller seed and re-folds what has already been recorded.
    ///
    /// The store opens a turn before it knows the conversation's section identity, and a projector
    /// minted from the thin seed would keep the wrong block-id prefix for the whole run. Re-folding the
    /// events already in `record` (rather than throwing the projector away at event 0) is what keeps the
    /// `live == replay` invariant: the fold a restart rebuilds goes through this same path.
    pub fn set_seed(&mut self, factory: &dyn ProjectorFactory, seed: ProjectorSeed) {
        self.seed = seed;
        self.projector = factory.create(&self.seed);
        self.folded = 0;
        // Cloned, because the fold writes through `&mut self` while it reads the seed.
        let seed = self.seed.clone();
        self.replay_onto(&seed, factory);
    }

    /// The agent this turn runs as.
    #[must_use]
    pub fn agent(&self) -> &str {
        self.record
            .extra
            .get(EXTRA_AGENT)
            .and_then(Value::as_str)
            .unwrap_or("")
    }

    /// The executor this turn was submitted with, if the client named one.
    #[must_use]
    pub fn executor(&self) -> Option<&str> {
        self.record
            .extra
            .get(EXTRA_EXECUTOR)
            .and_then(Value::as_str)
    }

    /// The final `stopReason` the run reported, if any.
    #[must_use]
    pub fn stop_reason(&self) -> Option<&str> {
        self.record
            .extra
            .get(EXTRA_STOP_REASON)
            .and_then(Value::as_str)
    }

    /// The invocation id this turn's root run reported, if the run reached `start`.
    #[must_use]
    pub fn invocation_id(&self) -> Option<&str> {
        self.record
            .extra
            .get(EXTRA_INVOCATION)
            .and_then(Value::as_str)
    }

    /// The turn's status as a typed value.
    #[must_use]
    pub fn status(&self) -> SessionTurnStatus {
        parse_turn_status(&self.record.status)
    }

    /// The recorded prompt length in UTF-16 code units: the exact figure beside every bounded
    /// preview (`turn-display.ts:412-416`, invariant 5 - TS states `.length`, which counts units).
    #[must_use]
    pub fn prompt_chars(&self) -> u64 {
        u64::try_from(crate::projector::display_chars(&self.record.prompt)).unwrap_or(u64::MAX)
    }

    /// `sessions.ts:6704 recordTurnEvent`: one event, recorded and projected in the order the recorder
    /// stores them.
    ///
    /// Returns the display ops this event produced, which is what a `display.ops` frame carries. The
    /// recorder is the ONLY place those ops are minted, so a live run and a replay cannot diverge.
    ///
    /// `lane` is the conversation's `contextLane` read LIVE by the caller, at this moment, and the
    /// recorder stamps it on the event whenever it is non-zero (`sessions.ts:7925-7936`). Reading it per
    /// event rather than caching it at turn start is the whole point: a compaction that happens
    /// mid-run has to take effect on the very next event, which is what puts one turn's pre- and
    /// post-compaction segments on two rows.
    pub fn record_event(&mut self, event: &HarnessEvent, at: u64, lane: Option<u64>) -> Vec<Value> {
        self.record
            .events
            .push(crate::persistence::SessionTurnEvent::with_lane(
                event.clone(),
                lane,
            ));
        match self.record.event_times.as_mut() {
            // An older document has no `eventTimes` at all; opening one here keeps the two arrays
            // parallel, which is the invariant `persistence.ts:162-167` states.
            Some(times) => times.push(at),
            None => self.record.event_times = Some(vec![at]),
        }
        // `apply` gets the event's ordinal WITHIN the turn, because a display step's id IS that ordinal
        // as a decimal string (`harness-api.ts:1792`).
        //
        // The ops are stamped HERE, at the one place they are minted: the fold has no turn record, so it
        // mints `turnId: ""` and `s{ordinal}` (`projector.rs:418-434`), and the wire identity is
        // `${turnId}:${ordinal}`. `TurnState::display` stamps the snapshot with the same rule
        // (`projector::section_wire_id`), which is what makes a streamed section id equal the id a
        // display read reports.
        // The lane the fold sees is the lane just stamped, or `None` when the stamp was suppressed —
        // one source, so the live fold and a replay of these same records cannot disagree.
        let stamped = self.record.events.last().and_then(|entry| entry.lane);
        let ops = crate::projector::stamp_display_ops(
            &self.record.id,
            self.projector.apply(event, stamped, at, self.folded as u64),
        );
        self.folded += 1;
        self.record.updated_at = at;
        ops
    }

    /// Completes the fold at a terminal status (`turn-projector.ts`'s settle step): a turn that ended
    /// `error`/`cancelled` must not keep streaming state in its row.
    pub fn settle_projection(&mut self, at: u64) {
        self.projector.settle(&self.record.status, at);
        self.record.updated_at = at;
    }

    /// Folds every recorded event through a FRESH projector — `live == replay`, the invariant the restore
    /// path depends on: a restored turn's rows are identical to the rows its live run made.
    pub fn replay(&mut self, factory: &dyn ProjectorFactory) {
        self.replay_onto(&self.seed.clone(), factory);
    }

    /// The fold body, split out so [`TurnState::set_seed`] can re-fold with a seed it was handed
    /// without borrowing `self.seed` while the fold writes through `self`.
    fn replay_onto(&mut self, seed: &ProjectorSeed, factory: &dyn ProjectorFactory) {
        let mut projector = factory.create(seed);
        let events = std::mem::take(&mut self.record.events);
        let times = self.record.event_times.clone().unwrap_or_default();
        for (index, entry) in events.iter().enumerate() {
            let at = times.get(index).copied().unwrap_or(self.record.updated_at);
            // Each event carries its own recorded stamp, so a restart replays the SAME lanes
            // (`persistence::SessionTurnEvent`).
            projector.apply(&entry.event, entry.lane, at, index as u64);
        }
        self.record.events = events;
        self.folded = self.record.events.len();
        if matches!(
            self.status(),
            SessionTurnStatus::Done | SessionTurnStatus::Error | SessionTurnStatus::Cancelled
        ) {
            projector.settle(&self.record.status, self.record.updated_at);
        }
        self.projector = projector;
    }

    /// Which agent emitted one event (`shared/turn-display.ts eventAgent`).
    #[must_use]
    pub fn event_agent(event: &HarnessEvent) -> &str {
        match event {
            HarnessEvent::Start { agent, .. }
            | HarnessEvent::Text { agent, .. }
            | HarnessEvent::Reasoning { agent, .. }
            | HarnessEvent::ToolCall { agent, .. }
            | HarnessEvent::ToolResult { agent, .. }
            | HarnessEvent::Usage { agent, .. }
            | HarnessEvent::Done { agent, .. }
            | HarnessEvent::Retry { agent, .. }
            | HarnessEvent::AttemptCommit { agent, .. }
            | HarnessEvent::Wakeup { agent, .. }
            | HarnessEvent::ActionRequest { agent, .. }
            | HarnessEvent::ActionAnswer { agent, .. }
            | HarnessEvent::BranchStart { agent, .. }
            | HarnessEvent::BranchEnd { agent, .. }
            | HarnessEvent::Error { agent, .. } => agent,
        }
    }
}

/// One conversation's execution lane (`session.lanes`, `sessions.ts:2394-2400`).
///
/// The pump runs one turn per lane; a lane's backlog is the work addressed at THAT conversation, which
/// is what lets two conversations progress at the same time.
#[derive(Debug, Clone, Default)]
pub struct ThreadLane {
    pub thread_id: String,
    /// The turn running in this lane right now, if any.
    pub active_turn: Option<String>,
    /// The backlog addressed at this conversation, in arrival order.
    pub queue: Vec<String>,
    /// Single-flight flag for this lane's pump (`sessions.ts:2399 lane.pumping`). Set INSIDE the store
    /// guard before `tokio::spawn`, so a second submit for the same conversation never starts a second
    /// worker and one conversation never runs two turns at once. Cleared by `end_pump`.
    pub pumping: bool,
    /// Set by a stop request: the lane finishes its current turn and then claims nothing else, so a
    /// cancel does not throw away the backlog of a CONVERSATION the operator did not name
    /// (`sessions.ts:7530 stopRequested`).
    pub stop_requested: bool,
}

/// A run this store is executing right now (`ActiveThreadRun`, `sessions.ts:642`).
///
/// NOT `Clone`: a [`tokio::task::JoinHandle`] and a live steer channel belong to exactly one run, and
/// copying them would pretend two runs exist.
#[derive(Debug)]
pub struct ActiveRun {
    pub turn_id: String,
    pub index: u64,
    pub thread_id: String,
    pub invocation_id: Option<String>,
    pub started_at: u64,
    /// Cancels this turn (`turn_cancelled` / `thread_cancelled`, `sessions.ts:3774-3795`).
    pub cancel: CancellationToken,
    /// Set by the store BEFORE the abort, because the executor rejects the run with its own cancel
    /// error: this flag is what records the turn as `cancelled` rather than `error`
    /// (`sessions.ts:7506-7518`).
    pub cancel_requested: bool,
    /// The live end of this conversation's steer channel. `None` once the executor has taken it, and
    /// always `None` after a restore — which is exactly why the durable mailbox exists.
    pub steer_sender: Option<mpsc::UnboundedSender<String>>,
    /// The answer channel of the owner questions this run parked, so `answer_action` reaches a live run
    /// without materializing a second registry.
    pub action_sink: Option<Arc<StoreActionHost>>,
    /// The executor task, kept so a delete can abort a run and so a test can await it. `None` while the
    /// run is being constructed.
    pub joiner: Option<tokio::task::JoinHandle<()>>,
}

/// One owner question this session parked, plus the promise its tool call waits on.
///
/// Named `PendingActionEntry` because [`crate::contract::PendingAction`] is the WIRE row: this is the
/// store-side entry it is projected from (`api-reads.ts:1323 pendingActionRowOf`).
#[derive(Debug)]
pub struct PendingActionEntry {
    pub request: HarnessActionRequest,
    /// `None` when the question came from the session's root run, which has no conversation of its own;
    /// `api-reads.ts:1327-1330` resolves that join at read time.
    pub turn_id: Option<String>,
    pub thread_id: Option<String>,
    pub agent: String,
    pub opened_at: u64,
    /// `None` while the question is live; the answer once it settled.
    pub answer: Option<HarnessActionAnswer>,
    pub resolved_at: Option<u64>,
    /// `Some` only while live: the promise the `actionRequest` call is parked on. A restored session has
    /// none, which is why a restored question reads as EXPIRED (`sessions.ts:2684-2688`: the promise no
    /// longer exists).
    pub responder: Option<oneshot::Sender<Result<Value, String>>>,
    pub extra: Map<String, Value>,
}

impl PendingActionEntry {
    /// A question is live while nobody has answered it AND its promise still exists.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.answer.is_none() && self.responder.is_some()
    }
}

/// A session agent binding as the STORE holds it.
///
/// [`crate::contract::SessionAgentBinding`] is the WIRE row; the store additionally carries the
/// instruction override and the audit fields, so this is the internal superset and
/// [`agents::binding_of`] projects it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionAgentEntry {
    pub agent_id: String,
    /// The trusted catalogue profile this binding runs (`CreateSessionAgentRequest.profile`).
    pub profile: String,
    pub origin: BindingOrigin,
    pub role: crate::contract::AgentRole,
    /// The live model override; `None` means "the session model".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The session-scoped instruction override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    pub created_at: u64,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A session that is no longer live but was real, kept so `GET /sessions?includeExpired=true` can name
/// it (`sessions.ts:926-933`).
#[derive(Debug, Clone)]
pub struct ExpiredSession {
    pub id: String,
    pub harness: String,
    pub created_at: u64,
    pub expired_at: u64,
    pub turn_count: u64,
}

/// The store's mutable half.
///
/// THE HOST VIEW (`Store.registry` / `Store.agents` / `Store.settings` in TS, where the store object
/// literally holds the live registry and the live `globalSettings`): the CURRENT registry, agent
/// catalogue and deployment settings live HERE, behind the same mutex as the sessions, and the host
/// routes push a fresh copy through [`Store::set_host_view`] after every committed host write. They
/// are NOT boot-time snapshots: `POST /harnesses`, `PATCH /harnesses`, `PATCH /agents` and
/// `PATCH /config` must be visible to the NEXT `POST /sessions` without a restart
/// (`sessions.ts:1827` reads `this.registry` and `api.ts:1974` reads `store.globalSettings` on every
/// create). Keeping them under the store lock — rather than in a second lock the store reaches into —
/// is what makes "read the host view and create the record" one atomic step and keeps the documented
/// lock order (host locks -> store lock) inversion-free.
#[derive(Debug)]
pub struct StoreInner {
    /// Sessions by id. A `BTreeMap` because every route addresses one session by id and the crate has no
    /// insertion-ordered map dependency.
    pub sessions: BTreeMap<String, SessionState>,
    /// The session ids in ARRIVAL order, which IS the session list order: TS lists `liveSessions()`
    /// (`sessions.ts:946`), the `Map`'s insertion order, NOT `createdAt` descending. Keeping the order in
    /// its own list is what lets a delete remove one entry without renumbering the rest.
    pub session_order: Vec<String>,
    /// Tombstones of sessions dropped on idle or eviction.
    pub expired: Vec<ExpiredSession>,
    /// Per-session journals. They live HERE and not on [`SessionState`] because a [`HarnessJournal`] owns
    /// a file handle and is not `Clone`, while a session state must stay restorable (`sessions.ts:735`
    /// `this.journals`, the same split).
    pub journals: BTreeMap<String, HarnessJournal>,
    /// Why a session or the document was skipped at restore. The crate has no logging dependency, so a
    /// degraded boot is REPORTED (`Store::diagnostics`) rather than printed.
    pub diagnostics: Vec<String>,
    /// Journal refusals collected while serving (`sessions.ts:1121-1131`): the durable write succeeded and
    /// the replay log degraded, which is never a caller-visible failure.
    pub journal_problems: Vec<String>,
    /// `sessions.ts:8638 shuttingDown`: once set, no pump claims another queued turn.
    pub shutting_down: bool,
    /// The CURRENT harness registry, as the host routes committed it (`sessions.ts:1827` reads the
    /// live registry on every create). An `Arc` so a session's frozen copy and a route's snapshot
    /// stay cheap; replaced whole, never mutated in place, so a reader under the lock cannot see a
    /// half-applied `PATCH`.
    pub host_registry: Arc<HarnessRegistry>,
    /// The CURRENT agent catalogue plus the operator's global overlay (`harnesses/agent-state.ts`,
    /// reached through `api.ts:1707`). The instruction ladder's `global` layer and every roster row
    /// read this, so a `PATCH /agents` is visible to the next session and the next turn.
    pub host_agents: Arc<AgentCatalogue>,
    /// The CURRENT deployment configuration (`api.ts:1368-1446 globalSettings` + the runtime knobs).
    /// `settings()` is the provider trio `POST /sessions` merges over; `runtime_config()` is where
    /// `runtime_execution` (`api-reads.ts:226 context.runtime.threadExecution`) comes from.
    pub host_config: ServerConfig,
    /// The operator's built-in PLUGIN setting overrides, plus the counter every `/plugins` page
    /// quotes (`sessions.ts:1510-1516 this.plugins = createPluginSettingsStore(...)`, and the
    /// `pluginSettings` member of `databaseRecord` at `:2758-2766`).
    ///
    /// It lives HERE, inside the guarded state, for one reason: [`Store::database_of`] is the only
    /// writer of `state.json`, so an override becomes durable in the SAME atomic commit as the
    /// sessions around it. A second store with a second writer would let a session mutation and a
    /// settings patch overwrite each other's document.
    ///
    /// The catalogued metadata is NOT stored here - it is the `static` table in
    /// [`crate::plugins`], the Rust shape of the generated built-in manifest this build has no scan
    /// to produce.
    pub plugins: crate::plugins::PluginSettingsStore,
}

impl StoreInner {
    /// The host view of the harness registry (`sessions.ts:1827`).
    #[must_use]
    pub fn registry(&self) -> &HarnessRegistry {
        &self.host_registry
    }

    /// The host view of the agent catalogue (`api.ts:1707`).
    #[must_use]
    pub fn agents(&self) -> &AgentCatalogue {
        &self.host_agents
    }

    /// The provider trio the NEXT session merges over (`api.ts:1974-1989` reads
    /// `store.globalSettings` at create time, never a boot snapshot).
    #[must_use]
    pub fn settings(&self) -> HarnessRuntimeSettings {
        self.host_config.settings()
    }

    /// The deployment runtime block, live (`api-reads.ts:226 context.runtime.threadExecution`).
    #[must_use]
    pub fn runtime_config(&self) -> &ServerConfig {
        &self.host_config
    }

    #[must_use]
    pub fn find(&self, session_id: &str) -> Option<&SessionState> {
        self.sessions.get(session_id)
    }

    pub fn find_mut(&mut self, session_id: &str) -> Option<&mut SessionState> {
        self.sessions.get_mut(session_id)
    }

    /// The read spelling used by the session-domain routes.
    #[must_use]
    pub fn session(&self, session_id: &str) -> Option<&SessionState> {
        self.find(session_id)
    }

    /// The write spelling used by the session-domain routes.
    pub fn session_mut(&mut self, session_id: &str) -> Option<&mut SessionState> {
        self.find_mut(session_id)
    }

    /// Adds a session, keeping list order = arrival order.
    pub fn insert(&mut self, state: SessionState) {
        let session_id = state.record.id.clone();
        if self.sessions.insert(session_id.clone(), state).is_none() {
            self.session_order.push(session_id);
        }
    }

    /// Removes one session and its place in the list order, keeping the order of the rest intact.
    pub fn remove(&mut self, session_id: &str) -> Option<SessionState> {
        let removed = self.sessions.remove(session_id)?;
        self.session_order.retain(|existing| existing != session_id);
        Some(removed)
    }

    /// The session ids, in list order.
    #[must_use]
    pub fn ids(&self) -> Vec<String> {
        self.session_order
            .iter()
            .filter(|id| self.sessions.contains_key(*id))
            .cloned()
            .collect()
    }

    /// Every session in list order — the shape every list, save and evict path iterates.
    #[must_use]
    pub fn list(&self) -> Vec<&SessionState> {
        self.ids()
            .iter()
            .filter_map(|id| self.sessions.get(id))
            .collect()
    }

    /// Every session, for the paths that iterate the whole store (save, evict, prune). The BTreeMap's key
    /// Every session, for the paths that iterate the whole store (save, evict, prune). The BTreeMap's key
    /// order is fine here: only [`StoreInner::list`] has to match the LIST order the contract promises.
    // No `#[must_use]`: an iterator is already must-use, and repeating the attribute adds a lint warning.
    pub fn all(&self) -> impl Iterator<Item = &SessionState> {
        self.sessions.values()
    }

    pub fn all_mut(&mut self) -> impl Iterator<Item = &mut SessionState> {
        self.sessions.values_mut()
    }

    /// Every collection empty, the host view NOT included: the three host fields have no sane
    /// default of their own (a registry needs the deployment settings), so [`Store::open`] supplies
    /// them and this fills the rest. It replaces the `Default` derive the T3 port used while the
    /// struct held only collections.
    #[must_use]
    pub fn default_state() -> Self {
        Self {
            sessions: BTreeMap::new(),
            session_order: Vec::new(),
            expired: Vec::new(),
            journals: BTreeMap::new(),
            diagnostics: Vec::new(),
            journal_problems: Vec::new(),
            shutting_down: false,
            host_registry: Arc::new(HarnessRegistry::shipped(
                &ServerConfig::default().settings(),
                Vec::new(),
            )),
            host_agents: Arc::new(AgentCatalogue::builtin()),
            host_config: ServerConfig::default(),
            // A fresh store has no override for any plugin, and a revision of 0 - "never changed",
            // exactly like every other collection before its first write. The real deployment's
            // saved values arrive through `Store::restore_all`.
            plugins: crate::plugins::PluginSettingsStore::default(),
        }
    }
}

/// The deployment configuration a bare [`StoreConfig`] implies: the settings trio the caller handed
/// in, plus every runtime knob at its built-in default (`config.rs`'s `DEFAULT_*` constants).
///
/// A store opened directly (a test, an embedder that supplies its own trio) therefore reports the
/// same `runtime_execution` TS's server reports when `HARNESS_THREAD_EXECUTION` is unset. The real
/// deployment values arrive through [`Store::set_host_view`], which `runtime::build_runtime` calls
/// immediately after `open` with the loaded `ServerConfig`.
#[must_use]
pub fn runtime_config_from(settings: &HarnessRuntimeSettings) -> ServerConfig {
    ServerConfig {
        base_url: settings.base_url.clone(),
        api_key: settings.api_key.clone(),
        model: settings.model.clone(),
        ..ServerConfig::default()
    }
}

/// One live session, exactly as the pump sees it (`SessionRecord`, `api-reads.ts:88`).
///
/// NOT `Clone`: an [`ActiveRun`] owns a task handle and an [`PendingActionEntry`] owns a oneshot sender,
/// so a copy would name runs and promises that do not exist. A mutation that needs an undo point clones
/// [`SessionState::record`] instead — see [`Store::snapshot_state`].
#[derive(Debug)]
pub struct SessionState {
    /// The durable row, kept current (`sessions.ts:3885 sessionRecordOf` is the same idea: the record IS
    /// the session as the document sees it).
    pub record: HarnessSessionRecord,

    /// Recorded turns by id, with their live display folds.
    pub turns: HashMap<String, TurnState>,
    /// Recorded turn ids in recorded order (`session.turnOrder`, `:642`).
    pub turn_order: Vec<String>,

    /// The conversation roster, each entry carrying that conversation's committed transcript and
    /// transcript revision (`core.threads[]`, a [`HarnessThreadRecord`] each).
    pub threads: HashMap<String, HarnessThreadRecord>,
    /// Conversation ids in creation order: the roster's stable iteration order.
    pub thread_order: Vec<String>,
    /// Conversation ordinal high-water (`core.threadSequence`), the source of `thread-<n>` ids.
    pub thread_seq: u64,
    /// Turn INDEX high-water. TS names this counter `threadSequence` on the durable record
    /// (`persistence.ts:177-181`), where it is a high-water of TURN indexes; the store keeps the two
    /// counters apart and writes this one to that field, so a document written here reloads in TS
    /// unchanged.
    pub turn_seq: u64,
    /// thread id -> display label (`record.threadLabels`).
    pub thread_labels: BTreeMap<String, String>,
    /// code -> highest ordinal ever minted (`record.threadLabelHighWater`). Deletion never lowers this,
    /// which is what stops a retired name being re-minted.
    pub label_high_water: BTreeMap<String, u64>,
    /// The live label allocator (`labels.rs:144`), seeded from [`SessionState::label_high_water`] at
    /// restore and written back to it on every sync.
    pub labels: LabelAllocator,
    /// Conversations whose stored transcript could not be read (`record.invalidThreads`).
    pub invalid_threads: HashMap<String, HarnessInvalidThreadRecord>,

    /// The legacy (threadless) queue: turn ids in FIFO order (`record.queue`).
    pub queue: Vec<String>,
    /// Per-conversation lanes, the pump's unit of concurrency.
    pub lanes: HashMap<String, ThreadLane>,
    /// Every run in flight.
    pub active: Vec<ActiveRun>,
    /// The conversation a task lands in when the client names none.
    pub active_thread: Option<String>,

    /// The steering mailbox: conversation id -> undelivered guidance. The durable copy of a DELIVERED
    /// steer rides on the owning turn (`turn.steers`); this is the accepted-but-not-delivered half, which
    /// is what survives a restart.
    pub steer_mailbox: HashMap<String, Vec<SteerRecord>>,
    /// Guidance accepted and not yet consumed, session-wide (`session.steerActive`).
    pub steer_active: u64,
    /// The LIVE steer channel of each conversation that is running: `thread id -> sender`. A steer
    /// accepted while a run is in flight goes straight to that run; a steer accepted with no channel
    /// waits in [`SessionState::steer_mailbox`] until the next turn opens one (`sessions.ts:6461-6495`).
    /// This is in-memory only, which is exactly why the mailbox exists.
    pub steer_tx: HashMap<String, mpsc::UnboundedSender<String>>,

    /// Owner questions this session parked or answered, oldest first.
    pub pending_actions: Vec<PendingActionEntry>,
    pub answered_actions: u64,

    /// The created-tool ledger (`ActiveToolRegistry`'s `created` list, `src/harness.ts`), keyed by name
    /// because a name has at most one entry per session.
    pub created_tools: HashMap<String, CreatedTool>,
    /// The ever-incrementing creation count, which stamps each `sourceRevision`.
    pub created_tool_count: u64,
    /// Names this session switched OFF. `disabled` rather than `enabled` because the enabled SET is
    /// derived (`api-reads.ts:803-828 sessionToolRowsOf`) and a disable list is the only part a client
    /// states.
    pub disabled_tools: HashSet<String>,
    /// Per-agent tool allow-lists (`HarnessSnapshot.agentTools`).
    pub agent_tools: BTreeMap<String, Vec<String>>,
    /// `discovery.enabled` (`core/discovery.ts`): off means no searchable tool can be enabled.
    pub discovery_enabled: bool,

    /// The session-scoped agent bindings (`POST /sessions/:id/agents`).
    pub session_agents: BTreeMap<String, SessionAgentEntry>,
    /// Per-agent instruction overrides created IN this session. Keeping a separate map is what makes the
    /// instruction ladder's `session` layer a one-line lookup.
    pub agent_instructions: BTreeMap<String, String>,

    /// The session's task rows, mirrored from the task board so a view read costs nothing
    /// (`api-reads.ts:460 tasks: taskPage.tasks`).
    pub tasks: Vec<TaskRow>,

    /// The credential of a session-scoped provider override, held in memory ONLY
    /// (`sessions.ts:1618-1634 providerConnections`, which is a side map for exactly this reason).
    pub provider_key: Option<String>,

    /// `sessions.ts:1570 expiresAt`: an idle session is dropped once this passes.
    pub expires: u64,
    /// `sessions.ts:1496/1535 legacyPumping`: a session restored from a document that predates the thread
    /// lanes runs its queue through the single legacy pump.
    pub legacy_pumping: bool,

    /// Set when the session is removed while a run is still in flight: the settle step then finds nothing
    /// to write, which is the difference between "the run was cancelled" and "the session is gone"
    /// (`sessions.ts:8513 disposed`).
    pub disposed: bool,
    /// The next moment the pump forces a checkpoint of this session's live events
    /// (`sessions.ts CHECKPOINT_INTERVAL_MS`), so an hour-long run does not lose everything when the
    /// process dies.
    pub checkpoint_at: u64,
}

/// The message a run's task sends when its turn settles.
///
/// It carries the whole outcome because the settle step must run under the store lock; the task must
/// never touch the store while that lock is held elsewhere.
#[derive(Debug, Clone)]
pub struct SettleMessage {
    pub session_id: String,
    pub turn_id: String,
    pub outcome: TurnOutcome,
}

/// The session domain: sessions, tasks, turns, conversations, transcripts, actions, commands, agents and
/// tools.
///
/// Every collaborator is a shared `Arc`, so a `Store` is cheap to clone into a transport state type and
/// can be rebuilt from [`StoreConfig`] alone at boot.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<StoreInner>>,
    hub: Arc<EventHub>,
    config: Arc<StoreConfig>,
    runtime: Arc<dyn ToolRuntime>,
    executor: Arc<dyn TurnExecutor>,
    projector: Arc<dyn ProjectorFactory>,
    /// `task_host` (`harnessService.ts:306`). `None` removes `task` from every tool policy, the same
    /// condition that leaves `task` unregistered in TS.
    task_host: Option<Arc<StoreTaskHost>>,
}

impl Store {
    /// Builds a store over `config` and loads the durable document ONCE (`sessions.ts:1196 initialize`).
    ///
    /// A document that fails validation is NOT fatal: TS logs and starts empty, because one hand-edited
    /// field must not stop a deployment from booting. This crate has no logging dependency, so the
    /// reason is kept in [`Store::diagnostics`] for the transport to surface.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        config: StoreConfig,
        settings: HarnessRuntimeSettings,
        agents: Arc<AgentCatalogue>,
        runtime: Arc<dyn ToolRuntime>,
        executor: Arc<dyn TurnExecutor>,
        projector: Arc<dyn ProjectorFactory>,
        registry: Arc<HarnessRegistry>,
        hub: Arc<EventHub>,
        task_host: Option<Arc<StoreTaskHost>>,
    ) -> Self {
        let host_config = runtime_config_from(&settings);

        let inner = StoreInner {
            host_registry: registry,
            host_agents: agents,
            host_config,
            ..StoreInner::default_state()
        };

        let store = Self {
            inner: Arc::new(Mutex::new(inner)),
            hub,
            config: Arc::new(config),
            runtime,
            executor,
            projector,
            task_host,
        };
        store.restore_all();
        store
    }

    // ------------------------------------------------------------------
    // The host view (registry / agents / settings)
    // ------------------------------------------------------------------

    /// Replaces the store's view of the registry, the catalogue and the deployment settings.
    ///
    /// This is the write half of the host view: a host route that commits a registry row, an agent
    /// overlay or a config patch hands the WHOLE new value over, so the next `POST /sessions`
    /// resolves the new harness, the new instruction override and the new model defaults exactly as
    /// TS's store does when it reads `this.registry` / `this.agents` / `this.globalSettings` at
    /// create time (`sessions.ts:1827`, `api.ts:1974-1989`).
    ///
    /// The caller must NOT hold the store lock (this takes it), and must have already released every
    /// registry / catalogue / settings lock it held — the documented order is host lock -> store
    /// lock, and holding the store lock while taking a host lock is the inversion that deadlocks.
    pub fn set_host_view(
        &self,
        registry: Arc<HarnessRegistry>,
        agents: Arc<AgentCatalogue>,
        config: ServerConfig,
    ) {
        let mut inner = self.lock();

        inner.host_registry = registry;
        inner.host_agents = agents;
        inner.host_config = config;
    }

    // ------------------------------------------------------------------
    // Built-in plugin settings (`GET /plugins`, `PATCH /plugins/{plugin_id}`)
    // ------------------------------------------------------------------

    /// The collection revision every `/plugins` page and patch quotes (`sessions.ts
    /// pluginSettings.revision()`).
    ///
    /// This is NOT a hub counter: `plugins` is not a `StreamResourceKind` in the contract, and
    /// adding one would move the stream's bounded per-scope baseline for a pane that rereads on
    /// open and on save (`api.ts getPlugins`'s note). The EPOCH is still the shared one, so a
    /// client's staleness test holds across both transports.
    #[must_use]
    pub fn plugin_revision(&self) -> u64 {
        self.lock().plugins.revision()
    }

    /// Every built-in this build ships, in catalogue order, with the values in force right now
    /// (`api.ts pluginRowOf` over the manifest's stable order).
    #[must_use]
    pub fn plugin_rows(&self) -> Vec<crate::contract::PluginSummary> {
        self.lock().plugins.rows()
    }

    /// One plugin row, `None` when this build does not ship it.
    #[must_use]
    pub fn plugin_row(&self, plugin: &str) -> Option<crate::contract::PluginSummary> {
        self.lock().plugins.row_of(plugin)
    }

    /// The EFFECTIVE value map of one plugin, resolved through the same rules a row reports
    /// (`plugins.ts values`). An unknown plugin answers an empty map.
    #[must_use]
    pub fn plugin_values(&self, plugin: &str) -> Map<String, Value> {
        self.lock().plugins.values(plugin)
    }

    /// The durable projection of the operator's overrides - the `pluginSettings` member this store
    /// writes into `state.json` (`plugins.ts snapshot`).
    #[must_use]
    pub fn plugin_snapshot(&self) -> crate::persistence::HarnessPluginSettingsRecord {
        self.lock().plugins.snapshot()
    }

    /// Validate and apply one plugin's settings patch, in the shape `sessions.ts
    /// updatePluginSettings` uses MINUS the commit, which this crate's host write owns.
    ///
    /// Validation is the store's decision and it refuses BEFORE anything mutates, so a rejected
    /// patch is a no-op rather than a partial write. On success the in-memory value is applied and
    /// the counter has moved; the caller makes it durable through
    /// [`crate::runtime::HarnessRuntime::save`]. A FAILED write leaves the value applied on
    /// purpose (`sessions.ts:1848-1851`): every running session was handed the new value the moment
    /// it was applied, so rolling it back would tell the operator "the change failed" while the
    /// process kept it anyway.
    ///
    /// Takes the store lock and releases it before returning, so the caller may take a host lock
    /// afterwards - the documented order is host lock -> store lock, never the reverse.
    ///
    /// # Errors
    ///
    /// 404 for a plugin this build does not ship, and 400 for an empty patch, an unknown key, or an
    /// out-of-bounds / mistyped value. Every sentence comes from [`crate::plugins`].
    ///
    /// [`ApiError::not_found`], [`ApiError::bad_request`].
    pub fn update_plugin_settings(
        &self,
        plugin: &str,
        patch: &Map<String, Value>,
    ) -> Result<(), ApiError> {
        let mut inner = self.lock();

        inner.plugins.update(plugin, patch)
    }

    /// The store's own view of the registry, for a caller that holds no lock (`sessions.ts:1827`).
    #[must_use]
    pub fn registry(&self) -> Arc<HarnessRegistry> {
        Arc::clone(&self.lock().host_registry)
    }

    /// The store's own view of the catalogue, for a caller that holds no lock.
    #[must_use]
    pub fn agents(&self) -> Arc<AgentCatalogue> {
        Arc::clone(&self.lock().host_agents)
    }

    /// The provider trio currently in force, for a caller that holds no lock.
    ///
    /// NEVER call this while holding the store guard: the guard is not reentrant, and these three
    /// accessors take it again. The locked paths read the view through `StoreInner::registry`,
    /// `StoreInner::agents` and `StoreInner::settings` instead.
    #[must_use]
    pub fn settings(&self) -> HarnessRuntimeSettings {
        self.lock().settings()
    }

    /// `sessions.ts:8638 dispose()`: stop claiming new work, so a graceful shutdown drains.
    ///
    /// The flag is what makes [`SessionStore::spawn_pump`] and the pump's own claim loop answer
    /// "nothing more"; live runs keep their cancellation tokens, and the durable document is written
    /// by `server::flush` on the way out.
    pub fn begin_shutdown(&self) {
        self.lock().shutting_down = true;
    }

    /// Whether the store has been told to stop claiming work.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.lock().shutting_down
    }

    // ------------------------------------------------------------------
    // Collaborator access (the transport reads these, never `inner`)
    // ------------------------------------------------------------------

    #[must_use]
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    #[must_use]
    pub fn hub(&self) -> &EventHub {
        &self.hub
    }

    /// The lock, shared with a collaborator that has to call back in (a run's action host). Cloning this
    /// `Arc` is what avoids an ownership cycle: store -> run -> host -> store.
    #[must_use]
    pub fn inner_arc(&self) -> Arc<Mutex<StoreInner>> {
        Arc::clone(&self.inner)
    }

    #[must_use]
    pub fn hub_arc(&self) -> Arc<EventHub> {
        Arc::clone(&self.hub)
    }

    /// This process' epoch, verbatim (`api-events.ts:282`).
    #[must_use]
    pub fn epoch(&self) -> String {
        self.hub.epoch().to_owned()
    }

    #[must_use]
    pub fn runtime(&self) -> &Arc<dyn ToolRuntime> {
        &self.runtime
    }

    #[must_use]
    pub fn projector_factory(&self) -> &Arc<dyn ProjectorFactory> {
        &self.projector
    }

    #[must_use]
    pub fn task_host(&self) -> Option<&Arc<StoreTaskHost>> {
        self.task_host.as_ref()
    }

    /// The provider trio the NEXT turn runs with, session override first
    /// (`api-reads.ts:1078-1093 providerSnapshot`).
    ///
    /// Takes the guard: the deployment trio is part of the host view, and the recorder reaches this
    /// while it already holds the store lock.
    #[must_use]
    pub fn provider_of(&self, inner: &StoreInner, state: &SessionState) -> ProviderConfig {
        read::effective_provider(&inner.settings(), state.settings_provider().as_ref())
    }

    /// `shared/runtime.ts:109 configuredProviderHost`: the `host[:port]` the process credential is
    /// scoped to. This is what makes a restart on a DIFFERENT base URL discard the stored key instead of
    /// sending it somewhere else.
    #[must_use]
    pub fn provider_host(&self) -> Option<String> {
        read::provider_host_of(&self.settings().base_url)
    }

    /// The working directory a tool body runs in.
    ///
    /// TS runs tool bodies with the SERVER PROCESS cwd, and the harness service is started from the repo
    /// root it serves (`packages/underload/service/server.ts`); a self-hosted deployment keeps its scratch
    /// tree in `<root>/workspace`, which is therefore preferred when it exists. The DATABASE root is
    /// never the answer — it holds `harness/state.json`, not the project a task edits. See knowledge.md
    /// "Decisions".
    #[must_use]
    pub fn cwd(&self) -> PathBuf {
        let root = crate::package_root::repo_root();
        let workspace = root.join("workspace");
        if workspace.is_dir() { workspace } else { root }
    }

    // ------------------------------------------------------------------
    // Locking, revisions, publishing
    // ------------------------------------------------------------------

    /// Takes the store lock, recovering from a poisoned mutex the way the hub does
    /// (`events.rs:216-218`): no store method mutates and then panics mid-way, so a panic elsewhere cannot
    /// leave a half-applied session behind.
    pub fn lock(&self) -> MutexGuard<'_, StoreInner> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Why a boot was partial (`sessions.ts` logs these; this crate reports them).
    pub fn diagnostics(&self) -> Vec<String> {
        self.lock().diagnostics.clone()
    }

    /// `api-events.ts:271 bumpRevision` over a set of resources, answering with the SESSION's counter —
    /// the figure every session-scoped page, patch and snapshot reports.
    #[must_use]
    pub fn bump(&self, session_id: &str, targets: &[(&str, Option<&str>)]) -> u64 {
        let mut session_revision = self.hub.revision("session", Some(session_id));
        for (resource, id) in targets {
            let next = self.hub.bump(resource, *id);
            if *resource == "session" {
                session_revision = next;
            }
        }
        session_revision
    }

    /// Bumps the session counter alone (`sessions.ts:2254 touch` with no publish): what a route that
    /// mutated state WITHOUT a dedicated stream signal reports.
    #[must_use]
    pub fn bump_session(&self, session_id: &str) -> u64 {
        self.hub.bump("session", Some(session_id))
    }

    /// The session's current revision, without touching it (`api-events.ts:269 read`).
    #[must_use]
    pub fn revision_of(&self, session_id: &str) -> u64 {
        self.hub.revision("session", Some(session_id))
    }

    /// One conversation's transcript revision (`hub.revision('transcript', threadKey)`). The hub keys
    /// a conversation as `session:thread` (`api.ts` `threadKey`), so the session id is part of the
    /// address - a bare thread id is a DIFFERENT counter, which is why this takes both.
    #[must_use]
    pub fn transcript_revision_of(&self, session_id: &str, thread_id: &str) -> u64 {
        self.hub
            .revision("transcript", Some(&thread_key(session_id, thread_id)))
    }

    /// `api-events.ts:299 publish`, AFTER the caller's bumps: report-only, fanned out synchronously.
    #[allow(clippy::too_many_arguments)]
    pub fn publish(
        &self,
        kind: &str,
        targets: &[(&str, Option<&str>)],
        session_id: &str,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        payload: Value,
    ) {
        self.publish_ops(
            kind,
            targets,
            session_id,
            thread_id,
            turn_id,
            payload,
            Vec::new(),
        );
    }

    /// The same, with compiled display ops (`display.ops`, `api-events.ts:320`).
    #[allow(clippy::too_many_arguments)]
    pub fn publish_ops(
        &self,
        kind: &str,
        targets: &[(&str, Option<&str>)],
        session_id: &str,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        payload: Value,
        ops: Vec<Value>,
    ) {
        use crate::events::ResourceRef;
        let resources: Vec<ResourceRef> = targets
            .iter()
            .map(|(resource, id)| ResourceRef::new(*resource, *id))
            .collect();
        // The fan-out's return value is the hub's own newest-revision figure; a store publish reports
        // nothing back (`events.rs:288`), so it is dropped here rather than at every call site.
        let _ = self.hub.publish_with_ops(
            kind,
            &resources,
            Some(session_id),
            thread_id,
            turn_id,
            payload,
            ops,
        );
    }

    /// Applies one mutation's intent: save the document (the caller is still inside the guard, which is
    /// the point), then bump, then publish each event in order.
    ///
    /// The snapshots are reconciled on the way out (`SessionState::durable_record`), so `inner` is only
    /// ever READ here: a mutation that still holds a borrow out of the guard can commit without ending it.
    ///
    /// A failed save returns the error WITHOUT publishing, which is what makes a refused write invisible
    /// to subscribers; the caller then rolls its state back.
    pub fn commit(
        &self,
        inner: &StoreInner,
        session_id: &str,
        outcome: &mut CommitOutcome,
    ) -> StoreResult<u64> {
        if self.config.persist {
            let database = self.database_of(inner);
            crate::persistence::write_harness_database(
                &self.config.root,
                &database,
                &Self::write_owner(),
            )?;
        }

        let bumps: Vec<(&str, Option<&str>)> = outcome
            .bumps
            .iter()
            .map(|(resource, id)| (resource.as_str(), id.as_deref()))
            .collect();

        if !bumps.is_empty() {
            let revision = self.bump(session_id, &bumps);
            if outcome.revision == 0 {
                // A mutation that only bumps the session counter states its revision up front; one that
                // bumps children still reports the SESSION's figure, which is what the contract's
                // `revision` members mean (`harness-api.ts:830`).
                outcome.revision = revision;
            }
        }

        for emit in &outcome.events {
            let targets: Vec<(&str, Option<&str>)> = emit
                .targets
                .iter()
                .map(|(resource, id)| (resource.as_str(), id.as_deref()))
                .collect();
            self.publish_ops(
                &emit.kind,
                &targets,
                session_id,
                emit.thread_id.as_deref(),
                emit.turn_id.as_deref(),
                emit.payload.clone(),
                emit.ops.clone(),
            );
        }

        Ok(outcome.revision)
    }

    /// The `owner` stamp of an atomic write (`persistence.rs:1419-1422`): the process id. Two processes
    /// sharing a root get different staging names, which the bounded retry inside `write_file_atomic`
    /// relies on.
    #[must_use]
    pub fn write_owner() -> String {
        std::process::id().to_string()
    }

    /// Serializes every live session and commits the document (`sessions.ts:1233 save`).
    pub fn save(&self, inner: &StoreInner) -> StoreResult<()> {
        if !self.config.persist {
            return Ok(());
        }
        let database = self.database_of(inner);
        crate::persistence::write_harness_database(
            &self.config.root,
            &database,
            &Self::write_owner(),
        )
    }

    /// Assembles the durable document from the live sessions (`persistence.ts:1619`), including the
    /// registry rows the store's frozen copies describe.
    #[must_use]
    pub fn database_of(&self, inner: &StoreInner) -> HarnessDatabase {
        let mut registry: Vec<HarnessPresetRecord> = Vec::new();
        for state in inner.sessions.values() {
            let record = state.preset_record();
            if !registry.iter().any(|seen| seen.name == record.name) {
                registry.push(record);
            }
        }

        HarnessDatabase {
            version: HARNESS_DATABASE_VERSION,
            saved_at: crate::events::now_millis(),
            registry,
            deleted: None,
            global_settings: None,
            // Only what the operator changed, and only when they changed something: an empty
            // snapshot stays ABSENT so a deployment that never opened the Plugins pane keeps its
            // exact previous document (`sessions.ts:2758-2766`).
            plugin_settings: {
                let snapshot = inner.plugins.snapshot();

                (!snapshot.is_empty()).then_some(snapshot)
            },
            sessions: inner
                .sessions
                .values()
                .map(SessionState::durable_record)
                .collect(),
            extra: Map::new(),
        }
    }

    /// Loads the document and restores every session it holds (`sessions.ts:1196-1231`).
    fn restore_all(&self) {
        let database = match crate::persistence::read_harness_database(&self.config.root) {
            Ok(Some(database)) => database,
            // An absent document is a first boot, not a failure.
            Ok(None) => return,
            Err(error) => {
                self.lock().diagnostics.push(format!(
                    "harness state could not be loaded: {}",
                    error.message
                ));
                return;
            }
        };

        let mut inner = self.lock();

        // The persisted document IS the seed: a restored process reads back exactly what the
        // operator last saved BEFORE its first session is built (`sessions.ts:1510-1516` hands the
        // loaded `database.pluginSettings` to `createPluginSettingsStore`). No revision bump - a
        // boot is not a write.
        inner.plugins.restore(database.plugin_settings.as_ref());

        for record in database.sessions {
            let session_id = record.id.clone();
            if inner.find(&session_id).is_some() {
                continue;
            }
            match self.restore_session(record) {
                Ok(state) => {
                    inner.insert(state);
                    // The journal opens AFTER the state exists: a session whose log cannot be opened
                    // still runs, and reports the degradation (`sessions.ts:1121-1131`).
                    match HarnessJournal::open(&self.config.sessions_dir(), &session_id, None) {
                        Ok(journal) => {
                            inner.journals.insert(session_id, journal);
                        }
                        Err(error) => {
                            inner.diagnostics.push(format!(
                                "journal for session \"{session_id}\" is unavailable: {}",
                                error.message
                            ));
                        }
                    }
                }
                Err(error) => {
                    // A session this build cannot run (its harness was renamed or deleted) is SKIPPED and
                    // stays on disk — `sessions.ts:1207`.
                    inner
                        .diagnostics
                        .push(format!("session skipped at restore: {}", error.message));
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Sessions: lookup and identity
    // ------------------------------------------------------------------

    /// Looks a session up by id without creating it (`sessions.ts:984 findSession`).
    ///
    /// The returned [`SessionRef`] derefs to the session while keeping the store lock, which is what makes
    /// it impossible to read a session without holding it. `MutexGuard::map` would be the one-line answer,
    /// but that API is still unstable on this toolchain, so the re-lookup happens per deref instead: the
    /// session cannot move or vanish while the guard is held, so the lookup cannot fail.
    pub fn find_session(&self, session_id: &str) -> StoreResult<SessionRef<'_>> {
        let guard = self.lock();
        if guard.find(session_id).is_none() {
            return Err(session_not_found(session_id));
        }

        Ok(SessionRef {
            guard,
            session_id: session_id.to_owned(),
        })
    }

    /// The refusal every session-scoped route answers with (`sessions.ts:989`), byte-identical.
    #[must_use]
    pub fn unknown_session(&self, session_id: &str) -> ApiError {
        session_not_found(session_id)
    }

    // ------------------------------------------------------------------
    // Paging
    // ------------------------------------------------------------------

    /// The `limit` of a paged read (`api-paging.ts:97 resolveLimit`), which owns the message texts.
    pub fn limit_of(&self, query: &Query) -> StoreResult<u64> {
        crate::paging::resolve_limit(query.get("limit").map(String::as_str))
    }

    /// The `cursor` of a paged read (`api-paging.ts:63 decodeCursor`): an ordinal that must carry this
    /// process' epoch, so a cursor from a previous process never silently re-bases.
    pub fn cursor_of(&self, query: &Query) -> StoreResult<u64> {
        crate::paging::decode_cursor(self.hub.epoch(), query.get("cursor").map(String::as_str))
    }

    /// The `PageMeta` of one served slice (`api-paging.ts:142 pageMeta`).
    #[must_use]
    pub fn page_meta(&self, revision: u64, next: Option<u64>, items_len: usize) -> PageMeta {
        PageMeta {
            limit: items_len as u64,
            next_cursor: next
                .map(|ordinal| crate::paging::encode_cursor(self.hub.epoch(), ordinal)),
            observed_at: crate::events::now_millis(),
            revision,
        }
    }

    /// The `limit`/`cursor` pair every paged route starts from.
    pub fn paging(&self, query: &Query) -> StoreResult<(u64, u64)> {
        Ok((self.limit_of(query)?, self.cursor_of(query)?))
    }
}

/// The reconciled, durable view of one session.
impl SessionState {
    /// The session id (`record.id`).
    #[must_use]
    pub fn id(&self) -> &str {
        &self.record.id
    }

    /// The harness this session runs (`record.preset.name`).
    #[must_use]
    pub fn harness(&self) -> &str {
        &self.record.preset.name
    }

    #[must_use]
    pub fn turn_ids(&self) -> Vec<String> {
        self.turn_order.clone()
    }

    #[must_use]
    pub fn turn(&self, turn_id: &str) -> Option<&TurnState> {
        self.turns.get(turn_id)
    }

    #[must_use]
    pub fn turn_mut(&mut self, turn_id: &str) -> Option<&mut TurnState> {
        self.turns.get_mut(turn_id)
    }

    /// Every conversation id in creation order.
    #[must_use]
    pub fn thread_ids(&self) -> Vec<String> {
        self.thread_order.clone()
    }

    #[must_use]
    pub fn thread(&self, thread_id: &str) -> Option<&HarnessThreadRecord> {
        self.threads.get(thread_id)
    }

    /// The conversation's CURRENT context-lane counter (`sessions.ts:2745 contextLaneOf`).
    ///
    /// `None` means lane 0 — absence already says it, and [`crate::persistence::LaneStamp`] turns a 0
    /// back into absence on the way out, so a session that never compacted writes no lane key anywhere.
    ///
    /// The recorder calls this LIVE, once per event (`sessions.ts:7925-7936`) and once again when a turn
    /// is claimed (`:8871-8874`), never cached at turn start: a compaction that lands mid-run has to
    /// take effect on the very next event, which is the only way one turn's context bar can show both
    /// rows. The counter itself is durable (`persistence::HarnessThreadRecord::context_lane`, written
    /// back into `core.threads[].contextLane`), which is what lets a restarted conversation keep its
    /// rows stacked where the operator left them.
    #[must_use]
    pub fn context_lane_of(&self, thread_id: Option<&str>) -> Option<u64> {
        thread_id
            .and_then(|thread_id| self.threads.get(thread_id))
            .and_then(|thread| thread.context_lane)
            .filter(|lane| *lane != 0)
    }

    /// Everything the projector needs that is NOT in the event stream, for one turn
    /// (`createTurnProjector`'s `DisplayProjectContext`, `sessions.ts:4160-4240`).
    ///
    /// The three non-event segments — the one `system` block, the `user` prompt block, and every
    /// `steer` block — are minted from session state (`turn-projector.ts:1543 syncRecordBlocks`,
    /// `:1609 syncSteerBlocks`), so a fold that started from the events alone would rebuild a context
    /// bar missing them after a restart. The section identity comes from the snapshot the session
    /// committed; the instruction sources from the per-turn map T1 persists.
    #[must_use]
    pub fn projector_seed(&self, record: &SessionTurn) -> ProjectorSeed {
        let mut seed = seed_from_turn_record(record);
        let core = self.record.core.as_object();

        // The conversation's SECTION, which is what a block id is prefixed with
        // (`blockIdOf(record.ordinal, ...)`, `turn-projector.ts:1407`). A conversation that has no
        // section record yet takes the ordinal its section WOULD have, so the ids a live run mints and
        // the ids a rebuild mints after the section appears still line up.
        let sections = core
            .and_then(|core| core.get("sections"))
            .and_then(Value::as_array);

        if let (Some(thread_id), Some(sections)) = (record.thread_id.as_deref(), sections) {
            let section = sections
                .iter()
                .find(|section| section.get("threadId").and_then(Value::as_str) == Some(thread_id))
                .or_else(|| {
                    // A legacy snapshot keyed its section by the composite id rather than a threadId
                    // (`{sessionId}-{threadId}`, the same spelling the store writes).
                    let composite = format!("{}-{thread_id}", self.record.id);

                    sections.iter().find(|section| {
                        section.get("id").and_then(Value::as_str) == Some(composite.as_str())
                    })
                });

            match section {
                Some(section) => {
                    seed.section_ordinal = section
                        .get("ordinal")
                        .and_then(Value::as_u64)
                        .unwrap_or_else(|| {
                            sections
                                .iter()
                                .position(|entry| std::ptr::eq(entry, section))
                                .map_or(0, |position| position as u64)
                        });
                    seed.section_id = section.get("id").and_then(Value::as_str).map(str::to_owned);
                }
                None => seed.section_ordinal = sections.len() as u64,
            }

            // The call id of the `user` segment is `transcript-${messageId}` of the conversation's
            // FIRST user entry (`:1578`) — the entry the block detail route resolves to answer "what
            // did the operator actually type here".
            seed.user_message_id = self
                .thread(thread_id)
                .and_then(|thread| {
                    thread.messages.iter().find(|message| {
                        matches!(message, crate::contract::HarnessMessage::User { .. })
                    })
                })
                .and_then(|message| match message {
                    crate::contract::HarnessMessage::User { message_id, .. } => {
                        message_id.clone().filter(|id| !id.is_empty())
                    }
                    _ => None,
                });
        }

        // The system instructions THIS turn's agents ran with (`turnInstructionSources[turnId]`). An
        // absent entry means "no recorded source", and the fold then mints no `system` segment at all
        // rather than inventing one from the live registry, which may have changed since.
        seed.instruction_sources = self
            .record
            .turn_instruction_sources
            .as_ref()
            .and_then(|sources| sources.get(&record.id))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        seed
    }

    #[must_use]
    pub fn thread_mut(&mut self, thread_id: &str) -> Option<&mut HarnessThreadRecord> {
        self.threads.get_mut(thread_id)
    }

    /// The label of one conversation (`record.threadLabels`).
    #[must_use]
    pub fn thread_label_of(&self, thread_id: &str) -> Option<&str> {
        self.thread_labels.get(thread_id).map(String::as_str)
    }

    #[must_use]
    pub fn active_turn_ids(&self) -> Vec<String> {
        self.active.iter().map(|run| run.turn_id.clone()).collect()
    }

    #[must_use]
    pub fn active_run_of(&self, thread_id: &str) -> Option<&ActiveRun> {
        self.active.iter().find(|run| run.thread_id == thread_id)
    }

    /// The questions still waiting on an answer.
    #[must_use]
    pub fn live_actions(&self) -> Vec<&PendingActionEntry> {
        self.pending_actions
            .iter()
            .filter(|entry| entry.is_live())
            .collect()
    }

    /// Where one toolbox row comes from (`api-reads.ts:816-827 sessionToolRowsOf`).
    #[must_use]
    pub fn tool_origin_of(&self, name: &str) -> SessionToolOrigin {
        if self.created_tools.contains_key(name) {
            SessionToolOrigin::Created
        } else if self
            .agent_tools
            .values()
            .any(|names| names.iter().any(|tool| tool == name))
            && !self.disabled_tools.contains(name)
        {
            SessionToolOrigin::Discovered
        } else {
            SessionToolOrigin::Catalogue
        }
    }

    /// The live per-agent model map (`record.agentModels`).
    #[must_use]
    pub fn agent_models_map(&self) -> BTreeMap<String, String> {
        self.record.agent_models.clone().unwrap_or_default()
    }

    /// Records one live per-agent model (`PATCH /sessions/:id` writes this map).
    pub fn set_agent_model(&mut self, agent: &str, model: &str) {
        let mut models = self.agent_models_map();
        models.insert(agent.to_owned(), model.to_owned());
        self.record.agent_models = Some(models);
    }

    /// The session's provider override WITHOUT its credential (`record.settings`).
    ///
    /// An empty `baseURL` means "no session override", which is TS's own test (`api-reads.ts:1081`): the
    /// deployment settings then answer alone.
    #[must_use]
    pub fn settings_provider(&self) -> Option<ProviderConfig> {
        if self.record.settings.base_url.trim().is_empty() {
            return None;
        }
        Some(ProviderConfig {
            base_url: self.record.settings.base_url.clone(),
            api_key: self.provider_key.clone(),
            model: self.record.settings.model.clone(),
            headers: BTreeMap::new(),
            // The record has no sampling column (`persistence.ts:134-137` is a base URL and a model),
            // so the session states no override and the deployment default applies
            // (`read::default_sampling`).
            sampling: None,
        })
    }

    /// States (or clears) the session's provider override. The credential lands on
    /// [`SessionState::provider_key`], which the record never sees.
    pub fn set_settings_provider(&mut self, provider: Option<ProviderConfig>) {
        match provider {
            Some(provider) => {
                self.record.settings = HarnessSessionSettingsRecord {
                    base_url: provider.base_url,
                    model: provider.model,
                    extra: Map::new(),
                };
                self.provider_key = provider.api_key;
            }
            None => {
                self.record.settings = HarnessSessionSettingsRecord::default();
                self.provider_key = None;
            }
        }
    }

    /// Writes the live runtime state back into the record: its columns, and its snapshot envelope.
    ///
    /// Called by [`Store::commit`] for every session before serialization, and read in reverse by
    /// [`Store::restore_session`]. It is the only place the two halves meet, which is what keeps the
    /// document honest.
    fn write_core_into(&self, record: &mut HarnessSessionRecord) {
        let mut core = record.core.as_object().cloned().unwrap_or_default();

        core.insert(
            "activeThreadId".into(),
            self.active_thread
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        core.insert(
            "threads".into(),
            serde_json::to_value(self.thread_records_in_order()).unwrap_or(Value::Null),
        );
        core.insert("threadSequence".into(), Value::from(self.thread_seq));
        core.insert(
            "createdTools".into(),
            serde_json::to_value(self.created_tools_in_order()).unwrap_or(Value::Null),
        );
        core.insert(
            "createdToolCount".into(),
            Value::from(self.created_tool_count),
        );
        let mut disabled: Vec<String> = self.disabled_tools.iter().cloned().collect();
        disabled.sort();
        core.insert(
            "disabledTools".into(),
            Value::Array(disabled.into_iter().map(Value::String).collect()),
        );
        core.insert(
            "agentTools".into(),
            serde_json::to_value(&self.agent_tools).unwrap_or(Value::Null),
        );
        let mut discovery = Map::new();
        discovery.insert("enabled".into(), Value::Bool(self.discovery_enabled));
        core.insert("discovery".into(), Value::Object(discovery));
        core.insert(
            "taskRows".into(),
            serde_json::to_value(&self.tasks).unwrap_or(Value::Null),
        );
        core.insert(
            CORE_SESSION_AGENTS.into(),
            serde_json::to_value(&self.session_agents).unwrap_or(Value::Null),
        );
        core.insert(
            CORE_STEER_MAILBOX.into(),
            serde_json::to_value(&self.steer_mailbox).unwrap_or(Value::Null),
        );
        core.insert("steerActive".into(), Value::from(self.steer_active));
        record.core = Value::Object(core);

        // The record's own columns, refreshed from the live maps.
        record.turns = self
            .turn_order
            .iter()
            .filter_map(|id| self.turns.get(id))
            .map(|turn| turn.record.clone())
            .collect();
        // The durable backlog is the WHOLE pending set, not just the legacy array. TS keeps ONE
        // `session.queue` array and lets each lane reference entries of it
        // (`sessions.ts:5947-5953`), which is why its restore can re-home a queued turn into its lane
        // (`:3202-3228`, F1). Writing only `self.queue` here made a lane's backlog memory-only, so a
        // restart dropped the ordering guarantee and a fresh submit overtook work the user queued first.
        //
        // The union is stamped in TURN INDEX order: `index` is minted at enqueue (`turns.rs:197`), so the
        // document states the backlog in the order the pump claims it, and `restore_session`'s per-lane
        // split preserves it. A lane's ACTIVE turn is already out of its queue (`claim_next_turn`), so no
        // id appears twice.
        let mut backlog: Vec<String> = self.queue.clone();

        for lane in self.lanes.values() {
            backlog.extend(lane.queue.iter().cloned());
        }

        backlog.sort_by_key(|turn_id| {
            self.turns
                .get(turn_id)
                .map(|turn| turn.record.index)
                .unwrap_or(u64::MAX)
        });
        record.queue = backlog;
        record.thread_sequence = Some(self.turn_seq);
        record.thread_labels = (!self.thread_labels.is_empty()).then(|| self.thread_labels.clone());
        record.thread_label_high_water =
            (!self.label_high_water.is_empty()).then(|| self.label_high_water.clone());
        // The same high-water in the shape T1 persists it (`labelHighWater` is a JSON value, while
        // `threadLabelHighWater` is the typed map).
        record.label_high_water = record
            .thread_label_high_water
            .clone()
            .map(|water| serde_json::to_value(water).unwrap_or(Value::Null));
        record.invalid_threads = (!self.invalid_threads.is_empty()).then(|| {
            self.invalid_threads
                .iter()
                .map(|(id, entry)| (id.clone(), entry.clone()))
                .collect()
        });
        let invocations = self.turn_invocations();
        record.turn_invocations = (!invocations.is_empty()).then_some(invocations);
        let invocation_threads = self.invocation_threads();
        record.invocation_threads = (!invocation_threads.is_empty()).then_some(invocation_threads);
        let instructions = self.turn_instruction_sources();
        record.turn_instruction_sources = (!instructions.is_empty())
            .then(|| serde_json::to_value(instructions).unwrap_or(Value::Null));
        record.session_agents = (!self.session_agents.is_empty())
            .then(|| self.session_agents.keys().cloned().collect());
    }

    /// Folds every live map into this session's own record (`sessions.ts:3885 sessionRecordOf`).
    ///
    /// The store mutates the live maps directly, so this is the one place the two halves meet; running
    /// it before serialization is what makes the document a faithful picture of the machine.
    pub fn sync_core(&mut self) {
        let mut record = self.record.clone();
        self.write_core_into(&mut record);
        self.record = record;
    }

    /// turn id -> the invocation id its root run reported (`record.turnInvocations`).
    #[must_use]
    pub fn turn_invocations(&self) -> BTreeMap<String, String> {
        self.turn_order
            .iter()
            .filter_map(|id| self.turns.get(id))
            .filter_map(|turn| {
                Some((
                    turn.record.id.clone(),
                    turn.record
                        .extra
                        .get(EXTRA_INVOCATION)?
                        .as_str()?
                        .to_owned(),
                ))
            })
            .collect()
    }

    /// invocation id -> the conversation it ran in (`record.invocationThreads`).
    #[must_use]
    pub fn invocation_threads(&self) -> BTreeMap<String, String> {
        self.threads
            .iter()
            .flat_map(|(id, thread)| {
                thread
                    .invocation_ids
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|invocation| (invocation, id.clone()))
            })
            .collect()
    }

    /// turn id -> the system instructions that turn ran with (`record.turnInstructionSources`).
    #[must_use]
    pub fn turn_instruction_sources(&self) -> BTreeMap<String, Vec<Value>> {
        self.turn_order
            .iter()
            .filter_map(|id| self.turns.get(id))
            .filter(|turn| !turn.instruction_sources.is_empty())
            .map(|turn| (turn.record.id.clone(), turn.instruction_sources.clone()))
            .collect()
    }

    /// The durable copy of this session: `record` with every live map reconciled into it.
    ///
    /// Takes the undo point a mutation rolls back to (`Store::snapshot_state`) and is what
    /// [`Store::database_of`] serializes, both without needing a `Clone` of the live state.
    #[must_use]
    pub fn durable_record(&self) -> HarnessSessionRecord {
        let mut record = self.record.clone();
        self.write_core_into(&mut record);
        record
    }

    /// The frozen registry row of this session (`record.preset`).
    #[must_use]
    pub fn preset_record(&self) -> HarnessPresetRecord {
        crate::registry::definition_from_descriptor_record(&self.record.definition).to_record()
    }

    /// The roster in creation order.
    #[must_use]
    pub fn thread_records_in_order(&self) -> Vec<&HarnessThreadRecord> {
        self.thread_order
            .iter()
            .filter_map(|id| self.threads.get(id))
            .collect()
    }

    /// The created-tool ledger in creation order.
    #[must_use]
    pub fn created_tools_in_order(&self) -> Vec<&CreatedTool> {
        let mut tools: Vec<&CreatedTool> = self.created_tools.values().collect();
        tools.sort_by_key(|tool| (tool.created_at, tool.name.clone()));
        tools
    }

    /// The task rows this session reports (`api-reads.ts:460`).
    #[must_use]
    pub fn task_rows(&self) -> Vec<TaskRow> {
        self.tasks.clone()
    }

    /// Appends one plugin diagnostic to this session's journal and NOTHING else
    /// (`notePluginDiagnostic`, `sessions.ts:3252-3290`).
    ///
    /// TWO jobs, in TS's order: journal it (bounded, durable, and projected onto the identified session
    /// read, which is the observable surface a broken plugin was missing), then log the three FRAMEWORK
    /// kinds to stderr so an operator tailing the server sees the same thing with the plugin NAMED. A
    /// `policy` report is deliberately not logged: that is a plugin saying what it DECIDED, information a
    /// reader opens the session for, not a problem to print on every turn (`LOGGED_DIAGNOSTIC_KINDS`,
    /// `:3222-3230`).
    ///
    /// What this does NOT do is schedule a save: the journal rides the NEXT state-change save, exactly like
    /// `invalid_threads` does, because a diagnostic is an observation rather than a transition - and a
    /// chatty or failing plugin must not drive the whole-document save rate the persistence layer was built
    /// around (`:3241-3247`).
    ///
    /// It can never fail a run in either direction (the framework's own fail-open rule): every step here is
    /// total, an unnamed owner stays `''`, and an empty message becomes a stated placeholder instead of
    /// being dropped.
    pub fn note_plugin_diagnostic(
        &mut self,
        entry: crate::contract::SessionPluginDiagnostic,
        at: u64,
    ) {
        // The writer's own normalization first (`:3253-3255`). The sink, not the reporter, owns the
        // placeholder: a plugin that failed without a word still has to be readable after the fact.
        let owner = entry.owner.clone();
        let message = if entry.message.trim().is_empty() {
            "no message".to_owned()
        } else {
            entry.message.clone()
        };

        // `at` arrives from the reporter's clock so a persistence sink stays deterministic under test; the
        // entry's own stamp wins when it carries one, because that is the figure the reporter measured.
        let stamped = if entry.at == 0 { at } else { entry.at };

        // The entry as the JOURNAL will hold it: same normalized owner, same placeholder message, same
        // stamp. The log line is composed from this value rather than from the caller's, so the operator
        // reads exactly what the document says.
        let stamped_entry = crate::contract::SessionPluginDiagnostic {
            at: stamped,
            kind: entry.kind,
            owner: owner.clone(),
            event: entry.event.clone(),
            message: message.clone(),
            data: entry.data.clone(),
        };

        // Key order is the WRITE order (`:3256-3264`): `at, kind, owner, [event], message, [data]`. An
        // absent `event` stays absent - a `policy` report is not tied to one hook, and inventing one would
        // claim a failure that did not happen. The restore path re-reads these entries in the reader's
        // order (`persistence::normalize_plugin_diagnostics`), exactly as TS's does, and every save
        // rewrites the document, so the two orders never mix on disk.
        let mut value = Map::new();

        value.insert("at".to_owned(), serde_json::json!(stamped));
        value.insert("kind".to_owned(), serde_json::json!(entry.kind));
        value.insert("owner".to_owned(), serde_json::json!(owner));

        if let Some(event) = entry.event.as_deref() {
            value.insert("event".to_owned(), serde_json::json!(event));
        }

        value.insert("message".to_owned(), serde_json::json!(message));

        if let Some(data) = entry.data.as_ref() {
            value.insert("data".to_owned(), Value::Object(data.clone()));
        }

        // The kinds worth printing, decided off the enum rather than off the serialized spelling, and
        // through the SAME predicate a test pins - so a renamed variant cannot silently stop being logged.
        let logged = plugin_diagnostic_is_failure(entry.kind);

        let journal = self
            .record
            .plugin_diagnostics
            .get_or_insert_with(Vec::new);

        journal.push(Value::Object(value));

        // Bounded, oldest first (`:3268-3272`): the newest entries are the ones that explain a live
        // problem, and a plugin that fails every turn must not grow `state.json` without limit.
        if journal.len() > crate::persistence::MAX_PLUGIN_DIAGNOSTICS {
            let dropped = journal.len() - crate::persistence::MAX_PLUGIN_DIAGNOSTICS;

            journal.drain(0..dropped);
        }

        if !logged {
            return;
        }

        // The stderr line, TS's shape verbatim (`:3279`); `console.error` is this crate's `eprintln!`. The
        // text itself is built by `plugin_diagnostic_line` so the sentence is assertable without owning a
        // process, and so a host that mirrors these lines somewhere else prints the SAME one.
        eprintln!("{}", plugin_diagnostic_line(&self.record.id, &stamped_entry));
    }

    /// `sessions.ts:2254 touch`: any activity moves `updatedAt` and re-arms the idle expiry, so a busy
    /// session is never collected while an idle one is.
    ///
    /// `idle_ms` comes from the CALLER (`Store::config().idle_ms`): the window is a deployment setting, and
    /// a session that was restored under a different deployment keeps that deployment's figure until its
    /// next touch.
    pub fn touch_idle(&mut self, at: u64, idle_ms: u64) {
        self.record.updated_at = at;
        self.expires = at.saturating_add(idle_ms);
    }

    /// The moment this session becomes collectable, which is what an evict pass compares against.
    #[must_use]
    pub fn is_expired(&self, at: u64) -> bool {
        self.expires != 0 && at >= self.expires
    }
}

/// The operator-facing line one framework diagnostic prints (`sessions.ts:3279`'s `console.error`), built
/// as a string so it can be asserted without owning a process and printed identically by any sink.
///
/// The two spelled-out placeholders are TypeScript's, not this port's invention: an unnamed owner prints
/// `(no owner)` and a report that names no hook prints `no event`, because a line missing either figure
/// is unreadable in a log scrape. The separator is an EM DASH (`\u{2014}`), which is what the TypeScript
/// line carries. Only the three framework kinds reach this line at all - a `policy` report is the plugin
/// stating a decision, not a failure, and TS stays silent for it.
#[must_use]
pub fn plugin_diagnostic_line(
    session_id: &str,
    entry: &crate::contract::SessionPluginDiagnostic,
) -> String {
    format!(
        "[harness] plugin diagnostic on session {session_id}: {} \u{2014} {} {}: {}",
        if entry.owner.is_empty() {
            "(no owner)"
        } else {
            &entry.owner
        },
        entry.event.as_deref().unwrap_or("no event"),
        diagnostic_kind_str(entry.kind),
        entry.message,
    )
}

/// The kinds the OPERATOR has to hear about: the three the framework raises about a plugin. A `policy`
/// report is a plugin stating a decision it made, which is durable but not a failure, so TypeScript keeps
/// it out of the console (`sessions.ts:3277-3281`) and this port keeps it out for the same reason.
#[must_use]
pub const fn plugin_diagnostic_is_failure(
    kind: crate::contract::SessionPluginDiagnosticKind,
) -> bool {
    use crate::contract::SessionPluginDiagnosticKind as Kind;

    matches!(kind, Kind::HandlerThrew | Kind::HandlerTimedOut | Kind::InvalidDecision)
}

/// The wire spelling of one diagnostic kind (`harness-api.ts:930`), for the log line the sink composes.
///
/// The sink keeps the enum until this last step: matching on the serialized string would let a renamed
/// variant silently stop being logged, which is exactly the observable the operator relies on.
#[must_use]
fn diagnostic_kind_str(kind: crate::contract::SessionPluginDiagnosticKind) -> &'static str {
    use crate::contract::SessionPluginDiagnosticKind as Kind;

    match kind {
        Kind::HandlerThrew => "handler-threw",
        Kind::HandlerTimedOut => "handler-timed-out",
        Kind::InvalidDecision => "invalid-decision",
        Kind::Policy => "policy",
    }
}

/// The durable string spellings, in both directions.
#[must_use]
pub fn turn_status_str(status: SessionTurnStatus) -> &'static str {
    match status {
        SessionTurnStatus::Queued => "queued",
        SessionTurnStatus::Running => "running",
        SessionTurnStatus::Done => "done",
        SessionTurnStatus::Error => "error",
        SessionTurnStatus::Cancelled => "cancelled",
    }
}

/// A `SessionTurnStatus` from the record's string form (`persistence.rs:566 TURN_STATUSES`).
#[must_use]
pub fn parse_turn_status(value: &str) -> SessionTurnStatus {
    match value {
        "queued" => SessionTurnStatus::Queued,
        "running" => SessionTurnStatus::Running,
        "done" => SessionTurnStatus::Done,
        "cancelled" => SessionTurnStatus::Cancelled,
        _ => SessionTurnStatus::Error,
    }
}

/// The session status as the durable record spells it (`persistence.rs:569 SESSION_STATUSES`).
#[must_use]
pub fn status_str(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Idle => "idle",
        SessionStatus::Running => "running",
        SessionStatus::Error => "error",
    }
}

/// A `SessionStatus` from the record's string form. An unknown spelling restores as `idle` because the
/// loader already refused it (`persistence.rs:901`).
#[must_use]
pub fn parse_status(value: &str) -> SessionStatus {
    match value {
        "running" => SessionStatus::Running,
        "error" => SessionStatus::Error,
        _ => SessionStatus::Idle,
    }
}

/// A conversation origin as the record spells it. (`read::parse_origin` reads the other direction.)
#[must_use]
pub fn origin_str(origin: ThreadOrigin) -> &'static str {
    match origin {
        ThreadOrigin::Host => "host",
        ThreadOrigin::Agent => "agent",
    }
}

/// A conversation lifecycle as the record spells it (`read::parse_lifecycle` reads the other
/// direction).
#[must_use]
pub fn lifecycle_str(lifecycle: ThreadLifecycle) -> &'static str {
    match lifecycle {
        ThreadLifecycle::Running => "running",
        ThreadLifecycle::Stopped => "stopped",
        // R7: the fourth spelling `harness-api.ts:1481` added. This build never parks a
        // conversation itself (no plugin runtime), but the word must round-trip rather than
        // collapse into `idle`, so a `paused` record restored from a TypeScript-written document
        // still reports the state it was saved in.
        ThreadLifecycle::Paused => "paused",
        ThreadLifecycle::Idle => "idle",
    }
}

/// `sessions.ts:1078 turnKey`: the hub target of one turn, `${sessionId}:${turnId}`.
#[must_use]
pub fn turn_key(session_id: &str, turn_id: &str) -> String {
    format!("{session_id}:{turn_id}")
}

/// `sessions.ts:1081 turnEventsKey`.
#[must_use]
pub fn turn_events_key(session_id: &str, turn_id: &str) -> String {
    turn_key(session_id, turn_id)
}

/// `sessions.ts:1085 threadKey`: the hub target of one conversation.
#[must_use]
pub fn thread_key(session_id: &str, thread_id: &str) -> String {
    // TS composes a `session:child` target id; the T2 hub keeps the routing ids separate, so the
    // conversation id IS the target id (`events.rs:201 resource_key`).
    let _ = session_id;
    thread_id.to_owned()
}

/// The frozen definition of one session, as the registry's own descriptor type
/// (`api-reads.ts:939 record.definition`).
#[must_use]
pub fn definition_of(state: &SessionState) -> crate::registry::HarnessDescriptor {
    crate::registry::definition_from_descriptor_record(&state.record.definition)
}

/// Projects a live descriptor onto its durable form (`registry/mod.rs:699`).
#[must_use]
pub fn descriptor_record_of(
    definition: &crate::registry::HarnessDescriptor,
) -> HarnessDescriptorRecord {
    crate::registry::descriptor_record_of(definition)
}

/// One created tool as the snapshot envelope stores it.
#[must_use]
pub fn created_tool_value(tool: &CreatedTool) -> Value {
    serde_json::to_value(tool).unwrap_or(Value::Null)
}

/// The session-not-found refusal, byte-identical to TS's (`sessions.ts:989`).
#[must_use]
pub fn session_not_found(session_id: &str) -> ApiError {
    ApiError::not_found(format!("Unknown session \"{session_id}\"."))
}

/// The message a full session refuses a mutating route with; TS has ONE sentence for this gate
/// (`sessions.ts:4253-4262`) and every busy route shares it, which is why it lives here.
#[must_use]
pub fn session_busy_error(session_id: &str, verb: &str) -> ApiError {
    ApiError::conflict(format!(
        "Cannot {verb} while session \"{session_id}\" has a live run or queued work; wait for it to settle or cancel it first."
    ))
}

/// The wall clock, from the same source the hub stamps its events with.
#[must_use]
pub fn now_ms() -> u64 {
    crate::events::now_millis()
}

/// Token totals as the usage fold reports them (`api-reads.ts:740-752`): an unreported `cached` stays
/// unknown rather than becoming zero.
#[must_use]
pub fn usage_total(usage: &HarnessUsage) -> u64 {
    usage.total
}

/// A `SessionAgentEntry` for one catalogue agent, before any session override.
#[must_use]
pub fn definition_binding(
    agent: &str,
    role: crate::contract::AgentRole,
    created_at: u64,
) -> SessionAgentEntry {
    SessionAgentEntry {
        agent_id: agent.to_owned(),
        profile: agent.to_owned(),
        origin: BindingOrigin::Definition,
        role,
        model: None,
        instruction: None,
        created_at,
        extra: Map::new(),
    }
}

/// The labels a submit's `#`-references carried, which is what a queue row prints.
#[must_use]
pub fn reference_labels(references: &[AgentResultReference]) -> Vec<String> {
    references
        .iter()
        .map(|reference| reference.label.clone())
        .collect()
}

/// One session borrowed out of the store lock, which is how a route holds a session and the lock in one
/// value.
///
/// `Deref`/`DerefMut` re-look the session up through the held guard rather than caching a reference,
/// because `MutexGuard::map` is not stable on this toolchain. The lookup cannot fail: the guard is what
/// keeps the map from being mutated, and no code removes a session while one of these is alive.
#[derive(Debug)]
pub struct SessionRef<'a> {
    guard: MutexGuard<'a, StoreInner>,
    session_id: String,
}

impl<'a> SessionRef<'a> {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.session_id
    }

    /// The whole store, as the guard sees it: what [`Store::commit`] and [`Store::save`] serialize.
    #[must_use]
    pub fn inner(&self) -> &StoreInner {
        &self.guard
    }

    /// The whole store, mutable, for the paths that add or drop a journal while holding a session.
    pub fn inner_mut(&mut self) -> &mut StoreInner {
        &mut self.guard
    }

    /// Gives up the session borrow and hands the caller the store guard (the shape every commit path
    /// wants: mutate, then commit through the same guard).
    pub fn into_inner(self) -> MutexGuard<'a, StoreInner> {
        self.guard
    }

    /// The session's replay log, opening it on demand (`sessions.ts:1919-1924`: the journal is attached
    /// lazily, so an in-memory deployment never creates the directory).
    pub fn journal_mut(&mut self, store: &Store) -> Option<&mut HarnessJournal> {
        if self.guard.journals.contains_key(&self.session_id) {
            return self.guard.journals.get_mut(&self.session_id);
        }

        match HarnessJournal::open(&store.config.sessions_dir(), &self.session_id, None) {
            Ok(journal) => {
                self.guard.journals.insert(self.session_id.clone(), journal);

                self.guard.journals.get_mut(&self.session_id)
            }
            Err(error) => {
                // A session whose log cannot be opened still runs; the degradation is reported
                // (`sessions.ts:1121-1131`).
                self.guard.journal_problems.push(format!(
                    "journal for session \"{}\" is unavailable: {}",
                    self.session_id, error.message
                ));

                None
            }
        }
    }
}

impl std::ops::Deref for SessionRef<'_> {
    type Target = SessionState;

    fn deref(&self) -> &SessionState {
        self.guard
            .find(&self.session_id)
            .expect("the session cannot vanish while the store lock is held")
    }
}

impl std::ops::DerefMut for SessionRef<'_> {
    fn deref_mut(&mut self) -> &mut SessionState {
        self.guard
            .find_mut(&self.session_id)
            .expect("the session cannot vanish while the store lock is held")
    }
}

/// The questions the session-domain modules ask a live session.
///
/// The counting and ordering questions (`pending_count`, `busy_or_pending`, `turns_in_index_order`,
/// `roster`, `primary_agent`) already live in [`read`], next to the read models that ask them; only the
/// undo-point helpers are here.
impl SessionState {
    /// The durable view of this session, in the shape a mutation captures its undo point from. The store
    /// argument documents WHY the call reads through a `Store` in the routes: the undo point is a record,
    /// not a live state, so it survives a restart the same way.
    #[must_use]
    pub fn projected_record(&self, _store: &Store) -> HarnessSessionRecord {
        self.durable_record()
    }

    /// The label a conversation carries, or the one its agent's next ordinal would mint
    /// (`api-reads.ts:1150 threadLabelOf`): a conversation created before labels existed still prints one.
    #[must_use]
    pub fn label_or_mint(&self, thread_id: &str, agent: &str) -> String {
        self.thread_labels
            .get(thread_id)
            .cloned()
            .unwrap_or_else(|| crate::labels::thread_label(agent, 1))
    }
}

/// One steering record exactly as a turn's `steers` array and the durable mailbox store it
/// (`persistence.rs:SteerRecord`, the in-memory twin of `session.steerRecords`).
///
/// The WIRE row is `contract::SteerRecord`, which replaces `prompt` with `promptPreview` and
/// `promptChars`; this is the store-side verbatim copy.
#[must_use]
pub fn steer_record(
    id: String,
    at: u64,
    prompt: String,
    thread_id: Option<String>,
    references: Option<Vec<AgentResultReference>>,
) -> SteerRecord {
    SteerRecord {
        id: Some(id),
        at,
        prompt,
        thread_id,
        references,
        extra: Map::new(),
    }
}

/// `api-events.ts:299 publish`, in the shape a session-domain route calls it from: bump every addressed
/// resource FIRST, then fan the event out.
///
/// The bump comes first for the reason `api-events.ts:302-306` states — a subscriber that re-reads the
/// resource after this event must see a newer revision, never the one it just rendered. The free-function
/// form exists because a route that already owns the store guard has the payloads in hand and must not
/// re-enter the mutex through a `&Store` method.
#[allow(clippy::too_many_arguments)]
pub fn publish(
    hub: &EventHub,
    kind: &str,
    resources: &[(String, Option<String>)],
    session_id: Option<&str>,
    thread_id: Option<&str>,
    turn_id: Option<&str>,
    payload: Value,
    ops: Vec<Value>,
) {
    use crate::events::ResourceRef;

    for (resource, id) in resources {
        // The bump's figure is the point of the call; the publish below reports it, so the return value
        // is dropped here (`api-events.ts:302-306`).
        let _ = hub.bump(resource, id.as_deref());
    }

    let targets: Vec<ResourceRef> = resources
        .iter()
        .map(|(resource, id)| ResourceRef::new(resource.as_str(), id.clone()))
        .collect();

    let _ = hub.publish_with_ops(kind, &targets, session_id, thread_id, turn_id, payload, ops);
}

/// The working directory a tool body runs in, given the durable root.
///
/// A self-hosted deployment keeps its scratch tree in `<root>/workspace`; a store pointed at a fixture
/// directory has none, and then the package's own repo root is the answer. The database root is never the
/// answer on its own — it holds `harness/state.json`, not the project a task edits. See the note on
/// [`Store::cwd`].
#[must_use]
pub fn tool_cwd(root: &std::path::Path) -> PathBuf {
    let workspace = root.join("workspace");
    if workspace.is_dir() {
        workspace
    } else {
        crate::package_root::repo_root()
    }
}

/// The store type under the name the session-domain modules use. `Store` is the honest name; the modules
/// written against `SessionStore` (the TS class name, `sessions.ts:626`) keep reading it unchanged.
pub type SessionStore = Store;

/// One conversation lane, under the pump's shorter name (`ThreadLane` is the durable-shaped name).
pub type Lane = ThreadLane;

/// The wire spelling of a turn status read backwards, under the name the tool and action rows use
/// (`parse_turn_status` is the implementation).
pub use self::parse_turn_status as status_of_turn;

// ----------------------------------------------------------------------
// Restore: reading `record.core` back into the live maps
//
// These are the exact inverse of `SessionState::write_core_into`, and they are deliberately tolerant: a
// document written by an older build (or hand-edited) restores with the missing half defaulted, because
// `Store::restore_all` skips a session rather than failing a boot.
// ----------------------------------------------------------------------

/// Reads one `core` key as `T`, defaulting anything this build cannot read.
fn core_of<T: serde::de::DeserializeOwned>(core: &Value, key: &str, fallback: T) -> T {
    core.get(key)
        .and_then(|value| serde_json::from_value::<T>(value.clone()).ok())
        .unwrap_or(fallback)
}

/// `core.threads` — every conversation with its committed transcript and transcript revision.
#[must_use]
pub fn restore_threads(core: &Value) -> Vec<HarnessThreadRecord> {
    let mut threads: Vec<HarnessThreadRecord> = core_of(core, "threads", Vec::new());

    for thread in &mut threads {
        // A WRITTEN `contextLane: 0` is normalized away on read (`core/snapshot.ts:758-763` drops it,
        // because `snapshot()` never emits the key for lane 0). Without this, a document a hand-edit
        // left a 0 in would come back as lane 0 and this crate would RE-WRITE the key TS never wrote.
        thread.context_lane = crate::persistence::LaneStamp::of(thread.context_lane);
    }

    threads
}

/// The projector seed the TURN RECORD alone can supply (`sessions.ts:4160-4240`).
///
/// The conversation-only half — section identity, the first user entry's id, the recorded instruction
/// sources — is added by [`SessionState::projector_seed`], which is what the session-level callers use.
/// This half exists so a store that has not loaded a conversation yet can still open a fold that mints
/// the right turn id, prompt, steer list, and LANE.
#[must_use]
pub fn seed_from_turn_record(record: &SessionTurn) -> ProjectorSeed {
    ProjectorSeed {
        turn_id: record.id.clone(),
        prompt: record.prompt.clone(),
        // Already normalized: a stored 0 is absence, and the projector's lane for the non-event
        // segments must be `None` in exactly the same cases the recorder wrote no key.
        lane: crate::persistence::LaneStamp::of(record.lane),
        started_at: record.started_at,
        thread_id: record.thread_id.clone(),
        thread_label: record.thread_label.clone(),
        agent: record
            .extra
            .get(crate::store::EXTRA_AGENT)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        // 0 is the ordinal of a session's first section, which is also what a session with no
        // sections at all starts from; `SessionState::projector_seed` corrects it from the snapshot.
        section_ordinal: 0,
        section_id: None,
        user_message_id: None,
        instruction_sources: Vec::new(),
        steers: merged_steer_seeds(record),
    }
}

/// The merged steer-submit list the fold mints its `steer` blocks from
/// (`turn-projector.ts:1609-1660 syncSteerBlocks`, fed by `sessions.ts`'s `displayContext`).
///
/// Two durable lists hold the same submits for different reasons: `turn.steers` is the BOUNDED audit
/// ring and `turn.steerTexts` is the UNBOUNDED text archive (`protocol.ts:174-206`). The archive wins
/// for an id it holds — it is the copy that keeps the EXACT bytes — and the ring only supplies submits
/// the archive never wrote (a document written before the archive existed). Ordering is by `at`, then
/// by id, because a block id is `ordinal-user-{submitId}` and a fold that ordered the same submits
/// differently would mint the same ids in a different sequence and renumber the bar.
#[must_use]
pub fn merged_steer_seeds(record: &SessionTurn) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::new();
    let mut seen: Vec<String> = Vec::new();

    // The archive first, then the ring, so the first-seen entry for an id is the archive's copy.
    let mut entries: Vec<Value> = record.steer_texts.clone().unwrap_or_default();

    entries.extend(
        record
            .steers
            .iter()
            .flatten()
            .map(|steer| serde_json::to_value(steer).unwrap_or(Value::Null)),
    );

    for entry in &entries {
        let Some(entry) = entry.as_object() else {
            continue;
        };

        // A submit with no id has no block identity, so the fold skips it exactly as the fold's own
        // derive-a-key rule would when the archive carries none (`persistence.ts:682-717` refuses a
        // blank id in the archive; a hand-written ring entry may still have one).
        let id = entry
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let Some(id) = id else { continue };

        if seen.iter().any(|existing| existing == id) {
            continue;
        }

        seen.push(id.to_owned());
        merged.push(Value::Object(entry.clone()));
    }

    // The sort key is OWNED (`at`, then `id`) rather than borrowed: the fold's rule is a total order on
    // those two fields, and an owned key keeps `sort_by`'s closure lifetime inference simple.
    let key = |entry: &Value| {
        (
            entry.get("at").and_then(Value::as_u64).unwrap_or(0),
            entry
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
        )
    };

    merged.sort_by_key(key);

    merged
}

/// `core.createdTools` — the created-tool ledger, keyed by name.
#[must_use]
pub fn restore_created_tools(core: &Value) -> HashMap<String, CreatedTool> {
    restore_tools_in_order(core)
        .into_iter()
        .map(|tool| (tool.name.clone(), tool))
        .collect()
}

/// `core.createdTools` in the order it was written.
#[must_use]
pub fn restore_tools_in_order(core: &Value) -> Vec<CreatedTool> {
    core_of(core, "createdTools", Vec::new())
}

/// `core.createdToolCount`, the counter behind every `sourceRevision` stamp.
#[must_use]
pub fn restore_created_tool_count(core: &Value) -> u64 {
    core.get("createdToolCount")
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

/// `core.disabledTools` — the names this session switched off.
#[must_use]
pub fn restore_disabled_tools(core: &Value) -> HashSet<String> {
    core.get("disabledTools")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// `core.agentTools` — the per-agent allow-lists.
#[must_use]
pub fn restore_agent_tools(core: &Value) -> BTreeMap<String, Vec<String>> {
    core_of(core, "agentTools", BTreeMap::new())
}

/// `core.discovery.enabled` — absent means enabled, which is `core/discovery.ts`'s default.
#[must_use]
pub fn restore_discovery_enabled(core: &Value) -> bool {
    core.get("discovery")
        .and_then(|discovery| discovery.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

/// `core.taskRows` — the task board this session mirrors.
#[must_use]
pub fn restore_task_rows(core: &Value) -> Vec<TaskRow> {
    core_of(core, "taskRows", Vec::new())
}

/// `core.steerMailbox` — guidance accepted and not yet delivered, per conversation.
#[must_use]
pub fn restore_steer_mailbox(core: &Value) -> HashMap<String, Vec<SteerRecord>> {
    core_of(core, CORE_STEER_MAILBOX, HashMap::new())
}

/// `core.activeThreadId` — the conversation a task lands in when the client names none.
#[must_use]
pub fn restore_active_thread(core: &Value) -> Option<String> {
    core.get("activeThreadId")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// `core.sessionAgentOverrides` — the session-scoped bindings, with the profile, role, instruction
/// override and provenance the record's own name list cannot carry.
#[must_use]
pub fn restore_session_agents(
    record: &HarnessSessionRecord,
) -> BTreeMap<String, SessionAgentEntry> {
    let core = &record.core;
    let overrides: Vec<SessionAgentEntry> = core_of(core, CORE_SESSION_AGENTS, Vec::new());
    if !overrides.is_empty() {
        return overrides
            .into_iter()
            .map(|entry| (entry.agent_id.clone(), entry))
            .collect();
    }

    // A document this build never wrote carries only the binding NAMES (`persistence.ts:241-249`). Each
    // name still has to appear in the roster, so the frozen definition's own profile and model answer.
    record
        .session_agents
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|agent| {
            let model = record
                .agent_models
                .as_ref()
                .and_then(|models| models.get(&agent))
                .cloned();

            (
                agent.clone(),
                SessionAgentEntry {
                    agent_id: agent.clone(),
                    // A name-list document states no profile, and the profile of a session binding that
                    // came from the harness itself IS the agent's own catalogue profile.
                    profile: agent,
                    origin: BindingOrigin::Session,
                    role: crate::contract::AgentRole::Subagent,
                    model,
                    instruction: None,
                    created_at: record.created_at,
                    extra: Map::new(),
                },
            )
        })
        .collect()
}

impl Store {
    /// Rebuilds one session's state from a durable record (`sessions.ts:3560 restoreSession`).
    ///
    /// Three things happen here in this order, and the order matters:
    /// 1. an interrupted run is repaired, so no restored turn is left `running`;
    /// 2. the record's own columns become the live maps (turns, threads, labels, tools, agents, tasks);
    /// 3. every turn's display fold is rebuilt by replaying its recorded events — `live == replay`, the
    ///    invariant a restart and a rollback both depend on.
    pub(crate) fn restore_session(
        &self,
        record: HarnessSessionRecord,
    ) -> StoreResult<SessionState> {
        let mut record = record;

        // 1. Interrupted-run recovery. The snapshot envelope is repaired first because the turn pass walks
        //    `turns`, and a tool call whose result vanished becomes a failed result INSIDE the snapshot
        //    (`persistence.rs:repair_interrupted_snapshot`, `repair_interrupted_tool_calls`).
        let mut core = record.core.clone();
        crate::persistence::repair_interrupted_snapshot(&mut core);
        record.core = core;
        crate::persistence::repair_interrupted_tool_calls(&mut record);

        let core = record.core.clone();

        // 2a. Turns, in recorded order, each with a fold rebuilt from its own events.
        let instruction_sources: BTreeMap<String, Vec<Value>> = record
            .turn_instruction_sources
            .as_ref()
            .and_then(|sources| sources.as_object())
            .map(|sources| {
                sources
                    .iter()
                    .map(|(turn_id, value)| {
                        (
                            turn_id.clone(),
                            value.as_array().cloned().unwrap_or_default(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let invocations = record.turn_invocations.clone().unwrap_or_default();

        let mut turns: HashMap<String, TurnState> = HashMap::new();
        let mut turn_order: Vec<String> = Vec::with_capacity(record.turns.len());

        for turn in &record.turns {
            let mut state = TurnState::from_record(self, turn);

            if let Some(sources) = instruction_sources.get(&turn.id) {
                state.instruction_sources = sources.clone();
            }

            // The invocation id rides back onto the turn's `extra`, which is where the read models look
            // for it (`SessionState::turn_invocations` is the same map seen from the other side).
            if let Some(invocation) = invocations.get(&turn.id) {
                state.record.extra.insert(
                    EXTRA_INVOCATION.to_owned(),
                    Value::String(invocation.clone()),
                );
            }

            state.replay(&*self.projector);

            turn_order.push(turn.id.clone());
            turns.insert(turn.id.clone(), state);
        }

        // 2b. Conversations, transcripts and labels.
        let threads: Vec<HarnessThreadRecord> = restore_threads(&core);
        let thread_order: Vec<String> = threads.iter().map(|thread| thread.id.clone()).collect();
        let threads_by_id: HashMap<String, HarnessThreadRecord> = threads
            .into_iter()
            .map(|thread| (thread.id.clone(), thread))
            .collect();
        let label_high_water: BTreeMap<String, u64> = record
            .thread_label_high_water
            .clone()
            .or_else(|| {
                record.label_high_water.as_ref().and_then(|value| {
                    serde_json::from_value::<BTreeMap<String, u64>>(value.clone()).ok()
                })
            })
            .unwrap_or_default();

        let thread_seq = core
            .get("threadSequence")
            .and_then(Value::as_u64)
            .unwrap_or(0);

        // The durable backlog exactly as the document states it, before the lane split below.
        let queue = record.queue.clone();

        // F1 distribution (`sessions.ts:3202-3228`): every restored queued turn belongs to the LANE of
        // the conversation it names, now that the roster has been rebuilt — or to the legacy queue when
        // the harness/conversation predates the lane API. Keeping everything in the legacy array instead
        // lets a fresh submit on another lane be claimed BEFORE a turn the user queued first, because
        // `queued_turn_ids` is the only thing that would order them.
        //
        // Order is preserved by APPEND: `record.queue` is arrival order, and a turn's `index` is minted
        // at enqueue time (`turns.rs:197`), so array order and index order are the same order. NO pump is
        // started here — TS's restore starts none, and the backlog stays paused until an explicit submit
        // names that lane.
        let (queue, lanes) = if core.get("threads").is_none() {
            (queue, HashMap::new())
        } else {
            let mut legacy: Vec<String> = Vec::new();
            let mut lanes: HashMap<String, ThreadLane> = HashMap::new();

            for turn_id in &queue {
                let owner = turns
                    .get(turn_id)
                    .and_then(|turn| turn.record.thread_id.clone())
                    .filter(|thread_id| threads_by_id.contains_key(thread_id));

                match owner {
                    Some(thread_id) => {
                        lanes
                            .entry(thread_id.clone())
                            .or_insert_with(|| ThreadLane {
                                thread_id: thread_id.clone(),
                                ..ThreadLane::default()
                            })
                            .queue
                            .push(turn_id.clone());
                    }
                    // A turn that names no conversation, or one this restore could not rebuild, stays
                    // where a legacy document keeps it: the session's serialized backlog.
                    None => legacy.push(turn_id.clone()),
                }
            }

            (legacy, lanes)
        };

        // Everything the record owns is read into a local BEFORE the record itself moves into the state,
        // because a struct literal moves its fields in written order.
        let thread_sequence = record.thread_sequence.unwrap_or(0);
        let updated_at = record.updated_at;
        let thread_labels = record.thread_labels.clone().unwrap_or_default();
        let invalid_threads = record.invalid_threads.clone().unwrap_or_default();
        let agent_instructions = record
            .definition
            .agent_instructions
            .clone()
            .unwrap_or_default();
        let session_agents = restore_session_agents(&record);
        // `sessions.ts:1570 expiresAt`: the restored session is only ever idle from the moment its last
        // write happened, measured with THIS process' window.
        let expires = updated_at.saturating_add(self.config.idle_ms);
        // A document with no `threads` key predates the conversation lanes; its queue is the legacy one.
        let legacy_pumping = core.get("threads").is_none();

        let mut state = SessionState {
            record,
            turns,
            turn_order,
            threads: threads_by_id,
            thread_order,
            thread_seq,
            // `record.threadSequence` is the TURN index high-water (see the field note).
            turn_seq: thread_sequence,
            thread_labels,
            label_high_water: label_high_water.clone(),
            labels: crate::labels::LabelAllocator::with_seed(&label_high_water),
            invalid_threads: invalid_threads.into_iter().collect(),
            queue,
            // The lanes the F1 distribution above rebuilt, so a restored backlog keeps its per-
            // conversation ordering. A conversation with no queued turn has no lane entry: the pump
            // still creates one on demand (`turns.rs:lane_of`).
            lanes,
            active: Vec::new(),
            active_thread: restore_active_thread(&core),
            steer_mailbox: restore_steer_mailbox(&core),
            steer_active: core.get("steerActive").and_then(Value::as_u64).unwrap_or(0),
            steer_tx: HashMap::new(),
            pending_actions: Vec::new(),
            answered_actions: 0,
            created_tools: restore_created_tools(&core),
            created_tool_count: restore_created_tool_count(&core),
            disabled_tools: restore_disabled_tools(&core),
            agent_tools: restore_agent_tools(&core),
            discovery_enabled: restore_discovery_enabled(&core),
            session_agents,
            agent_instructions,
            tasks: restore_task_rows(&core),
            // The credential is never persisted, so a restart always loses it
            // (`sessions.ts:1618-1634`).
            provider_key: None,
            expires,
            legacy_pumping,
            disposed: false,
            checkpoint_at: crate::events::now_millis(),
        };

        // A session restored while its record still says `running` has no run: the repair above turned the
        // turn into an error row, and the session mirrors its turns (`sessions.ts:3016`).
        if state.record.status == status_str(SessionStatus::Running) && state.active.is_empty() {
            state.record.status = if state.record.last_error.is_some() {
                status_str(SessionStatus::Error).to_owned()
            } else {
                status_str(SessionStatus::Idle).to_owned()
            };
        }

        // DISPLAY LANE / projector seed: a restored fold has to be re-opened with the SESSION's seed
        // (`sessions.ts:4160-4240 displayContext`). The pass that rebuilt the folds above could only
        // seed them from the turn record, because the conversation roster and the committed snapshot are
        // not assembled into a `SessionState` until here — and the three non-event segments (system,
        // prompt, steers), the turn lane, and every block id's section prefix all come from that
        // context. `TurnState::set_seed` re-folds what was already recorded, so a restarted
        // conversation's context bar is byte-for-byte the bar its live run painted.
        let seeds: Vec<(String, ProjectorSeed)> = state
            .turn_order
            .iter()
            .filter_map(|turn_id| {
                state
                    .turns
                    .get(turn_id)
                    .map(|turn| (turn_id.clone(), state.projector_seed(&turn.record)))
            })
            .collect();

        for (turn_id, seed) in seeds {
            if let Some(turn) = state.turns.get_mut(&turn_id) {
                turn.set_seed(&*self.projector, seed);
            }
        }

        Ok(state)
    }

    /// The harness name one session was created under (`record.harness`), for the registry lookup a
    /// restored session's tool policy needs.
    #[must_use]
    pub fn harness_of(&self, state: &SessionState) -> String {
        state.record.harness.clone()
    }
}

use serde::{Deserialize, Serialize};
