//! Cross-stage seams: the traits the persistent data plane defines and the later stages
//! implement or consume.
//!
//! WHY THESE LIVE IN THE DATA PLANE
//! --------------------------------
//! The session store, the turn executor, the tool runtime and the HTTP layer all need the same
//! handful of seams, and none of them may own the others: the store must be able to run a turn
//! without importing the agent runtime, a tool must be able to ask the host for an action without
//! importing the HTTP layer, and the stream layer must be able to ask "does this id exist" without
//! importing the store's internals. Each seam is therefore one trait plus the context struct it is
//! called with, declared here, where both sides can see it.
//!
//! The signatures are contractual, not incidental: workers in the next stages implement and call
//! exactly these.
//!
//! TypeScript origins
//! ------------------
//!   * `TurnInput` / `TurnOutcome` - `src/server/harnesses/toolbox.ts` (what a harness run is
//!     seeded with) plus the `Harness.run` loop in `src/harness.ts:3800+`;
//!   * `ToolRuntime` - `src/harness.ts`'s `tools` / `toolDeclarations` / `toolRequest` surface and
//!     `src/tools/catalogue.ts`'s entries;
//!   * `ActionHost` - `src/core/action.ts` + `src/server/api.ts`'s pending-action resolution (the
//!     host answers an `actionRequest` the model raised);
//!   * `TaskHost` - `src/core/task.ts` (the session-shared todo list a `task` call mutates);
//!   * `StreamExists` - `src/server/stream/mount.ts`'s subscription validity checks, which answer
//!     "is this id still here" without pulling a session into memory;
//!   * `Projector` / `ProjectorFactory` - `src/server/turn-projector.ts`'s `TurnProjector`, which
//!     the store folds every recorded event through (`sessions.ts:7107-7150`);
//!   * `Query` - the parsed query string every paged route reads (`api.ts:482-488` `paramOf`).

use std::path::PathBuf;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::contract;

/// Which tools one session's agents may see and call
/// (`src/harness.ts`'s registration/enablement state, projected for a runtime call).
///
/// Four lists rather than one set because the rules differ: `enabled` are the names declared to
/// the model on this request, `declared` are the names the harness registered at startup,
/// `created` are the names this session authored at runtime (`toolCreate`), and `subagents` are
/// the delegation targets. A runtime that collapsed them could not answer the catalogue's
/// `declared` / `enabled` flags (`harness-api.ts:1023-1035`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolPolicy {
    pub enabled: Vec<String>,
    pub declared: Vec<String>,
    pub created: Vec<String>,
    pub subagents: Vec<String>,
}

/// Everything one executed turn needs.
///
/// The system instruction and the transcript arrive pre-composed because the store owns the
/// instruction ladder (harness -> global -> catalogue) and the projector owns the transcript: a
/// turn executor must not be able to run with a different prompt than the one the store recorded
/// in `SessionTurn`/`turn_instruction_sources`.
pub struct TurnInput {
    pub session_id: String,
    pub turn_id: String,
    pub index: u64,
    pub prompt: String,
    pub thread_id: String,
    pub system_instruction: String,
    pub transcript: Vec<contract::HarnessMessage>,
    pub tools: Vec<contract::ToolDecl>,
    pub provider: contract::ProviderConfig,
    pub attachments: Vec<contract::HarnessAttachment>,
    pub signal: CancellationToken,
    /// The steering channel of THIS turn's conversation (`sessions.ts:6359` `steerIntoThread`):
    /// a submit that arrives while this run is live is folded into the run at its next safe
    /// model boundary instead of opening a second turn. `None` when the thread already has an
    /// active run (only one run per thread is ever steerable, so a second caller never gets a
    /// channel), and the executor must drain it to empty when it settles.
    ///
    /// An unbounded channel on purpose: `enqueue` never blocks on a run that may be mid-request,
    /// and the TS host's steer queue inside the runtime is likewise unbounded.
    pub steering: Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
    /// The working directory a tool body runs in (`harness.ts`'s tool context `cwd`).
    ///
    /// TS tools execute with the SERVER PROCESS cwd, which for the harness service is the repo
    /// root it was started from (`packages/underload/service/server.ts`); the store therefore
    /// hands the package root's repo root, and `<root>/workspace` when that directory exists
    /// (the deployment-local scratch tree a self-hosted server runs against). See `knowledge.md`
    /// for why this is the repo root and not the database root.
    pub cwd: std::path::PathBuf,
    /// The store's event sink: every event the run emits is recorded on the turn AND fanned out
    /// to the hub, in one call.
    pub emit: Arc<dyn Fn(contract::HarnessEvent) + Send + Sync>,
    pub tool_runtime: Arc<dyn ToolRuntime>,
    /// `None` when this session has no owner-interaction hook, which is the same condition that
    /// keeps `actionRequest` unregistered in TS (`src/harness.ts`: presence registers the tool).
    pub action_host: Option<Arc<dyn ActionHost>>,
    pub task_host: Option<Arc<dyn TaskHost>>,
    /// The tool-loop budget (`harnesses/toolbox.ts:103` `PRESET_MAX_ITERATIONS = 1000`).
    pub max_iterations: u64,
}

/// Runs one turn to a terminal outcome.
///
/// `BoxFuture<'static, _>` because the store holds the executor behind an `Arc` and the returned
/// future must outlive the call (a cancelled turn is still draining).
pub trait TurnExecutor: Send + Sync {
    fn run(&self, input: TurnInput) -> BoxFuture<'static, contract::TurnOutcome>;
}

/// One tool invocation, as the runtime hands it to an implementation
/// (`src/harness.ts`'s per-call context: identity, arguments, cwd, cancellation, policy).
pub struct ToolExecCtx {
    pub name: String,
    pub arguments: Value,
    pub call_id: String,
    /// `None` for a GLOBAL catalogue call, which is exactly the case the contract answers
    /// `session_required` for (`harness-api.ts:660`).
    pub session_id: Option<String>,
    pub cwd: PathBuf,
    pub signal: CancellationToken,
    pub policy: ToolPolicy,
    pub action_host: Option<Arc<dyn ActionHost>>,
    pub task_host: Option<Arc<dyn TaskHost>>,
}

/// What a tool call settled with.
///
/// `ok` and `error` are kept separate from `content` because the contract says a failed call still
/// carries the text the model reads back (`harness-api.ts:723-731`), and `duration_ms` is reported
/// only for a call that EXECUTED (`types/index.ts:1903-1909`: an immediate validation failure
/// executed and reports nothing).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ToolExecOutcome {
    pub ok: bool,
    pub content: String,
    pub panels: Option<Value>,
    pub media: Option<Value>,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// The tool catalogue plus its executable half.
///
/// Declarations (what a model is offered) and summaries (what a UI lists) are separate methods
/// because they are separate contract shapes - `ToolDecl` vs `ToolSummary` - and only the entries
/// the CURRENT policy enables belong in a request.
pub trait ToolRuntime: Send + Sync {
    fn declarations(&self, policy: &ToolPolicy) -> Vec<contract::ToolDecl>;
    fn summaries(&self) -> Vec<contract::ToolSummary>;
    fn detail(&self, name: &str) -> Option<contract::ToolDetail>;
    fn execute(&self, ctx: ToolExecCtx) -> BoxFuture<'static, ToolExecOutcome>;
}

/// The host's answer channel for an owner question the model raised (`src/core/action.ts`).
///
/// The request and the answer stay `Value`s: the contract's `HarnessActionRequest` is what the
/// RUNTIME parses after this seam answers, so the host may return any JSON shape (including a
/// refusal, which arrives as `Err(text)`).
pub trait ActionHost: Send + Sync {
    fn request(&self, request: Value) -> BoxFuture<'static, Result<Value, String>>;
}

/// The session's shared task list (`src/core/task.ts`), which is synchronous by construction: the
/// todo store is an in-memory table, so no await is needed to answer a `task` call.
pub trait TaskHost: Send + Sync {
    fn execute(&self, arguments: Value) -> Result<Value, String>;
}

/// Existence checks for stream subscriptions (`src/server/stream/mount.ts`).
///
/// A subscription must be validated when it is opened AND when a target disappears mid-life,
/// without the stream layer being allowed to materialize a session.
pub trait StreamExists: Send + Sync {
    fn has_session(&self, session_id: &str) -> bool;
    fn has_thread(&self, session_id: &str, thread_id: &str) -> bool;
    fn has_turn(&self, session_id: &str, turn_id: &str) -> bool;
}

/// A parsed query string (`api.ts` reads each route through `paramOf`/`enumParamOf`, which
/// trim and map an empty value to "absent"). A `BTreeMap` so a query is deterministic to
/// iterate and compare in a test; the store never needs more than string values.
pub type Query = std::collections::BTreeMap<String, String>;

/// One turn's compiled DISPLAY state (`src/server/turn-projector.ts`'s `TurnProjector`).
///
/// WHY A TRAIT AND NOT THE TYPE
/// ----------------------------
/// The projector is stage T4's implementation (`turn-projector.ts` is 3,600 lines of display
/// folding); the store owns WHEN it is folded - exactly once per recorded event, in the same
/// order the recorder stores them (`sessions.ts:7102-7150`) - and owns the caching of the
/// result (`session.projections`, `:642`). The seam is declared here so the store can compile
/// and be tested against a scripted projector while T4 replaces the mock with the real fold
/// without touching a line of store logic.
///
/// THE ONE RULE THE STORE RELIES ON: `apply` is a pure fold of the event stream, so folding a
/// restored turn's recorded events through a FRESH projector produces byte-identical rows to
/// the live run (`live == replay`, the invariant `turn-projector.ts` is built on). The store
/// therefore rebuilds a projector at restore by folding every recorded event, and never folds
/// an event twice (see `TurnState::folded`).
pub trait Projector: Send + Sync {
    /// Folds one recorded event and returns the bounded display ops it produced
    /// (`projector.apply(event, at, index)`, `sessions.ts:7150`). `index` is the event's
    /// ordinal WITHIN the turn, because a display step's id IS that ordinal as a decimal
    /// string (`harness-api.ts:1792`).
    fn apply(
        &mut self,
        event: &contract::HarnessEvent,
        at: u64,
        index: u64,
    ) -> Vec<serde_json::Value>;

    /// The turn's compact row, camelCase (`projector.summary()`), which is the wire
    /// `TurnSummary` verbatim apart from the prompt rename `api-reads.turnRowOf` performs
    /// (`prompt` -> `promptPreview` + `promptChars`). The store does that rename; the
    /// projector states `prompt`.
    fn summary(&self) -> serde_json::Value;

    /// The turn's compiled display tree (`projector.display()`, the `TurnDisplay` document).
    fn display(&self) -> serde_json::Value;

    /// Closes the fold at a terminal status (`turn-projector.ts`'s settle step): a turn that
    /// ended as `error`/`cancelled` must not keep streaming state in its row. `status` is the
    /// turn status string, and `at` is the terminal timestamp the row reports.
    fn settle(&mut self, status: &str, at: u64);
}

/// Builds one projector per turn (`createTurnProjector`, `turn-projector.ts`). A factory is
/// needed because the projector is per-turn state, not a shared service: the store calls
/// `create` when a turn is opened (and again at restore, before folding).
pub trait ProjectorFactory: Send + Sync {
    fn create(&self) -> Box<dyn Projector>;
}
