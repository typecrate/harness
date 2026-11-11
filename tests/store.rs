//! Integration tests for the session store (T3): the submit surface, the lanes and the pump, the
//! command path, the recorder, transcripts, threads, owner actions, and what a run leaves durable.
//!
//! Every expected value below is taken from the TypeScript source of record - `src/server/sessions.ts`
//! for the submit/queue/steer/cancel/pump discipline, `src/core/command.ts` + `command-builtins.ts` for
//! the command text, `src/server/api.ts` + `api-reads.ts` for the refusals and read shapes, and
//! `src/shared/attachments.ts` for the media bounds - NOT observed from this Rust build. Where the store
//! owns a figure a test cannot recompute (a generated id, an opaque cursor, a wall-clock stamp), the
//! assertion states the RELATIONSHIP the contract pins instead, and says so in place.
//!
//! THE PROJECTOR IS A SCRIPTED STAND-IN ON PURPOSE: `ports.rs:185-200` declares the display fold as
//! stage T4's implementation; the store owns WHEN it folds (exactly once per recorded event) and what
//! the row is renamed to. These tests pin the fold count, the settle step, and the store-owned fields -
//! never the display tree.
//!
//! Every fixture lives under the repo `Temp/typecrate-harness/store/<pid>-<test>/` (repo rule: scratch
//! stays inside the repository) and is removed when the guard drops.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio::sync::{Barrier, mpsc};

use typecrate_harness::agents::AgentCatalogue;
use typecrate_harness::config::HarnessRuntimeSettings;
use typecrate_harness::contract::{
    self, ActionChoiceInput, AttachmentKind, CancelSessionResult, CancelThreadResult,
    CreateSessionRequest, CreateSessionToolRequest, DeleteSessionResult, DeleteSessionToolResult,
    HarnessActionAnswer, HarnessAssistantMessage, HarnessAttachment, HarnessErrorInfo,
    HarnessEvent, HarnessMessage, HarnessStopReason, HarnessUsage, MAX_PENDING_TASKS, MessageRole,
    ResetSessionResult, SessionDefinitionResponse, SessionResponse, SessionStatus,
    SessionToolOrigin, SessionTurnStatus, SessionUsageResponse, TaskReceipt, ThreadLifecycle,
    ThreadOrigin, ThreadResponse, ThreadSummary, TranscriptAcceptedAs, TranscriptState,
    TranscriptWriteReceipt, UpdateSessionToolRequest, UpdateThreadTranscriptRequest,
};
use typecrate_harness::error::ApiError;
use typecrate_harness::events::EventHub;
use typecrate_harness::persistence;
use typecrate_harness::ports::{
    ActionHost, Projector, ProjectorFactory, Query, ToolExecCtx, ToolExecOutcome, ToolPolicy,
    ToolRuntime, TurnExecutor, TurnInput,
};
use typecrate_harness::registry::HarnessRegistry;
use typecrate_harness::store::actions::{StoreActionHost, StoreTaskHost};
use typecrate_harness::store::{self, Store, StoreConfig};

// ---------------------------------------------------------------------------
// Collaborator doubles
// ---------------------------------------------------------------------------

/// What one scripted run answers with: the assistant text, plus the tally its provider reported in
/// the `usage` EVENT (the durable figure the store folds).
#[derive(Clone, Debug)]
struct Answer {
    text: String,
    billed: HarnessUsage,
}

impl Answer {
    fn new(text: &str, input: u64, output: u64) -> Self {
        Self {
            text: text.to_owned(),
            billed: usage(input, output),
        }
    }
}

/// One turn's scripted behavior, consumed in the order the runs actually start.
enum Script {
    /// Streams the answer and settles `done`.
    Ok(Answer),
    /// Parks until the test releases the sender's pair OR the run's cancellation token fires, then
    /// answers. This is how a test pins a run that is genuinely still `running`, and how a cancel is
    /// observed by the executor the way a real provider client observes it.
    Park {
        release: mpsc::Receiver<()>,
        answer: Answer,
    },
    /// Joins a rendezvous before answering: the proof that two lanes really run at once.
    Rendezvous {
        barrier: Arc<Barrier>,
        answer: Answer,
    },
    /// Settles `error` with the provider's failure text and no transcript change.
    Fail { error: String },
    /// Drains ONE steer out of the conversation's channel, then answers with both texts.
    Steer { answer: Answer },
    /// Waits for a steer the way a live run does, drains whatever else is queued, and then FAILS.
    ///
    /// This is `harness.ts:6820-6837`'s catch arm: the guidance was drained into the transcript, the
    /// rollback unwound it, and the run reports it as `unused_steering` so the store can hand it back
    /// to the conversation instead of losing the user's input.
    SteerFail { error: String },
}

/// Everything one run was handed, kept so a test can assert what the store prepared.
#[derive(Clone, Debug)]
struct Observed {
    turn_id: String,
    index: u64,
    thread_id: String,
    prompt: String,
    system_instruction: String,
    model: String,
    tools: Vec<String>,
    attachments: usize,
    transcript_entries: usize,
    steering_channel: bool,
    action_host: bool,
    task_host: bool,
}

/// The scripted executor. It emits events THROUGH THE STORE'S SINK (`input.emit`), so the recorder,
/// the projector and the hub are exercised exactly as a production run exercises them.
struct MockExecutor {
    scripts: Mutex<VecDeque<Script>>,
    observed: Mutex<Vec<Observed>>,
}

impl MockExecutor {
    fn new(scripts: Vec<Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            observed: Mutex::new(Vec::new()),
        }
    }

    /// The runs this executor started, in start order.
    fn observed(&self) -> Vec<Observed> {
        self.observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Appends more scripts, for a test that needs a second wave after the first settles.
    fn queue(&self, scripts: Vec<Script>) {
        let mut guard = self.scripts.lock().unwrap_or_else(|p| p.into_inner());
        for script in scripts {
            guard.push_back(script);
        }
    }
}

impl TurnExecutor for MockExecutor {
    fn run(
        &self,
        input: TurnInput,
    ) -> BoxFuture<'static, typecrate_harness::contract::TurnOutcome> {
        let note = Observed {
            turn_id: input.turn_id.clone(),
            index: input.index,
            thread_id: input.thread_id.clone(),
            prompt: input.prompt.clone(),
            system_instruction: input.system_instruction.clone(),
            model: input.provider.model.clone(),
            tools: input.tools.iter().map(|decl| decl.name.clone()).collect(),
            attachments: input.attachments.len(),
            transcript_entries: input.transcript.len(),
            steering_channel: input.steering.is_some(),
            action_host: input.action_host.is_some(),
            task_host: input.task_host.is_some(),
        };

        let script = self
            .scripts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pop_front()
            .unwrap_or_else(|| Script::Ok(Answer::new("unscripted", 1, 1)));

        self.observed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(note.clone());

        let model = input.provider.model.clone();
        let emit = Arc::clone(&input.emit);
        let signal = input.signal.clone();
        let prompt = input.prompt.clone();
        let attachments = input.attachments.clone();
        let mut steering = input.steering;

        Box::pin(async move {
            let answer = match script {
                Script::Ok(answer) => answer,
                Script::Park { release, answer } => {
                    let mut release = release;
                    // A cancel is an OUTCOME the run reports, not an exception: the token wins the race
                    // and the store still settles the turn (`sessions.ts:7341-7348`).
                    tokio::select! {
                        Some(()) = release.recv() => {}
                        () = signal.cancelled() => {}
                    }
                    answer
                }
                Script::Rendezvous { barrier, answer } => {
                    let _ = barrier.wait().await;
                    answer
                }
                Script::Fail { error } => {
                    emit(HarnessEvent::Start {
                        agent: AGENT.to_owned(),
                        model: model.clone(),
                        thread: None,
                    });
                    emit(HarnessEvent::Error {
                        agent: AGENT.to_owned(),
                        error: HarnessErrorInfo {
                            message: error.clone(),
                        },
                        thread: None,
                    });

                    // A failed run hands back NO messages: an empty vector means "the executor changed
                    // nothing", so the conversation keeps the transcript it had (`ports.rs:2891-2892`).
                    return typecrate_harness::contract::TurnOutcome {
                        usage: None,
                        error: Some(error),
                        cancelled: false,
                        messages: Vec::new(),
                        stop_reason: None,
                        unused_steering: Vec::new(),
                    };
                }
                Script::Steer { answer } => {
                    let mut receiver = steering
                        .take()
                        .expect("a live run owns its conversation's steer channel");
                    let steered = receiver
                        .recv()
                        .await
                        .expect("the store delivers a steer to the live run");

                    Answer {
                        text: format!("{}>{steered}", answer.text),
                        ..answer
                    }
                }
                Script::SteerFail { error } => {
                    let mut receiver = steering
                        .take()
                        .expect("a live run owns its conversation's steer channel");
                    let mut drained: Vec<String> = Vec::new();

                    // Wait for the steer (the deterministic half), then take anything that arrived
                    // behind it, in order.
                    drained.push(
                        receiver
                            .recv()
                            .await
                            .expect("the store delivers a steer to the live run"),
                    );

                    while let Ok(text) = receiver.try_recv() {
                        drained.push(text);
                    }

                    emit(HarnessEvent::Start {
                        agent: AGENT.to_owned(),
                        model: model.clone(),
                        thread: None,
                    });
                    emit(HarnessEvent::Error {
                        agent: AGENT.to_owned(),
                        error: HarnessErrorInfo {
                            message: error.clone(),
                        },
                        thread: None,
                    });

                    return typecrate_harness::contract::TurnOutcome {
                        usage: None,
                        error: Some(error),
                        cancelled: false,
                        // A failed run hands back no messages: the conversation keeps what it had.
                        messages: Vec::new(),
                        stop_reason: None,
                        // Everything this run drained is owed back, because the transcript it was
                        // appended into is rolled back (`harness.ts:6826-6837`).
                        unused_steering: drained,
                    };
                }
            };

            stream(&emit, &model, &answer.text, &answer.billed);

            typecrate_harness::contract::TurnOutcome {
                // The OUTCOME tally deliberately disagrees with the EVENT tally: the fold of the
                // recorded events is authoritative, and every run test asserts the event figure.
                usage: Some(usage(900, 900)),
                error: None,
                cancelled: signal.is_cancelled(),
                messages: vec![
                    HarnessMessage::User {
                        content: prompt,
                        attachments,

                        included: None,
                        message_id: None,
                    },
                    HarnessMessage::Assistant {
                        content: answer.text.clone(),
                        tool_calls: Vec::new(),
                        reasoning: None,

                        included: None,
                        reasoning_included: None,
                        message_id: None,
                    },
                ],
                stop_reason: Some("stop".to_owned()),
                unused_steering: Vec::new(),
            }
        })
    }
}

/// Every scripted run executes as the harness's entry agent (`sessions.ts:7046-7060`).
const AGENT: &str = "instruct";

/// Emits the four events one successful provider request produces, in the runtime's order.
fn stream(
    emit: &Arc<dyn Fn(HarnessEvent) + Send + Sync>,
    model: &str,
    text: &str,
    billed: &HarnessUsage,
) {
    emit(HarnessEvent::Start {
        agent: AGENT.to_owned(),
        model: model.to_owned(),
        thread: None,
    });
    emit(HarnessEvent::Text {
        agent: AGENT.to_owned(),
        delta: text.to_owned(),
        thread: None,
    });
    emit(HarnessEvent::Usage {
        agent: AGENT.to_owned(),
        usage: billed.clone(),
        thread: None,
    });
    emit(HarnessEvent::Done {
        agent: AGENT.to_owned(),
        message: HarnessAssistantMessage {
            role: MessageRole::Assistant,
            content: text.to_owned(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            model: model.to_owned(),
            stop_reason: HarnessStopReason::Stop,
            usage: Some(billed.clone()),
        },
        pending: Vec::new(),
        thread: None,
    });
}

/// The usage shorthand: `total` is always the sum, exactly as the contract's fold computes it.
fn usage(input: u64, output: u64) -> HarnessUsage {
    HarnessUsage {
        input,
        output,
        total: input + output,
        cached: None,
    }
}

/// The tool catalogue the store projects its toolbox from.
struct MockRuntime {
    entries: Vec<typecrate_harness::contract::ToolSummary>,
}

impl MockRuntime {
    fn new() -> Self {
        Self {
            entries: vec![
                catalog_entry("probe", "Probe the fixture."),
                catalog_entry("gizmo", "Adjust a gizmo."),
            ],
        }
    }
}

fn catalog_entry(name: &str, description: &str) -> typecrate_harness::contract::ToolSummary {
    typecrate_harness::contract::ToolSummary {
        name: name.to_owned(),
        group: "fixture".to_owned(),
        description: description.to_owned(),
        tags: vec!["demo".to_owned()],
        visibility: typecrate_harness::contract::ToolVisibility::Searchable,
        availability: None,
        session_bound: false,
    }
}

impl ToolRuntime for MockRuntime {
    fn declarations(&self, policy: &ToolPolicy) -> Vec<typecrate_harness::contract::ToolDecl> {
        self.entries
            .iter()
            .filter(|entry| policy.enabled.contains(&entry.name))
            .map(|entry| typecrate_harness::contract::ToolDecl {
                name: entry.name.clone(),
                description: entry.description.clone(),
                parameters: Some(json!({"type": "object"})),
            })
            .collect()
    }

    fn summaries(&self) -> Vec<typecrate_harness::contract::ToolSummary> {
        self.entries.clone()
    }

    /// The mock has no listing rule: every row it holds is both listed and part of a session
    /// toolbox (`ports.rs`'s two summaries methods).
    fn catalogue_summaries(&self) -> Vec<typecrate_harness::contract::ToolSummary> {
        self.entries.clone()
    }

    fn detail(&self, name: &str) -> Option<typecrate_harness::contract::ToolDetail> {
        self.entries
            .iter()
            .find(|entry| entry.name == name)
            .cloned()
            .map(|summary| typecrate_harness::contract::ToolDetail {
                parameters: Some(json!({"type": "object"})),
                execution: typecrate_harness::contract::ToolExecutionScope {
                    scope: typecrate_harness::contract::ToolExecutionScopeKind::Global,
                    manual: true,
                },
                summary,
            })
    }

    fn execute(&self, ctx: ToolExecCtx) -> BoxFuture<'static, ToolExecOutcome> {
        Box::pin(async move {
            if ctx.name == "probe" {
                return ToolExecOutcome {
                    ok: true,
                    content: format!("probed {}", ctx.arguments),
                    panels: None,
                    media: None,
                    error: None,
                    duration_ms: 3,
                };
            }

            ToolExecOutcome {
                ok: false,
                content: String::new(),
                panels: None,
                media: None,
                error: Some(format!("unknown tool \"{}\"", ctx.name)),
                duration_ms: 0,
            }
        })
    }
}

/// The scripted fold: it states only what the events tell it, which is what makes the store's
/// one-apply-per-event discipline and its terminal `settle` observable from outside.
#[derive(Default)]
struct MockProjector {
    status: String,
    events: Vec<String>,
    settled_at: Option<u64>,
}

impl Projector for MockProjector {
    fn apply(
        &mut self,
        event: &HarnessEvent,
        _lane: Option<u64>,
        at: u64,
        index: u64,
    ) -> Vec<Value> {
        let kind = store::read::event_type_name(event).to_owned();
        self.events.push(kind.clone());

        // A display step's id IS the within-turn ordinal as a decimal string (`harness-api.ts:1792`),
        // so the ordinal the store passes has to reach the fold unchanged.
        vec![json!({"index": index, "type": kind, "at": at})]
    }

    fn summary(&self) -> Value {
        json!({
            "status": self.status,
            "eventCount": self.events.len(),
            "settledAt": self.settled_at,
        })
    }

    fn display(&self) -> Value {
        json!({"steps": self.events})
    }

    fn settle(&mut self, status: &str, at: u64) {
        self.status = status.to_owned();
        self.settled_at = Some(at);
    }
}

struct MockProjectorFactory;

impl ProjectorFactory for MockProjectorFactory {
    fn create(&self, _seed: &typecrate_harness::ports::ProjectorSeed) -> Box<dyn Projector> {
        Box::new(MockProjector::default())
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Unique per process AND per test, so a parallel `cargo test` never shares a database root.
static FIXTURE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// One store over one disposable database root, plus the doubles it runs on.
struct Fixture {
    directory: PathBuf,
    store: Store,
    executor: Arc<MockExecutor>,
    runtime: Arc<MockRuntime>,
    task_host: Arc<StoreTaskHost>,
    registry: Arc<HarnessRegistry>,
}

impl Fixture {
    fn with_scripts(name: &str, scripts: Vec<Script>) -> Self {
        Self::with_fold(name, scripts, Arc::new(MockProjectorFactory))
    }

    /// The same fixture over a caller-chosen display fold, so a test can pin what the REAL projector
    /// produces (`projector::TurnProjectorFactory`) while every other double stays scripted.
    fn with_fold(name: &str, scripts: Vec<Script>, projector: Arc<dyn ProjectorFactory>) -> Self {
        let sequence = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = typecrate_harness::package_root::repo_root()
            .join("Temp")
            .join("typecrate-harness")
            .join("store")
            .join(format!("{}-{sequence}-{name}", std::process::id()));

        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("a fixture directory is creatable");

        let deployment = settings();
        let runtime = Arc::new(MockRuntime::new());
        let executor = Arc::new(MockExecutor::new(scripts));
        let registry = Arc::new(HarnessRegistry::shipped(&deployment, Vec::new()));
        let task_host = Arc::new(StoreTaskHost::new());

        let store = Store::open(
            StoreConfig::at(&directory),
            deployment,
            Arc::new(AgentCatalogue::builtin()),
            runtime.clone(),
            executor.clone(),
            projector,
            registry.clone(),
            Arc::new(EventHub::new()),
            Some(task_host.clone()),
        );

        Self {
            directory,
            store,
            executor,
            runtime,
            task_host,
            registry,
        }
    }

    /// Creates one `instruct` session and returns its id.
    async fn session(&self, title: Option<&str>) -> String {
        let response = self
            .store
            .create_session(&CreateSessionRequest {
                harness: "instruct".to_owned(),
                title: title.map(str::to_owned),
                settings: None,
            })
            .await
            .expect("a shipped preset creates a session");

        resource_of(&response).session.id
    }

    /// `GET /sessions/:id` as the typed resource.
    async fn resource(&self, id: &str) -> typecrate_harness::contract::SessionResource {
        resource_of(
            &self
                .store
                .get_session(id)
                .await
                .expect("the session exists"),
        )
    }

    /// Opens one conversation and returns its id, reading the roster row the create answer carries.
    async fn thread(&self, id: &str, body: &Value) -> String {
        let (payload, _revision) = self
            .store
            .create_thread(id, body)
            .await
            .expect("a roster member opens a conversation");

        serde_json::from_value::<ThreadSummary>(payload["thread"].clone())
            .expect("the create answer carries a roster row")
            .id
    }

    /// Submits plain text, which takes the queue path.
    async fn submit(
        &self,
        id: &str,
        request: typecrate_harness::contract::SubmitTaskRequest,
    ) -> Result<TaskReceipt, ApiError> {
        self.store.submit_task(id, request).await.map(|value| {
            serde_json::from_value::<TaskReceipt>(value).expect("a submit answer is a receipt")
        })
    }

    /// Submits a task with no attachments and no references.
    async fn task(
        &self,
        id: &str,
        input: &str,
        thread: Option<&str>,
    ) -> Result<TaskReceipt, ApiError> {
        self.submit(
            id,
            typecrate_harness::contract::SubmitTaskRequest {
                input: input.to_owned(),
                attachments: Vec::new(),
                references: Vec::new(),
                thread_id: thread.map(str::to_owned),
            },
        )
        .await
    }

    /// Submits a task that CARRIES a clip. Attachments are never steered - the runtime's steer API is
    /// text-only - so such a submit joins the lane's backlog even while its conversation is running
    /// (`turns.rs:155-158`). This is how a test builds a real queue behind a live run.
    async fn task_with_clip(
        &self,
        id: &str,
        input: &str,
        thread: &str,
    ) -> Result<TaskReceipt, ApiError> {
        self.submit(
            id,
            contract::SubmitTaskRequest {
                input: input.to_owned(),
                attachments: vec![clip(
                    "att-1",
                    "pic.png",
                    "image/png",
                    AttachmentKind::Image,
                    "[Image 1]",
                    3,
                )],
                references: Vec::new(),
                thread_id: Some(thread.to_owned()),
            },
        )
        .await
    }

    /// One turn's durable record, read straight out of the store's own state.
    fn turn(&self, id: &str, turn_id: &str) -> persistence::SessionTurn {
        let inner = self.store.lock();
        inner
            .session(id)
            .expect("the session exists")
            .turn(turn_id)
            .expect("the turn exists")
            .record
            .clone()
    }

    /// The transcript of one conversation, from the live roster.
    fn transcript(&self, id: &str, thread_id: &str) -> Vec<HarnessMessage> {
        let inner = self.store.lock();

        inner
            .session(id)
            .expect("the session exists")
            .transcript_messages(thread_id)
            .expect("the conversation exists")
    }

    fn session_status(&self, id: &str) -> String {
        let inner = self.store.lock();

        inner
            .session(id)
            .expect("the session exists")
            .record
            .status
            .clone()
    }

    fn thread_status(&self, id: &str, thread_id: &str) -> String {
        let inner = self.store.lock();

        inner
            .session(id)
            .expect("the session exists")
            .thread(thread_id)
            .expect("the conversation exists")
            .status
            .clone()
    }

    fn turn_ids(&self, id: &str) -> Vec<String> {
        let inner = self.store.lock();

        inner.session(id).expect("the session exists").turn_ids()
    }

    /// One turn's status as the typed value the contract states.
    fn turn_status(&self, id: &str, turn_id: &str) -> SessionTurnStatus {
        let inner = self.store.lock();

        store::parse_turn_status(
            &inner
                .session(id)
                .expect("the session exists")
                .turn(turn_id)
                .expect("the turn exists")
                .record
                .status,
        )
    }

    /// The questions a run is still waiting on, which is what the contract lists (`live_actions`).
    fn waiting_actions(&self, id: &str) -> Vec<typecrate_harness::contract::PendingAction> {
        let inner = self.store.lock();

        inner
            .session(id)
            .expect("the session exists")
            .pending_action_rows()
    }

    /// Polls until `probe` answers `Some`. The timeout names the state the store never produced, so a
    /// stuck pump reads as a contract failure rather than a hang.
    async fn wait_until<T>(&self, what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(5);

        loop {
            if let Some(value) = probe() {
                return value;
            }

            assert!(Instant::now() < deadline, "the store never reached {what}");

            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Waits for one turn to report `want`, then hands back its record.
    async fn wait_turn(
        &self,
        id: &str,
        turn_id: &str,
        want: SessionTurnStatus,
    ) -> persistence::SessionTurn {
        let status = store::turn_status_str(want).to_owned();

        self.wait_until(&format!("turn {turn_id} to settle as {status}"), || {
            let record = self.turn(id, turn_id);
            (record.status == status).then_some(record)
        })
        .await
    }

    /// Waits for one turn to reach `running`, which is the precondition every steer/cancel test needs.
    async fn wait_running(&self, id: &str, turn_id: &str) {
        self.wait_until(&format!("turn {turn_id} to start"), || {
            (self.turn(id, turn_id).status == "running").then_some(())
        })
        .await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// The deployment settings a fixture runs with: the configured origin plus no credential, which is
/// the pair the registry tests use.
fn settings() -> HarnessRuntimeSettings {
    HarnessRuntimeSettings {
        base_url: "http://localhost:5500/providers/private/v1".to_owned(),
        api_key: String::new(),
        model: "merge/ds4-flash".to_owned(),
    }
}

/// `{"session": ...}` as the typed envelope.
fn resource_of(response: &Value) -> typecrate_harness::contract::SessionResource {
    serde_json::from_value::<SessionResponse>(response.clone())
        .expect("a session answer is a session resource")
        .session
}

/// A `Query` from name/value pairs, so a route's filter reads like a URL.
fn query(pairs: &[(&str, &str)]) -> Query {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

/// A valid clip whose payload satisfies BOTH the size rule and the media-signature rule: a PNG clip
/// has to open with the PNG magic bytes or the validator refuses it (`attachments.rs:551-555`), and
/// `size` has to equal the DECODED length. `bytes` is therefore a floor, not the literal length.
fn clip(
    id: &str,
    name: &str,
    mime: &str,
    kind: AttachmentKind,
    marker: &str,
    bytes: usize,
) -> HarnessAttachment {
    let mut raw = vec![0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
    raw.resize(bytes.max(raw.len()), b'u');

    HarnessAttachment {
        id: id.to_owned(),
        kind,
        marker: marker.to_owned(),
        name: name.to_owned(),
        mime: mime.to_owned(),
        size: u64::try_from(raw.len()).unwrap_or(u64::MAX),
        data: base64::engine::general_purpose::STANDARD.encode(&raw),
    }
}

/// The two facts every refusal test pins: the status AND the exact sentence.
fn refused(error: ApiError) -> (u16, String) {
    (error.status, error.message)
}

/// The session ids the durable document holds, which is the only honest durability check.
fn persisted_ids(store: &Store) -> Vec<String> {
    let path = store
        .config()
        .state_dir()
        .join(persistence::HARNESS_DATABASE_FILE);

    persistence::read_harness_database_file(&path)
        .expect("the document is readable")
        .sessions
        .iter()
        .map(|record| record.id.clone())
        .collect()
}

/// Commits a sync mutation the way the HTTP stage does: reconcile the snapshot, then write, bump and
/// publish through `Store::commit`.
fn commit_mutation(store: &Store, id: &str, resources: &[&str]) -> u64 {
    {
        let mut inner = store.lock();
        let state = inner.session_mut(id).expect("the session exists");
        state.sync_core();
    }

    let mut outcome = store::CommitOutcome::touch(id);
    for resource in resources {
        outcome
            .bumps
            .push(((*resource).to_owned(), Some(id.to_owned())));
    }

    store
        .commit(&store.lock(), id, &mut outcome)
        .expect("the mutation is durable")
}

// ---------------------------------------------------------------------------
// 1. Create, read, list, refuse
// ---------------------------------------------------------------------------

#[tokio::test]
async fn creating_a_session_reports_the_contract_resource_and_an_exactly_full_page_ends() {
    let fixture = Fixture::with_scripts("create", Vec::new());
    let store = &fixture.store;

    let response = store
        .create_session(&CreateSessionRequest {
            harness: "instruct".to_owned(),
            title: Some("  Named conversation  ".to_owned()),
            settings: None,
        })
        .await
        .expect("a shipped preset creates a session");
    let created = resource_of(&response);

    // A session id is an opaque UUID v4 (`ids::new_session_id`), which is what makes it safe to
    // put in a URL and a cursor.
    uuid::Uuid::parse_str(&created.session.id).expect("a session id is a uuid");
    // `normalize_title` trims and keeps the text (`sessions.ts:1775`); 200 characters is the cap.
    assert_eq!(created.session.title.as_deref(), Some("Named conversation"));
    assert_eq!(created.session.harness, "instruct");
    assert_eq!(created.session.status, SessionStatus::Idle);
    assert_eq!(created.session.model, "merge/ds4-flash");
    // A fresh session has no turns and no conversations, and `queue_depth` is ABSENT rather than 0
    // while nothing is pending (`api-reads.ts:203-205`).
    assert_eq!(created.session.turn_count, 0);
    assert_eq!(created.session.thread_count, 0);
    assert_eq!(created.session.queue_depth, None);
    assert_eq!(created.session.last_error, None);
    // The live model map states the model EVERY member agent runs, not the operator's overrides: the
    // frozen roster's agents all resolve to the deployment model until a PATCH names another one
    // (`read.rs:317 agent_models_of`).
    assert_eq!(
        created.agent_models,
        std::collections::BTreeMap::from([
            ("instruct".to_owned(), "merge/ds4-flash".to_owned()),
            ("planner".to_owned(), "merge/ds4-flash".to_owned()),
            ("research".to_owned(), "merge/ds4-flash".to_owned()),
            ("reviewer".to_owned(), "merge/ds4-flash".to_owned()),
            ("summarizer".to_owned(), "merge/ds4-flash".to_owned()),
            ("workspace".to_owned(), "merge/ds4-flash".to_owned()),
        ]),
        "every member of the frozen definition has a live model entry"
    );
    assert_eq!(created.pending_action_count, 0);
    assert_eq!(created.created_tool_count, 0);
    assert_eq!(created.created_tool_limit, 32);
    // The deployment's own thread-execution knob, stated by the store's runtime snapshot
    // (`session.rs:825 runtime_snapshot`): `inline` is the built-in default (`config.ts`).
    assert_eq!(created.runtime_execution, "inline");
    // The revision a read reports IS the hub's counter for that session: two readers of one figure
    // cannot disagree, which is what makes `X-Harness-Revision` trustworthy.
    assert_eq!(
        created.session.revision,
        store.revision_of(&created.session.id)
    );

    // The frozen definition the session was built with, by identified read.
    let definition = store
        .session_definition(&created.session.id)
        .await
        .expect("the frozen definition is readable");
    let definition = serde_json::from_value::<SessionDefinitionResponse>(definition)
        .expect("a definition answer is a definition resource");
    // `SessionDefinitionResponse` wraps the resource, which flattens the definition card.
    let definition = definition.definition;
    assert_eq!(definition.session_id, created.session.id);
    assert_eq!(definition.definition.card.name, "instruct");
    assert!(
        definition.current,
        "a fresh session is at the current revision"
    );

    // The resource and the create answer agree on every counter, including the definition stamp:
    // a reload of the same figure must not move it.
    let reread = fixture.resource(&created.session.id).await;
    assert_eq!(reread.definition_revision, created.definition_revision);
    assert_eq!(reread, created);

    // The shipped preset the session was built from is in the registry the store holds.
    assert_eq!(
        fixture.registry.find("instruct").expect("shipped").name,
        "instruct"
    );

    // A second session, so a page can be EXACTLY full: an exactly-full page that has reached a known
    // total ends the collection rather than pointing at an empty one (`api-paging.ts:130-136`).
    let second = resource_of(
        &store
            .create_session(&CreateSessionRequest {
                harness: "instruct".to_owned(),
                title: None,
                settings: None,
            })
            .await
            .expect("the second session"),
    );

    let page = store
        .list_sessions(&query(&[("limit", "1")]))
        .await
        .expect("the list is readable");
    let first =
        serde_json::from_value::<typecrate_harness::contract::SessionSummaryPage>(page.clone())
            .expect("a list answer is a session page");
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].id, created.session.id);
    // `limit` echoes the EFFECTIVE page size, i.e. what this page actually carries.
    assert_eq!(first.meta.limit, 1);
    let cursor = first
        .meta
        .next_cursor
        .expect("a page that has more behind it names the next one");

    let next = serde_json::from_value::<typecrate_harness::contract::SessionSummaryPage>(
        store
            .list_sessions(&query(&[("limit", "1"), ("cursor", &cursor)]))
            .await
            .expect("the second page is readable"),
    )
    .expect("a list answer is a session page");
    assert_eq!(next.items.len(), 1);
    assert_eq!(next.items[0].id, second.session.id);
    assert_eq!(next.meta.next_cursor, None);

    // The collection's own order is insertion order - `store.liveSessions()` is a Map walk and TS
    // never sorts this page (`sessions.ts:1651-1653`).
    let all = serde_json::from_value::<typecrate_harness::contract::SessionSummaryPage>(
        store.list_sessions(&Query::new()).await.expect("the page"),
    )
    .expect("a list answer is a session page");
    assert_eq!(
        all.items
            .iter()
            .map(|row| row.id.clone())
            .collect::<Vec<_>>(),
        vec![created.session.id.clone(), second.session.id.clone()]
    );
    assert_eq!(all.meta.next_cursor, None);

    // Both sessions are durable. The document's array order is NOT asserted: `database_of` walks the
    // session MAP (`mod.rs:1181`), whose order is not the store's insertion order, which is a
    // deviation from TS's `Array.from(liveSessions().values())` worth reporting (see knowledge.md).
    let mut durable = persisted_ids(store);
    durable.sort();
    let mut wanted = vec![created.session.id.clone(), second.session.id.clone()];
    wanted.sort();
    assert_eq!(durable, wanted);

    // A foreign cursor is refused with the sentence that names the parameter, and the two failure
    // kinds are distinct: an undecodable token versus a token this server did not issue in THIS epoch
    // (`api-paging.ts:54-87`).
    assert_eq!(
        refused(
            store
                .list_sessions(&query(&[("cursor", "!!!!")]))
                .await
                .unwrap_err()
        ),
        (
            400,
            "Query parameter \"cursor\" is not a cursor this server issued.".to_owned(),
        )
    );
    // `not-a-cursor` decodes cleanly but carries another epoch half, so it is stale/foreign.
    assert_eq!(
        refused(
            store
                .list_sessions(&query(&[("cursor", "bm90LWEtY3Vyc29y")]))
                .await
                .unwrap_err()
        ),
        (
            400,
            "Query parameter \"cursor\" is stale or foreign; restart from the first page."
                .to_owned(),
        )
    );
}

#[tokio::test]
async fn a_session_refusal_names_the_harness_and_the_session() {
    let fixture = Fixture::with_scripts("refusals", Vec::new());
    let store = &fixture.store;

    // The lookup is alias-aware and the refusal quotes the trimmed name the caller asked for
    // (`sessions.ts:1827-1831`).
    assert_eq!(
        refused(
            store
                .create_session(&CreateSessionRequest {
                    harness: "  nope  ".to_owned(),
                    title: None,
                    settings: None,
                })
                .await
                .unwrap_err()
        ),
        (404, "Unknown harness \"nope\".".to_owned())
    );

    assert_eq!(
        refused(
            store
                .create_session(&CreateSessionRequest {
                    harness: "   ".to_owned(),
                    title: None,
                    settings: None,
                })
                .await
                .unwrap_err()
        ),
        (400, "A session requires a harness name.".to_owned())
    );

    // One sentence for a missing session on every route (`sessions.ts:989`).
    assert_eq!(
        refused(store.get_session("gone").await.unwrap_err()),
        (404, "Unknown session \"gone\".".to_owned())
    );
    assert_eq!(
        refused(store.reset_session("gone").await.unwrap_err()),
        (404, "Unknown session \"gone\".".to_owned())
    );
    assert_eq!(
        refused(
            store
                .update_session("gone", &json!({"title": "x"}))
                .await
                .unwrap_err()
        ),
        (404, "Unknown session \"gone\".".to_owned())
    );
    assert_eq!(
        refused(store.delete_session("gone").await.unwrap_err()),
        (404, "Unknown session \"gone\".".to_owned())
    );
    assert_eq!(
        refused(store.cancel_session("gone", true).await.unwrap_err()),
        (404, "Unknown session \"gone\".".to_owned())
    );
    assert_eq!(
        refused(store.list_threads("gone", &Query::new()).await.unwrap_err()),
        (404, "Unknown session \"gone\".".to_owned())
    );
    assert_eq!(
        refused(store.session_agents("gone").await.unwrap_err()),
        (404, "Unknown session \"gone\".".to_owned())
    );

    // An empty body is a 400 no-op rather than a silent success (`api.ts:2030-2036`).
    let id = fixture.session(None).await;
    assert_eq!(
        refused(store.update_session(&id, &json!({})).await.unwrap_err()).0,
        400
    );
}

// ---------------------------------------------------------------------------
// 2. The queue path, end to end
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_queued_submit_runs_the_turn_and_the_record_keeps_every_event() {
    let answer = Answer::new("the answer", 7, 3);
    let fixture = Fixture::with_scripts("run", vec![Script::Ok(answer.clone())]);
    let store = &fixture.store;
    let id = fixture.session(None).await;
    // A top-level conversation is a HOST conversation: `thread_count` counts host threads only,
    // because an agent's own lane is internal plumbing (`api-reads.ts:175-177 hostThreads`).
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;

    let receipt = fixture
        .task(&id, "  do the thing  ", Some(&thread))
        .await
        .expect("the submit is accepted");

    // The 202 is a QUEUED receipt: the pump has claimed nothing yet, and the position counts this
    // submit as the first waiting task (`api.ts:2195-2244`).
    assert_eq!(receipt.session_id, id);
    assert_eq!(receipt.turn_id, fixture.turn_ids(&id)[0]);
    assert_eq!(receipt.status, SessionTurnStatus::Queued);
    assert_eq!(receipt.thread_id, thread);
    assert_eq!(receipt.steered, None);
    assert_eq!(receipt.steered_thread_id, None);
    assert_eq!(receipt.queue_position, Some(1));
    assert_eq!(
        receipt.links.turn,
        format!(
            "{}/sessions/{id}/turns/{}",
            contract::HARNESS_API_ROOT,
            receipt.turn_id
        )
    );
    assert_eq!(
        receipt.links.turns,
        format!("{}/sessions/{id}/turns", contract::HARNESS_API_ROOT)
    );
    assert_eq!(
        receipt.links.thread,
        format!(
            "{}/sessions/{id}/threads/{thread}",
            contract::HARNESS_API_ROOT
        )
    );
    assert_eq!(
        receipt.links.stream,
        format!("{}/stream", contract::HARNESS_API_ROOT)
    );

    let record = fixture
        .wait_turn(&id, &receipt.turn_id, SessionTurnStatus::Done)
        .await;

    // The index is the monotonic high-water, 0-based (`sessions.ts:5844-5845`), and the recorded
    // prompt is the TRIMMED text: the store bounds and trims before a turn exists.
    assert_eq!(record.index, 0);
    assert_eq!(record.prompt, "do the thing");
    assert_eq!(record.thread_id.as_deref(), Some(thread.as_str()));
    assert!(record.started_at.is_some(), "a run reports when it started");
    assert_eq!(record.id, receipt.turn_id);

    // The event list is EXACTLY what the run emitted, in emission order and once each: the recorder
    // is the only place a fold happens, so a live run cannot double-count (`sessions.ts:6704-6760`).
    assert_eq!(
        record
            .events
            .iter()
            .map(|entry| store::read::event_type_name(&entry.event))
            .collect::<Vec<_>>(),
        vec!["start", "text", "usage", "done"]
    );
    // `eventTimes` stays parallel with `events` (`persistence.ts:162-167`).
    assert_eq!(record.event_times.as_deref().map(<[u64]>::len), Some(4));

    // The turn's tally is the FOLD OF THE EVENTS (7/3/10), not the outcome's 900/900: the events are
    // the durable record, so they win when both exist (`sessions.ts:7139-7144`).
    assert_eq!(record.usage, Some(usage(7, 3)));

    // The projector settled exactly once, at the terminal status. The display projector owns the fold
    // and receives no turn record (stage T4), so the STAGE-T4 row stated an empty prompt pair; T5b
    // merged the record at the read boundary (`store/read.rs::TurnState::row`), so the row now carries
    // the record's own text as the bounded `promptPreview` + exact `promptChars` pair that
    // `api-reads.ts:283-284` prints - which is what the paged conversation surface shows in TypeScript.
    let (row, resource) = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");
        let turn = state.turn(&receipt.turn_id).expect("the turn");

        (
            turn.row(),
            store::read::turn_resource_of(state, turn, store.hub()),
        )
    };
    assert_eq!(row["status"], json!("done"));
    assert_eq!(row["eventCount"], json!(4));
    assert!(row["settledAt"].is_number());
    assert!(
        row.get("prompt").is_none(),
        "the row never carries the raw prompt"
    );
    assert_eq!(row["promptPreview"], json!("do the thing"));
    assert_eq!(row["promptChars"], json!(12));
    // The row's identity members come from the record now too (`turnRowOf`'s TypeScript projector).
    assert_eq!(row["id"], json!(receipt.turn_id));
    assert_eq!(row["index"], json!(0));
    assert_eq!(row["threadId"], json!(thread));

    // The identified turn read is the one route that states the prompt verbatim, with counts beside
    // it (`harness-api.ts:1609`).
    assert_eq!(resource.prompt, "do the thing");
    assert_eq!(resource.status, SessionTurnStatus::Done);
    assert_eq!(resource.event_count, 4);
    assert_eq!(resource.event_time_count, Some(4));
    assert_eq!(resource.attachment_count, 0);
    assert_eq!(resource.steer_count, 0);
    assert_eq!(resource.usage, Some(usage(7, 3)));
    assert_eq!(resource.error, None);

    // The transcript is the executor's object, REPLACED rather than appended: `[user, assistant]`,
    // so the prompt cannot appear twice (`ports.rs:2887-2892`).
    assert_eq!(
        fixture.transcript(&id, &thread),
        vec![
            HarnessMessage::User {
                content: "do the thing".to_owned(),
                attachments: Vec::new(),

                included: None,
                message_id: None,
            },
            HarnessMessage::Assistant {
                content: "the answer".to_owned(),
                tool_calls: Vec::new(),
                reasoning: None,

                included: None,
                reasoning_included: None,
                message_id: None,
            },
        ]
    );

    // What the store handed the run: the trimmed prompt, the resolved conversation, the deployment's
    // provider trio, the enabled tools, the instruction ladder, and both hosts.
    let observed = fixture.executor.observed();
    assert_eq!(observed.len(), 1);
    let observed = &observed[0];
    assert_eq!(observed.turn_id, receipt.turn_id);
    assert_eq!(observed.index, 0);
    assert_eq!(observed.thread_id, thread);
    assert_eq!(observed.prompt, "do the thing");
    assert_eq!(observed.model, "merge/ds4-flash");
    assert_eq!(observed.attachments, 0);
    assert_eq!(
        observed.transcript_entries, 0,
        "a first turn has no history"
    );
    assert!(
        observed.steering_channel,
        "a live run owns its conversation's steer channel"
    );
    assert!(observed.action_host, "the owner-question host is wired");
    assert!(observed.task_host, "the task host is wired");
    assert!(
        !observed.system_instruction.is_empty(),
        "the instruction ladder always resolves for a roster member"
    );
    // The run is handed the enabled toolbox: only the two entries this fixture's runtime offers,
    // because a declaration cannot exist without an implementation behind it.
    let mut tools = observed.tools.clone();
    tools.sort();
    assert_eq!(tools, vec!["gizmo".to_owned(), "probe".to_owned()]);

    // The lane is released, the conversation is idle again, and the session reports idle with no
    // banner: a success clears `last_error` (`sessions.ts:7390-7405`).
    assert_eq!(fixture.thread_status(&id, &thread), "idle");
    assert_eq!(fixture.session_status(&id), "idle");
    let after = fixture.resource(&id).await;
    assert_eq!(after.session.turn_count, 1);
    assert_eq!(after.session.thread_count, 1);
    assert_eq!(after.session.queue_depth, None);
    assert_eq!(after.session.last_error, None);

    // The usage read folds the same figure the turn row carries and attributes it to the conversation
    // that produced it (`harness-api.ts:1364`).
    let folded = serde_json::from_value::<SessionUsageResponse>(
        store.session_usage(&id).await.expect("usage is readable"),
    )
    .expect("a usage answer");
    assert_eq!(folded.usage, Some(usage(7, 3)));
    assert_eq!(folded.turn_count, 1);
    assert_eq!(folded.reported_turns, 1);
    assert_eq!(
        folded.by_thread,
        vec![typecrate_harness::contract::UsageByThread {
            thread_id: thread.clone(),
            usage: usage(7, 3),
        }]
    );
    assert!(!folded.truncated);

    // The journal behind the turn is durable: `<root>/harness/sessions/<id>/events.jsonl` is the log a
    // restart replays (`mod.rs:220 sessions_dir`, `journal.rs:89-111`). Its first line is the header,
    // and every event line is the TypeScript envelope `{type,v,turnId,index,at,event}` (`journal.ts:303`)
    // with the TURN's own append position - `index` counts within the turn, not within the file.
    let journal = store.config().sessions_dir().join(&id).join("events.jsonl");
    let text = std::fs::read_to_string(&journal)
        .unwrap_or_else(|error| panic!("{} should exist: {error}", journal.display()));
    let lines: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("every journal line is one JSON record"))
        .collect();

    assert_eq!(
        lines.len(),
        5,
        "the header line plus one line per recorded event, and no more"
    );
    assert_eq!(lines[0]["type"], json!("header"));
    assert_eq!(lines[0]["v"], json!(1));
    assert_eq!(lines[0]["sessionId"], json!(id.clone()));
    assert!(lines[0]["createdAt"].is_number());
    assert_eq!(
        lines[1..]
            .iter()
            .map(|line| (
                line["type"].as_str().expect("a record type"),
                line["v"].as_u64().expect("a format version"),
                line["turnId"].as_str().expect("a turnId").to_owned(),
                line["index"].as_u64().expect("an index"),
                line["event"]["type"].as_str().expect("an event type"),
            ))
            .collect::<Vec<_>>(),
        vec![
            ("event", 1, receipt.turn_id.clone(), 0, "start"),
            ("event", 1, receipt.turn_id.clone(), 1, "text"),
            ("event", 1, receipt.turn_id.clone(), 2, "usage"),
            ("event", 1, receipt.turn_id.clone(), 3, "done"),
        ],
        "the journal keeps emission order and the turn's own 0-based index per line"
    );
    assert_eq!(
        lines[1..]
            .iter()
            .map(|line| line["at"].as_u64().expect("a stamp"))
            .collect::<Vec<u64>>(),
        record
            .event_times
            .as_deref()
            .expect("the durable record keeps one stamp per event")
            .to_vec(),
        "the envelope stamp IS the record's own `eventTimes` figure for that event: one clock, three \
         surfaces - the journal line, the durable record, and the published frame \
         (`sessions.ts:7930-7961`)"
    );
    assert!(
        lines[1..].iter().all(|line| line["event"].get("at").is_none()),
        "an event body carries no `at` of its own, so the envelope's stamp is the single figure every \
         surface agrees on rather than one of two that could disagree"
    );
    assert!(
        lines[1..].iter().all(|line| line["at"].as_u64().is_some_and(|at| at > 0)),
        "the envelope stamp is a real clock figure on every line"
    );
    assert_eq!(
        lines[1..]
            .iter()
            .map(|line| {
                line.as_object()
                    .expect("an event line is an object")
                    .keys()
                    .cloned()
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<Vec<String>>>(),
        vec![
            vec![
                "type".to_owned(),
                "v".to_owned(),
                "turnId".to_owned(),
                "index".to_owned(),
                "at".to_owned(),
                "event".to_owned(),
            ];
            4
        ],
        "`append` writes `type`/`v` FIRST (`journal.ts:303`) and the payload LAST, so a TS-written \
         journal and a Rust-written one diff to nothing but the payload bytes"
    );
}

#[tokio::test]
async fn a_failed_run_is_an_error_turn_and_raises_the_session_banner() {
    let fixture = Fixture::with_scripts(
        "fail",
        vec![Script::Fail {
            error: "the provider refused".to_owned(),
        }],
    );
    let id = fixture.session(None).await;
    let thread = fixture.thread(&id, &json!({"agent": "instruct"})).await;

    let receipt = fixture
        .task(&id, "boom", Some(&thread))
        .await
        .expect("submit");
    let record = fixture
        .wait_turn(&id, &receipt.turn_id, SessionTurnStatus::Error)
        .await;

    assert_eq!(record.status, "error");
    // A failed run reports no usage at all: an absent tally is "nothing was billed", not zero
    // (`harness-api.ts:1626`).
    assert_eq!(record.usage, None);
    assert_eq!(
        record
            .events
            .iter()
            .map(|entry| store::read::event_type_name(&entry.event))
            .collect::<Vec<_>>(),
        vec!["start", "error"]
    );
    // An empty `messages` means "the executor changed nothing", so the conversation keeps the
    // transcript it had - which here is none.
    assert!(fixture.transcript(&id, &thread).is_empty());

    // A failure raises the session-wide banner with the provider's text (`sessions.ts:7400-7405`).
    assert_eq!(fixture.session_status(&id), "error");
    let resource = fixture.resource(&id).await;
    assert_eq!(
        resource.session.last_error.as_deref(),
        Some("the provider refused")
    );
    assert_eq!(
        resource.session.status,
        typecrate_harness::contract::SessionStatus::Error
    );

    // The next success clears it.
    fixture
        .executor
        .queue(vec![Script::Ok(Answer::new("ok", 1, 1))]);
    let retry = fixture
        .task(&id, "again", Some(&thread))
        .await
        .expect("retry");
    fixture
        .wait_turn(&id, &retry.turn_id, SessionTurnStatus::Done)
        .await;
    assert_eq!(fixture.resource(&id).await.session.last_error, None);
    assert_eq!(fixture.session_status(&id), "idle");
}

// ---------------------------------------------------------------------------
// 3. Lanes: FIFO inside one, concurrency across two
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_second_submit_in_one_lane_waits_and_the_queue_page_states_the_order() {
    let (release_tx, release_rx) = mpsc::channel::<()>(1);

    let fixture = Fixture::with_scripts(
        "fifo",
        vec![
            Script::Park {
                release: release_rx,
                answer: Answer::new("first", 1, 1),
            },
            Script::Ok(Answer::new("second", 2, 2)),
            Script::Ok(Answer::new("third", 3, 3)),
        ],
    );
    let store = &fixture.store;
    let id = fixture.session(None).await;
    let thread = fixture.thread(&id, &json!({"agent": "instruct"})).await;

    let first = fixture
        .task(&id, "one", Some(&thread))
        .await
        .expect("the first submit");
    // The submit started this lane's pump, so the turn is genuinely `running` while the next two wait.
    fixture.wait_running(&id, &first.turn_id).await;

    let second = fixture
        .task_with_clip(&id, "two", &thread)
        .await
        .expect("the second submit");
    let third = fixture
        .task_with_clip(&id, "three", &thread)
        .await
        .expect("the third submit");

    // Neither one steered: a clip cannot ride a steer, so both fall through to a queued continuation
    // that keeps its bytes (`turns.rs:155-158`) - which is what makes a lane backlog observable.
    assert_eq!(second.steered, None);
    assert_eq!(third.steered, None);

    // The receipt ordinal is 1-based (`api.ts:2195-2230`): the pump ordinal plus one.
    assert_eq!(second.queue_position, Some(1));
    assert_eq!(third.queue_position, Some(2));
    assert_eq!(second.status, SessionTurnStatus::Queued);

    // The queue page is PUMP ORDER, and its `position` is the 0-based slot of that ordering - a
    // different figure from the receipt on purpose (`read.rs:1160-1162`).
    let page = serde_json::from_value::<contract::QueuedTaskPage>(
        store
            .session_queue(&id, &Query::new())
            .await
            .expect("the backlog is readable"),
    )
    .expect("a queue answer is a page");
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].turn_id, second.turn_id);
    assert_eq!(page.items[0].position, 0);
    assert_eq!(
        page.items[0].index, 1,
        "the running turn still consumed index 0"
    );
    assert_eq!(page.items[0].status, SessionTurnStatus::Queued);
    assert_eq!(page.items[0].prompt_preview, "two");
    assert_eq!(page.items[0].prompt_chars, 3);
    assert_eq!(page.items[0].thread_id.as_deref(), Some(thread.as_str()));
    assert_eq!(
        page.items[0].attachment_count,
        Some(1),
        "a queued continuation carries the clip it arrived with"
    );
    assert_eq!(page.items[1].turn_id, third.turn_id);
    assert_eq!(page.items[1].position, 1);
    assert_eq!(page.items[1].index, 2);
    assert_eq!(page.meta.next_cursor, None);

    // The session row counts the backlog, which is how a rail shows a badge without a second read,
    // and reports `running` because one turn is executing (`harness-api.ts:790`).
    let resource = fixture.resource(&id).await;
    assert_eq!(resource.session.queue_depth, Some(2));
    assert_eq!(resource.session.status, SessionStatus::Running);
    assert_eq!(fixture.session_status(&id), "running");

    release_tx
        .send(())
        .await
        .expect("the parked run is released");

    for (receipt, index) in [(second, 1_u64), (third, 2)] {
        let record = fixture
            .wait_turn(&id, &receipt.turn_id, SessionTurnStatus::Done)
            .await;
        assert_eq!(record.index, index);
        assert_eq!(record.usage, Some(usage(index + 1, index + 1)));
    }

    // Strict FIFO, proven by what the executor actually RECEIVED rather than by what the queue
    // claimed (`sessions.ts:6593-6605`: the claim happens when the run starts, never at peek time).
    assert_eq!(
        fixture
            .executor
            .observed()
            .iter()
            .map(|run| run.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["one", "two", "three"]
    );

    let resource = fixture.resource(&id).await;
    assert_eq!(resource.session.status, SessionStatus::Idle);
    assert_eq!(resource.session.queue_depth, None);
    assert_eq!(resource.session.turn_count, 3);
    assert_eq!(
        fixture.turn_status(&id, &first.turn_id),
        SessionTurnStatus::Done
    );
}

#[tokio::test]
async fn two_lanes_run_at_the_same_time_while_one_lane_stays_single_flight() {
    let barrier = Arc::new(Barrier::new(2));

    let fixture = Fixture::with_scripts(
        "lanes",
        vec![
            Script::Rendezvous {
                barrier: Arc::clone(&barrier),
                answer: Answer::new("left", 1, 1),
            },
            Script::Rendezvous {
                barrier: Arc::clone(&barrier),
                answer: Answer::new("right", 2, 2),
            },
        ],
    );
    let id = fixture.session(None).await;
    let left = fixture.thread(&id, &json!({"agent": "instruct"})).await;
    let right = fixture.thread(&id, &json!({"agent": "instruct"})).await;
    assert_ne!(left, right, "each conversation is its own lane");

    let first = fixture
        .task(&id, "left", Some(&left))
        .await
        .expect("the left submit");
    let second = fixture
        .task(&id, "right", Some(&right))
        .await
        .expect("the right submit");

    // The rendezvous can only complete if BOTH runs are live at once, so reaching it is the proof
    // that the single-flight gate is per-conversation and not per-session (`sessions.ts:6655-6681`).
    for receipt in [&first, &second] {
        fixture
            .wait_turn(&id, &receipt.turn_id, SessionTurnStatus::Done)
            .await;
    }

    let observed = fixture.executor.observed();
    assert_eq!(observed.len(), 2);
    assert_ne!(observed[0].thread_id, observed[1].thread_id);
    assert_eq!(fixture.session_status(&id), "idle");

    // A third submit joins the lane it names, and the two lanes stay independent.
    fixture.executor.queue(vec![
        Script::Ok(Answer::new("left again", 1, 1)),
        Script::Ok(Answer::new("right again", 1, 1)),
    ]);
    let again = fixture
        .task(&id, "left two", Some(&left))
        .await
        .expect("a settled lane accepts work again");
    fixture
        .wait_turn(&id, &again.turn_id, SessionTurnStatus::Done)
        .await;
    assert_eq!(
        fixture
            .executor
            .observed()
            .last()
            .expect("the run happened")
            .thread_id,
        left
    );
}

// ---------------------------------------------------------------------------
// 4. Steering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_submit_to_a_running_conversation_steers_instead_of_opening_a_turn() {
    let fixture = Fixture::with_scripts(
        "steer",
        vec![Script::Steer {
            answer: Answer::new("base", 1, 1),
        }],
    );
    let store = &fixture.store;
    let id = fixture.session(None).await;
    let thread = fixture.thread(&id, &json!({"agent": "instruct"})).await;

    let running = fixture
        .task(&id, "go", Some(&thread))
        .await
        .expect("the run opens");
    fixture.wait_running(&id, &running.turn_id).await;

    let steered = fixture
        .task(&id, "  and louder  ", Some(&thread))
        .await
        .expect("the steer is accepted");

    // A steer is folded into the LIVE run: the receipt names the turn that accepted it, says so
    // explicitly, and reports no backlog position (`sessions.ts:6440-6504`).
    assert_eq!(steered.steered, Some(true));
    assert_eq!(steered.steered_thread_id.as_deref(), Some(thread.as_str()));
    assert_eq!(steered.turn_id, running.turn_id);
    assert_eq!(steered.status, SessionTurnStatus::Running);
    assert_eq!(steered.queue_position, None);
    assert_eq!(steered.thread_id, thread);

    // No second turn, and nothing waiting: the guidance consumed no queue slot.
    assert_eq!(fixture.turn_ids(&id).len(), 1);
    assert_eq!(fixture.resource(&id).await.session.queue_depth, None);

    // The run consumed the steer off its own channel and answered with both texts.
    let record = fixture
        .wait_turn(&id, &running.turn_id, SessionTurnStatus::Done)
        .await;

    // The bounded audit ring keeps the VERBATIM steer (`persistence::SteerRecord`), and the owner's
    // own conversation omits the attribution field (`sessions.ts:6434-6440`).
    let steers = record.steers.clone().expect("one steer recorded");
    assert_eq!(steers.len(), 1);
    assert_eq!(
        steers[0].prompt, "and louder",
        "the prompt is trimmed on the way in"
    );
    assert_eq!(
        steers[0].thread_id, None,
        "a steer into the owner's own thread is unattributed"
    );
    assert!(!steers[0].id.clone().unwrap_or_default().is_empty());

    // The unbounded text archive is the second copy, and it carries the same figure.
    let archive = record.steer_texts.clone().expect("the archive");
    assert_eq!(archive.len(), 1);
    assert_eq!(archive[0]["prompt"], json!("and louder"));
    // DEVIATION, reported rather than hidden: the store writes the key with a JSON `null` where TS
    // leaves the property undefined (`sessions.ts:6434-6440`). A TS reader reads `undefined` either
    // way, so the difference is in the durable bytes, not in behavior.
    assert_eq!(archive[0]["threadId"], Value::Null);

    // The row a listener sees after a steer states the steer COUNT, and the compact row renames the
    // steered prompt the same way it renames the turn prompt (`api-reads.ts:315-325`).
    let resource = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store::read::turn_resource_of(
            state,
            state.turn(&running.turn_id).expect("the turn"),
            store.hub(),
        )
    };
    assert_eq!(resource.steer_count, 1);
    let steers = resource.steers.clone().expect("the wire rows");
    assert_eq!(steers[0].prompt_chars, 10);
    assert_eq!(steers[0].prompt_preview, "and louder");

    // The transcript is the run's object end to end: the steer reached the model and the answer
    // carries both texts.
    assert_eq!(
        fixture
            .transcript(&id, &thread)
            .last()
            .map(|message| match message {
                HarnessMessage::Assistant { content, .. } => content.clone(),
                other => panic!("the last entry is the answer, got {other:?}"),
            })
            .as_deref(),
        Some("base>and louder")
    );
}

#[tokio::test]
async fn a_steer_never_addresses_an_idle_conversation() {
    let fixture =
        Fixture::with_scripts("steer-idle", vec![Script::Ok(Answer::new("queued", 1, 1))]);
    let id = fixture.session(None).await;
    let thread = fixture.thread(&id, &json!({"agent": "instruct"})).await;

    // Nothing is running, so this is an ordinary queued submit: steering is a property of a LIVE run
    // (`sessions.ts:6370-6378`).
    let queued = fixture
        .task(&id, "not a steer", Some(&thread))
        .await
        .expect("the submit is accepted");
    assert_eq!(queued.steered, None);
    assert_eq!(queued.steered_thread_id, None);
    assert_eq!(queued.status, SessionTurnStatus::Queued);
    fixture
        .wait_turn(&id, &queued.turn_id, SessionTurnStatus::Done)
        .await;

    // A conversation the caller invented is refused rather than silently opened.
    assert_eq!(
        refused(
            fixture
                .task(&id, "lost", Some("thread-nope"))
                .await
                .unwrap_err()
        ),
        (404, "Unknown thread \"thread-nope\".".to_owned())
    );

    // And an empty `threadId` is a shape error, not a missing conversation.
    assert_eq!(
        refused(
            fixture
                .submit(
                    &id,
                    contract::SubmitTaskRequest {
                        input: "lost".to_owned(),
                        attachments: Vec::new(),
                        references: Vec::new(),
                        thread_id: Some("   ".to_owned()),
                    },
                )
                .await
                .unwrap_err()
        ),
        (
            400,
            "`threadId` must be a non-empty string when present.".to_owned(),
        )
    );
}

// ---------------------------------------------------------------------------
// 5. Cancel, reset, delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancelling_a_session_aborts_the_run_and_drops_the_backlog() {
    let (_release_tx, release_rx) = mpsc::channel::<()>(1);

    let fixture = Fixture::with_scripts(
        "cancel",
        vec![
            Script::Park {
                release: release_rx,
                answer: Answer::new("never finished", 1, 1),
            },
            Script::Ok(Answer::new("never started", 1, 1)),
        ],
    );
    let store = &fixture.store;
    let id = fixture.session(None).await;
    let thread = fixture.thread(&id, &json!({"agent": "instruct"})).await;

    let running = fixture
        .task(&id, "run me", Some(&thread))
        .await
        .expect("the run opens");
    fixture.wait_running(&id, &running.turn_id).await;
    let queued = fixture
        .task_with_clip(&id, "queue me", &thread)
        .await
        .expect("the backlog entry");

    // `all: true` drops the backlog AND aborts the live run (`sessions.ts:4300-4360`).
    let cancelled = serde_json::from_value::<CancelSessionResult>(
        store
            .cancel_session(&id, true)
            .await
            .expect("a cancel is an answer, not a refusal"),
    )
    .expect("a cancel answer");

    assert!(cancelled.cancelled);
    assert_eq!(cancelled.session_id, id);
    assert_eq!(cancelled.dropped_queued_tasks, 1);
    assert_eq!(
        cancelled.active_turn_id.as_deref(),
        Some(running.turn_id.as_str())
    );

    // The aborted turn is `cancelled`, never `error` (`sessions.ts:7341-7348`). Its tally survives:
    // the usage fold is driven by the RECORDED events, and a cancel does not erase what a provider
    // request actually billed (`sessions.ts:7139-7144`).
    let record = fixture
        .wait_turn(&id, &running.turn_id, SessionTurnStatus::Cancelled)
        .await;
    assert_eq!(record.usage, Some(usage(1, 1)));
    assert_eq!(
        fixture.turn_status(&id, &running.turn_id),
        SessionTurnStatus::Cancelled
    );

    // A dropped backlog KILLS its turns as `cancelled` rows: the record is append-only until a reset,
    // so a reload still shows what was stopped.
    assert_eq!(
        fixture.turn_status(&id, &queued.turn_id),
        SessionTurnStatus::Cancelled
    );
    let page = serde_json::from_value::<contract::QueuedTaskPage>(
        store
            .session_queue(&id, &Query::new())
            .await
            .expect("the backlog is readable"),
    )
    .expect("a queue answer is a page");
    assert!(page.items.is_empty());
    assert_eq!(fixture.resource(&id).await.session.queue_depth, None);

    // The lane released its slot, and the parked run did not resurrect it.
    assert_eq!(fixture.thread_status(&id, &thread), "idle");
    assert_eq!(fixture.executor.observed().len(), 1);

    // A cancel with nothing live answers `false` rather than inventing a turn.
    let again = serde_json::from_value::<CancelSessionResult>(
        store.cancel_session(&id, false).await.expect("idle cancel"),
    )
    .expect("a cancel answer");
    assert!(!again.cancelled);
    assert_eq!(again.active_turn_id, None);
    assert_eq!(again.dropped_queued_tasks, 0);
}

#[tokio::test]
async fn reset_and_patch_refuse_a_live_run_and_a_quiet_session_empties() {
    let (release_tx, release_rx) = mpsc::channel::<()>(1);

    let fixture = Fixture::with_scripts(
        "reset",
        vec![Script::Park {
            release: release_rx,
            answer: Answer::new("busy", 1, 1),
        }],
    );
    let store = &fixture.store;
    let id = fixture.session(None).await;
    // A host conversation, because a reset discards the conversations a client can SEE, and
    // `discarded_threads` counts host threads exactly as `thread_count` does (`api-reads.ts:175-177`).
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;

    let busy = fixture
        .task(&id, "busy", Some(&thread))
        .await
        .expect("submit");
    fixture.wait_running(&id, &busy.turn_id).await;

    // A run in flight refuses the reset with TS's one sentence (`sessions.ts:8395-8400`).
    assert_eq!(
        refused(store.reset_session(&id).await.unwrap_err()),
        (
            409,
            "Cancel the running task before resetting the session.".to_owned(),
        )
    );

    // A live run refuses a DELETE of the conversation it runs in. The store states the SPECIFIC
    // reason first - the run is what a client has to wait for - and only falls back to the shared
    // busy sentence for a lane that is pumping without a registered run (`threads.rs:362-377`).
    assert_eq!(
        refused(store.delete_thread(&id, &thread).await.unwrap_err()),
        (
            409,
            format!(
                "Thread \"{thread}\" still has live work; wait for it to stop before deleting it."
            )
        )
    );

    // A PATCH is DECLARATIVE, so it lands while a run is live: TS gates the verbs that would move the
    // aggregate out from under a run (reset, a conversation delete, an agent-binding change), and
    // model/title writes are not among them (`sessions.ts:4253-4262` lists the gated verbs).
    let patched = store
        .update_session(&id, &json!({"title": "renamed mid-run"}))
        .await
        .expect("a declarative patch lands during a run");
    let patched = resource_of(&patched);
    assert_eq!(patched.session.title.as_deref(), Some("renamed mid-run"));
    assert_eq!(
        patched.session.status,
        SessionStatus::Running,
        "the answer restates the live aggregate rather than claiming idle"
    );

    release_tx.send(()).await.expect("the run is released");
    fixture
        .wait_turn(&id, &busy.turn_id, SessionTurnStatus::Done)
        .await;

    let reset = serde_json::from_value::<ResetSessionResult>(
        store
            .reset_session(&id)
            .await
            .expect("a quiet session resets"),
    )
    .expect("a reset answer");

    // The receipt counts everything that went with the old conversation, because the cascade is the
    // part a client has to report (`harness-api.ts:1222`).
    assert!(reset.reset);
    assert_eq!(reset.session_id, id);
    assert_eq!(reset.discarded_turns, 1);
    assert_eq!(reset.discarded_threads, 1);
    assert_eq!(reset.dropped_queued_tasks, 0);

    let after = fixture.resource(&id).await;
    assert_eq!(after.session.turn_count, 0);
    assert_eq!(after.session.thread_count, 0);
    assert_eq!(after.session.status, SessionStatus::Idle);
    assert_eq!(
        after.session.harness, "instruct",
        "a reset keeps the harness"
    );
    assert!(
        store
            .lock()
            .session(&id)
            .expect("the session survives its own reset")
            .thread(&thread)
            .is_none(),
        "a reset drops the roster rows, so their conversations and transcripts are gone entirely"
    );

    // The next submit starts the numbering again, since a reset begins a new conversation.
    fixture
        .executor
        .queue(vec![Script::Ok(Answer::new("fresh", 1, 1))]);
    let next = fixture
        .task(&id, "after reset", None)
        .await
        .expect("a reset session accepts work");
    let record = fixture
        .wait_turn(&id, &next.turn_id, SessionTurnStatus::Done)
        .await;
    assert_eq!(
        record.index, 0,
        "the turn high-water restarts with the conversation"
    );

    // A PATCH merges `agent_models` and replaces `title`; `{}` is a 400.
    let patched = store
        .update_session(&id, &json!({"agent_models": {"instruct": "model-a"}}))
        .await
        .expect("the first patch lands");
    assert_eq!(
        resource_of(&patched).agent_models["instruct"],
        "model-a",
        "the response is the whole resource, not a patch"
    );
    let patched = store
        .update_session(&id, &json!({"agent_models": {"research": "model-b"}}))
        .await
        .expect("the second patch lands");
    // The merge keeps BOTH overrides. The read states the whole live map - every roster member with
    // the model it actually runs - so the untouched members still report the deployment model
    // (`read.rs:317 agent_models_of`, which is why `agent_model()` never has to guess).
    assert_eq!(
        resource_of(&patched).agent_models,
        std::collections::BTreeMap::from([
            ("instruct".to_owned(), "model-a".to_owned()),
            ("planner".to_owned(), "merge/ds4-flash".to_owned()),
            ("research".to_owned(), "model-b".to_owned()),
            ("reviewer".to_owned(), "merge/ds4-flash".to_owned()),
            ("summarizer".to_owned(), "merge/ds4-flash".to_owned()),
            ("workspace".to_owned(), "merge/ds4-flash".to_owned()),
        ]),
        "a merge keeps the entry the body did not name"
    );

    // An explicit null clears the title; a title over the cap is refused (`sessions.ts:1770-1780`).
    let cleared = store
        .update_session(&id, &json!({"title": null}))
        .await
        .expect("the title clears");
    assert_eq!(resource_of(&cleared).session.title, None);
    assert_eq!(
        fixture.resource(&id).await.session.title,
        None,
        "the clear is readable, not just reported"
    );

    // A delete reports what it took with it, and the document a restart reads forgets the session.
    let deleted = serde_json::from_value::<DeleteSessionResult>(
        store.delete_session(&id).await.expect("the delete lands"),
    )
    .expect("a delete answer");
    assert_eq!(
        deleted,
        DeleteSessionResult {
            deleted: true,
            session_id: id.clone(),
            aborted_turns: 0,
            dropped_queued_tasks: 0,
        }
    );
    assert_eq!(persisted_ids(store), Vec::<String>::new());
    assert_eq!(
        refused(store.get_session(&id).await.unwrap_err()),
        (404, format!("Unknown session \"{id}\"."))
    );
}

// ---------------------------------------------------------------------------
// 6. Transcripts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_transcript_write_pages_the_document_and_refuses_a_stale_edit() {
    let fixture = Fixture::with_scripts("transcript", Vec::new());
    let store = &fixture.store;
    let id = fixture.session(None).await;
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;

    let draft = vec![
        HarnessMessage::User {
            content: "draft one".to_owned(),
            attachments: Vec::new(),

            included: None,
            message_id: None,
        },
        HarnessMessage::Assistant {
            content: "answer one".to_owned(),
            tool_calls: Vec::new(),
            reasoning: None,

            included: None,
            reasoning_included: None,
            message_id: None,
        },
    ];

    let (receipt, mut outcome) = {
        let mut inner = store.lock();
        store
            .put_transcript(
                &mut inner,
                &id,
                &thread,
                &UpdateThreadTranscriptRequest {
                    messages: json!([
                        {"role": "user", "content": "draft one"},
                        {"role": "assistant", "content": "answer one"},
                    ]),
                    if_revision: None,
                },
            )
            .expect("a valid document is installed")
    };

    // A whole-document write is a NEW revision of that conversation, and the receipt states the
    // counts plus where to read it back (`api.ts:1142-1196`).
    assert_eq!(
        receipt,
        TranscriptWriteReceipt {
            session_id: id.clone(),
            thread_id: thread.clone(),
            accepted_as: TranscriptAcceptedAs::Transcript,
            message_count: 2,
            revision: 1,
            invalid: None,
            links: Some(Store::transcript_links(&id, &thread)),
        }
    );

    store
        .commit(&store.lock(), &id, &mut outcome)
        .expect("the write is durable");

    // The three reads of the same document agree.
    let page = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.transcript_page(
            state,
            &thread,
            "instruct",
            &query(&[("role", "assistant")]),
            25,
            0,
        )
    };
    assert_eq!(page.items, vec![draft[1].clone()]);
    // `total` is the entry count of the WHOLE transcript, independent of the role filter
    // (`api.ts:1120-1140`).
    assert_eq!(page.total, 2);
    // TWO hub keys are in play for one conversation's transcript, and the test pins both so the
    // mismatch is visible to whoever unifies them: the WRITE bumps the BARE thread id
    // (`transcript.rs:331-345`), while every transcript READ addresses the composite
    // `session:thread` key (`read.rs:1044`, `read.rs:936`). See knowledge.md "T3" / the report.
    // TWO different "transcript revision" figures exist on purpose, and the test pins each to its
    // source: the thread record's own counter is what the next `if_revision` guard compares against
    // (`transcript.rs:364-376`), while the hub keeps a counter per conversation for the stream - the
    // METADATA read reports `transcript:<session:thread>` and the PAGED read reports
    // `transcript_messages:<session:thread>`, exactly the two kinds TS bumps for one write
    // (`api.ts:1176-1185`).
    assert_eq!(
        store.transcript_revision_of(&id, &thread),
        1,
        "one bump of the conversation's `transcript` counter per write"
    );
    assert_eq!(
        page.meta.revision,
        store.hub().revision(
            "transcript_messages",
            Some(&store::thread_key(&id, &thread))
        ),
        "the page reports the counter a `transcript_messages` subscriber watches"
    );
    assert_eq!(page.thread_id, thread);
    assert_eq!(page.agent, "instruct");
    assert_eq!(page.transcript_state, TranscriptState::Ok);
    // A role filter narrows the ITEMS without narrowing `total`, and a page shorter than the request
    // ENDS the collection: minting a cursor after a short page would make a filtered client loop
    // forever on an empty tail (`api.ts:3605`).
    assert_eq!(
        page.meta.limit, 1,
        "`limit` echoes what the page actually carries"
    );
    assert_eq!(page.meta.next_cursor, None);
    let tail = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.transcript_page(
            state,
            &thread,
            "instruct",
            &query(&[("role", "assistant")]),
            25,
            1,
        )
    };
    assert_eq!(tail.items, Vec::new());
    assert_eq!(
        tail.total, 2,
        "`total` is the whole transcript, filter or not"
    );
    assert_eq!(tail.meta.next_cursor, None);

    // The metadata read states counts and sizes with no content at all (`api-reads.ts:626-658`).
    let metadata = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.transcript_metadata(state, &thread, "instruct")
    };
    assert_eq!(metadata.message_count, 2);
    assert_eq!(
        metadata.chars, 19,
        "the exact character count of both entries"
    );
    assert_eq!(
        metadata.bytes,
        u64::try_from(serde_json::to_string(&draft).expect("the document").len())
            .expect("a byte count"),
        "the stored document's own length, not an estimate"
    );
    assert_eq!(metadata.revision, 1);
    assert_eq!(metadata.transcript_state, TranscriptState::Ok);
    assert_eq!(metadata.unreadable_reason, None);
    assert_eq!(metadata.stored_type, None);
    assert_eq!(metadata.role_counts.system, 0);
    assert_eq!(metadata.role_counts.user, 1);
    assert_eq!(metadata.role_counts.assistant, 1);
    assert_eq!(metadata.role_counts.tool, 0);

    // The document read answers with the same JSON, as text (`harness-api.ts:1526`).
    let document = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.transcript_document(state, &thread, "instruct")
    };
    assert_eq!(
        document.document,
        serde_json::to_string(&draft).expect("the document")
    );
    assert_eq!(document.transcript_state, TranscriptState::Ok);
    assert_eq!(document.revision, 1);
    assert_eq!(document.content_type.as_deref(), Some("application/json"));

    // An optimistic-concurrency guard: a stale `if_revision` is a 409 that names BOTH figures,
    // because a silent last-write-wins would throw away an edit the user can still see
    // (`api.ts:1150-1158`).
    let error = {
        let mut inner = store.lock();
        store
            .put_transcript(
                &mut inner,
                &id,
                &thread,
                &UpdateThreadTranscriptRequest {
                    messages: json!([]),
                    if_revision: Some(0),
                },
            )
            .unwrap_err()
    };
    assert_eq!(
        refused(error),
        (
            409,
            "This transcript is at revision 1; the write expected 0. Re-read before writing."
                .to_owned(),
        )
    );

    // A conversation the caller invented is refused, with the roster's own sentence.
    let error = {
        let mut inner = store.lock();
        store
            .put_transcript(
                &mut inner,
                &id,
                "thread-nope",
                &UpdateThreadTranscriptRequest {
                    messages: json!([]),
                    if_revision: None,
                },
            )
            .unwrap_err()
    };
    assert_eq!(
        refused(error),
        (404, "Unknown thread \"thread-nope\".".to_owned())
    );

    // The roster row reports the write: a revision, a message count, and a bounded preview of the
    // LAST entry (`harness-api.ts:1381`).
    let resource = serde_json::from_value::<ThreadResponse>(
        store
            .get_thread(&id, &thread)
            .await
            .expect("the conversation is readable"),
    )
    .expect("a thread answer is a thread resource")
    .thread;
    // TWO different "transcript revision" figures exist on purpose: the resource reports the hub's
    // per-conversation counter (the same figure the metadata read states), while the thread record's
    // own counter is what an `if_revision` guard compares against, and the PAGED read reports the
    // session counter (`transcript.rs:364-376`, `read.rs:936`, `read.rs:1044`).
    assert_eq!(resource.transcript_revision, 1);
    assert_eq!(resource.thread.message_count, 2);
    assert_eq!(resource.thread.preview.as_deref(), Some("answer one"));
    assert!(resource.thread.has_transcript);
    assert_eq!(resource.session_id, id);
    assert_eq!(
        resource.transcript_bytes,
        Some(
            u64::try_from(serde_json::to_string(&draft).expect("the document").len())
                .expect("a byte count")
        )
    );

    // The thread record's OWN counter is the figure the next `if_revision` guard compares against, and
    // it is deliberately NOT the hub counter a page reports: a failed write that advanced the record
    // would otherwise make every later edit look stale (`transcript.rs:364-376`).
    let document_revision = {
        let inner = store.lock();

        store.stored_transcript_revision_of(inner.session(&id).expect("the session"), &thread)
    };
    assert_eq!(document_revision, 1);
}

#[tokio::test]
async fn a_broken_transcript_is_accepted_as_raw_and_the_live_view_keeps_a_placeholder() {
    let fixture = Fixture::with_scripts("transcript-raw", Vec::new());
    let store = &fixture.store;
    let id = fixture.session(None).await;
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;

    // A structurally broken document is a SUCCESSFUL write: it is stored raw, explained, and the
    // live transcript becomes a readable placeholder (`api.ts:1160`, `sessions.ts:806-817`).
    let (receipt, mut outcome) = {
        let mut inner = store.lock();
        store
            .put_transcript(
                &mut inner,
                &id,
                &thread,
                &UpdateThreadTranscriptRequest {
                    messages: json!({"not": "a list"}),
                    if_revision: None,
                },
            )
            .expect("a broken draft is accepted, not refused")
    };
    assert_eq!(receipt.accepted_as, TranscriptAcceptedAs::Raw);
    assert_eq!(receipt.message_count, 1, "the placeholder is one entry");
    assert_eq!(receipt.revision, 1);
    let invalid = receipt.invalid.expect("the write explains itself");
    assert_eq!(invalid.stored_type, "object");
    assert!(!invalid.error.is_empty());

    store
        .commit(&store.lock(), &id, &mut outcome)
        .expect("the write is durable");

    let placeholder = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        assert!(state.transcript_is_unreadable(&thread));
        state
            .transcript_messages(&thread)
            .expect("the conversation")
    };
    assert_eq!(placeholder.len(), 1);
    match &placeholder[0] {
        HarnessMessage::System { content, .. } => {
            assert!(
                content.starts_with("[transcript unreadable: "),
                "the placeholder states why ({content})"
            );
            assert!(content.ends_with(']'));
        }
        other => panic!("the placeholder is a system entry, got {other:?}"),
    }

    // The roster row states the breakage rather than hiding it, and an unreadable conversation
    // reports ZERO live entries: its raw value is not a transcript (`api-reads.ts:511`).
    let row = store
        .get_thread(&id, &thread)
        .await
        .expect("the conversation is readable");
    assert_eq!(row["thread"]["transcript_state"], json!("unreadable"));
    assert_eq!(row["thread"]["message_count"], json!(0));
    assert_eq!(row["thread"]["has_transcript"], json!(false));
    assert!(
        row["thread"]["transcript_error"]
            .as_str()
            .is_some_and(|error| !error.is_empty()),
        "the row names why the transcript is unreadable"
    );

    // The document read keeps the RAW value, so an editor can show what the user actually wrote.
    let document = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.transcript_document(state, &thread, "instruct")
    };
    assert_eq!(document.document, r#"{"not":"a list"}"#);
    assert_eq!(document.transcript_state, TranscriptState::Unreadable);
    assert_eq!(document.revision, 1);

    // And the metadata read carries the same diagnosis (`api-reads.ts:626-658`).
    let metadata = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.transcript_metadata(state, &thread, "instruct")
    };
    assert_eq!(metadata.transcript_state, TranscriptState::Unreadable);
    assert_eq!(metadata.stored_type.as_deref(), Some("object"));
    assert_eq!(
        metadata.unreadable_reason.as_deref(),
        Some(invalid.error.as_str())
    );

    // A readable write replaces the broken document, and the invalid record goes with it.
    let (receipt, mut outcome) = {
        let mut inner = store.lock();
        store
            .put_transcript(
                &mut inner,
                &id,
                &thread,
                &UpdateThreadTranscriptRequest {
                    messages: json!([{"role": "user", "content": "repaired"}]),
                    if_revision: Some(1),
                },
            )
            .expect("the repair is installed")
    };
    assert_eq!(receipt.accepted_as, TranscriptAcceptedAs::Transcript);
    assert_eq!(receipt.revision, 2);
    assert!(receipt.invalid.is_none());

    store
        .commit(&store.lock(), &id, &mut outcome)
        .expect("the repair is durable");

    let state = store.lock();
    let state = state.session(&id).expect("the session");
    assert!(!state.transcript_is_unreadable(&thread));
    assert_eq!(
        state
            .transcript_messages(&thread)
            .expect("the conversation"),
        vec![HarnessMessage::User {
            content: "repaired".to_owned(),
            attachments: Vec::new(),

            included: None,
            message_id: None,
        }]
    );
    assert!(state.transcript_invalid(&thread).is_none());
}

// ---------------------------------------------------------------------------
// 7. Attachments and the pending cap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attachment_refusals_happen_before_any_turn_exists() {
    let fixture = Fixture::with_scripts(
        "attachments",
        vec![Script::Ok(Answer::new("clipped", 4, 2))],
    );
    let store = &fixture.store;
    let id = fixture.session(None).await;
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;

    let submit = |attachments: Vec<HarnessAttachment>, input: &str| {
        fixture.submit(
            &id,
            contract::SubmitTaskRequest {
                input: input.to_owned(),
                attachments,
                references: Vec::new(),
                thread_id: Some(thread.clone()),
            },
        )
    };

    // 415: a MIME the kind table does not claim for that kind (`attachments.ts:465`, and TS's
    // `ATTACHMENT_MEDIA_TYPE` regex decides the status).
    assert_eq!(
        refused(
            submit(
                vec![clip(
                    "a1",
                    "notes.txt",
                    "text/plain",
                    AttachmentKind::Image,
                    "[Image 1]",
                    8
                )],
                "see [Image 1]",
            )
            .await
            .unwrap_err()
        ),
        (
            415,
            "attachments[0].mime \"text/plain\" is not valid for kind \"image\".".to_owned(),
        )
    );

    // 415 again, one step later: the MIME is allowed for the kind, but the bytes are another media's,
    // so the magic-signature table refuses it (`attachments.ts:290-373`).
    assert_eq!(
        refused(
            submit(
                vec![clip("a1", "pic.jpg", "image/jpeg", AttachmentKind::Image, "[Image 1]", 8)],
                "see [Image 1]",
            )
            .await
            .unwrap_err()
        ),
        (
            415,
            "attachments[0].data does not match its declared MIME \"image/jpeg\" (magic signature mismatch)."
                .to_owned(),
        )
    );

    // 413: one clip past the per-attachment byte cap. The cheap base64 CEILING fires first, and its
    // sentence names the decoded cap (`attachments.ts:483-485`, `sessions.ts:1353`).
    let huge = clip(
        "a1",
        "big.png",
        "image/png",
        AttachmentKind::Image,
        "[Image 1]",
        4 * 1024 * 1024 + 1,
    );
    assert_eq!(
        refused(submit(vec![huge], "see [Image 1]").await.unwrap_err()),
        (
            413,
            "attachments[0].data is 4194305 bytes, above the 4194304-byte attachment limit."
                .to_owned(),
        )
    );

    // 400: a marker whose label disagrees with the kind (`attachments.ts:436-443`).
    assert_eq!(
        refused(
            submit(
                vec![clip(
                    "a1",
                    "pic.png",
                    "image/png",
                    AttachmentKind::Image,
                    "[File 1]",
                    8
                )],
                "see [File 1]",
            )
            .await
            .unwrap_err()
        ),
        (
            400,
            "attachments[0].marker \"[File 1]\" does not match kind \"image\".".to_owned(),
        )
    );

    // 400: five clips, one past `attachments_per_turn` (`attachments.ts:530-532`).
    let five = (1..=5)
        .map(|n| {
            clip(
                &format!("a{n}"),
                &format!("pic{n}.png"),
                "image/png",
                AttachmentKind::Image,
                &format!("[Image {n}]"),
                8,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        refused(submit(five, "many clips").await.unwrap_err()),
        (
            400,
            "attachments may contain at most 4 entries; received 5.".to_owned(),
        )
    );

    // 400: two clips with the same id, and the refusal names the SECOND occurrence (`:539-551`).
    let dupe = vec![
        clip(
            "a1",
            "one.png",
            "image/png",
            AttachmentKind::Image,
            "[Image 1]",
            8,
        ),
        clip(
            "a1",
            "two.png",
            "image/png",
            AttachmentKind::Image,
            "[Image 2]",
            8,
        ),
    ];
    assert_eq!(
        refused(submit(dupe, "two clips").await.unwrap_err()),
        (
            400,
            "attachments contain the duplicate id \"a1\".".to_owned()
        )
    );

    // Every refusal created nothing at all: no turn, no backlog, no transcript.
    assert_eq!(fixture.resource(&id).await.session.turn_count, 0);
    assert_eq!(fixture.resource(&id).await.session.queue_depth, None);

    // An empty prompt with NO clip is the one thing a submit cannot be (`sessions.ts:1282`).
    assert_eq!(
        refused(submit(Vec::new(), "   ").await.unwrap_err()),
        (400, "A task cannot be empty.".to_owned())
    );
    // An empty prompt WITH a clip is legitimate: the clip is the work.
    let clip_only = submit(
        vec![clip(
            "a1",
            "pic.png",
            "image/png",
            AttachmentKind::Image,
            "[Image 1]",
            8,
        )],
        "   ",
    )
    .await
    .expect("a clip alone is a task");
    assert_eq!(clip_only.status, SessionTurnStatus::Queued);

    // A slash command carries neither clips nor references (`sessions.ts:1305-1306`).
    assert_eq!(
        refused(
            submit(
                vec![clip(
                    "a1",
                    "pic.png",
                    "image/png",
                    AttachmentKind::Image,
                    "[Image 1]",
                    8
                )],
                "/help",
            )
            .await
            .unwrap_err()
        ),
        (400, "A slash command cannot carry attachments.".to_owned())
    );

    let record = fixture
        .wait_turn(&id, &clip_only.turn_id, SessionTurnStatus::Done)
        .await;

    // The clip rides the turn record, and the transcript's user entry keeps it (`types/index.ts:121`).
    assert_eq!(
        record.attachments.as_ref().map(|entries| entries.len()),
        Some(1)
    );
    assert_eq!(record.attachments.as_ref().expect("one")[0].id, "a1");
    assert_eq!(fixture.transcript(&id, &thread)[0], {
        HarnessMessage::User {
            content: String::new(),
            attachments: vec![clip(
                "a1",
                "pic.png",
                "image/png",
                AttachmentKind::Image,
                "[Image 1]",
                8,
            )],

            included: None,
            message_id: None,
        }
    });

    // The run was handed the bytes, and the identified turn read projects the BYTE-FREE descriptor
    // (`api-reads.ts:414-422`).
    assert_eq!(fixture.executor.observed()[0].attachments, 1);
    let resource = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store::read::turn_resource_of(
            state,
            state.turn(&clip_only.turn_id).expect("the turn"),
            store.hub(),
        )
    };
    assert_eq!(resource.attachment_count, 1);
    let descriptors = resource.attachments.expect("the descriptor list");
    assert_eq!(descriptors[0].id, "a1");
    assert!(descriptors[0].has_bytes);
    assert_eq!(descriptors[0].size, 8);
    assert_eq!(descriptors[0].name, "pic.png");
}

#[tokio::test]
async fn a_full_session_refuses_more_work_with_the_pending_cap() {
    let (_release_tx, release_rx) = mpsc::channel::<()>(1);

    // Only ONE script is needed: a parked run holds the lane, and the executor's default answer
    // covers any run the backlog starts after the assertions are done.
    let fixture = Fixture::with_scripts(
        "pending-cap",
        vec![Script::Park {
            release: release_rx,
            answer: Answer::new("busy", 1, 1),
        }],
    );
    let id = fixture.session(None).await;
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;

    let running = fixture
        .task(&id, "the parked run", Some(&thread))
        .await
        .expect("the first submit runs");
    fixture.wait_running(&id, &running.turn_id).await;

    // 100 clips queue behind it (a clip is never steered), filling the session's whole budget.
    for ordinal in 0..MAX_PENDING_TASKS {
        let accepted = fixture
            .task_with_clip(&id, &format!("queued {ordinal}"), &thread)
            .await
            .unwrap_or_else(|error| panic!("submit {ordinal} should be accepted: {error}"));
        assert_eq!(accepted.queue_position, Some(ordinal as u64 + 1));
    }

    let resource = fixture.resource(&id).await;
    assert_eq!(resource.session.queue_depth, Some(MAX_PENDING_TASKS as u64));

    // The 101st waiting task is refused BEFORE a turn exists, with the cap's own sentence
    // (`sessions.ts:8553-8561`).
    assert_eq!(
        refused(
            fixture
                .task_with_clip(&id, "one too many", &thread)
                .await
                .unwrap_err()
        ),
        (
            429,
            "A session cannot queue more than 100 pending tasks.".to_owned(),
        )
    );
    assert_eq!(
        fixture.resource(&id).await.session.queue_depth,
        Some(MAX_PENDING_TASKS as u64)
    );
    assert_eq!(
        fixture.turn_ids(&id).len(),
        MAX_PENDING_TASKS + 1,
        "the refusal created no turn"
    );
}

// ---------------------------------------------------------------------------
// 8. Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_command_turn_is_terminal_in_its_own_receipt_and_never_reaches_the_model() {
    let fixture = Fixture::with_scripts("commands", Vec::new());
    let _store = &fixture.store;
    let id = fixture.session(Some("commands")).await;

    let added = fixture
        .task(&id, "/task add buy milk", None)
        .await
        .expect("a command is accepted");

    // A command is an OPERATOR action: no queue, no pump, and the receipt is already terminal
    // (`sessions.ts:6111-6337`).
    assert_eq!(added.session_id, id);
    assert_eq!(added.status, SessionTurnStatus::Done);
    assert_eq!(added.queue_position, None);
    assert_eq!(added.steered, None);
    assert_eq!(
        added.thread_id, "",
        "a command acts on the session, so it has no conversation"
    );

    let record = fixture.turn(&id, &added.turn_id);
    assert_eq!(record.status, "done");
    assert_eq!(record.prompt, "/task add buy milk");
    assert!(
        record.started_at.is_some(),
        "the command turn is born running, never queued"
    );
    assert_eq!(
        record.usage, None,
        "no provider request means no tally at all"
    );
    assert_eq!(
        record
            .events
            .iter()
            .map(|entry| store::read::event_type_name(&entry.event))
            .collect::<Vec<_>>(),
        vec!["start", "tool_call", "tool_result"],
        "the command is recorded as one synthesized tool round"
    );

    // `HarnessToolCall.arguments` is a raw JSON STRING, so the command line rides INSIDE it
    // (`sessions.ts:6210-6214`), and the call name is the reserved `command` tool.
    let call = match &record.events[1].event {
        HarnessEvent::ToolCall { call, agent, .. } => {
            assert_eq!(agent, "instruct");
            call
        }
        other => panic!("the second event is a tool call, got {other:?}"),
    };
    assert_eq!(call.name, "command");
    assert_eq!(
        serde_json::from_str::<Value>(&call.arguments).expect("the arguments are JSON"),
        json!({"input": "/task add buy milk"})
    );
    let (content, is_error, outcome) = match &record.events[2].event {
        HarnessEvent::ToolResult {
            content,
            is_error,
            outcome,
            call_id,
            name,
            ..
        } => {
            assert_eq!(name, "command");
            // One synthesized call id pairs the call with its result: callId matching, never
            // positional (`sessions.ts:6156-6158`).
            assert_eq!(call_id.as_deref(), Some(call.id.as_str()));
            (content.clone(), *is_error, *outcome)
        }
        other => panic!("the third event is a tool result, got {other:?}"),
    };
    assert!(!is_error);
    assert_eq!(
        outcome,
        Some(typecrate_harness::contract::HarnessToolOutcome::Completed)
    );
    // A successful command's result text is the board line it produced.
    assert!(content.contains("buy milk"));

    // The board the command wrote is the store's own task host, and `/task list` prints TS's page
    // form: one line per row, then the counts, then the budget.
    let rows = fixture.task_host.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject, "buy milk");
    assert_eq!(
        content,
        format!(
            "#{} [ ] buy milk\n1 total, 0 completed, 1 pending\nbudget: used 1, limit 1000",
            rows[0].id
        )
    );

    // A command turn carries NO conversation, creates none, and leaves the session idle with an empty
    // backlog: a command is not a run (`sessions.ts:6267-6271`).
    assert!(record.thread_id.is_none());
    assert_eq!(fixture.resource(&id).await.session.thread_count, 0);
    assert_eq!(fixture.session_status(&id), "idle");
    assert_eq!(fixture.resource(&id).await.session.queue_depth, None);
    // The executor was never consulted.
    assert!(fixture.executor.observed().is_empty());
    // A command still increments the turn counter, because the turn IS the audit record.
    assert_eq!(fixture.resource(&id).await.session.turn_count, 1);

    // An unknown name is a FAILED result, never a 4xx and never a thrown error
    // (`command.ts:122-127`).
    let unknown = fixture
        .task(&id, "/nope", None)
        .await
        .expect("the refusal is an answer, not an error");
    assert_eq!(unknown.status, SessionTurnStatus::Error);
    let unknown = fixture.turn(&id, &unknown.turn_id);
    assert_eq!(unknown.status, "error");
    match &unknown.events[2].event {
        HarnessEvent::ToolResult {
            content,
            is_error,
            outcome,
            ..
        } => {
            assert_eq!(content, "unknown command: /nope");
            assert!(is_error);
            assert_eq!(
                *outcome,
                Some(typecrate_harness::contract::HarnessToolOutcome::Failed),
                "a failed command is `failed`, never `rejected`"
            );
        }
        other => panic!("expected a tool result, got {other:?}"),
    }
    // A failed command does NOT raise the session-wide banner.
    assert_eq!(fixture.resource(&id).await.session.last_error, None);

    // `/help` is the live registry in TS's column form.
    let help = fixture.task(&id, "/help", None).await.expect("help");
    let help = match &fixture.turn(&id, &help.turn_id).events[2].event {
        HarnessEvent::ToolResult { content, .. } => content.clone(),
        other => panic!("expected a tool result, got {other:?}"),
    };
    let expected: Vec<String> = [
        ("help", "/help [command]"),
        ("tools", "/tools [query|tool-name]"),
        (
            "task",
            "/task list | /task create <subject> [-d <description>] [-b \"blocked by\"] | /task get <id> | /task update <id> [-s <status>] [-o <owner>] [-m key=value ...] | /task delete <id>",
        ),
        ("websearch", "/websearch <query> [context]"),
        ("webfetch", "/webfetch <url> [instruction]"),
    ]
    .iter()
    .map(|(name, usage)| format!("/{name:<12} - {usage}"))
    .collect();
    assert_eq!(
        help,
        format!("Available commands:\n{}", expected.join("\n"))
    );

    // A bare `/name` is that command's usage card (`command-builtins.ts:26-34`).
    let card = fixture.task(&id, "/tools", None).await.expect("the card");
    let card = match &fixture.turn(&id, &card.turn_id).events[2].event {
        HarnessEvent::ToolResult { content, .. } => content.clone(),
        other => panic!("expected a tool result, got {other:?}"),
    };
    assert_eq!(
        card,
        "/tools - List enabled tools, search the catalogue (`/tools <query>`), or inspect one tool (`/tools <name>`).\nUsage: /tools [query|tool-name]"
    );

    // `/websearch` cannot run a tool body in this build, so it answers with the bridge note - a
    // deliberate simplification, stated rather than hidden (see knowledge.md "T3").
    let web = fixture
        .task(&id, "/websearch rust lifetimes", None)
        .await
        .expect("the note is an answer");
    let web = match &fixture.turn(&id, &web.turn_id).events[2].event {
        HarnessEvent::ToolResult { content, .. } => content.clone(),
        other => panic!("expected a tool result, got {other:?}"),
    };
    assert_eq!(
        web,
        "/websearch rust lifetimes\nThis build routes websearch through the integrated websearch tool; enable it with toolRequest and call it directly."
    );

    // Five command turns, all recorded in order, and the session read agrees.
    assert_eq!(fixture.resource(&id).await.session.turn_count, 5);
    assert_eq!(
        fixture
            .turn_ids(&id)
            .iter()
            .map(|turn_id| fixture.turn(&id, turn_id).prompt.clone())
            .collect::<Vec<_>>(),
        vec![
            "/task add buy milk",
            "/nope",
            "/help",
            "/tools",
            "/websearch rust lifetimes",
        ]
    );
}

#[tokio::test]
async fn the_command_list_a_session_offers_is_its_live_registry() {
    let fixture = Fixture::with_scripts("command-rows", Vec::new());
    let store = &fixture.store;
    let id = fixture.session(None).await;

    let page = serde_json::from_value::<contract::CommandPage>(
        store
            .session_commands(&id, &Query::new())
            .await
            .expect("the registry is readable"),
    )
    .expect("a commands answer is a page");

    // Sorted by name, the way `commandRowsOf` orders the registry (`api-reads.ts:770-781`).
    assert_eq!(
        page.items
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["help", "task", "tools", "webfetch", "websearch"]
    );
    assert_eq!(page.items[0].invocation, "/help");
    assert_eq!(page.items[0].usage.as_deref(), Some("/help [command]"));
    assert_eq!(
        page.items[0].description,
        "List available commands. `/help` lists all; `/help <command>` explains one."
    );
    assert_eq!(page.meta.next_cursor, None);
    assert_eq!(page.meta.limit, 5);

    // A slash line is recognized as a command only after trimming, and any registered name counts -
    // including an unknown one, whose refusal IS the answer (`command.ts:130-140`).
    assert!(store::commands::is_command_prompt("  /help me"));
    assert!(store::commands::is_command_prompt("/whatever"));
    assert!(!store::commands::is_command_prompt("help me"));
    assert!(!store::commands::is_command_prompt(""));
    assert!(store::commands::known_command("webfetch"));
    assert!(!store::commands::known_command("nope"));
}

// ---------------------------------------------------------------------------
// 9. Owner actions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_action_request_waits_answers_and_expires_with_its_run() {
    let fixture = Fixture::with_scripts("actions", Vec::new());
    let store = &fixture.store;
    let id = fixture.session(None).await;

    // The host the store hands a live run (`turns.rs:build_input`) is reachable the same way outside
    // one: the same inner Arc, the same hub, the same addressing.
    let host = Arc::new(StoreActionHost::new(
        store.inner_arc(),
        store.hub_arc(),
        &id,
        "turn-probe",
        None,
        "instruct",
    ));
    let question = json!({
        "question": "Which build should I ship?",
        "recommended": "debug",
        "options": [
            {"id": "release", "label": "Release"},
            {"id": "debug", "label": "Debug", "description": "slower, with symbols"},
        ],
    });

    let waiting = {
        let host = Arc::clone(&host);
        let question = question.clone();
        tokio::spawn(async move { host.request(question).await })
    };

    fixture
        .wait_until("the question to be parked", || {
            (fixture.waiting_actions(&id).len() == 1).then_some(())
        })
        .await;

    // The listed row carries the VERBATIM recorded request, so the options a client may answer with
    // are the options the model offered (`harness-api.ts:1283`).
    let request_id = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");
        let list = store.list_actions(state, &Query::new(), store.revision_of(&id));

        assert_eq!(list.total, 1);
        assert_eq!(list.pending.len(), 1);
        assert!(list.completed.is_empty(), "no `?completed=1`, no history");
        let pending = &list.pending[0];
        assert_eq!(pending.turn_id, "turn-probe");
        assert_eq!(pending.agent, "instruct");
        assert_eq!(pending.thread_id, None);
        assert_eq!(pending.request.question, "Which build should I ship?");
        assert_eq!(pending.request.recommended, "debug");
        assert!(
            pending.request.allow_custom,
            "free text is part of the contract"
        );
        assert_eq!(pending.request.options.len(), 2);
        assert_eq!(
            pending.request.options[1].description.as_deref(),
            Some("slower, with symbols")
        );
        pending.request.request_id.clone()
    };

    // The id is the RUNTIME's, never the caller's (`types/index.ts:865`): a submitted `requestId` is
    // replaced, which is why two questions with the same submitted id are two distinct questions.
    assert_ne!(request_id, "req-dup");
    assert_eq!(fixture.resource(&id).await.pending_action_count, 1);

    // An option the recorded request does not offer is refused, and the refusal lists the valid ones
    // (`api.ts:1295-1305`).
    let error = {
        let mut inner = store.lock();
        store
            .answer_action(
                &mut inner,
                &id,
                &request_id,
                &ActionChoiceInput::Option {
                    option_id: "nope".to_owned(),
                },
            )
            .unwrap_err()
    };
    assert_eq!(
        refused(error),
        (
            400,
            "Unknown option \"nope\". Valid options: release (Release), debug (Debug).".to_owned(),
        )
    );
    // An empty custom answer is refused too, and the question is still waiting.
    let error = {
        let mut inner = store.lock();
        store
            .answer_action(
                &mut inner,
                &id,
                &request_id,
                &ActionChoiceInput::Custom {
                    text: "   ".to_owned(),
                },
            )
            .unwrap_err()
    };
    assert_eq!(
        refused(error),
        (400, "A custom answer cannot be empty.".to_owned())
    );
    assert_eq!(fixture.waiting_actions(&id).len(), 1);

    // The answer resolves the parked promise, and the payload IS the recorded answer.
    let (result, mut outcome) = {
        let mut inner = store.lock();
        store
            .answer_action_option(&mut inner, &id, &request_id, "release")
            .expect("the owner answered")
    };
    assert!(result.ok);
    assert_eq!(result.request_id, request_id);
    assert!(result.resolved_at > 0);
    store
        .commit(&store.lock(), &id, &mut outcome)
        .expect("the answer is durable");

    let answered = waiting
        .await
        .expect("the run's task")
        .expect("the host answered");
    let answer: HarnessActionAnswer =
        serde_json::from_value(answered).expect("the promise payload is the recorded answer");
    assert_eq!(answer.request_id, request_id);
    assert_eq!(
        answer.choice,
        typecrate_harness::contract::HarnessActionChoice::Option {
            option_id: "release".to_owned(),
        }
    );
    assert_eq!(
        answer.source,
        typecrate_harness::contract::ActionAnswerSource::Owner
    );

    // A question cannot be answered twice: the entry survives as history, so the second attempt is
    // the "already answered" conflict (`sessions.ts:2750-2760`).
    let error = {
        let mut inner = store.lock();
        store
            .answer_action_option(&mut inner, &id, &request_id, "debug")
            .unwrap_err()
    };
    assert_eq!(
        refused(error),
        (
            409,
            format!("Action request \"{request_id}\" was already answered or has expired.")
        )
    );

    // The list now has no live question, and `?completed=1` shows the answer history. The guard is
    // scoped to a block so nothing is held across the `resource()` await below (`mod.rs` "THE LOCK").
    {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");
        let list = store.list_actions(state, &query(&[("completed", "1")]), store.revision_of(&id));

        assert!(list.pending.is_empty());
        assert_eq!(list.completed.len(), 1);
        assert_eq!(list.completed[0].request.request_id, request_id);
    }

    // `pending_action_count` is the ENTRY count, not the live count: TS reports
    // `record.pendingActions.size` (`api-reads.ts:227`), and an answered question stays in that map
    // until it is pruned. The live list is the one the actions route pages.
    assert_eq!(fixture.resource(&id).await.pending_action_count, 1);
    assert!(fixture.waiting_actions(&id).is_empty());

    // The cap is enforced before anything is recorded, so an over-limit question never appears in the
    // list its owner would read (`core/action.ts:181-186`).
    let mut waiting_tasks = Vec::new();
    for ordinal in 0..4 {
        let host = Arc::clone(&host);
        waiting_tasks.push(tokio::spawn(async move {
            host.request(json!({
                "question": format!("Question {ordinal}?"),
                "recommended": "yes",
                "options": [{"id": "yes", "label": "Yes"}, {"id": "no", "label": "No"}],
            }))
            .await
        }));
    }
    fixture
        .wait_until("four questions to be parked", || {
            (fixture.waiting_actions(&id).len() == 4).then_some(())
        })
        .await;
    assert_eq!(fixture.waiting_actions(&id).len(), 4);
    assert_eq!(
        host.request(json!({
            "question": "The fifth?",
            "recommended": "yes",
            "options": [{"id": "yes", "label": "Yes"}, {"id": "no", "label": "No"}],
        }))
        .await
        .expect_err("a fifth question is refused"),
        "This run already has 4 action requests waiting; answer one before asking again."
    );

    // And a question cannot outlive its run: the expiry rejects the promise with the sentence the
    // model reads back (`sessions.ts:7377-7382`). The settled ids are what the store announces, one
    // `action.closed` per settlement (`sessions.ts:2325-2335`, T16).
    let parked: Vec<String> = fixture
        .waiting_actions(&id)
        .iter()
        .map(|row| row.request.request_id.clone())
        .collect();
    let expired = {
        let mut inner = store.lock();
        let state = inner.session_mut(&id).expect("the session");

        state.expire_actions(store::session::ACTION_RUN_ENDED_MESSAGE, None)
    };
    assert_eq!(expired, parked);
    assert_eq!(expired.len(), 4);
    assert!(fixture.waiting_actions(&id).is_empty());

    for task in waiting_tasks {
        assert_eq!(
            task.await
                .expect("the run's task")
                .expect_err("an expiry is a failure"),
            "The run ended before the action request was answered. Ask again if the question is still required."
        );
    }
}

#[tokio::test]
async fn a_malformed_action_request_is_a_tool_failure_the_model_can_read() {
    let fixture = Fixture::with_scripts("action-parse", Vec::new());
    let store = &fixture.store;
    let id = fixture.session(None).await;

    let host = StoreActionHost::new(
        store.inner_arc(),
        store.hub_arc(),
        &id,
        "turn-x",
        None,
        "instruct",
    );

    // The runtime's parser is the authority on the SHAPE, and every refusal is a tool error the
    // model reads rather than a 4xx (`core/action.ts:140-176`).
    let cases: Vec<(Value, &str)> = vec![
        (
            json!({"recommended": "a", "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}]}),
            "An action request needs a question.",
        ),
        (
            json!({"question": "No array?", "recommended": "a"}),
            "An action request needs an options array.",
        ),
        (
            json!({"question": "One option?", "recommended": "a", "options": [{"id": "a", "label": "A"}]}),
            "An action request needs at least 2 options.",
        ),
        (
            json!({"question": "None?", "recommended": "a", "options": []}),
            "An action request needs at least 2 options.",
        ),
        (
            json!({"question": "Too many?", "recommended": "a", "options": [
                {"id": "a", "label": "A"}, {"id": "b", "label": "B"}, {"id": "c", "label": "C"},
                {"id": "d", "label": "D"}, {"id": "e", "label": "E"}, {"id": "f", "label": "F"},
                {"id": "g", "label": "G"},
            ]}),
            "An action request can carry at most 6 options.",
        ),
        (
            json!({"question": "Blank id?", "recommended": "a", "options": [{"id": " ", "label": "A"}, {"id": "b", "label": "B"}]}),
            "An action option needs an id.",
        ),
        (
            json!({"question": "Bad recommendation?", "recommended": "missing", "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}]}),
            "\"recommended\" must be one of the options: a, b.",
        ),
    ];

    for (request, expected) in cases {
        let refusal = host
            .request(request.clone())
            .await
            .expect_err("a refusal is expected");
        assert_eq!(refusal, expected, "the parser's sentence for {request}");
    }

    // Nothing was recorded by any refusal.
    assert!(fixture.waiting_actions(&id).is_empty());

    // Answering a question that was never asked is a 404 naming it.
    let error = {
        let mut inner = store.lock();
        store
            .answer_action_option(&mut inner, &id, "req-gone", "a")
            .unwrap_err()
    };
    assert_eq!(refused(error).0, 404);
}

// ---------------------------------------------------------------------------
// 10. The session toolbox
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_created_tool_is_listed_reported_and_removed_with_the_budget() {
    let fixture = Fixture::with_scripts("tools", Vec::new());
    let store = &fixture.store;
    let id = fixture.session(None).await;

    // The fixture's catalogue, which is the only tool surface this session can project.
    assert_eq!(
        fixture
            .runtime
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["probe", "gizmo"]
    );

    // The catalogue rows are the runtime's, projected with this session's facts. `declared` is the
    // FROZEN definition's say-so; `enabled` is the runtime offer minus what this session switched
    // off (`tools.rs:106 tools_for`) - so an unclaimed catalogue tool is enabled but not declared.
    let (rows, total) = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.session_tool_rows(state, &Query::new(), 25, 0)
    };
    assert_eq!(total, 2);
    assert_eq!(
        rows.iter()
            .map(|row| (row.tool.name.clone(), row.origin, row.declared, row.enabled))
            .collect::<Vec<_>>(),
        vec![
            (
                "probe".to_owned(),
                SessionToolOrigin::Catalogue,
                false,
                true
            ),
            (
                "gizmo".to_owned(),
                SessionToolOrigin::Catalogue,
                false,
                true
            ),
        ]
    );
    assert_eq!(rows[0].session_id, id);
    assert!(
        !rows[0].source_available,
        "a catalogue entry has no installed source"
    );
    assert_eq!(rows[0].source_revision, None);
    assert_eq!(
        Store::tool_budget(store.lock().session(&id).expect("the session")),
        contract::ResourceBudget { used: 0, limit: 32 },
    );

    // Creation is a DECLARATION: this build registers the tool and cannot compile a body, so the
    // receipt is metadata only (`harness-api.ts:1128`).
    // A created tool's name has to carry the session-tool prefix, and the refusal names the grammar
    // it failed (`tools.rs validate_tool_name`).
    let rejected = {
        let mut inner = store.lock();
        let state = inner.session_mut(&id).expect("the session");

        Store::create_session_tool(
            state,
            store,
            &CreateSessionToolRequest {
                name: "counter".to_owned(),
                description: "Counts things.".to_owned(),
                tags: vec!["fixture".to_owned()],
                parameters: None,
                source: None,
                module_text: None,
                source_path: None,
                dependencies: None,
                visibility: None,
                replace: None,
            },
            None,
        )
        .unwrap_err()
    };
    assert_eq!(
        refused(rejected),
        (
            400,
            "A created tool name must start with \"tool\" and use letters and digits only, e.g. \"toolWordCount\". Got \"counter\".".to_owned(),
        )
    );

    let input = CreateSessionToolRequest {
        name: "toolCounter".to_owned(),
        description: "Counts things.".to_owned(),
        tags: vec!["fixture".to_owned()],
        parameters: Some(json!({"type": "object"})),
        source: Some("export const tool = 1;".to_owned()),
        module_text: Some("export const tool = 1;".to_owned()),
        source_path: None,
        dependencies: None,
        visibility: Some(typecrate_harness::contract::ToolVisibility::Searchable),
        replace: None,
    };

    let registration = {
        let mut inner = store.lock();
        let state = inner.session_mut(&id).expect("the session");

        Store::create_session_tool(state, store, &input, None).expect("the tool registers")
    };
    assert!(registration.created);
    assert!(!registration.replaced);
    assert_eq!(registration.name, "toolCounter");
    assert!(!registration.source_revision.is_empty());
    assert_eq!(registration.previous_revision, None);
    assert_eq!(
        registration.budget,
        contract::ResourceBudget { used: 1, limit: 32 }
    );
    commit_mutation(store, &id, &["tools"]);

    // The created tool joins the ledger, enabled and declared by this session alone.
    let (rows, total) = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.session_tool_rows(state, &Query::new(), 25, 0)
    };
    assert_eq!(total, 3, "a created tool is appended to the catalogue rows");
    let created = rows.last().expect("the created row");
    assert_eq!(created.tool.name, "toolCounter");
    assert_eq!(created.origin, SessionToolOrigin::Created);
    // `declared` is the FROZEN definition's membership, so a tool this session created is enabled and
    // source-available but still NOT declared - the ledger row is the only place it appears
    // (`api-reads.ts:830-860`). Worth a look in T4: a UI that gates an affordance on `declared` would
    // hide a session's own tools.
    assert!(!created.declared);
    assert!(created.enabled, "a created tool is enabled immediately");
    assert!(created.source_available);
    assert_eq!(
        created.source_revision.as_deref(),
        Some(registration.source_revision.as_str())
    );
    assert!(
        store
            .tools_for(store.lock().session(&id).expect("the session"))
            .contains(&"toolCounter".to_owned())
    );

    // The source route answers with the installed revision and the text, and nothing else.
    let source = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store
            .session_tool_source(state, "toolCounter")
            .expect("the source is installed")
    };
    assert_eq!(source.session_id, id);
    assert_eq!(source.name, "toolCounter");
    assert_eq!(source.revision, registration.source_revision);
    assert_eq!(
        source.module_text.as_deref(),
        Some("export const tool = 1;")
    );
    assert_eq!(source.source.as_deref(), Some("export const tool = 1;"));

    // An update REPLACES the fields it names and stamps a new revision, keeping the previous one.
    let updated = {
        let mut inner = store.lock();
        let state = inner.session_mut(&id).expect("the session");

        Store::update_session_tool(
            state,
            store,
            "toolCounter",
            &UpdateSessionToolRequest {
                description: Some("Counts things, loudly.".to_owned()),
                // A SOURCE change is what mints a new revision: a metadata-only edit keeps the
                // installed stamp, because the revision identifies the source (`tools.rs:518-533`).
                module_text: Some("export const tool = 2;".to_owned()),
                enabled: Some(false),
                ..Default::default()
            },
        )
        .expect("the tool updates")
    };
    assert!(!updated.created);
    // A update that supplies new source installs a NEW revision, and the receipt says so
    // (`harness-api.ts:1128`: `replaced` is "a new source revision went in", not "the name is new").
    assert!(updated.replaced);
    assert_eq!(
        updated.previous_revision.as_deref(),
        Some(registration.source_revision.as_str())
    );
    assert_ne!(updated.source_revision, registration.source_revision);
    commit_mutation(store, &id, &["tools"]);

    let (in_tools, row_enabled, description) = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");
        let detail = store
            .session_tool_detail(state, "toolCounter")
            .expect("the tool is still listed");

        (
            store.tools_for(state).contains(&"toolCounter".to_owned()),
            detail.summary.enabled,
            detail.summary.tool.description.clone(),
        )
    };
    assert!(!in_tools, "a disabled tool is off the model's list");
    assert!(!row_enabled);
    assert_eq!(description, "Counts things, loudly.");

    // Removing a created tool ends it: nothing is restored, because it was never inherited.
    let deleted = {
        let mut inner = store.lock();
        let state = inner.session_mut(&id).expect("the session");

        Store::delete_session_tool(state, store, "toolCounter").expect("the tool is removed")
    };
    assert_eq!(
        deleted,
        DeleteSessionToolResult {
            deleted: true,
            tool_id: "toolCounter".to_owned(),
            session_id: id.clone(),
            removed_created: true,
            restored_inherited: false,
        }
    );
    commit_mutation(store, &id, &["tools"]);

    let (rows, total) = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store.session_tool_rows(state, &Query::new(), 25, 0)
    };
    assert_eq!(total, 2);
    assert!(!rows.iter().any(|row| row.tool.name == "toolCounter"));
    assert_eq!(
        Store::tool_budget(store.lock().session(&id).expect("the session")),
        contract::ResourceBudget { used: 0, limit: 32 },
        "the budget is a live count, not a high-water"
    );

    // A tool the session does not have is a 404 that names it, on both reads.
    let (detail_status, source_status) = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        (
            store
                .session_tool_detail(state, "missing")
                .unwrap_err()
                .status,
            store
                .session_tool_source(state, "missing")
                .unwrap_err()
                .status,
        )
    };
    assert_eq!(detail_status, 404);
    assert_eq!(source_status, 404);

    // The harness's own control tools can never be switched off, and the frozen definition's tools
    // are protected with them (`api-reads.ts:104-106`).
    assert!(store::read::is_protected_tool_name("toolRequest"));
    assert!(store::read::is_protected_tool_name("tasks"));
    assert!(!store::read::is_protected_tool_name("probe"));
    let protected = {
        let inner = store.lock();
        let state = inner.session(&id).expect("the session");

        store::tools::protected_names(store, state)
    };
    assert!(protected.contains(&"toolRequest".to_owned()));
    // Deleting one is refused rather than silently hiding a control the model still sees.
    let error = {
        let mut inner = store.lock();
        let state = inner.session_mut(&id).expect("the session");

        Store::delete_session_tool(state, store, "toolRequest").unwrap_err()
    };
    assert_eq!(refused(error).0, 409);
}

// ---------------------------------------------------------------------------
// 11. Conversations and the stream identity checks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conversations_open_read_page_stop_and_cascade_their_descendants() {
    let fixture = Fixture::with_scripts("threads", Vec::new());
    let store = &fixture.store;
    let id = fixture.session(None).await;

    // An agent outside the frozen roster cannot own a conversation (`sessions.ts:2255-2262`).
    assert_eq!(
        refused(
            store
                .create_thread(&id, &json!({"agent": "nobody"}))
                .await
                .unwrap_err()
        ),
        (
            404,
            format!("Agent \"nobody\" is not a member of session \"{id}\"."),
        )
    );

    // A caller-supplied label is honored, and it RESERVES its ordinal so a later mint cannot collide
    // with it (`labels.rs:177-186`).
    let (payload, _revision) = store
        .create_thread(&id, &json!({"agent": "research", "label": "RH9"}))
        .await
        .expect("a roster member opens a conversation");
    let parent = serde_json::from_value::<ThreadSummary>(payload["thread"].clone())
        .expect("the create answer carries a roster row");
    assert_eq!(parent.agent, "research");
    assert_eq!(parent.origin, ThreadOrigin::Agent);
    assert_eq!(parent.status, ThreadLifecycle::Idle);
    assert_eq!(parent.sequence, 1);
    assert_eq!(parent.label.as_deref(), Some("RH9"));
    assert_eq!(parent.owner_thread_id, None);
    assert_eq!(parent.transcript_state, TranscriptState::Ok);
    assert_eq!(parent.message_count, 0);
    assert!(!parent.has_transcript);
    assert_eq!(parent.chars, None, "an empty transcript states no size");
    assert_eq!(parent.preview, None);
    assert_eq!(parent.transcript_error, None);

    // A child conversation names its owner, which is what the reference graph and the stop cascade
    // join on. Its minted label continues the RESERVED sequence.
    let (payload, _) = store
        .create_thread(
            &id,
            &json!({"agent": "research", "owner_thread_id": parent.id}),
        )
        .await
        .expect("a child opens");
    let child = serde_json::from_value::<ThreadSummary>(payload["thread"].clone())
        .expect("a thread answer carries a roster row");
    assert_eq!(child.sequence, 2);
    assert_eq!(child.owner_thread_id.as_deref(), Some(parent.id.as_str()));
    assert_eq!(child.label.as_deref(), Some("RH10"));

    // The full resource adds the invocation roster and the reference graph - still no messages.
    let resource = serde_json::from_value::<ThreadResponse>(
        store
            .get_thread(&id, &parent.id)
            .await
            .expect("the conversation is readable"),
    )
    .expect("a thread answer is a thread resource")
    .thread;
    assert_eq!(resource.session_id, id);
    assert_eq!(resource.thread.id, parent.id);
    assert_eq!(resource.invocation_ids, Vec::<String>::new());
    assert!(!resource.invocation_ids_truncated);
    assert_eq!(resource.invocation_id, None);
    assert_eq!(resource.origin_turn_id, None);
    assert_eq!(resource.references, None);
    assert_eq!(resource.connected_thread_ids, None);
    assert_eq!(
        resource.transcript_revision, 0,
        "never written is revision 0"
    );

    // The roster is in creation order (`api.ts:2440-2462`).
    let roster = serde_json::from_value::<contract::ThreadSummaryPage>(
        store
            .list_threads(&id, &Query::new())
            .await
            .expect("the roster is readable"),
    )
    .expect("a threads answer is a page");
    assert_eq!(roster.items.len(), 2);
    assert_eq!(roster.items[0].id, parent.id);
    assert_eq!(roster.items[1].id, child.id);
    assert_eq!(roster.meta.next_cursor, None);

    // A stop is a STATE, not a deletion: an idle conversation answers `cancelled: false` and is still
    // marked stopped (`sessions.ts:7612-7613`).
    let stopped = serde_json::from_value::<CancelThreadResult>(
        store
            .cancel_thread(&id, &child.id)
            .await
            .expect("a stop is an answer"),
    )
    .expect("a stop answer");
    assert!(!stopped.cancelled);
    assert_eq!(stopped.thread_id, child.id);
    assert_eq!(stopped.session_id, id);
    assert_eq!(stopped.status, ThreadLifecycle::Stopped);
    assert_eq!(stopped.dropped_queued_tasks, 0);
    assert_eq!(fixture.thread_status(&id, &child.id), "stopped");

    // Deleting the parent takes its descendants with it, and says what went with the conversation
    // (`harness-api.ts:1447`).
    let deleted = store
        .delete_thread(&id, &parent.id)
        .await
        .expect("the parent is removed");
    assert_eq!(deleted["deleted"], json!(true));
    assert_eq!(deleted["threadId"], json!(parent.id));
    assert_eq!(deleted["sessionId"], json!(id));
    assert_eq!(deleted["droppedQueued"], json!(0));

    for gone in [&parent.id, &child.id] {
        assert_eq!(
            refused(store.get_thread(&id, gone).await.unwrap_err()),
            (404, format!("Unknown thread \"{gone}\"."))
        );
    }
    assert_eq!(
        refused(store.delete_thread(&id, &parent.id).await.unwrap_err()),
        (404, format!("Unknown thread \"{}\".", parent.id))
    );
    let roster = serde_json::from_value::<contract::ThreadSummaryPage>(
        store
            .list_threads(&id, &Query::new())
            .await
            .expect("the roster is readable"),
    )
    .expect("a threads answer is a page");
    assert!(roster.items.is_empty());
}

#[tokio::test]
async fn the_stream_identity_check_answers_for_sessions_threads_and_turns() {
    let fixture = Fixture::with_scripts("stream-exists", vec![Script::Ok(Answer::new("a", 1, 1))]);
    let store = &fixture.store;
    let id = fixture.session(None).await;
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;
    let receipt = fixture
        .task(&id, "work", Some(&thread))
        .await
        .expect("submit");
    fixture
        .wait_turn(&id, &receipt.turn_id, SessionTurnStatus::Done)
        .await;

    // A subscription is validated against the live maps, never by materializing a session
    // (`ports.rs:174-177`, `stream/mount.ts:410-430`).
    let exists: &dyn typecrate_harness::ports::StreamExists = store;

    assert!(exists.has_session(&id));
    assert!(!exists.has_session("nope"));
    assert!(exists.has_thread(&id, &thread));
    assert!(!exists.has_thread(&id, "thread-nope"));
    assert!(!exists.has_thread("nope", &thread));
    assert!(exists.has_turn(&id, &receipt.turn_id));
    assert!(!exists.has_turn(&id, "turn-nope"));
    assert!(!exists.has_turn("nope", &receipt.turn_id));

    // A disposed session answers `false`, which is what makes the stream drop the subscription
    // instead of spinning on it.
    store
        .delete_session(&id)
        .await
        .expect("the session is disposed");
    assert!(!exists.has_session(&id));
    assert!(!exists.has_thread(&id, &thread));
    assert!(!exists.has_turn(&id, &receipt.turn_id));
}

#[tokio::test]
async fn an_unfocused_submit_opens_its_own_host_conversation() {
    let fixture = Fixture::with_scripts(
        "host",
        vec![
            Script::Ok(Answer::new("a", 1, 1)),
            Script::Ok(Answer::new("b", 1, 1)),
        ],
    );
    let store = &fixture.store;
    let id = fixture.session(None).await;

    let first = fixture.task(&id, "one", None).await.expect("first");
    fixture
        .wait_turn(&id, &first.turn_id, SessionTurnStatus::Done)
        .await;
    let second = fixture.task(&id, "two", None).await.expect("second");
    fixture
        .wait_turn(&id, &second.turn_id, SessionTurnStatus::Done)
        .await;

    // Two unfocused submits are two conversations, each with its own ordinal and its own lane, so
    // neither submit can see the other's history (`sessions.ts:5905-5915`).
    assert_ne!(first.thread_id, second.thread_id);
    assert!(first.thread_id.starts_with("thread-"));
    assert!(second.thread_id.starts_with("thread-"));

    let roster = serde_json::from_value::<contract::ThreadSummaryPage>(
        store
            .list_threads(&id, &Query::new())
            .await
            .expect("the roster is readable"),
    )
    .expect("a threads answer is a page");
    assert_eq!(roster.items.len(), 2);
    assert_eq!(roster.items[0].id, first.thread_id);
    assert_eq!(roster.items[0].agent, "instruct");
    assert_eq!(roster.items[0].origin, ThreadOrigin::Host);
    assert_eq!(roster.items[0].owner_thread_id, None);
    assert_eq!(roster.items[0].sequence, 1);
    // The display label is bound at CREATION and minted per AGENT CODE: two conversations of one
    // agent get consecutive ordinals (`sessions.ts:5917-5921`, `labels.rs:165-171`).
    assert_eq!(roster.items[0].label.as_deref(), Some("IT1"));
    assert_eq!(roster.items[1].label.as_deref(), Some("IT2"));

    // Each conversation holds only its own transcript, which is the point of the lane.
    let content_of = |thread_id: &str| {
        fixture
            .transcript(&id, thread_id)
            .iter()
            .map(|entry| match entry {
                HarnessMessage::User { content, .. }
                | HarnessMessage::Assistant { content, .. } => content.clone(),
                other => panic!("unexpected entry {other:?}"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(content_of(&first.thread_id), vec!["one", "a"]);
    assert_eq!(content_of(&second.thread_id), vec!["two", "b"]);
    assert_eq!(fixture.resource(&id).await.session.thread_count, 2);
}

// ---------------------------------------------------------------------------
// T7a - the live host view, the session's own endpoint, stop-all, and steering
// that outlives a failed run
// ---------------------------------------------------------------------------

/// The LIVE `display.ops` channel and the display snapshot carry the SAME section ids.
///
/// The fold has no turn record (`ProjectorFactory::create()` takes none), so it mints `s{ordinal}` and
/// stamps `turnId: ""` on every op (`projector.rs:418-434`), while the wire identity is
/// `${turnId}:${ordinal}` (`turn-projector.ts:698`). Before T7a only the REST read re-stamped
/// (`store/read.rs`'s display merge), so a listener that applied the ops it streamed and then read the
/// snapshot had two different id spaces for one turn. Both readers now run through
/// `projector::stamp_display_ops` / `projector::section_wire_id`, which this test pins against the REAL
/// projector with a scripted run.
#[tokio::test]
async fn the_live_display_ops_carry_the_snapshot_section_ids() {
    let factory: Arc<dyn ProjectorFactory> =
        typecrate_harness::projector::TurnProjectorFactory::new();
    let fixture = Fixture::with_fold(
        "display-identity",
        vec![Script::Ok(Answer::new("the answer", 3, 2))],
        factory,
    );
    let id = fixture.session(None).await;

    // Every `display.ops` frame the recorder publishes, exactly as a stream listener sees them.
    let frames: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&frames);
    let _subscription = fixture.store.hub().subscribe(Arc::new(
        move |event: &typecrate_harness::events::HubEvent| {
            if event.kind == "display.ops" {
                sink.lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(json!({
                        "turn_id": event.turn_id,
                        "session_id": event.session_id,
                        "ops": event.ops,
                    }));
            }
        },
    ));

    let receipt = fixture
        .task(&id, "say something", None)
        .await
        .expect("the submit is accepted");

    fixture
        .wait_until("the turn to answer", || {
            (fixture.turn_status(&id, &receipt.turn_id) == SessionTurnStatus::Done).then_some(())
        })
        .await;

    // The snapshot the display read builds (`GET /turns/:id/display` reads exactly this fold).
    let display = {
        let inner = fixture.store.lock();

        inner
            .session(&id)
            .expect("the session")
            .turn(&receipt.turn_id)
            .expect("the turn")
            .display()
    };
    let snapshot_ids: Vec<String> = display["sections"]
        .as_array()
        .expect("a display document carries sections")
        .iter()
        .map(|section| section["id"].as_str().expect("a section id").to_owned())
        .collect();

    assert!(
        !snapshot_ids.is_empty(),
        "a streamed answer mints at least the root flow: {display}"
    );
    assert!(
        snapshot_ids
            .iter()
            .all(|section| section.starts_with(&format!("{}:", receipt.turn_id))),
        "every snapshot id is `{}`-prefixed, got {snapshot_ids:?}",
        receipt.turn_id
    );

    let seen = frames
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    assert!(!seen.is_empty(), "a running turn publishes `display.ops`");

    let mut live_ids: Vec<String> = Vec::new();

    for frame in &seen {
        assert_eq!(frame["session_id"], json!(id));
        assert_eq!(frame["turn_id"], json!(receipt.turn_id));

        for op in frame["ops"].as_array().expect("ops are an array") {
            // No live op still carries the seam's empty turn id, at the top level or inside an
            // embedded section / step payload.
            assert!(
                !op.to_string().contains("\"turnId\":\"\""),
                "a live op must state the turn it belongs to, got {op}"
            );

            for key in ["sectionId", "parentSectionId"] {
                if let Some(section) = op.get(key).and_then(Value::as_str) {
                    // The turn-level `usage` frame addresses the whole turn, not a section
                    // (`projector.rs:1152`, `sectionId: "turn"`), and that sentinel is not an id the
                    // snapshot mints - it must survive the stamp unchanged.
                    if section == "turn" {
                        continue;
                    }

                    assert!(
                        section.starts_with(&format!("{}:", receipt.turn_id)),
                        "the live `{key}` `{section}` is stamped with the turn id, got {op}"
                    );

                    if !live_ids.contains(&section.to_owned()) {
                        live_ids.push(section.to_owned());
                    }
                }
            }

            // A `section:open` carries the whole section payload, whose `id` is the same identity.
            if let Some(section) = op.pointer("/section/id").and_then(Value::as_str) {
                assert!(
                    section.starts_with(&format!("{}:", receipt.turn_id)),
                    "an opened section announces its wire id, got {op}"
                );

                if !live_ids.contains(&section.to_owned()) {
                    live_ids.push(section.to_owned());
                }
            }
        }
    }

    live_ids.sort();
    let mut expected = snapshot_ids.clone();

    expected.sort();

    assert_eq!(
        live_ids, expected,
        "the ids a listener applied from the stream are the ids the snapshot prints"
    );
}

/// `SessionResource.endpoint` is `providerDefaultsOf(record.settings)` (`api-reads.ts:225`): the
/// SESSION's merged pair plus the presence of the credential THAT session holds. The deployment's own
/// defaults are `/config`'s document. The three cases below are the whole credential rule as it reads
/// on this member: same origin inherits the deployment key, a foreign origin never does, and a key the
/// client supplied is the session's own (and never reaches the document).
#[tokio::test]
async fn the_session_endpoint_is_the_sessions_own_provider_view() {
    let fixture = Fixture::with_scripts("endpoint-view", vec![Script::Ok(Answer::new("x", 1, 1))]);
    let store = fixture.store.clone();

    // A keyed deployment, pushed the way `PATCH /config` pushes it.
    store.set_host_view(
        store.registry(),
        store.agents(),
        typecrate_harness::config::ServerConfig {
            api_key: "deployment-key".to_owned(),
            ..typecrate_harness::config::ServerConfig::default()
        },
    );

    let deployment = typecrate_harness::config::ServerConfig::default();

    // (a) No session settings at all: the merged pair IS the deployment's, and so is its key.
    let inherited = resource_of(
        &store
            .create_session(&CreateSessionRequest {
                harness: "instruct".to_owned(),
                title: None,
                settings: None,
            })
            .await
            .expect("a session on the deployment endpoint"),
    );

    assert_eq!(
        inherited.endpoint,
        contract::ProviderDefaults {
            base_url: deployment.base_url.clone(),
            model: deployment.model.clone(),
            has_api_key: true,
        },
        "a session on the configured origin runs with the deployment credential"
    );

    // (b) The regression: a session pointed at a FOREIGN origin, given no key. The scoped-credential
    // rule refuses to move the operator's secret, so `has_api_key` is false - and the block states the
    // session's own base URL and model, not the deployment's.
    let foreign = resource_of(
        &store
            .create_session(&CreateSessionRequest {
                harness: "instruct".to_owned(),
                title: None,
                settings: Some(contract::RunnerSettingsInput {
                    base_url: Some("http://elsewhere.example/v1".to_owned()),
                    api_key: None,
                    model: Some("other/model".to_owned()),
                }),
            })
            .await
            .expect("a session on its own endpoint"),
    );

    assert_eq!(
        foreign.endpoint,
        contract::ProviderDefaults {
            base_url: "http://elsewhere.example/v1".to_owned(),
            model: "other/model".to_owned(),
            has_api_key: false,
        },
        "a keyed deployment never makes a foreign session look authenticated"
    );
    assert_eq!(
        foreign.session.model, "other/model",
        "and the session reports the model it actually runs"
    );

    // (c) A key the client supplied for its own origin: present on the resource, absent from the disk.
    let keyed = resource_of(
        &store
            .create_session(&CreateSessionRequest {
                harness: "instruct".to_owned(),
                title: None,
                settings: Some(contract::RunnerSettingsInput {
                    base_url: Some("http://elsewhere.example/v1".to_owned()),
                    api_key: Some("session-key".to_owned()),
                    model: None,
                }),
            })
            .await
            .expect("a session with its own credential"),
    );

    assert_eq!(
        keyed.endpoint,
        contract::ProviderDefaults {
            base_url: "http://elsewhere.example/v1".to_owned(),
            model: deployment.model.clone(),
            has_api_key: true,
        },
        "the session's own key is what the presence flag states"
    );

    let document = std::fs::read_to_string(fixture.store.config().state_dir().join("state.json"))
        .expect("the document is written");

    assert!(
        !document.contains("session-key") && !document.contains("deployment-key"),
        "neither credential ever enters the durable document"
    );
}

/// A cancel-all on a session with no live run and no backlog still reports `cancelled: true` when it
/// stopped a host conversation: `sessions.ts:7765-7788` counts the threads `stopThread` actually
/// transitioned (`if (stopped > 0) cancelled = true`), and a second stop of the same conversation is
/// the no-op TS answers `false` for.
#[tokio::test]
async fn cancel_all_reports_a_stop_of_host_threads_on_an_idle_session() {
    let fixture = Fixture::with_scripts("cancel-all-idle", Vec::new());
    let id = fixture.session(None).await;
    let thread = fixture
        .thread(&id, &json!({"agent": "instruct", "origin": "host"}))
        .await;

    assert_eq!(fixture.session_status(&id), "idle");

    let stopped = serde_json::from_value::<CancelSessionResult>(
        fixture
            .store
            .cancel_session(&id, true)
            .await
            .expect("a cancel-all on an idle session succeeds"),
    )
    .expect("a cancel answer is the contract's result");

    assert!(
        stopped.cancelled,
        "stopping a live host conversation IS a cancellation"
    );
    assert_eq!(stopped.dropped_queued_tasks, 0);
    assert_eq!(
        stopped.active_turn_id, None,
        "no turn was running or recorded"
    );
    assert_eq!(
        fixture.thread_status(&id, &thread),
        "stopped",
        "and the conversation is marked stopped, exactly as `stopThread` leaves it"
    );

    // Every host thread is stopped, so the next stop-all has nothing left to do.
    let again = serde_json::from_value::<CancelSessionResult>(
        fixture
            .store
            .cancel_session(&id, true)
            .await
            .expect("the second cancel-all succeeds"),
    )
    .expect("a cancel answer is the contract's result");

    assert!(
        !again.cancelled,
        "an already-stopped conversation is the no-op `cancelled: false` (`api.ts:2341`)"
    );
}

/// `api-reads.ts:226` states `runtime_execution` as `context.runtime.threadExecution`, i.e. the
/// DEPLOYMENT's live configuration - not a constant. The store's host view is what carries it, so a
/// `HARNESS_THREAD_EXECUTION=worker` deployment (or a host that changes its mind) is visible on the
/// next session read.
#[tokio::test]
async fn the_session_resource_reports_the_live_thread_execution() {
    let fixture = Fixture::with_scripts("thread-execution", Vec::new());
    let store = fixture.store.clone();
    let id = fixture.session(None).await;

    assert_eq!(
        fixture.resource(&id).await.runtime_execution,
        "inline",
        "a store built from the trio alone states the built-in default"
    );

    store.set_host_view(
        store.registry(),
        store.agents(),
        typecrate_harness::config::ServerConfig {
            thread_execution: "worker".to_owned(),
            io_execution: "worker".to_owned(),
            ..typecrate_harness::config::ServerConfig::default()
        },
    );

    let resource = fixture.resource(&id).await;

    assert_eq!(
        resource.runtime_execution, "worker",
        "the session resource follows the LIVE runtime configuration"
    );

    // A session created AFTER the change resolves the same way.
    let created = resource_of(
        &store
            .create_session(&CreateSessionRequest {
                harness: "instruct".to_owned(),
                title: None,
                settings: None,
            })
            .await
            .expect("a session after the runtime view moved"),
    );

    assert_eq!(created.runtime_execution, "worker");
}

/// R5 preservation (`harness.ts:6826-6837`): steering a run drained and then FAILED is rolled back
/// with the transcript, so the store hands it back to the conversation instead of losing the user's
/// guidance. The hand-back is skipped for a stop - the conversation was stopped on purpose.
#[tokio::test]
async fn a_failed_run_hands_its_drained_steering_back_to_the_conversation() {
    let fixture = Fixture::with_scripts(
        "steer-requeue",
        vec![
            Script::SteerFail {
                error: "the provider refused".to_owned(),
            },
            Script::Steer {
                answer: Answer::new("second", 1, 1),
            },
        ],
    );
    let id = fixture.session(None).await;

    let first = fixture.task(&id, "start", None).await.expect("accepted");
    let thread = first.thread_id.clone();

    // The run owns its steer channel once the turn is `running` (`turns.rs:834-846`), which is the
    // gate `submit_queued` uses to decide steer-or-queue (`sessions.ts:6359-6504`).
    fixture
        .wait_until("the first turn to be running", || {
            (fixture.turn_status(&id, &first.turn_id) == SessionTurnStatus::Running).then_some(())
        })
        .await;

    let steered = fixture
        .task(&id, "guide me", Some(&thread))
        .await
        .expect("a text submit on a live conversation steers");

    assert_eq!(
        steered.steered,
        Some(true),
        "the guidance joined the live run, it did not open a turn"
    );

    fixture
        .wait_until("the drained run to fail", || {
            (fixture.turn_status(&id, &first.turn_id) == SessionTurnStatus::Error).then_some(())
        })
        .await;

    // The guidance is back where the next run of THIS conversation will find it.
    let mailbox = {
        let inner = fixture.store.lock();
        let state = inner.session(&id).expect("the session");

        (
            state
                .steer_mailbox
                .get(&thread)
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|record| record.prompt.clone())
                .collect::<Vec<_>>(),
            state.steer_active,
        )
    };

    assert_eq!(
        mailbox.0,
        vec!["guide me".to_owned()],
        "the drained guidance is re-queued on the conversation, in order"
    );
    assert_eq!(
        mailbox.1, 1,
        "and it counts as accepted-but-undelivered again"
    );

    // The NEXT run on that conversation receives it before its first model boundary, so nothing the
    // user paid for is lost, and the mailbox is drained as it is delivered.
    let second = fixture
        .task(&id, "try again", Some(&thread))
        .await
        .expect("the retry is accepted");

    fixture
        .wait_until("the retry to answer", || {
            (fixture.turn_status(&id, &second.turn_id) == SessionTurnStatus::Done).then_some(())
        })
        .await;

    let transcript = fixture.transcript(&id, &thread);
    let last = transcript.last().expect("the retry wrote a transcript");

    assert_eq!(
        match last {
            HarnessMessage::Assistant { content, .. } => content.clone(),
            other => panic!("the last entry is the answer, got {other:?}"),
        },
        "second>guide me",
        "the answer carries the steer the failed run had drained"
    );

    let cleared = {
        let inner = fixture.store.lock();
        let state = inner.session(&id).expect("the session");

        (
            state.steer_mailbox.contains_key(&thread),
            state.steer_active,
        )
    };

    assert_eq!(
        cleared,
        (false, 0),
        "delivery consumes the mailbox and the accepted counter"
    );
}
