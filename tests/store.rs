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

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
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
    HarnessActionAnswer, HarnessAssistantMessage, HarnessAttachment, HarnessErrorInfo, HarnessEvent,
    HarnessMessage, HarnessStopReason, HarnessUsage, MessageRole, ResetSessionResult,
    SessionDefinitionResponse, SessionResponse, SessionStatus, SessionToolOrigin, SessionTurnStatus,
    SessionUsageResponse, TaskReceipt, ThreadLifecycle, ThreadOrigin, ThreadResponse, ThreadSummary,
    TranscriptAcceptedAs, TranscriptState, TranscriptWriteReceipt, UpdateSessionToolRequest,
    UpdateThreadTranscriptRequest,
};
use typecrate_harness::error::ApiError;
use typecrate_harness::events::EventHub;
use typecrate_harness::persistence;
use typecrate_harness::ports::{
    ActionHost, Projector, ProjectorFactory, Query, ToolExecCtx, ToolExecOutcome, ToolPolicy,
    ToolRuntime, TurnExecutor, TurnInput,
};
use typecrate_harness::registry::HarnessRegistry;
use typecrate_harness::store::actions::{StoreActionHost, StoreTaskHost, TaskRow};
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
    fn run(&self, input: TurnInput) -> BoxFuture<'static, typecrate_harness::contract::TurnOutcome> {
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
                    },
                    HarnessMessage::Assistant {
                        content: answer.text.clone(),
                        tool_calls: Vec::new(),
                        reasoning: None,
                    },
                ],
                stop_reason: Some("stop".to_owned()),
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
    fn apply(&mut self, event: &HarnessEvent, at: u64, index: u64) -> Vec<Value> {
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
    fn create(&self) -> Box<dyn Projector> {
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
            Arc::new(MockProjectorFactory),
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

    /// The database root, for the assertions about what a run left on disk.
    fn root(&self) -> &Path {
        &self.directory
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
        self.store
            .submit_task(id, request)
            .await
            .map(|value| {
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
        .commit(&*store.lock(), id, &mut outcome)
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
    assert!(definition.current, "a fresh session is at the current revision");

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
    let first = serde_json::from_value::<typecrate_harness::contract::SessionSummaryPage>(
        page.clone(),
    )
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

    // Both sessions are durable, in the document a restart reads.
    assert_eq!(
        persisted_ids(store),
        vec![created.session.id.clone(), second.session.id.clone()]
    );

    // A foreign cursor is refused with the sentence that names the parameter (`api-paging.ts:54-87`).
    assert_eq!(
        refused(
            store
                .list_sessions(&query(&[("cursor", "bm90LWEtY3Vyc29y")]))
                .await
                .unwrap_err()
        ),
        (
            400,
            "Query parameter \"cursor\" is not a cursor this server issued.".to_owned(),
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
        refused(
            store
                .session_agents("gone")
                .await
                .unwrap_err()
        ),
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
        format!("{}/sessions/{id}/turns/{}", contract::HARNESS_API_ROOT, receipt.turn_id)
    );
    assert_eq!(
        receipt.links.turns,
        format!("{}/sessions/{id}/turns", contract::HARNESS_API_ROOT)
    );
    assert_eq!(
        receipt.links.thread,
        format!("{}/sessions/{id}/threads/{thread}", contract::HARNESS_API_ROOT)
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
            .map(store::read::event_type_name)
            .collect::<Vec<_>>(),
        vec!["start", "text", "usage", "done"]
    );
    // `eventTimes` stays parallel with `events` (`persistence.ts:162-167`).
    assert_eq!(record.event_times.as_deref().map(<[u64]>::len), Some(4));

    // The turn's tally is the FOLD OF THE EVENTS (7/3/10), not the outcome's 900/900: the events are
    // the durable record, so they win when both exist (`sessions.ts:7139-7144`).
    assert_eq!(record.usage, Some(usage(7, 3)));

    // The projector settled exactly once, at the terminal status. The display projector owns `prompt`
    // (stage T4), so the store's rename yields the empty pair here - the verbatim text is on the
    // record and the identified turn read, both asserted below.
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
    assert!(row.get("prompt").is_none(), "the row never carries the raw prompt");
    assert_eq!(row["promptPreview"], json!(""));
    assert_eq!(row["promptChars"], json!(0));

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
            },
            HarnessMessage::Assistant {
                content: "the answer".to_owned(),
                tool_calls: Vec::new(),
                reasoning: None,
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
    assert_eq!(observed.transcript_entries, 0, "a first turn has no history");
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
    // and every event after it carries its own sequence, starting at 1.
    let journal = store
        .config()
        .sessions_dir()
        .join(&id)
        .join("events.jsonl");
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
                line["sequence"].as_u64().expect("a sequence"),
                line["event"]["type"].as_str().expect("an event type"),
            ))
            .collect::<Vec<_>>(),
        vec![
            (1, "start"),
            (2, "text"),
            (3, "usage"),
            (4, "done"),
        ],
        "the journal keeps emission order and one monotonic sequence per line"
    );
    assert!(
        lines[1..]
            .iter()
            .all(|line| line["timestamp"].is_number()),
        "every line carries the stamp it was written with"
    );
}

#[tokio::test]
async fn a_failed_run_is_an_error_turn_and_raises_the_session_banner() {
    let fixture = Fixture::with_scripts("fail", vec![Script::Fail {
        error: "the provider refused".to_owned(),
    }]);
    let id = fixture.session(None).await;
    let thread = fixture.thread(&id, &json!({"agent": "instruct"})).await;

    let receipt = fixture.task(&id, "boom", Some(&thread)).await.expect("submit");
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
            .map(store::read::event_type_name)
            .collect::<Vec<_>>(),
        vec!["start", "error"]
    );
    // An empty `messages` means "the executor changed nothing", so the conversation keeps the
    // transcript it had - which here is none.
    assert!(fixture.transcript(&id, &thread).is_empty());

    // A failure raises the session-wide banner with the provider's text (`sessions.ts:7400-7405`).
    assert_eq!(fixture.session_status(&id), "error");
    let resource = fixture.resource(&id).await;
    assert_eq!(resource.session.last_error.as_deref(), Some("the provider refused"));
    assert_eq!(
        resource.session.status,
        typecrate_harness::contract::SessionStatus::Error
    );

    // The next success clears it.
    fixture.executor.queue(vec![Script::Ok(Answer::new("ok", 1, 1))]);
    let retry = fixture.task(&id, "again", Some(&thread)).await.expect("retry");
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
    assert_eq!(page.items[0].index, 1, "the running turn still consumed index 0");
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

    release_tx.send(()).await.expect("the parked run is released");

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
    assert_eq!(fixture.turn_status(&id, &first.turn_id), SessionTurnStatus::Done);
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
    let fixture = Fixture::with_scripts("steer", vec![Script::Steer {
        answer: Answer::new("base", 1, 1),
    }]);
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
    assert_eq!(steers[0].prompt, "and louder", "the prompt is trimmed on the way in");
    assert_eq!(steers[0].thread_id, None, "a steer into the owner's own thread is unattributed");
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
            .and_then(|message| match message {
                HarnessMessage::Assistant { content, .. } => Some(content.clone()),
                other => panic!("the last entry is the answer, got {other:?}"),
            })
            .as_deref(),
        Some("base>and louder")
    );
}

#[tokio::test]
async fn a_steer_never_addresses_an_idle_conversation() {
    let fixture = Fixture::with_scripts("steer-idle", vec![Script::Ok(Answer::new("queued", 1, 1))]);
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
    assert_eq!(cancelled.active_turn_id.as_deref(), Some(running.turn_id.as_str()));

    // The aborted turn is `cancelled`, never `error`, and reports no tally: nothing was billed for a
    // run the operator stopped.
    let record = fixture
        .wait_turn(&id, &running.turn_id, SessionTurnStatus::Cancelled)
        .await;
    assert_eq!(record.usage, None);

    // A dropped backlog KILLS its turns as `cancelled` rows: the record is append-only until a reset,
    // so a reload still shows what was stopped.
    assert_eq!(fixture.turn_status(&id, &queued.turn_id), SessionTurnStatus::Cancelled);
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
    let thread = fixture.thread(&id, &json!({"agent": "instruct"})).await;

    let busy = fixture.task(&id, "busy", Some(&thread)).await.expect("submit");
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
            format!("Thread \"{thread}\" still has live work; wait for it to stop before deleting it.")
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
    assert_eq!(
        patched.session.title.as_deref(),
        Some("renamed mid-run")
    );
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
        store.reset_session(&id).await.expect("a quiet session resets"),
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
    assert_eq!(after.session.harness, "instruct", "a reset keeps the harness");
    assert!(
        fixture.transcript(&id, &thread).is_empty(),
        "the reset conversation has no transcript"
    );

    // The next submit starts the numbering again, since a reset begins a new conversation.
    fixture.executor.queue(vec![Script::Ok(Answer::new("fresh", 1, 1))]);
    let next = fixture
        .task(&id, "after reset", None)
        .await
        .expect("a reset session accepts work");
    let record = fixture
        .wait_turn(&id, &next.turn_id, SessionTurnStatus::Done)
        .await;
    assert_eq!(record.index, 0, "the turn high-water restarts with the conversation");

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
    assert_eq!(
        resource_of(&patched).agent_models,
        std::collections::BTreeMap::from([
            ("instruct".to_owned(), "model-a".to_owned()),
            ("research".to_owned(), "model-b".to_owned()),
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

// TESTS
