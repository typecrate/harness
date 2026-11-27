//! Integration tests for the store's RESTART path (T3): what a boot reads back out of
//! `<root>/harness/state.json` plus the per-session journal, and what it deliberately does not keep.
//!
//! The expectations below come from the TypeScript source of record - `sessions.ts:1196-1240`
//! (`initialize`), `persistence.ts` (`repairInterrupted*`), and `sessions.ts:1618-1634` (the provider
//! credential is process state and is never written) - not from this Rust build's observed output.
//!
//! Fixtures live under the repo `Temp/typecrate-harness/store-restore/<pid>-<test>/` and are removed
//! when the guard drops.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::future::BoxFuture;
use serde_json::{Value, json};

use typecrate_harness::agents::AgentCatalogue;
use typecrate_harness::config::HarnessRuntimeSettings;
use typecrate_harness::contract::{
    CreateSessionRequest, HarnessEvent, HarnessMessage, HarnessToolCall, HarnessUsage,
    SubmitTaskRequest, TaskReceipt,
};
use typecrate_harness::events::EventHub;
use typecrate_harness::persistence;
use typecrate_harness::ports::{
    Projector, ProjectorFactory, ToolExecCtx, ToolExecOutcome, ToolPolicy, ToolRuntime,
    TurnExecutor, TurnInput,
};
use typecrate_harness::registry::HarnessRegistry;
use typecrate_harness::store::actions::StoreTaskHost;
use typecrate_harness::store::{self, Store, StoreConfig};

// ---------------------------------------------------------------------------
// Doubles: the executor answers once, with the transcript a real loop would have built
// ---------------------------------------------------------------------------

/// Every run this process's executor started, in order, so a test can prove whether a restart
/// drained a backlog on its own or only after the next submit. A plain `std::sync::Mutex` is enough:
/// the list is touched synchronously and never held across an await, which is the same rule the store
/// follows (`store/mod.rs` "THE LOCK").
struct RunLog(std::sync::Mutex<Vec<String>>);

impl RunLog {
    const fn new() -> Self {
        Self(std::sync::Mutex::new(Vec::new()))
    }

    fn note(&self, turn_id: &str) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(turn_id.to_owned());
    }

    fn ids(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

static RUNS: RunLog = RunLog::new();

/// The answer every scripted run produces, stated per test so the assertions stay exact.
struct Scripted {
    /// The assistant text the run answers with, and the tally its `usage` event reports.
    text: String,
    billed: HarnessUsage,
    /// `true` holds the run open forever, which is how a test keeps a lane busy so the NEXT submit
    /// is still waiting when the process "dies".
    park: bool,
}

impl Scripted {
    fn new(text: &str, input: u64, output: u64) -> Self {
        Self {
            text: text.to_owned(),
            billed: HarnessUsage {
                input,
                output,
                total: input + output,
                cached: None,
            },
            park: false,
        }
    }

    /// A run that answers like any other, but only after an unbounded wait: the caller ends the
    /// process instead.
    fn parked(text: &str) -> Self {
        Self {
            text: text.to_owned(),
            billed: HarnessUsage {
                input: 0,
                output: 0,
                total: 0,
                cached: None,
            },
            park: true,
        }
    }
}

/// The scripted executor: `Start`, `Text`, `Usage`, `Done` through the store's sink, then the whole
/// updated transcript back (`ports.rs:2887-2892`).
struct Executor {
    answers: std::sync::Mutex<Vec<Scripted>>,
}

impl Executor {
    fn new(answers: Vec<Scripted>) -> Self {
        Self {
            answers: std::sync::Mutex::new(answers),
        }
    }
}

impl TurnExecutor for Executor {
    fn run(
        &self,
        input: TurnInput,
    ) -> BoxFuture<'static, typecrate_harness::contract::TurnOutcome> {
        let answer = self
            .answers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(0);
        let model = input.provider.model.clone();
        let emit = Arc::clone(&input.emit);
        let prompt = input.prompt.clone();
        let signal = input.signal.clone();

        RUNS.note(&input.turn_id);

        Box::pin(async move {
            if answer.park {
                // Hold the lane: the pump stays inside this run until the process ends, exactly as a
                // provider request would.
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(3600)) => {}
                    () = signal.cancelled() => {}
                }
            }
            emit(HarnessEvent::Start {
                agent: "instruct".to_owned(),
                model: model.clone(),
                thread: None,
            });
            emit(HarnessEvent::Text {
                agent: "instruct".to_owned(),
                delta: answer.text.clone(),
                thread: None,
            });
            emit(HarnessEvent::Usage {
                agent: "instruct".to_owned(),
                usage: answer.billed.clone(),
                thread: None,
            });
            emit(HarnessEvent::Done {
                agent: "instruct".to_owned(),
                message: typecrate_harness::contract::HarnessAssistantMessage {
                    role: typecrate_harness::contract::MessageRole::Assistant,
                    content: answer.text.clone(),
                    reasoning: String::new(),
                    tool_calls: Vec::new(),
                    model,
                    stop_reason: typecrate_harness::contract::HarnessStopReason::Stop,
                    usage: Some(answer.billed.clone()),
                },
                pending: Vec::new(),
                thread: None,
            });

            typecrate_harness::contract::TurnOutcome {
                usage: Some(answer.billed),
                error: None,
                cancelled: false,
                messages: vec![
                    HarnessMessage::User {
                        content: prompt,
                        attachments: Vec::new(),

                        included: None,
                        message_id: None,
                    },
                    HarnessMessage::Assistant {
                        content: answer.text,
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

/// The catalogue: one entry, enough for the tool policy to be non-empty.
struct Runtime;

impl ToolRuntime for Runtime {
    fn declarations(&self, policy: &ToolPolicy) -> Vec<typecrate_harness::contract::ToolDecl> {
        if policy.enabled.contains(&"probe".to_owned()) {
            return vec![typecrate_harness::contract::ToolDecl {
                name: "probe".to_owned(),
                description: "Probe the fixture.".to_owned(),
                parameters: None,
            }];
        }

        Vec::new()
    }

    fn summaries(&self) -> Vec<typecrate_harness::contract::ToolSummary> {
        vec![typecrate_harness::contract::ToolSummary {
            name: "probe".to_owned(),
            group: "fixture".to_owned(),
            description: "Probe the fixture.".to_owned(),
            tags: Vec::new(),
            visibility: typecrate_harness::contract::ToolVisibility::Searchable,
            availability: None,
            session_bound: false,
        }]
    }

    /// The fixture has no listing rule: its one row is both listed and part of a session toolbox.
    fn catalogue_summaries(&self) -> Vec<typecrate_harness::contract::ToolSummary> {
        self.summaries()
    }

    fn detail(&self, _name: &str) -> Option<typecrate_harness::contract::ToolDetail> {
        None
    }

    fn execute(&self, ctx: ToolExecCtx) -> BoxFuture<'static, ToolExecOutcome> {
        Box::pin(async move {
            ToolExecOutcome {
                ok: true,
                content: ctx.name,
                panels: None,
                media: None,
                error: None,
                duration_ms: 0,
            }
        })
    }
}

/// The scripted fold: it counts what the store folds, which is what a restored row is judged on.
#[derive(Default)]
struct Fold {
    status: String,
    events: usize,
}

impl Projector for Fold {
    fn apply(
        &mut self,
        event: &HarnessEvent,
        _lane: Option<u64>,
        _at: u64,
        _index: u64,
    ) -> Vec<Value> {
        self.events += 1;
        vec![json!({"type": store::read::event_type_name(event)})]
    }

    fn summary(&self) -> Value {
        json!({"status": self.status, "eventCount": self.events})
    }

    fn display(&self) -> Value {
        json!({"events": self.events})
    }

    fn settle(&mut self, status: &str, _at: u64) {
        self.status = status.to_owned();
    }
}

struct FoldFactory;

impl ProjectorFactory for FoldFactory {
    fn create(&self, _seed: &typecrate_harness::ports::ProjectorSeed) -> Box<dyn Projector> {
        Box::new(Fold::default())
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// One disposable database root, opened as many times as a test needs - which is the whole point:
/// a RESTART is just a second `Store::open` over the same directory.
struct Fixture {
    directory: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = typecrate_harness::package_root::repo_root()
            .join("Temp")
            .join("typecrate-harness")
            .join("store-restore")
            .join(format!("{}-{sequence}-{name}", std::process::id()));

        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("a fixture directory is creatable");

        Self { directory }
    }

    /// Opens a store over this fixture's root. `api_key` is the DEPLOYMENT credential, which is the
    /// only place a key ever comes from (`sessions.ts:1618-1634`).
    fn open(&self, answers: Vec<Scripted>, api_key: &str) -> Store {
        let deployment = HarnessRuntimeSettings {
            base_url: "http://localhost:8800/providers/private/v1".to_owned(),
            api_key: api_key.to_owned(),
            model: "merge/ds4-flash".to_owned(),
        };

        Store::open(
            StoreConfig::at(&self.directory),
            deployment,
            Arc::new(AgentCatalogue::builtin()),
            Arc::new(Runtime),
            Arc::new(Executor::new(answers)),
            Arc::new(FoldFactory),
            Arc::new(HarnessRegistry::shipped(
                &HarnessRuntimeSettings {
                    base_url: "http://localhost:8800/providers/private/v1".to_owned(),
                    api_key: api_key.to_owned(),
                    model: "merge/ds4-flash".to_owned(),
                },
                Vec::new(),
            )),
            Arc::new(EventHub::new()),
            Some(Arc::new(StoreTaskHost::new())),
        )
    }

    /// The durable document's path, for the tests that edit what the next boot will read.
    fn document(&self, store: &Store) -> PathBuf {
        store
            .config()
            .state_dir()
            .join(persistence::HARNESS_DATABASE_FILE)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Creates one `instruct` session and returns its id.
async fn session(store: &Store, title: Option<&str>) -> String {
    let response = store
        .create_session(&CreateSessionRequest {
            harness: "instruct".to_owned(),
            title: title.map(str::to_owned),
            settings: None,
        })
        .await
        .expect("a shipped preset creates a session");

    response["session"]["id"]
        .as_str()
        .expect("the answer names the session")
        .to_owned()
}

/// Submits plain text and returns the receipt.
async fn task(store: &Store, id: &str, input: &str, thread: Option<&str>) -> TaskReceipt {
    serde_json::from_value::<TaskReceipt>(
        store
            .submit_task(
                id,
                SubmitTaskRequest {
                    input: input.to_owned(),
                    attachments: Vec::new(),
                    references: Vec::new(),
                    thread_id: thread.map(str::to_owned),
                },
            )
            .await
            .expect("the submit is accepted"),
    )
    .expect("a submit answer is a receipt")
}

/// Opens one HOST conversation (the kind `thread_count` counts) and returns its id.
async fn host_thread(store: &Store, id: &str) -> String {
    let (payload, _revision) = store
        .create_thread(id, &json!({"agent": "instruct", "origin": "host"}))
        .await
        .expect("a conversation opens");

    payload["thread"]["id"]
        .as_str()
        .expect("the answer names the conversation")
        .to_owned()
}

/// Polls for a terminal turn, because the pump is detached by design.
async fn wait_done(store: &Store, id: &str, turn_id: &str) -> persistence::SessionTurn {
    for _ in 0..250 {
        let record = {
            let inner = store.lock();

            inner
                .session(id)
                .expect("the session exists")
                .turn(turn_id)
                .expect("the turn exists")
                .record
                .clone()
        };

        if record.status == "done" {
            return record;
        }

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    panic!("the restored turn never settled: {turn_id}");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_restarted_store_restores_the_session_the_turn_and_the_transcript() {
    let fixture = Fixture::new("restart");
    let first = fixture.open(vec![Scripted::new("the answer", 7, 3)], "");

    let id = session(&first, Some("Kept conversation")).await;
    let thread = host_thread(&first, &id).await;
    let receipt = task(&first, &id, "do the thing", Some(&thread)).await;
    let durable = wait_done(&first, &id, &receipt.turn_id).await;

    assert_eq!(durable.status, "done");
    assert_eq!(durable.events.len(), 4);

    // A restart is a second `open` over the same root (`sessions.ts:1196 initialize`).
    drop(first);
    let second = fixture.open(Vec::new(), "");

    let restored = {
        let inner = second.lock();
        let state = inner
            .session(&id)
            .expect("the session survives the restart with its id");

        (
            state.record.title.clone(),
            state.record.status.clone(),
            state.turn_ids(),
            state.thread_ids(),
            state
                .transcript_messages(&thread)
                .expect("the conversation survives too"),
            state
                .turn(&receipt.turn_id)
                .expect("the turn survives")
                .record
                .clone(),
            state.turn(&receipt.turn_id).expect("the turn").row(),
            state.label_or_mint(&thread, "instruct"),
        )
    };

    // Identity, title and the frozen preset come back; a restored session is idle, never running.
    assert_eq!(restored.0.as_deref(), Some("Kept conversation"));
    assert_eq!(restored.1, "idle");
    assert_eq!(restored.2, vec![receipt.turn_id.clone()]);
    assert_eq!(restored.3, vec![thread.clone()]);

    // The transcript is the RUNTIME's object, restored verbatim: the user prompt and the answer.
    assert_eq!(
        restored.4,
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

    // The turn keeps its index, its trimmed prompt, its terminal status and the folded tally - the
    // events are replayed, so a reload states exactly what the live run stated (`turn-projector.ts`'s
    // `live == replay` invariant).
    let turn = restored.5;
    assert_eq!(turn.index, 0);
    assert_eq!(turn.prompt, "do the thing");
    assert_eq!(turn.status, "done");
    assert_eq!(turn.events.len(), 4);
    assert_eq!(
        turn.usage,
        Some(HarnessUsage {
            input: 7,
            output: 3,
            total: 10,
            cached: None,
        })
    );
    assert_eq!(restored.6["status"], json!("done"));
    assert_eq!(restored.6["eventCount"], json!(4));

    // The label survives, so a conversation never gets renamed by a restart.
    assert_eq!(restored.7, "IT1");

    // And the reads agree with the live state: the counters a client sees after a restart are the
    // ones it saw before.
    let page = serde_json::from_value::<typecrate_harness::contract::SessionSummaryPage>(
        second
            .list_sessions(&Default::default())
            .await
            .expect("the page"),
    )
    .expect("a list answer is a page");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, id);
    assert_eq!(page.items[0].turn_count, 1);
    assert_eq!(page.items[0].thread_count, 1);
    assert_eq!(page.items[0].queue_depth, None);
    assert_eq!(page.items[0].title.as_deref(), Some("Kept conversation"));

    // The journal reopens where it left off, so the next event continues the sequence instead of
    // restarting it (`journal.rs:142-152`).
    let journal = second
        .config()
        .sessions_dir()
        .join(&id)
        .join("events.jsonl");
    let lines = std::fs::read_to_string(&journal).expect("the journal is still there");
    assert_eq!(
        lines.lines().count(),
        5,
        "the header plus the four recorded events"
    );
}

#[tokio::test]
async fn a_durable_backlog_survives_a_restart_and_runs_in_order() {
    let fixture = Fixture::new("backlog");
    let first = fixture.open(vec![Scripted::parked("holds the lane")], "");

    let id = session(&first, None).await;
    let thread = host_thread(&first, &id).await;
    let running = task(&first, &id, "the parked run", Some(&thread)).await;

    // A clip is never steered, so this submit joins the lane's BACKLOG behind the live run.
    let clip = typecrate_harness::contract::HarnessAttachment {
        id: "att-1".to_owned(),
        kind: typecrate_harness::contract::AttachmentKind::Image,
        marker: "[Image 1]".to_owned(),
        name: "pic.png".to_owned(),
        mime: "image/png".to_owned(),
        size: 8,
        data: {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .encode([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
        },
    };
    let queued = serde_json::from_value::<TaskReceipt>(
        first
            .submit_task(
                &id,
                SubmitTaskRequest {
                    input: "queued work".to_owned(),
                    attachments: vec![clip],
                    references: Vec::new(),
                    thread_id: Some(thread.clone()),
                },
            )
            .await
            .expect("the backlog submit is accepted"),
    )
    .expect("a submit answer is a receipt");

    // Wait for the first run to be genuinely live, so the backlog cannot drain before the restart.
    for _ in 0..250 {
        let status = {
            let inner = first.lock();

            inner
                .session(&id)
                .expect("the session")
                .turn(&running.turn_id)
                .expect("the turn")
                .record
                .status
                .clone()
        };

        if status == "running" {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        {
            let inner = first.lock();

            inner.session(&id).expect("the session").pending_count()
        },
        1,
        "one task waits behind the live run"
    );

    // "Restart".
    drop(first);
    let second = fixture.open(
        vec![
            // The restored backlog runs FIRST, and the fresh submit runs behind it.
            Scripted::new("the restored one", 1, 1),
            Scripted::new("the next one", 1, 1),
        ],
        "",
    );

    // Both rows come back with the session, still recorded as `queued`: the accept-time write is what
    // the disk holds, and a turn that never started keeps that status.
    let restored = {
        let inner = second.lock();
        let state = inner
            .session(&id)
            .expect("the session survived the restart");

        (
            state.turn_ids().len(),
            state
                .turn(&queued.turn_id)
                .expect("the queued row survived")
                .record
                .status
                .clone(),
            state
                .turn(&queued.turn_id)
                .expect("the queued row")
                .record
                .attachments
                .as_ref()
                .map(|entries| entries.len()),
            state.pending_count(),
        )
    };
    assert_eq!(restored.0, 2);
    assert_eq!(restored.1, "queued");
    assert_eq!(
        restored.2,
        Some(1),
        "a durable row keeps the clip it arrived with"
    );

    // The durable backlog is the WHOLE pending set (`mod.rs::write_core_into` unions the legacy array
    // with every lane backlog, in turn-index order), and `restore_session` re-homes each restored turn
    // into the lane of the conversation it names (`sessions.ts:3202-3228`, F1). So the row that was
    // waiting behind the crashed run comes back as pending work - and NO pump starts on its own, which
    // is why it is still `queued` here.
    assert_eq!(
        restored.3, 1,
        "the lane backlog the document holds is pending work again"
    );
    let page = serde_json::from_value::<typecrate_harness::contract::QueuedTaskPage>(
        second
            .session_queue(&id, &Default::default())
            .await
            .expect("the queue is readable"),
    )
    .expect("a queue answer is a page");
    assert_eq!(
        page.items
            .iter()
            .map(|row| row.turn_id.clone())
            .collect::<Vec<_>>(),
        vec![queued.turn_id.clone()],
        "the queue page names the restored turn, and nothing else"
    );
    assert_eq!(page.items[0].position, 0);

    // The next submit joins the SAME lane behind it, so the restored turn is claimed first: peek-never-
    // shift FIFO (`sessions.ts:6593-6605`) is what keeps a restart from letting new work overtake old.
    let trigger = task(&second, &id, "the next submit", Some(&thread)).await;
    wait_done(&second, &id, &trigger.turn_id).await;

    let after = {
        let inner = second.lock();
        let state = inner.session(&id).expect("the session");

        (
            state
                .turn(&queued.turn_id)
                .expect("the restored backlog row")
                .record
                .status
                .clone(),
            state
                .turn(&running.turn_id)
                .expect("the interrupted run")
                .record
                .status
                .clone(),
        )
    };
    assert_eq!(
        after.0, "done",
        "the restored turn ran, in the order it was queued"
    );
    assert_eq!(
        // The second submit's accept-time write persisted the whole document, and by then the pump had
        // already claimed the first turn as `running`. The restart therefore REPAIRS that row into an
        // error with the interrupted explanation, rather than leaving a run that cannot exist
        // (`persistence.ts` interrupted-run repair, `store/mod.rs:2285-2293`).
        after.1,
        "error",
        "a turn the disk recorded as running is closed as an error on boot"
    );

    // The restart never invents a run: the only turns THIS process executed are the restored backlog
    // and its own submit, in that order. The log is process-wide (the test binary shares one process),
    // so the assertion slices to the runs started after the restart.
    let runs = RUNS.ids();
    assert_eq!(
        runs[runs.len() - 2..],
        [queued.turn_id.clone(), trigger.turn_id.clone()],
        "a boot runs nothing on its own, and the first run it does start is the turn that was queued first"
    );
}

#[tokio::test]
async fn a_running_turn_is_repaired_into_an_error_row_on_restart() {
    let fixture = Fixture::new("interrupted");
    let first = fixture.open(vec![Scripted::new("settled", 2, 2)], "");

    let id = session(&first, None).await;
    let thread = host_thread(&first, &id).await;
    let receipt = task(&first, &id, "a finished run", Some(&thread)).await;
    wait_done(&first, &id, &receipt.turn_id).await;
    drop(first);

    // Edit what the next boot will read: the turn is `running`, its session says the same, and one
    // tool call never got a result. This is exactly the state a killed process leaves behind.
    let path = fixture.document(&typecrate_harness::store::Store::open(
        StoreConfig::at(&fixture.directory),
        HarnessRuntimeSettings {
            base_url: "http://localhost:8800/providers/private/v1".to_owned(),
            api_key: String::new(),
            model: "merge/ds4-flash".to_owned(),
        },
        Arc::new(AgentCatalogue::builtin()),
        Arc::new(Runtime),
        Arc::new(Executor::new(Vec::new())),
        Arc::new(FoldFactory),
        Arc::new(HarnessRegistry::shipped(
            &HarnessRuntimeSettings {
                base_url: "http://localhost:8800/providers/private/v1".to_owned(),
                api_key: String::new(),
                model: "merge/ds4-flash".to_owned(),
            },
            Vec::new(),
        )),
        Arc::new(EventHub::new()),
        Some(Arc::new(StoreTaskHost::new())),
    ));
    let mut database =
        persistence::read_harness_database_file(&path).expect("the document reads back");
    {
        let record = database
            .sessions
            .iter_mut()
            .find(|record| record.id == id)
            .expect("the session is in the document");
        let turn = record.turns.first_mut().expect("one turn");

        turn.status = "running".to_owned();
        turn.started_at = Some(1_700_000_000_000);
        turn.events
            .push(typecrate_harness::persistence::SessionTurnEvent::new(
                HarnessEvent::ToolCall {
                    agent: "instruct".to_owned(),
                    call: HarnessToolCall {
                        id: "call-open".to_owned(),
                        name: "probe".to_owned(),
                        arguments: "{}".to_owned(),
                    },
                    thread: None,
                },
            ));
        record.status = "running".to_owned();
    }
    persistence::write_harness_database(&fixture.directory, &database, "restored-by-test")
        .expect("the edited document is durable");

    // Boot on top of it.
    let second = fixture.open(Vec::new(), "");
    let repaired = {
        let inner = second.lock();
        let state = inner.session(&id).expect("the session booted");

        (
            state.record.status.clone(),
            state
                .turn(&receipt.turn_id)
                .expect("the turn booted")
                .record
                .clone(),
        )
    };

    // An interrupted turn is closed as an ERROR row with the fixed explanation, never left running
    // (`persistence.ts:704-705`, `INTERRUPTED_TURN_MESSAGE`).
    assert_eq!(repaired.1.status, "error");
    assert!(
        !repaired.1.events.iter().any(|entry| matches!(&entry.event, HarnessEvent::ToolResult { call_id, .. } if call_id.as_deref() == Some("call-open")
        )) || repaired
            .1
            .events
            .iter()
            .any(|event| matches!(&event.event, HarnessEvent::ToolResult { is_error: true, .. })),
        "a dangling call is answered by a FAILURE result, never left open"
    );

    // And the session mirrors its turns: a record that says `running` with no live run becomes a
    // settled status, because the run is gone (`sessions.ts:3016`).
    assert_ne!(repaired.0, "running");
    let page = serde_json::from_value::<typecrate_harness::contract::SessionSummaryPage>(
        second
            .list_sessions(&Default::default())
            .await
            .expect("the page"),
    )
    .expect("a list answer is a page");
    assert_eq!(page.items[0].turn_count, 1);
    assert_eq!(page.items[0].thread_count, 1);
}

#[tokio::test]
async fn the_provider_credential_is_never_persisted() {
    let fixture = Fixture::new("credential");
    let first = fixture.open(
        vec![Scripted::new("answered", 1, 1)],
        "server-key-that-must-not-hit-disk",
    );

    let id = session(&first, None).await;

    // The create answer carries the key: the deployment pair resolved one for the configured origin
    // (`config::merge_settings`).
    let created = first.get_session(&id).await.expect("the session");
    assert_eq!(created["session"]["endpoint"]["has_api_key"], json!(true));
    assert_eq!(
        created["session"]["endpoint"]["base_url"],
        json!("http://localhost:8800/providers/private/v1")
    );

    // The document on disk has no key field at all - `HarnessSessionSettingsRecord` has no such
    // member, and a document that grows one is REFUSED (`persistence.ts:889-893`).
    let text = std::fs::read_to_string(fixture.document(&first)).expect("the document is text");
    assert!(
        !text.contains("server-key-that-must-not-hit-disk"),
        "the credential never enters the durable document"
    );
    assert!(!text.contains("\"apiKey\""));

    drop(first);
    let second = fixture.open(Vec::new(), "server-key-that-must-not-hit-disk");

    // The session's LIVE credential is gone: `restore_session` sets `provider_key: None` because the
    // document has no place for it, so the next provider call re-resolves it from the deployment
    // (`sessions.ts:1618-1634`, `mod.rs:2276-2278`).
    // The session's LIVE credential is gone: `restore_session` sets `provider_key: None` because the
    // document has no place for it, so the next provider call re-resolves it from the deployment
    // (`sessions.ts:1618-1634`, `mod.rs:2276-2278`). `settings_provider()` is NOT the right probe:
    // it synthesizes the endpoint pair from the deployment, exactly as TS's read does.
    let live_key = {
        let inner = second.lock();

        inner
            .session(&id)
            .expect("the session booted")
            .provider_key
            .is_some()
    };
    assert!(
        !live_key,
        "a restart loses the credential the session was created with"
    );

    // The RESOURCE figure now states the SESSION's own endpoint view - `api-reads.ts:225` calls
    // `providerDefaultsOf(record.settings)`, and the credential that made `has_api_key` true at create
    // time is memory-only - so after a restart the base URL and model survive (they are in the
    // document) while the key's presence honestly does not. This assertion used to read `true`, which
    // was the deployment-defaults bug this fix removes: it proved nothing about persistence, and now it
    // proves the credential is gone.
    let restored = second.get_session(&id).await.expect("the session booted");
    assert_eq!(restored["session"]["endpoint"]["has_api_key"], json!(false));
    assert_eq!(
        restored["session"]["endpoint"]["base_url"],
        json!("http://localhost:8800/providers/private/v1"),
        "the session's own endpoint pair is durable even when its credential is not"
    );
    assert_eq!(
        restored["session"]["endpoint"]["model"],
        json!("merge/ds4-flash")
    );
    assert_eq!(restored["session"]["id"], json!(id));

    // The DEPLOYMENT still offers its key, which is `/config`'s document and a different fact from
    // the session's `endpoint` block.
    let live_key = {
        let inner = second.lock();

        inner.settings().api_key
    };
    assert_eq!(
        live_key, "server-key-that-must-not-hit-disk",
        "the deployment pair is untouched by the session losing its credential"
    );

    // A deployment that boots WITHOUT a key restores the same session with no key available to the next
    // provider call either (`config::merge_settings` has nothing to fall back to).
    let redeployed = fixture.open(Vec::new(), "");
    let keyless = {
        let inner = redeployed.lock();

        inner
            .session(&id)
            .expect("the session booted again")
            .provider_key
            .is_none()
    };
    assert!(
        keyless,
        "a deployment without a key restores a keyless session"
    );
}

// ---------------------------------------------------------------------------
// The per-session `pluginSettings` slot: what a damaged or older one costs
//
// C3 added one additive member to the SESSION record (`HarnessSessionRecord::plugin_settings`,
// `persistence.rs:840-844`), typed as an OPAQUE `Value` rather than the settings record on purpose:
// the tolerant reader (`persistence::normalize_session_plugin_settings`, then
// `PluginSettingsStore::restore`) is what decides what survives, so a hand-edited or older document
// loses the SLOT and not every session in the document. A typed slot would fail serde for the whole
// record, which is the difference between "one operator's stray edit" and "the deployment does not
// boot".
// ---------------------------------------------------------------------------

/// A settings patch map, spelled as JSON so a call site reads like the request body it mirrors.
fn patch_map(entries: Value) -> serde_json::Map<String, Value> {
    entries
        .as_object()
        .expect("a settings patch is an object")
        .clone()
}

/// This conversation's EFFECTIVE value for one declared key, off its own row.
fn session_plugin_value(store: &Store, id: &str, plugin: &str, key: &str) -> Value {
    store
        .session_plugin_row(id, plugin)
        .expect("the session exists")
        .expect("the plugin ships")
        .values
        .get(key)
        .cloned()
        .unwrap_or(Value::Null)
}

/// Replaces ONE session record's `pluginSettings` member in the durable document and writes it back,
/// which is how a test becomes the thing the next boot reads. Everything else in the document is the
/// bytes the previous boot produced, so the case under test is the only variable.
fn restamp_slot(fixture: &Fixture, store: &Store, id: &str, slot: Option<Value>) {
    let mut database = persistence::read_harness_database_file(&fixture.document(store))
        .expect("the document reads back");
    let record = database
        .sessions
        .iter_mut()
        .find(|record| record.id == id)
        .expect("the session is in the document");

    record.plugin_settings = slot;

    persistence::write_harness_database(&fixture.directory, &database, "restamped-by-test")
        .expect("the edited document is durable");
}

/// The `pluginSettings` member one session record carries on DISK, after the next boot wrote it.
fn persisted_slot(store: &Store, id: &str) -> Option<Value> {
    let path = store
        .config()
        .state_dir()
        .join(persistence::HARNESS_DATABASE_FILE);

    persistence::read_harness_database_file(&path)
        .expect("the document is readable")
        .sessions
        .into_iter()
        .find(|record| record.id == id)
        .and_then(|record| record.plugin_settings)
}

#[tokio::test]
async fn a_malformed_session_plugin_slot_costs_the_slot_and_nothing_else() {
    let fixture = Fixture::new("plugin-slot");
    let first = fixture.open(Vec::new(), "");
    let id = session(&first, None).await;

    first
        .update_session_plugin_settings(
            &id,
            "context-compaction",
            &patch_map(json!({ "mode": "exclude" })),
        )
        .expect("a conversation may set its own value");
    first.save(&first.lock()).expect("the document is writable");
    assert_eq!(
        persisted_slot(&first, &id),
        Some(json!({ "context-compaction": { "mode": "exclude" } })),
        "the shape a C3 build writes, so the cases below start from a real document"
    );

    drop(first);

    // Nine ways a slot goes wrong, and the one answer they all get: the session boots, the plugin is
    // at its declared default, and the garbage is not resurrected by the next write. The first five
    // are dropped by the document reader (`normalize_session_plugin_settings`: the block must be an
    // object, a plugin's block must be an object, and only a finite number, a boolean or a string is
    // a value); the last four are dropped by the store's own restore (`restore` walks the EFFECTIVE
    // descriptor list and `check_value`s every entry), which is why a wrong type, an unknown plugin, an
    // unknown key and an out-of-range value all land in the same place.
    for (label, submitted) in [
        ("a string", json!("context-compaction")),
        ("an array", json!([{ "mode": "exclude" }])),
        ("a number", json!(7)),
        (
            "a plugin block that is not an object",
            json!({ "context-compaction": "exclude" }),
        ),
        (
            "values that are not values",
            json!({ "context-compaction": { "minTokens": null, "mode": { "any": "object" }, "contextLimit": [1] } }),
        ),
        (
            "a value of the wrong declared type",
            json!({ "context-compaction": { "contextLimit": "abc" } }),
        ),
        (
            "a plugin this build does not ship",
            json!({ "legacy-trimmer": { "enabled": true } }),
        ),
        (
            "a key no plugin declares",
            json!({ "context-compaction": { "nothingLikeThis": 1 } }),
        ),
        (
            "a value past the declared bound",
            json!({ "context-compaction": { "contextLimit": 1 } }),
        ),
    ] {
        let editor = fixture.open(Vec::new(), "");
        restamp_slot(&fixture, &editor, &id, Some(submitted));
        drop(editor);

        let booted = fixture.open(Vec::new(), "");

        // The session is there, and so is every read the pane makes: tolerance here means the
        // CONVERSATION still opens, which is the whole point of the opaque slot.
        let read = booted
            .get_session(&id)
            .await
            .unwrap_or_else(|error| panic!("{label}: the session must still load, got {error:?}"));
        assert_eq!(read["session"]["id"], json!(id), "{label}");
        assert_eq!(
            booted
                .session_plugin_revision(&id)
                .expect("the session booted"),
            0,
            "{label}: a dropped slot is not a write, so the counter still starts at 0"
        );
        assert_eq!(
            session_plugin_value(&booted, &id, "context-compaction", "mode"),
            json!("summarize"),
            "{label}"
        );
        assert_eq!(
            session_plugin_value(&booted, &id, "context-compaction", "contextLimit"),
            json!(200_000),
            "{label}: the declared default, not the damaged override"
        );
        assert_eq!(
            session_plugin_value(&booted, &id, "context-trimmer", "dropDuplicateCalls"),
            json!(false),
            "{label}"
        );

        // And the damaged half is GONE, not carried: the next write re-derives the member from the
        // live store, so a stray key cannot outlive the restart that survived it.
        booted
            .update_session_plugin_settings(
                &id,
                "context-trimmer",
                &patch_map(json!({ "dropDuplicateCalls": true })),
            )
            .expect("{label}: the restored store is writable");
        booted
            .save(&booted.lock())
            .expect("the document is writable");
        assert_eq!(
            persisted_slot(&booted, &id),
            Some(json!({ "context-trimmer": { "dropDuplicateCalls": true } })),
            "{label}: the document keeps only what this boot actually holds"
        );

        drop(booted);
    }
}

#[tokio::test]
async fn a_record_without_the_plugin_slot_boots_empty_and_gains_it_on_the_first_write() {
    let fixture = Fixture::new("plugin-slot-absent");
    let first = fixture.open(Vec::new(), "");
    let id = session(&first, None).await;
    let sibling = session(&first, None).await;

    first
        .update_session_plugin_settings(
            &id,
            "context-compaction",
            &patch_map(json!({ "contextLimit": 4000 })),
        )
        .expect("a conversation may set its own value");
    first
        .update_plugin_settings(
            "context-compaction",
            &patch_map(json!({ "modelLimits": "{\"m\": {\"limit\": 1}}" })),
        )
        .expect("the template has its own override");
    first.save(&first.lock()).expect("the document is writable");
    assert_eq!(
        persisted_slot(&first, &id),
        Some(json!({ "context-compaction": { "contextLimit": 4000 } }))
    );

    drop(first);

    // THE PRE-C3 DOCUMENT: strip the member from both records, which is exactly what a database written
    // by the previous build looks like, and boot. `#[serde(default)]` on the field is what makes this a
    // load rather than a refusal, so an upgrade needs no migration step.
    let editor = fixture.open(Vec::new(), "");
    {
        let mut database = persistence::read_harness_database_file(&fixture.document(&editor))
            .expect("the document reads back");
        for record in &mut database.sessions {
            record.plugin_settings = None;
        }
        persistence::write_harness_database(&fixture.directory, &database, "pre-c3-by-test")
            .expect("the edited document is durable");
    }
    drop(editor);

    let booted = fixture.open(Vec::new(), "");
    assert_eq!(
        booted
            .session_plugin_revision(&id)
            .expect("the session booted"),
        0,
        "a record with no slot is a conversation with no overrides"
    );
    assert_eq!(
        session_plugin_value(&booted, &id, "context-compaction", "contextLimit"),
        json!(200_000),
        "and it does NOT inherit the template's value on upgrade: the seed is a creation-time decision"
    );
    assert_eq!(
        session_plugin_value(&booted, &sibling, "context-compaction", "contextLimit"),
        json!(200_000)
    );
    assert_eq!(
        booted.plugin_values("context-compaction")["modelLimits"],
        json!("{\"m\": {\"limit\": 1}}"),
        "the template's own durable override is untouched by any of this"
    );

    // A save that changes nothing must not stamp an empty slot into an old record: the member stays
    // absent, which is what keeps an upgraded document byte-comparable with the one it came from.
    booted
        .save(&booted.lock())
        .expect("the document is writable");
    assert_eq!(
        persisted_slot(&booted, &id),
        None,
        "no change, so no member"
    );
    assert_eq!(persisted_slot(&booted, &sibling), None);

    // The first write after the upgrade lands the member normally, from the restored store.
    booted
        .update_session_plugin_settings(
            &id,
            "context-compaction",
            &patch_map(json!({ "mode": "exclude" })),
        )
        .expect("an upgraded conversation is writable");
    booted
        .save(&booted.lock())
        .expect("the document is writable");
    assert_eq!(
        persisted_slot(&booted, &id),
        Some(json!({ "context-compaction": { "mode": "exclude" } })),
        "and it carries only the new key, because the old slot really was empty"
    );
    assert_eq!(
        booted.session_plugin_revision(&id).expect("exists"),
        1,
        "the counter starts at 0 on the new boot, as every other restored figure does"
    );
}
