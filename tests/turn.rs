//! Integration tests for the turn engine: the event sequence, the commit boundaries, the
//! retry policy, the rollback rules, and the steering channel.
//!
//! WHAT THIS FILE PINS
//! -------------------
//!   * the exact frame sequence for each shape of turn: a text answer, a tool round, a retry,
//!     an exhausted tool loop, a cancelled run, a steered run, and a refused call;
//!   * the two commit rules (`turn.rs` header): a tool round and a superseded text answer get an
//!     `attempt_commit`, an ordinary answer gets only its `done`;
//!   * that a run which does not succeed hands the store the ORIGINAL transcript, because the
//!     store replaces the conversation with whatever comes back (`turns.rs:1143-1159`);
//!   * the retry arithmetic (`attempt`, `maxAttempts = 11`, `delayMs = 1000 * 2^(attempt-1)`)
//!     and the exhaustion sentence, word for word;
//!   * that the system instruction is projected on every request exactly once and never stored.
//!
//! EVERY ASSERTION IS AGAINST A LOCAL LISTENER plus a scripted tool runtime, so the suite needs
//! no credential and no filesystem. The invocation id a run mints is a uuid, so `masked`
//! replaces it with a constant and every event is then compared WHOLE (`events`), while `raw`
//! keeps the minted identity for the test that pins it.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

use typecrate_harness::contract::{
    HarnessEvent, HarnessMessage, HarnessUsage, ProviderConfig, ToolDecl, ToolDetail, ToolSummary,
    TurnOutcome,
};
use typecrate_harness::ports::{
    ToolExecCtx, ToolExecOutcome, ToolPolicy, ToolRuntime, TurnExecutor, TurnInput,
};
use typecrate_harness::turn::{self, DEFAULT_MAX_ITERATIONS, MAX_RETRIES, PRIMARY_AGENT};

// ---------------------------------------------------------------------------
// The scripted provider
// ---------------------------------------------------------------------------

/// One scripted provider answer.
enum Answer {
    /// A 200 `text/event-stream` body.
    Stream(String),
    /// Any other status, which the transport turns into `HTTP {status}: {body}`.
    Status(u16, String),
}

/// The provider: a queue of answers, and every request body it saw.
struct Provider {
    answers: Mutex<VecDeque<Answer>>,
    seen: Mutex<Vec<Value>>,
    /// `(request number, guidance)`: pushed onto the run's steering channel when that request is
    /// handled, which is how a test puts guidance on the channel WHILE a request is in flight.
    inject: Mutex<Option<(usize, Vec<String>)>>,
    sender: Mutex<Option<UnboundedSender<String>>>,
}

impl Provider {
    /// Pops the next answer. Once the script is down to its last entry that entry REPEATS, so
    /// a test that expects an unbounded loop (the exhaustion case) scripts one answer and gets
    /// it every time.
    fn next(&self) -> (u16, String) {
        let mut queue = self.answers.lock().expect("the answer lock is uncontended");

        match if queue.len() > 1 {
            queue.pop_front().expect("a non-empty queue")
        } else {
            queue
                .front()
                .map(|answer| match answer {
                    Answer::Stream(body) => Answer::Stream(body.clone()),
                    Answer::Status(status, body) => Answer::Status(*status, body.clone()),
                })
                .unwrap_or_else(|| Answer::Status(500, "the script is empty".to_owned()))
        } {
            Answer::Stream(body) => (200, body),
            Answer::Status(status, body) => (status, body),
        }
    }
}

/// Records the request, injects any steering bound to it, and replays the next answer.
async fn handle(State(state): State<Arc<Provider>>, request: axum::Json<Value>) -> Response {
    let request_number = {
        let mut seen = state.seen.lock().expect("the request lock is uncontended");

        seen.push(request.0);
        seen.len()
    };

    // The steer lands mid-request: the run's post-response drain is what picks it up, which is
    // the sequence `harness.ts:6706-6711` describes.
    let bound = state
        .inject
        .lock()
        .expect("the injection lock is uncontended")
        .clone();

    if let Some((index, guidance)) = bound
        && index == request_number
    {
        if let Some(sender) = state
            .sender
            .lock()
            .expect("the sender lock is uncontended")
            .as_ref()
        {
            for text in guidance {
                sender.send(text).expect("an unbounded channel");
            }
        }

        *state
            .inject
            .lock()
            .expect("the injection lock is uncontended") = None;
    }

    let (status, body) = state.next();

    Response::builder()
        .status(StatusCode::from_u16(status).expect("a scripted status is real"))
        .header(
            "content-type",
            if status == 200 {
                "text/event-stream"
            } else {
                "application/json"
            },
        )
        .body(axum::body::Body::from(body))
        .expect("a response made from a script")
}

/// Starts a provider and returns its `baseURL` plus its state.
async fn provider(answers: Vec<Answer>) -> (String, Arc<Provider>) {
    let state = Arc::new(Provider {
        answers: Mutex::new(VecDeque::from(answers)),
        seen: Mutex::new(Vec::new()),
        inject: Mutex::new(None),
        sender: Mutex::new(None),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("an ephemeral port is available");
    let address = listener.local_addr().expect("a bound address");
    let served = Arc::clone(&state);

    tokio::spawn(async move {
        let app = axum::Router::new()
            .route("/chat/completions", axum::routing::post(handle))
            .with_state(served);

        axum::serve(listener, app)
            .await
            .expect("the scripted listener serves");
    });

    (format!("http://{address}"), state)
}

/// One SSE frame.
fn frame(value: Value) -> String {
    format!(
        "data: {}\n\n",
        serde_json::to_string(&value).expect("a frame serializes")
    )
}

/// A whole stream: the frames, then the `[DONE]` sentinel.
fn stream(frames: &[Value]) -> Answer {
    let mut body = frames
        .iter()
        .map(|frame_value| frame(frame_value.clone()))
        .collect::<String>();

    body.push_str("data: [DONE]\n\n");
    Answer::Stream(body)
}

/// A text answer with its usage block, as a provider delivers it.
fn answered(text: &str, usage: Value) -> Answer {
    stream(&[
        json!({"choices": [{"delta": {"content": text}}]}),
        json!({"choices": [{"delta": {}, "finish_reason": "stop"}], "usage": usage}),
    ])
}

/// A tool-requesting answer: one call, `finish_reason: tool_calls`, and its usage block.
fn tool_turn(id: &str, name: &str, arguments: &str, usage: Value) -> Answer {
    stream(&[json!({"choices": [{
        "delta": {"tool_calls": [{
            "index": 0,
            "id": id,
            "type": "function",
            "function": {"name": name, "arguments": arguments},
        }]},
        "finish_reason": "tool_calls",
    }], "usage": usage})])
}

/// The usage block one scripted request reports.
fn usage(input: u64, output: u64) -> Value {
    json!({"prompt_tokens": input, "completion_tokens": output, "total_tokens": input + output})
}

// ---------------------------------------------------------------------------
// The scripted tool runtime
// ---------------------------------------------------------------------------

/// A runtime with one scripted answer per tool name, recording every call it received.
struct ScriptedRuntime {
    /// The answer for a tool with no scripted row.
    default: ToolExecOutcome,
    answers: BTreeMap<String, ToolExecOutcome>,
    calls: Mutex<Vec<(String, Value)>>,
}

impl ScriptedRuntime {
    /// A runtime that answers every call with this successful content.
    fn answering(content: &str) -> Arc<Self> {
        Arc::new(Self {
            default: completed(content, 5),
            answers: BTreeMap::new(),
            calls: Mutex::new(Vec::new()),
        })
    }

    /// A runtime whose `name` answers with `outcome`.
    fn scripted(name: &str, outcome: ToolExecOutcome) -> Arc<Self> {
        Arc::new(Self {
            default: completed("answered", 5),
            answers: BTreeMap::from([(name.to_owned(), outcome)]),
            calls: Mutex::new(Vec::new()),
        })
    }

    /// The calls this runtime executed, in order, with the arguments it was handed.
    fn calls(&self) -> Vec<(String, Value)> {
        self.calls
            .lock()
            .expect("the call lock is uncontended")
            .clone()
    }
}

impl ToolRuntime for ScriptedRuntime {
    fn declarations(&self, _policy: &ToolPolicy) -> Vec<ToolDecl> {
        vec![]
    }

    fn summaries(&self) -> Vec<ToolSummary> {
        vec![]
    }

    /// The scripted runtime has no catalogue rows at all, listed or otherwise.
    fn catalogue_summaries(&self) -> Vec<ToolSummary> {
        vec![]
    }

    fn detail(&self, _name: &str) -> Option<ToolDetail> {
        None
    }

    fn execute(&self, ctx: ToolExecCtx) -> BoxFuture<'static, ToolExecOutcome> {
        self.calls
            .lock()
            .expect("the call lock is uncontended")
            .push((ctx.name.clone(), ctx.arguments.clone()));

        let outcome = self
            .answers
            .get(&ctx.name)
            .cloned()
            .unwrap_or_else(|| self.default.clone());

        Box::pin(async move { outcome })
    }
}

/// A successful tool outcome, the shape a real tool commits.
fn completed(content: &str, duration_ms: u64) -> ToolExecOutcome {
    ToolExecOutcome {
        ok: true,
        content: content.to_owned(),
        panels: None,
        media: None,
        error: None,
        duration_ms,
    }
}

/// A failed tool outcome, in the shape `result::failed_outcome` commits.
fn failed(content: &str) -> ToolExecOutcome {
    ToolExecOutcome {
        ok: false,
        content: content.to_owned(),
        panels: None,
        media: None,
        error: Some(content.to_owned()),
        duration_ms: 7,
    }
}

// ---------------------------------------------------------------------------
// The turn harness
// ---------------------------------------------------------------------------

/// What a turn test varies; everything else is the store's boilerplate.
struct Case {
    prompt: String,
    system: String,
    transcript: Vec<HarnessMessage>,
    tools: Vec<ToolDecl>,
    /// Guidance already on the channel when the run starts.
    steering: Vec<String>,
    /// Request number (1-based) that injects `steering` while it is being served. `None` keeps
    /// the guidance queued from before the run.
    steer_on_request: Option<usize>,
    max_iterations: u64,
}

impl Default for Case {
    fn default() -> Self {
        Self {
            prompt: "What is 2 + 2?".to_owned(),
            system: "Answer briefly.".to_owned(),
            transcript: vec![],
            tools: vec![],
            steering: vec![],
            steer_on_request: None,
            max_iterations: 4,
        }
    }
}

/// Assembles one `TurnInput` from a case, a live provider, and the test's own signal.
fn input(
    case: &Case,
    base: &str,
    runtime: Arc<ScriptedRuntime>,
    signal: CancellationToken,
    // The sink is taken by VALUE because `TurnInput::emit` must be `'static`: a borrowed
    // `Arc` here would be a closure that outlives the call that made it.
    sink: Arc<Mutex<Vec<HarnessEvent>>>,
    steering: Option<UnboundedReceiver<String>>,
) -> TurnInput {
    TurnInput {
        session_id: "session-1".to_owned(),
        turn_id: "turn-1".to_owned(),
        index: 0,
        prompt: case.prompt.clone(),
        thread_id: "thread-1".to_owned(),
        system_instruction: case.system.clone(),
        transcript: case.transcript.clone(),
        tools: case.tools.clone(),
        provider: ProviderConfig {
            base_url: base.to_owned(),
            api_key: Some("secret-key".to_owned()),
            model: "model-of-the-hour".to_owned(),
            headers: BTreeMap::new(),
            // The scripted provider states no sampling, so the request body is the plain
            // `{model, stream, messages, tools?}` document.
            sampling: None,
        },
        attachments: vec![],
        signal,
        steering,
        cwd: std::env::current_dir().unwrap_or_default(),
        emit: Arc::new(move |event: HarnessEvent| {
            sink.lock()
                .expect("the event lock is uncontended")
                .push(event);
        }),
        tool_runtime: runtime,
        action_host: None,
        task_host: None,
        max_iterations: case.max_iterations,
    }
}

/// The recorded events as JSON, with the minted invocation id replaced by a constant so every
/// event can be compared whole.
fn masked(events: &[Value]) -> Vec<Value> {
    events
        .iter()
        .map(|event| {
            let mut value = event.clone();

            if let Some(thread) = value.get_mut("thread").and_then(Value::as_object_mut) {
                thread.insert("id".to_owned(), json!("<invocation>"));
            }

            value
        })
        .collect()
}

/// One finished run: the outcome, the masked event sequence, the raw event JSON (with the
/// minted identity), the request bodies the provider saw, and the calls the runtime executed.
struct Finished {
    outcome: TurnOutcome,
    events: Vec<Value>,
    raw: Vec<Value>,
    requests: Vec<Value>,
    calls: Vec<(String, Value)>,
}

/// Runs one turn end to end against a scripted provider and runtime.
async fn run(case: Case, answers: Vec<Answer>, runtime: Arc<ScriptedRuntime>) -> Finished {
    let (base, provider_state) = provider(answers).await;
    let signal = CancellationToken::new();
    let (sender, receiver): (_, UnboundedReceiver<String>) = tokio::sync::mpsc::unbounded_channel();

    if let Some(request_number) = case.steer_on_request {
        // The provider owns the sender, so the guidance appears DURING that request.
        *provider_state
            .sender
            .lock()
            .expect("the sender lock is uncontended") = Some(sender);
        *provider_state
            .inject
            .lock()
            .expect("the injection lock is uncontended") =
            Some((request_number, case.steering.clone()));
    } else {
        for guidance in &case.steering {
            sender.send(guidance.clone()).expect("an unbounded channel");
        }

        // The sender goes away, so a drain that finds nothing gets "empty" rather than waiting.
        drop(sender);
    }

    let events: Arc<Mutex<Vec<HarnessEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let outcome = turn::AgentExecutor::new()
        .run(input(
            &case,
            &base,
            runtime.clone(),
            signal,
            Arc::clone(&events),
            Some(receiver),
        ))
        .await;

    let collected: Vec<Value> = events
        .lock()
        .expect("the event lock is uncontended")
        .iter()
        .map(|event| serde_json::to_value(event).expect("an event serializes"))
        .collect();

    Finished {
        raw: collected.clone(),
        events: masked(&collected),
        outcome,
        requests: provider_state
            .seen
            .lock()
            .expect("the request lock is uncontended")
            .clone(),
        calls: runtime.calls(),
    }
}

/// The event types of one run, in order.
fn types(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|event| event["type"].as_str())
        .collect()
}

// ---------------------------------------------------------------------------
// The shapes of turn
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_text_answer_streams_then_done_with_no_commit_boundary() {
    let finished = run(
        Case::default(),
        vec![answered("Four.", usage(10, 2))],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    assert_eq!(
        finished.events,
        vec![
            json!({
                "type": "start",
                "agent": "primary",
                "model": "model-of-the-hour",
                "thread": {"id": "<invocation>", "input": "What is 2 + 2?"},
            }),
            json!({
                "type": "text",
                "agent": "primary",
                "delta": "Four.",
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "usage",
                "agent": "primary",
                "usage": {"input": 10, "output": 2, "total": 12},
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "done",
                "agent": "primary",
                "message": {
                    "role": "assistant",
                    "content": "Four.",
                    "reasoning": "",
                    "toolCalls": [],
                    "model": "model-of-the-hour",
                    "stopReason": "stop",
                    "usage": {"input": 10, "output": 2, "total": 12},
                },
                "thread": {"id": "<invocation>"},
            }),
        ],
        "an ordinary answer needs NO `attempt_commit`: its `done` closes the window"
    );

    assert_eq!(
        finished.outcome,
        TurnOutcome {
            usage: Some(HarnessUsage {
                input: 10,
                output: 2,
                total: 12,
                cached: None,
            }),
            error: None,
            cancelled: false,
            messages: vec![
                HarnessMessage::User {
                    content: "What is 2 + 2?".to_owned(),
                    attachments: vec![],

                    included: None,
                    message_id: None,
                },
                HarnessMessage::Assistant {
                    content: "Four.".to_owned(),
                    tool_calls: vec![],
                    reasoning: None,

                    included: None,
                    reasoning_included: None,
                    message_id: None,
                },
            ],
            stop_reason: Some("stop".to_owned()),
            unused_steering: Vec::new(),
        },
        "the transcript the store keeps is the conversation, never the instruction"
    );

    // The instruction rides the REQUEST, exactly once.
    assert_eq!(
        finished.requests[0]["messages"],
        json!([
            {"role": "system", "content": "Answer briefly."},
            {"role": "user", "content": "What is 2 + 2?"},
        ])
    );
    assert_eq!(finished.requests.len(), 1, "one request, one answer");
}

#[tokio::test]
async fn a_tool_round_anchors_its_commit_and_the_next_request_carries_the_answer() {
    let finished = run(
        Case {
            tools: vec![ToolDecl {
                name: "timeNow".to_owned(),
                description: "Read the clock.".to_owned(),
                parameters: Some(json!({"type": "object", "properties": {}})),
            }],
            ..Case::default()
        },
        vec![
            tool_turn("call-1", "timeNow", "{\"timezone\":\"UTC\"}", usage(8, 1)),
            answered("One o'clock.", usage(9, 3)),
        ],
        ScriptedRuntime::scripted("timeNow", completed("13:00", 4)),
    )
    .await;

    assert_eq!(
        finished.events,
        vec![
            json!({
                "type": "start",
                "agent": "primary",
                "model": "model-of-the-hour",
                "thread": {"id": "<invocation>", "input": "What is 2 + 2?"},
            }),
            json!({
                "type": "usage",
                "agent": "primary",
                "usage": {"input": 8, "output": 1, "total": 9},
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "tool_call",
                "agent": "primary",
                "call": {"id": "call-1", "name": "timeNow", "arguments": "{\"timezone\":\"UTC\"}"},
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "tool_result",
                "agent": "primary",
                "name": "timeNow",
                "content": "13:00",
                "isError": false,
                "callId": "call-1",
                "outcome": "completed",
                "durationMs": 4,
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "attempt_commit",
                "agent": "primary",
                "attempt": 1,
                "maxAttempts": MAX_RETRIES + 1,
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "text",
                "agent": "primary",
                "delta": "One o'clock.",
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "usage",
                "agent": "primary",
                "usage": {"input": 9, "output": 3, "total": 12},
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "done",
                "agent": "primary",
                "message": {
                    "role": "assistant",
                    "content": "One o'clock.",
                    "reasoning": "",
                    "toolCalls": [],
                    "model": "model-of-the-hour",
                    "stopReason": "stop",
                    "usage": {"input": 9, "output": 3, "total": 12},
                },
                "thread": {"id": "<invocation>"},
            }),
        ],
        "the tool pair streams in request order and ONE boundary closes the whole round"
    );

    // Usage is folded across requests: the outcome reports the RUN's total.
    assert_eq!(
        finished.outcome.usage,
        Some(HarnessUsage {
            input: 17,
            output: 4,
            total: 21,
            cached: None,
        })
    );

    // The second request answers the call: assistant tool_calls, then the tool message.
    assert_eq!(
        finished.requests[1]["messages"],
        json!([
            {"role": "system", "content": "Answer briefly."},
            {"role": "user", "content": "What is 2 + 2?"},
            {"role": "assistant", "content": "", "tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {"name": "timeNow", "arguments": "{\"timezone\":\"UTC\"}"},
            }]},
            {"role": "tool", "tool_call_id": "call-1", "content": "13:00"},
        ]),
        "the strict pairing a provider requires"
    );
    assert_eq!(
        finished.requests[1]["tools"]
            .as_array()
            .expect("the tool set rides every request")
            .len(),
        1,
        "the declared set is re-sent, never dropped after a round"
    );
    assert_eq!(
        finished.calls,
        vec![("timeNow".to_owned(), json!({"timezone": "UTC"}))]
    );

    // The committed transcript: prompt, the call, the answer, the reply.
    assert_eq!(
        finished
            .outcome
            .messages
            .iter()
            .map(
                |message| serde_json::to_value(message).expect("a message serializes")["role"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            )
            .collect::<Vec<_>>(),
        vec!["user", "assistant", "tool", "assistant"]
    );
}

#[tokio::test]
async fn a_failed_attempt_reports_a_retry_with_the_documented_backoff() {
    let finished = run(
        Case::default(),
        vec![
            Answer::Status(500, "upstream exploded".to_owned()),
            answered("Four.", usage(10, 2)),
        ],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    assert_eq!(
        types(&finished.events),
        vec!["start", "retry", "text", "usage", "done"],
        "the discarded attempt streamed NOTHING that survived: no text, no usage before the retry"
    );
    assert_eq!(
        finished.events[1],
        json!({
            "type": "retry",
            "agent": "primary",
            "attempt": 1,
            "maxAttempts": 11,
            "delayMs": 1_000,
            "error": {"message": "HTTP 500: upstream exploded"},
            "thread": {"id": "<invocation>"},
        }),
        "the retry frame carries the provider's own sentence and the first backoff"
    );
    assert_eq!(finished.outcome.error, None);
    assert_eq!(
        finished
            .events
            .last()
            .expect("a finished run ends with a frame")["type"],
        json!("done")
    );
    assert_eq!(
        finished.requests.len(),
        2,
        "one failed attempt, one successful one"
    );

    // The retry re-sends the SAME request bytes, so the discarded attempt left nothing behind.
    assert_eq!(finished.requests[0], finished.requests[1]);
}

#[tokio::test]
async fn an_exhausted_tool_loop_answers_with_the_documented_sentence() {
    let finished = run(
        Case {
            max_iterations: 2,
            ..Case::default()
        },
        vec![tool_turn("call-1", "timeNow", "{}", usage(1, 1))],
        ScriptedRuntime::answering("13:00"),
    )
    .await;

    assert_eq!(
        types(&finished.events),
        vec![
            "start",
            "usage",
            "tool_call",
            "tool_result",
            "attempt_commit",
            "usage",
            "tool_call",
            "tool_result",
            "attempt_commit",
            "error",
        ],
        "two rounds, then the loop gives up. `usage` rides the request, so it always precedes the \
         calls that request asked for (`harness.ts:6656` precedes `:6739`)"
    );
    assert_eq!(
        finished.events.last(),
        Some(&json!({
            "type": "error",
            "agent": "primary",
            "error": {
                "message": "Agent \"primary\" requested tools more than 2 times without answering."
            },
            "thread": {"id": "<invocation>"},
        })),
        "the sentence names the bound the run hit"
    );
    assert_eq!(
        finished.outcome.error.as_deref(),
        Some("Agent \"primary\" requested tools more than 2 times without answering.")
    );
    assert!(!finished.outcome.cancelled);
    assert_eq!(finished.outcome.stop_reason, None);

    // ROLLBACK: the run consumed two rounds of transcript and hands back NONE of it, because
    // the store REPLACES the conversation with whatever comes back.
    assert_eq!(finished.outcome.messages, vec![]);
    assert_eq!(
        finished.calls.len(),
        2,
        "both rounds did real work before the loop gave up"
    );
}

#[tokio::test]
async fn a_cancelled_run_reports_no_error_and_keeps_the_original_transcript() {
    let case = Case {
        transcript: vec![HarnessMessage::User {
            content: "an earlier question".to_owned(),
            attachments: vec![],

            included: None,
            message_id: None,
        }],
        ..Case::default()
    };
    let (base, _provider_state) = provider(vec![answered("Four.", usage(10, 2))]).await;
    let runtime = ScriptedRuntime::answering("unused");
    let signal = CancellationToken::new();
    let events: Arc<Mutex<Vec<HarnessEvent>>> = Arc::new(Mutex::new(Vec::new()));

    // Cancelled BEFORE the first boundary: the abort check at the top of the loop stops it.
    signal.cancel();

    let outcome = turn::AgentExecutor::new()
        .run(input(
            &case,
            &base,
            runtime.clone(),
            signal,
            Arc::clone(&events),
            None,
        ))
        .await;

    let collected: Vec<Value> = events
        .lock()
        .expect("the event lock is uncontended")
        .iter()
        .map(|event| serde_json::to_value(event).expect("an event serializes"))
        .collect();

    assert_eq!(
        masked(&collected),
        vec![json!({
            "type": "start",
            "agent": "primary",
            "model": "model-of-the-hour",
            "thread": {"id": "<invocation>", "input": "What is 2 + 2?"},
        })],
        "an aborted run opens, stops at the first boundary, and never raises an error frame"
    );
    assert_eq!(
        outcome,
        TurnOutcome {
            usage: None,
            error: None,
            cancelled: true,
            messages: case.transcript.clone(),
            stop_reason: None,
            // Nothing was drained, so nothing is owed back; a run that DID drain steering before an
            // abort reports it here and the store discards it (a stop drops the guidance).
            unused_steering: Vec::new(),
        },
        "cancellation is a control outcome: the conversation stays exactly as it was"
    );
    assert!(runtime.calls().is_empty(), "no tool ran for an aborted run");
}

#[tokio::test]
async fn guidance_queued_during_a_run_buys_exactly_one_more_turn() {
    let finished = run(
        Case {
            // Delivered while request 1 is in flight; the blank steer is dropped by the drain,
            // so only the real one reaches the model.
            steer_on_request: Some(1),
            steering: vec!["Answer in French.".to_owned(), "   ".to_owned()],
            ..Case::default()
        },
        vec![
            answered("Four.", usage(4, 1)),
            answered("Quatre.", usage(5, 2)),
        ],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    assert_eq!(
        finished.events,
        vec![
            json!({
                "type": "start",
                "agent": "primary",
                "model": "model-of-the-hour",
                "thread": {"id": "<invocation>", "input": "What is 2 + 2?"},
            }),
            json!({
                "type": "text",
                "agent": "primary",
                "delta": "Four.",
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "usage",
                "agent": "primary",
                "usage": {"input": 4, "output": 1, "total": 5},
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "attempt_commit",
                "agent": "primary",
                "attempt": 1,
                "maxAttempts": 11,
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "text",
                "agent": "primary",
                "delta": "Quatre.",
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "usage",
                "agent": "primary",
                "usage": {"input": 5, "output": 2, "total": 7},
                "thread": {"id": "<invocation>"},
            }),
            json!({
                "type": "done",
                "agent": "primary",
                "message": {
                    "role": "assistant",
                    "content": "Quatre.",
                    "reasoning": "",
                    "toolCalls": [],
                    "model": "model-of-the-hour",
                    "stopReason": "stop",
                    "usage": {"input": 5, "output": 2, "total": 7},
                },
                "thread": {"id": "<invocation>"},
            }),
        ],
        "the superseded answer is committed, then the guidance is asked about once"
    );

    // The guidance is committed as its own user turn before the next request.
    assert_eq!(
        finished.requests[1]["messages"],
        json!([
            {"role": "system", "content": "Answer briefly."},
            {"role": "user", "content": "What is 2 + 2?"},
            {"role": "assistant", "content": "Four."},
            {"role": "user", "content": "Answer in French."},
        ])
    );
    assert_eq!(
        finished.requests.len(),
        2,
        "one extra turn, not a steering loop"
    );
}

#[tokio::test]
async fn a_steer_already_queued_when_the_run_starts_is_committed_first() {
    let finished = run(
        Case {
            steering: vec!["Answer in French.".to_owned()],
            ..Case::default()
        },
        vec![answered("Quatre.", usage(4, 1))],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    assert_eq!(
        types(&finished.events),
        vec!["start", "attempt_commit", "text", "usage", "done"],
        "guidance that predates the first request is applied BEFORE it, and committed on arrival \
         because it is recorded history rather than the partial output of a retryable attempt"
    );
    assert_eq!(
        finished.requests[0]["messages"],
        json!([
            {"role": "system", "content": "Answer briefly."},
            {"role": "user", "content": "What is 2 + 2?"},
            {"role": "user", "content": "Answer in French."},
        ]),
        "the steer is part of the conversation the model is shown, once"
    );
    assert_eq!(
        finished.requests.len(),
        1,
        "a steer applied before a request costs no extra turn"
    );
}

#[tokio::test]
async fn a_malformed_argument_string_is_answered_without_reaching_the_tool() {
    let runtime = ScriptedRuntime::answering("13:00");
    let finished = run(
        Case::default(),
        vec![
            tool_turn("call-1", "timeNow", "{\"broken", usage(2, 1)),
            answered("Done.", usage(3, 1)),
        ],
        runtime.clone(),
    )
    .await;

    let results: Vec<&Value> = finished
        .events
        .iter()
        .filter(|event| event["type"] == json!("tool_result"))
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["isError"], json!(true));
    assert_eq!(results[0]["outcome"], json!("failed"));
    assert_eq!(results[0]["callId"], json!("call-1"));
    assert_eq!(
        results[0].get("durationMs"),
        None,
        "a call that never executed reports no span"
    );
    assert!(
        results[0]["content"]
            .as_str()
            .unwrap_or_default()
            .starts_with("Tool \"timeNow\" received malformed JSON arguments: "),
        "the parse error is the answer the model reads: {}",
        results[0]["content"]
    );
    assert_eq!(runtime.calls(), vec![], "the tool body never ran");

    let content = results[0]["content"].clone();

    assert_eq!(
        finished.requests[1]["messages"][3],
        json!({"role": "tool", "tool_call_id": "call-1", "content": content}),
        "the transcript still answers the call, so the next request is valid"
    );
}

#[tokio::test]
async fn a_failing_tool_is_a_completed_round_not_a_failed_run() {
    let finished = run(
        Case::default(),
        vec![
            tool_turn("call-1", "timeNow", "{}", usage(2, 1)),
            answered("Never mind.", usage(3, 1)),
        ],
        ScriptedRuntime::scripted("timeNow", failed("Tool \"timeNow\" failed: no clock.")),
    )
    .await;

    let result = finished
        .events
        .iter()
        .find(|event| event["type"] == json!("tool_result"))
        .expect("a tool round answers");

    assert_eq!(result["isError"], json!(true));
    assert_eq!(result["outcome"], json!("failed"));
    assert_eq!(
        result["durationMs"],
        json!(7),
        "the call DID execute and reports its span"
    );
    assert_eq!(
        finished.outcome.error, None,
        "a failed tool is not a failed turn"
    );
    assert_eq!(
        finished
            .events
            .last()
            .expect("a finished run ends with a frame")["type"],
        json!("done")
    );
}

#[tokio::test]
async fn an_existing_system_entry_is_never_duplicated() {
    let finished = run(
        Case {
            transcript: vec![HarnessMessage::System {
                content: "The store's own ladder.".to_owned(),

                included: None,
                message_id: None,
            }],
            ..Case::default()
        },
        vec![answered("Four.", usage(1, 1))],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    assert_eq!(
        finished.requests[0]["messages"],
        json!([
            {"role": "system", "content": "The store's own ladder."},
            {"role": "user", "content": "What is 2 + 2?"},
        ]),
        "the store owns the instruction, so a transcript that already leads with one is used as is"
    );
    assert_eq!(finished.outcome.messages.len(), 3);
}

#[tokio::test]
async fn a_recorded_transcript_continues_the_conversation() {
    let history = vec![
        HarnessMessage::User {
            content: "Earlier question".to_owned(),
            attachments: vec![],

            included: None,
            message_id: None,
        },
        HarnessMessage::Assistant {
            content: "Earlier answer".to_owned(),
            tool_calls: vec![],
            reasoning: None,

            included: None,
            reasoning_included: None,
            message_id: None,
        },
    ];
    let case = Case {
        transcript: history.clone(),
        ..Case::default()
    };
    let finished = run(
        case,
        vec![answered("Four.", usage(1, 1))],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    assert_eq!(
        finished.requests[0]["messages"],
        json!([
            {"role": "system", "content": "Answer briefly."},
            {"role": "user", "content": "Earlier question"},
            {"role": "assistant", "content": "Earlier answer"},
            {"role": "user", "content": "What is 2 + 2?"},
        ]),
        "the run continues the conversation the store recorded instead of starting a new one"
    );

    // The returned transcript REPLACES the store's conversation, so it must still carry the
    // history it was handed.
    let returned: Vec<&HarnessMessage> = finished.outcome.messages.iter().collect();

    // Two recorded entries, the new prompt, and the answer.
    assert_eq!(returned.len(), 4);
    assert_eq!(&finished.outcome.messages[..2], &history[..]);
    assert_eq!(
        finished.outcome.messages[2],
        HarnessMessage::User {
            content: "What is 2 + 2?".to_owned(),
            attachments: vec![],

            included: None,
            message_id: None,
        }
    );
}

#[tokio::test]
async fn a_zero_iteration_budget_falls_back_to_the_library_default() {
    // The library default is NO LIMIT: the optional safeguard is off unless a host names
    // a finite count.
    assert_eq!(DEFAULT_MAX_ITERATIONS, u64::MAX);

    let finished = run(
        Case {
            max_iterations: 0,
            ..Case::default()
        },
        vec![answered("Four.", usage(1, 1))],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    assert_eq!(
        finished
            .events
            .last()
            .expect("a finished run ends with a frame")["type"],
        json!("done")
    );
    assert_eq!(finished.outcome.stop_reason.as_deref(), Some("stop"));
}

#[tokio::test]
async fn every_frame_of_a_run_carries_one_invocation_identity() {
    let finished = run(
        Case::default(),
        vec![answered("Four.", usage(1, 1))],
        ScriptedRuntime::answering("unused"),
    )
    .await;

    // The masked copy replaced the id, so the minted value comes from the raw frames.
    let minted = finished.raw[0]["thread"]["id"]
        .as_str()
        .expect("the raw start frame keeps the minted id")
        .to_owned();

    assert_eq!(finished.events[0]["thread"]["id"], json!("<invocation>"));
    assert_eq!(minted.len(), 36, "a uuid v4 string");

    let parsed = uuid::Uuid::parse_str(&minted).expect("the invocation id is a uuid");

    assert_eq!(parsed.get_version_num(), 4);

    for frame in &finished.raw {
        assert_eq!(
            frame["thread"]["id"],
            json!(minted),
            "one run, one identity, on every frame"
        );
        assert_eq!(frame["agent"], json!(PRIMARY_AGENT));
    }

    assert_eq!(PRIMARY_AGENT, "primary");
}
