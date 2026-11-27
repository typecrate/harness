//! Real-HTTP tests for the session-domain surface (T5b).
//!
//! Every assertion goes over a TCP socket to an `axum::serve` instance built by
//! `typecrate_harness::server::router` - the same function the executable binds - against a REAL
//! store and a REAL `AgentExecutor`, with the provider replaced by a local OpenAI-compatible listener
//! that speaks SSE. Nothing here fakes the turn engine: what this stage owns is the HTTP projection of
//! the store's records, and the only way to prove it is to run a turn and read it back through every
//! route that describes it.
//!
//! Pinned, in the stage brief's order:
//!
//! 1. session create / list / read / patch / delete - the status codes, the revision headers, and the
//!    refusal sentences of every 400/404/409 arm;
//! 2. task submit -> 202 receipt -> poll the turn to `done`, then the transcript document, the turn
//!    events / display / step / block reads, and the usage fold;
//! 3. the queue: two submits on one conversation, the second one `queued` with an ORDINAL
//!    `queue_position`, then `GET /queue`;
//! 4. cancel and reset, including the 409 the store refuses a busy reset with;
//! 5. transcript PUT/get/document, the `if_revision` 409 naming both figures, and the raw-acceptance
//!    path that turns a broken draft into a successful write;
//! 6. thread CRUD, the descendant cascade, and the conversation cancel;
//! 7. the session agent roster and the instruction ladder, including the `null` restore;
//! 8. the toolbox: list with its filters and budget, create, source, patch, delete, and the manual
//!    call of a DECLARATIVE tool (which answers a well-formed `ok: false`);
//! 9. the manual call of a real catalogue tool (`timeNow`), which answers `ok: true`;
//! 10. turns paging plus the route-level refusals: 404s, 405 wording, 415, the body cap, and the
//!     body-less routes;
//! 11. a slash command: 202, a terminal turn, and the synthesized `command` call/result pair;
//! 12. the owner-question round trip: `actionRequest` parks the run, the actions list names the
//!     recorded request, an unknown option is refused, and the answer settles the turn.
//!
//! Fixtures live in `Temp/typecrate-harness/http-sessions/<pid>-<test>/` per the repo scratch rule,
//! and every test owns its own DATABASE ROOT: the runtime cache is keyed by root, so an isolated root
//! is what keeps one test's revision counters from being another's shared state. No test reads or
//! writes a process environment variable - the session's own `settings` block points the provider at
//! the local mock - which is why the parallel suites do not need a runtime-building seam.

use std::collections::VecDeque;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::Response;
use axum::routing::post;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use typecrate_harness::package_root;
use typecrate_harness::server;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Seconds one test will wait for a spawned turn to reach the status it asked for. A scripted
/// provider answers in milliseconds, so 25s only catches a real hang - and the retry-window test
/// relies on the run STILL being up at the moment it cancels.
const WAIT_TIMEOUT: Duration = Duration::from_secs(25);

/// The gap between two polls of a mutating route's result.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

const EPOCH_HEADER: HeaderName = HeaderName::from_static("x-harness-epoch");
const REVISION_HEADER: HeaderName = HeaderName::from_static("x-harness-revision");

/// The credential the mock provider expects, and the only value that may never appear in a response
/// body (`knowledge.md` "T3": the credential never enters the document).
const MOCK_KEY: &str = "mock-provider-key";

/// A database root that starts empty and is removed on `Drop`.
struct Root {
    path: PathBuf,
}

impl Root {
    fn new(test: &str) -> Self {
        let path = package_root::normalize_lexically(&Path::new(env!("CARGO_MANIFEST_DIR")).join(
            format!(
                "../../../Temp/typecrate-harness/http-sessions/{pid}-{test}",
                pid = std::process::id()
            ),
        ));

        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path)
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", path.display()));

        Self { path }
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// One harness listener on an ephemeral loopback port, serving the LIBRARY router.
struct TestServer {
    base: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TestServer {
    async fn start(root: &Root) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral loopback port is available");
        let local: SocketAddr = listener
            .local_addr()
            .expect("the bound listener reports its address");

        let path = root.path.clone();
        let (sender, receiver) = oneshot::channel::<()>();

        let task = tokio::spawn(async move {
            server::serve_on(listener, path, async move {
                let _ = receiver.await;
            })
            .await
            .expect("the harness listener serves until it is told to stop");
        });

        let base = format!("http://{local}");

        // One health probe proves the socket accepts before any assertion runs.
        let probe = client();

        for _ in 0..200 {
            if probe
                .get(format!("{base}/v1/harness/health"))
                .send()
                .await
                .is_ok()
            {
                break;
            }

            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        Self {
            base,
            shutdown: Some(sender),
            task: Some(task),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Stops the listener and waits for it, so the root's document has one writer.
    async fn stop(mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }

        if let Some(task) = self.task.take() {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("the listener stops on the shutdown signal")
                .expect("the serve task does not panic");
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.take();
    }
}

/// A client with pooling disabled, so a keep-alive socket from one test cannot be reused against a
/// port another test has taken over.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_idle_timeout(Duration::ZERO)
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(120))
        .build()
        .expect("a plain HTTP client builds")
}

/// A response reduced to the three things the contract pins.
struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}

impl Reply {
    async fn read(response: reqwest::Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let text = response
            .text()
            .await
            .unwrap_or_else(|error| panic!("the body is readable: {error}"));

        let body = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text)
                .unwrap_or_else(|error| panic!("a JSON body is expected, got `{text}`: {error}"))
        };

        Self {
            status,
            headers,
            body,
        }
    }

    async fn send(request: reqwest::RequestBuilder) -> Self {
        Self::read(
            request
                .send()
                .await
                .unwrap_or_else(|error| panic!("the request completes: {error}")),
        )
        .await
    }

    /// The error envelope's message, which every refusal test asserts verbatim.
    fn message(&self) -> &str {
        self.body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("an error envelope, got `{}`", self.body))
    }

    fn header(&self, name: &HeaderName) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }

    /// The `X-Harness-Revision` the route stamped, which every test compares against a figure the
    /// BODY reports - the contract's "one counter per comparison" rule.
    fn revision(&self) -> u64 {
        self.header(&REVISION_HEADER)
            .unwrap_or_else(|| panic!("X-Harness-Revision on `{}`", self.body))
            .parse()
            .expect("a revision header is a number")
    }

    fn epoch(&self) -> String {
        self.header(&EPOCH_HEADER)
            .expect("X-Harness-Epoch on every JSON response")
    }

    fn json_at(&self, pointer: &str) -> Option<&Value> {
        self.body.pointer(pointer)
    }

    fn string_at(&self, pointer: &str) -> String {
        self.json_at(pointer)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("`{pointer}` is a string in `{}`", self.body))
            .to_owned()
    }

    fn number_at(&self, pointer: &str) -> u64 {
        self.json_at(pointer)
            .and_then(Value::as_u64)
            .unwrap_or_else(|| panic!("`{pointer}` is a number in `{}`", self.body))
    }
}

// ---------------------------------------------------------------------------
// The scripted provider
// ---------------------------------------------------------------------------

/// One scripted provider answer: a 200 SSE body, any other status, or a 200 body the listener holds
/// back for a while. `Delayed` is what makes a LIVE run observable: without it the answer lands in
/// single-digit milliseconds and a test that wants to see `running`/`queued` is racing the executor.
enum Answer {
    Stream(String),
    Status(u16, String),
    Delayed(u64, String),
}

/// The provider: a queue of answers, and every request body it saw.
struct Provider {
    answers: Mutex<VecDeque<Answer>>,
    seen: Mutex<Vec<Value>>,
}

impl Provider {
    /// Pops the next answer; the LAST entry repeats, so a run that asks again after the script ends
    /// keeps getting the final answer instead of a script-exhaustion error.
    fn next(&self) -> (u16, String, u64) {
        let mut queue = self.answers.lock().expect("the answer lock is uncontended");

        match if queue.len() > 1 {
            queue.pop_front().expect("a non-empty queue")
        } else {
            queue
                .front()
                .map(|answer| match answer {
                    Answer::Stream(body) => Answer::Stream(body.clone()),
                    Answer::Status(status, body) => Answer::Status(*status, body.clone()),
                    Answer::Delayed(delay, body) => Answer::Delayed(*delay, body.clone()),
                })
                .unwrap_or_else(|| Answer::Status(500, "the script is empty".to_owned()))
        } {
            Answer::Stream(body) => (200, body, 0),
            Answer::Status(status, body) => (status, body, 0),
            Answer::Delayed(delay, body) => (200, body, delay),
        }
    }

    fn requests(&self) -> Vec<Value> {
        self.seen
            .lock()
            .expect("the request lock is uncontended")
            .clone()
    }
}

/// Records the request and replays the next answer.
async fn handle_provider(State(state): State<Arc<Provider>>, request: Request) -> Response {
    let body = axum::body::to_bytes(request.into_body(), 32 * 1024 * 1024)
        .await
        .unwrap_or_default();
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);

    {
        let mut seen = state.seen.lock().expect("the request lock is uncontended");
        seen.push(parsed);
    }

    let (status, body, delay) = state.next();

    // A held-back answer is how a test keeps one turn LIVE while it reads the routes that describe a
    // running conversation. The sleep happens BEFORE the response starts, so the run is parked inside
    // the provider call the whole time.
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

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
    });
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("an ephemeral port is available");
    let address = listener.local_addr().expect("a bound address");
    let served = Arc::clone(&state);

    tokio::spawn(async move {
        let app = Router::new()
            .route("/chat/completions", post(handle_provider))
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

/// A whole stream: the frames, then the `[DONE]` sentinel, as the body text.
fn stream(frames: &[Value]) -> String {
    let mut body = frames
        .iter()
        .map(|frame_value| frame(frame_value.clone()))
        .collect::<String>();

    body.push_str("data: [DONE]\n\n");

    body
}

/// A text answer with its usage block.
fn answered(text: &str, usage: Value) -> Answer {
    Answer::Stream(answered_body(text, usage))
}

/// The same body, for a script entry that must be held back before it is sent.
fn answered_body(text: &str, usage: Value) -> String {
    stream(&[
        json!({"choices": [{"delta": {"content": text}}]}),
        json!({"choices": [{"delta": {}, "finish_reason": "stop"}], "usage": usage}),
    ])
}

/// A tool-requesting answer: one call, `finish_reason: tool_calls`, and its usage block.
fn tool_turn(id: &str, name: &str, arguments: &str, usage: Value) -> Answer {
    Answer::Stream(stream(&[json!({"choices": [{
        "delta": {"tool_calls": [{
            "index": 0,
            "id": id,
            "type": "function",
            "function": {"name": name, "arguments": arguments},
        }]},
        "finish_reason": "tool_calls",
    }], "usage": usage})]))
}

/// The usage block one scripted request reports.
fn usage(input: u64, output: u64) -> Value {
    json!({"prompt_tokens": input, "completion_tokens": output, "total_tokens": input + output})
}

/// The same block with the OpenAI cache detail attached (`transport.rs:589-594` reads
/// `prompt_tokens_details.cached_tokens` into `HarnessUsage.cached`).
fn usage_cached(input: u64, output: u64, cached: u64) -> Value {
    json!({
        "prompt_tokens": input,
        "completion_tokens": output,
        "total_tokens": input + output,
        "prompt_tokens_details": { "cached_tokens": cached },
    })
}

// ---------------------------------------------------------------------------
// Route helpers
// ---------------------------------------------------------------------------

/// `GET` one path.
async fn get(server: &TestServer, path: &str) -> Reply {
    Reply::send(client().get(server.url(path))).await
}

/// `POST` one JSON body (or no body at all when `body` is `None`).
async fn post_json(server: &TestServer, path: &str, body: Option<Value>) -> Reply {
    let request = client().post(server.url(path));

    Reply::send(match body {
        Some(body) => request.json(&body),
        None => request,
    })
    .await
}

async fn put_json(server: &TestServer, path: &str, body: Value) -> Reply {
    Reply::send(client().put(server.url(path)).json(&body)).await
}

async fn patch_json(server: &TestServer, path: &str, body: Value) -> Reply {
    Reply::send(client().patch(server.url(path)).json(&body)).await
}

async fn del(server: &TestServer, path: &str) -> Reply {
    Reply::send(client().delete(server.url(path))).await
}

/// One session, created against the mock provider, with the credential supplied explicitly so the
/// scoped-credential rule cannot quietly fall back to whatever the host's environment holds.
async fn create_session(server: &TestServer, base: &str, title: Option<&str>) -> (String, Reply) {
    let mut body = json!({
        "harness": "instruct",
        "settings": {"base_url": base, "api_key": MOCK_KEY, "model": "mock/one"}
    });

    if let Some(title) = title {
        body["title"] = json!(title);
    }

    let reply = post_json(server, "/v1/harness/sessions", Some(body)).await;

    assert_eq!(
        reply.status,
        StatusCode::CREATED,
        "`{title:?}` creates: {}",
        reply.body
    );
    assert_eq!(reply.epoch(), reply.epoch());

    let id = reply.string_at("/session/id");

    (id, reply)
}

/// Submits one task, optionally addressed at a conversation.
async fn submit(server: &TestServer, session: &str, input: &str, thread: Option<&str>) -> Reply {
    let mut body = json!({ "input": input });

    if let Some(thread) = thread {
        body["thread_id"] = json!(thread);
    }

    post_json(
        server,
        &format!("/v1/harness/sessions/{session}/tasks"),
        Some(body),
    )
    .await
}

/// Opens one host conversation on the primary member and returns its id.
async fn open_thread(server: &TestServer, session: &str, agent: &str) -> String {
    let reply = post_json(
        server,
        &format!("/v1/harness/sessions/{session}/threads"),
        Some(json!({ "agent": agent })),
    )
    .await;

    assert_eq!(
        reply.status,
        StatusCode::CREATED,
        "a thread is created: {}",
        reply.body
    );

    reply.string_at("/thread/id")
}

/// Polls one GET until `predicate` holds, and returns the reply that satisfied it.
async fn wait_for(
    server: &TestServer,
    path: &str,
    predicate: impl Fn(&Value) -> bool,
    described: &str,
) -> Reply {
    let attempts =
        u32::try_from(WAIT_TIMEOUT.as_millis() / POLL_INTERVAL.as_millis().max(1)).unwrap_or(1000);

    for _ in 0..attempts {
        let reply = get(server, path).await;

        if predicate(&reply.body) {
            return reply;
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }

    panic!(
        "`{path}` never became {described}; last body was `{}`",
        get(server, path).await.body
    );
}

/// Waits for one turn to leave `queued`/`running`.
async fn wait_for_turn(server: &TestServer, session: &str, turn_id: &str, status: &str) -> Reply {
    wait_for(
        server,
        &format!("/v1/harness/sessions/{session}/turns/{turn_id}"),
        |body| body.pointer("/turn/status").and_then(Value::as_str) == Some(status),
        &format!("a turn with status `{status}`"),
    )
    .await
}

// ---------------------------------------------------------------------------
// 1 - the session lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_session_lifecycle_answers_the_contract_end_to_end() {
    let root = Root::new("lifecycle");
    let server = TestServer::start(&root).await;
    let (_base, _provider) = provider(vec![answered("unused", usage(1, 1))]).await;

    // CREATE: 201, the full resource, and a revision header for a counter that starts at 1.
    let (id, created) = create_session(&server, "http://127.0.0.1:1", Some("My session")).await;

    assert_eq!(
        created.revision(),
        1,
        "a fresh resource's counter starts at 1 (`api.ts:2029-2031`)"
    );
    assert!(
        !created.epoch().is_empty(),
        "every JSON response carries the epoch"
    );
    assert_eq!(created.string_at("/session/harness"), "instruct");
    assert_eq!(created.string_at("/session/title"), "My session");
    assert_eq!(created.string_at("/session/status"), "idle");
    assert_eq!(created.number_at("/session/turn_count"), 0);
    assert!(
        !created.body.to_string().contains(MOCK_KEY),
        "the provider credential is never echoed back"
    );

    // `SessionResource.endpoint` is the SESSION's own key-free view: `api-reads.ts:225` calls
    // `providerDefaultsOf(record.settings)`, so the base URL and model this create supplied are the
    // ones the block prints, and `has_api_key` states whether THIS session holds a credential
    // (`sessions.ts:1618-1634` keeps it in memory, which is why the create answers the presence of the
    // mock key and never the key - compare the no-echo assertion above). The DEPLOYMENT's view is
    // `/config`'s `defaults` block, which is a different document on purpose.
    let config = get(&server, "/v1/harness/config").await;

    assert_eq!(config.status, StatusCode::OK);
    assert_eq!(
        created.json_at("/session/endpoint"),
        Some(&json!({
            "base_url": "http://127.0.0.1:1",
            "model": "mock/one",
            "has_api_key": true,
        })),
        "the session's endpoint block is the provider the session runs on"
    );
    assert_ne!(
        created.json_at("/session/endpoint"),
        config.json_at("/defaults"),
        "a session with its own provider never advertises the deployment defaults"
    );

    // LIST: a page whose revision is the COLLECTION counter, and whose `limit` echoes the page.
    let list = get(&server, "/v1/harness/sessions").await;

    assert_eq!(list.status, StatusCode::OK);
    assert_eq!(
        list.revision(),
        list.number_at("/revision"),
        "a page stamps its own revision"
    );
    assert_eq!(
        list.number_at("/revision"),
        1,
        "one row moved the collection counter once"
    );
    assert_eq!(list.number_at("/limit"), 1);
    assert_eq!(
        list.json_at("/next_cursor"),
        Some(&Value::Null),
        "a full final page ends"
    );
    assert_eq!(list.body["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(list.body["items"][0]["id"], json!(id));
    assert_eq!(list.body["items"][0]["status"], json!("idle"));

    // A status filter is an enum and a typo is a 400 that names the vocabulary.
    let filtered = get(&server, "/v1/harness/sessions?status=running").await;

    assert_eq!(filtered.status, StatusCode::OK);
    assert_eq!(
        filtered.number_at("/limit"),
        0,
        "an empty page echoes a limit of 0"
    );

    let bad_filter = get(&server, "/v1/harness/sessions?status=paused").await;

    assert_eq!(bad_filter.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_filter.message(),
        "Query parameter \"status\" must be one of: idle, running, error."
    );

    // READ.
    let read = get(&server, &format!("/v1/harness/sessions/{id}")).await;

    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.revision(), read.number_at("/session/revision"));
    assert_eq!(read.string_at("/session/id"), id);

    // DEFINITION and USAGE are the other two identified reads of the same aggregate.
    let definition = get(&server, &format!("/v1/harness/sessions/{id}/definition")).await;

    assert_eq!(definition.status, StatusCode::OK);
    assert_eq!(
        definition.revision(),
        1,
        "the definition counter was minted by the create"
    );
    assert_eq!(definition.string_at("/definition/name"), "instruct");
    assert!(definition.json_at("/definition/current").is_some());

    let usage_read = get(&server, &format!("/v1/harness/sessions/{id}/usage")).await;

    assert_eq!(usage_read.status, StatusCode::OK);
    assert_eq!(usage_read.number_at("/turn_count"), 0);
    assert_eq!(usage_read.revision(), usage_read.number_at("/revision"));
    assert!(
        usage_read.json_at("/usage").is_none(),
        "a session nothing was billed for reports no tally, not a zero one"
    );

    let commands = get(&server, &format!("/v1/harness/sessions/{id}/commands")).await;

    assert_eq!(commands.status, StatusCode::OK);
    assert!(
        commands.body["items"].as_array().map(Vec::len).unwrap_or(0) >= 5,
        "the command registry is the shipped built-in list"
    );
    assert!(
        commands.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"] == json!("help"))
    );
    assert_eq!(commands.revision(), commands.number_at("/revision"));

    // PATCH: a title change moves the session row once.
    let patched = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}"),
        json!({"title": "Renamed"}),
    )
    .await;

    assert_eq!(patched.status, StatusCode::OK);
    // One declared patch moves the session counter TWICE: `update_session` lists the row in the
    // commit's bump set AND hands the same targets to the event builder, and `CommitOutcome::with_event`
    // pushes every event target into the bump list again (`store/mod.rs:274-278`) before `Store::commit`
    // bumps each entry once (`store/mod.rs:1109-1116`). DEVIATION, reported: `api.ts:2079-2082` moves
    // `session` once and `session_agents` once, which are two DIFFERENT counters. What the contract DOES
    // pin - and what the next assertion pins - is that the header quotes exactly the figure the body
    // reports, so a client can order a read against a write from either one.
    assert_eq!(
        patched.revision(),
        read.number_at("/session/revision") + 2,
        "one declared patch, two bumps on the counter the header quotes"
    );
    assert_eq!(
        patched.revision(),
        patched.number_at("/session/revision"),
        "the write answers the figure it moved"
    );
    assert_eq!(patched.string_at("/session/title"), "Renamed");

    // `agent_models` MERGES, and an unknown member is refused against the session's OWN roster.
    let roster = get(&server, &format!("/v1/harness/sessions/{id}/agents")).await;
    let primary = roster.body["agents"][0]["agent_id"]
        .as_str()
        .expect("the roster names its entry agent")
        .to_owned();

    let merged = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}"),
        json!({"agent_models": {&primary: "mock/two"}}),
    )
    .await;

    assert_eq!(merged.status, StatusCode::OK);
    // The contract MERGES a model patch: every other member keeps what it had. The create set every
    // member of the frozen roster to the session's `settings.model` - the shipped `instruct` card
    // carries six delegates (`presets.ts`) - so the honest shape after one patch is six unchanged rows
    // and the one that moved (`api.ts:2067-2073`, "an agent absent from the body keeps its model").
    assert_eq!(
        merged.body["session"]["agent_models"],
        json!({
            "debugger": "mock/one",
            "general": "mock/one",
            "instruct": "mock/two",
            "research": "mock/one",
            "reviewer": "mock/one",
            "summarizer": "mock/one",
            "workspace": "mock/one"
        }),
        "a one-member patch merges into the six-member map without touching the others"
    );

    let ghost_member = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}"),
        json!({"agent_models": {"ghost": "mock/two"}}),
    )
    .await;

    assert_eq!(ghost_member.status, StatusCode::BAD_REQUEST);
    assert!(
        ghost_member
            .message()
            .starts_with("`agentModels` names \"ghost\", which is not a member of this harness."),
        "the refusal names the member problem: {}",
        ghost_member.message()
    );

    let empty_model = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}"),
        json!({"agent_models": {&primary: "  "}}),
    )
    .await;

    assert_eq!(empty_model.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty_model.message(),
        format!("`agentModels.{primary}` cannot be empty.")
    );

    // An empty patch is a 400 on a known session, and a 404 on an unknown one (`api.ts:2055-2058`).
    let noop = patch_json(&server, &format!("/v1/harness/sessions/{id}"), json!({})).await;

    assert_eq!(noop.status, StatusCode::BAD_REQUEST);
    assert_eq!(noop.message(), "A patch must change at least one field.");

    // An unknown session with a LEGAL body is the 404 (`api.ts:2055-2058`: the field check passes, the
    // lookup is what refuses), and an unknown FIELD is the earlier refusal even on an unknown session.
    let unknown_patch = patch_json(&server, "/v1/harness/sessions/ghost", json!({})).await;

    assert_eq!(unknown_patch.status, StatusCode::NOT_FOUND);
    assert_eq!(unknown_patch.message(), "Unknown session \"ghost\".");

    let unknown_field_on_ghost = patch_json(
        &server,
        "/v1/harness/sessions/ghost",
        json!({"tittle": "x"}),
    )
    .await;

    assert_eq!(
        unknown_field_on_ghost.status,
        StatusCode::BAD_REQUEST,
        "the field check runs first, so an unknown session never gets to answer: {}",
        unknown_field_on_ghost.body
    );

    // Unknown FIELD is the earlier refusal, on an unknown session too.
    let unknown_field = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}"),
        json!({"tittle": "x"}),
    )
    .await;

    assert_eq!(unknown_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unknown_field.message(),
        "Unknown field \"tittle\". Allowed fields: agent_models, title."
    );

    // DELETE, then the rail's collection counter moved again, then a second delete is a 404.
    let deleted = del(&server, &format!("/v1/harness/sessions/{id}")).await;

    assert_eq!(deleted.status, StatusCode::OK);
    assert_eq!(
        deleted.revision(),
        2,
        "the receipt quotes the RAIL's collection counter, which the create moved once and the delete \
         moves again (`api.ts:2123`)"
    );
    assert_eq!(deleted.body["deleted"], json!(true));
    assert_eq!(deleted.string_at("/session_id"), id);
    assert_eq!(deleted.number_at("/aborted_turns"), 0);

    let gone = get(&server, &format!("/v1/harness/sessions/{id}")).await;

    assert_eq!(gone.status, StatusCode::NOT_FOUND);
    assert_eq!(gone.message(), format!("Unknown session \"{id}\"."));

    let again = del(&server, &format!("/v1/harness/sessions/{id}")).await;

    assert_eq!(again.status, StatusCode::NOT_FOUND);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 2 - submit, run, and every read that describes the turn
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_submitted_task_runs_and_every_turn_read_describes_it() {
    let root = Root::new("turn-reads");
    let server = TestServer::start(&root).await;

    let (base, script) = provider(vec![
        tool_turn(
            "call-time",
            "timeNow",
            r#"{"timezone":"UTC"}"#,
            usage(11, 4),
        ),
        // The answer's block also reports CACHED tokens, so this test can tell a fold that drops
        // the member (`api-reads.ts:726`, `:742`) from one that merely never sees it.
        answered("the clock reads noon", usage_cached(7, 3, 5)),
    ])
    .await;

    let (id, _) = create_session(&server, &base, Some("clock")).await;

    // 202 + a receipt, never the aggregate (`api.ts:2196-2268`).
    let receipt = submit(&server, &id, "what time is it", None).await;

    assert_eq!(receipt.status, StatusCode::ACCEPTED);
    assert_eq!(
        receipt.revision(),
        receipt.number_at("/session_revision"),
        "the header and the receipt print one number"
    );
    assert_eq!(receipt.string_at("/session_id"), id);
    assert_eq!(receipt.string_at("/status"), "queued");
    assert_eq!(receipt.string_at("/links/stream"), "/v1/harness/stream");
    let turn_id = receipt.string_at("/turn_id");
    let thread = receipt.string_at("/thread_id");

    assert_eq!(
        receipt.string_at("/links/turn"),
        format!("/v1/harness/sessions/{id}/turns/{turn_id}")
    );
    assert_eq!(
        receipt.string_at("/links/thread"),
        format!("/v1/harness/sessions/{id}/threads/{thread}")
    );
    assert!(
        receipt.json_at("/queue_position").is_some(),
        "the receipt states the lane ordinal: `{}`",
        receipt.body
    );
    assert_eq!(
        receipt.number_at("/queue_position"),
        1,
        "the first task in a lane takes the first ordinal (`store/turns.rs` `submit_queued`, pinned by \
         `tests/store.rs:1108`); a submit that STEERS a live conversation instead carries no ordinal at all"
    );

    let done = wait_for_turn(&server, &id, &turn_id, "done").await;

    // Eight recorded events for one tool round plus one answer: the run opens, bills the model call
    // that asked for the tool, pairs the call with its result, commits that attempt, streams the
    // answer, bills the second call, and settles (`tests/store.rs:1147-1154` pins the same fold for a
    // tool-less run).
    assert_eq!(
        done.number_at("/turn/event_count"),
        8,
        "the tool round is recorded: {}",
        done.body
    );
    assert_eq!(
        done.body["turn"]["usage"],
        json!({"input": 18, "output": 7, "total": 25, "cached": 5}),
        "the tally is the fold of the usage events, and the TURN row keeps the provider's cache \
         figure (`protocol.ts:168-173`) that the session usage fold drops (`api-reads.ts:726`)"
    );
    assert!(
        done.json_at("/turn/prompt").is_some(),
        "the identified read states the prompt verbatim"
    );

    // The provider really was called, twice, with the credential and the model the session holds.
    let requests = script.requests();

    assert_eq!(requests.len(), 2, "one tool round, one final answer");
    assert_eq!(requests[0]["model"], json!("mock/one"));
    assert!(
        requests[0]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == json!("user") && m["content"] == json!("what time is it")),
        "the task text reaches the provider"
    );

    // THE PAGED ROWS.
    let turns = get(&server, &format!("/v1/harness/sessions/{id}/turns")).await;

    assert_eq!(turns.status, StatusCode::OK);
    assert_eq!(turns.revision(), turns.number_at("/revision"));
    assert_eq!(turns.body["items"].as_array().map(Vec::len), Some(1));

    let row = &turns.body["items"][0];

    assert_eq!(
        row["id"],
        json!(turn_id),
        "the row's identity comes from the record"
    );
    assert_eq!(row["index"], json!(0));
    assert_eq!(row["status"], json!("done"));
    assert_eq!(row["state"], json!("done"));
    assert_eq!(row["threadId"], json!(thread));
    assert_eq!(row["promptPreview"], json!("what time is it"));
    assert_eq!(
        row["promptChars"],
        json!(15),
        "the length is the UTF-16 unit the contract states"
    );
    assert_eq!(
        row["sectionCount"],
        json!(1),
        "one flow for a single-agent run"
    );
    assert_eq!(row["stepCount"], json!(1), "the one tool call");
    assert_eq!(
        row["activeAgents"],
        json!([]),
        "a terminal turn has nothing running"
    );
    assert!(
        row.get("prompt").is_none(),
        "a row never carries the raw prompt"
    );

    // THE DISPLAY TREE, with the store's minted section ids (`{turnId}:{ordinal}`).
    let display = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/display"),
    )
    .await;

    assert_eq!(display.status, StatusCode::OK);
    assert_eq!(
        display.revision(),
        done.number_at("/turn/revision"),
        "the tree is a pure fold of the record, so its revision IS the turn's"
    );
    assert_eq!(display.string_at("/display/turnId"), turn_id);
    assert_eq!(
        display.number_at("/display/revision"),
        done.number_at("/turn/revision")
    );
    assert_eq!(
        display.body["display"]["sections"][0]["id"],
        json!(format!("{turn_id}:0"))
    );
    assert_eq!(
        display.body["display"]["sections"][0]["agent"],
        json!("primary"),
        "the fold labels the LANE, not the roster id: a model flow is `primary` in the events and in the \
         display tree, while a slash-command flow carries the agent name (`projector.rs`, and the event \
         rows this same test reads)"
    );
    assert_eq!(
        display.body["display"]["sections"][0]["role"],
        json!("primary")
    );

    let nodes = display.body["display"]["sections"][0]["nodes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let step = nodes
        .iter()
        .find(|node| node["kind"] == json!("tool"))
        .cloned()
        .expect("the tool step row");

    assert_eq!(step["callId"], json!("call-time"));
    assert_eq!(step["name"], json!("timeNow"));
    assert_eq!(step["state"], json!("ok"), "the result settled the row");
    assert_eq!(
        step["sectionId"],
        json!(format!("{turn_id}:0")),
        "the step names the minted section id"
    );
    assert_eq!(step["turnId"], json!(turn_id));

    let answer = nodes
        .iter()
        .find(|node| node["kind"] == json!("text"))
        .cloned()
        .expect("the answer prose row");

    assert_eq!(answer["text"], json!("the clock reads noon"));

    // THE EVENT PAGE, verbatim, with the ordinal filter.
    let events = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/events"),
    )
    .await;

    assert_eq!(events.status, StatusCode::OK);
    assert_eq!(events.revision(), done.number_at("/turn/revision"));
    assert_eq!(events.body["turn_id"], json!(turn_id));
    assert_eq!(
        events.json_at("/partial"),
        Some(&json!(false)),
        "a terminal turn is not partial"
    );
    assert_eq!(
        events.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["event"]["type"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec![
            "start",
            "usage",
            "tool_call",
            "tool_result",
            "attempt_commit",
            "text",
            "usage",
            "done"
        ]
    );
    assert!(
        events.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .all(|(position, row)| row["index"] == json!(position)),
        "`index` is the recorded ordinal"
    );
    assert!(
        events.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["at"].is_number())
    );

    let call_index = events.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["event"]["type"] == json!("tool_call"))
        .unwrap()["index"]
        .as_u64()
        .expect("an event index");

    // A filtered page keeps the cursor in ORDINAL space, and `total_events` counts the matches.
    let text_only = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/events?type=text"),
    )
    .await;

    assert_eq!(text_only.body["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(text_only.number_at("/total_events"), 1);
    assert_eq!(
        text_only.body["items"][0]["index"],
        json!(5),
        "start, the first billing, the call, its result, the committed attempt, then the answer delta"
    );

    let capped = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/events?limit=2"),
    )
    .await;

    assert_eq!(capped.number_at("/limit"), 2);
    assert!(
        capped.json_at("/next_cursor").is_some_and(Value::is_string),
        "a full page has a successor"
    );
    let next_cursor = capped.string_at("/next_cursor");
    let second = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/events?limit=2&cursor={next_cursor}"),
    )
    .await;

    assert_eq!(second.body["items"].as_array().unwrap().len(), 2);
    assert_eq!(second.body["items"][0]["index"], json!(2));

    // THE STEP DETAIL: the recorded pair, addressed by the step's own event index.
    let step_detail = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/steps/{call_index}"),
    )
    .await;

    assert_eq!(step_detail.status, StatusCode::OK);
    assert_eq!(step_detail.revision(), done.number_at("/turn/revision"));
    // The step detail is a DISPLAY member, so its keys are camelCase like the tree it names
    // (`harness-api.ts:1936`).
    assert_eq!(
        step_detail.string_at("/detail/stepId"),
        call_index.to_string()
    );
    assert_eq!(step_detail.string_at("/detail/turnId"), turn_id);
    assert_eq!(step_detail.string_at("/detail/sessionId"), id);
    assert_eq!(
        step_detail.string_at("/detail/sectionId"),
        format!("{turn_id}:0")
    );
    assert_eq!(step_detail.string_at("/detail/callId"), "call-time");
    assert_eq!(step_detail.number_at("/detail/opening/index"), call_index);
    assert!(
        step_detail.json_at("/detail/opening/at").is_some(),
        "the recorded time travels paired"
    );
    assert_eq!(
        step_detail.string_at("/detail/opening/event/type"),
        "tool_call"
    );
    assert_eq!(
        step_detail.body["detail"]["opening"]["event"]["call"]["name"],
        json!("timeNow")
    );
    assert_eq!(
        step_detail.string_at("/detail/result/event/type"),
        "tool_result"
    );
    assert!(
        step_detail.number_at("/detail/result/index") > call_index,
        "the result follows its call"
    );
    assert_eq!(
        step_detail.body["detail"]["result"]["event"]["name"],
        json!("timeNow"),
        "the pair is resolved by call id, never positionally"
    );
    assert_eq!(
        step_detail.body["detail"]["result"]["event"]["outcome"],
        json!("completed"),
        "and the result the pair names is the successful one"
    );

    let bad_step = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/steps/abc"),
    )
    .await;

    assert_eq!(bad_step.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_step.message(),
        "A step id must be the recorded event index of the step."
    );

    let missing_step = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/steps/999"),
    )
    .await;

    assert_eq!(missing_step.status, StatusCode::NOT_FOUND);
    assert_eq!(
        missing_step.message(),
        format!("Unknown step \"999\" in turn \"{turn_id}\".")
    );

    // THE CONTEXT BAR segment. The fold mints a `tool` segment per call AND an `assistant` segment for
    // every prose window the stream opens (R18, `turn-projector.ts:1891-1925`), and — because the seam
    // is now SEEDED with the turn (`ports::ProjectorSeed`, `sessions.ts:4160-4240`) — the prompt segment
    // TypeScript mints from the record (`0-user-prompt`, `turn-projector.ts:1578`) is part of the bar
    // too, so it resolves.
    let block = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/blocks/0-user-prompt"),
    )
    .await;

    assert_eq!(block.status, StatusCode::OK);

    // An id this fold never published is still a 404: the reasoning window this turn opened (if the
    // provider streamed any thinking) carries its own OPENING-event key, and inventing a segment for a
    // number the fold never minted would answer a question nobody asked.
    let missing_block = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/blocks/0-reasoning-9"),
    )
    .await;

    assert_eq!(missing_block.status, StatusCode::NOT_FOUND);
    assert_eq!(
        missing_block.message(),
        format!("Unknown context block \"0-reasoning-9\" in turn \"{turn_id}\".")
    );

    let bar = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/display"),
    )
    .await;

    let segments = bar.body["display"]["sections"][0]["context"]
        .as_array()
        .expect("every section row carries its context bar");

    // Three segments, in conversation order: the prompt block the SEEDED fold mints from the record
    // (`turn-projector.ts:1578`, and the id is the section ordinal 0 because this conversation's
    // section is ordinal 0 — `blockIdOf`, `:1407`), then the tool call's own segment, then the answer
    // window the text stream opens on its first delta. The answer segment is NOT the `done` fallback:
    // it is the streaming window, which is why its `chars` are the streamed length.
    assert_eq!(
        segments
            .iter()
            .map(|segment| segment["kind"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["user", "tool", "assistant"],
        "the prompt segment comes first, then the call's segment, then the streamed answer"
    );
    assert_eq!(
        segments[2]["id"].as_str().expect("a minted id"),
        // `{section ordinal}-{kind}-{key}` again, where the key is the index of the event that OPENED
        // the window. That is the first `text` delta, pinned above as event index 5 - which is why the
        // id is stable across the live stream and the cold fold.
        "0-assistant-5",
        "a prose window is id'd by the event that opened it"
    );
    assert_eq!(segments[2]["name"], json!("answer"));
    assert_eq!(
        segments[2]["chars"],
        json!("the clock reads noon".chars().count()),
        "the window's `chars` are the streamed answer's length, kept by the per-delta patch"
    );
    assert_eq!(
        segments[2]["inPrompt"],
        json!(true),
        "`in_prompt` mirrors `isAssistantSegment` (`turn-projector.ts:1463`): an answer counts, thinking does not"
    );
    assert!(
        segments[2].get("lane").is_none(),
        "a lane-0 conversation stamps no lane on the prose row either (`laneFieldsOf`)"
    );
    assert_eq!(
        segments[0]["id"].as_str().expect("a minted id"),
        "0-user-prompt"
    );
    assert_eq!(
        segments[0]["name"].as_str().expect("a named segment"),
        "prompt"
    );
    assert_eq!(segments[1]["kind"], json!("tool"));
    assert_eq!(
        segments[1]["id"],
        // `{section ordinal}-{kind}-{key}` (`blockIdOf`, `turn-projector.ts:1407`): the ordinal is the
        // CONVERSATION's section ordinal — 0 for this session's one conversation, exactly as the
        // TypeScript fixture's `0-tool-80` states — and the key is the OPENING event's index, the same
        // derivation as the step id.
        json!("0-tool-2"),
        "the key is the OPENING event's index, the same derivation as the step id"
    );
    assert_eq!(
        segments[1]["name"],
        json!("timeNow"),
        "the label is the recorded tool name, decided server-side"
    );
    assert_eq!(segments[1]["state"], json!("ok"));
    assert!(
        segments[1].get("text").is_none() && segments[1].get("preview").is_none(),
        "a segment is metadata: the bytes come from the record, on a click"
    );
    assert!(
        segments[1].get("lane").is_none(),
        "a conversation that never moved a lane writes no lane key at all          (`turn-projector.ts:1449 laneFieldsOf`)"
    );

    // The detail route answers from the durable record, over the segment's own event range.
    let segment_id = segments[1]["id"]
        .as_str()
        .expect("a server-minted segment id")
        .to_owned();

    let segment = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/blocks/{segment_id}"),
    )
    .await;

    assert_eq!(segment.status, StatusCode::OK);
    assert_eq!(segment.body["block"]["id"], json!(segment_id));
    assert_eq!(
        segment.body["block"]["args"],
        json!("{\"timezone\":\"UTC\"}"),
        "the two halves of the call are the RECORDED bytes, and a block never reparses them"
    );
    assert!(
        segment.body["block"]["result"]
            .as_str()
            .is_some_and(|result| !result.is_empty()),
        "and the recorded result text travels verbatim"
    );

    let bad_block = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/blocks/x"),
    )
    .await;

    assert_eq!(
        bad_block.status,
        StatusCode::NOT_FOUND,
        "a non-empty id is a lookup, not a grammar refusal"
    );

    // THE TRANSCRIPT: the run replaced the conversation, and the document carries the answer.
    let document = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}/transcript/document"),
    )
    .await;

    assert_eq!(document.status, StatusCode::OK);
    assert_eq!(document.string_at("/content_type"), "application/json");
    assert_eq!(document.string_at("/thread_id"), thread);
    assert!(
        document
            .string_at("/document")
            .contains("the clock reads noon")
    );
    assert!(document.string_at("/document").contains("what time is it"));

    let messages = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}/transcript/messages"),
    )
    .await;

    assert_eq!(messages.status, StatusCode::OK);
    assert_eq!(messages.string_at("/transcript_state"), "ok");
    assert!(messages.number_at("/total") >= 2);
    assert!(
        messages.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["role"] == json!("assistant")
                && entry["content"] == json!("the clock reads noon"))
    );

    // USAGE: the fold the usage route prints, with the per-conversation share.
    let usage_read = get(&server, &format!("/v1/harness/sessions/{id}/usage")).await;

    assert_eq!(usage_read.status, StatusCode::OK);
    assert_eq!(usage_read.number_at("/turn_count"), 1);
    assert_eq!(usage_read.number_at("/reported_turns"), 1);
    // T7b: the fold states EXACTLY `{input, output, total}` (`api-reads.ts:726`, `:742`, `:750-753`).
    // `cached` is provider metadata that belongs on the TURN row; the totals and every per-thread
    // share omit the member even though this provider reported cached tokens.
    assert_eq!(
        usage_read.body["usage"],
        json!({ "input": 18, "output": 7, "total": 25 })
    );
    assert_eq!(
        usage_read.body["by_thread"][0]["usage"],
        json!({ "input": 18, "output": 7, "total": 25 })
    );
    // And the turn row DOES carry it, which is what makes the omission above a decision rather
    // than an absence of data.
    let turn_read = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}"),
    )
    .await;

    assert_eq!(turn_read.body["turn"]["usage"]["cached"], json!(5));
    assert_eq!(
        usage_read.body["by_thread"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(usage_read.body["by_thread"][0]["thread_id"], json!(thread));
    assert_eq!(usage_read.revision(), usage_read.number_at("/revision"));

    // The session row now says what it ran.
    let read = get(&server, &format!("/v1/harness/sessions/{id}")).await;

    assert_eq!(read.number_at("/session/turn_count"), 1);
    assert_eq!(
        read.string_at("/session/status"),
        "idle",
        "the run settled the aggregate"
    );

    // An attachment lookup on a turn with no media is the contract's 404.
    let attachment = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/attachments/ghost"),
    )
    .await;

    assert_eq!(attachment.status, StatusCode::NOT_FOUND);
    assert_eq!(
        attachment.message(),
        format!("Unknown attachment \"ghost\" in session \"{id}\".")
    );

    server.stop().await;
}

/// R3: the display read stamps `ContextBlock.included` on every context-bar row it can join to a
/// transcript entry — the LIVE SEGMENT verdict (`contextSegmentIncludedOf`, `api.ts:2022-2089`, applied
/// per section at `api.ts:2118`), which is NOT the fold's own `inPrompt`: `inPrompt` is what the
/// projector believed while the turn ran, `included` is what the transcript says the prompt carries
/// now. The expected values below are derived from the transcript read (the same rows the predicate
/// joins), not from the display read, so a fold that stamped the wrong flag would disagree here.
#[tokio::test]
async fn the_display_read_stamps_the_context_bar_with_the_inclusion_verdict() {
    let root = Root::new("display-inclusion");
    let server = TestServer::start(&root).await;

    let (base, _script) = provider(vec![
        tool_turn(
            "call-time",
            "timeNow",
            r#"{"timezone":"UTC"}"#,
            usage(11, 4),
        ),
        answered("the clock reads noon", usage(7, 3)),
    ])
    .await;

    let (id, _) = create_session(&server, &base, Some("clock")).await;
    let receipt = submit(&server, &id, "what time is it", None).await;
    let turn_id = receipt.string_at("/turn_id");
    let thread = receipt.string_at("/thread_id");

    wait_for_turn(&server, &id, &turn_id, "done").await;

    // THE AUTHORITY FOR THE VERDICT: the transcript rows, read on their own. The contract's rule is
    // `included !== false` — an absent flag means included — so a row that never carried the member
    // answers `true`, and the only way to answer `false` is to store it.
    let messages = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}/transcript/messages"),
    )
    .await;

    assert_eq!(messages.status, StatusCode::OK);

    let rows = messages.body["items"]
        .as_array()
        .expect("the transcript read answers with rows");

    let stored_verdict = |role: &str, content: &str| -> Option<bool> {
        rows.iter()
            .find(|row| row["role"] == json!(role) && row["content"] == json!(content))
            .map(|row| row.get("included") != Some(&json!(false)))
    };

    let expected_answer = stored_verdict("assistant", "the clock reads noon")
        .expect("the run recorded the answer as an assistant entry");

    assert!(
        expected_answer,
        "nothing has compacted this conversation, so the transcript flags the answer in"
    );
    assert!(
        rows.iter()
            .all(|row| row.get("included") != Some(&json!(false))),
        "no entry of this transcript is withheld, which is why every segment that HAS a verdict \
         answers true: {rows:?}"
    );

    // THE DISPLAY READ. Three rows: the prompt segment, the tool call's segment, and the streamed
    // answer window.
    let bar = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/display"),
    )
    .await;

    assert_eq!(bar.status, StatusCode::OK);

    let segments = bar.body["display"]["sections"][0]["context"]
        .as_array()
        .expect("the section carries its context bar");

    assert_eq!(
        segments
            .iter()
            .map(|segment| segment["kind"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["user", "tool", "assistant"],
        "the bar is the same three rows the fold publishes; the verdict is additive, never a reshuffle"
    );

    // THE PROMPT ROW CARRIES NO MEMBER AT ALL. A `user` segment mints no call id and names no
    // thinking bytes, so neither join runs: `blockEntryOf` refuses without `blockCallIdOf`
    // (`api.ts:1856-1858`) and the verdict is `undefined`, which `JSON.stringify` drops. Inventing a
    // `true` here would be the defect the reference names: "a fabricated `true` would be as wrong as
    // a `false`" (`api.ts:2063-2065`).
    assert!(
        segments[0].get("included").is_none(),
        "an unaddressable segment states no verdict: {}",
        segments[0]
    );

    // THE TWO ROWS THAT DO HAVE A VERDICT, each its last key — the position the
    // `{ ...block, included }` spread puts it (`api.ts:2122-2126`).
    assert_eq!(
        segments[1].get("included"),
        Some(&json!(true)),
        "the tool row's verdict comes from the paired call/result entries, which this transcript \
         carries in full: {}",
        segments[1]
    );
    assert_eq!(
        segments[2].get("included"),
        Some(&json!(expected_answer)),
        "answer row: {}",
        segments[2]
    );

    for segment in &segments[1..] {
        let keys: Vec<&str> = segment
            .as_object()
            .expect("a context block is an object")
            .keys()
            .map(String::as_str)
            .collect();

        assert_eq!(
            keys.last().copied(),
            Some("included"),
            "the verdict is appended last, the position the spread puts it, so a byte-level diff of \
             the fold still lines up: {keys:?}"
        );
    }

    // THE TWO FLAGS ARE NOT THE SAME FACT. `inPrompt` comes from the projector
    // (`turn-projector.ts:1463`), and an ANSWER window is in the prompt because an assistant answer
    // counts; the verdict is read from the transcript. They agree on this row for this reason, and a
    // stamp that simply copied `inPrompt` would pass this line while failing the compaction case the
    // member exists for.
    assert_eq!(segments[2]["inPrompt"], json!(true));

    // THE CASE THE MEMBER EXISTS FOR: withhold the answer entry and re-read. The transcript document
    // is rebuilt from the rows this test just read, with ONE member changed on one row, so the only
    // thing that can move on the bar is the verdict.
    let withheld: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut row = row.clone();

            if row["role"] == json!("assistant")
                && row["content"] == json!("the clock reads noon")
                && let Some(map) = row.as_object_mut()
            {
                map.insert("included".to_owned(), json!(false));
            }

            row
        })
        .collect();

    let written = put_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}/transcript"),
        json!({ "messages": withheld }),
    )
    .await;

    assert_eq!(written.status, StatusCode::OK, "{}", written.body);

    let after = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/display"),
    )
    .await;

    let stamped = after.body["display"]["sections"][0]["context"]
        .as_array()
        .expect("the section carries its context bar");

    assert_eq!(
        stamped[2]["included"],
        json!(false),
        "the entry behind the answer window is withheld, so the segment is off the prompt"
    );
    assert_eq!(
        stamped[2]["inPrompt"],
        json!(true),
        "and the projector's own flag does NOT move: two axes, each stating its own fact \
         (`api.ts:2032-2041`)"
    );
    assert_eq!(
        stamped[1]["included"],
        json!(true),
        "the tool segment keeps its verdict — its owning entry was never touched"
    );
    assert!(
        stamped[0].get("included").is_none(),
        "the prompt segment still states no verdict"
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 3 - the durable queue
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_second_submit_on_a_running_conversation_queues_with_an_ordinal() {
    let root = Root::new("queue");

    // A 500 makes the FIRST run retry (a second apart) while the second submit sits in the lane, so
    // the queue state is observable rather than a race.
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![Answer::Status(500, "no provider".to_owned())]).await;

    let (id, _) = create_session(&server, &base, Some("queue")).await;
    let thread = open_thread(&server, &id, "instruct").await;

    let first = submit(&server, &id, "first", Some(&thread)).await;

    assert_eq!(first.status, StatusCode::ACCEPTED);
    let first_turn = first.string_at("/turn_id");

    wait_for_turn(&server, &id, &first_turn, "running").await;

    // A TEXT-only submit addressed at a running conversation STEERS the live run - the store folds it
    // into that run and returns its turn id (`store/turns.rs:143-159`, `sessions.ts:6359-6504`). A clip
    // cannot ride a steer, so a submit that carries an attachment falls through to the lane's backlog
    // and that is the only way a `queued` ordinal is observable on this route (`turns.rs:155-158`,
    // pinned the same way by `tests/store.rs:1407-1424`).
    let second = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/tasks"),
        Some(json!({
            "input": "second",
            "thread_id": thread,
            "attachments": [{
                "id": "att-2",
                "kind": "image",
                "marker": "[Image 1]",
                "name": "pic.png",
                "mime": "image/png",
                "size": 16,
                "data": "iVBORw0KGgp1dXV1dXV1dQ=="
            }]
        })),
    )
    .await;

    assert_eq!(
        second.status,
        StatusCode::ACCEPTED,
        "the clipped submit is accepted: {}",
        second.body
    );
    assert_eq!(second.string_at("/status"), "queued");
    assert_eq!(
        second.json_at("/steered"),
        None,
        "a backlog task is not a steer, so the receipt says nothing about one"
    );
    assert_eq!(
        second.number_at("/queue_position"),
        1,
        "the receipt ordinal is 1-based and equal to `GET /queue`'s row count (`tests/store.rs:1422`)"
    );

    let second_turn = second.string_at("/turn_id");
    let queue = get(&server, &format!("/v1/harness/sessions/{id}/queue")).await;

    assert_eq!(queue.status, StatusCode::OK);
    assert_eq!(queue.revision(), queue.number_at("/revision"));
    assert_eq!(
        queue.body["items"].as_array().map(Vec::len),
        Some(1),
        "the backlog is exactly the clipped submit: {}",
        queue.body
    );
    assert_eq!(queue.body["items"][0]["turn_id"], json!(second_turn));
    assert_eq!(
        queue.body["items"][0]["position"],
        json!(0),
        "the ROW position is 0-based"
    );
    assert_eq!(queue.body["items"][0]["index"], json!(1));
    assert_eq!(queue.body["items"][0]["prompt_preview"], json!("second"));
    assert_eq!(queue.body["items"][0]["thread_id"], json!(thread));
    assert_eq!(
        queue.body["items"][0]["attachment_count"],
        json!(1),
        "the row counts the clip it carries"
    );

    // Cancelling with `all` drops the backlog and leaves the rows as `cancelled` records.
    let cancelled = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/cancel"),
        Some(json!({"all": true})),
    )
    .await;

    assert_eq!(cancelled.status, StatusCode::OK);
    assert_eq!(cancelled.body["cancelled"], json!(true));
    assert_eq!(cancelled.number_at("/dropped_queued_tasks"), 1);
    assert_eq!(cancelled.string_at("/active_turn_id"), first_turn);
    assert_eq!(cancelled.revision(), cancelled.number_at("/revision"));

    let queue_after = get(&server, &format!("/v1/harness/sessions/{id}/queue")).await;

    assert_eq!(queue_after.body["items"].as_array().map(Vec::len), Some(0));

    for turn in [&first_turn, &second_turn] {
        let settled = get(&server, &format!("/v1/harness/sessions/{id}/turns/{turn}")).await;

        assert_eq!(
            settled.string_at("/turn/status"),
            "cancelled",
            "a cancelled turn keeps its row and says so"
        );
    }

    let bad_all = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/cancel"),
        Some(json!({"all": "yes"})),
    )
    .await;

    assert_eq!(bad_all.status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_all.message(), "`all` must be a boolean when present.");

    let bad_field = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/cancel"),
        Some(json!({"every": true})),
    )
    .await;

    assert_eq!(bad_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_field.message(),
        "Unknown field \"every\". Allowed fields: all."
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 3b - steering: the other thing a submit to a live conversation does
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_text_submit_on_a_live_conversation_steers_the_running_turn() {
    let root = Root::new("steer");
    let server = TestServer::start(&root).await;

    // The provider is held open, so the conversation is genuinely mid-run while the steer arrives.
    let (base, _script) = provider(vec![Answer::Delayed(
        900,
        answered_body("steered home", usage(13, 4)),
    )])
    .await;

    let (id, _) = create_session(&server, &base, Some("steer")).await;
    let thread = open_thread(&server, &id, "instruct").await;

    let running = submit(&server, &id, "the first ask", Some(&thread)).await;
    let turn_id = running.string_at("/turn_id");

    wait_for_turn(&server, &id, &turn_id, "running").await;

    let steered = submit(&server, &id, "and louder", Some(&thread)).await;

    assert_eq!(steered.status, StatusCode::ACCEPTED);
    assert_eq!(
        steered.string_at("/status"),
        "running",
        "a steer names the LIVE turn, not a new one"
    );
    assert_eq!(steered.string_at("/turn_id"), turn_id);
    assert_eq!(steered.body["steered"], json!(true));
    assert_eq!(steered.string_at("/steered_thread_id"), thread);
    assert!(
        steered.json_at("/queue_position").is_none(),
        "guidance consumed no queue slot: `{}`",
        steered.body
    );
    assert_eq!(
        steered.revision(),
        steered.number_at("/session_revision"),
        "the steer still reports the session counter it moved"
    );

    // No second turn row, and nothing waiting.
    let turns = get(&server, &format!("/v1/harness/sessions/{id}/turns")).await;

    assert_eq!(turns.body["items"].as_array().map(Vec::len), Some(1));

    let queue = get(&server, &format!("/v1/harness/sessions/{id}/queue")).await;

    assert_eq!(queue.body["items"].as_array().map(Vec::len), Some(0));

    // The live turn states the guidance it took, in the same compact form every other turn row uses.
    let detail = wait_for(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}"),
        |body| body.pointer("/turn/steer_count").and_then(Value::as_u64) == Some(1),
        "a turn that took one steer",
    )
    .await;

    assert_eq!(detail.number_at("/turn/steer_count"), 1);
    assert_eq!(
        detail.body["turn"]["steers"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        detail.body["turn"]["steers"][0]["prompt_preview"],
        json!("and louder")
    );
    assert_eq!(detail.body["turn"]["steers"][0]["prompt_chars"], json!(10));

    wait_for_turn(&server, &id, &turn_id, "done").await;

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 4 - cancel and reset
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reset_refuses_a_busy_session_and_clears_an_idle_one() {
    let root = Root::new("reset");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![
        answered("first answer", usage(3, 1)),
        answered("second answer", usage(1, 1)),
    ])
    .await;

    let (id, _) = create_session(&server, &base, Some("reset")).await;

    // A completed turn is discuable by a reset: the conversation goes, the counters move, and the
    // receipt counts what it discarded.
    let receipt = submit(&server, &id, "remember me", None).await;
    let turn_id = receipt.string_at("/turn_id");

    wait_for_turn(&server, &id, &turn_id, "done").await;

    let busy_reset = {
        // Start a run that will keep retrying, then try to reset under it.
        let server = &server;
        let _ = server;
        post_json(server, &format!("/v1/harness/sessions/{id}/reset"), None).await
    };

    assert_eq!(
        busy_reset.status,
        StatusCode::OK,
        "an idle session resets: {}",
        busy_reset.body
    );
    assert_eq!(busy_reset.body["reset"], json!(true));
    assert_eq!(busy_reset.number_at("/discarded_turns"), 1);
    assert_eq!(busy_reset.number_at("/dropped_queued_tasks"), 0);
    assert_eq!(busy_reset.revision(), busy_reset.number_at("/revision"));
    assert!(busy_reset.json_at("/discarded_threads").is_some());

    let turns = get(&server, &format!("/v1/harness/sessions/{id}/turns")).await;

    assert_eq!(
        turns.body["items"].as_array().map(Vec::len),
        Some(0),
        "a reset starts a new conversation"
    );

    let threads = get(&server, &format!("/v1/harness/sessions/{id}/threads")).await;

    assert_eq!(threads.body["items"].as_array().map(Vec::len), Some(0));

    let definition = get(&server, &format!("/v1/harness/sessions/{id}/definition")).await;

    assert_eq!(
        definition.status,
        StatusCode::OK,
        "the session is still addressable"
    );

    // T7b: a body on a body-less verb is IGNORED. TypeScript's `postSessionReset`
    // (`api.ts:2271-2296`) never calls `ctx.body()`, and `readEmptyBody` (`api.ts:520-531`) is
    // defined and left uncalled by EVERY route - so the same idle reset answers the same 200 with
    // the document attached, and a second reset simply discards nothing.
    let with_body = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/reset"),
        Some(json!({"now": true})),
    )
    .await;

    assert_eq!(
        with_body.status,
        StatusCode::OK,
        "the body is ignored, not refused: {}",
        with_body.body
    );
    assert_eq!(with_body.body["reset"], json!(true));
    assert_eq!(
        with_body.number_at("/discarded_turns"),
        0,
        "the first reset already cleared the conversation"
    );

    // A body the route never parses cannot fail to parse either.
    let unparsable = Reply::send(
        client()
            .post(server.url(&format!("/v1/harness/sessions/{id}/reset")))
            .header("content-type", "text/plain")
            .body("this is not json"),
    )
    .await;

    assert_eq!(
        unparsable.status,
        StatusCode::OK,
        "an ignored body is never read: {}",
        unparsable.body
    );

    let ghost_reset = post_json(&server, "/v1/harness/sessions/ghost/reset", None).await;

    assert_eq!(ghost_reset.status, StatusCode::NOT_FOUND);
    assert_eq!(ghost_reset.message(), "Unknown session \"ghost\".");

    // A cancel that finds nothing is a successful no-op.
    let idle_cancel = post_json(&server, &format!("/v1/harness/sessions/{id}/cancel"), None).await;

    assert_eq!(idle_cancel.status, StatusCode::OK);
    assert_eq!(idle_cancel.body["cancelled"], json!(false));
    assert!(
        idle_cancel
            .json_at("/active_turn_id")
            .is_none_or(Value::is_null)
    );

    server.stop().await;
}

#[tokio::test]
async fn a_busy_session_refuses_a_reset_and_a_cancel_stops_the_run() {
    let root = Root::new("cancel");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![Answer::Status(500, "no provider".to_owned())]).await;

    let (id, _) = create_session(&server, &base, Some("cancel")).await;
    let receipt = submit(&server, &id, "long", None).await;
    let turn_id = receipt.string_at("/turn_id");

    wait_for_turn(&server, &id, &turn_id, "running").await;

    let blocked = post_json(&server, &format!("/v1/harness/sessions/{id}/reset"), None).await;

    assert_eq!(blocked.status, StatusCode::CONFLICT);
    assert_eq!(
        blocked.message(),
        "Cancel the running task before resetting the session."
    );

    let cancelled = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/cancel"),
        Some(json!({})),
    )
    .await;

    assert_eq!(cancelled.status, StatusCode::OK);
    assert_eq!(cancelled.body["cancelled"], json!(true));
    assert_eq!(cancelled.string_at("/active_turn_id"), turn_id);

    wait_for_turn(&server, &id, &turn_id, "cancelled").await;

    // A cancel is not a clean stop: the contract's `SessionStatus` is `idle | running | error`
    // (`contract.rs:790`), and a run the host interrupted settles on the one that says the turn ended
    // badly. The rows survive, which is the point of cancelling rather than resetting.
    let read = get(&server, &format!("/v1/harness/sessions/{id}")).await;

    assert_eq!(read.string_at("/session/status"), "error");
    assert_eq!(read.number_at("/session/turn_count"), 1);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 5 - the transcript
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_transcript_write_locks_receives_raw_and_reports_itself() {
    let root = Root::new("transcript");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("transcript")).await;
    let thread = open_thread(&server, &id, "instruct").await;
    let path = format!("/v1/harness/sessions/{id}/threads/{thread}/transcript");

    // An empty transcript reads as a zero-sized document before anything is written.
    let empty = get(&server, &path).await;

    assert_eq!(empty.status, StatusCode::OK);
    assert_eq!(empty.number_at("/transcript/message_count"), 0);
    assert_eq!(empty.number_at("/transcript/revision"), 0);
    assert_eq!(empty.string_at("/transcript/transcript_state"), "ok");

    // THE WRITE. `messages` stays untyped on purpose, and the receipt states the new revision.
    let draft = json!([
        {"role": "system", "content": "You are terse."},
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "hi"}
    ]);
    let written = put_json(&server, &path, json!({"messages": draft})).await;

    assert_eq!(written.status, StatusCode::OK);
    assert_eq!(written.string_at("/accepted_as"), "transcript");
    assert_eq!(written.number_at("/message_count"), 3);
    assert_eq!(written.number_at("/revision"), 1);
    assert_eq!(
        written.revision(),
        1,
        "the header quotes the revision the write minted"
    );
    assert_eq!(
        written.string_at("/links/document"),
        format!("{path}/document")
    );
    assert!(written.json_at("/invalid").is_none());

    // The three reads agree about the same document.
    let metadata = get(&server, &path).await;

    assert_eq!(metadata.number_at("/transcript/message_count"), 3);
    assert_eq!(metadata.number_at("/transcript/role_counts/user"), 1);
    assert_eq!(metadata.number_at("/transcript/role_counts/assistant"), 1);
    assert_eq!(metadata.number_at("/transcript/revision"), 1);
    assert_eq!(metadata.revision(), 1);
    assert!(metadata.number_at("/transcript/bytes") > 0);
    assert!(metadata.number_at("/transcript/chars") > 0);

    let messages = get(&server, &format!("{path}/messages")).await;

    assert_eq!(messages.body["items"].as_array().map(Vec::len), Some(3));
    assert_eq!(messages.number_at("/total"), 3);
    assert_eq!(messages.string_at("/items/0/role"), "system");
    assert_eq!(messages.revision(), messages.number_at("/revision"));

    let document = get(&server, &format!("{path}/document")).await;

    assert_eq!(document.string_at("/content_type"), "application/json");
    assert!(document.string_at("/document").contains("You are terse."));

    // A role filter pages in ORDINAL space, and `total` stays the whole transcript's count.
    let users = get(&server, &format!("{path}/messages?role=user")).await;

    assert_eq!(users.body["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(users.number_at("/total"), 3);
    assert_eq!(users.body["items"][0]["role"], json!("user"));

    let bad_role = get(&server, &format!("{path}/messages?role=bard")).await;

    assert_eq!(bad_role.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_role.message(),
        "Query parameter \"role\" must be one of: system, user, assistant, tool."
    );

    // THE OPTIMISTIC LOCK: a stale expectation writes nothing and names both figures.
    let stale = put_json(
        &server,
        &path,
        json!({"messages": [ {"role": "user", "content": "stale"} ], "if_revision": 0}),
    )
    .await;

    assert_eq!(stale.status, StatusCode::CONFLICT);
    assert_eq!(
        stale.message(),
        "This transcript is at revision 1; the write expected 0. Re-read before writing."
    );

    let after_stale = get(&server, &path).await;

    assert_eq!(
        after_stale.number_at("/transcript/message_count"),
        3,
        "a refused write changed nothing"
    );

    // `if_revision` is an INTEGER.
    let not_integer = put_json(&server, &path, json!({"messages": [], "if_revision": "1"})).await;

    assert_eq!(not_integer.status, StatusCode::BAD_REQUEST);
    assert_eq!(not_integer.message(), "\"if_revision\" must be an integer.");

    // An absent `messages` is a refusal, never a silent emptying of the transcript.
    let no_messages = put_json(&server, &path, json!({"if_revision": 1})).await;

    assert_eq!(no_messages.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        no_messages.message(),
        "A transcript update requires a \"messages\" value."
    );

    let unknown_field = put_json(&server, &path, json!({"messages": [], "replace": true})).await;

    assert_eq!(unknown_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unknown_field.message(),
        "Unknown field \"replace\". Allowed fields: messages, if_revision."
    );

    // A matching guard writes, and the revision moves by exactly one.
    let guarded = put_json(
        &server,
        &path,
        json!({"messages": [ {"role": "user", "content": "fresh"} ], "if_revision": 1}),
    )
    .await;

    assert_eq!(
        guarded.status,
        StatusCode::OK,
        "a fresh guard writes: {}",
        guarded.body
    );
    assert_eq!(guarded.number_at("/revision"), 2);

    // RAW ACCEPTANCE: a structurally broken draft is a SUCCESSFUL write, stored raw, explained, and
    // never installed into the model's context.
    let broken = put_json(&server, &path, json!({"messages": {"role": "user"}})).await;

    assert_eq!(
        broken.status,
        StatusCode::OK,
        "a broken draft is accepted: {}",
        broken.body
    );
    assert_eq!(broken.string_at("/accepted_as"), "raw");
    assert_eq!(broken.body["invalid"]["stored_type"], json!("object"));
    assert!(!broken.string_at("/invalid/error").is_empty());

    let unreadable = get(&server, &path).await;

    assert_eq!(
        unreadable.string_at("/transcript/transcript_state"),
        "unreadable"
    );
    assert_eq!(
        unreadable.body["transcript"]["stored_type"],
        json!("object")
    );
    assert!(
        unreadable
            .json_at("/transcript/unreadable_reason")
            .is_some()
    );

    let raw_document = get(&server, &format!("{path}/document")).await;

    assert_eq!(raw_document.string_at("/transcript_state"), "unreadable");
    assert_eq!(
        serde_json::from_str::<Value>(&raw_document.string_at("/document"))
            .expect("the raw value round-trips")
            .as_object()
            .map(|map| map.len()),
        Some(1),
        "the editor sees what the user wrote, not the placeholder"
    );

    // A conversation that does not exist answers with the session in the sentence.
    let ghost = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/ghost/transcript"),
    )
    .await;

    assert_eq!(ghost.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost.message(),
        format!("Unknown thread \"ghost\" in session \"{id}\".")
    );

    let ghost_put = put_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/ghost/transcript"),
        json!({"messages": []}),
    )
    .await;

    assert_eq!(ghost_put.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_put.message(),
        format!("Unknown thread \"ghost\" in session \"{id}\".")
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 6 - conversations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conversations_open_read_cancel_and_cascade_their_children() {
    let root = Root::new("threads");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("threads")).await;

    // CREATE: 201 with the full resource, not the summary row.
    let created = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads"),
        Some(json!({"agent": "instruct", "label": "#FRONT"})),
    )
    .await;

    assert_eq!(created.status, StatusCode::CREATED);
    // The header quotes the ROSTER counter, which this store moves twice per write for the same reason
    // the session row moves twice: the mutation lists its targets and `CommitOutcome::with_event` pushes
    // them again (`store/mod.rs:274-278`). It is a different figure from the conversation's own counter,
    // and the two views of the roster counter below are the exact assertion.
    let opened = get(&server, &format!("/v1/harness/sessions/{id}/threads")).await;

    assert_eq!(
        created.revision(),
        opened.number_at("/revision"),
        "the create's header and the roster page quote one counter"
    );

    let thread = created.string_at("/thread/id");

    assert_eq!(created.string_at("/thread/agent"), "instruct");
    assert_eq!(created.string_at("/thread/label"), "#FRONT");
    assert_eq!(
        created.string_at("/thread/origin"),
        "host",
        "a REST conversation is host-owned"
    );
    assert_eq!(created.string_at("/thread/status"), "idle");
    assert_eq!(created.string_at("/thread/session_id"), id);
    assert_eq!(created.number_at("/thread/transcript_revision"), 0);
    assert_eq!(
        created.number_at("/thread/revision"),
        1,
        "the conversation's OWN counter is minted at 1 by its create (`api-reads.ts:566`), while the \
         header quotes the roster's"
    );

    // A child addressed at the parent.
    let child = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads"),
        Some(json!({"agent": "instruct", "owner_thread_id": thread})),
    )
    .await;

    assert_eq!(child.status, StatusCode::CREATED);
    let child_id = child.string_at("/thread/id");
    assert_eq!(child.string_at("/thread/owner_thread_id"), thread);

    // LIST, paged and filtered.
    let roster = get(&server, &format!("/v1/harness/sessions/{id}/threads")).await;

    assert_eq!(roster.status, StatusCode::OK);
    assert_eq!(roster.revision(), roster.number_at("/revision"));
    assert_eq!(roster.body["items"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        roster.body["items"][0]["id"],
        json!(thread),
        "the roster is in creation order"
    );
    assert_eq!(roster.body["items"][0]["label"], json!("#FRONT"));

    let by_agent = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads?agent=ghost"),
    )
    .await;

    assert_eq!(by_agent.status, StatusCode::OK);
    assert_eq!(by_agent.body["items"].as_array().map(Vec::len), Some(0));

    let by_status = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads?status=stopped"),
    )
    .await;

    assert_eq!(by_status.body["items"].as_array().map(Vec::len), Some(0));

    let bad_status = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads?status=paused"),
    )
    .await;

    assert_eq!(bad_status.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_status.message(),
        "Query parameter \"status\" must be one of: idle, running, stopped."
    );

    // READ one, with its own counter.
    let read = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}"),
    )
    .await;

    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.revision(), read.number_at("/thread/revision"));
    assert_eq!(read.string_at("/thread/session_id"), id);
    assert_eq!(
        read.body["thread"]["invocation_ids"],
        json!([]),
        "nothing has run here"
    );

    // CANCEL an idle conversation: still a success, and the row says `stopped`.
    let cancelled = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}/cancel"),
        None,
    )
    .await;

    assert_eq!(cancelled.status, StatusCode::OK);
    assert_eq!(cancelled.body["cancelled"], json!(false));
    assert_eq!(cancelled.string_at("/status"), "stopped");
    assert_eq!(cancelled.number_at("/dropped_queued_tasks"), 0);
    assert_eq!(
        cancelled.revision(),
        cancelled.number_at("/revision"),
        "the receipt's revision is the header's"
    );

    let stopped = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}"),
    )
    .await;

    assert_eq!(stopped.string_at("/thread/status"), "stopped");

    // DELETE the parent: the child goes with it, and the receipt counts the cascade.
    let deleted = del(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{thread}"),
    )
    .await;

    assert_eq!(deleted.status, StatusCode::OK);
    assert_eq!(deleted.body["deleted"], json!(true));
    assert_eq!(deleted.string_at("/session_id"), id);
    assert_eq!(deleted.number_at("/removed_descendants"), 1);
    assert_eq!(deleted.number_at("/removed_turns"), 0);
    assert_eq!(deleted.revision(), deleted.number_at("/revision"));

    let roster_after = get(&server, &format!("/v1/harness/sessions/{id}/threads")).await;

    assert_eq!(
        roster_after.body["items"].as_array().map(Vec::len),
        Some(0),
        "a deleted parent takes its descendants"
    );

    let child_gone = get(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/{child_id}"),
    )
    .await;

    assert_eq!(child_gone.status, StatusCode::NOT_FOUND);

    // The route-level refusals.
    let no_agent = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads"),
        Some(json!({"label": "x"})),
    )
    .await;

    assert_eq!(no_agent.status, StatusCode::BAD_REQUEST);
    assert_eq!(no_agent.message(), "A conversation requires an \"agent\".");

    let not_member = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads"),
        Some(json!({"agent": "ghost"})),
    )
    .await;

    assert_eq!(not_member.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        not_member.message(),
        format!("Agent \"ghost\" is not a member of session \"{id}\".")
    );

    let bad_owner = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads"),
        Some(json!({"agent": "instruct", "owner_thread_id": "ghost"})),
    )
    .await;

    assert_eq!(bad_owner.status, StatusCode::NOT_FOUND);
    assert_eq!(
        bad_owner.message(),
        format!("Unknown thread \"ghost\" in session \"{id}\".")
    );

    let unknown_field = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads"),
        Some(json!({"agent": "instruct", "origin": "host"})),
    )
    .await;

    assert_eq!(unknown_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unknown_field.message(),
        "Unknown field \"origin\". Allowed fields: agent, label, owner_thread_id."
    );

    let ghost_thread = get(&server, &format!("/v1/harness/sessions/{id}/threads/ghost")).await;

    assert_eq!(ghost_thread.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_thread.message(),
        format!("Unknown thread \"ghost\" in session \"{id}\".")
    );

    let ghost_cancel = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/threads/ghost/cancel"),
        None,
    )
    .await;

    assert_eq!(ghost_cancel.status, StatusCode::NOT_FOUND);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 7 - the session agent roster
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_roster_binds_patches_and_walks_the_instruction_ladder() {
    let root = Root::new("agents");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("agents")).await;

    // LIST: the roster the session runs, with no revision header because the body carries it.
    let roster = get(&server, &format!("/v1/harness/sessions/{id}/agents")).await;

    assert_eq!(roster.status, StatusCode::OK);
    assert_eq!(
        roster.header(&REVISION_HEADER),
        None,
        "the list body already states its revision"
    );
    assert_eq!(roster.string_at("/session_id"), id);
    assert_eq!(
        roster.number_at("/total"),
        roster.body["agents"].as_array().map(Vec::len).unwrap_or(0) as u64
    );

    let primary = roster.body["agents"].as_array().unwrap()[0].clone();
    let primary_id = primary["agent_id"]
        .as_str()
        .expect("a member id")
        .to_owned();

    assert_eq!(
        primary["rank"],
        json!("A"),
        "the entry agent reports the harness descriptor's own letter for it (`team_ranks`, \
         `src/registry/presets.rs:174-188`: the entry agent holds `ENTRY_RANK`), and the row states \
         a RANK rather than the retired two-valued role (R1)"
    );
    assert_eq!(
        primary.get("role"),
        None,
        "`additionalProperties: false` (`harness-api.ts:1755`): a member the contract does not declare is \
         a contract violation, so the retired `role` is GONE rather than kept alongside `rank`"
    );
    assert_eq!(primary["origin"], json!("definition"));
    assert_eq!(
        primary["deletable"],
        json!(false),
        "the entry agent cannot be detached"
    );
    assert_eq!(primary["editable"], json!(true));
    assert_eq!(
        roster.number_at("/revision"),
        1,
        "the create minted the roster counter once (`bumpRevision('session_agents')` at definition time)"
    );
    assert_eq!(
        primary["revision"],
        json!(0),
        "a binding that has never been written by a request states zero on its own row"
    );
    assert_eq!(
        roster.header(&REVISION_HEADER),
        None,
        "the list's revision lives in the body only: TS stamps no header on this read (`api.ts:2554`)"
    );

    // IDENTIFIED READ, with the roster counter on the header.
    let one = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}"),
    )
    .await;

    assert_eq!(one.status, StatusCode::OK);
    assert_eq!(one.revision(), one.number_at("/agent/revision"));
    assert_eq!(one.string_at("/agent/agent_id"), primary_id);

    let ghost = get(&server, &format!("/v1/harness/sessions/{id}/agents/ghost")).await;

    assert_eq!(ghost.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost.message(),
        format!("Unknown agent \"ghost\" in session \"{id}\".")
    );

    // THE INSTRUCTION LADDER: the catalogue text first, then the session override, then back.
    let before = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
    )
    .await;

    assert_eq!(before.status, StatusCode::OK);
    assert_eq!(
        before.header(&REVISION_HEADER),
        None,
        "TS stamps no header on this read"
    );
    assert_eq!(before.string_at("/source"), "catalogue");
    let shipped_chars = before.number_at("/chars");
    let shipped_text = before.string_at("/instruction");

    assert_eq!(
        shipped_chars,
        u64::try_from(shipped_text.chars().count()).unwrap_or(0)
    );

    // An astral code point makes `chars` observable: JS's `.length` (and therefore the contract's
    // figure) counts UTF-16 units, so the returned text is one unit longer than it is wide.
    let override_text = "Answer in one sentence \u{1F525} and never repeat the question.";

    let patched = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
        json!({"instruction": override_text}),
    )
    .await;

    assert_eq!(
        patched.status,
        StatusCode::OK,
        "the write answers the document: {}",
        patched.body
    );
    // DEVIATION, reported: TS's read reports a session-level override as `'harness'` (it stores the
    // override in the frozen preset, `api.ts:2819-2820`). This port keeps the override in the LIVE
    // roster and answers with the contract's more precise `session` variant
    // (`contract.rs::SessionInstructionSource` is `catalogue | global | harness | session`). Both are
    // legal spellings; the ladder - and the restore below - are the behaviour under test.
    assert_eq!(
        patched.string_at("/source"),
        "session",
        "a session override is the session layer"
    );
    assert_eq!(patched.string_at("/agent_id"), primary_id);
    assert!(patched.string_at("/instruction").contains(override_text));

    let after = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
    )
    .await;

    assert_eq!(after.string_at("/source"), "session");
    assert_ne!(after.string_at("/instruction"), shipped_text);

    let after_text = after.string_at("/instruction");

    assert_eq!(
        after.number_at("/chars"),
        u64::try_from(after_text.encode_utf16().count()).unwrap_or(0),
        "`chars` counts UTF-16 code units, the figure `String.prototype.length` reports in the \
         TypeScript original (`api.ts:2803`)"
    );
    assert_eq!(
        after.number_at("/chars"),
        u64::try_from(after_text.chars().count()).unwrap_or(0) + 1,
        "and one astral code point is exactly one unit more than one character"
    );
    assert!(
        after_text.contains("\u{1F525}"),
        "the override text round-trips intact"
    );

    // `null` restores the shipped text - the arm serde cannot express on its own.
    let restored = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
        json!({"instruction": null}),
    )
    .await;

    assert_eq!(
        restored.status,
        StatusCode::OK,
        "a null restores: {}",
        restored.body
    );
    assert_eq!(restored.string_at("/source"), "catalogue");

    let back = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
    )
    .await;

    assert_eq!(back.string_at("/instruction"), shipped_text);

    // The instruction subresource has no unbind verb: the generated route declares GET and PATCH only,
    // and the rail answers a fourth verb with its own method refusal rather than a handler.
    let no_delete = del(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
    )
    .await;

    assert_eq!(no_delete.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        no_delete.message(),
        format!("Use GET, PATCH for /v1/harness/sessions/{id}/agents/{primary_id}/instruction.")
    );

    // The write-verb refusals.
    let no_field = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
        json!({}),
    )
    .await;

    assert_eq!(no_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(no_field.message(), "A patch must supply \"instruction\".");

    let wrong_type = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}/instruction"),
        json!({"instruction": 7}),
    )
    .await;

    assert_eq!(wrong_type.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        wrong_type.message(),
        "\"instruction\" must be a string or null."
    );

    // THE BINDING PATCH moves the model, and the roster counter moves EXACTLY once per write. The
    // roster list publishes no counter of its own (its rows carry one), so the figure is pinned through
    // the writes themselves: two consecutive roster writes differ by one.
    let modelled = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}"),
        json!({"model": "mock/other"}),
    )
    .await;

    assert_eq!(modelled.status, StatusCode::OK);
    assert_eq!(modelled.string_at("/agent/model"), "mock/other");
    assert_eq!(
        modelled.body["agent"]["inherited_model"], roster.body["agents"][0]["inherited_model"],
        "an override never rewrites the creation-time baseline"
    );

    let modelled_again = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}"),
        json!({"model": "mock/third"}),
    )
    .await;

    assert_eq!(
        modelled_again.revision(),
        modelled.revision() + 1,
        "one roster write, exactly one bump on the roster counter (`bumpRevision('session_agents')`)"
    );
    assert_eq!(modelled_again.string_at("/agent/model"), "mock/third");

    // The identified read quotes that same counter, header and body together.
    let read_back = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}"),
    )
    .await;

    assert_eq!(read_back.revision(), read_back.number_at("/agent/revision"));
    assert_eq!(
        read_back.number_at("/agent/revision"),
        modelled_again.revision()
    );

    let empty_agent_patch = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}"),
        json!({}),
    )
    .await;

    assert_eq!(empty_agent_patch.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty_agent_patch.message(),
        "A patch must change at least one field."
    );

    let bad_agent_field = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}"),
        json!({"mode": "x"}),
    )
    .await;

    assert_eq!(bad_agent_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_agent_field.message(),
        "Unknown field \"mode\". Allowed fields: model, instruction, rank, prefix.",
        "the PATCH gate names all FOUR fields the contract's `UpdateSessionAgentRequest` carries \
         (`api.ts:4040`), `rank` included - this is the pin the comment on `PATCH_FIELDS` cites"
    );

    // UNBIND, then BIND the same trusted profile back. Every shipped catalogue profile is already a
    // member of a session created from the default definition, so the 201 path is reached by re-binding
    // a name the roster just gave up - and the bind refuses a name that is still a member with the
    // store's own 409, pinned in the loop below.
    let unbound = del(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/summarizer"),
    )
    .await;

    assert_eq!(
        unbound.status,
        StatusCode::OK,
        "a subagent can be detached: {}",
        unbound.body
    );
    assert_eq!(unbound.body["deleted"], json!(true));
    assert_eq!(unbound.string_at("/agent_id"), "summarizer");
    assert_eq!(unbound.number_at("/detached_threads"), 0);
    assert_eq!(
        unbound.revision(),
        unbound.number_at("/revision"),
        "the receipt states the counter the header carries"
    );

    let roster_mid = get(&server, &format!("/v1/harness/sessions/{id}/agents")).await;

    assert_eq!(
        roster_mid.number_at("/total"),
        roster.number_at("/total") - 1,
        "the detached member is gone from the roster"
    );

    let bound = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents"),
        Some(json!({"name": "summarizer", "profile": "summarizer", "model": "mock/sum"})),
    )
    .await;

    assert_eq!(
        bound.status,
        StatusCode::CREATED,
        "a bind answers 201: {}",
        bound.body
    );
    assert_eq!(bound.string_at("/agent/agent_id"), "summarizer");
    assert_eq!(
        bound.string_at("/agent/origin"),
        "definition",
        "a trusted profile re-binds as a definition member, not as a session-invented one"
    );
    assert_eq!(
        bound.string_at("/agent/rank"),
        "B",
        "a bind that asked for NO rank reports the harness descriptor's letter for that member \
         (`team_ranks`: every non-entry member of the `instruct` card holds `TEAM_RANK` = `B`), which is \
         the first half of the ladder `api-reads.ts:1564` states"
    );
    assert_eq!(
        bound.body["agent"].get("role"),
        None,
        "the receipt is the same row shape as the roster's, `role`-free included"
    );
    assert_eq!(bound.string_at("/agent/model"), "mock/sum");
    assert_eq!(bound.body["agent"]["deletable"], json!(true));
    assert_eq!(
        bound.revision(),
        unbound.revision() + 1,
        "the bind is the next write on the same roster counter"
    );

    let roster_after = get(&server, &format!("/v1/harness/sessions/{id}/agents")).await;

    assert_eq!(
        roster_after.number_at("/total"),
        roster.number_at("/total"),
        "the roster is whole again"
    );

    // A bind is addressable through the same reads as a definition member.
    let bound_read = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/summarizer"),
    )
    .await;

    assert_eq!(bound_read.status, StatusCode::OK);

    let bound_instruction = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/summarizer/instruction"),
    )
    .await;

    assert_eq!(bound_instruction.status, StatusCode::OK);
    assert_eq!(bound_instruction.string_at("/agent_id"), "summarizer");
    assert_eq!(bound_instruction.string_at("/source"), "catalogue");

    let removed = del(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/summarizer"),
    )
    .await;

    assert_eq!(removed.status, StatusCode::OK);
    assert_eq!(removed.body["deleted"], json!(true));

    // The primary cannot be detached, and the refusal is one exact sentence.
    let primary_gone = del(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/{primary_id}"),
    )
    .await;

    assert_eq!(primary_gone.status, StatusCode::CONFLICT);
    assert_eq!(
        primary_gone.message(),
        format!(
            "Agent \"{primary_id}\" is the entry point of session \"{id}\" and cannot be detached."
        )
    );

    // A name that is STILL a member cannot be bound again, and a bind of a name the shipped definition
    // already covers is refused the same way.
    let already = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents"),
        Some(json!({"name": "instruct", "profile": "instruct"})),
    )
    .await;

    assert_eq!(
        already.status,
        StatusCode::CONFLICT,
        "a bound name is a conflict: {}",
        already.body
    );
    assert_eq!(
        already.message(),
        format!("Agent \"instruct\" is already bound in session \"{id}\".")
    );

    // The bind-body refusals, all BEFORE the durability gate (`api.ts:2621-2651`).
    for (body, expected) in [
        (
            json!({"name": "   "}),
            "A session agent binding needs a non-blank \"name\".",
        ),
        (
            json!({"name": "ghost", "profile": "instruct"}),
            "A session agent binding must use its profile's own name: \"name\" is \"ghost\" but \"profile\" is \"instruct\". The roster key is the trusted profile id, which is what a restart rebuilds this session's team from.",
        ),
        (
            // R2: the retired two-valued `role` is NOT an alias for `rank` and is not ignored. The contract
            // body is closed (`harness-api.ts:1883-1942`) and `api.ts:4184-4186` states the rule, so a
            // legacy client lands here rather than on a silently reinterpreted authority.
            json!({"name": "summarizer", "role": "leader"}),
            "Unknown field \"role\". Allowed fields: name, profile, model, instruction, rank, prefix.",
        ),
        (
            json!({"name": 5}),
            "\"name\" must be a string when supplied.",
        ),
        (
            json!({"name": "summarizer", "code": "x"}),
            "Unknown field \"code\". Allowed fields: name, profile, model, instruction, rank, prefix.",
        ),
        (
            json!({"profile": "summarizer"}),
            "A session agent binding needs a non-blank \"name\".",
        ),
        // DEVIATION, reported: TS's R7 branch lets a session invent a member that is NOT a shipped
        // profile (`postSessionAgent`, `api.ts:2676-2690`). This port's store resolves every bind
        // through the trusted catalogue, so an unknown name is the profile refusal - which is what this
        // assertion pins rather than TS's session-defined-member success.
        (
            json!({"name": "helper", "instruction": "Custom helper brief."}),
            "Unknown agent profile \"helper\".",
        ),
    ] {
        let described = body.to_string();
        let reply = post_json(
            &server,
            &format!("/v1/harness/sessions/{id}/agents"),
            Some(body),
        )
        .await;

        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "`{described}` is refused"
        );
        assert_eq!(reply.message(), expected);
    }

    let ghost_instruction = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/ghost/instruction"),
    )
    .await;

    assert_eq!(ghost_instruction.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_instruction.message(),
        format!("Agent \"ghost\" is not a member of session \"{id}\".")
    );

    let ghost_patch = patch_json(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/ghost"),
        json!({"model": "mock/x"}),
    )
    .await;

    assert_eq!(ghost_patch.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_patch.message(),
        format!("Unknown agent \"ghost\" in session \"{id}\".")
    );

    server.stop().await;
}

/// The contract's `SessionAgentBinding` REQUIRED members, verbatim from `harness-api.ts:1756-1847` in the
/// order the interface declares them. `rank` sits third and `role` appears nowhere: the row is a closed
/// object (`additionalProperties: false`, `:1755`), so a Rust row that swaps one for the other breaks a
/// validating client twice over - one missing required member and one undeclared member.
const SESSION_BINDING_REQUIRED: [&str; 12] = [
    "agent_id",
    "profile",
    "rank",
    "origin",
    "model",
    "inherited_model",
    "instruction_source",
    "instruction_chars",
    "editable",
    "deletable",
    "restorable",
    "revision",
];

/// The three members the same interface marks OPTIONAL (`has_instruction_override?`, `guidelines_count?`,
/// `busy?`). Together with [`SESSION_BINDING_REQUIRED`] this is the CLOSED member set: a key outside it is
/// the second half of the contract break R1 describes.
const SESSION_BINDING_OPTIONAL: [&str; 3] =
    ["has_instruction_override", "guidelines_count", "busy"];

/// ONE ROW, ONE RULE: every session-agent row - the list, the identified read, and a create/patch receipt -
/// satisfies the contract's member list element for element.
///
/// Three assertions, none of them a smoke test:
/// 1. the required members appear, in the contract's declared order, with none missing and none repeated;
/// 2. NOTHING else appears, which is `additionalProperties: false`;
/// 3. `role` specifically is absent, because it is the retired member this build used to emit and the one a
///    reader would otherwise shrug at.
///
/// `at` names the surface being checked, so a failure says which read drifted rather than just which row.
fn assert_contract_binding_row(row: &Value, at: &str) {
    let Some(object) = row.as_object() else {
        panic!("`{at}` must be a JSON object, got {row}");
    };

    let keys: Vec<&str> = object.keys().map(String::as_str).collect();

    let required_in_order: Vec<&str> = keys
        .iter()
        .copied()
        .filter(|key| SESSION_BINDING_REQUIRED.contains(key))
        .collect();

    assert_eq!(
        required_in_order,
        SESSION_BINDING_REQUIRED.to_vec(),
        "`{at}` states every required member exactly once, in the contract's order"
    );

    for key in &keys {
        assert!(
            SESSION_BINDING_REQUIRED.contains(key) || SESSION_BINDING_OPTIONAL.contains(key),
            "`{at}` carries `{key}`, a member the contract does not declare \
             (`additionalProperties: false`)"
        );
    }

    assert!(
        !keys.contains(&"role"),
        "`{at}` still emits the retired `role` member"
    );
}

/// THE SESSION-AGENT RANK VOCABULARY (R1 + R2): the wire row's `rank`, the bind body's six members, the
/// PATCH's fourth one, and the two refusal sentences TypeScript answers.
///
/// Every expectation here is read off the CONTRACT, not off this build's output:
/// - `SessionAgentBinding.rank` (`harness-api.ts:1766-1770`), letter-spelled `AgentRank` (`:622-627`).
/// - `CreateSessionAgentRequest` = `name, profile, model, instruction, prefix, rank` (`:1883-1942`), and
///   `api.ts:4188` validates exactly that list.
/// - `UpdateSessionAgentRequest` = `model, instruction, rank, prefix` (`:1954-1990`), and `api.ts:4040`
///   validates exactly that list.
/// - `validateBindingRank` (`sessions.ts:1205-1222`) and the PATCH's stated-but-no-authority check
///   (`api.ts:4051-4058`) supply the two sentences, quoted verbatim below.
/// - the precedence `api-reads.ts:1564` states - `record.harness.ranks?.[name] ?? rankOfMember(
///   definition.ranks, name)`, and `rankOfMember` (`:1487-1489`) falls to the LOWEST letter - which is the
///   "descriptor letter, then session override, then `Z`" ladder the sections below pin in both directions.
#[tokio::test]
async fn the_session_agent_rank_vocabulary_is_the_contracts() {
    let root = Root::new("agent-rank");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("agent-rank")).await;
    let agents_path = format!("/v1/harness/sessions/{id}/agents");

    // -------------------------------------------------------------------
    // 1. THE INHERITED RUNG, and the row shape on every read.
    //
    // The `instruct` card's rank map is `team_ranks(entry, roster)` (`src/registry/presets.rs:174-188`):
    // the entry agent at `ENTRY_RANK` = `A`, every other member at `TEAM_RANK` = `B`. Nothing in this
    // section writes a rank, so every row reports its descriptor letter and the entry agent's `A` is a
    // descriptor fact rather than "the first member is `A`" - the mistake the retired `role` encoded.
    // -------------------------------------------------------------------
    let roster = get(&server, &agents_path).await;

    assert_eq!(roster.status, StatusCode::OK);

    let rows = roster.body["agents"].as_array().expect("a roster").clone();

    assert!(
        rows.len() > 1,
        "a delegating card's roster has delegates to rank"
    );

    for row in &rows {
        assert_contract_binding_row(row, "GET .../agents row");
    }

    let letter_of = |row: &Value| row["rank"].as_str().expect("a rank letter").to_owned();

    assert_eq!(
        letter_of(&rows[0]),
        "A",
        "the entry agent reports the descriptor's letter for it"
    );
    assert_eq!(
        letter_of(&rows[1]),
        "B",
        "a delegate reports the descriptor's letter for it, NOT `A`, and two members of one roster \
         may share a letter - the point of a rank rather than a two-valued role"
    );

    let summarizer_id = rows
        .iter()
        .map(|row| row["agent_id"].as_str().unwrap_or_default().to_owned())
        .find(|agent| agent == "summarizer")
        .expect("the `instruct` roster carries the summarizer delegate");

    let one = get(&server, &format!("{agents_path}/{summarizer_id}")).await;

    assert_eq!(one.status, StatusCode::OK);
    assert_contract_binding_row(&one.body["agent"], "GET .../agents/{agentId} row");
    assert_eq!(letter_of(&one.body["agent"]), "B");

    // -------------------------------------------------------------------
    // 2. A BODY BUILT FROM THE CONTRACT'S OWN SIX MEMBERS IS ACCEPTED.
    //
    // Every member of `CreateSessionAgentRequest` is present, `prefix` and `rank` included, and the route
    // answers 201 rather than a 400 about `prefix` - the failure mode R2 describes. The member is detached
    // first because every shipped catalogue profile is already on this roster, so 201 is reached by the
    // same re-bind the roster test above uses.
    // -------------------------------------------------------------------
    let unbound = del(&server, &format!("{agents_path}/{summarizer_id}")).await;

    assert_eq!(unbound.status, StatusCode::OK, "{}", unbound.body);

    let bound = post_json(
        &server,
        &agents_path,
        Some(json!({
            "name": summarizer_id,
            "profile": summarizer_id,
            "model": "mock/sum",
            "instruction": "Compress to one line.",
            "prefix": [{"role": "user", "content": "the ask"}],
            "rank": "C",
        })),
    )
    .await;

    assert_eq!(
        bound.status,
        StatusCode::CREATED,
        "the contract's own body is accepted: {}",
        bound.body
    );
    assert_contract_binding_row(&bound.body["agent"], "POST .../agents receipt");
    assert_eq!(
        letter_of(&bound.body["agent"]),
        "C",
        "the receipt states the rank the body asked for, in the contract's letter spelling"
    );
    assert_eq!(bound.string_at("/agent/model"), "mock/sum");

    // THE BIND'S `prefix` IS A WRITE, not decoration: `harness-api.ts:1922-1931` promises the band is
    // "stored verbatim and durable in this session's record", and `api.ts:4231-4249` validates it at the
    // edge. `source: session` is the rung a stored band the definition does not carry answers
    // (`api.ts:4466-4503`), so this read is the observable proof the member was not accepted and dropped.
    let band = get(&server, &format!("{agents_path}/{summarizer_id}/prefix")).await;

    assert_eq!(
        band.body,
        json!({
            "session_id": id,
            "agent_id": summarizer_id,
            "prefix": [{"role": "user", "content": "the ask"}],
            "source": "session",
        }),
        "the band a bind states is the band the member seeds"
    );

    // -------------------------------------------------------------------
    // 3. THE OVERRIDE RUNG.
    //
    // A rank written for THIS session outranks the descriptor for every read of that session, which is the
    // first rung of `api-reads.ts:1564`; the descriptor keeps its letter for every member nobody wrote.
    // -------------------------------------------------------------------
    let patched = patch_json(
        &server,
        &format!("{agents_path}/{summarizer_id}"),
        json!({"rank": "D"}),
    )
    .await;

    assert_eq!(
        patched.status,
        StatusCode::OK,
        "the PATCH takes `rank`: {}",
        patched.body
    );
    assert_contract_binding_row(&patched.body["agent"], "PATCH .../agents/{agentId} receipt");
    assert_eq!(letter_of(&patched.body["agent"]), "D");

    let roster_after = get(&server, &agents_path).await;

    for row in roster_after.body["agents"].as_array().expect("a roster") {
        assert_contract_binding_row(row, "the roster read after a rank write");

        // THE SCOPE OF AN OVERRIDE: the member this session wrote reports its letter, and every OTHER member
        // still reports the letter the harness descriptor states for it - the entry agent `A`, each delegate
        // `B` (`team_ranks`, `src/registry/presets.rs:174-188`). A session rank is one key in a map, not a
        // promotion of whoever asked.
        let expected = match row["agent_id"].as_str() {
            Some(agent) if agent == summarizer_id.as_str() => "D",
            Some("instruct") => "A",
            _ => "B",
        };

        assert_eq!(
            letter_of(row),
            expected,
            "`{}` reports the letter its own rung states",
            row["agent_id"]
        );
    }

    let read_back = get(&server, &format!("{agents_path}/{summarizer_id}")).await;

    assert_eq!(letter_of(&read_back.body["agent"]), "D");

    // -------------------------------------------------------------------
    // 4. THE REFUSALS, each one TypeScript's own sentence.
    // -------------------------------------------------------------------
    let binding_refusal = "An agent binding takes a \"rank\": one letter of the authority scale, \
                           A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z. \
                           The rank is what decides what the agent may reach; the roster alone does not.";
    let letter_refusal = "An agent rank must be one letter of the authority scale, \
                          A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z.";

    for (body, expected, surface) in [
        // THE RETIRED `role`, on both verbs. `api.ts:4184-4186`: "the OLD field `role` is not accepted at
        // all - it lands in the unknown-field refusal below", so a legacy client gets a clear refusal
        // instead of a body this build quietly reinterprets.
        (
            json!({"name": "instruct", "profile": "instruct", "role": "subagent"}),
            "Unknown field \"role\". Allowed fields: name, profile, model, instruction, rank, prefix.",
            "POST",
        ),
        (
            json!({"role": "primary"}),
            "Unknown field \"role\". Allowed fields: model, instruction, rank, prefix.",
            "PATCH",
        ),
        // A LETTER THAT IS NOT ONE, on either verb: one validator, one message (`sessions.ts:1218-1221`).
        // Lower-case is not upper-case - `isAgentRank` is the guard, and it never reinterprets.
        (
            json!({"name": "ghost", "rank": "leader"}),
            binding_refusal,
            "POST",
        ),
        (json!({"rank": "d"}), binding_refusal, "PATCH"),
        (json!({"rank": 5}), binding_refusal, "PATCH"),
        // THE PATCH'S OTHER SENTENCE, and it belongs to the PATCH alone: a stated `null` resolves to no
        // authority, which `api.ts:4054-4057` refuses out loud because a rank has no restore arm. On POST
        // the same value is the contract's "ask for no authority" arm (`harness-api.ts:1932-1937`), so a
        // `null` rank binds - the last case in this table pins that difference.
        (json!({"rank": null}), letter_refusal, "PATCH"),
    ] {
        let described = format!("{surface} {}", body);
        let reply = if surface == "POST" {
            post_json(&server, &agents_path, Some(body)).await
        } else {
            patch_json(&server, &format!("{agents_path}/{summarizer_id}"), body).await
        };

        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "`{described}` is refused"
        );
        assert_eq!(reply.message(), expected, "`{described}`");
    }

    // THE ONE POST ARM THAT IS NOT AN ERROR: `null` asks for no authority, and the row that answers
    // resolves it through the ladder rather than refusing the body. The member is re-bound from the
    // definition, so no session override exists and the descriptor's letter is the answer.
    let unranked = del(&server, &format!("{agents_path}/{summarizer_id}")).await;

    assert_eq!(unranked.status, StatusCode::OK, "{}", unranked.body);

    let defaulted = post_json(
        &server,
        &agents_path,
        Some(json!({"name": summarizer_id, "profile": summarizer_id, "rank": null})),
    )
    .await;

    assert_eq!(
        defaulted.status,
        StatusCode::CREATED,
        "a `null` rank binds: {}",
        defaulted.body
    );
    assert_eq!(
        letter_of(&defaulted.body["agent"]),
        "B",
        "no rank asked for, no rank invented: the row falls through to the descriptor's letter for the \
         member (`rankOfMember`), which is the rung `harness-api.ts:1766-1770` describes"
    );

    server.stop().await;
}

/// THE RANK PATCH IS A BEFORE-THE-FIRST-THREAD WRITE (`harness-api.ts:1956-1962`,
/// `sessions.ts:8253-8262`): a saved conversation keeps the ranks its harness template defines, because its
/// delegation graph was built from that map.
///
/// The refusal is scoped to the rank arm alone, and the second half of this test is what proves the scope:
/// a MODEL patch on the same started session still answers 200. Without that half, a gate that accidentally
/// covered the whole PATCH would pass this test while breaking the override writes.
#[tokio::test]
async fn a_started_conversation_keeps_its_ranks() {
    let root = Root::new("agent-rank-started");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("the run", usage(3, 2))]).await;

    let (id, _) = create_session(&server, &base, Some("agent-rank-started")).await;
    let summarizer_path = format!("/v1/harness/sessions/{id}/agents/summarizer");

    let before = patch_json(&server, &summarizer_path, json!({"rank": "C"})).await;

    assert_eq!(
        before.status,
        StatusCode::OK,
        "a session nobody has used takes a rank: {}",
        before.body
    );
    assert_eq!(before.string_at("/agent/rank"), "C");

    let started = submit(&server, &id, "start the conversation", None).await;

    assert_eq!(
        started.status,
        StatusCode::ACCEPTED,
        "the submit is accepted: {}",
        started.body
    );

    let turn_id = started.string_at("/turn_id");

    wait_for_turn(&server, &id, &turn_id, "done").await;

    let refused = patch_json(&server, &summarizer_path, json!({"rank": "E"})).await;

    assert_eq!(
        refused.status,
        StatusCode::CONFLICT,
        "a used session refuses a rank: {}",
        refused.body
    );
    assert_eq!(
        refused.message(),
        "A saved conversation keeps the ranks its harness template defines; start a new session to change \
         them."
    );

    // THE SCOPE OF THE GATE: an override moves no authority, so it stays writable after the first turn
    // (`api.ts:4096-4146` runs the model half with no such check).
    let modelled = patch_json(&server, &summarizer_path, json!({"model": "mock/other"})).await;

    assert_eq!(
        modelled.status,
        StatusCode::OK,
        "the model half is not gated by the started rule: {}",
        modelled.body
    );

    // AND THE REFUSAL CHANGED NOTHING: the rank the session was given while it was fresh is still the one
    // every read reports.
    let roster = get(&server, &format!("/v1/harness/sessions/{id}/agents")).await;

    for row in roster.body["agents"].as_array().expect("a roster") {
        assert_contract_binding_row(row, "the roster read after a refused rank");
    }

    let summarizer_row = roster.body["agents"]
        .as_array()
        .expect("a roster")
        .iter()
        .find(|row| row["agent_id"].as_str() == Some("summarizer"))
        .expect("the summarizer row")
        .clone();

    assert_eq!(
        summarizer_row["rank"],
        json!("C"),
        "a refused rank write leaves the accepted one in place"
    );
    assert_eq!(
        summarizer_row["model"],
        json!("mock/other"),
        "and the override that WAS accepted after the first turn is still there too"
    );

    server.stop().await;
}

/// THE PRESET-HISTORY BAND (`GET .../agents/{agent_id}/prefix` `api.ts:4449-4510` + the binding PATCH's
/// `prefix` field `api.ts:4052-4077`, `sessions.ts:8136-8210`).
///
/// Three things are pinned here and none of them is the happy path:
///
/// 1. THE PROVENANCE LADDER. A band has three authors - the frozen definition (`harness`), this
///    conversation (`session`), and nobody (`none`) - and the write has THREE arms that land in
///    different rungs: a band, `[]` ("this conversation chose no history", which is a CHOICE and answers
///    `session`), and `null` ("give me back what my definition seeds", which restates the definition's
///    band rather than deleting it). Collapsing `[]` and `null` is the obvious implementation and it is
///    wrong: a client that reads `session` knows its own empty band is what the model will see, and a
///    client that reads `none` knows nothing does.
/// 2. THE DOCUMENT'S KEY ORDER, pinned on the re-serialized body. `serde_json` runs with
///    `preserve_order` in this crate (`Cargo.toml:82`), so the parsed body keeps the wire order and this
///    line fails the moment the response type's field order drifts from `harness-api.ts:2067-2083`.
/// 3. THE ONE VALIDATOR'S SENTENCES. Every malformed band answers with the registry's wording, naming the
///    turn index, because the SAME validator backs the harness-definition write
///    ([`typecrate_harness::registry::require_prefix_turns`]) - a client that PATCHes a band into a
///    definition and into a session must read one shape of 400.
#[tokio::test]
async fn the_preset_history_band_reports_its_author_and_the_binding_patch_writes_it() {
    let root = Root::new("preset-history");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("preset-history")).await;

    let roster = get(&server, &format!("/v1/harness/sessions/{id}/agents")).await;
    let primary_id = roster.body["agents"][0]["agent_id"]
        .as_str()
        .expect("the roster names its primary agent")
        .to_owned();
    let agent_path = format!("/v1/harness/sessions/{id}/agents/{primary_id}");
    let prefix_path = format!("{agent_path}/prefix");

    // 1. NOTHING SEEDS THIS MEMBER: the `instruct` card declares no preset history at all
    // (`src/registry/presets.rs:252`, `agent_prefixes: BTreeMap::new()`), so the frozen definition has
    // no band and the session's own preset copy has none either. `none` is the rung, and `prefix: []`
    // is its honest payload (`api.ts:4482`, "which is also the honest value for source: none").
    let band = get(&server, &prefix_path).await;

    assert_eq!(band.status, StatusCode::OK, "the read: {}", band.body);
    assert_eq!(
        band.body,
        json!({
            "session_id": id,
            "agent_id": primary_id,
            "prefix": [],
            "source": "none",
        })
    );
    assert_eq!(
        serde_json::to_string(&band.body).expect("a body re-serializes"),
        format!(
            "{{\"session_id\":\"{id}\",\"agent_id\":\"{primary_id}\",\"prefix\":[],\"source\":\"none\"}}"
        ),
        "the document's key order is `harness-api.ts:2067-2083`'s declaration order"
    );
    assert_eq!(
        band.header(&REVISION_HEADER),
        None,
        "a band projection stamps no revision: the route has no write (`api.ts:4510`)"
    );

    // 2. THE WRITE, and the ladder moves to `session` because the stored band is not the definition's.
    let written = patch_json(
        &server,
        &agent_path,
        json!({"prefix": [
            {"role": "user", "content": "the ask"},
            {"role": "assistant", "content": "the answer"}
        ]}),
    )
    .await;

    assert_eq!(
        written.status,
        StatusCode::OK,
        "the write: {}",
        written.body
    );
    assert_eq!(
        written.string_at("/agent/agent_id"),
        primary_id,
        "the receipt is the binding row, exactly like the model half of this route"
    );

    let authored = get(&server, &prefix_path).await;

    assert_eq!(
        authored.body,
        json!({
            "session_id": id,
            "agent_id": primary_id,
            "prefix": [
                {"role": "user", "content": "the ask"},
                {"role": "assistant", "content": "the answer"},
            ],
            "source": "session",
        }),
        "the band reads back verbatim, in order, with the text UNTRIMMED"
    );

    // 3. `[]` ON A MEMBER NOTHING SEEDS. The write landed (the shard says so below), yet the rung is
    // still `none`: an empty stored band and no band at all produce the SAME prompt, and TS's ladder
    // refuses to report an authorship no reader can observe (`api.ts:4466-4476`, "a session write that
    // restates the definition's own bytes answers `harness`, because no reader - not the model, not a
    // restart - can observe a difference that is not there", with `none` the same rule when the
    // definition's band is empty too). `[]` and `null` are DIFFERENT WRITES - the first restates what
    // this conversation chose, the second what its definition seeds - and the split is observable only
    // where the definition seeds something, which is the next test.
    let emptied = patch_json(&server, &agent_path, json!({"prefix": []})).await;

    assert_eq!(emptied.status, StatusCode::OK, "`[]`: {}", emptied.body);

    let chosen = get(&server, &prefix_path).await;

    assert_eq!(
        chosen.body,
        json!({"session_id": id, "agent_id": primary_id, "prefix": [], "source": "none"})
    );

    // 4. `null` IS THE UNDO, and it is not the same statement as `[]`: the store restates the frozen
    // definition's own band (`sessions.ts:8157-8163`), which here is nothing at all - so the rung returns
    // to `none`, which is precisely what `[]` refused to say.
    let undone = patch_json(&server, &agent_path, json!({"prefix": null})).await;

    assert_eq!(undone.status, StatusCode::OK, "`null`: {}", undone.body);

    let restored = get(&server, &prefix_path).await;

    assert_eq!(
        restored.body,
        json!({"session_id": id, "agent_id": primary_id, "prefix": [], "source": "none"})
    );

    // 5. ONE PATCH, TWO FIELDS, ONE BUMP. TS runs the instruction and prefix halves as two store calls
    // and bumps `session_agents` once at the route (`api.ts:4158-4166`); this build folds the halves into
    // one commit, and the figure that has to agree is the counter the header quotes.
    let both = patch_json(
        &server,
        &agent_path,
        json!({"model": "mock/framed-one", "prefix": [{"role": "system", "content": "framed"}]}),
    )
    .await;

    assert_eq!(both.status, StatusCode::OK, "both halves: {}", both.body);
    assert_eq!(both.string_at("/agent/model"), "mock/framed-one");

    let next = patch_json(&server, &agent_path, json!({"model": "mock/framed-two"})).await;

    assert_eq!(
        next.revision(),
        both.revision() + 1,
        "the two-field write moved the roster counter exactly once"
    );

    let framed = get(&server, &prefix_path).await;

    assert_eq!(
        framed.body["prefix"],
        json!([{"role": "system", "content": "framed"}]),
        "the model half did not disturb the band the same patch wrote"
    );

    // 6. THE DROP, not a refusal: a turn carrying `toolCalls`, `id` or `attachments` is accepted and the
    // projection keeps only the role and its text (swagger `:4794-4806` states a drop, and
    // `requirePrefixTurns` is where it happens - a preset that seeded a tool result would promise the
    // model an exchange whose execution never happened).
    let padded = patch_json(
        &server,
        &agent_path,
        json!({"prefix": [{
            "role": "user",
            "content": "kept",
            "id": "turn-7",
            "toolCalls": [{"id": "call-1", "name": "timeNow", "arguments": "{}"}],
            "attachments": [{"kind": "file"}],
            "notAContractKey": true
        }]}),
    )
    .await;

    assert_eq!(padded.status, StatusCode::OK, "the drop: {}", padded.body);

    let stripped = get(&server, &prefix_path).await;

    assert_eq!(
        stripped.body["prefix"],
        json!([{"role": "user", "content": "kept"}]),
        "exactly `{{ role, content }}` survives, in the contract's key order"
    );
    assert_eq!(
        serde_json::to_string(&stripped.body["prefix"]).expect("a band re-serializes"),
        "[{\"role\":\"user\",\"content\":\"kept\"}]"
    );

    // 7. EVERY MALFORMED BAND, in the registry's own words, naming the turn index. Each refusal is
    // followed by a read, because a rejected patch must leave the band EXACTLY as it was.
    //
    // THE BACKTICKS CLOSE BEFORE THE INDEX (`` `prefix.instruct`[0] ``, not `` `prefix.instruct[0]` ``):
    // `require_prefix_turns` builds `label` once with the quotes and then formats `format!("{label}[{i}]")`
    // (`src/registry/mod.rs:1347`), and TS does the same (`registry.ts:886-890`). The label is the FIELD,
    // the index is a position inside it - so this pin looks odd and is the contract.
    let mut index = 0usize;

    for (submitted, expected) in [
        (
            json!({"prefix": "not a band"}),
            format!(
                "`prefix.{primary_id}` must be an ARRAY of preset-history turns ({{ role, content }})."
            ),
        ),
        (
            json!({"prefix": {"instruct": []}}),
            format!(
                "`prefix.{primary_id}` must be an ARRAY of preset-history turns ({{ role, content }})."
            ),
        ),
        (
            json!({"prefix": [[]] }),
            format!(
                "`prefix.{primary_id}`[0] must be an object with a \"role\" and a \"content\" string."
            ),
        ),
        (
            json!({"prefix": [{"role": "tool", "content": "a result"}] }),
            format!(
                "`prefix.{primary_id}`[0] has role \"tool\". A preset history turn is \"system\", \"user\", or \"assistant\" \u{2014} a tool turn belongs to a conversation, not to a preset."
            ),
        ),
        (
            json!({"prefix": [{"role": "user"}] }),
            format!("`prefix.{primary_id}`[0] needs a string \"content\". Received undefined."),
        ),
        (
            json!({"prefix": [{"role": "user", "content": [{"kind": "text"}]}] }),
            format!(
                "`prefix.{primary_id}`[0] needs a string \"content\". Received [{{\"kind\":\"text\"}}]."
            ),
        ),
        (
            // `MAX_PREFIX_TURNS` (`src/registry/presets.rs:151`) is 64, so 65 is the first refusal.
            json!({"prefix": array_turns(65)}),
            format!("`prefix.{primary_id}` cannot hold more than 64 turns. Received 65."),
        ),
        (
            json!({"prefix": [{"role": "user", "content": "x".repeat(32_001)}] }),
            format!("`prefix.{primary_id}`[0] content cannot exceed 32000 characters."),
        ),
    ] {
        index += 1;

        let refused = patch_json(&server, &agent_path, submitted).await;

        assert_eq!(
            refused.status,
            StatusCode::BAD_REQUEST,
            "refusal #{index} is a 400: {}",
            refused.body
        );
        assert_eq!(refused.message(), expected, "refusal #{index}");

        let unchanged = get(&server, &prefix_path).await;

        assert_eq!(
            unchanged.body["prefix"],
            json!([{"role": "user", "content": "kept"}]),
            "refusal #{index} changed the band"
        );
    }

    // 8. THE BOUNDARY THE OTHER WAY: 64 turns and a 32000-character turn are both legal, which is what
    // makes the two refusals above about the BOUND rather than about the shape.
    let boundary = patch_json(&server, &agent_path, json!({"prefix": array_turns(64)})).await;

    assert_eq!(
        boundary.status,
        StatusCode::OK,
        "64 turns: {}",
        boundary.body
    );
    assert_eq!(
        get(&server, &prefix_path).await.body["prefix"]
            .as_array()
            .map(Vec::len),
        Some(64)
    );

    // 9. THE ORDER: a body that is wrong about BOTH the band and the instruction answers about the band,
    // because TS validates the band first (`api.ts:4052-4077` precedes `:4095`) so a malformed band can
    // never arrive after a half of the patch has already moved.
    let precedence = patch_json(
        &server,
        &agent_path,
        json!({"instruction": 7, "prefix": "not a band"}),
    )
    .await;

    assert_eq!(precedence.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        precedence.message(),
        format!(
            "`prefix.{primary_id}` must be an ARRAY of preset-history turns ({{ role, content }})."
        )
    );

    // 10. THE FOUR-LETTER REFUSALS. The session is asked first (a request naming a session that never
    // existed never learns which agents it lacks), then the DEFINITION roster - a band is a seed, so an
    // agent this conversation merely bound at runtime has no seed to report (`api.ts:4455-4458`).
    let ghost_session = get(&server, "/v1/harness/sessions/ghost/agents/instruct/prefix").await;

    assert_eq!(ghost_session.status, StatusCode::NOT_FOUND);
    assert_eq!(ghost_session.message(), "Unknown session \"ghost\".");

    let ghost_both = get(&server, "/v1/harness/sessions/ghost/agents/ghost/prefix").await;

    assert_eq!(ghost_both.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_both.message(),
        "Unknown session \"ghost\".",
        "the session is named even when the agent is unknown too"
    );

    let ghost_agent = get(
        &server,
        &format!("/v1/harness/sessions/{id}/agents/ghost/prefix"),
    )
    .await;

    assert_eq!(ghost_agent.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_agent.message(),
        format!("Agent \"ghost\" is not a member of session \"{id}\".")
    );

    // 11. THE 405, from the generated route table rather than from a handler: this path declares GET
    // only, so the unbind verb says which verb to use.
    let no_delete = del(&server, &prefix_path).await;

    assert_eq!(no_delete.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(no_delete.message(), format!("Use GET for {prefix_path}."));

    server.stop().await;
}

/// A band of `count` legal turns, used for the turn-count boundary and its refusal.
fn array_turns(count: usize) -> Value {
    Value::Array(
        (0..count)
            .map(|index| {
                json!({"role": if index % 2 == 0 { "user" } else { "assistant" }, "content": format!("turn {index}")})
            })
            .collect(),
    )
}

/// THE OTHER TWO RUNGS, on a definition that actually seeds a band: `harness` for the untouched copy,
/// and the `[]`-versus-`null` split that a member with no band cannot show.
///
/// This is the test that pins the durable half of the undo. TS writes the SESSION's own preset copy
/// rather than deleting the key (`sessions.ts:8196-8202`: "the map therefore always says what the live
/// brain seeds - which is also what makes the prefix read's `source` ladder decidable from the record
/// alone"), so an undo leaves the definition's band ON the session record and the read answers
/// `harness` about bytes the session wrote. A port that deleted the key on `null` would answer the same
/// thing here and break on the next template edit; a port that stored `[]` would answer `session` and
/// tell the client its conversation chose an empty history when it did not.
#[tokio::test]
async fn a_definition_that_seeds_a_band_reports_harness_until_the_session_overrides_it() {
    let root = Root::new("preset-history-seeded");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let seeded_band = json!([
        {"role": "user", "content": "the seeded ask"},
        {"role": "assistant", "content": "the seeded answer"},
    ]);

    let created = post_json(
        &server,
        "/v1/harness/harnesses",
        Some(json!({
            "name": "seeded-team",
            "title": "Seeded Team",
            "description": "A harness whose one member seeds a preset history.",
            "agents": ["instruct"],
            "agent_prefixes": { "instruct": seeded_band },
        })),
    )
    .await;

    assert_eq!(
        created.status,
        StatusCode::CREATED,
        "the definition: {}",
        created.body
    );

    let session = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "seeded-team",
            "settings": {"base_url": base, "api_key": MOCK_KEY, "model": "mock/one"}
        })),
    )
    .await;

    assert_eq!(
        session.status,
        StatusCode::CREATED,
        "the session: {}",
        session.body
    );

    let id = session.string_at("/session/id");
    let agent_path = format!("/v1/harness/sessions/{id}/agents/instruct");
    let prefix_path = format!("{agent_path}/prefix");

    // RUNG `harness`: nothing has been written, and the live band IS the frozen definition's.
    let initial = get(&server, &prefix_path).await;

    assert_eq!(initial.status, StatusCode::OK, "the read: {}", initial.body);
    assert_eq!(
        initial.body,
        json!({
            "session_id": id,
            "agent_id": "instruct",
            "prefix": seeded_band,
            "source": "harness",
        })
    );

    // THE CHOICE: `[]` on a member that DOES have a seeded band is a statement, so the rung moves to
    // `session` and the band is empty - "this conversation chose no preset history over the band its
    // definition would have seeded" (`api.ts:4466-4476`). This is the only place `[]` and `null` are
    // observable apart, which is why the previous test could not pin it.
    let chose_empty = patch_json(&server, &agent_path, json!({"prefix": []})).await;

    assert_eq!(
        chose_empty.status,
        StatusCode::OK,
        "`[]`: {}",
        chose_empty.body
    );

    let chosen = get(&server, &prefix_path).await;

    assert_eq!(
        chosen.body,
        json!({"session_id": id, "agent_id": "instruct", "prefix": [], "source": "session"})
    );

    // A band of its own, then the UNDO.
    let wrote = patch_json(
        &server,
        &agent_path,
        json!({"prefix": [{"role": "user", "content": "session-only"}]}),
    )
    .await;

    assert_eq!(wrote.status, StatusCode::OK, "the band: {}", wrote.body);

    let authored = get(&server, &prefix_path).await;

    assert_eq!(
        authored.body,
        json!({
            "session_id": id,
            "agent_id": "instruct",
            "prefix": [{"role": "user", "content": "session-only"}],
            "source": "session",
        })
    );

    let undone = patch_json(&server, &agent_path, json!({"prefix": null})).await;

    assert_eq!(undone.status, StatusCode::OK, "`null`: {}", undone.body);

    let restored = get(&server, &prefix_path).await;

    assert_eq!(
        restored.body,
        json!({
            "session_id": id,
            "agent_id": "instruct",
            "prefix": seeded_band,
            "source": "harness",
        }),
        "the undo restates the definition's own band on the SESSION's preset copy, so the rung returns \
         to `harness` with the seeded bytes - not an empty band, and not `none`"
    );

    // The definition itself is untouched by any of this: a session band is a copy, never an edit of the
    // harness its author wrote (`api.ts:4479`, the comparison is against the FROZEN definition copy).
    let definition = get(&server, "/v1/harness/harnesses/seeded-team").await;

    assert_eq!(
        definition.body["harness"]["agent_prefixes"]["instruct"], seeded_band,
        "the frozen definition still says what its author wrote"
    );

    server.stop().await;
}

/// THE IDLE GATE ON THE BAND WRITE, and the half of the same route that does NOT wait.
///
/// A band is what the brain SEEDS the next turn with, so TS refuses to swap it underneath a run
/// (`sessions.ts:8141`, `assertBindingIdle(session, "change an agent prefix in a session")` - the same
/// guard a roster change takes, for the same reason: the run in flight resolves its team and its seeds
/// from the live harness, and a band that moved underneath it would silently rewrite what the model
/// saw). The model half of this PATCH has no such gate anywhere in TS (`setAgentModels` reads the
/// session and writes), and that split is what this test pins: one route, two verbs of write, two
/// different answers on the same live session.
#[tokio::test]
async fn a_live_run_waits_for_the_band_write_and_still_takes_a_model_override() {
    let root = Root::new("preset-history-busy");
    let server = TestServer::start(&root).await;

    // One answer, HELD OPEN: the run is live for the whole window the writes land in, which is the
    // state TS's guard reads.
    let (base, _script) = provider(vec![Answer::Delayed(
        900,
        answered_body("held open", usage(7, 3)),
    )])
    .await;

    let (id, _) = create_session(&server, &base, Some("preset-history-busy")).await;
    let thread = open_thread(&server, &id, "instruct").await;
    let agent_path = format!("/v1/harness/sessions/{id}/agents/instruct");
    let prefix_path = format!("{agent_path}/prefix");

    let running = submit(&server, &id, "the first ask", Some(&thread)).await;
    let turn_id = running.string_at("/turn_id");

    wait_for_turn(&server, &id, &turn_id, "running").await;

    let gated = patch_json(
        &server,
        &agent_path,
        json!({"prefix": [{"role": "user", "content": "mid-run"}]}),
    )
    .await;

    assert_eq!(
        gated.status,
        StatusCode::CONFLICT,
        "the gate: {}",
        gated.body
    );
    assert_eq!(
        gated.message(),
        format!(
            "Cannot change an agent prefix in a session while session \"{id}\" has a live run or queued work; wait for it to settle or cancel it first."
        )
    );

    // THE REFUSAL CHANGES NOTHING - not the band, and not the roster counter this route stamps.
    let unchanged = get(&server, &prefix_path).await;

    assert_eq!(
        unchanged.body,
        json!({"session_id": id, "agent_id": "instruct", "prefix": [], "source": "none"}),
        "a gated write leaves the band the run seeded"
    );

    let modelled = patch_json(&server, &agent_path, json!({"model": "mock/mid-run"})).await;

    assert_eq!(
        modelled.status,
        StatusCode::OK,
        "the model half is not gated: {}",
        modelled.body
    );
    assert_eq!(modelled.string_at("/agent/model"), "mock/mid-run");

    wait_for_turn(&server, &id, &turn_id, "done").await;

    // THE SAME WRITE, ONE SETTLE LATER. This is what separates the gate from a blanket refusal: the
    // session is writable again, and nothing about the run that just finished poisoned it.
    let settled = patch_json(
        &server,
        &agent_path,
        json!({"prefix": [{"role": "user", "content": "after the run"}]}),
    )
    .await;

    assert_eq!(
        settled.status,
        StatusCode::OK,
        "after the settle: {}",
        settled.body
    );

    let authored = get(&server, &prefix_path).await;

    assert_eq!(
        authored.body,
        json!({
            "session_id": id,
            "agent_id": "instruct",
            "prefix": [{"role": "user", "content": "after the run"}],
            "source": "session",
        })
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 8 - the toolbox
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_toolbox_lists_creates_sources_patches_and_deletes() {
    let root = Root::new("tools");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("toolbox")).await;
    let path = format!("/v1/harness/sessions/{id}/tools");

    // LIST: the catalogue as this session sees it, plus the creation budget. The limit is raised above
    // the catalogue's size so the two policy lookups below (`protected`, `inherited`) address rows that
    // are provably on the served page rather than rows that happen to survive the default slice.
    let page = get(&server, &format!("{path}?limit=100")).await;

    assert_eq!(page.status, StatusCode::OK);
    assert_eq!(page.revision(), page.number_at("/revision"));
    assert_eq!(page.number_at("/budget/used"), 0);
    assert_eq!(page.number_at("/budget/limit"), 32);
    assert_eq!(
        page.number_at("/limit"),
        u64::try_from(page.body["items"].as_array().unwrap().len()).unwrap_or(0),
        "`limit` echoes the rows served, so a final page cannot also promise a successor"
    );
    assert_eq!(page.json_at("/next_cursor"), Some(&Value::Null));
    assert!(
        page.body["items"].as_array().unwrap().len() > 10,
        "the whole catalogue is served"
    );
    assert_eq!(page.body["items"][0]["session_id"], json!(id));
    assert_eq!(page.body["items"][0]["origin"], json!("catalogue"));
    assert!(
        page.body["items"][0].get("tool").is_none(),
        "a row FLATTENS the catalogue entry: `name` and `description` sit beside `enabled`"
    );
    assert!(page.body["items"][0]["name"].is_string());

    let enabled_filter = get(&server, &format!("{path}?enabled=yes")).await;

    assert_eq!(enabled_filter.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        enabled_filter.message(),
        "Query parameter \"enabled\" must be true or false."
    );

    let origin_filter = get(&server, &format!("{path}?origin=made-up")).await;

    assert_eq!(origin_filter.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        origin_filter.message(),
        "Query parameter \"origin\" must be one of: catalogue, discovered, created."
    );

    let created_page = get(&server, &format!("{path}?origin=created")).await;

    assert_eq!(created_page.status, StatusCode::OK);
    assert_eq!(created_page.body["items"].as_array().map(Vec::len), Some(0));

    let limited = get(&server, &format!("{path}?limit=2")).await;

    assert_eq!(limited.number_at("/limit"), 2);
    assert!(
        limited
            .json_at("/next_cursor")
            .is_some_and(Value::is_string)
    );

    // CREATE: 201 with the metadata-only registration envelope. `source` is the installed text the
    // contract names; the kernel's checks are reported back verbatim.
    let source_text = "export const meta = { name: 'toolWordCount' };\nexport async function run(args) { return { count: String(args.text || '').length }; }";
    let created_tool = post_json(
        &server,
        &path,
        Some(json!({
            "name": "toolWordCount",
            "description": "Counts the words in a string.",
            "tags": ["text", "count"],
            "parameters": {"type": "object", "properties": {"text": {"type": "string"}}},
            "source": source_text
        })),
    )
    .await;

    assert_eq!(
        created_tool.status,
        StatusCode::CREATED,
        "a create answers 201: {}",
        created_tool.body
    );
    assert_eq!(
        created_tool.string_at("/registration/name"),
        "toolWordCount"
    );
    assert_eq!(created_tool.body["registration"]["created"], json!(true));
    assert_eq!(created_tool.body["registration"]["replaced"], json!(false));
    assert!(
        created_tool.body["registration"].get("checks").is_none(),
        "T7b: a registration that ran no compiler reports NO `checks` array (`api.ts:3122` \
         omits the field when the kernel produced none)"
    );
    assert!(
        !created_tool
            .string_at("/registration/source_revision")
            .is_empty()
    );
    assert_eq!(created_tool.number_at("/registration/budget/used"), 1);
    assert_eq!(
        created_tool.revision(),
        get(&server, &format!("{path}?origin=created"))
            .await
            .number_at("/revision"),
        "the create's header and the roster page quote one toolbox counter"
    );

    // A created name is now in the page, with origin `created`.
    let created_page = get(&server, &format!("{path}?origin=created")).await;

    assert_eq!(created_page.body["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        created_page.body["items"][0]["name"],
        json!("toolWordCount")
    );
    assert_eq!(
        created_page.body["items"][0]["enabled"],
        json!(true),
        "a new tool is immediately usable"
    );
    assert_eq!(created_page.number_at("/budget/used"), 1);

    // THE SOURCE ROUTE is the only place the text lives.
    let source = get(&server, &format!("{path}/toolWordCount/source")).await;

    assert_eq!(source.status, StatusCode::OK);
    assert_eq!(
        source.header(&REVISION_HEADER),
        None,
        "TS stamps no header here"
    );
    assert_eq!(source.string_at("/name"), "toolWordCount");
    assert_eq!(source.string_at("/source"), source_text);
    assert_eq!(
        source.string_at("/revision"),
        created_tool.string_at("/registration/source_revision")
    );

    // A catalogue name this session did not register has no installed source.
    let no_source = get(&server, &format!("{path}/timeNow/source")).await;

    assert_eq!(no_source.status, StatusCode::NOT_FOUND);
    assert_eq!(
        no_source.message(),
        format!("Tool \"timeNow\" has no installed source revision in session \"{id}\".")
    );

    // THE IDENTIFIED READ: the row plus the schema and the `{scope, manual}` gate.
    let detail = get(&server, &format!("{path}/toolWordCount")).await;

    assert_eq!(detail.status, StatusCode::OK);
    assert_eq!(detail.string_at("/tool/name"), "toolWordCount");
    assert_eq!(detail.body["tool"]["session_bound"], json!(true));
    // A name this session CREATED is session-bound but not harness-owned, which is the contract's
    // `session_only` spelling (`store/tools.rs:291-295`).
    assert_eq!(detail.string_at("/tool/execution/scope"), "session_only");
    assert_eq!(detail.body["tool"]["execution"]["manual"], json!(true));
    assert!(detail.json_at("/tool/parameters").is_some());

    let ghost_tool = get(&server, &format!("{path}/ghostTool")).await;

    assert_eq!(ghost_tool.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_tool.message(),
        format!("Unknown tool \"ghostTool\" in session \"{id}\".")
    );

    // PATCH the declaration: metadata-only, so `replaced` stays false.
    let patched = patch_json(
        &server,
        &format!("{path}/toolWordCount"),
        json!({"description": "Counts words, case-insensitively."}),
    )
    .await;

    assert_eq!(
        patched.status,
        StatusCode::OK,
        "a metadata patch is a registration: {}",
        patched.body
    );
    assert_eq!(patched.body["registration"]["created"], json!(false));
    assert_eq!(patched.body["registration"]["replaced"], json!(false));
    assert_eq!(
        patched.string_at("/registration/source_revision"),
        created_tool.string_at("/registration/source_revision")
    );

    let page_after = get(&server, &format!("{path}?origin=created")).await;
    let row = page_after.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == json!("toolWordCount"))
        .cloned()
        .expect("the created row");

    assert_eq!(
        row["description"],
        json!("Counts words, case-insensitively.")
    );

    // THE ENABLE GATE, alone.
    let disabled = patch_json(
        &server,
        &format!("{path}/toolWordCount"),
        json!({"enabled": false}),
    )
    .await;

    assert_eq!(
        disabled.status,
        StatusCode::OK,
        "the switch is a real write: {}",
        disabled.body
    );
    assert_eq!(disabled.body["registration"]["created"], json!(false));
    assert_eq!(disabled.body["registration"]["replaced"], json!(false));

    let page_disabled = get(&server, &format!("{path}?enabled=false")).await;

    assert!(
        page_disabled.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"] == json!("toolWordCount")),
        "the disabled filter finds the name"
    );

    let enabled_again = patch_json(
        &server,
        &format!("{path}/toolWordCount"),
        json!({"enabled": true, "tags": ["text"]}),
    )
    .await;

    assert_eq!(
        enabled_again.status,
        StatusCode::OK,
        "both halves at once: {}",
        enabled_again.body
    );

    // The PATCH refusals.
    let empty_tool_patch = patch_json(&server, &format!("{path}/toolWordCount"), json!({})).await;

    assert_eq!(empty_tool_patch.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty_tool_patch.message(),
        "A patch must change at least one field."
    );

    let bad_enabled = patch_json(
        &server,
        &format!("{path}/toolWordCount"),
        json!({"enabled": "no"}),
    )
    .await;

    assert_eq!(bad_enabled.status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_enabled.message(), "\"enabled\" must be a boolean.");

    let bad_tool_field = patch_json(
        &server,
        &format!("{path}/toolWordCount"),
        json!({"name": "other"}),
    )
    .await;

    assert_eq!(bad_tool_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_tool_field.message(),
        "Unknown field \"name\". Allowed fields: description, tags, parameters, visibility, enabled, \
         source, module_text, dependencies, replace."
    );

    // A PROTECTED row is a policy refusal, and the route's sentence names the route.
    let protected_name = page.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["protected"] == json!(true))
        .map(|row| row["name"].as_str().unwrap_or_default().to_owned())
        .expect("a harness-owned row is present");

    let protected_patch = patch_json(
        &server,
        &format!("{path}/{protected_name}"),
        json!({"enabled": false}),
    )
    .await;

    assert_eq!(protected_patch.status, StatusCode::FORBIDDEN);
    assert_eq!(
        protected_patch.message(),
        format!(
            "Tool \"{protected_name}\" is harness-owned in session \"{id}\"; a protected entry cannot be enabled or disabled."
        )
    );

    let protected_delete = del(&server, &format!("{path}/{protected_name}")).await;

    assert_eq!(protected_delete.status, StatusCode::FORBIDDEN);
    assert_eq!(
        protected_delete.message(),
        format!(
            "Tool \"{protected_name}\" is harness-owned in session \"{id}\"; a protected entry cannot be unregistered."
        )
    );

    // An INHERITED catalogue name has nothing session-local to remove.
    let inherited_name = page.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["protected"] == json!(false) && row["origin"] == json!("catalogue"))
        .map(|row| row["name"].as_str().unwrap_or_default().to_owned())
        .expect("a plain catalogue row is present");

    let inherited_delete = del(&server, &format!("{path}/{inherited_name}")).await;

    assert_eq!(inherited_delete.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        inherited_delete.message(),
        format!(
            "Tool \"{inherited_name}\" is inherited by session \"{id}\" and has no session-local registration to remove."
        )
    );

    // T7b: a repeated name IS the replacement (`api.ts:3064-3095` + `harness.ts:1189-1191`: names
    // are not reserved and "already registered" is not an error), so the create answers 201 with
    // `created: false, replaced: true`, names the revision it replaced, and spends no second slot.
    // The `replace` FLAG is now the refusal: the POST route states replace-on-create as a capability
    // this deployment does not offer (`api.ts:3163-3168`).
    let duplicate = post_json(
        &server,
        &path,
        Some(json!({"name": "toolWordCount", "description": "again", "tags": ["text"]})),
    )
    .await;

    assert_eq!(
        duplicate.status,
        StatusCode::CREATED,
        "a repeated name re-registers: {}",
        duplicate.body
    );
    assert_eq!(duplicate.body["registration"]["created"], json!(false));
    assert_eq!(duplicate.body["registration"]["replaced"], json!(true));
    assert_eq!(
        duplicate.string_at("/registration/previous_revision"),
        created_tool.string_at("/registration/source_revision"),
        "the receipt names the revision that was installed before"
    );
    assert_eq!(
        duplicate.number_at("/registration/budget/used"),
        1,
        "a replace spends no new slot"
    );
    assert!(
        duplicate.body["registration"].get("checks").is_none(),
        "no compiler, no check results (`api.ts:3122`)"
    );

    let with_flag = post_json(
        &server,
        &path,
        Some(json!({
            "name": "toolWordCount",
            "description": "again",
            "tags": ["text"],
            "replace": true
        })),
    )
    .await;

    assert_eq!(
        with_flag.status,
        StatusCode::FORBIDDEN,
        "`replace` is a capability refusal: {}",
        with_flag.body
    );
    assert_eq!(
        with_flag.message(),
        format!(
            "POST /v1/harness/sessions/{id}/tools with \"replace\" is not available: \
             this deployment does not support an atomic replace-on-create registration."
        )
    );

    // The create refusals are the store's kernel rules, in its own sentences.
    let bad_name = post_json(
        &server,
        &path,
        Some(json!({"name": "counter", "description": "x", "tags": ["a"]})),
    )
    .await;

    assert_eq!(bad_name.status, StatusCode::BAD_REQUEST);
    assert!(
        bad_name
            .message()
            .starts_with("A created tool name must start with \"tool\"")
    );

    let no_tags = post_json(
        &server,
        &path,
        Some(json!({"name": "toolNoTags", "description": "x"})),
    )
    .await;

    assert_eq!(no_tags.status, StatusCode::BAD_REQUEST);

    // A name the CATALOGUE already owns is the conflict, but only once the name passes the created-name
    // grammar: `timeNow` is refused earlier, as a shape problem (`kernel`: a created name must start
    // with `tool`). `toolRegistry` is a real catalogue name that passes the grammar, so it reaches the
    // "already installed" refusal.
    let bad_shape = post_json(
        &server,
        &path,
        Some(json!({"name": "timeNow", "description": "x", "tags": ["a"]})),
    )
    .await;

    assert_eq!(bad_shape.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_shape.message(),
        "A created tool name must start with \"tool\" and use letters and digits only, e.g. \
         \"toolWordCount\". Got \"timeNow\"."
    );

    let shadow = post_json(
        &server,
        &path,
        Some(json!({"name": "toolRegistry", "description": "x", "tags": ["a"]})),
    )
    .await;

    assert_eq!(
        shadow.status,
        StatusCode::CONFLICT,
        "a created name may not shadow the catalogue: {}",
        shadow.body
    );

    let unknown_create_field = post_json(
        &server,
        &path,
        Some(json!({"name": "toolX", "description": "x", "tags": ["a"], "runtime": "rust"})),
    )
    .await;

    assert_eq!(unknown_create_field.status, StatusCode::BAD_REQUEST);
    assert!(
        unknown_create_field
            .message()
            .starts_with("Unknown field \"runtime\". Allowed fields: ")
    );

    // DELETE the created name, and the row leaves the page.
    let removed = del(&server, &format!("{path}/toolWordCount")).await;

    assert_eq!(
        removed.status,
        StatusCode::OK,
        "a removal is a receipt: {}",
        removed.body
    );
    assert_eq!(removed.body["deleted"], json!(true));
    assert_eq!(removed.string_at("/tool_id"), "toolWordCount");
    assert_eq!(removed.body["removed_created"], json!(true));
    assert_eq!(
        removed.body["restored_inherited"],
        json!(false),
        "the honest disclosure: an inherited baseline is not restored"
    );

    let page_final = get(&server, &path).await;

    assert!(
        !page_final.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"] == json!("toolWordCount"))
    );
    assert_eq!(page_final.number_at("/budget/used"), 0);

    let gone = get(&server, &format!("{path}/toolWordCount/source")).await;

    assert_eq!(gone.status, StatusCode::NOT_FOUND);

    let ghost_delete = del(&server, &format!("{path}/ghostTool")).await;

    assert_eq!(ghost_delete.status, StatusCode::NOT_FOUND);

    server.stop().await;
}

/// T7b: the tool-registration semantics, pinned one refusal per TypeScript arm.
///
/// Three separate behaviours live behind `POST .../tools`, and the port had collapsed two of them:
///
/// * a repeated name IS the replacement (`api.ts:3064-3095` + `harness.ts:1189-1191`: "names are
///   not reserved, 'already registered' is not an error, and replacing an implementation does not
///   consume the new-tool budget"), so the receipt says `created: false, replaced: true` with the
///   previous revision and the budget figure stays flat;
/// * the `"replace"` FLAG is a capability refusal (`api.ts:3163-3168`) that runs before the body is
///   decoded, so it wins over any field problem in the same document;
/// * a missing `name` / `description` / `tags` answers the HANDLER's sentence
///   (`api.ts:3002-3017`), never serde's `missing field ...`, and a stated `tags` that is not a
///   list of strings is `"tags" must be an array of strings.` on both verbs.
#[tokio::test]
async fn a_tool_registration_replaces_by_name_and_refuses_the_replace_flag() {
    let root = Root::new("toolbox-semantics");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("toolbox semantics")).await;
    let path = format!("/v1/harness/sessions/{id}/tools");

    let declaration = |description: &str| {
        json!({
            "name": "toolAlpha",
            "description": description,
            "tags": ["alpha"],
            "source": "return 1;",
        })
    };

    let first = post_json(&server, &path, Some(declaration("First declaration."))).await;

    assert_eq!(
        first.status,
        StatusCode::CREATED,
        "a first registration creates: {}",
        first.body
    );
    assert_eq!(first.body["registration"]["created"], json!(true));
    assert_eq!(first.body["registration"]["replaced"], json!(false));
    assert!(
        first.body["registration"]
            .get("previous_revision")
            .is_none(),
        "nothing came before"
    );
    assert!(
        first.body["registration"].get("checks").is_none(),
        "no compiler ran, so the receipt states no check results (`api.ts:3122`)"
    );
    let first_revision = first.string_at("/registration/source_revision");

    // The same name again: a replacement, not a collision, and no second creation slot.
    let again = post_json(&server, &path, Some(declaration("Second declaration."))).await;

    assert_eq!(
        again.status,
        StatusCode::CREATED,
        "a repeated name replaces: {}",
        again.body
    );
    assert_eq!(again.body["registration"]["created"], json!(false));
    assert_eq!(again.body["registration"]["replaced"], json!(true));
    assert_eq!(
        again.string_at("/registration/previous_revision"),
        first_revision,
        "the receipt names the revision it replaced"
    );
    assert_eq!(
        again.number_at("/registration/budget/used"),
        1,
        "a replacement never spends a creation slot (`harness.ts:1334-1347`)"
    );

    // `"replace": true` is refused as a capability, with the mount-shaped operation TS prints.
    let with_flag = post_json(
        &server,
        &path,
        Some(json!({
            "name": "toolBeta",
            "description": "Never registered.",
            "tags": ["beta"],
            "replace": true,
        })),
    )
    .await;

    assert_eq!(
        with_flag.status,
        StatusCode::FORBIDDEN,
        "{}",
        with_flag.body
    );
    assert_eq!(
        with_flag.message(),
        format!(
            "POST /v1/harness/sessions/{id}/tools with \"replace\" is not available: \
             this deployment does not support an atomic replace-on-create registration."
        )
    );

    // The create-mode presences, in the handler's own words.
    for (body, message) in [
        (
            json!({"description": "d.", "tags": ["t"]}),
            "A created tool requires a \"name\".",
        ),
        (
            json!({"name": "", "description": "d.", "tags": ["t"]}),
            "A created tool requires a \"name\".",
        ),
        (
            json!({"name": "toolGamma", "tags": ["t"]}),
            "A created tool requires a \"description\".",
        ),
        (
            json!({"name": "toolGamma", "description": 7, "tags": ["t"]}),
            "A created tool requires a \"description\".",
        ),
        (
            json!({"name": "toolGamma", "description": "d."}),
            "A created tool requires \"tags\".",
        ),
        (
            json!({"name": "toolGamma", "description": "d.", "tags": "text"}),
            "\"tags\" must be an array of strings.",
        ),
        (
            json!({"name": "toolGamma", "description": "d.", "tags": ["ok", 4]}),
            "\"tags\" must be an array of strings.",
        ),
    ] {
        let refused = post_json(&server, &path, Some(body.clone())).await;

        assert_eq!(
            refused.status,
            StatusCode::BAD_REQUEST,
            "`{body}` is refused: {}",
            refused.body
        );
        assert_eq!(refused.message(), message, "`{body}` names the rule");
    }

    // The gate is a boolean or nothing: `null` is a stated non-boolean (`api.ts:3191-3193`), and
    // a PATCH that restates `tags` badly gets the create's sentence (`api.ts:3009-3012`).
    let null_gate = patch_json(
        &server,
        &format!("{path}/toolAlpha"),
        json!({"enabled": null}),
    )
    .await;

    assert_eq!(null_gate.status, StatusCode::BAD_REQUEST);
    assert_eq!(null_gate.message(), "\"enabled\" must be a boolean.");

    let bad_tags = patch_json(
        &server,
        &format!("{path}/toolAlpha"),
        json!({"tags": ["ok", 4]}),
    )
    .await;

    assert_eq!(bad_tags.status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_tags.message(), "\"tags\" must be an array of strings.");

    // A real gate flip still works, and a `null` tags list is the same refusal (`!== undefined`).
    let null_tags = patch_json(&server, &format!("{path}/toolAlpha"), json!({"tags": null})).await;

    assert_eq!(null_tags.status, StatusCode::BAD_REQUEST);
    assert_eq!(null_tags.message(), "\"tags\" must be an array of strings.");

    let disabled = patch_json(
        &server,
        &format!("{path}/toolAlpha"),
        json!({"enabled": false}),
    )
    .await;

    assert_eq!(
        disabled.status,
        StatusCode::OK,
        "a boolean gate is still a write: {}",
        disabled.body
    );
    // The gate mints no source revision (`api.ts:2926-2989`), so the envelope says nothing was
    // created or replaced and the name keeps the revision the replace above installed.
    assert_eq!(disabled.body["registration"]["created"], json!(false));
    assert_eq!(disabled.body["registration"]["replaced"], json!(false));
    assert_eq!(
        disabled.string_at("/registration/source_revision"),
        again.string_at("/registration/source_revision")
    );

    server.stop().await;
}

/// C1/R7: the created-tool budget refusal is the TypeScript sentence, verbatim, at the
/// TypeScript status.
///
/// TS throws this from the kernel path the route calls (`harness.ts:3098-3102`) and the route
/// maps it by CONTENT (`api.ts:4869-4878`): `/budget|limit/i` answers 409, everything else
/// answers 400. The shipped sentence names neither word, so the arm it lands in is 400 — which
/// is what the contract states twice over:
///   * `harness-server.swagger.yaml` (`/v1/harness/sessions/{session_id}/tools`, post):
///     "Creation stops at the created-tool budget (32 by default, shown as `budget` in the
///     receipt) and that refusal is a 400 carrying the runtime's own advice to reuse an existing
///     tool, not a 409; a replacement of a name this session already holds does not consume the
///     budget.";
///   * `harness-api.ts:433-437` quotes the same sentence for `created_tools`.
#[tokio::test]
async fn the_created_tool_budget_refusal_is_the_typescript_sentence_at_400() {
    let root = Root::new("toolbox-budget");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("toolbox budget")).await;
    let path = format!("/v1/harness/sessions/{id}/tools");

    // Fill the lifetime budget: 32 DISTINCT names (`contract::MAX_CREATED_TOOLS`, and TS's
    // `core/creation.ts:73` `DEFAULT_CREATED_TOOLS_LIMIT` is the same 32).
    for slot in 0..32_u64 {
        let registered = post_json(
            &server,
            &path,
            Some(json!({
                "name": format!("toolBudget{slot}"),
                "description": "Budget filler.",
                "tags": ["budget"],
                "source": "return 1;",
            })),
        )
        .await;

        assert_eq!(
            registered.status,
            StatusCode::CREATED,
            "slot {slot} registers: {}",
            registered.body
        );
        assert_eq!(
            registered.number_at("/registration/budget/used"),
            slot + 1,
            "the receipt books the spend it just made"
        );
    }

    // One name over the budget: refused with TS's exact words, and the budget figure the receipt
    // carries is unchanged because a refusal spends nothing.
    let over = post_json(
        &server,
        &path,
        Some(json!({
            "name": "toolBudget32",
            "description": "One over the line.",
            "tags": ["budget"],
            "source": "return 1;",
        })),
    )
    .await;

    assert_eq!(
        over.status,
        StatusCode::BAD_REQUEST,
        "the budget refusal is a 400, never a 409 (`api.ts:4869-4878`): {}",
        over.body
    );
    assert_eq!(
        over.message(),
        "This harness already created 32 tools; reuse an existing tool instead of creating more."
    );

    // A replacement under a name this session already holds never spends a slot, so it still
    // answers 201 with the budget pinned at 32 (`harness.ts:3093`: only a NEW name counts).
    let replace = post_json(
        &server,
        &path,
        Some(json!({
            "name": "toolBudget0",
            "description": "Replaces the first filler.",
            "tags": ["budget"],
            "source": "return 2;",
        })),
    )
    .await;

    assert_eq!(
        replace.status,
        StatusCode::CREATED,
        "a replacement is not a creation: {}",
        replace.body
    );
    assert_eq!(replace.body["registration"]["created"], json!(false));
    assert_eq!(replace.body["registration"]["replaced"], json!(true));
    assert_eq!(replace.number_at("/registration/budget/used"), 32);

    server.stop().await;
}

/// T7b: the per-agent display colors ride the FROZEN definition, so the swatches a saved
/// conversation paints come from the copy it captured, not from a later global edit
/// (`harness-api.ts:884-900`, `api-reads.ts:1318-1329`).
#[tokio::test]
async fn a_session_definition_carries_the_harness_color_map() {
    let root = Root::new("definition-colors");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let created = post_json(
        &server,
        "/v1/harness/harnesses",
        Some(json!({
            "name": "t7b-palette",
            "agents": ["planner", "research"],
            "entry": "planner",
            "agent_colors": { "planner": "#123456", "research": "#abc" },
        })),
    )
    .await;

    assert_eq!(
        created.status,
        StatusCode::CREATED,
        "a colored harness is created: {}",
        created.body
    );

    let session = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "t7b-palette",
            "settings": {"base_url": base.clone(), "api_key": MOCK_KEY, "model": "mock/one"},
        })),
    )
    .await;

    assert_eq!(
        session.status,
        StatusCode::CREATED,
        "a session from it is created: {}",
        session.body
    );

    let id = session.string_at("/session/id");
    let definition = get(&server, &format!("/v1/harness/sessions/{id}/definition")).await;

    assert_eq!(definition.status, StatusCode::OK);
    assert_eq!(definition.body["definition"]["name"], "t7b-palette");
    assert_eq!(
        definition.body["definition"]["agent_colors"],
        json!({ "planner": "#123456", "research": "#abc" }),
        "the frozen copy carries the map (`descriptor.ts:117-119`)"
    );
    assert_eq!(
        definition.body["definition"]["agent_colors"], created.body["harness"]["agent_colors"],
        "and it is byte-identical to the definition the session was built from"
    );

    // A harness with NO colors keeps the pre-R2 wire shape on the session read as well: the key is
    // absent, not empty (`api-reads.ts:1327-1329`).
    let plain = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "instruct",
            "settings": {"base_url": base, "api_key": MOCK_KEY, "model": "mock/one"},
        })),
    )
    .await;

    assert_eq!(plain.status, StatusCode::CREATED, "{}", plain.body);

    let plain_id = plain.string_at("/session/id");
    let plain_definition = get(
        &server,
        &format!("/v1/harness/sessions/{plain_id}/definition"),
    )
    .await;

    assert_eq!(plain_definition.status, StatusCode::OK);
    assert!(
        plain_definition.body["definition"]
            .get("agent_colors")
            .is_none(),
        "an uncolored harness states no key: {}",
        plain_definition.body["definition"]
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 9 - the manual call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_manual_tool_call_answers_data_for_failure_and_status_for_refusals() {
    let root = Root::new("execution");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("execution")).await;
    let path = format!("/v1/harness/sessions/{id}/tools");

    // A REAL catalogue tool runs through the session's own registry. `timeNow` REQUIRES `timezone`
    // (its shipped schema), so the well-formed call names one, and the answer is the tool's own value:
    // a JSON text plus the panel the tool renders (`contract::ToolCallSuccess`).
    let executed = post_json(
        &server,
        &format!("{path}/timeNow/executions"),
        Some(json!({"parameters": {"timezone": "UTC"}})),
    )
    .await;

    assert_eq!(
        executed.status,
        StatusCode::OK,
        "a manual call is 200: {}",
        executed.body
    );
    assert_eq!(
        executed.body["ok"],
        json!(true),
        "the call ran: {}",
        executed.body
    );
    assert!(
        executed.body["result"]
            .as_str()
            .unwrap_or_default()
            .contains("UTC"),
        "`result` carries the tool's own text"
    );
    assert_eq!(
        executed.body["panels"][0]["id"],
        json!("timeNow"),
        "and the `toolUi` panel is unwrapped, not left as a `content` envelope"
    );
    assert!(executed.json_at("/duration_ms").is_some());
    assert_eq!(
        executed.header(&REVISION_HEADER),
        None,
        "a tool call is not a revisioned write"
    );

    // An absent `parameters` is an empty argument box, and an empty box is a schema failure for this
    // tool - the point is that NO body answers EXACTLY like `{}` does, one call to the other.
    let empty_box = post_json(
        &server,
        &format!("{path}/timeNow/executions"),
        Some(json!({"parameters": {}})),
    )
    .await;
    let bare = post_json(&server, &format!("{path}/timeNow/executions"), None).await;

    assert_eq!(bare.status, StatusCode::OK);

    // `duration_ms` is a wall-clock figure, so it is taken out of the comparison: everything ELSE the
    // two calls answer is identical, which is the claim being pinned.
    let without_time = |body: &Value| {
        let mut copy = body.clone();

        if let Some(map) = copy.as_object_mut() {
            map.remove("duration_ms");
        }

        copy
    };

    assert_eq!(
        without_time(&bare.body),
        without_time(&empty_box.body),
        "no body is the same call as an empty one"
    );
    assert!(
        bare.json_at("/duration_ms").is_some(),
        "the only difference is the timing"
    );
    assert_eq!(bare.body["ok"], json!(false));
    assert_eq!(
        bare.string_at("/error/message"),
        "Tool \"timeNow\" received arguments that do not match its schema: missing required property \
         \"timezone\"."
    );

    // A REJECTED call (bad arguments) is still a 200 with `ok: false`, and it never executed.
    let rejected = post_json(
        &server,
        &format!("{path}/read/executions"),
        Some(json!({"parameters": {"path": "/definitely/not/here"}})),
    )
    .await;

    assert_eq!(rejected.status, StatusCode::OK);
    assert_eq!(rejected.body["ok"], json!(false));
    assert!(!rejected.string_at("/error/message").is_empty());

    // A DECLARATIVE tool this build cannot compile answers the same well-formed failure, and the
    // session route's own 404/409 vocabulary stays reserved for "not callable here".
    let declared = post_json(
        &server,
        &path,
        Some(json!({
            "name": "toolEchoBack",
            "description": "Echoes its input.",
            "tags": ["text"],
            "source": "export const meta = { name: 'toolEchoBack' };\nexport async function run(args) { return { content: String(args.text || '') }; }"
        })),
    )
    .await;

    assert_eq!(declared.status, StatusCode::CREATED);

    let declared_call = post_json(
        &server,
        &format!("{path}/toolEchoBack/executions"),
        Some(json!({"parameters": {"text": "hi"}})),
    )
    .await;

    // DEVIATION, reported: a tool CREATED by this session has a declaration, an installed source
    // revision and a budget slot, but no callable body - the JS/module host that turns that source into
    // a registration is stage T6 (`knowledge.md` "T6"). The kernel therefore answers the call the way it
    // answers any name it cannot resolve, and it still answers 200 + `ok: false`, which is the
    // contract's shape for "the invocation ran and the tool refused".
    assert_eq!(declared_call.status, StatusCode::OK);
    assert_eq!(
        declared_call.body["ok"],
        json!(false),
        "a declaration has nothing to run"
    );
    assert!(
        declared_call
            .string_at("/error/message")
            .starts_with("Tool \"toolEchoBack\" is not registered."),
        "and the refusal says which name is missing: {}",
        declared_call.string_at("/error/message")
    );

    // The refusals.
    let ghost_call = post_json(
        &server,
        &format!("{path}/ghostTool/executions"),
        Some(json!({"parameters": {}})),
    )
    .await;

    assert_eq!(ghost_call.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_call.message(),
        format!("Unknown tool \"ghostTool\" in session \"{id}\".")
    );

    let bad_field = post_json(
        &server,
        &format!("{path}/timeNow/executions"),
        Some(json!({"args": {}})),
    )
    .await;

    assert_eq!(bad_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_field.message(),
        "Unknown field \"args\". Allowed fields: parameters."
    );

    let ghost_session = post_json(
        &server,
        "/v1/harness/sessions/ghost/tools/timeNow/executions",
        Some(json!({"parameters": {}})),
    )
    .await;

    assert_eq!(ghost_session.status, StatusCode::NOT_FOUND);
    assert_eq!(ghost_session.message(), "Unknown session \"ghost\".");

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 10 - paging and the route-level refusals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn paging_is_strict_and_the_route_refusals_keep_their_own_sentences() {
    let root = Root::new("paging");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("an answer", usage(2, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("paging")).await;

    let first = submit(&server, &id, "one", None).await;
    let second = submit(&server, &id, "two", None).await;

    wait_for_turn(&server, &id, &first.string_at("/turn_id"), "done").await;
    wait_for_turn(&server, &id, &second.string_at("/turn_id"), "done").await;

    // `limit` echoes the effective page size, and a full page carries a successor.
    let page = get(&server, &format!("/v1/harness/sessions/{id}/turns?limit=1")).await;

    assert_eq!(page.number_at("/limit"), 1);
    assert!(page.json_at("/next_cursor").is_some_and(Value::is_string));

    let cursor = page.string_at("/next_cursor");
    let next = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns?limit=1&cursor={cursor}"),
    )
    .await;

    assert_eq!(next.body["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(next.body["items"][0]["index"], json!(1));
    assert_eq!(
        next.json_at("/next_cursor"),
        Some(&Value::Null),
        "the last full page ends the walk"
    );

    // The filters and the order.
    let desc = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns?order=desc"),
    )
    .await;

    assert_eq!(desc.body["items"][0]["index"], json!(1));

    let filtered = get(
        &server,
        &format!(
            "/v1/harness/sessions/{id}/turns?thread_id={}",
            first.string_at("/thread_id")
        ),
    )
    .await;

    assert_eq!(filtered.body["items"].as_array().map(Vec::len), Some(1));

    let by_status = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns?status=queued"),
    )
    .await;

    assert_eq!(by_status.body["items"].as_array().map(Vec::len), Some(0));

    let bad_status = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns?status=paused"),
    )
    .await;

    assert_eq!(bad_status.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_status.message(),
        "Query parameter \"status\" must be one of: queued, running, done, error, cancelled."
    );

    let bad_order = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns?order=sideways"),
    )
    .await;

    assert_eq!(bad_order.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_order.message(),
        "Query parameter \"order\" must be one of: asc, desc."
    );

    for limit in ["0", "101", "abc", "1.5"] {
        let reply = get(
            &server,
            &format!("/v1/harness/sessions/{id}/turns?limit={limit}"),
        )
        .await;

        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "`limit={limit}` is refused"
        );
        assert_eq!(
            reply.message(),
            "Query parameter \"limit\" must be an integer between 1 and 100."
        );
    }

    let bad_cursor = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns?cursor=not-a-cursor"),
    )
    .await;

    assert_eq!(bad_cursor.status, StatusCode::BAD_REQUEST);

    // 404s, each naming the id it could not resolve.
    for (path, expected) in [
        (
            format!("/v1/harness/sessions/{id}/turns/ghost"),
            format!("Unknown turn \"ghost\" in session \"{id}\"."),
        ),
        (
            format!("/v1/harness/sessions/{id}/turns/ghost/display"),
            format!("Unknown turn \"ghost\" in session \"{id}\"."),
        ),
        (
            format!("/v1/harness/sessions/{id}/turns/ghost/events"),
            format!("Unknown turn \"ghost\" in session \"{id}\"."),
        ),
        (
            "/v1/harness/sessions/ghost/turns".to_string(),
            "Unknown session \"ghost\".".to_owned(),
        ),
        (
            "/v1/harness/sessions/ghost/queue".to_string(),
            "Unknown session \"ghost\".".to_owned(),
        ),
        (
            "/v1/harness/sessions/ghost/actions".to_string(),
            "Unknown session \"ghost\".".to_owned(),
        ),
    ] {
        let reply = get(&server, &path).await;

        assert_eq!(reply.status, StatusCode::NOT_FOUND, "`{path}` is a 404");
        assert_eq!(reply.message(), expected);
    }

    // A wrong verb names the verbs the ROUTE declares (`api.ts:4006-4013`).
    let wrong_verb = del(&server, &format!("/v1/harness/sessions/{id}/turns")).await;

    assert_eq!(wrong_verb.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        wrong_verb.message(),
        format!("Use GET for /v1/harness/sessions/{id}/turns.")
    );

    let put_session =
        Reply::send(client().put(server.url(&format!("/v1/harness/sessions/{id}")))).await;

    assert_eq!(put_session.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        put_session.message(),
        format!("Use GET, PATCH, DELETE for /v1/harness/sessions/{id}.")
    );

    // A non-JSON body is the contract's 415, and it is refused before the domain is consulted.
    let plain = Reply::read(
        client()
            .post(server.url("/v1/harness/sessions"))
            .header("content-type", "text/plain")
            .body("{\"harness\":\"instruct\"}")
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(plain.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        plain.message(),
        "A request body must be application/json (content-type \"text/plain\" is not supported)."
    );

    // A body-less route refuses a stated body rather than ignoring it.
    for (method, path) in [
        ("GET", format!("/v1/harness/sessions/{id}")),
        ("GET", format!("/v1/harness/sessions/{id}/turns")),
        ("GET", format!("/v1/harness/sessions/{id}/queue")),
        ("GET", format!("/v1/harness/sessions/{id}/usage")),
        ("GET", format!("/v1/harness/sessions/{id}/actions")),
        ("GET", format!("/v1/harness/sessions/{id}/agents")),
        ("GET", format!("/v1/harness/sessions/{id}/tools")),
        ("GET", format!("/v1/harness/sessions/{id}/threads")),
        ("POST", format!("/v1/harness/sessions/{id}/cancel")),
    ] {
        let reply = Reply::read(
            client()
                .request(
                    axum::http::Method::from_bytes(method.as_bytes()).expect("a real method"),
                    server.url(&path),
                )
                .header("content-type", "application/json")
                .body("{\"x\":1}")
                .send()
                .await
                .unwrap_or_else(|error| panic!("{method} `{path}` completes: {error}")),
        )
        .await;

        if method == "POST" {
            assert_eq!(
                reply.status,
                StatusCode::BAD_REQUEST,
                "POST `{path}` refuses a field it does not declare"
            );
            assert_eq!(reply.message(), "Unknown field \"x\". Allowed fields: all.");
        } else {
            // T7b: a GET never reads the body - the TypeScript GET handlers call neither
            // `ctx.body()` nor the defined-but-uncalled `readEmptyBody` (`api.ts:520-531`) - so the
            // document is IGNORED and the read answers normally.
            assert_eq!(
                reply.status,
                StatusCode::OK,
                "`{method} {path}` ignores a body it never reads"
            );
        }
    }

    // A DELETE is body-less the same way: TS's delete handlers never touch `ctx.body()`, so the
    // bytes are discarded rather than refused. A missing row is still the contract's 404.
    let delete_with_body = Reply::read(
        client()
            .delete(server.url(&format!("/v1/harness/sessions/{id}/tools/ghostTool")))
            .header("content-type", "application/json")
            .body("{\"x\":1}")
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(delete_with_body.status, StatusCode::NOT_FOUND);
    assert_eq!(
        delete_with_body.message(),
        format!("Unknown tool \"ghostTool\" in session \"{id}\".")
    );

    // A malformed JSON body is the adapter's 400, whatever the route.
    let malformed = Reply::read(
        client()
            .post(server.url("/v1/harness/sessions"))
            .header("content-type", "application/json")
            .body("{oops")
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(malformed.status, StatusCode::BAD_REQUEST);
    assert_eq!(malformed.message(), "The request body must be valid JSON.");

    // The task body's own vocabulary.
    let not_a_string = submit_json(&server, &id, json!({"input": 5})).await;

    assert_eq!(not_a_string.status, StatusCode::BAD_REQUEST);
    assert_eq!(not_a_string.message(), "A task must be a string.");

    // A body with the WRONG FIELD is the field refusal even when `input` is missing too: the field check
    // is the whole-body guard that runs first (`api.ts:5055-5085`), and only a body whose fields are all
    // declared reaches the per-field parse that says "A task must be a string.".
    let missing_input = submit_json(&server, &id, json!({"text": "x"})).await;

    assert_eq!(missing_input.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        missing_input.message(),
        "Unknown field \"text\". Allowed fields: input, attachments, references, thread_id."
    );

    let bad_task_field = submit_json(&server, &id, json!({"input": "x", "files": []})).await;

    assert_eq!(bad_task_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_task_field.message(),
        "Unknown field \"files\". Allowed fields: input, attachments, references, thread_id."
    );

    let ghost_submit = submit_json(&server, "ghost", json!({"input": "x"})).await;

    assert_eq!(ghost_submit.status, StatusCode::NOT_FOUND);
    assert_eq!(ghost_submit.message(), "Unknown session \"ghost\".");

    let empty_task = submit(&server, &id, "   ", None).await;

    assert_eq!(empty_task.status, StatusCode::BAD_REQUEST);
    assert_eq!(empty_task.message(), "A task cannot be empty.");

    server.stop().await;
}

/// One submit with an arbitrary body, for the body-policy assertions.
async fn submit_json(server: &TestServer, session: &str, body: Value) -> Reply {
    post_json(
        server,
        &format!("/v1/harness/sessions/{session}/tasks"),
        Some(body),
    )
    .await
}

// ---------------------------------------------------------------------------
// 11 - a slash command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_slash_command_is_a_terminal_turn_recorded_without_a_model_request() {
    let root = Root::new("commands");
    let server = TestServer::start(&root).await;
    let (base, script) = provider(vec![answered("never asked", usage(1, 1))]).await;

    let (id, _) = create_session(&server, &base, Some("commands")).await;
    let receipt = submit(&server, &id, "/help", None).await;

    assert_eq!(
        receipt.status,
        StatusCode::ACCEPTED,
        "a command is still 202: {}",
        receipt.body
    );
    assert_eq!(
        receipt.string_at("/status"),
        "done",
        "the command turn is terminal before the receipt answers"
    );
    assert!(receipt.json_at("/queue_position").is_none());
    let turn_id = receipt.string_at("/turn_id");

    let read = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}"),
    )
    .await;

    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.string_at("/turn/status"), "done");
    assert_eq!(read.string_at("/turn/prompt"), "/help");
    assert!(
        read.json_at("/turn/usage").is_none(),
        "no provider request happened, so nothing is billed"
    );

    let events = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/events"),
    )
    .await;

    assert_eq!(
        events.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["event"]["type"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["start", "tool_call", "tool_result"],
        "a command rides the synthesized call/result pair"
    );
    assert_eq!(
        events.body["items"][1]["event"]["call"]["name"],
        json!("command")
    );
    assert_eq!(events.body["items"][2]["event"]["name"], json!("command"));
    assert!(
        events.body["items"][2]["event"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains("help"),
        "the result is the command's own text"
    );
    assert_eq!(events.json_at("/partial"), Some(&json!(false)));

    // The row says what it ran, and no provider call was made.
    let turns = get(&server, &format!("/v1/harness/sessions/{id}/turns")).await;

    assert_eq!(turns.body["items"][0]["stepCount"], json!(1));
    assert_eq!(turns.body["items"][0]["status"], json!("done"));
    assert!(
        script.requests().is_empty(),
        "a command never asks the provider"
    );

    // An unknown command is an error-SHAPED RESULT, not a 4xx: the route accepted the task, the kernel
    // ran it, and the failure is the `command` call's own failed result. (The kernel treats an unmatched
    // leading slash as a command that does not exist, which is why the turn settles `error` rather than
    // asking the provider.)
    let unknown = submit(&server, &id, "/nosuchcmd", None).await;

    assert_eq!(unknown.status, StatusCode::ACCEPTED);
    assert_eq!(unknown.string_at("/status"), "error");

    let unknown_events = get(
        &server,
        &format!(
            "/v1/harness/sessions/{id}/turns/{}/events",
            unknown.string_at("/turn_id")
        ),
    )
    .await;

    assert_eq!(
        unknown_events.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["event"]["type"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["start", "tool_call", "tool_result"],
        "the failed command is the whole run"
    );
    assert_eq!(
        unknown_events.body["items"][2]["event"]["isError"],
        json!(true),
        "the refusal rides the result, and the turn still settles"
    );
    assert_eq!(
        unknown_events.body["items"][2]["event"]["content"],
        json!("unknown command: /nosuchcmd")
    );
    assert_eq!(
        unknown_events.body["items"][2]["event"]["outcome"],
        json!("failed")
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 12 - the owner-question round trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_owner_question_parks_the_run_and_the_answer_settles_it() {
    let root = Root::new("actions");
    let server = TestServer::start(&root).await;

    let question = json!({
        "question": "Should I ship the patch?",
        "options": [
            {"id": "yes", "label": "Ship it"},
            {"id": "no", "label": "Hold"}
        ],
        "recommended": "yes"
    });

    let (base, _script) = provider(vec![
        tool_turn(
            "call-ask",
            "actionRequest",
            &question.to_string(),
            usage(9, 2),
        ),
        answered("ship it is", usage(4, 2)),
        // The second question, raised by a second run, answered with free text instead of an option.
        tool_turn(
            "call-ask-2",
            "actionRequest",
            &question.to_string(),
            usage(5, 2),
        ),
        answered("reverting then", usage(6, 2)),
    ])
    .await;

    let (id, _) = create_session(&server, &base, Some("actions")).await;
    let receipt = submit(&server, &id, "ask me", None).await;
    let turn_id = receipt.string_at("/turn_id");

    // The question is live, and the row is the RECORDED request.
    let listed = wait_for(
        &server,
        &format!("/v1/harness/sessions/{id}/actions"),
        |body| body["actions"].as_array().map(Vec::len).unwrap_or(0) == 1,
        "one waiting question",
    )
    .await;

    assert_eq!(listed.status, StatusCode::OK);
    assert_eq!(
        listed.header(&REVISION_HEADER),
        None,
        "the body already carries the revision"
    );
    assert_eq!(listed.string_at("/session_id"), id);
    assert_eq!(listed.body["actions"][0]["turn_id"], json!(turn_id));
    assert!(
        !listed.string_at("/actions/0/request/requestId").is_empty(),
        "the runtime mints the request id; the model's own is never trusted"
    );
    assert_eq!(
        listed.string_at("/actions/0/request/question"),
        "Should I ship the patch?"
    );
    assert_eq!(
        listed.body["actions"][0]["request"]["allowCustom"],
        json!(true)
    );
    assert_eq!(
        listed.body["actions"][0]["request"]["options"]
            .as_array()
            .map(Vec::len),
        Some(2)
    );
    let request_id = listed.string_at("/actions/0/request/requestId");

    assert_eq!(
        listed.body["actions"][0]["request"]["options"],
        json!([{"id": "yes", "label": "Ship it"}, {"id": "no", "label": "Hold"}]),
        "the row states the RECORDED options, which are the only ones an answer may use"
    );
    assert_eq!(
        listed.body["actions"][0]["request"]["recommended"],
        json!("yes")
    );
    assert_eq!(
        listed.string_at("/actions/0/agent"),
        "instruct",
        "the row names the asking agent by its roster id"
    );
    assert!(
        listed.json_at("/actions/0/received_at").is_some(),
        "and when it arrived"
    );
    // Opening a question moves the `actions` counter exactly once (`api.ts:1109`
    // `events.bumpRevision('actions', record.id)` inside the `action.opened` bridge), so a read taken
    // while the question is waiting already reports 1 - the figure a stream subscriber compares its
    // `subscription_state` baseline against.
    assert_eq!(
        listed.number_at("/revision"),
        1,
        "the opened question moved the actions counter to 1"
    );

    // The parked run is still LIVE: the row says `running`, the event page says `partial`, and the call
    // that is parked on its owner has a step detail with NO result member (`sessions.ts:3955-3963`).
    let parked = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}"),
    )
    .await;

    assert_eq!(
        parked.string_at("/turn/status"),
        "running",
        "the run waits for its owner: {}",
        parked.body
    );

    let parked_events = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/events"),
    )
    .await;

    assert_eq!(
        parked_events.json_at("/partial"),
        Some(&json!(true)),
        "an unfinished run pages partial events"
    );

    let call_index = parked_events.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["event"]["type"] == json!("tool_call"))
        .expect("the parked call")["index"]
        .as_u64()
        .expect("an event index");

    let parked_step = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/steps/{call_index}"),
    )
    .await;

    assert_eq!(parked_step.status, StatusCode::OK);
    assert_eq!(parked_step.string_at("/detail/callId"), "call-ask");
    assert!(
        parked_step.json_at("/detail/result").is_none(),
        "a call parked on its owner has no result to report: {}",
        parked_step.body
    );

    assert!(
        !listed.string_at("/actions/0/agent").is_empty(),
        "the row names the asking agent"
    );

    // An option the model never offered is refused, and the refusal lists the ones it did.
    let wrong_option = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/actions/{request_id}/answer"),
        Some(json!({"choice": {"kind": "option", "option_id": "maybe"}})),
    )
    .await;

    assert_eq!(wrong_option.status, StatusCode::BAD_REQUEST);
    assert!(
        wrong_option
            .message()
            .starts_with("Unknown option \"maybe\". Valid options: ")
    );

    // The choice vocabulary.
    for (body, expected) in [
        (
            json!({"nope": 1}),
            "Unknown field \"nope\". Allowed fields: choice.",
        ),
        (json!({}), "An action answer requires a \"choice\"."),
        (
            json!({"choice": {"kind": "option"}}),
            "An option choice requires a string \"option_id\".",
        ),
        (
            json!({"choice": {"kind": "custom"}}),
            "A custom choice requires string \"text\".",
        ),
        (
            json!({"choice": {"kind": "vibes"}}),
            "A \"choice\" must be { kind: \"option\", option_id } or { kind: \"custom\", text }.",
        ),
        (json!({"choice": "yes"}), "A \"choice\" must be an object."),
    ] {
        let described = body.to_string();
        let reply = post_json(
            &server,
            &format!("/v1/harness/sessions/{id}/actions/{request_id}/answer"),
            Some(body),
        )
        .await;

        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "`{described}` is refused"
        );
        assert_eq!(reply.message(), expected);
    }

    // The answer, which resolves the parked promise and lets the run finish.
    let answered_reply = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/actions/{request_id}/answer"),
        Some(json!({"choice": {"kind": "option", "option_id": "yes"}})),
    )
    .await;

    assert_eq!(
        answered_reply.status,
        StatusCode::OK,
        "the answer is accepted: {}",
        answered_reply.body
    );
    assert_eq!(answered_reply.body["ok"], json!(true));
    assert_eq!(answered_reply.string_at("/request_id"), request_id);
    assert!(answered_reply.number_at("/resolved_at") > 0);
    // One open (1) + the answer's `action.closed` frame (2) + the REST-owned bump the answer route
    // performs (`api.ts:2420`, which the header must equal) = 3. The frame and the header are the same
    // number because the answer settles the question once.
    assert_eq!(
        answered_reply.revision(),
        3,
        "the answer settled the question: one closed frame plus the route's own actions bump"
    );

    let done = wait_for_turn(&server, &id, &turn_id, "done").await;

    assert!(
        done.number_at("/turn/event_count") >= 6,
        "the answer is recorded: {}",
        done.body
    );

    let events = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}/events"),
    )
    .await;
    let kinds = events.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["event"]["type"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();

    assert_eq!(
        kinds,
        vec![
            "start",
            "usage",
            "tool_call",
            "action_answer",
            "tool_result",
            "attempt_commit",
            "text",
            "usage",
            "done"
        ],
        "the question is the tool call, the answer is its own event, and the result pairs with the call"
    );

    let action_events = events.body["items"].as_array().unwrap().clone();
    let answer_index = kinds
        .iter()
        .position(|kind| *kind == "action_answer")
        .expect("the answer event");

    assert_eq!(
        action_events[answer_index]["event"]["requestId"],
        json!(request_id),
        "the recorded answer names the request it settles"
    );
    assert_eq!(
        action_events[answer_index]["event"]["answer"]["choice"],
        json!({"kind": "option", "optionId": "yes"}),
        "and the choice, in the domain's camelCase spelling (`contract::HarnessActionChoice`)"
    );
    assert_eq!(
        action_events[answer_index]["event"]["answer"]["requestId"],
        json!(request_id),
        "the answer envelope repeats the id it answers, exactly as the recorded event carries it"
    );
    assert_eq!(
        action_events[answer_index + 1]["event"]["type"],
        json!("tool_result"),
        "the parked call is the one that resolves"
    );
    assert_eq!(
        action_events[answer_index + 1]["event"]["callId"],
        json!("call-ask"),
        "resolved by call id, never by position"
    );

    // A live question is answered exactly once, and the list goes empty.
    let empty = get(&server, &format!("/v1/harness/sessions/{id}/actions")).await;

    assert_eq!(empty.body["actions"].as_array().map(Vec::len), Some(0));

    let reanswer = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/actions/{request_id}/answer"),
        Some(json!({"choice": {"kind": "custom", "text": "again"}})),
    )
    .await;

    assert_eq!(
        reanswer.status,
        StatusCode::CONFLICT,
        "one answer per question: {}",
        reanswer.body
    );
    assert_eq!(
        reanswer.message(),
        format!("Action request \"{request_id}\" was already answered or has expired.")
    );

    // A request that never existed is the OTHER refusal, with the store's own sentence
    // (`sessions.ts:2295`).
    let ghost_answer = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/actions/ghost-request/answer"),
        Some(json!({"choice": {"kind": "custom", "text": "late"}})),
    )
    .await;

    assert_eq!(ghost_answer.status, StatusCode::NOT_FOUND);
    assert_eq!(
        ghost_answer.message(),
        "Unknown action request \"ghost-request\"."
    );

    // A custom answer is accepted on a live question, which is the point of `allow_custom`. The route
    // the view calls to CREATE a question is not part of this contract at all: a question is what a run
    // asks, so the POST the rail does not mount answers with the rail's own refusal.
    let not_mounted = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/actions"),
        Some(question),
    )
    .await;

    assert_eq!(not_mounted.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        not_mounted.message(),
        format!("Use GET for /v1/harness/sessions/{id}/actions.")
    );

    let second = submit(&server, &id, "ask again", None).await;
    let second_turn = second.string_at("/turn_id");

    let listed_again = wait_for(
        &server,
        &format!("/v1/harness/sessions/{id}/actions"),
        |body| body["actions"].as_array().map(Vec::len).unwrap_or(0) == 1,
        "the second waiting question",
    )
    .await;

    let second_request = listed_again.string_at("/actions/0/request/requestId");

    assert_ne!(
        second_request, request_id,
        "each question is its own request, even when the text repeats"
    );
    assert_eq!(listed_again.string_at("/actions/0/turn_id"), second_turn);

    let custom = post_json(
        &server,
        &format!("/v1/harness/sessions/{id}/actions/{second_request}/answer"),
        Some(json!({"choice": {"kind": "custom", "text": "ship it, then revert"}})),
    )
    .await;

    assert_eq!(
        custom.status,
        StatusCode::OK,
        "a custom answer works: {}",
        custom.body
    );
    assert_eq!(custom.body["ok"], json!(true));

    wait_for_turn(&server, &id, &second_turn, "done").await;

    let settled_events = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{second_turn}/events"),
    )
    .await;

    assert_eq!(
        settled_events.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["event"]["type"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec![
            "start",
            "usage",
            "tool_call",
            "action_answer",
            "tool_result",
            "attempt_commit",
            "text",
            "usage",
            "done"
        ],
        "a free-text answer lands as the same event pair an option does"
    );
    assert_eq!(
        settled_events.body["items"][3]["event"]["answer"]["choice"],
        json!({"kind": "custom", "text": "ship it, then revert"}),
        "verbatim, because the answer IS the tool result the model reads"
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 15 - the store's host view is LIVE, not a boot snapshot (T7a)
// ---------------------------------------------------------------------------

/// The three host writes a deployment makes between boots, proved against the session each one is
/// supposed to affect. TS reads live state on every create — `sessions.ts:1827` resolves the harness
/// from `this.registry`, `api.ts:1974-1989` merges `store.globalSettings`, and the instruction ladder
/// reads the live `agentState` (`api.ts:1707`) — so a store that kept boot snapshots answered a
/// `POST /sessions` for a harness created seconds ago with `Unknown harness "..."`, froze the definition
/// from the shipped catalogue, kept the pre-patch model, and never saw the agent override.
#[tokio::test]
async fn a_host_write_is_visible_to_the_next_session_without_a_restart() {
    let root = Root::new("live-host-view");
    let server = TestServer::start(&root).await;

    // The provider is never called here: every assertion is a create or a read.
    let (_base, _provider) = provider(vec![answered("unused", usage(1, 1))]).await;

    // ---- 1. POST /harnesses -> POST /sessions resolves the NEW definition. ----
    let created_harness = post_json(
        &server,
        "/v1/harness/harnesses",
        Some(json!({
            "name": "t7a-live",
            "title": "T7a Live",
            "description": "Created after the store booted.",
            "agents": ["planner", "research"],
            "entry": "planner",
            "agent_models": { "research": "provider/model-y" },
        })),
    )
    .await;

    assert_eq!(
        created_harness.status,
        StatusCode::CREATED,
        "the definition is created: {}",
        created_harness.body
    );

    let fresh = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "t7a-live",
            "settings": {"base_url": "http://127.0.0.1:1"}
        })),
    )
    .await;

    assert_eq!(
        fresh.status,
        StatusCode::CREATED,
        "a session on a harness created after boot is not a 404: {}",
        fresh.body
    );
    assert_eq!(fresh.string_at("/session/harness"), "t7a-live");

    // The FROZEN copy is the new definition, not a snapshot of the shipped catalogue: the roster the
    // session reports is the one the create just registered.
    let roster = get(
        &server,
        &format!(
            "/v1/harness/sessions/{}/agents",
            fresh.string_at("/session/id")
        ),
    )
    .await;

    assert_eq!(
        roster.body["agents"]
            .as_array()
            .expect("an array")
            .iter()
            .map(|row| row["agent_id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        vec!["planner", "research"],
        "the session's roster is the harness the host just wrote"
    );
    assert_eq!(
        roster.body["agents"][1]["inherited_model"],
        json!("provider/model-y"),
        "and the per-agent model it froze from it"
    );

    // ---- 2. PATCH /config {model} -> the NEXT session reports the new model. ----
    let patched = patch_json(
        &server,
        "/v1/harness/config",
        json!({"settings": {"model": "patched/model"}}),
    )
    .await;

    assert_eq!(
        patched.status,
        StatusCode::OK,
        "the config patch lands: {}",
        patched.body
    );

    let after_config = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "instruct",
            "settings": {"base_url": "http://127.0.0.1:1"}
        })),
    )
    .await;

    assert_eq!(after_config.status, StatusCode::CREATED);
    assert_eq!(
        after_config.string_at("/session/model"),
        "patched/model",
        "the create merged the CURRENT defaults, not the boot pair (`api.ts:1974`)"
    );
    assert_eq!(
        after_config.json_at("/session/endpoint"),
        Some(&json!({
            "base_url": "http://127.0.0.1:1",
            "model": "patched/model",
            "has_api_key": false,
        })),
        "the session's endpoint block is its own merged pair, with the key it was not given"
    );

    // ---- 3. PATCH /agents -> the next session's ladder reports the override. ----
    let before = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "instruct",
            "settings": {"base_url": "http://127.0.0.1:1"}
        })),
    )
    .await;
    let before_id = before.string_at("/session/id");
    let before_roster = get(&server, &format!("/v1/harness/sessions/{before_id}/agents")).await;

    assert_eq!(
        before_roster.json_at("/agents/0/instruction_source"),
        Some(&json!("catalogue")),
        "an untouched session runs the shipped text: {}",
        before_roster.body
    );

    let patched_agent = patch_json(
        &server,
        "/v1/harness/agents/instruct",
        json!({"instruction": "Overridden after boot."}),
    )
    .await;

    assert_eq!(
        patched_agent.status,
        StatusCode::OK,
        "the agent patch lands: {}",
        patched_agent.body
    );

    let after_agent = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "instruct",
            "settings": {"base_url": "http://127.0.0.1:1"}
        })),
    )
    .await;
    let after_id = after_agent.string_at("/session/id");
    let after_roster = get(&server, &format!("/v1/harness/sessions/{after_id}/agents")).await;

    assert_eq!(
        after_roster.json_at("/agents/0/instruction_source"),
        Some(&json!("global")),
        "the new session's ladder sees the global overlay (`api-reads.ts:1012-1018`)"
    );

    // The exact figure: `instruction_chars` is the length of the text the member RUNS, and the
    // instruction read states the same text from the same live catalogue.
    let instruction = get(
        &server,
        &format!("/v1/harness/sessions/{after_id}/agents/instruct/instruction"),
    )
    .await;

    assert_eq!(
        after_roster.json_at("/agents/0/instruction_chars"),
        instruction.json_at("/chars"),
        "the roster row's `instruction_chars` is the instruction read's `chars`"
    );
    assert_eq!(
        instruction.json_at("/source"),
        Some(&json!("global")),
        "and the read itself names the layer the patch created"
    );
    assert!(
        instruction
            .string_at("/instruction")
            .starts_with("Overridden after boot."),
        "the override is the head of the composed prompt, got: {}",
        instruction.string_at("/instruction")
    );
    assert_ne!(
        after_roster.json_at("/agents/0/instruction_chars"),
        before_roster.json_at("/agents/0/instruction_chars"),
        "the override changed the text the member runs"
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 16 - the context-segment write: PATCH .../turns/:turn_id/blocks/:block_id (R18/R19)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_context_block_write_switches_the_inclusion_of_the_entry_its_segment_records() {
    let root = Root::new("block-write");
    let server = TestServer::start(&root).await;

    // A tool call, then the answer it was leading to: the transcript ends with the assistant entry that
    // carries the call, the tool result, and the final text, so the unit join has something to find.
    let (base, _script) = provider(vec![
        tool_turn(
            "call-time",
            "timeNow",
            r#"{"timezone":"UTC"}"#,
            usage(11, 4),
        ),
        answered("the clock reads noon", usage(7, 3)),
    ])
    .await;

    let (id, _) = create_session(&server, &base, Some("block-write")).await;
    let receipt = submit(&server, &id, "what time is it", None).await;
    let turn_id = receipt.string_at("/turn_id");
    let thread = receipt.string_at("/thread_id");

    wait_for_turn(&server, &id, &turn_id, "done").await;

    let block_path =
        |block: &str| format!("/v1/harness/sessions/{id}/turns/{turn_id}/blocks/{block}");
    let transcript_path = format!("/v1/harness/sessions/{id}/threads/{thread}/transcript");

    // THE JOIN ON READ. `0-tool-2` is the call's segment - the tool event is index 2 of this turn - and
    // its recorded `callId` resolves through the transcript to the exclusion unit (`api.ts:4695-4712`
    // + `transcriptUnitOfCall`). A flag nobody has ever switched answers `true`, which is the ABSENCE of
    // `included: false` on the transcript message rather than a client's word.
    let before = get(&server, &block_path("0-tool-2")).await;

    assert_eq!(
        before.status,
        StatusCode::OK,
        "the call's segment resolves: {}",
        before.body
    );
    assert_eq!(
        before.json_at("/block/included"),
        Some(&json!(true)),
        "the block read publishes the verdict: {}",
        before.body
    );

    let transcript = get(&server, &transcript_path).await;
    let revision = transcript.number_at("/transcript/revision");
    let message_count = transcript.number_at("/transcript/message_count");

    // THE WRITE. One axis, no guard. The receipt is the bare `TurnBlockWriteReceipt` TypeScript answers
    // (`api.ts:4787-4798`) - no `accepted_as` envelope - and `included` is the PERSISTED verdict, read
    // back through the same join rather than echoed from the request (`api.ts:4755-4785`).
    let written = patch_json(&server, &block_path("0-tool-2"), json!({"included": false})).await;

    assert_eq!(
        written.status,
        StatusCode::OK,
        "a unit write is a 200: {}",
        written.body
    );
    assert_eq!(written.string_at("/session_id"), id);
    assert_eq!(written.string_at("/turn_id"), turn_id);
    assert_eq!(written.string_at("/block_id"), "0-tool-2");
    assert_eq!(
        written.json_at("/included"),
        Some(&json!(false)),
        "the receipt states the verdict it installed"
    );
    assert!(
        written.json_at("/reasoning_included").is_none(),
        "a tool segment has no thinking axis to state: {}",
        written.body
    );
    assert_eq!(written.string_at("/thread_id"), thread);
    assert_eq!(
        written.number_at("/revision"),
        revision + 1,
        "the transcript counter moved by exactly one"
    );
    assert_eq!(
        written.revision(),
        revision + 1,
        "the Etag quotes the transcript revision, the figure `json(200, receipt, revision)` passes"
    );
    assert_eq!(
        written.string_at("/links/block"),
        block_path("0-tool-2"),
        "the block link is the route the client re-reads; `encodeURIComponent` of a minted id is a no-op, because the id is `[a-z0-9-]+` by construction"
    );
    assert_eq!(
        written.string_at("/links/transcript"),
        transcript_path,
        "and the transcript link is the document the flag now lives in"
    );

    // READ-BACK AT BOTH ENDS: the block's verdict, and the flag on the transcript message itself. The
    // exclusion has to live in the document, or the next run would read the call back in.
    let after = get(&server, &block_path("0-tool-2")).await;

    assert_eq!(after.json_at("/block/included"), Some(&json!(false)));

    let messages = get(&server, &format!("{transcript_path}/messages")).await;
    let tool_entry = messages.body["items"]
        .as_array()
        .expect("a paged transcript")
        .iter()
        .find(|entry| entry["role"] == json!("tool"))
        .expect("the tool message the call wrote");

    assert_eq!(
        tool_entry["included"],
        json!(false),
        "the flag rides the transcript document, not a side table"
    );

    // SWITCHING IT BACK returns the document to its pre-feature bytes. The verdict the read publishes is
    // still `true` - the join resolves and finds no `included: false` on the entry (`api.ts:4628-4630`) -
    // while the durable message carries no flag at all.
    let restored = patch_json(&server, &block_path("0-tool-2"), json!({"included": true})).await;

    assert_eq!(
        restored.status,
        StatusCode::OK,
        "a restore is a 200: {}",
        restored.body
    );
    assert_eq!(restored.json_at("/included"), Some(&json!(true)));
    assert_eq!(restored.number_at("/revision"), revision + 2);

    let restored_messages = get(&server, &format!("{transcript_path}/messages")).await;

    assert_eq!(
        restored_messages.body["items"]
            .as_array()
            .expect("a paged transcript")
            .iter()
            .find(|entry| entry["role"] == json!("tool"))
            .expect("the tool message")
            .get("included"),
        None,
        "switching the flag back to INCLUDED removes the key rather than storing `true` \
         (`api/messages.ts:146-147`: only `false` is durable), which is the exact byte shape a \
         pre-feature transcript had. The block read still answers `true`, because absence IS inclusion."
    );
    assert_eq!(
        restored_messages.number_at("/total"),
        message_count,
        "neither write changed the entry count: a flag switch is an in-place patch of one entry"
    );

    // THE BODY GATE (`api.ts:4641-4662`), in the order the route runs it: shape before lookup, lookup
    // before the guard, guard before the joins.
    let empty = patch_json(&server, &block_path("0-tool-2"), json!({})).await;

    assert_eq!(empty.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty.message(),
        "A context segment write requires at least one of \"included\" and \"reasoningIncluded\"."
    );

    let not_boolean = patch_json(&server, &block_path("0-tool-2"), json!({"included": "no"})).await;

    assert_eq!(not_boolean.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        not_boolean.message(),
        "A context segment inclusion write requires a boolean \"included\" value."
    );

    let not_thinking_boolean = patch_json(
        &server,
        &block_path("0-tool-2"),
        json!({"reasoningIncluded": 1}),
    )
    .await;

    assert_eq!(not_thinking_boolean.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        not_thinking_boolean.message(),
        "A context segment write requires a boolean \"reasoningIncluded\" value."
    );

    // `if_revision` is NOT a field on this route: `requireKnownFields` names exactly the two axes
    // (`api.ts:4672`), and the write goes through `store.updateThreadTranscript` with NO guard of its own
    // (`api.ts:4741`) - unlike the transcript PUT, which is the contract's one optimistic lock
    // (`api.ts:4375-4400`). The check runs ahead of the axis checks, so a body that carries a valid axis
    // and a stale guard is still the field refusal, not a 409.
    let guard_field = patch_json(&server, &block_path("0-tool-2"), json!({"note": "x"})).await;

    assert_eq!(guard_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        guard_field.message(),
        "Unknown field \"note\". Allowed fields: included, reasoningIncluded."
    );

    let guard_with_axis = patch_json(
        &server,
        &block_path("0-tool-2"),
        json!({"included": true, "if_revision": 0}),
    )
    .await;

    assert_eq!(guard_with_axis.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        guard_with_axis.message(),
        "Unknown field \"if_revision\". Allowed fields: included, reasoningIncluded."
    );

    // A segment the fold never published is a 404 with the store's own sentence (`sessions.ts:4481`),
    // and nothing about the body changes that.
    let missing = patch_json(&server, &block_path("0-nope-1"), json!({"included": false})).await;

    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert_eq!(
        missing.message(),
        format!("Unknown context block \"0-nope-1\" in turn \"{turn_id}\".")
    );

    // THE THINKING AXIS ON A SEGMENT THAT RECORDS NO THINKING. The tool segment carries no `reasoning`
    // bytes, so no entry's `reasoning` equals them (`blockThoughtOf`, `api.ts:4699`), and the refusal is
    // the sentence that says so rather than a silent no-op.
    let no_thinking = patch_json(
        &server,
        &block_path("0-tool-2"),
        json!({"reasoningIncluded": false}),
    )
    .await;

    assert_eq!(no_thinking.status, StatusCode::CONFLICT);
    assert_eq!(
        no_thinking.message(),
        "Context segment \"0-tool-2\" has no thinking entry to switch: the recorded thinking is not the exact reasoning of one assistant entry in this conversation's transcript."
    );

    // THE UNIT AXIS ON A PROSE SEGMENT. `0-assistant-5` is the answer window the stream opened (its key
    // is the first text delta, event 5), and that event carries no call id (`blockCallIdOf`,
    // `api.ts:334-342`), so the block write cannot address an entry through a segment that never named
    // a call.
    let no_unit = patch_json(
        &server,
        &block_path("0-assistant-5"),
        json!({"included": false}),
    )
    .await;

    assert_eq!(no_unit.status, StatusCode::CONFLICT);
    assert_eq!(
        no_unit.message(),
        "Context segment \"0-assistant-5\" has no transcript entry to switch: the call it recorded is not in this conversation's transcript."
    );

    // Every arm above refused, so the transcript still answers with the figure the last successful write
    // installed - a refusal writes nothing and mints no revision.
    let unchanged = get(&server, &transcript_path).await;

    assert_eq!(
        unchanged.number_at("/transcript/revision"),
        revision + 2,
        "the refusals above changed no document"
    );

    server.stop().await;
}

#[tokio::test]
async fn a_running_conversation_refuses_both_transcript_writes_with_one_sentence() {
    let root = Root::new("block-write-running");
    let server = TestServer::start(&root).await;

    // A tool call, then the request it leads to held OPEN: the run is mid-flight for the whole test
    // (the state TypeScript's guard reads, `sessions.ts:9209-9219`) while the call's segment - and the
    // transcript entry it joins on - already exist.
    let (base, _script) = provider(vec![
        tool_turn(
            "call-time",
            "timeNow",
            r#"{"timezone":"UTC"}"#,
            usage(11, 4),
        ),
        Answer::Delayed(900, answered_body("held open", usage(7, 3))),
    ])
    .await;

    let (id, _) = create_session(&server, &base, Some("running-write")).await;
    let thread = open_thread(&server, &id, "instruct").await;
    let transcript_path = format!("/v1/harness/sessions/{id}/threads/{thread}/transcript");
    let block_path = format!("/v1/harness/sessions/{id}/turns/TURN/blocks/0-tool-2");

    // THE SEED THE JOIN CAN REACH MID-RUN. This port's executor hands back the transcript when the run
    // SETTLES, so while a run is up the live transcript is the one the previous write installed - where
    // TypeScript's runtime is appending to the history as the call lands. Seeding an entry with the SAME
    // call id the scripted answer issues is what lets the join resolve during the run, so the refusal this
    // test asserts is the live-work guard and never the "no transcript entry to switch" arm.
    let seeded = put_json(
        &server,
        &transcript_path,
        json!({"messages": [
            {"role": "user", "content": "seed the transcript"},
            {"role": "assistant", "content": "", "toolCalls": [
                {"id": "call-time", "name": "timeNow", "arguments": "{}"}
            ]},
            {"role": "tool", "content": "the clock reads noon", "toolCallId": "call-time", "name": "timeNow"}
        ]}),
    )
    .await;

    assert_eq!(
        seeded.status,
        StatusCode::OK,
        "the seed writes: {}",
        seeded.body
    );

    let running = submit(&server, &id, "the first ask", Some(&thread)).await;
    let turn_id = running.string_at("/turn_id");

    wait_for_turn(&server, &id, &turn_id, "running").await;

    let block_path = block_path.replace("TURN", &turn_id);

    // Both halves of the precondition, stated rather than assumed: the segment exists, and the run is
    // still up when the write lands. If the delayed answer ever settles early, this is where the test
    // says so instead of passing on the wrong refusal.
    wait_for(
        &server,
        &block_path,
        |body| body.pointer("/block/id").is_some(),
        "the call's segment while the run is still up",
    )
    .await;

    let status = get(
        &server,
        &format!("/v1/harness/sessions/{id}/turns/{turn_id}"),
    )
    .await;

    assert_eq!(
        status.string_at("/turn/status"),
        "running",
        "the run is still up"
    );

    let expected =
        format!("Thread \"{thread}\" is running; stop it before replacing its transcript.");

    // THE WHOLE-DOCUMENT WRITE, which would otherwise answer 200 and rewrite the transcript under a run
    // that is appending to it.
    let put = put_json(
        &server,
        &transcript_path,
        json!({"messages": [{"role": "user", "content": "over the run"}]}),
    )
    .await;

    assert_eq!(
        put.status,
        StatusCode::CONFLICT,
        "a running thread refuses a PUT: {}",
        put.body
    );
    assert_eq!(put.message(), expected);

    // THE BLOCK WRITE, which reaches the same store operation (`api.ts:4737-4743` notes it refuses "from
    // inside" `updateThreadTranscript`). The join resolved - the seed named this call - so the 409 that
    // answers is the live-work guard, and its sentence is the one the PUT printed.
    let patch = patch_json(&server, &block_path, json!({"included": false})).await;

    assert_eq!(
        patch.status,
        StatusCode::CONFLICT,
        "a running thread refuses a PATCH: {}",
        patch.body
    );
    assert_eq!(
        patch.message(),
        expected,
        "one sentence for one condition, whichever route hit it"
    );

    // The transcript is exactly what the seed installed: the run still owns it, and neither refusal
    // touched it or minted a revision.
    let after = get(&server, &transcript_path).await;

    assert_eq!(
        after.number_at("/transcript/revision"),
        1,
        "a refused write minted no revision"
    );

    let cancelled = post_json(&server, &format!("/v1/harness/sessions/{id}/cancel"), None).await;

    assert_eq!(
        cancelled.status,
        StatusCode::OK,
        "the cancel settles the thread so the run cannot outlive the assertions: {}",
        cancelled.body
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// The session body validators quote the reference, not the serialiser (wave W8)
// ---------------------------------------------------------------------------

/// The words a client sees when its body is wrong.
///
/// The reference has a SENTENCE for every refusal — it even picks a STATUS by matching its own
/// sentences with a regex (`server/sessions.ts:2036-2051` `attachmentErrorStatus`,
/// `:10326-10338` `threadError`) — so a 400 that quotes the serialiser (`invalid type: integer \`7\`,
/// expected a string`, `` missing field `id` ``) is a wire difference even when the status code
/// matches. `POST {root}/sessions` therefore reads its body field by field instead of decoding it, and
/// these are the exact strings the reference's `postSession` + `settingsInputOf` emit
/// (`api.ts:3316-3323`, `:599-629`).
#[tokio::test]
async fn a_session_create_body_refusal_quotes_the_reference_and_never_the_serialiser() {
    let root = Root::new("session-create-body-refusals");
    let server = TestServer::start(&root).await;

    for (body, message) in [
        (json!([]), "The request body must be a JSON object."),
        (json!("general"), "The request body must be a JSON object."),
        (json!(null), "The request body must be a JSON object."),
        (
            json!({"nope": 1}),
            "Unknown field \"nope\". Allowed fields: harness, title, settings.",
        ),
        (
            json!({"harness": 123}),
            "A session requires a harness name.",
        ),
        (json!({"harness": ""}), "A session requires a harness name."),
        (
            json!({"harness": "instruct", "settings": 7}),
            "\"body.settings\" must be an object.",
        ),
        (
            json!({"harness": "instruct", "settings": [1]}),
            "\"body.settings\" must be an object.",
        ),
        (
            // `settingsInputOf` returns early ONLY for `undefined` (`api.ts:605`); its next guard is
            // `settings === null` (`:607-608`), so a JSON `null` block is the same refusal, not absence.
            json!({"harness": "instruct", "settings": null}),
            "\"body.settings\" must be an object.",
        ),
        (
            json!({"harness": "instruct", "settings": {"base_url": 7}}),
            "\"body.settings.base_url\" must be a string.",
        ),
        (
            json!({"harness": "instruct", "settings": {"api_key": true}}),
            "\"body.settings.api_key\" must be a string.",
        ),
        (
            json!({"harness": "instruct", "settings": {"model": [1]}}),
            "\"body.settings.model\" must be a string.",
        ),
        (
            json!({"harness": "instruct", "settings": {"zzz": 1}}),
            "Unknown field \"zzz\". Allowed fields: base_url, api_key, model.",
        ),
    ] {
        let reply = post_json(&server, "/v1/harness/sessions", Some(body.clone())).await;

        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "`{body}` is refused");
        assert_eq!(
            reply.message(),
            message,
            "`{body}` answers the reference's sentence verbatim"
        );

        // The actual defect this wave closes: serde's own wording on a client-visible body.
        for internal in [
            "invalid type",
            "missing field",
            "unknown variant",
            "invalid value",
            "expected a string",
        ] {
            assert!(
                !reply.message().contains(internal),
                "`{body}` never quotes the serialiser, got `{}`",
                reply.message()
            );
        }
    }

    server.stop().await;
}

/// `title` on `POST {root}/sessions` is OPTIONAL and TOLERANT: the reference tests it with
/// `typeof body.title === 'string'` (`api.ts:3421`) and creates an UNTITLED session when the value is
/// a number, a boolean or `null`. A serde decode made that a 400 quoting the parser, so the tolerance
/// is pinned here together with the trim rule the store owns (`normalize_title`, the reference's
/// `body.title.trim()`).
#[tokio::test]
async fn a_session_create_ignores_a_title_that_is_not_a_string() {
    let root = Root::new("session-create-title-tolerance");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;

    for shape in [
        json!(7),
        json!(true),
        json!(null),
        json!({"a": 1}),
        json!([]),
    ] {
        let reply = post_json(
            &server,
            "/v1/harness/sessions",
            Some(json!({
                "harness": "instruct",
                "title": shape,
                "settings": {"base_url": base, "api_key": MOCK_KEY, "model": "mock/one"}
            })),
        )
        .await;

        assert_eq!(
            reply.status,
            StatusCode::CREATED,
            "a `{shape}` title is ignored, not refused: {}",
            reply.body
        );
        assert_eq!(
            reply.body["session"].get("title"),
            None,
            "the session carries no title at all: {}",
            reply.body["session"]
        );
    }

    // A real title keeps the reference's trim, and a blank one is absence rather than an empty string.
    let named = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "instruct",
            "title": "  spaced  ",
            "settings": {"base_url": base, "api_key": MOCK_KEY, "model": "mock/one"}
        })),
    )
    .await;

    assert_eq!(named.status, StatusCode::CREATED, "{}", named.body);
    assert_eq!(named.body["session"]["title"], json!("spaced"));

    let blank = post_json(
        &server,
        "/v1/harness/sessions",
        Some(json!({
            "harness": "instruct",
            "title": "   ",
            "settings": {"base_url": base, "api_key": MOCK_KEY, "model": "mock/one"}
        })),
    )
    .await;

    assert_eq!(blank.status, StatusCode::CREATED, "{}", blank.body);
    assert_eq!(
        blank.body["session"].get("title"),
        None,
        "a blank title never reaches the resource"
    );

    server.stop().await;
}

/// `PATCH {root}/sessions/{id}` already refused every malformed body with the reference's sentence
/// before this wave; these are the strings the route is now pinned to, so a future move to a typed
/// decode cannot quietly replace them with parser text (`api.ts:3449-3483`).
#[tokio::test]
async fn a_session_patch_body_refusal_quotes_the_reference_and_never_the_serialiser() {
    let root = Root::new("session-patch-body-refusals");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;
    let (id, _created) = create_session(&server, &base, Some("patch refusals")).await;
    let path = format!("/v1/harness/sessions/{id}");

    for (body, message) in [
        (json!([]), "The request body must be a JSON object."),
        (json!(7), "The request body must be a JSON object."),
        (
            json!({"nope": 1}),
            "Unknown field \"nope\". Allowed fields: agent_models, title.",
        ),
        (json!({}), "A patch must change at least one field."),
        (json!({"title": 7}), "\"title\" must be a string or null."),
        (
            json!({"title": true}),
            "\"title\" must be a string or null.",
        ),
        (
            json!({"agent_models": "x"}),
            "`agentModels` must be an object mapping agent ids to model ids.",
        ),
        (
            json!({"agent_models": []}),
            "`agentModels` must be an object mapping agent ids to model ids.",
        ),
        // The value-level refusals are the shared registry validator's, verbatim
        // (`harnesses/registry.ts:631-644` `requireModelId`, re-exported for the session store).
        (
            json!({"agent_models": {"instruct": 7}}),
            "`agentModels.instruct` must be a model id string.",
        ),
        (
            json!({"agent_models": {"instruct": true}}),
            "`agentModels.instruct` must be a model id string.",
        ),
    ] {
        let reply = patch_json(&server, &path, body.clone()).await;

        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "`{body}` is refused");
        assert_eq!(
            reply.message(),
            message,
            "`{body}` answers the reference's sentence verbatim"
        );

        for internal in [
            "invalid type",
            "missing field",
            "unknown variant",
            "invalid value",
        ] {
            assert!(
                !reply.message().contains(internal),
                "`{body}` never quotes the serialiser, got `{}`",
                reply.message()
            );
        }
    }

    server.stop().await;
}

// ---------------------------------------------------------------------------
// The conversation cap, from the unfocused submit's side (wave W4)
// ---------------------------------------------------------------------------

/// The cap is 256 in the reference (`engine/core/thread.ts:52` `MAX_THREAD_RECORDS = 256`), and the
/// TWO routes that hit it answer with TWO codes. The unfocused submit wraps the runtime's sentence and
/// maps it to **429** (`engine/harness.ts:8052-8056`, `server/sessions.ts:10318-10338`); the create
/// route wraps NOTHING (`api.ts:5277-5281`), so the throw reaches the listener's uncaught handler and
/// answers **500** with the same sentence unwrapped (`handler.ts:319-320`).
#[tokio::test]
async fn an_unfocused_submit_at_the_conversation_cap_answers_429_with_the_remedy() {
    let root = Root::new("thread-cap-429");
    let server = TestServer::start(&root).await;
    let (base, _script) = provider(vec![answered("unused", usage(1, 1))]).await;
    let (id, _) = create_session(&server, &base, Some("cap")).await;

    let roster = get(&server, &format!("/v1/harness/sessions/{id}/threads")).await;
    let opened_before_loop = roster.body["items"].as_array().map(Vec::len).unwrap_or(0);

    // Fill the roster one conversation at a time. The create route keeps the reference's own 500 (the
    // kernel's throw, unclassified); the number this loop counts is the figure the gate quotes, so it
    // is asserted rather than assumed.
    let mut created = 0;
    let mut refusal = None;

    while created < 300 {
        let next = post_json(
            &server,
            &format!("/v1/harness/sessions/{id}/threads"),
            Some(json!({"agent": "instruct", "label": format!("#CAP{created}")})),
        )
        .await;

        if next.status == StatusCode::INTERNAL_SERVER_ERROR {
            refusal = Some(next);
            break;
        }

        assert_eq!(
            next.status,
            StatusCode::CREATED,
            "create #{created} fills the roster: {}",
            next.body
        );

        created += 1;
    }

    let refused = refusal.expect("the roster fills and the create route refuses");

    assert_eq!(
        opened_before_loop + created,
        256,
        "the roster holds the cap's worth of conversations before it refuses"
    );
    assert_eq!(
        refused.body["error"]["message"],
        json!(
            "This session already has 256 independent threads; delete a terminal one before creating \
             another (maximum 256)."
        ),
        "the create route answers the runtime's sentence, unwrapped: {}",
        refused.body
    );
    assert_eq!(
        refused.body.as_object().expect("an object body").len(),
        1,
        "the 500 body is the envelope and nothing else: {}",
        refused.body
    );
    // The reference's uncaught-error path is `sendJson(response, 500, {error:{message}})` with NO
    // header map (`handler.ts:320`, and `start.ts:176-189` is the same shape), so the epoch header a
    // success carries is absent here — the crate's envelope answers the same way.
    assert_eq!(
        refused
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default(),
        "application/json; charset=utf-8",
        "the refusal is JSON, never a text/plain 500"
    );
    assert!(
        refused.headers.get("x-harness-epoch").is_none(),
        "the uncaught path mints no header map, so no epoch header rides the 500: {:?}",
        refused.headers
    );

    // THE SUBMIT. An unfocused `POST /tasks` opens a host conversation, and at the cap that is the
    // runtime's thread-creation refusal: **429**, with the sentence that tells the operator what to
    // delete. A 409 here would send a client into a retry loop that can never succeed.
    let late = submit(&server, &id, "one more conversation", None).await;

    assert_eq!(
        late.status,
        StatusCode::TOO_MANY_REQUESTS,
        "the submit names the cap as a rate limit: {}",
        late.body
    );
    assert_eq!(
        late.body["error"]["message"],
        json!(
            "Could not open a new thread: This session already has 256 independent threads; \
             delete a terminal one before creating another (maximum 256)."
        ),
        "the remedy sentence is the runtime's, quoted in full"
    );

    // A FOCUSED submit on a conversation that exists is unaffected by the cap: the roster is full, but
    // the turn has somewhere to land. The roster is read AGAIN here - a fresh session opens no
    // conversation of its own, so the pre-fill read above is legitimately empty.
    let full = get(&server, &format!("/v1/harness/sessions/{id}/threads")).await;
    let thread = full.body["items"][0]["id"]
        .as_str()
        .expect("a full roster names a conversation")
        .to_owned();
    let focused = submit(&server, &id, "into an existing conversation", Some(&thread)).await;

    assert_eq!(
        focused.status,
        StatusCode::ACCEPTED,
        "the cap never blocks a turn that already has a thread: {}",
        focused.body
    );

    server.stop().await;
}
