//! Real-HTTP tests for the `/v1/harness` host surface (T5a).
//!
//! Every assertion goes over a TCP socket to an `axum::serve` instance built by
//! `typecrate_harness::server::router` - no in-process fake - because what this stage owns is
//! transport facts as much as logic facts: which headers are stamped, which body cap applies to
//! which route, what the mount/404/405 sentences say, and whether a refusal left the state alone.
//!
//! Pinned, in the stage brief's order:
//!
//! 1. the bootstrap document, and the 405 for a verb the root does not declare;
//! 2. health;
//! 3. a legacy `/api/...` prefix, which must NAME the canonical root rather than alias it;
//! 4. an unknown route under the mount, and the honest non-upgrade answer for `/stream`;
//! 5. `GET`/`PATCH /config`: the document shape, the null semantics, the three credential
//!    channels, the atomic-refusal rule, "no secret in `state.json`", and durability across a
//!    restart;
//! 6. the harness definition surface, including the tombstone a built-in delete leaves;
//! 7. the agent catalogue surface, including the instruction ladder an override moves;
//! 8. the tool catalogue reads and their filters;
//! 9. model discovery against a local provider: the parse rules, the credential scope, and the
//!    status every failure kind maps to;
//! 10. the body policy: the media-type rule, the per-route cap, the 16 MiB submit route, and the
//!     fact that a route which refuses first never reads a body;
//! 11. the Origin gate;
//! 12. the paging parameters and the `limit` echo.
//!
//! Fixtures live in `Temp/typecrate-harness/http-host/<pid>-<test>/` per the repo scratch rule, and
//! every test owns its own DATABASE ROOT: the runtime cache is keyed by root, so an isolated root is
//! what keeps the revision counters a parallel test asserts from being shared state.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::Response;
use axum::routing::get;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use typecrate_harness::agents::AgentCatalogue;
use typecrate_harness::config::{self, ServerConfig};
use typecrate_harness::contract;
use typecrate_harness::environment;
use typecrate_harness::events;
use typecrate_harness::paging;
use typecrate_harness::ports::ToolRuntime as _;
use typecrate_harness::runtime;
use typecrate_harness::server;
use typecrate_harness::tools::ToolRegistry;

/// Seconds a graceful shutdown may take before the test calls it hung. Every handler here is
/// sub-millisecond, so 5s only catches a real hang.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// The contract headers, lowercased the way `http::HeaderMap` stores them.
const EPOCH_HEADER: HeaderName = HeaderName::from_static("x-harness-epoch");
const REVISION_HEADER: HeaderName = HeaderName::from_static("x-harness-revision");

/// The five shipped presets, in the listing order `presets.ts:391-397` fixes (`instruct` first).
const SHIPPED_HARNESSES: [&str; 5] = ["instruct", "research", "workspace", "reviewer", "planner"];

/// The six trusted profiles, in catalogue order.
const SHIPPED_AGENTS: [&str; 6] = [
    "instruct",
    "research",
    "workspace",
    "reviewer",
    "planner",
    "summarizer",
];

/// The refusal `api-paging.ts:99` prints for every unusable `?limit=`.
const LIMIT_MESSAGE: &str = "Query parameter \"limit\" must be an integer between 1 and 100.";

// ---------------------------------------------------------------------------
// Fixtures and the test listener
// ---------------------------------------------------------------------------

/// A database root that starts empty and is removed on `Drop`.
///
/// The pid prefix separates concurrent `cargo test` processes and the name separates the tests
/// inside one process, so no two tests share a runtime cache entry, a `state.json`, or a journal.
struct Root {
    path: PathBuf,
}

impl Root {
    fn new(test: &str) -> Self {
        let path = typecrate_harness::package_root::normalize_lexically(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../../Temp/typecrate-harness/http-host/{}-{test}",
                std::process::id()
            )),
        );

        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path)
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", path.display()));

        Self { path }
    }

    /// `<root>/harness/state.json` (`persistence::state_file_path`).
    fn state_file(&self) -> PathBuf {
        self.path.join("harness").join("state.json")
    }

    /// `<root>/harness/agents.json` (`persistence::agent_file_path`).
    fn agent_file(&self) -> PathBuf {
        self.path.join("harness").join("agents.json")
    }

    /// The durable document, read from DISK. A test that asserts durability reads the file rather
    /// than making another round trip, because the point is that the bytes landed.
    fn state(&self) -> Value {
        let text = self.state_text();

        serde_json::from_str(&text).expect("state.json is JSON")
    }

    /// The raw document text, which is what "no secret anywhere in the file" is asserted against.
    fn state_text(&self) -> String {
        fs::read_to_string(self.state_file())
            .unwrap_or_else(|error| panic!("`{}` must exist: {error}", self.state_file().display()))
    }

    fn agent_records(&self) -> Value {
        let text = fs::read_to_string(self.agent_file()).unwrap_or_else(|error| {
            panic!("`{}` must exist: {error}", self.agent_file().display())
        });

        serde_json::from_str(&text).expect("agents.json is JSON")
    }

    /// The names on the durable registry rows, in document order.
    fn saved_registry_names(&self) -> Vec<String> {
        self.state()["registry"]
            .as_array()
            .expect("registry rows")
            .iter()
            .map(|row| row["name"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    /// The names one HTTP page carries, in page order. Every collection page names its row `name`
    /// (`HarnessDescriptor`, `AgentCard`, `ToolSummary`), which is what `SHIPPED_AGENTS` and the tool
    /// catalogue compare against.
    fn page_names(body: &Value) -> Vec<String> {
        Root::page_field(body, "name")
    }

    /// The same helper for a page whose rows are keyed differently. The instruction index is a list of
    /// `HarnessInstructionSummaryEntry` (`harness-api.ts`, `contract.rs:1206`), whose member field is
    /// `agent` - the row is about an agent inside a harness, not about a harness.
    fn page_field(body: &Value, field: &str) -> Vec<String> {
        body["items"]
            .as_array()
            .expect("an items array")
            .iter()
            .map(|row| row[field].as_str().unwrap_or_default().to_owned())
            .collect()
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// One harness listener on an ephemeral loopback port, serving the LIBRARY router - the same
/// function the executable binds, so a passing test is evidence about the real surface.
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
            // The receiver is awaited by value: tokio's `OneshotReceiver` polls itself in place.
            server::serve_on(listener, path, async move {
                let _ = receiver.await;
            })
            .await
            .expect("the harness listener serves until it is told to stop");
        });

        let base = format!("http://{local}");

        // One health probe proves the socket is accepting before any assertion runs. The listener
        // is bound before `serve_on` is polled, so the wait is a scheduling race, not a timeout.
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

    /// The `host:port` of this listener, for a same-origin `Origin` header.
    fn host(&self) -> String {
        self.base.trim_start_matches("http://").to_owned()
    }

    /// Stops the listener and waits for it, which is what makes the restart test's second
    /// `serve_on` the only writer of that root's document.
    async fn stop(mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }

        if let Some(task) = self.task.take() {
            tokio::time::timeout(SHUTDOWN_TIMEOUT, task)
                .await
                .expect("the listener stops on the shutdown signal")
                .expect("the serve task does not panic");
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // A test that never reaches `stop()` - an assertion fired first - still releases the socket
        // here, because dropping the sender ends the graceful-shutdown future.
        self.shutdown.take();
    }
}

/// A client with pooling disabled, so a keep-alive socket from one test cannot be reused against a
/// port another test has taken over.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_idle_timeout(Duration::ZERO)
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(60))
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

    /// Sends one request and reads the reply, so a refused write is asserted in one expression.
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
            .unwrap_or_else(|| {
                panic!(
                    "an `{{\"error\":{{\"message\":...}}}}` body, got `{}`",
                    self.body
                )
            })
    }

    fn header(&self, name: &HeaderName) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }

    /// The contract requires the epoch on every JSON response, so nearly every test asserts it on
    /// the way past.
    fn epoch(&self) -> String {
        self.header(&EPOCH_HEADER)
            .expect("X-Harness-Epoch on every JSON response")
    }
}

/// The headers every JSON response carries. `revision` is what `X-Harness-Revision` must say, or
/// `None` when the route has no revision to stamp (`api.ts:1224-1230`).
fn assert_contract_headers(reply: &Reply, revision: Option<u64>) {
    assert_eq!(
        reply.headers["content-type"].to_str().unwrap(),
        "application/json; charset=utf-8",
        "`handler.ts:86` sets the content type, charset included"
    );
    assert_eq!(
        reply.headers["cache-control"].to_str().unwrap(),
        "no-store",
        "a control-plane read is never cacheable"
    );

    let epoch = reply.epoch();
    let tail = epoch.rsplit('-').next().unwrap_or_default().to_owned();

    assert_eq!(
        tail.len(),
        12,
        "the epoch suffix is 12 base36 characters (`api-events.ts:65-80`): `{epoch}`"
    );
    assert!(
        tail.bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
        "the epoch suffix is `[0-9a-z]{{12}}`: `{epoch}`"
    );

    assert_eq!(
        reply.header(&REVISION_HEADER).is_some(),
        revision.is_some(),
        "X-Harness-Revision appears exactly when the route has a revision, and a receipt without \
         one carries none (`api.ts:1640-1643`)"
    );

    if let Some(expected) = revision {
        assert_eq!(
            reply.header(&REVISION_HEADER).unwrap(),
            expected.to_string(),
            "the header quotes the counter the route owns"
        );
    }
}

/// The boot document this process resolves from the environment: the reset target `PATCH /config`
/// nulls point at. Read through the library loader, because the CONTRACT rule under test is "a null
/// restores the boot pair" - not one machine's `HARNESS_*` values.
fn boot_config() -> ServerConfig {
    config::load_server_config(&environment::harness_runtime_env())
        .expect("the test host's HARNESS_* values are valid")
}

/// The `defaults` block the boot document must report.
fn boot_defaults(boot: &ServerConfig) -> Value {
    json!({
        "base_url": boot.base_url,
        "model": boot.model,
        "has_api_key": !boot.api_key.trim().is_empty(),
    })
}

// ---------------------------------------------------------------------------
// 1 - the bootstrap document
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_boot_document_advertises_the_contract() {
    let root = Root::new("boot");
    let server = TestServer::start(&root).await;

    let before = events::now_millis();
    let reply = Reply::send(client().get(server.url("/v1/harness"))).await;
    let after = events::now_millis();

    assert_eq!(reply.status, StatusCode::OK);
    // A boot document carries no revision: `api.ts:1316` calls `json(200, info)` without one.
    assert_contract_headers(&reply, None);

    let epoch = reply.epoch();

    assert_eq!(reply.body["api_root"], contract::HARNESS_API_ROOT);
    assert_eq!(
        reply.body["service"], "noobscript-harness",
        "`api.ts:1307` advertises the published service name"
    );
    assert_eq!(reply.body["api_version"], "v1");
    assert_eq!(reply.body["build_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(reply.body["epoch"], epoch.as_str());

    let server_time = reply.body["server_time"]
        .as_u64()
        .expect("server_time is a millisecond clock");
    assert!(
        (before..=after).contains(&server_time),
        "server_time is this process' clock: {server_time} not in {before}..={after}"
    );

    // `harness-api.ts:197-213`, verbatim: a limit that moves is a contract change.
    assert_eq!(
        reply.body["limits"],
        json!({
            "body_bytes": 1_000_000,
            "submit_body_bytes": 16_777_216,
            "input_chars": 200_000,
            "pending_tasks": 100,
            "thread_records": 256,
            "attachments_per_turn": 4,
            "attachment_bytes": 4_194_304,
            "total_attachment_bytes": 8_388_608,
            "result_references": 16,
            "pending_actions": 4,
            "steer_records": 64,
            "created_tools": 32,
            "tool_result_bytes": 65_536,
            "page_limit_default": 25,
            "page_limit_max": 100,
        })
    );

    // `harness-api.ts:216-226`: every advertised capability has a backing route in this build.
    assert_eq!(
        reply.body["capabilities"],
        json!({
            "websocket_stream": true,
            "tool_execution": true,
            "tool_registration": true,
            "transcript_editing": true,
            "command_dispatch": true,
            "model_discovery": true,
            "harness_definition_writes": true,
            "agent_writes": true,
            "persistent_threads": true,
        })
    );

    // The link map is keyed by route id and follows the mount (`api.ts:1293-1303`).
    assert_eq!(
        reply.body["links"],
        json!({
            "self": "/v1/harness",
            "health": "/v1/harness/health",
            "config": "/v1/harness/config",
            "harnesses": "/v1/harness/harnesses",
            "agents": "/v1/harness/agents",
            "tools": "/v1/harness/tools",
            "models": "/v1/harness/models",
            "sessions": "/v1/harness/sessions",
            "stream": "/v1/harness/stream",
        })
    );
    assert_eq!(reply.body["links"].as_object().expect("links").len(), 9);

    // A verb the route does not declare is a 405 naming the allowed list (`api.ts:4010-4013`).
    let wrong = Reply::send(client().post(server.url("/v1/harness"))).await;

    assert_eq!(wrong.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(wrong.message(), "Use GET for /v1/harness.");
    assert_contract_headers(&wrong, None);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 2 - health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_reports_the_live_session_count() {
    let root = Root::new("health");
    let server = TestServer::start(&root).await;

    let reply = Reply::send(client().get(server.url("/v1/harness/health"))).await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_contract_headers(&reply, None);

    // `api.ts:1319-1324`: exactly three members, and `sessions` is the live store count. Nothing a
    // T5a route can do creates a session, so the honest figure on a fresh root is 0.
    assert_eq!(
        reply.body,
        json!({ "ok": true, "sessions": 0, "epoch": reply.epoch() })
    );
    assert_eq!(reply.body.as_object().expect("a closed object").len(), 3);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 3 - the mount guard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_legacy_prefix_is_a_404_that_names_the_canonical_root() {
    let root = Root::new("legacy-mount");
    let server = TestServer::start(&root).await;

    // T13 removed the `/api` mount. An alias would keep the old surface alive behind a flag nobody
    // can see, so the answer names the root this server DOES serve (`api.ts:3987-3989`).
    for (path, mount) in [
        ("/api/sessions", "/api"),
        ("/api", "/api"),
        ("/", "/"),
        ("/health", "/health"),
        ("/v1/harnesss", "/v1"),
    ] {
        let reply = Reply::send(client().get(server.url(path))).await;

        assert_eq!(
            reply.status,
            StatusCode::NOT_FOUND,
            "`{path}` is not mounted"
        );
        assert_eq!(
            reply.message(),
            format!("This server serves /v1/harness; \"{mount}\" is not mounted.")
        );
        assert_contract_headers(&reply, None);
    }

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 4 - unknown routes, wrong verbs, and the stream mount
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unknown_route_names_the_request_and_the_stream_answers_400() {
    let root = Root::new("unknown");
    let server = TestServer::start(&root).await;

    for path in [
        "/v1/harness/nope",
        "/v1/harness/harnesses/instruct/nope",
        "/v1/harness/sessions/a/b/c/d/e",
        "/v1/harness/sessions/a/turns/b/nope",
    ] {
        let reply = Reply::send(client().get(server.url(path))).await;

        assert_eq!(
            reply.status,
            StatusCode::NOT_FOUND,
            "`{path}` has no template"
        );
        assert_eq!(
            reply.message(),
            format!("Unknown API route: GET {path}"),
            "the refusal quotes the method and the path the caller used"
        );
        assert_contract_headers(&reply, None);
    }

    // A path that exists but not for this verb lists EVERY method the route declares, in the order
    // the route table declares them (`api.ts:3925-3974`).
    let deleted = Reply::send(client().delete(server.url("/v1/harness/health"))).await;

    assert_eq!(deleted.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(deleted.message(), "Use GET for /v1/harness/health.");

    let sessions = Reply::send(client().put(server.url("/v1/harness/sessions"))).await;

    assert_eq!(sessions.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        sessions.message(),
        "Use GET, POST for /v1/harness/sessions."
    );

    let transcript = Reply::send(client().delete(server.url("/v1/harness/config"))).await;

    assert_eq!(transcript.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        transcript.message(),
        "Use GET, PATCH for /v1/harness/config."
    );

    // The stream mount: a plain GET that reaches the JSON dispatcher asked for an upgrade it did
    // not send (`api.ts:3906-3912`, the sentence `queue.test.ts:1090` pins).
    let stream = Reply::send(client().get(server.url("/v1/harness/stream"))).await;

    assert_eq!(stream.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        stream.message(),
        "GET /stream expects a WebSocket upgrade (Sec-WebSocket-Key/Version) for the harness.v1 subprotocol."
    );
    assert_contract_headers(&stream, None);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 5 - configuration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_config_document_reports_defaults_runtime_and_writable_fields() {
    let root = Root::new("config-shape");
    let server = TestServer::start(&root).await;
    let boot = boot_config();

    let reply = Reply::send(client().get(server.url("/v1/harness/config"))).await;

    assert_eq!(reply.status, StatusCode::OK);
    // A read stamps the counter it landed at, and a fresh runtime has never written config.
    assert_contract_headers(&reply, Some(0));

    assert_eq!(
        reply.body["writable_fields"],
        json!(["settings.base_url", "settings.model", "settings.api_key"]),
        "`api.ts:1332` - the only three writable paths over HTTP"
    );

    // Key-free by contract: the value never appears, only its presence.
    assert_eq!(reply.body["defaults"], boot_defaults(&boot));
    assert!(
        reply.body["defaults"].get("api_key").is_none(),
        "the credential is never serialized on the wire"
    );

    // The runtime block is deployment-owned and always present (`api-reads.ts:155-164`).
    assert_eq!(
        reply.body["runtime"],
        json!({
            "thread_execution": boot.thread_execution,
            "worker_idle_ms": boot.worker_idle_ms,
            "max_lane_workers": boot.max_lane_workers,
            "worker_handshake_ms": boot.worker_handshake_ms,
        })
    );
    assert_eq!(reply.body["revision"], 0);
    assert_eq!(reply.body.as_object().expect("a closed object").len(), 4);

    server.stop().await;
}

#[tokio::test]
async fn a_config_patch_moves_the_defaults_and_survives_a_restart() {
    let root = Root::new("config-patch");
    let server = TestServer::start(&root).await;
    let boot = boot_config();

    let patched = Reply::send(
        client()
            .patch(server.url("/v1/harness/config"))
            .json(&json!({
                "settings": {
                    "base_url": "http://provider.test/v1",
                    "model": "provider/model-x",
                }
            })),
    )
    .await;

    assert_eq!(patched.status, StatusCode::OK);
    // The document comes back on the NEW revision, because the route bumps first and THEN builds it
    // (`api.ts:1457-1461`).
    assert_contract_headers(&patched, Some(1));
    assert_eq!(patched.body["revision"], 1);
    assert_eq!(
        patched.body["defaults"],
        json!({
            "base_url": "http://provider.test/v1",
            "model": "provider/model-x",
            "has_api_key": !boot.api_key.trim().is_empty(),
        })
    );

    let again = Reply::send(client().get(server.url("/v1/harness/config"))).await;

    assert_eq!(
        again.body["defaults"]["base_url"],
        "http://provider.test/v1"
    );
    assert_eq!(
        again.body["revision"], 1,
        "a read never advances the counter"
    );

    // DURABILITY, from the file: exactly the two keys TS writes, with `baseURL` spelled capital-URL
    // (`sessions.ts:2691`), and no tombstone list while nothing was deleted (`:2688`).
    let state = root.state();

    assert_eq!(
        state["globalSettings"],
        json!({ "baseURL": "http://provider.test/v1", "model": "provider/model-x" })
    );
    assert_eq!(state["version"], 1);
    assert_eq!(state["sessions"], json!([]));
    assert_eq!(
        root.saved_registry_names(),
        SHIPPED_HARNESSES
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>(),
        "`sessions.ts:2681` writes every registry entry, unmodified built-ins included"
    );
    assert!(
        state.get("deleted").is_none(),
        "an empty tombstone list stays absent"
    );
    assert!(
        !root.state_text().contains("apiKey"),
        "the document has no credential member at all (`persistence.ts:1241` refuses one)"
    );

    server.stop().await;

    // A SECOND runtime over the same root - the restart the per-root cache would otherwise hide -
    // must read the saved pair off the disk.
    runtime::evict_runtime(&root.path);
    let restarted = TestServer::start(&root).await;

    let after_restart = Reply::send(client().get(restarted.url("/v1/harness/config"))).await;

    assert_eq!(
        after_restart.body["defaults"],
        json!({
            "base_url": "http://provider.test/v1",
            "model": "provider/model-x",
            "has_api_key": !boot.api_key.trim().is_empty(),
        }),
        "a saved `PATCH /config` result is applied at construction (`api.ts:840-849`)"
    );
    assert_eq!(
        after_restart.body["revision"], 0,
        "the counter is per-process, which is exactly why a cursor is only comparable inside one \
         epoch (`harness-api.ts:15-23`)"
    );

    restarted.stop().await;
}

#[tokio::test]
async fn config_null_semantics_and_the_three_credential_channels() {
    let root = Root::new("config-credential");
    let server = TestServer::start(&root).await;
    let boot = boot_config();
    let boot_has_key = !boot.api_key.trim().is_empty();

    let patch = |body: Value| {
        let url = server.url("/v1/harness/config");
        async move { Reply::send(client().patch(url).json(&body)).await }
    };

    // 1. A base URL move does NOT move the credential's home: `api.ts:831-833` states the scope
    //    follows only an explicit `api_key` write, so the advertised claim is unchanged.
    let moved = patch(json!({"settings": {"base_url": "http://elsewhere.test/v1"}})).await;

    assert_eq!(moved.status, StatusCode::OK);
    assert_eq!(
        moved.body["defaults"]["base_url"],
        "http://elsewhere.test/v1"
    );
    assert_eq!(
        moved.body["defaults"]["has_api_key"], boot_has_key,
        "a `base_url` patch changes where sessions GO, not what this process holds"
    );

    // 2. An explicit key issues a NEW process-only credential, and the claim follows it.
    let issued = patch(json!({"settings": {"api_key": "sk-test-credential"}})).await;

    assert_eq!(issued.status, StatusCode::OK);
    assert_eq!(issued.body["defaults"]["has_api_key"], true);

    // 3. The key is never echoed, and it never reaches the document.
    assert!(issued.body["defaults"].get("api_key").is_none());
    assert!(
        !root.state_text().contains("sk-test-credential"),
        "state.json holds no secret, so an api key set over HTTP stays process-only"
    );

    // 4. An explicit empty string means "no key" - the credential rule and the advertisement both
    //    enforce it (`api.ts:1441-1445`).
    let cleared = patch(json!({"settings": {"api_key": ""}})).await;

    assert_eq!(cleared.body["defaults"]["has_api_key"], false);

    // 5. `null` restores the SHIPPED credential together with the provider it belongs to - not
    //    "no key" (`api.ts:1428-1431`).
    let reset = patch(json!({"settings": {"api_key": null}})).await;

    assert_eq!(
        reset.body["defaults"]["has_api_key"], boot_has_key,
        "a null key write restores the boot claim, whatever the boot claim was"
    );

    // 6. `null` on the endpoint pair restores the BOOT document, not the previous patch
    //    (`api.ts:1384-1385`, whose reset target is `defaults`).
    let restored = patch(json!({"settings": {"base_url": null, "model": null}})).await;

    assert_eq!(restored.body["defaults"], boot_defaults(&boot));
    assert_eq!(
        root.state()["globalSettings"],
        json!({ "baseURL": boot.base_url, "model": boot.model }),
        "the restored pair is what the next restart boots with"
    );

    server.stop().await;
}

#[tokio::test]
async fn every_refused_config_patch_leaves_the_settings_untouched() {
    let root = Root::new("config-atomic");
    let server = TestServer::start(&root).await;
    let boot = boot_config();

    // The T39 regression: a write-as-you-go loop answered 400 for a late key while the EARLIER keys
    // had already moved the live defaults, and an unbounded `base_url` poisoned every later
    // `POST /sessions`. Every case below must be a refusal AND a no-op.
    let long_url = "http://provider.test/".repeat(200);
    let long_model = "m".repeat(config::MAX_MODEL_LENGTH + 1);

    assert_eq!(long_url.chars().count(), 4_200);
    assert!(long_url.chars().count() > config::MAX_BASE_URL_LENGTH);

    let cases: Vec<(Value, u16, &str)> = vec![
        (
            json!({ "title": "x" }),
            400,
            "Unknown field \"title\". Allowed fields: settings.",
        ),
        (
            json!({}),
            400,
            "A config patch requires a \"settings\" object.",
        ),
        (
            json!({ "settings": {} }),
            400,
            "A config patch must change at least one setting.",
        ),
        (
            json!({ "settings": null }),
            400,
            "A config patch requires a \"settings\" object.",
        ),
        (
            json!({ "settings": [] }),
            400,
            "A config patch requires a \"settings\" object.",
        ),
        (
            // A deployment-only knob stays enforceable rather than being prose: the allowed set
            // refuses it by name (`api.ts:1376-1378`).
            json!({ "settings": { "thread_execution": "worker" } }),
            400,
            "Unknown field \"thread_execution\". Allowed fields: base_url, model, api_key.",
        ),
        (
            json!({ "settings": { "worker_idle_ms": 5 } }),
            400,
            "Unknown field \"worker_idle_ms\". Allowed fields: base_url, model, api_key.",
        ),
        (
            json!({ "settings": { "base_url": 5 } }),
            400,
            "\"settings.base_url\" must be a string or null.",
        ),
        (
            json!({ "settings": { "api_key": true } }),
            400,
            "\"settings.api_key\" must be a string or null.",
        ),
        (
            json!({ "settings": { "base_url": long_url.clone() } }),
            400,
            "A provider base URL cannot exceed 2048 characters.",
        ),
        (
            json!({ "settings": { "model": long_model } }),
            400,
            "A model name cannot exceed 256 characters.",
        ),
        (
            json!({ "settings": { "base_url": "   " } }),
            400,
            "A provider base URL cannot be empty.",
        ),
        (
            // The second defect: the refusal is for the FIRST key, but `model` was already
            // assigned by the time it ran. A refused patch must move nothing.
            json!({ "settings": { "base_url": "", "model": "moved-by-a-refused-patch" } }),
            400,
            "A provider base URL cannot be empty.",
        ),
        (json!([]), 400, "The request body must be a JSON object."),
        (
            json!("text"),
            400,
            "The request body must be a JSON object.",
        ),
        (json!(7), 400, "The request body must be a JSON object."),
        (Value::Null, 400, "The request body must be a JSON object."),
    ];

    for (body, status, message) in &cases {
        let reply = Reply::send(client().patch(server.url("/v1/harness/config")).json(body)).await;

        assert_eq!(
            reply.status.as_u16(),
            *status,
            "`{body}` answers {status}: got `{}`",
            reply.body
        );
        assert_eq!(reply.message(), *message, "`{body}` states the refusal");
        assert_eq!(
            reply.header(&REVISION_HEADER),
            None,
            "a refusal never carries a revision"
        );
    }

    // Nothing moved: the same defaults, the same revision, and no document written by a refused
    // patch.
    let after = Reply::send(client().get(server.url("/v1/harness/config"))).await;

    assert_eq!(after.body["defaults"], boot_defaults(&boot));
    assert_eq!(
        after.body["revision"], 0,
        "a refused patch never bumps the counter"
    );
    assert!(
        !root.path.join("harness").join("state.json").exists(),
        "every write in this test was refused, so nothing was committed"
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 6 - harness definitions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_definition_catalogue_is_listed_read_and_described() {
    let root = Root::new("harness-read");
    let server = TestServer::start(&root).await;

    let page = Reply::send(client().get(server.url("/v1/harness/harnesses"))).await;

    assert_eq!(page.status, StatusCode::OK);
    assert_contract_headers(&page, Some(0));
    assert_eq!(
        Root::page_names(&page.body),
        SHIPPED_HARNESSES,
        "the shipped catalogue, in `presets.ts:391-397` order"
    );
    // Five rows on one page, and `limit` echoes the EFFECTIVE size (`api-paging.ts:142-153`).
    assert_eq!(page.body["limit"], 5);
    assert_eq!(page.body["next_cursor"], Value::Null);
    assert_eq!(page.body["revision"], 0);
    assert!(page.body["observed_at"].is_u64());

    // The filter is a case-insensitive substring over `name title description` joined with single
    // spaces (`api.ts:1494-1499`), and `?q=` is absent, not empty (`paramOf`).
    for (query, expected) in [
        ("q=research", vec!["research", "planner"]),
        ("q=REVIEW", vec!["reviewer", "planner"]),
        ("q=Coder", vec!["workspace", "planner"]),
        ("q=Skeptical", vec!["reviewer"]),
        // `summarizer` is a MEMBER of every live harness but appears in none of the three
        // fields the filter reads, which is what proves the field set (`api.ts:1497-1499`).
        ("q=summarizer", Vec::<&str>::new()),
        ("q=", SHIPPED_HARNESSES.to_vec()),
        ("q=nothing-matches-this", Vec::<&str>::new()),
    ] {
        let filtered =
            Reply::send(client().get(server.url(&format!("/v1/harness/harnesses?{query}")))).await;

        assert_eq!(
            filtered.status,
            StatusCode::OK,
            "`{query}` is a filter, not a refusal"
        );
        assert_eq!(
            Root::page_names(&filtered.body),
            expected,
            "`{query}` selects exactly this set"
        );
    }

    // The identified read: a card plus the per-agent maps, the resolved core tools, and the
    // instruction SUMMARY (`api.ts:1474-1486`).
    let one = Reply::send(client().get(server.url("/v1/harness/harnesses/instruct"))).await;

    assert_eq!(one.status, StatusCode::OK);
    assert_contract_headers(&one, Some(0));

    let harness = &one.body["harness"];
    let instruct = harness.as_object().expect("a flat card");

    assert_eq!(harness["name"], "instruct");
    assert_eq!(harness["title"], "Instruct");
    assert_eq!(harness["primary"], "instruct");
    assert_eq!(harness["builtin"], true);
    assert_eq!(harness["requires_endpoint"], true);
    assert_eq!(harness["definition_revision"], 0);
    assert_eq!(
        harness["subagents"],
        json!(["research", "workspace", "reviewer", "planner", "summarizer"]),
        "`team_of(primary)` is the other roles, then the summarizer (`presets.rs:118-125`)"
    );
    assert_eq!(
        harness["agents"],
        json!([
            "instruct",
            "research",
            "workspace",
            "reviewer",
            "planner",
            "summarizer"
        ]),
        "`agents` is `[primary, ...subagents]`"
    );
    assert!(harness["core_tools"].is_array());
    assert!(
        harness.get("agent_models").is_none(),
        "the map is emitted only when the harness overrides something"
    );
    assert_eq!(
        harness["instruction_summary"]
            .as_array()
            .expect("rows")
            .len(),
        6
    );
    assert_eq!(harness["instruction_summary"][0]["source"], "catalogue");
    assert_eq!(
        instruct.keys().cloned().collect::<Vec<_>>().len(),
        12,
        "a card's ten members, plus the two only a definition read adds"
    );

    // The instruction index and one member's text (`api.ts:1646-1678`).
    let index =
        Reply::send(client().get(server.url("/v1/harness/harnesses/instruct/instructions"))).await;
    let text = Reply::send(
        client().get(server.url("/v1/harness/harnesses/instruct/instructions/research")),
    )
    .await;

    let catalogue = AgentCatalogue::builtin();
    let research = catalogue.effective("research").expect("a shipped profile");
    let research_chars = u64::try_from(research.instruction.chars().count()).unwrap();

    assert_eq!(index.status, StatusCode::OK);
    assert_contract_headers(&index, None);
    assert_eq!(index.body["harness_name"], "instruct");
    assert_eq!(index.body["total"], 6);
    assert_eq!(
        Root::page_field(&index.body, "agent"),
        SHIPPED_AGENTS,
        "an instruction index row is keyed `agent`, not `name`"
    );
    assert_eq!(
        index.body["items"][0],
        json!({
            "agent": "instruct",
            "source": "catalogue",
            "overridden": false,
            "chars": u64::try_from(
                catalogue
                    .effective("instruct")
                    .expect("a shipped profile")
                    .instruction
                    .chars()
                    .count()
            )
            .unwrap(),
        }),
        "one row per member, with provenance and LENGTH - never the text"
    );
    assert_eq!(
        index.body["items"][1],
        json!({
            "agent": "research",
            "source": "catalogue",
            "overridden": false,
            "chars": research_chars,
        })
    );

    assert_eq!(text.status, StatusCode::OK);
    assert_contract_headers(&text, None);
    assert_eq!(
        text.body,
        json!({
            "harness_name": "instruct",
            "agent": "research",
            "source": "catalogue",
            "instruction": research.instruction,
            "guidelines": research.guidelines,
            "chars": research_chars,
        })
    );

    // An unknown harness, and a member this definition does not select.
    let missing_harness = Reply::send(client().get(server.url("/v1/harness/harnesses/nope"))).await;

    assert_eq!(missing_harness.status, StatusCode::NOT_FOUND);
    assert_eq!(missing_harness.message(), "Unknown harness \"nope\".");

    let not_member =
        Reply::send(client().get(server.url("/v1/harness/harnesses/instruct/instructions/ghost")))
            .await;

    assert_eq!(not_member.status, StatusCode::NOT_FOUND);
    assert_eq!(
        not_member.message(),
        "Agent \"ghost\" is not a member of harness \"instruct\"."
    );

    server.stop().await;
}

#[tokio::test]
async fn a_harness_write_round_trips_persists_and_deletes() {
    let root = Root::new("harness-write");
    let server = TestServer::start(&root).await;

    let created = Reply::send(
        client()
            .post(server.url("/v1/harness/harnesses"))
            .json(&json!({
                "name": "t5a-suite",
                "title": "T5a Suite",
                "description": "A harness this test owns.",
                "primary": "planner",
                "subagents": ["research"],
                "agent_models": { "research": "provider/model-y" },
                "agent_instructions": { "research": "Suite-specific prose." },
            })),
    )
    .await;

    assert_eq!(created.status, StatusCode::CREATED);
    assert_contract_headers(&created, Some(1));
    assert_eq!(created.body["harness"]["name"], "t5a-suite");
    assert_eq!(created.body["harness"]["builtin"], false);
    assert_eq!(created.body["harness"]["primary"], "planner");
    assert_eq!(
        created.body["harness"]["agents"],
        json!(["planner", "research"]),
        "the created membership is `[primary, ...subagents]`"
    );
    assert_eq!(
        created.body["harness"]["agent_models"],
        json!({ "research": "provider/model-y" })
    );
    assert_eq!(created.body["harness"]["definition_revision"], 1);

    // The duplicate refusal is the registry's, and it is a CONFLICT (`api.ts:1534-1535`).
    let duplicate = Reply::send(
        client()
            .post(server.url("/v1/harness/harnesses"))
            .json(&json!({ "name": "t5a-suite", "primary": "planner" })),
    )
    .await;

    assert_eq!(duplicate.status, StatusCode::CONFLICT);
    assert_eq!(duplicate.message(), "Harness \"t5a-suite\" already exists.");

    // An unknown primary is a 400 that lists the known names: the registry's selectability gate.
    let unknown_primary = Reply::send(
        client()
            .post(server.url("/v1/harness/harnesses"))
            .json(&json!({ "name": "t5a-ghost", "primary": "ghost" })),
    )
    .await;

    assert_eq!(unknown_primary.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unknown_primary.message(),
        "`primary` names unknown agent \"ghost\". Known agents: instruct, research, workspace, reviewer, planner, summarizer."
    );

    // The refusals this ROUTE owns, all before any typed decode (`api.ts:1515-1532`).
    for (body, message) in [
        (
            json!({ "primary": "planner" }),
            "A harness definition requires a \"name\".",
        ),
        (
            json!({ "name": "t5a-no-primary" }),
            "A harness definition requires a \"primary\" agent.",
        ),
        (
            json!({ "name": "t5a bad!", "primary": "planner" }),
            "`name` must start with a letter and contain only letters, digits, \"_\" or \"-\", at most 64 characters.",
        ),
        (
            json!({ "name": "x", "primary": "planner", "color": "red" }),
            "Unknown field \"color\". Allowed fields: name, title, description, primary, subagents, agent_models, agent_instructions.",
        ),
        (json!([]), "The request body must be a JSON object."),
    ] {
        let reply = Reply::send(
            client()
                .post(server.url("/v1/harness/harnesses"))
                .json(&body),
        )
        .await;

        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "`{body}` is refused");
        assert_eq!(reply.message(), message);
    }

    // PATCH moves the card and the counters; the row is durable.
    let patched = Reply::send(
        client()
            .patch(server.url("/v1/harness/harnesses/t5a-suite"))
            .json(&json!({ "title": "Renamed Suite" })),
    )
    .await;

    assert_eq!(patched.status, StatusCode::OK);
    assert_contract_headers(&patched, Some(2));
    assert_eq!(patched.body["harness"]["title"], "Renamed Suite");
    assert_eq!(patched.body["harness"]["definition_revision"], 2);

    let empty_patch = Reply::send(
        client()
            .patch(server.url("/v1/harness/harnesses/t5a-suite"))
            .json(&json!({})),
    )
    .await;

    assert_eq!(empty_patch.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty_patch.message(),
        "A patch must change at least one field."
    );

    let unknown_patch = Reply::send(
        client()
            .patch(server.url("/v1/harness/harnesses/ghost"))
            .json(&json!({ "title": "x" })),
    )
    .await;

    assert_eq!(unknown_patch.status, StatusCode::NOT_FOUND);
    assert_eq!(unknown_patch.message(), "Unknown harness \"ghost\".");

    let state = root.state();

    assert_eq!(
        root.saved_registry_names(),
        [
            "instruct".to_string(),
            "research".to_string(),
            "workspace".to_string(),
            "reviewer".to_string(),
            "planner".to_string(),
            "t5a-suite".to_string(),
        ],
        "a created entry joins the durable list in creation order"
    );
    let saved = state["registry"]
        .as_array()
        .expect("registry rows")
        .iter()
        .find(|row| row["name"] == "t5a-suite")
        .expect("the created row is durable");

    assert_eq!(saved["title"], "Renamed Suite");
    assert_eq!(saved["builtin"], false);
    assert_eq!(saved["editable"], true);
    assert_eq!(saved["primary"], "planner");
    assert_eq!(saved["agents"], json!(["planner", "research"]));
    assert!(
        state.get("deleted").is_none(),
        "no built-in has been removed yet"
    );

    // DELETE: a user row leaves nothing behind, so `tombstoned` is false.
    let deleted = Reply::send(client().delete(server.url("/v1/harness/harnesses/t5a-suite"))).await;

    assert_eq!(deleted.status, StatusCode::OK);
    assert_eq!(
        deleted.body,
        json!({
            "deleted": true,
            "harness_name": "t5a-suite",
            "tombstoned": false,
            "restored_by_restart": false,
        })
    );
    assert_eq!(
        deleted.header(&REVISION_HEADER),
        None,
        "a delete receipt carries no revision member, so the header is absent (`api.ts:1640-1643`)"
    );

    let gone = Reply::send(client().get(server.url("/v1/harness/harnesses/t5a-suite"))).await;

    assert_eq!(gone.status, StatusCode::NOT_FOUND);
    assert_eq!(gone.message(), "Unknown harness \"t5a-suite\".");

    // A built-in delete DOES tombstone, so this restart does not re-ship the card.
    let shipped = Reply::send(client().delete(server.url("/v1/harness/harnesses/planner"))).await;

    assert_eq!(shipped.status, StatusCode::OK);
    assert_eq!(
        shipped.body,
        json!({
            "deleted": true,
            "harness_name": "planner",
            "tombstoned": true,
            "restored_by_restart": false,
        })
    );
    assert_eq!(root.state()["deleted"], json!(["planner"]));

    let removed = Reply::send(client().get(server.url("/v1/harness/harnesses/planner"))).await;

    assert_eq!(removed.status, StatusCode::NOT_FOUND);

    let already = Reply::send(client().delete(server.url("/v1/harness/harnesses/planner"))).await;

    assert_eq!(already.status, StatusCode::NOT_FOUND);
    assert_eq!(
        already.message(),
        "Harness \"planner\" has already been deleted."
    );

    // An alias is a READ redirect, never an editable identity (`knowledge.md` T2: "`get` is direct,
    // `find`/`resolve` follow an alias", because "PATCH/DELETE /harnesses/writer must never rewrite
    // instruct").
    let alias_get = Reply::send(client().get(server.url("/v1/harness/harnesses/writer"))).await;

    assert_eq!(alias_get.status, StatusCode::OK);
    assert_eq!(alias_get.body["harness"]["name"], "instruct");

    let alias_delete =
        Reply::send(client().delete(server.url("/v1/harness/harnesses/writer"))).await;

    assert_eq!(alias_delete.status, StatusCode::NOT_FOUND);
    assert_eq!(
        alias_delete.message(),
        "Unknown harness \"writer\".",
        "`remove` is direct-name only, so an alias cannot be deleted"
    );

    server.stop().await;

    runtime::evict_runtime(&root.path);
    let restarted = TestServer::start(&root).await;

    let still_gone = Reply::send(client().get(restarted.url("/v1/harness/harnesses"))).await;

    assert_eq!(
        Root::page_names(&still_gone.body),
        ["instruct", "research", "workspace", "reviewer"],
        "the tombstone is durable, so a restart does NOT re-ship the removed card"
    );

    restarted.stop().await;
}

// ---------------------------------------------------------------------------
// 7 - global agents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_agent_surface_lists_reads_persists_and_retires() {
    let root = Root::new("agents");
    let server = TestServer::start(&root).await;
    let catalogue = AgentCatalogue::builtin();

    let page = Reply::send(client().get(server.url("/v1/harness/agents"))).await;

    assert_eq!(page.status, StatusCode::OK);
    assert_contract_headers(&page, Some(0));
    assert_eq!(
        Root::page_names(&page.body),
        SHIPPED_AGENTS,
        "the SELECTABLE profiles, in catalogue order (`api-reads.ts` `registry.agents()`)"
    );
    assert_eq!(page.body["limit"], 6);
    assert_eq!(page.body["next_cursor"], Value::Null);

    for row in page.body["items"].as_array().expect("items") {
        let name = row["name"].as_str().expect("a name");
        let profile = catalogue.effective(name).expect("a shipped profile");

        assert_eq!(row["title"], profile.title);
        assert_eq!(row["description"], profile.description);
        assert_eq!(row["has_instruction_override"], false);
        assert_eq!(row["has_global_override"], false);
        assert_eq!(
            row.as_object().expect("a closed row").len(),
            5,
            "`harness-api.ts:557` is the whole row"
        );
    }

    let one = Reply::send(client().get(server.url("/v1/harness/agents/instruct"))).await;

    assert_eq!(one.status, StatusCode::OK);
    assert_contract_headers(&one, Some(0));

    let profile = &one.body["agent"];
    let shipped = catalogue.effective("instruct").expect("a shipped profile");

    assert_eq!(profile["name"], "instruct");
    assert_eq!(profile["title"], shipped.title);
    assert_eq!(profile["instruction"], shipped.instruction);
    assert_eq!(profile["guidelines"], json!(shipped.guidelines));
    assert_eq!(profile["instruction_source"], "catalogue");
    assert!(
        profile.get("overridden_fields").is_none(),
        "the list appears only when the global layer replaces something"
    );
    assert_eq!(profile["revision"], 0);
    assert_eq!(
        profile["referenced_by"],
        json!(["instruct", "research", "workspace", "reviewer", "planner"]),
        "only the harness registry knows which definitions select this profile"
    );

    // PATCH moves the profile, the ladder, and the disk.
    let patched = Reply::send(
        client()
            .patch(server.url("/v1/harness/agents/instruct"))
            .json(&json!({ "instruction": "Overridden by the T5a test." })),
    )
    .await;

    assert_eq!(patched.status, StatusCode::OK);
    // `agents` is a HOST kind (`api-events.ts:99-105`), so `bumpRevision('agents')` and
    // `bumpRevision('agents', name)` (`api.ts:1763-1764`) land on the SAME counter: one write moves
    // it by two, and the header quotes the second value. This is why the first edit reports 2, not 1.
    assert_contract_headers(&patched, Some(2));
    assert_eq!(
        patched.body["agent"],
        json!({
            "name": "instruct",
            "title": shipped.title,
            "description": shipped.description,
            "instruction": "Overridden by the T5a test.",
            "guidelines": json!(shipped.guidelines),
            "instruction_source": "global",
            "overridden_fields": ["instruction"],
            "referenced_by": [
                "instruct",
                "research",
                "workspace",
                "reviewer",
                "planner"
            ],
            "revision": 2,
        })
    );

    let index =
        Reply::send(client().get(server.url("/v1/harness/harnesses/instruct/instructions"))).await;

    assert_eq!(
        index.body["items"][0],
        json!({
            "agent": "instruct",
            "source": "global",
            // `overridden` is the HARNESS layer only; the global override shows up in `source`.
            "overridden": false,
            // `chars` is the text's UTF-16 length (`instructionSummaryOf`, `api-reads.ts`): the
            // override "Overridden by the T5a test." is 27 units, and an index row taken from a
            // construction-time snapshot would still report the shipped profile's text.
            "chars": 27,
        }),
        "the definition read resolves a member's text through the global layer, live \
         (`api.ts:901-909`: a snapshot would report the pre-edit text for the life of the process)"
    );

    let records = root.agent_records();

    assert_eq!(
        records["records"]
            .as_array()
            .expect("records")
            .iter()
            .map(|row| (
                row["name"].as_str().unwrap_or_default().to_owned(),
                row["instruction"].as_str().unwrap_or_default().to_owned(),
            ))
            .collect::<Vec<_>>(),
        vec![(
            "instruct".to_string(),
            "Overridden by the T5a test.".to_string()
        )],
        "`agents.json` stores ONLY what the operator changed (`persistence.rs:233-239`)"
    );

    // A `null` restores the shipped value, which also removes the decision from the record list.
    let restored = Reply::send(
        client()
            .patch(server.url("/v1/harness/agents/instruct"))
            .json(&json!({ "instruction": null })),
    )
    .await;

    assert_eq!(restored.status, StatusCode::OK);
    assert_eq!(restored.body["agent"]["instruction"], shipped.instruction);
    assert_eq!(restored.body["agent"]["instruction_source"], "catalogue");
    // The second write on the same collapsed host counter (`api.ts:1763-1764`).
    assert_eq!(restored.body["agent"]["revision"], 4);
    assert_eq!(
        root.agent_records()["records"]
            .as_array()
            .expect("records")
            .len(),
        0,
        "an override equal to the shipped value is DROPPED from the decision list \
         (`agent-state.ts:274-290`)"
    );

    // The body policy: an empty patch, an unknown field, and an unknown id.
    let empty = Reply::send(
        client()
            .patch(server.url("/v1/harness/agents/instruct"))
            .json(&json!({})),
    )
    .await;

    assert_eq!(empty.status, StatusCode::BAD_REQUEST);
    assert_eq!(empty.message(), "A patch must change at least one field.");

    let unknown_field = Reply::send(
        client()
            .patch(server.url("/v1/harness/agents/instruct"))
            .json(&json!({ "color": "red" })),
    )
    .await;

    assert_eq!(unknown_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unknown_field.message(),
        "Unknown field \"color\". Allowed fields: title, description, instruction, guidelines."
    );

    let unknown_agent = Reply::send(
        client()
            .patch(server.url("/v1/harness/agents/ghost"))
            .json(&json!({ "title": "x" })),
    )
    .await;

    assert_eq!(unknown_agent.status, StatusCode::NOT_FOUND);
    assert_eq!(unknown_agent.message(), "Unknown agent \"ghost\".");

    // Retirement. TypeScript does NOT refuse a profile a harness still selects: `api.ts:1799-1810`
    // states what retirement means (no NEW selection), and `referenced_by` is the REPORT field that
    // tells a client what is stranded. See `knowledge.md` "T5a" for the brief's 409 question.
    let retired = Reply::send(client().delete(server.url("/v1/harness/agents/summarizer"))).await;

    assert_eq!(retired.status, StatusCode::OK);
    // The third write on this host counter: 5 for the collection bump, 6 for the row bump.
    assert_contract_headers(&retired, Some(6));
    assert_eq!(
        retired.body,
        json!({
            "deleted": true,
            "agent_id": "summarizer",
            "tombstoned": true,
            "restored_by_restart": false,
        })
    );

    let listed = Reply::send(client().get(server.url("/v1/harness/agents"))).await;

    assert_eq!(
        Root::page_names(&listed.body),
        ["instruct", "research", "workspace", "reviewer", "planner"],
        "a retired profile leaves the selectable page while the harness that selects it survives"
    );

    let repeat = Reply::send(client().delete(server.url("/v1/harness/agents/summarizer"))).await;

    assert_eq!(repeat.status, StatusCode::NOT_FOUND);
    assert_eq!(repeat.message(), "Unknown agent \"summarizer\".");

    let harness_still =
        Reply::send(client().get(server.url("/v1/harness/harnesses/instruct"))).await;

    assert_eq!(harness_still.status, StatusCode::OK);
    assert_eq!(
        harness_still.body["harness"]["subagents"]
            .as_array()
            .expect("subagents")
            .last(),
        Some(&json!("summarizer")),
        "an existing definition keeps a member whose profile was retired"
    );

    assert_eq!(
        root.agent_records()["records"]
            .as_array()
            .expect("records")
            .iter()
            .map(|row| (
                row["name"].as_str().unwrap_or_default().to_owned(),
                row["retired"].as_bool().unwrap_or(false),
            ))
            .collect::<Vec<_>>(),
        vec![("summarizer".to_string(), true)],
        "the retirement is the durable decision, which is what `restored_by_restart: false` promises"
    );

    server.stop().await;

    runtime::evict_runtime(&root.path);
    let restarted = TestServer::start(&root).await;

    let still_retired =
        Reply::send(client().get(restarted.url("/v1/harness/agents/summarizer"))).await;

    assert_eq!(still_retired.status, StatusCode::NOT_FOUND);
    assert_eq!(still_retired.message(), "Unknown agent \"summarizer\".");

    restarted.stop().await;
}

// ---------------------------------------------------------------------------
// 8 - the tool catalogue
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_tool_read_carries_the_catalogue_the_filters_and_the_gate() {
    let root = Root::new("tools");
    let server = TestServer::start(&root).await;

    let catalogue = ToolRegistry::new();
    let summaries = catalogue.summaries();

    assert_eq!(
        summaries.len(),
        35,
        "the generated catalogue (`tools/catalogue.rs`)"
    );

    let page = Reply::send(client().get(server.url("/v1/harness/tools?limit=100"))).await;

    assert_eq!(page.status, StatusCode::OK);
    assert_contract_headers(&page, Some(0));
    assert_eq!(
        page.body["items"],
        serde_json::to_value(&summaries).expect("rows serialize"),
        "`GET /tools` is the deployment catalogue, in catalogue order and unsorted"
    );
    assert_eq!(page.body["limit"], 35);
    assert_eq!(page.body["next_cursor"], Value::Null);

    // The default page is 25 of 35 rows, so it must advertise the next one.
    let first = Reply::send(client().get(server.url("/v1/harness/tools"))).await;

    assert_eq!(first.body["limit"], 25);
    assert_eq!(
        Root::page_names(&first.body),
        summaries[..25]
            .iter()
            .map(|row| row.name.clone())
            .collect::<Vec<_>>()
    );

    let cursor = first.body["next_cursor"].as_str().expect("a next page");
    let second =
        Reply::send(client().get(server.url(&format!("/v1/harness/tools?cursor={cursor}")))).await;

    assert_eq!(
        Root::page_names(&second.body),
        summaries[25..]
            .iter()
            .map(|row| row.name.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(second.body["limit"], 10);
    assert_eq!(
        second.body["next_cursor"],
        Value::Null,
        "the last page ends"
    );

    // The filters: `group` is the catalogue's own label, and `availability` is the two-state enum
    // with an entry that carries no block counted as `available` (`api.ts:1823-1827`).
    let group = summaries[0].group.clone();
    let grouped = summaries
        .iter()
        .filter(|row| row.group == group)
        .map(|row| row.name.clone())
        .collect::<Vec<_>>();

    let filtered = Reply::send(client().get(server.url(&format!(
        "/v1/harness/tools?limit=100&group={}",
        urlencoded(&group)
    ))))
    .await;

    assert_eq!(filtered.status, StatusCode::OK);
    assert_eq!(
        Root::page_names(&filtered.body),
        grouped,
        "the group filter narrows the page and changes nothing else"
    );

    let unconfigured = Reply::send(
        client().get(server.url("/v1/harness/tools?limit=100&availability=unconfigured")),
    )
    .await;

    assert_eq!(unconfigured.status, StatusCode::OK);

    // The filter compares `entry?.availability?.state ?? 'available'` (`api.ts:1826`), so a row with
    // NO availability block counts as `available` and never lands on this page; a row that carries a
    // block must say `unconfigured`, and `/tools` names environment VARIABLES only, never their
    // values (`knowledge.md` T4).
    let unconfigured_rows = unconfigured.body["items"].as_array().expect("items");

    assert!(
        !unconfigured_rows.is_empty(),
        "an unconfigured deployment has web rows to report"
    );

    for row in unconfigured_rows {
        assert_eq!(row["availability"]["state"], "unconfigured");
        assert!(
            !row["availability"]["environment"]
                .as_array()
                .expect("names")
                .is_empty(),
            "an unconfigured row names the variables that would configure it"
        );

        for name in row["availability"]["environment"]
            .as_array()
            .expect("names")
        {
            assert!(
                name.as_str()
                    .is_some_and(|value| value == value.to_ascii_uppercase()),
                "`{name}` is a variable name, not a value"
            );
        }
    }

    let bad_filter =
        Reply::send(client().get(server.url("/v1/harness/tools?availability=nope"))).await;

    assert_eq!(bad_filter.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        bad_filter.message(),
        "Query parameter \"availability\" must be one of: available, unconfigured."
    );

    // The identified read adds the schema and the closed `{scope, manual}` gate.
    let detail = Reply::send(client().get(server.url("/v1/harness/tools/read"))).await;

    assert_eq!(detail.status, StatusCode::OK);
    assert_contract_headers(&detail, None);

    let tool = &detail.body["tool"];

    assert_eq!(tool["name"], "read");
    // `read` is a file tool with a real standalone implementation, so it is NOT session-bound:
    // `harnesses/core.ts:16-24` states the rule in words - "core" means always declared, and only the
    // harness-owned runtime set is `sessionBound: true` (`:87`). The scope half of the gate is derived
    // from that same flag (`api-reads.ts:1211`), so a file tool reads `global`.
    assert_eq!(tool["session_bound"], false);
    assert_eq!(
        tool["execution"],
        json!({ "scope": "global", "manual": true }),
        "`harness-api.ts:660-664`: BOTH halves are stated, never a bare scope string"
    );
    assert_eq!(tool["parameters"]["type"], "object");
    assert!(tool["parameters"].get("properties").is_some());
    assert_eq!(
        tool.as_object()
            .expect("a flat row")
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .len(),
        8,
        "the six summary members a listed-and-always-available tool has, plus `parameters` and \
         `execution`; `availability` is emitted only for an entry that needs a credential"
    );

    // The global execution route is a policy refusal for a session-bound tool, and it is a 403
    // because a session-bound tool has no standalone implementation at all (`api.ts:1871-1876`).
    // `toolList` is one of the 18 harness-owned rows the catalogue marks `session_bound` - the group
    // `harnesses/core.ts:77-88` builds - while `read`, above, is not.
    let session_tool = Reply::send(
        client()
            .post(server.url("/v1/harness/tools/toolList/executions"))
            .json(&json!({ "parameters": {} })),
    )
    .await;

    assert_eq!(session_tool.status, StatusCode::FORBIDDEN);
    assert_eq!(
        session_tool.message(),
        "Tool \"toolList\" runs inside a session; call it through /v1/harness/sessions/{session_id}/tools/toolList/executions."
    );

    // A tool that is NOT session-bound runs on this route, and the answer is the contract's
    // discriminated `ToolCallResult`: `ok` is a boolean and BOTH outcomes are 200, because "the
    // invocation succeeded and the tool answered" is what the route reports (`harness-api.ts:730-743`,
    // `api.ts:1884-1904`). An empty `paths` box is a readable tool error, not an HTTP failure.
    let runnable = Reply::send(
        client()
            .post(server.url("/v1/harness/tools/read/executions"))
            .json(&json!({ "parameters": { "paths": [] } })),
    )
    .await;

    assert_eq!(runnable.status, StatusCode::OK);
    assert!(
        matches!(runnable.body["ok"], json!(true) | json!(false)),
        "`ok` is a real boolean on the wire, not a tag: {}",
        runnable.body
    );

    let outcome = if runnable.body["ok"] == json!(true) {
        "result"
    } else {
        "error"
    };

    assert!(
        runnable.body.get(outcome).is_some(),
        "a 200 carries the half the discrimnant promises: {}",
        runnable.body
    );
    assert!(
        runnable
            .body
            .get("duration_ms")
            .is_some_and(|value| { value.as_u64().is_some_and(|duration| duration < 60_000) }),
        "`duration_ms` is the server's own clock, bounded by the tool it measured: {}",
        runnable.body
    );

    let missing_tool = Reply::send(client().get(server.url("/v1/harness/tools/ghost"))).await;

    assert_eq!(missing_tool.status, StatusCode::NOT_FOUND);
    assert_eq!(missing_tool.message(), "Unknown tool \"ghost\".");

    let missing_execution = Reply::send(
        client()
            .post(server.url("/v1/harness/tools/ghost/executions"))
            .json(&json!({ "parameters": {} })),
    )
    .await;

    assert_eq!(missing_execution.status, StatusCode::NOT_FOUND);
    assert_eq!(missing_execution.message(), "Unknown tool \"ghost\".");

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 9 - model discovery
// ---------------------------------------------------------------------------

/// One canned provider answer, keyed by the path the harness will request.
type ProviderAnswer = (&'static str, (StatusCode, &'static str));

/// A local OpenAI-compatible provider, so discovery is tested against a real socket rather than a
/// mocked transport. `seen` collects the `authorization` headers it received, which is how the
/// credential-scope rule is observed from the provider's side, and `targets` collects the request
/// target (path AND query) so the URL-resolution rule is observable too - `modelsUrl` appends
/// `/models` to the PATHNAME and keeps the query (`transport/models.ts:129-131`).
#[derive(Clone)]
struct MockProvider {
    seen: Arc<Mutex<Vec<Option<String>>>>,
    targets: Arc<Mutex<Vec<String>>>,
    answers: Arc<Vec<ProviderAnswer>>,
}

impl MockProvider {
    /// The provider's own view of what arrived: one entry per request, in order.
    fn seen(&self) -> Vec<Option<String>> {
        self.seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// The request targets, `path?query`, one per request, in order.
    fn targets(&self) -> Vec<String> {
        self.targets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

async fn provider_handler(State(provider): State<MockProvider>, request: Request) -> Response {
    let uri = request.uri().clone();
    let path = uri.path().to_owned();
    let target = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.clone(),
    };

    let authorization = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    provider
        .seen
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(authorization);

    provider
        .targets
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(target);

    let (status, body) = provider
        .answers
        .iter()
        .find(|(route, _)| *route == path)
        .map_or((StatusCode::NOT_FOUND, "{}"), |(_, answer)| *answer);

    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_owned()))
        .expect("a canned response builds")
}

/// Serves the mock and reports its base URL plus the handler that records the requests.
async fn start_mock_provider(
    answers: Vec<ProviderAnswer>,
) -> (String, MockProvider, JoinHandle<()>) {
    let provider = MockProvider {
        seen: Arc::new(Mutex::new(Vec::new())),
        targets: Arc::new(Mutex::new(Vec::new())),
        answers: Arc::new(answers),
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("an ephemeral port for the mock provider");
    let local: SocketAddr = listener.local_addr().expect("the port is known");

    let router: Router = Router::new()
        .fallback(get(provider_handler))
        .with_state(provider.clone());

    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("the mock provider serves");
    });

    (format!("http://{local}"), provider, task)
}

/// Percent-encodes the handful of characters these tests put in a query value.
fn urlencoded(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b' ' => "%20".to_owned(),
            b'&' | b'=' | b'?' | b'#' | b'!' => format!("%{byte:02X}"),
            other => char::from(other).to_string(),
        })
        .collect()
}

#[tokio::test]
async fn model_discovery_reads_and_filters_the_provider_list() {
    let root = Root::new("models-list");
    let server = TestServer::start(&root).await;

    // An empty process credential first, so the "nothing leaked to a foreign provider" assertion
    // below is about the SCOPE rule rather than about whatever this machine exports.
    let cleared = Reply::send(
        client()
            .patch(server.url("/v1/harness/config"))
            .json(&json!({ "settings": { "api_key": "" } })),
    )
    .await;

    assert_eq!(cleared.status, StatusCode::OK);
    assert_eq!(cleared.body["defaults"]["has_api_key"], false);

    let (provider, mock, provider_task) = start_mock_provider(vec![(
        "/ok/v1/models",
        (
            StatusCode::OK,
            r#"{"object":"list","data":[{"id":"m1"},{"id":" m1 "},{"id":""},{"id":5},{"id":"m2"},{"models":"ignored"}]}"#,
        ),
    )])
    .await;

    let before = events::now_millis();
    let reply = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": format!("{provider}/ok/v1") } })),
    )
    .await;
    let after = events::now_millis();

    assert_eq!(reply.status, StatusCode::OK);
    assert_contract_headers(&reply, None);

    // Trim, drop the blanks, ignore a non-string `id`, dedupe, and KEEP the provider's order
    // (`transport/models.ts:138-176`).
    assert_eq!(reply.body["models"], json!(["m1", "m2"]));
    assert_eq!(reply.body.as_object().expect("a closed object").len(), 2);

    let observed_at = reply.body["observed_at"].as_u64().expect("a clock");
    assert!(
        (before..=after).contains(&observed_at),
        "observed_at is the server clock: {observed_at} not in {before}..={after}"
    );

    // The body's base URL is a FOREIGN provider, so this process's credential never travels with
    // the request (`api.ts:1915-1928`, `config.ts:434-446`) - and one success is one request,
    // because only a `request`/`response` failure retries (`api.ts:334`).
    assert_eq!(
        mock.seen(),
        vec![None],
        "no `authorization` header left this process, and the provider was asked exactly once"
    );

    // `/models` is appended to the PATHNAME, not to the raw string, and the base URL's own query is
    // KEPT so it cannot swallow the suffix (`transport/models.ts:129-131`).
    let with_query = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": format!("{provider}/ok/v1?tag=live") } })),
    )
    .await;

    assert_eq!(with_query.status, StatusCode::OK);
    assert_eq!(
        mock.targets().last().cloned().expect("a request arrived"),
        "/ok/v1/models?tag=live",
        "the suffix lands on the path and the caller's query survives it"
    );

    // An explicit CLIENT key does travel: it is the caller's own credential for the caller's own
    // endpoint.
    let with_key = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({
                "settings": {
                    "base_url": format!("{provider}/ok/v1"),
                    "api_key": "sk-client-owned",
                }
            })),
    )
    .await;

    assert_eq!(with_key.status, StatusCode::OK);
    assert_eq!(
        mock.seen().last().cloned().expect("a request arrived"),
        Some("Bearer sk-client-owned".to_owned()),
        "the key is placed in the header, never in the URL (`transport/models.ts:18-21`)"
    );

    drop(provider_task);
    server.stop().await;
}

#[tokio::test]
async fn model_discovery_maps_every_failure_kind_onto_the_contract() {
    let root = Root::new("models-errors");
    let server = TestServer::start(&root).await;

    let (provider, mock, provider_task) = start_mock_provider(vec![
        (
            "/boom/v1/models",
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "provider exploded over and over",
            ),
        ),
        (
            "/nolist/v1/models",
            (StatusCode::OK, r#"{"object":"list"}"#),
        ),
        ("/bare/v1/models", (StatusCode::OK, r#"["z","a","z"]"#)),
        ("/none/v1/models", (StatusCode::OK, r#"{"data":[]}"#)),
    ])
    .await;

    // A 500 provider is a 502 that quotes the provider's own status and detail
    // (`transport/models.ts:216-224`), after every attempt of the retry policy.
    let boom = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": format!("{provider}/boom/v1") } })),
    )
    .await;

    assert_eq!(boom.status, StatusCode::BAD_GATEWAY);
    assert_eq!(boom.message(), "HTTP 500: provider exploded over and over");
    assert_contract_headers(&boom, None);
    assert_eq!(
        mock.seen().len(),
        MODEL_LIST_MAX_ATTEMPTS,
        "a `response` failure retries, and the budget doubles per attempt (`api.ts:322-341`)"
    );

    // A 200 with no list at all is the malformed answer, also a 502 (`transport/models.ts:154-159`).
    let nolist = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": format!("{provider}/nolist/v1") } })),
    )
    .await;

    assert_eq!(nolist.status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        nolist.message(),
        "The provider's /models response did not include a model list."
    );

    // A gateway that answers a bare ARRAY is a supported shape (`transport/models.ts:148-152`).
    let bare = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": format!("{provider}/bare/v1") } })),
    )
    .await;

    assert_eq!(bare.status, StatusCode::OK);
    assert_eq!(bare.body["models"], json!(["z", "a"]));

    // An empty catalogue is a STATE, not an error: "the provider answered and served nothing
    // usable", which is different from 502 (`transport/models.ts:184-187`).
    let none = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": format!("{provider}/none/v1") } })),
    )
    .await;

    assert_eq!(none.status, StatusCode::OK);
    assert_eq!(none.body["models"], json!([]));

    // No endpoint at all is a 400, decided before any request is attempted (`api.ts:1930-1932`).
    let no_url = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": "   " } })),
    )
    .await;

    assert_eq!(no_url.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        no_url.message(),
        "Set a provider base URL before listing models."
    );

    // A relative base URL is a CONFIGURATION refusal (400) - and a configuration refusal never
    // retries, which is why this answers at once instead of after ten attempts (`api.ts:334`).
    let before_relative = events::now_millis();
    let relative = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": "providers/private/v1" } })),
    )
    .await;
    let relative_ms = events::now_millis().saturating_sub(before_relative);

    assert_eq!(relative.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        relative.message(),
        "\"providers/private/v1\" is not an absolute URL."
    );
    assert!(
        relative_ms < 5_000,
        "a configuration error is decided synchronously: {relative_ms}ms"
    );

    let wrong_scheme = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": "file:///tmp/provider" } })),
    )
    .await;

    assert_eq!(wrong_scheme.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        wrong_scheme.message(),
        // `displayUrl` is `url.origin + url.pathname`, and `URL.origin` is the literal `"null"` for
        // an opaque-origin scheme (`node -e "new URL('file:///tmp/provider').origin"`), which is how
        // a `file:` base URL reaches the client as `"null/tmp/provider"`.
        "\"null/tmp/provider\" must use http or https to list models."
    );

    // A scheme that DOES have an authority keeps its origin and its path, minus the userinfo, the
    // query and the fragment (`transport/models.ts:97`).
    let ftp_scheme = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": "ftp://user:pw@provider.test/v1?q=1#f" } })),
    )
    .await;

    assert_eq!(ftp_scheme.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        ftp_scheme.message(),
        "\"ftp://provider.test/v1\" must use http or https to list models."
    );

    // The body policy on this route, with `settingsInputOf`'s path-qualified refusals.
    for (body, message) in [
        (
            json!({ "session": "x" }),
            "Unknown field \"session\". Allowed fields: settings.",
        ),
        (
            json!({ "settings": { "base_url": 5 } }),
            "\"body.settings.base_url\" must be a string.",
        ),
        (
            json!({ "settings": "http://x/v1" }),
            "\"body.settings\" must be an object.",
        ),
        (
            json!({ "settings": { "mode": "fast" } }),
            "Unknown field \"mode\". Allowed fields: base_url, api_key, model.",
        ),
        (
            // An EMPTY string is kept as empty through the merge (`config.ts:406`), and an empty base
            // URL is the route's one configuration refusal. `postModels` resolves the pair key-free
            // from `context.defaults` (`api.ts:1915-1928`), so nothing else on this route can produce
            // this message.
            json!({ "settings": { "base_url": "" } }),
            "Set a provider base URL before listing models.",
        ),
        (
            json!({ "settings": { "api_key": 7 } }),
            "\"body.settings.api_key\" must be a string.",
        ),
        (
            json!({ "settings": { "model": 7 } }),
            "\"body.settings.model\" must be a string.",
        ),
    ] {
        let reply = Reply::send(client().post(server.url("/v1/harness/models")).json(&body)).await;

        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "`{body}` is refused");
        assert_eq!(reply.message(), message);
    }

    // A `null` is "NOT SUPPLIED", never "the empty string" (`settingsInputOf`, `api.ts:548-561`), so the
    // merge falls back to the EFFECTIVE pair (`api.ts:1915-1928`: `context.defaults` plus the live
    // overlay). Pinning that pair with a config patch is what makes the rule checkable: the discovery
    // must dial the patched URL, and a provider nobody is listening on is the contract's 502. Port 9 is
    // `discard`, so the refusal is immediate and the ten attempts cost nothing.
    let pinned = "http://127.0.0.1:9/v1";
    let moved = Reply::send(
        client()
            .patch(server.url("/v1/harness/config"))
            .json(&json!({ "settings": { "base_url": pinned } })),
    )
    .await;

    assert_eq!(moved.status, StatusCode::OK);

    let omitted = Reply::send(
        client()
            .post(server.url("/v1/harness/models"))
            .json(&json!({ "settings": { "base_url": null } })),
    )
    .await;

    assert_eq!(omitted.status, StatusCode::BAD_GATEWAY);

    // The sentence quotes the endpoint the port DIALED, which is `{base}/models` - `displayUrl` runs
    // on the resolved URL (`transport/models.ts:189`, `:211`), not on the caller's base URL.
    assert!(
        omitted
            .message()
            .starts_with(&format!("Request to {pinned}/models failed")),
        "the omitted base URL resolves to the live pair, not to the shipped default: {}",
        omitted.message()
    );

    drop(provider_task);
    server.stop().await;
}

/// The retry budget `api.ts:316` fixes: ten attempts, the first included.
const MODEL_LIST_MAX_ATTEMPTS: usize = 10;

// ---------------------------------------------------------------------------
// 10 - the body policy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_body_policy_is_route_aware_media_type_checked_and_lazily_read() {
    let root = Root::new("body");
    let server = TestServer::start(&root).await;

    // A declared content type that is not JSON is 415. TS's own adapters inherit an already-parsed
    // body, so they have no gate to copy; the contract documents 415 for a request body, and this
    // port enforces it in the same voice as the two messages below.
    let text = Reply::read(
        client()
            .patch(server.url("/v1/harness/config"))
            .header("content-type", "text/plain")
            .body("hello")
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(text.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        text.message(),
        "A request body must be application/json (content-type \"text/plain\" is not supported)."
    );

    // The right content type with malformed JSON is the 400 `handler.ts:128` prints.
    let malformed = Reply::read(
        client()
            .patch(server.url("/v1/harness/config"))
            .header("content-type", "application/json")
            .body("{oops")
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(malformed.status, StatusCode::BAD_REQUEST);
    assert_eq!(malformed.message(), "The request body must be valid JSON.");

    // No body at all on a JSON route is `{}`, which is the value TS resolves and what keeps the
    // "object body" rules unchanged (`handler.ts:120-123`).
    let empty = Reply::read(
        client()
            .patch(server.url("/v1/harness/config"))
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(empty.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty.message(),
        "A config patch requires a \"settings\" object."
    );

    // A 1 MB route over its cap is 413, and the sentence carries the cap that applied
    // (`handler.ts:109`, `service.ts:315`).
    let oversized = format!(
        "{{\"settings\":{{\"model\":\"{}\"}}}}",
        "m".repeat(contract::MAX_BODY_BYTES + 21)
    );

    assert!(oversized.len() > contract::MAX_BODY_BYTES);

    let too_big = Reply::read(
        client()
            .patch(server.url("/v1/harness/config"))
            .header("content-type", "application/json")
            .body(oversized)
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(too_big.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        too_big.message(),
        "A request body cannot exceed 1000000 bytes."
    );

    // THE BOUNDARY: only the submit route carries raw base64 media, so only it gets the 16 MiB cap
    // (`bodyLimitFor`, `api.ts:713-723`). A 2 MB submit is READ, which is why the answer is the
    // SESSION route's refusal and not a 413. (T5b replaced the session stub, so the route now runs
    // its real `requireSession` first — `api.ts:2200` — and `session-1` was never created in this
    // test; either way the point stands: the handler ran under the LARGE cap.)
    let submit = format!("{{\"prompt\":\"{}\"}}", "x".repeat(2_000_000));

    let accepted = Reply::read(
        client()
            .post(server.url("/v1/harness/sessions/session-1/tasks"))
            .header("content-type", "application/json")
            .body(submit)
            .send()
            .await
            .expect("the submit answers"),
    )
    .await;

    assert_eq!(
        accepted.status,
        StatusCode::NOT_FOUND,
        "2 MB is under the 16 MiB submit cap, so the handler ran"
    );
    assert_eq!(accepted.message(), "Unknown session \"session-1\".");

    // The same size on a 1 MB route IS a 413, which is the boundary the contract draws. The submit
    // call above is what makes this one prove something: it touched the same session id under the
    // LARGE cap, and this route now resolves the small one.
    let same_size = Reply::read(
        client()
            .post(server.url("/v1/harness/models"))
            .header("content-type", "application/json")
            .body(format!(
                "{{\"settings\":{{\"model\":\"{}\"}}}}",
                "x".repeat(2_000_000)
            ))
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(same_size.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        same_size.message(),
        "A request body cannot exceed 1000000 bytes."
    );

    // Over the submit cap the refusal names the LARGER limit, proving the cap is route-resolved
    // before the first byte is read. The payload is 16 MiB + 200 bytes, and `{"prompt":"…"}}` is 13
    // bytes of framing, so it lands clear of the 16_777_216 boundary the 2 MB case sits UNDER.
    let huge = Reply::read(
        client()
            .post(server.url("/v1/harness/sessions/session-1/tasks"))
            .header("content-type", "application/json")
            .body(format!(
                "{{\"prompt\":\"{}\"}}",
                "x".repeat(16 * 1024 * 1024 + 200)
            ))
            .send()
            .await
            .expect("the refusal arrives"),
    )
    .await;

    assert_eq!(huge.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        huge.message(),
        "A request body cannot exceed 16777216 bytes."
    );

    // Laziness is the contract, twice over: a 405 never consumes the body (`api.ts:4009`), and a
    // route that refuses on an unknown id never reads one either (`api.ts:4022-4024`).
    let wrong_verb = Reply::read(
        client()
            .delete(server.url("/v1/harness/health"))
            .header("content-type", "application/json")
            .body(format!("{{\"prompt\":\"{}\"}}", "x".repeat(2_000_000)))
            .send()
            .await
            .expect("the 405 arrives"),
    )
    .await;

    assert_eq!(wrong_verb.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(wrong_verb.message(), "Use GET for /v1/harness/health.");

    let unknown_with_bad_body = Reply::read(
        client()
            .get(server.url("/v1/harness/tools/ghost"))
            .header("content-type", "text/plain")
            .body("not json at all")
            .send()
            .await
            .expect("the 404 arrives"),
    )
    .await;

    assert_eq!(unknown_with_bad_body.status, StatusCode::NOT_FOUND);
    assert_eq!(unknown_with_bad_body.message(), "Unknown tool \"ghost\".");

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 11 - the Origin gate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_origin_gate_is_the_contracts_policy() {
    let root = Root::new("origin");
    let server = TestServer::start(&root).await;

    // A foreign page origin is refused before any runtime work, with the policy sentence
    // (`service.ts:529-539`). `0.0.0.0` and every other host are NOT loopback
    // (`knowledge.md`: `LOOPBACK_HOSTNAMES` is exactly four entries).
    for origin in [
        "http://evil.example",
        "https://evil.example:8443",
        "null",
        "http://localhost.other.test:5181",
        "http://127.0.0.2:5181",
        "javascript:alert(1)",
    ] {
        let reply = Reply::read(
            client()
                .get(server.url("/v1/harness"))
                .header("origin", origin)
                .send()
                .await
                .unwrap_or_else(|error| panic!("`{origin}` answers: {error}")),
        )
        .await;

        assert_eq!(reply.status, StatusCode::FORBIDDEN, "`{origin}` is refused");
        assert_eq!(reply.message(), "Cross-origin requests are not allowed.");
    }

    // The same host, a loopback origin, and a GitHub Pages origin are all trusted
    // (`origin.ts:27-43`).
    for origin in [
        format!("http://{}", server.host()),
        "http://localhost:5181".to_owned(),
        "http://127.0.0.1:4321".to_owned(),
        "http://[::1]:5181".to_owned(),
        "https://any-user.github.io".to_owned(),
    ] {
        let reply = Reply::read(
            client()
                .get(server.url("/v1/harness"))
                .header("origin", &origin)
                .send()
                .await
                .unwrap_or_else(|error| panic!("`{origin}` answers: {error}")),
        )
        .await;

        assert_eq!(reply.status, StatusCode::OK, "`{origin}` is trusted");
    }

    // No Origin header is a non-browser client, which the policy has never gated: the check runs
    // only when a non-empty Origin is present (`service.ts:530`).
    let no_origin = Reply::send(client().get(server.url("/v1/harness/health"))).await;

    assert_eq!(no_origin.status, StatusCode::OK);

    // The stream mount applies the SAME rule, in its own sentence (`stream/mount.ts:41-44` says the
    // rule is "deliberately identical" to `service.ts`'s).
    let stream = Reply::read(
        client()
            .get(server.url("/v1/harness/stream"))
            .header("origin", "http://evil.example")
            .send()
            .await
            .expect("the stream refusal arrives"),
    )
    .await;

    assert_eq!(stream.status, StatusCode::FORBIDDEN);
    assert_eq!(
        stream.message(),
        "Cross-origin stream upgrades are not allowed."
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 12 - the paging parameters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_paging_parameters_are_strict_and_the_limit_echoes_the_page() {
    let root = Root::new("paging");
    let server = TestServer::start(&root).await;

    let foreign = paging::encode_cursor("a-foreign-epoch", 3);
    let long_cursor = "x".repeat(300);

    let cases: Vec<(String, &str)> = vec![
        ("limit=0".to_owned(), LIMIT_MESSAGE),
        ("limit=101".to_owned(), LIMIT_MESSAGE),
        ("limit=-1".to_owned(), LIMIT_MESSAGE),
        ("limit=abc".to_owned(), LIMIT_MESSAGE),
        ("limit=1.5".to_owned(), LIMIT_MESSAGE),
        ("limit=1e2".to_owned(), LIMIT_MESSAGE),
        (
            "cursor=%21%21%21".to_owned(),
            "Query parameter \"cursor\" is not a cursor this server issued.",
        ),
        (
            "cursor=%%%".to_owned(),
            "Query parameter \"cursor\" is not a cursor this server issued.",
        ),
        (
            format!("cursor={long_cursor}"),
            "Query parameter \"cursor\" is not a cursor this server issued.",
        ),
        (
            format!("cursor={foreign}"),
            "Query parameter \"cursor\" is stale or foreign; restart from the first page.",
        ),
    ];

    for (query, message) in cases {
        let reply = Reply::read(
            client()
                .get(server.url(&format!("/v1/harness/harnesses?{query}")))
                .send()
                .await
                .unwrap_or_else(|error| panic!("`{query}` answers: {error}")),
        )
        .await;

        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "`{query}` is refused"
        );
        assert_eq!(reply.message(), message, "`{query}` names the rule");
    }

    // `?limit=` is ABSENT, not zero: `paramOf` trims and maps an empty value to `undefined` before
    // `resolveLimit` sees it (`api.ts:482-488`, `api-paging.ts:95`).
    let empty_limit = Reply::send(client().get(server.url("/v1/harness/harnesses?limit="))).await;

    assert_eq!(empty_limit.status, StatusCode::OK);
    assert_eq!(
        empty_limit.body["limit"], 5,
        "the contract default of 25 covers all five rows"
    );

    // The page walk: `limit` echoes the EFFECTIVE size, and a known total ENDS an exactly-full
    // final page instead of pointing at an empty one (`api-paging.ts:124-139`).
    let first = Reply::send(client().get(server.url("/v1/harness/harnesses?limit=2"))).await;

    assert_eq!(first.body["limit"], 2);
    assert_eq!(
        Root::page_names(&first.body),
        ["instruct", "research"],
        "a page of 2 out of 5 carries the first two and a cursor"
    );

    let cursor = first.body["next_cursor"].as_str().expect("a next page");
    let second = Reply::send(
        client().get(server.url(&format!("/v1/harness/harnesses?limit=2&cursor={cursor}"))),
    )
    .await;

    assert_eq!(Root::page_names(&second.body), ["workspace", "reviewer"]);

    let third_cursor = second.body["next_cursor"].as_str().expect("a third page");
    let third = Reply::send(client().get(server.url(&format!(
        "/v1/harness/harnesses?limit=2&cursor={third_cursor}"
    ))))
    .await;

    assert_eq!(Root::page_names(&third.body), ["planner"]);
    assert_eq!(third.body["limit"], 1);
    assert_eq!(
        third.body["next_cursor"],
        Value::Null,
        "the collection ended, so no cursor to an empty page is advertised"
    );

    server.stop().await;
}
