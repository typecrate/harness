//! Real-socket tests for the harness update stream (T5c).
//!
//! Every assertion goes over TCP: the HTTP gates through `reqwest`, the conversation through
//! `tokio-tungstenite`, against an `axum::serve` instance built by `typecrate_harness::server::router`
//! (the same function the executable binds) with a REAL store and a REAL `EventHub`. Nothing here fakes
//! the broker or reaches into it in-process, except where a test reads the live subscription count: what
//! this stage owns is the wire, and the only way to prove it is a client that shakes hands, subscribes,
//! and reads what a REST write fans out.
//!
//! Pinned, in the stage brief's order:
//!
//! 1. the handshake: `101`, the `harness.v1` echo when offered, no echo when not, and
//!    `X-Harness-Epoch` matching the epoch the REST surface reports;
//! 2. `welcome` as the first frame, with the protocol, the epoch, and all eighteen limit numbers;
//! 3. `hello` answers NOTHING, `ping` answers `pong` echoing `at`;
//! 4. `subscribe` -> `subscription_state` with the per-scope baseline revisions, the publish set and
//!    the cursor, then a REST `PATCH` arriving as a `session.updated` frame whose `revision` equals the
//!    header the REST write carried;
//! 5. cursor replay: unsubscribe, then re-subscribe from an earlier cursor and get the retained frames
//!    plus `replayed_frames`;
//! 6. a cursor from another epoch -> `resync_required{ epoch_mismatch }`, echoing that cursor and
//!    naming the bounded reads, with no `subscription_state` and no subscription registered;
//! 7. `unsubscribe` answers nothing, and an `ack` on an `ordered` subscription is ignored; an
//!    `ack`-paced subscription sends ONE frame and waits, then releases the backlog on the ack;
//! 8. an invalid frame answers `invalid_payload`, an oversize one answers `frame_too_large` and closes
//!    1009, a non-JSON frame closes 1003, and three consecutive union failures close 1007;
//! 9. `bye` closes 1000;
//! 10. the pre-upgrade gates answer HTTP statuses, not close frames: 403 cross-origin, 400 non-upgrade
//!     GET (TypeScript's own sentence), 405 non-GET, 400 missing key, 426 wrong version, 400 wrong
//!     upgrade token, and the exact-path gate naming the path it was given;
//! 11. an unknown `session_id` answers `unknown_resource`, a duplicate id answers `invalid_payload`, a
//!     filter the scope does not publish answers `unsupported_filter`, and a late `hello` answers
//!     `late_hello` - each leaving the connection usable;
//! 12. the subscription budget answers `too_many_subscriptions` and closes 4401;
//! 13. the per-second message budget answers `rate_limited` and closes 4429;
//! 14. `broker.close_all(1012)` fans an `error` frame with the close code to every live connection,
//!     which is the API T6's service route reaches on an epoch change;
//! 15. the `actions` scope (T7a): the REST answer route arriving as an `action.answered` frame on a
//!     second subscription, the `action.opened` frame the publish path stamps with the open revision,
//!     the `action.closed` frame the store fans on every terminal action transition, and the baseline
//!     `active_actions` an action subscription opens with;
//! 16. `server::begin_shutdown` (T7a): the listener keeps accepting, the upgrade answers 503, and an
//!     already-open conversation closes with the 1001 going-away frame.
//!
//! Fixtures live in `Temp/typecrate-harness/stream/<pid>-<test>/` per the repo scratch rule, and every
//! test owns its own DATABASE ROOT: the runtime cache is keyed by root, so an isolated root is what
//! keeps one test's counters, subscriptions and replay windows from being another's shared state. No
//! test touches a process environment variable. The provider base URL points at port 9 (discard): the
//! stream never runs a turn, and session create/patch validate the settings shape without reaching the
//! provider (`api/sessions.rs:198-217`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

use typecrate_harness::contract::HARNESS_WS_STREAM_PATH;
use typecrate_harness::package_root;
use typecrate_harness::ports::ActionHost;
use typecrate_harness::runtime;
use typecrate_harness::server;
use typecrate_harness::store::actions::StoreActionHost;
use typecrate_harness::stream::{self, StreamBroker, protocol::encode_stream_cursor};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// How long a test waits for one frame. A local scripted server answers in milliseconds, so 10 s only
/// catches a real hang - and it stays under the contract's 15 s heartbeat, so a missing frame is never
/// a heartbeat racing the assertion.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

const EPOCH_HEADER: HeaderName = HeaderName::from_static("x-harness-epoch");
const REVISION_HEADER: HeaderName = HeaderName::from_static("x-harness-revision");
const PROTOCOL_HEADER: HeaderName = HeaderName::from_static("sec-websocket-protocol");

/// A database root that starts empty and is removed on `Drop`.
struct Root {
    path: PathBuf,
}

impl Root {
    fn new(test: &str) -> Self {
        let path = package_root::normalize_lexically(&Path::new(env!("CARGO_MANIFEST_DIR")).join(
            format!(
                "../../../Temp/typecrate-harness/stream/{pid}-{test}",
                pid = std::process::id()
            ),
        ));

        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path)
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", path.display()));

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// One harness listener on an ephemeral loopback port, serving the LIBRARY router.
struct TestServer {
    base: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    root: PathBuf,
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
            root: root.path.clone(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// The stream mount as a `ws://` target.
    fn stream_url(&self) -> String {
        self.url(HARNESS_WS_STREAM_PATH)
            .replacen("http://", "ws://", 1)
    }

    /// This runtime's live broker, reached exactly the way T6's service route reaches it.
    fn broker(&self) -> std::sync::Arc<StreamBroker> {
        runtime::cached_runtime(&self.root)
            .expect("a served request created the runtime for this root")
            .stream_broker()
    }

    /// Fires the graceful-shutdown signal WITHOUT waiting for the listener to drain, so a test can
    /// read the frames the shutdown arm itself sends (`server::serve_on` closes the live streams when
    /// this future resolves).
    fn signal_shutdown(&mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }
    }

    /// Stops the listener and waits for it. Callers drop every stream first: a live WebSocket is an
    /// open connection, and graceful shutdown waits for one.
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

/// The four upgrade tokens every handshake gate reads, spelled once.
const UPGRADE_HEADERS: [(&str, &str); 4] = [
    ("connection", "Upgrade"),
    ("upgrade", "websocket"),
    ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
    ("sec-websocket-version", "13"),
];

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

    fn message(&self) -> String {
        self.body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }

    fn header(&self, name: &HeaderName) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }

    fn epoch(&self) -> String {
        self.header(&EPOCH_HEADER)
            .expect("X-Harness-Epoch on every JSON response")
    }

    fn revision(&self) -> u64 {
        self.header(&REVISION_HEADER)
            .expect("X-Harness-Revision on this route")
            .parse()
            .expect("a numeric revision")
    }
}

// ---------------------------------------------------------------------------
// The stream client
// ---------------------------------------------------------------------------

/// One connected stream client: the socket plus the handshake response's status and headers.
struct Stream {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    status: StatusCode,
    headers: HeaderMap,
}

impl Stream {
    /// Completes the handshake against `server`, offering `harness.v1` unless told otherwise.
    async fn connect(server: &TestServer, offer: Option<&str>) -> Self {
        Self::connect_at(&server.stream_url(), offer).await
    }

    async fn connect_at(url: &str, offer: Option<&str>) -> Self {
        let mut request = url
            .into_client_request()
            .expect("the stream URL is a valid client request");

        match offer {
            Some(protocol) => {
                request.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_str(protocol).expect("an ASCII subprotocol"),
                );
            }
            None => {
                request.headers_mut().remove("Sec-WebSocket-Protocol");
            }
        }

        let (socket, response) =
            tokio::time::timeout(READ_TIMEOUT, tokio_tungstenite::connect_async(request))
                .await
                .expect("the handshake completes")
                .unwrap_or_else(|error| panic!("the upgrade is accepted: {error:?}"));

        Self {
            socket,
            status: response.status(),
            headers: response.headers().clone(),
        }
    }

    fn header(&self, name: &HeaderName) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }

    /// Sends one JSON frame.
    async fn send(&mut self, frame: Value) {
        let text = serde_json::to_string(&frame).expect("a frame serializes");

        self.send_raw(&text).await;
    }

    /// Sends a frame whose bytes the test built by hand (the oversize and non-JSON arms).
    async fn send_raw(&mut self, text: &str) {
        tokio::time::timeout(
            READ_TIMEOUT,
            self.socket.send(WsMessage::Text(text.to_owned().into())),
        )
        .await
        .expect("the frame is written")
        .expect("the socket accepts the frame");
    }

    /// The next frame on the wire, as JSON. A close where a frame was expected panics, so a refusal
    /// never reads as a missing frame.
    async fn next_frame(&mut self) -> Value {
        let message = tokio::time::timeout(READ_TIMEOUT, self.socket.next())
            .await
            .expect("a frame arrives")
            .expect("the stream yields a message")
            .expect("the socket is readable");

        match message {
            WsMessage::Text(text) => serde_json::from_str(text.as_str())
                .unwrap_or_else(|error| panic!("a JSON text frame, got `{text}`: {error}")),
            WsMessage::Close(frame) => panic!("the stream closed with {frame:?}"),
            other => panic!("an unexpected websocket frame: {other:?}"),
        }
    }

    /// The next frame whose `type` is `expected`. A `heartbeat` may legitimately interleave at any
    /// point in the conversation, so it is skipped; anything else in that slot is a failure.
    async fn expect_frame(&mut self, expected: &str) -> Value {
        loop {
            let frame = self.next_frame().await;

            if frame["type"] == json!(expected) {
                return frame;
            }

            assert_eq!(
                frame["type"],
                json!("heartbeat"),
                "only a `heartbeat` may interleave a `{expected}` frame, got: {frame}"
            );
        }
    }

    /// Proves a frame is answered with NOTHING: it sends `frame`, sends a `ping` probe, and requires
    /// the probe's `pong` to be the very next frame. Any reply to `frame` would land first.
    async fn expect_silence(&mut self, frame: Value, probe_at: u64) {
        let stated = frame.to_string();

        self.send(frame).await;
        self.send(json!({"type": "ping", "at": probe_at})).await;

        let pong = self.next_frame().await;

        assert_eq!(
            pong,
            json!({"type": "pong", "at": probe_at}),
            "the contract sends no reply to {stated}, so the pong must come next, got: {pong}"
        );
    }

    /// Reads until the close frame and returns the reason the server sent.
    async fn expect_close(&mut self, code: u16) -> String {
        loop {
            let message = tokio::time::timeout(READ_TIMEOUT, self.socket.next())
                .await
                .expect("the close arrives")
                .expect("the stream yields a message")
                .expect("the socket is readable");

            match message {
                WsMessage::Close(Some(frame)) => {
                    let sent = u16::from(frame.code);
                    let reason = frame.reason.as_str().to_owned();

                    assert_eq!(
                        sent, code,
                        "the stream closes with {code}, got {sent} (`{reason}`)"
                    );

                    return reason;
                }
                WsMessage::Close(None) => panic!("the close frame carries a status code"),
                WsMessage::Text(text) => {
                    let frame: Value = serde_json::from_str(text.as_str()).expect("a JSON frame");

                    assert_eq!(
                        frame["type"],
                        json!("heartbeat"),
                        "only a heartbeat may precede the close, got: {frame}"
                    );
                }
                other => panic!("an unexpected websocket frame: {other:?}"),
            }
        }
    }

    /// Closes the client side, so the listener can shut down gracefully.
    ///
    /// `WebSocketStream::close` is tungstenite's own method (an inherent one, ahead of `SinkExt::close`
    /// in method resolution): it sends a close frame and finishes the handshake.
    async fn disconnect(mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(2), self.socket.close(None)).await;
    }
}

// ---------------------------------------------------------------------------
// REST helpers (the mutations the stream is proved against)
// ---------------------------------------------------------------------------

/// A provider URL nothing is ever called on: session create and patch validate the shape and stop.
const DEAD_BASE: &str = "http://127.0.0.1:9/v1";

async fn create_session(server: &TestServer, title: &str) -> (String, Reply) {
    let reply = Reply::send(
        client()
            .post(server.url("/v1/harness/sessions"))
            .json(&json!({
                "harness": "instruct",
                "title": title,
                "settings": {"base_url": DEAD_BASE, "api_key": "stream-test-key", "model": "mock/one"},
            })),
    )
    .await;

    assert_eq!(
        reply.status,
        StatusCode::CREATED,
        "`{title}` creates: {}",
        reply.body
    );

    let session_id = reply
        .body
        .pointer("/session/id")
        .and_then(Value::as_str)
        .expect("the create names the session it made")
        .to_owned();

    (session_id, reply)
}

async fn patch_session(server: &TestServer, session_id: &str, title: &str) -> Reply {
    let reply = Reply::send(
        client()
            .patch(server.url(&format!("/v1/harness/sessions/{session_id}")))
            .json(&json!({ "title": title })),
    )
    .await;

    assert_eq!(
        reply.status,
        StatusCode::OK,
        "patching `{session_id}` works: {}",
        reply.body
    );

    reply
}

/// Subscribes one session scope and returns its `subscription_state`.
async fn subscribe(ws: &mut Stream, subscription_id: &str, session_id: &str) -> Value {
    ws.send(json!({
        "type": "subscribe",
        "subscription_id": subscription_id,
        "scope": "session",
        "session_id": session_id,
    }))
    .await;

    ws.expect_frame("subscription_state").await
}

/// Reads the ONE `update` frame each of this connection's subscriptions receives for the next event,
/// in registration order, and returns them as `(s1, s2)`.
///
/// A publish reaches every subscription addressed at its resource (`stream/mod.rs:161-163` keeps one
/// slot per `(connection, subscription id)` pair), so an event on a two-subscription connection
/// arrives TWICE - once per subscription, each with that subscription's OWN `sequence`. A test that
/// reads one frame per event would then read the sibling copy as the next event, which is exactly the
/// confusion this helper removes.
async fn expect_update_pair(ws: &mut Stream, kind: &str) -> [Value; 2] {
    let first = ws.expect_frame("update").await;
    let second = ws.expect_frame("update").await;

    assert_eq!(
        [first["kind"].as_str(), second["kind"].as_str()],
        [Some(kind), Some(kind)],
        "both subscriptions receive the `{kind}` event, got: {first} / {second}"
    );
    assert_eq!(
        first["subscription_id"],
        json!("s1"),
        "the copies arrive in subscription order: {first} / {second}"
    );
    assert_eq!(
        second["subscription_id"],
        json!("s2"),
        "the copies arrive in subscription order: {first} / {second}"
    );

    [first, second]
}

/// The wall clock, in the milliseconds the contract's `at` stamps are stated in.
fn wall_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_millis() as u64
}

/// The `session` scope's baseline kinds, in the contract's order (`protocol.ts:127`).
const SESSION_BASELINE: [&str; 11] = [
    "session",
    "definition",
    "turns",
    "queue",
    "actions",
    "usage",
    "threads",
    "session_agents",
    "session_tools",
    "commands",
    // Wave 1: the record store is the session snapshot's last baseline member
    // (`shared/harness-stream.ts` `HARNESS_WS_SCOPE_BASELINES`).
    "records",
];

/// The `session` scope's publish set, in the contract's order (`protocol.ts:74-86`).
const SESSION_PUBLISHES: [&str; 12] = [
    "session.updated",
    "turn.upsert",
    "turn.removed",
    "action.opened",
    "action.closed",
    "usage.updated",
    // Wave 1: `records.updated` rides the session scope, right after the usage counter
    // (`harness-stream.ts` `HARNESS_WS_SCOPE_PUBLISHES`).
    "records.updated",
    "thread.upsert",
    "thread.removed",
    "tool.registry_updated",
    "agent.registry_updated",
    "run.event",
];

// ---------------------------------------------------------------------------
// 1 - the handshake
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_upgrade_answers_101_with_the_subprotocol_and_the_epoch() {
    let root = Root::new("handshake");
    let server = TestServer::start(&root).await;

    // The epoch the REST surface reports is the one the 101 must carry: a client learns it is on a
    // different server before it sends a single frame.
    let health = Reply::send(client().get(server.url("/v1/harness/health"))).await;
    let epoch = health.epoch();

    let stream = Stream::connect(&server, Some("harness.v1")).await;

    assert_eq!(stream.status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        stream
            .header(&HeaderName::from_static("upgrade"))
            .as_deref(),
        Some("websocket"),
        "`transport.ts:657` - the 101 names the protocol it switched to"
    );
    assert_eq!(
        stream
            .header(&HeaderName::from_static("connection"))
            .as_deref(),
        Some("upgrade")
    );
    assert!(
        stream
            .header(&HeaderName::from_static("sec-websocket-accept"))
            .is_some(),
        "`Sec-WebSocket-Accept` is the hash the client verifies"
    );
    assert_eq!(
        stream.header(&PROTOCOL_HEADER).as_deref(),
        Some("harness.v1"),
        "the offered subprotocol is echoed, and only the offered one"
    );
    assert_eq!(
        stream.header(&EPOCH_HEADER).as_deref(),
        Some(epoch.as_str()),
        "`mount.ts:146` - `X-Harness-Epoch` rides the 101"
    );

    drop(stream);

    // A client that offered nothing must not be handed a vocabulary it did not ask for
    // (`transport.ts:647-653`), and the handshake still succeeds.
    let bare = Stream::connect(&server, None).await;

    assert_eq!(bare.status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        bare.header(&PROTOCOL_HEADER),
        None,
        "no offer, no echo: {:?}",
        bare.header(&PROTOCOL_HEADER)
    );

    bare.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 2 - `welcome`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn welcome_is_the_first_frame_and_carries_every_number_in_force() {
    let root = Root::new("welcome");
    let server = TestServer::start(&root).await;
    let epoch = Reply::send(client().get(server.url("/v1/harness/health")))
        .await
        .epoch();

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;
    let welcome = stream.expect_frame("welcome").await;

    let keys: Vec<&str> = welcome
        .as_object()
        .expect("a closed object")
        .keys()
        .map(String::as_str)
        .collect();

    assert_eq!(
        keys,
        ["type", "protocol", "epoch", "at", "limits"],
        "`WsWelcome` is a closed object"
    );
    assert_eq!(welcome["protocol"], json!("harness.v1"));
    assert_eq!(welcome["epoch"], json!(epoch));

    let at = welcome["at"].as_u64().expect("a millisecond stamp");

    assert!(
        at > 1_700_000_000_000 && at <= wall_now() + 5_000,
        "`at` is the server clock at connect: {at}"
    );

    // Every number in the contract's `x-websocket.limits` map, in the contract's spelling. A client
    // that hard-codes one of these and reads a different value back has a broken reconnect.
    assert_eq!(
        welcome["limits"],
        json!({
            "subscriptions_per_connection": 64,
            "ops_per_update_frame": 32,
            "delta_chars_per_append_op": 2000,
            "events_per_update_frame": 16,
            "ids_per_invalidation_frame": 64,
            "max_client_frame_bytes": 16384,
            "max_server_frame_bytes": 65536,
            "client_messages_per_second": 32,
            "heartbeat_interval_ms": 15000,
            "heartbeat_timeout_intervals": 3,
            "heartbeat_timeout_close_code": 4408,
            "replay_frames": 512,
            "replay_seconds": 60,
            "send_queue_frames": 256,
            "send_queue_bytes": 1048576,
            "sustained_stall_ms": 10000,
            "ack_timeout_ms": 5000,
            "revision_records_per_state": 21,
        })
    );

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 3 - `hello` and `ping`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hello_answers_nothing_and_ping_answers_pong_with_the_echoed_at() {
    let root = Root::new("hello-ping");
    let server = TestServer::start(&root).await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    // There is NO hello reply: the first frame after `hello` is the answer to whatever comes next,
    // which is the only way to prove nothing was sent in between.
    stream
        .expect_silence(
            json!({"type": "hello", "protocol": "harness.v1", "epoch": "unused", "cursor": "unused"}),
            41,
        )
        .await;

    // A second `hello` is not a contract condition either: the later one is ignored, and no error is
    // invented for it.
    stream.expect_silence(json!({"type": "hello"}), 42).await;

    // A `ping` with no `at` is answered with the server clock.
    stream.send(json!({"type": "ping"})).await;

    let echoed = stream.expect_frame("pong").await;

    assert_eq!(
        echoed
            .as_object()
            .expect("a closed object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["type", "at"],
        "`WsPong` is a closed object"
    );

    let at = echoed["at"].as_u64().expect("a millisecond stamp");

    assert!(
        at >= wall_now().saturating_sub(5_000) && at <= wall_now() + 5_000,
        "a `ping` with no `at` is answered with the server clock: {at}"
    );

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 4 - subscribe, the baseline, and a REST write arriving as an update
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_session_subscription_reports_the_baseline_and_a_rest_patch_arrives_as_an_update() {
    let root = Root::new("subscribe-session");
    let server = TestServer::start(&root).await;
    let epoch = Reply::send(client().get(server.url("/v1/harness/health")))
        .await
        .epoch();

    let (session_id, created) = create_session(&server, "baseline").await;
    let created_revision = created.revision();

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    let state = subscribe(&mut stream, "s1", &session_id).await;

    let keys: Vec<&str> = state
        .as_object()
        .expect("a closed object")
        .keys()
        .map(String::as_str)
        .collect();

    assert_eq!(
        keys,
        [
            "type",
            "subscription_id",
            "scope",
            "epoch",
            "sequence",
            "cursor",
            "revisions",
            "publishes"
        ],
        "a bare subscribe has no `filters_applied` and no `replayed_frames`"
    );
    assert_eq!(state["subscription_id"], json!("s1"));
    assert_eq!(state["scope"], json!("session"));
    assert_eq!(state["epoch"], json!(epoch));
    assert_eq!(
        state["sequence"],
        json!(0),
        "a fresh subscription is reconciled against 0, so every frame above it applies"
    );
    assert_eq!(
        state["cursor"],
        json!(encode_stream_cursor(&epoch, 0)),
        "the cursor is the opaque form of `epoch:sequence`"
    );

    let publishes: Vec<&str> = state["publishes"]
        .as_array()
        .expect("an array")
        .iter()
        .map(|kind| kind.as_str().unwrap())
        .collect();

    assert_eq!(
        publishes, SESSION_PUBLISHES,
        "`publishes` is the SCOPE's set, so `run.event` is listed even with the filter off"
    );

    // The per-scope baseline: one record per counter the scope can move, in the contract's order, and
    // never a resource body.
    let revisions = state["revisions"].as_array().expect("an array of records");

    let resources: Vec<&str> = revisions
        .iter()
        .map(|record| record["resource"].as_str().unwrap())
        .collect();

    assert_eq!(resources, SESSION_BASELINE);

    // One create touched `session`, `definition`, `turns` and `threads`; nothing else has a counter
    // yet, and "never moved in this epoch" reads as 0 - the same figure a REST page reports for a
    // collection nobody has written to.
    assert_eq!(
        revisions
            .iter()
            .map(|record| record["revision"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        // One more trailing zero: the `records` counter exists and has never moved.
        vec![1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0]
    );

    for record in revisions {
        assert_eq!(
            record["id"],
            json!(session_id),
            "a session-scoped counter is keyed by the session id"
        );
        assert_eq!(record["epoch"], json!(epoch));
    }

    assert_eq!(
        revisions[0]["revision"],
        json!(created_revision),
        "the `session` baseline record IS the counter the create's header carried"
    );

    // The REST write the stream must now mirror.
    let patched = patch_session(&server, &session_id, "streamed").await;
    let patched_revision = patched.revision();

    let update = stream.expect_frame("update").await;

    assert_eq!(update["subscription_id"], json!("s1"));
    assert_eq!(update["kind"], json!("session.updated"));
    assert_eq!(
        update["sequence"],
        json!(1),
        "sequences start at 1 on the first frame and increase by one"
    );
    assert_eq!(
        update["cursor"],
        json!(encode_stream_cursor(&epoch, 1)),
        "the cursor is the opaque form of this frame's `epoch:sequence`"
    );
    assert_eq!(update["session"]["id"], json!(session_id));
    assert_eq!(update["session"]["title"], json!("streamed"));
    assert_eq!(
        update["session"]["revision"],
        json!(created_revision),
        "the payload carries the counter as the MUTATION read it: `store/session.rs:339-354` builds the \
         patch before `Store::commit` moves `session`, so the frame quotes the pre-write figure"
    );

    // The cross-transport rule that actually decides freshness: a subscription opened AFTER the write
    // reports the counter the write's header carried. A client comparing `subscription_state.revisions`
    // with a REST read sees one number, in one epoch, on both transports.
    let reopened = subscribe(&mut stream, "s2", &session_id).await;
    let session_record = reopened["revisions"]
        .as_array()
        .expect("an array of records")
        .iter()
        .find(|record| record["resource"] == json!("session"))
        .expect("the session counter is in the session baseline")
        .clone();

    assert_eq!(
        session_record["revision"],
        json!(patched_revision),
        "`subscription_state.revisions` and `X-Harness-Revision` are the same counter"
    );

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 5 - cursor replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_cursor_resumes_a_parked_window_and_counts_the_frames_it_replays() {
    let root = Root::new("replay");
    let server = TestServer::start(&root).await;
    let epoch = Reply::send(client().get(server.url("/v1/harness/health")))
        .await
        .epoch();

    let (session_id, _) = create_session(&server, "first").await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;
    subscribe(&mut stream, "s1", &session_id).await;

    patch_session(&server, &session_id, "second").await;

    let first = stream.expect_frame("update").await;

    assert_eq!(first["sequence"], json!(1));
    assert_eq!(first["session"]["title"], json!("second"));

    patch_session(&server, &session_id, "third").await;

    let second = stream.expect_frame("update").await;

    assert_eq!(second["sequence"], json!(2));
    assert_eq!(second["session"]["title"], json!("third"));

    // `unsubscribe` answers NOTHING, and the parked window it leaves behind is what a later cursor
    // resumes from.
    stream
        .expect_silence(json!({"type": "unsubscribe", "subscription_id": "s1"}), 43)
        .await;

    stream
        .send(json!({
            "type": "subscribe",
            "subscription_id": "s2",
            "scope": "session",
            "session_id": session_id.clone(),
            "cursor": first["cursor"],
            "filters": {"events": false},
        }))
        .await;

    let resumed = stream.expect_frame("subscription_state").await;

    assert_eq!(resumed["subscription_id"], json!("s2"));
    assert_eq!(
        resumed["sequence"],
        json!(2),
        "the resume position is the window's own sequence, not the client's cursor"
    );
    assert_eq!(resumed["cursor"], json!(encode_stream_cursor(&epoch, 2)));
    assert_eq!(
        resumed["replayed_frames"],
        json!(1),
        "one frame was re-sent from the retained window"
    );
    assert_eq!(
        resumed["filters_applied"],
        json!({"events": false}),
        "`filters_applied` echoes exactly what the client asked for"
    );

    let replayed = stream.expect_frame("update").await;

    assert_eq!(
        replayed["subscription_id"],
        json!("s1"),
        "a replayed frame is re-sent EXACTLY as it was minted (`broker.ts:774` maps the retained frames \
         themselves), so it still names the subscription that first received it - the state frame is what \
         identifies this subscription"
    );
    assert_eq!(replayed["sequence"], json!(2));
    assert_eq!(replayed["cursor"], second["cursor"]);
    assert_eq!(replayed["session"]["title"], json!("third"));

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 6 - the epoch rule
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_cursor_from_another_epoch_answers_resync_required_and_registers_nothing() {
    let root = Root::new("epoch-mismatch");
    let server = TestServer::start(&root).await;
    let epoch = Reply::send(client().get(server.url("/v1/harness/health")))
        .await
        .epoch();

    let (session_id, _) = create_session(&server, "epochs").await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    let foreign = encode_stream_cursor("a-previous-process", 7);

    stream
        .send(json!({
            "type": "subscribe",
            "subscription_id": "s1",
            "scope": "session",
            "session_id": session_id.clone(),
            "cursor": foreign.clone(),
        }))
        .await;

    let resync = stream.expect_frame("resync_required").await;

    assert_eq!(resync["subscription_id"], json!("s1"));
    assert_eq!(
        resync["reason"],
        json!("epoch_mismatch"),
        "a cursor from a previous process is dead on arrival"
    );
    assert_eq!(resync["epoch"], json!(epoch));
    assert_eq!(
        resync["cursor"],
        json!(foreign),
        "the client's own position is echoed so the frame is comparable with what it sent"
    );
    assert!(
        !resync
            .as_object()
            .expect("a closed object")
            .contains_key("lost_frames"),
        "`lost_frames` is only ever a live-wire fact"
    );

    // `resources` names the bounded reads that restore this subscription - never a snapshot.
    let resources = resync["resources"].as_array().expect("an array of links");
    let kinds: Vec<&str> = resources
        .iter()
        .map(|link| link["resource"].as_str().unwrap())
        .collect();

    assert_eq!(kinds, SESSION_BASELINE);
    assert_eq!(
        resources[0],
        json!({
            "resource": "session",
            "id": session_id,
            "revision": 1,
            "epoch": epoch,
            "link": format!("/v1/harness/sessions/{session_id}"),
        }),
        "a link carries the counter it is behind on and the concrete bounded read"
    );
    assert_eq!(
        resources[2]["link"],
        json!(format!("/v1/harness/sessions/{session_id}/turns?limit=25")),
        "a paged link repeats its page size, so recovery is one bounded page"
    );

    // The refused subscribe registered nothing, and the connection stayed usable.
    assert_eq!(server.broker().subscriptions(), 0);

    stream.send(json!({"type": "ping", "at": 7})).await;

    let pong = stream.expect_frame("pong").await;

    assert_eq!(pong["at"], json!(7));

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 7 - unsubscribe, ack, and ack pacing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_ack_on_an_ordered_subscription_is_ignored_and_an_unknown_id_is_reported() {
    let root = Root::new("ack-ordered");
    let server = TestServer::start(&root).await;

    let (session_id, _) = create_session(&server, "ordered").await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;
    subscribe(&mut stream, "s1", &session_id).await;

    // "an `ack` on a subscription that did not ask for pacing is ignored rather than an error, so a
    // client may always ack".
    stream
        .expect_silence(
            json!({"type": "ack", "subscription_id": "s1", "sequence": 0}),
            44,
        )
        .await;

    // An id that is not open IS a condition, reported for the id the client named.
    stream
        .send(json!({"type": "unsubscribe", "subscription_id": "nope"}))
        .await;

    let error = stream.expect_frame("error").await;

    assert_eq!(
        error,
        json!({
            "type": "error",
            "code": "unknown_subscription",
            "message": "No subscription nope is open on this connection.",
            "subscription_id": "nope",
            "retryable": false,
        })
    );

    // The same for an ack on an id that is not open - precedence step 4.
    stream
        .send(json!({"type": "ack", "subscription_id": "nope", "sequence": 3}))
        .await;

    let error = stream.expect_frame("error").await;

    assert_eq!(error["code"], json!("unknown_subscription"));
    assert_eq!(error["subscription_id"], json!("nope"));

    stream.disconnect().await;
    server.stop().await;
}

#[tokio::test]
async fn an_ack_paced_subscription_sends_one_frame_and_waits_for_the_ack() {
    let root = Root::new("ack-paced");
    let server = TestServer::start(&root).await;
    let epoch = Reply::send(client().get(server.url("/v1/harness/health")))
        .await
        .epoch();

    let (session_id, _) = create_session(&server, "paced").await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    stream
        .send(json!({
            "type": "subscribe",
            "subscription_id": "s1",
            "scope": "session",
            "session_id": session_id.clone(),
            "pace": "ack",
        }))
        .await;

    stream.expect_frame("subscription_state").await;

    // Two mutations, and the stop-and-wait rule promises the client ONE frame until it acks.
    patch_session(&server, &session_id, "one").await;

    let first = stream.expect_frame("update").await;

    assert_eq!(first["sequence"], json!(1));
    assert_eq!(first["session"]["title"], json!("one"));

    patch_session(&server, &session_id, "two").await;

    // Nothing arrives for the second mutation, and the connection is demonstrably alive: the pong is
    // the next frame on the wire, so the held frame is held rather than merely late.
    stream.send(json!({"type": "ping", "at": 12})).await;

    let pong = stream.expect_frame("pong").await;

    assert_eq!(pong["at"], json!(12));

    stream
        .send(json!({"type": "ack", "subscription_id": "s1", "sequence": 1}))
        .await;

    let released = stream.expect_frame("update").await;

    assert_eq!(released["sequence"], json!(2));
    assert_eq!(released["cursor"], json!(encode_stream_cursor(&epoch, 2)));
    assert_eq!(
        released["session"]["title"],
        json!("two"),
        "the held frame is released in order, with the envelope it was minted with"
    );

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 8 - invalid frames
// ---------------------------------------------------------------------------

#[tokio::test]
async fn three_consecutive_union_failures_close_1007_and_a_single_one_does_not() {
    let root = Root::new("invalid-frames");
    let server = TestServer::start(&root).await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    // An unknown tag, then a stray field: each answers `invalid_payload` and keeps the connection.
    stream.send(json!({"type": "nope"})).await;

    let first = stream.expect_frame("error").await;

    assert_eq!(
        first,
        json!({
            "type": "error",
            "code": "invalid_payload",
            "message": "Unknown client frame type: nope.",
            "retryable": false,
        })
    );

    stream
        .send(json!({"type": "ping", "at": 1, "extra": 2}))
        .await;

    let second = stream.expect_frame("error").await;

    assert_eq!(
        second["message"],
        json!("A ping frame does not accept extra."),
        "every frame schema is `additionalProperties: false`"
    );

    // A frame that validates resets the streak, which is what "three CONSECUTIVE" means.
    stream.send(json!({"type": "ping", "at": 2})).await;

    let pong = stream.expect_frame("pong").await;

    assert_eq!(pong["at"], json!(2));

    for _ in 0..3 {
        stream
            .send(json!({"type": "subscribe", "subscription_id": "s1"}))
            .await;
    }

    for _ in 0..3 {
        let error = stream.expect_frame("error").await;

        assert_eq!(error["code"], json!("invalid_payload"));
        assert_eq!(error["message"], json!("A subscribe frame requires scope."));
    }

    let reason = stream.expect_close(1007).await;

    assert_eq!(reason, "Repeated invalid frames.");

    server.stop().await;
}

#[tokio::test]
async fn an_oversize_frame_answers_frame_too_large_and_closes_1009() {
    let root = Root::new("oversize");
    let server = TestServer::start(&root).await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    let raw = format!("{{\"type\":\"bye\",\"reason\":\"{}\"}}", "x".repeat(16_384));

    assert!(
        raw.len() > 16_384,
        "the probe is over the advertised cap: {}",
        raw.len()
    );

    stream.send_raw(&raw).await;

    let error = stream.expect_frame("error").await;

    assert_eq!(
        error,
        json!({
            "type": "error",
            "code": "frame_too_large",
            "message": "A client frame cannot exceed 16384 bytes.",
            "retryable": false,
            "close_code": 1009,
        }),
        "step (1) of the precedence is a sizing condition, not a schema one"
    );

    let reason = stream.expect_close(1009).await;

    assert_eq!(reason, "A client frame cannot exceed 16384 bytes.");

    server.stop().await;
}

#[tokio::test]
async fn a_non_json_frame_closes_1003_and_a_binary_frame_is_refused() {
    let root = Root::new("framing");
    let server = TestServer::start(&root).await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;
    stream.send_raw("not json").await;

    let error = stream.expect_frame("error").await;

    assert_eq!(
        error,
        json!({
            "type": "error",
            "code": "frame_too_large",
            "message": "A client frame must be a JSON text object.",
            "retryable": false,
            "close_code": 1003,
        })
    );

    let reason = stream.expect_close(1003).await;

    assert_eq!(reason, "A client frame must be a JSON text object.");

    drop(stream);

    // A binary frame never reaches the JSON layer at all: the transport refuses it with 1003 and the
    // sentence below the frames speak (`transport.ts:449`).
    let mut binary = Stream::connect(&server, Some("harness.v1")).await;

    binary.expect_frame("welcome").await;

    let _ = tokio::time::timeout(
        READ_TIMEOUT,
        binary
            .socket
            .send(WsMessage::Binary(b"{\"type\":\"ping\"}".to_vec().into())),
    )
    .await
    .expect("the binary frame is written");

    let reason = binary.expect_close(1003).await;

    assert_eq!(
        reason, "This conversation carries JSON text frames only",
        "`harness.swagger.yaml:2568` - a non-text frame is `unsupported_data`"
    );

    drop(binary);
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 9 - `bye`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bye_closes_with_1000_and_carries_the_client_reason() {
    let root = Root::new("bye");
    let server = TestServer::start(&root).await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;
    stream
        .send(json!({"type": "bye", "reason": "done here"}))
        .await;

    let reason = stream.expect_close(1000).await;

    assert_eq!(reason, "done here");

    drop(stream);
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 10 - the pre-upgrade gates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_handshake_gates_answer_http_status_and_not_a_close_frame() {
    let root = Root::new("gates");
    let server = TestServer::start(&root).await;
    let url = server.url(HARNESS_WS_STREAM_PATH);
    let http = client();

    // A policy refusal, before the upgrade and before any per-root state is touched.
    let refused = Reply::send(
        http.get(&url)
            .header("origin", "https://evil.example")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13"),
    )
    .await;

    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(
        refused.message(),
        "Cross-origin stream upgrades are not allowed."
    );

    // A plain GET that reaches the mount asked for an upgrade it did not send (`api.ts:3911`).
    let plain = Reply::send(http.get(&url)).await;

    assert_eq!(plain.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        plain.message(),
        "GET /stream expects a WebSocket upgrade (Sec-WebSocket-Key/Version) for the harness.v1 subprotocol."
    );
    // WAVE 2-C: a handshake refusal carries NO `X-Harness-Epoch` (`stream/transport.ts`
    // `rejectUpgrade` writes the body plus Content-Type/Content-Length/Connection only; the mount's
    // epoch header rides the 101 lines, `transport.ts:655-666`), so this pin asserts the absence
    // rather than the value. The 101 still stamps it, above.
    assert_eq!(
        plain.header(&EPOCH_HEADER),
        None,
        "the refusal names no epoch (`src/stream/mod.rs`'s `json_error` mirrors `rejectUpgrade`)"
    );

    // A verb that is not the upgrade's (`transport.ts:618`).
    let posted = Reply::send(http.post(&url)).await;

    assert_eq!(posted.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(posted.message(), "The stream upgrade is a GET.");

    // The tokens are there and the key is not, so there is nothing to sign (`transport.ts:630`). A
    // token LIST in `Connection` is still an upgrade, which is why this reaches the key gate.
    let no_key = Reply::send(
        http.get(&url)
            .header("connection", "keep-alive, Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13"),
    )
    .await;

    assert_eq!(no_key.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        no_key.message(),
        "A WebSocket upgrade requires Sec-WebSocket-Key."
    );

    // A client speaking another version, with the RFC's answer: 426 plus the version it must use.
    let wrong_version = Reply::send(
        http.get(&url)
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "8"),
    )
    .await;

    assert_eq!(wrong_version.status, StatusCode::UPGRADE_REQUIRED);
    assert_eq!(
        wrong_version.message(),
        "This stream speaks WebSocket protocol version 13."
    );
    assert_eq!(
        wrong_version.header(&HeaderName::from_static("sec-websocket-version")),
        Some("13".to_owned()),
        "`transport.ts:638` - the client is told which version to speak"
    );

    // The upgrade tokens are present but the protocol is not websocket (`transport.ts:623`).
    let wrong_upgrade = Reply::send(
        http.get(&url)
            .header("connection", "Upgrade")
            .header("upgrade", "h2")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13"),
    )
    .await;

    assert_eq!(wrong_upgrade.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        wrong_upgrade.message(),
        "The request is not a WebSocket upgrade."
    );

    // The exact-path gate names the path the client used, with the query string stripped. It runs
    // after the version gate, and it is only reachable this way because the router mounts one path.
    let off_mount = {
        let mut builder = Request::builder()
            .method("GET")
            .uri(format!("{HARNESS_WS_STREAM_PATH}/elsewhere?resume=ignored"));

        for (name, value) in UPGRADE_HEADERS {
            builder = builder.header(name, value);
        }

        let request = builder.body(Body::empty()).expect("a built request");

        let response = stream::upgrade(root.path().to_path_buf(), request).await;
        let status = response.status();
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("the refusal body")
            .to_bytes();
        let text = String::from_utf8(bytes.to_vec()).expect("a UTF-8 refusal body");

        (status, text)
    };

    assert_eq!(off_mount.0, StatusCode::NOT_FOUND);
    assert_eq!(
        off_mount.1,
        json!({"error": {"message": "No stream is mounted at /v1/harness/stream/elsewhere."}})
            .to_string()
    );

    // Every refusal is an HTTP answer with the envelope, and none of them touched a subscription.
    assert_eq!(server.broker().subscriptions(), 0);

    server.stop().await;
}

#[tokio::test]
async fn a_query_string_does_not_move_the_exact_path_gate() {
    let root = Root::new("gates-query");
    let server = TestServer::start(&root).await;

    // `GET /stream?...` still upgrades: the handshake carries NO resume state, so the query is
    // irrelevant and no resume value may be read from it (`harness.swagger.yaml:2363-2364`).
    let mut stream = Stream::connect_at(
        &format!("{}?resume=ignored", server.stream_url()),
        Some("harness.v1"),
    )
    .await;

    stream.expect_frame("welcome").await;
    stream.send(json!({"type": "ping", "at": 3})).await;

    let pong = stream.expect_frame("pong").await;

    assert_eq!(pong["at"], json!(3));

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 11 - the resource, duplicate, and filter refusals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unknown_session_id_answers_unknown_resource_and_leaves_the_connection_usable() {
    let root = Root::new("unknown-resource");
    let server = TestServer::start(&root).await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    stream
        .send(json!({
            "type": "subscribe",
            "subscription_id": "s1",
            "scope": "session",
            "session_id": "session-404",
        }))
        .await;

    let error = stream.expect_frame("error").await;

    assert_eq!(
        error,
        json!({
            "type": "error",
            "code": "unknown_resource",
            "message": "No session matches session-404.",
            "subscription_id": "s1",
            "retryable": false,
        }),
        "a well-formed id that does not exist is step (5), never a schema failure"
    );

    // A duplicate id on the SAME connection is a different condition.
    let (session_id, _) = create_session(&server, "duplicate").await;

    subscribe(&mut stream, "s2", &session_id).await;

    stream
        .send(json!({
            "type": "subscribe",
            "subscription_id": "s2",
            "scope": "session",
            "session_id": session_id.clone(),
        }))
        .await;

    let duplicate = stream.expect_frame("error").await;

    assert_eq!(duplicate["code"], json!("invalid_payload"));
    assert_eq!(
        duplicate["message"],
        json!("subscription_id s2 is already in use on this connection.")
    );

    // A filter the scope does not publish answers for THAT subscription only.
    stream
        .send(json!({
            "type": "subscribe",
            "subscription_id": "s3",
            "scope": "global",
            "filters": {"events": true},
        }))
        .await;

    let unsupported = stream.expect_frame("error").await;

    assert_eq!(
        unsupported,
        json!({
            "type": "error",
            "code": "unsupported_filter",
            "message": "global does not publish events.",
            "subscription_id": "s3",
            "retryable": false,
        })
    );

    // And the connection is still there to answer a ping.
    stream.send(json!({"type": "ping", "at": 5})).await;

    let pong = stream.expect_frame("pong").await;

    assert_eq!(pong["at"], json!(5));

    stream.disconnect().await;
    server.stop().await;
}

#[tokio::test]
async fn a_late_hello_is_refused_and_changes_nothing() {
    let root = Root::new("late-hello");
    let server = TestServer::start(&root).await;

    let (session_id, _) = create_session(&server, "late").await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;
    subscribe(&mut stream, "s1", &session_id).await;

    stream
        .send(json!({"type": "hello", "cursor": "a-foreign-cursor"}))
        .await;

    let error = stream.expect_frame("error").await;

    assert_eq!(
        error,
        json!({
            "type": "error",
            "code": "late_hello",
            "message": "hello must arrive before the first subscribe.",
            "retryable": false,
        }),
        "there is one resume input, and it is not this one any more"
    );

    // The refused hello must not have moved anything: a later subscribe without a cursor starts fresh
    // at the server's own position rather than trying to honour the refused cursor.
    let state = subscribe(&mut stream, "s2", &session_id).await;

    assert_eq!(state["sequence"], json!(0));
    assert!(
        !state
            .as_object()
            .expect("a closed object")
            .contains_key("replayed_frames")
    );

    stream.disconnect().await;
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 12 - the subscription budget
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_sixty_fifth_subscription_answers_too_many_subscriptions_and_closes_4401() {
    let root = Root::new("budget");
    let server = TestServer::start(&root).await;

    let (session_id, _) = create_session(&server, "budget").await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    // 64 registrations, each acknowledged by its own `subscription_state`, and none of them sharing an
    // id - the budget is per CONNECTION, and the same scope+ids pair may be subscribed twice.
    //
    // The frames are sent in groups of 16 with a full rate window between them: the budget probe needs
    // 65 messages, and the per-second budget is 32, so an unpaced burst would be refused by the RATE
    // limiter (4429) long before it reached the SUBSCRIPTION budget (4401).
    for index in 1..=64_u32 {
        if index > 1 && (index - 1) % 16 == 0 {
            tokio::time::sleep(Duration::from_millis(1_050)).await;
        }

        stream
            .send(json!({
                "type": "subscribe",
                "subscription_id": format!("s{index}"),
                "scope": "session",
                "session_id": session_id.clone(),
            }))
            .await;

        let state = stream.expect_frame("subscription_state").await;

        assert_eq!(state["subscription_id"], json!(format!("s{index}")));
    }

    assert_eq!(server.broker().subscriptions(), 64);

    tokio::time::sleep(Duration::from_millis(1_050)).await;

    stream
        .send(json!({
            "type": "subscribe",
            "subscription_id": "s65",
            "scope": "session",
            "session_id": session_id.clone(),
        }))
        .await;

    let error = stream.expect_frame("error").await;

    assert_eq!(
        error,
        json!({
            "type": "error",
            "code": "too_many_subscriptions",
            "message": "A connection carries at most 64 subscriptions.",
            "subscription_id": "s65",
            "retryable": false,
            "close_code": 4401,
        }),
        "the budget is the advertised number, and the frame names the subscription it refused"
    );

    stream.expect_close(4401).await;

    // The close released every subscription, and none of them had a frame to park.
    assert_eq!(server.broker().subscriptions(), 0);

    drop(stream);
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 13 - the message budget
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_per_second_message_budget_answers_rate_limited_and_closes_4429() {
    let root = Root::new("rate-limit");
    let server = TestServer::start(&root).await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    // 33 frames inside one window: the first 32 are answered, the 33rd is the refusal, and the
    // connection closes right after it. The burst stops AT the trip on purpose - a client that keeps
    // writing past it leaves frames in the server's receive queue when the server closes, and a socket
    // closed with unread data is RESET on Windows, which discards the pongs and the close frame the
    // server already sent. The refusal is the same answer either way; the burst just makes this test
    // race instead of pinning.
    for index in 1..=33_u32 {
        stream.send(json!({"type": "ping", "at": index})).await;
    }

    let mut pongs: Vec<u64> = Vec::new();
    let mut error: Option<Value> = None;

    loop {
        let message = tokio::time::timeout(READ_TIMEOUT, stream.socket.next())
            .await
            .expect("the conversation resolves")
            .expect("the stream yields a message")
            .expect("the socket is readable");

        let text = match message {
            WsMessage::Text(text) => text,
            WsMessage::Close(Some(frame)) => {
                assert_eq!(
                    u16::from(frame.code),
                    4429,
                    "the rate refusal closes with 4429"
                );
                assert_eq!(
                    frame.reason.as_str(),
                    "A client may send at most 32 messages per second.",
                    "the close reason is the refusal the client already read"
                );

                break;
            }
            other => panic!("an unexpected frame after the burst: {other:?}"),
        };

        let frame: Value = serde_json::from_str(text.as_str()).expect("a JSON frame");

        match frame["type"].as_str() {
            Some("pong") => pongs.push(frame["at"].as_u64().expect("the echoed `at`")),
            Some("error") => error = Some(frame),
            Some(other) => panic!("only a pong or the refusal may answer the burst: {other}"),
            None => panic!("a typed frame: {frame}"),
        }
    }

    assert_eq!(
        pongs,
        (1..=32_u64).collect::<Vec<_>>(),
        "`client_messages_per_second` is the advertised 32, in order, not an approximation"
    );
    assert_eq!(
        error.expect("the refusal frame"),
        json!({
            "type": "error",
            "code": "rate_limited",
            "message": "A client may send at most 32 messages per second.",
            "retryable": true,
            "close_code": 4429,
        }),
        "`retryable` is true for a rate refusal, because the client CAN resend"
    );

    drop(stream);
    server.stop().await;
}

// ---------------------------------------------------------------------------
// 14 - the host-facing close, which is what T6 wires to an epoch change
// ---------------------------------------------------------------------------

#[tokio::test]
async fn close_all_answers_every_live_connection_with_the_error_frame_and_the_close_code() {
    let root = Root::new("close-all");
    let server = TestServer::start(&root).await;

    let (session_id, _) = create_session(&server, "closing").await;

    let mut first = Stream::connect(&server, Some("harness.v1")).await;

    first.expect_frame("welcome").await;
    subscribe(&mut first, "s1", &session_id).await;

    let mut second = Stream::connect(&server, Some("harness.v1")).await;

    second.expect_frame("welcome").await;
    subscribe(&mut second, "s1", &session_id).await;

    assert_eq!(server.broker().subscriptions(), 2);

    server.broker().close_all(1012);

    for stream in [&mut first, &mut second] {
        let error = stream.expect_frame("error").await;

        assert_eq!(
            error,
            json!({
                "type": "error",
                "code": "internal",
                "message": "The stream is closing (code 1012).",
                "retryable": true,
                "close_code": 1012,
            }),
            "`broker.close` is the host's shutdown/restart path, and the frame states the reason"
        );

        stream.expect_close(1012).await;
    }

    assert_eq!(server.broker().subscriptions(), 0);

    drop(first);
    drop(second);
    server.stop().await;
}

/// The host's own shutdown step closes a live stream (`start.ts:319-327` closes the mount, then
/// disposes the store): the contract's `error` frame carrying `close_code: 1001`, then the close, with
/// NO client action required. `StreamBroker::close_all` had no production caller before T7a, so a
/// graceful host left every listener holding an open socket until hyper dropped the process.
#[tokio::test]
async fn the_host_shutdown_closes_a_live_stream_with_the_1001_frame() {
    let root = Root::new("shutdown-1001");
    let mut server = TestServer::start(&root).await;

    let (session_id, _) = create_session(&server, "shutdown").await;

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;
    subscribe(&mut stream, "s1", &session_id).await;

    server.signal_shutdown();

    let error = stream.expect_frame("error").await;

    assert_eq!(
        error,
        json!({
            "type": "error",
            "code": "internal",
            "message": "The stream is closing (code 1001).",
            "retryable": true,
            "close_code": 1001,
        }),
        "`go away` states its reason on the wire before it closes"
    );

    stream.expect_close(1001).await;

    assert_eq!(
        server.broker().subscriptions(),
        0,
        "the shutdown arm drains the subscription table, not just the socket"
    );

    // The store-side half of the same step: no pump claims new work behind the checkpoint.
    let active = runtime::cached_runtime(&root.path).expect("the runtime served this root");

    assert!(
        active.store.is_shutting_down(),
        "`dispose()` sets the store's shuttingDown flag (`sessions.ts:8638`)"
    );

    drop(stream);
    server.stop().await;
}

/// The owner-question channel end to end on the wire (T16 + `WsUpdateActionOpened` /
/// `WsUpdateActionClosed`, `harness-stream.ts:586-612`):
///
/// * `action.opened` carries `action` - one ready-to-answer row - and NO other payload field, and moves
///   the `actions` counter (`api.ts:1105-1125`);
/// * every settlement publishes `action.closed` with the contract's reason vocabulary, from the ONE
///   point that settles (`sessions.ts:2325-2335`): an owner answer is `answered`, a cancel-all is
///   `cancelled`;
/// * the `actions` counter the baseline and the REST read quote is the one those frames moved.
#[tokio::test]
async fn the_action_channel_publishes_the_opened_and_closed_frames() {
    let root = Root::new("action-frames");
    let server = TestServer::start(&root).await;

    let (session_id, _) = create_session(&server, "actions").await;

    let active = runtime::cached_runtime(&root.path).expect("the runtime served this root");
    let store = std::sync::Arc::clone(&active.store);

    let mut stream = Stream::connect(&server, Some("harness.v1")).await;

    stream.expect_frame("welcome").await;

    let state = subscribe(&mut stream, "s1", &session_id).await;
    let baseline = state["revisions"]
        .as_array()
        .expect("a baseline")
        .iter()
        .find(|record| record["resource"] == json!("actions"))
        .expect("the session scope baselines `actions`")
        .clone();

    assert_eq!(
        baseline["revision"],
        json!(0),
        "no question has been asked in this epoch"
    );

    // The question is raised through the SAME host a live run uses, so the frames are the production
    // ones; `turn-frame-1` stands in for the run that would own it.
    let host = StoreActionHost::new(
        store.inner_arc(),
        store.hub_arc(),
        &session_id,
        "turn-frame-1",
        None,
        "instruct",
    );
    let question = json!({
        "question": "Ship it?",
        "recommended": "yes",
        "options": [
            {"id": "yes", "label": "Ship it"},
            {"id": "no", "label": "Hold"},
        ],
    });

    let waiting = tokio::spawn(host.request(question.clone()));
    let opened = stream.expect_frame("update").await;

    assert_eq!(opened["kind"], json!("action.opened"));

    let keys: Vec<&str> = opened
        .as_object()
        .expect("a frame object")
        .keys()
        .map(String::as_str)
        .collect();

    assert_eq!(
        keys,
        [
            "type",
            "subscription_id",
            "sequence",
            "cursor",
            "kind",
            "at",
            "action"
        ],
        "`action.opened` carries `action` and no other payload field, in the union's declared order"
    );

    let row_keys: Vec<&str> = opened["action"]
        .as_object()
        .expect("a row object")
        .keys()
        .map(String::as_str)
        .collect();

    assert_eq!(
        row_keys,
        ["request", "turn_id", "agent", "received_at"],
        "the row is `pendingActionRowOf` (`api-reads.ts:1323`), and a threadless question omits it"
    );

    let request_id = opened["action"]["request"]["requestId"]
        .as_str()
        .expect("the host mints the request id")
        .to_owned();

    assert!(
        !request_id.is_empty(),
        "a minted id is never blank (`types/index.ts:865`)"
    );
    assert_eq!(
        opened["action"]["request"],
        json!({
            "requestId": request_id,
            "question": "Ship it?",
            "recommended": "yes",
            "options": [
                {"id": "yes", "label": "Ship it"},
                {"id": "no", "label": "Hold"},
            ],
            "allowCustom": true,
        }),
        "the recorded request rides verbatim, camelCase inside the recorded payload"
    );
    assert_eq!(opened["action"]["turn_id"], json!("turn-frame-1"));
    assert_eq!(opened["action"]["agent"], json!("instruct"));
    assert!(opened["action"]["received_at"].as_u64().is_some());

    // The counter this frame moved is the counter the REST read and a fresh baseline report.
    let listed = Reply::send(
        client().get(server.url(&format!("/v1/harness/sessions/{session_id}/actions"))),
    )
    .await;

    assert_eq!(listed.body["revision"], json!(1));
    assert_eq!(
        listed.body["actions"]
            .as_array()
            .expect("a list")
            .iter()
            .map(|row| row["request"]["requestId"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        vec![request_id.clone()],
        "the frame's row is the row the read lists"
    );

    let resubscribed = subscribe(&mut stream, "s2", &session_id).await;

    assert_eq!(
        resubscribed["revisions"]
            .as_array()
            .expect("a baseline")
            .iter()
            .find(|record| record["resource"] == json!("actions"))
            .expect("`actions` is baselined")["revision"],
        json!(1),
        "the baseline moves with the settlement channel, so a reconnect reconciles against it"
    );

    // ---- settlement 1: an owner answer closes it as `answered`. ----
    let answered = Reply::send(
        client()
            .post(server.url(&format!(
                "/v1/harness/sessions/{session_id}/actions/{request_id}/answer"
            )))
            .json(&json!({"choice": {"kind": "option", "option_id": "yes"}})),
    )
    .await;

    assert_eq!(
        answered.status,
        StatusCode::OK,
        "the answer is accepted: {}",
        answered.body
    );

    // The settlement reaches BOTH subscriptions.
    let [closed, sibling] = expect_update_pair(&mut stream, "action.closed").await;

    assert_eq!(
        closed["action_closed"],
        json!({"request_id": request_id.clone(), "reason": "answered"}),
        "the reason vocabulary is the contract's (`harness-stream.ts:600-612`)"
    );
    assert_eq!(
        sibling["action_closed"], closed["action_closed"],
        "one settlement, identical payload on every subscription"
    );
    assert_eq!(
        closed["sequence"],
        json!(2),
        "s1 numbers its own frames: the open was 1, this is 2"
    );
    assert_eq!(
        sibling["sequence"],
        json!(1),
        "s2 subscribed AFTER the open, so the close is ITS first frame"
    );

    let keys: Vec<&str> = closed
        .as_object()
        .expect("a frame object")
        .keys()
        .map(String::as_str)
        .collect();

    assert_eq!(
        keys,
        [
            "type",
            "subscription_id",
            "sequence",
            "cursor",
            "kind",
            "at",
            "action_closed"
        ],
        "`action.closed` carries `action_closed` and no other payload field, in the union's declared order"
    );

    // The parked tool call wakes with the recorded answer, verbatim. `branchId` is OPTIONAL in
    // `HarnessActionAnswer` (`types/index.ts:904-911`: "`branchId` is present"), and an owner answer
    // never has a branch, so the key is absent rather than null.
    assert_eq!(
        waiting.await.expect("the host task ran"),
        Ok(json!({
            "requestId": request_id,
            "choice": {"kind": "option", "optionId": "yes"},
            "source": "owner",
        })),
        "the resolved promise payload IS the recorded answer"
    );

    // ---- settlement 2: a cancel-all closes the next question as `cancelled`. ----
    let second_id = {
        let host = StoreActionHost::new(
            store.inner_arc(),
            store.hub_arc(),
            &session_id,
            "turn-frame-2",
            None,
            "instruct",
        );

        tokio::spawn(host.request(question));
        let [opened, sibling] = expect_update_pair(&mut stream, "action.opened").await;

        assert_eq!(
            sibling["action"], opened["action"],
            "the question reaches both subscriptions"
        );
        assert_eq!(opened["sequence"], json!(3), "s1's third frame");
        assert_eq!(sibling["sequence"], json!(2), "s2's second frame");

        opened["action"]["request"]["requestId"]
            .as_str()
            .expect("a minted id")
            .to_owned()
    };

    let cancelled = Reply::send(
        client()
            .post(server.url(&format!("/v1/harness/sessions/{session_id}/cancel")))
            .json(&json!({"all": true})),
    )
    .await;

    assert_eq!(
        cancelled.status,
        StatusCode::OK,
        "a cancel-all is a 200 whatever it found: {}",
        cancelled.body
    );

    let closed = expect_update_pair(&mut stream, "action.closed").await;

    assert_eq!(
        closed[0]["action_closed"],
        json!({"request_id": second_id, "reason": "cancelled"}),
        "`ACTION_CANCELLED_MESSAGE` classifies as `cancelled` (`sessions.ts:361-370`)"
    );
    assert_eq!(
        closed[1]["action_closed"], closed[0]["action_closed"],
        "the cancel-all settles the question once, and both subscriptions see that one settlement"
    );
    assert_eq!(
        closed
            .iter()
            .map(|frame| &frame["sequence"])
            .collect::<Vec<_>>(),
        vec![&json!(4), &json!(3)],
        "each subscription counts its own frames: s1 has seen four, s2 three"
    );

    assert_eq!(
        server.broker().subscriptions(),
        2,
        "the frames went to both subscriptions of this connection"
    );

    drop(stream);
    server.stop().await;
}
