//! The delivery surface: the static mirror, port resolution, and the probe surface.
//!
//! Every assertion here is a REAL request to a real socket, and where the behaviour lives in the
//! executable rather than the library, a REAL child process, because what this stage owns is delivery
//! facts: which bytes of `static.ts`'s cache header land on which file, which 404 answers a miss,
//! which port a probe is told about, and whether a SIGTERM actually reaps the process.
//!
//! WHAT EACH EXPECTED VALUE WAS DERIVED FROM
//! -----------------------------------------
//! Not from running this code and copying the output. Every literal below is read off the TypeScript
//! reference or the contract. The one rule that has no counterpart there - the absolute mirror
//! root - is named as an extension in the module that implements it:
//!
//!   * `packages/underload/harness/src/server/static.ts` - the 11-entry media-type table (`:13-24`),
//!     octet-stream for anything else (`:27`), `public, max-age=31536000, immutable` for a file whose
//!     path contains an `assets` DIRECTORY and `no-cache` otherwise (`:45-50`), `Content-Length` from
//!     `stat` and no body for `HEAD` (`:51-56`), GET/HEAD only (`:72`), `/` -> `index.html` (`:83`),
//!     the traversal rejection (`:84-89`), and the extensionless SPA fallback (`:93-97`);
//!   * `packages/underload/harness/src/server/start.ts` - `sendNotFound` (`:165-174`), the
//!     api -> mirror -> 404 order (`:404-414`), and the read-the-bound-port-back bind (`:421-440`);
//!   * `packages/underload/harness/src/server/handler.ts:277` - the API takes the mount and hands
//!     everything outside it to the mirror, which is why `/v1/harness/*` never paints the entry page;
//!   * `packages/underload/harness/src/server/dev.ts:101-105` and `cli/index.ts:376-380` - the port
//!     rule and the `invalid port: <what was typed>` refusal, `0` included as a valid value;
//!   * `packages/typecrate/harness/src/api/mod.rs` - the mount sentence and the
//!     `Unknown API route: <METHOD> <path>` sentence, asserted here UNCHANGED so the delivery surface
//!     is proven not to have swallowed the contract;
//!   * `src/harness-server.swagger.yaml` - declares `/v1/harness` paths only, no `text/html` response
//!     anywhere, which is why this crate answers no `/docs` either (wave W2 retired the page);
//!
//! Fixtures live in `Temp/typecrate-harness/delivery-surface/<pid>-<test>/` per the repo scratch rule,
//! and every test owns its own database root AND its own mirror directory: the mirror is read by path,
//! so a shared directory would let one test's file prove another test's assertion.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::http::{HeaderMap, HeaderName, StatusCode};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use typecrate_harness::api;
use typecrate_harness::contract;
use typecrate_harness::listen;
use typecrate_harness::manifest;
use typecrate_harness::server;

/// Seconds a graceful shutdown or a child-process handshake may take before the test calls it hung.
const WAIT: Duration = Duration::from_secs(20);

/// `static.ts:48`.
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// `static.ts:49`.
const CACHE_NO_CACHE: &str = "no-cache";

/// `start.ts:172-173`: the terminal 404's content type.
const PLAIN_TYPE: &str = "text/plain; charset=utf-8";

/// `start.ts:173`: and its whole body.
const PLAIN_BODY: &str = "Not found";

// The mirror's own fixtures. These strings are the assertions' ground truth: the bytes a request must
// return, and therefore the `Content-Length` each response must carry.
const INDEX_HTML: &str = "<!doctype html><title>mirror</title><p>entry</p>";
const ENTRY_SCRIPT: &str = "console.log('hashed asset');";
const DEEP_STYLES: &str = "body{margin:0}";
const DATA_JSON: &str = "{\"ok\":true,\"rows\":[1,2,3]}";
const UPPER_SCRIPT: &str = "var upper = 1;";
const WASM_BYTES: &[u8] = &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
/// The file OUTSIDE the mirror root that no request may ever read.
const SECRET: &str = "the-parent-directory-secret";

/// One fixture directory: a database root, a mirror root, and the parent that holds the secret file
/// the traversal tests must not leak.
struct Fixture {
    /// The per-test parent: the mirror is `<parent>/dist`, so `<parent>/secret.txt` is one `..` away.
    dir: PathBuf,
    /// The directory passed to `--static-root` (`start.ts`'s `staticRoot`).
    dist: PathBuf,
    /// The database root, so no test touches the shared default dump.
    database: PathBuf,
}

impl Fixture {
    fn new(test: &str) -> Self {
        let dir = typecrate_harness::package_root::normalize_lexically(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../../Temp/typecrate-harness/delivery-surface/{}-{test}",
                std::process::id()
            )),
        );

        let _ = fs::remove_dir_all(&dir);

        let dist = dir.join("dist");

        fs::create_dir_all(dist.join("assets").join("deep"))
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", dist.display()));
        fs::create_dir_all(dir.join("database"))
            .unwrap_or_else(|error| panic!("could not create the database root: {error}"));

        // The mirror's tree, written once per test. `index.` exists to pin that Node's `extname`
        // answer (`.`) counts as HAVING an extension, so the SPA fallback must not fire for it.
        write(&dist.join("index.html"), INDEX_HTML);
        write(&dist.join("assets").join("index-8Kq2.js"), ENTRY_SCRIPT);
        write(
            &dist.join("assets").join("deep").join("chunk-7f.css"),
            DEEP_STYLES,
        );
        write(&dist.join("data.json"), DATA_JSON);
        write(&dist.join("app.JS"), UPPER_SCRIPT);
        write(&dist.join("index."), "trailing-dot");
        fs::write(dist.join("module.wasm"), WASM_BYTES).expect("a binary fixture is writable");
        // A file INSIDE the mirror at a mount-shaped path: the dispatcher must still win there.
        fs::create_dir_all(dist.join("v1").join("harness"))
            .expect("the mount-shaped mirror directory is writable");
        write(
            &dist.join("v1").join("harness").join("health"),
            "the mirror must never answer inside the mount",
        );

        // And the one file the mirror must never reach.
        write(&dir.join("secret.txt"), SECRET);

        Self {
            dist,
            database: dir.join("database"),
            dir,
        }
    }

    /// Removes the fixture, so a rerun starts from the same tree.
    fn touch(&self, name: &str, text: &str) {
        write(&self.dist.join(name), text);
    }
}

fn write(path: &Path, text: &str) {
    fs::write(path, text)
        .unwrap_or_else(|error| panic!("could not write `{}`: {error}", path.display()));
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// One listener on an ephemeral loopback port, built by the SAME factory the executable calls.
struct Served {
    base: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Served {
    /// The API-only shape (`server::router`, i.e. what the `typecrate-services` gateway mounts).
    async fn start(database: &Path) -> Self {
        Self::spawn(database, server::Delivery::default()).await
    }

    /// The shape a standalone listener builds: mount + mirror, the reference's two members.
    async fn with_delivery(database: &Path, delivery: server::Delivery) -> Self {
        Self::spawn(database, delivery).await
    }

    async fn spawn(database: &Path, delivery: server::Delivery) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral loopback port is available");
        let local: SocketAddr = listener
            .local_addr()
            .expect("the bound listener reports its address");

        let (sender, receiver) = oneshot::channel::<()>();
        let path = database.to_path_buf();

        let task = tokio::spawn(async move {
            server::serve_on_with(listener, path, delivery, async move {
                let _ = receiver.await;
            })
            .await
            .expect("the listener serves until it is told to stop");
        });

        let base = format!("http://{local}");
        let probe = client();

        // One health read proves the socket is accepting, so no assertion races the runtime.
        for _ in 0..200 {
            if probe
                .get(format!("{base}{}", contract::HARNESS_WS_STREAM_PATH))
                .header("connection", "keep-alive")
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

    async fn stop(mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }

        if let Some(task) = self.task.take() {
            // The wait is part of the assertion: a serve that hangs past its signal never reaches
            // the `shutdown complete` tail (`start.ts:134`), and Drop alone would not notice.
            tokio::time::timeout(WAIT, task)
                .await
                .expect("the serve future settles once the signal lands")
                .expect("the serve future does not panic on the way out");
        }
    }
}

impl Drop for Served {
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
        .timeout(Duration::from_secs(60))
        .build()
        .expect("a plain HTTP client builds")
}

/// A response reduced to headers plus RAW TEXT, because a mirror body is html, javascript or bytes -
/// never the JSON the API tests parse.
struct Raw {
    status: StatusCode,
    headers: HeaderMap,
    text: String,
}

impl Raw {
    async fn send(request: reqwest::RequestBuilder) -> Self {
        let response = request
            .send()
            .await
            .unwrap_or_else(|error| panic!("the request completes: {error}"));

        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .expect("the body is readable")
            .to_vec();

        Self {
            status,
            headers,
            // Lossy on purpose: one fixture is binary, and the assertion against it is the header.
            text: String::from_utf8_lossy(&bytes).into_owned(),
        }
    }

    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(HeaderName::from_bytes(name.as_bytes()).expect("a valid header name"))
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }

    /// The three facts every static response is pinned on: media type, cache header, and the length
    /// that must equal the file's own size.
    fn assert_served(&self, content_type: &str, cache_control: &str, bytes: &[u8]) {
        assert_eq!(self.status, StatusCode::OK);
        assert_eq!(self.header("content-type").as_deref(), Some(content_type));
        assert_eq!(self.header("cache-control").as_deref(), Some(cache_control));
        assert_eq!(
            self.header("content-length").as_deref(),
            Some(bytes.len().to_string().as_str()),
            "`static.ts:51` stamps the size `stat` reported"
        );
        assert_eq!(self.text.as_bytes(), bytes);
    }

    /// The reference's terminal 404 (`start.ts:165-174`), which is what a mirror miss is.
    fn assert_plain_not_found(&self) {
        assert_eq!(self.status, StatusCode::NOT_FOUND);
        assert_eq!(self.header("content-type").as_deref(), Some(PLAIN_TYPE));
        assert_eq!(self.text, PLAIN_BODY);
        assert_eq!(
            self.header("content-length").as_deref(),
            Some(PLAIN_BODY.len().to_string().as_str())
        );
    }
}

/// A child `typecrate-harness-server`, with every listen-related variable REMOVED from the inherited
/// environment: the tests assert the built-in defaults, and an ambient `HARNESS_PORT` on the machine
/// running them must not move a single one.
fn child(database: &Path, args: &[&str]) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_typecrate-harness-server"));

    command
        .arg(args.first().copied().unwrap_or("serve"))
        .args(&args[1..])
        .arg("--database-root")
        .arg(database)
        .env_remove(listen::PORT_ENV)
        .env_remove(listen::HOST_ENV)
        .env_remove(listen::STATIC_ROOT_ENV)
        .env("HARNESS_THREAD_EXECUTION", "inline")
        .env("HARNESS_IO_EXECUTION", "inline")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    command
}

/// Runs one short-lived invocation (a refusal, or `parity-manifest`) to completion.
async fn run_once(fixture: &Fixture, args: &[&str]) -> std::process::Output {
    child(&fixture.database, args)
        .output()
        .await
        .expect("the executable runs")
}

/// The line a probe waits for, parsed into the JSON object it carries.
async fn listen_line(
    lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
) -> Value {
    let line = tokio::time::timeout(WAIT, lines.next_line())
        .await
        .expect("the listen line arrives")
        .expect("stdout is readable")
        .expect("stdout has a line");

    let (marker, payload) = line
        .split_once(' ')
        .unwrap_or_else(|| panic!("a `{}` line, got `{line}`", listen::LISTENING));

    assert_eq!(marker, listen::LISTENING, "the probe marker is one literal");

    serde_json::from_str(payload).unwrap_or_else(|error| panic!("the payload is JSON: {error}"))
}

/// Sends SIGTERM, the signal a supervisor and a plain `kill <pid>` send, and returns the exit status
/// plus everything the process wrote to stderr.
///
/// `tokio`'s own `kill()` is SIGKILL, which would prove nothing about the graceful path, so the
/// signal goes through the platform's `kill` (unix only, which is the only place SIGTERM exists).
#[cfg(unix)]
async fn terminate(child: &mut tokio::process::Child) -> (std::process::ExitStatus, String) {
    let pid = child.id().expect("the child is running").to_string();

    std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("SIGTERM is delivered");

    let status = tokio::time::timeout(WAIT, child.wait())
        .await
        .expect("the process stops on SIGTERM rather than ignoring it")
        .expect("the exit status is readable");

    // stderr closes with the process, so the whole drain transcript is readable afterwards. The
    // LAST line is the proof that the close order ran to the end: `shutdown complete` is printed
    // after `server::flush`, which is this crate's `dispose()` + `drainIo()`.
    let mut bytes = Vec::new();

    if let Some(mut pipe) = child.stderr.take() {
        tokio::time::timeout(WAIT, pipe.read_to_end(&mut bytes))
            .await
            .expect("stderr closes with the process")
            .expect("stderr is readable");
    }

    let drained = String::from_utf8_lossy(&bytes).into_owned();

    assert_eq!(
        status.code(),
        Some(0),
        "a signalled listener drains and exits 0 (`start.ts:319-340`)"
    );

    (status, drained)
}

// ---------------------------------------------------------------------------
// A1 - the retired docs page: the reference answers `/docs` nowhere
// ---------------------------------------------------------------------------

/// `GET /docs` was a crate invention, so the surface it displaced is what gets pinned now. The
/// reference publishes no HTML document: `api.ts:6149-6320` lists every route, none of them a
/// document; `src/harness-server.swagger.yaml` declares `/v1/harness` paths with no `text/html`
/// response; and `start.ts:404-414` hands everything outside the mount to the mirror or to
/// `sendNotFound`. On a mirror listener that makes `/docs` an ordinary extensionless route, which
/// `static.ts:93-97` renders as the entry document.
#[tokio::test]
async fn the_docs_path_is_the_mirrors_spa_route_and_not_a_generated_map() {
    let fixture = Fixture::new("docs-retired");
    let served = Served::with_delivery(
        &fixture.database,
        server::Delivery::with_static_root(fixture.dist.clone()),
    )
    .await;

    let reply = Raw::send(client().get(served.url("/docs"))).await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.header("content-type").as_deref(),
        Some("text/html; charset=utf-8"),
        "`static.ts:93-97`: an extensionless miss renders `index.html`"
    );
    assert_eq!(
        reply.header("cache-control").as_deref(),
        Some(CACHE_NO_CACHE),
        "`static.ts:49`: a page revalidates; only an `assets` directory is immutable"
    );
    assert_eq!(reply.text, INDEX_HTML, "the entry document, byte for byte");
    assert!(
        !reply.text.contains("<td class=\"id\">"),
        "there is no generated route map left to serve"
    );

    // `HEAD` follows the mirror's own rule, not a docs-page special case (`static.ts:51-56`).
    let head = Raw::send(client().head(served.url("/docs"))).await;

    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.text, "", "a HEAD body is empty");
    assert_eq!(
        head.header("content-length").as_deref(),
        Some(INDEX_HTML.len().to_string().as_str()),
        "HEAD carries the entry document's size"
    );

    // A write verb outside the mount is the terminal 404, NOT a 405 (`static.ts:72` declines and
    // `start.ts:409` sends `Not found`).
    let posted = Raw::send(client().post(served.url("/docs"))).await;

    posted.assert_plain_not_found();

    served.stop().await;
}

/// Without a delivery surface the router is the one the gateway mounts, and `/docs` is just another
/// unmounted path: the dispatcher's JSON sentence, not the plain 404.
#[tokio::test]
async fn a_listener_without_a_delivery_surface_keeps_the_dispatchers_answer() {
    let fixture = Fixture::new("no-delivery");
    let served = Served::start(&fixture.database).await;

    for path in ["/docs", "/", "/assets/index-8Kq2.js"] {
        let reply = Raw::send(client().get(served.url(path))).await;

        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert_eq!(
            reply.header("content-type").as_deref(),
            Some("application/json; charset=utf-8"),
            "an API-only listener answers its 404s as JSON, as it did before the mirror existed"
        );
    }

    let docs = Raw::send(client().get(served.url("/docs"))).await;

    assert_eq!(
        docs.text,
        format!(
            "{{\"error\":{{\"message\":\"This server serves {}; \\\"/docs\\\" is not \
             mounted.\"}}}}",
            contract::HARNESS_API_ROOT
        ),
        "the envelope is byte-stable, not pretty-printed"
    );

    served.stop().await;
}

// ---------------------------------------------------------------------------
// A2 - the static mirror
// ---------------------------------------------------------------------------

/// The whole cache-header matrix, in one table, against a real listener and real files.
#[tokio::test]
async fn the_mirror_cache_matrix_is_the_references() {
    let fixture = Fixture::new("cache-matrix");
    let served = Served::with_delivery(
        &fixture.database,
        server::Delivery::with_static_root(&fixture.dist),
    )
    .await;

    // A hashed asset under `assets/` is immutable: that is the ONE rule `static.ts:45-50` has, and
    // depth inside the directory does not change it.
    Raw::send(client().get(served.url("/assets/index-8Kq2.js")))
        .await
        .assert_served(
            "text/javascript; charset=utf-8",
            CACHE_IMMUTABLE,
            ENTRY_SCRIPT.as_bytes(),
        );
    Raw::send(client().get(served.url("/assets/deep/chunk-7f.css")))
        .await
        .assert_served(
            "text/css; charset=utf-8",
            CACHE_IMMUTABLE,
            DEEP_STYLES.as_bytes(),
        );

    // Everything else revalidates: the entry document, a JSON file, and a top-level script.
    Raw::send(client().get(served.url("/")))
        .await
        .assert_served(
            "text/html; charset=utf-8",
            CACHE_NO_CACHE,
            INDEX_HTML.as_bytes(),
        );
    Raw::send(client().get(served.url("/index.html")))
        .await
        .assert_served(
            "text/html; charset=utf-8",
            CACHE_NO_CACHE,
            INDEX_HTML.as_bytes(),
        );
    Raw::send(client().get(served.url("/data.json")))
        .await
        .assert_served(
            "application/json; charset=utf-8",
            CACHE_NO_CACHE,
            DATA_JSON.as_bytes(),
        );

    // The extension is matched case-insensitively (`static.ts:27`), and an unknown one is
    // octet-stream (`:27`'s fallback).
    Raw::send(client().get(served.url("/app.JS")))
        .await
        .assert_served(
            "text/javascript; charset=utf-8",
            CACHE_NO_CACHE,
            UPPER_SCRIPT.as_bytes(),
        );
    let binary = Raw::send(client().get(served.url("/module.wasm"))).await;

    assert_eq!(
        binary.header("content-type").as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(
        binary.header("content-length").as_deref(),
        Some(WASM_BYTES.len().to_string().as_str())
    );

    served.stop().await;
}

#[tokio::test]
async fn a_head_carries_the_size_of_the_file_and_no_bytes() {
    let fixture = Fixture::new("mirror-head");
    let served = Served::with_delivery(
        &fixture.database,
        server::Delivery::with_static_root(&fixture.dist),
    )
    .await;

    let head = Raw::send(client().head(served.url("/assets/index-8Kq2.js"))).await;

    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(
        head.header("content-type").as_deref(),
        Some("text/javascript; charset=utf-8")
    );
    assert_eq!(
        head.header("cache-control").as_deref(),
        Some(CACHE_IMMUTABLE)
    );
    assert_eq!(head.text, "");
    assert_eq!(
        head.header("content-length").as_deref(),
        Some(ENTRY_SCRIPT.len().to_string().as_str()),
        "the size of the FILE, not the size of the empty body (`static.ts:51-56`)"
    );

    served.stop().await;
}

/// A miss with an extension stays a 404; an extensionless route renders the entry document
/// (`static.ts:93-97`), which is what lets a client-side router own `/sessions/<id>`.
#[tokio::test]
async fn an_unknown_asset_404s_and_an_unknown_route_falls_back_to_the_document() {
    let fixture = Fixture::new("fallback");
    let served = Served::with_delivery(
        &fixture.database,
        server::Delivery::with_static_root(&fixture.dist),
    )
    .await;

    for path in [
        "/missing.png",
        "/assets/chunk-00.js",
        "/favicon.ico",
        "/nope.json",
    ] {
        let reply = Raw::send(client().get(served.url(path))).await;

        reply.assert_plain_not_found();
    }

    for path in ["/sessions", "/sessions/9c0f2b", "/deep/nested/route"] {
        let reply = Raw::send(client().get(served.url(path))).await;

        // The fallback serves the ENTRY DOCUMENT, so the media type and the cache header are html's
        // and no-cache's - not octet-stream, and not immutable.
        reply.assert_served(
            "text/html; charset=utf-8",
            CACHE_NO_CACHE,
            INDEX_HTML.as_bytes(),
        );
    }

    // A name with ANY extension is not extensionless, not even the odd `index.` (Node's `extname`
    // answers `.`, and the reference treats that as having one).
    let dotted = Raw::send(client().get(served.url("/index."))).await;

    assert_eq!(dotted.status, StatusCode::OK);
    assert_eq!(
        dotted.header("content-type").as_deref(),
        Some("application/octet-stream")
    );

    served.stop().await;
}

/// The mirror is GET/HEAD-only, and a decline is a fall-through, so every write verb lands on the
/// reference's plain 404 - not a 405, and not the JSON sentence that names the mount.
#[tokio::test]
async fn a_write_verb_on_the_mirror_namespace_is_the_plain_404() {
    let fixture = Fixture::new("verbs");
    let served = Served::with_delivery(
        &fixture.database,
        server::Delivery::with_static_root(&fixture.dist),
    )
    .await;

    for (method, path) in [
        ("POST", "/index.html"),
        ("PUT", "/index.html"),
        ("PATCH", "/data.json"),
        ("DELETE", "/assets/index-8Kq2.js"),
        ("POST", "/"),
        ("POST", "/sessions/9c0f2b"),
    ] {
        let reply = Raw::send(
            client()
                .request(method.parse().expect("a method"), served.url(path))
                .body("{}"),
        )
        .await;

        reply.assert_plain_not_found();

        assert_eq!(
            reply.header("x-harness-epoch").as_deref(),
            None,
            "a mirror answer never carries the contract headers - they belong to the dispatcher"
        );
    }

    served.stop().await;
}

/// Traversal is rejected before the filesystem, including an encoded `..` - and the proof is a real
/// file one directory up that must never come back.
#[tokio::test]
async fn traversal_outside_the_mirror_never_reads_the_parent() {
    let fixture = Fixture::new("traversal");
    let served = Served::with_delivery(
        &fixture.database,
        server::Delivery::with_static_root(&fixture.dist),
    )
    .await;

    for path in [
        "/../secret.txt",
        "/%2e%2e/secret.txt",
        "/%2E%2E/secret.txt",
        "/assets/../../secret.txt",
        "/./../secret.txt",
        "//secret.txt",
    ] {
        let reply = Raw::send(client().get(served.url(path))).await;

        assert!(
            !reply.text.contains(SECRET),
            "`{path}` leaked the parent: `{}`",
            reply.text
        );
    }

    // The one traversal that STAYS inside is honoured: `assets/../index.html` is the entry document.
    Raw::send(client().get(served.url("/assets/../index.html")))
        .await
        // `%2e%2e` inside the root decodes, resolves, and is still contained.
        .assert_served(
            "text/html; charset=utf-8",
            CACHE_NO_CACHE,
            INDEX_HTML.as_bytes(),
        );

    // A query string and a fragment never reach the file name (`new URL(...).pathname`).
    Raw::send(client().get(served.url("/index.html?v=7#top")))
        .await
        .assert_served(
            "text/html; charset=utf-8",
            CACHE_NO_CACHE,
            INDEX_HTML.as_bytes(),
        );

    // And a malformed escape is a fall-through, not a filesystem error.
    Raw::send(client().get(served.url("/bad%zz.txt")))
        .await
        .assert_plain_not_found();

    served.stop().await;
}

/// THE MOUNT BEATS THE MIRROR. `handler.ts:277` gives the API every path that merely starts with the
/// mount, so a mirror file at a mount-shaped path is never served and a mount miss is still the
/// dispatcher's JSON sentence naming the root.
#[tokio::test]
async fn the_mount_outranks_the_mirror_and_keeps_its_own_404() {
    let fixture = Fixture::new("mount-first");
    let served = Served::with_delivery(
        &fixture.database,
        server::Delivery::with_static_root(fixture.dist.clone()),
    )
    .await;

    // The fixture really does hold a file at <dist>/v1/harness/health; the dispatcher answers there.
    let health = Raw::send(client().get(served.url("/v1/harness/health"))).await;

    assert_eq!(health.status, StatusCode::OK);
    assert_eq!(
        health.header("content-type").as_deref(),
        Some("application/json; charset=utf-8")
    );
    assert!(health.header("x-harness-epoch").is_some());

    let unknown = Raw::send(client().get(served.url("/v1/harness/nope"))).await;

    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    assert_eq!(
        unknown.text,
        "{\"error\":{\"message\":\"Unknown API route: GET /v1/harness/nope\"}}"
    );

    // A path that merely LOOKS like the mount is still the dispatcher's, per the same `startsWith`
    // rule, and the mount it names is the first segment.
    let near = Raw::send(client().get(served.url("/v1/harnessXYZ"))).await;

    assert_eq!(near.status, StatusCode::NOT_FOUND);
    assert_eq!(
        near.text,
        format!(
            "{{\"error\":{{\"message\":\"This server serves {}; \\\"/v1\\\" is not \
             mounted.\"}}}}",
            contract::HARNESS_API_ROOT
        )
    );

    // Wave W2: a mirror file named `docs` IS the answer at `/docs`. The generated map is gone, so
    // the mirror owns the path like any other file, and an unknown extension is octet-stream
    // (`static.ts:91` a real file wins immediately, `:27` the media-type fallback).
    fixture.touch("docs", "the mirror's own docs file");

    let docs = Raw::send(client().get(served.url("/docs"))).await;

    assert_eq!(docs.status, StatusCode::OK);
    assert_eq!(docs.text, "the mirror's own docs file");
    assert_eq!(
        docs.header("content-type").as_deref(),
        Some("application/octet-stream")
    );

    served.stop().await;
}

// ---------------------------------------------------------------------------
// A3 + A5 - the executable's bind, probe line, and shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_parity_listener_reports_the_port_it_actually_bound() {
    let fixture = Fixture::new("parity-auto");
    let mut process = child(&fixture.database, &["parity"])
        .spawn()
        .expect("parity spawns");

    let stdout = process.stdout.take().expect("stdout is piped");
    let mut lines = BufReader::new(stdout).lines();
    let reported = listen_line(&mut lines).await;

    // `auto` is `--port 0`: the OS picked the port and the line reports THAT, never the sentinel.
    let port = reported["port"].as_u64().expect("a port");

    assert_ne!(
        port, 0,
        "the bound port is read back off the socket (`start.ts:439-440`)"
    );
    assert_eq!(reported["host"], Value::String("127.0.0.1".to_owned()));
    assert_eq!(
        reported["api"],
        Value::String(format!(
            "http://127.0.0.1:{port}{}",
            contract::HARNESS_API_ROOT
        ))
    );
    // The runtime id is the process epoch, so it is non-empty and identical to the header value.
    let runtime_id = reported["runtime_id"].as_str().expect("a runtime id");

    assert!(!runtime_id.is_empty());

    // The line is the ONLY thing on stdout: a probe reads one line and connects.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), lines.next_line())
            .await
            .is_err(),
        "parity prints nothing else on stdout"
    );

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::ZERO)
        .pool_max_idle_per_host(0)
        .build()
        .expect("a client");
    let health = client
        .get(format!(
            "http://127.0.0.1:{port}{}",
            contract::HARNESS_WS_STREAM_PATH
        ))
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "abc")
        .send()
        .await
        .expect("the probe listener answers an upgrade");

    assert_eq!(health.status(), StatusCode::SWITCHING_PROTOCOLS);

    let reply = client
        .get(format!(
            "http://127.0.0.1:{port}{}/health",
            contract::HARNESS_API_ROOT
        ))
        .send()
        .await
        .expect("the probe listener answers the contract");
    let epoch = reply
        .headers()
        .get("x-harness-epoch")
        .and_then(|value| value.to_str().ok())
        .expect("the epoch header")
        .to_owned();
    let body: Value = reply.json().await.expect("health is JSON");

    assert_eq!(body["ok"], Value::Bool(true));
    assert_eq!(
        epoch, runtime_id,
        "the runtime id a probe is handed is the value every response header quotes"
    );

    #[cfg(unix)]
    {
        let (_status, drained) = terminate(&mut process).await;

        // The last stderr line is the end of the close order: the listener stopped, the durable
        // document was checkpointed, and only then does the process leave (`start.ts:319-340`).
        assert_eq!(
            drained.lines().last(),
            Some("typecrate-harness-server: shutdown complete"),
            "the drain ran to the final state write"
        );
    }

    #[cfg(not(unix))]
    process.start_kill().expect("the child is stopped");
}

/// An explicit port is honoured exactly, `auto` reports the port the OS picked, and a taken one
/// refuses with the reference's loud refusal (`start.ts:421-440`): one bind, nothing killed.
#[tokio::test]
async fn the_port_is_explicit_or_auto_and_a_taken_bind_refuses() {
    let fixture = Fixture::new("ports");

    // Ask the OS for a port and hold it, so the collision under test is this test's own.
    let held = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("an ephemeral port");
    let taken: SocketAddr = held.local_addr().expect("the held port");
    let taken_port = taken.port().to_string();

    // (a) explicit port, free: the process reports exactly the port it was given.
    let free = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a second ephemeral port");
    let chosen = free.local_addr().expect("its port").port().to_string();

    drop(free);

    let mut exact = child(
        &fixture.database,
        &["parity", "--port", &chosen, "--host", "127.0.0.1"],
    )
    .spawn()
    .expect("parity spawns on an explicit port");
    let mut exact_lines = BufReader::new(exact.stdout.take().expect("stdout")).lines();
    let exact_report = listen_line(&mut exact_lines).await;

    assert_eq!(
        exact_report["port"],
        Value::Number(chosen.parse::<u64>().expect("a port number").into()),
        "an explicit port is bound as asked, not replaced"
    );
    // An explicit host rides through verbatim; only `auto` reports something the operator did not ask
    // for, and even then it is the port the socket actually bound (`start.ts:439-440`).
    assert_eq!(exact_report["host"], Value::String("127.0.0.1".to_owned()));
    assert!(
        exact_report.get("public_base_url").is_none(),
        "wave W2: the probe line reports no public origin, because the reference has none"
    );
    assert_eq!(
        exact_report["stream"],
        Value::String(format!(
            "ws://127.0.0.1:{chosen}{}",
            contract::HARNESS_WS_STREAM_PATH
        )),
        "the stream url is the same origin with the `ws` scheme, as `dev.ts:143` prints it"
    );
    assert_eq!(
        exact_report["api"],
        Value::String(format!(
            "http://127.0.0.1:{chosen}{}",
            contract::HARNESS_API_ROOT
        )),
        "the api url is built from the port that was actually bound"
    );

    #[cfg(unix)]
    {
        let (_status, drained) = terminate(&mut exact).await;

        assert_eq!(
            drained.lines().last(),
            Some("typecrate-harness-server: shutdown complete"),
            "the explicit-port listener drains the same way"
        );
    }

    // (b) the port is HELD, default retries: exit 1 and the "address already in use" sentence, with
    // nothing killed.
    let output = run_once(&fixture, &["parity", "--port", &taken_port]).await;

    assert_eq!(
        output.status.code(),
        Some(1),
        "a refused bind is \"could not serve\", not bad usage"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "",
        "the probe's stdout stays clean even on the refusal path"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim_end(),
        format!(
            "typecrate-harness-server: cannot bind {taken} - address already in use. \
             Nothing was killed; pass --host/--port or free the port first."
        )
    );

    // (c) wave W2: there is no free-port walk. `--port-retry` is an unknown option (0 counterparts
    // in the reference, whose `start.ts:421-440` performs one `server.listen` that rejects on
    // EADDRINUSE), and the refusal is the same exit 1 the reference's launchers use for bad usage.
    let walked = run_once(
        &fixture,
        &["parity", "--port", &taken_port, "--port-retry", "1"],
    )
    .await;

    assert_eq!(
        walked.status.code(),
        Some(1),
        "an unknown option is a failure"
    );
    assert_eq!(
        String::from_utf8_lossy(&walked.stderr).lines().next(),
        Some("typecrate-harness-server: unknown option `--port-retry`"),
        "the refusal names the flag, and the process bound nothing"
    );

    // (d) the held port STILL refuses after the walk was removed: the same address, the same
    // sentence, and no neighbour moved.
    let again = run_once(&fixture, &["parity", "--port", &taken_port]).await;

    assert_eq!(
        again.status.code(),
        Some(1),
        "a taken port refuses rather than steps aside"
    );

    drop(held);
}

#[tokio::test]
async fn a_parity_listener_serves_the_mirror_it_reports() {
    let fixture = Fixture::new("parity-delivery");
    let dist = fixture.dist.display().to_string();
    let mut process = child(
        &fixture.database,
        &["parity", "--static-root", &dist, "--host", "localhost"],
    )
    .spawn()
    .expect("parity spawns with a mirror on a hostname");

    let mut lines = BufReader::new(process.stdout.take().expect("stdout")).lines();
    let reported = listen_line(&mut lines).await;
    let port = reported["port"].as_u64().expect("a port");

    assert_eq!(reported["static_root"], Value::String(dist.clone()));
    // Wave W2: the probe line carries only what the socket and the router actually know. There is
    // no public origin, and `--host localhost` resolved to the loopback it bound.
    assert!(
        reported.get("public_base_url").is_none(),
        "the reference has no `publicBaseUrl` to report (`grep -rn publicBaseUrl src/` is empty)"
    );
    assert_eq!(reported["host"], Value::String("127.0.0.1".to_owned()));

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::ZERO)
        .pool_max_idle_per_host(0)
        .build()
        .expect("a client");

    // The real process, the real mirror: the cache header the file's own path decides.
    let asset = client
        .get(format!("{base}/assets/index-8Kq2.js"))
        .send()
        .await
        .expect("the mirror answers");

    assert_eq!(asset.status(), StatusCode::OK);
    assert_eq!(
        asset.headers()["cache-control"]
            .to_str()
            .expect("a cache header"),
        CACHE_IMMUTABLE
    );
    assert_eq!(
        asset.headers()["content-type"]
            .to_str()
            .expect("a content type"),
        "text/javascript; charset=utf-8"
    );
    assert_eq!(asset.text().await.expect("the body"), ENTRY_SCRIPT);

    // `/docs` on a real listener holding a real mirror: the entry document, because the reference
    // answers an extensionless SPA route with `index.html` (`static.ts:93-97`) and has no route map.
    let page = client
        .get(format!("{base}/docs"))
        .send()
        .await
        .expect("the mirror answers");

    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(
        page.headers()["content-type"]
            .to_str()
            .expect("a content type"),
        "text/html; charset=utf-8"
    );
    assert_eq!(page.text().await.expect("the body"), INDEX_HTML);

    let miss = client
        .post(format!("{base}/index.html"))
        .send()
        .await
        .expect("the miss answers");

    assert_eq!(miss.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        miss.headers()["content-type"]
            .to_str()
            .expect("a content type"),
        PLAIN_TYPE
    );
    assert_eq!(miss.text().await.expect("a body"), PLAIN_BODY);

    #[cfg(unix)]
    {
        let (_status, drained) = terminate(&mut process).await;

        assert_eq!(
            drained.lines().last(),
            Some("typecrate-harness-server: shutdown complete"),
            "a probe listener holding a mirror drains and checkpoints the same way"
        );
    }

    #[cfg(not(unix))]
    process.start_kill().expect("the child is stopped");
}

// ---------------------------------------------------------------------------
// The probe surface: the manifest, and the refusals an operator sees
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_manifest_subcommand_writes_the_table_the_router_mounts() {
    let fixture = Fixture::new("manifest");
    let out = fixture.dir.join("route-table.json");
    let out_path = out.display().to_string();

    let output = run_once(&fixture, &["parity-manifest", "--out", &out_path]).await;

    assert!(output.status.success(), "the manifest write succeeds");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "",
        "with --out the document goes to the file, and stdout stays empty"
    );

    let text = fs::read_to_string(&out).expect("the file was written");
    let parsed: Value = serde_json::from_str(&text).expect("the manifest is JSON");

    // The counts are the dispatcher's own, from the table `src/api/mod.rs` owns.
    assert_eq!(parsed["counts"]["templates"], Value::Number(52.into()));
    assert_eq!(parsed["counts"]["operations"], Value::Number(74.into()));
    assert_eq!(parsed["api_root"], Value::String("/v1/harness".to_owned()));
    assert_eq!(
        parsed["version"],
        Value::String(typecrate_harness::version().to_owned())
    );

    // And the rows are literally the mounted table, in the contract's declaration order.
    let rows = parsed["routes"].as_array().expect("the route rows");

    assert_eq!(rows.len(), api::ROUTES.len());

    for (row, route) in rows.iter().zip(api::ROUTES.iter()) {
        assert_eq!(row["id"], Value::String(route.id.to_owned()));
        assert_eq!(row["template"], Value::String(route.template.to_owned()));
        assert_eq!(
            row["operations"]
                .as_array()
                .expect("operations")
                .iter()
                .map(|operation| format!(
                    "{}:{}",
                    operation["method"].as_str().expect("a method"),
                    operation["operation"].as_str().expect("an operation id")
                ))
                .collect::<Vec<String>>(),
            route
                .operations
                .iter()
                .map(|(method, operation)| format!("{method}:{operation:?}"))
                .collect::<Vec<String>>(),
            "the manifest cannot claim an operation the router does not mount"
        );
    }

    // The delivery plane is reported separately, including the mirror this process serves. Wave W2:
    // the plane has the mirror and the terminal 404, and no docs entry of any kind.
    assert!(parsed["delivery_surface"].get("docs_page").is_none());
    assert_eq!(
        parsed["delivery_surface"]["not_found"]["body"],
        Value::String(PLAIN_BODY.to_owned())
    );
    assert_eq!(parsed["listen"]["default_port"], Value::Number(5181.into()));
    assert_eq!(
        parsed["listen"]["default_host"],
        Value::String("127.0.0.1".to_owned())
    );
    assert!(parsed["listen"].get("public_base_url_env").is_none());

    // `--out -` is the stdout form.
    let stdout_form = run_once(&fixture, &["parity-manifest", "--out", "-"]).await;

    assert!(stdout_form.status.success());
    assert_eq!(
        serde_json::from_str::<Value>(&String::from_utf8_lossy(&stdout_form.stdout))
            .expect("stdout carries the same document")["counts"]["templates"],
        Value::Number(52.into())
    );

    // And the library's own document is the one the bin wrote: no second table anywhere.
    assert_eq!(
        serde_json::from_str::<Value>(&text).expect("the file is JSON"),
        manifest::manifest_document(None),
        "an API-only process reports no mirror, which is the truth of this run"
    );
}

#[tokio::test]
async fn a_bad_port_or_a_relative_mirror_root_refuses_naming_the_value() {
    let fixture = Fixture::new("refusals");

    // `dev.ts:104` / `cli/index.ts:379`: `invalid port: ` plus what was typed. The reference's
    // launchers set `process.exitCode = 1` for a parse error, a bad port and a failed start
    // (`dev.ts:82-105`) and never use 2, so exit 1 is the pinned contract here too.
    let bad_port = run_once(&fixture, &["serve", "--port", "12s"]).await;

    assert_eq!(
        bad_port.status.code(),
        Some(1),
        "a bad port exits 1, as the reference sets it"
    );
    assert_eq!(
        String::from_utf8_lossy(&bad_port.stderr).lines().next(),
        Some("typecrate-harness-server: invalid port: 12s"),
        "the refusal is the launcher's sentence, quoted first"
    );
    assert_eq!(
        String::from_utf8_lossy(&bad_port.stdout),
        "",
        "a usage refusal binds nothing and prints no banner"
    );

    let out_of_range = run_once(&fixture, &["serve", "--port", "65536"]).await;

    assert_eq!(out_of_range.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&out_of_range.stderr).lines().next(),
        Some("typecrate-harness-server: invalid port: 65536")
    );

    // A relative mirror root is refused rather than resolved against the launch directory.
    let relative = run_once(&fixture, &["serve", "--static-root", "relative/dist"]).await;

    assert_eq!(relative.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&relative.stderr)
            .lines()
            .next()
            .unwrap_or_default(),
        format!(
            "typecrate-harness-server: {}: `relative/dist`",
            listen::STATIC_ROOT_REFUSAL
        ),
        "a relative mirror root is a usage refusal, not a resolve"
    );

    // Wave W2: a hostname is NOT a refusal any more. `server.listen(port, host)` takes one, so the
    // crate resolves it too; only a name nothing can resolve is refused, and the refusal is the
    // bind's failure (exit 1), not a usage error.
    let named = run_once(&fixture, &["serve", "--host", "no-such-host.invalid"]).await;

    assert_eq!(named.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&named.stderr).lines().next(),
        Some(
            "typecrate-harness-server: `no-such-host.invalid` --host is not a valid address or hostname \
             (expected <host>, <host>:<port>, [<ipv6>], or [<ipv6>]:<port>)"
        )
    );

    // The flags the reference never had are refused as unknown options rather than ignored.
    for flag in ["--port-retry", "--public-base-url", "--no-docs"] {
        let refused = run_once(&fixture, &["serve", flag, "1"]).await;

        assert_eq!(
            refused.status.code(),
            Some(1),
            "`{flag}` is not a flag of this CLI"
        );
        assert_eq!(
            String::from_utf8_lossy(&refused.stderr).lines().next(),
            Some(format!("typecrate-harness-server: unknown option `{flag}`").as_str())
        );
    }

    // An unknown command names the command, and `--help` is exit 0 with the surface documented.
    let unknown = run_once(&fixture, &["stat"]).await;

    assert_eq!(unknown.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&unknown.stderr).lines().next(),
        Some("typecrate-harness-server: unknown command `stat`")
    );

    let help = run_once(&fixture, &["--help"]).await;

    assert_eq!(help.status.code(), Some(0));
    let printed = String::from_utf8_lossy(&help.stdout);

    for fact in [
        "parity-manifest",
        "PARITY-LISTEN",
        "--static-root",
        "--port <port|auto>",
        "1 for anything else - bad usage, a bind that",
    ] {
        assert!(printed.contains(fact), "`--help` never mentions `{fact}`");
    }

    // The invented surface is absent from the documentation as well as from the router.
    for absent in ["/docs", "--port-retry", "--public-base-url", "--no-docs"] {
        assert!(
            !printed.contains(absent),
            "`--help` still advertises the removed `{absent}`"
        );
    }
}
