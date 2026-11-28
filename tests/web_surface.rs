//! Integration tests for the three web tools AT THE DELIVERY BOUNDARY: the tool registry, the
//! model result budget, and the session folder a downloaded file is expected to die in.
//!
//! WHAT THIS FILE PINS (and why `src/tools/web.rs`'s own tests cannot)
//! ------------------------------------------------------------------
//! The unit tests inside `web.rs` pin the MIRRORS of the frozen TypeScript: the fit ladders, the
//! refusal sentences, the disposition parser, the SSRF corpus, by calling the helpers directly.
//! Everything here is about the seam those calls cannot see:
//!
//!   * **delivery, not just size.** `web.rs` proves a worst-case fetch or search result measures at
//!     most `MODEL_TOOL_RESULT_BUDGET_BYTES`. What the model actually gets is decided one layer up,
//!     in `result::completed_outcome` (`tools/mod.rs:470`), which serializes the value and runs
//!     `bound_tool_result` - and that step can REPLACE an oversized document with a REFUSAL. These
//!     tests assert the outcome is `ok` (never JSON-refused) AND within the cap, which is the
//!     property the review asked the port to keep.
//!   * **the destination wiring.** `download` writes under `<sessions root>/<session id>/downloads`
//!     (`webdownload/download.tool.ts:294-311`), and this crate supplies the sessions root from
//!     `persistence::sessions_dir(root)` (`runtime.rs:560`) - the same per-session directory
//!     `Store::delete_session` removes (`store/session.rs:482`). That shared-directory claim is a
//!     cross-module fact, so it is tested across the module boundary.
//!   * **the gate in production clothing.** The corpus in `ssrf.rs` calls the predicate; these tests
//!     call the TOOL through the registry and prove a refusal costs no socket and no folder.
//!
//! NO NETWORK: every response comes from a one-connection-per-request stub on loopback, reached
//! either through `WebKeys::endpoint_base` (fetch/search) or through a PUBLIC host name pinned to
//! the stub by `WebKeys::connect_to` while `dns_answers` answers TEST-NET-3 (download is the gated
//! tool and may not dial a private address on its own account).
//!
//! FIXTURES live in the repo-root `Temp/typecrate-harness/web/<pid>-<seq>-<name>/` and are removed
//! on `Drop`, per the repo scratch-space rule (`AGENTS.md`).

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use typecrate_harness::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;
use typecrate_harness::payload::model_payload_bytes;
use typecrate_harness::persistence;
use typecrate_harness::ports::{ToolExecCtx, ToolExecOutcome, ToolPolicy, ToolRuntime};
use typecrate_harness::tools::{Options, ToolRegistry, web::WebKeys};

/// Unique per process AND per test, so a parallel `cargo test` never shares a directory.
static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// One disposable store ROOT, removed when the test ends.
///
/// It is deliberately shaped like the runtime's own root: the download folder under test is
/// `persistence::sessions_dir(root)/<session>/downloads`, which is what `runtime.rs:560` hands the
/// registry and what a session delete removes.
struct Fixture {
    directory: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = typecrate_harness::package_root::repo_root()
            .join("Temp")
            .join("typecrate-harness")
            .join("web")
            .join(format!(
                "{pid:0>9}-{sequence:0>4}-{name}",
                pid = std::process::id()
            ));

        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("a fixture directory is creatable");

        Self { directory }
    }

    /// The directory a session's downloads live in - the same composition `runtime.rs` supplies.
    fn sessions_root(&self) -> PathBuf {
        persistence::sessions_dir(&self.directory)
    }

    /// The one folder one session owns, as the tool composes it.
    fn session_downloads(&self, session_id: &str) -> PathBuf {
        self.sessions_root().join(session_id).join("downloads")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

// ---------------------------------------------------------------------------
// The stub origin
// ---------------------------------------------------------------------------

/// One queued response: status line, headers, then the body bytes.
struct Reply {
    status: u16,
    reason: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    fn text(headers: &[(&str, &str)], body: &str) -> Self {
        Self {
            status: 200,
            reason: "OK",
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            body: body.as_bytes().to_vec(),
        }
    }

    /// A 3xx with a `Location`, and no body.
    fn redirect(location: &str) -> Self {
        Self {
            status: 302,
            reason: "Found",
            headers: vec![("location".to_owned(), location.to_owned())],
            body: Vec::new(),
        }
    }

    fn render(&self) -> String {
        let mut head = format!("HTTP/1.1 {} {}\r\n", self.status, self.reason);

        for (name, value) in &self.headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }

        // `connection: close` is what lets a body with no content-length be EOF-delimited, which is
        // how a test produces a response with NO declaration at all.
        head.push_str("connection: close\r\n\r\n");
        head
    }
}

/// A loopback origin that answers each connection with the next queued reply IN ORDER, logging the
/// request line of every one. One reply per connection matches how the tools issue requests (one
/// per hop), and `connection: close` ends each exchange cleanly.
async fn stub_origin(
    replies_for: impl FnOnce(&str) -> Vec<Reply>,
) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("an ephemeral port");
    let address = listener.local_addr().expect("a bound address");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let base = format!("http://127.0.0.1:{}", address.port());
    let queue = Arc::new(Mutex::new(VecDeque::from(replies_for(&base))));
    // The task reports what it received; the test keeps its own handle on the same log.
    let logged = Arc::clone(&seen);

    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };

            // A GET carries no body, so the header block is the whole request.
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4_096];

            loop {
                let read = socket.read(&mut chunk).await.unwrap_or(0);

                if read == 0 {
                    break;
                }

                request.extend_from_slice(&chunk[..read]);

                if String::from_utf8_lossy(&request).contains("\r\n\r\n") {
                    break;
                }
            }

            let first_line = String::from_utf8_lossy(&request)
                .lines()
                .next()
                .unwrap_or_default()
                .to_owned();
            let reply = queue
                .lock()
                .expect("the reply queue")
                .pop_front()
                .expect("a reply for every request");
            let head = reply.render();

            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&reply.body).await;
            let _ = socket.shutdown().await;
            seen.lock().expect("the request log").push(first_line);
        }
    });

    (base, logged, server)
}

/// The fetch/search keys: every provider endpoint resolves against the stub, which keeps each
/// provider's path and replaces only the authority. Fetch and search are NOT SSRF gated (the module
/// header on `tools/web.rs` states why), so they may dial the loopback stub directly.
fn proxied_keys(base: &str) -> WebKeys {
    WebKeys {
        exa: Some("exa-stub-key".to_owned()),
        endpoint_base: Some(base.to_owned()),
        ..WebKeys::default()
    }
}

/// The download keys: the tool may only dial a PUBLIC address, so the stub is addressed by a public
/// name (`dl.example`) that `dns_answers` resolves to TEST-NET-3 (documented-public) while
/// `connect_to` lands the socket on loopback. Without this seam no gated download could be tested
/// against a local origin at all.
///
/// `localhost` is answered with loopback ON PURPOSE: as a NAME it is public (the reference's own
/// quirk), so only the resolver can refuse it - which is what a production resolver does, and what
/// gate 2 exists for.
fn download_keys(base: &str) -> WebKeys {
    let port = base
        .rsplit(':')
        .next()
        .and_then(|tail| tail.parse::<u16>().ok())
        .unwrap_or(80);

    WebKeys {
        dns_answers: BTreeMap::from([
            (
                "dl.example".to_owned(),
                vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                    203, 0, 113, 7,
                ))],
            ),
            (
                "localhost".to_owned(),
                vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            ),
        ]),
        connect_to: BTreeMap::from([(
            "dl.example".to_owned(),
            vec![SocketAddr::from(([127, 0, 0, 1], port))],
        )]),
        ..WebKeys::default()
    }
}

/// The same origin under its public name: `http://127.0.0.1:PORT/x` -> `http://dl.example:PORT/x`.
fn public_url(base: &str, path: &str) -> String {
    format!(
        "http://dl.example{}{path}",
        base.strip_prefix("http://127.0.0.1").unwrap_or("")
    )
}

/// A registry carrying one set of web keys plus the sessions root the real runtime supplies.
fn registry(keys: WebKeys, downloads_root: Option<&Path>) -> Arc<ToolRegistry> {
    ToolRegistry::with_options(Options {
        web_keys: keys,
        fetch_enabled: true,
        downloads_root: downloads_root.map(PathBuf::from),
    })
}

/// One call through the registry - the shape a turn hands the runtime.
async fn call(
    runtime: &ToolRegistry,
    name: &str,
    arguments: Value,
    session_id: Option<&str>,
) -> ToolExecOutcome {
    runtime
        .execute(ToolExecCtx {
            name: name.to_owned(),
            arguments,
            call_id: format!("call-{name}"),
            session_id: session_id.map(str::to_owned),
            cwd: std::env::temp_dir(),
            signal: CancellationToken::new(),
            policy: ToolPolicy::default(),
            action_host: None,
            task_host: None,
        })
        .await
}

/// Parses an outcome's committed content.
fn delivered(outcome: &ToolExecOutcome) -> Value {
    serde_json::from_str(&outcome.content).expect("a committed tool result is JSON")
}

// ---------------------------------------------------------------------------
// The delivery ceiling
// ---------------------------------------------------------------------------

/// THE PROPERTY THE REVIEW ASKED FOR, AT THE BOUNDARY: a whole page larger than the model result
/// budget is DELIVERED (a truncated page with a resume offset), never replaced by a budget refusal,
/// and what is delivered measures at most the cap.
///
/// The page is 70,000 `é` - two bytes each in a JSON document - so the content budget bites long
/// before the character count would. That currency choice is the reference's own (`fitResult`,
/// `webfetch/index.ts`), and the exact character figure is pinned in `web.rs`; what belongs HERE is
/// `ok` plus the byte ceiling measured the way the cap measures it (`model_payload_bytes` over the
/// committed text, the same figure `bound_tool_result` gated on).
#[tokio::test]
async fn a_whole_page_over_the_budget_is_delivered_rather_than_refused() {
    let page = format!(
        "<html><head><title>Two bytes each</title></head><body><p>{}</p></body></html>",
        "\u{e9}".repeat(70_000)
    );
    let (base, seen, server) = stub_origin(|_base| {
        vec![Reply::text(
            &[("content-type", "text/html; charset=utf-8")],
            &page,
        )]
    })
    .await;

    let outcome = call(
        &registry(proxied_keys(&base), None),
        "fetch",
        json!({ "url": format!("{base}/page") }),
        Some("session-1"),
    )
    .await;

    assert!(
        outcome.ok,
        "an over-budget page must be delivered, not refused: {:?}",
        outcome.error
    );

    let committed = model_payload_bytes(&outcome.content);

    assert!(
        committed <= MODEL_TOOL_RESULT_BUDGET_BYTES,
        "the delivered page costs {committed} payload bytes, over the {MODEL_TOOL_RESULT_BUDGET_BYTES} cap"
    );

    // Delivered means usable: a substantial page body plus the offset to resume from. The exact
    // figure is `web.rs`'s business; an empty or stub page is this test's business.
    let content = delivered(&outcome);
    let characters = content["characters"].as_u64().expect("a character count");

    assert!(
        characters > 10_000,
        "the fit ladder should still deliver most of a page, got {characters} characters"
    );
    assert_eq!(content["truncated"], json!(true));
    assert_eq!(
        content["nextOffset"],
        json!(characters),
        "the resume offset is the next code unit after the delivered text"
    );
    assert_eq!(seen.lock().expect("the request log").len(), 1);
    server.abort();
}

/// The same property on the other side of the provider boundary: 20 fat rows are TRIMMED to what
/// fits and the result still arrives. A search that answered "result too large" instead of "here
/// are 7 of 20" would turn a busy query into a failed turn.
#[tokio::test]
async fn an_oversized_search_is_delivered_rather_than_refused() {
    // One quote escapes to `\"` (2 bytes), so a 1,000-quote excerpt costs 2,000 payload bytes plus
    // the row's own frame: 20 of them cannot fit the 32 KiB result cap. Exa's excerpt source is the
    // `highlights` LIST (`websearch/request.ts:193`), not `text` - a fixture that names the wrong
    // field produces 20 EMPTY snippets, all of which fit, and the trim the test is watching for
    // never happens.
    let rows = vec![
        json!({
            "title": "A result with a very long excerpt",
            "url": "https://example.com/fat",
            "highlights": ["\"".repeat(1_000)],
        });
        20
    ];
    let payload = json!({ "results": rows }).to_string();
    let (base, seen, server) = stub_origin(|_base| {
        vec![Reply::text(
            &[("content-type", "application/json")],
            &payload,
        )]
    })
    .await;

    let outcome = call(
        &registry(proxied_keys(&base), None),
        "search",
        json!({ "query": "harness", "maxResults": 40, "provider": "exa" }),
        Some("session-1"),
    )
    .await;

    assert!(
        outcome.ok,
        "an over-budget row set must be trimmed, not refused: {:?}",
        outcome.error
    );

    let content = delivered(&outcome);
    let committed = model_payload_bytes(&outcome.content);

    assert!(
        committed <= MODEL_TOOL_RESULT_BUDGET_BYTES,
        "the delivered search costs {committed} payload bytes, over the {MODEL_TOOL_RESULT_BUDGET_BYTES} cap"
    );
    assert_eq!(content["resultsTruncated"], json!(true));
    assert_eq!(
        content["results"].as_array().expect("rows").len(),
        7,
        "the note names the kept rows, so they must be the number the fit ladder computed"
    );
    assert!(
        content["note"]
            .as_str()
            .expect("a note naming what was dropped")
            .contains("32768"),
        "the model has to be told which budget bit: {:?}",
        content["note"]
    );
    assert_eq!(seen.lock().expect("the request log").len(), 1);
    server.abort();
}

// ---------------------------------------------------------------------------
// The session folder
// ---------------------------------------------------------------------------

/// THE CROSS-MODULE CLAIM: the folder `download` writes into is INSIDE the per-session directory
/// `delete_session` removes, so a session's downloaded bytes cannot outlive it
/// (`webdownload/download.tool.ts:294-311` is the reference's same rule; `runtime.rs:560` is this
/// crate's one wiring of it, and `store/session.rs:482` is the deletion).
#[tokio::test]
async fn a_download_lands_in_the_folder_the_session_delete_removes() {
    let (base, _seen, server) = stub_origin(|_base| {
        vec![Reply::text(
            &[("content-type", "image/png"), ("content-length", "11")],
            "pixel-bytes",
        )]
    })
    .await;
    let fixture = Fixture::new("download-folder");

    let outcome = call(
        &registry(download_keys(&base), Some(&fixture.sessions_root())),
        "download",
        json!({ "url": public_url(&base, "/pixel.png") }),
        Some("session-1"),
    )
    .await;

    assert!(outcome.ok, "{:?}", outcome.error);

    let summary = delivered(&outcome);
    let expected = fixture.session_downloads("session-1").join("pixel.png");

    // The summary's own `path` is the file, and the file is where the summary says it is.
    assert_eq!(summary["saved"], json!(true));
    assert_eq!(summary["path"], json!(expected.to_string_lossy()));
    assert_eq!(summary["name"], json!("pixel.png"));
    assert_eq!(
        std::fs::read(&expected).expect("the written file"),
        b"pixel-bytes"
    );

    // And the deletion the store performs - `remove_dir_all(sessions_dir/<id>)` - takes the download
    // with it, because the folder is UNDER that directory rather than beside it. A different root
    // (the old `.underload/downloads/` default) would leave this file behind.
    std::fs::remove_dir_all(fixture.sessions_root().join("session-1")).expect("a session delete");

    assert!(
        !expected.exists(),
        "a deleted session must not leave its bytes behind"
    );
    server.abort();
}

/// The bytes travel on the ATTACHMENT; the model reads the summary. At the boundary that means the
/// committed content is a small summary and the media slot carries the DESCRIPTOR (no base64), which
/// is what keeps a multi-megabyte download from spending the model's result budget.
#[tokio::test]
async fn a_download_reports_a_summary_and_carries_the_bytes_as_media() {
    let body = "x".repeat(4_096);
    let (base, _seen, server) = stub_origin(|_base| {
        vec![Reply::text(
            &[
                ("content-type", "application/octet-stream"),
                ("content-length", &body.len().to_string()),
            ],
            &body,
        )]
    })
    .await;
    let fixture = Fixture::new("download-media");

    let outcome = call(
        &registry(download_keys(&base), Some(&fixture.sessions_root())),
        "download",
        json!({ "url": public_url(&base, "/blob.bin") }),
        Some("session-1"),
    )
    .await;

    assert!(outcome.ok, "{:?}", outcome.error);

    let summary = delivered(&outcome);

    assert_eq!(summary["bytes"], json!(4_096));
    assert_eq!(summary["media"], json!("attached"));
    assert_eq!(summary["mediaId"], json!("dl-1"));
    assert!(
        summary.get("data").is_none(),
        "the base64 body must not sit in the model's summary: that is the attachment's job"
    );

    let media = outcome.media.as_ref().expect("the attachment descriptor");

    assert_eq!(media[0]["id"], json!("dl-1"));
    assert_eq!(media[0]["kind"], json!("file"));
    assert_eq!(media[0]["size"], json!(4_096));
    assert!(
        media[0].get("data").is_none(),
        "the event carries a descriptor, not the payload"
    );

    // The summary alone stays far inside the budget the body would otherwise spend.
    let committed = model_payload_bytes(&outcome.content);

    assert!(
        committed <= MODEL_TOOL_RESULT_BUDGET_BYTES / 8,
        "the summary is a report, not the payload: {committed} bytes"
    );
    server.abort();
}

// ---------------------------------------------------------------------------
// The gate, in production clothing
// ---------------------------------------------------------------------------

/// Every notation of a private address is refused, and the PROOF is the absence of a request: the
/// gate decided before a socket was built. This is the cloud-metadata path (`169.254.169.254`, and
/// the `169-254-169-254.nip.io` name trick that is why gate 2 exists at all).
#[tokio::test]
async fn every_private_address_form_is_refused_before_a_socket_opens() {
    let (base, seen, server) = stub_origin(|_base| {
        vec![Reply::text(
            &[("content-type", "text/plain"), ("content-length", "2")],
            "hi",
        )]
    })
    .await;
    let port = base.rsplit(':').next().expect("a port").to_owned();
    let fixture = Fixture::new("download-gate");
    let runtime = registry(download_keys(&base), Some(&fixture.sessions_root()));

    for host in [
        "127.0.0.1",
        "localhost",
        "127.1",
        "2130706433",
        "0177.0.0.1",
        "0x7f.1",
        "[::1]",
        "[::ffff:127.0.0.1]",
        "100.64.0.1",
        "169.254.169.254",
        "10.0.0.9",
        "192.168.1.1",
        "172.16.0.1",
    ] {
        let outcome = call(
            &runtime,
            "download",
            json!({ "url": format!("http://{host}:{port}/secret") }),
            Some("session-1"),
        )
        .await;

        assert!(!outcome.ok, "{host} must be refused by the download gate");
        assert!(
            outcome
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("it is not a public absolute http(s) address"),
            "{host} refused with the wrong sentence: {:?}",
            outcome.error
        );
    }

    assert!(
        seen.lock().expect("the request log").is_empty(),
        "a refused URL must not open a socket"
    );
    assert!(
        !fixture.session_downloads("session-1").exists(),
        "a download that never started must not create the session folder"
    );
    server.abort();
}

/// A public host that REDIRECTS into loopback is refused at the HOP: the first request happened,
/// the second never did. Auto-following redirects (the platform default) is the hole this closed,
/// and only the request log can tell the two failures apart.
#[tokio::test]
async fn a_redirect_into_loopback_is_refused_at_the_hop() {
    // The `Location` names the stub's OWN address, which is unknown until the listener is bound -
    // which is why the reply queue is built by a callback that receives the base URL.
    let (base, seen, server) = stub_origin(|base| {
        vec![
            Reply::redirect(&format!("{base}/internal")),
            Reply::text(
                &[("content-type", "text/plain"), ("content-length", "2")],
                "hi",
            ),
        ]
    })
    .await;
    let fixture = Fixture::new("download-hop");

    let outcome = call(
        &registry(download_keys(&base), Some(&fixture.sessions_root())),
        "download",
        json!({ "url": public_url(&base, "/move") }),
        Some("session-1"),
    )
    .await;

    assert!(!outcome.ok, "{:?}", outcome.error);
    // The committed `error` is the failure wrapper AROUND the tool's sentence
    // (`Tool "download" failed: ...`); the sentence itself is pinned byte-for-byte in `web.rs`, so
    // what belongs here is that the boundary carries it, and carries it instead of a transfer.
    assert!(
        outcome
            .error
            .as_deref()
            .expect("a refusal sentence")
            .ends_with("download refused a redirect to a non-public address (HTTP 302)."),
        "{:?}",
        outcome.error
    );

    let log = seen.lock().expect("the request log");

    assert_eq!(log.len(), 1, "only the first hop may be requested: {log:?}");
    assert!(
        log[0].contains("/move"),
        "the first hop is the one that ran"
    );
    server.abort();
}

// ---------------------------------------------------------------------------
// Refusals that must cost nothing
// ---------------------------------------------------------------------------

/// Two destination failures the reference proves are PRE-TRANSFER, mirrored here at the boundary:
/// neither may pay for a download it cannot deliver.
#[tokio::test]
async fn an_unusable_destination_is_refused_before_the_transfer() {
    let body = "payload";
    let (base, seen, server) = stub_origin(|_base| {
        vec![
            Reply::text(
                &[
                    ("content-type", "text/plain"),
                    ("content-length", &body.len().to_string()),
                ],
                body,
            ),
            Reply::text(
                &[
                    ("content-type", "text/plain"),
                    ("content-length", &body.len().to_string()),
                ],
                body,
            ),
        ]
    })
    .await;
    let fixture = Fixture::new("download-precheck");
    let runtime = registry(download_keys(&base), Some(&fixture.sessions_root()));

    // No session: this harness has nowhere to save.
    let sessionless = call(
        &runtime,
        "download",
        json!({ "url": public_url(&base, "/orphan.bin") }),
        None,
    )
    .await;

    assert!(!sessionless.ok);
    assert!(
        sessionless
            .error
            .as_deref()
            .expect("a refusal")
            .contains("download needs a session:"),
        "{:?}",
        sessionless.error
    );

    // A `path` that leaves the folder is an ARGUMENT error, said loudly rather than degraded into
    // `saved: false`, which would let a model believe it chose a location and got the default.
    let escaping = call(
        &runtime,
        "download",
        json!({ "url": public_url(&base, "/escape.bin"), "path": "../outside.bin" }),
        Some("session-1"),
    )
    .await;

    assert!(!escaping.ok);
    assert!(
        escaping
            .error
            .as_deref()
            .expect("a refusal")
            .contains("relative to the session downloads folder"),
        "{:?}",
        escaping.error
    );

    assert!(
        seen.lock().expect("the request log").is_empty(),
        "neither refusal may transfer a byte"
    );
    server.abort();
}

/// A registry built WITHOUT a sessions root - the shape a bare library embedding gets - refuses
/// rather than writing next to the process's working directory. The reference dropped that default
/// for the same reason: a file the caller never named is a side effect nobody authorized.
#[tokio::test]
async fn a_registry_with_no_downloads_root_refuses_rather_than_guessing_a_folder() {
    let body = "payload";
    let (base, seen, server) = stub_origin(|_base| {
        vec![Reply::text(
            &[
                ("content-type", "text/plain"),
                ("content-length", &body.len().to_string()),
            ],
            body,
        )]
    })
    .await;

    let outcome = call(
        &registry(download_keys(&base), None),
        "download",
        json!({ "url": public_url(&base, "/homeless.bin") }),
        Some("session-1"),
    )
    .await;

    assert!(!outcome.ok);
    assert!(
        outcome
            .error
            .as_deref()
            .expect("a refusal")
            .contains("download needs a session:"),
        "{:?}",
        outcome.error
    );
    assert!(seen.lock().expect("the request log").is_empty());
    server.abort();
}
