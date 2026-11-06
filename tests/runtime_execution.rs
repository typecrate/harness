//! The runtime-SUPPORT gate: which execution modes this build may start on (T9).
//!
//! `config.rs` ports the TypeScript loader verbatim, so `HARNESS_THREAD_EXECUTION=worker` and
//! `HARNESS_IO_EXECUTION=worker` PARSE - they have to, because `GET /config` reports the configured
//! value back to an operator. What this build cannot do is provide a lane worker or an IO worker:
//! there is no worker backend in the Rust port at all. The TypeScript server never breaks that
//! promise (`workers/capability.ts`, `workers/io-client.ts:282`, `sessions.ts:1513-1516` abort a
//! worker deployment instead of running it inline), so the honest Rust equivalent is a refusal at
//! the same moment - never a silent fallback to an inline server nobody asked for.
//!
//! The refusal is pinned at three levels:
//!
//! 1. [`typecrate_harness::runtime::validate_supported_execution`] - the pure support gate, with the
//!    exact sentence an operator reads (and the exact env var to unset);
//! 2. [`typecrate_harness::runtime::runtime_for_with`] - the construction path the gateway dispatcher
//!    uses (`api/mod.rs:1225` -> `runtime::runtime_for`), so the first request answers 400 instead of
//!    serving an inline turn, and a refused build is NOT cached (a fixed environment works without a
//!    restart, the same rule `runtime.ts:296-308` states for a corrupt document);
//! 3. the default `inline` deployment - it still builds, still serves the mount, and still reports
//!    `thread_execution: "inline"` on `GET /config`.
//!
//! Nothing here writes the PROCESS environment (that is `unsafe` in edition 2024 and this crate
//! forbids unsafe), so every configuration in this file comes from an explicit environment MAP. The
//! `.env`-driven HTTP case lives in `tests/worker_execution_http.rs`, which owns its own test binary
//! because loading a `.env` file injects values into the process environment.
//!
//! Fixtures live in `Temp/typecrate-harness/runtime-execution/<pid>-<test>/` per the repo scratch
//! rule, and every test owns its own DATABASE ROOT so the shared runtime cache stays partitioned.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::Request;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use typecrate_harness::api;
use typecrate_harness::config::{self, ServerConfig};
use typecrate_harness::contract;
use typecrate_harness::error::ApiError;
use typecrate_harness::runtime;

/// The standalone executable's "could not serve" code, restated here so the refusal's exit code is
/// asserted against a named value (`src/bin/server.rs` is the source of the convention).
const EXIT_RUNTIME: i32 = 1;

/// How long a banner line may take before the spawned child counts as hung. The debug binary starts
/// in well under a second; this only catches a real stall.
const BANNER_TIMEOUT: Duration = Duration::from_secs(30);

/// The refusal for `HARNESS_THREAD_EXECUTION=worker`, stated once so the message under test is the
/// message a human reads: the unsupported value, the only mode this build has, the variable to fix,
/// and the promise that nothing degrades silently.
const THREAD_WORKER_REFUSAL: &str = concat!(
    "Invalid runtime settings: thread_execution \"worker\" is not supported in this Rust build; ",
    "only \"inline\" is available. Unset HARNESS_THREAD_EXECUTION or set it to \"inline\" - this ",
    "build never runs a \"worker\" deployment inline silently.",
);

/// The refusal for `HARNESS_IO_EXECUTION=worker`.
const IO_WORKER_REFUSAL: &str = concat!(
    "Invalid runtime settings: io_execution \"worker\" is not supported in this Rust build; only ",
    "\"inline\" is available. Unset HARNESS_IO_EXECUTION or set it to \"inline\" - this build ",
    "never runs a \"worker\" deployment inline silently.",
);

/// Both knobs at once: one pass, two sentences, joined by `; ` the way `load_server_config` joins
/// its own collected issues (`config::format_server_runtime_issues`).
const BOTH_WORKER_REFUSAL: &str = concat!(
    "Invalid runtime settings: thread_execution \"worker\" is not supported in this Rust build; ",
    "only \"inline\" is available. Unset HARNESS_THREAD_EXECUTION or set it to \"inline\" - this ",
    "build never runs a \"worker\" deployment inline silently.; io_execution \"worker\" is not ",
    "supported in this Rust build; only \"inline\" is available. Unset HARNESS_IO_EXECUTION or ",
    "set it to \"inline\" - this build never runs a \"worker\" deployment inline silently.",
);

/// The gate for one environment map, through the SAME loader the runtime uses: the point is that a
/// value the loader accepts is the value the gate refuses.
fn gate(env: &[(&str, &str)]) -> Result<(), ApiError> {
    runtime::validate_supported_execution(&configuration(env))
}

/// `config::load_server_config` over an explicit map (`config.rs` reads a map, never the process).
fn configuration(env: &[(&str, &str)]) -> ServerConfig {
    let values: BTreeMap<String, String> = env
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();

    config::load_server_config(&values).unwrap_or_else(|error| {
        panic!("`{env:?}` parses - acceptance is the parser's, refusal is the gate's: {error}")
    })
}

/// A database root that starts empty and is removed on `Drop`.
struct Root {
    path: PathBuf,
}

impl Root {
    fn new(test: &str) -> Self {
        let path = typecrate_harness::package_root::normalize_lexically(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../../Temp/typecrate-harness/runtime-execution/{}-{test}",
                std::process::id()
            )),
        );

        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path)
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", path.display()));

        Self { path }
    }

    /// `<root>/harness/state.json` - absent until the first durable write, which is how "the refusal
    /// happened BEFORE the disk was touched" is observed.
    fn state_file(&self) -> PathBuf {
        self.path.join("harness").join("state.json")
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        // The store keeps a journal fd open, so a Windows removal can still be pending; releasing
        // the cache entry first is what makes the next run of this test start from an empty root.
        runtime::evict_runtime(&self.path);

        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Reads one in-process dispatcher reply down to `(status, json body)`. `api::handle_request` IS the
/// gateway handler (`endpoint/harness/harness-route.rs` calls the same function), so no socket is
/// needed to observe what a mounted request gets.
async fn dispatch(root: &Path, method: &str, path: &str) -> (u16, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        // `Host` is what the origin gate reads; an absent Origin is a non-browser client, which the
        // contract allows (`origin.ts`, `service.ts:461`).
        .header("host", "127.0.0.1:5181")
        .body(Body::empty())
        .expect("a bare GET builds");

    let response = api::handle_request(root.to_path_buf(), request).await;
    let status = response.status().as_u16();
    let text = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable")
        .to_vec();

    (
        status,
        serde_json::from_slice(&text).unwrap_or_else(|error| {
            panic!(
                "a JSON body is expected, got `{}`: {error}",
                String::from_utf8_lossy(&text)
            )
        }),
    )
}

/// 1a. The shipped default - `inline` for both knobs - is exactly what this build provides, so the
/// gate is a no-op for every deployment that changes nothing.
#[test]
fn the_default_inline_deployment_is_supported() {
    assert_eq!(
        config::DEFAULT_THREAD_EXECUTION,
        runtime::SUPPORTED_EXECUTION_MODE
    );
    assert_eq!(
        config::DEFAULT_IO_EXECUTION,
        runtime::SUPPORTED_EXECUTION_MODE
    );

    assert_eq!(
        runtime::validate_supported_execution(&ServerConfig::default()),
        Ok(())
    );
    assert_eq!(gate(&[]), Ok(()));

    // The explicit spelling passes too: the gate is about the MODE, not about whether a variable is
    // set at all.
    assert_eq!(
        gate(&[
            ("HARNESS_THREAD_EXECUTION", "inline"),
            ("HARNESS_IO_EXECUTION", "inline"),
        ]),
        Ok(())
    );
}

/// 1b. `HARNESS_THREAD_EXECUTION=worker` is a 400 whose message names the value, the mode this build
/// has, and the variable to unset.
#[test]
fn a_worker_thread_lane_is_refused_with_the_support_sentence() {
    let refused = gate(&[("HARNESS_THREAD_EXECUTION", "worker")])
        .expect_err("a lane-worker deployment is refused");

    assert_eq!(refused, ApiError::bad_request(THREAD_WORKER_REFUSAL));
    assert_eq!(refused.status, 400);

    // The PARSE still succeeds, which is the part the brief keeps separate: `load_server_config` is
    // also what reports the configuration, so it must not be the place a mode is refused.
    let loaded = configuration(&[("HARNESS_THREAD_EXECUTION", "worker")]);

    assert_eq!(loaded.thread_execution, "worker");
    assert_eq!(loaded.io_execution, "inline");
}

/// 1c. The IO knob is refused on its own, with ITS variable named.
#[test]
fn a_worker_io_lane_is_refused_with_the_support_sentence() {
    let refused = gate(&[("HARNESS_IO_EXECUTION", "worker")])
        .expect_err("an IO-worker deployment is refused");

    assert_eq!(refused, ApiError::bad_request(IO_WORKER_REFUSAL));
    assert_eq!(refused.status, 400);

    let loaded = configuration(&[("HARNESS_IO_EXECUTION", "worker")]);

    assert_eq!(loaded.thread_execution, "inline");
    assert_eq!(loaded.io_execution, "worker");
}

/// 1d. Both knobs: one pass, one refusal carrying both sentences, so the operator fixes both in one
/// edit instead of one per restart.
#[test]
fn both_worker_knobs_are_refused_in_one_pass() {
    let refused = gate(&[
        ("HARNESS_THREAD_EXECUTION", "worker"),
        ("HARNESS_IO_EXECUTION", "worker"),
    ])
    .expect_err("a deployment that asks for both lanes is refused");

    assert_eq!(refused, ApiError::bad_request(BOTH_WORKER_REFUSAL));
}

/// 1e. The gate is a support statement, not a spelling check: any value that is not `inline` is
/// refused, and the message quotes the value the deployment actually stated. (`config.rs` still
/// accepts only `inline` | `worker`, so an unusable spelling is caught earlier by the loader.)
#[test]
fn anything_but_inline_is_refused_and_the_value_is_quoted() {
    // A value the PARSE would reject (`fork`) reaches the gate only through a constructed config,
    // which is exactly what an embedder with its own settings channel can do.
    let config = ServerConfig {
        thread_execution: "fork".to_owned(),
        ..ServerConfig::default()
    };

    let refused =
        runtime::validate_supported_execution(&config).expect_err("an unknown mode is refused");

    assert_eq!(refused.status, 400);
    assert_eq!(
        refused.message,
        concat!(
            "Invalid runtime settings: thread_execution \"fork\" is not supported in this Rust ",
            "build; only \"inline\" is available. Unset HARNESS_THREAD_EXECUTION or set it to ",
            "\"inline\" - this build never runs a \"fork\" deployment inline silently."
        )
    );
}

// ---------------------------------------------------------------------------
// 2. The construction path the gateway uses
// ---------------------------------------------------------------------------

/// 2a. The refusal happens before the first filesystem read, and NOTHING is cached: every later
/// request for that root fails clearly until the environment is fixed, which is the same rule
/// `runtime.ts:296-308` states for a corrupt document.
#[test]
fn the_runtime_refuses_a_worker_deployment_before_touching_the_disk() {
    let root = Root::new("runtime-worker");
    let worker = configuration(&[("HARNESS_THREAD_EXECUTION", "worker")]);

    let refused = runtime::runtime_for_with(&root.path, worker)
        .err()
        .expect("a worker runtime cannot be built in this build");

    assert_eq!(refused, ApiError::bad_request(THREAD_WORKER_REFUSAL));
    assert!(
        runtime::cached_runtime(&root.path).is_none(),
        "a refused runtime must not leave a half-built entry behind"
    );
    assert!(
        !root.state_file().exists(),
        "the refusal happens before the document is read or written"
    );
}

/// 2b. An IO-worker deployment refuses through the same construction path.
#[test]
fn the_runtime_refuses_an_io_worker_deployment() {
    let root = Root::new("runtime-io-worker");
    let worker = configuration(&[("HARNESS_IO_EXECUTION", "worker")]);

    let refused = runtime::runtime_for_with(&root.path, worker)
        .err()
        .expect("an IO worker is not provided");

    assert_eq!(refused, ApiError::bad_request(IO_WORKER_REFUSAL));
    assert!(runtime::cached_runtime(&root.path).is_none());
}

/// 2c. The default deployment still starts: `runtime_for` - the exact call the router and the route
/// bridge make on the first request - builds the graph, and the mount answers.
#[tokio::test]
async fn the_default_deployment_still_starts_and_serves_the_mount() {
    let root = Root::new("runtime-inline-default");

    let built = runtime::runtime_for(&root.path).expect("the default inline deployment starts");

    assert_eq!(
        built.effective_settings().base_url,
        config::DEFAULT_BASE_URL
    );
    assert_eq!(built.root, root.path);

    // The health route is the cheapest proof that the runtime behind the dispatcher is live.
    let (status, body) = dispatch(&root.path, "GET", "/v1/harness/health").await;

    assert_eq!(status, 200);
    assert_eq!(
        body,
        serde_json::json!({
            "ok": true,
            "sessions": 0,
            "epoch": built.epoch(),
        })
    );

    // And the reported configuration is still the deployment's own: the gate refuses to START on a
    // worker mode, it never rewrites what the deployment stated, and the runtime block of `GET
    // /config` is the operator's diagnosis channel (that is why the gate is NOT in the parser).
    let (status, config_body) = dispatch(&root.path, "GET", "/v1/harness/config").await;

    assert_eq!(status, 200);
    assert_eq!(
        config_body["runtime"],
        serde_json::json!({
            "thread_execution": runtime::SUPPORTED_EXECUTION_MODE,
            "worker_idle_ms": config::DEFAULT_WORKER_IDLE_MS,
            "max_lane_workers": config::DEFAULT_MAX_LANE_WORKERS,
            "worker_handshake_ms": config::DEFAULT_WORKER_HANDSHAKE_MS,
        })
    );
}

/// 2d. An embedder that injects its own configuration gets the same gate, and a root that already
/// has a runtime keeps it - the injected settings apply when the runtime is BUILT.
#[test]
fn an_injected_configuration_passes_the_same_gate() {
    let root = Root::new("runtime-injected");

    let worker = configuration(&[("HARNESS_THREAD_EXECUTION", "worker")]);
    let refused = runtime::runtime_for_with(&root.path, worker.clone());

    assert!(
        refused.is_err(),
        "the seam runs the gate, not only the builder"
    );

    // The same root with the default settings now builds (the refusal cached nothing).
    let built =
        runtime::runtime_for_with(&root.path, ServerConfig::default()).expect("inline starts");

    assert_eq!(built.root, root.path);

    // A third call - any configuration, including a worker one - returns the runtime that exists.
    let again = runtime::runtime_for_with(&root.path, worker).expect("the cached runtime answers");

    assert!(
        std::ptr::eq(&*again, &*built),
        "one root has exactly one runtime, whatever the injected settings are"
    );
}

// ---------------------------------------------------------------------------
// 3. The standalone executable, as a real process
// ---------------------------------------------------------------------------

/// Spawns the built `typecrate-harness-server` with an explicit environment (the CHILD's
/// environment block is set by `Command::env`, which is safe - unlike the test process's own),
/// captures everything, and returns the finished output.
async fn run_standalone(root: &Root, env: &[(&str, &str)]) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_typecrate-harness-server"));

    command
        // `--port 0` asks for any free port; a deployment that is refused never binds one anyway, and
        // the start test reads the port the banner reports.
        .args(["--port", "0", "--database-root"])
        .arg(&root.path)
        .env("HARNESS_THREAD_EXECUTION", "inline")
        .env("HARNESS_IO_EXECUTION", "inline")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    for (key, value) in env {
        command.env(*key, *value);
    }

    command
        .output()
        .await
        .expect("the standalone executable runs to completion")
}

/// 3a. `HARNESS_THREAD_EXECUTION=worker` aborts the standalone start the way TypeScript's does: the
/// message on stderr, exit 1, and NO banner - which is the proof that the process refused before
/// binding rather than after.
#[tokio::test]
async fn the_standalone_executable_refuses_a_worker_thread_lane_before_binding() {
    let root = Root::new("standalone-thread-worker");

    let output = run_standalone(&root, &[("HARNESS_THREAD_EXECUTION", "worker")]).await;

    assert_eq!(
        output.status.code(),
        Some(EXIT_RUNTIME),
        "a deployment this build cannot serve exits 1, not 2: the flags were valid"
    );
    assert_eq!(
        std::str::from_utf8(&output.stdout).expect("stdout is text"),
        "",
        "nothing is printed, so nothing was bound"
    );
    assert_eq!(
        std::str::from_utf8(&output.stderr)
            .expect("stderr is text")
            .trim_end(),
        format!("typecrate-harness-server: {THREAD_WORKER_REFUSAL}")
    );
    assert!(
        !root.state_file().exists(),
        "the refusal happens before the state document is touched"
    );
}

/// 3b. The IO knob aborts the same way, naming its own variable.
#[tokio::test]
async fn the_standalone_executable_refuses_a_worker_io_lane_before_binding() {
    let root = Root::new("standalone-io-worker");

    let output = run_standalone(&root, &[("HARNESS_IO_EXECUTION", "worker")]).await;

    assert_eq!(output.status.code(), Some(EXIT_RUNTIME));
    assert_eq!(
        std::str::from_utf8(&output.stderr)
            .expect("stderr is text")
            .trim_end(),
        format!("typecrate-harness-server: {IO_WORKER_REFUSAL}")
    );
}

/// 3c. The default deployment is NOT caught by its own gate: the executable binds, prints the banner
/// with the port the OS actually gave `--port 0`, and serves the mount over a real socket.
#[tokio::test]
async fn the_standalone_executable_starts_the_default_inline_deployment() {
    let root = Root::new("standalone-inline");

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_typecrate-harness-server"))
        .args(["--port", "0", "--database-root"])
        .arg(&root.path)
        .env("HARNESS_THREAD_EXECUTION", "inline")
        .env("HARNESS_IO_EXECUTION", "inline")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("the standalone executable spawns");

    let stdout = child.stdout.take().expect("stdout is piped");
    let mut lines = BufReader::new(stdout).lines();

    // Line 1: `<service> v<version> listening on 127.0.0.1:<port>`. The port is READ back, because
    // `--port 0` means "any free port".
    let listening = banner_line(&mut lines).await;
    let announced = format!(
        "{} v{} listening on 127.0.0.1:",
        contract::SERVICE_NAME,
        typecrate_harness::version()
    );

    assert_eq!(&listening[..announced.len()], announced.as_str());

    let port: u16 = listening[announced.len()..]
        .parse()
        .unwrap_or_else(|error| panic!("the banner prints a port, got `{listening}`: {error}"));

    assert_ne!(port, 0, "the OS reports the port it actually handed out");

    // The three remaining banner lines, exact.
    assert_eq!(
        banner_line(&mut lines).await,
        format!(
            "  api:            http://127.0.0.1:{port}{}",
            contract::HARNESS_API_ROOT
        )
    );
    assert_eq!(
        banner_line(&mut lines).await,
        format!(
            "  stream:         ws://127.0.0.1:{port}{}",
            contract::HARNESS_WS_STREAM_PATH
        )
    );
    assert_eq!(
        banner_line(&mut lines).await,
        format!("  database root:  {}", root.path.display())
    );

    // One real request, over the socket, through the SAME runtime path the gateway mount uses.
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::ZERO)
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(60))
        .build()
        .expect("a plain HTTP client builds");

    let response = client
        .get(format!(
            "http://127.0.0.1:{port}{}/health",
            contract::HARNESS_API_ROOT
        ))
        .send()
        .await
        .expect("the listener answers");

    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let body: Value = response.json().await.expect("health is JSON");
    let mut keys: Vec<&String> = body.as_object().expect("object").keys().collect();

    keys.sort();

    assert_eq!(keys, ["epoch", "ok", "sessions"]);
    assert_eq!(body["ok"], serde_json::json!(true));
    assert_eq!(body["sessions"], serde_json::json!(0));
    assert!(
        body["epoch"]
            .as_str()
            .is_some_and(|epoch| !epoch.is_empty()),
        "the epoch is minted by the child process, so only its presence is knowable here"
    );

    child
        .kill()
        .await
        .expect("the listener is stopped once the assertion is done");
}

/// One banner line, with a deadline: a child that never prints is a failure, not a hung suite.
async fn banner_line<R>(lines: &mut tokio::io::Lines<BufReader<R>>) -> String
where
    R: tokio::io::AsyncRead + Unpin,
{
    tokio::time::timeout(BANNER_TIMEOUT, lines.next_line())
        .await
        .expect("the banner line arrives")
        .expect("stdout is readable")
        .expect("the process printed the line")
}
