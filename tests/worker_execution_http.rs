//! The gateway-side half of the worker-mode refusal (T9).
//!
//! This file exists on its own because it is the one runtime-execution test that drives the
//! deployment through a `.env` FILE: `environment::load_harness_environment` reads it through
//! dotenvy, which fills the PROCESS environment as a side effect. Writing the process environment
//! directly is `unsafe` in edition 2024 (and this crate forbids unsafe), so the injection happens
//! inside the library and cannot be undone from here. A dedicated test binary is therefore the
//! honest scope of that leak: nothing else in this process reads `HARNESS_*`.
//!
//! What is pinned is the brief's functional requirement: `HARNESS_THREAD_EXECUTION=worker` is
//! accepted by the parser and stays reportable through the config document, but the Rust build has
//! no worker backend, so the FIRST mounted request answers 400 with the support sentence - the same
//! thing TypeScript's `service.ts:515-528` does when runtime creation fails - instead of silently
//! running every turn inline.
//!
//! The request goes through `api::handle_request`, which is literally what the discovered route
//! bridge calls (`endpoint/harness/harness-route.rs` hands the same `Request` to the same function),
//! so this is the gateway path, not a stand-in for it.

use std::fs;
use std::path::{Path, PathBuf};

use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::HeaderMap;
use serde_json::{Value, json};

use typecrate_harness::contract;
use typecrate_harness::{api, environment, events, runtime};

/// The exact refusal, identical to the one `tests/runtime_execution.rs` and the standalone
/// executable print: one message, wherever the deployment is refused.
const THREAD_WORKER_REFUSAL: &str = concat!(
    "Invalid runtime settings: thread_execution \"worker\" is not supported in this Rust build; ",
    "only \"inline\" is available. Unset HARNESS_THREAD_EXECUTION or set it to \"inline\" - this ",
    "build never runs a \"worker\" deployment inline silently.",
);

/// A `.env` fixture that states a worker lane, plus the database root it is paired with.
struct Fixture {
    /// The per-test scratch directory (`Temp/typecrate-harness/worker-execution-http/...`), which
    /// holds the `.env` file and the dump root.
    scratch: PathBuf,
    /// The database root handed to the dispatcher - a SIBLING of the `.env` so the state document is
    /// never written next to the fixture.
    root: PathBuf,
}

impl Fixture {
    /// Writes `<scratch>/.env` and points the loader at it, which is the supported test seam
    /// (`environment::set_harness_environment_path`).
    fn new(test: &str, contents: &str) -> Self {
        let scratch = typecrate_harness::package_root::normalize_lexically(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../../Temp/typecrate-harness/worker-execution-http/{}-{test}",
                std::process::id()
            )),
        );

        let _ = fs::remove_dir_all(&scratch);
        fs::create_dir_all(&scratch)
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", scratch.display()));

        let file = scratch.join(".env");

        fs::write(&file, contents)
            .unwrap_or_else(|error| panic!("could not write `{}`: {error}", file.display()));

        environment::set_harness_environment_path(Some(file));

        let root = scratch.join("dump");

        fs::create_dir_all(&root)
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", root.display()));

        Self { scratch, root }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Restore the package-root default and drop the cached load, so a re-run of this test reads a
        // fresh file instead of the previous run's values. The dotenvy injection into the process
        // environment stays for the life of THIS test binary, which has no other test.
        environment::set_harness_environment_path(None);
        environment::clear_harness_environment_cache();

        runtime::evict_runtime(&self.root);

        let _ = fs::remove_dir_all(&self.scratch);
    }
}

/// One in-process request through the dispatcher, reduced to the three facts the contract pins.
struct Reply {
    status: u16,
    headers: HeaderMap,
    body: Value,
}

impl Reply {
    async fn get(root: &Path, pathname: &str) -> Self {
        let request = Request::builder()
            .method("GET")
            .uri(pathname)
            // `Host` is what the origin gate reads; an absent `Origin` is a non-browser client, which
            // the contract allows (`origin.ts`, `service.ts:461`).
            .header("host", "127.0.0.1:5181")
            .body(Body::empty())
            .expect("a bare GET builds");

        let response = api::handle_request(root.to_path_buf(), request).await;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let text = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body is readable")
            .to_vec();

        Self {
            status,
            headers,
            body: serde_json::from_slice(&text).unwrap_or_else(|error| {
                panic!(
                    "a JSON body is expected, got `{}`: {error}",
                    String::from_utf8_lossy(&text)
                )
            }),
        }
    }
}

/// The whole point of the stage: a `.env` that asks for a lane worker gets a 400 on the FIRST
/// mounted request, on EVERY mounted route (the runtime is resolved before the route match, exactly
/// as TS's gateway adapter does it), with nothing cached and nothing written.
#[tokio::test]
async fn a_worker_deployment_answers_every_mounted_request_with_400() {
    let fixture = Fixture::new("env", "HARNESS_THREAD_EXECUTION=worker\n");

    // The boot document is the request a client tries first.
    let boot = Reply::get(&fixture.root, contract::HARNESS_API_ROOT).await;

    assert_eq!(boot.status, 400);
    assert_eq!(
        boot.body,
        json!({ "error": { "message": THREAD_WORKER_REFUSAL } })
    );

    // A refusal is still a contract response, so the epoch header is stamped (`api/mod.rs`'s
    // `write_error`): the process epoch, because no runtime was built to own one.
    assert_eq!(
        boot.headers
            .get(contract::HARNESS_EPOCH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(events::process_epoch()),
        "the 400 carries X-Harness-Epoch"
    );

    // The refusal is not route-specific: a session read, the config document and an unknown path are
    // refused the same way, because the RUNTIME is what this deployment cannot build.
    for (pathname, what) in [
        ("/v1/harness/health", "health"),
        ("/v1/harness/config", "config"),
        ("/v1/harness/sessions", "the session page"),
        ("/v1/harness/nope", "an unknown route"),
    ] {
        let reply = Reply::get(&fixture.root, pathname).await;

        assert_eq!(reply.status, 400, "{pathname} ({what})");
        assert_eq!(
            reply.body,
            json!({ "error": { "message": THREAD_WORKER_REFUSAL } }),
            "{pathname} ({what})"
        );
    }

    // Nothing was cached and nothing was written: every later request fails the same clear way, and a
    // fixed environment works without restarting the process (`runtime.ts:296-308`'s rule for a
    // refused creation).
    assert!(runtime::cached_runtime(&fixture.root).is_none());
    assert!(
        !fixture.root.join("harness").join("state.json").exists(),
        "the gate fires before the first filesystem read"
    );
}
