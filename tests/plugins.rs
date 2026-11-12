//! The built-in PLUGIN surface over real HTTP: `GET /v1/harness/plugins` and
//! `PATCH /v1/harness/plugins/{plugin_id}` - the Rust port of
//! `packages/underload/harness/src/server/plugins-api.test.ts`.
//!
//! WHAT THIS FILE PINS, IN THE REFERENCE SUITE'S ORDER (one item per line, the shape
//! `tests/http_host.rs` uses, because a doc list with a deeper-indented continuation is a clippy
//! error in this workspace)
//! --------------------------------------------------
//!
//! 1. LIST - both built-ins, in manifest (scan) order, with their declared settings, defaults, hooks, tags, versions and the values in force; paging and `q` behave like every other collection in this contract; the collection revision rides the page BODY only, because the list route hands `json()` two arguments (`api.ts:1988-1991`).
//! 2. WRITE - a patch validates against the plugin's own descriptor, answers the WHOLE row, bumps the counter on the header AND on the next page, and is durable BEFORE the answer; a failed write answers 500 and KEEPS the applied value, which is `sessions.ts:1848-1851`'s documented choice.
//! 3. REFUSALS - unknown plugin => 404 naming what this build ships; unknown key, wrong type, out-of-range value, empty patch and an unknown body field => 400 with the exact sentence; a verb the path does not declare => 405 naming the one it does. A refusal mutates nothing, bumps nothing and writes no document.
//! 4. NORMALIZATION - a numeric STRING lands as a NUMBER, an integral value keeps its INTEGER spelling, `step` is NOT enforced, and an explicit `null` restores the declared default and drops the plugin's now-empty override map.
//! 5. DURABLE - `state.json` carries the override and nothing else, a fresh runtime on the same root boots with it, the counter restarts at 0 while the value does not, and a deployment that never edited anything writes no `pluginSettings` field at all.
//! 6. RESTORE - an override naming a retired plugin, a retired key, or another plugin's key is dropped silently; a stored value outside the declared bounds is dropped rather than fatal; a document whose `pluginSettings` SHAPE is wrong names the slot.
//!
//! Every test owns its own DATABASE ROOT: the runtime cache is keyed by root, so the revision
//! counters a parallel test asserts are never shared state. Fixtures live under
//! `Temp/typecrate-harness/plugins/<pid>-<test>/`, the repo scratch root.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::http::{HeaderMap, HeaderName, StatusCode};
use serde_json::{Map, Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use typecrate_harness::config::ServerConfig;
use typecrate_harness::runtime;
use typecrate_harness::server;

/// The contract headers, lowercased the way `http::HeaderMap` stores them.
const EPOCH_HEADER: HeaderName = HeaderName::from_static("x-harness-epoch");
const REVISION_HEADER: HeaderName = HeaderName::from_static("x-harness-revision");

/// The canonical mount.
const MOUNT: &str = "/v1/harness";

/// The refusal `api-paging.ts:99` prints for every unusable `?limit=`.
const LIMIT_MESSAGE: &str = "Query parameter \"limit\" must be an integer between 1 and 100.";

/// The two built-ins this build ships, in scan order: the list the 404 sentence prints, and the order
/// `GET /plugins` answers in.
/// The shipped catalogue, in the TypeScript manifest's scan order (path ascending from
/// `src/plugins`): `async-ping.plugin.ts`, `context-compaction.plugin.ts`, then
/// `context-trimmer.plugin.ts`. The order is contract-visible - `GET /plugins` answers in
/// it, the pane renders it as-is, and the 404 sentence joins these names with `", "`.
const CATALOGUE: [&str; 3] = ["async-ping", "context-compaction", "context-trimmer"];

/// The EFFECTIVE values of a compaction plugin nobody has touched, except the limit: the
/// row projects every DECLARED key (`plugins.ts valuesOf`), and the compactor declares six.
/// The five that no test patches stay at their declared defaults, so writing them once here
/// keeps each assertion about `contextLimit` honest about the whole map.
fn compaction_values(limit: Value) -> Value {
    json!({
        "contextLimit": limit,
        "modelLimits": "{}",
        "enabled": true,
        "minTokens": 200,
        "safetyTurns": 6,
        "mode": "summarize",
    })
}

// ---------------------------------------------------------------------------
// Fixtures and the test listener
// ---------------------------------------------------------------------------

/// A database root that starts empty and is removed on `Drop`.
struct Root {
    path: PathBuf,
}

impl Root {
    fn new(test: &str) -> Self {
        let path = typecrate_harness::package_root::normalize_lexically(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../../Temp/typecrate-harness/plugins/{}-{test}",
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

    /// The durable document, read from DISK: what a durability assertion is about is that the BYTES
    /// landed, not that a second round trip agrees.
    fn state(&self) -> Value {
        let text = self.state_text();

        serde_json::from_str(&text).expect("state.json is JSON")
    }

    fn state_text(&self) -> String {
        fs::read_to_string(self.state_file())
            .unwrap_or_else(|error| panic!("`{}` must exist: {error}", self.state_file().display()))
    }

    /// Replaces the durable document with a hand-edited one - the way an operator or a future
    /// migration would. The parent directory is created because a fixture may run before the server
    /// ever wrote a document.
    fn write_state(&self, document: &Value) {
        if let Some(parent) = self.state_file().parent() {
            fs::create_dir_all(parent)
                .unwrap_or_else(|error| panic!("`{}`: {error}", parent.display()));
        }

        fs::write(
            self.state_file(),
            serde_json::to_string_pretty(document).expect("the fixture is JSON"),
        )
        .unwrap_or_else(|error| panic!("`{}`: {error}", self.state_file().display()));
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// One harness listener on an ephemeral loopback port, serving the LIBRARY router - the same function
/// the standalone executable binds, so a passing test is evidence about the real surface.
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
        let probe = client();

        // One health probe proves the socket is accepting before any assertion runs.
        for _ in 0..200 {
            if probe
                .get(format!("{base}{MOUNT}/health"))
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
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("the listener stops on the shutdown signal")
                .expect("the serve task does not panic");
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // A test that asserted and never reached `stop()` still releases the socket here.
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
    async fn send(request: reqwest::RequestBuilder) -> Self {
        let response = request
            .send()
            .await
            .unwrap_or_else(|error| panic!("the request completes: {error}"));
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
}

// ---------------------------------------------------------------------------
// The two routes, as one-line calls
// ---------------------------------------------------------------------------

/// `GET {MOUNT}/plugins`, with an optional query string.
async fn list(server: &TestServer, query: &str) -> Reply {
    let suffix = if query.is_empty() {
        String::new()
    } else {
        format!("?{query}")
    };

    Reply::send(client().get(format!("{}{MOUNT}/plugins{suffix}", server.base))).await
}

/// `PATCH {MOUNT}/plugins/{plugin}` with a WHOLE body document, so a missing or extra top-level
/// field is reachable.
async fn patch_body(server: &TestServer, plugin: &str, body: Value) -> Reply {
    Reply::send(
        client()
            .patch(format!("{}{MOUNT}/plugins/{plugin}", server.base))
            .json(&body),
    )
    .await
}

/// `PATCH {MOUNT}/plugins/{plugin}` with the `{ settings }` envelope.
async fn patch_settings(server: &TestServer, plugin: &str, settings: Value) -> Reply {
    patch_body(server, plugin, json!({ "settings": settings })).await
}

/// The names one page carries, in page order.
fn page_names(body: &Value) -> Vec<String> {
    body["items"]
        .as_array()
        .expect("an items array")
        .iter()
        .map(|row| row["name"].as_str().unwrap_or_default().to_owned())
        .collect()
}

/// The plugin overrides one durable document carries, with the "nothing was written" case spelled
/// out (`{}` never appears: the writer omits the member).
fn saved_overrides(root: &Root) -> Value {
    root.state()
        .get("pluginSettings")
        .cloned()
        .unwrap_or(Value::Null)
}

/// A `Value` object as the map the store's own accessors return, so a wire document can be compared
/// against a store projection without a second literal.
fn object(value: Value) -> Map<String, Value> {
    value.as_object().expect("the fixture is an object").clone()
}

// ---------------------------------------------------------------------------
// 1 - the catalogue read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_catalogue_answers_both_builtins_with_their_declared_metadata_and_values() {
    let root = Root::new("list");
    let server = TestServer::start(&root).await;

    let page = list(&server, "").await;

    assert_eq!(page.status, StatusCode::OK);

    // A list read is not one resource instance, so `json()` gets no revision argument
    // (`api.ts:1988-1991`, exactly like `GET /harnesses` at `:1503-1509`): the figure rides the page
    // BODY, and the header carries only the epoch.
    assert!(
        page.header(&REVISION_HEADER).is_none(),
        "the plugin list stamps no `X-Harness-Revision`"
    );
    assert!(
        page.header(&EPOCH_HEADER).is_some(),
        "every JSON response stamps the epoch"
    );
    assert_eq!(
        page.headers["content-type"].to_str().unwrap(),
        "application/json; charset=utf-8"
    );
    assert_eq!(page.headers["cache-control"].to_str().unwrap(), "no-store");

    // The five page members and nothing else (`PluginSummaryPage` is a closed object).
    let mut keys: Vec<&String> = page
        .body
        .as_object()
        .expect("a page object")
        .keys()
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["items", "limit", "next_cursor", "observed_at", "revision"]
    );

    // A two-entry catalogue fits one default page, so `limit` echoes the EFFECTIVE size and the
    // cursor ends the collection.
    assert_eq!(page.body["limit"], 3);
    assert_eq!(page.body["next_cursor"], Value::Null);
    assert!(page.body["observed_at"].is_u64());

    // An untouched runtime has a collection revision of 0 - "never changed" - and it is the plugin
    // SETTINGS store's own counter, not a hub counter.
    assert_eq!(page.body["revision"], 0);

    // Manifest order (path ascending from the TypeScript scan), pinned because the pane renders it
    // as-is.
    assert_eq!(page_names(&page.body), CATALOGUE);

    // The full row key set (the closed `PluginSummary` object). Both shipped plugins declare a
    // description, a version, tags and hooks, so all nine keys are present; one that declared none
    // would OMIT those keys rather than send `null`.
    let mut row_keys: Vec<&String> = page.body["items"][0]
        .as_object()
        .expect("a row object")
        .keys()
        .collect();
    row_keys.sort();
    assert_eq!(
        row_keys,
        vec![
            "builtin",
            "description",
            "enabled",
            "hooks",
            "name",
            "settings",
            "tags",
            "title",
            "values",
            "version"
        ]
    );

    // `async-ping`, byte for byte: identity, the declared descriptor (label, type, default, min,
    // step, description - and NO `max`, because the plugin declares none), and the effective value.
    assert_eq!(
        page.body["items"][0],
        json!({
            "name": "async-ping",
            "title": "Background Ping",
            "description": "Keeps a conversation waiting on background tool work moving by \
                            emitting a periodic ping with the calls that are still running, \
                            replacing the fixed scheduler heartbeat.",
            "version": "1.0.0",
            "tags": ["background", "async", "lifecycle"],
            "hooks": [
                "async.pending",
                "async.drained",
                "run.failed",
                "thread.stopped",
                "thread.deleted",
                "harness.dispose"
            ],
            "builtin": true,
            // R2's reserved switch key: `PluginSummary.enabled` is a non-optional `bool` in
            // `contract.rs:1923` and `api.ts:2373 pluginRowOf` emits it on EVERY row, so the row
            // literal must carry it - the key-set assertion above already demanded it.
            "enabled": true,
            "settings": [
                {
                    "key": "intervalMs",
                    "label": "Ping interval (ms)",
                    "type": "number",
                    "default": 600000,
                    "min": 1000,
                    "step": 1000,
                    "description": "How often a still-running background tool is reported while \
                                    work is pending. Defaults to the historical 10-minute wake."
                }
            ],
            "values": { "intervalMs": 600000 }
        }),
        "the row IS the plugin's own metadata beside the values in force"
    );

    // `context-compaction`, byte for byte. This literal was stale in every field the 2026-11
    // plugin-host push moved: the description text, FIVE hooks instead of one, the SIX declared
    // settings (contextLimit, modelLimits, enabled, minTokens, safetyTurns, mode) instead of
    // one, the 200000 default that replaced 100000, and the reserved `enabled` row key.
    // Cross-checked against the parity reference
    // `packages/underload/harness/src/plugins/context-compaction.plugin.ts:364-446` for every
    // structural field (name, title, version, tags, hooks, and each setting's key/label/type/
    // default/min/step) and against the shipped catalogue in `src/plugins.rs` for the prose.
    assert_eq!(
        page.body["items"][1],
        json!({
            "name": "context-compaction",
            "title": "Context Compaction",
            "description": "Watches each agent conversation against a context limit and, when the \
                    limit is reached, compacts the conversation at the turn boundary before the \
                    next model request is sent. In the default \"summarize\" mode the \
                    conversation does the work: the thread is parked, an instruction the plugin \
                    commits into the transcript is answered by this same agent with a summary of \
                    what is done, what it is working on and where it left off, and everything \
                    before that instruction (and the turns between it and the answer) stops being \
                    sent. The thread resumes with a nudge to continue, and the context is small \
                    again: the instruction plus the agent's own summary. The older \"exclude\" \
                    mode prunes only the tool units a later success already answered. Neither \
                    mode deletes anything: a withheld unit keeps its place in history and can be \
                    switched back on. The limit is the firing point: compaction starts as soon as \
                    the estimate reaches the effective limit, which is the per-model override for \
                    the model the conversation is running, capped by that model's declared \
                    context window when one is declared.",
            "version": "1.0.0",
            "tags": [
                "context",
                "compaction",
                "lifecycle"
            ],
            "hooks": [
                "turn.before",
                "turn.after",
                "thread.idle",
                "run.finished",
                "run.failed"
            ],
            "builtin": true,
            "enabled": true,
            "settings": [
                {
                    "key": "contextLimit",
                    "label": "Context limit (tokens)",
                    "type": "number",
                    "default": 200000,
                    "min": 1000,
                    "step": 1000,
                    "description": "The firing point: estimated tokens a conversation may reach \
                            before this plugin compacts it, measured with the ~4 characters/token \
                            estimate the UI context bar uses, anchored on the reported prompt \
                            size of the last request that reported usage — so after the first \
                            report it can differ from the bar by whatever the tool schemas and \
                            message framing actually cost. There is no separate headroom: the \
                            estimate at or above this number fires the pass on that turn. It is \
                            this value unless modelLimits overrides it for the model this \
                            conversation runs, or caps it at that model's declared context \
                            window. Set to 0 to turn compaction off for every model."
                },
                {
                    "key": "modelLimits",
                    "label": "Per-model limits (JSON)",
                    "type": "string",
                    "default": "{}",
                    "description": "A JSON object keyed by model id, each value `{ \"limit\": \
                            number, \"window\": number }` — both optional. `limit` replaces \
                            contextLimit for that model, for an operator who is willing to pay \
                            more (or less) for one model than for the rest. `window` states the \
                            model's HARD context window: it caps whatever limit is in force, so a \
                            512000 ceiling on a 262144-token model still compacts around the 256k \
                            behaviour rather than waiting for a size the provider never accepts. \
                            Example: `{\"gpt-4o\": {\"limit\": 512000, \"window\": 131072}, \
                            \"llama-8b\": {\"window\": 8192}}`. The model id is read live from \
                            the agent, so switching models changes the ceiling on the next turn. \
                            Anything malformed — broken JSON, a value that is not an object, a \
                            non-positive or non-numeric limit — is ignored and the global limit \
                            applies, never an error."
                },
                {
                    "key": "enabled",
                    "label": "Compact conversations",
                    "type": "boolean",
                    "default": true,
                    "description": "Runs compaction on or off, in either mode. Off changes nothing \
                            else: the limit is still read, the context bar still measures, and no \
                            other plugin is affected. It is the switch an operator expects when \
                            they want the behaviour gone without removing the built-in."
                },
                {
                    "key": "minTokens",
                    "label": "Minimum saving per pass (tokens)",
                    "type": "number",
                    "default": 200,
                    "min": 0,
                    "step": 50,
                    "description": "A pass must withhold at least this many estimated tokens, or \
                            it does nothing at all — no flag write, no persistence notification, \
                            no save. A prune that saves 40 tokens is a provider prompt-cache \
                            invalidation for nothing, so this floor is what makes running on \
                            every turn cheap. Measured as a difference of the same estimate, so \
                            any provider anchor cancels out of it and only the withheld \
                            characters count. In \"summarize\" mode the same number is read the \
                            other way round, as a trivial floor on the LIVE estimate: a \
                            conversation smaller than this is never worth a compaction round \
                            trip, so the pass refuses rather than paying for a summary of a \
                            handful of turns. Set it to 0 to remove the floor, which a small \
                            per-model limit needs: a floor above that limit would never be \
                            crossed."
                },
                {
                    "key": "safetyTurns",
                    "label": "Recent units to protect",
                    "type": "number",
                    "default": 6,
                    "min": 0,
                    "step": 1,
                    "description": "How many units at the END of the conversation no pass may \
                            touch. A unit is one assistant tool-call block plus its results, or \
                            one standalone message. The horizon counts UNITS from the end, never \
                            the per-run turn counter: a failure the model has not moved past yet \
                            is the highest-signal thing in the window, and pruning it is how an \
                            agent repeats the call it just watched fail. 0 removes the horizon. \
                            It belongs to \"exclude\" mode; \"summarize\" mode needs no horizon \
                            because the recent tail is exactly what the agent is summarizing."
                },
                {
                    "key": "mode",
                    "label": "Compaction mode",
                    "type": "string",
                    "default": "summarize",
                    "description": "Which compaction runs when the limit is reached. \"summarize\" \
                            — the default since the 2026-11-10 context-compaction ruling — parks \
                            the conversation at the turn boundary and lets its NEXT model turn \
                            write the summary: the agent condenses what is done, what it is \
                            working on and where it left off, everything before that instruction \
                            stops being sent, and the conversation resumes with a nudge. \
                            \"exclude\" is the earlier behaviour: a superseded tool unit keeps \
                            its place in canonical history and simply stops being sent, which is \
                            the reversible option the evidence favours and the cheaper one — no \
                            extra model turn. \"annotate\" — replace the result with a one-line \
                            handle back to the full text — is the planned SECOND WRITER over the \
                            same candidate list the policy already computes before writing \
                            anything, and any value other than these two (including that one) \
                            behaves as \"exclude\", the flow that cannot spend a model turn by \
                            accident. The value actually used is reported in the pass diagnostic."
                }
            ],
            "values": {
                "contextLimit": 200000,
                "modelLimits": "{}",
                "enabled": true,
                "minTokens": 200,
                "safetyTurns": 6,
                "mode": "summarize"
            }
        }),
        "the row IS the plugin's own metadata beside the values in force"
    );

    server.stop().await;
}

#[tokio::test]
async fn q_filters_the_catalogue_and_limit_pages_it_like_every_other_collection() {
    let root = Root::new("list-query");
    let server = TestServer::start(&root).await;

    // The haystack is `"{name} {title} {description} {tags}"`, case-insensitively.
    assert_eq!(
        page_names(&list(&server, "q=compact").await.body),
        vec!["context-compaction"]
    );

    // A capitalised query still hits a lower-cased tag: the case-insensitivity proof.
    assert_eq!(
        page_names(&list(&server, "q=BACKGROUND").await.body),
        vec!["async-ping"]
    );

    // A description substring selects exactly one row, and `%20` reaches the space it encodes.
    assert_eq!(
        page_names(&list(&server, "q=scheduler%20heartbeat").await.body),
        vec!["async-ping"]
    );

    // Hooks are NOT part of the haystack: `turn.before` is declared by the compaction plugin, and a
    // filter that matched it would answer a row the TypeScript server withholds.
    let hooks = list(&server, "q=turn.before").await;

    assert_eq!(hooks.status, StatusCode::OK);
    assert_eq!(hooks.body["items"], json!([]));

    let none = list(&server, "q=nonexistent").await;

    assert_eq!(none.body["items"], json!([]));
    assert_eq!(none.body["limit"], 0, "`limit` is the EFFECTIVE page size");
    assert_eq!(none.body["next_cursor"], Value::Null);

    // `?q=` is absent, not an empty substring (`paramOf`).
    assert_eq!(
        page_names(&list(&server, "q=").await.body),
        CATALOGUE,
        "a blank filter never filters on the empty substring"
    );

    // A full page always advertises a next page, and the cursor addresses the rest. The catalogue
    // ships THREE built-ins (`CATALOGUE`), so `limit=1` walks three pages; only the page that
    // REACHES the total ends the collection, because this route hands `slice_page` a known total
    // (`api/plugins.rs:69-70`) exactly like `api.ts:2386`'s
    // `slicePage(filtered, cursor, limit, filtered.length)`.
    let first = list(&server, "limit=1").await;

    assert_eq!(first.body["items"].as_array().expect("items").len(), 1);
    assert_eq!(first.body["limit"], 1);

    let cursor = first.body["next_cursor"]
        .as_str()
        .expect("a full page carries a cursor")
        .to_owned();

    let second = list(&server, &format!("limit=1&cursor={cursor}")).await;

    assert_eq!(
        page_names(&second.body),
        vec!["context-compaction"],
        "the second page holds the middle row of the three-row catalogue"
    );

    let cursor = second.body["next_cursor"]
        .as_str()
        .expect("an exactly-full page that has NOT reached the known total still carries a cursor")
        .to_owned();

    // The token is OPAQUE, so the fixture states what it must be through the crate's OWN encoder:
    // two of three rows served, so the cursor addresses ordinal 2 of THIS epoch.
    let epoch = second
        .header(&EPOCH_HEADER)
        .expect("the page stamps the epoch its cursor was issued for");

    assert_eq!(
        cursor,
        typecrate_harness::paging::encode_cursor(&epoch, 2),
        "page two of a three-row catalogue advertises ordinal 2, not the end"
    );

    let third = list(&server, &format!("limit=1&cursor={cursor}")).await;

    assert_eq!(
        page_names(&third.body),
        vec!["context-trimmer"],
        "the cursor of page two addresses the last row of the catalogue"
    );
    assert_eq!(
        third.body["next_cursor"],
        Value::Null,
        "the page that reaches the known total ENDS the collection instead of advertising an \
         empty fourth page"
    );

    // A cursor PAST the end answers an empty page rather than an error, like every other route. The
    // token is OPAQUE, so the fixture issues it through the crate's own encoder rather than spelling
    // a base64url string by hand (`epoch` is the one already read above - the same page family).
    let beyond = typecrate_harness::paging::encode_cursor(&epoch, 9);
    let past = list(&server, &format!("limit=1&cursor={beyond}")).await;

    assert_eq!(past.status, StatusCode::OK);
    assert_eq!(past.body["items"], json!([]));
    assert_eq!(past.body["next_cursor"], Value::Null);

    // A limit outside 1..=100 is a 400 naming the parameter.
    for query in ["limit=0", "limit=101", "limit=abc"] {
        let refused = list(&server, query).await;

        assert_eq!(
            refused.status,
            StatusCode::BAD_REQUEST,
            "`{query}` is refused"
        );
        assert_eq!(refused.message(), LIMIT_MESSAGE);
    }

    // A foreign cursor is the 400 the paging helper states, never a silent restart on page one.
    let foreign = list(&server, "cursor=bm90LW91cnM").await;

    assert_eq!(foreign.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        foreign.message(),
        "Query parameter \"cursor\" is stale or foreign; restart from the first page."
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 2 - the verbs each path declares
// ---------------------------------------------------------------------------

#[tokio::test]
async fn each_plugin_path_answers_the_contract_405_naming_the_verb_it_takes() {
    let root = Root::new("verbs");
    let server = TestServer::start(&root).await;

    // The collection is GET-only.
    let deleted = Reply::send(client().delete(server.url(&format!("{MOUNT}/plugins")))).await;

    assert_eq!(deleted.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(deleted.message(), "Use GET for /v1/harness/plugins.");

    // The item path declares PATCH only: enable/disable is not an HTTP affordance (loading is
    // auto-scanned), and a detail GET is not either - the row arrives with the list.
    let read =
        Reply::send(client().get(server.url(&format!("{MOUNT}/plugins/context-compaction")))).await;

    assert_eq!(read.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        read.message(),
        "Use PATCH for /v1/harness/plugins/context-compaction."
    );

    let created =
        Reply::send(client().post(server.url(&format!("{MOUNT}/plugins/async-ping")))).await;

    assert_eq!(created.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        created.message(),
        "Use PATCH for /v1/harness/plugins/async-ping."
    );

    // The sentence quotes the CALLER's path with the path parameter PERCENT-DECODED, because that is
    // what `matchRoute` captured (`api.ts:4012` re-joins the matched segments).
    let encoded = Reply::send(client().put(server.url(&format!("{MOUNT}/plugins/a%20b")))).await;

    assert_eq!(encoded.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(encoded.message(), "Use PATCH for /v1/harness/plugins/a b.");

    // A 405 never PARSES the body: a present-but-unparsable one still answers the verb sentence
    // rather than a JSON error.
    let with_body = Reply::send(
        client()
            .delete(server.url(&format!("{MOUNT}/plugins")))
            .header("content-type", "application/json")
            .body("[not json"),
    )
    .await;

    assert_eq!(with_body.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(with_body.message(), "Use GET for /v1/harness/plugins.");

    // A path under the mount that the table does not name at all stays a 404, not a 405.
    let unknown = Reply::send(client().get(server.url(&format!("{MOUNT}/plugins/x/y")))).await;

    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    assert_eq!(
        unknown.message(),
        "Unknown API route: GET /v1/harness/plugins/x/y"
    );

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 3 - the write
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_patched_context_limit_answers_the_row_bumps_the_counter_and_is_durable() {
    let root = Root::new("write");
    let server = TestServer::start(&root).await;

    let patched = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 200000 }),
    )
    .await;

    assert_eq!(patched.status, StatusCode::OK);

    // A patch returns the WHOLE row, not a receipt: the values it reports ARE the result of the
    // write, and an editor needs the reset targets beside them.
    assert_eq!(
        patched
            .body
            .as_object()
            .expect("an envelope object")
            .keys()
            .collect::<Vec<_>>(),
        vec!["plugin"]
    );
    assert_eq!(patched.body["plugin"]["name"], "context-compaction");
    assert_eq!(
        patched.body["plugin"]["values"],
        compaction_values(json!(200000))
    );

    // The DECLARED descriptor is unchanged by a write: the row still carries the reset target.
    assert_eq!(patched.body["plugin"]["settings"][0]["default"], 200000);
    assert_eq!(
        patched.body["plugin"]["settings"][0]["min"], 1000,
        "and the bounds the plugin published"
    );

    // One accepted write, one revision - on the header, and in the next page's body.
    assert_eq!(
        patched.header(&REVISION_HEADER).as_deref(),
        Some("1"),
        "`patchPlugin` passes the revision argument (`api.ts:2020-2022`)"
    );

    let reload = list(&server, "").await;

    assert_eq!(reload.body["revision"], 1);
    assert_eq!(
        reload.body["items"][1]["values"],
        compaction_values(json!(200000))
    );

    // The other plugin is untouched: settings are scoped by plugin id.
    assert_eq!(
        reload.body["items"][0]["values"],
        json!({ "intervalMs": 600000 })
    );

    // DURABLE BEFORE THE ANSWER, and only the override lands: the declared default (100000) is not
    // written, and no other plugin appears.
    assert_eq!(
        saved_overrides(&root),
        json!({ "context-compaction": { "contextLimit": 200000 } })
    );

    // A second accepted patch moves the counter again, and the two overrides coexist in CATALOGUE
    // order, not submission order.
    let second = patch_settings(&server, "async-ping", json!({ "intervalMs": 5000 })).await;

    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(second.header(&REVISION_HEADER).as_deref(), Some("2"));
    assert_eq!(
        second.body["plugin"]["values"],
        json!({ "intervalMs": 5000 })
    );

    assert_eq!(
        saved_overrides(&root),
        json!({
            "async-ping": { "intervalMs": 5000 },
            "context-compaction": { "contextLimit": 200000 }
        })
    );

    let after = list(&server, "").await;

    assert_eq!(after.body["revision"], 2);
    assert_eq!(
        after.body["items"][0]["values"],
        json!({ "intervalMs": 5000 })
    );

    // A read NEVER advances the counter: ten reads after two writes still report 2.
    for _ in 0..10 {
        list(&server, "").await;
    }

    assert_eq!(list(&server, "").await.body["revision"], 2);

    server.stop().await;
}

#[tokio::test]
async fn submitted_values_are_normalized_to_the_declared_type() {
    let root = Root::new("normalize");
    let server = TestServer::start(&root).await;

    // A numeric STRING is what a query parameter or an HTML form legitimately carries; `values`
    // answers a NUMBER, so a client round-trips types.
    let text = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": "150000" }),
    )
    .await;

    assert_eq!(text.status, StatusCode::OK);
    assert_eq!(
        text.body["plugin"]["values"],
        compaction_values(json!(150000)),
        "the stored value is exactly the declared type"
    );
    assert!(text.body["plugin"]["values"]["contextLimit"].is_i64());
    assert_eq!(
        saved_overrides(&root),
        json!({ "context-compaction": { "contextLimit": 150000 } }),
        "`state.json` holds the NUMBER, not the text"
    );

    // An integral value keeps INTEGER spelling, never `150000.0`.
    assert!(
        !root.state_text().contains("150000.0"),
        "a whole number prints as a whole number"
    );

    // A decimal stays a decimal, and `step` is deliberately NOT enforced: refusing 12345 because
    // `step: 1000` would make the API stricter than the runtime that lives with the value.
    let unstepped = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 12345 }),
    )
    .await;

    assert_eq!(
        unstepped.status,
        StatusCode::OK,
        "a value between the steps is accepted"
    );
    assert_eq!(
        unstepped.body["plugin"]["values"],
        compaction_values(json!(12345))
    );

    let decimal = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 1500.5 }),
    )
    .await;

    assert_eq!(decimal.status, StatusCode::OK);
    assert_eq!(
        decimal.body["plugin"]["values"],
        compaction_values(json!(1500.5))
    );

    // `min` is INCLUSIVE: the boundary itself is accepted.
    let boundary = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 1000 }),
    )
    .await;

    assert_eq!(boundary.status, StatusCode::OK);
    assert_eq!(
        boundary.body["plugin"]["values"],
        compaction_values(json!(1000))
    );

    // A padded numeric string is the same value: the runtime's own `asNumber` accepts it, so the
    // write edge normalizes it rather than refusing it.
    let padded = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": "  2500  " }),
    )
    .await;

    assert_eq!(padded.status, StatusCode::OK);
    assert_eq!(
        padded.body["plugin"]["values"],
        compaction_values(json!(2500))
    );

    server.stop().await;
}

#[tokio::test]
async fn an_explicit_null_restores_the_declared_default_and_drops_the_override() {
    let root = Root::new("reset");
    let server = TestServer::start(&root).await;

    patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 200000 }),
    )
    .await;

    let reset = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": Value::Null }),
    )
    .await;

    assert_eq!(reset.status, StatusCode::OK);
    assert_eq!(
        reset.body["plugin"]["values"],
        compaction_values(json!(200000)),
        "the declared default applies again"
    );
    assert_eq!(reset.body["plugin"]["settings"][0]["default"], 200000);

    // A reset is a MUTATION, so the counter moved again (1 -> 2).
    assert_eq!(reset.header(&REVISION_HEADER).as_deref(), Some("2"));

    // The override map is GONE, not empty, so the document carries no `pluginSettings` member at all.
    assert_eq!(
        saved_overrides(&root),
        Value::Null,
        "an empty snapshot is omitted, so the document keeps its pre-plugin shape"
    );

    // The list agrees, and reports the reset as a second write.
    let page = list(&server, "").await;

    assert_eq!(page.body["revision"], 2);
    assert_eq!(
        page.body["items"][1]["values"],
        compaction_values(json!(200000))
    );

    // Resetting a key that was never set is still an accepted write - the operator asked for the
    // default and got it - and it stores nothing.
    let again = patch_settings(&server, "async-ping", json!({ "intervalMs": Value::Null })).await;

    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.header(&REVISION_HEADER).as_deref(), Some("3"));
    assert_eq!(
        again.body["plugin"]["values"],
        json!({ "intervalMs": 600000 })
    );
    assert_eq!(saved_overrides(&root), Value::Null);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 4 - every refusal
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unknown_plugin_is_a_404_naming_the_catalogue_this_build_ships() {
    let root = Root::new("refuse-unknown-plugin");
    let server = TestServer::start(&root).await;

    let refused = patch_settings(&server, "nope", json!({ "contextLimit": 5000 })).await;

    assert_eq!(refused.status, StatusCode::NOT_FOUND);
    assert_eq!(
        refused.message(),
        "Unknown plugin \"nope\". This build ships: async-ping, context-compaction, context-trimmer."
    );
    assert!(
        refused.header(&REVISION_HEADER).is_none(),
        "a refusal stamps no revision"
    );
    assert_eq!(
        list(&server, "").await.body["revision"],
        0,
        "a refusal moves no counter"
    );
    assert!(
        !root.state_file().exists(),
        "and a refusal writes no document"
    );

    // The id is taken from the path parameter verbatim: names are case-sensitive, so a near miss gets
    // the same sentence with its own spelling.
    let near_miss =
        patch_settings(&server, "Context-Compaction", json!({ "contextLimit": 5 })).await;

    assert_eq!(near_miss.status, StatusCode::NOT_FOUND);
    assert_eq!(
        near_miss.message(),
        "Unknown plugin \"Context-Compaction\". This build ships: async-ping, context-compaction, context-trimmer."
    );

    // A percent-encoded id arrives decoded, which is what the front-end's
    // `encodeURIComponent(pluginId)` depends on.
    let encoded = patch_body(
        &server,
        "a%20b",
        json!({ "settings": { "contextLimit": 5 } }),
    )
    .await;

    assert_eq!(encoded.status, StatusCode::NOT_FOUND);
    assert_eq!(
        encoded.message(),
        "Unknown plugin \"a b\". This build ships: async-ping, context-compaction, context-trimmer."
    );

    server.stop().await;
}

#[tokio::test]
async fn every_malformed_patch_says_exactly_what_was_wrong_and_changes_nothing() {
    let root = Root::new("refuse-patch");
    let server = TestServer::start(&root).await;

    // 1. An unknown KEY, naming the declared set of THAT plugin.
    let unknown_key =
        patch_settings(&server, "context-compaction", json!({ "context_limit": 5 })).await;

    assert_eq!(unknown_key.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unknown_key.message(),
        "Unknown setting \"context_limit\" for plugin \"context-compaction\". Allowed settings: contextLimit, modelLimits, enabled, \
         minTokens, safetyTurns, mode."
    );

    // `intervalMs` is real, but not for this plugin: settings are scoped by plugin id.
    let wrong_scope =
        patch_settings(&server, "context-compaction", json!({ "intervalMs": 1000 })).await;

    assert_eq!(wrong_scope.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        wrong_scope.message(),
        "Unknown setting \"intervalMs\" for plugin \"context-compaction\". Allowed settings: contextLimit, modelLimits, enabled, \
         minTokens, safetyTurns, mode."
    );

    // 2. A value outside the declared bounds, quoting the bound and what arrived.
    let too_small = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 999 }),
    )
    .await;

    assert_eq!(too_small.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        too_small.message(),
        "\"contextLimit\" of plugin \"context-compaction\" must be at least 1000; 999 was \
         submitted."
    );

    // A numeric string out of range reports the COERCED number, like TS's template literal.
    let text_too_small = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": "999" }),
    )
    .await;

    assert_eq!(text_too_small.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        text_too_small.message(),
        "\"contextLimit\" of plugin \"context-compaction\" must be at least 1000; 999 was \
         submitted."
    );

    // 3. A value that is not a number at all: a non-numeric string, a boolean (NO coercion), an
    //    array, an object, and a blank string (refused rather than `Number("") === 0`).
    for submitted in [
        json!("plenty"),
        json!(true),
        json!([]),
        json!({}),
        json!("   "),
    ] {
        let refused = patch_settings(
            &server,
            "context-compaction",
            json!({ "contextLimit": submitted }),
        )
        .await;

        assert_eq!(
            refused.status,
            StatusCode::BAD_REQUEST,
            "`{submitted}` is not a number"
        );
        assert_eq!(
            refused.message(),
            "\"contextLimit\" of plugin \"context-compaction\" must be a finite number (it is \
             declared \"number\")."
        );
    }

    // 4a. SEVERAL bad fields: the refusal names the first one in the CLIENT's key order, because
    // `serde_json`'s `preserve_order` keeps the submitted order and TypeScript reports
    // `Object.keys(patch)`'s first offender (`plugins.ts update`). An alphabetical map would name the
    // BOUNDS error here instead, which is a different sentence.
    let first_offender = patch_settings(
        &server,
        "context-compaction",
        json!({ "zzz": 1, "contextLimit": 999 }),
    )
    .await;

    assert_eq!(first_offender.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        first_offender.message(),
        "Unknown setting \"zzz\" for plugin \"context-compaction\". Allowed settings: contextLimit, modelLimits, enabled, minTokens, safetyTurns, mode.",
        "the unknown key that arrived FIRST is the one named"
    );

    // 4b. One bad key inside an otherwise valid patch refuses the WHOLE patch. The bad key is an
    // INVENTED one: `enabled` can no longer play that role, because the reserved switch key is part
    // of the EFFECTIVE allowlist for EVERY plugin (`plugins.rs:1135-1138`, TS `plugins.ts:560`
    // `effectiveSettingsOf`) and `context-compaction` additionally DECLARES it
    // (`context-compaction.plugin.ts:419`) - `{settings:{contextLimit:5000, enabled:true}}` is
    // therefore a VALID patch and answers 200, which is the sentence the message below already
    // states by listing `enabled` among the allowed settings.
    let mixed = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 5000, "maxTokens": 1 }),
    )
    .await;

    assert_eq!(mixed.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        mixed.message(),
        "Unknown setting \"maxTokens\" for plugin \"context-compaction\". Allowed settings: contextLimit, modelLimits, enabled, \
         minTokens, safetyTurns, mode."
    );
    assert_eq!(
        list(&server, "").await.body["items"][1]["values"],
        compaction_values(json!(200000)),
        "the VALID half of a rejected patch applied nothing"
    );

    // 5. An empty patch.
    let empty = patch_settings(&server, "context-compaction", json!({})).await;

    assert_eq!(empty.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty.message(),
        "A plugin settings patch must change at least one setting."
    );

    // 6. A missing `settings` block, which the ROUTE checks before the store is asked about the id -
    //    so even an unknown plugin gets this 400.
    let missing = patch_body(&server, "context-compaction", json!({})).await;

    assert_eq!(missing.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        missing.message(),
        "A plugin settings patch requires a \"settings\" object."
    );

    let missing_unknown = patch_body(&server, "nope", json!({})).await;

    assert_eq!(missing_unknown.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        missing_unknown.message(),
        "A plugin settings patch requires a \"settings\" object.",
        "the route's body check runs before the store's 404 (`api.ts:2003-2007`)"
    );

    // 7. A body field the contract does not name is refused, never ignored.
    let extra_field = patch_body(
        &server,
        "context-compaction",
        json!({ "settings": { "contextLimit": 5000 }, "enabled": false }),
    )
    .await;

    assert_eq!(extra_field.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        extra_field.message(),
        "Unknown field \"enabled\". Allowed fields: settings."
    );

    // 8. A non-object body is the contract's body refusal.
    let array_body = Reply::send(
        client()
            .patch(server.url(&format!("{MOUNT}/plugins/context-compaction")))
            .json(&json!(["settings"])),
    )
    .await;

    assert_eq!(array_body.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        array_body.message(),
        "The request body must be a JSON object."
    );

    // 9. Every refusal above left the store exactly as it was: no value, no counter, no document.
    let untouched = list(&server, "").await;

    assert_eq!(untouched.body["revision"], 0);
    assert_eq!(
        untouched.body["items"][0]["values"],
        json!({ "intervalMs": 600000 })
    );
    assert_eq!(
        untouched.body["items"][1]["values"],
        compaction_values(json!(200000))
    );
    assert!(
        !root.state_file().exists(),
        "a rejected patch never wrote the document"
    );

    server.stop().await;
}

#[tokio::test]
async fn a_non_object_settings_block_produces_the_sentence_its_spread_produces() {
    let root = Root::new("refuse-settings-shape");
    let server = TestServer::start(&root).await;

    // TypeScript hands the store `{ ...body.settings }`, so each non-object shape produces its OWN
    // refusal instead of one collapsed message: `null`, a number and a boolean all spread to an
    // EMPTY patch, while an array or a string spreads to index-keyed entries.
    for submitted in [Value::Null, json!(5), json!(false)] {
        let refused = patch_body(
            &server,
            "context-compaction",
            json!({ "settings": submitted }),
        )
        .await;

        assert_eq!(
            refused.status,
            StatusCode::BAD_REQUEST,
            "`{submitted}` is not a settings object"
        );
        assert_eq!(
            refused.message(),
            "A plugin settings patch must change at least one setting."
        );
    }

    for submitted in [json!(["contextLimit"]), json!("ab")] {
        let refused = patch_body(
            &server,
            "context-compaction",
            json!({ "settings": submitted }),
        )
        .await;

        assert_eq!(
            refused.status,
            StatusCode::BAD_REQUEST,
            "`{submitted}` is not a settings object"
        );
        assert_eq!(
            refused.message(),
            "Unknown setting \"0\" for plugin \"context-compaction\". Allowed settings: \
             contextLimit, modelLimits, enabled, minTokens, safetyTurns, mode."
        );
    }

    // And an unknown plugin is still refused BEFORE any of that, because `requirePlugin` runs first.
    let unknown = patch_body(&server, "nope", json!({ "settings": [1] })).await;

    assert_eq!(unknown.status, StatusCode::NOT_FOUND);

    assert_eq!(list(&server, "").await.body["revision"], 0);

    server.stop().await;
}

// ---------------------------------------------------------------------------
// 5 - durability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plugin_settings_survive_a_restart_without_restarting_the_value() {
    let root = Root::new("restart");

    // BOOT ONE. A deployment that has never opened the Plugins pane keeps its exact previous
    // document: some OTHER durable write exists, and it carries no `pluginSettings` member.
    let first = TestServer::start(&root).await;

    let configured = Reply::send(
        client()
            .patch(first.url(&format!("{MOUNT}/config")))
            .json(&json!({ "settings": { "model": "restart/model" } })),
    )
    .await;

    assert_eq!(configured.status, StatusCode::OK);
    assert_eq!(saved_overrides(&root), Value::Null);
    assert_eq!(root.state()["sessions"], json!([]));

    let patched = patch_settings(
        &first,
        "context-compaction",
        json!({ "contextLimit": 200000 }),
    )
    .await;

    assert_eq!(patched.status, StatusCode::OK);
    assert_eq!(
        root.state()["pluginSettings"],
        json!({ "context-compaction": { "contextLimit": 200000 } }),
        "overrides only: the declared default is not written"
    );

    // The member sits BETWEEN `globalSettings` and `sessions`, which is the order TypeScript's
    // `databaseRecord` writes (`sessions.ts:2757-2767`) and what keeps a load -> save round trip
    // byte-stable for a document that already carries the field.
    let text = root.state_text();
    let global_at = text.find("\"globalSettings\"").expect("the pair is saved");
    let plugins_at = text
        .find("\"pluginSettings\"")
        .expect("the overrides are saved");
    let sessions_at = text
        .find("\"sessions\"")
        .expect("the session list is saved");

    assert!(
        global_at < plugins_at && plugins_at < sessions_at,
        "`pluginSettings` must land between `globalSettings` and `sessions`: {global_at} \
         {plugins_at} {sessions_at}"
    );

    first.stop().await;

    // BOOT TWO: the override is read back BEFORE the first request, and the counter restarts.
    runtime::evict_runtime(&root.path);
    let second = TestServer::start(&root).await;

    let page = list(&second, "").await;

    assert_eq!(
        page.body["revision"], 0,
        "the revision counter is epoch-local, so a restart does not inherit it"
    );
    assert_eq!(
        page.body["items"][1]["values"],
        compaction_values(json!(200000)),
        "while the VALUE does survive it"
    );

    // And the restored store is still the LIVE one: an edit after the restart moves the same map, and
    // the document follows it.
    let edited = patch_settings(
        &second,
        "context-compaction",
        json!({ "contextLimit": 150000 }),
    )
    .await;

    assert_eq!(edited.status, StatusCode::OK);
    assert_eq!(edited.header(&REVISION_HEADER).as_deref(), Some("1"));
    assert_eq!(
        root.state()["pluginSettings"],
        json!({ "context-compaction": { "contextLimit": 150000 } })
    );

    second.stop().await;

    // BOOT THREE: the NEWEST value, not the first one - the round trip is not one-way.
    runtime::evict_runtime(&root.path);
    let third = TestServer::start(&root).await;

    let restored = list(&third, "").await;

    assert_eq!(restored.body["revision"], 0);
    assert_eq!(
        restored.body["items"][1]["values"],
        compaction_values(json!(150000))
    );

    third.stop().await;
}

#[tokio::test]
async fn a_restored_document_drops_what_this_build_no_longer_declares() {
    let root = Root::new("restore-lenient");

    // Seed one REAL override, then hand-edit in a retired plugin, a retired key, and the OTHER
    // plugin's key. None of them may abort the boot; each is simply not restored.
    let first = TestServer::start(&root).await;
    let seeded = patch_settings(&first, "async-ping", json!({ "intervalMs": 9000 })).await;

    assert_eq!(seeded.status, StatusCode::OK);

    let mut document = root.state();

    first.stop().await;

    document["pluginSettings"] = json!({
        "retired-plugin": { "anything": 1 },
        "async-ping": { "intervalMs": 9000, "gone": "x" },
        "context-compaction": { "contextLimit": 4000, "intervalMs": 4000 }
    });
    root.write_state(&document);

    runtime::evict_runtime(&root.path);
    let second = TestServer::start(&root).await;

    let page = list(&second, "").await;

    assert_eq!(
        page.body["items"][0]["values"],
        json!({ "intervalMs": 9000 }),
        "the real override survives"
    );
    assert_eq!(
        page.body["items"][1]["values"],
        compaction_values(json!(4000)),
        "a key that belongs to the OTHER plugin is that plugin's own, when it is in range"
    );

    // The NEXT write holds only what this build declares: the retired plugin and the retired key are
    // gone rather than carried forward forever.
    let edited = patch_settings(&second, "async-ping", json!({ "intervalMs": 9500 })).await;

    assert_eq!(edited.status, StatusCode::OK);
    assert_eq!(
        root.state()["pluginSettings"],
        json!({
            "async-ping": { "intervalMs": 9500 },
            "context-compaction": { "contextLimit": 4000 }
        }),
        "catalogue order, declared keys only, no resurrection of a retired entry"
    );

    second.stop().await;
}

#[tokio::test]
async fn a_stored_value_outside_the_declared_bounds_is_dropped_not_fatal() {
    let root = Root::new("restore-out-of-range");

    // The LOADER validates shape; the STORE decides what is still usable. A document holding a value
    // below the plugin's `min` is therefore not corruption: the override is dropped and the declared
    // default applies.
    root.write_state(&json!({
        "version": 1,
        "savedAt": 1_700_000_000_000_u64,
        "registry": [],
        "pluginSettings": { "context-compaction": { "contextLimit": 1 } },
        "sessions": []
    }));

    runtime::evict_runtime(&root.path);
    let server = TestServer::start(&root).await;

    let page = list(&server, "").await;

    assert_eq!(page.status, StatusCode::OK);
    assert_eq!(
        page.body["items"][1]["values"],
        compaction_values(json!(200000)),
        "the dropped override falls back to the declared default"
    );
    assert_eq!(
        page.body["revision"], 0,
        "restoring is not a write, so the counter is untouched"
    );

    // And the store is still writable over it.
    let patched = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 3000 }),
    )
    .await;

    assert_eq!(patched.status, StatusCode::OK);
    assert_eq!(
        root.state()["pluginSettings"],
        json!({ "context-compaction": { "contextLimit": 3000 } })
    );

    server.stop().await;
}

#[tokio::test]
async fn a_document_whose_plugin_settings_shape_is_wrong_names_the_slot() {
    let root = Root::new("restore-corrupt");

    // A hand-edited document whose SHAPE is wrong is corruption, and a boot says so loudly - the
    // persistence rule for every other field of this document.
    root.write_state(&json!({
        "version": 1,
        "savedAt": 1_700_000_000_000_u64,
        "registry": [],
        "pluginSettings": { "async-ping": { "intervalMs": { "nested": 1 } } },
        "sessions": []
    }));

    runtime::evict_runtime(&root.path);

    let error = match runtime::runtime_for_with(&root.path, ServerConfig::default()) {
        Ok(_) => panic!("a malformed pluginSettings value must abort the boot"),
        Err(error) => error,
    };

    assert_eq!(
        error.status, 500,
        "corruption on disk is a loud failure, never a silently empty store"
    );
    assert!(
        error.message.contains(
            "The harness database pluginSettings.async-ping.intervalMs must be a number, a string, \
             or a boolean; plugin settings hold JSON scalars only."
        ),
        "the refusal names the exact slot: {}",
        error.message
    );

    // A plugin entry that is not an object is the same class of failure, one level up.
    root.write_state(&json!({
        "version": 1,
        "savedAt": 1_700_000_000_000_u64,
        "registry": [],
        "pluginSettings": { "async-ping": [1, 2] },
        "sessions": []
    }));
    runtime::evict_runtime(&root.path);

    let error = runtime::runtime_for_with(&root.path, ServerConfig::default())
        .err()
        .expect("a non-object plugin entry is corruption");

    assert!(
        error
            .message
            .contains("The harness database pluginSettings.async-ping must be an object."),
        "{}",
        error.message
    );

    // `pluginSettings` is OPTIONAL in both directions: the same document without the member boots,
    // and an EMPTY map normalizes to absent rather than to an error or a `{}` on disk.
    root.write_state(&json!({
        "version": 1,
        "savedAt": 1_700_000_000_000_u64,
        "registry": [],
        "pluginSettings": { "async-ping": {} },
        "sessions": []
    }));
    runtime::evict_runtime(&root.path);

    let booted = runtime::runtime_for_with(&root.path, ServerConfig::default())
        .expect("an empty pluginSettings entry is a first boot, not corruption");

    assert_eq!(booted.store.plugin_revision(), 0);
    assert_eq!(booted.store.plugin_snapshot(), Map::new());

    // The version did NOT move for this additive field: a pre-plugin document is still readable.
    assert_eq!(
        typecrate_harness::persistence::HARNESS_DATABASE_VERSION,
        1,
        "`pluginSettings` is additive and must not bump the database version"
    );

    runtime::evict_runtime(&root.path);
}

// ---------------------------------------------------------------------------
// 6 - the store surface the routes are built on
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failed_durable_write_answers_500_and_keeps_the_applied_value() {
    let root = Root::new("write-failure");
    let server = TestServer::start(&root).await;

    // One accepted write, so the canonical file exists and the listener holds no lock on it.
    let accepted = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 200000 }),
    )
    .await;

    assert_eq!(accepted.status, StatusCode::OK);
    assert_eq!(accepted.header(&REVISION_HEADER).as_deref(), Some("1"));

    // Make the commit fail by taking the target away from the rename: on POSIX a rename ONTO a
    // directory is `EISDIR`, which is not a transient error, so the writer's retry budget is spent on
    // one attempt and the refusal is the persistence layer's own message.
    fs::remove_file(root.state_file()).expect("the committed document is there");
    fs::create_dir(root.state_file()).expect("the target becomes a directory");

    let refused = patch_settings(
        &server,
        "context-compaction",
        json!({ "contextLimit": 300000 }),
    )
    .await;

    assert_eq!(refused.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        refused
            .message()
            .contains("Could not write the harness database"),
        "the 500 says which step failed: {}",
        refused.message()
    );

    // The value STAYS APPLIED, which is TypeScript's documented choice (`sessions.ts:1848-1851`):
    // every running session was handed the new number the moment it was applied, so rolling it back
    // would report a failure the process does not actually have.
    let page = list(&server, "").await;

    assert_eq!(
        page.body["items"][1]["values"],
        compaction_values(json!(300000)),
        "a failed write does not undo the live value"
    );
    assert_eq!(
        page.body["revision"], 2,
        "and the counter moved with the accepted patch, not with the commit"
    );

    // The OLD document on disk is untouched (the staging file is discarded), so nothing half-wrote.
    assert!(
        fs::read_dir(root.state_file()).is_ok(),
        "the rename target is still the directory that blocked it"
    );

    let _ = fs::remove_dir(root.state_file());
    server.stop().await;
}

#[tokio::test]
async fn the_store_projects_declared_defaults_until_an_override_lands() {
    let root = Root::new("store-projection");

    runtime::evict_runtime(&root.path);
    let booted = runtime::runtime_for_with(&root.path, ServerConfig::default())
        .expect("an empty root boots");

    // Nothing stored: every key answers its DECLARED default, and the snapshot is EMPTY, so the
    // writer omits the member entirely.
    assert_eq!(
        booted.store.plugin_values("context-compaction"),
        object(compaction_values(json!(200000)))
    );
    assert!(booted.store.plugin_snapshot().is_empty());
    assert_eq!(booted.store.plugin_revision(), 0);

    // An unknown plugin has no declared settings to resolve, so it answers an EMPTY map rather than
    // an error (`plugins.ts values`'s rule), and it has no row.
    assert!(booted.store.plugin_values("retired").is_empty());
    assert!(booted.store.plugin_row("retired").is_none());
    assert_eq!(
        booted
            .store
            .plugin_rows()
            .iter()
            .map(|row| row.name.clone())
            .collect::<Vec<_>>(),
        CATALOGUE
    );

    // A refused update mutates nothing and bumps nothing.
    let refused = booted
        .store
        .update_plugin_settings(
            "context-compaction",
            &object(json!({ "contextLimit": "nope" })),
        )
        .expect_err("a non-numeric value is refused");

    assert_eq!(refused.status, 400);
    assert_eq!(
        refused.message,
        "\"contextLimit\" of plugin \"context-compaction\" must be a finite number (it is declared \
         \"number\")."
    );
    assert_eq!(booted.store.plugin_revision(), 0);
    assert!(booted.store.plugin_snapshot().is_empty());

    // An accepted one moves the value, the counter, and the snapshot.
    booted
        .store
        .update_plugin_settings(
            "context-compaction",
            &object(json!({ "contextLimit": 4000 })),
        )
        .expect("4000 is inside the declared bounds");

    assert_eq!(booted.store.plugin_revision(), 1);
    assert_eq!(
        booted.store.plugin_snapshot(),
        object(json!({ "context-compaction": { "contextLimit": 4000 } }))
    );
    assert_eq!(
        booted.store.plugin_values("context-compaction"),
        object(compaction_values(json!(4000)))
    );

    // The row's own `values` and the store's projection cannot disagree: both are the same read.
    let row = booted
        .store
        .plugin_row("context-compaction")
        .expect("this build ships context-compaction");

    assert_eq!(row.values, object(compaction_values(json!(4000))));
    assert_eq!(
        row.settings[0].default,
        json!(200000),
        "the reset target stays"
    );

    runtime::evict_runtime(&root.path);
}
