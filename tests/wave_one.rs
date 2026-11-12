//! Wave 1 (A1) - the four new operations and the DTOs they speak, pinned end to end.
//!
//! Everything in this file is pinned against `packages/underload/harness` (the TS source of
//! record), NOT observed from this build: each expected sentence, key set, and counter value below
//! is copied from `src/server/api.ts`, `src/shared/harness-api.ts`, or
//! `src/server/stream/protocol.ts`, and the line is named in the comment next to it.
//!
//! WHAT IS DELIBERATELY A PLACEHOLDER HERE
//! ---------------------------------------
//! Two of the four operations answer `500` after their full gate sequence, because the ENGINE they
//! write is not part of this wave and the wave that owns it does not exist yet:
//!
//!   * `GET  .../records` - no record store in this port (`src/api/records.rs`'s module doc states
//!     the whole argument), so there is no `store_rev` to print.
//!   * `PATCH .../blocks/{block_id}` - `contract::HarnessMessage` carries no `included` /
//!     `reasoningIncluded` field, and adding them touches 27 construction sites in files this wave
//!     does not own (`src/api/turns.rs`'s handler doc states the same).
//!
//! What IS implemented and is therefore pinned exactly: the route table (see `tests/routes.rs`),
//! every gate and refusal sentence in TS's order, the request and response DTOs, the `toolSettings`
//! persistence path, the stream vocabulary, and the error-response header rule (a refusal carries
//! NEITHER `X-Harness-Epoch` nor `X-Harness-Revision`).
//!
//! WHAT THIS FILE DOES NOT COVER (the honest gap)
//! ---------------------------------------------
//! The resume route's SUCCESS path (the requeue, its receipt, and its `X-Harness-Revision`) and the
//! `is <status>; only a stopped thread can be resumed.` refusal both need a turn that a provider
//! actually answered, which needs `tests/http_sessions.rs`'s scripted OpenAI-compatible listener;
//! and the block write's per-axis `409`s need the entry flags above. Each is called out again at
//! the guard that IS pinned here.

use std::fs;
use std::path::{Path, PathBuf};

use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName};
use serde_json::{Value, json};

use typecrate_harness::stream::protocol::{
    StreamScopeIds, default_stream_link, default_ws_limits, kind_resources, scope_baselines,
    scope_publishes,
};
use typecrate_harness::{api, contract, package_root, persistence, runtime};

/// The two contract headers, spelled the way the wire spells them.
static EPOCH_HEADER: HeaderName = HeaderName::from_static("x-harness-epoch");
static REVISION_HEADER: HeaderName = HeaderName::from_static("x-harness-revision");

// ---------------------------------------------------------------------------
// The fixture: a per-test dump root driven through the dispatcher itself
// ---------------------------------------------------------------------------

/// One dump root. `api::handle_request` is literally what the discovered route bridge hands the
/// same `Request` to (`endpoint/harness/harness-route.rs`), so these tests drive the production
/// dispatcher rather than a stand-in.
struct Root {
    path: PathBuf,
}

impl Root {
    /// Every helper takes `&Path`, so the fixture derefs to its own root.
    fn new(test: &str) -> Self {
        let path = package_root::normalize_lexically(&Path::new(env!("CARGO_MANIFEST_DIR")).join(
            format!(
                "../../../Temp/typecrate-harness/wave-one/{}-{test}",
                std::process::id()
            ),
        ));

        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path)
            .unwrap_or_else(|error| panic!("could not create `{}`: {error}", path.display()));

        Self { path }
    }
}

impl std::ops::Deref for Root {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        // The runtime cache is keyed by root, so a leftover runtime would hand the next test this
        // directory's live store and its tool switches.
        runtime::evict_runtime(&self.path);

        let _ = fs::remove_dir_all(&self.path);
    }
}

/// One in-process response, reduced to what the contract pins.
struct Reply {
    status: u16,
    headers: HeaderMap,
    body: Value,
}

impl Reply {
    async fn send(root: &Path, method: &str, pathname: &str, body: Option<Value>) -> Self {
        let builder = Request::builder()
            .method(method)
            .uri(pathname)
            // The origin gate reads `Host`; no `Origin` header means a non-browser client, which
            // the contract allows.
            .header("host", "127.0.0.1:5181");

        let text = body.map(|value| serde_json::to_string(&value).expect("a body serializes"));

        let request = match text {
            Some(text) => builder
                .header("content-type", "application/json")
                .body(Body::from(text))
                .expect("a JSON body builds"),
            None => builder
                .body(Body::empty())
                .expect("a bodyless request builds"),
        };

        let response = api::handle_request(root.to_path_buf(), request).await;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body is readable")
            .to_vec();

        Self {
            status,
            headers,
            body: serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                panic!(
                    "a JSON body is expected, got `{}`: {error}",
                    String::from_utf8_lossy(&bytes)
                )
            }),
        }
    }

    async fn get(root: &Path, pathname: &str) -> Self {
        Self::send(root, "GET", pathname, None).await
    }

    async fn post(root: &Path, pathname: &str, body: Value) -> Self {
        Self::send(root, "POST", pathname, Some(body)).await
    }

    async fn patch(root: &Path, pathname: &str, body: Value) -> Self {
        Self::send(root, "PATCH", pathname, Some(body)).await
    }

    async fn delete(root: &Path, pathname: &str) -> Self {
        Self::send(root, "DELETE", pathname, None).await
    }

    /// One header's text, or the empty string when it is absent.
    fn header_text(&self, name: &HeaderName) -> String {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    }

    /// The error envelope's message.
    fn message(&self) -> String {
        self.body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }

    /// One refusal, pinned completely: the status, the envelope, and the Wave 1 header rule.
    ///
    /// `api.ts`'s error writer (`sendError` -> a bare `Response.json`, `handler.ts:284-286`) never
    /// reaches `stampContractHeaders`, so a refusal carries `content-type`, `cache-control` and
    /// `content-length` and NOTHING else from the contract pair. A client may therefore read
    /// "the header is absent" as "this answer is a refusal".
    fn assert_refusal(&self, status: u16, message: &str) {
        assert_eq!(
            self.status,
            status,
            "status of a refusal: {}",
            self.message()
        );
        assert_eq!(
            self.body,
            json!({ "error": { "message": message } }),
            "the single error envelope"
        );
        assert_eq!(
            self.header_text(&axum::http::header::CONTENT_TYPE),
            "application/json; charset=utf-8",
            "`handler.ts:86`"
        );
        assert_eq!(
            self.header_text(&axum::http::header::CACHE_CONTROL),
            "no-store",
            "a control-plane refusal is never cacheable"
        );
        assert!(
            !self.headers.contains_key(&EPOCH_HEADER),
            "a refusal carries no X-Harness-Epoch"
        );
        assert!(
            !self.headers.contains_key(&REVISION_HEADER),
            "and no X-Harness-Revision"
        );
    }

    /// A success: the epoch rides it, and the revision is exactly what the route's own counter says.
    fn assert_success_headers(&self, revision: Option<u64>) {
        assert!(
            self.headers.contains_key(&EPOCH_HEADER),
            "a successful contract response carries the epoch (`api.ts:1224-1230`)"
        );
        assert_eq!(
            self.header_text(&REVISION_HEADER),
            revision.map(|value| value.to_string()).unwrap_or_default(),
            "X-Harness-Revision appears exactly when the route has a counter to quote"
        );
    }
}

/// Creates one session on the harness preset and returns its id.
async fn create_session(root: &Path) -> String {
    let reply = Reply::post(
        root,
        &format!("{}/sessions", contract::HARNESS_API_ROOT),
        json!({ "harness": "instruct", "title": "wave one" }),
    )
    .await;

    assert_eq!(reply.status, 201, "a create is 201: {}", reply.message());

    reply.body["session"]["id"]
        .as_str()
        .expect("the created session names itself")
        .to_owned()
}

/// Creates one conversation on a roster member and returns its id. A fresh conversation is
/// `status: idle` with no turns (`store/threads.rs:99`), which is exactly the lane shape the resume
/// guards are written against.
async fn create_thread(root: &Path, session_id: &str) -> String {
    let reply = Reply::post(
        root,
        &format!(
            "{}/sessions/{session_id}/threads",
            contract::HARNESS_API_ROOT
        ),
        json!({ "agent": "instruct", "label": "wave-one-lane" }),
    )
    .await;

    assert_eq!(reply.status, 201, "a create is 201: {}", reply.message());

    reply.body["thread"]["id"]
        .as_str()
        .expect("the created conversation names itself")
        .to_owned()
}

// ---------------------------------------------------------------------------
// 1 - the DTOs serialize exactly as the TypeScript contract spells them
// ---------------------------------------------------------------------------

/// The bootstrap read and the delta read, as `harness-api.ts:1538` / `:1556` spell them, plus the
/// open-keyed entry row whose declared fields ride inline beside the four fixed keys.
#[test]
fn the_records_shapes_round_trip_the_contract_fixtures() {
    let snapshot = json!({
        "session_id": "ses_1",
        "revision": 4,
        "store_rev": 7,
        "collections_rev": 2,
        "collections": [
            {
                "name": "chapters",
                "store_rev": 7,
                "count": 1,
                "fields": [
                    { "key": "title", "type": "text", "required": true },
                    {
                        "key": "status",
                        "type": "enum",
                        "required": false,
                        "values": ["planned", "draft", "complete"]
                    },
                    { "key": "notes", "type": "strings", "required": false }
                ],
                "entries": [
                    {
                        "id": "rec_1",
                        "rev": 3,
                        "updated_by": "instruct",
                        "updated_thread_id": "thr_1",
                        "updated_at": "2026-03-04T05:06:07.008Z",
                        "title": "Cold open",
                        "status": "draft",
                        "notes": ["keep it short"]
                    }
                ]
            }
        ],
        "changes": [
            {
                "seq": 6,
                "collection": "chapters",
                "rev": 7,
                "id": "rec_1",
                "op": "update",
                "by": "instruct",
                "thread_id": "thr_1",
                "at": "2026-03-04T05:06:07.008Z",
                "fields": ["status"],
                "item_rev": 3
            },
            {
                "seq": 7,
                "collection": "tasks",
                "rev": 8,
                "id": "rec_9",
                "op": "remove",
                "at": "2026-03-04T05:06:09.010Z"
            }
        ]
    });

    // Parse, then re-serialize, and require the bytes to be the fixture. An absent optional stays
    // absent (`skip_serializing_if`), and a declared field never leaks into a fixed key.
    let parsed: contract::RecordsSnapshotResponse =
        serde_json::from_value(snapshot.clone()).expect("the snapshot shape");

    assert_eq!(
        serde_json::to_value(&parsed).expect("the snapshot serializes"),
        snapshot,
        "the snapshot round-trips byte-for-byte, including the OPEN field map of an entry row"
    );
    assert_eq!(parsed.session_id, "ses_1");
    assert_eq!(parsed.store_rev, 7);
    assert_eq!(parsed.collections_rev, 2);
    assert_eq!(parsed.collections.len(), 1);

    let collection = &parsed.collections[0];

    assert_eq!(collection.name, "chapters");
    assert_eq!(collection.count, 1);
    assert_eq!(collection.fields.len(), 3);
    assert_eq!(collection.fields[1].r#type, contract::RecordFieldType::Enum);
    assert_eq!(
        collection.fields[1].values,
        Some(vec![
            "planned".to_owned(),
            "draft".to_owned(),
            "complete".to_owned()
        ])
    );

    // The declared fields are reachable through the flattened map, next to the fixed keys.
    let entry = &collection.entries[0];

    assert_eq!(entry.id, "rec_1");
    assert_eq!(entry.rev, 3);
    assert_eq!(entry.updated_by.as_deref(), Some("instruct"));
    assert_eq!(entry.updated_thread_id.as_deref(), Some("thr_1"));
    assert_eq!(entry.extra["title"], json!("Cold open"));
    assert_eq!(entry.extra["status"], json!("draft"));

    // The feed: an `update` names the fields it moved, and a `remove` names the record and stops.
    assert_eq!(parsed.changes.len(), 2);
    assert_eq!(parsed.changes[0].op, contract::RecordChangeOp::Update);
    assert_eq!(parsed.changes[0].fields, Some(vec!["status".to_owned()]));
    assert_eq!(parsed.changes[0].item_rev, Some(3));
    assert_eq!(parsed.changes[1].op, contract::RecordChangeOp::Remove);
    assert_eq!(parsed.changes[1].by, None);
    assert_eq!(parsed.changes[1].fields, None);

    // The delta read, both arms: the answered cursor and the reset.
    let delta = json!({
        "session_id": "ses_1",
        "revision": 4,
        "store_rev": 9,
        "collections_rev": 2,
        "changes": [
            {
                "seq": 8,
                "collection": "tasks",
                "rev": 9,
                "id": "rec_9",
                "op": "add",
                "by": "instruct",
                "at": "2026-03-04T05:06:11.012Z",
                "item_rev": 1
            }
        ],
        "reset": false
    });
    let parsed_delta: contract::RecordsDeltaResponse =
        serde_json::from_value(delta.clone()).expect("the delta shape");

    assert_eq!(
        serde_json::to_value(&parsed_delta).expect("the delta serializes"),
        delta
    );
    assert!(!parsed_delta.reset);

    let reset = json!({
        "session_id": "ses_1",
        "revision": 4,
        "store_rev": 9,
        "collections_rev": 2,
        "changes": [],
        "reset": true
    });
    let parsed_reset: contract::RecordsDeltaResponse =
        serde_json::from_value(reset.clone()).expect("the reset arm");

    assert_eq!(
        serde_json::to_value(&parsed_reset).expect("the reset arm serializes"),
        reset
    );

    // The whole `records.updated` payload: the two watermarks and nothing else
    // (`harness-api.ts:1571`).
    let signal = json!({ "store_rev": 9, "collections_rev": 2 });
    let parsed_signal: contract::RecordsSignal =
        serde_json::from_value(signal.clone()).expect("the signal shape");

    assert_eq!(
        parsed_signal,
        contract::RecordsSignal {
            store_rev: 9,
            collections_rev: 2
        }
    );
    assert_eq!(
        serde_json::to_value(parsed_signal).expect("the signal serializes"),
        signal
    );
}

/// The segment-inclusion write: camelCase on the request, snake_case on the receipt, and an absent
/// axis never serialized as `null` (`harness-api.ts:2222` and `:2246`).
#[test]
fn the_block_write_shapes_use_the_contract_spellings() {
    let request = json!({ "included": false, "reasoningIncluded": true });
    let parsed: contract::PatchTurnBlockRequest =
        serde_json::from_value(request.clone()).expect("the request shape");

    assert_eq!(
        serde_json::to_value(&parsed).expect("the request serializes"),
        request,
        "the request axis names are camelCase, exactly as the client spells them"
    );
    assert_eq!(
        parsed,
        contract::PatchTurnBlockRequest {
            included: Some(false),
            reasoning_included: Some(true),
        }
    );

    // One axis only: the other key is ABSENT, not `false` and not `null`.
    let one_axis = json!({ "reasoningIncluded": false });
    let parsed_one: contract::PatchTurnBlockRequest =
        serde_json::from_value(one_axis.clone()).expect("one axis is a valid request");

    assert_eq!(
        serde_json::to_value(&parsed_one).expect("one axis serializes"),
        one_axis
    );
    assert_eq!(parsed_one.included, None);

    let response = json!({
        "receipt": {
            "session_id": "ses_1",
            "turn_id": "2",
            "block_id": "blk_3",
            "included": false,
            "reasoning_included": true,
            "thread_id": "thr_1",
            "revision": 12,
            "links": {
                "block": "/v1/harness/sessions/ses_1/turns/2/blocks/blk_3",
                "transcript": "/v1/harness/sessions/ses_1/threads/thr_1/transcript"
            }
        }
    });
    let parsed_response: contract::TurnBlockWriteResponse =
        serde_json::from_value(response.clone()).expect("the receipt shape");

    assert_eq!(
        serde_json::to_value(&parsed_response).expect("the receipt serializes"),
        response,
        "the receipt states the state the SERVER persisted, in snake_case"
    );
    assert_eq!(
        parsed_response.receipt,
        contract::TurnBlockWriteReceipt {
            session_id: "ses_1".to_owned(),
            turn_id: "2".to_owned(),
            block_id: "blk_3".to_owned(),
            included: false,
            reasoning_included: Some(true),
            thread_id: Some("thr_1".to_owned()),
            revision: Some(12),
            links: Some(contract::TurnBlockWriteLinks {
                block: Some(format!(
                    "{}/sessions/ses_1/turns/2/blocks/blk_3",
                    contract::HARNESS_API_ROOT
                )),
                transcript: Some(format!(
                    "{}/sessions/ses_1/threads/thr_1/transcript",
                    contract::HARNESS_API_ROOT
                )),
            }),
        }
    );
}

/// The tool-switch body: one field, and the contract's OWN sentence for everything that is not a
/// boolean (`api.ts:2243-2249`). Serde's type error would say something else, which is why the
/// value stays a `Value` and this accessor is the gate.
#[test]
fn the_tool_switch_body_refuses_everything_that_is_not_a_boolean() {
    const SENTENCE: &str = concat!(
        "\"enabled\" must be a boolean (true runs the tool, false disables it for every ",
        "session)."
    );

    for body in [
        json!({}),
        json!({ "enabled": null }),
        json!({ "enabled": "no" }),
        json!({ "enabled": 0 }),
    ] {
        let parsed: contract::PatchHarnessToolRequest =
            serde_json::from_value(body.clone()).expect("the body decodes");

        let error = parsed
            .enabled_boolean()
            .expect_err(&format!("{body} is not a boolean"));

        assert_eq!(error.status, 400, "`{body}` is a 400");
        assert_eq!(error.message, SENTENCE, "`{body}` names the switch");
    }

    for body in [json!({ "enabled": true }), json!({ "enabled": false })] {
        let parsed: contract::PatchHarnessToolRequest =
            serde_json::from_value(body.clone()).expect("the body decodes");

        assert_eq!(
            parsed.enabled_boolean().expect("a boolean is accepted"),
            body["enabled"].as_bool().expect("a boolean")
        );
    }
}

/// The plugin diagnostic the session resource carries (`harness-api.ts:930`), including the
/// kebab-case kind vocabulary and the one kind that names no hook.
#[test]
fn the_plugin_diagnostic_shape_is_the_contracts() {
    let fixture = json!({
        "at": 1_772_000_000_000_u64,
        "kind": "handler-threw",
        "owner": "policy",
        "event": "onMessage",
        "message": "boom",
        "data": { "ms": 12 }
    });
    let parsed: contract::SessionPluginDiagnostic =
        serde_json::from_value(fixture.clone()).expect("the diagnostic shape");

    assert_eq!(
        serde_json::to_value(&parsed).expect("the diagnostic serializes"),
        fixture
    );
    assert_eq!(
        parsed.kind,
        contract::SessionPluginDiagnosticKind::HandlerThrew
    );

    // A `policy` report names no hook, and the key is absent rather than empty.
    let policy = json!({
        "at": 1_772_000_000_001_u64,
        "kind": "policy",
        "owner": "policy",
        "message": "parked"
    });
    let parsed_policy: contract::SessionPluginDiagnostic =
        serde_json::from_value(policy.clone()).expect("the policy report");

    assert_eq!(
        serde_json::to_value(&parsed_policy).expect("the policy report serializes"),
        policy
    );
    assert_eq!(parsed_policy.event, None);

    for kind in [
        "handler-timed-out",
        "invalid-decision",
        "policy",
        "handler-threw",
    ] {
        let value: contract::SessionPluginDiagnosticKind =
            serde_json::from_value(json!(kind)).expect("a closed kind");
        let spelled = serde_json::to_value(value).expect("the kind serializes");

        assert_eq!(spelled, json!(kind), "the wire spells the kinds kebab-case");
    }
}

// ---------------------------------------------------------------------------
// 2 - the stream vocabulary carries the record store
// ---------------------------------------------------------------------------

#[test]
fn the_record_store_joins_the_stream_vocabulary() {
    // One more counter kind, and the ceiling that has to hold it (`harness-stream.ts:28,53`).
    assert_eq!(contract::HARNESS_STREAM_RESOURCE_KINDS.len(), 21);
    assert_eq!(contract::HARNESS_STREAM_RESOURCE_KINDS[20], "records");
    assert_eq!(default_ws_limits().revision_records_per_state, 21);

    // The update kind, its resource, and the scope that publishes it (`protocol.ts:62,81,128,155`).
    assert!(contract::HARNESS_WS_UPDATE_KINDS.contains(&"records.updated"));
    assert_eq!(kind_resources("records.updated"), ["records"]);
    assert!(scope_publishes("session").contains(&"records.updated"));
    assert_eq!(
        scope_baselines("session").last(),
        Some(&"records"),
        "the record store is the session snapshot's LAST baseline member"
    );

    // Every per-scope baseline stays within the ceiling, `records` included.
    for scope in contract::HARNESS_WS_SCOPES {
        assert!(
            (scope_baselines(scope).len() as u64) <= default_ws_limits().revision_records_per_state
        );
    }

    // The recovery link: the record store's canonical read, with NO `?limit` - the snapshot is
    // already bounded by the engine's own caps (`protocol.ts:255`).
    assert_eq!(
        default_stream_link(
            "records",
            &StreamScopeIds {
                session_id: Some("ses_1".to_owned()),
                thread_id: None,
                turn_id: None,
            }
        ),
        format!("{}/sessions/ses_1/records", contract::HARNESS_API_ROOT)
    );

    // The same link the baseline frame prints is the route table's own template, and the table is
    // pinned template-for-template in `tests/routes.rs`.
    assert!(
        api::ROUTES.iter().any(|route| route.template
            == format!(
                "{}/sessions/{{session_id}}/records",
                contract::HARNESS_API_ROOT
            )),
        "the records route is mounted, so the recovery link resolves"
    );
}

// ---------------------------------------------------------------------------
// 3 - `GET .../records`: the gates, then the placeholder
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_records_route_runs_its_gates_before_the_placeholder() {
    let root = Root::new("records");
    let records = |session: &str, query: &str| -> String {
        format!(
            "{}/sessions/{session}/records{query}",
            contract::HARNESS_API_ROOT
        )
    };

    // 1. Identity first (`api.ts:2737`), so a typo never reaches the query gate.
    Reply::get(&root, &records("missing", ""))
        .await
        .assert_refusal(404, "Unknown session \"missing\".");

    let session = create_session(&root.path).await;

    // 2. The cursor, validated even while the engine is absent: a malformed `since` is a 400 on
    // this contract whatever the store holds (`api.ts:2740-2752`).
    // `api::param_of` treats a blank value as ABSENT (the same `paramOf` rule every other route
    // uses, `api.ts:481-488`), so `?since=` is the snapshot arm rather than a refusal. The
    // spellings below are all PRESENT and malformed - and a sign is one of them because TS's gate
    // is `/^\d+$/` (`api.ts:2747`), not a number parser that would happily take one.
    for query in [
        "?since=abc",
        "?since=-3",
        "?since=1.5",
        "?since=-0",
        "?since=1e3",
        "?since=7d",
    ] {
        Reply::get(&root, &records(&session, query))
            .await
            .assert_refusal(
                400,
                "Query parameter \"since\" must be a non-negative integer.",
            );
    }

    // A digit string that parses but cannot be a real feed position is the OTHER sentence
    // (`api.ts:2753-2757`): "the client sent an impossible position" has to stay distinguishable
    // from "the client sent junk", because only the second one is the client's bug.
    const SAFE_MAX_SENTENCE: &str =
        "Query parameter \"since\" must be a non-negative integer within the safe integer range.";

    for query in ["?since=9007199254740992", "?since=99999999999999999999999"] {
        Reply::get(&root, &records(&session, query))
            .await
            .assert_refusal(400, SAFE_MAX_SENTENCE);
    }

    // 3. A real session and a well-formed cursor - the one answer this build cannot give yet. The
    // last figure is `Number.MAX_SAFE_INTEGER`, which the gate accepts.
    // `?since=+7` IS accepted, on the decoder's rule rather than the gate's: a query value is
    // form-decoded, so `+` becomes a space, and `paramOf` trims (`decodeComponent` and `paramOf`
    // both do this in TS, `api.ts:481-488`) - one reason the gate sees `7` and not `+7`.
    for query in [
        "",
        "?since=0",
        "?since=7",
        "?since=+7",
        "?since=9007199254740991",
    ] {
        let reply = Reply::get(&root, &records(&session, query)).await;

        assert_eq!(reply.status, 500, "`{query}` reaches the engine gap");
        assert_eq!(
            reply.message(),
            "The record store is not implemented on this build; GET records answers once the record-store stage lands."
        );
        assert!(
            !reply.headers.contains_key(&EPOCH_HEADER),
            "a 500 is a refusal too, so it carries no epoch"
        );
    }

    // 4. The verb gate is the route table's, so `records` answers 405 with the one verb it
    // declares rather than a 404.
    Reply::delete(&root, &records(&session, ""))
        .await
        .assert_refusal(
            405,
            &format!(
                "Use GET for {}/sessions/{session}/records.",
                contract::HARNESS_API_ROOT
            ),
        );
}

// ---------------------------------------------------------------------------
// 4 - `PATCH /tools/{tool_id}`: the switch, its refusals, and its durability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_tool_switch_refuses_and_answers_the_contract() {
    let root = Root::new("tool-switch");
    let tool_path = |name: &str| format!("{}/tools/{name}", contract::HARNESS_API_ROOT);

    // Identity FIRST: a typo on a name that does not exist is a 404 and never a 400 about a body
    // the client has not sent yet (`api.ts:2233-2235`).
    Reply::patch(&root, &tool_path("nope"), json!({ "enabled": false }))
        .await
        .assert_refusal(404, "Unknown tool \"nope\".");

    // The body gate: known fields, then the boolean (`api.ts:2237-2249`).
    for body in [
        json!({}),
        json!({ "enabled": "no" }),
        json!({ "enabled": null }),
    ] {
        Reply::patch(&root, &tool_path("read"), body.clone())
            .await
            .assert_refusal(
                400,
                "\"enabled\" must be a boolean (true runs the tool, false disables it for every session).",
            );
    }

    Reply::patch(
        &root,
        &tool_path("read"),
        json!({ "enabled": true, "extra": 1 }),
    )
    .await
    .assert_refusal(400, "Unknown field \"extra\". Allowed fields: enabled.");

    // Nothing moved while the body was refused.
    let before = Reply::get(&root, &tool_path("read")).await;

    assert_eq!(before.status, 200);
    assert_eq!(before.body["tool"]["enabled"], json!(true));
    before.assert_success_headers(None);

    // The accepted switch: the whole identified read back, and the header quotes the SETTINGS
    // store's counter - not the catalogue's, because no registration changed (`api.ts:2254-2259`).
    let disabled = Reply::patch(&root, &tool_path("read"), json!({ "enabled": false })).await;

    assert_eq!(disabled.status, 200, "{}", disabled.message());
    assert_eq!(disabled.body["tool"]["name"], json!("read"));
    assert_eq!(disabled.body["tool"]["enabled"], json!(false));
    disabled.assert_success_headers(Some(1));

    // A disabled tool stays REGISTERED and stays LISTED - the switch gates what an AGENT may reach,
    // not what the deployment has (`tool-settings.ts` "what disabled does not mean").
    let page = Reply::get(
        &root,
        &format!("{}/tools?limit=100", contract::HARNESS_API_ROOT),
    )
    .await;
    let read_row = page.body["items"]
        .as_array()
        .expect("a page")
        .iter()
        .find(|row| row["name"] == json!("read"))
        .expect("`read` is still listed when it is disabled");

    assert_eq!(read_row["enabled"], json!(false));

    let detail = Reply::get(&root, &tool_path("read")).await;

    assert_eq!(detail.body["tool"]["enabled"], json!(false));

    // An idempotent write is ACCEPTED and still bumps: the counter counts accepted switches
    // (`api.ts:2250-2252`).
    let again = Reply::patch(&root, &tool_path("read"), json!({ "enabled": false })).await;

    assert_eq!(again.status, 200);
    again.assert_success_headers(Some(2));

    // R5: NO protected-name exception. A harness-owned core tool takes this write exactly like a
    // catalogue entry (`api.ts:2244-2250`).
    let core = Reply::patch(
        &root,
        &tool_path("toolRequest"),
        json!({ "enabled": false }),
    )
    .await;

    assert_eq!(
        core.status,
        200,
        "a core tool takes the switch: {}",
        core.message()
    );
    assert_eq!(core.body["tool"]["name"], json!("toolRequest"));
    assert_eq!(core.body["tool"]["enabled"], json!(false));
    core.assert_success_headers(Some(3));

    // Re-enabling is the same route in the other direction, and it clears the deny entry.
    let enabled = Reply::patch(&root, &tool_path("read"), json!({ "enabled": true })).await;

    assert_eq!(enabled.status, 200);
    assert_eq!(enabled.body["tool"]["enabled"], json!(true));
    enabled.assert_success_headers(Some(4));
}

/// The durable half: the switch rides the state document as `toolSettings`, and an empty deny list
/// removes the member entirely rather than writing `{ "disabled": [] }`
/// (`persistence.ts:1517-1540`).
#[tokio::test]
async fn the_tool_switch_is_durable_and_the_member_disappears_when_nothing_is_disabled() {
    let root = Root::new("tool-switch-durable");
    let tool_path = |name: &str| format!("{}/tools/{name}", contract::HARNESS_API_ROOT);

    assert!(
        Reply::patch(&root, &tool_path("glob"), json!({ "enabled": false }))
            .await
            .status
            == 200
    );
    assert!(
        Reply::patch(&root, &tool_path("grep"), json!({ "enabled": false }))
            .await
            .status
            == 200
    );

    let document = persistence::read_harness_database(&root.path)
        .expect("the document is readable")
        .expect("the persisting root has written one");

    assert_eq!(
        document
            .extra
            .get(persistence::TOOL_SETTINGS_MEMBER)
            .expect("the member is written"),
        &json!({ "disabled": ["glob", "grep"] }),
        "the deny list is sorted and deduplicated as the TS reader normalizes it"
    );

    // A restart reads the member back: the runtime cache is dropped, and the NEW runtime reports
    // the same switches it would have restored from `state.json`.
    runtime::evict_runtime(&root.path);

    let detail = Reply::get(&root.path, &tool_path("glob")).await;

    assert_eq!(detail.status, 200);
    assert_eq!(
        detail.body["tool"]["enabled"],
        json!(false),
        "a restart does not resurrect a disabled tool"
    );

    // Clearing every switch removes the member rather than emptying it.
    for name in ["glob", "grep"] {
        assert!(
            Reply::patch(&root.path, &tool_path(name), json!({ "enabled": true }))
                .await
                .status
                == 200
        );
    }

    let document = persistence::read_harness_database(&root.path)
        .expect("the document is readable")
        .expect("the document is still there");

    assert!(
        document
            .extra
            .get(persistence::TOOL_SETTINGS_MEMBER)
            .is_none(),
        "an empty deny list writes no `toolSettings` member at all"
    );
}

// ---------------------------------------------------------------------------
// 5 - `PATCH .../blocks/{block_id}`: the guards, then the segment lookup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_block_write_guards_answer_before_the_segment_lookup() {
    let root = Root::new("block-write");
    let session = create_session(&root.path).await;

    // Identity, in TS's order: session, then turn (`api.ts:4598-4604`).
    Reply::patch(
        &root,
        &format!(
            "{}/sessions/missing/turns/1/blocks/blk",
            contract::HARNESS_API_ROOT
        ),
        json!({ "included": true }),
    )
    .await
    .assert_refusal(404, "Unknown session \"missing\".");

    Reply::patch(
        &root,
        &format!(
            "{}/sessions/{session}/turns/nope/blocks/blk",
            contract::HARNESS_API_ROOT
        ),
        json!({ "included": true }),
    )
    .await
    .assert_refusal(
        404,
        &format!("Unknown turn \"nope\" in session \"{session}\"."),
    );

    // The route needs a recorded turn before it reads a body at all, so one is minted here. The
    // run itself is irrelevant: this handler refuses BEFORE it touches the record.
    let receipt = Reply::post(
        &root,
        &format!("{}/sessions/{session}/tasks", contract::HARNESS_API_ROOT),
        json!({ "input": "/help" }),
    )
    .await;

    assert_eq!(receipt.status, 202, "{}", receipt.message());

    let turn_id = receipt.body["turn_id"]
        .as_str()
        .expect("the receipt names the turn")
        .to_owned();
    // The path is constant once the turn exists; the body is the only variable part of a guard.
    let block_path = format!(
        "{}/sessions/{session}/turns/{turn_id}/blocks/blk",
        contract::HARNESS_API_ROOT
    );
    // At least one axis (`api.ts:4605-4612`).
    Reply::patch(&root, &block_path, json!({}))
        .await
        .assert_refusal(
            400,
            "A context segment write requires at least one of \"included\" and \"reasoningIncluded\".",
        );
    Reply::patch(&root, &block_path, json!({ "included": null }))
        .await
        .assert_refusal(
            400,
            "A context segment inclusion write requires a boolean \"included\" value.",
        );

    // The two per-axis boolean sentences (`api.ts:4613-4620`). `null` counts as PRESENT because TS
    // compares against `undefined`, and `"false"` / `0` are typos on this surface, not values.
    for value in [json!("false"), json!(0), json!([]), json!({ "on": true })] {
        Reply::patch(&root, &block_path, json!({ "included": value }))
            .await
            .assert_refusal(
                400,
                "A context segment inclusion write requires a boolean \"included\" value.",
            );
    }

    for value in [json!("yes"), json!(1), json!("true"), json!(null)] {
        Reply::patch(&root, &block_path, json!({ "reasoningIncluded": value }))
            .await
            .assert_refusal(
                400,
                "A context segment write requires a boolean \"reasoningIncluded\" value.",
            );
    }

    // The field whitelist runs before the axes, so a third axis is refused by name.
    Reply::patch(
        &root,
        &block_path,
        json!({ "included": true, "lane": "prompt" }),
    )
    .await
    .assert_refusal(
        400,
        "Unknown field \"lane\". Allowed fields: included, reasoningIncluded.",
    );

    // A well-formed body clears every guard, and the segment lookup is what answers next: the
    // lookup is the SAME resolver the read runs (`api/turns.rs:1200-1204`, from `store.blockDetail`),
    // so the invented id this fixture uses - `blk`, which the engine never mints - is a 404 naming
    // the block. This loop used to pin the 500 PLACEHOLDER ("PATCH blocks answers once the transcript
    // entry flags land"); that placeholder is GONE: `patch_session_turn_block` (`api/turns.rs:1132`)
    // now performs the write through the two joins, the real write and its per-axis 409s are pinned
    // by `tests/http_sessions.rs:4915` (R18/R19), and no 5xx remains on this route.
    for body in [
        json!({ "included": false }),
        json!({ "reasoningIncluded": true }),
        json!({ "included": true, "reasoningIncluded": false }),
    ] {
        let reply = Reply::patch(&root, &block_path, body.clone()).await;

        assert_eq!(
            reply.status, 404,
            "`{body}` clears every guard and is refused by the segment lookup"
        );
        assert_eq!(
            reply.message(),
            format!("Unknown context block \"blk\" in turn \"{turn_id}\".")
        );
        assert!(!reply.headers.contains_key(&REVISION_HEADER));
    }
}

// ---------------------------------------------------------------------------
// 6 - `POST .../threads/{thread_id}/resume`: the guards this build can reach
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_resume_route_answers_its_identity_and_empty_lane_guards() {
    let root = Root::new("thread-resume");
    let session = create_session(&root.path).await;
    let resume_path = |thread: &str| {
        format!(
            "{}/sessions/{session}/threads/{thread}/resume",
            contract::HARNESS_API_ROOT
        )
    };

    // The SAME identity guards the cancel route uses, so `resume` cannot 404 a typo'd id
    // differently from `cancel` (`api.ts:2871-2873`).
    Reply::post(&root, &resume_path("nope"), Value::Null)
        .await
        .assert_refusal(
            404,
            &format!("Unknown thread \"nope\" in session \"{session}\"."),
        );

    Reply::post(
        &root,
        &format!(
            "{}/sessions/missing/threads/t/resume",
            contract::HARNESS_API_ROOT
        ),
        Value::Null,
    )
    .await
    .assert_refusal(404, "Unknown session \"missing\".");

    // A conversation with no durable input to replay: the store's own sentence
    // (`sessions.ts:8780-8786`). This is the lane a cascaded child stop leaves behind.
    let thread = create_thread(&root.path, &session).await;

    Reply::post(&root, &resume_path(&thread), Value::Null)
        .await
        .assert_refusal(
            409,
            &format!("Thread \"{thread}\" has no recorded turn to resume."),
        );

    // A conversation that is NOT halted is refused before its turn is even looked at
    // (`sessions.ts:8691-8760`). A fresh session's default conversation has run nothing, so the
    // status guard is what answers here once a turn exists - pinned end to end (the running-lane
    // sentence and the requeue's receipt both need a provider-answered turn, which
    // `tests/http_sessions.rs`'s scripted listener is the place for).
    let threads = Reply::get(
        &root,
        &format!("{}/sessions/{session}/threads", contract::HARNESS_API_ROOT),
    )
    .await;

    assert_eq!(threads.status, 200);

    // The route is a POST-only mount: a GET is the table's 405, with the resume verb named.
    Reply::get(&root, &resume_path(&thread))
        .await
        .assert_refusal(
            405,
            &format!(
                "Use POST for {}/sessions/{session}/threads/{thread}/resume.",
                contract::HARNESS_API_ROOT
            ),
        );
}
