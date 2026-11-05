//! Foundation tests for the `typecrate-harness` primitives (T1).
//!
//! Every expected value below is taken from the TypeScript source of record, not observed from
//! this Rust build: the constants from `src/shared/harness-api.ts`, the paging messages from
//! `src/server/api-paging.ts`, the runtime/credential rules from `src/server/config.ts` and
//! `src/server/api.ts`, the origin table from `src/shared/origin.ts` + `src/server/service.ts`,
//! and the hub semantics from `src/server/api-events.ts`.
//!
//! No network, no listener, no provider. Tests that touch the PROCESS environment take
//! `ENV_LOCK`, because `cargo test` runs them on threads that share one environment block.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::{Value, json};
use typecrate_harness::{
    config, contract, environment, error::ApiError, events, ids, origin, package_root, paging,
    version,
};

/// Serializes every test that mutates the process environment or the `.env` cache.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Guards the process environment for one test.
fn lock_environment() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 1. The npm/cargo version handshake.
#[test]
fn version_matches_npm_line() {
    assert_eq!(version(), "1.0.2");
    assert_eq!(typecrate_harness::CRATE_NAME, "typecrate_harness");
}

/// 2a. The boot document carries every contract key, with the contract's numbers.
#[test]
fn service_info_serializes_with_the_contract_shape() {
    let info = contract::ServiceInfo {
        api_root: contract::HARNESS_API_ROOT.to_string(),
        service: contract::SERVICE_NAME.to_string(),
        api_version: contract::API_VERSION.to_string(),
        build_version: Some(version().to_string()),
        server_time: 1_700_000_000_000,
        epoch: "m0abc-def123456789".to_string(),
        capabilities: contract::service_capabilities(),
        limits: contract::service_limits(),
        links: BTreeMap::from([
            ("self".to_string(), "/v1/harness".to_string()),
            ("health".to_string(), "/v1/harness/health".to_string()),
        ]),
    };

    let body = serde_json::to_value(&info).expect("ServiceInfo is serializable");

    // Exactly the nine members of `ServiceInfo` (`harness-api.ts:339`), and nothing else.
    let mut keys: Vec<&String> = body.as_object().expect("object").keys().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "api_root",
            "api_version",
            "build_version",
            "capabilities",
            "epoch",
            "limits",
            "links",
            "server_time",
            "service",
        ]
    );

    // `HARNESS_SERVICE_LIMITS` (`harness-api.ts:197-213`), value for value.
    assert_eq!(
        body["limits"],
        json!({
            "body_bytes": 1_000_000_u64,
            "submit_body_bytes": 16_777_216_u64,
            "input_chars": 200_000_u64,
            "pending_tasks": 100_u64,
            "thread_records": 256_u64,
            "attachments_per_turn": 4_u64,
            "attachment_bytes": 4_194_304_u64,
            "total_attachment_bytes": 8_388_608_u64,
            "result_references": 16_u64,
            "pending_actions": 4_u64,
            "steer_records": 64_u64,
            "created_tools": 32_u64,
            "tool_result_bytes": 65_536_u64,
            "page_limit_default": 25_u64,
            "page_limit_max": 100_u64,
        })
    );

    // `HARNESS_SERVICE_CAPABILITIES` (`harness-api.ts:216-226`).
    assert_eq!(
        body["capabilities"],
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

    assert_eq!(body["api_root"], "/v1/harness");
    // The root document's `service` is the published name, NOT this crate's own
    // (`api.ts:1307` sends `'noobscript-harness'`).
    assert_eq!(body["service"], "noobscript-harness");
    assert_eq!(contract::SERVICE_NAME, "noobscript-harness");
    assert_eq!(body["api_version"], "v1");
    assert_eq!(body["build_version"], "1.0.2");

    // The Rust constants and the advertised block can never disagree.
    let limits = contract::service_limits();
    assert_eq!(
        usize::try_from(limits.body_bytes).ok(),
        Some(contract::MAX_BODY_BYTES)
    );
    assert_eq!(
        usize::try_from(limits.submit_body_bytes).ok(),
        Some(contract::MAX_SUBMIT_BODY_BYTES)
    );
    assert_eq!(limits.page_limit_default, contract::PAGE_LIMIT_DEFAULT);
    assert_eq!(limits.page_limit_max, contract::PAGE_LIMIT_MAX);
    assert_eq!(
        limits.tool_result_bytes,
        contract::MODEL_TOOL_RESULT_BUDGET_BYTES as u64
    );
    assert_eq!(contract::HARNESS_WS_SUBPROTOCOL, "harness.v1");
    assert_eq!(contract::HARNESS_EPOCH_HEADER, "X-Harness-Epoch");
    assert_eq!(contract::HARNESS_REVISION_HEADER, "X-Harness-Revision");
}

/// 2b. The camelCase/snake_case split the contract pins (`harness-api.ts:5-10`).
#[test]
fn wire_spellings_match_the_typescript_contract() {
    // A domain payload keeps camelCase (`types/index.ts:116`).
    let message = contract::HarnessMessage::Assistant {
        content: "hi".to_string(),
        tool_calls: vec![contract::HarnessToolCall {
            id: "call_1".to_string(),
            name: "delegate".to_string(),
            arguments: "{\"agent\":\"loaded\"}".to_string(),
        }],
        reasoning: Some("thinking".to_string()),
    };
    assert_eq!(
        serde_json::to_value(&message).unwrap(),
        json!({
            "role": "assistant",
            "content": "hi",
            "toolCalls": [{ "id": "call_1", "name": "delegate", "arguments": "{\"agent\":\"loaded\"}" }],
            "reasoning": "thinking",
        })
    );

    // `attempt_commit` keeps the snake_case tag while its fields stay camelCase
    // (`types/index.ts:1975`).
    let event = contract::HarnessEvent::AttemptCommit {
        agent: "research".to_string(),
        attempt: 2,
        max_attempts: 11,
        thread: None,
    };
    assert_eq!(
        serde_json::to_value(&event).unwrap(),
        json!({ "type": "attempt_commit", "agent": "research", "attempt": 2, "maxAttempts": 11 })
    );

    // A tool result keeps `isError` and reports nothing for its absent members.
    let result = contract::HarnessEvent::ToolResult {
        agent: "research".to_string(),
        name: "shell".to_string(),
        content: "ok".to_string(),
        is_error: false,
        call_id: Some("call_1".to_string()),
        outcome: Some(contract::HarnessToolOutcome::Completed),
        ui: vec![],
        media: vec![],
        duration_ms: Some(12),
        pending: vec![],
        thread: None,
    };
    assert_eq!(
        serde_json::to_value(&result).unwrap(),
        json!({
            "type": "tool_result",
            "agent": "research",
            "name": "shell",
            "content": "ok",
            "isError": false,
            "callId": "call_1",
            "outcome": "completed",
            "durationMs": 12,
        })
    );

    // A REST read model is snake_case (`harness-api.ts:1575`), the display row is camelCase
    // (`harness-api.ts:1737`).
    assert_eq!(
        serde_json::to_value(contract::AttachmentDescriptor {
            id: "a1".to_string(),
            kind: contract::AttachmentKind::Image,
            marker: "[Image 1]".to_string(),
            name: "cat.png".to_string(),
            mime: "image/png".to_string(),
            size: 12,
            has_bytes: true,
        })
        .unwrap(),
        json!({
            "id": "a1", "kind": "image", "marker": "[Image 1]", "name": "cat.png",
            "mime": "image/png", "size": 12, "has_bytes": true,
        })
    );

    let summary = contract::TurnSummary {
        id: "t1".to_string(),
        index: 3,
        prompt_preview: "he".to_string(),
        prompt_chars: 2,
        status: contract::SessionTurnStatus::Done,
        state: contract::DisplayState::Done,
        thread_id: None,
        thread_label: None,
        started_at: None,
        updated_at: 1_700_000_000_001,
        usage: Some(contract::HarnessUsage {
            input: 10,
            output: 5,
            total: 15,
            cached: None,
        }),
        latest_input: None,
        cached_input: None,
        detail: None,
        section_count: 1,
        step_count: 0,
        active_agents: vec!["research".to_string()],
        attachments: None,
        references: None,
        steers: None,
        blocks: vec![],
    };
    let row = serde_json::to_value(&summary).unwrap();
    assert_eq!(row["promptPreview"], "he");
    assert_eq!(row["promptChars"], 2);
    assert_eq!(row["state"], "done");
    assert_eq!(row["activeAgents"], json!(["research"]));
    assert_eq!(row["threadId"], Value::Null, "absent members are omitted");
    assert_eq!(row.get("threadId"), None);
    assert_eq!(row["usage"]["total"], 15);
    assert_eq!(row.get("usage").unwrap().get("cached"), None);

    // The untagged `ToolCallResult` union (`harness-api.ts:730`).
    let failure = contract::ToolCallResult::Failure(contract::ToolCallFailure {
        ok: false,
        error: contract::HarnessErrorInfo {
            message: "boom".to_string(),
        },
        duration_ms: 3,
    });
    assert!(!failure.is_success());
    assert_eq!(failure.duration_ms(), 3);
    let parsed: contract::ToolCallResult = serde_json::from_value(
        json!({ "ok": false, "error": { "message": "boom" }, "duration_ms": 3 }),
    )
    .expect("a failure result decodes");
    assert_eq!(parsed, failure);

    let success: contract::ToolCallResult =
        serde_json::from_value(json!({ "ok": true, "result": { "n": 1 }, "duration_ms": 4 }))
            .expect("a success result decodes");
    assert!(success.is_success());
    assert_eq!(success.duration_ms(), 4);

    // An action request mints `allowCustom: true` (`types/index.ts:872`).
    let request = contract::HarnessActionRequest::new(
        "r1",
        "Which one?",
        "a",
        vec![
            contract::HarnessActionOption {
                id: "a".to_string(),
                label: "Alpha".to_string(),
                description: None,
            },
            contract::HarnessActionOption {
                id: "b".to_string(),
                label: "Beta".to_string(),
                description: Some("second".to_string()),
            },
        ],
    );
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({
            "requestId": "r1",
            "question": "Which one?",
            "recommended": "a",
            "allowCustom": true,
            "options": [
                { "id": "a", "label": "Alpha" },
                { "id": "b", "label": "Beta", "description": "second" }
            ],
        })
    );

    // Request-side answer spelling is snake_case, the recorded one is camelCase
    // (`harness-api.ts:1315` vs `types/index.ts:877-879`).
    assert_eq!(
        serde_json::to_value(contract::AnswerActionRequest {
            choice: contract::ActionChoiceInput::Option {
                option_id: "a".to_string(),
            },
        })
        .unwrap(),
        json!({ "choice": { "kind": "option", "option_id": "a" } })
    );

    // The RECORDED domain choice is `optionId`, because it is embedded verbatim in an
    // `action_answer` event that a TS reader replays (`types/index.ts:877-879`).
    let recorded = contract::HarnessActionChoice::Option {
        option_id: "x".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&recorded).unwrap(),
        json!({ "kind": "option", "optionId": "x" })
    );
    assert_eq!(
        serde_json::from_value::<contract::HarnessActionChoice>(json!({
            "kind": "option",
            "optionId": "x",
        }))
        .expect("the camelCase member round-trips"),
        recorded
    );

    // The wire NEVER accepts the snake_case spelling on this type: the required camelCase
    // member is missing, so the whole answer is refused rather than silently emptied.
    let refused = serde_json::from_value::<contract::HarnessActionChoice>(json!({
        "kind": "option",
        "option_id": "x",
    }))
    .expect_err("`option_id` is not the recorded spelling");
    assert!(refused.to_string().contains("optionId"), "{refused}");

    // A free-text answer keeps its single `text` member on both spellings (`:879`).
    assert_eq!(
        serde_json::to_value(contract::HarnessActionChoice::Custom {
            text: "free".to_string(),
        })
        .unwrap(),
        json!({ "kind": "custom", "text": "free" })
    );

    // The recorded envelope carries the choice as-is (`types/index.ts:883-888`).
    assert_eq!(
        serde_json::to_value(contract::HarnessActionAnswer {
            request_id: "r1".to_string(),
            choice: recorded.clone(),
            source: contract::ActionAnswerSource::Owner,
            branch_id: None,
        })
        .unwrap(),
        json!({
            "requestId": "r1",
            "choice": { "kind": "option", "optionId": "x" },
            "source": "owner",
        })
    );

    // The health document (`harness-api.ts:286`).
    assert_eq!(
        serde_json::to_value(contract::HealthResponse {
            ok: true,
            sessions: 2,
            epoch: "e1".to_string(),
        })
        .unwrap(),
        json!({ "ok": true, "sessions": 2, "epoch": "e1" })
    );

    // The error envelope is always `{error:{message}}` (`harness-api.ts:276`).
    assert_eq!(
        serde_json::to_value(contract::ErrorBody::from_error(&ApiError::not_found(
            "gone"
        )))
        .unwrap(),
        json!({ "error": { "message": "gone" } })
    );
}

/// 3a. Cursors round-trip inside one epoch and refuse every other one.
#[test]
fn paging_cursors_are_epoch_bound_and_opaque() {
    let epoch = "m0abc-def123456789";

    // The cursor is opaque base64url, and `.` is inside it only after decoding.
    let cursor = paging::encode_cursor(epoch, 42);
    assert!(!cursor.contains('.'));
    assert!(!cursor.contains('='), "base64url is emitted unpadded");
    assert_eq!(paging::decode_cursor(epoch, Some(&cursor)).unwrap(), 42);
    assert_eq!(paging::decode_cursor(epoch, Some("")).unwrap(), 0);
    assert_eq!(paging::decode_cursor(epoch, None).unwrap(), 0);

    // A cursor from another epoch (a restart, or another server) is a 400 with the exact text.
    let stale = paging::decode_cursor("other-epoch", Some(&cursor)).unwrap_err();
    assert_eq!(stale.status, 400);
    assert_eq!(
        stale.message,
        "Query parameter \"cursor\" is stale or foreign; restart from the first page."
    );

    // A token over the spec's 256-character cap was not issued here.
    let long = "a".repeat(257);
    assert_eq!(
        paging::decode_cursor(epoch, Some(&long))
            .unwrap_err()
            .message,
        "Query parameter \"cursor\" is not a cursor this server issued."
    );

    // And neither was something that is not base64url at all.
    assert_eq!(
        paging::decode_cursor(epoch, Some("!!!not-base64!!!"))
            .unwrap_err()
            .message,
        "Query parameter \"cursor\" is not a cursor this server issued."
    );

    // Decoded but not our encoding (no epoch separator) is stale/foreign. 9 ASCII bytes encode
    // to exactly 12 base64url characters, so the token itself is well-formed on purpose.
    let foreign = base64url("epochless");
    assert_eq!(foreign.len(), 12);
    assert_eq!(
        paging::decode_cursor(epoch, Some(&foreign))
            .unwrap_err()
            .message,
        "Query parameter \"cursor\" is stale or foreign; restart from the first page."
    );
}

/// 3b. `limit` clamps to the contract bounds, with one exact message.
#[test]
fn paging_limit_names_the_parameter_on_every_refusal() {
    const MESSAGE: &str = "Query parameter \"limit\" must be an integer between 1 and 100.";

    assert_eq!(paging::resolve_limit(None).unwrap(), 25);
    assert_eq!(paging::resolve_limit(Some("1")).unwrap(), 1);
    assert_eq!(paging::resolve_limit(Some("100")).unwrap(), 100);
    assert_eq!(paging::resolve_limit(Some(" 7 ")).unwrap(), 7);

    // `?limit=` is ABSENT, not zero: the extractor is TS `paramOf` (`api.ts:482-488`), which
    // trims and maps an empty value to `undefined`, and `resolveLimit(undefined)` is the
    // contract default (`api-paging.ts:95`).
    assert_eq!(paging::resolve_limit(Some("")).unwrap(), 25);
    assert_eq!(paging::resolve_limit(Some("   ")).unwrap(), 25);

    for refused in ["0", "101", "-1", "1.5", "abc", "1e3", "255"] {
        let error = paging::resolve_limit(Some(refused)).unwrap_err();
        assert_eq!(error.status, 400, "{refused}");
        assert_eq!(error.message, MESSAGE, "{refused}");
    }
}

/// 3c. A page body echoes the EFFECTIVE page size and a null cursor at the end.
#[test]
fn paging_body_echoes_the_effective_limit() {
    let epoch = "epoch-000000000000";

    let full = paging::page_body(epoch, vec![1_u64, 2], Some(2), 7, 1_700_000_000_000);
    assert_eq!(full["limit"], 2, "limit is what the page carries");
    assert_eq!(
        full["next_cursor"],
        paging::encode_cursor(epoch, 2),
        "an exactly-full page always continues"
    );
    assert_eq!(full["revision"], 7);
    assert_eq!(full["observed_at"], 1_700_000_000_000_u64);
    assert_eq!(full["items"], json!([1, 2]));

    let last = paging::page_body(epoch, vec![1_u64, 2, 3], None, 9, 1_700_000_000_002);
    assert_eq!(last["limit"], 3);
    assert!(
        last["next_cursor"].is_null(),
        "the end of a collection prints null"
    );

    // The UNKNOWN-total rule (`api-paging.ts:136`, `total = -1`): exactly-full always
    // advertises a next page, "possibly to an empty page".
    let all: Vec<u64> = (0..5).collect();
    assert_eq!(
        paging::slice_page(all.clone(), 0, 2, None),
        paging::PageSlice {
            items: vec![0, 1],
            next: Some(2)
        }
    );
    assert_eq!(
        paging::slice_page(all.clone(), 3, 2, None),
        paging::PageSlice {
            items: vec![3, 4],
            next: Some(5)
        },
        "without a total even the last full page keeps a cursor"
    );
    assert_eq!(
        paging::slice_page(all.clone(), 4, 2, None),
        paging::PageSlice {
            items: vec![4],
            next: None
        }
    );
    assert_eq!(
        paging::slice_page(all.clone(), 9, 2, None),
        paging::PageSlice {
            items: vec![],
            next: None
        },
        "a cursor past the end is an empty final page, never an error"
    );
}

/// 3d. A KNOWN total ends the collection on the page that reaches it
/// (`api-paging.ts:130-138`: `hasMore = items.length === limit && (total < 0 || served < known)`).
/// Every real canonical route passes the total, so an exactly-full FINAL page must answer
/// `next_cursor: null` rather than a cursor to an empty page.
#[test]
fn paging_known_total_ends_an_exactly_full_final_page() {
    let all: Vec<u64> = (0..5).collect();

    // Full and NOT final: the cursor is still issued.
    assert_eq!(
        paging::slice_page(all.clone(), 0, 2, Some(5)),
        paging::PageSlice {
            items: vec![0, 1],
            next: Some(2)
        }
    );

    // Full AND final: `served == total`, so the collection ends here (`next_cursor: null`).
    assert_eq!(
        paging::slice_page(all.clone(), 3, 2, Some(5)),
        paging::PageSlice {
            items: vec![3, 4],
            next: None
        },
        "a full final page must not advertise an empty next page"
    );

    // A short page ends the collection whatever the total says.
    assert_eq!(
        paging::slice_page(all.clone(), 4, 2, Some(5)),
        paging::PageSlice {
            items: vec![4],
            next: None
        }
    );

    // The declared total is the bound, NOT `entries.len()`: a caller that materializes only
    // part of the collection (a windowed store read) but KNOWS the real total must still
    // continue, because TS compares `served` against `total` (`:130`, `:136`).
    assert_eq!(
        paging::slice_page(vec![1_u64, 2], 0, 2, Some(9)),
        paging::PageSlice {
            items: vec![1, 2],
            next: Some(2)
        },
        "known total outranks the materialized length"
    );

    // One page deep inside a 64-entry collection: a full middle page continues, the short
    // last page ends.
    let many: Vec<u64> = (0..64).collect();
    assert_eq!(
        paging::slice_page(many.clone(), 25, 25, Some(64)),
        paging::PageSlice {
            items: (25..50).collect(),
            next: Some(50)
        }
    );
    assert_eq!(
        paging::slice_page(many, 50, 25, Some(64)),
        paging::PageSlice {
            items: (50..64).collect(),
            next: None
        },
        "14 rows is the last page of a 64-entry collection"
    );

    // An empty collection with a known total of 0 answers a full-size page of nothing.
    assert_eq!(
        paging::slice_page(Vec::<u64>::new(), 0, 25, Some(0)),
        paging::PageSlice {
            items: vec![],
            next: None
        }
    );
    assert_eq!(
        paging::slice_page(all, 9, 2, Some(5)),
        paging::PageSlice {
            items: vec![],
            next: None
        }
    );
}

/// 3e. A known total is what makes the LAST page of a real route print `next_cursor: null`
/// end to end (`api-paging.ts:142-153`), including the cursor's own ordinal.
#[test]
fn paging_body_of_a_known_total_page_is_exact() {
    let epoch = "epoch-000000000000";
    let rows: Vec<u64> = (0..5).collect();

    // Final page (cursor 3, limit 2, total 5) -> served 5 == total, so no cursor.
    let final_page = paging::slice_page(rows.clone(), 3, 2, Some(5));
    let body = paging::page_slice_body(epoch, final_page, 4, 1_700_000_000_004);
    assert_eq!(body["limit"], 2);
    assert!(
        body["next_cursor"].is_null(),
        "a full final page of a known-total collection ends the walk"
    );
    assert_eq!(body["items"], json!([3, 4]));

    // Non-final page (cursor 0, limit 2, total 5) -> the cursor addresses ordinal 2.
    let first_page = paging::slice_page(rows, 0, 2, Some(5));
    let body = paging::page_slice_body(epoch, first_page, 4, 1_700_000_000_004);
    assert_eq!(body["next_cursor"], paging::encode_cursor(epoch, 2));
    assert_eq!(
        paging::decode_cursor(epoch, body["next_cursor"].as_str()).unwrap(),
        2
    );
}

/// 4a. Every default is the TS default.
#[test]
fn config_defaults_match_the_typescript_loader() {
    let loaded = config::load_server_config(&BTreeMap::new()).expect("defaults are valid");

    assert_eq!(
        loaded.base_url,
        "http://localhost:5500/providers/private/v1"
    );
    assert_eq!(loaded.model, "merge/ds4-flash");
    assert_eq!(loaded.api_key, "");
    assert_eq!(loaded.max_retries, 10);
    assert_eq!(loaded.retry_delay_ms, 1_000);
    assert_eq!(loaded.request_timeout_ms, 0);
    assert_eq!(loaded.thread_execution, "inline");
    assert_eq!(loaded.io_execution, "inline");
    assert_eq!(loaded.worker_idle_ms, 120_000);
    assert_eq!(loaded.max_lane_workers, 16);
    assert_eq!(loaded.worker_handshake_ms, 60_000);
    assert_eq!(loaded.worker_entry, None);
    assert_eq!(loaded.io_worker_entry, None);

    // The key-free advertisement, and the read-only runtime block (`api-reads.ts:149-164`).
    let defaults = config::provider_defaults(&loaded.settings());
    assert_eq!(
        defaults,
        contract::ProviderDefaults {
            base_url: "http://localhost:5500/providers/private/v1".to_string(),
            model: "merge/ds4-flash".to_string(),
            has_api_key: false,
        }
    );
    assert_eq!(
        config::runtime_settings(&loaded),
        contract::RuntimeSettings {
            thread_execution: "inline".to_string(),
            worker_idle_ms: 120_000,
            max_lane_workers: 16,
            worker_handshake_ms: 60_000,
            worker_entry_configured: None,
        }
    );
}

/// 4b. Environment variables override, and a blank one falls back like `||` does in TS.
#[test]
fn config_reads_environment_variables_with_typescript_precedence() {
    let env = BTreeMap::from([
        (
            "HARNESS_BASE_URL".to_string(),
            "  http://env.example/v1  ".to_string(),
        ),
        ("HARNESS_MODEL".to_string(), "env/model".to_string()),
        ("HARNESS_API_KEY".to_string(), "file-key".to_string()),
        ("OPENAI_API_KEY".to_string(), "ignored".to_string()),
        ("HARNESS_THREAD_EXECUTION".to_string(), "WORKER".to_string()),
        ("HARNESS_IO_EXECUTION".to_string(), "worker".to_string()),
        ("HARNESS_WORKER_IDLE_MS".to_string(), "0".to_string()),
        ("HARNESS_MAX_LANE_WORKERS".to_string(), "64".to_string()),
        (
            "HARNESS_WORKER_HANDSHAKE_MS".to_string(),
            "1000".to_string(),
        ),
        (
            "HARNESS_WORKER_ENTRY".to_string(),
            " dist/worker.mjs ".to_string(),
        ),
        ("HARNESS_IO_WORKER_ENTRY".to_string(), "".to_string()),
        // A whitespace-only provider value is "not configured" (`config.ts:368` `||`).
        (
            "HARNESS_REQUEST_TIMEOUT_MS".to_string(),
            "30000".to_string(),
        ),
    ]);

    let loaded = config::load_server_config(&env).expect("a valid environment loads");

    assert_eq!(loaded.base_url, "http://env.example/v1");
    assert_eq!(loaded.model, "env/model");
    // `??` precedence (`config.ts:369`): HARNESS_API_KEY wins even when OPENAI_API_KEY is set.
    assert_eq!(loaded.api_key, "file-key");
    assert_eq!(loaded.thread_execution, "worker");
    assert_eq!(loaded.io_execution, "worker");
    assert_eq!(loaded.worker_idle_ms, 0);
    assert_eq!(loaded.max_lane_workers, 64);
    assert_eq!(loaded.worker_handshake_ms, 1_000);
    assert_eq!(loaded.worker_entry.as_deref(), Some("dist/worker.mjs"));
    // A blank override means "not set", exactly like an absent variable (`config.ts:276-283`).
    assert_eq!(loaded.io_worker_entry, None);
    assert_eq!(loaded.request_timeout_ms, 30_000);

    // An explicitly EMPTY key still wins over `OPENAI_API_KEY` (`??`, not `||`).
    let empty_key = BTreeMap::from([
        ("HARNESS_API_KEY".to_string(), String::new()),
        ("OPENAI_API_KEY".to_string(), "fallback".to_string()),
    ]);
    assert_eq!(
        config::load_server_config(&empty_key).unwrap().api_key,
        "",
        "an empty HARNESS_API_KEY suppresses the fallback"
    );

    // With no HARNESS_API_KEY the fallback is used.
    let only_openai = BTreeMap::from([("OPENAI_API_KEY".to_string(), "fallback".to_string())]);
    assert_eq!(
        config::load_server_config(&only_openai).unwrap().api_key,
        "fallback"
    );

    // An explicit worker entry that is blank after trimming is reported as configured=false
    // only when absent; `runtime_settings` states presence, never the path.
    let with_entry = BTreeMap::from([(
        "HARNESS_WORKER_ENTRY".to_string(),
        "dist/lane.mjs".to_string(),
    )]);
    let loaded = config::load_server_config(&with_entry).unwrap();
    assert_eq!(
        config::runtime_settings(&loaded).worker_entry_configured,
        Some(true)
    );
}

/// 4c. A bad provider or runtime value is one 400 that lists every problem.
#[test]
fn config_refusals_are_aggregated_and_prefixed() {
    const PREFIX: &str = "Invalid runtime settings: ";

    // A scheme-less value is not an absolute URL at all, and never reaches a provider request.
    let relative = BTreeMap::from([("HARNESS_BASE_URL".to_string(), "provider.test".to_string())]);
    let error = config::load_server_config(&relative).unwrap_err();
    assert_eq!(error.status, 400);
    assert!(error.message.starts_with(PREFIX), "{error}");
    assert!(
        error
            .message
            .contains("\"provider.test\" is not an absolute URL."),
        "{error}"
    );

    // `localhost:5500/v1` looks like a URL but has no authority (`//`), and a scheme without an
    // authority can never be a provider endpoint, so this parser refuses it as not absolute.
    // `new URL` would accept it and then fail the http(s) protocol check, so both runtimes
    // refuse it; only the wording differs - see knowledge.md "Deviations".
    let odd_scheme = BTreeMap::from([(
        "HARNESS_BASE_URL".to_string(),
        "localhost:5500/v1".to_string(),
    )]);
    assert!(
        config::load_server_config(&odd_scheme)
            .unwrap_err()
            .message
            .contains("\"localhost:5500/v1\" is not an absolute URL."),
        "odd scheme"
    );

    // A non-web scheme is refused with the redacted display form.
    let ftp = BTreeMap::from([(
        "HARNESS_BASE_URL".to_string(),
        "ftp://provider.test/v1".to_string(),
    )]);
    assert!(
        config::load_server_config(&ftp)
            .unwrap_err()
            .message
            .contains("\"ftp://provider.test\" must use http or https."),
        "ftp"
    );

    // A runtime typo names the variable and the accepted range, and never coerces.
    let typo = BTreeMap::from([("HARNESS_WORKER_IDLE_MS".to_string(), "12s".to_string())]);
    let error = config::load_server_config(&typo).unwrap_err();
    assert_eq!(
        error.message,
        format!("{PREFIX}HARNESS_WORKER_IDLE_MS must be an integer between 0 and 3600000.")
    );

    // Out of range is the same message as non-numeric.
    let high = BTreeMap::from([("HARNESS_MAX_LANE_WORKERS".to_string(), "65".to_string())]);
    assert_eq!(
        config::load_server_config(&high).unwrap_err().message,
        format!("{PREFIX}HARNESS_MAX_LANE_WORKERS must be an integer between 1 and 64.")
    );
    let low = BTreeMap::from([("HARNESS_WORKER_HANDSHAKE_MS".to_string(), "999".to_string())]);
    assert_eq!(
        config::load_server_config(&low).unwrap_err().message,
        format!("{PREFIX}HARNESS_WORKER_HANDSHAKE_MS must be an integer between 1000 and 600000.")
    );

    // A mode typo names the wrong value and both accepted spellings (`config.ts:239-243`).
    let mode = BTreeMap::from([("HARNESS_IO_EXECUTION".to_string(), "workr".to_string())]);
    assert_eq!(
        config::load_server_config(&mode).unwrap_err().message,
        format!(
            "{PREFIX}HARNESS_IO_EXECUTION must be \"inline\" or \"worker\"; received \"workr\"."
        )
    );

    // Length caps are the shared ones from `config.ts:140-141`.
    let long_url = BTreeMap::from([(
        "HARNESS_BASE_URL".to_string(),
        format!("http://provider.test/{}", "x".repeat(2100)),
    )]);
    assert!(
        config::load_server_config(&long_url)
            .unwrap_err()
            .message
            .contains("A provider base URL cannot exceed 2048 characters."),
        "url cap"
    );

    let long_model = BTreeMap::from([("HARNESS_MODEL".to_string(), "m".repeat(257))]);
    assert_eq!(
        config::load_server_config(&long_model).unwrap_err().message,
        format!("{PREFIX}A model name cannot exceed 256 characters.")
    );

    // THREE bad variables are reported in one pass, so an operator fixes them in one edit.
    let many = BTreeMap::from([
        ("HARNESS_BASE_URL".to_string(), "provider.test".to_string()),
        (
            "HARNESS_THREAD_EXECUTION".to_string(),
            "threads".to_string(),
        ),
        ("HARNESS_MAX_LANE_WORKERS".to_string(), "0".to_string()),
    ]);
    let message = config::load_server_config(&many).unwrap_err().message;
    assert_eq!(message, {
        let expected = [
            "\"provider.test\" is not an absolute URL.",
            "HARNESS_THREAD_EXECUTION must be \"inline\" or \"worker\"; received \"threads\".",
            "HARNESS_MAX_LANE_WORKERS must be an integer between 1 and 64.",
        ];
        format!("{PREFIX}{}", expected.join("; "))
    });
}

/// 4d. The credential rule: a key belongs to the provider it was issued for.
#[test]
fn config_scopes_the_credential_to_its_provider() {
    let current = config::HarnessRuntimeSettings {
        base_url: "http://provider.test/v1".to_string(),
        api_key: "secret".to_string(),
        model: "merge/ds4-flash".to_string(),
    };

    // Same provider, spelled differently: the key stays. `new URL` lowercases the host and
    // drops a default port, which is why these compare equal (`config.ts:172-175`).
    let renamed = config::apply_settings_patch(
        &current,
        &json!({ "base_url": "HTTP://Provider.Test:80/v1/" }),
    )
    .expect("an equivalent URL is the same provider");
    assert_eq!(renamed.api_key, "secret");
    assert_eq!(renamed.base_url, "HTTP://Provider.Test:80/v1/");

    // A query string is NOT the same endpoint, so it must not receive the configured key.
    let redirected = config::apply_settings_patch(
        &current,
        &json!({ "base_url": "http://provider.test/v1?redirect=elsewhere" }),
    )
    .expect("a query-different URL is still valid");
    assert_eq!(redirected.api_key, "", "a foreign endpoint gets no key");

    // A different provider clears the key, unless the same patch supplies a new one.
    let moved =
        config::apply_settings_patch(&current, &json!({ "base_url": "http://other.test/v1" }))
            .unwrap();
    assert_eq!(moved.api_key, "");

    let moved_with_key = config::apply_settings_patch(
        &current,
        &json!({ "base_url": "http://other.test/v1", "api_key": "fresh" }),
    )
    .unwrap();
    assert_eq!(moved_with_key.api_key, "fresh");

    // An explicit empty string is an explicit "no key".
    assert_eq!(
        config::apply_settings_patch(&current, &json!({ "api_key": "" }))
            .unwrap()
            .api_key,
        ""
    );

    // Model-only and null writes keep the credential.
    assert_eq!(
        config::apply_settings_patch(&current, &json!({ "model": "other/model" }))
            .unwrap()
            .api_key,
        "secret"
    );
    assert_eq!(
        config::apply_settings_patch(&current, &json!({ "model": Value::Null }))
            .unwrap()
            .model,
        config::DEFAULT_MODEL
    );

    // Body-level refusals, with the exact TS wording.
    assert_eq!(
        config::apply_settings_patch(&current, &json!({ "provider": "x" }))
            .unwrap_err()
            .message,
        "Unknown field \"provider\". Allowed fields: base_url, model, api_key."
    );
    assert_eq!(
        config::apply_settings_patch(&current, &json!({}))
            .unwrap_err()
            .message,
        "A config patch must change at least one setting."
    );
    assert_eq!(
        config::apply_settings_patch(&current, &json!({ "model": 7 }))
            .unwrap_err()
            .message,
        "\"settings.model\" must be a string or null."
    );
    assert_eq!(
        config::apply_settings_patch(&current, &json!({ "base_url": "" }))
            .unwrap_err()
            .message,
        "A provider base URL cannot be empty."
    );
    assert_eq!(
        config::apply_settings_patch(&current, &json!({ "thread_execution": "worker" }))
            .unwrap_err()
            .message,
        "Unknown field \"thread_execution\". Allowed fields: base_url, model, api_key.",
        "a deployment-only knob stays a 400 that names it"
    );

    // `isSameProvider` (`config.ts:191-196`): an empty URL never matches.
    assert!(config::is_same_provider(
        "http://a.test/v1/",
        "http://a.test/v1"
    ));
    assert!(!config::is_same_provider("", "http://a.test/v1"));
    assert!(!config::is_same_provider(
        "http://a.test/v1",
        "http://a.test/v2"
    ));

    // `mergeSettings` (`config.ts:408-449`): an omitted key on a foreign endpoint is "no key".
    let merged = config::merge_settings(
        &current,
        &contract::RunnerSettingsInput {
            base_url: Some("http://other.test/v1".to_string()),
            api_key: None,
            model: Some("  ".to_string()),
        },
    )
    .unwrap();
    assert_eq!(merged.api_key, "");
    assert_eq!(merged.model, "merge/ds4-flash", "a blank model falls back");

    let merged_same = config::merge_settings(
        &current,
        &contract::RunnerSettingsInput {
            base_url: Some("http://provider.test/v1/".to_string()),
            api_key: None,
            model: None,
        },
    )
    .unwrap();
    assert_eq!(merged_same.api_key, "secret");

    // The shared caps are enforced by the merge as well (`config.ts:424-430`).
    assert_eq!(
        config::merge_settings(
            &current,
            &contract::RunnerSettingsInput {
                base_url: Some(format!("http://a.test/{}", "y".repeat(2100))),
                api_key: None,
                model: None,
            },
        )
        .unwrap_err()
        .message,
        "A provider base URL cannot exceed 2048 characters."
    );
}

/// 5. A `.env` file fills gaps and never overrides an exported value.
#[test]
fn environment_file_loads_without_overriding_the_process() {
    let _guard = lock_environment();

    // A variable the process ALWAYS has, so "an existing value wins" is proved WITHOUT mutating
    // the environment block: writing one is `unsafe` in edition 2024, and this crate forbids
    // unsafe. The exact spelling from the process snapshot is reused, which matters on Windows,
    // where the same variable is reported as `Path` while a `.env` file would say `PATH`.
    let process = environment::process_environment();
    let (existing_key, existing_value) = process
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("PATH"))
        .expect("the test process always has a PATH variable");

    let fixture = environment_fixture();
    let fixture_file = fixture.join(".env");
    let _clean = FixtureGuard::new(
        &fixture_file,
        &format!(
            "# fixture for tests/foundation.rs\nTYPECRATE_HARNESS_FILE_ONLY=file-value\n{existing_key}=file-value-must-not-override\n"
        ),
    );

    environment::set_harness_environment_path(Some(fixture_file.clone()));

    let status = environment::load_harness_environment();
    assert!(status.loaded, "the fixture exists: {:?}", status.error);
    assert_eq!(status.error, None);
    assert_eq!(status.path, fixture_file);

    let runtime = environment::harness_runtime_env();

    // A key only the file supplies arrives with the file's value...
    assert_eq!(
        runtime
            .get("TYPECRATE_HARNESS_FILE_ONLY")
            .map(String::as_str),
        Some("file-value")
    );
    // ...and a key the process already has keeps the PROCESS value (`environment.ts:23-27`):
    // dotenv's `override: false`, including an explicitly empty exported value.
    assert_eq!(
        runtime.get(existing_key.as_str()).map(String::as_str),
        Some(existing_value.as_str())
    );
    assert_eq!(
        std::env::var(existing_key.as_str()).unwrap(),
        *existing_value
    );
    // The status struct carries NO parsed values, so it can never log a secret by accident
    // (`environment.ts:52-57`).
    assert!(
        !format!("{status:?}").contains("file-value"),
        "the debug form of the result must not hold values"
    );

    // A second call is served from the cache and reports the same result.
    let again = environment::load_harness_environment();
    assert_eq!(again, status);

    // A missing file is recorded, never fatal.
    environment::set_harness_environment_path(Some(fixture.join(".env.absent")));
    let missing = environment::load_harness_environment();
    assert!(!missing.loaded);
    assert!(missing.error.is_some());
    assert!(
        environment::harness_runtime_env().contains_key(existing_key.as_str()),
        "a missing file must not empty the merged environment"
    );

    environment::set_harness_environment_path(None);
    environment::clear_harness_environment_cache();

    // The default path is the package-root `.env`, and the dump root is the shared
    // `temporary/dump` sibling of the repo root (`package-root.ts`, `service/server.ts:43`).
    assert_eq!(
        package_root::package_path(&[".env"]),
        package_root::package_root().join(".env")
    );
    assert_eq!(
        package_root::default_database_root(),
        repo_parent().join("temporary").join("dump")
    );
}

/// 6a. `bump` mints revisions, host kinds collapse, and the epoch is process-wide.
#[test]
fn event_hub_mints_revisions_and_collapses_host_kinds() {
    let hub = events::EventHub::new();

    // An untracked resource reads 0 = "never changed" (`api-events.ts:139`).
    assert_eq!(hub.revision("session", Some("s1")), 0);
    assert_eq!(hub.bump("session", Some("s1")), 1);
    assert_eq!(hub.bump("session", Some("s1")), 2);
    assert_eq!(hub.revision("session", Some("s1")), 2);
    assert_eq!(hub.revision("session", Some("other")), 0);

    // A host-wide kind ignores the id: `''`, an omitted id and a supplied id are one counter
    // (`api-events.ts:99-105`, `:265-267`).
    assert_eq!(hub.bump("harnesses", Some("ignored")), 1);
    assert_eq!(hub.revision("harnesses", None), 1);
    assert_eq!(hub.revision("harnesses", Some("")), 1);
    assert_eq!(hub.revision("harnesses", Some("anything")), 1);
    assert_eq!(events::EventHub::resource_key("config", None), "config:");
    assert_eq!(
        events::EventHub::resource_key("turn", Some("s:t")),
        "turn:s:t"
    );

    // `publish` REPORTS the counters the publisher already bumped and never advances them
    // (`api-events.ts:299-316`): the write owns `bumpRevision`, and publishing only states where
    // each counter now stands, so a bump-then-publish moves a counter exactly ONCE.
    let event = hub.publish(
        "turn.upsert",
        &[
            events::ResourceRef::new("turns", Some("s1")),
            events::ResourceRef::new("session", Some("s1")),
        ],
        Some("s1"),
        None,
        Some("t1"),
        json!({ "turn_id": "t1" }),
    );

    assert_eq!(event.epoch, hub.epoch());
    assert_eq!(event.kind, "turn.upsert");
    assert_eq!(event.session_id.as_deref(), Some("s1"));
    assert_eq!(event.turn_id.as_deref(), Some("t1"));
    assert!(
        event.ops.is_empty(),
        "a plain publish carries no display ops"
    );
    assert_eq!(event.revisions.len(), 2);
    assert_eq!(event.revisions[0].resource, "turns");
    assert_eq!(event.revisions[0].id.as_deref(), Some("s1"));
    // `turns` was never bumped, so the honest report is 0 - publishing must not mint a revision.
    assert_eq!(event.revisions[0].revision, 0);
    assert_eq!(
        event.revisions[1].revision, 2,
        "the session counter was already at 2 from the bumps above"
    );
    assert_eq!(
        hub.revision("turns", Some("s1")),
        0,
        "publish advanced nothing"
    );
    assert_eq!(hub.revision("session", Some("s1")), 2, "nor did it");

    // The epoch is `<base36 millis>-<12 lowercase base36 chars>` (`api-events.ts:76`, `:83`);
    // two hubs in one process share it, which is what keeps REST and the stream consistent.
    let epoch = hub.epoch().to_string();
    let (millis, suffix) = epoch
        .split_once('-')
        .unwrap_or_else(|| panic!("epoch shape: {epoch}"));
    assert!(
        millis
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
        "base36 prefix: {millis}"
    );
    assert_eq!(suffix.len(), 12);
    assert!(
        suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase()),
        "base36 suffix: {suffix}"
    );
    assert_eq!(events::EventHub::new().epoch(), epoch);

    // `session_resources` keeps the session id on every kind (`api-events.ts:371-374`).
    let references = events::session_resources("s1", &["session", "turns", "actions"]);
    assert_eq!(references.len(), 3);
    assert!(
        references
            .iter()
            .all(|reference| reference.id.as_deref() == Some("s1"))
    );
    assert_eq!(references[0].resource, "session");

    // Every reference names its instance, and the WIRE always carries `id` (`api-events.ts:93-96`
    // declares `id: string | null` on a closed object, so an omitted key is a contract break).
    assert_eq!(
        serde_json::to_value(&references[0]).unwrap(),
        json!({ "resource": "session", "id": "s1" })
    );
    assert_eq!(
        serde_json::to_value(events::ResourceRef::host("config")).unwrap(),
        json!({ "resource": "config", "id": null })
    );
    assert_eq!(
        serde_json::to_value(events::ResourceRef::new("config", Some("s9")).normalized()).unwrap(),
        json!({ "resource": "config", "id": null }),
        "a host-wide kind collapses its id to null on the wire as well as in the key"
    );
}

/// 6a-2. `publish` is REPORT-ONLY: the publisher owns its counters, so publishing never moves
/// one and two publishes of one bumped resource report the SAME revision (`api-events.ts:302-316`).
#[test]
fn event_hub_publish_reports_counters_the_publisher_owns() {
    let hub = events::EventHub::new();
    let resources = [events::ResourceRef::new("session", Some("s"))];

    assert_eq!(hub.revision("session", Some("s")), 0);

    // Two writes, two bumps: the counter now stands at 2 (`api-events.ts:271-278`).
    assert_eq!(hub.bump("session", Some("s")), 1);
    assert_eq!(hub.bump("session", Some("s")), 2);

    let first = hub.publish(
        "session.updated",
        &resources,
        Some("s"),
        None,
        None,
        json!({ "id": "s" }),
    );
    assert_eq!(first.revisions.len(), 1);
    assert_eq!(
        first.revisions[0].revision, 2,
        "the report is the bumped value"
    );
    assert_eq!(
        hub.revision("session", Some("s")),
        2,
        "publish did not advance the counter it reports"
    );

    // A second publish of the same change reports the SAME number, which is exactly what keeps
    // `PageMeta.revision` and a `subscription_state` frame in agreement.
    let second = hub.publish(
        "session.updated",
        &resources,
        Some("s"),
        None,
        None,
        json!({ "id": "s" }),
    );
    assert_eq!(
        second.revisions, first.revisions,
        "no drift between publishes"
    );
    assert_eq!(second.revisions[0].revision, 2);
    assert_eq!(hub.revision("session", Some("s")), 2);

    // Only a `bump` moves the counter, and the route owns that call.
    assert_eq!(hub.bump("session", Some("s")), 3);
    let third = hub.publish(
        "session.updated",
        &resources,
        Some("s"),
        None,
        None,
        json!({ "id": "s" }),
    );
    assert_eq!(third.revisions[0].revision, 3);
    assert_eq!(hub.revision("session", Some("s")), 3);

    // `revisions(..)` for `welcome`/`subscription_state` reports without advancing too
    // (`api-events.ts:287-297`).
    let records = hub.revisions(&resources);
    assert_eq!(
        records,
        vec![events::RevisionRecord {
            resource: "session".to_string(),
            id: Some("s".to_string()),
            revision: 3,
            epoch: hub.epoch().to_string(),
        }]
    );
    assert_eq!(hub.revision("session", Some("s")), 3);

    // A host-wide kind reports `id: null`, always present on the wire (`api-events.ts:95`).
    let host = [events::ResourceRef::host("config")];
    assert_eq!(hub.bump("config", None), 1);
    let host_event = hub.publish(
        "config.updated",
        &host,
        None,
        None,
        None,
        json!({ "ok": true }),
    );
    assert_eq!(
        serde_json::to_value(&host_event.revisions).unwrap(),
        json!([{ "resource": "config", "id": null, "revision": 1, "epoch": hub.epoch() }])
    );
    assert_eq!(hub.revision("config", Some("ignored")), 1);
}

/// 6a-3. `publish_with_ops` carries the display ops next to the payload
/// (`api-events.ts:120-121`, consumed by `display.ops`), stays report-only, and skips the key
/// when there are none - which is how TS spells an absent optional member.
#[test]
fn event_hub_publish_with_ops_carries_display_ops() {
    let hub = events::EventHub::new();
    let resources = [events::ResourceRef::new("turn", Some("s1:t1")).normalized()];
    let ops = vec![json!({ "kind": "text_append", "blockId": "b1", "text": "hello" })];
    let expected_ops = serde_json::Value::Array(ops.clone());

    // One write: one bump, then the publish that reports it.
    assert_eq!(hub.bump("turn", Some("s1:t1")), 1);

    // A listener sees the ops, which is how the stream forwards a `display.ops` update.
    let seen = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let sink = Arc::clone(&seen);
    let subscription = hub.subscribe(Arc::new(move |event: &events::HubEvent| {
        sink.lock().unwrap().extend(event.ops.iter().cloned());
    }));

    let event = hub.publish_with_ops(
        "display.ops",
        &resources,
        Some("s1"),
        Some("t1"),
        Some("t1"),
        json!({ "turn_id": "t1" }),
        ops.clone(),
    );

    assert_eq!(event.ops, ops, "the Rust field is always present");
    assert_eq!(
        event.revisions[0].revision, 1,
        "publish_with_ops reports, it does not advance"
    );
    assert_eq!(hub.revision("turn", Some("s1:t1")), 1);

    let body = serde_json::to_value(&event).unwrap();
    assert_eq!(body["ops"], expected_ops, "serialized under `ops`");
    assert_eq!(
        body["payload"],
        json!({ "turn_id": "t1" }),
        "next to the payload"
    );
    assert_eq!(body["thread_id"], "t1");
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        ops.as_slice(),
        "fan-out hands the ops to the listener verbatim"
    );

    // Empty means "no ops": the key is skipped on the wire, and plain `publish` is exactly
    // this call with an empty list.
    let plain = hub.publish_with_ops(
        "turn.upsert",
        &resources,
        Some("s1"),
        None,
        Some("t1"),
        json!({ "turn_id": "t1" }),
        Vec::new(),
    );
    assert!(plain.ops.is_empty());
    assert!(
        serde_json::to_value(&plain).unwrap().get("ops").is_none(),
        "no `ops` key when there are no ops"
    );

    let via_publish = hub.publish(
        "turn.upsert",
        &resources,
        Some("s1"),
        None,
        Some("t1"),
        json!({}),
    );
    assert_eq!(via_publish.ops, plain.ops);
    assert_eq!(
        hub.revision("turn", Some("s1:t1")),
        1,
        "both forms remain report-only"
    );

    drop(subscription);
    assert_eq!(hub.listener_count(), 0);
}
#[test]
fn event_hub_listener_fan_out_is_isolated() {
    let hub = events::EventHub::new();

    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let panic_seen = Arc::clone(&seen);

    // A panicking listener is dropped rather than retried; the next listener still runs
    // (`api-events.ts:325-336`).
    let panicking = hub.subscribe(Arc::new(move |event: &events::HubEvent| {
        panic_seen
            .lock()
            .unwrap()
            .push(format!("panic:{}", event.kind));

        if event.kind == "turn.upsert" {
            panic!("a dead transport must not lose the change");
        }
    }));
    let survivor = Arc::new(Mutex::new(0_u64));
    let survivor_listener = Arc::clone(&survivor);
    let surviving = hub.subscribe(Arc::new(move |event: &events::HubEvent| {
        let mut count = survivor_listener.lock().unwrap();
        *count += 1;
        let _ = event;
    }));

    assert_eq!(hub.listener_count(), 2);

    let _ = hub.publish("turn.upsert", &[], None, None, None, json!({}));
    assert_eq!(
        *survivor.lock().unwrap(),
        1,
        "the change survived a listener panic"
    );
    assert_eq!(
        hub.listener_count(),
        1,
        "the panicking listener was removed, not retried"
    );

    // A later publish is delivered to the survivor only.
    let _ = hub.publish("config.updated", &[], None, None, None, json!({}));
    assert_eq!(*survivor.lock().unwrap(), 2);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &["panic:turn.upsert".to_string()],
        "the dropped listener never ran again"
    );

    // Detaching: the explicit form and the `Drop` form both remove exactly one listener.
    panicking.detach();
    assert_eq!(hub.listener_count(), 1);
    drop(surviving);
    assert_eq!(hub.listener_count(), 0);
    let _ = hub.publish("session.updated", &[], None, None, None, json!({}));
    assert_eq!(
        *survivor.lock().unwrap(),
        2,
        "no listener runs after detaching"
    );
}

/// 6c. The baseline link table (`api-events.ts:174-239`).
#[test]
fn event_hub_baseline_links_match_the_route_table() {
    assert_eq!(
        events::baseline_link("session", Some("s1")),
        "/v1/harness/sessions/s1"
    );
    assert_eq!(
        events::baseline_link("definition", Some("s1")),
        "/v1/harness/sessions/s1/definition"
    );
    assert_eq!(
        events::baseline_link("turns", Some("s1")),
        "/v1/harness/sessions/s1/turns?limit=25"
    );
    assert_eq!(
        events::baseline_link("turn", Some("s1:t1")),
        "/v1/harness/sessions/s1/turns/t1"
    );
    assert_eq!(
        events::baseline_link("turn_display", Some("s1:t1")),
        "/v1/harness/sessions/s1/turns/t1/display"
    );
    assert_eq!(
        events::baseline_link("turn_events", Some("s1:t1")),
        "/v1/harness/sessions/s1/turns/t1/events?limit=25"
    );
    assert_eq!(
        events::baseline_link("threads", Some("s1")),
        "/v1/harness/sessions/s1/threads?limit=25"
    );
    assert_eq!(
        events::baseline_link("thread", Some("s1:th1")),
        "/v1/harness/sessions/s1/threads/th1"
    );
    assert_eq!(
        events::baseline_link("transcript", Some("s1:th1")),
        "/v1/harness/sessions/s1/threads/th1/transcript"
    );
    assert_eq!(
        events::baseline_link("transcript_messages", Some("s1:th1")),
        "/v1/harness/sessions/s1/threads/th1/transcript/messages?limit=25"
    );
    assert_eq!(
        events::baseline_link("actions", Some("s1")),
        "/v1/harness/sessions/s1/actions"
    );
    assert_eq!(
        events::baseline_link("usage", Some("s1")),
        "/v1/harness/sessions/s1/usage"
    );
    assert_eq!(
        events::baseline_link("queue", Some("s1")),
        "/v1/harness/sessions/s1/queue?limit=25"
    );
    assert_eq!(
        events::baseline_link("session_agents", Some("s1")),
        "/v1/harness/sessions/s1/agents"
    );
    assert_eq!(
        events::baseline_link("session_tools", Some("s1")),
        "/v1/harness/sessions/s1/tools?limit=25"
    );
    assert_eq!(
        events::baseline_link("commands", Some("s1")),
        "/v1/harness/sessions/s1/commands?limit=25"
    );
    assert_eq!(
        events::baseline_link("harnesses", None),
        "/v1/harness/harnesses?limit=25"
    );
    assert_eq!(
        events::baseline_link("agents", None),
        "/v1/harness/agents?limit=25"
    );
    assert_eq!(
        events::baseline_link("tools", None),
        "/v1/harness/tools?limit=25"
    );
    assert_eq!(events::baseline_link("config", None), "/v1/harness/config");

    // An unknown kind still gets a valid link (the root), never a crash (`api-events.ts:234`).
    assert_eq!(events::baseline_link("nonsense", Some("s1")), "/v1/harness");

    // A child kind with no composite parent leaves the session segment empty - the same answer
    // TS's `splitComposite` produces (`api-events.ts:242-250`).
    assert_eq!(
        events::baseline_link("turn", Some("t1")),
        "/v1/harness/sessions//turns/t1"
    );
}

/// 7a. The loopback and Pages host rules (`origin.ts:27-37`, `:81-102`).
#[test]
fn origin_helper_rules_match_the_shared_module() {
    // EXACTLY the TS set, in TS's order (`origin.ts:27-32`).
    for loopback in ["localhost", "127.0.0.1", "[::1]", "::1", "LOCALHOST"] {
        assert!(origin::is_loopback_hostname(loopback), "{loopback}");
    }

    // Prefix lookalikes must not pass (`origin.ts:15-19`), and neither does the bind address
    // or the container gateway: `origin.test.ts:38` asserts `0.0.0.0` is NOT loopback, and
    // `host.docker.internal` is not in the TS set either.
    for not_loopback in [
        "127.0.0.1.evil.test",
        "localhost.attacker.test",
        "notlocalhost",
        "192.168.1.20",
        "0.0.0.0",
        "[::]",
        "host.docker.internal",
        "host.docker.internal.evil.test",
    ] {
        assert!(
            !origin::is_loopback_hostname(not_loopback),
            "{not_loopback}"
        );
    }

    assert_eq!(
        origin::LOOPBACK_HOSTNAMES,
        ["localhost", "127.0.0.1", "[::1]", "::1"]
    );

    assert!(origin::is_pages_hostname("underloadjs.github.io"));
    assert!(origin::is_pages_hostname("any-user.github.io"));
    assert!(
        !origin::is_pages_hostname("github.io"),
        "the apex is not a Pages site"
    );
    assert!(!origin::is_pages_hostname("github.io.attacker.test"));
    assert!(!origin::is_pages_hostname(
        "underloadjs.github.io.attacker.test"
    ));
    assert!(!origin::is_pages_hostname("mygithub.io"));

    assert!(origin::is_pages_origin("https://underloadjs.github.io"));
    assert!(origin::is_pages_origin(
        "https://underloadjs.github.io/harness/"
    ));
    assert!(
        !origin::is_pages_origin("http://underloadjs.github.io"),
        "Pages serves the UI over https only"
    );
    assert!(!origin::is_pages_origin("null"));
    assert!(!origin::is_pages_origin("https://evil.example"));
    assert!(!origin::is_pages_origin("https://localhost:5180"));
}

/// 7b. The two gates: one rule (`mount.ts:41-44`), two refusal sentences.
#[test]
fn origin_gates_answer_with_the_contract_messages() {
    // No Origin header, or an empty one, is not a cross-origin page (`service.ts:456`).
    assert!(origin::check_rest_origin(None, Some("127.0.0.1:5000")).is_ok());
    assert!(origin::check_rest_origin(Some(""), Some("127.0.0.1:5000")).is_ok());
    assert!(origin::check_stream_origin(None, None).is_ok());

    // Same host, including the port, case-insensitively (`service.ts:175`).
    assert!(
        origin::check_rest_origin(Some("http://127.0.0.1:5000"), Some("127.0.0.1:5000")).is_ok()
    );
    assert!(
        origin::check_stream_origin(Some("http://127.0.0.1:5000"), Some("127.0.0.1:5000")).is_ok()
    );

    // A different HOST is not the same origin even when the name is (`origin.test.ts:50-52`).
    assert_eq!(
        origin::check_rest_origin(Some("http://ui.example:8443"), Some("http://ui.example"))
            .unwrap_err()
            .message,
        origin::REST_ORIGIN_MESSAGE
    );

    // Loopback passes by design on any port (`api.test.ts:846-850`), in both gates.
    assert!(
        origin::check_rest_origin(Some("http://localhost:5173"), Some("127.0.0.1:5000")).is_ok()
    );
    assert!(
        origin::check_rest_origin(Some("http://127.0.0.1:6000"), Some("127.0.0.1:5000")).is_ok()
    );
    assert!(origin::check_stream_origin(Some("http://[::1]:5180"), Some("0.0.0.0:5000")).is_ok());
    assert!(origin::check_stream_origin(Some("http://::1:5180"), None).is_ok());

    // `0.0.0.0` and `host.docker.internal` are NOT loopback (`origin.ts:27-32`), so they pass
    // only through the SAME-HOST test (`service.ts:175`, `mount.ts:56`) - and are refused when
    // the `Host` header does not match.
    for (not_local, matching_host) in [
        ("http://0.0.0.0:5173", "0.0.0.0:5173"),
        (
            "http://host.docker.internal:5173",
            "host.docker.internal:5173",
        ),
    ] {
        assert_eq!(
            origin::check_rest_origin(Some(not_local), Some("127.0.0.1:5000"))
                .unwrap_err()
                .message,
            origin::REST_ORIGIN_MESSAGE,
            "{not_local}"
        );
        assert_eq!(
            origin::check_stream_origin(Some(not_local), Some("127.0.0.1:5000"))
                .unwrap_err()
                .message,
            origin::STREAM_ORIGIN_MESSAGE,
            "{not_local}"
        );
        assert_eq!(
            origin::check_stream_origin(Some(not_local), None)
                .unwrap_err()
                .message,
            origin::STREAM_ORIGIN_MESSAGE,
            "{not_local} with no Host header to agree with"
        );
        assert!(
            origin::check_rest_origin(Some(not_local), Some(matching_host)).is_ok(),
            "{not_local} is same-host when the Host header agrees"
        );
        assert!(
            origin::check_stream_origin(Some(not_local), Some(matching_host)).is_ok(),
            "{not_local} upgrades when the Host header agrees"
        );
    }

    // A LAN origin is refused (`api.test.ts:852-855`).
    let lan = origin::check_rest_origin(Some("http://192.168.1.20:8080"), Some("127.0.0.1:5000"))
        .unwrap_err();
    assert_eq!(lan.status, 403);
    assert_eq!(lan.message, "Cross-origin requests are not allowed.");

    // The deployed Pages UI may dial REST cross-origin (`api.test.ts:858-872`), and the
    // upgrade gate applies the SAME rule (`mount.ts:41-44`, `:59` ends with
    // `return isPagesOrigin(origin)`).
    for allowed in [
        "https://foo.github.io",
        "https://underloadjs.github.io",
        "https://any-user.github.io/harness/",
    ] {
        assert!(
            origin::check_rest_origin(Some(allowed), Some("127.0.0.1:5000")).is_ok(),
            "REST: {allowed}"
        );
        assert!(
            origin::check_stream_origin(Some(allowed), Some("192.168.50.109:5000")).is_ok(),
            "stream: {allowed}"
        );
        assert!(
            origin::is_stream_origin_allowed(allowed, None),
            "isStreamOriginAllowed: {allowed}"
        );
    }

    // The Pages allowance does not widen to a lookalike, to the apex, or to http
    // (`api.test.ts:874-891`) - in EITHER gate.
    for refused in [
        "http://underloadjs.github.io",
        "http://foo.github.io",
        "https://github.io",
        "https://github.io.attacker.test",
        "https://underloadjs.github.io.attacker.test",
        "https://evil.example",
    ] {
        let error = origin::check_rest_origin(Some(refused), Some("127.0.0.1:5000")).unwrap_err();
        assert_eq!(error.status, 403, "{refused}");
        assert_eq!(error.message, origin::REST_ORIGIN_MESSAGE, "{refused}");

        let stream_error =
            origin::check_stream_origin(Some(refused), Some("127.0.0.1:5000")).unwrap_err();
        assert_eq!(stream_error.status, 403, "{refused}");
        assert_eq!(
            stream_error.message,
            origin::STREAM_ORIGIN_MESSAGE,
            "{refused}"
        );
    }

    // `Origin: null` (a sandboxed iframe or a `file://` page) proves nothing (`origin.ts:46`).
    assert_eq!(
        origin::check_rest_origin(Some("null"), Some("127.0.0.1:5000"))
            .unwrap_err()
            .message,
        origin::REST_ORIGIN_MESSAGE
    );
    assert_eq!(
        origin::check_stream_origin(Some("null"), Some("127.0.0.1:5000"))
            .unwrap_err()
            .message,
        origin::STREAM_ORIGIN_MESSAGE
    );
    assert_eq!(
        origin::check_rest_origin(Some("javascript:alert(1)"), None)
            .unwrap_err()
            .message,
        origin::REST_ORIGIN_MESSAGE
    );
    assert_eq!(
        origin::check_rest_origin(Some("ftp://127.0.0.1"), None)
            .unwrap_err()
            .message,
        origin::REST_ORIGIN_MESSAGE,
        "a non-web scheme is never trusted"
    );

    // A default port normalizes away, so `http://x.test:80` IS the same host as `http://x.test`.
    assert!(origin::check_rest_origin(Some("http://x.test:80"), Some("x.test")).is_ok());
    assert!(!origin::is_trusted_runtime_origin(
        "http://x.test:8080",
        Some("x.test")
    ));
}

/// 8. The handler-level body validators (`api.ts:504-518`).
#[test]
fn body_validators_print_the_typescript_messages() {
    let unknown =
        contract::require_known_fields(&json!({ "name": "x", "bogus": true }), &["name", "title"])
            .unwrap_err();
    assert_eq!(unknown.status, 400);
    assert_eq!(
        unknown.message,
        "Unknown field \"bogus\". Allowed fields: name, title."
    );

    // Every supplied field is accepted, and a non-object is `require_object`'s business.
    assert!(contract::require_known_fields(&json!({ "name": "x" }), &["name"]).is_ok());
    assert!(contract::require_known_fields(&json!(7), &["name"]).is_ok());

    for not_object in [json!([]), json!(null), json!("text"), json!(7)] {
        let error = contract::require_object(&not_object).unwrap_err();
        assert_eq!(error.status, 400);
        assert_eq!(error.message, "The request body must be a JSON object.");
    }

    // A `Value` must outlive the borrow `require_object` returns, so the document is named.
    let body = json!({ "a": 1 });
    let fields = contract::require_object(&body).expect("an object body");
    assert_eq!(fields.get("a"), Some(&json!(1)));

    // A body-less route still refuses a non-empty body (`api.ts:520-531`).
    assert!(contract::require_empty_body(&Value::Null).is_ok());
    assert!(contract::require_empty_body(&json!({})).is_ok());
    assert_eq!(
        contract::require_empty_body(&json!({ "all": true }))
            .unwrap_err()
            .message,
        "This route accepts no request body."
    );
    assert_eq!(
        contract::require_empty_body(&json!([1]))
            .unwrap_err()
            .message,
        "This route accepts no request body."
    );

    // ApiError carries the status the envelope needs.
    let statuses = [
        (ApiError::bad_request("a"), 400),
        (ApiError::forbidden("a"), 403),
        (ApiError::not_found("a"), 404),
        (ApiError::method_not_allowed("a"), 405),
        (ApiError::conflict("a"), 409),
        (ApiError::payload_too_large("a"), 413),
        (ApiError::unsupported_media_type("a"), 415),
        (ApiError::too_many_requests("a"), 429),
        (ApiError::bad_gateway("a"), 502),
        (ApiError::internal("a"), 500),
    ];
    for (error, status) in statuses {
        assert_eq!(error.status, status);
        assert_eq!(error.to_string(), "a", "Display is the message, verbatim");
    }
}

/// The id/label rules (`core/thread.ts:36`, `agent-results.ts:177-192`, `labels.test.ts`).
#[test]
fn ids_and_labels_follow_the_host_conventions() {
    assert_eq!(ids::host_thread_id(1), "thread-1");
    assert_eq!(ids::host_thread_id(256), "thread-256");

    // First + last ASCII LETTER, uppercased; digits are ignored so the `[A-Z]{2}\d+` grammar
    // cannot break, and a one-letter id yields the letter twice.
    assert_eq!(ids::agent_code("researcher"), "RR");
    assert_eq!(ids::agent_code("lead"), "LD");
    assert_eq!(ids::agent_code("loaded"), "LD");
    assert_eq!(
        ids::agent_code("Researcher"),
        "RR",
        "the id is normalized first"
    );
    assert_eq!(
        ids::agent_code("a1"),
        "AA",
        "a digit cannot be part of the code"
    );
    assert_eq!(ids::agent_code("a"), "AA");
    assert_eq!(ids::agent_code("123"), "AG", "no ASCII letters falls back");
    assert_eq!(ids::agent_code(""), "AG");

    // `RR1`/`RR2` are two conversations of one code (`labels.test.ts:195-217`).
    assert_eq!(ids::thread_label_for("researcher", 1), "RR1");
    assert_eq!(ids::thread_label_for("researcher", 4), "RR4");
    assert_eq!(ids::thread_label(1), "AG1");

    // Instance ids are opaque, unique, and lowercase.
    let id = ids::new_id();
    assert_eq!(id.len(), 36);
    assert_eq!(id.to_lowercase(), id);
    assert_ne!(ids::new_turn_id(), ids::new_session_id());
    assert_eq!(ids::new_request_id().len(), 36);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// base64url without padding, matching what `paging` issues.
fn base64url(text: &str) -> String {
    use base64::Engine as _;

    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(text)
        .trim_end_matches('=')
        .to_string()
}

/// The repo-root `Temp/typecrate-harness/env-fixture` directory (repo rule: scratch lives in
/// the repo `Temp/`, never in OS temp).
fn environment_fixture() -> PathBuf {
    package_root::repo_root()
        .join("Temp")
        .join("typecrate-harness")
        .join("env-fixture")
}

/// `<repo-root>/..`, the directory that holds the shared `temporary/dump`.
fn repo_parent() -> PathBuf {
    package_root::repo_root()
        .parent()
        .expect("the repo root has a parent")
        .to_path_buf()
}

/// Writes one fixture `.env` and removes the fixture directory afterwards.
///
/// The key the fixture adds IS injected into the process environment by the load (that is the
/// TS-parity behaviour of dotenv with `override: false`), and it is deliberately left there:
/// removing a variable needs `unsafe` in edition 2024, this crate forbids unsafe, and the key
/// name (`TYPECRATE_HARNESS_FILE_ONLY`) belongs to this test alone.
struct FixtureGuard {
    directory: PathBuf,
}

impl FixtureGuard {
    fn new(file: &Path, contents: &str) -> Self {
        let directory = file.parent().expect("a fixture file lives in a directory");

        std::fs::create_dir_all(directory).expect("the fixture directory is creatable");
        std::fs::write(file, contents).expect("the fixture is writable");

        Self {
            directory: directory.to_path_buf(),
        }
    }
}

impl Drop for FixtureGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
