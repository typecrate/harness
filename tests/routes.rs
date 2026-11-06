//! The route table, pinned against the TypeScript contract (T9).
//!
//! `crate::api::ROUTES` is the whole HTTP surface of this build: 45 generated templates and the 64
//! operations `packages/underload/harness/src/server/api.ts:3925-3974` declares for them. Every
//! number and every path below is copied from the TypeScript source of record
//! (`packages/underload/harness/src/shared/harness-api.ts:54-100`, the `HARNESS_PATH_TEMPLATES`
//! table), NOT observed from this Rust build, and the list is deliberately duplicated here: a table
//! that silently loses a route, gains one, or REORDERS one changes which handler answers an
//! ambiguous path (`api.ts:657-687` `matchRoute` returns the FIRST fit in template-table order), and
//! nothing else in the suite notices.
//!
//! This file is also the answer to the verification finding "no test references `ROUTES`": the boot
//! document's link map, the 405 sentences and the playground listing all read this table, and until
//! now only their downstream effects were pinned.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use typecrate_harness::api::ROUTES;
use typecrate_harness::contract;

/// The 45 `(route id, template)` pairs of `HARNESS_PATH_TEMPLATES` (`shared/harness-api.ts:54-100`),
/// in the order the generated table lists them. The order is part of the contract, so the expected
/// value is an ORDERED list and the comparison is positional.
const EXPECTED_TEMPLATES: [(&str, &str); 45] = [
    ("root", "/v1/harness"),
    ("health", "/v1/harness/health"),
    ("config", "/v1/harness/config"),
    ("harnesses", "/v1/harness/harnesses"),
    (
        "harnessesByHarnessName",
        "/v1/harness/harnesses/{harness_name}",
    ),
    (
        "harnessesByHarnessNameInstructions",
        "/v1/harness/harnesses/{harness_name}/instructions",
    ),
    (
        "harnessesByHarnessNameInstructionsByAgentId",
        "/v1/harness/harnesses/{harness_name}/instructions/{agent_id}",
    ),
    ("agents", "/v1/harness/agents"),
    ("agentsByAgentId", "/v1/harness/agents/{agent_id}"),
    ("tools", "/v1/harness/tools"),
    ("toolsByToolId", "/v1/harness/tools/{tool_id}"),
    (
        "toolsByToolIdExecutions",
        "/v1/harness/tools/{tool_id}/executions",
    ),
    ("models", "/v1/harness/models"),
    ("sessions", "/v1/harness/sessions"),
    ("sessionsBySessionId", "/v1/harness/sessions/{session_id}"),
    (
        "sessionsBySessionIdDefinition",
        "/v1/harness/sessions/{session_id}/definition",
    ),
    (
        "sessionsBySessionIdUsage",
        "/v1/harness/sessions/{session_id}/usage",
    ),
    (
        "sessionsBySessionIdCommands",
        "/v1/harness/sessions/{session_id}/commands",
    ),
    (
        "sessionsBySessionIdQueue",
        "/v1/harness/sessions/{session_id}/queue",
    ),
    (
        "sessionsBySessionIdTasks",
        "/v1/harness/sessions/{session_id}/tasks",
    ),
    (
        "sessionsBySessionIdReset",
        "/v1/harness/sessions/{session_id}/reset",
    ),
    (
        "sessionsBySessionIdCancel",
        "/v1/harness/sessions/{session_id}/cancel",
    ),
    (
        "sessionsBySessionIdActions",
        "/v1/harness/sessions/{session_id}/actions",
    ),
    (
        "sessionsBySessionIdActionsByRequestIdAnswer",
        "/v1/harness/sessions/{session_id}/actions/{request_id}/answer",
    ),
    (
        "sessionsBySessionIdAgents",
        "/v1/harness/sessions/{session_id}/agents",
    ),
    (
        "sessionsBySessionIdAgentsByAgentId",
        "/v1/harness/sessions/{session_id}/agents/{agent_id}",
    ),
    (
        "sessionsBySessionIdAgentsByAgentIdInstruction",
        "/v1/harness/sessions/{session_id}/agents/{agent_id}/instruction",
    ),
    (
        "sessionsBySessionIdTools",
        "/v1/harness/sessions/{session_id}/tools",
    ),
    (
        "sessionsBySessionIdToolsByToolId",
        "/v1/harness/sessions/{session_id}/tools/{tool_id}",
    ),
    (
        "sessionsBySessionIdToolsByToolIdSource",
        "/v1/harness/sessions/{session_id}/tools/{tool_id}/source",
    ),
    (
        "sessionsBySessionIdToolsByToolIdExecutions",
        "/v1/harness/sessions/{session_id}/tools/{tool_id}/executions",
    ),
    (
        "sessionsBySessionIdThreads",
        "/v1/harness/sessions/{session_id}/threads",
    ),
    (
        "sessionsBySessionIdThreadsByThreadId",
        "/v1/harness/sessions/{session_id}/threads/{thread_id}",
    ),
    (
        "sessionsBySessionIdThreadsByThreadIdCancel",
        "/v1/harness/sessions/{session_id}/threads/{thread_id}/cancel",
    ),
    (
        "sessionsBySessionIdThreadsByThreadIdTranscript",
        "/v1/harness/sessions/{session_id}/threads/{thread_id}/transcript",
    ),
    (
        "sessionsBySessionIdThreadsByThreadIdTranscriptMessages",
        "/v1/harness/sessions/{session_id}/threads/{thread_id}/transcript/messages",
    ),
    (
        "sessionsBySessionIdThreadsByThreadIdTranscriptDocument",
        "/v1/harness/sessions/{session_id}/threads/{thread_id}/transcript/document",
    ),
    (
        "sessionsBySessionIdTurns",
        "/v1/harness/sessions/{session_id}/turns",
    ),
    (
        "sessionsBySessionIdTurnsByTurnId",
        "/v1/harness/sessions/{session_id}/turns/{turn_id}",
    ),
    (
        "sessionsBySessionIdTurnsByTurnIdDisplay",
        "/v1/harness/sessions/{session_id}/turns/{turn_id}/display",
    ),
    (
        "sessionsBySessionIdTurnsByTurnIdEvents",
        "/v1/harness/sessions/{session_id}/turns/{turn_id}/events",
    ),
    (
        "sessionsBySessionIdTurnsByTurnIdStepsByStepId",
        "/v1/harness/sessions/{session_id}/turns/{turn_id}/steps/{step_id}",
    ),
    (
        "sessionsBySessionIdTurnsByTurnIdBlocksByBlockId",
        "/v1/harness/sessions/{session_id}/turns/{turn_id}/blocks/{block_id}",
    ),
    (
        "sessionsBySessionIdTurnsByTurnIdAttachmentsByAttachmentId",
        "/v1/harness/sessions/{session_id}/turns/{turn_id}/attachments/{attachment_id}",
    ),
    ("stream", "/v1/harness/stream"),
];

/// The 64 operations of `api.ts:3925-3974` counted BY VERB, i.e. the row a reader should be able to
/// check against the TypeScript table without re-deriving it: `Object.keys(table)` of every route is
/// what the 405 sentence prints (`api.ts:4007`), so the per-verb totals are wire-visible facts.
const OPERATIONS_BY_METHOD: [(&str, usize); 5] = [
    ("GET", 37),
    ("POST", 13),
    ("PATCH", 7),
    ("DELETE", 6),
    ("PUT", 1),
];

/// 1. Every generated template is present, with the contract's id, at the contract's position.
#[test]
fn the_table_carries_the_45_contract_templates_in_the_contracts_order() {
    let actual: Vec<(&str, &str)> = ROUTES
        .iter()
        .map(|route| (route.id, route.template))
        .collect();

    assert_eq!(ROUTES.len(), 45, "the contract has 45 path templates");
    assert_eq!(actual.len(), EXPECTED_TEMPLATES.len());

    // Positional: a reordered table is a different router, not an equivalent one.
    for (index, expected) in EXPECTED_TEMPLATES.iter().enumerate() {
        assert_eq!(
            actual[index], *expected,
            "route #{index} must be `{}` -> `{}`; the table order is `matchRoute`'s tie-break",
            expected.0, expected.1
        );
    }
}

/// 2. The operation count and its per-verb split, exactly as the TypeScript table adds up.
#[test]
fn the_table_declares_sixty_four_operations_with_the_contracts_verb_split() {
    let mut per_method: BTreeMap<&str, usize> = BTreeMap::new();

    for route in ROUTES.iter() {
        for (method, _) in route.operations {
            *per_method.entry(method).or_insert(0) += 1;
        }
    }

    let total: usize = per_method.values().sum();

    assert_eq!(total, 64, "the contract declares 64 operations");

    // Exactly the five verbs the contract's `MethodTable` allows (`api.ts:3923`).
    let verbs: Vec<&&str> = per_method.keys().collect();

    assert_eq!(
        verbs,
        vec![&"DELETE", &"GET", &"PATCH", &"POST", &"PUT"],
        "an unexpected verb would answer 405 with a sentence no client can satisfy"
    );

    for (method, expected) in OPERATIONS_BY_METHOD {
        assert_eq!(
            per_method[method], expected,
            "`{method}` must appear {expected} times, like `api.ts:3925-3974`"
        );
    }

    // Cross-check the aggregate against the per-route sums, so the constant above cannot drift from
    // the table without this line moving too.
    let declared: usize = ROUTES.iter().map(|route| route.operations.len()).sum();

    assert_eq!(declared, 64);
}

/// 3. All 64 operations are reachable from the table, each exactly once: no variant is dead, no
///    variant is mounted twice, and no `(path, method)` pair is declared twice.
#[test]
fn every_operation_is_reachable_from_the_table_exactly_once() {
    let mut names: BTreeSet<String> = BTreeSet::new();
    let mut mounts: BTreeSet<(&str, &str)> = BTreeSet::new();
    let mut ids: BTreeSet<&str> = BTreeSet::new();
    let mut mentions = 0usize;

    for route in ROUTES.iter() {
        assert!(
            ids.insert(route.id),
            "route id `{}` is declared twice",
            route.id
        );

        for (method, operation) in route.operations {
            mentions += 1;

            // `Operation`'s `Debug` IS the variant name, which is the only stable handle the table
            // exposes for the "one variant per operation" rule.
            assert!(
                names.insert(format!("{operation:?}")),
                "operation `{operation:?}` is mounted more than once"
            );
            assert!(
                mounts.insert((route.template, method)),
                "`{method} {}` is declared twice",
                route.template
            );
        }
    }

    assert_eq!(
        names.len(),
        64,
        "the table must reach 64 distinct operations"
    );
    assert_eq!(mentions, 64, "and reach each of them exactly once");
    assert_eq!(mounts.len(), 64, "no path is mounted twice for one verb");
    assert_eq!(
        ids.len(),
        45,
        "and the 45 template ids are all different (a duplicate id would shadow a route)"
    );
}

/// 4. The three paths the standalone banner and the discovered route bridge advertise are table
///    entries with the verbs those mounts actually serve, so a banner can never name an unmounted
///    path.
#[test]
fn the_mount_paths_the_host_advertises_are_in_the_table() {
    let operations_of = |pathname: &str| -> Vec<&'static str> {
        ROUTES
            .iter()
            .filter(|route| route.template == pathname)
            .flat_map(|route| route.operations.iter().map(|(method, _)| *method))
            .collect()
    };

    assert_eq!(
        operations_of(contract::HARNESS_API_ROOT),
        vec!["GET"],
        "the boot document is a GET, and a POST to it is a contract 405"
    );
    assert_eq!(
        operations_of(contract::HARNESS_WS_STREAM_PATH),
        vec!["GET"],
        "the stream mount is the GET-with-upgrade the bridge registers"
    );
    assert_eq!(operations_of("/v1/harness/health"), vec!["GET"]);

    // The route bridge mounts exactly these three URL shapes
    // (`endpoint/harness/harness-route.rs`), and every one of them resolves through this table.
    assert!(
        ROUTES
            .iter()
            .any(|route| route.template == contract::HARNESS_API_ROOT)
    );
    assert!(
        ROUTES
            .iter()
            .any(|route| route.template == contract::HARNESS_WS_STREAM_PATH)
    );
}

/// 5. A mounted template never escapes the dispatcher's own matchers: every `{param}` segment is
///    lower-snake and every template is mount-relative, which is what `mount_segments` + `match_route`
///    assume when they answer 404 versus 405.
#[test]
fn every_template_is_mount_relative_with_snake_case_parameters() {
    let mount = contract::HARNESS_API_ROOT;

    for route in ROUTES.iter() {
        assert!(
            route.template == mount || route.template.starts_with(&format!("{mount}/")),
            "`{}` must live under the canonical mount",
            route.template
        );

        for segment in route.template[mount.len()..].split('/') {
            let Some(inner) = segment.strip_prefix('{') else {
                assert!(
                    !segment.contains('{'),
                    "`{segment}` mixes a literal and a parameter"
                );

                continue;
            };

            let name = inner
                .strip_suffix('}')
                .unwrap_or_else(|| panic!("`{}` has an unterminated parameter", route.template));

            assert!(
                !name.is_empty()
                    && name
                        .chars()
                        .all(|ch| ch.is_ascii_lowercase() || ch == '_' || ch.is_ascii_digit()),
                "parameter `{name}` of `{}` must be lower snake_case, like the OpenAPI spec's",
                route.template
            );
        }
    }
}

/// 6. The operation enum's variant names are pinned as a set, so "64 variants" is a checkable
///    statement rather than a comment. One variant per operation, and no variant without an operation.
#[test]
fn the_sixty_four_operation_names_are_the_ones_the_contract_describes() {
    let expected: BTreeSet<String> = [
        // host: root / health / config
        "GetRoot",
        "GetHealth",
        "GetConfig",
        "PatchConfig",
        // host: harness definitions
        "GetHarnesses",
        "PostHarness",
        "GetHarness",
        "PatchHarness",
        "DeleteHarness",
        "GetHarnessInstructions",
        "GetHarnessInstructionText",
        // host: global agents
        "GetAgents",
        "GetAgent",
        "PatchAgent",
        "DeleteAgent",
        // host: tool catalogue
        "GetTools",
        "GetTool",
        "PostToolExecution",
        // host: model discovery
        "PostModels",
        // sessions
        "GetSessions",
        "PostSession",
        "GetSession",
        "PatchSession",
        "DeleteSession",
        "GetSessionDefinition",
        "GetSessionUsage",
        "GetSessionCommands",
        "GetSessionQueue",
        "PostSessionTask",
        "PostSessionReset",
        "PostSessionCancel",
        // actions
        "GetSessionActions",
        "PostActionAnswer",
        // session agents
        "GetSessionAgents",
        "PostSessionAgent",
        "GetSessionAgent",
        "PatchSessionAgent",
        "DeleteSessionAgent",
        "GetSessionAgentInstruction",
        "PatchSessionAgentInstruction",
        // session tools
        "GetSessionTools",
        "PostSessionTool",
        "GetSessionTool",
        "PatchSessionTool",
        "DeleteSessionTool",
        "GetSessionToolSource",
        "PostSessionToolExecution",
        // threads
        "GetSessionThreads",
        "PostSessionThread",
        "GetSessionThread",
        "DeleteSessionThread",
        "PostThreadCancel",
        // transcripts
        "GetTranscriptMetadata",
        "PutTranscript",
        "GetTranscriptMessages",
        "GetTranscriptDocument",
        // turns
        "GetSessionTurns",
        "GetSessionTurn",
        "GetSessionTurnDisplay",
        "GetSessionTurnEvents",
        "GetSessionTurnStep",
        "GetSessionTurnBlock",
        "GetSessionTurnAttachment",
        // stream
        "StreamRoute",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();

    let mounted: BTreeSet<String> = ROUTES
        .iter()
        .flat_map(|route| route.operations.iter())
        .map(|(_, operation)| format!("{operation:?}"))
        .collect();

    assert_eq!(expected.len(), 64);
    assert_eq!(
        mounted, expected,
        "the table must reach exactly the operations the contract describes"
    );

    // The exhaustive `match` in `api/mod.rs` refuses to compile if a variant is missing, so the set
    // equality above is the runtime half of the same guarantee.
    let one_operation_per_name: usize = ROUTES
        .iter()
        .flat_map(|route| route.operations.iter())
        .count();

    assert_eq!(one_operation_per_name, 64);
}
