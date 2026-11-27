//! The route table, pinned against the TypeScript contract (T9).
//!
//! `crate::api::ROUTES` is the whole HTTP surface of this build: **52 generated templates and 74
//! operations** — every template and every operation
//! `packages/underload/harness/src/server/api.ts` declares. The gap that used to sit here (the session
//! plugin trio, `GET {root}/sessions/{session_id}/plugins` and the `GET`/`PATCH` pair on
//! `.../plugins/{plugin_id}`) is CLOSED as of C3: the missing state behind it, the per-session settings
//! store, is `Store::session_plugin_rows` and friends (`src/store/mod.rs`), so the table now mounts all
//! of the contract and the held-back lists at the bottom of this file are EMPTY.
//!
//! Every number and every path below is copied from the TypeScript source of record
//! (`packages/underload/harness/src/engine/shared/harness-api.ts:54`, the `HARNESS_PATH_TEMPLATES`
//! table, and the `routes` object of `server/api.ts` at `:6131-6198`), NOT observed from this Rust
//! build, and the list is deliberately duplicated here: a table that silently loses a route, gains one,
//! or REORDERS one changes which handler answers an ambiguous path (`api.ts` `matchRoute` returns the
//! FIRST fit in template-table order), and nothing else in the suite notices. The last test re-reads
//! both TypeScript files and pins that this build mounts every one of the 52 templates and 74
//! operations, so the equality is a checked fact rather than a comment.
//!
//! This file is also the answer to the verification finding "no test references `ROUTES`": the boot
//! document's link map, the 405 sentences and the playground listing all read this table, and until
//! now only their downstream effects were pinned.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use typecrate_harness::api::ROUTES;
use typecrate_harness::contract;

/// The 52 `(route id, template)` pairs of `HARNESS_PATH_TEMPLATES`
/// (`shared/harness-api.ts:54-104`), in the order the generated table lists them. The order is part
/// of the contract, so the expected value is an ORDERED list and the comparison is positional.
///
/// `plugins` and `pluginsByPluginId` are the T10 uplift: the Harness Plugin System's two routes,
/// which the contract places between `toolsByToolIdExecutions` and `models`.
///
/// `sessionsBySessionIdPlugins` and `sessionsBySessionIdPluginsByPluginId` are the C3 uplift: the
/// SESSION-scoped pair the contract places at `harness-api.ts:90-91`, immediately after
/// `sessionsBySessionIdToolsByToolIdExecutions` and before `sessionsBySessionIdThreads`. They sit in
/// that exact position in `ROUTES` for the same reason `matchRoute` uses it - template order is the
/// tie-break, so a session route moved past a more specific one is a different router.
const EXPECTED_TEMPLATES: [(&str, &str); 52] = [
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
    ("plugins", "/v1/harness/plugins"),
    ("pluginsByPluginId", "/v1/harness/plugins/{plugin_id}"),
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
        "sessionsBySessionIdRecords",
        "/v1/harness/sessions/{session_id}/records",
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
        "sessionsBySessionIdAgentsByAgentIdPrefix",
        "/v1/harness/sessions/{session_id}/agents/{agent_id}/prefix",
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
    // C3: the SESSION plugin pair, at `harness-api.ts:90-91` — between the session TOOL executions
    // route and the thread routes, which is exactly where `matchRoute` looks for it.
    (
        "sessionsBySessionIdPlugins",
        "/v1/harness/sessions/{session_id}/plugins",
    ),
    (
        "sessionsBySessionIdPluginsByPluginId",
        "/v1/harness/sessions/{session_id}/plugins/{plugin_id}",
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
        "sessionsBySessionIdThreadsByThreadIdResume",
        "/v1/harness/sessions/{session_id}/threads/{thread_id}/resume",
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

/// The 74 operations counted BY VERB, i.e. the row a reader should be able to check against the
/// TypeScript table without re-deriving it: `Object.keys(table)` of every route is what the 405
/// sentence prints (`api.ts:4007`), so the per-verb totals are wire-visible facts. The plugin pair
/// is the T10 uplift: one more GET (the catalogue) and one more PATCH (the settings write). The
/// Wave 1 uplift adds `GET .../records` (one more GET), `POST .../resume` (one more POST) and the
/// `PATCH /tools/{tool_id}` plus `PATCH .../blocks/{block_id}` writes (two more PATCHes). The C3
/// uplift is the SESSION plugin trio: `GET .../sessions/{id}/plugins` and
/// `GET .../sessions/{id}/plugins/{plugin_id}` (two more GETs) and
/// `PATCH .../sessions/{id}/plugins/{plugin_id}` (one more PATCH), which closes the gap.
const OPERATIONS_BY_METHOD: [(&str, usize); 5] = [
    ("GET", 42),
    ("POST", 14),
    ("PATCH", 11),
    ("DELETE", 6),
    ("PUT", 1),
];

/// 1. Every generated template is present, with the contract's id, at the contract's position.
#[test]
fn the_table_carries_the_52_contract_templates_in_the_contracts_order() {
    let actual: Vec<(&str, &str)> = ROUTES
        .iter()
        .map(|route| (route.id, route.template))
        .collect();

    assert_eq!(ROUTES.len(), 52, "the contract has 52 path templates");
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
fn the_table_declares_seventy_four_operations_with_the_contracts_verb_split() {
    let mut per_method: BTreeMap<&str, usize> = BTreeMap::new();

    for route in ROUTES.iter() {
        for (method, _) in route.operations {
            *per_method.entry(method).or_insert(0) += 1;
        }
    }

    let total: usize = per_method.values().sum();

    assert_eq!(total, 74, "the contract declares 74 operations");

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
            "`{method}` must appear {expected} times, like the TypeScript route table"
        );
    }

    // Cross-check the aggregate against the per-route sums, so the constant above cannot drift from
    // the table without this line moving too.
    let declared: usize = ROUTES.iter().map(|route| route.operations.len()).sum();

    assert_eq!(declared, 74);
}

/// 3. All 74 operations are reachable from the table, each exactly once: no variant is dead, no
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
        74,
        "the table must reach 74 distinct operations"
    );
    assert_eq!(mentions, 74, "and reach each of them exactly once");
    assert_eq!(mounts.len(), 74, "no path is mounted twice for one verb");
    assert_eq!(
        ids.len(),
        52,
        "and the 52 template ids are all different (a duplicate id would shadow a route)"
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

/// 6. The operation enum's variant names are pinned as a set, so "74 variants" is a checkable
///    statement rather than a comment. One variant per operation, and no variant without an operation.
#[test]
fn the_seventy_four_operation_names_are_the_ones_the_contract_describes() {
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
        "PatchTool",
        "PostToolExecution",
        // host: built-in plugin catalogue
        "GetPlugins",
        "PatchPlugin",
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
        "GetSessionRecords",
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
        "GetSessionAgentPrefix",
        // session tools
        "GetSessionTools",
        "PostSessionTool",
        "GetSessionTool",
        "PatchSessionTool",
        "DeleteSessionTool",
        "GetSessionToolSource",
        "PostSessionToolExecution",
        // session plugins — C3: the SESSION-scoped plugin trio. `GetSessionPlugins` is the session's
        // own catalogue page, `GetSessionPlugin` one row of it, and `PatchSessionPlugin` the write that
        // changes what THIS conversation runs with and no other (`api.ts:3132-3231`).
        "GetSessionPlugins",
        "GetSessionPlugin",
        "PatchSessionPlugin",
        // threads
        "GetSessionThreads",
        "PostSessionThread",
        "GetSessionThread",
        "DeleteSessionThread",
        "PostThreadCancel",
        "PostThreadResume",
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
        "PatchSessionTurnBlock",
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

    assert_eq!(expected.len(), 74);
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

    assert_eq!(one_operation_per_name, 74);
}

// ---------------------------------------------------------------------------
// The gap, pinned against the contract source instead of remembered
// ---------------------------------------------------------------------------

/// Reads one file out of the TypeScript source of record. The path is repo-root relative, and the
/// pin fails loudly (rather than skipping) when the file is missing: a silent skip is how a contract
/// pin becomes a comment.
fn reference_text(relative: &str) -> String {
    let path = typecrate_harness::package_root::repo_root().join(relative);

    std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "`{relative}` is the authority for this pin: {error} \
             (check out the reference repository at `packages/underload/harness`)"
        )
    })
}

/// The `(route id, template)` pairs of `HARNESS_PATH_TEMPLATES`
/// (`packages/underload/harness/src/engine/shared/harness-api.ts:54`), in table order. Parsed, not
/// copied, so this file's own list cannot drift from the contract unnoticed.
fn contract_templates() -> Vec<(String, String)> {
    let text = reference_text("packages/underload/harness/src/engine/shared/harness-api.ts");
    let start = text
        .find("export const HARNESS_PATH_TEMPLATES = {")
        .expect("the contract declares HARNESS_PATH_TEMPLATES");
    let body = &text[start..];
    let end = body.find("\n};").expect("the template table is closed");

    let mut rows = Vec::new();

    for line in body[..end].lines() {
        let line = line.trim();

        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        // The one shape inside that block: `routeId: '/v1/harness/...',`.
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };

        let value = rest.trim().trim_end_matches(',').trim();

        if value.len() < 3 || !value.starts_with('\'') || !value.ends_with('\'') {
            continue;
        }

        rows.push((key.trim().to_owned(), value[1..value.len() - 1].to_owned()));
    }

    rows
}

/// The `(route id, verb)` pairs the TypeScript `routes` object declares
/// (`packages/underload/harness/src/server/api.ts`, `const routes: Partial<Record<keyof typeof
/// HARNESS_PATH_TEMPLATES, MethodTable>>`). One row per operation, in table order, which is what the
/// contract's own 405 sentence enumerates per path.
fn contract_operations() -> Vec<(String, String)> {
    let text = reference_text("packages/underload/harness/src/server/api.ts");
    let start = text
        .find("const routes: Partial<Record<keyof typeof HARNESS_PATH_TEMPLATES, MethodTable>> = {")
        .expect("api.ts declares the routes table");

    let mut rows = Vec::new();
    let mut depth = 0_isize;
    let mut opened = false;

    for line in text[start..].lines() {
        depth += line.matches('{').count() as isize - line.matches('}').count() as isize;

        if !opened {
            // The marker line itself opens the object; its tail is the `{`.
            opened = true;
            continue;
        }

        if depth <= 0 {
            break;
        }

        let line = line.trim();

        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        let Some((id, table)) = line.split_once(':') else {
            continue;
        };

        let table = table.trim().trim_end_matches(',').trim();

        if table.len() < 3 || !table.starts_with('{') || !table.ends_with('}') {
            continue;
        }

        for entry in table[1..table.len() - 1].split(',') {
            let Some((verb, _)) = entry.split_once(':') else {
                continue;
            };

            let verb = verb.trim();

            // Only the five verbs the contract's `MethodTable` allows count as an operation.
            if matches!(verb, "GET" | "POST" | "PATCH" | "PUT" | "DELETE") {
                rows.push((id.trim().to_owned(), verb.to_owned()));
            }
        }
    }

    rows
}

/// 7. The contract, re-read: this build mounts EXACTLY the contract's templates under the ids the
///    contract gives them, and holds back NOTHING. The session plugin trio was the last gap, and it
///    closed when the per-session settings store landed (`src/store/mod.rs` `session_plugin_rows` and
///    friends); the two empty lists below are what "closed" means, and a contract route that arrives
///    without this pin moving — or a gap that reopens — fails here.
#[test]
fn this_build_mounts_every_contract_operation_with_nothing_held_back() {
    let templates = contract_templates();
    let operations = contract_operations();

    // The contract's own totals, measured from the source of record (52 templates, 74 operations).
    assert_eq!(
        templates.len(),
        52,
        "HARNESS_PATH_TEMPLATES declares 52 templates"
    );
    assert_eq!(
        operations.len(),
        74,
        "the routes table declares 74 operations"
    );

    let mut by_verb: BTreeMap<&str, usize> = BTreeMap::new();

    for (_, verb) in operations.iter() {
        *by_verb.entry(verb.as_str()).or_insert(0) += 1;
    }

    assert_eq!(
        by_verb,
        BTreeMap::from([
            ("DELETE", 6_usize),
            ("GET", 42),
            ("PATCH", 11),
            ("POST", 14),
            ("PUT", 1),
        ]),
        "the contract's per-verb split is the sum a client sees across every 405 sentence"
    );

    // Every mounted route must carry the contract's template under the contract's id: an id this
    // build invented, or a template retyped by hand, is a wire divergence at this line.
    let contract_by_id: BTreeMap<&str, &str> = templates
        .iter()
        .map(|(id, template)| (id.as_str(), template.as_str()))
        .collect();

    for route in ROUTES.iter() {
        assert_eq!(
            contract_by_id.get(route.id),
            Some(&route.template),
            "`{}` must mount the contract's template for that route id",
            route.id
        );
    }

    // The held-back templates, in contract order.
    let mounted_ids: BTreeSet<&str> = ROUTES.iter().map(|route| route.id).collect();
    let missing_templates: Vec<&str> = templates
        .iter()
        .filter(|(id, _)| !mounted_ids.contains(id.as_str()))
        .map(|(id, _)| id.as_str())
        .collect();

    assert_eq!(
        missing_templates,
        Vec::<&str>::new(),
        "this build mounts every template the contract declares; the session plugin pair was the last \
         gap and it closed with the per-session settings store (`src/store/mod.rs`)"
    );

    // The held-back operations, in contract order: NONE. Two GETs and one PATCH used to sit here (the
    // session plugin trio), and the delta between the contract's 74 and this build's 71 was those three.
    let mounted_rows: BTreeSet<(&str, &str)> = ROUTES
        .iter()
        .flat_map(|route| {
            route
                .operations
                .iter()
                .map(move |(verb, _)| (route.id, *verb))
        })
        .collect();

    let missing_operations: Vec<(&str, &str)> = operations
        .iter()
        .filter(|(id, verb)| !mounted_rows.contains(&(id.as_str(), verb.as_str())))
        .map(|(id, verb)| (id.as_str(), verb.as_str()))
        .collect();

    assert_eq!(
        missing_operations,
        Vec::<(&str, &str)>::new(),
        "the mounted 74 must be the contract's 74, with nothing held back"
    );
}
