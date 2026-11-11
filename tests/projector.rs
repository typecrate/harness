//! Integration tests for the display projector: the fold's vocabulary, the preview bounds, the
//! attempt window, the section/step rows, and the live-equals-replay invariant.
//!
//! WHAT THIS FILE PINS
//! -------------------
//!   * the state vocabularies a renderer prints verbatim (`turn-projector.ts:573-594`);
//!   * `flattenForDisplay` / `previewText` / `previewTextTail`, including the UTF-16 counting rule
//!     and the single `…` marker;
//!   * the section row a root flow mints, the prose nodes it streams, and the one step row a tool
//!     round collapses into;
//!   * the attempt window: a `retry` removes what the failed attempt streamed, `done`/
//!     `attempt_commit` keeps it (`committed: true`);
//!   * `settle`, which is the only way a root row stops streaming, and the abandoned-call rows it
//!     writes for a call that never returned;
//!   * LIVE EQUALS REPLAY: folding events one at a time and rebuilding the display from a cold
//!     fold of the same events produce the same document, which is the invariant `turn-projector.ts`
//!     is built on (`ports.rs:196-200`).
//!
//! Ops are asserted two ways: the ORDER of the meaningful ops (tail bookkeeping is asserted through
//! the display document instead, because a `tail:set` is emitted only when the designated block
//! actually changes), and the exact `display()`/`summary()` documents at the end of each fold.

use serde_json::{Value, json};

use typecrate_harness::contract::{
    HarnessAssistantMessage, HarnessErrorInfo, HarnessEvent, HarnessStopReason, HarnessThread,
    HarnessToolCall, HarnessToolOutcome, HarnessUsage, MessageRole,
};
use typecrate_harness::ports::{Projector, ProjectorFactory};
use typecrate_harness::projector::{
    self, ARGS_PREVIEW_CHARS, ERROR_CHARS, NODE_PREVIEW_CHARS, PREVIEW_CHARS, TurnProjector,
    TurnProjectorFactory,
};

/// The invocation every scripted event carries: a thread record with no parent, no call, no kind
/// and no branch is the TURN's own conversation (`turn-projector.ts:787-788`).
fn root_thread() -> Option<HarnessThread> {
    Some(HarnessThread {
        id: "inv-root".to_owned(),
        parent_id: None,
        call_id: None,
        input: None,
        kind: None,
        branch_id: None,
    })
}

/// The assistant message a `done` frame carries.
fn assistant(
    content: &str,
    reasoning: &str,
    model: &str,
    usage: Option<HarnessUsage>,
) -> HarnessAssistantMessage {
    HarnessAssistantMessage {
        role: MessageRole::Assistant,
        content: content.to_owned(),
        reasoning: reasoning.to_owned(),
        tool_calls: vec![],
        model: model.to_owned(),
        stop_reason: HarnessStopReason::Stop,
        usage,
    }
}

/// Folds a scripted event sequence through a projector from the FACTORY, settling at `at`.
///
/// `at` starts at 1_000 and advances by one per event, so every timestamp in the assertions is a
/// number a reader can recompute from the event's position. The event's ordinal is passed as the
/// display index, because a step's id IS that ordinal (`harness-api.ts:1792`).
fn fold(events: &[HarnessEvent], settle: Option<(&str, u64)>) -> (Vec<Vec<Value>>, Value, Value) {
    let mut projector =
        TurnProjectorFactory::new().create(&typecrate_harness::ports::ProjectorSeed::default());

    fold_with(projector.as_mut(), events, settle)
}

/// The same fold over a caller-supplied projector, which is how the replay test compares the
/// factory's projector with one built directly.
fn fold_with(
    projector: &mut dyn Projector,
    events: &[HarnessEvent],
    settle: Option<(&str, u64)>,
) -> (Vec<Vec<Value>>, Value, Value) {
    let mut ops: Vec<Vec<Value>> = Vec::with_capacity(events.len());

    for (index, event) in events.iter().enumerate() {
        let position = u64::try_from(index).expect("a small script");

        ops.push(projector.apply(event, None, 1_000 + position, position));
    }

    if let Some((status, at)) = settle {
        projector.settle(status, at);
    }

    (ops, projector.summary(), projector.display())
}

/// The op names one event produced, with the tail bookkeeping filtered out: whether a `tail:set`
/// or `tail:clear` rides an event depends on whether the designated block CHANGED, and the
/// resulting state is asserted exactly through `display()`.
fn op_names(ops: &[Value]) -> Vec<&str> {
    ops.iter()
        .filter(|op| {
            !matches!(
                op["op"].as_str().unwrap_or_default(),
                "tail:set" | "tail:clear" | "node:set"
            )
        })
        .map(|op| op["op"].as_str().unwrap_or_default())
        .collect()
}

/// The whole sequence, flattened to op names.
fn all_op_names(ops: &[Vec<Value>]) -> Vec<&str> {
    ops.iter().flat_map(|group| op_names(group)).collect()
}

/// Finds the one op of `name` whose payload names `node_id` (or the first of that name).
fn op_with<'a>(ops: &'a [Value], name: &str, node_id: Option<&str>) -> Option<&'a Value> {
    ops.iter().find(|op| {
        op["op"] == json!(name)
            && node_id.is_none_or(|id| {
                op.get("nodeId").and_then(Value::as_str) == Some(id)
                    || op.get("call").and_then(|call| call["id"].as_str()) == Some(id)
            })
    })
}

#[test]
fn the_state_vocabularies_are_the_ones_the_display_contract_prints() {
    assert_eq!(
        projector::outcome_name(HarnessToolOutcome::Completed),
        "completed"
    );
    assert_eq!(
        projector::outcome_name(HarnessToolOutcome::Failed),
        "failed"
    );
    assert_eq!(
        projector::outcome_name(HarnessToolOutcome::Rejected),
        "rejected"
    );
    assert_eq!(
        projector::outcome_name(HarnessToolOutcome::Cancelled),
        "cancelled"
    );

    // `isError` is the boolean projection of `outcome`, and a frame that predates `outcome` still
    // reads correctly (`turn-projector.ts:197-210`).
    assert_eq!(projector::step_state_of(Some("completed"), false), "ok");
    assert_eq!(
        projector::step_state_of(Some("cancelled"), true),
        "cancelled"
    );
    assert_eq!(projector::step_state_of(Some("failed"), true), "error");
    assert_eq!(projector::step_state_of(Some("rejected"), true), "error");
    assert_eq!(projector::step_state_of(None, true), "error");
    assert_eq!(projector::step_state_of(None, false), "ok");

    assert_eq!(projector::section_state_of("done"), "done");
    assert_eq!(projector::section_state_of("error"), "error");
    assert_eq!(projector::section_state_of("cancelled"), "cancelled");
    assert_eq!(projector::section_state_of("running"), "streaming");
    assert_eq!(projector::section_state_of("queued"), "open");

    assert_eq!(projector::abandoned_state_of("cancelled"), "cancelled");
    assert_eq!(projector::abandoned_state_of("error"), "error");

    assert_eq!(projector::turn_state_of("running"), "streaming");
    assert_eq!(projector::turn_state_of("done"), "done");
    assert_eq!(projector::turn_state_of("error"), "error");
    assert_eq!(projector::turn_state_of("cancelled"), "cancelled");
    assert_eq!(projector::turn_state_of("queued"), "queued");

    assert!(projector::is_terminal_status("done"));
    assert!(projector::is_terminal_status("error"));
    assert!(projector::is_terminal_status("cancelled"));
    assert!(!projector::is_terminal_status("running"));
    assert!(!projector::is_terminal_status("queued"));

    assert_eq!(
        projector::event_agent(&HarnessEvent::Start {
            agent: "worker".to_owned(),
            model: "m".to_owned(),
            thread: None,
        }),
        "worker"
    );
    assert_eq!(
        projector::event_agent(&HarnessEvent::Error {
            agent: "primary".to_owned(),
            error: HarnessErrorInfo {
                message: "x".to_owned()
            },
            thread: None,
        }),
        "primary"
    );
}

#[test]
fn previews_flatten_markdown_and_bound_on_code_points() {
    // `display_chars` counts UTF-16 code units, which is what TypeScript's `.length` counts, so a
    // preview budget means the SAME thing on both sides of the port.
    assert_eq!(projector::display_chars("plain"), 5);
    assert_eq!(projector::display_chars("😀 hi"), 5);
    assert_eq!(projector::display_chars(""), 0);

    // The nine rewrites run in TypeScript's order: fences, images, links, backticks, emphasis,
    // pipes, rules, whitespace runs, then a space before punctuation.
    assert_eq!(
        projector::flatten_for_display("# Title\n\nsome **bold** text"),
        "Title some bold text"
    );
    assert_eq!(
        projector::flatten_for_display("see [the docs](https://example.com/a) now"),
        "see the docs now",
        "a LINK keeps its text and loses its target; an image keeps its target"
    );
    assert_eq!(
        projector::flatten_for_display("![shot](https://example.com/s.png)"),
        "https://example.com/s.png"
    );
    assert_eq!(
        projector::flatten_for_display("run `cargo test` first"),
        "run cargo test first"
    );
    assert_eq!(projector::flatten_for_display("| a | b |"), "a b");
    assert_eq!(
        projector::flatten_for_display("line\n---\nnext"),
        "line next"
    );
    assert_eq!(
        projector::flatten_for_display("ends with a space , like this"),
        "ends with a space, like this"
    );

    // A head-bounded preview keeps the opening and marks the cut with one `…`, and its budget is
    // in code units INCLUDING the marker.
    assert_eq!(projector::preview_text("hello", 10), "hello");
    assert_eq!(projector::preview_text("0123456789", 5), "0123…");
    assert_eq!(projector::preview_text("", 5), "");
    assert_eq!(
        projector::preview_text("😀😀😀😀😀", 3),
        "😀…",
        "the budget is code units INCLUDING the marker, so a surrogate pair is never split"
    );

    // A tail-bounded preview keeps the LATEST lines, which is what a long answer that stopped
    // being the current block still has to show.
    assert_eq!(projector::preview_text_tail("0123456789", 5), "…6789");
    assert_eq!(projector::preview_text_tail("hello", 10), "hello");
    assert_eq!(
        projector::preview_text_tail("😀😀😀😀😀", 3),
        "…😀",
        "and the same rule on the other end: five astral characters are 10 code units"
    );

    // The caps themselves, so a test that quotes 280 is quoting the contract.
    assert_eq!(PREVIEW_CHARS, 280);
    assert_eq!(ARGS_PREVIEW_CHARS, 200);
    assert_eq!(NODE_PREVIEW_CHARS, 320);
    assert_eq!(ERROR_CHARS, 600);
}

#[test]
fn an_untouched_fold_reports_a_queued_row_with_no_sections() {
    let projector = TurnProjector::new();

    assert_eq!(projector.status(), "queued");
    assert_eq!(projector.updated_at(), 0);
    assert_eq!(
        projector.summary(),
        json!({
            "id": "",
            "index": 0,
            "prompt": "",
            "status": "queued",
            "state": "queued",
            "updatedAt": 0,
            "sectionCount": 0,
            "stepCount": 0,
            "activeAgents": [],
            "blocks": [],
        }),
        "the record-only members are stated as the seam allows: `create()` sees no turn record"
    );
    assert_eq!(projector.display(), json!({"turnId": "", "sections": []}));
}

#[test]
fn a_root_flow_opens_one_section_and_streams_its_answer() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-of-the-hour".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "Two ".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "plus two".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Done {
            agent: "primary".to_owned(),
            message: assistant("Two plus two", "", "model-of-the-hour", None),
            pending: vec![],
            thread: root_thread(),
        },
    ];
    let (ops, summary, display) = fold(&events, Some(("done", 1_004)));

    assert_eq!(
        all_op_names(&ops),
        // start: the flow opens (`section:open`) and its row is patched with model/state.
        // text: one insert, then one append per delta - the running window streams in FULL - and
        // the section row is re-patched as its designated tail moves. done: the attempt commits,
        // and the row is patched once more with the answer reference.
        //
        // R18 (prose context state): each delta ALSO keeps a `context` segment row per window, so the
        // bar carries the streaming text and its length. The first delta mints the segment
        // (`context:add`) and every delta patches its `chars` (`context:set`) - exactly the ops
        // `turn-projector.ts` puts in `live.ops` beside the node appends.
        vec![
            "section:open",
            "section:set",
            "node:insert",
            "context:add",
            "node:append",
            "context:set",
            "node:append",
            "context:set",
            "section:set",
            "attempt:commit",
        ]
    );

    // The insert mints an EMPTY node and the delta rides the append that follows it, which is
    // exactly TypeScript's order (`turn-projector.ts:1912-1913` then `:1900`): the internal node
    // keeps the exact text, and the inserted copy is the wire shape of the node AS OF insertion,
    // including the `redacted` flag while the section's designated tail has not moved to it.
    let inserted = op_with(&ops[1], "node:insert", None).expect("the first delta mints a node");

    assert_eq!(
        inserted["node"],
        json!({"kind": "text", "id": "n1", "text": "", "chars": 0, "committed": false, "redacted": true}),
        "a minted node arrives empty and bounded; the append streams its bytes"
    );

    let appended = op_with(&ops[2], "node:append", Some("n1")).expect("a second delta appends");

    assert_eq!(
        appended,
        &json!({"op": "node:append", "turnId": "", "sectionId": "s0", "nodeId": "n1", "delta": "plus two"}),
        "the running answer streams in FULL, one append per delta"
    );

    // The settled row: one section, one node, the whole answer, and the tail that designates it.
    assert_eq!(
        display,
        json!({
            "turnId": "",
            "lastSectionOrdinal": 0,
            "sections": [{
                "id": "s0",
                "role": "primary",
                "agent": "primary",
                "agentLabel": "",
                "state": "done",
                "model": "model-of-the-hour",
                "invocationId": "inv-root",
                "startedAt": 1_000,
                "endedAt": 1_004,
                "tail": {"kind": "assistant", "nodeId": "n1", "chars": 12, "truncated": false},
                // The streamed answer owns one context row: every prose window mints a segment on its
                // first delta and its `chars` track the stream (`turn-projector.ts:1891-1925`), and the
                // recorded TypeScript document says so byte for byte - `fixtures/lane-compaction/turn-displays.json`
                // carries `0-assistant-1079` with `chars: 32`, which is neither the whole answer (45)
                // nor absent, so it cannot be the `done` fallback (`turn-display.ts:373-376`) that the
                // pre-R18 fold produced here. The `done` fallback adds a row only for an answer that
                // never streamed, and this one streamed, so this array holds exactly the window's row.
                "context": [{
                    "kind": "assistant",
                    "id": "0-assistant-1",
                    "turnId": "",
                    "sectionId": "s0",
                    "name": "answer",
                    "at": 1_001,
                    "chars": 12,
                    "inPrompt": true,
                    "eventFrom": 1,
                    "eventTo": 2,
                    "durationMs": 1,
                }],
                "answerNodeId": "n1",
                "nodes": [{
                    "kind": "text",
                    "id": "n1",
                    "text": "Two plus two",
                    "chars": 12,
                    "committed": true,
                }],
            }],
        })
    );
    assert_eq!(
        summary,
        json!({
            "id": "",
            "index": 0,
            "prompt": "",
            "status": "done",
            "state": "done",
            "updatedAt": 1_004,
            "sectionCount": 1,
            "stepCount": 0,
            "activeAgents": [],
            "blocks": [],
            "startedAt": 1_000,
            "detail": "model-of-the-hour",
        }),
        "a DONE turn shows the model that answered, and a terminal turn has no active agents"
    );
}

#[test]
fn a_retry_discards_the_failed_attempt_and_a_commit_keeps_it() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "half an answer".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Retry {
            agent: "primary".to_owned(),
            attempt: 1,
            max_attempts: 11,
            delay_ms: 1_000,
            error: HarnessErrorInfo {
                message: "HTTP 500: boom".to_owned(),
            },
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "the real answer".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Done {
            agent: "primary".to_owned(),
            message: assistant("the real answer", "", "model-a", None),
            pending: vec![],
            thread: root_thread(),
        },
    ];
    let (ops, _, display) = fold(&events, Some(("done", 1_005)));

    assert!(
        all_op_names(&ops).contains(&"attempt:discard"),
        "a retry mints a discard so the renderer removes what it showed: {ops:?}"
    );

    let nodes = display["sections"][0]["nodes"].clone();

    assert_eq!(
        nodes,
        json!([{
            "kind": "text",
            "id": "n3",
            "text": "the real answer",
            "chars": 15,
            "committed": true,
        }]),
        "the discarded attempt left NO node behind, and the retry's own text is the answer"
    );
    assert_eq!(
        display["sections"][0]["tail"],
        json!({"kind": "assistant", "nodeId": "n3", "chars": 15, "truncated": false}),
        "and the tail moved to the surviving block"
    );
    assert_eq!(
        display["sections"][0]["answerNodeId"],
        json!("n3"),
        "the answer of record is the committed one"
    );

    // A `retry` mints no notice of its own (R16a): the only ops it produces are the discard and
    // the tail re-resolution - plus, since R18, the removal of every context segment that attempt
    // was streaming (`turn-projector.ts:1746-1749` removes the flow's blocks before dropping it).
    // This attempt streamed one answer delta, so exactly one segment goes.
    let retry_ops = op_names(&ops[2]);

    assert_eq!(retry_ops, vec!["context:remove", "attempt:discard"]);
}

#[test]
fn a_tool_round_becomes_one_step_row_with_its_result() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::ToolCall {
            agent: "primary".to_owned(),
            call: HarnessToolCall {
                id: "call-1".to_owned(),
                name: "read".to_owned(),
                arguments: "{\"path\":\"a.txt\"}".to_owned(),
            },
            thread: root_thread(),
        },
        HarnessEvent::ToolResult {
            agent: "primary".to_owned(),
            name: "read".to_owned(),
            content: "one\ntwo\n".to_owned(),
            is_error: false,
            call_id: Some("call-1".to_owned()),
            outcome: Some(HarnessToolOutcome::Completed),
            ui: vec![],
            media: vec![],
            duration_ms: Some(4),
            pending: vec![],
            thread: root_thread(),
        },
        HarnessEvent::AttemptCommit {
            agent: "primary".to_owned(),
            attempt: 1,
            max_attempts: 11,
            thread: root_thread(),
        },
    ];
    let (ops, summary, display) = fold(&events, Some(("done", 1_005)));

    assert_eq!(
        all_op_names(&ops),
        vec![
            "section:open",
            "section:set",
            "step:set",
            // The call mints the bar entry beside the step row (`turn-projector.ts:2043`)...
            "context:add",
            "step:set",
            // ...and its result rewrites THAT entry, in the same frame as the step patch (`:2195`).
            "context:set",
            "attempt:commit",
        ],
        "the call opens a step row and its result PATCHES that same row: one row per call"
    );

    // The bar is a second, METADATA-only view of the same round: one segment for the call, which
    // its result settles. No text rides on a segment - the bytes come from the recorded range.
    let added = ops
        .iter()
        .find_map(|frame| op_with(frame, "context:add", None))
        .expect("the call mints one context-bar segment");

    assert_eq!(
        added,
        &json!({
            "op": "context:add",
            "turnId": "",
            "sectionId": "s0",
            "block": {
                // The key is the OPENING event's index, the same derivation as the step id, so
                // the step row and the segment name the same round by construction.
                "kind": "tool",
                "id": "0-tool-1",
                "turnId": "",
                "sectionId": "s0",
                "name": "read",
                "at": 1_001,
                "chars": 16,
                "argsChars": 16,
                "inPrompt": true,
                "eventFrom": 1,
                "eventTo": 1,
            },
        }),
        "an open call reports its argument bytes and nothing else"
    );

    let settled = ops
        .iter()
        .find_map(|frame| op_with(frame, "context:set", None))
        .expect("the result settles that same segment");

    assert_eq!(
        settled,
        &json!({
            "op": "context:set",
            "turnId": "",
            "sectionId": "s0",
            "blockId": "0-tool-1",
            "patch": {
                // 16 argument code units + 8 result code units, counted exactly (invariant 5).
                "chars": 24,
                "resultChars": 8,
                "state": "ok",
                "endedAt": 1_002,
                "eventTo": 2,
                "durationMs": 4,
            },
        }),
        "one segment per call: the result patches the segment its call opened, identity members intact"
    );

    let nodes = display["sections"][0]["nodes"].clone();

    assert_eq!(
        nodes,
        json!([{
            "kind": "tool",
            "id": "1",
            "turnId": "",
            "sectionId": "s0",
            "callId": "call-1",
            "name": "read",
            "argsPreview": "{\"path\":\"a.txt\"}",
            "state": "ok",
            "startedAt": 1_001,
            "outcome": "completed",
            "preview": "one two",
            "durationMs": 4,
            "endedAt": 1_002,
            "detail": {
                "stepId": "1",
                "resultStepId": "2",
                "hasResult": true,
                "truncated": false,
            },
        }]),
        "the newline flattens into the preview, and the detail reference names both frames"
    );
    assert_eq!(
        display["sections"][0]["tail"],
        json!({
            "kind": "tool",
            "nodeId": "1",
            // `chars` is the displayed body's length: 16 code units of arguments preview plus 7 of
            // result preview.
            "chars": 23,
            "truncated": true,
            "args": "{\"path\":\"a.txt\"}",
            "result": "one two",
        }),
        "a tool block is the newest eligible block, and its tail is always marked truncated"
    );
    assert_eq!(
        display["sections"][0]["context"],
        json!([{
            "kind": "tool",
            "id": "0-tool-1",
            "turnId": "",
            "sectionId": "s0",
            "name": "read",
            "at": 1_001,
            "chars": 24,
            "argsChars": 16,
            "inPrompt": true,
            "eventFrom": 1,
            "eventTo": 2,
            "resultChars": 8,
            "state": "ok",
            "endedAt": 1_002,
            "durationMs": 4,
        }]),
        "the compiled tree carries the settled segment the ops built, so a cold replay says the same thing"
    );
    assert_eq!(summary["stepCount"], json!(1));
    assert_eq!(summary["status"], json!("done"));
}

#[test]
fn a_failed_call_renders_as_an_error_step_not_a_missing_one() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::ToolCall {
            agent: "primary".to_owned(),
            call: HarnessToolCall {
                id: "call-1".to_owned(),
                name: "shell".to_owned(),
                arguments: "{\"command\":\"false\"}".to_owned(),
            },
            thread: root_thread(),
        },
        HarnessEvent::ToolResult {
            agent: "primary".to_owned(),
            name: "shell".to_owned(),
            content: "Tool \"shell\" failed: exit 1".to_owned(),
            is_error: true,
            call_id: Some("call-1".to_owned()),
            outcome: Some(HarnessToolOutcome::Failed),
            ui: vec![],
            media: vec![],
            duration_ms: Some(11),
            pending: vec![],
            thread: root_thread(),
        },
    ];
    let (_, _, display) = fold(&events, Some(("done", 1_004)));
    let step = &display["sections"][0]["nodes"][0];

    assert_eq!(step["state"], json!("error"));
    assert_eq!(step["outcome"], json!("failed"));
    assert_eq!(step["durationMs"], json!(11));
    assert_eq!(step["preview"], json!("Tool \"shell\" failed: exit 1"));
}

#[test]
fn a_long_result_is_previewed_and_says_so() {
    // 400 code units against the 280 preview cap: the row shows a head-bounded preview with one
    // `…`, states the EXACT length of the flattened body, and flags the cut.
    let content = "x".repeat(400);
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::ToolCall {
            agent: "primary".to_owned(),
            call: HarnessToolCall {
                id: "call-1".to_owned(),
                name: "grep".to_owned(),
                arguments: "{\"pattern\":\"x\"}".to_owned(),
            },
            thread: root_thread(),
        },
        HarnessEvent::ToolResult {
            agent: "primary".to_owned(),
            name: "grep".to_owned(),
            content: content.clone(),
            is_error: false,
            call_id: Some("call-1".to_owned()),
            outcome: Some(HarnessToolOutcome::Completed),
            ui: vec![],
            media: vec![],
            duration_ms: Some(1),
            pending: vec![],
            thread: root_thread(),
        },
    ];
    let (_, _, display) = fold(&events, Some(("done", 1_004)));
    let step = &display["sections"][0]["nodes"][0];

    assert_eq!(step["preview"], json!(format!("{}…", "x".repeat(279))));
    assert_eq!(step["detail"]["truncated"], json!(true));

    // The unbounded body is still what the fold recorded: the cap is a display bound, and the
    // step detail route answers with the full bytes.
    assert_eq!(projector::display_chars(&content), 400);
}

#[test]
fn usage_frames_publish_both_lines_and_fold_the_tally() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Usage {
            agent: "primary".to_owned(),
            usage: HarnessUsage {
                input: 100,
                output: 20,
                total: 120,
                cached: Some(40),
            },
            thread: root_thread(),
        },
        HarnessEvent::Usage {
            agent: "primary".to_owned(),
            usage: HarnessUsage {
                input: 50,
                output: 10,
                total: 60,
                cached: Some(5),
            },
            thread: root_thread(),
        },
    ];
    let (ops, summary, _) = fold(&events, Some(("done", 1_004)));
    let usage_ops: Vec<&Value> = ops[1]
        .iter()
        .filter(|op| op["op"] == json!("usage"))
        .collect();

    assert_eq!(
        usage_ops,
        vec![
            &json!({
                "op": "usage",
                "turnId": "",
                "sectionId": "s0",
                "latestInput": 100,
                "cachedInput": 40,
            }),
            &json!({
                "op": "usage",
                "turnId": "",
                "sectionId": "turn",
                "latestInput": 100,
                "usage": {"input": 100, "output": 20, "total": 120, "cached": 40},
            }),
        ],
        "one usage frame publishes the invocation's line AND the turn tile's line"
    );

    // The turn tally folds both frames, while `latestInput` stays the LATEST request's figure.
    assert_eq!(
        summary["usage"],
        json!({"input": 150, "output": 30, "total": 180, "cached": 45})
    );
    assert_eq!(summary["latestInput"], json!(50));
    assert_eq!(summary["cachedInput"], json!(45));
}

#[test]
fn settle_closes_every_row_and_abandons_an_open_call() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::ToolCall {
            agent: "primary".to_owned(),
            call: HarnessToolCall {
                id: "call-1".to_owned(),
                name: "shell".to_owned(),
                arguments: "{}".to_owned(),
            },
            thread: root_thread(),
        },
    ];

    // A turn that ended while a call was still running: the call says WHY it has no result.
    let (_, summary, display) = fold(&events, Some(("error", 2_000)));

    assert_eq!(summary["status"], json!("error"));
    assert_eq!(summary["state"], json!("error"));
    assert_eq!(summary["updatedAt"], json!(2_000));
    assert_eq!(summary["activeAgents"], json!([]));
    assert_eq!(display["sections"][0]["state"], json!("error"));
    assert_eq!(display["sections"][0]["endedAt"], json!(2_000));
    assert_eq!(display["sections"][0]["nodes"][0]["state"], json!("error"));
    assert_eq!(
        display["sections"][0]["nodes"][0]["preview"],
        json!("the turn ended before this call returned"),
        "an abandoned call states the reason instead of pretending an empty result"
    );
    assert_eq!(
        display["sections"][0]["nodes"][0].get("durationMs"),
        None,
        "no duration is invented for a call that never reported one"
    );

    // A cancelled turn calls the same row a control outcome, not an error.
    let (_, cancelled_summary, cancelled_display) = fold(&events, Some(("cancelled", 3_000)));

    assert_eq!(cancelled_summary["status"], json!("cancelled"));
    assert_eq!(cancelled_summary["state"], json!("cancelled"));
    assert_eq!(
        cancelled_display["sections"][0]["state"],
        json!("cancelled")
    );
    assert_eq!(
        cancelled_display["sections"][0]["nodes"][0]["state"],
        json!("cancelled")
    );
    assert_eq!(
        cancelled_display["sections"][0]["nodes"][0]["preview"],
        json!("the turn was cancelled before this call returned")
    );
}

#[test]
fn an_error_event_becomes_the_turn_status_line() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "partial work".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Error {
            agent: "primary".to_owned(),
            error: HarnessErrorInfo {
                message: "the provider went away".to_owned(),
            },
            thread: root_thread(),
        },
    ];
    let (_, summary, display) = fold(&events, Some(("error", 1_004)));

    assert_eq!(summary["detail"], json!("the provider went away"));
    assert_eq!(
        display["sections"][0]["nodes"][0],
        json!({"kind": "text", "id": "n1", "text": "partial work", "chars": 12, "committed": false}),
        "the failed request's own text is still shown"
    );
    assert_eq!(
        display["sections"][0]["tail"]["nodeId"],
        json!("n1"),
        "the error NOTICE never takes the tail, so a reader still sees the lost answer"
    );

    let notices: Vec<&Value> = display["sections"][0]["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .filter(|node| node["kind"] == json!("notice"))
        .collect();

    assert_eq!(
        notices,
        vec![&json!({
            "kind": "notice",
            "id": "n2",
            "tone": "error",
            "text": "the provider went away",
            "at": 1_002,
        })],
        "the error rides as server status, bounded by the notice cap when it is long"
    );
}

#[test]
fn live_folding_equals_a_cold_replay_of_the_same_events() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Reasoning {
            agent: "primary".to_owned(),
            delta: "thinking about it".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "first half".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Retry {
            agent: "primary".to_owned(),
            attempt: 1,
            max_attempts: 11,
            delay_ms: 1_000,
            error: HarnessErrorInfo {
                message: "HTTP 500: boom".to_owned(),
            },
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "second half".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::ToolCall {
            agent: "primary".to_owned(),
            call: HarnessToolCall {
                id: "call-9".to_owned(),
                name: "timeNow".to_owned(),
                arguments: "{\"timezone\":\"UTC\"}".to_owned(),
            },
            thread: root_thread(),
        },
        HarnessEvent::ToolResult {
            agent: "primary".to_owned(),
            name: "timeNow".to_owned(),
            content: "12:00".to_owned(),
            is_error: false,
            call_id: Some("call-9".to_owned()),
            outcome: Some(HarnessToolOutcome::Completed),
            ui: vec![],
            media: vec![],
            duration_ms: Some(2),
            pending: vec![],
            thread: root_thread(),
        },
        HarnessEvent::AttemptCommit {
            agent: "primary".to_owned(),
            attempt: 2,
            max_attempts: 11,
            thread: root_thread(),
        },
        HarnessEvent::Usage {
            agent: "primary".to_owned(),
            usage: HarnessUsage {
                input: 7,
                output: 3,
                total: 10,
                cached: None,
            },
            thread: root_thread(),
        },
        HarnessEvent::Done {
            agent: "primary".to_owned(),
            message: assistant(
                "second half",
                "thinking about it",
                "model-a",
                Some(HarnessUsage {
                    input: 7,
                    output: 3,
                    total: 10,
                    cached: None,
                }),
            ),
            pending: vec![],
            thread: root_thread(),
        },
    ];

    let (live_ops, live_summary, live_display) = fold(&events, Some(("done", 2_000)));

    // THE REPLAY: a projector built DIRECTLY (the way a cold fold is built at restore) over the
    // same recorded events in the same order, then settled the same way (`ports.rs:196-200`).
    let (replay_ops, replay_summary, replay_display) =
        fold_with(&mut TurnProjector::new(), &events, Some(("done", 2_000)));

    assert_eq!(live_ops, replay_ops, "the same events produce the same ops");
    assert_eq!(live_summary, replay_summary, "and the same row");
    assert_eq!(live_display, replay_display, "and the same display tree");

    // The mixed fold is what makes the invariant worth pinning: the discarded half is gone, the
    // committed half and the settled step remain, in mint order.
    assert_eq!(
        replay_display["sections"][0]["nodes"]
            .as_array()
            .expect("nodes")
            .iter()
            .map(|node| node["kind"].as_str().unwrap_or_default().to_owned())
            .collect::<Vec<_>>(),
        vec!["text", "tool"],
        "the retried attempt's reasoning AND text nodes were both discarded - a retry removes \
         everything streamed since the boundary - and the surviving text plus the settled step \
         remain in mint order"
    );
    assert_eq!(
        replay_display["sections"][0]["nodes"][0],
        json!({
            "kind": "text",
            "id": "n4",
            "text": "second half",
            "chars": 11,
            "committed": true,
            // The settled STEP is the newest eligible block, so it holds the tail and this prose
            // node is history: bounded, and flagged as such even though the cap did not cut it.
            "redacted": true,
        })
    );
    assert_eq!(
        replay_summary["usage"],
        json!({"input": 7, "output": 3, "total": 10})
    );
    assert_eq!(replay_summary["stepCount"], json!(1));
    assert_eq!(replay_ops.len(), events.len());
}

#[test]
fn a_result_only_frame_renders_instead_of_being_dropped() {
    // A `tool_result` with no matching open call is a real result (a missed `tool_call`), and
    // dropping it would lose information (`:2145-2202`).
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::ToolResult {
            agent: "primary".to_owned(),
            name: "orphan".to_owned(),
            content: "arrived without a call".to_owned(),
            is_error: false,
            call_id: Some("call-missing".to_owned()),
            outcome: Some(HarnessToolOutcome::Completed),
            ui: vec![],
            media: vec![],
            duration_ms: Some(3),
            pending: vec![],
            thread: root_thread(),
        },
    ];
    let (_, summary, display) = fold(&events, Some(("done", 1_004)));
    let step = &display["sections"][0]["nodes"][0];

    assert_eq!(step["kind"], json!("tool"));
    assert_eq!(step["callId"], json!("call-missing"));
    assert_eq!(step["name"], json!("orphan"));
    assert_eq!(step["state"], json!("ok"));
    assert_eq!(step["startedAt"], json!(1_001));
    assert_eq!(step["endedAt"], json!(1_001));
    assert_eq!(step["detail"]["hasResult"], json!(false));
    assert_eq!(summary["stepCount"], json!(1));
}

#[test]
fn an_unrouted_agent_gets_its_own_flow() {
    // A threadless frame from another agent still needs a row of its own, so its prose is not
    // dropped, and the row is a subagent nested under the root.
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: None,
        },
        HarnessEvent::Text {
            agent: "worker".to_owned(),
            delta: "subagent work".to_owned(),
            thread: None,
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: "primary work".to_owned(),
            thread: None,
        },
    ];
    let (ops, summary, display) = fold(&events, Some(("done", 1_004)));
    let sections = display["sections"].as_array().expect("two flows");

    assert_eq!(sections.len(), 2);
    assert_eq!(sections[0]["role"], json!("primary"));
    assert_eq!(sections[0]["agent"], json!("primary"));
    assert_eq!(sections[1]["role"], json!("subagent"));
    assert_eq!(sections[1]["agent"], json!("worker"));
    assert_eq!(sections[1]["parentSectionId"], json!("s0"));
    assert_eq!(sections[1]["id"], json!("s1"));
    assert_eq!(summary["sectionCount"], json!(2));
    assert_eq!(ops.len(), events.len());
    assert_eq!(
        all_op_names(&ops),
        vec![
            "section:open",
            "section:set",
            "section:open",
            "node:insert",
            "context:add",
            "node:append",
            "context:set",
            "node:insert",
            "context:add",
            "node:append",
            "context:set",
        ],
        "a threadless start from the agent that begins the turn opens the ROOT row (the first flow \
         this fold sees), and the other agent's stream opens a flow of its own so its prose is \
         never dropped"
    );
}

#[test]
fn a_zero_length_delta_is_recorded_and_renders_nothing() {
    let events = vec![
        HarnessEvent::Start {
            agent: "primary".to_owned(),
            model: "model-a".to_owned(),
            thread: root_thread(),
        },
        HarnessEvent::Text {
            agent: "primary".to_owned(),
            delta: String::new(),
            thread: root_thread(),
        },
    ];
    let (ops, _, display) = fold(&events, Some(("done", 1_004)));

    assert_eq!(op_names(&ops[1]), Vec::<&str>::new());
    assert_eq!(display["sections"][0]["nodes"], json!([]));
    assert_eq!(
        display["sections"][0].get("tail"),
        None,
        "an empty delta leaves no block behind, so the flow has no body to designate"
    );
}
