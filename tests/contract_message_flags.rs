//! P0 - the three CONTEXT FLAGS on `contract::HarnessMessage`: `included`,
//! `reasoningIncluded` and `messageId`.
//!
//! Everything pinned below is read off `packages/underload/harness` (the TS source of
//! record), not observed from this build, and the line is named next to each expectation:
//!
//! * `src/types/index.ts:181-303` - the union. `included` and `messageId` sit on ALL four
//!   roles (`:182`, `:196-200`, `:212`/`:241`, `:298-302`); `reasoningIncluded` is the
//!   ASSISTANT-only second axis (`:213-236`), because `reasoning` exists nowhere else.
//! * THE STORAGE RULE - only the literal `false` has a stored form. `src/core/snapshot.ts
//!   :230-240 applyIncluded` and `:255-265 applyReasoningIncluded` both compare
//!   `readSnapshotBoolean(...) === false` and write NOTHING otherwise, so `true` is accepted
//!   and NORMALIZED to the absent form; `:226-228` names the reason (a document that carried
//!   `true` must survive `restore -> snapshot` unchanged). The writers do the same from the
//!   other side: `src/server/api.ts:919-920` and `:944-945` `delete copy.<flag>` instead of
//!   writing `true`.
//! * `src/core/agent.ts:203-230 withIncluded` - the request projection ALWAYS strips
//!   `messageId`, keeps a `false` flag, and strips a `true` flag.
//!
//! The byte-for-byte assertions below are therefore exact TS documents: key order is the
//! declaration order of the TS union, with the `role` tag first.

use serde_json::{Value, json};

use typecrate_harness::contract::HarnessMessage;

/// Parse, re-serialize, and compare as STRINGS - which is the whole point: a key the runtime
/// must not write (`included: true`) or a key written in the wrong slot fails here even
/// though it would parse fine.
fn round_trip(document: &Value) -> String {
    let message: HarnessMessage =
        serde_json::from_value(document.clone()).expect("the TS-shaped document must deserialize");

    serde_json::to_string(&message).expect("a harness message always serializes")
}

/// Serialize the RUST side of a flag directly, bypassing the reader, to pin the writer rule
/// (`skip_serializing_if = "context_flag_absent"`): the key exists only for `Some(false)`.
fn serialized(message: &HarnessMessage) -> String {
    serde_json::to_string(message).expect("a harness message always serializes")
}

// ---------------------------------------------------------------------------
// The baseline: an entry with no flags keeps its exact pre-flag shape
// ---------------------------------------------------------------------------

#[test]
fn a_flag_free_entry_keeps_its_exact_pre_flag_document() {
    // `types/index.ts:202-208` - role, content, toolCalls, reasoning and nothing else.
    let assistant = HarnessMessage::Assistant {
        content: "hello".to_owned(),
        tool_calls: vec![],
        reasoning: Some("thinking".to_owned()),
        included: None,
        reasoning_included: None,
        message_id: None,
    };

    assert_eq!(
        serialized(&assistant),
        r#"{"role":"assistant","content":"hello","reasoning":"thinking"}"#
    );

    // A `None` flag never prints, so every pre-flag transcript, snapshot and branch record
    // keeps its old bytes (`types/index.ts:126-129`).
    let system = HarnessMessage::System {
        content: "be terse".to_owned(),
        included: None,
        message_id: None,
    };

    assert_eq!(
        serialized(&system),
        r#"{"role":"system","content":"be terse"}"#
    );
}

// ---------------------------------------------------------------------------
// `false` is the only visible value, and it round-trips byte-for-byte
// ---------------------------------------------------------------------------

#[test]
fn an_assistant_document_with_false_flags_round_trips_byte_for_byte() {
    // The TS spelling of a thinking-withheld turn, in the order the union declares it:
    // role, content, toolCalls, reasoning, included, reasoningIncluded, messageId.
    let document = json!({
        "role": "assistant",
        "content": "done",
        "toolCalls": [{ "id": "call-1", "name": "read", "arguments": "{\"path\":\"a\"}" }],
        "reasoning": "because",
        "included": false,
        "reasoningIncluded": false,
        "messageId": "msg-7",
    });

    // The expected string is spelled out rather than derived from the fixture: a
    // `serde_json::Value` map is BTreeMap-ordered, so re-serializing the fixture would print
    // ALPHABETICAL keys and prove nothing about the contract's key order.
    #[rustfmt::skip]
    let expected = r#"{"role":"assistant","content":"done","toolCalls":[{"id":"call-1","name":"read","arguments":"{\"path\":\"a\"}"}],"reasoning":"because","included":false,"reasoningIncluded":false,"messageId":"msg-7"}"#;

    assert_eq!(round_trip(&document), expected);
}

#[test]
fn false_flags_survive_a_whole_transcript_round_trip() {
    // The exact document a snapshot carries for one withheld unit: the assistant entry plus
    // its result, both flagged (`types/index.ts:135-139` - the exclusion UNIT).
    let transcript = json!([
        { "role": "system", "content": "sys", "included": false, "messageId": "msg-1" },
        { "role": "user", "content": "ask", "messageId": "msg-2" },
        {
            "role": "assistant",
            "content": "",
            "toolCalls": [{ "id": "call-1", "name": "read", "arguments": "{}" }],
            "included": false,
            "messageId": "msg-3",
        },
        {
            "role": "tool",
            "content": "text",
            "toolCallId": "call-1",
            "name": "read",
            "included": false,
            "messageId": "msg-4",
        },
        {
            "role": "assistant",
            "content": "answer",
            "reasoning": "think",
            "reasoningIncluded": false,
            "messageId": "msg-5",
        },
    ]);

    let messages: Vec<HarnessMessage> = serde_json::from_value(transcript.clone()).unwrap();
    let rewritten = serde_json::to_value(&messages).unwrap();

    assert_eq!(rewritten, transcript);
}

#[test]
fn a_hand_set_true_flag_is_never_written_to_a_document() {
    // TS never stores `true`: re-enabling DELETES the key (`api.ts:919-920`, `:944-945`). The
    // reader normalizes it, and the writer drops it too, so a Rust caller that sets
    // `Some(true)` by hand still produces the canonical absent form.
    let message = HarnessMessage::User {
        content: "hi".to_owned(),
        attachments: vec![],
        included: Some(true),
        message_id: Some("msg-9".to_owned()),
    };

    assert_eq!(
        serialized(&message),
        r#"{"role":"user","content":"hi","messageId":"msg-9"}"#
    );
}

#[test]
fn a_true_flag_is_accepted_and_normalized_to_absent() {
    // `core/snapshot.ts:226-228` and `:247-249`: a document that carries `true` (a host that
    // round-trips its own JSON, or a hand-edited file) is ACCEPTED and normalized, so
    // `restore -> snapshot` returns the same document it got.
    let document = json!({
        "role": "assistant",
        "content": "x",
        "reasoning": "r",
        "included": true,
        "reasoningIncluded": true,
    });

    assert_eq!(
        round_trip(&document),
        r#"{"role":"assistant","content":"x","reasoning":"r"}"#
    );

    // Same rule on the other two roles that carry `included`.
    assert_eq!(
        round_trip(&json!({ "role": "user", "content": "u", "included": true })),
        r#"{"role":"user","content":"u"}"#
    );
    assert_eq!(
        round_trip(&json!({
            "role": "tool",
            "content": "t",
            "toolCallId": "call-1",
            "name": "read",
            "included": true,
        })),
        r#"{"role":"tool","content":"t","toolCallId":"call-1","name":"read"}"#
    );
}

// ---------------------------------------------------------------------------
// `messageId` is carried verbatim
// ---------------------------------------------------------------------------

#[test]
fn a_present_message_id_is_emitted_in_the_last_slot() {
    // `core/snapshot.ts:327-332` writes the id LAST so a flagged entry keeps `included`
    // before `messageId` in both directions, and `core/agent.ts:224-229` strips the key from
    // a projection copy WITHOUT touching the flags - so this type carries it and the
    // projection (a later wave) is what removes it from a request.
    let message = HarnessMessage::Tool {
        content: "out".to_owned(),
        tool_call_id: "call-1".to_owned(),
        name: "read".to_owned(),
        attachments: vec![],
        // Mechanical compile fix for the display-state wave: `outcome` is the new tool field
        // BEFORE `included` (`types/index.ts:293`); `None` is the unwritten `completed` form, so
        // the asserted byte order below is unchanged.
        outcome: None,
        included: Some(false),
        message_id: Some("msg-12".to_owned()),
    };

    assert_eq!(
        serialized(&message),
        r#"{"role":"tool","content":"out","toolCallId":"call-1","name":"read","included":false,"messageId":"msg-12"}"#
    );
    assert_eq!(message.message_id(), Some("msg-12"));
    assert!(message.is_withheld());
    assert!(!message.is_reasoning_withheld());
}

// ---------------------------------------------------------------------------
// Refusals and the assistant-only axis
// ---------------------------------------------------------------------------

#[test]
fn a_non_boolean_flag_is_refused() {
    // `core/snapshot.ts:237` / `:262` route both flags through `readSnapshotBoolean`, so a
    // string, a number, an object or a `null` is a hard error, never "absent = included".
    for value in [
        json!("false"),
        json!(0),
        json!(1),
        json!(["false"]),
        json!({}),
        Value::Null,
    ] {
        let document = json!({ "role": "user", "content": "u", "included": value });
        assert!(
            serde_json::from_value::<HarnessMessage>(document.clone()).is_err(),
            "a non-boolean included flag must be refused, got {document}"
        );

        let assistant_document =
            json!({ "role": "assistant", "content": "a", "reasoningIncluded": value });
        assert!(
            serde_json::from_value::<HarnessMessage>(assistant_document.clone()).is_err(),
            "a non-boolean reasoningIncluded flag must be refused, got {assistant_document}"
        );
    }
}

#[test]
fn a_reasoning_flag_on_a_non_assistant_entry_is_dropped_by_the_reader() {
    // `reasoningIncluded` is assistant-only (`types/index.ts:219-220`), and the type cannot
    // hold it on another role, so the value has nowhere to land: the entry reads as
    // unflagged and serializes back without the key.
    //
    // WHAT THIS PINS AND WHAT IT DOES NOT: serde's internally-tagged reader IGNORES the key
    // (this crate has no `deny_unknown_fields` - see `contract.rs`'s module header), while
    // TS's snapshot whitelist REFUSES it outright (`core/snapshot.ts:252-254`, `:420-430`
    // lists the key for the assistant case only). That refusal is a HANDLER-level job, like
    // every other TS refusal sentence (`api.ts:504-510`), and the transcript body is still an
    // untyped `Value` here (`api/transcript.rs`), so the check is recorded as follow-up work
    // rather than asserted at this boundary.
    for role in ["system", "user", "tool"] {
        let document = if role == "tool" {
            json!({
                "role": role,
                "content": "t",
                "toolCallId": "call-1",
                "name": "read",
                "reasoningIncluded": false,
            })
        } else {
            json!({ "role": role, "content": "c", "reasoningIncluded": false })
        };

        let message: HarnessMessage = serde_json::from_value(document).unwrap();

        assert!(!message.is_reasoning_withheld());
        assert_eq!(message.reasoning_included_flag(), None);
        assert!(
            !serialized(&message).contains("reasoningIncluded"),
            "a non-assistant entry must not re-emit the key"
        );
    }
}

#[test]
fn an_absent_flag_reads_as_included_on_every_role() {
    // The default-true half of the rule (`types/index.ts:127`): no key, no withholding.
    let documents = [
        json!({ "role": "system", "content": "s" }),
        json!({ "role": "user", "content": "u" }),
        json!({ "role": "assistant", "content": "a" }),
        json!({
            "role": "tool",
            "content": "t",
            "toolCallId": "call-1",
            "name": "read",
        }),
    ];

    for document in documents {
        let message: HarnessMessage = serde_json::from_value(document.clone()).unwrap();

        assert_eq!(message.included_flag(), None, "{document}");
        assert!(!message.is_withheld(), "{document}");
        assert_eq!(message.message_id(), None, "{document}");
        assert_eq!(serde_json::to_value(&message).unwrap(), document);
    }
}
