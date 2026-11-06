//! The result boundary every tool body passes through.
//!
//! TypeScript origins
//! ------------------
//!   * `core/result-budget.ts` - `MODEL_TOOL_RESULT_BUDGET_BYTES`, `boundToolResult`
//!     (the shrink loops are ported line for line, because the envelope a model may
//!     parse is asserted byte-for-byte by `result-budget.test.ts`);
//!   * `core/tool-media.ts` - `unwrapToolResult`, `toolMediaOf`, `toolMediaDescriptors`
//!     (the `__toolMedia` brand, and the "peel both brands in any nesting order" rule);
//!   * `core/tool-ui.ts` - `toolUiOf`, `normalizeToolUiPanels` (the `__toolUi` brand);
//!   * `harness.ts:4988-5097` `deliverTask` - the ORDER those pieces run in at the one
//!     place a result is committed: unwrap, serialize, bound, then project panels/media
//!     onto the event;
//!   * `harness.ts` `serialize` - `undefined` is the empty string, a string result is
//!     the text itself, anything else is `JSON.stringify`.
//!
//! WHY THIS IS A SEPARATE MODULE
//! -----------------------------
//! Every tool would otherwise re-implement the same four steps, and the two places the
//! contract is easiest to get wrong (the budget envelope's key order, and a branded
//! envelope leaking `__toolUi` into the text the model reads) are exactly the ones a
//! shared helper keeps identical for all thirty-five tools.

use std::time::Instant;

use serde_json::{Map, Value, json};

use crate::contract::{
    HarnessAttachment, MODEL_TOOL_RESULT_BUDGET_BYTES, ToolMediaDescriptor, ToolUiBlock,
    ToolUiPanel,
};
use crate::ports::ToolExecOutcome;

/// The brand key of a `toolUi(content, panels)` result (`core/tool-ui.ts:78`).
pub const TOOL_UI_BRAND: &str = "__toolUi";

/// The brand key of a `toolMedia(content, attachments)` result (`core/tool-media.ts:88`).
pub const TOOL_MEDIA_BRAND: &str = "__toolMedia";

/// A tool body's answer: the value the model reads, or the failure message TS's
/// `HarnessError` would carry (`harness.ts:5021` wraps the latter into
/// `Tool "<name>" failed: <message>`).
pub type ToolBody = Result<Value, String>;

/// One tool failure, as the runtime reports it: NOT executed (a validation refusal, no
/// duration reported, `harness.ts:5107` `immediateResult`) or executed (a body that
/// failed, with its measured span).
#[derive(Debug)]
pub enum ToolFailure {
    /// Arguments were malformed / the tool was unknown / the call was refused before
    /// any work: the result text is the explanation and no duration is reported.
    Rejected(String),
    /// The body ran and failed: `deliverTask`'s `failed` branch.
    Failed(String),
}

impl ToolFailure {
    /// The refusal text either branch carries.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::Rejected(message) | Self::Failed(message) => message,
        }
    }
}

/// UTF-8 byte length, the unit the result budget is expressed in
/// (`result-budget.ts:67` `utf8ByteLength`).
#[must_use]
pub fn utf8_byte_length(value: &str) -> usize {
    value.len()
}

/// The outcome of applying the budget (`result-budget.ts:54` `BoundedToolResult`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedResult {
    /// What goes on the wire: the exact input when it fits, otherwise the envelope.
    pub content: String,
    pub truncated: bool,
    pub original_bytes: usize,
}

/// A JSON prefix that lost its trailing surrogate half is not the text the tool
/// produced (`result-budget.ts:75` `trimBrokenSurrogate`). Rust strings are UTF-8, so a
/// character boundary can never split a code point; the check is kept as an explicit
/// no-op statement rather than silently dropped.
fn trim_broken_surrogate(_value: &str) -> &str {
    _value
}

/// Wraps a truncated prefix in the envelope whose keys are stable because a model may
/// parse it (`result-budget.ts:83` `envelope`). `preserve_order` keeps this the TS key
/// order (`truncated`, `originalBytes`, `budgetBytes`, `content`).
fn envelope(prefix: &str, original_bytes: usize, budget_bytes: usize) -> String {
    json!({
        "truncated": true,
        "originalBytes": original_bytes,
        "budgetBytes": budget_bytes,
        "content": prefix,
    })
    .to_string()
}

/// Caps one model-facing result to `budget_bytes` UTF-8 bytes
/// (`result-budget.ts:96` `boundToolResult`).
///
/// Exact behaviour the TypeScript pins and this reproduces:
///   * an input at or under the budget is returned UNCHANGED - no re-encoding;
///   * an over-budget input becomes a small valid JSON envelope holding as much of the
///     beginning as still fits after the envelope's own overhead is counted;
///   * the prefix shrinks geometrically (`keep * 0.9`) and then one character at a
///     time, so the result is the LARGEST envelope that fits, deterministically.
#[must_use]
pub fn bound_tool_result(content: &str, budget_bytes: usize) -> BoundedResult {
    let original_bytes = utf8_byte_length(content);

    if original_bytes <= budget_bytes {
        return BoundedResult {
            content: content.to_owned(),
            truncated: false,
            original_bytes,
        };
    }

    // One UTF-16 code unit never encodes to less than one byte, so a code-unit slice of
    // `budget_bytes` is an UPPER bound on a byte-fitting prefix. Rust indexes by byte,
    // so the initial window is `min(len, budget)` snapped to a character boundary.
    let mut keep = content.len().min(budget_bytes);
    keep = floor_char_boundary(content, keep);

    let mut bounded = envelope(
        trim_broken_surrogate(&content[..keep]),
        original_bytes,
        budget_bytes,
    );

    // Geometric shrink, then the exact shrink: at most O(log n) + a few steps, and every
    // step strictly reduces `keep`, so this terminates.
    while utf8_byte_length(&bounded) > budget_bytes && keep > 0 {
        keep = floor_char_boundary(content, (keep as f64 * 0.9) as usize);
        bounded = envelope(
            trim_broken_surrogate(&content[..keep]),
            original_bytes,
            budget_bytes,
        );
    }

    while utf8_byte_length(&bounded) > budget_bytes && keep > 0 {
        keep = floor_char_boundary(content, keep - 1);

        if keep == 0 {
            bounded = envelope("", original_bytes, budget_bytes);
            break;
        }

        bounded = envelope(
            trim_broken_surrogate(&content[..keep]),
            original_bytes,
            budget_bytes,
        );
    }

    // The budget being smaller than the envelope's fixed overhead still returns the
    // marker (`result-budget.ts:123`: a truncation notice beats silence).
    BoundedResult {
        content: bounded,
        truncated: true,
        original_bytes,
    }
}

/// The largest character boundary at or below `target`. Rust's own
/// `str::floor_char_boundary` is unstable, so the port does it by hand; slicing must
/// never panic on a multi-byte character.
fn floor_char_boundary(value: &str, target: usize) -> usize {
    if target >= value.len() {
        return value.len();
    }

    let mut position = target;

    while position > 0 && !value.is_char_boundary(position) {
        position -= 1;
    }

    position
}

/// `serialize` (`harness.ts`): the string form of a tool value.
///
/// A JSON `null` stands in for the TypeScript `undefined` a tool may return, and both
/// produce the empty string - a tool that "returns nothing" must not put the text
/// `null` in front of the model.
#[must_use]
pub fn serialize_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// A tool result with its envelopes peeled off (`tool-media.ts:151` `unwrapToolResult`).
///
/// Both brands peel in either nesting order, and each peels at most once (matching TS:
/// `media` is only extracted while absent, `panels` only while absent).
#[derive(Clone, Debug, Default)]
pub struct Extracted {
    /// The plain value the model reads.
    pub content: Value,
    /// Present (possibly empty) only when a `__toolUi` envelope was peeled.
    pub panels: Option<Vec<ToolUiPanel>>,
    /// Present (possibly empty) only when a `__toolMedia` envelope was peeled.
    pub media: Option<Vec<HarnessAttachment>>,
}

/// True when `value` is an object carrying `brand` AND `content` - the two own-property
/// checks `toolUiOf`/`toolMediaOf` both require before treating a value as an envelope
/// (a forged brand with no `content` must travel untouched).
fn branded<'a>(value: &'a Value, brand: &str) -> Option<&'a Map<String, Value>> {
    let map = value.as_object()?;

    if map.contains_key(brand) && map.contains_key("content") {
        Some(map)
    } else {
        None
    }
}

/// Normalizes a `__toolUi` payload into panels, dropping what is not a panel
/// (`tool-ui.ts:192` `normalizeToolUiPanels`: unknown block types and malformed panels
/// are dropped rather than stringified).
fn normalize_panels(value: &Value) -> Vec<ToolUiPanel> {
    let Some(entries) = value.as_array() else {
        return Vec::new();
    };

    let mut panels: Vec<ToolUiPanel> = Vec::with_capacity(entries.len());

    for entry in entries {
        let Some(map) = entry.as_object() else {
            continue;
        };

        // A panel is id + slot + title + blocks; anything else is dropped whole, which
        // is exactly the fail-safe `normalizePanel` performs.
        let blocks_value = match map.get("blocks") {
            Some(Value::Array(blocks)) => blocks.clone(),
            _ => Vec::new(),
        };

        let blocks: Vec<ToolUiBlock> = blocks_value
            .iter()
            .filter_map(|block| serde_json::from_value::<ToolUiBlock>(block.clone()).ok())
            .collect();

        let id = match map.get("id") {
            Some(Value::String(text)) => text.clone(),
            _ => continue,
        };
        let title = match map.get("title") {
            Some(Value::String(text)) => text.clone(),
            _ => continue,
        };
        let slot = match map.get("slot") {
            Some(Value::String(text)) => text.clone(),
            _ => "right".to_owned(),
        };

        panels.push(ToolUiPanel {
            id,
            slot,
            title,
            blocks,
        });
    }

    panels
}

/// Decodes one media entry, rejecting (by dropping) an entry that is not a complete
/// attachment. The download-profile byte bounds are enforced by the `download` tool
/// itself, which is the boundary that produced the bytes.
fn normalize_media(value: &Value) -> Vec<HarnessAttachment> {
    let Some(entries) = value.as_array() else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| serde_json::from_value::<HarnessAttachment>(entry.clone()).ok())
        .collect()
}

/// Peels `__toolMedia` and `__toolUi` until the plain value remains
/// (`unwrapToolResult`).
#[must_use]
pub fn unwrap_tool_result(value: Value) -> Extracted {
    let mut content = value;
    let mut panels: Option<Vec<ToolUiPanel>> = None;
    let mut media: Option<Vec<HarnessAttachment>> = None;

    loop {
        if media.is_none()
            && let Some(envelope) = branded(&content, TOOL_MEDIA_BRAND)
        {
            media = Some(normalize_media(
                envelope.get(TOOL_MEDIA_BRAND).unwrap_or(&Value::Null),
            ));
            content = envelope.get("content").cloned().unwrap_or(Value::Null);
            continue;
        }

        if panels.is_none()
            && let Some(envelope) = branded(&content, TOOL_UI_BRAND)
        {
            panels = Some(normalize_panels(
                envelope.get(TOOL_UI_BRAND).unwrap_or(&Value::Null),
            ));
            content = envelope.get("content").cloned().unwrap_or(Value::Null);
            continue;
        }

        break;
    }

    Extracted {
        content,
        panels,
        media,
    }
}

/// The event/catalogue projection of media: identity and size, never the bytes
/// (`tool-media.ts:125` `toolMediaDescriptors`).
#[must_use]
pub fn tool_media_descriptors(media: &[HarnessAttachment]) -> Vec<ToolMediaDescriptor> {
    media
        .iter()
        .map(|attachment| ToolMediaDescriptor {
            id: attachment.id.clone(),
            kind: attachment.kind,
            name: attachment.name.clone(),
            mime: attachment.mime.clone(),
            size: attachment.size,
        })
        .collect()
}

/// Media the wire may carry as an untyped value (the ports seam keeps `Option<Value>`
/// so a tool can attach descriptors without a transcript owning the bytes).
#[must_use]
pub fn media_value(media: &[HarnessAttachment]) -> Value {
    Value::Array(
        tool_media_descriptors(media)
            .into_iter()
            .filter_map(|descriptor| serde_json::to_value(descriptor).ok())
            .collect(),
    )
}

/// The committed form of a SUCCESSFUL tool body: unwrap the envelopes, serialize, bound
/// to the model budget, and stamp the measured span (`deliverTask`'s `completed`
/// branch, `harness.ts:5031-5092`).
#[must_use]
pub fn completed_outcome(value: Value, started: Instant) -> ToolExecOutcome {
    let extracted = unwrap_tool_result(value);
    let bounded = bound_tool_result(
        &serialize_value(&extracted.content),
        MODEL_TOOL_RESULT_BUDGET_BYTES,
    );
    let media = extracted.media.unwrap_or_default();
    let panels = extracted.panels;

    ToolExecOutcome {
        ok: true,
        content: bounded.content,
        panels: panels.map(|entries| {
            Value::Array(
                entries
                    .into_iter()
                    .filter_map(|panel| serde_json::to_value(panel).ok())
                    .collect(),
            )
        }),
        media: if media.is_empty() {
            None
        } else {
            Some(media_value(&media))
        },
        error: None,
        duration_ms: elapsed_millis(started),
    }
}

/// The committed form of a FAILED tool body (`deliverTask`'s `failed` branch): the text
/// the model reads is the wrapped failure, still budgeted, and the measured span is the
/// run's.
#[must_use]
pub fn failed_outcome(name: &str, message: &str, started: Instant) -> ToolExecOutcome {
    let bounded = bound_tool_result(
        &format!("Tool \"{name}\" failed: {message}"),
        MODEL_TOOL_RESULT_BUDGET_BYTES,
    );

    ToolExecOutcome {
        ok: false,
        content: bounded.content.clone(),
        panels: None,
        media: None,
        error: Some(bounded.content),
        duration_ms: elapsed_millis(started),
    }
}

/// The committed form of a call that NEVER EXECUTED (`harness.ts:5107`
/// `immediateResult` with `outcome: 'rejected'`): the explanation is the content, and
/// `duration_ms` stays 0 to state "this call did not run", which is what the turn
/// engine turns into an absent `durationMs` on the event.
#[must_use]
pub fn rejected_outcome(message: &str) -> ToolExecOutcome {
    let bounded = bound_tool_result(message, MODEL_TOOL_RESULT_BUDGET_BYTES);

    ToolExecOutcome {
        ok: false,
        content: bounded.content.clone(),
        panels: None,
        media: None,
        error: Some(bounded.content),
        duration_ms: 0,
    }
}

/// Wall clock from a tool's entry point, clamped at zero (`deliverTask`'s
/// `Math.max(0, Date.now() - task.startedAt)`).
#[must_use]
pub fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Argument helpers shared by every body
//
// TS reads `context.parameters`, which the runtime has already unwrapped from the
// schema's single property (`tools/README`'s "single-property rule"). The Rust seam
// hands the parsed JSON through unchanged, so each body uses these helpers to read the
// shape it documents. A missing/mis-typed value is a REFUSAL with TS's own wording
// where TS states one.
// ---------------------------------------------------------------------------

/// The `parameters` value an argument object carries, applying the single-property rule
/// in reverse: a body that documents one property accepts either the wrapper object or
/// the bare value (TS's direct-call path passes the bare value).
#[must_use]
pub fn argument_value<'a>(arguments: &'a Value, property: &str) -> Option<&'a Value> {
    match arguments {
        Value::Object(map) => map.get(property),
        Value::Null => None,
        other => Some(other),
    }
}

/// Reads a required string argument.
///
/// # Errors
///
/// The refusal text naming the tool and the argument.
pub fn required_text(arguments: &Value, property: &str, tool_name: &str) -> Result<String, String> {
    match argument_value(arguments, property) {
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(format!(
            "{tool_name} {property} must be a string; received {}.",
            type_of(arguments)
        )),
    }
}

/// Reads an optional string argument.
#[must_use]
pub fn optional_text(arguments: &Value, property: &str) -> Option<String> {
    match argument_value(arguments, property) {
        Some(Value::String(text)) => Some(text.clone()),
        _ => None,
    }
}

/// Reads an integer argument with TS's bounded-refusal wording
/// (`read.tool.ts:267` `readInteger`).
///
/// # Errors
///
/// Names the field and the received value.
pub fn bounded_integer(
    arguments: &Value,
    property: &str,
    default: i64,
    minimum: i64,
    maximum: i64,
) -> Result<i64, String> {
    let Some(value) = argument_value(arguments, property) else {
        return Ok(default);
    };

    match value {
        Value::Number(number) => {
            let Some(parsed) = number.as_i64() else {
                return Err(format!(
                    "{property} must be an integer between {minimum} and {maximum}; received {number}."
                ));
            };

            if parsed < minimum || parsed > maximum {
                return Err(format!(
                    "{property} must be an integer between {minimum} and {maximum}; received {parsed}."
                ));
            }

            Ok(parsed)
        }
        Value::Null => Ok(default),
        other => Err(format!(
            "{property} must be an integer between {minimum} and {maximum}; received {}.",
            type_of(other)
        )),
    }
}

/// Reads an optional string list argument (a bare string is accepted as one entry,
/// which is what `gitDiff`'s `oneOf` contract states).
#[must_use]
pub fn string_list(arguments: &Value, property: &str) -> Option<Vec<String>> {
    match argument_value(arguments, property) {
        Some(Value::Array(entries)) => Some(
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        ),
        Some(Value::String(text)) => Some(vec![text.clone()]),
        _ => None,
    }
}

/// Reads a boolean flag (absent is false).
#[must_use]
pub fn flag(arguments: &Value, property: &str) -> bool {
    matches!(argument_value(arguments, property), Some(Value::Bool(true)))
}

/// The JavaScript-ish type name TS interpolates into its refusals.
#[must_use]
pub fn type_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::ToolUiKeyValueItem;

    #[test]
    fn a_result_at_the_budget_is_byte_exact() {
        let content = "x".repeat(MODEL_TOOL_RESULT_BUDGET_BYTES);
        let bounded = bound_tool_result(&content, MODEL_TOOL_RESULT_BUDGET_BYTES);

        assert!(!bounded.truncated);
        assert_eq!(bounded.original_bytes, MODEL_TOOL_RESULT_BUDGET_BYTES);
        assert_eq!(bounded.content, content);
    }

    #[test]
    fn an_over_budget_result_becomes_the_named_envelope() {
        let content = "y".repeat(MODEL_TOOL_RESULT_BUDGET_BYTES + 512);
        let bounded = bound_tool_result(&content, MODEL_TOOL_RESULT_BUDGET_BYTES);
        let parsed: Value = serde_json::from_str(&bounded.content).expect("the envelope is JSON");
        let expected_keys: Vec<&str> = vec!["truncated", "originalBytes", "budgetBytes", "content"];

        assert!(bounded.truncated);
        assert_eq!(bounded.original_bytes, MODEL_TOOL_RESULT_BUDGET_BYTES + 512);
        assert_eq!(
            parsed
                .as_object()
                .expect("an object")
                .keys()
                .collect::<Vec<_>>(),
            expected_keys
        );
        assert_eq!(parsed["truncated"], json!(true));
        assert_eq!(parsed["originalBytes"], json!(bounded.original_bytes));
        assert_eq!(parsed["budgetBytes"], json!(MODEL_TOOL_RESULT_BUDGET_BYTES));
        assert!(
            utf8_byte_length(&bounded.content) <= MODEL_TOOL_RESULT_BUDGET_BYTES,
            "the envelope itself must fit the budget"
        );
        let prefix = parsed["content"].as_str().expect("a string prefix");

        assert!(prefix.chars().all(|character| character == 'y'));

        // The shrink sequence is the contract, not "the largest prefix that fits":
        // `result-budget.ts:113-116` stops at the FIRST window that fits, which is
        // `floor(budget * 0.9)` after the full-budget window proved too long. The
        // envelope's fixed overhead here is 73 bytes, so the first fitting window is
        // 58982 characters of a 66048-byte result.
        assert_eq!(prefix.chars().count(), 58_982);
        assert_eq!(
            bounded.content,
            envelope(
                prefix,
                bounded.original_bytes,
                MODEL_TOOL_RESULT_BUDGET_BYTES
            )
        );
    }

    #[test]
    fn both_envelope_orders_unwrap_to_the_same_triple() {
        let attachment = json!({
            "id": "dl-1",
            "kind": "image",
            "marker": "[Image 1]",
            "name": "shot.png",
            "mime": "image/png",
            "size": 4,
            "data": "aGVsbG8=",
        });

        let ui_then_media = json!({
            TOOL_UI_BRAND: [{ "id": "p", "slot": "right", "title": "T", "blocks": [{ "type": "text", "text": "hi" }] }],
            "content": { TOOL_MEDIA_BRAND: [attachment.clone()], "content": "body" },
        });
        let media_then_ui = json!({
            TOOL_MEDIA_BRAND: [attachment],
            "content": { TOOL_UI_BRAND: [{ "id": "p", "slot": "right", "title": "T", "blocks": [{ "type": "text", "text": "hi" }] }], "content": "body" },
        });

        let left = unwrap_tool_result(ui_then_media);
        let right = unwrap_tool_result(media_then_ui);

        assert_eq!(left.content, json!("body"));
        assert_eq!(right.content, json!("body"));
        assert_eq!(left.panels, right.panels);
        assert_eq!(left.media, right.media);
        let left_panels = left.panels.clone().expect("one panel");
        let left_media = left.media.clone().expect("one attachment");

        assert_eq!(
            left_panels[0].blocks,
            vec![ToolUiBlock::Text {
                text: "hi".to_owned()
            }]
        );
        assert_eq!(left_media[0].marker, "[Image 1]".to_owned());
        assert_eq!(
            tool_media_descriptors(&left_media)[0],
            ToolMediaDescriptor {
                id: "dl-1".to_owned(),
                kind: crate::contract::AttachmentKind::Image,
                name: "shot.png".to_owned(),
                mime: "image/png".to_owned(),
                size: 4,
            }
        );
    }

    #[test]
    fn a_forged_brand_without_content_travels_untouched() {
        let forged = json!({ TOOL_UI_BRAND: [], "other": 1 });
        let extracted = unwrap_tool_result(forged.clone());

        assert_eq!(extracted.content, forged);
        assert!(extracted.panels.is_none());
    }

    #[test]
    fn serialize_matches_the_script_value_rule() {
        assert_eq!(serialize_value(&Value::Null), "");
        assert_eq!(serialize_value(&json!("plain")), "plain");
        assert_eq!(serialize_value(&json!({ "a": 1 })), "{\"a\":1}");
        assert_eq!(serialize_value(&json!([1, 2])), "[1,2]");
    }

    #[test]
    fn a_rejected_call_reports_no_duration() {
        let outcome = rejected_outcome("no");

        assert!(!outcome.ok);
        assert_eq!(outcome.duration_ms, 0);
        assert_eq!(outcome.error.as_deref(), Some("no"));
    }

    #[test]
    fn a_key_value_panel_round_trips_through_the_contract_type() {
        let panels = normalize_panels(&json!([
            {
                "id": "timeNow",
                "slot": "right",
                "title": "Current time",
                "blocks": [
                    { "type": "keyValue", "items": [{ "label": "Unix", "value": "1" }] },
                    { "type": "unknown", "text": "dropped" }
                ]
            },
            { "id": "broken" }
        ]));

        assert_eq!(panels.len(), 1);
        assert_eq!(
            panels[0].blocks,
            vec![ToolUiBlock::KeyValue {
                items: vec![ToolUiKeyValueItem {
                    label: "Unix".to_owned(),
                    value: "1".to_owned()
                }]
            }]
        );
    }
}
