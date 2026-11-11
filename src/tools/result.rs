//! The result boundary every tool body passes through.
//!
//! TypeScript origins
//! ------------------
//!   * `core/result-budget.ts` - `MODEL_TOOL_RESULT_BUDGET_BYTES`,
//!     `MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES`, `boundToolResult` and
//!     `createToolResultTurnBudget` (the shrink loops and the envelope text are ported line
//!     for line, because the head/tail envelope a model may parse is asserted byte-for-byte
//!     by `result-budget.test.ts`, including the `note` sentence and the code-unit walk);
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
    HarnessAttachment, MODEL_TOOL_RESULT_BUDGET_BYTES, MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES,
    ToolMediaDescriptor, ToolUiBlock, ToolUiPanel,
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

/// The outcome of applying the budget (`result-budget.ts:126` `BoundedToolResult`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedResult {
    /// What goes on the wire: the exact input when it fits, otherwise the envelope.
    pub content: String,
    /// Whether the cap was applied, so a caller (the server, a test) can report it without
    /// re-measuring the string.
    pub truncated: bool,
    /// The UTF-8 byte length of the input the cap was applied to.
    pub original_bytes: usize,
}

/// The floor for an aggregate spill (`result-budget.ts:96` `SPILL_MIN_BUDGET_BYTES`).
/// Once a turn's headroom is down to a handful of bytes, spilling to exactly that many
/// produces an envelope whose OWN overhead no longer fits, i.e. a cut that saves nothing,
/// so a spilled result never gets a budget below this (it clears the ~250-byte overhead
/// and leaves room for a head preview plus the note).
const SPILL_MIN_BUDGET_BYTES: usize = 512;

/// Head/tail split of the preview allowance (`result-budget.ts:103-104`): three quarters
/// of the surviving budget goes to the beginning, where a structured result keeps its keys
/// and a log keeps its identification; the rest goes to the end, where the error, the
/// summary or the closing brace lives.
const HEAD_PREVIEW_RATIO: usize = 3;
const TAIL_PREVIEW_RATIO: usize = 1;

/// Why a result was cut (`result-budget.ts:148` `SpillReason`). Internal: it only selects
/// the wording of the note, so the two different caps that can bound one result are never
/// confused by the model reading the envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpillReason {
    /// The per-result cap (`MODEL_TOOL_RESULT_BUDGET_BYTES`).
    Cap,
    /// This turn's aggregate headroom (`MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES`).
    Aggregate,
}

/// Which call produced a result, so the envelope's `note` can name what to re-run
/// (`result-budget.ts:133` `ToolResultSource`). BOTH halves are optional on purpose: a
/// caller holding only an id or only a name still gets a useful address, and
/// [`source_phrase`] renders whichever half exists.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolResultSource {
    /// The tool name, e.g. `read`.
    pub name: Option<String>,
    /// The provider call id, e.g. `call-9`.
    pub call_id: Option<String>,
}

impl ToolResultSource {
    /// The fully named source the harness holds at every delivery site.
    #[must_use]
    pub fn named(name: &str, call_id: &str) -> Self {
        Self {
            name: Some(name.to_owned()),
            call_id: Some(call_id.to_owned()),
        }
    }
}

/// The address the model can act on (`result-budget.ts:186` `sourcePhrase`): naming the
/// tool and the call id is the whole point, because an envelope that says "600 KB were
/// omitted" without saying how to get them back is a worse version of the cut it replaces.
fn source_phrase(source: Option<&ToolResultSource>) -> String {
    let name = source
        .and_then(|value| value.name.as_deref())
        .filter(|value| !value.is_empty());
    let call_id = source
        .and_then(|value| value.call_id.as_deref())
        .filter(|value| !value.is_empty());

    match (name, call_id) {
        (None, None) => "the tool call".to_owned(),
        (None, Some(call_id)) => format!("the call {call_id}"),
        (Some(name), None) => format!("tool '{name}'"),
        (Some(name), Some(call_id)) => format!("tool '{name}' (call {call_id})"),
    }
}

/// The honest line: what was dropped, which cap did it, and where the rest is
/// (`result-budget.ts:204` `spillNote`). Numbers only - no prose that could drift with a
/// locale - because a model parses this and a test asserts it byte for byte.
#[allow(
    clippy::too_many_arguments,
    reason = "the note quotes every count it names"
)]
fn spill_note(
    original_bytes: usize,
    budget_bytes: usize,
    head_bytes: usize,
    tail_bytes: usize,
    omitted_bytes: usize,
    reason: SpillReason,
    aggregate_bytes: Option<usize>,
    source: Option<&ToolResultSource>,
) -> String {
    // For an aggregate spill `budgetBytes` is the HEADROOM the ledger handed out, so the
    // note attributes it to the per-turn cap rather than to a second per-result limit an
    // operator never configured.
    let cap = match reason {
        SpillReason::Aggregate => Some(aggregate_bytes).flatten().map_or_else(
            || format!("by a {budget_bytes}-byte cap"),
            |aggregate| {
                format!(
                    "by a {budget_bytes}-byte budget set by this turn's {aggregate}-byte per-turn cap"
                )
            },
        ),
        SpillReason::Cap => format!("by a {budget_bytes}-byte cap"),
    };

    format!(
        "Cut {omitted_bytes} of {original_bytes} bytes from the middle {cap}; kept a \
         {head_bytes}-byte head and a {tail_bytes}-byte tail. The full text is not in this \
         transcript: re-run {} with narrower arguments to fetch it.",
        source_phrase(source)
    )
}

/// A UTF-16 ruler over one string. TypeScript's `keep` counts `content.length`, i.e. UTF-16
/// code units, and slices by code unit; a byte-based port would stop at a different window
/// on multi-byte content (`result-budget.test.ts:184` pins `'é'.repeat(200)` at a 300-byte
/// budget to a 12-byte head / 6-byte tail, which only the code-unit walk produces).
///
/// A unit boundary may fall between the two units of an astral character. The reference
/// slices there and then drops the lone surrogate half (`trimBrokenSurrogate` /
/// `trimBrokenLeadSurrogate`, `result-budget.ts:175-185`); Rust strings cannot hold half a
/// code point, so the same rule is folded into the two lookups below: the head ends before
/// a split character (its lone high half would be trimmed) and the tail starts after one
/// (its lone low half would be trimmed).
struct UnitMap<'a> {
    content: &'a str,
    /// Byte offset of every character, plus the string length as a sentinel.
    char_starts: Vec<usize>,
    /// Code units before every character, plus the total as a sentinel.
    unit_starts: Vec<usize>,
}

impl<'a> UnitMap<'a> {
    fn new(content: &'a str) -> Self {
        let mut char_starts = Vec::with_capacity(content.len() + 1);
        let mut unit_starts = Vec::with_capacity(content.len() + 1);
        let mut units = 0_usize;

        for (byte, character) in content.char_indices() {
            char_starts.push(byte);
            unit_starts.push(units);
            units += character.len_utf16();
        }

        char_starts.push(content.len());
        unit_starts.push(units);

        Self {
            content,
            char_starts,
            unit_starts,
        }
    }

    /// Total UTF-16 code units, the ruler `keep` is measured on.
    fn total_units(&self) -> usize {
        self.unit_starts[self.unit_starts.len() - 1]
    }

    /// Byte index that ends the HEAD at or before `units` code units: every character whose
    /// units are fully below the cut is kept, a character the cut splits is dropped.
    fn head_end(&self, units: usize) -> usize {
        let kept = self.unit_starts[1..].partition_point(|&end| end <= units);

        self.char_starts[kept]
    }

    /// Byte index that starts the TAIL at or after `units` code units: a character the cut
    /// splits is skipped, so the tail never opens with a lone low surrogate.
    fn tail_start(&self, units: usize) -> usize {
        let skipped =
            self.unit_starts[..self.unit_starts.len() - 1].partition_point(|&start| start < units);

        self.char_starts[skipped]
    }
}

/// Builds the envelope for one candidate preview length `keep` (code units, split head/tail)
/// (`result-budget.ts:231` `envelope`). Recomputing the note each time is what keeps the
/// byte loop honest: the note quotes the byte counts of the previews, so it changes with
/// `keep` and can never claim a cut that did not happen. `preserve_order` keeps this the TS
/// key order (`truncated`, `originalBytes`, `budgetBytes`, `note`, `content`, `tail`).
#[allow(
    clippy::too_many_arguments,
    reason = "one candidate of the reference envelope"
)]
fn envelope(
    units: &UnitMap<'_>,
    keep: usize,
    original_bytes: usize,
    budget_bytes: usize,
    reason: SpillReason,
    aggregate_bytes: Option<usize>,
    source: Option<&ToolResultSource>,
) -> String {
    let total = keep.min(units.total_units());
    let head_units = total * HEAD_PREVIEW_RATIO / (HEAD_PREVIEW_RATIO + TAIL_PREVIEW_RATIO);
    let tail_units = total - head_units;

    let head = &units.content[..units.head_end(head_units)];
    // `tailKeep > 0` matters: a zero window must stay the empty string, not the whole text.
    let tail = if tail_units > 0 {
        &units.content[units.tail_start(units.total_units() - tail_units)..]
    } else {
        ""
    };

    let head_bytes = utf8_byte_length(head);
    let tail_bytes = utf8_byte_length(tail);
    // A multi-byte character trimmed at a boundary, or an extreme budget, can make this
    // momentarily negative; the reference floors it at zero (`result-budget.ts:253`).
    let omitted_bytes = original_bytes.saturating_sub(head_bytes + tail_bytes);

    json!({
        "truncated": true,
        "originalBytes": original_bytes,
        "budgetBytes": budget_bytes,
        "note": spill_note(
            original_bytes,
            budget_bytes,
            head_bytes,
            tail_bytes,
            omitted_bytes,
            reason,
            aggregate_bytes,
            source,
        ),
        "content": head,
        "tail": tail,
    })
    .to_string()
}

/// Shared implementation behind [`bound_tool_result`] and the per-turn ledger
/// (`result-budget.ts:265` `boundWithReason`).
fn bound_with_reason(
    content: &str,
    budget_bytes: usize,
    reason: SpillReason,
    aggregate_bytes: Option<usize>,
    source: Option<&ToolResultSource>,
) -> BoundedResult {
    let original_bytes = utf8_byte_length(content);

    // Small outputs are EXACT: no re-encoding, no normalization, no marker.
    if original_bytes <= budget_bytes {
        return BoundedResult {
            content: content.to_owned(),
            truncated: false,
            original_bytes,
        };
    }

    let units = UnitMap::new(content);

    // One code unit can never encode to less than one byte, so a code-unit slice of
    // `budget_bytes` is an upper bound on a preview pair that fits. JSON escaping of the
    // previews only makes the envelope longer, hence the shrink loops below.
    let mut keep = units.total_units().min(budget_bytes);
    let mut bounded = envelope(
        &units,
        keep,
        original_bytes,
        budget_bytes,
        reason,
        aggregate_bytes,
        source,
    );

    // Geometric shrink, then exact shrink: at most O(log n) + a few iterations, and it
    // terminates because every step strictly reduces `keep`. `Math.floor(keep * 0.9)` is
    // done with integers so a 300 KB result cannot drift on float rounding.
    while utf8_byte_length(&bounded) > budget_bytes && keep > 0 {
        keep = keep * 9 / 10;

        bounded = envelope(
            &units,
            keep,
            original_bytes,
            budget_bytes,
            reason,
            aggregate_bytes,
            source,
        );
    }

    while utf8_byte_length(&bounded) > budget_bytes && keep > 0 {
        keep -= 1;

        bounded = envelope(
            &units,
            keep,
            original_bytes,
            budget_bytes,
            reason,
            aggregate_bytes,
            source,
        );
    }

    // Extreme edge: the budget is smaller than the envelope's fixed overhead (~250 bytes,
    // most of it the note). Return the marker anyway - a truncated marker is more useful to
    // the model than silence, and the documented budgets are kilobytes.
    BoundedResult {
        content: bounded,
        truncated: true,
        original_bytes,
    }
}

/// Caps one model-facing result or result text to `budget_bytes` UTF-8 bytes
/// (`result-budget.ts:308` `boundToolResult`, without the `source` option).
#[must_use]
pub fn bound_tool_result(content: &str, budget_bytes: usize) -> BoundedResult {
    bound_tool_result_with_source(content, budget_bytes, None)
}

/// The same cap, naming the producing call in the envelope's `note`
/// (`result-budget.ts:308` `options.source`). The source only changes the wording of the
/// note, never the size rule, so a caller that cannot name the call gets the same cut with
/// a less specific line.
#[must_use]
pub fn bound_tool_result_with_source(
    content: &str,
    budget_bytes: usize,
    source: Option<&ToolResultSource>,
) -> BoundedResult {
    bound_with_reason(content, budget_bytes, SpillReason::Cap, None, source)
}

/// One turn's running tally of committed tool-result bytes
/// (`result-budget.ts:339` `ToolResultTurnBudget`).
///
/// WHY A LEDGER RATHER THAN A BATCH SCAN: results are committed one at a time as their
/// calls settle, and a provider request must contain an answer for EVERY call of the block,
/// so a result can never be refused outright and an already-committed message is never
/// rewritten behind an event that was already reported. The first results of a turn keep
/// their per-result size; once the turn's aggregate is spent each later result is spilled
/// through the SAME envelope at a budget equal to what is left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolResultTurnBudget {
    /// Bytes committed into this turn so far (envelope included, UTF-8).
    committed: usize,
    /// The aggregate this ledger enforces (`0` = no aggregate guard).
    aggregate: usize,
    /// The per-result cap the ledger applies before the aggregate headroom.
    per_result: usize,
}

/// Builds a turn ledger on the published budgets
/// (`result-budget.ts:361` `createToolResultTurnBudget`).
#[must_use]
pub fn create_tool_result_turn_budget() -> ToolResultTurnBudget {
    ToolResultTurnBudget::new(
        MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES,
        MODEL_TOOL_RESULT_BUDGET_BYTES,
    )
}

impl ToolResultTurnBudget {
    /// A ledger with explicit budgets. A non-positive `aggregate_bytes` disables the
    /// aggregate guard entirely (the per-result cap still applies), which is the honest
    /// reading of a corrupt or deliberately disabled setting; a non-positive
    /// `per_result_bytes` falls back to the published per-result cap.
    #[must_use]
    pub fn new(aggregate_bytes: usize, per_result_bytes: usize) -> Self {
        Self {
            committed: 0,
            aggregate: aggregate_bytes,
            per_result: if per_result_bytes == 0 {
                MODEL_TOOL_RESULT_BUDGET_BYTES
            } else {
                per_result_bytes
            },
        }
    }

    /// Bytes committed into this turn so far.
    #[must_use]
    pub fn committed_bytes(&self) -> usize {
        self.committed
    }

    /// The aggregate this ledger enforces (`0` = no aggregate guard).
    #[must_use]
    pub fn aggregate_bytes(&self) -> usize {
        self.aggregate
    }

    /// Start the next turn's tally. A model request is the boundary.
    pub fn reset(&mut self) {
        self.committed = 0;
    }

    /// Bound ONE result for this turn and record what it cost
    /// (`result-budget.ts:378` `commit`). The per-result cap is applied FIRST and only then
    /// the aggregate headroom, so a result that fits both is byte-identical to
    /// [`bound_tool_result`]'s output.
    pub fn commit(
        &mut self,
        content: &str,
        budget_bytes: Option<usize>,
        source: Option<&ToolResultSource>,
    ) -> BoundedResult {
        // A caller may ask for a SMALLER per-result cap than the global one (a tool that
        // budgets its own output); the aggregate always applies.
        let requested = budget_bytes
            .filter(|budget| *budget > 0)
            .unwrap_or(self.per_result);
        let capped = bound_with_reason(content, requested, SpillReason::Cap, None, source);
        let size = utf8_byte_length(&capped.content);

        if self.aggregate == 0 || self.committed + size <= self.aggregate {
            self.committed += size;

            return capped;
        }

        // The turn's remaining headroom, floored so the envelope still fits inside it: this
        // is a soft bound, and the only alternative to overshooting is dropping a result,
        // which provider pairing forbids.
        let remaining = self.aggregate.saturating_sub(self.committed);
        let spill_budget = requested.min(SPILL_MIN_BUDGET_BYTES.max(remaining));

        // Nothing left to gain: the per-result cap is already the tighter bound, so keep the
        // capped form rather than a pointless re-cut.
        if spill_budget >= requested {
            self.committed += size;

            return capped;
        }

        let spilled = bound_with_reason(
            content,
            spill_budget,
            SpillReason::Aggregate,
            Some(self.aggregate),
            source,
        );
        self.committed += utf8_byte_length(&spilled.content);

        spilled
    }
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

    /// One envelope, parsed. The shape is a contract: a model may read it and a host may
    /// render it, so the key set is asserted, not assumed (`result-budget.test.ts:38`).
    fn envelope_of(content: &str) -> Value {
        serde_json::from_str(content).expect("a truncated result stays valid JSON")
    }

    /// The named source the harness holds at a delivery site (`ToolResultSource`).
    fn source(name: &str, call_id: &str) -> ToolResultSource {
        ToolResultSource::named(name, call_id)
    }

    // ── `describe('boundToolResult')` ────────────────────────────────────────────────────

    /// `result-budget.test.ts:44-56`: the default is 256 KiB of UTF-8, and a small result
    /// comes back with no marker and no re-encoding.
    #[test]
    fn the_budget_is_256_kib_and_a_small_result_is_byte_exact() {
        assert_eq!(MODEL_TOOL_RESULT_BUDGET_BYTES, 262_144);
        assert_eq!(
            bound_tool_result("hello", MODEL_TOOL_RESULT_BUDGET_BYTES),
            BoundedResult {
                content: "hello".to_owned(),
                truncated: false,
                original_bytes: 5
            }
        );
        assert_eq!(
            bound_tool_result("", MODEL_TOOL_RESULT_BUDGET_BYTES),
            BoundedResult {
                content: String::new(),
                truncated: false,
                original_bytes: 0
            }
        );

        // A JSON result is not re-encoded: the exact same string comes back.
        assert_eq!(
            bound_tool_result("{\"a\":1}", MODEL_TOOL_RESULT_BUDGET_BYTES),
            BoundedResult {
                content: "{\"a\":1}".to_owned(),
                truncated: false,
                original_bytes: 7
            }
        );
    }

    /// `result-budget.test.ts:58-64`: the aggregate is 2x the per-result cap, and that
    /// relationship IS the design, so it is pinned as arithmetic.
    #[test]
    fn the_per_turn_aggregate_is_published_next_to_the_per_result_cap() {
        assert_eq!(MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES, 524_288);
        assert_eq!(
            MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES,
            MODEL_TOOL_RESULT_BUDGET_BYTES * 2
        );
    }

    /// `result-budget.test.ts:66-73`: the boundary is INCLUSIVE - exactly at the budget is
    /// untouched, one byte over is truncated.
    #[test]
    fn a_result_exactly_at_the_budget_is_left_untouched() {
        let exact = "x".repeat(100);

        assert_eq!(
            bound_tool_result(&exact, 100),
            BoundedResult {
                content: exact.clone(),
                truncated: false,
                original_bytes: 100
            }
        );
        assert!(bound_tool_result(&"x".repeat(101), 100).truncated);
    }

    /// `result-budget.test.ts:75-101`: the whole envelope, byte for byte, naming the call.
    /// The shrink loop settles on a 74-byte head and a 25-byte tail out of a 400-byte
    /// budget; the numbers are the rule's output, not a range.
    #[test]
    fn an_over_budget_result_becomes_an_exact_envelope_naming_the_call() {
        let bounded =
            bound_tool_result_with_source(&"y".repeat(1_000), 400, Some(&source("read", "c1")));

        assert_eq!(
            bounded,
            BoundedResult {
                content: format!(
                    "{{\"truncated\":true,\"originalBytes\":1000,\"budgetBytes\":400,\"note\":\
                     \"Cut 901 of 1000 bytes from the middle by a 400-byte cap; kept a 74-byte head \
                     and a 25-byte tail. The full text is not in this transcript: re-run tool \
                     'read' (call c1) with narrower arguments to fetch it.\",\"content\":\"{}\",\"tail\":\"{}\"}}",
                    "y".repeat(74),
                    "y".repeat(25)
                ),
                truncated: true,
                original_bytes: 1_000,
            }
        );

        // Key set AND order, because a model may parse the envelope.
        let parsed = envelope_of(&bounded.content);

        assert_eq!(
            parsed
                .as_object()
                .expect("an object")
                .keys()
                .collect::<Vec<_>>(),
            vec![
                "truncated",
                "originalBytes",
                "budgetBytes",
                "note",
                "content",
                "tail"
            ]
        );
        assert_eq!(
            parsed["note"],
            "Cut 901 of 1000 bytes from the middle by a 400-byte cap; kept a 74-byte head and a 25-byte tail. The full text is not in this transcript: re-run tool 'read' (call c1) with narrower arguments to fetch it."
        );
        assert_eq!(parsed["content"], "y".repeat(74));
        assert_eq!(parsed["tail"], "y".repeat(25));
        assert_eq!(utf8_byte_length(&bounded.content), 392);
    }

    /// `result-budget.test.ts:104-124`: the manual tool route has no transcript and no call
    /// id, so the note says only "the tool call" - and the shorter note leaves a BIGGER
    /// preview (82 + 28 instead of 74 + 25). Both halves are exact.
    #[test]
    fn the_refetch_line_degrades_when_the_call_cannot_be_named() {
        let unnamed = envelope_of(&bound_tool_result(&"y".repeat(1_000), 400).content);

        assert_eq!(
            unnamed["note"],
            "Cut 890 of 1000 bytes from the middle by a 400-byte cap; kept a 82-byte head and a 28-byte tail. The full text is not in this transcript: re-run the tool call with narrower arguments to fetch it."
        );
        assert_eq!(unnamed["content"], "y".repeat(82));
        assert_eq!(unnamed["tail"], "y".repeat(28));

        // A partial identity is still an identity: each form names what it knows.
        let only_id = ToolResultSource {
            name: None,
            call_id: Some("c9".to_owned()),
        };

        assert_eq!(
            envelope_of(
                &bound_tool_result_with_source(&"y".repeat(1_000), 400, Some(&only_id)).content
            )["note"],
            "Cut 890 of 1000 bytes from the middle by a 400-byte cap; kept a 82-byte head and a 28-byte tail. The full text is not in this transcript: re-run the call c9 with narrower arguments to fetch it."
        );

        let only_name = ToolResultSource {
            name: Some("grep".to_owned()),
            call_id: None,
        };

        assert_eq!(
            envelope_of(
                &bound_tool_result_with_source(&"y".repeat(1_000), 400, Some(&only_name)).content
            )["note"],
            "Cut 890 of 1000 bytes from the middle by a 400-byte cap; kept a 82-byte head and a 28-byte tail. The full text is not in this transcript: re-run tool 'grep' with narrower arguments to fetch it."
        );
    }

    /// `result-budget.test.ts:126-143`: the reason a head/tail split replaces the old
    /// mid-string cut - a large `read` answer keeps its opening structure AND its trailing
    /// continuation fields.
    #[test]
    fn both_ends_of_a_json_document_survive_the_cut() {
        let content =
            json!({ "files": [{ "path": "a.ts", "content": "q".repeat(2_000) }], "nextLine": 42 })
                .to_string();
        let bounded = bound_tool_result_with_source(&content, 1_000, Some(&source("read", "r1")));
        let envelope = envelope_of(&bounded.content);

        assert_eq!(bounded.original_bytes, utf8_byte_length(&content));
        assert!(
            envelope["content"]
                .as_str()
                .unwrap_or_default()
                .starts_with("{\"files\":[{\"path\":\"a.ts\",\"content\":\"qq")
        );
        assert!(
            envelope["tail"]
                .as_str()
                .unwrap_or_default()
                .ends_with("],\"nextLine\":42}")
        );
        assert_eq!(
            envelope["note"],
            "Cut 1398 of 2054 bytes from the middle by a 1000-byte cap; kept a 492-byte head and a 164-byte tail. The full text is not in this transcript: re-run tool 'read' (call r1) with narrower arguments to fetch it."
        );
    }

    /// `result-budget.test.ts:145-163`: `floor(262144 * 0.9)` = 235 929 code units is the
    /// first candidate that fits, split 176 946 head / 58 983 tail.
    #[test]
    fn a_huge_result_is_capped_with_an_exact_preview_pair() {
        let bounded = bound_tool_result_with_source(
            &"z".repeat(300_000),
            MODEL_TOOL_RESULT_BUDGET_BYTES,
            Some(&source("dump", "c1")),
        );

        assert!(bounded.truncated);
        assert_eq!(bounded.original_bytes, 300_000);
        assert_eq!(
            envelope_of(&bounded.content),
            json!({
                "truncated": true,
                "originalBytes": 300_000,
                "budgetBytes": 262_144,
                "note": "Cut 64071 of 300000 bytes from the middle by a 262144-byte cap; kept a 176946-byte head and a 58983-byte tail. The full text is not in this transcript: re-run tool 'dump' (call c1) with narrower arguments to fetch it.",
                "content": "z".repeat(176_946),
                "tail": "z".repeat(58_983),
            })
        );
        assert_eq!(utf8_byte_length(&bounded.content), 236_241);
    }

    /// `result-budget.test.ts:165-182`: the regression the 256 KiB cap exists for. `read`
    /// budgets its own output at this cap on the SERIALIZED form, so a read that fills its
    /// budget must arrive here byte-exact instead of losing its continuation fields.
    #[test]
    fn a_full_budget_read_result_arrives_untruncated() {
        let content = json!({
            "files": [{ "content": "x".repeat(MODEL_TOOL_RESULT_BUDGET_BYTES - 200) }],
            "truncated": true,
        })
        .to_string();

        assert!(utf8_byte_length(&content) <= MODEL_TOOL_RESULT_BUDGET_BYTES);
        assert_eq!(
            bound_tool_result(&content, MODEL_TOOL_RESULT_BUDGET_BYTES),
            BoundedResult {
                content: content.clone(),
                truncated: false,
                original_bytes: utf8_byte_length(&content),
            }
        );
    }

    /// `result-budget.test.ts:184-199`: 200 `é` is 400 bytes over a 300-byte budget. The
    /// code-unit walk, not a byte walk, is what lands on the 6-character head and the
    /// 3-character tail, and the envelope fills the budget exactly.
    #[test]
    fn a_multi_byte_character_is_never_split_at_either_cut() {
        let bounded = bound_tool_result(&"é".repeat(200), 300);

        assert_eq!(
            envelope_of(&bounded.content),
            json!({
                "truncated": true,
                "originalBytes": 400,
                "budgetBytes": 300,
                "note": "Cut 382 of 400 bytes from the middle by a 300-byte cap; kept a 12-byte head and a 6-byte tail. The full text is not in this transcript: re-run the tool call with narrower arguments to fetch it.",
                "content": "é".repeat(6),
                "tail": "é".repeat(3),
            })
        );
        assert_eq!(utf8_byte_length(&bounded.content), 300);
    }

    /// `result-budget.test.ts:201-214`: a cut between the two UTF-16 units of U+1F600 would
    /// leave a lone surrogate on either side; both halves are dropped, so the previews stay
    /// exact substrings of the source.
    #[test]
    fn a_split_surrogate_pair_is_dropped_from_the_head_and_the_tail() {
        let content = format!("{}{}{}", "😀".repeat(50), "x".repeat(400), "😀".repeat(50));
        let bounded = bound_tool_result(&content, 400);
        let envelope = envelope_of(&bounded.content);

        assert_eq!(envelope["content"], "😀".repeat(21));
        assert_eq!(envelope["tail"], "😀".repeat(7));
        assert!(content.starts_with(envelope["content"].as_str().unwrap()));
        assert!(content.ends_with(envelope["tail"].as_str().unwrap()));
        assert_eq!(utf8_byte_length(&bounded.content), 395);
    }

    /// `result-budget.test.ts:216-227`: the documented extreme edge - a budget below the
    /// envelope's fixed overhead still returns the marker, because a truncation notice beats
    /// silence.
    #[test]
    fn a_budget_below_the_envelope_overhead_still_returns_the_marker() {
        assert_eq!(
            bound_tool_result("abc", 0),
            BoundedResult {
                content: "{\"truncated\":true,\"originalBytes\":3,\"budgetBytes\":0,\
                          \"note\":\"Cut 3 of 3 bytes from the middle by a 0-byte cap; kept a 0-byte head \
                          and a 0-byte tail. The full text is not in this transcript: re-run the tool call \
                          with narrower arguments to fetch it.\",\"content\":\"\",\"tail\":\"\"}"
                    .to_owned(),
                truncated: true,
                original_bytes: 3,
            }
        );
    }

    /// `result-budget.test.ts:229-234`: the budget is bytes, because the wire cost of a
    /// transcript is bytes - one emoji is four of them.
    #[test]
    fn the_budget_is_measured_in_bytes_not_code_units() {
        assert_eq!(utf8_byte_length("abc"), 3);
        assert_eq!(utf8_byte_length("é"), 2);
        assert_eq!(utf8_byte_length("😀"), 4);
    }

    // ── `describe('createToolResultTurnBudget')` ─────────────────────────────────────────

    /// `result-budget.test.ts:237-242`: the published window is the 512 KiB aggregate.
    #[test]
    fn a_turn_window_starts_empty_on_the_published_aggregate() {
        let turn = create_tool_result_turn_budget();

        assert_eq!(
            turn.aggregate_bytes(),
            MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES
        );
        assert_eq!(turn.committed_bytes(), 0);
    }

    /// `result-budget.test.ts:244-254`: the no-cost property - a turn under the aggregate
    /// must be byte-identical to the per-result behaviour, notes included.
    #[test]
    fn a_result_inside_the_aggregate_is_exactly_the_per_result_result() {
        let content = "q".repeat(5_000);
        let named = source("echo", "e1");
        let mut turn = ToolResultTurnBudget::new(2_048, 1_024);

        assert_eq!(
            turn.commit(&content, Some(1_024), Some(&named)),
            bound_tool_result_with_source(&content, 1_024, Some(&named))
        );
        assert_eq!(turn.committed_bytes(), 968);
    }

    /// `result-budget.test.ts:256-288`: a 2048-byte window with a 1024-byte per-result cap,
    /// four deliveries. a/b are capped per result and still inside the window, c goes in
    /// whole, d has 13 bytes of headroom left and therefore spills to the 512-byte floor.
    #[test]
    fn the_window_bounds_the_sum_and_spills_only_what_no_longer_fits() {
        let mut turn = ToolResultTurnBudget::new(2_048, 1_024);

        let a = turn.commit(
            &"a".repeat(1_500),
            Some(1_024),
            Some(&source("alpha", "a1")),
        );

        assert_eq!(
            envelope_of(&a.content)["note"],
            "Cut 830 of 1500 bytes from the middle by a 1024-byte cap; kept a 502-byte head and a 168-byte tail. The full text is not in this transcript: re-run tool 'alpha' (call a1) with narrower arguments to fetch it."
        );
        assert_eq!(turn.committed_bytes(), 968);

        let b = turn.commit(&"b".repeat(1_500), Some(1_024), Some(&source("beta", "b2")));

        assert_eq!(envelope_of(&b.content)["budgetBytes"], json!(1_024));
        assert_eq!(turn.committed_bytes(), 1_935);

        let c = turn.commit(&"c".repeat(100), Some(1_024), Some(&source("gamma", "c3")));

        assert_eq!(
            c,
            BoundedResult {
                content: "c".repeat(100),
                truncated: false,
                original_bytes: 100
            }
        );
        assert_eq!(turn.committed_bytes(), 2_035);

        let d = turn.commit(
            &"d".repeat(1_500),
            Some(1_024),
            Some(&source("delta", "d4")),
        );
        let spilled = envelope_of(&d.content);

        assert_eq!(spilled["budgetBytes"], json!(512));
        assert_eq!(
            spilled["note"],
            "Cut 1342 of 1500 bytes from the middle by a 512-byte budget set by this turn's 2048-byte per-turn cap; kept a 118-byte head and a 40-byte tail. The full text is not in this transcript: re-run tool 'delta' (call d4) with narrower arguments to fetch it."
        );
        // The spill is what fits; the window overshoots by less than one envelope, which is
        // the soft-bound rule stated in the module header.
        assert_eq!(utf8_byte_length(&d.content), 499);
        assert_eq!(turn.committed_bytes(), 2_534);
        assert!(turn.committed_bytes() - 2_048 < 600);
    }

    /// `result-budget.test.ts:290-308`: a provider request must answer every `tool_calls[]`
    /// entry, so the last delivery of an over-budget turn is spilled, never dropped.
    #[test]
    fn a_result_is_never_refused_because_pairing_needs_an_answer() {
        let mut turn = create_tool_result_turn_budget();
        let full = "r".repeat(MODEL_TOOL_RESULT_BUDGET_BYTES);

        assert!(!turn.commit(&full, None, None).truncated);
        assert!(!turn.commit(&full, None, None).truncated);
        assert_eq!(
            turn.committed_bytes(),
            MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES
        );

        let third = turn.commit(&"t".repeat(3_000), None, Some(&source("tail", "t3")));

        assert!(third.truncated);
        assert_eq!(third.original_bytes, 3_000);
        assert_eq!(envelope_of(&third.content)["budgetBytes"], json!(512));
        assert_eq!(utf8_byte_length(&third.content), 500);
        // The window is over by one envelope and nothing more.
        assert_eq!(turn.committed_bytes(), 524_788);
    }

    /// `result-budget.test.ts:310-320`: the ledger is a pure function of the committed total
    /// and the arrival order - no clock, no heuristics.
    #[test]
    fn the_same_deliveries_in_the_same_order_produce_the_same_bytes() {
        let script = |mut turn: ToolResultTurnBudget| -> Vec<String> {
            ["a", "b", "c", "d"]
                .iter()
                .map(|letter| {
                    turn.commit(
                        &(*letter).repeat(1_500),
                        Some(1_024),
                        Some(&source(letter, letter)),
                    )
                    .content
                })
                .collect()
        };

        assert_eq!(
            script(ToolResultTurnBudget::new(2_048, 1_024)),
            script(ToolResultTurnBudget::new(2_048, 1_024))
        );
    }

    /// `result-budget.test.ts:322-333`: a model request is the boundary, and a fresh window
    /// takes the full per-result cap again.
    #[test]
    fn a_reset_reopens_the_window() {
        let mut turn = ToolResultTurnBudget::new(2_048, 1_024);

        turn.commit(&"a".repeat(1_500), None, None);
        assert!(turn.committed_bytes() > 0);

        turn.reset();
        assert_eq!(turn.committed_bytes(), 0);
        assert_eq!(
            envelope_of(&turn.commit(&"a".repeat(1_500), None, None).content)["budgetBytes"],
            json!(1_024)
        );
    }

    /// `result-budget.test.ts:335-348`: a host that configures no aggregate wants "no
    /// aggregate cap", not "no tool results". `usize` cannot spell NaN or a negative, so the
    /// single corrupt sentinel reachable here is `0`, which is exactly what the ledger reads
    /// as "guard disabled".
    #[test]
    fn a_non_positive_aggregate_disables_only_the_aggregate_guard() {
        let mut turn = ToolResultTurnBudget::new(0, 1_024);

        assert_eq!(turn.aggregate_bytes(), 0);

        let bounded = turn.commit(&"e".repeat(1_500), None, None);

        assert_eq!(envelope_of(&bounded.content)["budgetBytes"], json!(1_024));
        assert_eq!(turn.committed_bytes(), 959);
    }

    /// `result-budget.test.ts:350-358`: a tool that budgets its own output (`read`, `grep`)
    /// passes its own cap, and the aggregate still applies on top of it.
    #[test]
    fn a_caller_supplied_cap_below_the_aggregate_is_honoured() {
        let mut turn = ToolResultTurnBudget::new(4_000, 10_000);
        let capped = turn.commit(&"k".repeat(1_500), Some(700), None);

        assert_eq!(envelope_of(&capped.content)["budgetBytes"], json!(700));
        assert!(utf8_byte_length(&capped.content) <= 700);
        assert_eq!(turn.committed_bytes(), 658);
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
