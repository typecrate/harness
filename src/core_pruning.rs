//! The PURE context rules: the tool-call fingerprint, the token estimate, canonical unit
//! grouping, and the progressive-compaction prune plan.
//!
//! TypeScript origin, one section per ported helper:
//!   * `packages/underload/harness/src/core/argument-fingerprint.ts` - `argumentFingerprint`
//!     and `toolCallFingerprint`;
//!   * `src/core/context-estimate.ts` - `transcriptChars` and `estimateContextTokens` (the
//!     plain `~4 chars/token` half; the provider-anchored half lives on the host side of the
//!     seam, see [`crate::plugin`] and the note on [`estimate_context_tokens`]);
//!   * `src/core/tool-outcome.ts` - the read rule, already owned by
//!     [`contract::HarnessMessage::stored_outcome`], restated here as [`is_error_outcome`];
//!   * `src/core/context-pruning.ts` - `groupUnits`, `resolvesPrunedFailure`,
//!     `isCandidateFacts` and the policy `planContextPruning`;
//!   * `src/types/index.ts:4037` - `HarnessContextEntry`, the entry shape the policy reads,
//!     ported as [`ContextEntry`] because this crate has no `types` module.
//!
//! WHY ONE SMALL PURE MODULE, AND WHY IT IS NOT IN `src/plugins.rs`
//! ----------------------------------------------------------------
//! Every helper here is a total function of its input: no I/O, no clock, no store, no hook
//! bus. That is what lets the whole policy be pinned by a table of literal expected values
//! (see the tests at the bottom, each one captured from the TypeScript original - see
//! `Temp/w2b/pure-probe.json` in the authoring notes). The shipped compaction plugin owns the
//! TRIGGER, the SETTINGS and the WRITES; this module owns the DECISION, exactly like the
//! split in `core/context-pruning.ts`'s header ("an `annotate` mode later becomes a second
//! WRITER over the same plan rather than a second policy").
//!
//! THE THREE THINGS THAT CANNOT BE PORTED NAIVELY
//! ----------------------------------------------
//! 1. **Character weight is UTF-16 code units, not bytes and not code points.** Every TS
//!    `.length` in the estimator is a UTF-16 count, so an emoji costs 2 here too
//!    ([`utf16_len`]). Counting `chars()` would under-estimate the whole emoji-heavy class of
//!    tool output by 2x on exactly the field the token floor reads.
//! 2. **Key order is sorted UTF-16 code units.** `Object.keys(value).sort()` is UTF-16
//!    ascending, which differs from Rust's byte-wise `str` ordering ONLY for characters above
//!    the BMP ordering against U+E000..U+FFFF. [`sort_values_utf16`] compares on
//!    `encode_utf16()` so the fingerprint of one transcript cannot depend on the port.
//! 3. **The fallback preserves the raw text.** A non-object JSON value (`[1, 2]`, `null`, `3`)
//!    and anything unparseable return the TRIMMED RAW text, never a re-serialization:
//!    `[1, 2]` keeps its inner space, because rewriting it would invent a call the model never
//!    sent (`argument-fingerprint.ts`, last lines).
//!
//! TOTALITY: nothing in this module panics. `serde_json`'s parse failure is the `catch` branch
//! of the TS `try`, and the JS `trim` surface is matched deliberately (see [`trim_js`]).
//!
//! KNOWN, BOUNDED DEVIATIONS (recorded here rather than silently)
//! ---------------------------------------------------------------
//!   * A number magnitude where JS switches to exponential spelling (`>= 1e21`, `< 1e-7`)
//!     prints in full via [`crate::plugins::format_number`]. A tool argument of that size is
//!     not a fingerprint the model can produce, and the deviation only renames a key - it can
//!     never make two different calls collide.
//!   * A lone UTF-16 surrogate cannot exist in a Rust `String`, so the `JSON.stringify`
//!     surrogate escape has no input here.
//!

use serde_json::{Map, Value};

use crate::contract::{HarnessMessage, HarnessToolOutcome};
use crate::plugins::format_number;

// ---------------------------------------------------------------------------
// The work identity (`core/argument-fingerprint.ts`)
// ---------------------------------------------------------------------------

/// The boundary between the name and the argument text inside a call fingerprint. NUL cannot
/// appear in a registry tool name, so the split point can never be forged by argument content:
/// `('a', 'b')` and `('a\0b', '')` cannot collide the way they would with a printable separator.
const FINGERPRINT_SEPARATOR: char = '\0';

/// The canonical identity of one tool call's `arguments` text.
///
/// Sorted-key compact JSON for a JSON OBJECT; the trimmed raw text for everything else,
/// including a well-formed array (`[1, 2]` is never rewritten to `[1,2]`). Never panics: an
/// unparseable text is a fact to preserve, not an error.
#[must_use]
pub fn argument_fingerprint(raw: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
        // Only an object has a key order to canonicalize; `null` and arrays are `object` to
        // `typeof` but have no order, and the TS wire guard excludes them the same way
        // (`transport/openai.ts:275-279`).
        if let Value::Object(fields) = parsed {
            return canonical_object(&fields);
        }
    }

    // Whitespace is the only thing the fallback normalizes - trimming cannot change meaning,
    // while re-serializing a non-object would invent a call the model never sent.
    trim_js(raw)
}

/// The match key for "the same work": tool name, NUL, argument fingerprint.
///
/// Group transcript tool calls by this value, NEVER by their ids - id reuse is refused before
/// anything executes, so a retry always arrives with a new id.
#[must_use]
pub fn tool_call_fingerprint(name: &str, args: &str) -> String {
    let mut key = String::with_capacity(name.len() + args.len() + 8);

    key.push_str(name);
    key.push(FINGERPRINT_SEPARATOR);
    key.push_str(&argument_fingerprint(args));

    key
}

/// Rebuild a parsed JSON object so every nested object serializes with sorted keys, then
/// serialize it compactly the way `JSON.stringify` does.
///
/// Arrays keep their element order (order IS data: `[{"b":1,"a":2}]` and `[{"a":2,"b":1}]` are
/// different lists), while every object inside them is canonicalized.
fn canonical_object(fields: &Map<String, Value>) -> String {
    let mut out = String::new();

    write_canonical_map(fields, &mut out);

    out
}

/// `{ "k":v, ... }` with the keys in sorted UTF-16 order, compact, no spaces.
fn write_canonical_map(fields: &Map<String, Value>, out: &mut String) {
    let mut sorted: Vec<(&String, &Value)> = fields.iter().collect();

    sort_values_utf16(&mut sorted);

    out.push('{');

    for (position, (key, value)) in sorted.iter().enumerate() {
        if position > 0 {
            out.push(',');
        }

        write_json_string(key, out);
        out.push(':');
        write_canonical_value(value, out);
    }

    out.push('}');
}

/// One value in canonical form. Numbers print with ECMAScript's `String(number)` spelling
/// (so `1e3` becomes `1000`, matching `JSON.stringify(JSON.parse('{"a":1e3}'))`).
fn write_canonical_value(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
        Value::String(text) => write_json_string(text, out),
        Value::Number(number) => match number.as_f64() {
            // `format_number` is this crate's ECMAScript `String(number)` (integer spelling for
            // an integral value), which is also what `JSON.stringify` prints.
            Some(float) => out.push_str(&format_number(float)),
            // A `Number` that is not an `f64` cannot exist (serde's `as_f64` covers every
            // variant), so this arm is unreachable - stated rather than panicked on.
            None => out.push_str(&number.to_string()),
        },
        Value::Array(items) => {
            out.push('[');

            for (position, item) in items.iter().enumerate() {
                if position > 0 {
                    out.push(',');
                }

                write_canonical_value(item, out);
            }

            out.push(']');
        }
        Value::Object(fields) => write_canonical_map(fields, out),
    }
}

/// `Array.prototype.sort()`'s default order: ascending UTF-16 code units, so `'A'` precedes
/// `'_'` precedes `'b'`. Total, locale-free, and identical on every host - which is the whole
/// point of a fingerprint. Rust's own `str` ordering is UTF-8 bytes, and the two differ for
/// astral characters ordered against U+E000..U+FFFF, so the comparison is stated explicitly.
fn sort_values_utf16(entries: &mut Vec<(&String, &Value)>) {
    entries.sort_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));
}

/// `JSON.stringify`'s escaping of one string: the quote, the backslash, the five shortcut
/// escapes, and `\u00xx` (lower-case hex) for every other control character. Everything else -
/// including non-ASCII text and `U+2028`/`U+2029` - is emitted RAW, exactly like ECMAScript.
fn write_json_string(text: &str, out: &mut String) {
    out.push('"');

    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            other if (other as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", other as u32));
            }
            other => out.push(other),
        }
    }

    out.push('"');
}

/// ECMAScript's `String.prototype.trim()`, which removes every `White_Space` code point AND
/// `U+FEFF` (the BOM), while Rust's `str::trim` does not consider `U+FEFF` whitespace. A stored
/// argument text that begins with a BOM therefore trims to the same key on both ports.
#[must_use]
pub fn trim_js(text: &str) -> String {
    text.trim_end_matches(is_js_whitespace)
        .trim_start_matches(is_js_whitespace)
        .to_owned()
}

/// The character class JS's `trim` removes: `White_Space` plus the BOM.
fn is_js_whitespace(ch: char) -> bool {
    ch == '\u{feff}' || ch.is_whitespace()
}

// ---------------------------------------------------------------------------
// The character weight (`core/context-estimate.ts`)
// ---------------------------------------------------------------------------

/// The estimator's divisor: the same `~4 characters/token` rule of thumb the UI context bar
/// uses, so a limit configured against the bar and the plugin's check agree with each other.
pub const CHARS_PER_TOKEN: usize = 4;

/// ECMAScript's `.length`: UTF-16 CODE UNITS. An emoji costs 2 here, exactly as it does in the
/// estimator this feeds, and a code-point count would under-weigh every emoji-heavy tool result.
#[must_use]
pub fn utf16_len(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

/// The character weight of a whole request - every message plus the leading instruction blocks.
///
/// This is the SAME pass [`estimate_context_tokens`] divides by 4, exposed because the
/// provider-anchored estimate has to be recorded in the same unit or the delta term would
/// compare bytes with inches.
///
/// WHAT IS COUNTED: every message's `content` text; an assistant turn's tool-call NAMES and raw
/// `arguments` text; a tool result's own `name` (it rides the wire for pairing); an
/// attachment's NAME and MIME type as text only - never its bytes, because providers charge
/// media per image/page and counting the base64 would overstate the transcript by orders of
/// magnitude.
///
/// WHAT IS NEVER COUNTED: `reasoning`. It is retained for replay and is not resent.
///
/// `instructions` is a slice because an agent's instructions are an array while a caller may
/// hold one joined string; ECMAScript accepts either and sums the same characters either way.
#[must_use]
pub fn transcript_chars(messages: &[HarnessMessage], instructions: &[String]) -> usize {
    let mut chars = 0;

    for instruction in instructions {
        chars += utf16_len(instruction);
    }

    for message in messages {
        chars += message_chars(message);
    }

    chars
}

/// The character weight of ONE transcript entry.
fn message_chars(message: &HarnessMessage) -> usize {
    match message {
        HarnessMessage::System { content, .. } => utf16_len(content),
        HarnessMessage::User {
            content,
            attachments,
            ..
        } => {
            let mut total = utf16_len(content);

            for attachment in attachments {
                total += utf16_len(&attachment.name) + utf16_len(&attachment.mime);
            }

            total
        }
        HarnessMessage::Assistant {
            content,
            tool_calls,
            ..
        } => {
            let mut total = utf16_len(content);

            // Tool-call arguments are provider-visible text and the single largest payload a
            // turn can carry (a big file written through a tool call).
            for call in tool_calls {
                total += utf16_len(&call.name) + utf16_len(&call.arguments);
            }

            total
        }
        HarnessMessage::Tool {
            content,
            name,
            attachments,
            ..
        } => {
            // A tool result carries its own name on the wire (pairing), and that name costs a
            // handful of characters per call - measured, not guessed.
            let mut total = utf16_len(content) + utf16_len(name);

            for attachment in attachments {
                total += utf16_len(&attachment.name) + utf16_len(&attachment.mime);
            }

            total
        }
    }
}

/// Estimated token count of a transcript, optionally plus leading instruction text: the plain
/// `ceil(chars / 4)` heuristic on its own, which is the number the UI context bar matches.
///
/// Empty input is exactly 0, so a caller can tell "nothing to send" from a rounding artifact.
///
/// The PROVIDER-ANCHORED variant (`estimateContextTokensHybrid`) is NOT here on purpose: it
/// needs the last reported `usage.input` of the run, which is host state. The seam hands the
/// plugin that number already resolved
/// (`crate::plugin::TurnControls::estimated_tokens`), and every measurement this module makes is
/// a DIFFERENCE of the same estimator over the same instructions, so the anchor cancels out of
/// it either way.
#[must_use]
pub fn estimate_context_tokens(messages: &[HarnessMessage], instructions: &[String]) -> f64 {
    // `div_ceil` on integers is `Math.ceil(chars / 4)` exactly: no float division, so a huge
    // transcript cannot land on a value the integer path would round differently.
    f64::from(
        u64::try_from(transcript_chars(messages, instructions).div_ceil(CHARS_PER_TOKEN))
            .unwrap_or(u64::MAX) as f64,
    )
}

// ---------------------------------------------------------------------------
// The outcome read (`core/tool-outcome.ts`)
// ---------------------------------------------------------------------------

/// Rule 3 of `tool-outcome.ts`: every outcome except `completed` is an error. The boolean is
/// derived, never stored - `isError` is not a persisted field, so this is the only projection
/// from one to the other.
#[must_use]
pub fn is_error_outcome(outcome: HarnessToolOutcome) -> bool {
    outcome != HarnessToolOutcome::Completed
}

/// The outcome set the contract fixes as the ONLY one that resolves anything
/// (`types/index.ts:234-251`): a `completed` answer answers the work; a `cancelled` one is
/// evidence the agent CHOSE to stop and resolves nothing.
pub const RESOLVING_OUTCOME: HarnessToolOutcome = HarnessToolOutcome::Completed;

/// The outcomes that make a unit a prune candidate: a real failure or a refusal. `completed` is
/// delivering something the model asked for, and `cancelled` is a healthy control outcome.
pub const PRUNE_CANDIDATE_OUTCOMES: [HarnessToolOutcome; 2] =
    [HarnessToolOutcome::Failed, HarnessToolOutcome::Rejected];

// ---------------------------------------------------------------------------
// Units and the prune plan (`core/context-pruning.ts`)
// ---------------------------------------------------------------------------

/// One entry of the CANONICAL transcript, as the `turn.before` context exposes it for
/// context-inclusion control (`HarnessContextEntry`, `types/index.ts:4037`).
///
/// Unlike the projected message list, this is the committed history ITSELF: an entry that is
/// disabled is still listed, because context control never DELETES a turn - it hides it from the
/// model, reversibly. A plugin that only saw the projection could not re-enable anything, since
/// the entries it would need are exactly the ones the projection left out.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextEntry {
    /// Index in the agent's canonical transcript at the moment this entry list was built.
    pub index: usize,
    /// The canonical message itself - read-only by contract: an inclusion change goes through
    /// the host's `set_included`, which cascades the unit and notifies persistence.
    pub message: HarnessMessage,
    /// EFFECTIVE unit inclusion: `false` when this entry, OR ANY OTHER MEMBER of its unit, is
    /// disabled. A live view of canonical state, not a snapshot - the host answers what the
    /// transcript says NOW, so a later handler sees what an earlier hook already switched.
    pub included: bool,
}

impl ContextEntry {
    /// THE DURABLE ADDRESS (`message.messageId`, hoisted in TS): unlike the index it survives a
    /// history rewrite, a snapshot/restore, and a whole run, so a candidate named in one round
    /// can be re-resolved in a later one.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.message.message_id()
    }
}

/// Every shape a unit can have, from the point of view of the rule - an OPEN set of classes
/// rather than a boolean, because the diagnostic has to say WHY a unit was kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitClass {
    /// Every committed result failed or was refused, and every call has an answer committed.
    Candidate,
    /// Every committed result `completed` - a live answer. Never a candidate; the class a
    /// resolver must be in.
    Answered,
    /// A call of the block has no committed result yet: in flight, or never answered.
    Unanswered,
    /// A `cancelled` result, or a block mixing healthy and failed answers: conservative keep.
    Mixed,
    /// A message that is not a tool-call block, or a tool result whose block is gone (an orphan,
    /// which has a `name` but whose `arguments` lived on the missing call). It carries no
    /// recorded work, so it is neither a candidate nor a resolver.
    Standalone,
}

/// One unit: the messages that move together, with the identity the rule needs.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextUnit {
    /// The unit's LEADER: the assistant message that asked for the calls, or the standalone
    /// message itself. Its canonical index - the runtime may be given ANY member (it cascades
    /// the unit), and the leader is the readable, stable one to name.
    pub leader_index: usize,
    /// Every member's canonical index, in canonical order.
    pub members: Vec<usize>,
    pub kind: UnitClass,
    /// One fingerprint per CALL the unit made. A call with no committed result still counts here
    /// - it is what makes a unit `Unanswered` while keeping the keys a resolver must match.
    pub keys: Vec<String>,
    /// One outcome per committed RESULT, in result order.
    pub outcomes: Vec<HarnessToolOutcome>,
    /// The unit's effective inclusion, read from the leader.
    pub included: bool,
    /// The leader's durable address, when the entry carries one.
    pub leader_message_id: Option<String>,
}

/// The shape [`resolves_pruned_failure`] needs about one unit: enough to classify it and to
/// compare work, nothing else. A STRUCT rather than a `ContextUnit` so the predicate is testable
/// straight from the resolution matrix rows.
#[derive(Debug, Clone, PartialEq)]
pub struct UnitFacts {
    pub outcomes: Vec<HarnessToolOutcome>,
    pub keys: Vec<String>,
    /// Whether the unit is currently sent to the model. A resolver that is itself withheld must
    /// not be the evidence for pruning anything else: the model would then see NEITHER the
    /// failure nor the answer.
    pub included: bool,
}

impl From<&ContextUnit> for UnitFacts {
    fn from(unit: &ContextUnit) -> Self {
        Self {
            outcomes: unit.outcomes.clone(),
            keys: unit.keys.clone(),
            included: unit.included,
        }
    }
}

/// The candidate half of the predicate: every committed result is a real failure or refusal, and
/// there IS at least one committed result.
#[must_use]
pub fn is_candidate_facts(facts: &UnitFacts) -> bool {
    !facts.outcomes.is_empty()
        && facts
            .outcomes
            .iter()
            .all(|outcome| PRUNE_CANDIDATE_OUTCOMES.contains(outcome))
}

/// True when `later` answers the failed work `earlier` attempted.
///
/// Every clause is a decision, not an implementation detail: `earlier` must be an
/// all-failed/refused unit; `later` must be an all-`completed`, still-included unit with at least
/// one result; and EVERY key of `earlier` must appear in `later`, so a block that asked for two
/// things is only resolved when the retry did both. Keys compare by [`tool_call_fingerprint`], so
/// a reordered-JSON retry matches and a different `offset` does not.
#[must_use]
pub fn resolves_pruned_failure(earlier: &UnitFacts, later: &UnitFacts) -> bool {
    if !is_candidate_facts(earlier) {
        return false;
    }

    if !later.included {
        return false;
    }

    if later.outcomes.is_empty() {
        return false;
    }

    if !later
        .outcomes
        .iter()
        .all(|outcome| *outcome == RESOLVING_OUTCOME)
    {
        return false;
    }

    let answered: std::collections::HashSet<&str> = later.keys.iter().map(String::as_str).collect();

    !earlier.keys.is_empty()
        && earlier
            .keys
            .iter()
            .all(|key| answered.contains(key.as_str()))
}

/// Group the canonical transcript into units.
///
/// Two passes, because a late background result is spliced back at its ORIGINAL position while
/// the block it answers may sit far above it: collect the answer ids per assistant block first,
/// then walk the transcript once more to attach each result to the block that asked for it.
///
/// This MIRRORS the runtime's own grouping (`harness.ts transcriptUnits`) rather than importing
/// it, exactly as TS does, and for the same reason: a divergence can only change WHICH entry is
/// named and how many tokens are accounted - the write still cascades the runtime's own unit, and
/// estimating a subset of members UNDER-counts the saving, which is the conservative direction
/// for the token floor.
#[must_use]
pub fn group_units(entries: &[ContextEntry]) -> Vec<ContextUnit> {
    // toolCallId -> the index of the leader entry that asked for it. First owner wins: a duplicate
    // id is refused before anything executes, so a second claim can only be a hand-written
    // transcript, and the earliest block is the honest owner.
    let mut owners: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();

    for entry in entries {
        for call_id in call_ids_of(&entry.message) {
            owners.entry(call_id).or_insert(entry.index);
        }
    }

    // Leader order is the entries' own order (a HashMap cannot reproduce it), so the leaders and
    // the standalone messages are collected as ordered lists instead.
    let mut leaders: Vec<usize> = Vec::new();
    let mut members: std::collections::HashMap<usize, Vec<usize>> =
        std::collections::HashMap::new();
    let mut standalone: Vec<usize> = Vec::new();

    for entry in entries {
        let call_ids = call_ids_of(&entry.message);

        if !call_ids.is_empty() {
            // An assistant block is its own leader, even when nothing answers it yet.
            if !members.contains_key(&entry.index) {
                members.insert(entry.index, Vec::new());
                leaders.push(entry.index);
            }

            continue;
        }

        if is_tool_entry(&entry.message) {
            let owner = tool_call_id_of(&entry.message).and_then(|call_id| owners.get(call_id));

            if let Some(leader_index) = owner {
                // Every leader is registered above, so the entry exists.
                if let Some(list) = members.get_mut(leader_index) {
                    list.push(entry.index);
                }

                continue;
            }

            // An ORPHAN result (its block is gone, e.g. after a host `replaceHistory`) is a
            // standalone unit: the runtime's own grouping treats it the same way, and the
            // projection is what decides whether the provider sees it.
        }

        standalone.push(entry.index);
    }

    let lookup: std::collections::HashMap<usize, &ContextEntry> =
        entries.iter().map(|entry| (entry.index, entry)).collect();

    let mut units: Vec<ContextUnit> = Vec::new();

    // Canonical order, so a caller may reason about "later" by position.
    for leader_index in leaders {
        let member_list = members.get(&leader_index).cloned().unwrap_or_default();

        if let Some(unit) = classify_unit(leader_index, &member_list, &lookup) {
            units.push(unit);
        }
    }

    for entry_index in standalone {
        if let Some(unit) = classify_unit(entry_index, &[entry_index], &lookup) {
            units.push(unit);
        }
    }

    // A STABLE sort by leader index: `Array.prototype.sort` is stable in the original, and two
    // units can never share a leader index, so the outcome is order-deterministic either way.
    units.sort_by_key(|unit| unit.leader_index);

    units
}

/// Whether an entry is a committed tool RESULT (`message.role === 'tool'`). The contract models
/// the transcript as an enum, so the role test is a shape test.
fn is_tool_entry(message: &HarnessMessage) -> bool {
    matches!(message, HarnessMessage::Tool { .. })
}

/// The tool-call ids an assistant block asked for (every other role has none).
fn call_ids_of(message: &HarnessMessage) -> Vec<&str> {
    match message {
        HarnessMessage::Assistant { tool_calls, .. } => {
            tool_calls.iter().map(|call| call.id.as_str()).collect()
        }
        _ => Vec::new(),
    }
}

/// The `toolCallId` of a tool result.
fn tool_call_id_of(message: &HarnessMessage) -> Option<&str> {
    match message {
        HarnessMessage::Tool { tool_call_id, .. } => Some(tool_call_id.as_str()),
        _ => None,
    }
}

/// The calls a leader made, each with its fingerprint.
fn calls_of(leader: &HarnessMessage) -> Vec<(String, String)> {
    match leader {
        HarnessMessage::Assistant { tool_calls, .. } => tool_calls
            .iter()
            .map(|call| {
                (
                    call.id.clone(),
                    tool_call_fingerprint(&call.name, &call.arguments),
                )
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Classify one unit from its leader and members. THE ORDER OF THESE CHECKS IS THE RULE.
fn classify_unit(
    leader_index: usize,
    member_indices: &[usize],
    lookup: &std::collections::HashMap<usize, &ContextEntry>,
) -> Option<ContextUnit> {
    let leader = *lookup.get(&leader_index)?;

    let members: Vec<&ContextEntry> = member_indices
        .iter()
        .filter_map(|index| lookup.get(index).copied())
        .collect();

    let calls = calls_of(&leader.message);

    // Outcomes come from the committed RESULTS only, in result order, read through the contract's
    // "absent means completed" rule (`core/tool-outcome.ts` rule 2).
    let outcomes: Vec<HarnessToolOutcome> = members
        .iter()
        .filter(|member| is_tool_entry(&member.message))
        .map(|member| member.message.stored_outcome())
        .collect();

    let kind = if calls.is_empty() {
        // A standalone message (or an orphan result whose block is gone): no work of its own to
        // be superseded, so neither a candidate nor a resolver.
        UnitClass::Standalone
    } else if outcomes.len() < calls.len() {
        // At least one call of the block has no committed result yet: in flight, or never
        // answered. The rule: never touch it.
        UnitClass::Unanswered
    } else if is_candidate_facts(&UnitFacts {
        outcomes: outcomes.clone(),
        keys: calls.iter().map(|(_, key)| key.clone()).collect(),
        included: true,
    }) {
        UnitClass::Candidate
    } else if outcomes.iter().all(|outcome| *outcome == RESOLVING_OUTCOME) {
        UnitClass::Answered
    } else {
        // Anything else: a `cancelled` result, or a mix of healthy and failed answers.
        UnitClass::Mixed
    };

    Some(ContextUnit {
        leader_index,
        members: members.iter().map(|member| member.index).collect(),
        kind,
        keys: calls.iter().map(|(_, key)| key.clone()).collect(),
        outcomes,
        included: leader.included,
        leader_message_id: leader.message_id().map(str::to_owned),
    })
}

/// Units, counted from the END of the transcript, that no pass may touch; how many units ONE pass
/// may disable; and the minimum estimated tokens a pass must withhold to be worth doing.
///
/// All three arrive as plugin settings, i.e. JS numbers, so they are `f64` here and are coerced
/// the same way the original coerces them: a non-finite or negative value behaves as 0.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PruneOptions {
    pub safety_units: f64,
    pub max_units: f64,
    pub min_tokens: f64,
}

/// A unit the policy wants withheld, with everything an operator needs to audit it.
#[derive(Debug, Clone, PartialEq)]
pub struct PruneCandidate {
    /// The entry to disable, and its durable address when it has one.
    pub leader_index: usize,
    pub leader_message_id: Option<String>,
    /// Every canonical index that will stop being sent.
    pub members: Vec<usize>,
    /// The outcomes that made this unit a candidate.
    pub outcomes: Vec<HarnessToolOutcome>,
    /// The fingerprints withheld, so a reviewer can see what work left the context.
    pub keys: Vec<String>,
    /// THE SUPERSEDING UNIT: a diagnostic must name what answered.
    pub resolver_index: usize,
    pub resolver_message_id: Option<String>,
    /// Estimated tokens this unit withholds, measured with this module's estimator.
    pub tokens: f64,
}

/// The plan. An empty `candidates` means "write nothing this turn".
#[derive(Debug, Clone, PartialEq)]
pub struct PrunePlan {
    pub candidates: Vec<PruneCandidate>,
    /// Estimated tokens the whole plan withholds (0 for an empty plan).
    pub withheld_tokens: f64,
    /// Every unit the scan considered - for the diagnostic and for tests that assert what was
    /// DELIBERATELY KEPT (an unanswered failure, a cancelled block, the tail horizon).
    pub considered_units: usize,
    /// Set when candidates existed but the whole set fell under `min_tokens`. The floor rejects
    /// the WHOLE pass rather than part of it: half a batch is the cache churn the floor exists to
    /// avoid, and a partial prune of an old conversation is harder to reason about than "nothing
    /// happened this turn".
    pub below_floor: bool,
}

/// The bounded number a pass option means: non-finite or negative behaves as 0.
fn bounded(value: f64) -> f64 {
    if value.is_finite() {
        value.max(0.0)
    } else {
        0.0
    }
}

/// Estimated tokens the given canonical indices withhold, measured by REMOVING them from the
/// transcript and re-running the SAME estimator. The delta is the number the floor needs, and
/// the instructions are constant on both sides, so they cancel out of it.
#[must_use]
pub fn withheld_tokens_of(
    messages: &[HarnessMessage],
    indices: &std::collections::HashSet<usize>,
) -> f64 {
    let total = estimate_context_tokens(messages, &[]);

    let kept: Vec<HarnessMessage> = messages
        .iter()
        .enumerate()
        .filter(|(position, _)| !indices.contains(position))
        .map(|(_, message)| message.clone())
        .collect();

    (total - estimate_context_tokens(&kept, &[])).max(0.0)
}

/// THE POLICY. Pure: same entries, same options, same plan.
#[must_use]
pub fn plan_context_pruning(entries: &[ContextEntry], options: &PruneOptions) -> PrunePlan {
    let units = group_units(entries);

    // The tail horizon, in UNITS, counted from the END. `first_protected` is the index of the
    // first unit no pass may touch: 6 units with a horizon of 2 protects units 4 and 5, and a
    // horizon of 0 protects NOTHING (the comparison is `>=`, so 0 puts the boundary past the
    // last unit rather than in front of all of them).
    let safety = bounded(options.safety_units);
    let considered_units = units.len();
    let first_protected = considered_units.saturating_sub(safety as usize).max(0);

    let messages: Vec<HarnessMessage> = entries.iter().map(|entry| entry.message.clone()).collect();

    // (candidate, resolver) pairs, in ascending position order.
    let mut matches: Vec<(usize, usize)> = Vec::new();

    for (index, unit) in units.iter().enumerate() {
        // Inside the tail horizon: untouchable, but still eligible to ANSWER an older failure
        // (the most recent success is the best evidence an old failure is stale), which is why
        // the horizon only filters the CANDIDATE side of the scan.
        if index >= first_protected {
            continue;
        }

        // Idempotence: an already-disabled unit is invisible to a second pass, so a repeat scan
        // produces no writes, no persistence notification and no save.
        if !unit.included {
            continue;
        }

        if unit.kind != UnitClass::Candidate {
            continue;
        }

        for later in index + 1..units.len() {
            if resolves_pruned_failure(&(&units[index]).into(), &(&units[later]).into()) {
                matches.push((index, later));
                break;
            }
        }
    }

    // The bound, applied to the OLDEST eligible matches first - the loop above is already in
    // ascending position order, and oldest-first is also the cache-friendly order: the prefix
    // that stays is the prefix the provider has already cached.
    //
    // NOTE `0` means NOTHING here (a JS `slice(0, 0)`), which is compaction's semantics. The
    // trimmer gives its own cap the opposite reading (`0` = unbounded) and applies it inside its
    // own planner, so the difference never reaches this function.
    let max_units = bounded(options.max_units);
    let selected: Vec<(usize, usize)> = matches.into_iter().take(max_units as usize).collect();

    let member_indices: std::collections::HashSet<usize> = selected
        .iter()
        .flat_map(|(unit_index, _)| units[*unit_index].members.iter().copied())
        .collect();

    let withheld_tokens = if selected.is_empty() {
        0.0
    } else {
        withheld_tokens_of(&messages, &member_indices)
    };

    let min_tokens = bounded(options.min_tokens);

    let candidates: Vec<PruneCandidate> = selected
        .iter()
        .map(|(unit_index, resolver_index)| {
            let unit = &units[*unit_index];
            let resolver = &units[*resolver_index];

            let unit_indices: std::collections::HashSet<usize> =
                unit.members.iter().copied().collect();

            PruneCandidate {
                leader_index: unit.leader_index,
                leader_message_id: unit.leader_message_id.clone(),
                members: unit.members.clone(),
                outcomes: unit.outcomes.clone(),
                keys: unit.keys.clone(),
                resolver_index: resolver.leader_index,
                resolver_message_id: resolver.leader_message_id.clone(),
                // The per-unit number for the diagnostic: the same estimator, this unit's members
                // only. It is the sum of a subset, so the plan's total (which removes the whole
                // set at once) is the authoritative saving.
                tokens: withheld_tokens_of(&messages, &unit_indices),
            }
        })
        .collect();

    if !candidates.is_empty() && withheld_tokens < min_tokens {
        return PrunePlan {
            candidates: Vec::new(),
            withheld_tokens,
            considered_units,
            below_floor: true,
        };
    }

    PrunePlan {
        candidates,
        withheld_tokens,
        considered_units,
        below_floor: false,
    }
}
