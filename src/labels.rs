//! Conversation label grammar and the per-code allocator.
//!
//! TypeScript origin: `packages/underload/harness/src/shared/agent-results.ts`
//!   * `AGENT_REFERENCE_LABEL_PATTERN` (`:131`) - the only label shape this server emits,
//!   * `normalizeAgentLabel` (`:144-150`) - accept a typed `#it1` or a bare label,
//!   * `AgentLabelParts` / `agentLabelParts` (`:158-173`) - the parsed label used to VALIDATE
//!     a persisted label (`persistence.ts:629-643` `readTurn.threadLabel` and `:796-816`
//!     `readLabelMap`) and to reserve its ordinal in an allocator,
//!   * `agentKeyOf` / `codeOf` (`:177-192`) - the two-letter code (also ported in `ids`),
//!   * `createAgentLabels` (`:230-262`) - the shared label allocator.
//!
//! WHY THIS LIVES BESIDE `ids` AND DOES NOT JUST CALL IT
//! -----------------------------------------------------
//! `ids::agent_code` mints a code; this module owns the GRAMMAR and the STATE. The durable
//! reader has to reject a label the allocator could never emit (a lowercase, `RR0`, or
//! free-text value would be adopted as an authoritative display name and break `#LABEL`
//! resolution - the exact failure `persistence.ts:623-628` documents), and the store has to
//! continue a numbering sequence across turns and restarts (`labelSeeds` /
//! `threadLabelHighWater` in `HarnessSessionRecord`). Both need the parts, not just the code.

use std::collections::BTreeMap;

use crate::ids;

/// The label grammar (`agent-results.ts:131`): exactly two ASCII LETTERS plus a run of
/// digits. Digits inside an agent id are ignored by the allocator, so every emitted label
/// satisfies this; it also keeps a typed issue number (`#1234`) from looking like a
/// reference.
pub const AGENT_REFERENCE_LABEL_PATTERN: &str = r"^[A-Z]{2}\d+$";

/// The ordinal half of a label must be at least 1 (`agentLabelParts`, `:170`): an ordinal is
/// a COUNT, so `RR0` is not a label this system produces even though the grammar accepts it.
pub const MIN_LABEL_ORDINAL: u64 = 1;

/// One parsed compact label (`AgentLabelParts`, `agent-results.ts:158-162`): the canonical
/// text, its two-letter code, and its occurrence ordinal.
///
/// `label` is the NORMALIZED text as it was written (digits kept as typed), which is what a
/// caller stores back: `readLabelMap` returns `parts.label`, so `rr007` becomes `RR007` while
/// the ordinal it reserves is 7.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLabelParts {
    /// The normalized label text, e.g. `RR4`.
    pub label: String,
    /// The two-letter code half, e.g. `RR`.
    pub code: String,
    /// The occurrence ordinal, always >= [`MIN_LABEL_ORDINAL`].
    pub ordinal: u64,
}

/// Normalizes one typed or persisted label (`normalizeAgentLabel`, `agent-results.ts:144-150`).
///
/// Trim, drop a leading `#` (the composer's typed spelling, `#it1`), trim, uppercase, then
/// require the grammar. `None` means "not a label this system can produce", which is why
/// callers reject rather than guess a near match.
#[must_use]
pub fn normalize_agent_label(value: &str) -> Option<String> {
    let label = value.trim().trim_start_matches('#').trim().to_uppercase();

    if matches_label_grammar(&label) {
        Some(label)
    } else {
        None
    }
}

/// Parses a label into its parts (`agentLabelParts`, `agent-results.ts:164-173`).
///
/// The grammar alone accepts `RR0`; the ordinal check below is what refuses it. An ordinal
/// that does not fit a `u64` is refused as well (TS `Number()` yields a non-integer there,
/// which the `Number.isInteger` guard rejects).
#[must_use]
pub fn agent_label_parts(value: &str) -> Option<AgentLabelParts> {
    let label = normalize_agent_label(value)?;

    let digits = &label[2..];
    let ordinal: u64 = digits.parse().ok()?;

    if ordinal < MIN_LABEL_ORDINAL {
        return None;
    }

    Some(AgentLabelParts {
        label: label.clone(),
        code: label[..2].to_string(),
        ordinal,
    })
}

/// The bare two-letter code of an agent id (`agentCodeOf` + `codeOf`, `agent-results.ts:187-201`).
///
/// Delegates to [`ids::agent_code`], which ports the rule verbatim: trimmed lowercase, ASCII
/// LETTERS only, first + last, uppercased, `AG` when no letter exists. Note the TS comment at
/// `:194-200`: a transcript label keys on the agent ID, so `research` reads `RH1` - the
/// display-title abbreviation (`Researcher` -> `RR`) is a different input and belongs to the
/// frontend, not to the durable grammar.
#[must_use]
pub fn agent_code(agent: &str) -> String {
    ids::agent_code(agent)
}

/// The canonical conversation label for one agent at one occurrence (`createAgentLabels.next`,
/// `agent-results.ts:242-248`): `<CODE><ordinal>`, e.g. `IT1` for `instruct`.
#[must_use]
pub fn thread_label(agent: &str, sequence: u64) -> String {
    ids::thread_label_for(agent, sequence)
}

/// `true` when `text` is exactly two ASCII letters followed by at least one ASCII digit
/// (`AGENT_REFERENCE_LABEL_PATTERN`). Hand-rolled rather than `regex` because the pattern is
/// trivial and the durable reader runs per field on every load.
fn matches_label_grammar(text: &str) -> bool {
    let mut chars = text.chars();

    let Some(first) = chars.next() else {
        return false;
    };
    let Some(second) = chars.next() else {
        return false;
    };

    if !first.is_ascii_uppercase() || !second.is_ascii_uppercase() {
        return false;
    }

    let digits: String = chars.collect();

    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// The shared label allocator (`createAgentLabels`, `agent-results.ts:230-262`).
///
/// Semantics, frozen by the TS header at `:216-229`:
///   * numbering is keyed by the TWO-LETTER CODE, not the full agent name, so id collisions
///     share one sequence (`research` -> `RH1`, `rhesearch` -> `RH2`) instead of silently
///     producing duplicate labels;
///   * [`LabelAllocator::next`] is called once per invocation, never per event;
///   * [`LabelAllocator::reserve`] adopts an EXPLICIT label's ordinal without minting a new
///     one (a restored conversation keeps its name and the next conversation is higher);
///   * [`LabelAllocator::seed`] CONTINUES a sequence and never lowers one, because the
///     reference namespace is the whole conversation and a late seed must not renumber a
///     label that was already minted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelAllocator {
    /// code -> last used occurrence number, in the order the counters were first touched.
    occurrences: BTreeMap<String, u64>,
}

impl LabelAllocator {
    /// A fresh allocator (`createAgentLabels()` with no initial map).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An allocator that starts from a persisted high-water map (`createAgentLabels(initial)`).
    #[must_use]
    pub fn with_seed(counters: &BTreeMap<String, u64>) -> Self {
        let mut allocator = Self::default();
        allocator.seed(counters);
        allocator
    }

    /// Mints the next label for one agent (`next`, `:242-248`).
    pub fn next(&mut self, agent: &str) -> String {
        let code = agent_code(agent);
        let occurrence = self.occurrences.get(&code).copied().unwrap_or(0) + 1;
        self.occurrences.insert(code.clone(), occurrence);

        format!("{code}{occurrence}")
    }

    /// Adopts one persisted/explicit label into the sequence (`agent-results.ts:223-225`
    /// "an explicit conversation label does not call `next()` but still reserves its
    /// ordinal"). Returns the ordinal that was reserved, or `None` when `label` is not a
    /// label this allocator can emit.
    pub fn reserve(&mut self, label: &str) -> Option<u64> {
        let parts = agent_label_parts(label)?;
        let current = self.occurrences.get(&parts.code).copied().unwrap_or(0);

        if parts.ordinal > current {
            self.occurrences.insert(parts.code.clone(), parts.ordinal);
        }

        Some(parts.ordinal)
    }

    /// Continues an existing sequence, raising every per-code counter to at least the seeded
    /// value (`seed` + `adopt`, `:233-237` and `:254-256`).
    pub fn seed(&mut self, counters: &BTreeMap<String, u64>) {
        for (code, used) in counters {
            let current = self.occurrences.get(code).copied().unwrap_or(0);

            if *used > current {
                self.occurrences.insert(code.clone(), *used);
            }
        }
    }

    /// Starts a fresh sequence (`reset`, `:250-252`).
    pub fn reset(&mut self) {
        self.occurrences.clear();
    }

    /// A detached snapshot of the per-code counters (`counters`, `:258-260`), i.e. the value
    /// `HarnessSessionRecord.label_high_water` / `thread_label_high_water` persist.
    #[must_use]
    pub fn counters(&self) -> BTreeMap<String, u64> {
        self.occurrences.clone()
    }
}
