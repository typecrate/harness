//! Id and label minting.
//!
//! TypeScript origins:
//!   * opaque instance ids - the TS host mints session ids as `session-<n>` and turn ids as
//!     opaque unique strings inside `src/server/sessions.ts`; the Rust port uses one UUID v4
//!     accessor for every instance id, because nothing in the contract parses an id back into
//!     parts (the API always treats `session_id` / `turn_id` / `request_id` as an opaque
//!     string).
//!   * `THREAD_HOST_PREFIX` - `src/core/thread.ts:36`, and `thread-<N>` as spelled by
//!     `src/server/workers/lane-identity.ts:294`.
//!   * display labels - `src/shared/agent-results.ts:177-192` (`agentKeyOf` + `codeOf`): the
//!     label of a conversation is its agent code plus an ordinal, e.g. `RR1` for `researcher`
//!     (`labels.test.ts` pins that exact sequence).

/// The prefix of a HOST conversation id (`core/thread.ts:36`); a persistent thread id is
/// `thread-<sequence>` and is never derived from model input.
pub const THREAD_HOST_PREFIX: &str = "thread-";

/// The code an id with no ASCII LETTER falls back to (`agent-results.ts:185`). A non-Latin
/// name cannot produce a stable two-letter code, so it shares one neutral sequence and the
/// real agent id stays available on the block.
pub const FALLBACK_LABEL_CODE: &str = "AG";

/// One opaque instance id: UUID v4, lowercase, hyphenated.
#[must_use]
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A turn id (`TurnResource.id`, `TaskReceipt.turn_id`).
#[must_use]
pub fn new_turn_id() -> String {
    new_id()
}

/// A session id (`SessionSummary.id`).
#[must_use]
pub fn new_session_id() -> String {
    new_id()
}

/// An owner-question id (`HarnessActionRequest.request_id`), which the RUNTIME generates and
/// never the model (`types/index.ts:865`).
#[must_use]
pub fn new_request_id() -> String {
    new_id()
}

/// The id of one HOST conversation: `thread-<n>` (`core/thread.ts:36` +
/// `workers/lane-identity.ts:294`).
#[must_use]
pub fn host_thread_id(sequence: u64) -> String {
    format!("{THREAD_HOST_PREFIX}{sequence}")
}

/// The two-letter display code of an agent id (`agent-results.ts:177-192`).
///
/// Rules, in the TS order: normalize to trimmed lowercase, keep ASCII LETTERS only (a digit
/// can never break the `[A-Z]{2}\d+` label grammar), take first + last, uppercase. A
/// one-letter id yields the same letter twice; an id with no ASCII letters falls back to
/// [`FALLBACK_LABEL_CODE`].
#[must_use]
pub fn agent_code(agent: &str) -> String {
    let letters: Vec<char> = agent
        .trim()
        .to_lowercase()
        .chars()
        .filter(|glyph| glyph.is_ascii_alphabetic())
        .collect();

    let Some(first) = letters.first().copied() else {
        return FALLBACK_LABEL_CODE.to_string();
    };
    let last = letters.last().copied().unwrap_or(first);

    format!("{first}{last}").to_uppercase()
}

/// The canonical display label of one conversation: its agent's code plus the per-code
/// ordinal (`labels.test.ts:195-217`, where three legacy turns then one conversation give
/// `RR1`/`RR2`/`RR3`/`RR4`).
///
/// Numbering is keyed by the TWO-LETTER CODE, not the full agent name, so id collisions share
/// one sequence (`research` -> `RH1`, `rhesearch` -> `RH2`) instead of silently producing
/// duplicate labels (`agent-results.ts:219-221`).
#[must_use]
pub fn thread_label_for(agent: &str, sequence: u64) -> String {
    format!("{}{sequence}", agent_code(agent))
}

/// The fallback spelling of [`thread_label_for`], for a caller that has an ordinal but no
/// agent name to code: the neutral `AG` sequence (`agent-results.ts:185`).
#[must_use]
pub fn thread_label(sequence: u64) -> String {
    format!("{FALLBACK_LABEL_CODE}{sequence}")
}
