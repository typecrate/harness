//! The wire contract: constants + every request/response model of `/v1/harness`.
//!
//! TypeScript origins (this file is a faithful port, not a redesign):
//!   * `packages/underload/harness/src/shared/harness-api.ts` - the generated contract
//!     (constants, REST resources, receipts, read models);
//!   * `packages/underload/harness/src/shared/harness-stream.ts` - `HarnessEventType`
//!     and the resource-kind vocabulary (only the constants are needed at this stage);
//!   * `packages/underload/harness/src/types/index.ts` - the domain payloads the
//!     contract reuses VERBATIM (`HarnessMessage`, `HarnessEvent`, `HarnessUsage`,
//!     `HarnessAttachment`, `HarnessActionRequest`, `HarnessThread`, ...);
//!   * `packages/underload/harness/src/server/protocol.ts` - the internal records.
//!
//! NAMING RULE (verbatim from the generated header of `harness-api.ts:5-10`): REST
//! resources, receipts and stream frames are **snake_case**, while the display subtree
//! and the domain payloads keep the **camelCase** spelling the projector and the domain
//! already use. A snake_case struct therefore carries `#[serde(rename_all =
//! "snake_case")]` (a no-op that documents intent) and a camelCase one carries
//! `#[serde(rename_all = "camelCase")]`. `Option<T>` + `skip_serializing_if` is how a TS
//! OPTIONAL field is "omitted when absent"; a TS optional array omits the key when it is
//! empty, so those use `skip_serializing_if = "Vec::is_empty"` as well.
//!
//! NO `deny_unknown_fields` anywhere: the TS server rejects unknown body fields at the
//! HANDLER level with its own message (`src/server/api.ts:504-510`), and that message is
//! produced by [`require_known_fields`] below. A schema-level rejection would print
//! serde's wording where the contract pins its own.
//!
//! Literal-typed TS fields (`deleted: true`, `ok: true`) are plain `bool` in Rust: the
//! wire value is identical and every constructor site names the value. Where the TS type
//! is a large recursive union (`DisplayNode`, `DisplayStep`, `TurnSection`, `DisplayTail`,
//! `TurnDisplayOp`, `HarnessBranchSummary`) this file uses `serde_json::Value`, because
//! the projector - not this contract - decides the members and the HTTP layer passes the
//! document through verbatim (`api-reads.ts` casts rather than restates).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::error::ApiError;

// ---------------------------------------------------------------------------
// Constants (`harness-api.ts:39-48`, `:197-213`, and the domain bounds quoted by
// `shared/attachments.ts` / `core/agent-limits.ts`)
// ---------------------------------------------------------------------------

/// The one API root. `/api` is gone: standalone and gateway-mounted deployments share
/// this path (`harness-api.ts:39`).
pub const HARNESS_API_ROOT: &str = "/v1/harness";

/// Negotiated WebSocket subprotocol advertised by `GET {root}/stream` (`harness-api.ts:42`).
pub const HARNESS_WS_SUBPROTOCOL: &str = "harness.v1";

/// Response header carrying the server epoch (`harness-api.ts:45`).
pub const HARNESS_EPOCH_HEADER: &str = "X-Harness-Epoch";

/// Response header carrying the revision a read landed at (`harness-api.ts:48`).
pub const HARNESS_REVISION_HEADER: &str = "X-Harness-Revision";

/// Paging bounds for every paginated route (`harness-api.ts:51` `HARNESS_PAGE_LIMIT`).
pub const PAGE_LIMIT_MIN: u64 = 1;
/// Contract default page size, used when `limit` is absent.
pub const PAGE_LIMIT_DEFAULT: u64 = 25;
/// Largest page any route serves; a bigger `limit` is a 400.
pub const PAGE_LIMIT_MAX: u64 = 100;

/// Ordinary request-body cap (`body_bytes`, `harness-api.ts:198`).
pub const MAX_BODY_BYTES: usize = 1_000_000;
/// Submit-body cap, generous because attachments carry base64 (`submit_body_bytes`).
pub const MAX_SUBMIT_BODY_BYTES: usize = 16 * 1024 * 1024;
/// Trimmed task text cap (`input_chars`).
pub const MAX_INPUT_CHARS: usize = 200_000;
/// Pending task budget per session (`pending_tasks`).
pub const MAX_PENDING_TASKS: usize = 100;
/// Persistent conversations per session (`thread_records`).
pub const MAX_THREAD_RECORDS: usize = 256;
/// Attachments per submitted turn (`attachments_per_turn`).
pub const MAX_ATTACHMENTS_PER_TURN: usize = 4;
/// Decoded bytes for ONE attachment (`attachment_bytes`).
pub const MAX_ATTACHMENT_BYTES: usize = 4 * 1024 * 1024;
/// Decoded bytes for one whole attachment list (`total_attachment_bytes`).
pub const MAX_TOTAL_ATTACHMENT_BYTES: usize = 8 * 1024 * 1024;
/// `#LABEL` references one turn may carry (`result_references`).
pub const MAX_RESULT_REFERENCES: usize = 16;
/// Owner questions waiting at once (`pending_actions`).
pub const MAX_PENDING_ACTIONS: usize = 4;
/// Steer audit entries per turn (`steer_records`).
pub const MAX_STEER_RECORDS: usize = 64;
/// Tools one session may create (`created_tools`).
pub const MAX_CREATED_TOOLS: usize = 32;
/// Model-facing tool-result text budget (`tool_result_bytes`) — **32,768**, at parity.
///
/// CLOSED 2026-11-27 (contract-parity stage C1, R4). This constant used to sit at 256 KiB
/// against TS's 32 KiB, which let 8× more tool output reach the model than the replacement
/// target does. The magnitude gap was closed in C1; the CURRENCY it is spent in was closed after
/// it (slice R2), so the two harnesses now agree on the number and on the unit. What still is not
/// byte-for-byte agreement of the MEASUREMENT is listed in item 2, so nobody reads parity as more
/// than it is.
///
/// 1. THE TYPESCRIPT FIGURE THIS MIRRORS (re-verified 2026-11-27): **32,768** bytes per result.
///    `MODEL_TOOL_RESULT_BUDGET_BYTES` is token-derived at
///    `packages/underload/harness/src/engine/core/result-budget.ts:239-240`
///    (`MODEL_TOOL_RESULT_BUDGET_TOKENS` 8,192 at `:201` × `MODEL_TOKEN_BYTES` 4 at `:171`).
///    TS publishes the figure it enforces:
///    `packages/underload/harness/src/engine/shared/harness-api.ts:218-234`
///    `HARNESS_SERVICE_LIMITS.tool_result_bytes: 32768`, mirrored in the generated
///    `src/engine/shared/harness-api.ts` and in
///    `packages/underload/harness/src/harness-server.swagger.yaml`
///    (`tool_result_bytes: 32768`). The TS invariant that the published figure equals the
///    enforced one is pinned there by `engine/shared/harness-api.test.ts:237`.
/// 2. THE CURRENCY, NOW THE SAME ONE AT BOTH ENDS: the gate charges ESCAPED PAYLOAD bytes
///    ([`crate::payload::model_payload_bytes`], the port of the reference's `modelPayloadBytes`,
///    whose vectors are ported from `engine/core/payload-bytes.test.ts`), and the file tools that
///    size their own pages charge that same function - which is the pairing that matters, because
///    a tool that measures its page in raw bytes while the cutter charges escaped bytes delivers
///    a page the cutter refuses. The numbers an envelope QUOTES stay UTF-8 bytes
///    ([`crate::tools::result::utf8_byte_length`]), exactly TS's split (`result-budget.ts` gates
///    with `modelPayloadBytes` and reports with `utf8ByteLength`). RESIDUALS:
///      * a LONE surrogate cannot exist in a Rust `String`. TS prices one at the six-byte
///        `\udXXX` literal ES2019 `JSON.stringify` writes; this crate's decoder turns it into
///        U+FFFD, which costs 3 and is billed as 3 - always UNDER TS, never over, because the
///        crate bills the bytes it actually writes. The reference's surrogate vectors are pinned
///        on the code-unit twin [`crate::payload::model_payload_bytes_units`].
///      * the crate prices what its OWN serializer writes (the identity, and the test that asserts
///        the walk equals `serde_json`'s rendering on every escape class, are in
///        [`crate::payload`]), where TS prices `JSON.stringify`. The two agree on every class the
///        reference's corpus covers, U+2028/U+2029, DEL, the C1 controls and the BOM included.
///      * the full residual list lives in `src/tools/result.rs`'s module header, which is the
///        single place it is written out.
/// 3. WHY THE HISTORICAL "must stay ≥ read's own content budget" argument no longer binds at
///    this number: the old comment kept 256 KiB because `read` budgeted its own output at the same
///    order and the two had to agree. That interaction is now closed in the direction that
///    matters: [`crate::tools::files::READ_CONTENT_BUDGET`] IS this constant, as are
///    [`crate::tools::files::LIST_CONTENT_BUDGET`] and
///    [`crate::tools::files::SEARCH_CONTENT_BUDGET`], and each of those tools charges a page
///    against a published allowance that has ALREADY paid for the result's own scaffolding
///    ([`crate::tools::files::file_list_allowance`], [`crate::tools::files::glob_page_allowance`],
///    [`crate::tools::files::grep_page_allowance`], `read_result_overhead`). So a file tool that
///    fills its page is inside this cap and survives the cutter whole - `bound_with_reason` in
///    `crate::tools::result` (private, which is the point: nothing bypasses it); the
///    oversized-JSON refusal stays reachable only for a result no tool budgeted to this ceiling.
/// 4. THE WEB TOOLS NOW BUDGET THEMSELVES IN THIS CURRENCY, so they are no longer on that list.
///    `crate::tools::web::fetch` sizes a whole page against a content allowance measured with
///    [`crate::payload::document_wire_bytes`] (the envelope measured with an EMPTY `content`, the
///    5% step-down ladder, `webfetch/index.ts` `fitResult`), and `crate::tools::web::search` trims
///    ROWS to [`crate::tools::web::SEARCH_MAX_RESULT_BYTES`] before the result is assembled; the two
///    ceiling tests in `tests/web_surface.rs` assert the delivered outcome is inside this cap AND is
///    not refused, which is the property the character-currency version of this note could not claim.
///    `crate::tools::web::download` keeps its bytes off the model path altogether - the summary is
///    the result and the payload rides the attachment - so a 20 MiB file spends none of this budget.
///    The live unbudgeted producer is `command`'s 128 KiB stdout transcript, and anything else a
///    caller hands the runtime without sizing it.
pub const MODEL_TOOL_RESULT_BUDGET_BYTES: usize = 32 * 1024;

/// The `tool_result_bytes` figure THIS crate's boot document publishes: **32768**, identical to
/// the TS boot document (`harness-api.ts` `HARNESS_SERVICE_LIMITS`,
/// `harness-server.swagger.yaml`). It stays DERIVED from the ENFORCED budget so the advertised
/// figure and the enforced one can never drift apart again — which is how the old deviation 17
/// got away. `tests/foundation.rs` pins published == enforced.
pub const PUBLISHED_TOOL_RESULT_BYTES: u64 = MODEL_TOOL_RESULT_BUDGET_BYTES as u64;

/// The aggregate sibling of the cap above: the most TOTAL tool-result text ONE turn may
/// commit, summed over every call the model asked for in one assistant message
/// (`MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES`, enforced by
/// `createToolResultTurnBudget` / [`crate::tools::result::ToolResultTurnBudget`]) — **98,304**,
/// at parity.
///
/// CLOSED with [`MODEL_TOOL_RESULT_BUDGET_BYTES`] under the same C1 item: TS derives it at
/// `result-budget.ts:250-251` from `MODEL_TOOL_RESULTS_PER_TURN_BUDGET_TOKENS` 24,576 (`:202`) ×
/// `MODEL_TOKEN_BYTES` 4 (`:171`). The 3× per-result-to-per-turn ratio ("three full results per
/// turn") is TS's design and is now this crate's too; the historical Rust 2× ratio (512 KiB over
/// 256 KiB) went with the numbers. `tools::result::tests` re-pins the ratio as arithmetic.
pub const MODEL_TOOL_RESULTS_PER_TURN_BUDGET_BYTES: usize = 96 * 1024;

/// The `service` identity the root document advertises. TS sends the literal
/// `'noobscript-harness'` (`api.ts:1307`) and the published contract documents that value,
/// so the Rust deployment advertises the SAME string: a client that branches on the service
/// name must not see a different one per port.
pub const SERVICE_NAME: &str = "noobscript-harness";

/// The `api_version` the root document advertises (`api.ts:1308`).
pub const API_VERSION: &str = "v1";

/// The page size a stream baseline link carries (`api-events.ts:171` `BASELINE_LIMIT`).
pub const BASELINE_PAGE_LIMIT: u64 = 25;

/// The limits block of the boot document, built from the constants above so a route can
/// never advertise a number different from the one it enforces (`HARNESS_SERVICE_LIMITS`,
/// `harness-api.ts:197-213`).
#[must_use]
pub fn service_limits() -> ServiceLimits {
    ServiceLimits {
        body_bytes: MAX_BODY_BYTES as u64,
        submit_body_bytes: MAX_SUBMIT_BODY_BYTES as u64,
        input_chars: MAX_INPUT_CHARS as u64,
        pending_tasks: MAX_PENDING_TASKS as u64,
        thread_records: MAX_THREAD_RECORDS as u64,
        attachments_per_turn: MAX_ATTACHMENTS_PER_TURN as u64,
        attachment_bytes: MAX_ATTACHMENT_BYTES as u64,
        total_attachment_bytes: MAX_TOTAL_ATTACHMENT_BYTES as u64,
        result_references: MAX_RESULT_REFERENCES as u64,
        pending_actions: MAX_PENDING_ACTIONS as u64,
        steer_records: MAX_STEER_RECORDS as u64,
        created_tools: MAX_CREATED_TOOLS as u64,
        // THIS build's enforced window, published as its own boot-document figure
        // ([`PUBLISHED_TOOL_RESULT_BYTES`], derived from [`MODEL_TOOL_RESULT_BUDGET_BYTES`] so
        // the two can never drift). TS publishes 32768 at `engine/shared/harness-api.ts:231`;
        // the two documents differ on purpose — see the numbered divergence on that constant.
        tool_result_bytes: PUBLISHED_TOOL_RESULT_BYTES,
        page_limit_default: PAGE_LIMIT_DEFAULT,
        page_limit_max: PAGE_LIMIT_MAX,
    }
}

/// The capability advertisement of a full harness deployment (`HARNESS_SERVICE_CAPABILITIES`,
/// `harness-api.ts:216-226`). Every member is `true` in the TS package; a reduced Rust
/// deployment flips one field rather than inventing a second document.
#[must_use]
pub fn service_capabilities() -> ServiceCapabilities {
    ServiceCapabilities {
        websocket_stream: true,
        tool_execution: true,
        tool_registration: true,
        transcript_editing: true,
        command_dispatch: true,
        model_discovery: true,
        harness_definition_writes: true,
        agent_writes: true,
        persistent_threads: true,
    }
}

// ---------------------------------------------------------------------------
// String vocabularies (TS unions of string literals)
// ---------------------------------------------------------------------------

/// Session lifecycle as the server owns it (`harness-api.ts:790`). `running` means at
/// least one turn is executing; a queued turn alone does NOT set it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    Idle,
    Running,
    Error,
}

/// Recorded task state (`harness-api.ts:799`); `queued` = a stable id and index, no
/// `started_at` yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionTurnStatus {
    Queued,
    Running,
    Done,
    Error,
    Cancelled,
}

/// Folded turn-tile state (`turn-display.ts:186`), i.e. `TurnRecordStatus` with the
/// server-side `running -> streaming` mapping already applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DisplayState {
    Queued,
    Streaming,
    Done,
    Error,
    Cancelled,
}

/// One agent flow's display state (`turn-display.ts:189`); `open` = started, nothing committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SectionDisplayState {
    Open,
    Streaming,
    Done,
    Error,
    Cancelled,
}

/// One tool/action row state (`turn-display.ts:192`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StepDisplayState {
    Running,
    Ok,
    Error,
    Cancelled,
}

/// State of one `#`-referenceable result block (`agent-results.ts:58`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentResultStatus {
    Queued,
    Running,
    Done,
    Error,
    Cancelled,
}

/// Transcript readability (`ThreadSummary.transcript_state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TranscriptState {
    Ok,
    Unreadable,
}

/// Where an instruction TEXT comes from for a harness member (`harness-api.ts:444`).
///
/// R7: `harness-api.ts:451` states FOUR spellings for `HarnessInstructionSummaryEntry.source`,
/// because a member THIS session defined (`POST .../agents`, or a session-saved definition from a
/// TypeScript host that runs created agents) has no profile to describe it - `session` is the only
/// honest answer, and it is reachable from a restored `state.json` even in a build that creates no
/// agents itself. The two narrower members of the same contract (`HarnessInstructionTextResponse.source`
/// at `:547`, `AgentProfile.instruction_source` at `:594`) are 3- and 2-valued unions in TypeScript;
/// this port models provenance as ONE enum, so those fields accept the extra spelling on the way in
/// and never emit it (each producer below can only name catalogue / global / harness). That is an
/// accepted superset - a refusal of a value the contract names elsewhere would be the worse error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstructionSource {
    Catalogue,
    Global,
    Harness,
    Session,
}

/// The four-layer provenance of a SESSION agent's instruction (`harness-api.ts:938`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionInstructionSource {
    Catalogue,
    Global,
    Harness,
    Session,
}

/// The retired two-valued role, kept ONLY as a store-side member of the durable binding entry.
///
/// THE CONTRACT CARRIES NO `role` ANYWHERE ON THE AGENT FAMILY (R8 - the previous wording of this note
/// claimed the session-agent binding was "the one place the contract still carries a two-valued role",
/// and that was false): `SessionAgentBinding` requireds `rank` and forbids `role`
/// (`harness-api.ts:1756-1847`, `additionalProperties: false`), `CreateSessionAgentRequest` names
/// `rank` (`:1879`), and the generated TypeScript row is `rank: AgentRank`
/// (`engine/shared/harness-api.ts:1770`). The ONLY `role` in the contract is
/// [`crate::contract::HarnessPrefixTurn`]'s, which means `system | user | assistant` and has nothing to
/// do with authority.
///
/// So this enum is NOT a wire type any more: it is the legacy column of
/// [`crate::store::SessionAgentEntry`], the store-side binding record that this build (and a TypeScript
/// document written before the rank migration) persists into `core.sessionAgentOverrides`. Removing the
/// field would break a tolerant read of those documents for no benefit, so it stays - defaulted, never
/// emitted, and read by nothing. A new document simply omits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentRole {
    Primary,
    Subagent,
}

// ---------------------------------------------------------------------------
// THE RANK LETTER (R1) - swagger `AgentRank` (`:4669-4685`)
// ---------------------------------------------------------------------------

/// One rank letter, upper-case: `A` the highest authority through `Z` the lowest
/// (`AgentRank`, `types/index.ts:1583-1616`, aliased by the generated contract at
/// `harness-api.ts:622-627`).
///
/// An enum rather than a `String` because the YAML declares a closed 26-member `enum`: serde then
/// refuses a lower-case or multi-character value at the boundary, and the rank ARITHMETIC below is
/// total (no `Option`, no parse) because every value of this type is a legal rank. A rank is
/// AUTHORITY, not a singleton: several members of a roster may hold the same letter, including `A`,
/// and delegation exists exactly when a rank map holds two or more DISTINCT letters.
///
/// The variant order IS the rank order (`AGENT_RANKS`, `types/index.ts:1616-1643`, "highest rank
/// first"), so `rank_index_of` is the variant index and `derive(PartialOrd)` agrees with it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub enum AgentRank {
    /// Highest authority (variant index 0).
    #[default]
    A,
    B,
    C,
    D,
    E,
    F,
    G,
    H,
    I,
    J,
    K,
    L,
    M,
    N,
    O,
    P,
    Q,
    R,
    S,
    T,
    U,
    V,
    W,
    X,
    Y,
    Z,
}

/// `AGENT_RANKS` (`types/index.ts:1616-1643`): the rank scale AS DATA, highest first. Every
/// consumer that has to enumerate ranks reads this table rather than re-listing letters, so the
/// ordering is defined in exactly one place.
pub const AGENT_RANKS: [AgentRank; 26] = [
    AgentRank::A,
    AgentRank::B,
    AgentRank::C,
    AgentRank::D,
    AgentRank::E,
    AgentRank::F,
    AgentRank::G,
    AgentRank::H,
    AgentRank::I,
    AgentRank::J,
    AgentRank::K,
    AgentRank::L,
    AgentRank::M,
    AgentRank::N,
    AgentRank::O,
    AgentRank::P,
    AgentRank::Q,
    AgentRank::R,
    AgentRank::S,
    AgentRank::T,
    AgentRank::U,
    AgentRank::V,
    AgentRank::W,
    AgentRank::X,
    AgentRank::Y,
    AgentRank::Z,
];

/// The highest letter a rank may hold (`AGENT_RANKS[0]`), which the contract's refusal sentence
/// quotes.
pub const HIGHEST_AGENT_RANK: AgentRank = AgentRank::A;
/// The lowest letter a rank may hold (`AGENT_RANKS[25]`): what a member no rank map names gets,
/// because "an agent has no authority the definition did not grant it".
pub const LOWEST_AGENT_RANK: AgentRank = AgentRank::Z;

impl AgentRank {
    /// The wire spelling: the letter itself. The match mirrors `rank_index_of` because a `const fn`
    /// cannot index a table it is itself part of.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
            Self::D => "D",
            Self::E => "E",
            Self::F => "F",
            Self::G => "G",
            Self::H => "H",
            Self::I => "I",
            Self::J => "J",
            Self::K => "K",
            Self::L => "L",
            Self::M => "M",
            Self::N => "N",
            Self::O => "O",
            Self::P => "P",
            Self::Q => "Q",
            Self::R => "R",
            Self::S => "S",
            Self::T => "T",
            Self::U => "U",
            Self::V => "V",
            Self::W => "W",
            Self::X => "X",
            Self::Y => "Y",
            Self::Z => "Z",
        }
    }
}

/// `rankIndexOf` (`types/index.ts:1647`): 0 = `A` (highest), 25 = `Z` (lowest). A LARGER index is a
/// LOWER rank, which is the direction every comparison in the kernel uses.
#[must_use]
pub const fn rank_index_of(rank: AgentRank) -> usize {
    match rank {
        AgentRank::A => 0,
        AgentRank::B => 1,
        AgentRank::C => 2,
        AgentRank::D => 3,
        AgentRank::E => 4,
        AgentRank::F => 5,
        AgentRank::G => 6,
        AgentRank::H => 7,
        AgentRank::I => 8,
        AgentRank::J => 9,
        AgentRank::K => 10,
        AgentRank::L => 11,
        AgentRank::M => 12,
        AgentRank::N => 13,
        AgentRank::O => 14,
        AgentRank::P => 15,
        AgentRank::Q => 16,
        AgentRank::R => 17,
        AgentRank::S => 18,
        AgentRank::T => 19,
        AgentRank::U => 20,
        AgentRank::V => 21,
        AgentRank::W => 22,
        AgentRank::X => 23,
        AgentRank::Y => 24,
        AgentRank::Z => 25,
    }
}

/// `isLowerRank` (`types/index.ts:1654-1655`): may `candidate` receive a delegation FROM `caller`?
/// True only when `candidate` is STRICTLY lower-ranked, so a same-rank peer is not a subordinate.
#[must_use]
pub const fn is_lower_rank(candidate: AgentRank, caller: AgentRank) -> bool {
    rank_index_of(candidate) > rank_index_of(caller)
}

/// `isAgentRank` (`types/index.ts:1662-1663`): the untrusted-input guard. One upper-case letter of
/// the scale; `a`, `A1`, and `""` are all false, and the caller prints its own refusal naming the
/// scale rather than upper-casing the client's intent.
#[must_use]
pub fn is_agent_rank(text: &str) -> bool {
    let mut characters = text.chars();

    let Some(first) = characters.next() else {
        return false;
    };

    characters.next().is_none() && first.is_ascii_uppercase() && first.is_ascii_alphabetic()
}

/// The letter a rank one step BELOW `rank` is, clamped at `Z` (`toolbox.ts:280`
/// `AGENT_RANKS[Math.min(lowest + 1, AGENT_RANKS.length - 1)]`).
#[must_use]
pub const fn rank_below(rank: AgentRank) -> AgentRank {
    // `Z` is its own successor: the scale is closed, so the ladder clamps instead of wrapping, and
    // an unresolved member of an all-`Z` team lands on `Z` like its peers. Written as an `if` because
    // `Ord::min` is not const-stable.
    let next = rank_index_of(rank) + 1;

    AGENT_RANKS[if next >= AGENT_RANKS.len() {
        AGENT_RANKS.len() - 1
    } else {
        next
    }]
}

/// Parses one wire value into a rank letter. `None` is "not a rank", which the registry answers with
/// the contract's refusal rather than serde's.
#[must_use]
pub fn agent_rank_of_text(text: &str) -> Option<AgentRank> {
    if is_agent_rank(text) {
        // Safe: `is_agent_rank` proved one upper-case ASCII letter, and the scale is exactly those
        // 26 letters in order.
        Some(AGENT_RANKS[usize::from(text.as_bytes()[0] - b'A')])
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// THE TWO RANK REFUSALS (R2) - `validateBindingRank` (`sessions.ts:1205-1222`)
// ---------------------------------------------------------------------------

/// The scale spelled exactly as BOTH rank refusals quote it: `AGENT_RANKS.join(', ')`
/// (`sessions.ts:1220`, `api.ts:4055`). Built FROM [`AGENT_RANKS`] rather than typed out, so the
/// sentence a client reads can never drift from the scale the validator accepts - the same reason
/// TypeScript interpolates the table instead of naming the letters in the message.
#[must_use]
pub fn rank_scale_spelled() -> String {
    AGENT_RANKS
        .iter()
        .map(|rank| rank.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// THE ONE BINDING REFUSAL, verbatim from `validateBindingRank` (`sessions.ts:1218-1221`): a
/// `rank` that is present and is not one upper-case letter, on either write, answers this sentence.
///
/// It deliberately does NOT name the value the client sent: TS's validator never guesses a rank for a
/// bad one ("the validator NEVER reinterprets it as a rank, which is the whole point of not keeping a
/// compat shim"), and the letter list is the answer the caller needs.
#[must_use]
pub fn binding_rank_refusal() -> String {
    format!(
        "An agent binding takes a \"rank\": one letter of the authority scale, {}. The rank is what \
         decides what the agent may reach; the roster alone does not.",
        rank_scale_spelled()
    )
}

/// THE SINGLE-LETTER REFUSAL, verbatim from `api.ts:4054-4057` and `sessions.ts:8246-8249`: the PATCH
/// reaches this one when the caller STATED `rank` and the value resolved to no authority at all -
/// i.e. `{"rank": null}`. POST never answers it, because a `null` rank there is the contract's
/// "ask for no authority" arm (`harness-api.ts:1932-1937`), not a malformed letter.
#[must_use]
pub fn rank_letter_refusal() -> String {
    format!(
        "An agent rank must be one letter of the authority scale, {}.",
        rank_scale_spelled()
    )
}

// ---------------------------------------------------------------------------
// THE PRESET-HISTORY TURN (U6) - swagger `HarnessPrefixTurn` (`:4784-4830`)
// ---------------------------------------------------------------------------

/// The role a seeded history turn carries (`HarnessPrefixTurn.role`, swagger `:4813-4822`): the
/// three TEXT roles only. `tool` is refused - "a tool turn belongs to a conversation, not to a
/// preset" - and so is any other value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrefixTurnRole {
    /// Legal here: a preset band is a SEED, not a conversation, so a system turn sits BELOW the
    /// member's instruction rather than competing with it.
    System,
    User,
    Assistant,
}

/// One seeded history turn (`HarnessPrefixTurn`, `types/index.ts:727-731`, swagger `:4788-4830`): a
/// message the definition places in a member's transcript BEFORE its first real turn.
///
/// THE CLOSED PAIR IS LOAD-BEARING: only `role` + `content` exist, which is what keeps one band
/// meaning the same bytes in a template, a session override, and an engine band. `toolCalls`,
/// `toolCallId`, `usage`, and attachments belong to the transcript record, and a request that carries
/// one has it DROPPED rather than refused (swagger `:4794-4806`) - so this type models what is
/// STORED, and the registry's `require_prefix_turns` is the place that drops or refuses.
///
/// `content` is VERBATIM: the route never trims, re-wraps, or normalises it, because the bytes a
/// client saved are the bytes the model reads. An empty string is a real transcript entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessPrefixTurn {
    pub role: PrefixTurnRole,
    pub content: String,
}

/// Which layer wrote the preset-history band one session agent seeds
/// (`SessionPrefixTurnsResponse.source`, swagger `:6667-6680`).
///
/// THREE answers only: `session` for a band this conversation stored that DIFFERS from what its
/// definition seeds, `harness` for the frozen definition's own band, and `none` when no layer seeds
/// one. There is no `catalogue`/`global` arm - a preset history has no catalogue layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionPrefixSource {
    Harness,
    Session,
    /// No layer seeds this member; `prefix` is empty.
    None,
}

/// The preset history one session agent actually seeds, plus its provenance
/// (`SessionPrefixTurnsResponse`, swagger `:6644-6679`).
///
/// `prefix` is the LIVE band, in order, verbatim - EMPTY when nothing seeds this member, which is
/// also what `source: none` states. A session write that restates the definition's own bytes answers
/// `harness`, because no reader can observe a difference that is not there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionPrefixTurnsResponse {
    pub session_id: String,
    pub agent_id: String,
    pub prefix: Vec<HarnessPrefixTurn>,
    pub source: SessionPrefixSource,
}

/// Whether a roster binding came from the frozen definition or was created in the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BindingOrigin {
    Definition,
    Session,
}

/// Where a session toolbox row comes from (`harness-api.ts:1029`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionToolOrigin {
    Catalogue,
    Discovered,
    Created,
}

/// Model visibility before a search enables it (`types/index.ts:492`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolVisibility {
    Core,
    Searchable,
}

/// Whether the server can actually run a listed tool (`harness-api.ts:629`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolAvailabilityState {
    Available,
    Unconfigured,
}

/// The `scope` half of the execution gate (`harness-api.ts:1052`). The catalogue detail
/// read only ever answers `global` or `session_required` (`:660`); `session_only` is what
/// the session surface states for an entry that cannot run without its session (`:902`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionScopeKind {
    Global,
    SessionRequired,
    SessionOnly,
}

/// How this entry may be called over HTTP, STATED rather than inferred by the client
/// (`harness-api.ts:1051-1054` `ToolExecutionScope`): a CLOSED OBJECT with exactly `scope`
/// and `manual`, so the wire is `"execution": {"scope": ..., "manual": ...}` - never the
/// bare string an enum alone would print.
///
/// `manual` is "may a human/host invoke this entry directly": on the catalogue read it is
/// `entry?.tool !== undefined` (`api-reads.ts:1161`, only an entry with a real
/// implementation runs standalone), on the session read it is always `true`
/// (`api-reads.ts:900-905`, manual execution is the documented session surface).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolExecutionScope {
    pub scope: ToolExecutionScopeKind,
    pub manual: bool,
}

/// A conversation's identity kind (`harness-api.ts:1384` / `types/index.ts:1780`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThreadOrigin {
    Agent,
    Host,
}

/// Conversation run state (`types/index.ts:1785`); `stopped` = halted by the host, `paused` =
/// parked by a runtime layer (the shipped `context-compaction` plugin parks a conversation while
/// it compacts, so a roster read can report it).
///
/// R7: `harness-api.ts:1481` widened the union to FOUR spellings. This build never TRANSITIONS a
/// conversation into `paused` (no plugin runtime), but it must not silently turn a `paused` record
/// or filter value into `idle`, because the UI colours a parked conversation differently and a
/// roster read that lied about the state would strand that affordance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThreadLifecycle {
    Idle,
    Running,
    Paused,
    Stopped,
}

/// What an attachment IS (`types/index.ts:47`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttachmentKind {
    Image,
    Video,
    Audio,
    Pdf,
    File,
}

/// Transcript entry roles (`types/index.ts:24`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

/// Why generation ended (`types/index.ts:182`); `tool_calls` = the run is not finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessStopReason {
    Stop,
    Length,
    ToolCalls,
}

/// WHY a tool call ended (`types/index.ts:215`); `isError` is always
/// `outcome != completed`, so the two can never disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HarnessToolOutcome {
    Completed,
    Failed,
    Rejected,
    Cancelled,
}

/// Who answered an owner question (`types/index.ts:886`); `auto` = an automatic owner fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActionAnswerSource {
    Owner,
    Auto,
}

/// Recorded event kinds (`harness-stream.ts:112`). An unrecognised `type` is data to
/// tolerate, never a failure; `attempt_commit` keeps the snake_case spelling the TS
/// recorder emits, and serde's `snake_case` rename of `AttemptCommit` produces exactly it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessEventType {
    Start,
    Text,
    Reasoning,
    ToolCall,
    ToolResult,
    Usage,
    Done,
    Retry,
    AttemptCommit,
    Wakeup,
    ActionRequest,
    ActionAnswer,
    BranchStart,
    BranchEnd,
    Error,
}

// ---------------------------------------------------------------------------
// Domain payloads reused VERBATIM by the contract
// (`types/index.ts`; the generated contract imports rather than duplicates them,
// see `harness-api.ts:229-251` and the "Verbatim alias" entries at `:1955-2022`)
// ---------------------------------------------------------------------------

/// Token accounting normalized from the provider's `usage` block (`types/index.ts:218`).
/// Field names are identical on both spellings, so no rename is needed.
///
/// `cached` is OPTIONAL and never defaulted: "not reported" and "reported, 0 cached"
/// are different facts (`:227-230`), so a reader must treat `None` as unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessUsage {
    pub input: u64,
    pub output: u64,
    pub total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached: Option<u64>,
}

/// Serialisable error shape - events cross process boundaries, so failures travel as
/// data rather than as error instances (`types/index.ts:436`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessErrorInfo {
    pub message: String,
}

/// A tool invocation requested by the model (`types/index.ts:108`). `arguments` stays a
/// RAW JSON STRING: that is what the wire protocol delivers, and malformed JSON is the
/// harness's problem to report back to the model rather than a parse crash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// One composer attachment WITH its bytes (`types/index.ts:35`). `data` is raw canonical
/// base64 (no `data:` prefix, no whitespace) and `size` is the DECODED length that must
/// agree with it. This is the shape the attachment download route answers with
/// (`harness-api.ts:1673` `AttachmentResponse`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessAttachment {
    pub id: String,
    pub kind: AttachmentKind,
    /// The exact text label the composer inserted into the prompt, e.g. `[Image 1]`.
    pub marker: String,
    pub name: String,
    pub mime: String,
    pub size: u64,
    pub data: String,
}

/// The byte-free projection of an attachment that rides on a turn row
/// (`harness-api.ts:1575`). NOTE the snake_case `has_bytes`: this one is a REST resource,
/// not a domain payload, so it follows the REST spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AttachmentDescriptor {
    pub id: String,
    pub kind: AttachmentKind,
    pub marker: String,
    pub name: String,
    pub mime: String,
    pub size: u64,
    pub has_bytes: bool,
}

/// The model-facing projection of tool media carried on a `tool` message
/// (`types/index.ts:91`): descriptors only, NEVER base64, because every event is JSON
/// round-tripped and persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolMediaDescriptor {
    pub id: String,
    pub kind: AttachmentKind,
    pub name: String,
    pub mime: String,
    pub size: u64,
}

/// One background tool call still running when a request is made (`types/index.ts:192`):
/// ids plus a monotonic elapsed time, never a promise or a controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessPendingTool {
    pub call_id: String,
    pub name: String,
    pub elapsed_ms: u64,
}

/// One choice the model offers its owner (`types/index.ts:855`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessActionOption {
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The model-facing owner question (`types/index.ts:864`). `recommended` MUST be one of
/// the option ids (the parser refuses an unknown id rather than defaulting), and
/// `allowCustom` is a type-level `true`, not a switch the model may disable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessActionRequest {
    /// Generated by the runtime, never by the model.
    pub request_id: String,
    pub question: String,
    pub recommended: String,
    pub options: Vec<HarnessActionOption>,
    pub allow_custom: bool,
}

impl HarnessActionRequest {
    /// The runtime always mints `allowCustom: true` (`types/index.ts:872`: free-text
    /// answers are part of the contract, not a switch the model may disable), so the only
    /// constructor there takes no flag for it.
    #[must_use]
    pub fn new(
        request_id: impl Into<String>,
        question: impl Into<String>,
        recommended: impl Into<String>,
        options: Vec<HarnessActionOption>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            question: question.into(),
            recommended: recommended.into(),
            options,
            allow_custom: true,
        }
    }
}

/// The answer to an owner question, as chosen by a human/host or the automatic owner
/// fork (`types/index.ts:877-879`) - camelCase, because this is the recorded domain payload:
/// the `Option` member is `optionId`, and it is embedded VERBATIM in a recorded
/// `action_answer` event (`api-reads.ts` / the journal replay), so a recorder that printed
/// `option_id` would write a journal entry the TS reader cannot parse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum HarnessActionChoice {
    #[serde(rename_all = "camelCase")]
    Option {
        option_id: String,
    },
    Custom {
        text: String,
    },
}

/// The answer a REST client SENDS (`harness-api.ts:1315` `ActionChoiceInput`): request-side
/// spelling is snake_case `option_id`, and the recorded event keeps its own camelCase names
/// (see [`HarnessActionChoice`]). The two spellings are deliberately different.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ActionChoiceInput {
    Option { option_id: String },
    Custom { text: String },
}

/// Recorded answer envelope (`types/index.ts:883`); `branch_id` is present only when an
/// automatic owner fork answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessActionAnswer {
    pub request_id: String,
    pub choice: HarnessActionChoice,
    pub source: ActionAnswerSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<String>,
}

/// One `#LABEL` reference a submit resolved (`shared/agent-results.ts`, aliased verbatim
/// at `harness-api.ts:2015`): the turn whose result block it names plus the label the
/// user typed. camelCase on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentResultReference {
    pub turn_id: String,
    /// Absent = the ROOT invocation of that turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub label: String,
}

/// One transcript entry (`types/index.ts:181-303`), discriminated by `role`. The provider
/// wire mapping happens in the transport; this is the shape the transcript routes page.
///
/// THE THREE CONTEXT KEYS every variant now carries, and the exact rule each one follows
/// (all three are TS-persisted, none of them is a provider field - the transport
/// field-selects per role, `transport/openai.ts wireMessage`):
///
/// * `included` (`types/index.ts:117-139`, present on ALL four roles) - a persisted,
///   default-true flag that decides whether the entry reaches the MODEL, never whether it
///   stays in the transcript. Absent means included; the literal `false` is the ONLY stored
///   value; re-enabling DELETES the key, so `included: true` is never written anywhere
///   (`api.ts:925-949 messagesWithUnitInclusion`, mirrored from `harness.ts writeIncluded`).
/// * `reasoningIncluded` (assistant variant only, `types/index.ts:213-236`) - the second
///   axis. Same storage rule as `included`, but it does not exclude the entry: the entry
///   travels and the projection OMITS `reasoning` from its request copy
///   (`core/agent.ts:245-253 withoutWithheldReasoning`, `api.ts:899-923
///   messagesWithReasoningInclusion`). There is deliberately no key on the other roles,
///   because `reasoning` exists only beside it; the snapshot reader's assistant whitelist
///   is the only place that admits it (`core/snapshot.ts:420-430`).
/// * `messageId` (`types/index.ts:161-180`, all four roles) - the stable transcript address
///   of THIS turn. Minted once where a canonical message is created, copied verbatim by
///   every copy path, and stripped from every REQUEST projection copy
///   (`core/agent.ts:203-230 withIncluded` always drops the key, keeping the `false` flags),
///   while the host read surfaces (transcript routes, `threadMessages()`, `snapshot()`) DO
///   report it. Modeled here as a plain `Option<String>` that is emitted when present: the
///   mint/backfill rule (`core/snapshot.ts:300-332`) belongs to the store, not to this type.
///
/// HOW THE ONLY-`false`-IS-VISIBLE RULE IS ENFORCED (the three keys above are the only
/// fields with it, so it lives in the field attributes rather than in a hand-written impl):
/// `deserialize_with = "read_context_flag"` accepts a literal boolean and normalizes
/// `true` to the absent form - TS accepts a submitted `true` at every whole-message entry
/// point and silently normalizes it (types/index.ts:150-159 "HOST HYGIENE",
/// `core/snapshot.ts:225-265 applyIncluded`/`applyReasoningIncluded`), and refuses any
/// non-boolean, `null` included, exactly like TS's `readSnapshotBoolean`.
/// `skip_serializing_if = "context_flag_absent"` then emits the key ONLY for
/// `Some(false)`, so a `true` a Rust caller sets by hand can never reach a document either
/// (TS's writers `delete copy.included` instead, `api.ts:919-920` and `:944-945`).
///
/// * `outcome` (tool variant only, `types/index.ts:276-298`, compaction T1.4) - WHY the call
///   ended, persisted so the classification survives a restart, a snapshot, a fork and a
///   `threadRead` instead of living only on the live `tool_result` event. Same minimal-delta
///   storage family as the two flags: **only a NON-`completed` outcome is ever stored**, absent
///   reads as `completed`, and nothing re-derives an outcome from the failure prose
///   (`core/tool-outcome.ts` rules 1-3: `storedOutcomeEntry` / `toolOutcomeOf` /
///   `toolResultIsError`, the last of which is why `isError` stays DERIVED and is never stored
///   beside it). NOT model-facing: the transport field-selects the tool wire shape
///   (`transport/openai.ts`, mirrored by `transport.rs:1046-1064`), so a provider payload is
///   byte-identical with or without the key.
///
/// KEY ORDER is the declaration order below, and the internal `role` tag prints first;
/// each variant lists its fields in the exact order the TS union declares them, so a
/// round-trip of a TS-shaped document is byte-for-byte identical. The tool variant declares
/// `outcome` BEFORE `included` exactly as `types/index.ts:293-298` does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum HarnessMessage {
    /// A system message: one entry per recorded instruction.
    #[serde(rename_all = "camelCase")]
    System {
        content: String,
        /// Withholds this entry from the model request; only `Some(false)` is stored.
        #[serde(
            default,
            deserialize_with = "read_context_flag",
            skip_serializing_if = "context_flag_absent"
        )]
        included: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    /// A user message; `attachments` is omitted for a text-only entry so every pre-attachment
    /// transcript keeps its exact old shape (`:186-193`).
    #[serde(rename_all = "camelCase")]
    User {
        content: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<HarnessAttachment>,
        #[serde(
            default,
            deserialize_with = "read_context_flag",
            skip_serializing_if = "context_flag_absent"
        )]
        included: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    /// An assistant entry: `tool_calls` only when the turn requested tools, `reasoning`
    /// kept out of the wire payload but retained so a UI can replay a turn (`:202-242`).
    #[serde(rename_all = "camelCase")]
    Assistant {
        content: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<HarnessToolCall>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
        #[serde(
            default,
            deserialize_with = "read_context_flag",
            skip_serializing_if = "context_flag_absent"
        )]
        included: Option<bool>,
        /// THE SECOND AXIS, and the one field this variant owns that no other role may
        /// carry. Serde's internally-tagged reader IGNORES an unknown key on another
        /// variant (this crate has no `deny_unknown_fields` - see the module header), so
        /// the "a `reasoningIncluded` on a non-assistant entry is refused, not quietly
        /// dropped" rule of `core/snapshot.ts:252-254`/`:420-430` is a HANDLER-level check,
        /// where every other TS refusal sentence lives (`api.ts:504-510`).
        #[serde(
            default,
            deserialize_with = "read_context_flag",
            skip_serializing_if = "context_flag_absent"
        )]
        reasoning_included: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    /// A tool result; `attachments` are out-of-band media (`toolMedia()`), flushed by the
    /// transport as a labelled user-role message AFTER the tool block (`:243-303`).
    #[serde(rename_all = "camelCase")]
    Tool {
        content: String,
        tool_call_id: String,
        name: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<HarnessAttachment>,
        /// WHY this call ended, in the storage form `core/tool-outcome.ts` rule 1 states:
        /// `completed` (and "never classified") has NO stored form, so the three non-healthy
        /// values are the only ones a document ever shows, and ABSENT reads as `completed`
        /// (`types/index.ts:276-298`). Placed BEFORE `included` because the TS union declares it
        /// there (`:293`), which keeps a TS-written entry round-tripping in TS's key order.
        #[serde(
            default,
            deserialize_with = "read_stored_outcome",
            skip_serializing_if = "stored_outcome_absent"
        )]
        outcome: Option<HarnessToolOutcome>,
        /// A UNIT member: `false` here withholds the whole tool-call block, the assistant
        /// entry that asked for it included (`types/index.ts:135-139`, `:294-298`).
        #[serde(
            default,
            deserialize_with = "read_context_flag",
            skip_serializing_if = "context_flag_absent"
        )]
        included: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
}

impl HarnessMessage {
    /// Whether this entry is withheld from the model request: the literal `false` is the
    /// only excluding value, absent means included (`types/index.ts:127-128`).
    #[must_use]
    pub fn is_withheld(&self) -> bool {
        self.included_flag() == Some(&false)
    }

    /// Whether this entry's THINKING is withheld from the model request. Non-assistant
    /// entries have no thinking to withhold, so their answer is always `false`
    /// (`core/agent.ts:210-211`).
    #[must_use]
    pub fn is_reasoning_withheld(&self) -> bool {
        self.reasoning_included_flag() == Some(&false)
    }

    /// The stored `included` flag of whichever variant this is (`None` = included).
    #[must_use]
    pub fn included_flag(&self) -> Option<&bool> {
        match self {
            Self::System { included, .. }
            | Self::User { included, .. }
            | Self::Assistant { included, .. }
            | Self::Tool { included, .. } => included.as_ref(),
        }
    }

    /// The stored `reasoningIncluded` flag; only the assistant variant has one, so every
    /// other role reports `None` rather than a phantom `true`.
    #[must_use]
    pub fn reasoning_included_flag(&self) -> Option<&bool> {
        match self {
            Self::Assistant {
                reasoning_included, ..
            } => reasoning_included.as_ref(),
            _ => None,
        }
    }

    /// The stable transcript address of this entry, when it carries one.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        match self {
            Self::System { message_id, .. }
            | Self::User { message_id, .. }
            | Self::Assistant { message_id, .. }
            | Self::Tool { message_id, .. } => message_id.as_deref(),
        }
    }

    /// The stored `outcome` of a tool entry (`None` for every other role, and for a tool entry
    /// whose outcome is the unwritten `completed` form).
    #[must_use]
    pub fn outcome_flag(&self) -> Option<&HarnessToolOutcome> {
        match self {
            Self::Tool { outcome, .. } => outcome.as_ref(),
            _ => None,
        }
    }

    /// READ rule 2 of `core/tool-outcome.ts`: absent means `completed`. This is the ONLY reading
    /// that keeps every pre-outcome transcript valid, and it is a deliberate migration decision
    /// rather than a bridge - nothing here re-derives an outcome from the failure prose, which is
    /// why a pruning policy must additionally require a superseding later success
    /// (`types/index.ts:276-291`).
    #[must_use]
    pub fn stored_outcome(&self) -> HarnessToolOutcome {
        self.outcome_flag()
            .copied()
            .unwrap_or(HarnessToolOutcome::Completed)
    }
}

// ---------------------------------------------------------------------------
// The only-`false`-is-visible boundary for a TS context flag
// (`types/index.ts:117-148`, `core/snapshot.ts:225-265`, `api.ts:899-949`)
// ---------------------------------------------------------------------------

/// `skip_serializing_if` for `included` / `reasoningIncluded`: the key is written ONLY for
/// `Some(false)`. `None` is "included" and must stay unwritten so a pre-flag transcript
/// keeps its exact old shape; `Some(true)` is a value the runtime never stores - a host
/// that wants a turn back DELETES the key (`api.ts:919-920`, `:944-945`) - so it is omitted
/// here too, which keeps `restore -> snapshot` idempotent (`core/snapshot.ts:226-228`).
fn context_flag_absent(flag: &Option<bool>) -> bool {
    *flag != Some(false)
}

/// `deserialize_with` for the same two fields: read a literal boolean and normalize `true`
/// to the absent form (`core/snapshot.ts:237-239` and `:262-264` both compare `=== false`
/// and write nothing otherwise, and `core/agent.ts:213-222` strips a `true` key from every
/// copy). Anything that is not a literal boolean is REFUSED, `null` included, matching TS's
/// `readSnapshotBoolean`; an absent key never reaches this function at all, because the
/// fields carry `#[serde(default)]`.
fn read_context_flag<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // `bool::deserialize` goes through `deserialize_bool`, so a string, a number, an object
    // or a `null` all fail with serde's own "invalid type" wording instead of silently
    // reading as included - the only divergence from TS is the WORDING, which no route
    // prints today because the transcript body is still an untyped `Value`
    // (`api/transcript.rs` module header) until the wave that validates it lands.
    let value = bool::deserialize(deserializer)?;

    // `true` is accepted and normalized away; `false` is the only value with a stored form.
    Ok(if value { None } else { Some(false) })
}

/// WRITE rule 1 of `core/tool-outcome.ts` (`storedOutcomeEntry`): the spread a message literal
/// wants, where `completed` and "never classified" BOTH mean no key at all. That is what keeps
/// every pre-outcome transcript, snapshot and fixture byte-identical, and it is the form every
/// writer here uses so the omission decision cannot be spelled three ways
/// (`types/index.ts:276-291`).
#[must_use]
pub fn stored_outcome_entry(outcome: HarnessToolOutcome) -> Option<HarnessToolOutcome> {
    match outcome {
        HarnessToolOutcome::Completed => None,
        other => Some(other),
    }
}

/// `skip_serializing_if` for the tool `outcome` key: a hand-set `Some(completed)` is dropped on
/// the way out too, so `restore -> snapshot` stays idempotent (`core/snapshot.ts:294-297`
/// normalizes the same value away on the way in).
fn stored_outcome_absent(outcome: &Option<HarnessToolOutcome>) -> bool {
    !matches!(
        outcome,
        Some(
            HarnessToolOutcome::Failed
                | HarnessToolOutcome::Rejected
                | HarnessToolOutcome::Cancelled
        )
    )
}

/// `deserialize_with` for the same field: read one declared value and normalize `completed` to
/// the absent form (`core/snapshot.ts:281-298 applyOutcome`). Anything the union does not admit
/// is REFUSED rather than read as `completed`, because a mistyped outcome must not hide a
/// failure (`core/tool-outcome.ts isToolOutcome`); the refusal sentence is TS's own.
fn read_stored_outcome<'de, D>(deserializer: D) -> Result<Option<HarnessToolOutcome>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;

    let outcome = match text.as_str() {
        // `completed` is stored as nothing at all, so it reads as the absent form.
        "completed" => return Ok(None),
        "failed" => HarnessToolOutcome::Failed,
        "rejected" => HarnessToolOutcome::Rejected,
        "cancelled" => HarnessToolOutcome::Cancelled,
        other => {
            return Err(serde::de::Error::custom(format!(
                "outcome must be one of: completed, failed, rejected, cancelled (or be omitted); received \"{other}\"."
            )));
        }
    };

    Ok(Some(outcome))
}

/// A finished assistant turn (`types/index.ts:165`): the payload of the `done` event and
/// the value the harness resolves to, so it carries everything a CLI wants to print.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessAssistantMessage {
    /// Always `assistant`; kept explicit because the TS record carries it verbatim.
    pub role: MessageRole,
    pub content: String,
    /// Always present; empty for non-reasoning models (`:169`).
    pub reasoning: String,
    /// Empty when the model answered directly.
    pub tool_calls: Vec<HarnessToolCall>,
    /// The model that produced this turn, captured at request time.
    pub model: String,
    pub stop_reason: HarnessStopReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<HarnessUsage>,
}

/// INVOCATION identity carried by every non-root event (`types/index.ts:1733`). Its id is
/// stable for one invocation and unique within the owning turn; `parent_id` is omitted for
/// a direct child of the root, and `input` appears on the `start` event only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessThread {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    /// What spawned this non-root run. Optional: historical recorded events predate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<InvocationKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<String>,
}

/// How a non-root invocation was created (`types/index.ts:1744`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InvocationKind {
    Delegation,
    Fork,
    AutoAnswer,
}

/// One declarative block inside a panel (`harness-api.ts:738` / `types/index.ts:1017`),
/// discriminated by `type`. No block carries markup or an action, which is the whole
/// security story of panels. `keyValue` keeps the TS camelCase tag spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ToolUiBlock {
    Text {
        text: String,
    },
    Table {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    KeyValue {
        items: Vec<ToolUiKeyValueItem>,
    },
}

/// One `keyValue` row (`harness-api.ts:743`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolUiKeyValueItem {
    pub label: String,
    pub value: String,
}

/// One declarative panel a tool supplied with its result (`types/index.ts:1022`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolUiPanel {
    /// Stable within one tool result, so a client can update a panel in place.
    pub id: String,
    /// Placement hint; an OPEN string by contract (`:1005`), `right` being the default mount.
    pub slot: String,
    pub title: String,
    pub blocks: Vec<ToolUiBlock>,
}

// ---------------------------------------------------------------------------
// `HarnessEvent` - one recorded progress event, discriminated by `type`
// (`types/index.ts:1866-2030`, contracted flat at `harness-api.ts:2030-2052`)
// ---------------------------------------------------------------------------

/// Every model-facing event. The TS contract states ONE closed record whose per-variant
/// fields are optional; the recorder emits the discriminated union above, and the durable
/// turn record stores those objects VERBATIM (`api-reads.ts` casts rather than rebuilds),
/// so the union is what actually travels the wire and is what this enum models.
///
/// Rules that must survive a Rust rewrite:
///   * every event names its `agent`, so a multi-agent run stays readable in one stream;
///   * `thread` (invocation identity) rides ONLY on a non-root run (`types/index.ts:1864`);
///   * field spellings inside an event are camelCase (`callId`, `durationMs`,
///     `maxAttempts`, `delayMs`, `requestId`) EXCEPT the `type` tag, which is snake_case
///     (`attempt_commit`), and `isError`, which is already camelCase;
///   * `isError` is always `outcome != completed` (`types/index.ts:1882`) - the two are
///     never allowed to disagree, so `tool_result.is_error` is stored, not derived on read;
///   * a `retry` frame discards everything streamed since the previous boundary, and
///     `attempt_commit` is what makes a completed tool round final (`:1948-1975`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HarnessEvent {
    /// Opens a run with its agent and model (`types/index.ts:1867`).
    #[serde(rename_all = "camelCase")]
    Start {
        agent: String,
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// One answer delta (`:1868`).
    #[serde(rename_all = "camelCase")]
    Text {
        agent: String,
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// One reasoning delta (`:1869`).
    #[serde(rename_all = "camelCase")]
    Reasoning {
        agent: String,
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// The call the model requested (`:1870`); arguments are still a raw JSON string.
    #[serde(rename_all = "camelCase")]
    ToolCall {
        agent: String,
        call: HarnessToolCall,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// The answer to one call (`:1874-1917`). `call_id` is optional so an older recorded
    /// event stays valid, and a same-name pairing in a UI can still be exact.
    #[serde(rename_all = "camelCase")]
    ToolResult {
        agent: String,
        name: String,
        content: String,
        is_error: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<HarnessToolOutcome>,
        /// Panels a tool supplied with `toolUi()`; the model never sees them (`:1890-1894`).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        ui: Vec<ToolUiPanel>,
        /// Media DESCRIPTORS only - never base64 - because every event round-trips (`:1895-1902`).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        media: Vec<ToolMediaDescriptor>,
        /// Wall-clock span of an EXECUTED call; an immediate validation failure never
        /// executed and reports nothing (`:1903-1909`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        /// Calls STILL running when this result was delivered (`:1910-1914`).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pending: Vec<HarnessPendingTool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// Per-request token accounting, emitted for EVERY model request including tool rounds
    /// (`:1918-1925`): the usage events, not `done`, are the complete cost record.
    #[serde(rename_all = "camelCase")]
    Usage {
        agent: String,
        usage: HarnessUsage,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// A model response. A non-empty `pending` means the run is NOT terminal (`:1926-1941`).
    #[serde(rename_all = "camelCase")]
    Done {
        agent: String,
        message: HarnessAssistantMessage,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pending: Vec<HarnessPendingTool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// A discarded attempt of THIS request, with the backoff that precedes the next one
    /// (`:1942-1962`). `attempt` is 1-based; `attempt == max_attempts` failing is terminal.
    #[serde(rename_all = "camelCase")]
    Retry {
        agent: String,
        attempt: u64,
        max_attempts: u64,
        delay_ms: u64,
        error: HarnessErrorInfo,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// One model request SUCCEEDED (`:1963-1980`): everything streamed since the previous
    /// boundary is committed and a later `retry` must not discard it.
    #[serde(rename_all = "camelCase")]
    AttemptCommit {
        agent: String,
        attempt: u64,
        max_attempts: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// Heartbeat while background calls are pending (`:1981-1992`); a progress event, not
    /// a transcript entry.
    #[serde(rename_all = "camelCase")]
    Wakeup {
        agent: String,
        reason: WakeupReason,
        pending: Vec<HarnessPendingTool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// The model asked its owner a bounded question (`:1993-2001`); `request` is already
    /// parsed and bounds-checked, and `call_id` is the actionRequest tool call.
    #[serde(rename_all = "camelCase")]
    ActionRequest {
        agent: String,
        call_id: String,
        request: HarnessActionRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// An answer was recorded, owner or automatic (`:2002-2011`).
    #[serde(rename_all = "camelCase")]
    ActionAnswer {
        agent: String,
        request_id: String,
        answer: HarnessActionAnswer,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// A fork/autoanswer branch started (`:2012-2019`). `branch` is the branch SUMMARY a
    /// client can use before the transcript exists - carried as `Value`, because the
    /// branch record is the runtime owner's shape (`types/index.ts:931`).
    #[serde(rename_all = "camelCase")]
    BranchStart {
        agent: String,
        branch: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// A branch reached a terminal state (`:2020-2029`); `error` is absent for a completed
    /// branch and present for a failed or cancelled one.
    #[serde(rename_all = "camelCase")]
    BranchEnd {
        agent: String,
        branch_id: String,
        status: BranchTerminalStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<HarnessErrorInfo>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
    /// A run ended with a failure (`:2030`).
    #[serde(rename_all = "camelCase")]
    Error {
        agent: String,
        error: HarnessErrorInfo,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<HarnessThread>,
    },
}

/// The only `wakeup` reason the runtime reports (`types/index.ts:1989` `'pending-tools'`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WakeupReason {
    PendingTools,
}

/// The terminal statuses a `branch_end` may report (`types/index.ts:957`); `running` is
/// deliberately excluded because it is not terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BranchTerminalStatus {
    Completed,
    Failed,
    Cancelled,
}

// ---------------------------------------------------------------------------
// The recursive display subtree
//
// `harness-api.ts` states these as closed records whose kind-specific fields are
// optional, while `shared/turn-display.ts` emits a discriminated union of narrower
// members (the PROJECTION DELTAS note at `harness-api.ts:27-36`). The wire form is
// therefore a document the projector produces and the HTTP layer forwards verbatim
// (`api-reads.ts` casts `projector.display()` rather than restating it), which is
// exactly what a `Value` alias models: no re-derivation, no drift.
// ---------------------------------------------------------------------------

/// One agent flow (`harness-api.ts:1850`): the turn's root invocation, one nested
/// invocation, or one branch. Nesting is resolved by the SERVER from the recorded
/// invocation identity, so a renderer never infers a parent.
pub type TurnSection = serde_json::Value;

/// One node of a section's flow (`harness-api.ts:1774`), discriminated by `kind`:
/// `text`, `reasoning`, `step`, or `notice`.
pub type DisplayNode = serde_json::Value;

/// One tool or action row (`harness-api.ts:1792`); `id` is the opening event index as a
/// decimal string, which is also the step detail route's key.
pub type DisplayStep = serde_json::Value;

/// The live tail of one flow (`turn-display.ts:856`): the partial window the projector
/// keeps for the segment currently streaming.
pub type DisplayTail = serde_json::Value;

/// One compiled display mutation on the stream (`harness-stream.ts:130`), addressed by id
/// so a client applies it without understanding what happened.
pub type TurnDisplayOp = serde_json::Value;

// ---------------------------------------------------------------------------
// Boot documents, paging meta, and the error envelope
// ---------------------------------------------------------------------------

/// The one error envelope every failing route returns (`harness-api.ts:276` `ErrorBody`,
/// i.e. `ApiErrorBody` of `server/protocol.ts`). It is the only response shape shared by
/// all failures, so a client has one error path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

/// The single member of the envelope (`{ "error": { "message": ... } }`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub message: String,
}

impl ErrorBody {
    /// Built from the one error type the server raises (`crate::error::ApiError`).
    #[must_use]
    pub fn from_error(error: &ApiError) -> Self {
        Self {
            error: ErrorDetail {
                message: error.message.clone(),
            },
        }
    }
}

/// The fields every page carries (`harness-api.ts:264`).
///
/// `limit` echoes the EFFECTIVE page size - what this page actually carries - which is
/// how a client detects a short final page without comparing against its request
/// (`api-paging.ts:146-148`). `next_cursor` is null at the end of the collection, and is
/// ALWAYS present (TS prints the literal `null`), so it is not `skip_serializing_if`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PageMeta {
    pub limit: u64,
    pub next_cursor: Option<String>,
    pub observed_at: u64,
    pub revision: u64,
}

/// Liveness plus the number of sessions the store currently holds (`harness-api.ts:286`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HealthResponse {
    /// TS types this as the literal `true`; a bool keeps the wire value identical.
    pub ok: bool,
    pub sessions: u64,
    pub epoch: String,
}

/// What THIS deployment offers, stated as booleans so a client hides an affordance
/// instead of discovering a 403/404 by clicking it (`harness-api.ts:298`). A `false`
/// means the routes exist and answer 403/404, not that another path exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServiceCapabilities {
    pub websocket_stream: bool,
    pub tool_execution: bool,
    pub tool_registration: bool,
    pub transcript_editing: bool,
    pub command_dispatch: bool,
    pub model_discovery: bool,
    pub harness_definition_writes: bool,
    pub agent_writes: bool,
    pub persistent_threads: bool,
}

/// The numeric envelope every route is bounded by (`harness-api.ts:316`). These are
/// contract numbers: a route refuses a value above them with 400/413/415/429.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServiceLimits {
    pub body_bytes: u64,
    pub submit_body_bytes: u64,
    pub input_chars: u64,
    pub pending_tasks: u64,
    pub thread_records: u64,
    pub attachments_per_turn: u64,
    pub attachment_bytes: u64,
    pub total_attachment_bytes: u64,
    pub result_references: u64,
    pub pending_actions: u64,
    pub steer_records: u64,
    pub created_tools: u64,
    pub tool_result_bytes: u64,
    pub page_limit_default: u64,
    pub page_limit_max: u64,
}

/// The boot document (`harness-api.ts:339`): identity, capability advertisement, contract
/// limits, and links - nothing else, so booting a client costs one constant-size request.
/// `build_version` is the crate version (`crate::version()`), omitted when a build does
/// not know it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServiceInfo {
    pub api_root: String,
    pub service: String,
    pub api_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_version: Option<String>,
    pub server_time: u64,
    pub epoch: String,
    pub capabilities: ServiceCapabilities,
    pub limits: ServiceLimits,
    /// Route links, keyed by route id (`api.ts:1293-1303`): `self`, `health`, `config`,
    /// `harnesses`, `agents`, `tools`, `models`, `sessions`, `stream`.
    pub links: BTreeMap<String, String>,
}

/// Key-free provider defaults advertised to clients (`harness-api.ts:358`): the API key
/// itself is NEVER sent, only whether one exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ProviderDefaults {
    pub base_url: String,
    pub model: String,
    pub has_api_key: bool,
}

/// The provider settings a caller may change per session or per discovery request
/// (`harness-api.ts:369`). Partial by construction: an omitted field keeps the configured
/// default, and `api_key` is write-only (it travels only in a request body).
///
/// A JSON `null` deserializes to `None`, which is exactly how `api.ts:548-561` treats it:
/// `null` is "not supplied", never "the empty string".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunnerSettingsInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// DEPLOYMENT-owned execution configuration, readable for diagnosis and never writable
/// over HTTP (`harness-api.ts:381`, projected by `api-reads.ts:156`). A bad value is a
/// startup failure, so a client that sends one in a write body gets a 400 naming it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RuntimeSettings {
    pub thread_execution: String,
    pub worker_idle_ms: u64,
    pub max_lane_workers: u32,
    pub worker_handshake_ms: u64,
    /// Present only when an explicit entry was configured (`api-reads.ts:161-163`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_entry_configured: Option<bool>,
}

/// GLOBAL configuration (`harness-api.ts:394`): the session-visible provider defaults
/// plus read-only deployment runtime facts. A session's own merged settings are
/// per-session state and are reported on the session, never here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GlobalConfigDocument {
    pub defaults: ProviderDefaults,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeSettings>,
    /// The only writable paths over HTTP (`api.ts:1332`).
    pub writable_fields: Vec<String>,
    pub revision: u64,
}

/// Whole-field patch of the global defaults (`harness-api.ts:407`). Each inner field is
/// `string | null`: a string writes, `null` restores the shipped default, an omitted field
/// is no change, and `api.ts:1353` refuses an empty patch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GlobalConfigUpdateRequest {
    pub settings: ConfigSettingsPatch,
}

/// The `settings` block of [`GlobalConfigUpdateRequest`]. `Option<Option<String>>` is how
/// "absent" and "explicit null" stay distinguishable, which the credential rule needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ConfigSettingsPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<Option<String>>,
}

/// The endpoint settings a client would use for one model-discovery call
/// (`harness-api.ts:772`); absent means "use the host defaults".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ModelDiscoveryRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<RunnerSettingsInput>,
}

/// The provider's model ids in provider order, deduplicated (`harness-api.ts:781`). An
/// empty array is a valid answer: the provider answered and served nothing usable, which
/// is different from 502.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ModelIndex {
    pub models: Vec<String>,
    pub observed_at: u64,
}

// ---------------------------------------------------------------------------
// Harness definitions
// ---------------------------------------------------------------------------

/// One named harness definition as a picker needs it (`HarnessCard`, swagger `:4702-4765`,
/// `harness-api.ts:634-680`).
///
/// THREE INDEPENDENT FACTS, NOT ONE DERIVED SET: `agents` is the roster in DECLARATION ORDER,
/// `entry` is ROUTING, and `ranks` is AUTHORITY. Swagger `:4707-4712` states the rule the earlier
/// port violated - "`agents` is the roster in DECLARATION ORDER - it is not derived from anything,
/// and neither `entry` nor `ranks` is derived from it" - so nothing here computes one from another:
/// position in the roster states no authority, and the only place order matters is the DEFAULT for an
/// omitted `entry`. A card omits per-agent instruction TEXT, the per-agent model map, and the
/// resolved core-tool set; the identified read adds those.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HarnessCard {
    pub name: String,
    pub title: String,
    pub description: String,
    pub requires_endpoint: bool,
    /// THE ENTRY AGENT (routing only): the member an unfocused submit opens a conversation for, and
    /// the member a session refuses to delete. It carries whatever letter `ranks` gives it, and a
    /// definition with several top-ranked members still has exactly one entry.
    pub entry: String,
    /// THE RANK MAP, and it ALWAYS serializes: it covers EVERY name in `agents` because the server
    /// resolves a sparser request into it with the runtime's own fill rule (swagger `:4722-4733`).
    /// A card states what the harness RUNS, never a subset an operator happened to write.
    pub ranks: BTreeMap<String, AgentRank>,
    /// THE ROSTER, in declaration order.
    pub agents: Vec<String>,
    pub tools: Vec<String>,
    pub builtin: bool,
    pub definition_revision: u64,
}

/// One member's instruction PROVENANCE AND LENGTH with no text, so a definition read stays
/// constant-size however long the prompts are (`harness-api.ts:442`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HarnessInstructionSummaryEntry {
    pub agent: String,
    pub source: InstructionSource,
    pub overridden: bool,
    pub chars: u64,
}

/// The identified read of one definition (`harness-api.ts:453`): a card plus the per-agent
/// model overrides, the resolved core-tool set, and the instruction summary. Modeled as a
/// flat struct that FLATTENS the card, because TS composes it additively (`HarnessCard &`)
/// and the wire object is one flat record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessDefinition {
    #[serde(flatten)]
    pub card: HarnessCard,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    /// The per-agent display COLORS (`harness-api.ts:460`, `api-reads.ts:1104-1106`), emitted under
    /// the same non-empty rule as `agent_models`, so an uncolored definition keeps its exact
    /// pre-R2 shape. Position matters: TS spreads `agent_colors` between `agent_models` and
    /// `instruction_summary`, and `preserve_order` makes the declared order the wire order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_colors: Option<BTreeMap<String, String>>,
    /// The per-agent PRESET-HISTORY bands (swagger `:4859-4883`).
    ///
    /// THE ONE MAP WITHOUT THE NON-EMPTY RULE ON THE DEFINITION READ: the identified read ALWAYS
    /// states it, `{}` when the definition seeds nothing, because a harness PATCH REPLACES the map
    /// and a present `{}` is the explicit clear - so a client may only send the map it has READ, and
    /// "absent" would otherwise be ambiguous between "no preset-history editor" and "nothing seeded".
    /// `harness_definition_of` therefore always supplies `Some` (even an empty map), while the
    /// SAVED SESSION's copy keeps the non-empty rule (`api-reads.ts` `sessionDefinitionOf`, and
    /// `sessions.ts:1638-1650`, which deletes an empty map), so the field is `Option` here and the
    /// two projections decide.
    ///
    /// A member keyed `[]` is echoed back: an explicit "no preset history" is a stated choice, which
    /// is why an empty BAND keeps its key while an empty MAP is the clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_prefixes: Option<BTreeMap<String, Vec<HarnessPrefixTurn>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_summary: Option<Vec<HarnessInstructionSummaryEntry>>,
}

/// Envelope of a definition read, create, or patch (`harness-api.ts:511`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HarnessDefinitionResponse {
    pub harness: HarnessDefinition,
}

/// Page of definition cards (`harness-api.ts:502`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HarnessCardPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<HarnessCard>,
}

/// Envelope of the instruction index (`harness-api.ts:520`): one row per member with
/// provenance and length, no text, and no page cursor because the member set is bounded by
/// the definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HarnessInstructionIndexResponse {
    pub harness_name: String,
    pub total: u64,
    pub items: Vec<HarnessInstructionSummaryEntry>,
}

/// One member's effective instruction text, with its provenance (`harness-api.ts:530`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HarnessInstructionTextResponse {
    pub harness_name: String,
    pub agent: String,
    pub source: InstructionSource,
    pub instruction: String,
    pub guidelines: Vec<String>,
    pub chars: u64,
}

/// Receipt of a definition removal (`harness-api.ts:545`). Deliberately minimal: the
/// authoritative record is that the entry is gone - and, for a built-in, that its name is
/// tombstoned so a restart does not re-ship it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeleteHarnessResult {
    pub deleted: bool,
    pub harness_name: String,
    pub tombstoned: bool,
    pub restored_by_restart: bool,
}

/// Selection-only create body (`CreateHarnessRequest`, swagger `:4888-4981`): a client picks trusted
/// agent names and may record per-agent model ids, instruction overrides, preset-history bands, and
/// display colors; it cannot supply a provider, a credential, a transport, or a tool list.
///
/// THE ROSTER IS `agents`, in declaration order, and it is the ONLY roster field: `required`
/// `[name, agents]`, with `entry`, `ranks`, and the four maps optional. `primary` and `subagents` are
/// NOT accepted as aliases - they land in the `additionalProperties: false` refusal that names the
/// accepted set (swagger `:3998`, `api.ts:2372-2386`).
///
/// WHY `agents`, `entry`, `ranks`, and `agent_prefixes` ARE RAW `Value`s
/// ---------------------------------------------------------------------
/// TypeScript's route forwards all four UNVALIDATED (`api.ts:2408-2425`: "this handler only moves
/// bytes"), and the registry owns every refusal - the roster sentence, the membership sentence that
/// lists the members, and the rank sentence that quotes the scale. A typed field would let serde
/// answer first with serde's wording, so the shape stays JSON here and `crate::registry` validates
/// it, which is the split the contract documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateHarnessRequest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// THE ROSTER: at least one agent, at most 16, in declaration order, no duplicates. A harness
    /// with no agent has no entry and no rank, so it could never run a turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<Value>,
    /// THE ENTRY AGENT (routing). Omitted means the FIRST roster name - the only default the rank
    /// model allows, because position states no authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<Value>,
    /// THE RANK MAP. May name fewer members than the roster; every member it leaves out is filled one
    /// rank BELOW the lowest rank it does state, and the read returns the RESOLVED map. Omitted
    /// ENTIRELY is the ordinary team (entry at `A`, everyone else at `B`); an explicit `{}` is the
    /// flat team (everyone at `B`, no delegation at all).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ranks: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instructions: Option<BTreeMap<String, String>>,
    /// The fourth map of the same shape (`agent_prefixes`, swagger `:4953-4976`): agent name to that
    /// member's complete ordered preset-history band. A key REPLACES that member's whole band, an
    /// explicit `[]` states "starts with no preset history", and `{}` clears every band.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_prefixes: Option<Value>,
    /// The third map of the same shape (`harness-api.ts:484`): agent id -> CSS hex color. A client
    /// may record a swatch for each member it selects; the registry decides membership and the hex
    /// shape (`registry.ts:604-629`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_colors: Option<BTreeMap<String, String>>,
}

/// Patch body (`UpdateHarnessRequest`, swagger `:4982-5054`): an omitted field keeps its current
/// value and the name is immutable. `agents` REPLACES the roster, `entry` REPLACES routing, `ranks`
/// REPLACES the authority map, and the four per-agent maps REPLACE the map they name (`{}` clears
/// it). An override for a member this patch removes is dropped rather than rejected.
///
/// The same raw-`Value` reasoning as [`CreateHarnessRequest`] applies to `agents`, `entry`, `ranks`,
/// and `agent_prefixes`: the route forwards them unvalidated and the registry owns the sentences.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateHarnessRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ranks: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instructions: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_prefixes: Option<Value>,
    /// REPLACE semantics like the maps beside it (`harness-api.ts:507`): `{}` clears every color,
    /// and a color for a member this patch removes is dropped rather than rejected
    /// (`registry.ts:1377-1389`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_colors: Option<BTreeMap<String, String>>,
}

// ---------------------------------------------------------------------------
// Trusted agent profiles
// ---------------------------------------------------------------------------

/// One trusted profile without its text (`harness-api.ts:557`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AgentSummary {
    pub name: String,
    pub title: String,
    pub description: String,
    pub has_instruction_override: bool,
    pub has_global_override: bool,
}

/// The full trusted profile as a client needs it to choose a primary or subagent
/// (`harness-api.ts:571`). Every text field is trusted catalogue content, never client
/// input and never secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AgentProfile {
    pub name: String,
    pub title: String,
    pub description: String,
    pub instruction: String,
    pub guidelines: Vec<String>,
    pub instruction_source: InstructionSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overridden_fields: Option<Vec<OverriddenAgentField>>,
    pub referenced_by: Vec<String>,
    pub revision: u64,
}

/// Which shipped fields a global override currently replaces (`harness-api.ts:578`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OverriddenAgentField {
    Title,
    Description,
    Instruction,
    Guidelines,
}

/// Page of agent summaries (`harness-api.ts:584`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSummaryPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<AgentSummary>,
}

/// Envelope of an agent profile read or patch (`harness-api.ts:593`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentProfileResponse {
    pub agent: AgentProfile,
}

/// Declarative text overrides only (`harness-api.ts:604`): a value REPLACES the whole field
/// it names (`guidelines: []` is an explicit clear), an omitted field is no change, and an
/// explicit `null` restores the shipped catalogue value - hence `Option<Option<..>>`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateAgentRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guidelines: Option<Option<Vec<String>>>,
}

/// Receipt of a profile retirement (`harness-api.ts:616`): the profile is unavailable for
/// NEW sessions and member selection, and nothing else changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeleteAgentResult {
    pub deleted: bool,
    pub agent_id: String,
    pub tombstoned: bool,
    pub restored_by_restart: bool,
}

// ---------------------------------------------------------------------------
// Tool catalogue and executions
// ---------------------------------------------------------------------------

/// Whether the server can actually run a listed tool (`harness-api.ts:628`). `environment`
/// names environment VARIABLES only - never values - and `hint` is a non-secret sentence a
/// client can print verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolAvailability {
    pub state: ToolAvailabilityState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

/// One catalogue entry as a list projects it (`harness-api.ts:640`). The argument JSON
/// Schema is on the identified read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolSummary {
    pub name: String,
    pub group: String,
    pub description: String,
    pub tags: Vec<String>,
    pub visibility: ToolVisibility,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub availability: Option<ToolAvailability>,
    pub session_bound: bool,
}

/// A catalogue entry plus what only the identified read carries (`harness-api.ts:654-665`):
/// the argument schema the model is given, and the execution gate this entry supports.
/// `execution` is REQUIRED and always the `{scope, manual}` object (`:660-664`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDetail {
    #[serde(flatten)]
    pub summary: ToolSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    pub execution: ToolExecutionScope,
}

/// Page of catalogue rows (`harness-api.ts:668`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSummaryPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<ToolSummary>,
}

/// Envelope of one catalogue entry read (`harness-api.ts:677`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDetailResponse {
    pub tool: ToolDetail,
}

/// A global manual call (`harness-api.ts:687`). `parameters` is DELIBERATELY untyped: the
/// tool - not this contract - decides what is valid and reports a bad argument as an
/// `ok: false` result.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GlobalToolExecutionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
}

/// A session manual call: the session is already in the path, so no body can reach another
/// session's registry (`harness-api.ts:696`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionToolExecutionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
}

/// A call that executed and reported success (`harness-api.ts:707`). `result` is the tool's
/// ordinary value, already unwrapped from a `toolUi(content, panels)` envelope; media
/// entries are DESCRIPTORS, never base64, because a manual call has no transcript to attach
/// bytes to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolCallSuccess {
    pub ok: bool,
    pub result: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub panels: Vec<ToolUiPanel>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media: Vec<ToolMediaDescriptor>,
    pub duration_ms: u64,
}

/// A call that executed and reported failure (`harness-api.ts:720`). This is still a 200
/// response: the invocation succeeded and the tool answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolCallFailure {
    pub ok: bool,
    pub error: HarnessErrorInfo,
    pub duration_ms: u64,
}

/// The outcome of one manual tool execution (`harness-api.ts:730`). A client branches on
/// `ok`, never on the status code alone, because both outcomes are 200.
///
/// UNTAGGED, matching the TS union: `ok` is a boolean on the wire (not a string tag), so
/// serde's internal-tagging cannot key on it. `Failure` is listed FIRST because it is the
/// narrower shape (it requires `error`), and an untagged match tries members in order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolCallResult {
    Failure(ToolCallFailure),
    Success(ToolCallSuccess),
}

impl ToolCallResult {
    /// The `ok` discriminator, read without destructuring the payload.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success(_))
    }

    /// The measured span of the call, which both members carry.
    #[must_use]
    pub fn duration_ms(&self) -> u64 {
        match self {
            Self::Success(success) => success.duration_ms,
            Self::Failure(failure) => failure.duration_ms,
        }
    }
}

// ---------------------------------------------------------------------------
// Built-in plugin catalogue (`GET {root}/plugins`, `PATCH {root}/plugins/{plugin_id}`)
// ---------------------------------------------------------------------------

/// The declared type of one plugin setting (`harness-api.ts:667 PluginSetting.type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginSettingType {
    Number,
    String,
    Boolean,
}

/// The visual section one setting row is drawn under (`PluginSetting.group`,
/// `harness-server.swagger.yaml:5385-5407`). DISPLAY hint ONLY: the server validates, stores and
/// returns the value without consulting it, so this struct is a projection and never an input.
/// Both members are REQUIRED by the contract once the group is present (`required: [id, title]`),
/// and the whole group is absent — never `null` — for a row the plugin never grouped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginSettingGroup {
    /// Stable machine id of the section. Rows sharing an `id` render under one heading, in
    /// first-appearance order.
    pub id: String,
    /// The human heading a renderer prints verbatim.
    pub title: String,
}

/// One DECLARED setting of a plugin, copied verbatim from the plugin's own metadata
/// (`harness-api.ts:1071`). `default` is the value the plugin falls back to when nothing is
/// stored AND the target a `null` patch entry restores. `min`/`max` bound what the server
/// accepts; `step` bounds only what a client offers and is deliberately NOT enforced
/// (`server/plugins.ts` header, "VALIDATION AT THE WRITE EDGE").
///
/// The numeric members are `Value`, not `f64`, because the wire spells an integral bound as an
/// INTEGER (`min: 1000`, never `1000.0`): serde_json prints an `f64` with its fraction, and the
/// contract's rows are asserted byte-exact. `crate::plugins` owns the one conversion that keeps
/// that promise.
///
/// C1/R2 added `group` and `parentKey`, the two DISPLAY hints the contract declares after
/// `description` (YAML `PluginSetting`, `harness-api.ts:1096-1112`). They say WHERE a row is drawn,
/// never what its value means, so validation, defaults and persistence ignore them and this crate
/// treats them as pure projection. `parentKey` keeps the contract's camelCase — the display
/// subtree is the one place on this contract that does not use snake_case, so the field is named
/// `parent_key` in code and renamed on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginSetting {
    pub key: String,
    pub label: String,
    /// `type` is a Rust keyword, so the field is named `kind` in code and renamed on the wire.
    #[serde(rename = "type")]
    pub kind: PluginSettingType,
    pub default: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The section this row belongs to, when the plugin grouped it; absent renders the row in the
    /// flat top-level run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<PluginSettingGroup>,
    /// Key of the BOOLEAN setting that gates this row. camelCase on the wire.
    #[serde(default, rename = "parentKey", skip_serializing_if = "Option::is_none")]
    pub parent_key: Option<String>,
}

/// One built-in plugin as an operator sees it (`harness-api.ts:1118`): identity, the lifecycle
/// hooks it declares, its settings descriptors, and the values in force RIGHT NOW.
///
/// `builtin` is always `true` on this route - the catalogue is the set this build ships - and it
/// stays explicit on the wire so a client renders one row shape for a plugin a future release
/// makes configurable. The optional members are ABSENT, never `null`: a plugin that declares no
/// description, version, tags or hooks omits those keys (`api.ts pluginRowOf`'s presence filters).
///
/// `enabled` is the framework's global switch (`api.ts:2360 plugins.enabled(name)`, the reserved
/// `enabled` settings key - `server/plugins.ts PLUGIN_ENABLED_KEY`). It is REQUIRED, not optional:
/// every plugin has one whether or not it declares the setting, and the row reads the same stored
/// value the hook gate consults, so a row that says `false` means that plugin runs no hooks in ANY
/// session right now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginSummary {
    pub name: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// C1/R2: the system prompt this plugin sends to the model, shown READ-ONLY in the details
    /// pane (`harness-api.ts:1128-1141`, YAML `PluginSummary.system_prompt`). Display metadata
    /// only - it never shapes behaviour - and ABSENT-not-null for a plugin that never prompts,
    /// which is what keeps every other row's shape byte-identical. snake_case like every other
    /// multi-word key on this contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Vec<String>>,
    pub builtin: bool,
    /// Whether the plugin RUNS right now: the stored `enabled` override, otherwise its declared
    /// `enabled` default, otherwise `true` (`server/plugins.ts` `enabled()` - the same read the
    /// hook gate makes). Required by the contract (`harness-api.ts:690`), never absent.
    pub enabled: bool,
    pub settings: Vec<PluginSetting>,
    /// The EFFECTIVE map (`declared default` unless the operator stored an override), in the
    /// plugin's own DECLARED key order (`api.ts:2362 plugins.values(metadata.name)`).
    pub values: serde_json::Map<String, serde_json::Value>,
}

/// Page of built-in plugin rows (`harness-api.ts:698`). Modeled as a flat struct that FLATTENS
/// `PageMeta` exactly like `HarnessCardPage`, so the five page members keep their contract names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginSummaryPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<PluginSummary>,
}

/// Envelope of one plugin row, read or patched (`harness-api.ts:708`). A patch returns the WHOLE
/// row rather than a receipt, because the values it reports ARE the result of the write and an
/// editor needs the reset targets beside them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDetailResponse {
    pub plugin: PluginSummary,
}

/// Settings patch for one plugin (`harness-api.ts:721`). A value REPLACES that key's override, an
/// explicit `null` clears it (the declared default applies again), and an omitted key is no
/// change. Unknown keys are refused rather than ignored, and there is no enable/disable field:
/// plugin loading is auto-scanned, so no HTTP affordance exists for it.
///
/// The map is a raw `Map`, not a typed union: a submitted value is validated by the plugin's OWN
/// descriptor (`crate::plugins`), which is the only place that knows each key's declared type and
/// bounds. A malformed value therefore answers the descriptor's sentence, not a serde one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginSettingsUpdate {
    pub settings: serde_json::Map<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// One session as a list row (`harness-api.ts:812`): metadata and counters, nothing else.
/// Deliberately absent: turns, events, the thread roster, transcripts, display trees, the
/// frozen definition, and the live `agent_models` map - which is what makes a rail of a
/// hundred long conversations a constant-size response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionSummary {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub harness: String,
    pub status: SessionStatus,
    /// Queued work only; a running turn does not appear here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<u64>,
    pub model: String,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub turn_count: u64,
    pub thread_count: u64,
    pub revision: u64,
}

/// The identified read of one session (`harness-api.ts:832`): everything a row carries plus
/// the live model map, the key-free endpoint view, the created-tool and pending-action
/// counts, and where turns actually execute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionResource {
    #[serde(flatten)]
    pub session: SessionSummary,
    pub definition_revision: u64,
    pub agent_models: BTreeMap<String, String>,
    pub endpoint: ProviderDefaults,
    pub runtime_execution: String,
    pub pending_action_count: u64,
    pub created_tool_count: u64,
    pub created_tool_limit: u64,
    /// T1.5's last mile (ADDITIVE, and ABSENT when empty): the plugin diagnostics this session reported,
    /// each naming its OWNING plugin (`api-reads.ts:242-256`). This is the surface that makes "the
    /// compactor is broken" answerable from the identified session read instead of only from stderr.
    ///
    /// The entries travel as raw JSON VALUES, in the order the sink wrote them - the same "stored by value,
    /// copied on the way out" rule the unreadable-transcript records follow, and what keeps a consumer from
    /// mutating the stored journal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_diagnostics: Option<Vec<serde_json::Value>>,
}

/// Envelope of a session read, create, or patch (`harness-api.ts:851`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionResponse {
    pub session: SessionResource,
}

/// Page of session rows (`harness-api.ts:856`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummaryPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<SessionSummary>,
}

/// Receipt of a disposal (`harness-api.ts:866`): reports what was discarded and nothing else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeleteSessionResult {
    pub deleted: bool,
    pub session_id: String,
    pub aborted_turns: u64,
    pub dropped_queued_tasks: u64,
}

/// The harness definition this session captured at creation (`harness-api.ts:878`), which
/// is the authority for its member set, delegation allowlist, declared and core tools, and
/// per-agent overrides. A saved session restores from this record, so a later global edit
/// cannot move it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionDefinitionResource {
    #[serde(flatten)]
    pub definition: HarnessDefinition,
    pub session_id: String,
    pub revision: u64,
    pub frozen_at: u64,
    pub source_revision: u64,
    pub current_revision: u64,
    pub current: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_tools: Option<Vec<String>>,
}

/// Envelope of the frozen-definition read (`harness-api.ts:893`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionDefinitionResponse {
    pub definition: SessionDefinitionResource,
}

/// Selects the definition to freeze and optionally overrides the endpoint settings for THIS
/// session only (`harness-api.ts:903`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateSessionRequest {
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<RunnerSettingsInput>,
}

/// The session-level writes, both declarative (`harness-api.ts:918`). `agent_models`
/// MERGES; `title` REPLACES and an explicit `null` clears it; `{}` is a 400 no-op.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateSessionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
}

/// One entry of a session's live agent roster (`harness-api.ts:1756-1847`): a row states both what
/// the session was BUILT with and what it RUNS now. Instruction TEXT lives on the
/// instruction subresource.
///
/// THE AUTHORITY MEMBER IS `rank`, NOT `role` (R1). The contract's `required` list is
/// `[agent_id, profile, rank, origin, model, inherited_model, instruction_source, instruction_chars,
/// editable, deletable, restorable, revision]` under `additionalProperties: false`, and the generated
/// row is `rank: AgentRank` (`engine/shared/harness-api.ts:1770`), so emitting a `role` member was a
/// double break: a validating client saw a missing required `rank` AND an undeclared `role`. This
/// struct therefore carries neither - the letter is projected by
/// [`crate::store::read::session_agent_rank`], and the retired enum stays store-side
/// ([`AgentRole`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionAgentBinding {
    pub agent_id: String,
    pub profile: String,
    /// The letter this session RUNS this member at: the session's own override when it set one, else
    /// the frozen harness definition's rank map, else the LOWEST letter (`Z`) - never a rank this
    /// build invented (`api-reads.ts:1564`, `rankOfMember`).
    pub rank: AgentRank,
    pub origin: BindingOrigin,
    pub model: String,
    pub inherited_model: String,
    pub instruction_source: SessionInstructionSource,
    pub instruction_chars: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_instruction_override: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guidelines_count: Option<u64>,
    pub editable: bool,
    pub deletable: bool,
    pub restorable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy: Option<bool>,
    pub revision: u64,
}

/// Envelope of the session agent roster (`harness-api.ts:954`). Not paged: membership comes
/// from the frozen definition plus the 64-binding roster budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionAgentListResponse {
    pub session_id: String,
    pub total: u64,
    pub agents: Vec<SessionAgentBinding>,
    pub revision: u64,
}

/// Envelope of a session agent read, create, or patch (`harness-api.ts:965`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionAgentResponse {
    pub agent: SessionAgentBinding,
}

/// Binds an existing trusted profile into this session (`harness-api.ts:1883-1942`): no
/// implementation, tool list, or transport is accepted, so nothing here can inject code.
///
/// THE BODY IS THE CONTRACT'S SIX MEMBERS - `name, profile, model, instruction, prefix, rank` - and the
/// retired `role` is NOT one of them (`api.ts:4188` validates exactly this list, and the generated
/// request names `rank?: AgentRank` at `harness-api.ts:1938`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateSessionAgentRequest {
    pub name: String,
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    /// THE RANK this session gives the member (`harness-api.ts:1938`). Absent or `null` asks for NO
    /// authority, which this build resolves through the frozen definition's map and finally the LOWEST
    /// letter; the route refuses any value that is not one upper-case `A-Z` letter
    /// (`sessions.ts:1205 validateBindingRank`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<AgentRank>,
    /// The member's PRESET HISTORY (`harness-api.ts:1931`), a `TurnBlock[]` so it travels as raw JSON
    /// through the ONE band validator ([`crate::registry::require_prefix_turns`]). Unlike the PATCH, a
    /// `null` here asks for NOTHING of its own rather than restoring a band (`harness-api.ts:1927-1931`),
    /// so the store treats it as unstated.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_stated_value"
    )]
    pub prefix: Option<serde_json::Value>,
}

/// An omitted property is no change; a string REPLACES the whole field; an explicit `null`
/// clears the session override and RESTORES the inherited binding (`harness-api.ts:988`).
///
/// `prefix` is a `TurnBlock[]`, so it travels as raw JSON rather than a typed block list: the band is
/// validated by [`crate::registry::require_prefix_turns`], the ONE validator this crate owns
/// (`api.ts:4060`), and re-validated by the store write exactly as TS's `setSessionAgentPrefix`
/// re-validates it (`sessions.ts:8166`). `Eq` cannot survive a `Value` field, so this DTO compares with
/// `PartialEq` only.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateSessionAgentRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<Option<String>>,
    /// THE AUTHORITY WRITE (`harness-api.ts:1956-1962`). This field has NO restore arm: `api.ts:4051-4058`
    /// refuses a stated `null` with the single-letter sentence, so the only value that can reach the store
    /// is a letter, and an absent key means no change (the plain `Option` serde gives exactly that).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<AgentRank>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_stated_value"
    )]
    pub prefix: Option<serde_json::Value>,
}

/// Reads a field that has THREE states: absent, stated `null`, and stated value.
///
/// WHY NOT `Option<Option<Value>>`: serde absorbs a JSON `null` into the OUTER `None`, which is the same
/// trap this crate already documents for `model` and `instruction` (`api/session_agents.rs:405-410`,
/// where the route rewrites a stated `null` into the blank string the store reads as a restore). A band
/// has no spare string to borrow - `[]` is a real choice, "this conversation seeded no history", and it
/// answers `source: session` where `null` answers `source: harness` - so the field deserializes through
/// `Value` directly: present means present, and `Value::Null` IS the restore arm
/// (`sessions.ts:8157-8163`).
fn deserialize_stated_value<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <serde_json::Value as serde::Deserialize>::deserialize(deserializer).map(Some)
}

/// Receipt of a roster removal (`harness-api.ts:998`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeleteSessionAgentResult {
    pub deleted: bool,
    pub agent_id: String,
    pub session_id: String,
    pub detached_threads: u64,
    pub revision: u64,
}

/// The effective instruction text one session agent actually runs with, plus its
/// provenance (`harness-api.ts:1010`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionInstructionTextResponse {
    pub session_id: String,
    pub agent_id: String,
    pub source: SessionInstructionSource,
    pub instruction: String,
    pub guidelines: Vec<String>,
    pub chars: u64,
}

// ---------------------------------------------------------------------------
// Session toolbox
// ---------------------------------------------------------------------------

/// A catalogue projection plus the live session facts the catalogue cannot state
/// (`harness-api.ts:1023`), composed additively so the global shape stays unaffected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionToolSummary {
    #[serde(flatten)]
    pub tool: ToolSummary,
    pub session_id: String,
    pub declared: bool,
    pub enabled: bool,
    pub origin: SessionToolOrigin,
    /// `protected` is a Rust keyword, so the field is `r#protected`; serde still prints the
    /// bare `protected` spelling the contract pins.
    pub r#protected: bool,
    pub source_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

/// A session tool row plus the argument schema and the execution gate (`harness-api.ts:1038-1044`).
/// `execution` is the SAME closed `{scope, manual}` object as on the global detail read; it is
/// optional here only because TS marks the member optional (`:1043`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionToolDetail {
    #[serde(flatten)]
    pub summary: SessionToolSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ToolExecutionScope>,
}

/// A `used`/`limit` pair, as the toolbox budget and the registration receipt print it
/// (`harness-api.ts:1063`, `:1136`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceBudget {
    pub used: u64,
    pub limit: u64,
}

/// Page of session toolbox rows (`harness-api.ts:1060`). Deliberately absent from every
/// row: the argument JSON Schema, the source text, and any tool result payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionToolPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<SessionToolSummary>,
    pub budget: ResourceBudget,
}

/// Envelope of one session tool detail read (`harness-api.ts:1073`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionToolResponse {
    pub tool: SessionToolDetail,
}

/// One captured local helper of an installed source revision (`types/index.ts:628`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSourceDependency {
    pub path: String,
    pub text: String,
}

/// Declarative tool creation (`harness-api.ts:1082`): compiles a candidate and NEVER runs
/// it, so registering has no side effect until a call. Tags are REQUIRED because an untagged
/// tool could never be discovered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateSessionToolRequest {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<ToolSourceDependency>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<ToolVisibility>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace: Option<bool>,
}

/// An omitted property is no change and a supplied value REPLACES the whole field (arrays
/// and schemas are replaced, never merged); at least one property is required
/// (`harness-api.ts:1106`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateSessionToolRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<ToolVisibility>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<ToolSourceDependency>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace: Option<bool>,
}

/// Metadata only (`harness-api.ts:1128`): no function, schema body, or source text is
/// echoed, so a create or update costs one short result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionToolRegistrationResult {
    pub name: String,
    pub source_revision: String,
    pub created: bool,
    pub replaced: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checks: Option<Vec<String>>,
    pub budget: ResourceBudget,
}

/// Envelope of a session tool create or update (`harness-api.ts:1146`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionToolRegistrationResponse {
    pub registration: SessionToolRegistrationResult,
}

/// Receipt of a session tool removal (`harness-api.ts:1155`): removing a name this session
/// OVERRODE restores the inherited catalogue binding, while removing a name created in this
/// session simply ends it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeleteSessionToolResult {
    pub deleted: bool,
    pub tool_id: String,
    pub session_id: String,
    pub removed_created: bool,
    pub restored_inherited: bool,
}

/// The installed source revision of one session tool (`harness-api.ts:1167`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionToolSourceResponse {
    pub session_id: String,
    pub name: String,
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<ToolSourceDependency>>,
}

// ---------------------------------------------------------------------------
// Tasks, queue, owner actions, commands, usage
// ---------------------------------------------------------------------------

/// The durable submit body (`harness-api.ts:1187`): `input` is trimmed and bounded
/// server-side, empty input is accepted only with at least one attachment, and every field
/// is re-validated BEFORE a turn exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SubmitTaskRequest {
    pub input: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<HarnessAttachment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<AgentResultReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

/// The 202 answer (`harness-api.ts:1200`): what the server accepted, where it put it, and
/// which resources moved. It never echoes the session or the turn record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TaskReceipt {
    pub session_id: String,
    pub turn_id: String,
    pub status: SessionTurnStatus,
    pub thread_id: String,
    /// Present only when the submit steered a running turn instead of opening one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steered: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steered_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<u64>,
    pub session_revision: u64,
    pub accepted_at: u64,
    pub links: TaskReceiptLinks,
}

/// The four links every receipt carries (`harness-api.ts:1210`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TaskReceiptLinks {
    pub turn: String,
    pub turns: String,
    pub thread: String,
    pub stream: String,
}

/// Receipt of a reset (`harness-api.ts:1222`): what was discarded and the new revision,
/// never the emptied aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ResetSessionResult {
    pub reset: bool,
    pub session_id: String,
    pub discarded_turns: u64,
    pub discarded_threads: u64,
    pub dropped_queued_tasks: u64,
    pub revision: u64,
}

/// An absent body or `{}` cancels the active run only and leaves queued tasks untouched
/// (`harness-api.ts:1235`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CancelSessionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all: Option<bool>,
}

/// Outcome of a session-level cancel (`harness-api.ts:1243`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CancelSessionResult {
    pub cancelled: bool,
    pub session_id: String,
    pub dropped_queued_tasks: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_id: Option<String>,
    pub revision: u64,
}

/// One waiting task (`harness-api.ts:1256`): the prompt is a bounded PREVIEW with the exact
/// recorded length, because a hundred queued 200k-character prompts must not make a queue
/// panel a multi-megabyte read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QueuedTask {
    pub turn_id: String,
    pub index: u64,
    pub position: u64,
    pub status: SessionTurnStatus,
    pub prompt_preview: String,
    pub prompt_chars: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_labels: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<u64>,
}

/// Page of queued tasks, in pump order (`harness-api.ts:1271`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedTaskPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<QueuedTask>,
}

/// One question a model asked its owner, still waiting (`harness-api.ts:1283`): `request` is
/// the verbatim recorded `HarnessActionRequest`, so the options a client may answer with are
/// exactly the options the model offered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PendingAction {
    pub request: HarnessActionRequest,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub agent: String,
    pub received_at: u64,
}

/// Envelope of the owner questions still waiting (`harness-api.ts:1296`): live requests only,
/// in the order they were opened, at most `MAX_PENDING_ACTIONS`. A request restored from disk
/// is expired and is NOT listed, because its promise no longer exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PendingActionListResponse {
    pub session_id: String,
    pub actions: Vec<PendingAction>,
    pub revision: u64,
}

/// Exactly one field is accepted (`harness-api.ts:1307`); the choice is validated against
/// the RECORDED request, so an unknown option id is refused rather than defaulted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AnswerActionRequest {
    pub choice: ActionChoiceInput,
}

/// The answer receipt (`harness-api.ts:1324`): the authoritative record is the persisted
/// `action_answer` event on the turn, so this route does not restate the choice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AnswerActionResult {
    pub ok: bool,
    pub request_id: String,
    pub resolved_at: u64,
}

/// One registered slash command, projected for a composer (`harness-api.ts:1336`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CommandDescriptor {
    pub name: String,
    pub invocation: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<String>,
}

/// Page of command descriptors from this session's live registry (`harness-api.ts:1344`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<CommandDescriptor>,
}

/// One conversation's share of the session's reported usage (`harness-api.ts:1353`), so cost
/// follows what the user is looking at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UsageByThread {
    pub thread_id: String,
    pub usage: HarnessUsage,
}

/// A server-computed fold of the session's per-turn tallies (`harness-api.ts:1364`). Honest
/// about unknowns: `usage` is ABSENT when no turn reported any, rather than printed as zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionUsageResponse {
    pub session_id: String,
    pub turn_count: u64,
    pub reported_turns: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<HarnessUsage>,
    pub by_thread: Vec<UsageByThread>,
    pub truncated: bool,
    pub revision: u64,
}

// ---------------------------------------------------------------------------
// Conversations (persistent threads)
// ---------------------------------------------------------------------------

/// One conversation as a roster row (`harness-api.ts:1381`): identity, ownership, lifecycle,
/// and transcript SIGNALS. The committed transcript is deliberately absent, which is what
/// makes a 256-conversation roster a bounded read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ThreadSummary {
    pub id: String,
    pub agent: String,
    pub origin: ThreadOrigin,
    pub status: ThreadLifecycle,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub transcript_state: TranscriptState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_error: Option<String>,
    pub message_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chars: Option<u64>,
    pub has_transcript: bool,
    /// A bounded tail of the last entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

/// One conversation in full (`harness-api.ts:1401`): a roster row plus the invocation roster
/// and the reference graph. Still no messages - those are the transcript routes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadResource {
    #[serde(flatten)]
    pub thread: ThreadSummary,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_invocation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    pub invocation_ids: Vec<String>,
    pub invocation_ids_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connected_thread_ids: Option<Vec<String>>,
    pub transcript_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_bytes: Option<u64>,
    pub revision: u64,
}

/// Envelope of a conversation read or create (`harness-api.ts:1420`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadResponse {
    pub thread: ThreadResource,
}

/// Page of conversation rows (`harness-api.ts:1425`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadSummaryPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<ThreadSummary>,
}

/// Opens an additional independent conversation for one member agent (`harness-api.ts:1435`).
/// Nothing runs until a task names it, so thread creation is free.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateSessionThreadRequest {
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_thread_id: Option<String>,
}

/// Receipt of a conversation removal (`harness-api.ts:1447`): it names what went WITH the
/// conversation, because the cascade is the part a client must report to the user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeleteThreadResult {
    pub deleted: bool,
    pub thread_id: String,
    pub session_id: String,
    pub removed_descendants: u64,
    pub removed_turns: u64,
    pub revision: u64,
}

/// Outcome of stopping one conversation (`harness-api.ts:1460`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CancelThreadResult {
    pub cancelled: bool,
    pub thread_id: String,
    pub session_id: String,
    /// The TS type is the literal `"stopped"`; `ThreadLifecycle::Stopped` prints the same word.
    pub status: ThreadLifecycle,
    pub dropped_queued_tasks: u64,
    pub revision: u64,
}

// ---------------------------------------------------------------------------
// Transcripts
// ---------------------------------------------------------------------------

/// The cheap head of one transcript (`harness-api.ts:1474`): size, counts, revision, and
/// validity - no content at all, so a client can decide whether a paged read or a document
/// read is the cheaper next step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TranscriptMetadata {
    pub session_id: String,
    pub thread_id: String,
    pub agent: String,
    pub transcript_state: TranscriptState,
    pub message_count: u64,
    pub chars: u64,
    pub bytes: u64,
    pub revision: u64,
    pub role_counts: TranscriptRoleCounts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreadable_reason: Option<String>,
    /// The JSON type actually stored when the document is not a valid transcript.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored_type: Option<String>,
}

/// Per-role entry counts (`harness-api.ts:1483`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TranscriptRoleCounts {
    pub system: u64,
    pub user: u64,
    pub assistant: u64,
    pub tool: u64,
}

/// Envelope of the transcript metadata read (`harness-api.ts:1497`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptMetadataResponse {
    pub transcript: TranscriptMetadata,
}

/// Page of transcript entries (`harness-api.ts:1509`). `total` is the entry count of the
/// WHOLE transcript, independent of the `role` filter; `revision` is the transcript's own
/// counter, the same figure the `transcript`/`transcript_messages` stream kinds report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptMessagePage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub thread_id: String,
    pub agent: String,
    pub transcript_state: TranscriptState,
    pub total: u64,
    pub items: Vec<HarnessMessage>,
}

/// The whole transcript as one JSON TEXT document (`harness-api.ts:1526`) - the editor's and
/// the export surface, and the counterpart of the whole-document PUT.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TranscriptDocumentResponse {
    pub session_id: String,
    pub thread_id: String,
    pub agent: String,
    pub revision: u64,
    pub transcript_state: TranscriptState,
    pub document: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// `messages` is DELIBERATELY untyped (`harness-api.ts:1541`): the transcript is fully
/// user-modifiable JSON, and a structurally broken value is a SUCCESSFUL write, not a 400.
/// Only an absent `messages` property (a malformed body) is a 400.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateThreadTranscriptRequest {
    pub messages: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_revision: Option<u64>,
}

/// Receipt of a whole-document write (`harness-api.ts:1552`): the validation outcome is
/// data, not a status code - a broken draft is accepted, stored raw, and explained, while
/// the live transcript stays a valid placeholder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TranscriptWriteReceipt {
    pub session_id: String,
    pub thread_id: String,
    pub accepted_as: TranscriptAcceptedAs,
    pub message_count: u64,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid: Option<TranscriptInvalidDetail>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<TranscriptWriteLinks>,
}

/// Whether the stored document became a live transcript or a raw placeholder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TranscriptAcceptedAs {
    Transcript,
    Raw,
}

/// Why a stored document was NOT installed as a transcript (`harness-api.ts:1558`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TranscriptInvalidDetail {
    pub error: String,
    pub stored_type: String,
}

/// The three transcript reads of the same conversation (`harness-api.ts:1562`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TranscriptWriteLinks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document: Option<String>,
}

// ---------------------------------------------------------------------------
// Turns: the durable record, projected as metadata plus COUNTS
// ---------------------------------------------------------------------------

/// One recorded task (`harness-api.ts:1609`). The heavy members of a turn record -
/// `events`, `eventTimes`, `steerTexts`, attachment bytes - are counted here and fetched
/// from their own routes, because a turn that ran for an hour must not be unreadable in one
/// request. This route is a projection; the events route is the verbatim record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TurnResource {
    pub session_id: String,
    pub id: String,
    pub index: u64,
    pub status: SessionTurnStatus,
    /// The VERBATIM prompt - this is the one route that carries it.
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<HarnessUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<HarnessErrorInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<AttachmentDescriptor>>,
    pub event_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_count: Option<u64>,
    pub attachment_count: u64,
    pub steer_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer_text_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steers: Option<Vec<SteerRecord>>,
    pub revision: u64,
}

/// Envelope of the turn metadata read (`harness-api.ts:1636`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnResponse {
    pub turn: TurnResource,
}

/// One steered submit as the audit ring records it (`harness-api.ts:1593`): the submit's own
/// durable identity, when it was accepted, a bounded preview plus its exact length, and the
/// conversation it was delivered to. Never contains attachments, because attachments are
/// never steered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SteerRecord {
    /// Absent only on a record written before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub at: u64,
    pub prompt_preview: String,
    pub prompt_chars: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
}

/// One recorded event with its ordinal and the server's wall-clock time for it
/// (`harness-api.ts:1646`): the two parallel arrays of the durable record (`events` and
/// `eventTimes`) arrive PAIRED, so a client never re-pairs indices by position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TurnEventRow {
    pub index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<u64>,
    /// The stored record, VERBATIM — including the display-lane stamp the recorder put inside it
    /// (`persistence::SessionTurnEvent`). TS copies the same value here
    /// (`api-reads.ts:484-490`), and the spec's `HarnessEvent` object is OPEN, so the extra key is
    /// contract-valid; dropping it on the read path would make a reload draw the context bar with
    /// every segment on row 0.
    pub event: crate::persistence::SessionTurnEvent,
}

/// The same pairing on the step-detail route (`harness-api.ts:1924`).
pub type RecordedEventRef = TurnEventRow;

/// Page of one turn's verbatim recorded events (`harness-api.ts:1659`). `revision` is the
/// owning turn's `TurnResource.revision` (the `turn_events` counter is DEFINED to be that
/// figure) and `total_events` is the position within it, so one counter per comparison,
/// never two. `partial` is true while the turn is still running.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnEventPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub turn_id: String,
    pub total_events: u64,
    pub partial: bool,
    pub items: Vec<TurnEventRow>,
}

/// Envelope carrying one attachment's recorded bytes (`harness-api.ts:1672`): `data` is
/// canonical base64 with no prefix and no whitespace, and `size` agrees with its decoded
/// length - the same invariant the submit route validates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentResponse {
    pub attachment: HarnessAttachment,
}

// ---------------------------------------------------------------------------
// The compact display read models (camelCase on the wire)
// ---------------------------------------------------------------------------

/// One composer chip on the compact wire (`turn-display.ts:245`, aliased at
/// `harness-api.ts:1702`). `data` is present ONLY for an image small enough to inline;
/// `hasBytes` tells the renderer a fetch can still show the rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisplayAttachmentSummary {
    pub id: String,
    pub marker: String,
    pub name: String,
    pub kind: AttachmentKind,
    pub mime: String,
    /// DECODED byte length, same meaning as `HarnessAttachment::size`.
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    pub has_bytes: bool,
}

/// A `#`-referenceable block (an agent invocation) WITHOUT its full text
/// (`turn-display.ts:262`, aliased at `harness-api.ts:1709`): what the composer popup and
/// the reference graph need. `label` is the canonical `[A-Z]{2}\d+` display name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisplayBlockSummary {
    pub turn_id: String,
    /// Absent = the ROOT invocation; otherwise the invocation key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub label: String,
    pub agent: String,
    pub status: AgentResultStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    /// `isReferenceableBlock` decided server-side, so the popup applies one boolean.
    pub referenceable: bool,
}

/// One steered submit as the compact row sees it (`harness-api.ts:1718`), keyed by the
/// submit's own durable identity so two submits in one millisecond stay distinct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisplaySteerSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub at: u64,
    pub prompt_preview: String,
    pub prompt_chars: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
}

/// Everything one turn tile shows, computed by the server from the recorded turn
/// (`harness-api.ts:1737`): no event list, no full block content, no attachment bytes, no
/// instruction or reasoning text, and NO verbatim prompt - a row carries a bounded preview
/// plus the EXACT recorded length. A page of these is the default conversation surface and
/// its size is bounded by the page limit alone.
///
/// This is a REAL struct (not a `Value`) because the API pages it and `limit` must echo its
/// count; the nested display tree below it stays `Value`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnSummary {
    pub id: String,
    pub index: u64,
    pub prompt_preview: String,
    pub prompt_chars: u64,
    pub status: SessionTurnStatus,
    pub state: DisplayState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<HarnessUsage>,
    /// The largest prompt the server last sent for this turn, precomputed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_input: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub section_count: u64,
    pub step_count: u64,
    pub active_agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<DisplayAttachmentSummary>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steers: Option<Vec<DisplaySteerSummary>>,
    pub blocks: Vec<DisplayBlockSummary>,
}

/// Page of turn rows, in the requested order (`harness-api.ts:1762`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnSummaryPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<TurnSummary>,
}

/// One turn's compiled display tree (`harness-api.ts:1881`). `revision` is the
/// `turn_display` counter, defined to EQUAL the owning turn's `TurnResource.revision`: the
/// tree is a pure fold of that record, and minting a second counter would create a
/// comparison that cannot be made honestly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnDisplay {
    pub turn_id: String,
    pub revision: u64,
    pub sections: Vec<TurnSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_section_ordinal: Option<u64>,
}

/// Envelope of one turn's display tree (`harness-api.ts:1892`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnDisplayResponse {
    pub display: TurnDisplay,
}

/// One segment of the ONE context bar (`harness-api.ts:1818`): METADATA ONLY, carrying no
/// text, which is what lets the default wire grow with the NUMBER of segments and never with
/// their length. The property names ARE the projector's field names - `name`, never a
/// renamed `label`, is what the server sends - and no time figure or `inPrompt` verdict is
/// dropped, because every one of them is a figure the durable record already holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextBlock {
    pub kind: ContextBlockKind,
    pub id: String,
    pub turn_id: String,
    pub section_id: String,
    /// DISPLAY LANE (`harness.swagger.yaml:8122`, `integer` + `minimum: 0`): which stacked row of the
    /// context bar this segment belongs to. Pure UI metadata, never model-facing, and `None` IS lane 0 —
    /// the spec's "absent means lane 0" reading, spelled in Rust as an `Option` so a decoder can tell
    /// "the producer said nothing" from "the producer said 1" without inventing a number.
    ///
    /// It is minted by the `context:add` that creates the block and immutable afterwards, so it is NOT a
    /// key of the block-patch shape: the fold's `context:set` path builds its patch as a JSON map of the
    /// mutable fields only (`projector.rs:1448-1471`), which is why no patch here can carry a lane.
    ///
    /// The writer is the fold's ONE `context:add` mint point (`projector::add_block`, `:961-980`), which
    /// stamps through [`crate::projector`]'s `with_lane` (`:450-463`): the key goes in LAST and only for a
    /// non-zero lane, and the value is the lane of the event that opened the segment — or the TURN's
    /// starting lane for a segment that is not an event, which the fold swaps in around the same call
    /// (`add_block_with_lane`, `:587-598`). That suppression is what keeps a session that never compacted
    /// byte-identical to the document this crate wrote before lanes existed (`persistence::LaneStamp`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub chars: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<StepDisplayState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_prompt: Option<bool>,
    /// C1/R3: the LIVE, reader-facing verdict for this segment (`harness-api.ts:3766-3815`,
    /// `harness-server.swagger.yaml` `ContextBlock.included`). The PROJECTOR never mints it —
    /// `inPrompt` is the static per-kind figure and `included` is a fact about the CONVERSATION's
    /// transcript — so the reads that can name the entry behind the segment stamp it at read time:
    /// the block-detail route answers the ENTRY verdict (`api.ts:4597-4630`) and the display route
    /// answers the SEGMENT verdict (`api.ts:2022` `contextSegmentIncludedOf`, applied by
    /// `displaySectionsWithInclusionOf` at `:2100-2131`). ABSENT is never SYNTHESIZED: a segment
    /// with no addressable entry carries no key at all, which is the reader's cue to show no
    /// control. Only a literal `false` dims a segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub included: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_from: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_to: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
}

/// Which transcript segment a context bar entry describes (`harness-api.ts:1819`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContextBlockKind {
    User,
    System,
    Reasoning,
    Assistant,
    Tool,
}

/// One context segment answered with its recorded text (`harness-api.ts:1903`), by exactly
/// one payload shape per kind: `text` for a user/reasoning/assistant segment, `args` and
/// `result` for a tool call's two halves, `entries` for a system segment (one entry per
/// recorded system message, in send order).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnBlockDetail {
    #[serde(flatten)]
    pub block: ContextBlock,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<String>>,
}

/// Envelope of one context segment detail (`harness-api.ts:1916`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnBlockDetailResponse {
    pub block: TurnBlockDetail,
}

/// The recorded OPENING event and the recorded RESULT event of one step, both VERBATIM
/// (`harness-api.ts:1936`), each with its index and its recorded time. Nothing is truncated
/// and nothing is re-projected, and the pair comes together because fetching separately would
/// double the round trips and could pair same-name siblings differently.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStepDetail {
    pub session_id: String,
    pub turn_id: String,
    pub step_id: String,
    pub section_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    pub opening: RecordedEventRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<RecordedEventRef>,
}

/// Envelope of one step detail, carrying the recorded request and its recorded result
/// together (`harness-api.ts:1950`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnStepDetailResponse {
    pub detail: TurnStepDetail,
}

// ---------------------------------------------------------------------------
// Stream vocabulary (`harness-stream.ts`) - the constants the realtime stage needs
// ---------------------------------------------------------------------------

/// The one stream path, canonical in both mounts (`stream/protocol.ts`; the contract's
/// path table ends at `harness-api.ts:192`).
pub const HARNESS_WS_STREAM_PATH: &str = "/v1/harness/stream";

/// Closed vocabulary of subscription scopes (`harness-stream.ts:50`).
pub const HARNESS_WS_SCOPES: [&str; 4] = ["global", "session", "thread", "turn"];

/// Every resource kind that owns a revision counter (`harness-stream.ts:53`). Order is the
/// contract's, so a baseline iteration matches the TS one.
pub const HARNESS_STREAM_RESOURCE_KINDS: [&str; 21] = [
    "session",
    "definition",
    "turns",
    "turn",
    "turn_display",
    "turn_events",
    "threads",
    "thread",
    "transcript",
    "transcript_messages",
    "actions",
    "usage",
    "queue",
    "session_agents",
    "session_tools",
    "harnesses",
    "agents",
    "tools",
    "config",
    "commands",
    // Wave 1: the record-store surface owns a counter, so it participates in the baseline.
    "records",
];

/// Frame tags a client may send (`harness-stream.ts:56`).
pub const HARNESS_WS_CLIENT_MESSAGE_TYPES: [&str; 6] =
    ["hello", "subscribe", "unsubscribe", "ack", "ping", "bye"];

/// Frame tags a client must handle (`harness-stream.ts:59`).
pub const HARNESS_WS_SERVER_MESSAGE_TYPES: [&str; 7] = [
    "welcome",
    "subscription_state",
    "update",
    "resync_required",
    "error",
    "heartbeat",
    "pong",
];

/// Update kinds inside `update` frames (`harness-stream.ts:62`).
pub const HARNESS_WS_UPDATE_KINDS: [&str; 16] = [
    "session.updated",
    "turn.upsert",
    "turn.removed",
    "display.ops",
    "usage.updated",
    "action.opened",
    "action.closed",
    "thread.upsert",
    "thread.removed",
    "transcript.updated",
    // Wave 1: a record-store mutation; ordering matches `harness-stream.ts:62`.
    "records.updated",
    "tool.registry_updated",
    "agent.registry_updated",
    "run.event",
    "catalogue.updated",
    "config.updated",
];

/// Whether `kind` is one of the [`HARNESS_WS_UPDATE_KINDS`].
///
/// The set exists once, in this constant, and this is the only reader of it: every publish path
/// asserts against it (`events.rs::publish_with_ops`) so a new kind cannot be published without
/// being added to the set the contract and the stream's `subscription_state.publishes` tables
/// describe. `harness-stream.ts:62` states the same list as a `satisfies` clause, which is the
/// compile-time version of the same gate.
#[must_use]
pub fn is_update_kind(kind: &str) -> bool {
    HARNESS_WS_UPDATE_KINDS.contains(&kind)
}

/// Protocol error codes (`harness-stream.ts:65`).
pub const HARNESS_WS_PROTOCOL_CODES: [&str; 9] = [
    "invalid_payload",
    "unknown_subscription",
    "unknown_resource",
    "unsupported_filter",
    "late_hello",
    "too_many_subscriptions",
    "frame_too_large",
    "rate_limited",
    "internal",
];

/// The stream numbers in force (`harness-stream.ts:28` `HARNESS_WS_STREAM_LIMITS`), which
/// the `welcome` frame carries verbatim so a client never guesses a bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WsStreamLimits {
    pub subscriptions_per_connection: u64,
    pub ops_per_update_frame: u64,
    pub delta_chars_per_append_op: u64,
    pub events_per_update_frame: u64,
    pub ids_per_invalidation_frame: u64,
    pub max_client_frame_bytes: u64,
    pub max_server_frame_bytes: u64,
    pub client_messages_per_second: u64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_intervals: u64,
    pub heartbeat_timeout_close_code: u64,
    pub replay_frames: u64,
    pub replay_seconds: u64,
    pub send_queue_frames: u64,
    pub send_queue_bytes: u64,
    pub sustained_stall_ms: u64,
    pub ack_timeout_ms: u64,
    pub revision_records_per_state: u64,
}

/// The limits in force - the contract's own numbers, not tuning advice.
#[must_use]
pub fn ws_stream_limits() -> WsStreamLimits {
    WsStreamLimits {
        subscriptions_per_connection: 64,
        ops_per_update_frame: 32,
        delta_chars_per_append_op: 2_000,
        events_per_update_frame: 16,
        ids_per_invalidation_frame: 64,
        max_client_frame_bytes: 16_384,
        max_server_frame_bytes: 65_536,
        client_messages_per_second: 32,
        heartbeat_interval_ms: 15_000,
        heartbeat_timeout_intervals: 3,
        heartbeat_timeout_close_code: 4_408,
        replay_frames: 512,
        replay_seconds: 60,
        send_queue_frames: 256,
        send_queue_bytes: 1_048_576,
        sustained_stall_ms: 10_000,
        ack_timeout_ms: 5_000,
        // 20 kinds plus `records` (`harness-stream.ts:28`).
        revision_records_per_state: 21,
    }
}

/// A session metadata delta (`harness-api.ts:2060`): status, queue depth, model map, and the
/// last error - the fields a rail or a header draws. `revision` is always present so the
/// frame can be ordered against a read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionStatePatch {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SessionStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub revision: u64,
}

/// The server's running token figures for a turn or a session (`harness-api.ts:2079`):
/// figures travel PRECOMPUTED because the server is the one that summed them from per-request
/// `usage` events, so a client never accumulates anything and can never double-count a replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UsagePatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<HarnessUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_input: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input: Option<u64>,
    pub revision: u64,
}

/// A transcript changed, stated as counts and a revision (`harness-api.ts:2092`): message
/// bodies never ride the stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TranscriptSignal {
    pub thread_id: String,
    pub revision: u64,
    pub message_count: u64,
    pub transcript_state: TranscriptState,
}

// ---------------------------------------------------------------------------
// Shared domain / transport types (NOT wire models: these are the internal shapes the
// store, the registry, and the agent runtime pass around, ported from
// `types/index.ts:272` `HarnessStreamTool`, `:108` `HarnessToolCall`, and
// `harnesses/types` `RunnerSettings`).
// ---------------------------------------------------------------------------

/// A tool DECLARATION handed to a model (`types/index.ts:272`): definitions, not
/// implementations - a transport never executes. `parameters` is the tool's own JSON Schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDecl {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
}

/// A model-requested call inside the runtime (`types/index.ts:108`): `arguments` stays a raw
/// JSON string, exactly like the provider wire format, so malformed JSON is reported back to
/// the model instead of crashing a parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// What one executed turn settled with. The Rust replacement for the TS run's terminal
/// state: usage is what the provider reported, `error` is the failure text, and `cancelled`
/// distinguishes an expected control outcome from a failure (never by string-matching, which
/// is the rule `HarnessToolOutcome` states at `types/index.ts:198-215`).
///
/// `messages` is the FULL updated thread transcript, because the runtime (not the store) owns
/// the append: `sessions.ts:7329` runs `harness.sendThreadInput`, and the harness's own agent
/// history is then what `harness.snapshot()` commits (`:7430`). A Rust executor that appends
/// the user prompt, the assistant answers, and every tool result must hand the whole resulting
/// transcript back here, and the store REPLACES the conversation's messages with it - it does
/// not append, which would double the prompt.
///
/// `stop_reason` is the final model response's reason (`types/index.ts:182`), retained so a
/// later stage can tell `tool_calls` (the run was not finished) from `stop` without re-reading
/// the events. `None` is "the provider never reported one", i.e. a failed or cancelled run.
///
/// DATED 2026-11-15: the TypeScript source no longer leaves a non-concluded run without a cause.
/// Its one non-concluded exit classifies WHY the run stopped (`classifyRunStopReason` upstream)
/// and keeps that cause on the conversation record beside an `idle` status, so `stopReason` is now
/// a CAUSE there rather than only a provider field. This crate still answers `None` on every
/// failure and cancellation arm, and has no thread-level cause field at all - so do not read this
/// field as mirroring the current TypeScript shape. The dated section of this crate's
/// `knowledge.md` (`:668-709`) lists what is unported.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TurnOutcome {
    pub usage: Option<HarnessUsage>,
    pub error: Option<String>,
    pub cancelled: bool,
    pub messages: Vec<HarnessMessage>,
    pub stop_reason: Option<String>,
    /// Guidance this run DRAINED from its steering channel and then lost, because the run failed or
    /// was aborted and its transcript was rolled back.
    ///
    /// DATED 2026-11-15: the TypeScript source this field mirrored is gone. Its comment used to
    /// read "R5 preservation: steering this run drained is rolled back with the transcript, so hand
    /// it to the NEXT host turn in its original order", and that sentence no longer exists there -
    /// `harness.ts:6826-6837` is the thread-stop family now, not the steering hand-back. Upstream
    /// issue 0036
    /// (now closed)
    /// retired the rollback for a conversation-bound run, so the drained steering now survives
    /// INSIDE the retained transcript and the TypeScript side hands back an empty list in exactly
    /// that case.
    ///
    /// This crate still rolls back, so the behaviour described here is still correct HERE and is no
    /// longer a mirror of TypeScript: empty on a successful run, where every drained steer reached
    /// the model, and the store re-queues the list onto the conversation's lane unless the run was
    /// explicitly stopped. The divergence is listed in the dated section of this crate's
    /// `knowledge.md` (`:668-709`).
    pub unused_steering: Vec<String>,
}

/// The endpoint one provider call actually uses (`harnesses/types` `RunnerSettings` plus the
/// extra headers the TS provider configuration allows at `types/index.ts:315`).
///
/// `api_key` is `Option<String>`: an ABSENT credential is "no key", never a placeholder
/// string (the rule `config.ts:14-17` states as T28).
///
/// `sampling` is `HarnessSampling` (`types/index.ts:296-304`): the OpenAI field names the built-in
/// transport forwards VERBATIM onto every request body (`transport/openai.ts:618-627 samplingBody`
/// copies only the entries that are numbers, and `server/harnesses/toolbox.ts:245` is the only writer
/// in the server, with `{ temperature: 0.3 }`). A JSON `Value` rather than five `Option`s because the
/// TS type is an open record of numeric knobs and the transport must not decide which names exist.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProviderConfig {
    pub base_url: String,
    pub api_key: Option<String>,
    pub model: String,
    /// Extra headers merged over the generated ones; ordered so a built request is stable.
    pub headers: BTreeMap<String, String>,
    /// The sampling fields to merge into the request body; `None` sends none.
    pub sampling: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Body validators - the handler-level checks `src/server/api.ts` performs
// ---------------------------------------------------------------------------

/// Rejects unknown properties instead of ignoring them (`api.ts:504-510`, "BODY POLICY").
///
/// The message is the contract's, byte for byte:
/// `Unknown field "<key>". Allowed fields: <comma list>.`
///
/// Key order here is the DOCUMENT's own, because this crate enables serde_json's `preserve_order`
/// feature (`Cargo.toml`), so `fields.keys()` walks the client's insertion order exactly like TS's
/// `Object.keys` - and when a body carries several offenders, the first one the CLIENT wrote is the
/// one named. `tests/plugins.rs` ("several bad fields") pins that pairing for the plugin patch, whose
/// unknown-SETTING refusal follows the same rule from `serde_json::Map` (`src/plugins.rs::update`).
///
/// # Errors
///
/// [`ApiError::bad_request`] naming the first unknown key.
pub fn require_known_fields(value: &serde_json::Value, allowed: &[&str]) -> Result<(), ApiError> {
    let serde_json::Value::Object(fields) = value else {
        // A non-object has no fields to reject; `require_object` owns that refusal, and the
        // TS helper is only ever called after it (`api.ts:1339-1341`).
        return Ok(());
    };

    for key in fields.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ApiError::bad_request(format!(
                "Unknown field \"{key}\". Allowed fields: {}.",
                allowed.join(", ")
            )));
        }
    }

    Ok(())
}

/// A body that must be a JSON object (`api.ts:512-518`), including the array and `null`
/// refusals TS spells out explicitly.
///
/// # Errors
///
/// [`ApiError::bad_request`] with `The request body must be a JSON object.`
pub fn require_object(
    value: &serde_json::Value,
) -> Result<&serde_json::Map<String, serde_json::Value>, ApiError> {
    match value {
        serde_json::Value::Object(fields) => Ok(fields),
        _ => Err(ApiError::bad_request(
            "The request body must be a JSON object.",
        )),
    }
}

/// The other half of `readEmptyBody` (`api.ts:520-531`): a route documented as body-less
/// still refuses a malformed or non-empty one rather than ignoring it.
///
/// # Errors
///
/// [`ApiError::bad_request`] with `This route accepts no request body.`
pub fn require_empty_body(value: &serde_json::Value) -> Result<(), ApiError> {
    if value.is_null() {
        return Ok(());
    }

    // An object with no keys is the `{}` a body-less route is allowed to send (`api.ts:528`).
    if let serde_json::Value::Object(fields) = value
        && fields.is_empty()
    {
        return Ok(());
    }

    Err(ApiError::bad_request("This route accepts no request body."))
}

/// A JSON `Value` -> typed request decode with the contract's error mapping: a shape or type
/// mismatch is a 400 naming the path serde reported, never a 500 from an unwrap.
///
/// Later handlers use this instead of calling `serde_json::from_value` directly, so every
/// route answers a malformed body the same way.
///
/// # Errors
///
/// [`ApiError::bad_request`] carrying serde's message.
pub fn decode_body<T>(value: serde_json::Value) -> Result<T, ApiError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(value).map_err(|error| ApiError::bad_request(error.to_string()))
}
// ---------------------------------------------------------------------------
// Wave 1 (`A1`) additions - the record surface, the two new writes, and the
// harness-wide tool switch. Every shape here is a verbatim projection of the TS
// contract; the comments name the interface it mirrors.
// ---------------------------------------------------------------------------

/// Body of `PATCH {root}/tools/{tool_id}` (`harness-api.ts` `PatchHarnessToolRequest`, parsed at
/// `api.ts:2228-2260`). ONE field, the harness-wide enable switch.
///
/// `enabled` stays a raw `Value` on purpose: the contract refuses a non-boolean with its OWN
/// sentence ("`\"enabled\"` must be a boolean ..."), which serde's type error would otherwise
/// replace. The handler reads the boolean out of it after the unknown-field gate, so a missing,
/// `null`, string, or number body all answer the contract's message.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PatchHarnessToolRequest {
    #[serde(default)]
    pub enabled: Option<serde_json::Value>,
}

impl PatchHarnessToolRequest {
    /// The boolean the contract demands, or the contract's own refusal (`api.ts:2243-2249`).
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] with the enable-switch sentence.
    pub fn enabled_boolean(&self) -> Result<bool, ApiError> {
        self.enabled
            .as_ref()
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| {
                ApiError::bad_request(
                    "\"enabled\" must be a boolean (true runs the tool, false disables it for every session)."
                        .to_owned(),
                )
            })
    }
}

/// One catalogue row plus the harness-wide enable switch (`harness-api.ts:640` `ToolSummary` as
/// `GET /tools` sends it, with the `enabled` member `persistence.ts:322` `toolSettings` decides).
///
/// A WRAPPER rather than a field on [`ToolSummary`], because the summary is built by
/// `read.rs::tool_summary_of`, which Wave 1 does not touch; `#[serde(flatten)]` makes the wire
/// identical to TS's single flat object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolListEntry {
    #[serde(flatten)]
    pub summary: ToolSummary,
    pub enabled: bool,
}

/// A catalogue detail plus the same switch (`harness-api.ts:654` `ToolDetail` as
/// `GET /tools/{tool_id}` and the PATCH answer send it).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDetailEntry {
    #[serde(flatten)]
    pub detail: ToolDetail,
    pub enabled: bool,
}

/// Envelope of the identified catalogue read and of the enable switch (`harness-api.ts:677`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDetailEntryResponse {
    pub tool: ToolDetailEntry,
}

/// Body of `PATCH {root}/sessions/{session_id}/turns/{turn_id}/blocks/{block_id}`
/// (`harness-api.ts:2222` `PatchTurnBlockRequest`): the two inclusion axes, and at least one of
/// them must be present. Request-side spelling is camelCase, so `reasoningIncluded` is spelled as
/// TS spells it.
///
/// The values stay `Option<bool>` because that is the shape; the ROUTE does not decode through
/// this type, it validates the raw body first (`api.ts:4597-4620` states one refusal sentence per
/// axis, which serde's type error would replace), then answers. Keeping the shape here is what
/// lets `tests/wave_one.rs` pin the wire form the route accepts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchTurnBlockRequest {
    /// The segment's own prompt axis - the exclusion unit behind a recorded call, or the
    /// `included` flag of the assistant entry a `reasoning` segment names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub included: Option<bool>,
    /// The thinking axis (`reasoningIncluded`), writable on a `reasoning` segment only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_included: Option<bool>,
}

/// The links a block-write receipt hands back (`harness-api.ts:2266-2269`), so the caller can
/// refetch exactly what it changed without guessing a path.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TurnBlockWriteLinks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript: Option<String>,
}

/// Receipt of the segment inclusion write (`harness-api.ts:2246`): the state the server
/// PERSISTED, which is what lets the dialog repaint from the write rather than from what it asked
/// for. `reasoning_included` appears exactly when the segment names a thinking entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TurnBlockWriteReceipt {
    pub session_id: String,
    pub turn_id: String,
    pub block_id: String,
    pub included: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_included: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<TurnBlockWriteLinks>,
}

/// Envelope of the segment write (`harness-api.ts` `PatchTurnBlockResponse`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TurnBlockWriteResponse {
    pub receipt: TurnBlockWriteReceipt,
}

/// One declared field of a record collection, projected for a renderer (`harness-api.ts:1463`
/// `RecordFieldDescriptor`): enough to draw columns and a filter, never the full engine spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RecordFieldDescriptor {
    pub key: String,
    pub r#type: RecordFieldType,
    pub required: bool,
    /// The closed value set of an `enum` field; absent for every other type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// The value types a declared field may take (`harness-api.ts:1466`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecordFieldType {
    Text,
    Boolean,
    Integer,
    Enum,
    Strings,
}

/// What one record did, in the store-wide feed (`harness-api.ts:1509` `RecordChangeRow.op`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecordChangeOp {
    Add,
    Update,
    Remove,
}

/// One stored record, projected exactly like a list page (`harness-api.ts:1478`
/// `RecordEntryRow`). Keys are OPEN: the declared fields are the collection's own, so they ride in
/// `extra` and serialise inline beside the four fixed keys. `rev` is the CAS token a guarded write
/// sends back as `expects: [{id, rev}]` - NOT the stream's `store_rev`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RecordEntryRow {
    pub id: String,
    pub rev: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_thread_id: Option<String>,
    /// The recorded time as the engine spells it (an ISO-8601 text stamp, not an epoch figure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// The collection's declared fields, in canonical order. A `getOnly` field (a chapter body)
    /// is ABSENT here, never emptied.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// One entry of the store-wide change feed (`harness-api.ts:1502` `RecordChangeRow`). A `remove`
/// names the record and stops: its `rev`/`item_rev` is the removal's store event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RecordChangeRow {
    pub seq: u64,
    pub collection: String,
    pub rev: u64,
    pub id: String,
    pub op: RecordChangeOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub at: String,
    /// Which fields an `update` actually changed, in canonical order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_rev: Option<u64>,
}

/// One collection of the session's record store, whole (`harness-api.ts:1524`
/// `RecordCollectionRow`). Entries are the whole collection up to its own bound, not a paged slice.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RecordCollectionRow {
    pub name: String,
    pub store_rev: u64,
    pub count: u64,
    pub fields: Vec<RecordFieldDescriptor>,
    pub entries: Vec<RecordEntryRow>,
}

/// The bootstrap read (`harness-api.ts:1538` `RecordsSnapshotResponse`), which IS the response
/// body of `GET {root}/sessions/{session_id}/records` - no envelope key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RecordsSnapshotResponse {
    pub session_id: String,
    pub revision: u64,
    pub store_rev: u64,
    pub collections_rev: u64,
    pub collections: Vec<RecordCollectionRow>,
    pub changes: Vec<RecordChangeRow>,
}

/// The delta read (`harness-api.ts:1556` `RecordsDeltaResponse`): the feed after `since` and
/// nothing else. `reset: true` says the cursor is no longer answerable and the client re-bootstraps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RecordsDeltaResponse {
    pub session_id: String,
    pub revision: u64,
    pub store_rev: u64,
    pub collections_rev: u64,
    pub changes: Vec<RecordChangeRow>,
    pub reset: bool,
}

/// The whole `records.updated` payload (`harness-api.ts:1571` `RecordsSignal`): the two
/// watermarks and no record, ever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RecordsSignal {
    pub store_rev: u64,
    pub collections_rev: u64,
}

/// Why a plugin's contribution is missing (`harness-api.ts:930` `SessionPluginDiagnostic.kind`).
/// The wire spells these kebab-case; the Rust names are the same words.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionPluginDiagnosticKind {
    /// A hook that threw.
    HandlerThrew,
    /// A hook that outlived its budget.
    HandlerTimedOut,
    /// A hook whose decision could not be parsed, so the fail-open rule dropped it.
    InvalidDecision,
    /// The plugin's own report of what it decided - the only kind with no `event`.
    Policy,
}

/// One plugin diagnostic the server received for a session (`harness-api.ts:930`): without this
/// record a broken policy plugin and an inert one answer identically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionPluginDiagnostic {
    pub at: u64,
    pub kind: SessionPluginDiagnosticKind,
    pub owner: String,
    /// The hook the diagnostic names; absent for a `policy` report, which names no hook.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Map<String, serde_json::Value>>,
}

// -- CONTRACT-END --
