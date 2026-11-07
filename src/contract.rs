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
/// Model-facing tool-result text budget (`tool_result_bytes`, `core/result-budget.ts`).
///
/// THIS BUILD's enforced window, not the published one. The uplift moved the budget to
/// 256 KiB (`core/result-budget.ts:54 MODEL_TOOL_RESULT_BUDGET_BYTES = 256 * 1024`), and
/// `harness-api.ts:216` / `harness.swagger.yaml:243` publish the same `262144` in the boot
/// document. The tool stage here still cuts one model-facing result at 64 KiB, because the
/// tool implementations and their suite (`tests/tools.rs`) are written against that window and
/// widening it is a behaviour change of THAT stage, not of this contract. The boot document
/// therefore advertises [`PUBLISHED_TOOL_RESULT_BYTES`] while enforcement keeps this number;
/// `knowledge.md` "T10 plugin uplift" records the split so the next agent sees the gap.
pub const MODEL_TOOL_RESULT_BUDGET_BYTES: usize = 65_536;

/// The `tool_result_bytes` figure the boot document publishes (`harness-api.ts:216`,
/// `harness.swagger.yaml:243`). Named as a constant so the ONE place this build advertises a
/// budget a different stage enforces more tightly is greppable rather than a bare literal.
pub const PUBLISHED_TOOL_RESULT_BYTES: u64 = 262_144;

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
        // The CONTRACT figure (`harness-api.ts:216`), not this build's enforced window
        // ([`MODEL_TOOL_RESULT_BUDGET_BYTES`]); see that constant for the documented split.
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

/// Role of one agent inside a definition or roster (`harness-api.ts:934`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentRole {
    Primary,
    Subagent,
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

/// One transcript entry (`types/index.ts:116`), discriminated by `role`. The provider
/// wire mapping happens in the transport; this is the shape the transcript routes page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum HarnessMessage {
    /// A system message: one entry per recorded instruction.
    #[serde(rename_all = "camelCase")]
    System { content: String },
    /// A user message; `attachments` is omitted for a text-only entry so every pre-attachment
    /// transcript keeps its exact old shape (`:121-128`).
    #[serde(rename_all = "camelCase")]
    User {
        content: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<HarnessAttachment>,
    },
    /// An assistant entry: `tool_calls` only when the turn requested tools, `reasoning`
    /// kept out of the wire payload but retained so a UI can replay a turn (`:130-137`).
    #[serde(rename_all = "camelCase")]
    Assistant {
        content: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<HarnessToolCall>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
    },
    /// A tool result; `attachments` are out-of-band media (`toolMedia()`), flushed by the
    /// transport as a labelled user-role message AFTER the tool block (`:143-160`).
    #[serde(rename_all = "camelCase")]
    Tool {
        content: String,
        tool_call_id: String,
        name: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<HarnessAttachment>,
    },
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

/// One named harness definition as a picker needs it (`harness-api.ts:422`). `agents` is
/// derived as `[primary, ...subagents]`. A card omits per-agent instruction TEXT, the
/// per-agent model map, and the resolved core-tool set; the identified read adds those.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HarnessCard {
    pub name: String,
    pub title: String,
    pub description: String,
    pub requires_endpoint: bool,
    pub primary: String,
    /// ALWAYS serialized (`harness-api.ts:432`): an empty array is a FACT, not an absence -
    /// it is what tells a picker that `delegate` is not registered for this harness.
    #[serde(default)]
    pub subagents: Vec<String>,
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

/// Selection-only create body (`harness-api.ts:468`): a client picks trusted agent names
/// and may record per-agent model ids and instruction overrides; it cannot supply a
/// provider, a credential, a transport, or a tool list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateHarnessRequest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub primary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagents: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instructions: Option<BTreeMap<String, String>>,
    /// The third map of the same shape (`harness-api.ts:484`): agent id -> CSS hex color. A client
    /// may record a swatch for each member it selects; the registry decides membership and the hex
    /// shape (`registry.ts:604-629`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_colors: Option<BTreeMap<String, String>>,
}

/// Patch body (`harness-api.ts:488`): an omitted field keeps its current value and the name
/// is immutable. `agent_models` and `agent_instructions` REPLACE the map they name (`{}`
/// clears it).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateHarnessRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagents: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instructions: Option<BTreeMap<String, String>>,
    /// REPLACE semantics like the two maps above (`harness-api.ts:507`): `{}` clears every color,
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

/// The declared type of one plugin setting (`harness-api.ts:650 PluginSetting.type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginSettingType {
    Number,
    String,
    Boolean,
}

/// One DECLARED setting of a plugin, copied verbatim from the plugin's own metadata
/// (`harness-api.ts:649`). `default` is the value the plugin falls back to when nothing is
/// stored AND the target a `null` patch entry restores. `min`/`max` bound what the server
/// accepts; `step` bounds only what a client offers and is deliberately NOT enforced
/// (`server/plugins.ts` header, "VALIDATION AT THE WRITE EDGE").
///
/// The numeric members are `Value`, not `f64`, because the wire spells an integral bound as an
/// INTEGER (`min: 1000`, never `1000.0`): serde_json prints an `f64` with its fraction, and the
/// contract's rows are asserted byte-exact. `crate::plugins` owns the one conversion that keeps
/// that promise.
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
}

/// One built-in plugin as an operator sees it (`harness-api.ts:663`): identity, the lifecycle
/// hooks it declares, its settings descriptors, and the values in force RIGHT NOW.
///
/// `builtin` is always `true` on this route - the catalogue is the set this build ships - and it
/// stays explicit on the wire so a client renders one row shape for a plugin a future release
/// makes configurable. The optional members are ABSENT, never `null`: a plugin that declares no
/// description, version, tags or hooks omits those keys (`api.ts pluginRowOf`'s presence filters).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginSummary {
    pub name: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Vec<String>>,
    pub builtin: bool,
    pub settings: Vec<PluginSetting>,
    /// The EFFECTIVE map (`declared default` unless the operator stored an override), in the
    /// plugin's own DECLARED key order (`api.ts:1963 plugins.values(metadata.name)`).
    pub values: serde_json::Map<String, serde_json::Value>,
}

/// Page of built-in plugin rows (`harness-api.ts:681`). Modeled as a flat struct that FLATTENS
/// `PageMeta` exactly like `HarnessCardPage`, so the five page members keep their contract names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginSummaryPage {
    #[serde(flatten)]
    pub meta: PageMeta,
    pub items: Vec<PluginSummary>,
}

/// Envelope of one plugin row, read or patched (`harness-api.ts:693`). A patch returns the WHOLE
/// row rather than a receipt, because the values it reports ARE the result of the write and an
/// editor needs the reset targets beside them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginDetailResponse {
    pub plugin: PluginSummary,
}

/// Settings patch for one plugin (`harness-api.ts:706`). A value REPLACES that key's override, an
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

/// Envelope of a session read, create, or patch (`harness-api.ts:851`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// One entry of a session's live agent roster (`harness-api.ts:931`): a row states both what
/// the session was BUILT with and what it RUNS now. Instruction TEXT lives on the
/// instruction subresource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionAgentBinding {
    pub agent_id: String,
    pub profile: String,
    pub role: AgentRole,
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

/// Binds an existing trusted profile into this session (`harness-api.ts:974`): no
/// implementation, tool list, or transport is accepted, so nothing here can inject code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateSessionAgentRequest {
    pub name: String,
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<AgentRole>,
}

/// An omitted property is no change; a string REPLACES the whole field; an explicit `null`
/// clears the session override and RESTORES the inherited binding (`harness-api.ts:988`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateSessionAgentRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<Option<String>>,
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
    pub event: HarnessEvent,
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
pub const HARNESS_STREAM_RESOURCE_KINDS: [&str; 20] = [
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
pub const HARNESS_WS_UPDATE_KINDS: [&str; 15] = [
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
    "tool.registry_updated",
    "agent.registry_updated",
    "run.event",
    "catalogue.updated",
    "config.updated",
];

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
        revision_records_per_state: 20,
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
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TurnOutcome {
    pub usage: Option<HarnessUsage>,
    pub error: Option<String>,
    pub cancelled: bool,
    pub messages: Vec<HarnessMessage>,
    pub stop_reason: Option<String>,
    /// Guidance this run DRAINED from its steering channel and then lost, because the run failed or
    /// was aborted and its transcript was rolled back (`harness.ts:6826-6837`: "R5 preservation:
    /// steering this run drained is rolled back with the transcript, so hand it to the NEXT host turn
    /// in its original order").
    ///
    /// Empty on a successful run, where every drained steer reached the model. The store re-queues
    /// this list onto the conversation's lane unless the run was explicitly stopped
    /// (`sessions.ts`/`harness.ts` discard it for a stopped thread), which is what keeps guidance a
    /// user already paid for from vanishing on a provider hiccup.
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
// -- CONTRACT-END --
