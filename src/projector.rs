//! The display projector: the fold every recorded event passes through on its way to a wire
//! row, a wire tree, and a bounded `display.ops` frame.
//!
//! TypeScript origin: `src/server/turn-projector.ts` (2,495 lines) plus the display model it
//! emits, `src/shared/turn-display.ts`. The op vocabulary, the node/step/tail/notice shapes,
//! the preview caps and the settle rules all come from there.
//!
//! THE ONE RULE THE STORE DEPENDS ON
//! ---------------------------------
//! `apply` is a PURE fold of the event stream: folding a restored turn's recorded events
//! through a FRESH projector produces the same row and the same tree the live run produced
//! (`store::TurnState::replay`, `ports.rs:197-200`). Nothing here reads a clock, the
//! filesystem, or a global: every figure comes from the event and the `at`/`index` the
//! recorder hands in.
//!
//! WHAT THIS PORT DOES NOT MINT, AND WHY
//! -------------------------------------
//! `ProjectorFactory::create()` takes no turn record (`ports.rs:228-232`), so three things
//! TypeScript reads off that record are not available here. Each is stated, not faked:
//!
//!   1. **`turnId`.** Ops and the display document carry `turnId: ""`, and section ids are
//!      minted `s{ordinal}` instead of `${turnId}:${ordinal}`. Both are opaque, unique within
//!      the turn, and stable across live and replay, which is all the wire contract requires
//!      ("Stable, opaque, server-minted", `turn-display.ts:891`). The HTTP stage holds the
//!      turn id and rewrites them if a client cares about the TypeScript spelling.
//!   2. **The prompt, thread id/label, attachments, references and the steer ring**, which
//!      `summaryOf` reads off the record (`turn-projector.ts:2846-2869`). `summary()` states
//!      `prompt: ""` and omits the rest; the row read model replaces them from the
//!      `SessionTurn` it already has (`store::read::TurnState::row` renames `prompt` into
//!      `promptPreview`/`promptChars`).
//!   3. **The context bar's record-only half.** The `user` and `system` blocks TypeScript mints
//!      on its prompt section (`turn-projector.ts:98-113`) have no input here: the projector seam
//!      is `ProjectorFactory::create(&self)` (`ports.rs:239-241`), which carries no prompt, and
//!      the protocol's event enum has no user or instruction event. The fold DOES mint the blocks
//!      its events describe - a `tool` block per call, rewritten by its result, and an
//!      `assistant` block per final answer - while `blocks` stays `[]`, because the list
//!      TypeScript publishes there is the prompt section's.
//!
//!      THE ID OFFSET THAT FOLLOWS FROM IT: TypeScript numbers a block with
//!      `${section.sections.length}-${kind}-${key}`, and its `sections` array already holds the
//!      prompt section at index 0. This fold has none, so the same block on the same flow is ONE
//!      LOWER (`1-tool-c…` where TypeScript says `2-tool-c…`), stable across live and replay like
//!      every other id here.
//!
//! The pending-tool line (`pending:set` / `PENDING_NODE_ID`) is absent too: this crate's turn
//! engine runs a tool round to completion before the next request, so from the projector's
//! point of view nothing is ever "still running".
//!
//! TWO PLACES THE STAGE BRIEF AND TYPESCRIPT DISAGREE, AND TYPESCRIPT WINS
//! ----------------------------------------------------------------------
//!   * a transient `retry` mints NO notice node (`turn-projector.ts:2327-2359`, R16a: eleven
//!     attempts printed eleven lines of chatter and the tile read as a failing run). The failed
//!     attempt's nodes still go, through `attempt:discard`, which is the ONLY thing that
//!     handler does;
//!   * a `wakeup` notice does not take the tail: a wake line is server status, not model
//!     content, so the block a reader is looking at stays displayed beside it (R23).
//!
//! A `steer` is not a recorded event in this contract (`contract::HarnessEventType` has no
//! `steer` member; the store keeps guidance in the turn's `steers` ring), so the `steer` notice
//! tone TypeScript supports is never minted from the event fold here.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Map, Value, json};

use crate::contract::{
    BranchTerminalStatus, HarnessActionChoice, HarnessEvent, HarnessToolOutcome, HarnessUsage,
    ToolMediaDescriptor, ToolUiBlock, ToolUiPanel,
};
use crate::ports::{Projector, ProjectorFactory, ProjectorSeed};

/// `DISPLAY_PREVIEW_CHARS` (`turn-display.ts:102`): a settled step's result line.
pub const PREVIEW_CHARS: usize = 280;
/// `DISPLAY_ARGS_PREVIEW_CHARS` (`:105`): a step's argument line.
pub const ARGS_PREVIEW_CHARS: usize = 200;
/// `DISPLAY_NOTICE_CHARS` (`:108`).
pub const NOTICE_CHARS: usize = 400;
/// `DISPLAY_ERROR_CHARS` (`:111`): the error line, and the row's `detail`.
pub const ERROR_CHARS: usize = 600;
/// `DISPLAY_BLOCK_PREVIEW_CHARS` (`:117`): the block preview, used by the summary's `blocks`.
pub const BLOCK_PREVIEW_CHARS: usize = 200;
/// `DISPLAY_PANEL_TITLE_CHARS` (`:120`).
pub const PANEL_TITLE_CHARS: usize = 120;
/// `DISPLAY_PANEL_PREVIEW_CHARS` (`:123`).
pub const PANEL_PREVIEW_CHARS: usize = 240;
/// `DISPLAY_NODE_PREVIEW_CHARS` (`:140`): the bound on a SUPERSEDED prose node.
pub const NODE_PREVIEW_CHARS: usize = 320;
/// `DISPLAY_PANELS_PER_STEP` (`:158`).
pub const PANELS_PER_STEP: usize = 4;
/// `DISPLAY_MEDIA_PER_STEP` (`:162`).
pub const MEDIA_PER_STEP: usize = 16;
/// The section key of the turn's own root flow (`turn-projector.ts:103`).
pub const ROOT_KEY: &str = "root";

// ---------------------------------------------------------------------------
// The two preview directions (`turn-projector.ts:134-190`)
//
// Every count is in UTF-16 code units, because every TypeScript cap is
// `String.prototype.length` and the wire states the unit ("UTF-16 units, invariant 5").
// ---------------------------------------------------------------------------

/// The exact recorded length of a body in UTF-16 code units (invariant 5).
#[must_use]
pub fn display_chars(text: &str) -> usize {
    text.encode_utf16().count()
}

/// The byte offset at which a prefix of `units` UTF-16 code units ends.
fn byte_offset_of_units(text: &str, units: usize) -> usize {
    let mut taken = 0usize;

    for (offset, character) in text.char_indices() {
        if taken + character.len_utf16() > units {
            return offset;
        }

        taken += character.len_utf16();
    }

    text.len()
}

/// The suffix of `text` that begins after `units` UTF-16 code units.
fn suffix_after_units(text: &str, units: usize) -> &str {
    let offset = byte_offset_of_units(text, units).min(text.len());

    &text[offset..]
}

/// `flattenForDisplay` (`turn-projector.ts:134-148`): the one-way markdown flattening every
/// preview is built from. The nine rewrites run in TypeScript's order; its `/gm` rule becomes
/// Rust's `(?m)`.
#[must_use]
pub fn flatten_for_display(text: &str) -> String {
    // A bad pattern here is a programming error, and falling back to the input keeps a
    // preview from ever becoming a failure path.
    fn rewrite(source: &str, pattern: &str, replacement: &str) -> String {
        match regex::Regex::new(pattern) {
            Ok(regex) => regex.replace_all(source, replacement).into_owned(),
            Err(_) => source.to_owned(),
        }
    }

    let mut current = text.to_owned();

    current = rewrite(&current, r"```[^\n]*\n?", " ");
    current = rewrite(&current, r"!\[[^\]]*\]\(([^)]*)\)", "$1");
    current = rewrite(&current, r"\[([^\]]*)\]\(([^)]*)\)", "$1");
    current = rewrite(&current, "`", "");
    current = rewrite(&current, r"[*_~>#]+", " ");
    current = rewrite(&current, r"\|", " ");
    current = rewrite(&current, r"(?m)^\s*[-=]{3,}\s*$", " ");
    current = rewrite(&current, r"\s+", " ");
    current = rewrite(&current, r"\s+([.,!?;:])", "$1");

    current.trim().to_owned()
}

/// `previewText` (`:150-156`): flatten, then bound the HEAD with a single `…`. The right
/// shape for "what is this block about", i.e. a step, panel or notice line.
#[must_use]
pub fn preview_text(text: &str, limit: usize) -> String {
    let flattened = flatten_for_display(text);

    if display_chars(&flattened) <= limit {
        return flattened;
    }

    let head = &flattened[..byte_offset_of_units(&flattened, limit.saturating_sub(1))];

    format!("{}…", head.trim_end())
}

/// `previewTextTail` (`:176-190`): flatten, then bound from the END, so a prose node that
/// lost the tail keeps showing its LATEST lines instead of a frozen opening. The window opens
/// on a whole character, never between the halves of a surrogate pair.
#[must_use]
pub fn preview_text_tail(text: &str, limit: usize) -> String {
    let flattened = flatten_for_display(text);
    let total = display_chars(&flattened);

    if total <= limit {
        return flattened;
    }

    let skip = total.saturating_sub(limit.saturating_sub(1));

    format!("…{}", suffix_after_units(&flattened, skip).trim_start())
}

/// The display name of one tool outcome (`types/index.ts:215` spellings).
#[must_use]
pub fn outcome_name(outcome: HarnessToolOutcome) -> &'static str {
    match outcome {
        HarnessToolOutcome::Completed => "completed",
        HarnessToolOutcome::Failed => "failed",
        HarnessToolOutcome::Rejected => "rejected",
        HarnessToolOutcome::Cancelled => "cancelled",
    }
}

/// `outcomeState` + `outcomeOfResult` (`turn-projector.ts:197-210`): `isError` is the boolean
/// projection of `outcome`, so a frame that predates `outcome` still reads correctly.
#[must_use]
pub fn step_state_of(outcome: Option<&str>, is_error: bool) -> &'static str {
    match outcome {
        Some("completed") => "ok",
        Some("cancelled") => "cancelled",
        Some("failed") | Some("rejected") => "error",
        _ => {
            if is_error {
                "error"
            } else {
                "ok"
            }
        }
    }
}

/// `sectionStateOf` (`:573-582`).
#[must_use]
pub fn section_state_of(status: &str) -> &'static str {
    match status {
        "done" => "done",
        "error" => "error",
        "cancelled" => "cancelled",
        "running" => "streaming",
        _ => "open",
    }
}

/// `abandonedStateOf` (`:584`): a cancelled call is a control outcome, everything else an error.
#[must_use]
pub fn abandoned_state_of(status: &str) -> &'static str {
    if status == "cancelled" {
        "cancelled"
    } else {
        "error"
    }
}

/// `TURN_STATE_OF` (`:586-592`): the tile prints this, never a mapping of its own.
#[must_use]
pub fn turn_state_of(status: &str) -> &'static str {
    match status {
        "running" => "streaming",
        "done" => "done",
        "error" => "error",
        "cancelled" => "cancelled",
        _ => "queued",
    }
}

/// `isTerminalStatus` (`:594`).
#[must_use]
pub fn is_terminal_status(status: &str) -> bool {
    matches!(status, "done" | "error" | "cancelled")
}

/// `TurnProjector.eventAgent` / `shared/turn-display.ts eventAgent`: every event names its
/// agent, so a multi-agent run stays readable in one stream.
#[must_use]
pub fn event_agent(event: &HarnessEvent) -> &str {
    match event {
        HarnessEvent::Start { agent, .. }
        | HarnessEvent::Text { agent, .. }
        | HarnessEvent::Reasoning { agent, .. }
        | HarnessEvent::ToolCall { agent, .. }
        | HarnessEvent::ToolResult { agent, .. }
        | HarnessEvent::Usage { agent, .. }
        | HarnessEvent::Done { agent, .. }
        | HarnessEvent::Retry { agent, .. }
        | HarnessEvent::AttemptCommit { agent, .. }
        | HarnessEvent::Wakeup { agent, .. }
        | HarnessEvent::ActionRequest { agent, .. }
        | HarnessEvent::ActionAnswer { agent, .. }
        | HarnessEvent::BranchStart { agent, .. }
        | HarnessEvent::BranchEnd { agent, .. }
        | HarnessEvent::Error { agent, .. } => agent,
    }
}

/// The invocation identity one event carries, if any (`types/index.ts:1864`).
fn event_thread(event: &HarnessEvent) -> Option<&crate::contract::HarnessThread> {
    match event {
        HarnessEvent::Start { thread, .. }
        | HarnessEvent::Text { thread, .. }
        | HarnessEvent::Reasoning { thread, .. }
        | HarnessEvent::ToolCall { thread, .. }
        | HarnessEvent::ToolResult { thread, .. }
        | HarnessEvent::Usage { thread, .. }
        | HarnessEvent::Done { thread, .. }
        | HarnessEvent::Retry { thread, .. }
        | HarnessEvent::AttemptCommit { thread, .. }
        | HarnessEvent::Wakeup { thread, .. }
        | HarnessEvent::ActionRequest { thread, .. }
        | HarnessEvent::ActionAnswer { thread, .. }
        | HarnessEvent::BranchStart { thread, .. }
        | HarnessEvent::BranchEnd { thread, .. }
        | HarnessEvent::Error { thread, .. } => thread.as_ref(),
    }
}

// ---------------------------------------------------------------------------
// The fold
// ---------------------------------------------------------------------------

/// One node's INTERNAL state: the exact recorded text, with the wire copy derived from it
/// (`turn-projector.ts:1197-1199`: "the internal node keeps the exact text; the inserted copy
/// is the bounded wire shape").
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Node {
    /// `text`, `reasoning`, `notice`, `tool`, or `action`.
    pub kind: String,
    /// Stable id: `n{index}` for prose and notices, `{index}` for a step.
    pub id: String,
    /// The whole recorded body (prose/notice); empty for a step.
    pub text: String,
    /// The exact recorded length, in UTF-16 units, whether or not the wire shows all of it.
    pub chars: usize,
    /// A `text` node's committed flag: an attempt window a later `retry` may still discard.
    pub committed: bool,
    /// True once the wire copy of this node is a bounded preview.
    pub redacted: bool,
    pub tone: Option<String>,
    pub at: Option<u64>,
    /// The step this notice explains (an answered question, a wake line).
    pub step_id: Option<String>,
    /// The compact step descriptor, for a `tool`/`action` node.
    pub step: Option<Value>,
}

/// One attempt's minted nodes, so a `retry` can name the exact ones to drop
/// (`turn-projector.ts:1023-1027`).
#[derive(Clone, Debug, Default, PartialEq)]
struct Attempt {
    node_ids: Vec<String>,
}

/// One agent flow: the turn's root invocation, or one nested invocation/branch
/// (`turn-display.ts:890-964`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Section {
    /// The fold's routing key: `root`, an invocation id, `inline:<agent>`, or a branch id.
    key: String,
    ordinal: u64,
    id: String,
    role: String,
    agent: String,
    model: Option<String>,
    invocation_id: Option<String>,
    branch_id: Option<String>,
    parent_section_id: Option<String>,
    state: String,
    started_at: Option<u64>,
    ended_at: Option<u64>,
    latest_input: Option<u64>,
    cached_input: Option<u64>,
    cached_reported: bool,
    answer_node_id: Option<String>,
    tail: Option<Value>,
    /// The CONTEXT BAR entries of this flow (`TurnSection.context`, `turn-display.ts:953`), in
    /// mint order: a `tool` block follows every call and is rewritten when its result lands, and
    /// the root flow’s final answer becomes an `assistant` block (`pushBlock`,
    /// `turn-projector.ts:1029-1054`). Empty for a flow whose events describe no bar entry.
    context: Vec<Value>,
    nodes: Vec<Node>,
    /// The prose node this request is still streaming into; detached at every boundary.
    text_writer: Option<usize>,
    reasoning_writer: Option<usize>,
    attempt: Option<Attempt>,
    started: bool,
    terminal: bool,
}

/// One turn's compiled DISPLAY state.
#[derive(Debug, Default)]
pub struct TurnProjector {
    sections: Vec<Section>,
    section_ordinal: u64,
    step_count: u64,
    turn_latest_input: Option<u64>,
    turn_cached_input: Option<u64>,
    usage: Option<HarnessUsage>,
    last_error: Option<String>,
    settled_status: Option<String>,
    settled_at: Option<u64>,
    last_applied_at: Option<u64>,
    /// An owner question's step, so its answer can settle the same row by request id.
    action_steps: BTreeMap<String, String>,
    /// The context-bar segment the NEXT tool result settles, as (section index, block id)
    /// (`pendingToolBlocks`, `turn-projector.ts:3378`). The pairing is by call, in order, exactly
    /// as the step pairing is, so an out-of-order result settles the right segment or none.
    pending_block: Option<(usize, String)>,
    /// What the store seeded this fold with (`ports::ProjectorSeed`).
    ///
    /// It is the fold's answer to the seam's one honest limitation: `createTurnProjector` is handed a
    /// `DisplayProjectContext`, not just an event stream (`sessions.ts:4160-4240`), and three context-bar
    /// segments (the system block, the prompt block, and the steer blocks) plus every block id's section
    /// prefix come from THAT, not from the events. A fold created without a seed still compiles a
    /// document — it simply states nothing it was never told.
    seed: ProjectorSeed,
    /// The lane stamped on the event being folded right now, already normalized (`None` means lane 0).
    ///
    /// It is per-EVENT state, deliberately not per-turn: the recorder stamps each event with the lane the
    /// conversation was on when the event arrived, so a turn that spans a compaction mints segments on two
    /// rows and the fold reproduces that from the durable records alone (`turn-projector.ts:1435`).
    event_lane: Option<u64>,
}

impl TurnProjector {
    /// A fold over an empty turn, with nothing to seed it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A fold opened with the turn's context (`createTurnProjector(ctx)`, `turn-projector.ts:620-700`).
    #[must_use]
    pub fn seeded(seed: ProjectorSeed) -> Self {
        Self {
            seed,
            ..Self::default()
        }
    }

    /// The `{ordinal}-` prefix every block id carries (`blockIdOf`, `turn-projector.ts:1407`).
    ///
    /// This is the CONVERSATION'S section ordinal — the fixture's seed turn states `0-system-instructions`
    /// and `0-tool-80` because its thread's section is ordinal 0. It is NOT the number of sections this
    /// fold has opened: a fold that counted its own rows would renumber every segment the moment a
    /// delegation opened a second flow, and a captured block id would stop resolving after a restart
    /// (invariant 4).
    #[must_use]
    fn block_ordinal(&self) -> u64 {
        self.seed.section_ordinal
    }

    /// Stamps the lane `block` was minted on, LAST, and only when there is one.
    ///
    /// The value is `self.event_lane`, which has exactly two suppliers: [`Self::apply`] sets it from the
    /// event being folded (`turn-projector.ts:1435 laneOfEvent`) and [`Self::add_block_with_lane`] swaps
    /// in the TURN's starting lane for a segment that is not an event (`:1441 laneOfTurn`). Both feed the
    /// same writer because there is one mint point, [`Self::add_block`].
    ///
    /// Absence means lane 0 and the fold never invents a number (`:1449 laneFieldsOf`): a lane-0 segment
    /// writes no key, which is what keeps a session that never compacted byte-identical to the document
    /// this crate wrote before lanes existed.
    fn with_lane(&self, block: Value) -> Value {
        let Some(lane) = self.event_lane else {
            return block;
        };

        match block.as_object() {
            Some(entries) if lane != 0 => {
                let mut entries = entries.clone();
                entries.insert("lane".to_owned(), json!(lane));
                Value::Object(entries)
            }
            _ => block,
        }
    }

    /// The context-bar segments that are NOT events, minted once when the root flow opens
    /// (`syncRecordBlocks`, `turn-projector.ts:1543-1600`, and `syncSteerBlocks`, `:1609-1660`).
    ///
    /// All three take the lane the TURN started on (`seed.lane`, `:1441 laneOfTurn`) — the pairing that
    /// puts a post-compaction turn's prompt and answer on the same row — and each is metadata only, like
    /// every other segment.
    fn seed_root_segments(&mut self, section: usize, ops: &mut Vec<Value>) {
        let (turn_id, section_id) = {
            let section = &self.sections[section];
            (self.seed.turn_id.clone(), section.id.clone())
        };

        if turn_id.is_empty() {
            // Nothing was seeded, so nothing can be stated: the fold mints no segment with an empty
            // identity rather than one the block route could never resolve.
            return;
        }

        let at = self.seed.started_at.unwrap_or(0);
        let ordinal = self.block_ordinal();

        // The ONE system block, from the instruction sources recorded for THIS turn. No recorded source
        // means no segment at all (`:1544-1552`), and the call id is `instructions-${sectionId}` so the
        // block route can answer "what was the agent told".
        if !self.seed.instruction_sources.is_empty() {
            let chars: usize = self
                .seed
                .instruction_sources
                .iter()
                .map(|source| {
                    source
                        .get("text")
                        .and_then(Value::as_str)
                        .map(display_chars)
                        .unwrap_or(0)
                })
                .sum();

            let block = json!({
                "kind": "system",
                "id": format!("{ordinal}-system-instructions"),
                "turnId": turn_id,
                "sectionId": section_id,
                "name": "instructions",
                "at": at,
                "chars": chars,
                "inPrompt": true,
                "callId": format!("instructions-{section_id}"),
                "stepId": "instructions-system",
            });

            self.add_block_with_lane(section, block, self.seed.lane, ops);
        }

        // The prompt block. Its call id resolves to the conversation's first user transcript entry, which
        // is what lets the block detail route show the operator's own bytes (`:1578-1590`).
        let prompt_chars: usize = display_chars(&self.seed.prompt);

        if prompt_chars > 0 {
            let mut block = json!({
                "kind": "user",
                "id": format!("{ordinal}-user-prompt"),
                "turnId": turn_id,
                "sectionId": section_id,
                "name": "prompt",
                "at": at,
                "chars": prompt_chars,
                "inPrompt": true,
            });

            // Let-chains (`rust-toolchain` edition allows them): one binding for the message id and one
            // for the map it is written into.
            if let Some(message_id) = self.seed.user_message_id.as_deref()
                && let Some(entries) = block.as_object_mut()
            {
                entries.insert(
                    "callId".to_owned(),
                    json!(format!("transcript-{message_id}")),
                );
                entries.insert("stepId".to_owned(), json!("transcript-user"));
            }

            self.add_block_with_lane(section, block, self.seed.lane, ops);
        }

        // One block per accepted steered submit (`:1609-1660`). The id keys on the SUBMIT id, so the same
        // submit re-folded after a restart keeps its row and a discarded attempt cannot renumber it.
        // Cloned because the loop mints through `&mut self`.
        let steers = self.seed.steers.clone();

        for submit in &steers {
            let Some(id) = submit.get("id").and_then(Value::as_str) else {
                continue;
            };

            let prompt = submit.get("prompt").and_then(Value::as_str).unwrap_or("");
            let at = submit.get("at").and_then(Value::as_u64).unwrap_or(at);

            let block = json!({
                "kind": "user",
                "id": format!("{ordinal}-user-{id}"),
                "turnId": turn_id,
                "sectionId": section_id,
                "name": "steer",
                "at": at,
                "chars": display_chars(prompt),
                "inPrompt": true,
                "callId": format!("steer-{id}"),
                "stepId": format!("steer-{id}"),
            });

            self.add_block_with_lane(section, block, self.seed.lane, ops);
        }
    }

    /// [`Self::add_block`] with an explicit lane — the lane a NON-event segment takes is the turn's, never
    /// the current event's, so the two sources cannot be conflated.
    ///
    /// The swap stays in scope for the whole `add_block` call, because that call is where the lane is
    /// actually written (`add_block` stamps through [`Self::with_lane`], the single mint point). The
    /// remembered value is then put back so the NEXT event-driven segment still sees the lane of the
    /// event that opened it (`turn-projector.ts:1441 laneOfTurn` vs `:1435 laneOfEvent`).
    fn add_block_with_lane(
        &mut self,
        section: usize,
        block: Value,
        lane: Option<u64>,
        ops: &mut Vec<Value>,
    ) {
        let remembered = self.event_lane;
        self.event_lane = crate::persistence::LaneStamp::of(lane);
        self.add_block(section, block, ops);
        self.event_lane = remembered;
    }

    /// The status the row reports: the settled one once the store settled the fold, else
    /// `running` for a fold that has seen events and `queued` for one that has not.
    #[must_use]
    pub fn status(&self) -> String {
        self.settled_status.clone().unwrap_or_else(|| {
            if self.last_applied_at.is_none() {
                "queued".to_owned()
            } else {
                "running".to_owned()
            }
        })
    }

    /// The last timestamp this fold saw: the record's `updatedAt` figure the row reports.
    #[must_use]
    pub fn updated_at(&self) -> u64 {
        self.last_applied_at.unwrap_or(0)
    }

    /// The root flow's model, which is a DONE turn's status line.
    fn root_model(&self) -> Option<String> {
        self.sections
            .iter()
            .find(|section| section.key == ROOT_KEY)
            .and_then(|section| section.model.clone())
    }

    /// Allocates one section (`newSection`, `turn-projector.ts:663-723`): the ordinal is
    /// allocated in mint order and the nesting is resolved by the SERVER, so a renderer never
    /// infers a parent.
    fn mint_section(&mut self, key: &str, agent: &str, parent_id: Option<String>) -> Section {
        let ordinal = self.section_ordinal;

        self.section_ordinal += 1;

        // A child this fold did not resolve a parent for is nested under the root row, which
        // is the only nesting the fold can prove without the store's invocation map.
        let derived_parent = if key != ROOT_KEY && ordinal > 0 {
            self.sections.first().map(|section| section.id.clone())
        } else {
            None
        };

        Section {
            key: key.to_owned(),
            ordinal,
            // The wire id is `${turnId}:${ordinal}` (`turn-projector.ts:698`). A seeded fold can mint it
            // directly; an unseeded one keeps the `s{ordinal}` placeholder the store rewrites on the way
            // out (`projector::section_wire_id`), which is the same fact stated later instead of now.
            id: if self.seed.turn_id.is_empty() {
                format!("s{ordinal}")
            } else {
                format!("{}:{ordinal}", self.seed.turn_id)
            },
            // The root flow is `primary`; every other row this fold mints is a subagent
            // (`turn-display.ts:896`).
            role: if key == ROOT_KEY {
                "primary"
            } else {
                "subagent"
            }
            .to_owned(),
            agent: agent.to_owned(),
            state: "open".to_owned(),
            parent_section_id: parent_id.or(derived_parent),
            ..Section::default()
        }
    }

    /// The section one event belongs to, minting and opening it when needed (`route`,
    /// `turn-projector.ts:921-1017`, condensed to the identity rules the Rust events produce).
    fn route(&mut self, event: &HarnessEvent, ops: &mut Vec<Value>) -> usize {
        let agent = event_agent(event).to_owned();
        let meta = event_thread(event).cloned();

        if let Some(meta) = meta {
            // A known invocation answers with its own row - never a second one.
            if let Some(index) = self.sections.iter().position(|section| {
                section.key == meta.id || section.invocation_id.as_deref() == Some(meta.id.as_str())
            }) {
                return index;
            }

            // A thread record with no parent, no spawning call, no kind and no branch is the
            // TURN's own conversation (`:787-788`), never an agent-spawned child.
            let is_conversation_root = meta.parent_id.is_none()
                && meta.call_id.is_none()
                && meta.kind.is_none()
                && meta.branch_id.is_none();

            if is_conversation_root {
                if let Some(index) = self
                    .sections
                    .iter()
                    .position(|section| section.key == ROOT_KEY)
                {
                    // Same agent, no parent: another frame of the host-thread run. Adopt the
                    // invocation id if the row has none yet (`:944-952`).
                    if self.sections[index].invocation_id.is_none() {
                        self.sections[index].invocation_id = Some(meta.id.clone());
                        self.sections[index].key = meta.id.clone();

                        ops.push(section_set(
                            &self.sections[index],
                            &json!({ "invocationId": &meta.id }),
                        ));
                    }

                    return index;
                }

                let mut section = self.mint_section(ROOT_KEY, &agent, None);

                section.invocation_id = Some(meta.id.clone());
                section.state = "streaming".to_owned();
                section.started = true;

                self.sections.push(section);
                let index = self.sections.len() - 1;

                open_section(&self.sections[index], ops);

                // The non-event segments are minted with the flow that owns them, BEFORE anything this
                // event adds, which is the ordering contract `context:add` honors: the bar arrives in
                // conversation order, so a client appends.
                self.seed_root_segments(index, ops);

                return index;
            }

            let parent = meta
                .parent_id
                .as_deref()
                .and_then(|parent| {
                    self.sections.iter().position(|section| {
                        section.key == parent || section.invocation_id.as_deref() == Some(parent)
                    })
                })
                .map(|index| self.sections[index].id.clone());

            let mut section = self.mint_section(&meta.id, &agent, parent);

            section.branch_id = meta.branch_id.clone();
            section.state = "streaming".to_owned();
            section.started = true;

            self.sections.push(section);
            let index = self.sections.len() - 1;

            open_section(&self.sections[index], ops);

            return index;
        }

        // A threadless frame from the agent the root row already speaks for is that row.
        if let Some(index) = self
            .sections
            .iter()
            .position(|section| section.key == ROOT_KEY && section.agent == agent)
        {
            return index;
        }

        // The turn's own conversation is the FIRST flow this fold sees. TypeScript creates that
        // row when it builds the projector, and this seam's `create()` has no turn record, so a
        // threadless frame from the agent that starts the turn IS the root row - and the root
        // flow is `primary`, never a subagent (`turn-display.ts:896`).
        let key = if self.sections.is_empty() {
            ROOT_KEY.to_owned()
        } else {
            format!("inline:{agent}")
        };

        if let Some(index) = self.sections.iter().position(|section| section.key == key) {
            return index;
        }

        // A threadless stream from another agent still needs a flow of its own, so its prose
        // and calls are not dropped (`:862-873`).
        let mut section = self.mint_section(&key, &agent, None);

        section.state = "streaming".to_owned();
        section.started = true;

        self.sections.push(section);
        let index = self.sections.len() - 1;

        open_section(&self.sections[index], ops);

        index
    }

    /// Detaches the prose writers, so the next request streams into nodes of its OWN and a
    /// retry cannot remove a committed half (`closeWriters`, `:1032-1039`).
    fn close_writers(&mut self, section: usize) {
        let section = &mut self.sections[section];

        section.text_writer = None;
        section.reasoning_writer = None;
    }

    /// Opens the attempt window a later `retry` may discard (`beginAttempt`, `:1023-1027`).
    fn begin_attempt(&mut self, section: usize) {
        let section = &mut self.sections[section];

        if section.attempt.is_none() {
            section.attempt = Some(Attempt::default());
        }
    }

    fn track_attempt_node(&mut self, section: usize, node_id: &str) {
        if let Some(attempt) = self.sections[section].attempt.as_mut() {
            attempt.node_ids.push(node_id.to_owned());
        }
    }

    /// Marks this attempt's prose committed, states the copy target, and closes the window
    /// (`commitAttempt`, `:1041-1077`).
    fn commit_attempt(&mut self, section: usize, ops: &mut Vec<Value>) {
        self.close_writers(section);

        let Some(attempt) = self.sections[section].attempt.take() else {
            return;
        };

        let section_id = self.sections[section].id.clone();

        for node_id in &attempt.node_ids {
            let Some(position) = self.sections[section]
                .nodes
                .iter()
                .position(|node| node.id == *node_id)
            else {
                continue;
            };

            // Only PROSE has a committed flag: a step and a notice are not discardable here.
            if self.sections[section].nodes[position].kind != "text" {
                continue;
            }

            self.sections[section].nodes[position].committed = true;

            ops.push(json!({
                "op": "node:set",
                "turnId": "",
                "sectionId": section_id,
                "nodeId": node_id,
                "patch": { "committed": true },
            }));
        }

        // The copy target is the LAST committed prose node of this attempt - a pointer, so the
        // prose is carried once rather than duplicated (`:1060-1072`).
        let answer = self.sections[section]
            .nodes
            .iter()
            .rev()
            .find(|node| node.kind == "text" && !node.text.is_empty())
            .map(|node| node.id.clone());

        if let Some(node_id) = answer {
            self.sections[section].answer_node_id = Some(node_id.clone());

            ops.push(section_set(
                &self.sections[section],
                &json!({ "answerNodeId": node_id }),
            ));
        }

        ops.push(json!({
            "op": "attempt:commit",
            "turnId": "",
            "sectionId": section_id,
        }));
    }

    /// Removes exactly the nodes a failed attempt streamed (`discardAttempt`, `:1079-1120`).
    fn discard_attempt(&mut self, section: usize, ops: &mut Vec<Value>) {
        self.close_writers(section);

        let Some(attempt) = self.sections[section].attempt.take() else {
            return;
        };

        if attempt.node_ids.is_empty() {
            return;
        }

        let section_id = self.sections[section].id.clone();

        // The op names the exact nodes to drop, so a retry never depends on the client
        // tracking an attempt window (`turn-display.ts:1068-1069`).
        ops.push(json!({
            "op": "attempt:discard",
            "turnId": "",
            "sectionId": section_id,
            "nodeIds": attempt.node_ids,
        }));

        self.sections[section]
            .nodes
            .retain(|node| !attempt.node_ids.contains(&node.id));
    }

    /// Inserts one notice node (`noticeNode`, `:1251-1273`). A notice is server status, so it
    /// joins no attempt window.
    //
    // The eight-argument shape is the node's own field list plus the fold's two context
    // arguments (which section, which op buffer): every parameter is a display field the op must
    // carry, and a wrapper struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    fn insert_notice(
        &mut self,
        section: usize,
        tone: &str,
        text: &str,
        id: &str,
        at: Option<u64>,
        step_id: Option<&str>,
        ops: &mut Vec<Value>,
    ) {
        let bounded = preview_text(text, NOTICE_CHARS);

        let node = Node {
            kind: "notice".to_owned(),
            id: id.to_owned(),
            chars: display_chars(text),
            text: bounded,
            tone: Some(tone.to_owned()),
            at: at.filter(|value| *value > 0),
            step_id: step_id.map(str::to_owned),
            ..Node::default()
        };

        let section = &mut self.sections[section];
        let position = section.nodes.len();

        section.nodes.push(node);

        ops.push(json!({
            "op": "node:insert",
            "turnId": "",
            "sectionId": section.id.clone(),
            "node": wire_node(section, position),
        }));
    }

    /// `addBlock` (`turn-projector.ts:1029-1054`, id from `blockIdOf:1005-1007`): append one
    /// context-bar entry to a flow and publish it as `context:add`. The caller builds the shape.
    ///
    /// A `ContextBlock` is METADATA ONLY (`turn-display.ts:434-559`): no text and no preview, so
    /// the bar stays bounded however long the conversation runs. The bytes of the recorded range
    /// `[eventFrom..=eventTo]` are fetched per block on a click, which is why the lazy block route
    /// answers from the durable record (`api::turns::find_block` + `build_turn_block_detail`).
    ///
    /// The id is `{section count}-{kind}-{key}`, the count being TypeScript's
    /// `section.sections.length`. This fold has no prompt section at index 0, so the same segment
    /// sits ONE ORDINAL LOWER than TypeScript's (`1-tool-7` where TypeScript says `2-tool-7`);
    /// it is still minted once and never renumbered, so an id captured before a restart resolves
    /// after it (invariant 4).
    fn add_block(&mut self, section: usize, block: Value, ops: &mut Vec<Value>) {
        // THE ONE MINT POINT, so the lane is stamped the same way for every segment: an event-driven
        // segment takes `self.event_lane`, which `apply` set from the event's own recorded stamp
        // (`turn-projector.ts:1435 laneOfEvent`, applied at every `context:add` mint site — `:1564`,
        // `:1588`, `:1662`, `:2163`, `:2219`, `:2313`, `:2490`, `:2603`). The non-event seam
        // ([`Self::add_block_with_lane`]) swaps that value for the TURN's starting lane before
        // delegating here, so no caller can mint an unlaned segment and no caller can patch a lane.
        let block = self.with_lane(block);

        self.sections[section].context.push(block);

        ops.push(json!({
            "op": "context:add",
            "turnId": "",
            "sectionId": self.sections[section].id,
            // The op carries the row AS STORED, so a client that applies the op reaches exactly
            // the document a cold replay compiles.
            "block": self.sections[section].context.last().cloned().unwrap_or(Value::Null),
        }));
    }

    /// `patchBlock` (`:1061-1082`): merge a patch into one segment and publish it as
    /// `context:set`. Identity members (`id`, `kind`, `turnId`, `sectionId`, `eventFrom`) are
    /// immutable (`turn-display.ts:561-566`), so a patch that would change one of those must be a
    /// `context:remove` + `context:add` pair instead - none of the patches here does.
    ///
    /// A patch for a block this fold has never minted is dropped: TypeScript's `updateToolBlock`
    /// returns at `:1065` for the same reason, and inventing a late segment would put a row in the
    /// bar that no event ordered.
    fn patch_block(
        &mut self,
        section: usize,
        block_id: &str,
        patch: Map<String, Value>,
        ops: &mut Vec<Value>,
    ) {
        let Some(position) = self.sections[section]
            .context
            .iter()
            .position(|block| block.get("id").and_then(Value::as_str) == Some(block_id))
        else {
            return;
        };

        let block = &mut self.sections[section].context[position];

        let Some(entries) = block.as_object_mut() else {
            return;
        };

        for (key, value) in &patch {
            entries.insert(key.clone(), value.clone());
        }

        ops.push(json!({
            "op": "context:set",
            "turnId": "",
            "sectionId": self.sections[section].id,
            "blockId": block_id,
            "patch": Value::Object(patch),
        }));
    }

    /// Adds one step node and reports it (`pushStep`, `:1215-1239`).
    ///
    /// `attempt_owned` decides whether a later `retry` may remove this step: a tool call IS
    /// attempt output, an actionRequest card is NOT (a retry must not eat a live question).
    fn push_step(
        &mut self,
        section: usize,
        step: Value,
        attempt_owned: bool,
        ops: &mut Vec<Value>,
    ) {
        let id = step
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let kind = step
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("tool")
            .to_owned();

        let node = Node {
            kind,
            id: id.clone(),
            step: Some(step.clone()),
            ..Node::default()
        };

        let section_id = self.sections[section].id.clone();

        self.sections[section].nodes.push(node);
        self.step_count += 1;

        // A step changes ONLY through `step:set`, which REPLACES the whole object
        // (`turn-display.ts:1065-1066`: no field merging, because merging creates ambiguity).
        ops.push(json!({
            "op": "step:set",
            "turnId": "",
            "sectionId": section_id,
            "step": step,
        }));

        if attempt_owned {
            self.track_attempt_node(section, &id);
        }
    }

    /// Patches one step and re-sends the WHOLE descriptor (`setStep`, `:1241-1249`).
    fn patch_step(&mut self, step_id: &str, patch: Map<String, Value>, ops: &mut Vec<Value>) {
        for section in &mut self.sections {
            let Some(position) = section
                .nodes
                .iter()
                .position(|node| node.id == step_id && node.step.is_some())
            else {
                continue;
            };

            let Some(step) = section.nodes[position]
                .step
                .as_mut()
                .and_then(Value::as_object_mut)
            else {
                continue;
            };

            for (key, value) in patch {
                if key == "action" {
                    // An action summary MERGES its named members (`:2484-2488`), because the
                    // answer adds `answered`/`expired` to the options the question minted.
                    if let (Some(existing), Value::Object(incoming)) =
                        (step.get_mut("action").and_then(Value::as_object_mut), value)
                    {
                        for (member, member_value) in incoming {
                            existing.insert(member, member_value);
                        }
                    }
                } else {
                    step.insert(key, value);
                }
            }

            let step = Value::Object(step.clone());

            ops.push(json!({
                "op": "step:set",
                "turnId": "",
                "sectionId": section.id.clone(),
                "step": step,
            }));

            return;
        }
    }

    /// The label one option id carries, for an `answered · <label>` line (`optionLabel`,
    /// `:2601-2612`).
    fn option_label(&self, step_id: &str, option_id: &str) -> Option<String> {
        for section in &self.sections {
            let Some(node) = section.nodes.iter().find(|node| node.id == step_id) else {
                continue;
            };

            let options = node
                .step
                .as_ref()?
                .get("action")?
                .get("options")?
                .as_array()?
                .clone();

            return options
                .iter()
                .find(|option| option.get("id").and_then(Value::as_str) == Some(option_id))
                .and_then(|option| option.get("label").and_then(Value::as_str))
                .map(str::to_owned);
        }

        None
    }

    /// Re-resolves every section's ONE displayed content block and bounds every superseded
    /// prose body (`syncTails` + `resolveTail`, `:1604-1806`).
    ///
    /// ONE rule for every kind of supersession: the surviving body is designated first, then
    /// the older bodies are bounded, in the same frame - which is what stops a live stream and
    /// a cold reload from disagreeing about which block is whole.
    fn sync_tails(&mut self, ops: &mut Vec<Value>) {
        for index in 0..self.sections.len() {
            let designated = resolve_tail(&self.sections[index]);
            let section_id = self.sections[index].id.clone();

            self.sections[index].tail = designated.clone();

            // Snapshot the node facts BEFORE mutating, so the fold cannot read a body it is
            // about to rewrite.
            let decisions: Vec<(String, String, usize, bool)> = self.sections[index]
                .nodes
                .iter()
                .map(|node| {
                    (
                        node.id.clone(),
                        node.kind.clone(),
                        node.chars,
                        node.redacted,
                    )
                })
                .collect();

            for (node_id, kind, chars, was_redacted) in decisions {
                if !matches!(kind.as_str(), "text" | "reasoning") {
                    continue;
                }

                let position = match self.sections[index]
                    .nodes
                    .iter()
                    .position(|node| node.id == node_id)
                {
                    Some(position) => position,
                    None => continue,
                };

                let keeps_tail = designated
                    .as_ref()
                    .and_then(|tail| tail.get("nodeId").and_then(Value::as_str))
                    == Some(node_id.as_str());

                if keeps_tail {
                    if !was_redacted {
                        continue;
                    }

                    // `unredacted: false` states the OPPOSITE in the same addressed patch: this
                    // node's text is the whole recorded body again, because the block that
                    // superseded it was discarded (`turn-display.ts:795-801`).
                    self.sections[index].nodes[position].redacted = false;

                    ops.push(json!({
                        "op": "node:set",
                        "turnId": "",
                        "sectionId": section_id,
                        "nodeId": node_id,
                        "patch": { "unredacted": true, "chars": chars },
                    }));

                    continue;
                }

                if was_redacted {
                    // Still history, still bounded: re-sending the same preview would be
                    // a frame that says nothing.
                    continue;
                }

                let bounded = preview_text_tail(
                    &self.sections[index].nodes[position].text,
                    NODE_PREVIEW_CHARS,
                );

                self.sections[index].nodes[position].redacted = true;

                ops.push(json!({
                    "op": "node:set",
                    "turnId": "",
                    "sectionId": section_id,
                    "nodeId": node_id,
                    "patch": { "text": bounded, "chars": chars, "redacted": true },
                }));
            }

            match designated {
                Some(tail) => ops.push(json!({
                    "op": "tail:set",
                    "turnId": "",
                    "sectionId": section_id,
                    "tail": tail,
                })),
                None => ops.push(json!({
                    "op": "tail:clear",
                    "turnId": "",
                    "sectionId": section_id,
                })),
            }
        }
    }
}

impl Projector for TurnProjector {
    /// Folds one recorded event into the bounded display ops it produced.
    fn apply(
        &mut self,
        event: &HarnessEvent,
        lane: Option<u64>,
        at: u64,
        index: u64,
    ) -> Vec<Value> {
        let mut ops: Vec<Value> = Vec::new();

        // Every segment this event mints takes THIS event's recorded lane, and nothing else can
        // (`turn-projector.ts:1435 laneOfEvent`). Normalized once, here, so a stamped 0 and an absent
        // lane mint the same bytes.
        self.event_lane = crate::persistence::LaneStamp::of(lane);
        let section = self.route(event, &mut ops);

        self.last_applied_at = Some(at);

        match event {
            HarnessEvent::Start { model, .. } => {
                // `start` opens a flow: agent, model, and the streaming state
                // (`:1820-1851`). A section that already settled stays settled.
                self.sections[section].started = true;

                let mut patch = Map::new();

                if !model.is_empty() {
                    self.sections[section].model = Some(model.clone());
                    patch.insert("model".to_owned(), json!(model));
                }

                if !self.sections[section].terminal {
                    self.sections[section].state = "streaming".to_owned();
                    patch.insert("state".to_owned(), json!("streaming"));
                }

                if self.sections[section].started_at.is_none() {
                    self.sections[section].started_at = Some(at);
                    patch.insert("startedAt".to_owned(), json!(at));
                }

                if !patch.is_empty() {
                    ops.push(section_set(&self.sections[section], &Value::Object(patch)));
                }
            }

            HarnessEvent::Text { delta, .. } => {
                self.fold_prose(section, delta, "text", index, at, &mut ops);
            }

            HarnessEvent::Reasoning { delta, .. } => {
                self.fold_prose(section, delta, "reasoning", index, at, &mut ops);
            }

            HarnessEvent::ToolCall { call, .. } => {
                // Text before a call is finished text: the writer detaches so the call is a
                // node of its own and post-tool prose opens a fresh block (`:1973-1979`).
                self.close_writers(section);
                self.begin_attempt(section);

                let step = json!({
                    "kind": "tool",
                    "id": index.to_string(),
                    "turnId": "",
                    "sectionId": self.sections[section].id,
                    "callId": call.id,
                    "name": call.name,
                    // `arguments` is the raw provider JSON string and stays UNPARSED for
                    // display: malformed JSON is the model's problem, not the preview's.
                    "argsPreview": preview_text(&call.arguments.replace(['\r', '\n'], " "), ARGS_PREVIEW_CHARS),
                    "state": "running",
                    "startedAt": at,
                    "detail": {
                        "stepId": index.to_string(),
                        "hasResult": false,
                        "truncated": display_chars(&call.arguments) > ARGS_PREVIEW_CHARS,
                    },
                });

                self.push_step(section, step, true, &mut ops);

                // The bar’s segment for this call (`turn-projector.ts:2043`): the key is the call's
                // OWN event index, so the step row and the bar agree by construction
                // (`turn-display.ts:446-448`). No text travels with the segment - it is metadata,
                // and the payload route reads `[eventFrom..=eventTo]` out of the record on a click.
                let args_chars = display_chars(&call.arguments);
                let section_id = self.sections[section].id.clone();
                let block_id = format!("{}-tool-{index}", self.block_ordinal());

                self.add_block(
                    section,
                    json!({
                        "kind": "tool",
                        "id": block_id,
                        "turnId": "",
                        "sectionId": section_id,
                        "name": call.name,
                        "at": at,
                        // `chars` is the WHOLE recorded length (invariant 5), and while the call is
                        // open that is exactly the argument bytes, because no result exists yet.
                        "chars": args_chars,
                        "argsChars": args_chars,
                        // The arguments travelled with the committed message that asked for the
                        // call, so an open call already counts as prompt text (`:529-541`).
                        "inPrompt": true,
                        "eventFrom": index,
                        "eventTo": index,
                    }),
                    &mut ops,
                );

                self.pending_block = Some((section, block_id));
            }

            HarnessEvent::ToolResult {
                call_id,
                name,
                content,
                is_error,
                outcome,
                ui,
                media,
                duration_ms,
                ..
            } => {
                let outcome = outcome.map(outcome_name);
                let state = step_state_of(outcome, *is_error);
                let settled = self.find_open_step(section, call_id.as_deref(), name);

                if let Some(step_id) = settled {
                    let mut patch = Map::new();

                    patch.insert("state".to_owned(), json!(state));
                    patch.insert("outcome".to_owned(), json!(outcome.unwrap_or("completed")));
                    patch.insert(
                        "preview".to_owned(),
                        json!(preview_text(content, PREVIEW_CHARS)),
                    );

                    if let Some(duration) = duration_ms {
                        patch.insert("durationMs".to_owned(), json!(duration));
                    }

                    patch.insert("endedAt".to_owned(), json!(at));

                    if let Some(panels) = summarize_panels(ui) {
                        patch.insert("panels".to_owned(), panels);
                    }

                    if let Some(entries) = summarize_media(media) {
                        patch.insert("media".to_owned(), entries);
                    }

                    // The step's detail reference is what the lazy step route answers from
                    // (`DisplayStepDetailRef`, `turn-display.ts:660-671`).
                    patch.insert(
                        "detail".to_owned(),
                        json!({
                            "stepId": step_id,
                            "resultStepId": index.to_string(),
                            "hasResult": true,
                            "truncated": display_chars(content) > PREVIEW_CHARS,
                        }),
                    );

                    self.patch_step(&step_id, patch, &mut ops);

                    // The SAME segment, settled: the result's length joins `chars`, and `state`,
                    // `endedAt` and the measured span come straight from the record - never a
                    // re-derivation (`turn-display.ts:489-545`). One block per call, so a result
                    // patches the row its call opened instead of adding a second one.
                    let settling = self
                        .pending_block
                        .take()
                        .filter(|(block_section, _)| *block_section == section);

                    // The pending entry is CONSUMED either way: a segment opened in another flow
                    // is not this result's to settle, and it must not linger to settle the wrong
                    // one later. The shadow is deliberate - the filter proved the two are one.
                    if let Some((section, block_id)) = settling {
                        let prior = self.sections[section].context.iter().find(|block| {
                            block.get("id").and_then(Value::as_str) == Some(block_id.as_str())
                        });

                        let args_chars = prior
                            .and_then(|block| block.get("argsChars").and_then(Value::as_u64))
                            .unwrap_or(0);

                        let restamped = prior
                            .and_then(|block| block.get("name").and_then(Value::as_str))
                            .is_some_and(|recorded| recorded != name.as_str());

                        let result_chars = display_chars(content);

                        let mut block_patch = Map::new();

                        block_patch.insert(
                            "chars".to_owned(),
                            json!(args_chars + u64::try_from(result_chars).unwrap_or(u64::MAX)),
                        );
                        block_patch.insert("resultChars".to_owned(), json!(result_chars));
                        block_patch.insert("state".to_owned(), json!(state));
                        block_patch.insert("endedAt".to_owned(), json!(at));
                        block_patch.insert("eventTo".to_owned(), json!(index));

                        // A measured span only when the runtime recorded one: an immediate
                        // validation failure executed nothing (`contract.rs:784-787`).
                        if let Some(duration) = duration_ms {
                            block_patch.insert("durationMs".to_owned(), json!(duration));
                        }

                        // `name` is re-stamped ONLY when the recorded name changed
                        // (`turn-display.ts:478-480`), which an out-of-order pair can do.
                        if restamped {
                            block_patch.insert("name".to_owned(), json!(name));
                        }

                        self.patch_block(section, &block_id, block_patch, &mut ops);
                    }
                } else {
                    // A genuine result-only frame (a missed `tool_call`): show it rather than
                    // dropping information (`:2145-2202`). This frame is not attempt-owned:
                    // its own result is the opening evidence.
                    let step = json!({
                        "kind": "tool",
                        "id": index.to_string(),
                        "turnId": "",
                        "sectionId": self.sections[section].id,
                        "callId": call_id,
                        "name": name,
                        "preview": preview_text(content, PREVIEW_CHARS),
                        "state": state,
                        "outcome": outcome,
                        "durationMs": duration_ms,
                        "startedAt": at,
                        "endedAt": at,
                        "detail": {
                            "stepId": index.to_string(),
                            "hasResult": false,
                            "truncated": display_chars(content) > PREVIEW_CHARS,
                        },
                    });

                    self.push_step(section, step, false, &mut ops);
                }
            }

            HarnessEvent::Usage { usage, .. } => {
                // `latestInput` is the LATEST request's input, `cachedInput` the turn-wide
                // cached total, mirrored onto the section AND published twice: once for this
                // invocation's line, once for the turn tile's line (`:2229-2263`).
                {
                    let section_state = &mut self.sections[section];

                    section_state.latest_input = Some(usage.input);

                    if let Some(cached) = usage.cached {
                        section_state.cached_reported = true;
                        section_state.cached_input =
                            Some(section_state.cached_input.unwrap_or(0) + cached);
                    }
                }

                let section_state = &self.sections[section];
                let section_id = section_state.id.clone();
                let latest_input = section_state.latest_input;
                let cached_input = section_state
                    .cached_reported
                    .then(|| section_state.cached_input.unwrap_or(0));

                self.turn_latest_input = latest_input;
                self.turn_cached_input = match (self.turn_cached_input, usage.cached) {
                    (Some(prior), Some(next)) => Some(prior + next),
                    (None, Some(next)) => Some(next),
                    (prior, None) => prior,
                };

                // The running tally is the STORE's canonical fold, reused rather than
                // re-derived: `cached` stays absent unless BOTH sides reported one, and a first
                // frame that DOES report one is kept (`turns.rs:1589-1614` `addUsage`, whose
                // `(None, next)` arm a two-arm match silently drops).
                self.usage = Some(crate::store::turns::add_usage(self.usage.as_ref(), usage));

                let mut section_op =
                    json!({ "op": "usage", "turnId": "", "sectionId": section_id });

                if let Some(value) = latest_input {
                    section_op["latestInput"] = json!(value);
                }

                if let Some(value) = cached_input {
                    section_op["cachedInput"] = json!(value);
                }

                let mut turn_op = json!({ "op": "usage", "turnId": "", "sectionId": "turn" });

                if let Some(value) = self.turn_latest_input {
                    turn_op["latestInput"] = json!(value);
                }

                if let Some(usage) = self.usage.clone() {
                    turn_op["usage"] = serde_json::to_value(usage).unwrap_or(Value::Null);
                }

                ops.push(section_op);
                ops.push(turn_op);
            }

            HarnessEvent::Done { message, .. } => {
                // `done` closes one AGENT turn, not the user's: the root keeps streaming until
                // the turn settles, and a response still waiting on background work has not
                // finished at all (`:2307-2321`).
                self.commit_attempt(section, &mut ops);

                // The bar's answer segment. TypeScript mints an `assistant` block per WINDOW of
                // streamed `text` deltas and falls back to the `done` message ONLY when that
                // window holds no deltas - the two carry the same bytes, and counting both would
                // double the answer (`turn-display.ts:373-376`). This fold does not open a bar
                // segment per prose window (the deltas are folded into nodes; a segment per window
                // would need a window-scoped id and a patch on every delta), so the fallback IS
                // the answer segment here, keyed by the `done` event's own index exactly as
                // TypeScript keys that same case.
                //
                // Only the ROOT flow gets one: a nested flow's answer is already a row in its
                // parent's transcript (`:2380`). A pending set is never consulted, because this
                // engine runs a tool round to completion before the next request - the module
                // header says why.
                let streamed = self.sections[section]
                    .nodes
                    .iter()
                    .any(|node| node.kind == "text");

                if self.sections[section].key == ROOT_KEY
                    && !streamed
                    && !message.content.is_empty()
                {
                    let answer_chars = display_chars(&message.content);
                    let section_id = self.sections[section].id.clone();

                    self.add_block(
                        section,
                        json!({
                            "kind": "assistant",
                            "id": format!("{}-assistant-{index}", self.block_ordinal()),
                            "turnId": "",
                            "sectionId": section_id,
                            "name": "answer",
                            "at": at,
                            "chars": answer_chars,
                            // A committed answer IS prompt text for the requests that follow
                            // (`turn-display.ts:529-541`).
                            "inPrompt": true,
                            "eventFrom": index,
                            "eventTo": index,
                        }),
                        &mut ops,
                    );
                }

                let mut patch = Map::new();

                if self.sections[section].key != ROOT_KEY {
                    self.sections[section].terminal = true;
                    self.sections[section].state = "done".to_owned();
                    patch.insert("state".to_owned(), json!("done"));

                    patch.insert("endedAt".to_owned(), json!(at));
                }

                if self.sections[section].model.is_none() && !message.model.is_empty() {
                    self.sections[section].model = Some(message.model.clone());
                    patch.insert("model".to_owned(), json!(&message.model));
                }

                if !patch.is_empty() {
                    ops.push(section_set(&self.sections[section], &Value::Object(patch)));
                }
            }

            HarnessEvent::Retry { .. } => {
                // R16a: discarding the failed attempt's output is the ONLY thing this handler
                // does, in both directions of the one rule (live `apply` and a cold replay), so
                // a reload can never disagree with the stream.
                self.discard_attempt(section, &mut ops);
            }

            HarnessEvent::AttemptCommit { .. } => {
                self.commit_attempt(section, &mut ops);
            }

            HarnessEvent::Wakeup { pending, .. } => {
                self.close_writers(section);

                let text = if pending.is_empty() {
                    "pending tools finished · continuing".to_owned()
                } else {
                    format!(
                        "{} tool{} still running · continuing",
                        pending.len(),
                        if pending.len() == 1 { "" } else { "s" }
                    )
                };

                self.insert_notice(
                    section,
                    "wake",
                    &text,
                    &format!("n{index}"),
                    Some(at),
                    None,
                    &mut ops,
                );
            }

            HarnessEvent::Error { error, .. } => {
                self.close_writers(section);

                // The error line is server status, so it does NOT take the tail and does not
                // bound the block behind it: a reader who lost an answer to a failed request
                // still sees that answer beside the notice (R23).
                self.last_error = Some(preview_text(&error.message, ERROR_CHARS));
                self.insert_notice(
                    section,
                    "error",
                    &error.message,
                    &format!("n{index}"),
                    Some(at),
                    None,
                    &mut ops,
                );

                // A child that reported its own failure is terminal; the root's state belongs
                // to the turn (`:2409-2414`).
                if self.sections[section].key != ROOT_KEY {
                    self.sections[section].terminal = true;

                    ops.push(section_set(
                        &self.sections[section],
                        &json!({ "state": "error", "endedAt": at }),
                    ));
                }
            }

            HarnessEvent::ActionRequest {
                call_id, request, ..
            } => {
                // An action card is minted OUTSIDE the attempt window: a retry must not eat a
                // live question (`:1211-1214`).
                self.close_writers(section);

                let options: Vec<Value> = request
                    .options
                    .iter()
                    .map(|option| {
                        let mut entry = json!({
                            "id": option.id,
                            "label": preview_text(&option.label, NOTICE_CHARS),
                        });

                        if let Some(description) = option
                            .description
                            .as_deref()
                            .filter(|text| !text.is_empty())
                        {
                            entry["description"] = json!(preview_text(description, NOTICE_CHARS));
                        }

                        entry
                    })
                    .collect();

                let step = json!({
                    "kind": "action",
                    "id": index.to_string(),
                    "turnId": "",
                    "sectionId": self.sections[section].id,
                    "callId": call_id,
                    "name": preview_text(&request.question, PREVIEW_CHARS),
                    "state": "running",
                    "startedAt": at,
                    "action": {
                        "requestId": request.request_id,
                        "question": preview_text(&request.question, NOTICE_CHARS),
                        "options": options,
                        "recommended": request.recommended,
                        "allowCustom": true,
                        // The SERVER knows what is answerable; a restored request is expired
                        // from the start (`sessions.ts:679`). A live fold never saw one.
                        "expired": false,
                    },
                    "detail": { "stepId": index.to_string(), "hasResult": false, "truncated": false },
                });

                self.action_steps
                    .insert(request.request_id.clone(), index.to_string());
                self.push_step(section, step, false, &mut ops);
            }

            HarnessEvent::ActionAnswer {
                request_id, answer, ..
            } => {
                let Some(step_id) = self.action_steps.get(request_id).cloned() else {
                    // An answer for a question this fold never saw renders nothing.
                    self.sync_tails(&mut ops);
                    return ops;
                };

                let choice_text = match &answer.choice {
                    HarnessActionChoice::Option { option_id } => self
                        .option_label(&step_id, option_id)
                        .unwrap_or_else(|| option_id.clone()),
                    HarnessActionChoice::Custom { text } => text.clone(),
                };

                let mut patch = Map::new();

                patch.insert("state".to_owned(), json!("ok"));
                patch.insert("endedAt".to_owned(), json!(at));
                patch.insert(
                    "action".to_owned(),
                    json!({
                        "answered": {
                            "choice": answer.choice,
                            "source": answer.source,
                            "at": at,
                        },
                        "expired": false,
                    }),
                );
                patch.insert(
                    "detail".to_owned(),
                    json!({ "stepId": step_id, "resultStepId": index.to_string(), "hasResult": true }),
                );

                self.patch_step(&step_id, patch, &mut ops);
                self.insert_notice(
                    section,
                    "answer",
                    &format!("answered · {choice_text}"),
                    &format!("n{index}"),
                    Some(at),
                    Some(&step_id),
                    &mut ops,
                );
            }

            HarnessEvent::BranchStart { branch, .. } => {
                // A branch is its own flow, keyed by the BRANCH id and nested under its
                // requester when the summary names one (`:2507-2548`).
                let Some(branch_id) = branch
                    .get("branchId")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_owned())
                else {
                    self.sync_tails(&mut ops);
                    return ops;
                };

                if let Some(index) = self
                    .sections
                    .iter()
                    .position(|section| section.branch_id.as_deref() == Some(branch_id.as_str()))
                {
                    if !self.sections[index].terminal {
                        self.sections[index].state = "streaming".to_owned();

                        ops.push(section_set(
                            &self.sections[index],
                            &json!({ "state": "streaming" }),
                        ));
                    }

                    self.sync_tails(&mut ops);
                    return ops;
                }

                let agent = branch
                    .get("agent")
                    .and_then(Value::as_str)
                    .unwrap_or(event_agent(event))
                    .to_owned();

                let parent = branch
                    .get("sourceThreadId")
                    .and_then(Value::as_str)
                    .and_then(|source| {
                        self.sections
                            .iter()
                            .position(|section| section.invocation_id.as_deref() == Some(source))
                    })
                    .map(|index| self.sections[index].id.clone());

                let mut minted = self.mint_section(&branch_id, &agent, parent);

                minted.branch_id = Some(branch_id.clone());
                minted.state = "streaming".to_owned();
                minted.started = true;
                minted.started_at = Some(at);

                if let Some(model) = branch.get("model").and_then(Value::as_str) {
                    minted.model = Some(model.to_owned());
                }

                // A branch owns no persistent conversation; a frame that DOES name one is
                // direct evidence, so it is kept (`:846-852`).
                if let Some(thread_id) = branch.get("threadId").and_then(Value::as_str) {
                    let _ = thread_id;
                }

                if branch.get("role").and_then(Value::as_str) == Some("primary")
                    && branch
                        .get("sourceThreadId")
                        .and_then(Value::as_str)
                        .is_none()
                {
                    minted.role = "primary".to_owned();
                }

                self.sections.push(minted);
                let created = self.sections.len() - 1;

                open_section(&self.sections[created], &mut ops);
            }

            HarnessEvent::BranchEnd {
                branch_id,
                status,
                error,
                ..
            } => {
                let target = self
                    .sections
                    .iter()
                    .position(|section| section.branch_id.as_deref() == Some(branch_id.as_str()));

                let Some(target) = target else {
                    self.sync_tails(&mut ops);
                    return ops;
                };

                self.sections[target].terminal = true;
                self.sections[target].state = match status {
                    BranchTerminalStatus::Completed => "done",
                    BranchTerminalStatus::Cancelled => "cancelled",
                    BranchTerminalStatus::Failed => "error",
                }
                .to_owned();

                ops.push(section_set(
                    &self.sections[target],
                    &json!({ "state": self.sections[target].state, "endedAt": at }),
                ));

                if let Some(error) = error {
                    self.insert_notice(
                        target,
                        "error",
                        &error.message,
                        &format!("n{index}"),
                        Some(at),
                        None,
                        &mut ops,
                    );
                }
            }
        }

        // The frame-end tail pass: the newest eligible block is designated, every superseded
        // prose body is bounded, in this one place (`:1759-1806`).
        self.sync_tails(&mut ops);

        ops
    }

    /// The compact row (`summaryOf`, `:2826-2870`).
    ///
    /// TypeScript memoizes this read; this port recomputes it, because the memo lives behind
    /// `&self` in the [`Projector`] seam and `unsafe_code` is forbidden in this crate. The row
    /// is small and it is read once per published frame.
    fn summary(&self) -> Value {
        let status = self.status();
        let terminal = is_terminal_status(&status);
        let mut active: Vec<String> = Vec::new();

        for section in &self.sections {
            if section.terminal || !section.started {
                continue;
            }

            if !active.contains(&section.agent) {
                active.push(section.agent.clone());
            }
        }

        // The settled status line: a recorded failure wins, a DONE turn shows the model that
        // answered, and a cancelled turn with no message states nothing rather than the model
        // of a run that never answered (`:2837-2841`).
        let detail = self.last_error.clone().or_else(|| {
            if status == "done" {
                self.root_model()
            } else {
                None
            }
        });

        // The record-only members (`id`, `index`, `prompt`, `threadId`, attachments,
        // references, steers) are stated as the seam allows - see the module header.
        let mut row = json!({
            "id": "",
            "index": 0,
            "prompt": "",
            "status": status,
            "state": turn_state_of(&status),
            "updatedAt": self.updated_at(),
            // TS counts NON-placeholder sections; this fold mints no delegation placeholder
            // (no `delegate` runtime), so every section it made is visible.
            "sectionCount": self.sections.len(),
            "stepCount": self.step_count,
            "activeAgents": if terminal { Vec::<String>::new() } else { active },
            "blocks": Vec::<Value>::new(),
        });

        if let Some(started_at) = self.sections.iter().find_map(|section| section.started_at) {
            row["startedAt"] = json!(started_at);
        }

        if let Some(usage) = self.usage.clone() {
            row["usage"] = serde_json::to_value(usage).unwrap_or(Value::Null);
        }

        if let Some(value) = self.turn_latest_input {
            row["latestInput"] = json!(value);
        }

        if let Some(value) = self.turn_cached_input {
            row["cachedInput"] = json!(value);
        }

        if let Some(value) = detail.filter(|text| !text.is_empty()) {
            row["detail"] = json!(value);
        }

        row
    }

    /// The compiled display tree (`display`, `:2743-2765`).
    fn display(&self) -> Value {
        let sections: Vec<Value> = self.sections.iter().map(wire_section).collect();

        let mut display = json!({ "turnId": "", "sections": sections });

        if self.section_ordinal > 0 {
            display["lastSectionOrdinal"] = json!(self.section_ordinal - 1);
        }

        display
    }

    /// Closes the fold at a terminal status (`settle`, `:2640-2735`).
    ///
    /// The Rust [`Projector::settle`] seam returns nothing, so the ops TypeScript emits here
    /// (`section:set`, the `step:set` for an abandoned call, and the final `turn` frame) are
    /// not published; the store re-reads `summary()` and `display()` immediately after, which
    /// is how the same state becomes observable. This method therefore only MUTATES: no flow
    /// stays `streaming`, and no step stays `running`.
    fn settle(&mut self, status: &str, at: u64) {
        self.settled_status = Some(status.to_owned());
        self.settled_at = Some(at);
        self.last_applied_at = Some(at);

        let section_state = section_state_of(status);
        let abandoned = abandoned_state_of(status);

        for section in &mut self.sections {
            if section.terminal {
                continue;
            }

            section.terminal = true;
            section.state = section_state.to_owned();

            if section.started_at.is_none() {
                section.started_at = Some(at);
            }

            section.ended_at = Some(at);
        }

        for section in &mut self.sections {
            for node in &mut section.nodes {
                let Some(step) = node.step.as_mut() else {
                    continue;
                };

                if step.get("state").and_then(Value::as_str) != Some("running") {
                    continue;
                }

                step["state"] = json!(abandoned);
                step["endedAt"] = json!(at);

                // A call the turn outlived says WHY it has no result, instead of pretending an
                // empty one (`:2686-2695`). No duration is invented for a call that never
                // reported one.
                let no_preview = step
                    .get("preview")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty);

                if no_preview {
                    step["preview"] = json!(preview_text(
                        if status == "cancelled" {
                            "the turn was cancelled before this call returned"
                        } else {
                            "the turn ended before this call returned"
                        },
                        PREVIEW_CHARS
                    ));
                }
            }
        }
    }
}

impl TurnProjector {
    /// Streams one prose delta into the section's current writer, minting a node the first
    /// time (`:1853-1965`).
    fn fold_prose(
        &mut self,
        section: usize,
        delta: &str,
        kind: &str,
        index: u64,
        at: u64,
        ops: &mut Vec<Value>,
    ) {
        if delta.is_empty() {
            // An empty delta is recorded and renders nothing (`:1854`, `:1907`).
            return;
        }

        self.begin_attempt(section);

        let existing = if kind == "text" {
            self.sections[section].text_writer
        } else {
            self.sections[section].reasoning_writer
        };

        let position = match existing {
            Some(position) => position,
            None => {
                let node = Node {
                    kind: kind.to_owned(),
                    id: format!("n{index}"),
                    ..Node::default()
                };

                let section_state = &mut self.sections[section];
                let position = section_state.nodes.len();

                section_state.nodes.push(node);

                if kind == "text" {
                    section_state.text_writer = Some(position);
                } else {
                    section_state.reasoning_writer = Some(position);
                }

                let _ = at;

                ops.push(json!({
                    "op": "node:insert",
                    "turnId": "",
                    "sectionId": section_state.id.clone(),
                    // A freshly minted node is the section's newest block, so its wire copy is
                    // whole (`insertNode`, `:1195-1209`).
                    "node": wire_node(section_state, position),
                }));

                self.track_attempt_node(section, &format!("n{index}"));

                position
            }
        };

        let node = &mut self.sections[section].nodes[position];

        node.text.push_str(delta);
        node.chars = display_chars(&node.text);

        let node_id = node.id.clone();

        // A current answer or thinking window streams in FULL: one append per delta, and the
        // exact running length travels beside it (invariant 5).
        ops.push(json!({
            "op": "node:append",
            "turnId": "",
            "sectionId": self.sections[section].id,
            "nodeId": node_id,
            "delta": delta,
        }));
    }

    /// Finds the step one `tool_result` settles, pairing by call id and only then by name, and
    /// preferring this section's own open call (`:2066-2084`).
    fn find_open_step(&self, section: usize, call_id: Option<&str>, name: &str) -> Option<String> {
        let matches = |node: &Node| -> bool {
            let Some(step) = node.step.as_ref() else {
                return false;
            };

            if step.get("state").and_then(Value::as_str) != Some("running") {
                return false;
            }

            match call_id {
                Some(call_id) => step.get("callId").and_then(Value::as_str) == Some(call_id),
                None => step.get("name").and_then(Value::as_str) == Some(name),
            }
        };

        if let Some(node) = self.sections[section]
            .nodes
            .iter()
            .find(|node| matches(node))
        {
            return Some(node.id.clone());
        }

        // A result whose call was opened in another section still settles that row: the id is
        // the identity, and the fallback is turn-wide (`:2071`).
        self.sections
            .iter()
            .flat_map(|section| section.nodes.iter())
            .find(|node| matches(node))
            .map(|node| node.id.clone())
    }
}

/// `resolveTail` (`:1604-1672`), condensed to the rule the wire depends on: the newest
/// ELIGIBLE block of the section is its one body. `user`/`system` are excluded by rule, an
/// `action` step is an interactive question with its own card, and a `notice` is server
/// status - none of them may erase the block a reader is looking at (`:832-837`).
fn resolve_tail(section: &Section) -> Option<Value> {
    let node = section
        .nodes
        .iter()
        .rev()
        .find(|node| matches!(node.kind.as_str(), "text" | "reasoning" | "tool"))?;

    match node.kind.as_str() {
        "tool" => {
            let step = node.step.as_ref()?;
            let args = step
                .get("argsPreview")
                .and_then(Value::as_str)
                .unwrap_or("");
            let result = step.get("preview").and_then(Value::as_str);

            // A designated payload is the exact recorded bytes with no display-side cap
            // (T25, R23). This fold holds the recorded arguments in `argsPreview` as a
            // preview, and a settled result in `preview`; the full bytes stay on the step
            // detail route.
            let mut tail = json!({
                "kind": "tool",
                "nodeId": node.id,
                "chars": display_chars(args) + result.map_or(0, display_chars),
                "truncated": true,
            });

            if !args.is_empty() {
                tail["args"] = json!(args);
            }

            if let Some(result) = result {
                tail["result"] = json!(result);
            }

            Some(tail)
        }
        "reasoning" => Some(json!({
            "kind": "reasoning",
            "nodeId": node.id,
            "chars": node.chars,
            "truncated": false,
        })),
        _ => Some(json!({
            "kind": "assistant",
            "nodeId": node.id,
            "chars": node.chars,
            "truncated": false,
        })),
    }
}

/// The wire copy of one node (`wireNodeOf`, `:1673-1758`): the designated tail keeps its whole
/// body, everything else arrives bounded with its exact length and `redacted: true`.
fn wire_node(section: &Section, position: usize) -> Value {
    let node = &section.nodes[position];

    match node.kind.as_str() {
        "text" | "reasoning" => {
            let designated = section
                .tail
                .as_ref()
                .and_then(|tail| tail.get("nodeId").and_then(Value::as_str))
                == Some(node.id.as_str());

            let mut entry = json!({
                "kind": node.kind,
                "id": node.id,
                "text": if designated {
                    node.text.clone()
                } else {
                    preview_text_tail(&node.text, NODE_PREVIEW_CHARS)
                },
                "chars": node.chars,
            });

            if node.kind == "text" {
                entry["committed"] = json!(node.committed);
            }

            if !designated {
                entry["redacted"] = json!(true);
            }

            entry
        }
        "notice" => {
            let mut entry = json!({
                "kind": "notice",
                "id": node.id,
                "tone": node.tone,
                "text": node.text,
            });

            if let Some(at) = node.at {
                entry["at"] = json!(at);
            }

            if let Some(step_id) = &node.step_id {
                entry["stepId"] = json!(step_id);
            }

            entry
        }
        other => node
            .step
            .clone()
            .unwrap_or(json!({ "kind": other, "id": node.id })),
    }
}

/// The wire copy of one section, in display order (`:2760-2764`).
fn wire_section(section: &Section) -> Value {
    let mut entry = Map::new();

    entry.insert("id".to_owned(), json!(&section.id));

    if let Some(parent) = &section.parent_section_id {
        entry.insert("parentSectionId".to_owned(), json!(parent));
    }

    entry.insert("role".to_owned(), json!(&section.role));
    entry.insert("agent".to_owned(), json!(&section.agent));
    // Labels arrive with the store's block projection and the invocation-thread join, neither
    // of which `create()` can see; an empty label is the honest absence (`:704`).
    entry.insert("agentLabel".to_owned(), json!(""));
    entry.insert("state".to_owned(), json!(&section.state));

    if let Some(model) = &section.model {
        entry.insert("model".to_owned(), json!(model));
    }

    if let Some(invocation) = &section.invocation_id {
        entry.insert("invocationId".to_owned(), json!(invocation));
    }

    if let Some(branch) = &section.branch_id {
        entry.insert("branchId".to_owned(), json!(branch));
    }

    if let Some(started_at) = section.started_at {
        entry.insert("startedAt".to_owned(), json!(started_at));
    }

    if let Some(ended_at) = section.ended_at {
        entry.insert("endedAt".to_owned(), json!(ended_at));
    }

    if let Some(value) = section.latest_input {
        entry.insert("latestInput".to_owned(), json!(value));
    }

    if section.cached_reported {
        entry.insert(
            "cachedInput".to_owned(),
            json!(section.cached_input.unwrap_or(0)),
        );
    }

    if let Some(value) = &section.answer_node_id {
        entry.insert("answerNodeId".to_owned(), json!(value));
    }

    if let Some(tail) = &section.tail {
        entry.insert("tail".to_owned(), tail.clone());
    }

    let nodes: Vec<Value> = (0..section.nodes.len())
        .map(|position| wire_node(section, position))
        .collect();

    // The bar rides the row whether or not it has entries, because TypeScript’s
    // `createBaseSection` always sets `context: []` (`:700-712`); a client may map over it
    // without a presence check.
    entry.insert("context".to_owned(), Value::Array(section.context.clone()));

    entry.insert("nodes".to_owned(), Value::Array(nodes));

    Value::Object(entry)
}

/// One `section:set` op: patches the fields it names and leaves the rest (`:743-751`).
fn section_set(section: &Section, patch: &Value) -> Value {
    json!({
        "op": "section:set",
        "turnId": "",
        "sectionId": section.id,
        "patch": patch,
    })
}

/// One `section:open` op: the section WITHOUT its nodes, which always arrive separately
/// (`:738-741`).
fn open_section(section: &Section, ops: &mut Vec<Value>) {
    let mut wire = wire_section(section);

    wire["nodes"] = json!([]);

    ops.push(json!({ "op": "section:open", "turnId": "", "section": wire }));
}

/// `summarizePanels` (`:383-457`): the compact descriptors a step row carries. Capped, and
/// when extra panels exist the FIRST summary is marked truncated too, so a renderer cannot
/// show four panels and claim that was all of them.
fn summarize_panels(panels: &[ToolUiPanel]) -> Option<Value> {
    if panels.is_empty() {
        return None;
    }

    let total = panels.len();
    let mut kept: Vec<Value> = panels
        .iter()
        .take(PANELS_PER_STEP)
        .map(|panel| {
            let blocks: Vec<Value> = panel
                .blocks
                .iter()
                .map(|block| match block {
                    ToolUiBlock::Text { text } => json!({
                        "type": "text",
                        "preview": preview_text(text, PANEL_PREVIEW_CHARS),
                        "length": display_chars(text),
                    }),
                    ToolUiBlock::Table { columns, rows } => {
                        json!({ "type": "table", "rows": rows.len(), "columns": columns.len() })
                    }
                    ToolUiBlock::KeyValue { items } => {
                        json!({ "type": "keyValue", "items": items.len() })
                    }
                })
                .collect();

            json!({
                "id": panel.id,
                "slot": panel.slot,
                "title": preview_text(&panel.title, PANEL_TITLE_CHARS),
                "blocks": blocks,
                "blockCount": panel.blocks.len(),
            })
        })
        .collect();

    if total > PANELS_PER_STEP {
        kept[0]["truncated"] = json!(true);
    }

    Some(json!({ "panels": kept, "count": total }))
}

/// `summarizeMedia` (`:458-473`): descriptors only, never bytes, because every event is JSON
/// round-tripped and persisted.
fn summarize_media(media: &[ToolMediaDescriptor]) -> Option<Value> {
    if media.is_empty() {
        return None;
    }

    let total = media.len();
    let mut kept: Vec<Value> = media
        .iter()
        .take(MEDIA_PER_STEP)
        .map(|entry| {
            json!({
                "id": entry.id,
                "kind": entry.kind,
                "name": entry.name,
                "mime": entry.mime,
                "size": entry.size,
            })
        })
        .collect();

    if total > MEDIA_PER_STEP {
        kept[0]["truncated"] = json!(true);
    }

    Some(json!({ "media": kept, "count": total }))
}

/// Builds one projector per turn (`createTurnProjector`, `turn-projector.ts:620`): the fold is
/// per-turn state, not a shared service.
#[derive(Clone, Copy, Debug, Default)]
pub struct TurnProjectorFactory;

impl TurnProjectorFactory {
    /// A shared factory, the shape `Store::open` wants.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl ProjectorFactory for TurnProjectorFactory {
    fn create(&self, seed: &ProjectorSeed) -> Box<dyn Projector> {
        Box::new(TurnProjector::seeded(seed.clone()))
    }
}

// ---------------------------------------------------------------------------
// The wire identity the seam cannot mint (`knowledge.md` "The projector seam's limits")
//
// `TurnProjector` has NO turn record (`ProjectorFactory::create()` takes none), so its own section
// keys are `s{ordinal}` and every op says `turnId: ""`. The wire identity is `${turnId}:${ordinal}`
// (`turn-projector.ts:698`), and the ordinal is exactly the position `mint_section` allocated — the
// same fact `display()`'s section array states by its order.
//
// BOTH readers therefore run through the functions below: the LIVE `display.ops` frames the store
// publishes (`store::TurnState::record_event`) and the REST display document
// (`store::read::TurnState::display`). Before this existed, only the REST path re-stamped, so a
// listener that applied the ops it streamed and then read the snapshot saw two different sets of
// section ids and had to rebuild the tree to reconcile them.
// ---------------------------------------------------------------------------

/// The wire id of one projector section (`turn-projector.ts:698`).
///
/// An id this fold did not mint (`"turn"`, a `{ordinal}-{kind}-{key}` block id, a `{turnId}:{n}` that
/// is already stamped, a node id `n{index}`) is returned unchanged, which makes the rewrite idempotent.
#[must_use]
pub fn section_wire_id(turn_id: &str, id: &str) -> String {
    let Some(ordinal) = id.strip_prefix('s') else {
        return id.to_owned();
    };

    if ordinal.is_empty() || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
        return id.to_owned();
    }

    format!("{turn_id}:{ordinal}")
}

/// Rewrites one value in place: every `turnId` string field gets the turn id, and every string that IS
/// a projector section key becomes its wire id — at any depth, because an op embeds the section payload
/// (`section:open`), a section patch, a step copy and a tail descriptor, and each of those names the
/// section under its own key (`sectionId`, `parentSectionId`, `id`, ...).
///
/// The "value is a section key" test is the shape, not the key name: only `s{digits}` matches, so a
/// prose string that happens to read "s1" is the only content that could ever be rewritten, and the
/// fold never puts prose in an id position.
pub fn stamp_turn_identity(turn_id: &str, value: &mut Value) {
    match value {
        Value::Object(entries) => {
            for (key, entry) in entries.iter_mut() {
                if let Some(text) = entry.as_str() {
                    if key == "turnId" {
                        *entry = Value::String(turn_id.to_owned());
                    } else {
                        let stamped = section_wire_id(turn_id, text);

                        if stamped != text {
                            *entry = Value::String(stamped);
                        }
                    }

                    continue;
                }

                stamp_turn_identity(turn_id, entry);
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                stamp_turn_identity(turn_id, item);
            }
        }
        _ => {}
    }
}

/// Stamps every op of one turn with the identity the wire carries (`sectionWireId` + `turnId`,
/// `turn-projector.ts:698`/`:1982-1990`).
///
/// The store mints ops in exactly one place — `TurnState::record_event` — so stamping here is what
/// makes a streamed `display.ops` section id EQUAL the id `GET /sessions/:id/turns/:turn/display`
/// reports for the same section, which is the point of the compact board channel.
#[must_use]
pub fn stamp_display_ops(turn_id: &str, ops: Vec<Value>) -> Vec<Value> {
    if turn_id.is_empty() {
        return ops;
    }

    let mut stamped = ops;

    for op in &mut stamped {
        stamp_turn_identity(turn_id, op);
    }

    stamped
}
