//! The bounded read models of the session domain.
//!
//! TypeScript origin: `packages/underload/harness/src/server/api-reads.ts`, function by function.
//! Every derivation below quotes the TS line it comes from, because these rows ARE the contract:
//! a stream frame's payload, a page's items, and a receipt's counts are all built here, so a field
//! that drifts here drifts on the wire.
//!
//! WHY `Value` AND WHY TYPED SIDE BY SIDE
//! --------------------------------------
//! Every projection returns the `crate::contract` struct it documents (a real type, so a later
//! stage cannot send a misspelled member), and the STORE serializes it at the publish/return site.
//! The two exceptions are the compact turn row and the display document, which TS forwards as
//! VERBATIM projector output (`turnRowOf`, `:276-296`: a cast, never a restate) - those are JSON
//! values here too, because the projector (stage T4) owns their shape.
//!
//! The bounds are TS's, restated here so a preview length is one line away from the row that uses
//! it (`api-reads.ts:95-102`).

use std::collections::BTreeMap;

use serde_json::Value;

use crate::agents::AgentCatalogue;
use crate::config::{HarnessRuntimeSettings, ServerConfig};
use crate::contract::{
    self, AgentResultStatus, AgentRole, BindingOrigin, CommandDescriptor, HarnessErrorInfo,
    HarnessEvent, HarnessMessage, HarnessUsage, InstructionSource, PendingAction, ProviderConfig,
    QueuedTask, ResourceBudget, SessionAgentBinding, SessionDefinitionResource,
    SessionInstructionSource, SessionStatePatch, SessionStatus, SessionSummary, SessionToolSummary,
    ThreadLifecycle, ThreadOrigin, ThreadResource, ThreadSummary, TranscriptMetadata,
    TranscriptRoleCounts, TranscriptSignal, TranscriptState, TurnEventRow, TurnResource,
    UsageByThread, UsagePatch,
};
use crate::events::{EventHub, now_millis};
use crate::registry::HarnessRegistry;
use crate::store::{
    SessionState, TurnState, created_tool_value, parse_status, parse_turn_status, thread_key,
    turn_key,
};

/// A queued task's row preview (`api-reads.ts:96` `QUEUE_PREVIEW_CHARS`). A queue panel of 100 x
/// 200k-character prompts must stay small.
pub const QUEUE_PREVIEW_CHARS: usize = 160;

/// A roster row's transcript tail (`:97` `THREAD_PREVIEW_CHARS`). The verbatim text is the
/// transcript route's job.
pub const THREAD_PREVIEW_CHARS: usize = 160;

/// `TurnResource.prompt` is the verbatim task text; a ROW carries this bounded preview instead
/// (`:102` `TURN_PROMPT_PREVIEW_CHARS`).
pub const TURN_PROMPT_PREVIEW_CHARS: usize = 320;

/// Cap on the invocation ids one roster row reports (`:573` `MAX_ROSTER_INVOCATIONS`).
pub const MAX_ROSTER_INVOCATIONS: usize = 64;

/// The per-thread usage rows before `truncated` is stated (`:762-764`, the thread-record budget).
pub const USAGE_THREAD_LIMIT: usize = crate::contract::MAX_THREAD_RECORDS;

/// The harness-owned control tools a session can neither un-register nor switch off
/// (`api-reads.ts:104-106`, `CORE_TOOL_NAMES` from `harnesses/toolbox.ts:188-196`).
///
/// TS reads this list off the toolbox module; the Rust port has the same list as
/// `registry::presets::CORE_TOOL_NAMES` plus the declared names, which is what
/// `sessionToolRowsOf`'s `protected` test actually is (`protected: isProtectedToolName(name) ||
/// isDeclared`).
#[must_use]
pub fn is_protected_tool_name(name: &str) -> bool {
    crate::registry::presets::CORE_TOOL_NAMES.contains(&name)
}

// ---------------------------------------------------------------------------
// Preview text (`turn-projector.ts:134-156`, reached through `api-reads.ts:328-329`)
// ---------------------------------------------------------------------------

/// `flattenForDisplay` (`turn-projector.ts:134-148`): the one-way markdown flattening a preview
/// runs before bounding, so a row never carries code fences, image syntax, or heading noise.
#[must_use]
pub fn flatten_for_display(text: &str) -> String {
    let mut value = text.to_string();

    // Each substitution is TS's regex, in TS's order (`:138-146`).
    value = replace_regex(&value, r"```[^\n]*\n?", " ");
    value = replace_regex(&value, r"!\[([^\]]*)]\([^)]*\)", "$1");
    value = replace_regex(&value, r"\[([^\]]*)]\([^)]*\)", "$1");
    value = replace_regex(&value, "`", "");
    value = replace_regex(&value, r"[*_~>#]+", " ");
    value = replace_regex(&value, r"\|", " ");
    value = replace_regex_multiline(&value, r"(?m)^\s*[-=]{3,}\s*$", " ");
    value = replace_regex(&value, r"\s+", " ");
    value = replace_regex(&value, r"\s+([.,!?;:])", "$1");

    value.trim().to_string()
}

/// `previewText` (`turn-projector.ts:150-156`): flatten, then bound with a single `…`.
///
/// Delegates to [`crate::projector::preview_text`], because that is the SAME TypeScript function
/// `promptPreview` calls, and it bounds in UTF-16 code units - the unit every `DISPLAY_*` cap and
/// every `chars` figure uses (`turn-display.ts:412-416`, invariant 5). A character count here would
/// disagree with TS for any astral character in a prompt.
#[must_use]
pub fn preview_text(text: &str, limit: usize) -> String {
    crate::projector::preview_text(text, limit)
}

/// `promptPreview` (`api-reads.ts:328-329`): the RAW length decides whether anything happens at
/// all, so an already-short prompt is returned by identity - flattening only runs on a long one.
#[must_use]
pub fn prompt_preview(prompt: &str, limit: usize) -> String {
    if crate::projector::display_chars(prompt) <= limit {
        return prompt.to_string();
    }

    preview_text(prompt, limit)
}

/// The turn-row bound, the queue bound, and the steer-audit bound in one place, so a caller cannot
/// pick the wrong constant by accident.
#[must_use]
pub fn turn_prompt_preview(prompt: &str) -> String {
    prompt_preview(prompt, TURN_PROMPT_PREVIEW_CHARS)
}

// ---------------------------------------------------------------------------
// Shared session-state accessors the projections need
// ---------------------------------------------------------------------------

impl SessionState {
    /// `hostThreads` (`api-reads.ts:175-177`): an agent's default `agent:<name>` lane is internal
    /// plumbing and never appears in a count, a roster, or a page.
    #[must_use]
    pub fn host_thread_records(&self) -> Vec<&crate::persistence::HarnessThreadRecord> {
        self.thread_order
            .iter()
            .filter_map(|id| self.threads.get(id))
            .filter(|thread| thread.origin == "host")
            .collect()
    }

    /// `pendingCountOf` (`api-reads.ts:166-173`): the legacy queue plus every lane backlog - the
    /// figure `MAX_PENDING_TASKS` bounds and `queue_depth` reports.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.queue.len()
            + self
                .lanes
                .values()
                .map(|lane| lane.queue.len())
                .sum::<usize>()
    }

    /// `laneActive` (`sessions.ts:2394-2400`): true while ANY conversation lane is executing, which
    /// is what makes the aggregate session status `running`.
    #[must_use]
    pub fn lane_active(&self) -> bool {
        self.lanes.values().any(|lane| lane.active_turn.is_some())
    }

    /// Every live run (`sessions.ts:2391-2400`).
    #[must_use]
    pub fn active_run_count(&self) -> usize {
        self.active.len()
    }

    /// The recorded turns in recorded (insertion) order, oldest first (`session.turns`, `:515-519`).
    #[must_use]
    pub fn turns_in_order(&self) -> Vec<&TurnState> {
        self.turn_order
            .iter()
            .filter_map(|id| self.turns.get(id))
            .collect()
    }

    /// The recorded turns sorted by `index`, the order the turns page defaults to
    /// (`api.ts:3682-3684`).
    #[must_use]
    pub fn turns_in_index_order(&self, descending: bool) -> Vec<&TurnState> {
        let mut turns = self.turns_in_order();

        turns.sort_by_key(|turn| turn.record.index);

        if descending {
            turns.reverse();
        }

        turns
    }

    /// The queued turn ids in pump order: every backlog concatenated, then sorted by `index`
    /// (`queueRowsOf`, `api-reads.ts:695-699`).
    #[must_use]
    pub fn queued_turn_ids(&self) -> Vec<String> {
        let mut ids: Vec<&String> = Vec::new();

        ids.extend(self.queue.iter());

        for lane in self.lanes.values() {
            ids.extend(lane.queue.iter());
        }

        let mut entries: Vec<(u64, String)> = ids
            .into_iter()
            .filter_map(|id| {
                self.turns
                    .get(id)
                    .map(|turn| (turn.record.index, id.clone()))
            })
            .collect();

        entries.sort_by_key(|(index, _)| *index);

        entries.into_iter().map(|(_, id)| id).collect()
    }

    /// `owningTurnOf` (`api-reads.ts:179-183`): the turn a conversation's run belongs to.
    #[must_use]
    pub fn owning_turn_id(&self, thread_id: Option<&str>) -> Option<String> {
        let thread_id = thread_id?;

        self.turn_order
            .iter()
            .filter_map(|id| self.turns.get(id))
            .find(|turn| turn.record.thread_id.as_deref() == Some(thread_id))
            .map(|turn| turn.record.id.clone())
    }

    /// One conversation's committed transcript. `None` is "this conversation is unknown"; an unreadable
    /// transcript keeps a valid PLACEHOLDER here and its raw value in `invalid_threads`
    /// (`sessions.ts:806-817` `unreadableTranscript`).
    #[must_use]
    pub fn thread_messages(&self, thread_id: &str) -> Option<Vec<HarnessMessage>> {
        self.threads
            .get(thread_id)
            .map(|thread| thread.messages.clone())
    }

    /// `sessions.ts:2405-2411` `busyOrPending`: a live run anywhere plus accepted-but-unstarted
    /// work, which is the gate `reset`, an agent binding, and a tool registration all share.
    #[must_use]
    pub fn busy_or_pending(&self) -> bool {
        self.lane_active() || self.active_run_count() > 0 || self.pending_count() > 0
    }
}

// ---------------------------------------------------------------------------
// The model / instruction ladders
// ---------------------------------------------------------------------------

impl SessionState {
    /// The frozen definition copy this session runs (`api-reads.ts:939`, `record.definition`).
    #[must_use]
    pub fn definition(&self) -> crate::registry::HarnessDescriptor {
        crate::store::definition_of(self)
    }

    /// The roster: the definition's members plus every agent bound at session scope
    /// (`api-reads.ts:940-960`; the contract reports a session binding with
    /// `origin: "session"`).
    #[must_use]
    pub fn roster(&self) -> Vec<String> {
        let definition = self.definition();
        let mut members = definition.agents.clone();

        for binding in self.session_agents.values() {
            if !members.contains(&binding.agent_id) {
                members.push(binding.agent_id.clone());
            }
        }

        members
    }

    /// The entry agent of this session (`definition.primary`, the one member that is never
    /// detachable, `api-reads.ts:968-969`).
    #[must_use]
    pub fn primary_agent(&self) -> String {
        self.definition().primary
    }

    /// The model one member actually runs with.
    ///
    /// The ladder is the live map first (`agentModelsOf`, `api-reads.ts:232-241` - the record's
    /// `agentModels` IS the live map here, since this port has no separate brain), then the
    /// session-scoped binding override, then the frozen definition's per-agent model
    /// (`inherited_model`, `:946`), then the session's own model (`record.settings.model`).
    #[must_use]
    pub fn agent_model(&self, agent: &str) -> String {
        if let Some(model) = self
            .record
            .agent_models
            .as_ref()
            .and_then(|models| models.get(agent))
        {
            return model.clone();
        }

        if let Some(model) = self.session_agents.get(agent).and_then(|b| b.model.clone()) {
            return model;
        }

        let definition = self.definition();

        if let Some(model) = definition.agent_models.get(agent) {
            return model.clone();
        }

        self.record.settings.model.clone()
    }

    /// `agentModelsOf` (`api-reads.ts:232-241`), over the roster.
    #[must_use]
    pub fn agent_models_of(&self) -> BTreeMap<String, String> {
        self.roster()
            .into_iter()
            .map(|agent| {
                let model = self.agent_model(&agent);
                (agent, model)
            })
            .collect()
    }

    /// The model the session reports as its own (`record.harness.agent.model()`,
    /// `api-reads.ts:206`: the ENTRY agent's model).
    #[must_use]
    pub fn model(&self) -> String {
        self.agent_model(&self.primary_agent())
    }

    /// The instruction provenance ladder for one member: session -> harness -> global -> catalogue
    /// (`api-reads.ts:992-1019` `instructionSourceOf`, widened with the session layer the contract
    /// defines at `harness-api.ts:938`).
    ///
    /// Returns `(text, source, guidelines)`; `text` is the WHOLE system prompt the member runs,
    /// because `instruction_chars` is measured over it (`:947`, `brain.instructions.join('\n')`).
    #[must_use]
    pub fn instruction_for(
        &self,
        agent: &str,
        catalogue: &AgentCatalogue,
    ) -> (String, SessionInstructionSource, Vec<String>) {
        let definition = self.definition();
        let profile = catalogue
            .effective(agent)
            .or_else(|| catalogue.shipped(agent));
        let guidelines = profile
            .as_ref()
            .map(|profile| profile.guidelines.clone())
            .unwrap_or_default();

        // A per-SESSION override is the most specific decision this conversation made.
        if let Some(instruction) = self
            .session_agents
            .get(agent)
            .and_then(|binding| binding.instruction.clone())
        {
            let trimmed = instruction.trim();

            if !trimmed.is_empty() {
                return (
                    compose_instruction(Some(trimmed), &guidelines),
                    SessionInstructionSource::Session,
                    guidelines,
                );
            }
        }

        // A per-HARNESS override is next: it names this team, so it wins over the deployment layer
        // (`api-reads.ts:1002-1004`).
        if let Some(instruction) = definition.agent_instructions.get(agent) {
            let trimmed = instruction.trim();

            if !trimmed.is_empty() {
                return (
                    compose_instruction(Some(trimmed), &guidelines),
                    SessionInstructionSource::Harness,
                    guidelines,
                );
            }
        }

        // The global layer is a REAL source when the operator stated an instruction
        // (`api-reads.ts:1012-1018`); the text then comes from the EFFECTIVE profile, which already
        // carries that override, so the containment test is satisfied by construction.
        let global = catalogue.overrides_of(agent).instruction;

        let instruction = profile
            .map(|profile| profile.instruction)
            .unwrap_or_default();

        let source = if global.is_some() {
            SessionInstructionSource::Global
        } else {
            SessionInstructionSource::Catalogue
        };

        (
            compose_instruction(Some(&instruction), &guidelines),
            source,
            guidelines,
        )
    }
}

/// `agentInstruction` (`agents/index.ts:113-126`): ONE message, identity sentence plus the trusted
/// guideline bullets, so a saved override can retune a role without dropping the shared rules.
#[must_use]
pub fn compose_instruction(instruction: Option<&str>, guidelines: &[String]) -> String {
    let identity = instruction.map(str::trim).unwrap_or_default().to_string();

    let kept: Vec<&str> = guidelines
        .iter()
        .map(String::as_str)
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();

    if kept.is_empty() {
        return identity;
    }

    let bullets: Vec<String> = kept.iter().map(|line| format!("- {line}")).collect();

    format!("{identity}\n\nGuidelines:\n{}", bullets.join("\n"))
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// One session row: metadata + counters (`sessionSummaryOf`, `api-reads.ts:193-214`).
#[must_use]
pub fn session_summary_of(state: &SessionState, hub: &EventHub) -> SessionSummary {
    let pending = u64::try_from(state.pending_count()).unwrap_or(u64::MAX);

    SessionSummary {
        id: state.record.id.clone(),
        title: state.record.title.clone(),
        harness: state.record.preset.name.clone(),
        status: parse_status(&state.record.status),
        // `queue_depth` is emitted only when there IS pending work: an absent field means "nothing
        // queued", which is also what the spec's optional field means (`:203-205`).
        queue_depth: (pending > 0).then_some(pending),
        model: state.model(),
        created_at: state.record.created_at,
        updated_at: state.record.updated_at,
        last_error: state.record.last_error.clone(),
        turn_count: u64::try_from(state.turns.len()).unwrap_or(u64::MAX),
        thread_count: u64::try_from(state.host_thread_records().len()).unwrap_or(u64::MAX),
        revision: hub.revision("session", Some(&state.record.id)),
    }
}

/// The key-free endpoint view of THIS session (`api-reads.ts:225`, which calls
/// `providerDefaultsOf(record.settings)`).
///
/// The session's OWN settings, never the deployment's: `record.settings` is what
/// `sessions.ts:1861-1870` wrote at create time (the merged trio) and `has_api_key` is the presence
/// of the credential THAT session holds. The credential is memory-only (`sessions.ts:1618-1634`), so
/// a restored session honestly reports `false` even while the deployment still has a key — the
/// deployment's own view is `/config`'s `defaults` document, not this member.
#[must_use]
pub fn session_endpoint_of(state: &SessionState) -> contract::ProviderDefaults {
    contract::ProviderDefaults {
        base_url: state.record.settings.base_url.clone(),
        model: state.record.settings.model.clone(),
        has_api_key: state
            .provider_key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty()),
    }
}

/// The identified session read (`sessionResourceOf`, `api-reads.ts:221-230`).
#[must_use]
pub fn session_resource_of(
    state: &SessionState,
    hub: &EventHub,
    runtime: &ServerConfig,
) -> contract::SessionResource {
    contract::SessionResource {
        session: session_summary_of(state, hub),
        definition_revision: hub.revision("definition", Some(&state.record.id)),
        agent_models: state.agent_models_of(),
        // `api-reads.ts:225` reads the SESSION's settings, not the deployment's.
        endpoint: session_endpoint_of(state),
        // `api-reads.ts:226` reads `context.runtime.threadExecution`, i.e. the live deployment
        // configuration, not a built-in constant.
        runtime_execution: runtime.thread_execution.clone(),
        pending_action_count: u64::try_from(state.pending_actions.len()).unwrap_or(u64::MAX),
        created_tool_count: u64::try_from(state.created_tools.len()).unwrap_or(u64::MAX),
        created_tool_limit: u64::try_from(crate::contract::MAX_CREATED_TOOLS).unwrap_or(u64::MAX),
        // T1.5's last mile (`api-reads.ts:242-256`): the journal rides the identified session read ONLY
        // when it is non-empty, so a healthy session keeps its exact pre-T4 key set. The entries are
        // copied, never aliased, which is what stops a consumer writing through the response into the
        // stored journal.
        plugin_diagnostics: state
            .record
            .plugin_diagnostics
            .as_ref()
            .filter(|entries| !entries.is_empty())
            .cloned(),
    }
}

/// The `session.updated` patch (`sessionStatePatchOf`, `api-reads.ts:1307-1320`): metadata and
/// counters, never turns/threads/transcripts.
#[must_use]
pub fn session_state_patch_of(state: &SessionState, hub: &EventHub) -> SessionStatePatch {
    let pending = u64::try_from(state.pending_count()).unwrap_or(u64::MAX);

    SessionStatePatch {
        id: state.record.id.clone(),
        title: state.record.title.clone(),
        status: Some(parse_status(&state.record.status)),
        queue_depth: (pending > 0).then_some(pending),
        model: Some(state.model()),
        agent_models: Some(state.agent_models_of()),
        last_error: state.record.last_error.clone(),
        revision: hub.revision("session", Some(&state.record.id)),
    }
}

/// The frozen definition copy one session runs (`sessionDefinitionOf`, `api-reads.ts:1225-1289`).
///
/// `revision`/`definition_revision` are the session-scoped `definition` counter (the frozen copy's
/// own), while `source_revision == current_revision` are the LIVE registry entry's counter
/// (`:1282-1283`), and `current` compares the frozen document with the live description
/// (`api.ts:2120`: a JSON equality check, so a removed harness is never current).
#[must_use]
pub fn session_definition_of(
    state: &SessionState,
    hub: &EventHub,
    registry: &HarnessRegistry,
    catalogue: &AgentCatalogue,
) -> SessionDefinitionResource {
    let definition = state.definition();
    // `current` compares the frozen copy with the live definition of the same name (`api.ts:2120`:
    // a JSON equality check), so a removed harness is never current.
    let current = registry
        .find(&definition.name)
        .is_some_and(|live| crate::store::descriptor_record_of(live) == state.record.definition);

    let live_revision = hub.revision("definition", Some(&definition.name));
    let session_revision = hub.revision("definition", Some(&state.record.id));

    let card = crate::read::harness_card_of_revision(&definition, session_revision);

    // `instruction_summary` is emitted only when the frozen copy declares overrides
    // (`:1247`), and its `chars` is the length of the text the NEXT session on this definition runs
    // (`:1250-1277`).
    let summary = (!definition.agent_instructions.is_empty()).then(|| {
        definition
            .agents
            .iter()
            .map(|agent| {
                let harness_override = definition.agent_instructions.contains_key(agent);
                let global_instruction = catalogue.overrides_of(agent).instruction;
                let effective = crate::registry::instruction_summary_of(&definition, catalogue)
                    .into_iter()
                    .find(|entry| entry.agent == *agent)
                    .map(|entry| entry.chars)
                    .unwrap_or(0);

                contract::HarnessInstructionSummaryEntry {
                    agent: agent.clone(),
                    source: if harness_override {
                        InstructionSource::Harness
                    } else if global_instruction.is_some() {
                        InstructionSource::Global
                    } else {
                        InstructionSource::Catalogue
                    },
                    overridden: harness_override,
                    chars: effective,
                }
            })
            .collect()
    });

    let core_tools = definition.core_tools();

    SessionDefinitionResource {
        definition: contract::HarnessDefinition {
            card,
            core_tools: (!core_tools.is_empty()).then_some(core_tools),
            agent_models: (!definition.agent_models.is_empty())
                .then(|| definition.agent_models.clone()),
            // R2/R1: the frozen copy's per-agent display COLORS, under the exact non-empty rule
            // `harnessDefinitionOf` and `describeHarness` use (`api-reads.ts:1318-1329`), so the
            // swatch a saved-session UI reads here is byte-identical to the one the live
            // conversation paints. A member the map omits has NO configured color.
            agent_colors: (!definition.agent_colors.is_empty())
                .then(|| definition.agent_colors.clone()),
            instruction_summary: summary,
        },
        session_id: state.record.id.clone(),
        revision: session_revision,
        frozen_at: state.record.created_at,
        source_revision: live_revision,
        current_revision: live_revision,
        current,
        // Only the names the RUN enabled through `toolRequest` (`:1285-1287`); this port tracks
        // discovery as the session's enabled set, and a created tool is listed under `origin`, not
        // here.
        discovered_tools: None,
    }
}

// ---------------------------------------------------------------------------
// Turns
// ---------------------------------------------------------------------------

impl TurnState {
    /// The compact row of this turn (`turnRowOf`, `api-reads.ts:276-296`): the projector's
    /// camelCase `TurnSummary` with `prompt` replaced by the bounded `promptPreview` + exact
    /// `promptChars` pair, and the same rename applied to every `steers[i]` entry (`steerRowOf`,
    /// `:315-325`).
    ///
    /// THE T5b MERGE (knowledge.md "The projector seam's limits"): `ProjectorFactory::create()` is
    /// handed NO turn record, so the fold states only what the events prove - `id: ""`, `index: 0`,
    /// `prompt: ""`, no `threadId`/`threadLabel`, no attachments, no references, no steers. In the
    /// TypeScript original the projector HAS the record (`turn-projector.ts:2826-2870` `summaryOf`)
    /// and prints those members itself. This is the one place the two halves meet: the record is
    /// borrowed here, so the fold stays pure and the WIRE row is the contract's `TurnSummary`.
    #[must_use]
    pub fn row(&self) -> Value {
        let mut row = self.projector.summary();

        let Some(map) = row.as_object_mut() else {
            return row;
        };

        let record = &self.record;

        // The record-only members, stamped in the projector's own key order (`turnId` stays absent:
        // a ROW is addressed by its id, the display tree is the document that names its turn).
        map.insert("id".to_owned(), Value::String(record.id.clone()));
        map.insert("index".to_owned(), Value::from(record.index));

        // `threadId`/`threadLabel`/`startedAt` are optional on the contract and TS omits them when
        // the turn has none, so an absent record member must not print as `null`.
        if let Some(thread_id) = record.thread_id.as_deref() {
            map.insert("threadId".to_owned(), Value::String(thread_id.to_owned()));
        }

        if let Some(label) = record.thread_label.as_deref() {
            map.insert("threadLabel".to_owned(), Value::String(label.to_owned()));
        }

        if let Some(started_at) = record.started_at {
            map.insert("startedAt".to_owned(), Value::from(started_at));
        }

        let prompt = record.prompt.clone();

        map.remove("prompt");
        map.insert(
            "promptPreview".to_owned(),
            Value::String(turn_prompt_preview(&prompt)),
        );
        map.insert(
            "promptChars".to_owned(),
            Value::Number(
                // UTF-16 units: `promptChars` is TS's `prompt.length` (`api-reads.ts:284`).
                u64::try_from(crate::projector::display_chars(&prompt))
                    .unwrap_or(u64::MAX)
                    .into(),
            ),
        );

        // The attachment chips (`turn-projector.ts:2767-2784`), byte-bounded and camelCase like the
        // rest of the display subtree; only this turn's own attachments, and only when it has any.
        let attachments = record.attachments.clone().unwrap_or_default();

        if !attachments.is_empty() {
            map.insert(
                "attachments".to_owned(),
                Value::Array(
                    attachments
                        .iter()
                        .map(crate::attachments::inline_display_attachment)
                        .collect(),
                ),
            );
        }

        // The `#LABEL` references of this submit (`:2862`), omitted when empty like every other
        // optional display member.
        let references = record.references.clone().unwrap_or_default();

        if !references.is_empty() {
            map.insert(
                "references".to_owned(),
                serde_json::to_value(references).unwrap_or(Value::Null),
            );
        }

        // The steer entries: the record's own ring, projected by the fold (`syncSteerBlocks`), then
        // the same `prompt` -> `promptPreview` + `promptChars` rename `steerRowOf` applies.
        let steers = record.steers.clone().unwrap_or_default();

        if !steers.is_empty() {
            let renamed: Vec<Value> = steers
                .iter()
                .map(|steer| steer_row(&serde_json::to_value(steer).unwrap_or(Value::Null)))
                .collect();

            map.insert("steers".to_owned(), Value::Array(renamed));
        } else if let Some(steers) = map.get("steers").cloned().filter(Value::is_array) {
            // A fold that did mint steer rows keeps them, and they ride the same rename.
            let renamed: Vec<Value> = steers
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .map(steer_row)
                .collect();

            map.insert("steers".to_owned(), Value::Array(renamed));
        }

        row
    }

    /// The turn's display document (`projector.display()`), with the record-only identity the seam
    /// cannot see stamped in: `turnId`, and every section id rewritten to the store's mint
    /// (`${turnId}:${ordinal}`, `turn-projector.ts:698`) along with the references to it.
    ///
    /// WHY HERE: the fold's internal keys are `s{ordinal}` (`projector.rs:434`) because it has no
    /// turn id, and the wire identity is the minted one - so the id rename runs at the read boundary
    /// where the record IS in hand, and the block ids (`{ordinal}-{kind}-{key}`, `:1287`) need no
    /// rewrite at all because they never embed the turn id.
    #[must_use]
    pub fn display(&self) -> Value {
        let mut display = self.projector.display();
        let turn_id = self.record.id.clone();

        let Some(map) = display.as_object_mut() else {
            return display;
        };

        map.insert("turnId".to_owned(), Value::String(turn_id.clone()));

        // Old id -> wire id, by MINT POSITION: `mint_section` allocates the ordinal and appends the
        // section in the same step, so the array order IS the ordinal order.
        let Some(sections) = map.get_mut("sections").and_then(Value::as_array_mut) else {
            return display;
        };

        // Old id -> wire id through the ONE rule the live `display.ops` frames use
        // (`projector::section_wire_id` / `projector::stamp_display_ops`). The projector mints
        // `s{ordinal}` in the same step that appends the section to this array (`projector.rs:418-434`),
        // so the ordinal embedded in the key and the array position state the same fact, and a listener
        // that applied the ops it streamed reads the SAME section ids this snapshot prints.
        let lookup = |id: &str| -> String { crate::projector::section_wire_id(&turn_id, id) };

        for section in sections.iter_mut() {
            let Some(entry) = section.as_object_mut() else {
                continue;
            };

            if let Some(id) = entry.get("id").and_then(Value::as_str) {
                let id = id.to_owned();
                entry.insert("id".to_owned(), Value::String(lookup(&id)));
            }

            // The parent reference is rewritten in a SECOND pass per section, so a parent minted
            // later than its child still resolves (`parentSectionId` is a mint-time id).
            if let Some(parent) = entry
                .get("parentSectionId")
                .and_then(Value::as_str)
                .map(str::to_owned)
            {
                entry.insert("parentSectionId".to_owned(), Value::String(lookup(&parent)));
            }

            // A step NODE is a full `DisplayStep` copy, and TS's copy names its turn and its
            // section (`turn-projector.ts:1982-1990`); a prose/notice node names neither.
            if let Some(nodes) = entry.get_mut("nodes").and_then(Value::as_array_mut) {
                for node in nodes.iter_mut() {
                    let Some(step) = node.as_object_mut() else {
                        continue;
                    };

                    step.insert("turnId".to_owned(), Value::String(turn_id.clone()));

                    if let Some(section_id) = step
                        .get("sectionId")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                    {
                        step.insert("sectionId".to_owned(), Value::String(lookup(&section_id)));
                    }
                }
            }
        }

        display
    }
}

/// `steerRowOf` (`api-reads.ts:315-325`).
#[must_use]
pub fn steer_row(steer: &Value) -> Value {
    let mut entry = match steer {
        Value::Object(map) => map.clone(),
        other => return other.clone(),
    };

    let text = entry
        .get("prompt")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    entry.remove("prompt");
    entry.insert(
        "promptPreview".to_owned(),
        Value::String(prompt_preview(&text, TURN_PROMPT_PREVIEW_CHARS)),
    );
    entry.insert(
        "promptChars".to_owned(),
        Value::Number(
            u64::try_from(crate::projector::display_chars(&text))
                .unwrap_or(u64::MAX)
                .into(),
        ),
    );

    Value::Object(entry)
}

/// The identified turn read (`turnResourceOf`, `api-reads.ts:336-390`): metadata plus COUNTS. The
/// verbatim prompt lives here because this is the one route the contract says the full task text is
/// readable on.
#[must_use]
pub fn turn_resource_of(state: &SessionState, turn: &TurnState, hub: &EventHub) -> TurnResource {
    let record = &turn.record;
    let attachments = record.attachments.clone().unwrap_or_default();
    let steers = record.steers.clone().unwrap_or_default();
    let steer_texts = record.steer_texts.clone().unwrap_or_default();

    TurnResource {
        session_id: state.record.id.clone(),
        id: record.id.clone(),
        index: record.index,
        status: parse_turn_status(&record.status),
        prompt: record.prompt.clone(),
        thread_id: record.thread_id.clone(),
        thread_label: record.thread_label.clone(),
        started_at: record.started_at,
        updated_at: record.updated_at,
        usage: record.usage.clone(),
        error: turn_error_of(turn),
        references: record
            .references
            .clone()
            .filter(|references| !references.is_empty()),
        // The BYTE-FREE projection rides only when the turn carries media (`:362-364`).
        attachments: (!attachments.is_empty()).then(|| {
            attachments
                .iter()
                .map(crate::attachments::attachment_descriptor)
                .collect()
        }),
        event_count: u64::try_from(record.events.len()).unwrap_or(u64::MAX),
        event_time_count: record
            .event_times
            .as_ref()
            .map(|times| u64::try_from(times.len()).unwrap_or(u64::MAX)),
        attachment_count: u64::try_from(attachments.len()).unwrap_or(u64::MAX),
        steer_count: u64::try_from(steers.len()).unwrap_or(u64::MAX),
        steer_text_count: (!steer_texts.is_empty())
            .then(|| u64::try_from(steer_texts.len()).unwrap_or(u64::MAX)),
        steers: (!steers.is_empty()).then(|| {
            steers
                .iter()
                .map(|steer| contract::SteerRecord {
                    id: steer.id.clone(),
                    at: steer.at,
                    prompt_preview: prompt_preview(&steer.prompt, QUEUE_PREVIEW_CHARS),
                    prompt_chars: u64::try_from(crate::projector::display_chars(&steer.prompt))
                        .unwrap_or(u64::MAX),
                    // A steer delivered to the owning turn's own conversation is not restated
                    // (`:378-382`).
                    thread_id: steer
                        .thread_id
                        .clone()
                        .filter(|id| Some(id) != record.thread_id.as_ref()),
                    references: steer
                        .references
                        .clone()
                        .filter(|references| !references.is_empty()),
                })
                .collect()
        }),
        revision: hub.revision("turn", Some(&turn_key(&state.record.id, &record.id))),
    }
}

/// `turnErrorOf` (`api-reads.ts:398-411`): the recorded failure, read off the LAST `error` event,
/// verbatim - never a second copy of the message.
#[must_use]
pub fn turn_error_of(turn: &TurnState) -> Option<HarnessErrorInfo> {
    turn.record
        .events
        .iter()
        .rev()
        .find_map(|entry| match &entry.event {
            HarnessEvent::Error { error, .. } => Some(error.clone()),
            _ => None,
        })
}

/// `turnEventRowsOf` (`api-reads.ts:429-478`).
///
/// The filter runs over the ORDINAL space, not the filtered array: a cursor addresses recorded
/// positions, so skipping non-matching events must not shift what the next cursor means (`:438-441`).
#[must_use]
pub fn turn_event_rows_of(
    turn: &TurnState,
    offset: u64,
    limit: u64,
    type_filter: Option<&str>,
) -> (Vec<TurnEventRow>, u64, bool) {
    let times = turn.record.event_times.clone().unwrap_or_default();
    let matching = |event: &HarnessEvent| -> bool {
        type_filter.is_none_or(|wanted| event_type_name(event) == wanted)
    };

    let mut rows: Vec<TurnEventRow> = Vec::new();
    let mut served = 0u64;
    let mut collected = 0u64;

    for (index, entry) in turn.record.events.iter().enumerate() {
        if !matching(&entry.event) {
            continue;
        }

        if served < offset {
            served += 1;
            continue;
        }

        if collected >= limit {
            break;
        }

        let ordinal = u64::try_from(index).unwrap_or(u64::MAX);

        rows.push(TurnEventRow {
            index: ordinal,
            at: times.get(index).copied(),
            event: entry.clone(),
        });

        served += 1;
        collected += 1;
    }

    let total = u64::try_from(
        turn.record
            .events
            .iter()
            .filter(|entry| matching(&entry.event))
            .count(),
    )
    .unwrap_or(u64::MAX);

    let partial = matches!(turn.record.status.as_str(), "running" | "queued");

    (rows, total, partial)
}

/// The `type` tag of one recorded event, in the wire spelling (`contract::HarnessEventType`).
#[must_use]
pub fn event_type_name(event: &HarnessEvent) -> &'static str {
    match event {
        HarnessEvent::Start { .. } => "start",
        HarnessEvent::Text { .. } => "text",
        HarnessEvent::Reasoning { .. } => "reasoning",
        HarnessEvent::ToolCall { .. } => "tool_call",
        HarnessEvent::ToolResult { .. } => "tool_result",
        HarnessEvent::Usage { .. } => "usage",
        HarnessEvent::Done { .. } => "done",
        HarnessEvent::Retry { .. } => "retry",
        HarnessEvent::AttemptCommit { .. } => "attempt_commit",
        HarnessEvent::Wakeup { .. } => "wakeup",
        HarnessEvent::ActionRequest { .. } => "action_request",
        HarnessEvent::ActionAnswer { .. } => "action_answer",
        HarnessEvent::BranchStart { .. } => "branch_start",
        HarnessEvent::BranchEnd { .. } => "branch_end",
        HarnessEvent::Error { .. } => "error",
    }
}

/// The server-summed token figures for one turn (`usagePatchOf`, `api-reads.ts:1342-1366`).
///
/// `latest_input` is the figure a display ROW carries, so it is read out of the projector's row
/// rather than recomputed; `cached_input` is the provider's own cache figure, present only when the
/// provider reported one.
#[must_use]
pub fn usage_patch_of(state: &SessionState, turn_id: &str, hub: &EventHub) -> Option<UsagePatch> {
    let turn = state.turns.get(turn_id)?;
    let usage = turn.record.usage.clone();

    let row = turn.row();
    let latest_input = row
        .get("latestInput")
        .and_then(Value::as_u64)
        .or_else(|| row.get("latest_input").and_then(Value::as_u64));

    Some(UsagePatch {
        turn_id: Some(turn_id.to_string()),
        usage,
        latest_input,
        cached_input: turn.record.usage.as_ref().and_then(|u| u.cached),
        revision: hub.revision("turn", Some(&turn_key(&state.record.id, turn_id))),
    })
}

// ---------------------------------------------------------------------------
// Threads and transcripts
// ---------------------------------------------------------------------------

/// `threadRowOf` (`api-reads.ts:489-516`), built from the roster record plus the store's label -
/// so no transcript travels on a roster page.
#[must_use]
pub fn thread_row_of(state: &SessionState, thread_id: &str) -> Option<ThreadSummary> {
    let thread = state.threads.get(thread_id)?;
    let invalid = state.invalid_threads.get(thread_id);
    let unreadable = invalid.is_some();
    let entries = thread.messages.clone();

    let preview = last_entry_preview(&entries);
    let chars = entry_chars(&entries);

    Some(ThreadSummary {
        id: thread.id.clone(),
        agent: thread.agent.clone(),
        origin: parse_origin(&thread.origin),
        status: parse_lifecycle(&thread.status),
        sequence: thread.sequence,
        owner_thread_id: thread.owner_thread_id.clone(),
        label: thread.label.clone(),
        transcript_state: if unreadable {
            TranscriptState::Unreadable
        } else {
            TranscriptState::Ok
        },
        transcript_error: invalid.map(|invalid| invalid.error.clone()),
        // An unreadable conversation reports ZERO live entries: its raw value is not a transcript
        // (`:511`).
        message_count: if unreadable { 0 } else { entry_count(&entries) },
        chars: (!entries.is_empty()).then(|| u64::try_from(chars).unwrap_or(u64::MAX)),
        has_transcript: !unreadable && !entries.is_empty(),
        preview,
    })
}

/// `threadResourceOf` (`api-reads.ts:536-570`): a row plus the invocation roster and the reference
/// graph - still no messages, which is what makes a 256-conversation roster a bounded read.
#[must_use]
pub fn thread_resource_of(
    state: &SessionState,
    thread_id: &str,
    hub: &EventHub,
    transcript_bytes: Option<usize>,
) -> Option<ThreadResource> {
    let row = thread_row_of(state, thread_id)?;
    let key = thread_key(&state.record.id, thread_id);
    let owning_turn = state.owning_turn_id(Some(thread_id));
    let turn = owning_turn
        .as_ref()
        .and_then(|id| state.turns.get(id))
        .map(|turn| turn.record.clone());

    let invocation_ids = unique_invocation_ids(state, thread_id);
    let latest = latest_invocation_id(state, thread_id);
    let connected = connected_thread_ids(state, thread_id);

    Some(ThreadResource {
        thread: row,
        session_id: state.record.id.clone(),
        origin_turn_id: turn.as_ref().map(|turn| turn.id.clone()),
        origin_invocation_id: turn.as_ref().and_then(|turn| {
            state
                .record
                .turn_invocations
                .as_ref()
                .and_then(|map| map.get(&turn.id))
                .cloned()
        }),
        invocation_id: latest,
        invocation_ids: invocation_ids.clone(),
        // The roster budget is the store's own cap on the persisted invocation map; a longer
        // history reports `true` so a client never reads a truncated list as total (`:555-557`).
        invocation_ids_truncated: invocation_ids.len() >= MAX_ROSTER_INVOCATIONS,
        references: turn
            .as_ref()
            .and_then(|turn| turn.references.clone())
            .filter(|references| !references.is_empty()),
        connected_thread_ids: (!connected.is_empty()).then_some(connected),
        transcript_revision: hub.revision("transcript", Some(&key)),
        transcript_bytes: transcript_bytes.map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX)),
        revision: hub.revision("thread", Some(&key)),
    })
}

/// `uniqueInvocationIds` (`api-reads.ts:576-591`): every invocation recorded in this conversation,
/// oldest first, capped.
#[must_use]
pub fn unique_invocation_ids(state: &SessionState, thread_id: &str) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();

    for (invocation_id, mapped) in state.record.invocation_threads.clone().unwrap_or_default() {
        if mapped != thread_id || ids.contains(&invocation_id) {
            continue;
        }

        ids.push(invocation_id);

        if ids.len() >= MAX_ROSTER_INVOCATIONS {
            break;
        }
    }

    ids
}

/// `latestInvocationId` (`api-reads.ts:593-602`).
#[must_use]
pub fn latest_invocation_id(state: &SessionState, thread_id: &str) -> Option<String> {
    let mut latest: Option<String> = None;

    for (invocation_id, mapped) in state.record.invocation_threads.clone().unwrap_or_default() {
        if mapped == thread_id {
            latest = Some(invocation_id);
        }
    }

    latest
}

/// `connectedThreadsOf` (`api-reads.ts:604-619`): reference EDGES out of this conversation - a link,
/// never ownership - sorted for a stable wire order.
#[must_use]
pub fn connected_thread_ids(state: &SessionState, thread_id: &str) -> Vec<String> {
    let mut connected: Vec<String> = Vec::new();

    for turn in state.turns_in_order() {
        if turn.record.thread_id.as_deref() != Some(thread_id) {
            continue;
        }

        for reference in turn.record.references.clone().unwrap_or_default() {
            let Some(target) = reference.thread_id else {
                continue;
            };

            if target == thread_id || connected.contains(&target) {
                continue;
            }

            connected.push(target);
        }
    }

    connected.sort();
    connected
}

/// `transcriptMetadataOf` (`api-reads.ts:626-658`): the cheap head of one transcript - size, counts,
/// revision, and validity, with no content at all.
#[must_use]
pub fn transcript_metadata_of(
    state: &SessionState,
    thread_id: &str,
    agent: &str,
    messages: &[HarnessMessage],
    hub: &EventHub,
) -> TranscriptMetadata {
    let invalid = state.invalid_threads.get(thread_id);
    let mut role_counts = TranscriptRoleCounts::default();

    for entry in messages {
        match entry {
            HarnessMessage::System { .. } => role_counts.system += 1,
            HarnessMessage::User { .. } => role_counts.user += 1,
            HarnessMessage::Assistant { .. } => role_counts.assistant += 1,
            HarnessMessage::Tool { .. } => role_counts.tool += 1,
        }
    }

    let chars = entry_chars(messages);
    let bytes = document_bytes(messages);

    TranscriptMetadata {
        session_id: state.record.id.clone(),
        thread_id: thread_id.to_string(),
        agent: agent.to_string(),
        transcript_state: if invalid.is_none() {
            TranscriptState::Ok
        } else {
            TranscriptState::Unreadable
        },
        message_count: entry_count(messages),
        chars: u64::try_from(chars).unwrap_or(u64::MAX),
        // `chars` is what an editor shows, `bytes` what a client would download; they differ only
        // for non-ASCII content, which is honest (`:651-653`).
        bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
        revision: hub.revision("transcript", Some(&thread_key(&state.record.id, thread_id))),
        role_counts,
        unreadable_reason: invalid.map(|invalid| invalid.error.clone()),
        stored_type: invalid.map(|invalid| stored_type_of(&invalid.messages)),
    }
}

/// `transcriptSignalOf` (`api-reads.ts:673-683`): counts + state for the `transcript.updated`
/// stream signal, never the bodies.
#[must_use]
pub fn transcript_signal_of(
    thread_id: &str,
    revision: u64,
    messages: &[HarnessMessage],
    unreadable: bool,
) -> TranscriptSignal {
    TranscriptSignal {
        thread_id: thread_id.to_string(),
        revision,
        message_count: entry_count(messages),
        transcript_state: if unreadable {
            TranscriptState::Unreadable
        } else {
            TranscriptState::Ok
        },
    }
}

/// `storedTypeOf` (`api-reads.ts:661-670`): the JSON type name of a stored unreadable value, which
/// is what the editor shows when it cannot parse a transcript.
#[must_use]
pub fn stored_type_of(value: &Value) -> String {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
    .to_string()
}

/// The serialized size of a transcript document (`:653`, `Buffer.byteLength(JSON.stringify(...))`).
#[must_use]
pub fn document_bytes(messages: &[HarnessMessage]) -> usize {
    serde_json::to_string(messages)
        .map(|text| text.len())
        .unwrap_or(0)
}

/// `entryChars` (`api-reads.ts:532-533`).
#[must_use]
pub fn entry_chars(messages: &[HarnessMessage]) -> usize {
    messages
        .iter()
        .map(|entry| crate::projector::display_chars(message_content(entry)))
        .sum()
}

/// The entry count of a transcript (`:511`, `message_count`).
#[must_use]
pub fn entry_count(messages: &[HarnessMessage]) -> u64 {
    u64::try_from(messages.len()).unwrap_or(u64::MAX)
}

/// The `content` of one transcript entry; every variant carries one (`types/index.ts:116`).
#[must_use]
pub fn message_content(message: &HarnessMessage) -> &str {
    match message {
        HarnessMessage::System { content, .. }
        | HarnessMessage::User { content, .. }
        | HarnessMessage::Assistant { content, .. }
        | HarnessMessage::Tool { content, .. } => content,
    }
}

/// `lastEntryPreview` (`api-reads.ts:519-529`): a bounded tail of the last entry, never the whole
/// transcript.
#[must_use]
pub fn last_entry_preview(messages: &[HarnessMessage]) -> Option<String> {
    let last = messages.last()?;
    let content = message_content(last);

    if content.is_empty() {
        return None;
    }

    Some(preview_text(content, THREAD_PREVIEW_CHARS))
}

/// `ThreadOrigin` from the record's string spelling.
#[must_use]
pub fn parse_origin(value: &str) -> ThreadOrigin {
    match value {
        "agent" => ThreadOrigin::Agent,
        _ => ThreadOrigin::Host,
    }
}

/// `ThreadLifecycle` from the record's string spelling.
///
/// R7: `paused` is one of the four spellings `harness-api.ts:1481` publishes, and it is a REAL state
/// a record can carry: the shipped `context-compaction` plugin parks a conversation while it compacts
/// (a TypeScript host writes that word into `state.json`). This build never transitions a conversation
/// into it - no plugin hook executes here - but it must round-trip rather than fold into `Idle`,
/// because the UI colours a parked conversation differently and a lying roster read would strand
/// that affordance.
#[must_use]
pub fn parse_lifecycle(value: &str) -> ThreadLifecycle {
    match value {
        "running" => ThreadLifecycle::Running,
        "stopped" => ThreadLifecycle::Stopped,
        "paused" => ThreadLifecycle::Paused,
        _ => ThreadLifecycle::Idle,
    }
}

// ---------------------------------------------------------------------------
// Queue / usage / actions / commands
// ---------------------------------------------------------------------------

/// Every pending task, in pump order (`queueRowsOf`, `api-reads.ts:694-722`).
///
/// `position` is the 0-based slot of THIS ordering (`:707`), while the submit receipt reports
/// `position + 1` (`api.ts:2195-2230`): the contract's `queue_position` has `minimum: 1` and the
/// page's `position` is 0-based, and one figure must not be derived from the other by accident.
#[must_use]
pub fn queue_rows_of(state: &SessionState) -> Vec<QueuedTask> {
    state
        .queued_turn_ids()
        .into_iter()
        .filter_map(|id| state.turns.get(&id))
        .enumerate()
        .map(|(position, turn)| {
            let record = &turn.record;
            let attachments = record.attachments.clone().unwrap_or_default();

            QueuedTask {
                turn_id: record.id.clone(),
                index: record.index,
                position: u64::try_from(position).unwrap_or(u64::MAX),
                status: parse_turn_status(&record.status),
                prompt_preview: prompt_preview(&record.prompt, QUEUE_PREVIEW_CHARS),
                prompt_chars: u64::try_from(crate::projector::display_chars(&record.prompt))
                    .unwrap_or(u64::MAX),
                thread_id: record.thread_id.clone(),
                thread_label: record.thread_label.clone(),
                reference_labels: record
                    .references
                    .clone()
                    .filter(|references| !references.is_empty())
                    .map(|references| {
                        references
                            .iter()
                            .map(|reference| reference.label.clone())
                            .collect()
                    }),
                attachment_count: (!attachments.is_empty())
                    .then(|| u64::try_from(attachments.len()).unwrap_or(u64::MAX)),
                // A queued turn has no `startedAt` yet, and its `updatedAt` IS the instant the
                // server accepted it (`:717-719`).
                accepted_at: Some(record.started_at.unwrap_or(record.updated_at)),
            }
        })
        .collect()
}

/// The usage fold (`usageFoldOf`, `api-reads.ts:725-767`): server-summed per-turn tallies, honest
/// about providers that report none.
#[must_use]
pub fn usage_fold_of(state: &SessionState, hub: &EventHub) -> contract::SessionUsageResponse {
    // TS builds BOTH objects as `{ input: 0, output: 0, total: 0 }` and adds only those three
    // members (`api-reads.ts:726`, `:742`): `cached` is provider metadata that belongs on the TURN
    // row, and this fold never states it, so neither the totals nor a per-thread share carries the
    // key.
    let mut totals = HarnessUsage {
        input: 0,
        output: 0,
        total: 0,
        cached: None,
    };
    // A `Map` in TS (`:727`), so `by_thread` comes back in FIRST-APPEARANCE order, not sorted:
    // `[...byThread.entries()]` (`:750`). A Vec with a linear key scan is the same thing here, and
    // the per-thread set is bounded by the thread budget.
    let mut by_thread: Vec<(String, HarnessUsage)> = Vec::new();
    let mut reported = 0u64;

    for turn in state.turns_in_order() {
        let Some(usage) = turn.record.usage.clone() else {
            continue;
        };

        reported += 1;
        totals.input += usage.input;
        totals.output += usage.output;
        totals.total += usage.total;

        let Some(thread_id) = turn.record.thread_id.clone() else {
            continue;
        };

        let position = match by_thread.iter().position(|(id, _)| *id == thread_id) {
            Some(position) => position,
            None => {
                by_thread.push((
                    thread_id.clone(),
                    HarnessUsage {
                        input: 0,
                        output: 0,
                        total: 0,
                        cached: None,
                    },
                ));

                by_thread.len() - 1
            }
        };

        let share = &mut by_thread[position].1;

        share.input += usage.input;
        share.output += usage.output;
        share.total += usage.total;
    }

    let per_thread: Vec<UsageByThread> = by_thread
        .into_iter()
        .map(|(thread_id, usage)| UsageByThread { thread_id, usage })
        .collect();

    contract::SessionUsageResponse {
        session_id: state.record.id.clone(),
        turn_count: u64::try_from(state.turns.len()).unwrap_or(u64::MAX),
        reported_turns: reported,
        // Absent, not zero: "the provider never reported" is not "the session cost 0" (`:759-760`).
        usage: (reported > 0).then_some(totals),
        truncated: per_thread.len() >= USAGE_THREAD_LIMIT,
        by_thread: per_thread,
        revision: hub.revision("usage", Some(&state.record.id)),
    }
}

/// One live owner question as `action.opened` ships it (`pendingActionRowOf`,
/// `api-reads.ts:1323-1339`): the recorded request plus the routing join.
#[must_use]
pub fn pending_action_row_of(
    state: &SessionState,
    entry: &crate::store::PendingActionEntry,
) -> PendingAction {
    let turn_id = entry.turn_id.clone().or_else(|| {
        // The conversation's owning turn, else the running turn when the question came from the
        // session's root run (`:1327-1330`).
        entry.thread_id.as_ref().map_or_else(
            || {
                state
                    .turns_in_order()
                    .into_iter()
                    .find(|turn| turn.record.status == "running")
                    .map(|turn| turn.record.id.clone())
            },
            |thread_id| {
                state
                    .turns_in_order()
                    .into_iter()
                    .rev()
                    .find(|turn| turn.record.thread_id.as_deref() == Some(thread_id.as_str()))
                    .map(|turn| turn.record.id.clone())
            },
        )
    });

    PendingAction {
        request: entry.request.clone(),
        turn_id: turn_id.unwrap_or_default(),
        thread_id: entry.thread_id.clone(),
        agent: entry.agent.clone(),
        received_at: entry.opened_at,
    }
}

/// The slash commands this session's registry answers (`commandRowsOf`, `api-reads.ts:770-781`),
/// sorted by name (`localeCompare` over the registry's keys).
#[must_use]
pub fn command_rows_of() -> Vec<CommandDescriptor> {
    let mut rows: Vec<CommandDescriptor> = crate::store::commands::builtin_commands()
        .into_iter()
        .map(|command| CommandDescriptor {
            name: command.name.clone(),
            invocation: format!("/{}", command.name),
            description: command.description.clone(),
            usage: command.usage.clone(),
        })
        .collect();

    rows.sort_by(|left, right| left.name.cmp(&right.name));

    rows
}

/// The live roster rows (`sessionAgentRowsOf`, `api-reads.ts:938-975`).
///
/// One row states BOTH what the session was built with and what it runs now, which is the whole
/// point of the contract's `inherited_model` / `instruction_source` pair.
#[must_use]
pub fn session_agent_rows_of(
    state: &SessionState,
    hub: &EventHub,
    catalogue: &AgentCatalogue,
) -> Vec<SessionAgentBinding> {
    let definition = state.definition();
    let revision = hub.revision("session_agents", Some(&state.record.id));

    state
        .roster()
        .into_iter()
        .map(|agent| {
            let (text, source, guidelines) = state.instruction_for(&agent, catalogue);
            let binding = state.session_agents.get(&agent);
            let definition_member = definition.agents.contains(&agent);

            SessionAgentBinding {
                agent_id: agent.clone(),
                profile: binding
                    .map(|binding| binding.profile.clone())
                    .unwrap_or_else(|| agent.clone()),
                role: if agent == definition.primary {
                    AgentRole::Primary
                } else {
                    AgentRole::Subagent
                },
                origin: if binding.is_some() && !definition_member {
                    BindingOrigin::Session
                } else {
                    BindingOrigin::Definition
                },
                model: state.agent_model(&agent),
                // The creation-time inherited baseline, which is what a "restore" affordance means
                // (`:962`).
                inherited_model: definition
                    .agent_models
                    .get(&agent)
                    .cloned()
                    .unwrap_or_else(|| state.record.settings.model.clone()),
                instruction_source: source,
                instruction_chars: u64::try_from(crate::projector::display_chars(&text))
                    .unwrap_or(u64::MAX),
                has_instruction_override: matches!(
                    source,
                    SessionInstructionSource::Harness | SessionInstructionSource::Session
                )
                .then_some(true),
                guidelines_count: Some(u64::try_from(guidelines.len()).unwrap_or(u64::MAX)),
                editable: true,
                // The entry agent cannot be detached: every session needs an entry point (`:968`).
                deletable: agent != definition.primary,
                restorable: source != SessionInstructionSource::Catalogue,
                busy: is_agent_busy(state, &agent).then_some(true),
                revision,
            }
        })
        .collect()
}

/// `isAgentBusy` (`api-reads.ts:977-990`): whether one member currently has a run in flight (a lane
/// whose active turn belongs to a thread of THIS agent).
#[must_use]
pub fn is_agent_busy(state: &SessionState, agent: &str) -> bool {
    state
        .lanes
        .values()
        .filter(|lane| lane.active_turn.is_some())
        .any(|lane| {
            state
                .threads
                .get(&lane.thread_id)
                .is_some_and(|thread| thread.agent == agent)
        })
        || (state.record.status == "running" && state.primary_agent() == agent)
}

/// The `{used, limit}` pair the toolbox page and the registration receipt print (`:1759-1765`).
#[must_use]
pub fn budget_of(used: usize) -> ResourceBudget {
    ResourceBudget {
        used: u64::try_from(used).unwrap_or(u64::MAX),
        limit: u64::try_from(crate::contract::MAX_CREATED_TOOLS).unwrap_or(u64::MAX),
    }
}

/// The session tool row's `enabled` decision (`sessionToolRowsOf`, `api-reads.ts:803-828`): the
/// frozen definition's declarations, the core set the harness always registers, and every name
/// this session created or explicitly enabled - minus anything this session switched off.
#[must_use]
pub fn enabled_tool_names_of(
    state: &SessionState,
    registry_names: &[String],
    core_names: &[String],
) -> Vec<String> {
    let mut enabled: Vec<String> = Vec::new();

    for name in registry_names {
        if state.disabled_tools.contains(name) {
            continue;
        }

        if core_names.contains(name) || enabled.contains(name) {
            continue;
        }

        enabled.push(name.clone());
    }

    for tool in state.created_tools.values() {
        if tool.enabled && !enabled.contains(&tool.name) {
            enabled.push(tool.name.clone());
        }
    }

    enabled
}

/// One created tool as the session toolbox stores it, for the durable `core` block
/// (`mod::created_tool_value`).
#[must_use]
pub fn created_tool_json(tool: &crate::store::CreatedTool) -> Value {
    created_tool_value(tool)
}

// ---------------------------------------------------------------------------
// Small regex helpers (the preview pipeline only)
// ---------------------------------------------------------------------------

/// `String.replace(regex, replacement)` over the whole string.
fn replace_regex(text: &str, pattern: &str, replacement: &str) -> String {
    apply_regex(text, pattern, replacement, false)
}

/// The `m`-flag form, used by the horizontal-rule flattening (`/^\s*[-=]{3,}\s*$/gm`).
fn replace_regex_multiline(text: &str, pattern: &str, replacement: &str) -> String {
    apply_regex(text, pattern, replacement, true)
}

fn apply_regex(text: &str, pattern: &str, replacement: &str, _multiline: bool) -> String {
    let Some(expression) = crate::store::read::compiled_preview_regex(pattern) else {
        return text.to_string();
    };

    expression.replace_all(text, replacement).into_owned()
}

/// One preview regex, compiled once per pattern (`regex` is already a dependency, and these
/// patterns are static).
fn compiled_preview_regex(pattern: &str) -> Option<&'static regex::Regex> {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static CACHE: OnceLock<Mutex<HashMap<String, &'static regex::Regex>>> = OnceLock::new();

    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(expression) = guard.get(pattern) {
        return Some(*expression);
    }

    let expression: &'static regex::Regex = Box::leak(Box::new(regex::Regex::new(pattern).ok()?));
    guard.insert(pattern.to_owned(), expression);

    Some(expression)
}

/// The timestamp every store signal stamps, kept in one place so a frame and the record it
/// describes agree (`sessions.ts:7095-7096`: one `at` for the event, the `eventTimes` entry, and the
/// published frame).
#[must_use]
pub fn signal_time() -> u64 {
    now_millis()
}

/// The `SessionStatus` spelling of a summary, kept for the tests that assert a filter value.
#[must_use]
pub fn status_value(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Idle => "idle",
        SessionStatus::Running => "running",
        SessionStatus::Error => "error",
    }
}

/// The `AgentResultStatus` a turn row's block reports, derived from the turn status the same way
/// `turn-projector` does. Exposed because the projector's row is a `Value` and a caller that builds
/// a block reference still needs the same vocabulary.
#[must_use]
pub fn result_status_of(turn_status: &str) -> AgentResultStatus {
    match turn_status {
        "done" => AgentResultStatus::Done,
        "error" => AgentResultStatus::Error,
        "cancelled" => AgentResultStatus::Cancelled,
        "running" => AgentResultStatus::Running,
        _ => AgentResultStatus::Queued,
    }
}

/// The `SessionToolSummary` shell every session tool row shares, so the catalogue projection and the
/// created-tool projection cannot disagree about the session-scoped half (`api-reads.ts:816-827`).
///
/// Eight arguments is the shape of the row, not a design smell: `SessionToolSummary` is a flat contract
/// record with seven members, and every one of them is a decision only the CALLER knows (the catalogue
/// cannot state `declared`, and the session cannot state the catalogue's `tags`).
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn session_tool_row_shell(
    tool: contract::ToolSummary,
    session_id: &str,
    declared: bool,
    enabled: bool,
    origin: contract::SessionToolOrigin,
    protected: bool,
    source_available: bool,
    source_revision: Option<String>,
) -> SessionToolSummary {
    SessionToolSummary {
        tool,
        session_id: session_id.to_string(),
        declared,
        enabled,
        origin,
        protected,
        source_available,
        source_revision,
    }
}

// ----------------------------------------------------------------------
// Provider resolution and the two preview aliases the command rows use
// ----------------------------------------------------------------------

/// A bounded preview under the name the command rows use; [`preview_text`] owns the algorithm.
#[must_use]
pub fn preview(text: &str, limit: usize) -> String {
    preview_text(text, limit)
}

/// The preview budget a slash command row prints (`api-reads.ts:102`), named after the row that uses it.
pub const TURN_PREVIEW_CHARS: usize = TURN_PROMPT_PREVIEW_CHARS;

/// The `role` a transcript entry reports (`api-reads.ts:494`): the variant name in wire spelling, which
/// is also the spelling a `?role=` filter compares against.
#[must_use]
pub fn message_role_name(message: &HarnessMessage) -> &'static str {
    match message {
        HarnessMessage::System { .. } => "system",
        HarnessMessage::User { .. } => "user",
        HarnessMessage::Assistant { .. } => "assistant",
        HarnessMessage::Tool { .. } => "tool",
    }
}

/// `shared/runtime.ts:109 configuredProviderHost`: the `host[:port]` a credential is scoped to.
///
/// The scheme, the path and any URL userinfo are dropped, because the identity that matters is only
/// "which server does this key belong to". `None` is an endpoint that names no host at all, which is what
/// makes an unset base URL never match a stored credential.
#[must_use]
pub fn provider_host_of(base_url: &str) -> Option<String> {
    let after_scheme = match base_url.split_once("://") {
        Some((_, rest)) => rest,
        None => base_url,
    };
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // `user:pass@host` is part of the endpoint's syntax, not part of the host a key is scoped to.
    let host = authority.rsplit('@').next().unwrap_or(authority);

    (!host.is_empty()).then(|| host.to_owned())
}

/// The sampling fields every provider request this server builds carries
/// (`server/harnesses/toolbox.ts:242-246 providerOf`, which attaches `sampling: { temperature: 0.3 }`
/// to the transport configuration of EVERY session agent). It is an application-layer choice, not a
/// deployment knob: a client never supplies sampling, exactly as it never supplies the transport.
#[must_use]
pub fn default_sampling() -> Value {
    serde_json::json!({ "temperature": 0.3 })
}

/// The provider trio the NEXT turn runs with (`api-reads.ts:1078-1093 providerSnapshot`).
///
/// A session override replaces the base URL and the model. The CREDENTIAL follows the host rather than the
/// session: `configuredProviderHost` scopes the process key to one host, so a session pointed somewhere
/// else never receives the deployment's key — which is the difference between "my own endpoint" and
/// "leaking the operator's secret to a third-party server". A session override that stored its own key
/// (held in memory only, never in the document) wins over either.
#[must_use]
pub fn effective_provider(
    settings: &HarnessRuntimeSettings,
    session: Option<&ProviderConfig>,
) -> ProviderConfig {
    let deployment = || ProviderConfig {
        base_url: settings.base_url.clone(),
        api_key: (!settings.api_key.is_empty()).then(|| settings.api_key.clone()),
        model: settings.model.clone(),
        headers: BTreeMap::new(),
        sampling: Some(default_sampling()),
    };

    let Some(session) = session.filter(|session| !session.base_url.trim().is_empty()) else {
        return deployment();
    };

    let same_host = provider_host_of(&session.base_url) == provider_host_of(&settings.base_url);
    let api_key = session.api_key.clone().or_else(|| {
        same_host
            .then(|| settings.api_key.clone())
            .filter(|key| !key.is_empty())
    });
    let model = if session.model.trim().is_empty() {
        settings.model.clone()
    } else {
        session.model.clone()
    };

    ProviderConfig {
        base_url: session.base_url.clone(),
        api_key,
        model,
        headers: session.headers.clone(),
        // The sampling is the SERVER's, never the caller's: a session cannot retune the model's
        // temperature over the wire (`toolbox.ts:245` is the only writer of this field).
        sampling: session
            .sampling
            .clone()
            .or_else(|| Some(default_sampling())),
    }
}
