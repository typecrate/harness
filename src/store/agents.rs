//! The session-scoped agent roster: binding, patching, detaching, and the instruction ladder.
//!
//! TypeScript origin: `packages/underload/harness/src/server/api.ts`'s `/sessions/:id/agents` routes and
//! the ladder `api-reads.ts:939-990 sessionAgentRowsOf` reads through. The wire rows themselves are
//! [`read::session_agent_rows_of`]; this module owns the WRITE half and the two questions every read asks:
//! which MODEL does this agent run with, and which INSTRUCTION text does it run with.
//!
//! WHY THE LADDER LIVES ON [`SessionState`]
//! ---------------------------------------
//! Both answers are pure functions of the session's own state plus the trusted catalogue, so a read model,
//! a run's tool policy and a wire row all resolve them the same way. Keeping one implementation is what
//! stops a `GET` from promising a model the next turn does not use.
//!
//! The four instruction layers, strongest first (`harness-api.ts:1010 SessionInstructionSource`):
//! `session` (the override created in THIS session) > `harness` (the frozen definition of the harness this
//! session was built from) > `global` (the operator's catalogue override) > `catalogue` (the shipped
//! profile). A restore (`PATCH` with `instruction: null`) drops the strongest applicable layer and falls
//! back through the rest, which is why `restorable` on a wire row means "some layer below this one exists".

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::agents::AgentCatalogue;
use crate::contract::{
    self, AgentRole, BindingOrigin, CreateSessionAgentRequest, InstructionSource,
    SessionAgentBinding, SessionAgentListResponse, SessionInstructionSource,
    SessionInstructionTextResponse, UpdateSessionAgentRequest,
};
use crate::error::ApiError;
use crate::events::EventHub;
use crate::store::{
    self, SessionAgentEntry, SessionState, Store, StoreInner, read, session_busy_error,
};

/// Bindings one session can hold, including the definition's own members (`harness-api.ts:954`: the list
/// envelope is unpaged because the roster is bounded).
pub const MAX_SESSION_AGENTS: usize = 64;

/// The `extra` key a session binding's guidance lines ride on. T1's `SessionAgentEntry` shape has no
/// column for them, and the contract's row only COUNTS them (`guidelinesCount`), so they live here.
pub const EXTRA_GUIDELINES: &str = "guidelines";

/// The roster entry a request is about, with its budget already checked.
#[must_use]
fn roster_full_error(session_id: &str) -> ApiError {
    ApiError::conflict(format!(
        "Session \"{session_id}\" cannot bind more than {MAX_SESSION_AGENTS} agents."
    ))
}

/// The refusal a roster route gives an agent the session does not run (`api.ts:2404`).
#[must_use]
pub fn unknown_agent(session_id: &str, agent: &str) -> ApiError {
    ApiError::not_found(format!(
        "Agent \"{agent}\" is not a member of session \"{session_id}\"."
    ))
}

/// The guideline lines a binding carries, in order, ignoring anything that is not a string.
#[must_use]
pub fn guidelines_of(entry: Option<&SessionAgentEntry>) -> Vec<String> {
    entry
        .and_then(|entry| entry.extra.get(EXTRA_GUIDELINES))
        .and_then(Value::as_array)
        .map(|lines| {
            lines
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

impl Store {
    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    /// The live roster (`GET /sessions/:id/agents`, `api.ts:2378`).
    pub async fn session_agents(&self, id: &str) -> Result<Value, ApiError> {
        let inner = self.lock();
        let state = inner
            .session(id)
            .ok_or_else(|| store::session_not_found(id))?;

        let agents = read::session_agent_rows_of(state, self.hub(), inner.agents());
        let total = u64::try_from(agents.len()).unwrap_or(u64::MAX);

        let response = SessionAgentListResponse {
            session_id: id.to_owned(),
            total,
            agents,
            revision: self.revision_of(id),
        };

        Ok(serde_json::to_value(response).unwrap_or(Value::Null))
    }

    /// The instruction one member actually runs with
    /// (`GET /sessions/:id/agents/:agentId/instruction`, `api.ts:2404`).
    pub async fn session_agent_instruction(
        &self,
        id: &str,
        agent_id: &str,
    ) -> Result<Value, ApiError> {
        let inner = self.lock();
        let state = inner
            .session(id)
            .ok_or_else(|| store::session_not_found(id))?;

        if !state.roster().iter().any(|member| member == agent_id) {
            return Err(unknown_agent(id, agent_id));
        }

        let (instruction, source, guidelines) = state.instruction_for(agent_id, inner.agents());

        let response = SessionInstructionTextResponse {
            session_id: id.to_owned(),
            agent_id: agent_id.to_owned(),
            source,
            // `chars` counts UTF-16 code units, exactly like TypeScript's `instruction.length`
            // (`api.ts:2803`, `:2831`); `projector::display_chars` is this crate's one helper for that
            // unit, so the two spellings cannot drift apart (`knowledge.md` "T5a", deviation 4).
            chars: u64::try_from(crate::projector::display_chars(&instruction)).unwrap_or(u64::MAX),
            instruction,
            guidelines,
        };

        Ok(serde_json::to_value(response).unwrap_or(Value::Null))
    }

    // ------------------------------------------------------------------
    // Writes
    // ------------------------------------------------------------------

    /// Binds an existing trusted profile into this session (`POST /sessions/:id/agents`, `api.ts:2386`).
    ///
    /// No implementation, tool list or transport is accepted, which is what makes this route unable to
    /// inject code (`harness-api.ts:974`).
    pub async fn create_session_agent(&self, id: &str, body: &Value) -> Result<Value, ApiError> {
        let request: CreateSessionAgentRequest = serde_json::from_value(body.clone())
            .map_err(|error| ApiError::bad_request(format!("Invalid request body: {error}")))?;

        let name = request.name.trim();
        if name.is_empty() {
            return Err(ApiError::bad_request(
                "An agent binding requires a `name`.".to_owned(),
            ));
        }

        // The profile must be a REAL catalogue profile: a session can point an agent at a trusted
        // definition, never at a made-up one. The CURRENT catalogue (`api.ts:1707` reads the live
        // `agentState`), fetched before the guard so the borrow below stays single.
        let catalogue = self.agents();

        if catalogue.effective(&request.profile).is_none() {
            return Err(ApiError::bad_request(format!(
                "Unknown agent profile \"{}\".",
                request.profile
            )));
        }

        let mut inner = self.lock();
        let state = inner
            .session_mut(id)
            .ok_or_else(|| store::session_not_found(id))?;

        let roster = state.roster();
        if roster.iter().any(|member| member == name) {
            return Err(ApiError::conflict(format!(
                "Agent \"{name}\" is already bound in session \"{id}\"."
            )));
        }

        // A run that has already started reads its tool policy and instruction from the roster, so a
        // binding change waits for the session to settle (`sessions.ts:4253`, the shared idle gate).
        if state.busy_or_pending() {
            return Err(session_busy_error(id, "change an agent binding"));
        }

        if u64::try_from(roster.len() + 1).unwrap_or(u64::MAX) > MAX_SESSION_AGENTS as u64 {
            return Err(roster_full_error(id));
        }

        let before = self.snapshot_state(state);
        let created_at = store::now_ms();
        let role = request.role.unwrap_or(AgentRole::Subagent);
        let mut extra = Map::new();

        if !request
            .instruction
            .as_deref()
            .unwrap_or_default()
            .is_empty()
        {
            // A binding may carry guidance WITHOUT an identity override; the ladder keeps them apart so
            // `instruction: null` restores the identity and leaves the guidance in place.
            extra.insert(
                EXTRA_GUIDELINES.to_owned(),
                Value::Array(
                    request
                        .instruction
                        .as_deref()
                        .unwrap_or_default()
                        .lines()
                        .map(str::trim)
                        .filter(|line| !line.is_empty())
                        .map(|line| Value::String(line.to_owned()))
                        .collect(),
                ),
            );
        }

        state.session_agents.insert(
            name.to_owned(),
            SessionAgentEntry {
                agent_id: name.to_owned(),
                profile: request.profile.clone(),
                origin: BindingOrigin::Session,
                role,
                model: request
                    .model
                    .clone()
                    .filter(|model| !model.trim().is_empty()),
                instruction: None,
                created_at,
                extra,
            },
        );

        // The roster IS part of the definition the session runs, so the definition's members grow with it.
        let mut definition = state.definition();
        definition.agents.push(name.to_owned());
        state.record.definition = store::descriptor_record_of(&definition);

        let agents = read::session_agent_rows_of(state, self.hub(), &catalogue);
        let agent = agents
            .into_iter()
            .find(|row| row.agent_id == name)
            .ok_or_else(|| unknown_agent(id, name))?;

        let mut outcome = store::CommitOutcome::default();
        outcome
            .bumps
            .push(("session_agents".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("session".to_owned(), Some(id.to_owned())));
        outcome.events.push(store::HubEmit {
            kind: "session_agents.updated".to_owned(),
            targets: vec![("session_agents".to_owned(), Some(id.to_owned()))],
            thread_id: None,
            turn_id: None,
            payload: serde_json::to_value(&agent).unwrap_or(Value::Null),
            ops: Vec::new(),
        });

        let revision = commit_roster(self, &mut inner, id, &before, &mut outcome)?;

        Ok(serde_json::json!({
            "agent": agent,
            "revision": revision,
        }))
    }

    /// Patches one binding's model or instruction (`PATCH /sessions/:id/agents/:agentId`).
    ///
    /// An omitted property is no change, a string REPLACES the field, and an explicit `null` clears the
    /// session override and RESTORES the inherited value (`harness-api.ts:988`).
    pub async fn update_session_agent(
        &self,
        id: &str,
        agent_id: &str,
        body: &Value,
    ) -> Result<Value, ApiError> {
        let request: UpdateSessionAgentRequest = serde_json::from_value(body.clone())
            .map_err(|error| ApiError::bad_request(format!("Invalid request body: {error}")))?;

        if request.model.is_none() && request.instruction.is_none() {
            return Err(ApiError::bad_request(
                "A patch must change at least one field.".to_owned(),
            ));
        }

        // The roster rows are projected through the CURRENT catalogue, taken before the guard so the
        // mutable session borrow below stays the only one out of `inner`.
        let catalogue = self.agents();

        let mut inner = self.lock();
        let state = inner
            .session_mut(id)
            .ok_or_else(|| store::session_not_found(id))?;

        let bound = state.session_agents.contains_key(agent_id)
            || state.roster().iter().any(|member| member == agent_id);

        if !bound {
            return Err(unknown_agent(id, agent_id));
        }

        let before = self.snapshot_state(state);

        // A definition member with no binding entry yet still accepts an override, so the entry is
        // CREATED here with the definition's own profile.
        let mut entry = state
            .session_agents
            .get(agent_id)
            .cloned()
            .unwrap_or_else(|| {
                store::definition_binding(agent_id, AgentRole::Subagent, store::now_ms())
            });

        if let Some(model) = request.model {
            // `Some(None)` is an explicit null: drop the override and inherit again.
            entry.model = model.filter(|model| !model.trim().is_empty());
            entry.instruction = entry.instruction.clone();
        }

        if let Some(instruction) = request.instruction {
            entry.instruction = instruction.filter(|text| !text.trim().is_empty());
        }

        state
            .session_agents
            .insert(agent_id.to_owned(), entry.clone());

        // The live model map is what `PATCH /sessions/:id` and the run read, so a model override written
        // through the roster has to land there too.
        if let Some(model) = entry.model.clone() {
            state.set_agent_model(agent_id, &model);
        }

        let agents = read::session_agent_rows_of(state, self.hub(), &catalogue);
        let agent = agents
            .into_iter()
            .find(|row| row.agent_id == agent_id)
            .ok_or_else(|| unknown_agent(id, agent_id))?;

        let mut outcome = store::CommitOutcome::default();
        outcome
            .bumps
            .push(("session_agents".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("session".to_owned(), Some(id.to_owned())));
        outcome.events.push(store::HubEmit {
            kind: "session_agents.updated".to_owned(),
            targets: vec![("session_agents".to_owned(), Some(id.to_owned()))],
            thread_id: None,
            turn_id: None,
            payload: serde_json::to_value(&agent).unwrap_or(Value::Null),
            ops: Vec::new(),
        });

        let revision = commit_roster(self, &mut inner, id, &before, &mut outcome)?;

        Ok(serde_json::json!({
            "agent": agent,
            "revision": revision,
        }))
    }

    /// Detaches one binding (`DELETE /sessions/:id/agents/:agentId`).
    ///
    /// The conversations that agent owned are DETACHED, not deleted: a persistent conversation keeps its
    /// transcript and reports the removal, and a subagent conversation with no owner left stops
    /// (`api.ts:2456`).
    pub async fn delete_session_agent(&self, id: &str, agent_id: &str) -> Result<Value, ApiError> {
        let mut inner = self.lock();
        let state = inner
            .session_mut(id)
            .ok_or_else(|| store::session_not_found(id))?;

        let definition = state.definition();
        let roster = state.roster();

        if !roster.iter().any(|member| member == agent_id) {
            return Err(unknown_agent(id, agent_id));
        }

        if agent_id == definition.primary {
            return Err(ApiError::conflict(format!(
                "Agent \"{agent_id}\" is the entry point of session \"{id}\" and cannot be detached."
            )));
        }

        // A member with work in flight cannot go: its run would be left naming an agent the session no
        // longer runs.
        for thread_id in state.thread_ids() {
            let owns = state
                .thread(&thread_id)
                .is_some_and(|thread| thread.agent == agent_id);

            if owns && state.active_run_of(&thread_id).is_some() {
                return Err(ApiError::conflict(format!(
                    "Thread \"{thread_id}\" still has live work; wait for it to stop before deleting it."
                )));
            }
        }

        let before = self.snapshot_state(state);

        let owned: Vec<String> = state
            .thread_ids()
            .into_iter()
            .filter(|thread_id| {
                state
                    .thread(thread_id)
                    .is_some_and(|thread| thread.agent == agent_id)
            })
            .collect();

        for thread_id in &owned {
            if let Some(thread) = state.thread_mut(thread_id) {
                thread.status = store::lifecycle_str(contract::ThreadLifecycle::Stopped).to_owned();
            }
        }

        state.session_agents.remove(agent_id);
        state.agent_instructions.remove(agent_id);

        if let Some(models) = state.record.agent_models.as_mut() {
            models.remove(agent_id);
        }

        // The roster loses the name; the label tombstones deliberately stay, so a re-bound agent cannot
        // re-mint a retired label (`labels.rs:144`).
        let mut definition = state.definition();
        definition.agents.retain(|member| member != agent_id);
        state.record.definition = store::descriptor_record_of(&definition);

        let detached_threads = u64::try_from(owned.len()).unwrap_or(u64::MAX);
        let mut outcome = store::CommitOutcome::default();
        outcome
            .bumps
            .push(("session_agents".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("threads".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("session".to_owned(), Some(id.to_owned())));
        outcome.revision = self.revision_of(id);

        let revision = commit_roster(self, &mut inner, id, &before, &mut outcome)?;

        Ok(serde_json::json!({
            "deleted": true,
            "agentId": agent_id,
            "sessionId": id,
            "detachedThreads": detached_threads,
            "revision": revision,
        }))
    }
}

/// Commits one roster mutation through the guard the caller already holds: save the document, bump the
/// roster and session counters, publish the row, and REBUILD the session state from the pre-mutation
/// record if the write failed (`sessions.ts:2256-2270`).
///
/// The caller passes `inner` mutably so the rollback can replace the state; the borrow of the session
/// itself must be finished by then, which is why every route builds its payload first.
fn commit_roster(
    store: &Store,
    inner: &mut StoreInner,
    id: &str,
    before: &crate::persistence::HarnessSessionRecord,
    outcome: &mut store::CommitOutcome,
) -> Result<u64, ApiError> {
    store.commit(&*inner, id, outcome).inspect_err(|_error| {
        store.rollback_state(inner, id, before);
    })
}

/// The store-side entry projected onto its WIRE row, for a caller that has one entry and the roster budget
/// at hand (`api-reads.ts:939`).
///
/// The list route reads through [`read::session_agent_rows_of`] instead, because a row's `busy` and
/// `restorable` figures need the whole session; this is the single-entry form.
#[must_use]
pub fn binding_of(
    state: &SessionState,
    hub: &EventHub,
    catalogue: &AgentCatalogue,
    entry: &SessionAgentEntry,
) -> SessionAgentBinding {
    let (text, source, guidelines) = state.instruction_for(&entry.agent_id, catalogue);

    SessionAgentBinding {
        agent_id: entry.agent_id.clone(),
        profile: entry.profile.clone(),
        role: entry.role,
        origin: entry.origin,
        model: state.agent_model(&entry.agent_id),
        inherited_model: state
            .definition()
            .agent_models
            .get(&entry.agent_id)
            .cloned()
            .unwrap_or_else(|| state.record.settings.model.clone()),
        instruction_source: source,
        // `chars` is `instruction.length` (`api-reads.ts`'s roster row), i.e. UTF-16 code units.
        instruction_chars: u64::try_from(crate::projector::display_chars(&text))
            .unwrap_or(u64::MAX),
        has_instruction_override: matches!(
            source,
            SessionInstructionSource::Harness | SessionInstructionSource::Session
        )
        .then_some(true),
        guidelines_count: Some(u64::try_from(guidelines.len()).unwrap_or(u64::MAX)),
        editable: true,
        deletable: entry.agent_id != state.definition().primary,
        restorable: source != SessionInstructionSource::Catalogue,
        busy: read::is_agent_busy(state, &entry.agent_id).then_some(true),
        revision: hub.revision("session_agents", Some(&state.record.id)),
    }
}

/// Maps the four-layer SESSION provenance onto the three-layer CARD vocabulary.
///
/// `contract::InstructionSource` (`harness-api.ts:444`) has no `session` member, because the harness CARD
/// cannot state a decision one conversation made. A session override is reported to the card-level reader
/// as `harness`: both are "an override authored above the shipped profile", and the session layer stays
/// visible on the roster rows, which use `SessionInstructionSource` directly.
#[must_use]
pub fn turn_instruction_source(source: SessionInstructionSource) -> InstructionSource {
    match source {
        SessionInstructionSource::Catalogue => InstructionSource::Catalogue,
        SessionInstructionSource::Global => InstructionSource::Global,
        SessionInstructionSource::Harness | SessionInstructionSource::Session => {
            InstructionSource::Harness
        }
    }
}

/// The empty per-agent model map of a session with no overrides, which is what a fresh create starts with.
#[must_use]
pub fn empty_agent_models() -> BTreeMap<String, String> {
    BTreeMap::new()
}
