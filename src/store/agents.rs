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

/// The `extra` key a session's OWN rank override rides on, for the same reason the guidance lines do:
/// [`SessionAgentEntry`] (`store/mod.rs:667`) is the durable binding record this build inherited from T1,
/// and it has no rank column. `extra` is a `#[serde(flatten)]` map, so the key is written and read
/// verbatim inside `core.sessionAgentOverrides` — which makes the rank ADDITIVE on the document: an older
/// file simply has no key, and a newer reader that ignores it loses nothing else. No
/// `HARNESS_DATABASE_VERSION` move and no `deny_unknown_fields` is involved
/// (`knowledge.md:303-310`, the tolerant-durable rule).
///
/// WHY THE OVERRIDE IS STORED RATHER THAN DERIVED: the contract lets a session rank one member for itself
/// without touching the harness definition (`harness-api.ts:1932-1942` POST, `:1956-1962` PATCH), and the
/// frozen definition's map is the rung the row falls back to (`api-reads.ts:1564`). Both have to survive a
/// restart independently, so they are stored in two places on purpose: the override here, the template's
/// map in `record.definition.ranks`.
pub const EXTRA_RANK: &str = "rank";

/// The SESSION'S OWN rank for one binding, or `None` when this session never named one
/// ([`read::session_agent_rank`] is the caller that resolves the ladder).
///
/// TOLERANT BY CONSTRUCTION, exactly like the other `extra` reads in this module: a document a hand-edit
/// left with `{"rank": "leader"}` or `{"rank": 3}` reads as "no override" and falls through to the
/// definition's letter, because a durable file is never a reason to fail a boot
/// (`store::restore_session_agents` defaults anything it cannot parse).
#[must_use]
pub fn entry_rank(entry: Option<&SessionAgentEntry>) -> Option<contract::AgentRank> {
    entry?
        .extra
        .get(EXTRA_RANK)
        .and_then(|value| value.as_str())
        .and_then(contract::agent_rank_of_text)
}

/// States one binding's session-level rank ([`EXTRA_RANK`], the write half of [`entry_rank`]).
///
/// The value is written as the LETTER STRING rather than through serde, so the durable key is the same
/// spelling every other rank surface uses and a reader that only string-compares it still works.
pub fn set_entry_rank(entry: &mut SessionAgentEntry, rank: contract::AgentRank) {
    entry.extra.insert(
        EXTRA_RANK.to_owned(),
        Value::String(rank.as_str().to_owned()),
    );
}

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

    /// One member's PRESET HISTORY: the band it actually seeds, plus which layer wrote it
    /// (`getSessionAgentPrefix`, `api.ts:4449-4510`).
    ///
    /// READ-ONLY by itself; the write is `update_session_agent`'s `prefix` arm, and the three arms that
    /// write lands in are what make THIS answer decidable: a band the session named, `null` which restates
    /// the definition's own band, and `[]` which is a real choice that reads back as `session`. The route
    /// has no way to ask the live runtime what a turn will seed, and it should not have to - the record IS
    /// the answer, which is the same reason TS reads it off the record's preset copy (`api.ts:4478`).
    ///
    /// # Errors
    ///
    /// [`ApiError::not_found`] for an unknown session (checked FIRST, exactly as TS's `requireSession`
    /// runs before any membership test), or for a name the FROZEN definition roster does not carry - a
    /// preset history is a definition statement, so an agent this conversation merely bound at runtime
    /// is a 404, "never an empty band pretending to be a member" (`api.ts:4455-4458`).
    pub async fn session_agent_prefix(
        &self,
        id: &str,
        agent_id: &str,
    ) -> Result<contract::SessionPrefixTurnsResponse, ApiError> {
        let inner = self.lock();
        let state = inner
            .session(id)
            .ok_or_else(|| store::session_not_found(id))?;

        let definition = state.definition();

        if !definition.agents.iter().any(|member| member == agent_id) {
            return Err(unknown_agent(id, agent_id));
        }

        let record = &state.record;

        // THE SESSION'S OWN COPY, which is where a session-scoped band lives (`api.ts:4478`
        // `record.preset.agentPrefixes`). A session created on this build gets that copy from
        // `HarnessDescriptor::to_record`, so it starts byte-identical to the frozen definition - which is
        // what makes the `harness` answer below correct rather than accidental.
        let stored = record
            .preset
            .agent_prefixes
            .as_ref()
            .and_then(|bands| bands.get(agent_id));

        // THE FROZEN DEFINITION COPY, i.e. the state a `null` patch restores to. Never another harness's
        // map, and never the live registry entry (`api.ts:4479`).
        let seeded = record
            .definition
            .agent_prefixes
            .as_ref()
            .and_then(|bands| bands.get(agent_id))
            .cloned()
            .unwrap_or_default();

        // THE LIVE BAND. TS reads it off the running brain (`agentPrefixOf`, `api.ts:4461-4464`); this
        // port has no in-process brain, so the band the next turn seeds IS the session's stored override
        // when the record carries one, and the frozen definition's band otherwise. A member nothing seeds
        // answers `[]`, "which is also the honest value for `source: none`".
        let prefix = stored.cloned().unwrap_or_else(|| seeded.clone());

        // PROVENANCE, exactly the three-way rule `api.ts:4466-4503` states, and for the same reasons:
        //   - the band IS the definition's own when the live band matches the frozen one AND nothing
        //     stored contradicts it - a session write that happened to restate identical bytes is not a
        //     session choice, because no reader can observe a difference the model cannot see;
        //   - `none` is that same answer with nothing seeded at all;
        //   - `session` is a stored band the definition's copy is not - the documented "this conversation
        //     chose no preset history" when the stored band is EMPTY, which is why the empty case must not
        //     collapse into `none`.
        let matches_seeded = |band: &[contract::HarnessPrefixTurn]| band == seeded.as_slice();

        let source = if matches_seeded(&prefix) && stored.is_none_or(|band| matches_seeded(band)) {
            if seeded.is_empty() {
                contract::SessionPrefixSource::None
            } else {
                contract::SessionPrefixSource::Harness
            }
        } else if stored.is_some() {
            contract::SessionPrefixSource::Session
        } else if prefix.is_empty() && seeded.is_empty() {
            // Nothing stored, and the live band is not the definition's either - in TS a band written
            // straight into the brain by a session-scoped bind, which is still a session statement. With
            // no band on either side, `none` is the only honest answer.
            contract::SessionPrefixSource::None
        } else {
            contract::SessionPrefixSource::Session
        };

        Ok(contract::SessionPrefixTurnsResponse {
            session_id: record.id.clone(),
            agent_id: agent_id.to_owned(),
            prefix,
            source,
        })
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

        let mut entry = SessionAgentEntry {
            agent_id: name.to_owned(),
            profile: request.profile.clone(),
            origin: BindingOrigin::Session,
            // THE LEGACY COLUMN, and it is written for the durable shape only. `role` left the contract on
            // the rank migration (see [`crate::contract::AgentRole`]), so nothing projects it any more; a
            // binding this build creates states the neutral value, and a document written before the
            // migration keeps whatever its writer said without changing any read.
            role: AgentRole::Subagent,
            model: request
                .model
                .clone()
                .filter(|model| !model.trim().is_empty()),
            instruction: None,
            created_at,
            extra,
        };

        // THE RANK THE CALLER ASKED FOR, and only that. An omitted or `null` rank stores NOTHING rather than
        // a guess: `harness-api.ts:1932-1937` calls that arm "asks for NO authority", and the row then
        // resolves through the definition's map to the lowest letter
        // ([`read::session_agent_rank`]). TS records a resolved figure here because it reads one back off
        // the live harness (`sessions.ts:7815-7822`); this build stores the REQUEST and resolves it on read,
        // which keeps the record from claiming an authority no runtime enforces.
        if let Some(rank) = request.rank {
            set_entry_rank(&mut entry, rank);
        }

        state.session_agents.insert(name.to_owned(), entry);

        // U6: THE BAND THIS BINDING SEEDS (`sessions.ts:7752-7760`, the `prefix` arm of
        // `applyBindingMaps`). A bind writes it into the SESSION's own preset copy, never into the frozen
        // definition's map, for the reason the PATCH write gives: `agentPrefixes` is the seed source a
        // restart rebuilds from, so it must say what this session actually seeds. An unstated - or `null` -
        // band writes NOTHING here, which is what keeps `GET .../prefix` able to answer `source: harness`
        // for a member that was merely bound (`harness-api.ts:1927-1931`).
        //
        // THE SAME VALIDATOR, called twice as TS calls it twice: once at the HTTP edge (`api.ts:4238`) and
        // once here (`sessions.ts:8166`'s store half), so a bind that reached this point by another path
        // cannot land a malformed band in the durable preset. The label is the bare `` `prefix` `` because a
        // bind names the member being created, exactly as TS's POST call passes no member.
        if let Some(band) = request.prefix.as_ref().filter(|band| !band.is_null()) {
            let band = crate::registry::require_prefix_turns(band, "prefix", name)?;

            state
                .record
                .preset
                .agent_prefixes
                .get_or_insert_default()
                .insert(name.to_owned(), band);
        }

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
        // THE FRAME (`api.ts:4280-4282`): a roster write publishes `agent.registry_updated` — the ONLY
        // kind in the 16-kind vocabulary that names the `session_agents` counter
        // (`harness-stream.ts` `HARNESS_WS_UPDATE_KINDS`, `protocol.ts:157` `HARNESS_WS_KIND_RESOURCES`).
        // A kind outside that set is dropped by `is_kind_published_to` on EVERY scope
        // (`stream/broker.rs::matches`), which is how an earlier revision of this file published
        // `session_agents.updated`: the counter moved, the REST read saw it, and no client ever did.
        // The `session` target travels too, because the reference's call names both resources
        // (`[{resource: session_agents, id}, {resource: session, id}]`), and the payload is the
        // reference's invalidation receipt — `{agent_id, action: 'added'}` — not the whole row: the
        // swagger's `WsUpdateAgentRegistry` is PURE INVALIDATION (`additionalProperties: false`).
        outcome.events.push(roster_update(
            id,
            true,
            serde_json::json!({ "agent_id": name, "action": "added" }),
        ));

        let revision = commit_roster(self, &mut inner, id, &before, &mut outcome)?;

        Ok(serde_json::json!({
            "agent": agent,
            "revision": revision,
        }))
    }

    /// Patches one binding's model, instruction, or preset-history band
    /// (`PATCH /sessions/:id/agents/:agentId`).
    ///
    /// An omitted property is no change, a string REPLACES the field, and an explicit `null` clears the
    /// session override and RESTORES the inherited value (`harness-api.ts:988`).
    ///
    /// THE PREFIX WRITE IS IDLE-GATED and the two halves of that sentence are the whole reason this method
    /// looks the way it does (`sessions.ts:8136-8210 setSessionAgentPrefix`): a band is what the brain
    /// SEEDS the next turn with, so the run that is already talking is entitled to finish with the band it
    /// was given, and the write waits. TS gates it at the same point as a binding change
    /// (`assertBindingIdle(session, "change an agent prefix in a session")`), and for the same pair of
    /// reasons - an in-flight turn resolves its team from the live harness, and a band that moved
    /// underneath it would rewrite what the model actually saw.
    ///
    /// WHY THE GATE STILL SITS AFTER THE BODY READING, unlike a tool switch that reads its body after the
    /// gate: the route has ALREADY refused a malformed band with 400 before calling here
    /// (`api.ts:4052-4077`), so the only way this method could reach a validation refusal is a body that
    /// changed between the two, and an accepted patch commits ONE revision and ONE document for the model,
    /// instruction and prefix halves together.
    ///
    /// `route` is not decoration: the two routes that reach this one method publish DIFFERENT counters.
    /// The binding PATCH bumps and reports `session_agents` AND `session` (`api.ts:4141-4147` /
    /// `:4161-4167`), because a model, band or rank change moves what the session runs; the
    /// instruction-subresource PATCH bumps and reports `session_agents` ALONE
    /// (`api.ts:4361-4368`), because an override retunes a seed inside the roster without touching the
    /// session row. Publishing the session counter the reference never moved would hand a stream client a
    /// revision it cannot find on the REST read of the same resource.
    pub async fn update_session_agent(
        &self,
        id: &str,
        agent_id: &str,
        body: &Value,
        route: AgentPatchRoute,
    ) -> Result<Value, ApiError> {
        let request: UpdateSessionAgentRequest = serde_json::from_value(body.clone())
            .map_err(|error| ApiError::bad_request(format!("Invalid request body: {error}")))?;

        if request.model.is_none()
            && request.instruction.is_none()
            && request.rank.is_none()
            && request.prefix.is_none()
        {
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

        // THE RANK GATES, and they run AHEAD of the band's gate because TS sequences the rank write ahead of
        // every other half of this patch (`api.ts:4101-4110`: "RANK comes first, before the other fields, so
        // a rank the session cannot take ... refuses the whole patch before any override is written"). A body
        // that is half rank and half override therefore commits nothing when the rank is refused, which is the
        // one ordering difference a client can observe between this route and a model-only patch.
        //
        // 1. THE IDLE GATE, under the rank's OWN operation name, so the sentence is TS's
        //    (`sessions.ts:8234`, `assertBindingIdle(session, 'change an agent rank in a session')`): a run in
        //    flight is already delegating under the map this write replaces.
        if request.rank.is_some() && state.busy_or_pending() {
            return Err(session_busy_error(id, "change an agent rank in a session"));
        }

        // 2. THE STARTED-CONVERSATION GATE (`sessions.ts:8253-8262`), the contract's own words for it
        //    (`harness-api.ts:1956-1962`): "A `rank` patch changes the authority map IN PLACE and is accepted
        //    only before the session's first thread starts - a saved conversation keeps the ranks its harness
        //    template defines and answers 409, because its delegation graph was built from that map." A model
        //    or instruction override stays writable after the first turn precisely because it moves no
        //    authority, so this refusal belongs to the rank arm alone.
        if request.rank.is_some() && conversation_started(state) {
            return Err(ApiError::conflict(
                "A saved conversation keeps the ranks its harness template defines; start a new session to \
                 change them."
                    .to_owned(),
            ));
        }

        // THE GATE, and it asks about this PATCH rather than the roster: a band moves no member, yet a run
        // in flight is already seeding turns from the band this write is about to replace
        // (`sessions.ts:8141`). A prefix-only patch therefore refuses exactly when a binding change would,
        // and a model-only patch still never waits - the gate runs only when a band is actually stated, so
        // `PATCH .../agents/x {"model": "m"}` keeps answering on a busy session (`api.ts:4106-4146`).
        if request.prefix.is_some() && state.busy_or_pending() {
            return Err(session_busy_error(
                id,
                "change an agent prefix in a session",
            ));
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

        // THE AUTHORITY WRITE (`sessions.ts:8232-8273 setSessionAgentRank`), and it lands on the SESSION's
        // binding entry rather than on the definition copy: `applyBindingMaps` writes the rank into the
        // session's own map, the frozen definition keeps the map the harness was built with, and the read
        // resolves override-first (`api-reads.ts:1564`). Because the override dies with the binding, a
        // `DELETE` takes the session's rank back with it exactly as TS's `removed` arm drops the key
        // ("A REMOVED member's rank goes with it", `sessions.ts:7391-7394`).
        //
        // PRESENCE IS READ BEFORE THE WRITES: the arms below move the owned values out of `request`, and
        // the field list the update frame reports describes the REQUEST (which keys were sent), so the
        // four flags are captured while the fields are still whole (`api.ts:4096-4160`).
        let sent_rank = request.rank.is_some();
        let sent_instruction = request.instruction.is_some();
        let sent_prefix = request.prefix.is_some();
        let sent_model = request.model.is_some();

        if let Some(rank) = request.rank {
            set_entry_rank(&mut entry, rank);
        }

        if let Some(model) = request.model {
            // `Some(None)` is an explicit null: drop the override and inherit again.
            entry.model = model.filter(|model| !model.trim().is_empty());
            entry.instruction = entry.instruction.clone();
        }

        if let Some(instruction) = request.instruction {
            entry.instruction = instruction.filter(|text| !text.trim().is_empty());
        }

        // THE PRESET-HISTORY WRITE (`sessions.ts:8136-8210 setSessionAgentPrefix`).
        //
        // THE KEY IS RESTATED, NEVER DELETED, EVEN FOR `null`. `record.preset.agent_prefixes` is the map
        // the next harness start rebuilds this session's seeds from, so emptying it would rebuild the
        // member WITHOUT the band the session now holds while the frozen definition's copy survived as
        // display metadata only - the map has to say what the session actually seeds, which is also the
        // only way the prefix READ's provenance ladder stays decidable from the record alone.
        //
        // `null` therefore writes the DEFINITION's own band (`sessions.ts:8157-8163`), not "nothing" and
        // not `[]`: after an undo the read answers `source: harness`, because the stored band is
        // byte-identical to the seeded one - the honest answer, since the conversation is back to what it
        // was given. An empty array stays an empty array, and reads back as `source: session`.
        if let Some(prefix) = request.prefix {
            // TS's store half asks the DEFINITION roster (`sessions.ts:8146`), which is narrower than the
            // binding check above and is the same membership rule the prefix READ applies. A band is a
            // SEED, so a member this conversation only bound at runtime has no seed to rewrite.
            if !state
                .definition()
                .agents
                .iter()
                .any(|member| member == agent_id)
            {
                return Err(unknown_agent(id, agent_id));
            }

            // THE BAND THIS WRITE SEEDS, resolved BEFORE anything moves (`sessions.ts:8153-8171`).
            let band = if prefix.is_null() {
                state
                    .record
                    .definition
                    .agent_prefixes
                    .as_ref()
                    .and_then(|bands| bands.get(agent_id))
                    .cloned()
                    .unwrap_or_default()
            } else {
                // THE SAME VALIDATOR, called twice exactly as TS calls it twice - once at the HTTP edge
                // and once here (`sessions.ts:8166`) - so a band that reached this point by another path
                // cannot land in the durable preset unvalidated, and both callers answer one shape of
                // 400 naming the turn index.
                crate::registry::require_prefix_turns(&prefix, "prefix", agent_id)?
            };

            state
                .record
                .preset
                .agent_prefixes
                .get_or_insert_default()
                .insert(agent_id.to_owned(), band);
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

        // THE FIELD LIST THE FRAME REPORTS, in the reference's own push order — rank, instruction,
        // prefix, then model last (`api.ts:4096-4160`: the model half is applied AFTER the other three and
        // is pushed last even when the body spelled it first). A client uses it to decide which pane to
        // reread, so an invented order is a wire change.
        let mut fields: Vec<&str> = Vec::new();

        if sent_rank {
            fields.push("rank");
        }

        if sent_instruction {
            fields.push("instruction");
        }

        if sent_prefix {
            fields.push("prefix");
        }

        if sent_model {
            fields.push("model");
        }

        let mut outcome = store::CommitOutcome::default();
        outcome
            .bumps
            .push(("session_agents".to_owned(), Some(id.to_owned())));

        // The session counter moves only on the binding route (`api.ts:4141-4143` for the PATCH's
        // no-model half and `:4161-4163` for its model half, against the single-counter
        // `setSessionAgentInstruction` at `:4361`).
        if route == AgentPatchRoute::Binding {
            outcome
                .bumps
                .push(("session".to_owned(), Some(id.to_owned())));
        }

        outcome.events.push(roster_update(
            id,
            route == AgentPatchRoute::Binding,
            serde_json::json!({ "agent_id": agent_id, "fields": fields }),
        ));

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

        if agent_id == definition.entry {
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
        // THE FRAME (`api.ts:4321-4325`): a DETACH is a roster write like a bind is, so it publishes the
        // same kind with `action: 'removed'`. An earlier revision of this file bumped the counters and
        // published nothing at all here, which left a client holding a roster it had no way to learn was
        // gone short of rereading the whole page.
        //
        // The `threads` bump above stays OUT of the frame's targets: the reference's call names only
        // `session_agents` and `session` (`api.ts:4322-4324`), so a frame reports exactly the counters
        // the reference's frame reports.
        outcome.events.push(roster_update(
            id,
            true,
            serde_json::json!({ "agent_id": agent_id, "action": "removed" }),
        ));
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

/// WHICH route drove an agent-binding patch, i.e. which of the reference's two store halves ran.
///
/// The distinction survives down here because the reference publishes DIFFERENT counters per route: the
/// binding PATCH (`api.ts:4035-4175` `patchSessionAgent`, the two publish sites at `:4143` and `:4164`)
/// bumps and reports `session_agents` AND `session`, while the instruction subresource's PATCH
/// (`api.ts:4343-4370` `patchSessionAgentInstruction`, publish at `:4365`) bumps and reports
/// `session_agents` alone. Both verbs write through this crate's one store method, so the route has to be
/// carried in or the frame would move a counter the reference never moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentPatchRoute {
    /// `PATCH /sessions/:id/agents/:agentId` — the binding itself.
    Binding,
    /// `PATCH /sessions/:id/agents/:agentId/instruction` — the instruction subresource.
    Instruction,
}

/// One roster-change frame: the kind the contract publishes for the `session_agents` counter, addressed
/// at the resources the reference's own `publish(...)` call names for that operation.
///
/// `with_session` carries the reference's second resource, `{resource: session, id}`, which three of the
/// four roster writes name (`api.ts:4280`, `:4323`, `:4143`/`:4164`) and one deliberately omits
/// (`:4365`). The kind is `agent.registry_updated` — the only update kind whose counter set is
/// `session_agents` (`protocol.ts:157` `HARNESS_WS_KIND_RESOURCES`, ported at
/// `stream::protocol::kind_resources`) and one of the two kinds the `session` scope publishes
/// (`protocol.ts:72-88`). Anything else is not in the 16-kind vocabulary
/// (`harness-stream.ts:62 HARNESS_WS_UPDATE_KINDS`) and is therefore dropped on every scope by
/// `stream::protocol::is_kind_published_to` before a client can see it.
#[must_use]
fn roster_update(id: &str, with_session: bool, payload: Value) -> store::HubEmit {
    let mut targets = vec![("session_agents".to_owned(), Some(id.to_owned()))];

    if with_session {
        targets.push(("session".to_owned(), Some(id.to_owned())));
    }

    store::HubEmit {
        kind: "agent.registry_updated".to_owned(),
        targets,
        thread_id: None,
        turn_id: None,
        payload,
        ops: Vec::new(),
    }
}

/// Whether this session has STARTED the conversation its rank map was built for
/// (`sessions.ts:8253-8258`): any turn at all, or any conversation a reader started.
///
/// TS's predicate is `session.turns.length > 0 || session.harness.threads().some((thread) => thread.origin
/// === 'host')`, and the second half is the whole distinction: a member's OWN default conversation
/// (`agent:<name>`) travels with the roster and says nothing about whether anyone has used the session,
/// while a `host` conversation is the independent one the reader opened - "its presence is the moment this
/// stops being a fresh session". A fresh session therefore has neither, which is exactly when
/// [`Self::update_session_agent`] accepts a rank.
///
/// ONE STATED DIFFERENCE, because it is the only place this predicate is looser than TS's: TS skips
/// SOFT-DELETED rows when it asks the harness for its threads, and this build reads its own thread map
/// whole. The difference is unreachable in practice for this gate — a conversation soft-deleted after a run
/// leaves the session's turn list non-empty, which is the first half of the same `||` — so the looser read
/// cannot let a rank through that TS would refuse.
#[must_use]
fn conversation_started(state: &SessionState) -> bool {
    !state.turn_order.is_empty()
        || state
            .threads
            .values()
            .any(|thread| thread.origin == store::origin_str(crate::contract::ThreadOrigin::Host))
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
        // THE LETTER, through the SAME ladder the list route uses, so a single-entry row and the roster can
        // never report two different authorities for one member (R1).
        rank: read::session_agent_rank(&entry.agent_id, Some(entry), &state.definition()),
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
        deletable: entry.agent_id != state.definition().entry,
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
