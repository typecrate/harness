//! The trusted agent catalogue plus the GLOBAL override layer.
//!
//! TypeScript origins, merged into one Rust type:
//!   * `src/agents/index.ts` - the frozen catalogue (`AGENT_PROFILES`, `AGENT_PROFILE_NAMES`,
//!     `agentProfile`, `agentInstruction`),
//!   * `src/server/harnesses/agent-state.ts` - the operator's overlay above it (`createAgentState`:
//!     `effective`, `isKnown`, `shipped`, `retired`, `overridesOf`, `records`, `patch`, `retire`,
//!     `restore`),
//!   * `src/server/api-reads.ts:1174-1222` - `agentSummaryOf` / `agentProfileOf`,
//!   * `src/server/api.ts:1705-1787` - the PATCH/DELETE rules this type's `update`/`delete` carry.
//!
//! WHY ONE TYPE AND NOT THREE
//! --------------------------
//! In TS the catalogue is a frozen module, the overlay is a per-registry closure, and the
//! projections are free functions - three places that have to agree about which profile is
//! selectable and which text it runs. The Rust port puts the catalogue, the overlay, and the two
//! projections behind one owner, so "effective profile" is computed in exactly one place, and a
//! retired profile can never be reachable through one accessor and hidden from another.
//!
//! SEMANTICS THAT MUST SURVIVE THE MERGE (`agent-state.ts:17-31`)
//! --------------------------------------------------------------
//!   * a stated field REPLACES the catalogue value for every FUTURE read;
//!   * an explicit `null` REMOVES the override, which restores the shipped value for that field
//!     only;
//!   * an override that equals the shipped value is DROPPED, so `records()` stays a record of
//!     decisions and a shipped-default improvement still lands on a profile nobody intentionally
//!     changed;
//!   * `retired` removes the profile from the SELECTABLE catalogue. It does not delete it: the
//!     durable record keeps its text overrides, existing sessions hold the agents they were built
//!     with, and a saved session restores from its own frozen definition.
//!
//! THE 409 THE CALLER OWNS
//! -----------------------
//! A DELETE that would strand a harness is refused with 409 by the HTTP layer (`api.ts` joins
//! `referencedBy` with the refusal), not here: this type cannot see the harness registry without
//! importing it, and the registry already asks THIS type which agents it may select.

pub mod profiles;

pub use profiles::AgentProfileDef;

use crate::contract::{
    AgentProfile, AgentSummary, DeleteAgentResult, InstructionSource, OverriddenAgentField,
    UpdateAgentRequest,
};
use crate::error::ApiError;
use crate::events::EventHub;
use crate::persistence::HarnessAgentRecord;
use crate::read;

/// Bounds and text rules mirror `harnesses/agent-state.ts:76-84` (a client must not be able to
/// write a 4 MB prompt into an agent profile while the harness name field is capped at 120).
pub const MAX_AGENT_TITLE_LENGTH: usize = 120;
pub const MAX_AGENT_DESCRIPTION_LENGTH: usize = 2_000;
pub const MAX_AGENT_INSTRUCTION_LENGTH: usize = 8_000;
pub const MAX_AGENT_GUIDELINES: usize = 64;
pub const MAX_AGENT_GUIDELINE_LENGTH: usize = 1_000;

/// The override fields one profile currently carries (`AgentOverrideFields`,
/// `agent-state.ts:64-70`). A field is present only when the operator stated it AND the stated
/// value differs from the shipped one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentOverrides {
    pub title: Option<String>,
    pub description: Option<String>,
    pub instruction: Option<String>,
    pub guidelines: Option<Vec<String>>,
}

impl AgentOverrides {
    /// `true` when nothing is stated, i.e. the profile is exactly the shipped one.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.description.is_none()
            && self.instruction.is_none()
            && self.guidelines.is_none()
    }

    /// The field names a global override currently replaces, in the wire order `agentProfileOf`
    /// enumerates them (`api-reads.ts:1207-1209`).
    #[must_use]
    pub fn replaced_fields(&self) -> Vec<OverriddenAgentField> {
        let mut fields = Vec::new();

        if self.title.is_some() {
            fields.push(OverriddenAgentField::Title);
        }

        if self.description.is_some() {
            fields.push(OverriddenAgentField::Description);
        }

        if self.instruction.is_some() {
            fields.push(OverriddenAgentField::Instruction);
        }

        if self.guidelines.is_some() {
            fields.push(OverriddenAgentField::Guidelines);
        }

        fields
    }
}

/// One profile's overlay entry: the stated fields plus the retirement tombstone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AgentOverlay {
    fields: AgentOverrides,
    retired: bool,
}

/// The catalogue of selectable agents: six compiled-in profiles, the operator's overlay, and the
/// retirement tombstones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCatalogue {
    /// The shipped profiles, in catalogue order. Never mutated by anything in this type.
    shipped: Vec<AgentProfileDef>,
    /// Insertion-ordered decision log, so `records()` - and therefore `agents.json` - does not
    /// reshuffle between restarts, and a later decision for the same id keeps its original
    /// position (`agent-state.ts:159-162`).
    overlays: Vec<(String, AgentOverlay)>,
}

impl Default for AgentCatalogue {
    fn default() -> Self {
        Self::builtin()
    }
}

impl AgentCatalogue {
    /// The compiled-in catalogue, with no overlay: the six profiles in `shipped_profiles()` order.
    #[must_use]
    pub fn builtin() -> Self {
        Self {
            shipped: profiles::shipped_profiles(),
            overlays: Vec::new(),
        }
    }

    /// Replays the durable overlay (`agents.json`) onto a catalogue (`createAgentState`'s
    /// `initialRecords` loop, `agent-state.ts:166-177`).
    ///
    /// Sets one entry per record WITHOUT clearing what is already there, which is the TS restore
    /// semantics for a freshly built catalogue: a record with no override and no tombstone states
    /// "the shipped profile" and replaces whatever the layer held for that id.
    pub fn apply_records(&mut self, records: Vec<HarnessAgentRecord>) {
        for record in records {
            let fields = AgentOverrides {
                title: record.title.clone(),
                description: record.description.clone(),
                instruction: record.instruction.clone(),
                guidelines: record.guidelines.clone(),
            };

            self.set_overlay(record.name.clone(), fields, record.retired == Some(true));
        }
    }

    /// The durable projection of every decision this catalogue holds, in first-decision order
    /// (`records`, `agent-state.ts:316-324`).
    #[must_use]
    pub fn records(&self) -> Vec<HarnessAgentRecord> {
        self.overlays
            .iter()
            .map(|(name, entry)| HarnessAgentRecord {
                name: name.clone(),
                title: entry.fields.title.clone(),
                description: entry.fields.description.clone(),
                instruction: entry.fields.instruction.clone(),
                guidelines: entry.fields.guidelines.clone(),
                retired: if entry.retired { Some(true) } else { None },
                extra: serde_json::Map::new(),
            })
            .collect()
    }

    /// The SHIPPED profile for one id, with no overlay applied - a receipt's baseline
    /// (`shipped`, `agent-state.ts:347-351`). Also the membership test a NEW selection needs: an
    /// id outside the shipped catalogue is unknown even when it is retired.
    #[must_use]
    pub fn shipped(&self, name: &str) -> Option<AgentProfileDef> {
        self.shipped
            .iter()
            .find(|profile| profile.name == name)
            .cloned()
    }

    /// The EFFECTIVE profile for one id - catalogue text with the global overlay merged - or `None`
    /// when the id is unknown or retired (`effective`, `agent-state.ts:203-213`).
    ///
    /// NO overlay is a normal state, not a miss: `agent-state.ts:212` serves `{ ...profile }` when
    /// `overlays.get(name)` is `undefined`, so the shipped text is what an untouched profile runs.
    #[must_use]
    pub fn effective(&self, name: &str) -> Option<AgentProfileDef> {
        let profile = self.shipped(name)?;
        let entry = self.overlay(name);

        if entry.is_some_and(|entry| entry.retired) {
            return None;
        }

        Some(match entry {
            Some(entry) => merge_profile(profile, &entry.fields),
            None => profile,
        })
    }

    /// Every SELECTABLE profile in catalogue order (`profiles`, `agent-state.ts:189-201`).
    #[must_use]
    pub fn selectable(&self) -> Vec<AgentProfileDef> {
        self.shipped
            .iter()
            .filter(|profile| self.effective(&profile.name).is_some())
            .map(|profile| {
                self.effective(&profile.name)
                    .unwrap_or_else(|| profile.clone())
            })
            .collect()
    }

    /// The SHIPPED catalogue names, in catalogue order.
    ///
    /// This is the list the "Known agents: ..." refusal enumerates (`agents/index.ts`
    /// `AGENT_PROFILE_NAMES` via `agentProfiles()`), NOT the selectable list: a retired profile is
    /// still a known identity, it is just no longer chooseable.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.shipped
            .iter()
            .map(|profile| profile.name.clone())
            .collect()
    }

    /// Whether one id is retired - known, durable, and out of the selectable catalogue
    /// (`retired`, `agent-state.ts:352`).
    #[must_use]
    pub fn is_retired(&self, name: &str) -> bool {
        self.overlay(name).is_some_and(|entry| entry.retired)
    }

    /// The override fields currently stated for one profile (`overridesOf`,
    /// `agent-state.ts:353-361`). An empty answer means "the shipped profile".
    #[must_use]
    pub fn overrides_of(&self, name: &str) -> AgentOverrides {
        self.overlay(name)
            .map_or_else(AgentOverrides::default, |entry| entry.fields.clone())
    }

    /// THE system prompt an agent actually runs (`agentInstruction`, `agents/index.ts:113-126`).
    ///
    /// ONE string, not two messages: `agent({ instruction })` seeds one `system` message per array
    /// entry, and several OpenAI-compatible gateways accept only one system turn - so the identity
    /// sentence and the guidelines are joined HERE, with a blank line between the sections and one
    /// `- ` bullet per non-blank guideline.
    #[must_use]
    pub fn composed_instruction(&self, name: &str) -> Option<String> {
        let profile = self.effective(name)?;

        let instruction = profile.instruction.trim();

        let guidelines: Vec<String> = profile
            .guidelines
            .iter()
            .map(|guideline| guideline.trim())
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect();

        if guidelines.is_empty() {
            return Some(instruction.to_string());
        }

        let bullets: Vec<String> = guidelines.iter().map(|line| format!("- {line}")).collect();

        Some(format!(
            "{instruction}\n\nGuidelines:\n{}",
            bullets.join("\n")
        ))
    }

    /// A trusted profile as a list row (`agentSummaryOf`, `api-reads.ts:1174-1187`). `None` for an
    /// unknown or retired id, which the list page filters out anyway.
    #[must_use]
    pub fn summary(&self, name: &str, hub: &EventHub) -> Option<AgentSummary> {
        read::agent_summary_of(self, name, hub)
    }

    /// The full trusted profile, with its instruction provenance (`agentProfileOf`,
    /// `api-reads.ts:1200-1222`).
    ///
    /// `referenced_by` is PASSED IN because only the harness registry knows which definitions
    /// select this profile (`api.ts:1659-1663`); this type must not reach back into it.
    #[must_use]
    pub fn profile(
        &self,
        name: &str,
        hub: &EventHub,
        referenced_by: Vec<String>,
    ) -> Option<AgentProfile> {
        read::agent_profile_of(self, name, hub, referenced_by)
    }

    /// Declarative text override of one trusted profile (`agentState.patch` + the `api.ts:1705`
    /// route rules). Never touches the shipped profile; affects FUTURE sessions only.
    ///
    /// A stated value REPLACES the override, an explicit `null` REMOVES it (which restores the
    /// shipped value for that field alone), and an omitted field leaves the override exactly as it
    /// was. An override that ends up equal to the shipped text is dropped, so the durable record
    /// stays a list of decisions.
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] `A patch must change at least one field.` for an empty body, or
    /// the per-field refusal text; [`ApiError::not_found`] for an unknown or retired id.
    pub fn update(
        &mut self,
        name: &str,
        request: UpdateAgentRequest,
        hub: &EventHub,
    ) -> Result<AgentProfile, ApiError> {
        let profile = self
            .shipped(name)
            .ok_or_else(|| ApiError::not_found(format!("Unknown agent \"{name}\".")))?;

        if self.is_retired(name) {
            return Err(ApiError::not_found(format!(
                "Agent \"{name}\" is retired; it is no longer part of the selectable catalogue."
            )));
        }

        // `api.ts:1716-1720`: a body that states none of the four fields changes nothing, so it
        // is a refusal rather than a no-op the client believes succeeded.
        if request.title.is_none()
            && request.description.is_none()
            && request.instruction.is_none()
            && request.guidelines.is_none()
        {
            return Err(ApiError::bad_request(
                "A patch must change at least one field.",
            ));
        }

        let mut fields = self
            .overlay(name)
            .map(|entry| entry.fields.clone())
            .unwrap_or_default();
        let retired = self.is_retired(name);

        // One rule per field: a stated value REPLACES the override, an explicit `null` REMOVES it
        // (which restores the shipped value below), and an omitted field leaves it exactly as it
        // was (`agent-state.ts:259-272`).
        //
        // Every field is validated into the LOCAL copy before anything is committed, which is how
        // `agent-state.ts:217-239` keeps a refused patch from half-applying: the catalogue below is
        // only written once, after the last `?`.
        if let Some(stated) = request.title {
            fields.title = match stated {
                Some(text) => Some(normalize_agent_text(
                    &text,
                    "title",
                    MAX_AGENT_TITLE_LENGTH,
                )?),
                None => None,
            };
        }

        if let Some(stated) = request.description {
            fields.description = match stated {
                Some(text) => Some(normalize_agent_text(
                    &text,
                    "description",
                    MAX_AGENT_DESCRIPTION_LENGTH,
                )?),
                None => None,
            };
        }

        if let Some(stated) = request.instruction {
            fields.instruction = match stated {
                Some(text) => Some(normalize_agent_text(
                    &text,
                    "instruction",
                    MAX_AGENT_INSTRUCTION_LENGTH,
                )?),
                None => None,
            };
        }

        if let Some(stated) = request.guidelines {
            fields.guidelines = match stated {
                Some(lines) => Some(normalize_agent_guidelines(&lines)?),
                None => None,
            };
        }

        // A decision that equals the shipped value is not a decision (`agent-state.ts:274-288`).
        if fields.title.as_deref() == Some(profile.title.as_str()) {
            fields.title = None;
        }

        if fields.description.as_deref() == Some(profile.description.as_str()) {
            fields.description = None;
        }

        if fields.instruction.as_deref() == Some(profile.instruction.as_str()) {
            fields.instruction = None;
        }

        if fields.guidelines.as_deref() == Some(profile.guidelines.as_slice()) {
            fields.guidelines = None;
        }

        if fields.is_empty() && !retired {
            self.remove_overlay(name);
        } else {
            self.set_overlay(name.to_string(), fields, retired);
        }

        // `agentProfileOf` reports the CURRENT revision; the route bumps `agents` and
        // `agents:<id>` before calling, and `events::publish` is what advances a counter here, so
        // reading is the honest answer (`api.ts:1739-1752`).
        Ok(read::agent_profile_of(self, name, hub, Vec::new())
            .unwrap_or_else(|| fallback_profile(profile, &self.overrides_of(name))))
    }

    /// Retires one profile - the DELETE route (`retire`, `agent-state.ts:296-312`).
    ///
    /// # Errors
    ///
    /// [`ApiError::not_found`] for an unknown id (`Unknown agent "x".`) or a repeat delete
    /// (`Agent "x" is already retired.`), which is the rule `api.ts:1755-1787` states for a
    /// second success.
    pub fn delete(&mut self, name: &str) -> Result<DeleteAgentResult, ApiError> {
        if self.shipped(name).is_none() {
            return Err(ApiError::not_found(format!("Unknown agent \"{name}\".")));
        }

        if self.is_retired(name) {
            return Err(ApiError::not_found(format!(
                "Agent \"{name}\" is already retired."
            )));
        }

        let fields = self
            .overlay(name)
            .map(|entry| entry.fields.clone())
            .unwrap_or_default();

        self.set_overlay(name.to_string(), fields, true);

        Ok(DeleteAgentResult {
            deleted: true,
            agent_id: name.to_string(),
            // The tombstone is durable in `<root>/harness/agents.json`, so a restart does NOT
            // resurrect the profile. An in-memory deployment answers `true`; the HTTP layer knows
            // whether a root was configured and flips this field there.
            tombstoned: true,
            restored_by_restart: false,
        })
    }

    // -- internals ----------------------------------------------------------

    fn overlay(&self, name: &str) -> Option<&AgentOverlay> {
        self.overlays
            .iter()
            .find(|(entry, _)| entry == name)
            .map(|(_, overlay)| overlay)
    }

    fn set_overlay(&mut self, name: String, fields: AgentOverrides, retired: bool) {
        if let Some((_, entry)) = self.overlays.iter_mut().find(|(entry, _)| *entry == name) {
            entry.fields = fields;
            entry.retired = retired;
            return;
        }

        self.overlays.push((name, AgentOverlay { fields, retired }));
    }

    fn remove_overlay(&mut self, name: &str) {
        self.overlays.retain(|(entry, _)| entry != name);
    }
}

/// `merge` (`agent-state.ts:179-187`): a stated field replaces the catalogue value, and a stated
/// `guidelines` list replaces the shipped bullets wholesale (`[]` means none).
#[must_use]
fn merge_profile(profile: AgentProfileDef, fields: &AgentOverrides) -> AgentProfileDef {
    let mut merged = profile;

    if let Some(title) = &fields.title {
        merged.title = title.clone();
    }

    if let Some(description) = &fields.description {
        merged.description = description.clone();
    }

    if let Some(instruction) = &fields.instruction {
        merged.instruction = instruction.clone();
    }

    if let Some(guidelines) = &fields.guidelines {
        merged.guidelines = guidelines.clone();
    }

    merged
}

/// Builds the profile the catalogue would have served even if the projection had nothing to say
/// (`update`'s fall-back path: a stated override can never make a profile unreadable, so the
/// merged value is what a receipt must carry).
#[must_use]
fn fallback_profile(profile: AgentProfileDef, fields: &AgentOverrides) -> AgentProfile {
    let merged = merge_profile(profile, fields);

    AgentProfile {
        name: merged.name,
        title: merged.title,
        description: merged.description,
        instruction: merged.instruction,
        guidelines: merged.guidelines,
        instruction_source: if fields.instruction.is_some() {
            InstructionSource::Global
        } else {
            InstructionSource::Catalogue
        },
        overridden_fields: if fields.is_empty() {
            None
        } else {
            Some(fields.replaced_fields())
        },
        referenced_by: Vec::new(),
        revision: 0,
    }
}

/// `normalizeText` (`agent-state.ts:101-111`): trim, reject blank, bound the length, and reject
/// every control character except tab/newline/CR.
///
/// WHY THE BLANK RULE MENTIONS `null`
/// ---------------------------------
/// A client that means "restore the shipped value" has a spelling for it (`null`); an empty
/// string is a mistake, and accepting it would silently drop a saved override while the UI still
/// showed the old text.
fn normalize_agent_text(value: &str, field: &str, max: usize) -> Result<String, ApiError> {
    let text = value.trim();

    if text.is_empty() {
        return Err(ApiError::bad_request(format!(
            "`agent.{field}` cannot be empty. Send null to restore the shipped value."
        )));
    }

    // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
    if text.encode_utf16().count() > max {
        return Err(ApiError::bad_request(format!(
            "`agent.{field}` cannot exceed {max} characters."
        )));
    }

    if !is_prose_text(text) {
        return Err(ApiError::bad_request(format!(
            "`agent.{field}` contains control characters."
        )));
    }

    Ok(text.to_string())
}

/// `normalizeGuidelines` (`agent-state.ts:116-122`): `[]` is the explicit clear the contract
/// allows, while a blank or oversized entry is a visible 400 rather than a silently dropped line.
fn normalize_agent_guidelines(lines: &[String]) -> Result<Vec<String>, ApiError> {
    if lines.len() > MAX_AGENT_GUIDELINES {
        return Err(ApiError::bad_request(format!(
            "`agent.guidelines` cannot exceed {MAX_AGENT_GUIDELINES} entries."
        )));
    }

    lines
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            normalize_agent_text(
                entry,
                &format!("guidelines[{index}]"),
                MAX_AGENT_GUIDELINE_LENGTH,
            )
        })
        .collect()
}

/// `PROSE_PATTERN` (`agent-state.ts:84`): tab, newline and CR are allowed - a role definition is
/// often multi-line - every other control character is rejected, as is DEL.
#[must_use]
fn is_prose_text(text: &str) -> bool {
    !text.chars().any(|glyph| {
        (glyph.is_ascii_control() && glyph != '\t' && glyph != '\n' && glyph != '\r')
            || glyph == '\u{7f}'
    })
}
