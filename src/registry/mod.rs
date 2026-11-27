//! The harness definition registry: shipped presets, operator edits, and durable tombstones.
//!
//! TypeScript origin: `packages/underload/harness/src/server/harnesses/registry.ts`. The live
//! half of that file - `create` factories, `liveFactory`, the scripted/flaky template rebuild -
//! belongs to the runtime stage, so what lands here is the DEFINITION plane: the catalogue, the
//! validation, the create/patch/delete rules, the tombstones, and the restore policy.
//!
//! INVARIANTS (`registry.ts:17-28`, restated for the rank pass)
//! ------------------------------------------------------------
//!   * `agents` is the client's own ROSTER in declaration order - it is derived from nothing, and
//!     neither `entry` nor `ranks` is derived from it;
//!   * `entry` is ROUTING (one member, defaulted from roster order) and `ranks` is AUTHORITY (one
//!     letter per member, resolved at WRITE time with the kernel's fill); several members may hold
//!     `A`, and a roster with one distinct letter has no delegation surface at all;
//!   * no roster name is duplicated, and every name must be a known TRUSTED agent id;
//!   * names are unique case-insensitively, and a client may not take a built-in name or a legacy
//!     alias;
//!   * `create` ADDS an entry and `update` REPLACES one. Nothing is mutated in place, so a session
//!     that already captured a definition keeps the one it was built with - "registry changes
//!     affect future sessions only" is structural, not a convention.
//!
//! DELETION (`registry.ts:44-58`)
//! ------------------------------
//! A USER harness needs no tombstone: its row is written from `list()`, so removing the entry
//! removes the row. A SHIPPED built-in always re-seeds itself from `presets.rs`, so a deleted
//! built-in leaves a name in `deleted`, which the database persists as `deleted` in `state.json`
//! and `from_records` replays BEFORE the saved rows. `create` clears the tombstone for the name it
//! takes, so a re-created harness survives its own restart. Aliases follow their target: `writer`
//! resolves to nothing while `instruct` is deleted.
//!
//! VALIDATION OWNERSHIP (`registry.ts:70-76`)
//! ------------------------------------------
//! Wire-level checks (unknown body fields, non-object bodies) stay in the HTTP layer; SEMANTIC
//! validation - known agents, duplicate ids, reserved names, bounds - lives here, because the HTTP
//! API and every future caller must agree. Failures carry a status via [`ApiError`].

pub mod descriptor;
pub mod presets;

pub use descriptor::HarnessDescriptor;

use serde_json::Value;

use crate::agents::AgentCatalogue;
use crate::config::HarnessRuntimeSettings;
use crate::contract::{
    self, AgentRank, CreateHarnessRequest, DeleteHarnessResult, HarnessCard, HarnessDefinition,
    HarnessInstructionSummaryEntry, HarnessPrefixTurn, InstructionSource, PrefixTurnRole,
    UpdateHarnessRequest,
};
use crate::error::ApiError;
use crate::events::EventHub;
use crate::persistence::{HarnessDescriptorRecord, HarnessPresetRecord};
use crate::read;

/// The instruction provenance a definition read reports for one member
/// (`api-reads.ts:1058-1088`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberInstructionSource {
    Catalogue,
    Global,
    Harness,
}

impl MemberInstructionSource {
    /// The wire spelling (`harness-api.ts:444`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Harness => "harness",
            Self::Global => "global",
            Self::Catalogue => "catalogue",
        }
    }
}

/// The in-memory definition registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessRegistry {
    /// Insertion-ordered entries: shipped harnesses first (preset order), then user-created ones in
    /// creation order. `update` replaces an entry IN PLACE, so listing order is stable
    /// (`registry.ts:953`, `:1260`).
    entries: Vec<HarnessDescriptor>,
    /// Built-in names the operator deleted, in deletion order (`:975-985`). Durable:
    /// `state.json`'s `deleted` array.
    deleted: Vec<String>,
    /// The effective runtime settings the host resolved at boot. Carried because the tool half of a
    /// card (which environment tools this host can ACTUALLY run) is decided by the catalogue stage
    /// that reads them, and because a session merge starts from this pair.
    settings: HarnessRuntimeSettings,
}

impl HarnessRegistry {
    /// A registry seeded from the shipped presets, with `deleted` suppressed.
    #[must_use]
    pub fn shipped(settings: &HarnessRuntimeSettings, deleted: Vec<String>) -> Self {
        let tombstones = normalize_names(deleted);

        Self {
            entries: presets::shipped_presets()
                .into_iter()
                .filter(|preset| !tombstones.contains(&preset.name))
                .collect(),
            deleted: tombstones,
            settings: settings.clone(),
        }
    }

    /// Replays a saved registry onto the shipped catalogue (`restoreHarnessRegistry`,
    /// `registry.ts:842-930` + `createHarnessRegistry`'s seeding at `:977-992`).
    ///
    /// Order matters and is TS's:
    ///   1. tombstones are applied FIRST, so a shipped copy of a deleted built-in is gone before
    ///      the record loop can compare or patch it;
    ///   2. every saved row is then replayed. An untouched row is skipped. A built-in row whose
    ///      executable fields still match a recorded previous shipped definition is an unmodified
    ///      SHIPPED DEFAULT, not a decision, so the current template wins and only a genuine prose
    ///      edit is carried across. A row that differs in any executable field is a real
    ///      customisation and is replayed exactly. A user row is re-created in saved order.
    ///
    /// `deleted` therefore suppresses a shipped preset for the life of this process - which is the
    /// brief's "until restart" rule: the next boot re-seeds the shipped copy unless the tombstone is
    /// still in the document.
    ///
    /// # Errors
    ///
    /// The registry validation errors of a replayed row (an unknown or retired agent it names, a
    /// duplicate name), which is TS's behaviour too: `restoreHarnessRegistry` calls `create`/
    /// `update` and lets a refusal propagate.
    pub fn from_records(
        settings: &HarnessRuntimeSettings,
        records: Vec<HarnessPresetRecord>,
        deleted: Vec<String>,
        agents: &AgentCatalogue,
    ) -> Result<Self, ApiError> {
        let mut registry = Self::shipped(settings, deleted);

        for record in records {
            let Some(existing) = registry.get(&record.name).cloned() else {
                if record.builtin {
                    // A built-in row this build does not ship is an injected/fixture preset; its
                    // sessions still restore from their own frozen record (`registry.ts:832-834`,
                    // `:863`).
                    continue;
                }

                registry.create(
                    CreateHarnessRequest {
                        name: record.name.clone(),
                        title: Some(record.title.clone()),
                        description: Some(record.description.clone()),
                        // The saved ROSTER goes back in its saved ORDER, and the saved ENTRY is restated
                        // explicitly rather than left to the order default: a row written by a build whose
                        // entry was not `roster[0]` must still come back with the same routing
                        // (`registry.ts:1659-1676`).
                        agents: Some(json_strings(&record.agents)),
                        entry: record
                            .entry
                            .clone()
                            .or_else(|| record.primary.clone())
                            .map(Value::String),
                        // RANKS, replayed in full: a stored map is already RESOLVED - every member keyed -
                        // so the restart reproduces the SAME ranks instead of re-deriving them. A pre-rank
                        // row carries no map, and `None` here is the kernel's rule-1 default (entry `A`,
                        // everyone else `B`) - the exact statement that row's `primary`/`subagents` pair made.
                        ranks: record.ranks.as_ref().map(json_ranks),
                        agent_models: record.agent_models.clone(),
                        agent_instructions: record.agent_instructions.clone(),
                        // U6 MIRRORS TypeScript exactly, including where it stops: `restoreHarnessRegistry`
                        // replays models, instructions and colors but NOT `agentPrefixes` (`registry.ts:1660-1676`),
                        // so a registry ROW's bands are not restated on a restart. A band reaches a rebuilt
                        // harness through the SESSION path instead - `descriptor::from_record` reads the
                        // session's own frozen copy, which is what `persistence.ts:316-334` says must survive.
                        agent_prefixes: None,
                        agent_colors: record.agent_colors.clone(),
                    },
                    agents,
                )?;

                continue;
            };

            let existing_record = existing.to_record();

            if preset_records_equal(&existing_record, &record) {
                continue;
            }

            // Shipped-default refresh (`registry.ts:882-905`).
            if record.builtin
                && let Some(legacy) = presets::legacy_preset_definitions().get(record.name.as_str())
                && is_legacy_shipped_definition(legacy, &record)
            {
                // Only prose the operator actually wrote is carried across: text that still equals
                // the legacy shipped text IS the shipped text, so the new template's wording wins.
                let keep_title = record.title != legacy.title && record.title != existing.title;
                let keep_description = record.description != legacy.description
                    && record.description != existing.description;

                if keep_title || keep_description {
                    registry.update(
                        &record.name,
                        UpdateHarnessRequest {
                            title: keep_title.then_some(record.title.clone()),
                            description: keep_description.then_some(record.description.clone()),
                            ..UpdateHarnessRequest::default()
                        },
                        agents,
                    )?;
                }

                continue;
            }

            // Executable fields are replayed only for templates whose factory is derivable from
            // them (`registry.ts:907-927`); a scripted row keeps its captured factory, so only its
            // human metadata is applied.
            let executable = record.template.as_deref() != Some("scripted");

            registry.update(
                &record.name,
                UpdateHarnessRequest {
                    title: Some(record.title.clone()),
                    description: Some(record.description.clone()),
                    // Roster, routing and authority replay TOGETHER: the saved row is the definition, so
                    // the rebuild this patch drives must see the same members, in the same order, the same
                    // entry agent and the same rank map the row carried (`registry.ts:1716-1742`).
                    agents: executable.then(|| json_strings(&record.agents)),
                    entry: executable
                        .then(|| {
                            record
                                .entry
                                .clone()
                                .or_else(|| record.primary.clone())
                                .map(Value::String)
                        })
                        .flatten(),
                    ranks: executable
                        .then(|| record.ranks.as_ref().map(json_ranks))
                        .flatten(),
                    agent_models: executable
                        .then_some(record.agent_models.clone().unwrap_or_default()),
                    agent_instructions: executable
                        .then_some(record.agent_instructions.clone().unwrap_or_default()),
                    // U6: as in `create` above, TS does not restate a registry row's bands here, so the
                    // field stays `None` and a row's stored bands do not drive the rebuilt entry.
                    agent_prefixes: None,
                    // R2: the third map is replayed by the same rule (`registry.ts:1040`, `:1102`).
                    agent_colors: executable
                        .then_some(record.agent_colors.clone().unwrap_or_default()),
                },
                agents,
            )?;
        }

        Ok(registry)
    }

    /// The durable projection of the catalogue: every entry, in listing order
    /// (`registry.ts:616-639`, applied to `list()`).
    #[must_use]
    pub fn to_records(&self) -> Vec<HarnessPresetRecord> {
        self.entries
            .iter()
            .map(HarnessDescriptor::to_record)
            .collect()
    }

    /// The frozen-descriptor projection of every entry, for a caller that persists definitions
    /// rather than preset rows.
    #[must_use]
    pub fn to_descriptor_records(&self) -> Vec<HarnessDescriptorRecord> {
        self.entries.iter().map(descriptor_record_of).collect()
    }

    /// The built-in names the operator deleted, in deletion order (`deletedBuiltins`,
    /// `registry.ts:1370`).
    #[must_use]
    pub fn deleted_names(&self) -> Vec<String> {
        self.deleted.clone()
    }

    /// Every entry name, in listing order (`list().map(name)`, `registry.ts:1356`).
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect()
    }

    /// The host settings this registry was built with.
    #[must_use]
    pub fn runtime_settings(&self) -> &HarnessRuntimeSettings {
        &self.settings
    }

    /// Resolves a name INCLUDING a legacy alias (`find`, `registry.ts:1004-1010`).
    ///
    /// `None` is a 404 for a session request. Aliases are read compatibility redirects: they can
    /// resolve, and they can never be written or deleted.
    #[must_use]
    pub fn find(&self, name: &str) -> Option<&HarnessDescriptor> {
        if let Some(entry) = self.get(name) {
            return Some(entry);
        }

        presets::alias_target(name).and_then(|target| self.get(target))
    }

    /// Resolves a name or fails with the contract's 404.
    ///
    /// # Errors
    ///
    /// [`ApiError::not_found`] `Unknown harness "<name>".` (`api.ts:606-607`).
    pub fn resolve(&self, name: &str) -> Result<HarnessDescriptor, ApiError> {
        self.find(name)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("Unknown harness \"{name}\".")))
    }

    /// Direct lookup, alias NOT resolved - the identity `update`/`delete` use
    /// (`entries.get`, `registry.ts:1118`, `:1270`).
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&HarnessDescriptor> {
        self.entries.iter().find(|entry| entry.name == name)
    }

    /// Every card, hub-free: `definition_revision` is 0 because a revision counter is hub state and
    /// this projection has no hub. The paginated route uses [`HarnessRegistry::cards_with`].
    #[must_use]
    pub fn cards(&self) -> Vec<HarnessCard> {
        self.entries
            .iter()
            .map(|entry| read::harness_card_of_revision(entry, 0))
            .collect()
    }

    /// Every card stamped with its live revision counter (`api.ts:1468-1472` `cardOf`).
    #[must_use]
    pub fn cards_with(&self, hub: &EventHub) -> Vec<HarnessCard> {
        self.entries
            .iter()
            .map(|entry| read::harness_card_of(entry, hub))
            .collect()
    }

    /// The card for one name, alias resolved.
    ///
    /// # Errors
    ///
    /// As [`HarnessRegistry::resolve`].
    pub fn card(&self, name: &str, hub: &EventHub) -> Result<HarnessCard, ApiError> {
        Ok(read::harness_card_of(&self.resolve(name)?, hub))
    }

    /// The identified definition read for one name, alias resolved.
    ///
    /// # Errors
    ///
    /// As [`HarnessRegistry::resolve`].
    pub fn definition(
        &self,
        name: &str,
        hub: &EventHub,
        agents: &AgentCatalogue,
    ) -> Result<HarnessDefinition, ApiError> {
        Ok(read::harness_definition_of(
            &self.resolve(name)?,
            hub,
            agents,
        ))
    }

    /// One row per member with instruction provenance and length, no text
    /// (`instructionSummaryOf`, `api-reads.ts:1067-1088`).
    ///
    /// # Errors
    ///
    /// As [`HarnessRegistry::resolve`].
    pub fn instruction_index(
        &self,
        name: &str,
        agents: &AgentCatalogue,
    ) -> Result<Vec<HarnessInstructionSummaryEntry>, ApiError> {
        let definition = self.resolve(name)?;

        Ok(instruction_summary_of(&definition, agents))
    }

    /// One member's effective instruction text plus its provenance and guideline rules
    /// (`instructionTextOf`, `api-reads.ts:1091-1120`, with the route's membership gate from
    /// `api.ts:1647-1649`).
    ///
    /// Returns `(instruction, guidelines, source)`; `source` is `"harness"`, `"global"`, or
    /// `"catalogue"`.
    ///
    /// # Errors
    ///
    /// [`ApiError::not_found`] for an unknown harness, or for a member this definition does not
    /// select.
    pub fn instruction_text(
        &self,
        name: &str,
        agent: &str,
        agents: &AgentCatalogue,
    ) -> Result<(String, Vec<String>, &'static str), ApiError> {
        let definition = self.resolve(name)?;

        if !definition.agents.iter().any(|member| member == agent) {
            return Err(ApiError::not_found(format!(
                "Agent \"{agent}\" is not a member of harness \"{}\".",
                definition.name
            )));
        }

        let (instruction, guidelines, source) = instruction_text_of(&definition, agent, agents);

        Ok((instruction, guidelines, source.as_str()))
    }

    /// Adds a NEW definition (`create`, `registry.ts:1882-2074`).
    ///
    /// A client selects trusted agent names (`agents`, in declaration order) and may state routing
    /// (`entry`), authority (`ranks`), per-agent model ids, instruction overrides, preset-history
    /// bands and display colors; it cannot supply a provider, a credential, a transport, or a tool
    /// list. An omitted `entry` is the roster's first name and an omitted `ranks` is the ordinary
    /// two-level team, so the ONLY required selection field is the roster.
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] for a missing/badly-shaped name, a roster that is not a non-empty
    /// array / is empty / exceeds 16 entries / repeats a name / names an unknown agent, an `entry`
    /// that is not a member, a `ranks` map that is not an object, names a non-member, or holds a value
    /// that is not an upper-case rank letter, an `agent_prefixes` map that is not an object, names a
    /// non-member, or holds a band whose length, role, or content is refused, and an over-long or
    /// blank text; [`ApiError::conflict`] when the name is taken (case-insensitively) or when the
    /// roster selects a retired profile.
    pub fn create(
        &mut self,
        request: CreateHarnessRequest,
        agents: &AgentCatalogue,
    ) -> Result<HarnessDescriptor, ApiError> {
        let name = request.name.trim().to_string();

        if name.is_empty() {
            return Err(ApiError::bad_request(
                "A harness definition requires a \"name\".",
            ));
        }

        // There is no "a definition requires a primary agent" check any more: `entry` is a ROUTING
        // field with one legitimate default (the roster's first member), so the REQUIRED field is the
        // roster, and `normalize_roster` refuses the empty case with the sentence that explains why
        // (`api.ts:2399-2410`, `registry.ts:517-530`).
        if !presets::is_valid_harness_name(&name) {
            return Err(ApiError::bad_request(presets::harness_name_refusal()));
        }

        if presets::is_reserved_alias(&name) {
            return Err(ApiError::bad_request(format!(
                "Harness name \"{name}\" is reserved as an alias of \"{}\".",
                presets::alias_target(&name).unwrap_or_default()
            )));
        }

        if self.name_taken(&name) {
            return Err(ApiError::conflict(format!(
                "Harness \"{name}\" already exists."
            )));
        }

        // THE ROSTER IS RESOLVED FIRST, because both other roster fields are validated against it:
        // `entry` must be a member, and every `ranks` KEY must be a member. `agents` used to be
        // DERIVED here (`[primary, ...subagents]`); it is now the client's own explicit list, and its
        // ORDER is load-bearing (`registry.ts:1899-1912`).
        let roster = normalize_roster(request.agents.as_ref(), agents, None)?;

        // THE ENTRY AGENT: optional, and the default is the first name of the roster. It is a ROUTING
        // default only - the rank map says authority, nothing requires the entry to hold Rank `A`, and
        // a two-Rank-A harness is a normal shape in this model, not an invariant to police
        // (`registry.ts:1914-1926`).
        let entry = match &request.entry {
            Some(value) => normalize_entry(value, &roster)?,
            None => roster[0].clone(),
        };

        // THE RANK MAP, RESOLVED INTO THE MAP THE HARNESS RUNS WITH. An omitted map is the kernel's
        // rule-1 default (entry `A`, everyone else one below at `B`); a stated map is validated, then
        // filled by the rule the kernel itself applies - one rank below the LOWEST rank it names, entry
        // agent included - which is what gives `ranks: {}` the flat team it reads as. Storing the
        // RESOLVED map is what keeps the durable record, the card, and the run stating the same ranks,
        // resolved ONCE here instead of re-derived at every start (`registry.ts:1928-1949`).
        let stated_ranks = match &request.ranks {
            Some(value) => Some(normalize_ranks(value, &roster)?),
            None => None,
        };
        let ranks = presets::resolve_roster_ranks(&roster, &entry, stated_ranks.as_ref());

        // The final member set: the roster itself. The four per-agent maps are validated against
        // exactly this, so a key can never name an agent the harness does not contain.
        let members = roster;

        let agent_models = match &request.agent_models {
            Some(map) => normalize_agent_models(map, &members)?,
            None => std::collections::BTreeMap::new(),
        };

        let agent_instructions = match &request.agent_instructions {
            Some(map) => normalize_agent_instructions(map, &members)?,
            None => std::collections::BTreeMap::new(),
        };

        // The third map of the same shape (`registry.ts:1244-1250`): validated against the FINAL
        // member set, because a swatch for an agent this harness does not select can never be
        // painted.
        let agent_colors = match &request.agent_colors {
            Some(map) => normalize_agent_colors(map, &members)?,
            None => std::collections::BTreeMap::new(),
        };

        // Preset history (U6) is validated against the same final member set for the same reason: a
        // band for an unselected agent could never be seeded. Note the asymmetry this creates with the
        // three maps above - an EMPTY band is a VALUE, so `agent_prefixes: { x: [] }` is a stored key,
        // not an empty map, and a definition that clears one member keeps stating that member
        // (`registry.ts:1997-2008`).
        let agent_prefixes = match &request.agent_prefixes {
            Some(value) => normalize_agent_prefixes(value, &members)?,
            None => std::collections::BTreeMap::new(),
        };

        let title = match &request.title {
            Some(value) => normalize_title(value)?,
            // A created harness with no stated title is named by its id (`registry.ts:1062-1065`).
            None => name.clone(),
        };

        let description = match &request.description {
            Some(value) => normalize_description(value)?,
            None => String::new(),
        };

        let definition = HarnessDescriptor {
            name: name.clone(),
            title,
            description,
            // A user-created harness selects trusted live agents, so it needs a provider exactly
            // like the live built-ins do (`registry.ts:1070-1072`).
            requires_endpoint: true,
            entry,
            ranks,
            agents: members,
            // One toolbox for every live harness, user-created included (`registry.ts:1076-1083`).
            tools: presets::SHARED_TOOL_NAMES
                .iter()
                .map(|tool| (*tool).to_string())
                .collect(),
            environment_tools: presets::ENVIRONMENT_TOOL_NAMES
                .iter()
                .map(|tool| (*tool).to_string())
                .collect(),
            agent_models,
            agent_instructions,
            agent_colors,
            agent_prefixes,
            builtin: false,
            editable: true,
            template: Some("live".to_string()),
            live: None,
        };

        self.entries.push(definition.clone());

        // Taking a deleted built-in's NAME is an explicit decision to have that harness again, so
        // the tombstone goes with it (`registry.ts:1103-1109`).
        self.deleted.retain(|entry| entry != &name);

        Ok(definition)
    }

    /// Replaces one definition with a NEW value (`update`, `registry.ts:2076-2290`).
    ///
    /// `name` is immutable - a rename would break sessions, bookmarks, and the UI's selected card -
    /// and `agents` REPLACES the roster, `entry` REPLACES routing, `ranks` REPLACES the authority map
    /// (filled for every member it omits), and each of the four per-agent maps REPLACES the map it
    /// names (`{}` clears it). An override for a member this patch removed is DROPPED: a model,
    /// instruction, band, or color for an agent that is no longer selected cannot apply, and carrying
    /// it would silently resurrect when the agent returned.
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] `A patch must change at least one field.` (checked before the
    /// lookup, exactly as the route does) or the same validation refusals as
    /// [`HarnessRegistry::create`]; [`ApiError::not_found`] for an unknown name (an alias is a read
    /// redirect, not an editable identity).
    pub fn update(
        &mut self,
        name: &str,
        request: UpdateHarnessRequest,
        agents: &AgentCatalogue,
    ) -> Result<HarnessDescriptor, ApiError> {
        if request.title.is_none()
            && request.description.is_none()
            && request.agents.is_none()
            && request.entry.is_none()
            && request.ranks.is_none()
            && request.agent_models.is_none()
            && request.agent_instructions.is_none()
            // U6: an explicit `{}` or `{"member": []}` is a REAL change, so the field counts on
            // presence alone - the same rule the other three maps use (`api.ts:2464-2466`).
            && request.agent_prefixes.is_none()
            && request.agent_colors.is_none()
        {
            return Err(ApiError::bad_request(
                "A patch must change at least one field.",
            ));
        }

        let position = self
            .entries
            .iter()
            .position(|entry| entry.name == name)
            .ok_or_else(|| ApiError::not_found(format!("Unknown harness \"{name}\".")))?;

        let current = self.entries[position].clone();

        // A name this entry ALREADY selects stays valid after the operator retires the profile -
        // that is what keeps a saved row restorable - while a name the patch INTRODUCES must be
        // selectable (`registry.ts:1128-1133`).
        let previously = current.agents.clone();

        // THE ROSTER. A stated roster REPLACES the member list (order included, since order is the
        // entry default); an omitted one keeps the current members in their current order
        // (`registry.ts:2096-2106`).
        let roster = match &request.agents {
            Some(value) => normalize_roster(Some(value), agents, Some(&previously))?,
            None => current.agents.clone(),
        };

        // THE ENTRY AGENT. Stated: it must name a member of the FINAL roster. Omitted: the current
        // entry survives whenever the same patch left it a member - re-targeting routing is a separate
        // decision from changing the team - and when the patch dropped it, the entry falls back to the
        // first name of the new roster, the same default `create` applies. A patch can therefore never
        // leave a harness routed at an agent it no longer contains, which is the guarantee the old
        // "changing the primary removes it from subagents" rule was reaching for from the other
        // direction (`registry.ts:2108-2120`).
        let entry = match &request.entry {
            Some(value) => normalize_entry(value, &roster)?,
            None if roster.contains(&current.entry) => current.entry.clone(),
            None => roster[0].clone(),
        };

        // THE RANK MAP, RESOLVED THE SAME WAY. A stated map REPLACES the stored one and is filled for
        // every member it does not name; an omitted one keeps the current ranks RESTRICTED TO THE
        // SURVIVING MEMBERS, and a member this patch ADDED takes the filled value the kernel would give
        // it - one rank below the lowest rank that survives, so a newcomer never lands at `A` by
        // accident and a flat harness stays flat. A rank for an agent this patch removed can no longer
        // apply, so it is dropped rather than rejected, exactly like a model, instruction, band, or
        // color override for the same member (`registry.ts:2122-2152`).
        //
        // THE EMPTY-MAP DISTINCTION IS THE CLIENT'S, NOT THE HOST'S. `ranks: {}` in the body is an
        // explicit statement ("make this team flat") and reaches the fill as an empty map, which puts
        // every member on `B`. An OMITTED map is "no opinion", and when a roster swap drops every
        // member the retained map is legitimately empty - that must NOT read as "flat", because the
        // client never said so: passing `None` applies the same rule-1 default `create` uses.
        let retained_ranks = retain_ranks(&current.ranks, &roster);

        let ranks = presets::resolve_roster_ranks(
            &roster,
            &entry,
            match &request.ranks {
                Some(value) => Some(normalize_ranks(value, &roster)?),
                None if retained_ranks.is_empty() => None,
                None => Some(retained_ranks),
            }
            .as_ref(),
        );

        // The FINAL member set. The four override maps and the rank map are all validated against this,
        // so no key can name an agent the harness does not contain.
        let members = roster;

        let agent_models = match &request.agent_models {
            Some(map) => normalize_agent_models(map, &members)?,
            None => retain_members(&current.agent_models, &members),
        };

        let agent_instructions = match &request.agent_instructions {
            Some(map) => normalize_agent_instructions(map, &members)?,
            None => retain_members(&current.agent_instructions, &members),
        };

        // Colors follow the identical rule for the identical reason (`registry.ts:2186-2198`): a
        // swatch for an agent this patch removed can never be painted, so it is dropped, and a
        // stated map REPLACES the stored one (`{}` clears every color).
        let agent_colors = match &request.agent_colors {
            Some(map) => normalize_agent_colors(map, &members)?,
            None => retain_members(&current.agent_colors, &members),
        };

        // Preset history (U6) follows the identical drop-or-replace rule, with the one asymmetry
        // `normalize_agent_prefixes` documents (`registry.ts:2200-2213`): an empty BAND survives as a
        // key (`{ "editor": [] }` means "no history for editor"), while an empty MAP clears every band.
        // Both are the client's stated choice, so neither is collapsed away here.
        let agent_prefixes = match &request.agent_prefixes {
            Some(value) => normalize_agent_prefixes(value, &members)?,
            None => retain_prefixes(&current.agent_prefixes, &members),
        };

        // An executable change on a SCRIPTED template upgrades it to a live harness and corrects the
        // descriptor fields that would otherwise still advertise the offline fixture
        // (`registry.ts:1203-1256`).
        let executable = request.entry.is_some()
            || request.ranks.is_some()
            || request.agents.is_some()
            || request.agent_models.is_some()
            || request.agent_instructions.is_some()
            // R2: a color is display-only, but it is still EXECUTABLE in this sense (`registry.ts
            // :1411-1416`): the map reaches every agent built from this definition, so a stated
            // color must rebuild the factory - and, on the scripted demo, upgrade it to a live
            // harness exactly like an instruction edit does.
            || request.agent_colors.is_some()
            // U6: a preset band is executable for the same reason and then some - it is not display
            // data, it is transcript the next session's agents are seeded with, so a stated map must
            // rebuild the factory or the new band would never run (`registry.ts:2226-2231`).
            || request.agent_prefixes.is_some();

        let upgraded = executable && current.template.as_deref() == Some("scripted");

        let (title, description) = resolved_prose(&current, &request, upgraded)?;

        let definition = HarnessDescriptor {
            name: current.name.clone(),
            title,
            description,
            requires_endpoint: if upgraded {
                true
            } else {
                current.requires_endpoint
            },
            entry,
            ranks,
            agents: members,
            tools: if upgraded {
                presets::SHARED_TOOL_NAMES
                    .iter()
                    .map(|tool| (*tool).to_string())
                    .collect()
            } else {
                current.tools.clone()
            },
            environment_tools: if upgraded {
                presets::ENVIRONMENT_TOOL_NAMES
                    .iter()
                    .map(|tool| (*tool).to_string())
                    .collect()
            } else {
                current.environment_tools.clone()
            },
            agent_models,
            agent_instructions,
            // Stated explicitly, not inherited from `current`: `{}` from a patch means "no
            // colors", which `current` alone would not produce (`registry.ts:1460-1465`).
            agent_colors,
            // Same stated-not-inherited rule for the fourth map: `{}` from a patch is the explicit
            // clear of every band, which `current` alone would not produce (`registry.ts:2200-2213`).
            agent_prefixes,
            builtin: current.builtin,
            editable: true,
            template: if upgraded {
                Some("live".to_string())
            } else {
                current.template.clone()
            },
            live: current.live.clone(),
        };

        self.entries[position] = definition.clone();

        Ok(definition)
    }

    /// Removes one definition (`remove`, `registry.ts:1267-1293`).
    ///
    /// Direct name only, like `update`: a legacy alias is a read compatibility redirect, not an
    /// identity that can be deleted.
    ///
    /// # Errors
    ///
    /// [`ApiError::not_found`] with the wording that says WHICH kind of miss this is, so a UI
    /// cannot confuse "never existed" with "already deleted".
    pub fn delete(&mut self, name: &str) -> Result<DeleteHarnessResult, ApiError> {
        let position = match self.entries.iter().position(|entry| entry.name == name) {
            Some(position) => position,
            None => {
                return Err(ApiError::not_found(
                    if self.deleted.iter().any(|entry| entry == name) {
                        format!("Harness \"{name}\" has already been deleted.")
                    } else {
                        format!("Unknown harness \"{name}\".")
                    },
                ));
            }
        };

        let removed = self.entries.remove(position);

        // Only a built-in needs a tombstone, because only a built-in comes back on the next start
        // (`registry.ts:1286-1290`).
        if removed.builtin && !self.deleted.iter().any(|entry| entry == name) {
            self.deleted.push(name.to_string());
        }

        let tombstoned = removed.builtin && self.deleted.iter().any(|entry| entry == name);

        Ok(DeleteHarnessResult {
            deleted: true,
            harness_name: name.to_string(),
            tombstoned,
            // The tombstone is durable (`state.json`'s `deleted`), so a restart does NOT re-ship the
            // card the operator removed (`api.ts:1611-1613`).
            restored_by_restart: false,
        })
    }

    /// The harnesses that select one profile, so a client can show what a removal would strand
    /// (`api.ts:1658-1663`).
    #[must_use]
    pub fn referenced_by_agent(&self, agent: &str) -> Vec<String> {
        self.entries
            .iter()
            .filter(|entry| entry.agents.iter().any(|member| member == agent))
            .map(|entry| entry.name.clone())
            .collect()
    }

    /// Case-insensitive name test: "Instruct" would be a distinct key but the same card to a user,
    /// so a new name must not shadow an existing one that way (`registry.ts:1012-1015`).
    #[must_use]
    pub fn name_taken(&self, name: &str) -> bool {
        let lowered = name.to_lowercase();

        self.entries
            .iter()
            .any(|entry| entry.name.to_lowercase() == lowered)
    }
}

// ---------------------------------------------------------------------------
// Projections
// ---------------------------------------------------------------------------

/// A string list as a wire value. A replayed durable row re-enters the registry through the SAME
/// validating surface a HTTP body uses, and that surface takes raw JSON for the roster fields - the
/// route forwards them unvalidated (`api.ts:2408-2412`), so the registry, not serde, owns every
/// refusal sentence.
#[must_use]
fn json_strings(values: &[String]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|value| Value::String(value.clone()))
            .collect::<Vec<Value>>(),
    )
}

/// A rank map as a wire value: agent id -> the letter, which is the only spelling the contract has.
#[must_use]
fn json_ranks(ranks: &std::collections::BTreeMap<String, AgentRank>) -> Value {
    Value::Object(
        ranks
            .iter()
            .map(|(agent, rank)| (agent.clone(), Value::String(rank.as_str().to_string())))
            .collect(),
    )
}

/// Projects a definition onto the FROZEN descriptor a session records (`describeHarness`,
/// `descriptor.ts:96-146`, in the field order `persistence.ts` validates).
#[must_use]
pub fn descriptor_record_of(definition: &HarnessDescriptor) -> HarnessDescriptorRecord {
    HarnessDescriptorRecord {
        name: definition.name.clone(),
        title: definition.title.clone(),
        description: definition.description.clone(),
        requires_endpoint: definition.requires_endpoint,
        // Routing and authority are frozen as the two separate fields they are, both ALWAYS present
        // (`descriptor.ts:100-101`, `persistence.ts` `readDescriptor`).
        entry: Some(definition.entry.clone()),
        ranks: Some(definition.ranks.clone()),
        // The legacy pair is never re-written into a frozen copy: a session created by this build
        // states `entry`/`ranks`, which is what a restore rebuilds from.
        primary: None,
        subagents: None,
        agents: definition.agents.clone(),
        tools: definition.registered_tools(),
        // Emitted only when the card actually declares a core set, so an untouched card keeps its
        // exact pre-R18 shape (`descriptor.ts:95-96`).
        core_tools: Some(definition.core_tools()),
        agent_models: if definition.agent_models.is_empty() {
            None
        } else {
            Some(definition.agent_models.clone())
        },
        agent_instructions: if definition.agent_instructions.is_empty() {
            None
        } else {
            Some(definition.agent_instructions.clone())
        },
        // The color map follows the same non-empty rule (`descriptor.ts:117-119`), which is what
        // lets a session frozen before colors existed persist with no key at all.
        agent_colors: if definition.agent_colors.is_empty() {
            None
        } else {
            Some(definition.agent_colors.clone())
        },
        // U6's fourth map follows the same non-empty rule (`descriptor.ts:128-142`), which is what
        // keeps a session frozen on a band-less definition byte-identical to a pre-U6 one - while a
        // member keyed with `[]` keeps its key, because "no history for this member" is a statement.
        agent_prefixes: if definition.agent_prefixes.is_empty() {
            None
        } else {
            Some(definition.agent_prefixes.clone())
        },
        builtin: definition.builtin,
        editable: definition.editable,
        extra: serde_json::Map::new(),
    }
}

/// Rebuilds a definition from the frozen descriptor inside a session record
/// (`copyDescriptor`'s inverse; `persistence.ts` `readDescriptor`).
#[must_use]
pub fn definition_from_descriptor_record(record: &HarnessDescriptorRecord) -> HarnessDescriptor {
    // The same tolerant entry/rank reading `descriptor::from_record` applies to a preset row: a
    // session frozen before the rank pass carries `primary`/`subagents` here, and its conversation must
    // still restore.
    let entry = record
        .entry
        .clone()
        .or_else(|| record.primary.clone())
        .or_else(|| record.agents.first().cloned())
        .unwrap_or_else(|| record.name.clone());

    let ranks = record
        .ranks
        .clone()
        .unwrap_or_else(|| presets::resolve_roster_ranks(&record.agents, &entry, None));

    HarnessDescriptor {
        name: record.name.clone(),
        title: record.title.clone(),
        description: record.description.clone(),
        requires_endpoint: record.requires_endpoint,
        entry,
        ranks,
        agents: record.agents.clone(),
        tools: record.tools.clone(),
        // A frozen descriptor already carries the REGISTERED list (environment tools included), so
        // the environment half is empty on the rebuilt copy: re-deriving it would double-list.
        environment_tools: Vec::new(),
        agent_models: record.agent_models.clone().unwrap_or_default(),
        agent_instructions: record.agent_instructions.clone().unwrap_or_default(),
        agent_colors: record.agent_colors.clone().unwrap_or_default(),
        // The frozen bands are the UNTOUCHED definition copy a `null` prefix patch restores from
        // (`api.ts:4479`), so they come back verbatim.
        agent_prefixes: record.agent_prefixes.clone().unwrap_or_default(),
        builtin: record.builtin,
        editable: record.editable,
        template: Some("live".to_string()),
        live: None,
    }
}

/// The per-member instruction summary, in membership order
/// (`instructionSummaryOf`, `api-reads.ts:1067-1088`).
///
/// `source` is `harness` when THIS definition overrides the prompt, `global` when the
/// deployment-wide agent layer does, and `catalogue` when the trusted profile supplies it. The most
/// specific layer wins, which is also the order the runtime composes the prompt in, so a row cannot
/// name a layer whose text the next session will not actually run.
///
/// `chars` counts CHARACTERS of the text this member actually runs: the override when this harness
/// states one, otherwise the effective profile's instruction (which is the global override's text
/// when the operator stated one).
#[must_use]
pub fn instruction_summary_of(
    definition: &HarnessDescriptor,
    agents: &AgentCatalogue,
) -> Vec<HarnessInstructionSummaryEntry> {
    definition
        .agents
        .iter()
        .map(|member| {
            let override_text = definition.agent_instructions.get(member);
            let profile = agents.effective(member);

            // `chars` is a UTF-16 length, because TypeScript's `instruction.length` counts UTF-16 code
            // units and the wire states the unit (`projector::display_chars`, the one helper for that
            // count - `api.ts:2803` / `:2831` and `harness-api.ts`'s `chars` members).
            let chars = u64::try_from(override_text.map_or_else(
                || {
                    profile.map_or(0, |profile| {
                        crate::projector::display_chars(&profile.instruction)
                    })
                },
                |text| crate::projector::display_chars(text),
            ))
            .unwrap_or(u64::MAX);

            let harness_override = override_text.is_some();
            let global_override =
                !harness_override && agents.overrides_of(member).instruction.is_some();

            HarnessInstructionSummaryEntry {
                agent: member.clone(),
                source: if harness_override {
                    InstructionSource::Harness
                } else if global_override {
                    InstructionSource::Global
                } else {
                    InstructionSource::Catalogue
                },
                overridden: harness_override,
                chars,
            }
        })
        .collect()
}

/// One member's effective instruction text plus its provenance and guideline rules
/// (`instructionTextOf`, `api-reads.ts:1091-1120`).
///
/// A RETIRED profile is absent from the selectable list but is still named by an existing
/// definition, so the SHIPPED catalogue entry is the fallback for the text and guideline rules that
/// definition runs on (`:1097-1100`).
#[must_use]
pub fn instruction_text_of(
    definition: &HarnessDescriptor,
    agent: &str,
    agents: &AgentCatalogue,
) -> (String, Vec<String>, MemberInstructionSource) {
    let profile = agents.effective(agent).or_else(|| agents.shipped(agent));

    let override_text = definition.agent_instructions.get(agent);

    let instruction = override_text
        .cloned()
        .or_else(|| profile.as_ref().map(|profile| profile.instruction.clone()))
        .unwrap_or_default();

    let guidelines = profile
        .map(|profile| profile.guidelines)
        .unwrap_or_default();

    let source = if override_text.is_some() {
        MemberInstructionSource::Harness
    } else if agents.overrides_of(agent).instruction.is_some() {
        MemberInstructionSource::Global
    } else {
        MemberInstructionSource::Catalogue
    };

    (instruction, guidelines, source)
}

// ---------------------------------------------------------------------------
// Validation (`registry.ts:283-539`)
// ---------------------------------------------------------------------------

/// A registry refusal that names its field the way `HarnessRegistryError` does.
fn registry_bad_request(message: String) -> ApiError {
    ApiError::bad_request(message)
}

/// `requireAgent` (`registry.ts:329-353`).
///
/// The id must always be a shipped-catalogue identity; when `previously_selected` is supplied (an
/// UPDATE), a name the entry already selects stays valid even if the profile is retired, while a
/// name the patch INTRODUCES must still be selectable. A CREATE passes `None`, so a retired profile
/// is refused outright: a write that selected it could never run in a new session.
fn require_agent(
    value: &str,
    field: &str,
    agents: &AgentCatalogue,
    previously_selected: Option<&[String]>,
) -> Result<String, ApiError> {
    // TS splits this gate in TWO halves and only ONE of them is bypassable
    // (`registry.ts:2093-2095` + `requireAgent` `:475-499`):
    //   - the UNKNOWN half is never bypassed: `knownAgent` is shipped-catalogue membership
    //     (`registry.ts:1822`), and an id outside the catalogue cannot be resolved into an agent at
    //     all, so a saved row naming one is a row this build cannot run;
    //   - the RETIRED half IS bypassed by a name the entry already selects — `selectableHere` =
    //     `selectableAgent(name) || previouslySelected.has(name)` — which is what keeps a row whose
    //     profile the operator retired both replayable and patchable.
    //
    // R2 narrowed this helper back to that rule: it used to bypass BOTH halves for an already-selected
    // name, a workaround for the fact that this build shipped no `general`/`debugger` profile while
    // every shipped card names one. Both profiles are ported now, so the workaround would only hide a
    // typo in a replayed roster.
    if agents.shipped(value).is_none() {
        return Err(registry_bad_request(format!(
            "`{field}` names unknown agent \"{value}\". Known agents: {}.",
            agents.names().join(", ")
        )));
    }

    let already_selected =
        previously_selected.is_some_and(|names| names.iter().any(|name| name == value));

    if !already_selected && agents.is_retired(value) {
        return Err(ApiError::conflict(format!(
            "Agent \"{value}\" is retired and can no longer be selected. Existing harnesses and sessions that already select it are unaffected."
        )));
    }

    Ok(value.to_string())
}

/// `normalizeTitle` (`registry.ts:293-305`): a blank title would render as an unnamed card, so it
/// is rejected rather than silently replaced.
fn normalize_title(value: &str) -> Result<String, ApiError> {
    let title = value.trim();

    if title.is_empty() {
        return Err(registry_bad_request("`title` cannot be empty.".to_string()));
    }

    // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
    if title.encode_utf16().count() > presets::MAX_TITLE_LENGTH {
        return Err(registry_bad_request(format!(
            "`title` cannot exceed {} characters.",
            presets::MAX_TITLE_LENGTH
        )));
    }

    Ok(title.to_string())
}

/// `normalizeDescription` (`registry.ts:307-318`): blank is allowed - a harness with no prose is
/// legitimate - only the ceiling is enforced.
fn normalize_description(value: &str) -> Result<String, ApiError> {
    let description = value.trim();

    // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
    if description.encode_utf16().count() > presets::MAX_DESCRIPTION_LENGTH {
        return Err(registry_bad_request(format!(
            "`description` cannot exceed {} characters.",
            presets::MAX_DESCRIPTION_LENGTH
        )));
    }

    Ok(description.to_string())
}

/// `requireString` (`registry.ts:422-430`), over a raw wire value: the shape half of a field, printed
/// with the field name. A missing member is answered exactly like a wrongly-typed one, because
/// `JSON.stringify(undefined)` and a number both fail the same `typeof` check in TS.
fn require_text(value: &Value, field: &str) -> Result<String, ApiError> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| registry_bad_request(format!("`{field}` must be a string.")))
}

/// `JSON.stringify(value)` as it appears INSIDE a refusal sentence: a string is quoted, a number and
/// `null` print bare, and an absent member prints `undefined` (what TS's template literal shows when
/// `JSON.stringify` returns `undefined`).
fn wire_display(value: Option<&Value>) -> String {
    value.map_or_else(|| "undefined".to_string(), |value| value.to_string())
}

/// `normalizeRoster` (`registry.ts:517-552`): THE ROSTER is an explicit, NON-EMPTY list of trusted
/// agent ids in DECLARATION ORDER. It replaced the old `[primary, ...subagents]` derivation, so the
/// rules it carries are the ones the allowlist used to enforce plus one more - duplicates are an error
/// rather than a silent collapse, because a roster that names an agent twice is ambiguous and the
/// registry must not guess which occurrence, and which rank entry, was meant.
///
/// There is deliberately NO rule about which name comes first and no rule that the first name outranks
/// the others: rank says authority, order says only which agent is the routing DEFAULT.
///
/// `None` is answered with the "not an array" refusal, which is exactly TS's path for a body that
/// omitted `agents`: the route forwards the field unvalidated (`api.ts:2408`), so this is where an
/// absent roster becomes the contract's 400.
fn normalize_roster(
    value: Option<&Value>,
    agents: &AgentCatalogue,
    previously_selected: Option<&[String]>,
) -> Result<Vec<String>, ApiError> {
    let Some(entries) = value.and_then(Value::as_array) else {
        return Err(registry_bad_request(
            "`agents` must be a non-empty array of agent names.".to_string(),
        ));
    };

    if entries.is_empty() {
        return Err(registry_bad_request(
            "`agents` must name at least one agent; a harness with no agent cannot run a turn."
                .to_string(),
        ));
    }

    if entries.len() > presets::MAX_ROSTER_AGENTS {
        return Err(registry_bad_request(format!(
            "`agents` cannot exceed {} entries.",
            presets::MAX_ROSTER_AGENTS
        )));
    }

    let mut resolved: Vec<String> = Vec::with_capacity(entries.len());

    for entry in entries {
        let name = require_text(entry, "agents")
            .and_then(|name| require_agent(&name, "agents", agents, previously_selected))?;

        if resolved.contains(&name) {
            return Err(registry_bad_request(format!(
                "Agent \"{name}\" is listed in `agents` more than once."
            )));
        }

        resolved.push(name);
    }

    Ok(resolved)
}

/// `normalizeEntry` (`registry.ts:564-577`): THE ENTRY AGENT is routing, so the only rule is
/// membership. It is resolved separately from the roster - never derived inside this helper - because
/// the caller has to say which default applies when the client omitted it, and because a PATCH must
/// distinguish "the client restated routing" from "the roster moved under it". A non-member is a 400
/// that NAMES the roster: the common mistake is a client that renamed an agent or sent `entry` for a
/// member it forgot to add.
fn normalize_entry(value: &Value, roster: &[String]) -> Result<String, ApiError> {
    let name = require_text(value, "entry")?;

    if !roster.iter().any(|member| member == &name) {
        return Err(registry_bad_request(format!(
            "`entry` names \"{name}\", which is not a member of this harness. Members: {}.",
            roster.join(", ")
        )));
    }

    Ok(name)
}

/// `normalizeRanks` (`registry.ts:592-628`): THE RANK MAP, agent id -> A-Z letter. Two rules, both a
/// 400, both named exactly: every KEY must name a member of the FINAL roster (a rank for an agent this
/// harness does not select can never apply, so accepting it would leave the client believing it
/// configured something - the identical reasoning `normalizeAgentModels` gives the model map), and
/// every VALUE must be a rank letter, checked through the shared [`contract::is_agent_rank`] guard so
/// this edge and the kernel can never disagree about what a rank is.
///
/// CASE MATTERS: `'a'` is not a rank, and the message says so instead of quietly upper-casing the
/// client's intent. A member the map OMITS is not an error and does not mean "no rank" - the CALLER
/// fills it with [`presets::resolve_roster_ranks`], the kernel's own rule. Nothing here requires a Rank
/// `A` agent either: several members may hold `A`, and a roster with none is a team whose entry agent
/// was simply ranked like its peers.
fn normalize_ranks(
    value: &Value,
    roster: &[String],
) -> Result<std::collections::BTreeMap<String, AgentRank>, ApiError> {
    let Some(entries) = value.as_object() else {
        return Err(registry_bad_request(
            "`ranks` must be an object mapping agent ids to rank letters.".to_string(),
        ));
    };

    let mut resolved = std::collections::BTreeMap::new();

    for (agent, rank) in entries {
        if !roster.iter().any(|member| member == agent) {
            return Err(registry_bad_request(format!(
                "`ranks` names \"{agent}\", which is not a member of this harness. Members: {}.",
                roster.join(", ")
            )));
        }

        let letter = match rank.as_str() {
            Some(text) if contract::is_agent_rank(text) => {
                contract::agent_rank_of_text(text).unwrap_or_default()
            }
            _ => {
                return Err(registry_bad_request(format!(
                    "`ranks.{agent}` must be a single rank letter from {} (highest) to {} (lowest), upper-case. Received {}.",
                    contract::HIGHEST_AGENT_RANK.as_str(),
                    contract::LOWEST_AGENT_RANK.as_str(),
                    wire_display(Some(rank))
                )));
            }
        };

        resolved.insert(agent.clone(), letter);
    }

    Ok(resolved)
}

/// `requirePrefixTurns` (`registry.ts:874-938`): validate one band and return DETACHED text-only
/// turns. `path` names the field and `member` the agent it belongs to, so every refusal points at the
/// exact element a client sent.
///
/// THE ONE VALIDATOR, and the reason it is crate-visible rather than private: THREE callers answer a
/// malformed band, and the contract promises they answer with one shape of 400 - the harness definition
/// write (`normalize_agent_prefixes` below), the HTTP edge of the session binding PATCH
/// (`api.ts:4060-4078`), and the store write behind it (`sessions.ts:8157-8171`,
/// `setSessionAgentPrefix`). A session client that PATCHes a body into a session and the same body into
/// a definition therefore reads the same sentence either way, with only the `path.member` label
/// distinguishing them.
///
/// WHY THIS COPIES rather than passing the input through: a wire value arrives as untyped JSON, and a
/// registry entry captures the band for the life of the definition. Holding the caller's array would
/// let a later mutation of a parsed request body rewrite what a saved definition seeds, so each turn
/// becomes a fresh `{ role, content }`. This is also the single place the "only a role and its text"
/// rule runs: `toolCalls`, `attachments`, `id` and an unknown key are DROPPED here (the contract states
/// a drop, not a refusal - swagger `:4794-4806`), and a `tool` role is refused because a preset that
/// seeded one would promise the model an exchange whose tool execution never happened.
pub(crate) fn require_prefix_turns(
    value: &Value,
    path: &str,
    member: &str,
) -> Result<Vec<HarnessPrefixTurn>, ApiError> {
    let label = if member.is_empty() {
        format!("`{path}`")
    } else {
        format!("`{path}.{member}`")
    };

    let Some(turns) = value.as_array() else {
        return Err(registry_bad_request(format!(
            "{label} must be an ARRAY of preset-history turns ({{ role, content }})."
        )));
    };

    if turns.len() > presets::MAX_PREFIX_TURNS {
        return Err(registry_bad_request(format!(
            "{label} cannot hold more than {} turns. Received {}.",
            presets::MAX_PREFIX_TURNS,
            turns.len()
        )));
    }

    turns
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let at = format!("{label}[{index}]");
            let Some(turn) = entry.as_object() else {
                return Err(registry_bad_request(format!(
                    "{at} must be an object with a \"role\" and a \"content\" string."
                )));
            };

            // The role check is an ALLOW-list rather than "not a tool role": an unknown role string is
            // a typo the client should see, and the engine would refuse it at construction anyway.
            let role = match turn.get("role").and_then(Value::as_str) {
                Some("system") => PrefixTurnRole::System,
                Some("user") => PrefixTurnRole::User,
                Some("assistant") => PrefixTurnRole::Assistant,
                _ => {
                    return Err(registry_bad_request(format!(
                        "{at} has role {}. A preset history turn is \"system\", \"user\", or \"assistant\" \u{2014} a tool turn belongs to a conversation, not to a preset.",
                        wire_display(turn.get("role"))
                    )));
                }
            };

            // Content is VERBATIM: no trim, because the bytes a client saved are the bytes the model
            // reads. An empty string is a real transcript entry and stays legal; a non-string (a
            // content-block array, a number, null) is refused, because narrowing content to text is
            // what keeps one band shape across every layer that stores it.
            let Some(content) = turn.get("content").and_then(Value::as_str) else {
                return Err(registry_bad_request(format!(
                    "{at} needs a string \"content\". Received {}.",
                    wire_display(turn.get("content"))
                )));
            };

            // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
            if content.encode_utf16().count() > presets::MAX_PREFIX_TURN_CHARS {
                return Err(registry_bad_request(format!(
                    "{at} content cannot exceed {} characters.",
                    presets::MAX_PREFIX_TURN_CHARS
                )));
            }

            Ok(HarnessPrefixTurn {
                role,
                content: content.to_string(),
            })
        })
        .collect()
}

/// `normalizeAgentPrefixes` (`registry.ts:946-970`): validate a per-agent preset-history map against
/// the FINAL member set. The fourth map of the same shape, so the same all-or-nothing key-membership
/// rule applies - a band for an agent this harness does not select can never be seeded, so it is a
/// visible 400 rather than a stored no-op.
///
/// THE ONE DIFFERENCE, and the reason this does not just map values: an EMPTY array is a meaningful
/// band ("this member seeds no history"), so an empty VALUE must not be read as an empty MAP.
/// `{ writer: [] }` therefore keeps its key - the read echoes the client's own choice back - while `{}`
/// still clears every band.
fn normalize_agent_prefixes(
    value: &Value,
    members: &[String],
) -> Result<std::collections::BTreeMap<String, Vec<HarnessPrefixTurn>>, ApiError> {
    let Some(entries) = value.as_object() else {
        return Err(registry_bad_request(
            "`agentPrefixes` must be an object mapping agent ids to arrays of preset-history turns."
                .to_string(),
        ));
    };

    let mut resolved = std::collections::BTreeMap::new();

    for (agent, turns) in entries {
        if !members.iter().any(|member| member == agent) {
            return Err(registry_bad_request(format!(
                "`agentPrefixes` names \"{agent}\", which is not a member of this harness. Members: {}.",
                members.join(", ")
            )));
        }

        resolved.insert(
            agent.clone(),
            require_prefix_turns(turns, "agentPrefixes", agent)?,
        );
    }

    Ok(resolved)
}

/// `requireModelId` (`registry.ts:395-424`).
fn require_model_id(value: &str, agent: &str) -> Result<String, ApiError> {
    let model = value.trim();

    if model.is_empty() {
        return Err(registry_bad_request(format!(
            "`agentModels.{agent}` cannot be empty."
        )));
    }

    // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
    if model.encode_utf16().count() > presets::MAX_MODEL_ID_LENGTH {
        return Err(registry_bad_request(format!(
            "`agentModels.{agent}` cannot exceed {} characters.",
            presets::MAX_MODEL_ID_LENGTH
        )));
    }

    if !presets::is_model_id(model) {
        return Err(registry_bad_request(format!(
            "`agentModels.{agent}` contains control characters."
        )));
    }

    Ok(model.to_string())
}

/// `normalizeAgentModels` (`registry.ts:439-464`), against the FINAL member set. An override for an
/// agent that is not selected could never apply, and accepting it would leave a client believing it
/// configured something, so an unknown key is refused explicitly.
fn normalize_agent_models(
    value: &std::collections::BTreeMap<String, String>,
    members: &[String],
) -> Result<std::collections::BTreeMap<String, String>, ApiError> {
    let mut resolved = std::collections::BTreeMap::new();

    for (agent, model) in value {
        if !members.iter().any(|member| member == agent) {
            return Err(registry_bad_request(format!(
                "`agentModels` names \"{agent}\", which is not a member of this harness. Members: {}.",
                members.join(", ")
            )));
        }

        resolved.insert(agent.clone(), require_model_id(model, agent)?);
    }

    Ok(resolved)
}

/// `requireInstruction` (`registry.ts:472-504`). A blank value is rejected rather than treated as
/// "no override": a client that means "revert to the trusted instruction" must OMIT the key (or send
/// `{}`), so an accidental paste of whitespace cannot silently discard a saved override.
fn require_instruction(value: &str, agent: &str) -> Result<String, ApiError> {
    let instruction = value.trim();

    if instruction.is_empty() {
        return Err(registry_bad_request(format!(
            "`agentInstructions.{agent}` cannot be empty. Omit the key to run the trusted instruction."
        )));
    }

    // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
    if instruction.encode_utf16().count() > presets::MAX_INSTRUCTION_LENGTH {
        return Err(registry_bad_request(format!(
            "`agentInstructions.{agent}` cannot exceed {} characters.",
            presets::MAX_INSTRUCTION_LENGTH
        )));
    }

    if !presets::is_instruction_text(instruction) {
        return Err(registry_bad_request(format!(
            "`agentInstructions.{agent}` contains control characters."
        )));
    }

    Ok(instruction.to_string())
}

/// `normalizeAgentInstructions` (`registry.ts:514-539`) - the same key-membership rule as the model
/// map, because an override for an agent this harness does not select can never apply.
fn normalize_agent_instructions(
    value: &std::collections::BTreeMap<String, String>,
    members: &[String],
) -> Result<std::collections::BTreeMap<String, String>, ApiError> {
    let mut resolved = std::collections::BTreeMap::new();

    for (agent, instruction) in value {
        if !members.iter().any(|member| member == agent) {
            return Err(registry_bad_request(format!(
                "`agentInstructions` names \"{agent}\", which is not a member of this harness. Members: {}.",
                members.join(", ")
            )));
        }

        resolved.insert(agent.clone(), require_instruction(instruction, agent)?);
    }

    Ok(resolved)
}

/// `requireAgentColor` (`registry.ts:564-594`). Trimmed, then matched against the hex shape. A
/// blank value is a rejection rather than "no color", for the same reason a blank model id and a
/// blank instruction are: a client that means "drop this swatch" must omit the key (or send `{}`),
/// so an accidental paste cannot silently discard a saved color while the UI still paints the old
/// one.
fn require_agent_color(value: &str, agent: &str) -> Result<String, ApiError> {
    let color = value.trim();

    if color.is_empty() {
        return Err(registry_bad_request(format!(
            "`agentColors.{agent}` cannot be empty. Omit the key to leave this agent uncolored."
        )));
    }

    if !presets::is_agent_color(color) {
        // `JSON.stringify(value)` quotes the RAW value the client sent, not the trimmed one
        // (`registry.ts:589`), so the refusal shows exactly what was rejected.
        return Err(registry_bad_request(format!(
            "`agentColors.{agent}` must be a CSS hex color (#rgb, #rgba, #rrggbb or #rrggbbaa). Received {}.",
            serde_json::Value::String(value.to_owned())
        )));
    }

    Ok(color.to_string())
}

/// `normalizeAgentColors` (`registry.ts:596-629`), against the FINAL member set - the third map of
/// the same shape with the same two rules as the model and instruction maps: an entry for an agent
/// this harness does not select can never be painted, so it is a visible 400 rather than a stored
/// no-op, and the whole map is validated all-or-nothing.
fn normalize_agent_colors(
    value: &std::collections::BTreeMap<String, String>,
    members: &[String],
) -> Result<std::collections::BTreeMap<String, String>, ApiError> {
    let mut resolved = std::collections::BTreeMap::new();

    for (agent, color) in value {
        if !members.iter().any(|member| member == agent) {
            return Err(registry_bad_request(format!(
                "`agentColors` names \"{agent}\", which is not a member of this harness. Members: {}.",
                members.join(", ")
            )));
        }

        resolved.insert(agent.clone(), require_agent_color(color, agent)?);
    }

    Ok(resolved)
}

/// Drops overrides whose agent is no longer a member (`registry.ts:1157-1161`, `:1171-1175`).
fn retain_members(
    map: &std::collections::BTreeMap<String, String>,
    members: &[String],
) -> std::collections::BTreeMap<String, String> {
    map.iter()
        .filter(|(agent, _)| members.iter().any(|member| member == *agent))
        .map(|(agent, value)| (agent.clone(), value.clone()))
        .collect()
}

/// The rank map of a patch that did NOT state one: the surviving members' ranks, with a rank for a
/// removed member dropped because it "is not a target and not a delegator" (`registry.ts:2136-2138`).
/// An empty result is meaningful to the CALLER, which turns it into the rule-1 default rather than
/// reading it as the client's `ranks: {}`.
fn retain_ranks(
    ranks: &std::collections::BTreeMap<String, AgentRank>,
    members: &[String],
) -> std::collections::BTreeMap<String, AgentRank> {
    ranks
        .iter()
        .filter(|(agent, _)| members.iter().any(|member| member == *agent))
        .map(|(agent, rank)| (agent.clone(), *rank))
        .collect()
}

/// The preset-history map of a patch that did NOT state one: every surviving member's band, with the
/// bands of removed members dropped (`registry.ts:2201-2205`). An empty band a client stated keeps its
/// key, so `{ editor: [] }` survives this filter exactly as it survived validation.
///
/// No copy discipline is needed here: Rust owns these turn structs, so a retained map can never alias
/// a parsed request body the way TS's one-level spread could (`registry.ts` `copyPrefixMap`).
fn retain_prefixes(
    prefixes: &std::collections::BTreeMap<String, Vec<HarnessPrefixTurn>>,
    members: &[String],
) -> std::collections::BTreeMap<String, Vec<HarnessPrefixTurn>> {
    prefixes
        .iter()
        .filter(|(agent, _)| members.iter().any(|member| member == *agent))
        .map(|(agent, turns)| (agent.clone(), turns.clone()))
        .collect()
}

/// Resolves the two prose fields of a patch, including the honest-on-upgrade rule
/// (`registry.ts:1205-1217`): an executable upgrade of a scripted template adopts that template's
/// `live` title/description ONLY while the current strings are still the shipped ones, so an
/// operator's earlier edit survives.
fn resolved_prose(
    current: &HarnessDescriptor,
    request: &UpdateHarnessRequest,
    upgraded: bool,
) -> Result<(String, String), ApiError> {
    let shipped = presets::shipped_presets()
        .into_iter()
        .find(|preset| preset.name == current.name);

    // The upgrade copy a scripted template carries (`presets.ts:293-297`).
    let live = current
        .live
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|live| {
            let title = live.get("title").and_then(Value::as_str)?;
            let description = live.get("description").and_then(Value::as_str)?;

            Some((title.to_string(), description.to_string()))
        });

    let (live_title, live_description) = live.unwrap_or_default();

    // Honest prose on upgrade: adopt the `live` strings ONLY while the current ones are still the
    // shipped template's, so an operator edit is never overwritten (`registry.ts:1205-1217`).
    let adopt_title = upgraded
        && !live_title.is_empty()
        && shipped
            .as_ref()
            .is_some_and(|preset| preset.title == current.title);

    let adopt_description = upgraded
        && !live_description.is_empty()
        && shipped
            .as_ref()
            .is_some_and(|preset| preset.description == current.description);

    let title = match &request.title {
        Some(value) => normalize_title(value)?,
        None if adopt_title => live_title,
        None => current.title.clone(),
    };

    let description = match &request.description {
        Some(value) => normalize_description(value)?,
        None if adopt_description => live_description,
        None => current.description.clone(),
    };

    Ok((title, description))
}

// ---------------------------------------------------------------------------
// Restore equality (`registry.ts:643-672`, `:803-823`)
// ---------------------------------------------------------------------------

/// Deterministic record equality for the restore pass (`presetRecordEquals`, `registry.ts:1261-1264`
/// over `presetToRecordShape` at `:1267-1342`).
///
/// Compares the MODELLED fields only, with absent and empty normalized together - `agentModels`
/// absent and `agentModels: {}` are the same fact in this catalogue (`registry.ts:1084-1090`) - and
/// ignores each record's `extra` catch-all, which is foreign-document noise rather than a
/// definition. The two roster fields (`entry`, `ranks`) are part of the fixed shape like every other
/// executable field, so a saved row that moved routing or authority compares UNEQUAL and is replayed.
#[must_use]
pub fn preset_records_equal(left: &HarnessPresetRecord, right: &HarnessPresetRecord) -> bool {
    let same_list = |left: &Option<Vec<String>>, right: &Option<Vec<String>>| {
        left.clone().unwrap_or_default() == right.clone().unwrap_or_default()
    };

    let same_map = |left: &Option<std::collections::BTreeMap<String, String>>,
                    right: &Option<std::collections::BTreeMap<String, String>>| {
        left.clone().unwrap_or_default() == right.clone().unwrap_or_default()
    };

    // `sortedRanks` on both sides (`registry.ts:1210-1213`), which is what a `BTreeMap` already is,
    // with an absent map normalized to an empty one.
    let same_ranks =
        |left: &Option<std::collections::BTreeMap<String, AgentRank>>,
         right: &Option<std::collections::BTreeMap<String, AgentRank>>| {
            left.clone().unwrap_or_default() == right.clone().unwrap_or_default()
        };

    let same_prefixes =
        |left: &Option<std::collections::BTreeMap<String, Vec<HarnessPrefixTurn>>>,
         right: &Option<std::collections::BTreeMap<String, Vec<HarnessPrefixTurn>>>| {
            left.clone().unwrap_or_default() == right.clone().unwrap_or_default()
        };

    left.name == right.name
        && left.title == right.title
        && left.description == right.description
        && left.requires_endpoint == right.requires_endpoint
        // ROUTING AND AUTHORITY (`registry.ts:1271-1272`). `entry` is compared as stored: a pre-rank
        // row carries `primary` instead, so the two sides differ and the row is replayed rather than
        // skipped, which is what upgrades it to the current spelling on the next write.
        && left.entry == right.entry
        && same_ranks(&left.ranks, &right.ranks)
        // The LEGACY pair stays in the comparison so a pre-rank document never compares equal to a
        // current row and silently loses its roster on restore.
        && left.primary == right.primary
        && left.subagents == right.subagents
        && left.agents == right.agents
        && left.tools == right.tools
        && same_list(&left.environment_tools, &right.environment_tools)
        && same_map(&left.agent_models, &right.agent_models)
        && same_map(&left.agent_instructions, &right.agent_instructions)
        // R2: colors are part of the fixed shape too, so a saved row that carries them compares
        // equal to the entry rebuilt from it (`registry.ts:790-795`).
        && same_map(&left.agent_colors, &right.agent_colors)
        // R7: part of the fixed key order (`registry.ts:1325-1333`), so a row carrying a session-defined
        // agent never compares equal to an entry that does not.
        && same_map(&left.custom_agents, &right.custom_agents)
        // U6: part of the fixed shape as well (`registry.ts:1334-1342`). Two consequences matter: a saved
        // row that carries a band compares EQUAL to the entry rebuilt from it, and a row naming a band
        // the current entry no longer holds compares UNEQUAL - which is the outcome that routes the
        // restore through the replay instead of skipping it.
        && same_prefixes(&left.agent_prefixes, &right.agent_prefixes)
        && left.builtin == right.builtin
        && (left.template.clone().unwrap_or_else(|| "live".to_string())
            == right.template.clone().unwrap_or_else(|| "live".to_string()))
        && left.live == right.live
}

/// The entry/rank pair a durable row actually states, with the pre-rank spellings folded in - the same
/// derivation `descriptor::from_record` applies, so the two readers can never disagree about what a row
/// means.
fn record_entry_ranks(
    record: &HarnessPresetRecord,
) -> (String, std::collections::BTreeMap<String, AgentRank>) {
    let entry = record
        .entry
        .clone()
        .or_else(|| record.primary.clone())
        .or_else(|| record.agents.first().cloned())
        .unwrap_or_else(|| record.name.clone());

    let ranks = record
        .ranks
        .clone()
        .unwrap_or_else(|| presets::resolve_roster_ranks(&record.agents, &entry, None));

    (entry, ranks)
}

/// `isLegacyShippedDefinition` (`registry.ts:1583-1622`): proves a saved built-in row is an
/// unmodified shipped default, which is the only condition under which a refresh may replace it. A
/// per-agent model, instruction, color, preset-history, or custom-agent map is a customisation even
/// when the team matches, because each is executable state.
#[must_use]
pub fn is_legacy_shipped_definition(
    definition: &presets::LegacyPresetDefinition,
    record: &HarnessPresetRecord,
) -> bool {
    let same = |left: &[String], right: &[String]| left == right;
    let (entry, ranks) = record_entry_ranks(record);

    record.requires_endpoint == definition.requires_endpoint
        // RANK comparison, canonicalised the way `presetToRecord` writes it: a saved row matches the
        // legacy shape only when both the roster order AND the rank assignment are untouched. Rank is
        // executable authority - a row whose planner sits at Rank B instead of Rank A is a real
        // customisation and must be replayed, not refreshed away (`registry.ts:1599-1609`).
        && entry == definition.entry
        && ranks == definition.ranks
        && same(&record.agents, &definition.agents)
        && same(&record.tools, &definition.tools)
        && same(
            &record.environment_tools.clone().unwrap_or_default(),
            &definition.environment_tools,
        )
        && record
            .template
            .clone()
            .unwrap_or_else(|| "live".to_string())
            == definition.template
        && record.agent_models.clone().unwrap_or_default().is_empty()
        && record
            .agent_instructions
            .clone()
            .unwrap_or_default()
            .is_empty()
        // R2: a saved color is a customisation as well (`registry.ts:979-982`) - the prose-only
        // refresh would drop the row's map, so an operator who colored one member of a built-in
        // keeps that decision instead of losing it to an upgrade.
        && record.agent_colors.clone().unwrap_or_default().is_empty()
        // U6: so is a saved preset history (`registry.ts:1614-1617`). No shipped template seeds one,
        // and a row that states a band - EVEN AN EMPTY ONE - records an executable decision about what
        // the next session's transcript starts with, so it is replayed rather than refreshed.
        && record.agent_prefixes.clone().unwrap_or_default().is_empty()
        // R7: and so is a session-defined agent (`registry.ts:1618-1621`): a definition row never has
        // one, so this only fires on a foreign document - where dropping its custom roster on a
        // prose refresh would be a silent roster change.
        && record.custom_agents.clone().unwrap_or_default().is_empty()
}

/// Trims and de-duplicates a name list while preserving order (`persistence.ts:1158-1171` and the
/// registry's own tombstone seeding at `registry.ts:979-985`).
fn normalize_names(names: Vec<String>) -> Vec<String> {
    let mut kept: Vec<String> = Vec::with_capacity(names.len());

    for entry in names {
        let name = entry.trim().to_string();

        if !name.is_empty() && !kept.contains(&name) {
            kept.push(name);
        }
    }

    kept
}
