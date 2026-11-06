//! The harness definition registry: shipped presets, operator edits, and durable tombstones.
//!
//! TypeScript origin: `packages/underload/harness/src/server/harnesses/registry.ts`. The live
//! half of that file - `create` factories, `liveFactory`, the scripted/flaky template rebuild -
//! belongs to the runtime stage, so what lands here is the DEFINITION plane: the catalogue, the
//! validation, the create/patch/delete rules, the tombstones, and the restore policy.
//!
//! INVARIANTS (`registry.ts:17-28`)
//! -------------------------------
//!   * exactly one primary; `agents` is derived as `[primary, ...subagents]`;
//!   * a subagent is never the primary, never duplicated, and must be a known TRUSTED agent id;
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
    CreateHarnessRequest, DeleteHarnessResult, HarnessCard, HarnessDefinition,
    HarnessInstructionSummaryEntry, InstructionSource, UpdateHarnessRequest,
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
                        primary: record.primary.clone(),
                        subagents: Some(record.subagents.clone()),
                        agent_models: record.agent_models.clone(),
                        agent_instructions: record.agent_instructions.clone(),
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
                    primary: executable.then_some(record.primary.clone()),
                    subagents: executable.then_some(record.subagents.clone()),
                    agent_models: executable
                        .then_some(record.agent_models.clone().unwrap_or_default()),
                    agent_instructions: executable
                        .then_some(record.agent_instructions.clone().unwrap_or_default()),
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

    /// Adds a NEW definition (`create`, `registry.ts:1017-1112`).
    ///
    /// A client selects trusted agent names and may record per-agent model ids and instruction
    /// overrides; it cannot supply a provider, a credential, a transport, or a tool list.
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] for a missing/badly-shaped name or primary, a reserved alias, an
    /// unknown or blank agent, an over-long text, a duplicate subagent, a subagent that names the
    /// primary, or an override key that is not a member; [`ApiError::conflict`] when the name is
    /// taken (case-insensitively) or when the patch introduces a retired agent.
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

        if request.primary.trim().is_empty() {
            return Err(ApiError::bad_request(
                "A harness definition requires a \"primary\" agent.",
            ));
        }

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

        let primary = require_agent(&request.primary, "primary", agents, None)?;
        let subagents = match &request.subagents {
            Some(list) => normalize_subagents(list, &primary, agents, None)?,
            None => Vec::new(),
        };

        // The final member set: one primary then its allowlist. The per-agent maps are validated
        // against exactly this, so a key can never name an agent the harness does not contain.
        let members = std::iter::once(primary.clone())
            .chain(subagents.clone())
            .collect::<Vec<String>>();

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
            primary,
            subagents,
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

    /// Replaces one definition with a NEW value (`update`, `registry.ts:1114-1262`).
    ///
    /// `name` is immutable - a rename would break sessions, bookmarks, and the UI's selected card -
    /// and `agent_models` / `agent_instructions` REPLACE the map they name (`{}` clears it).
    /// Overrides for a member this patch removed are DROPPED: a model or instruction for an agent
    /// that is no longer selected cannot apply, and carrying it would silently resurrect when the
    /// agent returned.
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
            && request.primary.is_none()
            && request.subagents.is_none()
            && request.agent_models.is_none()
            && request.agent_instructions.is_none()
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

        let primary = match &request.primary {
            Some(value) => require_agent(value, "primary", agents, Some(&previously))?,
            None => current.primary.clone(),
        };

        // Changing the primary without restating the allowlist must not leave the new primary
        // inside `subagents` (`registry.ts:1140-1146`).
        let subagents = match &request.subagents {
            Some(list) => normalize_subagents(list, &primary, agents, Some(&previously))?,
            None => current
                .subagents
                .iter()
                .filter(|entry| **entry != primary)
                .cloned()
                .collect(),
        };

        let members = std::iter::once(primary.clone())
            .chain(subagents.clone())
            .collect::<Vec<String>>();

        let agent_models = match &request.agent_models {
            Some(map) => normalize_agent_models(map, &members)?,
            None => retain_members(&current.agent_models, &members),
        };

        let agent_instructions = match &request.agent_instructions {
            Some(map) => normalize_agent_instructions(map, &members)?,
            None => retain_members(&current.agent_instructions, &members),
        };

        // Colors follow the identical rule for the identical reason (`registry.ts:1377-1389`): a
        // swatch for an agent this patch removed can never be painted, so it is dropped, and a
        // stated map REPLACES the stored one (`{}` clears every color).
        let agent_colors = match &request.agent_colors {
            Some(map) => normalize_agent_colors(map, &members)?,
            None => retain_members(&current.agent_colors, &members),
        };

        // An executable change on a SCRIPTED template upgrades it to a live harness and corrects the
        // descriptor fields that would otherwise still advertise the offline fixture
        // (`registry.ts:1203-1256`).
        let executable = request.primary.is_some()
            || request.subagents.is_some()
            || request.agent_models.is_some()
            || request.agent_instructions.is_some()
            // R2: a color is display-only, but it is still EXECUTABLE in this sense (`registry.ts
            // :1411-1416`): the map reaches every agent built from this definition, so a stated
            // color must rebuild the factory - and, on the scripted demo, upgrade it to a live
            // harness exactly like an instruction edit does.
            || request.agent_colors.is_some();

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
            primary,
            subagents,
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

/// Projects a definition onto the FROZEN descriptor a session records (`describeHarness`,
/// `descriptor.ts:86-109`, in the field order `persistence.ts:412-428` validates).
#[must_use]
pub fn descriptor_record_of(definition: &HarnessDescriptor) -> HarnessDescriptorRecord {
    HarnessDescriptorRecord {
        name: definition.name.clone(),
        title: definition.title.clone(),
        description: definition.description.clone(),
        requires_endpoint: definition.requires_endpoint,
        primary: definition.primary.clone(),
        subagents: definition.subagents.clone(),
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
        builtin: definition.builtin,
        editable: definition.editable,
        extra: serde_json::Map::new(),
    }
}

/// Rebuilds a definition from the frozen descriptor inside a session record
/// (`copyDescriptor`'s inverse; `persistence.ts:403-429`).
#[must_use]
pub fn definition_from_descriptor_record(record: &HarnessDescriptorRecord) -> HarnessDescriptor {
    HarnessDescriptor {
        name: record.name.clone(),
        title: record.title.clone(),
        description: record.description.clone(),
        requires_endpoint: record.requires_endpoint,
        primary: record.primary.clone(),
        subagents: record.subagents.clone(),
        agents: record.agents.clone(),
        tools: record.tools.clone(),
        // A frozen descriptor already carries the REGISTERED list (environment tools included), so
        // the environment half is empty on the rebuilt copy: re-deriving it would double-list.
        environment_tools: Vec::new(),
        agent_models: record.agent_models.clone().unwrap_or_default(),
        agent_instructions: record.agent_instructions.clone().unwrap_or_default(),
        agent_colors: record.agent_colors.clone().unwrap_or_default(),
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

/// `normalizeSubagents` (`registry.ts:358-390`). Duplicates are an error rather than silently
/// collapsed: a request that names an agent twice is ambiguous, and the registry should not guess
/// which occurrence was meant.
fn normalize_subagents(
    value: &[String],
    primary: &str,
    agents: &AgentCatalogue,
    previously_selected: Option<&[String]>,
) -> Result<Vec<String>, ApiError> {
    if value.len() > presets::MAX_SUBAGENTS {
        return Err(registry_bad_request(format!(
            "`subagents` cannot exceed {} entries.",
            presets::MAX_SUBAGENTS
        )));
    }

    let mut resolved: Vec<String> = Vec::with_capacity(value.len());

    for entry in value {
        let name = require_agent(entry, "subagents", agents, previously_selected)?;

        if name == primary {
            return Err(registry_bad_request(format!(
                "Subagent \"{name}\" is the primary agent; a harness has exactly one primary."
            )));
        }

        if resolved.contains(&name) {
            return Err(registry_bad_request(format!(
                "Subagent \"{name}\" is listed more than once."
            )));
        }

        resolved.push(name);
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

/// Deterministic record equality for the restore pass (`presetRecordEquals`, `registry.ts:643-644`).
///
/// Compares the MODELLED fields only, with absent and empty normalized together - `agentModels`
/// absent and `agentModels: {}` are the same fact in this catalogue (`registry.ts:1084-1090`) - and
/// ignores each record's `extra` catch-all, which is foreign-document noise rather than a
/// definition.
#[must_use]
pub fn preset_records_equal(left: &HarnessPresetRecord, right: &HarnessPresetRecord) -> bool {
    let same_list = |left: &Option<Vec<String>>, right: &Option<Vec<String>>| {
        left.clone().unwrap_or_default() == right.clone().unwrap_or_default()
    };

    let same_map = |left: &Option<std::collections::BTreeMap<String, String>>,
                    right: &Option<std::collections::BTreeMap<String, String>>| {
        left.clone().unwrap_or_default() == right.clone().unwrap_or_default()
    };

    left.name == right.name
        && left.title == right.title
        && left.description == right.description
        && left.requires_endpoint == right.requires_endpoint
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
        && left.builtin == right.builtin
        && (left.template.clone().unwrap_or_else(|| "live".to_string())
            == right.template.clone().unwrap_or_else(|| "live".to_string()))
        && left.live == right.live
}

/// `isLegacyShippedDefinition` (`registry.ts:803-823`): proves a saved built-in row is an
/// unmodified shipped default, which is the only condition under which a refresh may replace it. A
/// per-agent model OR instruction map is a customisation even when the team matches, because both
/// are executable state.
#[must_use]
pub fn is_legacy_shipped_definition(
    definition: &presets::LegacyPresetDefinition,
    record: &HarnessPresetRecord,
) -> bool {
    let same = |left: &[String], right: &[String]| left == right;

    record.requires_endpoint == definition.requires_endpoint
        && record.primary == definition.primary
        && same(&record.subagents, &definition.subagents)
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
