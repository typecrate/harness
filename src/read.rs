//! The HOST-domain read models: every projection that turns registry/catalogue state into the
//! snake_case wire objects the REST surface returns.
//!
//! TypeScript origin: `packages/underload/harness/src/server/api-reads.ts`, specifically the host
//! (non-session) half of it - `providerDefaultsOf` (`:149`), `runtimeSettingsOf` (`:156`),
//! `harnessCardOf` (`:1026`), `harnessDefinitionOf` (`:1043`), `toolSummaryOf` (`:1130`),
//! `toolDetailOf` (`:1155`), `agentSummaryOf` (`:1174`), `agentProfileOf` (`:1200`) - plus the
//! `configDocument` builder in `src/server/api.ts:1329-1334` and the model index
//! (`harness-api.ts:781`).
//!
//! WHY A SEPARATE MODULE
//! ---------------------
//! The session half of `api-reads.ts` needs a `SessionRecord`, a projector, and a live harness -
//! none of which exist yet. These eight builders need only the registry, the catalogue, and the
//! hub, so they can be ported EXACTLY, and both the later store stage and the HTTP layer read
//! through one place instead of re-deriving a shape. The field sets and nullability below are the
//! contract's, and a projection that omitted a field would silently change the wire.
//!
//! THE `Value`-versus-struct boundary
//! ---------------------------------
//! Every function here returns a STRUCT from `contract`, so a later stage cannot send a misspelled
//! field. `Option` + `skip_serializing_if` model the "emitted only when non-empty" rules TS applies
//! with spread conditionals (`agentModels`, `agentInstructions`, `availability`,
//! `overridden_fields`), which is what keeps an untouched card byte-identical to the shape it had
//! before the field existed.

use crate::agents::AgentCatalogue;
use crate::config::{self, HarnessRuntimeSettings, ServerConfig};
use crate::contract::{
    AgentProfile, AgentSummary, GlobalConfigDocument, HarnessCard, HarnessDefinition, ModelIndex,
    ProviderDefaults, RuntimeSettings, ToolAvailability, ToolAvailabilityState, ToolDetail,
    ToolExecutionScope, ToolExecutionScopeKind, ToolSummary, ToolVisibility,
};
use crate::events::EventHub;
use crate::registry::HarnessDescriptor;

/// The writable paths of the global config document (`api.ts:1332`).
pub const WRITABLE_CONFIG_FIELDS: [&str; 3] =
    ["settings.base_url", "settings.model", "settings.api_key"];

/// The group a created (non-catalogue) tool is listed under when its entry states none
/// (`toolSummaryOf`, `api-reads.ts:1136`).
pub const CREATED_TOOLS_GROUP: &str = "Created tools";

/// Key-free endpoint view: the contract never leaks a credential, only its presence
/// (`providerDefaultsOf`, `api-reads.ts:148-153`).
///
/// Delegates to [`config::provider_defaults`] so the "is a key present" rule has exactly one
/// owner; the trim is load-bearing, because an explicitly empty key means "no key" (T28).
#[must_use]
pub fn provider_defaults(settings: &HarnessRuntimeSettings) -> ProviderDefaults {
    config::provider_defaults(settings)
}

/// The read-only runtime block, projected snake_case from the deployment config
/// (`runtimeSettingsOf`, `api-reads.ts:155-164`).
#[must_use]
pub fn runtime_settings_of(config: &ServerConfig) -> RuntimeSettings {
    config::runtime_settings(config)
}

/// GLOBAL configuration (`api.ts:1329-1334`): the session-visible provider defaults plus
/// read-only deployment runtime facts. A session's own merged settings are per-session state and
/// are reported on the session, never here.
///
/// `revision` is the CURRENT `config` counter. The publisher owns its counter
/// (`api-events.ts:302-306`), so a route that mutates the configuration calls
/// `hub.bump("config", None)` FIRST, then `hub.publish(...)`, then this builder - which reads
/// the counter the bump produced and can therefore never disagree with the event.
#[must_use]
pub fn global_config_document(config: &ServerConfig, hub: &EventHub) -> GlobalConfigDocument {
    GlobalConfigDocument {
        defaults: provider_defaults(&config.settings()),
        runtime: Some(runtime_settings_of(config)),
        writable_fields: WRITABLE_CONFIG_FIELDS
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        revision: hub.revision("config", None),
    }
}

/// A definition card: identity, membership, declared tools, and the revision counter
/// (`harnessCardOf`, `api-reads.ts:1026-1037`).
///
/// `tools` is the card's REGISTERED list - the declared toolbox plus the environment-backed names
/// this host can run (`descriptor.ts:69-95`) - not the raw preset list. `subagents` is ALWAYS on
/// the wire (`harness-api.ts:428`): an empty array states that `delegate` is not registered, so
/// it must not be skipped.
#[must_use]
pub fn harness_card_of(definition: &HarnessDescriptor, hub: &EventHub) -> HarnessCard {
    harness_card_of_revision(
        definition,
        hub.revision("definition", Some(&definition.name)),
    )
}

/// The same projection with an explicit revision, for a caller that has no hub (a hub-free listing
/// passes 0) or that already resolved the counter.
#[must_use]
pub fn harness_card_of_revision(definition: &HarnessDescriptor, revision: u64) -> HarnessCard {
    HarnessCard {
        name: definition.name.clone(),
        title: definition.title.clone(),
        description: definition.description.clone(),
        requires_endpoint: definition.requires_endpoint,
        primary: definition.primary.clone(),
        subagents: definition.subagents.clone(),
        agents: definition.agents.clone(),
        tools: definition.registered_tools(),
        builtin: definition.builtin,
        definition_revision: revision,
    }
}

/// The identified definition read: a card plus the per-agent model overrides, the resolved
/// core-tool set, and the instruction SUMMARY (provenance + length, never the text)
/// (`harnessDefinitionOf`, `api-reads.ts:1043-1056`).
#[must_use]
pub fn harness_definition_of(
    definition: &HarnessDescriptor,
    hub: &EventHub,
    agents: &AgentCatalogue,
) -> HarnessDefinition {
    harness_definition_of_revision(
        definition,
        hub.revision("definition", Some(&definition.name)),
        agents,
    )
}

/// The identified definition read with an explicit revision.
#[must_use]
pub fn harness_definition_of_revision(
    definition: &HarnessDescriptor,
    revision: u64,
    agents: &AgentCatalogue,
) -> HarnessDefinition {
    HarnessDefinition {
        card: harness_card_of_revision(definition, revision),
        core_tools: Some(definition.core_tools()),
        // Emitted only when the harness overrides something, so a card that changes nothing keeps
        // its exact pre-R9 shape (`api-reads.ts:1052-1054`).
        agent_models: if definition.agent_models.is_empty() {
            None
        } else {
            Some(definition.agent_models.clone())
        },
        // The per-agent display colors, emitted under the same non-empty rule as `agent_models`, so
        // an uncolored definition keeps its exact pre-R2 shape (`api-reads.ts:1099-1106`). This is
        // the field a client paints swatches from; a member it omits is uncolored and the host
        // applies its own derivation.
        agent_colors: if definition.agent_colors.is_empty() {
            None
        } else {
            Some(definition.agent_colors.clone())
        },
        instruction_summary: Some(crate::registry::instruction_summary_of(definition, agents)),
    }
}

/// A trusted profile as a list row: identity plus whether anything overrides it
/// (`agentSummaryOf`, `api-reads.ts:1174-1187`).
///
/// `has_instruction_override` is the instruction specifically, because that is the field a prompt
/// editor highlights; `has_global_override` is ANY stated field, because that is what makes a
/// "restore the shipped profile" affordance meaningful. Both come from the overlay, never from
/// comparing text.
///
/// `_hub` is accepted for symmetry with the other projections (the tools/agents PAGE revision is
/// stamped by the page wrapper, `api.ts:1672-1675`, not by a row).
#[must_use]
pub fn agent_summary_of(
    agents: &AgentCatalogue,
    name: &str,
    _hub: &EventHub,
) -> Option<AgentSummary> {
    let profile = agents.effective(name)?;
    let overrides = agents.overrides_of(name);

    Some(AgentSummary {
        name: profile.name,
        title: profile.title,
        description: profile.description,
        has_instruction_override: overrides.instruction.is_some(),
        has_global_override: !overrides.is_empty(),
    })
}

/// The full trusted profile as a client needs it to choose a primary or subagent
/// (`agentProfileOf`, `api-reads.ts:1200-1222`).
///
/// `instruction_source` says which layer the text the client is looking at came from, and
/// `overridden_fields` names every field the global layer currently replaces, so a UI can offer
/// "restore" per field instead of overwriting the whole profile with one PATCH.
///
/// `referenced_by` is supplied by the caller: only the harness registry knows which definitions
/// select this profile.
#[must_use]
pub fn agent_profile_of(
    agents: &AgentCatalogue,
    name: &str,
    hub: &EventHub,
    referenced_by: Vec<String>,
) -> Option<AgentProfile> {
    let profile = agents.effective(name)?;
    let overrides = agents.overrides_of(name);

    Some(AgentProfile {
        name: profile.name,
        title: profile.title,
        description: profile.description,
        instruction: profile.instruction,
        guidelines: profile.guidelines,
        instruction_source: if overrides.instruction.is_some() {
            crate::contract::InstructionSource::Global
        } else {
            crate::contract::InstructionSource::Catalogue
        },
        overridden_fields: if overrides.is_empty() {
            None
        } else {
            Some(overrides.replaced_fields())
        },
        referenced_by,
        revision: hub.revision("agents", Some(name)),
    })
}

/// What the HOST read model knows about one tool row.
///
/// Mirrors the `CatalogueEntry` fields `toolSummaryOf`/`toolDetailOf` actually read
/// (`api-reads.ts:1130-1163`), NOT the whole TS catalogue entry: the tool stage builds these from
/// its own registry, which keeps the projection free of a tool implementation dependency.
///
/// `visibility` is a `String` because it arrives from tool metadata (`'core' | 'searchable'`, and
/// anything else falls back to `searchable` exactly as TS does). `manual` is the entry's own flag
/// (only an entry with a real implementation can run standalone) and becomes
/// `contract::ToolDetail.execution.manual` (`api-reads.ts:1160-1161`), while `session_bound`
/// picks `scope` (`:1159`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolCatalogueEntry {
    pub name: String,
    pub group: String,
    pub description: String,
    pub tags: Vec<String>,
    /// `'core' | 'searchable'`; any other spelling projects as `searchable`.
    pub visibility: String,
    pub session_bound: bool,
    pub parameters: Option<serde_json::Value>,
    pub availability: Option<ToolAvailability>,
    pub manual: bool,
}

impl ToolCatalogueEntry {
    /// An entry with its argument schema attached, for the identified read only (the list row never
    /// carries it).
    #[must_use]
    pub fn with_parameters(mut self, parameters: Option<serde_json::Value>) -> Self {
        self.parameters = parameters;
        self
    }
}

/// A catalogue row (`toolSummaryOf`, `api-reads.ts:1126-1152`).
///
/// Defaults, exactly as TS states them: an entry that names no group is listed under
/// "Created tools", an entry that names no visibility is `searchable`, an entry with no tags lists
/// none, and `availability` is emitted only when the entry carries one (a listed tool the server can
/// always run sends nothing).
#[must_use]
pub fn tool_summary_of(entry: &ToolCatalogueEntry, _hub: &EventHub) -> ToolSummary {
    ToolSummary {
        name: entry.name.clone(),
        group: group_of(entry),
        description: entry.description.clone(),
        tags: entry.tags.clone(),
        visibility: visibility_of(entry),
        availability: entry.availability.clone(),
        session_bound: entry.session_bound,
    }
}

/// A catalogue entry plus the schema and the execution gate only the detail read carries
/// (`toolDetailOf`, `api-reads.ts:1154-1163`).
#[must_use]
pub fn tool_detail_of(entry: &ToolCatalogueEntry, hub: &EventHub) -> ToolDetail {
    ToolDetail {
        summary: tool_summary_of(entry, hub),
        parameters: entry.parameters.clone(),
        execution: execution_scope_of(entry),
    }
}

/// How an entry may be called from HTTP: a session-bound tool needs the session in the path,
/// and only an entry with a real implementation may be invoked standalone
/// (`toolDetailOf`'s `execution` member, `api-reads.ts:1158-1162`). Both halves are stated on
/// the wire, so the answer is the closed `{scope, manual}` object (`harness-api.ts:660-664`) -
/// never a bare scope string.
#[must_use]
pub fn execution_scope_of(entry: &ToolCatalogueEntry) -> ToolExecutionScope {
    ToolExecutionScope {
        scope: if entry.session_bound {
            ToolExecutionScopeKind::SessionRequired
        } else {
            ToolExecutionScopeKind::Global
        },
        // `entry?.tool !== undefined` (`api-reads.ts:1161`): the Rust stand-in for "this entry
        // has an implementation" is the entry's own `manual` flag.
        manual: entry.manual,
    }
}

/// `entry?.group ?? 'Created tools'` (`api-reads.ts:1136`). An empty string is treated as
/// "not stated", which is what a Rust `String` default means.
#[must_use]
fn group_of(entry: &ToolCatalogueEntry) -> String {
    if entry.group.trim().is_empty() {
        return CREATED_TOOLS_GROUP.to_string();
    }

    entry.group.clone()
}

/// `entry?.visibility ?? 'searchable'` (`api-reads.ts:1139`).
#[must_use]
fn visibility_of(entry: &ToolCatalogueEntry) -> ToolVisibility {
    if entry.visibility == "core" {
        ToolVisibility::Core
    } else {
        ToolVisibility::Searchable
    }
}

/// The provider's model ids, in provider order, deduplicated (`ModelIndex`,
/// `harness-api.ts:776-785`; the dedupe and order come from `transport/models.ts`).
///
/// An empty list is a valid answer: the provider answered and served nothing usable, which is
/// different from 502.
#[must_use]
pub fn model_index(models: Vec<String>, observed_at: u64) -> ModelIndex {
    let mut kept: Vec<String> = Vec::with_capacity(models.len());

    for model in models {
        if !kept.contains(&model) {
            kept.push(model);
        }
    }

    ModelIndex {
        models: kept,
        observed_at,
    }
}

/// The availability block of a listed tool that needs a credential the host has not configured
/// (`api-reads.ts:1140-1150`), exposed here so the tool stage builds the same shape rather than
/// re-deriving it from the wire types.
#[must_use]
pub fn unconfigured_availability(environment: Vec<String>, hint: String) -> ToolAvailability {
    ToolAvailability {
        state: ToolAvailabilityState::Unconfigured,
        environment: Some(environment),
        hint: Some(hint),
    }
}
