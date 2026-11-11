//! One harness DEFINITION as the registry holds it, as a session freezes it, and as a card reports
//! it.
//!
//! TypeScript origin: `src/server/harnesses/types.ts` (`HarnessPreset`, minus the `create`
//! factory), `src/server/harnesses/descriptor.ts` (`describeHarness`), and
//! `src/server/protocol.ts:261-301` (`HarnessDescriptor`, the frozen wire view).
//!
//! WHY ONE TYPE FOR THREE JOBS
//! ---------------------------
//! In TS the same field set appears three times: the preset metadata a registry stores, the
//! descriptor a session freezes inside `HarnessSessionRecord.definition`, and the card a client
//! reads. `descriptor.ts:3-12` exists for exactly that reason - "Keeping one `describeHarness()`
//! means the card a client sees before creating a session and the frozen definition inside that
//! session can never disagree" - so the Rust port keeps ONE struct and derives the three views from
//! it.
//!
//! WHAT `environment_tools` MEANS WITHOUT A CATALOGUE
//! --------------------------------------------------
//! TS computes `descriptor.tools` as the declared list plus every environment-backed name THIS host
//! can actually run, i.e. filtered by the runtime tool catalogue (`descriptor.ts:69-95`). The
//! definition registry has no catalogue dependency - the tool runtime is a later stage - so this
//! type exposes both lists and one accessor, [`HarnessDescriptor::registered_tools`], that reports
//! every declared environment tool. The stage that owns a catalogue filters that list before
//! building a card; see `knowledge.md`.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::events::EventHub;
use crate::persistence::HarnessPresetRecord;
use crate::registry::presets;

/// One harness definition: identity, membership, tool declaration, per-agent overrides, and
/// provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessDescriptor {
    /// The registry key, and by convention the entry agent's name.
    pub name: String,
    /// The card label.
    pub title: String,
    /// One paragraph: what this harness is for.
    pub description: String,
    /// `true` for every live harness: it cannot run without a provider endpoint
    /// (`types.ts`'s `requiresEndpoint`, `presets.ts:156`).
    pub requires_endpoint: bool,
    /// The selected entry agent.
    pub primary: String,
    /// The exact allowlist the primary may delegate to.
    pub subagents: Vec<String>,
    /// Derived membership: `[primary, ...subagents]`.
    pub agents: Vec<String>,
    /// The declared toolbox (`SHARED_TOOL_NAMES` for every live harness).
    pub tools: Vec<String>,
    /// The environment-backed declarations this card advertises (`["search", "fetch"]`).
    pub environment_tools: Vec<String>,
    /// Per-agent MODEL overrides: agent id -> model id. An agent it omits uses the session's model
    /// (`protocol.ts:279-283`).
    pub agent_models: BTreeMap<String, String>,
    /// Per-agent INSTRUCTION overrides: agent id -> instruction text. The override replaces only
    /// the identity sentence - a member's trusted guidelines still apply - and it never rewrites
    /// the shipped profile (`protocol.ts:284-291`).
    pub agent_instructions: BTreeMap<String, String>,
    /// Per-agent display COLOR overrides: agent id -> CSS hex color (`protocol.ts:292-301`,
    /// `registry.ts:604-629`). Display-only, but still EXECUTABLE state: the map reaches the frozen
    /// descriptor, the definition read and the swatches a saved session paints
    /// (`api-reads.ts:1318-1329`).
    pub agent_colors: BTreeMap<String, String>,
    /// Shipped with the application vs created through `POST /harnesses`.
    pub builtin: bool,
    /// Compatibility wire field, always `true` (`protocol.ts:296-300`): the registry never gates a
    /// PATCH on it.
    pub editable: bool,
    /// The factory-kind hint a restart needs (`types.ts` `template`): `'live'` for everything this
    /// port ships, `'scripted'`/`'flaky'` only in a document written by a TS build that shipped the
    /// fixtures. Carried as data, because dropping it would silently promote a fixture to a live
    /// harness (`persistence.ts:107-110`).
    pub template: Option<String>,
    /// The metadata a scripted template adopts when an executable patch upgrades it (`live`).
    pub live: Option<Value>,
}

impl HarnessDescriptor {
    /// The exact names this card registers: the declarative list plus every environment-backed
    /// name, de-duplicated (`descriptor.ts:79-84`, which appends an environment name only when the
    /// runtime catalogue carries it AND it is not already declared).
    #[must_use]
    pub fn registered_tools(&self) -> Vec<String> {
        let mut names = self.tools.clone();

        for name in &self.environment_tools {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }

        names
    }

    /// The core tools this card's runs declare up front, in DECLARED order (`descriptor.ts:95`
    /// calling `coreToolNames`, `toolbox.ts:216-238`).
    #[must_use]
    pub fn core_tools(&self) -> Vec<String> {
        presets::core_tool_names(&self.subagents, &self.registered_tools())
    }

    /// The final member set: one primary then its allowlist (`registry.ts:1044-1047`). The two
    /// override maps are validated against exactly this, so a key can never name an agent the
    /// harness does not contain.
    #[must_use]
    pub fn members(&self) -> Vec<String> {
        self.agents.clone()
    }

    /// Projects the definition onto durable metadata (`presetToRecord`, `registry.ts:616-639`).
    ///
    /// Both override maps are emitted ONLY when they carry something, and `environmentTools` only
    /// when the card declares any, so an untouched shipped row keeps the exact key set TS writes.
    #[must_use]
    pub fn to_record(&self) -> HarnessPresetRecord {
        HarnessPresetRecord {
            name: self.name.clone(),
            title: self.title.clone(),
            // A harness DEFINITION never writes `customAgents` (`persistence.ts:148-153`): the
            // registry's create/PATCH surface has no custom-agent input, so the map appears only
            // inside a saved session's own `preset` copy. Stating it absent here is what keeps an
            // untouched shipped row's key set exactly what TS writes.
            custom_agents: None,
            description: self.description.clone(),
            requires_endpoint: self.requires_endpoint,
            primary: self.primary.clone(),
            subagents: self.subagents.clone(),
            agents: self.agents.clone(),
            tools: self.tools.clone(),
            environment_tools: if self.environment_tools.is_empty() {
                None
            } else {
                Some(self.environment_tools.clone())
            },
            agent_models: if self.agent_models.is_empty() {
                None
            } else {
                Some(self.agent_models.clone())
            },
            agent_instructions: if self.agent_instructions.is_empty() {
                None
            } else {
                Some(self.agent_instructions.clone())
            },
            // The third map follows the identical non-empty rule (`descriptor.ts:117-119`).
            agent_colors: if self.agent_colors.is_empty() {
                None
            } else {
                Some(self.agent_colors.clone())
            },
            builtin: self.builtin,
            editable: self.editable,
            template: self.template.clone(),
            live: self.live.clone(),
            extra: serde_json::Map::new(),
        }
    }

    /// Rebuilds a definition from durable metadata (`presetFromRecord`'s metadata half,
    /// `registry.ts:753-779`).
    ///
    /// Metadata comes from the RECORD - the session's or the operator's frozen view - while the
    /// factory the record implies is this port's live definition. `editable` is normalized to
    /// `true`, exactly as TS does there, because R20.1 removed the read-only category.
    #[must_use]
    pub fn from_record(record: &HarnessPresetRecord) -> Self {
        Self {
            name: record.name.clone(),
            title: record.title.clone(),
            description: record.description.clone(),
            requires_endpoint: record.requires_endpoint,
            primary: record.primary.clone(),
            subagents: record.subagents.clone(),
            agents: record.agents.clone(),
            tools: record.tools.clone(),
            environment_tools: record.environment_tools.clone().unwrap_or_default(),
            agent_models: record.agent_models.clone().unwrap_or_default(),
            agent_instructions: record.agent_instructions.clone().unwrap_or_default(),
            // An absent map is "no configured color", which is what TS's `presetFromRecord`
            // normalizes it to (`registry.ts:793-795`).
            agent_colors: record.agent_colors.clone().unwrap_or_default(),
            builtin: record.builtin,
            editable: true,
            template: record.template.clone(),
            live: record.live.clone(),
        }
    }

    /// The picker's card for this definition (`harnessCardOf`, `api-reads.ts:1026-1037`), stamped
    /// with this definition's revision counter.
    #[must_use]
    pub fn card(&self, hub: &EventHub) -> crate::contract::HarnessCard {
        crate::read::harness_card_of(self, hub)
    }

    /// The identified definition read: a card plus the override map and the instruction summary
    /// (`harnessDefinitionOf`, `api-reads.ts:1043-1056`).
    #[must_use]
    pub fn definition(
        &self,
        hub: &EventHub,
        agents: &crate::agents::AgentCatalogue,
    ) -> crate::contract::HarnessDefinition {
        crate::read::harness_definition_of(self, hub, agents)
    }
}
