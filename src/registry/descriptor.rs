//! One harness DEFINITION as the registry holds it, as a session freezes it, and as a card reports it.
//!
//! TypeScript origin: `src/server/harnesses/types.ts` (`HarnessPreset`, minus the `create` factory),
//! `src/server/harnesses/descriptor.ts` (`describeHarness`), and `src/server/protocol.ts:390-455`
//! (`HarnessDescriptor`, the frozen wire view).
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
//! ROUTING AND AUTHORITY ARE SEPARATE FIELDS (R1/R6)
//! -------------------------------------------------
//! [`HarnessDescriptor::entry`] says who an unfocused submit reaches and [`HarnessDescriptor::ranks`]
//! says what each member may reach. They replaced `primary` + `subagents`: a rank map states BOTH who
//! may run and who may be handed work down, so the allowlist and the rule "a subagent is never the
//! primary" left together, and a two-Rank-A roster became a legal shape instead of an error
//! (`protocol.ts:390-400`, `types.ts`'s `entry`/`ranks` notes).
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

use crate::contract::{AgentRank, HarnessPrefixTurn};
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
    /// THE ENTRY AGENT: routing, i.e. which member an unfocused submit reaches (`protocol.ts:399`,
    /// `types.ts` `HarnessPreset.entry`). It replaces `primary`, and it is stated SEPARATELY from
    /// [`Self::ranks`] because the two axes answer different questions - who an unfocused turn goes to
    /// vs what each member may reach. Several members may hold the entry rank, and nothing here picks
    /// one of them as "the" leader.
    pub entry: String,
    /// THE RANK MAP (authority): agent id -> rank letter, always the RESOLVED map - one entry per
    /// roster member, because the registry fills the gaps with the kernel's rule at WRITE time
    /// (`registry.ts` create/update + `toolbox.ts` `resolveRosterRanks`). `delegate` is registered
    /// exactly when this map holds two or more DISTINCT letters ([`presets::ranks_declare_delegation`]),
    /// which is what replaced the old `subagents` allowlist: a rank map says both who may run and who
    /// may hand work down.
    pub ranks: BTreeMap<String, AgentRank>,
    /// The roster in DECLARATION ORDER (`protocol.ts:401`). Order is load-bearing: it is the read
    /// order, and it is what `entry` defaults to when a request omits it.
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
    /// PER-AGENT PRESET HISTORY (U6): agent id -> that member's complete ordered band of seeded
    /// `system` / `user` / `assistant` turns (`protocol.ts:443-453`, `registry.ts` `agentPrefixes`).
    ///
    /// THE ASYMMETRY WITH THE THREE TEXT MAPS ABOVE, and the reason this map is never collapsed: an
    /// EMPTY band is a statement a client made ("this member seeds no history"), so `{ writer: [] }`
    /// keeps its key, while an entirely empty MAP is still equivalent to absence
    /// (`registry.ts:1016-1018`, `descriptor.ts:128-142`).
    pub agent_prefixes: BTreeMap<String, Vec<HarnessPrefixTurn>>,
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
    ///
    /// THE RANK MAP IS AN INPUT: `delegate` joins the projection exactly when the map states two or
    /// more DISTINCT ranks, which is why a single-rank card (`general`, `chat`) never advertises a
    /// delegation tool it would not declare (`toolbox.ts:318-320`).
    #[must_use]
    pub fn core_tools(&self) -> Vec<String> {
        presets::core_tool_names(&self.ranks, &self.registered_tools())
    }

    /// The final member set, in declaration order (`protocol.ts:401`, and `registry.ts`'s
    /// `agents = roster`). The four override maps are validated against exactly this, so a key can
    /// never name an agent the harness does not contain.
    #[must_use]
    pub fn members(&self) -> Vec<String> {
        self.agents.clone()
    }

    /// The members one agent may hand work to: every roster member STRICTLY BELOW its own rank, in
    /// roster order (`types/index.ts` `isLowerRank` - a same-rank peer is not a subordinate).
    ///
    /// WHY THIS EXISTS WHEN `ranks` IS THE MODEL: the runtime's tool policy (`ports::ToolPolicy`)
    /// still carries a name list, because the `delegate` implementation resolves a target by name and
    /// must refuse a target the rank map does not allow. Deriving it here keeps ONE place that reads
    /// the rank arithmetic, so a policy can never claim a delegation surface the rank map denies -
    /// a flat (single-letter) map yields an empty list, and `src/tools/mod.rs`'s `subagents.is_empty()`
    /// gate then hides the tool exactly as `ranks_declare_delegation` predicts.
    #[must_use]
    pub fn subagents(&self) -> Vec<String> {
        let Some(caller) = self.ranks.get(&self.entry).copied() else {
            // A map that does not rank its own entry agent has nothing to compare against, and every
            // member is then an equal: no delegation, which is the same answer a flat map gives.
            return Vec::new();
        };

        self.agents
            .iter()
            .filter(|name| {
                // The entry itself is never its own target, even when the map names it twice.
                **name != self.entry
                    && self
                        .ranks
                        .get(*name)
                        .is_some_and(|rank| crate::contract::is_lower_rank(*rank, caller))
            })
            .cloned()
            .collect()
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
            // Routing and authority are written as the two separate fields they are, and BOTH are
            // always present (`persistence.ts:286-294`: `entry` and `ranks` are required, and
            // `presetToRecord` writes `ranks` through `sortedRanks`, i.e. a key-sorted map - which is
            // exactly what a `BTreeMap` already is).
            entry: Some(self.entry.clone()),
            ranks: Some(self.ranks.clone()),
            // The legacy pair is DROPPED on every write: `from_record` folds a pre-rank row into the
            // two fields above, so re-emitting the old keys would leave a restored row stating the
            // same decision twice.
            primary: None,
            subagents: None,
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
            // U6's fourth map follows the same OUTER rule, with the one asymmetry the contract states:
            // the gate is on the MAP, so a member keyed with an empty band keeps its key and the
            // statement "this member seeds no history" survives the round trip
            // (`registry.ts:1243-1252`, `descriptor.ts:128-142`).
            agent_prefixes: if self.agent_prefixes.is_empty() {
                None
            } else {
                Some(self.agent_prefixes.clone())
            },
            builtin: self.builtin,
            editable: self.editable,
            template: self.template.clone(),
            live: self.live.clone(),
            extra: serde_json::Map::new(),
        }
    }

    /// Rebuilds a definition from durable metadata (`presetFromRecord`'s metadata half,
    /// `registry.ts:1505-1530`).
    ///
    /// Metadata comes from the RECORD - the session's or the operator's frozen view - while the
    /// factory the record implies is this port's live definition. `editable` is normalized to
    /// `true`, exactly as TS does there, because R20.1 removed the read-only category.
    #[must_use]
    pub fn from_record(record: &HarnessPresetRecord) -> Self {
        // THE ENTRY AGENT, with a pre-rank row's `primary` as its alias: TS REFUSES such a row
        // (`readPresetRecord` -> `requireNoLegacyRosterKeys`, `persistence.ts:281-286`), and this port
        // restores it, because a store that loads under TS's older build must still load here. The
        // next write emits the new spelling only (`to_record`). Falling back to `agents[0]` is the same
        // default `create` applies to a request that omits `entry` (`registry.ts:1921-1926`); `name` is
        // the last resort for a roster-less row, which this port can list but never run.
        let entry = record
            .entry
            .clone()
            .or_else(|| record.primary.clone())
            .or_else(|| record.agents.first().cloned())
            .unwrap_or_else(|| record.name.clone());

        // THE RANK MAP VERBATIM when the row states one (`presetFromRecord` hands `record.ranks`
        // straight to the factory, `registry.ts:1497`, so a hand-edited sparse map is NOT re-filled
        // here), and the kernel's rule-1 default when the row states none: entry at `A`, every other
        // member at `B`. That derived map is the exact statement the legacy `primary`/`subagents` pair
        // made, so a pre-rank row keeps the delegation surface it used to have.
        let ranks = record
            .ranks
            .clone()
            .unwrap_or_else(|| presets::resolve_roster_ranks(&record.agents, &entry, None));

        Self {
            name: record.name.clone(),
            title: record.title.clone(),
            description: record.description.clone(),
            requires_endpoint: record.requires_endpoint,
            entry,
            ranks,
            agents: record.agents.clone(),
            tools: record.tools.clone(),
            environment_tools: record.environment_tools.clone().unwrap_or_default(),
            agent_models: record.agent_models.clone().unwrap_or_default(),
            agent_instructions: record.agent_instructions.clone().unwrap_or_default(),
            // An absent map is "no configured color", which is what TS's `presetFromRecord`
            // normalizes it to (`registry.ts:793-795`).
            agent_colors: record.agent_colors.clone().unwrap_or_default(),
            // U6's map follows the same absent-means-empty rule (`registry.ts:1306-1310`).
            agent_prefixes: record.agent_prefixes.clone().unwrap_or_default(),
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
