//! The shipped harness catalogue: five live presets, the alias table, and the name/text rules.
//!
//! TypeScript origin: `packages/underload/harness/src/server/harnesses/presets.ts` (the presets,
//! `teamOf`, `SPECIALIST_AGENT_NAMES`, `LEGACY_PRESET_DEFINITIONS`), `registry.ts`
//! (`PRESET_ALIASES`, `NAME_PATTERN`, the text bounds), and `toolbox.ts` (`SHARED_TOOL_NAMES`,
//! `CORE_TOOL_NAMES`, `CORE_PRESET_TOOL_NAMES`, `CORE_ENVIRONMENT_TOOL_NAMES`, `coreToolNames`).
//!
//! WHAT THIS PORT DOES NOT CARRY
//! -----------------------------
//! A TS preset is metadata PLUS a `create` factory. The Rust port models the DEFINITION only - the
//! live harness is built by the runtime stage through `ports::TurnExecutor` - so a preset here is a
//! `HarnessDescriptor` and the factory-kind hints (`template`, `live`) are carried as data for the
//! restart policy. `demo` (scripted) and `flaky` (the `testFlaky` example) are therefore absent:
//! they are fixtures, and TS itself keeps them out of the shipped catalogue (`presets.ts:7-28`,
//! `FIXTURE_PRESETS` at `:405`).
//!
//! TEAMS ARE SYMMETRIC ON PURPOSE (`presets.ts:37-44`)
//! --------------------------------------------------
//! Every real preset offers every OTHER real role plus `summarizer` as a direct subagent, so
//! switching the entry role never removes a specialist from the team and the Planner's
//! `coder`/`researcher`/`reviewer` targets exist by id. `team_of` derives the allowlist from ONE
//! list, so the five presets cannot drift.

use std::collections::BTreeMap;

use super::descriptor::HarnessDescriptor;

/// The exact shared tool registry, in registry order: the package tools first, then the app tools
/// (`toolbox.ts:123-133` `SHARED_TOOL_NAMES = Object.keys(SHARED_TOOLS)`, pinned verbatim by
/// `presets.test.ts:86-102` and re-asserted entry by entry in `toolbox-sources.test.ts:284-311`).
///
/// Hardcoded here on purpose: the Rust tool catalogue is a later stage, and a preset's advertised
/// toolbox must not depend on what that stage happens to register. `toolbox-sources.test.ts` pins
/// the same sixteen-name list from the file system on the TS side.
pub const SHARED_TOOL_NAMES: [&str; 15] = [
    "syntax",
    "read",
    "write",
    "edit",
    "fileList",
    "glob",
    "grep",
    "shell",
    "gitDiff",
    "download",
    "timeNow",
    "textCount",
    "diceRoll",
    "noteSearch",
    "textSummarize",
];

/// The real specialist roles, in catalogue/descriptor order (`presets.ts:119-125`). This is the ONE
/// list the teams are derived from.
pub const SPECIALIST_AGENT_NAMES: [&str; 5] =
    ["instruct", "research", "workspace", "reviewer", "planner"];

/// `summarizer` stays a subagent everywhere so the `textSummarize` delegated path keeps working in
/// the real presets (`presets.ts:127-129`).
pub const SUMMARIZER_AGENT_NAME: &str = "summarizer";

/// The environment-backed declarations every live preset carries (`presets.ts:161-167`,
/// `toolbox.ts:208` `CORE_ENVIRONMENT_TOOL_NAMES`): search is core only when the host has a key,
/// fetch registers either way because the native provider needs none.
pub const ENVIRONMENT_TOOL_NAMES: [&str; 2] = ["search", "fetch"];

/// The harness-owned core names that are declared on every request
/// (`toolbox.ts:188-190` `CORE_TOOL_NAMES`, pinned by `frontend/contract.test.ts:126` and by the
/// configured-card assertion in `presets.test.ts:479-492`).
pub const CORE_TOOL_NAMES: [&str; 2] = ["toolRequest", "task"];

/// The package presets declared up front (`toolbox.ts:197-201` `CORE_PRESET_TOOL_NAMES`, i.e. the
/// PRESET_DESCRIPTORS with `visibility: 'core'`; visibilities pinned by `presets.test.ts:394-412`).
/// `fileList` and `download` are deliberately absent - they stay discoverable.
pub const CORE_PRESET_TOOL_NAMES: [&str; 5] = ["read", "write", "edit", "glob", "grep"];

/// `delegate` is declared to a primary only when the harness has subagents
/// (`toolbox.ts:228-233`), which is why it is projected separately instead of being part of the
/// static core list.
pub const DELEGATE_TOOL_NAME: &str = "delegate";

/// Legacy preset identifiers (`registry.ts:113-117`). The catalogue is agent-named now, but a CLI
/// invocation, script, or bookmarked API client written against the old names must still run.
/// Aliases resolve to the canonical harness and are never listed a second time; they are also
/// reserved, so a user-created harness cannot shadow one.
pub const PRESET_ALIASES: [(&str, &str); 3] = [
    ("writer", "instruct"),
    ("assistant", "instruct"),
    ("researcher", "research"),
];

/// A harness id appears in a URL path and in a session record. Letters first (so it is never
/// confused with a number), then letters/digits/`_`/`-`, at most 64 (`registry.ts:119-121`).
pub const MAX_HARNESS_NAME_LENGTH: usize = 64;

/// Bounds on human text (`registry.ts:125-132`): generous for a description, tight enough that a
/// descriptor cannot bloat a bootstrap document.
pub const MAX_TITLE_LENGTH: usize = 120;
pub const MAX_DESCRIPTION_LENGTH: usize = 2_000;

/// A harness with a huge allowlist is almost certainly a mistake, and every name is a trusted id
/// (`registry.ts:129-132`).
pub const MAX_SUBAGENTS: usize = 16;

/// Bounds on a client-supplied model id (`registry.ts:134-142`): generous for provider-prefixed
/// ids, tight enough that the map cannot smuggle a payload into a descriptor.
pub const MAX_MODEL_ID_LENGTH: usize = 200;

/// Bounds on a client-supplied INSTRUCTION override (`registry.ts:144-152`): 8000 characters is
/// generous for a role definition while keeping one saved harness from smuggling megabytes into
/// every request of every session.
pub const MAX_INSTRUCTION_LENGTH: usize = 8_000;

/// The subagent allowlist for one primary: every OTHER real role, then the summarizer
/// (`presets.ts:134-137` `teamOf`). The primary is excluded because a harness has exactly one
/// primary and the registry rejects a subagent that names it.
#[must_use]
pub fn team_of(primary: &str) -> Vec<String> {
    SPECIALIST_AGENT_NAMES
        .iter()
        .filter(|name| *name != &primary)
        .map(|name| (*name).to_string())
        .chain(std::iter::once(SUMMARIZER_AGENT_NAME.to_string()))
        .collect()
}

/// Builds one live preset descriptor from its visible metadata (`presets.ts:151-263`).
///
/// `agents` order is `[primary, ...subagents]`, the tool list is the shared toolbox, both web rows
/// are declared, and every entry is `builtin`/`editable`/`template: 'live'` (`registry.ts:549-562`
/// `copyTemplate` forces `editable: true` because R20.1 removed the read-only category).
#[must_use]
fn live_preset(name: &str, title: &str, description: &str) -> HarnessDescriptor {
    let subagents = team_of(name);
    let agents = std::iter::once(name.to_string())
        .chain(subagents.clone())
        .collect();

    HarnessDescriptor {
        name: name.to_string(),
        title: title.to_string(),
        description: description.to_string(),
        requires_endpoint: true,
        primary: name.to_string(),
        subagents,
        agents,
        tools: SHARED_TOOL_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        environment_tools: ENVIRONMENT_TOOL_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        agent_models: BTreeMap::new(),
        agent_instructions: BTreeMap::new(),
        // A shipped template carries no colors: R2's map is an operator/client decision, and an
        // empty map is what makes the persisted row keep its pre-R2 shape (`presets.ts:151-263`).
        agent_colors: BTreeMap::new(),
        builtin: true,
        editable: true,
        template: Some("live".to_string()),
        live: None,
    }
}

/// The PRODUCTION catalogue, in UI/CLI order - instruct is the default/first card
/// (`presets.ts:391-397`).
#[must_use]
pub fn shipped_presets() -> Vec<HarnessDescriptor> {
    vec![
        live_preset(
            "instruct",
            "Instruct",
            "General instruction-following agent with the full shared toolbox and every other role as a direct subagent. File read/write/edit/glob and a configured search are always available; call toolRequest for fileList, commands, diffs, syntax checks, time, text, or notes. Only the agents differ between live presets, never the tools.",
        ),
        live_preset(
            "research",
            "Researcher",
            "Researcher with the same full toolbox as every live harness and the other roles as direct subagents. read and glob are always available; search joins the toolbox when the host configured a key. Investigates, cross-checks sources, and cites what it used.",
        ),
        live_preset(
            "workspace",
            "Coder",
            "Coding agent with the full shared toolbox and the other roles as direct subagents: file read/write/edit/glob are always available, and fileList, syntax checks, diffs, and commands are discovered by tag. Web search is available when a key is configured. Everything runs on the server host.",
        ),
        live_preset(
            "reviewer",
            "Reviewer",
            "Skeptical review agent with the full shared toolbox and the other roles as direct subagents. Looks for small bugs and inconsistencies, reports each with file/line evidence, and says plainly when there is nothing to find. Reads and verifies by default; edits only when asked.",
        ),
        live_preset(
            "planner",
            "Planner",
            "Planning lead with the full shared toolbox and every other role as a direct subagent. Restates the goal and acceptance criteria, writes an explicit plan, hands each step to the specialist who owns it (researcher, coder, reviewer, instruct), and verifies the evidence before answering.",
        ),
    ]
}

/// The canonical target of a legacy alias, if `name` is one (`registry.ts:1008-1010`, which reads
/// `PRESET_ALIASES[name]` and then resolves the target).
#[must_use]
pub fn alias_target(name: &str) -> Option<&'static str> {
    PRESET_ALIASES
        .iter()
        .find(|(alias, _)| *alias == name)
        .map(|(_, target)| *target)
}

/// `true` when `name` is a reserved alias, which a created harness may not take
/// (`registry.ts:1027-1032`).
#[must_use]
pub fn is_reserved_alias(name: &str) -> bool {
    alias_target(name).is_some()
}

/// A harness name is a lowercase-or-uppercase letter, then letters, digits, `_` or `-`, at most 64
/// (`registry.ts:121` `NAME_PATTERN = /^[A-Za-z][A-Za-z0-9_-]{0,63}$/`).
///
/// The stage brief spells this `^[a-z][a-z0-9_-]*$`; the TS pattern is the one the refusal message
/// describes ("at most 64 characters"), so the TS rule is ported and the message stays true.
#[must_use]
pub fn is_valid_harness_name(name: &str) -> bool {
    let Some(first) = name.chars().next() else {
        return false;
    };

    name.len() <= MAX_HARNESS_NAME_LENGTH
        && first.is_ascii_alphabetic()
        && name
            .chars()
            .all(|glyph| glyph.is_ascii_alphanumeric() || glyph == '_' || glyph == '-')
}

/// The exact refusal `registry.ts:1020-1025` prints for a bad name.
#[must_use]
pub const fn harness_name_refusal() -> &'static str {
    "`name` must start with a letter and contain only letters, digits, \"_\" or \"-\", at most 64 characters."
}

/// `MODEL_ID_PATTERN` (`registry.ts:142`): a model id is an identifier for the session's own
/// provider. Control characters are rejected because a provider id with a newline is a
/// header-injection smell, not a model.
#[must_use]
pub fn is_model_id(text: &str) -> bool {
    !text.chars().any(char::is_control)
}

/// `INSTRUCTION_TEXT_PATTERN` (`registry.ts:152`): instruction text keeps its prose shape - tab,
/// newline and CR are allowed (a role definition is often multi-line), every other control
/// character is rejected.
#[must_use]
pub fn is_instruction_text(text: &str) -> bool {
    !text.chars().any(|glyph| {
        (glyph.is_ascii_control() && glyph != '\t' && glyph != '\n' && glyph != '\r')
            || glyph == '\u{7f}'
    })
}

/// `AGENT_COLOR_PATTERN` (`registry.ts:155-164`): an accepted per-agent identity color is a CSS hex
/// color, 3/4/6/8 digits, either case. The SAME expression the library validates at agent
/// construction (`core/agent.ts`) and the HTTP contract publishes (`harness.swagger.yaml`
/// `agent_colors.pattern`), so a color the edge accepts is always one a UI can paint. Written as a
/// predicate rather than a regex for the same reason as the two above: the rule is a character
/// class plus a length set, and the duplication is only ever about the ERROR, not the rule.
#[must_use]
pub fn is_agent_color(text: &str) -> bool {
    let Some(digits) = text.strip_prefix('#') else {
        return false;
    };

    matches!(digits.len(), 3 | 4 | 6 | 8)
        && digits
            .bytes()
            .all(|glyph| glyph.is_ascii_digit() || matches!(glyph, b'a'..=b'f' | b'A'..=b'F'))
}

/// Projects the core tool set for one card (`toolbox.ts:216-238` `coreToolNames`).
///
/// `registered` is the exact tool-name set the card was built with (preset tools plus the
/// environment tools THIS host can run). A core name is advertised only when it is in that set, so
/// a timeNow-only fixture does not claim file tools and a keyless host does not claim search. The
/// DECLARED order is harness-owned first, then `delegate`, then the core presets, then the web
/// pair.
#[must_use]
pub fn core_tool_names(subagents: &[String], registered: &[String]) -> Vec<String> {
    let is_registered = |name: &str| registered.iter().any(|entry| entry == name);

    let mut names: Vec<String> = CORE_TOOL_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect();

    if !subagents.is_empty() {
        names.push(DELEGATE_TOOL_NAME.to_string());
    }

    names.extend(
        CORE_PRESET_TOOL_NAMES
            .iter()
            .filter(|name| is_registered(name))
            .map(|name| (*name).to_string()),
    );

    names.extend(
        ENVIRONMENT_TOOL_NAMES
            .iter()
            .filter(|name| is_registered(name))
            .map(|name| (*name).to_string()),
    );

    names
}

/// One previously shipped EXECUTABLE definition (`presets.ts:433-443`).
///
/// WHY A TABLE OF OLD DEFAULTS
/// ---------------------------
/// A persisted registry row is a copy of what shipped at the time. Replaying it verbatim would pin
/// an old build's defaults forever, so an improved roster would never reach an existing
/// installation. These entries are the exact fields a past change touched, which makes "the
/// operator never customised this row" provable: a saved record whose executable fields still match
/// the table is a SHIPPED default, not a decision, so the current template wins. A record that
/// differs in any executable field is a real customisation and is replayed unchanged
/// (`presets.ts:418-432`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyPresetDefinition {
    pub title: String,
    pub description: String,
    pub requires_endpoint: bool,
    pub primary: String,
    pub subagents: Vec<String>,
    pub agents: Vec<String>,
    pub tools: Vec<String>,
    pub environment_tools: Vec<String>,
    pub template: String,
}

/// `LEGACY_PRESET_DEFINITIONS` (`presets.ts:445-481`): only the presets whose shipped defaults
/// actually moved need an entry.
#[must_use]
pub fn legacy_preset_definitions() -> BTreeMap<&'static str, LegacyPresetDefinition> {
    let shared: Vec<String> = SHARED_TOOL_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    let web = vec!["search".to_string()];

    let entries = [
        (
            "instruct",
            LegacyPresetDefinition {
                title: "Instruct".to_string(),
                description: "A general instruction-following agent with the full shared toolbox. File read/write/edit/glob and a configured search are always available; call toolRequest with capability tags for fileList, commands, diffs, syntax checks, time, text, or notes. The live presets differ only in their agents, not their tools.".to_string(),
                requires_endpoint: true,
                primary: "instruct".to_string(),
                subagents: Vec::new(),
                agents: vec!["instruct".to_string()],
                tools: shared.clone(),
                environment_tools: web.clone(),
                template: "live".to_string(),
            },
        ),
        (
            "research",
            LegacyPresetDefinition {
                title: "Research".to_string(),
                description: "A research agent with the same full toolbox as every live harness, plus a summarizer subagent. read and glob are always available; `primary` and `subagents` are explicit: the research agent is the entry point and the only agent that may delegate, and delegation is one level deep. Web search is core and joins the toolbox when the host configured a search key.".to_string(),
                requires_endpoint: true,
                primary: "research".to_string(),
                subagents: vec![SUMMARIZER_AGENT_NAME.to_string()],
                agents: vec!["research".to_string(), SUMMARIZER_AGENT_NAME.to_string()],
                tools: shared.clone(),
                environment_tools: web.clone(),
                template: "live".to_string(),
            },
        ),
        (
            "workspace",
            LegacyPresetDefinition {
                title: "Workspace (coding)".to_string(),
                description: "A coding agent with the full shared toolbox: file read/write/edit/glob are always available, and fileList, syntax checks, diffs, and commands are discovered by tag. Web search is available when a key is configured. It has the same registered tools as every live harness and differs only in its agent profile and routing. Everything runs on the server host.".to_string(),
                requires_endpoint: true,
                primary: "workspace".to_string(),
                subagents: Vec::new(),
                agents: vec!["workspace".to_string()],
                tools: shared,
                environment_tools: web,
                template: "live".to_string(),
            },
        ),
    ];

    entries.into_iter().collect()
}
