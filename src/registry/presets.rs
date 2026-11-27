//! The shipped harness catalogue: eight live presets, the alias table, the rank rules, and the
//! name/text/prefix bounds.
//!
//! TypeScript origin: `packages/underload/harness/src/server/harnesses/presets.ts` (the eight cards,
//! `SPECIALIST_AGENT_NAMES`, `ENTRY_RANK`/`TEAM_RANK`, `LEGACY_PRESET_DEFINITIONS`), `registry.ts`
//! (`PRESET_ALIASES`, `NAME_PATTERN`, `MAX_ROSTER_AGENTS`, the text bounds, `MAX_PREFIX_TURNS` /
//! `MAX_PREFIX_TURN_CHARS`), and `toolbox.ts` (`SHARED_TOOL_NAMES`, `CORE_TOOL_NAMES`,
//! `CORE_PRESET_TOOL_NAMES`, `CORE_ENVIRONMENT_TOOL_NAMES`, `coreToolNames`,
//! `ranksDeclareDelegation`, `resolveRosterRanks`).
//!
//! THE DEFINITION STATES ROUTING AND AUTHORITY SEPARATELY (`presets.ts:59-84`)
//! ---------------------------------------------------------------------------
//! Every card states `entry` (which member an unfocused submit routes to) and `ranks` (what each
//! roster member may reach) SIDE BY SIDE. `teamOf(primary)` is deleted upstream, and with it this
//! file's old derived allowlist: a rank map says both who runs at all and who may hand work to whom,
//! so `delegate` is registered exactly when the map holds two or more DISTINCT letters
//! ([`ranks_declare_delegation`]) - no derivation, no guess.
//!
//! WHAT THIS PORT DOES NOT CARRY
//! -----------------------------
//! A TS preset is metadata PLUS a `create` factory. The Rust port models the DEFINITION only - the
//! live harness is built by the runtime stage through `ports::TurnExecutor` - so a preset here is a
//! `HarnessDescriptor` and the factory-kind hints (`template`, `live`) are carried as data for the
//! restart policy. `demo` (scripted) and `flaky` (the `testFlaky` example) are therefore absent:
//! upstream deleted them (`presets.ts:20-35`), and a card a user can pick has to be a configuration
//! that actually runs a model on the shared toolbox.
//!
//! TWO CARDS NAME AGENTS THIS PORT DOES NOT SHIP
//! ---------------------------------------------
//! `general` and `debugger` are trusted profiles in the TypeScript catalogue
//! (`src/agents/index.ts`) and this crate ships six profiles (`src/agents/profiles.rs`), so three of
//! the eight cards (`general`, `chat`, and every delegating roster) name ids outside this build's
//! agent catalogue. Nothing breaks - a shipped preset is DATA, and only a `POST /harnesses` roster is
//! validated against the catalogue - but an instruction-summary row for such a member reports the
//! `catalogue` source with 0 chars, and `debugger`/`general` cannot be SELECTED by a created harness
//! until the agent catalogue catches up. That gap is reported, not papered over here.

use std::collections::BTreeMap;

use crate::contract::{AgentRank, HarnessPrefixTurn, PrefixTurnRole};

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
    "files",
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

/// The real specialist roles, in catalogue/descriptor order, DEFAULT FIRST (`presets.ts:146-154`).
/// This is the list a roster is drawn FROM; each card then states its own roster and rank map, so
/// adding a role here does not silently join every built-in team.
pub const SPECIALIST_AGENT_NAMES: [&str; 7] = [
    "general",
    "instruct",
    "research",
    "workspace",
    "reviewer",
    "planner",
    "debugger",
];

/// `summarizer` stays in every built-in team so the `textSummarize` delegated path keeps working in
/// the real presets (`presets.ts:156-158`; it condenses locally when absent).
pub const SUMMARIZER_AGENT_NAME: &str = "summarizer";

/// The Rank letters the shipped cards use, named once so a rank map reads as a decision rather than
/// a letter (`presets.ts:168`): `ENTRY_RANK` is the card's own agent, `TEAM_RANK` everyone it may hand
/// work to. A card with a single member states only `ENTRY_RANK`, and that is what keeps `delegate`
/// off its tool list.
pub const ENTRY_RANK: AgentRank = AgentRank::A;
/// The team letter (`presets.ts:169`): every member the entry may delegate to.
pub const TEAM_RANK: AgentRank = AgentRank::B;

/// The environment-backed declarations every live preset carries (`presets.ts`'s
/// `environmentTools: ['search', 'fetch']`, `toolbox.ts:208` `CORE_ENVIRONMENT_TOOL_NAMES`): search
/// is core only when the host has a key, fetch registers either way because the native provider needs
/// none. `chat` declares NEITHER - `fetch` registers without a credential, so a card that declared
/// the flag would advertise a declaration its harness no longer makes.
pub const ENVIRONMENT_TOOL_NAMES: [&str; 2] = ["search", "fetch"];

/// The harness-owned core names that are declared on every request
/// (`toolbox.ts:188-190` `CORE_TOOL_NAMES`, pinned by `frontend/contract.test.ts:126` and by the
/// configured-card assertion in `presets.test.ts:479-492`).
pub const CORE_TOOL_NAMES: [&str; 2] = ["toolRequest", "tasks"];

/// The package presets declared up front (`toolbox.ts:197-201` `CORE_PRESET_TOOL_NAMES`, i.e. the
/// PRESET_DESCRIPTORS with `visibility: 'core'`; visibilities pinned by `presets.test.ts:394-412`).
/// `files` and `download` are deliberately absent - they stay discoverable.
pub const CORE_PRESET_TOOL_NAMES: [&str; 5] = ["read", "write", "edit", "glob", "grep"];

/// `delegate` is declared only when the rank map states two or more DISTINCT ranks
/// (`toolbox.ts:318-320`), which is why it is projected separately instead of being part of the
/// static core list.
pub const DELEGATE_TOOL_NAME: &str = "delegate";

/// Legacy preset identifiers (`registry.ts` `PRESET_ALIASES`). The catalogue is agent-named now, but
/// a CLI invocation, script, or bookmarked API client written against the old names must still run.
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

/// `MAX_ROSTER_AGENTS` (`registry.ts:197`). A harness with a huge roster is almost certainly a
/// mistake, and every name is a trusted id. The old `MAX_SUBAGENTS` name is gone with the allowlist
/// it bounded.
pub const MAX_ROSTER_AGENTS: usize = 16;

/// Bounds on a client-supplied model id (`registry.ts:134-142`): generous for provider-prefixed
/// ids, tight enough that the map cannot smuggle a payload into a descriptor.
pub const MAX_MODEL_ID_LENGTH: usize = 200;

/// Bounds on a client-supplied INSTRUCTION override (`registry.ts:144-152`): 8000 characters is
/// generous for a role definition while keeping one saved harness from smuggling megabytes into
/// every request of every session.
pub const MAX_INSTRUCTION_LENGTH: usize = 8_000;

/// `MAX_PREFIX_TURNS` (`registry.ts:851`): the ceiling on ONE member's preset-history band, and the
/// same `maxItems: 64` the contract puts on every prefix array (swagger `:4881`, `:4972`, `:5049`).
pub const MAX_PREFIX_TURNS: usize = 64;

/// `MAX_PREFIX_TURN_CHARS` (`registry.ts:852`): the ceiling on one turn's `content`, matching the
/// schema's `maxLength: 32000` (swagger `:4825`).
pub const MAX_PREFIX_TURN_CHARS: usize = 32_000;

/// `PREFIX_TURN_ROLES` (`registry.ts:847`): the CLOSED role set of a preset-history turn. The check
/// is an allow-list rather than "not a tool role" because an unknown role string is a typo the client
/// should see.
pub const PREFIX_TURN_ROLES: [PrefixTurnRole; 3] = [
    PrefixTurnRole::System,
    PrefixTurnRole::User,
    PrefixTurnRole::Assistant,
];

/// The rank map one card ships: the entry agent at [`ENTRY_RANK`], every other member at
/// [`TEAM_RANK`] (`presets.ts` states exactly this per card, e.g. `instructRanks`).
///
/// This is the same statement `resolveRosterRanks(roster, entry, None)` produces for a request that
/// omits `ranks` (`toolbox.ts:265-267`) - the ordinary two-level team - which is why the eight shipped
/// cards can share one builder without hiding a decision: a single-member card gets ONE rank (no
/// delegation), and every delegating card gets exactly two distinct letters.
#[must_use]
pub fn team_ranks(entry: &str, roster: &[&str]) -> BTreeMap<String, AgentRank> {
    roster
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                if *name == entry {
                    ENTRY_RANK
                } else {
                    TEAM_RANK
                },
            )
        })
        .collect()
}

/// The roster one delegating card ships: the entry agent first, then every OTHER real role except the
/// Manager, then the summarizer (`presets.ts`'s seven explicit rosters, which differ from each other
/// by exactly this rule).
///
/// WHY `planner` IS LEFT OUT OF EVERY OTHER CARD: the Manager refuses to run as a delegate
/// (`presets.ts:160-166` quotes `agents/planner.ts`), and offering a member that answers "I do not run
/// as a delegate" is a wasted run. A user-created team may still select it. `summarizer` stays so the
/// `textSummarize` delegated path keeps working.
#[must_use]
fn delegating_roster(entry: &'static str) -> Vec<&'static str> {
    let mut roster: Vec<&'static str> = vec![entry];

    roster.extend(
        SPECIALIST_AGENT_NAMES
            .iter()
            .copied()
            .filter(|name| *name != entry && *name != "planner"),
    );

    roster.push(SUMMARIZER_AGENT_NAME);

    roster
}

/// Builds one live preset descriptor from its visible metadata and its explicit roster/rank pair
/// (`presets.ts:180-573`).
///
/// `agents` is the DECLARATION-ORDER roster (never derived from a rank map, and the rank map is never
/// derived from it); the tool list is the shared toolbox plus both web rows; and every entry is
/// `builtin`/`editable`/`template: 'live'` (`registry.ts`'s `copyTemplate` forces `editable: true`
/// because R20.1 removed the read-only category).
#[must_use]
fn live_preset(
    name: &str,
    title: &str,
    description: &str,
    entry: &str,
    roster: &[&str],
    tools: &[&str],
    environment_tools: &[&str],
) -> HarnessDescriptor {
    HarnessDescriptor {
        name: name.to_string(),
        title: title.to_string(),
        description: description.to_string(),
        requires_endpoint: true,
        entry: entry.to_string(),
        ranks: team_ranks(entry, roster),
        agents: roster.iter().map(|name| (*name).to_string()).collect(),
        tools: tools.iter().map(|tool| (*tool).to_string()).collect(),
        environment_tools: environment_tools
            .iter()
            .map(|tool| (*tool).to_string())
            .collect(),
        agent_models: BTreeMap::new(),
        agent_instructions: BTreeMap::new(),
        // A shipped template carries no colors: R2's map is an operator/client decision, and an
        // empty map is what makes the persisted row keep its pre-R2 shape (`presets.ts:180-573`).
        agent_colors: BTreeMap::new(),
        // No shipped template seeds preset history either: a band is an operator statement, and
        // shipping one would put prompt text in a card the UI has no affordance to edit
        // (`presets.ts` carries no `agentPrefixes` on any card).
        agent_prefixes: BTreeMap::new(),
        builtin: true,
        editable: true,
        template: Some("live".to_string()),
        live: None,
    }
}

/// THE PRODUCTION CATALOGUE, in UI/CLI order - `general` is the default/first card
/// (`presets.ts:529-538`).
///
/// ORDER IS A CONTRACT, NOT A PREFERENCE (`presets.ts:522-528`): it is the order `GET /harnesses`
/// returns, the order a picker renders, and the order the frontend palette assigns tones by. The
/// eight cards are `general, instruct, research, workspace, reviewer, planner, debugger, chat`, and
/// `chat` is LAST because it is not a team and never joins one.
#[must_use]
pub fn shipped_presets() -> Vec<HarnessDescriptor> {
    vec![
        // The default card: a one-member roster and a ONE-rank map, which is a load-bearing choice -
        // `delegate` registers only when two distinct ranks exist, so this harness never declares it.
        live_preset(
            "general",
            "General",
            "The default general-purpose agent, with the full shared toolbox and NO delegates: it does every step itself, so `delegate` is not registered and cannot be enabled. File read/write/edit/glob and a configured search are always available; call toolRequest for files, commands, diffs, syntax checks, time, text, or notes.",
            "general",
            &["general"],
            &SHARED_TOOL_NAMES,
            &ENVIRONMENT_TOOL_NAMES,
        ),
        live_preset(
            "instruct",
            "Instruct",
            "General worker with the full shared toolbox and the other roles as its Rank B delegates. It does its own building and editing, and delegates only the steps that would burn its context: research, exploration, a long read, a review. Unlike the Manager, it never hands out work it must still finish itself.",
            "instruct",
            &delegating_roster("instruct"),
            &SHARED_TOOL_NAMES,
            &ENVIRONMENT_TOOL_NAMES,
        ),
        // The visible title moved to "Explore"; the id stays `research` for every saved session.
        live_preset(
            "research",
            "Explore",
            "Read-only investigator with the same full toolbox as every live harness and the other roles as its Rank B delegates. Maps files, symbols, configuration and the web, cross-checks what decides the answer, and cites file, line, tool, or URL. Changes nothing on disk unless the brief authorises a change.",
            "research",
            &delegating_roster("research"),
            &SHARED_TOOL_NAMES,
            &ENVIRONMENT_TOOL_NAMES,
        ),
        // Visible title "Analyst"; the id is legacy for compatibility (`presets.ts:262-266`).
        live_preset(
            "workspace",
            "Analyst",
            "The implementor, with the full shared toolbox and the other roles as its Rank B delegates: file read/write/edit/glob are always available, and files, syntax checks, diffs, and commands are discovered by tag. Writes the change in the existing style of the codebase, keeps it minimal and efficient, and verifies it with a real check.",
            "workspace",
            &delegating_roster("workspace"),
            &SHARED_TOOL_NAMES,
            &ENVIRONMENT_TOOL_NAMES,
        ),
        live_preset(
            "reviewer",
            "Reviewer",
            "Strict, independent reviewer with the full shared toolbox and the other roles as its Rank B delegates. Judges plans, code, documents, and claims against their own acceptance criteria, reports each finding with file/line or quote evidence plus the smallest fix, and says plainly when there is nothing to find. Reads and verifies by default; edits only when asked.",
            "reviewer",
            &delegating_roster("reviewer"),
            &SHARED_TOOL_NAMES,
            &ENVIRONMENT_TOOL_NAMES,
        ),
        // Visible title "Manager": the orchestrator, and the only card whose roster holds every other
        // role, because it is the card that exists FOR delegation.
        live_preset(
            "planner",
            "Manager",
            "Planning and orchestration lead with the full shared toolbox, Rank A itself and every other role as its Rank B delegates. Holds the whole goal, writes an explicit plan, delegates every step (research, workspace, reviewer, debugger, general, instruct), and accepts each result only after an independent review. It never executes a step itself, and refuses to run as a delegate.",
            "planner",
            // No special case is needed for the Manager's roster: `delegating_roster` already
            // excludes only the subject, so `planner` lands at the tail like every other card
            // (`presets.ts:346`, roster `planner, general, instruct, research, workspace, reviewer,
            // debugger, summarizer`).
            &delegating_roster("planner"),
            &SHARED_TOOL_NAMES,
            &ENVIRONMENT_TOOL_NAMES,
        ),
        live_preset(
            "debugger",
            "Debugger",
            "Stress-test harness with the full shared toolbox, Rank A itself and every other role as its Rank B delegates. Nothing is off-limits: it enables any searchable tool, runs any command, exercises each capability at its edges, reproduces what breaks, and reports the exact input and error.",
            "debugger",
            &delegating_roster("debugger"),
            &SHARED_TOOL_NAMES,
            &ENVIRONMENT_TOOL_NAMES,
        ),
        // THE TOOL-LESS CARD (`presets.ts:455-478`): the trusted `general` profile on an EMPTY
        // catalogue. `tools: []` with no `environmentTools` is the load-bearing line - a card that
        // declared the shared set would promise a toolbox the factory does not register. It is last
        // in the catalogue because it is not a team and never joins one.
        live_preset(
            "chat",
            "Chat",
            "A single answer with no toolbox: the General profile on an empty catalogue (no file, search, delegation, or record tool is registered), so it cannot read or write the server host \u{2014} the only tools it can ever call are the harness-owned core rows. It carries no iteration budget, so it keeps going until the model answers.",
            "general",
            &["general"],
            &[],
            &[],
        ),
    ]
}

/// The canonical target of a legacy alias, if `name` is one (`registry.ts`, which reads
/// `PRESET_ALIASES[name]` and then resolves the target).
#[must_use]
pub fn alias_target(name: &str) -> Option<&'static str> {
    PRESET_ALIASES
        .iter()
        .find(|(alias, _)| *alias == name)
        .map(|(_, target)| *target)
}

/// `true` when `name` is a reserved alias, which a created harness may not take
/// (`registry.ts`, `hasOwnProperty(PRESET_ALIASES, name)`).
#[must_use]
pub fn is_reserved_alias(name: &str) -> bool {
    alias_target(name).is_some()
}

/// A harness name is a lowercase-or-uppercase letter, then letters, digits, `_` or `-`, at most 64
/// (`registry.ts` `NAME_PATTERN = /^[A-Za-z][A-Za-z0-9_-]{0,63}$/`).
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

/// The exact refusal `registry.ts` prints for a bad name.
#[must_use]
pub const fn harness_name_refusal() -> &'static str {
    "`name` must start with a letter and contain only letters, digits, \"_\" or \"-\", at most 64 characters."
}

/// `MODEL_ID_PATTERN` (`registry.ts`): a model id is an identifier for the session's own provider.
/// Control characters are rejected because a provider id with a newline is a header-injection smell,
/// not a model.
#[must_use]
pub fn is_model_id(text: &str) -> bool {
    !text.chars().any(char::is_control)
}

/// `INSTRUCTION_TEXT_PATTERN` (`registry.ts`): instruction text keeps its prose shape - tab,
/// newline and CR are allowed (a role definition is often multi-line), every other control
/// character is rejected.
#[must_use]
pub fn is_instruction_text(text: &str) -> bool {
    !text.chars().any(|glyph| {
        (glyph.is_ascii_control() && glyph != '\t' && glyph != '\n' && glyph != '\r')
            || glyph == '\u{7f}'
    })
}

/// `AGENT_COLOR_PATTERN` (`registry.ts`): an accepted per-agent identity color is a CSS hex color,
/// 3/4/6/8 digits, either case. The SAME expression the library validates at agent construction
/// (`core/agent.ts`) and the HTTP contract publishes (`agent_colors.pattern`), so a color the edge
/// accepts is always one a UI can paint.
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

/// `ranksDeclareDelegation` (`toolbox.ts:296-297`): whether one RANK MAP declares a delegation
/// surface, i.e. whether `delegate` is registered and therefore whether a descriptor may advertise
/// it. Two or more DISTINCT letters, counted on the FILLED map.
///
/// Counting distinct VALUES (not members) is what makes this exact: seven members all at `B` still
/// cannot delegate, because delegation is strictly downward and `B` to `B` is refused.
#[must_use]
pub fn ranks_declare_delegation(ranks: &BTreeMap<String, AgentRank>) -> bool {
    let mut letters: Vec<AgentRank> = ranks.values().copied().collect();

    letters.sort_unstable();
    letters.dedup();

    letters.len() >= 2
}

/// `resolveRosterRanks` (`toolbox.ts:257-283`): the RESOLVED authority map a harness runs with.
///
///   - `ranks == None` is the kernel's rule-1 default (`harness.ts:1540`): the ENTRY agent at `A` and
///     every other member one below at `B` - the ordinary two-level team, and the direct successor of
///     the old `primary` + `subagents` shape. The entry is ranked highest here as a DEFAULT, not a
///     rule; pass any map, even an empty one, and the entry is ranked like everybody else.
///   - a stated map is kept as stated, and every member it leaves out takes one rank BELOW the LOWEST
///     rank it does state, clamped at `Z`. An empty map takes the same branch with a floor index of 0,
///     which is what lands every member on `B` - the flat team the contract documents.
///
/// Only the roster's own keys count: a key outside the roster is the CALLER's error, refused by the
/// registry before this runs, and ignoring it here keeps a stale map from lowering the floor for a
/// member that no longer exists.
#[must_use]
pub fn resolve_roster_ranks(
    roster: &[String],
    entry: &str,
    ranks: Option<&BTreeMap<String, AgentRank>>,
) -> BTreeMap<String, AgentRank> {
    let Some(ranks) = ranks else {
        return roster
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    if name == entry { ENTRY_RANK } else { TEAM_RANK },
                )
            })
            .collect();
    };

    // The lowest AUTHORITY the map does state, i.e. the HIGHEST index among its roster keys.
    let lowest = roster
        .iter()
        .filter_map(|name| ranks.get(name))
        .map(|rank| crate::contract::rank_index_of(*rank))
        .max()
        .unwrap_or(0);

    let floor = crate::contract::rank_below(
        // `AGENT_RANKS[Math.min(lowest + 1, 25)]`, spelled through the rank helper so the clamp and
        // the scale cannot drift.
        crate::contract::AGENT_RANKS[lowest.min(crate::contract::AGENT_RANKS.len() - 1)],
    );

    roster
        .iter()
        .map(|name| (name.clone(), ranks.get(name).copied().unwrap_or(floor)))
        .collect()
}

/// Projects the core tool set for one card (`toolbox.ts:308-330` `coreToolNames`).
///
/// `ranks` is the card's RESOLVED rank map; `delegate` joins the projection exactly when
/// [`ranks_declare_delegation`] says the map has someone strictly below the delegator to hand work
/// to. `registered` is the exact tool-name set the card was built with (preset tools plus the
/// environment tools THIS host can run); a core name is advertised only when it is in that set, so a
/// timeNow-only fixture does not claim file tools and a keyless host does not claim search. The
/// DECLARED order is harness-owned first, then `delegate`, then the core presets, then the web pair.
#[must_use]
pub fn core_tool_names(ranks: &BTreeMap<String, AgentRank>, registered: &[String]) -> Vec<String> {
    let is_registered = |name: &str| registered.iter().any(|entry| entry == name);

    let mut names: Vec<String> = CORE_TOOL_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect();

    if ranks_declare_delegation(ranks) {
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

/// One previously shipped EXECUTABLE definition (`presets.ts` `LegacyPresetDefinition`), restated in
/// the CURRENT vocabulary (`entry` + `ranks`).
///
/// WHY A TABLE OF OLD DEFAULTS
/// ---------------------------
/// A persisted registry row is a copy of what shipped at the time. Replaying it verbatim would pin
/// an old build's defaults forever, so an improved roster would never reach an existing
/// installation. These entries are the exact fields a past change touched, which makes "the
/// operator never customised this row" provable: a saved record whose executable fields still match
/// the table is a SHIPPED default, not a decision, so the current template wins. A record that
/// differs in any executable field is a real customisation and is replayed unchanged.
///
/// `ranks` here is the authority map of that shipped shape: the entry agent ranked above whoever the
/// card could hand work to. An empty roster had ONE rank - the same statement the old
/// `subagents: []` made, made directly (`presets.ts`, the note above the table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyPresetDefinition {
    pub title: String,
    pub description: String,
    pub requires_endpoint: bool,
    /// The routing agent of that shipped shape (what `primary` used to name).
    pub entry: String,
    /// The authority map of that shipped shape.
    pub ranks: BTreeMap<String, AgentRank>,
    pub agents: Vec<String>,
    pub tools: Vec<String>,
    pub environment_tools: Vec<String>,
    pub template: String,
}

/// `LEGACY_PRESET_DEFINITIONS` (`presets.ts:560-590`): only the presets whose shipped defaults
/// actually moved need an entry, and each holds ONE snapshot per preset id - the pre-T3 shape.
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
                description: "A general instruction-following agent with the full shared toolbox. File read/write/edit/glob and a configured search are always available; call toolRequest with capability tags for files, commands, diffs, syntax checks, time, text, or notes. The live presets differ only in their agents, not their tools.".to_string(),
                requires_endpoint: true,
                entry: "instruct".to_string(),
                ranks: BTreeMap::from([("instruct".to_string(), AgentRank::A)]),
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
                description: "A research agent with the same full toolbox as every live harness, plus a summarizer delegate. read and glob are always available; `entry` and `ranks` are explicit: the research agent is the entry point and holds Rank A, the summarizer holds Rank B, and delegation runs strictly downward, so only a higher-ranked agent may hand work to a lower-ranked one. Web search is core and joins the toolbox when the host configured a search key.".to_string(),
                requires_endpoint: true,
                entry: "research".to_string(),
                ranks: BTreeMap::from([
                    ("research".to_string(), AgentRank::A),
                    (SUMMARIZER_AGENT_NAME.to_string(), AgentRank::B),
                ]),
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
                description: "A coding agent with the full shared toolbox: file read/write/edit/glob are always available, and files, syntax checks, diffs, and commands are discovered by tag. Web search is available when a key is configured. It has the same registered tools as every live harness and differs only in its agent profile and routing. Everything runs on the server host.".to_string(),
                requires_endpoint: true,
                entry: "workspace".to_string(),
                ranks: BTreeMap::from([("workspace".to_string(), AgentRank::A)]),
                agents: vec!["workspace".to_string()],
                tools: shared,
                environment_tools: web,
                template: "live".to_string(),
            },
        ),
    ];

    entries.into_iter().collect()
}

/// One preset-history turn written back out of the domain shape, verbatim (`descriptor.ts:133-141`
/// copies turn-by-turn so a projection never aliases the stored band).
#[must_use]
pub fn copy_prefix_map(
    map: &BTreeMap<String, Vec<HarnessPrefixTurn>>,
) -> BTreeMap<String, Vec<HarnessPrefixTurn>> {
    map.iter()
        .map(|(agent, turns)| {
            (
                agent.clone(),
                turns
                    .iter()
                    .map(|turn| HarnessPrefixTurn {
                        role: turn.role,
                        content: turn.content.clone(),
                    })
                    .collect(),
            )
        })
        .collect()
}
