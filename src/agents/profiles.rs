//! The trusted agent catalogue: one compiled-in profile per selectable agent id.
//!
//! TypeScript origin: `packages/underload/harness/src/agents/` - `types.ts` (the `AgentProfile`
//! record), `guidelines.ts` (the shared policy lines), and one file per profile.
//!
//! WHY THESE ARE DATA, NOT CONFIG
//! ------------------------------
//! An agent is an identity with an instruction and a set of guidelines, and that text is TRUSTED
//! catalogue data: a client may choose an agent, but must never supply the words a model is seeded
//! with (`types.ts:1-26`). Keeping the text compiled in - rather than in the database - is what
//! lets the REST contract expose a profile, validate a harness's `primary`/`subagents` against a
//! stable registry of known ids, and still improve a shipped instruction for every profile nobody
//! overrode.
//!
//! NOT SHIPPED HERE: `demo` and `flaky`
//! ------------------------------------
//! The TS roster is EIGHT entries (`agents/index.ts:57-66`) and this port ships all eight. The two
//! entries it used to be missing were `general` and `debugger`, and both are now ported: every
//! shipped preset card names them (`src/registry/presets.rs`), and a card whose roster member is
//! outside the agent catalogue cannot be PATCHED, which is why the two profiles are a contract
//! requirement rather than a nicety.
//!
//! The catalogue used to end with two FIXTURES — `demo` (the scripted offline transport) and
//! `flaky` (the `testFlaky` retry example). Upstream DELETED both (`agents/index.ts:16-22`): a
//! registry entry is a role a user can pick and a harness can select, and neither of those was a
//! usable configuration. They are absent here for the same reason, not because the port dropped
//! them, so the "Known agents: ..." refusal lists the same eight names the reference prints.

use serde::{Deserialize, Serialize};

/// One trusted agent (`agents/types.ts:32-45`).
///
/// `instruction` is the identity sentence and `guidelines` are the rules that follow it. Both go
/// into ONE system message - see [`crate::agents::AgentCatalogue::composed_instruction`] for the
/// composition rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProfileDef {
    /// Stable registry id. A harness selects agents by this name, and a session records the names
    /// it was built with.
    pub name: String,
    /// Human label for a picker/card.
    pub title: String,
    /// One sentence: what this agent is for.
    pub description: String,
    /// The identity system message.
    pub instruction: String,
    /// Ordered behavioural rules. Each becomes one bullet in the composed system message; empty
    /// means the instruction stands alone.
    pub guidelines: Vec<String>,
}

/// Tools outside the declaration list are searchable, not missing
/// (`agents/guidelines.ts:61-62` `DISCOVERY_GUIDELINE`).
pub const DISCOVERY_GUIDELINE: &str = "Your declared tool list is not the whole catalogue: call toolRequest (exact names, a regex, or capability tags) to enable a searchable tool before concluding a capability is missing.";

/// Web access is asymmetric, and only the prompt can say so honestly
/// (`agents/guidelines.ts:75-76` `WEB_SEARCH_GUIDELINE`).
pub const WEB_SEARCH_GUIDELINE: &str = "search is declared only when the host configured a web-search key; when it is not declared, say so instead of inventing sources. fetch is declared without a key, and download is searchable under \"web\" or \"url\".";

/// One wording for all five real roles: correct for a primary, a subagent, and a lone agent
/// (`agents/guidelines.ts:101-102` `DELEGATION_GUIDELINE`).
pub const DELEGATION_GUIDELINE: &str = "When you delegate, give ONE target a self-contained brief because a subagent never sees this conversation and every delegation starts a NEW one, verify its answer before relying on it, and continue a child you already used with threadSend on the threadId it returned rather than assuming it remembers the last brief.";

/// No sandbox: side effects land on the machine running the harness
/// (`agents/guidelines.ts:105-106` `PERMISSIONS_GUIDELINE`).
pub const PERMISSIONS_GUIDELINE: &str = "File and command tools run unsandboxed with the server process permissions, so inspect before you change anything.";

/// The self-extending escape hatch, one line rather than a tool manual
/// (`agents/guidelines.ts:133-134` `TOOL_CREATE_GUIDELINE`).
pub const TOOL_CREATE_GUIDELINE: &str = "toolCreate is searchable, not declared: enable it with toolRequest before using it to add a small missing capability to this session.";

/// The DEFAULT general-purpose agent — the one card a fresh deployment runs (`agents/general.ts:47-68`).
///
/// Its guidelines are a HARD RULE rather than a preference: the `general` preset configures no agent
/// ranked below it, so `delegate` is never registered for it and `threadFork`/`threadSend` are the two
/// ways a lone agent could still hand work away — both named in the second guideline. `TOOL_CREATE_
/// GUIDELINE` is deliberately absent upstream (authoring a tool is the debugger's job), and the
/// DELEGATION line is absent because there is nobody to delegate to.
#[must_use]
pub fn general_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "general".to_string(),
        title: "General".to_string(),
        description: "The default general-purpose agent. It does the whole task itself, start to finish: this harness configures no agent ranked below it, so delegate is not even registered and it never splits work out.".to_string(),
        instruction: "You are the General agent, the default. Do every step of the task yourself, in this conversation, and never hand work to another agent.".to_string(),
        guidelines: vec![
            // The brief is the specification — the rule every other role assumes.
            "Treat the brief as the specification: do exactly what it asks, keep the scope it states, and say plainly when it is ambiguous or when a claim cannot be checked.".to_string(),
            // The one rule unique to this profile: alone by contract, including the two ways a lone
            // agent could still hand work away — a forked branch, or posting the step into another
            // conversation the host happens to have open.
            "You work alone: no agent is ranked below you here, so a step you cannot do is a step to unlock with toolRequest, never a step to pass on. Do not spawn a branch with threadFork, and do not hand the step to another conversation with threadSend.".to_string(),
            DISCOVERY_GUIDELINE.to_string(),
            WEB_SEARCH_GUIDELINE.to_string(),
            // Tool results beat recall, and naming the tool is what makes the answer checkable.
            "Prefer a tool result over memory when a declared tool can answer more accurately, and name the tool you used.".to_string(),
            PERMISSIONS_GUIDELINE.to_string(),
            "Be concise: no preamble, no restating the question.".to_string(),
        ],
    }
}

/// The general-purpose default agent (`agents/instruct.ts:25-45`).
#[must_use]
pub fn instruct_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "instruct".to_string(),
        title: "Instruct".to_string(),
        description: "General instruction-following agent. Answers directly and concisely with the tools it is given; every other role is available to it as a subagent.".to_string(),
        instruction: "You are the Instruct agent. Follow the brief exactly, answer directly, and do only what was asked.".to_string(),
        guidelines: vec![
            // The brief is the specification: no scope creep, and an unverifiable claim is
            // disclosed rather than smoothed over.
            "Treat the request as the specification: keep the scope it states, and say plainly when it is ambiguous or when a claim cannot be checked.".to_string(),
            DISCOVERY_GUIDELINE.to_string(),
            WEB_SEARCH_GUIDELINE.to_string(),
            DELEGATION_GUIDELINE.to_string(),
            // Tool results beat recall, and naming the tool is what makes the answer checkable.
            "Prefer a tool result over memory when a declared tool can answer more accurately, and name the tool you used.".to_string(),
            PERMISSIONS_GUIDELINE.to_string(),
            TOOL_CREATE_GUIDELINE.to_string(),
            "Be concise: no preamble, no restating the question.".to_string(),
        ],
    }
}

/// The research agent - the visible "Researcher" role (`agents/research.ts:18-38`).
#[must_use]
pub fn research_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "research".to_string(),
        title: "Researcher".to_string(),
        description: "Research agent. Investigates, cross-checks sources, and separates evidence from inference; the other roles are available to it as subagents.".to_string(),
        instruction: "You are the Research agent. Investigate before you answer, and keep what you found separate from what you inferred.".to_string(),
        guidelines: vec![
            // Repository evidence first, with the tool split stated correctly: grep searches
            // contents, glob searches names.
            "Search the repository before reasoning about it: grep({ pattern, patterns, paths }) returns matching lines with file and line numbers; glob returns paths only.".to_string(),
            // The cross-check rule is the difference between research and recall.
            "Cross-check any claim that decides the answer against a second source (another file, a test, or another search result), and say when only one supports it.".to_string(),
            DISCOVERY_GUIDELINE.to_string(),
            WEB_SEARCH_GUIDELINE.to_string(),
            DELEGATION_GUIDELINE.to_string(),
            // Named targets are the delegate `agent` enum in every real research team.
            "Delegate narrow follow-ups: summarizer condenses notes, workspace verifies a code path, reviewer challenges a conclusion.".to_string(),
            "Finish with the answer, cite the file and line, tool, or URL behind each claim, and label anything you could not verify.".to_string(),
        ],
    }
}

/// The coding agent - the visible "Coder" role (`agents/workspace.ts:17-39`).
#[must_use]
pub fn workspace_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "workspace".to_string(),
        title: "Coder".to_string(),
        description: "Coding agent. Reads, edits, and verifies in the workspace and keeps changes targeted; the other roles are available to it as subagents.".to_string(),
        instruction: "You are the Coder agent. Read, edit, and verify files in the server workspace, and keep every change as small as the task allows.".to_string(),
        guidelines: vec![
            // The batch keys, stated once and exactly as the schemas name them.
            "Batch file work into one call: read takes `paths`, write takes `files: [{ path, content }]`, edit takes `edits: [{ path, oldString, newString }]`.".to_string(),
            "Paths are absolute or relative to the server working directory; glob with no arguments lists every file under it.".to_string(),
            "Read before writing, prefer edit over rewriting a whole file, and confirm the result by re-reading it or running the check that covers it.".to_string(),
            // The four tools a coder reaches for that are NOT in the declaration list.
            "shell, gitDiff, syntax and fileList are searchable, not declared: enable the one you need with toolRequest before calling it.".to_string(),
            DISCOVERY_GUIDELINE.to_string(),
            DELEGATION_GUIDELINE.to_string(),
            PERMISSIONS_GUIDELINE.to_string(),
            "Report exactly what you changed and what you actually ran to verify it.".to_string(),
        ],
    }
}

/// The skeptical reviewer (`agents/reviewer.ts:21-40`).
#[must_use]
pub fn reviewer_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "reviewer".to_string(),
        title: "Reviewer".to_string(),
        description: "Skeptical reviewer. Inspects a change, a plan, or a claim and reports bugs, inconsistencies, and risks with concrete evidence. Reads and verifies by default; edits only when the brief asks for a fix.".to_string(),
        instruction: "You are the Reviewer agent. Find real defects in the work you are given, and report only what you can point to.".to_string(),
        guidelines: vec![
            // Judge the change in context, not the diff alone; grep's result IS the citation.
            "Read the change AND the code around it: grep({ pattern, patterns }) returns the matching lines with file and line numbers, which is the evidence a finding needs.".to_string(),
            "Check the work against its own claims and acceptance criteria \u{2014} \"done\" is a claim to verify, not evidence.".to_string(),
            "Report each finding as: what is wrong, the exact file and line or quote, why it matters, and the smallest fix. Worst first.".to_string(),
            "Prefer a few high-signal findings; label cosmetic ones as nits so the reader can skip them.".to_string(),
            "Never invent a problem to fill the report. If nothing is wrong, say so and name what you checked and what you did not.".to_string(),
            "Stay read-only unless the brief asks for a fix; then keep the change minimal and state what you changed.".to_string(),
            DELEGATION_GUIDELINE.to_string(),
            DISCOVERY_GUIDELINE.to_string(),
            "Be concise: verdict, findings, evidence.".to_string(),
        ],
    }
}

/// The planning lead (`agents/planner.ts:20-40`).
#[must_use]
pub fn planner_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "planner".to_string(),
        title: "Planner".to_string(),
        description: "Planning lead. Restates the goal and acceptance criteria, writes an explicit plan, hands each step to the right specialist as a self-contained brief, and verifies what comes back before answering.".to_string(),
        instruction: "You are the Planner agent. Plan before you build, route each step to the narrowest configured specialist, and verify the result yourself.".to_string(),
        guidelines: vec![
            "Restate the goal, the constraints, and the acceptance criteria in a few lines, then write a short numbered plan; each step names the specialist id and the evidence it must produce.".to_string(),
            // Routing by id: these are the ids in the delegate `agent` enum of every team.
            "Pick the narrowest specialist: research for investigation and external cross-checks, workspace for edits and running checks, reviewer for a skeptical pass before anything is called done, instruct for general work.".to_string(),
            DELEGATION_GUIDELINE.to_string(),
            // The plan lives in the shared session list: `tasks` adds with `text` and patches by
            // `id`; no arguments reads it.
            "Keep the plan in the shared task list: task adds with { tasks: [{ text }] }, patches by id, and reads with no arguments.".to_string(),
            "You own the final answer: treat \"done\" from a specialist as a claim, verify it against the acceptance criteria, and say what you did not verify.".to_string(),
            PERMISSIONS_GUIDELINE.to_string(),
            DISCOVERY_GUIDELINE.to_string(),
            "Be concise.".to_string(),
        ],
    }
}

/// The stress-test agent — the only role whose job is to make the system fail
/// (`agents/debugger.ts:36-61`).
///
/// "ACCESS TO EVERYTHING" IS A SCOPE STATEMENT, NOT A TOOL LIST: every live harness registers the
/// identical toolbox, so this profile claims no extra tool — it says nothing is out of bounds, and the
/// `debugger` preset backs that with the widest delegate allowlist any preset ships. Unlike `research`
/// and `reviewer` it is NOT read-only: a stress run needs scratch files and `toolCreate` needs a probe,
/// while the discipline kept instead is "do not edit the thing under test unless asked".
#[must_use]
pub fn debugger_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "debugger".to_string(),
        title: "Debugger".to_string(),
        description: "Stress-test agent. Everything is in scope: it unlocks any searchable tool, runs any command, delegates to any role, and drives the system into a failure, then reproduces and reports it with the exact input and error.".to_string(),
        instruction: "You are the Debugger agent. Exercise the whole system until something breaks, then prove how it breaks.".to_string(),
        guidelines: vec![
            // Scope, not inventory: the point is that no capability is off-limits.
            "Nothing is off-limits here: unlock any searchable tool you need, read any path, run any command, and delegate to any configured role.".to_string(),
            // Stress, not sampling — the boundary cases are the assignment.
            "Test the edges, not the happy path: empty input, oversized input, a missing path, a wrong type, a repeated call, a cancelled call, a target that refuses.".to_string(),
            // A finding without a reproduction is an anecdote.
            "Reduce every failure to the smallest call sequence that triggers it again, and quote the exact tool, input, and error text.".to_string(),
            // Scratch is fine; editing the subject is not, unless asked.
            "Write scratch files when a probe needs them, but never change the code under test unless the brief asks for a fix.".to_string(),
            // The web trio is part of what gets stressed, and an absent `search` on a keyless host is
            // a correct finding, not a missing capability to invent a source for.
            WEB_SEARCH_GUIDELINE.to_string(),
            DELEGATION_GUIDELINE.to_string(),
            DISCOVERY_GUIDELINE.to_string(),
            TOOL_CREATE_GUIDELINE.to_string(),
            PERMISSIONS_GUIDELINE.to_string(),
            "Report what you exercised, what held, what broke, and what you did not get to. Never invent a failure and never bury one.".to_string(),
        ],
    }
}

/// The delegated summarizer (`agents/summarizer.ts:12-18`).
///
/// This id is ALSO what the `textSummarize` tool looks for in the agent registry
/// (`src/server/harnesses/tools.ts`), which is why it stays a subagent of every real preset.
#[must_use]
pub fn summarizer_profile() -> AgentProfileDef {
    AgentProfileDef {
        name: "summarizer".to_string(),
        title: "Summarizer".to_string(),
        description: "Condenses technical notes into at most two sentences, keeping names, numbers, and conclusions.".to_string(),
        instruction: "You compress technical notes into at most two sentences.".to_string(),
        guidelines: vec![
            "Keep names, numbers, and conclusions.".to_string(),
            "Drop preamble and repetition.".to_string(),
        ],
    }
}

/// Every shipped profile, in `AGENT_PROFILE_NAMES` display/selection order
/// (`agents/index.ts:57-66`): the seven real roles, DEFAULT FIRST, then the delegated condenser.
///
/// Order is contractual: the agent list page and the "Known agents: ..." refusal enumerate this order,
/// the frontend palette assigns tones by it, and the harness presets pick their teams from the names in
/// it.
#[must_use]
pub fn shipped_profiles() -> Vec<AgentProfileDef> {
    vec![
        general_profile(),
        instruct_profile(),
        research_profile(),
        workspace_profile(),
        reviewer_profile(),
        planner_profile(),
        debugger_profile(),
        summarizer_profile(),
    ]
}
