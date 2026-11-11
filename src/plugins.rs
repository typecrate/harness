//! The built-in PLUGIN catalogue and the operator's settings for it - the server-visible half of
//! the Harness Plugin System.
//!
//! TypeScript origin: `packages/underload/harness/src/server/plugins.ts` (the settings store and
//! every validation sentence in it), `src/plugin/manifest.ts` (the GENERATED built-in catalogue
//! this static table replaces), the `metadata` export of each
//! `src/plugins/{async-ping,context-compaction,context-trimmer}.plugin.ts` (what each row carries),
//! and `src/server/api.ts` `pluginSettingOf` / `pluginRowOf` (`:2300-2364`) with `getPlugins` /
//! `patchPlugin` (`:2368-2432`) for the wire projection.
//!
//! WHAT THIS MODULE IS, AND WHAT IT IS NOT
//! ---------------------------------------
//! The plugin HOST - the bus that delivers lifecycle events to registered handlers - is
//! [`crate::plugin`]. THIS module is the half the HTTP contract touches:
//!
//!   * the CATALOGUE - the three built-ins this build ships, as a STATIC table (there is no scan, so
//!     this table IS the manifest: same rows, same order, same declared metadata);
//!   * the SETTINGS DESCRIPTORS - key, label, declared type, declared default, `min`/`max`/`step`,
//!     description;
//!   * the EFFECTIVE VALUES - the declared default unless the operator stored an override, which is
//!     what a row paints and what a `null` patch restores;
//!   * the FRAMEWORK SWITCH - the reserved `enabled` key ([`PLUGIN_ENABLED_KEY`]), which the host's
//!     delivery gate consults ([`crate::plugin`] / `plugin/host.ts` rule 9) and this store resolves
//!     through the SAME stored -> declared -> fallback chain as every declared key;
//!   * the WRITE EDGE - validation against the descriptor, with the contract's exact sentences;
//!   * the DURABLE DELTA - [`PluginSettingsStore::snapshot`] is the `pluginSettings` member of
//!     `state.json`.
//!
//! The one behaviour the TypeScript build adds on top is a LIVE PROVIDER a running harness reads on
//! its next turn (`plugins.ts provider`). Here the live map IS the store, so an accepted patch is
//! immediately visible to every later read of every key by every session in the process - which is
//! also what the host's handlers see when they re-read their settings per pass. `knowledge.md`
//! "T10 plugin uplift" records the port decisions and the remaining deviations.
//!
//! WHY THE CATALOGUE IS A `static`
//! -------------------------------
//! `BUILTIN_PLUGINS` is compile-time data: the rows ARE the contract, the suite pins them
//! byte-exact, and a `static` makes it impossible for one request to see a different catalogue than
//! the next. Only the OPERATOR's overrides move at runtime, and they are stored in the descriptor's
//! declared type (see [`SettingValue`]) so a restored document, a submitted patch and an answered
//! row cannot disagree about a value's spelling.
//!
//! HOW THE ROWS WERE WRITTEN
//! -------------------------
//! Every title, description, label, default and bound below was generated mechanically from the
//! reference server's own `GET /v1/harness/plugins` body (the captured fixture
//! `Temp/fixtures/harness/plugin-catalogue/descriptors-by-plugin.json`, see its `MANIFEST.md`), not
//! transcribed by hand - these descriptions run to a thousand characters each and a single drifted
//! word would fail the byte-exact row test. `tests/plugins.rs` re-pins the same rows from fixture
//! copies under `tests/fixtures/`, so a drift is caught at either end.
//!
//! THE NUMBER SPELLING RULE
//! ------------------------
//! The contract prints an integral setting as a JSON INTEGER - `600000`, `1000`, never `600000.0`.
//! Rust does its bound arithmetic in `f64`, and serde_json prints an `f64` WITH its fraction. So
//! every number that reaches the wire or `state.json` goes through [`number_to_json`], and every
//! number quoted back in an error message through [`format_number`]. Those two helpers are
//! ECMAScript's `JSON.stringify(number)` and `String(number)`, which the contract's tests assert
//! literally.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::contract::{self, PluginSettingType};
use crate::error::ApiError;
use crate::persistence::HarnessPluginSettingsRecord;

// ---------------------------------------------------------------------------
// The catalogue: the generated manifest, written by hand because there is no scan
// ---------------------------------------------------------------------------

/// The declared default of one setting.
///
/// A plain enum rather than a `Value`, because `Value` is not const-constructible and the catalogue
/// has to be a `static`. [`DeclaredDefault::to_json`] is the one bridge to the wire.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeclaredDefault {
    Number(f64),
    Text(&'static str),
    Flag(bool),
}

impl DeclaredDefault {
    /// The wire / `state.json` form, with the integer spelling rule applied.
    #[must_use]
    pub fn to_json(self) -> Value {
        match self {
            Self::Number(value) => number_to_json(value),
            Self::Text(text) => Value::String(text.to_owned()),
            Self::Flag(flag) => Value::Bool(flag),
        }
    }

    /// The declared TYPE this default belongs to, which must equal the descriptor's own `kind`.
    #[must_use]
    pub fn kind(self) -> PluginSettingType {
        match self {
            Self::Number(_) => PluginSettingType::Number,
            Self::Text(_) => PluginSettingType::String,
            Self::Flag(_) => PluginSettingType::Boolean,
        }
    }

    /// The stored form of the default, for [`PluginSettingsStore::values`].
    #[must_use]
    fn to_stored(self) -> SettingValue {
        match self {
            Self::Number(value) => SettingValue::Number(value),
            Self::Text(text) => SettingValue::Text(text.to_owned()),
            Self::Flag(flag) => SettingValue::Flag(flag),
        }
    }
}

/// One DECLARED setting of a plugin (`HarnessPluginSettingDescriptor`, `types/index.ts:3764`, and
/// the `metadata.settings` literal of each `*.plugin.ts`).
#[derive(Debug, Clone, Copy)]
pub struct PluginSettingDescriptor {
    /// The stable id the runtime reads and the patch writes.
    pub key: &'static str,
    /// The form label an editor renders.
    pub label: &'static str,
    /// The declared type, which decides the validation branch.
    pub kind: PluginSettingType,
    /// The value the plugin falls back to when nothing is stored, and the target a `null` patch
    /// restores.
    pub default: DeclaredDefault,
    /// Inclusive lower bound the SERVER enforces.
    pub min: Option<f64>,
    /// Inclusive upper bound the SERVER enforces.
    pub max: Option<f64>,
    /// Editor hint, deliberately NEVER enforced. `plugins.ts` states why: refusing `contextLimit:
    /// 1500` because `step: 1000` would make the API stricter than the runtime that has to live
    /// with the value, and would strand anything a migration or a hand-edited document produced.
    pub step: Option<f64>,
    pub description: Option<&'static str>,
}

/// One built-in plugin's declared metadata (`HarnessPluginMetadata`).
#[derive(Debug, Clone, Copy)]
pub struct PluginMetadata {
    pub name: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub version: &'static str,
    pub tags: &'static [&'static str],
    /// The lifecycle hooks the plugin DECLARES. Server-visible metadata only - nothing here runs.
    pub hooks: &'static [&'static str],
    pub settings: &'static [PluginSettingDescriptor],
}

/// The shipped catalogue, in the TypeScript manifest's SCAN ORDER (path ascending from
/// `src/plugins`): `async-ping.plugin.ts`, then `context-compaction.plugin.ts`, then
/// `context-trimmer.plugin.ts`.
///
/// The order is contract-visible: `GET /plugins` answers in it, the Plugins pane renders it as-is,
/// the `state.json` snapshot is written in it, and the 404 sentence joins these names with `", "`.
pub static BUILTIN_PLUGINS: [PluginMetadata; 3] = [
    // `async-ping.plugin.ts`'s `metadata` export, verbatim - captured from the reference
    // server's own `GET /plugins` row (see the module header for the provenance).
    PluginMetadata {
        name: "async-ping",
        title: "Background Ping",
        description: "Keeps a conversation waiting on background tool work moving by emitting a periodic ping with the calls that are still running, replacing the fixed scheduler heartbeat.",
        version: "1.0.0",
        tags: &["background", "async", "lifecycle"],
        hooks: &[
            "async.pending",
            "async.drained",
            "run.failed",
            "thread.stopped",
            "thread.deleted",
            "harness.dispose",
        ],
        settings: &[PluginSettingDescriptor {
            key: "intervalMs",
            label: "Ping interval (ms)",
            kind: PluginSettingType::Number,
            default: DeclaredDefault::Number(600000.0),
            min: Some(1000.0),
            max: None,
            step: Some(1000.0),
            description: Some(
                "How often a still-running background tool is reported while work is pending. Defaults to the historical 10-minute wake.",
            ),
        }],
    },
    // `context-compaction.plugin.ts`'s `metadata` export, verbatim - captured from the reference
    // server's own `GET /plugins` row (see the module header for the provenance).
    PluginMetadata {
        name: "context-compaction",
        title: "Context Compaction",
        description: "Watches each agent conversation against a context limit and, when the limit is reached, compacts the conversation at the turn boundary before the next model request is sent. In the default \"summarize\" mode the conversation does the work: the thread is parked, an instruction the plugin commits into the transcript is answered by this same agent with a summary of what is done, what it is working on and where it left off, and everything before that instruction (and the turns between it and the answer) stops being sent. The thread resumes with a nudge to continue, and the context is small again: the instruction plus the agent's own summary. The older \"exclude\" mode prunes only the tool units a later success already answered. Neither mode deletes anything: a withheld unit keeps its place in history and can be switched back on. The limit is the firing point: compaction starts as soon as the estimate reaches the effective limit, which is the per-model override for the model the conversation is running, capped by that model's declared context window when one is declared.",
        version: "1.0.0",
        tags: &["context", "compaction", "lifecycle"],
        hooks: &[
            "turn.before",
            "turn.after",
            "thread.idle",
            "run.finished",
            "run.failed",
        ],
        settings: &[
            PluginSettingDescriptor {
                key: "contextLimit",
                label: "Context limit (tokens)",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(200000.0),
                min: Some(1000.0),
                max: None,
                step: Some(1000.0),
                description: Some(
                    "The firing point: estimated tokens a conversation may reach before this plugin compacts it, measured with the ~4 characters/token estimate the UI context bar uses, anchored on the reported prompt size of the last request that reported usage — so after the first report it can differ from the bar by whatever the tool schemas and message framing actually cost. There is no separate headroom: the estimate at or above this number fires the pass on that turn. It is this value unless modelLimits overrides it for the model this conversation runs, or caps it at that model's declared context window. Set to 0 to turn compaction off for every model.",
                ),
            },
            PluginSettingDescriptor {
                key: "modelLimits",
                label: "Per-model limits (JSON)",
                kind: PluginSettingType::String,
                default: DeclaredDefault::Text("{}"),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "A JSON object keyed by model id, each value `{ \"limit\": number, \"window\": number }` — both optional. `limit` replaces contextLimit for that model, for an operator who is willing to pay more (or less) for one model than for the rest. `window` states the model's HARD context window: it caps whatever limit is in force, so a 512000 ceiling on a 262144-token model still compacts around the 256k behaviour rather than waiting for a size the provider never accepts. Example: `{\"gpt-4o\": {\"limit\": 512000, \"window\": 131072}, \"llama-8b\": {\"window\": 8192}}`. The model id is read live from the agent, so switching models changes the ceiling on the next turn. Anything malformed — broken JSON, a value that is not an object, a non-positive or non-numeric limit — is ignored and the global limit applies, never an error.",
                ),
            },
            PluginSettingDescriptor {
                key: "enabled",
                label: "Compact conversations",
                kind: PluginSettingType::Boolean,
                default: DeclaredDefault::Flag(true),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "Runs compaction on or off, in either mode. Off changes nothing else: the limit is still read, the context bar still measures, and no other plugin is affected. It is the switch an operator expects when they want the behaviour gone without removing the built-in.",
                ),
            },
            PluginSettingDescriptor {
                key: "minTokens",
                label: "Minimum saving per pass (tokens)",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(200.0),
                min: Some(0.0),
                max: None,
                step: Some(50.0),
                description: Some(
                    "A pass must withhold at least this many estimated tokens, or it does nothing at all — no flag write, no persistence notification, no save. A prune that saves 40 tokens is a provider prompt-cache invalidation for nothing, so this floor is what makes running on every turn cheap. Measured as a difference of the same estimate, so any provider anchor cancels out of it and only the withheld characters count. In \"summarize\" mode the same number is read the other way round, as a trivial floor on the LIVE estimate: a conversation smaller than this is never worth a compaction round trip, so the pass refuses rather than paying for a summary of a handful of turns. Set it to 0 to remove the floor, which a small per-model limit needs: a floor above that limit would never be crossed.",
                ),
            },
            PluginSettingDescriptor {
                key: "safetyTurns",
                label: "Recent units to protect",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(6.0),
                min: Some(0.0),
                max: None,
                step: Some(1.0),
                description: Some(
                    "How many units at the END of the conversation no pass may touch. A unit is one assistant tool-call block plus its results, or one standalone message. The horizon counts UNITS from the end, never the per-run turn counter: a failure the model has not moved past yet is the highest-signal thing in the window, and pruning it is how an agent repeats the call it just watched fail. 0 removes the horizon. It belongs to \"exclude\" mode; \"summarize\" mode needs no horizon because the recent tail is exactly what the agent is summarizing.",
                ),
            },
            PluginSettingDescriptor {
                key: "mode",
                label: "Compaction mode",
                kind: PluginSettingType::String,
                default: DeclaredDefault::Text("summarize"),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "Which compaction runs when the limit is reached. \"summarize\" — the default since the 2026-11-10 context-compaction ruling — parks the conversation at the turn boundary and lets its NEXT model turn write the summary: the agent condenses what is done, what it is working on and where it left off, everything before that instruction stops being sent, and the conversation resumes with a nudge. \"exclude\" is the earlier behaviour: a superseded tool unit keeps its place in canonical history and simply stops being sent, which is the reversible option the evidence favours and the cheaper one — no extra model turn. \"annotate\" — replace the result with a one-line handle back to the full text — is the planned SECOND WRITER over the same candidate list the policy already computes before writing anything, and any value other than these two (including that one) behaves as \"exclude\", the flow that cannot spend a model turn by accident. The value actually used is reported in the pass diagnostic.",
                ),
            },
        ],
    },
    // `context-trimmer.plugin.ts`'s `metadata` export, verbatim - captured from the reference
    // server's own `GET /plugins` row (see the module header for the provenance).
    PluginMetadata {
        name: "context-trimmer",
        title: "Context Trimmer",
        description: "The posture this built-in exists to hold: the engine core trims nothing by default — every recorded message stays in the provider request — and this plugin is the one place trimming happens, one rule per switch. At the start of each assistant turn it finds tool calls that failed or were rejected and were later replaced by a successful call to the same tool, and excludes those paired call+result units from the context. Which outcomes count as invalid, whether supersession matches the tool name or the exact call fingerprint, whether thinking from earlier turns stops being replayed, and how much a pass must save before it writes anything are each a separate switch, and every one of them defaults to today's behaviour. A unit is withheld whole or not at all, an unanswered call is never touched, and a completed result is never invalid on its own unless dropDuplicateCalls is on and the fingerprints match. Nothing is deleted: a withheld unit keeps its place in canonical history and can be switched back on, and a switch flipped off stops new writes without restoring what an earlier turn already withheld.",
        version: "1.0.0",
        tags: &["context", "trimming", "lifecycle"],
        hooks: &["turn.before"],
        settings: &[
            PluginSettingDescriptor {
                key: "enabled",
                label: "Context trimming",
                kind: PluginSettingType::Boolean,
                default: DeclaredDefault::Flag(true),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "Runs every trimming policy this plugin has, on or off. Off stops the pass, so none of the switches below is consulted and nothing is withheld from that turn on; it changes nothing else: units an earlier turn already trimmed stay trimmed until they are switched back on from the context view, no other plugin is affected, and the transcript is never rewritten. This is the framework's own key, so the switch in the Plugins row and this read are one stored value with one write path. Off is NOT the engine's posture: the engine's posture is every policy below off, which sends everything that was recorded.",
                ),
            },
            PluginSettingDescriptor {
                key: "dropSupersededToolCalls",
                label: "Drop superseded tool calls",
                kind: PluginSettingType::Boolean,
                default: DeclaredDefault::Flag(true),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "The shipped rule, verbatim. A tool-call unit whose every committed result was failed or rejected, that is FULLY answered, and where EVERY tool name it called has a completed answer in some later unit that is still included, stops being sent as a whole unit: the assistant block and every result answering one of its calls leave together, so the request stays pair-clean. Every name must be answered, so a block that called A and B where only A was retried successfully stays whole — half a unit leaving is worse than the whole unit staying, and the failed half's result is what the model would lose. Flipping this off stops new writes and restores nothing an earlier turn already withheld. An unanswered unit is never touched, and a completed result is never invalid on its own: only dropDuplicateCalls can withhold a healthy unit, and only on an exact fingerprint match.",
                ),
            },
            PluginSettingDescriptor {
                key: "dropInvalidToolCallsOnceAnswered",
                label: "Drop invalid tool calls once answered",
                kind: PluginSettingType::Boolean,
                default: DeclaredDefault::Flag(true),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "The same rule generalised over what counts as an invalid result, which is what invalidOutcomes decides. It is INDEPENDENT of dropSupersededToolCalls: turning one off leaves the other running, and with both off no tool unit is ever withheld, so the request carries every recorded tool call the harness has — the engine's posture. Like the shipped rule it withholds only a fully answered unit, only as a whole unit, and only once a still-included later unit has answered every name it called; a resolver that is itself withheld is not evidence, because then the model would see neither the failure nor the answer. Off stops new writes and restores nothing.",
                ),
            },
            PluginSettingDescriptor {
                key: "invalidOutcomes",
                label: "Outcomes that count as invalid",
                kind: PluginSettingType::String,
                default: DeclaredDefault::Text("failed,rejected"),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "Which committed results count as invalid for dropInvalidToolCallsOnceAnswered, as a comma-separated set drawn from failed, rejected, cancelled, empty. failed and rejected are the shipped rule. cancelled is opt-in only: the agent CHOSE to stop there, and a healthy control outcome is not garbage in this package — a unit mixing a cancelled answer with a healthy one is a keep either way. empty is opt-in and dangerous: it treats a completed result whose text is blank as invalid, and a legitimately empty answer — a grep with no matches, a zero-hit search — is indistinguishable from a stub, so withholding it removes the one fact that proves the agent already looked. Unknown names (including completed, which is only ever reachable through dropDuplicateCalls) are dropped and the pass says so; a set that resolves to nothing disables the generalisation, leaving dropSupersededToolCalls as the only tool policy. The set actually used is reported in the pass diagnostic.",
                ),
            },
            PluginSettingDescriptor {
                key: "dropOldThoughts",
                label: "Drop old thinking",
                kind: PluginSettingType::String,
                default: DeclaredDefault::Text("turn"),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "Which of the model's own thinking stops being replayed. \"off\": nothing is ever withheld and every recorded thought stays eligible for replay — the engine's posture, and the right value for a backend whose template concatenates historical thinking. \"turn\" (the default): the OPEN turn keeps its thinking — every committed assistant entry after the last user entry, which is why a mailbox or autoanswer user message legitimately moves the boundary — and the thinking of every CLOSED turn is withheld; it is the only tier every strict provider contract accepts at once, and it pins the provider's cache divergence point one turn back forever instead of walking it backwards through history. \"all\": every committed thought is withheld, including the open turn's — only sane on a transport that replays nothing, and it costs the current-turn continuity that every measured failure mode blames for re-deciding and for thin tool calls. Any other string behaves as \"turn\" and the diagnostic says so. The write is a per-entry FLAG on the assistant entry, never an edit of the text: the entry still travels, with its content and tool calls intact, and only its request copy drops the thinking, so provider pairing is untouched and canonical history keeps every thought — including of an entry this pass flagged. An entry with no recorded thinking, one that is already flagged, and one whose whole unit is already withheld are never candidates, so a second pass over an unchanged transcript writes nothing. The estimator does not count reasoning, so a withheld thought registers as zero saved tokens; measure the effect in characters with minThoughtChars. Withhold-only: switching to \"off\" stops new writes and restores nothing.",
                ),
            },
            PluginSettingDescriptor {
                key: "thoughtKeepUnits",
                label: "Earlier units that keep thinking",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(0.0),
                min: Some(0.0),
                max: Some(32.0),
                step: Some(1.0),
                description: Some(
                    "How many otherwise-withheld thinking entries, counted from the NEWEST entry backwards, additionally keep their thinking — the recent-N tier, for the backends that are trained on their own reasoning (local Qwen/GLM/Kimi, DeepSeek with tools, the keep-all Claude generations). Only consulted while dropOldThoughts is \"turn\" or \"all\"; it counts only entries that actually carry recorded thinking, and it never re-enables thinking an earlier turn already withheld. Every value ABOVE 0 is documented as cache-hostile: a sliding window walks the provider's divergence point backwards through previously-stable history on every eviction, which is strictly worse than 0 on both the window and the cache — use \"off\" if the whole history is wanted. 0 keeps exactly one turn: the open one. It runs BEFORE minThoughtChars, which measures what the rescue leaves behind.",
                ),
            },
            PluginSettingDescriptor {
                key: "supersedeMatch",
                label: "Supersession match",
                kind: PluginSettingType::String,
                default: DeclaredDefault::Text("name"),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "What makes a later call the answer to an earlier one. \"name\" is the shipped rule: the tool NAME alone, deliberately looser than compaction's fingerprint, because a failed call followed later by a successful call of the same tool is almost always superseded work whether or not the retry used the same arguments (read(\"a.ts\") failing and read(\"b.ts\") answering the question the agent actually had). \"fingerprint\" is compaction's exact rule — tool name AND canonical arguments must match — which makes the two passes agree by construction instead of by comment and is the value that makes compaction's failure-retry pruning redundant. Any other string behaves as \"name\" and the diagnostic says so. Neither value relaxes granularity: the unit still leaves whole, and every name the block called still has to be answered.",
                ),
            },
            PluginSettingDescriptor {
                key: "dropDuplicateCalls",
                label: "Drop duplicate tool calls",
                kind: PluginSettingType::Boolean,
                default: DeclaredDefault::Flag(false),
                min: None,
                max: None,
                step: None,
                description: Some(
                    "Withhold an EARLIER unit when a later, still-included unit re-ran the SAME work and answered it — matched on the exact fingerprint (tool name plus canonical arguments), NEVER on name alone — whatever the earlier unit's own outcome, so a unit the model read and used is reachable. Off by default: this is the one switch here that can withhold work the model actually used, and it is the largest class of redundant context in a long trajectory. It never touches an unanswered unit, never acts on a name-only match, and only ever withholds a unit whose re-run produced completed answers.",
                ),
            },
            PluginSettingDescriptor {
                key: "minTokens",
                label: "Minimum saving per pass (tokens)",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(0.0),
                min: Some(0.0),
                max: Some(1000000.0),
                step: Some(50.0),
                description: Some(
                    "A pass must withhold at least this many estimated tokens of TOOL-UNIT text, or it writes nothing at all: no flag, no notification, no save, no diagnostic. A prune that saves 40 tokens is a provider prompt-cache invalidation for nothing, so this floor is what makes trimming on every turn cheap. 0 (the default) is the shipped posture: the rule only ever removes pure noise, so no floor was needed. The scoping to tool writes is not a detail — the estimator does not count reasoning, so a floor shared with the thinking writes would silently disable the thoughts switch whenever it was above zero.",
                ),
            },
            PluginSettingDescriptor {
                key: "minThoughtChars",
                label: "Minimum thinking saved per pass (characters)",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(0.0),
                min: Some(0.0),
                max: Some(1000000.0),
                step: Some(50.0),
                description: Some(
                    "The thinking twin of minTokens, measured in CHARACTERS of withheld thinking — the only measure available for text the estimator does not count. A thinking pass below this floor writes nothing at all. 0 (the default) removes the floor. Do not convert this number into tokens: while the estimator is not reasoning-aware the two figures cannot be compared, and a tokens-saved number that includes thinking would disagree with the context bar the operator is reading.",
                ),
            },
            PluginSettingDescriptor {
                key: "safetyUnits",
                label: "Recent units to protect",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(0.0),
                min: Some(0.0),
                max: Some(4096.0),
                step: Some(1.0),
                description: Some(
                    "How many units at the END of the transcript no tool trim may touch. A unit is one assistant tool-call block plus its results, or one standalone message, and the horizon counts UNITS from the end — never the per-run turn counter, which counts model steps inside one run and cannot express a conversation-level position. 0 (the default) removes the horizon, which is the shipped argument: a failure the model just replaced with a success is exactly the pair that is safe to drop from the NEXT request. Raise it when re-sending the last few units is cheaper than re-prefilling from the oldest trimmed position in the provider's cache.",
                ),
            },
            PluginSettingDescriptor {
                key: "maxUnits",
                label: "Maximum units per pass",
                kind: PluginSettingType::Number,
                default: DeclaredDefault::Number(0.0),
                min: Some(0.0),
                max: Some(4096.0),
                step: Some(1.0),
                description: Some(
                    "Cap on how many tool units ONE pass may withhold; 0 (the default) is unbounded. It exists for the first pass over a conversation restored weeks ago: unbounded, that single turn withholds every superseded unit at once and the provider's cached prefix diverges at the oldest trimmed position. Withholding is oldest-first, so a cap removes the oldest noise now and leaves the newest superseded units for a later turn.",
                ),
            },
        ],
    },
];

/// Every known plugin name, in catalogue order - the list the 404 sentence prints.
#[must_use]
pub fn plugin_names() -> Vec<&'static str> {
    BUILTIN_PLUGINS.iter().map(|entry| entry.name).collect()
}

/// Declared metadata for one plugin, `None` when this build does not ship it.
#[must_use]
pub fn metadata_of(plugin: &str) -> Option<&'static PluginMetadata> {
    BUILTIN_PLUGINS.iter().find(|entry| entry.name == plugin)
}

/// The declared settings of one plugin (empty when it declares none, or is unknown).
#[must_use]
pub fn settings_of(plugin: &str) -> &'static [PluginSettingDescriptor] {
    metadata_of(plugin).map_or(&[], |entry| entry.settings)
}

/// The DECLARED descriptor of one key, when the plugin declared it.
///
/// Deliberately declared-only: this is the lookup behind the row's `settings[]` and `values`
/// projection, which are both declared-only by contract. Everything that DECIDES about a key - the
/// patch allowlist, the snapshot order, the restore filter - goes through
/// [`effective_descriptor_of`] instead, because the reserved `enabled` key is writable on every
/// plugin.
#[must_use]
pub fn descriptor_of(plugin: &str, key: &str) -> Option<&'static PluginSettingDescriptor> {
    settings_of(plugin).iter().find(|entry| entry.key == key)
}

// ---------------------------------------------------------------------------
// The framework's reserved `enabled` key
// ---------------------------------------------------------------------------

/// The one setting key every plugin answers to whether or not it declares one
/// (`plugins.ts PLUGIN_ENABLED_KEY`), and the key the host's delivery gate consults.
///
/// The rule is one line: effective enablement = stored override ?? declared default ?? `true`.
/// Nothing else about a plugin changes - it stays loaded, its tools / agents / commands stay
/// registered, and `GET /plugins` still lists it with `enabled: false`; only hook delivery stops.
/// It is deliberately a normal key in the same override map rather than a second per-plugin boolean
/// column, so the durable document, the validation edge and the settings view all carry it with no
/// new storage shape.
pub const PLUGIN_ENABLED_KEY: &str = "enabled";

/// The synthesized descriptor for a plugin that does not declare the key itself
/// (`plugins.ts RESERVED_ENABLED_DESCRIPTOR`). Its `default: true` is the "a plugin nobody switched
/// off runs" answer the gate falls back to, and it is what lets `enabled` resolve through the SAME
/// stored -> declared -> fallback chain as every other key - no special-case read anywhere.
///
/// It never reaches the wire: a row's `settings[]` and `values` are declared-only
/// (`api.ts:2361-2362`), which is why the capture of `GET /plugins` shows no `enabled` entry in
/// async-ping's `values` even though that plugin is switchable.
pub static RESERVED_ENABLED_DESCRIPTOR: PluginSettingDescriptor = PluginSettingDescriptor {
    key: PLUGIN_ENABLED_KEY,
    label: "Enabled",
    kind: PluginSettingType::Boolean,
    default: DeclaredDefault::Flag(true),
    min: None,
    max: None,
    step: None,
    description: Some(
        "Framework switch: off stops this plugin's lifecycle hooks from running, in every session, from the next turn. The plugin stays loaded and listed; its tools and commands are unaffected.",
    ),
};

/// The descriptors a WRITE is validated against: the plugin's declared list plus the reserved
/// `enabled` key appended when the plugin does not declare it (`plugins.ts effectiveSettingsOf`).
///
/// That is the allowlist, so `PATCH ... {settings:{enabled:false}}` is accepted for EVERY plugin
/// while an invented key is still a 400 that names the whole accepted set. A plugin that declares
/// `enabled` itself (both context plugins do) keeps its OWN descriptor - and therefore its own label,
/// description and default - so the framework switch and the plugin's own read are one stored value.
///
/// An unknown plugin answers only the reserved descriptor, which is what keeps
/// [`PluginSettingsStore::enabled`] total (no read path throws; the 404 is the ROUTE's decision).
#[must_use]
pub fn effective_settings_of(plugin: &str) -> Vec<&'static PluginSettingDescriptor> {
    let declared = settings_of(plugin);

    if declared.iter().any(|entry| entry.key == PLUGIN_ENABLED_KEY) {
        return declared.iter().collect();
    }

    let mut effective: Vec<&'static PluginSettingDescriptor> = declared.iter().collect();

    effective.push(&RESERVED_ENABLED_DESCRIPTOR);

    effective
}

/// The effective descriptor of one key - declared, or the reserved `enabled` ([`effective_settings_of`]).
#[must_use]
pub fn effective_descriptor_of(
    plugin: &str,
    key: &str,
) -> Option<&'static PluginSettingDescriptor> {
    effective_settings_of(plugin)
        .into_iter()
        .find(|entry| entry.key == key)
}

// ---------------------------------------------------------------------------
// The value space, and the two spellings every value must pass through
// ---------------------------------------------------------------------------

/// One setting value, normalized to its DECLARED type.
///
/// The store never holds a raw `Value`: a numeric string is normalized to a number on write, which
/// is `plugins.ts`'s rule ("a stored value is always exactly the type the descriptor declares, and
/// therefore always JSON-serializable"). Keeping that invariant here is what lets `values`, the
/// 405-adjacent error sentences and `state.json` all agree on one spelling.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingValue {
    Number(f64),
    Text(String),
    Flag(bool),
}

impl SettingValue {
    /// The wire / `state.json` form.
    #[must_use]
    pub fn to_json(&self) -> Value {
        match self {
            Self::Number(value) => number_to_json(*value),
            Self::Text(text) => Value::String(text.clone()),
            Self::Flag(flag) => Value::Bool(*flag),
        }
    }

    /// The declared type this value belongs to.
    #[must_use]
    pub fn kind(&self) -> PluginSettingType {
        match self {
            Self::Number(_) => PluginSettingType::Number,
            Self::Text(_) => PluginSettingType::String,
            Self::Flag(_) => PluginSettingType::Boolean,
        }
    }
}

/// The exact integer form of an integral `f64`, or `None` when the value is fractional,
/// non-finite, or too large for a 64-bit integer to hold exactly.
///
/// Rust has no `TryFrom<f64>` for the integer types (a float-to-int `as` cast SATURATES, which is
/// the wrong answer for a value out of range), so the bound is stated explicitly: the largest
/// integer an `f64` represents exactly below `2^63` is `2^63 - 1`, and the test is therefore
/// strict on the upper end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Integer {
    Signed(i64),
    Unsigned(u64),
}

/// [`Integer::Signed`] / [`Integer::Unsigned`] split of one integral float.
///
/// The float-to-int `as` cast SATURATES, so every branch states its own bound and then verifies the
/// result by converting back. The bounds are written as literals rather than as `i64::MAX as f64`:
/// an `f64` cannot represent `2^63 - 1`, so `i64::MAX as f64` ROUNDS UP to `2^63` and a `<=` test
/// against it would wrongly admit `2^63` itself (which saturates on the cast).
#[must_use]
pub fn as_integer(value: f64) -> Option<Integer> {
    if value.fract() != 0.0 || !value.is_finite() {
        return None;
    }

    // Every integer in `[-2^63, 2^63)` is exactly representable and fits an `i64`.
    if (-9_223_372_036_854_775_808_f64..9_223_372_036_854_775_808_f64).contains(&value) {
        let signed = value as i64;

        if (signed as f64) == value {
            return Some(Integer::Signed(signed));
        }
    }

    // A value above `i64::MAX` - which a JSON document can legitimately carry - still prints exactly
    // as a `u64` up to `2^64`, and serde_json has that arm.
    if (0.0..18_446_744_073_709_551_616_f64).contains(&value) {
        let unsigned = value as u64;

        if (unsigned as f64) == value {
            return Some(Integer::Unsigned(unsigned));
        }
    }

    None
}

/// A JSON number that keeps an integral value in INTEGER spelling (`600000`, not `600000.0`),
/// because that is what `JSON.stringify` prints and what the contract's rows are pinned against.
///
/// A value with a fraction, or outside the range a 64-bit integer holds exactly, keeps the float
/// form - ECMAScript prints those with a fraction too.
#[must_use]
pub fn number_to_json(value: f64) -> Value {
    match as_integer(value) {
        Some(Integer::Signed(signed)) => Value::from(signed),
        Some(Integer::Unsigned(unsigned)) => Value::from(unsigned),
        // A NaN or an infinity cannot be stored: the persistence loader refuses a non-finite
        // number and the patch validator only accepts a finite one, so this arm is reached by a
        // huge or fractional float, which serde_json prints as a number.
        None => Value::from(value),
    }
}

/// ECMAScript's `String(number)` for the figures the contract quotes back
/// (`must be at least 1000; 999 was submitted.`). An integral value prints without a fraction,
/// exactly like the template literal TypeScript uses.
#[must_use]
pub fn format_number(value: f64) -> String {
    match as_integer(value) {
        Some(Integer::Signed(signed)) => signed.to_string(),
        Some(Integer::Unsigned(unsigned)) => unsigned.to_string(),
        None => value.to_string(),
    }
}

/// ECMAScript's `Number(text)` for the string forms a `number` setting legitimately accepts: a
/// query parameter or a hand-edited document carries `"150000"`, and the runtime's own read edge
/// accepts it too (`src/plugin/settings.ts asNumber`).
///
/// `None` is every text JS turns into `NaN`. A blank text is `None` here even though `Number("")`
/// is `0`, because `checkNumber` only coerces a text with a NON-blank trim and a blank one is the
/// "not a number" answer there.
#[must_use]
pub fn number_from_text(text: &str) -> Option<f64> {
    let trimmed = text.trim();

    if trimmed.is_empty() {
        return None;
    }

    // The sign is split off by hand, because Rust's own parser refuses `+0x10` the way `Number()`
    // does not, and `Number("-1e3")` is `-1000`.
    let (negative, rest) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };

    let magnitude = match radix_prefix(rest) {
        Some((radix, digits)) => u128::from_str_radix(digits, radix).ok()? as f64,
        None if rest.eq_ignore_ascii_case("infinity") => f64::INFINITY,
        None => {
            // `f64::from_str` also accepts `inf`/`NaN`, which `Number()` spells differently; both
            // are non-finite, and every caller rejects a non-finite value, so the two agree on
            // every text that is ACCEPTED.
            rest.parse::<f64>().ok()?
        }
    };

    Some(if negative { -magnitude } else { magnitude })
}

/// The `(radix, digits)` of a `0x` / `0o` / `0b` integer literal, `None` for a plain text.
fn radix_prefix(text: &str) -> Option<(u32, &str)> {
    let digits = text.get(2..)?;

    match text.get(..2)?.as_bytes() {
        [b'0', b'x' | b'X'] => Some((16, digits)),
        [b'0', b'o' | b'O'] => Some((8, digits)),
        [b'0', b'b' | b'B'] => Some((2, digits)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Per-type validation and normalization (the WRITE edge; see the header rules)
// ---------------------------------------------------------------------------

/// The contract's prefix of every per-value refusal: `"contextLimit" of plugin
/// "context-compaction"` (`plugins.ts labelOf`).
#[must_use]
fn where_of(plugin: &str, key: &str) -> String {
    format!("\"{key}\" of plugin \"{plugin}\"")
}

/// A `number` setting accepts a JSON number, or a numeric string the runtime would also accept,
/// and must land on a FINITE value inside `min`/`max`.
///
/// # Errors
///
/// [`ApiError::bad_request`] with the finite-number sentence, or the `min` / `max` sentence.
fn check_number(
    plugin: &str,
    descriptor: &PluginSettingDescriptor,
    value: &Value,
) -> Result<f64, ApiError> {
    let where_text = where_of(plugin, descriptor.key);

    let numeric = match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) if !text.trim().is_empty() => number_from_text(text),
        _ => None,
    };

    let Some(numeric) = numeric.filter(|value| value.is_finite()) else {
        return Err(ApiError::bad_request(format!(
            "{where_text} must be a finite number (it is declared \"number\")."
        )));
    };

    // `min` / `max` are the bounds the plugin PUBLISHED, so a value outside them is a value the
    // plugin never said it could handle. `step` is deliberately absent - see the descriptor.
    if let Some(min) = descriptor.min
        && numeric < min
    {
        return Err(ApiError::bad_request(format!(
            "{where_text} must be at least {}; {} was submitted.",
            format_number(min),
            format_number(numeric)
        )));
    }

    if let Some(max) = descriptor.max
        && numeric > max
    {
        return Err(ApiError::bad_request(format!(
            "{where_text} must be at most {}; {} was submitted.",
            format_number(max),
            format_number(numeric)
        )));
    }

    Ok(numeric)
}

/// A `string` setting accepts a string ONLY. Unlike the read path, a write must say what it
/// means, so `settings: true` for a text field is more likely a client bug than a value.
///
/// # Errors
///
/// [`ApiError::bad_request`] with the string sentence.
fn check_string(
    plugin: &str,
    descriptor: &PluginSettingDescriptor,
    value: &Value,
) -> Result<String, ApiError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        _ => Err(ApiError::bad_request(format!(
            "{} must be a string (it is declared \"string\").",
            where_of(plugin, descriptor.key)
        ))),
    }
}

/// A `boolean` setting accepts a JSON boolean, or the two words the runtime also parses - trimmed
/// and lower-cased, because a query parameter legitimately carries `"TRUE"`.
///
/// # Errors
///
/// [`ApiError::bad_request`] with the boolean sentence.
fn check_boolean(
    plugin: &str,
    descriptor: &PluginSettingDescriptor,
    value: &Value,
) -> Result<bool, ApiError> {
    match value {
        Value::Bool(flag) => Ok(*flag),
        // The two words the runtime's own `asBoolean` accepts, normalized the same way.
        Value::String(text) if matches!(text.trim().to_lowercase().as_str(), "true" | "false") => {
            Ok(text.trim().to_lowercase() == "true")
        }
        _ => Err(ApiError::bad_request(format!(
            "{} must be true or false (it is declared \"boolean\").",
            where_of(plugin, descriptor.key)
        ))),
    }
}

/// Validate one submitted value against its descriptor and normalize it to the declared type.
///
/// # Errors
///
/// [`ApiError::bad_request`] carrying the declared type's sentence.
fn check_value(
    plugin: &str,
    descriptor: &PluginSettingDescriptor,
    value: &Value,
) -> Result<SettingValue, ApiError> {
    match descriptor.kind {
        PluginSettingType::Number => {
            check_number(plugin, descriptor, value).map(SettingValue::Number)
        }
        PluginSettingType::Boolean => {
            check_boolean(plugin, descriptor, value).map(SettingValue::Flag)
        }
        PluginSettingType::String => {
            check_string(plugin, descriptor, value).map(SettingValue::Text)
        }
    }
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

/// The operator's per-plugin setting OVERRIDES, plus the counter every plugin page quotes.
///
/// `Default` is a fresh deployment: nothing stored, revision 0, and every key answering its
/// declared default.
#[derive(Debug, Clone, Default)]
pub struct PluginSettingsStore {
    /// plugin name -> declared key -> stored override.
    ///
    /// ONLY operator-changed values live here, so `state.json` carries a DELTA and a release that
    /// moves a declared default needs no migration (`plugins.ts` "DEFAULTS BELONG TO THE PLUGIN,
    /// OVERRIDES TO THE HOST"). A `BTreeMap` because identity order is irrelevant here - every
    /// wire-facing iteration runs off the CATALOGUE's declared order, never off this map.
    overrides: BTreeMap<String, BTreeMap<String, SettingValue>>,
    /// One bump per ACCEPTED patch. Never persisted: a restart is not a write, so the figure
    /// returns to 0 while the stored values do not (`plugins-api.test.ts`: "the revision counter is
    /// epoch-local, so it restarts at 0 while the VALUE does not").
    revision: u64,
}

impl PluginSettingsStore {
    /// An untouched store: nothing overridden, revision 0.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The store a boot builds, seeded from the loaded `state.json` (`runtime.ts` reads the
    /// document and the store forwards `database.pluginSettings` here).
    #[must_use]
    pub fn seeded(values: Option<&HarnessPluginSettingsRecord>) -> Self {
        let mut store = Self::default();

        store.restore(values);

        store
    }

    /// Reads that mutate: one bump per accepted patch, so a page's `revision` is a staleness test.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// The raw STORED override of one key - `None` when the operator never changed it, which is the
    /// provider's documented "nothing stored" signal.
    #[must_use]
    pub fn get(&self, plugin: &str, key: &str) -> Option<SettingValue> {
        self.overrides.get(plugin)?.get(key).cloned()
    }

    /// Whether one plugin RUNS right now: the stored `enabled` override, otherwise the plugin's
    /// declared `enabled` default, otherwise `true` (`plugins.ts enabled()` - and the exact value
    /// the host's delivery gate consults, so a row and a live session can never disagree).
    ///
    /// Resolved through the EFFECTIVE descriptor, which is what makes an undeclaring plugin
    /// (async-ping) switchable while its `values` map stays declared-only.
    ///
    /// A stored value of the WRONG kind cannot exist here: the patch validator and the restore filter
    /// both normalize to the declared type, so anything else in the map is treated as "nothing
    /// stored" and the default applies rather than panicking on a mismatch.
    #[must_use]
    pub fn enabled(&self, plugin: &str) -> bool {
        if let Some(SettingValue::Flag(flag)) = self.get(plugin, PLUGIN_ENABLED_KEY) {
            return flag;
        }

        // The effective descriptor answers the plugin's OWN default when it declares the key, and the
        // reserved `true` otherwise; an unknown plugin has no descriptors besides the reserved one, so
        // it answers `true` and stays total. A non-boolean default would be a catalogue bug, and the
        // fallback side of the chain (`?? true`) is the documented answer for it.
        effective_descriptor_of(plugin, PLUGIN_ENABLED_KEY)
            .filter(|descriptor| descriptor.kind == PluginSettingType::Boolean)
            .map_or(true, |descriptor| match descriptor.default {
                DeclaredDefault::Flag(flag) => flag,
                // Declared `boolean`, defaulted to something else: the framework's own fallback.
                _ => true,
            })
    }

    /// The EFFECTIVE map a plugin would read right now, in DECLARED key order: the stored override
    /// when there is one, the declared default otherwise. This is the projection the UI paints, so
    /// a row can never disagree with runtime behaviour.
    ///
    /// Deliberately DECLARED-ONLY (`plugins.ts valuesOf`): `values` is the projection of the plugin's
    /// own form fields, and the contract states a key absent from `settings[]` can never appear here.
    /// The reserved `enabled` therefore travels in its own row field ([`PluginSettingsStore::enabled`])
    /// rather than in this map - which is why the reference capture of async-ping (a plugin that does
    /// NOT declare `enabled`) has exactly one `values` entry.
    ///
    /// The resolution is equivalent to the plugin's own settings view (`src/plugin/settings.ts`:
    /// stored -> declared -> fallback) because every stored value here was normalized to its declared
    /// type on the way in: the view's `number`/`boolean` coercions can never fire on a value this
    /// store holds, so the third link of that chain is unreachable from a row.
    ///
    /// An unknown plugin answers an empty map, exactly like `plugins.ts values` - it has no
    /// declared settings to resolve.
    #[must_use]
    pub fn values(&self, plugin: &str) -> Map<String, Value> {
        let mut values = Map::new();

        for descriptor in settings_of(plugin) {
            let stored = self
                .overrides
                .get(plugin)
                .and_then(|entries| entries.get(descriptor.key))
                .cloned();

            let effective = stored.unwrap_or_else(|| descriptor.default.to_stored());

            // A declared key is unique, so an insert never overwrites and the map stays in the
            // descriptor's order - the order the pane renders its form in.
            values.insert(descriptor.key.to_owned(), effective.to_json());
        }

        values
    }

    /// One row of the catalogue: the declared metadata beside the values in force NOW
    /// (`api.ts pluginRowOf`). `None` for a plugin this build does not ship.
    #[must_use]
    pub fn row_of(&self, plugin: &str) -> Option<contract::PluginSummary> {
        let metadata = metadata_of(plugin)?;

        Some(contract::PluginSummary {
            name: metadata.name.to_owned(),
            title: metadata.title.to_owned(),
            // An absent description / version / tags / hook list is OMITTED on the wire, never
            // `null` and never `[]`: `pluginRowOf` filters on presence, so a row that lost one is a
            // catalogue bug the absence makes visible instead of hiding.
            description: non_blank(Some(metadata.description)),
            version: non_blank(Some(metadata.version)),
            tags: (!metadata.tags.is_empty())
                .then(|| metadata.tags.iter().map(|tag| (*tag).to_owned()).collect()),
            hooks: (!metadata.hooks.is_empty()).then(|| {
                metadata
                    .hooks
                    .iter()
                    .map(|hook| (*hook).to_owned())
                    .collect()
            }),
            // Every row on this route is auto-scanned in the TypeScript build and compiled in here,
            // so `builtin` is `true`; the field stays explicit (never inferred client-side) for a
            // plugin a later release makes configurable.
            builtin: true,
            // R2 - read live from the same store the host's gate consults, so the row and the runtime
            // answer with one voice (`api.ts:2360`). A plugin that declares no `enabled` setting still
            // has one (the reserved key), while its `values` map stays declared-only.
            enabled: self.enabled(metadata.name),
            settings: settings_of(metadata.name)
                .iter()
                .map(plugin_setting_of)
                .collect(),
            values: self.values(metadata.name),
        })
    }

    /// The whole catalogue, in scan order - the collection `GET /plugins` pages over.
    #[must_use]
    pub fn rows(&self) -> Vec<contract::PluginSummary> {
        BUILTIN_PLUGINS
            .iter()
            .filter_map(|entry| self.row_of(entry.name))
            .collect()
    }

    /// Only what the operator changed, in CATALOGUE order and EFFECTIVE-DESCRIPTOR key order - the
    /// `state.json` projection (`plugins.ts snapshot` / `snapshotOf`).
    ///
    /// A plugin with no remaining override keeps no entry, so a document that does not name a
    /// plugin means exactly "nothing has ever been changed about it".
    #[must_use]
    pub fn snapshot(&self) -> HarnessPluginSettingsRecord {
        let mut record: HarnessPluginSettingsRecord = Map::new();

        for metadata in BUILTIN_PLUGINS.iter() {
            let Some(entries) = self.overrides.get(metadata.name) else {
                continue;
            };

            let mut values: Map<String, Value> = Map::new();

            // EFFECTIVE-DESCRIPTOR order, so a document stays DIFF-STABLE even when an older build
            // wrote the keys in another order (`plugins.ts snapshotOf`). The reserved `enabled` key is
            // part of that order, which is what persists a switch flipped on a plugin that does NOT
            // declare it - and a declared `enabled` keeps the position the plugin gave it.
            for descriptor in effective_settings_of(metadata.name) {
                if let Some(value) = entries.get(descriptor.key) {
                    values.insert(descriptor.key.to_owned(), value.to_json());
                }
            }

            if !values.is_empty() {
                // `Map<String, Value>` is the record's shape (see the type's note), so a plugin's
                // entry is the OBJECT of its overrides.
                record.insert(metadata.name.to_owned(), Value::Object(values));
            }
        }

        record
    }

    /// Replace every override with a restored document. No revision bump: a boot is not a write.
    ///
    /// LENIENT ABOUT IDENTITY: the loader has already refused a document whose SHAPE is wrong
    /// ([`crate::persistence`]'s `pluginSettings` pass), so what arrives here is a map of JSON
    /// scalars. What it may still name is a plugin or a key THIS build no longer declares, or a
    /// value that violates a bound the plugin has since tightened - and none of those may abort a
    /// server boot, so each is simply not restored (`plugins.ts applyRestore`).
    pub fn restore(&mut self, values: Option<&HarnessPluginSettingsRecord>) {
        self.overrides.clear();

        let Some(values) = values else {
            return;
        };

        for (plugin, raw) in values {
            // A plugin this build stopped shipping: drop the whole entry. The identity check is the
            // only use of the metadata here - the descriptor walk below runs off the EFFECTIVE list.
            if metadata_of(plugin).is_none() {
                continue;
            }

            // The loader refuses a member that is not an object (`persistence::validate_document`),
            // so reaching here without one means a caller handed this store an UNvalidated document -
            // a foreign root or a test fixture - and the entry is skipped rather than trusted.
            let Some(entries) = raw.as_object() else {
                continue;
            };

            // The EFFECTIVE set is the allowlist here too, so an `enabled` override saved for a
            // plugin that never declares the key survives the restart exactly like a declared one
            // (`plugins.ts applyRestore` - R2's durability criterion).
            for descriptor in effective_settings_of(plugin) {
                let Some(value) = entries.get(descriptor.key) else {
                    continue;
                };

                // A stored value that fails the descriptor is dropped, not reported: the document is
                // operator-visible data from an older build, and one stale key must never hide every
                // session (`normalizeForRestore`).
                let Ok(normalized) = check_value(plugin, descriptor, value) else {
                    continue;
                };

                self.put(plugin, descriptor.key, normalized);
            }
        }
    }

    /// Validate and apply ONE plugin's patch, bumping the counter on success.
    ///
    /// ALL-OR-NOTHING: every entry is validated BEFORE any of them is written, so one bad value
    /// cannot leave the others applied - the same rule `PATCH /config` follows. A refusal mutates
    /// nothing and moves no counter.
    ///
    /// Durability is the OWNER's decision, not this one: the caller writes the document that
    /// carries the change before it answers (`Store::update_plugin_settings`).
    ///
    /// # Errors
    ///
    /// [`ApiError::not_found`] `Unknown plugin "<id>". This build ships: async-ping,
    /// context-compaction, context-trimmer.`, and [`ApiError::bad_request`] for an empty patch, an
    /// unknown key, or an out-of-bounds / mistyped value.
    pub fn update(&mut self, plugin: &str, patch: &Map<String, Value>) -> Result<(), ApiError> {
        // The plugin check comes FIRST: `requirePlugin` throws the 404 before the patch is even
        // looked at, so a wrong id with an empty body is a 404, not a 400.
        if metadata_of(plugin).is_none() {
            let known = plugin_names().join(", ");

            return Err(ApiError::not_found(format!(
                "Unknown plugin \"{plugin}\". This build ships: {}.",
                if known.is_empty() { "none" } else { &known }
            )));
        }

        if patch.is_empty() {
            return Err(ApiError::bad_request(
                "A plugin settings patch must change at least one setting.",
            ));
        }

        // The EFFECTIVE list is the allowlist: the plugin's declared keys plus the reserved
        // `enabled`, so the operator's switch works on EVERY plugin (R2) while an invented key is
        // still a 400 that names everything accepted (`plugins.ts update`).
        let allowed_descriptors = effective_settings_of(plugin);

        // Validate in SUBMISSION order (`serde_json`'s `preserve_order` keeps the client's own key
        // order, so the first refusal names the first field the operator got wrong).
        let mut prepared: Vec<(&'static str, Option<SettingValue>)> =
            Vec::with_capacity(patch.len());

        for (key, submitted) in patch {
            let Some(descriptor) = effective_descriptor_of(plugin, key) else {
                let allowed: Vec<&str> =
                    allowed_descriptors.iter().map(|entry| entry.key).collect();

                return Err(ApiError::bad_request(format!(
                    "Unknown setting \"{key}\" for plugin \"{plugin}\". Allowed settings: {}.",
                    if allowed.is_empty() {
                        "none (this plugin declares no settings)".to_owned()
                    } else {
                        allowed.join(", ")
                    }
                )));
            };

            // `null` is the documented RESET: drop the override so the declared default applies
            // again - the same `null`-means-reset rule as `PATCH /config`.
            let value = if submitted.is_null() {
                None
            } else {
                Some(check_value(plugin, descriptor, submitted)?)
            };

            prepared.push((descriptor.key, value));
        }

        // Nothing below can fail, so the patch is atomic from here.
        for (key, value) in prepared {
            if let Some(value) = value {
                self.put(plugin, key, value);
            } else {
                self.delete(plugin, key);
            }
        }

        // A plugin with no remaining override keeps no map, so a snapshot stays minimal.
        if self.overrides.get(plugin).is_none_or(BTreeMap::is_empty) {
            self.overrides.remove(plugin);
        }

        self.revision += 1;

        Ok(())
    }

    /// Record one override, creating the plugin's map on first write.
    fn put(&mut self, plugin: &str, key: &str, value: SettingValue) {
        self.overrides
            .entry(plugin.to_owned())
            .or_default()
            .insert(key.to_owned(), value);
    }

    /// Drop one override - the `null` reset.
    fn delete(&mut self, plugin: &str, key: &str) {
        if let Some(entries) = self.overrides.get_mut(plugin) {
            entries.remove(key);
        }
    }
}

/// One declared descriptor -> one wire `PluginSetting` (`api.ts pluginSettingOf`).
///
/// The spellings already match - the contract copied them from the descriptor - so this is a
/// presence filter, not a rename: an unset optional stays ABSENT rather than becoming `null`.
#[must_use]
pub fn plugin_setting_of(entry: &PluginSettingDescriptor) -> contract::PluginSetting {
    contract::PluginSetting {
        key: entry.key.to_owned(),
        label: entry.label.to_owned(),
        kind: entry.kind,
        default: entry.default.to_json(),
        min: entry.min.map(number_to_json),
        max: entry.max.map(number_to_json),
        step: entry.step.map(number_to_json),
        description: non_blank(entry.description),
    }
}

/// The contract's "absent, never empty" rule for one optional declared string.
#[must_use]
fn non_blank(text: Option<&'static str>) -> Option<String> {
    text.filter(|text| !text.is_empty()).map(str::to_owned)
}

// ---------------------------------------------------------------------------
// Unit tests: the write-edge rules the SHIPPED catalogue cannot reach
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A descriptor with every numeric bound set, because neither shipped plugin declares a `max`.
    fn bounded(
        key: &'static str,
        kind: PluginSettingType,
        default: DeclaredDefault,
        min: Option<f64>,
        max: Option<f64>,
        step: Option<f64>,
    ) -> PluginSettingDescriptor {
        PluginSettingDescriptor {
            key,
            label: "Probe",
            kind,
            default,
            min,
            max,
            step,
            description: None,
        }
    }

    /// 1. The number spellings. An integral value prints as an INTEGER, which is what both the wire
    ///    and `state.json` are pinned against, and what the error sentences quote back.
    #[test]
    fn an_integral_number_keeps_integer_spelling() {
        assert_eq!(number_to_json(600_000.0), json_int(600_000));
        assert_eq!(number_to_json(-5.0), json_int(-5));
        assert_eq!(number_to_json(0.0), json_int(0));
        assert_eq!(number_to_json(1_500.5), Value::from(1_500.5));

        // Past `i64`, a `u64` still prints exactly; past `u64`, the float does.
        assert_eq!(
            number_to_json(9_223_372_036_854_775_808.0),
            Value::from(9_223_372_036_854_775_808_u64)
        );

        assert_eq!(format_number(1_000.0), "1000");
        assert_eq!(format_number(999.0), "999");
        assert_eq!(format_number(1_500.5), "1500.5");
    }

    fn json_int(value: i64) -> Value {
        Value::from(value)
    }

    /// 2. The numeric STRING forms `checkNumber` accepts, and the texts that stay `NaN`.
    #[test]
    fn a_numeric_string_is_coerced_the_way_number_is() {
        assert_eq!(number_from_text("150000"), Some(150_000.0));
        assert_eq!(number_from_text("  42  "), Some(42.0));
        assert_eq!(number_from_text("+7"), Some(7.0));
        assert_eq!(number_from_text("-1e3"), Some(-1_000.0));
        assert_eq!(number_from_text("0x10"), Some(16.0));
        assert_eq!(number_from_text("0o17"), Some(15.0));
        assert_eq!(number_from_text("0b101"), Some(5.0));

        // `Number("")` is `0` in JS, but `checkNumber` only coerces a NON-blank trim.
        assert_eq!(number_from_text(""), None);
        assert_eq!(number_from_text("   "), None);
        assert_eq!(number_from_text("plenty"), None);
        assert_eq!(number_from_text("1.2.3"), None);
        // An overflow is `Infinity`, which every caller then refuses as non-finite.
        assert_eq!(number_from_text("1e400"), Some(f64::INFINITY));
    }

    /// 3. The declared type decides the sentence, and a mismatch is refused rather than coerced.
    ///    These two branches are unreachable through the shipped catalogue (both built-ins declare a
    ///    `number`), so they are pinned at the descriptor directly.
    #[test]
    fn a_string_setting_accepts_only_a_string() {
        let text = bounded(
            "note",
            PluginSettingType::String,
            DeclaredDefault::Text(""),
            None,
            None,
            None,
        );

        assert_eq!(
            check_value("probe", &text, &Value::String("hello".into())),
            Ok(SettingValue::Text("hello".into()))
        );
        // No coercion: a number, a boolean and an object are all the same refusal.
        for rejected in [Value::from(1), Value::Bool(true), Value::Array(vec![])] {
            let error =
                check_value("probe", &text, &rejected).expect_err("a non-string is refused");

            assert_eq!(error.status, 400);
            assert_eq!(
                error.message,
                "\"note\" of plugin \"probe\" must be a string (it is declared \"string\")."
            );
        }
    }

    /// 4. A boolean setting takes a JSON boolean or the two words the runtime also parses - trimmed
    ///    and lower-cased - and nothing else.
    #[test]
    fn a_boolean_setting_takes_a_bool_or_the_two_words() {
        let flag = bounded(
            "enabled",
            PluginSettingType::Boolean,
            DeclaredDefault::Flag(true),
            None,
            None,
            None,
        );

        assert_eq!(
            check_value("probe", &flag, &Value::Bool(false)),
            Ok(SettingValue::Flag(false))
        );
        for accepted in ["true", "TRUE", " true ", "False"] {
            let expected = accepted.trim().eq_ignore_ascii_case("true");

            assert_eq!(
                check_value("probe", &flag, &Value::String(accepted.into())),
                Ok(SettingValue::Flag(expected)),
                "`{accepted}` is one of the two words"
            );
        }

        for rejected in [Value::from(0), Value::String("yes".into()), Value::Null] {
            let error =
                check_value("probe", &flag, &rejected).expect_err("a non-boolean is refused");

            assert_eq!(
                error.message,
                "\"enabled\" of plugin \"probe\" must be true or false (it is declared \"boolean\")."
            );
        }
    }

    /// 5. `max` is enforced with its own sentence; `step` NEVER is, because it is an editor hint and
    ///    refusing a value the runtime can live with would strand a hand-edited document.
    #[test]
    fn bounds_are_enforced_and_the_step_is_not() {
        let bounded = bounded(
            "level",
            PluginSettingType::Number,
            DeclaredDefault::Number(10.0),
            Some(5.0),
            Some(20.0),
            Some(5.0),
        );
        let plugin = "probe";

        assert_eq!(
            check_number(plugin, &bounded, &Value::from(4)),
            Err(ApiError::bad_request(
                "\"level\" of plugin \"probe\" must be at least 5; 4 was submitted."
            ))
        );
        assert_eq!(
            check_number(plugin, &bounded, &Value::from(21)),
            Err(ApiError::bad_request(
                "\"level\" of plugin \"probe\" must be at most 20; 21 was submitted."
            ))
        );

        // `7` is inside the bounds and off the step: ACCEPTED.
        assert_eq!(check_number(plugin, &bounded, &Value::from(7)), Ok(7.0));
        // Both bounds are INCLUSIVE.
        assert_eq!(check_number(plugin, &bounded, &Value::from(5)), Ok(5.0));
        assert_eq!(check_number(plugin, &bounded, &Value::from(20)), Ok(20.0));
    }

    /// 6. A rejected patch is ALL-OR-NOTHING: the valid key in the same document applied nothing, and
    ///    the counter did not move.
    #[test]
    fn a_rejected_patch_leaves_the_store_untouched() {
        let mut store = PluginSettingsStore::new();

        let rejected = store.update(
            "context-compaction",
            &object(json!({ "contextLimit": 5_000, "nope": 1 })),
        );

        assert_eq!(
            rejected
                .expect_err("the unknown key refuses the patch")
                .status,
            400
        );
        assert_eq!(
            store.values("context-compaction"),
            object(json!({ "contextLimit": 200_000 })),
            "the valid half did not land"
        );
        assert_eq!(store.revision(), 0, "and a refusal is not a write");
        assert!(store.snapshot().is_empty());

        // The unknown-plugin refusal comes FIRST, before any of that.
        let unknown = store
            .update("retired", &object(json!({ "anything": 1 })))
            .expect_err("an unknown plugin is a 404");

        assert_eq!(unknown.status, 404);
        assert_eq!(
            unknown.message,
            "Unknown plugin \"retired\". This build ships: async-ping, context-compaction, context-trimmer."
        );
    }

    /// 7. A `null` resets ONE key, and a plugin whose overrides are all gone keeps no entry at all,
    ///    so `state.json` carries neither an empty object nor a `null`.
    #[test]
    fn a_reset_drops_the_override_and_an_empty_plugin_disappears() {
        let mut store = PluginSettingsStore::new();

        store
            .update("async-ping", &object(json!({ "intervalMs": 5_000 })))
            .expect("5000 is in bounds");
        assert_eq!(
            store.snapshot(),
            object(json!({ "async-ping": { "intervalMs": 5_000 } }))
        );

        store
            .update("async-ping", &object(json!({ "intervalMs": Value::Null })))
            .expect("a reset is accepted");

        assert!(store.get("async-ping", "intervalMs").is_none());
        assert_eq!(
            store.values("async-ping"),
            object(json!({ "intervalMs": 600_000 })),
            "the declared default applies again"
        );
        assert!(
            store.snapshot().is_empty(),
            "and the now-empty plugin map is dropped, not written as an empty object"
        );
        assert_eq!(store.revision(), 2, "both writes counted");
    }

    /// 8. The snapshot is CATALOGUE order with EFFECTIVE-DESCRIPTOR keys, whatever order the writes
    ///    arrived in - the property that keeps a saved document diff-stable.
    #[test]
    fn the_snapshot_follows_the_catalogue_not_the_submission() {
        let mut store = PluginSettingsStore::new();

        store
            .update(
                "context-compaction",
                &object(json!({ "contextLimit": 2_000 })),
            )
            .expect("in bounds");
        store
            .update("async-ping", &object(json!({ "intervalMs": 3_000 })))
            .expect("in bounds");

        let snapshot = store.snapshot();
        let keys: Vec<&String> = snapshot.keys().collect();

        assert_eq!(keys, vec!["async-ping", "context-compaction"]);
    }

    /// 9. Restore is LENIENT about identity and about usability: a retired plugin, a retired key, and
    ///    an out-of-bounds value are each dropped rather than aborting the boot.
    #[test]
    fn a_restore_drops_what_this_build_cannot_use() {
        let document = object(json!({
            "retired-plugin": { "anything": 1 },
            "async-ping": { "intervalMs": 9_000, "gone": "x" },
            "context-compaction": { "contextLimit": 1 }
        }));

        let store = PluginSettingsStore::seeded(Some(&document));

        assert_eq!(
            store.values("async-ping"),
            object(json!({ "intervalMs": 9_000 }))
        );
        assert_eq!(
            store.values("context-compaction"),
            object(json!({ "contextLimit": 200_000 })),
            "1 is below the plugin's own `min`, so the override is not restored"
        );
        assert_eq!(
            store.snapshot(),
            object(json!({ "async-ping": { "intervalMs": 9_000 } }))
        );
        assert_eq!(
            store.revision(),
            0,
            "restoring is a boot, not a write, so the counter stays at 0"
        );

        // A restore replaces everything: a second call with nothing yields an empty store.
        let mut cleared = store;

        cleared.restore(None);

        assert!(cleared.snapshot().is_empty());
    }

    /// 10. `values` resolves declared defaults, so an untouched deployment already answers a complete
    ///     row - and an unknown plugin has nothing to resolve. The maps below are byte-equal to the
    ///     `values` member of each row in the reference capture.
    #[test]
    fn values_answer_the_declared_defaults_for_every_known_plugin() {
        let store = PluginSettingsStore::new();

        assert_eq!(
            store.values("async-ping"),
            object(json!({ "intervalMs": 600_000 }))
        );
        assert_eq!(
            store.values("context-compaction"),
            object(json!({
                "contextLimit": 200_000,
                "modelLimits": "{}",
                "enabled": true,
                "minTokens": 200,
                "safetyTurns": 6,
                "mode": "summarize"
            }))
        );
        assert_eq!(
            store.values("context-trimmer"),
            object(json!({
                "enabled": true,
                "dropSupersededToolCalls": true,
                "dropInvalidToolCallsOnceAnswered": true,
                "invalidOutcomes": "failed,rejected",
                "dropOldThoughts": "turn",
                "thoughtKeepUnits": 0,
                "supersedeMatch": "name",
                "dropDuplicateCalls": false,
                "minTokens": 0,
                "minThoughtChars": 0,
                "safetyUnits": 0,
                "maxUnits": 0
            }))
        );
        assert!(store.values("nobody").is_empty());

        // The catalogue is the three shipped built-ins, in scan order.
        assert_eq!(
            plugin_names(),
            vec!["async-ping", "context-compaction", "context-trimmer"]
        );
        assert_eq!(
            store
                .rows()
                .iter()
                .map(|row| row.name.as_str())
                .collect::<Vec<_>>(),
            vec!["async-ping", "context-compaction", "context-trimmer"]
        );

        // Every row states `builtin` AND `enabled`, and carries its full declared descriptor beside the
        // value: the projection (`pluginSettingOf`) may not drop or respell a declared bound.
        for row in store.rows() {
            assert!(row.builtin);
            assert!(row.enabled, "an untouched deployment runs every built-in");

            let declared = settings_of(&row.name);

            assert_eq!(row.settings.len(), declared.len());
            assert_eq!(row.values.len(), declared.len());

            for setting in &row.settings {
                let entry = descriptor_of(&row.name, &setting.key).expect("the key is declared");

                assert_eq!(setting.default, entry.default.to_json());
                assert_eq!(setting.min, entry.min.map(number_to_json));
                assert_eq!(setting.max, entry.max.map(number_to_json));
                assert_eq!(setting.step, entry.step.map(number_to_json));
                assert_eq!(setting.kind, entry.kind);
                assert_eq!(setting.label, entry.label);
                assert_eq!(setting.description, entry.description.map(str::to_owned));
            }
        }
    }

    /// 11. The reserved `enabled` key switches EVERY plugin - including the one that never declares it
    ///     - through the same write path, the same durable document and the same reset as a declared
    ///     key, and never leaks into the row's `settings[]` or `values`.
    #[test]
    fn the_reserved_enabled_key_switches_a_plugin_that_never_declared_it() {
        let mut store = PluginSettingsStore::new();

        // async-ping declares ONE key; its effective allowlist has TWO, with the reserved one last.
        assert_eq!(
            effective_settings_of("async-ping")
                .iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec!["intervalMs", "enabled"]
        );
        // context-compaction declares its OWN `enabled`, so its descriptor wins - position and label
        // included - and the framework switch and the plugin's own read are one stored value.
        assert_eq!(
            effective_settings_of("context-compaction")
                .iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec![
                "contextLimit",
                "modelLimits",
                "enabled",
                "minTokens",
                "safetyTurns",
                "mode"
            ]
        );
        assert_eq!(
            effective_descriptor_of("context-compaction", PLUGIN_ENABLED_KEY)
                .expect("declared")
                .label,
            "Compact conversations"
        );
        assert_eq!(
            effective_descriptor_of("async-ping", PLUGIN_ENABLED_KEY)
                .expect("reserved")
                .label,
            RESERVED_ENABLED_DESCRIPTOR.label
        );

        // The word `"false"` is one of the two the runtime parses, so the switch writes a real boolean.
        store
            .update("async-ping", &object(json!({ "enabled": "false" })))
            .expect("the reserved key is allowed on every plugin");

        assert_eq!(
            store.get("async-ping", PLUGIN_ENABLED_KEY),
            Some(SettingValue::Flag(false))
        );
        assert!(!store.enabled("async-ping"));
        assert!(!store.row_of("async-ping").expect("known").enabled);
        assert_eq!(
            store.snapshot(),
            object(json!({ "async-ping": { "enabled": false } })),
            "a switch flipped on an undeclaring plugin is still persisted"
        );
        assert!(
            !store.values("async-ping").contains_key(PLUGIN_ENABLED_KEY),
            "`values` stays declared-only (`api.ts:2362`), so the switch travels in its own row field"
        );

        // It survives a restart: the effective set is the restore allowlist too.
        let restored = PluginSettingsStore::seeded(Some(&store.snapshot()));

        assert!(!restored.enabled("async-ping"));
        assert_eq!(restored.revision(), 0, "a boot is not a write");

        // A declared default of `false` would be respected the same way - the fallback chain is
        // stored -> declared -> `true`, and the reserved descriptor is only the THIRD link.
        store
            .update("context-trimmer", &object(json!({ "enabled": false })))
            .expect("a declaring plugin takes the same write");

        assert!(!store.enabled("context-trimmer"));

        // `null` resets it exactly like a declared key, and the plugin disappears from the document.
        store
            .update("async-ping", &object(json!({ "enabled": Value::Null })))
            .expect("a reset is accepted");

        assert!(store.enabled("async-ping"));
        assert!(
            store.snapshot().is_empty(),
            "the only override was `enabled`"
        );

        // An unknown plugin still answers the TOTAL `true` (the 404 is the route's decision), and an
        // unknown key still names the EFFECTIVE set in its refusal.
        assert!(store.enabled("retired"));

        let refused = store
            .update("async-ping", &object(json!({ "nope": 1 })))
            .expect_err("an invented key is still a 400");

        assert_eq!(
            refused.message,
            "Unknown setting \"nope\" for plugin \"async-ping\". Allowed settings: intervalMs, enabled."
        );
    }

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().expect("the fixture is an object").clone()
    }
}
