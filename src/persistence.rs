//! The durable harness database (`<root>/harness/state.json` + `<root>/harness/agents.json`).
//!
//! TypeScript origin: `packages/underload/harness/src/server/persistence.ts`. That file is the
//! schema of record: every struct below mirrors one of its exported types, field for field, in
//! the order it WRITES them, because a Rust-written document must stay readable by the TS
//! server and vice versa.
//!
//! WHAT IS PERSISTED (`persistence.ts:3-36`)
//! -----------------------------------------
//! One versioned JSON document: `registry` (the metadata of every harness entry), `deleted`
//! (the tombstones of shipped built-ins the operator removed), `globalSettings` (the operator's
//! provider defaults WITHOUT the credential), and `sessions` (identity, the frozen preset and
//! descriptor the session was created with, provider settings without the API key, timestamps,
//! every turn, the queue order, and the last committed core snapshot).
//!
//! SECRETS ARE NEVER WRITTEN (`:20-26`)
//! ------------------------------------
//! `HarnessSessionSettingsRecord` and `HarnessGlobalSettingsRecord` hold `baseURL`/`model` and
//! nothing else BY CONSTRUCTION - there is no key field to set. A document that carries one is
//! refused rather than quietly stripped, mirroring `:889-893` and `:1186-1188`: silently
//! dropping a foreign writer's credential would hide the defect that put it there.
//!
//! ATOMIC WRITE (`:28-35`, `:1429-1511`)
//! -------------------------------------
//! The whole document is one commit unit: write the full JSON to a sibling staging file
//! (`state.json.<owner>.<seq>.tmp`), fsync it, then rename over `state.json` in the SAME
//! directory. The canonical file is never unlinked first, so a crash leaves either the old
//! complete document or the new one. Writes are synchronous, which also serializes them.
//! Windows adds a second problem the TS writer never sees: a rename onto a file another handle
//! still holds fails with `ERROR_SHARING_VIOLATION`, so the commit retries a bounded number of
//! times and always removes its staging file.
//!
//! FAILURE POLICY (`:63-69`)
//! -------------------------
//! An unknown version, malformed JSON, or a structurally invalid document is an error naming
//! the file. Nothing is reset or deleted: a silent clean slate would let a corrupt file
//! masquerade as "no sessions".
//!
//! TOLERANCE (`persistence.ts` has none of this explicitly, but every reader there defaults an
//! absent optional)
//! -------------------------------------------------------------------------------------------
//! Every record carries a `#[serde(flatten)] extra` catch-all and `#[serde(default)]` on every
//! optional/collection field, and NO record uses `deny_unknown_fields`. That is what lets a
//! document written by a newer TS build round-trip through this server without losing the
//! fields this build does not model. Field-level REASONS for a bad shape come from
//! [`validate_document`] (which walks the raw document and prints the TS sentence), never from
//! serde, so an operator sees the same diagnostic on both servers.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::contract::{self, AgentResultReference, HarnessAttachment, HarnessEvent, HarnessUsage};
use crate::error::ApiError;
use crate::events;
use crate::labels;

/// Bumped only when the on-disk document becomes incompatible (`persistence.ts:99`). The core
/// snapshot carries its own version; this one gates the server-owned envelope, and a mismatch
/// is an error, never a migration guess (`:1126-1130`).
pub const HARNESS_DATABASE_VERSION: u32 = 1;

/// The namespace below the supplied database root (`persistence.ts:104`). This package owns
/// exactly one child directory and never writes at the root itself.
pub const HARNESS_DATABASE_DIR: &str = "harness";

/// The checkpoint document inside that namespace (`persistence.ts:105`).
pub const HARNESS_DATABASE_FILE: &str = "state.json";

/// The durable global agent overlay, a sibling of `state.json` (`persistence.ts:1613`).
pub const HARNESS_AGENT_FILE: &str = "agents.json";

/// The journal namespace beside the checkpoint (`journal.ts:82`). `sessions_dir` is the one
/// place that composes it, so the two lifecycles stay obvious in code.
pub const HARNESS_SESSIONS_DIR: &str = "sessions";

/// The version stamped into `agents.json`'s envelope by [`serialize_agent_records`].
pub const HARNESS_AGENT_FILE_VERSION: u32 = 1;

/// The synthetic tool result a call that never finished gets (`persistence.ts:1273`
/// `INTERRUPTED_TOOL_RESULT_TEXT`). The text says the run stopped, so neither the model nor a
/// reader can mistake it for a successful result.
pub const INTERRUPTED_TOOL_RESULT_TEXT: &str =
    "The harness stopped while this tool call was running.";

/// The session-level explanation of an interrupted run (`sessions.ts:704-705`
/// `INTERRUPTED_MESSAGE`). A turn-level interruption and a session-level one are different
/// sentences, so both constants exist rather than one being overloaded.
pub const INTERRUPTED_TURN_MESSAGE: &str = "The previous run was interrupted by a server restart; it was not replayed. Review the recorded events before continuing.";

/// Marks the synthetic call id of an orphaned tool result (`<turnId>:interrupted-result:<n>`,
/// see [`interrupted_result_call_id`]).
pub const INTERRUPTED_RESULT_MARKER: &str = "interrupted-result";

/// The label the repair gives a result that had no call (brief: `kind: "interrupted_result"`).
/// The recorded event union in `contract::HarnessEvent` is closed and carries no `kind` field,
/// so the marker is reported through [`interrupted_result_call_id`] (whose prefix a reader can
/// test) and this constant; see the note in `knowledge.md`.
pub const INTERRUPTED_RESULT_KIND: &str = "interrupted_result";

/// The staging-file prefix of the checkpoint (`persistence.ts:1454-1457`).
const STAGING_SUFFIX: &str = ".tmp";

/// How many times a Windows sharing violation is retried before the write is reported. The
/// failure is real either way; the retry only covers the sub-second window in which an
/// antimalware or indexer handle is still attached to the canonical file.
const WRITE_ATTEMPTS: usize = 8;

/// Per-attempt backoff, in milliseconds. Bounded and tiny: the store treats a failed commit as
/// a 500, so a long stall would be worse than the failure it is trying to avoid.
const WRITE_BACKOFF_MS: [u64; WRITE_ATTEMPTS] = [0, 2, 5, 10, 20, 40, 80, 160];

/// Post-increment staging suffix, unique per process and write, so a stale staging file from a
/// crashed write is never reused (`persistence.ts:1405-1414` `createHarnessStagingSequence`).
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Path accessors (`persistence.ts:1530-1531`, `:1615-1616`, `journal.ts:245-246`)
// ---------------------------------------------------------------------------

/// `<root>/harness` - the directory this package owns (`HarnessPersistence.directory`).
#[must_use]
pub fn state_dir(root: &Path) -> PathBuf {
    root.join(HARNESS_DATABASE_DIR)
}

/// `<root>/harness/state.json` (`HarnessPersistence.file`).
#[must_use]
pub fn state_file_path(root: &Path) -> PathBuf {
    state_dir(root).join(HARNESS_DATABASE_FILE)
}

/// `<root>/harness/agents.json` (`harnessAgentFileOf`, `persistence.ts:1615-1616`).
#[must_use]
pub fn agent_file_path(root: &Path) -> PathBuf {
    state_dir(root).join(HARNESS_AGENT_FILE)
}

/// `<root>/harness/sessions` - the journal root (`journal.ts:245`, which joins
/// `root/sessions/<sessionId>` for one session's directory).
#[must_use]
pub fn sessions_dir(root: &Path) -> PathBuf {
    state_dir(root).join(HARNESS_SESSIONS_DIR)
}

// ---------------------------------------------------------------------------
// Records (`persistence.ts:111-309`)
// ---------------------------------------------------------------------------

/// Serializable view of one registry entry: `HarnessPresetRecord` (`persistence.ts:111-129`),
/// i.e. a preset WITHOUT its factory.
///
/// `template`/`live` are internal replay hints (`:107-110`); they are stored so a restart can
/// reproduce the same factory kind. This Rust port has no factories (the live harness is a
/// later stage), so both fields are carried as opaque data and survive a round trip.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessPresetRecord {
    pub name: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub requires_endpoint: bool,
    #[serde(default)]
    pub primary: String,
    #[serde(default)]
    pub subagents: Vec<String>,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instructions: Option<BTreeMap<String, String>>,
    /// R2's third map (`persistence.ts:131`): agent id -> CSS hex color, written only when the
    /// entry carries one, and read back with the same absent-means-empty rule (`registry.ts:793`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_colors: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub builtin: bool,
    #[serde(default)]
    pub editable: bool,
    /// `'live' | 'scripted' | 'flaky'` when present (`:127`, validated in `readPresetRecord`
    /// at `:481-486`). Kept as a `String` so a future spelling survives the round trip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// The metadata a scripted template adopts when an executable patch upgrades it (`:128`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<Value>,
    /// Unknown keys, preserved verbatim (`:509-527` builds the record from a validator, which
    /// would silently drop anything a newer build added).
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Provider settings MINUS the credential (`HarnessSessionSettingsRecord`, `persistence.ts:134-137`).
/// Only these two fields are durable; `baseURL` decides whether the environment key may be
/// re-attached on restore.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessSessionSettingsRecord {
    /// TS spells this `baseURL` - capital `URL` - which `rename_all = "camelCase"` would not
    /// produce, so the field is renamed explicitly (`persistence.ts:134-137`). `baseUrl` stays
    /// accepted as an alias for tolerance.
    #[serde(rename = "baseURL", alias = "baseUrl")]
    pub base_url: String,
    pub model: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The operator's GLOBAL provider defaults, credential-free (`HarnessGlobalSettingsRecord`,
/// `persistence.ts:255-258`). Either field may appear alone, because the writer builds it as a
/// partial and a reset deletes one key rather than the whole map (`:1183-1186`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessGlobalSettingsRecord {
    /// `baseURL`, capital `URL`, exactly as `sessions.ts databaseRecord` writes it.
    #[serde(rename = "baseURL", alias = "baseUrl")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The persisted shape of the operator's PLUGIN setting overrides (`HarnessPluginSettingsRecord`,
/// `persistence.ts:296`): plugin name -> setting key -> value.
///
/// Modeled as `Map<String, Value>` because `serde_json::Map` only implements `Serialize` /
/// `Deserialize` / `Clone` for the `Map<String, Value>` shape, so a nested
/// `Map<String, Map<String, Value>>` would not compile. The INVARIANT is what the type alias cannot
/// state: every member is itself an OBJECT whose values are the three JSON scalars a descriptor can
/// declare. The loader enforces that shape before anything reads it
/// ([`validate_document`]'s `pluginSettings` pass), and [`crate::plugins`] is the only other writer.
///
/// ONLY overrides, never defaults. Each built-in declares its own default in the catalogue
/// (`src/plugins.rs`, the port of the generated `src/plugin/manifest.ts`), so a release that moves a
/// default moves it for every deployment with no migration in this file, and a plugin with no entry
/// means "nothing has ever been changed about it". `null` is NOT stored: a reset DROPS the key,
/// which is the same fact in one fewer byte and keeps `values(plugin)` free of "explicitly nothing"
/// states. `Map`, not `BTreeMap`, because the writer emits CATALOGUE order with each plugin's keys
/// in DECLARED order (`plugins.ts snapshot`), and a documented key order is what makes a saved diff
/// reviewable.
pub type HarnessPluginSettingsRecord = Map<String, Value>;

/// One trusted-catalogue agent profile as the GLOBAL override layer records it
/// (`HarnessAgentRecord`, `persistence.ts:280-287`). A record stores ONLY what the operator
/// changed plus the retirement flag - never a copy of the catalogue text - so a restart replays
/// decisions, not prompts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessAgentRecord {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guidelines: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired: Option<bool>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// One persisted unreadable transcript (`HarnessInvalidThreadRecord`, `persistence.ts:143-153`).
/// `messages` is the raw, unvalidated value EXACTLY as submitted, because it is the user's
/// broken text and normalising it here would destroy the thing being shown.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessInvalidThreadRecord {
    #[serde(default)]
    pub error: String,
    /// `unknown` in TS: kept as a `Value` so an object, an array, or a bare string all survive.
    #[serde(default)]
    pub messages: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The frozen harness definition a session reports (`HarnessDescriptorRecord`).
///
/// This is `HarnessDescriptor` as `persistence.ts:403-429` (`readDescriptor`) validates it and
/// as `protocol.ts:261-301` declares it: the scalar and list fields are typed, and `coreTools`
/// / `agentModels` / `agentInstructions` stay OPTIONAL because they are emitted only when the
/// card actually carries something (`descriptor.ts:95-106`), so an untouched card keeps its
/// exact pre-feature shape.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessDescriptorRecord {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub requires_endpoint: bool,
    #[serde(default)]
    pub primary: String,
    #[serde(default)]
    pub subagents: Vec<String>,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instructions: Option<BTreeMap<String, String>>,
    /// R2's third map (`persistence.ts:538-540`, `protocol.ts:301`): agent id -> CSS hex color.
    /// Stored only when the card carries one, so a session frozen before colors existed restores
    /// with no key at all (`descriptor.ts:117-119`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_colors: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub builtin: bool,
    #[serde(default)]
    pub editable: bool,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// One steered submit recorded on the turn that owned the running thread
/// (`SessionSteerRecord`, `protocol.ts:224-246`).
///
/// NOT `contract::SteerRecord`: that one is the bounded WIRE row (`prompt_preview` /
/// `prompt_chars`), this one is the durable audit entry with the verbatim `prompt`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SteerRecord {
    /// The submit's own durable identity (`protocol.ts:225-233`); optional because a document
    /// written before the field existed has no key and the fold derives one for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default)]
    pub at: u64,
    #[serde(default)]
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// One task submitted to a session, as the server recorded it (`SessionTurn`,
/// `protocol.ts:122-207`; validated by `persistence.ts:533-671`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTurn {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub index: u64,
    /// The trimmed task text the harness received, not the raw request body.
    #[serde(default)]
    pub prompt: String,
    /// The persistent thread that owns this turn (`protocol.ts:127-132`); absent for a legacy
    /// `/messages` turn, which keeps its recorded shape byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    /// The conversation's agent-prefixed display label recorded WITH the turn (`:133-141`);
    /// validated against [`labels::AGENT_REFERENCE_LABEL_PATTERN`] on load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<HarnessAttachment>>,
    /// A `SessionTurnStatus` string, kept untyped so an unknown spelling is a validation
    /// message (`sessions[i].turns[j].status is not a known turn status.`) and not a
    /// deserialization failure that hides which field broke.
    #[serde(default)]
    pub status: String,
    /// Absent while `queued`; always set once the turn is running or terminal (`:158-160`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    #[serde(default)]
    pub updated_at: u64,
    /// Every event observed while the turn ran, in emission order (`:161`). Stored verbatim.
    #[serde(default)]
    pub events: Vec<HarnessEvent>,
    /// Epoch milliseconds parallel to `events`: entry i is when `events[i]` was recorded
    /// (`:162-167`). Optional so an older recorded turn stays valid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_times: Option<Vec<u64>>,
    /// The turn's token tally, summed by the server from every per-request `usage` event
    /// (`:168-173`). Absent means "no request reported any", which is NOT a zero-spend turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<HarnessUsage>,
    /// The bounded steered-submit audit ring (`:174-181`; at most
    /// [`contract::MAX_STEER_RECORDS`] entries, enforced by `readSteerRecords`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steers: Option<Vec<SteerRecord>>,
    /// The UNBOUNDED text archive of every accepted steer (`:182-206`). Entries are
    /// `{ id, at, prompt, threadId? }` and stay opaque here: the projector owns their fold, and
    /// the only rule the loader enforces is a non-blank `id` (`persistence.ts:682-717`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer_texts: Option<Vec<Value>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// One restorable session (`HarnessSessionRecord`, `persistence.ts:157-250`).
///
/// `preset` and `definition` are FROZEN copies of the creation-time metadata, so restoring does
/// not depend on the live registry still matching (`:155-156`). Field order follows the stage
/// brief, which is the TS write order modulo the three fields TS appends last (`title`,
/// `sessionAgents`) - JSON objects are unordered, so this changes only byte layout.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessSessionRecord {
    #[serde(default)]
    pub id: String,
    /// Preset name as the session reported it (`SessionSnapshot.harness`).
    #[serde(default)]
    pub harness: String,
    #[serde(default)]
    pub preset: HarnessPresetRecord,
    #[serde(default)]
    pub definition: HarnessDescriptorRecord,
    #[serde(default)]
    pub settings: HarnessSessionSettingsRecord,
    /// The session's display title (`:164-169`); absent means "no title" and stays absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// A `SessionStatus` string (`idle` | `running` | `error`), validated in
    /// [`validate_document`] rather than by serde so the message names the field.
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default)]
    pub turns: Vec<SessionTurn>,
    /// Ids of the pending (queued) turns, in FIFO order; references into `turns` (`:175-176`).
    #[serde(default)]
    pub queue: Vec<String>,
    /// The next turn INDEX high-water mark (`:177-181`); the loader derives it from the
    /// recorded turns when absent, so an old file stays readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_sequence: Option<u64>,
    /// `turnId -> { code: occurrence }` label seeds (`:182-185`). Opaque: the projector owns
    /// the counters, and this map only has to survive the round trip unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_seeds: Option<Value>,
    /// Highest label occurrence ever minted per code (`:186-194`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_high_water: Option<Value>,
    /// Stable agent-prefixed conversation labels by thread id (`:195-197`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_labels: Option<BTreeMap<String, String>>,
    /// The per-code high-water that keeps a deleted conversation's name retired (`:198`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_label_high_water: Option<BTreeMap<String, u64>>,
    /// Invocation id recorded by each THREAD turn's root run, keyed by turn id (`:199-202`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_invocations: Option<BTreeMap<String, String>>,
    /// The system-instruction text each turn's agents actually ran with, keyed by turn id
    /// (`:203-218`; `TurnInstructionSource[]` per turn). Opaque by design: an absent field
    /// means "no recorded source", and the fold then emits NO system block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_instruction_sources: Option<Value>,
    /// Every invocation observed in a persistent conversation, `invocationId -> threadId`
    /// (`:219-222`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_threads: Option<BTreeMap<String, String>>,
    /// Conversations whose stored transcript could not be read, keyed by thread id (`:224-231`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid_threads: Option<BTreeMap<String, HarnessInvalidThreadRecord>>,
    /// The last committed core snapshot (`:232-236`; `HarnessSnapshot` from `types/index.ts`).
    /// Opaque because the core owns that schema; [`repair_interrupted_snapshot`] walks it as
    /// data, exactly as `repairInterruptedToolCalls` does.
    #[serde(default)]
    pub core: Value,
    /// Live per-agent models, recorded beside the snapshot so a PATCH accepted during a run
    /// still survives a restart (`:237-240`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_models: Option<BTreeMap<String, String>>,
    /// The agents bound at session scope by `POST /sessions/{id}/agents` (`:241-249`); values
    /// are trimmed and de-duplicated, because a duplicate entry would report one binding twice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_agents: Option<Vec<String>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The durable envelope (`HarnessDatabase`, `persistence.ts:289-309`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessDatabase {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub saved_at: u64,
    #[serde(default)]
    pub registry: Vec<HarnessPresetRecord>,
    /// Names of SHIPPED built-ins the operator deleted (`:293-299`). Absent means "no
    /// deletions recorded" and is preserved as absent on a round trip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_settings: Option<HarnessGlobalSettingsRecord>,
    /// The operator's per-plugin setting OVERRIDES (`persistence.ts:352`, `sessions.ts:2758-2766`).
    ///
    /// ADDITIVE, and deliberately NOT a version bump: absent means "no plugin setting has ever
    /// been changed" - which is every document written before the plugin uplift - and an absent
    /// field stays absent on a round trip, so a deployment that never opened the Plugins pane keeps
    /// its exact previous document. The declared defaults these values override are NOT stored;
    /// they belong to the catalogue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_settings: Option<HarnessPluginSettingsRecord>,
    #[serde(default)]
    pub sessions: Vec<HarnessSessionRecord>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A conversation held by the server, as the durable roster records it (`SessionThread`,
/// `protocol.ts:415-447`).
///
/// This mirrors the persisted thread record for the store stage: the live roster lives inside
/// `HarnessSessionRecord.core`, and this typed form is what the store materialises when it
/// folds or rewrites one conversation. `messages` is the thread's committed transcript, which
/// is what [`repair_interrupted_snapshot`] pairs tool calls against inside `core`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessThreadRecord {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub agent: String,
    /// `'agent' | 'host'` (`protocol.ts:418`), kept as a string for the same reason turn
    /// status is: an unknown spelling must be a named validation failure.
    #[serde(default)]
    pub origin: String,
    /// `'idle' | 'running' | 'stopped'` (`:419`).
    #[serde(default)]
    pub status: String,
    /// Creation order, monotonic per session (`:420-421`).
    #[serde(default)]
    pub sequence: u64,
    /// The persistent parent thread - a delegated child links to its caller (`:422-423`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The thread's committed transcript, a detached copy (`:443-446`).
    #[serde(default)]
    pub messages: Vec<crate::contract::HarnessMessage>,
    /// The transcript write revision the store carries alongside the roster entry. Absent for a
    /// document written before the transcript editor existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_invocation_id: Option<String>,
    /// The invocation this thread is running right now (`:425`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    /// EVERY invocation id observed in this thread, oldest first (`:428-437`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_ids: Option<Vec<String>>,
    /// The result blocks this thread's turns consume (`:438`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<AgentResultReference>>,
    /// Persistent threads those blocks belong to - a reference EDGE, never ownership (`:439-442`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connected_thread_ids: Option<Vec<String>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

// ---------------------------------------------------------------------------
// Validation and normalization (`persistence.ts:362-1250`)
// ---------------------------------------------------------------------------

/// The five turn statuses `readTurn` accepts (`persistence.ts:537`).
pub const TURN_STATUSES: [&str; 5] = ["queued", "running", "done", "error", "cancelled"];

/// The three session statuses `readSessionRecord` accepts (`persistence.ts:901`).
pub const SESSION_STATUSES: [&str; 3] = ["idle", "running", "error"];

/// The three preset template spellings `readPresetRecord` accepts (`persistence.ts:483`).
pub const PRESET_TEMPLATES: [&str; 3] = ["live", "scripted", "flaky"];

/// Validates a parsed document the way `readHarnessDatabase` does (`persistence.ts:1122-1250`),
/// in the SAME order: version first (a future/older document must not be interpreted with
/// today's field meanings), then the two array gates, then per-record field rules, then the
/// additive collections, then duplicate session ids.
///
/// Returns the TS sentence on failure; the callers wrap it with the file path and their own
/// prefix, exactly as `HarnessPersistence.load` does (`:1568-1574`).
///
/// WHY A RAW-DOCUMENT PASS INSTEAD OF SERDE
/// ----------------------------------------
/// `readTurn` / `readSessionRecord` / `readPresetRecord` report the offending PATH on every
/// refusal (`sessions[3].turns[1].threadLabel must be a conversation label such as "RR1".`).
/// serde's message would name a different field, in a different wording, and would lose the
/// "which session" context an operator needs.
///
/// # Errors
///
/// The first structural refusal found, as the TS sentence.
pub fn validate_document(document: &Value) -> Result<(), String> {
    let Some(root) = document.as_object() else {
        return Err("The harness database must be an object.".to_string());
    };

    // `requireNumber(record.version, 'The harness database version')` + the equality gate
    // (`:1124-1130`).
    let version = require_document_number(root.get("version"), "The harness database version")?;

    if version != f64::from(HARNESS_DATABASE_VERSION) {
        return Err(format!(
            "Unsupported harness database version {}; this server reads version {}.",
            format_number(version),
            HARNESS_DATABASE_VERSION
        ));
    }

    if !root.get("registry").is_some_and(Value::is_array) {
        return Err("The harness database registry must be an array.".to_string());
    }

    if !root.get("sessions").is_some_and(Value::is_array) {
        return Err("The harness database sessions must be an array.".to_string());
    }

    for (index, entry) in root["registry"]
        .as_array()
        .expect("checked")
        .iter()
        .enumerate()
    {
        validate_preset_value(entry, &format!("registry[{index}]"))?;
    }

    // T5 deleted names (`:1150-1171`): an absent field stays absent; a present one must be an
    // array of non-blank strings. Values are NOT checked against the shipped catalogue, because
    // a tombstone may legitimately name a built-in this build stopped shipping.
    if let Some(deleted) = root.get("deleted") {
        let Some(names) = deleted.as_array() else {
            return Err(
                "The harness database deleted must be an array of harness names.".to_string(),
            );
        };

        for (index, entry) in names.iter().enumerate() {
            let Some(name) = entry.as_str() else {
                return Err(format!("deleted[{index}] must be a string."));
            };

            if name.trim().is_empty() {
                return Err(format!("deleted[{index}] must not be blank."));
            }
        }
    }

    // T16 global settings (`:1189-1228`): an `apiKey` key is REFUSED, not dropped.
    if let Some(settings) = root.get("globalSettings") {
        let Some(entry) = settings.as_object() else {
            return Err("The harness database globalSettings must be an object.".to_string());
        };

        if entry.contains_key("apiKey") {
            return Err(
                "The harness database globalSettings must not contain an API key.".to_string(),
            );
        }

        let mut stated = false;

        for key in ["baseURL", "model"] {
            if let Some(value) = entry.get(key) {
                let text = value.as_str().unwrap_or_default().trim();

                if text.is_empty() {
                    return Err(format!(
                        "The harness database globalSettings.{key} must not be blank."
                    ));
                }

                stated = true;
            }
        }

        if !stated {
            return Err(
                "The harness database globalSettings must carry at least one of baseURL or model."
                    .to_string(),
            );
        }
    }

    // T10 (R4) plugin settings (`persistence.ts:1302-1352`): STRICT about shape, LENIENT about
    // identity.
    //
    //   - OPTIONAL in both directions: an absent field stays absent, so a document from a
    //     deployment that never opened the Plugins pane keeps its exact previous shape;
    //   - the map must be an object of objects, and every value must be one of the three JSON
    //     scalars a descriptor can declare. A nested object or an array is corruption of the NAMED
    //     slot and fails the load, exactly like every other field of this document;
    //   - plugin NAMES and KEY NAMES are deliberately NOT checked against this build's catalogue -
    //     the same rule `deleted` follows above. A saved override may name a plugin this build
    //     stopped shipping or will ship again later, and `plugins::PluginSettingsStore::restore`
    //     decides what is still meaningful;
    //   - an empty map (or a map whose only entries are empty) normalizes to ABSENT in
    //     `normalize_database`, because the writer never emits `{}`: gating here is what keeps a
    //     load-save round trip byte-stable.
    if let Some(settings) = root.get("pluginSettings") {
        let Some(entries) = settings.as_object() else {
            return Err("The harness database pluginSettings must be an object.".to_string());
        };

        for (plugin, raw) in entries {
            let Some(inner) = raw.as_object() else {
                return Err(format!(
                    "The harness database pluginSettings.{plugin} must be an object."
                ));
            };

            for (key, value) in inner {
                let label = format!("The harness database pluginSettings.{plugin}.{key}");

                // The three scalars a descriptor can declare. Anything else - an object, an array,
                // or a `null` (which a RESET never persists, so it can only be a hand edit) - is
                // corruption of the NAMED slot.
                //
                // TypeScript has one more refusal here, `must be a finite number`, for a JSON
                // number that overflows to `Infinity`. `serde_json` maps such a literal to `Null`
                // while PARSING (`Number::from_f64` refuses a non-finite value), so this port
                // reports the same bad slot through the scalar sentence one step earlier. Every
                // shape the reference suite pins - a nested object, an array, a `null` - produces
                // the identical message in both builds.
                if !matches!(value, Value::Number(_) | Value::Bool(_) | Value::String(_)) {
                    return Err(format!(
                        "{label} must be a number, a string, or a boolean; plugin settings hold \
                         JSON scalars only."
                    ));
                }
            }
        }
    }

    let mut seen: Vec<&str> = Vec::new();

    for (index, entry) in root["sessions"]
        .as_array()
        .expect("checked")
        .iter()
        .enumerate()
    {
        let label = format!("sessions[{index}]");
        validate_session_value(entry, &label)?;

        // Duplicate ids would make the store's Map keep only one session (`:1231-1238`).
        let id = entry
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim();

        if seen.contains(&id) {
            return Err(format!(
                "The harness database lists session \"{id}\" more than once."
            ));
        }

        seen.push(id);
    }

    require_document_number(root.get("savedAt"), "The harness database savedAt")?;

    Ok(())
}

/// `readPresetRecord`'s template gate (`persistence.ts:481-486`) plus the object gate.
fn validate_preset_value(value: &Value, label: &str) -> Result<(), String> {
    let Some(record) = value.as_object() else {
        return Err(format!("{label} must be an object."));
    };

    if let Some(template) = record.get("template")
        && !template.is_null()
    {
        let spelling = template.as_str().unwrap_or_default();

        if !PRESET_TEMPLATES.contains(&spelling) {
            return Err(format!(
                "{label}.template must be \"live\", \"scripted\" or \"flaky\" when present."
            ));
        }
    }

    Ok(())
}

/// The per-session gates `readSessionRecord` runs (`persistence.ts:885-1118`): the credential
/// refusal, the known-status gates, the queue join, the core record, the label grammar, the
/// steer bound, and the session-agent name rules.
fn validate_session_value(value: &Value, label: &str) -> Result<(), String> {
    let Some(record) = value.as_object() else {
        return Err(format!("{label} must be an object."));
    };

    // `requireRecord(record.settings, ...)` then the outright refusal of a persisted credential
    // (`:887-893`): it would mean an older/foreign writer stored one, and silently dropping it
    // would hide that defect.
    if let Some(settings) = record.get("settings") {
        let Some(entry) = settings.as_object() else {
            return Err(format!("{label}.settings must be an object."));
        };

        if entry.contains_key("apiKey") {
            return Err(format!("{label}.settings must not contain an API key."));
        }
    }

    // `readDescriptor` requires booleans for the two flags the card is built on (`:416`, `:426`).
    for field in ["requiresEndpoint", "builtin", "editable"] {
        if let Some(definition) = record.get("definition")
            && let Some(definition) = definition.as_object()
            && let Some(flag) = definition.get(field)
            && !flag.is_boolean()
        {
            return Err(format!("{label}.definition.{field} must be a boolean."));
        }
    }

    let status = record.get("status").and_then(Value::as_str);

    if status.is_some_and(|value| !SESSION_STATUSES.contains(&value)) {
        return Err(format!("{label}.status is not a known session status."));
    }

    // Turn-level gates, in `readTurn`'s order (`:533-671`).
    let turns = record.get("turns").and_then(Value::as_array);

    let mut queued: Vec<&str> = Vec::new();

    if let Some(turns) = turns {
        for (index, turn) in turns.iter().enumerate() {
            let turn_label = format!("{label}.turns[{index}]");
            let Some(turn) = turn.as_object() else {
                return Err(format!("{turn_label} must be an object."));
            };

            let Some(turn_status) = turn.get("status").and_then(Value::as_str) else {
                return Err(format!("{turn_label}.status must be a string."));
            };

            if !TURN_STATUSES.contains(&turn_status) {
                return Err(format!("{turn_label}.status is not a known turn status."));
            }

            if turn.get("threadId").is_some_and(|value| {
                value.as_str().map(str::trim) == Some("") || !value.is_string()
            }) {
                return Err(format!("{turn_label}.threadId must not be blank."));
            }

            if let Some(thread_label) = turn.get("threadLabel")
                && labels::agent_label_parts(thread_label.as_str().unwrap_or_default()).is_none()
            {
                return Err(format!(
                    "{turn_label}.threadLabel must be a conversation label such as \"RR1\"."
                ));
            }

            if turn.get("events").is_some_and(|events| !events.is_array()) {
                return Err(format!("{turn_label}.events must be an array."));
            }

            if let Some(times) = turn.get("eventTimes") {
                let Some(times) = times.as_array() else {
                    return Err(format!("{turn_label}.eventTimes must be an array."));
                };

                for (position, entry) in times.iter().enumerate() {
                    if !entry.is_number() {
                        return Err(format!(
                            "{turn_label}.eventTimes[{position}] must be a finite number."
                        ));
                    }
                }
            }

            if let Some(steers) = turn.get("steers") {
                let Some(steers) = steers.as_array() else {
                    return Err(format!("{turn_label}.steers must be an array."));
                };

                if steers.len() > contract::MAX_STEER_RECORDS {
                    return Err(format!(
                        "{turn_label}.steers cannot contain more than {} entries.",
                        contract::MAX_STEER_RECORDS
                    ));
                }
            }

            if let Some(texts) = turn.get("steerTexts") {
                let Some(texts) = texts.as_array() else {
                    return Err(format!("{turn_label}.steerTexts must be an array."));
                };

                for (position, entry) in texts.iter().enumerate() {
                    let id = entry
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .trim();

                    if id.is_empty() {
                        return Err(format!(
                            "{turn_label}.steerTexts[{position}].id must not be blank."
                        ));
                    }
                }
            }

            if turn_status == "queued"
                && let Some(id) = turn.get("id").and_then(Value::as_str)
            {
                queued.push(id);
            }
        }
    }

    // Every queued id must reference a QUEUED turn in this session (`:910-923`): a dangling
    // entry would make the restored `queueDepth` lie about runnable work.
    if let Some(queue) = record.get("queue") {
        let Some(ids) = queue.as_array() else {
            return Err(format!("{label}.queue must be an array of strings."));
        };

        for entry in ids {
            let id = entry.as_str().unwrap_or_default();

            if !queued.contains(&id) {
                let known = turns.is_some_and(|turns| {
                    turns
                        .iter()
                        .any(|turn| turn.get("id").and_then(Value::as_str) == Some(id))
                });

                return Err(if known {
                    format!("{label}.queue names turn \"{id}\", which is not queued.")
                } else {
                    format!("{label}.queue names turn \"{id}\", which is not in this session.")
                });
            }
        }
    }

    // `core` is the snapshot the store restores; only its envelope is this loader's business
    // (`:925-926`) - `core.version` gates the snapshot the same way the document gates itself.
    if let Some(core) = record.get("core") {
        let Some(core) = core.as_object() else {
            return Err(format!("{label}.core must be an object."));
        };

        if core.contains_key("version") && !core["version"].is_number() {
            return Err(format!("{label}.core.version must be a finite number."));
        }
    }

    // R8 label map (`:796-816`): every key a non-blank thread id, every value a label the
    // allocator can emit.
    if let Some(map) = record.get("threadLabels") {
        let Some(entries) = map.as_object() else {
            return Err(format!("{label}.threadLabels must be an object."));
        };

        for (key, value) in entries {
            if key.trim().is_empty() {
                return Err(format!("{label}.threadLabels has a blank thread id key."));
            }

            if labels::agent_label_parts(value.as_str().unwrap_or_default()).is_none() {
                return Err(format!(
                    "{label}.threadLabels.{key} must be a conversation label such as \"RR1\"."
                ));
            }
        }
    }

    // C4 invalid transcripts (`:1010-1041`): `error` is the one field with a hard shape
    // requirement; `messages` is opaque by design.
    if let Some(map) = record.get("invalidThreads") {
        let Some(entries) = map.as_object() else {
            return Err(format!("{label}.invalidThreads must be an object."));
        };

        for (thread_id, entry) in entries {
            let Some(item) = entry.as_object() else {
                return Err(format!(
                    "{label}.invalidThreads.{thread_id} must be an object."
                ));
            };

            if item
                .get("error")
                .is_some_and(|error| error.as_str().is_none())
            {
                return Err(format!(
                    "{label}.invalidThreads.{thread_id}.error must be a string."
                ));
            }
        }
    }

    // T27 session-scoped bindings (`:1067-1086`): a blank entry corrupts the named slot,
    // because a roster member name cannot be blank.
    if let Some(names) = record.get("sessionAgents") {
        let Some(list) = names.as_array() else {
            return Err(format!(
                "{label}.sessionAgents must be an array of strings."
            ));
        };

        for (index, entry) in list.iter().enumerate() {
            if entry.as_str().map(str::trim).unwrap_or_default().is_empty() {
                return Err(format!("{label}.sessionAgents[{index}] must not be blank."));
            }
        }
    }

    Ok(())
}

/// `requireNumber` for the document's own scalars (`persistence.ts:380-386`).
fn require_document_number(value: Option<&Value>, label: &str) -> Result<f64, String> {
    let Some(number) = value.and_then(Value::as_f64) else {
        return Err(format!("{label} must be a finite number."));
    };

    Ok(number)
}

/// Renders a JSON number the way `JSON.stringify` does, so the version refusal quotes the value
/// the file carries (`999`, not `999.0`).
fn format_number(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < f64::from(u32::MAX) {
        return format!("{}", value as i64);
    }

    value.to_string()
}

/// Applies the normalizations `readHarnessDatabase` performs while decoding (`persistence.ts`
/// `:1056-1086`, `:1158-1171`, `:1199-1227`, `:629-643`, `:796-816`): trims, blank-means-absent,
/// duplicate collapse, and the allocator's canonical label spelling.
///
/// Splitting this from [`validate_document`] keeps the two jobs readable; the rules are the
/// same ones, applied after the shape is known good.
fn normalize_database(database: &mut HarnessDatabase) {
    if let Some(deleted) = &database.deleted {
        let mut names: Vec<String> = Vec::with_capacity(deleted.len());

        for entry in deleted {
            let name = entry.trim().to_string();

            if !name.is_empty() && !names.contains(&name) {
                names.push(name);
            }
        }

        database.deleted = Some(names);
    }

    if let Some(settings) = &mut database.global_settings {
        settings.base_url = settings
            .base_url
            .take()
            .map(|value| value.trim().to_string());
        settings.model = settings.model.take().map(|value| value.trim().to_string());
    }

    // T10 plugin settings (`persistence.ts:1341-1349`): an EMPTY plugin map states nothing, so it
    // is not carried forward, and a map whose every entry is empty normalizes to ABSENT. The writer
    // never emits `{}`, so gating here is what makes load -> save byte-stable for a document a
    // hand-edit left an empty slot in.
    if let Some(entries) = &mut database.plugin_settings {
        entries.retain(|_, value| value.as_object().is_some_and(|values| !values.is_empty()));

        if entries.is_empty() {
            database.plugin_settings = None;
        }
    }

    for session in &mut database.sessions {
        // `title`: trim, plus "blank means no title" (`:1056-1062`). The 200-character cap is
        // the WRITER's (`sessions.ts renameSession`) and is deliberately not enforced here -
        // throwing on a long hand-edited name would drop the whole session.
        session.title = session.title.take().map(|value| value.trim().to_string());

        if session.title.as_deref().is_some_and(str::is_empty) {
            session.title = None;
        }

        if session.last_error.as_deref().is_some_and(str::is_empty) {
            session.last_error = None;
        }

        // `sessionAgents`: trimmed, blank-rejected (above), duplicates collapsed because they
        // name one binding (`:1070-1086`).
        if let Some(names) = &mut session.session_agents {
            let mut trimmed: Vec<String> = Vec::with_capacity(names.len());

            for entry in names.iter() {
                let name = entry.trim().to_string();

                if !name.is_empty() && !trimmed.contains(&name) {
                    trimmed.push(name);
                }
            }

            *names = trimmed;
        }

        for turn in &mut session.turns {
            turn.prompt = turn.prompt.trim_end().to_string();

            // `threadLabel` is NORMALISED to the allocator's uppercase form (`:641`), never
            // merely accepted, so a persisted `rr3` and a freshly minted `RR3` compare equal.
            if let Some(label) = &turn.thread_label
                && let Some(parts) = labels::agent_label_parts(label)
            {
                turn.thread_label = Some(parts.label);
            }
        }

        if let Some(map) = &mut session.thread_labels {
            let canonical: BTreeMap<String, String> = map
                .iter()
                .map(|(key, value)| {
                    let parts = labels::agent_label_parts(value);
                    (
                        key.clone(),
                        parts.map_or_else(|| value.clone(), |parts| parts.label),
                    )
                })
                .collect();

            *map = canonical;
        }
    }
}

// ---------------------------------------------------------------------------
// Read (`persistence.ts:1540-1575`)
// ---------------------------------------------------------------------------

/// Reads and validates the checkpoint under `root`.
///
/// A missing file is `Ok(None)` - "first run", `:1548`. Every other failure, including a
/// malformed document, is an error: nothing is reset or deleted (`:63-69`).
///
/// # Errors
///
/// [`ApiError::bad_request`] naming the file and the TS sentence when the document is malformed
/// or structurally invalid; [`ApiError::internal`] when the file cannot be read at all.
#[must_use = "the result says whether the database exists and is readable"]
pub fn read_harness_database(root: &Path) -> Result<Option<HarnessDatabase>, ApiError> {
    let file = state_file_path(root);

    match read_state_text(&file)? {
        None => Ok(None),
        Some(text) => Ok(Some(parse_state_document(&file, &text)?)),
    }
}

/// Reads one checkpoint FILE, requiring it to exist.
///
/// The root-level reader is the one that answers `None` for an absent document; this accessor is
/// for a caller that already resolved a concrete path (a test fixture, or a foreign database).
///
/// # Errors
///
/// As [`read_harness_database`], plus [`ApiError::not_found`] when `path` does not exist.
pub fn read_harness_database_file(path: &Path) -> Result<HarnessDatabase, ApiError> {
    let text = match read_state_text(path)? {
        Some(text) => text,
        None => {
            return Err(ApiError::not_found(format!(
                "{}: Could not read the harness database: {}",
                display(path),
                MISSING_FILE_MESSAGE
            )));
        }
    };

    parse_state_document(path, &text)
}

/// The stable text of a failed open on a missing document. Node spells this
/// `ENOENT: no such file or directory`; the Rust port states the same fact without leaking a
/// platform error string into a test.
pub const MISSING_FILE_MESSAGE: &str = "no such file or directory";

/// Reads the document's bytes, or `None` for an absent file.
fn read_state_text(file: &Path) -> Result<Option<String>, ApiError> {
    match fs::read_to_string(file) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ApiError::internal(format!(
            "{}: Could not read the harness database: {error}",
            display(file)
        ))),
    }
}

/// Parses and validates one document body, reproducing `load()`'s two wrappers (`:1556-1574`).
fn parse_state_document(file: &Path, text: &str) -> Result<HarnessDatabase, ApiError> {
    let parsed: Value = serde_json::from_str(text).map_err(|error| {
        ApiError::bad_request(format!(
            "{}: The harness database is not valid JSON: {error}",
            display(file)
        ))
    })?;

    validate_document(&parsed).map_err(|reason| {
        ApiError::bad_request(format!(
            "{}: The harness database has an invalid shape or version: {reason}",
            display(file)
        ))
    })?;

    let mut database: HarnessDatabase = serde_json::from_value(parsed).map_err(|error| {
        ApiError::bad_request(format!(
            "{}: The harness database has an invalid shape or version: {error}",
            display(file)
        ))
    })?;

    normalize_database(&mut database);

    Ok(database)
}

// ---------------------------------------------------------------------------
// Write (`persistence.ts:1398-1511`)
// ---------------------------------------------------------------------------

/// The commit payload: `{ ...database, version: HARNESS_DATABASE_VERSION, savedAt }`,
/// two-space pretty-printed (`serializeHarnessDatabase`, `persistence.ts:1398-1399`).
///
/// The writer stamps BOTH fields, so a caller cannot persist a stale version or a stale
/// timestamp by accident.
#[must_use]
pub fn serialize_harness_database(database: &HarnessDatabase, saved_at: u64) -> String {
    let mut document = serde_json::to_value(database).unwrap_or(Value::Null);

    if let Some(object) = document.as_object_mut() {
        object.insert("version".to_string(), json!(HARNESS_DATABASE_VERSION));
        object.insert("savedAt".to_string(), json!(saved_at));
    }

    serde_json::to_string_pretty(&document).unwrap_or_else(|_| "{}".to_string())
}

/// The atomic full-document commit (`save` + `writeHarnessDatabaseFile`, `:1448-1511` +
/// `:1584-1597`).
///
/// `owner` is the identity baked into the staging file name (`:1419-1422`): the process id on
/// the coordinator, `<pid>.io<threadId>` on a worker. It keeps two writers' staging names apart,
/// which is the property the retry loop below depends on.
///
/// Durability boundary, stated exactly as `:1436-1439` does: when this returns, the payload
/// bytes are fsynced and the directory ENTRY is replaced. The directory entry is fsynced too
/// where the platform allows it, and skipped where it does not (Windows cannot open a directory
/// as a file).
///
/// # Errors
///
/// [`ApiError::internal`] naming the canonical file and the failing step, after the bounded
/// retry budget is spent.
pub fn write_harness_database(
    root: &Path,
    database: &HarnessDatabase,
    owner: &str,
) -> Result<(), ApiError> {
    let directory = state_dir(root);
    let file = state_file_path(root);
    let payload = serialize_harness_database(database, events::now_millis());

    write_file_atomic(&directory, &file, &payload, owner, HARNESS_DATABASE_FILE)
}

/// Reads the global agent overlay (`readHarnessAgentRecords`, `persistence.ts:1621-1662`).
///
/// An absent file means "no decisions recorded" - every pre-T19 database - and is `Ok(vec![])`.
/// A corrupt or foreign file is a LOUD failure naming the path: a silently ignored overlay would
/// let a restart resurrect a retired agent while claiming durability.
///
/// # Errors
///
/// As [`read_harness_database`]: [`ApiError::bad_request`] for a bad document, [`ApiError::internal`]
/// for a failed open.
pub fn read_harness_agent_records(root: &Path) -> Result<Vec<HarnessAgentRecord>, ApiError> {
    let file = agent_file_path(root);

    let text = match fs::read_to_string(&file) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "{}: Could not read the harness agent overlay: {error}",
                display(&file)
            )));
        }
    };

    parse_harness_agent_records(&text).map_err(|error| {
        ApiError::new(
            error.status,
            format!("{}: {}", display(&file), error.message),
        )
    })
}

/// Commits the overlay with the same staging+fsync+rename discipline as `state.json`
/// (`writeHarnessAgentRecords`, `persistence.ts:1668-1685`).
///
/// The staging prefix is this file's own name, not `state.json.` - see the note in
/// [`write_file_atomic`].
///
/// # Errors
///
/// As [`write_harness_database`].
pub fn write_harness_agent_records(
    root: &Path,
    records: &[HarnessAgentRecord],
) -> Result<(), ApiError> {
    let directory = state_dir(root);
    let file = agent_file_path(root);
    let payload = serde_json::to_string_pretty(&serialize_agent_records(records))
        .unwrap_or_else(|_| "{}".to_string());

    write_file_atomic(&directory, &file, &payload, "agents", HARNESS_AGENT_FILE)
}

/// The overlay document: `{ version, records }`.
///
/// TS writes a bare ARRAY here (`persistence.ts:1679`); this envelope is the stage-brief shape,
/// and [`parse_harness_agent_records`] accepts BOTH, so an `agents.json` written by the TS
/// server still loads and the envelope's version gates a future format change.
#[must_use]
pub fn serialize_agent_records(records: &[HarnessAgentRecord]) -> Value {
    let entries: Vec<Value> = records
        .iter()
        .map(|record| serde_json::to_value(record).unwrap_or(Value::Null))
        .collect();

    json!({ "version": HARNESS_AGENT_FILE_VERSION, "records": entries })
}

/// Parses the overlay text in either spelling (`readHarnessAgentRecords` + `readAgentRecord`,
/// `persistence.ts:436-477`, `:1640-1661`).
///
/// # Errors
///
/// [`ApiError::bad_request`] with the TS sentence (`The harness agent overlay ...` /
/// `agents[i].<field> ...`); the caller prepends the file path.
pub fn parse_harness_agent_records(text: &str) -> Result<Vec<HarnessAgentRecord>, ApiError> {
    let parsed: Value = serde_json::from_str(text).map_err(|error| {
        ApiError::bad_request(format!(
            "The harness agent overlay is not valid JSON: {error}"
        ))
    })?;

    let entries = if let Some(list) = parsed.as_array() {
        // The TS shape: one array of records, no envelope.
        list.clone()
    } else if let Some(object) = parsed.as_object() {
        if let Some(version) = object.get("version") {
            let number = version
                .as_f64()
                .filter(|value| value.fract() == 0.0 && *value > 0.0)
                .map(|value| value as u64);

            match number {
                Some(value)
                    if u32::try_from(value).unwrap_or(u32::MAX) == HARNESS_AGENT_FILE_VERSION => {}
                Some(value) => {
                    return Err(ApiError::bad_request(format!(
                        "Unsupported harness agent overlay version {value}; this server reads version {HARNESS_AGENT_FILE_VERSION}."
                    )));
                }
                None => {
                    return Err(ApiError::bad_request(
                        "The harness agent overlay version must be a positive integer.",
                    ));
                }
            }
        }

        object
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| {
                // `{}` with no `records` at all is the writer's "no decisions" answer.
                Vec::new()
            })
    } else {
        return Err(ApiError::bad_request(
            "The harness agent overlay must be an array of agent records.",
        ));
    };

    let mut records: Vec<HarnessAgentRecord> = Vec::with_capacity(entries.len());
    let mut seen: Vec<String> = Vec::with_capacity(entries.len());

    for (index, entry) in entries.iter().enumerate() {
        let label = format!("agents[{index}]");
        let record = read_agent_value(entry, &label)?;

        if seen.contains(&record.name) {
            return Err(ApiError::bad_request(format!(
                "The harness agent overlay lists agent \"{}\" more than once.",
                record.name
            )));
        }

        seen.push(record.name.clone());
        records.push(record);
    }

    Ok(records)
}

/// `readAgentRecord` (`persistence.ts:436-477`): a stated override field must be a string,
/// `guidelines` must be a list of non-blank strings, `retired` is the tombstone, and a record
/// with NO override and NO tombstone is accepted as "the shipped profile".
fn read_agent_value(value: &Value, label: &str) -> Result<HarnessAgentRecord, ApiError> {
    let Some(record) = value.as_object() else {
        return Err(ApiError::bad_request(format!("{label} must be an object.")));
    };

    let name = record
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request(format!("{label}.name must be a string.")))?
        .trim();

    if name.is_empty() {
        return Err(ApiError::bad_request(format!(
            "{label}.name must not be blank."
        )));
    }

    // Built field by field (not through serde) so every refusal carries THIS label; a
    // `from_value` failure would print serde's wording and lose `agents[i].<field>`.
    let mut parsed = HarnessAgentRecord {
        name: name.to_string(),
        ..HarnessAgentRecord::default()
    };

    for field in ["title", "description", "instruction"] {
        if let Some(entry) = record.get(field) {
            let Some(text) = entry.as_str() else {
                return Err(ApiError::bad_request(format!(
                    "{label}.{field} must be a string."
                )));
            };

            match field {
                "title" => parsed.title = Some(text.to_string()),
                "description" => parsed.description = Some(text.to_string()),
                _ => parsed.instruction = Some(text.to_string()),
            }
        }
    }

    if let Some(lines) = record.get("guidelines") {
        let Some(list) = lines.as_array() else {
            return Err(ApiError::bad_request(format!(
                "{label}.guidelines must be an array of strings."
            )));
        };

        let mut kept: Vec<String> = Vec::with_capacity(list.len());

        for (index, entry) in list.iter().enumerate() {
            let Some(text) = entry.as_str() else {
                return Err(ApiError::bad_request(format!(
                    "{label}.guidelines[{index}] must be a string."
                )));
            };

            if text.trim().is_empty() {
                return Err(ApiError::bad_request(format!(
                    "{label}.guidelines[{index}] must not be blank."
                )));
            }

            kept.push(text.to_string());
        }

        parsed.guidelines = Some(kept);
    }

    if let Some(retired) = record.get("retired") {
        let Some(flag) = retired.as_bool() else {
            return Err(ApiError::bad_request(format!(
                "{label}.retired must be a boolean."
            )));
        };

        // TS only writes the key when it is true (`toRecords`, `agent-state.ts:323`), and an
        // explicit `false` means the same thing as absent, so both normalize to "no tombstone".
        parsed.retired = if flag { Some(true) } else { None };
    }

    // Anything the record carries beyond the modelled fields survives the round trip.
    let mut extra = record.clone();

    for field in [
        "name",
        "title",
        "description",
        "instruction",
        "guidelines",
        "retired",
    ] {
        let _ = extra.remove(field);
    }

    parsed.extra = extra;

    Ok(parsed)
}

/// The shared atomic commit: create the directory, write a staging file, fsync it, rename it
/// over the canonical path, fsync the directory where the platform supports it.
///
/// `staging_prefix` is the canonical file's own name, so the staging entry always sorts next to
/// the file it replaces. TS reuses `state.json.` for BOTH files (`persistence.ts:1680-1683`),
/// which works but makes an `agents.json` staging file look like an abandoned checkpoint; naming
/// each commit after its own target removes that ambiguity and cannot collide, since the two
/// prefixes differ.
///
/// The retry loop is the Windows half of `writeHarnessDatabaseFile`: a rename onto a file that a
/// scanner still holds fails with a sharing violation, and TS never sees it because POSIX
/// renames silently displace an open file. Every attempt removes its staging file, so a failed
/// commit leaves no partial document (`:1501-1507`).
fn write_file_atomic(
    directory: &Path,
    file: &Path,
    payload: &str,
    owner: &str,
    staging_prefix: &str,
) -> Result<(), ApiError> {
    fs::create_dir_all(directory).map_err(|error| {
        ApiError::internal(format!(
            "{}: Could not create the harness database directory: {error}",
            display(file)
        ))
    })?;

    let mut last_error = String::new();

    for (attempt, backoff) in WRITE_BACKOFF_MS.iter().enumerate() {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(*backoff));
        }

        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let staging = directory.join(format!(
            "{staging_prefix}.{owner}.{sequence}{STAGING_SUFFIX}"
        ));

        match commit_attempt(&staging, file, payload) {
            Ok(()) => return Ok(()),
            Err(error) => {
                // A partial staging file is not a commit: remove it so a later run cannot
                // confuse it with a real document (the canonical file is kept).
                let _ = fs::remove_file(&staging);
                last_error = error.to_string();

                if !is_transient(&error) {
                    break;
                }
            }
        }
    }

    Err(ApiError::internal(format!(
        "{}: Could not write the harness database: {last_error}",
        display(file)
    )))
}

/// One staging write + rename + directory fence.
fn commit_attempt(staging: &Path, file: &Path, payload: &str) -> std::io::Result<()> {
    // `create_new`, never `append`: a staging file must be exactly this payload, and a name that
    // is somehow already taken is a reason to pick the next sequence number, not to concatenate.
    let mut handle = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(staging)?;

    let written = (|| -> std::io::Result<()> {
        handle.write_all(payload.as_bytes())?;
        // The OS buffer must be flushed into the file before fsync can make it durable.
        handle.flush()?;
        // fsync BEFORE the rename: the rename may be durable while the bytes are not, which
        // would publish an empty/truncated canonical file (`persistence.ts:1484-1487`).
        handle.sync_all()
    })();

    drop(handle);
    written?;

    // Same-directory rename: atomic on the platforms this server targets (`:1490-1491`).
    fs::rename(staging, file)?;

    // The directory fence: Node's writer never fsyncs the directory either (`:1436-1439`), and
    // Windows cannot open a directory as a file, so a failure here is ignored on purpose.
    if let Some(parent) = file.parent() {
        let _ = File::open(parent).map(|directory| directory.sync_all());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Interrupted-run recovery
// ---------------------------------------------------------------------------

/// The synthetic call id of an orphaned tool result: `<turnId>:interrupted-result:<index>`
/// (brief; the index is the result's position in the turn's event list).
///
/// An orphan has no real call id to key on, and every later join - the projector's step map, the
/// transcript pairing - addresses a step by call id. A minted id in this shape is stable across
/// restarts (the turn id and the position are both durable) and cannot collide with a provider
/// call id.
#[must_use]
pub fn interrupted_result_call_id(turn_id: &str, index: usize) -> String {
    format!("{turn_id}:{INTERRUPTED_RESULT_MARKER}:{index}")
}

/// The message an interrupted turn reports (`sessions.ts:704-705`).
#[must_use]
pub const fn interrupted_turn_message() -> &'static str {
    INTERRUPTED_TURN_MESSAGE
}

/// One recorded `tool_result` whose `tool_call` is missing, captured as owned data so the
/// repair pass can rewrite the event list it came from (see
/// [`repair_interrupted_tool_calls`], step 3).
#[derive(Debug, Clone)]
struct OrphanResult {
    /// Position in the turn's ORIGINAL event list - the `<index>` of the synthetic call id.
    index: usize,
    agent: String,
    name: String,
    call_id: Option<String>,
    content: String,
    is_error: bool,
    outcome: Option<contract::HarnessToolOutcome>,
    /// The invocation identity, carried as a `Value` because `contract::HarnessThread` has to be
    /// re-decoded after the borrow on the event list ends.
    thread: Option<Value>,
}

/// Marks an in-flight turn as interrupted and closes its dangling tool calls.
///
/// The recovery policy is the user-approved one, stated at `sessions.ts:700-705` and
/// `persistence.ts:1262-1268`: never execute a tool again, never trim completed evidence. For
/// this record that means, per turn whose status is still `running`:
///
///   1. the turn becomes an `error` row stamped `now`, and the SESSION becomes an error row with
///      [`INTERRUPTED_TURN_MESSAGE`] - one sentence per interrupted run (`sessions.ts:3007-3013`
///      and `:3036-3043`);
///   2. every `tool_call` with no matching `tool_result` gets EXACTLY ONE terminal record: a
///      failed `tool_result` carrying [`INTERRUPTED_TOOL_RESULT_TEXT`]. `isError` stays
///      `outcome != completed` (`types/index.ts:1882`), so the pair is `isError: true` +
///      `outcome: cancelled` - cancelled because the call was cut short, not because it ran and
///      reported a failure;
///   3. a `tool_result` with no matching `tool_call` (the call event was lost, or the result was
///      recorded for a call this build never saw) is marked with a synthetic call id from
///      [`interrupted_result_call_id`] and the same failed/cancelled pair, so it can never be
///      mistaken for an executed call's answer.
///
/// Pure and idempotent: a repaired turn has every call answered and no orphan, so a second call
/// changes nothing and returns 0.
///
/// Returns the number of individual repairs applied (turns closed + records synthesized).
pub fn repair_interrupted_tool_calls(record: &mut HarnessSessionRecord) -> usize {
    let restored_at = events::now_millis();
    let mut repairs = 0;

    for turn in &mut record.turns {
        if turn.status != "running" {
            continue;
        }

        // (1) The turn keeps its recorded events; only its status and clock move.
        turn.status = "error".to_string();
        turn.updated_at = restored_at;
        repairs += 1;

        // (2) Calls without a result, in call order, so the appended records follow the order the
        // model asked for them (`persistence.ts:1312-1322`).
        let answered: Vec<String> = turn
            .events
            .iter()
            .filter_map(|event| match event {
                HarnessEvent::ToolResult { call_id, .. } => call_id.clone(),
                _ => None,
            })
            .collect();

        let requested: Vec<(String, String, String, Option<Value>)> = turn
            .events
            .iter()
            .filter_map(|event| match event {
                HarnessEvent::ToolCall {
                    agent,
                    call,
                    thread,
                    ..
                } => Some((
                    agent.clone(),
                    call.id.clone(),
                    call.name.clone(),
                    thread
                        .as_ref()
                        .map(|value| serde_json::to_value(value).unwrap_or(Value::Null)),
                )),
                _ => None,
            })
            .collect();

        for (agent, call_id, name, thread) in requested {
            if answered.contains(&call_id) {
                continue;
            }

            repairs += 1;
            turn.events.push(synthetic_result_event(
                &agent,
                &name,
                Some(call_id),
                thread.as_ref(),
            ));
        }

        // (3) Results without a call. The call list is snapshotted first because the scan below
        // is followed by mutations to the list it is walking.
        let calls: Vec<String> = turn
            .events
            .iter()
            .filter_map(|event| match event {
                HarnessEvent::ToolCall { call, .. } => Some(call.id.clone()),
                _ => None,
            })
            .collect();

        // Owned copies, so the mutation pass never holds a borrow into `turn.events` while it
        // writes one back.
        let mut orphans: Vec<OrphanResult> = Vec::new();

        for (index, event) in turn.events.iter().enumerate() {
            let HarnessEvent::ToolResult {
                agent,
                name,
                call_id,
                content,
                is_error,
                outcome,
                thread,
                ..
            } = event
            else {
                continue;
            };

            let owned = call_id.clone();

            if owned.as_ref().is_some_and(|id| calls.contains(id)) {
                continue;
            }

            orphans.push(OrphanResult {
                index,
                agent: agent.clone(),
                name: name.clone(),
                call_id: owned,
                content: content.clone(),
                is_error: *is_error,
                outcome: *outcome,
                thread: thread
                    .as_ref()
                    .map(|value| serde_json::to_value(value).unwrap_or(Value::Null)),
            });
        }

        // Reverse order, so rewriting entry `index` never shifts an entry still to be rewritten.
        for orphan in orphans.into_iter().rev() {
            // A result that never had an identity needs one to stay addressable; an unmatched id
            // it DID carry is evidence, so it is kept and only the pairing is marked.
            let needs_identity = orphan
                .call_id
                .as_deref()
                .is_none_or(|id| id.trim().is_empty());

            let call_id = if needs_identity {
                interrupted_result_call_id(&turn.id, orphan.index)
            } else {
                orphan.call_id.clone().unwrap_or_default()
            };

            // A `tool_result` event is terminal by construction, so an orphan whose call event is
            // gone cannot be trusted as a success: it becomes an interrupted result
            // ([`INTERRUPTED_RESULT_KIND`], addressable through the synthetic call id above) while
            // its recorded text is preserved verbatim, because trimming it would discard evidence
            // the run already produced (`persistence.ts:1263-1267`).
            if !orphan.is_error || orphan.outcome != Some(contract::HarnessToolOutcome::Cancelled) {
                repairs += 1;
            }

            turn.events[orphan.index] = HarnessEvent::ToolResult {
                agent: orphan.agent,
                name: orphan.name,
                content: orphan.content,
                is_error: true,
                call_id: Some(call_id),
                outcome: Some(contract::HarnessToolOutcome::Cancelled),
                ui: Vec::new(),
                media: Vec::new(),
                duration_ms: None,
                pending: Vec::new(),
                thread: thread_from_value(&orphan.thread),
            };
        }
    }

    if repairs > 0 {
        // The session mirrors its turn: an interrupted run is an error row whose explanation is
        // the fixed sentence, never a per-turn message (`sessions.ts:3036-3043`).
        record.status = "error".to_string();
        record.updated_at = restored_at;

        if record.last_error.as_deref().is_none_or(str::is_empty) {
            record.last_error = Some(INTERRUPTED_TURN_MESSAGE.to_string());
        }
    }

    repairs
}

/// Builds the one synthetic terminal record a dangling call gets (`isError` + `cancelled` +
/// the interrupted text), reusing the call's own agent, tool name and invocation identity.
fn synthetic_result_event(
    agent: &str,
    name: &str,
    call_id: Option<String>,
    thread: Option<&Value>,
) -> HarnessEvent {
    HarnessEvent::ToolResult {
        agent: agent.to_string(),
        name: name.to_string(),
        content: INTERRUPTED_TOOL_RESULT_TEXT.to_string(),
        is_error: true,
        call_id,
        outcome: Some(contract::HarnessToolOutcome::Cancelled),
        ui: Vec::new(),
        media: Vec::new(),
        duration_ms: None,
        pending: Vec::new(),
        thread: thread_from_value(&thread.cloned()),
    }
}

/// Re-decodes the invocation identity that was carried through the repair as a `Value`.
///
/// `contract::HarnessThread` is `Serialize + Deserialize`, and the repair has to move the value
/// through a `Value` because the event list is borrowed while it is being scanned. A decode
/// failure can only mean the entry was never a thread, in which case "no invocation identity" is
/// the honest answer.
fn thread_from_value(value: &Option<Value>) -> Option<contract::HarnessThread> {
    value
        .as_ref()
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

/// Repairs the persisted CORE snapshot the way `repairInterruptedToolCalls` does
/// (`persistence.ts:1334-1374`), in place, over the snapshot's `Value`.
///
/// WHY THIS IS SEPARATE FROM [`repair_interrupted_tool_calls`]
/// ----------------------------------------------------------
/// The two fix different records. The function above closes the server's TURN rows; this one
/// fixes the agent and thread TRANSCRIPTS inside `HarnessSessionRecord.core`, which is what the
/// next provider request is built from. A mutation-boundary snapshot taken after an assistant
/// message committed but before its tools appended results is schema-valid yet UNMATCHED for a
/// strict provider (`:1256-1260`), and restoring it without this repair would make the endpoint
/// reject the transcript.
///
/// Rules, verbatim from `repairAgentMessages` (`:1278-1330`): keep every contiguous tool result
/// that is already there - matched or not, because they are evidence, not something to discard -
/// and append ONE explicit non-success synthetic result for each call id that has no result yet,
/// immediately after the existing block so the provider sees one coherent exchange.
///
/// Returns the number of transcripts that were changed.
pub fn repair_interrupted_snapshot(core: &mut Value) -> usize {
    let mut changed = 0;

    // `snapshot.agents` is an object of `{ ..., messages }` records (`types/index.ts`).
    if let Some(agents) = core.get_mut("agents").and_then(Value::as_object_mut) {
        for agent in agents.values_mut() {
            if let Some(messages) = agent.get_mut("messages").and_then(Value::as_array_mut)
                && repair_transcript(messages)
            {
                changed += 1;
            }
        }
    }

    // `snapshot.threads` needs the SAME repair (`:1350-1365`): a thread left mid-call would
    // otherwise make the next provider request for that conversation invalid.
    if let Some(threads) = core.get_mut("threads").and_then(Value::as_array_mut) {
        for thread in threads {
            if let Some(messages) = thread.get_mut("messages").and_then(Value::as_array_mut)
                && repair_transcript(messages)
            {
                changed += 1;
            }
        }
    }

    changed
}

/// `repairAgentMessages` over raw transcript entries. Returns true when entries were appended.
fn repair_transcript(messages: &mut Vec<Value>) -> bool {
    let mut output: Vec<Value> = Vec::with_capacity(messages.len());
    let mut changed = false;
    let mut cursor = 0;

    while cursor < messages.len() {
        let message = messages[cursor].clone();
        output.push(message.clone());
        cursor += 1;

        // Only an assistant message can own tool calls.
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }

        let Some(calls) = message.get("toolCalls").and_then(Value::as_array) else {
            continue;
        };

        if calls.is_empty() {
            continue;
        }

        let wanted: Vec<String> = calls
            .iter()
            .filter_map(|call| call.get("id").and_then(Value::as_str).map(str::to_string))
            .collect();
        let mut answered: Vec<String> = Vec::new();

        // Consume the contiguous tool-result block that follows this message.
        while cursor < messages.len()
            && messages[cursor].get("role").and_then(Value::as_str) == Some("tool")
        {
            let result = messages[cursor].clone();
            cursor += 1;

            if let Some(id) = result.get("toolCallId").and_then(Value::as_str)
                && wanted.iter().any(|call| call == id)
            {
                answered.push(id.to_string());
            }

            output.push(result);
        }

        // Append the missing results in call order.
        for call in calls {
            let id = call.get("id").and_then(Value::as_str).unwrap_or_default();

            if answered.iter().any(|done| done == id) {
                continue;
            }

            // The synthetic entry is appended in call order, immediately after the block that
            // was just consumed, so the provider sees one coherent exchange (`:1310-1322`).
            changed = true;
            output.push(json!({
                "role": "tool",
                "content": INTERRUPTED_TOOL_RESULT_TEXT,
                "toolCallId": id,
                "name": call.get("name").and_then(Value::as_str).unwrap_or_default(),
            }));
        }
    }

    if changed {
        *messages = output;
    }

    changed
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

/// `true` for the failures a retry can plausibly clear: a sharing/lock violation, an access
/// denial, a name that already exists, or a directory that is not yet empty.
///
/// Windows spells a sharing violation ERROR_SHARING_VIOLATION (32) / ERROR_LOCK_VIOLATION (33),
/// and std does not give either its own `ErrorKind`, so the raw codes are checked as well as the
/// kinds. The brief's EPERM/EACCES/EBUSY/EEXIST/ENOTEMPTY set is the POSIX spelling of the same
/// list, and `raw_os_error` carries those too (1, 13, 16, 17, 39 / 66 on macOS).
fn is_transient(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::DirectoryNotEmpty
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::ResourceBusy
    ) {
        return true;
    }

    matches!(
        error.raw_os_error(),
        Some(5) | Some(32) | Some(33) | Some(145) | Some(183) // Windows
            | Some(1) | Some(13) | Some(16) | Some(17) | Some(39) | Some(66) // POSIX
    )
}

/// Formats a path the way the TS errors do: the string the operator typed, with the platform
/// separator, never a debug-quoted form.
fn display(path: &Path) -> String {
    path.display().to_string()
}

/// Keeps the module's private map type name reachable for readers of the public records.
pub type ExtraFields = Map<String, Value>;
