//! The per-database-root runtime: everything one `/v1/harness` deployment owns, built once.
//!
//! TypeScript origin: `src/server/runtime.ts` (`createHarnessRuntime` + the
//! `Map<string, HarnessRuntime>` cache behind `getHarnessRuntime`) plus the construction half of
//! `src/server/start.ts` (the standalone listener builds the same graph eagerly). The gateway
//! selects the runtime BY DATABASE ROOT (`service.ts:399-410`, `runtime.ts:287-308`: "Two
//! different roots NEVER share a runtime ... a supplied root can never be silently ignored"), and
//! this module is the port of that rule: the cache key is the canonical database root, and the
//! first request for a root pays for the `.env` load, the document read and the store open.
//!
//! WHY LAZY (`runtime.ts:3-11`): a route module must not read `.env`, build a provider catalogue,
//! or construct sessions at import time - "route modules only describe a URL and a handler". The
//! Rust equivalent is a `router()` that carries only the root, so nothing exists until a request
//! arrives, and an embedder that serves several roots keeps several isolated graphs.
//!
//! ORDER OF CONSTRUCTION IS LOAD-BEARING and is TS's (`runtime.ts:139-221`):
//!   1. `.env` load, before the configuration and before the tool catalogue - a search key that
//!      lives in the file has to be visible when the catalogue resolves availability
//!      (`knowledge.md` T4: "a host that loads `.env` later must build the registry afterwards");
//!   2. the configuration (`load_server_config`), which validates every `HARNESS_*` value;
//!   3. `state.json` + `agents.json`, read BEFORE any session is constructed, so the registry is
//!      rebuilt first and a session's harness resolves from its own frozen copy afterwards;
//!   4. the registry replay (tombstones first, then saved rows) and the agent overlay;
//!   5. the tool catalogue, the turn executor, the projector factory, then the store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use serde_json::Map;

use crate::agents::AgentCatalogue;
use crate::config::{self, HarnessRuntimeSettings, ServerConfig};
use crate::contract::RunnerSettingsInput;
use crate::environment;
use crate::error::ApiError;
use crate::events::EventHub;
use crate::package_root;
use crate::persistence::{self, HarnessGlobalSettingsRecord};
use crate::ports::{ProjectorFactory, ToolRuntime, TurnExecutor};
use crate::projector::TurnProjectorFactory;
use crate::registry::HarnessRegistry;
use crate::store::Store;
use crate::store::StoreConfig;
use crate::store::actions::StoreTaskHost;
use crate::tools::ToolRegistry;
use crate::turn::AgentExecutor;

/// The live graph one database root runs. `HarnessRuntime` in `runtime.ts:64-97`, with the two
/// members that graph holds implicitly (`defaults` + `settings`) resolved into ONE settings owner
/// plus the three credential channels the T28 rule needs.
///
/// Every collaborator is an `Arc` because the store keeps its own strong references to the same
/// objects (`Store::open` takes them by value), so a route that reaches a collaborator through the
/// runtime is touching the store's copy, not a second one - except where `knowledge.md`'s T5a
/// section records the two snapshots the store cannot advance.
pub struct HarnessRuntime {
    /// The database root this runtime persists below (`<root>/harness/state.json`).
    ///
    /// Lexically normalized, never the `\\?\`-verbatim form `std::fs::canonicalize` returns on
    /// Windows: this value appears in logs and in the store's directory names, and a printable
    /// path is what an operator can act on. The cache key is the canonical form instead.
    pub root: PathBuf,
    /// The `.env` status this runtime booted with (`environment.ts:52-57` reports it; values
    /// never live here).
    pub environment: environment::HarnessEnvironment,
    /// The EFFECTIVE provider defaults plus the deployment runtime knobs.
    ///
    /// `globalSettings` (`api.ts:822-829`): `base_url` and `model` move under `PATCH /config`, and
    /// `api_key` carries the process credential the outbound provider calls use. A write replaces
    /// the whole value, so a reader that holds the guard sees a consistent trio, never a torn pair.
    pub settings: Arc<RwLock<ServerConfig>>,
    /// The one epoch/revision plane this runtime publishes through (`runtime.ts:241`). REST and
    /// the stream read the SAME counters, which is the only way the contract's "compare a revision
    /// within one epoch" rule can hold across both transports.
    pub hub: Arc<EventHub>,
    /// The harness definition registry (`runtime.ts:168-199`), mutable because `POST/PATCH/DELETE
    /// /harnesses` write through it.
    pub registry: Arc<RwLock<HarnessRegistry>>,
    /// The trusted catalogue plus the global agent overlay (`runtime.ts:166`,
    /// `harnesses/agent-state.ts`), mutable because `PATCH/DELETE /agents/{id}` writes through it.
    pub agents: Arc<RwLock<AgentCatalogue>>,
    /// The tool catalogue and its executable half. Built AFTER the `.env` load on purpose, and
    /// therefore never rebuilt: availability is a boot-time fact (`tools/mod.rs` `Options`).
    pub tools: Arc<ToolRegistry>,
    /// The turn engine (`ports::TurnExecutor`).
    pub executor: Arc<dyn TurnExecutor>,
    /// The display projector factory (`ports::ProjectorFactory`).
    pub projector: Arc<dyn ProjectorFactory>,
    /// The session store - the single source of truth for sessions, turns, threads and
    /// transcripts. Host routes never touch a session aggregate directly.
    pub store: Arc<Store>,
    /// The credential channels `api.ts:795-833` keeps separate (`globalSettings.apiKey`,
    /// `globalSettings.hasKey`, `credentialBaseURL`) plus the boot document they reset to.
    pub credentials: Mutex<CredentialState>,
}

/// The three credential channels of `api.ts:795-838`, in one lock.
///
/// TS splits them because conflating them was the T28 defect: the advertised defaults document
/// used to hand the literal string `'configured'` to a real provider call instead of the secret.
/// Each channel answers a different question, so each is stored separately:
///
/// * `boot_*` - the shipped state, i.e. the RESET TARGET. A `null` write to any settings key
///   restores exactly this, including the credential AND the provider it belongs to
///   (`api.ts:1428-1431`: "resetting the key restores the SHIPPED credential together with the
///   provider it belongs to - not 'no key', and not the same key pointed at an endpoint a later
///   `base_url` patch happened to move").
/// * `api_key` - the ACTUAL key this process may send. `''` means none. It is never serialized to
///   a client, only its presence is (`config.settings().api_key` is the same value, kept in step
///   so the outbound paths and the advertisement cannot disagree).
/// * `has_key` - the client-visible CLAIM (`has_api_key`). `declares_credential` is true when an
///   embedding host injected the settings channel, in which case the claim is NOT authority and
///   only the real key is (`api.ts:860-868`). A standalone process has no injected channel, so
///   its claim is its own and stands on its own.
/// * `credential_base_url` - the provider the CURRENT key was issued for. Moved ONLY by an
///   explicit `api_key` write (`api.ts:831-833`), never by a `base_url` patch: a base URL move
///   changes where sessions GO, not whose secret may travel.
#[derive(Debug, Clone)]
pub struct CredentialState {
    /// The provider base URL the process booted with (before any saved `globalSettings`).
    pub boot_base_url: String,
    /// The model id the process booted with.
    pub boot_model: String,
    /// The credential the process booted with (`bootCredential.apiKey`).
    pub boot_api_key: String,
    /// The claim that booted with it (`defaults.hasApiKey`).
    pub boot_has_key: bool,
    /// Whether an injected `settings` channel states this process's credential
    /// (`api.ts:838`). A standalone start has none, so it is `false`.
    pub declares_credential: bool,
    /// The live key (`globalSettings.apiKey`).
    pub api_key: String,
    /// The live claim (`globalSettings.hasKey`).
    pub has_key: bool,
    /// The provider the live key belongs to (`credentialBaseURL`).
    pub credential_base_url: String,
}

impl CredentialState {
    /// The boot state, from the configuration the runtime resolved
    /// (`api.ts:817-838`: `bootCredential` + `globalSettings` + `credentialBaseURL`).
    #[must_use]
    pub fn boot(config: &ServerConfig) -> Self {
        let key = config.api_key.trim().to_string();
        let has_key = !key.is_empty();

        Self {
            boot_base_url: config.base_url.clone(),
            boot_model: config.model.clone(),
            credential_base_url: config.base_url.clone(),
            boot_api_key: key.clone(),
            boot_has_key: has_key,
            // The standalone host has no injected settings channel: `HARNESS_API_KEY` IS this
            // process's statement about its own credential.
            declares_credential: false,
            api_key: key,
            has_key,
        }
    }

    /// The (key, provider) pair for ONE outbound provider call (`api.ts:875-878
    /// providerCredential`), handed to the merge as the scoping source so the scope check
    /// compares against the provider the key belongs to - never against a base URL the merge may
    /// have just moved. `model` is carried for shape only; the credential rule never reads it.
    #[must_use]
    pub fn provider_credential(&self) -> HarnessRuntimeSettings {
        HarnessRuntimeSettings {
            base_url: self.credential_base_url.clone(),
            api_key: self.api_key.clone(),
            model: String::new(),
        }
    }

    /// What `defaults.has_api_key` may honestly claim (`api.ts:860-868`): the real key when this
    /// process holds one, otherwise the deployment's own claim - but only while no injected
    /// channel overrides it, because an injected `settings` object is the authoritative statement.
    #[must_use]
    pub fn advertised_has_api_key(&self) -> bool {
        !self.api_key.trim().is_empty() || (self.has_key && !self.declares_credential)
    }

    /// Applies one COMMITTED config patch to the credential channels (`api.ts:1424-1446`), in the
    /// TS order: the reset first, then an issued key, so a body that states both ends with the
    /// issued credential and its scope, exactly as the "order-independent" comment requires.
    ///
    /// `reset_key`/`issued_key` are the two flags `patchConfig` collects while validating the
    /// candidate; `next_key` is the trimmed string the body stated.
    pub fn apply_patch(
        &mut self,
        reset_key: bool,
        issued_key: bool,
        next_key: Option<&str>,
        effective_base_url: &str,
    ) {
        if reset_key {
            self.api_key = self.boot_api_key.clone();
            self.has_key = self.boot_has_key;
            self.credential_base_url = self.boot_base_url.clone();
        }

        if issued_key {
            let key = next_key.unwrap_or_default().trim().to_string();
            self.api_key = key.clone();
            self.credential_base_url = effective_base_url.to_string();
            // The claim follows a credential this process actually issued; an explicit empty
            // string means "no key" (`api.ts:1441-1445`).
            self.has_key = !key.trim().is_empty();
        }
    }
}

/// The process-wide runtime cache (`runtime.ts:291 runtimes`), keyed by the canonical database
/// root. `''` in TS is the in-memory runtime; this crate always persists, so every key is a root.
static RUNTIMES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<HarnessRuntime>>>> = OnceLock::new();

/// The cache map, created on first use. A poisoned lock is taken anyway: the map holds `Arc`s, so
/// a panic under the lock loses nothing worth recovering.
fn cache() -> &'static Mutex<BTreeMap<PathBuf, Arc<HarnessRuntime>>> {
    RUNTIMES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// The cache key for a root: the canonical path when the directory exists, the lexically
/// normalized path when it does not (`normalizeHarnessDatabaseRoot` resolves to an absolute path
/// and refuses nothing, so both spellings of one directory must land on one runtime).
///
/// `std::fs::canonicalize` is what makes `Temp/x` and `Temp/./x` share one store instead of
/// opening two documents over the same files.
#[must_use]
pub fn canonical_root(root: &Path) -> PathBuf {
    match std::fs::canonicalize(root) {
        Ok(path) => path,
        Err(_) => package_root::normalize_lexically(root),
    }
}

/// The cache key actually used: the canonical form of a root that may not exist yet, reduced to
/// the same value a later `canonicalize` would produce for it.
///
/// A verbatim `\\?\` path and its plain spelling must never sit in the map as two entries, so the
/// prefix is compared AFTER stripping it, and a relative root keeps its `..` resolution lexical.
fn cache_key(root: &Path) -> PathBuf {
    let canonical = canonical_root(root);

    // Windows `canonicalize` adds a verbatim prefix; the same directory addressed through a
    // not-yet-created subpath has no prefix at all. Keying on the stripped form unifies both.
    let text = canonical.to_string_lossy().to_string();

    match text.strip_prefix(r"\\?\") {
        Some(stripped) => PathBuf::from(stripped),
        None => canonical,
    }
}

/// Returns the runtime for `root`, building it on first use (`getHarnessRuntime`).
///
/// The cache lock is held ACROSS the build, so two concurrent first requests for one root cannot
/// each open a store over the same `state.json` (TS is single-threaded and gets this for free).
/// Nothing here awaits, so the held lock can never span a suspension point.
///
/// A failed build is NOT cached (`runtime.ts:296-308`: "A corrupt database throws on creation and
/// is NOT replaced by an empty one: every later request for that root fails clearly until the file
/// is fixed"), so a retry after fixing the file succeeds without a restart.
///
/// # Errors
///
/// [`ApiError::bad_request`] (400) when a provider or runtime setting is unusable - the
/// `SettingsError`/`ServerRuntimeError` case, which TS's dispatcher also answers 400 for - and
/// [`ApiError::internal`] (500) for anything else: an unreadable or malformed document, or a saved
/// registry this build cannot replay, both of which TS wraps in `HarnessPersistenceError` and
/// `service.ts:517-528` surfaces as a 500 with the real message.
pub fn runtime_for(root: &Path) -> Result<Arc<HarnessRuntime>, ApiError> {
    let key = cache_key(root);

    let mut guard = cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(existing) = guard.get(&key) {
        return Ok(Arc::clone(existing));
    }

    let runtime = Arc::new(build_runtime(root)?);
    guard.insert(key, Arc::clone(&runtime));

    Ok(runtime)
}

/// Drops every cached runtime. Tests use this to force a cold restart of one root (the same
/// directory read by a second runtime is how the "a saved config patch survives a restart" rule is
/// observed), and an embedder uses it to release a root it is done with.
///
/// In-flight requests keep working: each holds its own `Arc`, so this only means "the NEXT
/// request rebuilds".
pub fn clear_runtime_cache() {
    let mut guard = cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    guard.clear();
}

/// Drops one root's cached runtime, leaving every other root alone.
///
/// A test that wants to observe "a second process reading the same database" evicts ONLY its own
/// root: [`clear_runtime_cache`] would also drop the runtimes of the other tests running in
/// parallel, resetting their revision counters mid-assertion.
pub fn evict_runtime(root: &Path) {
    let mut guard = cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    guard.remove(&cache_key(root));
}

/// The cached runtime for a root, WITHOUT building one (`setHarnessRuntime`'s read side).
///
/// The shutdown checkpoint needs exactly this: a root that never served a request has no state to
/// flush, and opening a store at exit time would re-read the document it just wrote.
#[must_use]
pub fn cached_runtime(root: &Path) -> Option<Arc<HarnessRuntime>> {
    let guard = cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    guard.get(&cache_key(root)).map(Arc::clone)
}

/// Builds one runtime (`createHarnessRuntime`). The error mapping is the brief's: 400 for bad
/// provider settings (already the status `load_server_config` raises), 500 for the disk and the
/// replay.
fn build_runtime(root: &Path) -> Result<HarnessRuntime, ApiError> {
    // 1. `.env` first, and never an override (`environment.rs`, `dotenvy::from_path`).
    let environment = environment::load_harness_environment();

    // 2. Configuration + runtime knobs. A bad value is a 400 that names the variable.
    let config = config::load_server_config(&environment::harness_runtime_env())?;

    // The BOOT document, kept aside because it is the reset target `PATCH /config` nulls point at
    // (`api.ts:789-793 shipped` + `:1384-1385`, which resets to `defaults`, i.e. the environment's
    // values - NOT to whatever a saved `globalSettings` later moved them to), and because
    // `bootCredential` is the credential `api_key: null` restores (`api.ts:1428-1431`).
    let boot_config = config.clone();
    let mut effective = config;
    let boot = effective.settings();

    // 3. The durable documents, read before any session exists.
    let database = persistence::read_harness_database(root)
        .map_err(|error| ApiError::internal(error.message))?;
    let agent_records = persistence::read_harness_agent_records(root)
        .map_err(|error| ApiError::internal(error.message))?;

    // 4a. The agent overlay is replayed BEFORE the registry reads the catalogue, because a saved
    // harness row may select a profile a later retirement took out of the selectable list
    // (`runtime.ts:162-168`: the overlay is read "at the same moment as state.json").
    let mut agents = AgentCatalogue::builtin();
    agents.apply_records(agent_records);

    // 4b. The SAVED global defaults are applied on top of the boot settings (`api.ts:840-849`,
    // `store.globalSettings`). The credential is NOT part of that record - `state.json` holds no
    // secret (`persistence.ts:1241`) - so `merge_settings` re-decides it: the boot key is kept
    // only when the saved base URL is still the provider it was issued for.
    let merged = match database
        .as_ref()
        .and_then(|database| database.global_settings.as_ref())
    {
        Some(saved) => {
            let input = RunnerSettingsInput {
                base_url: saved.base_url.clone(),
                api_key: None,
                model: saved.model.clone(),
            };

            config::merge_settings(&boot, &input)?
        }
        None => boot.clone(),
    };

    effective.base_url = merged.base_url.clone();
    effective.model = merged.model.clone();
    effective.api_key = merged.api_key.clone();
    let runtime_settings = effective.settings();
    let settings = Arc::new(RwLock::new(effective));

    // 4c. The registry: shipped presets minus the tombstones, with the saved rows replayed onto
    // them. A row this build cannot replay aborts the whole start (TS's message verbatim).
    let registry = match database.as_ref() {
        Some(database) => HarnessRegistry::from_records(
            &runtime_settings,
            database.registry.clone(),
            database.deleted.clone().unwrap_or_default(),
            &agents,
        )
        .map_err(|error| {
            ApiError::internal(format!(
                "The saved harness registry could not be restored: {}",
                error.message
            ))
        })?,
        // A first boot: the shipped catalogue, with nothing tombstoned.
        None => HarnessRegistry::shipped(&runtime_settings, Vec::new()),
    };

    // 5. The collaborators. The catalogue resolves availability NOW, which is the point of the
    // ordering above; the executor and the projector are stateless factories.
    let tools = ToolRegistry::new();
    let executor = AgentExecutor::new();
    let projector = TurnProjectorFactory::new();
    let task_host = Arc::new(StoreTaskHost::new());
    let hub = Arc::new(EventHub::new());

    // The store keeps its own `Arc<HarnessRegistry>`/`Arc<AgentCatalogue>` snapshots (its fields
    // are immutable and it is not this stage's file): the session paths that resolve a harness or
    // an agent profile read the BOOT state. See `knowledge.md` "T5a" for the consequence and the
    // one-line store change that removes it.
    let store_registry = Arc::new(registry.clone());

    let store = Arc::new(Store::open(
        StoreConfig::at(root),
        runtime_settings.clone(),
        // The store keeps an immutable catalogue, so it gets a snapshot of the decisions this
        // runtime now owns: the same consequence (and the same one-line store fix) as the
        // registry snapshot above.
        Arc::new(agents.clone()),
        Arc::clone(&tools) as Arc<dyn ToolRuntime>,
        Arc::clone(&executor) as Arc<dyn TurnExecutor>,
        Arc::clone(&projector) as Arc<dyn ProjectorFactory>,
        store_registry,
        Arc::clone(&hub),
        Some(task_host),
    ));

    Ok(HarnessRuntime {
        root: package_root::normalize_lexically(root),
        environment,
        settings,
        hub,
        registry: Arc::new(RwLock::new(registry)),
        agents: Arc::new(RwLock::new(agents)),
        tools,
        executor,
        projector,
        store,
        // The boot document, not the merged one: see the `boot_config` note above.
        credentials: Mutex::new(CredentialState::boot(&boot_config)),
    })
}

impl HarnessRuntime {
    /// The effective provider trio, read under the settings lock.
    #[must_use]
    pub fn effective_settings(&self) -> HarnessRuntimeSettings {
        let guard = self
            .settings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        guard.settings()
    }

    /// The credential channels, cloned out of their lock so a caller never holds a `MutexGuard`
    /// across an await.
    #[must_use]
    pub fn credential_state(&self) -> CredentialState {
        let guard = self
            .credentials
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        guard.clone()
    }

    /// Commits the HOST half of the durable state: every live session (which only the store can
    /// serialize) plus the registry rows, the built-in tombstones and the operator's saved
    /// provider pair, in one `state.json` write.
    ///
    /// This is `sessions.ts:2675-2691 databaseRecord`, which is what the store's own `save`
    /// serializes EXCEPT for three members it cannot see from inside: `registry` (it writes the
    /// per-session frozen copies), `deleted`, and `globalSettings`. Adding those is exactly the
    /// host write's job, and the store still owns the session half, so no route ever hand-builds a
    /// document.
    ///
    /// TS's commit order is the same unit (`api.ts:765-779 commitAgentEdit`): the document first,
    /// the overlay file second. `save_agents` is therefore the second call.
    ///
    /// # Errors
    ///
    /// The persistence writer's refusal (an unwritable root), which the caller surfaces as a 500
    /// after rolling back whatever it mutated in memory.
    pub fn save(&self) -> Result<(), ApiError> {
        if !self.store.config().persist {
            return Ok(());
        }

        // Registry first, and the store lock LAST: `store/session.rs:72-74` states the order every
        // path in this crate uses (registry -> store) so two writes can never invert and deadlock.
        let (records, deleted) = {
            let registry = self
                .registry
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            (registry.to_records(), registry.deleted_names())
        };

        let global_settings = {
            let settings = self
                .settings
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            HarnessGlobalSettingsRecord {
                base_url: Some(settings.base_url.clone()),
                model: Some(settings.model.clone()),
                extra: Map::new(),
            }
        };

        let mut database = {
            let inner = self.store.lock();
            self.store.database_of(&inner)
        };

        database.registry = records;
        // TS writes the key only when a deletion exists (`sessions.ts:2688`), so an empty list
        // stays absent rather than becoming `[]`.
        database.deleted = (!deleted.is_empty()).then_some(deleted);
        // The credential is never in this document; the pair is the whole saved state.
        database.global_settings = Some(global_settings);

        persistence::write_harness_database(&self.root, &database, &Store::write_owner())
    }

    /// Commits the agent overlay to `<root>/harness/agents.json`
    /// (`persistence.ts:1679 writeHarnessAgentRecords`, reached through `api.ts:771`).
    ///
    /// Only what the operator CHANGED is written (`agent-state.ts:316-324`), never a copy of the
    /// catalogue text, so a restart replays decisions rather than prompts.
    pub fn save_agents(&self) -> Result<(), ApiError> {
        let records = {
            let agents = self
                .agents
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            agents.records()
        };

        persistence::write_harness_agent_records(&self.root, &records)
    }

    /// Replaces the agent catalogue with one rebuilt from `records` (the rollback half of
    /// `api.ts:765-777`: when the overlay write fails, the in-memory decision is put back before
    /// the refusal propagates, so no channel can disagree with it).
    ///
    /// A fresh `builtin()` is required because `apply_records` SETS entries without clearing them,
    /// so replaying onto the mutated catalogue would keep the decision that just failed.
    pub fn restore_agents(&self, records: Vec<persistence::HarnessAgentRecord>) {
        let mut next = AgentCatalogue::builtin();
        next.apply_records(records);

        let mut guard = self
            .agents
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        *guard = next;
    }

    /// Replaces the harness registry with a snapshot (the same rollback shape, for a definition
    /// write whose document commit failed).
    pub fn restore_registry(&self, registry: HarnessRegistry) {
        let mut guard = self
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        *guard = registry;
    }

    /// A snapshot of the registry, for a route that needs an undo point before mutating.
    #[must_use]
    pub fn registry_snapshot(&self) -> HarnessRegistry {
        let guard = self
            .registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        guard.clone()
    }

    /// This runtime's epoch (`api.epoch`, `runtime.ts:214-216`): one value for REST headers and
    /// the stream's `welcome`.
    #[must_use]
    pub fn epoch(&self) -> String {
        self.hub.epoch().to_owned()
    }

    /// Advances one counter and publishes the change (`api.ts:1215-1221 publish` over
    /// `events.bumpRevision` + `events.publish`).
    ///
    /// The split is the contract's (`api-events.ts:299-316`): the publisher owns its counters, so
    /// a write bumps EVERY resource it moves and only then publishes, which reports where each
    /// counter stands. A route that bumps and publishes moves a counter exactly once, and a
    /// `PageMeta.revision` can therefore never disagree with a `subscription_state` frame.
    ///
    /// `bumps` is the list of `(resource, id)` pairs to advance; the same list is what the event
    /// reports.
    pub fn commit_revisions(
        &self,
        kind: &str,
        bumps: &[(&str, Option<&str>)],
        payload: serde_json::Value,
    ) {
        for (resource, id) in bumps {
            // The advance is what the publish below reports. A route that needs the figure for a
            // response header calls `hub.bump` itself, so the value here is deliberately dropped.
            let _advanced = self.hub.bump(resource, *id);
        }

        let resources: Vec<crate::events::ResourceRef> = bumps
            .iter()
            .map(|(resource, id)| crate::events::ResourceRef::new(*resource, *id))
            .collect();

        // `publish` reports the settled counters and fans the change out synchronously; the event it
        // returns is the stream layer's business, so a REST route discards it.
        let _event = self
            .hub
            .publish(kind, &resources, None, None, None, payload);
    }
}
