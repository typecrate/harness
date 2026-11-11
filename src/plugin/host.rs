// =============================================================================
// plugin/host.rs - the plugin HOST: what is loaded, in what order, under what name.
//
// TypeScript origin: `packages/underload/harness/src/plugin/host.ts` (239 lines, read in full).
//   * `metadataOf` (`:114-117`)                     -> [`Plugin::metadata`], with `builtin` stamped
//     by the HOST rather than taken from the plugin;
//   * `resolvePluginList` (`:123-161`)              -> [`resolve_plugin_list`], the precedence
//     rules, kept separate from application so the REST listing and a test can read them without
//     running any setup();
//   * `runPlugins` (`:164-238`)                     -> [`PluginHost::run`]: per-plugin scoped
//     settings (`createPluginSettingsView`), the ATTRIBUTED report channel, the `on` wrapper that
//     carries `{ owner, api, enabled }`, and the framework enablement gate (`isEnabled`).
//
// THE PRECEDENCE RULES (`host.ts:14-35`, verbatim intent)
// -------------------------------------------------------
//  1. The auto-discovered built-ins (this crate: [`crate::plugins::BUILTIN_PLUGINS`], the hand-
//     written twin of the generated manifest) are placed FIRST, in scan order.
//  2. A host-supplied plugin that NAMES a built-in REPLACES it IN PLACE. Position is part of the
//     documented order: a replacement that ran later would change when its hooks fire relative to
//     every other plugin's.
//  3. Two built-ins with one name is a manifest bug, and two host entries with one name have no
//     honest precedence - both are the same refusal.
//  4. `builtin` is a fact about how the plugin was LOADED, so a hand-written entry cannot upgrade
//     itself to "shipped"; the UI offers a reset-to-default affordance on that flag.
//
// THE FRAMEWORK GATE, STATED ONCE HERE (`host.ts:182-197`)
// -------------------------------------------------------
// The reserved `enabled` boolean key of THIS plugin's settings view decides whether its handlers
// are delivered a hook. The view re-reads the provider on every access, so an operator's
// `PATCH /v1/harness/plugins/{plugin_id}` changes what an ALREADY RUNNING session does on its next
// turn - every session, because every harness was built with the one live provider.
//
// The resolution order is stored override -> the plugin's own declared `enabled` default -> `true`
/// for a plugin that declares no such key, and it is THE SAME READ the plugin itself performs (a
// plugin that declares `enabled: false` is off from the first turn with nothing stored, and one
// stored `true` turns both reads on at once - `host.test.ts:415-469`).
//
// THE PLUGIN IS NOT UNREGISTERED: it stays loaded, stays listed with `enabled: false`, and
// `harness.dispose` still reaches it so its own teardown runs (`hooks.rs` rule 9).
//
// WHY `setup` TAKES `&PluginApi` AND RETURNS `Result`
// --------------------------------------------------
// TS's `setup(api)` can throw, and a throwing setup must not half-register a plugin: the host
// applies plugins in order and stops at the first failure, with the registrations made so far
// still on the bus. Rust says the same thing in the type - `Err` out of [`Plugin::setup`] aborts
// the run and [`PluginHost::run`] reports which plugin failed.
// =============================================================================

use std::sync::{Arc, Mutex};

use super::context::{PluginControls, PluginRuntime, fallback_controls};
use super::diagnostics::{AttributedReporter, DiagnosticEvent, DiagnosticReporter};
use super::event::LifecycleEvent;
use super::hooks::{
    HandlerBody, HandlerCall, HandlerFailure, HandlerFuture, HandlerResult, HookBus, HookOptions,
};
use super::settings::{MapSettingsProvider, SettingsProvider, SettingsView};
use crate::plugins::{PLUGIN_ENABLED_KEY, PluginMetadata, PluginSettingsStore, metadata_of};

// ---------------------------------------------------------------------------
// The plugin definition
// ---------------------------------------------------------------------------

/// One plugin: its declared metadata and the setup that registers its hooks
/// (`HarnessPlugin`, `types/index.ts` + every `src/plugins/*.plugin.ts`).
///
/// `metadata` returns a `&'static` reference because the catalogue is a `static` in this crate
/// ([`crate::plugins::BUILTIN_PLUGINS`]) and the REST row is derived from it: a plugin cannot
/// claim a settings descriptor the build does not compile, and that is a feature - the allowlist
/// the PATCH edge enforces and the descriptor a plugin reads are one table.
pub trait Plugin: Send + Sync {
    /// The declared row: name, title, description, version, tags, hooks, settings.
    fn metadata(&self) -> &'static PluginMetadata;

    /// Registers this plugin's hooks (`plugin.setup(api)`). Called exactly once, in resolution
    /// order, by [`PluginHost::run`].
    fn setup(&self, api: &PluginApi) -> Result<(), HostError>;
}

/// A plugin definition is a compile-time thing, so printing its identity is enough for a log line -
/// and it keeps `ResolvedPlugin` derivable. (Same reasoning as `context.rs`'s seam `Debug` views.)
impl std::fmt::Debug for dyn Plugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plugin")
            .field("name", &self.metadata().name)
            .finish_non_exhaustive()
    }
}

/// A host's refusal to build a plugin list (`resolvePluginList`'s two `HarnessError`s).
#[derive(Debug, Clone, PartialEq)]
pub enum HostError {
    /// `host.ts:132` - an entry with no name cannot be scoped, gated, or listed.
    MissingName,
    /// `host.ts:148` - two built-ins with one name, or two host entries with one name.
    Duplicate { name: String },
    /// A plugin's own `setup` failed; `name` is the plugin and `message` its error text.
    SetupFailed { name: String, message: String },
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Both sentences are byte-identical to TS: the plugin list is operator-facing, and a
            // manifest bug has to say which name collided.
            Self::MissingName => f.write_str("A plugin requires a name."),
            Self::Duplicate { name } => {
                write!(f, "Plugin \"{name}\" is registered more than once.")
            }
            Self::SetupFailed { name, message } => {
                write!(f, "Plugin \"{name}\" failed to register: {message}")
            }
        }
    }
}

impl std::error::Error for HostError {}

/// One entry of the resolved list (`resolvePluginList`'s `{ plugin, builtin }` pair).
pub struct ResolvedPlugin<'a> {
    /// The definition, borrowed: the host keeps no copy, because after `setup` the only facts that
    /// matter are the registrations on the bus and the metadata in the catalogue.
    pub plugin: &'a dyn Plugin,
    /// Provenance, stamped by the loader (`host.ts:110-117`) - never read from the plugin itself.
    pub builtin: bool,
}

/// Manual `Debug` rather than a derive: `plugin` is a `dyn` reference with a borrow lifetime, and
/// `#[derive(Debug)]` would demand `'a: 'static` for it. The name plus the provenance flag is the
/// whole identity of the entry, which is all a log line has to say.
impl std::fmt::Debug for ResolvedPlugin<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedPlugin")
            .field("name", &self.plugin.metadata().name)
            .field("builtin", &self.builtin)
            .finish()
    }
}

/// Built-ins plus host plugins, resolved into the single ordered list to APPLY.
///
/// Kept separate from application so a test (and the server's plugin listing) reads the same
/// precedence rules without running any setup() - exactly the split `host.ts:119-122` describes.
///
/// # Errors
///
/// [`HostError::MissingName`] for an entry with no name, and [`HostError::Duplicate`] for the two
/// illegal collisions (built-in vs built-in, host vs host). A host entry naming a built-in is NOT
/// an error: it replaces it in place.
pub fn resolve_plugin_list<'a>(
    builtins: &[&'a dyn Plugin],
    plugins: &[&'a dyn Plugin],
) -> Result<Vec<ResolvedPlugin<'a>>, HostError> {
    let mut resolved: Vec<ResolvedPlugin<'a>> = Vec::new();

    // The order matters twice over: built-ins first, then the host's own entries, and within each
    // group the declaration order the catalogue is written in (the pane renders it as-is).
    for (plugin, builtin) in builtins
        .iter()
        .map(|entry| (*entry, true))
        .chain(plugins.iter().map(|entry| (*entry, false)))
    {
        let name = plugin.metadata().name;

        if name.is_empty() {
            return Err(HostError::MissingName);
        }

        let Some(existing) = resolved.iter().position(|entry| entry.plugin.metadata().name == name)
        else {
            resolved.push(ResolvedPlugin { plugin, builtin });
            continue;
        };

        // The ONLY legal collision is a host plugin replacing a discovered built-in. A built-in
        // naming another built-in is a manifest bug, and two host entries with one name have no
        // honest precedence - both are errors, because a silent drop would hide the bug.
        if builtin || !resolved[existing].builtin {
            return Err(HostError::Duplicate {
                name: name.to_owned(),
            });
        }

        // Replaced IN PLACE: the position is part of the documented order, and a replacement that
        // ran later would change when its hooks fire relative to every other plugin's.
        resolved[existing] = ResolvedPlugin { plugin, builtin };
    }

    Ok(resolved)
}

// ---------------------------------------------------------------------------
// The api handed to one plugin's setup
// ---------------------------------------------------------------------------

/// The plugin-scoped api handed to [`Plugin::setup`] (`host.ts:199-231`'s `api`).
///
/// Three things are baked in, and none of them is the plugin's choice:
///   * `owner` - the plugin name, so a registration and a diagnostic are attributed;
///   * the SCOPED settings view - so `settings.boolean('enabled')` cannot read another plugin's
///     namespace, and the fallback default comes from what THIS plugin declares;
///   * the live enablement gate - so every registration this api makes is gated by the same stored
///     key the operator's switch writes.
/// `Clone` so a plugin's setup can hand its own scoped api to the closures it registers, exactly
/// as TS closes over `api` (async-ping.plugin.ts:112-133). Every field is an `Arc`, so a clone is
/// four reference-count bumps and there is no second copy of any state to keep in sync.
#[derive(Clone)]
pub struct PluginApi {
    owner: Arc<str>,
    controls: Arc<dyn PluginControls>,
    bus: Arc<HookBus>,
    enabled: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl std::fmt::Debug for PluginApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginApi")
            .field("owner", &self.owner.as_ref())
            .field("live", &self.bus.live().len())
            .finish()
    }
}

impl PluginApi {
    /// This plugin's name. A setup that needs its own name for a log line reads it here rather
    /// than re-deriving it from the metadata.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// The plugin's settings, live on every read (`api.settings`).
    #[must_use]
    pub fn settings(&self) -> Option<SettingsView<'_>> {
        self.controls.settings()
    }

    /// The shared harness handle (`api.threads` / `api.agents` / `api.async`).
    #[must_use]
    pub fn runtime(&self) -> &dyn PluginRuntime {
        self.controls.runtime()
    }

    /// `api.report.diagnostic`, attributed to this plugin (`host.ts:207-209`). A policy report's
    /// `owner` is overwritten HERE, so a plugin cannot forge a report about another plugin.
    pub fn report(&self, event: DiagnosticEvent) {
        self.controls.report_diagnostic(event);
    }

    /// The bus, for a plugin that needs to reason about the whole surface (`harness.dispose` sweeps
    /// and the Plugins pane's declared-vs-subscribed comparison are the two real uses).
    #[must_use]
    pub fn bus(&self) -> &Arc<HookBus> {
        &self.bus
    }

    /// The live enablement gate this api registers behind (`host.ts:197`). Exposed so a plugin that
    /// needs the same answer OUTSIDE a dispatch - async-ping's timer body checks it before waking a
    /// conversation - reads the one key the framework gate consults, not a second copy of the rule.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        (self.enabled)()
    }

    /// `api.on(event, handler, options)` — the registration shape a plugin's `setup` actually
    /// writes, with this api's owner injected and the bus's refusal folded into
    /// [`HostError::SetupFailed`] under the same owner name.
    ///
    /// [`Self::on_sync`] is the raw call for a caller that wants the registration id or the
    /// specific [`super::hooks::RegistrationError`]; `setup` bodies want neither, they want `?`.
    pub fn on(
        &self,
        name: &str,
        handler: impl Fn(&HandlerCall) -> HandlerResult + Send + Sync + 'static,
        options: HookOptions,
    ) -> Result<(), HostError> {
        let owner = self.owner.to_string();
        self.on_sync(name, handler, options)
            .map(|_| ())
            .map_err(|error| HostError::SetupFailed {
                name: owner,
                message: error.0,
            })
    }

    /// The deferred twin of [`Self::on`]: registers an ASYNC handler - "deferred" because the body
    /// returns a future the round awaits, which is the property that decides whether a budget timer
    /// is armed at all (see `hooks.rs`'s `HandlerBody`).
    pub fn on_deferred(
        &self,
        name: &str,
        handler: impl Fn(Arc<HandlerCall>) -> HandlerFuture + Send + Sync + 'static,
        options: HookOptions,
    ) -> Result<(), HostError> {
        let owner = self.owner.to_string();
        self.on_async(name, handler, options)
            .map(|_| ())
            .map_err(|error| HostError::SetupFailed {
                name: owner,
                message: error.0,
            })
    }

    /// `api.on(event, handler, options)` for a SYNCHRONOUS handler.
    ///
    /// The author's `{ priority, timeoutMs }` ride through unchanged; the bus owns every default
    /// (`hooks.ts:214-219`), so an author who passes nothing gets exactly what every plugin has
    /// always got: priority 0, unbounded, registration order.
    pub fn on_sync(
        &self,
        name: &str,
        handler: impl Fn(&HandlerCall) -> HandlerResult + Send + Sync + 'static,
        options: HookOptions,
    ) -> Result<u64, super::hooks::RegistrationError> {
        self.register(name, HandlerBody::Sync(Arc::new(handler)), options)
    }

    /// `api.on(event, async handler, options)`.
    pub fn on_async(
        &self,
        name: &str,
        handler: impl Fn(Arc<HandlerCall>) -> HandlerFuture + Send + Sync + 'static,
        options: HookOptions,
    ) -> Result<u64, super::hooks::RegistrationError> {
        self.register(name, HandlerBody::Async(Arc::new(handler)), options)
    }

    /// The one registration path: owner, scoped api and gate are injected, and the caller's options
    /// fill everything else (`host.ts:220-229`, `{ owner, api, enabled, ...options }`).
    fn register(
        &self,
        name: &str,
        body: HandlerBody,
        mut options: HookOptions,
    ) -> Result<u64, super::hooks::RegistrationError> {
        // The host's identity always wins: `options.owner` is what the caller passed, and a plugin
        // that named another plugin here would otherwise attribute its hooks to someone else.
        options.owner = self.owner.to_string();
        options.controls = Some(Arc::clone(&self.controls));

        if options.enabled.is_none() {
            options.enabled = Some(Arc::clone(&self.enabled));
        }

        self.bus.register(name, body, options)
    }

    /// The dotted name of one event, for a `setup` that builds its `on` key from the catalogue
    /// rather than spelling it (`LifecycleEvent::as_str`).
    #[must_use]
    pub const fn event_name(event: LifecycleEvent) -> &'static str {
        event.as_str()
    }
}

/// A failure a setup reports, so the host can name the plugin that failed.
#[must_use]
pub fn setup_failed(name: &str, message: impl Into<String>) -> HostError {
    HostError::SetupFailed {
        name: name.to_owned(),
        message: message.into(),
    }
}

/// The shorthand a setup uses for a failure it has no message for.
#[must_use]
pub fn handler_failure(message: impl Into<String>) -> HandlerFailure {
    HandlerFailure::new(message)
}

// ---------------------------------------------------------------------------
// The scoped controls
// ---------------------------------------------------------------------------

/// The per-plugin [`PluginControls`]: a settings provider scoped to one owner, an ATTRIBUTED
/// reporter, and the shared runtime (`host.ts:180-209`).
struct ScopedControls {
    owner: String,
    provider: Arc<dyn SettingsProvider>,
    reporter: Arc<dyn DiagnosticReporter>,
    runtime: Arc<dyn PluginRuntime>,
}

impl PluginControls for ScopedControls {
    fn owner(&self) -> &str {
        &self.owner
    }

    fn settings(&self) -> Option<SettingsView<'_>> {
        Some(SettingsView::live(self.provider.as_ref()))
    }

    fn settings_provider(&self) -> Option<Arc<dyn SettingsProvider>> {
        Some(Arc::clone(&self.provider))
    }

    fn report_diagnostic(&self, event: DiagnosticEvent) {
        // `host.ts:208` - `{ ...report, owner: plugin.name }`: the attribution is not a default, it
        // is an override, and it happens here so every authorable kind is stamped the same way.
        self.reporter.report(event);
    }

    fn runtime(&self) -> &dyn PluginRuntime {
        self.runtime.as_ref()
    }
}

// ---------------------------------------------------------------------------
// The host
// ---------------------------------------------------------------------------

/// What a completed [`PluginHost::run`] reports (`HarnessPluginHostResult`: `names`, `metadata`).
#[derive(Debug, Clone)]
pub struct PluginHostReport {
    /// The applied plugins, in the order they run (`host.ts:171`'s `names`).
    names: Vec<String>,
    /// Declared metadata for the UI's Plugins pane, same order as `names` (`harness.ts:11327`).
    metadata: Vec<&'static PluginMetadata>,
    /// Whether each applied plugin was loaded from the shipped catalogue.
    builtin: Vec<bool>,
    /// The bus every plugin registered on, kept so the server can report the LIVE hook surface.
    bus: Arc<HookBus>,
}

impl PluginHostReport {
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.names
    }

    #[must_use]
    pub fn metadata(&self) -> &[&'static PluginMetadata] {
        &self.metadata
    }

    #[must_use]
    pub fn is_builtin(&self, plugin: &str) -> bool {
        self.names
            .iter()
            .position(|name| name == plugin)
            .is_some_and(|at| self.builtin[at])
    }

    /// The hooks one plugin ACTUALLY subscribed to, in the order it subscribed (`api`-side truth:
    /// declared `metadata.hooks` is the intent, this is the fact).
    ///
    /// This is where the REST row's `hooks` field comes from: the captured TS catalogue answers
    /// `async-ping` with its six SUBSCRIBED events (`async.pending`, `async.drained`, `run.failed`,
    /// `thread.stopped`, `thread.deleted`, `harness.dispose`) and NOT with its declared
    /// `['thread.*', 'run.*', ...]` patterns, because `harness.ts:11885` reports
    /// `pluginMetadata` while the row's `hooks` is the bus's own `live()` surface.
    #[must_use]
    pub fn subscribed(&self, plugin: &str) -> Vec<&'static str> {
        // The bus owns the ordering rule (registration order, wildcard expanded in place); this is
        // the one call the row needs.
        self.bus.live_for(plugin)
    }

    /// Whether this plugin has any live subscription at all (the Plugins pane's inert check).
    #[must_use]
    pub fn is_live(&self, plugin: &str) -> bool {
        !self.subscribed(plugin).is_empty()
    }
}

/// The plugin host (`runPlugins`).
///
/// Stateless: [`Self::run`] is the whole type. It exists so a caller can name the operation without
/// naming the bus, the store, and the reporter separately.
#[derive(Debug, Clone, Copy, Default)]
pub struct PluginHost;

impl PluginHost {
    /// Resolves and APPLIES every plugin, in order, onto `bus`.
    ///
    /// * `builtins` - the shipped plugins (the Rust twin of `BUILTIN_PLUGINS`);
    /// * `plugins` - the host's own entries, which may replace a built-in by name;
    /// * `bus` - the one hook bus this harness owns;
    /// * `store` - the shared plugin-settings store: the source of both the plugin's own reads and
    ///   the framework gate, which is what makes them ONE value (`server/plugins.ts`'s provider);
    /// * `reporter` - the diagnostic sink every policy report reaches, wrapped per plugin with that
    ///   plugin's own owner;
    /// * `runtime` - the shared harness handle (threads, agents, the async board).
    ///
    /// # Errors
    ///
    /// The first resolution or setup failure, with the plugin that failed named. Registrations made
    /// before the failure stay on the bus - the same half-applied state TS leaves, which the caller
    /// treats as "this harness did not start".
    pub fn run(
        builtins: &[&dyn Plugin],
        plugins: &[&dyn Plugin],
        bus: &Arc<HookBus>,
        store: Arc<Mutex<PluginSettingsStore>>,
        reporter: Arc<dyn DiagnosticReporter>,
        runtime: Arc<dyn PluginRuntime>,
    ) -> Result<PluginHostReport, HostError> {
        let resolved = resolve_plugin_list(builtins, plugins)?;

        let mut names = Vec::with_capacity(resolved.len());
        let mut metadata = Vec::with_capacity(resolved.len());
        let mut builtin = Vec::with_capacity(resolved.len());

        for entry in resolved {
            let declared = entry.plugin.metadata();
            let name = declared.name;

            if name.is_empty() {
                return Err(HostError::MissingName);
            }

            // SCOPED SETTINGS (`host.ts:177-180`): the scoped view needs the DECLARED descriptors,
            // so a host with no settings document still gets a sensible value. `MapSettingsProvider`
            // resolves through the shared store, which is the whole reason a PATCH reaches a running
            // session without a rebuild.
            //
            // The descriptors come from the PLUGIN, not from the build-time catalogue: a host-supplied
            // plugin is not in that table, and TS resolves its declared defaults from the loaded
            // plugin. For a built-in the two are the same list, so this changes nothing there.
            let provider: Arc<dyn SettingsProvider> = Arc::new(MapSettingsProvider::with_declared(
                Arc::clone(&store),
                name,
                declared.settings,
            ));

            // THE ATTRIBUTED REPORT CHANNEL (`host.ts:202-209`): the base reporter is shared by
            // every plugin and reports as the framework; a policy report has to name the plugin that
            // made it, or an operator reading one stream cannot tell the compactor's decisions from a
            // host subscription's.
            let plugin_reporter: Arc<dyn DiagnosticReporter> =
                Arc::new(AttributedReporter::new(Arc::clone(&reporter), name.to_owned()));

            let controls: Arc<dyn PluginControls> = Arc::new(ScopedControls {
                owner: name.to_owned(),
                provider: Arc::clone(&provider),
                reporter: plugin_reporter,
                runtime: Arc::clone(&runtime),
            });

            // THE FRAMEWORK GATE (`host.ts:197`): `settings.boolean('enabled', true)`, read live on
            // every dispatch. It closes over the same provider the plugin's own `api.settings`
            // reads, which is what makes one stored value answer both questions.
            let gate_provider = Arc::clone(&provider);
            let enabled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
                SettingsView::live(gate_provider.as_ref()).boolean(PLUGIN_ENABLED_KEY, true)
            });

            let api = PluginApi {
                owner: Arc::from(name),
                controls: if name.is_empty() {
                    fallback_controls()
                } else {
                    controls
                },
                bus: Arc::clone(bus),
                enabled,
            };

            entry
                .plugin
                .setup(&api)
                .map_err(|error| HostError::SetupFailed {
                    name: name.to_owned(),
                    message: error.to_string(),
                })?;

            names.push(name.to_owned());
            metadata.push(lookup_metadata(name).unwrap_or(declared));
            builtin.push(entry.builtin);
        }

        Ok(PluginHostReport {
            names,
            metadata,
            builtin,
            bus: Arc::clone(bus),
        })
    }
}

/// The catalogue's row for one name, which is the same `static` the REST surface serves.
///
/// A host-supplied plugin that is NOT in the catalogue carries its own metadata, and that is the
/// one case this returns `None`: the PATCH allowlist for it is its own declared settings, exactly
/// as `server/plugins.ts` resolves them from the plugin rather than from a build-time table.
fn lookup_metadata(name: &str) -> Option<&'static PluginMetadata> {
    metadata_of(name)
}
