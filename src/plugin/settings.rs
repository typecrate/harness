// =============================================================================
// plugin/settings.rs - the plugin-facing settings contract (W2-B2).
//
// TS SPEC: `packages/underload/harness/src/plugin/settings.ts` (`createSettingsView`, 126 lines)
// plus the provider factory the host passes to it (`host.ts:415-427`). Both were read in full; the
// port is 1:1 on the resolution rules, and the two places Rust must differ are marked.
//
// WHAT THE HANDLER ACTUALLY GETS
//   A handler receives `context: &mut dyn TurnControls`; `context.settings()` returns a
//   `SettingsView` built from THIS turn's payload snapshot. The view resolves through the JS chain,
//   in order:
//     1. the payload's own `settings` (a per-turn snapshot - a mid-run flip is NOT seen),
//     2. the provider's STORED value (the durable override, read live),
//     3. the plugin's DECLARED default,
//     4. the caller's fallback.
//   Then the declared rule COERCES the winner. That is why a plain `.get()` is not enough: a stale
//   or out-of-bounds stored value must land on the rule's clamp, not on whatever the operator typed.
//
// THE LIVE PART IS DELIBERATE
//   `get*` reads the provider on EVERY call (the JS view's `resolveStored()` is a closure over the
//   live store). TS relies on that for the compaction self-disable (`settings.set('mode','exclude')`
//   then `context.settings.get('mode') === 'exclude'`) and for the trimmer's once-per-turn
//   fingerprint. A caller that wants a stable read across an await takes `changed_since` and
//   re-reads, exactly as JS does - `changed_since` is the cheap "has anything moved" probe that
//   answers the async-staleness question without re-resolving every key.
//
//   The one Rust-only refinement: a read-through provider is a LOCK, and a plugin handler that
//   reports a diagnostic or writes a setting re-enters the same store. So the view resolves stored
//   values THROUGH THE PROVIDER (which owns its locking) and never through a guard the view holds.
//   No lock is ever held across a plugin callback here - the same rule the bus follows (hooks.rs).
//
// THE WRITE PATH IS THE STORE'S, NOT THIS FILE'S
//   `set_own` does NOT validate. It stamps the owner and hands the write to the provider, because
//   `crate::plugins::PluginSettingsStore::update` is already the single source of truth for the
//   allowlist, the type checks, the clamp, the `null`-means-reset rule, and the exact 400/404
//   sentences. Re-implementing them here is how two surfaces start disagreeing.
// =============================================================================

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde_json::{Map, Value};

use crate::contract::PluginSettingType;
use crate::plugins::{
    PLUGIN_ENABLED_KEY, PluginSettingDescriptor, PluginSettingsStore, SettingValue,
    effective_descriptor_of, effective_settings_of, number_from_text,
};

/// Where a plugin's settings come from, and where a write goes.
///
/// The integrator implements this over `SessionStore` (`plugin_values` + `update_plugin_settings`);
/// [`MapSettingsProvider`] is the in-memory twin, and the reference implementation for tests.
pub trait SettingsProvider: Send + Sync {
    /// The plugin this provider is scoped to. `SettingsView` is meaningless without it: it is the
    /// key that selects both the stored values and the declared rules.
    fn owner(&self) -> &str;

    /// The descriptors THIS plugin declared, which for a host-supplied plugin are NOT in the
    /// build-time catalogue ([`crate::plugins::settings_of`] answers only what this binary ships).
    ///
    /// A default of "nothing extra" is honest: the store's own effective list is the whole story for
    /// a built-in, and the two are identical for every shipped plugin. The host passes the plugin's
    /// own row here so a declared `enabled: false` default is honoured from the first turn with
    /// nothing stored (`host.test.ts:415-440`), which is the case a catalogue-only lookup misses.
    fn declared(&self) -> &'static [PluginSettingDescriptor] {
        &[]
    }

    /// The durable overrides for this owner, as a JSON object (`settings.ts`'s `resolveStored()`).
    /// A missing plugin answers an empty object, never an error.
    fn stored(&self) -> Map<String, Value>;

    /// A cheap change probe: two equal tokens mean "nothing moved since the last look".
    ///
    /// The JS host has no such probe (it re-reads the store), so this is additive - it exists so the
    /// trimmer's once-per-turn fingerprint and an async handler's staleness check do not have to
    /// clone the whole map. The default implementation hashes the stored map's TEXT, which is
    /// correct (a change always moves the text) without requiring an implementation.
    fn revision(&self) -> u64 {
        let stored = self.stored();
        let text = serde_json::to_string(&stored).unwrap_or_default();
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a, any stable hasher will do
        for byte in text.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }

        hash
    }

    /// Applies one authored value and returns what was stored, or the store's own refusal message.
    ///
    /// Returning the APPLIED value (not the submitted one) is what lets a diagnostic report `to`
    /// truthfully after a clamp. `Ok(None)` means "reset": the override was dropped so the declared
    /// default applies again.
    fn write(&self, key: &str, value: Option<&Value>) -> Result<Option<Value>, String>;
}

/// The reference provider: a shared [`PluginSettingsStore`] plus an owner.
///
/// Everything validating - the allowlist, the type checks, the reserved `enabled` key, the exact
/// sentences - is the store's own code, so a test that passes here passes on the real store.
#[derive(Debug, Clone)]
pub struct MapSettingsProvider {
    owner: Arc<str>,
    store: Arc<Mutex<PluginSettingsStore>>,
    /// The plugin's OWN declared descriptors, empty for a built-in whose row the catalogue already
    /// holds. See [`SettingsProvider::declared`].
    declared: &'static [PluginSettingDescriptor],
}

impl MapSettingsProvider {
    #[must_use]
    pub fn new(store: Arc<Mutex<PluginSettingsStore>>, owner: &str) -> Self {
        Self {
            owner: Arc::from(owner),
            store,
            declared: &[],
        }
    }

    /// The host's constructor: the same store, plus the descriptors of the plugin being set up.
    #[must_use]
    pub fn with_declared(
        store: Arc<Mutex<PluginSettingsStore>>,
        owner: &str,
        declared: &'static [PluginSettingDescriptor],
    ) -> Self {
        Self {
            owner: Arc::from(owner),
            store,
            declared,
        }
    }

    /// A provider over a fresh store - enough for a handler that only reads declared defaults.
    #[must_use]
    pub fn with_defaults(owner: &str) -> Self {
        Self::new(Arc::new(Mutex::new(PluginSettingsStore::default())), owner)
    }

    /// Seeds one stored override, bypassing the allowlist (a test fixture convenience).
    pub fn seed(&self, key: &str, value: SettingValue) {
        let mut store = self.lock();
        store.put(self.owner_ref(), key, value);
    }

    fn owner_ref(&self) -> &str {
        &self.owner
    }

    fn lock(&self) -> MutexGuard<'_, PluginSettingsStore> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl SettingsProvider for MapSettingsProvider {
    fn owner(&self) -> &str {
        self.owner_ref()
    }

    fn declared(&self) -> &'static [PluginSettingDescriptor] {
        self.declared
    }

    fn stored(&self) -> Map<String, Value> {
        // The RAW overrides, not `values()`: `values` is the declared-only ROW projection, and the
        // framework gate reads the reserved `enabled` key of a plugin that never declared it
        // (async-ping). Feeding it the projection would make an operator's switch invisible to the
        // gate that consults it.
        self.lock().stored_overrides(self.owner_ref())
    }

    fn write(&self, key: &str, value: Option<&Value>) -> Result<Option<Value>, String> {
        // The store is the validator, so a plugin write is the SAME patch shape the HTTP surface
        // uses. `null` (i.e. `value == None`) is the reset; TS's `null` provider behaves the same.
        let mut patch = Map::new();
        patch.insert(
            key.to_owned(),
            value.cloned().unwrap_or(Value::Null),
        );

        let applied = {
            let mut store = self.lock();
            match store.update(self.owner_ref(), &patch) {
                Ok(()) => store
                    .get(self.owner_ref(), key)
                    .map(|value| value.to_json()),
                Err(error) => return Err(error.message.clone()),
            }
        };

        Ok(applied)
    }
}

/// The read side of the contract handed to a handler.
///
/// `data` is the payload snapshot for THIS turn; the provider is consulted live for everything the
/// snapshot does not pin.
#[derive(Clone)]
pub struct SettingsView<'a> {
    owner: &'a str,
    /// The per-turn snapshot (`payload.settings`). `None` = "this event carries no snapshot", which
    /// is the JS `context.settings` path: straight to the live store.
    data: Option<&'a Map<String, Value>>,
    provider: Option<&'a dyn SettingsProvider>,
    /// The estimate the view reports for the "should I act at all" guard. The JS view gets this from
    /// the live context; here it is a plain field so the view has no context dependency and can be
    /// built for a handler that never touches the context.
    estimate: f64,
    /// Entries observed at view creation - the trimmer's `contextEntries` field.
    entries: usize,
}

/// Manual `Debug` rather than a derive: the provider is a `&'a dyn` reference, and a derive would
/// demand `'a: 'static` to print it. What an operator debugging a plugin read needs is the scope
/// and the two live-mode flags, which is exactly what this prints.
impl std::fmt::Debug for SettingsView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingsView")
            .field("owner", &self.owner)
            .field("has_snapshot", &self.data.is_some())
            .field("live", &self.provider.is_some())
            .field("estimate", &self.estimate)
            .field("entries", &self.entries)
            .finish()
    }
}

impl<'a> SettingsView<'a> {
    /// A live view: every read resolves through the provider, no per-turn snapshot.
    #[must_use]
    pub fn live(provider: &'a dyn SettingsProvider) -> Self {
        Self {
            owner: provider.owner(),
            data: None,
            provider: Some(provider),
            estimate: 0.0,
            entries: 0,
        }
    }

    /// The per-turn view a handler receives: the payload snapshot first, then the live store.
    #[must_use]
    pub fn from_payload(
        provider: Option<&'a dyn SettingsProvider>,
        owner: &'a str,
        data: Option<&'a Map<String, Value>>,
        estimate: f64,
        entries: usize,
    ) -> Self {
        Self {
            owner,
            data,
            provider,
            estimate,
            entries,
        }
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        self.owner
    }

    /// The estimate in force when this turn started - the number the "did anything change" guard
    /// compares against (`settings.ts` `estimateTokens`).
    #[must_use]
    pub fn estimate_tokens(&self) -> f64 {
        self.estimate
    }

    /// How many conversation entries this view was built with.
    #[must_use]
    pub fn context_entries(&self) -> usize {
        self.entries
    }

    /// Whether the operator has stored an override for one key at all - the trimmer's fingerprint
    /// question ("is this key even mine to read?").
    #[must_use]
    pub fn has_stored(&self, key: &str) -> bool {
        self.stored_raw(key).is_some()
    }

    /// The change probe: compare two of these to learn whether ANY stored value moved since the last
    /// look, without re-resolving keys.
    #[must_use]
    pub fn changed_since(&self, previous: u64) -> bool {
        self.provider.map_or(previous != 0, |provider| provider.revision() != previous)
    }

    /// The declared rule for one key, through the EFFECTIVE overlay, so the reserved `enabled` key of
    /// a plugin that never declared it still resolves (`PluginSettingType::Boolean`).
    #[must_use]
    pub fn declared_type(&self, key: &str) -> Option<PluginSettingType> {
        effective_descriptor_of(self.owner, key).map(|descriptor| descriptor.kind)
    }

    /// Every key this plugin may read, declared order first and the reserved `enabled` included
    /// exactly once (`effective_settings_of`).
    #[must_use]
    pub fn declared_keys(&self) -> Vec<&'static str> {
        effective_settings_of(self.owner)
            .into_iter()
            .map(|descriptor| descriptor.key)
            .collect()
    }

    /// The raw winner of the resolution chain, BEFORE coercion: snapshot, then stored, then declared
    /// default, then the caller's fallback. An explicit JSON `null` in the snapshot counts as
    /// ABSENT - it is the reset spelling, not a value (`settings.ts:47`, `data[key] ?? ...`).
    #[must_use]
    pub fn raw(&self, key: &str, fallback: Option<&Value>) -> Option<Value> {
        if let Some(value) = self.data.and_then(|data| data.get(key)).filter(|value| !value.is_null())
        {
            return Some((*value).clone());
        }

        if let Some(value) = self.stored_raw(key) {
            return Some(value);
        }

        if let Some(default) = self.declared_default(key) {
            return Some(default);
        }

        fallback.cloned()
    }

    /// Resolves AND coerces one key through the declared rule.
    ///
    /// `fallback` is the caller's last resort, already a JSON value; for a typed reader the fallback
    /// goes through the same rule as the winner, which is what the JS `fallbackSettings` map does.
    #[must_use]
    pub fn value(&self, key: &str, fallback: Option<&Value>) -> Value {
        let Some(rule) = effective_descriptor_of(self.owner, key) else {
            // No rule: nothing to coerce with, so the winner travels untouched. `settings.ts` falls
            // into its string branch for an undeclared key, which is a `String(..)` cast; keeping
            // the raw value instead is the Rust choice because the crate's typed readers (a number
            // knob) would otherwise be fed text. It only differs for a key the plugin never
            // declared, where the caller already has a fallback.
            return self
                .raw(key, fallback)
                .unwrap_or_else(|| Value::String(String::new()));
        };

        let winner = self.raw(key, fallback);
        self.coerce_with(&rule.kind, rule.min, rule.max, winner.as_ref())
    }

    /// A number knob: resolves, coerces, and answers `None` for anything that is not a number.
    #[must_use]
    pub fn number(&self, key: &str, fallback: f64) -> Option<f64> {
        let fallback = Value::from(fallback);
        match self.value(key, Some(&fallback)) {
            Value::Number(number) => number.as_f64(),
            _ => None,
        }
    }

    /// A text knob. A declared string rule with no `max` still trims: that is the JS behaviour, and
    /// every shipped text knob is a path or a model id.
    #[must_use]
    pub fn string(&self, key: &str, fallback: &str) -> String {
        let fallback = Value::String(fallback.to_owned());
        match self.value(key, Some(&fallback)) {
            Value::String(text) => text,
            Value::Null => String::new(),
            // `String(source)` for a number/bool winner.
            other => value_to_text(&other),
        }
    }

    /// A boolean knob, including the JS text spelling: any non-blank string that is not exactly
    /// `false` (case-insensitive) is TRUE.
    #[must_use]
    pub fn boolean(&self, key: &str, fallback: bool) -> bool {
        let fallback = Value::Bool(fallback);
        matches!(self.value(key, Some(&fallback)), Value::Bool(true))
    }

    /// The plugin's own write: stamp the owner, hand it to the provider, and surface the store's own
    /// refusal verbatim. `Ok(applied)` carries what the store actually kept (`None` = reset).
    pub fn set_own(&self, key: &str, value: Option<&Value>) -> Result<Option<Value>, String> {
        let provider = self.provider.ok_or_else(|| {
            format!(
                "Plugin '{self_owner}' has no settings provider, so '{key}' cannot be written.",
                self_owner = self.owner
            )
        })?;

        provider.write(key, value)
    }

    // -- internals ----------------------------------------------------------

    /// The stored override for one key, live through the provider. An empty stored map for an
    /// unknown owner is the `None` case.
    fn stored_raw(&self, key: &str) -> Option<Value> {
        let provider = self.provider?;
        provider.stored().get(key).filter(|value| !value.is_null()).cloned()
    }

    /// The declared default, already clamped the way the JS `fallbackSettings` builder clamps.
    fn declared_default(&self, key: &str) -> Option<Value> {
        // The plugin's OWN descriptor wins: a host-supplied plugin is not in the build-time
        // catalogue, and TS resolves its defaults from the loaded plugin (`plugin/settings.ts`).
        let descriptor = self
            .provider
            .into_iter()
            .flat_map(|provider| provider.declared().iter())
            .find(|descriptor| descriptor.key == key)
            .or_else(|| effective_descriptor_of(self.owner, key))?;
        let default = match descriptor.default {
            crate::plugins::DeclaredDefault::Number(value) => Value::from(value),
            crate::plugins::DeclaredDefault::Text(text) => Value::String(text.to_owned()),
            crate::plugins::DeclaredDefault::Flag(flag) => Value::Bool(flag),
        };

        Some(self.coerce_with(
            &descriptor.kind,
            descriptor.min,
            descriptor.max,
            Some(&default),
        ))
    }

    /// The rule-driven coercion, verbatim from `settings.ts:52-84`.
    fn coerce_with(
        &self,
        kind: &PluginSettingType,
        min: Option<f64>,
        max: Option<f64>,
        source: Option<&Value>,
    ) -> Value {
        match kind {
            PluginSettingType::Number => {
                let Some(source) = source else {
                    return Value::Null;
                };

                match source {
                    // A real number is CLAMPED into the rule's range, never rejected.
                    Value::Number(number) => number.as_f64().map_or(Value::Null, |value| {
                        Value::from(clamp(value, min, max))
                    }),
                    // Text goes through `Number(trimmed)`: a blank text falls to the rule-less
                    // default path (the JS `return fallbackSettings[key]`, i.e. the declared
                    // default), which the caller already resolved - so an unparsable text answers
                    // the source unchanged and the typed reader decides.
                    Value::String(text) => {
                        let trimmed = text.trim();
                        if trimmed.is_empty() {
                            return Value::Null;
                        }

                        number_from_text(trimmed).map_or(Value::Null, |value| {
                            Value::from(clamp(value, min, max))
                        })
                    }
                    // A boolean/null/object for a number rule: JS `Number(..)` would give 0 / NaN.
                    // The JS branch returns the FALLBACK there, which the caller has already placed
                    // in `source`; answering Null lets that fallback win upstream.
                    _ => Value::Null,
                }
            }
            PluginSettingType::Boolean => match source {
                Some(Value::Bool(flag)) => Value::Bool(*flag),
                Some(Value::String(text)) => {
                    let trimmed = text.trim();
                    if trimmed.is_empty() {
                        return Value::Bool(false); // `Boolean('')`
                    }

                    Value::Bool(trimmed.to_lowercase() != "false") // the documented text spelling
                }
                Some(Value::Null) => Value::Bool(false),
                Some(other) => Value::Bool(!other.is_null()), // `Boolean(source)`
                None => Value::Bool(false),
            },
            // The declared TYPE spelling for a text setting is `String` on the wire
            // (`contract::PluginSettingType`, mirroring `HarnessPluginSettingType = 'number' |
            // 'string' | 'boolean'`, types/index.ts:3762) - there is no `Text` variant, and the
            // trim/length rule below is what `settings.ts:76-83` does for it.
            PluginSettingType::String => match source {
                Some(Value::String(text)) => {
                    let mut trimmed = text.trim().to_owned();
                    // `max` is CHARACTER length in JS; a Rust `String` is already a char sequence.
                    if let Some(max) = max
                        && trimmed.chars().count() > max as usize
                    {
                        trimmed = trimmed.chars().take(max as usize).collect();
                    }

                    Value::String(trimmed)
                }
                Some(Value::Null) | None => Value::String(String::new()),
                Some(other) => Value::String(value_to_text(other)),
            },
        }
    }
}

/// The JS clamp: `max` first, then `min`, both optional.
fn clamp(value: f64, min: Option<f64>, max: Option<f64>) -> f64 {
    let mut value = value;
    if let Some(max) = max
        && value > max
    {
        value = max;
    }

    if let Some(min) = min
        && value < min
    {
        value = min;
    }

    value
}

/// JS `String(source)` for the non-string winner of a text rule.
fn value_to_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "null".to_owned(),
        other => other.to_string(),
    }
}

/// The reserved key itself, re-exported so a handler does not have to reach into `crate::plugins`.
pub const ENABLED_KEY: &str = PLUGIN_ENABLED_KEY;
