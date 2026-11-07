//! The built-in PLUGIN catalogue and the operator's settings for it - the server-visible half of
//! the Harness Plugin System.
//!
//! TypeScript origin: `packages/underload/harness/src/server/plugins.ts` (the settings store and
//! every validation sentence in it), `src/plugin/manifest.ts` (the GENERATED built-in catalogue
//! this static table replaces), `src/plugins/async-ping.plugin.ts` +
//! `src/plugins/context-compaction.plugin.ts` (the exact metadata each row carries), and
//! `src/server/api.ts` `pluginSettingOf` / `pluginRowOf` (`:1925-1965`) for the wire projection.
//!
//! WHAT THIS MODULE IS, AND WHAT IT IS NOT
//! ---------------------------------------
//! The TypeScript package has a plugin HOST: `harness()` scans `src/plugins/**/*.plugin.ts`, the
//! generated manifest wires what the scan found, and each plugin's `setup(api)` registers hooks
//! that change a run. THIS build has no plugin host and no runtime compiler, so it executes no
//! hook at all - and it does not need one to satisfy the HTTP contract. Everything the harness
//! front-end's Plugins pane reads and writes is here:
//!
//!   * the CATALOGUE - the two built-ins this build ships, as a STATIC table (there is no scan, so
//!     this table IS the manifest: same rows, same order, same declared metadata);
//!   * the SETTINGS DESCRIPTORS - key, label, declared type, declared default, `min`/`max`/`step`,
//!     description;
//!   * the EFFECTIVE VALUES - the declared default unless the operator stored an override, which is
//!     what a row paints and what a `null` patch restores;
//!   * the WRITE EDGE - validation against the descriptor, with the contract's exact sentences;
//!   * the DURABLE DELTA - [`PluginSettingsStore::snapshot`] is the `pluginSettings` member of
//!     `state.json`.
//!
//! The one behaviour a plugin runtime would add is a LIVE PROVIDER a running harness reads on its
//! next turn (`plugins.ts provider`). Here the live map IS the store, so an accepted patch is
//! immediately visible to every later read of every key by every session in the process; there is
//! simply no plugin code that consumes it. `knowledge.md` "T10 plugin uplift" records this.
//!
//! WHY THE CATALOGUE IS A `static`
//! -------------------------------
//! `BUILTIN_PLUGINS` is compile-time data: the rows ARE the contract, the suite pins them
//! byte-exact, and a `static` makes it impossible for one request to see a different catalogue than
//! the next. Only the OPERATOR's overrides move at runtime, and they are stored in the descriptor's
//! declared type (see [`SettingValue`]) so a restored document, a submitted patch and an answered
//! row cannot disagree about a value's spelling.
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

/// One DECLARED setting of a plugin (`HarnessPluginSettingDescriptor`, `types/index.ts:3053`, and
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
/// `src/plugins`): `async-ping.plugin.ts` before `context-compaction.plugin.ts`.
///
/// The order is contract-visible: `GET /plugins` answers in it, the Plugins pane renders it as-is,
/// and the 404 sentence joins these names with `", "`.
pub static BUILTIN_PLUGINS: [PluginMetadata; 2] = [
    // `src/plugins/async-ping.plugin.ts`'s `metadata` export, verbatim.
    PluginMetadata {
        name: "async-ping",
        title: "Background Ping",
        description: "Keeps a conversation waiting on background tool work moving by emitting a \
                      periodic ping with the calls that are still running, replacing the fixed \
                      scheduler heartbeat.",
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
            // The documented 10-minute wake (`async-ping.plugin.ts DEFAULT_PING_INTERVAL_MS`).
            default: DeclaredDefault::Number(600_000.0),
            min: Some(1_000.0),
            max: None,
            step: Some(1_000.0),
            description: Some(
                "How often a still-running background tool is reported while work \
                               is pending. Defaults to the historical 10-minute wake.",
            ),
        }],
    },
    // `src/plugins/context-compaction.plugin.ts`'s `metadata` export, verbatim.
    PluginMetadata {
        name: "context-compaction",
        title: "Context Compaction",
        description: "Watches each agent conversation against a context limit and, when the limit \
                      is reached, compacts the transcript at the turn boundary before the next \
                      model request is sent.",
        version: "1.0.0",
        tags: &["context", "compaction", "lifecycle"],
        hooks: &["turn.before"],
        settings: &[PluginSettingDescriptor {
            key: "contextLimit",
            label: "Context limit (tokens)",
            kind: PluginSettingType::Number,
            // `context-compaction.plugin.ts CONTEXT_LIMIT_DEFAULT`, which the plugin file requires
            // to equal this declared default (the number is written twice and both places move).
            default: DeclaredDefault::Number(100_000.0),
            min: Some(1_000.0),
            max: None,
            step: Some(1_000.0),
            description: Some(
                "Estimated tokens a conversation may reach before this plugin \
                               compacts it. Measured with the same ~4 characters/token estimate \
                               the UI context bar uses.",
            ),
        }],
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

/// The declared descriptor of one key, when the plugin declared it.
#[must_use]
pub fn descriptor_of(plugin: &str, key: &str) -> Option<&'static PluginSettingDescriptor> {
    settings_of(plugin).iter().find(|entry| entry.key == key)
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

    /// The EFFECTIVE map a plugin would read right now, in DECLARED key order: the stored override
    /// when there is one, the declared default otherwise. This is the projection the UI paints, so
    /// a row can never disagree with runtime behaviour.
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

    /// Only what the operator changed, in CATALOGUE order and DECLARED key order - the `state.json`
    /// projection (`plugins.ts snapshot`).
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

            // Declared order, so a document stays DIFF-STABLE even when an older build wrote the
            // keys in another order.
            for descriptor in metadata.settings {
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
            // A plugin this build stopped shipping: drop the whole entry.
            let Some(metadata) = metadata_of(plugin) else {
                continue;
            };

            // The loader refuses a member that is not an object (`persistence::validate_document`),
            // so reaching here without one means a caller handed this store an UNvalidated document -
            // a foreign root or a test fixture - and the entry is skipped rather than trusted.
            let Some(entries) = raw.as_object() else {
                continue;
            };

            for descriptor in metadata.settings {
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
    /// context-compaction.`, and [`ApiError::bad_request`] for an empty patch, an unknown key, or
    /// an out-of-bounds / mistyped value.
    pub fn update(&mut self, plugin: &str, patch: &Map<String, Value>) -> Result<(), ApiError> {
        // The plugin check comes FIRST: `requirePlugin` throws the 404 before the patch is even
        // looked at, so a wrong id with an empty body is a 404, not a 400.
        let Some(metadata) = metadata_of(plugin) else {
            let known = plugin_names().join(", ");

            return Err(ApiError::not_found(format!(
                "Unknown plugin \"{plugin}\". This build ships: {}.",
                if known.is_empty() { "none" } else { &known }
            )));
        };

        if patch.is_empty() {
            return Err(ApiError::bad_request(
                "A plugin settings patch must change at least one setting.",
            ));
        }

        let declared = metadata.settings;

        // Validate in SUBMISSION order (`serde_json`'s `preserve_order` keeps the client's own key
        // order, so the first refusal names the first field the operator got wrong).
        let mut prepared: Vec<(&'static str, Option<SettingValue>)> =
            Vec::with_capacity(patch.len());

        for (key, submitted) in patch {
            let Some(descriptor) = descriptor_of(plugin, key) else {
                let allowed: Vec<&str> = declared.iter().map(|entry| entry.key).collect();

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
            object(json!({ "contextLimit": 100_000 })),
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
            "Unknown plugin \"retired\". This build ships: async-ping, context-compaction."
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

    /// 8. The snapshot is CATALOGUE order with DECLARED keys, whatever order the writes arrived in -
    ///    the property that keeps a saved document diff-stable.
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
            object(json!({ "contextLimit": 100_000 })),
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
    ///     row - and an unknown plugin has nothing to resolve.
    #[test]
    fn values_answer_the_declared_defaults_for_every_known_plugin() {
        let store = PluginSettingsStore::new();

        assert_eq!(
            store.values("async-ping"),
            object(json!({ "intervalMs": 600_000 }))
        );
        assert_eq!(
            store.values("context-compaction"),
            object(json!({ "contextLimit": 100_000 }))
        );
        assert!(store.values("nobody").is_empty());

        // The catalogue is the two shipped built-ins, in scan order.
        assert_eq!(plugin_names(), vec!["async-ping", "context-compaction"]);
        assert_eq!(store.rows().len(), 2);
        assert_eq!(
            store
                .rows()
                .iter()
                .map(|row| row.name.as_str())
                .collect::<Vec<_>>(),
            vec!["async-ping", "context-compaction"]
        );

        // Both rows state `builtin` and carry their declared descriptor beside the value.
        for row in store.rows() {
            assert!(row.builtin);
            assert_eq!(row.settings.len(), 1);
            assert_eq!(row.values.len(), 1);

            let setting = &row.settings[0];
            let declared = descriptor_of(&row.name, &setting.key).expect("the key is declared");

            assert_eq!(setting.default, declared.default.to_json());
            assert_eq!(setting.min, declared.min.map(number_to_json));
            assert_eq!(setting.step, declared.step.map(number_to_json));
            assert_eq!(
                setting.max, None,
                "neither shipped plugin declares an upper bound"
            );
        }
    }

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().expect("the fixture is an object").clone()
    }
}
