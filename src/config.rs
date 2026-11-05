//! Server configuration: provider settings + deployment runtime, and their rules.
//!
//! TypeScript origin: `packages/underload/harness/src/server/config.ts` (T28/R3/D4), plus the
//! `PATCH /config` credential semantics of `src/server/api.ts:1338-1461`.
//!
//! CREDENTIAL RULE (`config.ts:7-13`, the reason a settings merge is not a plain spread): the
//! server's configured key belongs to ONE provider - the origin and base path of the configured
//! base URL. A client may point a model listing or a session at any base URL it likes, but an
//! omitted/blank key then means "no key", NEVER "use the server key". Without this rule a foreign
//! base URL plus a blank key would send the server's credential to a host of the caller's
//! choosing.
//!
//! A bad runtime value is a DEPLOYMENT error (`config.ts:95-103`): it must abort startup with
//! the variable name and the accepted range rather than degrade into an inline server nobody
//! asked for. The Rust port surfaces it as the 400 the API layer can print, because in Rust the
//! same loader also backs a `PATCH`-style settings review, and `ApiError` is this crate's one
//! refusal type. `numberEnsures`-style coercion is deliberately NOT used: it maps a typo
//! (`HARNESS_WORKER_IDLE_MS=12s`) to 0, which would silently change a deployment's behaviour
//! (`config.ts:202-207`).
//!
//! THE API KEY IS NEVER SERIALIZED into any config document (this crate has no document that
//! carries it), which is what `persistence.ts` calls "the key is never persisted".

use std::collections::BTreeMap;

use regex::Regex;

use crate::contract::{
    ProviderDefaults, RunnerSettingsInput, RuntimeSettings, require_known_fields,
};
use crate::error::ApiError;
use crate::origin::{ParsedUrl, parse_url};

/// Safe default, kept deliberately (`config.ts:105-106`): today's behaviour, so no existing host
/// changes shape by upgrading.
pub const DEFAULT_BASE_URL: &str = "http://localhost:5500/providers/private/v1";
/// The default model (`config.ts:106`).
pub const DEFAULT_MODEL: &str = "merge/ds4-flash";

/// How a session's logical threads get CPU (`config.ts:42`, `:113`). `worker` is a promise, not
/// a hint: a session whose harness cannot be rebuilt from durable data is REJECTED rather than
/// quietly executed inline.
pub const DEFAULT_THREAD_EXECUTION: &str = "inline";
/// How the durable writes get their CPU (`config.ts:60`, `:119`). Independent of
/// `thread_execution`: one lane worker isolates a turn's CPU, this one isolates the
/// checkpoint/journal syscalls, and a deployment may want either, both, or neither.
///
/// TS accepts exactly `inline` | `worker` (`parseIoExecutionMode` REUSES
/// `parseThreadExecutionMode`, `config.ts:249-250`); there is no `off` spelling.
pub const DEFAULT_IO_EXECUTION: &str = "inline";
/// The two accepted spellings for both execution knobs (`config.ts:237`).
pub const EXECUTION_MODES: [&str; 2] = ["inline", "worker"];

/// Reclaim a lane worker after this much idle time; 0 keeps it alive until the session goes
/// (`config.ts:69-71`, default `:131`).
pub const DEFAULT_WORKER_IDLE_MS: u64 = 120_000;
/// Bounds exist so one typo cannot park every lane behind a 40-second handshake or fork 400
/// harness replicas. They are limits, not tuning advice (`config.ts:121-126`).
pub const WORKER_IDLE_MS_MAX: u64 = 3_600_000;
/// Upper bound on concurrently live lane workers per store; each worker owns a full harness
/// replica, so this is the memory guard (`config.ts:73-75`, default `:132`, max `:124`).
pub const DEFAULT_MAX_LANE_WORKERS: u32 = 16;
/// Same, the maximum.
pub const MAX_LANE_WORKERS_LIMIT: u32 = 64;
/// Upper bound on the hello handshake (import + build + capability report)
/// (`config.ts:76-77`, defaults `:125-126`, `:133`).
pub const DEFAULT_WORKER_HANDSHAKE_MS: u64 = 60_000;
/// Handshake floor.
pub const WORKER_HANDSHAKE_MS_MIN: u64 = 1_000;
/// Handshake ceiling.
pub const WORKER_HANDSHAKE_MS_MAX: u64 = 600_000;

/// Shared bounds for client-supplied settings (`config.ts:136-141`): a base URL is a URL, not a
/// document; a model name is an id. Both caps are generous and exist so a settings body cannot
/// smuggle an unbounded string into every provider request.
pub const MAX_BASE_URL_LENGTH: usize = 2048;
/// Model-name cap.
pub const MAX_MODEL_LENGTH: usize = 256;

/// Retries AFTER the initial attempt (`core/agent.ts:175`): one initial attempt plus TEN retries.
pub const DEFAULT_MAX_RETRIES: u32 = 10;
/// Base of the doubling backoff (`core/agent.ts:176`): waits of `1000 * 2 ** n` ms.
pub const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;
/// No provider request timeout by default, because the TS transport sets none
/// (`transport/openai.ts` contains no timeout/abort of its own); `0` means "none".
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 0;

/// The largest accepted retry count (a Rust-side bound, so a typo cannot make a request
/// effectively infinite).
pub const MAX_RETRIES_LIMIT: u32 = 100;
/// The largest accepted backoff base.
pub const RETRY_DELAY_MS_MAX: u64 = 600_000;
/// The largest accepted request timeout.
pub const REQUEST_TIMEOUT_MS_MAX: u64 = 3_600_000;

/// The provider settings a session runs with (`harnesses/types` `RunnerSettings`): the trio the
/// credential rule scopes together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessRuntimeSettings {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
}

/// Everything the process was started with: provider defaults plus the deployment runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Effective provider endpoint.
    pub base_url: String,
    /// The process credential. Never serialized into a document, never advertised to a client.
    pub api_key: String,
    /// Effective model id.
    pub model: String,
    /// Model-request retries after the initial attempt (`core/agent.ts:175`).
    pub max_retries: u32,
    /// Backoff base for the doubling retry sequence (`core/agent.ts:176`).
    pub retry_delay_ms: u64,
    /// Per-request timeout; `0` = none (`DEFAULT_REQUEST_TIMEOUT_MS`).
    pub request_timeout_ms: u64,
    /// `inline` | `worker` (`config.ts:42`).
    pub thread_execution: String,
    /// `inline` | `worker` (`config.ts:60`).
    pub io_execution: String,
    /// Lane-worker reclaim interval, `0..=WORKER_IDLE_MS_MAX`.
    pub worker_idle_ms: u64,
    /// Concurrent lane-worker ceiling, `1..=MAX_LANE_WORKERS_LIMIT`.
    pub max_lane_workers: u32,
    /// Handshake budget, `WORKER_HANDSHAKE_MS_MIN..=WORKER_HANDSHAKE_MS_MAX`.
    pub worker_handshake_ms: u64,
    /// Explicit path to the built lane worker entry; `None` means "the host resolves it"
    /// (`config.ts:78-80`).
    pub worker_entry: Option<String>,
    /// Explicit path to the built IO worker entry (`config.ts:81-86`).
    pub io_worker_entry: Option<String>,
}

impl ServerConfig {
    /// The provider trio this deployment runs with, i.e. TS `ServerConfiguration.settings`.
    #[must_use]
    pub fn settings(&self) -> HarnessRuntimeSettings {
        HarnessRuntimeSettings {
            base_url: self.base_url.clone(),
            api_key: self.api_key.clone(),
            model: self.model.clone(),
        }
    }
}

/// Reads the configuration from an environment map (`config.ts:367-381` +
/// `loadRuntimeConfiguration`, `:253-294`).
///
/// Idempotent and side-effect free: the map is passed in rather than read from the process, so
/// a caller (or a test) controls exactly which values are in force; `crate::environment`'s
/// [`crate::environment::harness_runtime_env`] is what produces the real one.
///
/// EVERY problem is collected and reported in one refusal, because a deployment that sets three
/// bad variables should fix them in one pass instead of one per restart.
///
/// # Errors
///
/// [`ApiError::bad_request`] prefixed `Invalid runtime settings: ` and listing each issue, when
/// any provider or runtime value is unusable.
pub fn load_server_config(env: &BTreeMap<String, String>) -> Result<ServerConfig, ApiError> {
    let mut issues: Vec<String> = Vec::new();

    // `||` semantics (`config.ts:368`, `:370`): a blank or whitespace-only variable falls back to
    // the default, so a blank `HARNESS_BASE_URL` is "not configured", never "configured empty".
    let base_url = trimmed_or(env, "HARNESS_BASE_URL", DEFAULT_BASE_URL);
    // `??` semantics (`config.ts:369`): an explicitly EMPTY `HARNESS_API_KEY` is still "present",
    // so it suppresses the `OPENAI_API_KEY` fallback. That distinction is the whole point of the
    // credential rule, so it is preserved with `get`, not with a truthiness test.
    let api_key = env
        .get("HARNESS_API_KEY")
        .or_else(|| env.get("OPENAI_API_KEY"))
        .cloned()
        .unwrap_or_default();
    let model = trimmed_or(env, "HARNESS_MODEL", DEFAULT_MODEL);

    validate_provider(&base_url, &model, &mut issues);

    let thread_execution = parse_execution_mode(
        env.get("HARNESS_THREAD_EXECUTION"),
        "HARNESS_THREAD_EXECUTION",
        DEFAULT_THREAD_EXECUTION,
        &mut issues,
    );
    let io_execution = parse_execution_mode(
        env.get("HARNESS_IO_EXECUTION"),
        "HARNESS_IO_EXECUTION",
        DEFAULT_IO_EXECUTION,
        &mut issues,
    );

    let worker_idle_ms = integer_within(
        env.get("HARNESS_WORKER_IDLE_MS"),
        "HARNESS_WORKER_IDLE_MS",
        0,
        WORKER_IDLE_MS_MAX,
        DEFAULT_WORKER_IDLE_MS,
        &mut issues,
    );
    let max_lane_workers = integer_within(
        env.get("HARNESS_MAX_LANE_WORKERS"),
        "HARNESS_MAX_LANE_WORKERS",
        1,
        u64::from(MAX_LANE_WORKERS_LIMIT),
        u64::from(DEFAULT_MAX_LANE_WORKERS),
        &mut issues,
    ) as u32;
    let worker_handshake_ms = integer_within(
        env.get("HARNESS_WORKER_HANDSHAKE_MS"),
        "HARNESS_WORKER_HANDSHAKE_MS",
        WORKER_HANDSHAKE_MS_MIN,
        WORKER_HANDSHAKE_MS_MAX,
        DEFAULT_WORKER_HANDSHAKE_MS,
        &mut issues,
    );

    // Transport bounds (Rust-side: the TS retry policy is an AGENT option, not an env var, and
    // the TS transport sets no timeout at all).
    let max_retries = integer_within(
        env.get("HARNESS_MAX_RETRIES"),
        "HARNESS_MAX_RETRIES",
        0,
        u64::from(MAX_RETRIES_LIMIT),
        u64::from(DEFAULT_MAX_RETRIES),
        &mut issues,
    ) as u32;
    let retry_delay_ms = integer_within(
        env.get("HARNESS_RETRY_DELAY_MS"),
        "HARNESS_RETRY_DELAY_MS",
        0,
        RETRY_DELAY_MS_MAX,
        DEFAULT_RETRY_DELAY_MS,
        &mut issues,
    );
    let request_timeout_ms = integer_within(
        env.get("HARNESS_REQUEST_TIMEOUT_MS"),
        "HARNESS_REQUEST_TIMEOUT_MS",
        0,
        REQUEST_TIMEOUT_MS_MAX,
        DEFAULT_REQUEST_TIMEOUT_MS,
        &mut issues,
    );

    // A blank override means "not set", exactly like an absent variable: the host then resolves
    // the shipped bundle, which is the value that survives a rebuild (`config.ts:276-283`).
    let worker_entry = non_empty_trimmed(env.get("HARNESS_WORKER_ENTRY"));
    let io_worker_entry = non_empty_trimmed(env.get("HARNESS_IO_WORKER_ENTRY"));

    if !issues.is_empty() {
        return Err(ApiError::bad_request(format_server_runtime_issues(&issues)));
    }

    Ok(ServerConfig {
        base_url,
        api_key,
        model,
        max_retries,
        retry_delay_ms,
        request_timeout_ms,
        thread_execution,
        io_execution,
        worker_idle_ms,
        max_lane_workers,
        worker_handshake_ms,
        worker_entry,
        io_worker_entry,
    })
}

/// The one message prefix the deployment reports bad configuration under.
pub const RUNTIME_ISSUE_PREFIX: &str = "Invalid runtime settings: ";

/// Joins the collected issues the way a deployment operator needs to read them: the shared
/// prefix, then each issue naming its variable and its accepted range, separated by `; `.
///
/// (The TS original raises one `ServerRuntimeError` per problem from
/// `config.ts:221`/`:241`; this is the aggregated form the Rust port uses so one start reports
/// every mistake.)
#[must_use]
pub fn format_server_runtime_issues(issues: &[String]) -> String {
    let mut message = String::from(RUNTIME_ISSUE_PREFIX);

    message.push_str(&issues.join("; "));

    message
}

/// Validates the provider trio, appending every problem instead of stopping at the first.
///
/// Rules, in TS order: non-empty (`api.ts:1420-1422`), absolute http(s)
/// (`transport/models.ts:99-127`), `<= MAX_BASE_URL_LENGTH` (`config.ts:424-430`), non-empty
/// model, `<= MAX_MODEL_LENGTH` (`config.ts:428-430`).
fn validate_provider(base_url: &str, model: &str, issues: &mut Vec<String>) {
    if base_url.trim().is_empty() {
        issues.push("A provider base URL cannot be empty.".to_string());
    } else {
        match parse_url(base_url.trim()) {
            None => issues.push(format!("\"{}\" is not an absolute URL.", base_url.trim())),
            Some(url) if !(url.is_http() || url.is_https()) => {
                issues.push(format!("\"{}\" must use http or https.", display_url(&url)));
            }
            _ => {}
        }

        if base_url.chars().count() > MAX_BASE_URL_LENGTH {
            issues.push(format!(
                "A provider base URL cannot exceed {MAX_BASE_URL_LENGTH} characters."
            ));
        }
    }

    if model.trim().is_empty() {
        issues.push("A model name cannot be empty.".to_string());
    }

    if model.chars().count() > MAX_MODEL_LENGTH {
        issues.push(format!(
            "A model name cannot exceed {MAX_MODEL_LENGTH} characters."
        ));
    }
}

/// A URL safe to quote in an error: no userinfo, no query string, no fragment - defence in depth
/// against a base URL that embeds credentials (`transport/models.ts:94-97`).
fn display_url(url: &ParsedUrl) -> String {
    format!("{}://{}", url.scheme, url.host)
}

/// `env[key].trim()`, or `default` when the variable is absent or blank (`config.ts:368`, `:370`).
fn trimmed_or(env: &BTreeMap<String, String>, key: &str, default: &str) -> String {
    let value = env.get(key).map(String::as_str).unwrap_or("").trim();

    if value.is_empty() {
        return default.to_string();
    }

    value.to_string()
}

/// A trimmed value, `None` when blank: a blank override is treated exactly like an absent one
/// (`config.ts:276-292`).
fn non_empty_trimmed(value: Option<&String>) -> Option<String> {
    value
        .map(|raw| raw.trim().to_string())
        .filter(|trimmed| !trimmed.is_empty())
}

/// `integerWithin` (`config.ts:209-231`): validate, never coerce.
///
/// Digits only - no sign, no exponent, no trailing text - so `1_000` and `1e3` are refused
/// rather than guessed at, and an out-of-range value is refused with the variable name and the
/// accepted bounds.
fn integer_within(
    raw: Option<&String>,
    variable: &str,
    minimum: u64,
    maximum: u64,
    default: u64,
    issues: &mut Vec<String>,
) -> u64 {
    let range_error = || format!("{variable} must be an integer between {minimum} and {maximum}.");

    let Some(value) = raw.map(|raw| raw.trim()) else {
        return default;
    };

    // An empty variable is "not supplied" (`config.ts:215`).
    if value.is_empty() {
        return default;
    }

    // Digits only, the same `/^\d+$/` the TS validator applies (`config.ts:220`): `[0-9]`
    // because JS `\d` is ASCII-only, and Rust's `\d` is not.
    if !is_digits_only(value) {
        issues.push(range_error());

        return default;
    }

    let Ok(parsed) = value.parse::<u64>() else {
        issues.push(range_error());

        return default;
    };

    // `Number.isSafeInteger` is the TS guard against a 19-digit overflow; a value that parsed
    // into a `u64` but exceeds the range is the same refusal.
    if !(minimum..=maximum).contains(&parsed) {
        issues.push(range_error());

        return default;
    }

    parsed
}

/// `parseThreadExecutionMode` (`config.ts:233-244`), shared with `ioExecution` exactly as
/// `parseIoExecutionMode` shares it (`:249-250`).
///
/// The error names the wrong value AND both accepted spellings: an operator who typed `threads`
/// or `workr` should not have to read the source.
fn parse_execution_mode(
    raw: Option<&String>,
    variable: &str,
    default: &str,
    issues: &mut Vec<String>,
) -> String {
    let Some(raw_value) = raw else {
        return default.to_string();
    };

    let trimmed = raw_value.trim().to_lowercase();
    if trimmed.is_empty() {
        return default.to_string();
    }

    if EXECUTION_MODES.contains(&trimmed.as_str()) {
        return trimmed;
    }

    // TS prints `JSON.stringify(raw)`; `{:?}` on a `&str` is the same quoted form for the
    // values an operator can actually type into a variable.
    issues.push(format!(
        "{variable} must be \"inline\" or \"worker\"; received {raw_value:?}."
    ));

    default.to_string()
}

// ---------------------------------------------------------------------------
// The credential rule
// ---------------------------------------------------------------------------

/// Canonical comparison form for a provider URL: scheme + host + base path, with trailing
/// slashes removed (`config.ts:176-187`).
///
/// Query and hash are KEPT on purpose - a client that appends `?redirect=...` to the configured
/// base URL is not asking for the same provider endpoint, so it must not receive the configured
/// key. The parser also lowercases the host and normalizes a default port, which is why
/// `http://Provider.test:80/v1/` and `http://provider.test/v1` compare equal. A value the parser
/// rejects falls back to trimmed, slash-normalized text, so the comparison never throws.
#[must_use]
pub fn normalize_provider_url(value: &str) -> String {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    match parse_url(trimmed) {
        Some(url) => format!(
            "{}//{}{}{}{}",
            url.scheme,
            url.host,
            url.pathname.trim_end_matches('/'),
            url.search,
            url.hash
        ),
        None => trimmed.to_string(),
    }
}

/// True when both values describe the same provider endpoint (`config.ts:191-196`).
///
/// An empty base URL never matches: "no provider configured" is not a provider.
#[must_use]
pub fn is_same_provider(left: &str, right: &str) -> bool {
    let a = normalize_provider_url(left);
    let b = normalize_provider_url(right);

    !a.is_empty() && a == b
}

/// `mergeSettings` (`config.ts:408-449`): merge a client's per-session settings over the server
/// defaults.
///
/// Key resolution, in the TS order:
///   1. an explicit non-blank client key is used as given (trimmed);
///   2. otherwise the server key is used ONLY when the effective base URL is the provider the
///      key belongs to (`is_same_provider` against `defaults.base_url`);
///   3. otherwise the key is empty - the request goes out unauthenticated.
///
/// `base_url: ""` is kept as empty, which is how a live preset reports that it needs one;
/// `model: ""` falls back to the server model, like before.
///
/// # Errors
///
/// [`ApiError::bad_request`] with the shared caps when the effective pair is too long
/// (`config.ts:424-430`).
pub fn merge_settings(
    defaults: &HarnessRuntimeSettings,
    input: &RunnerSettingsInput,
) -> Result<HarnessRuntimeSettings, ApiError> {
    let base_url = match input.base_url.as_deref() {
        // An explicit value is used as given, trimmed; `""` stays empty, which is how a live
        // preset reports that it still needs one.
        Some(raw) => raw.trim().to_string(),
        None => defaults.base_url.clone(),
    };

    // `model: ''` falls back to the server model, like before (`config.ts:420`).
    let model = match input.model.as_deref().map(str::trim) {
        Some(raw) if !raw.is_empty() => raw.to_string(),
        _ => defaults.model.clone(),
    };

    if base_url.chars().count() > MAX_BASE_URL_LENGTH {
        return Err(ApiError::bad_request(format!(
            "A provider base URL cannot exceed {MAX_BASE_URL_LENGTH} characters."
        )));
    }

    if model.chars().count() > MAX_MODEL_LENGTH {
        return Err(ApiError::bad_request(format!(
            "A model name cannot exceed {MAX_MODEL_LENGTH} characters."
        )));
    }

    let explicit_key = input.api_key.as_deref().unwrap_or("").trim();

    let api_key = if explicit_key.is_empty() {
        if is_same_provider(&base_url, &defaults.base_url) {
            defaults.api_key.trim().to_string()
        } else {
            String::new()
        }
    } else {
        explicit_key.to_string()
    };

    Ok(HarnessRuntimeSettings {
        base_url: base_url.to_string(),
        api_key,
        model,
    })
}

/// Applies one global `settings` patch, i.e. the `PATCH {root}/config` semantics of
/// `api.ts:1338-1461`.
///
/// The patch is DECIDED on a candidate copy and only a fully valid candidate is committed
/// (`api.ts:1355-1367`: a refused patch must mutate nothing), and the scope of a credential is
/// assigned from the FINAL base URL, never from the order the body's keys arrived in
/// (`api.ts:1368-1374`).
///
/// THE CREDENTIAL RULE IN THIS FORM: when the patch moves `base_url` to a DIFFERENT provider
/// and does not supply a key in the same body, the effective key becomes empty - the shipped
/// key belongs to the provider it was issued for, so it never travels to a host the caller chose.
///
/// # Errors
///
/// [`ApiError::bad_request`] for an unknown field, an empty patch, a non-string non-null value,
/// an over-long value, or an empty base URL.
pub fn apply_settings_patch(
    current: &HarnessRuntimeSettings,
    patch: &serde_json::Value,
) -> Result<HarnessRuntimeSettings, ApiError> {
    let fields = match patch {
        serde_json::Value::Object(fields) => fields,
        _ => {
            return Err(ApiError::bad_request(
                "A config patch requires a \"settings\" object.",
            ));
        }
    };

    // `api.ts:1349` allows exactly these three keys; a runtime knob is refused by the same call
    // because it is not in the set, and `api.ts:1376-1378` explains that a deployment-only knob
    // stays enforceable rather than being prose.
    require_known_fields(patch, &["base_url", "model", "api_key"])?;

    if fields.is_empty() {
        return Err(ApiError::bad_request(
            "A config patch must change at least one setting.",
        ));
    }

    let mut next_base_url = current.base_url.clone();
    let mut next_model = current.model.clone();
    let mut next_api_key = current.api_key.clone();
    let mut issued_key = false;

    // `null` means "back to the shipped default" (`api.ts:1382-1391`). The Rust settings struct
    // carries no separate shipped-default channel, so a `null` writes this crate's built-in
    // default - see knowledge.md "Deviations".
    for (key, value) in fields {
        match value {
            serde_json::Value::Null => match key.as_str() {
                "base_url" => next_base_url = DEFAULT_BASE_URL.to_string(),
                "model" => next_model = DEFAULT_MODEL.to_string(),
                // Resetting the key restores the shipped credential, which in a single-settings
                // model is the key the process booted with, i.e. "leave it".
                _ => {}
            },
            serde_json::Value::String(text) => match key.as_str() {
                "base_url" => next_base_url = text.trim().to_string(),
                "model" => next_model = text.trim().to_string(),
                _ => {
                    next_api_key = text.trim().to_string();
                    issued_key = true;
                }
            },
            // `"settings.<key>" must be a string or null.` (`api.ts:1394-1396`).
            _ => {
                return Err(ApiError::bad_request(format!(
                    "\"settings.{key}\" must be a string or null."
                )));
            }
        }
    }

    // Bounds and the non-empty rule, ALL before the commit (`api.ts:1410-1422`). The caps measure
    // the TRIMMED value, because that is the string the provider request later sends.
    if next_base_url.chars().count() > MAX_BASE_URL_LENGTH {
        return Err(ApiError::bad_request(format!(
            "A provider base URL cannot exceed {MAX_BASE_URL_LENGTH} characters."
        )));
    }

    if next_model.chars().count() > MAX_MODEL_LENGTH {
        return Err(ApiError::bad_request(format!(
            "A model name cannot exceed {MAX_MODEL_LENGTH} characters."
        )));
    }

    if next_base_url.trim().is_empty() {
        return Err(ApiError::bad_request(
            "A provider base URL cannot be empty.",
        ));
    }

    // T28 credential scope, evaluated on the FINAL base URL: a body that sets `base_url` and
    // `api_key` together issues the key FOR that provider, and nothing else may receive it.
    if !issued_key && !is_same_provider(&next_base_url, &current.base_url) {
        next_api_key.clear();
    }

    Ok(HarnessRuntimeSettings {
        base_url: next_base_url,
        api_key: next_api_key,
        model: next_model,
    })
}

/// The key-free endpoint view advertised to clients (`api-reads.ts:149-153`): the contract never
/// leaks a credential, only its presence.
#[must_use]
pub fn provider_defaults(settings: &HarnessRuntimeSettings) -> ProviderDefaults {
    ProviderDefaults {
        base_url: settings.base_url.clone(),
        model: settings.model.clone(),
        has_api_key: !settings.api_key.trim().is_empty(),
    }
}

/// The read-only runtime block (`api-reads.ts:156-164`), projected snake_case from the
/// deployment configuration.
#[must_use]
pub fn runtime_settings(config: &ServerConfig) -> RuntimeSettings {
    RuntimeSettings {
        thread_execution: config.thread_execution.clone(),
        worker_idle_ms: config.worker_idle_ms,
        max_lane_workers: config.max_lane_workers,
        worker_handshake_ms: config.worker_handshake_ms,
        // Present only when an explicit entry exists, and it states "is one configured", never
        // the path itself (`api-reads.ts:161-163`).
        worker_entry_configured: config
            .worker_entry
            .as_ref()
            .map(|entry| !entry.trim().is_empty()),
    }
}

/// The digits-only test the runtime validators share, i.e. TS's `/^\d+$/` (`config.ts:220`).
///
/// `[0-9]` rather than `\d`: JS `\d` is ASCII-only while Rust's `\d` is Unicode, and
/// `\A..\z` rather than `^..$` so a trailing newline cannot sneak through. No coercion: the
/// rule is "validate, never coerce" (`config.ts:202-207`).
#[must_use]
pub fn is_digits_only(value: &str) -> bool {
    static DIGITS: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();

    DIGITS
        .get_or_init(|| Regex::new(r"\A[0-9]+\z").expect("the static digits pattern must compile"))
        .is_match(value)
}
