//! `POST {root}/models` - provider model discovery.
//!
//! TypeScript origin: `src/server/api.ts` - `postModels` (`:1908-1950`), `listModelsWithRetry`
//! (`:310-342`), `settingsInputOf` (`:533-564`) - and `src/transport/models.ts` in full
//! (`modelsUrl` `:109-132`, `modelIds` `:147-176`, `listModels` `:188-237`, `redact` `:89-92`,
//! `displayUrl` `:97`). There is no `src/server/models.ts`: the brief names the pair, and the two
//! files that hold the logic are `api.ts` (the retry policy) and `transport/models.ts` (the call).
//!
//! THE RETRY POLICY IS THE INTERESTING PART (`api.ts:318-321`): only a `request` or `response`
//! failure retries - "a configuration error cannot be fixed by asking again, and any other thrown
//! value is a programming error that must surface unchanged" - and EVERY attempt gets a fresh
//! budget that doubles (10s, 20s, ... ~85 min on the tenth) with NO sleep between attempts, because
//! the growing window IS the policy.
//!
//! THE CREDENTIAL NEVER TRAVELS IN A MESSAGE (`transport/models.ts:18-23`): the key goes only into
//! the `authorization` header, errors are built from a display URL that drops userinfo and query,
//! and the key is scrubbed out of the text in case a provider echoes request headers back.

use std::time::Duration;

use serde_json::{Map, Value};

use crate::api::{self, ApiResult, Ctx};
use crate::config;
use crate::contract;
use crate::error::ApiError;
use crate::events;
use crate::origin;

/// First attempt's budget; each retry doubles it (`api.ts:315`, `MODEL_LIST_TIMEOUT_MS`).
const MODEL_LIST_TIMEOUT: Duration = Duration::from_secs(10);

/// Attempts in total, the first included (`api.ts:316`, `MODEL_LIST_MAX_ATTEMPTS`).
const MODEL_LIST_MAX_ATTEMPTS: u32 = 10;

/// Provider error bodies are arbitrary text: cap what is echoed so a huge HTML error page never
/// becomes the client's error string (`transport/models.ts:80`, same rule as the transport).
const MAX_DETAIL_LENGTH: usize = 200;

/// Upper bound on the ids one answer may advertise. The stage brief's cap; the TS parse dedupes
/// and trims but bounds nothing, so this is the one place the list is truncated.
const MODEL_LIST_CAP: usize = 2_000;

/// Why a model list could not be produced, i.e. `ModelListErrorKind` (`transport/models.ts:61`).
/// The server maps these onto statuses, and keeping the distinction in a variant - not in the
/// message - is what lets the caller stay deterministic without parsing strings:
///
/// * `Configuration` - the caller's settings cannot be used (blank/invalid URL), so 400 and NO
///   request was attempted;
/// * `Request` - the network request itself failed (DNS, TLS, timeout/abort), so 502;
/// * `Response` - the provider answered, but not with a usable catalogue, so 502.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelListFailure {
    Configuration(String),
    Request(String),
    Response(String),
}

impl ModelListFailure {
    /// The retryable kinds (`api.ts:334`): a configuration error is decided by the caller's body,
    /// not by the provider's mood.
    fn is_retryable(&self) -> bool {
        !matches!(self, Self::Configuration(_))
    }

    /// The HTTP status this failure maps to (`api.ts:1944-1945`, `error.kind === 'configuration'
    /// ? 400 : 502`).
    fn status(&self) -> u16 {
        if matches!(self, Self::Configuration(_)) {
            400
        } else {
            502
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::Configuration(message) | Self::Request(message) | Self::Response(message) => {
                message
            }
        }
    }

    fn into_api_error(self) -> ApiError {
        ApiError::new(self.status(), self.message().to_owned())
    }
}

/// `POST {root}/models` (`api.ts:1908-1950`).
///
/// The body's optional `settings` block is resolved through the SAME credential rule a session uses
/// (`config::merge_settings`): a foreign base URL with no explicit key gets NO server key, so
/// discovery can never leak this process's secret to a host the caller named. A body-less call is
/// therefore decided entirely by what the deployment holds - the route never reads `process.env`
/// for a URL or a key at request time.
pub async fn post_models(ctx: &mut Ctx) -> ApiResult {
    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &["settings"])?;

    let input = match body.get("settings") {
        Some(settings) => Some(settings_input_of(settings, "body")?),
        None => None,
    };

    let settings = scoped_provider_settings(ctx, input.as_ref())?;

    if settings.base_url.trim().is_empty() {
        return Err(ApiError::bad_request(
            "Set a provider base URL before listing models.",
        ));
    }

    let key = settings.api_key.trim();
    let key = (!key.is_empty()).then(|| key.to_string());

    let models = list_models_with_retry(&settings.base_url, key.as_deref()).await;

    let models = match models {
        Ok(models) => models,
        Err(failure) => return Err(failure.into_api_error()),
    };

    let index = crate::read::model_index(models, events::now_millis());

    Ok(api::json(
        200,
        serde_json::to_value(index).unwrap_or_default(),
    ))
}

/// `settingsInputOf` (`api.ts:533-564`): the snake_case `settings` block of a create or discovery
/// body, validated field by field and translated for the merge.
///
/// A `null` field is "not supplied" (TS's `else if (x !== undefined && x !== null)`), which is why
/// the typed form is a plain `Option<String>` rather than a double one.
///
/// `at` is the path the contract names in its refusals (`body` on discovery); the parameter is
/// not called `where` because that is a Rust keyword.
pub fn settings_input_of(
    settings: &Value,
    at: &str,
) -> Result<contract::RunnerSettingsInput, ApiError> {
    let fields = api::require_block(settings, &format!("{at}.settings"))?;

    api::require_body_fields(fields, &["base_url", "api_key", "model"])?;

    Ok(contract::RunnerSettingsInput {
        base_url: optional_text(fields, at, "base_url")?,
        api_key: optional_text(fields, at, "api_key")?,
        model: optional_text(fields, at, "model")?,
    })
}

/// One optional string field of a `settings` block: absent, `null`, or a string; anything else is
/// the contract's 400 naming the full path.
fn optional_text(
    fields: &Map<String, Value>,
    at: &str,
    key: &str,
) -> Result<Option<String>, ApiError> {
    match fields.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        _ => Err(ApiError::bad_request(format!(
            "\"{at}.settings.{key}\" must be a string."
        ))),
    }
}

/// Resolves the (base URL, model, key) triple for one outbound provider call, with the credential
/// scoped to the provider that issued it (`api.ts:1924-1928`).
///
/// `config::merge_settings` derives the scope from its `defaults.base_url`, so the scoping pair is
/// passed as a second, explicit merge: TS's third `credential` argument
/// (`config.ts:408-449`) has no twin in the two-argument Rust signature, and doing the scoping in a
/// second step is what keeps `crate::config` untouched.
pub fn scoped_provider_settings(
    ctx: &Ctx,
    input: Option<&contract::RunnerSettingsInput>,
) -> Result<config::HarnessRuntimeSettings, ApiError> {
    let empty = contract::RunnerSettingsInput::default();
    let input = input.unwrap_or(&empty);

    // Step 1: the effective endpoint and model, with the shared caps. The key is deliberately blank
    // in this pass so step 2 alone decides whose secret may travel.
    let live = ctx.runtime.effective_settings();
    let merged = config::merge_settings(
        &config::HarnessRuntimeSettings {
            base_url: live.base_url.clone(),
            api_key: String::new(),
            model: live.model.clone(),
        },
        input,
    )?;

    // An explicit client key is authoritative; otherwise the process key applies only to its own
    // provider's origin and base path.
    if !merged.api_key.trim().is_empty() {
        return Ok(merged);
    }

    let credential = ctx.runtime.credential_state();

    if !config::is_same_provider(&merged.base_url, &credential.credential_base_url) {
        return Ok(merged);
    }

    Ok(config::HarnessRuntimeSettings {
        base_url: merged.base_url,
        api_key: credential.api_key.trim().to_string(),
        model: merged.model,
    })
}

/// `listModelsWithRetry` (`api.ts:322-342`): every attempt gets a FRESH controller and timer,
/// doubling, and the loop only swallows a retryable failure.
async fn list_models_with_retry(
    base_url: &str,
    api_key: Option<&str>,
) -> Result<Vec<String>, ModelListFailure> {
    let mut attempt = 0_u32;
    let mut last: Option<ModelListFailure> = None;

    while attempt < MODEL_LIST_MAX_ATTEMPTS {
        // `MODEL_LIST_TIMEOUT_MS * 2 ** attempt` (`api.ts:325`).
        let budget = MODEL_LIST_TIMEOUT.saturating_mul(1_u32 << attempt.min(30));

        let outcome = tokio::time::timeout(budget, list_models(base_url, api_key)).await;

        match outcome {
            Ok(Ok(models)) => return Ok(models),
            Ok(Err(failure)) => {
                if !failure.is_retryable() {
                    return Err(failure);
                }

                last = Some(failure);
            }
            // The per-attempt budget expired: TS's `controller.abort()` produces the same `request`
            // failure, so this retries too.
            Err(_) => {
                last = Some(ModelListFailure::Request(format!(
                    "Request to {} failed: the model list request exceeded {}s.",
                    display_url(base_url).unwrap_or_else(|| base_url.to_owned()),
                    budget.as_secs()
                )));
            }
        }

        attempt += 1;
    }

    // The last failure is the answer (`api.ts:335`: the final attempt rethrows).
    Err(last.unwrap_or(ModelListFailure::Request(
        "Model list request failed.".to_owned(),
    )))
}

/// `listModels` (`transport/models.ts:188-237`): `GET {baseURL}/models`, then the ids.
async fn list_models(
    base_url: &str,
    api_key: Option<&str>,
) -> Result<Vec<String>, ModelListFailure> {
    let url = models_url(base_url)?;

    // `displayUrl(url)` in TS is computed from the RESOLVED models URL, not from the caller's base
    // URL (`transport/models.ts:189` + `:211`), so a refused dial quotes `…/v1/models` - the endpoint
    // it actually attempted.
    let display = display_url(&url).unwrap_or_else(|| url.clone());

    let client = reqwest::Client::new();

    let mut request = client.get(&url).header("accept", "application/json");

    // Only when a key exists; `Bearer ` with an empty token is worse than no header at all
    // (`transport/models.ts:202-204`).
    if let Some(key) = api_key {
        request = request.header("authorization", format!("Bearer {key}"));
    }

    let response = request.send().await.map_err(|error| {
        ModelListFailure::Request(redact(
            &format!("Request to {display} failed: {error}"),
            api_key,
        ))
    })?;

    let status = response.status();

    if !status.is_success() {
        // Providers frequently explain the failure in the body; use it when it is short text,
        // otherwise fall back to the status line (`transport/models.ts:216-224`).
        let text = response.text().await.unwrap_or_default();
        let detail: String = text.trim().chars().take(MAX_DETAIL_LENGTH).collect();

        let detail = if detail.is_empty() {
            status.canonical_reason().unwrap_or("").to_owned()
        } else {
            detail
        };

        return Err(ModelListFailure::Response(redact(
            &format!("HTTP {}: {}", status.as_u16(), detail),
            api_key,
        )));
    }

    let payload: Value = response.json().await.map_err(|error| {
        ModelListFailure::Response(redact(
            &format!("The provider returned a model list that is not JSON: {error}"),
            api_key,
        ))
    })?;

    model_ids(&payload)
}

/// `modelsUrl` (`transport/models.ts:109-132`): trim, absolute-only, http/https-only, then
/// `{pathname}/models` with trailing slashes collapsed. The hash is dropped and the query is kept,
/// so a base URL that already carries a query string cannot swallow the suffix.
fn models_url(base_url: &str) -> Result<String, ModelListFailure> {
    let trimmed = base_url.trim();

    if trimmed.is_empty() {
        return Err(ModelListFailure::Configuration(
            "A provider base URL is required to list models.".to_owned(),
        ));
    }

    // `new URL()` accepts anything with a scheme and an authority, INCLUDING a special scheme with
    // an empty host (`file:///tmp/provider`). `origin::parse_url` is the origin gate's reader and it
    // requires a host to compare, so the empty-authority form is resolved here instead: the URL IS
    // absolute (the scheme-error branch must report it as such), it just has no origin.
    let url = match origin::parse_url(trimmed) {
        Some(url) => url,
        None => match originless_url(trimmed) {
            Some(url) => url,
            None => {
                return Err(ModelListFailure::Configuration(format!(
                    "\"{trimmed}\" is not an absolute URL."
                )));
            }
        },
    };

    if !url.is_http() && !url.is_https() {
        return Err(ModelListFailure::Configuration(format!(
            "\"{}\" must use http or https to list models.",
            display_of(&url)
        )));
    }

    // `{pathname}/models`, trailing slashes collapsed, and the query KEPT (`:129-131`): appending to
    // the raw string would let a base URL that already has a query swallow the suffix, while dropping
    // the query here would silently strip a provider's version selector. The hash is dropped, which
    // `modelsUrl` does by assigning `url.hash = ''`.
    Ok(format!(
        "{}://{}{}/models{}",
        url.scheme,
        url.host,
        url.pathname.trim_end_matches('/'),
        url.search
    ))
}

/// The URL form that is safe to quote in an error and in a log: no userinfo, no query, no fragment
/// (`transport/models.ts:94-97`, which is `url.origin + url.pathname` - the scheme WITH its colon).
fn display_url(base_url: &str) -> Option<String> {
    let trimmed = base_url.trim();

    origin::parse_url(trimmed)
        .or_else(|| originless_url(trimmed))
        .map(|url| display_of(&url))
}

/// WHATWG's `origin + pathname`. The origin is `scheme://host[:port]` with the default port already
/// dropped by [`origin::parse_url`], EXCEPT for an opaque-origin scheme: `URL.origin` is the literal
/// string `"null"` for `file:` (`node -e "new URL('file:///tmp').origin" // 'null'`), which is why
/// TypeScript's sentence for a `file:` base URL reads `"null/tmp/provider" must use http or https…`.
fn display_of(url: &origin::ParsedUrl) -> String {
    if url.hostname.is_empty() {
        return format!("null{}", url.pathname);
    }

    format!("{}://{}{}", url.scheme, url.host, url.pathname)
}

/// A `scheme:/path` URL whose authority is EMPTY, which [`origin::parse_url`] refuses because the
/// origin gate has nothing to compare. Only the shape `new URL()` accepts is recognised here: a
/// scheme, `//`, and then straight into the path (`file:///tmp/provider`, `mailto://x`).
fn originless_url(trimmed: &str) -> Option<origin::ParsedUrl> {
    let (scheme, rest) = trimmed.split_once(':')?;

    let is_scheme = !scheme.is_empty()
        && scheme
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|glyph| glyph.is_ascii_alphanumeric() || matches!(glyph, '+' | '-' | '.'));

    if !is_scheme {
        return None;
    }

    let path = rest.strip_prefix("//")?;

    Some(origin::ParsedUrl {
        scheme: scheme.to_lowercase(),
        host: String::new(),
        hostname: String::new(),
        // WHATWG normalizes a special scheme's empty path to `/`, so `file://` displays as `null/`.
        pathname: if path.is_empty() {
            "/".to_owned()
        } else {
            path.to_owned()
        },
        search: String::new(),
        hash: String::new(),
    })
}

/// Replaces the key anywhere it appears in a message (`transport/models.ts:85-92`). A short key
/// would over-redact, so only keys of a usable length are scrubbed - credential-shaped keys are far
/// longer than 8 characters.
fn redact(message: &str, key: Option<&str>) -> String {
    match key {
        Some(key) if key.len() >= 8 => message.replace(key, "[redacted]"),
        _ => message.to_owned(),
    }
}

/// `modelIds` (`transport/models.ts:147-176`): the ids from either supported shape, trimmed,
/// empties dropped, duplicates removed while PRESERVING the provider's order - "that order is often
/// the provider's own preference, and a dropdown should not reshuffle it".
///
/// A non-string `id` is ignored rather than stringified: `{id: 5}` is not a model a provider can
/// serve.
///
/// Both envelope spellings are read: the standard `{object: 'list', data: [...]}`, a bare array,
/// and - per the stage brief - a `{models: [...]}` gateway body.
fn model_ids(payload: &Value) -> Result<Vec<String>, ModelListFailure> {
    let entries = match payload {
        Value::Array(entries) => Some(entries.as_slice()),
        Value::Object(fields) => fields
            .get("data")
            .or_else(|| fields.get("models"))
            .and_then(Value::as_array)
            .map(Vec::as_slice),
        _ => None,
    };

    let entries = entries.ok_or_else(|| {
        ModelListFailure::Response(
            "The provider's /models response did not include a model list.".to_owned(),
        )
    })?;

    let mut ids: Vec<String> = Vec::new();

    for entry in entries {
        let raw = match entry {
            Value::String(text) => Some(text.as_str()),
            Value::Object(fields) => fields.get("id").and_then(Value::as_str),
            _ => None,
        };

        let Some(text) = raw else { continue };

        let id = text.trim();

        if id.is_empty() || ids.iter().any(|kept| kept == id) {
            continue;
        }

        ids.push(id.to_owned());

        if ids.len() >= MODEL_LIST_CAP {
            break;
        }
    }

    Ok(ids)
}
