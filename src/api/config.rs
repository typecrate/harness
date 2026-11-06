//! `GET`/`PATCH {root}/config` - the global provider defaults.
//!
//! TypeScript origin: `src/server/api.ts` - `configDocument` (`:1329-1334`), `getConfig`
//! (`:1336`), and `patchConfig` (`:1338-1462`) including the T28 credential rule
//! (`:795-838`, `:1368-1374`, `:1424-1446`) and the T39 atomicity rule (`:1355-1367`). The caps
//! themselves belong to `src/server/config.ts` (`MAX_BASE_URL_LENGTH` / `MAX_MODEL_LENGTH`,
//! `:424-430`) and are re-used from `crate::config` so the route that SETS a default answers
//! exactly what the route that CONSUMES it does.

use serde_json::Value;

use crate::api::{self, ApiResult, Ctx};
use crate::config;
use crate::contract;
use crate::error::ApiError;
use crate::events::ResourceRef;
use crate::read;

/// `GET {root}/config` (`api.ts:1336`): the key-free provider defaults, the read-only deployment
/// runtime block, the writable-field list, and the CURRENT `config` counter.
///
/// The counter is READ, never bumped, here: the publisher owns its counter
/// (`api-events.ts:302-306`), so a read that advanced it would make two identical GETs report two
/// different revisions.
pub async fn get_config(ctx: &mut Ctx) -> ApiResult {
    let document = {
        let settings = ctx
            .runtime
            .settings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        read::global_config_document(&settings, &ctx.runtime.hub)
    };

    let document = with_truthful_key_claim(ctx, document);

    let revision = document.revision;
    let body = serde_json::to_value(document).unwrap_or_default();

    Ok(api::with_revision(
        axum::http::StatusCode::OK.as_u16(),
        body,
        revision,
    ))
}

/// Re-states `defaults.has_api_key` from the credential channels rather than from the settings
/// string alone (`api.ts:860-868`): the real key when this process holds one, otherwise the
/// deployment's own claim - and the claim is not authority once an embedding host has injected a
/// settings channel.
fn with_truthful_key_claim(
    ctx: &Ctx,
    mut document: contract::GlobalConfigDocument,
) -> contract::GlobalConfigDocument {
    let credential = ctx.runtime.credential_state();

    document.defaults.has_api_key = credential.advertised_has_api_key();

    document
}

/// `PATCH {root}/config` (`api.ts:1338-1462`).
///
/// The patch is DECIDED on a candidate copy and only a fully valid candidate is committed: the
/// T39 regression is that a write-as-you-go loop answered 400 for a late key while the earlier keys
/// had already mutated the live defaults, and that an unbounded `base_url` poisoned every later
/// `POST /sessions`. Both are refused here by validating the whole candidate before the commit, so
/// a 400 leaves the settings, the credential, the document and the counters untouched.
pub async fn patch_config(ctx: &mut Ctx) -> ApiResult {
    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &["settings"])?;

    let Some(settings) = body.get("settings") else {
        return Err(ApiError::bad_request(
            "A config patch requires a \"settings\" object.",
        ));
    };

    let Value::Object(fields) = settings else {
        // TS casts here and lets `requireKnownFields` trip over a non-object; the honest refusal
        // is the same one the missing block gets, naming the same shape.
        return Err(ApiError::bad_request(
            "A config patch requires a \"settings\" object.",
        ));
    };

    api::require_body_fields(fields, &["base_url", "model", "api_key"])?;

    if fields.is_empty() {
        return Err(ApiError::bad_request(
            "A config patch must change at least one setting.",
        ));
    }

    // The candidate starts at the live values; every branch below is order-independent, and the
    // scope of a new credential is assigned from the FINAL base URL (`api.ts:1368-1374`).
    let live = ctx
        .runtime
        .settings
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    // The reset target is the BOOT document (`api.ts:1384-1385`, `defaults.baseURL`), which the
    // credential channels carry.
    let credential_boot = ctx.runtime.credential_state();

    let mut next_base_url = live.base_url.clone();
    let mut next_model = live.model.clone();
    let mut next_api_key: Option<String> = None;
    let mut issued_key = false;
    let mut reset_key = false;

    for (key, value) in fields {
        match value {
            // `null` = "back to the shipped default" (`api.ts:1382-1391`).
            Value::Null => match key.as_str() {
                "base_url" => next_base_url = credential_boot.boot_base_url.clone(),
                "model" => next_model = credential_boot.boot_model.clone(),
                // Resetting the key restores the shipped credential TOGETHER WITH the provider it
                // belongs to, which the commit does in one step (`api.ts:1428-1431`).
                _ => reset_key = true,
            },
            Value::String(text) => match key.as_str() {
                "base_url" => next_base_url = text.trim().to_string(),
                "model" => next_model = text.trim().to_string(),
                // An explicit key write issues a NEW process-only credential.
                _ => {
                    next_api_key = Some(text.trim().to_string());
                    issued_key = true;
                }
            },
            // Anything else, including a number or an object (`api.ts:1394-1396`).
            _ => {
                return Err(ApiError::bad_request(format!(
                    "\"settings.{key}\" must be a string or null."
                )));
            }
        }
    }

    // Bounds and the non-empty rule, ALL before the commit (`api.ts:1410-1422`). The caps measure
    // the trimmed value, because that is the string `merge_settings` later validates and sends.
    // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
    if next_base_url.encode_utf16().count() > config::MAX_BASE_URL_LENGTH {
        return Err(ApiError::bad_request(format!(
            "A provider base URL cannot exceed {} characters.",
            config::MAX_BASE_URL_LENGTH
        )));
    }

    // The bound is TypeScript's `.length`, i.e. UTF-16 code units, not scalar characters.
    if next_model.encode_utf16().count() > config::MAX_MODEL_LENGTH {
        return Err(ApiError::bad_request(format!(
            "A model name cannot exceed {} characters.",
            config::MAX_MODEL_LENGTH
        )));
    }

    if next_base_url.trim().is_empty() {
        return Err(ApiError::bad_request(
            "A provider base URL cannot be empty.",
        ));
    }

    // ---- COMMIT: nothing below can fail on the input anymore. ----
    {
        let mut settings = ctx
            .runtime
            .settings
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        settings.base_url = next_base_url.clone();
        settings.model = next_model.clone();

        if let Some(key) = next_api_key.as_deref() {
            settings.api_key = key.to_string();
        }
    }

    {
        let mut credential = ctx
            .runtime
            .credentials
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        credential.apply_patch(
            reset_key,
            issued_key,
            next_api_key.as_deref(),
            &next_base_url,
        );
    }

    // The change is DURABLE before the answer (base URL and model only; the credential never
    // enters the document), so a restart does not quietly revert the deployment
    // (`api.ts:1450-1455 store.updateGlobalSettings`).
    //
    // A failed write is a 500, and the LIVE defaults stay moved: TS applies the patch to
    // `globalSettings` first and only then awaits the document, so its `updateGlobalSettings`
    // rejection leaves the same in-memory state (`sessions.ts:1815-1831`). The counters below are
    // not advanced in that case, so a retry of the same body converges on the disk.
    if let Err(error) = ctx.runtime.save() {
        return Err(ApiError::internal(error.message));
    }

    let revision = ctx.runtime.hub.bump("config", None);

    // `publish` REPORTS the counters the bumps above already moved, and returns the event the

    // hub fanned out; the route needs neither, so both are named rather than ignored.

    let _event = ctx.runtime.hub.publish(
        "config.updated",
        &[ResourceRef::host("config")],
        None,
        None,
        None,
        Value::Null,
    );

    // The document is built AFTER the bump, so the revision it states is the revision the write
    // produced - which is the same number the frame the publish just fanned out carries.
    let document = {
        let settings = ctx
            .runtime
            .settings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        read::global_config_document(&settings, &ctx.runtime.hub)
    };

    let document = with_truthful_key_claim(ctx, document);
    let body = serde_json::to_value(document).unwrap_or_default();

    Ok(api::with_revision(
        axum::http::StatusCode::OK.as_u16(),
        body,
        revision,
    ))
}
