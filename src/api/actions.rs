//! `{root}/sessions/{session_id}/actions**` - the owner-question surface.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionActions` (`:2375-2391`) and
//! `postActionAnswer` (`:2404-2427`), plus `normalizeChoice` (`:2429-2451`).
//!
//! WHAT THE ROUTE OWNS AND THE STORE DOES
//! --------------------------------------
//! The action host (`store/actions.rs`) owns the four-waiting-questions cap, the minted
//! `requestId`, the answer-once rule and the validation of a choice against the RECORDED request.
//! This file owns the wire envelope (`PendingActionListResponse`, the contract's
//! `{session_id, actions, revision}`), the two `choice` shapes with TS's four refusal sentences, and
//! the `actions` counter - the one figure only a request knows, which is why the answer route bumps
//! it itself (`api.ts:2420`) instead of leaving it to the store's signal.
//!
//! Live requests only, in the order they were opened: a question restored from disk has no promise
//! left to resolve and is therefore never listed (`api.ts:2378-2382`).

use serde_json::Value;

use crate::api::{self, ApiResult, Ctx};
use crate::contract::{self, ActionChoiceInput};
use crate::error::ApiError;

use super::sessions::{require_session, session_of};

/// `GET {root}/sessions/{session_id}/actions` (`api.ts:2375-2391`).
///
/// `json(200, response)` with NO `X-Harness-Revision` header - TS stamps none on this read, because
/// the response body already carries the figure (`:2387`).
pub async fn get_session_actions(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    let response = contract::PendingActionListResponse {
        session_id: session_id.clone(),
        actions: state.pending_action_rows(),
        revision: ctx.revision("actions", Some(&session_id)),
    };

    Ok(api::json(
        200,
        serde_json::to_value(response).unwrap_or(Value::Null),
    ))
}

/// `POST {root}/sessions/{session_id}/actions/{request_id}/answer` (`api.ts:2404-2427`).
pub async fn post_action_answer(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let request_id = ctx.param("request_id").to_owned();

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &["choice"])?;

    let Some(choice) = body.get("choice").filter(|choice| !choice.is_null()) else {
        return Err(ApiError::bad_request(
            "An action answer requires a \"choice\".",
        ));
    };

    let choice = normalize_choice(choice)?;

    let store = ctx.runtime.store.as_ref();
    let mut inner = store.lock();

    let (receipt, mut outcome) =
        store.answer_action(&mut inner, &session_id, &request_id, &choice)?;

    // `bumpRevision('actions', id)` (`api.ts:2420`): the queue of waiting questions is roster
    // -visible state, so the answer moves the session's action counter and nothing else.
    outcome
        .bumps
        .push(("actions".to_owned(), Some(session_id.clone())));

    store.commit(&inner, &session_id, &mut outcome)?;

    drop(inner);

    let revision = ctx.revision("actions", Some(&session_id));
    let body = serde_json::to_value(receipt).unwrap_or(Value::Null);

    Ok(api::with_revision(200, body, revision))
}

/// `normalizeChoice` (`api.ts:2429-2451`), sentence for sentence.
///
/// snake_case at the edge on purpose: the wire says `option_id` while the recorded domain choice
/// says `optionId` (`contract::HarnessActionChoice`), and `contract::ActionChoiceInput` is the
/// request-side twin. The store still re-validates the id against the recorded options, so an option
/// the model never offered is a 400 naming every valid one rather than an answer the run must trust.
fn normalize_choice(value: &Value) -> Result<ActionChoiceInput, ApiError> {
    let Some(object) = value.as_object() else {
        return Err(ApiError::bad_request("A \"choice\" must be an object."));
    };

    match object.get("kind").and_then(Value::as_str) {
        Some("option") => {
            api::require_body_fields(object, &["kind", "option_id"])?;

            let option_id = object
                .get("option_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ApiError::bad_request("An option choice requires a string \"option_id\".")
                })?;

            Ok(ActionChoiceInput::Option {
                option_id: option_id.to_owned(),
            })
        }
        Some("custom") => {
            api::require_body_fields(object, &["kind", "text"])?;

            let text = object
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ApiError::bad_request("A custom choice requires string \"text\".")
                })?;

            Ok(ActionChoiceInput::Custom {
                text: text.to_owned(),
            })
        }
        // An absent or unrecognized `kind` is the shape refusal, and it comes LAST because an unknown
        // FIELD inside a well-formed `kind` is the more specific answer (`api.ts:2432-2450`).
        _ => Err(ApiError::bad_request(
            "A \"choice\" must be { kind: \"option\", option_id } or { kind: \"custom\", text }.",
        )),
    }
}


