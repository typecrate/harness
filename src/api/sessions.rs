//! `{root}/sessions` and the session-scoped verbs.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessions` (`:1958-1985`), `postSession`
//! (`:1987-2042`), `getSession` (`:2044-2048`), `patchSession` (`:2050-2104`), `deleteSession`
//! (`:2106-2135`), `getSessionDefinition` (`:2137-2151`), `getSessionCommands` (`:2164-2178`),
//! `postSessionTask` (`:2199-2269`), `postSessionReset` (`:2271-2312`) and `postSessionCancel`
//! (`:2314-2373`) - plus the three shared helpers every session route calls: `requireSession`
//! (`:1252`), `requireThread` (`:1262-1269`) and `requireTurn` (`:1254-1260`), which live here
//! because the Rust store keeps its own 404 wordings (`Unknown thread "x".` without the session
//! half) and the CONTRACT sentence is `Unknown <what> "<id>" in session "<sid>".`
//!
//! WHAT THIS FILE OWNS (`api.ts:934-944`'s counter split)
//! -----------------------------------------------------
//! The store moves the counters its own signal proves (`turn*`, `usage`, `actions`, `thread*`,
//! `transcript*`); the ROUTE moves the ones only a request can know (`session` collection,
//! `definition`, `commands`, `queue`, `threads` roster, `session_agents`, `session_tools`). Every
//! handler below therefore stamps `X-Harness-Revision` from the SAME figure its body reports, and
//! every page stamps `result.revision` (`api.ts:1984`, `:2177`, `:2193`) - reading the value back
//! out of the store's own body is what keeps the header and the document from being two numbers
//! for one change.
//!
//! THE HANDLER ORDER IS THE CONTRACT
//! --------------------------------
//! route -> method -> body/content-type (the dispatcher) -> `requireSession` -> body fields ->
//! field types -> the domain. `postSessionTask` and `patchSession` prove it: the session lookup
//! runs BEFORE the `{}`-body refusal (`api.ts:2055` precedes `:2057`, `:2200` precedes `:2201`),
//! so an unknown id answers 404 whatever the body says, while an unknown FIELD answers 400 before
//! the domain is consulted at all (`:2053`).

use serde_json::Value;

use crate::api::{self, ApiResult, ApiResponse, Ctx};
use crate::contract;
use crate::error::ApiError;
use crate::store;

/// The fields `POST {root}/sessions` accepts (`api.ts:1990`).
const CREATE_FIELDS: [&str; 3] = ["harness", "title", "settings"];

/// The fields `PATCH {root}/sessions/{session_id}` accepts (`api.ts:2053`).
const PATCH_FIELDS: [&str; 2] = ["agent_models", "title"];

/// The fields `POST {root}/sessions/{session_id}/tasks` accepts (`api.ts:2203`).
const SUBMIT_FIELDS: [&str; 4] = ["input", "attachments", "references", "thread_id"];

/// The fields `POST {root}/sessions/{session_id}/cancel` accepts (`api.ts:2322`).
const CANCEL_FIELDS: [&str; 1] = ["all"];

/// The `settings` block a create body may carry (`settingsInputOf`, `api.ts:544`).
const SETTINGS_FIELDS: [&str; 3] = ["base_url", "api_key", "model"];

// ---------------------------------------------------------------------------
// The helpers every session-domain handler shares
// ---------------------------------------------------------------------------

/// The session this request's route addressed (`ctx.params.session_id`).
#[must_use]
pub(crate) fn session_of(ctx: &Ctx) -> String {
    // Owned: `ctx.param(..)` borrows `ctx`, and every handler below then reads a body through
    // `&mut ctx`.
    ctx.param("session_id").to_owned()
}

/// `requireSession` (`api.ts:1252`): the aggregate lookup that every session route runs FIRST, so a
/// wrong id never reaches a body's field checks. The store's own wording is the contract's
/// (`Unknown session "<id>".`, `sessions.ts:2355-2362`).
///
/// The `SessionRef` guard is released by the end of the statement: a handler that then calls a store
/// method must not still be holding the store mutex.
pub(crate) fn require_session(ctx: &Ctx, session_id: &str) -> Result<(), ApiError> {
    let store = ctx.runtime.store.as_ref();
    let known = store.find_session(session_id).is_ok();

    if known {
        Ok(())
    } else {
        Err(store.unknown_session(session_id))
    }
}

/// `requireThread` (`api.ts:1262-1269`): a conversation exists when the roster holds it OR the
/// invalid-transcript map does - an UNREADABLE conversation is still addressable, which is what lets
/// its transcript be re-read and re-written. The refusal is the contract's, with the session named.
pub(crate) fn require_thread(ctx: &Ctx, session_id: &str, thread_id: &str) -> Result<(), ApiError> {
    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(session_id)
        .ok_or_else(|| store.unknown_session(session_id))?;

    if state.thread(thread_id).is_some() || state.invalid_threads.contains_key(thread_id) {
        return Ok(());
    }

    Err(api::missing_in("thread", thread_id, session_id))
}

/// `requireTurn` (`api.ts:1254-1260`), same wording rule as [`require_thread`].
pub(crate) fn require_turn(ctx: &Ctx, session_id: &str, turn_id: &str) -> Result<(), ApiError> {
    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(session_id)
        .ok_or_else(|| store.unknown_session(session_id))?;

    if state.turn(turn_id).is_some() {
        return Ok(());
    }

    Err(api::missing_in("turn", turn_id, session_id))
}

/// The counter one identified resource reports right now (`ctx.revision`), for the routes whose
/// header figure is a counter the store has already moved.
#[must_use]
pub(crate) fn resource_revision(ctx: &Ctx, resource: &str, id: &str) -> u64 {
    ctx.revision(resource, Some(id))
}

/// One conversation's hub key (`threadKey`, `api.ts:599`): the hub addresses a conversation as
/// `session:thread`, so a bare thread id is a DIFFERENT counter.
#[must_use]
pub(crate) fn thread_key(session_id: &str, thread_id: &str) -> String {
    store::thread_key(session_id, thread_id)
}

/// The `X-Harness-Revision` figure a paged read stamps: `api.ts` returns
/// `json(200, result, result.revision)` on EVERY page route (`:1984`, `:2177`, `:2193`, `:2872`,
/// `:3390`, `:3615`, `:3690`, `:3768`, `:3851`), so the header and the body's `revision` member are
/// read from the SAME document. A page body always carries one (`paging::PageBodyMeta::of`), and
/// re-reading the hub here would be a second lookup of a number that a concurrent run may have
/// moved in between - which is exactly the "one number per revision" rule the contract states.
#[must_use]
pub(crate) fn page_revision(body: &Value) -> u64 {
    number_of(body, "revision")
}

/// One numeric member of a JSON object, `0` when it is absent or not a number. Receipts, pages and
/// resources all print their own revision, and a missing figure is a bug rather than a refusal the
/// caller can act on - so the answer is a plain number, never an `Option` every call site unwinds.
#[must_use]
pub(crate) fn number_of(body: &Value, key: &str) -> u64 {
    body.get(key)
        .and_then(Value::as_u64)
        .or_else(|| body.get(key).and_then(Value::as_f64).map(|value| value as u64))
        .unwrap_or(0)
}

/// A success that stamps the header with the figure the BODY already reports.
#[must_use]
pub(crate) fn stamped(status: u16, body: Value, revision: u64) -> ApiResponse {
    api::with_revision(status, body, revision)
}

// ---------------------------------------------------------------------------
// The collection
// ---------------------------------------------------------------------------

/// `GET {root}/sessions` (`api.ts:1958-1985`).
///
/// `limit`/`cursor` resolve before the `status` enum, and `harness` is free text (a typo answers an
/// empty page rather than a refusal) - both are the store's `list_sessions` order, which is this
/// port's copy of the same recipe.
pub async fn get_sessions(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let body = ctx.runtime.store.list_sessions(&ctx.query).await?;
    let revision = page_revision(&body);

    Ok(stamped(200, body, revision))
}

/// `POST {root}/sessions` (`api.ts:1987-2042`): `201 {session}` plus the five counters a fresh
/// resource moves (`session` row, `definition`, `turns`, `threads`, and the `session` COLLECTION).
///
/// The handler-level validation is the whole of TS's: the field set, then `harness` as a non-empty
/// string, then the `settings` block through `settingsInputOf` (`:534-564`) - whose three refusal
/// sentences name the block path, so a bad `api_key` type says `"body.settings.api_key" must be a
/// string.` The credential MERGE and the endpoint requirement are the store's
/// (`config::merge_settings` inside `create_session`), which is what keeps this route and a direct
/// embedder from disagreeing about when a client key is honoured.
pub async fn post_session(ctx: &mut Ctx) -> ApiResult {
    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &CREATE_FIELDS)?;

    match body.get("harness") {
        Some(Value::String(name)) if !name.is_empty() => {}
        _ => {
            return Err(ApiError::bad_request("A session requires a harness name."));
        }
    }

    if let Some(settings) = body.get("settings") {
        // `null` is "not supplied" (`settingsInputOf` returns early for `undefined`, and serde maps a
        // JSON null to the same absence), so only a stated non-null block is validated.
        if !settings.is_null() {
            let block = api::require_block(settings, "body")?;

            api::require_body_fields(block, &SETTINGS_FIELDS)?;

            for field in SETTINGS_FIELDS {
                if let Some(value) = block.get(field)
                    && !value.is_null()
                    && !value.is_string()
                {
                    return Err(ApiError::bad_request(format!(
                        "\"body.settings.{field}\" must be a string."
                    )));
                }
            }
        }
    }

    let request: contract::CreateSessionRequest = contract::decode_body(Value::Object(body))?;
    let body = ctx.runtime.store.create_session(&request).await?;

    // The new session's own row counter, which the create bumped before this answer (`api.ts:2031`).
    let session_id = body
        .pointer("/session/id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let revision = resource_revision(ctx, "session", &session_id);

    Ok(stamped(201, body, revision))
}

// ---------------------------------------------------------------------------
// One session
// ---------------------------------------------------------------------------

/// `GET {root}/sessions/{session_id}` (`api.ts:2044-2048`).
pub async fn get_session(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let body = ctx.runtime.store.get_session(&session_id).await?;
    let revision = resource_revision(ctx, "session", &session_id);

    Ok(stamped(200, body, revision))
}

/// `PATCH {root}/sessions/{session_id}` (`api.ts:2050-2104`).
///
/// `agent_models` MERGES and `title` REPLACES - both are the store's rules, including the roster
/// check that makes an unknown member key a 400 naming the members. The lookup order is TS's: the
/// field set, then the session, then the "patch changes nothing" refusal, so `{}` on an unknown id
/// is a 404 and not a 400.
pub async fn patch_session(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &PATCH_FIELDS)?;
    require_session(ctx, &session_id)?;

    let body = ctx
        .runtime
        .store
        .update_session(&session_id, &Value::Object(body))
        .await?;
    let revision = resource_revision(ctx, "session", &session_id);

    Ok(stamped(200, body, revision))
}

/// `DELETE {root}/sessions/{session_id}` (`api.ts:2106-2135`).
///
/// The counts in the receipt are read off the live record BEFORE the removal (`api.ts:2112-2116`),
/// which the store does internally, and the header quotes the COLLECTION counter the delete bumped
/// (`bumpRevision('session', null)`, `:2123`) - the rail's number, not the row's, because the row is
/// gone.
pub async fn delete_session(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);

    // A DELETE carries no documented body; it is still read, because a body the contract does not
    // document must be refused (or accepted) by a parser rather than left half-consumed.
    let _ = ctx.body_value().await?;

    let body = ctx.runtime.store.delete_session(&session_id).await?;
    let revision = ctx.revision("session", None);

    Ok(stamped(200, body, revision))
}

/// `GET {root}/sessions/{session_id}/definition` (`api.ts:2137-2151`).
pub async fn get_session_definition(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let body = ctx.runtime.store.session_definition(&session_id).await?;
    let revision = resource_revision(ctx, "definition", &session_id);

    Ok(stamped(200, body, revision))
}

/// `GET {root}/sessions/{session_id}/commands` (`api.ts:2164-2178`).
pub async fn get_session_commands(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let body = ctx
        .runtime
        .store
        .session_commands(&session_id, &ctx.query)
        .await?;
    let revision = page_revision(&body);

    Ok(stamped(200, body, revision))
}

/// The queue page lives in `api::queue`; re-exported here so the route table has ONE module to name
/// for a session operation.
pub use crate::api::queue::get_session_queue;

/// The usage page lives in `api::usage`; re-exported for the same reason.
pub use crate::api::usage::get_session_usage;

// ---------------------------------------------------------------------------
// Submit / reset / cancel
// ---------------------------------------------------------------------------

/// `POST {root}/sessions/{session_id}/tasks` (`api.ts:2199-2269`): `202` and a receipt.
///
/// Three things the handler owns and the store does not: the session lookup runs BEFORE the body is
/// parsed (`:2200`), the field set is this route's list (`:2203`), and `input` must be a string with
/// TS's exact sentence (`:2205`) - a serde-shaped "invalid type: string" would be a different
/// contract. Everything else (trim, the 200k bound, the attachment matrix, the 16-reference cap, the
/// pending cap, steering, the lane, the queue ordinal and the 429) is `store::submit_task`, which is
/// also what makes a slash command reach 202 through the same door.
pub async fn post_session_task(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &SUBMIT_FIELDS)?;

    if !body.contains_key("input") || body["input"].is_null() || !body["input"].is_string() {
        return Err(ApiError::bad_request("A task must be a string."));
    }

    let request: contract::SubmitTaskRequest = contract::decode_body(Value::Object(body))?;
    let receipt = ctx.runtime.store.submit_task(&session_id, request).await?;

    // TS stamps the same figure the receipt prints (`sessionRevision`, `api.ts:2232` + `:2268`).
    let revision = number_of(&receipt, "session_revision");

    Ok(stamped(202, receipt, revision))
}

/// `POST {root}/sessions/{session_id}/reset` (`api.ts:2271-2312`): body-less, and busy is a 409.
pub async fn post_session_reset(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;
    ctx.require_empty_body().await?;

    let body = ctx.runtime.store.reset_session(&session_id).await?;

    // The receipt's own `revision` is the session counter the reset moved (`api.ts:2285` reads the
    // same figure it prints in both places).
    let revision = number_of(&body, "revision");

    Ok(stamped(200, body, revision))
}

/// `POST {root}/sessions/{session_id}/cancel` (`api.ts:2314-2373`).
///
/// `all` is a BOOLEAN or the request is refused (`:2329-2331`, the T39 regression): a truthy string
/// used to run as `all: false` and then report `dropped_queued_tasks: 0`, contradicting the caller's
/// own instruction. An absent key or `{}` cancels the active run only.
pub async fn post_session_cancel(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &CANCEL_FIELDS)?;

    let all = match body.get("all") {
        // Absent or `{}` is "cancel the active run only" (`api.ts:2316-2320`).
        None => false,
        Some(Value::Bool(all)) => *all,
        // Any other spelling - including `null`, which is `!== undefined` and not a boolean in TS -
        // is the refusal, before anything is cancelled (`:2329-2331`).
        Some(_) => {
            return Err(ApiError::bad_request(
                "`all` must be a boolean when present.",
            ));
        }
    };

    let body = ctx.runtime.store.cancel_session(&session_id, all).await?;
    let revision = number_of(&body, "revision");

    Ok(stamped(200, body, revision))
}

/// The `{}` guard `patchSession` and its siblings share; kept as a helper so the sentence cannot
/// drift between the three routes that print it (`api.ts:2058`, `:2537`, `:3189`).
pub(crate) fn empty_patch() -> ApiError {
    ApiError::bad_request("A patch must change at least one field.")
}

/// A short alias the tool and roster routes use for the store handle, so a handler body reads as the
/// domain rather than as a chain of field accesses.
pub(crate) fn store_of(ctx: &Ctx) -> &store::Store {
    ctx.runtime.store.as_ref()
}

/// `json(200, response)` with no revision at all, for the reads TS stamps nothing on
/// (`api.ts:1232-1233`): the roster list, the instruction text, the session tool reads.
#[must_use]
pub(crate) fn plain(body: Value) -> ApiResult {
    Ok(api::json(200, body))
}
