//! `{root}/sessions/{session_id}/threads` - the conversation roster.
//!
//! TypeScript origin: `src/server/api.ts` - `threadRowsOf` (`:3350-3369`), `getSessionThreads`
//! (`:3371-3391`), `getSessionThread` (`:3393-3413`), `postSessionThread` (`:3415-3462`),
//! `deleteSessionThread` (`:3464-3503`), `isDescendant` (`:3505-3520`) and `postThreadCancel`
//! (`:3522-3552`).
//!
//! THE COUNTER SPLIT (`api.ts:934-944`)
//! -----------------------------------
//! The store signals `thread.upsert` / `thread.removed`, which moves `threads` and the per
//! conversation counters; the ROUTE owns the session row (`api.ts:3483`, `:3536`) and, for a create,
//! the roster bump itself (`:3444`). Each handler therefore stamps exactly the figure TS stamps: the
//! roster counter for a create, `thread:<session>:<thread>` for a cancel, and the row's own
//! `revision` member for the identified read.
//!
//! WHY THE ROSTER PAGE IS BUILT HERE AND NOT IN THE STORE
//! -----------------------------------------------------
//! `api.ts:3378-3381` filters by `agent` and `status` BEFORE slicing, so the page and `next_cursor`
//! describe the FILTERED collection. The store's `list_threads` pages the whole roster, so this route
//! projects the rows itself through the same `store::read::thread_row_of` the create reads back
//! through - one projection, so a create and a later list can never state one conversation
//! differently (`api.ts:3451-3455`).

use serde_json::{Map, Value, json};

use crate::api::{self, ApiResult, Ctx};
use crate::contract;
use crate::error::ApiError;
use crate::events::now_millis;
use crate::paging;
use crate::store::read::thread_row_of;

use super::sessions::{require_session, require_thread, session_of, thread_key};

/// The three `status` spellings `GET .../threads` accepts (`api.ts:3376`).
const THREAD_STATUSES: [&str; 3] = ["idle", "running", "stopped"];

/// The fields `POST .../threads` accepts (`api.ts:3419`).
///
/// `origin` is deliberately absent: the contract's REST body has no origin field, and the route
/// creates a HOST conversation (`api.ts:3435` calls `harness.createThread`, whose rows are host rows
/// - the roster read filters `origin === 'host'`, `api.ts:3351`). The handler therefore injects
/// `"origin": "host"` into the store call rather than accepting it from the wire.
const CREATE_THREAD_FIELDS: [&str; 3] = ["agent", "label", "owner_thread_id"];

/// `GET {root}/sessions/{session_id}/threads` (`api.ts:3371-3391`).
pub async fn get_session_threads(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;
    let agent = api::param_of(&ctx.query, "agent");
    let status = api::enum_param(&ctx.query, "status", &THREAD_STATUSES)?;

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    // Creation order (`sequence`), which is the roster's stable order (`api.ts:3353`). The `agent`
    // and `status` filters run BEFORE the slice, so `next_cursor` addresses the filtered collection.
    let wanted = status.map(thread_lifecycle_of);
    let rows: Vec<contract::ThreadSummary> = state
        .thread_ids()
        .into_iter()
        .filter_map(|thread_id| thread_row_of(state, &thread_id))
        .filter(|row| {
            agent.is_none_or(|wanted| row.agent == wanted)
                && wanted.is_none_or(|wanted| row.status == wanted)
        })
        .collect();

    let total = rows.len();
    let slice = paging::slice_page(rows, cursor, limit, Some(total));

    let revision = ctx.revision("threads", Some(&session_id));
    let body = paging::page_slice_body(
        ctx.runtime.hub.epoch(),
        slice,
        revision,
        now_millis(),
    );

    Ok(api::with_revision(200, body, revision))
}

/// `POST {root}/sessions/{session_id}/threads` (`api.ts:3415-3462`): `201 {thread}`.
///
/// The agent must be a MEMBER of this session (`:3425-3427`) - a 400, not the store's 404 - and a
/// named `owner_thread_id` must exist (`:3429`). Nothing runs: opening a conversation is free.
pub async fn post_session_thread(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &CREATE_THREAD_FIELDS)?;

    let agent = body
        .get("agent")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|agent| !agent.is_empty())
        .ok_or_else(|| ApiError::bad_request("A conversation requires an \"agent\"."))?
        .to_owned();

    // The member check is the route's, with TS's sentence (`api.ts:3426`), and it runs before the
    // store's lookup so a non-member is a 400 rather than a 404.
    let store = ctx.runtime.store.as_ref();
    let member = {
        let inner = store.lock();

        inner
            .session(&session_id)
            .map(|state| state.roster().iter().any(|member| member == &agent))
            .unwrap_or(false)
    };

    if !member {
        return Err(ApiError::bad_request(format!(
            "Agent \"{agent}\" is not a member of session \"{session_id}\"."
        )));
    }

    if let Some(owner) = body.get("owner_thread_id").filter(|owner| !owner.is_null()) {
        let owner = owner
            .as_str()
            .ok_or_else(|| ApiError::bad_request("\"owner_thread_id\" must be a string."))?;

        require_thread(ctx, &session_id, owner)?;
    }

    // `origin: host` is what makes this the same conversation a `POST` from the view creates; the
    // contract's body has no such field, so the route supplies it (`knowledge.md` "T3").
    let mut payload = Map::clone(&body);
    payload.insert("origin".to_owned(), json!("host"));

    let (created, _session_revision) = store
        .create_thread(&session_id, &Value::Object(payload))
        .await?;

    // Read the row back through the SAME projection the roster and the identified read use
    // (`api.ts:3451-3458`), so the create answers the resource, not the summary.
    let thread_id = created
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let body = store.get_thread(&session_id, &thread_id).await?;
    let revision = ctx.revision("threads", Some(&session_id));

    Ok(api::with_revision(201, body, revision))
}

/// `GET {root}/sessions/{session_id}/threads/{thread_id}` (`api.ts:3393-3413`).
pub async fn get_session_thread(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;
    require_thread(ctx, &session_id, &thread_id)?;

    let body = ctx
        .runtime
        .store
        .get_thread(&session_id, &thread_id)
        .await?;

    // The resource carries its own `thread:<session>:<thread>` figure (`api-reads.ts:566`), which is
    // the exact number TS stamps (`api.ts:3412`).
    let revision = body
        .pointer("/thread/revision")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    Ok(api::with_revision(200, body, revision))
}

/// `DELETE {root}/sessions/{session_id}/threads/{thread_id}` (`api.ts:3464-3503`).
///
/// The cascade is the part a client must report, so both figures are counted off the roster BEFORE
/// the removal (`api.ts:3470-3475`): descendants are the host rows whose owner chain reaches this
/// conversation, and removed turns are the rows addressed to it.
pub async fn delete_session_thread(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;
    require_thread(ctx, &session_id, &thread_id)?;

    // The counts come first, from the live state, because afterwards the rows are gone and the
    // receipt could only guess.
    let (descendants, removed_turns) = {
        let store = ctx.runtime.store.as_ref();
        let inner = store.lock();
        let state = inner
            .session(&session_id)
            .ok_or_else(|| store.unknown_session(&session_id))?;

        let descendants = state
            .thread_ids()
            .into_iter()
            .filter(|candidate| candidate != &thread_id)
            .filter(|candidate| {
                state
                    .thread(candidate)
                    .is_some_and(|thread| thread.origin == crate::store::origin_str(contract::ThreadOrigin::Host))
                    && is_descendant(state, candidate, &thread_id)
            })
            .count();

        let turns = state
            .turn_ids()
            .into_iter()
            .filter(|turn_id| {
                state
                    .turn(turn_id)
                    .is_some_and(|turn| turn.record.thread_id.as_deref() == Some(thread_id.as_str()))
            })
            .count();

        (
            u64::try_from(descendants).unwrap_or(u64::MAX),
            u64::try_from(turns).unwrap_or(u64::MAX),
        )
    };

    let store = ctx.runtime.store.as_ref();
    let _deleted = store.delete_thread(&session_id, &thread_id).await?;

    // `api.ts:3485` quotes the roster counter the store's `thread.removed` signal just moved, and
    // `:3483` bumps the session row - which this port's store already does inside the same commit
    // (`store/threads.rs:431`), so the route does not bump it a second time.
    let revision = ctx.revision("threads", Some(&session_id));

    let result = contract::DeleteThreadResult {
        deleted: true,
        thread_id,
        session_id,
        removed_descendants: descendants,
        removed_turns,
        revision,
    };

    Ok(api::with_revision(
        200,
        serde_json::to_value(result).unwrap_or(Value::Null),
        revision,
    ))
}

/// `POST {root}/sessions/{session_id}/threads/{thread_id}/cancel` (`api.ts:3522-3552`).
///
/// The stop is a STATE, not a deletion. The receipt's `revision` is the CONVERSATION counter
/// (`api.ts:3534`), not the session's, and the session row is the bump only a request can make
/// (`:3536`) - the store's own signal moved the conversation and roster counters.
pub async fn post_thread_cancel(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;
    require_thread(ctx, &session_id, &thread_id)?;

    // `api.ts:3528` counts THIS conversation's backlog before the stop clears it.
    let dropped = {
        let store = ctx.runtime.store.as_ref();
        let inner = store.lock();

        inner
            .session(&session_id)
            .and_then(|state| state.lanes.get(&thread_id))
            .map(|lane| u64::try_from(lane.queue.len()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    };

    let store = ctx.runtime.store.as_ref();
    let mut body = store.cancel_thread(&session_id, &thread_id).await?;

    // The store reports the SESSION figure in its receipt (`store/threads.rs:640`); the contract's
    // figure is the conversation's, which its own `thread.upsert` signal moved.
    let revision = ctx.revision("thread", Some(&thread_key(&session_id, &thread_id)));

    if let Some(map) = body.as_object_mut() {
        map.insert("revision".to_owned(), json!(revision));
        map.insert("dropped_queued_tasks".to_owned(), json!(dropped));
    }

    let _ = store.bump_session(&session_id);

    Ok(api::with_revision(200, body, revision))
}

/// `isDescendant` (`api.ts:3505-3520`): true when `candidate`'s owner chain reaches `ancestor`.
///
/// The walk is bounded at 64 hops, exactly as TS bounds it, because a document written by hand could
/// name an owner cycle and this read runs inside the store guard.
fn is_descendant(
    state: &crate::store::SessionState,
    candidate: &str,
    ancestor: &str,
) -> bool {
    let mut current = state
        .thread(candidate)
        .and_then(|thread| thread.owner_thread_id.clone());
    let mut depth = 0;

    while let Some(owner) = current && depth < 64 {
        if owner == ancestor {
            return true;
        }

        current = state
            .thread(&owner)
            .and_then(|thread| thread.owner_thread_id.clone());

        depth += 1;
    }

    false
}

/// The enum form of one `status` filter value. `api::enum_param` already proved the spelling is one
/// of the three, so the fall-through arm is unreachable in practice and maps to `Idle`, the same
/// value the wire spelling `idle` carries.
#[must_use]
fn thread_lifecycle_of(raw: &str) -> contract::ThreadLifecycle {
    match raw {
        "running" => contract::ThreadLifecycle::Running,
        "stopped" => contract::ThreadLifecycle::Stopped,
        _ => contract::ThreadLifecycle::Idle,
    }
}
