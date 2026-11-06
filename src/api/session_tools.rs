//! `{root}/sessions/{session_id}/tools**` - the session toolbox.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionTools` (`:2839-2873`), `getSessionTool`
//! (`:2875-2886`), `getSessionToolSource` (`:2888-2902`), `assertIdleForToolMutation`
//! (`:2909-2920`), `setSessionToolEnabled` (`:2926-2989`), `registerSessionTool` (`:2994-3144`),
//! `postSessionTool` (`:3146-3171`), `patchSessionTool` (`:3173-3232`), `deleteSessionTool`
//! (`:3238-3309`) and `postSessionToolExecution` (`:3315-3346`).
//!
//! WHAT THE ROUTE OWNS (`api.ts:934-944`)
//! --------------------------------------
//! `session_tools` is a REST-owned counter: a toolbox write bumps exactly that one figure and stamps
//! it on the header. The busy gate (`assertIdleForToolMutation`) and the `protected` refusal are the
//! route's as well, because both are POLICY statements about the session rather than domain rules - a
//! harness-owned control tool is not a user-level edit, and a lane in flight would silently disagree
//! with an enable switch the coordinator applied after it started.
//!
//! A TOOL FAILURE IS DATA: manual execution answers `200 {ok: false}` for a call that executed and
//! failed, and reserves 404/409 for "this name is not callable here" (`api.ts:3338-3345`).
//!
//! THE ONE DEVIATION FROM `api.ts` IN THIS FILE, stated in `knowledge.md`
//! ---------------------------------------------------------------------
//! `postSessionTool` refuses `"replace": true` with a 403 capability denial (`api.ts:3163-3168`)
//! because the TypeScript runtime's `createTool` has no replace flag. This port's registration kernel
//! DOES have one (`store::tools::create_session_tool`, previous-revision report included), so refusing
//! it would contradict a capability the same build implements, and the pair of TypeScript's refusals
//! (409 without the flag, 403 with it) would leave no way to replace a created name at all. Here a
//! re-registration answers `201 {created: false, replaced: true, previous_revision}` and a plain
//! duplicate create answers the store's 409 naming the flag.

use serde_json::{Map, Value, json};

use crate::api::{self, ApiResult, Ctx, missing_in};
use crate::contract::{self, SessionToolOrigin};
use crate::error::{ApiError, HarnessResult};
use crate::events::now_millis;
use crate::ids;
use crate::paging;
use crate::ports::{Query, ToolRuntime as _};
use crate::store::{self, CommitOutcome};

use super::sessions::{empty_patch, plain, require_session, session_of, store_of};

/// The fields `POST .../tools` accepts (`api.ts:3150-3161`).
const CREATE_FIELDS: [&str; 10] = [
    "name",
    "description",
    "tags",
    "parameters",
    "source",
    "module_text",
    "source_path",
    "dependencies",
    "visibility",
    "replace",
];

/// The fields `PATCH .../tools/{tool_id}` accepts (`api.ts:3177-3187`).
const PATCH_FIELDS: [&str; 9] = [
    "description",
    "tags",
    "parameters",
    "visibility",
    "enabled",
    "source",
    "module_text",
    "dependencies",
    "replace",
];

/// The three `origin` spellings the toolbox page filters on (`api.ts:2844`).
const ORIGINS: [&str; 3] = ["catalogue", "discovered", "created"];

/// `GET {root}/sessions/{session_id}/tools` (`api.ts:2839-2873`).
///
/// The `enabled` filter is a strict boolean STRING (`:2848-2853`) and `origin` is a closed enum, both
/// refused before a row is built. The page carries the creation `budget` beside the paging meta, which
/// is why this route assembles its own body rather than reusing a store page.
pub async fn get_session_tools(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;

    let enabled = match api::param_of(&ctx.query, "enabled") {
        Some("true") => Some(true),
        Some("false") => Some(false),
        Some(_) => {
            return Err(ApiError::bad_request(
                "Query parameter \"enabled\" must be true or false.",
            ));
        }
        None => None,
    };

    let origin = api::enum_param(&ctx.query, "origin", &ORIGINS)?.map(origin_of);

    let store = store_of(ctx);
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    // `u64::MAX` asks the store for the WHOLE filtered collection, so the two filters this route owns
    // (`enabled`, `origin`) run before the slice and `next_cursor` describes the filtered collection.
    let (rows, _total) = store.session_tool_rows(state, &ctx.query, u64::MAX, 0);

    let budget = Store::tool_budget(state);

    let rows: Vec<contract::SessionToolSummary> = rows
        .into_iter()
        .filter(|row| {
            enabled.is_none_or(|wanted| row.enabled == wanted)
                && origin.is_none_or(|wanted| row.origin == wanted)
        })
        .collect();

    drop(inner);

    let total = rows.len();
    let slice = paging::slice_page(rows, cursor, limit, Some(total));

    let revision = ctx.revision("session_tools", Some(&session_id));
    let mut body = paging::page_slice_body(ctx.runtime.hub.epoch(), slice, revision, now_millis());

    if let Some(map) = body.as_object_mut() {
        map.insert(
            "budget".to_owned(),
            json!({ "used": budget.used, "limit": budget.limit }),
        );
    }

    Ok(api::with_revision(200, body, revision))
}

/// `POST {root}/sessions/{session_id}/tools` (`api.ts:3146-3171`): `201 {registration}`.
///
/// Registration goes through the SAME kernel the model's `toolCreate` uses, so validation, the
/// lifetime creation budget and immediate enablement are identical - and a registration compiles the
/// candidate and NEVER runs it.
pub async fn post_session_tool(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &CREATE_FIELDS)?;

    let request: contract::CreateSessionToolRequest = contract::decode_body(Value::Object(body))?;

    let registration = mutate_tool(ctx, &session_id, move |state, host| {
        Store::create_session_tool(state, host, &request, None)
            .map(|registration| serde_json::to_value(registration).unwrap_or(Value::Null))
    })?;

    let revision = ctx.revision("session_tools", Some(&session_id));

    Ok(api::with_revision(
        201,
        json!({ "registration": registration }),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/tools/{tool_id}` (`api.ts:2875-2886`).
///
/// Existence is decided by the ROW set (a catalogue name the session can see is addressable even
/// though it created nothing), and the detail adds the argument schema plus the `{scope, manual}`
/// gate. `json(200, response)` - TS stamps no header here (`:2885`).
pub async fn get_session_tool(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let name = ctx.param("tool_id").to_owned();

    require_session(ctx, &session_id)?;

    let store = store_of(ctx);
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    let detail = store.session_tool_detail(state, &name)?;

    drop(inner);

    plain(json!({ "tool": detail }))
}

/// `GET {root}/sessions/{session_id}/tools/{tool_id}/source` (`api.ts:2888-2902`).
///
/// The store's 404 is already the contract's sentence - `Tool "x" has no installed source revision in
/// session "y".` (`api.ts:2893-2896`) - and the response is the source object itself, not an envelope.
pub async fn get_session_tool_source(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let name = ctx.param("tool_id").to_owned();

    require_session(ctx, &session_id)?;

    let store = store_of(ctx);
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    let source = store.session_tool_source(state, &name)?;

    drop(inner);

    plain(serde_json::to_value(source).unwrap_or(Value::Null))
}

/// `PATCH {root}/sessions/{session_id}/tools/{tool_id}` (`api.ts:3173-3232`).
///
/// Two domain paths behind one verb: a patch that names ONLY `enabled` is the reversible discovery
/// gate, and one that names a declaration half goes through the registration kernel - which enables
/// the name at once, so a patch restating BOTH applies the gate AFTER the registration, because the
/// caller's `enabled` is the later, more specific instruction. The registration envelope stays the
/// answer either way.
pub async fn patch_session_tool(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let name = ctx.param("tool_id").to_owned();

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &PATCH_FIELDS)?;

    if body.is_empty() {
        return Err(empty_patch());
    }

    if let Some(enabled) = body.get("enabled")
        && !enabled.is_null()
        && !enabled.is_boolean()
    {
        return Err(ApiError::bad_request("\"enabled\" must be a boolean."));
    }

    // `api.ts:3199-3203`: a patch whose only key is the gate is a runtime write, not a registration.
    if body.keys().all(|key| key == "enabled") {
        let enabled = body_enabled(&body).unwrap_or(false);

        return set_session_tool_enabled(ctx, &session_id, &name, enabled);
    }

    let request: contract::UpdateSessionToolRequest =
        contract::decode_body(Value::Object(Map::clone(&body)))?;

    let registration = mutate_tool(ctx, &session_id, {
        let name = name.clone();

        move |state, host| {
            Store::update_session_tool(state, host, &name, &request)
                .map(|registration| serde_json::to_value(registration).unwrap_or(Value::Null))
        }
    })?;

    // A patch that restates both applies the gate after the registration (`api.ts:3207-3229`), and the
    // gate's own bump is the figure the header quotes.
    if let Some(enabled) = body_enabled(&body) {
        set_session_tool_enabled(ctx, &session_id, &name, enabled)?;
    }

    let revision = ctx.revision("session_tools", Some(&session_id));

    Ok(api::with_revision(
        200,
        json!({ "registration": registration }),
        revision,
    ))
}

/// `DELETE {root}/sessions/{session_id}/tools/{tool_id}` (`api.ts:3238-3309`).
///
/// Three refusals, kept distinct because they mean different things to a client: no such row -> 404;
/// a `protected` entry (a harness-owned control tool, or a name the frozen definition declares) -> 403
/// because it is not a user-level edit at all; a name this session merely INHERITS, with no
/// session-local registration -> 400 because there is nothing to remove (`:3245-3268`).
pub async fn delete_session_tool(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let name = ctx.param("tool_id").to_owned();

    require_session(ctx, &session_id)?;

    let row = tool_row(ctx, &session_id, &name)
        .ok_or_else(|| missing_in("tool", &name, &session_id))?;

    if row.r#protected {
        return Err(ApiError::forbidden(format!(
            "Tool \"{name}\" is harness-owned in session \"{session_id}\"; a protected entry cannot be unregistered."
        )));
    }

    let installed = {
        let store = store_of(ctx);
        let inner = store.lock();

        inner
            .session(&session_id)
            .is_some_and(|state| state.created_tools.contains_key(&name))
    };

    if !installed && row.origin != SessionToolOrigin::Discovered {
        return Err(ApiError::bad_request(format!(
            "Tool \"{name}\" is inherited by session \"{session_id}\" and has no session-local registration to remove."
        )));
    }

    assert_idle_for_tool_mutation(
        ctx,
        &session_id,
        &format!(
            "DELETE {root}/sessions/{session_id}/tools/{name}",
            root = contract::HARNESS_API_ROOT
        ),
    )?;

    let removed = mutate_tool(ctx, &session_id, move |state, host| {
        Store::delete_session_tool(state, host, &name)
            .map(|result| serde_json::to_value(result).unwrap_or(Value::Null))
    })?;

    let revision = ctx.revision("session_tools", Some(&session_id));

    // `restored_inherited` is the store's honest `false` (the T27 disclosure, `api.ts:3296-3306`).
    Ok(api::with_revision(200, removed, revision))
}

/// `POST {root}/sessions/{session_id}/tools/{tool_id}/executions` (`api.ts:3315-3346`).
///
/// EVERY name resolves in this session's LIVE registry, so a replaced preset, a created tool and a
/// session-owned control tool all run the implementation that session actually holds. The store's
/// guard is RELEASED before the await - it is never held across one (`knowledge.md` "T3", rule 1) -
/// and a failed call is `200 {ok: false}`, never a 5xx.
pub async fn post_session_tool_execution(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let name = ctx.param("tool_id").to_owned();

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &["parameters"])?;

    let store = store_of(ctx);

    // `api.ts:3322-3330`: an unknown name is a request failure, not tool data. The DETAIL read is the
    // authority on both the existence and the `{scope, manual}` gate, and its 404 is the contract's
    // `Unknown tool "x" in session "y".` (`store::tools::unknown_tool`).
    let gate = {
        let inner = store.lock();
        let state = inner
            .session(&session_id)
            .ok_or_else(|| store.unknown_session(&session_id))?;

        store.session_tool_detail(state, &name)?.execution
    };

    // This port's session surface declares every live entry manually callable
    // (`store/tools.rs:296-299`), so the refusal below is the case TS states and no other: a name the
    // gate closes answers `Tool "x" is not callable manually.` (`api.ts:3328-3330`).
    if !gate.is_some_and(|gate| gate.manual) {
        return Err(ApiError::forbidden(format!(
            "Tool \"{name}\" is not callable manually."
        )));
    }

    // `parameters` is optional; an absent field is an empty argument box, exactly like the global
    // route's (`api.ts:3335`, `api/tools.rs:132-138`).
    let parameters = body
        .get("parameters")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));

    let started = now_millis();

    let call = {
        let inner = store.lock();
        let state = inner
            .session(&session_id)
            .ok_or_else(|| store.unknown_session(&session_id))?;

        // A manual mutation must not race a streaming run (`api.ts:3311-3313`; the store owns the
        // sentence, and `store::session_busy_error` is this port's single wording for that gate).
        if state.record.status == "running"
            || state.lanes.values().any(|lane| lane.active_turn.is_some())
        {
            return Err(store::session_busy_error(&session_id, "run a tool"));
        }

        store.tool_ctx(state, &name, parameters, &ids::new_request_id())
    };

    let outcome = ctx.runtime.tools.execute(call).await;
    let duration_ms = now_millis().saturating_sub(started);

    let body = if outcome.ok {
        let mut map = Map::new();

        map.insert("ok".to_owned(), json!(true));
        map.insert("result".to_owned(), json!(outcome.content));

        // `panels`/`media` were peeled off the tool's branded envelope by the runtime, and an empty
        // list is TS's "absent" (`api.ts:596-601`).
        if let Some(panels) = outcome.panels.filter(|panels| !panels.is_null()) {
            map.insert("panels".to_owned(), panels);
        }

        if let Some(media) = outcome.media.filter(|media| !media.is_null()) {
            map.insert("media".to_owned(), media);
        }

        map.insert(
            "duration_ms".to_owned(),
            json!(duration_ms.max(outcome.duration_ms)),
        );

        Value::Object(map)
    } else {
        // A FAILED call reports the same sentence in `content` and `error` and the measured span
        // (`knowledge.md` "T4"); the wire contract carries `error`.
        let message = outcome.error.unwrap_or(outcome.content).trim().to_string();

        let failure = contract::ToolCallFailure {
            ok: false,
            error: contract::HarnessErrorInfo {
                message: if message.is_empty() {
                    format!("Tool \"{name}\" failed.")
                } else {
                    message
                },
            },
            duration_ms,
        };

        serde_json::to_value(failure).unwrap_or(Value::Null)
    };

    Ok(api::json(200, body))
}

// ---------------------------------------------------------------------------
// The shared write recipe
// ---------------------------------------------------------------------------

/// The store handle, spelled once so the three write verbs below read as domain calls.
type Store = store::Store;

/// Runs one toolbox mutation through the store's commit boundary: undo point, mutation, durable
/// write, ONE `session_tools` bump, then the rollback if the write failed (`api.ts:3097-3101` +
/// `:3103`).
///
/// WHY A CLOSURE: all three write verbs need the same guard choreography, and the borrow of the
/// session must END before `commit` serializes the whole store through that same guard.
fn mutate_tool<T>(ctx: &Ctx, session_id: &str, write: T) -> Result<Value, ApiError>
where
    T: FnOnce(&mut store::SessionState, &Store) -> HarnessResult<Value>,
{
    let store = store_of(ctx);
    let mut inner = store.lock();

    let before = {
        let state = inner
            .session(session_id)
            .ok_or_else(|| store.unknown_session(session_id))?;

        state.durable_record()
    };

    let value = {
        let state = inner
            .session_mut(session_id)
            .ok_or_else(|| store.unknown_session(session_id))?;

        write(state, store)?
    };

    let mut commit = CommitOutcome::default();

    commit
        .bumps
        .push(("session_tools".to_owned(), Some(session_id.to_owned())));

    if let Err(error) = store.commit(&inner, session_id, &mut commit) {
        store.rollback_state(&mut inner, session_id, &before);

        return Err(error);
    }

    Ok(value)
}

/// `setSessionToolEnabled` (`api.ts:2926-2989`): one enabled switch, answered in the SAME registration
/// envelope a source update uses. A gate change never mints a source revision, so `source_revision`
/// restates the installed one and `created`/`replaced` are both false.
///
/// It is NOT `async`: `Ctx` is `Send` but not `Sync` (its body reader is a boxed `HttpBody`, which is
/// `Send` only), so an async helper holding `&Ctx` across an await would make the dispatcher's future
/// non-`Send` - the exact bound axum's `Handler` impl requires. Nothing here awaits.
fn set_session_tool_enabled(ctx: &Ctx, session_id: &str, name: &str, enabled: bool) -> ApiResult {
    let row = tool_row(ctx, session_id, name)
        .ok_or_else(|| missing_in("tool", name, session_id))?;

    // A `protected` entry is the control plane the session runs on: the switch is not a user-level
    // edit, and the runtime would refuse it anyway (`api.ts:2936-2944`).
    if row.r#protected {
        return Err(ApiError::forbidden(format!(
            "Tool \"{name}\" is harness-owned in session \"{session_id}\"; a protected entry cannot be enabled or disabled."
        )));
    }

    assert_idle_for_tool_mutation(
        ctx,
        session_id,
        &format!(
            "PATCH {root}/sessions/{session_id}/tools/{name}",
            root = contract::HARNESS_API_ROOT
        ),
    )?;

    let request = contract::UpdateSessionToolRequest {
        enabled: Some(enabled),
        ..contract::UpdateSessionToolRequest::default()
    };

    let registration = mutate_tool(ctx, session_id, move |state, host| {
        Store::update_session_tool(state, host, name, &request)
            .map(|registration| serde_json::to_value(registration).unwrap_or(Value::Null))
    })?;

    let revision = ctx.revision("session_tools", Some(session_id));

    Ok(api::with_revision(
        200,
        json!({ "registration": registration }),
        revision,
    ))
}

/// `assertIdleForToolMutation` (`api.ts:2909-2920`), sentence for sentence: the route names itself, and
/// the reason distinguishes "lanes are running" from "a run is in progress".
fn assert_idle_for_tool_mutation(
    ctx: &Ctx,
    session_id: &str,
    route: &str,
) -> Result<(), ApiError> {
    let store = store_of(ctx);
    let inner = store.lock();
    let state = inner
        .session(session_id)
        .ok_or_else(|| store.unknown_session(session_id))?;

    let active_lanes = state
        .lanes
        .values()
        .filter(|lane| lane.active_turn.is_some())
        .count();

    if state.record.status == "running" || active_lanes > 0 {
        let reason = if active_lanes > 0 {
            format!("{active_lanes} conversation lane(s) are running")
        } else {
            "a run is in progress".to_owned()
        };

        return Err(ApiError::conflict(format!(
            "{route} needs the session idle: {reason}. Stop or wait for the work in flight, then retry."
        )));
    }

    Ok(())
}

/// One toolbox row (`sessionToolRowsOf(record, context).find(..)`, `api.ts:2932` / `:3241`).
#[must_use]
fn tool_row(ctx: &Ctx, session_id: &str, name: &str) -> Option<contract::SessionToolSummary> {
    let store = store_of(ctx);
    let inner = store.lock();
    let state = inner.session(session_id)?;

    let (rows, _total) = store.session_tool_rows(state, &Query::new(), u64::MAX, 0);

    rows.into_iter().find(|row| row.tool.name == name)
}

/// The enum form of one `origin` filter value (`api::enum_param` already proved the spelling).
#[must_use]
fn origin_of(raw: &str) -> SessionToolOrigin {
    match raw {
        "discovered" => SessionToolOrigin::Discovered,
        "created" => SessionToolOrigin::Created,
        _ => SessionToolOrigin::Catalogue,
    }
}

/// The `enabled` member as the contract states it: absent is "no instruction about the gate".
#[must_use]
fn body_enabled(body: &Map<String, Value>) -> Option<bool> {
    body.get("enabled").and_then(Value::as_bool)
}
