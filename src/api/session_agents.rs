//! `{root}/sessions/{session_id}/agents**` - the per-session agent bindings.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionAgents` (`:2455-2468`), `findAgentBinding`
//! (`:2470-2471`), `getSessionAgent` (`:2473-2482`), `assertDurableRoster` (`:2511-2525`),
//! `patchSessionAgent` (`:2527-2599`), `postSessionAgent` (`:2601-2692`), `deleteSessionAgent`
//! (`:2694-2731`), `patchSessionAgentInstruction` (`:2737-2768`) and `getSessionAgentInstruction`
//! (`:2770-2835`).
//!
//! THE PRECEDENCE LADDER THIS FILE ENFORCES (`api.ts:2621-2651`, restated at `:2698-2706`)
//! ---------------------------------------------------------------------------------------
//! BODY validation runs BEFORE the durability gate, and the durability gate runs before the roster
//! moves. The route mirrors two store rules (a blank name, and the profile-identity rule) purely so
//! that an invalid binding answers 400 on ANY session instead of a 409 about a harness template the
//! client cannot change; every semantic refusal - already a member, roster full, retired profile,
//! the idle gate - stays the store's, because `sessions.ts:4744-4761` owns those sentences.
//!
//! WHY THE ROSTER READ IS TAKEN FROM `store::read` AND NOT FROM A STORE METHOD
//! --------------------------------------------------------------------------
//! `GET /agents/{agentId}` needs one row plus a 404 worded `Unknown agent "x" in session "y".`
//! (`api.ts:2477`), while the store's own roster routes refuse with the instruction subresource's
//! sentence (`Agent "x" is not a member of session "y".`, `api.ts:2775`). The two sentences are
//! different on purpose, so the identified read projects the row itself through the SAME
//! `session_agent_rows_of` the list route uses - one projection, so a list and a row can never
//! disagree about what a member runs with.

use serde_json::{Map, Value, json};

use crate::api::{self, ApiResult, Ctx, missing_in};
use crate::contract::{self, SessionAgentBinding};
use crate::error::ApiError;

use crate::store::read::session_agent_rows_of;

use super::sessions::{number_of, plain, require_session, session_of, store_of};

/// The fields `POST .../agents` accepts (`api.ts:2607`) - a CLOSED body, which is what makes binding
/// an existing trusted profile unable to inject code.
const BIND_FIELDS: [&str; 5] = ["name", "profile", "model", "instruction", "role"];

/// The fields `PATCH .../agents/{agent_id}` accepts (`api.ts:2532`).
const PATCH_FIELDS: [&str; 2] = ["model", "instruction"];

/// The fields `PATCH .../agents/{agent_id}/instruction` accepts (`api.ts:2742`).
const INSTRUCTION_FIELDS: [&str; 1] = ["instruction"];

/// `GET {root}/sessions/{session_id}/agents` (`api.ts:2455-2468`).
///
/// `json(200, response)` with no revision header: the list body carries `revision` itself
/// (`:2464`), so a header would be a second copy of one number.
pub async fn get_session_agents(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);

    let body = store_of(ctx).session_agents(&session_id).await?;

    plain(body)
}

/// `GET {root}/sessions/{session_id}/agents/{agent_id}` (`api.ts:2473-2482`).
///
/// The header quotes the BINDING's own figure (`binding.revision`, `:2481`), which is the
/// `session_agents` counter of this session - one counter for the whole roster, by contract.
pub async fn get_session_agent(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let agent_id = ctx.param("agent_id").to_owned();

    require_session(ctx, &session_id)?;

    let binding = agent_row(ctx, &session_id, &agent_id)
        .ok_or_else(|| missing_in("agent", &agent_id, &session_id))?;

    let revision = binding.revision;
    let response = contract::SessionAgentResponse { agent: binding };

    Ok(api::with_revision(
        200,
        serde_json::to_value(response).unwrap_or(Value::Null),
        revision,
    ))
}

/// `POST {root}/sessions/{session_id}/agents` (`api.ts:2601-2692`).
pub async fn post_session_agent(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let mut body = ctx.body_object().await?;

    api::require_body_fields(&body, &BIND_FIELDS)?;

    // Every free-text member is a string when supplied (`:2609-2615`).
    for field in ["name", "profile", "model", "instruction"] {
        if let Some(value) = body.get(field)
            && !value.is_null()
            && !value.is_string()
        {
            return Err(ApiError::bad_request(format!(
                "\"{field}\" must be a string when supplied."
            )));
        }
    }

    // The role vocabulary is closed (`:2617-2619`).
    if let Some(role) = body.get("role").filter(|role| !role.is_null()) {
        if !matches!(role.as_str(), Some("primary") | Some("subagent")) {
            return Err(ApiError::bad_request(
                "\"role\" must be \"primary\" or \"subagent\".",
            ));
        }
    }

    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();

    if name.is_empty() {
        return Err(ApiError::bad_request(
            "A session agent binding needs a non-blank \"name\".",
        ));
    }

    // An omitted `profile` binds the profile the name spells (`:2633-2635`).
    let profile = body
        .get("profile")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
        .unwrap_or(&name)
        .to_owned();

    // The alias case the contract refuses: the roster key must BE the trusted profile id, because
    // that is what a restart rebuilds the team from (`:2637-2647`).
    if profile != name {
        return Err(ApiError::bad_request(format!(
            "A session agent binding must use its profile's own name: \"name\" is \"{name}\" but \"profile\" is \"{profile}\". The roster key is the trusted profile id, which is what a restart rebuilds this session's team from."
        )));
    }

    // Durability first, before the roster moves (`:2651`).
    assert_durable_roster(ctx, &session_id, "bind an agent")?;

    // The store decodes a `CreateSessionAgentRequest`, whose two identity members are the trimmed
    // ones this route just validated.
    body.insert(
        "name".to_owned(),
        Value::String(name.clone()),
    );
    body.insert("profile".to_owned(), Value::String(profile));

    let created = store_of(ctx)
        .create_session_agent(&session_id, &Value::Object(body))
        .await?;

    // The store's receipt is `{agent, revision}`; the contract's is `{agent}` (`SessionAgentResponse`),
    // and the figure the header quotes is the roster counter the write moved, not the session's.
    let agent = created.get("agent").cloned().unwrap_or(Value::Null);
    let revision = ctx.revision("session_agents", Some(&session_id));

    Ok(api::with_revision(
        201,
        json!({ "agent": agent }),
        revision,
    ))
}

/// `PATCH {root}/sessions/{session_id}/agents/{agent_id}` (`api.ts:2527-2599`).
pub async fn patch_session_agent(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let agent_id = ctx.param("agent_id").to_owned();

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &PATCH_FIELDS)?;

    // A member the session does not run keeps its 404 (`:2534`), and only a known member can be
    // refused for changing nothing.
    if agent_row(ctx, &session_id, &agent_id).is_none() {
        return Err(missing_in("agent", &agent_id, &session_id));
    }

    let stated = PATCH_FIELDS.iter().any(|field| body.contains_key(*field));

    if !stated {
        return Err(ApiError::bad_request("A patch must change at least one field."));
    }

    for field in PATCH_FIELDS {
        if let Some(value) = body.get(field)
            && !value.is_null()
            && !value.is_string()
        {
            return Err(ApiError::bad_request(format!(
                "\"{field}\" must be a string or null."
            )));
        }
    }

    let payload = restore_payload(&body, &PATCH_FIELDS);

    let patched = store_of(ctx)
        .update_session_agent(&session_id, &agent_id, &Value::Object(payload))
        .await?;

    let agent = patched.get("agent").cloned().unwrap_or(Value::Null);
    let revision = ctx.revision("session_agents", Some(&session_id));

    Ok(api::with_revision(200, json!({ "agent": agent }), revision))
}

/// `DELETE {root}/sessions/{session_id}/agents/{agent_id}` (`api.ts:2694-2731`).
pub async fn delete_session_agent(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let agent_id = ctx.param("agent_id").to_owned();

    require_session(ctx, &session_id)?;

    // T28 precedence: a member that does not exist keeps its 404, because "wrong id" says more than a
    // template refusal does (`api.ts:2698-2701`).
    if agent_row(ctx, &session_id, &agent_id).is_none() {
        return Err(missing_in("agent", &agent_id, &session_id));
    }

    assert_durable_roster(ctx, &session_id, "unbind an agent")?;

    let removed = store_of(ctx)
        .delete_session_agent(&session_id, &agent_id)
        .await?;

    // The store answers the domain record in its own camelCase (`store/agents.rs:439-445`); the
    // contract's receipt is snake_case, with the roster counter this write moved.
    let revision = ctx.revision("session_agents", Some(&session_id));

    let result = contract::DeleteSessionAgentResult {
        deleted: true,
        agent_id: agent_id.clone(),
        session_id: session_id.clone(),
        detached_threads: number_of(&removed, "detachedThreads"),
        revision,
    };

    Ok(api::with_revision(
        200,
        serde_json::to_value(result).unwrap_or(Value::Null),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/agents/{agent_id}/instruction` (`api.ts:2770-2835`).
pub async fn get_session_agent_instruction(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);
    let agent_id = ctx.param("agent_id").to_owned();

    instruction_document(ctx, &session_id, &agent_id).await
}

/// `PATCH {root}/sessions/{session_id}/agents/{agent_id}/instruction` (`api.ts:2737-2768`).
///
/// The write verb on the subresource, reached from the document a client is looking at: it runs the
/// SAME store operation the binding PATCH runs and then answers with the GET's document, so `source`
/// names the layer the session actually runs after the write (`:2764-2767`).
pub async fn patch_session_agent_instruction(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let agent_id = ctx.param("agent_id").to_owned();

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &INSTRUCTION_FIELDS)?;

    if !body.contains_key("instruction") {
        return Err(ApiError::bad_request("A patch must supply \"instruction\"."));
    }

    let stated = &body["instruction"];

    if !stated.is_null() && !stated.is_string() {
        return Err(ApiError::bad_request("\"instruction\" must be a string or null."));
    }

    // `null` is the RESTORE instruction (`api.ts:2755`), and serde maps a JSON null on an
    // `Option<Option<String>>` to the same absence as an omitted key (`knowledge.md` "T5a", deviation
    // 4). The store filters a blank string back to "no override", which IS the restore, so the
    // forward re-states the caller's `null` in the one spelling the decoded request can carry.
    let payload = restore_payload(&body, &INSTRUCTION_FIELDS);

    store_of(ctx)
        .update_session_agent(&session_id, &agent_id, &Value::Object(payload))
        .await?;

    // The GET's document, with no body read this time - the PATCH's own body is already consumed, and
    // TS re-runs the read handler, which never looks at a body (`api.ts:2767`).
    instruction_document(ctx, &session_id, &agent_id).await
}

/// The instruction read, with no body policy of its own, so the GET and the PATCH share one answer
/// shape (`api.ts:2795-2806` / `:2825-2834` - both `json(200, response)`, with no revision header).
async fn instruction_document(
    ctx: &mut Ctx,
    session_id: &str,
    agent_id: &str,
) -> ApiResult {
    let body = store_of(ctx)
        .session_agent_instruction(session_id, agent_id)
        .await?;

    plain(body)
}

// ---------------------------------------------------------------------------
// The two route-side rules the store cannot see
// ---------------------------------------------------------------------------

/// `assertDurableRoster` (`api.ts:2511-2525`): the durability gate for the two MEMBERSHIP writes.
///
/// A `scripted` preset replays its shipped factory and ignores the session's own team, and a
/// template-less (injected) preset cannot rebuild a saved team that differs from the registry entry -
/// so on either template a session-scoped roster change is a 201 a restart takes back. Refusing
/// BEFORE the mutation is the honest answer. A model or instruction override is NOT gated, because it
/// retunes a seed inside the roster the record already carries.
fn assert_durable_roster(ctx: &Ctx, session_id: &str, operation: &str) -> Result<(), ApiError> {
    let store = store_of(ctx);
    let inner = store.lock();
    let state = inner
        .session(session_id)
        .ok_or_else(|| store.unknown_session(session_id))?;

    let template = state
        .record
        .preset
        .template
        .clone()
        .unwrap_or_else(|| "live".to_owned());

    if template == "live" || template == "flaky" {
        return Ok(());
    }

    let harness = state.record.preset.name.clone();

    let reason = if template == "scripted" {
        format!(
            "harness \"{harness}\" replays its shipped scripted factory, which ignores the session's own team"
        )
    } else {
        format!(
            "harness \"{harness}\" was injected in memory, so a saved team that differs from the registry entry cannot be rebuilt"
        )
    };

    Err(ApiError::conflict(format!(
        "Cannot {operation} in session \"{session_id}\": {reason}, so a session-scoped agent change \
         would not survive a restart. Create a live harness (POST {root}/harnesses) to bind agents \
         durably.",
        root = contract::HARNESS_API_ROOT
    )))
}

/// The one roster row for one agent (`findAgentBinding`, `api.ts:2470-2471`).
#[must_use]
fn agent_row(ctx: &Ctx, session_id: &str, agent_id: &str) -> Option<SessionAgentBinding> {
    let store = store_of(ctx);
    let inner = store.lock();
    let state = inner.session(session_id)?;

    session_agent_rows_of(state, store.hub(), store.agents())
        .into_iter()
        .find(|row| row.agent_id == agent_id)
}

/// Re-attaches the RESTORE arms of a binding or instruction PATCH (`api.ts:2552-2561`, `:2755`).
///
/// serde maps a JSON `null` and an absent key to the same outer `None` on an `Option<Option<T>>`, so
/// the decoded request cannot tell "no change" from "restore the inherited value". A `null` the body
/// STATED is therefore rewritten to the one value the store reads as a restore: the blank string,
/// which `store::agents.rs:307`/`:312` filters back to `None` - "no override", inherited again.
fn restore_payload(body: &Map<String, Value>, fields: &[&str]) -> Map<String, Value> {
    let mut payload = Map::new();

    for field in fields {
        match body.get(*field) {
            Some(Value::Null) => {
                payload.insert((*field).to_owned(), Value::String(String::new()));
            }
            Some(value) => {
                payload.insert((*field).to_owned(), value.clone());
            }
            None => {}
        }
    }

    payload
}
