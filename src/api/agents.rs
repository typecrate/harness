//! `{root}/agents` - the trusted agent catalogue and its global overlay.
//!
//! TypeScript origin: `src/server/api.ts` - `referencedBy` (`:1682-1687`), `getAgents`
//! (`:1689-1705`), `findProfile`/`getAgentOne` (`:1707-1719`), `patchAgent` (`:1729-1777`),
//! `deleteAgent` (`:1779-1811`) and `commitAgentEdit` (`:765-779`).
//!
//! THE WRITE MODEL (`api.ts:1721-1728`): the overrides live in the global agent layer, which never
//! mutates the frozen catalogue objects - an effective profile is always a fresh merge. The write
//! commits as ONE durable unit (`state.json` plus `<root>/harness/agents.json`), and the layer is
//! only read when a NEW session builds its agents, which is what makes "future sessions only"
//! structural rather than a promise.
//!
//! DEVIATION FROM THE STAGE BRIEF, recorded in `knowledge.md`: the brief asks `DELETE /agents/{id}`
//! to answer 409 while any harness selects the profile. TypeScript does not: retirement makes a
//! profile unavailable for NEW selections, and an existing session keeps the brains it already
//! built (`api.ts:1799-1810`), which is why `referenced_by` is a REPORT field on the profile read
//! and never a refusal. The TS behaviour is what this port implements, because the TS package is
//! the specification.

use serde_json::{Value, json};

use crate::api::{self, ApiResult, Ctx, missing};
use crate::contract::{self, UpdateAgentRequest};
use crate::error::ApiError;
use crate::events::ResourceRef;
use crate::read;

/// The four fields `PATCH /agents/{agent_id}` accepts (`api.ts:1733`).
const PATCH_FIELDS: [&str; 4] = ["title", "description", "instruction", "guidelines"];

/// Re-attaches the restore arm TS builds from `hasOwnProperty(field) ? value ?? null : ...`
/// (`api.ts:1746-1759`).
///
/// The decoded request can only say "absent", because serde maps a JSON `null` to the outer `None` of
/// an `Option<Option<T>>`. Every key the body actually states with a `null` is therefore rewritten to
/// `Some(None)`, which the agent layer reads as "drop this override and go back to the catalogue".
fn restores(
    mut request: UpdateAgentRequest,
    body: &serde_json::Map<String, Value>,
) -> UpdateAgentRequest {
    if matches!(body.get("title"), Some(Value::Null)) {
        request.title = Some(None);
    }

    if matches!(body.get("description"), Some(Value::Null)) {
        request.description = Some(None);
    }

    if matches!(body.get("instruction"), Some(Value::Null)) {
        request.instruction = Some(None);
    }

    if matches!(body.get("guidelines"), Some(Value::Null)) {
        request.guidelines = Some(None);
    }

    request
}

/// `GET {root}/agents` (`api.ts:1689-1705`): the SELECTABLE profiles, in catalogue order.
///
/// A retired profile is absent from this page (`agent-state.ts` `profiles()` filters it out) while
/// it stays a known identity for an existing session - that is the two-gate rule in
/// `knowledge.md` "T2".
pub async fn get_agents(ctx: &mut Ctx) -> ApiResult {
    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;

    let revision = ctx.revision("agents", None);

    let rows: Vec<contract::AgentSummary> = {
        let agents = ctx
            .runtime
            .agents
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        agents
            .selectable()
            .iter()
            .filter_map(|profile| read::agent_summary_of(&agents, &profile.name, &ctx.runtime.hub))
            .collect()
    };

    let total = rows.len();
    let slice = crate::paging::slice_page(rows, cursor, limit, Some(total));

    Ok(api::page_response(ctx, revision, slice))
}

/// `GET {root}/agents/{agent_id}` (`api.ts:1709-1719`).
///
/// An unknown id and a retired one are both `Unknown agent "x".` here, because this contract has no
/// agent-CREATE route that could revive a retired profile - the shipped catalogue is the only
/// source of a profile (`api.ts:1707`, `findProfile` reads `registry.agents()`, i.e. the EFFECTIVE
/// list).
pub async fn get_agent(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("agent_id").to_owned();

    // Only the harness registry knows which definitions select this profile
    // (`api.ts:1658-1663`), and the registry read comes FIRST: `runtime::save` takes the registry
    // lock before the store's, and inverting that order anywhere would risk a deadlock.
    let referenced_by = ctx
        .runtime
        .registry
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .referenced_by_agent(&name);

    let profile = {
        let agents = ctx
            .runtime
            .agents
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        read::agent_profile_of(&agents, &name, &ctx.runtime.hub, referenced_by.clone())
    }
    .ok_or_else(|| missing("agent", &name))?;

    let revision = profile.revision;
    let response = contract::AgentProfileResponse { agent: profile };

    Ok(api::with_revision(
        200,
        serde_json::to_value(response).unwrap_or_default(),
        revision,
    ))
}

/// `PATCH {root}/agents/{agent_id}` (`api.ts:1729-1777`).
///
/// An explicit `null` RESTORES the shipped catalogue value for that field (the layer removes the
/// override), while an omitted field is no change - which is why the request type is
/// `Option<Option<String>>` (`contract::UpdateAgentRequest`).
pub async fn patch_agent(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("agent_id").to_owned();
    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &PATCH_FIELDS)?;

    // The unknown/retired pre-check (`api.ts:1735-1738`) uses the EFFECTIVE view, so a retired id
    // gets `Unknown agent "x".` rather than the layer's longer retirement sentence.
    let known = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .effective(&name)
        .is_some();

    if !known {
        return Err(missing("agent", &name));
    }

    // `hasOwnProperty` is the whole presence test in TS (`api.ts:1741-1759`), and serde cannot express
    // it on an `Option<Option<T>>`: a JSON `null` and an absent key both decode to the OUTER `None`
    // (probed against this exact shape). So presence is read off the body's own keys, and an explicit
    // `null` is re-attached as the restore arm AFTER the decode - which keeps `decode_body`'s type
    // validation and its error wording untouched. `{"instruction": null}` is therefore a change
    // ("restore the shipped text"), while `{}` is the 400.
    let stated = PATCH_FIELDS.iter().any(|field| body.contains_key(*field));

    if !stated {
        return Err(ApiError::bad_request(
            "A patch must change at least one field.",
        ));
    }

    let request = restores(contract::decode_body(Value::Object(body.clone()))?, &body);

    // An undo point BEFORE the mutation (`api.ts:766`, `const previous = registry.agentRecords()`).
    let undo = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .records();

    let patched = {
        let mut agents = ctx
            .runtime
            .agents
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        agents.update(&name, request, &ctx.runtime.hub)
    };

    let patched = match patched {
        Ok(patched) => patched,
        Err(error) => {
            return Err(error);
        }
    };

    // The overlay file is the second half of the unit; a failure there puts the in-memory decision
    // back so no channel can disagree with the refusal (`api.ts:770-776`).
    if let Err(error) = ctx.runtime.save().and_then(|()| ctx.runtime.save_agents()) {
        ctx.runtime.restore_agents(undo);

        return Err(ApiError::internal(error.message));
    }

    // The collection counter moves with the row counter; only the row figure is a header.

    let _collection = ctx.runtime.hub.bump("agents", None);
    let revision = ctx.runtime.hub.bump("agents", Some(&name));

    // `publish` REPORTS the counters the bumps above already moved, and returns the event the

    // hub fanned out; the route needs neither, so both are named rather than ignored.

    let _event = ctx.runtime.hub.publish(
        "agent.registry_updated",
        &[
            ResourceRef::host("agents"),
            ResourceRef::new("agents", Some(name.as_str())),
        ],
        None,
        None,
        None,
        json!({ "agent_id": name }),
    );

    // `referenced_by` is re-read after the write, exactly as TS does when it builds the response
    // (`api.ts:1772-1774`).
    let referenced_by = ctx
        .runtime
        .registry
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .referenced_by_agent(&name);

    let profile = {
        let agents = ctx
            .runtime
            .agents
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        read::agent_profile_of(&agents, &name, &ctx.runtime.hub, referenced_by)
    };

    let response = contract::AgentProfileResponse {
        agent: profile.unwrap_or(patched),
    };

    Ok(api::with_revision(
        200,
        serde_json::to_value(response).unwrap_or_default(),
        revision,
    ))
}

/// `DELETE {root}/agents/{agent_id}` (`api.ts:1779-1811`).
///
/// Same 404 rule as the patch: an unknown id and an already-retired one are both "nothing to
/// remove", so a repeat DELETE must not answer a second success.
pub async fn delete_agent(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("agent_id").to_owned();

    let known = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .effective(&name)
        .is_some();

    if !known {
        return Err(missing("agent", &name));
    }

    let undo = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .records();

    let retired = {
        let mut agents = ctx
            .runtime
            .agents
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        agents.delete(&name)
    };

    let mut retired = match retired {
        Ok(retired) => retired,
        Err(error) => return Err(error),
    };

    if let Err(error) = ctx.runtime.save().and_then(|()| ctx.runtime.save_agents()) {
        ctx.runtime.restore_agents(undo);

        return Err(ApiError::internal(error.message));
    }

    // Honest durability: with a database root the tombstone is in `<root>/harness/agents.json`, so
    // a restart does NOT resurrect the profile; without one the overlay is process state and the
    // shipped catalogue wins on the next start (`api.ts:1803-1807`,
    // `restored_by_restart: databaseRoot === undefined`). This host always has a root, so the
    // layer's `false` stands; `tombstoned` is the layer's own answer, which is `true` for a
    // retirement it just recorded.
    retired.restored_by_restart = false;

    // The collection counter moves with the row counter; only the row figure is a header.

    let _collection = ctx.runtime.hub.bump("agents", None);
    let revision = ctx.runtime.hub.bump("agents", Some(&name));

    // `publish` REPORTS the counters the bumps above already moved, and returns the event the

    // hub fanned out; the route needs neither, so both are named rather than ignored.

    let _event = ctx.runtime.hub.publish(
        "agent.registry_updated",
        &[
            ResourceRef::host("agents"),
            ResourceRef::new("agents", Some(name.as_str())),
        ],
        None,
        None,
        None,
        json!({ "agent_id": name, "retired": true }),
    );

    Ok(api::with_revision(
        200,
        serde_json::to_value(retired).unwrap_or_default(),
        revision,
    ))
}
