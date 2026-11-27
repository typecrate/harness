//! `{root}/sessions/{session_id}/agents**` - the per-session agent bindings.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionAgents` (`:3963-3977`), `findAgentBinding`
//! (`:3978-3980`), `getSessionAgent` (`:3981-4018`), `assertDurableRoster` (`:4019-4034`),
//! `patchSessionAgent` (`:4035-4175`), `postSessionAgent` (`:4176-4299`), `deleteSessionAgent`
//! (`:4300-4342`), `patchSessionAgentInstruction` (`:4343-4375`) and `getSessionAgentInstruction`
//! (`:4376-4448`). THE LINE NUMBERS IN THIS FILE WERE RE-DERIVED FROM THE CURRENT `api.ts` while the
//! `prefix` write landed, because the ones it carried predated the U6/B3 route growth and no longer
//! point at the handlers they name.
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

/// The fields `POST .../agents` accepts (`api.ts:4188` - `requireKnownFields(body, ['name', 'profile',
/// 'model', 'instruction', 'rank', 'prefix'])`) - a CLOSED body, which is what makes binding an existing
/// trusted profile unable to inject code.
///
/// THE OLD FIELD `role` IS NOT ON THIS LIST, and that is deliberate rather than an oversight: the
/// contract's `CreateSessionAgentRequest` is these six members under `additionalProperties: false`
/// (`harness-api.ts:1883-1942`), and `api.ts:4184-4186` states the rule out loud - "the OLD field `role`
/// is not accepted at all - it lands in the unknown-field refusal below". So a legacy body does not get a
/// quiet reinterpretation, it gets `Unknown field "role". Allowed fields: name, profile, model,
/// instruction, rank, prefix.` (`tests/http_sessions.rs` pins both this list and that refusal).
const BIND_FIELDS: [&str; 6] = ["name", "profile", "model", "instruction", "rank", "prefix"];

/// The fields `PATCH .../agents/{agent_id}` accepts (`api.ts:4040` in the live route:
/// `requireKnownFields(body, ['model', 'instruction', 'rank', 'prefix'])`).
///
/// ALL FOUR of TS's names now, `rank` included: the gap this comment used to describe was the wire row,
/// not the route - [`crate::contract::SessionAgentBinding`] now carries the `rank` member
/// `harness-api.ts:1770` requires, so the authority write has something to project and the field is
/// accepted exactly as TypeScript accepts it. The refusal sentence this gate answers names these four
/// in this order, and `tests/http_sessions.rs:2829` pins it verbatim (NOT `tests/http_host.rs`, which is
/// where an earlier revision of this comment claimed the pin lived).
const PATCH_FIELDS: [&str; 4] = ["model", "instruction", "rank", "prefix"];

/// The fields `PATCH .../agents/{agent_id}/instruction` accepts (`api.ts:2742`).
const INSTRUCTION_FIELDS: [&str; 1] = ["instruction"];

/// `GET {root}/sessions/{session_id}/agents` (`api.ts:2455-2468`).
///
/// `json(200, response)` with no revision header: the list body carries `revision` itself
/// (`:2464`), so a header would be a second copy of one number.
pub async fn get_session_agents(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);

    let body = store_of(ctx).session_agents(&session_id).await?;

    plain(body)
}

/// `GET {root}/sessions/{session_id}/agents/{agent_id}` (`api.ts:2473-2482`).
///
/// The header quotes the BINDING's own figure (`binding.revision`, `:2481`), which is the
/// `session_agents` counter of this session - one counter for the whole roster, by contract.
pub async fn get_session_agent(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

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

    // THE RANK VOCABULARY, at the edge and BEFORE the durability gate — TS's precedence, restated at
    // `api.ts:4199` ("Calling it HERE, before the durability and idle gates, keeps the established
    // precedence: an invalid body answers 400 on ANY session, not 409 about a busy one"). The retired
    // two-valued `role` this block replaced is gone with R2: a body that still sends one is refused by
    // the closed-field gate above, so nothing here can quietly reinterpret it into a rank.
    //
    // AN OMITTED OR `null` RANK IS NOT AN ERROR: `harness-api.ts:1932-1937` states that arm as "asks for
    // NO authority", and the row it answers reports the frozen definition's letter for the member, or the
    // LOWEST letter when no map names it (`api-reads.ts:1564`, `rankOfMember`).
    require_binding_rank(body.get("rank"))?;

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

    // U6 ON THE BIND BODY TOO: `prefix` is one of the six members the contract's
    // `CreateSessionAgentRequest` carries (`harness-api.ts:1922-1931`), and TypeScript validates it at the
    // edge with the ONE band validator before anything moves (`api.ts:4231-4249`, "a malformed array is a
    // 400 on ANY session rather than a 409 about a scripted template"). The label is the bare `` `prefix` ``
    // rather than TS's PATCH label `` `prefix.<agent>` `` for one reason: a bind has no member to name yet,
    // and TS's own POST call passes no member (`api.ts:4238`, `requirePrefixTurns(body.prefix, 'prefix')`).
    //
    // A STATED `null` IS NOT A WRITE HERE, and that is a real difference from the PATCH: the bind's `null`
    // arm asks for NOTHING of its own (`harness-api.ts:1927-1931`), while the PATCH's `null` restores the
    // definition's band. `api.ts:4273-4276` spells the consequence - `bindingPrefix` stays `undefined` and
    // the field is not forwarded at all - so the `null` is dropped from the payload below.
    if let Some(prefix) = body.get("prefix").filter(|prefix| !prefix.is_null()) {
        crate::registry::require_prefix_turns(prefix, "prefix", "")?;
    }

    // Durability first, before the roster moves (`:2651`).
    assert_durable_roster(ctx, &session_id, "bind an agent")?;

    // The store decodes a `CreateSessionAgentRequest`, whose two identity members are the trimmed
    // ones this route just validated.
    body.insert("name".to_owned(), Value::String(name.clone()));
    body.insert("profile".to_owned(), Value::String(profile));

    if body.get("prefix").is_none_or(|prefix| prefix.is_null()) {
        body.remove("prefix");
    }

    let created = store_of(ctx)
        .create_session_agent(&session_id, &Value::Object(body))
        .await?;

    // The store's receipt is `{agent, revision}`; the contract's is `{agent}` (`SessionAgentResponse`),
    // and the figure the header quotes is the roster counter the write moved, not the session's.
    let agent = created.get("agent").cloned().unwrap_or(Value::Null);
    let revision = ctx.revision("session_agents", Some(&session_id));

    Ok(api::with_revision(201, json!({ "agent": agent }), revision))
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
        return Err(ApiError::bad_request(
            "A patch must change at least one field.",
        ));
    }

    // THE RANK HALF, validated HERE and before the band (`api.ts:4050-4058`: "one validator for the
    // binding rank, at the HTTP edge for the same reason POST validates it here — an invalid body answers
    // 400 before any store gate can turn it into a 409 about a busy or started session"). TS runs
    // `validateBindingRank` and THEN the stated-but-no-authority check, which is why a malformed letter and
    // a stated `null` answer two different sentences; both are reproduced verbatim by the two helpers.
    let rank = require_patched_rank(body.get("rank"))?;

    // U6: THE PRESET-HISTORY BAND, validated HERE rather than left to the store (`api.ts:4052-4077`).
    // The reason is TS's, stated at that site: the same patch carries a model and an instruction half the
    // store would already have written, so a band with a `tool` turn, a content block, or a 65th turn has
    // to answer 400 naming the turn index BEFORE a busy-session gate can answer 409 about a body that was
    // never valid. `null` is not validated here - TS skips it too (`:4061`), because it names no band: it
    // is the undo, and the store resolves it to the frozen definition's own history.
    //
    // THE ORDER AHEAD OF THE OVERRIDE SHAPES IS LOAD-BEARING: TS validates the band at `:4060` and only
    // then type-checks `instruction` at `:4095`, so a body that is wrong about BOTH answers about the band
    // (`tests/http_sessions.rs` pins that precedence).
    //
    // THE ONE VALIDATOR, shared with the harness-definition write and with the store's own re-check, so
    // PATCHing this band into a definition and into a session answers the same 400 either way, with only
    // the `prefix.<agent>[<i>]` label distinguishing them (`crate::registry::require_prefix_turns`).
    if let Some(prefix) = body.get("prefix")
        && !prefix.is_null()
    {
        crate::registry::require_prefix_turns(prefix, "prefix", &agent_id)?;
    }

    for field in ["model", "instruction"] {
        if let Some(value) = body.get(field)
            && !value.is_null()
            && !value.is_string()
        {
            return Err(ApiError::bad_request(format!(
                "\"{field}\" must be a string or null."
            )));
        }
    }

    // THE DURABLE-ROSTER GATE, for the rank arm only, and only when a rank is actually stated
    // (`api.ts:4103-4110`): "unlike a model/instruction override, a rank change rewrites the team a restart
    // resolves, so it takes the same durable-roster gate as bind/detach: a scripted or injected template
    // cannot honour the saved map, and answering 200 would be a lie the next boot takes back." A band or an
    // override stays writable on every template, which is why this gate sits inside the rank arm and never
    // ahead of the patch.
    if rank.is_some() {
        assert_durable_roster(ctx, &session_id, "change an agent rank")?;
    }

    // `restore_payload` re-attaches the RESTORE arms of the two OVERRIDES by rewriting a stated `null`
    // into the blank string the store reads as "no override" (`:405-410` above). A band has no such
    // string to borrow - `[]` is a real choice that reads back as `source: session`, while `null` reads
    // back as `source: harness` - so `prefix` moves to the store VERBATIM and the store tells the two
    // arms apart itself (`store::agents.rs`'s prefix write).
    let mut payload = restore_payload(&body, &["model", "instruction"]);

    // THE RANK MOVES AS THE LETTER THE ROUTE VALIDATED, not as the caller's JSON value: the validator proved
    // it is one upper-case letter, and re-emitting `AgentRank::as_str` is what stops a body that spelled the
    // letter some other way (or a `null`, already refused above) reaching the store's decoder.
    if let Some(rank) = rank {
        payload.insert("rank".to_owned(), Value::String(rank.as_str().to_owned()));
    }

    if let Some(prefix) = body.get("prefix") {
        payload.insert("prefix".to_owned(), prefix.clone());
    }

    let patched = store_of(ctx)
        .update_session_agent(
            &session_id,
            &agent_id,
            &Value::Object(payload),
            crate::store::agents::AgentPatchRoute::Binding,
        )
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
    ctx.ignore_body().await;

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
        return Err(ApiError::bad_request(
            "A patch must supply \"instruction\".",
        ));
    }

    let stated = &body["instruction"];

    if !stated.is_null() && !stated.is_string() {
        return Err(ApiError::bad_request(
            "\"instruction\" must be a string or null.",
        ));
    }

    // `null` is the RESTORE instruction (`api.ts:2755`), and serde maps a JSON null on an
    // `Option<Option<String>>` to the same absence as an omitted key (`knowledge.md` "T5a", deviation
    // 4). The store filters a blank string back to "no override", which IS the restore, so the
    // forward re-states the caller's `null` in the one spelling the decoded request can carry.
    let payload = restore_payload(&body, &INSTRUCTION_FIELDS);

    store_of(ctx)
        .update_session_agent(
            &session_id,
            &agent_id,
            &Value::Object(payload),
            crate::store::agents::AgentPatchRoute::Instruction,
        )
        .await?;

    // The GET's document, with no body read this time - the PATCH's own body is already consumed, and
    // TS re-runs the read handler, which never looks at a body (`api.ts:2767`).
    instruction_document(ctx, &session_id, &agent_id).await
}

/// The instruction read, with no body policy of its own, so the GET and the PATCH share one answer
/// shape (`api.ts:2795-2806` / `:2825-2834` - both `json(200, response)`, with no revision header).
async fn instruction_document(ctx: &mut Ctx, session_id: &str, agent_id: &str) -> ApiResult {
    let body = store_of(ctx)
        .session_agent_instruction(session_id, agent_id)
        .await?;

    plain(body)
}

/// `GET {root}/sessions/{session_id}/agents/{agent_id}/prefix` (`getSessionAgentPrefix`,
/// `api.ts:4449-4510`): one member's PRESET HISTORY - the band it actually seeds, plus which layer
/// wrote it.
///
/// WHY A DOCUMENT OF ITS OWN: a band is a LIST OF PROMPT-LENGTH STRINGS, so it stays out of the roster
/// row (the same payload rule that keeps instruction text out of it) and is addressed per agent. The
/// route is READ-ONLY by itself: the write is the session-agent PATCH's `prefix` field
/// ([`patch_session_agent`], `api.ts:4128-4134`), and THIS route only projects the band the record
/// carries - so a client editing a band and a client reading one never disagree about which call owns
/// the change, and the `source` the read reports is decided by the write's three arms
/// (band / `[]` / `null`).
///
/// `json(200, response)` with NO revision header (`api.ts:4510`), exactly like the instruction
/// subresource: the body is a projection of state another route owns.
pub async fn get_session_agent_prefix(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let agent_id = ctx.param("agent_id").to_owned();

    // The session-404-then-membership order is the store's, and it is TS's: `requireSession` runs
    // before any roster question, so a request naming a session that never existed never learns which
    // agents it does not have.
    let response = store_of(ctx)
        .session_agent_prefix(&session_id, &agent_id)
        .await?;

    plain(serde_json::to_value(response).unwrap_or(Value::Null))
}

// ---------------------------------------------------------------------------
// The two route-side rules the store cannot see
// ---------------------------------------------------------------------------

/// `validateBindingRank` (`sessions.ts:1205-1222`), the ONE rank reader: `POST` calls it for the edge
/// exactly as `api.ts:4200` does, and `PATCH` calls it through [`require_patched_rank`].
///
/// THE THREE ARMS ARE THE WHOLE FUNCTION, and TS's ordering of them is the contract's:
/// - absent or `null` is `Ok(None)` — "ABSENT IS NOT AN ERROR: `undefined`/`null` means 'no authority
///   requested' and returns `undefined`, leaving the caller to resolve it" (`sessions.ts:1199-1203`). On a
///   bind the caller resolves it to the definition's letter or the lowest one; nothing here guesses.
/// - one upper-case `A-Z` letter is `Ok(Some(letter))`, read through [`contract::agent_rank_of_text`] so
///   this edge, the registry's rank map, and the kernel's own guard agree on what a rank IS.
/// - anything else is the ONE refusal, and it never reinterprets the value: `'a'` is not `A`, `1` is not a
///   letter, and the retired `primary` is not a rank either ("the validator NEVER reinterprets it as a
///   rank, which is the whole point of not keeping a compat shim", `sessions.ts:1195-1198`).
fn require_binding_rank(stated: Option<&Value>) -> Result<Option<contract::AgentRank>, ApiError> {
    let Some(value) = stated.filter(|value| !value.is_null()) else {
        return Ok(None);
    };

    match value.as_str().and_then(contract::agent_rank_of_text) {
        Some(rank) => Ok(Some(rank)),
        None => Err(ApiError::bad_request(contract::binding_rank_refusal())),
    }
}

/// THE PATCH ARM, which is TS's two-step check at `api.ts:4051-4058` reproduced in TS's order:
/// `validateBindingRank` first, THEN `if (body.rank !== undefined && rank === undefined)`.
///
/// The second step can only fire for a STATED `null`, and that is the point: a PATCH has no restore arm for
/// `rank` — a letter is the only value that means anything — so a caller who writes `{"rank": null}` gets
/// [`contract::rank_letter_refusal`] rather than the silent "no authority requested" a bind would accept.
/// An absent key stays `Ok(None)`: "an omitted property is no change" (`harness-api.ts:1952`).
fn require_patched_rank(stated: Option<&Value>) -> Result<Option<contract::AgentRank>, ApiError> {
    let Some(value) = stated else {
        return Ok(None);
    };

    if value.is_null() {
        return Err(ApiError::bad_request(contract::rank_letter_refusal()));
    }

    require_binding_rank(Some(value))
}

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

    session_agent_rows_of(state, store.hub(), inner.agents())
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
