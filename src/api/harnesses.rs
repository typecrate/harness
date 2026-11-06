//! `{root}/harnesses` - the harness definition surface.
//!
//! TypeScript origin: `src/server/api.ts` - `cardOf`/`definitionResponseOf` (`:1466-1486`),
//! `getHarnesses` (`:1488-1510`), `postHarness` (`:1512-1559`), `getHarnessOne` (`:1561-1567`),
//! `patchHarness` (`:1569-1616`), `deleteHarness` (`:1618-1644`), `getHarnessInstructions`
//! (`:1646-1662`) and `getHarnessInstructionText` (`:1664-1678`).
//!
//! Every semantic refusal (name pattern, reserved alias, duplicate name, unknown or retired agent,
//! map membership, over-long text) belongs to `crate::registry`, which is what `api.ts:1534-1535`
//! states: "Semantic validation (known agents, duplicates, reserved names, bounds) belongs to the
//! registry, which throws a `HarnessRegistryError` with the right status." This module only moves
//! bytes, decides durability, and advances the counters.

use serde_json::{Value, json};

use axum::http::StatusCode;

use crate::api::{self, ApiResult, Ctx, missing};
use crate::contract::{
    self, CreateHarnessRequest, HarnessDefinitionResponse, HarnessInstructionIndexResponse,
    HarnessInstructionTextResponse, InstructionSource,
};
use crate::error::ApiError;
use crate::events;
use crate::events::ResourceRef;
use crate::read;
use crate::registry::{self, HarnessDescriptor};

/// The eight body fields `POST /harnesses` accepts (`api.ts:1515-1524`).
const CREATE_FIELDS: [&str; 8] = [
    "name",
    "title",
    "description",
    "primary",
    "subagents",
    "agent_models",
    "agent_instructions",
    // `api.ts:1523`: display colors ride the same edge as the other two maps; membership and the
    // hex shape are the registry's decision (`registry.ts:604-629`).
    "agent_colors",
];

/// The seven body fields `PATCH /harnesses/{name}` accepts (`api.ts:1572-1580`), the same set as
/// the create minus the immutable `name`.
const UPDATE_FIELDS: [&str; 7] = [
    "title",
    "description",
    "primary",
    "subagents",
    "agent_models",
    "agent_instructions",
    "agent_colors",
];

/// `GET {root}/harnesses` (`api.ts:1488-1510`): the whole catalogue, paged, with an optional
/// case-insensitive substring filter over `name title description` joined with single spaces.
///
/// The filter bound is not a length cap on `q` (TS applies none); what is bounded is the page, via
/// `?limit=`. A card is a constant-size projection, so the whole catalogue fits in one page today.
pub async fn get_harnesses(ctx: &mut Ctx) -> ApiResult {
    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;
    let search = api::param_of(&ctx.query, "q").map(str::to_lowercase);

    let revision = ctx.revision("harnesses", None);

    // `registry.list()` in TS is the insertion-ordered catalogue; `cards_with` is the same list
    // projected with each entry's real `definition_revision` (`api.ts:1505-1508`).
    let cards: Vec<contract::HarnessCard> = {
        let registry = ctx
            .runtime
            .registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let all = registry.cards_with(&ctx.runtime.hub);

        match &search {
            None => all,
            Some(needle) => all
                .into_iter()
                .filter(|card| {
                    format!("{} {} {}", card.name, card.title, card.description)
                        .to_lowercase()
                        .contains(needle)
                })
                .collect(),
        }
    };

    let total = cards.len();
    let slice = crate::paging::slice_page(cards, cursor, limit, Some(total));

    // `api.ts:1503-1509` calls `json(200, pageMeta(...))` with only TWO arguments: the collection
    // revision rides the page BODY (`pageMeta`'s `revision` field) and never the `X-Harness-Revision`
    // header. Every other page route passes the third argument (`api.ts:1842` for `/tools`,
    // `:1696-1704` for `/agents`), so this one is the exception and not the rule.
    let body = crate::paging::page_slice_body(
        ctx.runtime.hub.epoch(),
        slice,
        revision,
        events::now_millis(),
    );

    Ok(api::json(StatusCode::OK.as_u16(), body))
}

/// `POST {root}/harnesses` (`api.ts:1512-1559`): create one definition, durably, before the 201.
pub async fn post_harness(ctx: &mut Ctx) -> ApiResult {
    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &CREATE_FIELDS)?;

    // The two presence checks the route owns (`api.ts:1526-1532`): the contract's own sentences,
    // which the registry repeats for a blank value. They run BEFORE the decode because a missing
    // field must print the contract's wording, not serde's.
    require_text_field(&body, "name", "A harness definition requires a \"name\".")?;
    require_text_field(
        &body,
        "primary",
        "A harness definition requires a \"primary\" agent.",
    )?;

    let request: CreateHarnessRequest = contract::decode_body(Value::Object(body))?;

    let agents = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    let undo = ctx.runtime.registry_snapshot();

    // The commit is durable BEFORE the 201 and rolls back on a failed write (`api.ts:1536-1552`'s
    // `store.mutateRegistry`, which restores the captured snapshot when the document cannot be
    // written).
    let created = {
        let mut registry = ctx
            .runtime
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        registry.create(request, &agents)
    }
    .inspect_err(|_| {
        // The registry mutation happened and the response is a refusal, so the snapshot taken
        // before it goes back in place (`api.ts:1536-1552`'s rollback).
        ctx.runtime.restore_registry(undo.clone());
    });

    let created = created?;

    if let Err(error) = ctx.runtime.save() {
        ctx.runtime.restore_registry(undo);

        return Err(ApiError::internal(error.message));
    }

    // The collection counter and this definition's counter move together, and the event reports
    // both (`api.ts:1554-1556`).
    let bumps = [
        ("harnesses", None),
        ("definition", Some(created.name.as_str())),
    ];

    ctx.runtime
        .commit_revisions("catalogue.updated", &bumps, Value::Null);

    let definition = definition_of(ctx, &created);
    let revision = ctx.revision("definition", Some(&created.name));

    Ok(api::with_revision(201, definition, revision))
}

/// `GET {root}/harnesses/{harness_name}` (`api.ts:1561-1567`).
///
/// `find` follows a legacy alias (`knowledge.md` T2: "`get` is direct, `find`/`resolve` follow an
/// alias"), so an old client reading `instruct` sees the current `writer` card - while PATCH and
/// DELETE below refuse the alias outright.
pub async fn get_harness(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("harness_name");

    let found = {
        let registry = ctx
            .runtime
            .registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        registry.find(name).cloned()
    };

    let definition = found.ok_or_else(|| missing("harness", name))?;

    let body = definition_of(ctx, &definition);
    let revision = ctx.revision("definition", Some(&definition.name));

    Ok(api::with_revision(200, body, revision))
}

/// `PATCH {root}/harnesses/{harness_name}` (`api.ts:1569-1616`).
///
/// An existing session keeps its frozen copy, so this affects FUTURE sessions only - which is why
/// a saved conversation survives the deletion of its harness.
pub async fn patch_harness(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("harness_name").to_owned();
    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &UPDATE_FIELDS)?;

    let request: contract::UpdateHarnessRequest = contract::decode_body(Value::Object(body))?;

    // `A patch must change at least one field.` is the registry's, checked BEFORE the lookup,
    // exactly as the route orders it (`api.ts:1582-1592`).
    let agents = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    let undo = ctx.runtime.registry_snapshot();

    let updated = {
        let mut registry = ctx
            .runtime
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        registry.update(&name, request, &agents)
    }
    .inspect_err(|_| {
        // The registry mutation happened and the response is a refusal, so the snapshot taken
        // before it goes back in place (`api.ts:1536-1552`'s rollback).
        ctx.runtime.restore_registry(undo.clone());
    });

    let updated = updated?;

    if let Err(error) = ctx.runtime.save() {
        ctx.runtime.restore_registry(undo);

        return Err(ApiError::internal(error.message));
    }

    // Bump the collection and this definition, then publish: the event reports the settled figures
    // (`api-events.ts:299-316`), so the header revision and the frame cannot disagree.
    // The collection counter moves with the row counter; only the row figure is a header.
    let _collection = ctx.runtime.hub.bump("harnesses", None);
    let revision = ctx.runtime.hub.bump("definition", Some(&updated.name));

    // `publish` REPORTS the counters the bumps above already moved, and returns the event the

    // hub fanned out; the route needs neither, so both are named rather than ignored.

    let _event = ctx.runtime.hub.publish(
        "catalogue.updated",
        &[
            ResourceRef::host("harnesses"),
            ResourceRef::new("definition", Some(updated.name.as_str())),
        ],
        None,
        None,
        None,
        Value::Null,
    );

    let body = definition_of(ctx, &updated);

    Ok(api::with_revision(200, body, revision))
}

/// `DELETE {root}/harnesses/{harness_name}` (`api.ts:1618-1644`).
pub async fn delete_harness(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("harness_name").to_owned();
    let undo = ctx.runtime.registry_snapshot();

    let removed = {
        let mut registry = ctx
            .runtime
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        registry.delete(&name)
    }
    .inspect_err(|_| {
        // The registry mutation happened and the response is a refusal, so the snapshot taken
        // before it goes back in place (`api.ts:1536-1552`'s rollback).
        ctx.runtime.restore_registry(undo.clone());
    });

    let removed = removed?;

    if let Err(error) = ctx.runtime.save() {
        ctx.runtime.restore_registry(undo);

        return Err(ApiError::internal(error.message));
    }

    // The collection counter moves with the row counter; only the row figure is a header.

    let _collection = ctx.runtime.hub.bump("harnesses", None);
    let _definition = ctx.runtime.hub.bump("definition", Some(&name));

    // `publish` REPORTS the counters the bumps above already moved, and returns the event the

    // hub fanned out; the route needs neither, so both are named rather than ignored.

    let _event = ctx.runtime.hub.publish(
        "catalogue.updated",
        &[ResourceRef::host("harnesses")],
        None,
        None,
        None,
        Value::Null,
    );

    // No revision header: TS passes `(result as {revision?}).revision`, and a delete receipt does
    // not carry one (`api.ts:1640-1643`).
    Ok(api::json(
        200,
        serde_json::to_value(removed).unwrap_or_default(),
    ))
}

/// `GET {root}/harnesses/{harness_name}/instructions` (`api.ts:1646-1662`).
///
/// Not paged: membership is bounded by the definition, so the whole index already IS the bounded
/// projection.
pub async fn get_harness_instructions(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("harness_name");

    let agents = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    let entries = {
        let registry = ctx
            .runtime
            .registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let definition = registry
            .find(name)
            .cloned()
            .ok_or_else(|| missing("harness", name))?;

        registry::instruction_summary_of(&definition, &agents)
    };

    let total = u64::try_from(entries.len()).unwrap_or(u64::MAX);

    let response = HarnessInstructionIndexResponse {
        harness_name: name.to_owned(),
        total,
        items: entries,
    };

    Ok(api::json(
        200,
        serde_json::to_value(response).unwrap_or_default(),
    ))
}

/// `GET {root}/harnesses/{harness_name}/instructions/{agent_id}` (`api.ts:1664-1678`).
///
/// A non-member is a 404 naming the harness by its CANONICAL name (`api.ts:1671-1673`), which is
/// why the lookup goes through `find` (alias-resolved) rather than the raw path value.
pub async fn get_harness_instruction_text(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("harness_name");
    let agent = ctx.param("agent_id");

    let agents = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();

    let response = {
        let registry = ctx
            .runtime
            .registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let definition = registry
            .find(name)
            .cloned()
            .ok_or_else(|| missing("harness", name))?;

        if !definition.agents.iter().any(|member| member == agent) {
            return Err(ApiError::not_found(format!(
                "Agent \"{agent}\" is not a member of harness \"{}\".",
                definition.name
            )));
        }

        let (instruction, guidelines, source) =
            registry::instruction_text_of(&definition, agent, &agents);

        // `chars` is `instruction.length` (`api-reads.ts` `instructionTextOf`), i.e. UTF-16 code
        // units. `projector.rs:98` already counts the same way for the same reason; a grapheme count
        // would disagree with TS for any astral character in an instruction.
        let chars = u64::try_from(instruction.encode_utf16().count()).unwrap_or(u64::MAX);

        HarnessInstructionTextResponse {
            harness_name: definition.name,
            agent: agent.to_owned(),
            source: source_of(&source),
            chars,
            instruction,
            guidelines,
        }
    };

    Ok(api::json(
        200,
        serde_json::to_value(response).unwrap_or_default(),
    ))
}

/// `MemberInstructionSource` -> the wire enum (the two share the same three spellings).
fn source_of(source: &registry::MemberInstructionSource) -> InstructionSource {
    match source.as_str() {
        "harness" => InstructionSource::Harness,
        "global" => InstructionSource::Global,
        _ => InstructionSource::Catalogue,
    }
}

/// The `{harness: HarnessDefinition}` envelope for one resolved definition, stamped with the
/// current counters (`definitionResponseOf`, `api.ts:1474-1486`).
fn definition_of(ctx: &Ctx, definition: &HarnessDescriptor) -> Value {
    let agents = ctx
        .runtime
        .agents
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let built = read::harness_definition_of(definition, &ctx.runtime.hub, &agents);

    let response = HarnessDefinitionResponse { harness: built };

    drop(agents);

    serde_json::to_value(response).unwrap_or_else(|_| json!({"harness": {}}))
}

/// One required non-empty string field, checked on the raw body before the typed decode.
fn require_text_field(
    body: &serde_json::Map<String, Value>,
    key: &str,
    message: &str,
) -> Result<(), ApiError> {
    match body.get(key) {
        Some(Value::String(text)) if !text.is_empty() => Ok(()),
        _ => Err(ApiError::bad_request(message)),
    }
}
