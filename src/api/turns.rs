//! `{root}/sessions/{session_id}/turns/**` - the turn reads.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionTurns` (`:3723-3769`), `getSessionTurn`
//! (`:3791-3798`), `getSessionTurnDisplay` (`:3800-3827`), `getSessionTurnEvents` (`:3829-3852`),
//! `getSessionTurnStep` (`:3854-3864`), `getSessionTurnBlock` (`:3866-3882`) and
//! `getSessionTurnAttachment` (`:3884-3898`) - plus the two lazy detail lookups those last two routes
//! delegate to, `store.stepDetail` (`server/sessions.ts:3897-3967`) and `store.blockDetail`
//! (`:3976-4018`) with its payload builder `blockPayloadOf` (`:1132-1226`).
//!
//! ONE COUNTER PER COMPARISON (`api.ts:3820-3826`, restated at `:3845` and `:3863`)
//! --------------------------------------------------------------------------------
//! The turn, the display tree and the event page all report the OWNING TURN's revision
//! (`turn:<session>:<turn>`): the tree is a pure fold of the turn record, so minting a second
//! counter would create a comparison no client can make. Every handler here therefore stamps that
//! one figure on the header and, where the contract has the member, in the body.
//!
//! THE DISPLAY MERGE IS IN `store::read::TurnState`
//! -----------------------------------------------
//! `ProjectorFactory::create()` receives no turn record (`knowledge.md` "T4"), so the fold states
//! `turnId: ""`, section ids `s{ordinal}` and a row with `id: ""` / `index: 0`. The record is in
//! hand at the read boundary, so `TurnState::row()` and `TurnState::display()` stamp the identity
//! and rewrite every section id to the store's mint (`{turnId}:{ordinal}`,
//! `turn-projector.ts:698`). What is left for these handlers is the contract's envelope.

use serde_json::{Map, Value, json};

use crate::api::{self, ApiResult, Ctx, missing_in};
use crate::contract::{self, RecordedEventRef, SessionTurnStatus};
use crate::error::ApiError;
use crate::events::now_millis;
use crate::paging;
use crate::store;
use crate::store::read::{turn_event_rows_of, turn_resource_of};
use crate::store::turn_key;

use super::sessions::{number_of, require_session, require_thread, require_turn, session_of};

/// The five `status` spellings `GET .../turns` accepts (`api.ts:3732`).
const TURN_STATUSES: [&str; 5] = ["queued", "running", "done", "error", "cancelled"];

/// The two `order` spellings (`api.ts:3729`); the default is `asc`, the roster's own order.
const TURN_ORDERS: [&str; 2] = ["asc", "desc"];

/// `GET {root}/sessions/{session_id}/turns` (`api.ts:3723-3769`).
///
/// Rows come from the per-turn projection in INDEX order, then the `thread_id`/`status` filters
/// apply, then the cursor slices what survives - so the page and `next_cursor` describe the filtered
/// collection and no turn record is copied on this path.
pub async fn get_session_turns(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;
    let thread_id = api::param_of(&ctx.query, "thread_id");
    let status = api::enum_param(&ctx.query, "status", &TURN_STATUSES)?;
    let order = api::enum_param(&ctx.query, "order", &TURN_ORDERS)?.unwrap_or("asc");

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    let rows: Vec<Value> = state
        .turns_in_index_order(order == "desc")
        .into_iter()
        .filter(|turn| {
            thread_id.is_none_or(|wanted| turn.record.thread_id.as_deref() == Some(wanted))
                && status.is_none_or(|wanted| turn.record.status == wanted)
        })
        .map(|turn| turn.row())
        .collect();

    drop(inner);

    let total = rows.len();
    let slice = paging::slice_page(rows, cursor, limit, Some(total));

    let revision = ctx.revision("turns", Some(&session_id));
    let body = paging::page_slice_body(ctx.runtime.hub.epoch(), slice, revision, now_millis());

    Ok(api::with_revision(200, body, revision))
}

/// `GET {root}/sessions/{session_id}/turns/{turn_id}` (`api.ts:3791-3798`).
pub async fn get_session_turn(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let turn_id = ctx.param("turn_id").to_owned();

    require_session(ctx, &session_id)?;
    require_turn(ctx, &session_id, &turn_id)?;

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;
    let turn = state
        .turn(&turn_id)
        .ok_or_else(|| missing_in("turn", &turn_id, &session_id))?;

    let response = contract::TurnResponse {
        turn: turn_resource_of(state, turn, store.hub()),
    };

    drop(inner);

    let revision = ctx.revision("turn", Some(&turn_key(&session_id, &turn_id)));

    Ok(api::with_revision(
        200,
        serde_json::to_value(response).unwrap_or(Value::Null),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/turns/{turn_id}/display` (`api.ts:3800-3827`).
///
/// The tree is the projector's document with the turn's identity stamped in (see the module header);
/// the route adds `revision`, which is the owning turn's figure by definition (`:3820-3822`).
pub async fn get_session_turn_display(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let turn_id = ctx.param("turn_id").to_owned();

    require_session(ctx, &session_id)?;
    require_turn(ctx, &session_id, &turn_id)?;

    let store = ctx.runtime.store.as_ref();
    let mut inner = store.lock();
    let mut display = inner
        .session_mut(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?
        .turn_mut(&turn_id)
        .ok_or_else(|| missing_in("turn", &turn_id, &session_id))?
        .display();

    drop(inner);

    let revision = ctx.revision("turn", Some(&turn_key(&session_id, &turn_id)));

    if let Some(map) = display.as_object_mut() {
        map.insert("revision".to_owned(), json!(revision));
    }

    Ok(api::with_revision(
        200,
        json!({ "display": display }),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/turns/{turn_id}/events` (`api.ts:3829-3852`).
///
/// `?type=` filters in the ORDINAL space, so the cursor keeps addressing recorded positions;
/// `next` therefore names the ordinal AFTER the last event served, and `total_events` is the count of
/// matching events while `partial` is true for as long as the turn is queued or running.
pub async fn get_session_turn_events(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let turn_id = ctx.param("turn_id").to_owned();

    require_session(ctx, &session_id)?;
    require_turn(ctx, &session_id, &turn_id)?;

    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;
    let type_filter = api::param_of(&ctx.query, "type");

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let turn = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?
        .turn(&turn_id)
        .ok_or_else(|| missing_in("turn", &turn_id, &session_id))?;

    let (rows, total, partial) = turn_event_rows_of(turn, cursor, limit, type_filter);
    let served = u64::try_from(rows.len()).unwrap_or(u64::MAX);
    let last = rows.last().map(|row| row.index);
    let recorded = u64::try_from(turn.record.events.len()).unwrap_or(u64::MAX);

    drop(inner);

    // The next cursor exists only for an exactly-full page that has not reached the end of the log
    // (`api.ts:3838-3842`).
    let next = last
        .filter(|last| served == limit && last + 1 < recorded)
        .map(|last| last + 1);

    let revision = ctx.revision("turn", Some(&turn_key(&session_id, &turn_id)));

    let page = contract::TurnEventPage {
        meta: store.page_meta(revision, next, rows.len()),
        turn_id,
        total_events: total,
        partial,
        items: rows,
    };

    Ok(api::with_revision(
        200,
        serde_json::to_value(page).unwrap_or(Value::Null),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/turns/{turn_id}/steps/{step_id}` (`api.ts:3854-3864` +
/// `sessions.ts:3897-3967`).
///
/// The step id IS the recorded event index of the step's opening event - that is what makes the pair
/// resolvable live, after a restart, and from a checkpoint whose journal is gone. The section and the
/// result index come from the compiled display state, because the projection is what made them
/// addressable; the two EVENTS come from the raw record, verbatim.
pub async fn get_session_turn_step(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let turn_id = ctx.param("turn_id").to_owned();
    let step_id = ctx.param("step_id").to_owned();

    require_session(ctx, &session_id)?;
    require_turn(ctx, &session_id, &turn_id)?;

    // `sessions.ts:3902`: the grammar first, so `steps/abc` is a 400 about the id and not a 404 about
    // an index that was never addressable.
    let index = step_id
        .parse::<usize>()
        .ok()
        .filter(|_| !step_id.is_empty() && step_id.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| {
            ApiError::bad_request("A step id must be the recorded event index of the step.")
        })?;

    let unknown =
        || ApiError::not_found(format!("Unknown step \"{step_id}\" in turn \"{turn_id}\"."));

    let store = ctx.runtime.store.as_ref();
    let mut inner = store.lock();
    let state = inner
        .session_mut(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;
    let turn = state
        .turn_mut(&turn_id)
        .ok_or_else(|| missing_in("turn", &turn_id, &session_id))?;

    let events = turn.record.events.clone();
    let times = turn.record.event_times.clone().unwrap_or_default();
    let display = turn.display();

    drop(inner);

    if index >= events.len() {
        return Err(unknown());
    }

    // The step's own record supplies the section (identity scoping) and the settled event's index
    // (`:3919-3930`); a `callId` a nested invocation reused therefore resolves to its own pair.
    let found = find_step(&display, &step_id).ok_or_else(unknown)?;

    let opening = event_ref(&events, &times, index).ok_or_else(unknown)?;

    let result = found
        .result_step_id
        .as_deref()
        .and_then(|id| id.parse::<usize>().ok())
        .filter(|result| *result != index && *result < events.len())
        .and_then(|result| event_ref(&events, &times, result));

    let detail = contract::TurnStepDetail {
        session_id: session_id.clone(),
        turn_id: turn_id.clone(),
        step_id,
        section_id: found.section_id,
        call_id: found.call_id.filter(|call_id| !call_id.is_empty()),
        opening,
        result,
    };

    let revision = ctx.revision("turn", Some(&turn_key(&session_id, &turn_id)));

    Ok(api::with_revision(
        200,
        json!({ "detail": detail }),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/turns/{turn_id}/blocks/{block_id}` (`api.ts:3866-3882` +
/// `sessions.ts:3976-4018`).
///
/// The METADATA comes from the compiled bar itself, so an answer can never disagree with the segment
/// that was clicked; the PAYLOAD comes from the durable record. The bar EXISTS for the kinds the
/// events describe - the fold mints a `tool` block per call and an `assistant` block per final
/// answer (`projector::TurnProjector::push_block`) - and the record-only kinds (`user`/`system`)
/// stay unminted, because the projector seam is created without the prompt (`ports.rs:239-241`);
/// a lookup for one of those is the honest 404, never a synthesized body. The lookup and the
/// payload builder are ported whole, so a block the bar does have answers in full.
pub async fn get_session_turn_block(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let turn_id = ctx.param("turn_id").to_owned();
    let block_id = ctx.param("block_id").to_owned();

    require_session(ctx, &session_id)?;
    require_turn(ctx, &session_id, &turn_id)?;

    if block_id.is_empty() {
        return Err(ApiError::bad_request(
            "A context block id must be the server-minted segment id.",
        ));
    }

    let store = ctx.runtime.store.as_ref();
    let mut inner = store.lock();
    let state = inner
        .session_mut(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;
    let turn = state
        .turn_mut(&turn_id)
        .ok_or_else(|| missing_in("turn", &turn_id, &session_id))?;

    let display = turn.display();
    let events = turn.record.events.clone();
    let times = turn.record.event_times.clone().unwrap_or_default();
    let prompt = turn.record.prompt.clone();
    let steer_texts = turn.record.steer_texts.clone().unwrap_or_default();
    let steers = turn
        .record
        .steers
        .clone()
        .unwrap_or_default()
        .iter()
        .map(|steer| serde_json::to_value(steer).unwrap_or(Value::Null))
        .collect::<Vec<Value>>();
    let sources = state.turn_instruction_sources();

    drop(inner);

    let unknown = || {
        ApiError::not_found(format!(
            "Unknown context block \"{block_id}\" in turn \"{turn_id}\"."
        ))
    };

    let Some((section_agent, block)) = find_block(&display, &block_id) else {
        return Err(unknown());
    };

    let kind = block
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let name = block.get("name").and_then(Value::as_str).map(str::to_owned);
    let range = |key: &str| {
        block
            .get(key)
            .and_then(Value::as_u64)
            .map(|value| value as usize)
    };
    let from = range("eventFrom");
    let to = range("eventTo");

    let mut detail = Map::new();

    if let Some(entries) = block.as_object() {
        for (key, value) in entries {
            detail.insert(key.clone(), value.clone());
        }
    }

    detail.insert("sessionId".to_owned(), json!(session_id));

    match kind.as_str() {
        // The prompt segment answers with the verbatim task text (`:1140-1144`).
        "user" if name.as_deref() == Some("prompt") => {
            detail.insert("text".to_owned(), json!(prompt));
        }
        // A steered segment answers with the text the user typed (`:1147-1197`), matched over the
        // MERGED submit list - the unbounded archive first, then the audit ring.
        "user" => {
            let suffix = block_id
                .rsplit_once("-user-")
                .map_or_else(String::new, |(_, tail)| tail.to_owned());

            let matched = steer_texts
                .iter()
                .chain(steers.iter())
                .find(|submit| {
                    submit
                        .get("prompt")
                        .and_then(Value::as_str)
                        .is_some_and(|_| {
                            let id = submit.get("id").and_then(Value::as_str).unwrap_or_default();

                            !id.is_empty() && suffix == format!("i{id}")
                        })
                })
                .and_then(|submit| submit.get("prompt").and_then(Value::as_str));

            let Some(text) = matched else {
                // `:4006-4011`: a segment the bar draws but the record cannot name is a 404, because
                // "success" with an empty panel is a lie about the bytes.
                return Err(ApiError::not_found(format!(
                    "The text of steered segment \"{block_id}\" is not in this turn's record."
                )));
            };

            detail.insert("text".to_owned(), json!(text));
        }
        // A system segment answers with the recorded instruction sources for its agent (`:1200-1206`).
        "system" => {
            let entries = sources
                .get(&turn_id)
                .and_then(|entries| {
                    entries.iter().find(|entry| {
                        entry.get("agent").and_then(Value::as_str) == Some(section_agent.as_str())
                    })
                })
                .and_then(|entry| entry.get("instructions"))
                .cloned();

            if let Some(entries) = entries {
                detail.insert("entries".to_owned(), entries);
            }
        }
        // A reasoning or assistant segment answers with the concatenated deltas of ITS OWN recorded
        // range (`recordedStreamText`, `:1104-1125`) - never a preview.
        "reasoning" | "assistant" => {
            if let Some(text) = recorded_stream_text(&events, &times, kind.as_str(), from, to) {
                detail.insert("text".to_owned(), json!(text));
            }
        }
        // A tool segment answers with the two halves of the call it describes (`:1227-1252`).
        "tool" => {
            if let Some((args, result)) = tool_pair(&events, from, to) {
                detail.insert("args".to_owned(), json!(args));

                if let Some(result) = result {
                    detail.insert("result".to_owned(), json!(result));
                }
            }
        }
        // An unknown kind keeps its metadata and answers with no payload, which is what the contract's
        // optional members mean.
        _ => {}
    }

    let revision = ctx.revision("turn", Some(&turn_key(&session_id, &turn_id)));

    Ok(api::with_revision(
        200,
        json!({ "block": Value::Object(detail) }),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/turns/{turn_id}/attachments/{attachment_id}`
/// (`api.ts:3884-3898`).
///
/// THE one route that carries `data`: the descriptor rows every other read prints exist precisely so
/// the bytes stay here until a client asks for them (`:3893-3895`).
pub async fn get_session_turn_attachment(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let turn_id = ctx.param("turn_id").to_owned();
    let attachment_id = ctx.param("attachment_id").to_owned();

    require_session(ctx, &session_id)?;
    require_turn(ctx, &session_id, &turn_id)?;

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let attachment = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?
        .turn(&turn_id)
        .ok_or_else(|| missing_in("turn", &turn_id, &session_id))?
        .record
        .attachments
        .clone()
        .unwrap_or_default()
        .into_iter()
        .find(|attachment| attachment.id == attachment_id);

    drop(inner);

    let attachment =
        attachment.ok_or_else(|| missing_in("attachment", &attachment_id, &session_id))?;

    let response = contract::AttachmentResponse { attachment };

    Ok(api::json(
        200,
        serde_json::to_value(response).unwrap_or(Value::Null),
    ))
}

// ---------------------------------------------------------------------------
// The lazy lookups `store.stepDetail` / `store.blockDetail` perform
// ---------------------------------------------------------------------------

/// What one step row contributes to the step detail (`sessions.ts:3915-3930`).
struct FoundStep {
    section_id: String,
    result_step_id: Option<String>,
    call_id: Option<String>,
}

/// Finds the step whose id is `step_id` in the compiled display tree.
///
/// A step NODE is a `tool` or an `action` node (`turn-display.ts`'s discriminated node union); prose
/// and notice nodes share the id space, so the kind test is what keeps a `n3` prose node from
/// answering a `steps/3` lookup.
fn find_step(display: &Value, step_id: &str) -> Option<FoundStep> {
    for section in display.get("sections")?.as_array()?.iter() {
        let section_id = section.get("id").and_then(Value::as_str)?;

        for node in section.get("nodes").and_then(Value::as_array)?.iter() {
            if !matches!(
                node.get("kind").and_then(Value::as_str),
                Some("tool") | Some("action")
            ) {
                continue;
            }

            if node.get("id").and_then(Value::as_str) != Some(step_id) {
                continue;
            }

            return Some(FoundStep {
                section_id: section_id.to_owned(),
                result_step_id: node
                    .get("detail")
                    .and_then(|detail| detail.get("resultStepId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                call_id: node
                    .get("callId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            });
        }
    }

    None
}

/// The section-agent pair a context segment belongs to, plus the segment itself
/// (`sessions.ts:3987-3988`).
fn find_block(display: &Value, block_id: &str) -> Option<(String, Value)> {
    for section in display.get("sections")?.as_array()?.iter() {
        let agent = section.get("agent").and_then(Value::as_str)?;

        for block in section.get("context").and_then(Value::as_array)?.iter() {
            if block.get("id").and_then(Value::as_str) == Some(block_id) {
                return Some((agent.to_owned(), block.clone()));
            }
        }
    }

    None
}

/// One `{index, at?, event}` reference into the recorded log (`:3943-3947`): the pair the contract
/// prints, with the time omitted when the record has none for that ordinal.
fn event_ref(
    events: &[crate::persistence::SessionTurnEvent],
    times: &[u64],
    index: usize,
) -> Option<RecordedEventRef> {
    let event = events.get(index)?;

    Some(RecordedEventRef {
        index: u64::try_from(index).unwrap_or(u64::MAX),
        at: times.get(index).copied().filter(|at| *at > 0),
        event: event.clone(),
    })
}

/// `recordedStreamText` (`sessions.ts:1104-1125`): the concatenated deltas of the recorded events in
/// `[from..to]` of one kind, and NO answer at all when either bound is unrecorded or out of range.
fn recorded_stream_text(
    events: &[crate::persistence::SessionTurnEvent],
    times: &[u64],
    kind: &str,
    from: Option<usize>,
    to: Option<usize>,
) -> Option<String> {
    let (from, to) = from.zip(to)?;

    if from >= events.len() || to >= events.len() {
        return None;
    }

    let mut text = String::new();
    let mut matched = false;

    // The slice is the range, and the loop is over its events in order (`sessions.ts:1227-1240` walks
    // the same half-open range forward).
    for event in &events[from..=to] {
        let delta = match (&event.event, kind) {
            (contract::HarnessEvent::Text { delta, .. }, "assistant") => Some(delta.clone()),
            (contract::HarnessEvent::Reasoning { delta, .. }, "reasoning") => Some(delta.clone()),
            _ => None,
        };

        if let Some(delta) = delta {
            text.push_str(&delta);
            matched = true;
        }
    }

    // The TS guard also requires the range's own events to exist, which the bounds check above
    // already proved; `times` is only consulted for the recorded stamp, so it stays named.
    let _ = times;

    matched.then_some(text)
}

/// The two halves of one tool segment (`sessions.ts:1227-1252`): the call's raw arguments and the
/// result's recorded content, both verbatim, both from inside the segment's own event range.
fn tool_pair(
    events: &[crate::persistence::SessionTurnEvent],
    from: Option<usize>,
    to: Option<usize>,
) -> Option<(String, Option<String>)> {
    let (from, to) = from.zip(to)?;

    let mut args = None;
    let mut result = None;

    // The segment's own event range, clamped to what the record holds. TS walks `from..=to` over the
    // same slice and reads no further than the last event it has (`sessions.ts:1227-1252`), and `get`
    // is how that bound is honoured without panicking on a range the record never reached.
    let last = to.min(events.len().saturating_sub(1));

    for event in events.get(from..=last).unwrap_or(&[]) {
        match &event.event {
            contract::HarnessEvent::ToolCall { call, .. } => {
                args.get_or_insert(call.arguments.clone());
            }
            contract::HarnessEvent::ToolResult { content, .. } => {
                result.get_or_insert(content.clone());
            }
            _ => {}
        }
    }

    args.map(|args| (args, result))
}

/// The turn status spelling one row reports, for a caller that compares rows to a filter value.
#[allow(dead_code)]
#[must_use]
fn status_name(status: SessionTurnStatus) -> &'static str {
    store::turn_status_str(status)
}

/// The body's own revision figure, for a receipt-shaped response whose header quotes the same number.
#[allow(dead_code)]
#[must_use]
fn body_revision(body: &Value) -> u64 {
    number_of(body, "revision")
}

// ---------------------------------------------------------------------------
// Wave 1 (`A1`): the two writes this wave adds to the turn surface
// ---------------------------------------------------------------------------

/// `PATCH {root}/sessions/{session_id}/turns/{turn_id}/blocks/{block_id}`
/// (`patchSessionTurnBlock`, `api.ts:4597-4700`).
///
/// Every refusal the route can raise BEFORE it touches the record is implemented here, in TS's
/// order, with the contract's own sentences (`api.ts:4605-4620`): identity, then the body's shape,
/// then the two axes. The raw body map is the judge rather than a decoded DTO, because TS compares
/// against `undefined`: an explicit `null` is PRESENT and not a boolean, and `"false"` or `0` are
/// typos on this surface, not values to coerce.
///
/// A1 PLACEHOLDER - WHY THE WRITE ITSELF ANSWERS 500
/// ------------------------------------------------
/// The write resolves two joins against the record - the exclusion UNIT behind a recorded call
/// (`blockUnitOf`) and the single assistant entry a `reasoning` segment names (`blockThoughtOf`) -
/// and then flips one flag on a transcript ENTRY. This port's transcript entries
/// (`contract::HarnessMessage`) carry no `included`/`messageId` field yet; adding them touches the
/// 27 construction sites in `store/*`, `turn.rs`, and `transport.rs`, none of which Wave 1 owns. With
/// no flag to flip, the two per-axis 409s (`api.ts:4638-4652`) cannot be reached honestly either, so
/// the refusal sits exactly where the block lookup begins.
///
/// The response the write will answer with is already modelled - [`contract::TurnBlockWriteReceipt`]
/// plus its links - and pinning it here means the wave that lands the entry flags changes this one
/// call rather than the contract.
pub async fn patch_session_turn_block(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let turn_id = ctx.param("turn_id").to_owned();
    let block_id = ctx.param("block_id").to_owned();

    require_session(ctx, &session_id)?;
    require_turn(ctx, &session_id, &turn_id)?;

    let body = ctx.body_object().await?;

    // The two axes, and only the two axes (`requireKnownFields`, `api.ts:4605`).
    api::require_body_fields(&body, &["included", "reasoningIncluded"])?;

    let has_included = body.contains_key("included");
    let has_thinking = body.contains_key("reasoningIncluded");

    if !has_included && !has_thinking {
        return Err(ApiError::bad_request(
            "A context segment write requires at least one of \"included\" and \"reasoningIncluded\".",
        ));
    }

    if has_included && !body["included"].is_boolean() {
        return Err(ApiError::bad_request(
            "A context segment inclusion write requires a boolean \"included\" value.",
        ));
    }

    if has_thinking && !body["reasoningIncluded"].is_boolean() {
        return Err(ApiError::bad_request(
            "A context segment write requires a boolean \"reasoningIncluded\" value.",
        ));
    }

    // A1 placeholder: the joins and the flag. `block_id` is in scope so the eventual message can
    // name the segment the way the read's 404 does.
    let _ = &block_id;

    Err(ApiError::internal(
        "The context segment inclusion write is not implemented on this build; PATCH blocks answers once the transcript entry flags land.",
    ))
}

/// One accepted resume: either the interrupted row put back on its own lane, or the command the
/// interrupted input actually was (`sessions.ts:8800-8810`).
enum Resume {
    /// The requeued row, plus the queue ordinal captured before the pump could drain it.
    Requeued {
        turn_id: String,
        position: Option<u64>,
    },
    /// The command-shaped input, replayed on the command path.
    Command(contract::SubmitTaskRequest),
}

/// `POST {root}/sessions/{session_id}/threads/{thread_id}/resume` (`postThreadResume`,
/// `api.ts:2867-2905`, with the store's decision at `sessions.ts:8671-8990`).
///
/// WHY THIS HANDLER LIVES IN `api/turns.rs` AND NOT `api/threads.rs`: the route is a thread-lane
/// write, so `threads.rs` would be its natural home, but this wave owns `turns.rs` and not
/// `threads.rs`, and the operation is a TURN re-run - it re-opens a recorded turn, which is the
/// thing every other handler in this file reads. Nothing here depends on its location, so a later
/// wave may move the function verbatim.
///
/// THE ANSWER IS A SUBMIT'S RECEIPT (`api.ts:2858-2860`): `200`, not `202`, because the caller asked
/// for one already-recorded task to be re-run rather than for a new one to be accepted - but
/// execution is still server-owned and never awaited, so the body is the acceptance receipt of the
/// REQUEUED row, naming the SAME `turn_id` the stop left behind.
///
/// ROLLBACK: `requeueTurnInPlace` is durable-before-response and rolls every step back if the write
/// fails (`sessions.ts:8936-8965`), so a failed resume leaves the conversation halted and resumable
/// again. The rollback is reproduced here in the same reverse order, with one documented difference:
/// TS also restores the row's display LANE (`turn.lane`), which this port's durable turn record does
/// not carry, so there is nothing to restore.
pub async fn post_thread_resume(ctx: &mut Ctx) -> ApiResult {
    // The route reads no body at all (`api.ts:2867-2905` never touches `ctx.body()`).
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;

    // The SAME guard the cancel route runs, so `resume` cannot 404 a typo'd id differently from
    // `cancel` (`api.ts:2871-2873`).
    require_thread(ctx, &session_id, &thread_id)?;

    let store = ctx.runtime.store.as_ref();
    let accepted_at = now_millis();

    // --- the store's half, in ONE guard -------------------------------------------------------
    // `resumeThread` is synchronous up to the durability fence precisely so a second caller cannot
    // observe the old terminal state, and the double-resume claim it sets for the command path
    // exists to close the window its first `await` opens. This port closes both windows a different
    // way: the whole decision below runs inside one store guard, and the command fallback runs
    // AFTER the guard is released, where the store's own single-flight gate is the authority. (The
    // claim flag itself has no field in `store::ThreadLane`, which this wave does not extend.)
    let accepted: Resume = 'resume: {
        let mut inner = store.lock();
        let state = inner
            .session_mut(&session_id)
            .ok_or_else(|| store.unknown_session(&session_id))?;

        // --- the roster guards (`sessions.ts:8691-8760`) --------------------------------------
        let status = state
            .thread(&thread_id)
            .map_or_else(String::new, |thread| thread.status.clone());

        // `paused` is a PLUGIN park, not a halt, so it states its own reason rather than hiding
        // behind the running sentence.
        if status == "paused" {
            return Err(ApiError::conflict(format!(
                "Thread \"{thread_id}\" is paused; the plugin that parked it unpauses it, not this route."
            )));
        }

        // Only a halted conversation is resumable. `stopped` is this server's own stop and `idle` is
        // what an interrupted run leaves behind; anything else still has work in flight.
        if status != "stopped" && status != "idle" {
            return Err(ApiError::conflict(format!(
                "Thread \"{thread_id}\" is {status}; only a stopped thread can be resumed."
            )));
        }

        // --- the newest recorded turn (`sessions.ts:8762-8786`) --------------------------------
        // Newest by the monotonic `index`, never by array position: a deletion or a restore can
        // reorder the array, and the index is the figure that always states the order.
        let newest = state
            .turns
            .values()
            .filter(|turn| turn.record.thread_id.as_deref() == Some(thread_id.as_str()))
            .max_by_key(|turn| turn.record.index)
            .map(|turn| {
                (
                    turn.record.id.clone(),
                    turn.record.index,
                    turn.record.status.clone(),
                    turn.record.prompt.clone(),
                    turn.record.attachments.clone().unwrap_or_default(),
                    turn.record.references.clone().unwrap_or_default(),
                )
            });

        let Some((turn_id, index, turn_status, prompt, attachments, references)) = newest else {
            // A runtime-driven child conversation records its runs inside its PARENT's turn, so a
            // cascaded child stop has no input the server durably holds to replay. Refusing is
            // honest; inventing a per-child prompt would be new persistence.
            return Err(ApiError::conflict(format!(
                "Thread \"{thread_id}\" has no recorded turn to resume."
            )));
        };

        // "As long as the answer isn't reached": `done` means it was reached.
        if turn_status == "done" {
            return Err(ApiError::conflict(format!(
                "Thread \"{thread_id}\" already reached its answer (turn {index} is done)."
            )));
        }

        // `queued`/`running` means work is already in flight - a stop never leaves either behind on
        // this thread, so reaching here is a concurrent submit, not a resume.
        if turn_status == "queued" || turn_status == "running" {
            return Err(ApiError::conflict(format!(
                "Thread \"{thread_id}\" still has turn {index} {turn_status}."
            )));
        }

        // --- the command fallback (`sessions.ts:8800-8810`) ------------------------------------
        // An interrupted input that WAS a command re-runs as the same command, on the command path,
        // which always mints a new record - the one case whose receipt names a NEW turn id.
        if store::commands::parse_command_line(&prompt) != store::commands::ParsedCommand::None {
            break 'resume Resume::Command(contract::SubmitTaskRequest {
                input: prompt,
                attachments,
                references,
                thread_id: Some(thread_id.clone()),
            });
        }

        // --- `requeueTurnInPlace` (`sessions.ts:8821-8990`) -------------------------------------
        // ROLL THE ABORTED ATTEMPT OUT OF THE RECORD. `attempt_commit` is the runtime's own "this
        // response is in history" boundary; everything after the LAST one belongs to the request
        // that was abandoned, and a cold fold would render it as if it were the answer. A run
        // stopped before its first commit keeps nothing, which is also the shape a fresh submit
        // mints - so the resumed row is indistinguishable from a new one.
        let keep = state.turns.get(&turn_id).map_or(0, |turn| {
            turn.record
                .events
                .iter()
                .rposition(|entry| {
                    matches!(&entry.event, contract::HarnessEvent::AttemptCommit { .. })
                })
                .map_or(0, |boundary| boundary + 1)
        });

        // The tail is kept ASIDE rather than discarded, so a failed write can put the record back
        // exactly as it was; `event_times` stays parallel to `events` in both directions.
        let previous = {
            let turn = state
                .turn_mut(&turn_id)
                .ok_or_else(|| missing_in("turn", &turn_id, &session_id))?;

            let undo = Undo {
                status: turn.record.status.clone(),
                started_at: turn.record.started_at,
                updated_at: turn.record.updated_at,
                usage: turn.record.usage.clone(),
                events: turn.record.events.drain(keep..).collect(),
                times: turn
                    .record
                    .event_times
                    .as_mut()
                    .map(|times| times.drain(keep..).collect()),
            };

            // BACK TO THE PRE-EXECUTION SHAPE. `startedAt` clears because the resumed run restamps
            // it when it opens and a queued record must not carry one; `usage` clears because the row
            // now represents the RESUMED attempt - keeping the stopped run's spend would bill the
            // same prompt twice, in the row and in the session fold.
            turn.record.status = store::turn_status_str(SessionTurnStatus::Queued).to_owned();
            turn.record.started_at = None;
            turn.record.usage = None;
            // Clients order rows by a monotonic `updatedAt`, so the requeue must stamp NEWER than
            // the stop did or its upsert would drop.
            turn.record.updated_at = accepted_at;

            // FORGET THE SETTLED PROJECTION (`sessions.ts:8813-8819`): a terminal turn keeps a
            // settled projector, and without a fresh fold the live tile would freeze at `cancelled`
            // while the resumed run streams into a row nobody renders. `replay` folds the TRUNCATED
            // record through a new projector - which is also this port's whole journal-compaction
            // step, because the restore folds from the durable record and `store/*` never reads the
            // session journal back (`crate::journal` has no reader in this crate), so the truncated
            // indices cannot be resurrected on the next start.
            turn.replay(store.projector_factory().as_ref());

            undo
        };

        state.record.updated_at = accepted_at;

        // THE RESTART BANNER (`sessions.ts:8883-8913`). The restore's recovery policy is a PAIR of
        // session-level writes - the in-flight row becomes `error` AND the session becomes
        // `status: 'error'` with `lastError: INTERRUPTED_MESSAGE` - and that pair is what the shell
        // paints as the pane's red failure note. The requeue IS the replay, so the write that makes
        // it durable retires the note. Scoped deliberately: only the restart sentence, and only
        // while no other lane is working, so a failure a sibling run is still reporting keeps its
        // banner.
        let clears_note = state.record.last_error.as_deref()
            == Some(crate::persistence::INTERRUPTED_TURN_MESSAGE)
            && !state.lane_active();

        let previous_session = (state.record.status.clone(), state.record.last_error.clone());

        if clears_note {
            state.record.last_error = None;

            // BOTH halves: clearing only the text would leave `status: 'error'`, which the same rule
            // renders as its fallback 'session error'. Nothing is running here (that is the guard
            // above), so `idle` is the honest status for a session whose only run is queued.
            if state.record.status == "error" {
                state.record.status =
                    store::status_str(crate::contract::SessionStatus::Idle).to_owned();
            }
        }

        // BACK ON ITS OWN LANE, at the tail (FIFO, the same slot a fresh submit takes). The row is
        // NOT pushed into the session's turn ledger again: it has been there since the original
        // submit, and the list is the record, not the backlog.
        //
        // `stop_requested` is deliberately untouched - TS clears it when the pump CLAIMS a row and
        // this port's pump does the same (`store/turns.rs:828`), so a requeued row on a stopped lane
        // is claimable without the route reaching for the flag.
        store::Store::lane_of(state, &thread_id)
            .queue
            .push(turn_id.clone());

        // Capture the ACCEPTED state BEFORE the pump can start the row again: the receipt and the
        // upsert must state `queued` even though execution may begin in this same tick.
        let position = store::session::queue_position(state, &turn_id);
        let turn_row = state
            .turns
            .get(&turn_id)
            .map(|turn| turn.row())
            .unwrap_or(Value::Null);

        // DURABILITY BEFORE THE RESPONSE (`sessions.ts:8928-8965`): a requeued row the disk does not
        // know about is a prompt the user already watched disappear once. A failed write rolls every
        // step back in the reverse order, so the conversation is halted and resumable again.
        if let Err(error) = store.save(&inner) {
            let state = inner
                .session_mut(&session_id)
                .ok_or_else(|| store.unknown_session(&session_id))?;

            if let Some(lane) = state.lanes.get_mut(&thread_id) {
                lane.queue.retain(|queued| queued != &turn_id);
            }

            if let Some(turn) = state.turn_mut(&turn_id) {
                turn.record.status = previous.status;
                turn.record.started_at = previous.started_at;
                turn.record.updated_at = previous.updated_at;
                turn.record.usage = previous.usage;
                turn.record.events.extend(previous.events);

                if let (Some(times), Some(dropped)) =
                    (turn.record.event_times.as_mut(), previous.times)
                {
                    times.extend(dropped);
                }

                // The fresh projector goes back with the record: an undo that kept the truncated
                // fold would render a row the disk never had.
                turn.replay(store.projector_factory().as_ref());
            }

            if clears_note {
                state.record.status = previous_session.0;
                state.record.last_error = previous_session.1;
            }

            // The store's own refusal, unchanged: this arm is the rollback's exit, and the caller
            // sees exactly the sentence the failed write printed (`sessions.ts:8960-8965`).
            return Err(error);
        }

        // One upsert naming the SAME turn id replaces the cancelled row for every watcher
        // (`sessions.ts:8972-8974`).
        let resources = store::turns::turn_resources(&session_id, &turn_id);
        let targets: Vec<(&str, Option<&str>)> = resources
            .iter()
            .map(|(resource, id)| (resource.as_str(), id.as_deref()))
            .collect();

        store.publish(
            "turn.upsert",
            &targets,
            &session_id,
            Some(&thread_id),
            Some(&turn_id),
            json!({ "turn": turn_row }),
        );

        // The lane pump is fired, never awaited - execution is server-owned (`sessions.ts:8985`).
        store.spawn_pump(&mut inner, &session_id, &thread_id);

        break 'resume Resume::Requeued { turn_id, position };
    };

    // --- the route's half (`api.ts:2884-2903`) -------------------------------------------------
    // Both paths end the same way: the same counter domains a normal submit moves, the same receipt,
    // and the one roster signal only this request can announce.
    let (turn_id, position, status) = match accepted {
        Resume::Command(request) => {
            // The command path mints its own row, its own bumps, and its own signals
            // (`sessions.ts:6047-6101`), so its receipt is the answer; the queue counter still moves
            // because the backlog the observer reads changed shape.
            let receipt = store.submit_command(&session_id, &request).await?;
            let revision = number_of(
                &serde_json::to_value(&receipt).unwrap_or_default(),
                "session_revision",
            );

            let _ = store.bump(&session_id, &[("queue", Some(session_id.as_str()))]);

            publish_thread_upsert(ctx, &session_id, &thread_id);

            return Ok(api::with_revision(
                200,
                serde_json::to_value(&receipt).unwrap_or_default(),
                revision,
            ));
        }
        Resume::Requeued { turn_id, position } => (turn_id, position, SessionTurnStatus::Queued),
    };

    // Same counter domains as a normal submit: the store's own live signals already moved
    // `turns`/`turn`/`turn_display`/`turn_events`, and the roster signal moves `threads`/`thread`.
    // What only this request can know is the session row and the queue view.
    let session_revision = store.bump(
        &session_id,
        &[
            ("session", Some(session_id.as_str())),
            ("queue", Some(session_id.as_str())),
        ],
    );

    let receipt = store::session::receipt(
        &session_id,
        &turn_id,
        status,
        Some(&thread_id),
        None,
        position,
        session_revision,
        accepted_at,
    );

    publish_resume_signals(ctx, &session_id, &thread_id, &turn_id);

    Ok(api::with_revision(
        200,
        serde_json::to_value(receipt).unwrap_or_default(),
        session_revision,
    ))
}

/// The undo point of one requeue (`sessions.ts:8851-8963`): everything the mutation overwrites,
/// captured before it happens.
struct Undo {
    status: String,
    started_at: Option<u64>,
    updated_at: u64,
    usage: Option<contract::HarnessUsage>,
    events: Vec<crate::persistence::SessionTurnEvent>,
    times: Option<Vec<u64>>,
}

/// The `session.updated` frame a resume announces (`api.ts:2898-2901`): the session row and the
/// queue view, the two things only this request knows. The patch is read AFTER the bumps so the
/// frame carries the revision the receipt quotes, and because the requeue may have retired the
/// restart banner, the same frame repaints the pane's failure note (`sessions.ts:8976-8983`).
fn publish_resume_signals(ctx: &Ctx, session_id: &str, thread_id: &str, turn_id: &str) {
    let store = ctx.runtime.store.as_ref();

    let patch = {
        let inner = store.lock();

        inner.session(session_id).map_or(Value::Null, |state| {
            json!({ "session": store::read::session_state_patch_of(state, &ctx.runtime.hub) })
        })
    };

    store.publish(
        "session.updated",
        &[("session", Some(session_id)), ("queue", Some(session_id))],
        session_id,
        Some(thread_id),
        Some(turn_id),
        patch,
    );

    publish_thread_upsert(ctx, session_id, thread_id);
}

/// The one roster signal a resume sends (`sessions.ts:8781`): the conversation went from parked to
/// holding accepted work, so the projected ROW is what a client needs again - the projected row, not
/// a REST-side hint claiming a status this call did not itself set.
fn publish_thread_upsert(ctx: &Ctx, session_id: &str, thread_id: &str) {
    let store = ctx.runtime.store.as_ref();

    let payload = {
        let inner = store.lock();

        inner.session(session_id).map_or(Value::Null, |state| {
            json!({
                "thread": store::read::thread_row_of(state, thread_id),
                "total": u64::try_from(state.threads.len()).unwrap_or(u64::MAX),
            })
        })
    };

    if payload.is_null() {
        return;
    }

    store.publish(
        "thread.upsert",
        &[
            ("threads", Some(session_id)),
            ("thread", Some(&store::thread_key(session_id, thread_id))),
        ],
        session_id,
        Some(thread_id),
        None,
        payload,
    );
}
