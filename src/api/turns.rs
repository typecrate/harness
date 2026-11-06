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

use super::sessions::{number_of, require_session, require_turn, session_of};

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
    events: &[contract::HarnessEvent],
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
    events: &[contract::HarnessEvent],
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
        let delta = match (event, kind) {
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
    events: &[contract::HarnessEvent],
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
        match event {
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
