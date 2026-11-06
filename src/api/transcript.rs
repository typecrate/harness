//! `{root}/sessions/{session_id}/threads/{thread_id}/transcript*` - transcript reads and the one
//! write verb.
//!
//! TypeScript origin: `src/server/api.ts` - `getTranscriptMetadata` (`:3556-3568`),
//! `getTranscriptMessages` (`:3570-3616`), `getTranscriptDocument` (`:3618-3638`) and
//! `putTranscript` (`:3640-3719`).
//!
//! THE THREE REVISION FIGURES (`knowledge.md` "T3"), and which route prints which
//! --------------------------------------------------------------------------------
//! * the HUB's `transcript:<session>:<thread>` counter - the metadata read, the document read and
//!   the write receipt's optimistic lock all quote THIS one (`api.ts:3567`, `:3631`, `:3656`);
//! * the hub's `transcript_messages:<session>:<thread>` counter - the paged read reports it, because
//!   a page is a message-list view (`api.ts:3605`);
//! * the THREAD RECORD's own `transcriptRevision` - what the store's own `if_revision` guard
//!   compares against (`store/transcript.rs:254`).
//!
//! `put_transcript` moves the first two through the commit's bump list and the third inside the
//! record, so the handler pre-checks the hub figure exactly where `api.ts:3660-3671` does and the
//! store keeps its independent guard. The document read therefore has its `revision` member
//! OVERWRITTEN with the hub figure: the store's builder prints the record's counter
//! (`store/transcript.rs:215-219`), and TS's route prints the hub's.
//!
//! A structurally broken transcript is a SUCCESSFUL write (`api.ts:3673-3676`): it is stored raw,
//! the live value becomes a valid placeholder, and the receipt explains it with `accepted_as: raw`
//! plus the recorded error. That is why `messages` stays an untyped `Value` here.

use serde_json::{Value, json};

use crate::api::{self, ApiResult, Ctx};
use crate::contract;
use crate::error::ApiError;

use super::sessions::{number_of, require_session, require_thread, session_of, thread_key};

/// The four `role` spellings the paged read accepts (`api.ts:3576`).
const ROLES: [&str; 4] = ["system", "user", "assistant", "tool"];

/// The fields `PUT .../transcript` accepts (`api.ts:3645`).
const PUT_FIELDS: [&str; 2] = ["messages", "if_revision"];

/// `GET {root}/sessions/{session_id}/threads/{thread_id}/transcript` (`api.ts:3556-3568`).
pub async fn get_transcript_metadata(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;
    require_thread(ctx, &session_id, &thread_id)?;

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    let response = contract::TranscriptMetadataResponse {
        transcript: store.transcript_metadata(state, &thread_id, &agent_of(state, &thread_id)),
    };

    drop(inner);

    let revision = ctx.revision("transcript", Some(&thread_key(&session_id, &thread_id)));

    Ok(api::with_revision(
        200,
        serde_json::to_value(response).unwrap_or(Value::Null),
        revision,
    ))
}

/// `PUT {root}/sessions/{session_id}/threads/{thread_id}/transcript` (`api.ts:3640-3719`).
///
/// `messages` must be PRESENT: a loader that reduced a malformed body to `{}` must not silently
/// EMPTY a transcript (`:3647-3651`). `if_revision` is THE one optimistic lock in this contract, and
/// an integer is the only acceptable spelling (`:3661-3663`) - a float, a string or a boolean all
/// refuse, while `null` is "not supplied" because TS's `!== undefined` test passes it through to the
/// comparison... and `typeof null === 'object'` refuses it. So `null` is the same 400 as any other
/// non-number, which is what the store's decoded `Option` cannot express.
pub async fn put_transcript(ctx: &mut Ctx) -> ApiResult {
    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &PUT_FIELDS)?;

    if !body.contains_key("messages") {
        return Err(ApiError::bad_request(
            "A transcript update requires a \"messages\" value.",
        ));
    }

    require_thread(ctx, &session_id, &thread_id)?;

    let key = thread_key(&session_id, &thread_id);
    let current = ctx.revision("transcript", Some(&key));

    let if_revision = match body.get("if_revision") {
        None => None,
        Some(value) => {
            let stated = value.as_i64().or_else(|| {
                value
                    .as_f64()
                    .filter(|number| number.fract() == 0.0)
                    .map(|n| n as i64)
            });

            let Some(expected) = stated else {
                return Err(ApiError::bad_request("\"if_revision\" must be an integer."));
            };

            if expected != i64::try_from(current).unwrap_or(i64::MAX) {
                return Err(ApiError::conflict(format!(
                    "This transcript is at revision {current}; the write expected {expected}. Re-read before writing."
                )));
            }

            Some(expected as u64)
        }
    };

    let request = contract::UpdateThreadTranscriptRequest {
        messages: body["messages"].clone(),
        if_revision,
    };

    let store = ctx.runtime.store.as_ref();
    let mut inner = store.lock();

    let (receipt, mut outcome) =
        store.put_transcript(&mut inner, &session_id, &thread_id, &request)?;

    // The write's own bumps (`transcript`, `transcript_messages`, `thread`) are already in the
    // outcome; this is the durable boundary that moves them, and it runs before the receipt answers.
    store.commit(&inner, &session_id, &mut outcome)?;

    drop(inner);

    // DEVIATION, reported: the store builds these links in the pre-R5 shape (`store/transcript.rs:397`,
    // `{root}/sessions/{id}/transcript/{thread}`), which is NOT a path this router mounts. TS's route
    // builds them itself, from the mount it is served under (`api.ts:3705-3715`), and the handler is
    // the only layer this stage may change - so the three URLs are rewritten to the mounted shape here
    // rather than left as links a client cannot follow.
    let mut receipt = receipt;
    let root = crate::contract::HARNESS_API_ROOT;
    let base = format!("{root}/sessions/{session_id}/threads/{thread_id}");

    if receipt.links.is_some() {
        // All three members are `Option` in the generated type because the contract marks them
        // optional; TS's route always fills all three, so this does too.
        receipt.links = Some(contract::TranscriptWriteLinks {
            transcript: Some(format!("{base}/transcript")),
            messages: Some(format!("{base}/transcript/messages?limit=25")),
            document: Some(format!("{base}/transcript/document")),
        });
    }

    let revision = receipt.revision;

    Ok(api::with_revision(
        200,
        serde_json::to_value(receipt).unwrap_or(Value::Null),
        revision,
    ))
}

/// `GET {root}/sessions/{session_id}/threads/{thread_id}/transcript/messages`
/// (`api.ts:3570-3616`).
///
/// The cursor addresses recorded POSITIONS, so a role filter must not renumber the collection -
/// `total` stays the whole transcript's entry count, which the store's page builder already does
/// together with the "an exactly-full page only" cursor rule (`api.ts:3605`).
pub async fn get_transcript_messages(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;
    require_thread(ctx, &session_id, &thread_id)?;

    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;

    // The enum check is the route's (`api.ts:3576`); an unrecognized role is a 400 naming the four,
    // not an empty page.
    api::enum_param(&ctx.query, "role", &ROLES)?;

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    let page = store.transcript_page(
        state,
        &thread_id,
        &agent_of(state, &thread_id),
        &ctx.query,
        limit,
        cursor,
    );

    drop(inner);

    let body = serde_json::to_value(page).unwrap_or(Value::Null);
    let revision = number_of(&body, "revision");

    Ok(api::with_revision(200, body, revision))
}

/// `GET {root}/sessions/{session_id}/threads/{thread_id}/transcript/document`
/// (`api.ts:3618-3638`).
///
/// An UNREADABLE conversation answers with its RAW value - the text the user is fixing - and
/// `content_type` is `application/json` on both paths.
pub async fn get_transcript_document(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);
    let thread_id = ctx.param("thread_id").to_owned();

    require_session(ctx, &session_id)?;
    require_thread(ctx, &session_id, &thread_id)?;

    let store = ctx.runtime.store.as_ref();
    let inner = store.lock();
    let state = inner
        .session(&session_id)
        .ok_or_else(|| store.unknown_session(&session_id))?;

    let mut body = serde_json::to_value(store.transcript_document(
        state,
        &thread_id,
        &agent_of(state, &thread_id),
    ))
    .unwrap_or(Value::Null);

    drop(inner);

    // TS quotes the HUB figure on this read (`api.ts:3631`); the store's builder prints the thread
    // record's own counter, so the route restates it with the number the contract compares against.
    let revision = ctx.revision("transcript", Some(&thread_key(&session_id, &thread_id)));

    if let Some(map) = body.as_object_mut() {
        map.insert("revision".to_owned(), json!(revision));
    }

    Ok(api::with_revision(200, body, revision))
}

/// The agent one conversation belongs to, as the three transcript reads print it
/// (`api.ts:3561` / `:3607` / `:3630`): the roster row's agent, else the invalid record's, else the
/// empty string - a transcript with no known agent still needs a string in that member.
fn agent_of(state: &crate::store::SessionState, thread_id: &str) -> String {
    state
        .thread(thread_id)
        .map(|thread| thread.agent.clone())
        .or_else(|| {
            state
                .invalid_threads
                .get(thread_id)
                .and_then(|invalid| invalid.agent.clone())
        })
        .unwrap_or_default()
}
