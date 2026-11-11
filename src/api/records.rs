//! `GET {root}/sessions/{session_id}/records` - the record-store bootstrap read.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionRecords` (`:2736-2810`), with the two
//! response shapes it answers (`harness-api.ts:1538` `RecordsSnapshotResponse` and `:1556`
//! `RecordsDeltaResponse`) and the signal every live panel is woken by (`:1571` `RecordsSignal`).
//!
//! WHAT THIS ROUTE ANSWERS (`api.ts:2736-2810`)
//! --------------------------------------------
//! One path, two answers. With no `since`, the response is the WHOLE store: every collection with
//! its entries and field spec, plus the retained store-wide change feed, next to the two
//! watermarks (`store_rev`, `collections_rev`) every later delta is compared against. With
//! `?since=N`, the response is the feed after position `N` and nothing else - and when `N` is no
//! longer answerable (the retained window trimmed past it, or the store was replaced by a reset,
//! a restore, or a restart) the answer is still a 200 with `reset: true`, because a stale cursor is
//! a normal race and the honest recovery is a re-bootstrap, not an error.
//!
//! A1 PLACEHOLDER - WHY THIS HANDLER ANSWERS 500
//! ---------------------------------------------
//! The route surface, the query gate, and the response DTOs (`contract::RecordsSnapshotResponse`,
//! `contract::RecordsDeltaResponse`, `contract::RecordCollectionRow`, `contract::RecordEntryRow`,
//! `contract::RecordChangeRow`, `contract::RecordFieldDescriptor`) all landed in this wave, because
//! they are contract. The ENGINE is not part of it: this port has no record store yet (there is no
//! `src/store/records.rs`, and `StoreInner` holds no collections, feed, or watermarks), so there is
//! no `store_rev` to report and no entry to project.
//!
//! The refusal therefore stays inside the contract's error envelope, and it is raised AFTER the
//! session gate, so the part of the route that IS implemented still answers exactly as TS does:
//! an unknown session is a 404, a bad `since` is a 400, a wrong verb is a 405, and only a real
//! session against a store with no engine reaches the 500. A later wave replaces the
//! `record_store_missing` call below with the projection of the engine's own snapshot, and the
//! response type does not move when it does.
//!
//! The 500 is not a capability refusal (`api::capability_denied`, `api-errors.ts:21`): the contract
//! declares no capability for records, so inventing one would put a sentence on the wire that no
//! client is written to expect.

use crate::api::{self, ApiResult, Ctx};
use crate::error::ApiError;

use super::sessions::{require_session, session_of};

/// The one query parameter the route answers for (`api.ts:2740`): a non-negative integer cursor
/// into the retained feed. An absent parameter is the snapshot form, not a zero.
const SINCE_PARAM: &str = "since";

/// `GET {root}/sessions/{session_id}/records` (`api.ts:2736-2810`).
///
/// The gates run in TS's order - session, then query - because "a route that rejects before looking
/// at the query never validates it" is the same laziness rule the dispatcher applies to bodies
/// (`api.ts:4022-4024`).
pub async fn get_session_records(ctx: &mut Ctx) -> ApiResult {
    ctx.ignore_body().await;

    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    // `since` is validated even while the engine is absent: a malformed cursor is a 400 on this
    // contract whatever the store holds, and pinning that here means the placeholder cannot turn a
    // client's typo into a 500 (`api.ts:2740-2752`).
    let since = parse_since(&ctx.query)?;

    // A1 placeholder - no record engine in this build yet. The value TS answers here is
    // `RecordsSnapshotResponse` (no cursor) or `RecordsDeltaResponse` (with one); both are
    // modelled in `contract.rs` and exercised by `tests/wave_one.rs`, and this is the one line that
    // a record-store wave replaces.
    let _ = since;

    Err(record_store_missing())
}

/// The largest position the feed can hold, i.e. JavaScript's `Number.MAX_SAFE_INTEGER`
/// (`api.ts:2755`'s `Number.isSafeInteger`). The check is spelled in Rust because this port counts
/// in `u64`; the NUMBER is the contract's, so a cursor that is a digit string but not a real store
/// position is refused rather than clamped into a `reset` the client never asked for.
const MAX_SAFE_CURSOR: u64 = (1_u64 << 53) - 1;

/// The `since` cursor, read the way TS reads it (`api.ts:2740-2760`): absent means the snapshot
/// form, anything that is not a bare digit string is a 400, and a digit string too large to be a
/// safe integer is a 400 with a DIFFERENT sentence, so a caller can tell "junk" from "impossible
/// position".
///
/// The digit test is spelled out rather than delegated to `u64::from_str`, because Rust's parser
/// accepts a leading `+` (`"+7".parse::<u64>() == Ok(7)`) while TS's `/^\d+$/` does not - and the
/// wire contract is TS's.
///
/// # Errors
///
/// [`ApiError::bad_request`] for either of the two cursor sentences.
fn parse_since(query: &crate::ports::Query) -> Result<Option<u64>, ApiError> {
    let Some(raw) = api::param_of(query, SINCE_PARAM) else {
        return Ok(None);
    };

    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ApiError::bad_request(format!(
            "Query parameter \"{SINCE_PARAM}\" must be a non-negative integer."
        )));
    }

    // A 20+ digit string overflows `u64` and a 17-digit one passes 2^53; both are the second
    // sentence, so the two paths deliberately share a refusal.
    raw.parse::<u64>()
        .ok()
        .filter(|value| *value <= MAX_SAFE_CURSOR)
        .map(Some)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "Query parameter \"{SINCE_PARAM}\" must be a non-negative integer within the safe \
                 integer range."
            ))
        })
}

/// The A1 refusal (`{ "error": { "message": ... } }`, status 500): the route exists and its gates
/// run, but this build has no engine behind it.
#[must_use]
fn record_store_missing() -> ApiError {
    ApiError::internal(
        "The record store is not implemented on this build; GET records answers once the record-store stage lands."
            .to_owned(),
    )
}
