//! `GET {root}/sessions/{session_id}/queue` - the durable backlog.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionQueue` (`:2180-2194`).
//!
//! `queue` is one of the counter kinds the REST route owns (`api.ts:934-944`), and the store owns
//! the rows: `store::read::queue_rows_of` walks the legacy array and every lane backlog in pump
//! order, and `QueuedTask::position` is the 0-based slot of THAT ordering while a submit's receipt
//! reports the 1-based ordinal of the same row (the T39 rule, `api.ts:2219-2230`). The route adds
//! only the paging window and the `queue` revision stamp.

use crate::api::{self, ApiResult, Ctx};

use super::sessions::{page_revision, require_session, session_of};

/// `GET {root}/sessions/{session_id}/queue` (`api.ts:2180-2194`).
pub async fn get_session_queue(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let body = ctx
        .runtime
        .store
        .session_queue(&session_id, &ctx.query)
        .await?;

    // Every page read stamps its own collection revision in the contract header (`api.ts:2193`).
    let revision = page_revision(&body);

    Ok(api::with_revision(200, body, revision))
}
