//! `GET {root}/sessions/{session_id}/usage` - the folded token accounting.
//!
//! TypeScript origin: `src/server/api.ts` - `getSessionUsage` (`:2153-2159`).
//!
//! The fold is `store::read::usage_fold_of` (`api-reads.ts:725-767`): counts walked off the recorded
//! turns, per-conversation shares, and `usage` ABSENT (never zero) when no provider reported
//! anything. The route adds one thing only - the `usage` counter stamp, which is the figure the
//! response body already carries (`SessionUsageResponse.revision`, `api.ts:2158`).

use crate::api::{self, ApiResult, Ctx};

use super::sessions::{number_of, require_session, session_of};

/// `GET {root}/sessions/{session_id}/usage` (`api.ts:2153-2159`).
pub async fn get_session_usage(ctx: &mut Ctx) -> ApiResult {
    ctx.require_empty_body().await?;

    let session_id = session_of(ctx);

    require_session(ctx, &session_id)?;

    let body = ctx.runtime.store.session_usage(&session_id).await?;
    let revision = number_of(&body, "revision");

    Ok(api::with_revision(200, body, revision))
}
