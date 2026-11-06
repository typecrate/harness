//! The bootstrap document, health, and the stream mount.
//!
//! TypeScript origin: `src/server/api.ts` - `root` (`:1292-1317`), `health` (`:1319-1324`), and
//! `streamRoute` (`:3900-3912`), plus the capability/limit constants those handlers quote
//! (`shared/harness-api.ts:197-226`, which `contract::service_limits` /
//! `contract::service_capabilities` already carry).

use std::collections::BTreeMap;

use serde_json::json;

use crate::api::{self, ApiResult, Ctx};
use crate::contract;
use crate::error::ApiError;
use crate::events;

/// `GET {root}` - the boot document (`api.ts:1292-1317`).
///
/// CONSTANT SIZE on purpose ("no catalogue, no defaults, no sessions - which is the whole point of
/// the contract's boot rule"): a client that boots this way pays one small request, not 50 MB. The
/// `links` map is the contract's route-link set keyed by route id, and `service` is the published
/// service name rather than this crate's (`contract::SERVICE_NAME`, `api.ts:1307`).
pub async fn get_root(ctx: &mut Ctx) -> ApiResult {
    let mount = contract::HARNESS_API_ROOT;

    // One entry per navigable route (`api.ts:1293-1303`). `HARNESS_ROUTES`
    // (`harness-api.ts:103-194`) is the client-side mirror of the same list.
    let mut links = BTreeMap::new();
    for name in [
        "self",
        "health",
        "config",
        "harnesses",
        "agents",
        "tools",
        "models",
        "sessions",
        "stream",
    ] {
        let path = if name == "self" {
            mount.to_owned()
        } else {
            format!("{mount}/{name}")
        };

        links.insert(name.to_owned(), path);
    }

    let info = contract::ServiceInfo {
        api_root: mount.to_owned(),
        service: contract::SERVICE_NAME.to_owned(),
        api_version: contract::API_VERSION.to_owned(),
        // The crate version, which `src/lib.rs` pins against `package.json`, so the advertised
        // build can never disagree with the artifact that answers.
        build_version: Some(crate::version().to_owned()),
        server_time: events::now_millis(),
        epoch: ctx.runtime.epoch(),
        capabilities: contract::service_capabilities(),
        limits: contract::service_limits(),
        links,
    };

    Ok(api::json(
        axum::http::StatusCode::OK.as_u16(),
        serde_json::to_value(info).unwrap_or_default(),
    ))
}

/// `GET {root}/health` - liveness plus the live session count (`api.ts:1319-1324`).
///
/// `store.size` in TS is the same figure as `store.lock().ids().len()` here: the sessions the
/// store currently holds, restored ones included.
pub async fn get_health(ctx: &mut Ctx) -> ApiResult {
    let sessions = {
        let inner = ctx.runtime.store.lock();

        inner.ids().len() as u64
    };

    let body = json!({
        "ok": true,
        "sessions": sessions,
        "epoch": ctx.runtime.epoch(),
    });

    Ok(api::json(axum::http::StatusCode::OK.as_u16(), body))
}

/// `GET {root}/stream` - the realtime mount (`api.ts:3906-3912`).
///
/// A plain GET that reaches the JSON dispatcher asked for an upgrade it did not send, so the answer
/// is 400 with the contract's sentence (the one `queue.test.ts:1090` and `view.test.ts:779` pin).
/// T5c replaces this with the WebSocket handshake; the capability refusal below is what a
/// deployment that withholds the stream answers with, and it stays because
/// `capabilities.websocket_stream` is the advertisement that decides it.
pub async fn stream_route(ctx: &mut Ctx) -> ApiResult {
    let _ = &ctx.body;

    if !contract::service_capabilities().websocket_stream {
        return Err(api::capability_denied(
            &format!("GET {}/stream", contract::HARNESS_API_ROOT),
            "a live stream on this deployment",
        ));
    }

    Err(ApiError::bad_request(
        "GET /stream expects a WebSocket upgrade (Sec-WebSocket-Key/Version) for the harness.v1 subprotocol.",
    ))
}
