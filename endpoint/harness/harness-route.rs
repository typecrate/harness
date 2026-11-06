//! Route bridge for the `/v1/harness` mount, discovered by `typecrate-services`.
//!
//! `packages/typecrate/services/build.rs` walks the repository root and compiles every
//! `<stem>-route.rs` inside an `endpoint`/`endpoints` tree into the services crate as a
//! module of its own (`build_support/mod.rs` "Route file contract"): this file is a route
//! source because of its NAME and its directory, never because of its contents, and the
//! generated `service_routes.rs` pulls it in with an absolute `#[path]`.
//!
//! There is no harness logic in here. The whole `/v1/harness` REST + stream contract lives in
//! the `typecrate-harness` crate (`packages/typecrate/harness/src/api/mod.rs`), which this file
//! only addresses; `AppState::database_root` is handed over as the harness runtime root, so the
//! services gateway and the standalone `typecrate-harness-server` serve the identical
//! dispatcher. That crate must never depend back on services (no cycle), which is why the
//! delegation goes one way, from the generated route module into the library.
//!
//! Two rules the registration below has to keep, both proven by
//! `packages/typecrate/services/tests/harness_gateway.rs`:
//!
//! * the STATIC `/v1/harness/stream` must win over the `{*rest}` catch-all - matchit prefers a
//!   static segment to a catch-all, so the WebSocket upgrade never reaches the JSON dispatcher
//!   (which would answer its non-upgrade 400 instead of handing the socket over);
//! * nothing here may shadow a services builtin (`/`, `/playground.css`, `/playground.js`,
//!   `/v1/services/routes`), and no other path is mounted: the harness owns `/v1/harness` only.
//!
//! `ANY` rather than a verb list is deliberate and matches the standalone router
//! (`packages/typecrate/harness/src/server.rs:53-62`): the harness dispatcher prints the 405
//! sentence naming the allowed verbs of the route it matched, so axum must not answer the verb
//! test itself - and the same is true of the WebSocket upgrade, which is a `GET` that axum would
//! otherwise 405 before the harness sees its upgrade headers.

use axum::extract::{Request, State};
use axum::response::Response;

/// `ANY /v1/harness` - the bootstrap document (`api::handle_request`, `api.ts:1292-1317`).
pub async fn harness_root(
    State(state): State<crate::server::AppState>,
    request: Request,
) -> Response {
    typecrate_harness::api::handle_request(state.database_root, request).await
}

/// `ANY /v1/harness/stream` - the `harness.v1` WebSocket mount (`api::handle_stream`).
///
/// Verb-agnostic for the same reason as the two other handlers: `crate::stream::upgrade` owns the
/// gate order (origin, capability, method, upgrade tokens, key, version, exact path) and prints its
/// own refusals, so the router has to deliver every verb to it intact - a `GET`-only registration
/// would answer a 405 before the harness ever saw the upgrade headers.
pub async fn harness_stream(
    State(state): State<crate::server::AppState>,
    request: Request,
) -> Response {
    typecrate_harness::api::handle_stream(state.database_root, request).await
}

/// `ANY /v1/harness/{*rest}` - every other mount-relative resource, answered by the same
/// dispatcher: its `Unknown API route: GET /v1/harness/...` 404 and its
/// `Use GET for /v1/harness/...` 405 are part of the contract, and the catch-all's only job is to
/// hand the request over untouched.
pub async fn harness_api(
    State(state): State<crate::server::AppState>,
    request: Request,
) -> Response {
    typecrate_harness::api::handle_request(state.database_root, request).await
}

// The one place the three mounts are written. `ANY` has no `axum::routing::MethodFilter`
// constant, so the `service_routes!` macro routes it through `axum::routing::any` and lists it
// as the literal `"ANY"` (`packages/typecrate/services/src/server/metadata.rs`); the listing is
// still generated from these same tokens, so it cannot disagree with the router.
crate::service_routes! {
    ANY "/v1/harness" => harness_root, "Underload harness service document";
    ANY "/v1/harness/stream" => harness_stream, "Underload harness realtime stream (harness.v1 WebSocket)";
    ANY "/v1/harness/{*rest}" => harness_api, "Underload harness API resources";
}
