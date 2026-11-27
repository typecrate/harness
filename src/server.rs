//! The axum surface: one mount, one router factory, one graceful-shutdown serve helper.
//!
//! TypeScript origin: the two ADAPTERS of `src/server/api.ts`, merged into one because the Rust port
//! has one transport: `src/server/handler.ts` (`createHarnessHandler`, the standalone node:http
//! adapter) and `src/server/service.ts` (`createHarnessServiceHandler`, the gateway adapter). Both
//! do the same four things, and `service.ts:553-556` states the ordering rule this file copies -
//! "a 413 must not require a store, but reading a stream needs no store either - the order only
//! matters because a rejected runtime creation should answer before any body work" - which is why
//! nothing here touches the filesystem: the root is carried as data and `api::handle_request`
//! resolves the runtime per request.
//!
//! THE ONE MOUNT (`harness-api.ts:39`, `api.ts:282-283`): `STANDALONE_MOUNT` and `SERVICE_MOUNT` are
//! the same constant, so the paths below are the paths the contract publishes. There is no `/api`
//! alias and no second mount (`api.ts:3984-3989`), and everything outside the mount is a 404 that
//! NAMES `/v1/harness` - which the fallback route does by handing the request to the same
//! dispatcher rather than by inventing a second 404 message.
//!
//! THE DELIVERY SURFACE (added by the C3 port of `src/server/start.ts:396-414` + `src/server/static.ts`)
//! -----------------------------------------------------------------------------------------------------
//! A standalone listener answers three things, in this order, and the order comes straight out of
//! `start.ts:404-414`:
//!
//! ```text
//!   1. the dispatcher        - every path under /v1/harness (`handler.ts:277` takes the mount and
//!                              hands over everything else)
//!   2. the static mirror     - GET/HEAD inside a directory the HOST named (`static.ts:72`)
//!   3. plain `Not found`     - `start.ts:165-174`, text/plain, 10 bytes, no mention of the mount
//! ```
//!
//! Those three steps are the whole surface: `Delivery` carries the assets root and nothing else,
//! because the reference has no HTML document of its own - `api.ts:6149-6320` publishes the route
//! list, `start.ts:404-414` answers the rest with the mirror or `sendNotFound`, and
//! `static.ts:21-25` has no `/docs` arm. `docs_page`/`GET /docs` was a crate invention and is gone.
//! The delivery half defaults to OFF, so `router()` - the shape the `packages/typecrate/services`
//! gateway mounts - keeps serving exactly the contract surface it served before, including step 3's
//! replacement by the dispatcher's mount sentence.

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use tokio::net::TcpListener;

use crate::api;
use crate::contract;
use crate::error::ApiError;
use crate::runtime;
use crate::static_files::{self, StaticMirror};

/// The non-contract half of one listener: the built UI mirror, and nothing else.
///
/// BOTH ARE OFF BY DEFAULT, and that default is the reference's, not a convenience: `start.ts:396-402`
/// states that "this server no longer carries a default" for the static root and `dev.ts:123` passes
/// `staticRoot: null`, so an API-only host - the `packages/typecrate/services` gateway mount above all
/// - gets exactly the surface it had before this type existed.
///
/// It is data rather than a live handler for the same reason the database root is data: the router is
/// built once per process and a mirror is a directory, not a resource.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delivery {
    /// The directory to serve outside the API mount (`start.ts:400-402`'s resolved `staticRoot`).
    /// `None` is the mirror being OFF, which is the reference's contract for a bare listener.
    pub static_root: Option<PathBuf>,
}

impl Delivery {
    /// A listener that serves the mirror at `root` and nothing else.
    #[must_use]
    pub fn with_static_root(root: impl Into<PathBuf>) -> Self {
        Self {
            static_root: Some(root.into()),
        }
    }

    /// True when this listener answers anything outside the mount, which is what decides the terminal
    /// 404: a listener with a delivery surface ends at `start.ts:165-174`'s plain-text `Not found`,
    /// and a pure API listener keeps the dispatcher's JSON sentence naming `/v1/harness`.
    #[must_use]
    pub fn serves_outside_mount(&self) -> bool {
        self.static_root.is_some()
    }
}

/// The frozen state one router carries: the root it dispatches against plus the built mirror.
#[derive(Clone)]
struct Surface {
    database_root: PathBuf,
    /// The built UI mirror, already rooted at a normalised directory (`static.ts:31-32`).
    mirror: Option<StaticMirror>,
    /// True when this listener serves ANYTHING outside the mount, which is what picks the terminal
    /// 404: the reference's plain `Not found` here, the dispatcher's mount sentence otherwise.
    serves_outside: bool,
}

impl Surface {
    fn new(database_root: PathBuf, delivery: &Delivery) -> Self {
        Self {
            database_root,
            // Built once, like `createStaticHandler(root)` (`static.ts:31`): the root is normalised at
            // construction so every request's containment test compares two clean paths.
            mirror: delivery
                .static_root
                .as_ref()
                .map(|root| StaticMirror::new(root.clone())),
            serves_outside: delivery.serves_outside_mount(),
        }
    }
}

/// The whole HTTP surface, frozen with the database root it serves and no delivery surface.
///
/// The root is the router's only state, and it is a `PathBuf` rather than a live runtime: the store
/// is created on the FIRST request for that root (`src/server/runtime.ts:299-308`, and the same
/// lazy rule `runtime.ts:3-11` states for the route modules - "route modules only describe a URL and
/// a handler"). A process that serves several roots therefore registers several of these routers,
/// each with its own store, registry and epoch, and `--database-root` is just the single-root case.
///
/// * `ANY {root}` - the bootstrap document plus every host route (`api.ts:3925-3974`);
/// * `ANY {root}/stream` - the realtime upgrade (`api.ts:3906-3912` until T5c takes it over);
/// * `ANY {root}/{*rest}` - every mount-relative path, including the 404 and 405 answers;
/// * fallback - anything outside the mount, which the dispatcher answers with the mount sentence.
///
/// `any` rather than a per-verb registration is deliberate: the 405 sentence names the allowed
/// verbs of the matched ROUTE (`api.ts:4006-4013`), which only the dispatcher can print, so axum
/// must not answer the verb test itself.
///
/// No `#[must_use]` on this factory: `axum::Router` is itself `#[must_use]` (axum-0.8.9
/// `src/routing/router.rs`), and restating it is `clippy::double_must_use` - the same rule
/// `packages/typecrate/services/src/server/mod.rs:289-291` documents for its own `app()`.
pub fn router(database_root: PathBuf) -> Router {
    // The API-only shape, i.e. the shape every existing caller (the gateway mount included) had
    // before the delivery surface existed. Behaviour is unchanged, down to which 404 answers an
    // out-of-mount path.
    router_with(database_root, Delivery::default())
}

/// The same surface plus the delivery half: the static mirror.
///
/// THE ORDER IS THE REFERENCE'S (`start.ts:404-414`):
///
/// ```text
///   api(request)              -> the mount, always first; `handler.ts:277` hands over everything else
///   mirror(request)           -> GET/HEAD inside the mirror root only (`static.ts:72`)
///   plain `Not found`         -> `start.ts:165-174`, text/plain, does NOT name the mount
/// ```
///
/// The mount keeps its registrations, so a request under `/v1/harness` never reaches the mirror -
/// which is what keeps `GET /v1/harness/health` a dispatcher answer even on a listener whose mirror
/// happens to contain a `v1` directory.
pub fn router_with(database_root: PathBuf, delivery: Delivery) -> Router {
    let mount = contract::HARNESS_API_ROOT;

    Router::new()
        .route(mount, any(rest))
        .route(contract::HARNESS_WS_STREAM_PATH, any(stream))
        .route(&format!("{mount}/{{*rest}}"), any(rest))
        // One fallback for everything outside the mount. Registering the mirror as a `get(...)`
        // route instead would make axum answer a `POST /index.html` with its own 405, while the
        // reference answers that request with the plain 404 (`static.ts:72` declines a write verb and
        // `start.ts:409` then sends `Not found`). One handler keeps the verb answers the reference's.
        .fallback(delivery_fallback)
        .with_state(Surface::new(database_root, &delivery))
}

async fn rest(State(surface): State<Surface>, request: Request) -> Response {
    api::handle_request(surface.database_root, request).await
}

async fn stream(State(surface): State<Surface>, request: Request) -> Response {
    api::handle_stream(surface.database_root, request).await
}

/// Every path outside the API mount, in the reference's order.
async fn delivery_fallback(State(surface): State<Surface>, request: Request) -> Response {
    let path = request.uri().path();

    // `handler.ts:277` decides by `pathname.startsWith(STANDALONE_MOUNT)`, NOT by segment: a path
    // like `/v1/harnessXYZ` is the DISPATCHER's (it 404s the mount sentence there), and the mirror
    // must not turn it into the entry document. The router only registers the exact mount and
    // `{mount}/{*rest}`, so this is where the remaining prefix-shaped paths are routed.
    if path.starts_with(contract::HARNESS_API_ROOT) {
        return api::handle_request(surface.database_root, request).await;
    }

    // 1. The mirror owns everything outside the mount (`handler.ts:277`), for the two verbs it
    //    accepts (`static.ts:72`). A decline is not an answer: it falls through.
    if let Some(mirror) = &surface.mirror
        && let Some(reply) = mirror.handle(request.method(), path)
    {
        return reply.into_response();
    }

    // 2. A listener that serves a mirror ends where `start.ts:409` ends: the plain `Not found`.
    //    This is also the answer a POST/PUT/PATCH/DELETE to a mirror path gets - NOT a 405,
    //    because nothing in the reference 405s an unmounted path.
    if surface.serves_outside {
        return plain_not_found();
    }

    // 3. No delivery surface at all: the dispatcher's mount sentence, unchanged from before.
    api::handle_request(surface.database_root, request).await
}

/// `start.ts:165-174`: 404, `text/plain; charset=utf-8`, body `Not found`, and no mention of the
/// mount. Kept here rather than in `static_files` because this is the listener's terminal answer.
fn plain_not_found() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, static_files::NOT_FOUND_TYPE)
        .header(header::CONTENT_LENGTH, static_files::NOT_FOUND_BODY.len())
        .body(Body::from(static_files::NOT_FOUND_BODY))
        .expect("a literal header set builds")
}

/// Binds `addr` and serves the API-only router until `shutdown` resolves.
pub async fn serve<F>(database_root: PathBuf, addr: SocketAddr, shutdown: F) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;

    serve_on(listener, database_root, shutdown).await
}

/// Binds `addr` and serves the router PLUS a delivery surface (the mirror) until `shutdown` resolves.
pub async fn serve_with<F>(
    database_root: PathBuf,
    addr: SocketAddr,
    delivery: Delivery,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;

    serve_on_with(listener, database_root, delivery, shutdown).await
}

/// Serves an already-bound listener, which is what lets the executable print the address it REALLY
/// got (`--port 0` asks for any free port, so the requested address is not the answer) and what
/// lets a test hold a socket on `127.0.0.1:0`.
///
/// The shutdown arm runs the stream half of the host's close step BEFORE the listener finishes
/// draining (`start.ts:319-340` closes the mount, then disposes the store): every live connection
/// gets its `error` frame plus close `1001` while the socket is still open, and the store stops
/// claiming queued work so a turn that has not started cannot start behind the drain. An upgraded
/// WebSocket is an in-flight request to hyper, so a stream that is never told to leave would hold
/// graceful shutdown open until the client's own timeout.
pub async fn serve_on<F>(
    listener: TcpListener,
    database_root: PathBuf,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_on_with(listener, database_root, Delivery::default(), shutdown).await
}

/// [`serve_on`] for a listener that also serves the built UI mirror. The
/// WebSocket upgrade wiring is identical either way: `listen` needs nothing beyond this router to
/// accept an upgrade at `/v1/harness/stream`, because the stream route is registered on the same
/// listener (`start.ts:416-419` attaches the upgrade to the ONE server that answers the REST mount).
pub async fn serve_on_with<F>(
    listener: TcpListener,
    database_root: PathBuf,
    delivery: Delivery,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let closing = database_root.clone();

    axum::serve(listener, router_with(database_root, delivery))
        .with_graceful_shutdown(async move {
            shutdown.await;

            begin_shutdown(&closing);
        })
        .await
}

/// Stops claiming queued work and closes every live stream of `database_root` with `1001`
/// (`start.ts:319-327`'s mount close, and `sessions.ts:8638 dispose`'s `shuttingDown` flag).
///
/// A root with no runtime yet served no request and holds no socket, so this answers quietly. The
/// durable checkpoint stays a separate step ([`flush`]), because that is the part that can fail and
/// the part an embedder may want to order differently — the `typecrate-services` gateway owns its own
/// listener, so it calls this from its own process-exit path rather than from `serve_on`.
pub fn begin_shutdown(database_root: &Path) {
    let Some(active) = runtime::cached_runtime(database_root) else {
        return;
    };

    active.store.begin_shutdown();
    active.close_streams(STREAM_SHUTDOWN_CODE);
}

/// [`begin_shutdown`] for EVERY root this process cached, for a host that serves several roots and
/// owns the listener (`typecrate-services`' executable).
pub fn begin_shutdown_all() {
    for active in runtime::cached_runtimes() {
        active.store.begin_shutdown();
        active.close_streams(STREAM_SHUTDOWN_CODE);
    }
}

/// The close code a graceful host shutdown sends (`stream/protocol.ts`'s `GOING_AWAY` arm: 1001,
/// which the connection turns into an `error` frame with that `close_code` and then the close).
pub const STREAM_SHUTDOWN_CODE: u16 = 1001;

/// Writes the final durable document for `database_root` on the way out: the live sessions, the
/// registry rows, the tombstones and the saved provider pair.
///
/// This is the honest form of TS's `store.dispose()` + `store.drainIo()` close step
/// (`start.ts:328-340`): the TS store queues its graceful document and awaits the IO drain, while
/// this crate's store writes every document synchronously inside the mutation that produced it
/// (`knowledge.md` "T3": durability gates the receipt), so there is no queue left to drain - and a
/// final checkpoint is still what turns "the last write was acknowledged" into "the last write is on
/// disk". An in-memory-only root (`persist: false`) answers `Ok` without touching the disk.
///
/// A root with no runtime yet has nothing to flush: no request ever created state to lose.
pub fn flush(database_root: &Path) -> Result<(), ApiError> {
    let Some(active) = runtime::cached_runtime(database_root) else {
        return Ok(());
    };

    // `begin_shutdown` already ran in `serve_on`'s arm for a standalone listener; repeating it is
    // what makes this callable on its own (an embedder that never went through `serve_on`), and both
    // steps are idempotent.
    active.shutdown()
}
