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

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use axum::routing::any;
use tokio::net::TcpListener;

use crate::api;
use crate::contract;
use crate::error::ApiError;
use crate::runtime;

/// The whole HTTP surface, frozen with the database root it serves.
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
    let mount = contract::HARNESS_API_ROOT;

    Router::new()
        .route(mount, any(rest))
        .route(contract::HARNESS_WS_STREAM_PATH, any(stream))
        .route(&format!("{mount}/{{*rest}}"), any(rest))
        .fallback(rest)
        .with_state(database_root)
}

async fn rest(State(database_root): State<PathBuf>, request: Request) -> Response {
    api::handle_request(database_root, request).await
}

async fn stream(State(database_root): State<PathBuf>, request: Request) -> Response {
    api::handle_stream(database_root, request).await
}

/// Binds `addr` and serves the router until `shutdown` resolves.
pub async fn serve<F>(database_root: PathBuf, addr: SocketAddr, shutdown: F) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;

    serve_on(listener, database_root, shutdown).await
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
    let closing = database_root.clone();

    axum::serve(listener, router(database_root))
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
