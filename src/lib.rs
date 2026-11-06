//! `typecrate_harness` - the Rust port of the `@underload/harness` server core.
//!
//! Crate role (mirror of `packages/underload/harness/src/server`):
//! the canonical `/v1/harness` control plane - a session/turn/thread store behind a
//! REST surface plus a live update stream. Every module in here has one TypeScript
//! origin, and that origin is the specification:
//!
//! | Rust module   | TypeScript origin                                             |
//! |---------------|---------------------------------------------------------------|
//! | `contract`    | `src/shared/harness-api.ts`, `src/shared/harness-stream.ts`,   |
//! |               | `src/types/index.ts`, `src/server/protocol.ts`                |
//! | `error`       | `src/server/api-errors.ts` (`HttpError`) + the `{error:{message}}` envelope |
//! | `paging`      | `src/server/api-paging.ts`                                    |
//! | `events`      | `src/server/api-events.ts`                                    |
//! | `config`      | `src/server/config.ts` (+ the `PATCH /config` rules in `src/server/api.ts`) |
//! | `environment` | `src/server/environment.ts`                                   |
//! | `package_root`| `src/server/package-root.ts` (+ the dump root in `packages/underload/service/server.ts`) |
//! | `origin`      | `src/shared/origin.ts`, `src/server/service.ts`, `src/server/stream/mount.ts` |
//! | `ids`         | `src/core/thread.ts` (`THREAD_HOST_PREFIX`), `src/shared/agent-results.ts` (label codes) |
//! | `persistence` | `src/server/persistence.ts` (the on-disk schema, the atomic commit, the interrupted-run repair) |
//! | `journal`     | `src/server/journal.ts` (the per-session `events.jsonl` append log)                |
//! | `labels`      | `src/shared/agent-results.ts:126-262` (the label grammar and the per-code allocator) |
//! | `ports`       | the seams `src/harness.ts` + `src/core/{action,task}.ts` + `src/server/stream/mount.ts` imply |
//! | `registry`    | `src/server/harnesses/{registry,presets,descriptor}.ts` (definitions only, no factories) |
//! | `agents`      | `src/agents/**` (the trusted catalogue) + `src/server/harnesses/agent-state.ts` (the global overlay) |
//! | `read`        | the host half of `src/server/api-reads.ts` + `api.ts:1329-1334` (`configDocument`) |
//! | `store`        | the half of `src/server/sessions.ts` behind `src/store/*.rs`:  |
//! |                | records, the turn recorder, the command registry, the          |
//! |                | toolbox, the conversations, the actions and the restore path   |
//! | `tools`        | `src/tools/**` (the generated catalogue, the descriptors and   |
//! |                | the executable tools) + `harnesses/{core,toolbox}.ts` (the     |
//! |                | session-bound rows the API lists)                              |
//! | `transport`    | `src/providers/openAICompatible.ts` + `src/transport/**`       |
//! |                | (the provider call, the stream fold, the wire transcript)      |
//! | `turn`         | `src/core/agent.ts` (`runAgentCore`) + `src/harness.ts` (the   |
//! |                | run loop, the retries, the steering drain, the tool loop)      |
//! | `projector`    | `src/server/turn-projector.ts` + `src/shared/turn-display.ts`  |
//! | `attachments`  | `src/shared/attachments.ts` (the brand grammar, the            |
//! |                | attachment budget)                                             |
//! | `runtime`      | `src/server/runtime.ts` (the per-database-root runtime and     |
//! |                | its cache) + the composition `api.ts:740-940` builds for one   |
//! |                | request                                                        |
//! | `api`          | `src/server/api.ts` (the route table, the dispatch order and   |
//! |                | every REST handler) + `src/server/{handler,service}.ts` - the  |
//! |                | two adapters, one transport here                               |
//! | `server`       | `src/server/{handler,service,start}.ts` (the router, the       |
//! |                | graceful shutdown, the final durable document on the way out)  |
//!
//! STILL TO PORT: the WebSocket upgrade BODY (`src/server/stream/**`; this crate answers the mount
//! with TypeScript's own 400 sentence), the session-domain route BODIES (`api.ts:1958-2820`, whose
//! handlers are reachable stubs that consume the documented body and then refuse), and the
//! `*-route.rs` discovery wiring. The public signatures below are contractual, not incidental,
//! because every one of those stages reads them.

/// Crate name, for banners and diagnostics (matches `[package] name` with `-` -> `_`).
pub const CRATE_NAME: &str = "typecrate_harness";

/// The crate version, i.e. the `build_version` the root document advertises.
///
/// `CARGO_PKG_VERSION` is the same string `package.json` carries, and the unit test
/// at the bottom of this file pins it, so a version bump that forgets one of the two
/// places fails the build instead of lying on the wire.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub mod agents;
pub mod api;
pub mod attachments;
pub mod config;
pub mod contract;
pub mod environment;
pub mod error;
pub mod events;
pub mod ids;
pub mod journal;
pub mod labels;
pub mod origin;
pub mod package_root;
pub mod paging;
pub mod persistence;
pub mod ports;
pub mod projector;
pub mod read;
pub mod registry;
pub mod runtime;
pub mod server;
pub mod store;
pub mod tools;
pub mod transport;
pub mod turn;

#[cfg(test)]
mod tests {
    /// The npm/cargo version handshake: `package.json` (version 1.0.2) and the crate
    /// must agree, or the advertised `build_version` is a lie.
    #[test]
    fn version_matches_the_npm_line() {
        assert_eq!(super::version(), "1.0.2");
    }

    #[test]
    fn crate_name_matches_the_library_target() {
        assert_eq!(super::CRATE_NAME, "typecrate_harness");
    }
}
