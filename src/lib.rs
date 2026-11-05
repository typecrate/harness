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
//!
//! The later stages of the port (the domain store, the agent runtime, the tool catalogue, the HTTP
//! layer, and the `*-route.rs` discovery wiring) build on exactly these primitives, which is why
//! the public signatures below are contractual, not incidental.

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
pub mod read;
pub mod registry;
pub mod store;

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
