# @typecrate/harness

Rust-native harness control plane: the port of the `packages/underload/harness` **server** into
`packages/typecrate/harness`, so the same `/v1/harness` surface can be discovered by
`packages/typecrate/services` through its build-time `*-route.rs` scanning.

Native only - no wasm target. This crate is a server: axum + tokio + a filesystem store.

## Status

**Stage T1: foundation primitives.** The crate compiles and its tests pass, and the primitives
every later stage is built on exist and are contractually named:

| Module | Ported from | Status |
| --- | --- | --- |
| `contract` | `src/shared/harness-api.ts`, `src/shared/harness-stream.ts`, `src/types/index.ts`, `src/server/protocol.ts` | done (wire types, limits, request/response models) |
| `error` | `src/server/api-errors.ts` + the `{error:{message}}` envelope | done |
| `paging` | `src/server/api-paging.ts` | done |
| `events` | `src/server/api-events.ts` | done (epoch, counters, hub, baseline links) |
| `config` | `src/server/config.ts` + the `PATCH /config` rules in `src/server/api.ts` | done |
| `environment` | `src/server/environment.ts` | done |
| `package_root` | `src/server/package-root.ts` + `packages/underload/service/server.ts` | done |
| `origin` | `src/shared/origin.ts`, `src/server/service.ts`, `src/server/stream/mount.ts` | done |
| `ids` | `src/core/thread.ts`, `src/shared/agent-results.ts` | done |
| `bin/server.rs` | `src/server/start.ts` | placeholder only (prints the resolved identity/config, exits 0) |
| store, persistence, journal, registry, agents, tools, transport, turn runtime, projector, stream, HTTP routes | the rest of `src/server/**` | not ported yet |

## Build and run

Everything is plain `cargo` from this directory (the repo-root `.cargo/config.toml` puts artifacts
in `.rust/target` and points cargo's temp variables at the repo `Temp/rust`).

| Command | What it does |
| --- | --- |
| `npm run check` | `cargo check --all-targets` |
| `npm run test` | `cargo test` |
| `npm run lint` | `cargo clippy --all-targets -- -D warnings` |
| `npm run fmt` / `npm run fmt:check` | `cargo fmt` / `cargo fmt -- --check` |
| `npm run build` | `cargo build --release --bin typecrate-harness-server` |
| `npm start --` | `cargo run --bin typecrate-harness-server --` (today: prints the resolved configuration and exits 0) |
| `npm run verify` | `fmt:check` + `lint` + `test` + `build` |

## Parity pointer

Behaviour is defined by the TypeScript package, not by this one. Read the corresponding TS file
before changing any module (each Rust file names its origin file and the line ranges it ports),
and read [`knowledge.md`](./knowledge.md) for the decisions and the short list of deliberate
deviations. The TS test suite is the behavioural spec:
`packages/underload/harness/src/server/*.test.ts`, `src/shared/harness-api.test.ts`,
`src/server/config.test.ts`, `src/server/environment.test.ts`, `src/server/origin.test.ts`.
