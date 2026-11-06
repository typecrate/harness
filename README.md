# @typecrate/harness

Rust-native harness control plane: the port of the `packages/underload/harness` **server** into
`packages/typecrate/harness`, so the same `/v1/harness` surface can be discovered by
`packages/typecrate/services` through its build-time `*-route.rs` scanning.

The behavioural spec is the TypeScript source, file by file:
`packages/underload/harness/src/shared/harness-api.ts`, `shared/harness-stream.ts`,
`shared/turn-display.ts`, `shared/origin.ts`, `server/api.ts`, `server/api-{paging,reads,events}.ts`,
`server/sessions.ts`, `server/turn-projector.ts`, `server/{config,journal,persistence,protocol}.ts`,
`core/{tool-manifest,creation,tool-source,action,task}.ts` and `types/index.ts`. Every deliberate
divergence is listed under [Deviations](#deviations-the-full-list) and recorded with its evidence in
[`knowledge.md`](knowledge.md).

Native only. `Cargo.toml` is a standalone workspace (its own `[workspace]` boundary, the same shape
`packages/typecrate/services` uses) with one lib plus the server bin; artifacts land in the repo-root
`.rust/target` because `../../../.cargo/config.toml` sets `target-dir`.

## What ships

| Area | Module | What it is |
| --- | --- | --- |
| Contract | `src/contract.rs`, `src/error.rs` | The wire types (`HarnessEvent`, `ToolSummary`, `TurnDisplay`, the error envelope), every cap, the header names. |
| Foundation | `src/{config,environment,origin,ids,labels,paging,attachments,package_root}.rs` | Env parsing with the TS defaults and sentences, the origin gate, session/turn ids, display labels, cursor paging, the attachment budget, the package-root probe. |
| Durability | `src/{persistence,journal,read,registry,agents}.rs` | `state.json` + `agents.json` checkpoints, the append-only `events.jsonl` journal, the read models, the harness registry (presets + the three colour maps). |
| Store | `src/store/{mod,session,turns,commands,tools,transcript,threads,agents,actions,read}.rs` | One session's in-memory state behind a `parking_lot` lock: the turn lifecycle, the tool surface, the transcript, threads, the task list, actions, the counters the stream publishes. |
| Runtime | `src/{transport,turn,projector}.rs`, `src/tools/{mod,catalogue,result,files,command,syntax,text,web}.rs` | The provider transport (streaming, retry, tool loop), the turn engine, the display projector, and the 19 catalogue tools this build executes (21 executable arms in total once the store's own `task` and `actionRequest` are counted - see [Parity](#parity-with-the-typescript-harness)). |
| HTTP | `src/runtime.rs`, `src/server.rs`, `src/api/**`, `src/bin/server.rs` | The runtime root cache, the standalone listener, and the 45 route templates. |
| Stream | `src/stream/{mod,protocol,broker,connection}.rs` | The `harness.v1` WebSocket, the bounded replay broker, the event hub fan-out. |
| Bridge | `endpoint/harness/harness-route.rs` | The `ANY` route file `typecrate-services` discovers at build time. |

## Commands

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings   # strict: nothing here is allowed to warn
cargo test                                  # 283 tests, 16 targets
cargo build --release --bin typecrate-harness-server
```

The same eight lines as npm scripts (`check`, `test`, `lint`, `fmt`, `fmt:check`, `build`, `start`,
`verify`; `verify` is the full gate). `npm start` runs the real listener (T5a), and `npm run build`
builds it in release profile; nothing in this package is a placeholder any more.

## Running it

```bash
cargo run --bin typecrate-harness-server -- --database-root ../temporary/dump
# or
npm start -- --host 127.0.0.1 --port 5181 --database-root /var/lib/harness
```

| Flag | Default | Notes |
| --- | --- | --- |
| `-h` / `--host <ip>` | `127.0.0.1` | Loopback by default: this server has no authentication and no shared secret. Accepts `ip`, `ip:port`, and bracketed IPv6 (`[::1]`, `[::1]:5181`); an inline port wins over `--port`. Numeric only - there is no DNS step. `-h` is the HOST flag, so `--help` has no short form. (`--addr` is the `typecrate-services` gateway's spelling; THIS binary rejects it as `unknown option` with exit 2.) |
| `--port <n>` | `5181` | `0..=65535`. `0` asks the OS for any free port, and the banner prints the port actually bound. |
| `--database-root <dir>` | `<repo>/../temporary/dump` - the repo root is found by walking up to the manifest that declares `workspaces` (fallback: four levels up from this package), then ONE level above it, lexically normalized | The same dump directory the TypeScript service defaults to (`D:\Projects\typescript\temporary\dump` here), so both runtimes read and write the same state. The directory is created by whoever writes first - see the shared-root warning under [Parity](#parity-with-the-typescript-harness). |

A bad flag value is exit 2 (`bad usage`); an unusable deployment is exit 1 (`could not serve`),
because the flags were fine and only the environment can fix it. Nothing is killed and no port is
stolen: a taken bind is reported and the process exits.

The banner prints the address it actually bound (read back, so `--port 0` is honest), the HTTP root
(`/v1/harness`) and the WS root (`/v1/harness/stream`) as URLs a client can open, and the resolved
data root. A wildcard bind is reported as loopback, because `0.0.0.0` is a bind answer and not a URL.

Environment (all parsed in `src/config.rs` with the TypeScript defaults, the accepted ranges, and the
TS refusal sentences - a bad value is a startup error, never a silent default):
`HARNESS_BASE_URL`, `HARNESS_API_KEY` (present-but-empty is "present"), `HARNESS_MODEL`,
`HARNESS_THREAD_EXECUTION` / `HARNESS_IO_EXECUTION` (exactly `inline` \| `worker`; the parse accepts
both spellings, and THIS build refuses to start on `worker` - see
[Deviations](#deviations-the-full-list) item 15), `HARNESS_WORKER_IDLE_MS`,
`HARNESS_MAX_LANE_WORKERS`, plus the provider and stream caps. A `.env` at the package root fills
gaps and never overrides an exported value (`src/environment.rs`).

The API key is **never persisted**: `state.json` stores the provider `baseURL` and `model` only, and
the credential lives in the process (or in the browser session that posted it).

## The surface

* 45 path templates (`shared/harness-api.ts:54-100`) crossed with the 64 operations
  `api.ts:3925-3974` declares; the table is `src/api/mod.rs`, and `tests/routes.rs` pins it against
  the TypeScript list - the 45 `(id, template)` pairs in table order, the 64 operations with their
  per-verb split (GET 37, POST 13, PATCH 7, DELETE 6, PUT 1), and the rule that every operation is
  reachable exactly once.
* The response envelope is the contract's: `{harness, session, turn, ...}` blocks plus the error
  envelope `{error: {message}}`. `X-Harness-Epoch` and `X-Harness-Revision` are stamped by
  `api::with_revision`; a cursor carries the epoch, so a cursor from a previous process is a refusal
  rather than an off-by-epoch page.
* The stream is a WebSocket at `/v1/harness/stream` with the `harness.v1` subprotocol
  (`contract::HARNESS_WS_SUBPROTOCOL`), subscribe/unsubscribe frames, bounded replay, and the display
  vocabulary of `shared/harness-stream.ts:142` - **18** op kinds (`turn`, `section:open|set|remove`,
  `node:insert|append|set|remove`, `step:set`, `context:add|set|remove`, `tail:set|clear`,
  `pending:set`, `usage`, `attempt:commit|discard`). The fold mints every kind its recorded events
  describe; `pending:set` has no source event in this crate, which `src/projector.rs`'s header states.
  A graceful host shutdown closes live streams with `1001` (`server::STREAM_SHUTDOWN_CODE`).
* The same surface is served embedded: `typecrate-services` mounts `ANY /v1/harness`,
  `ANY /v1/harness/stream` and `ANY /v1/harness/{*rest}` from
  `packages/typecrate/harness/endpoint/harness/harness-route.rs`, which its build script discovers by
  scanning the repository root. That bridge is the only route file discovered outside the services
  package, and it registers FIRST in the table (the scan root sorts `.../harness` before
  `.../services`).

## The data root

```
<database-root>/harness/state.json                     the checkpoint (sessions, settings, providers)
<database-root>/harness/agents.json                    the global agent overlay
<database-root>/harness/sessions/<sessionId>/events.jsonl   the append-only journal
```

* `state.json` is replaced atomically: `state.json.<owner>.<seq>.tmp` → fsync → rename in the same
  directory. Durability gates the receipt: a write that could not be made is not acknowledged.
* The journal is one line per event. Header line, verbatim TS:
  `{"type":"header","v":1,"sessionId":"<id>","createdAt":<ms>}`. Event line, this crate's envelope
  (a per-file monotonic sequence, which is what `read(from_sequence, limit)` replays from):
  `{"version":1,"sequence":<n>,"timestamp":<ms>,"event":{...}}`. A truncated LAST line is a crash
  artifact and is dropped silently by a fold; a malformed line anywhere else, a blank line, an
  unknown version, or a sequence gap is reported as a problem, never folded as healthy.
* `append` writes through a held-open fd (crash-safe, not power-fence-safe); `durable: true` adds the
  fsync; `rewrite` fsyncs before the rename so a compaction publishes complete bytes.

## Parity with the TypeScript harness

| Capability | TypeScript | This build |
| --- | --- | --- |
| Route surface | 45 templates × 64 ops | same, pinned by `tests/routes.rs`: the 45 `(id, template)` pairs in table order, the 64 operations, the verb split (GET 37, POST 13, PATCH 7, DELETE 6, PUT 1), and one reachable mount per operation |
| Tool catalogue | the manifest's runtime rows - 35 in the registry, of which `/tools` lists **31**, because `tool-manifest.ts:1002, 1046, 1692, 1811` mark four `listed: false` | 35 generated rows (`src/tools/catalogue.rs`): `GET /tools` lists **31** (35 rows minus the four `catalogue::UNLISTED` names), `summaries()` → **31** for the global surface, `catalogue_summaries()` → **35** for a session toolbox (TS's toolbox reads the live registry, `api-reads.ts:831-836`) |
| Executable here | every row, once the runtime compiler is loaded | **21 executable arms = 19 catalogue tools + `task` + `actionRequest`**. The 19 catalogue rows with bodies are `src/tools/mod.rs:486-527`: `read`, `write`, `edit`, `fileList`, `glob`, `grep`, `shell`, `gitDiff`, `syntax`, `timeNow`, `textCount`, `diceRoll`, `noteSearch`, `textSummarize`, `toolList`, `toolRequest`, `fetch`, `search`, `download`; `task` + `actionRequest` are answered by the STORE (they are declarations, not catalogue rows) |
| Catalogued-only rows | same 14, executable through the runtime compiler | **14** names in `MISSING_CAPABILITY` (`toolRegistry`, `toolManage`, `toolCreate`, `profileManage`, `chapterManage`, `locationManage`, `agentManage`, `agentResult`, `delegate`, `threadFork`, `toolCancel`, `threadSend`, `threadRead`, `threadList`) exist for discovery and answer `Err("Tool \"<name>\" requires the agent runtime.")`, and `is_declared()` is false for every one - so a model is never offered a tool this build cannot run |
| Declaration-only tools | declared + executable | `actionRequest` and `task` are DECLARED here (so the model is offered them, `harness-api.ts:1388`) and the store executes both; they are not catalogue rows |
| Worker modes | `inline` \| `worker` for both knobs, and BOTH are served: a lane worker is a forked isolate, and a deployment that cannot get one **aborts the start** rather than running inline (`workers/capability.ts`, `workers/io-client.ts:282`, `sessions.ts:1513-1516`) | `inline` only. The two knobs still PARSE with the TS defaults, ranges and sentences - `GET /config` reports what the deployment asked for - and `runtime::validate_supported_execution` refuses to build a runtime on anything but `inline`: the standalone executable prints the refusal and exits 1 before binding, and the gateway answers the first mounted request with 400. **Rejected, never silently degraded**, which is the parity decision: this build cannot make a `worker` promise, so it declines to start instead of becoming an inline server behind the operator's back. See [Deviations](#deviations-the-full-list) item 15 |
| Default data root | `@underload/service` defaults to `<repo>/../temporary/dump` | **the same directory** (`D:\Projects\typescript\temporary\dump` here), which is the point of porting the rule - and the warning that goes with it: there is no cross-process lock, so do NOT run the TypeScript service and this gateway against one root at the same time. Whichever starts second restores the other's sessions from `state.json`, and the restore pass repairs turns still marked `running` (`persistence.rs`, the interrupted-run repair). Use separate `--database-root` values to run both |
| Context bar | user / system / reasoning / assistant / tool segments per section, `context:*` ops, block detail route | **tool** segments (opened by the call, settled by its result) and the **assistant** fallback (a `done` answer that streamed no deltas). `user`/`system` need a prompt the projector seam never receives - see [Deviations](#deviations-the-full-list) |
| `TurnSummary.blocks` | the agent-results registry's block rows | `[]`: there is no agent-results registry in this crate |
| `delegate` / subagents | the delegation runtime registers it dynamically | absent: the catalogue row is discovery-only |
| Provider | any OpenAI-compatible base URL | same transport, plus the recorded retry/backoff rules |

## Deviations (the full list)

Stated, not hidden. Each is a decision with the TypeScript evidence beside it; `knowledge.md` carries
the longer reasoning.

1. **`turnId` inside ops and section ids.** Ops carry `turnId: ""` and sections are `s{ordinal}`,
   not `${turnId}:${ordinal}`. Both are opaque, unique within the turn, and identical across live and
   replay, which is all the wire contract states ("Stable, opaque, server-minted",
   `turn-display.ts:891`).
2. **Record-only members of `summary()`.** `prompt`, `threadId`, `threadLabel`, attachments,
   references and steers are filled by the store's row read model (`store::read::TurnState::row`),
   because the projector seam never sees the record; the fold states `prompt: ""` and omits the rest.
3. **The context bar's record-only half.** A `user` block (the prompt, and one per steered submit)
   and a `system` block (one per recorded instruction source) are not minted: `ProjectorFactory::create`
   (`ports.rs:239-241`) takes no prompt, and the protocol's event enum has no user or instruction
   event. Everything the events do describe is minted, and the ids follow TypeScript's
   `${section.sections.length}-${kind}-${key}` shape except for the missing prompt section at index 0,
   so a segment on the root flow is one ordinal lower (`1-tool-7` where TypeScript says `2-tool-7`).
4. **No prose-window segments.** TypeScript opens a `reasoning`/`assistant` block per window of
   consecutive deltas and patches it on every append. This fold puts those bytes in the node and the
   section `tail`, and mints the `assistant` segment only on the `done` fallback TypeScript uses when
   the window held no deltas.
5. **Duplicate tool creation REPLACES.** The TypeScript kernel does not reserve a created tool's name
   (`core/creation.ts`), so `POST .../tools` over an existing created name answers `201` with
   `{created: false, replaced: true, previousRevision}` and spends no slot. A name owned by the
   catalogue or the harness answers `409` instead, because this port has no override row to replace -
   TypeScript's override path is a different record.
6. **`agentColors` follows `agent_models`.** It is a typed `Option<BTreeMap<String, String>>`, so a
   non-object value is refused with serde's type sentence rather than the TypeScript domain sentence
   that names `agentColors`. The hex, membership and blank refusals are the TypeScript texts.
7. **`checks` is omitted from a registration receipt.** TypeScript runs a compiler probe; this build
   has none, so the member is absent rather than `[]`.
8. **`/tools/{name}` for an unlisted runtime row answers 404.** The row is in the catalogue but not
   listed, matching `api.ts:1847`; the SESSION toolbox still shows all 35.
9. **A created tool's name grammar** (`tool…`, the `toolWordCount` sentence) is this port's reading of
   the kernel; the TS kernel validates a created name differently.
10. **The budget sentence** is "This session can create at most N tools. Remove one first." - the
    TypeScript text, kept while the slot accounting matches.
11. **`glob`/`grep` read no `.gitignore`.** TypeScript parses ignore files
    (`core/files/gitignore.ts`); the counters that would carry a parsed pattern set are not in this
    crate, so both tools skip `.git`/`node_modules` and otherwise see everything.
12. **`edit` implements only TypeScript's first matching tier** (the LF view: a `\n` `oldString`
    matches a CRLF file and the file's endings are preserved). Tiers two and three, which handle a
    mixed-ending file, are not ported.
13. **Paging coercion is ECMAScript's `Number()`** (`paging::number_value`), because every TypeScript
    cap runs values through it. `?limit=` (empty) is therefore `0` and a 400, and a value above
    `Number.MAX_SAFE_INTEGER` is refused.
14. **The usage fold states `{input, output, total}`** and preserves first-appearance thread order;
    the `cached` line is reported by the transport, not folded into the tally row.
15. **`worker` execution is rejected, not degraded.** TypeScript's `HARNESS_THREAD_EXECUTION` /
    `HARNESS_IO_EXECUTION` accept `inline` | `worker` and actually provide the worker lanes, and when a
    lane cannot be isolated TypeScript **aborts** (`workers/capability.ts`, `workers/io-client.ts:282`,
    `sessions.ts:1513-1516`) - it never runs a promised worker turn inline. This build has no worker
    backend at all, so `src/config.rs` still PARSES both spellings (the loader also backs
    `GET /config`, which must keep reporting what the deployment asked for), while
    `runtime::validate_supported_execution` refuses to construct a runtime on anything but `inline`:
    the standalone executable prints `Invalid runtime settings: thread_execution "worker" is not
    supported in this Rust build; only "inline" is available. Unset HARNESS_THREAD_EXECUTION or set it
    to "inline" - this build never runs a "worker" deployment inline silently.` and exits 1 before
    binding. The gateway answers **every** mounted request with 400 while the environment asks for a
    worker lane - the runtime is resolved at dispatcher step 2, before the route match, exactly like
    TS's `service.ts:515-528` - and a refused build is never cached, so a fixed environment starts on
    the next request without a restart. The worker knobs
    (`HARNESS_WORKER_IDLE_MS`, `HARNESS_MAX_LANE_WORKERS`, `HARNESS_WORKER_HANDSHAKE_MS`,
    `HARNESS_WORKER_ENTRY`, `HARNESS_IO_WORKER_ENTRY`) therefore validate but describe no behaviour
    here. Pinned by `tests/runtime_execution.rs` (gate + runtime + the standalone process) and
    `tests/worker_execution_http.rs` (the mounted request).

## Verification

Latest full run, from `packages/typecrate/harness`:

```bash
cargo fmt -- --check                                 # clean
cargo check --all-targets                            # clean
cargo clippy --all-targets -- -D warnings            # clean
cargo test --no-fail-fast                            # 283 passed, 0 failed, exit 0
```

| Target | Tests |
| --- | --- |
| lib unit tests | 56 |
| `bin/server` unit tests | 8 |
| `data_plane` | 15 |
| `foundation` | 23 |
| `http_host` | 17 |
| `http_sessions` | 17 |
| `projector` | 15 |
| `routes` | 6 |
| `runtime_execution` | 12 |
| `store` | 27 |
| `store_restore` | 4 |
| `stream` | 21 |
| `tools` | 24 |
| `transport` | 24 |
| `turn` | 13 |
| `worker_execution_http` | 1 |

Beside it, from `packages/typecrate/services`: `cargo fmt -- --check`,
`cargo test --locked --no-fail-fast`, `cargo clippy --all-targets --locked -- -D warnings` and
`cargo check --locked --target wasm32-unknown-unknown` are all clean (exit 0), and the test run is
**96 passed, 0 failed** across 6 targets (13 lib, 15 `bin/server`, 1 `api`, 29 `discovery`,
8 `harness_gateway`, 30 `server_http`). `tests/harness_gateway.rs` pins the global
`/v1/harness/tools` page at **31 LISTED rows** - the 35-row pin moved with the listing rule.

The wasm32 build does NOT contain the harness: `cargo tree --target wasm32-unknown-unknown` resolves
**zero** `typecrate-harness` packages (the native tree resolves one), because services declares the
dependency under
`[target.'cfg(not(all(target_arch = "wasm32", target_os = "unknown")))'.dependencies]`. The harness
lib is therefore *excluded* from the wasm artifact, not linked into it - the wasm build compiles
neither the harness nor its axum/tokio/reqwest stack.

`node scripts/check-playground.mjs` is 122/122 against the 18-row listing (the three `ANY /v1/harness`
rows first), which is the shape `tests/server_http.rs::route_table` pins over a socket.
