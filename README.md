# @typecrate/harness

Rust-native harness control plane: the port of the `packages/underload/harness` **server** into
`packages/typecrate/harness`, so the same `/v1/harness` surface can be discovered by
`packages/typecrate/services` through its build-time `*-route.rs` scanning.

The behavioural spec is the TypeScript source, file by file. The reference package was restructured
under `src/engine/**`, so every citation below uses the CURRENT layout:
`packages/underload/harness/src/engine/shared/{harness-api,harness-stream,turn-display,origin}.ts`,
`src/server/{api,api-paging,api-reads,api-events,sessions,turn-projector,config,journal,persistence,protocol}.ts`
(the `src/server/**` prefix did NOT move), `src/engine/core/{tool-manifest,creation,tool-source,action,task}.ts`,
`src/engine/plugin/**`, `src/engine/plugins/**` and `src/engine/types/index.ts`. The generated
OpenAPI document is `src/harness-server.swagger.yaml` (it was `harness.swagger.yaml`). For anchors
into `api.ts` and `harness-api.ts` this manual names the **handler or symbol** - `getTools`,
`getPlugins`, `patchSessionAgent`, `getSessionAgentPrefix`, `getSessionRecords`, the `routes` table,
`HARNESS_PATH_TEMPLATES`, `HARNESS_SERVICE_LIMITS` - rather than a line number, because those anchors
drift with every contract wave. Every deliberate divergence is listed under
[Deviations](#deviations-the-full-list) and recorded with its evidence in [`knowledge.md`](knowledge.md).

Native only. `Cargo.toml` is a standalone workspace (its own `[workspace]` boundary, the same shape
`packages/typecrate/services` uses) with one lib plus the server bin; artifacts land in the repo-root
`.rust/target` because `../../../.cargo/config.toml` sets `target-dir`.

## What ships

| Area | Module | What it is |
| --- | --- | --- |
| Contract | `src/contract.rs`, `src/error.rs` | The wire types (`HarnessEvent`, `ToolSummary`, `TurnDisplay`, the error envelope), every cap, the header names. |
| Foundation | `src/{config,environment,origin,ids,labels,paging,attachments,package_root}.rs` | Env parsing with the TS defaults and sentences, the origin gate, session/turn ids, display labels, cursor paging, the attachment budget, the package-root probe. |
| Durability | `src/{persistence,journal,read,registry,agents}.rs` | The `state.json` (v1) + `agents.json` checkpoints, the append-only `events.jsonl` journal, the read models, the harness registry (presets + the three colour maps). |
| Context rules | `src/core_pruning.rs` | The PURE context helpers the compaction policy needs: the tool-call fingerprint, the token estimate, canonical unit grouping and the progressive-prune plan (ports `src/engine/core/{argument-fingerprint,context-estimate,context-pruning}.ts`). Pure by design - no I/O, no clock, no store - and NOT wired into a turn yet, because nothing calls the plan on the turn path. |
| Plugins (catalogue) | `src/plugins.rs` | The built-in plugin CATALOGUE - the **three** shipped built-ins as a `static` table (`BUILTIN_PLUGINS: [PluginMetadata; 3]`) instead of a scanned manifest - their settings descriptors, the operator's overrides, and every validation sentence the patch routes answer. |
| Plugin layer | `src/plugin/{mod,event,context,hooks,host,settings,diagnostics,async_ping}.rs` | The lifecycle vocabulary, the hook bus (`hooks`), the plugin host (`host::PluginHost`), the seam a plugin acts through (`context`), and `async-ping` as a Rust `Plugin` policy - the port of `src/engine/plugin/**`. **Ported, but not on the turn path:** no `PluginHost::run` call site exists anywhere under `src/` (the only callers are in `tests/plugin_host.rs`), `NullRuntime` (`plugin::context::NullRuntime`) is the only `PluginRuntime` implementation, and `context-trimmer` / `context-compaction` have no `Plugin` module. So no hook fires during a turn - see [Deviations](#deviations-the-full-list) item 16. |
| Store | `src/store/{mod,session,turns,commands,tools,transcript,threads,agents,actions,read}.rs` | One session's in-memory state behind a `parking_lot` lock: the turn lifecycle, the tool surface, the transcript, threads, the task list, actions, the counters the stream publishes. |
| Runtime | `src/{transport,turn,projector}.rs`, `src/tools/{mod,catalogue,result,files,command,syntax,text,web}.rs` | The provider transport (streaming, retry, tool loop), the turn engine, the display projector, and the 19 catalogue tools this build runs through a tool body (21 executable `dispatch` arms once `tasks` and `actionRequest` are counted - both are catalogue rows answered by host pass-through, not tool bodies; see [Parity](#parity-with-the-typescript-harness)). |
| HTTP | `src/runtime.rs`, `src/server.rs`, `src/api/**`, `src/bin/server.rs` | The runtime root cache, the standalone listener, and the route table `api::ROUTES` - 52 templates / 74 operations, which includes the two global `/plugins` routes and the three session-scoped plugin routes. |
| Stream | `src/stream/{mod,protocol,broker,connection}.rs` | The `harness.v1` WebSocket, the bounded replay broker, the event hub fan-out. |
| Bridge | `endpoint/harness/harness-route.rs` | The `ANY` route file `typecrate-services` discovers at build time. It is one of TWO route files outside the services package - `packages/typecrate/provider/endpoint/provider/provider-route.rs` is the other - and the scan's path sort makes this one register FIRST. |

Two modules `src/lib.rs`'s own module-origin table still omits (`plugin`, `core_pruning`), and its
"STILL TO PORT" list still leaves out the plugin-host INTEGRATION and the record engine; the rows
above are the corrected statement. `src/lib.rs` itself is another stage's file.

## Commands

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings   # strict: nothing here is allowed to warn
cargo test                                  # every pinned suite; see [Verification](#verification) for the targets
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
| `--database-root <dir>` | `<repo-parent>/temporary/dump` - the repo root is found by walking up to the manifest that declares `workspaces` (fallback: four levels up from this package), then ONE level above it, lexically normalized (`package_root::default_database_root`, `src/package_root.rs:96-101`) | The rule is the one `@underload/service` follows (`packages/underload/service/server.ts:25`, `provider-server.ts:80-84` - the latter also honors `UNDERLOAD_DATABASE_ROOT`), so the DEFAULT points at the same directory. The TypeScript harness package has no path of its own: "no root means in-memory" (`src/server/service.ts:327`), and the host hands it `variables.root`. Either way the two runtimes do **not** share state through that directory: this crate writes persistence **v1** and the reference reads and writes **v2** (see [The data root](#the-data-root) and [Deviations](#deviations-the-full-list) item 18). Use a separate `--database-root` per runtime. The directory is created by whoever writes first. |

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

* 52 path templates (`src/engine/shared/harness-api.ts:54-107`, the
  `HARNESS_PATH_TEMPLATES` table - 52 entries between the opening line at `:54` and the closing
  `} as const;` at `:107`) crossed with the 74 operations the TypeScript route table declares; the
  table is `api::ROUTES` in `src/api/mod.rs` (`pub static ROUTES: [Route; 52]`), and `tests/routes.rs`
  pins it against the TypeScript list - the 52 `(id, template)` pairs in table order, the 74
  operations with their per-verb split (GET 42, POST 14, PATCH 11, DELETE 6, PUT 1 - those five
  figures are the counts of the `("VERB", Operation::…)` entries in `ROUTES`, re-counted 2026-11-27),
  and the rule that every operation is reachable exactly once. Template ORDER is part of the pin:
  TypeScript's `matchRoute` answers the FIRST fit in table order, so a reordered table is a different
  router.
* The plugin surface is **four templates**, the global pair and the session-scoped pair:
  `GET /v1/harness/plugins` (the built-in catalogue, paged and `q`-filtered, each row carrying its
  declared settings AND the values in force) and `PATCH /v1/harness/plugins/{plugin_id}`
  (settings-only; the answer is the whole row, the write is durable before the answer, and the
  collection revision is the plugin store's own counter); plus
  `GET /v1/harness/sessions/{session_id}/plugins` and
  `GET`/`PATCH /v1/harness/sessions/{session_id}/plugins/{plugin_id}`, which answer the same three
  rows against a SECOND, per-session override layer (`Store::session_plugin_rows`,
  `src/store/mod.rs`) rather than the operator's global one. Only the global pair is in the boot
  document's `links` map - the ten navigable keys of `api::root` - which is what the front-end's
  Plugins pane boots from. The global list route stamps NO `X-Harness-Revision`, like
  `GET /harnesses`; `api.ts` `getPlugins` answers the revision on the page body only.
* The response envelope is the contract's: `{harness, session, turn, ...}` blocks plus the error
  envelope `{error: {message}}`. `X-Harness-Epoch` and `X-Harness-Revision` are stamped by
  `api::with_revision`; a cursor carries the epoch, so a cursor from a previous process is a refusal
  rather than an off-by-epoch page.
* The stream is a WebSocket at `/v1/harness/stream` with the `harness.v1` subprotocol
  (`contract::HARNESS_WS_SUBPROTOCOL`), subscribe/unsubscribe frames, bounded replay, and the display
  vocabulary of `src/engine/shared/harness-stream.ts:161` - the closed `op` union of **18** kinds
  (`turn`, `section:open|set|remove`, `node:insert|append|set|remove`, `step:set`,
  `context:add|set|remove`, `tail:set|clear`, `pending:set`, `usage`, `attempt:commit|discard`). The
  fold mints **14 of those 18**; four are never minted from a recorded event in this crate:
  `turn`, `section:remove`, `node:remove` and `pending:set` (verified 2026-11-27 by enumerating every
  `"op": "…"` literal in `src/projector.rs`). `src/projector.rs`'s own module header states only the
  `pending:set` half of that - the fold has no "still running" state because the turn engine runs a
  tool round to completion before the next request - and says nothing about the other three, so the
  header is not the authority for the 14/18 split.
  A graceful host shutdown closes live streams with `1001` (`server::STREAM_SHUTDOWN_CODE`).
* The same surface is served embedded: `typecrate-services` mounts `ANY /v1/harness`,
  `ANY /v1/harness/stream` and `ANY /v1/harness/{*rest}` from
  `packages/typecrate/harness/endpoint/harness/harness-route.rs`, which its build script discovers by
  scanning the repository root. That bridge is **not** the only route file discovered outside the
  services package: `packages/typecrate/provider/endpoint/provider/provider-route.rs` is a second
  one. What is true is the ORDER - `build_support/discovery.rs` sorts the found routes by their
  relative path (`:204-210`), so `.../harness` comes first, `.../provider` second, and this package's
  own trees after both. The scan root is the repository root by default (an ancestor of the services
  package, `discovery.rs::resolve_route_root`), and `TYPECRATE_SERVICES_ROUTE_ROOT` overrides it.

## The data root

```
<database-root>/harness/state.json                            the v1 checkpoint (sessions, settings, providers, pluginSettings)
<database-root>/harness/agents.json                           the global agent overlay
<database-root>/harness/sessions/<sessionId>/events.jsonl     the append-only journal
```

* **One root serves ONE runtime at a time.** This crate writes persistence **v1**: a single
  `harness/state.json` (`persistence::HARNESS_DATABASE_VERSION = 1`,
  `HARNESS_DATABASE_FILE = "state.json"`, `src/persistence.rs:70`/`:77`). The TypeScript reference
  has been at **v2** since its shard split (`HARNESS_DATABASE_VERSION = 2` at
  `src/server/persistence.ts:223`): `harness/index.json` plus `harness/sessions/<id>/session.json`
  plus `harness/sessions/<id>/threads/<tid>.json`
  (`HARNESS_INDEX_FILE` / `HARNESS_SESSION_FILE` / `HARNESS_THREAD_DIRECTORY`, `:243-246`). The
  reference **never opens `state.json`** - its own header states the v1 single-file document "is NOT
  read, migrated, rewritten, renamed or deleted", and an upgrade "therefore starts empty next to the
  old file" (`src/server/persistence.ts:31-33`). So the two runtimes do **not** read and write the
  same state, and neither restores the other's sessions. Point each one at its own `--database-root`.
  The hazard is not just invisibility: the reference's commit pass removes shard footprints its
  header no longer names, recursively (`eraseVanishedShards` → `eraseTree` →
  `eraseHarnessShardPaths`, `src/server/persistence.ts:3440`/`:3476`/`:3501`), and its layout
  treats `harness/sessions/<id>` as ONE footprint to delete (`:236-242`, "a session delete removes
  exactly one directory", with `HARNESS_SESSION_DIRECTORY = HARNESS_JOURNAL_DIRECTORY` at `:246`) -
  which is the directory this crate writes its journals into. Running the
  reference against a Rust-written root can therefore destroy Rust's journals; the reverse cannot
  destroy v2's shards, it just cannot see them. See
  [Deviations](#deviations-the-full-list) item 18.
  What IS shared between the two paths today is the journal file itself,
  `harness/sessions/<id>/events.jsonl`, because both use the same directory and file names
  (`journal::HARNESS_JOURNAL_DIRECTORY = "sessions"` / `HARNESS_JOURNAL_FILE = "events.jsonl"` at
  `src/journal.rs:103`/`:99` here; `HARNESS_JOURNAL_DIRECTORY = 'sessions'` and
  `HARNESS_JOURNAL_FILE = 'events.jsonl'` at `src/server/journal.ts:104`/`:105` in the reference) and this crate writes the reference's byte format - see
  the journal bullet below. `harness/agents.json` shares its NAME but not its document: this crate
  writes a `{version, records}` envelope and the reference's `readHarnessAgentRecords` requires a
  bare ARRAY and throws `The harness agent overlay must be an array of agent records.` on anything
  else (`src/server/persistence.ts:3570`), so a Rust-written overlay does not load there.
  A durable-write failure still leaves the in-memory value applied for the plugin `PATCH` (the
  documented exception the other writes share: every running session already has it), and the
  restore pass repairs turns still marked `running` (`persistence::repair_interrupted_tool_calls`).
* `state.json` is replaced atomically: `state.json.<owner>.<seq>.tmp` → fsync → rename in the same
  directory. Durability gates the receipt: a write that could not be made is not acknowledged.
* `state.json` carries `pluginSettings` - `plugin -> key -> number|string|boolean` - the operator's
  OVERRIDES only, never a declared default. The member is ADDITIVE and did not bump
  `HARNESS_DATABASE_VERSION` (still `1`): it is absent on every document written before the plugin
  uplift and stays absent while nothing has been changed. The loader is STRICT about its shape (a
  nested object or array names the slot and aborts the boot) and LENIENT about its identity (a plugin
  or key this build no longer declares is dropped by the restore).
* The journal is one line per event, in the **TypeScript byte format** - not this crate's old
  envelope. Header line: `{"type":"header","v":1,"sessionId":"<id>","createdAt":<ms>}`. Appended
  event line: `{"type":"event","v":1,"turnId":"<id>","index":<n>,"at":<ms>,"event":{...}}`; a
  REWRITTEN line puts `type`/`v` LAST, because the spread order is the key order
  (`src/journal.rs:17-37`). The position is the TURN's - `turnId` + `index` - and there is no
  per-file sequence anywhere in the format, which is why the old `{version, sequence, timestamp,
  event}` envelope and its `expected event sequence N` report are gone. That legacy envelope is
  **refused on read**, not accepted: it has no `v`, so the reference's reader rejects every line of
  it as `unsupported journal version undefined`, and accepting it here would have been a divergence
  in the one direction that matters - a Rust journal a TypeScript server cannot read
  (`src/journal.rs:74-81`). A truncated LAST line is a crash artifact and is dropped silently by a
  fold; a malformed line anywhere else, a blank line, an unknown version, a header that is not line
  1, an event before any header, or a gap in one turn's index sequence is reported as a problem,
  never folded as healthy.
* `append` writes through a held-open fd (crash-safe, not power-fence-safe); `durable: true` adds the
  fsync; `rewrite` fsyncs before the rename so a compaction publishes complete bytes.

## Parity with the TypeScript harness

## Parity with the TypeScript harness

Every row states a status. The vocabulary is exactly four values - **parity**, **deviation N** (the
numbered entry in the list below), **not ported**, **mounted, not implemented** - and every row names
the Rust symbol and the pinning test that prove the "This build" cell. Counts are stated as facts only
where they are readable from the Rust source in this tree; anything re-counted on a date is stamped
with that date, and anything only the reference build can count is attributed to the pass that counted
it.

| Capability | Status | TypeScript | This build | Evidence (Rust symbol · pinning test) |
| --- | --- | --- | --- | --- |
| Route surface | **parity** | 52 templates × 74 operations (`HARNESS_PATH_TEMPLATES`) | Same table, in contract order, all mounted: the 52 `(id, template)` pairs, the 74 operations, the verb split (GET 42, POST 14, PATCH 11, DELETE 6, PUT 1, re-counted from `ROUTES` on 2026-11-27), one reachable mount per operation, and nothing held back | `api::ROUTES` (`src/api/mod.rs`) · `tests/routes.rs::the_table_carries_the_52_contract_templates_in_the_contracts_order`, `::the_table_declares_seventy_four_operations_with_the_contracts_verb_split`, `::this_build_mounts_every_contract_operation_with_nothing_held_back` |
| Plugin catalogue + operator settings | **deviation 21** (settings key sets) | `src/engine/plugins/**` + the generated `src/engine/plugin/manifest.ts`, exposed by `src/server/plugins.ts` and `api.ts` `getPlugins` / `patchPlugin` | The settings contract in full: `GET /plugins`, `PATCH /plugins/{plugin_id}`, the session-scoped trio, **three** built-ins' metadata, descriptors, effective values, validation sentences, the durable `pluginSettings` delta, and the `group`/`parentKey` display projection | `plugins::BUILTIN_PLUGINS` (`[PluginMetadata; 3]`), `api::plugins`, `Store::session_plugin_rows` · `tests/plugins.rs::the_catalogue_answers_both_builtins_with_their_declared_metadata_and_values`, `::a_fresh_session_answers_the_template_rows_and_starts_its_own_counter_at_zero`, `::the_display_metadata_files_each_setting_row_under_its_section` |
| Plugin hook EXECUTION | **not ported** (integration) | `runPlugins` fires lifecycle hooks per plugin on every run | The bus, the host, the seam and the settings/diagnostic planes ARE ported (`src/plugin/**`), and `async-ping` is a real `Plugin` impl - but nothing on the request/turn path calls `PluginHost::run`, `NullRuntime` is the only `PluginRuntime`, and `context-trimmer` / `context-compaction` have no `Plugin` module, so **no hook fires during a turn** | `plugin::host::PluginHost::run` (zero call sites under `src/`), `plugin::context::NullRuntime`, `plugin::async_ping` · `tests/plugin_host.rs` (drives the host directly - it is not evidence that a turn runs a hook). See [Deviations](#deviations-the-full-list) item 16 |
| Default model | **parity** | `local/basalt`, the id the private provider at the default base URL serves (`src/server/config.ts`) | `local/basalt`, including the `PATCH /config` `"model": null` reset target | `config::DEFAULT_MODEL` · `tests/foundation.rs` (the loader default and the advertised `defaults.model`) |
| Tool catalogue | **parity** for 36 of the reference's 39 names (re-counted 2026-11-27 from `tool-manifest.ts`) · `threadDelete`, `knowledge`, `sleep`: **not ported** · 8 rows **re-mirrored 2026-11-27** — two of them twice, once under a same-day correction wave — wording only (`knowledge.md`'s dated sections) | The listing is COMPOSED, not one table: `api.ts` `getTools` answers `[...createCoreToolEntries(), ...createToolCatalogue().entries]` = 15 + 17 = **32 rows** on `/tools` (both factories run and counted on 2026-11-14; not re-counted in this pass, because only the reference build can count them). Five runtime rows reach no listing at all - `toolCancel`, `threadDelete`, `threadSend`, `threadRead`, `threadList` - because `src/engine/core/tool-manifest.ts` marks them `listed: false` and nothing re-appends them; `search`/`fetch` carry the same flag yet stay listed, appended by their own factories (`harnesses/core.ts` `createCoreToolEntries` and `harnesses/index.ts` `createToolCatalogue`, which since the reference's engine split live at `src/server/harnesses/core.ts:76` and `src/server/harnesses/index.ts`). So TS has **37** names, **32** listed (RE-COUNTED 2026-11-27: `tool-manifest.ts` now answers **34** package rows - `knowledge` and `sleep` joined it - so the reference has 39 names; the 32-row `/tools` figure is the reference's own factory count, counted 2026-11-14 and not re-run here). `threadDelete` has no row on this side at all - not an `Entry`, not in `UNLISTED`, not a `dispatch` arm - so it is absent by never having been a row, and the identified read 404s it as an unknown name | 36 generated rows (`pub const ENTRIES: [Entry; 36]`, `src/tools/catalogue.rs:115`) = those 39 names minus `threadDelete`, `knowledge` and `sleep`; `GET /tools` lists **32** (36 minus the four `catalogue::UNLISTED` names, `const UNLISTED: [&str; 4]`, `src/tools/catalogue.rs:575`), `summaries()` → 32 for the global surface, `catalogue_summaries()` → 36 for a session toolbox (the reference's toolbox reads the live registry, `api-reads.ts` `sessionToolRowsOf`). Eight rows were re-copied byte-for-byte off the 2026-11-27 manifest on 2026-11-27 — `agentManage`, `agentResult`, `delegate`, `threadFork`, `toolCancel`, `threadSend`, `threadList`, plus `threadRead`, which a parallel lane re-mirrored between the two passes — which is how `threadFork.allowFamilyWrites` and `agentResult.message` reached the advertised schemas and how the `subagent` tag became `rank` on the three agent-facing rows. A correction wave then moved two of them again the same day, and both were re-synced: `agentManage`'s `description:` gained 339 chars (the new `answerMessageIndex` sentence pointing at `threadRead`), and `threadSend`'s `parameters_json:` gained 2 chars (the separator in "a turn-cap, a timeout, a parent that closed - answers `started`"). Measured against the current TypeScript working copy by decoding the Rust rows back: **16** of the 31 rows that have a manifest row to compare match on `description` and `parameters_json`, and **15** still lag with pre-existing drift (`toolRequest`, `toolList`, `toolManage`, `toolCreate`, `records`, `tasks`, `profiles`, `chapters`, `locations`, `read`, `write`, `edit`, `files`, `glob`, `grep`); `search`/`fetch` are inside the 16 on those fields only, because their `sessionBound` flag keeps the documented SERVER reading (`src/tools/catalogue.rs`'s DEVIATIONS header) rather than the manifest's — a checker comparing that flag too reports 14 / 17. WORDING AND SCHEMA ONLY: every one of those eight is still a `MISSING_CAPABILITY` refusal here, no row was added or removed, and the 36 / 32 / 4 / 15 counts are unchanged. `cargo fmt -- --check`, `cargo check --all-targets`, `cargo clippy --all-targets -- -D warnings` and `cargo test --no-fail-fast` were all run for that mirror on 2026-11-27, as were `cargo fmt -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --no-fail-fast` and the cross-package `cargo test --manifest-path ../services/Cargo.toml --test harness_gateway` for the correction pass; the row-by-row deltas are in `knowledge.md`. | `catalogue::ENTRIES`, `catalogue::UNLISTED`, `ToolRuntime::summaries`, `ToolRuntime::catalogue_summaries`, `ToolRuntime::detail` · `tests/tools.rs::the_catalogue_lists_every_group_in_the_published_order`, `::the_identified_read_carries_the_schema_and_the_execution_gate` |
| Executable here | **parity** for 19 rows with tool bodies | every row, once the runtime compiler is loaded | 21 executable `dispatch` arms = 19 rows with bodies (`read`, `write`, `edit`, `files`, `glob`, `grep`, `shell`, `gitDiff`, `syntax`, `timeNow`, `textCount`, `diceRoll`, `noteSearch`, `textSummarize`, `toolList`, `toolRequest`, `fetch`, `search`, `download`) + `tasks` + `actionRequest`. The last two ARE catalogue rows (`src/tools/catalogue.rs`, `tasks` and `actionRequest` entries); this build answers them by handing the call to the session's task board (`ctx.task_host`) / action host rather than running a tool body. The current spellings are `tasks`, `actionRequest` and `files`; the pre-rename names `task`, `fileList`, `profileManage`, `chapterManage` and `locationManage` are pinned ABSENT from the listing | `tools::dispatch`, `catalogue::ENTRIES` · `tests/tools.rs::the_catalogue_lists_every_group_in_the_published_order`, `::declarations_are_catalogue_ordered_and_follow_the_policy`; the stale-name pin is in `src/tools/mod.rs`'s own test module |
| Catalogued-only rows | **deviation 23** (the availability vocabulary cannot say it) | the same 15, executable through the runtime compiler | 15 names in `MISSING_CAPABILITY` - `toolRegistry`, `toolManage`, `toolCreate`, `records`, `profiles`, `chapters`, `locations`, `agentManage`, `agentResult`, `delegate`, `threadFork`, `toolCancel`, `threadSend`, `threadRead`, `threadList` - exist for discovery and answer a well-formed **FAILED** tool result (`ToolFailure::Failed`, rendered by `result::failed_outcome`), and `is_declared()` is false for every one, so a model is never offered a tool this build cannot run. Since the availability-honesty fix the LISTED row's `availability.hint` carries the identical sentence `dispatch` refuses with, so `GET /tools` and an actual call can never disagree | `tools::MISSING_CAPABILITY`, `tools::missing_capability`, `tools::missing_capability_hint`, `ToolRegistry::availability_of`, `result::failed_outcome` · `tests/tools.rs::the_catalogue_lists_every_group_in_the_published_order` (it compares the row's hint with the refusal sentence directly) |
| Declaration-only tools | **parity** | declared + executable | `actionRequest` and `tasks` are DECLARED here (so the model is offered them) and the store executes both. They are **catalogue rows**, not side entries - the earlier claim that they were not was wrong and contradicted the row above | `tools::dispatch`, `tools::ToolRegistry::is_declared`, `catalogue::ENTRIES` · `tests/tools.rs::declarations_are_catalogue_ordered_and_follow_the_policy` |
| Worker modes | **deviation 15** | `inline` \| `worker` for both knobs, and BOTH are served; a deployment that cannot isolate a lane **aborts the start** rather than running inline (`src/server/workers/`, `sessions.ts`) | `inline` only. The two knobs still PARSE with the TS defaults, ranges and sentences - `GET /config` reports what the deployment asked for - and `runtime::validate_supported_execution` refuses to build a runtime on anything but `inline`: the standalone executable prints the refusal and exits 1 before binding, and the gateway answers every mounted request with 400. **Rejected, never silently degraded** | `runtime::validate_supported_execution` · `tests/runtime_execution.rs`, `tests/worker_execution_http.rs` |
| Default data root | **deviation 18** | the TS harness package has **no** default of its own - "no root means in-memory" (`src/server/service.ts:327`) - and the path it does run on comes from its host: `<repo-parent>/temporary/dump` (`packages/underload/service/server.ts:25`, `provider-server.ts:80-84`, the latter also honoring `UNDERLOAD_DATABASE_ROOT`), and its own persistence **v2** shard tree | The same rule is what `package_root::default_database_root` implements (`<repo-parent>/temporary/dump`, lexically normalized, `src/package_root.rs:96-101`), but this crate's persistence is **v1**. The two runtimes do NOT share state through it, and one root serves one runtime at a time - see [The data root](#the-data-root) | `package_root::default_database_root` (`src/package_root.rs:96-101`), `persistence::HARNESS_DATABASE_VERSION` · `tests/foundation.rs` (the default root is `repo_parent()/temporary/dump`) |
| Context bar | **deviation 3** (id offset) and **deviation 4** (no prose-window segments) | user / system / reasoning / assistant / tool segments per section, `context:*` ops, block detail route | **tool** segments (opened by the call, settled by its result), the **assistant** fallback (a `done` answer that streamed no deltas), and - since the projector seed landed - `system` and `user` segments from the prompt, instruction sources and steers the seed carries. `ProjectorFactory::create` takes a `ProjectorSeed` (`src/ports.rs:321`); the "user/system are impossible" claim is dead, see [Deviations](#deviations-the-full-list) item 3 | `ports::ProjectorSeed`, `ports::ProjectorFactory::create`, `projector`'s segment mints · `tests/projector.rs`, `tests/http_sessions.rs` (the block route resolves the segments the fold mints) |
| `TurnSummary.blocks` | **not ported** | the agent-results registry's block rows | `[]`: there is no agent-results registry in this crate | the summary fold's `"blocks": []` (`src/projector.rs:2065`) · `tests/projector.rs` |
| `delegate` / subagents | **not ported** | the delegation runtime registers it dynamically, and a call now takes the reference's reserved per-call `async` switch (a LONE `delegate` call defaults to SYNC, `async: true` to a background launch) | absent: the catalogue row is discovery-only (one of the 15 `MISSING_CAPABILITY` names) | `tools::MISSING_CAPABILITY` (`src/tools/mod.rs:103`) · `tests/tools.rs` |
| Per-call `async` argument | **deviation** (accepted, ignored, unadvertised) | `{"async":true}` launches the call in the background, `false` runs it sync, absence takes the tool's default - and the key is now offered on EVERY tool declaration, so the model can see the switch | The key is accepted and silently ignored, with no error text: argument checking stops at the documented `required` properties, so `async` is just another unknown key. Nothing strips or rejects it, and the refusal is not the route-level sentence `Unknown field "async". …` - that one fires only for `async` as a SIBLING of `parameters` on the two manual-execution routes. There is no turn-mode concept here at all (`Entry` has no mode field, calls run sequentially and awaited, `tool_result` always carries `pending: []`), so the delegation-mode question - sync or background for a lone `delegate` - does not arise; `async: true` asks for a background launch this crate cannot provide and the call just runs inline. The deliberate choice is NOT to advertise the key: `parameters_json` stays verbatim (`transport::wire_tool`), because a switch this build cannot honor would promise an effect it cannot deliver. The open half - strip the key, or advertise it - is recorded in `docs/parity.md` ("Not ported, and not claimed") | `tools::missing_required` (`src/tools/mod.rs:644-659`), `transport::wire_tool` (`src/transport.rs:639-648`), `turn::settle`'s `pending: Vec::new()` (`src/turn.rs:708`), `api::require_body_fields` (`src/api/mod.rs:1205-1216`) · no test - the behaviour is the absence of one; the test home for the open half is `tests/tools.rs` |
| Provider | **parity** | any OpenAI-compatible base URL | same transport, plus the recorded retry/backoff rules | `src/transport.rs`, `transport::*` · `tests/transport.rs` |
| Session records (`GET …/sessions/{id}/records`) | **mounted, not implemented** | `api.ts` `getSessionRecords` answers the store snapshot or the delta | Route, `since` gate and both response DTOs are contract-complete; the engine is not, so a real session answers 500. An unknown session still 404s and a malformed `since` still 400s | `api::records::get_session_records`, `api::records::record_store_missing`, `contract::RecordsSnapshotResponse` · `tests/wave_one.rs::the_records_route_runs_its_gates_before_the_placeholder`, `::the_records_shapes_round_trip_the_contract_fixtures`. See item 20 |
| Session-agent binding | **parity** | `POST …/agents` / `PATCH …/agents/{id}` accept `rank`; the retired `role` field is refused as an unknown field | The wire row speaks `rank`: `BIND_FIELDS` is `name, profile, model, instruction, rank, prefix` and a legacy body gets `Unknown field "role". …`. The one durable divergence is storage shape - `SessionAgentEntry` has no rank column, so the session's own rank rides its `extra` map and is tolerant on read | `api::session_agents::{BIND_FIELDS, PATCH_FIELDS}`, `contract::AgentRank`, `store::agents::{EXTRA_RANK, entry_rank, set_entry_rank}` · `tests/http_sessions.rs::the_session_agent_rank_vocabulary_is_the_contracts`, `::a_started_conversation_keeps_its_ranks`. See item 22 |
| Agent profiles + presets | **parity** | eight profiles in `AGENT_PROFILE_NAMES` order, eight shipped presets | Eight profiles in the same order (`general, instruct, research, workspace, reviewer, planner, debugger, summarizer`) and eight live preset cards - `general, instruct, research, workspace, reviewer, planner, debugger, chat`, where `chat` is a CARD and not a profile - so the `Unknown agent "x". Known agents: …` refusal names the same eight names the reference prints | `agents::profiles` (eight entries in `AGENT_PROFILE_NAMES` order), `registry::presets::shipped_presets` · `tests/data_plane.rs::catalogue_ships_eight_profiles_and_replays_operator_edits`, `::registry_ships_edits_and_tombstones_the_eight_presets`, and the `Known agents:` refusals pinned at `tests/data_plane.rs:1995`/`:3152` and `tests/http_host.rs:1375` |
| `toolFolders` deployment surface | **not ported** | the tool runtime resolves a configured `toolFolders` list and persists created modules into it | No deployment knob: nothing in this crate reads or configures `toolFolders`, although the `toolManage` / `toolCreate` rows' prose describes it. Those rows are discovery-only here anyway | `catalogue::ENTRIES` (the `toolManage` row's `parameters_json`), and the absence of any `toolFolders` knob under `src/` · no test - see item 24 |

## Deviations (the full list)

Stated, not hidden. Each is a decision with the TypeScript evidence beside it; `knowledge.md` carries
the longer reasoning.

1. **`turnId` inside ops and section ids.** Ops carry `turnId: ""` and sections are `s{ordinal}`,
   not `${turnId}:${ordinal}`. Both are opaque, unique within the turn, and identical across live and
   replay, which is all the wire contract states ("Stable, opaque, server-minted",
   `src/engine/shared/turn-display.ts:1047`).
2. **Record-only members of `summary()`.** `prompt`, `threadId`, `threadLabel`, attachments,
   references and steers are filled by the store's row read model (`store::read::TurnState::row`),
   because the projector seam never sees the record; the fold states `prompt: ""` and omits the rest.
3. **The context bar's record-only half — SUPERSEDED 2026-11-14.** This item used to say a `user`
   block (the prompt, and one per steered submit) and a `system` block (one per recorded instruction
   source) were NOT minted, because `ProjectorFactory::create` took no prompt. That cite had drifted
   (`ports.rs:239-241` is the `settle` doc comment today): the factory method is
   `fn create(&self, seed: &ProjectorSeed)` at `ports.rs:321` (trait at `:320`), and the seed it takes
   (`ProjectorSeed`, `ports.rs:272-314`) carries `prompt` (`:278`), `user_message_id` (`:304`),
   `instruction_sources` (`:308`) and `steers` (`:313`). The fold now mints the `system` segment from
   that seed (`src/projector.rs:545`) and the `user` segments (`:566`, `:605`), so the "not minted"
   half of this deviation no longer describes the build. The id rule below is what is left of it, and
   this pass did NOT re-derive it: the ids follow TypeScript's
   `${section.sections.length}-${kind}-${key}` shape except for the missing prompt section at index 0,
   so a segment on the root flow is one ordinal lower (`1-tool-7` where TypeScript says `2-tool-7`).
4. **No prose-window segments.** TypeScript opens a `reasoning`/`assistant` block per window of
   consecutive deltas and patches it on every append. This fold puts those bytes in the node and the
   section `tail`, and mints the `assistant` segment only on the `done` fallback TypeScript uses when
   the window held no deltas.
5. **Duplicate tool creation REPLACES.** The TypeScript kernel does not reserve a created tool's name
   (`src/engine/core/creation.ts`), so `POST .../tools` over an existing created name answers `201` with
   `{created: false, replaced: true, previousRevision}` and spends no slot. A name owned by the
   catalogue or the harness answers `409` instead, because this port has no override row to replace -
   TypeScript's override path is a different record.
6. **`agentColors` follows `agent_models`.** It is a typed `Option<BTreeMap<String, String>>`, so a
   non-object value is refused with serde's type sentence rather than the TypeScript domain sentence
   that names `agentColors`. The hex, membership and blank refusals are the TypeScript texts.
7. **`checks` is omitted from a registration receipt.** TypeScript runs a compiler probe; this build
   has none, so the member is absent rather than `[]`.
8. **`/tools/{name}` for an unlisted runtime row answers 404.** The row is in the catalogue but not
   listed, matching `api.ts` `getToolOne` - its `toolEntryOf` lookup (`api-reads.ts`) has no row for a
   withheld name, so `throw missing('tool', name)` is the answer); the SESSION
   toolbox still shows all 36.
9. **A created tool's name grammar** (`tool…`, the `toolWordCount` sentence) is this port's reading of
   the kernel; the TS kernel validates a created name differently.
10. **The budget sentence** is "This session can create at most N tools. Remove one first." - the
    TypeScript text, kept while the slot accounting matches.
11. **`glob`/`grep` read no `.gitignore`.** TypeScript parses ignore files
    (`src/engine/tools/files/gitignore.ts`); the counters that would carry a parsed pattern set are not in this
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
    lane cannot be isolated TypeScript **aborts** (`src/server/workers/capability.ts`,
    `src/server/workers/io-client.ts`, and the lane-capability refusal in `src/server/sessions.ts`) -
    it never runs a promised worker turn inline. This build has no worker
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

16. **The plugin host is PORTED but not INTEGRATED into the turn loop — rewritten 2026-11-27.**
    This item used to say "this build has no plugin host", and that is no longer true. What the
    reference does: it scans `src/engine/plugins/**/*.plugin.ts`, the generated
    `src/engine/plugin/manifest.ts` wires what the scan found, each plugin's `setup(api)` registers
    lifecycle hooks that change a run, and the server exposes that catalogue and the operator's
    overrides (`src/server/plugins.ts`, fed to every harness through `sessions.ts`'s live provider).
    What this crate now has, ported module for module from `src/engine/plugin/**`: the lifecycle
    event catalogue (`plugin::event`), the hook bus with the gate, budget and fail-open rules
    (`plugin::hooks::HookBus`), the host with resolve/replace-in-place and the enablement gate
    (`plugin::host::PluginHost`), the seam a plugin acts through (`plugin::context`), the
    plugin-facing settings view (`plugin::settings`), the attributed reporter
    (`plugin::diagnostics`), and `async-ping` as a real Rust `Plugin` (`plugin::async_ping`). What is
    NOT done is the wiring: **no `PluginHost::run` call site exists anywhere under `src/`** (every
    caller is in `tests/plugin_host.rs`), so the bus is never driven from a request or a turn;
    `plugin::context::NullRuntime` is the only `PluginRuntime` implementation, so a plugin that asks
    the seam for run state gets the null answer; and `context-trimmer` / `context-compaction` have no
    `Plugin` module at all (only `async-ping` does, as `src/plugin/mod.rs` records). So the honest
    sentence is not "no hook executes because there is no host" but **"the host exists and nothing
    calls it during a turn"** - and the consequence is the same one the original item stated:
    `async-ping` does not ping during a turn and `context-compaction` does not compact, a row is
    metadata plus values rather than a live registration, and the behaviour TypeScript proves
    (`src/server/plugins-api.test.ts` - the live settings provider reaching an existing session) has
    no consumer on the turn path here. What IS ported on top of the host is the whole HTTP contract:
    `src/plugins.rs` ships the **three** built-in rows with their exact metadata, the descriptors, the
    effective values, every validation sentence, the `null` reset, the revision counters and the
    durable `pluginSettings` delta, and an accepted patch mutates the one map every later read of
    every key goes through, in the same process, before the answer.
17. **The published / enforced tool-result budget split — SUPERSEDED 2026-11-27.** This item recorded
    a published `262144` against an enforced `65_536`. Neither figure is current, and the split itself
    is gone: both figures are now **32,768 bytes per result** and **98,304 per aggregate per turn**,
    and the published value is DERIVED from the enforced one
    (`contract::MODEL_TOOL_RESULT_BUDGET_BYTES = 32 * 1024` and
    `contract::PUBLISHED_TOOL_RESULT_BYTES = MODEL_TOOL_RESULT_BUDGET_BYTES as u64`,
    `src/contract.rs:122`/`:129`), which is what stops the two drifting again. The gap closed by
    **narrowing**, not widening: the reference gates at 8,192 result tokens × 4 bytes
    (`src/engine/core/result-budget.ts` `MODEL_TOOL_RESULT_BUDGET_TOKENS` × `MODEL_TOKEN_BYTES`) and
    24,576 aggregate tokens, so the contract publishes 32,768 / 98,304 and this crate matches it
    (closed under the contract-parity stage, 2026-11-27; `tests/foundation.rs` pins
    published == enforced). What is LEFT of the deviation is the **measurement currency**, not the
    magnitude: TypeScript gates on JSON-ESCAPED payload bytes (`modelPayloadBytes`, the number the
    provider bills), this crate gates on raw UTF-8 bytes (`tools::result::utf8_byte_length`), so the
    same cap bites slightly EARLIER here on text heavy with quotes or newlines - JSON escaping adds
    roughly 2-15% (`src/contract.rs:107-113`). A second, related interaction is stated in the same
    source comment and is not closed here: this crate's `read` tool budgets its own content at
    `tools::files::READ_CONTENT_BUDGET` (64 KiB, `src/tools/files.rs:57`), which is now ABOVE the
    32 KiB result cap, so a
    `read` that exactly fills its own budget arrives JSON-refused rather than whole; the reference
    aligned the two by setting `FILE_READ_WIRE_BUDGET = MODEL_TOOL_RESULT_BUDGET_BYTES`.

18. **Persistence is v1 while the reference is v2, so a data root is not an interchange format.**
    Stated in [The data root](#the-data-root), repeated here because it is the deviation an operator
    meets first. `persistence::HARNESS_DATABASE_VERSION = 1` and the document is one
    `harness/state.json`; the reference is at `HARNESS_DATABASE_VERSION = 2`
    (`src/server/persistence.ts:223`) with a shard tree - `harness/index.json`,
    `harness/sessions/<id>/session.json`, `harness/sessions/<id>/threads/<tid>.json`
    (`:243-246`) - and its header says the v1 single file "is NOT read, migrated, rewritten, renamed
    or deleted", so an upgrade "starts empty next to the old file" (`:31-33`).
    Consequences, in order of how much they hurt: (a) neither runtime restores the other's sessions,
    so the README's old "whichever starts second restores the other's sessions from `state.json`" was
    simply false; (b) the reference's commit pass recursively deletes shard footprints its committed
    header no longer names (`eraseVanishedShards` → `eraseTree` → `eraseHarnessShardPaths`,
    `src/server/persistence.ts:3440`/`:3476`/`:3501`) and treats `harness/sessions/<id>` as one
    footprint (`:236-242`, with `HARNESS_SESSION_DIRECTORY = HARNESS_JOURNAL_DIRECTORY` at `:246`), which is the directory this crate writes its journals into - so pointing
    the reference at a Rust-written root can destroy Rust journals, while pointing Rust at a
    v2 root only loses visibility; (c) `harness/agents.json` shares a name, not a document - this
    crate writes a `{version, records}` envelope and the reference requires a bare array
    (`src/server/persistence.ts:3570`). What IS shared today is the journal path and its bytes:
    `harness/sessions/<id>/events.jsonl` in the TypeScript line format
    (`journal::journal_event_line`, `src/journal.rs:145-153`, and the module header at `:17-37`), so
    a journal either side writes is readable by the other. The rule for operators: **one root serves
    one runtime at a time; give each deployment its own `--database-root`.** There is no cross-process
    lock either, which is the other half of the old warning.
19. **Durable reads tolerate a legacy roster row the reference refuses.** A harness-preset or
    session-definition row written before the rank pass carries the retired `primary` / `subagents`
    pair instead of `entry` / `ranks`. TypeScript REFUSES it with a named error and does not quietly
    translate it (`src/server/persistence.ts` `readPresetRecord` → `requireNoLegacyRosterKeys`, whose
    own comment says a silent rename "would let a row whose delegation shape this build cannot express"
    through). This crate READS it: `entry` and `ranks` are `Option` on both records
    (`persistence::HarnessPresetRecord`, `src/persistence.rs:174-204`) and a row carrying only the
    legacy pair is restored as the rank statement it used to mean (entry above, every name in
    `subagents` one rank below, the kernel's rule-1 default). The upgrade is one-way on save:
    `to_record` emits `entry`/`ranks` and clears both legacy keys, so a legacy root BOOTS here and is
    rewritten in the new spelling, while the same root fails to boot there. This is a deliberate
    departure in the direction the crate's "a durable file is never a reason to fail a boot" rule
    points; it means an operator who has booted the Rust server once against an old root can no longer
    compare the two documents byte for byte. The legacy pair rides the durable fixture
    `tests/data_plane.rs` builds at `:151`/`:166`/`:226`, and the wire half - the card answers with
    `entry` and `ranks` and neither retired key - is pinned by
    `tests/data_plane.rs::tool_detail_execution_gate_and_card_membership_serialize_exactly`.
20. **`GET …/sessions/{id}/records` is mounted but answers 500 - "mounted, not implemented".** The
    route, the session gate, the `since` grammar and BOTH response DTOs
    (`contract::RecordsSnapshotResponse`, `contract::RecordsDeltaResponse`,
    `contract::RecordCollectionRow`, `contract::RecordEntryRow`, `contract::RecordChangeRow`,
    `contract::RecordFieldDescriptor`) landed as contract. The ENGINE did not: there is no
    `src/store/records.rs`, `StoreInner` holds no collections, change feed or watermarks, so there is
    no `store_rev` to print and no entry to project. The refusal is raised AFTER the gates, inside the
    contract's error envelope, so everything that IS implemented answers as the reference does - an
    unknown session is a 404, a malformed `since` is a 400, a wrong verb is a 405 - and only a real
    session against a store with no engine reaches
    `api::records::record_store_missing` (`src/api/records.rs:118-121`):
    `The record store is not implemented on this build; GET records answers once the record-store
    stage lands.` It is not modelled as a capability refusal, because the contract declares no
    capability for records. Pinned by `tests/wave_one.rs::the_records_route_runs_its_gates_before_the_placeholder`
    and `::the_records_shapes_round_trip_the_contract_fixtures`.
21. **The plugin settings key sets are NARROWER than the reference declares, and two toggles are
    spelled the pre-2026-11-13 way.** The catalogue rows and the descriptors are ported, but the key
    sets are not upstream's. `context-compaction` ships 6 keys and has no `headroomRatio`
    (`src/plugins.rs:206-266`; the reference declares 7, `headroomRatio` at
    `src/engine/plugins/context-compaction.plugin.ts:480`). `context-trimmer` ships 12 keys against
    upstream's 17 (`src/plugins.rs:290-422`): it lacks `trimInvalidTools`, `invalidToolTurns`,
    `trimThoughts`, `thoughtTurns`, `supersededTurns`, `duplicateTurns`, `truncateToolRequests` and
    `truncateTurns`, and it carries three names upstream does not -
    `dropInvalidToolCallsOnceAnswered`, `dropOldThoughts` and `thoughtKeepUnits`, the first two being
    this catalogue's pre-2026-11-13 spellings of upstream's `trimInvalidTools` / `trimThoughts`
    toggles (`src/plugins.rs:454-462`). Two visible consequences are already modelled rather than
    faked: a `parentKey` must name a boolean row this catalogue actually holds, so `invalidOutcomes`
    is parented under `dropInvalidToolCallsOnceAnswered` instead of upstream's `trimInvalidTools` - a
    verbatim port would have hidden the row forever - and upstream's sixth settings group,
    `tool-requests`, is NOT declared here, because no row of that family exists to file under it
    (`PLUGIN_SETTING_GROUPS: [(&str, &str); 5]`, `src/plugins.rs:472-478`). The `group` / `parentKey`
    hints themselves are projected from a keyed SIDE table (`SETTING_DISPLAY`, `src/plugins.rs:486`)
    rather than declared inline on each descriptor, because the descriptor struct is constructed by
    two files this stage does not own. **Widening any key set is a wire change**, not a refactor: a
    pane renders what the row declares, and a stored override for a key that appears later has to
    survive the restore.
22. **The session-agent binding speaks `rank` on the wire and stores the override in `extra` - the
    `role` migration has LANDED.** This entry existed to record a wire mismatch (`role` accepted
    where the contract projects `rank`); as of 2026-11-27 it does not, and the finished statement is:
    `POST …/agents` accepts exactly `name, profile, model, instruction, rank, prefix` and
    `PATCH …/agents/{agent_id}` exactly `model, instruction, rank, prefix`
    (`api::session_agents::BIND_FIELDS` / `PATCH_FIELDS`, `src/api/session_agents.rs:48`/`:59`), and a
    legacy body carrying `role` gets the unknown-field refusal naming it - which is what the
    reference does (`CreateSessionAgentRequest` is those six members with `additionalProperties:
    false`; `api.ts` says out loud that the old `role` field "is not accepted at all"). Pinned by
    `tests/http_sessions.rs::the_session_agent_rank_vocabulary_is_the_contracts` and
    `::a_started_conversation_keeps_its_ranks`. The one real divergence left is STORAGE shape:
    `SessionAgentEntry` is the durable record this build inherited and has no rank column, so the
    session's own rank rides its `#[serde(flatten)] extra` map under the `rank` key
    (`store::agents::EXTRA_RANK`, `entry_rank`, `set_entry_rank`). That makes the member ADDITIVE -
    an older file simply has no key - and deliberately TOLERANT: a hand-edited `{"rank": "leader"}`
    or `{"rank": 3}` reads as "no override" and falls through to the harness definition's letter,
    because a durable file is never a reason to fail a boot. The reference, whose record has a real
    column, has no such fall-through to model.
23. **The tool-availability vocabulary cannot say "catalogued but not executable", so the honest
    channel is `availability.hint`.** The contract's `?availability=` filter is a TWO-valued closed
    enum (`available` / `unconfigured`, `api.ts`), so "listed but this server cannot run it" has no
    wire value of its own, and inventing one would be a contract change. A catalogued-only row
    therefore keeps `state: available` - which a client reading `state` alone will over-trust,
    inferring runnability from a field that was never about execution - and carries, in
    `availability.hint`, the EXACT sentence `dispatch` answers the call with: `missing_capability_hint`
    is built from `missing_capability`, the one lookup both halves use, so `GET /v1/harness/tools` and
    an actual call to that tool can never tell two different stories about the same name
    (`tools::{missing_capability, missing_capability_hint}`, `ToolRegistry::availability_of`,
    `src/tools/mod.rs:123-149`/`:224-245`). Pinned by
    `tests/tools.rs::the_catalogue_lists_every_group_in_the_published_order`, which compares the row's
    hint with the refusal sentence directly rather than restating it.
24. **There is no `toolFolders` deployment surface, although the catalogue rows describe one.** The
    `toolManage` and `toolCreate` rows' prose and JSON Schema describe persisting a module into a
    configured folder - `path` resolves "against a configured `toolFolders` entry", and a pathless
    `create` "targets the LAST configured entry" (`catalogue::ENTRIES`, the `toolManage` row's
    `description` and `parameters_json`). This deployment configures NO such list: no `src/` symbol
    reads or exposes a `toolFolders` setting, and no `HARNESS_*` variable carries one, and the crate
    compiles no `.tool.ts` module at all (the module host is still to port, `src/lib.rs`). So the
    folder language in those rows is a description of the REFERENCE runtime, mirrored for wire parity,
    and it is currently moot here because both rows are discovery-only `MISSING_CAPABILITY` names. If
    the runtime-compiler stage lands, the knob has to be added - it is a deployment-surface change,
    not an implementation detail.
25. **`tasks` rides the session's task board, not a record collection.** A client that bootstraps its
    task list from `GET …/sessions/{id}/records` will get nothing here even after the record store
    lands, because the `tasks` tool answers through the session's task host (`ctx.task_host`,
    `tools::dispatch`, `src/tools/mod.rs`) and the task rows are the session's own
    (`store::actions::TaskRow`, with `blocks` / `blockedBy` / `activeForm` / `metadata`). The HTTP
    surface for the same rows is `GET …/sessions/{id}/tasks`. Both are catalogue-visible: `tasks` is a
    row in `catalogue::ENTRIES`, and the task board is not one of the record collections the
    `records` DTOs enumerate. A client that conflates the two will show an empty board next to a
    populated tool.
26. **Restore tolerance is asymmetric on a thread roster: a failed `core.threads` decode restores an
    EMPTY roster here, where the reference throws.** `store::restore_threads` decodes through
    `core_of`, which swallows a failed decode, so a roster with one bad row - the case that surfaced
    it was a bad `contextLane` - comes back as "this session has no conversations" rather than
    refusing the boot. The reference's `readThreads` (`src/engine/core/snapshot.ts:969`, exact-key rows via
    `readSnapshotRecord`) THROWS, so the
    same document fails to start a reference server and starts here with its conversations silently
    emptied. What this crate DOES refuse is the lane at the typed record boundary
    (`persistence::HarnessThreadRecord::context_lane` is `Option<u64>`, `src/persistence.rs:936`, so
    `-1` / `2.5` / `"2"` are decode
    errors) and the per-turn lane in `persistence::validate_document`. Making the roster strict is a
    signature decision (`restore_threads` returns `Vec<_>` and is tolerant by design), so it is
    recorded rather than changed; both halves are pinned by `tests/context_lane.rs`, and
    `knowledge.md`'s 2026-11-11 lane section states the same fact as its "facts a later stage must not
    re-derive" item 3.

## Verification

The gate is the four commands in [Commands](#commands), run from `packages/typecrate/harness`;
`npm run verify` chains them (`fmt:check` → `lint` → `test` → release build of the server). This
section deliberately does **not** restate a pass count. A build is running against this tree right
now and every wave moves the numbers, so the truth to check is the suite itself, not a figure copied
from a log.

```bash
cargo fmt -- --check
cargo check --all-targets
cargo clippy --all-targets -- -D warnings      # strict: nothing here is allowed to warn
cargo test --no-fail-fast                      # every target below
```

The suites are the specification, and each one is named for what it pins:

| Target | What it pins |
| --- | --- |
| lib unit tests (`src/**`) | per-module rules the shipped surface cannot reach over HTTP - the write-edge validators, the coercion helpers, the pure pruning rules |
| `bin/server` unit tests | the standalone executable's flag parsing, the banner, and the refusal exit codes |
| `context_lane` | the published display lane end to end, and the thread-roster restore asymmetry (item 26) |
| `contract_message_flags` | the stored message axes (`included` / `reasoningIncluded`) and their decode rules |
| `data_plane` | the durable round trip, the atomic commit, both repairs, the journal, the registry (eight presets), the agent catalogue (eight profiles), the instruction ladder |
| `foundation` | the boot document (published == enforced `tool_result_bytes`, item 17), the wire vocabularies, cursors/limits/page slicing, the config loader, the hub, the origin gates, the body validators, the paths |
| `http_host` | the host routes over a real `axum::serve` listener, the boot document's `limits` and its ten `links`, the tool page and the identified read |
| `http_sessions` | the session domain: turns, threads, transcripts, actions, the task list, the session-agent rank vocabulary, the context-block inclusion write |
| `plugin_host` | the bus, the gate, the budget, fail-open, the round reducer, the host's resolve/replace-in-place - driven DIRECTLY, so it is not evidence that a turn runs a hook (item 16) |
| `plugins` | both plugin scopes over HTTP: the three catalogue rows and their descriptors, `q` and paging, the 405 sentences, the durable delta, the `null` reset, every refusal, the lenient and strict restores, the display-grouping projection |
| `projector` | the display fold: sections, steps, nodes, tails, notices, and the context segments |
| `routes` | the route table against the TypeScript contract - the 52 templates in order, the 74 operations and the verb split, one reachable mount per operation, and the reference-text re-read |
| `runtime_execution` | the `worker` gate: the refusal sentence, the runtime constructor, and the standalone process's exit |
| `store` | the session store's rules: the submit surface, the lanes and the pump, the command path, the recorder, transcripts, threads, owner actions, and what a run leaves durable |
| `store_restore` | the restart path: what a boot reads back out of `state.json` plus the per-session journal, the interrupted-run repair, and what it deliberately does not keep |
| `stream` | the `harness.v1` handshake, frames, cursors, bounded replay, and the `1001` host shutdown |
| `tools` | the catalogue listing order, the listed/unlisted split, the declarations and policy, and the catalogued-only refusal matching the advertised `availability.hint` (item 23) |
| `transport` | the provider request document and headers, the streamed SSE fold, every failure sentence against a local listener, and the wire transcript |
| `turn` | the turn engine: the frame sequence per turn shape, the two commit rules, the retry arithmetic, the rollback rule, and the steering channel |
| `wave_one` | the wave-one operations: the `records` gates and placeholder (item 20), the block-inclusion write, `toolSettings` persistence, the stream vocabulary, the refusal-header rule |
| `worker_execution_http` | a mounted request against a `worker` deployment answering 400 before the route match |

The target LIST is derivable without building: `Cargo.toml` declares no `[[test]]` section and no
`autotests` key, so every `tests/*.rs` file is its own integration target - 19 of them, plus the lib
unit tests and the `bin/server` unit tests, for 21 targets. That is the only count in this section
that is not a test count.

The per-target counts the earlier revision of this file tabulated are **retired as a historical
snapshot, not corrected**: they were the state at stage T10 and were already stale at several rows
before the current waves landed. The last time the full matrix was written down with figures is
`knowledge.md`'s dated verification sections (T7b, T9, T10, and the 2026-11-11 lane pass), which is
where a dated count belongs; nothing in this file claims a present-tense count.

### The `typecrate-services` side

The gateway bridge is `endpoint/harness/harness-route.rs`; `ANY /v1/harness/{*rest}` means new harness
routes are reachable through the gateway with no route-file change. Two claims this file used to make
about the sibling package are no longer true and are dropped rather than restated:

* **The wasm32 gate does not exist.** `packages/typecrate/services` is native-only - its
  `Cargo.toml` says so in its own header ("there is no wasm boundary and no wasm target"), it declares
  no wasm target in any script, and `typecrate-harness` is an ordinary
  `[dependencies]` entry, not one behind
  `[target.'cfg(not(all(target_arch = "wasm32", target_os = "unknown")))'.dependencies]`. So
  `cargo check --target wasm32-unknown-unknown` and the `cargo tree` "zero harness packages" argument
  are both gates that are never run: say "not ported to wasm", not "the wasm gate passes".
* **The SIGTERM build failure is a dated observation, not a current one.** The revision before this
  one reported that `packages/typecrate/services/src/bin/server.rs` had a `match`-arm type error in
  its SIGTERM handler, and that the package's `cargo test` therefore failed to build. That was one
  working-tree state at one moment; `knowledge.md`'s T10 section already hedges it the same way, and
  no build was run in this documentation pass, so no claim is made either way. Read the sibling
  package's own state from its own tree.

The one services-side pin worth naming here, because it is this crate's contract seen from outside:
`packages/typecrate/services/tests/harness_gateway.rs` pins the global
`/v1/harness/tools?limit=100` page at **32 LISTED rows** (the comment above that pin at
`:587-595` explains why 31 became 32 when the listed `records` row joined the table: 36 rows exist in
`tools/catalogue.rs`, four are `listed: false`).

`yarn vite-node scripts/check-playground.ts` (in the services package) drives the embedded playground
against a 23-row route-listing fixture - the three `ANY /v1/harness` rows first, then the four
provider-bridge rows, then this package's own - which is the shape
`packages/typecrate/services/tests/server_http.rs::route_table` pins over a socket. The row count is
readable from the fixture; the pass count is not, and the old `122/122` figure is not repeated here.
