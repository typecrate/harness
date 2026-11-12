# typecrate-harness - port knowledge

Decisions taken while porting `packages/underload/harness` (TypeScript) to
`packages/typecrate/harness` (Rust). Every entry names the TS source it came from, so a later
stage can re-check it instead of re-deriving it.

## Stage map (what exists, what is next)

1. **T1 (done)** - crate scaffold + primitives: `contract`, `error`, `paging`, `events`,
   `config`, `environment`, `package_root`, `origin`, `ids`, placeholder `bin/server.rs`.
2. **T2 (done)** - the persistent data plane and the host domain: `persistence` (`persistence.ts`),
   `journal` (`journal.ts`), `labels` (`shared/agent-results.ts`), `ports` (the cross-stage seams),
   `registry` (`harnesses/{registry,presets,descriptor}.ts`, definitions only), `agents`
   (`src/agents/**` + `harnesses/agent-state.ts`), `read` (the host half of `api-reads.ts`).
2.5. **T2.5 (done)** - foundation contract remediation against an independent audit: the
   `{scope, manual}` execution gate, `optionId`, a known page total, `subagents` on the card,
   report-only `publish` + the `ops` channel, the origin tables, `id: null`, the service name, and
   the base36 epoch suffix. See "T2.5" below for what a later stage must not re-break.
3. **T3 (done)** - the session store (`store/**`): the `sessions.ts` records, the turn recorder, the
   command registry, the toolbox, conversations, actions and the restore path.
4. **T4 (done)** - the runtime half: the tool catalogue and executable tools (`tools/**`), the
   provider transport (`transport/openai.ts`), the turn engine (`core/agent.ts` + `harness.ts`
   `runAgentCore`) and the display projector (`server/turn-projector.ts` +
   `shared/turn-display.ts`).
5. **T5a (done)** - the HTTP surface: the per-root runtime (`runtime.rs`), the `/v1/harness`
   dispatcher and route table (`api/mod.rs`), the host handlers (`api/{root,config,harnesses,
   agents,tools,models}.rs`), the router + graceful serve + shutdown flush (`server.rs`), the
   standalone executable (`bin/server.rs`). The session-domain routes shipped as reachable stubs;
   their bodies landed with T5b. See "T5a" below.
6. **T5b (done)** - the session-domain route bodies (`api.ts:1958-3898`): the nine
   `api/{sessions,actions,queue,usage,threads,transcript,turns,session_agents,session_tools}.rs`
   handlers, the projector / turn-record merge at the read boundary (`store/read.rs`), and
   `tests/http_sessions.rs`. See "T5b" below for the contract-shape deltas a reader must know.
7. **T5c (done)** - the update stream (`stream/**`): the wire vocabulary and the client-frame
   validator (`stream/protocol.rs`), the subscription/sequence/replay broker (`stream/broker.rs`),
   the per-connection conversation (`stream/connection.rs`), the handshake gates (`stream/mod.rs`),
   the `Arc<StreamBroker>` the runtime owns (`runtime.rs`), `api::handle_stream` delegating to it,
   and `tests/stream.rs`. See "T5c" below for the deviations the port had to make.
8. **T6 (done)** - the `typecrate-services` gateway bridge: `endpoint/harness/harness-route.rs`, the
   generated `service_routes!` table, the `ANY` arm it forced into the services macro, and the gateway
   tests in `packages/typecrate/services/tests/`. See "T6" below for the two facts that bridge rests on.
9. **T7a (done)** - the audit remediation of T2-T6 against the TypeScript spec: the LIVE host view
   inside the store, the session's own provider endpoint, the `actions` stream scope, snapshot section
   ids on live display ops, the durable lane backlog, the `action.opened` / `action.closed` frames, the
   host shutdown step, sampling on the provider request, and steering that survives a failed run. See
   "T7a" below - it changes facts stated in T3, T5c and T6.
10. **T7b (done)** - the second audit remediation: `agentColors`, the list-route revision, the
    replace-on-duplicate tool create, the 31-row LISTED tool surface, the context bar's tool and
    assistant segments, body-less verbs, ECMAScript paging coercion, the usage fold, UTF-16 measures,
    the gateway's harness shutdown hook, and the playground fixture. See "T7b" below.
11. **T9 (done)** - the final honesty pass: the runtime-SUPPORT gate that refuses a `worker` deployment
    instead of silently running it inline (`runtime::validate_supported_execution`, the standalone
    startup abort, the gateway's 400), the route-table pin (`tests/routes.rs`), and the documentation
    corrections the independent verification asked for. See "T9" below.
12. **T10 (done)** - the plugin-system uplift: the built-in catalogue and its operator settings
    (`src/plugins.rs`, `api/plugins.rs`), the two `/v1/harness/plugins` routes and their DTOs, the
    additive `state.json` `pluginSettings` member (no version bump), the boot document's `plugins`
    link, the `local/basalt` provider default, and the three cheap contract corrections (the published
    `tool_result_bytes`, the `paused` thread status, the `session` instruction source). NO plugin hook
    executes - see "T10" below for what is ported and what is not.
13. Still to port - the module host that turns a session-created tool's installed source into a callable
    registration, and the runtime-compiler tools the catalogue lists but this build cannot execute (see
    "T4"). A worker backend for the two `*_EXECUTION=worker` knobs is the other unported half: today the
    knobs validate and REPORT, and the runtime refuses to start on them (see "T9"). The plugin HOST is
    the third unported half: T10 shipped the catalogue, the settings contract and the durability, and no
    hook executes (`README.md` Deviations item 16).

## Decisions

### Naming and serde shape

- **snake_case vs camelCase is per-struct, not global.** `harness-api.ts:5-10` states the rule:
  REST resources, receipts and stream frames are snake_case; the display subtree and the domain
  payloads keep camelCase. Rust structs therefore carry an explicit `rename_all` so the intent is
  visible per type.
- **TS intersection "extension" types become `#[serde(flatten)]`.** `HarnessDefinition =
  HarnessCard & {...}`, `SessionResource = SessionSummary & {...}`, `ThreadResource`,
  `SessionToolDetail`, `ToolDetail`, `TurnBlockDetail = ContextBlock & {...}` and every `...Page =
  PageMeta & { items }` are flat JSON objects on the wire, so the Rust form is a struct that
  flattens the base struct. Deserialization round-trips exactly.
- **`Option<Option<String>>` where TS writes `string | null` in a PATCH body.** Absent ("no
  change") and explicit `null` ("restore the shipped default") are different instructions
  (`api.ts:1382-1407`), so the nesting is required. Where TS only writes `?: string` (e.g.
  `RunnerSettingsInput`), a plain `Option<String>` is enough because serde maps JSON `null` to
  `None`, which is exactly how `settingsInputOf` (`api.ts:548-561`) treats a null.
- **No `deny_unknown_fields` anywhere.** The TS server rejects unknown body fields at the handler
  level with `Unknown field "<key>". Allowed fields: ...` (`api.ts:504-510`), so that job belongs
  to `contract::require_known_fields`. A schema-level rejection would print serde's wording.
- **`Literal true` fields are `bool`.** `deleted: true`, `ok: true`, `reset: true` are TS literal
  types; a Rust `bool` set at the constructor site produces the same wire value.
- **`protected` is spelled `r#protected`** in `SessionToolSummary` (Rust keyword); serde still
  prints `protected`.

### Where `serde_json::Value` stands in for a typed model

Only where the TS type is a large recursive union the projector owns, never for a bounded record:

| Alias / field | Why |
| --- | --- |
| `contract::TurnSection`, `DisplayNode`, `DisplayStep`, `DisplayTail`, `TurnDisplayOp` | `harness-api.ts:27-36` ("PROJECTION DELTAS"): the spec models one closed record per type while `shared/turn-display.ts` emits a discriminated union. `api-reads.ts` sends `projector.display()` **verbatim** (a cast, never a restate), so the Rust HTTP layer must also pass the document through. `TurnDisplay.sections: Vec<Value>` is what the contract's `TurnDisplay` (`harness-api.ts:1881`) carries. |
| `HarnessEvent::BranchStart::branch` | `types/index.ts:2017` carries a `HarnessBranchSummary`, a runtime-owned record with 12 members that no REST route restates. |
| `ToolDetail.parameters`, `SessionToolDetail.parameters`, `GlobalToolExecutionRequest.parameters`, `SessionToolExecutionRequest.parameters`, `UpdateThreadTranscriptRequest.messages` | TS types these `unknown` on purpose (`harness-api.ts:657`, `:687`, `:1541`): the tool, not the contract, decides what is valid, and a structurally broken transcript is a SUCCESSFUL write. |
| `ToolCallSuccess.result` | TS `result: unknown` (`harness-api.ts:709`). Always emitted, so an absent value prints `null`. |

Everything the brief asked to define explicitly (`HarnessUsage`, `HarnessMessage`,
`HarnessEvent`, `HarnessAttachment`, `AttachmentDescriptor`, `AgentResultReference`,
`HarnessActionRequest`/`Option`, `HarnessPendingTool`, `TurnSummary`, `ContextBlock`,
`TurnBlockDetail`, `TurnStepDetail`, `RecordedEventRef`, the display-state enums) is a real Rust
type, so a later stage cannot send a misspelled field on those paths.

### Paging

- `paging::page_body` takes the **epoch** (to stamp `next_cursor`) and echoes the EFFECTIVE page
  size, exactly like `pageMeta` (`api-paging.ts:142-153`). The brief's alternative signature
  `(page, limit, revision, observed_at)` dropped the epoch, which would make an un-stampable
  cursor; `limit` is not a parameter because TS computes it from `items.length`.
- `Page<T>` is a **type alias** of `PageSlice<T>` (the brief lists both names; one struct, no
  drift).
- `slice_page(entries, cursor, limit, total)` takes the total exactly like TS
  (`api-paging.ts:124-139`): `None` is TS's `total = -1` (unknown), where an exactly-full page
  ALWAYS advertises a next page; `Some(n)` is what every real canonical route passes, and then
  `hasMore = items.length === limit && served < n` - so an exactly-full FINAL page answers
  `next_cursor: null` instead of a cursor to an empty page. The comparison uses `n`, never
  `entries.len()`, because a windowed store read legitimately materializes less than the total.
- `resolve_limit` treats `?limit=` as ABSENT (the default, 25), because TS reads the query
  through `paramOf` (`api.ts:482-488`), which trims and maps an empty value to `undefined`
  before `resolveLimit` sees it (`api-paging.ts:95`). A raw `Number('') === 0` refusal would be
  a Rust-only 400.
- A cursor token is decoded with `URL_SAFE_NO_PAD` after stripping any `=` the client re-added.
  A decode failure (bad symbols / bad length / non-UTF-8) answers `is not a cursor this server
  issued.`, matching the brief's "malformed" case; TS would fall through to the epoch check and
  answer `stale or foreign` instead. Both are 400 on `cursor`; the distinction is only which of
  the two contract sentences prints.
- The ordinal must be plain digits. TS accepts anything `Number()` coerces (`"1e3"`, `"+5"`);
  a token this server issued is always digits, so a coercion-form token is reported as
  stale/foreign rather than guessed.
- **`serde_json`'s `preserve_order` feature is not enabled** (the dependency list is fixed), so a
  `Value`-built page body enumerates keys alphabetically (`BTreeMap`) and
  `require_known_fields` reports the alphabetically-first unknown key. Struct-serialized
  responses keep their declared field order. JSON objects are unordered, so this changes only
  byte layout; a body with several unknown fields could name a different one than TS names, and
  no contract test pins that case.

### Event hub

- **`publish` REPORTS; `bump` advances.** This is the TS split verbatim
  (`api-events.ts:299-316`): the publisher owns its counters, so a write calls `hub.bump(..)`
  for every resource it moves and ONLY THEN `hub.publish(..)`, which states where each counter
  now stands and fans the change out. A route that bumped and published therefore moves a
  counter exactly once, and `PageMeta.revision` can never disagree with the
  `subscription_state` frame a listener sends. (The T1 port folded the advance into `publish`;
  that deviation is removed, and the T1 tests moved with it.)
- **`publish_with_ops` carries `ops` next to the payload** (`api-events.ts:120-121`
  `ops?: TurnDisplayOp[]`, the `display.ops` channel). The Rust field is always present
  (`HubEvent::ops: Vec<Value>`) and the wire skips it when empty, which is how TS spells an
  absent optional member; `publish` is the same call with an empty list. `Value` rather than a
  typed union because the display op model belongs to the display stage, and the hub only
  relays it.
- **`ResourceRef.id` and `RevisionRecord.id` always serialize**, as `id: null` for a host-wide
  kind (`api-events.ts:93-96` declares `id: string | null` on a closed object). The earlier
  `skip_serializing_if` printed nothing where TS prints `null`.
- The epoch is process-global (`events::process_epoch`, a `OnceLock`), because the contract's one
  comparison rule requires REST and the stream to print the same string
  (`api-events.ts:82-83`, `stream/mount.ts:304`). `EventHub::new()` uses it, so two hubs in one
  process share the epoch and keep separate counters (which is what a test wants).
- Epoch suffix is 12 lowercase **base36** characters: 6 random bytes, each rendered as 2 padded
  base36 digits, exactly like `randomEpochSuffix` (`api-events.ts:65-80`). Same length and same
  48 bits as before, and `uuid` (already a dependency) is the byte source because this crate
  declares no `rand`.
- Listener isolation uses `catch_unwind(AssertUnwindSafe(..))`. Under the release profile
  (`panic = "abort"`) a panicking listener still takes the process down; the isolation is debug/
  test behaviour, and it is documented at `events.rs::fan_out`.
- `HubEvent` carries `session_id`/`thread_id`/`turn_id` directly instead of TS's composite
  `target: {resource, id}` id (`api-events.ts:117`), so no stage has to split and re-join
  `session:turn` strings to route a frame. `baseline_link` still accepts the composite spelling
  for the child kinds, because that is the table's key format.

### Config

- `Invalid runtime settings: ` + each issue joined with `"; "`. The TS loader raises the FIRST bad
  value as a `ServerRuntimeError` (`config.ts:221`, `:241`); this port collects every problem so
  an operator fixes all of them in one pass. `format_server_runtime_issues` is the aggregator the
  brief asked for; there is no `config-validation.ts` / `formatServerRuntimeIssues` in the TS
  package today (verified by grep), so the message text of each individual issue is copied from
  `config.ts` verbatim and only the joining is new.
- A bad runtime/settings value surfaces as `ApiError::bad_request` (400) rather than a distinct
  deployment error type: `ApiError` is this crate's one refusal type, and the message keeps the
  variable name and range that made it a deployment error.
- **`HARNESS_IO_EXECUTION` accepts `inline` | `worker` only.** The brief listed `off` as well, but
  `parseIoExecutionMode` REUSES `parseThreadExecutionMode` (`config.ts:249-250`), so TS has no
  `off` spelling and the refusal message names both accepted words. Default is `inline`
  (`config.ts:119`).
- **ACCEPTANCE is the parser's, SUPPORT is the runtime's (T9).** This loader accepts `worker` for both
  knobs, exactly like TS, because it is also the channel `GET /config` reports through and the config
  tests feed it explicit maps. What this BUILD provides is `inline` only, so
  `runtime::validate_supported_execution` refuses a `worker` deployment at runtime-construction time
  (400, naming the variable) and the standalone executable aborts before binding. Do not move the
  check into `load_server_config`: that would make a worker deployment unreportable and would change
  every config-loader test.
- `ServerConfig.max_retries` / `retry_delay_ms` / `request_timeout_ms` are Rust-side additions (the
  TS retry policy is an agent constructor option, not an environment). Defaults come from
  `core/agent.ts:175-176` (10 retries, 1000 ms base); `request_timeout_ms` defaults to **0 = no
  timeout**, because the TS transport sets none (`transport/openai.ts`). Env names:
  `HARNESS_MAX_RETRIES`, `HARNESS_RETRY_DELAY_MS`, `HARNESS_REQUEST_TIMEOUT_MS`.
- The provider URL is validated at load time (absolute, http/https, <=2048) even though TS only
  validates it when a provider call is built (`transport/models.ts:99-131`). Messages are copied
  from that function. The parser here is a small one (no `url` crate), so `localhost:5500/v1`
  - which `new URL` accepts as the scheme `localhost:` and then refuses for the protocol - is
  refused as "not an absolute URL" instead. Same status, same value named, different sentence.
- `apply_settings_patch` models the credential rule as the brief states it (moving `base_url` to a
  different provider clears the key unless the same patch issues one). In TS the state is split
  (`globalSettings.apiKey` + `credentialBaseURL`, `api.ts:1370-1446`) and the clearing happens
  lazily inside `mergeSettings`; a single `HarnessRuntimeSettings` pair cannot represent "key
  retained but out of scope", so the observable answer (the key a provider call would send) is
  identical.
- A `null` write to `api_key` is a **no-op** in `apply_settings_patch`, whereas TS restores the
  *boot* credential (`api.ts:1428-1431`). The Rust settings struct has one key slot, so the boot
  credential is what it already holds. `null` on `base_url`/`model` restores this crate's built-in
  default rather than the boot document's value, for the same reason. A later stage that persists
  a `defaults` block separately should pass those in explicitly.
- `merge_settings` (the per-session/discovery merge of `config.ts:408-449`) is ported alongside it,
  with the same caps and messages, because sessions and model discovery need the scoped-key rule
  too.

### Origin

- `LOOPBACK_HOSTNAMES` is EXACTLY TS's four entries: `localhost`, `127.0.0.1`, `[::1]`, `::1`
  (`origin.ts:27-32`). `0.0.0.0` (the address the service gateway binds) and
  `host.docker.internal` are **not** loopback - `origin.test.ts:38` asserts the first one - so
  those origins pass only through the SAME-HOST rule when the `Host` header agrees. (The T1
  port widened the list to six; that deviation is removed and the tests moved with it.)
- The stream gate applies the SAME rule as REST, including `https://*.github.io`
  (`stream/mount.ts:46-60`, whose header at `:41-44` says the rule is "deliberately identical"
  to `service.ts`'s). The only remaining difference between `check_rest_origin` and
  `check_stream_origin` is the refusal sentence (`service.ts:461` vs `stream/mount.ts:80`).
  `matches_origin`'s `allow_pages` flag is kept so a host that narrows one gate does it in one
  place; both callers pass `true`.
- The host comparison normalizes a **default port away** and lowercases the host, which is what
  `new URL` does and what `config.ts:172-175` depends on for `isSameProvider`. IPv6 brackets are
  kept in the hostname (`[::1]`), matching `URL.hostname`.

### Ids and labels

- Session/turn/request ids are UUID v4 lowercase. The TS host mints `session-<n>` / opaque turn
  ids, and no route parses an id back into parts, so one opaque accessor is enough.
- `host_thread_id(n)` = `thread-<n>` (`core/thread.ts:36`, `workers/lane-identity.ts:294`).
- `agent_code` ports `codeOf` (`agent-results.ts:177-192`): normalize to trimmed lowercase, keep
  ASCII LETTERS only, first + last, uppercase, `AG` fallback.
- The brief's `thread_label(sequence: u64)` has no agent argument, and the TS label is
  `<CODE><ordinal>` where the CODE comes from the agent id, so:
  `thread_label_for(agent, sequence)` is the real rule and `thread_label(sequence)` is the `AG`
  fallback spelling.

### Environment and paths

- `load_harness_environment` uses `dotenvy::from_path`, which does NOT override an existing process
  value (including an explicitly empty one) - the same precedence as dotenv's default
  (`environment.ts:23-27`). The parsed pairs are read a second time with `from_path_iter` so the
  merged map can be built without re-reading the file.
- `HarnessEnvironment` carries `path`/`loaded`/`error` only - never values - because the status is
  what a caller logs (`environment.ts:52-57`). The parsed values live in the private cache.
- `HARNESS_ENV_DEBUG=1` prints the resolved path and the result. TS is deliberately silent
  (`quiet: true`), so this is an opt-in escape hatch only, read from the PROCESS environment.
- A test points the loader at a fixture with `set_harness_environment_path` (the Rust form of
  `EnvironmentFileOptions.path`), and the fixture lives under the repo `Temp/typecrate-harness/`
  per the repo scratch rule.
- `package_root()` is `env!("CARGO_MANIFEST_DIR")` (absolute at compile time), which is the stronger
  answer to the same problem `package-root.ts` solves by walking to `package.json`: no bundle
  layout can disagree, and nothing depends on the cwd.
- `repo_root()` walks up to the nearest `package.json` that declares `"workspaces"` (the monorepo
  marker; a workspace member manifest does not have one) and falls back to four parents.
- `default_database_root()` = `<repo-root>/../temporary/dump` lexically normalized, i.e.
  `D:\Projects\typescript\temporary\dump` on this machine, matching
  `packages/underload/service/server.ts:43` and `packages/typecrate/services/src/server/mod.rs:96`.
  The directory is not created here.

### Crate / build

- Standalone `[workspace]` with `resolver = "3"`, no `members`, so the root `rust/crates` workspace
  is untouched (same trick `packages/typecrate/services/Cargo.toml:28-31` documents).
- `panic = "abort"` in `[profile.release]` mirrors the sibling crate; it is why the hub's listener
  isolation is documented as debug/test behaviour.
- `src/bin/server.rs` is a placeholder that exits 0. It exists because a declared `[[bin]]` target
  needs a compilable file today; the HTTP stage replaces it.
- `Cargo.lock` is generated by the first build and left uncommitted-by-choice for this stage (the
  sibling package commits `--locked`; this one does not yet, because the dependency graph is still
  being added to by the next stages).

---

## T2 - the persistent data plane (`persistence`, `journal`, `labels`, `ports`, `registry`, `agents`, `read`)

Same rule as above: every entry names the TypeScript source it came from, and every place the stage
brief and `persistence.ts` disagreed is recorded with the side that won and why.

### On-disk shape

- **`baseURL`, capital `URL`.** `rename_all = "camelCase"` would print `baseUrl`, which is NOT what
  TS writes (`persistence.ts:134-137`, `sessions.ts databaseRecord`), so `base_url` carries an
  explicit `#[serde(rename = "baseURL", alias = "baseUrl")]` on both settings records. The alias
  keeps a document that used the lowercase spelling readable.
- **Tolerance is per-record.** Every record has `#[serde(flatten)] extra: Map<String, Value>` and
  `#[serde(default)]` on every optional/collection field; NO record uses `deny_unknown_fields`.
  Required SCALARS (`id`, `harness`, `name`, `title`, the settings pair) deliberately have no
  default, so a missing one is a refusal naming the field rather than a silent empty string.
  Consequence a later stage must know: a `SessionTurn.events` element is a `contract::HarnessEvent`,
  which is a CLOSED union with no catch-all, so unknown keys INSIDE an event are dropped on a
  round trip. Everything around the events survives; the event payloads themselves are the
  contract's, and `contract.rs` is not this stage's file.
- **Field-level reasons come from `validate_document`, not serde.** `readTurn`/`readSessionRecord`/
  `readPresetRecord` report the offending PATH (`sessions[3].turns[1].threadLabel must be a
  conversation label such as "RR1".`), so the port walks the raw `Value` first (version gate, the
  two array gates, the credential refusals, the known-status gates, the queue join, the label
  grammar, the 64-entry steer bound, duplicate session ids), then decodes into the typed structs,
  then applies the TS normalizations (`title` trim/blank-absent, `deleted`/`sessionAgents`
  trim+dedupe, label uppercase). Serde's own wording only appears for a shape the raw pass did not
  model, wrapped in TS's `The harness database has an invalid shape or version: ` prefix.
- **Messages are TS's, statuses are the brief's.** The brief paraphrases (`state.json is missing or
  malformed.`, `Unsupported state.json version: {v}.`) and then says "read TS for the exact set",
  so the sentences are `persistence.ts`'s verbatim, prefixed with the absolute path the way
  `HarnessPersistence.load` does (`:1550-1574`), and the status is 400 for every malformed/version
  case (the brief's test contract). TS has no status on a persistence error at all - the dispatcher
  maps an unwrapped throw to 500 (`:311-324`) - so a route that must answer 500 for a corrupt
  database re-maps this 400; the message is the load-bearing part either way.
- **`agents.json` accepts both spellings.** The brief specifies a `{ version, records }` envelope,
  TS writes a bare ARRAY (`persistence.ts:1679`). `serialize_agent_records` writes the envelope;
  `parse_harness_agent_records` reads the envelope OR a bare array, so a database written by the TS
  server still loads and a future format change has a version to gate on. Duplicate-name and
  per-field refusals are TS's (`The harness agent overlay lists agent "x" more than once.`,
  `agents[i].guidelines[j] must not be blank.`).
- **Staging names follow their own target.** TS reuses the `state.json.` prefix for the overlay
  commit (`:1680-1683`), which works but makes an `agents.json` staging file look like an abandoned
  checkpoint. Here the prefix is the canonical file's own name, so the two commits can never
  collide and `state.json.<owner>.<seq>.tmp` still matches the brief.
- **The retry loop is the Windows half of `writeHarnessDatabaseFile`.** A rename onto a file a
  scanner still holds fails with `ERROR_SHARING_VIOLATION` (32) / `ERROR_LOCK_VIOLATION` (33), which
  std reports as `ErrorKind::Uncategorized`, so both the `ErrorKind`s (incl. `PermissionDenied`,
  `AlreadyExists`, `DirectoryNotEmpty`, `WouldBlock`, `TimedOut`, `ResourceBusy`) and the raw codes
  (Windows 5/32/33/145/183, POSIX 1/13/16/17/39/66) are retried, 8 times with a capped 0-160 ms
  backoff. Every attempt removes its staging file, and the staging file is created with
  `create_new`, so a stale name means "take the next sequence", never "append to a half-write".
  `File::sync_all` before the rename is the fsync, and the directory is fsynced where the platform
  can open one (Windows cannot, so that half is best-effort - the same boundary `:1436-1439` states).
- **`repair_interrupted_tool_calls` fixes TURN rows; `repair_interrupted_snapshot` fixes
  transcripts.** The two functions in TS are different: `persistence.ts:1334` (`repairInterruptedTool
  Calls`) repairs the `core` snapshot's agent/thread transcripts, and `sessions.ts:3007-3013`
  closes the server's in-flight turn rows. The brief names the first function for the second job, so
  BOTH are ported: `repair_interrupted_tool_calls(&mut HarnessSessionRecord) -> usize` implements the
  brief's rules (running turn -> `error` + the session-level `INTERRUPTED_MESSAGE` sentence; one
  terminal `isError: true` + `outcome: cancelled` record per dangling call; an orphan result gets a
  synthetic id), and `repair_interrupted_snapshot(&mut Value) -> usize` is TS's transcript repair
  verbatim (`:1278-1330`), kept because the store stage needs it before `harness.restore()`.
- **`INTERRUPTED_TOOL_RESULT_TEXT` is the brief's sentence**, not TS's longer one
  (`persistence.ts:1273-1274`), and `interrupted_turn_message()` is TS's session-level sentence
  (`sessions.ts:704-705`). An orphan result keeps its recorded `content` (evidence is never trimmed,
  `:1263-1267`) and is marked failed/cancelled with the synthetic id `<turnId>:interrupted-result:
  <index>`. `kind: "interrupted_result"` has no wire home: `contract::HarnessEvent` is closed and
  carries no `kind`, so the marker is `INTERRUPTED_RESULT_KIND` + the id prefix
  (`interrupted_result_call_id`), which is what a later reader can test.

### Journal

- **The event line is the brief's envelope, the header line is TS's.** `{"version":1,"sequence":N,
  "timestamp":<ms>,"event":{...}}` replaced TS's per-turn `{"type":"event","v":1,"turnId","index",
  "at"}` (`journal.ts:99-106`) because `read(from_sequence, limit)` needs a per-FILE cursor, and the
  header `{"type":"header","v":1,"sessionId","createdAt"}` is kept byte-identical (`:280`) so an
  empty journal is distinguishable from a stray file. `scan` ACCEPTS both event spellings, so a
  database written by the TS server replays (a TS record simply has no `sequence`, and the gap check
  treats that as unknown).
- **Tail policy.** A file not ending in a newline means the last line is a crash tail: the snapshot
  fold (`read`, `truncate_after`, `create`) drops it silently whether or not it parses - its
  completion is unproven - while an explicit read (`read_all_with_problems`) returns it when it
  parses and always reports `... (truncated tail)`. A corrupt or blank MIDDLE line, an unknown
  version, an event before the header, a header on a later line, and a sequence gap are always
  problems and never carry the tail flag (`:20-22`, `:358-363`).
- **Problems are strings, not `JournalProblem` structs.** The brief's channel is `Vec<String>`, so
  the four TS kinds are folded into `line <n>: <reason>` sentences, and the boolean `tail` is the
  ` (truncated tail)` suffix a caller can test.
- **`truncate_after(keep_count)` re-emits the kept records verbatim** (their own `timestamp` kept)
  and only renumbers the sequence; `rewrite(events)` is the fresh-timestamp form.
- **`remove()` retries the whole directory removal**, for the same sharing-violation reason as the
  checkpoint writer.

### Registry and agents

- **Definitions, not factories.** A TS `HarnessPreset` carries a `create` closure; the Rust
  `HarnessDescriptor` carries metadata only, with `template`/`live` retained as data so a scripted
  row restored from a TS document is not silently promoted to a live harness (`persistence.ts:107-
  110`). The scripted-upgrade prose rule is still ported (`registry.ts:1205-1217`), because it is a
  restore-path rule, not a factory rule.
- **The harness name pattern is TS's `^[A-Za-z][A-Za-z0-9_-]{0,63}$`**, not the brief's
  `^[a-z][a-z0-9_-]*$`: the refusal sentence the registry prints names a 64-character ceiling and
  both cases, and the message and the rule have to agree.
- **`demo`/`flaky` are not shipped** - as agents and as presets. The catalogue has six entries, so
  the "Known agents: ..." refusal lists six names instead of TS's eight, and a saved row that names a
  fixture is skipped on restore (a built-in row this build does not ship) instead of being created.
- **`get` is direct, `find`/`resolve` follow an alias** - the TS distinction at
  `registry.ts:1118`/`:1270` ("`PATCH/DELETE /harnesses/writer` must never rewrite `instruct`"), kept
  because the write routes need the direct form.
- **`cards()` has no hub, so `definition_revision` is 0.** The route stamps real counters through
  `cards_with(hub)` (or `card(name, hub)`), which is what `api.ts:1505-1508` does per page.
- **`environment_tools` are all advertised.** `descriptor.ts:69-95` filters them against the runtime
  tool catalogue, which this stage has no dependency on, so `registered_tools()` reports every
  declared environment tool and the catalogue stage narrows the list before building a card. With a
  configured host that is exactly what TS computes (`presets.test.ts:479-492`).
- **`from_records` is `restoreHarnessRegistry`**: tombstones first, then per-row replay with
  `presetRecordEquals` skipping (absent and empty maps are the same fact), the
  `LEGACY_PRESET_DEFINITIONS` refresh (ported verbatim, incl. its three entries), and executable
  fields replayed only for non-scripted rows. A user row is re-created through `create`, which means
  a saved row naming a RETIRED agent still fails with the 409 - TS's behaviour at `:865-875`.
- **`AgentCatalogue` is catalogue + overlay + projections in one owner** (`src/agents/index.ts` +
  `harnesses/agent-state.ts` + `api-reads.ts:1174-1222`). The subtle rules that had to survive the
  merge: an override equal to the shipped value is DROPPED (`agent-state.ts:274-290`), so
  `records()` stays a decision list; `retired` removes a profile from `selectable()`/`effective()`
  but never from `shipped()`/`names()`; `apply_records` SETS entries without clearing (the
  `createAgentState(initialRecords)` loop, `:166-177`).
- **Two 404 wordings, on purpose.** `delete` answers `Agent "x" is already retired.` (the `retire`
  rule, `:303-305`) while `update` answers `Agent "x" is retired; it is no longer part of the
  selectable catalogue.` (the `patch` rule, `:248-253`). The api route pre-checks both with
  `Unknown agent "x".` (`api.ts:1714`, `:1760`); a route that wants that wording for a retired id
  checks `effective()` itself, exactly as TS does.
- **`update` returns `referenced_by: []`** and `delete` returns `restored_by_restart: false`: the
  first is the registry's knowledge (`api.ts:1659-1663`), the second is the deployment's
  (`restored_by_restart: databaseRoot === undefined`, `:1783`).
- **The instruction ladder lives in `registry`** (`instruction_index` / `instruction_text`), matching
  where TS puts the composition (`instructionSummaryOf`/`instructionTextOf`) and where it puts the
  membership 404 (`api.ts:1647-1649`) - the membership refusal was pulled down into
  `instruction_text` so a non-HTTP caller cannot skip it. `chars` counts `chars().count()`; TS counts
  UTF-16 code units, which differ only for astral text.
- **Selectability has two gates** (`requireAgent`, `registry.ts:329-353`): unknown id -> 400 listing
  the known names, retired id -> 409 - and a PATCH may keep a member the entry ALREADY selects
  (`:1128-1133`), which is what keeps a saved row restorable after a retirement.

### Read models and the hub

- **`read::provider_defaults` delegates to `config::provider_defaults`** (T1 already owned that
  rule), so the "is a credential present" test has one owner.
- **`/config`**: `runtime` is always present, `writable_fields` is exactly
  `["settings.base_url","settings.model","settings.api_key"]`, and `revision` is
  `hub.revision("config", None)` - read, not bumped. The route owns the advance (TS
  `api-events.ts:302-306`): bump `config`, publish, then call this builder, and the document it
  returns carries the revision the bump produced.
- **`ToolDetail.execution` / `SessionToolDetail.execution` are the closed `{scope, manual}`
  object** (`harness-api.ts:660-664`, `:1051-1054`, `:1038-1044`): `contract::ToolExecutionScope`
  is a struct over `ToolExecutionScopeKind` + `manual`, so the wire prints
  `{"execution":{"scope":"session_required","manual":false}}` and never the bare
  `"execution":"global"` string the T1 enum used to print. `read::execution_scope_of` fills both
  halves from the catalogue entry exactly like `toolDetailOf` (`api-reads.ts:1158-1162`):
  `session_bound` picks the scope, the entry's own `manual` flag is the second half (the TS
  `entry?.tool !== undefined`). The session surface states `scope` from the protected-tool test
  and `manual: true` (`api-reads.ts:900-905`), which the session/tool stage builds here.
- **`HarnessCard.subagents` is always serialized** (`harness-api.ts:428`): an empty array is the
  statement "this harness registers no `delegate`", so `skip_serializing_if` is off while
  `#[serde(default)]` stays for tolerant input.
- **`contract::SERVICE_NAME` is `noobscript-harness`** (`api.ts:1307`), i.e. the root document
  advertises the published service name, not this crate's. `api_root` (`/v1/harness`) and
  `api_version` (`v1`) are unchanged.
- **`_hub` on `tool_summary_of` / `agent_summary_of`**: TS takes no hub for either row (a page stamps
  the revision, `api.ts:1672-1675`); the parameter is kept for signature symmetry with the brief.
- **`serde_json`'s `preserve_order` is now ON.** The T1 note about alphabetically-ordered `Value`
  maps is obsolete: an unknown field now round-trips in the document's own key order, which is what
  makes the "no rewrites on a restart" guarantee testable, and `contract::require_known_fields` now
  reports the FIRST unknown key in document order like TS's `Object.keys` does.
- **`labels` re-uses `ids::agent_code`** and adds the grammar (`^[A-Z]{2}\d+$`), the ordinal rule
  (`RR0` is not a label), and `LabelAllocator` with `reserve` (an adopted label reserves its ordinal
  without minting a new one) and `seed` (never lowers a counter). `research` -> `RH`, not `RR`: the
  transcript label keys on the agent ID (`agent-results.ts:194-201`).

### Tests

- `tests/data_plane.rs`: 13 tests over the round trip, the read gate, the atomic commit, both
  repairs, the journal, the registry, the catalogue, the instruction ladder, and the config/tool/
  label projections. Fixtures live in `Temp/typecrate-harness/data-plane/<pid>-<n>-<test>/` and are
  removed on `Drop`, so a parallel `cargo test` cannot share a directory.
- `tests/foundation.rs`: 22 tests over the boot document, the wire spellings, cursors/limits/page
  slicing (including a known total), the config loader, the hub (bump ownership, report-only
  publish, ops fan-out, baseline links), the origin gates, the body validators, and the paths.

### T2.5 - foundation contract remediation (audit fixes, this stage)

Ten contract defects were fixed in the foundation files; the ones a later stage must not re-break:

1. `ToolDetail.execution` / `SessionToolDetail.execution` are `{scope, manual}` objects
   (`ToolExecutionScope` struct + `ToolExecutionScopeKind` enum).
2. `HarnessActionChoice::Option` serializes `optionId` (camelCase, recorded payload);
   `ActionChoiceInput` keeps the snake_case `option_id` (request wire type).
3. `paging::slice_page(entries, cursor, limit, total: Option<usize>)` honours a known total, and
   `resolve_limit` treats `?limit=` as absent.
4. `HarnessCard.subagents` always serializes.
5. `EventHub::publish` is report-only; callers `bump` first.
6. `HubEvent::ops` + `EventHub::publish_with_ops` carry the `display.ops` channel.
7. `LOOPBACK_HOSTNAMES` is TS's four hosts, and the stream gate allows `https://*.github.io`.
8. `ResourceRef.id` / `RevisionRecord.id` print `null` instead of being omitted.
9. `SERVICE_NAME` is `noobscript-harness`.
10. The epoch suffix is 12 base36 characters (`[0-9a-z]{12}`).

Still open for the later stages (deliberately NOT touched here): the WS frame models and the
stream scope tables, the three-channel `ProviderCredential` model in `config.rs` (the `/config`
handler stage decides), the journal line format, the registry name pattern, and the
`agents.json` envelope.

## T3 - the session store (`store/`: `mod`, `session`, `turns`, `commands`, `tools`, `transcript`, `threads`, `agents`, `actions`, `read`)

### What landed

`Store` (`pub type SessionStore = Store`) owns every session aggregate behind ONE
`std::sync::Mutex<StoreInner>`, with `EventHub` as the single revision/publish source.
`Store::open(config, settings, agents, runtime, executor, projector, registry, hub, task_host)`
loads the durable document once (`restore_all`) and never blocks on it.

The submit surface is `submit_task` (a slash line becomes a COMMAND turn, everything else the queue
path), and the queue path keeps TS's order exactly: attachments, empty/200k bounds, references, the
pending cap, steering, then the turn (`turns.rs:114-298`). `submit_command` settles a turn that is
TERMINAL before its own receipt returns: no lane, no pump, no model request, and the synthesized
`toolCall`/`tool` pair is what the record keeps (`turns.rs:305-565`).

Lanes are per conversation with a single-flight pump; the flag is taken synchronously under the guard
(`ThreadLane::pumping`, `spawn_pump`), the claim happens when a run starts (peek, never shift), and
`settle_turn` releases the slot, folds usage from the RECORDED events (an outcome tally loses to the
event fold when both exist), replaces the conversation's transcript from `TurnOutcome.messages`,
expires that conversation's unanswered action requests, and checkpoints before it publishes
(`turns.rs:688-1295`).

### Rules this stage re-proved

1. The mutex is never held across an `.await`; the only awaits are the executor, the action host and
   the journal's own fences.
2. Every mutation takes an undo point BEFORE it mutates (`durable_record()` / `snapshot_state()`) and
   rolls back on a failed write, so a 500 leaves the previous state readable.
3. Durability gates the receipt: a queued submit persists its backlog before the 202, and a command
   persists the turn before the first event. A terminal save is a CHECKPOINT, not a gate.
4. The credential never enters the document: `restore_session` sets `provider_key: None`, while
   `settings_provider()` (and `session_resource_of`'s `endpoint`) synthesize the deployment pair.

### Fixed while verifying (production, not test-only)

* `commands::dispatch` answered a bare `/help` with the usage card, so the command LIST was
  unreachable; `/help` now lists the registry and `/help <command>` still prints the card
  (`command-builtins.ts:20-34`, which is also what the registry's own description states).
* `transcript_page` minted `next_cursor` from the unfiltered total while serving filtered rows, so a
  role-filtered client looped forever on an empty tail; a page now ends unless it is exactly full
  (`api.ts:3605`).
* A transcript write bumped `transcript` under the BARE thread id while every transcript read
  addresses the composite `session:thread` key, and the paged read reported the SESSION counter. The
  write now bumps `transcript` and `transcript_messages` on the composite key (the two kinds TS bumps
  for one write, `api.ts:1176-1185`), and the page reports the `transcript_messages` figure.
  `Store::transcript_revision_of` therefore gained a `session_id` parameter - the only public store
  signature this stage changed.
* `create_session_tool` stamped the installed revision twice (the stamp embeds a random tail), so the
  receipt, the ledger row and the source route named three different revisions; one stamp is minted
  and reused.
* `create_thread` read only `ownerThreadId`; the contract's REST spelling is `owner_thread_id`
  (`contract::CreateSessionThreadRequest`), so both are accepted now.
* `delete_thread` removed only the named conversation, leaving its descendants pointing at a dead
  owner; the cascade is transitive now (`harness-api.ts:1447`'s `removed_descendants`).
* `delete_session_tool` reported `restored_inherited: true` whenever a session-local disable went
  away. TS states `false` unconditionally, because restoring an inherited baseline needs a second
  copy of the definition that the registration kernel does not keep (the T27 disclosure,
  `api.ts:3296-3306`).

### Deliberate simplifications (stated, not hidden)

* A created tool is a DECLARATION: it registers and enables, but this build cannot compile a body, so
  the source route is the only copy of the text and manual execution has nothing to run
  (`tools.rs:72 CREATED_TOOL_NOT_COMPILED`).
* `/websearch` and `/webfetch` answer with the tool-contract bridge note instead of running the
  agent-loop tool (`commands.rs:178-182`); the integrated runtime replaces the text in T7.
* `/task` addresses the STORE's task host, not a per-session board: the session argument is carried
  (`_state`) for the day the board becomes session-scoped, and `?thread=` narrowing on the task list
  is not implemented, so that filter answers empty.
* `Store::runtime_snapshot()` states the BUILT-IN deployment runtime with the session's provider trio
  substituted (no environment read inside a request), so `runtime_execution` is
  `DEFAULT_THREAD_EXECUTION` (`inline`) unless the operator changes it.
* Pump `JoinHandle`s are not retained: `shutting_down` plus each run's cancellation token are the
  cooperative stop, and a pump exits by itself once its lane is empty.

### Facts the transport (T4) must not rediscover

* Two "transcript revision" figures are live on purpose: the THREAD RECORD's counter is what an
  `if_revision` guard compares against (`transcript.rs:364-376`), the HUB's `transcript` counter is
  what the metadata read and the thread resource report, and the paged read reports
  `transcript_messages`. `put_transcript` moves all three except the record's own.
* `session_summary_of.thread_count` counts HOST conversations only (`api-reads.ts:175-177`), while
  `list_threads` pages the whole roster. An agent-origin row (a delegated child, or a `POST /threads`
  without `"origin": "host"`) is therefore in the roster but not in the count.
* A route that creates a conversation should pass `"origin": "host"`: the minted label continues the
  reserved per-agent-code ordinal (`labels.rs:165-186`), so an explicit `RH9` makes the next mint
  `RH10`.
* `pending_action_count` is the ENTRY count (`api-reads.ts:227` `pendingActions.size`), so an answered
  question keeps counting until it is pruned; the live list is `list_actions`'s `pending`.
* The action host mints its own `requestId` and REPLACES a submitted one (`types/index.ts:865`); the
  cap is four waiting questions per run, and the resolved promise payload is the recorded
  `HarnessActionAnswer` verbatim.
* `reset_session` refuses with its own sentence while a run or a backlog exists; the shared busy
  sentence is used by exactly two verbs (`agents.rs:174`, `threads.rs:376`), and a declarative PATCH
  lands during a run. `delete_thread` states the specific "still has live work" refusal first.
* `initialize` does not start a lane pump: the restored backlog waits for the next submit or the next
  free slot in that lane to claim it (`sessions.ts:6461-6495` starts a pump on submit, not on open).
  It IS durable, though - `record.queue` is the union of the legacy array and every lane's queue, in
  turn-index order, and `restore_session` re-homes each row into its own lane exactly as TS's F1
  distribution does (`mod.rs:1660-1682`, and the split in `restore_session`). T3 shipped this as the
  LEGACY array only, which made a per-lane backlog memory-only; T7a closed that simplification.

### Tests

`tests/store.rs` - 22 tests driving the public store surface over the real registry, catalogue and
hub with scripted `TurnExecutor` / `ToolRuntime` / `ProjectorFactory` doubles: create/read/list with
paging end-of-collection rules, every 404/400 refusal sentence, the full submit→receipt→pump→done
fold (event list, `eventTimes` parallelism, usage fold, transcript replacement, journal lines and
sequences), failure and banner clearing, lane FIFO with a durable clip backlog, per-conversation
concurrency via a rendezvous, steering (including the `null` vs absent `threadId` deviation),
cancel/reset/delete, the 100-task pending cap, the attachment matrix (415 table, 415 signature, 413
bytes, 400 marker/count/duplicate), transcripts (paged, metadata, document, raw-accepted placeholder,
`if_revision` 409 naming both figures), command turns and the command registry, the action
lifecycle (park → list → refuse malformed → answer → conflict on re-answer → cap → expiry), the
session toolbox, conversations with label reservation and descendant cascade, and the
`StreamExists` truth table. `tests/store_restore.rs` - 4 tests over a second `Store::open` on the same
root: full session/turn/transcript/label/journal restore, the durable-backlog gap above, the
interrupted-run repair written into the document by hand, and the credential rule.
Fixtures live under `Temp/typecrate-harness/{store,store-restore}/<pid>-<test>/` and are removed on
`Drop`.

## T4 - the runtime (`tools/`: `catalogue`, `result`, `files`, `command`, `syntax`, `text`, `web`, `mod`; `transport`, `turn`, `projector`)

### What landed

`src/tools/` is the catalogue AND the executable half of it: `catalogue.rs` (506 lines, generated),
`result.rs` (725 - the model result budget, the `__toolUi`/`__toolMedia` unwrap, and the three
outcome builders), `files.rs` (2 269 - `read`/`write`/`edit`/`fileList`/`glob`/`grep`), `command.rs`
(335 - `shell`/`gitDiff` through the platform shell), `syntax.rs` (294), `text.rs` (439 - the five
app tools), `web.rs` (818 - `fetch`/`search`/`download`), `mod.rs` (1 045 - `ToolRegistry`, the one
dispatch path, and the two discovery tools). `src/transport.rs` (1 090) is the OpenAI-compatible
client and the SSE fold; `src/turn.rs` (727) is `runAgentCore` plus `core/agent.ts`'s retry policy;
`src/projector.rs` (2 048) is `server/turn-projector.ts`'s display fold. `Cargo.toml` gained exactly
one build fact: tokio's `process` feature, because `shell`/`gitDiff` spawn the platform shell and a
blocking `std::process` would park the turn's task while a build runs.

### The catalogue is generated, not typed

`catalogue.rs` is written by `Temp/typecrate-harness/t4/generate-catalogue.mjs` from
`packages/underload/harness/src/core/tool-manifest.ts` (the AST scan of `src/tools/**/*.tool.ts`)
plus the `metadata` literals of `src/server/tools/shared/*.tool.ts`. Regenerate instead of editing: a
hand-copied description or schema drifts silently, and `/tools` is a document the model quotes back.
35 rows in `GET /tools` order (core, presets, app tools, then `search`/`fetch`), never sorted by
name. The deviations from the TS table are stated in the generated header, and the two that matter
for a diff are: TS lists only the 14 core descriptors whose `listed !== false` (`toolCancel`,
`threadSend`, `threadRead`, `threadList` are `listed: false` and therefore absent there), and the
`search`/`fetch` rows follow the SERVER's "not session-bound" reading rather than the descriptor
table's. `execution` is `{scope: session_required | global, manual: true}` per the stage contract;
TS spells the session scope `session_only`, which is the same gate under the contract's enum.
`testFlaky` (the example harness) is not part of the shared catalogue and is not listed.

### Executable here, catalogued only

21 of the 35 rows run: the six file tools, `shell`, `gitDiff`, `syntax`, `toolList`, `toolRequest`,
`fetch`, `search`, `download`, `task` (through the session's task host), `actionRequest` (through
the action host), and the five app tools. The other 14 are the runtime-compiler and thread family
(`toolRegistry`, `toolManage`, `toolCreate`, `profileManage`, `chapterManage`, `locationManage`,
`agentManage`, `agentResult`, `delegate`, `threadFork`, `toolCancel`, `threadSend`, `threadRead`,
`threadList`): each one mutates or reads live per-session state that the `ToolRuntime` seam does not
reach. They answer `Tool "{name}" failed: this server does not support {capability}, so "{name}"
cannot run here. The capability is on the port list; use the tools that are executable to make
progress.` - a well-formed FAILED outcome with a measured span, never a panic and never a fake
success. `MISSING_CAPABILITY` (`mod.rs:77`) is the list and the capability text in one table.

### Fixed while verifying (production, not test-only)

The integration tests were written against the TS tool bodies (`src/tools/**/*.tool.ts`), not against
the Rust output, and that found eight real divergences. All are fixed; the list is here because each
one is a rule a later stage might otherwise re-break:

* `turn.rs` seeded its request history from `Vec::new()`, so every turn answered as if it were the
  first and the transcript it returned dropped the conversation the store had handed in. `TurnInput`
  IS the conversation; the prompt is appended to it (`run_turn`, and `tests/turn.rs::a_recorded_transcript_continues_the_conversation`).
* `projector.rs` and `turn.rs` each re-derived `add_usage` with a two-arm `cached` match, which
  silently dropped a first frame's cached figure forever. Both now call the store's canonical
  `store::turns::add_usage`, whose `(None, next)` arm keeps it.
* `projector.rs` routed a conversation-root thread (and a threadless start) to a `subagent` row,
  because only a `ROOT_KEY` section is the primary flow (`turn-display.ts:896`). A thread record with
  no parent/call/kind/branch, and the first flow any fold sees, now mint the root row.
* `transport.rs` appended custom headers instead of replacing the generated ones; TS composes
  `{...generated, ...custom}`, so an `authorization` override was being sent twice. The merge now
  runs through a map and writes each name once.
* `fileList` echoed `limit` without slicing the page, ignored the `paths` property's rules, accepted
  an explicit `[]` (which reads as "the directory is empty"), and prefixed entries with the raw root
  so `./src` produced `./src/a.ts` and `src/` produced `src//a.ts`. TS `join()`s, which collapses
  both. Unknown keys are now refused, because `{paths: [...], offest: 200}` would otherwise return
  page 1 forever while the caller believed it was paging.
* `write` accepted the same target twice in one batch - the second write silently winning - and
  skipped the NUL rules (`write.tool.ts:136-168`).
* `glob`/`grep` presented a FILE root as `long.txt/long.txt` and skipped the root-error sentences:
  a typo'd root now answers `glob path "x" does not exist. Roots are resolved against the working
  directory (...)` instead of reporting an empty listing, and an empty `paths`/`patterns` array is
  refused rather than defaulted (`search.ts:532-568`). The flag vocabulary is JS's `dgimsuyv` with
  TS's own sentence.
* `edit` reported opencode's wordings instead of this package's, named the RAW path instead of the
  resolved one, omitted the match line numbers from the ambiguity error, and had no line-ending
  awareness at all: it now matches against the LF view (so the documented habit of writing
  `oldString` with `\n` in a CRLF file works), re-joins the replacement with the matched region's
  endings, applies the endings-only special case verbatim, and refuses a no-op replacement.
* `toolRequest` ranked matches in catalogue order; TS ranks by each row's strongest criterion -
  exact names in the CALLER's order, then a name regex, then tag-only - and a row the run already
  has never consumes a slot in the page (`discovery.ts:373-378`).

### Deliberate simplifications (stated, not hidden)

* `edit` ports TS's FIRST matching tier only (the LF view). The typographic-quote tier and the
  trailing-whitespace tier of `edit.tool.ts:20-33` are absent, so the optional `match` field never
  appears and a near-miss is a refusal the model can re-copy. TS also aligns replacement lines to
  region lines with a longest-common-subsequence pass (`alignLines`); this port joins every
  replacement line with the region's dominant ending, which differs only inside a hunk that mixes
  CRLF and LF.
* `glob`/`grep` read no `.gitignore` (`files/gitignore.ts`) and dedupe displayed paths rather than
  `realpath` file identity (`files/identity.ts`). The counters that would carry an ignore result are
  reported as honest zeros with `ignoreFilesParsed: 0`.
* `grep` compiles with the `regex` crate. JS-only constructs (lookaround, backreferences) REFUSE with
  the regex crate's message rather than matching; Rust-only inline flags (`(?i)`) therefore MATCH
  where JS reports "Invalid group"; and the invalid-pattern sentence is
  `grep could not compile pattern "{src}": {regex crate message}` rather than V8's
  `Invalid regular expression: /{src}/: Invalid group`.
* `textSummarize` always takes the documented local condense fallback: there is no agent-runner seam
  in the `ToolRuntime` contract yet, so the `Options` field that would carry one is present and
  unused (`tools/mod.rs:201-207`).
* Tool calls in one message run SEQUENTIALLY, in the order the model asked. TS launches a message's
  calls through its scheduler concurrently; sequential execution keeps file side effects in the
  requested order and keeps the event order deterministic for the projector.
* `transport` emits at most one `usage` event per REQUEST (from the response the request settled
  with), matching `harness.ts:6656`, because the store folds every event it sees. The per-chunk
  `StreamEvent::Usage` exists for callers that want it and is deliberately not forwarded by `turn`.

### The projector seam's limits (T5 must merge these)

`ProjectorFactory::create()` receives NO turn record, so the fold states only what the events prove.
Every op therefore carries `turnId: ""`, sections are `s{ordinal}` (never the store's section ids),
`summary()` states `id: ""`, `index: 0` and `prompt: ""`, there are no `context:*` ops (`blocks: []`),
no `pending:set` (this build has no background scheduler), and `settle()` MINTS no ops because the
seam returns `()` - the store re-reads `summary()`/`display()` immediately after, which is how the
same state becomes observable. `TurnInput` carries no agent name either, so a Rust turn reports the
agent `primary` (`turn.rs:62` `PRIMARY_AGENT`) and the exhaustion sentence names it. T5's job at the
read-model boundary is the merge the TS projector did with its record in hand: rename `prompt` to
`promptPreview`/`promptChars`, stamp `id`/`index`/`sectionCount` from `SessionTurn`, and attach the
labels the invocation join provides.

### Facts the HTTP stage (T5) must not rediscover

* The public surface is `tools::ToolRegistry::{new, with_options}`, `transport::OpenAiClient`,
  `turn::AgentExecutor::new` and `projector::TurnProjectorFactory::new` - each returns the `Arc` the
  store's `Store::open` expects, and each is `Send + Sync`.
* A `ToolRegistry` resolves availability WHEN BUILT (`Options::default()` reads the environment), so
  a host that loads `.env` later must build the registry afterwards, or it advertises the wrong state.
  The credential VALUES never reach a wire field: `/tools` names environment variables only
  (`tests/tools.rs::a_keyless_host_reports_search_unconfigured_and_never_leaks_a_credential`).
* `ToolExecCtx::cwd` is the run's working directory, and the file tools are UNSANDBOXED: relative
  paths resolve against it, absolute paths are used as given. Results are posix-styled; an absolute
  path is echoed the way the platform spells it, exactly like `path.resolve()`.
* The three outcome classes are distinguishable and the difference is the contract: a REJECTED call
  (bad arguments, unknown tool) reports `duration_ms: 0` because it never executed, a FAILED call
  reports the measured span and puts the same sentence in `content` and `error`, and a COMPLETED call
  puts the budgeted result in `content` with `panels`/`media` peeled off their brands
  (`result.rs:354-420`, `harness.ts:5031-5107`).
* An over-budget tool result becomes the named envelope `{truncated, originalBytes, budgetBytes,
  content}` - the FIRST geometric fit, byte-exact - and that envelope is what the model reads
  (`result::bound_tool_result`; `tests/tools.rs::an_oversized_result_becomes_the_named_budget_envelope`).
* `TurnInput::transcript` is the conversation and `system_instruction` is projected on EVERY request
  and never stored; a transcript that already leads with a system entry is left alone
  (`turn::Run::request_messages`). `max_iterations: 0` means "use the library default"
  (`u64::MAX` — no limit, since the count guard is an optional safeguard), the
  steering receiver must be drained to empty when the run settles, and a run that does not succeed
  returns the ORIGINAL transcript because the store REPLACES the conversation with the result.
* The projector must be folded exactly once per recorded event, in order, and settled once at the
  terminal status; a restored turn rebuilds by folding every event, which is what makes
  live == replay an assertion rather than a hope (`tests/projector.rs::live_folding_equals_a_cold_replay_of_the_same_events`).

### Tests

`tests/tools.rs` - 24 tests: the `/tools` document (row count, group order, visibility table, tags,
the session hint, the identified read's schema and `{scope, manual}` gate, the credential-name-only
availability rule), every executable tool's happy path and refusal sentences against real fixture
files, the three outcome classes, the result budget end to end, `shell`/`gitDiff` against a nested
`git init` fixture, the discovery pair including TS's ranking, the task/action pass-throughs, and the
catalogued-only refusal for all 14. `tests/transport.rs` - 23 tests over a local axum listener: the
request document and headers, the streamed fold (arrival order, tool-call merge by `index`, usage
normalisation and clamping, `[DONE]`), every failure sentence (`HTTP {status}: {body}`, empty error
body, `Malformed stream frame: {200 chars}`, a gateway error on a 200, `Truncated`, an aborted
request, an unreachable host), and the wire transcript (part order around markers, the two
cannot-represent-it sentences, the audio formats, tool-media provenance, the 4+1 grouping with
`(part N of M)`, marker defanging, id re-suffixing). `tests/turn.rs` - 13 tests over the same local
listener plus a scripted `ToolRuntime`: the exact frame sequence for a text answer, a tool round, a
retry (`attempt`, `maxAttempts: 11`, `delayMs: 1_000`), an exhausted tool loop, a cancelled run, a
steer queued before and during a run, a malformed argument string, a failing tool, request-identity
across rounds, and the minted invocation uuid. `tests/projector.rs` - 15 tests: the state
vocabularies, the markdown flattening and the head/tail preview rules (including the UTF-16 counting
that keeps a surrogate pair whole), the queued-row baseline, the root flow's insert/append stream,
the retry's discard, one step row per call, the abandoned-call rows `settle` writes, the usage
two-line publish, the error notice that never takes the tail, and live == replay. Fixtures live under
`Temp/typecrate-harness/t4/<pid>-<test>/` and are removed on `Drop`; no test in this stage touches a
real provider or the network.

## T5a - the HTTP surface (`runtime`, `api/**`, `server`, `bin/server`)

### What landed

* `src/runtime.rs` (565 lines) - `HarnessRuntime` and the per-root cache. The key is the
  CANONICAL root (`std::fs::canonicalize`, lexical fallback) with the `\\?\\` prefix stripped by
  `cache_key`, so `./x`, `x`, and `C:\\abs\\x` reach one runtime - the rule
  `src/server/runtime.ts:299-308` states. The cache lock is held ACROSS the build, which is what
  stops two concurrent first requests from constructing two registries for one root. A runtime
  holds the AUTHORITATIVE `Arc<RwLock<..>>` registry / agents / settings copies; the store keeps
  immutable boot snapshots (`StoreInner` holds `agents` / `registry` as bare `Arc`s with no writer-side
    accessor, `store/mod.rs:846-850`), so
  `save()` fills `registry` / `deleted` / `globalSettings` into `store.database_of(&inner)` and
  hands the result to `persistence::write_harness_database` - TS's two-file unit
  (`sessions.ts:2675-2691`: `state.json` first, then the `agents.json` overlay).
* `src/api/mod.rs` - the one composition every handler shares: `Ctx` (runtime + method + pathname +
  parsed query + captured params + headers + a `BodyReader`), `ApiResponse` / `ApiResult`,
  `body_limit_for`, `requested_mount`, `mount_segments`, `match_route`, `ROUTES`, `Operation`,
  `dispatch_operation`, `handle_request`, `handle_stream`, `write_response` / `write_error`.
* `src/api/{root,config,harnesses,agents,tools,models}.rs` - every host route in
  `api.ts:1292-1950`: the boot document, health, `GET/PATCH /config` with the credential channels,
  the definition catalogue (list + `q`, card, instructions index and text), harness CRUD with the
  name pattern and the primary-reference refusal, the agent catalogue with the global overlay,
  the tool catalogue with both filters and the manual gate, and model discovery with the retry
  policy and the scoped credential.
* `src/server.rs` + `src/bin/server.rs` - `router(database_root)` (the mount, the stream path, the
  `{*rest}` half, and a fallback that hands a non-mount path to the SAME dispatcher so the answer
  is the contract's mount sentence), `serve` / `serve_on` with graceful shutdown, and `flush`
  which writes the final durable document on the way out (`start.ts:328-340`'s dispose step). The
  executable takes `--host` / `--port` / `--database-root` / `--help`, defaults to
  `127.0.0.1:5181`, exits 2 on a usage error, and stops on SIGINT/SIGTERM.

### The dispatcher order is the contract

`api.ts:3980-4064` + `service.ts:553-556`, and each step exists on purpose: origin gate (before
any runtime work) -> per-root runtime -> mount guard -> route match -> verb -> handler, where the
body is read under the route's cap ONLY if the handler asks for it. A wrong verb and an unknown id
therefore never parse a body (`api.ts:4009`, `:4022-4024`).

The route table is `pub static ROUTES: [Route; 45]` - one id, the verbatim TS template, and the
operations in TS declaration order - paired with an `Operation` enum and ONE match in
`dispatch_operation`. It maps to operations rather than fn pointers because an async fn's future
cannot live in a `static`; the table stays data and the dispatcher stays the only place a handler
is named. `tests/routes.rs` is the pin: the 45 `(id, template)` pairs in table order copied from
`shared/harness-api.ts:54-100`, the 64 operations, the verb split (GET 37, POST 13, PATCH 7,
DELETE 6, PUT 1), one reachable mount per operation, and the parameter-grammar/mount-prefix rules
`mount_segments` + `match_route` rely on.

### Deliberate deviations (stated, not hidden)

1. **415 for a non-JSON media type.** TS's adapters never inspect the request `content-type` -
   node hands the handler an already-parsed body and the gateway parses before dispatch - so there
   is no TS sentence to copy. The contract documents 415 for a request body, and this port enforces
   it in the same voice as its neighbours: `A request body must be application/json (content-type
   "..." is not supported).` An EMPTY body is `{}` whatever the header says, which keeps TS's
   empty-chunks arm (`handler.ts:120-123`) unchanged.
2. **`X-Harness-Epoch` on every JSON response, errors included.** TS stamps the contract headers
   in `json()` and returns a bare result from its catch arm, so an error leaves without one. The
   value is the process epoch either way (`EventHub::new()` takes `events::process_epoch()`), so
   stamping it cannot make a client believe anything a success would not.
3. **The body drain grace.** `BODY_DRAIN_GRACE_BYTES = 32 MiB` (`api/mod.rs:289`). Hyper ends the
   connection the moment a handler answers with the request body still arriving, and a client
   mid-write reads `ConnectionReset` instead of the envelope. TypeScript cancels the reader and lets
   node absorb it (`service.ts:378-381`, "stop the transfer rather than drain a body nobody will
   read"). The equivalent here is to keep reading and throw the bytes away, bounded: every body the
   contract ALLOWS fits inside the grace, so a compliant client always receives its 413 / 415 / 400
   / 405, and only an unbounded upload is dropped - which is what TS's cancel does to it.
4. **`chars` is a UTF-16 length in the files this stage owns.** `String.length` in TS counts UTF-16
   code units; `projector.rs:98` already counts that way. `api/harnesses.rs:399` now matches.
   FOLLOW-UP for the owners of the other two producers: `registry/mod.rs:776-778`
   (`instruction_summary_of`) and `store/agents.rs:123` still use `chars().count()`. The two
   spellings differ only for astral text, and the difference is invisible in every shipped
   instruction, but one shared helper is the right end state. CLOSED by T5b: both sites now call
   `projector::display_chars`, and the astral case is pinned through the instruction ladder in
   `tests/http_sessions.rs`.
5. **The provider display URL is `URL.origin + URL.pathname`** (`transport/models.ts:97`), including
   WHATWG's literal `"null"` origin for `file:` - so a `file:///tmp/provider` base URL answers
   `"null/tmp/provider" must use http or https to list models.`, and the request-failure sentence
   quotes the RESOLVED `{base}/models` endpoint (`:189`, `:211`), not the caller's base.
   `origin::parse_url` is the origin gate's reader and it requires a host to compare, so
   `api/models.rs::originless_url` recognises the empty-authority shape locally rather than widening
   a T1 file.
6. **A failed runtime creation is not cached.** `src/server/runtime.ts:300` keeps the rejected
   promise, so TS replays one failure forever; this port retries. A transient open failure (a lock,
   a half-written document) should not poison a root for the life of the process.
7. **No `axum-test` dev-dependency.** 4.x needs `tokio ^1.44` and `hyper-util`, and this repo pins
   both `tokio` and `hyper` to workspace mirrors. The suites bind a real `axum::serve` listener
   instead, which is what makes `tests/http_host.rs` evidence about the executable's own accept
   path.

### Fixed while verifying (production, not test-only)

* `api/tools.rs` - the availability filter compared with `is_none_or`, which let a row with NO
  availability block match BOTH filters, so `?availability=unconfigured` returned rows that report
  no state at all. TS's rule is `entry?.availability?.state ?? 'available'` (`api.ts:1826`): the
  `??` half is what `availability_of_row` now implements, and such a row belongs on the `available`
  page only.
* `api/tools.rs` - the session refusal keyed on the projected `execution.scope` instead of the
  entry flag TS reads (`api.ts:1871`, `entry.sessionBound === true`). They agree for all 35 rows
  because `toolDetailOf` derives the scope from that flag (`api-reads.ts:1211`), but the flag is
  the contract, so the gate reads the flag.
* `api/agents.rs` - serde maps a JSON `null` and an ABSENT key to the same outer `None` on an
  `Option<Option<T>>` (probed against this exact shape), which made TS's
  `hasOwnProperty ? value ?? null : ...` restore arm unreachable: `{"instruction": null}` answered
  `A patch must change at least one field.` Presence now comes from the body's own keys and
  `restores()` re-attaches the `Some(None)` arm AFTER the decode, so the type validation and its
  error wording are untouched.
* `api/sessions.rs` and the eight sibling session stubs - a stub that answers without touching the
  stream leaves the client's upload half-written, so the 500 arrived as a reset. Each stub now
  consumes the body its verb documents (`require_empty_body` / `body_object` / `body_value`), which
  keeps the cap and media-type rules live on those routes; `POST {root}/sessions/{id}/tasks` keeps
  its 16 MiB boundary proof, and `api/mod.rs` drains on the 405 arm for the same transport reason.
* `api/models.rs` - the resolved provider URL DROPPED the base URL's query. `modelsUrl` assigns the
  pathname only (`transport/models.ts:129-131`), so the search survives into `url.href`; a base URL
  addressed as `{base}/v1?tag=live` lost its selector here. `models_url` now appends `url.search`,
  and the mock provider records each request target so the rule is asserted from the provider's side
  (`tests/http_host.rs::model_discovery_reads_and_filters_the_provider_list`).
* `store/mod.rs` (T2's file, touched deliberately and reported) - the shared vendor-registry open
  budget was 24 attempts at 5-25 ms, roughly 0.6 s, and the new suites' parallel `Store::open` load
  exceeded it: `Store open failed: the shared vendor registry is not yet available (transient)`.
  The budget is now 40 attempts at 20-60 ms. The error contract is unchanged; the wait is longer.
  If a later stage wants a principled fix, the registry should be opened once per process and
  shared, rather than retried per test.

### Facts the session-domain and stream stages must not rediscover

* **Host-kind counters collapse.** `events.rs:42` `HOST_RESOURCE_KINDS = [harnesses, agents,
  tools, config]` and `normalize_id` drops the id for them (`api-events.ts:99-105`), so
  `bump(kind)` + `bump(kind, id)` in ONE write move a single counter by two, and the header quotes
  the second value. The agent test therefore sees 2, then 4, then 6 - and TS's `api.ts:1763-1764`
  produces the same three numbers for the same reason.
* **Credential state is three channels, not one** (`api.ts:795-838`, `:1368-1374`, `:1424-1446`):
  the boot pair, the live key + its claimed pair, and the provider the live key belongs to.
  `advertised_has_api_key()` is `!api_key.is_empty() || (has_key && !declares_credential)`, and a
  PATCH that nulls a field falls back to the BOOT arm, not the live one.
  `api/models.rs::scoped_provider_settings` implements TS's third `credential` argument as two
  `merge_settings` passes, because `config::merge_settings` takes only defaults + input.
* **The route table is final data.** A new operation is one `Operation` variant plus one
  `dispatch_operation` arm; nothing else in `api/mod.rs` changes. Session handlers swap by editing
  their own module.
* `runtime::evict_runtime(root)` is the per-root reset (the restart tests use it);
  `clear_runtime_cache()` resets EVERY root and therefore every hub's counters, which is why a test
  that needs a fresh instance for one root must not call it.
* `handle_stream` currently answers the TS `api.ts:3911` sentence - `GET /stream expects a WebSocket
  upgrade (Sec-WebSocket-Key/Version) for the harness.v1 subprotocol.` - which is what
  `queue.test.ts:1090` and `view.test.ts:779` pin. T5c replaces the body of that function; the
  origin gate (`check_stream_origin`, including `https://*.github.io`) is already the stream's.

### Tests

`tests/http_host.rs` - 17 tests over a real `axum::serve` listener on `127.0.0.1:0` with pooling
disabled, one fresh database root per test under `Temp/typecrate-harness/http-host/<pid>-<test>/`:
the boot document's full capability surface, health and the live session count, the legacy-prefix
404 that names `/v1/harness`, the unknown-route 404 plus the stream's 400, the config document and
PATCH (every writable field, the null-restores-boot rule, all 18 refusal arms, the credential
channels, and a restart that proves the pair is durable), the definition catalogue (`q` matching
lowercased `name title description`, the alias-resolved read, the instructions index and member
text), harness CRUD (the name pattern, the duplicate refusal, the primary-reference 422, `harden`,
and a restart that shows an in-memory-only session is gone while the registry rows are not), the
agent surface (catalogue rows, the `referenced_by` field, patch/restore/retire with the collapsed
counter sequence, and `agents.json` holding ONLY the operator's decisions), the tool catalogue
(35 rows in catalogue order, both filters, the identified read's `{scope, manual}` gate, the manual
execution pair including the `ToolCallResult` discriminant), model discovery against a local mock
provider (parse and dedupe, the credential scoping rules, the ten-attempt retry budget, every
failure class verbatim, and the body policy), the body policy itself (the 64 KiB cap, the 16 MiB
submit boundary in both directions, the 415, the malformed-JSON 400, and the two laziness proofs),
the origin gate's whole policy table, and strict paging (`limit` refusals, the four cursor
refusals, and the exact `limit` echo of a short page).

## T5b - the session domain (`api/{sessions,actions,queue,usage,threads,transcript,turns,session_agents,session_tools}`)

### What landed

* The nine session files, all 64 declared operations, no stub body left: session CRUD + the
  definition / usage / commands / queue / tasks / reset / cancel rail, the owner-question pair, the
  conversation roster with its transcript trio, the turn surface (page, identified read, display tree,
  event page, step, block, attachment), the session agent roster with the instruction subresource, and
  the session toolbox (page, create, detail, source, patch, delete, manual call).
* `api/sessions.rs` also carries the helpers every sibling uses, because the same three problems recur
  on every session route: `session_of` / `require_session` / `require_thread` / `require_turn` (the
  contract's ownership sentences - the store's own 404s name only the id, so the route wraps them with
  `api::missing_in` to print `Unknown thread "t" in session "s".`), `thread_key` (the hub addresses a
  conversation as `session:thread`), `number_of` / `page_revision` (read the figure the BODY reports
  and stamp the header with it, which is the only race-free way to satisfy "one counter per
  comparison"), `stamped` / `plain` (a response with a revision header and one without), and
  `empty_patch` (TS's `bodyObject` plus the "must change at least one field" arm).
* `store/read.rs` - the projector / record merge, at the narrowest point that touches neither the
  projector nor the turn record: `TurnState::row()` and `TurnState::display()` now stamp the record's
  identity and bounded text onto the fold (`id`, `index`, `threadId`, `threadLabel`, `startedAt`,
  `promptPreview` + `promptChars`, `attachments`, `references`, `steers`), stamp `turnId` on every
  display node, and rename the projector's per-turn section ordinals (`s0`) to the wire ids the
  contract prints (`{turnId}:0`), rewriting `parentSectionId` and each step's `sectionId` to match.
  `promptChars` / `content_chars` count UTF-16 units through `projector::display_chars`, and the two
  `chars()` sites T5a listed as follow-ups (`registry/mod.rs:776`, `store/agents.rs:123`) now use the
  same helper, so the "one shared helper" end state T5a asked for exists.
* `api/mod.rs` - three seam edits, each reported: (1) the body is drained AFTER the handler answers, so
  a handler that refuses early cannot leave a 16 MiB upload half-read; (2) the declared-length 413 moved
  BEFORE the handler using `body_limit_for` plus a new `declared_length`, which is what makes
  `POST {root}/sessions/{id}/tasks` reject an over-budget submit against an UNKNOWN session - the order
  `service.ts:502-506` proves (`api.ts:503-509`'s cap check runs before the route's id lookup); (3)
  `assert_dispatch_future_is_send` documents the `Ctx` rule below as a compile-time probe.

### The wire contract, not the TypeScript response object

`src/contract.rs` (generated from `shared/harness-api.ts`) is what this port serialises, and it is
NARROWER than what `src/server/api.ts` builds in the TypeScript rail. The five differences a session
stage must not re-derive:

1. **The mounted surface is 45 templates / 64 operations.** `api.ts` also answers a bare
   `{root}/session`, a `{root}/sessions/{id}/toolbox`, and the instruction ladder reads
   (`/agents/{a}/instructions`, `/agents/{a}/prompt`); `POST {root}/sessions/{id}/actions` and
   `DELETE /agents/{a}/instruction` are not contract operations either. The generated table wins, and
   `tests/http_sessions.rs` pins the `Unknown API route: GET ...` / `Use GET, PATCH for ...` sentences
   so nobody later mistakes them for a handler that was never written.
2. **`SessionResource` is a `SessionSummary` plus eight members.** No `recentTurns`, no `threads`, no
   `projection`, no `settings` block. Its `endpoint` member is `ProviderDefaults` - the DEPLOYMENT's
   key-free defaults - not the session's own `settings`, which is why the create response does not echo
   the base URL it was given while the run still uses it.
3. **Two spellings in one API.** A turn PAGE answers camelCase display rows (`threadLabel`,
   `promptPreview`, `sectionCount`, `stepCount`, `activeAgents`, `latestInput`, `detail`) and the
   identified turn read answers the snake_case `SessionTurnDetail` (`event_count`, `attachment_count`,
   `steer_count`). `TaskReceipt`, `QueuedTask` and `SessionAgentBinding` are snake_case;
   `TurnSummary`, `TurnDisplay*` and the step detail are camelCase. Both are pinned per route.
4. **Usage is a fold summary** (`{session_id, turn_count, reported_turns, usage?, by_thread, truncated,
   revision}`) and there is no per-record `records` member on this route. `usage` is ABSENT (not zeroed)
   for a session nothing was billed for, and `by_thread` is keyed by the conversation that ran.
5. **Transcript metadata is size and role counts only** - no `turns` array; the messages page reports
   `total` for the WHOLE transcript regardless of the `role` filter; the document response has no
   `links` member. `GET /transcript` is metadata, `/transcript/messages` is the page, and
   `/transcript/document` is the raw text - three reads, three different revisions-figures (T3's list).

### Counters: what actually moves

* A mutation that lists its own bump targets AND builds its hub event from the same list bumps TWICE:
  `CommitOutcome::with_event` pushes every event target into `bumps` (`store/mod.rs:274-278`) and
  `Store::commit` then bumps each entry once (`store/mod.rs:1109-1116`). That is why one declared session
  patch moves `session` by two (1 -> 3 -> 5) and a thread create moves `threads` by two, while a roster
  write (which does not double-list) moves `session_agents` by exactly one. `api.ts:2079-2082` bumps two
  DIFFERENT kinds where this store bumps one key twice; what the contract pins - and what both suites
  assert - is that the header and the body's `revision` member are the same number.
* `submit_queued` publishes without bumping, so `TaskReceipt.session_revision` is the un-bumped figure
  and the 202 header equals it.
* The roster list, the instruction read, the instruction PATCH and the tool source read stamp NO header
  at all (`plain`), because their bodies already state the figure or the contract declares none.

### Behaviour the store owns and the route reports

* A lane's FIRST submit answers `status: queued` with `queue_position: 1` (`tests/store.rs:1108`); a
  TEXT submit addressed at a RUNNING conversation STEERS (`steered: true`, the live `turn_id`, no
  ordinal, no new row); only a submit carrying attachments or references falls through to the lane
  backlog (`store/turns.rs:143-159`), which is the only way an ordinal above 1 is observable over HTTP.
* An accepted cancel lands the session `error`: `SessionStatus` is `idle | running | error`
  (`contract.rs:790`), so "interrupted" has no other spelling. A run parked on an owner question is
  still steerable, and steering does not un-park it.
* A slash command is a TERMINAL turn: `202 status: done`, `thread_id: ""` with `links.thread` falling
  back to `/turns` (`api.ts:2259-2263`), and events `[start, tool_call, tool_result]` under the agent
  name. A model flow labels its display section `primary`, a command flow labels it with the roster id -
  the projector's choice, mirrored in the event rows. An unmatched leading slash is a failed `command`
  call (`isError: true`, `outcome: "failed"`), still a 202 with an `error` turn, not a 4xx.
* An answered owner question records `[start, usage, tool_call, action_answer, tool_result,
  attempt_commit, text, usage, done]`. There is no `action_request` event in this build's record even
  though `HarnessEvent::ActionRequest` exists and the projector handles it (`projector.rs:1256`): the
  RUNTIME never emits it, so the pending list is the only place the question itself is visible. OWNER
  gap for the stage that wires `actionRequest` end to end.
* A tool CREATED by this session has a declaration, an installed source revision, budget slot and check
  list, but NO callable body: `POST .../tools/{name}/executions` answers `200 ok:false` with the
  kernel's "not registered" sentence. The module host that turns installed source into a registration is
  T6. `POST .../tools` honours `replace: true` (api.ts answers a 403 capability refusal that contradicts
  this kernel's own `replace` field), and a name that fails the created-name grammar is refused BEFORE
  any "already installed" conflict, so `timeNow` is a 400 and `toolRegistry` is the 409.
* An instruction override reports `source: "session"`; api.ts's preset answers `'harness'` for the same
  write. `{"instruction": null}` restores the catalogue text through the `""`-forwarding arm in
  `api/session_agents.rs` (serde maps JSON null to absence on `Option<Option<String>>`, and the store
  filters `""` back to "no override"). The bind path requires a trusted catalogue profile whose `name`
  equals its `profile`; there is no session-invented (R7) member here, and an already-bound name is the
  store's 409.
* The transcript write receipt's links are REBUILT by the handler (`api/transcript.rs:142-160`):
  `store/transcript.rs:397` still emits the pre-R5 pair (`{root}/sessions/{id}/transcript/{thread}`),
  which this router does not mount and which duplicates `transcript` into `messages`. TS builds the
  three from the mount it is served under (`api.ts:3705-3715`), and the handler is the layer this stage
  owns.
* Step ids are the recorded EVENT INDEX of the step's opening event, not the projector's `s{n}` label; a
  step whose call has not answered has no `result` member at all (`sessions.ts:3955-3963`). Block ids
  are the display tree's context-segment ids, and this projector emits no `context:*` ops, so every
  block read is the honest `Unknown context block "..." in turn "..."` 404 until the context bar lands.
* `PATCH` bodies refuse an UNKNOWN FIELD before the session lookup (`api.ts:2055-2058`), so a typo
  against an unknown session is a 400, and an empty `{}` against one is the 404.

### Tests

`tests/http_sessions.rs` - 14 tests over the library router on a real `axum::serve` listener, each with
its own database root under `Temp/typecrate-harness/http-sessions/<pid>-<test>/` and a local
OpenAI-compatible provider that speaks SSE (scripted answers, scripted 500s, and a HELD-BACK answer for
every assertion that needs a live run). Pinned: the lifecycle with the 404/409/field-order refusals and
the header-equals-body rule on every write; a submitted task and every read that describes it (receipt,
detail, camelCase page row, display tree with the minted section ids, event page with the ordinal filter
and the cursor, step pair by recorded index, block 404, document, messages page, usage fold); the queue
ordinal behind a clipped submit; the steer path; reset both busy and idle plus cancel; the transcript
write, its optimistic lock, the raw-acceptance path and the three reads; conversation CRUD with the
descendant cascade and the thread cancel; the roster with the instruction ladder, the UTF-16 `chars`
discriminator, the unbind/rebind pair and every bind refusal; the toolbox page, budget, create, source,
patch, delete, the protected/inherited policy refusals and the replace path; the manual call of a real
tool and of a created one; paging and the route-level refusals (405 wording, 415, the body-less routes,
the malformed-JSON 400); a slash command end to end; and the owner-question round trip including the
parked turn's `partial: true`, the result-less step, the answer event and the custom-choice twin.

Two pre-existing assertions moved, both forced by deleting a stub rather than by weakening a check:
`tests/http_host.rs`'s submit-cap boundary now expects the real `404 Unknown session "session-1".`
instead of the stub's `500 not implemented` (the 16 MiB refusal below it still answers 413, which is the
proof that test exists for), and `tests/store.rs:1162`'s display-row block now expects the merged
`promptPreview` / `promptChars` pair plus the `id` / `index` / `threadId` stamps T5b added, replacing the
empty-pair expectation T4 recorded while the projector was the only source.

### Facts the stream and runtime stages must not rediscover

* `Ctx` is `Send` but NOT `Sync`: an `async fn` that takes `&Ctx` holds a non-`Sync` reference across an
  await, which breaks axum's `Handler` bound at the `router()` call site with "future cannot be sent
  between threads safely". Route handlers take `&mut Ctx`; anything called from inside one with `&Ctx`
  must be synchronous (e.g. `assert_idle_for_tool_mutation`).
* A handler that refuses BEFORE reading the body still has to let the dispatcher drain it
  (`api/mod.rs`, the post-dispatch drain), or a large upload turns the refusal into a reset. This is how
  `POST /sessions/{id}/turns` with a body answers 400 and how an over-budget submit to an unknown
  session answers 413.
* The display tree a listener reads and the `display.ops` a stream publishes are the SAME fold
  (`turn_display`), so the ids the section-rename mints here are the ids the stream must reuse.
* The usage route's `by_thread` shares the turn page's conversation labels; the queue page's `position`
  is the 0-based slot while the receipt's ordinal is 1-based (`read.rs:1160-1162`) - both figures appear
  in one test on purpose.

## T5c - the update stream (`stream/{mod,protocol,broker,connection}`)

### What landed

* `stream/protocol.rs` - `protocol.ts`: the twelve close codes, the per-scope `publishes` /
  `ids_required` / `ids_forbidden` / `optional_filters` / baseline tables, `kind_resources`,
  `stream_resource_id_for`, `default_stream_link`, the `base64url(epoch:sequence)` codec (split on the
  LAST colon), `chunk_stream_payload`, `resolve_stream_limits` with the six tunable bounds, the frame
  builders for all seven server frames, and `parse_client_frame` with TS's exact precedence and
  sentences. The 18 limit numbers come from `contract::ws_stream_limits()` - the module restates none.
* `stream/broker.rs` - `broker.ts`: subscriptions, per-subscription monotonic sequences and cursors,
  the 512-frame / 60 s retained window, the parked windows a reconnect resumes from, the three
  `resync_required` reasons, the subscribe validation ladder, `matches`/`narrow`, the
  `display.ops`(32) / `run.event`(16) / ids(64) chunking, `pace:"ack"` stop-and-wait with its 5 s
  deadline, and `close_all`.
* `stream/connection.rs` - `connection.ts`: `welcome` first, the read loop, `hello` (first-only, no
  reply), subscribe -> `subscription_state` + replay, ping/pong, `bye` -> 1000, the 15 s heartbeat with
  the 3-interval 4408 timeout, the 32/s budget -> `rate_limited` + 4429, three consecutive union
  failures -> 1007, the drop-oldest-droppable send queue -> one `frames_dropped` per subscription, the
  10 s stalled-write 1013, and `broker.detach` on the way out (a run is never cancelled).
* `stream/mod.rs` - the handshake gates + `is_websocket_upgrade` + `pathname_of`, in `transport.ts`'s
  order, and the upgrade itself (`protocols(["harness.v1"])`, `X-Harness-Epoch` on the 101).
* `runtime.rs` - `HarnessRuntime::stream_broker`, built with `store.clone()` as
  `ports::StreamExists` and `StreamBroker::with_hub_listener`, so every `hub.publish` fans out to the
  live streams without `api/**` importing the stream module at all.
* `api::handle_stream` - one delegation to `stream::upgrade`. `api::root::stream_route` stays as the
  capability refusal for a request that arrives through the JSON dispatcher instead of the mount.
* `tests/stream.rs` - 19 real-socket tests; `tokio-tungstenite = "0.29"` added as a dev-dependency (the
  version axum's `ws` feature already resolves, so the lock gains one dev edge and no new crate).

### The counters are the hub's, not the broker's

TS's broker owns a counter `Map` and moves it inside `publish`. This crate cannot: a stream revision
must equal the `X-Harness-Revision` a REST read carries, and `events.rs` is explicit that `publish` is
REPORT-ONLY because the PUBLISHER owns the bump (`api-events.ts:302-306`, `knowledge.md` "T5b"). So
`StreamBroker::publish` takes the `HubEvent` the hub already settled and never bumps; the baseline and
the resync links are counter READS against the same hub. Consequences a later stage must not re-derive:

* A child-scoped counter is keyed `session:child` (`events::ResourceRef::normalized`, the composite the
  REST write stamped). `StreamScopeIds::hub_resource_id` builds that key for the frames;
  `protocol::stream_resource_id_for` keeps TS's PLAIN id spelling and is exported for callers that want
  the contract table verbatim. They differ for `turn*`/`thread`/`transcript*` only.
* `protocol::default_stream_link` is the contract's table and quotes `?limit=25` on `turns`,
  `display`, `events`, `threads`, `actions`, `agents`, `tools`, `commands` and `messages`;
  `events::baseline_link` is the hub's own table and prints `/display` and `/agents` WITHOUT a limit.
  `resync_required.resources` uses the contract's (so a resync's links match swagger), and the two
  tables are therefore NOT interchangeable - do not "unify" them without checking swagger lines
  2197-2245.
* Because `ack`/`ping`/`subscribe` all read the store or the hub, the lock order is
  **store -> broker -> hub counters**. A REST mutation publishes while it still holds the store lock
  (`Store::commit` -> `hub.publish` -> `fan_out` -> `StreamBroker::publish`), which is why
  `StreamBroker::unknown_resource` (a store read) runs BEFORE the broker lock is taken. The cost is one
  reordered pair: a `subscribe` that is both a duplicate id and an unknown resource reports
  `unknown_resource` here where TS reports `invalid_payload`.

### Deliberate deviations (stated, not hidden)

* **Frames are `serde_json::Value`, not serde models.** The union is discriminated twice (`type`, then
  `scope`/`kind`) and `payload` is whatever the mutation published, so a typed model would either lose
  the payload's shape or re-serialize it. Field names are pinned by the builders and by the tests.
* **No transport module.** hyper + tokio-tungstenite own the codec, masking, fragmentation and the close
  handshake, so `transport.ts`'s 677 lines collapse into `stream/mod.rs`'s gates and one sentence:
  `This conversation carries JSON text frames only` (close 1003, sent for a binary frame, with no error
  frame - it is below the JSON layer).
* **The client-frame byte cap is enforced in the parser, not on the wire.** tungstenite's default
  message cap is left alone, so an oversize text message reaches `parse_client_frame` and the client
  reads `error{frame_too_large, close_code:1009}` BEFORE the close. TS's transport closes 1009 without
  the frame; this is the same outcome plus the reason.
* **Parked windows carry a deadline instead of a timer**, pruned at the next lookup; a refused resume
  (`cursor_expired` / `epoch_mismatch`) leaves the window in place, which is TS's behaviour too
  (`dropParked` sits inside the resume arm only).
* **The ack deadline is a tokio task** guarded by a per-subscription token, so an `ack` that arrives
  first makes the timer a no-op. Outside a runtime (`Handle::try_current` fails) there is no clock and
  the hold simply waits - the equivalent of TS's `unref()`'d timer in a process on its way out.
* **Two hub-shape adaptations at the payload level**: `display.ops` folds `HubEvent::ops` into the
  contract's `ops` field, and `run.event` wraps the ONE recorded row (`store/turns.rs:1424-1429`) into
  `{turn_id, events:[row]}` because `WsUpdateRunEvent` forbids any other top-level field.
* **Replayed frames are re-sent exactly as minted** (`broker.ts:774` maps the retained frames
  themselves), so a replayed `update` still names the `subscription_id` that first received it. Only
  `subscription_state` identifies the new subscription. A test that asserts the new id here is wrong.
* **`resolve_stream_limits` takes an override map, not `Partial<WsStreamLimits>`**, and nothing in this
  crate overrides anything yet: the deployment knob is `welcome`'s contract, not a config file.

### Facts the runtime and T6 stages must not rediscover

* A `session.updated` payload's `session.revision` is the PRE-write counter for a PATCH
  (`store/session.rs:339-354` builds the patch before `Store::commit` moves the bumps, and the patch
  lists its own bump targets, so `session` moves twice: 1 -> 3), while the CREATE payload is settled
  (`built_session_payload` bumps first). The freshness rule to assert is therefore
  `subscription_state.revisions` vs `X-Harness-Revision` (equal), not the payload's own `revision`
  (`tests/stream.rs` pins both facts).
* `stream::upgrade`'s two 400 sentences are different conditions: a GET with NEITHER upgrade token is
  the JSON dispatcher's case and answers TypeScript's own
  `GET /stream expects a WebSocket upgrade (Sec-WebSocket-Key/Version) for the harness.v1 subprotocol.`
  (the sentence `api.ts:3911`, `queue.test.ts:1090`, `view.test.ts:779` and `tests/http_host.rs:628`
  pin); tokens present but wrong answers `The request is not a WebSocket upgrade.` (`transport.ts:623`).
  The capability refusal runs before both, and the exact-path gate runs after the version gate - it is
  only observable by calling `stream::upgrade` directly, because `server::router` mounts exactly one
  path at the stream and sends everything else to the REST dispatcher.
* The handshake carries NO resume state, so a query string on the stream path is ignored (not an error)
  and `hello` is the only resume input: a `hello` after the first `subscribe` is `late_hello` and changes
  nothing, and a repeat `hello` is silently ignored rather than an invented error.
* An `error` frame with `close_code` closes the connection immediately AFTER the frame is on the wire.
  That is how 4401, 4429, 1009, 1003 and the host's 1001/1012 reach a client with their reason.
* The subscription budget (64) is per CONNECTION, and the same `scope`+ids pair may be subscribed twice
  with different ids. 65 subscribe frames cannot be sent in one second: the 32/s message budget refuses
  at 4429 long before the budget refuses at 4401, which is why the budget probe paces itself.
* The stream's epoch is the process epoch (`events::process_epoch`), shared by every hub in the process.
  A test that wants two epochs needs two cursor strings, not two hubs.

### T6's seam

`runtime::cached_runtime(root).stream_broker()` is the only way in. A service route that wants the
stream mounts `stream::upgrade(root, request)` on its own upgrade path (it already has the epoch gate and
the runtime cache), and calls `broker.close_all(1012)` on an epoch change or `close_all(1001)` on host
shutdown. T7a CLOSED the second half: `server::begin_shutdown` / `begin_shutdown_all` and
`runtime::close_streams` are the production callers (`server.rs:118-138`), and `serve_on`'s shutdown arm
runs the stream half before the listener drains.

### Tests

`cargo test` is 247 (was 209): 19 unit tests in `stream/protocol.rs` (the cursor codec including a colon
inside the epoch, every refusal sentence, the scope tables, the tunable bounds, the chunker, the frame
builders) and 19 in `tests/stream.rs` (handshake + subprotocol + epoch header; the exact 18-number
`welcome`; `hello` silence and `pong`; `subscription_state` with the baseline records equal to the REST
counters; a REST patch arriving as an `update`; cursor replay with `replayed_frames`;
`epoch_mismatch`; ack-ordered silence vs `unknown_subscription`; ack-paced hold-then-release; 1007 /
1009 / 1003 closes; `bye` 1000; the five HTTP gates plus the path gate; `unknown_resource`, duplicate
id, `unsupported_filter`, `late_hello`; 4401 at 65 subscriptions; 4429 at 33 messages; and
`close_all(1012)`).

## T6 - the `typecrate-services` gateway bridge (`endpoint/harness/harness-route.rs`)

### What landed

`packages/typecrate/services` now serves `/v1/harness`, and it serves it THROUGH its own build-time
route discovery rather than through a special case: one new file in this package,

```text
packages/typecrate/harness/endpoint/harness/harness-route.rs
```

is discovered by the services `build.rs` scanner (suffix rule `<stem>-route.rs` + a directory named
`endpoint`/`endpoints`, scanned from the REPOSITORY root - `services/build_support/mod.rs`), compiled
into the services crate as `<identifier>::register` + `<identifier>::ENDPOINTS`, and mounted on the
same `Router` that `typecrate-services-server` serves. The file is three handlers and one
`crate::service_routes!` block, and nothing else:

```rust
ANY "/v1/harness"          => harness_root,    // api::handle_request
ANY "/v1/harness/stream"   => harness_stream,  // api::handle_stream
ANY "/v1/harness/{*rest}"  => harness_api,     // api::handle_request
```

`State(crate::server::AppState).database_root` is handed to the dispatcher as the harness runtime root,
which is the SAME value the standalone `--database-root` takes, so the store, the registry, the epoch
and the durable document are per-root in both hosts and no path is re-derived from `cwd`.

### The two facts a future stage must not re-derive

* **`server::router(root)` and this bridge are alternative mounts of one dispatcher, not two
  implementations.** `src/server.rs` still owns the standalone surface (including its `fallback` for
  paths outside the mount, which the services router does NOT need: services answers those with axum's
  own 404, and the harness's "this server serves /v1/harness" sentence still comes from the dispatcher
  for anything UNDER the mount). A route file must never re-implement a harness answer.
* **`axum::extract::Request` must reach `api::handle_request` whole.** The dispatcher reads headers,
  the query, and the body itself (`api::mod.rs::BodyReader`), so a gateway that pre-extracts (`Json`,
  `Bytes`, a `FromRequest` guard, or a body-limit middleware that actually wraps the body) changes the
  contract: the 413/415/400 sentences, the laziness rule (`api.ts:4009`, `:4022-4024`) and the drain
  behaviour all live below the router. Services' `DefaultBodyLimit` is safe here for exactly one
  reason: that layer only governs extractors that ask for it, not a handler that reads `Request`
  (`axum-core-0.5.6/src/extract/default_body_limit.rs`), so the gateway's 1 MiB never becomes the
  harness's cap - the 1 000 000 / 16 MiB caps in `body_limit_for` stay in force, and
  `services/tests/harness_gateway.rs::the_gateway_body_cap_governs_extractors_not_the_harness_route`
  pins the three-way difference (harness 404 for a 1.5 MiB submit, harness 413 with its own sentence at
  1 000 500, axum's plain-text 413 for the same size on `POST /v1/services/echo`).

### Why `ANY`, and what the router must not decide

The bridge registers `ANY`, matching `src/server.rs:53-62`, and the services macro had to grow an `ANY`
arm for it: axum 0.8.9 has no `MethodFilter::Any` constant, so the verb resolves to
`axum::routing::any(handler)` while every real verb keeps `MethodFilter`. A verb-agnostic mount is what
keeps the contract's own answers on the wire: the 405 names the allowed verbs of the route the
DISPATCHER matched (`api.ts:4006-4013`), and the stream answers a `GET` with no upgrade headers with the
TypeScript sentence. A per-verb router registration would replace both with axum's generic 405.

Two properties of the services table are pinned over a socket in `services/tests/`:

* matchit prefers the STATIC `/v1/harness/stream` to the `/v1/harness/{*rest}` catch-all. The proof is a
  pair of sentences, not a status code: `DELETE /v1/harness/stream` must be
  `The stream upgrade is a GET.` (`stream::upgrade`'s method gate) while
  `DELETE /v1/harness/stream/anything` must be the dispatcher's
  `Unknown API route: DELETE /v1/harness/stream/anything`. A precedence inversion would keep the status
  (405/404) and lose the WebSocket.
* the harness rows are the FIRST rows of `GET /v1/services/routes`, because
  `packages/typecrate/harness/...` sorts before `packages/typecrate/services/examples/...` in the
  scanner's byte-wise order, and they are listed as `ANY`.

### Deviations from the TS host (stated)

* The TS harness reaches the world through `@underload/service`'s Hono gateway, which forks worker
  processes and answers a cross-origin hop with `307`. The Rust gateway is one process, one listener, no
  redirect tier; the CORS/`Access-Control-Allow-Private-Network` header set and the preflight `204` ARE
  reproduced (`services/src/server/mod.rs::gateway`, mirroring
  `packages/underload/service/src/server/start.ts:415-444`), including hono's condition that a preflight
  is an `OPTIONS` **that carries `Access-Control-Request-Method`** - a bare `OPTIONS` stays routable, so
  `OPTIONS /v1/harness` still reaches the dispatcher and still answers `Use GET for /v1/harness.`
* The stream's `close_all(1001)` on host shutdown (the seam above) has an API since T7a -
  `server::begin_shutdown_all()` stops every runtime this process cached - but the services EXECUTABLE
  still has to call it: `AppState` owns no runtime handle, because the runtime is per database root and
  lazily created, so closing it stays a host-lifecycle step in `packages/typecrate/services`, not a
  routing one in this package's route file.

### Tests

`cargo test` in this package is unchanged at 247 (the bridge adds no code to this crate's module graph:
`endpoint/**` is not a cargo target, it is compiled by the CONSUMER's generated table). What moved is
the consumer: `packages/typecrate/services` went from 84 to 96 tests, adding
`tests/harness_gateway.rs` (8: service document + epoch header + `api_root`; `health` and an exact empty
page; a created session listed and persisted at `<root>/harness/state.json`; the 35-row tool catalogue
with `timeNow`; the `harness.v1` 101 through the gateway layer with `welcome` + `subscription_state` on
the REST epoch; the non-upgrade 400; the static-vs-catch-all sentences; preflight `204` vs routed
`OPTIONS`; the body-cap triangle), one repo-root discovery test, one `ANY` macro test, two gateway-layer
unit tests, and the 18-row pin of `GET /v1/services/routes` inside `tests/server_http.rs` (whose dispatch
plan now sends the literal `ANY` verb to the three harness rows). `cargo check --target
wasm32-unknown-unknown` still passes: `cargo tree --target wasm32-unknown-unknown` lists `wasm-bindgen`
and nothing else, which is the proof that the non-wasm path dependency did not leak.

## T7a - the audit remediation (`store/**`, `projector`, `runtime`, `server`, `transport`, `turn`)

An independent audit of T2-T6 against the TypeScript spec produced ten defects. All ten are closed here,
in the audit's priority order, and every fix is a SPEC alignment - nothing was redesigned. Three facts
stated in earlier sections changed, and those sections were corrected in place: T3's "the durable queue is
the LEGACY array" bullet, T5c's "T6's seam" sentence, and T6's shutdown deviation.

### The ten fixes, and where each one lives

1. **A host write is live state, not boot state** (`runtime.rs:460-468`, `:589`, `:599`, `store/mod.rs:666-704`).
   `POST /harnesses`, `PATCH /config` and the agent routes mutate the runtime's OWN
   `RwLock<HarnessRegistry>` / `RwLock<AgentCatalogue>` / `RwLock<ServerConfig>`, while `create_session`
   resolved the harness, the provider trio and the instruction layers from the copies the store was
   `open`ed with. TS has no such split: `api.ts:1974` / `sessions.ts:1827` read `getLoadedConfig()` and
   `getAgentCatalogue()` fresh. The store now owns a HOST VIEW - `StoreInner::host_registry`,
   `host_agents`, `host_config` (`store/mod.rs:666-678`) with `registry()` / `agents()` / `settings()`
   accessors on the guard (`:684`, `:690`, `:697`) - and the runtime PUSHES a copy of its own three
   values into it at creation (`runtime.rs:464`) and after every save, every `save_agents` and every
   rollback (`:542`, `:589`, `:674`, `:700`, `:715`, all through `sync_store`).
2. **The session's endpoint is the session's own provider view** (`store/read.rs:467-478`,
   `api-reads.ts:225` `providerDefaultsOf(record.settings)`). `session_endpoint_of` joins the SESSION's
   base URL / model / key-name with the deployment credential, so a session created against
   `https://deep.example` no longer reports the deployment's URL. `Store::provider_of` and
   `Store::session_resource` now take `(inner, state)` (`store/session.rs:887`) because the host view
   they consult is a field of the same guard.
3. **The question channel reaches the wire** (`store/actions.rs:610-690`). `StoreActionHost::request`
   records the entry, projects the ONE ready-to-answer row through the same `read::pending_action_row_of`
   the REST read uses, bumps `actions`, and publishes `action.opened` addressed at
   `("actions", session_id)` - the payload field is `action`, and a question that vanished between the
   push and the read answers `action.closed{reason:"ended"}` instead of an empty `action.opened`
   (`api.ts:1105-1125`).
4. **Live display ops carry the snapshot's section ids** (`projector.rs:2070` `section_wire_id`,
   `:2090` `stamp_turn_identity`, `:2127` `stamp_display_ops`; called from `store/mod.rs` `record_event`
   and from `store/read.rs:739-743`). The projector mints per-turn-local ids (`s1`); the REST display doc
   prints them qualified (`<turnId>:1`). TS stamps once, in the projector, so both reads see the same
   string (`server/turn-display.ts` `qualifySectionId`). The rule is now ONE string function used by both
   paths, and the turn-level `usage` frame's `sectionId: "turn"` sentinel passes through unchanged
   because it is not an id the snapshot mints.
5. **A cancel-all reports the stops it made** (`store/session.rs:636-770`, `sessions.ts:7765-7788`,
   `api.ts:2334-2358`). TS's answer is not "a run was aborted": it counts the host conversations
   `stopThread` ACTUALLY transitioned (`if (stopped > 0) cancelled = true`), so an idle session with a
   live HOST conversation is still a real cancellation - while an internal `agent:<name>` conversation is
   plumbing and stays out of it, and a second stop-all over already-stopped conversations is the
   `cancelled: false` no-op. The response also names the running turn, falling back to the newest row
   while `record.busy` is set, and drops BOTH queues (legacy and every lane).
6. **The lane backlog is durable** (`store/mod.rs:1660-1682` write, `:2421-2451` restore).
   `record.queue` is the union of the legacy array and every lane queue, sorted by the turn `index`
   minted at enqueue, and `restore_session` re-homes each id into its own lane exactly as TS's F1
   distribution does (`sessions.ts:3202-3228`, `:5947-5953`). A lane's ACTIVE turn is already out of its
   queue (`claim_next_turn` peeks), so no id appears twice. This removes the ordering hole the T3 stage
   note admitted: before, a restart dropped a lane's backlog and a fresh submit overtook it.
7. **The session resource reports live execution** (`store/read.rs:480-493`): `runtime_execution` is
   `api-reads.ts:226` `context.runtime.threadExecution`, i.e. the host view's CURRENT `thread_execution`,
   not the built-in default substituted at open time. `session_resource_of` therefore lost its `settings`
   parameter - the runtime it already took IS the deployment view.
8. **A graceful host says goodbye** (`server.rs:100-138`, `runtime.rs:594-653`, `store/mod.rs:1047-1055`).
   `serve_on`'s shutdown arm calls `begin_shutdown(root)` BEFORE the listener finishes draining:
   `Store::begin_shutdown` sets the `shutting_down` flag TS's `dispose()` sets (`sessions.ts:8638`, so no
   pump claims queued work behind the checkpoint) and `runtime::close_streams(STREAM_SHUTDOWN_CODE)` fans
   the contract's `error{close_code:1001}` frame plus the close to every live connection. `flush` keeps
   calling `runtime::shutdown` (flag, sockets, then the durable document - the order
   `start.ts:319-340` states), and both steps are idempotent. The same item closed the settlement half
   of the action contract: `action_closed_reason` (`store/actions.rs:784-801`) reproduces TS's
   `actionClosedReason` phrase test (`sessions.ts:361-370` - `cancel` -> `cancelled`, `ended` -> `ended`,
   else `expired`), `SessionState::expire_actions` returns the ids it settled, and
   `Store::publish_actions_closed` (`store/actions.rs:803-829`) fans one `action.closed` per settlement,
   bumping `actions` once before each frame. Every terminal path routes through it: an owner answer
   (`store/actions.rs:1129-1146`), a stop and a cancel-all (`store/session.rs:416`, `:748`), a run that
   ended (`store/turns.rs:1359-1364`), a reset and a delete.
9. **Sampling reaches the provider request** (`transport.rs:271-300`, `:922`, `:937`;
   `store/read.rs:1762-1817`). `ProviderConfig` gained `sampling: Option<Value>` (`contract.rs:2935`),
   `read::default_sampling()` reproduces the toolbox default (`server/harnesses/toolbox.ts:242-246`:
   `temperature: 0.3`), `read::effective_provider` attaches it to the session's provider view, and
   `transport::sampling_body` merges ONLY the five numeric `HarnessSampling` keys
   (`SAMPLING_KEYS`) into the body - a server-side knob a client can never supply
   (`transport/openai.ts:126-140`).
10. **Steering a failed run never consumed goes back to the conversation**
    (`turn.rs:228`, `:248`, `contract.rs:2913`, `store/turns.rs:1239-1264`). `Run` tracks
    `delivered`, the mailbox drain hands those texts to the outcome as `TurnOutcome::Failed::unused_steering`,
    and `settle_turn` re-queues them into the conversation's steer mailbox and puts `steer_active` back,
    so the next run reads guidance the user already typed (`sessions.ts:6461-6495`, which re-queues on a
    failed turn rather than dropping it).

### Why the host view is a store field and not a second runtime lock

The alternative was for `create_session` to reach the runtime for the live registry. It cannot: the
documented lock order is **host lock -> store lock**, because `Store::commit` publishes while it still
holds the store mutex and the hub's listener is the stream broker
(see T5c's "the lock order is store -> broker -> hub counters"). A store-side read of a runtime
`RwLock` would invert that and deadlock the first `PATCH /config` that raced a submit. So the value moves
INTO the store (one push, no read back), and the rule is now:

* `Store::registry()` / `agents()` / `settings()` (`store/mod.rs:1022`, `:1028`, `:1038`) TAKE the store
  lock and return owned clones. **Never call one while holding a store guard** - use the guard's own
  `inner.registry()` / `inner.agents()` / `inner.settings()`, which borrow. This is the one trap the
  change created, and it is a runtime deadlock, not a compile error.
* A runtime method that wants the view calls `sync_store()` (`runtime.rs:599`) - which clones each value
  out of its own lock and releases all three before touching the store - and nothing in the store ever
  calls back into the runtime.

### Facts a later stage must not re-derive

* `record.queue` is the WHOLE pending set, in turn-index order. A writer that pushes only `self.queue`
  silently makes a lane's backlog memory-only again, and the restore split (`store/mod.rs:2421-2451`)
  will re-home nothing.
* The `actions` counter is the resource `action.opened` / `action.closed` are addressed at. Publishing
  those frames without bumping `actions` leaves a client that re-reads `GET /sessions/:id/actions` with a
  stale figure, and bumping without publishing leaves the baseline a reconnect reconciles against one
  ahead of the frames.
* Section ids are a STRING rule in one place (`projector::section_wire_id`). A second formatter that
  derives the qualified id from the ordinal alone is fine for this projector (the positional and ordinal
  mappings are identical because the projector emits sections in increasing ordinal order) and wrong for
  any projector that reuses an ordinal.
* `ProviderConfig` is no longer `Eq` (it carries a `Value`), and its `sampling` is server-side state:
  a route must not accept sampling fields from a request body.
* The frame key order this crate emits is the union's DECLARED order (`type, subscription_id, sequence,
  cursor, kind, at, <payload>`), because `serde_json` here runs with `preserve_order`. Tests that pin
  `frame.keys()` must state the declaration, not a sorted copy.

### Tests

`cargo test` is 256 (was 247), and `cargo clippy --all-targets -- -D warnings` plus `cargo fmt --check`
are clean. The ten tests below - nine new, one rewritten - pin one observable behaviour per audit item:

| Test | Pins |
| --- | --- |
| `tests/http_sessions.rs::a_host_write_is_visible_to_the_next_session_without_a_restart` | fix 1 (a saved harness, model and instruction reach the NEXT session over HTTP) |
| `tests/store.rs::the_session_endpoint_is_the_sessions_own_provider_view` | fix 2 |
| `tests/stream.rs::the_action_channel_publishes_the_opened_and_closed_frames` | fixes 3 + 8 (both frame kinds, the row shape, the reason vocabulary, the per-subscription fan, and the counter the baseline reports) |
| `tests/store.rs::the_live_display_ops_carry_the_snapshot_section_ids` | fix 4 (a real `TurnProjectorFactory`, live ops equal to the snapshot's ids) |
| `tests/store.rs::cancel_all_reports_a_stop_of_host_threads_on_an_idle_session` | fix 5 |
| `tests/store_restore.rs::a_durable_backlog_survives_a_restart_and_runs_in_order` | fix 6 (rewritten: a per-lane backlog, restarted in order) |
| `tests/store.rs::the_session_resource_reports_the_live_thread_execution` | fix 7 |
| `tests/stream.rs::the_host_shutdown_closes_a_live_stream_with_the_1001_frame` | fix 8 (the `error` frame plus close 1001, and the store's flag) |
| `tests/transport.rs::sampling_fields_merge_into_the_request_body` | fix 9 |
| `tests/store.rs::a_failed_run_hands_its_drained_steering_back_to_the_conversation` | fix 10 |

Three existing assertions encoded the OLD behaviour and were corrected to the spec, not to the code: the
session resource's `endpoint` in `tests/http_sessions.rs` (it now names the session's own base URL),
`the_provider_credential_is_never_persisted` in the same file (the credential rides the endpoint
read-model, the document still never carries it), and `a_durable_backlog_survives_a_restart_and_runs_in_order`
(which used to assert that a lane backlog came back UNCLAIMED).

**The one flake this stage removed (test-side, not one of the ten):**
`tests/stream.rs::the_per_second_message_budget_answers_rate_limited_and_closes_4429` failed on Windows
about one run in three - in isolation as well as under load - with
`Io(Os { code: 10054, kind: ConnectionReset })` (it failed the same way, at the same read, in the FIRST
baseline run before any T7a edit: `Temp/typecrate-harness/t7a/baseline-harness.txt`). The burst sent 40
frames past a 32-frame budget, so the server closed with seven of them still unread in its receive queue;
a socket closed with unread data is RESET rather than FIN'd, and the reset discards the pongs and the
close frame the server had already written. The burst now stops AT the trip (33 frames), which is the
same contract question with no race - every assertion is unchanged, and the test passed six consecutive
runs after the change where it had failed inside three before it.

---

## T7b - the second audit remediation (`projector`, `api/**`, `store/**`, `tools/**`, `registry/**`, `paging`, `read`, plus the `typecrate-services` shutdown hook)

Thirteen findings from a fresh audit of the port against the TypeScript spec. Each row states the
defect, the fix, the TypeScript authority, and the test that pins it. Nothing here is a redesign: the
spec is the TS source, and where a fix could not reach the seam it says so.

### What changed

| # | Defect | Fix | TypeScript authority | Test |
| --- | --- | --- | --- | --- |
| 1 | The harness registry dropped `agentColors`: the preset row, the durable document, the PATCH body and the resource read all lacked it. | `agent_colors: Option<BTreeMap<String, String>>` threaded through `registry/{mod,descriptor,presets}.rs`, `persistence.rs`, `contract.rs`, `read.rs`, `store/{read,session}.rs` and `api/harnesses.rs`, with the hex / membership / blank refusals. | `server/harnesses/registry.ts:430,569,589`, `persistence.ts:131`, `shared/harness-api.ts:454,694` | `tests/data_plane.rs::the_agent_color_map_follows_the_same_rules_as_the_other_two`, plus the `/harnesses` colour assertions already in `tests/http_host.rs` |
| 2 | `GET /harnesses` stamped `X-Harness-Revision`; the page route has no per-resource revision to quote. | The list route answers through the plain json helper, so only `X-Harness-Epoch` is stamped; the one-harness detail routes keep their revision. | `api.ts` (`getHarnesses` returns `json(...)` with no `revision` argument) | `tests/http_host.rs:983-998` (the header is asserted absent) |
| 3 | `POST .../tools` over an existing CREATED name answered 409, and `replace: true` was accepted silently. | A duplicate create REPLACES: `201 {created: false, replaced: true, previousRevision}` and the create slot is not spent; `replace: true` is refused as a capability, and `checks` is omitted from every registration receipt (this build has no runtime compiler to run them). | `core/creation.ts` (a created name is not reserved), `api.ts`'s `toolCreate` arm, `harness-api.ts:1355` (`checks` is the compiler's answer) | `tests/http_sessions.rs::a_tool_registration_replaces_by_name_and_refuses_the_replace_flag` |
| 4 | The global `/tools` surface listed all 35 catalogue rows, including the four the manifest withholds. | A name set `catalogue::UNLISTED` + `is_listed()`/`listed_entries()`, so `ENTRIES` stays a byte-for-byte projection of the generated manifest; `ToolRuntime::summaries()` answers **31** and the new `catalogue_summaries()` keeps a SESSION toolbox at **35**. | `core/tool-manifest.ts:1002, 1046, 1692, 1811` (`listed: false`), `tools/descriptors.ts:290-328`, `api-reads.ts:831-836` (`sessionToolRowsOf` reads the live registry) | `tests/tools.rs::the_catalogue_lists_every_group_in_the_published_order`, `tests/http_host.rs:1805-1840` |
| 5 | The projector minted no context segments, so `TurnSection.context` was always empty and `GET .../blocks/{id}` could never resolve. | `Section.context` plus `pushBlock`/`patchBlock` (`add_block`/`patch_block`): a `tool` segment per call, keyed by its own event index with `argsChars`, settled in place by its result with `chars = argsChars + resultChars`, `resultChars`, `state`, `endedAt`, `eventTo` and the recorded `durationMs`; the `done` fallback mints the `assistant` segment when no delta streamed. Both publish real `context:add` / `context:set` ops, and every wired section carries the `context` member. | `turn-projector.ts:1029-1082` (`pushBlock`, `blockIdOf`, `updateToolBlock`), `:2043`, `:2195`, `:2378-2390`; `turn-display.ts:370-559` (the bar's source list, the `ContextBlock` shape, invariant 5) | `tests/projector.rs::a_root_flow_opens_one_section_and_streams_its_answer`, `a_tool_round_becomes_one_step_row_with_its_result`; `tests/http_sessions.rs` (the display publishes `1-tool-2`, `GET .../blocks/1-tool-2` answers 200 with the recorded `args`/`result`) |
| 6 | Body-less verbs called `require_empty_body`, which PARSED a body they are meant to ignore. | Every `GET`/`DELETE`-style handler calls `ctx.ignore_body()` (drain, never parse); `require_empty_body` stays only for the one unit test of the TS `readEmptyBody` helper. | `api.ts`'s read handlers (`readEmptyBody` is applied where a body is meaningful) | the unchanged `tests/http_host.rs` read suite, which now exercises the drain path |
| 7 | Paging compared query strings by hand, so `?limit=` (empty) fell back to the default instead of the TS answer. | `paging::number_value` / `is_safe_integer` reproduce ECMAScript `Number()` coercion, so `?limit=` is `0` (a 400), a non-numeric value is a 400, and anything above `Number.MAX_SAFE_INTEGER` is refused. | `api-paging.ts:41,46,50,56,89` | the paging cases in `tests/http_host.rs` and `tests/data_plane.rs` |
| 8 | The usage fold invented a `cached` member and ordered threads by name. | The tally states `{input, output, total}` and preserves FIRST-APPEARANCE thread order; `cached` stays a transport-side figure. | `api-reads.ts`'s usage page builder, `sessions.ts`'s usage fold | `tests/store.rs` / `tests/turn.rs` usage cases |
| 9 | Several display bounds counted `chars().count()` (code points) where TypeScript counts UTF-16 code units. | `display_chars` is the single UTF-16 measure, used by every preview, notice, tail and block count. | `turn-projector.ts:134-190`, `turn-display.ts:95-99,412-416` (invariant 5) | `tests/foundation.rs::character_figures_are_utf16_code_units`, `tests/data_plane.rs::the_harness_text_bounds_are_measured_in_utf16_units` |
| 10 | The `typecrate-services` listener never told the harness runtime it was going away, so a live turn stream held the graceful drain open. | `bin/server.rs` runs `typecrate_harness::server::begin_shutdown_all()` as soon as the signal resolves, before axum stops accepting: every cached root's store flips `shuttingDown` and every live stream closes with `1001`. A process that serves no harness root (remote-hosts mode) is unaffected. | `start.ts:319-327`, `sessions.ts:8638` (`dispose`'s `shuttingDown`) | the harness side is pinned by `tests/stream.rs::the_host_shutdown_closes_a_live_stream_with_the_1001_frame`; the services wiring is a one-line hook in the bin |
| 11 | The playground checker's listing fixture was the pre-T6 fifteen-row table, so it validated a listing the server no longer produces. | `LISTING_JSON` is the eighteen-row listing with the three `ANY /v1/harness` rows first; the eight expectations that encoded the fifteen-row order moved with it. | `tests/server_http.rs::route_table`, `tests/discovery.rs` (R2 sort order) | `node scripts/check-playground.mjs` - 122/122 |
| 12 | `README.md` still described stage T1. | Rewritten as the crate's real manual: layout, commands, the standalone listener's flags and defaults, the environment ladder, the data-root and journal formats, the parity matrix, the full deviation list, and the verification counts. | - | - |

### Assertions that encoded the old behaviour, and were corrected to the spec

* `tests/http_host.rs::the_harness_page_lists_the_shipped_registry` - the page now stamps no
  `X-Harness-Revision` (fix 2).
* `tests/tools.rs::the_catalogue_lists_every_group_in_the_published_order` and
  `tests/http_host.rs`'s tool page cases - 35 rows became 31 for the LISTED surface, with the four
  withheld rows named in the message (fix 4). The session toolbox pin stays 35.
* `tests/http_sessions.rs`'s `?limit=` cases - an empty limit is `Number("") == 0`, which is a 400
  with the "A limit must be between 1 and 100." sentence, not a silent default (fix 7).
* `tests/projector.rs`'s two display fixtures - every wired section now carries `context`, and the
  tool round publishes `context:add` beside its `step:set` and `context:set` beside the result patch
  (fix 5). The op-name lists and the compiled tree were rewritten to the shapes above, not loosened.
* `tests/http_sessions.rs`'s block-segment block - `0-user-prompt` stays the honest 404 (the
  projector seam has no prompt to mint a `user` segment from), and the tool segment the fold DOES
  mint is now fetched by id and answers 200 with the recorded pair.

### Deliberate deviations added or sharpened this stage

1. **No `user` / `system` segments.** `ProjectorFactory::create(&self)` (`ports.rs:239-241`) receives
   no prompt, and the protocol's event enum (closed for this stage) has no user or instruction event,
   so the two kinds TypeScript mints from the RECORD have no input. The block route resolves every
   segment that exists and answers the recorded 404 for the rest - `0-user-prompt` is the pinned case.
2. **No prose-window segments.** TypeScript opens a `reasoning`/`assistant` block per window of
   consecutive deltas and patches it on each append (`turn-projector.ts:2044-2063`, `:1061`). This
   fold carries those bytes in the node and the section `tail` and mints the `assistant` segment only
   through the `done` fallback TypeScript uses when the window held no deltas
   (`turn-display.ts:373-376`). The `context:*` op vocabulary, the block id rule and the route are all
   in place, so widening the fold is a fold change, not a contract change.
3. **Block ids are one ordinal lower than TypeScript's.** The id rule is
   `${section.sections.length}-${kind}-${key}`, and TypeScript's array holds its prompt section at
   index 0 from the constructor. This fold has no prompt section, so the root flow's first tool
   segment is `1-tool-7` where TypeScript prints `2-tool-7`. Stable across live and replay, and
   internally consistent with this build's `s{ordinal}` section ids (T4's recorded deviation).
4. **A result-only frame settles no segment.** TypeScript's `updateToolBlock` returns when there is no
   block to update (`:1065`); the same rule here means a `tool_result` whose `tool_call` was never
   folded renders as a step row (T4's rule) and adds nothing to the bar.
5. **A pending segment is consumed even on a mismatch.** `pending_block` pairs a result with the
   segment the last call opened; when that segment belongs to another flow the entry is dropped rather
   than left to settle a later, unrelated result.

### Facts a later stage must not re-derive

* The listing rule lives in the catalogue (`UNLISTED` / `is_listed` / `listed_entries`), NOT in a
  per-row field: `ENTRIES` stays the generated manifest projection, and the two read surfaces
  (`summaries` for `/tools`, `catalogue_summaries` for a session toolbox) differ by exactly the four
  withheld names.
* The context bar is METADATA ONLY: no `text`, no `preview` on a segment. A segment names the
  recorded range `[eventFrom..=eventTo]`, and `api::turns::build_turn_block_detail` reads those bytes
  out of the journal. Anything that puts text on a segment breaks the wire bound the contract is built
  around (`turn-display.ts:397-410`).
* `display_chars` is the only character measure in this crate. A new bound that counts code points
  disagrees with TypeScript on any astral-plane text.
* A duplicate `POST .../tools` is a REPLACE, not a conflict, for a CREATED name; the 409 is reserved
  for a name owned by the catalogue or the harness, which this port cannot override.

### Verification (T7b's last run)

`cargo fmt -- --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test --no-fail-fast`
in `packages/typecrate/harness`: **261 passed, 0 failed** across 13 targets (56 lib, 5 bin, 15
`data_plane`, 23 `foundation`, 17 `http_host`, 17 `http_sessions`, 15 `projector`, 27 `store`, 4
`store_restore`, 21 `stream`, 24 `tools`, 24 `transport`, 13 `turn`).

`cargo check --locked --target wasm32-unknown-unknown` in `packages/typecrate/services` is clean, and
`node scripts/check-playground.mjs` is 122/122. `cargo test --locked` there is 71 tests with one
expected failure: `tests/harness_gateway.rs::health_the_empty_page_a_created_session_and_the_catalogue_reads_answer_through_the_gateway`
pins `GET /v1/harness/tools?limit=100` at **35** rows, and fix 4 makes the listed surface **31**. The
assertion belongs to a services test file, which T7b's brief did not put in scope - it needs the same
15 → 18 style fixture move as the playground checker (35 → 31 rows, `limit` 35 → 31, and the default
page's "25 of 31" cursor arithmetic).

The thirteenth finding - the lowest-priority group of the audit list - was not attempted: each item in
it needs an input channel the current seams do not carry (an agent-results registry behind
`TurnSummary.blocks`, the instruction-source snapshot reaching the projector, and the two remaining
`edit` matching tiers), and the stage's rule was to close the listed defects without redesigning.

## T9 - the runtime-support gate, the route-table pin, and the documentation honesty pass

An independent verification found three things: a `worker` execution mode that was accepted and
advertised but never provided (turns silently ran inline), no test that referenced the route table, and
documentation that had drifted. No architecture changed.

### What landed

| # | Change | TypeScript authority | Test |
| --- | --- | --- | --- |
| 1 | `runtime::validate_supported_execution(&ServerConfig)` + `runtime::SUPPORTED_EXECUTION_MODE`: a deployment whose `thread_execution` or `io_execution` is not `inline` is refused with a 400 that names the value AND the variable, and both knobs are reported in one pass. Called as step 2 of `build_runtime`, i.e. from `runtime_for`/`runtime_for_with` before the first filesystem read. | `workers/capability.ts`, `workers/io-client.ts:282`, `sessions.ts:1513-1516` - TS aborts on an unavailable isolation lane, it never downgrades to inline | `tests/runtime_execution.rs::the_default_inline_deployment_is_supported`, `a_worker_thread_lane_is_refused_with_the_support_sentence`, `a_worker_io_lane_is_refused_with_the_support_sentence`, `both_worker_knobs_are_refused_in_one_pass`, `anything_but_inline_is_refused_and_the_value_is_quoted` |
| 2 | `runtime::runtime_for_with(&Path, ServerConfig)` - the test/embedder seam - over a new private `cached_or_build`, so the "one build per root, a failed build is not cached" rule has ONE implementation. `build_runtime` now takes `(root, environment, config)` instead of reading the environment itself. | `runtime.ts:296-308` (a refused creation leaves nothing cached) | `tests/runtime_execution.rs::the_runtime_refuses_a_worker_deployment_before_touching_the_disk`, `the_runtime_refuses_an_io_worker_deployment`, `an_injected_configuration_passes_the_same_gate`, `the_default_deployment_still_starts_and_serves_the_mount` |
| 3 | The standalone executable refuses an unusable deployment BEFORE binding: `startup_support_check()` (config load + support gate) runs in `main` after `--help`, prints `typecrate-harness-server: <refusal>` on stderr, and exits 1 with no banner. `HELP` gained the RUNTIME EXECUTION section. | `start.ts` - the TS start throws on a bad runtime configuration, and node reports an uncaught startup failure as exit 1 | `src/bin/server.rs::the_default_deployment_passes_the_startup_support_gate`, `a_worker_deployment_is_refused_before_the_bind`, `help_states_that_a_worker_deployment_is_refused_before_the_bind`; end-to-end: `tests/runtime_execution.rs::the_standalone_executable_refuses_a_worker_thread_lane_before_binding`, `the_standalone_executable_refuses_a_worker_io_lane_before_binding`, `the_standalone_executable_starts_the_default_inline_deployment` |
| 4 | The gateway answer for a worker deployment is the refusal's own status: the runtime is resolved at dispatcher step 2, so EVERY mounted route (and even an unknown path) answers 400 until the environment is fixed. | `service.ts:515-528`, `:553-556` ("a rejected runtime creation should answer before any body work") | `tests/worker_execution_http.rs::a_worker_deployment_answers_every_mounted_request_with_400` |
| 5 | `tests/routes.rs` - the route table pinned against `shared/harness-api.ts:54-100` and `api.ts:3925-3974`. | the contract's own tables | 6 tests; see "The dispatcher order is the contract" in T5a for what each one pins |
| 6 | Documentation corrected: the services test count (71-with-one-expected-failure -> **96 passed, 0 failed**, `tests/harness_gateway.rs` pinning **31 LISTED** rows), the wasm claim (the harness lib is **excluded** from the wasm32 build - `cargo tree --target wasm32-unknown-unknown` resolves zero `typecrate-harness` packages - not "linked into" it), the standalone flags (`--host`/`-h`/`--port`/`--database-root`; `--addr` is the services gateway's spelling and THIS binary rejects it with exit 2), the default database root (`<repo>/../temporary/dump`, i.e. four levels up to the repo root and then one above it), the route-table owner (`tests/routes.rs`, not `tests/foundation.rs`), the tool counts (31 listed / 35 catalogue / 21 executable arms = 19 catalogue tools + `task` + `actionRequest`), the stream vocabulary (**18** display-op kinds, `harness-stream.ts:142` - not 64), the banner description (this bin has no LAN probe - that is the services banner), the shared-default-data-root warning, and `src/lib.rs`'s "STILL TO PORT" list (the `*-route.rs` wiring shipped with T6). `package.json`'s `#rust` note now describes a real listener. | - | the numbers in "Verification" below |

### Why the gate is NOT in `load_server_config`

`config::load_server_config` is a faithful port of `config.ts`, and it is also the channel that
REPORTS a deployment: `GET /config` prints `runtime.thread_execution`, and the config tests feed it
explicit environment maps. Refusing `worker` inside the parser would make the value unreportable and
would break the loader's contract for every caller that only inspects settings. So acceptance stays
the parser's business and SUPPORT is a separate, named gate in `runtime.rs` - which is also the only
place that knows what this build actually provides.

### Facts a later stage must not re-derive

* A refusal from the gate is a `bad_request` (400), and a failed runtime build is never cached
  (`cached_or_build`), so a fixed environment starts on the next request without a restart.
* The standalone exit code for an unusable deployment is **1** (could not serve), not 2 (bad usage):
  the flags were valid, so restarting will not help and the environment edit will.
* Tests cannot write the process environment - `set_var` is `unsafe` in edition 2024 and
  `[lints.rust] unsafe_code = "forbid"`. Everything therefore either passes an explicit map to
  `load_server_config`, injects a `ServerConfig` through `runtime_for_with`, sets the CHILD's
  environment via `Command::env` (`CARGO_BIN_EXE_typecrate-harness-server`), or points
  `environment::set_harness_environment_path` at a `.env` fixture.
* The `.env` route leaks: dotenvy fills the PROCESS environment when it loads a file, and nothing can
  undo it from safe code. That is why `tests/worker_execution_http.rs` is its own test binary with one
  test - the leak cannot reach a test that reads `HARNESS_*`.
* `contract::RuntimeSettings` still reports whatever the deployment stated, including `worker`: the
  gate stops the START, it never rewrites the reported configuration.

### Verification (T9's last run)

`cargo fmt -- --check`, `cargo check --all-targets`, `cargo clippy --all-targets -- -D warnings` and
`cargo test --no-fail-fast` in `packages/typecrate/harness`: **283 passed, 0 failed** across 16 targets
(56 lib, 8 `bin/server`, 15 `data_plane`, 23 `foundation`, 17 `http_host`, 17 `http_sessions`,
15 `projector`, 6 `routes`, 12 `runtime_execution`, 27 `store`, 4 `store_restore`, 21 `stream`,
24 `tools`, 24 `transport`, 13 `turn`, 1 `worker_execution_http`).

In `packages/typecrate/services`: `cargo fmt -- --check`, `cargo clippy --all-targets --locked --
-D warnings` and `cargo check --locked --target wasm32-unknown-unknown` are clean, `cargo test --locked
--no-fail-fast` is **96 passed, 0 failed** (13 lib, 15 `bin/server`, 1 `api`, 29 `discovery`,
8 `harness_gateway`, 30 `server_http`), and `node scripts/check-playground.mjs` is **122/122**. T7b's
"71 tests with one expected failure" paragraph above is the state AT THAT RUN; the `harness_gateway`
35-row pin has since moved to the 31 LISTED rows, which is what the gateway test asserts today.

Smoke check of the standalone abort, with `HARNESS_THREAD_EXECUTION=worker` in the shell:

```text
typecrate-harness-server: Invalid runtime settings: thread_execution "worker" is not supported in this
Rust build; only "inline" is available. Unset HARNESS_THREAD_EXECUTION or set it to "inline" - this
build never runs a "worker" deployment inline silently.        (exit code 1, banner never printed)
```



---


## T10 - the plugin uplift (`plugins`, `api/plugins`, `store`, `persistence`, `contract`, `config`)

The server contract moved under this port: `@underload/harness` gained the Harness Plugin System
(the `plugins` / `plugins/{plugin_id}` pair of `shared/harness-api.ts:67-68`, between
`toolsByToolIdExecutions` at `:66` and `models` at `:69`, plus `server/plugins.ts`; and a new provider
default model at `server/config.ts:111`). The reference
package is UNTRACKED in this repository, so the uplift cites files and lines, never commit ids -
those are the only stable cross-references available for `packages/underload/**`. The Rust harness is the
server the underload front-end talks to, so the front-end's Plugins pane was dead against this
endpoint: two routes, five DTOs, the boot link, and the durable overrides were missing. T10 closes the
gap and fixes three cheap contract drifts the same uplift carries.

### What the uplift is, in one paragraph

`GET /v1/harness/plugins` answers the built-in catalogue - each row carrying the plugin's OWN declared
metadata beside the values currently in force - and `PATCH /v1/harness/plugins/{plugin_id}` edits those
values, answers the whole row again, and is durable before the answer. There is no enable/disable
verb, no stream update kind, and no per-plugin subresource beyond the row: plugin loading is
auto-scanned in TypeScript, so the only thing an operator can change is a setting.

### The decisions a later stage must not re-break

* **The catalogue is a `static`, not a scan.** `src/plugins.rs::BUILTIN_PLUGINS` is the Rust shape of
  the GENERATED `src/plugin/manifest.ts`: the same two rows, in scan order (`async-ping` before
  `context-compaction`, path ascending), with the metadata each `*.plugin.ts` declares copied verbatim.
  Order is contract-visible - the page renders in it and the 404 sentence joins those names - so a
  reordering is a wire change, not a refactor. It is `static` rather than `LazyLock` because `Value` is
  not const-constructible and the descriptor defaults are `DeclaredDefault`; the price is one enum, the
  gain is that no request can ever see a different catalogue than the next.
* **No hook executes here.** There is no plugin host in this crate, so `hooks` is server-visible
  metadata and the two built-ins do not ping or compact. `README.md` Deviations item 16 states it, and
  the port deliberately does NOT grow an execution engine to make the row "true": the contract the UI
  consumes is metadata + values + validation + durability.
* **The plugin store lives inside `StoreInner`.** `state.json` has exactly one writer
  (`Store::database_of` → `persistence::write_harness_database`), so owning the overrides inside the
  guarded store is what makes a settings patch durable in the SAME atomic commit as the sessions
  around it - and it means a session mutation can never silently drop the operator's overrides. A
  second store with a second writer would let the two documents race.
* **The revision is the store's counter, not a hub counter.** `plugins` is not a `StreamResourceKind`
  in the contract, so `GET /plugins` quotes `Store::plugin_revision()` (one bump per accepted patch,
  starting at 0, never persisted: `plugins-api.test.ts` proves a restart keeps the VALUE and resets the
  counter). No publish, no stream frame: the pane rereads on open and on save.
* **The list route stamps NO `X-Harness-Revision`.** `api.ts getPlugins` calls `json(200, pageMeta(...))`
  with TWO arguments - the figure rides the page body only - exactly like `GET /harnesses`
  (`api.ts:1503-1509`), which this port already pinned at T7b. `patchPlugin` DOES pass the third
  argument (`:2020-2022`), because there the figure is the result of the write. The brief left this
  open ("whether the plugin list stamps the header"); the TypeScript source and its test
  (`expect(page.headers).toEqual({ [EPOCH]: epoch })`) settle it.
* **`null` means reset, and a reset is a write.** An explicit `null` drops the key's override so the
  DECLARED default applies again, and it bumps the counter like any other accepted patch. A plugin
  whose overrides are all gone keeps no entry, so `state.json` carries neither `{}` nor `null` - and
  with no overrides left at all, the `pluginSettings` member is omitted entirely.
* **Validation lives at the write edge; the read edge stays permissive.** `min`/`max` are enforced
  because they are the bounds the plugin published; `step` NEVER is, because refusing `1500` on
  `step: 1000` would make the API stricter than the runtime that has to live with the value. A numeric
  STRING is accepted and normalized to a NUMBER (a query parameter or a form legitimately carries one,
  and TypeScript's read path accepts it too); a string setting accepts a string only; a boolean setting
  accepts a boolean or the two words, trimmed and lower-cased.
* **A patch is all-or-nothing, and the store refuses before it mutates.** One bad key refuses the
  whole document, moves no counter, and writes nothing - the same rule `PATCH /config` follows. A
  plugin lookup comes FIRST: an unknown id is a 404 even when the body is also wrong.
* **A failed durable write leaves the value applied and answers 500.** `sessions.ts:1848-1851` states
  why: every running session was handed the value the instant it was applied, so rolling it back would
  report a failure the process does not have. `tests/plugins.rs` pins it by turning the rename target
  into a directory.
* **`pluginSettings` is additive and does NOT bump `HARNESS_DATABASE_VERSION`.** Still `1`. The loader
  is STRICT about shape - a nested object or an array names the exact slot and aborts the boot, which
  is the persistence rule for every other member of this document - and LENIENT about identity: a
  plugin or a key this build no longer declares is dropped by the restore, as is a stored value that
  now violates its bounds. Empty maps normalize to absent on load, because the writer never emits `{}`.
* **Settings PATCH durability goes through `runtime.save()`, not `store.save()`.** The store's own
  commit serializes the session half (`global_settings: None`, `deleted: None`); the host write adds
  the registry, the tombstones and the operator's provider pair. A settings patch must not drop those,
  so it uses the same whole-document path `PATCH /config` and `PATCH /harnesses/{name}` use, under the
  documented lock order (the store call releases its guard, then the host write takes its own).

### The three cheap contract corrections (R7)

| Drift | Contract | This build after T10 |
| --- | --- | --- |
| `limits.tool_result_bytes` | `262144` (`harness-api.ts:216`, `harness.swagger.yaml:243`) | the boot document publishes `contract::PUBLISHED_TOOL_RESULT_BYTES = 262_144`; the ENFORCED budget stays `MODEL_TOOL_RESULT_BUDGET_BYTES = 65_536` until the tool stage widens (Deviations item 17), and both figures are asserted so the gap cannot drift |
| `ThreadSummary.status` | `"idle" \| "running" \| "paused" \| "stopped"` (`harness-api.ts:1481`) | `ThreadLifecycle::Paused` exists, `store::lifecycle_str` / `store::read::parse_lifecycle` carry the word, and a `paused` record no longer collapses into `Idle`. The `?status=` FILTER vocabulary stays the contract's three (`api.ts:3497`), so `?status=paused` is still a 400 there |
| `HarnessInstructionSummaryEntry.source` | `"catalogue" \| "global" \| "harness" \| "session"` (`harness-api.ts:451`) | `InstructionSource::Session` exists, so a session-defined member's provenance parses instead of failing the read. This port models provenance as ONE enum, so the two narrower TypeScript members (`:547`, `:594`) accept the extra spelling on the way in and never emit it - an accepted superset, stated on the enum |
| default model | `local/basalt` (`config.ts:105-111`) | `config::DEFAULT_MODEL`, the boot reset target of `PATCH /config {"model": null}`, and `apply_config_patch`'s reset all moved with it; the base URL is unchanged |

### Test inventory

* `tests/plugins.rs` - 15 real-HTTP tests over the library router: the exact catalogue rows (all nine
  members, both descriptors, the effective values), paging + `q` (including the hooks are NOT in the
  haystack, and a foreign cursor is the 400), the two 405 sentences with a percent-decoded path, the
  write with its header revision and its `state.json` delta, numeric-string normalization and integer
  spelling, the `null` reset, every refusal sentence (unknown plugin / key / value / empty patch /
  unknown body field / non-object body, with "a refusal wrote nothing"), the non-object `settings`
  spread shapes, three boots of durability, the lenient restore (retired plugin, retired key, foreign
  key, out-of-bounds value), the strict shape (both slot-naming messages), the 500-with-value-applied
  write failure, and the store projection the routes are built on.
* `src/plugins.rs` `mod tests` - 10 unit tests for the rules the SHIPPED catalogue cannot reach over
  HTTP: the integer-spelling helpers, `Number()` coercion (including `0x10` / `0o17` / `0b101` / a
  blank text), the `string` and `boolean` branches (neither shipped plugin declares one, so an HTTP test
  would be fiction), `max` enforced with `step` NOT enforced and both bounds inclusive,
  all-or-nothing, the `null` reset dropping an empty plugin map, catalogue-ordered snapshots, and
  restore leniency.
* `tests/routes.rs` - 6 tests, now pinned at 47 templates / 66 operations with the new verb split, and
  the two plugin templates at their contract positions between `toolsByToolIdExecutions` and `models`.
* `tests/foundation.rs` - the published-vs-enforced budget pair, `local/basalt` as the loader default
  and the advertised `defaults.model`, and `the_uplifted_vocabularies_are_accepted_not_folded` for the
  four thread statuses and the four instruction sources (parse AND print, and the store's two
  lifecycle directions round-trip them).
* `tests/http_host.rs` - the boot document's `limits` and its ten `links`, with `plugins` among them.

### Follow-ups this stage deliberately left open

1. A plugin HOST (a module loader plus the hook bus) would make `async-ping` and
   `context-compaction` actually run, and would then need the live provider that
   `sessions.ts buildHarness` hands every harness. Nothing in the HTTP contract changes when that
   lands, which is the point of porting the surface first.
2. Widening `MODEL_TOOL_RESULT_BUDGET_BYTES` to the contract's 256 KiB belongs to the tool stage: it
   changes what a model is shown, and `tests/tools.rs`'s budget envelope moves with it.
3. A session-defined created agent would make `InstructionSource::Session` a value this build EMITS,
   not merely accepts - the same hole "still to port" already records for the created-tool module host.

### Verification (T10's last run)

From `packages/typecrate/harness`: `cargo fmt -- --check`, `cargo check --all-targets`,
`cargo clippy --all-targets -- -D warnings` and `cargo test --no-fail-fast` are all clean - **309
passed, 0 failed** across 17 targets (66 lib - 56 before, +10 for `src/plugins.rs`'s write-edge units;
8 `bin/server`, 15 `data_plane`, 24 `foundation`, 17 `http_host`, 17 `http_sessions`, **15 `plugins`**
(new), 15 `projector`, 6 `routes`, 12 `runtime_execution`, 27 `store`, 4 `store_restore`, 21 `stream`,
24 `tools`, 24 `transport`, 13 `turn`, 1 `worker_execution_http`). The T9 baseline was **283 passed,
0 failed**, so every one of those 283 still passes with the default model, the published budget and the
route table moved underneath it.

`packages/typecrate/services` was NOT touched by this stage and needs no change: its
`endpoint/harness/harness-route.rs` bridge mounts `ANY /v1/harness/{*rest}`, so the two new routes are
reachable through the gateway as soon as the harness crate builds - `cargo check --lib` in that package
compiles the uplifted harness cleanly. Its own `cargo test` currently fails to BUILD for a reason
unrelated to T10: the working tree's uncommitted `src/bin/server.rs` has a `match`-arm type error in the
SIGTERM handler (`Option<()>` vs `()` at `src/bin/server.rs:633-639`). That file is outside this
stage's ownership, so the failure is reported rather than fixed.

## Contract resync - the published display lane (`ContextBlock.lane`) - 2026-11-11

Issue 0017 (`packages/underload/harness/docs/issues/minor-rust-port-context-lane-drift.md`) recorded
the display lane as the ONE published field this port had not caught up with. Its 2026-11-11 update
claimed steps 2-4 of the proposal had landed and only the contract field was left; the field had not
landed and neither had the event half of step 3. This section is the spec-sync record criterion 1 asks
for: what the field is, where each TypeScript rule now lives in Rust, and the facts a later stage must
not re-derive.

### What landed

* `contract::ContextBlock::lane: Option<u64>` (`src/contract.rs:3029-3076`, the field at `:3050`),
  `#[serde(default, skip_serializing_if = "Option::is_none")]`, positioned between `section_id` and
  `name` - the position the generated TypeScript uses (`src/shared/harness-api.ts:3299-3320` puts
  `lane?` at `:3320`, directly after `sectionId` at `:3309`) and the order
  a TypeScript-written document already carries. Swagger `harness.swagger.yaml:8122` publishes
  `integer` + `minimum: 0` + "absent means lane 0", so the Rust spelling is an `Option`: `None` is what
  "the producer said nothing" looks like, and `LaneStamp::read_as` is what turns it into the number 0.
  A `TurnBlockDetail` flattens a `ContextBlock` (`src/contract.rs:3097`), so the block-detail route
  round-trips the lane with no extra work. The lane is NOT a patch key: the fold's `context:set` path
  builds its patch as a map of the mutable fields only (`src/projector.rs:1448-1471`), and this crate
  has no `ContextBlockPatch` struct to grow.
* `src/projector.rs` - the lane is now stamped at the fold's ONE `context:add` mint point
  (`add_block:961-980`, through `with_lane:450-463`). Before this pass `with_lane` had exactly one
  caller, `add_block_with_lane`, which is the NON-event seam: the turn's starting lane reached the
  prompt, instruction and steer segments (the three seams at `:516`, `:547`, `:576`), while the two
  event-driven mint sites (`tool`, `:1343`; the `done` answer fallback, `:1593`) called `add_block`
  directly and so minted an UNLANED segment. The consequence was visible in the recorded fixture: a
  post-compaction turn's prompt sat on row 1 while
  its tool segments, whose events carried `lane: 1`, sat on row 0 - the exact live-vs-replay divergence
  the lane exists to prevent. `add_block_with_lane` (`:587-598`) now swaps `event_lane` for the turn's
  lane, delegates to the same mint point, and restores the previous value, which is what keeps the two
  sources (`turn-projector.ts:1435 laneOfEvent` vs `:1441 laneOfTurn`) from being conflated while
  leaving exactly one writer.
* Nothing else moved. A lane-0 or unlaned fold still writes no key, so the 15 `projector`, 17
  `http_sessions`, 27 `store`, 4 `store_restore`, 21 `stream` and 13 `turn` tests - every one of which
  folds lane-free scripts and several of which assert the ABSENCE of a `lane` key
  (`tests/http_sessions.rs:1329-1330`) - pass unchanged.

### The fixture, and what it can and cannot prove

`tests/fixtures/lane-compaction/` vendors the recorded bytes the issue names
(`Temp/fixtures/harness/lane-compaction/`, git-ignored and therefore disposable): `MANIFEST.md`,
`lane-evidence.json`, `turn-displays.json`, `threads-after.json`, `durable/state.json` and
`durable/events.d8aa98d7-1886-4d3f-a6db-f4869a6623d4.jsonl` - about 250 KiB, the same vendoring move
`src/plugins.rs:47-50` describes for the plugin-catalogue fixture. A test that compared Rust output with
`Temp/` would pass on the authoring machine and vanish in a fresh clone.

The fixture proves the two-row case: the seed turn's `0-system-instructions` / `0-user-prompt` carry no
lane key (that turn started on lane 0) while the segments its lane-1 events opened carry `lane: 1`, and
the compaction turn - `turns[1].lane = 1`, `core.threads[0].contextLane = 1` - carries a WRITTEN `1` on
every segment including its prompt. This build mints 4 of the recorded 10 segments and 2 of the
recorded 4: a streamed `reasoning`/`text` window becomes a prose NODE here, not a bar segment per
window (`README.md` "Context bar", `src/projector.rs:1301-1305` folding deltas through `fold_prose` and
the answer-segment note at `src/projector.rs:1568-1573`), so the six prose segments are absent
from its output. Parity is asserted over every segment the fold DOES mint, ids and lanes included, and
the recorded tables are pinned as literals in the test so a swapped fixture cannot quietly weaken the
comparison.

### Tests

`tests/context_lane.rs` - 11 tests: the contract decode/encode both ways (present, absent, `0`
accepted, negative / fractional / text / boolean refused, and the same through the flattened
`TurnBlockDetail`); the journal's 337 records read back with their 336 stamps and re-print byte-identically;
the checkpoint's per-turn and per-conversation lanes plus the round-tripped shape; the fold's lane table
against the recorded displays for BOTH turns; the restored store's replay of the same document, the
conversation counter a restarted recorder reads (`context_lane_of`), and the events page's verbatim
stamp; the live `context:add` ops (stamped, and no `context:set` patch ever carries a lane); the
lane-free fold reproducing the lane-bearing fold byte for byte with the lane keys removed; and the
snapshot reader's accept/refuse pair with the written-`0` normalization.

### Facts a later stage must not re-derive

1. `serde_json::Map` under `preserve_order` removes a member with a SWAP-REMOVE: the LAST member moves
   into the freed slot. A test that removes a `lane` key to compare against a pre-lane document must
   REBUILD the map (`tests/context_lane.rs::strip_lane_keys`), or the comparison measures the removal
   mechanics instead of the fold.
2. The TypeScript journal line is `{type, v, turnId, index, at, event}` (see the fixture's
   `durable/events.*.jsonl`), while `journal::journal_event` (`src/journal.rs:104-112`) writes
   `{version, sequence, timestamp, event}` and `HarnessJournal::read` filters on `sequence` - so a
   TypeScript-written journal reads back as ZERO records here. The fixture's events load through
   `state.json`'s per-turn `events` array, which is the same `SessionTurnEvent` shape; the `.jsonl` half
   is exercised at the record-decode level only. Do not claim journal-file interoperability from these
   tests until that envelope is ported (open item 3 below).
3. `store::restore_threads` (`src/store/mod.rs:2469-2478`) is TOLERANT by design: `core_of` swallows a
   failed decode, so a `core.threads` carrying a bad `contextLane` restores as an EMPTY roster rather
   than refusing the load. TypeScript's `core/snapshot.ts:758-763` `readThreads` THROWS. What this build
   does refuse is the lane at the typed record boundary (`persistence::HarnessThreadRecord::context_lane`
   is a `u64`, so `-1` / `2.5` / `"2"` are decode errors) and the per-turn lane in
   `persistence::validate_document` (`.lane must be an integer >= 0.`, `:1194-1198`).
4. The doc comment at `src/persistence.rs:846` attributes the `contextLane` sentence to
   `persistence.ts:1201`. TypeScript's `server/persistence.ts` has NO `contextLane` clause at all (zero
   matches), and the rule that does exist lives in `core/snapshot.ts:758-763` under the label
   `snapshot.threads` (`:950`), not `core.threads`. The cite is stale; the behaviour it claims is real
   but lives one module over.
5. A conversation row publishes no lane and no `contextLane` (the fixture's own `threads-after.json`
   row is pinned whole in the test to prove it). Restored threads from a TypeScript document are also
   invisible to `SessionState::host_thread_records` (`src/store/read.rs:132-138`), which filters on
   `origin == "host"` while `readThreads` never reads an `origin` - use `store::restore_threads` when a
   test needs the durable roster of a foreign-authored session.

### Follow-ups this pass deliberately left open

1. **Proposal step 5, blocked not skipped.** The summarize pass is the only producer of a lane bump
   (`src/plugins/context-compaction.plugin.ts:1059-1113`), and no plugin hook executes in this build
   (T10, "NO plugin hook executes"), so nothing in the port can RAISE a lane: it can carry, stamp,
   persist, fold and publish one.
2. **Prose bar segments.** Until a streamed prose window mints its own segment, a lane-1 turn's Rust bar
   holds only the tool and non-event segments, and parity against a TypeScript display covers that
   subset. The recorded lanes of the prose segments are pinned in `tests/context_lane.rs` so the port
   that adds them has the expected values in front of it.
3. **The journal envelope** (fact 2 above): porting `{type, v, turnId, index, at, event}` is what turns
   "the same records, decoded from a different file" into "the same file".
4. **`restore_threads` strictness.** Making it refuse like `readThreads` is a signature decision
   (`Vec<_>` today, tolerant by design); this pass recorded the difference and pinned both halves.

### Verification (this pass's last run)

From `packages/typecrate/harness` with `TMPDIR` pointed at repo-root `Temp/T7` (cargo 1.99.1 /
rustc 1.99.1):

- `cargo fmt -- --check` - clean.
- `cargo test --no-fail-fast` - **414 passed, 16 failed** across 20 targets. Every one of the 16
  failures is the plugin-catalogue WIP that was already in the tree before this pass: 3 in
  `unittests src/lib.rs` (`plugins::tests::a_restore_drops_what_this_build_cannot_use`,
  `a_rejected_patch_leaves_the_store_untouched`,
  `the_reserved_enabled_key_switches_a_plugin_that_never_declared_it`) and 13 in `tests/plugins.rs`.
  Their assertions are catalogue-shaped, not lane-shaped: `left: Number(3) / right: 2` for the row count
  (`context-trimmer` is an uncommitted addition - zero matches for that id under `git grep
  "context-trimmer" HEAD -- src/plugins.rs`, `src/plugins.rs` is +584/-87 against HEAD while
  `tests/plugins.rs` is UNMODIFIED against HEAD), the "This build ships:" sentence now naming three
  plugins, and `context-compaction` answers carrying `modelLimits` / `enabled` / `minTokens` /
  `safetyTurns` / `mode` plus a `contextLimit` default of `200000`. No failure message mentions a lane.
- The lane target on its own: `cargo test --test context_lane` - **11 passed, 0 failed**.
- Green after this pass, with these counts: `bin/server` 8, `contract_message_flags` 9, `data_plane` 15,
  `foundation` 24, `http_host` 17, `http_sessions` 17, `projector` 15, `routes` 6,
  `runtime_execution` 12, `store` 27, `store_restore` 4, `stream` 21, `tools` 24, `transport` 24,
  `turn` 13, `wave_one` 10, `worker_execution_http` 1.
- `cargo clippy --all-targets -- -D warnings` under rustc 1.99.1 - still SIX findings, at
  `src/core_pruning.rs:76,80,331,541,778` and `src/plugins.rs:904,1598`; none in a file this pass
  touched. They are toolchain-version findings in that stage's own WIP files and were left for that
  stage.
