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
   standalone executable (`bin/server.rs`). The session-domain routes are reachable stubs; their
   bodies land with T5b. See "T5a" below.
6. Still to port - the WebSocket stream (`stream/**`, T5c), the session-domain route bodies
   (`api.ts:1958-2820`, T5b), and the `*-route.rs` discovery wiring, plus the runtime-compiler
   tools the catalogue lists but this build cannot execute (see "T4").

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
* `initialize` does not start a lane pump. The durable queue is the LEGACY array
  (`mod.rs:1502`), so a per-lane backlog comes back as unclaimed `queued` rows and drains only when
  the next submit starts that lane (`tests/store_restore.rs`).

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
  (`turn::Run::request_messages`). `max_iterations: 0` means "use the library default" (16), the
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
is named.

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
   instruction, but one shared helper is the right end state.
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
