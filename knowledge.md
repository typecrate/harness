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
3. Still to port - the domain store (`sessions.ts`, the `protocol.ts` session records, the turn
   projector), the agent runtime (`harness.ts`, `core/**`), the tool catalogue (`tools/**`), the
   transports (`transport/**`), the stream (`stream/**`), the HTTP layer (`api.ts`, `handler.ts`,
   `service.ts`) and the `*-route.rs` discovery wiring.

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

