# typecrate/harness ↔ underload/harness parity

Status record for the **delivery surface** of the Rust port: the behaviours the TS reference serves
around its API mount (`packages/underload/harness/src/server`) and the probe surface the executable
adds on top of them. The goal of the crate is a one-to-one Rust replacement of
`packages/underload/harness`, so every row below states the TS authority it is ported from, or says
plainly that it is a **Rust-side extension** with no TS counterpart.

## Authority, and where it can move under you

* **Contract**: `packages/underload/harness/src/harness-server.swagger.yaml` — the OpenAPI document.
  There is no `harness-server.ts`; the contract is the yaml and nothing else.
* **Runtime**: `packages/underload/harness/src/server/*.ts` for the HTTP/stream surface and
  `packages/underload/harness/src/engine/**` for what the routes ask the kernel to do. Where the two
  disagree, the yaml is quoted as the promise and the `.ts` as the behaviour, and the disagreement is
  written down under "Deltas" rather than resolved silently.
* **Commit**: the reference tree this parity was derived from is
  `e66bce87777cb5f53504dbc47ad38ebd2c0ca6ff`. `packages/underload/harness` is a **nested repository**
  that the enclosing workspace reports as untracked (`?? packages/underload/harness/`), so its HEAD
  can move without a single diff appearing in the outer `git status`. Re-run `git -C
  packages/underload/harness rev-parse HEAD` before trusting any `path:line` in this file; every line
  reference below was read at the commit above.

## Closed

| Surface | Rust | TS authority | Behaviour pinned |
| --- | --- | --- | --- |
| Static mirror (media type, cache, size, HEAD) | `src/static_files.rs` | `static.ts:12` (`MIME_TYPES`, 11 entries), `:27` (`application/octet-stream` default), `:47-49` (assets-dir rule), `:51` (`Content-Length` from `stat`), `:53-56` (HEAD ends without a body) | `public, max-age=31536000, immutable` **only** for a path containing `${sep}assets${sep}`; everything else `no-cache`; size header equals the file's own byte length |
| Mirror path resolution | `src/static_files.rs` (`StaticMirror::resolve`, `percent_decode`) | `static.ts:72` (GET/HEAD only), `:83` (`/` → `/index.html`), `:84` (resolve), `:87` (containment), `:86,93-96` (traversal, SPA fallback) | `%2e%2e%2f` never leaves the root; an extensionless unknown route falls back to `index.html`; an unknown **asset** stays a 404 instead of becoming HTML |
| Verb handling outside the mount | `src/server.rs` (`delivery_fallback`) | `static.ts:72` declines a write verb; `start.ts:409` then answers | `POST/PUT/PATCH/DELETE/OPTIONS` on a mirror path is the terminal plain 404, never a 405 and never a body |
| Terminal 404 of a delivery listener | `src/server.rs` (`plain_not_found`) | `start.ts:165-174` | `404` + `text/plain; charset=utf-8` + body exactly `Not found`; it does **not** name the mount |
| Ownership of non-mount paths | `src/server.rs` (`delivery_fallback` prefix test) | `handler.ts:277` — the static handler owns everything outside the mount, decided by `pathname.startsWith(STANDALONE_MOUNT)` | the mount wins first, so `GET /v1/harness/health` stays a dispatcher answer even when the mirror contains a `v1` directory; prefix-shaped non-mounts (`/v1/harnessXYZ`) still belong to the dispatcher |
| Dispatch order | `src/server.rs` (`router_with`) | `start.ts:404-414` (`api → serveStatic → sendNotFound`) | **api → mirror → plain 404**, and nothing else. The reference has no docs route, so neither does the crate: a mirror file named `docs` is served by the mirror, and with no mirror configured `GET /docs` is the plain 404 |
| Listen/serve startup | `src/bin/server.rs` (`run`), `src/server.rs` (`serve`, `serve_on`, `serve_with`, `serve_on_with`) | `start.ts:400-441` | bind **one** port, then read the bound address back (`start.ts:439-440`), then serve; shutdown drains the listener before the final state write, and `server::flush` is the port of `dispose()` + `drainIo()` (`start.ts:319-340`) |
| Listen config | `src/listen.rs` | `start.ts:160-161` (defaults), `dev.ts:96-106` (port rules), `src/server/cli/index.ts:377-380` (exit code) | flag → env → default precedence; defaults `5181` / `127.0.0.1`; `auto` = port `0`; a non-integer or out-of-range port refuses with `invalid port: <what you typed>` (`dev.ts:101-105`) |
| Host values | `src/listen.rs` (`ListenConfig::address`) | `start.ts:436` — TS hands the host string to `http.createServer`, which resolves it | a **hostname is legal** (`--host localhost`, `--host example.invalid`): the name is resolved with `to_socket_addrs` at bind time, preferring an IPv4 answer, and an unresolvable name is the bind-time refusal that exits `1`. Resolution is lazy, so `--help` still prints without a network |
| Probe surface | `src/bin/server.rs` (`parity`), `src/manifest.rs` (`parity-manifest`) | none — a Rust-side addition | one `PARITY-LISTEN <json>` line on stdout, nothing else on stdout, clean SIGTERM drain |
| Parity manifest | `src/manifest.rs` | none — a Rust-side extension | the route table the router mounts, plus the delivery arms a listener actually owns: `delivery` keys are `not_found` and `static_mirror`, in that order |
| Stream frame plane | `src/stream/broker.rs`, `src/events.rs` | `server/stream/broker.ts`, `engine/shared/harness-stream.ts:56,62` | **6 client message types** (`hello`, `subscribe`, `unsubscribe`, `ack`, `ping`, `bye`) and **7 server message types** (`welcome`, `subscription_state`, `update`, `resync_required`, `error`, `heartbeat`, `pong`), and the 16 `update` kinds are the contract's list, in the contract's order (`contract::HARNESS_WS_UPDATE_KINDS`) |
| Registry publishes | `src/store/agents.rs`, `src/api/session_tools.rs` | `api.ts:4143-4146,4280,4323-4326,4365` (`agent.registry_updated`), `api.ts:4686,4841,5085` (`tool.registry_updated`) | a roster write publishes `agent.registry_updated` with `{agent_id, fields}` (`action: removed` on a DELETE, no `fields`); a tool verb publishes `tool.registry_updated` with `{name, created}` (registration), `{name, enabled}` (the enable-only PATCH) or `{name, removed: true}` (unregister). A PATCH that changes the declaration **and** the switch publishes **one** frame, because the reference's enable half is folded into the registration path (`api.ts:4955-4970`) |
| Thread-cap status | `src/store/threads.rs` (`thread_cap_roster_error`, `thread_cap_submit_error`), `src/store/turns.rs` (`open_host_thread`) | `engine/harness.ts:8052-8056` (the sentence), `server/api.ts:5277-5281` + `server/handler.ts:319-320` (the unwrapped 500), `server/sessions.ts:10318-10338` (the 429 mapping) | ONE runtime sentence, TWO codes, because only one route classifies it. An **unfocused** submit answers **429** with `Could not open a new thread: This session already has <n> independent threads; delete a terminal one before creating another (maximum 256).` (`sessions.ts` wraps it by regex). `POST {root}/sessions/{id}/threads` wraps nothing, so the throw reaches the listener's uncaught handler and answers **500** with the same sentence UNWRAPPED, as `{error:{message}}` — the swagger's create-route arms are 201/400/404/413/500, with no 409. A 409 on the create route was the crate's own classification and is gone. |

## Deliberate deltas

Each of these is a place where the crate states something the reference states differently. They are
not open work; they are the record of a disagreement, with the authority that produced it.

1. **`ids` on an invalidation frame vs the payload on the wire.** The contract types
   `tool.registry_updated` and `agent.registry_updated` as "pure invalidation with a non-empty `ids`
   list" (`harness-stream.ts:801-812`, `:887`), but the reference's own publish sites pass a
   **payload** (`{agent_id, fields}` / `{name, created}`), and `server/stream/broker.ts:551-578`
   spreads that payload into the frame with no projection to `ids`. The reference therefore never
   emits `ids` for these kinds. The crate delivers the payload verbatim, exactly as the reference
   delivers it, and `tests/stream.rs` pins the delivered shape. Changing either side is a contract
   edit, not a porting fix.
2. **What the cap counts — and when that can start to matter.** The reference's bound filters
   `origin === 'host'` rows (`engine/harness.ts:8047-8050`) and so leaves its own per-agent DEFAULT
   conversations out of the count, while the crate's gate compares `state.threads.len()` over every row
   it holds. Retired (inactive) rows are NOT part of this: the reference filters them out at
   `engine/harness.ts:8049` and the crate never stores one (`detach_thread` removes the row), so that
   source contributes exactly zero. The rows that would differ are `origin: 'agent'` conversations, and
   the crate cannot produce one over HTTP today — the only writer is `SessionState::resolve_thread`,
   which has no callers, and `POST {root}/sessions/{id}/threads` forces `origin: "host"` itself
   (`src/api/threads.rs:134-137`, behind the `CREATE_THREAD_FIELDS` gate at `:44`). **The observable
   difference right now is 0 on every route.** It is a forward-looking gap: once default conversations
   are materialized, the crate refuses EARLY by at most one row per agent and can never refuse LATE. A
   client that must run against both treats the sentence, not the number, as the signal.
3. **A relative `--static-root` is refused, not resolved.** TS `start.ts` silently `path.resolve`s a
   relative root; the launcher exits `1` naming the value it refused
   (`static root must be an absolute path`) rather than guessing the launch directory.

## Body refusals that still quote the serialiser (open, bounded)

The reference has a SENTENCE for every client refusal and even classifies some statuses by matching
its own sentences by regex (`server/sessions.ts:2036-2051` `attachmentErrorStatus`,
`:10326-10338` `threadError`), so a body that prints the serialiser's wording is a wire difference
even when the status is right. Wave W8 fixed the two routes the review probed and then ENUMERATED the
rest rather than chasing all 52 routes.

**Fixed here (`POST {root}/sessions`)**: the handler no longer decodes the body through
`contract::decode_body`, so no shape on that route can print serde. A non-string `title` is now
IGNORED, as the reference's `typeof` guard does (`api.ts:3421`) — it used to be a 400 quoting
`invalid type: integer \`7\`, expected a string` — and `settings: null` is now the reference's refusal
`"body.settings" must be an object.` (the reference skips only `undefined`, `api.ts:605-608`, and the
block label is the path the reference passes, not the bare word `body`).

**Remaining sites — 9 total, 7 on routes, 5 proven live (7 route → message pairs across 5 routes):**

| # | Route | Site | Internal text a client can read |
| --- | --- | --- | --- |
| 1 | `POST {root}/sessions/{id}/tasks` | `src/api/sessions.rs:348` | `` missing field `id` `` (an attachment with no `id`) |
| 2 | `POST {root}/sessions/{id}/tasks` | `src/api/sessions.rs:348` | `` missing field `turnId` `` (a `references[]` entry with no turn) |
| 3 | `POST {root}/sessions/{id}/tasks` | `src/api/sessions.rs:348` | `` invalid type: integer `7`, expected a string `` (`thread_id`) |
| 4 | `PATCH {root}/sessions/{id}/tools/{tool_id}` | `src/api/session_tools.rs:294` | `` invalid type: integer `7`, expected a string `` (`description`) |
| 5 | `POST {root}/harnesses` | `src/api/harnesses.rs:139` | `` invalid type: integer `7`, expected a string `` (`title`) |
| 6 | `PATCH {root}/harnesses/{harness_name}` | `src/api/harnesses.rs:228` | `` invalid type: integer `7`, expected a string `` (`title`) |
| 7 | `PATCH {root}/agents/{agent_id}` | `src/api/agents.rs:170` | `` invalid type: integer `7`, expected a string `` (`title`) |
| 8 | `POST {root}/sessions/{id}/agents` | `src/store/agents.rs:279` | `Invalid request body: <serde text>` — the prefix is the crate's own; every malformed field probed answered the ROUTE's sentence first, so this pair is static, not observed |
| 9 | `PATCH {root}/sessions/{id}/agents/{agent_id}` | `src/store/agents.rs:479` | same prefix, same caveat |

Two more decode sites exist and cannot leak today, so they are counted separately and not in the
seven above: `src/api/mod.rs:167` (`Ctx::body_typed`) has no route callers, and
`src/store/transcript.rs:301` answers a malformed transcript by storing it as an unreadable document —
the reference's reader-repair rule — rather than with a 400.

Closing items 1-7 means porting the reference's per-route validators (`requireObjectBody` +
`requireKnownFields` + the `settingsInputOf`/`readHarnessMessages`/attachment families), which is
outside this wave.

## Not ported, and not claimed

| Item | Why it is still open |
| --- | --- |
| **Storage root** | THE FORK RUNS THE OTHER WAY from what an earlier draft of this row said: the **reference** writes the v2 shard tree (`server/persistence.ts:15-33` — `harness/index.json`, one `sessions/<id>/session.json`, one `threads/<tid>.json` per transcript, and `events.jsonl` beside them), and it is that reference which deliberately does NOT read, migrate or rewrite a v1 `state.json` (`persistence.ts:31-33`). The **crate** is the v1 side: it checkpoints one `harness/state.json` document (`src/persistence.rs:70-77` — `HARNESS_DATABASE_VERSION = 1`, `HARNESS_DATABASE_FILE = "state.json"`). Neither reads the other's layout, so "same data dir" is not yet a cross-implementation claim; the open work is the crate's shard migration, including the `rollback` journal record kind and the document's byte key order. |
| **Lane / `worker` execution** | The reference runs conversations in worker lanes (`server/workers/**`). The crate accepts a lane-bearing submit and refuses `worker` execution rather than pretending to run it; the refusal wording names the crate's own limit. |
| **`delegate` / nested-run execution** | Same shape: the fan-out is refused honestly. The words "validated and accepted" were loose for tool ARGUMENTS and are now scoped: what the crate actually validates on that route is the NAME (`unknown_tool_message` for an unregistered name, `src/tools/mod.rs:502`) and the documented `required` properties (`missing_required`, `:516` + `:644-659`) — and a `delegate` call is refused BEFORE the argument check runs, because a missing capability is not something a model fixes by re-sending better arguments (`:510-512`). What is NOT validated is everything else in the schema: no `type`, `enum`, `additionalProperties` or unknown-key rule runs against tool arguments anywhere (`:638-643`), so a `delegate` body with any extra key still reaches the refusal with the capability sentence. That is the same tolerance the executable rows already have, and it is why the reference's reserved `async` argument is accepted here (next row). Route BODIES are the separate matter covered under "Body refusals that still quote the serialiser" — the manual-execution routes do refuse `async` as a sibling of `parameters` (`Unknown field "async". Allowed fields: parameters.`, `src/api/mod.rs:1205-1216` with the field sets at `src/api/tools.rs:363` and `src/api/session_tools.rs:416`) while accepting it inside `parameters`. |
| **The catalogue rows describe the REFERENCE, not this runtime** | `src/tools/catalogue.rs` is a byte-for-byte copy of the reference's generated manifest, and it is HAND-MIRRORED — the generator its header names is not a checked-in artifact, so each re-sync is a dated wave recorded in `knowledge.md`. Eight rows (`agentManage`, `agentResult`, `delegate`, `threadFork`, `toolCancel`, `threadSend`, `threadList`, `threadRead`) were re-copied off the 2026-11-27 tree on 2026-11-27 — `threadRead` by a parallel lane between the two passes, and `agentManage` and `threadSend` again later the same day when a correction wave moved their text once more (the `answerMessageIndex` sentence, +339 chars, and the 2-char separator in the `threadSend` stopped-row sentence) — so the advertised rows and schemas now carry the reference's current prose and its two newest arguments (`threadFork.allowFamilyWrites`, `agentResult.message`) — while a call to any of the eight still answers the `MISSING_CAPABILITY` sentence, because the branch, thread, delegation and result-ring runtimes named above are unported. Decoding the Rust rows back against the current TypeScript working copy measures SIXTEEN of the 31 manifest-backed rows matching on `description`, `parameters_json` and `tags` — `search`/`fetch` among them on those fields only, since their `sessionBound` flag follows the SERVER rather than the manifest — and FIFTEEN still lagging by text no wave touched (`toolRequest`, `toolList`, `toolManage`, `toolCreate`, `records`, `tasks`, `profiles`, `chapters`, `locations`, `read`, `write`, `edit`, `files`, `glob`, `grep`); `threadRead` is no longer first among them — it is in sync now. `threadDelete` plus the manifest's two newest names, `knowledge` and `sleep`, have no row here at all — absent by never having been ported, not withheld. Nothing in the mirror changes a count: 36 rows, 4 withheld, 15 capability refusals, 32 listed. |
| **The reserved `async` argument: strip it, or advertise it** | OPEN, deliberately not touched by this pass. The reference now offers `async` on every tool declaration and reads it off each call, stripping the key before validation and `execute` (`src/engine/types/index.ts:100-123`); this crate neither strips it nor advertises it. Today `{"async":true}` is therefore accepted and silently ignored (no error text), which already matches what the model can observe on the wire, but the crate also keeps advertising the closed `parameters_json` verbatim (`src/transport.rs:639-648`) — the model is offered a schema that does not mention the key the reference tells it about. Two ways to close it, and the crate should pick ONE in a later pass rather than half-doing both: (a) strip the key in `dispatch` at `src/tools/mod.rs:524` before a body reads `ctx.arguments`, so a tool that spreads its arguments can never echo a key it never declared; or (b) inject `async` into the advertised declarations and honor it, which needs the background scheduler this crate does not have (`turn.rs:708` always settles `pending: []`). This pass chose neither on purpose: advertising a switch the crate cannot honor promises an effect it cannot deliver, and a strip changes tool output bytes for no observable gain. Test home when it is decided: `tests/tools.rs` beside the rejection pin at `tests/tools.rs:1934`. |
| **Programmatic engine barrel** | The reference's `createHarnessServer` entry is out of scope for the delivery surface, and the crate's in-process `api` barrel is not claimed as its equivalent. |
| **`body.messages` on `POST {root}/sessions`** | The reference accepts a SEEDED TRANSCRIPT on session create (`api.ts:3344-3352`: it runs the value through the runtime's own `readHarnessMessages`, then writes it to the entry agent's default conversation `agent:<entry>`). The crate's create route admits only `harness`/`title`/`settings`, so the field answers `Unknown field "messages". Allowed fields: harness, title, settings.` — a clean sentence, but a refusal where the reference has a 201. It needs default-conversation materialization (see the cap delta above), so it is open work, not a wording fix. |

## The delivery contract, as built

* `GET`/`HEAD` anywhere outside the mount, when a mirror root is configured — the file, or the SPA
  fallback, or the plain 404.
* every other path and verb outside the mount — plain `Not found`.
* everything under `/v1/harness` — the dispatcher, unchanged, including
  `This server serves /v1/harness; "<mount>" is not mounted.` and
  `Unknown API route: <METHOD> <path>`.
* There is **no docs page**. `src/docs_page.rs` was a Rust-side invention with zero occurrences of its
  subject in the reference (`grep -rn "publicBaseUrl\|GET /docs" packages/underload/harness/src` is
  empty), and the swagger declares `/v1/harness` paths with no `text/html` response
  (`tests/delivery_surface.rs` asserts that fact rather than asserting a page). A browser that wants
  the API map reads the yaml.
* The mirror is registered through the router's **fallback**, not as a `get(...)` route: a `get`
  registration would make axum answer `POST /index.html` with axum's own 405, where the reference
  answers the plain 404. `router()` (the API-only shape every existing caller uses, the gateway mount
  included) is still `Delivery::default()`, i.e. **no mirror at all**, so the 52-template / 74-operation
  route pins are untouched.

## Bind policy

| Case | Result |
| --- | --- |
| no `--port` | `5181` (`start.ts:160`), host `127.0.0.1` (`start.ts:161`) |
| `--port 0` / `--port auto` | the OS picks; the **bound** port is read back and reported (`start.ts:439-440`) |
| `--port <n>` | bound exactly as asked, **once** |
| a taken port | exits `1` with `cannot bind <addr> - address already in use. Nothing was killed; pass --host/--port or free the port first.` — the loud refusal, killing nothing, is the reference's behaviour (`start.ts:436` binds one port and surfaces the error) |
| `--host <hostname>` | resolved at bind time; the probe line reports the resolved `SocketAddr` |
| `--host <unresolvable>` | exits `1` with `` `<host>` --host is not a valid address or hostname (expected <host>, <host>:<port>, [<ipv6>], or [<ipv6>]:<port>) `` |
| `::1:5199` | an **IPv6 literal**, not an address-with-port: it binds `[::1:5199]:5181`. The old refusal was wrong, and the reference never parses a host at all. |

There is no `--port-retry` and no `--public-base-url`: neither flag has a counterpart anywhere in the
reference (the retry walk invented a bind loop the TS never runs; `public_base_url` invented a URL the
stream contract never carries), so both were removed rather than documented as extensions.

Exit codes: `0` after a clean drain, `1` for **every** failure to serve — a bad port, a relative
mirror root, an unresolvable host, a taken port. The reference launcher sets `process.exitCode = 1`
and never a usage-specific `2` (`dev.ts:82-105`, `src/server/cli/index.ts:377-380`), so `EXIT_USAGE` is gone from
`src/bin/server.rs` alongside it. `--help` and a bad flag are the launcher's own `0`/`1` answers.

## Probe line

`PARITY-LISTEN {"port":…,"host":…,"api":…,"stream":…,"runtime_id":…[,"static_root":…]}` — one line on
stdout, nothing else on stdout, every human line on stderr. `runtime_id` is `events::process_epoch()`,
the exact value of every `X-Harness-Epoch` header the process emits, and `static_root` is present only
when a mirror is configured.

## Verification

Run from `packages/typecrate/harness` (the crate is its own cargo workspace), with `TMPDIR` pointed at
the repo-root `Temp/` and the shared target dir the workspace uses:

```sh
export TMPDIR="$(git rev-parse --show-toplevel)/Temp"
cargo fmt --all -- --check
cargo clippy --all-targets --target-dir ../../../.rust/target -- -Dwarnings
cargo test --workspace --no-fail-fast --target-dir ../../../.rust/target
```

The three commands, and the counts they last produced, are recorded in `Temp/review/` of the wave that
made the last change here — this file does not carry a test count, because a count copied from a run
that already happened is the kind of claim that goes stale silently.

## Ground rules for the next change here

* A new delivery-surface path is an **extension**: name the TS line it is modelled on, or say in this
  file that there is none — and prefer "there is none" as a reason to delete the path.
* Never move the mirror onto the mount namespace: `handler.ts:277` gives it everything *outside* the
  mount, and the mount's 404/405 sentences are contract.
* Keep `router()`'s shape (delivery off) so the gateway mount and the route-table pins stay valid.
* The static root is resolved at **runtime** from `--static-root` / `HARNESS_STATIC_ROOT` and must be
  absolute. No build output is committed to the repository.
* `PARITY-LISTEN` is the only stdout write of a `parity` process; everything human goes to stderr.
* A launcher failure exits `1`. Adding an exit code needs a reference line, not a preference.
* A new `update` kind goes in `contract::HARNESS_WS_UPDATE_KINDS` **first** —
  `src/events.rs::publish_with_ops` debug-asserts against that list, so an invented kind fails in the
  suite instead of reaching a client that never promised it.
