# lane-compaction

## Status
ACHIEVED end to end: a real `summarize` compaction ran inside a real TS server run, and the durable document carries the lane stamps.

## Provenance / code path used
- **Path: full TS server over HTTP** (not a projector-level shortcut). `startHarnessServer` (`src/server/start.ts`) with `databaseRoot = Temp/fixtures/harness/work/lane-compaction` and `runtime: { threadExecution: 'inline', ioExecution: 'inline' }` — the scripted fixture's transport is a closure, so it is inline-only (see `src/server/workers/provider-stub.test-support.ts` header).
- Harness: the offline scripted preset `demo` from `src/server/harnesses/fixture-presets.test-support.ts` (`PRODUCTION_WITH_FIXTURES`). Its transport streams reasoning deltas, text deltas, a `toolRequest` discovery round, one `timeNow` call, and an answer with pinned usage figures — **no LLM provider endpoint is dialled**.
- Compaction: `PATCH /v1/harness/plugins/context-compaction` with `{ enabled: true, mode: 'summarize', contextLimit: 1000, minTokens: 0, safetyTurns: 0 }` (`summarize` is the shipped default mode, spelled out anyway; `contextLimit: 1000` is the LOWEST value the descriptor accepts — `min: 1000, step: 1000`, so a smaller ceiling is a 400, which the plugin-catalogue fixture pins for the out-of-range class; `minTokens: 0` removes the summarize floor so the pass is never refused for being small). The pass itself is `src/plugins/context-compaction.plugin.ts` (`beginSummarize` → the lane bump at `src/plugins/context-compaction.plugin.ts:1059-1108`).
- Driver: `Temp/f1/lane-compaction.drive.ts` (vitest, config `Temp/f1/vitest.config.ts`). Reproduce:
```
export TMPDIR=$PWD/Temp
node_modules/.bin/vitest run --config Temp/f1/vitest.config.ts Temp/f1/lane-compaction.drive.ts
```

## What the fixture proves (all figures are in `lane-evidence.json`)
| Surface | Where it lives in this fixture | TS owner |
|---|---|---|
| per-event `lane` stamp | `durable/events.<id>.jsonl`, inside the open `event` object (`336` of `undefined` events carry one; lanes seen: `1`) | `src/server/sessions.ts:7924` (`contextLaneOf` at record time), omitted when 0 |
| durable per-conversation lane | `state.json` → `sessions[].core.threads[].contextLane` | `src/types/index.ts` record + `src/core/snapshot.ts` `readThreads`; `0` is never written |
| per-turn starting lane | `state.json` → `sessions[].turns[].lane` | `src/server/protocol.ts:165`, `readTurn` (`>= 0`, 0 omitted) |
| plugin diagnostics | `state.json` → `sessions[].pluginDiagnostics` (`3` entries) | `src/server/protocol.ts` `SessionPluginDiagnostic`, limit `PLUGIN_DIAGNOSTIC_LIMIT` |
| the published field | `turn-displays.json` → `ContextBlock.lane` on the sections of the turn the boundary happened in | swagger `:8122` → `src/shared/turn-display.ts`, folded by `src/server/turn-projector.ts` (`laneOfEvent` / `laneOfTurn` / `laneFieldsOf`) |

## Parity assertions this fixture supports
1. **Replay**: folding `durable/events.<id>.jsonl` must reproduce the `ContextBlock.lane` values recorded in `turn-displays.json` per section — the issue's headline acceptance criterion, asserted against a recorded fixture rather than restated.
2. **Lane as an input**: the per-event `lane` key must survive a journal read AND the document read **verbatim** (it rides the open `HarnessEvent`, and the generated TS union has no `lane` member — `src/types/index.ts:2578` — so its absence from the generated type is not permission to drop it).
3. **Two-source rule**: an event-driven segment takes the event's lane; a non-event segment (`system` / `prompt` / `steer`) takes the turn's starting lane (`turns[].lane`).
4. **Zero never persists**: `contextLane: 0` and `lane: 0` must be absent, not written — check the conversations/turns listed in `lane-evidence.json` that have no such key.
5. **Diagnostics**: the summarize pass's diagnostic entries in `state.json` must decode with the same field set the TS protocol type states.
6. Compare against `../baseline-session` for the no-compaction shape: there, no `lane`, no `contextLane`, and no `pluginDiagnostics` appear at all, and the fold is byte-identical to the pre-feature output.

## Known wrinkle (recorded, not hidden)
The turn addressed to the compacted conversation ends `status: 'error'` with
`Agent "general" reused tool call id "call_time_now_1" from an earlier turn; tool call ids must be unique`.
That is the SCRIPTED fixture's id rule colliding with compaction, not a lane defect: the fixture mints the next
free id from the messages in the REQUEST (`src/server/harnesses/fixture-presets.test-support.ts`,
`usedCallIds`), and after a summarize pass the earlier tool calls are withheld from the request while remaining
in canonical history, so the "free" id is one history already used. The lane fields this fixture exists to pin
were all recorded before/independently of that refusal — `turns[1].lane = 1`, the thread's `contextLane: 1`,
80 journal events carrying `lane: 1`, and a display whose `system`/`user` blocks carry a WRITTEN `lane: 1`.
A Rust replay reads the same bytes and must reach the same lanes; whether the turn then ends `done` or
`error` is the scripted transport's behaviour, which the port does not implement.

## Files
| File | What it is |
|---|---|
| `durable/state.json`, `durable/events.<sessionId>.jsonl` | the TS checkpoint and journal after the compaction (captured after `server.close()`) |
| `lane-evidence.json` | every lane-related figure, extracted from the durable document and from the display responses |
| `turn-displays.json` | `GET .../turns/{id}/display` for every turn — the fold output with `ContextBlock.lane` |
| `turn-events.json` | `GET .../turns/{id}/events` pages (the REST view of the same journal events) |
| `threads-before.json` / `threads-after.json` | the conversation rows before and after, so the lane bump is visible as a delta |
| `get-session.json`, `get-turns.json` | the session/turn documents as TS answered them |
| `http-log.jsonl` | the full request/response trail |
