# journal-cases

## Provenance
- Source of truth: `packages/underload/harness` (nested repo tip read by the driver from `.git/refs/heads/main`; also indexed in `Temp/fixtures/harness/MANIFEST.md`).
- Producer: `Temp/f1/journal-cases.drive.ts` (vitest, config `Temp/f1/vitest.config.ts`).
- The baseline bytes come from the REAL recorder: a TS server (`startHarnessServer`, inline/inline) running the offline scripted preset `demo` over a scratch `databaseRoot`; session `727837c9-4013-4547-9da9-ec100b309248`, turn `bff7e976-e1a0-48f7-a662-858ac850d90a`, `178` journal lines. No provider endpoint was dialled.
- Mutations are structural only: byte truncation, line splice/removal, and re-serialisation through the TS writer's own `journalLine()`. No JSON was hand-authored.
- Every case file sits at the path the reader expects (`<root>/sessions/<case>/events.jsonl` — `HARNESS_JOURNAL_DIRECTORY`) and was read back with the real TS `HarnessJournal`; the read result is captured verbatim in `ts-reader-observations.json`.

## Cases
| Case | Mutation | TS reader verdict the Rust port must match |
|---|---|---|
| `healthy` | none — the recorder's own bytes | `problems: []`, `truncatedTail: false`, every event |
| `truncated-tail` | final line cut mid-token, no trailing newline | the partial line is DROPPED, but it is still REPORTED: one problem `{kind:'malformed', tail: true}` whose reason ends `(truncated tail)`, plus `truncatedTail: true`. Everything before it stands. |
| `malformed-middle` | non-JSON line spliced in the middle | `{kind:'malformed', tail: false}` at the spliced line — corruption is EXPLICIT, never skipped-and-pretended-healthy. Contrast with `truncated-tail`: the SAME break class is tolerated only when it is the unterminated LAST line. |
| `version-mismatch` | header re-written with `v: 2` | a parse refusal naming the version: `{kind:'malformed', line: 1, tail: false}`, reason `unsupported journal version 2; this server writes version 1`. (`kind:'version'` is declared in `JournalProblemKind` but a read never produces it — see `parseLine`, journal.ts:175.) The knock-on is load-bearing: with no header accepted, EVERY event line also reports `{kind:'shape', reason:'event before header'}`, and all `events.length` events are still accepted. |
| `sequence-gap` | one middle line removed | `{kind:'sequence', tail: false}` — "a gap is never a crash tail: appends are sequential" |
| `event-missing-payload` | event line with no `event` object | `{kind:'malformed', reason:'event payload is missing'}` (a null/absent event would fold into the projector as a phantom step) |
| `unknown-type` | well-formed line, `type: 'bogus'` | `{kind:'malformed', reason:'unknown journal record type "bogus"'}` — reported, not repaired |
| `shape-header-late` | header moved to line 2 | `kind:'shape'` on EVERY line (one problem per line, all events still accepted): line 1 `event before header`, line 2 `header is not the first line`, lines 3+ `event before header` again because no header was ever accepted. These are the only places a read emits `shape`. |
| `header-only` | TS `open()` with no append | `empty: true`, `missing: false` |
| `missing` | no journal file at all | `missing: true`, `empty: true` (the pre-R4 database shape — the caller folds `turns[].events` instead) |

Exact line numbers, kinds, and reasons are in `ts-reader-observations.json` (`cases.<name>.read.problems`), together with the accepted-event envelopes and the full `indexes` array.

## Contents
| File | What it is |
|---|---|
| `cases/<name>.events.jsonl` | the case file exactly as TS wrote/mutated it |
| `ts-reader-observations.json` | the TS reader's verdict per case (header, events, problems, empty/missing/truncatedTail) plus accepted-event digests |
| `http-log.jsonl` | the REST drive that produced the baseline journal (method, generated route, status, bodies) |

## How to reproduce
```
export TMPDIR=$PWD/Temp
node_modules/.bin/vitest run --config Temp/f1/vitest.config.ts Temp/f1/journal-cases.drive.ts
```

## Parity assertions this fixture supports
1. A Rust journal reader must accept `healthy` with zero problems and the same event count/index sequence.
2. It must TOLERATE `truncated-tail` (drop exactly the partial line, set the equivalent of `truncatedTail`) — and must NOT tolerate the same class of breakage in the middle of the file.
3. It must report `version` / `sequence` / `shape` / malformed-middle as explicit problems with `tail: false`, at the same 1-based line numbers.
4. It must distinguish `empty` (header only) from `missing` (no file), because the restore path takes different branches.
5. The envelope is exactly `{"type":"event","v":1,"turnId":...,"index":...,"at":...,"event":{...}}` — key order as written by `journalLine()`, and a header line first.
