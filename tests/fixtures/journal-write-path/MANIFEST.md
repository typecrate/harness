# journal-write-path — the write / repair / teardown half of journal.ts

Companion to `../journal-cases` (reader verdicts for ten mutated files). This fixture pins the
operations that PRODUCE those bytes, so a Rust port can be checked on the write path too.
Only `createdAt` inside a header line is non-reproducible; every event line is byte-equal to
`journalLine(record)` (`JSON.stringify(record) + '\n'`, journal.ts:112).

## Key order differs between the two writers

The same record becomes two different byte strings depending on the call that wrote it, and a
byte-for-byte port has to keep both:

| Writer | Serialisation | Line for record 0 |
|---|---|---|
| `append()` :303 | `journalLine({ type, v, ...record })` | `{"type":"event","v":1,"turnId":"turn-a","index":0,"at":1,"event":{"type":"start","agent":"general"}}` |
| `rewrite()` :455 | `journalLine({ ...record, type, v })` | `{"turnId":"turn-a","index":0,"at":1,"event":{"type":"start","agent":"general"},"type":"event","v":1}` |

The reader accepts either (it never compares key order), so the difference is only observable
in the file — which is exactly the kind of thing a fixture has to pin.

## Operations

| Operation (journal.ts) | Exact TS behaviour captured here | What the port must match |
|---|---|---|
| `open()` :271 | writes one header line `{"type":"header","v":1,"sessionId":"write-path","createdAt":1794327611390}` (only `createdAt` is wall clock), and creates `sessions/<id>/`. The header's `sessionId` is `path.basename(this.directory)` (:280) and the layout `<root>/sessions/<id>/events.jsonl` is fixed by the constructor (:245-246), so the id a port reports comes from the PATH | the header shape and the directory layout |
| `append()` :295 | returns `true` per record (`false` + retained error on failure), opens the file itself, never rewrites the header; the file becomes header + 3 canonical lines | append is `writeSync` on a held fd, not a rewrite |
| hand-made crash tail | last line cut to 30 chars with no newline → read accepts the first 2 events, `truncatedTail: true`, one problem `{line: 4, kind: 'malformed', tail: true, reason: 'not valid JSON: … (truncated tail)'}`, header intact | drop only the partial last line and report it at its 1-based line number |
| `truncate()` :491 | literally `rewrite([])`: the file becomes a header ONLY — and `rewrite` stamps a **fresh** `createdAt` (:453), so the header bytes change even though nothing else did | `truncate` is a reset, NOT a tail repair |
| `rewrite(records)` :439 | stages `<file>.<pid>.<ms>.tmp`, fsyncs BEFORE the rename, then renames; the kept records are re-emitted in rewrite key order and no `*.tmp` survives in the directory. It does **not** renumber: keeping records 0 and 2 leaves a gap, and the next read reports `{line: 3, kind: 'sequence', reason: 'turn turn-a expected event index 1, found 2', tail: false}` | durability ordering, atomic publish, and gap-preserving rewrite |
| `remove()` :496 | `exists(): false`, the whole `sessions/<id>/` directory is gone, and `read()` answers exactly `{events: [], problems: [], empty: true, missing: true, truncatedTail: false}` with NO `header` key | the delete lifecycle and the missing-file shape |
| junk file | `not json at all\n` as the only line → one problem `{line: 1, kind: 'malformed', tail: false}`, `empty: true`, `missing: false`, no header, nothing thrown | fail LOUDLY rather than repair |
| legacy database | no file → `{events: [], problems: [], empty: true, missing: true, truncatedTail: false}` | pre-R4 databases fold `turns[].events` instead |

## Files

- `write-path-observations.json` — every byte and every `JournalReadResult` above, verbatim,
  plus the three input records (`RECORDS` in the drive) and both key-order line forms.

## Reproduce

```bash
export TMPDIR=$PWD/Temp
node_modules/.bin/vitest run --config Temp/f1/vitest.config.ts Temp/f1/journal-cases-extra.drive.ts
```

Scratch journal root: `work/journal-write-path` (git-ignored). No HTTP is involved, so this
fixture is independent of the lane/durability settings the HTTP fixtures pin.
