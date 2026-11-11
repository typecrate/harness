//! The durable per-session event journal (`<root>/harness/sessions/<sessionId>/events.jsonl`).
//!
//! TypeScript origin: `packages/underload/harness/src/server/journal.ts` (545 lines), ported byte for
//! byte including the line format, the reader's tolerance rules, and the problem texts.
//!
//! WHY A JOURNAL BESIDE THE CHECKPOINT (`journal.ts:3-13`)
//! ------------------------------------------------------
//! `state.json` is a CHECKPOINT: one atomic full-document replace. Rewriting it for every harness
//! event is O(every session's every event), so the journal is the other half of durability: an
//! append-only, one-line-per-event log that is O(1) per event and survives a crash.
//!
//! LINE FORMAT - THE ONE THING THIS FILE MUST GET EXACTLY RIGHT (`journal.ts:85-110`, `:296-300`,
//! `:439-455`)
//! -----------------------------------------------------------------------------------------------
//! The header line:
//!
//! ```text
//! {"type":"header","v":1,"sessionId":"<id>","createdAt":<ms>}
//! ```
//!
//! One APPENDED event line (`append` builds `{ type, v, ...record }`, so `type`/`v` come FIRST):
//!
//! ```text
//! {"type":"event","v":1,"turnId":"<id>","index":<n>,"at":<ms>,"event":{...}}
//! ```
//!
//! One REWRITTEN event line (`rewrite` builds `{ ...record, type:'event', v:1 }`, so `type`/`v`
//! come LAST - the spread order is the key order, and `JSON.stringify` follows insertion order):
//!
//! ```text
//! {"turnId":"<id>","index":<n>,"at":<ms>,"event":{...},"type":"event","v":1}
//! ```
//!
//! Both spellings are reproduced here verbatim, because a recorded byte-for-byte fixture (see
//! `tests/data_plane.rs`) compares these lines as STRINGS: `index.ts`/`serde_json` keep insertion
//! order (the `preserve_order` feature is on), so `json!`'s key order IS the file's key order. The
//! two writers differ only in where `type`/`v` sit - a client that diffs a TS-written journal
//! against a Rust-written one must see nothing.
//!
//! THE POSITION IS THE TURN'S, NOT THE FILE'S (`journal.ts:99-106`, `sessions.ts:7930-7961`)
//! -----------------------------------------------------------------------------------------
//! `turnId` + `index` are the turn's append-only position - the SAME address a display step id
//! carries (`DisplayStepDetailRef.stepId`). The recorder computes `index = turn.events.len()` before
//! the push and passes the ONE `at` it shares with the record and the published frame. There is no
//! per-file sequence anywhere in this format, which is why the old per-file counter and its
//! `expected event sequence N` report are gone: they were a second, conflicting notion of position.
//!
//! READ MODEL (`journal.ts:15-22`, `:357-433`)
//! ------------------------------------------
//!   * a TRUNCATED LAST LINE is a crash artifact: it was never a proven-complete append, so a
//!     snapshot fold drops it silently, while an explicit read reports it as a problem with
//!     `tail: true` and the reason suffixed ` (truncated tail)`;
//!   * corruption ANYWHERE ELSE is EXPLICIT and is never a tail: a blank line, an unparseable line,
//!     an unknown version, a header that is not line 1, an event before any header, or a gap in one
//!     turn's index sequence;
//!   * the newline test decides `truncatedTail` BEFORE the loop, so a shape or sequence problem on
//!     the last line stays a real corruption report.
//!
//! DURABILITY LEVELS (`journal.ts:33-45`)
//! -------------------------------------
//!   * `append` writes to a held-open fd: safe across a PROCESS crash, not fenced against a power
//!     loss. `durable: true` adds the fsync fence the caller asked for;
//!   * `flush` is that fence on its own;
//!   * `rewrite` is fsync-then-rename, so a compaction publishes complete bytes.
//!
//! ONE WRITER (`journal.ts:54-66`)
//! ------------------------------
//! One instance per live session, holding the append handle. A second writer of the same file is
//! exactly what the store's serialized commit exists to prevent, so this type is `&mut`-self
//! throughout rather than internally-synchronized. `rewrite`'s staging name is
//! `<file>.<pid>.<ms>.tmp` (`:445`), which is also why the one-writer rule matters: a second writer
//! in the same process would produce two staging names in the same millisecond.
//!
//! WHY THE OLD RUST ENVELOPE IS NOT ACCEPTED ON READ
//! ------------------------------------------------
//! `{version, sequence, timestamp, event}` (what this file wrote before this uplift) has no `v`, so
//! TS's reader rejects every line of it as `unsupported journal version undefined`. Accepting it
//! here anyway would be a divergence in the one direction that matters - a Rust journal a TS server
//! cannot read. A legacy file is therefore reported exactly the way TS reports it, and the new
//! writer only ever emits TS bytes.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};

use crate::contract::HarnessEvent;
use crate::error::ApiError;
use crate::events;

/// The journal format version (`journal.ts:84`). A line whose version is not this one is a
/// reported problem, never a guess.
pub const HARNESS_JOURNAL_VERSION: u32 = 1;

/// The journal file inside one session's directory (`journal.ts:83`).
pub const HARNESS_JOURNAL_FILE: &str = "events.jsonl";

/// The directory the per-session journals live in (`journal.ts:82`), i.e. the checkpoint's
/// `sessions` sibling so an operator sees both in one place.
pub const HARNESS_JOURNAL_DIRECTORY: &str = "sessions";

/// How many times a Windows sharing violation is retried before a removal is reported. Node's
/// writer never needs this: POSIX unlinks an open file, Windows refuses while a handle exists.
const REMOVE_ATTEMPTS: usize = 6;

/// Per-attempt backoff for the removal retry, in milliseconds.
const REMOVE_BACKOFF_MS: [u64; REMOVE_ATTEMPTS] = [0, 5, 10, 25, 50, 100];

/// A journal failure naming the file, matching `HarnessPersistenceError`'s convention so an
/// operator finds the file without guessing (`journal.ts:137-147` `HarnessJournalError`).
fn journal_error(file: &Path, message: &str) -> ApiError {
    ApiError::internal(format!("{}: {message}", file.display()))
}

/// Renders one record as a journal line, with its terminating newline
/// (`journalLine`, `journal.ts:153-154`). Kept a free function so a test can assert the exact
/// wire format without touching the filesystem.
#[must_use]
pub fn journal_line(record: &Value) -> String {
    let serialized = serde_json::to_string(record).unwrap_or_else(|_| "{}".to_string());

    format!("{serialized}\n")
}

/// The header record every journal starts with, so a reader can tell "an empty journal for a real
/// session" from "a stray file" and gate a future format change on a version (`journal.ts:86-92`
/// `JournalHeaderRecord`).
#[must_use]
pub fn journal_header(session_id: &str, created_at: u64) -> Value {
    json!({
        "type": "header",
        "v": HARNESS_JOURNAL_VERSION,
        "sessionId": session_id,
        "createdAt": created_at,
    })
}

/// One APPENDED event line (`journal.ts:96-103` `JournalEventRecord`, emitted by
/// `journalLine({ type: 'event', v, ...record })` at `:298`): `type` and `v` first, then the
/// recorded position, then the payload.
#[must_use]
pub fn journal_event_line(turn_id: &str, index: u64, at: u64, event: &HarnessEvent) -> Value {
    json!({
        "type": "event",
        "v": HARNESS_JOURNAL_VERSION,
        "turnId": turn_id,
        "index": index,
        "at": at,
        "event": event_payload(event),
    })
}

/// One REWRITTEN event line (`journalLine({ ...record, type: 'event', v })` at `:455`): the
/// recorded position and payload first, `type`/`v` LAST. This is not a stylistic choice - it is
/// what `JSON.stringify` produces from the spread order, and a compaction therefore re-emits every
/// retained record with a different key order than the append that first wrote it.
#[must_use]
pub fn journal_compacted_line(record: &JournalEventRecord) -> Value {
    json!({
        "turnId": record.turn_id,
        "index": record.index,
        "at": record.at,
        "event": record.event,
        "type": "event",
        "v": HARNESS_JOURNAL_VERSION,
    })
}

/// The wire form of one event: the same object the REST stream publishes, so a journal line and a
/// `event` frame carry identical bytes.
fn event_payload(event: &HarnessEvent) -> Value {
    serde_json::to_value(event).unwrap_or(Value::Null)
}

/// One recorded event, in the shape the reader hands the caller (`journal.ts:96-103`).
///
/// `event` stays an untyped [`Value`]: TS's reader validates only that `event` is an object with a
/// string `type` (`:207-210`), so an event this build cannot deserialize is still a readable record
/// (a forward-compatible log), and refusing it here would be a divergence.
/// [`JournalEventRecord::event_typed`] is the seam that asks for the typed form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalEventRecord {
    /// The turn this event belongs to (`turnId`).
    pub turn_id: String,
    /// The turn's append-only position (`index`), i.e. the display step id.
    pub index: u64,
    /// The single timestamp the record, the journal line, and the published frame share (`at`).
    pub at: u64,
    /// The event payload, verbatim.
    pub event: Value,
}

impl JournalEventRecord {
    /// The event as the contract type it should be (`event as HarnessEvent`, `:213`).
    ///
    /// # Errors
    ///
    /// The serde message when this build cannot deserialize the payload.
    pub fn event_typed(&self) -> Result<HarnessEvent, String> {
        serde_json::from_value(self.event.clone()).map_err(|error| error.to_string())
    }

    /// The value form of the record's position - what a test asserts against.
    #[must_use]
    pub fn to_value(&self) -> Value {
        json!({
            "turnId": self.turn_id,
            "index": self.index,
            "at": self.at,
            "event": self.event,
        })
    }
}

/// One parsed header line (`journal.ts:86-92`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalHeaderRecord {
    /// The session the journal belongs to; the reader never cross-checks it against the directory
    /// name (`:184-186` only requires a non-empty string).
    pub session_id: String,
    /// When this journal was created (`createdAt`).
    pub created_at: u64,
}

/// What kind of structural problem a line produced (`journal.ts:112` `JournalProblemKind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum JournalProblemKind {
    /// The line is not JSON, is not an object, or is an empty line (`kind: 'malformed'`).
    Malformed,
    /// The line's `v` is not this server's version (`kind: 'version'`).
    Version,
    /// One turn's index sequence has a hole (`kind: 'sequence'`).
    Sequence,
    /// A structurally valid line in an impossible position (`kind: 'shape'`).
    Shape,
}

/// One structural problem found while reading (`journal.ts:114-121` `JournalProblem`).
///
/// `tail: true` marks the crash-truncated last line (tolerated); every other problem is a real
/// corruption report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JournalProblem {
    /// 1-based line number; `0` means "the file itself could not be read".
    pub line: u64,
    /// The class of problem.
    pub kind: JournalProblemKind,
    /// The human report, byte-identical to TS's text.
    pub reason: String,
    /// Whether this is the tolerated crash tail.
    pub tail: bool,
}

/// The whole read (`journal.ts:123-137` `JournalReadResult`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalReadResult {
    /// Event records in file order; gaps and problems are preserved for the caller to merge.
    pub events: Vec<JournalEventRecord>,
    /// The header line, when the file has one (`header?`).
    pub header: Option<JournalHeaderRecord>,
    /// Every structural problem, in line order.
    pub problems: Vec<JournalProblem>,
    /// True when the file exists but holds nothing but the header (`empty`).
    pub empty: bool,
    /// True when there is no journal at all - a database written before the journal existed, whose
    /// events the caller folds out of `turns[].events` instead (`missing`).
    pub missing: bool,
    /// True when the file's LAST line is incomplete (no terminating newline): a crash mid-append.
    /// That one record is dropped and everything before it stands (`truncatedTail`).
    pub truncated_tail: bool,
}

impl JournalReadResult {
    /// The events of one turn, in recorded order.
    #[must_use]
    pub fn events_of(&self, turn_id: &str) -> Vec<&JournalEventRecord> {
        self.events
            .iter()
            .filter(|record| record.turn_id == turn_id)
            .collect()
    }

    /// Whether any problem is real corruption rather than the tolerated crash tail.
    #[must_use]
    pub fn has_corruption(&self) -> bool {
        self.problems.iter().any(|problem| !problem.tail)
    }
}

/// Owns one session's journal file: its directory, its append handle, and its last error.
#[derive(Debug)]
pub struct HarnessJournal {
    /// `<sessions_dir>/<sessionId>` - the whole directory belongs to this journal (`:245`).
    directory: PathBuf,
    /// `<directory>/events.jsonl` (or the explicit file name a caller passed).
    file: PathBuf,
    /// The session this journal belongs to, kept because the header line names it.
    session_id: String,
    /// The held-open append handle. `append` on a fresh fd per event is one syscall pair per DELTA;
    /// a persistent fd plus `flush` at the boundaries that matter is the durability story the
    /// checkpoint already tells (`:233-238`).
    handle: Option<File>,
    /// Bytes currently on disk, maintained so `size()` needs no stat.
    size: u64,
    /// Whether the directory and the header line exist (`opened`).
    opened: bool,
    /// Last journal failure (open/append/flush/rewrite/remove), cleared by the next success
    /// (`:250-254`).
    last_error: Option<String>,
    /// Count of failed writes, so the store can surface "the journal is degraded" without
    /// re-reading the error text (`:256-258`).
    append_failures: u64,
}

impl HarnessJournal {
    /// Creates the session directory and writes the header once (`journal.ts:270-289` `open`).
    ///
    /// `events_file_name` overrides the file name for a caller that needs a second log in the same
    /// directory; `None` is the shipped `events.jsonl`.
    ///
    /// Nothing is scanned: the format carries no per-file counter, so opening is O(1) regardless of
    /// how large the log already is.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the directory cannot be created or the header
    /// cannot be written.
    pub fn open(
        sessions_dir: &Path,
        session_id: &str,
        events_file_name: Option<&str>,
    ) -> Result<Self, ApiError> {
        let directory = sessions_dir.join(session_id);
        let file = directory.join(events_file_name.unwrap_or(HARNESS_JOURNAL_FILE));

        let mut journal = Self {
            directory,
            file,
            session_id: session_id.to_string(),
            handle: None,
            size: 0,
            opened: false,
            last_error: None,
            append_failures: 0,
        };

        journal.create()?;

        Ok(journal)
    }

    /// This journal's file path (`journal.ts:246` `file`).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.file
    }

    /// The session directory that holds this journal (`journal.ts:245` `directory`).
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// The session id this journal belongs to.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Bytes on disk right now.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The last recorded failure, or `None` after a success (`journal.ts:252-254` `error`).
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// How many writes have failed on this handle (`journal.ts:256-258` `failures`).
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.append_failures
    }

    /// Whether the journal file exists (`journal.ts:260-267` `exists`).
    #[must_use]
    pub fn exists(&self) -> bool {
        self.file.exists()
    }

    /// Appends one event line at the position the recorder supplies (`journal.ts:292-311` `append`).
    ///
    /// `turn_id`/`index` are the turn's append-only position and `at` is the ONE timestamp the
    /// record, this line, and the published frame share - all three come from the caller
    /// (`sessions.ts:7930-7961`), which is what keeps a live view and a reload in agreement.
    /// `durable: true` fsyncs the tail before returning, the fence for "every event of this turn is
    /// on stable storage before the response" (`:38-41`).
    ///
    /// A failure does NOT abort the caller's run (`journal.ts:24-29`: a full disk must not change a
    /// run's outcome) - it is returned so the caller can record it, and is retained in
    /// [`HarnessJournal::error`] either way.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the write or the fence fails.
    pub fn append(
        &mut self,
        turn_id: &str,
        index: u64,
        at: u64,
        event: &HarnessEvent,
        durable: bool,
    ) -> Result<(), ApiError> {
        self.ensure_open()?;

        let line = journal_line(&journal_event_line(turn_id, index, at, event));

        match self.write_bytes(line.as_bytes()) {
            Ok(()) => {
                // The append itself succeeded, so the byte count advances whether or not the fence
                // does: the bytes are in the file, and a size that disagreed with the file would
                // misreport the log's growth.
                self.size += u64::try_from(line.len()).unwrap_or(line.len() as u64);

                let fenced = if durable { self.fsync() } else { Ok(()) };

                match fenced {
                    Ok(()) => {
                        self.last_error = None;
                        Ok(())
                    }
                    Err(error) => {
                        self.note_failure(&error);
                        self.close_handle();

                        Err(journal_error(&self.file, &error))
                    }
                }
            }
            Err(error) => {
                self.note_failure(&error);
                // Drop the handle so the next append reopens: a directory removed under us must not
                // wedge every later write (`:308-311`).
                self.close_handle();

                Err(journal_error(&self.file, &error))
            }
        }
    }

    /// fsyncs the tail (`journal.ts:315-326` `flush`). Called at turn-terminal and at every
    /// checkpoint, i.e. exactly where the existing code already treats durability as load-bearing.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the fence fails.
    pub fn flush(&mut self) -> Result<(), ApiError> {
        if self.handle.is_none() {
            return Ok(());
        }

        match self.fsync() {
            Ok(()) => {
                self.last_error = None;
                Ok(())
            }
            Err(error) => {
                let message = error;
                self.note_failure(&message);

                Err(journal_error(&self.file, &message))
            }
        }
    }

    /// Reads the whole journal (`journal.ts:328-434` `read`).
    ///
    /// NEVER fails for a data problem: an unreadable file is a `malformed` problem on line 0 and a
    /// missing file is `missing: true`, because the caller is the restore path, which must degrade to
    /// the checkpoint rather than refuse to boot. Only a failure to even attempt the read is
    /// distinguishable, and TS reports that as a problem too.
    #[must_use]
    pub fn read(&mut self) -> JournalReadResult {
        scan_file(&self.file)
    }

    /// Replaces the whole log with `records` through an atomic tmp + rename (`journal.ts:439-487`
    /// `rewrite`, the checkpoint's fsync-then-rename rule).
    ///
    /// GAP-PRESERVING: each record keeps its own `turnId`/`index`/`at` verbatim - a compaction must
    /// not invent new facts about events it did not change, and a hole in the sequence is the caller's
    /// corruption report to keep (`tests/data_plane.rs` pins the re-read problem
    /// `turn turn-a expected event index 1, found 2`). The HEADER is fresh (`createdAt` is
    /// `Date.now()` again), which is what TS's `rewrite` does.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the staging write, the fence, or the rename fails.
    pub fn rewrite(&mut self, records: &[JournalEventRecord]) -> Result<(), ApiError> {
        self.ensure_open()?;

        // The worker must not hold the file the rename replaces (`:443`).
        self.close_handle();

        let payload = rewrite_payload(&self.session_id, records);

        match self.commit_payload(&payload) {
            Ok(()) => {
                self.size = u64::try_from(payload.len()).unwrap_or(payload.len() as u64);
                self.last_error = None;

                Ok(())
            }
            Err(error) => {
                self.note_failure(&error);

                Err(journal_error(&self.file, &error))
            }
        }
    }

    /// Drops every recorded event (`journal.ts:491-493` `truncate`): the log starts over, and a
    /// stale event can never be replayed onto a turn that no longer exists.
    ///
    /// This is `rewrite([])`, which means the file keeps a header with a FRESH `createdAt` - not an
    /// empty file, and not the original header.
    ///
    /// # Errors
    ///
    /// As [`HarnessJournal::rewrite`].
    pub fn truncate(&mut self) -> Result<(), ApiError> {
        self.rewrite(&[])
    }

    /// Closes the handle and removes this session's whole journal directory (`journal.ts:496-506`
    /// `remove`).
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the directory when the removal keeps failing after the bounded
    /// retry.
    pub fn remove(&mut self) -> Result<(), ApiError> {
        self.close_handle();
        self.opened = false;

        let mut last_error = String::new();

        for (attempt, backoff) in REMOVE_BACKOFF_MS.iter().enumerate() {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(*backoff));
            }

            if !self.directory.exists() {
                self.reset_counters();
                self.last_error = None;

                return Ok(());
            }

            match fs::remove_dir_all(&self.directory) {
                Ok(()) => {
                    self.reset_counters();
                    self.last_error = None;

                    return Ok(());
                }
                Err(error) => {
                    last_error = error.to_string();

                    if !is_transient(&error) {
                        break;
                    }
                }
            }
        }

        self.note_failure(&last_error);

        Err(journal_error(
            &self.directory,
            &format!("Could not remove the harness journal: {last_error}"),
        ))
    }

    /// Flushes and closes the handle; the journal may be reopened by the next append
    /// (`journal.ts:508-512` `close`).
    ///
    /// # Errors
    ///
    /// As [`HarnessJournal::flush`].
    pub fn close(&mut self) -> Result<(), ApiError> {
        let flushed = self.flush();
        self.close_handle();
        self.opened = false;

        flushed
    }

    // -- internals ----------------------------------------------------------

    /// Creates the directory and writes the header if the file is absent (`:272-287`).
    fn create(&mut self) -> Result<(), ApiError> {
        fs::create_dir_all(&self.directory).map_err(|error| {
            journal_error(
                &self.file,
                &format!("Could not create the harness journal directory: {error}"),
            )
        })?;

        if !self.file.exists() {
            fs::write(
                &self.file,
                journal_line(&journal_header(&self.session_id, events::now_millis())),
            )
            .map_err(|error| {
                journal_error(
                    &self.file,
                    &format!("Could not write the harness journal header: {error}"),
                )
            })?;
        }

        self.size = fs::metadata(&self.file)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        self.opened = true;
        self.last_error = None;

        Ok(())
    }

    /// Re-creates the directory/header if a `remove` or an external cleanup took the file away
    /// (`:270-289`, which is itself idempotent).
    fn ensure_open(&mut self) -> Result<(), ApiError> {
        if self.opened && self.file.exists() {
            return Ok(());
        }

        self.create()
    }

    /// Writes bytes through the held-open append handle, opening it on demand
    /// (`journal.ts:514-527` `ensureHandle`).
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        if self.handle.is_none() {
            // 'a' never truncates and positions at the end, which is the whole point of an
            // append-only log (and is what makes a torn tail possible).
            let opened = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.file)
                .map_err(|error| error.to_string())?;

            self.handle = Some(opened);
        }

        let handle = self
            .handle
            .as_mut()
            .ok_or_else(|| "the journal handle is not open".to_string())?;

        handle.write_all(bytes).map_err(|error| error.to_string())?;
        // Push the payload into the OS page cache; `fsync` is what makes it stable storage.
        handle.flush().map_err(|error| error.to_string())
    }

    /// fsyncs the held handle.
    fn fsync(&mut self) -> Result<(), String> {
        let handle = self
            .handle
            .as_mut()
            .ok_or_else(|| "the journal handle is not open".to_string())?;

        handle.sync_all().map_err(|error| error.to_string())
    }

    /// Closes the handle, swallowing an error: an unusable handle is already the reported failure
    /// (`journal.ts:529-539` `closeHandle`).
    fn close_handle(&mut self) {
        let _ = self.handle.take();
    }

    /// Atomically replaces the file with `payload` (staging + fsync + rename, then a bounded retry
    /// over a Windows sharing violation).
    ///
    /// The staging name is `<file>.<pid>.<ms>.tmp` (`journal.ts:445`); the process id is the same
    /// value Node would use, and the retry replaces only the millisecond.
    fn commit_payload(&mut self, payload: &str) -> Result<(), String> {
        let pid = std::process::id();
        let mut last_error = String::new();

        for (attempt, backoff) in REMOVE_BACKOFF_MS.iter().enumerate() {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(*backoff));
            }

            let name = self
                .file
                .file_name()
                .map_or_else(
                    || HARNESS_JOURNAL_FILE.to_string(),
                    |name| name.to_string_lossy().into_owned(),
                );

            let staging = self
                .directory
                .join(format!("{name}.{pid}.{}.tmp", events::now_millis() + u64::try_from(attempt).unwrap_or(attempt as u64)));

            let staged = (|| -> Result<(), String> {
                let mut handle = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&staging)
                    .map_err(|error| error.to_string())?;

                let written = (|| -> Result<(), String> {
                    handle
                        .write_all(payload.as_bytes())
                        .map_err(|error| error.to_string())?;
                    handle.flush().map_err(|error| error.to_string())?;
                    // fsync BEFORE the rename, the checkpoint's rule (`:458-461`): a rename can be
                    // durable while the bytes are not.
                    handle.sync_all().map_err(|error| error.to_string())
                })();

                drop(handle);
                written?;

                fs::rename(&staging, &self.file).map_err(|error| error.to_string())
            })();

            match staged {
                Ok(()) => return Ok(()),
                Err(error) => {
                    // The staging file is the lesser problem; `error` is the report (`:482-485`).
                    let _ = fs::remove_file(&staging);
                    last_error = error;
                }
            }
        }

        Err(last_error)
    }

    /// Records a failure the way `noteWrite` does (`journal.ts:541-544`).
    fn note_failure(&mut self, message: &str) {
        self.last_error = Some(message.to_string());
        self.append_failures += 1;
    }

    /// Zeroes the recovered counters after a `remove`, so a reopened journal starts over.
    fn reset_counters(&mut self) {
        self.size = 0;
    }
}

/// The whole rewritten payload: one fresh header line plus every record in the compaction's key
/// order (`journal.ts:448-455`).
#[must_use]
fn rewrite_payload(session_id: &str, records: &[JournalEventRecord]) -> String {
    let mut payload = journal_line(&journal_header(session_id, events::now_millis()));

    for record in records {
        payload.push_str(&journal_line(&journal_compacted_line(record)));
    }

    payload
}

/// One parsed journal line (`journal.ts:162-225` `parseLine`'s discriminated result).
enum ParsedLine {
    Header(JournalHeaderRecord),
    Event(JournalEventRecord),
}

/// Reads and validates one journal file, implementing `journal.ts:328-434` line for line.
///
/// Tolerance, in the order TS applies it: the newline test decides the crash tail BEFORE any
/// parsing, a blank line in the middle is corruption, a version mismatch is corruption, a header
/// that is not line 1 is skipped (so every later line reports `event before header`), and a gap in
/// one turn's index sequence is corruption that is NEVER a tail.
#[must_use]
fn scan_file(file: &Path) -> JournalReadResult {
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // `{ events: [], problems: [], empty: true, missing: true, truncatedTail: false }`.
            return JournalReadResult {
                events: Vec::new(),
                header: None,
                problems: Vec::new(),
                empty: true,
                missing: true,
                truncated_tail: false,
            };
        }
        Err(error) => {
            let message = error.to_string();

            return JournalReadResult {
                events: Vec::new(),
                header: None,
                problems: vec![JournalProblem {
                    line: 0,
                    kind: JournalProblemKind::Malformed,
                    reason: message,
                    tail: false,
                }],
                empty: true,
                missing: false,
                truncated_tail: false,
            };
        }
    };

    let mut events: Vec<JournalEventRecord> = Vec::new();
    let mut problems: Vec<JournalProblem> = Vec::new();
    let mut header: Option<JournalHeaderRecord> = None;

    // Per-turn expected next index (`:392`). A gap is corruption - appends are sequential - so it
    // is reported rather than silently closed.
    let mut expected: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();

    // A truncated tail is a record WITHOUT its terminating newline: the writer died mid-append.
    // `split` cannot see the difference, so the newline check decides - and it decides BEFORE the
    // loop, because `tail` means "this is the tolerated crash tail", NOT "this happened to be the
    // last line" (`:358-363`).
    let truncated_tail = !text.is_empty() && !text.ends_with('\n');

    let mut segments: Vec<&str> = text.split('\n').collect();

    // Drop only a TRAILING empty segment (every complete line ends with a newline). A blank line in
    // the middle stays and reports as corruption (`:366-369`).
    if segments.last().copied() == Some("") {
        segments.pop();
    }

    for (position, line) in segments.iter().enumerate() {
        let number = u64::try_from(position + 1).unwrap_or(position as u64);
        let last = position + 1 == segments.len();

        if line.is_empty() {
            problems.push(JournalProblem {
                line: number,
                kind: JournalProblemKind::Malformed,
                reason: "empty line".to_string(),
                // A blank line is never the crash tail, even at the end (`:375`).
                tail: false,
            });
            continue;
        }

        let parsed = match parse_line(line) {
            Ok(parsed) => parsed,
            Err(reason) => {
                let tail = last && truncated_tail;

                problems.push(JournalProblem {
                    line: number,
                    kind: JournalProblemKind::Malformed,
                    reason: if tail {
                        format!("{reason} (truncated tail)")
                    } else {
                        reason
                    },
                    tail,
                });
                continue;
            }
        };

        match parsed {
            ParsedLine::Header(record) => {
                // A header anywhere but line 1 is corruption, and the line is DROPPED rather than
                // kept as an event (`:397-402`).
                if position != 0 {
                    problems.push(JournalProblem {
                        line: number,
                        kind: JournalProblemKind::Shape,
                        reason: "header is not the first line".to_string(),
                        tail: false,
                    });
                    continue;
                }

                header = Some(record);
            }
            ParsedLine::Event(record) => {
                // The event still stands - the reader's job is to report, not to repair - but a log
                // whose first line is an event is a stray file, and that has to be said (`:404-406`).
                if header.is_none() {
                    problems.push(JournalProblem {
                        line: number,
                        kind: JournalProblemKind::Shape,
                        reason: "event before header".to_string(),
                        tail: false,
                    });
                }

                // The per-turn sequence gate (`:409-422`). Only a turn that has already produced a
                // line has an expectation, so the first event of a turn never reports a gap even when
                // the log starts at index 7 (a compaction of a pruned turn keeps the tail's indices).
                if let Some(next) = expected.get(&record.turn_id)
                    && record.index != next
                {
                    problems.push(JournalProblem {
                        line: number,
                        kind: JournalProblemKind::Sequence,
                        reason: format!(
                            "turn {} expected event index {next}, found {}",
                            record.turn_id, record.index
                        ),
                        // A gap is never a crash tail: appends are sequential, so a hole means a lost
                        // or rewritten line, and a reader must say so (`:416-418`).
                        tail: false,
                    });
                }

                expected.insert(record.turn_id.clone(), record.index + 1);
                events.push(record);
            }
        }
    }

    JournalReadResult {
        events,
        header,
        problems,
        empty: events.is_empty(),
        missing: false,
        truncated_tail,
    }
}

/// Validates one line (`journal.ts:162-225` `parseLine`): anything that is not a journal record of
/// the supported version is REPORTED, not repaired. The version gate comes FIRST, on both record
/// kinds, which is why a one-line version mismatch on the header cascades into `event before
/// header` on every event line below it.
fn parse_line(line: &str) -> Result<ParsedLine, String> {
    let parsed: Value =
        serde_json::from_str(line).map_err(|error| format!("not valid JSON: {error}"))?;

    let object = match parsed.as_object() {
        Some(object) => object,
        // `isRecordObject` (`:158-160`): an array is not a record either.
        None => return Err("not a JSON object".to_string()),
    };

    // `if (v !== HARNESS_JOURNAL_VERSION)` (`:174-179`). `JSON.stringify(v)` prints `undefined` for
    // an absent key, which is what serde's `Display` for a missing value cannot say - hence the
    // explicit arm.
    if object.get("v").and_then(Value::as_u64) != Some(u64::from(HARNESS_JOURNAL_VERSION)) {
        return Err(format!(
            "unsupported journal version {}; this server writes version {HARNESS_JOURNAL_VERSION}",
            shown(object.get("v"))
        ));
    }

    match object.get("type").and_then(Value::as_str) {
        Some("header") => {
            let session_id = match object.get("sessionId") {
                Some(Value::String(id)) if !id.is_empty() => id.clone(),
                _ => return Err("header sessionId is missing".to_string()),
            };

            let created_at = match object.get("createdAt").and_then(Value::as_u64) {
                Some(created_at) => created_at,
                None => return Err("header createdAt is missing".to_string()),
            };

            Ok(ParsedLine::Header(JournalHeaderRecord {
                session_id,
                created_at,
            }))
        }
        Some("event") => {
            let turn_id = match object.get("turnId") {
                Some(Value::String(id)) if !id.is_empty() => id.clone(),
                _ => return Err("event turnId is missing".to_string()),
            };

            let index = match object.get("index").and_then(Value::as_u64) {
                Some(index) => index,
                None => {
                    return Err(
                        "event index is missing or not a non-negative integer".to_string(),
                    );
                }
            };

            let at = match object.get("at").and_then(Value::as_u64) {
                Some(at) => at,
                None => return Err("event at is missing".to_string()),
            };

            let event = match object.get("event") {
                Some(Value::Object(payload)) if payload.contains_key("type") => {
                    if payload.get("type").is_some_and(Value::is_string) {
                        parsed.get("event").cloned().unwrap_or(Value::Null)
                    } else {
                        return Err("event payload is missing".to_string());
                    }
                }
                _ => return Err("event payload is missing".to_string()),
            };

            Ok(ParsedLine::Event(JournalEventRecord {
                turn_id,
                index,
                at,
                event,
            }))
        }
        // `unknown journal record type ${JSON.stringify(type)}` (`:223`).
        _ => Err(format!(
            "unknown journal record type {}",
            shown(object.get("type"))
        )),
    }
}

/// `JSON.stringify(value)` for a problem text: absent reads as `undefined`, everything else keeps
/// its JSON spelling.
fn shown(value: Option<&Value>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "undefined".to_string(),
    }
}

/// `true` for the failures a retry can plausibly clear - a sharing/lock violation, an access
/// denial, an existing name, or a directory that is not yet empty. Windows spells the first two
/// ERROR_SHARING_VIOLATION (32) and ERROR_LOCK_VIOLATION (33), which std reports as
/// `ErrorKind::Uncategorized`, so the raw codes are checked as well.
fn is_transient(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::DirectoryNotEmpty
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::ResourceBusy
    ) {
        return true;
    }

    matches!(
        error.raw_os_error(),
        Some(5) | Some(32) | Some(33) | Some(145) | Some(183) // Windows
            | Some(1) | Some(13) | Some(16) | Some(17) | Some(39) | Some(66) // POSIX
    )
}
