//! The durable per-session event journal (`<root>/harness/sessions/<sessionId>/events.jsonl`).
//!
//! TypeScript origin: `packages/underload/harness/src/server/journal.ts`.
//!
//! WHY A JOURNAL BESIDE THE CHECKPOINT (`journal.ts:3-13`)
//! ------------------------------------------------------
//! `state.json` is a CHECKPOINT: one atomic full-document replace. Rewriting it for every harness
//! event is O(every session's every event), so the journal is the other half of durability: an
//! append-only, one-line-per-event log that is O(1) per event and survives a crash.
//!
//! READ MODEL (`journal.ts:15-22`, restated for this envelope)
//! ----------------------------------------------------------
//!   * a TRUNCATED LAST LINE is a crash artifact: it was never a proven-complete append, so a
//!     snapshot fold drops it silently, while an explicit read reports it as a problem - a reader
//!     must never treat a repaired-by-skipping stream as healthy;
//!   * corruption ANYWHERE ELSE is EXPLICIT: a malformed middle line, a blank line, an unknown
//!     version, a sequence gap, or an event recorded before the header produces a problem string
//!     that is never a tail note.
//!
//! LINE FORMAT
//! -----------
//! The header line is TS's, verbatim (`journalLine({ type: 'header', ... })`, `:280`):
//!
//! ```text
//! {"type":"header","v":1,"sessionId":"<id>","createdAt":<ms>}
//! ```
//!
//! One event line is the stage brief's envelope - a per-file monotonic sequence rather than TS's
//! per-turn `(turnId, index)` pair, which is what `read(from_sequence, limit)` replays from:
//!
//! ```text
//! {"version":1,"sequence":<n>,"timestamp":<ms>,"event":{...}}
//! ```
//!
//! DURABILITY LEVELS (`journal.ts:33-45`)
//! -------------------------------------
//!   * `append` writes to a held-open fd: safe across a PROCESS crash, not fenced against a power
//!     loss. `durable: true` adds the fsync fence the caller asked for;
//!   * `flush` is that fence on its own;
//!   * `rewrite` fsyncs BEFORE the rename, so a compaction publishes complete bytes.
//!
//! ONE WRITER (`journal.ts:54-66`)
//! ------------------------------
//! One instance per live session, holding the append handle. A second writer of the same file is
//! exactly what the store's serialized commit exists to prevent, so this type is `&mut`-self
//! throughout rather than internally-synchronized.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};

use crate::contract::HarnessEvent;
use crate::error::ApiError;
use crate::events;

/// The journal format version (`journal.ts:84`). A line whose version is not this one is a
/// reported problem, never a guess.
pub const HARNESS_JOURNAL_VERSION: u32 = 1;

/// The journal file inside one session's directory (`journal.ts:83`).
pub const HARNESS_JOURNAL_FILE: &str = "events.jsonl";

/// How many times a Windows sharing violation is retried before a removal is reported. Node's
/// writer never needs this: POSIX unlinks an open file, Windows refuses while a handle exists.
const REMOVE_ATTEMPTS: usize = 6;

/// Per-attempt backoff for the removal retry, in milliseconds.
const REMOVE_BACKOFF_MS: [u64; REMOVE_ATTEMPTS] = [0, 5, 10, 25, 50, 100];

/// A journal failure naming the file, matching `HarnessPersistenceError`'s convention so an
/// operator finds the file without guessing (`journal.ts:137-147`).
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

/// The header record every journal starts with, so a reader can tell "an empty journal for a
/// real session" from "a stray file" and gate a future format change on a version (`:86-94`).
#[must_use]
pub fn journal_header(session_id: &str, created_at: u64) -> Value {
    json!({
        "type": "header",
        "v": HARNESS_JOURNAL_VERSION,
        "sessionId": session_id,
        "createdAt": created_at,
    })
}

/// One event record, in the envelope this port writes (`:99-106` restated for a sequence-based
/// log): `{ version, sequence, timestamp, event }`.
#[must_use]
pub fn journal_event(sequence: u64, timestamp: u64, event: &HarnessEvent) -> Value {
    json!({
        "version": HARNESS_JOURNAL_VERSION,
        "sequence": sequence,
        "timestamp": timestamp,
        "event": serde_json::to_value(event).unwrap_or(Value::Null),
    })
}

/// Owns one session's journal file: its directory, its append handle, and the sequence the next
/// append will use.
#[derive(Debug)]
pub struct HarnessJournal {
    /// `<sessions_dir>/<sessionId>` - the whole directory belongs to this journal (`:245`).
    directory: PathBuf,
    /// `<directory>/events.jsonl` (or the explicit file name a caller passed).
    file: PathBuf,
    /// The session this journal belongs to, kept because the header line names it.
    session_id: String,
    /// The held-open append handle. `append` on a fresh fd per event is one syscall pair per
    /// DELTA; a persistent fd plus `flush` at the boundaries that matter is the durability story
    /// the checkpoint already tells (`:233-238`).
    handle: Option<File>,
    /// The sequence the next appended event will get (1-based, monotonic).
    next_sequence: u64,
    /// Bytes currently on disk, maintained so `size()` needs no stat.
    size: u64,
    /// Whether the directory and the header line exist.
    opened: bool,
    /// Last journal failure (open/append/flush/rewrite/remove), cleared by the next success
    /// (`:250-254`).
    last_error: Option<String>,
    /// Count of failed writes, so the store can surface "the journal is degraded" without
    /// re-reading the error text (`:256-258`).
    append_failures: u64,
}

impl HarnessJournal {
    /// Creates the session directory, writes the header once, and recovers the sequence from an
    /// existing file.
    ///
    /// `events_file_name` overrides the file name for a caller that needs a second log in the
    /// same directory; `None` is the shipped `events.jsonl`.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the directory cannot be created, the header
    /// cannot be written, or the existing log cannot be read.
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
            next_sequence: 1,
            size: 0,
            opened: false,
            last_error: None,
            append_failures: 0,
        };

        journal.create_and_scan()?;

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

    /// The sequence the next append will use - i.e. one past the highest durable record.
    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
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

    /// Appends one event and returns the sequence it was written with.
    ///
    /// `durable: true` fsyncs the tail before returning, which is the fence a caller takes when
    /// it wants "every event of this turn is on stable storage before the response"
    /// (`journal.ts:38-41`). A plain append is process-crash-safe only.
    ///
    /// A failure does NOT abort the caller's run (`journal.ts:24-29`: a full disk must not change
    /// a run's outcome) - it is returned so the caller can record it, and is retained in
    /// [`HarnessJournal::error`] either way.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the write or the fence fails.
    pub fn append(&mut self, event: &HarnessEvent, durable: bool) -> Result<u64, ApiError> {
        self.ensure_open()?;

        let sequence = self.next_sequence;
        let line = journal_line(&journal_event(sequence, events::now_millis(), event));

        match self.write_bytes(line.as_bytes()) {
            Ok(()) => {
                // The append itself succeeded, so the sequence and the byte count advance whether
                // or not the fence does: the bytes are in the file, and a counter that disagreed
                // with the file would mint a duplicate sequence on the next retry.
                self.next_sequence = sequence + 1;
                self.size += u64::try_from(line.len()).unwrap_or(line.len() as u64);

                let fenced = if durable { self.fsync() } else { Ok(()) };

                match fenced {
                    Ok(()) => {
                        self.last_error = None;
                        Ok(sequence)
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
                // Drop the handle so the next append reopens: a directory removed under us must
                // not wedge every later write (`:308-311`).
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

    /// Reads the durable events, newest records last.
    ///
    /// `from_sequence` is INCLUSIVE; `None` reads from the beginning. `limit` caps the number of
    /// records; `None` reads all of them. Each returned value is the whole line record
    /// (`{ version, sequence, timestamp, event }`), because a reader needs the sequence to
    /// continue the cursor and the timestamp to stamp the frame it rebuilds.
    ///
    /// This is the SNAPSHOT fold: a truncated tail is dropped silently (see the module header).
    /// Use [`HarnessJournal::read_all_with_problems`] for the explicit read that reports it.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the file exists but cannot be read.
    pub fn read(
        &mut self,
        from_sequence: Option<u64>,
        limit: Option<u64>,
    ) -> Result<Vec<Value>, ApiError> {
        let (records, _problems) = self.scan(false)?;

        let floor = from_sequence.unwrap_or(1);
        let cap = limit.unwrap_or(u64::MAX);

        let selected = records
            .into_iter()
            .filter(|record| record.get("sequence").and_then(Value::as_u64).unwrap_or(0) >= floor)
            .take(usize::try_from(cap).unwrap_or(usize::MAX))
            .collect();

        Ok(selected)
    }

    /// Every durable event record plus every structural problem found on the way
    /// (`journal.ts:328-434` `read`, flattened to strings for this port's reporter channel).
    ///
    /// This is the EXPLICIT read: the truncated tail is reported as a problem rather than being
    /// folded away, and a parseable-but-unterminated tail record is still returned (the record
    /// itself is complete; only its durability is unproven).
    ///
    /// # Errors
    ///
    /// As [`HarnessJournal::read`].
    pub fn read_all_with_problems(&mut self) -> Result<(Vec<Value>, Vec<String>), ApiError> {
        self.scan(true)
    }

    /// Replaces the whole log with `events` - header first, sequences renumbered from 1 - through
    /// an atomic tmp + rename (`journal.ts:439-487` `rewrite`, the checkpoint's rule).
    ///
    /// Used after a checkpoint proves the same events are durable in `state.json`, and to repair a
    /// file whose tail was truncated.
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the file when the staging write, the fence, or the rename
    /// fails.
    pub fn rewrite(&mut self, events: &[HarnessEvent]) -> Result<(), ApiError> {
        self.ensure_open()?;

        // The worker must not hold the file the rename replaces (`:443`).
        self.close_handle();

        let timestamp = events::now_millis();
        let records: Vec<Value> = events
            .iter()
            .map(|event| journal_event(0, timestamp, event))
            .collect();

        match self.commit_records(&records) {
            Ok(count) => {
                self.next_sequence = count + 1;
                self.last_error = None;
                Ok(())
            }
            Err(error) => {
                self.note_failure(&error);

                Err(journal_error(&self.file, &error))
            }
        }
    }

    /// Keeps the first `keep_count` events and rewrites the rest away
    /// (`journal.ts:491-493` `truncate`, generalized to a retained count).
    ///
    /// The retained records are re-emitted VERBATIM (their recorded `timestamp` included) and only
    /// renumbered, because a compaction must not invent new facts about events it did not change.
    ///
    /// # Errors
    ///
    /// As [`HarnessJournal::rewrite`].
    pub fn truncate_after(&mut self, keep_count: usize) -> Result<(), ApiError> {
        self.ensure_open()?;
        self.close_handle();

        let (records, _problems) = self.scan(false)?;
        let kept: Vec<Value> = records.into_iter().take(keep_count).collect();

        match self.commit_records(&kept) {
            Ok(count) => {
                self.next_sequence = count + 1;
                self.last_error = None;
                Ok(())
            }
            Err(error) => {
                self.note_failure(&error);

                Err(journal_error(&self.file, &error))
            }
        }
    }

    /// Closes the handle and removes this session's whole journal directory
    /// (`journal.ts:496-506` `remove`).
    ///
    /// # Errors
    ///
    /// [`ApiError::internal`] naming the directory when the removal keeps failing after the
    /// bounded retry.
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

    /// Creates the directory, writes the header once, and recovers `next_sequence`/`size`.
    fn create_and_scan(&mut self) -> Result<(), ApiError> {
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

        let (records, _problems) = self.scan_file(&self.file, false)?;

        self.next_sequence = records
            .iter()
            .filter_map(|record| record.get("sequence").and_then(Value::as_u64))
            .max()
            .map_or(1, |highest| highest + 1);
        self.size = fs::metadata(&self.file)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        self.opened = true;
        self.last_error = None;

        Ok(())
    }

    /// Re-creates the directory/header if a `remove` or an external cleanup took the file away.
    fn ensure_open(&mut self) -> Result<(), ApiError> {
        if self.opened && self.file.exists() {
            return Ok(());
        }

        self.create_and_scan()
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

    /// Re-emits `records` under a fresh header (sequences renumbered from 1, a recorded
    /// `timestamp` kept) and commits the result atomically. Returns the number of records written.
    fn commit_records(&mut self, records: &[Value]) -> Result<u64, String> {
        let timestamp = events::now_millis();
        let mut payload = journal_line(&journal_header(&self.session_id, timestamp));
        let mut sequence = 1_u64;

        for record in records {
            let mut entry = record.clone();

            if let Some(object) = entry.as_object_mut() {
                object.insert("version".to_string(), json!(HARNESS_JOURNAL_VERSION));
                object.insert("sequence".to_string(), json!(sequence));

                let stamped = object.get("timestamp").is_some_and(Value::is_number);

                if !stamped {
                    object.insert("timestamp".to_string(), json!(timestamp));
                }
            }

            payload.push_str(&journal_line(&entry));
            sequence += 1;
        }

        match self.commit_payload(&payload) {
            Ok(()) => {
                self.size = u64::try_from(payload.len()).unwrap_or(payload.len() as u64);

                Ok(sequence - 1)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically replaces the file with `payload` (staging + fsync + rename, then a bounded retry
    /// over a Windows sharing violation).
    fn commit_payload(&mut self, payload: &str) -> Result<(), String> {
        let mut last_error = String::new();

        for (attempt, backoff) in REMOVE_BACKOFF_MS.iter().enumerate() {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(*backoff));
            }

            let staging = self.directory.join(format!(
                "{}.{}.{}.tmp",
                HARNESS_JOURNAL_FILE, self.session_id, attempt
            ));

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
                    // fsync BEFORE the rename, the checkpoint's rule (`:458-461`).
                    handle.sync_all().map_err(|error| error.to_string())
                })();

                drop(handle);
                written?;

                fs::rename(&staging, &self.file).map_err(|error| error.to_string())
            })();

            match staged {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let _ = fs::remove_file(&staging);
                    last_error = error;
                }
            }
        }

        Err(last_error)
    }

    /// The explicit-read scan.
    fn scan(&mut self, tail_is_problem: bool) -> Result<(Vec<Value>, Vec<String>), ApiError> {
        let (records, problems) = self.scan_file(&self.file, tail_is_problem)?;

        Ok((records, problems))
    }

    /// Reads and validates one journal file.
    ///
    /// Tolerance, in the order `journal.ts:357-433` applies it: the newline test decides whether
    /// the LAST line is the crash tail BEFORE any parsing, a blank line in the middle is
    /// corruption, an unknown version is corruption, and a sequence gap is never a tail.
    fn scan_file(
        &self,
        file: &Path,
        tail_is_problem: bool,
    ) -> Result<(Vec<Value>, Vec<String>), ApiError> {
        let text = match fs::read_to_string(file) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), Vec::new()));
            }
            Err(error) => {
                return Err(journal_error(
                    file,
                    &format!("Could not read the harness journal: {error}"),
                ));
            }
        };

        let mut records: Vec<Value> = Vec::new();
        let mut problems: Vec<String> = Vec::new();
        let mut header_seen = false;
        let mut expected: u64 = 1;

        // A truncated tail is a record WITHOUT its terminating newline: the writer died
        // mid-append. `split` cannot see the difference, so the newline check decides - and it
        // decides BEFORE the loop, because `tail` means "this is the tolerated crash tail", not
        // "this happened to be the last line" (`:358-363`).
        let truncated_tail = !text.is_empty() && !text.ends_with('\n');

        let mut segments: Vec<&str> = text.split('\n').collect();

        // Drop only a TRAILING empty segment (every complete line ends with a newline); a blank
        // line in the middle stays and reports as corruption (`:366-369`).
        if segments.last().copied() == Some("") {
            segments.pop();
        }

        for (position, line) in segments.iter().enumerate() {
            let number = position + 1;
            let tail = number == segments.len() && truncated_tail;

            if line.is_empty() {
                if tail && !tail_is_problem {
                    continue;
                }

                problems.push(format!(
                    "line {number}: empty line{}",
                    if tail { " (truncated tail)" } else { "" }
                ));
                continue;
            }

            let parsed = match parse_line(line) {
                Ok(parsed) => parsed,
                Err(reason) => {
                    if tail && !tail_is_problem {
                        // The snapshot fold: an append whose completion is unproven is dropped,
                        // exactly because the record was never complete.
                        continue;
                    }

                    problems.push(format!(
                        "line {number}: {reason}{}",
                        if tail { " (truncated tail)" } else { "" }
                    ));
                    continue;
                }
            };

            if tail && !tail_is_problem {
                continue;
            }

            match parsed {
                ParsedLine::Header => {
                    if position != 0 {
                        problems.push(format!("line {number}: header is not the first line"));
                    } else {
                        header_seen = true;
                    }
                }
                ParsedLine::Event(record) => {
                    if !header_seen {
                        problems.push(format!("line {number}: event before header"));
                    }

                    let sequence = record.get("sequence").and_then(Value::as_u64).unwrap_or(0);

                    if sequence != expected {
                        problems.push(format!(
                            "line {number}: expected event sequence {expected}, found {sequence}"
                        ));
                    }

                    expected = sequence + 1;
                    records.push(record);

                    if tail {
                        // A tail that PARSES is still returned - the record is complete, only its
                        // durability is unproven - but the file condition is reported, because a
                        // reader must never treat a repaired-by-skipping stream as healthy.
                        problems.push(format!(
                            "line {number}: record is complete but its terminating newline is missing (truncated tail)"
                        ));
                    }
                }
            }
        }

        Ok((records, problems))
    }

    /// Records a failure the way `noteWrite` does (`journal.ts:541-544`).
    fn note_failure(&mut self, message: &str) {
        self.last_error = Some(message.to_string());
        self.append_failures += 1;
    }

    /// Zeroes the recovered counters after a `remove`, so a reopened journal starts over.
    fn reset_counters(&mut self) {
        self.next_sequence = 1;
        self.size = 0;
    }
}

/// One parsed journal line.
enum ParsedLine {
    Header,
    Event(Value),
}

/// Validates one line (`journal.ts:162-225` `parseLine`): anything that is not a journal record of
/// the supported version is REPORTED, not repaired.
///
/// Both envelopes are accepted on read - this port's `{ version, sequence, timestamp, event }`
/// and TS's `{ type: 'event', v, turnId, index, at, event }` - so a database written by the TS
/// server still replays. An event recorded by TS keeps its own fields; the sequence gate is simply
/// skipped for it (`sequence` is absent, and the gap check treats 0 as "unknown").
fn parse_line(line: &str) -> Result<ParsedLine, String> {
    let parsed: Value =
        serde_json::from_str(line).map_err(|error| format!("not valid JSON: {error}"))?;

    let object = parsed.as_object().ok_or("not a JSON object")?;

    if object.get("type").and_then(Value::as_str) == Some("header") {
        let version = object.get("v").and_then(Value::as_u64);

        if version != Some(u64::from(HARNESS_JOURNAL_VERSION)) {
            return Err(format!(
                "unsupported journal version {}; this server writes version {HARNESS_JOURNAL_VERSION}",
                object
                    .get("v")
                    .map_or_else(|| "undefined".to_string(), |value| value.to_string())
            ));
        }

        if !object
            .get("sessionId")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty())
        {
            return Err("header sessionId is missing".to_string());
        }

        if !object.get("createdAt").is_some_and(Value::is_number) {
            return Err("header createdAt is missing".to_string());
        }

        return Ok(ParsedLine::Header);
    }

    let version = object
        .get("version")
        .or_else(|| object.get("v"))
        .and_then(Value::as_u64);

    if version != Some(u64::from(HARNESS_JOURNAL_VERSION)) {
        return Err(format!(
            "unsupported journal version {}; this server writes version {HARNESS_JOURNAL_VERSION}",
            version.map_or_else(|| "undefined".to_string(), |value| value.to_string())
        ));
    }

    let event = object
        .get("event")
        .and_then(Value::as_object)
        .ok_or("event payload is missing")?;

    if !event.get("type").is_some_and(Value::is_string) {
        return Err("event payload is missing".to_string());
    }

    if object
        .get("sequence")
        .is_some_and(|sequence| !sequence.is_number())
    {
        return Err("event sequence is missing or not a non-negative integer".to_string());
    }

    if object
        .get("timestamp")
        .is_some_and(|stamp| !stamp.is_number())
    {
        return Err("event at is missing".to_string());
    }

    Ok(ParsedLine::Event(parsed))
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
