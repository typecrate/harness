//! The command tools: `shell` and `gitDiff`.
//!
//! TypeScript origins
//! ------------------
//!   * `tools/command/shell.tool.ts` - the whole run contract: `COMMAND_DEFAULT_TIMEOUT_MS`
//!     (120 s) and the `COMMAND_MAX_TIMEOUT_MS` (600 000 ms) ceiling, the refusal
//!     sentences, `stdin` IGNORED unless `input` is given, the Windows `&&` line-chaining
//!     rule, the tree kill on timeout/abort, the full-output tee, and the
//!     `{exitCode, stdout, stderr, truncated, stdoutTruncated, stderrTruncated,
//!     stdoutBytes, stderrBytes, stdoutDroppedBytes, stderrDroppedBytes, outputPath,
//!     timedOut, aborted, exitSignal, durationMs, linesChained}` document;
//!   * `tools/command/boundedOutput.ts` - `BoundedOutput`: the HEAD+TAIL capture with the
//!     in-band `[... N bytes omitted ...]` marker, 1/4 head + 3/4 tail, per-stream
//!     `bytes`/`droppedBytes`/`truncated` (`DEFAULT_HEAD_SHARE = 4`);
//!   * `tools/command/boundedText.ts:26-27` - `COMMAND_STDOUT_MAX_BYTES` (128 KiB) and
//!     `COMMAND_STDERR_MAX_BYTES` (16 KiB), the shared caps; `BoundedText` keeps the HEAD
//!     and drops the TAIL, which is still what `gitDiff` does;
//!   * `tools/command/processTree.ts` - `processGroupOptions` (POSIX `detached`, i.e. the
//!     child LEADS a new process group) and `terminateTree` (`kill -9 -pgid` on POSIX,
//!     `taskkill /PID /T /F` then a root kill on Windows);
//!   * `tools/command/gitDiff.tool.ts` - `git diff` argument assembly (the `--` separator
//!     so a pathspec can never read as a flag, and the "must not start with `-`" rule on
//!     `ref`) and the HEAD-kept `{exitCode, stdout, stderr, truncated, stderrTruncated,
//!     timedOut, aborted}` shape.
//!
//! WHY HEAD *AND* TAIL FOR `shell` (`boundedOutput.ts`, header comment)
//! --------------------------------------------------------------------
//! The commands that actually overflow are builds, test runners and installers, and those
//! print their verdict LAST - `npm test` ends with the failure summary, `tsc` with the
//! error count. A head-only cap returns the least useful part and throws away the one line
//! the model needed, so `shell` keeps a head window and a tail window with the marker
//! spliced between them, reports EACH STREAM's own flag plus its true byte size and the
//! bytes the marker replaced, and tees the complete stream to a file so `grep`/`read` can
//! recover the middle without re-running the command. `gitDiff` keeps the HEAD-kept policy
//! because a diff is read top-down and its `diff --git`/`index` header lines are the
//! useful part - which is why it runs through `Sink::Head` rather than `Sink::HeadTail`.
//!
//! THE SHELL IS THE PLATFORM SHELL (`harness.ts`'s tool context has no sandbox):
//! `cmd /C` on Windows, `sh -c` everywhere else. TypeScript reaches for `shell: true`;
//! both forms run the command string through a real shell, so quoting rules are the host
//! shell's, not this tool's.
//!
//! A non-zero exit code is a SUCCESSFUL call: `git diff` exits 1 when there are
//! differences, and the model reads `exitCode` to decide what the output means.
//!
//! WHY ONE BLOCKING OWNER (`shell.tool.ts` awaits `child.exit`, which is an OS wait)
//! --------------------------------------------------------------------------------
//! This crate's async runtime cannot borrow a child mutably in a `select!` condition and
//! then kill it in the winning arm, so the whole run - wait, deadline, cancellation poll,
//! tree kill, drain join - lives in ONE blocking task that owns the child, and the token is
//! handed to it as a flag a watcher task sets. Same observable behaviour, and it keeps the
//! "a run that already finished is never reported as cancelled" rule the reference tests
//! pin (`shell.test.ts:346`): the flag is only honoured while the child is still alive.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use serde_json::json;
use tokio_util::sync::CancellationToken;

/// `COMMAND_DEFAULT_TIMEOUT_MS` (`shell.tool.ts:145`): a hung command must not hold a turn.
pub const COMMAND_DEFAULT_TIMEOUT_MS: i64 = 120_000;
/// `COMMAND_MAX_TIMEOUT_MS` (`shell.tool.ts:154`): a longer run belongs in `task`, so a
/// bigger request is clamped rather than refused.
pub const COMMAND_MAX_TIMEOUT_MS: i64 = 600_000;
/// `COMMAND_STDOUT_MAX_BYTES` (`boundedText.ts:26`).
pub const COMMAND_STDOUT_MAX_BYTES: usize = 128 * 1024;
/// `COMMAND_STDERR_MAX_BYTES` (`boundedText.ts:27`). A build is loud on stdout; the
/// diagnosis is short and lives on stderr, so the smaller cap goes there.
pub const COMMAND_STDERR_MAX_BYTES: usize = 16 * 1024;
/// `DEFAULT_HEAD_SHARE` (`boundedOutput.ts:32`): 1 part head to 3 parts tail.
pub const DEFAULT_HEAD_SHARE: usize = 4;
/// `MAX_TRANSCRIPT_BYTES` (`shell.tool.ts:47`): the tee stops rather than filling the disk.
pub const OUTPUT_FILE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// `OUTPUT_FOLDER` (`shell.tool.ts:52`): inside the session workspace, so `read`/`grep`
/// reach it by the same relative path as every other artifact.
pub const OUTPUT_FOLDER: &str = ".underload/shell";
/// `shell.tool.ts:49` `KILL_GRACE_MS`: bound the post-kill pipe drain so a grandchild
/// holding a stream cannot hang the call after the tree is already dead.
pub const KILL_GRACE_MS: u64 = 2_000;
/// `processTree.ts:19`: the bound on the Windows `taskkill` invocation itself.
pub const TASKKILL_TIMEOUT_MS: u64 = 2_000;
/// How often the owning blocking task re-checks the cancellation flag. The token is an async
/// primitive, so the runner polls it rather than awaiting it; 50 ms is far below every
/// timeout this tool can be given and far above any scheduling jitter worth a wakeup.
const CANCEL_POLL_MS: u64 = 50;

/// `omissionMarker` (`boundedOutput.ts:49`): the in-band marker naming how many bytes the
/// user of the result cannot see. ASCII on purpose - it must survive every decoder and be
/// greppable.
pub fn omission_marker(dropped_bytes: usize) -> String {
    if dropped_bytes == 1 {
        return "[... 1 byte omitted ...]".to_owned();
    }

    format!("[... {dropped_bytes} bytes omitted ...]")
}

/// `BoundedOutput` (`boundedOutput.ts:62`): head+tail bounded capture.
///
/// The invariant the reference states (`:181-184`): the marker's count is the SAME number
/// `dropped_bytes` reports, so the published text and the published metadata can never
/// disagree about how much is missing. Filling the cap EXACTLY is not truncation (`:129`).
#[derive(Clone, Debug, Default)]
pub struct BoundedOutput {
    /// `maxBytes`.
    max_bytes: usize,
    /// `headKeep`: at most `maxBytes / DEFAULT_HEAD_SHARE`, and never so large that the tail
    /// has no room (`:121` - "the tail must keep at least one byte, or a truncated stream
    /// would lose its ending").
    head_bytes: usize,
    /// `tailKeep = maxBytes - headKeep`.
    tail_bytes: usize,
    /// `headBuffer`/`headFilled`.
    head: Vec<u8>,
    /// `tailBuffer` as a ring of the LAST `tailKeep` bytes, in stream order.
    tail: VecDeque<u8>,
    /// `total`.
    bytes_seen: usize,
}

impl BoundedOutput {
    /// `new(maxBytes)` (`:105`) with the documented 1/4 head, 3/4 tail split.
    pub fn new(max_bytes: usize) -> Self {
        Self::with_head(max_bytes, max_bytes / DEFAULT_HEAD_SHARE)
    }

    /// `new(maxBytes, headBytes)` (`:105`): the configurable split.
    ///
    /// The reference THROWS a `RangeError` on a non-positive cap or a bad head window; every
    /// caller in this crate passes a compile-time constant, so the Rust port clamps instead
    /// of offering an error path no caller can reach.
    pub fn with_head(max_bytes: usize, head_bytes: usize) -> Self {
        let max_bytes = max_bytes.max(1);
        let head_bytes = head_bytes.min(max_bytes.saturating_sub(1));

        Self {
            max_bytes,
            head_bytes,
            tail_bytes: max_bytes - head_bytes,
            ..Self::default()
        }
    }

    /// `truncated` (`:129`): true once the stream passed the cap. Filling it exactly is NOT
    /// truncation, which is the same rule `BoundedText` follows.
    pub fn was_truncated(&self) -> bool {
        self.bytes_seen > self.max_bytes
    }

    /// `totalBytes` (`:134`): the true size of the stream, whatever `end()` keeps.
    pub fn bytes_seen(&self) -> usize {
        self.bytes_seen
    }

    /// `keptBytes` (`:148`): the bytes that actually reach the returned text, after the
    /// codepoint-boundary trims.
    pub fn kept_bytes(&self) -> usize {
        let (head, tail) = self.trimmed_windows();

        head.len() + tail.len()
    }

    /// `droppedBytes` (`:142`): the count the marker names, so a caller can page the gap
    /// without guessing.
    pub fn dropped_bytes(&self) -> usize {
        self.bytes_seen.saturating_sub(self.kept_bytes())
    }

    /// `write(chunk)` (`:162`).
    ///
    /// Edge cases, all ported from the reference:
    ///   - an empty chunk is ignored (`:163`);
    ///   - the head absorbs the chunk while it is short, so early bytes are always kept;
    ///   - once the head is full every later byte goes into the tail ring, whose oldest byte
    ///     is evicted - which is how the MIDDLE is dropped while BOTH ends survive;
    ///   - a chunk at least as large as the tail window keeps only ITS last `tailKeep` bytes
    ///     (`:210`), because only those can still be the stream's end;
    ///   - a chunk can fill the head and spill into the tail in the same call;
    ///   - nothing is decoded until `end()`, so a codepoint split across chunks stays whole.
    pub fn write(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }

        self.bytes_seen += chunk.len();

        let room = self.head_bytes.saturating_sub(self.head.len());
        let take = chunk.len().min(room);

        self.head.extend_from_slice(&chunk[..take]);

        let rest = &chunk[take..];

        if rest.is_empty() || self.tail_bytes == 0 {
            return;
        }

        if rest.len() >= self.tail_bytes {
            self.tail.clear();
            self.tail
                .extend(rest[rest.len() - self.tail_bytes..].iter().copied());
            return;
        }

        for byte in rest {
            self.tail.push_back(*byte);

            if self.tail.len() > self.tail_bytes {
                self.tail.pop_front();
            }
        }
    }

    /// `end()` (`:172`): head + marker + tail, or the whole stream when nothing was cut.
    /// Repeatable and side-effect free, so the accounting getters read the same numbers
    /// before or after it.
    pub fn end(&self) -> String {
        let (head, tail) = self.trimmed_windows();

        if !self.was_truncated() {
            // Under the cap the two windows concatenate to the whole stream: the tail is
            // only ever filled once the head is full.
            let mut text = String::from_utf8_lossy(&head).into_owned();

            text.push_str(&String::from_utf8_lossy(&tail));

            return text;
        }

        // The marker's count is `droppedBytes`, i.e. the SAME number the published field
        // carries (`:181`).
        let marker = omission_marker(self.dropped_bytes());

        // No stray newline on either side when a window is empty: a caller that asks for a
        // zero-byte head gets a marker that STARTS the text (`:186-188`).
        if head.is_empty() {
            return format!("{marker}\n{}", String::from_utf8_lossy(&tail));
        }

        if tail.is_empty() {
            return format!("{}\n{marker}", String::from_utf8_lossy(&head));
        }

        format!(
            "{}\n{marker}\n{}",
            String::from_utf8_lossy(&head),
            String::from_utf8_lossy(&tail)
        )
    }

    /// `trimmedWindows` (`:228`): the two windows exactly as `end()` decodes them. Over the
    /// cap each side is trimmed to a codepoint boundary so neither half can emit a lone
    /// U+FFFD; the dropped bytes therefore count the trimmed halves.
    fn trimmed_windows(&self) -> (Vec<u8>, Vec<u8>) {
        let head_view: Vec<u8> = self.head.clone();
        let tail_view: Vec<u8> = self.tail.iter().copied().collect();

        if !self.was_truncated() {
            return (head_view, tail_view);
        }

        (
            trim_incomplete_tail(&head_view).to_vec(),
            trim_incomplete_head(&tail_view).to_vec(),
        )
    }
}

/// Drops a trailing INCOMPLETE sequence so the head never ends mid character
/// (`trimIncompleteTail`, `boundedOutput.ts:62`).
///
/// Walking back over continuation bytes alone is NOT enough: `[E2]` has no continuation to
/// drop, yet decoding it alone emits a lone U+FFFD. So the scan finds the last lead byte
/// (within 3 bytes of the end) and drops the whole sequence when it is shorter than that
/// lead byte announces.
fn trim_incomplete_tail(bytes: &[u8]) -> &[u8] {
    let len = bytes.len();
    let mut index = len as i64 - 1;

    while index >= 0 && index >= len as i64 - 3 {
        let byte = bytes[index as usize];

        // A continuation byte (`0b10xx_xxxx`) belongs to an earlier lead byte.
        if byte & 0b1100_0000 == 0b1000_0000 {
            index -= 1;
            continue;
        }

        let need = if byte < 0x80 {
            1
        } else if byte & 0b1110_0000 == 0b1100_0000 {
            2
        } else if byte & 0b1111_0000 == 0b1110_0000 {
            3
        } else {
            4
        };

        return if len - index as usize >= need {
            bytes
        } else {
            &bytes[..index as usize]
        };
    }

    bytes
}

/// Drops a leading partial sequence from the tail window (`boundedOutput.ts:377`). A
/// continuation byte cannot START a valid codepoint, so it goes.
fn trim_incomplete_head(bytes: &[u8]) -> &[u8] {
    let mut start = 0;

    while start < bytes.len() && bytes[start] & 0b1100_0000 == 0b1000_0000 {
        start += 1;
    }

    &bytes[start..]
}

/// What one stream publishes: the text the model sees plus the flags describing the cut
/// (`shell.tool.ts`'s four stdout fields, and the same pair for gitDiff's head-kept case).
struct StreamReport {
    text: String,
    truncated: bool,
    bytes: usize,
    dropped: usize,
    /// The RAW mirror that becomes the tee body: `rawStdout` / `rawStderr`
    /// (`shell.tool.ts:492-493`), an independent `BoundedOutput(OUTPUT_FILE_MAX_BYTES)`.
    /// Only `Sink::HeadTail` with `saveOutput` on keeps one, and it keeps one for BOTH
    /// streams even when only one of them was cut, because the tee file always carries
    /// both sections (`:401-409`).
    transcript: Option<BoundedOutput>,
}

/// A stream's retention policy.
enum Sink {
    /// `BoundedOutput` + the raw mirror for the tee: `shell`. The mirror is itself capped at
    /// `OUTPUT_FILE_MAX_BYTES` by its own `BoundedOutput`, so a 500 MB stream stays bounded
    /// in memory AND in the file. `saveOutput` is NOT part of the sink: the mirror is fed
    /// either way and the flag is consulted once, at write time (`RunRequest::tee`).
    HeadTail {
        capture: BoundedOutput,
        raw: BoundedOutput,
    },
    /// `BoundedText` (`boundedText.ts:26-27`): keep the head, drop the tail: `gitDiff`.
    Head {
        bytes: Vec<u8>,
        total: usize,
        cap: usize,
    },
    /// The placeholder left behind when the owner takes the sink out of its lock while a
    /// straggler drain thread may still write into it.
    Empty,
}

impl Sink {
    /// Feeds one read chunk. Bytes are counted even after the retention stops, because the
    /// published `*Bytes` fields are the TRUE stream size.
    fn write(&mut self, chunk: &[u8]) {
        match self {
            Self::Empty => {}
            Self::HeadTail { capture, raw, .. } => {
                capture.write(chunk);

                // The 64x mirror is fed whether or not `saveOutput` is on, because that is
                // what the reference does (`shell.tool.ts:492-493` builds `rawStdout` and
                // `rawStderr` unconditionally). Its own `BoundedOutput` cap is the only
                // thing keeping a 500 MB stream bounded in memory; `tee` decides later
                // whether the mirror is worth a file.
                raw.write(chunk);
            }
            Self::Head { bytes, total, cap } => {
                *total += chunk.len();
                let room = cap.saturating_sub(bytes.len());

                if room > 0 {
                    bytes.extend_from_slice(&chunk[..chunk.len().min(room)]);
                }
            }
        }
    }

    /// Builds the published fields once the stream has ended (or the run was killed).
    /// Builds the published fields once the stream has ended (or the run was killed).
    fn finish(self) -> StreamReport {
        match self {
            Self::Empty => StreamReport {
                text: String::new(),
                truncated: false,
                bytes: 0,
                dropped: 0,
                transcript: None,
            },
            Self::HeadTail { capture, raw, .. } => StreamReport {
                text: capture.end(),
                truncated: capture.was_truncated(),
                bytes: capture.bytes_seen(),
                dropped: capture.dropped_bytes(),
                transcript: Some(raw),
            },
            Self::Head { bytes, total, cap } => {
                let kept = capture(&bytes, cap);

                StreamReport {
                    text: kept.text,
                    truncated: kept.truncated,
                    bytes: total,
                    dropped: total.saturating_sub(cap),
                    transcript: None,
                }
            }
        }
    }
}

/// One captured stream: the kept head plus the flags saying what was dropped
/// (`BoundedText`'s shape, still what `gitDiff` publishes).
///
/// The byte cap is applied on a character boundary, so a truncated stream never ends in
/// half a UTF-8 sequence the model would read as mojibake.
struct Captured {
    text: String,
    truncated: bool,
}

fn capture(bytes: &[u8], cap: usize) -> Captured {
    if bytes.len() <= cap {
        return Captured {
            text: String::from_utf8_lossy(bytes).into_owned(),
            truncated: false,
        };
    }

    let mut cut = cap;

    // Walk back off a UTF-8 CONTINUATION byte until the window ends on a character boundary.
    while cut > 0 && bytes[cut - 1] & 0b1100_0000 == 0b1000_0000 {
        cut -= 1;
    }

    Captured {
        text: String::from_utf8_lossy(&bytes[..cut]).into_owned(),
        truncated: true,
    }
}

/// `signalName` (`shell.tool.ts:111`): the published field is the SIGNAL NAME, not a bare
/// number, because the model reads it. Windows never reports one, so it stays `null` there.
///
/// `tools/syntax` reuses this table: its Python checker publishes the same kind of sentence
/// (`syntax Python interpreter ended on signal SIGTERM.`), and one table keeps the two tools
/// naming a signal identically.
pub(crate) fn signal_name(signal: i32) -> String {
    let named = match signal {
        1 => "SIGHUP",
        2 => "SIGINT",
        3 => "SIGQUIT",
        6 => "SIGABRT",
        8 => "SIGFPE",
        9 => "SIGKILL",
        11 => "SIGSEGV",
        12 => "SIGSTOP",
        13 => "SIGPIPE",
        14 => "SIGALRM",
        15 => "SIGTERM",
        19 => "SIGSTOP",
        18 => "SIGCONT",
        28 => "SIGWINCH",
        _ => "",
    };

    if named.is_empty() {
        return format!("SIG{signal}");
    }

    named.to_owned()
}

/// The signal that ended the run, if the platform reported one: `status.signal()` on
/// unix, always `None` on Windows, where the kill goes through `taskkill`
/// (`shell.tool.ts:534`, `signalCode === null`).
fn exit_signal_of(status: &std::process::ExitStatus) -> Option<String> {
    #[cfg_attr(windows, allow(unused_variables))]
    fn pick(number: Option<i32>) -> Option<String> {
        number.map(signal_name)
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        pick(status.signal())
    }

    #[cfg(not(unix))]
    {
        pick(None)
    }
}

/// `runShell`'s outcome, already in the shape the published document uses.
struct Ran {
    exit_code: i64,
    exit_signal: Option<String>,
    stdout: StreamReport,
    stderr: StreamReport,
    timed_out: bool,
    aborted: bool,
    duration_ms: u64,
    lines_chained: u64,
    tee_path: Option<std::path::PathBuf>,
}

impl Ran {
    /// A run that never started because the token was already cancelled
    /// (`shell.test.ts:442`): exitCode -1, `aborted`, nothing captured.
    fn never_spawned(lines_chained: u64) -> Self {
        Self {
            exit_code: -1,
            exit_signal: None,
            stdout: StreamReport {
                text: String::new(),
                truncated: false,
                bytes: 0,
                dropped: 0,
                transcript: None,
            },
            stderr: StreamReport {
                text: String::new(),
                truncated: false,
                bytes: 0,
                dropped: 0,
                transcript: None,
            },
            timed_out: false,
            aborted: true,
            duration_ms: 0,
            lines_chained,
            tee_path: None,
        }
    }

    /// The published model-facing document (`shell.tool.ts:542`). Key order and field
    /// names are the contract; `durationMs` is real wall-clock, never an estimate.
    fn publish(&self) -> Value {
        json!({
            "exitCode": self.exit_code,
            "stdout": self.stdout.text,
            "stderr": self.stderr.text,
            "truncated": self.stdout.truncated || self.stderr.truncated,
            "stdoutTruncated": self.stdout.truncated,
            "stderrTruncated": self.stderr.truncated,
            "stdoutBytes": self.stdout.bytes,
            "stderrBytes": self.stderr.bytes,
            "stdoutDroppedBytes": self.stdout.dropped,
            "stderrDroppedBytes": self.stderr.dropped,
            "outputPath": self.tee_path.as_ref().map(|path| path.display().to_string()),
            "timedOut": self.timed_out,
            "aborted": self.aborted,
            "exitSignal": self.exit_signal.clone(),
            "durationMs": self.duration_ms,
            "linesChained": self.lines_chained,
        })
    }
}

/// `processGroupOptions` (`processTree.ts:24`): POSIX `detached: true` / Windows
/// `CREATE_NEW_PROCESS_GROUP`. The child LEADS a new group, so `terminate_tree` can signal
/// every descendant and not just the shell that wraps them.
#[cfg(unix)]
fn set_group_leader(builder: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;

    builder.process_group(0);
}

#[cfg(not(unix))]
fn set_group_leader(builder: &mut std::process::Command) {
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    use std::os::windows::process::CommandExt;

    builder.creation_flags(CREATE_NEW_PROCESS_GROUP);
}

/// `terminateTree` (`processTree.ts:32`): kill the whole tree, then the root.
///
/// A crate-wide `unsafe` ban rules out `libc::killpg`, so the group signal goes through
/// `/bin/kill -9 -<pgid>` - the same `kill -9 -PID` the reference runs. Windows uses
/// `taskkill /T /F`, exactly as the reference does, and gets the reference's 2 s bound on
/// the killer itself so a wedged `taskkill` cannot hold the call open.
#[cfg_attr(unix, allow(unused_variables))]
fn terminate_tree(child_pid: u32, process_group: u32) {
    #[cfg(unix)]
    {
        // Bounded like the Windows branch: `/bin/kill` is exec'd and reaped with a ceiling.
        run_bounded(
            "/bin/kill",
            &["-9".to_owned(), format!("-{process_group}")],
            TASKKILL_TIMEOUT_MS,
        );
    }

    #[cfg(not(unix))]
    {
        run_bounded(
            "taskkill",
            &[
                "/PID".to_owned(),
                child_pid.to_string(),
                "/T".to_owned(),
                "/F".to_owned(),
            ],
            TASKKILL_TIMEOUT_MS,
        );
    }
}

/// Runs one short-lived helper process with a wall-clock ceiling.
///
/// The reference bounds its killers (`timeout: TASKKILL_TIMEOUT_MS`); `Command::output()`
/// has no timeout, so the child is polled and killed instead.
fn run_bounded(program: &str, arguments: &[String], ceiling_ms: u64) {
    let spawned = std::process::Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    let mut killer = match spawned {
        Ok(child) => child,
        Err(_) => return,
    };

    let deadline = Instant::now() + Duration::from_millis(ceiling_ms);

    loop {
        match killer.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => {}
        }

        if Instant::now() >= deadline {
            let _ = killer.kill();
            let _ = killer.wait();
            return;
        }

        std::thread::sleep(Duration::from_millis(20));
    }
}

/// One stream's drain thread: reads to EOF and feeds the shared sink.
///
/// The sink lives behind a lock so the owning thread can build the published report at the
/// grace deadline even if a grandchild is still holding the pipe open - the same
/// "await the drainers, bounded by `KILL_GRACE_MS`" shape the reference uses. `done` is set
/// just before the thread leaves, which is how the owner knows the capture is complete.
fn drain_thread<R>(
    pipe: Option<R>,
    sink: &Arc<Mutex<Sink>>,
    done: &Arc<AtomicBool>,
) -> Option<std::thread::JoinHandle<()>>
where
    R: Read + Send + 'static,
{
    let sink = Arc::clone(sink);
    let done = Arc::clone(done);

    pipe.map(move |mut pipe| {
        std::thread::spawn(move || {
            let mut chunk = vec![0_u8; 8 * 1024];

            loop {
                match pipe.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        let mut guard =
                            sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

                        guard.write(&chunk[..read]);
                    }
                }
            }

            done.store(true, Ordering::SeqCst);
        })
    })
}

/// Waits for both drain threads up to the grace ceiling.
fn wait_for_drains(first: &AtomicBool, second: &AtomicBool, ceiling_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(ceiling_ms);

    while (!first.load(Ordering::SeqCst) || !second.load(Ordering::SeqCst))
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Takes a settled sink out of its shared lock.
///
/// Swapping rather than unwrapping the `Arc` matters: if a drain thread outlived the grace
/// window it still holds a clone, and an unwrap would have to throw the captured bytes away.
/// The straggler's later writes land in the `Sink::Empty` placeholder and change nothing.
fn take_sink(sink: &Arc<Mutex<Sink>>) -> Sink {
    let mut guard = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    std::mem::replace(&mut guard, Sink::Empty)
}

/// `OUTPUT_FOLDER` + the unique per-run name (`shell.tool.ts:409`): the time, the pid and a
/// process-wide sequence, so two runs in one workspace can never overwrite each other.
fn output_file_path(directory: &std::path::Path) -> std::path::PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);

    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0);
    let sequence = SEQ.fetch_add(1, Ordering::SeqCst);

    let folder = directory.join(OUTPUT_FOLDER);

    if std::fs::create_dir_all(&folder).is_err() {
        return folder;
    }

    folder.join(format!(
        "{}-{}-{}.txt",
        base36(millis),
        base36(u64::from(std::process::id())),
        base36(sequence)
    ))
}

/// Base-36, the reference's `(.+base36.+)` file-name format.
fn base36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

    if value == 0 {
        return "0".to_owned();
    }

    let mut digits = Vec::new();

    while value > 0 {
        digits.push(DIGITS[(value % 36) as usize]);
        value /= 36;
    }

    digits.reverse();

    String::from_utf8(digits).unwrap_or_default()
}

/// `command.replace(/[\r\n]+/g, ' ')` (`shell.tool.ts:404`): every RUN of CR/LF collapses to
/// ONE space. The tee is line-oriented and meant to be grepped, so a multi-line command must
/// not push the metadata lines around.
fn flatten_command(command: &str) -> String {
    let mut out = String::with_capacity(command.len());
    let mut collapsed = false;

    for character in command.chars() {
        if character == '\n' || character == '\r' {
            if !collapsed {
                out.push(' ');
                collapsed = true;
            }

            continue;
        }

        collapsed = false;
        out.push(character);
    }

    out
}

/// The tee's first two lines (`shell.tool.ts:404-405`): which command produced the file and
/// how big each stream really was. A reader holding only the file has to learn that.
fn full_output_header(
    command: &str,
    exit_code: i64,
    duration_ms: u64,
    stdout_bytes: usize,
    stderr_bytes: usize,
) -> String {
    format!(
        "# shell: {}\n# exitCode={exit_code} durationMs={duration_ms} stdout={stdout_bytes}B stderr={stderr_bytes}B",
        flatten_command(command)
    )
}

/// Writes the tee (`writeFullOutput`, `shell.tool.ts:388-419`): the two header lines, the
/// stdout mirror, a labelled stderr section, then the stderr mirror, joined by `\n`.
///
/// A failed write is NOT a run failure (`:416-418` - every error collapses to `null`):
/// `outputPath` stays absent and the result still carries the flags, because a result that
/// already has the head, the tail and the exact byte counts is not worth failing over a
/// scratch file.
fn write_full_output(
    path: &std::path::Path,
    command: &str,
    exit_code: i64,
    duration_ms: u64,
    stdout: &StreamReport,
    stderr: &StreamReport,
) -> bool {
    // BOTH sections are always written, cut or not (`:401-409`), and each one is that
    // mirror's `end()`: the whole stream when it fit in `OUTPUT_FILE_MAX_BYTES`, otherwise
    // head + `[... N bytes omitted ...]` + tail. That in-band marker is how a capped tee
    // says so - the reference has no separate "capped" note.
    let (Some(stdout_raw), Some(stderr_raw)) = (&stdout.transcript, &stderr.transcript) else {
        return false;
    };

    let body = [
        full_output_header(
            command,
            exit_code,
            duration_ms,
            stdout_raw.bytes_seen(),
            stderr_raw.bytes_seen(),
        ),
        stdout_raw.end(),
        format!("# --- stderr ({} bytes) ---", stderr_raw.bytes_seen()),
        stderr_raw.end(),
    ]
    .join("\n");

    std::fs::write(path, body).is_ok()
}

/// What one settled run carries back to the tool entry.
struct RunRequest<'a> {
    /// The command line as the model wrote it (after any Windows chaining), for the header.
    label: String,
    program: String,
    arguments: Vec<String>,
    cwd: std::path::PathBuf,
    timeout_ms: u64,
    input: Option<&'a str>,
    stdout: Sink,
    stderr: Sink,
    /// `saveOutput !== false`.
    tee: bool,
    lines_chained: u64,
}

/// Spawns one process on a blocking thread and owns it until it settles.
///
/// Every step needs the child, so the deadline, the cancellation poll, the tree kill, the
/// stdin pump, the drain wait and the tee all happen here, in one owner. `started` is stamped
/// by the caller BEFORE the spawn so `durationMs` covers spawn latency, like the reference's
/// single `Date.now()` at the top of `runShell`.
fn run_blocking(
    request: RunRequest<'_>,
    started: &Instant,
    cancelled: &AtomicBool,
) -> Result<Ran, String> {
    let mut builder = std::process::Command::new(&request.program);

    builder
        .args(&request.arguments)
        .current_dir(&request.cwd)
        .stdin(if request.input.is_some() {
            Stdio::piped()
        } else {
            // `stdin: input === undefined ? 'ignore' : 'pipe'`: a command that reads stdin
            // gets EOF at once instead of hanging to the timeout.
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    set_group_leader(&mut builder);

    let mut child = builder
        .spawn()
        .map_err(|error| format!("could not start \"{}\": {error}", request.program))?;

    let process_group = child.id();

    // The stdin pump is a thread, not an `await`: a big write can block, and a child that has
    // already left fails it with EPIPE/EOF, which is never the run's failure
    // (`shell.test.ts:332`).
    let writer = request.input.map(|text| {
        let mut pipe = child.stdin.take().expect("stdin is piped");
        let payload = text.as_bytes().to_vec();

        std::thread::spawn(move || {
            let _ = pipe.write_all(&payload);
            let _ = pipe.flush();
            drop(pipe);
        })
    });

    let stdout_sink = Arc::new(Mutex::new(request.stdout));
    let stderr_sink = Arc::new(Mutex::new(request.stderr));
    let stdout_done = Arc::new(AtomicBool::new(false));
    let stderr_done = Arc::new(AtomicBool::new(false));

    let stdout_thread = drain_thread(child.stdout.take(), &stdout_sink, &stdout_done);
    let stderr_thread = drain_thread(child.stderr.take(), &stderr_sink, &stderr_done);

    let deadline = *started + Duration::from_millis(request.timeout_ms);
    let mut timed_out = false;
    let mut aborted = false;

    loop {
        match child.try_wait() {
            // The child settled on its own: honour the REAL exit code even if the deadline
            // fires in the same instant (`shell.test.ts:346`).
            Ok(Some(_)) => break,
            Ok(None) => {}
            Err(error) => return Err(error.to_string()),
        }

        if cancelled.load(Ordering::SeqCst) {
            terminate_tree(child.id(), process_group);
            aborted = true;
            break;
        }

        let now = Instant::now();

        if now >= deadline {
            terminate_tree(child.id(), process_group);
            timed_out = true;
            break;
        }

        let remaining = deadline.saturating_duration_since(now).as_millis();

        std::thread::sleep(Duration::from_millis(
            u64::from(u32::try_from(remaining).unwrap_or(u32::MAX)).min(CANCEL_POLL_MS),
        ));
    }

    // Belt and braces: the group signal above reaches the descendants, `kill()` the root in
    // case the group id was not signalable.
    if timed_out || aborted {
        let _ = child.kill();
    }

    // Reaping also gives the honest `exitSignal` (`status.signal()` on unix; the Windows
    // kill path reports none).
    let status = child.wait().ok();

    let (exit_code, exit_signal) = match status.as_ref() {
        Some(status) => (
            i64::from(status.code().unwrap_or(-1)),
            exit_signal_of(status),
        ),
        None => (-1, None),
    };

    if let Some(writer) = writer {
        let _ = writer.join();
    }

    wait_for_drains(&stdout_done, &stderr_done, KILL_GRACE_MS);

    drop((stdout_thread, stderr_thread));

    let stdout_report = take_sink(&stdout_sink).finish();
    let stderr_report = take_sink(&stderr_sink).finish();

    // `durationMs` is stamped HERE because the tee header carries it and the published field
    // must name the SAME number (`shell.tool.ts:551` then `:559`).
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    // The tee is for a cut stream only, and only when `saveOutput` did not turn it off
    // (`:554` - `!saveOutput || !result.truncated` skips it).
    let mut tee_path = None;

    if request.tee && (stdout_report.truncated || stderr_report.truncated) {
        let path = output_file_path(&request.cwd);

        if write_full_output(
            &path,
            &request.label,
            exit_code,
            duration_ms,
            &stdout_report,
            &stderr_report,
        ) {
            tee_path = Some(path);
        }
    }

    Ok(Ran {
        exit_code,
        exit_signal,
        stdout: stdout_report,
        stderr: stderr_report,
        timed_out,
        aborted,
        duration_ms,
        lines_chained: request.lines_chained,
        tee_path,
    })
}

/// `resolveTimeout` (`shell.tool.ts:585`): a non-positive or non-finite request is a bug
/// outside the documented range, so it is REFUSED; an oversized one is clamped to the
/// ceiling, because running long is allowed and choosing that ceiling is the harness's.
fn resolve_timeout(arguments: &Value) -> Result<u64, String> {
    const INVALID: &str = "shell timeoutMs must be a positive finite number of milliseconds.";

    let requested = match arguments.get("timeoutMs") {
        Some(Value::Number(value)) => value.as_f64(),
        Some(Value::Null) | None => return Ok(COMMAND_DEFAULT_TIMEOUT_MS as u64),
        // A non-number is refused rather than coerced: the harness does not validate JSON
        // Schema, so a model can send `"60000"`, and reading that as 60 000 silently changes
        // what the run means.
        Some(_) => return Err(INVALID.to_owned()),
    };

    let requested = requested.ok_or_else(|| INVALID.to_owned())?;

    if !requested.is_finite() || requested <= 0.0 {
        return Err(INVALID.to_owned());
    }

    let ceiling = COMMAND_MAX_TIMEOUT_MS;

    // `Math.min(Math.floor(timeoutMs), COMMAND_MAX_TIMEOUT_MS)`: floor, never round, so a
    // fractional request can never buy more wall clock than it asked for.
    Ok(requested.min(ceiling as f64).floor() as u64)
}

/// `resolveInput` (`:603`): a non-string `input` is refused, not stringified - feeding the
/// child `"5"` for a number is not what the caller meant.
fn resolve_input(arguments: &Value) -> Result<Option<String>, String> {
    match arguments.get("input") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => {
            // NUL cannot travel through a shell argument, and the reference rejects it.
            if text.contains('\0') {
                return Err("shell input must be a string when provided.".to_owned());
            }

            Ok(Some(text.clone()))
        }
        Some(_) => Err("shell input must be a string when provided.".to_owned()),
    }
}

/// `saveOutput` (`:617`): only an explicit `false` turns the transcript off, and a
/// non-boolean is refused rather than coerced to true by the shell.
fn resolve_save_output(arguments: &Value) -> Result<bool, String> {
    match arguments.get("saveOutput") {
        None | Some(Value::Null) => Ok(true),
        Some(Value::Bool(save)) => Ok(*save),
        Some(_) => Err("shell saveOutput must be a boolean when provided.".to_owned()),
    }
}

/// `chainWindowsLines` (`shell.tool.ts:168`): `cmd /c` takes ONE command line and discards
/// everything after the first newline, so a multi-line command silently loses every later
/// statement - including `exit 9`. Statement-level breaks become `&&`; breaks inside
/// quotes/parens/brackets/braces are argument continuations and stay, because chaining them
/// would corrupt the argument. The count is published as `linesChained`.
///
/// Only called on Windows; elsewhere the shell already runs every line, so `linesChained`
/// stays 0 there.
fn chain_windows_lines(command: &str) -> (String, u64) {
    let mut out = String::with_capacity(command.len());
    let mut single = false;
    let mut double = false;
    let mut depth = 0_u32;
    let mut joined = 0_u64;
    let mut characters = command.chars().peekable();

    while let Some(character) = characters.next() {
        if character == '\'' && !double {
            single = !single;
        } else if character == '"' && !single {
            // A doubled quote INSIDE a double-quoted region is cmd's escape, not a boundary.
            if double && characters.peek().copied() == Some('"') {
                characters.next();
                out.push('"');
                out.push('"');
                continue;
            }

            double = !double;
        } else if !single && (character == '(' || character == '{' || character == '[') {
            depth += 1;
        } else if !single && (character == ')' || character == '}' || character == ']') {
            depth = depth.saturating_sub(1);
        }

        if character != '\n' || single || double || depth > 0 {
            out.push(character);
            continue;
        }

        // A trailing `\` continues the line; `^` is cmd's escape, and the following newline
        // is part of the command, not a separator.
        let escaped = out.ends_with('\\') || out.ends_with('^');

        if escaped {
            out.push(character);
            continue;
        }

        let rest: String = characters.clone().collect();
        let next = rest.trim_start_matches([' ', '\t']);

        // A blank line and an `&&`/`||`-terminated line already say what comes next;
        // `|`/`&` terminators are the same story.
        if next.is_empty()
            || next.starts_with('\n')
            || next.starts_with("&&")
            || next.starts_with("||")
            || next.starts_with('|')
            || next.starts_with('&')
        {
            out.push(character);
            continue;
        }

        let trimmed = out.trim_end_matches([' ', '\t', '\r']);

        out = trimmed.to_owned();
        out.push_str(" && ");
        joined += 1;

        // Leading whitespace on the next statement is dropped; the join supplies it.
        for skipped in rest.chars().take_while(|c| *c == ' ' || *c == '\t') {
            let _ = skipped;
            characters.next();
        }
    }

    // A trailing newline must not leave a dangling `&&`.
    (out.trim_end().to_owned(), joined)
}

/// The platform shell invocation for one command string, with cmd's quoting rule applied.
fn shell_invocation(command: &str) -> (&'static str, Vec<String>) {
    if cfg!(windows) {
        ("cmd", vec!["/C".to_owned(), escape_for_cmd(command)])
    } else {
        ("sh", vec!["-c".to_owned(), command.to_owned()])
    }
}

/// cmd's argument-escaping quirk (`shell.tool.ts:211`): when the first character is `"`,
/// cmd keeps its own quoting rules; otherwise EVERY internal quote doubles. Getting this
/// wrong makes a quoted newline corrupt the argument instead of surviving it.
fn escape_for_cmd(command: &str) -> String {
    if command.starts_with('"') {
        return command.to_owned();
    }

    command.replace('"', "\"\"")
}

/// `shell`: `{command, cwd?, timeoutMs?, input?, saveOutput?}` -> the settled run
/// (`shell.tool.ts`).
///
/// # Errors
///
/// Only a bad argument or a process that could not be STARTED is a failure; a non-zero exit,
/// a timeout and an abort are all data.
pub(crate) async fn shell(
    arguments: &Value,
    cwd: &std::path::Path,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let command = super::result::required_text(arguments, "command", "shell")?;

    if command.trim().is_empty() {
        return Err("shell requires a non-empty command string.".to_owned());
    }

    let directory = match super::result::optional_text(arguments, "cwd") {
        Some(raw) => super::files::resolve_target(cwd, &raw)?,
        None => cwd.to_path_buf(),
    };

    let timeout_ms = resolve_timeout(arguments)?;
    let input = resolve_input(arguments)?;
    let save_output = resolve_save_output(arguments)?;

    // Windows needs the line chaining; on POSIX the shell already runs every line.
    let (command_line, lines_chained) = if cfg!(windows) {
        chain_windows_lines(&command)
    } else {
        (command.clone(), 0)
    };

    let (program, argv) = shell_invocation(&command_line);

    // A token that was already cancelled never spawns (`shell.test.ts:442`).
    if signal.is_cancelled() {
        return Ok(Ran::never_spawned(lines_chained).publish());
    }

    // The token is an async primitive and the runner is a blocking owner, so a watcher task
    // lowers it to a flag the owner can poll.
    let cancelled = Arc::new(AtomicBool::new(false));
    let watcher = Arc::clone(&cancelled);
    let token = signal.clone();

    tokio::spawn(async move {
        token.cancelled().await;
        watcher.store(true, Ordering::SeqCst);
    });

    let started = Instant::now();
    let flag = Arc::clone(&cancelled);
    // The tee header names the command the CALLER wrote, not the line the shell got
    // (`shell.tool.ts:429-433`): on Windows those differ, and the reader recognises its own
    // request, not the rewrite.
    let label = command.clone();
    let program = program.to_owned();
    let tee = save_output;

    let ran = tokio::task::spawn_blocking(move || {
        run_blocking(
            RunRequest {
                label,
                program,
                arguments: argv,
                cwd: directory,
                timeout_ms,
                input: input.as_deref(),
                stdout: Sink::HeadTail {
                    capture: BoundedOutput::new(COMMAND_STDOUT_MAX_BYTES),
                    raw: BoundedOutput::new(OUTPUT_FILE_MAX_BYTES),
                },
                stderr: Sink::HeadTail {
                    capture: BoundedOutput::new(COMMAND_STDERR_MAX_BYTES),
                    raw: BoundedOutput::new(OUTPUT_FILE_MAX_BYTES),
                },
                tee,
                lines_chained,
            },
            &started,
            &flag,
        )
    })
    .await
    .map_err(|error| format!("the shell run could not be joined: {error}"))??;

    // The watcher is done once the run has settled; dropping it is enough.
    drop(cancelled);

    Ok(ran.publish())
}

/// `gitDiff`: `{ref?, staged?, paths?, cwd?}` -> the captured diff
/// (`gitDiff.tool.ts:54` `GitDiffResult`).
///
/// HEAD-kept on purpose (`BoundedText`, not `BoundedOutput`): a diff is read top-down and
/// its `diff --git`/`index` header lines are the useful part, so the published shape stays
/// `{exitCode, stdout, stderr, truncated, stderrTruncated, timedOut, aborted}`.
pub(crate) async fn git_diff(
    arguments: &Value,
    cwd: &std::path::Path,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let mut argv: Vec<String> = vec!["diff".to_owned()];

    if super::result::flag(arguments, "staged") {
        argv.push("--staged".to_owned());
    }

    if let Some(reference) = super::result::optional_text(arguments, "ref") {
        if reference.starts_with('-') {
            return Err("gitDiff ref must not start with \"-\".".to_owned());
        }

        argv.push(reference);
    }

    let paths = super::result::string_list(arguments, "paths").unwrap_or_default();

    if !paths.is_empty() {
        // The `--` separator is what makes a pathspec never read as a flag.
        argv.push("--".to_owned());
        argv.extend(paths);
    }

    let directory = match super::result::optional_text(arguments, "cwd") {
        Some(raw) => super::files::resolve_target(cwd, &raw)?,
        None => cwd.to_path_buf(),
    };

    if signal.is_cancelled() {
        return Ok(json!({
            "exitCode": -1,
            "stdout": "",
            "stderr": "",
            "truncated": false,
            "stderrTruncated": false,
            "timedOut": false,
            "aborted": true,
        }));
    }

    let cancelled = Arc::new(AtomicBool::new(false));
    let watcher = Arc::clone(&cancelled);
    let token = signal.clone();

    tokio::spawn(async move {
        token.cancelled().await;
        watcher.store(true, Ordering::SeqCst);
    });

    let started = Instant::now();
    let flag = Arc::clone(&cancelled);
    let label = format!("git {}", argv.join(" "));

    let ran = tokio::task::spawn_blocking(move || {
        run_blocking(
            RunRequest {
                label,
                program: "git".to_owned(),
                arguments: argv,
                cwd: directory,
                timeout_ms: COMMAND_DEFAULT_TIMEOUT_MS as u64,
                input: None,
                stdout: Sink::Head {
                    bytes: Vec::new(),
                    total: 0,
                    cap: COMMAND_STDOUT_MAX_BYTES,
                },
                stderr: Sink::Head {
                    bytes: Vec::new(),
                    total: 0,
                    cap: COMMAND_STDERR_MAX_BYTES,
                },
                // `gitDiff` has no tee in the reference contract.
                tee: false,
                lines_chained: 0,
            },
            &started,
            &flag,
        )
    })
    .await
    .map_err(|error| format!("the git run could not be joined: {error}"))??;

    drop(cancelled);

    Ok(json!({
        "exitCode": ran.exit_code,
        "stdout": ran.stdout.text,
        "stderr": ran.stderr.text,
        "truncated": ran.stdout.truncated,
        "stderrTruncated": ran.stderr.truncated,
        "timedOut": ran.timed_out,
        "aborted": ran.aborted,
    }))
}

#[cfg(test)]
#[path = "command_tests.rs"]
mod tests;
