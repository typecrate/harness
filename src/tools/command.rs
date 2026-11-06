//! The command tools: `shell` and `gitDiff`.
//!
//! TypeScript origins
//! ------------------
//!   * `tools/command/shell.tool.ts` - `COMMAND_DEFAULT_TIMEOUT_MS` (120 s), the
//!     `{exitCode, stdout, stderr, truncated, timedOut, aborted}` document, and the
//!     process-tree kill on timeout;
//!   * `tools/command/gitDiff.tool.ts` - `git diff` argument assembly (the `--` separator
//!     so a pathspec can never read as a flag, and the "must not start with `-`" rule on
//!     `ref`), the 128 KiB stdout / 16 KiB stderr caps, and the
//!     `{exitCode, stdout, stderr, truncated, stderrTruncated, timedOut, aborted}` shape;
//!   * `tools/command/boundedText.ts:25-26` - `COMMAND_STDOUT_MAX_BYTES` /
//!     `COMMAND_STDERR_MAX_BYTES`: the HEAD is kept and the TAIL is dropped, and the
//!     `truncated` flag states that it happened.
//!
//! THE SHELL IS THE PLATFORM SHELL (`harness.ts`'s tool context has no sandbox):
//! `cmd /C` on Windows, `sh -c` everywhere else, exactly as the stage contract states.
//! TypeScript reaches for `shell: true`; both forms run the command string through a
//! real shell, so quoting rules are the host shell's, not this tool's.
//!
//! A non-zero exit code is a SUCCESSFUL call: `git diff` exits 1 when there are
//! differences, and the model reads `exitCode` to decide what the output means.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// `COMMAND_DEFAULT_TIMEOUT_MS` (`shell.tool.ts:69`).
pub const DEFAULT_TIMEOUT_MS: i64 = 120_000;
/// `COMMAND_STDOUT_MAX_BYTES` (`boundedText.ts:25`).
pub const STDOUT_MAX_BYTES: usize = 128 * 1024;
/// `COMMAND_STDERR_MAX_BYTES` (`boundedText.ts:26`).
pub const STDERR_MAX_BYTES: usize = 16 * 1024;
/// `shell`'s documented timeout range (`shell.tool.ts` metadata: 1..600000).
pub const TIMEOUT_MIN_MS: i64 = 1;
/// The upper bound of a requested timeout (`shell.tool.ts` metadata).
pub const TIMEOUT_MAX_MS: i64 = 600_000;

/// One captured stream: the kept head plus the flags saying what was dropped.
struct Captured {
    text: String,
    truncated: bool,
}

/// Decodes captured bytes, keeping at most `cap` bytes of the HEAD.
///
/// The byte cap is applied on a character boundary, so a truncated stream never ends in
/// half a UTF-8 sequence the model would read as mojibake.
fn capture(bytes: &[u8], cap: usize) -> Captured {
    if bytes.len() <= cap {
        return Captured {
            text: String::from_utf8_lossy(bytes).into_owned(),
            truncated: false,
        };
    }

    let mut cut = cap;

    // Walk back off a UTF-8 CONTINUATION byte (`0b10xx_xxxx`) until the window ends on a
    // character boundary.
    while cut > 0 && bytes[cut - 1] & 0b1100_0000 == 0b1000_0000 {
        cut -= 1;
    }

    Captured {
        text: String::from_utf8_lossy(&bytes[..cut]).into_owned(),
        truncated: true,
    }
}

/// What one spawned process settled with.
struct Ran {
    exit_code: i64,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    timed_out: bool,
    aborted: bool,
}

/// Drains one pipe into a shared, byte-capped buffer.
///
/// The buffer stops APPENDING past `cap` but the pipe keeps being read, so a chatty
/// command is never left blocked on a full pipe while the deadline is what actually ends
/// the run.
async fn drain<R>(mut pipe: R, sink: &Arc<Mutex<Vec<u8>>>, cap: usize)
where
    R: AsyncReadExt + Unpin,
{
    let mut chunk = vec![0_u8; 8 * 1024];

    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => {
                let mut buffer = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

                if buffer.len() < cap {
                    let room = cap - buffer.len();
                    let take = read.min(room);
                    buffer.extend_from_slice(&chunk[..take]);
                }
            }
        }
    }
}

/// Spawns one process and waits for it under a timeout and a cancellation token.
///
/// The buffers live OUTSIDE the future that owns the child, so dropping that future (the
/// timeout and the abort branches) kills the process through `kill_on_drop` while the
/// bytes it had already written survive to be reported - which is the honest answer a
/// caller wants from a killed `shell` run.
async fn run_command(
    program: &str,
    arguments: &[&str],
    cwd: &std::path::Path,
    timeout: Duration,
    signal: &CancellationToken,
) -> Result<Ran, String> {
    let mut builder = Command::new(program);
    builder
        .args(arguments)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let mut child = builder
        .spawn()
        .map_err(|error| format!("could not start \"{program}\": {error}"))?;

    let stdout_sink = Arc::new(Mutex::new(Vec::<u8>::new()));
    let stderr_sink = Arc::new(Mutex::new(Vec::<u8>::new()));

    let stdout_task = {
        let sink = Arc::clone(&stdout_sink);
        let pipe = child.stdout.take();

        tokio::spawn(async move {
            if let Some(pipe) = pipe {
                drain(pipe, &sink, STDOUT_MAX_BYTES).await;
            }
        })
    };

    let stderr_task = {
        let sink = Arc::clone(&stderr_sink);
        let pipe = child.stderr.take();

        tokio::spawn(async move {
            if let Some(pipe) = pipe {
                drain(pipe, &sink, STDERR_MAX_BYTES).await;
            }
        })
    };

    // This future OWNS the child, so dropping it is what cancels the process.
    let waiting = async move { child.wait().await.map_err(|error| error.to_string()) };
    tokio::pin!(waiting);

    enum Settled {
        Exited(Result<std::process::ExitStatus, String>),
        TimedOut,
        Aborted,
    }

    let settled = tokio::select! {
        biased;
        () = signal.cancelled() => Settled::Aborted,
        () = tokio::time::sleep(timeout) => Settled::TimedOut,
        status = &mut waiting => Settled::Exited(status),
    };

    // A killed child's pipes reach EOF on their own; awaiting the drainers bounds the
    // wait and keeps the partial capture honest.
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        let _ = stdout_task.await;
        let _ = stderr_task.await;
    })
    .await;

    let stdout = take_buffer(&stdout_sink);
    let stderr = take_buffer(&stderr_sink);

    match settled {
        Settled::Exited(Ok(status)) => Ok(Ran {
            exit_code: i64::from(status.code().unwrap_or(-1)),
            stdout,
            stderr,
            timed_out: false,
            aborted: false,
        }),
        Settled::Exited(Err(error)) => Err(error),
        Settled::TimedOut => Ok(Ran {
            exit_code: -1,
            stdout,
            stderr,
            timed_out: true,
            aborted: false,
        }),
        Settled::Aborted => Ok(Ran {
            exit_code: -1,
            stdout,
            stderr,
            timed_out: false,
            aborted: true,
        }),
    }
}

/// Moves a captured buffer out of its shared lock.
fn take_buffer(sink: &Arc<Mutex<Vec<u8>>>) -> Vec<u8> {
    let mut guard = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    std::mem::take(&mut guard)
}

/// The platform shell invocation for one command string.
fn shell_invocation(command: &str) -> (&'static str, Vec<&str>) {
    if cfg!(windows) {
        ("cmd", vec!["/C", command])
    } else {
        ("sh", vec!["-c", command])
    }
}

/// `shell`: `{command, cwd?, timeoutMs?}` -> the captured run (`shell.tool.ts`).
///
/// # Errors
///
/// Only a process that could not be STARTED is a failure; a non-zero exit is data.
pub(crate) async fn shell(
    arguments: &Value,
    cwd: &std::path::Path,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let command = super::result::required_text(arguments, "command", "shell")?;

    if command.trim().is_empty() {
        return Err("shell command must not be empty.".to_owned());
    }

    let directory = match super::result::optional_text(arguments, "cwd") {
        Some(raw) => super::files::resolve_target(cwd, &raw)?,
        None => cwd.to_path_buf(),
    };

    let timeout_ms = super::result::bounded_integer(
        arguments,
        "timeoutMs",
        DEFAULT_TIMEOUT_MS,
        TIMEOUT_MIN_MS,
        TIMEOUT_MAX_MS,
    )?;
    let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));

    let (program, arguments) = shell_invocation(&command);
    let ran = run_command(program, &arguments, &directory, timeout, signal).await?;

    let stdout = capture(&ran.stdout, STDOUT_MAX_BYTES);
    let stderr = capture(&ran.stderr, STDERR_MAX_BYTES);

    Ok(json!({
        "exitCode": ran.exit_code,
        "stdout": stdout.text,
        "stderr": stderr.text,
        "truncated": stdout.truncated || stderr.truncated,
        "timedOut": ran.timed_out,
        "aborted": ran.aborted,
    }))
}

/// `gitDiff`: `{ref?, staged?, paths?, cwd?}` -> the captured diff
/// (`gitDiff.tool.ts:54` `GitDiffResult`).
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

    let borrowed: Vec<&str> = argv.iter().map(String::as_str).collect();
    let ran = run_command(
        "git",
        &borrowed,
        &directory,
        Duration::from_millis(u64::try_from(DEFAULT_TIMEOUT_MS).unwrap_or(0)),
        signal,
    )
    .await?;

    let stdout = capture(&ran.stdout, STDOUT_MAX_BYTES);
    let stderr = capture(&ran.stderr, STDERR_MAX_BYTES);

    Ok(json!({
        "exitCode": ran.exit_code,
        "stdout": stdout.text,
        "stderr": stderr.text,
        "truncated": stdout.truncated,
        "stderrTruncated": stderr.truncated,
        "timedOut": ran.timed_out,
        "aborted": ran.aborted,
    }))
}
