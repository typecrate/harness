//! Shell transport fidelity tests — the Rust mirror of the reference contract in
//! `packages/underload/harness/src/tools/command/shell.test.ts` and
//! `boundedText.test.ts`.
//!
//! What is pinned, mapped to the reference test that pins it in TS:
//!   - the capture keeps the HEAD and the TAIL with the exact in-band marker, and reports
//!     per-stream `truncated`/`bytes`/`droppedBytes` (`boundedText.test.ts:41-95`);
//!   - the published JSON shape for an ordinary run, a cut run, a timed-out run and a
//!     cancelled run (`shell.test.ts:110-131`, `:362-386`, `:421-461`);
//!   - the tee transcript, byte for byte (`shell.test.ts:254-271`);
//!   - `stdin` closed unless `input` is given (`shell.test.ts:300-330`);
//!   - a completed run is never reported as cancelled (`shell.test.ts:346`);
//!   - the refusal sentences (`shell.test.ts:477-505`).
//!
//! Every command is a real child process (`node -e …` / POSIX `cat`, `sleep`, `touch`),
//! exactly like the reference tests, so the assertions describe real stream behaviour.
//! Byte counts are derived from the emitted text, never probed. `durationMs` is the only
//! field no assertion can pin to an exact number, so it is range-checked the way the
//! reference does (`shell.test.ts:95-99`).

use super::{
    BoundedOutput, COMMAND_DEFAULT_TIMEOUT_MS, COMMAND_MAX_TIMEOUT_MS, COMMAND_STDERR_MAX_BYTES,
    COMMAND_STDOUT_MAX_BYTES, DEFAULT_HEAD_SHARE, base36, chain_windows_lines, escape_for_cmd,
    flatten_command, full_output_header, omission_marker, resolve_input, resolve_save_output,
    resolve_timeout, shell, shell_invocation,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;

/// The published caps, so an assertion names the number rather than the constant.
const OUT_CAP: usize = COMMAND_STDOUT_MAX_BYTES;
const ERR_CAP: usize = COMMAND_STDERR_MAX_BYTES;

/// A process-wide scratch directory for the tee and tree-kill probes. The reference tests
/// use the repo's `Temp/`; a Rust test's runtime artifacts live in the OS temp dir, keyed by
/// pid so parallel runs cannot collide.
fn scratch_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();

    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("noobshell-{}", std::process::id()));

        std::fs::create_dir_all(&dir).expect("the scratch directory is creatable");
        dir
    })
    .as_path()
}

/// Runs `shell` through the real entry point with an optional cwd.
async fn shell_run(arguments: Value, cwd: Option<&Path>) -> Value {
    let directory = match cwd {
        Some(path) => path.to_path_buf(),
        None => std::env::current_dir().expect("a current directory"),
    };

    shell(&arguments, &directory, &CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("the call must succeed: {error}"))
}

/// `settled` in the reference tests (`shell.test.ts:88-94`): the published shape minus the
/// one un-pinnable field, so `==` over the whole object catches a field appearing silently.
fn settled(mut outcome: Value) -> Value {
    outcome
        .as_object_mut()
        .expect("the result is an object")
        .remove("durationMs");
    outcome
}

/// The exact shape of a run that wrote `stdout` and exited 0 (`shell.test.ts:110-131`).
fn settled_exit_zero(stdout: &str, stderr: &str) -> Value {
    json!({
        "exitCode": 0,
        "stdout": stdout,
        "stderr": stderr,
        "truncated": false,
        "stdoutTruncated": false,
        "stderrTruncated": false,
        "stdoutBytes": stdout.len(),
        "stderrBytes": stderr.len(),
        "stdoutDroppedBytes": 0,
        "stderrDroppedBytes": 0,
        "outputPath": null,
        "timedOut": false,
        "aborted": false,
        "exitSignal": null,
        "linesChained": 0,
    })
}

/// What the platform reports for a SIGKILLed child: the signal name on POSIX, nothing on
/// the Windows `taskkill` path (`shell.test.ts:85-86`).
fn expected_kill_signal() -> Option<&'static str> {
    if cfg!(windows) { None } else { Some("SIGKILL") }
}

/// The head+tail text the capture must publish for a stream of `total` copies of `byte`.
/// The marker is spliced BETWEEN the windows and is not carved out of either one
/// (`boundedOutput.ts:190`), so the head is a quarter of the cap and the tail is the rest.
fn expected_windows(total: usize, cap: usize, byte: char) -> String {
    let head = byte.to_string().repeat(cap / DEFAULT_HEAD_SHARE);
    let tail = byte.to_string().repeat(cap - cap / DEFAULT_HEAD_SHARE);

    format!("{}\n{}\n{}", head, omission_marker(total - cap), tail)
}

// ── the capture itself (`boundedText.test.ts:41-95`) ─────────────────────────

#[test]
fn the_marker_is_the_documented_ascii_text() {
    assert_eq!(
        omission_marker(1_842_331),
        "[... 1842331 bytes omitted ...]"
    );
    assert_eq!(omission_marker(1), "[... 1 byte omitted ...]");
    assert_eq!(omission_marker(0), "[... 0 bytes omitted ...]");
}

#[test]
fn a_stream_under_the_cap_is_returned_whole() {
    let mut capture = BoundedOutput::new(OUT_CAP);

    capture.write(b"hello ");
    capture.write(b"world");

    assert_eq!(capture.end(), "hello world");
    assert_eq!(capture.bytes_seen(), 11);
    assert!(!capture.was_truncated());
    assert_eq!(capture.dropped_bytes(), 0);
    assert!(!capture.end().contains("omitted"));
}

#[test]
fn the_default_cap_splits_one_quarter_head_and_three_quarters_tail() {
    // `boundedOutput.test.ts:88-97`, at the shell's real cap: 128 KiB with a 200 000-byte
    // stream is 32 768 head bytes, 98 304 tail bytes and a marker naming 68 928.
    let mut capture = BoundedOutput::new(OUT_CAP);

    capture.write("a".repeat(200_000).as_bytes());

    let text = capture.end();
    let lines = text.split('\n').collect::<Vec<&str>>();

    assert_eq!(lines.len(), 3, "head, marker, tail");
    assert_eq!(lines[0].len(), OUT_CAP / DEFAULT_HEAD_SHARE);
    assert_eq!(lines[0], "a".repeat(OUT_CAP / DEFAULT_HEAD_SHARE));
    assert_eq!(lines[1], omission_marker(200_000 - OUT_CAP));
    assert_eq!(lines[2].len(), OUT_CAP - OUT_CAP / DEFAULT_HEAD_SHARE);
    assert_eq!(capture.dropped_bytes(), 200_000 - OUT_CAP);

    // The small-scale version of the same assertion (`boundedOutput.test.ts:88`).
    let mut small = BoundedOutput::new(16);

    small.write("x".repeat(100).as_bytes());

    let small_text = small.end();
    let parts = small_text.split('\n').collect::<Vec<&str>>();

    assert_eq!(parts[0], "x".repeat(4));
    assert_eq!(parts[1], omission_marker(84));
    assert_eq!(parts[2], "x".repeat(12));
}

#[test]
fn a_zero_head_window_starts_the_text_with_the_marker() {
    // `boundedOutput.test.ts:152-158`.
    let mut capture = BoundedOutput::with_head(4, 0);

    capture.write(b"abcdefg");

    assert_eq!(capture.end(), format!("{}\ndefg", omission_marker(3)));
}

#[test]
fn end_is_repeatable_and_side_effect_free() {
    // `boundedOutput.test.ts:160-171`: the accounting getters read the same numbers before
    // and after `end()`.
    let mut capture = BoundedOutput::with_head(8, 2);

    capture.write(b"0123456789AB");

    let first = capture.end();
    let lines = first.split('\n').collect::<Vec<&str>>();

    assert_eq!(capture.end(), first);
    assert_eq!(capture.dropped_bytes(), 4);
    assert_eq!(
        capture.kept_bytes(),
        lines[0].len() + lines[lines.len() - 1].len()
    );
}

#[test]
fn the_tail_is_never_starved_whatever_head_is_asked_for() {
    // `boundedOutput.test.ts:180-189`: a head window at or past the cap would leave no room
    // for the end, which is the half this class exists to keep.
    let mut capture = BoundedOutput::with_head(8, 99);

    capture.write(b"abcdefghij");

    let text = capture.end();

    assert!(text.ends_with('j'), "{text}");
    assert!(text.starts_with("abcdefg"), "{text}");
}

#[test]
fn an_empty_chunk_changes_nothing() {
    // `boundedOutput.test.ts:143-150`.
    let mut capture = BoundedOutput::new(4);

    capture.write(b"");
    capture.write(b"abc");

    assert_eq!(capture.end(), "abc");
    assert!(!capture.was_truncated());
    assert_eq!(capture.bytes_seen(), 3);
}

#[test]
fn a_one_byte_cap_keeps_the_last_byte_behind_the_marker() {
    // The degenerate split: `headKeep = min(cap/4, cap-1) = 0`, so the tail keeps the last
    // byte and the marker starts the text (`boundedOutput.ts:121`, `:186`).
    let mut capture = BoundedOutput::new(1);

    capture.write(b"hello");

    assert_eq!(capture.end(), format!("{}\no", omission_marker(4)));
    assert!(capture.was_truncated());
    assert_eq!(capture.bytes_seen(), 5);
    assert_eq!(capture.dropped_bytes(), 4);
}

#[test]
fn a_codepoint_split_by_either_cut_is_trimmed_rather_than_emitted_as_garbage() {
    // `boundedOutput.test.ts:105-128`: cap 6, head 3 ends mid-sequence; cap 6, head 0 starts
    // the tail on continuation bytes. Neither may produce U+FFFD.
    let mut head_cut = BoundedOutput::with_head(6, 3);

    head_cut.write(format!("ab{}", "\u{20AC}".repeat(3)).as_bytes());

    let head_text = head_cut.end();

    assert!(!head_text.contains('\u{FFFD}'), "{head_text}");
    assert!(head_text.starts_with("ab"), "{head_text}");

    let mut tail_cut = BoundedOutput::with_head(6, 0);

    tail_cut.write("\u{20AC}\u{20AC}\u{20AC}\u{20AC}".as_bytes());

    let tail_text = tail_cut.end();

    assert!(!tail_text.contains('\u{FFFD}'), "{tail_text}");
    assert!(tail_text.ends_with("\u{20AC}\u{20AC}"), "{tail_text}");
    assert_eq!(tail_cut.bytes_seen(), 12);
    assert_eq!(tail_cut.dropped_bytes(), 6);
}

#[test]
fn a_split_codepoint_never_becomes_a_replacement_character() {
    // `boundedOutput.test.ts:130-141`: U+20AC = E2 82 AC split across chunks. Nothing is
    // decoded until `end()`, which is what keeps this lossless.
    let mut capture = BoundedOutput::new(16);

    capture.write(&[0xE2, 0x82]);
    capture.write(&[0xAC]);

    assert_eq!(capture.end(), "\u{20AC}");
    assert!(!capture.end().contains('\u{FFFD}'));
    assert_eq!(capture.bytes_seen(), 3);
    assert!(!capture.was_truncated());
}

#[test]
fn both_windows_are_kept_and_the_middle_is_named() {
    let mut capture = BoundedOutput::new(OUT_CAP);

    capture.write("a".repeat(200_000).as_bytes());

    assert_eq!(capture.end(), expected_windows(200_000, OUT_CAP, 'a'));
    assert_eq!(capture.bytes_seen(), 200_000);
    assert!(capture.was_truncated());
    assert_eq!(capture.dropped_bytes(), 200_000 - OUT_CAP);
}

#[test]
fn only_the_stream_that_overflowed_is_flagged() {
    // The cross-stream bug the per-stream split fixes (`boundedText.test.ts:133-148`).
    let mut out = BoundedOutput::new(OUT_CAP);

    out.write("a".repeat(OUT_CAP).as_bytes());

    assert!(!out.was_truncated());
    assert_eq!(out.dropped_bytes(), 0);
    assert_eq!(out.bytes_seen(), OUT_CAP);

    let mut err = BoundedOutput::new(ERR_CAP);

    err.write("E".repeat(17_000).as_bytes());

    assert!(err.was_truncated());
    assert_eq!(err.dropped_bytes(), 17_000 - ERR_CAP);
}

#[test]
fn the_full_transcript_header_names_the_command_and_the_cut_streams() {
    // The reference's two header lines, byte for byte (`shell.tool.ts:404-405`): the
    // command, then exit code + duration + each stream's TRUE byte size.
    assert_eq!(
        full_output_header("echo hi", 7, 12, 400_000, 0),
        "# shell: echo hi\n# exitCode=7 durationMs=12 stdout=400000B stderr=0B"
    );
    assert_eq!(
        full_output_header("echo hi", 0, 0, 1, 2),
        "# shell: echo hi\n# exitCode=0 durationMs=0 stdout=1B stderr=2B"
    );
    // `command.replace(/[\r\n]+/g, ' ')`: a RUN of newlines collapses to ONE space, so the
    // header stays one line for a multi-line command.
    assert_eq!(
        full_output_header("echo a\n\n\r\nexit 9", 0, 0, 0, 0),
        "# shell: echo a exit 9\n# exitCode=0 durationMs=0 stdout=0B stderr=0B"
    );
    assert_eq!(flatten_command("a\r\nb\nc"), "a b c");
    assert_eq!(flatten_command("single line"), "single line");
}

// ── the run contract's validation, through the same helpers the entry uses ───

#[test]
fn the_timeout_is_defaulted_clamped_and_refused_exactly_as_documented() {
    assert_eq!(resolve_timeout(&json!({})).unwrap(), 120_000);
    assert_eq!(
        resolve_timeout(&json!({ "timeoutMs": null })).unwrap(),
        120_000
    );
    assert_eq!(
        resolve_timeout(&json!({ "timeoutMs": 5_000 })).unwrap(),
        5_000
    );
    // Over the ceiling: clamped, not refused - running long is allowed, the ceiling is the
    // harness's (`shell.tool.ts:154`).
    assert_eq!(
        resolve_timeout(&json!({ "timeoutMs": 900_000 })).unwrap(),
        600_000
    );

    // `Math.floor(timeoutMs)`: a fractional request is FLOORED, never rounded up, so it can
    // never buy more wall clock than it asked for. A positive sub-millisecond value therefore
    // becomes 0, which is what the reference hands to its timer.
    assert_eq!(
        resolve_timeout(&json!({ "timeoutMs": 1_500.7 })).unwrap(),
        1_500
    );
    assert_eq!(resolve_timeout(&json!({ "timeoutMs": 0.4 })).unwrap(), 0);

    const INVALID: &str = "shell timeoutMs must be a positive finite number of milliseconds.";

    for bad in [
        json!({ "timeoutMs": 0 }),
        json!({ "timeoutMs": -1 }),
        json!({ "timeoutMs": "60000" }),
        json!({ "timeoutMs": true }),
    ] {
        assert_eq!(
            resolve_timeout(&bad).unwrap_err(),
            INVALID,
            "{bad} must be refused"
        );
    }

    // The bounded ceiling the reference exposes (`shell.test.ts:507-511`).
    assert_eq!(COMMAND_DEFAULT_TIMEOUT_MS, 120_000);
    assert_eq!(COMMAND_MAX_TIMEOUT_MS, 600_000);
}

#[test]
fn input_and_save_output_are_refused_instead_of_coerced() {
    // `shell.tool.ts:603`/`:617`: a model can send 5 for a string, and silently feeding the
    // child "5" is worse than refusing.
    assert_eq!(
        resolve_input(&json!({ "input": 5 })).unwrap_err(),
        "shell input must be a string when provided."
    );
    assert_eq!(
        resolve_input(&json!({ "input": "\u{0}" })).unwrap_err(),
        "shell input must be a string when provided."
    );
    assert_eq!(resolve_input(&json!({})).unwrap(), None);
    assert_eq!(
        resolve_input(&json!({ "input": "fed-in" })).unwrap(),
        Some("fed-in".to_owned())
    );

    assert_eq!(
        resolve_save_output(&json!({ "saveOutput": "no" })).unwrap_err(),
        "shell saveOutput must be a boolean when provided."
    );
    assert!(resolve_save_output(&json!({})).unwrap());
    assert!(!resolve_save_output(&json!({ "saveOutput": false })).unwrap());
}

#[test]
fn lines_are_chained_only_when_a_break_is_a_statement_separator() {
    // `chainWindowsLines` (`shell.tool.ts:168`). The rewrite is pure logic, so it is asserted
    // independently of the platform that would run it.
    assert_eq!(chain_windows_lines("echo hi"), ("echo hi".to_owned(), 0));

    let (joined, count) = chain_windows_lines("echo first\nexit 9");

    assert_eq!(count, 1);
    assert_eq!(joined, "echo first && exit 9");

    let (three, three_count) = chain_windows_lines("echo 1\necho 2\necho 3");

    assert_eq!(three_count, 2);
    assert_eq!(three, "echo 1 && echo 2 && echo 3");

    // A break inside `"…"` is part of an argument, not a statement separator.
    let (quoted, quoted_count) = chain_windows_lines("node -e \"a\nb\"");

    assert_eq!(quoted_count, 0);
    assert_eq!(quoted, "node -e \"a\nb\"");

    // A trailing break must not leave a dangling `&&`.
    let (trailing, trailing_count) = chain_windows_lines("echo a\n");

    assert_eq!(trailing_count, 0);
    assert_eq!(trailing, "echo a");

    // cmd.exe escaping: an internal quote doubles, a leading one keeps cmd's own rules.
    assert_eq!(escape_for_cmd("/c"), "/c");
    assert_eq!(escape_for_cmd("echo \"a b\""), "echo \"\"a b\"\"");
    assert_eq!(escape_for_cmd("\"echo a\""), "\"echo a\"");

    // The POSIX branch never escapes: `sh -c` takes the string as it stands.
    if !cfg!(windows) {
        assert_eq!(
            shell_invocation("echo \"a b\""),
            ("sh", vec!["-c".to_owned(), "echo \"a b\"".to_owned()])
        );
    }
}

// ── the published shape ──────────────────────────────────────────────────────

#[tokio::test]
async fn a_small_command_reports_the_exact_documented_shape() {
    let outcome = shell_run(
        json!({ "command": r##"node -e "process.stdout.write('hello')""## }),
        None,
    )
    .await;

    assert_eq!(settled(outcome.clone()), settled_exit_zero("hello", ""));

    // The one field no assertion can pin exactly, exactly as the reference allows.
    let duration = outcome["durationMs"]
        .as_u64()
        .expect("durationMs is a number of milliseconds");

    assert!(duration < 600_000, "durationMs looks wrong: {duration}");
}

#[tokio::test]
async fn stderr_is_separate_and_a_non_zero_exit_is_data() {
    // `shell.test.ts:133-152`: a non-zero exit never throws, and `git diff`'s exit 1 style
    // results are the same story.
    let outcome = shell_run(
        json!({ "command": r##"node -e "process.stderr.write('boom');process.exit(3)""## }),
        None,
    )
    .await;

    assert_eq!(
        settled(outcome),
        json!({
            "exitCode": 3,
            "stdout": "",
            "stderr": "boom",
            "truncated": false,
            "stdoutTruncated": false,
            "stderrTruncated": false,
            "stdoutBytes": 0,
            "stderrBytes": 4,
            "stdoutDroppedBytes": 0,
            "stderrDroppedBytes": 0,
            "outputPath": null,
            "timedOut": false,
            "aborted": false,
            "exitSignal": null,
            "linesChained": 0,
        })
    );
}

#[tokio::test]
async fn a_cut_stdout_keeps_the_head_and_the_tail_and_names_the_drop() {
    // `shell.test.ts:178-206`: 200 000 ASCII bytes against the 128 KiB cap.
    let outcome = shell_run(
        json!({
            "command": r##"node -e "process.stdout.write('a'.repeat(200000))""##,
            "saveOutput": false,
        }),
        None,
    )
    .await;

    assert_eq!(settled(outcome), {
        let mut shape = settled_exit_zero("", "");

        shape["stdout"] = json!(expected_windows(200_000, OUT_CAP, 'a'));
        shape["truncated"] = json!(true);
        shape["stdoutTruncated"] = json!(true);
        shape["stdoutBytes"] = json!(200_000);
        shape["stdoutDroppedBytes"] = json!(200_000 - OUT_CAP);

        shape
    });
}

#[tokio::test]
async fn the_last_lines_survive_and_a_middle_line_does_not() {
    // `shell.test.ts:208-224`: the behaviour the head+tail policy exists for. 20 000
    // numbered lines: line 19 999 must be in the result and line 6 000 must be gone.
    let outcome = shell_run(
        json!({
            "command": r##"node -e "for(let i=0;i<20000;i++)console.log('line '+i)""##,
            "saveOutput": false,
        }),
        None,
    )
    .await;

    let stdout = outcome["stdout"].as_str().expect("stdout is a string");

    assert!(stdout.starts_with("line 0\n"), "head window missing");
    assert!(
        stdout.trim_end().ends_with("line 19999"),
        "tail window missing"
    );
    assert!(!stdout.contains("line 6000\n"), "the middle survived");

    // Every emitted byte is counted: 20 000 `line i\n` records = 20 000 x 6 bytes plus the
    // digit count of 0..19999 (10x1 + 90x2 + 900x3 + 9 000x4 + 10 000x5 = 88 890).
    assert_eq!(outcome["stdoutBytes"], json!(208_890));
    assert_eq!(outcome["stdoutDroppedBytes"], json!(208_890 - OUT_CAP));
    assert_eq!(outcome["stdoutTruncated"], json!(true));
    assert_eq!(outcome["stderrBytes"], json!(0));
    assert_eq!(outcome["stderrTruncated"], json!(false));
}

#[tokio::test]
async fn the_tee_holds_every_line_the_published_text_had_to_drop() {
    // `shell.test.ts:254-271`: the transcript is byte-for-byte the whole stream, so
    // `grep`/`read` can reach the middle without running the command again.
    let directory = scratch_dir().join("tee-cwd");

    std::fs::create_dir_all(&directory).expect("the tee cwd is creatable");

    let outcome = shell_run(
        json!({ "command": r##"node -e "for(let i=0;i<20000;i++)console.log('line '+i)""## }),
        Some(&directory),
    )
    .await;

    let path = PathBuf::from(
        outcome["outputPath"]
            .as_str()
            .expect("a cut stream reports outputPath"),
    );

    // The documented location and name shape: <cwd>/.underload/shell/<ms>-<pid>-<seq>.txt.
    assert_eq!(
        path.parent(),
        Some(directory.join(".underload").join("shell").as_path())
    );

    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("a file name");
    let segments: Vec<&str> = file_name.trim_end_matches(".txt").split('-').collect();

    assert_eq!(segments.len(), 3, "unexpected transcript name: {file_name}");
    assert_eq!(
        segments[1],
        base36(u64::from(std::process::id())),
        "the name carries the harness pid in base 36: {file_name}"
    );

    let saved = std::fs::read_to_string(&path).expect("the tee is readable");
    let body: String = (0..20_000)
        .map(|index| format!("line {index}\n"))
        .collect::<Vec<String>>()
        .join("");

    // The stream is 208 890 bytes: over the 128 KiB model-facing cap, far under the tee's own
    // 8 MiB cap. So the FILE holds the whole stream (no in-band marker) while the RESULT only
    // carries head + marker + tail, which is the entire point of the 64x mirror.
    assert_eq!(body.len(), 208_890);
    assert_eq!(outcome["stdoutBytes"], json!(208_890));
    assert_eq!(outcome["stderrBytes"], json!(0));

    // Body shape (`shell.tool.ts:401-409`): two header lines, the stdout mirror, a labelled
    // stderr section, the stderr mirror, joined by newlines. The header's `durationMs` and
    // byte counts are the SAME numbers the result publishes, so a reader of the file and a
    // reader of the result cannot disagree.
    //
    // The join is what it is: `node`'s last line already ends with a newline AND the join
    // inserts one between the stdout section and the stderr label, so the file carries a
    // blank line there. An empty stderr contributes nothing after the label's newline.
    let expected = format!(
        "# shell: {}\n# exitCode=0 durationMs={} stdout={}B stderr=0B\n{}\n# --- stderr ({} bytes) ---\n",
        r##"node -e "for(let i=0;i<20000;i++)console.log('line '+i)""##,
        outcome["durationMs"]
            .as_u64()
            .expect("a published duration"),
        outcome["stdoutBytes"]
            .as_u64()
            .expect("a published stdout size"),
        body,
        outcome["stderrBytes"]
            .as_u64()
            .expect("a published stderr size"),
    );

    assert_eq!(saved, expected);

    let _ = std::fs::remove_dir_all(&directory);
}

#[tokio::test]
async fn no_transcript_is_written_when_nothing_was_cut_or_saving_is_off() {
    // `shell.test.ts:273-287`.
    let directory = scratch_dir().join("no-tee-cwd");

    std::fs::create_dir_all(&directory).expect("the tee cwd is creatable");

    let small = shell_run(
        json!({ "command": r##"node -e "process.stdout.write('hi')""## }),
        Some(&directory),
    )
    .await;

    assert_eq!(small["outputPath"], json!(null));

    let cut = shell_run(
        json!({
            "command": r##"node -e "process.stdout.write('a'.repeat(200000))""##,
            "saveOutput": false,
        }),
        Some(&directory),
    )
    .await;

    assert_eq!(cut["truncated"], json!(true));
    assert_eq!(cut["outputPath"], json!(null));
    assert!(
        !directory.join(".underload").exists(),
        "saveOutput: false must not create the folder"
    );

    let _ = std::fs::remove_dir_all(&directory);
}

#[tokio::test]
async fn stdin_is_closed_when_no_input_is_given() {
    // `shell.test.ts:300-312`: `cat` on an open-but-empty stdin hangs to the timeout; on a
    // closed one it gets EOF, prints nothing and exits 0.
    let outcome = shell_run(json!({ "command": "cat", "timeoutMs": 4_000 }), None).await;

    assert_eq!(settled(outcome), settled_exit_zero("", ""));
}

#[tokio::test]
async fn input_is_fed_to_stdin() {
    // `shell.test.ts:314-322`.
    let outcome = shell_run(
        json!({ "command": "cat", "input": "fed-in", "timeoutMs": 4_000 }),
        None,
    )
    .await;

    assert_eq!(settled(outcome), settled_exit_zero("fed-in", ""));
}

#[tokio::test]
async fn a_child_that_exits_early_survives_a_big_stdin_write() {
    // `shell.test.ts:332-344`: the write fails with EPIPE/EOF once the child is gone, which
    // is never the run's failure.
    let outcome = shell_run(
        json!({
            "command": "exit 9",
            "input": "x",
            "timeoutMs": 4_000,
        }),
        None,
    )
    .await;

    assert_eq!(outcome["exitCode"], json!(9));
    assert_eq!(outcome["timedOut"], json!(false));
    assert_eq!(outcome["aborted"], json!(false));
}

#[tokio::test]
async fn a_run_that_finished_is_never_reported_as_cancelled() {
    // `shell.test.ts:346-360`: the direction that must never happen.
    let outcome = shell_run(
        json!({
            "command": r##"node -e "console.log('RACE');process.exit(5)""##,
            "timeoutMs": 60_000,
        }),
        None,
    )
    .await;

    assert_eq!(
        settled(outcome),
        json!({
            "exitCode": 5,
            "stdout": "RACE\n",
            "stderr": "",
            "truncated": false,
            "stdoutTruncated": false,
            "stderrTruncated": false,
            "stdoutBytes": 5,
            "stderrBytes": 0,
            "stdoutDroppedBytes": 0,
            "stderrDroppedBytes": 0,
            "outputPath": null,
            "timedOut": false,
            "aborted": false,
            "exitSignal": null,
            "linesChained": 0,
        })
    );
}

#[tokio::test]
async fn a_timeout_kills_the_run_and_reports_the_documented_shape() {
    // `shell.test.ts:362-386`.
    let outcome = shell_run(
        json!({
            "command": r##"node -e "setTimeout(() => {}, 5000)""##,
            "timeoutMs": 150,
        }),
        None,
    )
    .await;

    assert_eq!(
        settled(outcome.clone()),
        json!({
            "exitCode": -1,
            "stdout": "",
            "stderr": "",
            "truncated": false,
            "stdoutTruncated": false,
            "stderrTruncated": false,
            "stdoutBytes": 0,
            "stderrBytes": 0,
            "stdoutDroppedBytes": 0,
            "stderrDroppedBytes": 0,
            "outputPath": null,
            "timedOut": true,
            "aborted": false,
            "exitSignal": expected_kill_signal(),
            "linesChained": 0,
        })
    );

    assert!(outcome["durationMs"].as_u64().expect("durationMs") < 5_000);
}

#[tokio::test]
async fn a_timeout_kills_the_whole_process_tree() {
    // `shell.test.ts:388-402`, the tree-kill regression: a backgrounded descendant writes a
    // marker one second later. Killing only the direct child leaves it alive, so the marker
    // would appear.
    let marker = scratch_dir().join("tree-marker.txt");

    let _ = std::fs::remove_file(&marker);

    let command = format!(
        "(sleep 1; touch '{}') &\nsleep 30",
        marker.to_str().expect("a UTF-8 scratch path")
    );

    let outcome = shell_run(json!({ "command": command, "timeoutMs": 150 }), None).await;

    assert_eq!(outcome["timedOut"], json!(true));
    assert_eq!(outcome["exitCode"], json!(-1));

    // Past the moment the descendant would have written.
    tokio::time::sleep(std::time::Duration::from_millis(1_400)).await;

    assert!(!marker.exists(), "the descendant outlived the run");
}

#[tokio::test]
async fn a_cancelled_run_reports_aborted() {
    // `shell.test.ts:421-440`.
    let token = CancellationToken::new();
    let canceller = token.clone();

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        canceller.cancel();
    });

    let directory = std::env::current_dir().expect("a current directory");
    let outcome = shell(
        &json!({ "command": r##"node -e "setTimeout(() => {}, 5000)""##, "saveOutput": false }),
        &directory,
        &token,
    )
    .await
    .expect("a cancelled run is data, not an error");

    assert_eq!(
        settled(outcome),
        json!({
            "exitCode": -1,
            "stdout": "",
            "stderr": "",
            "truncated": false,
            "stdoutTruncated": false,
            "stderrTruncated": false,
            "stdoutBytes": 0,
            "stderrBytes": 0,
            "stdoutDroppedBytes": 0,
            "stderrDroppedBytes": 0,
            "outputPath": null,
            "timedOut": false,
            "aborted": true,
            "exitSignal": expected_kill_signal(),
            "linesChained": 0,
        })
    );
}

#[tokio::test]
async fn a_pre_cancelled_token_never_spawns() {
    // `shell.test.ts:442-461`: exitCode -1, `aborted`, nothing captured, no signal.
    let token = CancellationToken::new();

    token.cancel();

    let directory = std::env::current_dir().expect("a current directory");
    let outcome = shell(
        &json!({ "command": r##"node -e "process.stdout.write('never')""## }),
        &directory,
        &token,
    )
    .await
    .expect("a pre-cancelled token returns before spawning");

    assert_eq!(
        settled(outcome),
        json!({
            "exitCode": -1,
            "stdout": "",
            "stderr": "",
            "truncated": false,
            "stdoutTruncated": false,
            "stderrTruncated": false,
            "stdoutBytes": 0,
            "stderrBytes": 0,
            "stdoutDroppedBytes": 0,
            "stderrDroppedBytes": 0,
            "outputPath": null,
            "timedOut": false,
            "aborted": true,
            "exitSignal": null,
            "linesChained": 0,
        })
    );
}

#[tokio::test]
async fn the_validation_refusals_are_the_exact_sentences() {
    // `shell.test.ts:477-505`, verbatim, through the entry point the registry calls.
    let missing = shell(
        &json!({ "command": "   " }),
        &std::env::current_dir().expect("a current directory"),
        &CancellationToken::new(),
    )
    .await
    .expect_err("a blank command is refused");

    assert_eq!(missing, "shell requires a non-empty command string.");

    let timed = shell(
        &json!({ "command": "true", "timeoutMs": 0 }),
        &std::env::current_dir().expect("a current directory"),
        &CancellationToken::new(),
    )
    .await
    .expect_err("timeoutMs 0 is refused");

    assert_eq!(
        timed,
        "shell timeoutMs must be a positive finite number of milliseconds."
    );

    let fed = shell(
        &json!({ "command": "true", "input": 5 }),
        &std::env::current_dir().expect("a current directory"),
        &CancellationToken::new(),
    )
    .await
    .expect_err("a non-string input is refused");

    assert_eq!(fed, "shell input must be a string when provided.");

    let saved = shell(
        &json!({ "command": "true", "saveOutput": "no" }),
        &std::env::current_dir().expect("a current directory"),
        &CancellationToken::new(),
    )
    .await
    .expect_err("a non-boolean saveOutput is refused");

    assert_eq!(saved, "shell saveOutput must be a boolean when provided.");
}

/// The watcher flag the blocking runner polls is the same one the token drives; this pins
/// the wiring itself, which no end-to-end run can distinguish from a lucky poll.
#[tokio::test]
async fn the_cancellation_flag_is_set_by_the_watcher() {
    let token = CancellationToken::new();
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watcher = Arc::clone(&flag);
    let watched = token.clone();

    let handle = tokio::spawn(async move {
        watched.cancelled().await;
        watcher.store(true, Ordering::SeqCst);
    });

    assert!(!flag.load(Ordering::SeqCst));

    token.cancel();
    handle.await.expect("the watcher task does not panic");

    assert!(flag.load(Ordering::SeqCst));
}
