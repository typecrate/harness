//! The `syntax` tool: does this snippet PARSE, without executing it.
//!
//! TypeScript origins
//! ------------------
//!   * `tools/syntax/syntax.tool.ts` - the language table and its aliases, the two
//!     refusals, the dispatch, and the `{code, language} -> boolean` contract;
//!   * `tools/syntax/javascript.ts` - a V8 CLASSIC-SCRIPT parse (`new node:vm.Script`), so
//!     `import`/`export` and top-level `await` are parse errors on the `javascript` half;
//!   * `tools/syntax/typescript.ts` - `transpileModule` with `reportDiagnostics`, i.e.
//!     SYNTAX diagnostics only, with the dialect chosen by the file name
//!     (`snippet.ts` / `snippet.tsx` / `snippet.jsx`);
//!   * `tools/syntax/json.ts` - `JSON.parse` after dropping one leading byte-order mark;
//!   * `tools/syntax/python.ts` - a real CPython 3 interpreter through `compile()`, with the
//!     exit codes used as a PROTOCOL (0 valid, 1 `SyntaxError`, anything else infrastructure)
//!     and every child process async + bounded.
//!
//! THE BOOLEAN CONTRACT IS PRESERVED EXACTLY
//! -----------------------------------------
//! `false` always means "this snippet did not parse". Everything the tool cannot attempt is
//! an ERROR, never a verdict: an unsupported language name, a non-string `code`, a missing
//! Python interpreter, a cancelled or timed-out Python check, and a Python run that ended on
//! a signal or an unexpected exit code. A model that reads `false` is entitled to conclude
//! its source is broken, which is why an unavailable checker must not impersonate one.
//!
//! HOW EACH HALF IS ANSWERED HERE
//! ------------------------------
//!   * `javascript` / `typescript` / `tsx` / `jsx`: the `oxc_parser` crate, the same
//!     grammar-for-grammar approach as the reference - a real ECMAScript/TypeScript/JSX
//!     parser, no transforms, no emit, no type checking. `javascript` is parsed as a
//!     SCRIPT (`SourceType::script()`), which is what makes module syntax and top-level
//!     `await` parse `false` exactly like `node:vm`'s `Script`; the TypeScript family uses
//!     `SourceType::ts()` / `tsx()` / `jsx()`, the direct equivalent of the reference's
//!     per-dialect file name. The verdict is "no diagnostics AND no fatal error".
//!   * `json`: `serde_json`, the strict RFC 8259 reader `JSON.parse` is - comments, trailing
//!     commas, single quotes and two top-level values are all `false`. One leading BOM is
//!     dropped first (`json.ts:35`).
//!   * `python`: a real `python3`/`python` child process, async and bounded exactly like
//!     `python.ts` (probe cached for the process lifetime, a 10 s ceiling on the check, a
//!     5 s ceiling on the probe, an abort path, and 8 KiB of captured diagnostics).
//!
//! Documented residuals against the reference (verdict-equivalent inputs only):
//!   * DIAGNOSTIC TEXT differs from V8's and tsc's - the tool publishes a boolean, so no
//!     caller can observe it;
//!   * a JSON number outside the IEEE-754 range (`1e400`) is `true` for `JSON.parse`
//!     (it coerces to `Infinity`) and `false` here, because serde_json rejects the value
//!     rather than the grammar. Every other JSON case in the reference test suite agrees.
//!   * JSON nesting deeper than serde_json's 128-frame limit is `false` here, where the
//!     reference's limit is V8's call stack;
//!   * a Python check that a host cannot cancel mid-run is reported by KILLING the
//!     interpreter: the reference signals the process GROUP, but its child is not a group
//!     leader, so `kill(-pid)` misses and Node's fallback root kill is what actually lands.
//!     A plain root kill here is therefore the same observable behaviour.

use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use oxc_allocator::Allocator;
use oxc_parser::Parser;
use oxc_span::SourceType;

/// `SYNTAX_LANGUAGE_SUMMARY` (`syntax.tool.ts:103`), one definition for the refusal and for
/// the catalogue text.
pub const LANGUAGE_SUMMARY: &str =
    "javascript (js), typescript (ts), tsx, jsx, json, python (py, python3)";

/// `PYTHON_PROBE_TIMEOUT_MS` (`python.ts:40`): `--version` answers in milliseconds, so
/// anything slower is a hung shim rather than an interpreter.
pub const PYTHON_PROBE_TIMEOUT_MS: u64 = 5_000;
/// `PYTHON_CHECK_TIMEOUT_MS` (`python.ts:44`): parsing is fast; ten seconds is a cold start
/// on a loaded machine, and there is no unbounded value.
pub const PYTHON_CHECK_TIMEOUT_MS: u64 = 10_000;
/// `PYTHON_STDERR_MAX_BYTES` (`python.ts:47`): the cap on captured interpreter
/// diagnostics, so a pathological traceback cannot flood the tool result.
pub const PYTHON_STDERR_MAX_BYTES: usize = 8 * 1024;
/// `PYTHON_CANDIDATES` (`python.ts:37`): `python3` is the POSIX name, `python` the Windows
/// one. The probe tests the EXIT STATUS, because the Windows Store `python3` shim spawns
/// fine and then refuses to run.
const PYTHON_CANDIDATES: [&str; 2] = ["python3", "python"];

/// `PYTHON_CHECK_SCRIPT` (`python.ts:64-75`), verbatim. `compile()` turns the source into a
/// code object without running anything, and the exit code is the protocol: 0 parsed, 1
/// `SyntaxError`, 2 anything else (a stdin decode failure, a `ValueError`, an unexpected
/// exception) - which is infrastructure, never a verdict.
///
/// stdin is read from the BINARY buffer and decoded as UTF-8 explicitly: on Windows a text
/// pipe uses the locale codec, which turned valid non-ASCII source into a false verdict.
pub const PYTHON_CHECK_SCRIPT: &str = concat!(
    "import sys\n",
    "import traceback\n",
    "try:\n",
    "    source = sys.stdin.buffer.read().decode(\"utf-8\")\n",
    "    compile(source, \"<harness>\", \"exec\")\n",
    "except SyntaxError:\n",
    "    sys.exit(1)\n",
    "except BaseException:\n",
    "    traceback.print_exc()\n",
    "    sys.exit(2)"
);

/// The tail of the unsupported-language refusal, kept as a constant so the sentence the tool
/// throws and the sentence the tests pin cannot drift (`syntax.tool.ts:121-123`).
pub const UNSUPPORTED_TAIL: &str = "A language outside that list has no parser here, so it is reported as an error rather than answered with a syntax verdict.";

/// The missing-interpreter refusal (`python.ts:156-158`): a setup problem is named as one, so
/// `false` keeps meaning "your source is broken".
pub const NO_INTERPRETER_MESSAGE: &str =
    "syntax requires a Python 3 interpreter on PATH (tried: python3, python).";

/// `LANGUAGE_ALIASES` (`syntax.tool.ts:74-87`), keyed by the normalized spelling
/// (trimmed, case-folded, one leading dot removed).
const LANGUAGE_ALIASES: [(&str, &str); 12] = [
    ("javascript", "javascript"),
    ("js", "javascript"),
    ("ecmascript", "javascript"),
    ("typescript", "typescript"),
    ("ts", "typescript"),
    ("tsx", "tsx"),
    ("jsx", "jsx"),
    ("json", "json"),
    ("python", "python"),
    ("python3", "python"),
    ("py", "python"),
    ("py3", "python"),
];

/// `String(value)` for a JSON value, because both refusals quote what the caller SENT
/// (`syntax.tool.ts:122`, `:134`).
///
/// The canonical copy of this helper is `stream::protocol::display_value`
/// (`protocol.rs:672`); it is private there and that file is outside this task's scope, so
/// the semantics are restated rather than reached across a module boundary: `undefined` and
/// `null` print differently, an array is comma-joined, an object is `"[object Object]"`.
fn display_value(value: Option<&Value>) -> String {
    match value {
        // An absent field is `undefined` in the reference; JSON `null` is a different value.
        None => "undefined".to_owned(),
        Some(Value::Null) => "null".to_owned(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Bool(flag)) => flag.to_string(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                // `Array.prototype.join` renders `null`/`undefined` elements as the empty
                // string rather than their usual spelling.
                Value::Null => String::new(),
                other => display_value(Some(other)),
            })
            .collect::<Vec<String>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".to_owned(),
    }
}

/// `resolveLanguage` (`syntax.tool.ts:111-124`), including its verbatim refusal.
///
/// A non-string and an absent language go through the same rejection: "no language" and
/// "the wrong type of language" are one caller mistake, and the tool has to refuse rather
/// than guess a parser. The echo is the RAW value, so `'  Rust  '` shows its padding.
///
/// # Errors
///
/// Names the support list and the value that was received.
fn resolve_language(value: Option<&Value>) -> Result<&'static str, String> {
    if let Some(Value::String(raw)) = value {
        // A leading dot is how a caller names a language by its extension (".ts").
        let normalized = raw.trim().to_lowercase();
        let normalized = normalized.strip_prefix('.').unwrap_or(&normalized);

        for (alias, canonical) in LANGUAGE_ALIASES {
            if alias == normalized {
                return Ok(match canonical {
                    "javascript" => "javascript",
                    "typescript" => "typescript",
                    "tsx" => "tsx",
                    "jsx" => "jsx",
                    "json" => "json",
                    _ => "python",
                });
            }
        }
    }

    Err(format!(
        "syntax supports {LANGUAGE_SUMMARY}; received \"{}\". {}",
        display_value(value),
        UNSUPPORTED_TAIL
    ))
}

/// `requireCode` (`syntax.tool.ts:132-138`).
///
/// Every checker reads "did not parse" as `false`, so a value that is not source text at
/// all is a malformed REQUEST, not a verdict - without this guard `code: 7` would reach a
/// parser and come back as a misleading `false`.
///
/// # Errors
///
/// `syntax code must be a string; received "<String(value)>".`
fn require_code(arguments: &Value, key: &str) -> Result<String, String> {
    match super::result::argument_value(arguments, key) {
        Some(Value::String(text)) => Ok(text.clone()),
        other => Err(format!(
            "syntax code must be a string; received \"{}\".",
            display_value(other)
        )),
    }
}

/// Whether one snippet parses under `source_type` with NO diagnostics.
///
/// `fatal_error` means the parser gave up; `diagnostics` also covers the recoverable syntax
/// errors, which still make the program invalid (`ParserReturn` documents exactly that).
/// Anything the parser reports is a `false` verdict, matching both V8 (a thrown `Script`)
/// and `transpileModule` (a non-empty `diagnostics` list).
fn parses(source_type: SourceType, code: &str) -> bool {
    // The arena owns the AST the parser returns, so it has to outlive `parse()`.
    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, code, source_type).parse();

    !parsed.fatal_error && parsed.diagnostics.is_empty()
}

/// `checkJavaScript` (`javascript.ts:20`): a V8 CLASSIC SCRIPT parse.
///
/// `SourceType::script()` is the equivalent goal symbol, and it is what makes module syntax
/// (`import`/`export`) and top-level `await` parse `false` here as they do there. That is
/// the published contract of the `javascript` name, not an accident: a caller who means a
/// module uses `typescript`.
fn check_javascript(code: &str) -> bool {
    parses(SourceType::script(), code)
}

/// `checkTypeScript` (`typescript.ts:73`): syntax diagnostics only, dialect from the
/// language NAME (the reference picks the compiler's `ScriptKind` from the file name).
///
/// A TYPE error is not a syntax error, so `const x: number = "s"` is `true` by contract.
/// `<div/>` parses in `tsx`/`jsx` and does not parse in `typescript`, where `<div>` is a
/// type assertion; `.jsx` has no TypeScript grammar, so an annotation is a parse error.
fn check_typescript(code: &str, language: &str) -> bool {
    let source_type = match language {
        "tsx" => SourceType::tsx(),
        "jsx" => SourceType::jsx(),
        // `SourceType::ts()` is unambiguous, i.e. a module if ESM syntax appears, which is
        // what `transpileModule` compiles.
        _ => SourceType::ts(),
    };

    parses(source_type, code)
}

/// `checkJson` (`json.ts:39-48`): one valid JSON document, strictly.
///
/// `JSON.parse` IS the ECMAScript definition of that grammar, so comments, trailing commas,
/// single quotes and two top-level values are all `false`. One leading byte-order mark is
/// dropped first: the grammar lets an implementation ignore it, files on disk carry one
/// often, and the ts/tsx/jsx half ignores it too. Every parse failure is a verdict, which is
/// why this half has no infrastructure path.
fn check_json(code: &str) -> bool {
    let body = code.strip_prefix('\u{feff}').unwrap_or(code);

    serde_json::from_str::<Value>(body).is_ok()
}

/// `findPython`/`probe` (`python.ts:84-134`), cached for the process lifetime.
///
/// The CACHE HOLDS THE LOOKUP, not just its answer (`:77-79`), so two concurrent checks
/// cannot probe twice. `null` is a real answer too: a host without Python is a permanent
/// property of the process, and re-probing every call would pay a 5 s ceiling forever.
async fn find_python() -> Option<String> {
    for candidate in PYTHON_CANDIDATES {
        if probe(candidate).await {
            return Some(candidate.to_owned());
        }
    }

    None
}

/// `probe` (`python.ts:84-119`): is this interpreter real? Only the EXIT STATUS counts, so
/// a shim that spawns and then fails, or one that never answers at all, is unusable.
async fn probe(candidate: &str) -> bool {
    let mut command = tokio::process::Command::new(candidate);

    command
        .arg("--version")
        .stdin(Stdio::null())
        // Both streams are discarded: the answer is the exit status, and `--version` writes
        // to stdout on some builds and stderr on others.
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    hide_console(&mut command);

    let mut child = match command.spawn() {
        Ok(child) => child,
        // The candidate is not on PATH at all.
        Err(_) => return false,
    };

    let answered =
        tokio::time::timeout(Duration::from_millis(PYTHON_PROBE_TIMEOUT_MS), child.wait()).await;

    match answered {
        Ok(Ok(status)) => status.success(),
        // A hung shim is killed and treated as absent (`:110-113`).
        Ok(Err(_)) | Err(_) => {
            let _ = child.start_kill();

            false
        }
    }
}

/// `windowsHide: true` on every child this tool spawns.
#[cfg(windows)]
fn hide_console(command: &mut tokio::process::Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn hide_console(_command: &mut tokio::process::Command) {}

/// Ends one child process by id, without holding the `Child`.
///
/// The reference calls `terminateTree`, whose POSIX branch signals the child's process
/// GROUP - but that child is not a group leader, so `kill(-pid)` misses and Node's fallback
/// ROOT kill is what actually lands. A root kill here is therefore the same observable
/// behaviour. `tokio`'s `start_kill` needs `&mut Child`, which the deadline and cancellation
/// arms cannot reach while the wait borrows the child, and this crate bans `unsafe` and
/// `libc`, so the signal goes through a killer program - the same route
/// `tools::command::terminate_tree` takes for a whole tree.
fn kill_pid(pid: u32) {
    let (program, arguments): (&str, Vec<String>) = if cfg!(windows) {
        (
            "taskkill",
            vec!["/PID".to_owned(), pid.to_string(), "/F".to_owned()],
        )
    } else {
        ("/bin/kill", vec!["-9".to_owned(), pid.to_string()])
    };

    // Fire-and-forget: the bounded wait on the reaper in the caller is what bounds the call,
    // and tokio reaps the killer process itself.
    let _ = tokio::process::Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

/// `withDiagnostics` (`python.ts:137-138`): append captured diagnostics without a dangling
/// space when there are none.
fn with_diagnostics(message: &str, diagnostics: &str) -> String {
    if diagnostics.is_empty() {
        return message.to_owned();
    }

    format!("{message} {diagnostics}")
}

/// The signal name a killed interpreter ended on, or `None` when it exited by itself.
///
/// Node reports the NAME (`exitSignal`), and this crate has no `libc`, so the number comes
/// from `ExitStatus::signal` and `tools::command::signal_name` turns it into the same name.
fn signal_of(status: &std::process::ExitStatus) -> Option<String> {
    // The signal number exists only on unix; `ExitStatusExt` is the only way to read it and
    // it is not in scope on windows, so the platform split is on the BINDING rather than on
    // a block with a `return` inside (a block form trips `needless_return` on unix builds
    // and an `unused_variable` error on windows builds at the same time).
    #[cfg(not(unix))]
    let _ = status;

    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;

        status.signal()
    };

    #[cfg(not(unix))]
    let signal: Option<i32> = None;

    signal.map(super::command::signal_name)
}

/// `checkPython` (`python.ts:143-308`): a real interpreter, async and bounded.
///
/// Cancellation and the 10 s ceiling both KILL the interpreter and then settle, so a caller
/// never observes completion while the child is still alive; neither is ever reported as
/// `false`, because `false` means "your source is broken".
///
/// # Errors
///
/// Any of the reference's setup/cancellation sentences, or the exit-code protocol's
/// infrastructure sentence with the captured diagnostics appended.
async fn check_python(code: &str, signal: &CancellationToken) -> Result<bool, String> {
    // Cancelled before anything ran: do not even probe the interpreter (`:149-151`).
    if signal.is_cancelled() {
        return Err("syntax Python check was cancelled before it started.".to_owned());
    }

    // The process-lifetime interpreter lookup, shared by concurrent checks.
    static INTERPRETER: tokio::sync::OnceCell<Option<String>> = tokio::sync::OnceCell::const_new();

    let interpreter = INTERPRETER.get_or_init(find_python).await;

    let Some(interpreter) = interpreter.as_deref() else {
        return Err(NO_INTERPRETER_MESSAGE.to_owned());
    };

    // Cancelled while the probe was running: do not start work that is already unwanted.
    if signal.is_cancelled() {
        return Err("syntax Python check was cancelled before it started.".to_owned());
    }

    let mut command = tokio::process::Command::new(interpreter);

    command
        .args(["-X", "utf8", "-c", PYTHON_CHECK_SCRIPT])
        .stdin(Stdio::piped())
        // stdout is never read: the script prints nothing.
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        // UTF-8 mode for the interpreter's stdio, independent of the host locale.
        .env("PYTHONUTF8", "1")
        .env("PYTHONIOENCODING", "utf-8");

    hide_console(&mut command);

    let mut child = command
        .spawn()
        .map_err(|_| "syntax could not start the Python interpreter.".to_owned())?;

    // Ownership is split so the stdin pump, the stderr drain and the wait can run at the
    // same time: a child that fills its stderr pipe while still reading stdin would
    // otherwise deadlock against the wait.
    let stdin = child.stdin.take();
    let stderr = child.stderr.take();

    // Hand over the source and close stdin; `compile()` reads it to EOF. An EPIPE here is
    // the interpreter having already exited, and the exit status below is the real verdict
    // (`python.ts:241-243`), so it is ignored.
    let payload = code.as_bytes().to_vec();

    let feeder = tokio::spawn(async move {
        if let Some(mut pipe) = stdin {
            let _ = pipe.write_all(&payload).await;
            let _ = pipe.shutdown().await;
        }
    });

    // `BoundedText(PYTHON_STDERR_MAX_BYTES)`: the HEAD is kept. The pipe is still drained
    // past the cap, or the interpreter would block writing a traceback nobody reads.
    let drainer = tokio::spawn(async move {
        let Some(mut pipe) = stderr else {
            return String::new();
        };

        let mut kept: Vec<u8> = Vec::new();
        let mut chunk = [0_u8; 8 * 1024];

        // A read error ends the drain exactly as the EOF does: whatever was kept is what
        // the note reports, and the pipe must stay drained or the child stays blocked.
        while let Ok(read) = pipe.read(&mut chunk).await {
            if read == 0 {
                break;
            }

            let room = PYTHON_STDERR_MAX_BYTES.saturating_sub(kept.len());

            if room > 0 {
                kept.extend_from_slice(&chunk[..read.min(room)]);
            }
        }

        String::from_utf8_lossy(&kept).into_owned()
    });

    // The waiter task OWNS the child, which is what frees the deadline and the cancellation
    // arm to kill it: `wait` holds a mutable borrow for as long as it is pending, so an arm
    // could not reach the child itself, and this crate has no `libc` to signal a pid
    // directly. tokio reaps the child inside the task, so nothing here leaks a zombie.
    let pid = child.id().unwrap_or_default();
    let mut waiter = tokio::spawn(async move { child.wait().await });

    enum Settlement {
        Exited(std::process::ExitStatus),
        /// The run was killed because it outlived its ceiling, or because the platform
        /// refused to reap it inside the grace window.
        TimedOut,
        Cancelled,
    }

    let settlement = tokio::select! {
        biased;

        status = &mut waiter => match status {
            Ok(Ok(status)) => Settlement::Exited(status),
            // A waiter that could not be joined never produced a verdict either, so it is
            // reported as infrastructure rather than as a syntax answer.
            _ => Settlement::Cancelled,
        },

        () = tokio::time::sleep(Duration::from_millis(PYTHON_CHECK_TIMEOUT_MS)) => {
            kill_pid(pid);

            // The reference awaits its tree kill before settling, so the reaped child is
            // awaited here too - bounded, because a wedged interpreter must not outlive the
            // call it was killed for.
            if tokio::time::timeout(
                Duration::from_millis(super::command::KILL_GRACE_MS),
                &mut waiter,
            )
            .await
            .is_err()
            {
                return Err(format!(
                    "syntax timed out after {PYTHON_CHECK_TIMEOUT_MS} ms checking Python syntax."
                ));
            }

            Settlement::TimedOut
        }

        () = signal.cancelled() => {
            kill_pid(pid);

            if tokio::time::timeout(
                Duration::from_millis(super::command::KILL_GRACE_MS),
                &mut waiter,
            )
            .await
            .is_err()
            {
                return Err("syntax Python check was cancelled.".to_owned());
            }

            Settlement::Cancelled
        }
    };

    let _ = feeder.await;

    match settlement {
        Settlement::TimedOut => Err(format!(
            "syntax timed out after {PYTHON_CHECK_TIMEOUT_MS} ms checking Python syntax."
        )),
        Settlement::Cancelled => Err("syntax Python check was cancelled.".to_owned()),
        Settlement::Exited(status) => {
            // Diagnostics only matter for the failure sentences; a verdict carries none.
            let captured = drainer.await.unwrap_or_default();
            let diagnostics = captured.trim();

            // A run ended by a SIGNAL never produced a verdict (`:270-280`).
            if let Some(name) = signal_of(&status) {
                return Err(with_diagnostics(
                    &format!("syntax Python interpreter ended on signal {name}."),
                    diagnostics,
                ));
            }

            // The exit-code protocol: 1 is the ONLY non-zero code that means "invalid
            // syntax" (`compile()` raised SyntaxError); 2 is the script's infrastructure
            // code, and anything else is a host problem.
            match status.code() {
                Some(0) => Ok(true),
                Some(1) => Ok(false),
                Some(code) => Err(with_diagnostics(
                    &format!("syntax Python interpreter exited with code {code}."),
                    diagnostics,
                )),
                // No code and no signal name: the platform reported neither, so the check
                // never produced a verdict.
                None => Err(with_diagnostics(
                    "syntax Python interpreter ended without an exit code.",
                    diagnostics,
                )),
            }
        }
    }
}

/// `syntax`: `{code, language}` -> `true` / `false`, with the reference's two refusals.
///
/// The order of the two validations is the reference's (`syntax.tool.ts:198-199`): a bad
/// `code` is named before a bad `language`, so a caller that got both wrong learns about the
/// payload first.
///
/// # Errors
///
/// `syntax code must be a string; received "..."`, the unsupported-language sentence, or any
/// of the Python checker's setup/cancellation/infrastructure sentences.
pub(crate) async fn check(arguments: &Value, signal: &CancellationToken) -> Result<Value, String> {
    let code = require_code(arguments, "code")?;
    let language = resolve_language(super::result::argument_value(arguments, "language"))?;

    match language {
        "json" => Ok(Value::Bool(check_json(&code))),
        "javascript" => Ok(Value::Bool(check_javascript(&code))),
        "typescript" | "tsx" | "jsx" => Ok(Value::Bool(check_typescript(&code, language))),
        // The only half that runs a process, and the only one that can fail for a reason
        // other than the source itself.
        _ => check_python(&code, signal).await.map(Value::Bool),
    }
}

#[cfg(test)]
#[path = "syntax_tests.rs"]
mod tests;
