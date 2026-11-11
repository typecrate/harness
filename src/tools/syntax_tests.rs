//! Parity tests for `tools/syntax.rs`, ported case-for-case from
//! `packages/underload/harness/src/tools/syntax/syntax.test.ts`.
//!
//! Every verdict asserted here is the reference test's own expected value, written out
//! exactly: the JavaScript half is a CLASSIC script, the TypeScript family reports SYNTAX
//! diagnostics only, JSX needs the tsx/jsx dialect, JSON is strict RFC 8259 with one BOM
//! ignored, and Python answers through the real interpreter's exit-code protocol.
//!
//! Two reference cases cannot be ported as-is and are covered differently, honestly:
//!   * "reports a missing Python interpreter" rewrites `PATH` and re-imports the module.
//!     Here the interpreter lookup is cached for the process lifetime and the suite runs in
//!     parallel, so clearing `PATH` would race every other Python case; the refusal sentence
//!     is pinned textually instead.
//!   * the timeout case needs an interpreter that hangs, which `compile()` cannot be; the
//!     ceiling is pinned as the published constant the reference pins (`PYTHON_CHECK_TIMEOUT_MS`).

use super::{
    LANGUAGE_SUMMARY, NO_INTERPRETER_MESSAGE, PYTHON_CHECK_SCRIPT, PYTHON_CHECK_TIMEOUT_MS,
    PYTHON_STDERR_MAX_BYTES, UNSUPPORTED_TAIL, check, check_javascript, check_json,
    check_typescript, display_value, find_python, with_diagnostics,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

/// The exact text a caller sees for a language with no parser here, written out (not
/// interpolated from `LANGUAGE_SUMMARY`) so a change to the support list is a deliberate edit
/// in two places, exactly like the reference test file.
fn unsupported(received: &str) -> String {
    format!("syntax supports {LANGUAGE_SUMMARY}; received \"{received}\". {UNSUPPORTED_TAIL}")
}

/// The tool's own verdict for one `{code, language}` pair.
async fn verdict(code: Value, language: Value) -> Result<Value, String> {
    check(
        &json!({ "code": code, "language": language }),
        &CancellationToken::new(),
    )
    .await
}

// ── the two refusals ─────────────────────────────────────────────────────────

#[tokio::test]
async fn an_unsupported_language_is_an_error_that_names_the_supported_set() {
    // `syntax.test.ts:279-303`: a language with no parser here is answered with neither
    // `true` nor `false`, and the echo is the RAW spelling the caller sent.
    for (code, language, received) in [
        (json!("puts 1"), json!("ruby"), "ruby"),
        (json!("package main"), json!("go"), "go"),
        (json!("x = 1"), json!("  Rust  "), "  Rust  "),
    ] {
        assert_eq!(
            verdict(code, language).await.expect_err("refused"),
            unsupported(received),
            "{received} must be refused"
        );
    }

    // No language at all is the same caller mistake: an absent field is `undefined`.
    assert_eq!(
        check(&json!({ "code": "x = 1" }), &CancellationToken::new())
            .await
            .expect_err("refused"),
        unsupported("undefined")
    );
    assert_eq!(
        verdict(json!("x = 1"), json!(null))
            .await
            .expect_err("refused"),
        unsupported("null")
    );
    // A non-string language echoes the value, not the type (`String(7)` is `"7"`).
    assert_eq!(
        verdict(json!("x = 1"), json!(7))
            .await
            .expect_err("refused"),
        unsupported("7")
    );

    // The support list in the refusal is the one the catalogue publishes.
    assert_eq!(
        LANGUAGE_SUMMARY,
        "javascript (js), typescript (ts), tsx, jsx, json, python (py, python3)"
    );
    assert_eq!(
        UNSUPPORTED_TAIL,
        "A language outside that list has no parser here, so it is reported as an error rather than answered with a syntax verdict."
    );
}

#[tokio::test]
async fn a_non_string_code_is_refused_instead_of_becoming_a_verdict() {
    // `syntax.test.ts:305-314`. Without this guard `code: 7` would reach a parser and come
    // back as a misleading `false` about source text that was never source text.
    assert_eq!(
        verdict(json!(7), json!("javascript"))
            .await
            .expect_err("refused"),
        "syntax code must be a string; received \"7\"."
    );
    assert_eq!(
        check(&json!({ "language": "json" }), &CancellationToken::new())
            .await
            .expect_err("refused"),
        "syntax code must be a string; received \"undefined\"."
    );
    assert_eq!(
        verdict(json!(null), json!("json"))
            .await
            .expect_err("refused"),
        "syntax code must be a string; received \"null\"."
    );
    assert_eq!(
        verdict(json!(true), json!("json"))
            .await
            .expect_err("refused"),
        "syntax code must be a string; received \"true\"."
    );
    // `String(value)` for the structured kinds: an object, and an array joined by commas
    // with `null` rendering as the empty string.
    assert_eq!(
        verdict(json!({ "a": 1 }), json!("json"))
            .await
            .expect_err("refused"),
        "syntax code must be a string; received \"[object Object]\"."
    );
    assert_eq!(
        verdict(json!(["a", null, 2]), json!("json"))
            .await
            .expect_err("refused"),
        "syntax code must be a string; received \"a,,2\"."
    );
}

#[test]
fn the_value_echo_matches_javascript_string() {
    // The refusal text is observable output, so the echo is pinned directly as well.
    assert_eq!(display_value(None), "undefined");
    assert_eq!(display_value(Some(&Value::Null)), "null");
    assert_eq!(display_value(Some(&json!("ts"))), "ts");
    assert_eq!(display_value(Some(&json!(7.5))), "7.5");
    assert_eq!(display_value(Some(&json!([1, "x", true]))), "1,x,true");
    assert_eq!(display_value(Some(&json!({ "a": [1] }))), "[object Object]");
}

// ── javascript: a classic script, nothing else ───────────────────────────────

#[tokio::test]
async fn javascript_answers_exact_verdicts() {
    // `syntax.test.ts:136-141`.
    assert_eq!(
        verdict(
            json!("const a = 1; function f() { return a + 1; }"),
            json!("javascript")
        )
        .await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(json!("const = ;"), json!("javascript")).await,
        Ok(json!(false))
    );
}

#[tokio::test]
async fn javascript_is_parsed_as_a_classic_script_only() {
    // `syntax.test.ts:143-151`: module syntax and top-level await are parse errors on this
    // half, which is why a caller who means a module uses `typescript`.
    assert_eq!(
        verdict(
            json!("import fs from 'node:fs';\nexport const a = fs;"),
            json!("javascript")
        )
        .await,
        Ok(json!(false))
    );
    assert_eq!(
        verdict(json!("await fetch(\"x\");"), json!("javascript")).await,
        Ok(json!(false))
    );
    // The pure half, called the way the reference calls it (`syntax.test.ts:316-320`).
    assert!(check_javascript("const a = 1;"));
    assert!(!check_javascript("const = ;"));
}

// ── the typescript family: syntax only, dialect by name ─────────────────────

#[tokio::test]
async fn typescript_parses_syntax_without_type_checking() {
    // `syntax.test.ts:153-179`.
    assert_eq!(
        verdict(
            json!("const a: number = 1; function f(x: number): number { return x + a; }"),
            json!("typescript")
        )
        .await,
        Ok(json!(true))
    );
    // Modules and enums parse, unlike the classic-script half.
    assert_eq!(
        verdict(
            json!("import * as fs from 'node:fs';\nexport const a: number = 1;"),
            json!("typescript")
        )
        .await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(
            json!("enum E { A, B }\nconst e: E = E.A;"),
            json!("typescript")
        )
        .await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(json!("const = ;"), json!("typescript")).await,
        Ok(json!(false))
    );
    // A TYPE error is not a syntax error: `true` by contract, not by accident.
    assert_eq!(
        verdict(json!("const x: number = \"s\";"), json!("typescript")).await,
        Ok(json!(true))
    );
}

#[tokio::test]
async fn jsx_parses_only_in_the_tsx_and_jsx_dialects() {
    // `syntax.test.ts:181-204`: the dialect comes from the language NAME, because that is how
    // the reference picks the compiler's ScriptKind.
    assert_eq!(
        verdict(
            json!("const el = <div className=\"x\" />;"),
            json!("typescript")
        )
        .await,
        Ok(json!(false))
    );
    assert_eq!(
        verdict(json!("const el = <div className=\"x\" />;"), json!("tsx")).await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(json!("const el: number = <div>{1}</div>;"), json!("tsx")).await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(
            json!("export const El = () => <div>hi</div>;"),
            json!("jsx")
        )
        .await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(json!("const el = <div>;"), json!("tsx")).await,
        Ok(json!(false))
    );
    // `.jsx` has no TypeScript grammar, so an annotation is a parse error there.
    assert_eq!(
        verdict(json!("const x: number = 1;"), json!("jsx")).await,
        Ok(json!(false))
    );

    // The halves are directly callable parsers, as in `syntax.test.ts:322-329`.
    assert!(check_typescript("const a: number = 1;", "typescript"));
    assert!(check_typescript("const el = <div />;", "tsx"));
    assert!(!check_typescript("const el = <div />;", "typescript"));
}

// ── json: strict rfc 8259, one leading bom ignored ───────────────────────────

#[tokio::test]
async fn json_is_validated_strictly_and_ignores_one_leading_byte_order_mark() {
    // `syntax.test.ts:206-218`. JSONC conveniences and multi-value documents are VERDICTS,
    // never errors.
    for (code, expected) in [
        (json!("[1, 2, 3]"), true),
        (json!("{ /*c*/ \"a\": 1 }"), false),
        (json!("{'a': 1}"), false),
        (json!("{\"a\":1,}"), false),
        (json!("{\"a\":1} {\"b\":2}"), false),
        (json!("\u{feff}{\"a\":1}"), true),
    ] {
        assert_eq!(
            verdict(code.clone(), json!("json")).await,
            Ok(json!(expected)),
            "{code} must answer {expected}"
        );
    }

    assert_eq!(
        verdict(json!("{\"a\":1,\"b\":[1,2,3]}"), json!("json")).await,
        Ok(json!(true))
    );
    assert!(check_json("{\"a\":1}"));
}

// ── python: the real interpreter, through the exit-code protocol ─────────────

#[tokio::test]
async fn python_answers_exact_verdicts_through_the_interpreter() {
    // `syntax.test.ts:220-234`.
    assert_eq!(
        verdict(json!("def f(x):\n    return x + 1"), json!("python")).await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(json!("def f(x) return x"), json!("python")).await,
        Ok(json!(false))
    );
    assert_eq!(
        verdict(json!("x = 1\nprint(x)"), json!("python")).await,
        Ok(json!(true))
    );
    assert_eq!(
        verdict(json!("if True print(1)"), json!("python")).await,
        Ok(json!(false))
    );
}

#[tokio::test]
async fn python_source_is_decoded_as_utf8_whatever_the_host_locale_is() {
    // `syntax.test.ts:236-244`: a Windows pipe used to decode with cp1252 and turn valid
    // non-ASCII source into a false verdict.
    for code in [
        json!("x = \"\u{1F601}\"\n"),
        json!("x = \"caf\u{e9}\"\n"),
        json!("# \u{1F601}\nx = 1\n"),
    ] {
        assert_eq!(
            verdict(code.clone(), json!("python")).await,
            Ok(json!(true)),
            "{code} is valid source"
        );
    }
}

#[tokio::test]
async fn only_a_compile_syntax_error_is_a_false_verdict() {
    // `syntax.test.ts:246-255`, driving `PYTHON_CHECK_SCRIPT` with RAW stdin bytes (invalid
    // UTF-8 cannot travel as a string): 0 valid, 1 SyntaxError, 2 any other failure.
    assert_eq!(script_exit_code(b"x = 1\n").await, 0);
    assert_eq!(script_exit_code("\"café\"\n".as_bytes()).await, 0);
    assert_eq!(script_exit_code(b"def f(x) return x\n").await, 1);
    // 0xFF is not a valid UTF-8 start byte: a DECODE failure, not a verdict.
    assert_eq!(script_exit_code(&[0xff_u8, 0xfe]).await, 2);
}

/// Runs the helper script with raw bytes and returns its exit code
/// (`scriptExitCode`, `syntax.test.ts:62-71`).
async fn script_exit_code(stdin: &[u8]) -> i32 {
    let interpreter = find_python().await.expect("a Python 3 interpreter");
    let mut child = std::process::Command::new(interpreter)
        .args(["-X", "utf8", "-c", PYTHON_CHECK_SCRIPT])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("the helper script starts");

    use std::io::Write;

    {
        let mut pipe = child.stdin.take().expect("a stdin pipe");
        let _ = pipe.write_all(stdin);
    }

    child
        .wait()
        .expect("the helper script settles")
        .code()
        .unwrap_or(-1)
}

#[tokio::test]
async fn a_run_that_never_started_reports_the_cancellation_without_running_anything() {
    // `syntax.test.ts:257-261`: an already-cancelled signal must not even probe PATH.
    let cancelled = CancellationToken::new();

    cancelled.cancel();

    assert_eq!(
        check(
            &json!({ "code": "x = 1", "language": "python" }),
            &cancelled
        )
        .await
        .expect_err("cancelled"),
        "syntax Python check was cancelled before it started."
    );
}

#[tokio::test]
async fn every_documented_alias_resolves_to_its_parser() {
    // `syntax.test.ts:263-277`: names are trimmed, case-folded, and may carry a leading dot.
    let module_source = json!("import fs from 'node:fs';\nexport const a = fs;");

    for (code, language, expected) in [
        (json!("const a = 1;"), json!("JS"), true),
        (json!("const a = 1;"), json!(" ecmascript "), true),
        (module_source.clone(), json!("TS"), true),
        (module_source.clone(), json!(".ts"), true),
        (module_source.clone(), json!("TypeScript"), true),
        (json!("const el = <div />;"), json!(".TSX"), true),
        (json!("x = 1"), json!("py"), true),
        (json!("x = 1"), json!("python3"), true),
        (json!("x = 1"), json!("PY3"), true),
    ] {
        assert_eq!(
            verdict(code.clone(), language.clone()).await,
            Ok(json!(expected)),
            "{language} must resolve to a parser"
        );
    }
}

// ── the constants and messages the reference publishes ───────────────────────

#[test]
fn the_python_ceiling_and_diagnostics_cap_are_the_published_ones() {
    // `syntax.test.ts:350-352`, plus the 8 KiB cap from `python.ts:47`.
    assert_eq!(PYTHON_CHECK_TIMEOUT_MS, 10_000);
    assert_eq!(PYTHON_STDERR_MAX_BYTES, 8 * 1024);
    // The script is the verdict boundary, so its protocol lines are pinned verbatim.
    assert_eq!(
        PYTHON_CHECK_SCRIPT,
        "import sys\nimport traceback\ntry:\n    source = sys.stdin.buffer.read().decode(\"utf-8\")\n    compile(source, \"<harness>\", \"exec\")\nexcept SyntaxError:\n    sys.exit(1)\nexcept BaseException:\n    traceback.print_exc()\n    sys.exit(2)"
    );
}

#[test]
fn diagnostics_are_appended_without_a_dangling_space() {
    // `withDiagnostics` (`python.ts:137-138`).
    assert_eq!(
        with_diagnostics("syntax Python interpreter exited with code 2.", ""),
        "syntax Python interpreter exited with code 2."
    );
    assert_eq!(
        with_diagnostics("syntax Python interpreter ended on signal SIGTERM.", "boom"),
        "syntax Python interpreter ended on signal SIGTERM. boom"
    );
}

#[tokio::test]
async fn the_interpreter_lookup_resolves_on_a_host_that_has_one() {
    // `syntax.test.ts:331-348` clears PATH to pin the missing-interpreter refusal, which a
    // process-lifetime probe cache cannot do under a parallel suite. What IS pinned here is
    // the other half of that contract: the lookup finds a real interpreter (so every Python
    // verdict above is a real one), and the refusal it would otherwise throw is published as
    // a constant the reference sentence matches byte for byte.
    assert!(
        find_python().await.is_some(),
        "this suite needs a Python 3 interpreter on PATH, exactly like the reference tests"
    );
    assert_eq!(
        NO_INTERPRETER_MESSAGE,
        "syntax requires a Python 3 interpreter on PATH (tried: python3, python)."
    );
}
