//! The `syntax` tool: does this snippet PARSE, without executing it.
//!
//! TypeScript origin: `tools/syntax/syntax.tool.ts` (the language table, the two
//! refusals, and the boolean contract) plus its per-language checkers.
//!
//! THE BOOLEAN CONTRACT IS PRESERVED EXACTLY: `false` always means "this snippet did
//! not parse", and anything the tool cannot attempt (an unsupported language, a
//! non-string `code`) is an ERROR, never a verdict.
//!
//! PARSER FIDELITY, STATED: TypeScript runs a real V8 classic-script parse for
//! `javascript`, the TypeScript compiler for `ts/tsx/jsx`, `JSON.parse` for `json`, and a
//! CPython `compile()` for `python`. This port runs the real `serde_json` parser for JSON
//! and a bounded bracket/quote/comment balance check for the code languages (the stage
//! contract's "lightweight check"). The consequences are honest and documented:
//!   * unbalanced braces, quotes that never close, and a comment left open are `false`;
//!   * a snippet that balances but is not grammatically valid (e.g. `const = 1;`) is
//!     `true` here where V8 would answer `false`;
//!   * `import`/`export` in a `javascript` snippet is `true` here where TS's classic-script
//!     parse answers `false`.

use serde_json::Value;

/// `SYNTAX_LANGUAGE_SUMMARY` (`syntax.tool.ts:89`), one definition for the refusal and
/// for the catalogue text.
pub const LANGUAGE_SUMMARY: &str =
    "javascript (js), typescript (ts), tsx, jsx, json, python (py, python3)";

/// `LANGUAGE_ALIASES` (`syntax.tool.ts:74-87`), keyed by the normalized spelling
/// (trimmed, case-folded, leading dot removed).
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

/// `resolveLanguage` (`syntax.tool.ts:96-110`), including its verbatim refusal.
///
/// # Errors
///
/// Names the support list and the value that was received.
fn resolve_language(value: Option<&str>) -> Result<&'static str, String> {
    if let Some(raw) = value {
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

    let received = value.unwrap_or("undefined");

    Err(format!(
        "syntax supports {LANGUAGE_SUMMARY}; received \"{received}\". A language outside that list has no parser here, so it is reported as an error rather than answered with a syntax verdict."
    ))
}

/// `checkJson` (`syntax/json.ts`): one valid JSON document, strictly.
///
/// serde_json is the strict RFC 8259 reader TS needs (comments, trailing commas and
/// single quotes are all `false`). One leading byte-order mark is tolerated, exactly as
/// `json.ts:38` documents.
fn check_json(code: &str) -> bool {
    let body = code.strip_prefix('\u{feff}').unwrap_or(code);

    serde_json::from_str::<Value>(body).is_ok()
}

/// The lightweight balance check the code languages get in this port (see the module
/// header for what it does and does not catch).
///
/// The scanner is total - it never panics and never runs unbounded - and it tracks:
///   * string/comment nesting, so an unterminated `'`, `"`, `` ` ``, `/*` or `"""` fails;
///   * bracket nesting, so an unmatched or mis-ordered `{}`, `[]`, `()` fails;
///   * escapes, so `"\""` closes;
///   * line comments (`//`, and `#` for python) and the `/* */` block comment.
fn balanced(code: &str, language: &str) -> bool {
    let is_python = language == "python";
    let bytes: Vec<char> = code.chars().collect();
    let mut stack: Vec<char> = Vec::new();
    let mut index = 0;
    // The open delimiter, if any, that a string or block comment is holding.
    let mut state: State = State::Code;

    while index < bytes.len() {
        let character = bytes[index];
        let next = bytes.get(index + 1).copied();

        match state {
            State::Code => {
                // Line comments run to the end of the line and are skipped whole.
                if character == '/' && next == Some('/') {
                    while index < bytes.len() && bytes[index] != '\n' {
                        index += 1;
                    }

                    continue;
                }

                if character == '/' && next == Some('*') {
                    state = State::BlockComment;
                    index += 2;
                    continue;
                }

                if is_python && character == '#' {
                    while index < bytes.len() && bytes[index] != '\n' {
                        index += 1;
                    }

                    continue;
                }

                if character == '"' || character == '\'' || character == '`' {
                    state = State::String(character);
                    index += 1;
                    continue;
                }

                if matches!(character, '{' | '[' | '(') {
                    stack.push(matching(character));
                } else if matches!(character, '}' | ']' | ')') {
                    match stack.last() {
                        Some(expected) if *expected == character => {
                            stack.pop();
                        }
                        // An unmatched closer is decisive: no later balance can make the
                        // snippet parse.
                        _ => return false,
                    }
                }
            }
            State::BlockComment => {
                if character == '*' && next == Some('/') {
                    state = State::Code;
                    index += 2;
                    continue;
                }
            }
            State::String(open) => {
                if character == '\\' {
                    // An escape consumes its partner, including a real newline.
                    index += 2;
                    continue;
                }

                // A plain `'`/`"` string cannot cross a newline in any of these languages;
                // a template literal can.
                if character == open || (character == '\n' && open != '`') {
                    state = State::Code;
                    index += 1;
                    continue;
                }
            }
        }

        index += 1;
    }

    // An unterminated string or block comment, or any bracket left open, is a parse
    // failure.
    state == State::Code && stack.is_empty()
}

/// The scanner's mode.
#[derive(PartialEq, Eq)]
enum State {
    Code,
    BlockComment,
    String(char),
}

/// The closer a bracket expects.
fn matching(character: char) -> char {
    match character {
        '{' => '}',
        '[' => ']',
        _ => ')',
    }
}

/// `syntax`: `{code, language}` -> `true` / `false`, with TS's two refusals.
///
/// # Errors
///
/// `syntax code must be a string; received "..."` or the unsupported-language sentence.
pub(crate) fn check(arguments: &Value) -> Result<Value, String> {
    let code = match super::result::argument_value(arguments, "code") {
        Some(Value::String(text)) => text.clone(),
        Some(other) => {
            return Err(format!(
                "syntax code must be a string; received \"{}\".",
                super::result::type_of(other)
            ));
        }
        None => {
            return Err("syntax code must be a string; received \"undefined\".".to_owned());
        }
    };

    let requested = super::result::optional_text(arguments, "language");
    let language = resolve_language(requested.as_deref())?;

    Ok(Value::Bool(match language {
        "json" => check_json(&code),
        _ => balanced(&code, language),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_answers_the_parse_verdict() {
        assert_eq!(
            check(&json!({"code": "{\"a\":1}", "language": "json"})),
            Ok(json!(true))
        );
        assert_eq!(
            check(&json!({"code": "{a:1}", "language": "json"})),
            Ok(json!(false))
        );
        assert_eq!(
            check(&json!({"code": "{}", "language": ".json"})),
            Ok(json!(true))
        );
    }

    #[test]
    fn an_unsupported_language_is_an_error_not_a_verdict() {
        let error = check(&json!({"code": "x", "language": "rust"})).expect_err("refused");

        assert!(error.contains(
            "syntax supports javascript (js), typescript (ts), tsx, jsx, json, python (py, python3)"
        ));
        assert!(error.contains("received \"rust\""));
    }

    #[test]
    fn a_non_string_code_is_refused() {
        assert_eq!(
            check(&json!({"code": 7, "language": "js"})),
            Err("syntax code must be a string; received \"number\".".to_owned())
        );
    }

    #[test]
    fn unbalanced_brackets_and_open_quotes_fail() {
        assert_eq!(
            check(&json!({"code": "function f() { return 1; }", "language": "js"})),
            Ok(json!(true))
        );
        assert_eq!(
            check(&json!({"code": "function f() { return 1;", "language": "js"})),
            Ok(json!(false))
        );
        assert_eq!(
            check(&json!({"code": "const s = \"unclosed;", "language": "js"})),
            Ok(json!(false))
        );
        assert_eq!(
            check(&json!({"code": "const s = \"a\\\"b\";", "language": "js"})),
            Ok(json!(true))
        );
        assert_eq!(
            check(&json!({"code": "# c\nx = (1, 2)", "language": "py"})),
            Ok(json!(true))
        );
        assert_eq!(
            check(&json!({"code": "x = (1, 2))", "language": "python"})),
            Ok(json!(false))
        );
    }
}
