//! The file tools: `read`, `write`, `edit`, `fileList`, `glob`, `grep`.
//!
//! TypeScript origins
//! ------------------
//!   * `tools/files/read.tool.ts` - the bounded window, the fair byte split, the
//!     per-entry ENOENT shape (`missingEntry`), and the binary/directory/permission
//!     refusal sentences;
//!   * `tools/files/write.tool.ts` - mkdir-on-write and the
//!     `write failed on {path}: {reason} Writes already completed: {list}. No rollback was performed.`
//!     partial-success sentence;
//!   * `tools/files/edit.tool.ts` - one entry per requested edit, per-file "read once,
//!     apply in request order, write once", and the same partial-success sentence;
//!   * `tools/files/fileList.tool.ts` - the paged listing whose entries are prefixed
//!     with the directory they came from, directories end with `/`, sorted by code
//!     point, 200/1000 entries and a 64 KiB text budget;
//!   * `tools/files/glob.tool.ts` + `glob.ts` - the walker, the pattern language, and
//!     the `{files, truncated, offset, limit, nextOffset, bytes, contentBudget,
//!     stopReason, partial, searched, excluded, patterns, note?}` document;
//!   * `tools/files/grep.tool.ts` - the three output modes, the per-line snippet cap
//!     (1024 B), the 1 MiB file cap, and the 8 KiB binary sniff.
//!
//! RESULT STRINGS ARE POSIX-STYLE ON EVERY PLATFORM (`glob.ts:117` `toPosixPath`),
//! because the model must not have to know which host the server runs on. Absolute
//! paths are reported as the platform spells them, exactly like `path.resolve()`.
//!
//! STATED SIMPLIFICATIONS (the full TS engine is a project of its own):
//!   * `edit` ports TS's FIRST matching tier only: the LF view (a `\n` oldString matches
//!     a CRLF file and the file's endings are preserved through `with_region_endings`).
//!     The typographic-quote tier and the trailing-whitespace tier of `edit.tool.ts:20-33`
//!     are omitted, so the optional `match` field is never present and a near-miss is a
//!     refusal the model can fix. TS also aligns replacement lines to region lines with a
//!     longest-common-subsequence pass (`alignLines`); this port joins every replacement
//!     line with the matched region's dominant ending, which differs only inside a hunk
//!     that mixes CRLF and LF.
//!   * `glob`/`grep` skip `.git` and `node_modules` and read no `.gitignore`. TS parses
//!     ignore files (`files/gitignore.ts`); the counters that would carry the result are
//!     reported honestly as zero with `ignoreFilesParsed: 0`. TS also deduplicates roots by
//!     `realpath` identity (`files/identity.ts`); this port deduplicates displayed paths.
//!   * `grep` compiles the pattern with the `regex` crate. JS-only constructs
//!     (look-around, backreferences) therefore REFUSE with the regex crate's message
//!     instead of matching, and Rust-only inline flags (`(?i)`) therefore MATCH where JS
//!     reports "Invalid group"; every other documented pattern behaves the same.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use regex::Regex;
use serde_json::{Value, json};

/// `FILE_READ_DEFAULT_MAX_LINES` (`read.tool.ts:122`).
pub const READ_DEFAULT_MAX_LINES: i64 = 200;
/// `FILE_READ_MAX_LINES` (`read.tool.ts:123`).
pub const READ_MAX_LINES: i64 = 5_000;
/// `FILE_READ_CONTENT_BUDGET` (`read.tool.ts:124`).
pub const READ_CONTENT_BUDGET: usize = 64 * 1024;
/// `FILE_READ_MAX_TARGETS` (`read.tool.ts:127`).
pub const READ_MAX_TARGETS: usize = 100;
/// `FILE_LIST_DEFAULT_LIMIT` / `FILE_LIST_MAX_LIMIT` (`fileList.tool.ts`).
pub const LIST_DEFAULT_LIMIT: usize = 200;
/// The hard page ceiling for `fileList` (`fileList.tool.ts`).
pub const LIST_MAX_LIMIT: usize = 1_000;
/// `FILE_LIST_CONTENT_BUDGET` (`fileList.tool.ts:109`).
pub const LIST_CONTENT_BUDGET: usize = 64 * 1024;
/// `FILE_GLOB_CONTENT_BUDGET` (`glob.tool.ts:124`) and the grep budget (`grep.tool.ts:186`).
pub const SEARCH_CONTENT_BUDGET: usize = 64 * 1024;
/// `FILE_GLOB_DEFAULT_LIMIT` / the clamped maximum (`glob.ts`).
pub const SEARCH_DEFAULT_MAX_RESULTS: i64 = 100;
/// The hard result cap `glob`/`grep` clamp `maxResults` to (`glob.ts`).
pub const SEARCH_MAX_RESULTS: i64 = 1_000;
/// `MAX_SEARCH_FILE_BYTES` (`glob.ts:81`): a larger file is reported, never searched.
pub const MAX_SEARCH_FILE_BYTES: u64 = 1024 * 1024;
/// `BINARY_SNIFF_BYTES` (`glob.ts:515`): NULs inside this window make a file binary.
pub const BINARY_SNIFF_BYTES: usize = 8 * 1024;
/// `FILE_GREP_MATCH_TEXT_BYTES` (`grep.tool.ts:182`): one snippet's byte cap.
pub const GREP_MATCH_TEXT_BYTES: usize = 1_024;
/// `SKIPPED_DIRECTORIES` (`glob.ts`): the two names a source walk never descends into.
pub const SKIPPED_DIRECTORIES: [&str; 2] = [".git", "node_modules"];
/// `ALLOWED_MATCH_FLAGS` (`bounded-regex.ts:131`): the JavaScript flag letters `grep`
/// accepts, in the order its error names them.
pub const ALLOWED_MATCH_FLAGS: &str = "dgimsuyv";

/// The tool-prefixed pair every file tool states for a path string it cannot use
/// (`read.tool.ts:335-342` `validatePath`): an empty path and a NUL byte are caller
/// mistakes the tool names better than the OS does, and naming the TOOL is what lets a
/// model that mistyped `grep`'s arguments fix `grep` rather than `glob`.
fn validate_path_text(tool: &str, raw: &str) -> Result<(), String> {
    if raw.is_empty() {
        return Err(format!("{tool} paths must be non-empty strings."));
    }

    // NUL cannot exist in a real path and would only confuse the filesystem.
    if raw.contains('\0') {
        return Err(format!("{tool} paths must not contain NUL characters."));
    }

    Ok(())
}

/// `normalizeStringList` (`search.ts:535-568`): one string-array argument, validated with
/// the property NAME and the TOOL in every sentence.
///
/// An explicitly EMPTY array is refused rather than defaulted: `{"patterns": []}` reads as
/// "match nothing", which would make a model conclude a file does not exist. Omitting the
/// property is how a caller asks for the documented default.
fn string_list_checked(
    tool: &str,
    arguments: &Value,
    name: &str,
    fallback: &[&str],
) -> Result<Vec<String>, String> {
    let value = match super::result::argument_value(arguments, name) {
        None | Some(Value::Null) => {
            return Ok(fallback.iter().map(|entry| (*entry).to_owned()).collect());
        }
        Some(value) => value,
    };

    let Some(entries) = value.as_array() else {
        return Err(format!("{tool} {name} must be an array of strings."));
    };

    if entries.is_empty() {
        return Err(format!(
            "{tool} received an empty {name} array. Omit {name} to use the default ({}).",
            fallback
                .iter()
                .map(|entry| format!("\"{entry}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    entries
        .iter()
        .map(|entry| {
            let Some(text) = entry.as_str() else {
                return Err(format!("{tool} {name} must be non-empty strings."));
            };

            if text.is_empty() {
                return Err(format!("{tool} {name} must be non-empty strings."));
            }

            // NUL cannot exist in a real path or pattern and would only confuse the
            // filesystem or the matcher.
            if text.contains('\0') {
                return Err(format!("{tool} {name} must not contain NUL characters."));
            }

            Ok(text.to_owned())
        })
        .collect()
}

/// Resolves one tool path against the run's working directory.
///
/// Relative paths resolve under `cwd`; absolute paths are used as given, because the TS
/// tools are unsandboxed and a workspace agent legitimately reads `/etc/hosts` or
/// `C:\tmp\x`. A NUL byte cannot exist in a real path, and an empty path is refused
/// with the tool-agnostic wording the entries share.
pub(crate) fn resolve_target(cwd: &Path, raw: &str) -> Result<PathBuf, String> {
    if raw.is_empty() {
        return Err("paths must be non-empty strings.".to_owned());
    }

    if raw.contains('\0') {
        return Err("paths must not contain NUL characters.".to_owned());
    }

    let candidate = Path::new(raw);

    Ok(if candidate.is_absolute() {
        normalize_absolute(candidate)
    } else {
        normalize_absolute(&cwd.join(candidate))
    })
}

/// Lexically normalizes `.`/`..`/separators without touching the disk, so a path that
/// does not exist yet still has one canonical spelling (`path.resolve` semantics).
fn normalize_absolute(path: &Path) -> PathBuf {
    let mut stack: Vec<Component<'_>> = Vec::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // A leading `..` under a root stays: `/../x` resolves to `/x`.
                if matches!(stack.last(), Some(Component::Normal(_))) {
                    stack.pop();
                }
            }
            other => stack.push(other),
        }
    }

    stack.iter().collect()
}

/// The posix-style spelling every search/list result uses (`glob.ts:117`).
fn to_posix(value: &str) -> String {
    value.replace('\\', "/")
}

/// The display form of a resolved path: posix for RELATIVE output (roots are echoed the
/// way the caller typed them) and platform-native for absolute paths, which is what
/// `path.resolve()` yields on the host.
fn display_path(path: &Path) -> String {
    path.display().to_string()
}

/// Node's `path.join(root, relative)` with the result posix-ified, which is exactly how
/// `search.ts:394` and `fileList.tool.ts:428` build a displayed path: `join` COLLAPSES a
/// `.` segment (`./src` becomes `src`), drops an empty segment (a root written `src/`
/// never yields `src//a.ts`), and leaves `.` prefix-free.
///
/// A posix/Windows root that is absolute keeps its leading separator, which naive
/// segment filtering would lose.
fn join_root(root: &str, relative: &str) -> String {
    let root = to_posix(root);
    let absolute = root.starts_with('/');

    let mut segments: Vec<&str> = root
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect();

    segments.extend(
        relative
            .split('/')
            .filter(|segment| !segment.is_empty() && *segment != "."),
    );

    let joined = segments.join("/");

    if absolute {
        format!("/{joined}")
    } else {
        joined
    }
}

/// TS's `rootErrorMessage` (`search.ts:268`): one sentence per way a root fails, each
/// naming the fix, because the raw OS string (`ENOTDIR: not a directory, scandir`) never
/// tells a model what to type instead. A root whose stat fails is a HARD error for
/// `glob`/`grep`, not a skipped root - a typo must not read as "nothing matched".
fn root_error(tool: &str, raw: &str, cwd: &Path, error: &std::io::Error) -> String {
    match error.kind() {
        ErrorKind::NotFound => format!(
            "{tool} path \"{raw}\" does not exist. Roots are resolved against the working directory ({}); check the spelling, or list the parent with fileList to see what is there.",
            to_posix(&cwd.display().to_string())
        ),
        // A component of the root is a file: the caller meant the file itself or its
        // parent directory, and only one of the two sentences says which.
        ErrorKind::NotADirectory => format!(
            "{tool} path \"{raw}\" is not a directory: a component of it is a file. Pass the file itself as the root to search just that file, or the directory that contains it."
        ),
        ErrorKind::PermissionDenied => format!(
            "{tool} path \"{raw}\" could not be read (EACCES: permission denied). Narrow the search to a directory this process can open."
        ),
        // The fallback keeps the OS text, because the Rust port has no portable
        // `code` string the way Node's errors do (`EPERM`/`EROFS`).
        _ => format!("{tool} path \"{raw}\" could not be walked ({error})."),
    }
}

// ---------------------------------------------------------------------------
// read
// ---------------------------------------------------------------------------

/// One normalised read request (`read.tool.ts:304` `normalizeTargets`).
struct ReadTarget {
    path: PathBuf,
    /// The path exactly as requested, which is what the ENOENT message names.
    requested: String,
    start_line: i64,
    start_column: usize,
    max_lines: i64,
}

/// `read`: `paths` is an array of strings or `{path, startLine, startColumn, maxLines}`
/// objects. Every target is validated BEFORE any file is opened (TS's rule, so a typo in
/// the last entry cannot produce a misleading partial answer).
///
/// # Errors
///
/// The TS refusal sentences, verbatim where TS states one.
pub(crate) fn read(arguments: &Value, cwd: &Path) -> Result<Value, String> {
    // `paths` is the documented wrapper; a bare array (the direct-call shape the
    // single-property rule allows) is accepted too.
    let paths: Vec<Value> = match super::result::argument_value(arguments, "paths") {
        Some(Value::Array(entries)) => entries.clone(),
        Some(other) => {
            return Err(format!(
                "read expects an array of paths (strings or {{ path, startLine, startColumn, maxLines }} objects), e.g. [\"package.json\"]; received {}.",
                super::result::type_of(other)
            ));
        }
        None => arguments.as_array().cloned().unwrap_or_default(),
    };

    if paths.is_empty() {
        return Err("read received an empty paths array; pass at least one file path.".to_owned());
    }

    if paths.len() > READ_MAX_TARGETS {
        return Err(format!(
            "read accepts at most {READ_MAX_TARGETS} paths per call; received {}. Split the batch.",
            paths.len()
        ));
    }

    let mut targets: Vec<ReadTarget> = Vec::with_capacity(paths.len());

    for target in &paths {
        match target {
            Value::String(raw) => {
                validate_path_text("read", raw)?;

                targets.push(ReadTarget {
                    path: resolve_target(cwd, raw)?,
                    requested: raw.clone(),
                    start_line: 1,
                    start_column: 0,
                    max_lines: READ_DEFAULT_MAX_LINES,
                });
            }
            Value::Object(map) => {
                let raw = map
                    .get("path")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .ok_or_else(|| "read paths must be non-empty strings.".to_owned())?;

                validate_path_text("read", raw)?;

                let integer = |key: &str,
                               default: i64,
                               minimum: i64,
                               maximum: i64|
                 -> Result<i64, String> {
                    match map.get(key) {
                        None | Some(Value::Null) => Ok(default),
                        Some(value) => {
                            let parsed = value.as_i64().ok_or_else(|| {
                                format!("read {key} must be an integer between {minimum} and {maximum}; received {value}.")
                            })?;

                            if parsed < minimum || parsed > maximum {
                                return Err(format!(
                                    "read {key} must be an integer between {minimum} and {maximum}; received {parsed}."
                                ));
                            }

                            Ok(parsed)
                        }
                    }
                };

                targets.push(ReadTarget {
                    path: resolve_target(cwd, raw)?,
                    requested: raw.to_owned(),
                    start_line: integer("startLine", 1, 1, i64::MAX)?,
                    start_column: usize::try_from(integer("startColumn", 0, 0, i64::MAX)?)
                        .unwrap_or(usize::MAX),
                    max_lines: integer("maxLines", READ_DEFAULT_MAX_LINES, 1, READ_MAX_LINES)?,
                });
            }
            _ => {
                return Err(
                    "read paths must be strings or { path, startLine, startColumn, maxLines } objects.".to_owned(),
                );
            }
        }
    }

    // PHASE 1 of TS's loop: a missing file becomes a per-entry error, everything else
    // fails the whole call, and only the readable files take part in the byte split.
    #[derive(Default)]
    struct Planned {
        target: Option<ReadTarget>,
        text: Option<String>,
        missing: bool,
    }

    let mut planned: Vec<Planned> = Vec::with_capacity(targets.len());

    for target in targets {
        match fs::metadata(&target.path) {
            Ok(metadata) if metadata.is_dir() => {
                return Err(format!(
                    "read cannot read \"{}\": it is a directory, not a file. Use fileList to list its entries, or glob to find files inside it.",
                    display_path(&target.path)
                ));
            }
            Ok(_) => match fs::read(&target.path) {
                Ok(bytes) => {
                    let text = decode_text(&bytes, &target.path)?;

                    planned.push(Planned {
                        target: Some(target),
                        text: Some(text),
                        missing: false,
                    });
                }
                Err(error) => return Err(read_io_error(&target.path, &error)),
            },
            Err(error) if error.kind() == ErrorKind::NotFound => {
                planned.push(Planned {
                    target: Some(target),
                    text: None,
                    missing: true,
                });
            }
            Err(error) => return Err(read_io_error(&target.path, &error)),
        }
    }

    let mut remaining_budget = READ_CONTENT_BUDGET;
    let mut files: Vec<Value> = Vec::with_capacity(planned.len());

    for (index, entry) in planned.iter().enumerate() {
        let target = entry
            .target
            .as_ref()
            .expect("a planned entry names its target");

        if entry.missing {
            files.push(json!({
                "path": display_path(&target.path),
                "content": "",
                "startLine": target.start_line,
                "endLine": target.start_line - 1,
                "truncated": false,
                "nextLine": Value::Null,
                "nextColumn": Value::Null,
                "bytes": 0,
                "error": {
                    "code": "ENOENT",
                    "message": format!(
                        "read could not find \"{}\": no such file or directory (ENOENT).",
                        target.requested
                    ),
                },
            }));

            continue;
        }

        // Fair share of what is left: one file may use the whole budget, two split it,
        // and a file that returns fewer bytes leaves the remainder to the rest
        // (`read.tool.ts` PHASE 2).
        let remaining_readable = planned[index..]
            .iter()
            .filter(|entry| !entry.missing)
            .count()
            .max(1);
        let allowance = remaining_budget / remaining_readable;

        let window = window_of(
            entry.text.as_deref().unwrap_or_default(),
            target.start_line,
            target.start_column,
            usize::try_from(target.max_lines).unwrap_or(usize::MAX),
            allowance,
        );

        remaining_budget = remaining_budget.saturating_sub(window.bytes);

        files.push(json!({
            "path": display_path(&target.path),
            "content": window.content,
            "startLine": window.start_line,
            "endLine": window.end_line,
            "truncated": window.truncated,
            "nextLine": window.next_line.map_or(Value::Null, |value| json!(value)),
            "nextColumn": window.next_column.map_or(Value::Null, |value| json!(value)),
            "bytes": window.bytes,
        }));
    }

    let bytes: usize = files
        .iter()
        .map(|entry| entry.get("bytes").and_then(Value::as_u64).unwrap_or(0) as usize)
        .sum();
    let truncated = files
        .iter()
        .any(|entry| entry.get("truncated").and_then(Value::as_bool) == Some(true));

    Ok(json!({
        "files": files,
        "truncated": truncated,
        "bytes": bytes,
        "contentBudget": READ_CONTENT_BUDGET,
    }))
}

/// One returned window (`read.tool.ts:159` `FileReadEntry`, minus the path/error).
struct Window {
    content: String,
    start_line: i64,
    end_line: i64,
    truncated: bool,
    next_line: Option<i64>,
    next_column: Option<usize>,
    bytes: usize,
}

/// Cuts one window out of a decoded file.
///
/// The rules the TypeScript scanner states (`read.tool.ts:367-382`) and this reproduces
/// on the whole text instead of a stream:
///   1. once `max_lines` lines are emitted, the NEXT character proves there is more
///      content, so the window is truncated (even when that character is a newline);
///   2. lines before `start_line` are skipped;
///   3. the first `start_column` BYTES of the first requested line are skipped, snapped
///      up to a character boundary (an offset is only ever produced at a boundary);
///   4. a character that would exceed the byte budget truncates at that exact character,
///      reporting `nextLine`/`nextColumn` for resuming inside a giant line;
///   5. only then is the character appended and counted.
fn window_of(
    text: &str,
    start_line: i64,
    start_column: usize,
    max_lines: usize,
    allowance: usize,
) -> Window {
    let lines: Vec<&str> = split_preserving_terminators(text);
    let first_index = usize::try_from(start_line - 1)
        .unwrap_or(usize::MAX)
        .min(lines.len());

    let mut content = String::new();
    let mut emitted = 0usize;
    let mut end_line = start_line - 1;
    let mut index = first_index;

    // Rule 3: the byte offset into the first requested line, snapped to a boundary.
    let mut column = if first_index < lines.len() {
        snap_to_boundary(
            lines[first_index],
            start_column.min(lines[first_index].len()),
        )
    } else {
        start_column
    };

    while index < lines.len() && emitted < max_lines {
        let line = lines[index];
        let line_number = i64::try_from(index + 1).unwrap_or(i64::MAX);

        let slice = if column > 0 {
            let tail = &line[snap_to_boundary(line, column.min(line.len()))..];
            column = 0;

            tail
        } else {
            line
        };

        let fits = content.len() + slice.len() <= allowance;

        if !fits {
            // Rule 4: cut this line at the byte budget and report how to resume, so a
            // single giant line can still be paged through without omission.
            let room = allowance.saturating_sub(content.len());
            let cut = snap_to_boundary(slice, room.min(slice.len()));
            let line_start = line.len() - slice.len();

            if cut > 0 {
                content.push_str(&slice[..cut]);
            }

            let bytes = content.len();

            return Window {
                content,
                start_line,
                end_line,
                truncated: true,
                next_line: Some(line_number),
                next_column: Some(line_start + cut),
                bytes,
            };
        }

        content.push_str(slice);
        end_line = line_number;
        emitted += 1;
        index += 1;
    }

    // Rule 1: the line limit closed the window and MORE content follows (another line,
    // or an unterminated remainder) - reaching EOF means not truncated.
    let truncated = index < lines.len();
    let next_line = if truncated {
        Some(i64::try_from(index + 1).unwrap_or(i64::MAX))
    } else {
        None
    };
    let bytes = content.len();

    Window {
        content,
        start_line,
        end_line,
        truncated,
        next_line,
        // A truncated window resumes at the start of the first line that was not
        // returned; a complete one reports no resume point at all.
        next_column: next_line.map(|_| 0),
        bytes,
    }
}

/// Splits text into lines that KEEP their terminators (so the returned window is the
/// exact recorded bytes, `read.tool.ts`'s scanner appends characters, never re-joins).
fn split_preserving_terminators(text: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = Vec::new();
    let mut start = 0;

    for (offset, character) in text.char_indices() {
        if character == '\n' {
            lines.push(&text[start..=offset]);
            start = offset + 1;
        }
    }

    if start < text.len() {
        lines.push(&text[start..]);
    }

    lines
}

/// The smallest byte index >= `target` that begins a character.
fn snap_to_boundary(line: &str, target: usize) -> usize {
    let mut position = target.min(line.len());

    while position < line.len() && !line.is_char_boundary(position) {
        position += 1;
    }

    position
}

/// Decodes bytes as UTF-8 text, refusing a NUL or invalid sequence with the shared
/// binary sentence (`read.tool.ts:260` `binaryError`).
fn decode_text(bytes: &[u8], path: &Path) -> Result<String, String> {
    if bytes.contains(&0) || std::str::from_utf8(bytes).is_err() {
        return Err(format!(
            "read refused \"{}\": the file contains binary data (NUL bytes or invalid UTF-8). read reads UTF-8 text only.",
            display_path(path)
        ));
    }

    Ok(String::from_utf8_lossy(bytes).into_owned())
}

/// Classifies a filesystem failure with the path named (`read.tool.ts:529-570`).
fn read_io_error(path: &Path, error: &std::io::Error) -> String {
    match error.kind() {
        ErrorKind::NotFound => format!(
            "read could not find \"{}\": no such file or directory (ENOENT).",
            display_path(path)
        ),
        ErrorKind::PermissionDenied => format!(
            "read was denied permission to read \"{}\": {}. This is a filesystem permission problem, not a missing path or a directory read.",
            display_path(path),
            error
        ),
        _ => format!("read failed to read \"{}\": {error}", display_path(path)),
    }
}

// ---------------------------------------------------------------------------
// write / edit
// ---------------------------------------------------------------------------

/// One requested write: `{path, content}`.
struct WriteRequest {
    path: PathBuf,
    content: String,
}

/// The comparison key for "is this the same file?" (`identity.ts` `fileIdentity`).
///
/// TS resolves the nearest EXISTING ancestor through `realpath` and folds case on
/// win32, so `a.txt`, `./a.txt` and `A.TXT` collide. A file that does not exist yet has
/// no real path to read, so its lexically normalized spelling is the key - which is
/// what `resolve_target` already produces - and on a case-insensitive platform the
/// spelling is folded the same way.
fn identity_key(path: &Path) -> String {
    let key = match fs::canonicalize(path) {
        Ok(real) => real.display().to_string(),
        Err(_) => path.display().to_string(),
    };

    // `\\?\` prefixes are an artifact of the Windows canonical form; the same file
    // reached through a relative path has no prefix, so the marker is stripped before
    // comparing.
    let key = key.strip_prefix(r"\\?\").unwrap_or(&key).to_owned();

    if cfg!(windows) {
        key.to_lowercase()
    } else {
        key
    }
}

fn write_requests(
    arguments: &Value,
    cwd: &Path,
    tool_name: &str,
) -> Result<Vec<WriteRequest>, String> {
    // The wrapper is `{files: [...]}`; a bare array is the direct-call shape.
    let entries: Vec<Value> = match super::result::argument_value(arguments, "files") {
        Some(Value::Array(entries)) => entries.clone(),
        Some(other) => {
            return Err(format!(
                "{tool_name} expects an array of {{ path, content }} entries, e.g. [{{\"path\":\"a.txt\",\"content\":\"hi\"}}] (received {}).",
                super::result::type_of(other)
            ));
        }
        None if arguments.is_array() => arguments.as_array().cloned().unwrap_or_default(),
        None => {
            return Err(format!(
                "{tool_name} expects an array of {{ path, content }} entries, e.g. [{{\"path\":\"a.txt\",\"content\":\"hi\"}}]."
            ));
        }
    };

    if entries.is_empty() {
        return Err(format!(
            "{tool_name} received an empty files array; pass at least one entry."
        ));
    }

    let mut requests: Vec<WriteRequest> = Vec::with_capacity(entries.len());
    // Uniqueness is by FILE IDENTITY, checked before anything is written: a batch that
    // names one file twice would otherwise let the last entry silently win
    // (`write.tool.ts:158-170`).
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for entry in entries {
        let Some(map) = entry.as_object() else {
            return Err(format!(
                "{tool_name} files must be objects with \"path\" and \"content\" strings."
            ));
        };

        let raw = match map.get("path") {
            Some(Value::String(text)) if !text.is_empty() => text.clone(),
            _ => {
                return Err(format!(
                    "{tool_name} files require a non-empty \"path\" string."
                ));
            }
        };

        // NUL cannot exist in a real path and would only confuse the filesystem.
        if raw.contains('\0') {
            return Err(format!(
                "{tool_name} paths must not contain NUL characters."
            ));
        }

        // An empty string is a legitimate "truncate the file" request, so only the
        // type is checked here.
        let Some(content) = map.get("content").and_then(Value::as_str) else {
            return Err(format!("{tool_name} files require a \"content\" string."));
        };

        let target = resolve_target(cwd, &raw)?;

        // A NUL byte is the harness's own binary marker (`read` rejects it), so writing
        // one creates a file this package cannot read back. Preflight, before any write.
        if content.contains('\0') {
            return Err(format!(
                "{tool_name} content must not contain NUL characters ({target}); the file would be unreadable by read, which rejects binary content.",
                target = display_path(&target)
            ));
        }

        let identity = identity_key(&target);

        if !seen.insert(identity) {
            return Err(format!(
                "{tool_name} received the same target path twice ({}); write each file once per call.",
                display_path(&target)
            ));
        }

        requests.push(WriteRequest {
            path: target,
            content: content.to_owned(),
        });
    }

    Ok(requests)
}

/// `write`: replaces each file's contents, creating missing parent directories
/// (`write.tool.ts:237-258`). Returns one entry per requested file, in order.
pub(crate) fn write(arguments: &Value, cwd: &Path) -> Result<Value, String> {
    let requests = write_requests(arguments, cwd, "write")?;
    let mut written: Vec<Value> = Vec::with_capacity(requests.len());

    for request in requests {
        // Agents write into directories that do not exist yet far more often than not, so the
        // parent is created rather than complained about (`write.tool.ts:239-241`).
        if let Some(parent) = request.path.parent()
            && let Err(error) = fs::create_dir_all(parent)
        {
            return Err(partial_write_message(&written, &request, &error));
        }

        if let Err(error) = fs::write(&request.path, request.content.as_bytes()) {
            return Err(partial_write_message(&written, &request, &error));
        }

        written.push(json!({
            "path": display_path(&request.path),
            "bytes": request.content.len(),
        }));
    }

    Ok(Value::Array(written))
}

/// The honest partial-success sentence (`write.tool.ts:247-252`).
fn partial_write_message(
    written: &[Value],
    request: &WriteRequest,
    error: &std::io::Error,
) -> String {
    let completed = if written.is_empty() {
        "none".to_owned()
    } else {
        written
            .iter()
            .filter_map(|entry| entry.get("path").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(", ")
    };

    format!(
        "write failed on {}: {error} Writes already completed: {completed}. No rollback was performed.",
        display_path(&request.path)
    )
}

/// One requested edit (`edit.tool.ts`'s `{path, oldString, newString, replaceAll?}`).
struct EditRequest {
    /// The resolved absolute target, which is what every error sentence names
    /// (`edit.tool.ts:488`, `:496`).
    path: PathBuf,
    /// Canonical file identity, so two spellings of one existing file group together and
    /// are written once (`edit.tool.ts:433-437`).
    identity: String,
    old_string: String,
    new_string: String,
    replace_all: bool,
}

/// Byte offsets of every occurrence of `needle` in `haystack` (`edit.tool.ts:152`
/// `findOccurrences`). A byte scan is enough: every offset produced here indexes the
/// same `String`, and Rust slicing is byte-addressed.
fn find_occurrences(haystack: &str, needle: &str) -> Vec<(usize, usize)> {
    let mut found: Vec<(usize, usize)> = Vec::new();

    if needle.is_empty() {
        return found;
    }

    let mut cursor = 0usize;

    while let Some(offset) = haystack[cursor..].find(needle) {
        let start = cursor + offset;

        found.push((start, start + needle.len()));
        cursor = start + needle.len();
    }

    found
}

/// The dominant line ending in `text`, or `fallback` when neither wins
/// (`edit.tool.ts:217` `lineEnding`).
fn line_ending(text: &str, fallback: &str) -> String {
    let crlf = text.matches("\r\n").count();
    let lf = text.matches('\n').count() - crlf;

    if crlf == lf {
        return fallback.to_owned();
    }

    if crlf > lf { "\r\n" } else { "\n" }.to_owned()
}

/// Re-joins the replacement's lines with the endings of the matched region
/// (`edit.tool.ts:268` `withRegionEndings`).
///
/// STATED SIMPLIFICATION: TS aligns replacement lines to region lines through a
/// longest-common-subsequence pass, so an unchanged line keeps ITS OWN ending and a line
/// changed in place borrows the ending of the line it replaces. This port applies the
/// region's dominant ending to every joined line, which is byte-identical whenever the
/// matched region uses one ending (the normal case) and only differs inside a file that
/// mixes CRLF and LF within one matched hunk.
fn with_region_endings(region: &str, replacement: &str, fallback: &str) -> String {
    let ending = line_ending(region, fallback);
    let mut lines = replacement.split('\n').peekable();
    let mut text = String::new();

    while let Some(line) = lines.next() {
        text.push_str(line);

        // The final line carries no ending: the region's trailing ending (if any) lives
        // outside the match, so re-adding it would double a newline.
        if lines.peek().is_some() {
            text.push_str(&ending);
        }
    }

    text
}

/// 1-based line numbers of each match start, so the ambiguity error can say WHERE the
/// candidates are instead of only how many (`edit.tool.ts:287` `lineNumbers`).
fn match_line_numbers(source: &str, matches: &[(usize, usize)]) -> Vec<usize> {
    matches
        .iter()
        .map(|(start, _)| source[..*start].matches('\n').count() + 1)
        .collect()
}

/// Number of sorted positions strictly before `offset` (`edit.tool.ts:291` `countBefore`).
fn count_before(positions: &[usize], offset: usize) -> usize {
    positions.partition_point(|position| *position < offset)
}

/// Tier 1 of TS's matcher (`edit.tool.ts:309` `matchView`): CRLF is folded to LF for
/// MATCHING ONLY and the view offsets are mapped back to source offsets, so a model that
/// writes `\n` in a CRLF file still gets its edit and the file's endings stay put.
///
/// Returns the matches in SOURCE coordinates, the `\n`-view replacement text, and the
/// file's fallback ending. The typographic and trailing-whitespace tiers are not ported
/// (module header), so no fallback `match` tier is ever reported.
fn match_view(
    source: &str,
    old_string: &str,
    new_string: &str,
) -> (Vec<(usize, usize)>, String, String) {
    let view = source.replace("\r\n", "\n");
    // View offset of every CRLF, in ascending order: adding the count strictly before an
    // offset maps view coordinates back to source coordinates (the fold is one-to-one).
    let mut removed: Vec<usize> = Vec::new();
    let mut cursor = 0usize;
    let mut index = 0usize;

    while let Some(offset) = source[cursor..].find("\r\n") {
        let position = cursor + offset;

        removed.push(position - index);
        index += 1;
        cursor = position + 2;
    }

    let search = old_string.replace("\r\n", "\n");
    let replacement = new_string.replace("\r\n", "\n");

    let matches = find_occurrences(&view, &search)
        .into_iter()
        .map(|(start, end)| {
            (
                start + count_before(&removed, start),
                end + count_before(&removed, end),
            )
        })
        .collect();

    // The region decides the joined endings, with the file's dominant ending as the
    // fallback (`matchView`'s `replace` closure).
    (matches, replacement, line_ending(source, "\n"))
}

/// `edit`: exact-match replacement against the LF view, computed for EVERY entry before
/// anything is written, so a failure on the last entry leaves the first file untouched
/// (`edit.tool.ts:455-534` `prepareEdits`).
pub(crate) fn edit(arguments: &Value, cwd: &Path) -> Result<Value, String> {
    let entries: Vec<Value> = match super::result::argument_value(arguments, "edits") {
        Some(Value::Array(entries)) => entries.clone(),
        Some(other) => {
            return Err(format!(
                "edit expects an array of {{ path, oldString, newString }} entries, e.g. [{{\"path\":\"a.txt\",\"oldString\":\"a\",\"newString\":\"b\"}}] (received {}).",
                super::result::type_of(other)
            ));
        }
        None if arguments.is_array() => arguments.as_array().cloned().unwrap_or_default(),
        None => {
            return Err(
                "edit expects an array of { path, oldString, newString } entries, e.g. \
                 [{\"path\":\"a.txt\",\"oldString\":\"a\",\"newString\":\"b\"}]"
                    .to_owned(),
            );
        }
    };

    if entries.is_empty() {
        return Err("edit received an empty edits array; pass at least one edit.".to_owned());
    }

    // SHAPE FIRST: every entry is validated before any file is opened, so a typo in the
    // last entry cannot produce a misleading partial answer (`normalizeEdits`).
    let mut requests: Vec<EditRequest> = Vec::with_capacity(entries.len());

    for entry in &entries {
        let Some(map) = entry.as_object() else {
            return Err(
                "edit edits must be objects with \"path\", \"oldString\", and \"newString\" strings.".to_owned(),
            );
        };

        let raw = match map.get("path") {
            Some(Value::String(text)) if !text.is_empty() => text.clone(),
            _ => return Err("edit edits require a non-empty \"path\" string.".to_owned()),
        };

        if raw.contains('\0') {
            return Err("edit paths must not contain NUL characters.".to_owned());
        }

        // An empty oldString matches everywhere (including between every pair of
        // characters), so it is never a meaningful edit.
        let Some(old_string) = map.get("oldString").and_then(Value::as_str) else {
            return Err("edit requires a non-empty oldString.".to_owned());
        };

        if old_string.is_empty() {
            return Err("edit requires a non-empty oldString.".to_owned());
        }

        let Some(new_string) = map.get("newString").and_then(Value::as_str) else {
            return Err("edit requires a \"newString\" string.".to_owned());
        };

        // Rejecting a no-op keeps "the edit applied" an honest claim.
        if old_string == new_string {
            return Err(
                "edit did not change the file: oldString and newString are identical.".to_owned(),
            );
        }

        let replace_all = match map.get("replaceAll") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => return Err("edit replaceAll must be a boolean when provided.".to_owned()),
        };

        let path = resolve_target(cwd, &raw)?;
        let identity = identity_key(&path);

        requests.push(EditRequest {
            path,
            identity,
            old_string: old_string.to_owned(),
            new_string: new_string.to_owned(),
            replace_all,
        });
    }

    // Current text per file IDENTITY, filled on first use, so several entries naming one
    // file see each other's output and the file is written once (`edit.tool.ts:460-471`).
    let mut texts: Vec<(String, PathBuf, String)> = Vec::new();
    let mut prepared: Vec<(usize, usize)> = Vec::with_capacity(requests.len());

    for request in &requests {
        let index = match texts
            .iter()
            .position(|(identity, _, _)| *identity == request.identity)
        {
            Some(index) => index,
            None => {
                // A missing, unreadable, or non-file target fails here, before anything is
                // written. Editing is for existing files; write creates.
                let metadata = fs::metadata(&request.path).map_err(|error| match error.kind() {
                    ErrorKind::NotFound => format!(
                        "edit could not read \"{}\": no such file or directory. edit changes an existing file; use write to create one.",
                        display_path(&request.path)
                    ),
                    _ => format!(
                        "edit could not read \"{}\": {error}",
                        display_path(&request.path)
                    ),
                })?;

                if metadata.is_dir() {
                    return Err(format!(
                        "edit cannot edit \"{}\": it is a directory, not a file.",
                        display_path(&request.path)
                    ));
                }

                let text = fs::read(&request.path).map_err(|error| {
                    format!(
                        "edit could not read \"{}\": {error}",
                        display_path(&request.path)
                    )
                })?;

                let text = decode_text(&text, &request.path)?;

                texts.push((request.identity.clone(), request.path.clone(), text));
                texts.len() - 1
            }
        };

        let content = texts[index].2.clone();
        let target = display_path(&texts[index].1);

        // oldString and newString that differ ONLY in carriage returns ask to change the
        // file's line endings themselves, so that edit applies to the exact bytes instead
        // of the LF view (`edit.tool.ts:475-484`).
        let endings_only =
            request.old_string.replace('\r', "") == request.new_string.replace('\r', "");

        let (matches, replacement, file_ending) = if endings_only {
            (
                find_occurrences(&content, &request.old_string),
                request.new_string.clone(),
                line_ending(&content, "\n"),
            )
        } else {
            match_view(&content, &request.old_string, &request.new_string)
        };

        if matches.is_empty() {
            return Err(format!(
                "edit found no match for oldString in {target}. Read the file first and copy the text exactly, including indentation."
            ));
        }

        if matches.len() > 1 && !request.replace_all {
            // Naming the lines is what makes the failure actionable: a model can copy
            // more surrounding text from a line it can actually locate.
            return Err(format!(
                "edit found {} matches for oldString in {target} (lines {}). Include more surrounding text to make it unique, or pass replaceAll: true to change every match.",
                matches.len(),
                match_line_numbers(&content, &matches)
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        // Splice right-to-left so earlier match offsets stay valid through every
        // replacement. Without replaceAll only the single unique match is applied.
        let chosen: &[(usize, usize)] = if request.replace_all {
            &matches
        } else {
            &matches[..1]
        };

        let mut updated = content.clone();

        for (start, end) in chosen.iter().rev() {
            // TS's `replace` closure, branched the same way: the exact-bytes branch hands
            // back `newString` VERBATIM (that edit is precisely about the carriage
            // returns), while the LF-view branch re-joins the replacement with the matched
            // region's endings so the file's conventions survive.
            let joined = if endings_only {
                replacement.clone()
            } else {
                with_region_endings(&updated[*start..*end], &replacement, &file_ending)
            };

            updated.replace_range(*start..*end, &joined);
        }

        // A fallback match against text that already equals newString would make
        // "the edit applied" dishonest, so the edit is rejected (`edit.tool.ts:514-518`).
        if updated == content {
            return Err(format!(
                "edit did not change {target}: the replacement text is identical to the text it matched. Line endings follow the file; to change them, pass oldString and newString that differ only in carriage returns."
            ));
        }

        texts[index].2 = updated;
        prepared.push((index, chosen.len()));
    }

    // One write per distinct file, in first-appearance order; a failure names what has
    // already landed, with no claim of a transaction (`edit.tool.ts:622-639`).
    let mut sizes: Vec<usize> = Vec::with_capacity(texts.len());
    let mut written: Vec<String> = Vec::with_capacity(texts.len());

    for (_, path, text) in &texts {
        if let Err(error) = fs::write(path, text.as_bytes()) {
            let completed = if written.is_empty() {
                "none".to_owned()
            } else {
                written.join(", ")
            };

            return Err(format!(
                "edit failed on {}: {error} Edits already applied: {completed}. No rollback was performed.",
                display_path(path)
            ));
        }

        written.push(display_path(path));
        sizes.push(text.len());
    }

    // One entry per requested edit, in request order: entries that share a file each
    // report their own replacement count against that file's FINAL size. `match` would
    // name a fallback tier; none is ported, so the key is always absent.
    let result: Vec<Value> = prepared
        .iter()
        .map(|(file_index, count)| {
            json!({
                "path": display_path(&texts[*file_index].1),
                "replacements": count,
                "bytes": sizes[*file_index],
            })
        })
        .collect();

    Ok(Value::Array(result))
}

// ---------------------------------------------------------------------------
// fileList
// ---------------------------------------------------------------------------

/// `normalizePageNumber` (`fileList.tool.ts:220-233`): a missing value means the default,
/// a present value must be a NUMBER (silently treating the string `"200"` as absent would
/// restart page 1 forever), and it is then floored and clamped rather than rejected.
fn page_number(arguments: &Value, name: &str, fallback: usize) -> Result<usize, String> {
    let message = || format!("fileList {name} must be a number when provided.");

    match super::result::argument_value(arguments, name) {
        None | Some(Value::Null) => Ok(fallback),
        Some(Value::Number(value)) => {
            let Some(number) = value.as_f64() else {
                return Err(message());
            };

            if !number.is_finite() {
                return Err(message());
            }

            // A negative offset clamps to 0: asking for "before the start" means the
            // first page, which is what `nextOffset` arithmetic can produce.
            Ok(if number < 0.0 {
                0
            } else {
                usize::try_from(number.floor() as i64).unwrap_or(fallback)
            })
        }
        Some(_) => Err(message()),
    }
}

/// `fileList`: one page of the flat, prefixed, sorted listing of one or more directories
/// (`fileList.tool.ts:132-148` and the ORDERING notes).
pub(crate) fn file_list(arguments: &Value, cwd: &Path) -> Result<Value, String> {
    // `normalizeRequest` (`fileList.tool.ts:240-288`). The legacy string/array form is
    // still accepted because it is the documented direct-call shape; the object form is
    // STRICT about its property names, because `{paths: ["src"], offest: 200}` would
    // otherwise return page 1 forever while the caller believed it was paging.
    if arguments.is_number() || arguments.is_boolean() {
        // A number or a boolean used to fall through the property reads and mean "list
        // the working directory"; that is a wrong answer, so it is rejected.
        return Err(
            "fileList expects an array of directories or an object with \"paths\", \"offset\", and \"limit\"."
                .to_owned(),
        );
    }

    let legacy = arguments.is_array() || arguments.is_string();

    if !legacy && let Some(map) = arguments.as_object() {
        for key in map.keys() {
            if !matches!(key.as_str(), "paths" | "offset" | "limit") {
                return Err(
                    "fileList accepts only \"paths\", \"offset\", and \"limit\" in the object form."
                        .to_owned(),
                );
            }
        }
    }

    let raw_paths: Option<&Value> = if legacy {
        Some(arguments)
    } else {
        super::result::argument_value(arguments, "paths")
    };

    let directories: Vec<String> = match raw_paths {
        // `undefined`/`null`/an empty object all mean "no preference": the working
        // directory (`normalizeDirectories`).
        None | Some(Value::Null) => vec![".".to_owned()],
        Some(Value::String(single)) => vec![single.clone()],
        Some(Value::Array(entries)) => {
            // An explicit empty array is refused rather than answered with no entries: a
            // model that sends `{"paths": []}` would read an empty list as "the directory
            // is empty", which is a wrong answer.
            if entries.is_empty() {
                return Err(
                    "fileList received an empty paths array. Omit paths to list the working directory."
                        .to_owned(),
                );
            }

            entries
                .iter()
                .map(|entry| match entry.as_str() {
                    Some(text) if !text.is_empty() => Ok(text.to_owned()),
                    _ => Err("fileList paths must be non-empty strings.".to_owned()),
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        Some(other) => {
            return Err(format!(
                "fileList paths must be non-empty strings; received {}.",
                super::result::type_of(other)
            ));
        }
    };

    for directory in &directories {
        // NUL cannot exist in a real path and would only confuse the filesystem.
        if directory.contains('\0') {
            return Err("fileList paths must not contain NUL characters.".to_owned());
        }
    }

    // The legacy form cannot express paging (`fileList.tool.ts:245-251`).
    let (offset, requested_limit) = if legacy {
        (0, LIST_DEFAULT_LIMIT)
    } else {
        (
            page_number(arguments, "offset", 0)?,
            page_number(arguments, "limit", LIST_DEFAULT_LIMIT)?,
        )
    };

    // A limit is a knob, not a contract: oversized requests are clamped, never rejected
    // (`fileList.tool.ts:284-286`).
    let limit = requested_limit.clamp(1, LIST_MAX_LIMIT);

    let mut entries: Vec<String> = Vec::new();

    for directory in &directories {
        let resolved = resolve_target(cwd, directory)?;
        let mut names: Vec<String> = Vec::new();

        // A missing or unreadable directory fails the whole call - the listing must not
        // read as complete when it is not (`fileList.tool.ts` header).
        let read = fs::read_dir(&resolved).map_err(|error| {
            format!(
                "fileList could not list \"{}\": {error}",
                display_path(&resolved)
            )
        })?;

        for entry in read {
            let entry = entry.map_err(|error| {
                format!(
                    "fileList could not list \"{}\": {error}",
                    display_path(&resolved)
                )
            })?;

            let name = entry.file_name().to_string_lossy().into_owned();
            let is_directory = entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false);

            // Directories carry a trailing "/" so a model can tell them from files
            // without a second call; the marker is added AFTER the join, because
            // `join` itself collapses a trailing separator (`fileList.tool.ts:71`).
            names.push(if is_directory {
                format!("{name}/")
            } else {
                name
            });
        }

        // Sorted by code point (`compareNames`), never locale-aware.
        names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        for name in names {
            // `join(directory, name)`: "." contributes no prefix, "./src" collapses to
            // "src/", and a trailing separator on the root never doubles
            // (`fileList.tool.ts:428`).
            let marked = name.ends_with('/');
            let joined = join_root(directory, name.trim_end_matches('/'));

            entries.push(if marked { format!("{joined}/") } else { joined });
        }
    }

    // The window is taken over the WHOLE flat list, so `offset` counts every directory
    // and the sequence never depends on the page size (`paginateEntries`).
    let total = entries.len();
    let mut page: Vec<String> = Vec::new();
    let mut bytes = 0usize;

    for entry in entries.into_iter().skip(offset).take(limit) {
        let size = entry.len();

        // A single entry bigger than the whole budget can never be returned at ANY
        // offset: stopping before it would report `truncated: true` with a `nextOffset`
        // that never advances, an infinite loop the caller cannot escape.
        if size > LIST_CONTENT_BUDGET {
            return Err(format!(
                "fileList entry at offset {} is {size} UTF-8 bytes, larger than the {LIST_CONTENT_BUDGET}-byte page budget, so it can never be returned. List a deeper directory so the entry prefix is shorter, or shorten the entry's name.",
                offset + page.len()
            ));
        }

        // Stop BEFORE the entry that would cross the cap: an entry is never split, and
        // `nextOffset` below resumes at exactly this entry.
        if bytes + size > LIST_CONTENT_BUDGET {
            break;
        }

        bytes += size;
        page.push(entry);
    }

    let consumed = offset + page.len();
    let truncated = consumed < total;

    Ok(json!({
        "entries": page,
        "truncated": truncated,
        "nextOffset": if truncated { json!(consumed) } else { Value::Null },
        "offset": offset,
        "limit": limit,
        "bytes": bytes,
        "contentBudget": LIST_CONTENT_BUDGET,
    }))
}

// ---------------------------------------------------------------------------
// Walking + glob matching
// ---------------------------------------------------------------------------

/// One walked file: its path relative to the root (`a/b.ts`) and its absolute path.
pub(crate) struct WalkedFile {
    pub relative: String,
    pub absolute: PathBuf,
}

/// What a walk did not yield (`glob.tool.ts:143` `FileGlobExcluded`).
///
/// `ignored_files`/`ignore_files_parsed`/`ignore_rules_truncated` are honest zeros: this
/// port reads no `.gitignore` (stated in the module header).
pub(crate) fn excluded_value(unreadable_directories: usize, symlinks: usize) -> Value {
    json!({
        "ignoredFiles": 0,
        "prunedDirectories": 0,
        "skippedDirectories": 0,
        "unreadableDirectories": unreadable_directories,
        "symlinks": symlinks,
        "ignoreFilesParsed": 0,
        "ignoreRulesTruncated": false,
    })
}

/// The outcome of reading one candidate file (`glob.ts:521` `SearchFileRead`).
pub(crate) enum FileRead {
    Text(String),
    TooLarge,
    Binary,
    Unreadable,
}

/// Reads a file for search, applying the size cap, the binary sniff, and the "unreadable
/// is reported, never guessed about" rule.
pub(crate) fn read_for_search(path: &Path) -> FileRead {
    let Ok(metadata) = fs::metadata(path) else {
        return FileRead::Unreadable;
    };

    if !metadata.is_file() {
        return FileRead::Unreadable;
    }

    if metadata.len() > MAX_SEARCH_FILE_BYTES {
        return FileRead::TooLarge;
    }

    let Ok(bytes) = fs::read(path) else {
        return FileRead::Unreadable;
    };

    let sniff = bytes.len().min(BINARY_SNIFF_BYTES);

    if bytes[..sniff].contains(&0) {
        return FileRead::Binary;
    }

    match std::str::from_utf8(&bytes) {
        Ok(text) => FileRead::Text(text.to_owned()),
        Err(_) => FileRead::Binary,
    }
}

/// Walks one root and yields every regular file under it, as posix paths RELATIVE to the
/// root, in a deterministic order (`glob.ts:393` yields walk order; directories are
/// visited in sorted-name order, files are collected as encountered).
///
/// A root that is itself a FILE yields that one file with its own name as the relative
/// path, which is the documented "a FILE is listed on its own" behaviour.
pub(crate) fn walk(
    root: &Path,
    cap: usize,
    unreadable: &mut usize,
    symlinks: &mut usize,
) -> Vec<WalkedFile> {
    let mut found: Vec<WalkedFile> = Vec::new();

    match fs::symlink_metadata(root) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                *symlinks += 1;
            }

            if metadata.is_file() {
                let name = root
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_default();

                found.push(WalkedFile {
                    relative: to_posix(&name),
                    absolute: root.to_path_buf(),
                });

                return found;
            }

            if !metadata.is_dir() {
                return found;
            }
        }
        Err(_) => {
            *unreadable += 1;
            return found;
        }
    }

    walk_directory(root, root, &mut found, cap, unreadable, symlinks);

    found
}

/// The recursive half of `walk`, depth-first in sorted-name order.
fn walk_directory(
    root: &Path,
    directory: &Path,
    found: &mut Vec<WalkedFile>,
    cap: usize,
    unreadable: &mut usize,
    symlinks: &mut usize,
) {
    let Ok(entries) = fs::read_dir(directory) else {
        *unreadable += 1;
        return;
    };

    let mut names: Vec<(String, PathBuf, bool)> = Vec::new();

    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        let is_dir = entry
            .file_type()
            .map(|kind| kind.is_dir())
            .unwrap_or_else(|_| path.is_dir());

        names.push((name, path, is_dir));
    }

    names.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));

    for (name, path, is_dir) in names {
        if found.len() >= cap {
            return;
        }

        if is_dir {
            // `.git` and `node_modules` never descend (`SKIPPED_DIRECTORIES`).
            if SKIPPED_DIRECTORIES.contains(&name.as_str()) {
                continue;
            }

            walk_directory(root, &path, found, cap, unreadable, symlinks);
            continue;
        }

        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() => {
                if metadata.file_type().is_symlink() {
                    *symlinks += 1;
                }
            }
            _ => continue,
        }

        let relative = path
            .strip_prefix(root)
            .map(|rest| to_posix(&rest.display().to_string()))
            .unwrap_or_else(|_| to_posix(&name));

        found.push(WalkedFile {
            relative,
            absolute: path,
        });
    }
}

/// Compiles one posix glob into a regex anchored at both ends.
///
/// Supported syntax (the schema's documented language): `**` crossing directories,
/// `*`/`?` within one segment, `{a,b}` alternatives, `[abc]` classes, and `!` as a
/// NEGATION PREFIX handled by the caller.
fn glob_regex(pattern: &str) -> Result<Regex, String> {
    let mut source = String::from("^");
    let characters: Vec<char> = pattern.chars().collect();
    let mut index = 0;

    while index < characters.len() {
        let character = characters[index];

        match character {
            '*' => {
                let is_double = index + 1 < characters.len() && characters[index + 1] == '*';

                if is_double {
                    index += 2;

                    // `**/` matches zero or more whole segments; a bare `**` matches
                    // anything, including across separators.
                    if index < characters.len() && characters[index] == '/' {
                        index += 1;
                        source.push_str("(?:[^/]*/)*");
                    } else {
                        source.push_str(".*");
                    }
                } else {
                    index += 1;
                    source.push_str("[^/]*");
                }
            }
            '?' => {
                index += 1;
                source.push_str("[^/]");
            }
            '{' => {
                // One level of alternatives: `{a,b}` becomes `(?:a|b)`.
                let Some(close) = characters[index..].iter().position(|c| *c == '}') else {
                    source.push_str("\\{");
                    index += 1;
                    continue;
                };

                let body: String = characters[index + 1..index + close].iter().collect();
                let parts: Vec<&str> = body.split(',').collect();

                if parts.len() < 2 {
                    source.push_str("\\{");
                    source.push_str(&regex::escape(&body));
                    source.push('}');
                } else {
                    let alternatives: Vec<String> = parts.into_iter().map(regex::escape).collect();

                    source.push_str(&format!("(?:{})", alternatives.join("|")));
                }

                index += close + 1;
            }
            '[' => {
                let Some(close) = characters[index + 1..].iter().position(|c| *c == ']') else {
                    source.push_str("\\[");
                    index += 1;
                    continue;
                };

                let body: String = characters[index + 1..index + 1 + close].iter().collect();
                source.push_str(&format!("[{}]", body.replace(']', "\\]")));
                index += close + 2;
            }
            other => {
                source.push_str(&regex::escape(&other.to_string()));
                index += 1;
            }
        }
    }

    source.push('$');

    Regex::new(&source)
        .map_err(|error| format!("glob could not compile pattern \"{pattern}\": {error}"))
}

/// One compiled pattern plus its negation flag.
pub(crate) struct CompiledPattern {
    pub regex: Regex,
    pub negated: bool,
    /// The pattern text as the request stated it (echoed back in `patterns`).
    pub source: String,
}

/// Compiles every pattern, keeping the `!` prefix as the exclude marker
/// (`glob.tool.ts:188-200`).
pub(crate) fn compile_patterns(patterns: &[String]) -> Result<Vec<CompiledPattern>, String> {
    let mut compiled: Vec<CompiledPattern> = Vec::with_capacity(patterns.len());

    for pattern in patterns {
        let negated = pattern.starts_with('!');
        let body = if negated {
            &pattern[1..]
        } else {
            pattern.as_str()
        };
        let normalized = body.replace('\\', "/");

        compiled.push(CompiledPattern {
            regex: glob_regex(&normalized)?,
            negated,
            source: pattern.clone(),
        });
    }

    Ok(compiled)
}

/// True when a relative path survives the pattern set: OR of the positive patterns, MINUS
/// any `!` pattern (`glob.tool.ts` "OR-ed together ... a leading ! excludes").
pub(crate) fn matches_patterns(compiled: &[CompiledPattern], relative: &str) -> bool {
    let mut included = false;

    for pattern in compiled.iter().filter(|pattern| !pattern.negated) {
        if pattern.regex.is_match(relative) {
            included = true;
            break;
        }
    }

    if !included {
        return false;
    }

    !compiled
        .iter()
        .filter(|pattern| pattern.negated)
        .any(|pattern| pattern.regex.is_match(relative))
}

// ---------------------------------------------------------------------------
// glob
// ---------------------------------------------------------------------------

/// `glob`: every file under the requested roots whose root-relative path matches the
/// pattern set, paged by `offset`/`maxResults` (`glob.tool.ts:155` `FileGlobResult`).
pub(crate) fn glob(arguments: &Value, cwd: &Path) -> Result<Value, String> {
    // The shape error fires only for a NON-object argument; an object with unknown keys is
    // tolerated the way TS's property reads tolerate them.
    if !(arguments.is_object() || arguments.is_null()) {
        return Err(
            "glob expects an object with optional \"patterns\", \"paths\", \"maxResults\", and \"offset\" properties, e.g. {\"patterns\":[\"**/*.ts\"]}."
                .to_owned(),
        );
    }

    let patterns = string_list_checked("glob", arguments, "patterns", &["**/*"])?;
    let roots = string_list_checked("glob", arguments, "paths", &["."])?;
    let compiled = compile_patterns(&patterns)?;
    let max_results = super::result::bounded_integer(
        arguments,
        "maxResults",
        SEARCH_DEFAULT_MAX_RESULTS,
        1,
        SEARCH_MAX_RESULTS,
    )
    .unwrap_or(SEARCH_DEFAULT_MAX_RESULTS);
    let limit = usize::try_from(max_results.clamp(1, SEARCH_MAX_RESULTS)).unwrap_or(1);
    let offset = usize::try_from(
        super::result::bounded_integer(arguments, "offset", 0, 0, i64::MAX).unwrap_or(0),
    )
    .unwrap_or(0);

    let mut unreadable = 0usize;
    let mut symlinks = 0usize;
    let mut files: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut walked_roots = 0usize;
    let mut in_scope = 0usize;

    for root in roots {
        let resolved = resolve_target(cwd, &root)?;

        // One stat per root decides the three cases `search.ts:324-337` separates: a FILE
        // (search just it), a directory (walk it), and anything that cannot be opened (a
        // sentence naming the fix). A missing root therefore ERRORS rather than reporting
        // an empty listing, which is how a typo is told apart from a clean tree.
        let metadata =
            fs::metadata(&resolved).map_err(|error| root_error("glob", &root, cwd, &error))?;
        let root_is_file = metadata.is_file();

        walked_roots += 1;
        let walked = if root_is_file {
            vec![WalkedFile {
                relative: resolved
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                absolute: resolved.clone(),
            }]
        } else {
            walk(&resolved, usize::MAX, &mut unreadable, &mut symlinks)
        };

        for file in walked {
            in_scope += 1;

            // A FILE root is presented exactly as the caller named it (`search.ts:352`),
            // so `paths: ["src/a.ts"]` answers about `src/a.ts` instead of doubling the
            // name. The pattern still filters, tested against BOTH the bare name and that
            // spelling, because a file root has no root-relative path.
            let displayed = if root_is_file {
                to_posix(&root)
            } else {
                join_root(&root, &file.relative)
            };

            if !matches_patterns(&compiled, &file.relative)
                && !(root_is_file && matches_patterns(&compiled, &displayed))
            {
                continue;
            }

            if seen.insert(displayed.clone()) {
                files.push(displayed);
            }
        }
    }

    let page: Vec<String> = files.iter().skip(offset).take(limit + 1).cloned().collect();
    let truncated = page.len() > limit;
    let returned: Vec<String> = page.into_iter().take(limit).collect();

    // The byte budget is a second, independent stop: an over-budget page keeps the
    // entries that fit and reports the reason it stopped.
    let mut kept: Vec<String> = Vec::with_capacity(returned.len());
    let mut charged = 0usize;
    let mut stopped_on_budget = false;

    for entry in returned {
        if charged + entry.len() > SEARCH_CONTENT_BUDGET && !kept.is_empty() {
            stopped_on_budget = true;
            break;
        }

        charged += entry.len();
        kept.push(entry);
    }

    let budget_truncated = stopped_on_budget || charged > SEARCH_CONTENT_BUDGET;
    let consumed = offset + kept.len();
    let truncated = truncated || budget_truncated;

    let note = build_note(in_scope, kept.len(), unreadable, truncated);

    let mut result = json!({
        "files": kept,
        "truncated": truncated,
        "offset": offset,
        "limit": limit,
        "nextOffset": if truncated { json!(consumed) } else { Value::Null },
        "bytes": charged,
        "contentBudget": SEARCH_CONTENT_BUDGET,
        "stopReason": if truncated { "resultCap" } else { "complete" },
        "partial": unreadable > 0,
        "searched": { "inScope": in_scope, "roots": walked_roots },
        "excluded": excluded_value(unreadable, symlinks),
        "patterns": compiled.iter().map(|pattern| pattern.source.clone()).collect::<Vec<_>>(),
    });

    // `note` rides ONLY when something was withheld for a reason the caller can act on
    // (`glob.tool.ts:172`), so an all-clean listing states nothing extra.
    if let Some(text) = note {
        result["note"] = json!(text);
    }

    Ok(result)
}

/// The sentence an incomplete listing cannot produce on its own (`glob.tool.ts:196`
/// `buildNote`, condensed to the clauses this port can honestly state).
fn build_note(
    in_scope: usize,
    returned: usize,
    unreadable: usize,
    truncated: bool,
) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();

    if truncated {
        parts.push(format!(
            "{in_scope} files were in scope and only {returned} were returned; page with `offset`."
        ));
    }

    if unreadable > 0 {
        parts.push(format!("the walk could not read {unreadable} directories."));
    }

    if parts.is_empty() {
        return None;
    }

    Some(parts.join(" "))
}

// ---------------------------------------------------------------------------
// grep
// ---------------------------------------------------------------------------

/// `grep`: one pattern over the files a glob selects, in `content`/`files`/`count` mode
/// (`grep.tool.ts:273` `FileGrepResult`).
pub(crate) fn grep(arguments: &Value, cwd: &Path) -> Result<Value, String> {
    let pattern = super::result::required_text(arguments, "pattern", "grep")?;
    let flags = super::result::optional_text(arguments, "flags").unwrap_or_default();
    let output =
        super::result::optional_text(arguments, "output").unwrap_or_else(|| "content".to_owned());

    if !matches!(output.as_str(), "content" | "files" | "count") {
        return Err(format!(
            "grep output must be one of content, files, count; received \"{output}\"."
        ));
    }

    let context = super::result::bounded_integer(arguments, "context", 0, 0, 10)
        .unwrap_or(0)
        .max(0) as usize;
    let max_results = super::result::bounded_integer(
        arguments,
        "maxResults",
        SEARCH_DEFAULT_MAX_RESULTS,
        1,
        SEARCH_MAX_RESULTS,
    )
    .unwrap_or(SEARCH_DEFAULT_MAX_RESULTS);
    let limit = usize::try_from(max_results.clamp(1, SEARCH_MAX_RESULTS)).unwrap_or(1);
    let offset = usize::try_from(
        super::result::bounded_integer(arguments, "offset", 0, 0, i64::MAX).unwrap_or(0),
    )
    .unwrap_or(0);

    let compiled = build_regex(&pattern, &flags)?;
    let selection = string_list_checked("grep", arguments, "patterns", &["**/*"])?;
    let compiled_patterns = compile_patterns(&selection)?;
    let roots = string_list_checked("grep", arguments, "paths", &["."])?;

    let mut unreadable_directories = 0usize;
    let mut symlinks = 0usize;
    let mut scanned = 0usize;
    // The three skip classes `grep.tool.ts:264` `FileGrepSkipped` counts.
    let mut skipped_too_large = 0usize;
    let mut skipped_binary = 0usize;
    let mut skipped_unreadable = 0usize;
    let mut units: Vec<Value> = Vec::new();
    let mut in_scope = 0usize;
    let mut walked_roots = 0usize;
    let mut partial = false;

    for root in roots {
        let resolved = resolve_target(cwd, &root)?;

        // Same root boundary as `glob`: one stat, three honest outcomes (`search.ts:324`).
        let metadata =
            fs::metadata(&resolved).map_err(|error| root_error("grep", &root, cwd, &error))?;
        let root_is_file = metadata.is_file();

        walked_roots += 1;

        let files = if root_is_file {
            vec![WalkedFile {
                relative: resolved
                    .file_name()
                    .map(|value| to_posix(&value.to_string_lossy()))
                    .unwrap_or_default(),
                absolute: resolved.clone(),
            }]
        } else {
            walk(
                &resolved,
                usize::MAX,
                &mut unreadable_directories,
                &mut symlinks,
            )
        };

        for file in files {
            in_scope += 1;

            // A file root is displayed as typed and matched on name OR spelling
            // (`search.ts:341-363`); a directory root on its root-relative path.
            let displayed = if root_is_file {
                to_posix(&root)
            } else {
                join_root(&root, &file.relative)
            };

            if !matches_patterns(&compiled_patterns, &file.relative)
                && !(root_is_file && matches_patterns(&compiled_patterns, &displayed))
            {
                continue;
            }

            match read_for_search(&file.absolute) {
                FileRead::TooLarge => {
                    skipped_too_large += 1;
                    partial = true;
                }
                FileRead::Binary => {
                    skipped_binary += 1;
                }
                FileRead::Unreadable => {
                    skipped_unreadable += 1;
                    partial = true;
                }
                FileRead::Text(text) => {
                    scanned += 1;
                    collect_matches(
                        &compiled,
                        &displayed,
                        &text,
                        output.as_str(),
                        context,
                        &mut units,
                    );
                }
            }
        }
    }

    let total = units.len();
    let truncated = offset + limit < total;
    let page: Vec<Value> = units.into_iter().skip(offset).take(limit).collect();
    let bytes: usize = page.iter().map(|entry| entry.to_string().len()).sum();
    let consumed = offset + page.len();

    Ok(json!({
        "output": output,
        "matches": page,
        "truncated": truncated,
        "offset": offset,
        "limit": limit,
        "nextOffset": if truncated { json!(consumed) } else { Value::Null },
        "bytes": bytes,
        "contentBudget": SEARCH_CONTENT_BUDGET,
        "partial": partial,
        "stopReason": if truncated { "resultCap" } else { "complete" },
        "searched": { "inScope": in_scope, "scanned": scanned, "roots": walked_roots },
        "skipped": {
            "tooLarge": skipped_too_large,
            "binary": skipped_binary,
            "unreadable": skipped_unreadable,
        },
        "excluded": excluded_value(unreadable_directories, symlinks),
    }))
}

/// Builds the matcher from the pattern plus the JS-style flag letters (`i`, `m`, `s`).
fn build_regex(pattern: &str, flags: &str) -> Result<Regex, String> {
    let mut builder = regex::RegexBuilder::new(pattern);

    for flag in flags.chars() {
        if flag == 'i' {
            builder.case_insensitive(true);
        } else if flag == 'm' {
            builder.multi_line(true);
        } else if flag == 's' {
            builder.dot_matches_new_line(true);
        } else if matches!(flag, 'd' | 'g' | 'u' | 'v' | 'y') {
            // `d` (match indices), `u`/`v` (unicode modes), `g` and `y` change no
            // first-match semantics for a per-line test, so they are accepted and
            // ignored - which is what they do in TS's matcher loop too.
        } else {
            // An unknown flag letter is a caller mistake worth naming with the exact
            // allowed set, because `new RegExp` alone blames "Invalid flags" without
            // naming the argument (`bounded-regex.ts:191-201`).
            return Err(format!(
                "grep flags accepts only the JavaScript flag letters \"{ALLOWED_MATCH_FLAGS}\" (\"i\" for case-insensitive is the usual one); got \"{flag}\"."
            ));
        }
    }

    builder
        .build()
        .map_err(|error| format!("grep could not compile pattern \"{pattern}\": {error}"))
}

/// Appends this file's units (lines, or one file entry) to the result list.
fn collect_matches(
    pattern: &Regex,
    displayed: &str,
    text: &str,
    output: &str,
    context: usize,
    units: &mut Vec<Value>,
) {
    let lines = split_lines(text);

    match output {
        "files" => {
            if lines.iter().any(|line| pattern.is_match(line)) {
                units.push(json!({ "path": displayed, "matchCount": Value::Null }));
            }
        }
        "count" => {
            let count = lines.iter().filter(|line| pattern.is_match(line)).count();

            if count > 0 {
                units.push(json!({ "path": displayed, "matchCount": count }));
            }
        }
        _ => {
            for (index, line) in lines.iter().enumerate() {
                if !pattern.is_match(line) {
                    continue;
                }

                let (snippet, text_truncated) = bound_snippet(line);
                let charged = snippet.len();

                let mut entry = json!({
                    "path": displayed,
                    "line": index + 1,
                    "text": snippet,
                    "textTruncated": text_truncated,
                    "bytes": charged,
                });

                if context > 0 {
                    let low = index.saturating_sub(context);
                    let before: Vec<&str> = lines[low..index].to_vec();
                    let after_end = (index + 1 + context).min(lines.len());
                    let after: Vec<&str> = lines[index + 1..after_end].to_vec();

                    entry["contextBefore"] = json!(before);
                    entry["contextAfter"] = json!(after);
                }

                units.push(entry);
            }
        }
    }
}

/// Splits text into lines WITHOUT terminators (`grep.tool.ts` reports a line "without
/// its terminator").
fn split_lines(text: &str) -> Vec<&str> {
    text.split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect()
}

/// Cuts one snippet to `FILE_GREP_MATCH_TEXT_BYTES` and says whether it cut.
fn bound_snippet(line: &str) -> (String, bool) {
    if line.len() <= GREP_MATCH_TEXT_BYTES {
        return (line.to_owned(), false);
    }

    let mut cut = GREP_MATCH_TEXT_BYTES;

    while cut > 0 && !line.is_char_boundary(cut) {
        cut -= 1;
    }

    (line[..cut].to_owned(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_window_reports_its_lines_and_its_resume_point() {
        let text = "one\ntwo\nthree\n";
        let window = window_of(text, 2, 0, 1, 4_096);

        assert_eq!(window.content, "two\n");
        assert_eq!(window.start_line, 2);
        assert_eq!(window.end_line, 2);
        assert!(window.truncated);
        assert_eq!(window.next_line, Some(3));
        assert_eq!(window.next_column, Some(0));
        assert_eq!(window.bytes, 4);
    }

    #[test]
    fn a_byte_allowance_cuts_inside_a_line_and_reports_the_offset() {
        let text = "abcdefgh\n";
        let window = window_of(text, 1, 0, 10, 5);

        assert_eq!(window.content, "abcde");
        assert!(window.truncated);
        assert_eq!(window.next_line, Some(1));
        assert_eq!(window.next_column, Some(5));
    }

    #[test]
    fn an_exact_file_is_not_truncated() {
        let text = "one\ntwo\n";
        let window = window_of(text, 1, 0, 2, 4_096);

        assert_eq!(window.content, "one\ntwo\n");
        assert!(!window.truncated);
        assert_eq!(window.next_line, None);
        assert_eq!(window.next_column, None);
    }

    #[test]
    fn a_glob_pattern_matches_paths_posix_style() {
        let compiled = compile_patterns(&["**/*.ts".to_owned(), "!**/*.test.ts".to_owned()])
            .expect("compiles");

        assert!(matches_patterns(&compiled, "src/a.ts"));
        assert!(matches_patterns(&compiled, "a.ts"));
        assert!(!matches_patterns(&compiled, "src/a.test.ts"));
        assert!(!matches_patterns(&compiled, "src/a.tsx"));
    }

    #[test]
    fn brace_alternatives_expand_through_the_matcher() {
        let compiled = compile_patterns(&["**/*.{ts,tsx}".to_owned()]).expect("compiles");

        assert!(matches_patterns(&compiled, "src/a.ts"));
        assert!(matches_patterns(&compiled, "src/a.tsx"));
        assert!(!matches_patterns(&compiled, "src/a.js"));
    }

    #[test]
    fn a_nul_in_the_head_window_is_binary() {
        assert!(matches!(read_for_search_of(b"a\0b"), FileRead::Binary));
        assert!(matches!(read_for_search_of(b"plain"), FileRead::Text(_)));
    }

    /// Reads a temporary file so `read_for_search` can be exercised on real bytes.
    fn read_for_search_of(bytes: &[u8]) -> FileRead {
        let directory = std::env::temp_dir().join(format!("t4-sniff-{}", uuid::Uuid::new_v4()));
        let _ = fs::create_dir_all(&directory);
        let path = directory.join("sniff.bin");
        let _ = fs::write(&path, bytes);
        let outcome = read_for_search(&path);

        let _ = fs::remove_dir_all(&directory);
        outcome
    }
}
