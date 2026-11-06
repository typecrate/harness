//! Integration tests for the tool runtime: the catalogue shape and every executable tool.
//!
//! WHAT THIS FILE PINS
//! -------------------
//!   * the `/tools` document (row count, groups, order, availability, the identified read's
//!     schema + execution gate) - the metadata the HTTP stage forwards verbatim;
//!   * each implemented tool's happy path AND its refusal sentences, against real files under a
//!     disposable fixture directory;
//!   * the three answer classes: executed-success (content + panels), executed-failure, and
//!     never-executed (a rejected call reports NO duration);
//!   * the model-facing result budget, end to end through a tool that overflows it.
//!
//! FIXTURES live in the repo-root `Temp/typecrate-harness/t4/<pid>-<test>/` and are removed on
//! `Drop`, per the repo scratch-space rule (`AGENTS.md`). Nothing here touches the network: the
//! two network tools (`fetch`, `search`) are covered in `tests/transport.rs`-style local
//! listeners, and `download` needs one too, so this file exercises the catalogue row instead.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use typecrate_harness::contract::{AttachmentKind, ToolAvailabilityState, ToolVisibility};
use typecrate_harness::ports::{
    ActionHost, TaskHost, ToolExecCtx, ToolExecOutcome, ToolPolicy, ToolRuntime,
};
use typecrate_harness::tools::{Options, ToolRegistry, catalogue, result};

/// Unique per process AND per test, so a parallel `cargo test` never shares a directory.
static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// One disposable fixture directory, removed when the test ends.
struct Fixture {
    directory: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = typecrate_harness::package_root::repo_root()
            .join("Temp")
            .join("typecrate-harness")
            .join("t4")
            .join(format!("{}-{sequence}-{name}", std::process::id()));

        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("a fixture directory is creatable");

        Self { directory }
    }

    /// Writes one file, creating parent directories.
    fn write(&self, relative: &str, content: &str) -> PathBuf {
        let path = self.directory.join(relative);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("a parent directory is creatable");
        }

        std::fs::write(&path, content).expect("a fixture file is writable");
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// A keyless registry: the state a harness server starts in when no provider credential is set.
fn registry() -> std::sync::Arc<ToolRegistry> {
    ToolRegistry::with_options(Options {
        web_keys: Default::default(),
        fetch_enabled: true,
    })
}

/// Runs one call and returns the outcome, with the fixture's directory as `cwd`.
async fn call(runtime: &ToolRegistry, name: &str, arguments: Value, cwd: &Path) -> ToolExecOutcome {
    runtime
        .execute(ToolExecCtx {
            name: name.to_owned(),
            arguments,
            call_id: format!("call-{name}"),
            session_id: Some("session-1".to_owned()),
            cwd: cwd.to_path_buf(),
            signal: CancellationToken::new(),
            policy: ToolPolicy::default(),
            action_host: None,
            task_host: None,
        })
        .await
}

/// The same, with a task host wired in (the `task` pass-through).
async fn call_with_task(
    runtime: &ToolRegistry,
    name: &str,
    arguments: Value,
    cwd: &Path,
    task_host: std::sync::Arc<dyn TaskHost>,
) -> ToolExecOutcome {
    runtime
        .execute(ToolExecCtx {
            name: name.to_owned(),
            arguments,
            call_id: format!("call-{name}"),
            session_id: Some("session-1".to_owned()),
            cwd: cwd.to_path_buf(),
            signal: CancellationToken::new(),
            policy: ToolPolicy::default(),
            action_host: None,
            task_host: Some(task_host),
        })
        .await
}

/// The same, with an action host wired in (the `actionRequest` round trip).
async fn call_with_action(
    runtime: &ToolRegistry,
    name: &str,
    arguments: Value,
    cwd: &Path,
    action_host: std::sync::Arc<dyn ActionHost>,
) -> ToolExecOutcome {
    runtime
        .execute(ToolExecCtx {
            name: name.to_owned(),
            arguments,
            call_id: format!("call-{name}"),
            session_id: Some("session-1".to_owned()),
            cwd: cwd.to_path_buf(),
            signal: CancellationToken::new(),
            policy: ToolPolicy::default(),
            action_host: Some(action_host),
            task_host: None,
        })
        .await
}

/// A task host that echoes its arguments, so the pass-through is observable.
struct EchoTaskHost;

impl TaskHost for EchoTaskHost {
    fn execute(&self, arguments: Value) -> Result<Value, String> {
        Ok(json!({ "echo": arguments, "tasks": [] }))
    }
}

/// A task host that refuses, which must become a FAILED call (with a measured span).
struct RefusingTaskHost;

impl TaskHost for RefusingTaskHost {
    fn execute(&self, _arguments: Value) -> Result<Value, String> {
        Err("the task board is full".to_owned())
    }
}

/// An action host that answers with the recorded choice.
struct AnsweringActionHost;

impl ActionHost for AnsweringActionHost {
    fn request(
        &self,
        request: Value,
    ) -> futures_util::future::BoxFuture<'static, Result<Value, String>> {
        Box::pin(async move {
            Ok(json!({
                "requestId": request.get("question").and_then(Value::as_str).unwrap_or(""),
                "choice": { "kind": "option", "optionId": "b" },
                "source": "owner",
            }))
        })
    }
}

// ---------------------------------------------------------------------------
// The catalogue
// ---------------------------------------------------------------------------

#[test]
fn the_catalogue_lists_every_group_in_the_published_order() {
    let runtime = registry();
    let rows = runtime.summaries();
    let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();

    assert_eq!(
        rows.len(),
        31,
        "every LISTED runtime, preset, harness and web row: the 35 generated rows minus the four \
         the manifest marks `listed: false` (`tool-manifest.ts:1002, 1046, 1692, 1811`)"
    );
    assert_eq!(names.len(), 31);
    assert_eq!(rows[0].name, "toolRegistry");
    assert_eq!(names[0], "toolRegistry");

    // T7b: the four unlisted runtime rows are absent from the listing.
    for withheld in ["toolCancel", "threadSend", "threadRead", "threadList"] {
        assert!(
            !names.contains(&withheld),
            "`{withheld}` is `listed: false` in the manifest, so the server does not advertise it"
        );
    }

    // Group boundaries: core rows, then presets, then app tools, then the web pair.
    let group_of = |name: &str| {
        rows.iter()
            .find(|row| row.name == name)
            .map(|row| row.group.clone())
            .unwrap_or_default()
    };

    assert_eq!(group_of("threadFork"), "Core tools");
    assert_eq!(group_of("syntax"), "Package presets");
    assert_eq!(group_of("timeNow"), "Harness tools");
    assert_eq!(group_of("search"), "Web search");
    assert_eq!(group_of("fetch"), "Web fetch");

    // Session binding follows the SERVER rule (`harnesses/core.ts:87`).
    assert!(
        rows.iter()
            .filter(|row| row.group == "Core tools")
            .all(|row| row.session_bound)
    );
    assert!(
        rows.iter()
            .filter(|row| matches!(row.group.as_str(), "Package presets" | "Harness tools"))
            .all(|row| !row.session_bound)
    );

    // The core rows carry the session hint and nothing else does.
    for row in rows.iter().filter(|row| row.group == "Core tools") {
        let availability = row
            .availability
            .clone()
            .expect("a core row states availability");

        assert_eq!(availability.state, ToolAvailabilityState::Available);
        assert_eq!(
            availability.hint.as_deref(),
            Some("Runs inside a session harness: send a sessionId with the call.")
        );
    }

    for row in rows.iter().filter(|row| row.group == "Package presets") {
        assert!(row.availability.is_none(), "{} states nothing", row.name);
    }

    // Visibilities match the manifest, including the demoted rows.
    let visibility = |name: &str| {
        rows.iter()
            .find(|row| row.name == name)
            .map(|row| row.visibility)
            .expect("a row")
    };

    assert_eq!(visibility("toolRequest"), ToolVisibility::Core);
    assert_eq!(visibility("task"), ToolVisibility::Core);
    assert_eq!(visibility("read"), ToolVisibility::Core);
    assert_eq!(visibility("fileList"), ToolVisibility::Searchable);
    assert_eq!(visibility("toolList"), ToolVisibility::Searchable);

    // Tags come from the same table the TS runtime registers.
    let tags = |name: &str| {
        rows.iter()
            .find(|row| row.name == name)
            .map(|row| row.tags.clone())
            .unwrap_or_default()
    };

    assert_eq!(tags("timeNow"), vec!["time", "clock", "date"]);
    assert_eq!(
        tags("grep"),
        vec!["files", "grep", "search", "content", "regex", "filesystem"]
    );
    assert_eq!(tags("shell"), vec!["shell", "command", "process"]);
}

#[test]
fn the_identified_read_carries_the_schema_and_the_execution_gate() {
    let runtime = registry();

    let edit = runtime.detail("edit").expect("a catalogued row");

    assert_eq!(
        edit.summary.description,
        catalogue::find("edit").expect("row").description
    );
    assert_eq!(
        edit.parameters
            .as_ref()
            .expect("a schema")
            .pointer("/properties/edits/items/required")
            .expect("the item's required list"),
        &json!(["path", "oldString", "newString"])
    );
    assert!(edit.execution.manual, "every live entry is manual-executed");
    assert_eq!(
        serde_json::to_value(edit.execution.scope).expect("the scope spelling"),
        json!("global")
    );

    let task = runtime.detail("task").expect("a catalogued row");

    assert_eq!(
        serde_json::to_value(task.execution.scope).expect("the scope spelling"),
        json!("session_required")
    );

    assert!(runtime.detail("notATool").is_none());
}

#[test]
fn a_keyless_host_reports_search_unconfigured_and_never_leaks_a_credential() {
    let keyless = registry();
    let search = keyless
        .summaries()
        .into_iter()
        .find(|row| row.name == "search")
        .expect("a search row");
    let availability = search
        .availability
        .clone()
        .expect("search always states it");

    assert_eq!(availability.state, ToolAvailabilityState::Unconfigured);
    assert_eq!(
        availability.environment,
        Some(vec![
            "EXA_API_KEY".to_owned(),
            "TAVILY_API_KEY".to_owned(),
            "LINKUP_API_KEY".to_owned()
        ])
    );
    assert!(
        availability
            .hint
            .as_deref()
            .unwrap_or_default()
            .starts_with("Web search is not configured on this server.")
    );

    let configured = ToolRegistry::with_options(Options {
        web_keys: typecrate_harness::tools::web::WebKeys {
            exa: Some("super-secret".to_owned()),
            tavily: None,
            linkup: None,
        },
        fetch_enabled: false,
    });
    let configured_search = configured
        .summaries()
        .into_iter()
        .find(|row| row.name == "search")
        .expect("a search row");

    assert_eq!(
        configured_search
            .availability
            .as_ref()
            .expect("availability")
            .state,
        ToolAvailabilityState::Available
    );

    let fetch = configured
        .summaries()
        .into_iter()
        .find(|row| row.name == "fetch")
        .expect("a fetch row");
    let fetch_availability = fetch.availability.clone().expect("fetch always states it");

    assert_eq!(
        fetch_availability.state,
        ToolAvailabilityState::Unconfigured
    );
    assert_eq!(
        fetch_availability.environment,
        Some(vec!["EXA_API_KEY".to_owned(), "LINKUP_API_KEY".to_owned()])
    );

    let document = serde_json::to_string(&configured.summaries()).expect("rows serialize");

    assert!(
        !document.contains("super-secret"),
        "names only, never values"
    );
}

#[test]
fn declarations_are_catalogue_ordered_and_follow_the_policy() {
    let runtime = registry();
    let policy = ToolPolicy {
        enabled: vec!["shell".to_owned()],
        declared: vec!["read".to_owned(), "grep".to_owned()],
        created: vec!["customTool".to_owned()],
        subagents: vec!["summarizer".to_owned()],
    };

    let declarations = runtime.declarations(&policy);
    let names: Vec<&str> = declarations.iter().map(|decl| decl.name.as_str()).collect();

    assert!(names.contains(&"shell"), "enabled");
    assert!(names.contains(&"read"), "declared");
    assert!(names.contains(&"toolRequest"), "always declared");
    assert!(names.contains(&"task"), "always declared");
    // T7b: `delegate` is catalogued-only in this build (no nested-run runtime), so a subagent
    // roster no longer buys the model a tool whose call can only fail. The same rule covers every
    // name in `MISSING_CAPABILITY` - including one the model ENABLED, which is the case that used
    // to reach a declaration through `policy.enabled`.
    assert!(
        !names.contains(&"delegate"),
        "a catalogued-only name is never declared"
    );
    assert!(
        !names.contains(&"customTool"),
        "a created tool is appended by the store"
    );
    assert!(
        !names.contains(&"fileList"),
        "searchable waits for toolRequest"
    );

    let enabled_but_unexecutable = ToolPolicy {
        enabled: vec!["profileManage".to_owned(), "threadRead".to_owned()],
        ..ToolPolicy::default()
    };
    let offered = runtime.declarations(&enabled_but_unexecutable);

    assert!(
        !offered
            .iter()
            .any(|decl| decl.name == "profileManage" || decl.name == "threadRead"),
        "an enabled tool this build cannot execute is offered to neither the model nor toolRequest"
    );
    assert!(
        offered.iter().any(|decl| decl.name == "toolRequest"),
        "the executable core pair is still offered, so the filter is per-row"
    );

    // Catalogue order, never policy order, so the same set sends the same bytes.
    let position = |name: &str| names.iter().position(|row| *row == name).expect("declared");

    assert!(position("read") < position("grep"));
    assert!(position("grep") < position("shell"));

    for name in names.iter() {
        let declaration = declarations
            .iter()
            .find(|decl| decl.name == *name)
            .expect("a declaration");

        assert!(!declaration.description.is_empty());
        assert!(
            declaration.parameters.is_some(),
            "{name} carries its schema"
        );
    }

    // No subagents: `delegate` is not offered.
    let alone = runtime.declarations(&ToolPolicy::default());

    assert!(!alone.iter().any(|decl| decl.name == "delegate"));
}

// ---------------------------------------------------------------------------
// The file tools
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_then_read_then_edit_round_trip_is_the_published_shape() {
    let runtime = registry();
    let fixture = Fixture::new("files-round-trip");

    let written = call(
        &runtime,
        "write",
        json!({ "files": [{ "path": "notes/a.txt", "content": "one\ntwo\n" }] }),
        &fixture.directory,
    )
    .await;

    assert!(written.ok, "{}", written.error.unwrap_or_default());

    let entries: Vec<Value> =
        serde_json::from_str(&written.content).expect("the write result is JSON");

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["bytes"], json!(8));
    assert!(
        entries[0]["path"]
            .as_str()
            .unwrap_or_default()
            .ends_with("a.txt"),
        "the entry names the file it wrote"
    );

    let read = call(
        &runtime,
        "read",
        json!({ "paths": ["notes/a.txt"] }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&read.content).expect("the read result is JSON");

    assert_eq!(document["files"][0]["content"], json!("one\ntwo\n"));
    assert_eq!(document["files"][0]["startLine"], json!(1));
    assert_eq!(document["files"][0]["endLine"], json!(2));
    assert_eq!(document["files"][0]["truncated"], json!(false));
    assert_eq!(document["files"][0]["nextLine"], Value::Null);
    assert_eq!(document["contentBudget"], json!(65_536));
    assert_eq!(document["bytes"], json!(8));

    let edited = call(
        &runtime,
        "edit",
        json!({ "edits": [{ "path": "notes/a.txt", "oldString": "two", "newString": "THREE" }] }),
        &fixture.directory,
    )
    .await;
    let edits: Value = serde_json::from_str(&edited.content).expect("the edit result is JSON");

    assert_eq!(edits[0]["replacements"], json!(1));
    // "one\nTHREE\n" is the file's final size: the edit changed 3 bytes of text, and `bytes`
    // reports the file, not the delta.
    assert_eq!(edits[0]["bytes"], json!(10));
    assert!(
        edited.content.contains("notes"),
        "the entry names the file it changed"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.directory.join("notes/a.txt")).expect("the file exists"),
        "one\nTHREE\n"
    );
    assert_eq!(
        edits[0]["path"]
            .as_str()
            .unwrap_or_default()
            .rsplit(['\\', '/'])
            .next(),
        Some("a.txt"),
        "the entry names the file it changed"
    );

    // A CRLF file is matched through the LF view (`edit.tool.ts:309` `matchView`): the
    // documented caller habit - write oldString with `\n` even for a CRLF file - works, and
    // the file's OWN endings survive the write.
    fixture.write("windows.txt", "first\r\nsecond\r\n");

    let crlf = call(
        &runtime,
        "edit",
        json!({ "edits": [{
            "path": "windows.txt",
            "oldString": "first\nsecond",
            "newString": "ONE\nTWO"
        }] }),
        &fixture.directory,
    )
    .await;
    let crlf_edits: Value = serde_json::from_str(&crlf.content).expect("the edit result is JSON");

    assert!(crlf.ok, "{}", crlf.error.unwrap_or_default());
    assert_eq!(crlf_edits[0]["replacements"], json!(1));
    assert_eq!(crlf_edits[0]["bytes"], json!(10));
    assert!(
        crlf_edits[0].get("match").is_none(),
        "the LF view is byte-exact, so no fallback tier is reported"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.directory.join("windows.txt")).expect("the file exists"),
        "ONE\r\nTWO\r\n",
        "the replacement took the matched region's CRLF endings"
    );

    // The same target twice in one batch is refused BEFORE anything is written, because the
    // second write would silently win (`write.tool.ts:158-168`). The two spellings differ
    // only by a `./` prefix, which is exactly the collision identity checking exists for.
    let twice = call(
        &runtime,
        "write",
        json!({ "files": [
            { "path": "twice.txt", "content": "a" },
            { "path": "./twice.txt", "content": "b" },
        ] }),
        &fixture.directory,
    )
    .await;
    let refusal = twice.error.clone().unwrap_or_default();

    assert!(!twice.ok);
    assert!(
        refusal.starts_with("Tool \"write\" failed: write received the same target path twice ("),
        "the sentence names the resolved target: {refusal}"
    );
    assert!(
        !fixture.directory.join("twice.txt").exists(),
        "a rejected batch writes nothing"
    );
}

#[tokio::test]
async fn read_reports_a_missing_file_per_entry_and_refuses_a_directory() {
    let runtime = registry();
    let fixture = Fixture::new("read-missing");

    fixture.write("here.txt", "present\n");

    let outcome = call(
        &runtime,
        "read",
        json!({ "paths": ["here.txt", "gone.txt"] }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&outcome.content).expect("the read result is JSON");

    assert!(
        outcome.ok,
        "a missing file is a per-entry answer, not a failure"
    );
    assert_eq!(document["files"][0]["error"], Value::Null);
    assert_eq!(document["files"][1]["error"]["code"], json!("ENOENT"));
    assert_eq!(
        document["files"][1]["error"]["message"],
        json!("read could not find \"gone.txt\": no such file or directory (ENOENT).")
    );
    assert_eq!(document["files"][1]["bytes"], json!(0));

    let directory = call(
        &runtime,
        "read",
        json!({ "paths": ["."] }),
        &fixture.directory,
    )
    .await;

    assert!(!directory.ok);
    assert!(
        directory
            .error
            .unwrap_or_default()
            .contains("it is a directory, not a file. Use fileList to list its entries")
    );

    let no_paths = call(&runtime, "read", json!({ "paths": [] }), &fixture.directory).await;

    assert!(!no_paths.ok);
    assert_eq!(
        no_paths.error.as_deref(),
        Some(
            "Tool \"read\" failed: read received an empty paths array; pass at least one file path."
        )
    );
}

#[tokio::test]
async fn a_read_window_pages_by_line_and_reports_where_to_resume() {
    let runtime = registry();
    let fixture = Fixture::new("read-window");
    let text = (1..=10)
        .map(|line| format!("line {line}\n"))
        .collect::<String>();

    fixture.write("ten.txt", &text);

    let outcome = call(
        &runtime,
        "read",
        json!({ "paths": [{ "path": "ten.txt", "startLine": 4, "maxLines": 2 }] }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&outcome.content).expect("JSON");
    let entry = &document["files"][0];

    assert_eq!(entry["content"], json!("line 4\nline 5\n"));
    assert_eq!(entry["startLine"], json!(4));
    assert_eq!(entry["endLine"], json!(5));
    assert_eq!(entry["truncated"], json!(true));
    assert_eq!(entry["nextLine"], json!(6));
    assert_eq!(entry["nextColumn"], json!(0));
}

#[tokio::test]
async fn an_ambiguous_edit_is_refused_and_replace_all_applies() {
    let runtime = registry();
    let fixture = Fixture::new("edit-ambiguous");

    fixture.write("dup.txt", "x\nx\n");

    let ambiguous = call(
        &runtime,
        "edit",
        json!({ "edits": [{ "path": "dup.txt", "oldString": "x", "newString": "y" }] }),
        &fixture.directory,
    )
    .await;

    assert!(!ambiguous.ok);
    assert!(
        ambiguous
            .error
            .unwrap_or_default()
            .contains("found 2 matches for oldString")
    );

    let all = call(
        &runtime,
        "edit",
        json!({ "edits": [{ "path": "dup.txt", "oldString": "x", "newString": "y", "replaceAll": true }] }),
        &fixture.directory,
    )
    .await;
    let edits: Value = serde_json::from_str(&all.content).expect("JSON");

    assert_eq!(edits[0]["replacements"], json!(2));
    assert_eq!(
        std::fs::read_to_string(fixture.directory.join("dup.txt")).expect("the file exists"),
        "y\ny\n"
    );

    let missing = call(
        &runtime,
        "edit",
        json!({ "edits": [{ "path": "nope.txt", "oldString": "a", "newString": "b" }] }),
        &fixture.directory,
    )
    .await;

    assert!(!missing.ok);
    assert!(
        missing
            .error
            .unwrap_or_default()
            .contains("edit could not read")
    );
}

#[tokio::test]
async fn file_list_pages_prefixes_and_marks_directories() {
    let runtime = registry();
    let fixture = Fixture::new("file-list");

    fixture.write("src/a.ts", "a");
    fixture.write("src/b.ts", "b");
    fixture.write("src/nested/c.ts", "c");
    fixture.write("top.md", "t");

    let outcome = call(
        &runtime,
        "fileList",
        json!({ "paths": ["src", "."] }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&outcome.content).expect("JSON");
    let entries: Vec<&str> = document["entries"]
        .as_array()
        .expect("an array")
        .iter()
        .filter_map(Value::as_str)
        .collect();

    // Directories are visited in CALLER order and each listing is sorted by code point
    // (`fileList.tool.ts` ORDERING). `"."` contributes no prefix, so the default listing shows
    // bare names; a directory entry ends with a slash so it is distinguishable.
    assert_eq!(
        entries,
        vec!["src/a.ts", "src/b.ts", "src/nested/", "src/", "top.md"]
    );
    assert_eq!(document["limit"], json!(200));
    assert_eq!(document["contentBudget"], json!(65_536));

    let paged = call(
        &runtime,
        "fileList",
        json!({ "paths": ["src"], "limit": 1, "offset": 1 }),
        &fixture.directory,
    )
    .await;
    let page: Value = serde_json::from_str(&paged.content).expect("JSON");

    assert_eq!(page["entries"], json!(["src/b.ts"]));
    assert_eq!(page["truncated"], json!(true));
    assert_eq!(page["nextOffset"], json!(2));

    let missing = call(
        &runtime,
        "fileList",
        json!({ "paths": ["gone"] }),
        &fixture.directory,
    )
    .await;

    assert!(!missing.ok);
    assert!(
        missing
            .error
            .unwrap_or_default()
            .contains("fileList could not list")
    );
}

#[tokio::test]
async fn glob_matches_excludes_pages_and_echoes_the_patterns() {
    let runtime = registry();
    let fixture = Fixture::new("glob");

    fixture.write("src/a.ts", "a");
    fixture.write("src/a.test.ts", "a");
    fixture.write("src/b.tsx", "b");
    fixture.write("README.md", "r");

    let outcome = call(
        &runtime,
        "glob",
        json!({ "patterns": ["**/*.ts", "!**/*.test.ts"] }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&outcome.content).expect("JSON");

    // Walk order, posix separators, prefixed by the root that found them: the root's own file
    // comes before the subdirectory's (`glob.ts:393` yields walk order).
    assert_eq!(document["files"], json!(["src/a.ts"]));
    assert_eq!(document["truncated"], json!(false));
    assert_eq!(document["nextOffset"], Value::Null);
    assert_eq!(document["limit"], json!(100));
    assert_eq!(document["stopReason"], json!("complete"));
    assert_eq!(document["partial"], json!(false));
    assert_eq!(document["patterns"], json!(["**/*.ts", "!**/*.test.ts"]));
    assert_eq!(document["searched"]["roots"], json!(1));
    assert_eq!(
        document["searched"]["inScope"].as_u64().expect("a count"),
        4,
        "every walked file is in scope, whether or not the patterns selected it"
    );
    assert_eq!(document["excluded"]["ignoreFilesParsed"], json!(0));
    assert!(
        document.get("note").is_none(),
        "a clean listing states nothing extra"
    );

    let paged = call(
        &runtime,
        "glob",
        json!({ "patterns": ["**/*"], "maxResults": 2 }),
        &fixture.directory,
    )
    .await;
    let page: Value = serde_json::from_str(&paged.content).expect("JSON");

    assert_eq!(page["files"].as_array().expect("an array").len(), 2);
    assert_eq!(page["truncated"], json!(true));
    assert_eq!(page["nextOffset"], json!(2));
    assert_eq!(page["stopReason"], json!("resultCap"));

    let brace = call(
        &runtime,
        "glob",
        json!({ "patterns": ["**/*.{md,tsx}"] }),
        &fixture.directory,
    )
    .await;
    let braces: Value = serde_json::from_str(&brace.content).expect("JSON");

    assert_eq!(braces["files"], json!(["README.md", "src/b.tsx"]));

    // A root that does not exist is a HARD error naming the fix, never an empty listing: a
    // typo must be told apart from a clean tree (`search.ts:268` `rootErrorMessage`).
    let missing_root = call(
        &runtime,
        "glob",
        json!({ "paths": ["gone"] }),
        &fixture.directory,
    )
    .await;
    let refusal = missing_root.error.clone().unwrap_or_default();

    assert!(!missing_root.ok);
    assert!(
        refusal.starts_with(
            "Tool \"glob\" failed: glob path \"gone\" does not exist. Roots are resolved \
             against the working directory ("
        ),
        "the sentence names the path and the working directory to check: {refusal}"
    );
    assert!(
        refusal.contains("list the parent with fileList"),
        "and the action the model can take: {refusal}"
    );

    // An empty `paths`/`patterns` array is refused rather than defaulted, because "match
    // nothing" would read as "this file does not exist" (`search.ts:532-553`).
    let empty_roots = call(&runtime, "glob", json!({ "paths": [] }), &fixture.directory).await;

    assert_eq!(
        empty_roots.error.as_deref(),
        Some(
            "Tool \"glob\" failed: glob received an empty paths array. Omit paths to use the default (\".\")."
        )
    );

    let empty_patterns = call(
        &runtime,
        "glob",
        json!({ "patterns": [] }),
        &fixture.directory,
    )
    .await;

    assert_eq!(
        empty_patterns.error.as_deref(),
        Some(
            "Tool \"glob\" failed: glob received an empty patterns array. Omit patterns to use the default (\"**/*\")."
        )
    );

    // A FILE root is presented exactly as the caller typed it - no doubled basename - and is
    // filtered on both the bare name and that spelling (`search.ts:341-363`).
    let file_root = call(
        &runtime,
        "glob",
        json!({ "paths": ["src/a.ts"], "patterns": ["*.ts"] }),
        &fixture.directory,
    )
    .await;
    let single: Value = serde_json::from_str(&file_root.content).expect("JSON");

    assert_eq!(single["files"], json!(["src/a.ts"]));
    assert_eq!(single["searched"]["roots"], json!(1));
    assert_eq!(single["searched"]["inScope"], json!(1));

    // `join` collapses a `.` segment and a trailing separator, so "./src/" names the same
    // files with the SAME prefix as "src" (`search.ts:391-394`).
    let dotted = call(
        &runtime,
        "glob",
        json!({ "paths": ["./src/"], "patterns": ["*.tsx"] }),
        &fixture.directory,
    )
    .await;
    let collapsed: Value = serde_json::from_str(&dotted.content).expect("JSON");

    assert_eq!(collapsed["files"], json!(["src/b.tsx"]));
}

#[tokio::test]
async fn grep_answers_all_three_output_modes_with_context() {
    let runtime = registry();
    let fixture = Fixture::new("grep");

    fixture.write("src/a.ts", "first\nNEEDLE here\nthird\n");
    fixture.write("src/b.ts", "nothing\nNEEDLE two\nNEEDLE three\n");

    let content = call(
        &runtime,
        "grep",
        json!({ "pattern": "NEEDLE" }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&content.content).expect("JSON");

    assert_eq!(document["output"], json!("content"));
    assert_eq!(document["matches"][0]["path"], json!("src/a.ts"));
    assert_eq!(document["matches"][0]["line"], json!(2));
    assert_eq!(document["matches"][0]["text"], json!("NEEDLE here"));
    assert_eq!(document["matches"][0]["textTruncated"], json!(false));
    assert_eq!(document["matches"].as_array().expect("an array").len(), 3);
    assert_eq!(document["stopReason"], json!("complete"));
    assert_eq!(document["searched"]["scanned"], json!(2));
    assert_eq!(document["skipped"]["binary"], json!(0));

    let files = call(
        &runtime,
        "grep",
        json!({ "pattern": "NEEDLE", "output": "files" }),
        &fixture.directory,
    )
    .await;
    let found: Value = serde_json::from_str(&files.content).expect("JSON");

    assert_eq!(
        found["matches"][0],
        json!({ "path": "src/a.ts", "matchCount": null })
    );
    assert_eq!(found["matches"].as_array().expect("an array").len(), 2);

    let counts = call(
        &runtime,
        "grep",
        json!({ "pattern": "NEEDLE", "output": "count" }),
        &fixture.directory,
    )
    .await;
    let tallies: Value = serde_json::from_str(&counts.content).expect("JSON");

    assert_eq!(
        tallies["matches"][0],
        json!({ "path": "src/a.ts", "matchCount": 1 })
    );
    assert_eq!(
        tallies["matches"][1],
        json!({ "path": "src/b.ts", "matchCount": 2 })
    );

    let with_context = call(
        &runtime,
        "grep",
        json!({ "pattern": "NEEDLE two", "context": 1 }),
        &fixture.directory,
    )
    .await;
    let context_document: Value = serde_json::from_str(&with_context.content).expect("JSON");

    assert_eq!(
        context_document["matches"][0]["contextBefore"],
        json!(["nothing"])
    );
    assert_eq!(
        context_document["matches"][0]["contextAfter"],
        json!(["NEEDLE three"])
    );

    let scoped = call(
        &runtime,
        "grep",
        json!({ "pattern": "NEEDLE", "patterns": ["**/a.ts"] }),
        &fixture.directory,
    )
    .await;
    let scoped_document: Value = serde_json::from_str(&scoped.content).expect("JSON");

    assert_eq!(
        scoped_document["matches"]
            .as_array()
            .expect("an array")
            .len(),
        1
    );

    // A FILE root is displayed exactly as typed: `paths: ["src/a.ts"]` answers about
    // `src/a.ts`, never about a doubled `src/a.ts/src/a.ts` (`search.ts:341-363`).
    let one_file = call(
        &runtime,
        "grep",
        json!({ "pattern": "NEEDLE", "paths": ["src/a.ts"] }),
        &fixture.directory,
    )
    .await;
    let single: Value = serde_json::from_str(&one_file.content).expect("JSON");

    assert_eq!(
        single["matches"]
            .as_array()
            .expect("an array")
            .iter()
            .map(|hit| hit["path"].clone())
            .collect::<Vec<_>>(),
        vec![json!("src/a.ts")],
        "one root, one spelling"
    );
    assert_eq!(single["searched"]["roots"], json!(1));
    assert_eq!(single["searched"]["scanned"], json!(1));

    let bad_flag = call(
        &runtime,
        "grep",
        json!({ "pattern": "x", "flags": "z" }),
        &fixture.directory,
    )
    .await;

    assert!(!bad_flag.ok);
    assert_eq!(
        bad_flag.error.as_deref(),
        Some(
            "Tool \"grep\" failed: grep flags accepts only the JavaScript flag letters \
             \"dgimsuyv\" (\"i\" for case-insensitive is the usual one); got \"z\"."
        )
    );

    // Every letter TS admits is admitted here, including the ones that change nothing for
    // a per-line test (`d`, `v`), so a ripgrep-trained caller is not punished for them.
    for flags in ["d", "g", "i", "m", "s", "u", "y", "v", "gi"] {
        let accepted = call(
            &runtime,
            "grep",
            json!({ "pattern": "nothing-matches-this", "flags": flags, "paths": ["src"] }),
            &fixture.directory,
        )
        .await;

        assert!(
            accepted.ok,
            "\"{flags}\" is a JavaScript flag set: {}",
            accepted.error.unwrap_or_default()
        );
    }
}

#[tokio::test]
async fn paths_resolve_against_the_cwd_and_reject_nul() {
    let runtime = registry();
    let fixture = Fixture::new("path-rules");

    let absolute = fixture.write("abs.txt", "absolute\n");

    let read = call(
        &runtime,
        "read",
        // An absolute path OUTSIDE the cwd is legal: the TS tools are unsandboxed and a
        // workspace agent reads real paths.
        json!({ "paths": [absolute.to_string_lossy()] }),
        &fixture.directory,
    )
    .await;

    assert!(read.ok, "{}", read.error.unwrap_or_default());

    let nul = call(
        &runtime,
        "read",
        json!({ "paths": ["bad\0path"] }),
        &fixture.directory,
    )
    .await;

    assert!(!nul.ok);
    assert_eq!(
        nul.error.as_deref(),
        Some("Tool \"read\" failed: read paths must not contain NUL characters.")
    );
    assert_eq!(nul.duration_ms, 0, "a refused call never executed");
}

#[tokio::test]
async fn an_oversized_result_becomes_the_named_budget_envelope() {
    let runtime = registry();
    let fixture = Fixture::new("result-budget");

    // One line per 2 KiB, 200 lines: `read` caps its own content at 64 KiB, so the serialized
    // document lands just over the 65 536-byte model budget and the envelope must fire.
    let text = (0..200)
        .map(|line| format!("{:0<2047}\n", format!("line {line}")))
        .collect::<String>();

    fixture.write("big.txt", &text);

    let outcome = call(
        &runtime,
        "read",
        json!({ "paths": [{ "path": "big.txt", "maxLines": 5000 }] }),
        &fixture.directory,
    )
    .await;
    let envelope: Value = serde_json::from_str(&outcome.content).expect("the envelope is JSON");

    assert!(outcome.ok);
    assert_eq!(envelope["truncated"], json!(true));
    assert_eq!(envelope["budgetBytes"], json!(65_536));
    assert!(envelope["originalBytes"].as_u64().expect("a count") > 65_536);
    assert!(
        envelope["content"]
            .as_str()
            .expect("a prefix")
            .starts_with('{')
    );
    assert!(
        result::utf8_byte_length(&outcome.content) <= 65_536,
        "the wire text never exceeds the budget"
    );
}

// ---------------------------------------------------------------------------
// Commands and syntax
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shell_captures_exit_stdout_stderr_and_the_timeout_flags() {
    let runtime = registry();
    let fixture = Fixture::new("shell");

    let outcome = call(
        &runtime,
        "shell",
        json!({ "command": "echo harness-probe" }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&outcome.content).expect("the shell result is JSON");

    assert!(outcome.ok, "{}", outcome.error.unwrap_or_default());
    assert_eq!(document["exitCode"], json!(0));
    assert!(
        document["stdout"]
            .as_str()
            .unwrap_or_default()
            .contains("harness-probe")
    );
    assert_eq!(document["timedOut"], json!(false));
    assert_eq!(document["aborted"], json!(false));
    assert_eq!(document["truncated"], json!(false));
    assert!(outcome.duration_ms > 0, "an executed call reports its span");

    let failing = call(
        &runtime,
        "shell",
        json!({ "command": "exit 3" }),
        &fixture.directory,
    )
    .await;
    let failure: Value = serde_json::from_str(&failing.content).expect("JSON");

    // A non-zero exit is DATA: the model decides what it means.
    assert!(failing.ok);
    assert_eq!(failure["exitCode"], json!(3));

    let empty = call(
        &runtime,
        "shell",
        json!({ "command": "   " }),
        &fixture.directory,
    )
    .await;

    assert!(!empty.ok);
    assert_eq!(
        empty.error.as_deref(),
        Some("Tool \"shell\" failed: shell command must not be empty.")
    );
    assert_eq!(empty.duration_ms, 0);
}

#[tokio::test]
async fn git_diff_runs_the_command_and_reports_the_capture_document() {
    let runtime = registry();
    let fixture = Fixture::new("git-diff");

    fixture.write("changed.txt", "before\n");

    // A repository of its own, so the answer is THIS fixture's diff. Without it, `git diff
    // HEAD` walks up to the enclosing checkout and returns that working tree - over 100 KiB
    // here - which would arrive budget-wrapped instead of as the capture document.
    for command in [
        "git init -q .",
        "git add -A",
        "git -c user.email=harness@local -c user.name=harness commit -qm init",
    ] {
        let setup = call(
            &runtime,
            "shell",
            json!({ "command": command }),
            &fixture.directory,
        )
        .await;

        assert!(setup.ok, "{command}: {}", setup.error.unwrap_or_default());

        let document: Value =
            serde_json::from_str(&setup.content).expect("the shell result is JSON");

        assert_eq!(document["exitCode"], json!(0), "{command} must succeed");
    }

    // One tracked file, changed after the commit: exactly one hunk, in one diff header.
    fixture.write("changed.txt", "after\n");

    let outcome = call(
        &runtime,
        "gitDiff",
        json!({ "ref": "HEAD" }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&outcome.content).expect("the diff result is JSON");

    assert!(outcome.ok, "{}", outcome.error.unwrap_or_default());
    assert_eq!(document["exitCode"], json!(0));
    assert_eq!(document["truncated"], json!(false));
    assert_eq!(document["stderrTruncated"], json!(false));
    assert_eq!(document["timedOut"], json!(false));
    assert_eq!(document["aborted"], json!(false));

    let stdout = document["stdout"].as_str().unwrap_or_default();

    assert!(
        stdout.contains("diff --git a/changed.txt b/changed.txt"),
        "the diff header names the changed file: {stdout}"
    );
    assert!(
        stdout.contains("after"),
        "the hunk carries the new text: {stdout}"
    );

    let bad_ref = call(
        &runtime,
        "gitDiff",
        json!({ "ref": "--staged" }),
        &fixture.directory,
    )
    .await;

    assert!(!bad_ref.ok);
    assert_eq!(
        bad_ref.error.as_deref(),
        Some("Tool \"gitDiff\" failed: gitDiff ref must not start with \"-\".")
    );
}

#[tokio::test]
async fn syntax_answers_the_parse_verdict_and_refuses_an_unknown_language() {
    let runtime = registry();
    let fixture = Fixture::new("syntax");

    let json_ok = call(
        &runtime,
        "syntax",
        json!({ "code": "{\"a\": [1, 2]}", "language": "json" }),
        &fixture.directory,
    )
    .await;

    assert!(json_ok.ok);
    assert_eq!(json_ok.content, "true");

    let json_bad = call(
        &runtime,
        "syntax",
        json!({ "code": "{a: 1}", "language": ".json" }),
        &fixture.directory,
    )
    .await;

    assert!(json_bad.ok, "a parse failure is a verdict, not an error");
    assert_eq!(json_bad.content, "false");

    let unbalanced = call(
        &runtime,
        "syntax",
        json!({ "code": "function f() {", "language": "JS" }),
        &fixture.directory,
    )
    .await;

    assert_eq!(unbalanced.content, "false");

    let unsupported = call(
        &runtime,
        "syntax",
        json!({ "code": "fn f() {}", "language": "rust" }),
        &fixture.directory,
    )
    .await;

    assert!(!unsupported.ok);
    assert!(unsupported.error.unwrap_or_default().contains(
        "syntax supports javascript (js), typescript (ts), tsx, jsx, json, python (py, python3)"
    ));
}

// ---------------------------------------------------------------------------
// The harness tools, discovery, and the two session hosts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn time_now_returns_the_triple_and_its_declarative_panel() {
    let runtime = registry();
    let fixture = Fixture::new("time-now");

    let outcome = call(
        &runtime,
        "timeNow",
        json!({ "timezone": "UTC" }),
        &fixture.directory,
    )
    .await;
    let content: Value = serde_json::from_str(&outcome.content).expect("the model reads the JSON");

    assert!(outcome.ok);
    assert_eq!(content["timezone"], json!("UTC"));
    assert!(content["iso"].as_str().unwrap_or_default().ends_with('Z'));
    assert!(content["unix"].as_i64().unwrap_or_default() > 1_500_000_000);

    let panels = outcome
        .panels
        .expect("the toolUi envelope peeled into panels");
    let panels: Vec<Value> = serde_json::from_value(panels).expect("panel JSON");

    assert_eq!(panels.len(), 1);
    assert_eq!(panels[0]["id"], json!("timeNow"));
    assert_eq!(panels[0]["slot"], json!("right"));
    assert_eq!(panels[0]["title"], json!("Current time"));
    assert_eq!(panels[0]["blocks"][0]["type"], json!("keyValue"));
    assert_eq!(
        panels[0]["blocks"][1]["text"],
        json!("Read from the server clock at execution time.")
    );
    assert!(
        !outcome.content.contains("__toolUi"),
        "the brand never reaches the model"
    );

    let oversized = "z".repeat(65);
    let refused = call(
        &runtime,
        "timeNow",
        json!({ "timezone": oversized }),
        &fixture.directory,
    )
    .await;

    assert!(!refused.ok);
    assert_eq!(
        refused.error.as_deref(),
        Some(
            "Tool \"timeNow\" failed: timeNow timezone must be a non-empty string of at most 64 characters."
        )
    );
}

#[tokio::test]
async fn the_counting_search_and_condensing_tools_answer_their_documents() {
    let runtime = registry();
    let fixture = Fixture::new("text-tools");

    let counted = call(
        &runtime,
        "textCount",
        json!({ "text": "two words" }),
        &fixture.directory,
    )
    .await;

    assert_eq!(counted.content, "{\"words\":2,\"characters\":9}");

    let rolled = call(&runtime, "diceRoll", json!({}), &fixture.directory).await;
    let die: Value = serde_json::from_str(&rolled.content).expect("JSON");

    assert_eq!(die["sides"], json!(6));
    assert!(
        (1..=6).contains(&die["value"].as_i64().expect("a number")),
        "the documented range"
    );

    let refused = call(
        &runtime,
        "diceRoll",
        json!({ "sides": 1 }),
        &fixture.directory,
    )
    .await;

    assert_eq!(
        refused.error.as_deref(),
        Some("Tool \"diceRoll\" failed: diceRoll sides must be a whole number from 2 to 1000.")
    );

    let searched = call(
        &runtime,
        "noteSearch",
        json!({ "query": "single-property" }),
        &fixture.directory,
    )
    .await;
    let notes: Value = serde_json::from_str(&searched.content).expect("JSON");

    assert_eq!(notes.as_array().expect("an array").len(), 1);
    assert_eq!(notes[0]["title"], json!("Single-property rule"));

    let summarized = call(
        &runtime,
        "textSummarize",
        json!({ "notes": "First point. Second point. Third point that stays out." }),
        &fixture.directory,
    )
    .await;

    assert_eq!(
        summarized.content, "First point. Second point.",
        "the documented local condensation: at most two sentences"
    );
}

#[tokio::test]
async fn tool_list_and_tool_request_answer_from_the_catalogue_and_the_policy() {
    let runtime = registry();
    let fixture = Fixture::new("discovery");

    let listed = runtime
        .execute(ToolExecCtx {
            name: "toolList".to_owned(),
            arguments: json!({}),
            call_id: "call-list".to_owned(),
            session_id: Some("session-1".to_owned()),
            cwd: fixture.directory.clone(),
            signal: CancellationToken::new(),
            policy: ToolPolicy {
                declared: vec!["read".to_owned(), "grep".to_owned()],
                enabled: vec!["shell".to_owned()],
                created: vec![],
                subagents: vec![],
            },
            action_host: None,
            task_host: None,
        })
        .await;
    let entries: Value = serde_json::from_str(&listed.content).expect("JSON");
    let names: Vec<&str> = entries
        .as_array()
        .expect("an array")
        .iter()
        .filter_map(|entry| entry.get("name").and_then(Value::as_str))
        .collect();

    assert_eq!(
        entries.as_array().expect("an array")[0]["description"],
        json!(catalogue::find("toolRequest").expect("row").description)
    );
    assert!(names.contains(&"read"));
    assert!(names.contains(&"shell"));
    assert!(names.contains(&"task"));

    let requested = call(
        &runtime,
        "toolRequest",
        json!({ "names": ["fileList"], "tags": ["git", "diff"], "regex": "^shell$" }),
        &fixture.directory,
    )
    .await;
    let document: Value = serde_json::from_str(&requested.content).expect("JSON");
    let tools: Vec<&str> = document["tools"]
        .as_array()
        .expect("an array")
        .iter()
        .filter_map(|entry| entry.get("name").and_then(Value::as_str))
        .collect();

    // Ranked by each row's STRONGEST criterion (`discovery.ts:373-378`): the exact requested
    // name first (in the caller's own order), then the regex hit, then the tag-only match.
    assert_eq!(tools, vec!["fileList", "shell", "gitDiff"]);
    assert_eq!(document["matched"], json!(3));
    assert_eq!(document["remaining"], json!(0));
    assert!(
        document["enabled"]
            .as_array()
            .expect("an array")
            .contains(&json!("fileList"))
    );
    assert!(
        document["availableTags"]
            .as_array()
            .expect("a vocabulary")
            .contains(&json!("files"))
    );

    // A capability the run ALREADY has is reported, not enabled.
    let already = call(
        &runtime,
        "toolRequest",
        json!({ "names": ["read"] }),
        &fixture.directory,
    )
    .await;
    let already_document: Value = serde_json::from_str(&already.content).expect("JSON");

    assert_eq!(already_document["tools"], json!([]));
    assert_eq!(
        already_document["alreadyAvailable"][0]["name"],
        json!("read")
    );
    assert!(
        !already_document["enabled"]
            .as_array()
            .expect("an array")
            .contains(&json!("read"))
    );

    let limited = call(
        &runtime,
        "toolRequest",
        json!({ "regex": "^t", "limit": 2 }),
        &fixture.directory,
    )
    .await;
    let limited_document: Value = serde_json::from_str(&limited.content).expect("JSON");

    assert_eq!(
        limited_document["tools"].as_array().expect("a page").len(),
        2
    );
    assert_eq!(limited_document["truncated"], json!(true));
    assert!(limited_document["remaining"].as_u64().expect("a count") > 0);
}

#[tokio::test]
async fn task_passes_through_the_host_and_action_request_answers_with_the_choice() {
    let runtime = registry();
    let fixture = Fixture::new("session-hosts");

    let task = call_with_task(
        &runtime,
        "task",
        json!({ "tasks": [{ "text": "ship T4" }] }),
        &fixture.directory,
        std::sync::Arc::new(EchoTaskHost),
    )
    .await;
    let document: Value = serde_json::from_str(&task.content).expect("JSON");

    assert!(task.ok);
    assert_eq!(
        document["echo"]["tasks"][0]["text"],
        json!("ship T4"),
        "the host's own JSON is the result content"
    );

    let no_host = call(&runtime, "task", json!({}), &fixture.directory).await;

    assert!(!no_host.ok);
    assert_eq!(
        no_host.error.as_deref(),
        Some("Tool \"task\" failed: task needs this session's task board, and this run has none.")
    );

    let refused = call_with_task(
        &runtime,
        "task",
        json!({}),
        &fixture.directory,
        std::sync::Arc::new(RefusingTaskHost),
    )
    .await;

    assert!(!refused.ok);
    assert_eq!(
        refused.content,
        "Tool \"task\" failed: the task board is full"
    );
    // A call that EXECUTED and failed states the same text in `error` as in `content`, and it
    // carries no panels or media - the shape the turn engine turns into `isError: true`.
    assert_eq!(refused.error.as_deref(), Some(refused.content.as_str()));
    assert_eq!(refused.panels, None);
    assert_eq!(refused.media, None);

    let action = call_with_action(
        &runtime,
        "actionRequest",
        json!({
            "question": "Which build should ship?",
            "recommended": "b",
            "options": [
                { "id": "a", "label": "Debug" },
                { "id": "b", "label": "Release" }
            ],
        }),
        &fixture.directory,
        std::sync::Arc::new(AnsweringActionHost),
    )
    .await;
    let answer: Value = serde_json::from_str(&action.content).expect("the answer is JSON");

    assert!(action.ok);
    assert_eq!(answer["choice"]["optionId"], json!("b"));
    assert_eq!(answer["source"], json!("owner"));

    let unknown_option = call_with_action(
        &runtime,
        "actionRequest",
        json!({
            "question": "Which build should ship?",
            "recommended": "zzz",
            "options": [
                { "id": "a", "label": "Debug" },
                { "id": "b", "label": "Release" }
            ],
        }),
        &fixture.directory,
        std::sync::Arc::new(AnsweringActionHost),
    )
    .await;

    assert!(!unknown_option.ok);
    assert_eq!(
        unknown_option.error.as_deref(),
        Some("actionRequest recommended \"zzz\" is not one of the supplied option ids.")
    );
    assert_eq!(unknown_option.duration_ms, 0, "the host was never asked");
}

#[tokio::test]
async fn a_catalogued_only_tool_fails_well_formed_and_never_panics() {
    let runtime = registry();
    let fixture = Fixture::new("catalogued-only");

    for name in [
        "toolRegistry",
        "toolManage",
        "toolCreate",
        "profileManage",
        "chapterManage",
        "locationManage",
        "agentManage",
        "agentResult",
        "delegate",
        "threadFork",
        "toolCancel",
        "threadSend",
        "threadRead",
        "threadList",
    ] {
        let outcome = call(&runtime, name, json!({}), &fixture.directory).await;

        assert!(!outcome.ok, "{name} must not report success");
        assert!(
            outcome.content.starts_with("Tool \"") && outcome.content.contains("failed"),
            "{name} answers in the failed-call shape: {}",
            outcome.content
        );
        assert!(
            outcome.content.contains("this server does not support"),
            "{name} names the missing capability"
        );

        if name == "delegate" {
            assert!(outcome.content.contains("nested agent runs"));
        }
    }

    // `search` with no credential answers with the catalogue's own hint.
    let search = call(
        &runtime,
        "search",
        json!({ "query": "anything" }),
        &fixture.directory,
    )
    .await;

    assert!(!search.ok);
    assert!(
        search
            .error
            .unwrap_or_default()
            .starts_with("Tool \"search\" failed: Web search is not configured on this server."),
        "a keyless host gets the catalogue's own hint, wrapped in the failed-call shape"
    );
}

#[tokio::test]
async fn an_unknown_tool_and_missing_required_arguments_are_rejections() {
    let runtime = registry();
    let fixture = Fixture::new("rejections");

    let unknown = call(
        &runtime,
        "definitelyNotATool",
        json!({}),
        &fixture.directory,
    )
    .await;
    // Read the sentence ONCE: `error` moves out of the outcome, so a second `unwrap_or_default`
    // would be a use-after-move.
    let refusal = unknown.error.clone().unwrap_or_default();

    assert!(!unknown.ok);
    assert!(
        refusal.starts_with("Tool \"definitelyNotATool\" is not registered. Available tools: "),
        "the discovery pointer names the tool and lists what is declared: {refusal}"
    );
    assert!(
        refusal.contains("Call toolRequest with capability tags to search the catalogue."),
        "the refusal ends with the action the model can take"
    );
    assert_eq!(unknown.duration_ms, 0, "an immediate refusal executed");
    assert_eq!(unknown.panels, None);
    assert_eq!(unknown.media, None);

    let missing = call(
        &runtime,
        "write",
        json!({ "path": "x" }),
        &fixture.directory,
    )
    .await;

    assert!(!missing.ok);
    assert_eq!(
        missing.error.as_deref(),
        Some(
            "Tool \"write\" received arguments that do not match its schema: missing required property \"files\"."
        )
    );

    let not_object = call(&runtime, "read", json!("package.json"), &fixture.directory).await;

    assert!(!not_object.ok);
    assert!(
        not_object
            .error
            .unwrap_or_default()
            .contains("received arguments that are not a JSON object (string)")
    );
}

#[tokio::test]
async fn every_catalogued_row_has_a_detail_and_a_known_kind() {
    let runtime = registry();
    let fixture = Fixture::new("every-row");

    for entry in catalogue::ENTRIES.iter() {
        let detail = runtime.detail(entry.name);

        if catalogue::is_listed(entry.name) {
            let detail = detail.unwrap_or_else(|| panic!("{} details", entry.name));

            assert!(!detail.summary.description.is_empty());
            assert_eq!(
                detail.parameters.as_ref().expect("a schema")["type"],
                json!("object"),
                "{}'s schema is an object schema",
                entry.name
            );
            assert!(matches!(
                detail.summary.availability.as_ref().map(|a| a.state),
                None | Some(ToolAvailabilityState::Available)
                    | Some(ToolAvailabilityState::Unconfigured)
            ));
        } else {
            // T7b: a `listed: false` row resolves on the session toolbox and the dispatch path, but
            // NOT on the global identified read (`api.ts:1847` looks it up in `coreEntries`, which
            // `tools/descriptors.ts:290-328` already filtered).
            assert!(
                detail.is_none(),
                "{} is unlisted, so GET /tools/{{name}} must not resolve it",
                entry.name
            );
        }

        let outcome = call(&runtime, entry.name, json!({}), &fixture.directory).await;

        // Whatever the answer, the runtime ANSWERS: a call never panics and never leaves the
        // content empty for a failed call.
        if !outcome.ok {
            assert!(
                !outcome.content.is_empty(),
                "{name} explains itself",
                name = entry.name
            );
        }
    }
}

#[test]
fn the_attachment_kind_table_is_the_shared_one() {
    // `download`'s MIME policy and the composer's `kind` share one table, so an image stays an
    // image on both sides of the port.
    assert_eq!(
        typecrate_harness::tools::web::kind_for_mime("image/png"),
        AttachmentKind::Image
    );
    assert_eq!(
        typecrate_harness::tools::web::kind_for_mime("application/octet-stream"),
        AttachmentKind::File
    );
}
