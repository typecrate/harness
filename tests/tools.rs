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

        // FIXED-WIDTH `pid` and `sequence` ONLY, on purpose: padding them (`:55`) stops a parallel
        // `cargo test` handing two tests the same directory when either gains a digit, which is the
        // collision this guards and nothing else. `-{name}` is unpadded and the `repo_root()` prefix
        // is whatever this checkout is, so a fixture path's TOTAL length is not constant across
        // tests or machines — the 120 characters this checkout measures belong to the
        // `Fixture::new("result-budget")` fixture alone. The byte-count pin that used to be hostage
        // to all this (`an_oversized_json_read_result_is_refused_rather_than_truncated`) now computes
        // its own path term at runtime, so it depends on neither the padding nor the checkout root.
        let directory = typecrate_harness::package_root::repo_root()
            .join("Temp")
            .join("typecrate-harness")
            .join("t4")
            .join(format!(
                "{pid:0>9}-{sequence:0>4}-{name}",
                pid = std::process::id()
            ));

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
        ..Options::default()
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
        32,
        "every LISTED runtime, preset, harness and web row: the 36 generated rows minus the four \
         `listed: false` rows this table carries (`tool-manifest.ts:1446, 2265, 2379, 2410`) - TS \
         withholds FIVE runtime names with that flag, the fifth being `threadDelete` (`:2219`), \
         which has no row here at all"
    );
    assert_eq!(names.len(), 32);
    assert_eq!(rows[0].name, "toolRegistry");
    assert_eq!(names[0], "toolRegistry");

    // T7b: the unlisted runtime names are absent from the listing. The first four ARE rows in
    // `ENTRIES` and are withheld by `catalogue::UNLISTED`; `threadDelete` is the fifth name the
    // manifest marks `listed: false` (`tool-manifest.ts:2219`) and is absent here for the OTHER
    // reason - it was never ported as a row at all. That is the port-owner decision recorded once
    // in `README.md`'s "Tool catalogue" parity row (`README.md:145`); this line pins only the
    // observable half (the listing never carries the name), so pinning it does not pre-decide the
    // row.
    for withheld in [
        "toolCancel",
        "threadSend",
        "threadRead",
        "threadList",
        "threadDelete",
    ] {
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

    // A4: every core row states `available`, and the HINT is now the honest half. An executable core
    // row carries the session-bound sentence (`harnesses/core.ts:87`); a row this build LISTS but
    // cannot run carries the same capability sentence a call to it answers with, because a row that
    // advertises a capability this server does not have was the defect the audit found.
    let session_hint = "Runs inside a session harness: send a sessionId with the call.";

    // The four core names the runtime stage actually executes (the rest of the group is
    // catalogued-only), so the split below is a stated set rather than an inference.
    let executable_core: Vec<&str> = rows
        .iter()
        .filter(|row| row.group == "Core tools")
        .filter(|row| {
            row.availability.as_ref().and_then(|a| a.hint.as_deref()) == Some(session_hint)
        })
        .map(|row| row.name.as_str())
        .collect();

    assert_eq!(
        executable_core,
        vec!["toolRequest", "toolList", "tasks", "actionRequest"],
        "exactly the core rows this build executes keep the session-bound hint"
    );

    // Two verbatim pins, one per capability family, so a drifted sentence is caught by a literal and
    // not only by the cross-check against execution.
    for (name, capability) in [
        ("delegate", "nested agent runs (the delegation runtime)"),
        (
            "toolRegistry",
            "compiling and registering a tool from editable source (the runtime compiler)",
        ),
    ] {
        let row = rows
            .iter()
            .find(|row| row.name == name)
            .unwrap_or_else(|| panic!("{name} is listed"));
        let availability = row
            .availability
            .clone()
            .expect("a core row states availability");

        assert_eq!(availability.state, ToolAvailabilityState::Available);
        assert_eq!(
            availability.hint.as_deref(),
            Some(format!(
                "this server does not support {capability}, so \"{name}\" cannot run here. The capability is on the port list; use the tools that are executable to make progress."
            ).as_str()),
            "{name} advertises the sentence a call to it answers"
        );
    }

    // Every OTHER catalogued-only row states the same shape, and no executable row states it.
    let stated_only: Vec<&str> = rows
        .iter()
        .filter(|row| {
            row.availability
                .as_ref()
                .and_then(|a| a.hint.as_deref())
                .is_some_and(|hint| hint.starts_with("this server does not support"))
        })
        .map(|row| row.name.as_str())
        .collect();

    assert_eq!(
        stated_only,
        vec![
            "toolRegistry",
            "toolManage",
            "toolCreate",
            "records",
            "profiles",
            "chapters",
            "locations",
            "agentManage",
            "agentResult",
            "delegate",
            "threadFork",
        ],
        "the catalogued-only rows in listing order - and only they - advertise the missing capability"
    );

    for row in rows.iter().filter(|row| row.group == "Core tools") {
        let availability = row
            .availability
            .clone()
            .expect("a core row states availability");

        assert_eq!(availability.state, ToolAvailabilityState::Available);
        assert!(
            availability.hint.is_some(),
            "{} states a hint, never an empty availability block",
            row.name
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
    assert_eq!(visibility("tasks"), ToolVisibility::Core);
    assert_eq!(visibility("read"), ToolVisibility::Core);
    assert_eq!(visibility("files"), ToolVisibility::Searchable);
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

/// A4: the row a client reads and the call the client makes must tell ONE story.
///
/// Every advertised row whose `availability.hint` names a missing capability must answer a call to
/// that tool with that exact sentence inside the failed-call body, and no row that does NOT name one
/// may ever answer with it. The hint and the refusal come from the same function
/// (`tools::missing_capability_hint`); this test is the half that proves it, because a shared helper
/// can still be bypassed by a second call site.
#[tokio::test]
async fn every_advertised_availability_hint_agrees_with_what_execution_answers() {
    let runtime = registry();
    let fixture = Fixture::new("hint-parity");

    for row in runtime.summaries() {
        let hint = row
            .availability
            .as_ref()
            .and_then(|availability| availability.hint.clone())
            .unwrap_or_default();

        let outcome = call(&runtime, &row.name, json!({}), &fixture.directory).await;

        if hint.starts_with("this server does not support ") {
            assert!(
                !outcome.ok,
                "{} advertises a missing capability, so the call cannot succeed",
                row.name
            );
            assert_eq!(
                outcome.content,
                format!("Tool \"{}\" failed: {hint}", row.name),
                "{} answers the sentence its row advertises, byte for byte",
                row.name
            );
        } else {
            assert!(
                !outcome.content.contains("this server does not support"),
                "{} is executable, yet execution answered with the capability refusal: {}",
                row.name,
                outcome.content
            );
        }
    }
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

    let task = runtime.detail("tasks").expect("a catalogued row");

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
            // The published provider endpoints; only the key's effect on availability is under
            // test here.
            endpoint_base: None,
            // The download SSRF/DNS seams override a production lookup; a catalogue test has none.
            ..typecrate_harness::tools::web::WebKeys::default()
        },
        fetch_enabled: false,
        ..Options::default()
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
    assert!(names.contains(&"tasks"), "always declared");
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
        !names.contains(&"files"),
        "searchable waits for toolRequest"
    );

    let enabled_but_unexecutable = ToolPolicy {
        enabled: vec!["profiles".to_owned(), "threadRead".to_owned()],
        ..ToolPolicy::default()
    };
    let offered = runtime.declarations(&enabled_but_unexecutable);

    assert!(
        !offered
            .iter()
            .any(|decl| decl.name == "profiles" || decl.name == "threadRead"),
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
    assert_eq!(document["contentBudget"], json!(32_768));
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
        "files",
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
    assert_eq!(document["contentBudget"], json!(32_768));

    let paged = call(
        &runtime,
        "files",
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
        "files",
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
    // `duration_ms` is deliberately NOT pinned here. The NUL refusal happens INSIDE the `read`
    // body, so the call goes through `failed_outcome`, whose duration is the MEASURED span of the
    // attempt (`src/tools/mod.rs:1066-1071`) — 0 on an idle machine and several milliseconds on a
    // loaded one. Only a STRUCTURAL refusal, which never reaches a tool body, reports a hard zero
    // (`src/tools/mod.rs:1076-1087`; `an_unknown_tool_and_missing_required_arguments_are_rejections`
    // below pins that one). What
    // "the call never executed" MEANS is filesystem-visible, so that is what gets pinned: the
    // refusal happened before any access, and the fixture still holds exactly the one file this
    // test created — nothing opened, created, or touched.
    let entries: Vec<String> = std::fs::read_dir(&fixture.directory)
        .expect("the fixture directory is listable")
        .map(|entry| {
            entry
                .expect("a fixture entry is readable")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect();

    assert_eq!(
        entries,
        vec!["abs.txt".to_owned()],
        "the refused read touched nothing"
    );
}

#[tokio::test]
async fn a_read_result_at_the_wire_ceiling_is_delivered_rather_than_refused() {
    let runtime = registry();
    let fixture = Fixture::new("result-budget");

    // Named from the contract rather than mirrored as a literal, so a future budget change moves
    // this pin with it instead of leaving a stale number behind.
    let budget = typecrate_harness::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;

    // 200 lines, each 2,047 characters of text plus a newline (the `{:0<2047}` pad and the `\n`):
    // 2,048 raw bytes per line, 2,049 PAYLOAD bytes, because a newline inside a JSON string can only
    // travel as the two-byte `\n` escape. `maxLines: 5000` keeps the line guard out of it, so the
    // window closes on BYTES. This is the shape that used to be REFUSED: `read` filled its content
    // budget, the document it returns came out bigger than the budget because the budget was
    // counted in raw bytes, and an over-budget result that parses as JSON is dropped whole
    // (`rejected: { reason: 'oversized-json' }`) instead of being cut
    // (`src/tools/result.rs`'s oversized-JSON branch).
    //
    // Both harnesses now spend the SAME currency at both ends, so the tool's ceiling IS the
    // delivery ceiling: `READ_CONTENT_BUDGET` is `MODEL_TOOL_RESULT_BUDGET_BYTES`
    // (`src/tools/files.rs:59-75`) and the window's allowance is that budget MINUS the serialized
    // cost of the document around it (`read_result_overhead`, `src/tools/files.rs:554-601`, the
    // reference's `FILE_READ_WIRE_BUDGET` / entry-shell reserve, `read.tool.ts:1697-1740`). What
    // `read` decides to cut and what the cutter decides to keep are now the same measurement, so
    // the page arrives WHOLE with its own `truncated: true` — and the refusal this test used to pin
    // stays pinned one level down, where it is still the contract, at
    // `src/tools/result.rs`'s `an_oversized_json_document_is_refused_instead_of_two_broken_halves`
    // and `a_json_document_at_exactly_the_payload_budget_is_delivered_whole`.
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

    // THE RESULT IS DELIVERED, NOT REFUSED. The transcript text is the tool's own JSON document,
    // so it parses; the refusal sentence this pin used to assert is plain text and would not.
    assert!(
        outcome.ok,
        "a read page at the delivery ceiling arrives whole: {}",
        outcome.error.unwrap_or_default()
    );

    let document: Value =
        serde_json::from_str(&outcome.content).expect("the delivered result is a JSON document");

    // THE PAGE IS CLOSED BY BYTES, NOT BY LINES, AND ITS WIDTH IS DERIVED RATHER THAN FROZEN.
    //
    // 200 lines were requested with `maxLines: 5000`, so the line guard is far away and only the
    // byte allowance can close this page. The allowance is the 32,768-byte budget MINUS the
    // serialized cost of the document AROUND the window (`read_result_overhead`,
    // `src/tools/files.rs`; the entry-shell/RESULT_SHELL reserve that stands behind the reference's
    // `FILE_READ_WIRE_BUDGET`, `read.tool.ts:1697-1740`). A line costs 2,047 payload bytes of text -
    // the fixture's pad is pure ASCII, so text costs one byte per character in either currency -
    // plus 2 for its newline (`char_payload_bytes`, `src/payload.rs`): 2,049 per line. 15 lines fit
    // and the 16th does not, and the room left over is spent INSIDE line 16, which is why
    // `nextColumn` is a column rather than 0.
    //
    // The overhead is read from the crate instead of being written out as a literal: it echoes the
    // caller's path, so any frozen window width here would only be true at one checkout's path
    // length - the same reason the frozen `65864` this test used to carry was never
    // machine-independent.
    let displayed_path = fixture.directory.join("big.txt").display().to_string();
    let overhead =
        typecrate_harness::tools::files::read_result_overhead(&[(displayed_path.clone(), None)]);
    let allowance = budget - overhead;
    let full_lines = allowance / 2_049;
    let partial = allowance - (full_lines * 2_049);

    // The fixture's shape is what makes the numbers above mean anything: 15 lines fit and a 16th
    // does not, while 200 lines wait behind it.
    assert_eq!(full_lines, 15, "the allowance holds 15 whole lines");
    assert!(
        partial > 0 && partial < 2_049,
        "and {partial} bytes into the 16th, which is the mid-line cut this page must report"
    );

    let window = &document["files"][0];

    assert_eq!(
        window["path"],
        json!(displayed_path),
        "the entry echoes the caller's path in the spelling the overhead reserved"
    );
    assert_eq!(window["startLine"], json!(1), "the window starts at line 1");
    assert_eq!(
        window["endLine"],
        json!(full_lines),
        "and names the last WHOLE line it delivered: the partial line counts toward `bytes` but \
         not toward a line the model can read, which is exactly what `nextLine`/`nextColumn` are \
         for (`read.tool.ts:309-310` 'endLine is the last line represented' and `:322-326` for the \
         0-based byte offset that resumes a cut line)"
    );
    assert_eq!(
        window["bytes"],
        json!(full_lines * 2_048 + partial),
        "the entry REPORTS the UTF-8 width of its text (15 x 2,048 + the partial line) while the \
         budget spends the escaped width (15 x 2,049 + the same partial) - the two-unit split \
         `result-budget.ts`'s `spillNote` states and `src/payload.rs` keeps honest"
    );
    assert_eq!(
        window["content"]
            .as_str()
            .expect("a content window")
            .chars()
            .count(),
        full_lines * 2_048 + partial,
        "and the text itself has exactly that length"
    );
    assert_eq!(
        window["truncated"],
        json!(true),
        "the page says more remains"
    );
    assert_eq!(
        window["nextLine"],
        json!(full_lines + 1),
        "and hands back the line to resume from"
    );
    assert_eq!(
        window["nextColumn"],
        json!(partial),
        "at the byte offset inside that line where the allowance ran out"
    );
    assert_eq!(
        document["contentBudget"],
        json!(32_768),
        "the budget the page publishes is the model-facing one, not a private 64 KiB"
    );
    assert_eq!(document["truncated"], json!(true));
    assert_eq!(
        document["bytes"],
        json!(full_lines * 2_048 + partial),
        "one entry, so the document sum is the window's own width"
    );

    // THE CURRENCY PIN. What reaches the model costs at most the budget IN THE UNIT THE BUDGET IS
    // DEFINED IN - the escaped payload width `crate::payload::model_payload_bytes` measures, kept
    // in sync with the reference's `modelPayloadBytes` by `src/payload.rs`'s vectors - and it is
    // within ONE LINE of the ceiling, which is what "the tool sized the page, the cutter did not
    // have to" looks like from outside. This is the assertion that failed when the gate counted
    // payload bytes while `read` still spent raw ones: the page cost ~68.2 KB of payload against a
    // 32,768-byte budget and was dropped whole, because `read` believed it had 64 KiB to fill.
    let delivered_payload = result::model_payload_bytes(&outcome.content);

    assert!(
        delivered_payload <= budget,
        "the delivered page costs {delivered_payload} payload bytes against a {budget}-byte budget"
    );
    assert!(
        delivered_payload > budget - 2_049,
        "the page filled the ceiling instead of leaving a line of it unused: {delivered_payload} \
         payload bytes spent"
    );
    assert!(
        result::utf8_byte_length(&outcome.content) < delivered_payload,
        "the same document is {delivered_payload} payload bytes and only {} raw ones, because \
         every newline in the window serializes as an escape - the gap that used to make the raw \
         count useless as a delivery test",
        result::utf8_byte_length(&outcome.content)
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
        // `shell.tool.ts:196` — the reference sentence, not the older local one.
        Some("Tool \"shell\" failed: shell requires a non-empty command string.")
    );
    // The pinned sentence IS the proof that nothing ran: `shell` rejects a blank command in its
    // body before spawning `sh -c`, so this is a failed call (`src/tools/mod.rs:1066-1071`) rather
    // than a structural rejection. Its `duration_ms` is therefore the measured span of the attempt,
    // which a loaded machine can report as a few milliseconds; only a call that never reaches a
    // tool body is contracted to report 0 (`src/tools/mod.rs:1076-1087`), so no duration pin here.
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
    assert!(names.contains(&"tasks"));

    let requested = call(
        &runtime,
        "toolRequest",
        json!({ "names": ["files"], "tags": ["git", "diff"], "regex": "^shell$" }),
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
    assert_eq!(tools, vec!["files", "shell", "gitDiff"]);
    assert_eq!(document["matched"], json!(3));
    assert_eq!(document["remaining"], json!(0));
    assert!(
        document["enabled"]
            .as_array()
            .expect("an array")
            .contains(&json!("files"))
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
        "tasks",
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

    let no_host = call(&runtime, "tasks", json!({}), &fixture.directory).await;

    assert!(!no_host.ok);
    assert_eq!(
        no_host.error.as_deref(),
        Some("Tool \"tasks\" failed: task needs this session's task board, and this run has none.")
    );

    let refused = call_with_task(
        &runtime,
        "tasks",
        json!({}),
        &fixture.directory,
        std::sync::Arc::new(RefusingTaskHost),
    )
    .await;

    assert!(!refused.ok);
    assert_eq!(
        refused.content,
        "Tool \"tasks\" failed: the task board is full"
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
        "profiles",
        "chapters",
        "locations",
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

// ---------------------------------------------------------------------------
// The delivery currency at the file tools' own ceilings (F8)
// ---------------------------------------------------------------------------
//
// `read` above is the case that used to be REFUSED; these three are the same failure in the three
// other budgeted tools. Each fixture is sized so the TOOL closes its page on its byte allowance
// rather than on its entry cap (`maxResults`/`limit` are pushed to the published maximum so the
// allowance is the only thing that can fire), and each then asserts both halves of the F8 property:
//
//   1. the tool stopped itself and said so - `truncated`, a `nextOffset` that advances, and where
//      the row publishes it, the `stopReason` that names the budget that closed the page;
//   2. the document that stopped there is inside the model-facing cap IN THE CAP'S OWN UNIT, so
//      `crate::tools::result` never receives a result it would refuse. Before F8 the tools measured
//      their content in raw bytes while the cutter charged escaped payload bytes, so a tool that
//      believed it had filled its budget exactly arrived over the cap and a JSON document over the
//      cap is dropped whole.
//
// The page size is DERIVED from the tool's own exported allowance in every test, because the
// overhead inside that allowance echoes the caller's path and patterns: a frozen entry count here
// would be true only at this checkout's path length.

#[tokio::test]
async fn a_glob_page_closed_by_its_byte_allowance_is_delivered_rather_than_refused() {
    let runtime = registry();
    let fixture = Fixture::new("glob-wire-ceiling");

    // 400 names of IDENTICAL width, so the page's charge is the delivered count times one entry's
    // cost - and the cost is the same `entry_wire_bytes` the tool charges with, not a guess at it.
    for index in 0..400 {
        fixture.write(&format!("src/{:0<90}.ts", index), "x");
    }

    let outcome = call(
        &runtime,
        "glob",
        json!({ "patterns": ["**/*.ts"], "maxResults": 1000 }),
        &fixture.directory,
    )
    .await;
    assert!(
        outcome.ok,
        "a glob that fills its page is delivered, not refused: {}",
        outcome.error.unwrap_or_default()
    );

    let budget = typecrate_harness::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;
    let patterns = vec!["**/*.ts".to_owned()];
    let allowance = typecrate_harness::tools::files::glob_page_allowance(&patterns);
    let entry_cost =
        typecrate_harness::payload::entry_wire_bytes(&json!(format!("src/{:0<90}.ts", 0)));
    let fits = allowance / entry_cost;

    // The fixture's precondition: the allowance, not the cap, is what closes this page, and the
    // walk still has names left over to withhold.
    assert!(
        fits < 400 && fits > 100,
        "the allowance holds {fits} of the 400 names at {entry_cost} payload bytes each"
    );

    let document: Value =
        serde_json::from_str(&outcome.content).expect("the delivered result is a JSON document");

    assert_eq!(
        document["files"]
            .as_array()
            .expect("an array of paths")
            .len(),
        fits,
        "the page stops one name before the name that would cross the allowance"
    );
    assert_eq!(
        document["bytes"],
        json!(fits * entry_cost),
        "`bytes` reports what the kept names charged in the delivery currency"
    );
    assert_eq!(document["truncated"], json!(true));
    assert_eq!(
        document["nextOffset"],
        json!(fits),
        "and the cursor advances, so a caller paging this sequence is not trapped"
    );
    assert_eq!(
        document["limit"],
        json!(1000),
        "the cap that did NOT close the page"
    );
    assert_eq!(
        document["stopReason"],
        json!("contentBudget"),
        "the row names the bound that closed it instead of blaming the entry cap"
    );
    assert_eq!(document["contentBudget"], json!(32_768));
    assert!(
        document["note"]
            .as_str()
            .unwrap_or_default()
            .contains("path-text allowance"),
        "and `note` states the allowance by number: {}",
        document["note"]
    );

    let delivered_payload = result::model_payload_bytes(&outcome.content);

    assert!(
        delivered_payload <= budget,
        "a glob that fills its page is inside the cap it publishes: {delivered_payload} payload \
         bytes against a {budget}-byte budget"
    );
}

#[tokio::test]
async fn a_grep_page_closed_by_its_byte_allowance_is_delivered_rather_than_refused() {
    let runtime = registry();
    let fixture = Fixture::new("grep-wire-ceiling");

    // 500 matching lines of identical width: every entry is the same snippet under the same path,
    // so the only variation in an entry's cost is how many digits its line number prints.
    let line = format!("{:0<66}needle", 0);
    let text = (0..500).map(|_| format!("{line}\n")).collect::<String>();

    fixture.write("hits.txt", &text);

    let outcome = call(
        &runtime,
        "grep",
        json!({ "pattern": "needle", "paths": ["hits.txt"], "maxResults": 1000 }),
        &fixture.directory,
    )
    .await;
    assert!(
        outcome.ok,
        "a grep that fills its page is delivered, not refused: {}",
        outcome.error.unwrap_or_default()
    );

    let budget = typecrate_harness::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;
    let allowance = typecrate_harness::tools::files::grep_page_allowance();
    let document: Value =
        serde_json::from_str(&outcome.content).expect("the delivered result is a JSON document");
    let matches = document["matches"].as_array().expect("an array of hits");

    // THE NUMBER THE TOOL PUBLISHES IS THE NUMBER THE CURRENCY CHARGES. `bytes` is no longer the
    // sum of snippet text: it is what the page cost the budget, and this recomputes it from the
    // entries the page actually delivered.
    let charged: usize = matches
        .iter()
        .map(typecrate_harness::payload::entry_wire_bytes)
        .sum();

    assert_eq!(
        document["bytes"],
        json!(charged),
        "the result's `bytes` is the delivery-currency charge for exactly these {} entries",
        matches.len()
    );
    assert!(
        matches.len() >= 100 && matches.len() < 500,
        "the allowance closed the page after {} of the 500 matches",
        matches.len()
    );
    assert!(
        charged + typecrate_harness::payload::entry_wire_bytes(&matches[matches.len() - 1])
            > allowance,
        "and it stopped at {charged} of the {allowance}-byte allowance: one more of these entries \
         would not have fitted"
    );

    // An entry's OWN `bytes` stays the UTF-8 size of its snippet, which is a different number from
    // the page's charge and the unit the caller uses to judge a cut line (`grep.tool.ts:347-351`).
    assert_eq!(matches[0]["bytes"], json!(line.len()));
    assert_eq!(matches[0]["text"], json!(line));
    assert_eq!(matches[0]["line"], json!(1));
    assert_eq!(matches[0]["path"], json!("hits.txt"));

    assert_eq!(document["truncated"], json!(true));
    assert_eq!(document["nextOffset"], json!(matches.len()));
    assert_eq!(
        document["stopReason"],
        json!("contentBudget"),
        "a page the byte budget closed does not blame the result cap"
    );
    assert_eq!(document["contentBudget"], json!(32_768));

    let delivered_payload = result::model_payload_bytes(&outcome.content);

    assert!(
        delivered_payload <= budget,
        "a grep that fills its page is inside the cap it publishes: {delivered_payload} payload \
         bytes against a {budget}-byte budget"
    );
    assert!(
        delivered_payload > budget - 2_048,
        "the page used the ceiling instead of leaving a match of slack: {delivered_payload} spent"
    );
}

#[tokio::test]
async fn a_file_list_page_closed_by_its_byte_allowance_is_delivered_rather_than_refused() {
    let runtime = registry();
    let fixture = Fixture::new("file-list-wire-ceiling");

    // One flat directory, 400 entries of identical width, so the page's charge is the delivered
    // count times one entry's cost.
    for index in 0..400 {
        fixture.write(&format!("src/{:0<90}.ts", index), "x");
    }

    let outcome = call(
        &runtime,
        "files",
        json!({ "paths": ["src"], "limit": 1000 }),
        &fixture.directory,
    )
    .await;
    assert!(
        outcome.ok,
        "a listing that fills its page is delivered, not refused: {}",
        outcome.error.unwrap_or_default()
    );

    let budget = typecrate_harness::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;
    let allowance = typecrate_harness::tools::files::file_list_allowance();
    let entry_cost =
        typecrate_harness::payload::entry_wire_bytes(&json!(format!("src/{:0<90}.ts", 0)));
    let fits = allowance / entry_cost;

    assert!(
        fits < 400 && fits > 100,
        "the allowance holds {fits} of the 400 entries at {entry_cost} payload bytes each"
    );

    let document: Value =
        serde_json::from_str(&outcome.content).expect("the delivered result is a JSON document");

    assert_eq!(
        document["entries"]
            .as_array()
            .expect("an array of entry names")
            .len(),
        fits,
        "the page stops one entry before the entry that would cross the allowance"
    );
    assert_eq!(
        document["bytes"],
        json!(fits * entry_cost),
        "`bytes` is what the delivered names charged the page allowance"
    );
    assert_eq!(document["truncated"], json!(true));
    assert_eq!(document["nextOffset"], json!(fits));
    assert_eq!(document["limit"], json!(1000));
    assert_eq!(document["contentBudget"], json!(32_768));

    let delivered_payload = result::model_payload_bytes(&outcome.content);

    assert!(
        delivered_payload <= budget,
        "a listing that fills its page is inside the cap it publishes: {delivered_payload} payload \
         bytes against a {budget}-byte budget"
    );
}

#[tokio::test]
async fn a_grep_match_too_large_for_any_page_is_refused_with_the_remedy() {
    let runtime = registry();
    let fixture = Fixture::new("grep-unpageable");

    // One control character is ONE byte in the file and SIX in the delivery currency (`\u0001`,
    // `src/payload.rs`), so a page of them escapes past the allowance that the same number of bytes
    // would fit - exactly the case the reference's cursor guard exists for (`grep.tool.ts:915-933`).
    // `context: 10` then puts eleven of those snippets in the FIRST match, which is more than the
    // whole page allowance: the unit cannot be returned at any offset.
    let line = format!("{}a", "\u{1}".repeat(1_000));
    let text = (0..20).map(|_| format!("{line}\n")).collect::<String>();

    fixture.write("ctrl.txt", &text);

    let budget = typecrate_harness::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;
    let allowance = typecrate_harness::tools::files::grep_page_allowance();

    let outcome = call(
        &runtime,
        "grep",
        json!({ "pattern": "a", "paths": ["ctrl.txt"], "context": 10, "maxResults": 1000 }),
        &fixture.directory,
    )
    .await;

    // THE CALL IS REFUSED, NOT PAGED. An empty page with `nextOffset: 0` would send a caller paging
    // this sequence back to the same entry forever; the refusal names the three arguments that would
    // make the unit small enough to return.
    assert!(
        !outcome.ok,
        "an unpageable unit is a refusal, not an empty page: {}",
        outcome.content
    );

    // The refused unit, rebuilt exactly as `collect_matches` builds it: the matched line's snippet,
    // no `before` (line 1 has no neighbours), and ten lines of `after`. Its cost is the number the
    // sentence quotes, so the sentence is DERIVED here rather than frozen - the same discipline the
    // read refusal above follows.
    let snippet = &line[..1_001.min(line.len())];
    let refused_unit = json!({
        "path": "ctrl.txt",
        "line": 1,
        "text": snippet,
        "textTruncated": false,
        "bytes": snippet.len(),
        "contextBefore": [],
        "contextAfter": vec![json!(snippet); 10],
    });
    let unit_cost = typecrate_harness::payload::entry_wire_bytes(&refused_unit);

    assert!(
        unit_cost > allowance,
        "the fixture's premise: one match costs {unit_cost} payload bytes against a \
         {allowance}-byte page allowance"
    );

    let refusal = format!(
        "grep cannot return \"ctrl.txt\" at line 1: it costs {unit_cost} payload bytes, more than \
         the {allowance}-byte page allowance (the {budget}-byte delivery budget minus {} bytes of \
         result scaffolding). Ask for fewer \"context\" lines, \"output\": \"count\"/\"files\", \
         or read the file directly.",
        budget - allowance
    );

    // The registry prefixes a tool failure with the row that produced it; everything after that
    // prefix is the tool's own sentence, byte for byte.
    let expected = format!("Tool \"grep\" failed: {refusal}");

    assert_eq!(
        outcome.error.as_deref(),
        Some(expected.as_str()),
        "the refusal quotes the measured cost, the allowance, and the scaffolding the allowance \
         subtracted"
    );
    assert_eq!(outcome.content, expected);
    assert!(
        typecrate_harness::payload::utf8_byte_length(&outcome.content) <= budget,
        "and the refusal itself is inside the budget it enforces"
    );
}

#[tokio::test]
async fn a_glob_whose_patterns_fill_the_delivery_budget_is_refused_not_emptied() {
    let runtime = registry();
    let fixture = Fixture::new("glob-pattern-budget");

    // Two patterns of 16,384 characters each: the document echoes them, so the result's FIXED half
    // is already 32,774 bytes and the page allowance - the budget minus that scaffolding - is zero
    // (`glob.tool.ts:381-387`). The directory does not even have to exist: the guard prices only the
    // arguments, so it fires before the walk, and reaching this sentence rather than a
    // directory-not-found sentence IS the ordering proof.
    let patterns = vec![
        format!("**/{}.ts", "a".repeat(16_378)),
        format!("**/{}.ts", "b".repeat(16_378)),
    ];

    let outcome = call(
        &runtime,
        "glob",
        json!({ "patterns": patterns, "paths": ["never-created"] }),
        &fixture.directory,
    )
    .await;

    let budget = typecrate_harness::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;
    let echoed = typecrate_harness::payload::utf8_byte_length(&json!(&patterns).to_string());

    assert!(
        echoed > budget,
        "the fixture's premise: the echoed patterns alone are {echoed} bytes against a \
         {budget}-byte budget"
    );

    let refusal = format!(
        "glob cannot answer this call: its own result scaffolding - {} compiled pattern(s), \
         {echoed} bytes of pattern text the result echoes - fills the whole {budget}-byte delivery \
         budget, leaving no room for a path. Pass fewer or shorter \"patterns\".",
        patterns.len()
    );

    assert!(
        !outcome.ok,
        "a call the patterns already doomed is refused, not answered with an empty listing"
    );
    // Prefixed by the registry the same way every other tool failure is.
    let expected = format!("Tool \"glob\" failed: {refusal}");

    assert_eq!(
        outcome.error.as_deref(),
        Some(expected.as_str()),
        "and the sentence says which argument to shrink, with the measured numbers"
    );
    assert_eq!(outcome.content, expected);
    assert!(
        !outcome.content.contains("never-created"),
        "the walk never ran, so the doomed call never paid for it: {refusal}"
    );
}
