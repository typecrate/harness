//! Integration tests for the persistent data plane (T2): the on-disk database, the journal, the
//! definition registry, the agent catalogue, and the host read models.
//!
//! Every expected value below is taken from the TypeScript source of record - the message texts
//! from `src/server/persistence.ts` / `harnesses/registry.ts` / `harnesses/agent-state.ts`, the
//! preset and profile text from `src/server/harnesses/presets.ts` and `src/agents/*`, the team
//! table from `presets.test.ts:180-186`, and the shared toolbox from `presets.test.ts:86-102` - not
//! observed from this Rust build.
//!
//! Every fixture lives under the repo `Temp/typecrate-harness/data-plane/<pid>-<test>/` (repo rule:
//! scratch stays inside the repository) and is removed when the guard drops.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use typecrate_harness::{
    agents::AgentCatalogue,
    config::{self, HarnessRuntimeSettings},
    contract::{self, HarnessEvent, HarnessToolCall, HarnessToolOutcome, UpdateAgentRequest},
    events::EventHub,
    journal::{self, HarnessJournal},
    labels, persistence, read,
    registry::{HarnessRegistry, presets},
};

/// Makes every fixture directory unique per process AND per test, so a parallel `cargo test` run
/// never shares a directory (and one crashed run's leftovers can never poison the next).
static FIXTURE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// One disposable fixture directory, removed on `Drop`.
struct Fixture {
    directory: PathBuf,
}

impl Fixture {
    /// `<repo-root>/Temp/typecrate-harness/data-plane/<pid>-<n>-<test>`.
    fn new(name: &str) -> Self {
        let sequence = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = typecrate_harness::package_root::repo_root()
            .join("Temp")
            .join("typecrate-harness")
            .join("data-plane")
            .join(format!("{}-{sequence}-{name}", std::process::id()));

        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("a fixture directory is creatable");

        Self { directory }
    }

    /// The database ROOT the module under test is pointed at (it appends `harness/` itself).
    fn root(&self) -> &Path {
        &self.directory
    }

    /// Writes one file inside the fixture, creating parent directories.
    fn write(&self, relative: &Path, contents: &str) {
        let path = self.root().join(relative);

        std::fs::create_dir_all(path.parent().expect("a file has a parent"))
            .expect("a fixture directory is creatable");
        std::fs::write(path, contents).expect("a fixture file is writable");
    }

    /// Lists every entry of the harness state directory, sorted, so a staging-file leak is visible.
    fn state_entries(&self) -> Vec<String> {
        let directory = persistence::state_dir(self.root());

        let mut names: Vec<String> = std::fs::read_dir(&directory)
            .expect("the state directory exists")
            .map(|entry| {
                entry
                    .expect("a readable directory entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();

        names.sort();
        names
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// The settings a registry test runs with: the shipped defaults, no credential.
fn settings() -> HarnessRuntimeSettings {
    HarnessRuntimeSettings {
        base_url: "http://localhost:5500/providers/private/v1".to_string(),
        api_key: String::new(),
        model: "merge/ds4-flash".to_string(),
    }
}

/// String names, so a `Vec<String>` field compares against a literal list without noise.
fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|name| (*name).to_string()).collect()
}

/// One minimal-but-valid session record body, as a TS-written document would carry it.
fn session_value(id: &str) -> Value {
    json!({
        "id": id,
        "harness": "instruct",
        "preset": {
            "name": "instruct",
            "title": "Instruct",
            "description": "General agent.",
            "requiresEndpoint": true,
            "primary": "instruct",
            "subagents": ["research"],
            "agents": ["instruct", "research"],
            "tools": ["read"],
            "environmentTools": ["search", "fetch"],
            "builtin": true,
            "editable": true,
            "template": "live",
            "presetNote": "preset-level unknown field"
        },
        "definition": {
            "name": "instruct",
            "title": "Instruct",
            "description": "General agent.",
            "requiresEndpoint": true,
            "primary": "instruct",
            "subagents": ["research"],
            "agents": ["instruct", "research"],
            "tools": ["read", "search", "fetch"],
            "coreTools": ["toolRequest", "task", "delegate", "read"],
            "builtin": true,
            "editable": true
        },
        "settings": { "baseURL": "http://localhost:5500/providers/private/v1", "model": "merge/ds4-flash" },
        "title": "  Named conversation  ",
        "status": "idle",
        "createdAt": 1_700_000_000_000_u64,
        "updatedAt": 1_700_000_000_500_u64,
        "turns": [{
            "id": "turn-1",
            "index": 0,
            "prompt": "hello",
            "threadId": "thread-1",
            "threadLabel": "rr1",
            "status": "done",
            "startedAt": 1_700_000_000_100_u64,
            "updatedAt": 1_700_000_000_200_u64,
            "events": [],
            "eventTimes": [1_700_000_000_200_u64],
            "usage": { "input": 10, "output": 4, "total": 14, "cached": 2 },
            "steers": [{ "id": "steer-1", "at": 1_700_000_000_300_u64, "prompt": "steer me" }],
            "steerTexts": [{ "id": "steer-1", "at": 1_700_000_000_300_u64, "prompt": "steer me" }],
            "turnNote": "turn-level unknown field"
        }],
        "queue": [],
        "threadSequence": 2,
        "labelSeeds": { "turn-1": { "RR": 1 } },
        "labelHighWater": { "RR": 1 },
        "threadLabels": { "thread-1": "rr1" },
        "threadLabelHighWater": { "RR": 1 },
        "turnInvocations": { "turn-1": "inv-1" },
        "turnInstructionSources": { "turn-1": [{ "agent": "instruct", "instructions": ["You are the Instruct agent."] }] },
        "invocationThreads": { "inv-1": "thread-1" },
        "invalidThreads": { "thread-9": { "error": "A transcript must be an array.", "messages": "not an array", "label": "RR2", "agent": "instruct" } },
        "core": { "version": 3, "agents": { "instruct": { "messages": [] } } },
        "agentModels": { "research": "merge/ds4-flash" },
        "sessionAgents": [" research ", "research"],
        "sessionNote": "session-level unknown field"
    })
}

/// 1. A TS-shaped `state.json` survives read -> write -> read with its unknown fields intact and
///    no credential anywhere in the document.
#[test]
fn state_json_round_trips_with_unknown_fields_and_no_credential() {
    let fixture = Fixture::new("round-trip");
    let document = json!({
        "version": 1,
        "savedAt": 1_700_000_000_000_u64,
        "operatorComment": "written by the TS server",
        "registry": [{
            "name": "instruct",
            "title": "Instruct",
            "description": "General agent.",
            "requiresEndpoint": true,
            "primary": "instruct",
            "subagents": ["research", "summarizer"],
            "agents": ["instruct", "research", "summarizer"],
            "tools": ["read"],
            "environmentTools": ["search", "fetch"],
            "agentModels": { "research": "merge/ds4-flash" },
            "agentInstructions": { "instruct": "Be brief." },
            "builtin": true,
            "editable": true,
            "template": "live",
            "registryNote": "preset unknown field"
        }],
        "deleted": ["  demo  ", "demo"],
        "globalSettings": { "baseURL": "http://localhost:5500/providers/private/v1", "model": "merge/ds4-flash" },
        "sessions": [session_value("session-1")],
        "futureBlock": { "kept": true }
    });

    fixture.write(
        &Path::new("harness").join("state.json"),
        &document.to_string(),
    );

    let loaded = persistence::read_harness_database(fixture.root())
        .expect("a well-formed document loads")
        .expect("the file exists");

    // The typed view agrees with the TS field spellings.
    assert_eq!(loaded.version, persistence::HARNESS_DATABASE_VERSION);
    assert_eq!(loaded.saved_at, 1_700_000_000_000_u64);
    assert_eq!(loaded.registry.len(), 1);
    assert_eq!(loaded.deleted, Some(vec!["demo".to_string()]));
    assert_eq!(
        loaded.global_settings,
        Some(persistence::HarnessGlobalSettingsRecord {
            base_url: Some("http://localhost:5500/providers/private/v1".to_string()),
            model: Some("merge/ds4-flash".to_string()),
            extra: serde_json::Map::new(),
        })
    );

    let session = &loaded.sessions[0];
    assert_eq!(session.id, "session-1");
    assert_eq!(session.harness, "instruct");
    // T16: a padded title is TRIMMED, and a blank one would become "no title".
    assert_eq!(session.title.as_deref(), Some("Named conversation"));
    // T27: session agent names are trimmed and de-duplicated.
    assert_eq!(session.session_agents, Some(vec!["research".to_string()]));
    assert_eq!(session.queue, Vec::<String>::new());
    assert_eq!(session.thread_sequence, Some(2));
    assert_eq!(
        session.turn_invocations.as_ref().unwrap()["turn-1"],
        "inv-1"
    );
    assert_eq!(
        session.thread_label_high_water.as_ref().unwrap()["RR"],
        1_u64
    );
    // C4: an unreadable transcript keeps the RAW value it was refused for.
    assert_eq!(
        session.invalid_threads.as_ref().unwrap()["thread-9"].messages,
        Value::String("not an array".to_string())
    );
    // The core snapshot stays opaque data.
    assert_eq!(session.core["agents"]["instruct"]["messages"], json!([]));

    let turn = &session.turns[0];
    assert_eq!(turn.id, "turn-1");
    assert_eq!(turn.status, "done");
    // T14/F4: a persisted label is NORMALISED to the allocator's uppercase spelling.
    assert_eq!(turn.thread_label.as_deref(), Some("RR1"));
    assert_eq!(session.thread_labels.as_ref().unwrap()["thread-1"], "RR1");
    assert_eq!(
        turn.usage,
        Some(contract::HarnessUsage {
            input: 10,
            output: 4,
            total: 14,
            cached: Some(2),
        })
    );
    assert_eq!(turn.steers.as_ref().unwrap()[0].prompt, "steer me");

    // Writing stamps `version`/`savedAt` and keeps everything else, then a second load proves the
    // document is readable by the same rules it was written with.
    persistence::write_harness_database(fixture.root(), &loaded, "test-owner")
        .expect("the commit succeeds");

    let written = persistence::read_harness_database(fixture.root())
        .expect("the written document loads")
        .expect("the file exists");

    assert_eq!(written.sessions, loaded.sessions);
    assert_eq!(written.registry, loaded.registry);
    assert_eq!(written.deleted, loaded.deleted);
    assert!(written.saved_at >= loaded.saved_at);

    // The UNKNOWN fields are still on disk, which is the whole point of the catch-all.
    let raw: Value = serde_json::from_str(
        &std::fs::read_to_string(persistence::state_file_path(fixture.root())).expect("written"),
    )
    .expect("valid JSON");

    assert_eq!(raw["operatorComment"], "written by the TS server");
    assert_eq!(raw["futureBlock"]["kept"], true);
    assert_eq!(raw["registry"][0]["registryNote"], "preset unknown field");
    assert_eq!(
        raw["sessions"][0]["sessionNote"],
        "session-level unknown field"
    );
    assert_eq!(
        raw["sessions"][0]["turns"][0]["turnNote"],
        "turn-level unknown field"
    );
    assert_eq!(
        raw["sessions"][0]["turnInstructionSources"]["turn-1"][0]["instructions"][0],
        "You are the Instruct agent."
    );
    assert_eq!(
        raw["sessions"][0]["labelSeeds"],
        json!({ "turn-1": { "RR": 1 } })
    );

    // SECRETS ARE NEVER WRITTEN (`persistence.ts:20-26`): the whole document carries no key.
    let text =
        std::fs::read_to_string(persistence::state_file_path(fixture.root())).expect("readable");
    assert!(
        !text.contains("apiKey"),
        "the document must not name a credential"
    );
    assert!(
        !text.contains("api_key"),
        "the document must not name a credential"
    );
}

/// 2. The read gate: an absent file is "no database", and every malformed shape is the TS refusal.
#[test]
fn state_read_reports_the_exact_persistence_messages() {
    let fixture = Fixture::new("read-gate");
    let file = persistence::state_file_path(fixture.root());

    // Missing file -> "first run" (`persistence.ts:1548`).
    assert_eq!(
        persistence::read_harness_database(fixture.root()).unwrap(),
        None
    );

    let state_dir = persistence::state_dir(fixture.root());
    std::fs::create_dir_all(&state_dir).expect("the harness directory is creatable");

    // The three gate messages, in the TS evaluation order.
    std::fs::write(&file, "{ not json").expect("writable");
    let malformed = persistence::read_harness_database(fixture.root()).unwrap_err();

    assert_eq!(malformed.status, 400);
    assert!(
        malformed.message.starts_with(&format!(
            "{}: The harness database is not valid JSON: ",
            file.display()
        )),
        "unexpected text: {}",
        malformed.message
    );

    std::fs::write(
        &file,
        json!({ "version": 999, "savedAt": 0, "registry": [], "sessions": [] }).to_string(),
    )
    .expect("writable");
    assert_eq!(
        persistence::read_harness_database(fixture.root())
            .unwrap_err()
            .message,
        format!(
            "{}: The harness database has an invalid shape or version: Unsupported harness database version 999; this server reads version 1.",
            file.display()
        )
    );

    std::fs::write(
        &file,
        json!({ "version": 1, "savedAt": 0, "registry": {}, "sessions": [] }).to_string(),
    )
    .expect("writable");
    assert_eq!(
        persistence::read_harness_database(fixture.root())
            .unwrap_err()
            .message,
        format!(
            "{}: The harness database has an invalid shape or version: The harness database registry must be an array.",
            file.display()
        )
    );

    std::fs::write(
        &file,
        json!({ "version": 1, "savedAt": 0, "registry": [], "sessions": {} }).to_string(),
    )
    .expect("writable");
    assert!(
        persistence::read_harness_database(fixture.root())
            .unwrap_err()
            .message
            .ends_with("The harness database sessions must be an array.")
    );

    // A persisted credential is REFUSED, not dropped (`persistence.ts:889-893`).
    let mut session = session_value("session-key");
    session["settings"]["apiKey"] = json!("sk-secret");

    std::fs::write(
        &file,
        json!({ "version": 1, "savedAt": 0, "registry": [], "sessions": [session] }).to_string(),
    )
    .expect("writable");
    assert!(
        persistence::read_harness_database(fixture.root())
            .unwrap_err()
            .message
            .ends_with("sessions[0].settings must not contain an API key.")
    );

    // And so is a global one (`persistence.ts:1195-1197`).
    std::fs::write(
        &file,
        json!({
            "version": 1,
            "savedAt": 0,
            "registry": [],
            "sessions": [],
            "globalSettings": { "apiKey": "sk-secret" }
        })
        .to_string(),
    )
    .expect("writable");
    assert!(
        persistence::read_harness_database(fixture.root())
            .unwrap_err()
            .message
            .ends_with("The harness database globalSettings must not contain an API key.")
    );

    // A queue that names a turn which is not queued lies about runnable work (`:914-922`).
    let mut dangling = session_value("session-queue");
    dangling["queue"] = json!(["turn-404"]);

    std::fs::write(
        &file,
        json!({ "version": 1, "savedAt": 0, "registry": [], "sessions": [dangling] }).to_string(),
    )
    .expect("writable");
    assert!(
        persistence::read_harness_database(fixture.root())
            .unwrap_err()
            .message
            .ends_with("sessions[0].queue names turn \"turn-404\", which is not in this session.")
    );

    // A label the allocator could never emit fails the load (`:623-643`).
    let mut bad_label = session_value("session-label");
    bad_label["turns"][0]["threadLabel"] = json!("T8");

    std::fs::write(
        &file,
        json!({ "version": 1, "savedAt": 0, "registry": [], "sessions": [bad_label] }).to_string(),
    )
    .expect("writable");
    assert!(
        persistence::read_harness_database(fixture.root())
            .unwrap_err()
            .message
            .ends_with(
                "sessions[0].turns[0].threadLabel must be a conversation label such as \"RR1\"."
            )
    );
}

/// 3. The commit is atomic and leaves nothing behind: exactly one file, no staging leftovers, and
///    repeated commits all succeed.
#[test]
fn atomic_write_leaves_no_staging_file() {
    let fixture = Fixture::new("atomic-write");
    let mut database = persistence::HarnessDatabase::default();
    database.sessions.push(
        serde_json::from_value(session_value("session-atomic"))
            .expect("the fixture session record is valid"),
    );

    for _ in 0..4 {
        persistence::write_harness_database(fixture.root(), &database, "atomic")
            .expect("every commit succeeds");

        // The only file in the namespace is the canonical one - no `.tmp` from any attempt.
        assert_eq!(fixture.state_entries(), vec!["state.json".to_string()]);
    }

    // The document is valid JSON, and the writer stamped both envelope fields.
    let raw: Value = serde_json::from_str(
        &std::fs::read_to_string(persistence::state_file_path(fixture.root())).expect("readable"),
    )
    .expect("the canonical file is complete JSON");

    assert_eq!(raw["version"], persistence::HARNESS_DATABASE_VERSION);
    assert!(raw["savedAt"].is_number());
    assert_eq!(raw["sessions"][0]["id"], "session-atomic");

    // The agents overlay commits through the same primitive, with its own staging prefix.
    let records = vec![persistence::HarnessAgentRecord {
        name: "research".to_string(),
        instruction: Some("Investigate first.".to_string()),
        ..persistence::HarnessAgentRecord::default()
    }];

    persistence::write_harness_agent_records(fixture.root(), &records)
        .expect("the overlay commits");
    assert_eq!(fixture.state_entries(), vec!["agents.json", "state.json"]);

    let parsed =
        persistence::read_harness_agent_records(fixture.root()).expect("the overlay reads");
    assert_eq!(parsed, records);

    // A TS-written BARE ARRAY still parses, so an existing database keeps loading.
    let text =
        std::fs::read_to_string(persistence::agent_file_path(fixture.root())).expect("readable");
    let envelope: Value = serde_json::from_str(&text).expect("valid JSON");
    assert_eq!(envelope["version"], json!(1));
    assert_eq!(envelope["records"].as_array().expect("array").len(), 1);

    let from_array =
        persistence::parse_harness_agent_records(r#"[{"name":"workspace","retired":true}]"#)
            .expect("the TS shape parses");
    assert_eq!(from_array[0].name, "workspace");
    assert_eq!(from_array[0].retired, Some(true));

    // An absent overlay is "no decisions", never an error.
    let fresh = Fixture::new("agents-absent");
    assert_eq!(
        persistence::read_harness_agent_records(fresh.root()).unwrap(),
        Vec::new()
    );
}

/// 4a. An interrupted turn becomes an error row with the fixed explanation, and each unfinished
///     call gets exactly one terminal record.
#[test]
fn repair_closes_running_turns_and_dangling_calls() {
    let mut session: persistence::HarnessSessionRecord =
        serde_json::from_value(session_value("session-running"))
            .expect("the fixture session record is valid");

    session.turns[0].status = "running".to_string();
    session.turns[0].events = vec![
        call_event("call-a", "read"),
        result_event("call-a", "file text", false),
        // The call that was still executing when the process died.
        call_event("call-b", "glob"),
        // A result whose call event never made it to disk.
        HarnessEvent::ToolResult {
            agent: "instruct".to_string(),
            name: "grep".to_string(),
            content: "partial".to_string(),
            is_error: false,
            call_id: None,
            outcome: None,
            ui: Vec::new(),
            media: Vec::new(),
            duration_ms: None,
            pending: Vec::new(),
            thread: None,
        },
    ];

    let repairs = persistence::repair_interrupted_tool_calls(&mut session);

    // 1 closed turn + 1 synthesized result + 1 orphaned result marked.
    assert_eq!(repairs, 3);

    let turn = &session.turns[0];
    assert_eq!(turn.status, "error");
    // The session mirrors the turn, with the fixed sentence (`sessions.ts:3036-3043`).
    assert_eq!(session.status, "error");
    assert_eq!(
        session.last_error.as_deref(),
        Some(persistence::interrupted_turn_message())
    );
    assert_eq!(
        persistence::interrupted_turn_message(),
        "The previous run was interrupted by a server restart; it was not replayed. Review the recorded events before continuing."
    );

    // Exactly one terminal record for `call-b`, and it is a failure with the interrupted text.
    let for_b: Vec<&HarnessEvent> = turn
        .events
        .iter()
        .filter(|event| {
            matches!(
                event,
                HarnessEvent::ToolResult { call_id: Some(id), .. } if id == "call-b"
            )
        })
        .collect();

    assert_eq!(for_b.len(), 1);

    match &for_b[0] {
        HarnessEvent::ToolResult {
            content,
            is_error,
            outcome,
            name,
            agent,
            ..
        } => {
            assert_eq!(content, persistence::INTERRUPTED_TOOL_RESULT_TEXT);
            assert_eq!(
                persistence::INTERRUPTED_TOOL_RESULT_TEXT,
                "The harness stopped while this tool call was running."
            );
            assert!(*is_error, "an interrupted call is a failure");
            assert_eq!(*outcome, Some(HarnessToolOutcome::Cancelled));
            assert_eq!(name, "glob");
            assert_eq!(agent, "instruct");
        }
        other => panic!("expected a synthesized tool_result, got {other:?}"),
    }

    // The already-answered call keeps its recorded result untouched - evidence is never trimmed.
    let for_a: Vec<&HarnessEvent> = turn
        .events
        .iter()
        .filter(|event| {
            matches!(
                event,
                HarnessEvent::ToolResult { call_id: Some(id), .. } if id == "call-a"
            )
        })
        .collect();

    assert_eq!(for_a.len(), 1);
    assert!(matches!(
        for_a[0],
        HarnessEvent::ToolResult { content, is_error: false, .. } if content == "file text"
    ));

    // The orphan is addressable now: `<turnId>:interrupted-result:<index>` (position 3).
    assert_eq!(
        persistence::interrupted_result_call_id("turn-1", 3),
        "turn-1:interrupted-result:3"
    );

    match &turn.events[3] {
        HarnessEvent::ToolResult {
            call_id,
            is_error,
            content,
            ..
        } => {
            assert_eq!(call_id.as_deref(), Some("turn-1:interrupted-result:3"));
            assert!(*is_error, "an orphaned result is marked interrupted");
            assert_eq!(content, "partial");
        }
        other => panic!("the orphan must still be a tool_result, got {other:?}"),
    }

    // Idempotent: a repaired turn has nothing left to close.
    assert_eq!(persistence::repair_interrupted_tool_calls(&mut session), 0);
}

/// 4b. The TS snapshot repair (the transcripts inside `core`) still runs, in place.
#[test]
fn repair_closes_dangling_calls_inside_the_core_snapshot() {
    let mut session: persistence::HarnessSessionRecord =
        serde_json::from_value(session_value("session-core"))
            .expect("the fixture session record is valid");

    session.core = json!({
        "version": 3,
        "agents": {
            "instruct": {
                "messages": [
                    { "role": "user", "content": "go" },
                    { "role": "assistant", "content": "", "toolCalls": [
                        { "id": "one", "name": "read", "arguments": "{}" },
                        { "id": "two", "name": "glob", "arguments": "{}" },
                    ] },
                    { "role": "tool", "content": "answered", "toolCallId": "one", "name": "read" },
                ]
            }
        },
        "threads": [
            { "id": "thread-1", "messages": [
                { "role": "assistant", "content": "", "toolCalls": [
                    { "id": "three", "name": "grep", "arguments": "{}" },
                ] },
            ] }
        ]
    });

    assert_eq!(
        persistence::repair_interrupted_snapshot(&mut session.core),
        2
    );

    // The answered result stands, and exactly ONE synthetic result is appended per missing id.
    assert_eq!(
        session.core["agents"]["instruct"]["messages"],
        json!([
            { "role": "user", "content": "go" },
            { "role": "assistant", "content": "", "toolCalls": [
                { "id": "one", "name": "read", "arguments": "{}" },
                { "id": "two", "name": "glob", "arguments": "{}" },
            ] },
            { "role": "tool", "content": "answered", "toolCallId": "one", "name": "read" },
            { "role": "tool", "content": persistence::INTERRUPTED_TOOL_RESULT_TEXT, "toolCallId": "two", "name": "glob" },
        ])
    );
    assert_eq!(
        session.core["threads"][0]["messages"]
            .as_array()
            .expect("array")
            .len(),
        2
    );

    // Idempotent: a repaired transcript has every id answered.
    assert_eq!(
        persistence::repair_interrupted_snapshot(&mut session.core),
        0
    );
}

/// 5. The journal: append/read from a sequence, tail tolerance, corruption reporting, truncate,
///    rewrite, remove.
#[test]
fn journal_appends_reads_and_reports_problems() {
    let fixture = Fixture::new("journal");
    let sessions = persistence::sessions_dir(fixture.root());
    let mut journal =
        HarnessJournal::open(&sessions, "session-j", None).expect("the journal opens");

    // The header line is TS's, verbatim, so an empty journal is distinguishable from a stray file.
    let header = std::fs::read_to_string(journal.path())
        .expect("readable")
        .lines()
        .next()
        .expect("a header line")
        .to_string();
    let parsed: Value = serde_json::from_str(&header).expect("the header is JSON");

    assert_eq!(
        parsed["type"], "header",
        "the header line names its record type"
    );
    assert_eq!(parsed["v"], json!(journal::HARNESS_JOURNAL_VERSION));
    assert_eq!(parsed["sessionId"], "session-j");
    assert!(parsed["createdAt"].is_number());

    let first = journal.append(&text_event("one"), false).expect("append 1");
    let second = journal.append(&text_event("two"), false).expect("append 2");
    let third = journal
        .append(&text_event("three"), true)
        .expect("append 3 (fenced)");

    assert_eq!((first, second, third), (1, 2, 3));
    assert_eq!(journal.next_sequence(), 4);
    assert!(journal.size() > 0);

    // One event line, exactly: `{version,sequence,timestamp,event}` in that order.
    let recorded = std::fs::read_to_string(journal.path()).expect("readable");
    let lines: Vec<&str> = recorded.lines().collect();

    assert_eq!(lines.len(), 4);
    assert!(
        lines[1].starts_with(&format!(
            "{{\"version\":{},\"sequence\":1,\"timestamp\":",
            journal::HARNESS_JOURNAL_VERSION
        )),
        "unexpected line: {}",
        lines[1]
    );
    assert!(
        lines[1]
            .ends_with(",\"event\":{\"type\":\"text\",\"agent\":\"instruct\",\"delta\":\"one\"}}")
    );

    // Replay from a sequence, inclusive, and bounded by a limit.
    let replay = journal.read(Some(2), None).expect("the journal reads");

    assert_eq!(
        replay
            .iter()
            .map(|record| record["sequence"].as_u64().expect("a sequence"))
            .collect::<Vec<u64>>(),
        vec![2, 3]
    );
    assert_eq!(replay[0]["event"]["delta"], "two");
    assert!(replay[0]["timestamp"].is_number());

    assert_eq!(
        journal
            .read(None, Some(1))
            .expect("bounded read")
            .iter()
            .map(|record| record["sequence"].as_u64().expect("sequence"))
            .collect::<Vec<u64>>(),
        vec![1]
    );

    journal.flush().expect("flush succeeds");

    // A reopening recovers the sequence from the file rather than starting over.
    let reopened = HarnessJournal::open(&sessions, "session-j", None).expect("reopen");
    assert_eq!(reopened.next_sequence(), 4);

    // TAIL TOLERANCE: a last line with no terminating newline is a crash artifact.
    let torn = {
        let mut text = std::fs::read_to_string(journal.path()).expect("readable");
        text.push_str("{\"version\":1,\"sequence\":4,\"timestamp\":1700000000900,\"event\":{\"type\":\"text\",\"agent\":\"instruct\",\"delta\":\"cut");
        text
    };
    std::fs::write(journal.path(), torn).expect("the torn file is written");

    let snapshot = journal.read(None, None).expect("the snapshot fold reads");
    assert_eq!(
        snapshot
            .iter()
            .map(|record| record["sequence"].as_u64().expect("sequence"))
            .collect::<Vec<u64>>(),
        vec![1, 2, 3],
        "the fold drops the incomplete tail silently"
    );

    let (records, problems) = journal
        .read_all_with_problems()
        .expect("the explicit read works");
    assert_eq!(records.len(), 3);
    assert_eq!(problems.len(), 1, "an explicit read reports the tail");
    assert!(
        problems[0].ends_with("(truncated tail)"),
        "got {problems:?}"
    );

    // A corrupt MIDDLE line is always a problem, and the records around it still stand. The kept
    // record is sequence 1, so no spurious gap is reported alongside the corruption.
    let header_line = std::fs::read_to_string(journal.path())
        .expect("readable")
        .lines()
        .next()
        .expect("a header line")
        .to_string();

    std::fs::write(
        journal.path(),
        format!("{header_line}\nnot json at all\n{}\n", lines[1]),
    )
    .expect("written");

    let (records, problems) = journal
        .read_all_with_problems()
        .expect("the read survives corruption");

    assert_eq!(problems.len(), 1, "got {problems:?}");
    assert!(
        problems[0].starts_with("line 2: not valid JSON"),
        "got {problems:?}"
    );
    assert!(
        !problems[0].contains("truncated tail"),
        "a middle line is never a tail"
    );
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["sequence"], json!(1));

    // COMPACTION: rewrite renumbers from 1, truncate_after keeps a prefix.
    journal
        .rewrite(&[text_event("alpha"), text_event("beta")])
        .expect("rewrite succeeds");

    let rewritten = journal.read(None, None).expect("reads back");
    assert_eq!(rewritten.len(), 2);
    assert_eq!(rewritten[0]["sequence"], json!(1));
    assert_eq!(rewritten[1]["event"]["delta"], "beta");
    assert_eq!(journal.next_sequence(), 3);

    journal.truncate_after(1).expect("truncate succeeds");
    let kept = journal.read(None, None).expect("reads back");
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0]["event"]["delta"], "alpha");

    journal.close().expect("close succeeds");

    // REMOVE takes the whole session directory with it.
    journal.remove().expect("remove succeeds");
    assert!(!journal.directory().exists());
    assert!(journal.error().is_none());
}

/// 6. The registry: five shipped presets with the symmetric teams, aliases, create/patch/delete,
///    the tombstone and the restart rule, and the agent validation refusals.
#[test]
fn registry_ships_edits_and_tombstones_the_five_presets() {
    let agents = AgentCatalogue::builtin();
    let registry = HarnessRegistry::shipped(&settings(), Vec::new());

    assert_eq!(
        registry.names(),
        vec!["instruct", "research", "workspace", "reviewer", "planner"]
    );

    // The titles, exactly as `presets.test.ts:141-147` pins them.
    assert_eq!(
        registry
            .cards()
            .iter()
            .map(|card| card.title.clone())
            .collect::<Vec<String>>(),
        vec!["Instruct", "Researcher", "Coder", "Reviewer", "Planner"]
    );

    // One symmetric team per primary (`presets.test.ts:180-186`).
    let expected: [(&str, Vec<String>); 5] = [
        (
            "instruct",
            names(&["research", "workspace", "reviewer", "planner", "summarizer"]),
        ),
        (
            "research",
            names(&["instruct", "workspace", "reviewer", "planner", "summarizer"]),
        ),
        (
            "workspace",
            names(&["instruct", "research", "reviewer", "planner", "summarizer"]),
        ),
        (
            "reviewer",
            names(&["instruct", "research", "workspace", "planner", "summarizer"]),
        ),
        (
            "planner",
            names(&[
                "instruct",
                "research",
                "workspace",
                "reviewer",
                "summarizer",
            ]),
        ),
    ];

    for (name, team) in expected {
        let definition = registry.get(name).expect("the preset is shipped");

        assert_eq!(definition.subagents, team, "team of {name}");
        assert_eq!(
            definition.agents,
            std::iter::once(name.to_string())
                .chain(team.clone())
                .collect::<Vec<String>>()
        );
        assert_eq!(definition.primary, name);
        assert!(definition.builtin);
        assert!(definition.editable);
        assert!(definition.requires_endpoint);
        assert_eq!(definition.environment_tools, names(&["search", "fetch"]));
        assert_eq!(
            definition.tools,
            names(&presets::SHARED_TOOL_NAMES),
            "every live card carries the shared toolbox"
        );
        assert_eq!(presets::team_of(name), team, "team_of({name})");
    }

    // `delegate` is projected for a card WITH a team, and `search`/`fetch` are environment core.
    assert_eq!(
        registry.get("instruct").expect("shipped").core_tools(),
        names(&[
            "toolRequest",
            "task",
            "delegate",
            "read",
            "write",
            "edit",
            "glob",
            "grep",
            "search",
            "fetch",
        ]),
        "the configured live card's declared core set (`presets.test.ts:479-492`)"
    );

    // Aliases resolve for a READ and are never editable identities.
    assert_eq!(registry.find("writer").expect("alias").name, "instruct");
    assert_eq!(registry.find("assistant").expect("alias").name, "instruct");
    assert_eq!(
        registry.resolve("researcher").expect("alias").name,
        "research"
    );
    assert!(registry.get("writer").is_none());
    assert_eq!(
        registry.resolve("nobody").unwrap_err().message,
        "Unknown harness \"nobody\"."
    );

    let mut registry = HarnessRegistry::shipped(&settings(), Vec::new());

    // CREATE: selection only - the toolbox and the provider requirement come from the server.
    let created = registry
        .create(
            contract::CreateHarnessRequest {
                name: "my-team".to_string(),
                title: None,
                description: None,
                primary: "research".to_string(),
                subagents: Some(vec!["reviewer".to_string()]),
                agent_models: Some(std::collections::BTreeMap::from([(
                    "reviewer".to_string(),
                    "merge/ds4-flash".to_string(),
                )])),
                agent_instructions: Some(std::collections::BTreeMap::from([(
                    "research".to_string(),
                    "Cite every claim.".to_string(),
                )])),
                agent_colors: None,
            },
            &agents,
        )
        .expect("a valid selection creates");

    assert_eq!(created.name, "my-team");
    assert_eq!(created.title, "my-team", "an unstated title is the id");
    assert_eq!(created.description, "");
    assert!(!created.builtin);
    assert_eq!(created.agents, vec!["research", "reviewer"]);
    assert_eq!(created.template.as_deref(), Some("live"));
    assert_eq!(registry.names().last().expect("appended"), "my-team");

    // A duplicate name is a 409 - case-insensitively.
    assert_eq!(
        registry
            .create(
                contract::CreateHarnessRequest {
                    name: "INSTRUCT".to_string(),
                    title: None,
                    description: None,
                    primary: "instruct".to_string(),
                    subagents: None,
                    agent_models: None,
                    agent_instructions: None,
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "Harness \"INSTRUCT\" already exists."
    );

    // A legacy alias may not be taken either.
    assert_eq!(
        registry
            .create(
                contract::CreateHarnessRequest {
                    name: "writer".to_string(),
                    title: None,
                    description: None,
                    primary: "instruct".to_string(),
                    subagents: None,
                    agent_models: None,
                    agent_instructions: None,
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "Harness name \"writer\" is reserved as an alias of \"instruct\"."
    );

    // An unknown primary is a 400 that lists the known set (`registry.ts:336-342`).
    let unknown = registry
        .create(
            contract::CreateHarnessRequest {
                name: "ghost-team".to_string(),
                title: None,
                description: None,
                primary: "nobody".to_string(),
                subagents: None,
                agent_models: None,
                agent_instructions: None,
                agent_colors: None,
            },
            &agents,
        )
        .unwrap_err();

    assert_eq!(unknown.status, 400);
    assert_eq!(
        unknown.message,
        "`primary` names unknown agent \"nobody\". Known agents: instruct, research, workspace, reviewer, planner, summarizer."
    );

    // A bad name is refused with the pattern sentence.
    assert_eq!(
        registry
            .create(
                contract::CreateHarnessRequest {
                    name: "9lives".to_string(),
                    title: None,
                    description: None,
                    primary: "instruct".to_string(),
                    subagents: None,
                    agent_models: None,
                    agent_instructions: None,
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "`name` must start with a letter and contain only letters, digits, \"_\" or \"-\", at most 64 characters."
    );

    // An override key that is not a member could never apply, so it is visible.
    assert_eq!(
        registry
            .create(
                contract::CreateHarnessRequest {
                    name: "bad-map".to_string(),
                    title: None,
                    description: None,
                    primary: "instruct".to_string(),
                    subagents: None,
                    agent_models: Some(std::collections::BTreeMap::from([(
                        "planner".to_string(),
                        "m".to_string()
                    )])),
                    agent_instructions: None,
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "`agentModels` names \"planner\", which is not a member of this harness. Members: instruct."
    );

    // PATCH: an empty body changes nothing, so it is a refusal (and it is checked BEFORE the
    // lookup, which is why an unknown name with `{}` still reads 400).
    assert_eq!(
        registry
            .update(
                "instruct",
                contract::UpdateHarnessRequest::default(),
                &agents
            )
            .unwrap_err()
            .message,
        "A patch must change at least one field."
    );
    assert_eq!(
        registry
            .update("writer", contract::UpdateHarnessRequest::default(), &agents)
            .unwrap_err()
            .status,
        400
    );

    let patched = registry
        .update(
            "instruct",
            contract::UpdateHarnessRequest {
                title: Some("Instruct (edited)".to_string()),
                description: None,
                primary: None,
                subagents: None,
                agent_models: None,
                agent_instructions: Some(std::collections::BTreeMap::new()),
                agent_colors: None,
            },
            &agents,
        )
        .expect("a built-in is editable");

    assert_eq!(patched.title, "Instruct (edited)");
    assert_eq!(
        patched.description,
        registry_shipped_description("instruct")
    );
    // A built-in row keeps its shipped identity, and `name` never moved.
    assert_eq!(patched.name, "instruct");
    assert!(patched.builtin);

    // An override key that names an agent this harness does not select is refused on a PATCH too
    // (`my-team` selects research + reviewer only).
    assert_eq!(
        registry
            .update(
                "my-team",
                contract::UpdateHarnessRequest {
                    title: None,
                    description: None,
                    primary: None,
                    subagents: None,
                    agent_models: None,
                    agent_instructions: Some(std::collections::BTreeMap::from([(
                        "planner".to_string(),
                        "x".to_string()
                    )])),
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "`agentInstructions` names \"planner\", which is not a member of this harness. Members: research, reviewer."
    );

    // A blank override VALUE is refused rather than treated as "no override" (`registry.ts:482-487`).
    assert_eq!(
        registry
            .update(
                "my-team",
                contract::UpdateHarnessRequest {
                    title: None,
                    description: None,
                    primary: None,
                    subagents: None,
                    agent_models: None,
                    agent_instructions: Some(std::collections::BTreeMap::from([(
                        "research".to_string(),
                        "   ".to_string()
                    )])),
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "`agentInstructions.research` cannot be empty. Omit the key to run the trusted instruction."
    );

    // A subagent that names the primary, and a repeated subagent, are both refusals.
    assert_eq!(
        registry
            .create(
                contract::CreateHarnessRequest {
                    name: "bad-team".to_string(),
                    title: None,
                    description: None,
                    primary: "research".to_string(),
                    subagents: Some(vec!["research".to_string()]),
                    agent_models: None,
                    agent_instructions: None,
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "Subagent \"research\" is the primary agent; a harness has exactly one primary."
    );
    assert_eq!(
        registry
            .create(
                contract::CreateHarnessRequest {
                    name: "bad-team".to_string(),
                    title: None,
                    description: None,
                    primary: "research".to_string(),
                    subagents: Some(vec!["reviewer".to_string(), "reviewer".to_string()]),
                    agent_models: None,
                    agent_instructions: None,
                    agent_colors: None,
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "Subagent \"reviewer\" is listed more than once."
    );

    // DELETE tombstones a built-in and reports it honestly.
    let receipt = registry.delete("instruct").expect("a shipped card deletes");
    assert_eq!(
        receipt,
        contract::DeleteHarnessResult {
            deleted: true,
            harness_name: "instruct".to_string(),
            tombstoned: true,
            restored_by_restart: false,
        }
    );
    assert_eq!(registry.deleted_names(), vec!["instruct".to_string()]);
    assert!(!registry.names().contains(&"instruct".to_string()));
    // An alias follows its target: `writer` resolves to nothing while `instruct` is gone.
    assert!(registry.find("writer").is_none());
    // A repeat delete says which kind of miss this is.
    assert_eq!(
        registry.delete("instruct").unwrap_err().message,
        "Harness \"instruct\" has already been deleted."
    );
    // A user harness needs no tombstone: its row is simply gone.
    registry
        .delete("my-team")
        .expect("the created card deletes");
    assert_eq!(registry.deleted_names(), vec!["instruct".to_string()]);

    // RESTART: the tombstone suppresses the shipped copy; without it the preset is back.
    let restarted = HarnessRegistry::from_records(
        &settings(),
        registry.to_records(),
        registry.deleted_names(),
        &agents,
    )
    .expect("a saved registry replays");

    assert_eq!(
        restarted.names(),
        vec!["research", "workspace", "reviewer", "planner"]
    );
    assert_eq!(restarted.deleted_names(), vec!["instruct".to_string()]);

    let clean_restart = HarnessRegistry::from_records(&settings(), Vec::new(), Vec::new(), &agents)
        .expect("a fresh registry seeds itself");
    assert_eq!(clean_restart.names().len(), 5);
    assert_eq!(
        clean_restart.get("instruct").expect("re-shipped").title,
        "Instruct"
    );

    // An edited built-in row IS a decision, so a replay keeps the edited title.
    let mut edited = HarnessRegistry::shipped(&settings(), Vec::new());
    edited
        .update(
            "reviewer",
            contract::UpdateHarnessRequest {
                title: Some("Skeptical Lead".to_string()),
                description: None,
                primary: None,
                subagents: None,
                agent_models: None,
                agent_instructions: None,
                agent_colors: None,
            },
            &agents,
        )
        .expect("editable");

    let replayed =
        HarnessRegistry::from_records(&settings(), edited.to_records(), Vec::new(), &agents)
            .expect("the replay reads");

    assert_eq!(
        replayed.get("reviewer").expect("present").title,
        "Skeptical Lead"
    );

    // `referenced_by_agent` is what makes the HTTP layer's 409 possible.
    assert_eq!(
        clean_restart.referenced_by_agent("summarizer"),
        vec!["instruct", "research", "workspace", "reviewer", "planner"]
    );
}

/// T7b: `agent_colors` is the THIRD map of the same shape (`harness-api.ts:454-510`,
/// `registry.ts:564-629`), so it follows the same four rules as `agent_models` and
/// `agent_instructions`: validated against the FINAL member set, trimmed, REPLACE-on-patch with `{}`
/// as the explicit clear, and dropped for a member the patch removes. It is also durable metadata,
/// projected onto the frozen descriptor a session records (`descriptor.ts:117-119`).
#[test]
fn the_agent_color_map_follows_the_same_rules_as_the_other_two() {
    let agents = AgentCatalogue::builtin();
    let mut registry = HarnessRegistry::shipped(&settings(), Vec::new());
    let hub = EventHub::new();

    let colors = |pairs: &[(&str, &str)]| -> std::collections::BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(agent, color)| ((*agent).to_string(), (*color).to_string()))
            .collect()
    };

    let created = registry
        .create(
            contract::CreateHarnessRequest {
                name: "painted".to_string(),
                title: None,
                description: None,
                primary: "research".to_string(),
                subagents: Some(vec!["reviewer".to_string()]),
                agent_models: None,
                agent_instructions: None,
                // A padded value is stored trimmed (`registry.ts:577`), and every accepted shape
                // is 3/4/6/8 hex digits (`registry.ts:164`).
                agent_colors: Some(colors(&[("reviewer", " #0a86ff "), ("research", "#fff")])),
            },
            &agents,
        )
        .expect("a colored harness creates");

    assert_eq!(
        created.agent_colors,
        colors(&[("research", "#fff"), ("reviewer", "#0a86ff")]),
        "the map is validated and trimmed"
    );

    // The four hex-digit form is legal too, and the DEFINITION read emits the map only when it
    // carries something (`api-reads.ts:1099-1106`).
    let definition = created.definition(&hub, &agents);

    assert_eq!(
        definition.agent_colors,
        Some(colors(&[("research", "#fff"), ("reviewer", "#0a86ff")]))
    );

    let uncolored = HarnessRegistry::shipped(&settings(), Vec::new())
        .get("instruct")
        .expect("shipped")
        .definition(&hub, &agents);

    assert_eq!(
        uncolored.agent_colors, None,
        "a shipped template carries no colors, so the read keeps its pre-R2 shape"
    );

    // An unknown member is refused with the membership sentence, and a bad value with the shape
    // sentence - both naming the agent, both BEFORE anything is stored (`registry.ts:604-629`).
    assert_eq!(
        registry
            .create(
                contract::CreateHarnessRequest {
                    name: "miscolored".to_string(),
                    title: None,
                    description: None,
                    primary: "research".to_string(),
                    subagents: None,
                    agent_models: None,
                    agent_instructions: None,
                    agent_colors: Some(colors(&[("planner", "#fff")])),
                },
                &agents,
            )
            .unwrap_err()
            .message,
        "`agentColors` names \"planner\", which is not a member of this harness. Members: research."
    );

    for (value, message) in [
        (
            "reddish",
            "`agentColors.research` must be a CSS hex color (#rgb, #rgba, #rrggbb or #rrggbbaa). Received \"reddish\".",
        ),
        (
            "   ",
            "`agentColors.research` cannot be empty. Omit the key to leave this agent uncolored.",
        ),
        (
            "#ab",
            "`agentColors.research` must be a CSS hex color (#rgb, #rgba, #rrggbb or #rrggbbaa). Received \"#ab\".",
        ),
        (
            "#abcde",
            "`agentColors.research` must be a CSS hex color (#rgb, #rgba, #rrggbb or #rrggbbaa). Received \"#abcde\".",
        ),
    ] {
        let error = registry
            .create(
                contract::CreateHarnessRequest {
                    name: "miscolored".to_string(),
                    title: None,
                    description: None,
                    primary: "research".to_string(),
                    subagents: None,
                    agent_models: None,
                    agent_instructions: None,
                    agent_colors: Some(colors(&[("research", value)])),
                },
                &agents,
            )
            .unwrap_err();

        assert_eq!(error.status, 400, "`{value}` is refused");
        assert_eq!(error.message, message, "`{value}` names the rule");
    }

    // A colors-only PATCH is EXECUTABLE, not prose (`registry.ts:1406-1416`), and it REPLACES the
    // stored map.
    let recolored = registry
        .update(
            "painted",
            contract::UpdateHarnessRequest {
                agent_colors: Some(colors(&[("research", "#000000")])),
                ..contract::UpdateHarnessRequest::default()
            },
            &agents,
        )
        .expect("a colors-only patch is a change");

    assert_eq!(
        recolored.agent_colors,
        colors(&[("research", "#000000")]),
        "the stated map replaces the stored one"
    );

    // `{}` clears every color, and an omitted map keeps the current one while dropping any entry
    // whose member the patch removed (`registry.ts:1377-1389`).
    let cleared = registry
        .update(
            "painted",
            contract::UpdateHarnessRequest {
                agent_colors: Some(std::collections::BTreeMap::new()),
                ..contract::UpdateHarnessRequest::default()
            },
            &agents,
        )
        .expect("the clear is a change");

    assert!(
        cleared.agent_colors.is_empty(),
        "an explicit empty map clears every color"
    );

    let repainted = registry
        .update(
            "painted",
            contract::UpdateHarnessRequest {
                agent_colors: Some(colors(&[("research", "#111"), ("reviewer", "#222")])),
                ..contract::UpdateHarnessRequest::default()
            },
            &agents,
        )
        .expect("repaint");

    let narrowed = registry
        .update(
            "painted",
            contract::UpdateHarnessRequest {
                subagents: Some(Vec::new()),
                ..contract::UpdateHarnessRequest::default()
            },
            &agents,
        )
        .expect("a team change without restating the colors");

    assert_eq!(
        narrowed.agent_colors,
        colors(&[("research", "#111")]),
        "a swatch for the removed member is dropped, not rejected (`registry.ts:1380-1384`)"
    );
    assert_eq!(
        repainted.agent_colors.len(),
        2,
        "the entry the patch did not name keeps its map"
    );

    // Durable both ways: the preset row under `agentColors`, and the FROZEN descriptor a session
    // persists inside its own record (`persistence.ts:131`, `protocol.ts:301`).
    let row = created.to_record();

    assert_eq!(
        row.agent_colors,
        Some(colors(&[("research", "#fff"), ("reviewer", "#0a86ff")])),
        "the preset row carries the map under the document key"
    );
    assert_eq!(
        HarnessRegistry::from_records(&settings(), vec![row.clone()], Vec::new(), &agents)
            .expect("the saved row replays")
            .get("painted")
            .expect("replayed")
            .agent_colors,
        created.agent_colors,
        "a replay rebuilds exactly the map that was saved"
    );

    let frozen = typecrate_harness::registry::descriptor_record_of(&created);

    assert_eq!(
        frozen.agent_colors,
        Some(colors(&[("research", "#fff"), ("reviewer", "#0a86ff")])),
        "the frozen descriptor carries the map, so a saved session paints the same swatches"
    );
    assert_eq!(
        typecrate_harness::registry::definition_from_descriptor_record(&frozen).agent_colors,
        created.agent_colors,
        "and reading the frozen copy back loses nothing"
    );

    // An empty map is written as NO key at all, both on the row and on the descriptor.
    assert_eq!(cleared.to_record().agent_colors, None);
    assert_eq!(
        typecrate_harness::registry::descriptor_record_of(&cleared).agent_colors,
        None
    );

    // A saved built-in row that carries a color is NOT an unmodified shipped default, so the
    // prose-only refresh must not replace it (`registry.ts:979-982`). The record is rebuilt from the
    // LEGACY entry so the only difference under test is the color map.
    let legacy = presets::legacy_preset_definitions()
        .get("instruct")
        .expect("a legacy row")
        .clone();
    let mut saved_builtin = persistence::HarnessPresetRecord {
        name: "instruct".to_string(),
        title: legacy.title.clone(),
        description: legacy.description.clone(),
        requires_endpoint: legacy.requires_endpoint,
        primary: legacy.primary.clone(),
        subagents: legacy.subagents.clone(),
        agents: legacy.agents.clone(),
        tools: legacy.tools.clone(),
        environment_tools: Some(legacy.environment_tools.clone()),
        agent_models: None,
        agent_instructions: None,
        agent_colors: None,
        builtin: true,
        editable: true,
        template: Some(legacy.template.clone()),
        live: None,
        extra: serde_json::Map::new(),
    };

    assert!(
        typecrate_harness::registry::is_legacy_shipped_definition(&legacy, &saved_builtin),
        "an untouched row of the shipped defaults is refreshable"
    );

    saved_builtin.agent_colors = Some(colors(&[("instruct", "#ff0000")]));

    assert!(
        !typecrate_harness::registry::is_legacy_shipped_definition(&legacy, &saved_builtin),
        "a colored built-in row is an operator decision, not a default"
    );
}

/// T7b: every harness TEXT bound is TypeScript's `.length`, i.e. UTF-16 code units
/// (`registry.ts:323-336`, `:432`, `:512`). A scalar count would let 4001 astral glyphs - 8002
/// units - into a bound the TypeScript server refuses at 8000.
#[test]
fn the_harness_text_bounds_are_measured_in_utf16_units() {
    let agents = AgentCatalogue::builtin();
    let mut registry = HarnessRegistry::shipped(&settings(), Vec::new());

    let mut attempt = |name: &str, instruction: String| {
        registry.create(
            contract::CreateHarnessRequest {
                name: name.to_string(),
                title: None,
                description: None,
                primary: "research".to_string(),
                subagents: None,
                agent_models: None,
                agent_instructions: Some(std::collections::BTreeMap::from([(
                    "research".to_string(),
                    instruction,
                )])),
                agent_colors: None,
            },
            &agents,
        )
    };

    // 4000 astral glyphs are exactly 8000 units: inside the bound (`registry.ts:149`).
    assert!(
        attempt("astral-inside", "😀".repeat(4_000)).is_ok(),
        "8000 units is the bound, not a refusal"
    );

    let over = attempt("astral-over", "😀".repeat(4_001)).unwrap_err();

    assert_eq!(over.status, 400);
    assert_eq!(
        over.message,
        "`agentInstructions.research` cannot exceed 8000 characters."
    );

    // The same rule on the title (120) and the description (2000) bounds.
    let mut wide = HarnessRegistry::shipped(&settings(), Vec::new());

    assert_eq!(
        wide.update(
            "instruct",
            contract::UpdateHarnessRequest {
                title: Some("😀".repeat(61)),
                ..contract::UpdateHarnessRequest::default()
            },
            &agents,
        )
        .unwrap_err()
        .message,
        "`title` cannot exceed 120 characters."
    );

    assert_eq!(
        wide.update(
            "instruct",
            contract::UpdateHarnessRequest {
                description: Some("😀".repeat(1_001)),
                ..contract::UpdateHarnessRequest::default()
            },
            &agents,
        )
        .unwrap_err()
        .message,
        "`description` cannot exceed 2000 characters."
    );

    // And on a per-agent model id (200 units, `registry.ts:432`).
    assert_eq!(
        wide.update(
            "instruct",
            contract::UpdateHarnessRequest {
                agent_models: Some(std::collections::BTreeMap::from([(
                    "instruct".to_string(),
                    "😀".repeat(101),
                )])),
                ..contract::UpdateHarnessRequest::default()
            },
            &agents,
        )
        .unwrap_err()
        .message,
        "`agentModels.instruct` cannot exceed 200 characters."
    );
}

/// The shipped description of one preset, read from a fresh registry so a test never hardcodes the
/// prose twice.
fn registry_shipped_description(name: &str) -> String {
    HarnessRegistry::shipped(&settings(), Vec::new())
        .get(name)
        .expect("shipped")
        .description
        .clone()
}

/// 7. The agent catalogue: six profiles with the shipped titles, the composed prompt, and the
///    override/restore rules.
#[test]
fn catalogue_ships_six_profiles_and_replays_operator_edits() {
    let hub = EventHub::new();
    let mut catalogue = AgentCatalogue::builtin();

    assert_eq!(
        catalogue.names(),
        vec![
            "instruct",
            "research",
            "workspace",
            "reviewer",
            "planner",
            "summarizer"
        ]
    );

    // The titles, exactly as `agents/*.ts` states them.
    let titles: Vec<String> = catalogue
        .names()
        .iter()
        .map(|name| catalogue.effective(name).expect("known").title)
        .collect();
    assert_eq!(
        titles,
        vec![
            "Instruct",
            "Researcher",
            "Coder",
            "Reviewer",
            "Planner",
            "Summarizer"
        ]
    );

    // The identity sentence is verbatim, and so is the composition rule.
    assert_eq!(
        catalogue.effective("instruct").expect("known").instruction,
        "You are the Instruct agent. Follow the brief exactly, answer directly, and do only what was asked."
    );

    let composed = catalogue.composed_instruction("summarizer").expect("known");
    assert_eq!(
        composed,
        "You compress technical notes into at most two sentences.\n\nGuidelines:\n- Keep names, numbers, and conclusions.\n- Drop preamble and repetition."
    );
    assert!(
        catalogue
            .composed_instruction("instruct")
            .expect("known")
            .contains("\n\nGuidelines:\n- ")
    );

    // No overlay yet: nothing is overridden, and the record list is empty.
    let summary = catalogue.summary("instruct", &hub).expect("known");
    assert!(!summary.has_instruction_override);
    assert!(!summary.has_global_override);

    // An override REPLACES the shipped text and is durable-decision data.
    let patched = catalogue
        .update(
            "instruct",
            UpdateAgentRequest {
                instruction: Some(Some("Follow the brief and stop.".to_string())),
                ..UpdateAgentRequest::default()
            },
            &hub,
        )
        .expect("a known agent patches");

    assert_eq!(patched.instruction, "Follow the brief and stop.");
    assert_eq!(
        patched.instruction_source,
        contract::InstructionSource::Global
    );
    assert_eq!(
        patched.overridden_fields,
        Some(vec![contract::OverriddenAgentField::Instruction])
    );
    assert_eq!(
        catalogue.effective("instruct").expect("known").instruction,
        "Follow the brief and stop."
    );
    // The guidelines are trusted catalogue lines and are NOT replaced by an instruction edit, so the
    // composed prompt is the new sentence over the FULL shipped bullet list, in order.
    let composed = catalogue.composed_instruction("instruct").expect("known");

    assert!(
        composed.starts_with("Follow the brief and stop.\n\nGuidelines:\n- "),
        "the override replaces the identity sentence only: {composed}"
    );

    let shipped_guidelines = catalogue.shipped("instruct").expect("known").guidelines;

    for guideline in &shipped_guidelines {
        assert!(
            composed.contains(&format!("\n- {guideline}")),
            "the guideline is missing from the prompt: {guideline}"
        );
    }

    // One line per section: the sentence, a blank line, the `Guidelines:` header, then one line per
    // bullet - never a second system message (`agents/index.ts:92-102`).
    assert_eq!(composed.lines().count(), shipped_guidelines.len() + 3);
    assert_eq!(composed.matches("\n- ").count(), shipped_guidelines.len());

    let summary = catalogue.summary("instruct", &hub).expect("known");
    assert!(summary.has_instruction_override);
    assert!(summary.has_global_override);

    // The durable projection carries ONLY the decision.
    let records = catalogue.records();
    assert_eq!(
        records,
        vec![persistence::HarnessAgentRecord {
            name: "instruct".to_string(),
            title: None,
            description: None,
            instruction: Some("Follow the brief and stop.".to_string()),
            guidelines: None,
            retired: None,
            extra: serde_json::Map::new(),
        }]
    );

    // `null` RESTORES the shipped value for that field only.
    let restored = catalogue
        .update(
            "instruct",
            UpdateAgentRequest {
                instruction: Some(None),
                ..UpdateAgentRequest::default()
            },
            &hub,
        )
        .expect("a restore patches");

    assert_eq!(
        restored.instruction,
        "You are the Instruct agent. Follow the brief exactly, answer directly, and do only what was asked."
    );
    assert_eq!(
        restored.instruction_source,
        contract::InstructionSource::Catalogue
    );
    assert_eq!(restored.overridden_fields, None);
    assert!(
        catalogue.records().is_empty(),
        "a rolled-back edit leaves no decision"
    );

    // An override that EQUALS the shipped value is not a decision.
    catalogue
        .update(
            "planner",
            UpdateAgentRequest {
                title: Some(Some("Planner".to_string())),
                ..UpdateAgentRequest::default()
            },
            &hub,
        )
        .expect("an equal write is accepted");
    assert!(catalogue.records().is_empty());

    // Refusals.
    assert_eq!(
        catalogue
            .update(
                "ghost",
                UpdateAgentRequest {
                    title: Some(Some("X".to_string())),
                    ..UpdateAgentRequest::default()
                },
                &hub,
            )
            .unwrap_err()
            .message,
        "Unknown agent \"ghost\"."
    );
    assert_eq!(
        catalogue
            .update("instruct", UpdateAgentRequest::default(), &hub)
            .unwrap_err()
            .message,
        "A patch must change at least one field."
    );
    assert_eq!(
        catalogue
            .update(
                "instruct",
                UpdateAgentRequest {
                    instruction: Some(Some("   ".to_string())),
                    ..UpdateAgentRequest::default()
                },
                &hub,
            )
            .unwrap_err()
            .message,
        "`agent.instruction` cannot be empty. Send null to restore the shipped value."
    );
    assert_eq!(
        catalogue
            .update(
                "instruct",
                UpdateAgentRequest {
                    guidelines: Some(Some(vec!["ok".to_string(), " ".to_string()])),
                    ..UpdateAgentRequest::default()
                },
                &hub,
            )
            .unwrap_err()
            .message,
        "`agent.guidelines[1]` cannot be empty. Send null to restore the shipped value."
    );

    // DELETE retires; a repeat delete is a 404, not a second success.
    let retired = catalogue.delete("summarizer").expect("retire");
    assert_eq!(
        retired,
        contract::DeleteAgentResult {
            deleted: true,
            agent_id: "summarizer".to_string(),
            tombstoned: true,
            restored_by_restart: false,
        }
    );
    assert!(catalogue.is_retired("summarizer"));
    assert!(catalogue.effective("summarizer").is_none());
    assert_eq!(
        catalogue.selectable().len(),
        5,
        "the selectable list drops the retired profile only"
    );
    // A retired profile is still a KNOWN identity, so the durable record survives a restart.
    assert!(catalogue.shipped("summarizer").is_some());
    assert_eq!(
        catalogue.records(),
        vec![persistence::HarnessAgentRecord {
            name: "summarizer".to_string(),
            title: None,
            description: None,
            instruction: None,
            guidelines: None,
            retired: Some(true),
            extra: serde_json::Map::new(),
        }]
    );
    assert_eq!(
        catalogue.delete("summarizer").unwrap_err().message,
        "Agent \"summarizer\" is already retired."
    );
    assert_eq!(
        catalogue.delete("ghost").unwrap_err().message,
        "Unknown agent \"ghost\"."
    );

    // A replayed overlay reproduces the operator's state exactly.
    let replayed_records = catalogue.records();
    let mut replayed = AgentCatalogue::builtin();
    replayed.apply_records(replayed_records);
    assert!(replayed.is_retired("summarizer"));
    assert!(replayed.effective("summarizer").is_none());

    // The registry refuses to SELECT a retired profile, but a definition that already selects it
    // stays patchable (`registry.ts:329-353`, `:1128-1133`).
    let mut registry = HarnessRegistry::shipped(&settings(), Vec::new());
    let refused = registry
        .create(
            contract::CreateHarnessRequest {
                name: "needs-summarizer".to_string(),
                title: None,
                description: None,
                primary: "instruct".to_string(),
                subagents: Some(vec!["summarizer".to_string()]),
                agent_models: None,
                agent_instructions: None,
                agent_colors: None,
            },
            &replayed,
        )
        .unwrap_err();

    assert_eq!(refused.status, 409);
    assert_eq!(
        refused.message,
        "Agent \"summarizer\" is retired and can no longer be selected. Existing harnesses and sessions that already select it are unaffected."
    );

    let patched = registry
        .update(
            "instruct",
            contract::UpdateHarnessRequest {
                title: Some("Still editable".to_string()),
                description: None,
                primary: None,
                subagents: None,
                agent_models: None,
                agent_instructions: None,
                agent_colors: None,
            },
            &replayed,
        )
        .expect("an existing team with a retired member stays patchable");
    assert_eq!(patched.title, "Still editable");
}

/// 8. The instruction ladder: harness override beats the global layer, which beats the catalogue.
#[test]
fn instruction_resolution_follows_harness_global_then_catalogue() {
    let hub = EventHub::new();
    let mut catalogue = AgentCatalogue::builtin();
    let mut registry = HarnessRegistry::shipped(&settings(), Vec::new());

    let shipped = catalogue
        .effective("instruct")
        .expect("known")
        .instruction
        .clone();

    // LAYER 3 (catalogue): nothing overrides anything.
    let (instruction, guidelines, source) = registry
        .instruction_text("instruct", "instruct", &catalogue)
        .expect("a member read");

    assert_eq!(source, "catalogue");
    assert_eq!(instruction, shipped);
    assert_eq!(
        guidelines,
        catalogue.effective("instruct").expect("known").guidelines
    );

    let index = registry
        .instruction_index("instruct", &catalogue)
        .expect("the index reads");
    assert_eq!(index.len(), 6, "one row per member");
    assert_eq!(index[0].agent, "instruct");
    assert_eq!(index[0].source, contract::InstructionSource::Catalogue);
    assert!(!index[0].overridden);
    assert_eq!(
        index[0].chars,
        u64::try_from(shipped.chars().count()).expect("fits")
    );

    // LAYER 2 (global): `PATCH /agents/{id}` is a real layer.
    catalogue
        .update(
            "instruct",
            UpdateAgentRequest {
                instruction: Some(Some("Global operator prompt.".to_string())),
                ..UpdateAgentRequest::default()
            },
            &hub,
        )
        .expect("a stated override");

    let (instruction, _guidelines, source) = registry
        .instruction_text("instruct", "instruct", &catalogue)
        .expect("a member read");

    assert_eq!(source, "global");
    assert_eq!(instruction, "Global operator prompt.");

    let index = registry
        .instruction_index("instruct", &catalogue)
        .expect("index");
    assert_eq!(index[0].source, contract::InstructionSource::Global);
    assert!(
        !index[0].overridden,
        "a global override is not a harness override"
    );
    assert_eq!(
        index[0].chars, 23,
        "the length of the text this member actually runs"
    );

    // LAYER 1 (harness): the per-definition override names THIS team and wins.
    registry
        .update(
            "instruct",
            contract::UpdateHarnessRequest {
                title: None,
                description: None,
                primary: None,
                subagents: None,
                agent_models: None,
                agent_instructions: Some(std::collections::BTreeMap::from([(
                    "instruct".to_string(),
                    "Harness-specific prompt.".to_string(),
                )])),
                agent_colors: None,
            },
            &catalogue,
        )
        .expect("a built-in accepts an override map");

    let (instruction, guidelines, source) = registry
        .instruction_text("instruct", "instruct", &catalogue)
        .expect("a member read");

    assert_eq!(source, "harness");
    assert_eq!(instruction, "Harness-specific prompt.");
    assert_eq!(
        guidelines,
        catalogue.effective("instruct").expect("known").guidelines,
        "an override replaces the identity sentence only"
    );

    let index = registry
        .instruction_index("instruct", &catalogue)
        .expect("index");
    assert_eq!(index[0].source, contract::InstructionSource::Harness);
    assert!(index[0].overridden);

    // A member the harness does not select is a 404 naming the definition. Every shipped profile IS
    // a member of every live preset (the teams are symmetric), so the case that can miss is an id
    // outside the catalogue.
    assert_eq!(
        registry
            .instruction_text("instruct", "ghost", &catalogue)
            .unwrap_err()
            .message,
        "Agent \"ghost\" is not a member of harness \"instruct\"."
    );

    // `{}` clears the map, so every member runs the trusted instruction again - and the global
    // layer is next in line.
    let cleared = registry
        .update(
            "instruct",
            contract::UpdateHarnessRequest {
                title: None,
                description: None,
                primary: None,
                subagents: None,
                agent_models: None,
                agent_instructions: Some(std::collections::BTreeMap::new()),
                agent_colors: None,
            },
            &catalogue,
        )
        .expect("an explicit clear");

    assert!(cleared.agent_instructions.is_empty());

    let (instruction, _, source) = registry
        .instruction_text("instruct", "instruct", &catalogue)
        .expect("a member read");
    assert_eq!(source, "global");
    assert_eq!(instruction, "Global operator prompt.");

    // An alias resolves for an instruction read too.
    let (instruction, _, source) = registry
        .instruction_text("writer", "instruct", &catalogue)
        .expect("the alias resolves");
    assert_eq!(source, "global");
    assert_eq!(instruction, "Global operator prompt.");

    // Restoring the agent layer drops the harness back to the shipped catalogue text.
    catalogue
        .update(
            "instruct",
            UpdateAgentRequest {
                instruction: Some(None),
                ..UpdateAgentRequest::default()
            },
            &hub,
        )
        .expect("a restore");

    let (_, _, source) = registry
        .instruction_text("instruct", "instruct", &catalogue)
        .expect("a member read");
    assert_eq!(source, "catalogue");
}

/// 9. The `/config` document: exactly the contract's four keys, with the writable list and the
///    revision stamped from the hub.
#[test]
fn config_document_has_the_contract_shape() {
    let environment = std::collections::BTreeMap::from([
        (
            "HARNESS_BASE_URL".to_string(),
            "http://localhost:5500/v1".to_string(),
        ),
        ("HARNESS_MODEL".to_string(), "merge/test".to_string()),
        (
            "HARNESS_API_KEY".to_string(),
            "sk-presence-only".to_string(),
        ),
    ]);

    let config = config::load_server_config(&environment).expect("a valid deployment config");
    let hub = EventHub::new();

    let document = read::global_config_document(&config, &hub);
    let body = serde_json::to_value(&document).expect("serializable");

    let mut keys: Vec<&String> = body.as_object().expect("object").keys().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["defaults", "revision", "runtime", "writable_fields"]
    );

    // The credential itself is never sent - only whether one exists.
    assert_eq!(
        body["defaults"],
        json!({
            "base_url": "http://localhost:5500/v1",
            "model": "merge/test",
            "has_api_key": true,
        })
    );

    assert_eq!(
        body["writable_fields"],
        json!(["settings.base_url", "settings.model", "settings.api_key"])
    );

    assert_eq!(
        body["runtime"],
        json!({
            "thread_execution": "inline",
            "worker_idle_ms": 120_000_u64,
            "max_lane_workers": 16,
            "worker_handshake_ms": 60_000_u64,
        })
    );
    assert_eq!(body["revision"], json!(0));

    // `publish`/`bump` is what advances a counter, so a route bump reads back through this builder.
    let revision = hub.bump("config", None);
    let after =
        serde_json::to_value(read::global_config_document(&config, &hub)).expect("serializable");
    assert_eq!(after["revision"], json!(revision));
    assert_eq!(revision, 1);

    // `provider_defaults` keeps the key-free rule for a session or a discovery request too.
    let no_key = serde_json::to_value(read::provider_defaults(&HarnessRuntimeSettings {
        base_url: "http://localhost:5500/v1".to_string(),
        api_key: "   ".to_string(),
        model: "m".to_string(),
    }))
    .expect("serializable");
    assert_eq!(no_key["has_api_key"], json!(false));

    // The model index dedupes in provider order.
    let index = serde_json::to_value(read::model_index(
        vec!["b".to_string(), "a".to_string(), "b".to_string()],
        1_700_000_000_000,
    ))
    .expect("serializable");
    assert_eq!(
        index,
        json!({ "models": ["b", "a"], "observed_at": 1_700_000_000_000_u64 })
    );
}

/// 10. The tool-catalogue projections: defaults, scope, and the availability block.
#[test]
fn tool_rows_project_the_contract_defaults() {
    let hub = EventHub::new();

    // An entry that states nothing still projects a full row (`api-reads.ts:1130-1152`).
    let bare = read::ToolCatalogueEntry::default();
    let summary = read::tool_summary_of(&bare, &hub);

    assert_eq!(
        serde_json::to_value(&summary).expect("serializable"),
        json!({
            "name": "",
            "group": "Created tools",
            "description": "",
            "tags": [],
            "visibility": "searchable",
            "session_bound": false,
        })
    );

    let entry = read::ToolCatalogueEntry {
        name: "read".to_string(),
        group: "Files".to_string(),
        description: "Read files.".to_string(),
        tags: vec!["files".to_string(), "read".to_string()],
        visibility: "core".to_string(),
        session_bound: false,
        parameters: Some(json!({ "type": "object" })),
        availability: Some(read::unconfigured_availability(
            vec!["EXA_API_KEY".to_string()],
            "Set EXA_API_KEY to enable search.".to_string(),
        )),
        manual: true,
    };

    let detail = serde_json::to_value(read::tool_detail_of(&entry, &hub)).expect("serializable");

    assert_eq!(detail["group"], "Files");
    assert_eq!(detail["visibility"], "core");
    assert_eq!(detail["session_bound"], false);
    // `execution` is the closed `{scope, manual}` object (`harness-api.ts:660-664`), never the
    // bare scope string; a non-session-bound entry is `global`, and this one has an
    // implementation so `manual` is true (`api-reads.ts:1158-1162`).
    assert_eq!(
        detail["execution"],
        json!({ "scope": "global", "manual": true }),
        "a non-session-bound tool is global"
    );
    assert_eq!(detail["parameters"], json!({ "type": "object" }));
    assert_eq!(
        detail["availability"],
        json!({
            "state": "unconfigured",
            "environment": ["EXA_API_KEY"],
            "hint": "Set EXA_API_KEY to enable search.",
        })
    );

    let bound = read::ToolCatalogueEntry {
        name: "toolCreate".to_string(),
        session_bound: true,
        // Only an entry with a real implementation can run standalone (`api-reads.ts:1160-1161`);
        // this one states the flag, which is the TS `execution.manual` half.
        manual: true,
        ..read::ToolCatalogueEntry::default()
    };

    assert_eq!(
        serde_json::to_value(read::tool_detail_of(&bound, &hub)).expect("serializable")["execution"],
        json!({ "scope": "session_required", "manual": true })
    );
    // The entry's own flag is the TS `execution.manual` half.
    assert!(bound.manual);

    // A session-bound entry with NO implementation is the TS `{scope, manual:false}` answer:
    // the gate is stated, not inferred by the client.
    let bound_but_not_manual = read::ToolCatalogueEntry {
        name: "sessionAgents".to_string(),
        session_bound: true,
        ..read::ToolCatalogueEntry::default()
    };
    assert_eq!(
        serde_json::to_value(read::tool_detail_of(&bound_but_not_manual, &hub))
            .expect("serializable"),
        json!({
            "name": "sessionAgents",
            "group": "Created tools",
            "description": "",
            "tags": [],
            "visibility": "searchable",
            "session_bound": true,
            "execution": { "scope": "session_required", "manual": false },
        }),
        "the identified catalogue read is the contract's shape, key for key"
    );
}

/// 10b. The execution gate is a closed OBJECT on both detail reads
/// (`harness-api.ts:660-664`, `:1051-1054`, `:1038-1044`), and a card ALWAYS prints `subagents`
/// (`:428`) because an empty array is the fact "delegate is not registered".
#[test]
fn tool_detail_execution_gate_and_card_membership_serialize_exactly() {
    let hub = EventHub::new();

    // The exact JSON the audit requires: `{"execution":{"scope":"session_required","manual":false}}`.
    let session_only = read::ToolCatalogueEntry {
        name: "delegate".to_string(),
        session_bound: true,
        ..read::ToolCatalogueEntry::default()
    };
    let detail = read::tool_detail_of(&session_only, &hub);
    assert_eq!(
        detail.execution,
        contract::ToolExecutionScope {
            scope: contract::ToolExecutionScopeKind::SessionRequired,
            manual: false,
        }
    );
    assert_eq!(
        serde_json::to_value(&detail).expect("serializable")["execution"],
        json!({ "scope": "session_required", "manual": false })
    );

    // A TS-shaped document deserializes into the typed gate, and the bare string does not:
    // `"execution":"global"` is the shape this port used to print, and it is a contract break.
    let wire = json!({
        "name": "read",
        "group": "Files",
        "description": "Read files.",
        "tags": ["files"],
        "visibility": "core",
        "session_bound": false,
        "parameters": { "type": "object" },
        "execution": { "scope": "global", "manual": true },
    });
    let parsed: contract::ToolDetail =
        serde_json::from_value(wire.clone()).expect("the contract shape parses");
    assert_eq!(
        parsed.execution,
        contract::ToolExecutionScope {
            scope: contract::ToolExecutionScopeKind::Global,
            manual: true,
        }
    );
    assert_eq!(serde_json::to_value(&parsed).expect("serializable"), wire);

    let bare_string = json!({
        "name": "read",
        "group": "Files",
        "description": "",
        "tags": [],
        "visibility": "core",
        "session_bound": false,
        "execution": "global",
    });
    let bare_string_error =
        serde_json::from_value::<contract::ToolDetail>(bare_string).unwrap_err();
    assert!(
        bare_string_error
            .to_string()
            .contains("invalid type: string \"global\"")
            && bare_string_error.to_string().contains("ToolExecutionScope"),
        "the bare-scope spelling is refused, not coerced: {bare_string_error}"
    );

    // All three scope spellings keep their exact wire values inside the object
    // (`harness-api.ts:1052`), including the `session_only` answer the session surface uses
    // (`api-reads.ts:900-905`).
    for (spelling, kind) in [
        ("global", contract::ToolExecutionScopeKind::Global),
        (
            "session_required",
            contract::ToolExecutionScopeKind::SessionRequired,
        ),
        (
            "session_only",
            contract::ToolExecutionScopeKind::SessionOnly,
        ),
    ] {
        let gate: contract::ToolExecutionScope = serde_json::from_value(json!({
            "scope": spelling,
            "manual": true,
        }))
        .unwrap_or_else(|error| panic!("{spelling}: {error}"));
        assert_eq!(gate.scope, kind);
        assert_eq!(
            serde_json::to_value(&gate).unwrap(),
            json!({ "scope": spelling, "manual": true })
        );
    }

    // `SessionToolDetail.execution` is optional exactly as TS declares it (`:1043`), and when
    // present it is the same object.
    let session_detail = contract::SessionToolDetail {
        summary: contract::SessionToolSummary {
            tool: contract::ToolSummary {
                name: "delegate".to_string(),
                group: "Control".to_string(),
                description: "Delegate to a subagent.".to_string(),
                tags: vec![],
                visibility: contract::ToolVisibility::Core,
                availability: None,
                session_bound: true,
            },
            session_id: "s1".to_string(),
            declared: true,
            enabled: true,
            origin: contract::SessionToolOrigin::Catalogue,
            r#protected: true,
            source_available: false,
            source_revision: None,
        },
        parameters: None,
        execution: Some(contract::ToolExecutionScope {
            scope: contract::ToolExecutionScopeKind::SessionRequired,
            manual: true,
        }),
    };
    let body = serde_json::to_value(&session_detail).expect("serializable");
    assert_eq!(
        body["execution"],
        json!({ "scope": "session_required", "manual": true })
    );

    let without_gate = contract::SessionToolDetail {
        parameters: None,
        execution: None,
        ..session_detail
    };
    assert!(
        serde_json::to_value(&without_gate)
            .expect("serializable")
            .get("execution")
            .is_none(),
        "the optional member is absent when the row has no gate"
    );

    // A card ALWAYS serializes `subagents` (`harness-api.ts:428`): the empty array is what tells
    // a picker that `delegate` is not registered for this harness.
    let solo = contract::HarnessCard {
        name: "solo".to_string(),
        title: "Solo".to_string(),
        description: "One agent.".to_string(),
        requires_endpoint: true,
        primary: "solo".to_string(),
        subagents: vec![],
        agents: vec!["solo".to_string()],
        tools: vec!["read".to_string()],
        builtin: true,
        definition_revision: 3,
    };
    let card = serde_json::to_value(&solo).expect("serializable");
    assert_eq!(
        card["subagents"],
        json!([]),
        "the key is present when empty"
    );
    assert!(
        card.get("subagents").is_some(),
        "an empty team is a statement, not an omission"
    );

    // Deserialization still TOLERATES the key's absence (an old reader or a hand-written row),
    // so removing `skip_serializing_if` did not make the type strict on input.
    let mut sparse = card.as_object().expect("object").clone();
    sparse.remove("subagents");
    let parsed: contract::HarnessCard =
        serde_json::from_value(Value::Object(sparse)).expect("absence still deserializes");
    assert_eq!(parsed, solo);

    // The registry's own cards carry the team they declare, and a solo preset still prints the
    // empty array (`presets.ts`: only the teamed presets register `delegate`).
    for preset in presets::shipped_presets() {
        let card = read::harness_card_of_revision(&preset, 0);
        let body = serde_json::to_value(&card).expect("serializable");
        assert_eq!(
            body["subagents"],
            json!(card.subagents),
            "{} always states its team",
            preset.name
        );
    }
}

/// 11. The label grammar and allocator, which the persistence reader and the store both key on.
#[test]
fn labels_validate_and_allocate_like_the_reference_grammar() {
    // `research` -> RH, `instruct` -> IT: the code keys on the agent ID (`agent-results.ts:194-201`).
    assert_eq!(labels::thread_label("research", 4), "RH4");
    assert_eq!(labels::thread_label("instruct", 1), "IT1");
    assert_eq!(
        labels::thread_label("99-99", 2),
        "AG2",
        "no ASCII letters falls back to `AG`"
    );

    // The typed spelling and a bare label normalize to the same thing.
    let parts = labels::agent_label_parts("#it1").expect("a valid label");
    assert_eq!(parts.label, "IT1");
    assert_eq!(parts.code, "IT");
    assert_eq!(parts.ordinal, 1);

    for rejected in ["", "RR0", "R1", "RRR1", "R1X", "T8", "rr"] {
        assert!(
            labels::agent_label_parts(rejected).is_none(),
            "{rejected} is not an emittable label"
        );
    }

    let mut allocator = labels::LabelAllocator::new();
    assert_eq!(allocator.next("research"), "RH1");
    assert_eq!(
        allocator.next("rhesearch"),
        "RH2",
        "the sequence is keyed by the CODE"
    );
    assert_eq!(allocator.next("instruct"), "IT1");
    assert_eq!(
        allocator.counters(),
        std::collections::BTreeMap::from([("RH".to_string(), 2_u64), ("IT".to_string(), 1_u64),])
    );

    // A restored conversation RESERVES its own ordinal without minting a new one, and a seed never
    // lowers a counter.
    let mut restored = labels::LabelAllocator::with_seed(&std::collections::BTreeMap::from([(
        "RH".to_string(),
        3_u64,
    )]));
    assert_eq!(restored.reserve("RH3").expect("valid"), 3);
    assert_eq!(restored.next("research"), "RH4");
    restored.seed(&std::collections::BTreeMap::from([(
        "RH".to_string(),
        1_u64,
    )]));
    assert_eq!(restored.counters()["RH"], 4);
}

/// A `text` event with a known delta, for journal round-trips.
fn text_event(delta: &str) -> HarnessEvent {
    HarnessEvent::Text {
        agent: "instruct".to_string(),
        delta: delta.to_string(),
        thread: None,
    }
}

/// A recorded `tool_call` event.
fn call_event(id: &str, name: &str) -> HarnessEvent {
    HarnessEvent::ToolCall {
        agent: "instruct".to_string(),
        call: HarnessToolCall {
            id: id.to_string(),
            name: name.to_string(),
            arguments: "{}".to_string(),
        },
        thread: None,
    }
}

/// A recorded `tool_result` event for one call.
fn result_event(id: &str, content: &str, is_error: bool) -> HarnessEvent {
    HarnessEvent::ToolResult {
        agent: "instruct".to_string(),
        name: "read".to_string(),
        content: content.to_string(),
        is_error,
        call_id: Some(id.to_string()),
        outcome: Some(if is_error {
            HarnessToolOutcome::Failed
        } else {
            HarnessToolOutcome::Completed
        }),
        ui: Vec::new(),
        media: Vec::new(),
        duration_ms: Some(4),
        pending: Vec::new(),
        thread: None,
    }
}
