//! The DISPLAY LANE end to end (`docs/issues/minor-rust-port-context-lane-drift.md`, issue 0017).
//!
//! WHAT THIS FILE PINS
//! -------------------
//!   1. the published contract field: `ContextBlock.lane` (`harness.swagger.yaml:8122` — `integer`,
//!      `minimum: 0`, absent means lane 0) decodes, prints back in place, and refuses a non-integer;
//!   2. the durable inputs of the fold survive a read VERBATIM: the per-event `lane` stamp in the
//!      TypeScript server's journal (`durable/events.<sessionId>.jsonl`) and in its checkpoint
//!      (`durable/state.json`), the per-turn starting lane (`turns[].lane`), and the per-conversation
//!      counter (`core.threads[].contextLane`);
//!   3. the fold reproduces the lanes the TypeScript server published for the same records
//!      (`turn-displays.json`), including the two-row case: inside ONE turn the segments that are not
//!      events take the turn's starting lane while the event-driven segments take the events' lane;
//!   4. a session that never moved a lane folds to the SAME BYTES the fold produced before lanes
//!      existed — not "the same document plus a zero", which is the whole point of `LaneStamp`;
//!   5. the durable reader accepts a `contextLane` on a conversation and REFUSES a lane it cannot sort
//!      (negative / fractional / textual), like `core/snapshot.ts:758-763` `readThreads`.
//!
//! THE FIXTURE
//! -----------
//! `tests/fixtures/lane-compaction/` is a copy of the recorded fixture the issue names
//! (`Temp/fixtures/harness/lane-compaction/` at the repo root — git-ignored and disposable, so the
//! bytes this test compares against are vendored HERE, the way `src/plugins.rs:47-50` describes for
//! the plugin-catalogue fixture). It was written by a REAL TypeScript `startHarnessServer` run through
//! one `summarize` compaction; its `MANIFEST.md` carries the provenance and the parity list. The
//! expectations below are read out of those recorded bytes — the issue's criterion is "asserted
//! against a recorded fixture, not restated" — and the extracted lane tables are ALSO pinned as
//! literals, so swapping the fixture cannot silently weaken a comparison.
//!
//! THE SEGMENT SET THIS BUILD MINTS (recorded, not hidden)
//! -------------------------------------------------------
//! The recorded TypeScript display has 10 segments for the seed turn and 4 for the compaction turn.
//! This fold mints 4 and 2 of them: the prompt and instruction segments ARE minted here (they are
//! seeded from the fixture document), but streamed `reasoning`/`text` deltas become prose NODES rather
//! than a bar segment per prose window, so the six prose segments are not in its output — the same
//! stage boundary `README.md` "Context bar" and `projector.rs:1551-1561` record. Parity is therefore
//! asserted over EVERY segment this fold mints (ids and lanes included, so a fold that minted nothing
//! fails), and the fixture's prose segments stay pinned as literals so a later prose-segment port has
//! the recorded lanes to grow into.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::future::BoxFuture;
use serde_json::{Map, Value, json};

use typecrate_harness::agents::AgentCatalogue;
use typecrate_harness::config::HarnessRuntimeSettings;
use typecrate_harness::contract::{
    ContextBlock, ContextBlockKind, HarnessEvent, TurnBlockDetail, TurnOutcome,
};
use typecrate_harness::events::EventHub;
use typecrate_harness::persistence::{
    self, HarnessThreadRecord, LaneStamp, SessionTurn, SessionTurnEvent,
};
use typecrate_harness::ports::{
    Projector, ToolExecCtx, ToolExecOutcome, ToolPolicy, ToolRuntime, TurnExecutor, TurnInput,
};
use typecrate_harness::projector::{TurnProjector, TurnProjectorFactory};
use typecrate_harness::registry::HarnessRegistry;
use typecrate_harness::store::actions::StoreTaskHost;
use typecrate_harness::store::{self, Store, StoreConfig};

/// The one session the recorded fixture captured.
const SESSION_ID: &str = "d8aa98d7-1886-4d3f-a6db-f4869a6623d4";
/// The turn that was already running when the summarize pass raised the conversation's lane: its
/// non-event segments are lane 0 while its event-driven segments are lane 1 — the two-row case.
const SEED_TURN: &str = "af7b4021-15e9-4199-ad1f-4b537d7aa050";
/// The turn that STARTED on lane 1, so every segment of its bar — the prompt included — is lane 1.
const COMPACTION_TURN: &str = "ed1e7c3f-7ce5-496a-a428-986606904267";

// ---------------------------------------------------------------------------
// Fixture access
// ---------------------------------------------------------------------------

/// One file from the vendored fixture, read fresh so no test can hand the next one a mutated value.
fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/lane-compaction")
        .join(name);

    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));

    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("{} is JSON: {error}", path.display()))
}

/// The TypeScript checkpoint the fixture captured, verbatim.
fn document() -> Value {
    fixture("durable/state.json")
}

/// One session's recorded turn list, decoded into the typed durable shape.
fn recorded_turns(session: &Value) -> Vec<SessionTurn> {
    serde_json::from_value(session["turns"].clone()).expect("the recorded turn records decode")
}

/// The recorded turn for one id.
fn recorded_turn(session: &Value, turn_id: &str) -> SessionTurn {
    recorded_turns(session)
        .into_iter()
        .find(|turn| turn.id == turn_id)
        .unwrap_or_else(|| panic!("the fixture records turn {turn_id}"))
}

/// One fold output reduced to the lane table the bar is painted from: `[id, kind, lane]` per segment,
/// in mint order, with an ABSENT lane spelled `null`. That is exactly the shape `turn-displays.json`
/// records, so both sides are compared member for member rather than described.
fn lane_table(display: &Value) -> Vec<Vec<Value>> {
    let mut table: Vec<Vec<Value>> = Vec::new();

    for section in display["sections"].as_array().cloned().unwrap_or_default() {
        for block in section["context"].as_array().cloned().unwrap_or_default() {
            table.push(vec![
                block["id"].clone(),
                block["kind"].clone(),
                block.get("lane").cloned().unwrap_or(Value::Null),
            ]);
        }
    }

    table
}

/// The same table for one turn of the RECORDED TypeScript displays (`turn-displays.json`).
fn recorded_lane_table(turn_id: &str) -> Vec<Vec<Value>> {
    lane_table(&fixture("turn-displays.json")[turn_id]["display"])
}

/// `turnInstructionSources[turnId]`, the recorded instruction sources that make the `system` segment
/// exist at all (`store/mod.rs:1753` feeds the same map to the live fold).
fn instruction_sources(session: &Value, turn_id: &str) -> Vec<Value> {
    session["turnInstructionSources"]
        .get(turn_id)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Folds one recorded turn through the real projector with the recorded per-event stamps, exactly as
/// `TurnState::replay` does (`store/mod.rs:530-543`): the seed's lane is the TURN's starting lane,
/// each `apply` receives that event's own stamp, and the recorded `eventTimes` supply the clocks.
///
/// `lanes` picks the stamp source: `true` replays the recorded stamps and the turn's own lane, `false`
/// suppresses both, which is the pre-feature fold a lane-free document produces.
fn fold_recorded_turn(session: &Value, record: &SessionTurn, lanes: bool) -> Value {
    let mut seed = store::seed_from_turn_record(record);

    seed.instruction_sources = instruction_sources(session, &record.id);

    if !lanes {
        seed.lane = None;
    }

    let mut projector = TurnProjector::seeded(seed);
    let times = record.event_times.clone().unwrap_or_default();

    for (index, entry) in record.events.iter().enumerate() {
        let at = times.get(index).copied().unwrap_or(0);

        projector.apply(
            &entry.event,
            if lanes { entry.lane } else { None },
            at,
            index as u64,
        );
    }

    projector.settle(&record.status, record.updated_at);

    projector.display()
}

/// [`fold_recorded_turn`] returning the LIVE op stream instead of the document, which is where the
/// recorder's stamp is actually consumed: the `context:add` carries the row, and no `context:set`
/// patch may ever carry one (`turn-display.ts ContextBlockPatch`).
fn fold_recorded_turn_ops(session: &Value, record: &SessionTurn) -> Vec<Value> {
    let mut seed = store::seed_from_turn_record(record);

    seed.instruction_sources = instruction_sources(session, &record.id);

    let mut projector = TurnProjector::seeded(seed);
    let times = record.event_times.clone().unwrap_or_default();
    let mut ops: Vec<Value> = Vec::new();

    for (index, entry) in record.events.iter().enumerate() {
        let at = times.get(index).copied().unwrap_or(0);

        ops.extend(projector.apply(&entry.event, entry.lane, at, index as u64));
    }

    projector.settle(&record.status, record.updated_at);

    ops
}

/// Removes every `lane` key from one display document's bar segments: the pre-feature SHAPING, i.e.
/// what the lane-bearing document looks like once the only lane evidence is taken out of it.
///
/// The map is REBUILT rather than edited in place because `serde_json::Map` under `preserve_order`
/// removes a member with a SWAP-REMOVE: it moves the LAST member into the freed slot, which would make
/// this comparison about the removal mechanics instead of about the key. Rebuilding keeps every other
/// member exactly where the fold put it.
fn strip_lane_keys(mut display: Value) -> Value {
    if let Some(sections) = display.get_mut("sections").and_then(Value::as_array_mut) {
        for section in sections {
            if let Some(blocks) = section.get_mut("context").and_then(Value::as_array_mut) {
                for block in blocks {
                    let Some(entries) = block.as_object() else {
                        continue;
                    };

                    let kept: Map<String, Value> = entries
                        .iter()
                        .filter(|(key, _)| *key != "lane")
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();

                    *block = Value::Object(kept);
                }
            }
        }
    }

    display
}

/// Every `lane` key anywhere in a document, as a sorted `path=value` list — the exact way to state
/// "this document writes no lane at all" without a loose `contains` check.
fn every_lane_key(display: &Value) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    let mut stack: Vec<(String, &Value)> = vec![("$".to_owned(), display)];

    while let Some((path, value)) = stack.pop() {
        match value {
            Value::Object(entries) => {
                for (key, nested) in entries {
                    if key == "lane" {
                        found.push(format!("{path}.lane={nested}"));
                    }

                    stack.push((format!("{path}.{key}"), nested));
                }
            }
            Value::Array(entries) => {
                for (index, nested) in entries.iter().enumerate() {
                    stack.push((format!("{path}[{index}]"), nested));
                }
            }
            _ => {}
        }
    }

    found.sort();

    found
}

/// The lane tally of one recorded event list, as `{:?}` of the stamp mapped to a count — the exact
/// shape `lane-evidence.json` reports, so a read that dropped or invented a stamp cannot pass.
fn stamp_tally(events: &[SessionTurnEvent]) -> BTreeMap<String, usize> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();

    for entry in events {
        *counts
            .entry(format!("{:?}", entry.lane).to_uppercase())
            .or_insert(0) += 1;
    }

    counts
}

/// The `[id, kind, lane]` rows this file compares, spelled once per side.
fn rows(entries: &[(&str, &str, Value)]) -> Vec<Vec<Value>> {
    entries
        .iter()
        .map(|(id, kind, lane)| vec![json!(id), json!(kind), lane.clone()])
        .collect()
}

// ---------------------------------------------------------------------------
// Store doubles — a restore reads and folds; it never runs a turn
// ---------------------------------------------------------------------------

struct NoToolsRuntime;

impl ToolRuntime for NoToolsRuntime {
    fn declarations(&self, _policy: &ToolPolicy) -> Vec<typecrate_harness::contract::ToolDecl> {
        Vec::new()
    }

    fn summaries(&self) -> Vec<typecrate_harness::contract::ToolSummary> {
        Vec::new()
    }

    fn catalogue_summaries(&self) -> Vec<typecrate_harness::contract::ToolSummary> {
        Vec::new()
    }

    fn detail(&self, _name: &str) -> Option<typecrate_harness::contract::ToolDetail> {
        None
    }

    fn execute(&self, ctx: ToolExecCtx) -> BoxFuture<'static, ToolExecOutcome> {
        Box::pin(async move {
            ToolExecOutcome {
                ok: false,
                content: ctx.name,
                panels: None,
                media: None,
                error: Some("this store never executes a tool".to_owned()),
                duration_ms: 0,
            }
        })
    }
}

/// A restore never RUNS a turn: it rebuilds the display by replaying the RECORDED events (the same
/// cold-replay path `tests/projector.rs::live_folding_equals_a_cold_replay_of_the_same_events` pins),
/// and the executor is only ever called for a turn this process starts. So any call here means the
/// store is re-running the fixture's history instead of replaying it — it panics loudly rather than
/// quietly folding something else, which would still have produced lane-shaped blocks.
struct NeverRunsExecutor;

impl TurnExecutor for NeverRunsExecutor {
    fn run(&self, _input: TurnInput) -> BoxFuture<'static, TurnOutcome> {
        Box::pin(async move { panic!("a restore must not run a turn") })
    }
}

/// The deployment settings the store is opened with. The fixture's session carries its own endpoint,
/// so these only have to be well-formed.
fn settings() -> HarnessRuntimeSettings {
    HarnessRuntimeSettings {
        base_url: "http://localhost:5500/providers/private/v1".to_owned(),
        api_key: "fixture".to_owned(),
        model: "local/basalt".to_owned(),
    }
}

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A database root holding the fixture checkpoint under `<root>/harness/state.json` — the layout this
/// crate owns and the TypeScript server writes.
struct FixtureRoot {
    directory: PathBuf,
}

impl FixtureRoot {
    fn new(name: &str) -> Self {
        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = typecrate_harness::package_root::repo_root()
            .join("Temp")
            .join("typecrate-harness")
            .join("context-lane")
            .join(format!("{}-{sequence}-{name}", std::process::id()));

        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(directory.join("harness"))
            .expect("a fixture directory is creatable");

        Self { directory }
    }

    /// Writes the fixture checkpoint into the root, after `prepare` has rewritten it — which is how a
    /// lane-free comparison gets a document that never mentions a lane at all.
    fn with_document(self, prepare: impl FnOnce(&mut Value)) -> Self {
        let mut document = document();

        prepare(&mut document);

        std::fs::write(
            self.state_file(),
            serde_json::to_string(&document).expect("the document serializes"),
        )
        .expect("the fixture checkpoint is writable");

        self
    }

    fn state_file(&self) -> PathBuf {
        self.directory
            .join("harness")
            .join(persistence::HARNESS_DATABASE_FILE)
    }

    fn open(&self) -> Store {
        Store::open(
            StoreConfig::at(&self.directory),
            settings(),
            Arc::new(AgentCatalogue::builtin()),
            Arc::new(NoToolsRuntime),
            Arc::new(NeverRunsExecutor),
            TurnProjectorFactory::new(),
            Arc::new(HarnessRegistry::shipped(&settings(), Vec::new())),
            Arc::new(EventHub::new()),
            Some(Arc::new(StoreTaskHost::new())),
        )
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

// ---------------------------------------------------------------------------
// 1. The published contract field (issue criterion 1)
// ---------------------------------------------------------------------------

#[test]
fn the_published_block_carries_the_lane_the_spec_publishes() {
    // The wire shape swagger `:8122` publishes: an OPTIONAL integer with `minimum: 0`, sitting after
    // `sectionId` exactly where the generated TypeScript puts `lane?` (`src/shared/harness-api.ts:3310-3320`).
    let wire = json!({
        "kind": "tool",
        "id": "0-tool-80",
        "turnId": SEED_TURN,
        "sectionId": format!("{SEED_TURN}:0"),
        "lane": 1,
        "name": "toolRequest",
        "chars": 3_495,
    });

    let block: ContextBlock = serde_json::from_value(wire.clone()).expect("a lane decodes");

    assert_eq!(
        block,
        ContextBlock {
            kind: ContextBlockKind::Tool,
            id: "0-tool-80".to_owned(),
            turn_id: SEED_TURN.to_owned(),
            section_id: format!("{SEED_TURN}:0"),
            lane: Some(1),
            name: Some("toolRequest".to_owned()),
            label: None,
            chars: 3_495,
            state: None,
            at: None,
            ended_at: None,
            duration_ms: None,
            args_chars: None,
            result_chars: None,
            in_prompt: None,
            event_from: None,
            event_to: None,
            call_id: None,
        }
    );

    // Printed back it keeps its key AND its position: a decoder that dropped the field would
    // re-write a TypeScript-authored document without the lane.
    assert_eq!(serde_json::to_value(&block).expect("a block prints"), wire);

    // Lane 0 is decodable and stays a written 0 in the TYPE; only the writer rule (`LaneStamp`) turns
    // a 0 back into absence, and it does so on the way out.
    let mut zero_wire = wire.as_object().expect("an object").clone();
    zero_wire.insert("lane".to_owned(), json!(0));

    let zero: ContextBlock =
        serde_json::from_value(Value::Object(zero_wire)).expect("lane 0 decodes");

    assert_eq!(zero.lane, Some(0));

    // A lane a renderer cannot sort is refused at the boundary, never folded into 0.
    for bad in [json!(-1), json!(-0.5), json!(1.5), json!("1"), json!(true)] {
        let mut refusal = wire.as_object().expect("an object").clone();
        refusal.insert("lane".to_owned(), bad.clone());

        assert!(
            serde_json::from_value::<ContextBlock>(Value::Object(refusal)).is_err(),
            "`{bad}` is not an integer >= 0"
        );
    }

    // The block DETAIL route decodes the same field through the flattened shape
    // (`contract::TurnBlockDetail { block }`), so a click on a lane-1 row still knows its row.
    let detail: TurnBlockDetail = serde_json::from_value(json!({
        "kind": "tool",
        "id": "0-tool-80",
        "turnId": SEED_TURN,
        "sectionId": format!("{SEED_TURN}:0"),
        "lane": 1,
        "chars": 3_495,
        "args": "{\"tags\":[\"time\"]}",
    }))
    .expect("a block detail decodes");

    assert_eq!(detail.block.lane, Some(1));
    assert_eq!(detail.args.as_deref(), Some("{\"tags\":[\"time\"]}"));
}

#[test]
fn a_block_with_no_lane_prints_exactly_the_pre_lane_bytes() {
    // Absent means lane 0 and STAYS absent: this is the shape a never-compacted session's bar prints,
    // and what makes criterion 4's byte identity possible at all.
    let wire = json!({
        "kind": "user",
        "id": "0-user-prompt",
        "turnId": SEED_TURN,
        "sectionId": format!("{SEED_TURN}:0"),
        "name": "prompt",
        "chars": 5_657,
    });

    let block: ContextBlock =
        serde_json::from_value(wire.clone()).expect("a laneless block decodes");

    assert_eq!(block.lane, None);
    assert_eq!(serde_json::to_value(&block).expect("a block prints"), wire);
    assert_eq!(
        LaneStamp::read_as(block.lane),
        0,
        "absence READS as lane 0 without ever being written"
    );
}

// ---------------------------------------------------------------------------
// 2. The durable inputs survive the read verbatim (issue criterion 2, inputs)
// ---------------------------------------------------------------------------

#[test]
fn the_compaction_journal_keeps_every_recorded_lane_stamp() {
    // The journal is the TypeScript server's own bytes; the lane rides INSIDE the open event object
    // (`sessions.ts:7936`), which is the shape `persistence::SessionTurnEvent` models.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/lane-compaction/durable")
        .join(format!("events.{SESSION_ID}.jsonl"));
    let text = std::fs::read_to_string(&path).expect("the fixture journal is readable");

    let lines: Vec<Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("every journal line is JSON"))
        .collect();

    // The header record, exactly as recorded.
    assert_eq!(
        lines[0],
        json!({"type": "header", "v": 1, "sessionId": SESSION_ID, "createdAt": 1_794_327_611_696_u64})
    );

    let events: Vec<Value> = lines
        .iter()
        .skip(1)
        .map(|line| {
            assert_eq!(line["type"], json!("event"), "every record is an event");

            line["event"].clone()
        })
        .collect();

    // 337 event records, 336 of them stamped — the figures `lane-evidence.json` records.
    assert_eq!(events.len(), 337);
    assert_eq!(
        fixture("lane-evidence.json")["journal"]["eventsWithLane"],
        json!(336)
    );

    let tally_of = |turn: &str| -> BTreeMap<String, usize> {
        let decoded: Vec<SessionTurnEvent> = lines
            .iter()
            .skip(1)
            .filter(|line| line["turnId"] == json!(turn))
            .map(|line| {
                serde_json::from_value(line["event"].clone()).expect("an event record decodes")
            })
            .collect();

        stamp_tally(&decoded)
    };

    // The seed turn: its first event (`start`, index 0) was recorded BEFORE the summarize pass raised
    // the lane, and each of the other 256 carries `lane: 1`. That is the boundary the bar paints.
    assert_eq!(
        tally_of(SEED_TURN),
        BTreeMap::from([("NONE".to_owned(), 1), ("SOME(1)".to_owned(), 256)])
    );

    // The compaction turn: all 80 events stamped 1.
    assert_eq!(
        tally_of(COMPACTION_TURN),
        BTreeMap::from([("SOME(1)".to_owned(), 80)])
    );

    // VERBATIM: decoding and re-printing one record reproduces the recorded object member for member —
    // key sets included, so nothing was invented and nothing was dropped.
    for event in &events {
        let record: SessionTurnEvent =
            serde_json::from_value(event.clone()).expect("an event record decodes");

        assert_eq!(
            serde_json::to_value(&record).expect("an event record prints"),
            *event,
            "the stamp on a recorded event must survive the read unchanged"
        );
    }

    // The stamp rule itself, spelled once: `Some(0)` and absence are the SAME statement, which is what
    // keeps a never-compacted session's bytes stable (`persistence.rs:570-585`).
    assert_eq!(LaneStamp::of(Some(0)), None);
    assert_eq!(LaneStamp::of(None), None);
    assert_eq!(LaneStamp::of(Some(1)), Some(1));
    assert_eq!(LaneStamp::read_as(None), 0);
    assert_eq!(LaneStamp::read_as(Some(4)), 4);

    // A hand-minted `start` handed `Some(0)` prints no lane key at all.
    let unlaned = SessionTurnEvent::with_lane(
        HarnessEvent::Start {
            agent: "general".to_owned(),
            model: "local/basalt".to_owned(),
            thread: None,
        },
        Some(0),
    );

    assert_eq!(
        serde_json::to_value(&unlaned).expect("a record prints"),
        json!({"type": "start", "agent": "general", "model": "local/basalt"})
    );
}

#[test]
fn the_durable_document_carries_the_turn_and_conversation_lanes() {
    let session = document()["sessions"][0].clone();
    let turns = recorded_turns(&session);

    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0].id, SEED_TURN);
    assert_eq!(turns[1].id, COMPACTION_TURN);

    // The per-turn starting lane: absent for the turn that started on row 0, written for the turn that
    // started on row 1 (`protocol.ts:165`, `persistence.ts` `readTurn`).
    assert_eq!(turns[0].lane, None);
    assert_eq!(turns[1].lane, Some(1));

    // The per-event stamps, straight out of the document this crate restores from.
    assert_eq!(
        stamp_tally(&turns[0].events),
        BTreeMap::from([("NONE".to_owned(), 1), ("SOME(1)".to_owned(), 256)])
    );
    assert_eq!(
        stamp_tally(&turns[1].events),
        BTreeMap::from([("SOME(1)".to_owned(), 80)])
    );

    // A round trip re-writes the document's lane shape exactly: a 0 stays unwritten, a 1 stays a 1, so
    // this crate never re-writes a key the TypeScript server never emitted.
    let printed = serde_json::to_value(&turns).expect("the turn records print");

    assert_eq!(printed[0].get("lane"), None, "lane 0 is never re-written");
    assert_eq!(printed[1]["lane"], json!(1));

    // The per-conversation counter, read through the snapshot reader (`store::restore_threads`).
    let threads = store::restore_threads(&session["core"]);

    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].id, "thread-1");
    assert_eq!(threads[0].context_lane, Some(1));

    // The counter is a durable shape and NOT a wire shape: the recorded conversation ROW carries
    // neither key, which is what the issue's "the Rust API must not publish them either" rule is about.
    assert_eq!(
        fixture("threads-after.json"),
        json!([{
            "id": "thread-1",
            "agent": "general",
            "origin": "host",
            "status": "idle",
            "sequence": 2,
            "label": "GL1",
            "transcript_state": "ok",
            "message_count": 8,
            "chars": 11_520,
            "has_transcript": true,
        }]),
        "the fixture's conversation row is the recorded wire shape"
    );
    assert_eq!(
        every_lane_key(&fixture("threads-after.json")),
        Vec::<String>::new(),
        "a conversation row must publish no lane and no contextLane"
    );
}

// ---------------------------------------------------------------------------
// 3. The fold reproduces the recorded display (issue criterion 2)
// ---------------------------------------------------------------------------

#[test]
fn the_compaction_fixture_folds_to_the_recorded_display_lanes() {
    // FIXTURE INTEGRITY FIRST: the recorded TypeScript lane tables, pinned as literals so the parity
    // comparison below cannot be weakened by a swapped fixture. The `null` entries are ABSENT keys in
    // `turn-displays.json` — lane 0 written as nothing — and their sitting in the SAME turn as the `1`
    // entries is the two-row case this issue exists for.
    assert_eq!(
        recorded_lane_table(SEED_TURN),
        rows(&[
            ("0-system-instructions", "system", Value::Null),
            ("0-user-prompt", "user", Value::Null),
            ("0-reasoning-1", "reasoning", json!(1)),
            ("0-assistant-56", "assistant", json!(1)),
            ("0-tool-80", "tool", json!(1)),
            ("0-reasoning-83", "reasoning", json!(1)),
            ("0-tool-93", "tool", json!(1)),
            ("0-assistant-96", "assistant", json!(1)),
            ("0-reasoning-178", "reasoning", json!(1)),
            ("0-assistant-233", "assistant", json!(1)),
        ])
    );

    assert_eq!(
        recorded_lane_table(COMPACTION_TURN),
        rows(&[
            ("0-system-instructions", "system", json!(1)),
            ("0-user-prompt", "user", json!(1)),
            ("0-reasoning-1", "reasoning", json!(1)),
            ("0-assistant-56", "assistant", json!(1)),
        ])
    );

    let session = document()["sessions"][0].clone();

    // THE SEED TURN — the two-row case. The instruction and prompt segments take the turn's starting
    // lane (0, so no key) while the two tool segments take the events' lane (1), in ONE section.
    assert_eq!(
        lane_table(&fold_recorded_turn(
            &session,
            &recorded_turn(&session, SEED_TURN),
            true
        )),
        rows(&[
            ("0-system-instructions", "system", Value::Null),
            ("0-user-prompt", "user", Value::Null),
            ("0-tool-80", "tool", json!(1)),
            ("0-tool-93", "tool", json!(1)),
        ]),
        "one turn paints two rows: lane 0 for the non-event segments, lane 1 for the event-driven ones"
    );

    // THE COMPACTION TURN — one row, lane 1, because the turn STARTED there: the prompt segment carries
    // a WRITTEN lane, which is the pairing that keeps a post-compaction question and its answer on the
    // same row (`turn-projector.ts:1441 laneOfTurn`).
    assert_eq!(
        lane_table(&fold_recorded_turn(
            &session,
            &recorded_turn(&session, COMPACTION_TURN),
            true
        )),
        rows(&[
            ("0-system-instructions", "system", json!(1)),
            ("0-user-prompt", "user", json!(1)),
        ])
    );

    // PARITY, member for member: for every segment this fold mints, its lane equals the lane the
    // TypeScript server published for that very segment id.
    for turn_id in [SEED_TURN, COMPACTION_TURN] {
        let recorded = recorded_lane_table(turn_id);
        let mine = lane_table(&fold_recorded_turn(
            &session,
            &recorded_turn(&session, turn_id),
            true,
        ));

        let by_id: BTreeMap<String, (Value, Value)> = recorded
            .into_iter()
            .map(|row| {
                (
                    row[0].as_str().expect("an id").to_owned(),
                    (row[1].clone(), row[2].clone()),
                )
            })
            .collect();

        assert!(
            !mine.is_empty(),
            "a fold that mints no segment cannot prove parity for {turn_id}"
        );

        for row in mine {
            let id = row[0].as_str().expect("an id").to_owned();

            assert_eq!(
                by_id.get(&id),
                Some(&(row[1].clone(), row[2].clone())),
                "the lane this fold paints for `{id}` must equal the recorded lane"
            );
        }
    }
}

#[test]
fn a_restored_store_replays_the_recorded_lanes() {
    // The same parity through the durability half the bar actually reads: the TypeScript checkpoint
    // goes in, and the display a RESTART builds comes out. This is how a live session's bar is rebuilt
    // after the process dies, and it is the half the per-event stamp exists for.
    let fixture_root = FixtureRoot::new("replay").with_document(|_| {});
    let store = fixture_root.open();
    let inner = store.lock();
    let session = inner.session(SESSION_ID).expect("the session restores");

    assert_eq!(session.turns_in_order().len(), 2);

    // The conversation's counter came back through the snapshot read, which is the value the NEXT
    // event on this lane gets stamped with (`store/mod.rs:1665-1682` `context_lane_of`). It is read
    // through `store::restore_threads` — the same reader the restore itself uses — because
    // `SessionState::host_thread_records` filters on `origin == "host"`, and a TypeScript snapshot
    // writes no `origin` on a thread (`core/snapshot.ts` `readThreads` whitelists id, agent, status,
    // sequence, ownerThreadId, label, invocationId, contextLane, messages), so that view is empty for
    // a foreign-authored document either way. The lane figure below is what matters here.
    let threads = store::restore_threads(&session.record.core);

    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].id, "thread-1");
    assert_eq!(threads[0].context_lane, Some(1));

    let table = |turn_id: &str| -> Vec<Vec<Value>> {
        lane_table(
            &session
                .turns_in_order()
                .iter()
                .find(|turn| turn.record.id == turn_id)
                .expect("the recorded turn")
                .display(),
        )
    };

    assert_eq!(
        table(SEED_TURN),
        rows(&[
            ("0-system-instructions", "system", Value::Null),
            ("0-user-prompt", "user", Value::Null),
            ("0-tool-80", "tool", json!(1)),
            ("0-tool-93", "tool", json!(1)),
        ])
    );

    assert_eq!(
        table(COMPACTION_TURN),
        rows(&[
            ("0-system-instructions", "system", json!(1)),
            ("0-user-prompt", "user", json!(1)),
        ])
    );

    drop(inner);

    // The restored document keeps the durable shape it arrived with: no lane key on a lane-0 turn, one
    // on the lane-1 turn, and no `contextLane` re-written for a conversation that had none.
    let checkpoint: Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_root.state_file()).expect("the checkpoint is readable"),
    )
    .expect("the checkpoint is JSON");

    let turns = checkpoint["sessions"][0]["turns"]
        .as_array()
        .expect("turns");

    assert_eq!(turns.len(), 2);
    assert_eq!(
        turns[0].get("lane"),
        None,
        "a lane-0 turn still writes no key"
    );
    assert_eq!(turns[1]["lane"], json!(1));
    assert_eq!(
        checkpoint["sessions"][0]["core"]["threads"][0]["contextLane"],
        json!(1)
    );
}

#[test]
fn a_turn_that_never_moved_a_lane_folds_to_the_pre_lane_bytes() {
    let session = document()["sessions"][0].clone();

    for record in recorded_turns(&session) {
        let lanced = fold_recorded_turn(&session, &record, true);
        let laneless = fold_recorded_turn(&session, &record, false);

        // The lane-free fold writes NO lane key anywhere — not a `lane: 0`, not on any segment.
        assert_eq!(
            every_lane_key(&laneless),
            Vec::<String>::new(),
            "a fold with no lane evidence writes no lane key on turn {}",
            record.id
        );

        // And the lane-bearing fold really did write some, or the byte comparison below would be two
        // identical empty claims passing each other.
        assert!(
            !every_lane_key(&lanced).is_empty(),
            "the recorded fold of turn {} must write lanes",
            record.id
        );

        // BYTE-IDENTICAL: taking the lane keys out of the lane-bearing document leaves EXACTLY the
        // document the pre-feature fold produced — every other member, and their order, unchanged.
        assert_eq!(
            serde_json::to_string(&strip_lane_keys(lanced)).expect("a display serializes"),
            serde_json::to_string(&laneless).expect("a display serializes"),
            "suppressing the stamps must reproduce the pre-lane document byte for byte"
        );
    }
}

#[test]
fn the_live_ops_carry_the_lane_and_no_patch_ever_does() {
    // The bar is painted from the OP STREAM while a turn runs, so the stamp has to be visible there
    // too: the `context:add` that opens a segment carries the lane of the event that opened it, and the
    // `context:set` that settles it never carries one (`turn-display.ts ContextBlockPatch`: a lane is
    // mint-assigned and immutable, so a segment changes rows only by being removed and re-minted).
    let session = document()["sessions"][0].clone();
    let record = recorded_turn(&session, SEED_TURN);
    let ops = fold_recorded_turn_ops(&session, &record);

    let added = |id: &str| -> Value {
        ops.iter()
            .find(|op| op["op"] == json!("context:add") && op["block"]["id"] == json!(id))
            .and_then(|op| op.get("block"))
            .cloned()
            .unwrap_or(Value::Null)
    };

    // The two event-driven segments are stamped from their events; the two non-event segments are not
    // stamped at all, because this turn started on lane 0.
    assert_eq!(added("0-tool-80")["lane"], json!(1));
    assert_eq!(added("0-tool-93")["lane"], json!(1));
    assert_eq!(added("0-system-instructions").get("lane"), None);
    assert_eq!(added("0-user-prompt").get("lane"), None);

    // No `context:set` patch in either turn mentions a lane, and no op anywhere writes `lane: 0`.
    for turn_id in [SEED_TURN, COMPACTION_TURN] {
        for op in fold_recorded_turn_ops(&session, &recorded_turn(&session, turn_id)) {
            if op["op"] == json!("context:set") {
                assert_eq!(
                    op["patch"].get("lane"),
                    None,
                    "a block patch never carries a lane: {op}"
                );
            }

            // A written lane is always a REAL lane: this fold never emits `lane: 0`, which is the
            // other half of `LaneStamp` and the reason a pre-compaction session keeps its bytes.
            assert!(
                !every_lane_key(&op)
                    .iter()
                    .any(|entry| entry.ends_with("=0")),
                "no op writes a lane of zero: {op}"
            );
        }
    }

    // LIVE EQUALS REPLAY, one level up: replaying the same records through `TurnState::replay` (the
    // store's own cold fold, `store/mod.rs:530-543`) lands on the same bar the ops painted.
    let replayed = fold_recorded_turn(&session, &record, true);

    assert_eq!(
        lane_table(&replayed),
        rows(&[
            ("0-system-instructions", "system", Value::Null),
            ("0-user-prompt", "user", Value::Null),
            ("0-tool-80", "tool", json!(1)),
            ("0-tool-93", "tool", json!(1)),
        ]),
        "the stream and the cold fold must agree lane for lane"
    );
}

#[test]
fn a_restored_conversation_still_stamps_its_next_event_on_lane_1() {
    // The recorder reads the conversation's counter LIVE, once per event (`sessions.ts:2745`,
    // `:7925-7936`). A restart that lost the counter would stamp the next event 0 and put the next
    // answer back on row 0, which is the live-vs-replay divergence this port exists to prevent.
    let fixture_root = FixtureRoot::new("counter").with_document(|_| {});
    let store = fixture_root.open();
    let inner = store.lock();
    let session = inner.session(SESSION_ID).expect("the session restores");

    assert_eq!(session.context_lane_of(Some("thread-1")), Some(1));
    assert_eq!(
        session.context_lane_of(Some("thread-9")),
        None,
        "an unknown conversation has no lane to state"
    );

    // A document that carries a WRITTEN 0 reads back as absence, so the next stamp is suppressed
    // rather than written as `lane: 0` (`store/mod.rs:1680`, `persistence::LaneStamp`).
    let mut zeroed = document();

    zeroed["sessions"][0]["core"]["threads"][0]["contextLane"] = json!(0);

    drop(inner);

    let zeroed_root = FixtureRoot::new("counter-zero").with_document(|document| {
        *document = zeroed;
    });
    let zeroed_store = zeroed_root.open();
    let zeroed_inner = zeroed_store.lock();

    assert_eq!(
        zeroed_inner
            .session(SESSION_ID)
            .expect("the session restores")
            .context_lane_of(Some("thread-1")),
        None,
        "a written 0 must read back as absence"
    );
}

#[test]
fn the_events_page_carries_the_stamp_verbatim() {
    // The REST view of the same journal (`GET .../turns/{id}/events`) is the OTHER reader of the
    // stamp: `contract::TurnEventRow` carries the stored record, and a read that folded the key away
    // would make a reload draw the whole bar on row 0 (`api-reads.ts:484-490`).
    let fixture_root = FixtureRoot::new("events-page").with_document(|_| {});
    let store = fixture_root.open();
    let inner = store.lock();
    let session = inner.session(SESSION_ID).expect("the session restores");

    let tally = |turn_id: &str| -> BTreeMap<String, usize> {
        let turn = session
            .turns_in_order()
            .into_iter()
            .find(|turn| turn.record.id == turn_id)
            .expect("the recorded turn");

        let (rows, _served, _more) = store::read::turn_event_rows_of(turn, 0, u64::MAX, None);
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();

        for row in &rows {
            *counts
                .entry(format!("{:?}", row.event.lane).to_uppercase())
                .or_insert(0) += 1;
        }

        assert_eq!(
            rows.iter().map(|row| row.index).collect::<Vec<_>>(),
            (0..u64::try_from(rows.len()).expect("a small page")).collect::<Vec<_>>(),
            "a row's index is its position in the turn, so a cursor still addresses it"
        );

        counts
    };

    assert_eq!(
        tally(SEED_TURN),
        BTreeMap::from([("NONE".to_owned(), 1), ("SOME(1)".to_owned(), 256)])
    );
    assert_eq!(
        tally(COMPACTION_TURN),
        BTreeMap::from([("SOME(1)".to_owned(), 80)])
    );
}

// ---------------------------------------------------------------------------
// 4. The durable reader accepts a lane and refuses a lane it cannot sort (issue criterion 3)
// ---------------------------------------------------------------------------

#[test]
fn the_snapshot_reader_accepts_a_lane_and_refuses_a_bad_one() {
    let base = json!({"id": "thread-1", "agent": "general", "status": "idle", "sequence": 1});

    // ACCEPTED: a conversation carrying its counter, camelCased exactly as the TypeScript document
    // spells it (`core/snapshot.ts:674-763` `readThreads`).
    let mut laneled = base.as_object().expect("an object").clone();
    laneled.insert("contextLane".to_owned(), json!(7));

    let accepted: HarnessThreadRecord =
        serde_json::from_value(Value::Object(laneled)).expect("a lane loads");

    assert_eq!(accepted.context_lane, Some(7));
    assert_eq!(
        serde_json::to_value(&accepted).expect("a record prints")["contextLane"],
        json!(7),
        "the member prints back under the camelCase spelling the document uses"
    );

    // A conversation with no lane stays a conversation with no lane, and writes no key.
    let plain: HarnessThreadRecord =
        serde_json::from_value(base.clone()).expect("a laneless conversation loads");

    assert_eq!(plain.context_lane, None);
    assert_eq!(
        serde_json::to_value(&plain)
            .expect("a record prints")
            .get("contextLane"),
        None
    );

    // REFUSED: the spellings `readThreads` throws on — a negative, a fractional, a text, a boolean. A
    // lane a renderer cannot sort is worse than no lane.
    for bad in [json!(-1), json!(-0.5), json!(2.5), json!("2"), json!(true)] {
        let mut refusal = base.as_object().expect("an object").clone();
        refusal.insert("contextLane".to_owned(), bad.clone());

        assert!(
            serde_json::from_value::<HarnessThreadRecord>(Value::Object(refusal)).is_err(),
            "`{bad}` is not an integer >= 0 for `contextLane`"
        );
    }

    // A WRITTEN `0` loads, and the snapshot read NORMALIZES it away, so this crate never re-writes a
    // key the TypeScript server never emitted (`store/mod.rs:2469-2478`, `snapshot.ts:763`).
    let mut zeroed = base.as_object().expect("an object").clone();
    zeroed.insert("contextLane".to_owned(), json!(0));

    let threads = store::restore_threads(&json!({"threads": [zeroed]}));

    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].context_lane, None);
    assert_eq!(
        serde_json::to_value(&threads[0])
            .expect("a record prints")
            .get("contextLane"),
        None,
        "a loaded 0 must not come back as a written key"
    );

    // The per-turn lane goes through the document gate with TypeScript's own sentence
    // (`persistence.ts` `readTurn`: `<label>.lane must be an integer >= 0.`).
    assert_eq!(
        persistence::validate_document(&document()),
        Ok::<(), String>(())
    );

    for bad in [json!(-1), json!(1.5), json!(2.25)] {
        let mut mutated = document();

        mutated["sessions"][0]["turns"][1]["lane"] = bad.clone();

        assert_eq!(
            persistence::validate_document(&mutated),
            Err("sessions[0].turns[1].lane must be an integer >= 0.".to_owned()),
            "a corrupt turn lane fails the load, not the fold"
        );
    }

    // A lane spelled as TEXT fails the same gate, in the number-shaped sentence.
    let mut textual = document();

    textual["sessions"][0]["turns"][1]["lane"] = json!("1");

    assert_eq!(
        persistence::validate_document(&textual),
        Err("sessions[0].turns[1].lane must be a finite number.".to_owned())
    );
}
