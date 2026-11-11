//! TEMPORARY PROBE (deleted after reading): prints the real block bytes this port's projector
//! produces, so the block-write joins are written against actual display JSON.
use harness::projector::{Projector, ProjectorConfig};
use harness::{contract::HarnessEvent, Json};

#[test]
fn probe_blocks() {
    let mut projector = Projector::new(&ProjectorConfig::default());
    let text = "I will read the file first.\n\nThen summarize.";
    let r1 = "Reading config";
    let r2 = "Now summarizing";
    let out = "The config sets two agents.";
    projector
        .begin_window(
            "general",
            Json::from_str(&serde_json::to_string(&HarnessEvent::Input {
                content: "go".into(),
                ..Default::default()
            })
            .unwrap())
            .unwrap(),
            0,
        )
        .unwrap();
    projector.process(&Json::from_str(&serde_json::to_string(&HarnessEvent::ThinkingDelta { delta: r1.into(), ..Default::default() }).unwrap()).unwrap(), 2).unwrap();
    projector.process(&Json::from_str(&serde_json::to_string(&HarnessEvent::TextDelta { delta: "I will read the file first.".into(), ..Default::default() }).unwrap()).unwrap(), 3).unwrap();
    projector.process(&Json::from_str(&serde_json::to_string(&HarnessEvent::ToolCall { tool_call_id: "call_a".into(), name: "read".into(), args: "{}".into(), ..Default::default() }).unwrap()).unwrap(), 8).unwrap();
    projector.process(&Json::from_str(&serde_json::to_string(&HarnessEvent::ToolResult { tool_call_id: "call_a".into(), name: "read".into(), output: "ok".into(), is_error: false, ..Default::default() }).unwrap()).unwrap(), 12).unwrap();
    projector.process(&Json::from_str(&serde_json::to_string(&HarnessEvent::ThinkingDelta { delta: r2.into(), ..Default::default() }).unwrap()).unwrap(), 14).unwrap();
    projector.process(&Json::from_str(&serde_json::to_string(&HarnessEvent::TextDelta { delta: "\n\nThen summarize.".into(), ..Default::default() }).unwrap()).unwrap(), 15).unwrap();
    projector.end_window(16, 0).unwrap();
    let display = projector.snapshot_display().unwrap();
    println!("DISPLAY {}", serde_json::to_string_pretty(&display).unwrap());
    let _ = (text, out);
}
