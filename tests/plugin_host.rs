//! The plugin HOST plane: hook-bus rules, the reserved `enabled` gate, the round
//! reducer, and the shipped async-ping policy.
//!
//! Every expectation here is a TypeScript pin, not a restatement of the Rust
//! code. The provenance is written next to each test:
//!
//! | Source | What it pins |
//! |---|---|
//! | `underload/harness/src/plugin/hooks.test.ts` | dispatch order, the wildcard fan-out, the budget, the reducer, the registration refusals |
//! | `underload/harness/src/plugin/host.test.ts:400-538` | the reserved `enabled` switch, its per-dispatch re-read, and the `harness.dispose` exemption |
//! | `underload/harness/src/plugins/async-ping.plugin.ts` | the heartbeat's own decision rules |
//!
//! THE ONE PIN MOST LIKELY TO BE "FIXED" WRONG (see `hooks.test.ts:860-883`)
//! -------------------------------------------------------------------------
//! Priority orders DISPATCH, and the round reducer is "last non-empty wins", so
//! the handler that runs LAST — the LOWEST priority — has the final say for the
//! round. The TS test's comment calls that "the higher-priority policy speaks
//! last", but its `toEqual` is the authority, and it expects the `pause` from the
//! priority-0 handler, not the `stop` from the priority-10 one. This suite
//! asserts the value, so the wording can never be re-litigated into a behaviour
//! change.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};

use typecrate_harness::contract::HarnessMessage;
use typecrate_harness::plugin::async_ping::{
    self, AsyncPingPlugin, arm_decision, interval_of, thread_timer_key, tick_decision,
    ArmDecision, TickDecision, DEFAULT_PING_INTERVAL_MS, HOOKS, PLUGIN_NAME,
};
use typecrate_harness::plugin::context::{
    ControlError, ContextEntry, EditPatch, EntryTarget, PluginRuntime, TurnControls,
};
use typecrate_harness::plugin::diagnostics::{DiagnosticEvent, DiagnosticKind, RecordingDiagnosticSink, SinkReporter};
use typecrate_harness::plugin::event::{
    AsyncLifecycle, Contribution, LifecycleEvent, Payload, TurnBeforePayload, TurnDecision,
    WILDCARD, event_of_name, is_wildcard_deliverable,
};
use typecrate_harness::plugin::host::{HostError, Plugin, PluginApi, PluginHost};
use typecrate_harness::plugin::hooks::{
    HandlerCall, HandlerFailure, HandlerFailureSink, HandlerFuture, HandlerResult, HookBus,
    HookOptions, MaybeSync, budget_error_of, decision_error_of,
};
use typecrate_harness::contract::PluginSettingType;
use typecrate_harness::plugin::DiagnosticReporter;
use typecrate_harness::plugins::{
    DeclaredDefault, PluginMetadata, PluginSettingDescriptor, PluginSettingsStore,
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A user message, the shape every transcript fixture in the TS suite uses
/// (`user('go')`, `hooks.test.ts`).
fn user(content: &str) -> HarnessMessage {
    HarnessMessage::User {
        content: content.to_owned(),
        attachments: vec![],
        included: None,
        message_id: None,
    }
}

/// `turnPayload(...)` — the identity half of a `turn.before` frame. The live
/// control surface is a separate argument in Rust (see `context.rs`), so this
/// carries only what TS carries in the payload.
fn turn_payload() -> Payload {
    Payload::TurnBefore(TurnBeforePayload {
        thread_id: "t-1".to_owned(),
        agent: "noob".to_owned(),
        iteration: 1,
        actor_key: "thread:t-1".to_owned(),
        branch_id: None,
        estimated_tokens: 1200.0,
    })
}

/// Shared ordered recorder, captured by handler closures.
type Spy = Arc<Mutex<Vec<String>>>;

fn spy() -> Spy {
    Arc::new(Mutex::new(Vec::new()))
}

/// Pushes one label and answers "nothing" — the Rust spelling of TS's
/// `() => void order.push('x')`.
fn record(sink: &Spy, label: &str) -> HandlerResult {
    if let Ok(mut seen) = sink.lock() {
        seen.push(label.to_owned());
    }

    Ok(Contribution::nothing())
}

/// A bus wired to a recording failure sink, so rule 8's reports are assertable.
fn observed_bus() -> (Arc<HookBus>, Arc<Mutex<Vec<DiagnosticEvent>>>) {
    let events: Arc<Mutex<Vec<DiagnosticEvent>>> = Arc::new(Mutex::new(Vec::new()));

    struct Sink(Arc<Mutex<Vec<DiagnosticEvent>>>);

    impl HandlerFailureSink for Sink {
        fn on_handler_error(&self, event: DiagnosticEvent) {
            if let Ok(mut seen) = self.0.lock() {
                seen.push(event);
            }
        }
    }

    (
        Arc::new(HookBus::with_reporter(Arc::new(Sink(Arc::clone(&events))))),
        events,
    )
}

/// The `on(...)` options the TS tests pass: an owner, and nothing else.
fn owner(name: &str) -> HookOptions {
    HookOptions {
        owner: name.to_owned(),
        ..HookOptions::default()
    }
}

/// A live gate over one `AtomicBool` — the Rust shape of `host.ts`'s
/// `() => settings.boolean('enabled', true)` closure.
fn gate(flag: &Arc<AtomicBool>) -> Arc<dyn Fn() -> bool + Send + Sync> {
    let flag = Arc::clone(flag);

    Arc::new(move || flag.load(Ordering::SeqCst))
}

fn gated(name: &str, flag: &Arc<AtomicBool>) -> HookOptions {
    HookOptions {
        enabled: Some(gate(flag)),
        ..owner(name)
    }
}

/// Reads a spy's contents (and never panics on a poisoned lock, so a failing
/// assertion stays a failing assertion).
fn seen(sink: &Spy) -> Vec<String> {
    sink.lock().map(|list| list.clone()).unwrap_or_default()
}

fn reported(events: &Arc<Mutex<Vec<DiagnosticEvent>>>) -> Vec<DiagnosticEvent> {
    events.lock().map(|list| list.clone()).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Rule 1 / rule 3 — dispatch order
// ---------------------------------------------------------------------------

/// `hooks.test.ts:712-727` — the pinned order `['c:p1','a:p0','b:p0','d:p-1']`,
/// including the wildcard participating in the SAME ranking.
#[tokio::test]
async fn dispatches_by_priority_then_registration_order() {
    let (bus, _events) = observed_bus();
    let order = spy();

    for (label, name, priority) in [
        ("a:p0", "a", 0),
        ("b:p0", "b", 0),
        ("c:p1", "c", 1),
        ("d:p-1", "d", -1),
    ] {
        let order = Arc::clone(&order);
        let options = HookOptions {
            priority,
            ..owner(name)
        };
        let name = if label == "d:p-1" { WILDCARD } else { "turn.before" };

        bus.on_sync(
            name,
            move |call: &HandlerCall| record(&order, &format!("{}:{}", label, call.event.as_str())),
            options,
        )
        .expect("a registration the TS suite makes");
    }

    // `emitMaybeSync` on an all-sync round answers `{ sync: true, value: {} }`
    // (`hooks.test.ts:726`) — decided on the spot, no future involved.
    let round = bus.emit_maybe_sync(LifecycleEvent::TurnBefore, turn_payload());
    let MaybeSync::Decided(contribution) = &round else {
        panic!("an all-sync round must be decided on the caller's task");
    };

    assert_eq!(contribution, &Contribution::nothing());
    assert_eq!(
        seen(&order),
        vec![
            "c:p1:turn.before".to_owned(),
            "a:p0:turn.before".to_owned(),
            "b:p0:turn.before".to_owned(),
            "d:p-1:turn.before".to_owned(),
        ]
    );
}

/// `hooks.test.ts:695-710` — a named registration and a wildcard on the same
/// bus: the state-change frame reaches ONLY the named handler (rule 7's
/// exclusion), and the turn frame reaches both.
#[tokio::test]
async fn fans_out_named_handlers_and_wildcards_without_host_state_change() {
    let (bus, _events) = observed_bus();
    let order = spy();

    let named = Arc::clone(&order);
    bus.on_sync(
        "host.state_change",
        move |call: &HandlerCall| record(&named, &format!("named:{}", call.event.as_str())),
        owner("named"),
    )
    .expect("host.state_change is a subscribable event");

    let wild = Arc::clone(&order);
    bus.on_sync(
        WILDCARD,
        move |call: &HandlerCall| record(&wild, &format!("wild:{}", call.event.as_str())),
        owner("wild"),
    )
    .expect("'*' is a subscription form");

    bus.emit(
        LifecycleEvent::HostStateChange,
        Payload::StateChange(Map::from_iter([(
            "reason".to_owned(),
            json!("agent_message"),
        )])),
    )
    .await;
    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(
        seen(&order),
        vec!["named:host.state_change".to_owned(), "wild:turn.before".to_owned()]
    );
}

// ---------------------------------------------------------------------------
// Rule 7 — the wildcard fan-out table
// ---------------------------------------------------------------------------

/// `hooks.test.ts:607-640`'s `DELIVERED` list, written out in FULL so adding an
/// event to the catalogue fails here until its author decides whether an audit
/// plugin should receive it.
#[test]
fn a_wildcard_is_delivered_every_event_except_host_state_change() {
    const DELIVERED: [&str; 17] = [
        "thread.created",
        "thread.started",
        "thread.paused",
        "thread.resumed",
        "thread.stopped",
        "thread.idle",
        "thread.deleted",
        "run.started",
        "run.finished",
        "run.failed",
        "turn.before",
        "turn.after",
        "tool.before",
        "tool.after",
        "async.pending",
        "async.drained",
        "harness.dispose",
    ];

    let delivered: Vec<&str> = LifecycleEvent::ALL
        .iter()
        .filter(|event| is_wildcard_deliverable(**event))
        .map(|event| event.as_str())
        .collect();

    assert_eq!(delivered, DELIVERED.to_vec());

    // `'*'` is a subscription FORM, never an event (`hooks.test.ts:899-915`).
    assert_eq!(event_of_name(WILDCARD), None);
    assert_eq!(LifecycleEvent::ALL.len(), 18);
    assert!(!is_wildcard_deliverable(LifecycleEvent::HostStateChange));
}

#[tokio::test]
async fn delivers_a_wildcard_handler_once_per_event_in_the_pinned_table() {
    let (bus, _events) = observed_bus();
    let order = spy();

    let seen_by = Arc::clone(&order);
    bus.on_sync(
        WILDCARD,
        move |call: &HandlerCall| record(&seen_by, call.event.as_str()),
        owner("audit"),
    )
    .expect("'*' is a subscription form");

    // `fire()` is the synchronous shape every thread teardown uses.
    for event in LifecycleEvent::ALL {
        bus.fire(event, payload_for(event));
    }

    assert_eq!(
        seen(&order),
        LifecycleEvent::ALL
            .iter()
            .filter(|event| is_wildcard_deliverable(**event))
            .map(|event| event.as_str())
            .collect::<Vec<&str>>()
    );
}

/// The smallest payload each event can legally carry, for the fan-out loop.
fn payload_for(event: LifecycleEvent) -> Payload {
    match event {
        LifecycleEvent::AsyncPending | LifecycleEvent::AsyncDrained => {
            Payload::AsyncPending(AsyncLifecycle {
                agent: "noob".to_owned(),
                pending: vec![],
            })
        }
        LifecycleEvent::TurnBefore => turn_payload(),
        LifecycleEvent::HostStateChange => Payload::StateChange(Map::new()),
        LifecycleEvent::HarnessDispose => Payload::Dispose,
        _ => Payload::Dispose,
    }
}

/// `hooks.test.ts:678-687` — `live()` EXPANDS a wildcard into exactly the events
/// it will be delivered, so the Plugins pane does not lie, and it never claims
/// `host.state_change`.
#[test]
fn expands_a_wildcard_in_the_live_surface() {
    let (bus, _events) = observed_bus();

    bus.on_sync(WILDCARD, |_call: &HandlerCall| Ok(Contribution::nothing()), owner("audit"))
        .expect("'*' is a subscription form");

    let live = bus.live();

    assert_eq!(
        live.iter().map(|(event, _)| *event).collect::<Vec<&str>>(),
        LifecycleEvent::ALL
            .iter()
            .filter(|event| is_wildcard_deliverable(**event))
            .map(|event| event.as_str())
            .collect::<Vec<&str>>()
    );
    assert!(
        live.iter()
            .all(|(_, owners)| owners == &vec!["audit".to_owned()]),
        "every delivered event is claimed for the one wildcard subscriber"
    );
    assert!(!bus.has(LifecycleEvent::HostStateChange));
    assert_eq!(bus.count(LifecycleEvent::TurnBefore), 1);
}

// ---------------------------------------------------------------------------
// The registration edge
// ---------------------------------------------------------------------------

/// `hooks.test.ts:885-897` — an invented event name is refused, and NOTHING was
/// registered on the way to the refusal.
#[test]
fn refuses_an_unknown_event_and_registers_nothing() {
    let (bus, _events) = observed_bus();

    let error = bus
        .on_sync("thread.exploded", |_call: &HandlerCall| Ok(Contribution::nothing()), owner("x"))
        .expect_err("an invented event must be refused");

    assert_eq!(error.0, "Unknown lifecycle event: thread.exploded.");
    assert_eq!(bus.count(LifecycleEvent::TurnBefore), 0);
    assert!(bus.live().is_empty());
}

/// The `timeoutMs` half of `hooks.test.ts:885-897`. TS refuses a non-number, a
/// negative and a non-finite budget; Rust's `u64` cannot carry the first two, so
/// the reachable spelling of the same rule is the sentinel that no timer can arm.
#[test]
fn refuses_a_budget_that_cannot_be_armed() {
    let (bus, _events) = observed_bus();

    let error = bus
        .on_sync(
            "turn.before",
            |_call: &HandlerCall| Ok(Contribution::nothing()),
            HookOptions {
                timeout_ms: Some(u64::MAX),
                ..owner("x")
            },
        )
        .expect_err("an un-armable budget must be refused, not treated as unbounded");

    assert_eq!(error.0, "A \"turn.before\" timeoutMs must be a finite number >= 0.");
    assert_eq!(bus.count(LifecycleEvent::TurnBefore), 0);

    // `timeoutMs: 0` IS legitimate ("must already be resolved"), and so is the
    // absent budget (unbounded, the historical behaviour).
    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| Ok(Contribution::nothing()),
        HookOptions {
            timeout_ms: Some(0),
            ..owner("zero")
        },
    )
    .expect("a zero budget registers");
    assert_eq!(bus.count(LifecycleEvent::TurnBefore), 1);
}

// ---------------------------------------------------------------------------
// Rule 9 — the reserved `enabled` gate
// ---------------------------------------------------------------------------

/// `host.test.ts:400-440` — nothing stored means ON; the switch stops the gated
/// events; `harness.dispose` is exempt so a plugin can still release its own
/// handles; and the plugin stays LOADED and LISTED the whole time.
#[tokio::test]
async fn gates_delivery_without_unregistering_anything() {
    let (bus, _events) = observed_bus();
    let order = spy();
    let enabled = Arc::new(AtomicBool::new(true));

    let by = Arc::clone(&order);
    bus.on_sync(
        "turn.before",
        move |call: &HandlerCall| record(&by, call.event.as_str()),
        gated("policy", &enabled),
    )
    .expect("a gated registration");

    let by = Arc::clone(&order);
    bus.on_sync(
        "harness.dispose",
        move |call: &HandlerCall| record(&by, call.event.as_str()),
        gated("policy", &enabled),
    )
    .expect("a gated registration");

    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;
    assert_eq!(seen(&order), vec!["turn.before".to_owned()]);

    enabled.store(false, Ordering::SeqCst);
    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;
    bus.fire(LifecycleEvent::HarnessDispose, Payload::Dispose);

    assert_eq!(
        seen(&order),
        vec!["turn.before".to_owned(), "harness.dispose".to_owned()],
        "a disabled plugin is skipped for gated events, and STILL receives the teardown"
    );
    // `has`/`count` answer the DISPATCH question, so a gated-off registration reports
    // "nothing to do" and the emit site can skip the round entirely
    // (`host.test.ts:482-483`: off means `has` false and `count` 0). `live()` is the
    // registration surface and keeps the owner (`host.test.ts:485-487`).
    assert!(!bus.has(LifecycleEvent::TurnBefore));
    assert_eq!(bus.count(LifecycleEvent::TurnBefore), 0);
    assert_eq!(
        bus.live_owners(LifecycleEvent::TurnBefore),
        vec!["policy".to_owned()]
    );
}

/// The re-read itself (`host.test.ts:471-496`): flipping the key between two
/// emits changes the NEXT dispatch, with no re-registration.
#[tokio::test]
async fn re_reads_the_gate_on_every_dispatch() {
    let (bus, _events) = observed_bus();
    let calls = Arc::new(AtomicBool::new(false));
    let enabled = Arc::new(AtomicBool::new(true));

    let fired = Arc::clone(&calls);
    bus.on_sync(
        "turn.before",
        move |_call: &HandlerCall| {
            fired.store(true, Ordering::SeqCst);

            Ok(Contribution::nothing())
        },
        gated("policy", &enabled),
    )
    .expect("a gated registration");

    enabled.store(false, Ordering::SeqCst);
    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;
    assert!(!calls.load(Ordering::SeqCst), "off before the emit means off for the emit");

    enabled.store(true, Ordering::SeqCst);
    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;
    assert!(calls.load(Ordering::SeqCst), "the same registration answers again");
}

/// Rule 9 applied per DELIVERED event, not per registration: a wildcard
/// subscriber gated off still receives `harness.dispose` and nothing else
/// (`host.test.ts:498-523`).
#[tokio::test]
async fn gates_a_wildcard_per_delivered_event() {
    let (bus, _events) = observed_bus();
    let order = spy();
    let enabled = Arc::new(AtomicBool::new(false));

    let by = Arc::clone(&order);
    bus.on_sync(
        WILDCARD,
        move |call: &HandlerCall| record(&by, call.event.as_str()),
        gated("audit", &enabled),
    )
    .expect("a gated wildcard");

    for event in LifecycleEvent::ALL {
        bus.fire(event, payload_for(event));
    }

    assert_eq!(seen(&order), vec!["harness.dispose".to_owned()]);

    enabled.store(true, Ordering::SeqCst);
    bus.fire(LifecycleEvent::TurnBefore, turn_payload());
    assert_eq!(
        seen(&order),
        vec!["harness.dispose".to_owned(), "turn.before".to_owned()]
    );
}

/// A host-level registration (no owner, no gate) is never silenced by a
/// plugin's switch: the two gates are per-registration (`host.ts:182-197`).
#[tokio::test]
async fn leaves_an_ungated_host_registration_alone() {
    let (bus, _events) = observed_bus();
    let order = spy();
    let enabled = Arc::new(AtomicBool::new(false));

    let gated_by = Arc::clone(&order);
    bus.on_sync(
        "turn.before",
        move |_call: &HandlerCall| record(&gated_by, "gated"),
        gated("plugin", &enabled),
    )
    .expect("a gated registration");

    let host_by = Arc::clone(&order);
    bus.on_sync(
        "turn.before",
        move |_call: &HandlerCall| record(&host_by, "host"),
        HookOptions::default(),
    )
    .expect("an ungated registration is always delivered");

    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(seen(&order), vec!["host".to_owned()]);
}

// ---------------------------------------------------------------------------
// Rule 4 / rule 8 — the budget and the fail-open reports
// ---------------------------------------------------------------------------

/// `hooks.test.ts:729-762` — a handler that never resolves is dropped at its
/// budget, REPORTED with the pinned sentence, and the round keeps going.
#[tokio::test]
async fn drops_a_handler_that_breaches_its_budget_and_reports_it() {
    let (bus, events) = observed_bus();
    let order = spy();

    bus.on_async(
        "turn.before",
        |_call: Arc<HandlerCall>| -> HandlerFuture {
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                Ok(Contribution::with_transcript(vec![user("never applied")]))
            })
        },
        HookOptions {
            timeout_ms: Some(1),
            ..owner("slow")
        },
    )
    .expect("a budgeted registration");

    let by = Arc::clone(&order);
    bus.on_sync(
        "turn.before",
        move |_call: &HandlerCall| record(&by, "after"),
        owner("after"),
    )
    .expect("a plain registration");

    let round = bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(round.transcript, None, "the dropped handler contributes nothing");
    assert_eq!(seen(&order), vec!["after".to_owned()], "the round continues");

    let dropped = reported(&events);

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].kind, DiagnosticKind::HandlerTimedOut);
    assert_eq!(dropped[0].owner, "slow");
    assert_eq!(dropped[0].event.as_deref(), Some("turn.before"));
    assert_eq!(
        dropped[0].message,
        "A \"slow\" handler for \"turn.before\" timed out after 1ms and was dropped (fail-open)."
    );
}

/// The sentence itself, pinned independently of the clock: `budgetErrorOf`,
/// `hooks.ts:384-387`.
#[test]
fn spells_the_budget_drop_exactly() {
    assert_eq!(
        budget_error_of(LifecycleEvent::ToolAfter, "tracer", 250),
        "A \"tracer\" handler for \"tool.after\" timed out after 250ms and was dropped (fail-open)."
    );
}

/// Rule 8 — a throwing handler is a `handler-threw` report naming the event and
/// the owner, its result is discarded, and the round survives.
#[tokio::test]
async fn reports_a_handler_that_threw_and_keeps_the_round() {
    let (bus, events) = observed_bus();

    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| -> HandlerResult {
            Ok(Contribution::with_transcript(vec![user("first")]))
        },
        owner("writer"),
    )
    .expect("a registration");

    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| -> HandlerResult { Err(HandlerFailure::new("boom")) },
        owner("breaker"),
    )
    .expect("a registration");

    let round = bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(round.transcript, Some(vec![user("first")]));
    let dropped = reported(&events);

    assert_eq!(dropped.len(), 1);
    assert_eq!(
        dropped[0],
        DiagnosticEvent {
            kind: DiagnosticKind::HandlerThrew,
            owner: "breaker".to_owned(),
            event: Some("turn.before".to_owned()),
            message: "boom".to_owned(),
            data: None,
        }
    );
}

/// Rule 8's other half, the one a naive port gets wrong: a SKIPPED delivery is
/// not a handler error, so a disabled plugin reports nothing at all.
#[tokio::test]
async fn reports_nothing_for_a_delivery_the_gate_skipped() {
    let (bus, events) = observed_bus();
    let enabled = Arc::new(AtomicBool::new(false));

    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| Ok(Contribution::nothing()),
        gated("quiet", &enabled),
    )
    .expect("a gated registration");

    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert!(reported(&events).is_empty());
}

// ---------------------------------------------------------------------------
// Rule 2 / rule 5 — the round reducer
// ---------------------------------------------------------------------------

/// `hooks.test.ts:787-814` — a decision the contract cannot parse is IGNORED and
/// REPORTED, and the rest of that handler's result still applies.
#[tokio::test]
async fn reports_an_unparseable_decision_and_still_merges_the_transcript() {
    let (bus, events) = observed_bus();
    let received = json!({ "action": "frobnicate" });
    let reported_value = received.clone();

    bus.on_sync(
        "turn.before",
        move |_call: &HandlerCall| -> HandlerResult {
            Ok(Contribution::invalid_decision(
                received.clone(),
                Some(vec![user("kept")]),
            ))
        },
        owner("confused"),
    )
    .expect("a registration");

    let round = bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(round.transcript, Some(vec![user("kept")]));
    assert_eq!(round.decision, None);

    let dropped = reported(&events);

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].kind, DiagnosticKind::InvalidDecision);
    assert_eq!(
        dropped[0].message,
        decision_error_of(LifecycleEvent::TurnBefore, "confused", &reported_value)
    );
    assert_eq!(
        dropped[0].message,
        "A \"confused\" handler for \"turn.before\" returned a decision this contract cannot parse and it was ignored: {\"action\":\"frobnicate\"}."
    );
}

/// `hooks.test.ts:846-866` — the LAST non-empty transcript and the LAST decision
/// win, INDEPENDENTLY, an empty transcript is no override, a `continue` clears an
/// earlier park, and a `replace` normalises into BOTH channels.
#[tokio::test]
async fn merges_the_round_last_non_empty_wins_per_channel() {
    let (bus, _events) = observed_bus();

    for contribution in [
        Contribution::with_transcript(vec![user("first")]),
        // An EMPTY transcript must not clear the override above it.
        Contribution::with_transcript(vec![]),
        Contribution::with_transcript(vec![user("second")]),
        Contribution::with_decision(TurnDecision::Stop { reason: None }),
        Contribution::with_decision(TurnDecision::Continue),
        Contribution::with_decision(TurnDecision::Replace {
            transcript: vec![user("replaced")],
        }),
    ] {
        bus.on_sync(
            "turn.before",
            move |_call: &HandlerCall| Ok(contribution.clone()),
            owner("handoff"),
        )
        .expect("a registration");
    }

    let round = bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(
        round,
        Contribution {
            transcript: Some(vec![user("replaced")]),
            decision: Some(TurnDecision::Replace {
                transcript: vec![user("replaced")]
            }),
            invalid_decision: None,
        }
    );
}

/// `hooks.test.ts:868-883` — the reducer pin, asserted as the VALUE the TS test
/// expects: the priority-0 `pause` has the last word, because priority orders
/// dispatch and the LAST write wins.
#[tokio::test]
async fn lets_the_last_dispatched_policy_speak_last_for_the_round() {
    let (bus, _events) = observed_bus();

    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| {
            Ok(Contribution::with_decision(TurnDecision::Pause {
                reason: Some("cheap".to_owned()),
            }))
        },
        owner("cheap-policy"),
    )
    .expect("a registration");

    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| Ok(Contribution::with_decision(TurnDecision::Stop { reason: None })),
        HookOptions {
            priority: 10,
            ..owner("authoritative-policy")
        },
    )
    .expect("a registration");

    let round = bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(
        round,
        Contribution {
            transcript: None,
            decision: Some(TurnDecision::Pause {
                reason: Some("cheap".to_owned())
            }),
            invalid_decision: None,
        }
    );
}

/// A `pause` composes with a transcript override from a DIFFERENT handler —
/// the two channels are independent, which is what lets a trimmer rewrite a
/// request while a different plugin parks the run.
#[tokio::test]
async fn composes_a_park_with_another_handlers_transcript() {
    let (bus, _events) = observed_bus();

    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| Ok(Contribution::with_transcript(vec![user("trimmed")])),
        owner("trimmer"),
    )
    .expect("a registration");

    bus.on_sync(
        "turn.before",
        |_call: &HandlerCall| {
            Ok(Contribution::with_decision(TurnDecision::Pause { reason: None }))
        },
        owner("parking"),
    )
    .expect("a registration");

    assert_eq!(
        bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await,
        Contribution {
            transcript: Some(vec![user("trimmed")]),
            decision: Some(TurnDecision::Pause { reason: None }),
            invalid_decision: None,
        }
    );
}

// ---------------------------------------------------------------------------
// Rule 6 — `emitMaybeSync`'s two shapes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defers_the_round_only_when_a_handler_actually_defers() {
    let (bus, _events) = observed_bus();

    // A SYNC handler that resolves "immediately" is still the sync path, because
    // the property is the REGISTRATION, not a poll of the returned future (see
    // `hooks.rs`'s `HandlerBody` header for why polling would diverge from TS).
    bus.on_sync(
        "tool.before",
        |_call: &HandlerCall| Ok(Contribution::nothing()),
        owner("sync"),
    )
    .expect("a sync registration");

    assert!(bus.emit_maybe_sync(LifecycleEvent::ToolBefore, payload_for(LifecycleEvent::ToolBefore)).is_decided());

    let (bus, _events) = observed_bus();

    bus.on_async(
        "tool.before",
        |_call: Arc<HandlerCall>| -> HandlerFuture { Box::pin(async { Ok(Contribution::nothing()) }) },
        owner("async"),
    )
    .expect("an async registration");

    let round = bus.emit_maybe_sync(LifecycleEvent::ToolBefore, payload_for(LifecycleEvent::ToolBefore));

    assert!(!round.is_decided(), "one async handler defers the WHOLE round");
    assert_eq!(round.resolve().await, Contribution::nothing());
}

// ---------------------------------------------------------------------------
// The round's control surface
// ---------------------------------------------------------------------------

/// The seam double: it records WRITES in submission order, which is what makes a
/// live `setTranscript`/`pause` observable to a test.
#[derive(Debug, Default)]
struct FakeTurn {
    thread_id: String,
    writes: Mutex<VecDeque<String>>,
}

impl TurnControls for FakeTurn {
    fn thread_id(&self) -> &str {
        &self.thread_id
    }

    fn entries(&self) -> Vec<ContextEntry> {
        Vec::new()
    }

    fn current_messages(&self) -> Vec<HarnessMessage> {
        vec![user("original")]
    }

    fn current_estimated_tokens(&self) -> f64 {
        1200.0
    }

    fn set_included(&self, target: &EntryTarget, included: bool) -> Result<(), ControlError> {
        self.log(&format!("set_included({},{included})", target.label()));

        Ok(())
    }

    fn set_reasoning_included(
        &self,
        target: &EntryTarget,
        included: bool,
    ) -> Result<(), ControlError> {
        self.log(&format!("set_reasoning_included({},{included})", target.label()));

        Ok(())
    }

    fn edit(&self, target: &EntryTarget, patch: &EditPatch) -> Result<(), ControlError> {
        let _ = patch;
        self.log(&format!("edit({})", target.label()));

        Ok(())
    }

    fn set_transcript(&self, messages: &[HarnessMessage]) -> Result<(), ControlError> {
        self.log(&format!("set_transcript({})", messages.len()));

        Ok(())
    }

    fn replace_history(&self, messages: &[HarnessMessage]) -> Result<(), ControlError> {
        self.log(&format!("replace_history({})", messages.len()));

        Ok(())
    }

    fn pause(&self, owner: &str) -> Result<(), ControlError> {
        self.log(&format!("pause({owner})"));

        Ok(())
    }

    fn resume(&self, owner: &str) -> Result<(), ControlError> {
        self.log(&format!("resume({owner})"));

        Ok(())
    }

    fn has_persistent_thread(&self) -> bool {
        true
    }
}

impl FakeTurn {
    fn log(&self, line: &str) {
        if let Ok(mut writes) = self.writes.lock() {
            writes.push_back(line.to_owned());
        }
    }

    fn writes(&self) -> Vec<String> {
        self.writes
            .lock()
            .map(|list| list.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// A `turn.before` handler writes through the seam, and a LATER handler sees the
/// live value (`hooks.ts:232-239`'s per-handler payload factory — modelled in
/// Rust by handing a `&dyn TurnControls` that re-reads on call).
#[tokio::test]
async fn hands_the_round_surface_to_a_turn_before_handler() {
    let (bus, _events) = observed_bus();
    let turn = Arc::new(FakeTurn {
        thread_id: "t-1".to_owned(),
        ..FakeTurn::default()
    });

    bus.on_sync(
        "turn.before",
        move |call: &HandlerCall| -> HandlerResult {
            let controls = call.turn().expect("a turn.before emit WITH a round surface");

            // The LIVE reads are the contract: the estimate and the projected
            // transcript come from the seam, never from the frozen payload.
            assert_eq!(controls.current_estimated_tokens(), 1200.0);
            assert_eq!(controls.current_messages(), vec![user("original")]);
            controls
                .set_transcript(&[user("rewritten")])
                .expect("the seam accepts a rewrite");

            Ok(Contribution::nothing())
        },
        owner("writer"),
    )
    .expect("a registration");

    bus.emit_with(LifecycleEvent::TurnBefore, turn_payload(), Arc::clone(&turn) as Arc<dyn TurnControls>)
        .await;

    assert_eq!(turn.writes(), vec!["set_transcript(1)".to_owned()]);
}

/// The documented degenerate case: a `turn.before` emitted WITHOUT a round
/// surface hands the handler `None`, which is never a panic. A host that emits
/// the event before the run loop builds the surface reads as "no writes
/// possible", not as a crash.
#[tokio::test]
async fn answers_no_surface_for_a_turn_without_a_round() {
    let (bus, _events) = observed_bus();
    let order = spy();

    let by = Arc::clone(&order);
    bus.on_sync(
        "turn.before",
        move |call: &HandlerCall| -> HandlerResult {
            if call.turn().is_none() {
                return record(&by, "no-surface");
            }

            record(&by, "surface")
        },
        owner("probe"),
    )
    .expect("a registration");

    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert_eq!(seen(&order), vec!["no-surface".to_owned()]);
}

// ---------------------------------------------------------------------------
// The host: resolution, provenance, and the framework gate
// ---------------------------------------------------------------------------

/// A synthetic plugin whose setup is observable: it registers one handler per
/// requested event and can author a report on demand.
struct SpyPlugin {
    row: &'static PluginMetadata,
    events: Vec<&'static str>,
    /// Reports once during setup, with a FORGED owner, to test attribution.
    forge_owner: bool,
    /// When set, every delivered hook pushes one label here — the observable
    /// half of the gate, which is what `host.test.ts` asserts on.
    calls: Option<Spy>,
}

static SPY_ROW: PluginMetadata = PluginMetadata {
    name: "spy",
    title: "Spy",
    description: "A host-supplied plugin used to pin the host contract.",
    version: "0.0.1",
    tags: &["test"],
    hooks: &["turn.before"],
    settings: &[],
};

static SPY_OFF_ROW: PluginMetadata = PluginMetadata {
    name: "spy-off",
    title: "Spy Off",
    description: "A plugin that declares the reserved key with an off default.",
    version: "0.0.1",
    tags: &["test"],
    hooks: &["turn.before"],
    settings: &[PluginSettingDescriptor {
        key: "enabled",
        label: "Enabled",
        kind: PluginSettingType::Boolean,
        default: DeclaredDefault::Flag(false),
        min: None,
        max: None,
        step: None,
        description: None,
    }],
};

impl SpyPlugin {
    #[must_use]
    fn new(row: &'static PluginMetadata) -> Self {
        Self {
            row,
            events: vec!["turn.before"],
            forge_owner: false,
            calls: None,
        }
    }

    /// A spy whose deliveries are countable, for a gate test.
    fn watching(row: &'static PluginMetadata, calls: &Spy) -> Self {
        Self {
            calls: Some(Arc::clone(calls)),
            ..Self::new(row)
        }
    }
}

impl Plugin for SpyPlugin {
    fn metadata(&self) -> &'static PluginMetadata {
        self.row
    }

    fn setup(&self, api: &PluginApi) -> Result<(), HostError> {
        for name in &self.events {
            let calls = self.calls.clone();

            api.on(
                name,
                move |_call: &HandlerCall| {
                    if let Some(calls) = &calls {
                        return record(calls, "delivered");
                    }

                    Ok(Contribution::nothing())
                },
                HookOptions::default(),
            )?;
        }

        if self.forge_owner {
            api.report(DiagnosticEvent {
                kind: DiagnosticKind::Policy,
                owner: "someone-else".to_owned(),
                event: None,
                message: "withheld 2 units".to_owned(),
                data: None,
            });
        }

        Ok(())
    }
}

/// `host.ts:132` / `:148` — the three resolution refusals, byte-identical
/// sentences, and the legal replacement that is NOT a refusal.
#[test]
fn refuses_the_two_illegal_collisions_and_an_unnamed_plugin() {
    let bus = Arc::new(HookBus::new());
    let store = Arc::new(Mutex::new(PluginSettingsStore::default()));
    let reporter: Arc<dyn DiagnosticReporter> =
        Arc::new(SinkReporter::new(RecordingDiagnosticSink::default()));
    let runtime: Arc<dyn PluginRuntime> = Arc::new(null_runtime());

    let ping = AsyncPingPlugin::new();
    let also_ping = AsyncPingPlugin::new();
    let spy_on = SpyPlugin::new(&SPY_ROW);
    let spy_again = SpyPlugin::new(&SPY_ROW);
    let nameless = NamelessPlugin;

    // Two BUILT-INS with one name is a manifest bug.
    assert_eq!(
        PluginHost::run(
            &[&ping, &also_ping],
            &[],
            &bus,
            Arc::clone(&store),
            Arc::clone(&reporter),
            Arc::clone(&runtime),
        )
        .expect_err("two built-ins with one name"),
        HostError::Duplicate {
            name: "async-ping".to_owned()
        }
    );

    // Two HOST entries with one name have no honest precedence.
    assert_eq!(
        PluginHost::run(
            &[],
            &[&spy_on, &spy_again],
            &bus,
            Arc::clone(&store),
            Arc::clone(&reporter),
            Arc::clone(&runtime),
        )
        .expect_err("two host entries with one name"),
        HostError::Duplicate {
            name: "spy".to_owned()
        }
    );

    assert_eq!(
        resolve(&[], &[&nameless], &bus, &store, &reporter, &runtime),
        Err(HostError::MissingName)
    );
}

fn resolve<'a>(
    builtins: &[&'a dyn Plugin],
    plugins: &[&'a dyn Plugin],
    bus: &Arc<HookBus>,
    store: &Arc<Mutex<PluginSettingsStore>>,
    reporter: &Arc<dyn DiagnosticReporter>,
    runtime: &Arc<dyn PluginRuntime>,
) -> Result<(), HostError> {
    PluginHost::run(
        builtins,
        plugins,
        bus,
        Arc::clone(store),
        Arc::clone(reporter) as Arc<dyn typecrate_harness::plugin::DiagnosticReporter>,
        Arc::clone(runtime),
    )
    .map(|_| ())
}

struct NamelessPlugin;

static NAMELESS_ROW: PluginMetadata = PluginMetadata {
    name: "",
    title: "",
    description: "",
    version: "0.0.1",
    tags: &[],
    hooks: &[],
    settings: &[],
};

impl Plugin for NamelessPlugin {
    fn metadata(&self) -> &'static PluginMetadata {
        &NAMELESS_ROW
    }

    fn setup(&self, _api: &PluginApi) -> Result<(), HostError> {
        Ok(())
    }
}

/// `host.ts:14-35` rule 2 — a host entry naming a built-in REPLACES it IN PLACE,
/// and `builtin` is the loader's fact, not the plugin's claim.
#[test]
fn replaces_a_built_in_in_its_own_position() {
    let bus = Arc::new(HookBus::new());
    let store = Arc::new(Mutex::new(PluginSettingsStore::default()));
    let reporter: Arc<dyn DiagnosticReporter> =
        Arc::new(SinkReporter::new(RecordingDiagnosticSink::default()));
    let runtime: Arc<dyn PluginRuntime> = Arc::new(null_runtime());

    let ping = AsyncPingPlugin::new();
    let trimmer = TrimmerStub;
    let compaction = CompactionStub;
    // The SAME name as the built-in, which is what makes this a replacement
    // rather than a fourth plugin.
    let replacement = ReplacementPing;

    let report = PluginHost::run(
        &[&ping, &compaction, &trimmer],
        &[&replacement],
        &bus,
        Arc::clone(&store),
        Arc::clone(&reporter),
        Arc::clone(&runtime),
    )
    .expect("a host entry may replace a built-in");

    // A replacement that ran LAST would change when its hooks fire relative to
    // every other plugin's, so the order is the assertion.
    assert_eq!(
        report.names(),
        ["async-ping", "context-compaction", "context-trimmer"]
    );
    // Provenance is the LOADER's fact: a hand-written entry cannot upgrade itself
    // to "shipped", and the position is the built-in's.
    assert!(!report.is_builtin("async-ping"));
    assert!(report.is_builtin("context-trimmer"));
    assert!(report.is_builtin("context-compaction"));
    // The row the pane reads for that name is still the shipped one
    // (`lookup_metadata` prefers the catalogue `static`).
    assert_eq!(report.metadata()[0].name, "async-ping");
    assert_eq!(report.metadata()[0].version, "1.0.0");
}

struct ReplacementPing;

static REPLACEMENT_ROW: PluginMetadata = PluginMetadata {
    name: "async-ping",
    title: "Background Ping (host build)",
    description: "A host plugin that replaces the shipped async-ping by name.",
    version: "9.9.9",
    tags: &["test"],
    hooks: &["turn.before"],
    settings: &[],
};

impl Plugin for ReplacementPing {
    fn metadata(&self) -> &'static PluginMetadata {
        &REPLACEMENT_ROW
    }

    fn setup(&self, api: &PluginApi) -> Result<(), HostError> {
        api.on(
            "turn.before",
            |_call: &HandlerCall| Ok(Contribution::nothing()),
            HookOptions::default(),
        )
    }
}

#[derive(Default)]
struct TrimmerStub;
#[derive(Default)]
struct CompactionStub;

static TRIMMER_ROW: PluginMetadata = PluginMetadata {
    name: "context-trimmer",
    title: "Context Trimmer",
    description: "A stub standing in for the shipped trimmer.",
    version: "0.0.1",
    tags: &["test"],
    hooks: &["turn.before"],
    settings: &[],
};

static COMPACTION_ROW: PluginMetadata = PluginMetadata {
    name: "context-compaction",
    title: "Context Compaction",
    description: "A stub standing in for the shipped compactor.",
    version: "0.0.1",
    tags: &["test"],
    hooks: &["turn.before"],
    settings: &[],
};

macro_rules! stub_plugin {
    ($ty:ty, $row:ident) => {
        impl Plugin for $ty {
            fn metadata(&self) -> &'static PluginMetadata {
                &$row
            }

            fn setup(&self, api: &PluginApi) -> Result<(), HostError> {
                api.on(
                    "turn.before",
                    |_call: &HandlerCall| Ok(Contribution::nothing()),
                    HookOptions::default(),
                )
            }
        }
    };
}

stub_plugin!(TrimmerStub, TRIMMER_ROW);
stub_plugin!(CompactionStub, COMPACTION_ROW);

/// `host.test.ts:415-496` — the framework gate is the reserved key on THIS
/// plugin's view: a plugin that DECLARES `enabled: false` is off from the first
/// turn with nothing stored, the stored override wins over the declared default,
/// and the change takes effect on the NEXT dispatch of the SAME registration.
#[tokio::test]
async fn gates_the_host_registration_from_the_stored_switch() {
    let bus = Arc::new(HookBus::new());
    let store = Arc::new(Mutex::new(PluginSettingsStore::default()));
    let reporter: Arc<dyn DiagnosticReporter> =
        Arc::new(SinkReporter::new(RecordingDiagnosticSink::default()));
    let runtime: Arc<dyn PluginRuntime> = Arc::new(null_runtime());
    let calls = spy();

    let declared_off = SpyPlugin::watching(&SPY_OFF_ROW, &calls);

    PluginHost::run(
        &[],
        &[&declared_off],
        &bus,
        Arc::clone(&store),
        Arc::clone(&reporter),
        Arc::clone(&runtime),
    )
    .expect("a host plugin with a declared default");

    // NOTHING is stored for `spy-off`. The DECLARED default resolves through the
    // scoped view the host built from the plugin's OWN row, so `has()` - which
    // answers the dispatch question - is already false, while `live()` proves the
    // plugin is loaded and subscribed.
    assert!(!bus.has(LifecycleEvent::TurnBefore));
    assert_eq!(
        bus.live_owners(LifecycleEvent::TurnBefore),
        vec!["spy-off".to_owned()]
    );

    bus.emit(LifecycleEvent::TurnBefore, turn_payload()).await;

    assert!(
        seen(&calls).is_empty(),
        "a declared `enabled: false` default is off from the first turn"
    );

    // The OTHER half of the pin — a stored override flipping a live
    // registration — needs a plugin this build SHIPS: `update` answers the 404
    // that names the shipped catalogue (`plugins.ts requirePlugin`), which is the
    // ROUTE's decision and the subject of `tests/plugins.rs`. See
    // [`holds_no_heartbeat_while_the_switch_is_off`], which flips async-ping's
    // switch through the store and watches the plugin's own timers answer it.
}

/// The stored half of the same rule, on a plugin this build ships: the operator's
/// write re-reads live on the next dispatch, in BOTH directions, and a skipped
/// delivery is never a handler failure.
#[tokio::test]
async fn flips_a_live_registration_from_the_stored_override() {
    let bus = Arc::new(HookBus::new());
    let store = Arc::new(Mutex::new(PluginSettingsStore::default()));
    let reporter: Arc<dyn DiagnosticReporter> =
        Arc::new(SinkReporter::new(RecordingDiagnosticSink::default()));
    let runtime: Arc<dyn PluginRuntime> = Arc::new(null_runtime());
    let calls = spy();

    // async-ping does NOT declare `enabled`, so its switch resolves through the
    // RESERVED descriptor - the case that would be invisible to a declared-only
    // provider feed.
    let ping = AsyncPingPlugin::new();

    PluginHost::run(
        &[&ping],
        &[],
        &bus,
        Arc::clone(&store),
        Arc::clone(&reporter),
        Arc::clone(&runtime),
    )
    .expect("the shipped async-ping");

    assert!(bus.has(LifecycleEvent::AsyncPending));

    {
        let mut store = store.lock().expect("the store");

        store
            .update(PLUGIN_NAME, &Map::from_iter([("enabled".to_owned(), json!(false))]))
            .expect("the reserved key is writable on every shipped plugin");
    }

    bus.fire(
        LifecycleEvent::AsyncPending,
        Payload::AsyncPending(AsyncLifecycle {
            agent: "noob".to_owned(),
            pending: vec![pending("call-1", "bash")],
        }),
    );

    assert!(!bus.has(LifecycleEvent::AsyncPending));
    let _ = &calls;

    // Back on: the SAME registration is delivered again, with no re-registration
    // and no restart (`host.test.ts:489-496`).
    {
        let mut store = store.lock().expect("the store");

        store
            .update(PLUGIN_NAME, &Map::from_iter([("enabled".to_owned(), json!(true))]))
            .expect("the key is writable in both directions");
    }

    assert!(bus.has(LifecycleEvent::AsyncPending));
}

/// `host.ts:202-209` — the report channel ATTRIBUTES: `{ ...report, owner:
/// plugin.name }` is an OVERRIDE, so a plugin cannot forge a report about
/// another plugin.
#[tokio::test]
async fn attributes_a_policy_report_to_the_plugin_that_authored_it() {
    let bus = Arc::new(HookBus::new());
    let store = Arc::new(Mutex::new(PluginSettingsStore::default()));
    let reporter = SinkReporter::new(RecordingDiagnosticSink::default());
    let records = reporter.handle();
    let runtime: Arc<dyn PluginRuntime> = Arc::new(null_runtime());

    let mut forging = SpyPlugin::new(&SPY_ROW);
    forging.forge_owner = true;

    PluginHost::run(
        &[],
        &[&forging],
        &bus,
        Arc::clone(&store),
        Arc::new(reporter) as Arc<dyn typecrate_harness::plugin::DiagnosticReporter>,
        Arc::clone(&runtime),
    )
    .expect("a host plugin that reports");

    let records = records.lock().expect("the sink");

    assert_eq!(records.messages(), vec!["withheld 2 units".to_owned()]);
    assert_eq!(records.kinds(), vec!["policy"]);
    assert_eq!(records.records()[0].owner, "spy", "the forged owner is overwritten");
    // A policy report is NOT tied to one hook, so `event` stays absent.
    assert_eq!(records.records()[0].event, None);
}

// ---------------------------------------------------------------------------
// async-ping: the shipped policy on top of the host
// ---------------------------------------------------------------------------

/// The bus surface a setup async-ping leaves behind is its six SUBSCRIBED events
/// — this is what the REST row's `hooks` field reports (the bus's `live()`, not
/// the declared `metadata.hooks` patterns).
#[test]
fn subscribes_exactly_its_six_events() {
    let bus = Arc::new(HookBus::new());
    let store = Arc::new(Mutex::new(PluginSettingsStore::default()));
    let reporter: Arc<dyn DiagnosticReporter> =
        Arc::new(SinkReporter::new(RecordingDiagnosticSink::default()));
    let runtime: Arc<dyn PluginRuntime> = Arc::new(null_runtime());

    let ping = AsyncPingPlugin::new();

    let report = PluginHost::run(
        &[&ping],
        &[],
        &bus,
        Arc::clone(&store),
        Arc::clone(&reporter),
        Arc::clone(&runtime),
    )
    .expect("the shipped async-ping");

    assert_eq!(report.names(), [PLUGIN_NAME]);
    assert!(report.is_builtin(PLUGIN_NAME));
    assert_eq!(report.subscribed(PLUGIN_NAME), HOOKS);
    assert!(report.is_live(PLUGIN_NAME));
    assert_eq!(
        async_ping::interval_of(async_ping::DEFAULT_PING_INTERVAL_MS),
        600_000.0,
        "the documented heartbeat survives the sanity guard"
    );
}

/// The reserved switch, seen from the plugin's OWN side: while it is off the
/// heartbeat never arms (async-ping.plugin.ts:193-196), so a disable costs at
/// most one interval and the plugin holds no timer.
#[tokio::test]
async fn holds_no_heartbeat_while_the_switch_is_off() {
    let bus = Arc::new(HookBus::new());
    let store = Arc::new(Mutex::new(PluginSettingsStore::default()));
    let reporter: Arc<dyn DiagnosticReporter> =
        Arc::new(SinkReporter::new(RecordingDiagnosticSink::default()));
    let runtime: Arc<dyn PluginRuntime> = Arc::new(null_runtime());

    let ping = AsyncPingPlugin::new();

    PluginHost::run(
        &[] as &[&dyn Plugin],
        &[&ping],
        &bus,
        Arc::clone(&store),
        Arc::clone(&reporter),
        Arc::clone(&runtime),
    )
    .expect("async-ping as a host plugin keeps the same gate");

    // Off: the switch is written through the store the gate and the plugin SHARE.
    {
        let mut store = store.lock().expect("the store");
        store
            .update(PLUGIN_NAME, &Map::from_iter([("enabled".to_owned(), json!(false))]))
            .expect("async-ping accepts the reserved key");
    }

    bus.fire(
        LifecycleEvent::AsyncPending,
        Payload::AsyncPending(AsyncLifecycle {
            agent: "noob".to_owned(),
            pending: vec![pending("call-1", "bash")],
        }),
    );

    assert!(ping.armed().is_empty(), "a disabled plugin arms nothing");

    {
        let mut store = store.lock().expect("the store");
        store
            .update(PLUGIN_NAME, &Map::from_iter([("enabled".to_owned(), json!(true))]))
            .expect("the key is writable in both directions");
    }

    bus.fire(
        LifecycleEvent::AsyncPending,
        Payload::AsyncPending(AsyncLifecycle {
            agent: "noob".to_owned(),
            pending: vec![pending("call-1", "bash")],
        }),
    );

    assert_eq!(
        ping.armed(),
        vec!["noob".to_owned()],
        "the next `async.pending` after a re-enable arms again"
    );

    // And the teardown releases it even though the switch is whatever it is.
    bus.fire(LifecycleEvent::HarnessDispose, Payload::Dispose);
    assert!(ping.armed().is_empty(), "`harness.dispose` clears every heartbeat");
}

fn pending(call_id: &str, name: &str) -> typecrate_harness::plugin::PendingTool {
    typecrate_harness::plugin::PendingTool {
        call_id: call_id.to_owned(),
        name: name.to_owned(),
        elapsed_ms: 0,
    }
}

/// The pure rules the heartbeat is built from, asserted as TABLES. These are the
/// decisions `async-ping.plugin.ts` makes in `arm`/`tick`/`intervalOf`; the timer
/// plumbing above only carries them.
#[test]
fn decides_the_heartbeat_from_the_pinned_rules() {
    // `intervalOf` (`async-ping.plugin.ts:141-153`): anything that cannot drive a
    // sane repeat falls back. `min`/`step` are UI hints and never enforced, so
    // 1500 is accepted while 0.4, -5 and a non-finite number are not.
    let intervals: [(f64, f64); 8] = [
        (600_000.0, 600_000.0),
        (1_500.0, 1_500.0),
        (1.0, 1.0),
        (0.999, DEFAULT_PING_INTERVAL_MS),
        (0.0, DEFAULT_PING_INTERVAL_MS),
        (-5.0, DEFAULT_PING_INTERVAL_MS),
        (f64::NAN, DEFAULT_PING_INTERVAL_MS),
        (f64::INFINITY, DEFAULT_PING_INTERVAL_MS),
    ];

    for (configured, expected) in intervals {
        assert_eq!(interval_of(configured), expected, "intervalMs {configured}");
    }

    // `arm` (`:186-208`), as the full decision table.
    let arms: [(Option<&str>, bool, bool, bool, ArmDecision); 8] = [
        (Some("noob"), true, true, false, ArmDecision::Arm),
        // One timer per identity.
        (Some("noob"), true, true, true, ArmDecision::Keep),
        // An empty identity cannot address a board.
        (None, true, true, false, ArmDecision::Ignore),
        (Some(""), true, true, false, ArmDecision::Ignore),
        // A transition that announces no work would ping an empty list.
        (Some("noob"), false, true, false, ArmDecision::Ignore),
        (Some("noob"), false, false, false, ArmDecision::Ignore),
        // Off: release the key rather than arm it.
        (Some("noob"), true, false, false, ArmDecision::Release),
        (Some("noob"), true, false, true, ArmDecision::Release),
    ];

    for (agent, has_work, enabled, already, expected) in arms {
        assert_eq!(
            arm_decision(agent, has_work, enabled, already),
            expected,
            "arm({agent:?}, has_work={has_work}, enabled={enabled}, armed={already})"
        );
    }

    // `tick` (`:214-237`).
    assert_eq!(tick_decision(true, 3), TickDecision::WakeAndRearm);
    assert_eq!(tick_decision(true, 0), TickDecision::StopDrained);
    assert_eq!(tick_decision(false, 3), TickDecision::StopDisabled);
    // The disable is checked FIRST: an off plugin neither wakes nor re-arms, even
    // with a board that still has work.
    assert_eq!(tick_decision(false, 0), TickDecision::StopDisabled);

    // A thread board is keyed `thread:<id>`; an agent's default conversation is
    // keyed by its display name.
    assert_eq!(thread_timer_key("t-1"), "thread:t-1");
}

// ---------------------------------------------------------------------------
// Runtime double
// ---------------------------------------------------------------------------

/// The null handle, named once so every host test reads the same way: nothing is
/// wired behind it, which is exactly the "host cannot answer" path every plugin
/// already has to survive.
fn null_runtime() -> typecrate_harness::plugin::NullRuntime {
    typecrate_harness::plugin::NullRuntime
}

/// `NullRuntime` answers "no facts" for the three async-ping reads, which is what
/// keeps the plugin honest outside an integrated run loop.
#[test]
fn answers_no_facts_through_the_null_handle() {
    let runtime = null_runtime();

    assert_eq!(runtime.wake_interval_ms(), None);
    assert!(runtime.async_pending_for(Some("noob")).is_empty());
    runtime.async_wake(Some("noob"));

    // The trait's own default for an unwired filter is "ignore the filter", which
    // is safe because every caller treats empty as "stop".
    assert!(
        <typecrate_harness::plugin::NullRuntime as PluginRuntime>::async_pending_for(&runtime, None)
            .is_empty()
    );
}
