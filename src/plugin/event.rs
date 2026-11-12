//! The lifecycle VOCABULARY: the eighteen events, the one payload shape per event, the
//! wildcard key and its deliverable set, and the closed round-decision union.
//!
//! TypeScript origin, one section per ported surface:
//!   * `src/types/index.ts:4049-4072` - `HarnessLifecycleEvent`, the union this enum mirrors
//!     EXHAUSTIVELY (18 members, no more, no fewer);
//!   * `src/types/index.ts:4224-4255` - `HarnessLifecyclePayloads`, the one payload shape per
//!     event (`Payload` below);
//!   * `src/plugin/hooks.ts:140-159` - `WILDCARD_DELIVERABLE`, the exhaustive per-event
//!     decision about whether a `'*'` subscription is delivered that event;
//!   * `src/plugin/hooks.ts:130,136` - the `'*'` key, which is a SUBSCRIPTION FORM and never an
//!     event, and `ALWAYS_DELIVERED` (`harness.dispose`), the one event a gated owner still gets;
//!   * `src/types/index.ts:4744-4760` - `HarnessTurnDecision` / `HarnessHookResult`, ported as
//!     [`TurnDecision`] and [`Contribution`];
//!   * `src/types/index.ts:342-346` - `HarnessPendingTool`;
//!   * `src/types/index.ts:4339-4346,4697-4723` - the `run.*` / `turn.after` / `tool.*` identity
//!     blocks (`HarnessRunLifecycleContext`, `HarnessTurnAfterContext`,
//!     `HarnessToolLifecycleContext`, `HarnessAsyncLifecycleContext`).
//!
//! WHY THE EVENT IS A RUST ENUM RATHER THAN A STRING
//! -------------------------------------------------
//! TypeScript keeps this exhaustive by MAKING the compiler check a `Record<HarnessLifecycleEvent,
//! boolean>` (`hooks.ts:140`); a Rust enum gets the same guarantee from `match`, and the string
//! spellings survive only at the two boundaries that need them: the subscription key a plugin
//! author writes (`'*'` and the dotted names) and the `event` field of a diagnostic, which the
//! contract prints as a string.
//!
//! WHY THE PAYLOAD IS ONE ENUM AND THE TURN CONTEXT IS NOT
//! -------------------------------------------------------
//! Every payload is a COPY (`types:4225-4230` delivers `HarnessThreadRecord` copies;
//! `thread.deleted` carries the identity the record no longer has), so the payload is plain data
//! and a handler can be handed a fresh one per registration without aliasing anything.
//!
//! `turn.before` is the exception the brief calls out: `HarnessTurnBeforeContext`
//! (`types:4421-4680`) is not data, it is a LIVE CONTROL SURFACE - `entries` and
//! `currentMessages()` re-read the transcript, `setIncluded` writes through the host, and
//! `pause()`/`resume()` park the run. Rust models that half as the [`crate::plugin::TurnControls`]
//! seam, which every handler is handed beside the payload. Two consequences are deliberate and are
//! the reason this comment exists:
//!
//!   1. [`TurnBeforePayload`] carries identity and the ENTRY-TIME estimate ONLY. The projected
//!      `messages` and the canonical `entries` are read from the seam, so a handler that runs after
//!      another handler already rewrote the request sees the CURRENT value - exactly the "payload
//!      factories run once per handler" rule of `hooks.ts:232-239`, enforced by construction rather
//!      than by discipline.
//!   2. The `signal: AbortSignal` of `types:4657` has no Rust counterpart here; a Rust handler that
//!      cares about the run's cancellation reads the seam (`pause`/`resume` and the store's own
//!      stop state), and the run-loop wave decides where the cancel token lives.

use serde::{Deserialize, Serialize};

use crate::contract::{HarnessMessage, HarnessToolCall, HarnessToolOutcome};
use crate::persistence::HarnessThreadRecord;

// ---------------------------------------------------------------------------
// The event union
// ---------------------------------------------------------------------------

/// One lifecycle point the bus can deliver (`HarnessLifecycleEvent`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LifecycleEvent {
    ThreadCreated,
    ThreadStarted,
    ThreadPaused,
    ThreadResumed,
    ThreadStopped,
    ThreadIdle,
    ThreadDeleted,
    RunStarted,
    RunFinished,
    RunFailed,
    TurnBefore,
    TurnAfter,
    ToolBefore,
    ToolAfter,
    AsyncPending,
    AsyncDrained,
    /// The host's persistence plane, bridged (`types:4066-4071`). Explicit-subscription ONLY:
    /// never delivered through `'*'` ([`is_wildcard_deliverable`]).
    HostStateChange,
    HarnessDispose,
}

impl LifecycleEvent {
    /// Every event, in the order the TS union declares them. The order is what `live()` and any
    /// exhaustive table walk; it is NOT a priority.
    pub const ALL: [LifecycleEvent; 18] = [
        Self::ThreadCreated,
        Self::ThreadStarted,
        Self::ThreadPaused,
        Self::ThreadResumed,
        Self::ThreadStopped,
        Self::ThreadIdle,
        Self::ThreadDeleted,
        Self::RunStarted,
        Self::RunFinished,
        Self::RunFailed,
        Self::TurnBefore,
        Self::TurnAfter,
        Self::ToolBefore,
        Self::ToolAfter,
        Self::AsyncPending,
        Self::AsyncDrained,
        Self::HostStateChange,
        Self::HarnessDispose,
    ];

    /// The dotted spelling a plugin author writes in `on(...)` and a diagnostic prints.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThreadCreated => "thread.created",
            Self::ThreadStarted => "thread.started",
            Self::ThreadPaused => "thread.paused",
            Self::ThreadResumed => "thread.resumed",
            Self::ThreadStopped => "thread.stopped",
            Self::ThreadIdle => "thread.idle",
            Self::ThreadDeleted => "thread.deleted",
            Self::RunStarted => "run.started",
            Self::RunFinished => "run.finished",
            Self::RunFailed => "run.failed",
            Self::TurnBefore => "turn.before",
            Self::TurnAfter => "turn.after",
            Self::ToolBefore => "tool.before",
            Self::ToolAfter => "tool.after",
            Self::AsyncPending => "async.pending",
            Self::AsyncDrained => "async.drained",
            Self::HostStateChange => "host.state_change",
            Self::HarnessDispose => "harness.dispose",
        }
    }

    /// The dotted spelling, which is also the diagnostic's `event` field (`types:4307`).
    #[must_use]
    pub fn diagnostic_event(&self) -> String {
        self.as_str().to_owned()
    }
}

/// Resolves one dotted event name. `None` for `'*'` and for anything the catalogue does not
/// declare - `'*'` is a subscription form ([`SubscriptionTarget`]), and an invented name must not
/// reach a registration the bus would then never visit (TS throws at `hooks.ts:591-593`; the Rust
/// caller turns `None` into that same refusal).
#[must_use]
pub fn event_of_name(name: &str) -> Option<LifecycleEvent> {
    LifecycleEvent::ALL.into_iter().find(|event| event.as_str() == name)
}

/// What `on(...)` was pointed at: one event, or every deliverable one (`'*'`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionTarget {
    Event(LifecycleEvent),
    Wildcard,
}

/// The wildcard key (`hooks.ts:130`). NOT a member of [`LifecycleEvent`].
pub const WILDCARD: &str = "*";

/// Resolves a subscription key: an event name, or `'*'`. An empty name is refused
/// (`hooks.ts:591`, `Unknown lifecycle event`).
#[must_use]
pub fn subscription_of_name(name: &str) -> Option<SubscriptionTarget> {
    if name.is_empty() {
        return None;
    }

    if name == WILDCARD {
        return Some(SubscriptionTarget::Wildcard);
    }

    event_of_name(name).map(SubscriptionTarget::Event)
}

/// Whether a `'*'` subscription is DELIVERED this event (`hooks.ts:140-159`, exhaustive by
/// construction: the `_` arm cannot be reached, so adding an event to [`LifecycleEvent`] breaks
/// the build until its author decides).
///
/// `host.state_change` is the single `false`: it fires on every transcript/tool/thread mutation,
/// so delivering it through the default fan-out would hand an audit plugin a handler on every tool
/// execution (`types:4066-4071`).
#[must_use]
pub const fn is_wildcard_deliverable(event: LifecycleEvent) -> bool {
    match event {
        LifecycleEvent::HostStateChange => false,
        LifecycleEvent::ThreadCreated
        | LifecycleEvent::ThreadStarted
        | LifecycleEvent::ThreadPaused
        | LifecycleEvent::ThreadResumed
        | LifecycleEvent::ThreadStopped
        | LifecycleEvent::ThreadIdle
        | LifecycleEvent::ThreadDeleted
        | LifecycleEvent::RunStarted
        | LifecycleEvent::RunFinished
        | LifecycleEvent::RunFailed
        | LifecycleEvent::TurnBefore
        | LifecycleEvent::TurnAfter
        | LifecycleEvent::ToolBefore
        | LifecycleEvent::ToolAfter
        | LifecycleEvent::AsyncPending
        | LifecycleEvent::AsyncDrained
        | LifecycleEvent::HarnessDispose => true,
    }
}

/// The one event an owner receives even when its enablement gate is OFF (`hooks.ts:332-336`):
/// cleanup must always run, or a plugin switched off mid-session leaks its timers and handles for
/// the life of the process.
pub const ALWAYS_DELIVERED: LifecycleEvent = LifecycleEvent::HarnessDispose;

// ---------------------------------------------------------------------------
// The payloads (`HarnessLifecyclePayloads`)
// ---------------------------------------------------------------------------

/// One pending background call (`HarnessPendingTool`, `types:342-346`), the entry the
/// `async.pending` payload lists and `api.async.pending()` reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingTool {
    pub call_id: String,
    pub name: String,
    pub elapsed_ms: u64,
}

/// The identity block `run.*` and `turn.*` share (`HarnessRunLifecycleContext`, `types:4339-4346`).
///
/// `thread_id` is absent ONLY for a threadless run: `threadId` is `run.threadId ?? run.actorKey`,
/// so the store hands both fields and the summarize machine's `hasPersistentThread` test
/// (`context-compaction.plugin.ts:961-962`) compares `actor_key != thread_id` to tell a durable
/// conversation from a bare `writer('go')` turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunLifecycle {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub agent: String,
    pub depth: u64,
    pub input: String,
}

/// The `turn.before` identity fields. The live control surface - projected messages, canonical
/// entries, the inclusion writes, the park and the lane - is the
/// [`crate::plugin::TurnControls`] seam handed beside this value (see the module header for why).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnBeforePayload {
    pub thread_id: String,
    pub agent: String,
    /// Which model request of this run this is (1-based, `types:4429-4431`).
    pub iteration: u64,
    /// The scheduling key of the conversation (`thread:<id>` for a host thread, the display agent
    /// name for a default conversation).
    pub actor_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<String>,
    /// The estimate frozen at handler ENTRY (`types:4655`); the LIVE number is
    /// `TurnControls::current_estimated_tokens`.
    pub estimated_tokens: f64,
}

/// The `turn.after` payload (`types:4697-4701`): the identity block plus the assistant message the
/// run just committed.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnAfterPayload {
    pub run: RunLifecycle,
    pub message: HarnessMessage,
}

/// The `tool.before` payload: the identity block plus the call about to run
/// (`types:4239`, `HarnessToolLifecycleContext & { call }`).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolBeforePayload {
    pub run: RunLifecycle,
    pub call: HarnessToolCall,
}

/// The `tool.after` payload: the call, its result and why it ended (`types:4240-4247`).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolAfterPayload {
    pub run: RunLifecycle,
    pub call: HarnessToolCall,
    pub call_id: String,
    pub content: String,
    /// DERIVED, never stored: `outcome != completed` (`core/tool-outcome.ts` rule 3).
    pub is_error: bool,
    pub outcome: HarnessToolOutcome,
    pub duration_ms: u64,
}

/// The `async.pending` / `async.drained` payload (`types:4248-4249`, 4723).
///
/// `async.pending` is emitted BEFORE `scheduler.schedule()` puts the task on the board
/// (`async-ping.plugin.ts:16-21`), so `pending` is the transition's own claim, not a live board
/// read - which is why the shipped ping arms on `pending.len() > 0` rather than on the board.
#[derive(Debug, Clone, PartialEq)]
pub struct AsyncLifecycle {
    pub agent: String,
    pub pending: Vec<PendingTool>,
}

/// The one payload per event (`HarnessLifecyclePayloads`).
#[derive(Debug, Clone, PartialEq)]
pub enum Payload {
    // Boxed on purpose: `HarnessThreadRecord` is 424 bytes inline (the whole persisted row - id,
    // agent, messages, context, timestamps) while the second-largest variant here is 216, so an
    // unboxed enum would size EVERY event - including the field-less `Dispose` - at the record's
    // full width. The record reaches every handler as a COPY anyway (`event.rs` module header), so
    // the one extra allocation is paid where a copy is already being made, and the enum - which is
    // what gets moved into each hook registration and each round's `HandlerCall` - halves.
    Thread(Box<HarnessThreadRecord>),
    /// The record is already purged when this fires, so the payload is the identity a handler
    /// still needs (`types:4231-4233`).
    ThreadDeleted { thread_id: String, agent: String },
    RunStarted(RunLifecycle),
    RunFinished {
        run: RunLifecycle,
        message: HarnessMessage,
    },
    RunFailed {
        run: RunLifecycle,
        error: String,
    },
    TurnBefore(TurnBeforePayload),
    TurnAfter(TurnAfterPayload),
    ToolBefore(ToolBeforePayload),
    ToolAfter(ToolAfterPayload),
    AsyncPending(AsyncLifecycle),
    AsyncDrained(AsyncLifecycle),
    /// The host's persistence plane, bridged verbatim (`types:4250-4253`): the detail fields are
    /// the host's own JSON, which this build does not narrow.
    StateChange(serde_json::Map<String, serde_json::Value>),
    /// `Record<string, never>` (`types:4254`) - a dispose names nothing.
    Dispose,
}

// ---------------------------------------------------------------------------
// The round's result channels (`HarnessTurnDecision` + `HarnessHookResult`)
// ---------------------------------------------------------------------------

/// The closed run-level decision a `turn.before` handler may ask for (`HarnessTurnDecision`,
/// `types:4744-4754`).
///
/// `Continue` is a VALUE, not an absence: it explicitly clears a `pause`/`stop` an earlier handler
/// asked for (`hooks.ts:483-489`, "a later plugin can clear an earlier park/stop").
#[derive(Debug, Clone, PartialEq)]
pub enum TurnDecision {
    Continue,
    Replace { transcript: Vec<HarnessMessage> },
    Pause { reason: Option<String> },
    Stop { reason: Option<String> },
}

/// What ONE handler contributes to a round, already normalized (`contributionOf`,
/// `hooks.ts:406-445`).
///
/// The three channels are what they are in TS:
///   * `transcript` - `None` means "no override". An EMPTY array is never an override
///     (`hooks.ts:408-413`): a hook that returns `[]` is saying "nothing to add", not "send no
///     messages". [`Contribution::with_transcript`] enforces that rule so a caller cannot get it
///     wrong.
///   * `decision` - `None` means "no decision", which is NOT the same as `Some(Continue)`
///     (see [`TurnDecision`]).
///   * `invalid_decision` - the value a handler returned that the closed contract cannot parse.
///     `hooks.ts:430-442` reports it AND keeps the handler's transcript; [`Contribution`] keeps
///     both, so the round can do the same.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Contribution {
    pub transcript: Option<Vec<HarnessMessage>>,
    pub decision: Option<TurnDecision>,
    pub invalid_decision: Option<serde_json::Value>,
}

impl Contribution {
    /// The empty contribution: this handler had nothing to say.
    #[must_use]
    pub fn nothing() -> Self {
        Self::default()
    }

    /// A transcript override; an EMPTY list is stored as "no override" (see the struct comment).
    #[must_use]
    pub fn with_transcript(messages: Vec<HarnessMessage>) -> Self {
        Self {
            transcript: (!messages.is_empty()).then_some(messages),
            ..Self::default()
        }
    }

    /// A decision. `{ decision: { action: 'replace' } }` and `{ transcript }` are the SAME
    /// statement (`hooks.ts:437-440`, T2.1), so both channels are filled here and the round reads
    /// one of them.
    #[must_use]
    pub fn with_decision(decision: TurnDecision) -> Self {
        let transcript = match &decision {
            TurnDecision::Replace { transcript } if !transcript.is_empty() => Some(transcript.clone()),
            _ => None,
        };

        Self {
            transcript,
            decision: Some(decision),
            invalid_decision: None,
        }
    }

    /// The contribution of a handler whose decision could not be parsed: the decision channel
    /// stays empty, the transcript channel still applies, and the offending value is carried for
    /// the `invalid-decision` diagnostic.
    #[must_use]
    pub fn invalid_decision(received: serde_json::Value, transcript: Option<Vec<HarnessMessage>>) -> Self {
        Self {
            transcript,
            decision: None,
            invalid_decision: Some(received),
        }
    }

    /// The one reducer for a round (`mergeContribution`, `hooks.ts:483-489`): LAST wins for each
    /// channel INDEPENDENTLY, which is what composes a `pause` with a transcript override and lets
    /// a later `continue` clear an earlier `stop`.
    pub fn merge(&mut self, contribution: &Contribution) {
        if contribution.transcript.is_some() {
            self.transcript.clone_from(&contribution.transcript);
        }

        if contribution.decision.is_some() {
            self.decision.clone_from(&contribution.decision);
        }
    }

    /// [`Contribution`] with the transcript channel rewritten (used by the round after a `replace`
    /// decision has normalized into it).
    #[must_use]
    pub fn without_channels(self) -> Self {
        Self::default()
    }
}

/// The closed parser for a decision that arrives as JSON - the shape a handler bridging a
/// wire-supplied hook result has in hand. Returns `None` for anything `parseDecision`
/// (`hooks.ts:450-481`) would refuse, INCLUDING a `replace` whose transcript is empty or absent.
///
/// A non-string `reason` is DROPPED, never stringified (`hooks.ts:468-472`): the reason reaches the
/// run record and the UI, and `[object Object]` in front of an operator asking "why did this stop?"
/// is worse than no reason.
#[must_use]
pub fn decision_of_value(value: &serde_json::Value) -> Option<TurnDecision> {
    let decision = value.as_object()?;

    match decision.get("action").and_then(serde_json::Value::as_str)? {
        "continue" => Some(TurnDecision::Continue),
        "replace" => {
            let transcript = decision.get("transcript")?.as_array()?;

            if transcript.is_empty() {
                return None;
            }

            let messages = serde_json::from_value::<Vec<HarnessMessage>>(serde_json::Value::Array(
                transcript.clone(),
            ))
            .ok()?;

            Some(TurnDecision::Replace { transcript: messages })
        }
        action @ ("pause" | "stop") => {
            let reason = decision
                .get("reason")
                // A non-string reason is DROPPED, never stringified (`hooks.ts:468-472`).
                .filter(|value| value.is_string())
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);

            Some(if action == "pause" {
                TurnDecision::Pause { reason }
            } else {
                TurnDecision::Stop { reason }
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalogue_is_the_eighteen_events_in_declaration_order() {
        let names: Vec<&str> = LifecycleEvent::ALL.iter().map(|event| event.as_str()).collect();

        assert_eq!(
            names,
            vec![
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
                "host.state_change",
                "harness.dispose"
            ]
        );
    }

    #[test]
    fn every_name_round_trips_and_the_wildcard_is_not_an_event() {
        for event in LifecycleEvent::ALL {
            assert_eq!(event_of_name(event.as_str()), Some(event));
        }

        assert_eq!(event_of_name("*"), None);
        assert_eq!(event_of_name(""), None);
        assert_eq!(event_of_name("thread.created."), None);
        assert_eq!(event_of_name("harness.disposex"), None);

        assert_eq!(
            subscription_of_name("*"),
            Some(SubscriptionTarget::Wildcard)
        );
        assert_eq!(subscription_of_name(""), None);
    }

    #[test]
    fn the_wildcard_covers_everything_except_the_host_state_plane() {
        let deliverable: Vec<&str> = LifecycleEvent::ALL
            .into_iter()
            .filter(|event| is_wildcard_deliverable(*event))
            .map(|event| event.as_str())
            .collect();

        assert_eq!(
            deliverable,
            vec![
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
                "harness.dispose"
            ]
        );
        assert!(!is_wildcard_deliverable(LifecycleEvent::HostStateChange));
        assert_eq!(ALWAYS_DELIVERED, LifecycleEvent::HarnessDispose);
    }

    #[test]
    fn an_empty_transcript_is_never_an_override() {
        assert_eq!(Contribution::with_transcript(vec![]).transcript, None);
        assert_eq!(
            Contribution::with_transcript(vec![HarnessMessage::User {
                content: "next".to_owned(),
                attachments: vec![],
                included: None,
                message_id: None,
            }])
            .transcript
            .as_ref()
            .map(|messages| messages.len()),
            Some(1)
        );
    }

    #[test]
    fn a_replace_decision_fills_both_channels_and_a_pause_does_not() {
        let replacement = vec![HarnessMessage::User {
            content: "rewritten".to_owned(),
            attachments: vec![],
            included: None,
            message_id: None,
        }];

        let contribution = Contribution::with_decision(TurnDecision::Replace {
            transcript: replacement.clone(),
        });

        assert_eq!(contribution.transcript, Some(replacement.clone()));
        assert_eq!(
            contribution.decision,
            Some(TurnDecision::Replace { transcript: replacement })
        );

        let paused = Contribution::with_decision(TurnDecision::Pause { reason: None });

        assert_eq!(paused.transcript, None);
        assert_eq!(paused.decision, Some(TurnDecision::Pause { reason: None }));
    }

    #[test]
    fn the_reducer_is_last_wins_per_channel() {
        let first = Contribution {
            transcript: Some(vec![HarnessMessage::System {
                content: "first".to_owned(),
                included: None,
                message_id: None,
            }]),
            decision: Some(TurnDecision::Stop { reason: None }),
            invalid_decision: None,
        };

        let mut round = Contribution::nothing();

        round.merge(&first);
        assert_eq!(round.decision, Some(TurnDecision::Stop { reason: None }));

        // A later `continue` clears the stop; the transcript an earlier handler set survives it,
        // because the channels are independent.
        round.merge(&Contribution::with_decision(TurnDecision::Continue));

        assert_eq!(round.decision, Some(TurnDecision::Continue));
        assert!(round.transcript.is_some());

        // A handler with NOTHING to say changes neither channel.
        round.merge(&Contribution::nothing());

        assert_eq!(round.decision, Some(TurnDecision::Continue));
    }

    #[test]
    fn the_decision_parser_accepts_only_the_closed_union() {
        let parsed = serde_json::from_str::<serde_json::Value>(
            r#"{ "action": "pause", "reason": "compacting" }"#,
        )
        .expect("the fixture is JSON");

        assert_eq!(
            decision_of_value(&parsed),
            Some(TurnDecision::Pause {
                reason: Some("compacting".to_owned())
            })
        );

        // A non-string reason is dropped, not stringified.
        let odd = serde_json::json!({ "action": "stop", "reason": { "object": true } });

        assert_eq!(
            decision_of_value(&odd),
            Some(TurnDecision::Stop { reason: None })
        );

        // An empty `replace` transcript is "nothing to say", so the whole decision is refused.
        let empty = serde_json::json!({ "action": "replace", "transcript": [] });

        assert_eq!(decision_of_value(&empty), None);

        for value in [
            serde_json::json!(null),
            serde_json::json!("continue"),
            serde_json::json!([]),
            serde_json::json!({ "action": "halt" }),
            serde_json::json!({ "action": "replace" }),
        ] {
            assert_eq!(decision_of_value(&value), None, "{value:?} is not a decision");
        }
    }
}
