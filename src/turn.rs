//! The turn engine: one submitted prompt driven to a terminal outcome.
//!
//! TypeScript origins
//! ------------------
//!   * `core/agent.ts` - the retry policy (one initial attempt plus `DEFAULT_MAX_RETRIES`
//!     = 10, a doubling wait of `DEFAULT_RETRY_BASE_DELAY_MS * 2^(attempt-1)`, the abort
//!     boundaries, and the rule that the LAST attempt's own error is the answer
//!     (`agent.ts:626-669`));
//!   * `harness.ts:6539-6803` `runAgentCore` - the model-request loop, the
//!     `attempt_commit` boundaries, the exhaustion sentence, and the transcript rollback on
//!     every failure (`brain.messages.splice(commitIndex)`);
//!   * `harness.ts:5144-5177` `launchToolCall` - the per-call event pair and the validation
//!     refusals;
//!   * `harness.ts:4960-5097` `deliverTask` - the tool-result projection this module mirrors
//!     (`outcome` decides `isError`, the span is reported only for a call that EXECUTED).
//!
//! THE TWO COMMIT RULES THAT MATTER MOST
//! -------------------------------------
//!   1. **A failed attempt is discarded, a successful request is committed.** Every delta
//!      streamed since the previous boundary belongs to the attempt that produced it, so a
//!      `retry` means "delete what you showed me", and `attempt_commit` means "keep it".
//!      This engine emits `attempt_commit` at exactly the places TS does: after a tool round
//!      (because no `done` follows it), and after a text response that is superseded by
//!      steering instead of being reported as `done`. An ordinary answer needs NO boundary:
//!      its `done` closes the window.
//!   2. **A run that does not succeed leaves the transcript exactly as it found it.** The
//!      returned [`TurnOutcome::messages`] on a failure or a cancellation is the ORIGINAL
//!      transcript, because the store REPLACES the conversation's messages with whatever
//!      comes back (`turns.rs:1143-1159`) - a half-committed failed turn would otherwise
//!      replay a tool call the provider never completed.
//!
//! TOOL EXECUTION IS SEQUENTIAL AND IN ORDER. TypeScript launches a message's calls
//! concurrently through its scheduler; this port runs them one after another, which keeps
//! `read`/`write`/`edit` side effects in the order the model asked for them and keeps the
//! event order deterministic for the projector. `knowledge.md` records the difference.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::contract::{
    HarnessAssistantMessage, HarnessErrorInfo, HarnessEvent, HarnessMessage, HarnessStopReason,
    HarnessThread, HarnessToolCall, HarnessToolOutcome, HarnessUsage, MessageRole, ToolCall,
    ToolDecl, TurnOutcome,
};
use crate::ports::{ToolExecCtx, TurnExecutor, TurnInput};
use crate::transport::{OpenAiClient, StreamEvent, TransportError};

/// `agent.ts:211` `DEFAULT_MAX_RETRIES` - retries AFTER the initial attempt.
pub const MAX_RETRIES: u64 = 10;
/// `agent.ts:212` `DEFAULT_RETRY_BASE_DELAY_MS`.
pub const RETRY_BASE_DELAY_MS: u64 = 1_000;
/// `harness.ts` `DEFAULT_MAX_ITERATIONS`, the OPTIONAL runaway guard: `u64::MAX` means
/// NO LIMIT, which is the library default and what the store hands in
/// (`PRESET_MAX_ITERATIONS`, also uncapped). A host that names a finite count opts back in.
pub const DEFAULT_MAX_ITERATIONS: u64 = u64::MAX;
/// The agent name a Rust turn reports. TypeScript names the ENTRY agent; the
/// [`TurnInput`] seam carries no agent name, so the turn's own root flow answers for the
/// primary agent (`knowledge.md`, "the agent-name seam").
pub const PRIMARY_AGENT: &str = "primary";

/// The turn executor the store holds behind `Arc<dyn TurnExecutor>`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentExecutor;

impl AgentExecutor {
    /// A shared executor, the shape `Store::open` wants.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl TurnExecutor for AgentExecutor {
    /// Runs one turn to its terminal outcome, emitting every progress event through
    /// [`TurnInput::emit`] as it happens.
    fn run(&self, input: TurnInput) -> BoxFuture<'static, TurnOutcome> {
        Box::pin(run_turn(input))
    }
}

/// Everything one live run owns, kept in one place so the abort and commit paths cannot
/// disagree about it.
struct Run<'a> {
    /// The run owns its input mutably: the steering receiver it drains lives inside it.
    input: &'a mut TurnInput,
    client: OpenAiClient,
    /// The invocation identity of this run's root flow (`types/index.ts:1733`).
    invocation: Arc<HarnessThread>,
    /// The conversation as the model has been shown it, WITHOUT the system instruction.
    history: Vec<HarnessMessage>,
    /// The transcript the store handed in: what a failed or cancelled run hands back.
    original: Vec<HarnessMessage>,
    /// The running token tally (`TurnOutcome::usage`).
    usage: Option<HarnessUsage>,
    /// The attempt index and bound of the LAST successful request, which the boundary
    /// frames report.
    last_attempt: u64,
    max_attempts: u64,
    /// Text streamed in a later turn is appended after the prompt, never merged with it.
    iteration: u64,
    /// EVERY steer text this run drained from its channel, in arrival order, whether or not the
    /// model has seen it yet.
    ///
    /// TS keeps the same list as `run.steering` (`harness.ts:6831-6836`): a failure rolls the
    /// transcript back to `commitIndex`, so guidance that was appended into it is unwound WITH it and
    /// must be handed to the next run rather than silently consumed. A successful run's list is
    /// dropped with the run.
    delivered: Vec<String>,
}

impl Run<'_> {
    /// The `thread` frame member: present on every event of a non-root run, and on a root
    /// run it is what lets the store stamp the invocation identity (`turns.rs:1362-1396`).
    fn thread(&self) -> HarnessThread {
        HarnessThread {
            id: self.invocation.id.clone(),
            parent_id: None,
            call_id: None,
            input: None,
            kind: None,
            branch_id: None,
        }
    }

    fn emit(&self, event: HarnessEvent) {
        (self.input.emit)(event);
    }

    /// Folds one provider report into the running tally.
    ///
    /// The store's canonical fold is reused rather than re-derived, so the projector, the turn
    /// engine and the store's own tally can never disagree: `cached` stays absent unless BOTH
    /// sides reported one ("not reported" and "reported zero" are different facts), and a first
    /// frame that DOES report one is kept (`turns.rs:1589-1614` `addUsage`).
    fn add_usage(&mut self, usage: &HarnessUsage) {
        self.usage = Some(crate::store::turns::add_usage(self.usage.as_ref(), usage));
    }
}

/// One turn, start to finish.
async fn run_turn(mut input: TurnInput) -> TurnOutcome {
    // Everything the run reads after it takes the mutable borrow of its input is taken OUT
    // first: the steering receiver inside `TurnInput` has to be drained, and a `&mut` field
    // cannot coexist with the shared reads below.
    let original = input.transcript.clone();
    let model = input.provider.model.clone();
    let client = OpenAiClient::new(input.provider.clone());
    let prompt = input.prompt.clone();
    let attachments = input.attachments.clone();
    let limit = input.max_iterations;

    // The invocation identity this run reports on every frame. Minted once per run: the
    // store maps it to this turn and to its conversation (`turns.rs:1362-1396`).
    let invocation = Arc::new(HarnessThread {
        id: uuid::Uuid::new_v4().to_string(),
        parent_id: None,
        call_id: None,
        input: Some(prompt.clone()),
        kind: None,
        branch_id: None,
    });

    let mut run = Run {
        input: &mut input,
        client,
        invocation,
        // THE CONVERSATION STARTS AS THE STORE RECORDED IT. `TurnInput::transcript` is the
        // thread's history; the prompt is appended below. A run that ignored it would answer
        // every turn as if it were the first, and the returned `messages` REPLACE the
        // conversation in the store (`turns.rs:1143-1159`), so the prior entries would simply
        // vanish.
        history: original.clone(),
        original,
        usage: None,
        last_attempt: 1,
        max_attempts: MAX_RETRIES + 1,
        iteration: 0,
        delivered: Vec::new(),
    };

    // `start` FIRST: the store's contract is that the first turn's start predates a watcher
    // attached just after submission (`harness.ts:6835-6836`).
    run.emit(HarnessEvent::Start {
        agent: PRIMARY_AGENT.to_owned(),
        model: model.clone(),
        thread: Some(HarnessThread {
            input: Some(prompt.clone()),
            ..run.thread()
        }),
    });

    // THE PROMPT. Instructions are NOT part of the conversation the store keeps; they are
    // projected onto every request (see `request_messages`).
    run.history.push(HarnessMessage::User {
        content: prompt,
        attachments,
        // P0 added the three context keys to the contract and left them UNWIRED on purpose:
        // minting `messageId` (`core/snapshot.ts:300-332`) and running the request projection
        // that strips it (`core/agent.ts:203-230 withIncluded`) belong to the store/turn wave,
        // so a runtime-created entry here starts unflagged and id-less.
        included: None,
        message_id: None,
    });

    // The tool-loop budget: TS validates `maxIterations` at construction and never clamps;
    // a Rust host that hands in 0 gets the documented library default rather than a
    // zero-iteration run that could never answer.
    let limit = if limit == 0 {
        DEFAULT_MAX_ITERATIONS
    } else {
        limit
    };

    let outcome = run.loop_body(limit, &model).await;

    match outcome {
        Ok(outcome) => outcome,
        Err(failure) => {
            // Rollback path: the transcript the store keeps is the one this run STARTED
            // with, and the run reports the failure once (`runAgentCore`'s catch).
            if failure.cancelled {
                TurnOutcome {
                    usage: run.usage,
                    error: None,
                    cancelled: true,
                    messages: run.original.clone(),
                    stop_reason: None,
                    // A STOP discards the guidance by contract (`harness.ts:6831-6835`: the thread is
                    // stopped and its queue cleared), but it is still REPORTED here so the store can
                    // decide — it decides not to re-queue.
                    unused_steering: std::mem::take(&mut run.delivered),
                }
            } else {
                run.emit(HarnessEvent::Error {
                    agent: PRIMARY_AGENT.to_owned(),
                    error: HarnessErrorInfo {
                        message: failure.message.clone(),
                    },
                    thread: Some(run.thread()),
                });

                TurnOutcome {
                    usage: run.usage,
                    error: Some(failure.message),
                    cancelled: false,
                    messages: run.original.clone(),
                    stop_reason: None,
                    // R5 preservation: every steer this run drained is rolled back with the
                    // transcript, so it goes back to the store in arrival order
                    // (`harness.ts:6826-6837`).
                    unused_steering: std::mem::take(&mut run.delivered),
                }
            }
        }
    }
}

/// Why a run stopped without answering.
struct Failure {
    message: String,
    /// An abort is an EXPECTED control outcome, never an error row (`types/index.ts:198-215`:
    /// the classification is data, not a string match).
    cancelled: bool,
}

impl Run<'_> {
    /// The model-request loop.
    async fn loop_body(&mut self, limit: u64, model: &str) -> Result<TurnOutcome, Failure> {
        let tools: Vec<ToolDecl> = self.input.tools.clone();

        loop {
            // ABORT BOUNDARY (`harness.ts:6557-6560`): no new model request and no tool
            // launch once the run is cancelled.
            if self.input.signal.is_cancelled() {
                return Err(Failure {
                    message: crate::store::session::CANCELLED_RUN_MESSAGE.to_owned(),
                    cancelled: true,
                });
            }

            self.iteration += 1;

            if self.iteration > limit {
                // Exhaustion: every iteration asked for tools and never answered
                // (`harness.ts:6567-6577`).
                return Err(Failure {
                    message: format!(
                        "Agent \"{PRIMARY_AGENT}\" requested tools more than {limit} times without answering."
                    ),
                    cancelled: false,
                });
            }

            // R5: guidance that arrived while the previous request was in flight is applied
            // BEFORE this request, so the model sees it on the very next turn
            // (`harness.ts:6587-6590`). Each steer is committed on arrival: it is recorded
            // history, not the partial output of an attempt that may still be retried.
            for guidance in self.drain_steering() {
                self.history.push(HarnessMessage::User {
                    content: guidance,
                    attachments: Vec::new(),
                    included: None,
                    message_id: None,
                });
                self.emit_commit();
            }

            let streamed = self
                .request_with_retries(&tools)
                .await
                .map_err(|error| Failure {
                    message: error,
                    cancelled: self.input.signal.is_cancelled(),
                })?;

            // The success boundary is now known: everything that streamed belongs to a
            // committed request.
            self.last_attempt = streamed.attempt;

            let response = streamed.response;

            // The assistant message commits at request success, BEFORE any tool work
            // (`agent.ts:696-701`), which is what makes a later failure of THIS run roll back
            // rather than a later request lose it.
            self.history.push(HarnessMessage::Assistant {
                content: response.text.clone(),
                tool_calls: to_harness_calls(&response.tool_calls),
                reasoning: (!response.reasoning.is_empty()).then(|| response.reasoning.clone()),
                // Neither context axis is written at creation: `false` is the ONLY stored
                // value (`api.ts:919-920`, `:944-945`), so an unflagged turn prints no key.
                included: None,
                reasoning_included: None,
                message_id: None,
            });

            if response.tool_calls.is_empty() {
                // Steering that arrived WHILE this request was in flight buys one more turn
                // instead of being swallowed by the return (`harness.ts:6706-6711`).
                let guidance = self.drain_steering();

                if !guidance.is_empty() {
                    for text in guidance {
                        self.history.push(HarnessMessage::User {
                            content: text,
                            attachments: Vec::new(),
                            included: None,
                            message_id: None,
                        });
                    }

                    // No `done` follows this response, so its committed prose needs its
                    // boundary (`harness.ts:6645-6654`).
                    self.emit_commit();
                    continue;
                }

                if self.input.signal.is_cancelled() {
                    return Err(Failure {
                        message: crate::store::session::CANCELLED_RUN_MESSAGE.to_owned(),
                        cancelled: true,
                    });
                }

                self.emit(HarnessEvent::Done {
                    agent: PRIMARY_AGENT.to_owned(),
                    message: HarnessAssistantMessage {
                        role: MessageRole::Assistant,
                        content: response.text.clone(),
                        reasoning: response.reasoning.clone(),
                        tool_calls: Vec::new(),
                        model: model.to_owned(),
                        stop_reason: stop_reason_enum(response.stop_reason.as_deref()),
                        usage: response.usage.clone(),
                    },
                    pending: Vec::new(),
                    thread: Some(self.thread()),
                });

                return Ok(TurnOutcome {
                    usage: self.usage.clone(),
                    error: None,
                    cancelled: false,
                    messages: std::mem::take(&mut self.history),
                    stop_reason: response.stop_reason.clone(),
                    // The run answered: every steer it drained reached the model, so nothing is owed.
                    unused_steering: Vec::new(),
                });
            }

            // The tool round: `tool_call` frames, then execution, then `tool_result` frames,
            // then ONE boundary for the whole request (`harness.ts:6739-6750`).
            for call in &response.tool_calls {
                if self.input.signal.is_cancelled() {
                    return Err(Failure {
                        message: crate::store::session::CANCELLED_RUN_MESSAGE.to_owned(),
                        cancelled: true,
                    });
                }

                self.execute_call(call).await?;
            }

            self.emit_commit();
        }
    }

    /// The last `finish_reason` this run saw, kept so the terminal outcome can state it.
    fn emit_commit(&mut self) {
        self.emit(HarnessEvent::AttemptCommit {
            agent: PRIMARY_AGENT.to_owned(),
            attempt: self.last_attempt,
            max_attempts: self.max_attempts,
            thread: Some(self.thread()),
        });
    }

    /// Takes every queued steer, without waiting (`ports.rs:74-82`: an unbounded channel the
    /// executor must drain to empty when it settles).
    ///
    /// Whatever is taken is also remembered in [`Run::delivered`], because a later failure rolls the
    /// transcript back past it and the store has to be able to hand the same text to the next run
    /// (`harness.ts:6826-6837`).
    fn drain_steering(&mut self) -> Vec<String> {
        let Some(receiver) = self.input.steering.as_mut() else {
            return Vec::new();
        };

        let mut guidance: Vec<String> = Vec::new();

        while let Ok(text) = receiver.try_recv() {
            if !text.trim().is_empty() {
                self.delivered.push(text.clone());
                guidance.push(text);
            }
        }

        guidance
    }

    /// One model request, retried per `agent.ts`'s policy.
    ///
    /// A failed attempt discards its own partial text: the deltas that already went out are
    /// labelled by the `retry` frame, which is what tells the projector to remove them
    /// (`turn-projector.ts`'s `attempt:discard`).
    async fn request_with_retries(
        &mut self,
        tools: &[ToolDecl],
    ) -> Result<StreamedResponse, String> {
        let max_attempts = MAX_RETRIES + 1;
        let mut attempt = 0u64;

        loop {
            // ABORT BOUNDARY (`agent.ts:636-638`): never start another attempt for a
            // cancelled request, and never wait out a backoff for one.
            if self.input.signal.is_cancelled() {
                return Err("operation was aborted".to_owned());
            }

            attempt += 1;
            let messages = self.request_messages();

            // The sink is cloned OUT of the run so the streaming closure never borrows
            // `self`, which the transport call also borrows.
            let sink = self.input.emit.clone();
            let agent = PRIMARY_AGENT.to_owned();
            let thread = self.thread();

            let outcome = self
                .client
                .stream_chat(&messages, tools, &self.input.signal, &mut |event| {
                    match event {
                        StreamEvent::Text(delta) => sink(HarnessEvent::Text {
                            agent: agent.clone(),
                            delta,
                            thread: Some(thread.clone()),
                        }),
                        StreamEvent::Reasoning(delta) => sink(HarnessEvent::Reasoning {
                            agent: agent.clone(),
                            delta,
                            thread: Some(thread.clone()),
                        }),
                        // A `usage` chunk is NOT an event on its own. TypeScript reports one
                        // `usage` frame per REQUEST, from the response the request settled with
                        // (`harness.ts:6656-6658`), and the store folds every event it sees
                        // (`turns.rs:1358`), so a per-chunk event would multiply the tally.
                        StreamEvent::Usage(_) => {}
                    }
                })
                .await;

            match outcome {
                Ok(response) => {
                    if let Some(usage) = response.usage.as_ref() {
                        self.add_usage(usage);
                        self.emit(HarnessEvent::Usage {
                            agent: PRIMARY_AGENT.to_owned(),
                            usage: usage.clone(),
                            thread: Some(self.thread()),
                        });
                    }

                    return Ok(StreamedResponse { response, attempt });
                }
                Err(error) => {
                    let message = error.message();

                    // Cancellation is not an error to retry: the run stopped
                    // (`agent.ts:643-644`).
                    if matches!(error, TransportError::Request(_))
                        && self.input.signal.is_cancelled()
                    {
                        return Err(message);
                    }

                    if attempt >= max_attempts {
                        // Out of retries: the LAST attempt's own error is the answer, never a
                        // wrapper (`agent.ts:646-647`).
                        return Err(message);
                    }

                    // `baseDelayMs * 2^(attempt-1)`: 1s, 2s, 4s ... 512s.
                    let delay_ms =
                        RETRY_BASE_DELAY_MS.saturating_mul(1_u64 << (attempt - 1).min(20));

                    // The event precedes the wait, so a UI can show the backoff while it
                    // happens (`agent.ts:651-662`).
                    self.emit(HarnessEvent::Retry {
                        agent: PRIMARY_AGENT.to_owned(),
                        attempt,
                        max_attempts,
                        delay_ms,
                        error: HarnessErrorInfo {
                            message: message.clone(),
                        },
                        thread: Some(self.thread()),
                    });

                    if abort_aware_sleep(&self.input.signal, Duration::from_millis(delay_ms)).await
                    {
                        return Err("operation was aborted".to_owned());
                    }
                }
            }
        }
    }

    /// The provider transcript for one request: the system instruction, then the
    /// conversation.
    ///
    /// The instruction is projected on EVERY request and never stored in the conversation,
    /// which is what keeps the store's transcript the model's history rather than a copy of
    /// the ladder. A transcript that already leads with a system entry keeps its own: the
    /// store owns the instruction ladder, and duplicating it would double the prompt.
    fn request_messages(&self) -> Vec<HarnessMessage> {
        let mut messages: Vec<HarnessMessage> = Vec::with_capacity(self.history.len() + 1);

        if !self
            .history
            .first()
            .is_some_and(|message| matches!(message, HarnessMessage::System { .. }))
        {
            messages.push(HarnessMessage::System {
                content: self.input.system_instruction.clone(),
                // A REQUEST projection copy, and TS's `withIncluded` (`core/agent.ts:213-229`)
                // strips the id and any non-`false` flag from every one of these - which is
                // exactly the shape an unflagged entry projects to.
                included: None,
                message_id: None,
            });
        }

        messages.extend(self.history.clone());
        messages
    }

    /// Runs one requested call and commits both of its frames plus its tool message.
    async fn execute_call(&mut self, call: &ToolCall) -> Result<(), Failure> {
        let arguments = parse_arguments(&call.name, &call.arguments);

        self.emit(HarnessEvent::ToolCall {
            agent: PRIMARY_AGENT.to_owned(),
            call: HarnessToolCall {
                id: call.id.clone(),
                name: call.name.clone(),
                arguments: call.arguments.clone(),
            },
            thread: Some(self.thread()),
        });

        // A malformed JSON object never reaches a tool body: TS answers the call with the
        // parse error so the transcript stays valid for strict providers
        // (`harness.ts:5215-5224`).
        // The policy the call executes under: [`TurnInput`] carries the DECLARATIONS the run
        // was offered rather than a [`crate::ports::ToolPolicy`], so the enabled set is read
        // back off them - which is exactly the set this run may call.
        let policy = crate::ports::ToolPolicy {
            enabled: self
                .input
                .tools
                .iter()
                .map(|decl| decl.name.clone())
                .collect(),
            declared: self
                .input
                .tools
                .iter()
                .map(|decl| decl.name.clone())
                .collect(),
            created: Vec::new(),
            subagents: Vec::new(),
        };

        let outcome = match arguments {
            Ok(arguments) => {
                self.input
                    .tool_runtime
                    .execute(ToolExecCtx {
                        name: call.name.clone(),
                        arguments,
                        call_id: call.id.clone(),
                        session_id: Some(self.input.session_id.clone()),
                        cwd: self.input.cwd.clone(),
                        signal: self.input.signal.clone(),
                        policy,
                        action_host: self.input.action_host.clone(),
                        task_host: self.input.task_host.clone(),
                    })
                    .await
            }
            Err(message) => crate::ports::ToolExecOutcome {
                ok: false,
                content: message.clone(),
                panels: None,
                media: None,
                error: Some(message),
                duration_ms: 0,
            },
        };

        let cancelled = self.input.signal.is_cancelled();
        let outcome_kind = if outcome.ok {
            HarnessToolOutcome::Completed
        } else if cancelled {
            HarnessToolOutcome::Cancelled
        } else {
            HarnessToolOutcome::Failed
        };
        let is_error = outcome_kind != HarnessToolOutcome::Completed;

        // `durationMs` rides only a call that EXECUTED: an immediate validation failure
        // executed and reports nothing (`types/index.ts:1903-1909`).
        let duration_ms = if outcome.duration_ms == 0 && !outcome.ok {
            None
        } else {
            Some(outcome.duration_ms)
        };

        self.emit(HarnessEvent::ToolResult {
            agent: PRIMARY_AGENT.to_owned(),
            name: call.name.clone(),
            content: outcome.content.clone(),
            is_error,
            call_id: Some(call.id.clone()),
            outcome: Some(outcome_kind),
            ui: outcome
                .panels
                .clone()
                .and_then(value_to_panels)
                .unwrap_or_default(),
            media: outcome
                .media
                .clone()
                .and_then(value_to_media)
                .unwrap_or_default(),
            duration_ms,
            pending: Vec::new(),
            thread: Some(self.thread()),
        });

        // The tool message answers the call in the transcript. Media rides as descriptors on
        // the message's attachments only when the tool supplied real attachments; the event
        // never carries base64. The `outcome` key follows `core/tool-outcome.ts`
        // `storedOutcomeEntry` (compaction T1.4, `types/index.ts:276-298`): only a NON-`completed`
        // classification is stored, so a healthy result keeps its exact pre-outcome shape.
        self.history.push(HarnessMessage::Tool {
            content: outcome.content,
            tool_call_id: call.id.clone(),
            name: call.name.clone(),
            attachments: Vec::new(),
            outcome: crate::contract::stored_outcome_entry(outcome_kind),
            included: None,
            message_id: None,
        });

        Ok(())
    }
}

/// The response of one successful request, tagged with the attempt that produced it.
struct StreamedResponse {
    response: crate::transport::StreamResult,
    /// The 1-based attempt index that succeeded, which the commit boundary reports.
    attempt: u64,
}

/// Parses a call's raw JSON argument string.
///
/// # Errors
///
/// TS's malformed-JSON sentence, naming the tool and the parse failure.
fn parse_arguments(name: &str, raw: &str) -> Result<Value, String> {
    if raw.trim().is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }

    serde_json::from_str(raw)
        .map_err(|error| format!("Tool \"{name}\" received malformed JSON arguments: {error}"))
}

/// Maps a provider stop reason onto the contract enum (`contract::HarnessStopReason`).
#[must_use]
fn stop_reason_enum(reason: Option<&str>) -> HarnessStopReason {
    match reason {
        Some("length") => HarnessStopReason::Length,
        Some("tool_calls") => HarnessStopReason::ToolCalls,
        _ => HarnessStopReason::Stop,
    }
}

/// The runtime's tool calls are the recorded ones; only the field names differ
/// (`contract::ToolCall` vs `HarnessToolCall`).
fn to_harness_calls(calls: &[ToolCall]) -> Vec<HarnessToolCall> {
    calls
        .iter()
        .map(|call| HarnessToolCall {
            id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
        })
        .collect()
}

/// Decodes the panels a tool attached, which the ports seam carries as untyped JSON.
fn value_to_panels(value: Value) -> Option<Vec<crate::contract::ToolUiPanel>> {
    serde_json::from_value(value).ok()
}

/// Decodes the media descriptors a tool attached.
fn value_to_media(value: Value) -> Option<Vec<crate::contract::ToolMediaDescriptor>> {
    serde_json::from_value(value).ok()
}

/// Sleeps between retries, waking immediately on an abort (`agent.ts:664-666`
/// `waitForRetry`). Returns true when the abort is what ended the wait.
async fn abort_aware_sleep(signal: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        () = signal.cancelled() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

/// The JSON a caller can print for one outcome, kept for diagnostics and tests.
#[must_use]
pub fn outcome_document(outcome: &TurnOutcome) -> Value {
    json!({
        "error": outcome.error,
        "cancelled": outcome.cancelled,
        "stopReason": outcome.stop_reason,
        "usage": outcome.usage,
        "messages": outcome.messages.len(),
    })
}
