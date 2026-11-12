//! The turn pump: queue draining, run execution, the event recorder, and terminal settlement.
//!
//! TypeScript origin: `sessions.ts` - `pumpLane` (`:6655-6699`), `executeTurn`
//! (`:6961-7465`), the recorder `record` (`:7092-7201`), `isIdle`/`settle` (`:6702-6780`), the
//! submit path `enqueue` (`:5828-6017`), and `dispatch`/`executeCommandTurn` (`:6047-6337`).
//!
//! HOW THE TS PUMP MAPS ONTO TOKIO
//! -------------------------------
//! TS runs one async pump per lane on a single thread, so `lane.pumping` is a synchronous flag and
//! the "peek, never shift" rule (`:6593-6605`) keeps FIFO intact across a wait. The Rust port keeps
//! both properties literally: `pumping` is set INSIDE the store guard before `tokio::spawn`, so a
//! concurrent submit never starts a second worker for the same lane, and a turn leaves the queue
//! only when the pump actually starts it (or a cancel-all drops it).
//!
//! The guard discipline is the part a reader must not get wrong. The pump
//!   1. locks, claims the head, stamps `running`, persists the transition, builds the `TurnInput`,
//!      and releases;
//!   2. awaits `TurnExecutor::run` with NO guard held, so a provider that takes 60 seconds blocks
//!      nobody;
//!   3. locks again to settle, persist, publish, and release the slot.
//!
//! Every progress event re-enters the guard for one short recording step. Two conversations
//! therefore run concurrently, and a session-level operation (a read, a cancel, a tool call) never
//! waits on a provider.
//!
//! WHY THE STORE GUARD IS NEVER TAKEN AROUND A SEAM CALL
//! ----------------------------------------------------
//! The collaborators a turn needs (the agent catalogue, the deployment settings, the tool runtime, the
//! task host) are shared `Arc` members of the `Store`, not entries behind the mutex, so reading them
//! takes no lock at all and the lock order can only ever be store -> {registry, persistence}. The tool
//! runtime and the executor are called with the guard RELEASED (the executor) or from a caller that
//! never re-enters the store (declarations).
//!
//! CANCELLATION IS THE STORE'S WORD (`sessions.ts:7303-7308`)
//! ----------------------------------------------------------
//! `ActiveRun::cancel_requested` is set by `cancel_session`/`cancel_thread` BEFORE the token is
//! cancelled, and the terminal status is decided from THAT flag plus the token - never from the
//! executor's return value - because the provider rejects an abort with its own error, and a
//! sibling lane's cancel must never classify this turn.

use std::sync::Arc;

use futures_util::FutureExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::contract::{
    self, HarnessEvent, HarnessToolCall, HarnessToolOutcome, SubmitTaskRequest, TaskReceipt,
};
use crate::error::ApiError;
use crate::events::now_millis;
use crate::ids;
use crate::journal::HarnessJournal;
use std::panic::AssertUnwindSafe;

use crate::persistence::SessionTurn;
use crate::ports::TurnInput;
use crate::store::{
    self, ActiveRun, SessionState, SessionStore, StoreInner, TurnState,
    session::ACTION_RUN_ENDED_MESSAGE, session::CANCELLED_RUN_MESSAGE, session::MAX_PENDING_TASKS,
    session::receipt, session::require_session, session::require_session_mut,
    session::validate_input, turn_key,
};

/// `PRESET_MAX_ITERATIONS` (`harnesses/toolbox.ts`): the tool-loop budget every profiled harness
/// runs with — uncapped, because the count guard is an optional safeguard rather than policy.
pub const PRESET_MAX_ITERATIONS: u64 = u64::MAX;

/// `A session cannot queue more than 100 pending tasks.` (`sessions.ts:8556-8561`).
pub const PENDING_CAP_MESSAGE: &str = "A session cannot queue more than 100 pending tasks.";

/// `` `threadId` must be a non-empty string when present. `` (`sessions.ts:6345-6347`).
pub const THREAD_ID_SHAPE_MESSAGE: &str = "`threadId` must be a non-empty string when present.";

/// The message a Rust panic inside the executor becomes. `sessions.ts:6620-6632` is the same
/// last-resort guard: an unexpected internal throw must never park a queue with a stuck flag, and
/// the turn still goes terminal so the next queued task runs.
pub const EXECUTOR_PANIC_MESSAGE: &str =
    "The run failed inside the executor; its recorded events are intact.";

/// The `SessionTurn.extra` key a settled turn stores the provider's stop reason under, when the
/// executor reported one: the durable schema has no such field and the contract reads none, so the
/// value rides the record's flattened `extra` map rather than being dropped.
pub const STOP_REASON_KEY: &str = "stopReason";

/// What settlement needs about one claimed run — everything except the [`TurnInput`], which moves into
/// the executor and is gone for the rest of the loop.
pub(crate) struct RunKey {
    pub session_id: String,
    pub turn_id: String,
    pub lane_key: String,
    pub thread_id: Option<String>,
    pub token: CancellationToken,
}

/// Everything one claimed run needs to execute OUTSIDE the store guard.
pub(crate) struct RunPlan {
    pub key: RunKey,
    pub input: TurnInput,
}

impl SessionStore {
    // -----------------------------------------------------------------------
    // The submit surface
    // -----------------------------------------------------------------------

    /// The queued submit path (`sessions.ts:5828-6017` `enqueue`).
    ///
    /// ORDER, and why it is load-bearing: attachments, then the empty/200k bounds, then references,
    /// then the pending cap, then steering, then the turn itself. A malformed clip plus an empty
    /// prompt must answer the ATTACHMENT error, and the cap is checked before steering because a
    /// steer consumes no queue slot.
    pub async fn submit_queued(
        &self,
        id: &str,
        request: SubmitTaskRequest,
    ) -> Result<TaskReceipt, ApiError> {
        let attachments = crate::attachments::validate_attachments(&request.attachments)?;
        let input = validate_input(&request.input, attachments.len())?;
        let wanted_thread = normalize_thread_id(request.thread_id.as_deref())?;

        let references = if request.references.is_empty() {
            None
        } else {
            Some(request.references.clone())
        };

        let now = now_millis();
        let mut inner = self.lock();
        let state = require_session_mut(&mut inner, id)?;

        // The prospective index is the monotonic high-water, not the array length, so a deletion
        // never lets a new turn sort before a survivor (`sessions.ts:5844-5845`).
        let prospective_index = state.turn_seq;
        let bound = bind_references(state, prospective_index, references.as_deref())?;

        // Pending only. The active turn is not in a queue, so one task per lane can run while this
        // many wait; the cap spans every lane (`sessions.ts:8553-8561`).
        if state.pending_count() >= MAX_PENDING_TASKS {
            return Err(ApiError::too_many_requests(PENDING_CAP_MESSAGE));
        }

        // A conversation the caller named must exist (`sessions.ts:6370-6374`): an honest "no such
        // conversation" beats silently opening a new one.
        if let Some(thread_id) = wanted_thread.as_deref()
            && !state.threads.contains_key(thread_id)
            && !state.invalid_threads.contains_key(thread_id)
        {
            return Err(ApiError::not_found(format!(
                "Unknown thread \"{thread_id}\"."
            )));
        }

        // STEERING (`sessions.ts:6359-6504`): a submit addressed to a RUNNING conversation lands at
        // its next safe model boundary with no new turn recorded. Attachments are never steered (the
        // runtime's steer API is text-only), so such a submit falls through to a queued continuation
        // that keeps its bytes.
        if let Some(thread_id) = wanted_thread.as_deref()
            && attachments.is_empty()
            && self.thread_is_running(state, thread_id)
        {
            let steered = self.steer_into_thread(
                state,
                thread_id,
                &input,
                (!bound.is_empty()).then_some(bound.as_slice()),
                now,
            )?;

            if let Some(steered) = steered {
                // Steering is delivered to a LIVE run and is lost on restart by definition, so
                // the audit record is a BEST-EFFORT write: a failure must not claim the guidance
                // was rejected (`sessions.ts:6490-6494`).
                let _ = self.save(&inner);

                return Ok(steered);
            }
        }

        // Resolve the execution conversation: an explicit id is used as-is (a STOPPED thread is
        // revived by the explicit host action); an unfocused submit opens a NEW standalone host
        // thread (`sessions.ts:5905-5915`).
        let created_thread = if wanted_thread.is_some() {
            None
        } else {
            Some(self.open_host_thread(state)?)
        };

        let target_thread = wanted_thread.clone().or_else(|| created_thread.clone());

        // The conversation's display label, bound at CREATION (`sessions.ts:5917-5921`).
        let target_label = target_thread
            .as_deref()
            .map(|thread_id| self.ensure_thread_label(state, thread_id));

        let index = state.turn_seq;
        state.turn_seq += 1;

        let turn_id = ids::new_turn_id();
        let turn = build_queued_turn(
            &turn_id,
            index,
            &input,
            target_thread.as_deref(),
            target_label.as_deref(),
            &bound,
            &attachments,
            now,
        );

        // A fresh turn starts an empty fold and is settled at `queued`, so the row a listener receives
        // before the run starts is the same row a reload rebuilds (`turn-projector.ts`'s settle step).
        // The seed is assembled HERE because the fold's non-event segments — the system block, the
        // prompt block, the steer blocks — and every block id's section prefix come from the
        // CONVERSATION, not from the event stream (`sessions.ts:4160-4240`).
        let seed = state.projector_seed(&turn);
        let mut turn_state = TurnState::seeded(self, turn, seed);
        turn_state.projector.settle("queued", now);

        state.turn_order.push(turn_id.clone());
        state.turns.insert(turn_id.clone(), turn_state);

        // EVERY thread-capable submit runs in its thread's own lane, so two plain submits are
        // concurrent instead of serialized through the legacy queue (`sessions.ts:5947-5953`).
        match target_thread.as_deref() {
            Some(thread_id) => Self::lane_of(state, thread_id).queue.push(turn_id.clone()),
            None => state.queue.push(turn_id.clone()),
        }

        state.record.updated_at = now;

        // Persist the accepted backlog BEFORE the 202 (`sessions.ts:5965-6000`): a queued prompt is
        // user work a restart must preserve, and a failed write rolls back the turn AND the
        // conversation this submit minted, so a retry cannot duplicate either and a label ordinal
        // the user never saw is never retired.
        let before = state.projected_record(self);

        if let Err(error) = self.save(&inner) {
            self.rollback_state(&mut inner, id, &before);

            return Err(error);
        }

        // The queue ordinal and the payloads are read BEFORE the pump can start the turn: the 202
        // must report `status: queued` and the position that includes this submit
        // (`sessions.ts:6002-6014`).
        let state = require_session_mut(&mut inner, id)?;
        let position = store::session::queue_position(state, &turn_id);

        let turn_row = state
            .turns
            .get(&turn_id)
            .map(TurnState::row)
            .unwrap_or(Value::Null);
        let session_patch =
            json!({ "session": store::read::session_state_patch_of(state, &self.hub) });

        // Upsert first, so a listener that receives it can already count this turn as queued, then
        // start (or join) the pump.
        store::publish(
            &self.hub,
            "turn.upsert",
            &turn_resources(id, &turn_id),
            Some(id),
            target_thread.as_deref(),
            Some(&turn_id),
            json!({ "turn": turn_row }),
            Vec::new(),
        );

        store::publish(
            &self.hub,
            "session.updated",
            &[
                ("session".to_string(), Some(id.to_string())),
                ("queue".to_string(), Some(id.to_string())),
            ],
            Some(id),
            target_thread.as_deref(),
            Some(&turn_id),
            session_patch,
            Vec::new(),
        );

        let lane_key = target_thread.clone().unwrap_or_default();

        self.spawn_pump(&mut inner, id, &lane_key);

        let session_revision = self.hub.revision("session", Some(id));

        Ok(receipt(
            id,
            &turn_id,
            contract::SessionTurnStatus::Queued,
            target_thread.as_deref(),
            None,
            position,
            session_revision,
            now,
        ))
    }

    /// The command submit path (`sessions.ts:6047-6101` `dispatch` + `:6111-6337`
    /// `executeCommandTurn`).
    ///
    /// A command is an OPERATOR action, not a model turn: it bypasses the single-flight gate and the
    /// pending cap, it never enters a queue, and it is TERMINAL before the receipt is produced.
    pub async fn submit_command(
        &self,
        id: &str,
        request: &SubmitTaskRequest,
    ) -> Result<TaskReceipt, ApiError> {
        // Refuse what a command line cannot express, BEFORE any turn exists and before the input is
        // bounded (`sessions.ts:6073-6093`).
        if !request.attachments.is_empty() {
            return Err(ApiError::bad_request(
                store::session::COMMAND_ATTACHMENT_MESSAGE,
            ));
        }

        if !request.references.is_empty() {
            return Err(ApiError::bad_request(
                store::session::COMMAND_REFERENCE_MESSAGE,
            ));
        }

        let input = validate_input(&request.input, 0)?;

        // `threadId` is ACCEPTED and ignored on purpose: the command acts on the SESSION (its task
        // list, its tool registry), never on a conversation (`sessions.ts:6095-6099`).
        let now = now_millis();
        let turn_id = ids::new_turn_id();

        let (agent, model) = {
            let mut inner = self.lock();
            let state = require_session_mut(&mut inner, id)?;

            let index = state.turn_seq;
            state.turn_seq += 1;

            let agent = state.primary_agent();
            let model = state.model();

            // Born `running` (never queued - there is no pump in its life) and with NO thread, the
            // record shape the UI already renders for a thread-less turn (`:6119-6132`).
            let turn = SessionTurn {
                id: turn_id.clone(),
                index,
                prompt: input.clone(),
                thread_id: None,
                thread_label: None,
                // A thread-less turn has no conversation whose lane counter could be non-zero, so it
                // starts on lane 0 and the key is never written (`sessions.ts:8953`).
                lane: None,
                references: None,
                attachments: None,
                status: "running".to_string(),
                started_at: Some(now),
                updated_at: now,
                events: Vec::new(),
                event_times: Some(Vec::new()),
                usage: None,
                steers: None,
                steer_texts: None,
                extra: serde_json::Map::new(),
            };

            let seed = state.projector_seed(&turn);
            let mut turn_state = TurnState::seeded(self, turn, seed);
            // The command turn is born `running` but its fold opens at `queued`, exactly as a queued
            // turn's does: the projector's settle step is what makes the row terminal, and only the
            // terminal write does that (`sessions.ts:6119-6132`).
            turn_state.projector.settle("queued", now);

            state.turn_order.push(turn_id.clone());
            state.turns.insert(turn_id.clone(), turn_state);

            state.record.updated_at = now;

            // DURABILITY BEFORE WORK (`sessions.ts:6137-6147`): nothing has executed yet, so a
            // failed write means the command simply did not happen.
            let before = state.projected_record(self);

            // The session borrow ENDS here: `save` serializes the whole store through the same guard, so
            // no session reference may be alive while it runs.

            if let Err(error) = self.save(&inner) {
                self.rollback_state(&mut inner, id, &before);

                return Err(error);
            }

            // Open the turn frame before ANY event, so a listener can always attribute what it
            // receives (`sessions.ts:6149-6151`).
            let opening = require_session(&inner, id)?
                .turns
                .get(&turn_id)
                .map(TurnState::row)
                .unwrap_or(Value::Null);

            store::publish(
                &self.hub,
                "turn.upsert",
                &turn_resources(id, &turn_id),
                Some(id),
                None,
                Some(&turn_id),
                json!({ "turn": opening }),
                Vec::new(),
            );

            (agent, model)
        };

        // THE command: no model request, no lane, no steering, no gate. An unknown name and a
        // built-in's refusal both become an error-SHAPED RESULT, never a 500 (`:6237-6243`).
        let (content, is_error) = {
            let mut inner = self.lock();
            let state = require_session_mut(&mut inner, id)?;

            let outcome = store::commands::dispatch(state, self, &input);

            (outcome.content, outcome.is_error)
        };

        // One synthesized call id pairs the tool_call with its result, which is how a renderer
        // resolves a call row: callId matching, never positional (`:6156-6158`).
        let call_id = ids::new_id();

        self.record_event(
            id,
            &turn_id,
            HarnessEvent::Start {
                agent: agent.clone(),
                model,
                thread: None,
            },
        );

        self.record_event(
            id,
            &turn_id,
            HarnessEvent::ToolCall {
                agent: agent.clone(),
                call: HarnessToolCall {
                    id: call_id.clone(),
                    name: store::commands::COMMAND_TOOL_NAME.to_string(),
                    // `HarnessToolCall.arguments` is a raw JSON STRING, so the command line rides
                    // INSIDE that JSON (`sessions.ts:6210-6214`).
                    arguments: json!({ "input": input }).to_string(),
                },
                thread: None,
            },
        );

        self.record_event(
            id,
            &turn_id,
            HarnessEvent::ToolResult {
                agent,
                name: store::commands::COMMAND_TOOL_NAME.to_string(),
                content,
                is_error,
                call_id: Some(call_id),
                // `outcome` is stated explicitly: a renderer would read an unnamed error call as
                // `failed` anyway, and naming it keeps the record self-describing (`:6256-6259`).
                outcome: Some(if is_error {
                    HarnessToolOutcome::Failed
                } else {
                    HarnessToolOutcome::Completed
                }),
                ui: Vec::new(),
                media: Vec::new(),
                duration_ms: None,
                pending: Vec::new(),
                thread: None,
            },
        );

        // Terminal status, with NO `usage`: no provider request happened, and an absent tally is the
        // honest "nothing was billed" (`:6262-6264`).
        let at = now_millis();

        let mut inner = self.lock();
        let state = require_session_mut(&mut inner, id)?;

        if let Some(turn) = state.turns.get_mut(&turn_id) {
            let status = if is_error { "error" } else { "done" };

            turn.record.status = status.to_string();
            turn.record.updated_at = at;
            turn.projector.settle(status, at);
        }

        // `session.status` is NOT touched and neither is `last_error`: a command is not a run, so an
        // idle session stays idle while it executes and a failed `/task bogus` must not raise the
        // session-wide banner (`:6267-6271`).
        state.record.updated_at = at;

        // The TURN record must be durable before the answer (`:6283-6320`). A failed write still
        // publishes the terminal row - a listener's only view is the frames - and the status is
        // downgraded to `error`, because "done" would claim a transcript the disk refused.
        let write = self.save(&inner);

        let (status, row) = {
            let state = require_session(&inner, id)?;

            let row = state
                .turns
                .get(&turn_id)
                .map(TurnState::row)
                .unwrap_or(Value::Null);
            let status = match state
                .turns
                .get(&turn_id)
                .map(|turn| turn.record.status.clone())
                .as_deref()
            {
                Some("error") => contract::SessionTurnStatus::Error,
                _ => contract::SessionTurnStatus::Done,
            };

            (status, row)
        };

        if let Err(error) = write {
            let state = require_session_mut(&mut inner, id)?;

            if let Some(turn) = state.turns.get_mut(&turn_id) {
                turn.record.status = "error".to_string();
                turn.record.updated_at = now_millis();
                turn.projector.settle("error", turn.record.updated_at);
            }

            store::publish(
                &self.hub,
                "turn.upsert",
                &turn_resources(id, &turn_id),
                Some(id),
                None,
                Some(&turn_id),
                json!({ "turn": row }),
                Vec::new(),
            );

            return Err(error);
        }

        store::publish(
            &self.hub,
            "turn.upsert",
            &turn_resources(id, &turn_id),
            Some(id),
            None,
            Some(&turn_id),
            json!({ "turn": row }),
            Vec::new(),
        );

        let session_revision = self.hub.revision("session", Some(id));

        Ok(receipt(
            id,
            &turn_id,
            status,
            None,
            None,
            None,
            session_revision,
            at,
        ))
    }

    // -----------------------------------------------------------------------
    // Lanes and the pump
    // -----------------------------------------------------------------------

    /// The lane of one conversation, created on demand (`sessions.ts:2427-2440` `laneOf`).
    #[must_use]
    pub(crate) fn lane_of<'a>(state: &'a mut SessionState, thread_id: &str) -> &'a mut store::Lane {
        state
            .lanes
            .entry(thread_id.to_string())
            .or_insert_with(|| store::Lane {
                thread_id: thread_id.to_string(),
                active_turn: None,
                queue: Vec::new(),
                // A lane starts idle: the flag is taken by `spawn_pump` in the SAME guard region that
                // queues the work, so an inserted lane is never mid-pump.
                pumping: false,
                stop_requested: false,
            })
    }

    /// Opens the standalone HOST conversation an unfocused submit runs in (`sessions.ts:5905-5915`).
    ///
    /// TS names it `thread-<n>` with the session's next ordinal, marks it `host`, and gives it no owner:
    /// an unfocused submit is a top-level request, so nothing is a parent of it. It runs AS the harness's
    /// entry agent, which is the only agent a host conversation may impersonate without a delegation.
    pub(crate) fn open_host_thread(&self, state: &mut SessionState) -> Result<String, ApiError> {
        if state.threads.len() >= crate::store::threads::MAX_THREADS {
            return Err(crate::store::threads::thread_cap_error());
        }

        let agent = state.primary_agent();

        Ok(state
            .create_thread(&agent, crate::contract::ThreadOrigin::Host, None, None)
            .id)
    }

    /// The display label of one conversation, minted on first use (`sessions.ts:5917-5921`).
    ///
    /// The label is bound at CREATION and never re-derived, because the ordinal is per-agent-code and a
    /// re-derivation would let two conversations print the same name after one is deleted. A conversation
    /// created by `create_thread` already carries one, so this is the fallback for a restored roster whose
    /// label map was written by an older build.
    pub(crate) fn ensure_thread_label(&self, state: &mut SessionState, thread_id: &str) -> String {
        if let Some(label) = state.thread_label_of(thread_id) {
            return label.to_owned();
        }

        let agent = state
            .thread(thread_id)
            .map(|thread| thread.agent.clone())
            .unwrap_or_else(|| state.primary_agent());
        let label = state.labels.next(&agent);

        state
            .thread_labels
            .insert(thread_id.to_owned(), label.clone());
        if let Some(thread) = state.thread_mut(thread_id) {
            thread.label = Some(label.clone());
        }

        label
    }

    /// Whether one conversation currently owns a live run (`sessions.ts:6376-6378`: the thread
    /// record's status OR the lane's active turn).
    #[must_use]
    pub(crate) fn thread_is_running(&self, state: &SessionState, thread_id: &str) -> bool {
        state
            .lanes
            .get(thread_id)
            .is_some_and(|lane| lane.active_turn.is_some())
            || state
                .threads
                .get(thread_id)
                .is_some_and(|thread| thread.status == "running")
    }

    /// Starts (or joins) one lane's pump (`sessions.ts:6655-6657`). `lane_key` is the conversation
    /// id, or `""` for the legacy serialized backlog.
    pub(crate) fn spawn_pump(&self, inner: &mut StoreInner, session_id: &str, lane_key: &str) {
        let Some(state) = inner.sessions.get_mut(session_id) else {
            return;
        };

        // A closing store never starts new work (`sessions.ts:8638 shuttingDown`); the flag a running
        // pump is holding is cleared by `end_pump`, so a graceful stop drains and exits.
        if inner.shutting_down {
            return;
        }

        // The flag is set SYNCHRONOUSLY before the spawn, so a concurrent submit never starts a
        // second worker for this lane (`sessions.ts:6656-6657`).
        let pumping = if lane_key.is_empty() {
            &mut state.legacy_pumping
        } else {
            &mut Self::lane_of(state, lane_key).pumping
        };

        if *pumping {
            return;
        }

        *pumping = true;

        // `Store` is `Clone` through `Arc` members, which is the cheap "strong handle" a detached task
        // needs: the pump outlives the request that spawned it, and a dropped `Store` clone keeps the
        // shared state alive exactly like the original.
        let store = self.clone();
        let session_id = session_id.to_string();
        let lane_key = lane_key.to_string();

        // The handle is deliberately NOT retained: `shutting_down` plus each run's cancellation token are
        // the cooperative stop, and a pump always exits by itself once its lane is empty. Keeping the
        // `JoinHandle` would make the store own a task that owns the store.
        tokio::spawn(async move { Self::pump_loop(store, session_id, lane_key).await });
    }

    /// Drains ONE lane, strictly FIFO, while sibling lanes keep running (`sessions.ts:6648-6699`
    /// `pumpLane`).
    async fn pump_loop(store: SessionStore, session_id: String, lane_key: String) {
        loop {
            let Some(plan) = store.claim_next_turn(&session_id, &lane_key) else {
                break;
            };

            // `key` is what settlement needs; `input` moves into the executor and is gone after this
            // point, which is why the two are destructured apart BEFORE the await.
            let RunKey {
                session_id: run_session,
                turn_id: run_turn,
                lane_key: run_lane,
                thread_id: run_thread,
                token,
            } = plan.key;
            let executor = Arc::clone(&store.executor);
            let input = plan.input;

            // The run executes with NO guard held, and a panic becomes a failed turn rather than a
            // wedged lane (`sessions.ts:6620-6632`, same last-resort guard).
            let outcome = AssertUnwindSafe(async move { executor.run(input).await })
                .catch_unwind()
                .await;

            let settled = match outcome {
                Ok(outcome) => outcome,
                Err(_) => contract::TurnOutcome {
                    usage: None,
                    error: Some(EXECUTOR_PANIC_MESSAGE.to_string()),
                    cancelled: token.is_cancelled(),
                    messages: Vec::new(),
                    stop_reason: None,
                    // A panic unwinds the executor with it, so nothing can be recovered: the drained
                    // guidance of a run that died inside the executor is reported as none rather than
                    // guessed at.
                    unused_steering: Vec::new(),
                },
            };

            let keep_draining = store.settle_turn(
                &run_session,
                &run_turn,
                &run_lane,
                run_thread.as_deref(),
                &token,
                settled,
            );

            if !keep_draining {
                break;
            }
        }

        store.end_pump(&session_id, &lane_key);
    }

    /// Claims the next queued turn of one lane and prepares its run
    /// (`sessions.ts:6660-6681` + the setup half of `executeTurn`, `:7046-7078`).
    ///
    /// Returns `None` when the lane is empty, the session is gone, the session was disposed, or the
    /// store is closing - in every one of those cases the pump exits and the lane flag is cleared by
    /// [`SessionStore::end_pump`].
    fn claim_next_turn(&self, session_id: &str, lane_key: &str) -> Option<RunPlan> {
        let mut inner = self.lock();

        if inner.shutting_down {
            return None;
        }

        // The CURRENT catalogue, cloned out of the guard BEFORE the session is borrowed mutably: the
        // instruction ladder reads the live global overlay, and `inner` cannot be borrowed twice.
        let catalogue = Arc::clone(&inner.host_agents);

        let state = inner.sessions.get_mut(session_id)?;

        if state.disposed {
            return None;
        }

        // A lane under a STOP request finishes its current turn and claims nothing else
        // (`sessions.ts:7530 stopRequested`): the backlog survives for the next explicit submit.
        if !lane_key.is_empty()
            && state
                .lanes
                .get(lane_key)
                .is_some_and(|lane| lane.stop_requested)
        {
            return None;
        }

        // Peek, never shift, until the turn is actually started (`sessions.ts:6593-6605`): FIFO
        // survives a cancel, a steer, and a concurrent submit.
        let turn_id = if lane_key.is_empty() {
            state.queue.first().cloned()
        } else {
            state
                .lanes
                .get(lane_key)
                .and_then(|lane| lane.queue.first().cloned())
        }?;

        let thread_id = if lane_key.is_empty() {
            None
        } else {
            Some(lane_key.to_string())
        };

        let started_at = now_millis();

        // The agent this turn runs as, decided BEFORE the turn is mutated: the conversation's own agent
        // when the submit named a conversation, else the harness's entry agent (`sessions.ts:7046-7060`).
        // It lands on the record's `extra`, because `SessionTurn` mirrors `protocol.ts` and has no column
        // for it, and every later reader (the recorder, the action host, the display row) resolves it the
        // same way through `TurnState::agent`.
        let agent = thread_id
            .as_deref()
            .and_then(|id| state.thread(id))
            .map(|thread| thread.agent.clone())
            .unwrap_or_else(|| state.primary_agent());
        let index = state.turns.get(&turn_id).map(|turn| turn.record.index)?;

        // DISPLAY LANE, read at the moment the turn is CLAIMED (`sessions.ts:8871-8874` reads
        // `contextLaneOf` HERE and `:8953` writes the field, only when it is non-zero). This is the
        // `startingLane` the projector pairs with every event stamp: a turn that starts on lane 1 — the
        // continuation after a compaction — paints its prompt and its answer on the SAME row
        // (`turn-projector.ts:1441 laneOfTurn`), and a turn that starts on lane 0 writes no key at all.
        let starting_lane =
            crate::persistence::LaneStamp::of(state.context_lane_of(thread_id.as_deref()));

        {
            let turn = state.turns.get_mut(&turn_id)?;

            // `status: running` + `startedAt` (`sessions.ts:7049-7053`): a queued turn has no startedAt,
            // and a crash after this write leaves an explicit interrupted turn rather than a prompt that
            // vanished.
            turn.record.status = "running".to_string();
            turn.record.lane = starting_lane;
            turn.record.started_at = Some(started_at);
            turn.record.updated_at = started_at;
            turn.record
                .extra
                .insert(store::EXTRA_AGENT.to_owned(), Value::String(agent.clone()));
        }

        // The claim happens HERE, not at peek time: the turn leaves the backlog only once it is actually
        // running, so a cancel or a crash never loses a queued task (`sessions.ts:6593-6605`).
        if let Some(lane) = state.lanes.get_mut(lane_key) {
            lane.queue.remove(0);
            lane.active_turn = Some(turn_id.clone());
            lane.stop_requested = false;
        } else {
            state.queue.remove(0);
        }

        // The conversation is running for as long as its lane owns a turn (`sessions.ts:6004`).
        if let Some(thread_id) = thread_id.as_deref() {
            state.set_thread_status(thread_id, contract::ThreadLifecycle::Running);
        }

        let token = CancellationToken::new();

        // The steering channel: one per conversation, owned by the run (`sessions.ts:6461-6495`). A
        // conversation that already has a channel (a run that was never settled) keeps the old one.
        let receiver = thread_id.as_deref().map(|thread_id| {
            let (sender, receiver) = mpsc::unbounded_channel();

            state.steer_tx.insert(thread_id.to_owned(), sender);

            receiver
        });

        // The run's own copy of the sender: `steer_into_thread` writes through `state.steer_tx`, and
        // dropping THIS end at settle is what makes a send into a finished run fail loudly.
        let steer_sender = thread_id
            .as_deref()
            .and_then(|id| state.steer_tx.get(id).cloned());

        // The conversation's accepted-but-undelivered guidance goes to the run that can use it
        // (`sessions.ts:6461-6495`, and `harness.ts:6831-6836`'s hand-back for a run that drained
        // steering and then failed). The mailbox is the durable half of that promise: a steer accepted
        // with no live channel, and the guidance a failed run restored, both wait here and must reach
        // the NEXT run in the order they were accepted - never overtaken by a fresh submit.
        if let Some(conversation) = thread_id.as_deref() {
            let pending = state.steer_mailbox.remove(conversation);

            if let Some(records) = pending {
                let count = u64::try_from(records.len()).unwrap_or(u64::MAX);

                if let Some(sender) = state.steer_tx.get(conversation).cloned() {
                    for record in records {
                        let _ = sender.send(record.prompt.clone());
                    }
                }

                state.steer_active = state.steer_active.saturating_sub(count);
            }
        }

        state.active.push(ActiveRun {
            turn_id: turn_id.clone(),
            index,
            thread_id: thread_id.clone().unwrap_or_default(),
            invocation_id: None,
            started_at,
            cancel: token.clone(),
            cancel_requested: false,
            steer_sender,
            // Filled in below, once the host exists: a tool call made OUTSIDE this run (the manual
            // session-tool route) reaches the same parked questions through it.
            action_sink: None,
            joiner: None,
        });

        // Aggregate status: any live run reports `running`, and a fresh run clears the error banner
        // only when nothing else is executing (`sessions.ts:7055-7066`).
        let others_active = state.active.len() > 1;

        state.record.status = "running".to_string();

        if !others_active {
            state.record.last_error = None;
        }

        state.record.updated_at = started_at;
        state.touch_idle(started_at, self.config.idle_ms);

        // The owner-question host of THIS run (`core/action.ts createSessionActionHost`). It holds the
        // store's lock handle rather than the `Store`, so registering a question cannot form an
        // ownership cycle.
        let action_host = Arc::new(crate::store::actions::StoreActionHost::new(
            self.inner_arc(),
            self.hub_arc(),
            session_id,
            &turn_id,
            thread_id.as_deref(),
            &agent,
        ));

        if let Some(run) = state.active.iter_mut().find(|run| run.turn_id == turn_id) {
            run.action_sink = Some(Arc::clone(&action_host));
        }

        // The instruction ladder is resolved HERE, before the run starts, and its provenance is recorded
        // on the turn: a reload must show exactly what the model was told (`record.turnInstructionSources`,
        // `persistence.ts:778`). The layer order is `session > harness > global > catalogue`
        // (`read::instruction_for`).
        let (system_instruction, _instruction_source, _guidelines) =
            state.instruction_for(&agent, &catalogue);

        if let Some(turn) = state.turns.get_mut(&turn_id) {
            turn.instruction_sources = vec![json!({
                "agent": agent,
                "instructions": [system_instruction.clone()],
                "at": started_at,
            })];
        }

        // The event sink closes over a `Store` CLONE (not a reference): the executor calls it from the
        // spawned task, long after this call has returned, and each call takes the guard for one short
        // recording step (`record_event`).
        let emit = {
            let store = self.clone();
            let session_id = session_id.to_string();
            let turn_id = turn_id.clone();

            Arc::new(move |event: HarnessEvent| {
                store.record_event(&session_id, &turn_id, event);
            }) as Arc<dyn Fn(HarnessEvent) + Send + Sync>
        };

        let input = self.build_input(
            &inner,
            session_id,
            &turn_id,
            thread_id.as_deref(),
            index,
            &agent,
            &system_instruction,
            &token,
            receiver,
            emit,
            Some(Arc::clone(&action_host)),
        )?;

        // Durability at the turn-start transition (`sessions.ts:7068-7078`): the running row exists
        // before any provider work, and a write failure fails the turn BEFORE it spends provider
        // time. The failure is surfaced as a terminal error through the same release path, so the
        // lane flag cannot get stuck.
        if let Err(error) = self.save(&inner) {
            let state = inner.sessions.get_mut(session_id)?;

            if let Some(turn) = state.turns.get_mut(&turn_id) {
                turn.record.status = "error".to_string();
                turn.record.updated_at = now_millis();
                turn.projector.settle("error", turn.record.updated_at);
            }

            state.record.last_error = Some(error.message.clone());
            state.active.retain(|run| run.turn_id != turn_id);
            state.record.status = "error".to_string();

            if let Some(lane) = state.lanes.get_mut(lane_key) {
                lane.active_turn = None;
            }

            // The row is read BEFORE the second write, because the write needs the guard on its own:
            // `save` serializes every session, and no session reference may be alive under it.
            let row = state
                .turns
                .get(&turn_id)
                .map(TurnState::row)
                .unwrap_or(Value::Null);

            let _ = self.save(&inner);

            store::publish(
                &self.hub,
                "turn.upsert",
                &turn_resources(session_id, &turn_id),
                Some(session_id),
                thread_id.as_deref(),
                Some(&turn_id),
                json!({ "turn": row }),
                Vec::new(),
            );

            return None;
        }

        // Open the turn frame before the run emits anything, so a listener never sees an event it
        // cannot attribute (`sessions.ts:7206-7221`).
        let state = inner.sessions.get(session_id)?;
        let row = state
            .turns
            .get(&turn_id)
            .map(TurnState::row)
            .unwrap_or(Value::Null);
        let patch = json!({ "session": store::read::session_state_patch_of(state, &self.hub) });

        store::publish(
            &self.hub,
            "turn.upsert",
            &turn_resources(session_id, &turn_id),
            Some(session_id),
            thread_id.as_deref(),
            Some(&turn_id),
            json!({ "turn": row }),
            Vec::new(),
        );

        store::publish(
            &self.hub,
            "session.updated",
            &[("session".to_string(), Some(session_id.to_string()))],
            Some(session_id),
            thread_id.as_deref(),
            Some(&turn_id),
            patch,
            Vec::new(),
        );

        Some(RunPlan {
            key: RunKey {
                session_id: session_id.to_string(),
                turn_id,
                lane_key: lane_key.to_string(),
                thread_id,
                token,
            },
            input,
        })
    }

    /// Releases one lane's pump flag and announces the drain when the whole session settled
    /// (`sessions.ts:6688-6698` `pumpLane`'s `finally`).
    fn end_pump(&self, session_id: &str, lane_key: &str) {
        let mut inner = self.lock();

        let Some(state) = inner.sessions.get_mut(session_id) else {
            return;
        };

        if lane_key.is_empty() {
            state.legacy_pumping = false;
        } else if let Some(lane) = state.lanes.get_mut(lane_key) {
            lane.pumping = false;
        }

        if state.disposed {
            return;
        }

        // The drain frame rides the SAME rule as TS's lane pump: publish only when NOTHING else is
        // live, so a sibling conversation that is still executing never receives a premature
        // "everything settled" (`sessions.ts:6691-6695`).
        if state.pending_count() > 0 || state.lane_active() || state.active_run_count() > 0 {
            return;
        }

        let patch = json!({ "session": store::read::session_state_patch_of(state, &self.hub) });

        store::publish(
            &self.hub,
            "session.updated",
            &[("session".to_string(), Some(session_id.to_string()))],
            Some(session_id),
            None,
            None,
            patch,
            Vec::new(),
        );
    }

    // -----------------------------------------------------------------------
    // Settlement
    // -----------------------------------------------------------------------

    /// Settles one finished run (`sessions.ts:7341-7465`: the status decision, the `finally` block,
    /// and the terminal publish/save).
    ///
    /// Takes the run's identity as plain parts rather than a `&RunPlan`, because the plan's `TurnInput`
    /// has already been consumed by the executor and the settlement still has to name the run.
    ///
    /// Returns whether the pump should keep draining this lane.
    #[allow(clippy::too_many_arguments)]
    fn settle_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        lane_key: &str,
        thread_id: Option<&str>,
        token: &CancellationToken,
        outcome: contract::TurnOutcome,
    ) -> bool {
        let at = now_millis();
        let mut inner = self.lock();

        let Some(state) = inner.sessions.get_mut(session_id) else {
            return false;
        };

        if state.disposed {
            return false;
        }

        let cancelled = token.is_cancelled()
            || state
                .active_run_of(turn_id)
                .is_some_and(|run| run.cancel_requested);

        // A run that settled as an abort is terminal as `cancelled` even if the transport returned
        // instead of throwing (`sessions.ts:7341-7348`).
        let status = if cancelled {
            "cancelled"
        } else if outcome.error.is_some() {
            "error"
        } else {
            "done"
        };

        let failure = if cancelled {
            CANCELLED_RUN_MESSAGE.to_string()
        } else {
            outcome.error.clone().unwrap_or_default()
        };

        let Some(turn) = state.turns.get_mut(turn_id) else {
            return false;
        };

        turn.record.status = status.to_string();
        turn.record.updated_at = at;

        if let Some(usage) = outcome.usage.clone() {
            // The executor's tally is authoritative when the run reported one without `usage` events
            // (a provider that answers once); the event-folded tally wins when BOTH exist, because
            // the events are the durable record (`sessions.ts:7139-7144`).
            turn.record.usage = match turn.record.usage.take() {
                Some(folded) => Some(folded),
                None => Some(usage),
            };
        }

        if let Some(reason) = outcome.stop_reason.clone() {
            turn.record
                .extra
                .insert(store::EXTRA_STOP_REASON.to_owned(), Value::String(reason));
        }

        // The invocation identity is NOT part of `TurnOutcome`: the run reports it in its `start` event,
        // which the recorder stamps onto the turn record (`record_event`) and into
        // `record.invocationThreads`. Nothing to fold here.

        // The transcript is the RUNTIME's object in TS (`harness.sendThreadInput` appends into the
        // agent history the snapshot commits). The Rust executor returns the full updated
        // transcript, so the store REPLACES the conversation's messages - appending would double the
        // prompt. An empty vector means "the executor changed nothing", which is how a failed or
        // cancelled run keeps the transcript it had.
        let transcript_changed = !outcome.messages.is_empty();

        // The fold closes HERE, at the terminal status, before the session is touched again: `turn` is a
        // mutable borrow of `state.turns`, and the transcript write below needs `state.threads`.
        turn.projector.settle(status, at);

        if transcript_changed
            && let Some(conversation) = thread_id
            && let Some(thread) = state.thread_mut(conversation)
        {
            thread.messages = outcome.messages.clone();

            // The transcript counter moves with EVERY replacement, a run's own included. The thread row
            // carries the durable mirror (`set_transcript` bumps it for a route write, and the checkpoint
            // persists it), while the hub counter is what every read quotes; TypeScript has one figure for
            // the document read, the `if_revision` guard and the receipt (`api.ts:4361`, `:4390`, `:4765`),
            // so a run that left the mirror behind would make the next block write guard against a revision
            // nobody ever read.
            thread.transcript_revision = Some(thread.transcript_revision.unwrap_or(0) + 1);
        }

        // Release the slot exactly once, in this run's settle (`sessions.ts:7357-7375`).
        state.active.retain(|run| run.turn_id != turn_id);

        if let Some(lane) = state.lanes.get_mut(lane_key)
            && lane.active_turn.as_deref() == Some(turn_id)
        {
            lane.active_turn = None;
            lane.stop_requested = false;
        }

        if let Some(conversation) = thread_id {
            // The channel dies with the run: a steer accepted after this point waits in the mailbox
            // for the NEXT turn (`sessions.ts:6461-6495`).
            state.steer_tx.remove(conversation);

            // The invocation id is read OUT of the mutable borrow and applied after it, because the
            // record map below is a second borrow of the same session.
            let mut invocation_id = None;

            if let Some(thread) = state.thread_mut(conversation) {
                if thread.status == "running" {
                    thread.status =
                        store::lifecycle_str(contract::ThreadLifecycle::Idle).to_owned();
                }

                invocation_id = thread.invocation_id.clone();
            }

            // The invocation identity the roster joins on (`sessions.ts:7333-7339`): the thread keeps the
            // id its root run reported, and the record maps it back to THIS turn.
            if let Some(invocation_id) = invocation_id {
                let mut map = state.record.invocation_threads.clone().unwrap_or_default();
                map.insert(invocation_id, turn_id.to_owned());
                state.record.invocation_threads = Some(map);
            }
        }

        // N4: an unanswered question cannot outlive its run (`sessions.ts:7377-7382`), scoped to THIS
        // conversation so a sibling lane's question stays answerable.
        let closed_actions = state.expire_actions(ACTION_RUN_ENDED_MESSAGE, thread_id);

        // R5 preservation (`harness.ts:6826-6837`): steering this run drained but never showed the
        // model is rolled back WITH the transcript, so it goes back to the conversation instead of
        // vanishing. Two arms of that rule are load-bearing:
        //   * a STOP discards it - `cancelled` here is the same fact as `run.aborted`, and a cancel-all
        //     or a thread stop has already said this conversation should not continue;
        //   * a thread whose record is `stopped` discards it, because `stopThread` cleared its queue.
        // Ordering note: this runs AFTER `expire_actions`, which subtracts the conversation's mailbox
        // from `steer_active` - re-queueing first would have the expiry eat the guidance just restored.
        if !outcome.unused_steering.is_empty()
            && !cancelled
            && let Some(conversation) = thread_id
        {
            let stopped = state.thread(conversation).is_some_and(|thread| {
                thread.status == store::lifecycle_str(contract::ThreadLifecycle::Stopped)
            });

            if !stopped {
                let requeued = u64::try_from(outcome.unused_steering.len()).unwrap_or(u64::MAX);

                for text in &outcome.unused_steering {
                    state
                        .steer_mailbox
                        .entry(conversation.to_owned())
                        .or_default()
                        .push(store::steer_record(
                            ids::new_id(),
                            at,
                            text.clone(),
                            None,
                            None,
                        ));
                }

                state.steer_active = state.steer_active.saturating_add(requeued);
            }
        }

        state.record.updated_at = at;
        state.touch_idle(at, self.config.idle_ms);

        // Aggregate session status: only the LAST live run decides idle/error, and a success clears a
        // sibling's banner (`sessions.ts:7390-7405`). Failure AND cancellation report an error with
        // `last_error`, while the turn itself still records `cancelled`.
        let others = state.lane_active() || state.active_run_count() > 0;

        if others {
            state.record.status = "running".to_string();
        } else if status == "done" {
            state.record.status = "idle".to_string();
            state.record.last_error = None;
        } else {
            state.record.status = "error".to_string();
            state.record.last_error = Some(failure.clone());
        }

        // A transcript the run replaced is a new DOCUMENT for that conversation: the hub counter a
        // transcript page reports has to move with it, or a client watching `transcript` never learns to
        // re-read (`api-events.ts:302-306` — the writer owns the bump).
        if transcript_changed && let Some(conversation) = thread_id {
            let _ = self.hub.bump(
                "transcript",
                Some(&store::thread_key(session_id, conversation)),
            );
        }

        // The terminal save is a CHECKPOINT, not a gate (`sessions.ts:7428-7440`
        // `persistBestEffort`): the run has already settled, and a disk failure must not rewrite the
        // outcome the provider produced. The failure is recorded and the frames still go out.
        if let Err(error) = self.save(&inner) {
            inner.journal_problems.push(format!(
                "session \"{session_id}\" could not be persisted: {}",
                error.message
            ));
        }

        let Some(state) = inner.sessions.get(session_id) else {
            // The session was removed while this run was in flight: nothing left to announce.
            return false;
        };

        let row = state
            .turns
            .get(turn_id)
            .map(TurnState::row)
            .unwrap_or(Value::Null);

        store::publish(
            &self.hub,
            "turn.upsert",
            &turn_resources(session_id, turn_id),
            Some(session_id),
            thread_id,
            Some(turn_id),
            json!({ "turn": row }),
            Vec::new(),
        );

        // `usage.updated` is announced once per DISTINCT figure set (`api.ts:1082-1092`): the store
        // may reach the terminal tally on more than one frame of one transition.
        let usage_patch = store::read::usage_patch_of(state, turn_id, &self.hub);

        if let Some(patch) = usage_patch {
            store::publish(
                &self.hub,
                "usage.updated",
                &[("usage".to_string(), Some(session_id.to_string()))],
                Some(session_id),
                thread_id,
                Some(turn_id),
                json!({ "usage_patch": patch, "turn_id": turn_id }),
                Vec::new(),
            );
        }

        let patch = json!({ "session": store::read::session_state_patch_of(state, &self.hub) });

        store::publish(
            &self.hub,
            "session.updated",
            &[("session".to_string(), Some(session_id.to_string()))],
            Some(session_id),
            thread_id,
            Some(turn_id),
            patch,
            Vec::new(),
        );

        // T16: the run ending settled every unanswered question of this conversation, so each one
        // announces `action.closed` with reason `ended` (`sessions.ts:2325-2335` publishes from the ONE
        // settlement point, and `api.ts:1126-1138` moves `actions` once per settlement).
        self.publish_actions_closed(
            session_id,
            &closed_actions,
            store::actions::action_closed_reason(ACTION_RUN_ENDED_MESSAGE),
        );

        // A cancelled session (removed or disposed mid-run) stops the pump entirely; a cancelled TURN
        // keeps the lane draining, because the backlog was not dropped (`sessions.ts:7398-7411`).
        !state.disposed
    }

    // -----------------------------------------------------------------------
    // The recorder
    // -----------------------------------------------------------------------

    /// Records one event on one turn and fans it out (`sessions.ts:7092-7201` `record`).
    ///
    /// The order inside is TS's order, and every step of it is load-bearing:
    ///   1. capture the event's POSITION before the push - it is the address a display step carries;
    ///   2. append the event and its parallel timestamp: ONE `at` for the record, the journal line,
    ///      and the published frame, so a live view and a reload agree exactly;
    ///   3. journal it - best-effort by POLICY, since a full disk must not change a run's outcome,
    ///      and never silent, since the failure is retained and reported;
    ///   4. fold the usage tally HERE, so every client reads the SERVER's figure and the `done`
    ///      event's repeated usage cannot double the last step;
    ///   5. fold the display projector, the cache every compact read serves;
    ///   6. checkpoint the whole document on a bounded interval, never once per step;
    ///   7. publish.
    pub(crate) fn record_event(&self, session_id: &str, turn_id: &str, event: HarnessEvent) {
        let mut inner = self.lock();

        // The recorder writes to TWO halves of the store in one guard region — the turn row (in `sessions`)
        // and the replay log (in `journals`) — so the guard is destructured into its fields, which is the
        // only way Rust lets both be reached without aliasing the guard itself.
        let store::StoreInner {
            sessions,
            journals,
            journal_problems,
            ..
        } = &mut *inner;

        let Some(state) = sessions.get_mut(session_id) else {
            return;
        };

        if state.disposed {
            return;
        }

        let at = now_millis();

        // R4: the journal carries every step, so the whole document is rewritten on a BOUNDED interval
        // rather than once per step (`sessions.ts:7152-7156`).
        let checkpoint_due =
            at.saturating_sub(state.checkpoint_at) >= store::CHECKPOINT_INTERVAL_MS;

        // DISPLAY LANE, read LIVE per event (`sessions.ts:7925-7936`): the counter is read from the
        // CONVERSATION before every event rather than cached at turn start, so a mid-run compaction
        // takes effect on the very next event and one turn's bar can show both rows. Read here, outside
        // the `turns` borrow below, because the counter lives in the conversation roster.
        let lane = state
            .turns
            .get(turn_id)
            .and_then(|turn| turn.record.thread_id.as_deref())
            .and_then(|thread_id| state.context_lane_of(Some(thread_id)));

        let (index, ops, thread_id, usage) = {
            let Some(turn) = state.turns.get_mut(turn_id) else {
                return;
            };

            // 1. the event's POSITION before the push is the address a display step carries;
            // 2. ONE `at` for the record, the journal line and the published frame, so a live view and a
            //    reload agree exactly;
            // 5. the display fold, the cache every compact read serves.
            // All three happen inside `TurnState::record_event`, which is the ONLY place display ops are
            // minted — that is what makes "live == replay" true by construction.
            let index = u64::try_from(turn.record.events.len()).unwrap_or(u64::MAX);
            let ops = turn.record_event(&event, at, lane);

            // 4. the token tally, folded from the per-request `usage` event only (`:7139-7144`), so a
            //    client never recomputes it and a `done` event cannot double the last step.
            if let HarnessEvent::Usage { usage, .. } = &event {
                turn.record.usage = Some(add_usage(turn.record.usage.as_ref(), usage));
            }

            // F6: the invocation a `start` frame names rides the turn record, which is where
            // `SessionState::turn_invocations` reads it (`sessions.ts:7120-7131`).
            if let HarnessEvent::Start {
                thread: Some(invocation),
                ..
            } = &event
                && invocation.kind.is_none()
                && invocation.branch_id.is_none()
            {
                turn.record.extra.insert(
                    store::EXTRA_INVOCATION.to_owned(),
                    Value::String(invocation.id.clone()),
                );
            }

            (
                index,
                ops,
                turn.record.thread_id.clone(),
                turn.record.usage.clone(),
            )
        };

        // F6: remember which conversation each invocation ran in (`sessions.ts:7120-7131`) — the
        // session-level map the roster joins on. Only a root host run or a delegated child's `start`
        // names one; branch frames are transcript-ephemeral and deliberately skipped.
        if let HarnessEvent::Start {
            thread: Some(invocation),
            ..
        } = &event
            && invocation.kind.is_none()
            && invocation.branch_id.is_none()
        {
            note_invocation_thread(state, &invocation.id);
        }

        state.record.updated_at = at;

        if checkpoint_due {
            state.checkpoint_at = at;
        }

        // 3. journal append, best-effort BY POLICY (`sessions.ts:1121-1131`): a full disk must not change
        //    a run's outcome, and the refusal is retained and reported rather than swallowed.
        // The position the journal line carries is the TURN's, not the file's: `turn_id` + `index`
        // are the same address a display step id carries, and `at` is the timestamp step 2 already
        // stamped the record with (`sessions.ts:7930-7961`, `journal.ts:292-300`).
        if let Some(journal) = journal_of(
            journals,
            journal_problems,
            &self.config.sessions_dir(),
            session_id,
        ) && let Err(error) = journal.append(turn_id, index, at, &event, false)
        {
            journal_problems.push(error.message);
        }

        // 6. the bounded checkpoint. The guard is RE-READ afterwards (not reused) because `save` serializes
        //    the whole store, which needs the guard immutably.
        if checkpoint_due {
            let _ = self.save(&inner);
        }

        // 7. publish: `run.event` for the raw stream, `display.ops` for the compact board, with the three
        //    turn counters moved exactly once (`api.ts:1011-1067`).
        let row = json!({
            "index": index,
            "at": at,
            "event": event,
            "usage": usage,
        });

        publish_event_frames(
            self,
            session_id,
            turn_id,
            thread_id.as_deref(),
            row,
            ops,
            &mut inner,
        );
    }

    // -----------------------------------------------------------------------
    // Input construction
    // -----------------------------------------------------------------------

    /// Assembles the [`TurnInput`] one claimed run executes with (`ports::TurnInput`).
    ///
    /// Every field is read from the session UNDER the guard, because the store owns the instruction ladder,
    /// the transcript, the tool policy and the provider trio: an executor must never be able to run with a
    /// different prompt than the one this stage recorded on the turn (`knowledge.md`, "the record IS the
    /// session as the document sees it").
    #[allow(clippy::too_many_arguments)]
    fn build_input(
        &self,
        inner: &StoreInner,
        session_id: &str,
        turn_id: &str,
        thread_id: Option<&str>,
        index: u64,
        agent: &str,
        system_instruction: &str,
        token: &CancellationToken,
        steering: Option<mpsc::UnboundedReceiver<String>>,
        emit: Arc<dyn Fn(HarnessEvent) + Send + Sync>,
        action_host: Option<Arc<crate::store::actions::StoreActionHost>>,
    ) -> Option<TurnInput> {
        let state = inner.find(session_id)?;
        let turn = state.turn(turn_id)?;

        Some(TurnInput {
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
            index,
            prompt: turn.record.prompt.clone(),
            // `TurnInput::thread_id` is a plain String: a thread-less turn (a slash command) has no
            // conversation to address, and the empty string is the store's spelling of that.
            thread_id: thread_id.unwrap_or_default().to_owned(),
            system_instruction: system_instruction.to_owned(),
            // The conversation's committed transcript, verbatim — the projector owns the DISPLAY history;
            // the transcript is the model's input (`sessions.ts:7071-7076`).
            transcript: thread_id
                .and_then(|thread_id| state.thread_messages(thread_id))
                .unwrap_or_default(),
            tools: self.declarations_for(state),
            // The trio is the session override merged over the deployment, and the MODEL is the agent's
            // own: `agentModels[agent]` wins over the session model (`read::agent_model`,
            // `api-reads.ts:740-752`), which is the figure the roster row advertises, so a run never uses
            // a model its own row denies.
            provider: {
                let mut provider = self.provider_of(inner, state);
                provider.model = state.agent_model(agent);

                provider
            },
            attachments: turn.record.attachments.clone().unwrap_or_default(),
            signal: token.clone(),
            steering,
            cwd: self.cwd(),
            emit,
            tool_runtime: Arc::clone(&self.runtime),
            // `None` removes `actionRequest` from the run's tool set, which is exactly TS's condition
            // (`src/harness.ts`: presence registers the tool).
            action_host: action_host.map(|host| host as Arc<dyn crate::ports::ActionHost>),
            task_host: self
                .task_host
                .as_ref()
                .map(|host| Arc::clone(host) as Arc<dyn crate::ports::TaskHost>),
            max_iterations: PRESET_MAX_ITERATIONS,
        })
    }
}

/// The session's replay log, opened on first use (`sessions.ts:4133-4165` `attachJournal`).
///
/// It lives in [`StoreInner::journals`] rather than on [`SessionState`] because a [`HarnessJournal`] owns
/// an open file handle and is not `Clone`, while a session state must stay restorable. A log that cannot be
/// opened is a REPORTED degradation, never a failed run (`sessions.ts:1121-1131`), which is why the
/// problems list is passed in next to the map.
fn journal_of<'a>(
    journals: &'a mut std::collections::BTreeMap<String, HarnessJournal>,
    problems: &mut Vec<String>,
    sessions_dir: &std::path::Path,
    session_id: &str,
) -> Option<&'a mut HarnessJournal> {
    if !journals.contains_key(session_id) {
        match HarnessJournal::open(sessions_dir, session_id, None) {
            Ok(journal) => {
                journals.insert(session_id.to_owned(), journal);
            }
            Err(error) => {
                problems.push(format!(
                    "journal for session \"{session_id}\" is unavailable: {}",
                    error.message
                ));

                return None;
            }
        }
    }

    journals.get_mut(session_id)
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Publishes the two frames one recorded event produces (`run.event` for the raw stream,
/// `display.ops` for the compact board), after moving the three turn counters exactly once.
#[allow(clippy::too_many_arguments)]
fn publish_event_frames(
    store: &SessionStore,
    session_id: &str,
    turn_id: &str,
    thread_id: Option<&str>,
    row: Value,
    ops: Vec<Value>,
    inner: &mut StoreInner,
) {
    let _ = inner;

    store::publish(
        &store.hub,
        "run.event",
        &[("turn".to_string(), Some(turn_key(session_id, turn_id)))],
        Some(session_id),
        thread_id,
        Some(turn_id),
        row,
        Vec::new(),
    );

    // The counters moved with the frame above; `display.ops` REPORTS the same numbers
    // (`api.ts:1046-1067`, which bumps the trio and publishes both kinds from one signal).
    let _ = store.hub.publish_with_ops(
        "display.ops",
        &[crate::events::ResourceRef::new(
            "turn_display",
            Some(turn_key(session_id, turn_id)),
        )],
        Some(session_id),
        thread_id,
        Some(turn_id),
        json!({ "turn_id": turn_id }),
        ops,
    );
}

/// `sessions.ts:7104-7144` `addUsage`: the running tally of two provider reports. `cached` stays
/// absent unless BOTH sides reported one, because "not reported" and "reported zero" are different
/// facts (`types/index.ts:227-230`).
#[must_use]
pub fn add_usage(
    prior: Option<&contract::HarnessUsage>,
    next: &contract::HarnessUsage,
) -> contract::HarnessUsage {
    let prior = prior.cloned().unwrap_or(contract::HarnessUsage {
        input: 0,
        output: 0,
        total: 0,
        cached: None,
    });

    contract::HarnessUsage {
        input: prior.input + next.input,
        output: prior.output + next.output,
        total: prior.total + next.total,
        cached: match (prior.cached, next.cached) {
            (Some(prior), Some(next)) => Some(prior + next),
            (prior, None) => prior,
            (None, next) => next,
        },
    }
}

/// `noteInvocationThread` (`sessions.ts:7738-7747`): an invocation already mapped is left alone,
/// because ids are unique and never reused.
fn note_invocation_thread(state: &mut SessionState, invocation_id: &str) {
    let mut map = state.record.invocation_threads.clone().unwrap_or_default();

    if map.contains_key(invocation_id) {
        return;
    }

    for thread in state.threads.values() {
        if thread.invocation_id.as_deref() == Some(invocation_id) {
            map.insert(invocation_id.to_string(), thread.id.clone());

            break;
        }
    }

    state.record.invocation_threads = Some(map);
}

/// `turnKey`-scoped resources of one turn (`api.ts:1011-1019`): the three turn kinds are folds of
/// one record, and the collection counter moves with them.
#[must_use]
pub(crate) fn turn_resources(session_id: &str, turn_id: &str) -> Vec<(String, Option<String>)> {
    let key = turn_key(session_id, turn_id);

    vec![
        ("turn".to_string(), Some(key.clone())),
        ("turn_display".to_string(), Some(key.clone())),
        ("turn_events".to_string(), Some(key)),
        ("turns".to_string(), Some(session_id.to_string())),
    ]
}

/// `normalizeThreadId` (`sessions.ts:6342-6350`): a non-string or blank value is a 400 BEFORE any
/// mutation; an unknown id is a 404, which the caller answers.
pub(crate) fn normalize_thread_id(value: Option<&str>) -> Result<Option<String>, ApiError> {
    let Some(raw) = value else {
        return Ok(None);
    };

    let trimmed = raw.trim();

    if trimmed.is_empty() {
        return Err(ApiError::bad_request(THREAD_ID_SHAPE_MESSAGE));
    }

    Ok(Some(trimmed.to_string()))
}

/// The queued turn record (`sessions.ts:5926-5942`): `queued`, no `startedAt`, empty event arrays,
/// and BOTH optional lists omitted when empty so a plain task keeps the historical shape.
#[must_use]
#[allow(clippy::too_many_arguments)]
fn build_queued_turn(
    id: &str,
    index: u64,
    prompt: &str,
    thread_id: Option<&str>,
    thread_label: Option<&str>,
    references: &[contract::AgentResultReference],
    attachments: &[contract::HarnessAttachment],
    now: u64,
) -> SessionTurn {
    SessionTurn {
        id: id.to_string(),
        index,
        prompt: prompt.to_string(),
        thread_id: thread_id.map(str::to_string),
        thread_label: thread_label.map(str::to_string),
        // A QUEUED turn carries no lane: the lane is read when the turn STARTS
        // (`sessions.ts:8871-8874`), so a task that sat in the backlog across a compaction starts on
        // the lane that is current when it is claimed, not on the one that was current when it was
        // submitted. A turn refused before it started is the exception and copies the lane it was
        // handed (`:6727-6774`).
        lane: None,
        references: (!references.is_empty()).then(|| references.to_vec()),
        attachments: (!attachments.is_empty()).then(|| attachments.to_vec()),
        status: "queued".to_string(),
        started_at: None,
        updated_at: now,
        events: Vec::new(),
        event_times: Some(Vec::new()),
        usage: None,
        steers: None,
        steer_texts: None,
        extra: serde_json::Map::new(),
    }
}

/// `normalizeReferences` + `bindReference` (`sessions.ts:5155-5220`, `:5601-5673`), ported to the
/// turn-level binding this stage can honour: a reference names a PRIOR turn, and the result-block
/// label resolution belongs to the projector (T4), which is why a label that names no block is not
/// refused here.
///
/// Refusals, verbatim: an unknown turn, a forward reference, and the 16-reference cap.
pub(crate) fn bind_references(
    state: &SessionState,
    turn_index: u64,
    references: Option<&[contract::AgentResultReference]>,
) -> Result<Vec<contract::AgentResultReference>, ApiError> {
    let Some(references) = references else {
        return Ok(Vec::new());
    };

    let mut bound: Vec<contract::AgentResultReference> = Vec::with_capacity(references.len());

    for reference in references {
        let target = state
            .turns
            .get(&reference.turn_id)
            .map(|turn| turn.record.clone())
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "Referenced turn \"{}\" does not exist in this session.",
                    reference.turn_id
                ))
            })?;

        // A reference is always BACKWARD (`sessions.ts:5616-5621`); this also makes a queued target
        // safe, because the target precedes the pump head that will expand it.
        if target.index >= turn_index {
            return Err(ApiError::bad_request(format!(
                "A reference must target an earlier block; turn \"{}\" is not earlier than this task.",
                target.id
            )));
        }

        // A terminal target resolves at EXECUTION in TS; a running/queued one keeps the caller's
        // binding (`:5625-5631`), and the same rule holds here.
        bound.push(reference.clone());
    }

    if bound.len() > crate::contract::MAX_RESULT_REFERENCES {
        return Err(ApiError::bad_request(format!(
            "A task cannot reference more than {} result blocks.",
            crate::contract::MAX_RESULT_REFERENCES
        )));
    }

    Ok(bound)
}

/// `steerIntoThread`'s audit half (`sessions.ts:6420-6503`).
///
/// The steer lands on the turn that OWNS the running conversation: a focused submit adds no turn
/// record, so the response names the owning one and its event log is where the steered text appears.
/// The ring is bounded at 64 entries; the TEXT archive is unbounded, because an accepted submit is
/// the user's own data (`:6448-6472`).
impl SessionStore {
    fn steer_into_thread(
        &self,
        state: &mut SessionState,
        thread_id: &str,
        input: &str,
        references: Option<&[contract::AgentResultReference]>,
        at: u64,
    ) -> Result<Option<TaskReceipt>, ApiError> {
        let Some(sender) = state.steer_tx.get(thread_id).cloned() else {
            return Ok(None);
        };

        let owning = Self::owning_turn_of(state, thread_id);

        let Some(owning_turn_id) = owning else {
            // A run with no recorded owning turn cannot report what accepted the guidance.
            return Err(ApiError::conflict(format!(
                "Thread \"{thread_id}\" is running but no recorded turn owns it; stop it before submitting."
            )));
        };

        let submit_id = ids::new_id();
        let references = references.map(|references| references.to_vec());
        // The session id is taken FIRST: `turn` is a mutable borrow of `state.turns`, and the announce
        // step below needs `state.record` — the borrow checker cannot see that the two are disjoint.
        let session_id = state.record.id.clone();

        // Attribute the steer to the conversation it actually reached; a steer into the owner's own
        // thread omits the field, keeping the pre-T12b wire shape (`:6434-6440`).
        let attributed = state
            .turns
            .get(&owning_turn_id)
            .and_then(|owner| owner.record.thread_id.clone())
            .filter(|owner_thread| owner_thread != thread_id);

        // Assigned inside the borrow block below: the row a steer announces IS the owning turn's row
        // after the steer landed, and `turn.row()` needs the mutable turn borrow.
        let row: Value;

        {
            let Some(turn) = state.turns.get_mut(&owning_turn_id) else {
                return Ok(None);
            };

            let record = store::steer_record(
                submit_id.clone(),
                at,
                input.to_owned(),
                attributed.clone(),
                references.clone(),
            );

            let mut ring = turn.record.steers.clone().unwrap_or_default();
            ring.push(record);

            while ring.len() > crate::contract::MAX_STEER_RECORDS {
                ring.remove(0);
            }

            turn.record.steers = Some(ring);

            let mut archive = turn.record.steer_texts.clone().unwrap_or_default();
            archive.push(json!({
                "id": submit_id,
                "at": at,
                "prompt": input,
                "threadId": attributed,
            }));
            turn.record.steer_texts = Some(archive);

            turn.record.updated_at = at;

            // The row is read under the same borrow, before the session's own `updatedAt` moves.
            row = turn.row();
        }

        state.record.updated_at = at;

        // Deliver the text. A closed channel means the run settled between the check and the send,
        // and the submit then becomes a queued continuation like any other (`:6377-6378`).
        if sender.send(input.to_owned()).is_err() {
            return Ok(None);
        }

        // The session counter moves BEFORE the frame that reports it (`api-events.ts:302-306`), and the
        // receipt carries the same figure the frame printed.
        let session_revision = self.hub.bump("session", Some(&session_id));

        store::publish(
            &self.hub,
            "turn.upsert",
            &[(
                "turn".to_string(),
                Some(turn_key(&session_id, &owning_turn_id)),
            )],
            Some(&session_id),
            Some(thread_id),
            Some(&owning_turn_id),
            json!({ "turn": row }),
            Vec::new(),
        );

        Ok(Some(receipt(
            &session_id,
            &owning_turn_id,
            contract::SessionTurnStatus::Running,
            Some(thread_id),
            Some(thread_id),
            None,
            session_revision,
            at,
        )))
    }

    /// `owningTurnOf` (`sessions.ts:6510-6539`): the recorded turn that owns a running
    /// conversation - its own lane, an ancestor lane's turn, else the newest running turn.
    #[must_use]
    fn owning_turn_of(state: &SessionState, thread_id: &str) -> Option<String> {
        if let Some(turn_id) = state
            .lanes
            .get(thread_id)
            .and_then(|lane| lane.active_turn.clone())
        {
            return Some(turn_id);
        }

        // The owner chain: a delegated child runs inside its parent's turn.
        let mut seen = vec![thread_id.to_string()];
        let mut current = state
            .threads
            .get(thread_id)
            .and_then(|thread| thread.owner_thread_id.clone());

        while let Some(owner) = current {
            if seen.contains(&owner) {
                break;
            }

            seen.push(owner.clone());

            if let Some(turn_id) = state
                .lanes
                .get(&owner)
                .and_then(|lane| lane.active_turn.clone())
            {
                return Some(turn_id);
            }

            current = state
                .threads
                .get(&owner)
                .and_then(|thread| thread.owner_thread_id.clone());
        }

        state
            .turns_in_index_order(false)
            .into_iter()
            .filter(|turn| turn.record.status == "running")
            .rfind(|turn| turn.record.thread_id.as_deref() == Some(thread_id))
            .or_else(|| {
                state
                    .turns_in_index_order(false)
                    .into_iter()
                    .rfind(|turn| turn.record.status == "running")
            })
            .map(|turn| turn.record.id.clone())
    }
}

// The `start` frame's invocation identity is `contract::HarnessThread`; the recorder destructures it
// directly (`record_event`), and the session-level join lives in `note_invocation_thread`.
