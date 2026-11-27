//! Conversations: the roster a session's work is addressed at, and its lifecycle.
//!
//! TypeScript origin: `packages/underload/harness/src/server/api.ts`'s `/sessions/:id/threads` routes and
//! the conversation half of `sessions.ts` (`createThread`, `threadRowsOf`, `deleteThread`). The rows
//! themselves are [`read::thread_row_of`] / [`read::thread_resource_of`]; this module owns the roster:
//! minting an id and a label, admitting a task to a conversation, and removing one.
//!
//! WHY A CONVERSATION IS A RECORD, NOT A RUN
//! -----------------------------------------
//! A [`HarnessThreadRecord`] is durable: it carries the conversation's committed transcript and its
//! transcript revision, so the transcript editor (`transcript.rs`) and a restarted store read the same
//! object a live run writes. The RUN half lives in [`SessionState::lanes`] and is rebuilt on restore,
//! which is why deleting a conversation only has to refuse while a lane is busy.
//!
//! Two counters, deliberately apart (`persistence.rs:177-181` names the durable one `threadSequence`
//! while it counts TURNS): [`SessionState::thread_seq`] mints conversation ordinals and
//! [`SessionState::turn_seq`] mints turn ordinals. See knowledge.md "Decisions".

use serde_json::{Value, json};

use crate::contract::{self, HarnessMessage, ThreadLifecycle, ThreadOrigin};
use crate::error::{ApiError, HarnessResult};
use crate::ids;
use crate::paging;
use crate::persistence::HarnessThreadRecord;
use crate::ports::Query;
use crate::store::{
    self, SessionState, SessionStore, read, session::paging_query, session_busy_error,
};

/// Conversations one session may hold (`contract::MAX_THREAD_RECORDS`, `persistence.ts:107`
/// `threadRecords: 256`). Each route states the refusal ITS OWN way, and the two codes are different:
/// the roster route answers [`thread_cap_roster_error`] (500, the runtime sentence unwrapped) and an
/// unfocused submit answers [`thread_cap_submit_error`] (429, the sentence wrapped in the store's
/// context). Neither is a conflict, and the difference is the reference's, not a choice here.
pub const MAX_THREADS: usize = crate::contract::MAX_THREAD_RECORDS;

/// A conversation the caller named that this session does not have (`api.ts:2500`), byte-identical.
#[must_use]
pub fn unknown_thread(thread_id: &str) -> ApiError {
    ApiError::not_found(format!("Unknown thread \"{thread_id}\"."))
}

/// The roster is full, as the ROSTER route answers it (`POST /sessions/{id}/threads`): **500** carrying
/// the runtime's own remedy sentence, UNWRAPPED.
///
/// This is the reference's answer, not a classification this build makes. The chain, verified at
/// `packages/underload/harness` @ `e66bce87777cb5f53504dbc47ad38ebd2c0ca6ff`:
///   * `server/api.ts:5258-5304` `postSessionThread` calls `record.harness.createThread(...)` at
///     `:5277-5281` with NO `try`/`catch` around it — of the 21 `catch` blocks in that file none
///     covers this route — so the kernel's capacity `HarnessError`
///     (`engine/harness.ts:8052-8056`, the same sentence [`thread_cap_submit_error`] quotes) escapes
///     the dispatcher as a plain non-`HttpError` throw;
///   * the listener then answers it as an uncaught error: `server/handler.ts:319` is
///     `error instanceof HttpError ? error.status : 500`, and `:320` sends
///     `sendJson(response, status, { error: { message: messageOf(error) } })`, i.e.
///     `application/json; charset=utf-8` and the message VERBATIM (`start.ts:176-189` `sendFailure`
///     is the same envelope for the same throw on the programmatic listener);
///   * the swagger agrees: `POST /sessions/{id}/threads` declares 201/400/404/413/**500** and has no
///     409 and no 429 arm (`harness-server.swagger.yaml:2544-2638`).
///
/// WHY THE SUBMIT GETS 429 AND THIS ROUTE GETS 500: only `server/sessions.ts:10318-10338`
/// (`createSubmitThread` → `threadError`) classifies the refusal, and it runs on the submit path alone.
/// Two routes, two codes, one sentence — so 500 here is the reference's, and "fixing" it to 429 would
/// be a change the reference does not make. The remedy text is what makes the 500 actionable anyway:
/// it names the count, says which conversation to delete, and states the bound.
///
/// `count` is THIS build's roster size, i.e. the figure the gate compared against; see
/// [`thread_cap_submit_error`] for why the two builds' counters agree today and can only ever diverge
/// EARLY once default conversations are materialized.
#[must_use]
pub fn thread_cap_roster_error(count: usize) -> ApiError {
    ApiError::internal(format!(
        "This session already has {count} independent threads; delete a terminal one before creating \
         another (maximum {MAX_THREADS})."
    ))
}

/// The roster is full, as an UNFOCUSED SUBMIT answers it: **429**, with the runtime's own remedy sentence
/// wrapped in the store's context.
///
/// The reference's chain is exact (`packages/underload/harness` @ `e66bce87777cb5f53504dbc47ad38ebd2c0ca6ff`):
///   * the kernel refuses at 256 ACTIVE conversations with
///     `` `This session already has ${n} independent threads; delete a terminal one before creating
///        another (maximum ${MAX_THREAD_RECORDS}).` `` (`engine/harness.ts:8052-8056`,
///     `engine/core/thread.ts:52` `MAX_THREAD_RECORDS = 256`);
///   * the session layer wraps that refusal as `Could not open a new thread: <runtime sentence>` and maps
///     it to **429** by regex (`server/sessions.ts:10318-10323` `createSubmitThread` → `:10326-10338`
///     `threadError`, whose `/already has \d+ independent threads|delete a terminal one/i` arm is a
///     `SessionError(429, ...)`). Any OTHER runtime refusal about a thread is a 400, so this arm is about
///     capacity alone;
///   * the dispatcher answers a `SessionError` with its own status (`server/api.ts:6266-6277`), so 429 is
///     the code on the wire, and the swagger's `POST /sessions/{id}/tasks` declares 429
///     (`harness-server.swagger.yaml:1488-1573`) alongside the pending-task cap's 429.
///
/// WHY NOT THE 500 ABOVE: capacity is retryable once somebody deletes a terminal conversation, which
/// is what 429 says and what 500 does not. The reference calls this out at `sessions.ts:10315-10317`
/// ("the runtime's host-thread bound is a lifecycle failure, not a server bug. It becomes an
/// actionable 429"). The roster route never runs that classification, so it keeps the bare 500; the
/// two codes are the reference's own split, not a coincidence.
///
/// `count` is THIS build's roster size, i.e. the figure its own cap gate compared against. WHAT THE
/// TWO COUNTERS ACTUALLY COUNT — restated, because the first reading of this delta was wrong:
///   * retired (inactive) rows are NOT a difference. The reference filters them out of its own count
///     (`engine/harness.ts:8049`, `state.record.inactive !== true`) and this build never STORES one —
///     [`SessionState::detach_thread`] removes the row instead of marking it — so neither side counts
///     what the other has already dropped, and the delta from this source is exactly zero;
///   * the one real difference is `origin: 'agent'` rows: the reference keeps a default conversation
///     per agent in the same map (a plugin-added agent gets one at `engine/harness.ts:2304-2316`) but
///     leaves it out of the bound, whose filter admits only `origin === 'host'`
///     (`engine/harness.ts:8047-8050`), while this build's gate counts `state.threads.len()` over every
///     row it holds;
///   * those rows are UNREACHABLE today. The only producer is [`SessionState::resolve_thread`], which
///     has no callers, and `POST {root}/sessions/{id}/threads` forces `origin: "host"` itself
///     (`api/threads.rs:134-137`) behind a field gate that admits no other spelling
///     (`api/threads.rs:44`). The observable difference right now is therefore **0**, on every route.
///
/// So this is a FORWARD-LOOKING gap, not present-tense behaviour: the day default conversations are
/// materialized, a session that reaches the bound through them refuses EARLY by at most A rows (A = the
/// agent's own default conversations), and it can never refuse LATE. Until then the sentence and the
/// number agree across the two builds.
#[must_use]
pub fn thread_cap_submit_error(count: usize) -> ApiError {
    ApiError::too_many_requests(format!(
        "Could not open a new thread: This session already has {count} independent threads; delete a \
         terminal one before creating another (maximum {MAX_THREADS})."
    ))
}

/// The lifecycle a conversation reports while it holds a live run (`protocol.ts:419`).
#[must_use]
pub fn lifecycle_status(lifecycle: ThreadLifecycle) -> &'static str {
    store::lifecycle_str(lifecycle)
}

/// The origin spelling a roster row carries (`protocol.ts:418`).
#[must_use]
pub fn origin_status(origin: ThreadOrigin) -> &'static str {
    store::origin_str(origin)
}

impl SessionState {
    /// The next conversation ordinal, without taking it (`sessions.ts:5846`).
    #[must_use]
    pub fn peek_thread_id(&self) -> String {
        ids::host_thread_id(self.thread_seq + 1)
    }

    /// Mints the conversation's durable row (`sessions.ts:5850 createThread`).
    ///
    /// `agent` must already be a roster member: a conversation runs AS an agent the harness knows, and
    /// the label allocator refuses an agent it has never seen. The transcript starts empty, and the
    /// transcript revision starts at `None` so a reader can tell "never written" from "written empty".
    pub fn create_thread(
        &mut self,
        agent: &str,
        origin: ThreadOrigin,
        owner_thread_id: Option<&str>,
        label: Option<&str>,
    ) -> HarnessThreadRecord {
        self.thread_seq += 1;

        let sequence = self.thread_seq;
        let id = ids::host_thread_id(sequence);
        let label = label
            .map(str::to_owned)
            .or_else(|| self.thread_labels.get(&id).cloned())
            .unwrap_or_else(|| self.labels.next(agent));

        // A caller-supplied label still has to reserve its ordinal, or a later mint could hand the same
        // name to a different conversation (`labels.rs:166 reserve`).
        self.labels.reserve(&label);
        self.thread_labels.insert(id.clone(), label.clone());

        let record = HarnessThreadRecord {
            id: id.clone(),
            agent: agent.to_owned(),
            origin: origin_status(origin).to_owned(),
            status: lifecycle_status(ThreadLifecycle::Idle).to_owned(),
            sequence,
            owner_thread_id: owner_thread_id.map(str::to_owned),
            label: Some(label),
            messages: Vec::new(),
            transcript_revision: None,
            ..Default::default()
        };

        self.threads.insert(id.clone(), record.clone());
        self.thread_order.push(id);
        self.lanes
            .entry(record.id.clone())
            .or_insert_with(|| store::ThreadLane {
                thread_id: record.id.clone(),
                active_turn: None,
                queue: Vec::new(),
                // A fresh lane is idle until `spawn_pump` takes the flag.
                pumping: false,
                stop_requested: false,
            });

        record
    }

    /// The conversation a task lands in (`sessions.ts:5890 ensureThreadFor`): the one the client named, or
    /// this session's default conversation for `agent`, created on first use.
    ///
    /// `Err(unknown_thread)` only for a name the client INVENTED; the default conversation is named
    /// `agent:<name>`, which is why a session that never created one still accepts work.
    pub fn resolve_thread(
        &mut self,
        requested: Option<&str>,
        agent: &str,
    ) -> Result<String, ApiError> {
        if let Some(requested) = requested {
            if self.threads.contains_key(requested) {
                return Ok(requested.to_owned());
            }

            return Err(unknown_thread(requested));
        }

        // The default conversation is the agent's own; TS names it after the agent so a host can point a
        // follow-up task at it without knowing the minted id (`sessions.ts:5899 defaultThreadId`).
        let default_id = format!("agent:{agent}");

        if self.threads.contains_key(&default_id) {
            return Ok(default_id);
        }

        if self.threads.len() >= MAX_THREADS {
            // The submit-side refusal: this is the conversation a TASK is trying to land in, so the
            // code is the submit route's 429 with the wrapped remedy, not the roster route's 500.
            // Unreachable today (`resolve_thread` has no callers); see [`thread_cap_submit_error`].
            return Err(thread_cap_submit_error(self.threads.len()));
        }

        let record = self.create_thread(agent, ThreadOrigin::Agent, None, Some(&default_id));

        Ok(record.id)
    }

    /// The conversation's transcript, verbatim (`api-reads.ts:490 messages`).
    #[must_use]
    pub fn transcript_of(&self, thread_id: &str) -> Option<&[HarnessMessage]> {
        self.threads
            .get(thread_id)
            .map(|thread| thread.messages.as_slice())
    }

    /// Marks a conversation running or idle (`sessions.ts:6004 updateThreadStatus`).
    pub fn set_thread_status(&mut self, thread_id: &str, lifecycle: ThreadLifecycle) {
        if let Some(thread) = self.threads.get_mut(thread_id) {
            thread.status = lifecycle_status(lifecycle).to_owned();
        }
    }

    /// Drops a conversation and everything addressed at it (`sessions.ts:8345 deleteThread`), returning the
    /// turn ids that were waiting on it so the caller can drop their rows too.
    pub fn detach_thread(&mut self, thread_id: &str) -> Vec<String> {
        let mut dropped = Vec::new();

        self.threads.remove(thread_id);
        self.thread_order.retain(|existing| existing != thread_id);
        self.thread_labels.remove(thread_id);
        self.invalid_threads.remove(thread_id);

        if let Some(lane) = self.lanes.remove(thread_id) {
            dropped = lane.queue;
        }

        // The legacy queue carries the same backlog, addressed by the turn's own thread id.
        self.queue.retain(|turn_id| {
            let keep = self
                .turns
                .get(turn_id)
                .and_then(|turn| turn.record.thread_id.as_deref())
                .is_none_or(|owner| owner != thread_id);

            if !keep {
                dropped.push(turn_id.clone());
            }

            keep
        });

        self.steer_mailbox.remove(thread_id);
        self.steer_tx.remove(thread_id);
        dropped.sort();
        dropped.dedup();

        if self.active_thread.as_deref() == Some(thread_id) {
            // The default conversation is the next one in creation order, which is what TS's
            // `activeThreadId` falls back to after a delete (`sessions.ts:8372`).
            self.active_thread = self.thread_ids().into_iter().next();
        }

        dropped
    }
}

impl SessionStore {
    /// `POST /sessions/{id}/threads` (`api.ts:2482-2520`): bind a new conversation.
    pub async fn create_thread(&self, id: &str, body: &Value) -> HarnessResult<(Value, u64)> {
        let agent = body
            .get("agent")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|agent| !agent.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                let inner = self.lock();

                inner
                    .session(id)
                    .map(SessionState::primary_agent)
                    .unwrap_or_default()
            });
        let label = body
            .get("label")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(str::to_owned);

        let mut inner = self.lock();
        let state = inner
            .session_mut(id)
            .ok_or_else(|| store::session_not_found(id))?;

        if !state.roster().contains(&agent) {
            return Err(ApiError::not_found(format!(
                "Agent \"{agent}\" is not a member of session \"{id}\"."
            )));
        }

        if state.threads.len() >= MAX_THREADS {
            // 500, not 409 and not 429: the reference wraps NOTHING around the kernel's refusal on
            // this route, so the throw reaches the listener's uncaught handler
            // (`handler.ts:319-320`), which is where 500 + `{error:{message}}` comes from. The
            // sentence is the runtime's own, quoted with the count this gate compared against.
            return Err(thread_cap_roster_error(state.threads.len()));
        }

        let at = crate::events::now_millis();
        let before = state.durable_record();
        let host = body
            .get("origin")
            .and_then(Value::as_str)
            .is_some_and(|origin| origin == "host");
        // The contract's REST spelling is snake_case `owner_thread_id` (`contract::CreateSessionThreadRequest`);
        // the camelCase half is accepted too, because the TS route reads a camelCase body and an old
        // client must keep working.
        let owner_thread_id = body
            .get("owner_thread_id")
            .or_else(|| body.get("ownerThreadId"))
            .and_then(Value::as_str);
        let record = state.create_thread(
            &agent,
            if host {
                ThreadOrigin::Host
            } else {
                ThreadOrigin::Agent
            },
            owner_thread_id,
            label.as_deref(),
        );
        let thread_id = record.id.clone();

        state.touch_idle(at, self.config.idle_ms);

        let row = read::thread_row_of(state, &thread_id);
        let payload = json!({
            "thread": row,
            "total": u64::try_from(state.threads.len()).unwrap_or(u64::MAX),
        });

        let mut outcome = store::CommitOutcome::default();
        outcome
            .bumps
            .push(("threads".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("session".to_owned(), Some(id.to_owned())));
        outcome = outcome.with_event(read_thread_emit("thread.upsert", id, &thread_id, &payload));

        let revision = self
            .commit(&inner, id, &mut outcome)
            .inspect_err(|_error| {
                self.rollback_state(&mut inner, id, &before);
            })?;

        Ok((payload, revision))
    }

    /// `GET /sessions/{id}/threads` (`api.ts:2440-2462`): the roster, paged, in creation order.
    pub async fn list_threads(&self, id: &str, query: &Query) -> Result<Value, ApiError> {
        let epoch = self.hub().epoch().to_string();
        let page = paging_query(query, &epoch)?;
        let inner = self.lock();
        let state = inner
            .session(id)
            .ok_or_else(|| store::session_not_found(id))?;

        let rows: Vec<contract::ThreadSummary> = state
            .thread_ids()
            .into_iter()
            .filter_map(|thread_id| read::thread_row_of(state, &thread_id))
            .collect();
        let total = rows.len();
        let slice = paging::slice_page(rows, page.cursor, page.limit, Some(total));

        Ok(paging::page_slice_body(
            &epoch,
            slice,
            self.hub().revision("threads", Some(&state.record.id)),
            crate::events::now_millis(),
        ))
    }

    /// `GET /sessions/{id}/threads/{threadId}` (`api.ts:2464-2480`).
    pub async fn get_thread(&self, id: &str, thread_id: &str) -> Result<Value, ApiError> {
        let inner = self.lock();
        let state = inner
            .session(id)
            .ok_or_else(|| store::session_not_found(id))?;

        if state.thread(thread_id).is_none() {
            return Err(unknown_thread(thread_id));
        }

        // The document size is part of the resource (`api-reads.ts:566 transcript_bytes`), and an
        // unreadable conversation states none (`thread_messages` is the live transcript only).
        let transcript_bytes = state
            .thread_messages(thread_id)
            .map(|messages| read::document_bytes(&messages));

        Ok(json!({
            "thread": read::thread_resource_of(state, thread_id, self.hub(), transcript_bytes),
        }))
    }

    /// `DELETE /sessions/{id}/threads/{threadId}` (`api.ts:2522-2560`).
    ///
    /// A conversation with work in flight refuses, because the run would outlive the roster row its events
    /// address; its BACKLOG is dropped by the delete, which is what "cancelled" means here.
    pub async fn delete_thread(&self, id: &str, thread_id: &str) -> Result<Value, ApiError> {
        let mut inner = self.lock();
        let state = inner
            .session_mut(id)
            .ok_or_else(|| store::session_not_found(id))?;

        if state.thread(thread_id).is_none() {
            return Err(unknown_thread(thread_id));
        }

        if state.active_run_of(thread_id).is_some() {
            return Err(ApiError::conflict(format!(
                "Thread \"{thread_id}\" still has live work; wait for it to stop before deleting it."
            )));
        }

        // A conversation the store is still pumping has a lane with an active turn even when the run
        // itself has already been handed to the executor.
        if state
            .lanes
            .get(thread_id)
            .and_then(|lane| lane.active_turn.as_ref())
            .is_some()
        {
            return Err(session_busy_error(id, "delete this conversation"));
        }

        let at = crate::events::now_millis();
        let before = state.durable_record();

        // A conversation's DESCENDANTS go with it: their owner chain reaches this row, and a child
        // whose parent is gone is unreachable plumbing (`api.ts:2522-2560`, and the contract's
        // `removed_descendants` figure at `harness-api.ts:1447` exists for exactly this cascade).
        let removed: Vec<String> = {
            let mut names = vec![thread_id.to_owned()];
            names.extend(descendant_threads(state, thread_id));

            names
        };
        let mut dropped: Vec<String> = Vec::new();
        for name in &removed {
            dropped.extend(state.detach_thread(name));
        }
        dropped.sort();
        dropped.dedup();

        let dropped_queued = u64::try_from(dropped.len()).unwrap_or(u64::MAX);

        for turn_id in &dropped {
            if let Some(turn) = state.turn_mut(turn_id) {
                turn.record.status =
                    store::turn_status_str(contract::SessionTurnStatus::Cancelled).to_owned();
            }
        }

        state.touch_idle(at, self.config.idle_ms);

        let payload = json!({
            "threadId": thread_id,
            "droppedTurns": dropped,
        });

        let mut outcome = store::CommitOutcome::default();
        outcome
            .bumps
            .push(("threads".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("turns".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("queue".to_owned(), Some(id.to_owned())));
        outcome
            .bumps
            .push(("session".to_owned(), Some(id.to_owned())));
        outcome = outcome.with_event(read_thread_emit("thread.removed", id, thread_id, &payload));

        let revision = self
            .commit(&inner, id, &mut outcome)
            .inspect_err(|_error| {
                self.rollback_state(&mut inner, id, &before);
            })?;

        Ok(json!({
            "deleted": true,
            "threadId": thread_id,
            "sessionId": id,
            "droppedQueued": dropped_queued,
            "revision": revision,
        }))
    }
}

/// One hub event addressed at a conversation (`api-events.ts:320`'s target set).
#[must_use]
fn read_thread_emit(
    kind: &str,
    session_id: &str,
    thread_id: &str,
    payload: &Value,
) -> store::HubEmit {
    store::HubEmit {
        kind: kind.to_owned(),
        targets: vec![
            ("threads".to_owned(), Some(session_id.to_owned())),
            (
                "thread".to_owned(),
                Some(store::thread_key(session_id, thread_id)),
            ),
        ],
        thread_id: Some(thread_id.to_owned()),
        turn_id: None,
        payload: payload.clone(),
        ops: Vec::new(),
    }
}

/// Every conversation whose OWNER chain reaches `root` (`sessions.ts:7590 isDescendantThread`).
///
/// The walk is transitive on purpose: `harness.stopThread` cascades to a delegated child, so a stop
/// that named only the parent would abort the child's run while its lane kept claiming work.
#[must_use]
fn descendant_threads(state: &SessionState, root: &str) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = vec![root.to_owned()];

    while let Some(parent) = frontier.pop() {
        for thread_id in state.thread_ids() {
            if found.iter().any(|seen| seen == &thread_id) || thread_id == root {
                continue;
            }

            let owned = state
                .thread(&thread_id)
                .is_some_and(|thread| thread.owner_thread_id.as_deref() == Some(parent.as_str()));

            if owned {
                found.push(thread_id.clone());
                frontier.push(thread_id);
            }
        }
    }

    found
}

impl SessionStore {
    /// `POST /sessions/{id}/threads/{threadId}/cancel` (`api.ts:3487-3517` +
    /// `sessions.ts:7618-7730 cancelThread`).
    ///
    /// The stop is a STATE, not a deletion: the conversation stays in the roster as `stopped`, its
    /// queued turns end as `cancelled` rows (the record is append-only until a reset, so a reload still
    /// shows what was stopped), and only THIS conversation's owner questions expire — a sibling lane may
    /// still be waiting for its owner.
    ///
    /// `cancelled` reports whether live work was aborted or a queued turn dropped; an idle conversation
    /// is still marked stopped and the call is still a success (`sessions.ts:7612-7613`).
    pub async fn cancel_thread(&self, id: &str, thread_id: &str) -> Result<Value, ApiError> {
        let at = crate::events::now_millis();
        let mut inner = self.lock();
        let state = inner
            .session_mut(id)
            .ok_or_else(|| store::session_not_found(id))?;

        if state.thread(thread_id).is_none() {
            return Err(unknown_thread(thread_id));
        }

        // The thread plus everything its stop cascades to (`sessions.ts:7636-7639`).
        let stopped: Vec<String> = {
            let mut names = vec![thread_id.to_owned()];
            names.extend(descendant_threads(state, thread_id));

            names
        };

        let mut cancelled = false;
        let mut dropped: Vec<String> = Vec::new();

        // Signal OUR classification BEFORE aborting (`sessions.ts:7641-7649`): the executor rejects the
        // run with its own cancel error, and `cancel_requested` is what records the turn as `cancelled`
        // rather than `error`. An idle lane needs no flag.
        for target in &stopped {
            if let Some(lane) = state
                .lanes
                .get_mut(target)
                .filter(|lane| lane.active_turn.is_some())
            {
                lane.stop_requested = true;
            }
        }

        for run in &mut state.active {
            if stopped.iter().any(|target| target == &run.thread_id) {
                run.cancel_requested = true;
                run.cancel.cancel();
                cancelled = true;
            }
        }

        // The backlogs: every cascaded lane, then the legacy queue entries addressed at one of them
        // (`sessions.ts:7674-7707`).
        for target in &stopped {
            if let Some(lane) = state.lanes.get_mut(target) {
                dropped.extend(std::mem::take(&mut lane.queue));
            }
        }

        let legacy: Vec<String> = state.queue.clone();

        for turn_id in legacy {
            let addressed_here = state
                .turns
                .get(&turn_id)
                .and_then(|turn| turn.record.thread_id.clone())
                .is_some_and(|owner| stopped.iter().any(|target| target == &owner));

            if addressed_here {
                state.queue.retain(|existing| existing != &turn_id);
                dropped.push(turn_id);
            }
        }

        for turn_id in &dropped {
            if let Some(turn) = state.turns.get_mut(turn_id) {
                turn.record.status =
                    store::turn_status_str(contract::SessionTurnStatus::Cancelled).to_owned();
                turn.record.updated_at = at;
                turn.projector.settle("cancelled", at);
                cancelled = true;
            }
        }

        // A run whose conversation is stopped but that has not settled yet keeps the session `running`
        // until its abort lands; the terminal settle frame then fixes the aggregate
        // (`sessions.ts:7715-7717`).
        // The questions this stop settled, in settlement order; the announce loop below turns each one
        // into an `action.closed`.
        let mut closed_actions: Vec<String> = Vec::new();

        for target in &stopped {
            state.set_thread_status(target, ThreadLifecycle::Stopped);

            // T16: the stop settled that conversation's unanswered questions, so each one announces
            // itself (`sessions.ts:2325-2335`); a sibling lane's questions stay open because the expiry
            // is scoped to THIS thread.
            closed_actions.extend(state.expire_actions(
                crate::store::session::ACTION_CANCELLED_MESSAGE,
                Some(target),
            ));
        }

        state.record.updated_at = at;
        state.touch_idle(at, self.config.idle_ms);

        let dropped_queued_tasks = u64::try_from(dropped.len()).unwrap_or(u64::MAX);

        // `api.ts:3502-3505`: the store signals `thread.upsert` with `status: stopped`, which moves the
        // conversation and roster counters; the session counter is the REST-owned figure the handler adds.
        let mut outcome = store::CommitOutcome::default();

        for (resource, key) in [
            ("thread", Some(store::thread_key(id, thread_id))),
            ("threads", Some(id.to_owned())),
            ("turns", Some(id.to_owned())),
            ("queue", Some(id.to_owned())),
        ] {
            outcome.bumps.push((resource.to_owned(), key));
        }

        outcome = outcome.with_event(read_thread_emit(
            "thread.upsert",
            id,
            thread_id,
            &json!({ "thread_id": thread_id, "status": "stopped" }),
        ));

        // T16: one `action.closed` per question this stop settled, each moving the `actions` counter
        // once (`api.ts:1126-1138`).
        for request_id in &closed_actions {
            outcome = outcome.event(
                "action.closed",
                &[("actions", Some(id))],
                Some(thread_id),
                None,
                json!({
                    "action_closed": {
                        "request_id": request_id,
                        "reason": store::actions::action_closed_reason(
                            crate::store::session::ACTION_CANCELLED_MESSAGE,
                        ),
                    }
                }),
            );
        }

        // The abort ALREADY happened, exactly as in TS, where `harness.stopThread` runs before
        // `persistOrThrow` (`sessions.ts:7652/7722`): a failed write means the stop is not durable, and
        // the run's own settle path reports the terminal status. Nothing is un-cancelled — a
        // `CancellationToken` has no undo, and pretending otherwise would lie about the live run.
        self.commit(&inner, id, &mut outcome)?;

        Ok(serde_json::to_value(contract::CancelThreadResult {
            cancelled,
            thread_id: thread_id.to_owned(),
            session_id: id.to_owned(),
            status: ThreadLifecycle::Stopped,
            dropped_queued_tasks,
            revision: outcome.revision,
        })
        .unwrap_or(Value::Null))
    }
}

/// The stream layer's existence checks (`ports::StreamExists`, `stream/mount.ts`).
///
/// A subscription is validated when it opens AND while it lives, and the stream may never materialize a
/// session to do it — these three reads walk the already-live maps and take the guard for one lookup
/// each. An EXPIRED or deleted session answers `false`, which is what makes the stream drop the
/// subscription instead of spinning on it (`stream/mount.ts:410-430`).
impl crate::ports::StreamExists for SessionStore {
    fn has_session(&self, session_id: &str) -> bool {
        self.lock().session(session_id).is_some()
    }

    fn has_thread(&self, session_id: &str, thread_id: &str) -> bool {
        self.lock()
            .session(session_id)
            .is_some_and(|state| state.thread(thread_id).is_some())
    }

    fn has_turn(&self, session_id: &str, turn_id: &str) -> bool {
        self.lock()
            .session(session_id)
            .is_some_and(|state| state.turn(turn_id).is_some())
    }
}
