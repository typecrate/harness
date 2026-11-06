//! Session lifecycle: create / list / read / patch / delete, the submit surface, reset, and cancel.
//!
//! TypeScript origins, named per function below:
//!   * `create` - `sessions.ts:1826-1945` + the 201 recipe of `api.ts:1963-2018`;
//!   * `list` / `get` / `patch` / `delete` - `api.ts:1934-2111`;
//!   * `dispatch` / `enqueue` - `sessions.ts:6047-6101` and `:5828-6017`;
//!   * `executeCommandTurn` - `sessions.ts:6111-6337`;
//!   * `reset` / `remove` / `cancel` - `sessions.ts:8370-8460`, `:8468-8498`, `:7488-7588`.
//!
//! THE SUBMIT ORDER IS THE CONTRACT (`sessions.ts:6054-6062`, restated at `:6084-6093`)
//! ------------------------------------------------------------------------------------
//! A leading `/` decides command-vs-prompt BEFORE anything else is validated, and attachment
//! validation runs BEFORE the empty/length check, because "an over-long submit that also carries an
//! attachment must answer the attachment problem it answers on the queue path, not a length 413".
//! Every rule below is in that order for that reason; a reordering is a behaviour change.

use serde_json::{Value, json};

use crate::contract::{
    self, CreateSessionRequest, MAX_INPUT_CHARS, SubmitTaskRequest, TaskReceipt, TaskReceiptLinks,
};
use crate::error::ApiError;
use crate::events::now_millis;
use crate::ids;
use crate::paging;
use crate::ports::Query;
use crate::store::{
    self, CommitOutcome, SessionState, SessionStore, read::QUEUE_PREVIEW_CHARS,
    read::prompt_preview, read::queue_rows_of, tool_cwd,
};

/// The per-turn pending-task budget (`sessions.ts:294-297` `MAX_PENDING_TASKS`), spanning every
/// lane so spreading work over threads cannot bypass the backlog bound.
pub const MAX_PENDING_TASKS: usize = crate::contract::MAX_PENDING_TASKS;

/// The session title bound (`sessions.ts:1775`: `trimmed.slice(0, 200)`).
pub const MAX_SESSION_TITLE_CHARS: usize = 200;

/// `A task cannot be empty.` (`sessions.ts:1282`).
pub const EMPTY_TASK_MESSAGE: &str = "A task cannot be empty.";

/// `A slash command cannot carry attachments.` / `... references.` (`sessions.ts:1305-1306`).
pub const COMMAND_ATTACHMENT_MESSAGE: &str = "A slash command cannot carry attachments.";
pub const COMMAND_REFERENCE_MESSAGE: &str = "A slash command cannot carry result references.";

impl SessionStore {
    // -----------------------------------------------------------------------
    // Create
    // -----------------------------------------------------------------------

    /// Creates one session (`sessions.ts:1826-1945`).
    ///
    /// The harness is resolved ALIAS-AWARE (`registry.find`, the TS comment at `:1827-1831`: an
    /// older client keeps working), the definition is FROZEN at the current revision, and the
    /// endpoint trio follows the one credential rule (`config::merge_settings`): a blank client key
    /// means "server key" only for the configured provider's origin, so a foreign base URL creates
    /// an unauthenticated session instead of leaking the server credential.
    ///
    /// Returns the `201` body `{ "session": SessionResource }`.
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] for an empty harness name, or a harness that needs an endpoint the
    /// request and the deployment do not provide; [`ApiError::not_found`] `Unknown harness "<x>".`.
    pub async fn create_session(&self, request: &CreateSessionRequest) -> Result<Value, ApiError> {
        let harness = request.harness.trim();

        if harness.is_empty() {
            return Err(ApiError::bad_request("A session requires a harness name."));
        }

        let now = now_millis();
        let session_id = ids::new_session_id();

        // ONE lock for the whole decision: the harness resolves from the CURRENT registry, the
        // defaults are the CURRENT global settings, and the record lands in the same guarded region
        // (`sessions.ts:1827` + `api.ts:1974-1989` both read live store state, never a boot copy). A
        // `POST /harnesses` that committed before this call is therefore visible here without a
        // restart, and a `PATCH /config` is visible to the very next create.
        let mut inner = self.lock();

        let definition = inner
            .registry()
            .find(harness)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("Unknown harness \"{harness}\".")))?;

        let defaults = inner.settings();
        let input = request.settings.clone().unwrap_or_default();
        let settings = crate::config::merge_settings(&defaults, &input)?;

        if definition.requires_endpoint && settings.base_url.trim().is_empty() {
            return Err(ApiError::bad_request(format!(
                "Harness \"{harness}\" needs a provider base URL."
            )));
        }

        // The canonical name the snapshot reports, which an alias resolves away
        // (`sessions.ts:1832` + `:1882`).
        let name = definition.name.clone();

        let record = crate::persistence::HarnessSessionRecord {
            id: session_id.clone(),
            harness: name.clone(),
            preset: definition.to_record(),
            definition: store::descriptor_record_of(&definition),
            settings: crate::persistence::HarnessSessionSettingsRecord {
                base_url: settings.base_url.clone(),
                model: settings.model.clone(),
                extra: serde_json::Map::new(),
            },
            title: normalize_title(request.title.as_deref()),
            status: "idle".to_string(),
            created_at: now,
            updated_at: now,
            last_error: None,
            turns: Vec::new(),
            queue: Vec::new(),
            thread_sequence: None,
            label_seeds: None,
            label_high_water: None,
            thread_labels: None,
            thread_label_high_water: None,
            turn_invocations: None,
            turn_instruction_sources: None,
            invocation_threads: None,
            invalid_threads: None,
            core: initial_core_value(),
            agent_models: None,
            session_agents: None,
            extra: serde_json::Map::new(),
        };

        // The new session's live maps come from the SAME decoder a restart uses: a session that boots
        // empty and one that boots from a document cannot hold different state, so the 201 and the first
        // read after a restart cannot disagree.
        //
        // The credential the merged pair resolved to is then attached to the LIVE state only — the record
        // never sees it (`mod.rs` "THE LOCK", rule 5: the provider credential never enters the document,
        // the rule `sessions.ts:1618-1634` enforces with a side map).
        let mut state = self.restore_session(record)?;
        state.provider_key = (!settings.api_key.is_empty()).then(|| settings.api_key.clone());

        // Order is the recipe: insert, COMMIT, then move the counters and publish, so the 201 can
        // never report a session the next restart has forgotten (`sessions.ts:1934-1942`).
        inner.insert(state);

        if let Err(error) = self.save(&inner) {
            inner.remove(&session_id);

            return Err(error);
        }

        let payload = self.built_session_payload(&inner, &session_id);

        Ok(json!({ "session": payload }))
    }

    // -----------------------------------------------------------------------
    // List / read / patch / delete
    // -----------------------------------------------------------------------

    /// Every session, filtered and paged (`api.ts:1934-1960`).
    ///
    /// The order is the store's own insertion order - file order after a restart, creation order
    /// otherwise - because `store.liveSessions()` is a `Map` walk (`sessions.ts:1651-1653`) and TS
    /// never sorts this page. Sorting by date would reorder a restored rail.
    pub async fn list_sessions(&self, query: &Query) -> Result<Value, ApiError> {
        let epoch = self.hub.epoch().to_string();
        let page = paging_query(query, &epoch)?;
        let status = enum_param(query, "status", &["idle", "running", "error"])?;
        let harness = param(query, "harness").map(str::to_string);

        let inner = self.lock();

        let ordered: Vec<&SessionState> = inner
            .session_order
            .iter()
            .filter_map(|id| inner.sessions.get(id))
            .filter(|state| {
                status.is_none_or(|wanted| state.record.status == wanted)
                    && harness
                        .as_deref()
                        .is_none_or(|wanted| state.record.preset.name == wanted)
            })
            .collect();

        let total = ordered.len();

        let rows: Vec<contract::SessionSummary> = ordered
            .into_iter()
            .map(|state| store::read::session_summary_of(state, &self.hub))
            .collect();

        let slice = paging::slice_page(rows, page.cursor, page.limit, Some(total));

        Ok(paging::page_slice_body(
            &epoch,
            slice,
            self.hub.revision("session", None),
            now_millis(),
        ))
    }

    /// One session in full (`api.ts:2020-2024`), the `200` body `{ "session": ... }`.
    pub async fn get_session(&self, id: &str) -> Result<Value, ApiError> {
        let inner = self.lock();
        let state = require_session(&inner, id)?;
        let payload = self.session_resource(&inner, state);

        Ok(json!({ "session": payload }))
    }

    /// The identified read of the frozen definition (`api.ts:2113-2127`).
    ///
    /// The live registry and the live catalogue are read from the SAME guard that holds the session,
    /// because they are part of the store's host view (`StoreInner::registry` /
    /// [`StoreInner::agents`]); `session_definition_of` only ever reads a name out of the frozen copy
    /// to compare it with the live one.
    pub async fn session_definition(&self, id: &str) -> Result<Value, ApiError> {
        let inner = self.lock();
        let state = require_session(&inner, id)?;
        // The registry and the catalogue come from the guard, not from a `Store` accessor: those
        // accessors take the lock again, and the lock is not reentrant.
        let definition =
            store::read::session_definition_of(state, &self.hub, inner.registry(), inner.agents());

        Ok(json!({ "definition": definition }))
    }

    /// The usage fold (`api.ts:2129-2135`).
    pub async fn session_usage(&self, id: &str) -> Result<Value, ApiError> {
        let inner = self.lock();
        let state = require_session(&inner, id)?;

        Ok(json!(store::read::usage_fold_of(state, &self.hub)))
    }

    /// The command inventory of this session's live registry (`api.ts:2140-2154`).
    pub async fn session_commands(&self, id: &str, query: &Query) -> Result<Value, ApiError> {
        let epoch = self.hub.epoch().to_string();
        let page = paging_query(query, &epoch)?;
        let inner = self.lock();
        let state = require_session(&inner, id)?;

        let rows = store::read::command_rows_of();
        let total = rows.len();
        let slice = paging::slice_page(rows, page.cursor, page.limit, Some(total));

        Ok(paging::page_slice_body(
            &epoch,
            slice,
            self.hub.revision("commands", Some(&state.record.id)),
            now_millis(),
        ))
    }

    /// Every pending task, in pump order (`api.ts:2156-2170`).
    ///
    /// `position` is the 0-based slot of this ordering; the submit receipt reports the same row's
    /// 1-based ordinal (`api.ts:2195-2230`, and the comment there is the whole reason the two
    /// figures exist).
    pub async fn session_queue(&self, id: &str, query: &Query) -> Result<Value, ApiError> {
        let epoch = self.hub.epoch().to_string();
        let page = paging_query(query, &epoch)?;
        let inner = self.lock();
        let state = require_session(&inner, id)?;

        let rows = queue_rows_of(state);
        let total = rows.len();
        let slice = paging::slice_page(rows, page.cursor, page.limit, Some(total));

        Ok(paging::page_slice_body(
            &epoch,
            slice,
            self.hub.revision("queue", Some(&state.record.id)),
            now_millis(),
        ))
    }

    /// `PATCH /sessions/{id}` (`api.ts:2026-2080`).
    ///
    /// `agent_models` MERGES (an agent absent from the body keeps its model), `title` REPLACES and
    /// an explicit `null` clears it, and `{}` is a 400 no-op. Both halves move the session counter
    /// and the roster counter, because a model map is roster-visible state (`api.ts:2067-2077`).
    pub async fn update_session(&self, id: &str, body: &Value) -> Result<Value, ApiError> {
        let object = require_object(body)?;
        crate::contract::require_known_fields(body, &["agent_models", "title"])?;

        let models = object.get("agent_models");
        let title = object.get("title");

        if models.is_none() && title.is_none() {
            return Err(ApiError::bad_request(
                "A patch must change at least one field.",
            ));
        }

        // Both mutations are validated BEFORE anything moves, so a refused patch changes nothing.
        let requested_models = match models {
            None => Vec::new(),
            Some(Value::Null) => Vec::new(),
            Some(models) => parse_agent_models(models, &self.roster_of(id)?)?,
        };

        let requested_title = match title {
            None => None,
            Some(Value::Null) => Some(None),
            Some(Value::String(text)) => Some(normalize_title(Some(text))),
            Some(_) => {
                return Err(ApiError::bad_request("\"title\" must be a string or null."));
            }
        };

        let payload = {
            let mut inner = self.lock();
            let state = require_session_mut(&mut inner, id)?;

            let before = self.snapshot_state(state);

            if !requested_models.is_empty() {
                let mut models = state.record.agent_models.clone().unwrap_or_default();

                for (agent, model) in requested_models {
                    models.insert(agent, model);
                }

                state.record.agent_models = Some(models);
            }

            if let Some(title) = requested_title {
                state.record.title = title;
            }

            state.touch_idle(now_millis(), self.config.idle_ms);

            // `api.ts:2067-2077`: `session` and `session_agents` move together, because a model map is
            // roster-visible state.
            let patch = json!({
                "session": store::read::session_state_patch_of(state, &self.hub),
            });

            let mut outcome = store::CommitOutcome::default();
            let targets: Vec<(&str, Option<&str>)> =
                vec![("session", Some(id)), ("session_agents", Some(id))];

            for (resource, key) in targets.clone() {
                outcome
                    .bumps
                    .push((resource.to_owned(), key.map(str::to_owned)));
            }

            outcome =
                outcome.event_with_ops("session.updated", &targets, None, None, patch, Vec::new());

            // The borrow of `state` ends HERE: `commit` serializes the whole store, and the resource it
            // answers with is read back afterwards through the same guard.

            if let Err(error) = self.commit(&inner, id, &mut outcome) {
                self.rollback_state(&mut inner, id, &before);

                return Err(error);
            }

            self.session_resource(
                &inner,
                inner
                    .sessions
                    .get(id)
                    .expect("the session still exists after a successful patch"),
            )
        };

        Ok(json!({ "session": payload }))
    }

    /// Removes one session (`api.ts:2082-2111` + `sessions.ts:8468-8498`).
    ///
    /// The counts are read off the live record BEFORE the removal, because afterwards the record is
    /// gone and the receipt could only guess. The document is written while the session is already
    /// dropped from the map (so the persisted state matches the answer), and a failed write puts the
    /// session back.
    pub async fn delete_session(&self, id: &str) -> Result<Value, ApiError> {
        let (aborted_turns, dropped_queued, journal_dir) = {
            let inner = self.lock();
            let state = require_session(&inner, id)?;

            let aborted = u64::try_from(state.active.len()).unwrap_or(u64::MAX);
            let dropped = u64::try_from(state.pending_count()).unwrap_or(u64::MAX);
            // The journal lives in `StoreInner::journals`, not on the session: it owns a file handle and
            // a session state must stay restorable (`mod.rs`, "SHAPE OF THE STATE").
            let journal = inner
                .journals
                .get(id)
                .map(|journal| journal.directory().to_path_buf());

            (aborted, dropped, journal)
        };

        // Abort the live runs BEFORE the document moves, so an aborted continuation cannot publish
        // over the deletion (`sessions.ts:8462-8467`).
        {
            let mut inner = self.lock();

            if let Some(state) = inner.sessions.get_mut(id) {
                for run in &state.active {
                    run.cancel.cancel();
                }

                let closed = state.expire_actions(ACTION_SESSION_GONE_MESSAGE, None);

                // T16: the delete settled every question the session still had, so every question card
                // a client is holding gets its `action.closed` (`sessions.ts:2325-2335` publishes from
                // the settlement itself, not from a later read). The session id still names the frame:
                // the resource it addressed is gone, which is exactly what the frame says.
                self.publish_actions_closed(
                    id,
                    &closed,
                    store::actions::action_closed_reason(ACTION_SESSION_GONE_MESSAGE),
                );
            }
        }

        let removed = {
            let mut inner = self.lock();

            let Some(state) = inner.remove(id) else {
                return Err(ApiError::not_found(format!("Unknown session \"{id}\".")));
            };

            let position = inner
                .session_order
                .iter()
                .position(|existing| existing == id)
                .unwrap_or(inner.session_order.len());

            let outcome = self.save(&inner);

            if let Err(error) = outcome {
                // The document still holds the session, so the live map must hold it too — put it back in
                // the LIST position it was taken out of, not at the end (`api.ts:2098-2101`).
                inner.sessions.insert(id.to_string(), state);
                inner.session_order.insert(position, id.to_string());

                return Err(error);
            }

            // The handle goes with the row: a session re-created under the same id must open its OWN new
            // log, never append to the deleted one.
            inner.journals.remove(id);

            state
        };

        // The per-step log is part of this session's durable footprint: a deleted session must not
        // leave events behind that a restart could replay as ghost turns (`sessions.ts:8484-8495`).
        if let Some(directory) = journal_dir {
            let _ = std::fs::remove_dir_all(&directory);
        } else {
            let _ = std::fs::remove_dir_all(
                crate::persistence::sessions_dir(&self.config.root).join(id),
            );
        }

        drop(removed);

        // The rail's collection counter moves with an added/removed row (`api.ts:2098-2101`), and
        // the host rail hears it as a `session.updated` on the collection.
        let _ = self.hub.bump("session", None);

        let _ = self.hub.publish(
            "session.updated",
            &[crate::events::ResourceRef::host("session")],
            Some(id),
            None,
            None,
            json!({ "session_id": id, "deleted": true }),
        );

        let result = contract::DeleteSessionResult {
            deleted: true,
            session_id: id.to_string(),
            aborted_turns,
            dropped_queued_tasks: dropped_queued,
        };

        Ok(serde_json::to_value(result).unwrap_or(Value::Null))
    }

    // -----------------------------------------------------------------------
    // Reset / cancel
    // -----------------------------------------------------------------------

    /// Discards the whole conversation (`sessions.ts:8370-8459` + the receipt in
    /// `api.ts:2247-2288`).
    ///
    /// A live run OR accepted-but-unstarted work is a 409: those tasks were accepted against this
    /// harness, so the caller must stop them first (`cancel { all: true }`) rather than have them
    /// discarded silently.
    pub async fn reset_session(&self, id: &str) -> Result<Value, ApiError> {
        let (discarded_turns, discarded_threads, dropped_queued) = {
            let inner = self.lock();
            let state = require_session(&inner, id)?;

            if state.busy_or_pending() {
                return Err(ApiError::conflict(
                    "Cancel the running task before resetting the session.",
                ));
            }

            (
                u64::try_from(state.turns.len()).unwrap_or(u64::MAX),
                u64::try_from(state.host_thread_records().len()).unwrap_or(u64::MAX),
                u64::try_from(state.pending_count()).unwrap_or(u64::MAX),
            )
        };

        let revision = {
            let mut guard = self.lock();
            // One deref of the guard, so `sessions` and `journals` can be borrowed as the DISJOINT fields
            // they are (a second `inner.field` access through a `MutexGuard` re-borrows the whole guard).
            let inner = &mut *guard;

            let state = require_session_mut(inner, id)?;

            // The undo point is the FIRST thing taken: `durable_record` reconciles the live maps into the
            // record, so capturing it after the clears would roll a failed write back to the ALREADY
            // reset state (`mod.rs` rule 3).
            let before = self.snapshot_state(state);

            // A reset starts a NEW conversation: turns, threads, lanes, counters, labels, joins,
            // unreadable transcripts and the action history all go with the old one
            // (`sessions.ts:8402-8432`). Deleted turns keep their label tombstones; a reset does
            // not, because there is no survivor whose name could collide.
            state.turns.clear();
            state.turn_order.clear();
            state.threads.clear();
            state.thread_order.clear();
            state.lanes.clear();
            state.queue.clear();
            state.active.clear();
            state.steer_tx.clear();
            state.steer_mailbox.clear();
            state.steer_active = 0;
            state.turn_seq = 0;
            state.thread_seq = 0;
            state.labels.reset();
            state.invalid_threads.clear();
            // `pending_actions` holds BOTH halves of the history: a live question is pending and an
            // answered or expired one is the completed list `completed_actions(limit)` projects.
            state.pending_actions.clear();
            state.answered_actions = 0;
            state.record.turns.clear();
            state.record.queue.clear();
            state.record.thread_labels = None;
            state.record.thread_label_high_water = None;
            state.record.turn_invocations = None;
            state.record.invocation_threads = None;
            state.record.invalid_threads = None;
            state.record.label_seeds = None;
            state.record.turn_instruction_sources = None;
            state.record.thread_sequence = None;

            state.record.status = "idle".to_string();
            state.record.last_error = None;
            state.record.updated_at = now_millis();

            // The session borrow ends HERE: everything below reaches the guard through other fields
            // (`journals`, then the whole store for `commit`), and `require_session_mut` ties the
            // session borrow to the entire `StoreInner`.

            // The durable per-step log restarts with the conversation: without the truncation a
            // restart would replay the OLD turns' events onto the new record as ghost steps
            // (`sessions.ts:8433-8444`). The truncate is part of the SAME mutation, so the undo point
            // taken above covers it: a rolled-back reset never leaves an empty journal behind a full
            // record.
            if let Some(journal) = inner.journals.get_mut(id)
                && let Err(error) = journal.rewrite(&[])
            {
                // The document is what makes the reset real; a log that refuses to restart is
                // reported, exactly as a journal refusal is anywhere else in this store.
                inner.journal_problems.push(error.message);
            }

            let mut outcome = CommitOutcome::default();
            for resource in ["session", "turns", "threads", "queue"] {
                outcome
                    .bumps
                    .push((resource.to_owned(), Some(id.to_owned())));
            }

            outcome = outcome.event_with_ops(
                "turn.removed",
                &[
                    ("session", Some(id)),
                    ("turns", Some(id)),
                    ("threads", Some(id)),
                    ("queue", Some(id)),
                ],
                None,
                None,
                json!({
                    "session_id": id,
                    "discarded_turns": discarded_turns,
                }),
                Vec::new(),
            );

            if let Err(error) = self.commit(inner, id, &mut outcome) {
                self.rollback_state(inner, id, &before);

                return Err(error);
            }

            outcome.revision
        };

        let result = contract::ResetSessionResult {
            reset: true,
            session_id: id.to_string(),
            discarded_turns,
            discarded_threads,
            dropped_queued_tasks: dropped_queued,
            revision,
        };

        Ok(serde_json::to_value(result).unwrap_or(Value::Null))
    }

    /// Cancels work in a session (`sessions.ts:7488-7588` + `api.ts:2290-2349`).
    ///
    /// The default cancels the ACTIVE run only and leaves the queued backlog untouched. `all` also
    /// drops every queued turn - the records REMAIN as `cancelled` rows, because the server record is
    /// append-only until reset, so a reload still shows what was stopped - and marks every host
    /// conversation stopped. `cancelled: false` is a successful no-op.
    pub async fn cancel_session(&self, id: &str, all: bool) -> Result<Value, ApiError> {
        let (active_turn_id, dropped) = {
            let inner = self.lock();
            let state = require_session(&inner, id)?;

            // `api.ts:2334-2335`: the running turn first, and the LAST recorded turn when the session is
            // busy with a run whose row is not `running` any more. A cancel-all that only stops threads
            // therefore still names the turn it interrupted rather than answering `null`.
            let active = state
                .turns_in_index_order(false)
                .into_iter()
                .find(|turn| turn.record.status == "running")
                .map(|turn| turn.record.id.clone())
                .or_else(|| {
                    // `record.busy` is TS's "a run is executing" flag (`sessions.ts:3337`, `:7201`),
                    // and the fallback names the newest recorded turn.
                    (state.active_run_count() > 0)
                        .then(|| state.turn_order.last().cloned())
                        .flatten()
                });

            let dropped = if all {
                u64::try_from(state.pending_count()).unwrap_or(u64::MAX)
            } else {
                0
            };

            (active, dropped)
        };

        let now = now_millis();
        let mut cancelled = false;

        let revision = {
            let mut inner = self.lock();
            let state = require_session_mut(&mut inner, id)?;
            let before = self.snapshot_state(state);

            // The live runs first: mark OUR classification before the abort, because the executor
            // rejects the run with its own cancel error and this flag is what records the turn as
            // `cancelled` rather than `error` (`sessions.ts:7506-7518`).
            let run_threads: Vec<String> = state
                .active
                .iter()
                .map(|run| run.thread_id.clone())
                .collect();

            for run in &mut state.active {
                run.cancel_requested = true;
                run.cancel.cancel();
                cancelled = true;
            }

            // A cancelled run claims nothing else in its lane, but the lane's BACKLOG survives unless
            // `all` says otherwise (`sessions.ts:7530 stopRequested`). A legacy run has `""` as its
            // conversation, which is the legacy queue and has no lane row.
            for thread_id in run_threads {
                if let Some(lane) = state.lanes.get_mut(&thread_id) {
                    lane.stop_requested = true;
                }
            }

            let closed_actions = state.expire_actions(ACTION_CANCELLED_MESSAGE, None);

            if all {
                // Stop-ALL marks every HOST conversation stopped and drops every backlog. The
                // default `agent:<name>` conversations are internal plumbing, so they stay usable
                // (`sessions.ts:7540-7556`).
                //
                // A conversation that ACTUALLY transitioned counts as a cancellation on its own
                // (`sessions.ts:7765-7788`: `if (stopped > 0) cancelled = true`), which is the case TS
                // answers `cancelled: true` for on a session with host threads, no live run and no
                // backlog. One already stopped is a no-op, exactly as TS's `stopThread` returning
                // `false` is.
                for thread_id in state.thread_ids() {
                    if let Some(thread) = state.thread_mut(&thread_id)
                        && thread.origin == store::origin_str(contract::ThreadOrigin::Host)
                    {
                        let stopped_now = thread.status
                            != store::lifecycle_str(contract::ThreadLifecycle::Stopped);

                        thread.status =
                            store::lifecycle_str(contract::ThreadLifecycle::Stopped).to_owned();

                        cancelled |= stopped_now;
                    }
                }

                let mut dropped_ids: Vec<String> = state.queue.clone();

                state.queue.clear();

                for lane in state.lanes.values_mut() {
                    dropped_ids.extend(lane.queue.iter().cloned());
                    lane.queue.clear();
                }

                for turn_id in dropped_ids {
                    if let Some(turn) = state.turns.get_mut(&turn_id) {
                        turn.record.status = "cancelled".to_string();
                        turn.record.updated_at = now;
                        turn.projector.settle("cancelled", now);
                        cancelled = true;
                    }
                }
            }

            if !cancelled {
                // A cancel that found nothing changes no state, so it persists and publishes
                // nothing (`api.ts:2341`: `cancelled: false` is still a 200). A question that DID
                // settle above still gets its announcement, because its promise is already resolved.
                if !closed_actions.is_empty() {
                    self.publish_actions_closed(
                        id,
                        &closed_actions,
                        store::actions::action_closed_reason(ACTION_CANCELLED_MESSAGE),
                    );
                }

                return Ok(serde_json::to_value(contract::CancelSessionResult {
                    cancelled: false,
                    session_id: id.to_string(),
                    dropped_queued_tasks: 0,
                    active_turn_id,
                    revision: self.hub.revision("session", Some(id)),
                })
                .unwrap_or(Value::Null));
            }

            state.record.updated_at = now;

            // The announce payload is built while the session is still borrowed, and the borrow ends
            // before the write: `commit` serializes the whole store through the same guard.
            let payload = json!({
                "session": store::read::session_state_patch_of(state, &self.hub),
            });

            // The `turn` target has to be a live String for the whole announce, so the key is owned
            // outside the vector rather than borrowed out of a temporary.
            let turn_resource = active_turn_id
                .as_ref()
                .map(|turn_id| store::turn_key(id, turn_id));

            let mut targets: Vec<(&str, Option<&str>)> =
                vec![("session", Some(id)), ("queue", Some(id))];

            if let Some(key) = turn_resource.as_deref() {
                targets.push(("turn", Some(key)));
            }

            let mut outcome = CommitOutcome::default();

            for (resource, key) in targets.clone() {
                outcome
                    .bumps
                    .push((resource.to_owned(), key.map(str::to_owned)));
            }

            outcome = outcome.event_with_ops(
                "session.updated",
                &targets,
                None,
                active_turn_id.as_deref(),
                payload,
                Vec::new(),
            );

            // T16: the cancel settled every question it found, so each one announces itself on the
            // `actions` resource (`api.ts:1126-1138` moves that counter once per settlement). The
            // frames ride the same write, so a failed document never publishes a settlement that was
            // rolled back.
            for request_id in &closed_actions {
                outcome = outcome.event(
                    "action.closed",
                    &[("actions", Some(id))],
                    None,
                    active_turn_id.as_deref(),
                    json!({
                        "action_closed": {
                            "request_id": request_id,
                            "reason": store::actions::action_closed_reason(
                                ACTION_CANCELLED_MESSAGE,
                            ),
                        }
                    }),
                );
            }

            if let Err(error) = self.commit(&inner, id, &mut outcome) {
                self.rollback_state(&mut inner, id, &before);

                return Err(error);
            }

            outcome.revision
        };

        let result = contract::CancelSessionResult {
            cancelled,
            session_id: id.to_string(),
            dropped_queued_tasks: dropped,
            active_turn_id,
            revision,
        };

        Ok(serde_json::to_value(result).unwrap_or(Value::Null))
    }

    // -----------------------------------------------------------------------
    // Submit
    // -----------------------------------------------------------------------

    /// The one submit surface (`sessions.ts:6047-6101` `dispatch`).
    ///
    /// A slash line becomes a fully-recorded COMMAND turn that is terminal before this call returns
    /// (no model, no queue, no lane); everything else goes to the queue path untouched.
    ///
    /// Returns the `202` [`contract::TaskReceipt`].
    pub async fn submit_task(
        &self,
        id: &str,
        request: SubmitTaskRequest,
    ) -> Result<Value, ApiError> {
        // `parseCommandLine` answers "not a command" for text that does not start with `/` after
        // trimming, and a `/name` that is not a registered command still takes the COMMAND path
        // because the refusal is the answer (`core/command.ts:170-188`, `sessions.ts:6054-6062`).
        if store::commands::is_command_prompt(&request.input) {
            return self
                .submit_command(id, &request)
                .await
                .map(|receipt| json!(receipt));
        }

        self.submit_queued(id, request)
            .await
            .map(|receipt| json!(receipt))
    }

    // -----------------------------------------------------------------------
    // Shared helpers
    // -----------------------------------------------------------------------

    /// The current session resource, for a caller that already holds the guard.
    ///
    /// Both live figures come out of the SAME guard: `endpoint` from the session's own settings and
    /// credential (`api-reads.ts:225 providerDefaultsOf(record.settings)`), `runtime_execution` from
    /// the store's host view of the deployment (`api-reads.ts:226 context.runtime.threadExecution`).
    /// [`StoreConfig`] owns only the SESSION deployment (root, caps, TTLs); the process runtime lives
    /// in [`crate::config::ServerConfig`], which the host loads once and pushes in through
    /// [`Store::set_host_view`] — so no environment read happens inside a request, and two reads of
    /// one session cannot disagree.
    pub(crate) fn session_resource(
        &self,
        inner: &store::StoreInner,
        state: &SessionState,
    ) -> Value {
        serde_json::to_value(store::read::session_resource_of(
            state,
            &self.hub,
            inner.runtime_config(),
        ))
        .unwrap_or(Value::Null)
    }

    /// The roster keys a session accepts a model or an agent PATCH for
    /// (`normalizeAgentModels`' member set: `Object.keys(harness.agents)`, `sessions.ts:4494`).
    pub(crate) fn roster_of(&self, id: &str) -> Result<Vec<String>, ApiError> {
        let inner = self.lock();
        let state = require_session(&inner, id)?;

        Ok(state.roster())
    }

    /// The durable snapshot a mutation rolls back to, captured before the mutation runs.
    ///
    /// WHY A RECORD RATHER THAN A FULL STATE CLONE: `TurnState` owns a `Box<dyn Projector>`, which
    /// is not `Clone`, and the projector is a pure fold of the turn's recorded events (`live ==
    /// replay`). Rebuilding the state from the prior record therefore restores every projector
    /// exactly, using the same code path a restart uses - one mechanism, not two.
    pub(crate) fn snapshot_state(
        &self,
        state: &SessionState,
    ) -> crate::persistence::HarnessSessionRecord {
        state.projected_record(self)
    }

    /// Rebuilds one session's state from a prior record after a failed write, keeping the handles a
    /// record cannot carry (the live runs, the steering channels, the lanes, the parked questions, the
    /// credential).
    ///
    /// The journal needs no restoring: it lives in [`StoreInner::journals`] and this store only ever
    /// appends to it, so a rolled-back mutation leaves the log one write ahead — which is what the
    /// "durable state committed, replay log degraded" rule already tolerates
    /// (`sessions.ts:1121-1131`).
    pub(crate) fn rollback_state(
        &self,
        inner: &mut store::StoreInner,
        id: &str,
        previous: &crate::persistence::HarnessSessionRecord,
    ) {
        // The current state is TAKEN out of the map rather than borrowed, because the rebuilt state
        // replaces it in place. The LIST order is left alone, so a rollback never reorders the rail.
        let current = inner.sessions.remove(id);

        let Ok(mut rebuilt) = self.restore_session(previous.clone()) else {
            // A record this store just wrote must rebuild; if it somehow does not, put the caller's
            // state back and let the 500 report the failed write.
            if let Some(current) = current {
                inner.sessions.insert(id.to_owned(), current);
            }

            return;
        };

        if let Some(current) = current {
            rebuilt.active = current.active;
            rebuilt.steer_tx = current.steer_tx;
            rebuilt.lanes = current.lanes;
            rebuilt.pending_actions = current.pending_actions;
            rebuilt.answered_actions = current.answered_actions;
            // The credential is not in the record by design, so a rollback that rebuilt the state from
            // one would otherwise silently drop the session's own endpoint key.
            rebuilt.provider_key = current.provider_key;
            rebuilt.disposed = current.disposed;
            rebuilt.checkpoint_at = current.checkpoint_at;
            rebuilt.expires = current.expires;
        }

        inner.sessions.insert(id.to_owned(), rebuilt);
    }

    /// The create-path publish, which needs five counters to move and one event to go out
    /// (`api.ts:2005-2017`).
    ///
    /// The bumps come FIRST and the resource is built AFTER them, because every revision the resource
    /// prints is a hub counter: a resource built before the bump would report the state of the world the
    /// create just replaced.
    fn built_session_payload(&self, inner: &store::StoreInner, id: &str) -> Value {
        let state = require_session(inner, id).expect("the session was just inserted");

        let resources: Vec<(&str, Option<&str>)> = vec![
            ("session", Some(id)),
            ("definition", Some(id)),
            ("turns", Some(id)),
            ("threads", Some(id)),
            ("session", None),
        ];

        for (resource, key) in resources {
            let _ = self.hub.bump(resource, key);
        }

        let resource = self.session_resource(inner, state);
        let patch = json!({ "session": store::read::session_state_patch_of(state, &self.hub) });

        let _ = self.hub.publish(
            "session.updated",
            &[
                crate::events::ResourceRef::new("session", Some(id.to_owned())),
                crate::events::ResourceRef::host("session"),
            ],
            Some(id),
            None,
            None,
            patch,
        );

        resource
    }

    /// The tool working directory a run's tools use (see `ports::TurnInput::cwd`).
    #[must_use]
    pub fn workspace(&self) -> std::path::PathBuf {
        tool_cwd(&self.config.root)
    }
}

// ---------------------------------------------------------------------------
// Message constants the lifecycle shares with the turn layer
// ---------------------------------------------------------------------------

/// `ACTION_CANCELLED_MESSAGE` (`sessions.ts:330-331`).
pub const ACTION_CANCELLED_MESSAGE: &str = "The action request was cancelled before the owner answered. Ask again only if the question is still required.";

/// `ACTION_RUN_ENDED_MESSAGE` (`sessions.ts:332-333`).
pub const ACTION_RUN_ENDED_MESSAGE: &str = "The run ended before the action request was answered. Ask again if the question is still required.";

/// `ACTION_SESSION_GONE_MESSAGE` (`sessions.ts:334-335`).
pub const ACTION_SESSION_GONE_MESSAGE: &str =
    "The session was removed before the action request was answered.";

/// `ACTION_DUPLICATE_MESSAGE` (`sessions.ts:336`).
pub const ACTION_DUPLICATE_MESSAGE: &str =
    "An action request with this requestId is already pending.";

/// The turn-level sentence a cancelled run reports (`sessions.ts:7344`).
pub const CANCELLED_RUN_MESSAGE: &str = "The run was cancelled.";

// ---------------------------------------------------------------------------
// Query helpers (`api.ts:481-501`)
// ---------------------------------------------------------------------------

/// The resolved paging window of one route (`api-paging.ts:95-121` through `paging::resolve_limit`
/// and `paging::decode_cursor`).
#[derive(Debug, Clone, Copy)]
pub struct PageWindow {
    pub cursor: u64,
    pub limit: u64,
}

/// Reads `limit`/`cursor` the way every paged route does. The `epoch` is the hub's, because a cursor
/// this server issued always names the CURRENT process epoch (`api-paging.ts:53-58`).
pub(crate) fn paging_query(query: &Query, epoch: &str) -> Result<PageWindow, ApiError> {
    let limit = paging::resolve_limit(param(query, "limit"))?;
    let cursor = paging::decode_cursor(epoch, param(query, "cursor"))?;

    Ok(PageWindow { cursor, limit })
}

/// `paramOf` (`api.ts:482-488`): trimmed, and an absent-or-empty value is "not supplied".
#[must_use]
pub(crate) fn param<'a>(query: &'a Query, name: &str) -> Option<&'a str> {
    query
        .get(name)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// `enumParamOf` (`api.ts:491-501`): an unrecognized value is a 400 naming the parameter.
pub(crate) fn enum_param<'a>(
    query: &'a Query,
    name: &str,
    allowed: &[&'static str],
) -> Result<Option<&'a str>, ApiError> {
    let Some(value) = param(query, name) else {
        return Ok(None);
    };

    if allowed.contains(&value) {
        return Ok(Some(value));
    }

    Err(ApiError::bad_request(format!(
        "Query parameter \"{name}\" must be one of: {}.",
        allowed.join(", ")
    )))
}

/// `requireObjectBody` (`api.ts:512-518`).
pub(crate) fn require_object(body: &Value) -> Result<serde_json::Map<String, Value>, ApiError> {
    match body {
        Value::Object(map) => Ok(map.clone()),
        _ => Err(ApiError::bad_request(
            "The request body must be a JSON object.",
        )),
    }
}

/// `Unknown session "<id>".` (`sessions.ts:2355-2362`).
pub(crate) fn require_session<'a>(
    inner: &'a store::StoreInner,
    id: &str,
) -> Result<&'a SessionState, ApiError> {
    inner
        .session(id)
        .ok_or_else(|| ApiError::not_found(format!("Unknown session \"{id}\".")))
}

/// The mutable form of [`require_session`].
pub(crate) fn require_session_mut<'a>(
    inner: &'a mut store::StoreInner,
    id: &str,
) -> Result<&'a mut SessionState, ApiError> {
    if inner.session(id).is_none() {
        return Err(ApiError::not_found(format!("Unknown session \"{id}\".")));
    }

    Ok(inner
        .session_mut(id)
        .expect("the session was just confirmed present"))
}

/// `renameSession`'s title rule (`sessions.ts:1765-1783`): trimmed, blank clears, bounded at 200.
#[must_use]
pub fn normalize_title(value: Option<&str>) -> Option<String> {
    let text = value?.trim();

    if text.is_empty() {
        return None;
    }

    Some(text.chars().take(MAX_SESSION_TITLE_CHARS).collect())
}

/// `normalizeAgentModels` (`harnesses/registry.ts:439-464`), against THIS session's roster.
pub(crate) fn parse_agent_models(
    value: &Value,
    members: &[String],
) -> Result<Vec<(String, String)>, ApiError> {
    let Value::Object(map) = value else {
        return Err(ApiError::bad_request(
            "`agentModels` must be an object mapping agent ids to model ids.",
        ));
    };

    let mut resolved = Vec::with_capacity(map.len());

    for (agent, model) in map {
        if !members.iter().any(|member| member == agent) {
            return Err(ApiError::bad_request(format!(
                "`agentModels` names \"{agent}\", which is not a member of this harness. Members: {}.",
                members.join(", ")
            )));
        }

        let Value::String(text) = model else {
            return Err(ApiError::bad_request(format!(
                "`agentModels.{agent}` must be a model id string."
            )));
        };

        let model = text.trim();

        if model.is_empty() {
            return Err(ApiError::bad_request(format!(
                "`agentModels.{agent}` cannot be empty."
            )));
        }

        // The bound is TS's `.length`, i.e. UTF-16 code units (`registry.ts:432`).
        if model.encode_utf16().count() > crate::registry::presets::MAX_MODEL_ID_LENGTH {
            return Err(ApiError::bad_request(format!(
                "`agentModels.{agent}` cannot exceed {} characters.",
                crate::registry::presets::MAX_MODEL_ID_LENGTH
            )));
        }

        resolved.push((agent.clone(), model.to_string()));
    }

    Ok(resolved)
}

/// The `core` block a brand-new session commits (`harness.snapshot()` of a fresh harness,
/// `sessions.ts:1928-1929`): empty rosters and every sequence high-water at zero.
///
/// It states EXACTLY the keys [`SessionState::write_core_into`] writes and the `restore_*` decoders read,
/// because `Store::restore_session` is also the create path: a session that boots empty and one that
/// reloads from its own first commit must decode to the same live state, including
/// `legacy_pumping` (false, because `threads` is present).
#[must_use]
pub fn initial_core_value() -> Value {
    json!({
        "version": 1,
        "activeThreadId": null,
        "threads": [],
        "threadSequence": 0,
        "createdTools": [],
        "createdToolCount": 0,
        "disabledTools": [],
        "agentTools": {},
        // `core/discovery.ts`: absent means ENABLED; stating it keeps a document written by this build
        // from reading as "pre-discovery".
        "discovery": { "enabled": true },
        "taskRows": [],
        "sessionAgentOverrides": [],
        "steerMailbox": {},
        "steerActive": 0,
    })
}

/// `validateInput` (`sessions.ts:1278-1290`), shared by both submit surfaces so the two cannot
/// disagree about what a valid task is.
pub(crate) fn validate_input(raw: &str, attachment_count: usize) -> Result<String, ApiError> {
    let input = raw.trim().to_string();

    if input.is_empty() && attachment_count == 0 {
        return Err(ApiError::bad_request(EMPTY_TASK_MESSAGE));
    }

    // `validateInput` bounds with `.length`, i.e. UTF-16 code units (`sessions.ts:1278-1290`).
    if input.encode_utf16().count() > MAX_INPUT_CHARS {
        return Err(ApiError::payload_too_large(format!(
            "A task cannot exceed {MAX_INPUT_CHARS} characters."
        )));
    }

    Ok(input)
}

/// The queue ordinal a receipt reports: `findIndex(row.turn_id) + 1`, or absent when the submit is
/// not in a backlog (`api.ts:2195-2230`).
#[must_use]
pub fn queue_position(state: &SessionState, turn_id: &str) -> Option<u64> {
    queue_rows_of(state)
        .iter()
        .position(|row| row.turn_id == turn_id)
        .map(|position| u64::try_from(position).unwrap_or(u64::MAX) + 1)
}

/// The four links every receipt carries (`api.ts:2233-2241`), built from the mount + the ids the
/// submit knows.
#[must_use]
pub fn receipt_links(session_id: &str, turn_id: &str, thread_id: Option<&str>) -> TaskReceiptLinks {
    let root = crate::contract::HARNESS_API_ROOT;

    TaskReceiptLinks {
        turn: format!("{root}/sessions/{session_id}/turns/{turn_id}"),
        turns: format!("{root}/sessions/{session_id}/turns"),
        thread: thread_id.map_or_else(
            || format!("{root}/sessions/{session_id}/turns"),
            |thread| format!("{root}/sessions/{session_id}/threads/{thread}"),
        ),
        stream: format!("{root}/stream"),
    }
}

/// The `TaskReceipt` body of one accepted submit (`api.ts:2223-2244`).
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn receipt(
    session_id: &str,
    turn_id: &str,
    status: contract::SessionTurnStatus,
    thread_id: Option<&str>,
    steered_thread_id: Option<&str>,
    queue_position: Option<u64>,
    session_revision: u64,
    accepted_at: u64,
) -> TaskReceipt {
    TaskReceipt {
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        status,
        thread_id: thread_id
            .or(steered_thread_id)
            .unwrap_or_default()
            .to_string(),
        steered: steered_thread_id.map(|_| true),
        steered_thread_id: steered_thread_id.map(str::to_string),
        queue_position,
        session_revision,
        accepted_at,
        links: receipt_links(session_id, turn_id, thread_id),
    }
}

/// `sessions.ts:419` `VIEW_CHECKPOINT_INTERVAL_MS`, re-exported where the recorder reads it.
pub const CHECKPOINT_INTERVAL_MS: u64 = store::CHECKPOINT_INTERVAL_MS;

/// The queue preview bound, re-exported for the command/task paths that print a row.
pub const QUEUE_PREVIEW: usize = QUEUE_PREVIEW_CHARS;

/// A queued row's bounded preview, so the pump and the queue page cannot state different text.
#[must_use]
pub fn queued_prompt_preview(prompt: &str) -> String {
    prompt_preview(prompt, QUEUE_PREVIEW_CHARS)
}
