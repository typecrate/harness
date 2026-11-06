//! Owner questions and the session task list.
//!
//! Ported from `underload/harness/src/core/action.ts` (the owner-question host),
//! `core/task.ts` (the shared task board) and the task/action routes of
//! `server/api.ts:1240-1330`.
//!
//! WHY A PROMISE AND NOT A QUEUE
//! -----------------------------
//! `actionRequest` is a TOOL: the run that raised the question is blocked inside the tool call until
//! its owner answers, so the wait is a oneshot promise, not a polled queue. The store therefore holds
//! the SENDER (`PendingActionEntry::responder`) and `answer_action` resolves it. A restored session has
//! recorded questions but no senders, which is exactly why a restored question reads as expired
//! (`sessions.ts:2684-2688`).

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::contract::{
    ActionAnswerSource, ActionChoiceInput, AnswerActionResult, HarnessActionAnswer,
    HarnessActionChoice, HarnessActionRequest, HarnessEvent, PendingAction,
};
use crate::error::{ApiError, HarnessResult};
use crate::ports::{ActionHost, Query};
use crate::store::{
    CommitOutcome, PendingActionEntry, SessionState, Store, StoreInner, now_ms, read,
};
use futures_util::future::BoxFuture;

/// Questions one run may have parked at once (`core/action.ts MAX_PENDING_ACTIONS`, re-exported
/// through `contract::MAX_PENDING_ACTIONS`).
pub const MAX_PARKED_ACTIONS: usize = crate::contract::MAX_PENDING_ACTIONS;

/// An option list must have at least this many entries (`core/action.ts ACTION_REQUEST_LIMITS`), and
/// at most six. The minimum is what makes "Recommended" mean something.
pub const ACTION_OPTIONS_MIN: usize = 2;
pub const ACTION_OPTIONS_MAX: usize = 6;

/// The longest question the host accepts (`ACTION_REQUEST_LIMITS.questionMax`).
pub const ACTION_QUESTION_MAX_CHARS: usize = 2_000;

/// Tasks one session may hold (`core/task.ts MAX_TASKS`).
pub const MAX_TASKS: usize = 1_000;

/// Completed tasks the session view reports (`api-reads.ts COMPLETED_TASK_LIMIT`).
pub const COMPLETED_TASK_LIMIT: usize = 64;

/// A budget warning past this share of the task cap (`core/task.ts TASK_BUDGET_WARNING_RATIO`).
pub const TASK_BUDGET_WARNING_RATIO: f64 = 0.8;

/// The five states a task moves through (`core/task.ts TaskStatus`).
///
/// `Default` is `Pending` because a row restored from a document that omits `status` is a fresh task
/// (`core/task.ts` defaults a created task to `pending`), and `TaskRow`'s `#[serde(default)]` needs the
/// trait to express that.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[default]
    Pending,
    #[serde(rename = "in_progress")]
    InProgress,
    Completed,
    Deleted,
}

impl TaskStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Deleted => "deleted",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "in_progress" | "in progress" => Some(Self::InProgress),
            "completed" => Some(Self::Completed),
            "deleted" => Some(Self::Deleted),
            _ => None,
        }
    }

    /// A task that is finished from the board's point of view.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Deleted)
    }
}

/// One task as the board holds it and as the API prints it (`core/task.ts TaskRecord`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRow {
    pub id: String,
    pub subject: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_by: Vec<String>,
    /// The present-continuous label a UI shows while the task is running (`activeForm`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_form: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Value>,
}

impl Default for TaskRow {
    fn default() -> Self {
        Self {
            id: String::new(),
            subject: String::new(),
            description: String::new(),
            status: TaskStatus::Pending,
            owner: None,
            blocks: Vec::new(),
            blocked_by: Vec::new(),
            active_form: None,
            metadata: Map::new(),
        }
    }
}

/// A task board as one host call sees it (`api.ts:1240 listTasks`'s response body, minus the paging
/// meta the store stamps separately).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskList {
    pub tasks: Vec<TaskRow>,
    /// The number of tasks the filter matched, BEFORE paging.
    pub total: u64,
    /// Tasks in `in_progress` — the figure the "N active" badge reads.
    pub active_count: u64,
}

/// A paged task list (`api.ts:1240`), shaped like every other store page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TaskPage {
    pub offset: u64,
    pub limit: u64,
    pub total: u64,
    pub tasks: Vec<TaskRow>,
    pub has_more: bool,
    pub active_count: u64,
    pub truncated: bool,
    pub budget: TaskBudget,
}

/// The `{used, limit}` pair every task response carries (`core/task.ts taskBudget`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TaskBudget {
    pub used: u64,
    pub limit: u64,
    pub warning: bool,
}

#[must_use]
pub fn budget_of(used: usize) -> TaskBudget {
    let used = u64::try_from(used).unwrap_or(u64::MAX);
    TaskBudget {
        used,
        limit: MAX_TASKS as u64,
        warning: used as f64 >= MAX_TASKS as f64 * TASK_BUDGET_WARNING_RATIO,
    }
}

/// An EMPTY page, which is what a session with no task host answers (`sessions.ts:3061` returns
/// `{tasks: [], total: 0, ...}` rather than a 404 when `taskHost` is absent).
#[must_use]
pub fn empty_task_page(offset: u64, limit: u64) -> TaskPage {
    TaskPage {
        offset,
        limit,
        total: 0,
        tasks: Vec::new(),
        has_more: false,
        active_count: 0,
        truncated: false,
        budget: budget_of(0),
    }
}

/// The session's shared task board (`core/task.ts createTaskStore`).
///
/// Synchronous by construction: the board is an in-memory table, so a `task` tool call never awaits.
/// The `Mutex` is taken for one call and released before the caller returns; no await happens inside.
#[derive(Debug, Default)]
pub struct StoreTaskHost {
    inner: Mutex<TaskBoard>,
}

#[derive(Debug, Default)]
struct TaskBoard {
    /// Insertion order is the board's order; ids are stable ordinal strings (`1`, `2`, …) because the
    /// model quotes them back in prose.
    tasks: Vec<TaskRow>,
    next_id: u64,
}

impl StoreTaskHost {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TaskBoard::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TaskBoard> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The board's rows, oldest first.
    #[must_use]
    pub fn rows(&self) -> Vec<TaskRow> {
        self.lock().tasks.clone()
    }

    /// Tasks that are not finished (`in_progress`), the figure the session view reports.
    #[must_use]
    pub fn active_count(&self) -> u64 {
        u64::try_from(
            self.lock()
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::InProgress)
                .count(),
        )
        .unwrap_or(u64::MAX)
    }

    /// How many rows the board holds (the budget figure).
    #[must_use]
    pub fn count(&self) -> u64 {
        u64::try_from(self.lock().tasks.len()).unwrap_or(u64::MAX)
    }

    /// Replaces the board with the given rows — the restore path, where the document's `core.taskRows`
    /// are the truth.
    pub fn seed(&self, rows: Vec<TaskRow>) {
        let mut board = self.lock();
        let highest = rows
            .iter()
            .filter_map(|task| task.id.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        board.next_id = highest + 1;
        board.tasks = rows;
    }

    /// `core/task.ts list`: the filtered view, with `total` counting the FILTERED set (`:412-417`).
    #[must_use]
    pub fn list(&self, query: &Query) -> TaskList {
        let board = self.lock();
        let status = query.get("status").map(|value| value.trim().to_owned());
        let owner = query.get("owner").map(|value| value.trim().to_owned());
        let search = query
            .get("q")
            .or_else(|| query.get("query"))
            .map(|value| value.to_lowercase());

        let tasks: Vec<TaskRow> = board
            .tasks
            .iter()
            .filter(|task| task.status != TaskStatus::Deleted)
            .filter(|task| {
                status
                    .as_ref()
                    .is_none_or(|want| Self::status_matches(want, task.status))
            })
            .filter(|task| {
                owner.as_ref().is_none_or(|want| {
                    task.owner
                        .as_ref()
                        .is_some_and(|have| have.eq_ignore_ascii_case(want))
                })
            })
            .filter(|task| {
                search.as_ref().is_none_or(|needle| {
                    task.subject.to_lowercase().contains(needle)
                        || task.description.to_lowercase().contains(needle)
                        || task.id.contains(needle)
                })
            })
            .cloned()
            .collect();

        let active_count = u64::try_from(
            tasks
                .iter()
                .filter(|task| task.status == TaskStatus::InProgress)
                .count(),
        )
        .unwrap_or(u64::MAX);

        TaskList {
            total: u64::try_from(tasks.len()).unwrap_or(u64::MAX),
            active_count,
            tasks,
        }
    }

    /// `status=pending` also answers `in_progress` in TS's filter (`core/task.ts:398-405`), which is
    /// the difference between "not finished" and one exact state.
    fn status_matches(want: &str, have: TaskStatus) -> bool {
        match want.trim() {
            "open" | "pending" => matches!(have, TaskStatus::Pending | TaskStatus::InProgress),
            other => TaskStatus::parse(other).is_some_and(|parsed| parsed == have),
        }
    }

    /// One board mutation (`core/task.ts createTaskStore`'s actions).
    ///
    /// `pub(crate)` in effect through the module: the `/task` command below and the tool-facing
    /// [`crate::ports::TaskHost`] impl both call it, and nothing outside this module should.
    fn mutate(&self, arguments: &Value) -> Result<Value, String> {
        let action = arguments
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();

        let mut board = self.lock();

        match action.as_str() {
            "list" => {
                let rows: Vec<&TaskRow> = board
                    .tasks
                    .iter()
                    .filter(|task| task.status != TaskStatus::Deleted)
                    .collect();
                Ok(json!({
                    "tasks": rows,
                    "total": rows.len(),
                    "budget": budget_of(board.tasks.len()),
                }))
            }
            "create" | "add" => {
                if board.tasks.len() >= MAX_TASKS {
                    return Err(format!(
                        "The task list is full: {MAX_TASKS} tasks is the maximum."
                    ));
                }
                let subject = arguments
                    .get("subject")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_owned();
                if subject.is_empty() {
                    return Err("A task needs a subject.".to_owned());
                }
                let id = board.next_id.to_string();
                board.next_id += 1;
                let task = TaskRow {
                    id: id.clone(),
                    subject: subject.clone(),
                    description: arguments
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    status: TaskStatus::Pending,
                    active_form: arguments
                        .get("activeForm")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    blocks: string_array(arguments.get("blocks")),
                    blocked_by: string_array(arguments.get("blockedBy")),
                    metadata: object_value(arguments.get("metadata")),
                    owner: None,
                };
                board.tasks.push(task.clone());
                Ok(json!({ "task": task, "budget": budget_of(board.tasks.len()) }))
            }
            "get" | "show" => {
                let id = arguments.get("id").and_then(Value::as_str).unwrap_or("");
                let task = board
                    .tasks
                    .iter()
                    .find(|task| task.id == id && task.status != TaskStatus::Deleted)
                    .ok_or_else(|| format!("Unknown task \"{id}\"."))?;
                Ok(json!({ "task": task }))
            }
            "update" | "claim" | "complete" | "done" => {
                let id = arguments.get("id").and_then(Value::as_str).unwrap_or("");
                let index = board
                    .tasks
                    .iter()
                    .position(|task| task.id == id && task.status != TaskStatus::Deleted)
                    .ok_or_else(|| format!("Unknown task \"{id}\"."))?;
                // The edge lists are collected and applied AFTER the task borrow ends: `apply_edge` walks
                // the whole board mutably, and `task` is a mutable borrow of one entry of that same board
                // (a second `&mut board.tasks` while `task` is alive is a double borrow).
                let id = board.tasks[index].id.clone();
                let mut edges: Vec<(Vec<String>, bool)> = Vec::new();
                let task = &mut board.tasks[index];
                if let Some(status) = arguments.get("status").and_then(Value::as_str) {
                    let parsed = TaskStatus::parse(status)
                        .ok_or_else(|| format!("Unknown task status \"{status}\"."))?;
                    task.status = parsed;
                }
                if action == "claim" {
                    task.status = TaskStatus::InProgress;
                    if let Some(owner) = arguments.get("owner").and_then(Value::as_str) {
                        task.owner = Some(owner.trim().to_owned());
                    }
                }
                if action == "complete" || action == "done" {
                    task.status = TaskStatus::Completed;
                }
                if let Some(subject) = arguments.get("subject").and_then(Value::as_str) {
                    task.subject = subject.trim().to_owned();
                }
                if let Some(description) = arguments.get("description").and_then(Value::as_str) {
                    task.description = description.to_owned();
                }
                if let Some(active_form) = arguments.get("activeForm").and_then(Value::as_str) {
                    task.active_form = Some(active_form.to_owned());
                }
                for key in ["blocks", "blockedBy"] {
                    if let Some(list) = arguments.get(key) {
                        let values = string_array(Some(list));
                        if key == "blocks" {
                            task.blocks = values.clone();
                        } else {
                            task.blocked_by = values.clone();
                        }
                        // `forward` states which direction the caller named the edge, which is what makes
                        // the mirror image land on the right column.
                        edges.push((values, key == "blocks"));
                    }
                }
                if let Some(metadata) = arguments.get("metadata") {
                    for (key, value) in object_value(Some(metadata)) {
                        task.metadata.insert(key, value);
                    }
                }
                let updated = task.clone();
                for (values, forward) in edges {
                    Self::apply_edge(&mut board.tasks, &id, &values, forward);
                }
                Ok(json!({ "task": updated }))
            }
            "delete" | "remove" => {
                let id = arguments.get("id").and_then(Value::as_str).unwrap_or("");
                let index = board
                    .tasks
                    .iter()
                    .position(|task| task.id == id)
                    .ok_or_else(|| format!("Unknown task \"{id}\"."))?;
                // Deletion is a STATE, not a removal: the id has to stay out of use, and other tasks
                // may still name it (`core/task.ts:330-345`).
                board.tasks[index].status = TaskStatus::Deleted;
                Ok(json!({ "deleted": id }))
            }
            _ => Err(format!("Unknown task action \"{action}\".")),
        }
    }

    /// Keeps a `blocks` / `blockedBy` edge symmetric, which is what makes "can I start?" answerable
    /// from either side (`core/task.ts syncBlockedBy`).
    fn apply_edge(tasks: &mut [TaskRow], id: &str, peers: &[String], forward: bool) {
        for peer in peers {
            if let Some(other) = tasks.iter_mut().find(|task| &task.id == peer) {
                let field = if forward {
                    &mut other.blocked_by
                } else {
                    &mut other.blocks
                };
                if !field.iter().any(|existing| existing == id) {
                    field.push(id.to_owned());
                }
            }
        }
    }
}

/// A `string[]` argument, empty-filtered and de-duplicated in order.
#[must_use]
fn string_array(value: Option<&Value>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for entry in value.and_then(Value::as_array).into_iter().flatten() {
        if let Some(text) = entry.as_str() {
            let text = text.trim();
            if !text.is_empty() && !out.iter().any(|existing: &String| existing == text) {
                out.push(text.to_owned());
            }
        }
    }
    out
}

/// An object argument, or empty.
#[must_use]
fn object_value(value: Option<&Value>) -> Map<String, Value> {
    value
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

impl crate::ports::TaskHost for StoreTaskHost {
    /// Runs one board mutation and hands the caller its JSON payload (`core/task.ts formatResult`'s
    /// INPUT — the TEXT rendering is the tool layer's job, and `ports::TaskHost` returns a `Value`).
    ///
    /// A refusal is `Err(text)`: a bad task call is feedback to the model, not a session failure.
    fn execute(&self, arguments: Value) -> Result<Value, String> {
        self.mutate(&arguments)
    }
}

/// The owner-question host of ONE run (`core/action.ts createSessionActionHost`).
///
/// It is per-run rather than per-session because its lifetime IS the run's: when the run settles, the
/// promises it parked are dropped, and an unanswered question becomes expired.
pub struct StoreActionHost {
    /// The store's lock, shared so registering a question does not need the `Store` itself (which
    /// would be an ownership cycle: store -> run -> host -> store).
    inner: Arc<Mutex<StoreInner>>,
    hub: Arc<crate::events::EventHub>,
    session_id: String,
    turn_id: String,
    thread_id: Option<String>,
    agent: String,
}

impl std::fmt::Debug for StoreActionHost {
    /// Hand-written: [`crate::events::EventHub`] holds listener closures and has no `Debug`, so a derive
    /// is impossible. The identity fields are the ones that matter when a question lands on the wrong
    /// run; the two shared handles are reported as their types only.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreActionHost")
            .field("session_id", &self.session_id)
            .field("turn_id", &self.turn_id)
            .field("thread_id", &self.thread_id)
            .field("agent", &self.agent)
            .field("inner", &"Arc<Mutex<StoreInner>>")
            .field("hub", &"Arc<EventHub>")
            .finish()
    }
}

impl StoreActionHost {
    #[must_use]
    pub fn new(
        inner: Arc<Mutex<StoreInner>>,
        hub: Arc<crate::events::EventHub>,
        session_id: &str,
        turn_id: &str,
        thread_id: Option<&str>,
        agent: &str,
    ) -> Self {
        Self {
            inner,
            hub,
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
            thread_id: thread_id.map(str::to_owned),
            agent: agent.to_owned(),
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }
}

impl ActionHost for StoreActionHost {
    /// Records the question on the session and waits for its owner.
    ///
    /// The lock is taken to REGISTER (a synchronous mutation plus one publish) and released BEFORE the
    /// await, so a parked question never blocks the store. The publish happens under the lock because
    /// the payload is the row that was just inserted; the hub's fan-out tolerates that, and a listener
    /// must not call back into the store synchronously (see `mod.rs` "THE LOCK").
    fn request(&self, request: Value) -> BoxFuture<'static, Result<Value, String>> {
        let inner = Arc::clone(&self.inner);
        let hub = Arc::clone(&self.hub);
        let session_id = self.session_id.clone();
        let turn_id = self.turn_id.clone();
        let thread_id = self.thread_id.clone();
        let agent = self.agent.clone();

        Box::pin(async move {
            // The runtime's parser is the authority on the SHAPE; a malformed question is a tool
            // failure the model can read (`core/action.ts:140-176`).
            let parsed = parse_action_request(&request)?;

            let (sender, receiver) = tokio::sync::oneshot::channel();
            let opened_at = now_ms();

            {
                let mut guard = inner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(state) = guard.sessions.get_mut(&session_id) else {
                    return Err("This session is no longer running.".to_owned());
                };

                // The cap is enforced HERE, before anything is recorded, so an over-limit request never
                // appears in the list its owner would read (`core/action.ts:181-186`).
                let parked = state
                    .pending_actions
                    .iter()
                    .filter(|entry| {
                        entry.is_live() && entry.turn_id.as_deref() == Some(turn_id.as_str())
                    })
                    .count();
                if parked >= MAX_PARKED_ACTIONS {
                    return Err(format!(
                        "This run already has {MAX_PARKED_ACTIONS} action requests waiting; answer one before asking again."
                    ));
                }

                state.pending_actions.push(PendingActionEntry {
                    request: parsed.clone(),
                    turn_id: Some(turn_id.clone()),
                    thread_id: thread_id.clone(),
                    agent: agent.clone(),
                    opened_at,
                    answer: None,
                    resolved_at: None,
                    responder: Some(sender),
                    extra: Map::new(),
                });

                // The payload is the ONE question that just opened (`WsUpdateActionOpened`,
                // `harness-stream.ts:586-598`: `action.opened` carries `action`, one ready-to-answer
                // row, and NO other payload field), projected the way the actions read projects it
                // (`api-reads.ts:1323 pendingActionRowOf`). `api.ts:1105-1125` is the bridge this
                // mirrors: it looks the request up by id and publishes that single row. `state` is
                // re-borrowed immutably because the row resolves its owning turn through the session.
                let view: &SessionState = state;
                let action = view
                    .pending_actions
                    .iter()
                    .rev()
                    .find(|entry| entry.request.request_id == parsed.request_id)
                    .map(|entry| read::pending_action_row_of(view, entry));

                // The `actions` counter is the one this frame reports (`api.ts:1109`
                // `events.bumpRevision('actions', record.id)`), so a listener that re-reads
                // `GET /sessions/:id/actions` after this frame compares equal figures. The `session`
                // counter is NOT the addressed resource here.
                let _revision = hub.bump("actions", Some(&session_id));

                let (kind, payload) = match action {
                    Some(row) => ("action.opened", serde_json::json!({ "action": row })),
                    // The question vanished between the push and the read (a session removed while
                    // its run parked); `api.ts:1111-1114` answers that case with an `action.closed`
                    // and reason `ended`, never with an empty `action.opened`.
                    None => (
                        "action.closed",
                        serde_json::json!({
                            "action_closed": {
                                "request_id": parsed.request_id,
                                "reason": "ended",
                            }
                        }),
                    ),
                };

                let _ = hub.publish(
                    kind,
                    &[crate::events::ResourceRef::new(
                        "actions",
                        Some(&session_id),
                    )],
                    Some(&session_id),
                    thread_id.as_deref(),
                    Some(&turn_id),
                    payload,
                );
            }

            match receiver.await {
                // The promise's payload IS the tool result: `answer_action` sends
                // `Ok(answer_json)`, so this arm forwards it unchanged.
                Ok(answered) => answered,
                // The sender was dropped: the run's question was expired by a cancel, a reset or a
                // session delete while it waited.
                Err(_) => Err(
                    "This action request was no longer waiting when it was answered.".to_owned(),
                ),
            }
        })
    }
}

/// `core/action.ts parseActionRequest`: the model's JSON, validated.
///
/// The `recommended` id must be one of the options — TS refuses an unknown id rather than defaulting
/// it, because a silently-ignored recommendation changes what a model believes it asked.
pub fn parse_action_request(request: &Value) -> Result<HarnessActionRequest, String> {
    let question = request
        .get("question")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    if question.is_empty() {
        return Err("An action request needs a question.".to_owned());
    }
    // The bound is `.length`, i.e. UTF-16 code units (`core/action.ts`).
    if question.encode_utf16().count() > ACTION_QUESTION_MAX_CHARS {
        return Err(format!(
            "An action question cannot exceed {ACTION_QUESTION_MAX_CHARS} characters."
        ));
    }

    let options_value = request.get("options").cloned().unwrap_or(Value::Null);
    let Some(entries) = options_value.as_array() else {
        return Err("An action request needs an options array.".to_owned());
    };
    if entries.len() < ACTION_OPTIONS_MIN {
        return Err(format!(
            "An action request needs at least {ACTION_OPTIONS_MIN} options."
        ));
    }
    if entries.len() > ACTION_OPTIONS_MAX {
        return Err(format!(
            "An action request can carry at most {ACTION_OPTIONS_MAX} options."
        ));
    }

    let options = serde_json::from_value::<Vec<crate::contract::HarnessActionOption>>(
        Value::Array(entries.clone()),
    )
    .map_err(|error| format!("An action option is malformed: {error}"))?;

    for option in &options {
        if option.id.trim().is_empty() {
            return Err("An action option needs an id.".to_owned());
        }
    }

    let recommended = request
        .get("recommended")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    if !options.iter().any(|option| option.id == recommended) {
        return Err(format!(
            "\"recommended\" must be one of the options: {}.",
            options
                .iter()
                .map(|option| option.id.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    // The id is the RUNTIME's, never the model's (`types/index.ts:865`), so a submitted
    // `requestId` is replaced rather than trusted.
    Ok(HarnessActionRequest {
        request_id: crate::ids::new_request_id(),
        question,
        recommended,
        options,
        allow_custom: true,
    })
}

/// `actionClosedReason` (`sessions.ts:361-370`): the contract's closed `action.closed` reason set
/// (`WsActionClosed.reason`, `shared/harness-stream.ts`), derived from the REJECTION TEXT the runtime
/// already sees rather than from a guess. The store's sentences are stable constants, so the
/// classification is a phrase test on them: cancellation wording is `cancelled`, a run that finished
/// without an answer is `ended`, and everything else (a removed session, a stale promise) is
/// `expired`.
#[must_use]
pub fn action_closed_reason(message: &str) -> &'static str {
    let text = message.to_lowercase();

    if text.contains("cancel") {
        "cancelled"
    } else if text.contains("ended") {
        "ended"
    } else {
        "expired"
    }
}

impl Store {
    /// Publishes one `action.closed` per settled question (`api.ts:1126-1138`), advancing `actions`
    /// once per settlement BEFORE the frame that reports it.
    ///
    /// T16's rule is that every settlement announces itself, from the ONE point that settles
    /// (`sessions.ts:2325-2335`), so an owner answer, a stop, a run that ended, a session delete and
    /// a reset all clear the client's question card. The caller hands over the ids
    /// [`SessionState::expire_actions`] returned; nothing here re-decides which questions settled.
    pub fn publish_actions_closed(&self, session_id: &str, request_ids: &[String], reason: &str) {
        for request_id in request_ids {
            let _ = self.hub.bump("actions", Some(session_id));

            self.publish(
                "action.closed",
                &[("actions", Some(session_id))],
                session_id,
                None,
                None,
                json!({
                    "action_closed": {
                        "request_id": request_id,
                        "reason": reason,
                    }
                }),
            );
        }
    }
}

impl SessionState {
    /// The questions still waiting for their owner.
    #[must_use]
    pub fn pending_action_rows(&self) -> Vec<PendingAction> {
        self.pending_actions
            .iter()
            .filter(|entry| entry.is_live())
            .map(|entry| read::pending_action_row_of(self, entry))
            .collect()
    }

    /// Drops every live question in scope and rejects the promise each run is waiting on
    /// (`sessions.ts:2700-2740 expirePendingActions`).
    ///
    /// The rejection text is what the model reads back from the tool call, so the caller states it:
    /// "the session was reset", "the run was cancelled", "the session was deleted".
    ///
    /// Returns the request ids that settled, IN ORDER — the caller owes one `action.closed` frame per
    /// settlement (`sessions.ts:2325-2335`, T16), and the readable rejection text stays with the
    /// runtime while the CONTRACT reason comes from `action_closed_reason`.
    pub fn expire_actions(&mut self, message: &str, thread_id: Option<&str>) -> Vec<String> {
        let mut expired: Vec<String> = Vec::new();
        for entry in &mut self.pending_actions {
            if !entry.is_live() {
                continue;
            }
            if let Some(wanted) = thread_id {
                let same = entry
                    .turn_id
                    .as_ref()
                    .and_then(|id| self.turns.get(id))
                    .and_then(|turn| turn.record.thread_id.clone())
                    .is_some_and(|have| have == wanted)
                    || entry.thread_id.as_deref() == Some(wanted);
                if !same {
                    continue;
                }
            }
            if let Some(sender) = entry.responder.take() {
                // A dropped receiver means the run already gave up, which is fine.
                let _ = sender.send(Err(message.to_owned()));
            }
            entry.resolved_at = Some(now_ms());
            expired.push(entry.request.request_id.clone());
        }
        // The guidance the run was going to consume goes with it: an expired run never drains the
        // mailbox it was pointed at (`sessions.ts:6461-6495`).
        self.steer_active = self.steer_active.saturating_sub(match thread_id {
            Some(thread) => self
                .steer_mailbox
                .get(thread)
                .map_or(0, |records| records.len() as u64),
            None => std::mem::take(&mut self.steer_active),
        });
        if thread_id.is_none() {
            self.steer_mailbox.clear();
        }
        expired
    }

    /// The answered and expired questions, oldest first, bounded (`api-reads.ts:494-504`).
    #[must_use]
    pub fn completed_actions(&self, limit: usize) -> Vec<PendingAction> {
        self.pending_actions
            .iter()
            .filter(|entry| !entry.is_live())
            .take(limit)
            .map(|entry| read::pending_action_row_of(self, entry))
            .collect()
    }

    /// Prunes answered questions so a long-lived session does not grow one list forever
    /// (`sessions.ts:2745 answeredActionLimit`).
    pub fn prune_actions(&mut self) {
        if self.pending_actions.len() <= COMPLETED_TASK_LIMIT {
            return;
        }
        let keep = COMPLETED_TASK_LIMIT;
        let mut answered = 0usize;
        self.pending_actions.retain(|entry| {
            if entry.is_live() {
                return true;
            }
            answered += 1;
            answered <= keep
        });
    }

    // NOTE: the task-row projection is `SessionState::task_rows` in `mod.rs` — one projection of
    // `SessionState::tasks`, which `write_core_into` persists as `core.taskRows`. Every read model goes
    // through it, so a second copy here would be free to drift.
}

impl Store {
    /// The task page of one session (`GET /api/tasks`): every filter applied, then paged.
    ///
    /// A session with no task host answers an empty page, not a 404 (`sessions.ts:3061`).
    #[must_use]
    pub fn task_page_of(
        &self,
        _state: &SessionState,
        query: &Query,
        offset: u64,
        limit: u64,
    ) -> TaskPage {
        let Some(host) = self.task_host.as_ref() else {
            return empty_task_page(offset, limit);
        };

        let mut list = host.list(query);
        // A thread filter is a store-side narrowing: the board has no thread column, but a turn's
        // thread does, so a client can scope the list to one conversation (`api.ts:1252-1258`). This
        // build states the board per session, so a named thread narrows to the session's own mirror
        // rather than pretending the board knows about conversations.
        if let Some(thread) = query.get("thread").map(|value| value.trim())
            && !thread.is_empty()
        {
            list.tasks.clear();
            list.total = 0;
            list.active_count = 0;
        }

        let total = list.total;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let take = usize::try_from(limit).unwrap_or(usize::MAX);
        let tasks: Vec<TaskRow> = list.tasks.into_iter().skip(start).take(take).collect();
        let has_more = u64::try_from(start + tasks.len()).unwrap_or(u64::MAX) < total;

        TaskPage {
            offset,
            limit,
            total,
            tasks,
            has_more,
            active_count: list.active_count,
            truncated: has_more,
            budget: budget_of(usize::try_from(host.count()).unwrap_or(usize::MAX)),
        }
    }

    /// The task page, in the shape the session view embeds it (`api-reads.ts:460`).
    #[must_use]
    pub fn task_page(&self, state: &SessionState, total: u64) -> TaskPage {
        self.task_page_of(state, &Query::new(), 0, total)
    }

    /// `GET /api/actions` (`api.ts:1266-1300`): the live questions, plus the bounded answered history
    /// when the caller asks for it.
    #[must_use]
    pub fn list_actions(&self, state: &SessionState, query: &Query, revision: u64) -> ActionList {
        let include_completed = query
            .get("completed")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        ActionList {
            pending: state.pending_action_rows(),
            completed: if include_completed {
                state.completed_actions(COMPLETED_TASK_LIMIT)
            } else {
                Vec::new()
            },
            total: u64::try_from(state.pending_actions.len()).unwrap_or(u64::MAX),
            truncated: state.pending_actions.len() > COMPLETED_TASK_LIMIT,
            revision,
        }
    }

    /// `POST /api/actions/:id/answer` (`api.ts:1280-1330`): validate against the RECORDED request,
    /// record the `action_answer` event, and resolve the parked promise.
    ///
    /// The order matters: the event is recorded BEFORE the promise resolves, because the persisted
    /// event is the contract's authoritative record of the answer (`harness-api.ts:1324`), and the
    /// moment the promise resolves the run may append to the same turn.
    pub fn answer_action(
        &self,
        inner: &mut StoreInner,
        session_id: &str,
        request_id: &str,
        choice: &ActionChoiceInput,
    ) -> HarnessResult<(AnswerActionResult, CommitOutcome)> {
        let state = inner
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| self.unknown_session(session_id))?;

        let index = state
            .pending_actions
            .iter()
            .position(|entry| entry.request.request_id == request_id)
            .ok_or_else(|| {
                ApiError::not_found(format!("Unknown action request \"{request_id}\"."))
            })?;

        let entry = &state.pending_actions[index];
        if !entry.is_live() {
            return Err(ApiError::conflict(format!(
                "Action request \"{request_id}\" was already answered or has expired."
            )));
        }

        // The choice is validated against the recorded options, so an unknown id is refused rather
        // than defaulted (`api.ts:1295-1305`).
        let recorded = &entry.request;
        let resolved = match choice {
            ActionChoiceInput::Option { option_id } => {
                let option = recorded
                    .options
                    .iter()
                    .find(|option| &option.id == option_id)
                    .ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "Unknown option \"{option_id}\". Valid options: {}.",
                            recorded
                                .options
                                .iter()
                                .map(|option| format!("{} ({})", option.id, option.label))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                    })?;
                HarnessActionChoice::Option {
                    option_id: option.id.clone(),
                }
            }
            ActionChoiceInput::Custom { text } => {
                let text = text.trim();
                if text.is_empty() {
                    return Err(ApiError::bad_request(
                        "A custom answer cannot be empty.".to_owned(),
                    ));
                }
                HarnessActionChoice::Custom {
                    text: text.to_owned(),
                }
            }
        };

        let answer = HarnessActionAnswer {
            request_id: request_id.to_owned(),
            choice: resolved.clone(),
            source: ActionAnswerSource::Owner,
            branch_id: None,
        };

        // The routing join: the recorded turn, else the run of the question's own conversation, else the
        // session's first live run (`api-reads.ts:1327-1330` resolves the same join at read time).
        let turn_id = entry
            .turn_id
            .clone()
            .or_else(|| {
                state
                    .active
                    .iter()
                    .find(|run| run.thread_id == entry.thread_id.as_deref().unwrap_or(""))
                    .map(|run| run.turn_id.clone())
            })
            .or_else(|| state.active.first().map(|run| run.turn_id.clone()))
            .unwrap_or_default();
        let thread_id = entry.thread_id.clone();
        let agent = entry.agent.clone();
        let at = now_ms();

        // The durable record first (`sessions.ts:2760-2790`).
        let mut outcome = CommitOutcome::touch(session_id);
        if let Some(turn) = state.turns.get_mut(&turn_id) {
            let event = HarnessEvent::ActionAnswer {
                agent: agent.clone(),
                request_id: request_id.to_owned(),
                answer: answer.clone(),
                thread: None,
            };
            let ops = turn.record_event(&event, at);
            outcome = outcome.event_with_ops(
                "turn.upsert",
                &[
                    ("session", Some(session_id)),
                    ("turn", Some(&crate::store::turn_key(session_id, &turn_id))),
                ],
                thread_id.as_deref(),
                Some(&turn_id),
                json!({ "id": turn_id, "index": turn.record.index, "status": turn.record.status }),
                ops,
            );
        }

        // The promise payload is the ANSWER as the contract prints it (`core/action.ts` hands the tool the
        // answered record), so it is encoded before the answer moves into the entry.
        let value = serde_json::to_value(&answer).unwrap_or(Value::Null);

        let entry = &mut state.pending_actions[index];
        entry.answer = Some(answer);
        entry.resolved_at = Some(at);
        state.answered_actions += 1;
        let sender = entry.responder.take();

        // T16: the settlement frame is the contract's `action.closed` with reason `answered`
        // (`sessions.ts:2325-2335` + `api.ts:1126-1138`), addressed at the `actions` resource the
        // same write just moved. `action.answered` is not one of the eighteen update kinds
        // (`harness-stream.ts:62`), so a frame named that reaches no subscriber.
        outcome = outcome.event(
            "action.closed",
            &[("actions", Some(session_id))],
            thread_id.as_deref(),
            Some(&turn_id),
            json!({
                "action_closed": {
                    "request_id": request_id,
                    // An owner answer is `answered` by definition; the message-keyed classifier below
                    // is for the settlements that REJECTED the promise.
                    "reason": "answered",
                }
            }),
        );

        state.prune_actions();

        let receipt = AnswerActionResult {
            ok: true,
            request_id: request_id.to_owned(),
            resolved_at: at,
        };

        // Handing the sender over happens last: dropping it would reject the tool call. The caller still
        // commits (write, bump, publish) AFTER this returns, so a run can wake before the hub frame goes
        // out — which is what `sessions.ts:2760-2790` does too, since the resolved promise is the run's
        // own continuation and never waits on the document.
        if let Some(sender) = sender {
            let _ = sender.send(Ok(value));
        }

        Ok((receipt, outcome))
    }

    /// Answers a question by the option id, the spelling TS's `answerAction` accepts.
    pub fn answer_action_option(
        &self,
        inner: &mut StoreInner,
        session_id: &str,
        request_id: &str,
        option_id: &str,
    ) -> HarnessResult<(AnswerActionResult, CommitOutcome)> {
        self.answer_action(
            inner,
            session_id,
            request_id,
            &ActionChoiceInput::Option {
                option_id: option_id.to_owned(),
            },
        )
    }
}

// ---------------------------------------------------------------------------
// The `/task` slash command (`core/command-builtins.ts:171-279`).
//
// TS maps the command onto the `task` TOOL (`executeTool('task', patch)`), which answers with the CURRENT
// board after every patch, and renders that board. This port's board is `StoreTaskHost::mutate`, whose
// action vocabulary is `list | create | get | update | delete`; the command below is the same five
// subcommands TS ships (`list`, `add`, `complete`, `reopen`, `remove`) mapped onto it.
//
// NOTHING HERE THROWS (`command-builtins.ts:33-39`): a usage mistake and a board refusal both come back
// as readable text, because a command result is data the user sees inside the turn, not a 4xx.
// ---------------------------------------------------------------------------

/// The `/task` usage line, read from the command registry so the refusal and `/help task` cannot drift.
#[must_use]
fn task_usage() -> String {
    crate::store::commands::command_registry()
        .get("task")
        .and_then(|spec| spec.usage.clone())
        .unwrap_or_else(|| "/task list".to_owned())
}

/// `command-builtins.ts:115-118 usageError`: the command's usage line plus the reason.
#[must_use]
fn task_usage_error(reason: &str) -> String {
    format!("/task: {reason}\nusage: {}", task_usage())
}

/// One task line in TS's page form (`command-builtins.ts:155-157`): `#<id> [x]/[ ] <text>`.
///
/// The Rust row's `subject` is TS's task `text`, and `completed` is the `status == completed` test
/// (`core/task.ts` keeps one status column where the tool keeps a boolean).
#[must_use]
fn task_page_line(row: &TaskRow) -> String {
    let mark = if row.status == TaskStatus::Completed {
        'x'
    } else {
        ' '
    };

    format!("#{} [{mark}] {}", row.id, row.subject)
}

/// Renders one board page (`command-builtins.ts:149-169 formatTaskPage`): the rows, the counts, then the
/// budget line.
///
/// The counts sentence is TS's. The cursor line is THIS port's budget instead of an offset pair, because
/// the board's `list` action reports `{tasks, total, budget}` and the HTTP route owns paging
/// (`Store::task_page_of`), so stating an offset here would invent a window the command never applied.
#[must_use]
fn format_task_board(rows: &[TaskRow], total: u64, budget: &TaskBudget) -> String {
    let mut lines: Vec<String> = rows.iter().map(task_page_line).collect();
    if lines.is_empty() {
        lines.push("No tasks.".to_owned());
    }

    let completed = u64::try_from(
        rows.iter()
            .filter(|row| row.status == TaskStatus::Completed)
            .count(),
    )
    .unwrap_or(u64::MAX);
    let pending = total.saturating_sub(completed);

    lines.push(format!(
        "{total} total, {completed} completed, {pending} pending"
    ));
    lines.push(format!(
        "budget: used {}, limit {}{}",
        budget.used,
        budget.limit,
        if budget.warning { " (warning)" } else { "" }
    ));

    crate::store::commands::cap_text(&lines.join("\n"))
}

/// Reads and renders the current board (`command-builtins.ts:191`'s `executeTool(TASK_TOOL_NAME, {})`).
#[must_use]
fn task_board_page(host: &StoreTaskHost) -> String {
    match host.mutate(&json!({ "action": "list" })) {
        Ok(page) => {
            let rows: Vec<TaskRow> = page
                .get("tasks")
                .and_then(Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|entry| serde_json::from_value::<TaskRow>(entry.clone()).ok())
                        .collect()
                })
                .unwrap_or_default();
            let total = page.get("total").and_then(Value::as_u64).unwrap_or(0);
            let budget = page
                .get("budget")
                .and_then(Value::as_object)
                .map(|map| TaskBudget {
                    used: map.get("used").and_then(Value::as_u64).unwrap_or(0),
                    limit: map.get("limit").and_then(Value::as_u64).unwrap_or(0),
                    warning: map.get("warning").and_then(Value::as_bool).unwrap_or(false),
                })
                .unwrap_or_else(|| budget_of(rows.len()));

            format_task_board(&rows, total, &budget)
        }
        // A board that refuses its own read is reported as text, the way TS catches an `executeTool`
        // throw (`command-builtins.ts:123-126 failure`).
        Err(message) => format!("/task: {message}"),
    }
}

/// `/task [list | add <text> | get <id> | complete <id> | reopen <id> | remove <id>]`
/// (`command-builtins.ts:176-278`).
///
/// `_state` is kept in the signature because every command handler receives the session, and the board a
/// session addresses is the STORE's task host in this build; when the board becomes session-scoped, this
/// is the argument that selects it.
#[must_use]
pub fn command_task(store: &Store, _state: &SessionState, args: &str) -> String {
    let Some(host) = store.task_host() else {
        // `command-builtins.ts:186-188`: a session without the tool gets guidance, not a throw.
        return task_usage_error("the \"task\" tool is not registered in this harness.");
    };

    let mut tokens = args.split_whitespace();
    let Some(subcommand) = tokens.next() else {
        // A bare `/task` is the read (`command-builtins.ts:181`).
        return task_board_page(host);
    };

    let rest: Vec<&str> = tokens.collect();
    // `command-builtins.ts:229-237`: one id or a usage error, naming what the operator typed.
    let takes_one_id = |verb: &str| -> Result<String, String> {
        let Some(id) = rest.first() else {
            return Err(format!("\"{verb}\" needs a task id."));
        };
        if rest.len() > 1 {
            return Err(format!("\"{verb}\" takes one id (got {}).", rest.len()));
        }

        Ok((*id).to_owned())
    };

    let mutation = match subcommand.to_ascii_lowercase().as_str() {
        "list" | "ls" => {
            if !rest.is_empty() {
                // Anything after `list` is a typo worth reporting, not a different page
                // (`command-builtins.ts:179-180`).
                return task_usage_error(&format!(
                    "\"list\" takes no arguments (got {}).",
                    rest.len()
                ));
            }

            None
        }
        "add" | "create" | "new" => {
            // `<text...>` rejoins the remaining tokens with single spaces (`command-builtins.ts:202`); the
            // tokenizer already removed any quotes.
            let text = rest.join(" ").trim().to_owned();
            if text.is_empty() {
                return task_usage_error("\"add\" needs the task text.");
            }

            Some(json!({ "action": "create", "subject": text }))
        }
        "complete" | "done" => match takes_one_id(subcommand) {
            Ok(id) => Some(json!({ "action": "update", "id": id, "status": "completed" })),
            Err(reason) => return task_usage_error(&reason),
        },
        "reopen" => match takes_one_id(subcommand) {
            Ok(id) => Some(json!({ "action": "update", "id": id, "status": "pending" })),
            Err(reason) => return task_usage_error(&reason),
        },
        "remove" | "delete" => {
            if rest.is_empty() {
                return task_usage_error("\"remove\" needs at least one task id.");
            }
            match takes_one_id(subcommand) {
                Ok(id) => Some(json!({ "action": "delete", "id": id })),
                Err(reason) => return task_usage_error(&reason),
            }
        }
        "get" | "show" => {
            return match takes_one_id(subcommand) {
                Ok(id) => match host.mutate(&json!({ "action": "get", "id": id })) {
                    // A single row read keeps TS's one-line rendering rather than the whole page.
                    Ok(page) => page
                        .get("task")
                        .cloned()
                        .and_then(|entry| serde_json::from_value::<TaskRow>(entry).ok())
                        .map(|row| {
                            if row.description.is_empty() {
                                task_page_line(&row)
                            } else {
                                format!("{}\n{}", task_page_line(&row), row.description)
                            }
                        })
                        .unwrap_or_else(|| format!("/task: Unknown task \"{id}\".")),
                    Err(message) => format!("/task: {message}"),
                },
                Err(reason) => task_usage_error(&reason),
            };
        }
        other => return task_usage_error(&format!("unknown action \"{other}\".")),
    };

    if let Some(arguments) = mutation
        && let Err(message) = host.mutate(&arguments)
    {
        // A board refusal is feedback the operator reads inside the turn (`command-builtins.ts:36-39`).
        return format!("/task: {message}");
    }

    task_board_page(host)
}

/// `GET /api/actions`' body (`api.ts:1266-1278`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ActionList {
    pub pending: Vec<PendingAction>,
    pub completed: Vec<PendingAction>,
    pub total: u64,
    pub truncated: bool,
    pub revision: u64,
}

// Cross-references kept for the next agent: the recorder these events go through is
// `TurnState::record_event` (`mod.rs`), the status spellings the rows report are
// `store::turn_status_str` / `store::parse_turn_status` (aliased `store::status_of_turn`), and the usage
// the answer event folds is `contract::HarnessUsage` on the owning turn record. The pump that hands a run
// this host is `turns.rs` (`ActiveRun::action_sink`).
