//! `{root}/tools` - the tool catalogue read, and the global manual call.
//!
//! TypeScript origin: `src/server/api.ts` - `getTools` (`:1815-1843`), `getToolOne`
//! (`:1845-1854`), and `postToolExecution` (`:1859-1904`), with the two outcome builders
//! `successfulToolResult` (`:585-603`) and `toolCallFailure`.
//!
//! WHY THERE IS NO GLOBAL WRITE SURFACE (`api.ts:1856-1858`): global execution resolves through
//! the DEPLOYMENT catalogue only - "there is no session field to reach another session's registry,
//! and a harness-owned control tool has no standalone implementation at all (hence 403, not 400 -
//! it is a policy refusal)". The catalogue itself is built once at boot (`runtime.rs`), so the only
//! verbs here are reads plus one manual call.

use serde_json::{Map, Value};

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::api::{self, ApiResult, Ctx, missing};
use crate::contract::{self, ToolAvailabilityState};
use crate::error::ApiError;
use crate::events;
use crate::ids;
use crate::persistence;
use crate::ports::{ActionHost, TaskHost, ToolExecCtx, ToolPolicy, ToolRuntime};
use crate::store::Store;

// ---------------------------------------------------------------------------
// The deployment-wide enable switch (`tool-settings.ts`, R3)
// ---------------------------------------------------------------------------

/// One root's switch state: the deny list, the counter the PATCH answers with, and whether the
/// document has been read into memory yet.
#[derive(Debug, Default)]
struct ToolSwitchState {
    /// The deny list, held as a set because the question this store answers is `disabled.contains`.
    disabled: BTreeSet<String>,
    /// `toolSettings.revision()` (`tool-settings.ts`): ONE bump per accepted switch, including a
    /// switch that states the current value, because "the contract's switches never punish a client
    /// for stating the current state" (`api.ts:2252`).
    revision: u64,
    /// Seeded exactly once per process and root, from the document's `toolSettings` member.
    seeded: bool,
}

/// A1 CUSTODY OF THE SWITCH (`tool-settings.ts` "three consumers, one live provider")
///
/// TypeScript keeps this value in the runtime's session store, beside the plugin settings the
/// `PATCH /plugins/{id}` route commits through, and hands the same live object to every harness so
/// a switch changes what an ALREADY RUNNING session declares on its next request. This port has no
/// store field for it yet, so the holder lives in this module, keyed by the runtime's database
/// root: the value is still process-wide-per-root and live, and the document is still the durable
/// copy.
///
/// The gap the next wave closes is the durable CARRY, not the behaviour: `Store::database_of`
/// assembles `state.json` from store state and hands `extra` an empty map, so a store save after a
/// PATCH re-emits the document without the `toolSettings` member (`persistence.rs` models and
/// validates the member, and preserves it through `extra` on a read-write round trip, but the
/// store's own snapshot does not know about it). The one-line fix is to give `StoreInner` the
/// snapshot and re-emit it there; until then a switch survives a restart only when nothing else
/// saved the session state in between.
static TOOL_SWITCHES: OnceLock<Mutex<BTreeMap<PathBuf, ToolSwitchState>>> = OnceLock::new();

/// The per-root switch table, created on first use.
fn tool_switches() -> &'static Mutex<BTreeMap<PathBuf, ToolSwitchState>> {
    TOOL_SWITCHES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Whether one tool is enabled for EVERY session on this deployment
/// (`toolEnablementOf(name)`, `tool-settings.ts` "everything is enabled unless it is listed").
///
/// The first call for a root reads the document's `toolSettings` member, which is the restart half
/// of the same switch the route writes. A member this build cannot parse is treated as no list:
/// the document would already have failed its own load (`persistence::validate_document`), so a
/// value that reaches here is a hand edit, and a stale name is never an error (`persistence.ts:1512`).
fn tool_enabled(ctx: &Ctx, name: &str) -> bool {
    let mut table = tool_switches()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = table.entry(ctx.runtime.root.clone()).or_default();

    if !state.seeded {
        state.disabled = read_disabled_names(&ctx.runtime.root);
        state.seeded = true;
    }

    !state.disabled.contains(name)
}

/// The counter the PATCH answers with (`toolSettings.revision()`).
fn tool_switch_revision(ctx: &Ctx) -> u64 {
    let table = tool_switches()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    table
        .get(&ctx.runtime.root)
        .map_or(0, |state| state.revision)
}

/// Applies one switch (`store.updateToolEnabled`, `sessions.ts:2018`): the live value first, then
/// the durable document, and a failed write is reported WITHOUT rolling the value back
/// (`api.ts:2224-2227`: "the sessions already honour it and a rollback would claim a failure the
/// running conversations did not have").
///
/// Returns the settings-store revision the response header quotes.
///
/// # Errors
///
/// The persistence writer's refusal, surfaced by the route as a 500.
fn set_tool_enabled(ctx: &Ctx, name: &str, enabled: bool) -> Result<u64, ApiError> {
    let root = ctx.runtime.root.clone();

    // 1. The live switch, under the holder's own lock. The store lock is NOT taken while this lock
    // is held, so the crate's `registry -> store` lock order cannot invert here.
    let disabled: Vec<String> = {
        let mut table = tool_switches()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = table.entry(root.clone()).or_default();

        if !state.seeded {
            state.disabled = read_disabled_names(&root);
            state.seeded = true;
        }

        if enabled {
            state.disabled.remove(name);
        } else {
            state.disabled.insert(name.to_owned());
        }

        // An idempotent write still bumps: the counter counts ACCEPTED switches, not changes.
        state.revision += 1;

        state.disabled.iter().cloned().collect()
    };

    // 2. The document, under the store lock so a session commit cannot interleave and lose the
    // member. `database_of` is the store's CURRENT state, so this write is a store save plus the
    // `toolSettings` member rather than a read-modify-write of whatever happens to be on disk.
    if ctx.runtime.store.config().persist {
        let inner = ctx.runtime.store.lock();
        let mut database = ctx.runtime.store.database_of(&inner);

        match persistence::tool_settings_member_value(&disabled) {
            // An empty deny list removes the member entirely; the writer never emits
            // `{ "disabled": [] }` (`persistence.ts:1538-1540`).
            Some(member) => {
                database
                    .extra
                    .insert(persistence::TOOL_SETTINGS_MEMBER.to_owned(), member);
            }
            None => {
                database.extra.remove(persistence::TOOL_SETTINGS_MEMBER);
            }
        }

        persistence::write_harness_database(&root, &database, &Store::write_owner())?;
    }

    Ok(tool_switch_revision(ctx))
}

/// The deny list the durable document states for this root, normalized exactly as the TS reader
/// normalizes it (`persistence.ts:1517-1540`).
fn read_disabled_names(root: &Path) -> BTreeSet<String> {
    let Ok(Some(database)) = persistence::read_harness_database(root) else {
        // No document yet (an ephemeral root or a fresh deployment) is an empty deny list.
        return BTreeSet::new();
    };

    let Some(member) = database.extra.get(persistence::TOOL_SETTINGS_MEMBER) else {
        return BTreeSet::new();
    };

    persistence::normalize_tool_settings(member)
        .ok()
        .flatten()
        .and_then(|record| record.disabled)
        .map(|names| names.into_iter().collect())
        .unwrap_or_default()
}

/// The two filter vocabularies `GET /tools` accepts (`api.ts:1819`'s `enumParamOf` list). The
/// group filter is free text on purpose: the groups are the catalogue's own, and a typo answers an
/// empty page rather than a refusal, exactly as TS does.
const AVAILABILITY_FILTERS: [&str; 2] = ["available", "unconfigured"];

/// `GET {root}/tools` (`api.ts:1815-1843`): the deployment catalogue, paged, with an optional
/// `group` and `availability` filter.
///
/// `availability` is compared the way TS compares it - an entry that carries no availability block
/// is `available` (`entry?.availability?.state ?? 'available'`, `:1826`) - and `/tools` names
/// environment VARIABLES only, never their values, so the page cannot leak a credential
/// (`knowledge.md` T4).
pub async fn get_tools(ctx: &mut Ctx) -> ApiResult {
    let limit = ctx.limit()?;
    let cursor = ctx.cursor()?;

    let group = api::param_of(&ctx.query, "group");
    let availability = api::enum_param(&ctx.query, "availability", &AVAILABILITY_FILTERS)?;

    let revision = ctx.revision("tools", None);

    // R3: every row states whether this deployment runs it, so the Tools pane can draw the switch
    // without a second call (`toolSummaryOf(entry, name, undefined, toolEnablementOf(name))`,
    // `api.ts:2190`). The LIST itself is unfiltered by the switch - a disabled tool stays
    // registered and stays listed (`tool-settings.ts` "what disabled does not mean").
    let mut rows: Vec<contract::ToolListEntry> = ctx
        .runtime
        .tools
        .summaries()
        .into_iter()
        .map(|row| {
            let enabled = tool_enabled(ctx, &row.name);

            contract::ToolListEntry {
                summary: row,
                enabled,
            }
        })
        .collect();

    if let Some(group) = group {
        rows.retain(|row| row.summary.group == group);
    }

    if let Some(availability) = availability {
        let wanted = availability_state(availability);

        rows.retain(|row| availability_of_row(&row.summary) == wanted);
    }

    let total = rows.len();
    let slice = crate::paging::slice_page(rows, cursor, limit, Some(total));

    Ok(api::page_response(ctx, revision, slice))
}

/// The state one row's filter comparison uses: `entry?.availability?.state ?? 'available'`
/// (`api.ts:1826`). A row with NO availability block is therefore `available`, which keeps it OFF the
/// `unconfigured` page - `is_none_or` would have let it match both filters at once.
fn availability_of_row(row: &contract::ToolSummary) -> ToolAvailabilityState {
    row.availability
        .as_ref()
        .map_or(ToolAvailabilityState::Available, |entry| entry.state)
}

/// The enum form of one `availability` filter value (`api::enum_param` already guarantees the
/// spelling is one of the two).
fn availability_state(raw: &str) -> ToolAvailabilityState {
    if raw == "unconfigured" {
        ToolAvailabilityState::Unconfigured
    } else {
        ToolAvailabilityState::Available
    }
}

/// `GET {root}/tools/{tool_id}` (`api.ts:1845-1854`): the entry plus its argument schema, the
/// `{scope, manual}` execution gate, and the deployment-wide enable switch
/// (`toolDetailOf(entry, name, toolEnablementOf(name))`, `api.ts:2256`). No revision header - TS
/// stamps none on this read.
pub async fn get_tool(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("tool_id");

    let detail = ctx
        .runtime
        .tools
        .detail(name)
        .ok_or_else(|| missing("tool", name))?;

    let enabled = tool_enabled(ctx, name);

    let response = contract::ToolDetailEntryResponse {
        tool: contract::ToolDetailEntry { detail, enabled },
    };

    Ok(api::json(
        200,
        serde_json::to_value(response).unwrap_or_default(),
    ))
}

/// `PATCH {root}/tools/{tool_id}` (`patchTool`, `api.ts:2228-2260`): the harness-wide switch.
///
/// The gates, in TS's order, and each one is a contract rule:
///
/// 1. IDENTITY FIRST (`api.ts:2231-2235`): an unknown name is the 404 `Unknown tool "<id>".`, and
///    it is raised before any word about the body, so a typo never produces a 400 about a body the
///    client has not sent yet.
/// 2. `requireKnownFields(body, ["enabled"])` (`:2237`) - one field, and only that one.
/// 3. The value must be a BOOLEAN (`:2239-2241`). A missing field, a `null`, and a string all
///    answer the same sentence, because the contract states one refusal for the whole class.
/// 4. NO protected-name exception (R5, `:2244-2250`) and NO idle gate (R1, `:2216-2226`): the
///    switch gates what an AGENT may reach, so a harness-owned core tool takes this write exactly
///    like a catalogue entry, and a run in flight keeps the declarations it was already given.
/// 5. DURABLE BEFORE THE ANSWER (`:2224-2227`): the commit runs, then the answer. A failed write is
///    a 500 and the in-process value STAYS applied, because the sessions already honour it.
///
/// The answer is the whole identified read (`ToolDetailResponse`), and the header quotes the
/// SETTINGS store's counter - one bump per accepted switch, including an idempotent one - not the
/// catalogue's hub revision, because no registration changed (`:2254-2259`).
pub async fn patch_tool(ctx: &mut Ctx) -> ApiResult {
    // Owned: `ctx.param(..)` borrows `ctx` for the caller's lifetime, and the body read needs
    // `&mut ctx`.
    let name = ctx.param("tool_id").to_owned();

    // 1. Identity first, with the same lookup the GET uses.
    let detail = ctx
        .runtime
        .tools
        .detail(&name)
        .ok_or_else(|| missing("tool", &name))?;

    let body = ctx.body_object().await?;

    // 2. The one field this contract accepts.
    api::require_body_fields(&body, &["enabled"])?;

    // 3. The value, through the DTO so the refusal is the contract's sentence and not serde's
    //    (`contract::PatchHarnessToolRequest` keeps the value untyped for exactly that reason).
    let request: contract::PatchHarnessToolRequest =
        contract::decode_body(Value::Object(body.clone()))?;

    let enabled = request.enabled_boolean()?;

    // 4-5. Apply, persist, then answer.
    let revision = set_tool_enabled(ctx, &name, enabled)?;

    let response = contract::ToolDetailEntryResponse {
        tool: contract::ToolDetailEntry { detail, enabled },
    };

    Ok(api::with_revision(
        200,
        serde_json::to_value(response).unwrap_or_default(),
        revision,
    ))
}

/// `POST {root}/tools/{tool_id}/executions` (`api.ts:1859-1904`): one manual call against the
/// DEPLOYMENT catalogue.
///
/// Both refusals are 403 policy statements, and a tool failure is DATA: a listed-but-unrunnable
/// entry answers `200 {ok: false}` with the catalogue hint (variable names, never values), because
/// "the invocation succeeded and the tool answered" is what the contract distinguishes
/// (`contract::ToolCallResult`).
pub async fn post_tool_execution(ctx: &mut Ctx) -> ApiResult {
    // Owned: this handler reads a body, and `ctx.param(..)` borrows `ctx` for the caller's
    // lifetime, which would collide with the `&mut self` read.
    let name = ctx.param("tool_id").to_owned();

    let detail = ctx
        .runtime
        .tools
        .detail(&name)
        .ok_or_else(|| missing("tool", &name))?;

    let body = ctx.body_object().await?;

    api::require_body_fields(&body, &["parameters"])?;

    // A session-bound tool cannot run here: the session is not in the path (`api.ts:1871-1876`). The
    // test is the ENTRY flag, not the projected `{scope, manual}` object - TS reads
    // `entry.sessionBound === true`, and the two only coincide because `toolDetailOf` derives
    // `session_required` from that same flag (`api-reads.ts:1582`).
    if detail.summary.session_bound {
        return Err(ApiError::forbidden(format!(
            "Tool \"{name}\" runs inside a session; call it through {}/sessions/{{session_id}}/tools/{name}/executions.",
            contract::HARNESS_API_ROOT
        )));
    }

    // `parameters` is optional on this contract; an absent field is an empty argument box, exactly
    // like `{"parameters": {}}` (`api.ts:1878-1882`).
    let parameters = body
        .get("parameters")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));

    let started = events::now_millis();

    // Listed but not executable on this build: the catalogue hint is the message, and the answer is
    // still a well-formed 200 (`api.ts:1884-1891`).
    if !detail.execution.manual {
        let hint = detail
            .summary
            .availability
            .as_ref()
            .and_then(|entry| entry.hint.clone().filter(|hint| !hint.trim().is_empty()));

        let message =
            hint.unwrap_or_else(|| format!("Tool \"{name}\" is not configured on this server."));

        return Ok(api::json(200, failure(message, started)));
    }

    let outcome = ctx
        .runtime
        .tools
        .execute(tool_ctx(ctx, name.clone(), parameters))
        .await;

    let duration_ms = events::now_millis().saturating_sub(started);

    if outcome.ok {
        let mut body = Map::new();
        body.insert("ok".to_owned(), Value::Bool(true));
        body.insert("result".to_owned(), Value::String(outcome.content));

        // The branded `toolUi(...)`/`toolMedia(...)` envelopes were peeled by the runtime
        // (`tools/result.rs`), so what is left travels verbatim - and an empty list is TS's
        // "absent", which `ToolCallSuccess` skips.
        if let Some(panels) = outcome.panels.filter(|panels| !panels.is_null()) {
            body.insert("panels".to_owned(), panels);
        }

        if let Some(media) = outcome.media.filter(|media| !media.is_null()) {
            body.insert("media".to_owned(), media);
        }

        body.insert(
            "duration_ms".to_owned(),
            Value::from(duration_ms.max(outcome.duration_ms)),
        );

        return Ok(api::json(200, Value::Object(body)));
    }

    // A failed call reports the same sentence in `content` and `error`, and the measured span
    // (`knowledge.md` T4, "the three outcome classes").
    let message = outcome.error.unwrap_or(outcome.content).trim().to_string();

    Ok(api::json(
        200,
        failure(
            if message.is_empty() {
                format!("Tool \"{name}\" failed.")
            } else {
                message
            },
            started,
        ),
    ))
}

/// The `ok: false` arm of the manual-call contract (`toolCallFailure`, `api.ts:1887-1890`).
fn failure(message: impl Into<String>, started: u64) -> Value {
    let response = contract::ToolCallFailure {
        ok: false,
        error: contract::HarnessErrorInfo {
            message: message.into(),
        },
        duration_ms: events::now_millis().saturating_sub(started),
    };

    serde_json::to_value(response).unwrap_or_default()
}

/// The per-call context of a GLOBAL manual execution: no session, the deployment cwd, a fresh
/// cancellation token (the request is the run's whole life), the default policy (a manual call is
/// not restricted by a session's enablement list), and the deployment's own task board so a global
/// `task` call reads and writes the same rows the store's sessions see.
fn tool_ctx(ctx: &Ctx, name: String, arguments: Value) -> ToolExecCtx {
    // `Store::task_host()` hands back `Option<&Arc<StoreTaskHost>>`; cloning the concrete Arc and
    // coercing is what `store/tools.rs:603-620` does for a session call, and the trait object is
    // the shape `ToolExecCtx` takes.
    let task_host: Option<std::sync::Arc<dyn TaskHost>> = ctx
        .runtime
        .store
        .task_host()
        .map(|host| std::sync::Arc::clone(host) as std::sync::Arc<dyn TaskHost>);

    let action_host: Option<std::sync::Arc<dyn ActionHost>> = None;

    ToolExecCtx {
        name,
        arguments,
        call_id: ids::new_request_id(),
        session_id: None,
        cwd: ctx.runtime.store.cwd(),
        signal: tokio_util::sync::CancellationToken::new(),
        policy: ToolPolicy::default(),
        action_host,
        task_host,
    }
}
