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

use crate::api::{self, ApiResult, Ctx, missing};
use crate::contract::{self, ToolAvailabilityState};
use crate::error::ApiError;
use crate::events;
use crate::ids;
use crate::ports::{ActionHost, TaskHost, ToolExecCtx, ToolPolicy, ToolRuntime};

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

    let mut rows = ctx.runtime.tools.summaries();

    if let Some(group) = group {
        rows.retain(|row| row.group == group);
    }

    if let Some(availability) = availability {
        let wanted = availability_state(availability);

        rows.retain(|row| availability_of_row(row) == wanted);
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

/// `GET {root}/tools/{tool_id}` (`api.ts:1845-1854`): the entry plus its argument schema and the
/// `{scope, manual}` execution gate. No revision header - TS stamps none on this read.
pub async fn get_tool(ctx: &mut Ctx) -> ApiResult {
    let name = ctx.param("tool_id");

    let detail = ctx
        .runtime
        .tools
        .detail(name)
        .ok_or_else(|| missing("tool", name))?;

    let response = contract::ToolDetailResponse { tool: detail };

    Ok(api::json(
        200,
        serde_json::to_value(response).unwrap_or_default(),
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
