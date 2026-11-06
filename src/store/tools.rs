//! The session toolbox: rows, detail, sources, registration and execution.
//!
//! Ported from `underload/harness/src/server/api-reads.ts:785-925` (`sessionToolRowsOf`,
//! `sessionToolDetailOf`, `sessionToolSourceOf`, `toolPolicySourceOf`), `server/api.ts:820-1035`
//! (the toolbox routes) and `src/harness.ts`'s `ActiveToolRegistry` (the created-tool ledger).
//!
//! WHY DECLARATIONS RATHER THAN EXECUTION
//! --------------------------------------
//! A tool BODY is a TS module (`toolCreate` compiles source text). This Rust build ships the
//! catalogue through [`crate::ports::ToolRuntime`], so a tool CREATED in a session is recorded as a
//! declaration the model can be shown, and executing it answers with a stated refusal. The ledger, the
//! budget, the enablement rules and every HTTP-visible field are the real port; only the compile step
//! is absent. See knowledge.md "Decisions".

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::contract::{
    CreateSessionToolRequest, DeleteSessionToolResult, MAX_CREATED_TOOLS, ResourceBudget,
    SessionToolDetail, SessionToolOrigin, SessionToolRegistrationResult, SessionToolSourceResponse,
    SessionToolSummary, ToolDecl, ToolSourceDependency, ToolSummary, ToolVisibility,
};
use crate::error::{ApiError, HarnessResult};
use crate::ports::{ToolExecCtx, ToolPolicy};
use crate::store::read;
use crate::store::{SessionState, Store, now_ms, status_of_turn};

/// The `sourceRevision` stamp format: a monotonic per-session counter, so a client can tell a newer
/// revision from an older one without comparing text (`harness.ts ActiveToolRegistry`).
#[must_use]
pub fn revision_stamp(count: u64, name: &str) -> String {
    // The counter is the ordered half; the 8-character tail only keeps two creates issued in the same
    // millisecond from colliding, which is what TS's random suffix is for.
    let tail: String = crate::ids::new_id().chars().take(8).collect();

    format!("rev-{count}-{tail}{name}")
}

/// One tool this session created itself (`ActiveToolRegistry`'s `CreatedToolInfo`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedTool {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    /// The source text the operator or the model submitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// The compiled module text, when the submit carried one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<ToolSourceDependency>,
    pub visibility: ToolVisibility,
    /// Whether the model is told about this tool right now.
    pub enabled: bool,
    pub source_revision: String,
    pub created_at: u64,
    /// The run that created it (`toolCreate`'s audit trail), when a model did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_turn: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The refusal a created tool answers with, stated rather than implied.
pub const CREATED_TOOL_NOT_COMPILED: &str = "This tool has no Rust implementation in this build.";

/// Names a session can never un-register or switch off (`api-reads.ts:104-106`): the core set the
/// harness registers plus the frozen definition's own declarations.
#[must_use]
pub fn protected_names(store: &Store, state: &SessionState) -> Vec<String> {
    let definition = store.definition_of(state);
    let mut names: Vec<String> = definition.core_tools().to_vec();
    for name in definition.registered_tools() {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// Whether one name is protected in this session (`api-reads.ts:1078 isProtectedToolName`).
#[must_use]
pub fn is_protected(store: &Store, state: &SessionState, name: &str) -> bool {
    protected_names(store, state)
        .iter()
        .any(|protected| protected == name)
}

impl Store {
    /// The frozen definition of one session, as the registry's own descriptor type. The toolbox needs
    /// it for its declared/core sets, which is why it hangs off `Store` rather than the state.
    #[must_use]
    pub fn definition_of(&self, state: &SessionState) -> crate::registry::HarnessDescriptor {
        crate::store::definition_of(state)
    }

    /// The names one session can call (`api-reads.ts:803-828`'s `enabled` decision).
    #[must_use]
    pub fn tools_for(&self, state: &SessionState) -> Vec<String> {
        let definition = self.definition_of(state);
        // The FULL catalogue, not the listed page: this is the live-registry join TS reads
        // (`api-reads.ts:831-836`), and that registry holds the `listed: false` capability rows.
        let registry: Vec<String> = self
            .runtime
            .catalogue_summaries()
            .into_iter()
            .map(|summary| summary.name.clone())
            .collect();
        let core = definition.core_tools();
        read::enabled_tool_names_of(state, &registry, &core)
    }

    /// The declarations a run is offered: enabled catalogue entries plus this session's created tools
    /// (`src/harness.ts buildAgent`).
    #[must_use]
    pub fn declarations_for(&self, state: &SessionState) -> Vec<ToolDecl> {
        let policy = self.tool_policy(state);
        let mut declarations = self.runtime.declarations(&policy);
        for tool in state.created_tools_in_order() {
            if !tool.enabled {
                continue;
            }
            declarations.push(ToolDecl {
                name: tool.name.clone(),
                description: tool.description.clone(),
                parameters: tool.parameters.clone(),
            });
        }
        declarations
    }

    /// The registration/enablement state a runtime call is made with (`ports::ToolPolicy`).
    #[must_use]
    pub fn tool_policy(&self, state: &SessionState) -> ToolPolicy {
        let definition = self.definition_of(state);
        ToolPolicy {
            enabled: self.tools_for(state),
            declared: definition.registered_tools(),
            created: state
                .created_tools_in_order()
                .into_iter()
                .map(|tool| tool.name.clone())
                .collect(),
            subagents: definition.subagents.clone(),
        }
    }

    /// The `{used, limit}` the toolbox page and the registration receipt print.
    #[must_use]
    pub fn tool_budget(state: &SessionState) -> ResourceBudget {
        read::budget_of(state.created_tools.len())
    }

    /// `GET /sessions/:id/tools` (`api.ts:820-860`): the catalogue rows, filtered and paged, with the
    /// created tools appended.
    #[must_use]
    pub fn session_tool_rows(
        &self,
        state: &SessionState,
        query: &crate::ports::Query,
        limit: u64,
        cursor: u64,
    ) -> (Vec<SessionToolSummary>, u64) {
        let definition = self.definition_of(state);
        let declared_names = definition.registered_tools();
        let core_names = definition.core_tools();
        let enabled = self.tools_for(state);
        let protected = protected_names(self, state);
        let search = query
            .get("q")
            .or_else(|| query.get("query"))
            .map(|value| value.to_lowercase());
        let want_enabled = query
            .get("enabled")
            .and_then(|value| value.parse::<bool>().ok());

        let mut rows: Vec<SessionToolSummary> = Vec::new();

        // Catalogue rows, in catalogue order (`api-reads.ts:830-860`). Every row, including the
        // four the GLOBAL page withholds - a session's live registry holds them.
        for summary in self.runtime.catalogue_summaries() {
            let is_declared =
                declared_names.contains(&summary.name) || core_names.contains(&summary.name);
            let is_enabled = enabled.contains(&summary.name);
            let origin = state.tool_origin_of(&summary.name);
            let created = state.created_tools.get(&summary.name);
            let mut row = read::session_tool_row_shell(
                summary.clone(),
                state.id(),
                is_declared,
                is_enabled,
                origin,
                // `api-reads.ts:824` â€” a harness-owned control tool OR a name the frozen definition
                // declares is protected; the created half of the ledger never is.
                read::is_protected_tool_name(&summary.name) || protected.contains(&summary.name),
                created.is_some(),
                created.map(|tool| tool.source_revision.clone()),
            );
            // A name this session created carries THIS session's description, not the catalogue's
            // (`api-reads.ts:862-880`).
            if let Some(tool) = created {
                row.tool = ToolSummary {
                    description: tool.description.clone(),
                    ..summary.clone()
                };
            }
            rows.push(row);
        }

        // Created tools the catalogue does not know at all (`api-reads.ts:882-905`).
        for tool in state.created_tools_in_order() {
            if rows.iter().any(|row| row.tool.name == tool.name) {
                continue;
            }
            rows.push(read::session_tool_row_shell(
                ToolSummary {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    // A created tool is its own group, which is what the toolbox UI sorts by.
                    group: "Created".into(),
                    tags: tool.tags.clone(),
                    visibility: tool.visibility,
                    availability: None,
                    // Everything this session authored is session-bound by definition.
                    session_bound: true,
                },
                state.id(),
                false,
                tool.enabled,
                SessionToolOrigin::Created,
                false,
                true,
                Some(tool.source_revision.clone()),
            ));
        }

        // The two catalogue filters (`api.ts:832-852`): `?enabled=true|false` and a `q` that matches the
        // name, the description or any search term (`ToolSummary.tags` IS the search-term list â€” the
        // contract has no separate field).
        let rows: Vec<SessionToolSummary> = rows
            .into_iter()
            .filter(|row| {
                want_enabled.is_none_or(|want| row.enabled == want)
                    && search.as_ref().is_none_or(|needle| {
                        row.tool.name.to_lowercase().contains(needle)
                            || row.tool.description.to_lowercase().contains(needle)
                            || row
                                .tool
                                .tags
                                .iter()
                                .any(|term| term.to_lowercase().contains(needle))
                    })
            })
            .collect();

        let total = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let start = usize::try_from(cursor).unwrap_or(usize::MAX);
        let take = usize::try_from(limit).unwrap_or(usize::MAX);
        let page: Vec<SessionToolSummary> = rows.into_iter().skip(start).take(take).collect();
        (page, total)
    }

    /// `GET /sessions/:id/tools/:name` (`api.ts:862-905`): a row plus the argument schema and the
    /// execution gate.
    pub fn session_tool_detail(
        &self,
        state: &SessionState,
        name: &str,
    ) -> HarnessResult<SessionToolDetail> {
        let (rows, _) = self.session_tool_rows(state, &crate::ports::Query::new(), u64::MAX, 0);
        let summary = rows
            .into_iter()
            .find(|row| row.tool.name == name)
            .ok_or_else(|| unknown_tool(state, name))?;

        let created = state.created_tools.get(name);
        let parameters = created
            .and_then(|tool| tool.parameters.clone())
            .or_else(|| {
                self.runtime
                    .detail(name)
                    .and_then(|detail| detail.parameters.clone())
            });
        // `api-reads.ts:116-121 toolExecutionScopeOf`: the session surface states `session_required` for a
        // protected control tool and `session_only` for anything else, with `manual: true` for every live
        // entry â€” manual execution is the documented session surface, so it is never inferred by the client.
        let scope = if is_protected(self, state, name) || read::is_protected_tool_name(name) {
            crate::contract::ToolExecutionScopeKind::SessionRequired
        } else {
            crate::contract::ToolExecutionScopeKind::SessionOnly
        };
        let execution = Some(crate::contract::ToolExecutionScope {
            scope,
            manual: true,
        });

        Ok(SessionToolDetail {
            summary,
            parameters,
            execution,
        })
    }

    /// `GET /sessions/:id/tools/:name/source` (`api.ts:907-940`).
    pub fn session_tool_source(
        &self,
        state: &SessionState,
        name: &str,
    ) -> HarnessResult<SessionToolSourceResponse> {
        let tool = state.created_tools.get(name).ok_or_else(|| {
            ApiError::not_found(format!(
                "Tool \"{name}\" has no installed source revision in session \"{}\".",
                state.id()
            ))
        })?;
        Ok(SessionToolSourceResponse {
            session_id: state.id().to_owned(),
            name: tool.name.clone(),
            revision: tool.source_revision.clone(),
            source_path: tool.source_path.clone(),
            module_text: tool.module_text.clone(),
            source: tool.source.clone(),
            dependencies: (!tool.dependencies.is_empty()).then(|| tool.dependencies.clone()),
        })
    }

    /// `POST /sessions/:id/tools` (`api.ts:942-1000`): validate, budget-check, record.
    ///
    /// A create NEVER runs the tool (`api-reads.ts`'s "compiles and never executes" rule), so the only
    /// side effect is a ledger entry plus one hub bump.
    pub fn create_session_tool(
        state: &mut SessionState,
        store: &Store,
        input: &CreateSessionToolRequest,
        created_by_turn: Option<&str>,
    ) -> HarnessResult<SessionToolRegistrationResult> {
        let name = input.name.trim();
        validate_tool_name(state, store, name)?;
        let description = input.description.trim();
        if description.is_empty() {
            // The kernel's sentence for a blank description (`creation.ts:128-130`): the route
            // owns the ABSENT case with its own wording, and a present-but-blank one reaches here.
            return Err(ApiError::bad_request(format!(
                "Tool \"{name}\" requires a description."
            )));
        }
        // Tags are REQUIRED because an untagged tool could never be discovered
        // (`harness-api.ts:1082`); a blank/whitespace-only list is the kernel's refusal
        // (`creation.ts:138-140`).
        let tags: Vec<String> = input
            .tags
            .iter()
            .map(|tag| tag.trim().to_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect();
        if tags.is_empty() {
            return Err(ApiError::bad_request(format!(
                "Tool \"{name}\" requires at least one tag, so toolRequest can find it."
            )));
        }

        let existing = state.created_tools.get(name);
        // A name this session already holds is a REPLACEMENT, not a collision (`api.ts:3064-3095`,
        // `harness.ts:1189-1191`: "names are not reserved, 'already registered' is not an error,
        // and replacing an implementation does not consume the new-tool budget"). TypeScript's
        // create path therefore answers 201 with `created: false, replaced: true` plus the previous
        // revision, and `replace` never reaches it as a flag - the POST route refuses that spelling
        // as a capability (`api.ts:3163-3168`), which is what the handler does before calling here.
        let replacing = existing.is_some();
        if !replacing && state.created_tools.len() >= MAX_CREATED_TOOLS {
            return Err(ApiError::conflict(format!(
                "This session can create at most {MAX_CREATED_TOOLS} tools. Remove one first."
            )));
        }

        let previous_revision = existing.map(|tool| tool.source_revision.clone());
        state.created_tool_count += 1;
        let source_path = input
            .source_path
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .map(|path| format!("tools/{name}/{path}"));
        // ONE stamp for the installed revision: `revision_stamp` embeds a random suffix, so calling it
        // twice minted two different revisions and the receipt stopped naming the revision the ledger
        // and the source route report (`api.ts:920` returns this figure to the caller).
        let source_revision = revision_stamp(state.created_tool_count, name);
        let tool = CreatedTool {
            name: name.to_owned(),
            description: description.to_owned(),
            tags,
            parameters: input.parameters.clone(),
            source: input.source.clone(),
            module_text: input.module_text.clone(),
            source_path: source_path.clone(),
            dependencies: input.dependencies.clone().unwrap_or_default(),
            visibility: input.visibility.unwrap_or(ToolVisibility::Searchable),
            // A newly created tool is ENABLED: the model asked for the capability, so it should see it
            // (`harness.ts toolCreate`).
            enabled: true,
            source_revision: source_revision.clone(),
            created_at: now_ms(),
            created_by_turn: created_by_turn.map(str::to_owned),
            extra: Map::new(),
        };
        state.created_tools.insert(name.to_owned(), tool);
        // A replace also clears any earlier explicit disable.
        state.disabled_tools.remove(name);

        Ok(SessionToolRegistrationResult {
            name: name.to_owned(),
            source_revision,
            created: !replacing,
            replaced: replacing,
            previous_revision,
            source_path,
            // No `checks` (`api.ts:3122`: the field is emitted only when the registration produced
            // results). This port registers a declaration without compiling a body, so there is no
            // kernel check to report - an empty list would print as absent anyway, and stating one
            // would invent results this build never ran.
            checks: None,
            budget: read::budget_of(state.created_tools.len()),
        })
    }

    /// `PATCH /sessions/:id/tools/:name` (`api.ts:1002-1050`): whole-field replacement, and the
    /// enable/disable switch.
    pub fn update_session_tool(
        state: &mut SessionState,
        store: &Store,
        name: &str,
        input: &crate::contract::UpdateSessionToolRequest,
    ) -> HarnessResult<SessionToolRegistrationResult> {
        let tool = state
            .created_tools
            .get(name)
            .cloned()
            .ok_or_else(|| unknown_tool(state, name))?;
        let busy = is_tool_busy(state, name);
        if busy && input.enabled == Some(false) {
            return Err(ApiError::conflict(format!(
                "Tool \"{name}\" is running; wait for it to finish before disabling it."
            )));
        }

        let mut next = tool.clone();
        let mut changed = false;
        if let Some(description) = input.description.as_deref() {
            let description = description.trim();
            if description.is_empty() {
                // The kernel's sentence, same as the create path (`creation.ts:128-130`).
                return Err(ApiError::bad_request(format!(
                    "Tool \"{name}\" requires a description."
                )));
            }
            next.description = description.to_owned();
            changed = true;
        }
        if let Some(tags) = input.tags.as_ref() {
            let tags: Vec<String> = tags
                .iter()
                .map(|tag| tag.trim().to_lowercase())
                .filter(|tag| !tag.is_empty())
                .collect();
            if tags.is_empty() {
                return Err(ApiError::bad_request(format!(
                    "Tool \"{name}\" requires at least one tag, so toolRequest can find it."
                )));
            }
            next.tags = tags;
            changed = true;
        }
        if input.parameters.is_some() {
            next.parameters = input.parameters.clone();
            changed = true;
        }
        if let Some(visibility) = input.visibility {
            if is_protected(store, state, name) && visibility == ToolVisibility::Searchable {
                return Err(ApiError::conflict(format!(
                    "Tool \"{name}\" is harness-owned and cannot become searchable."
                )));
            }
            next.visibility = visibility;
            changed = true;
        }
        if let Some(enabled) = input.enabled {
            if enabled {
                state.disabled_tools.remove(name);
            } else {
                state.disabled_tools.insert(name.to_owned());
            }
            next.enabled = enabled;
            changed = true;
        }
        for key in ["source", "module_text"] {
            let stated = if key == "source" {
                input.source.as_ref()
            } else {
                input.module_text.as_ref()
            };
            if let Some(value) = stated {
                if is_protected(store, state, name) {
                    return Err(ApiError::conflict(format!(
                        "Tool \"{name}\" is harness-owned; its source cannot be rewritten."
                    )));
                }
                if key == "source" {
                    next.source = Some(value.clone());
                } else {
                    next.module_text = Some(value.clone());
                }
                changed = true;
            }
        }
        if input.dependencies.is_some() {
            next.dependencies = input.dependencies.clone().unwrap_or_default();
            changed = true;
        }

        if !changed {
            return Err(ApiError::bad_request(
                "A patch must change at least one field.",
            ));
        }

        let previous_revision = Some(tool.source_revision.clone());
        let replacing_source = input.source.is_some()
            || input.module_text.is_some()
            || input.dependencies.is_some()
            || input.replace.unwrap_or(false);
        if replacing_source {
            state.created_tool_count += 1;
            next.source_revision = revision_stamp(state.created_tool_count, name);
        }
        let revision = next.source_revision.clone();
        state.created_tools.insert(name.to_owned(), next);

        Ok(SessionToolRegistrationResult {
            name: name.to_owned(),
            source_revision: revision,
            created: false,
            replaced: replacing_source,
            previous_revision,
            source_path: state
                .created_tools
                .get(name)
                .and_then(|stored| stored.source_path.clone()),
            // Same rule as the create: no compiled revision, so no kernel check results
            // (`api.ts:3122`).
            checks: None,
            budget: read::budget_of(state.created_tools.len()),
        })
    }

    /// `DELETE /sessions/:id/tools/:name` (`api.ts:1052-1090`): a created tool ENDS; an overridden
    /// catalogue name restores the inherited binding.
    pub fn delete_session_tool(
        state: &mut SessionState,
        store: &Store,
        name: &str,
    ) -> HarnessResult<DeleteSessionToolResult> {
        if is_tool_busy(state, name) {
            return Err(ApiError::conflict(format!(
                "Tool \"{name}\" is running; wait for it to finish before removing it."
            )));
        }
        if is_protected(store, state, name) {
            return Err(ApiError::conflict(format!(
                "Tool \"{name}\" is harness-owned and cannot be removed."
            )));
        }

        let removed_created = state.created_tools.remove(name).is_some();
        // `disabled_tools` is a SET, so `remove` answers with the bool directly: a session-local
        // disable that only overrode a catalogue binding goes away with the name.
        let had_override = state.disabled_tools.remove(name);
        if !removed_created && !had_override {
            return Err(unknown_tool(state, name));
        }

        Ok(DeleteSessionToolResult {
            deleted: true,
            tool_id: name.to_owned(),
            session_id: state.id().to_owned(),
            removed_created,
            // Stated `false` unconditionally, exactly as TS does: restoring an INHERITED baseline
            // needs a second copy of the definition this session overrode, and the registration
            // kernel keeps only the INSTALLED source, so there is nothing to hand back
            // (`api.ts:3296-3306`, the T27 disclosure). Reporting `true` would promise a restore
            // that never happens.
            restored_inherited: false,
        })
    }

    /// The context one tool call runs in (`api.ts:700-760` builds it the same way), handed to
    /// [`crate::ports::ToolRuntime::execute`] by the transport. Kept sync because the await happens
    /// outside the store lock â€” the one rule this module exists to protect.
    #[must_use]
    pub fn tool_ctx(
        &self,
        state: &SessionState,
        name: &str,
        arguments: Value,
        call_id: &str,
    ) -> ToolExecCtx {
        ToolExecCtx {
            name: name.to_owned(),
            arguments,
            call_id: call_id.to_owned(),
            session_id: Some(state.id().to_owned()),
            cwd: self.cwd(),
            signal: tokio_util::sync::CancellationToken::new(),
            policy: self.tool_policy(state),
            action_host: state.active.iter().find_map(|run| {
                run.action_sink
                    .clone()
                    .map(|sink| sink as std::sync::Arc<dyn crate::ports::ActionHost>)
            }),
            task_host: self
                .task_host
                .clone()
                .map(|host| host as std::sync::Arc<dyn crate::ports::TaskHost>),
        }
    }
}

/// Whether a run is executing one name right now (`api.ts:978-986`'s busy gate).
#[must_use]
pub fn is_tool_busy(state: &SessionState, name: &str) -> bool {
    state.active.iter().any(|run| {
        state.turns.get(&run.turn_id).is_some_and(|turn| {
            status_of_turn(&turn.record.status) == crate::contract::SessionTurnStatus::Running
        }) && state
            .turns
            .get(&run.turn_id)
            .is_some_and(|turn| turn_mentions_tool(&turn.record, name))
    })
}

/// Does one recorded turn reference a tool name (`sessions.ts:4231-4248`).
#[must_use]
fn turn_mentions_tool(turn: &crate::persistence::SessionTurn, name: &str) -> bool {
    turn.events.iter().any(|event| match event {
        crate::contract::HarnessEvent::ToolCall { call, .. } => call.name == name,
        crate::contract::HarnessEvent::ToolResult { name: tool, .. } => tool == name,
        _ => false,
    })
}

/// The toolbox name grammar: `tool` plus a PascalCase-ish tail, or a known catalogue name.
fn validate_tool_name(state: &SessionState, store: &Store, name: &str) -> HarnessResult<()> {
    if name.is_empty() {
        return Err(ApiError::bad_request("A tool needs a name."));
    }
    if name.len() > 64 {
        return Err(ApiError::bad_request(
            "A tool name cannot exceed 64 characters.",
        ));
    }
    let valid = name.len() > 4
        && name.starts_with("tool")
        && name.chars().next().is_some_and(char::is_lowercase)
        && name.chars().all(|glyph| glyph.is_ascii_alphanumeric());
    if !valid {
        return Err(ApiError::bad_request(format!(
            "A created tool name must start with \"tool\" and use letters and digits only, e.g. \"toolWordCount\". Got \"{name}\"."
        )));
    }
    if is_protected(store, state, name) {
        return Err(ApiError::conflict(format!(
            "Tool \"{name}\" is harness-owned and cannot be replaced."
        )));
    }
    // The catalogue is the authority on "already a tool": a created name must not shadow a catalogue
    // entry, because the toolbox rows would then state one name twice. The FULL table, listed or
    // not - an unlisted capability row is still a name the toolbox already owns.
    if store
        .runtime()
        .catalogue_summaries()
        .iter()
        .any(|summary| summary.name == name)
    {
        return Err(ApiError::conflict(format!(
            "Tool \"{name}\" is already a catalogue tool in this session."
        )));
    }
    Ok(())
}

/// The toolbox refusal text, shared by the four routes that name a tool (`api.ts:1015-1035`).
#[must_use]
pub fn unknown_tool(state: &SessionState, name: &str) -> ApiError {
    ApiError::not_found(format!(
        "Unknown tool \"{name}\" in session \"{}\".",
        state.id()
    ))
}

/// The registration a create or update answers with, as a hub payload (`api.ts` publishes
/// `session.updated` after a toolbox write; the budget is the part a client re-reads).
#[must_use]
pub fn budget_json(budget: &ResourceBudget) -> Value {
    json!({ "used": budget.used, "limit": budget.limit })
}

// Cross-reference for the next agent: the ledger lives on `SessionState::created_tools`, and the durable
// copy is `record.core.createdTools` (`SessionState::sync_core` in `mod.rs`). The busy gate reads
// `TurnState::record.events` through `status_of_turn` above.
