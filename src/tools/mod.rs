//! The tool runtime: the catalogue plus the half of it this build can actually execute.
//!
//! TypeScript origin: `src/harness.ts`'s tool surface (`tools`, `toolDeclarations`,
//! `launchToolCall`, `deliverTask`, `immediateResult`) and `src/tools/**` /
//! `src/server/tools/shared/**` for the bodies.
//!
//! THE THREE ANSWER CLASSES, AND WHY THEY ARE DIFFERENT
//! ---------------------------------------------------
//! [`ToolRuntime::execute`] answers every call, never panics, and classifies the answer
//! the way `harness.ts` does:
//!
//!   * **executed and succeeded** - `ok: true`, content budgeted to
//!     `MODEL_TOOL_RESULT_BUDGET_BYTES`, panels/media peeled from a branded envelope, and
//!     the measured span (`deliverTask`'s `completed` branch);
//!   * **executed and failed** - `ok: false` with `Tool "<name>" failed: <message>` and
//!     its measured span (`deliverTask`'s `failed` branch);
//!   * **never executed** - `ok: false`, the explanation as content, and `duration_ms: 0`
//!     so the turn engine can state "no span exists" instead of reporting a lie
//!     (`immediateResult` with `outcome: 'rejected'`: an unknown tool, malformed
//!     arguments, or a schema mismatch).
//!
//! A tool the catalogue lists but this build cannot run (the session-scoped registry,
//! record-store, delegation and thread controls) takes the SECOND class with a sentence
//! naming the missing capability, so the model gets an actionable answer and the run
//! keeps going. Every such name is listed in [`MISSING_CAPABILITY`] and in
//! `knowledge.md`.
//!
//! PATHS ARE UNSANDBOXED ON PURPOSE: the TS tools run with the server process's
//! permissions, and a workspace agent legitimately reads an absolute path outside the
//! session's `cwd`. What IS refused is what no filesystem accepts: an empty path and a
//! NUL byte.

pub mod catalogue;
pub mod command;
pub mod files;
pub mod result;
pub mod syntax;
pub mod text;
pub mod web;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use serde_json::{Value, json};

use crate::contract::{
    ToolAvailability, ToolAvailabilityState, ToolDecl, ToolDetail, ToolExecutionScope,
    ToolExecutionScopeKind, ToolSummary,
};
use crate::ports::{ToolExecCtx, ToolExecOutcome, ToolPolicy, ToolRuntime};

use catalogue::{ENTRIES, Entry, Group};
use result::ToolFailure;
use web::WebKeys;

/// `harnesses/core.ts:69` `CORE_TOOL_SESSION_HINT` - the one sentence a client prints for
/// a session-bound row.
pub const CORE_TOOL_SESSION_HINT: &str =
    "Runs inside a session harness: send a sessionId with the call.";
/// `harnesses/websearch.ts:33` `WEBSEARCH_ENVIRONMENT` (names, never values).
pub const WEBSEARCH_ENVIRONMENT: [&str; 3] = web::SEARCH_ENVIRONMENT;
/// `harnesses/websearch.ts:38` `WEBSEARCH_UNCONFIGURED_HINT`.
pub const WEBSEARCH_UNCONFIGURED_HINT: &str = web::WEBSEARCH_UNCONFIGURED_HINT;
/// `harnesses/webfetch.ts:40` `WEBFETCH_ENVIRONMENT`.
pub const WEBFETCH_ENVIRONMENT: [&str; 2] = ["EXA_API_KEY", "LINKUP_API_KEY"];
/// `harnesses/webfetch.ts:44` `WEBFETCH_UNCONFIGURED_HINT`.
pub const WEBFETCH_UNCONFIGURED_HINT: &str = "Web fetch is not enabled on this server. Set `webfetch: true` (optionally with EXA_API_KEY or LINKUP_API_KEY for a hosted provider) and restart the harness server; the native provider needs no key.";

/// The catalogue rows this build lists but cannot execute, each with the capability the
/// model is missing (`knowledge.md` "Catalogued-only tools").
///
/// They are ALL session-scoped: every one of them mutates or reads the live registry, a
/// per-session record store, the agent roster, or another conversation - none of which
/// the tool-runtime seam reaches. A created tool's body is the same story (`T3` registers
/// declarations, it cannot compile a body).
const MISSING_CAPABILITY: [(&str, &str); 15] = [
    (
        "toolRegistry",
        "compiling and registering a tool from editable source (the runtime compiler)",
    ),
    (
        "toolManage",
        "reading and changing a live tool registration (the runtime compiler)",
    ),
    (
        "toolCreate",
        "authoring a new tool body at runtime (the runtime compiler)",
    ),
    ("records", "the session record store"),
    ("profiles", "the session profile record store"),
    ("chapters", "the session chapter record store"),
    ("locations", "the session location record store"),
    (
        "agentManage",
        "the live agent roster this session runs against",
    ),
    (
        "agentResult",
        "the agent result registry a finished run publishes",
    ),
    ("delegate", "nested agent runs (the delegation runtime)"),
    ("threadFork", "conversation forks (the branch runtime)"),
    (
        "toolCancel",
        "the background tool scheduler a cancellation would release",
    ),
    (
        "threadSend",
        "cross-conversation messaging (the thread runtime)",
    ),
    (
        "threadRead",
        "another conversation's transcript (the thread runtime)",
    ),
    (
        "threadList",
        "the session's conversation roster (the thread runtime)",
    ),
];

/// The knobs a deployment decides, resolved when the registry is BUILT
/// (`harnesses/index.ts:164-218` builds the catalogue after `.env` is loaded, so a
/// registry constructed before the environment exists would advertise the wrong state).
#[derive(Clone, Debug)]
pub struct Options {
    /// The provider credentials `search`/`fetch` may use.
    pub web_keys: WebKeys,
    /// Whether this deployment asked for `fetch` at all (`webfetch: true` in TS). The
    /// native provider needs no key, so the shipped default is enabled.
    pub fetch_enabled: bool,
}

impl Default for Options {
    /// Reads the environment, exactly once, at construction.
    fn default() -> Self {
        Self {
            web_keys: WebKeys::from_environment(),
            fetch_enabled: true,
        }
    }
}

/// The catalogue and its executable half, shared by every session as one `Arc`.
pub struct ToolRegistry {
    options: Options,
    /// One pooled HTTP client for every network tool (TS builds a fresh `fetch` per
    /// call; pooling is the Rust equivalent of the keep-alive the TS runtime gets from
    /// Node's global agent).
    client: reqwest::Client,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new_inner(Options::default())
    }
}

impl ToolRegistry {
    /// A registry over the current environment (`createToolCatalogue()` with no explicit
    /// websearch/webfetch entry).
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::new_inner(Options::default()))
    }

    /// A registry with the credentials and the `fetch` switch named by the caller - the
    /// seam tests and a `.env`-loading host need, because "construct after the
    /// environment" is a rule a library cannot enforce by itself.
    #[must_use]
    pub fn with_options(options: Options) -> Arc<Self> {
        Arc::new(Self::new_inner(options))
    }

    /// The shared construction path: one HTTP client with a connect timeout, so a dead
    /// provider cannot park a turn indefinitely.
    fn new_inner(options: Options) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .unwrap_or_default();

        Self { options, client }
    }

    /// Whether the model is configured to summarize by delegation; carried so a later
    /// stage that adds an agent-runner seam can hand `textSummarize` its summarizer
    /// without changing this type's constructor.
    #[must_use]
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// The availability state of one row, or `None` when the row has nothing to add
    /// (`harnesses/core.ts:88`, `harnesses/{websearch,webfetch}.ts:54-70`).
    #[must_use]
    pub fn availability_of(&self, entry: &Entry) -> Option<ToolAvailability> {
        match entry.group {
            Group::Core => Some(ToolAvailability {
                state: ToolAvailabilityState::Available,
                environment: None,
                hint: Some(CORE_TOOL_SESSION_HINT.to_owned()),
            }),
            Group::WebSearch => Some(if self.options.web_keys.has_search_key() {
                ToolAvailability {
                    state: ToolAvailabilityState::Available,
                    environment: Some(
                        WEBSEARCH_ENVIRONMENT
                            .iter()
                            .map(|name| (*name).to_owned())
                            .collect(),
                    ),
                    hint: None,
                }
            } else {
                ToolAvailability {
                    state: ToolAvailabilityState::Unconfigured,
                    environment: Some(
                        WEBSEARCH_ENVIRONMENT
                            .iter()
                            .map(|name| (*name).to_owned())
                            .collect(),
                    ),
                    hint: Some(WEBSEARCH_UNCONFIGURED_HINT.to_owned()),
                }
            }),
            Group::WebFetch => Some(if self.options.fetch_enabled {
                ToolAvailability {
                    state: ToolAvailabilityState::Available,
                    environment: Some(
                        WEBFETCH_ENVIRONMENT
                            .iter()
                            .map(|name| (*name).to_owned())
                            .collect(),
                    ),
                    hint: None,
                }
            } else {
                ToolAvailability {
                    state: ToolAvailabilityState::Unconfigured,
                    environment: Some(
                        WEBFETCH_ENVIRONMENT
                            .iter()
                            .map(|name| (*name).to_owned())
                            .collect(),
                    ),
                    hint: Some(WEBFETCH_UNCONFIGURED_HINT.to_owned()),
                }
            }),
            // A preset or an app tool with nothing to add omits `availability` rather than
            // repeating "available" (`api.test.ts:1155-1162`).
            Group::Package | Group::Harness => None,
        }
    }

    /// The wire row of one catalogue entry (`toolSummaryOf`, `api-reads.ts:1023-1035`).
    #[must_use]
    pub fn summary_of(&self, entry: &Entry) -> ToolSummary {
        ToolSummary {
            name: entry.name.to_owned(),
            group: entry.group.display().to_owned(),
            description: entry.description.to_owned(),
            tags: entry.tags.iter().map(|tag| (*tag).to_owned()).collect(),
            visibility: entry.visibility,
            availability: self.availability_of(entry),
            session_bound: entry.session_bound,
        }
    }

    /// Every name this session may call: the always-declared core set, the two
    /// environment rows this deployment configured, `delegate` when the harness has
    /// subagents, and whatever the policy enables, declares, or created
    /// (`src/harness.ts`'s `declared()`, projected through [`ToolPolicy`]).
    #[must_use]
    pub fn declared_names(&self, policy: &ToolPolicy) -> Vec<String> {
        ENTRIES
            .iter()
            .filter(|entry| self.is_declared(entry, policy))
            .map(|entry| entry.name.to_owned())
            .collect()
    }

    /// The one rule, stated per row, for whether a model is offered this tool.
    ///
    /// A row this build CANNOT execute is never offered, whatever the policy enabled
    /// (`knowledge.md` "Catalogued-only tools"): TS's `declared()` comes from the live registry, and
    /// every name in [`MISSING_CAPABILITY`] is absent from this port's registry, so offering it
    /// would send the model a tool whose call can only fail. The same filter is what keeps
    /// `toolRequest`/`toolList` from surfacing them - both answer from this list.
    #[must_use]
    pub fn is_declared(&self, entry: &Entry, policy: &ToolPolicy) -> bool {
        if MISSING_CAPABILITY
            .iter()
            .any(|(name, _)| *name == entry.name)
        {
            return false;
        }

        // The bootstrap pair and the session plan ride EVERY request
        // (`toolbox.ts:188-190` `CORE_TOOL_NAMES`), as do the core file tools
        // (`toolbox.ts:197-201` `CORE_PRESET_TOOL_NAMES`).
        if crate::registry::presets::CORE_TOOL_NAMES.contains(&entry.name)
            || crate::registry::presets::CORE_PRESET_TOOL_NAMES.contains(&entry.name)
        {
            return true;
        }

        // `delegate` is declared only where the harness has subagents (`toolbox.ts:228-233`).
        if entry.name == crate::registry::presets::DELEGATE_TOOL_NAME {
            return !policy.subagents.is_empty();
        }

        // `search` needs a credential and `fetch` needs the deployment opt-in
        // (`toolbox.ts:208` `CORE_ENVIRONMENT_TOOL_NAMES`: a keyless host is offline-safe).
        if entry.name == "search" {
            return self.options.web_keys.has_search_key();
        }

        if entry.name == "fetch" {
            return self.options.fetch_enabled;
        }

        policy
            .enabled
            .iter()
            .chain(policy.declared.iter())
            .chain(policy.created.iter())
            .any(|name| name == entry.name)
    }
}

impl ToolRuntime for ToolRegistry {
    /// The declarations a model is offered, in catalogue order (never policy order, so a
    /// session that enabled the same two tools in the opposite order sends the same
    /// request bytes).
    fn declarations(&self, policy: &ToolPolicy) -> Vec<ToolDecl> {
        let declared = self.declared_names(policy);

        ENTRIES
            .iter()
            .filter(|entry| declared.iter().any(|name| name == entry.name))
            .map(|entry| ToolDecl {
                name: entry.name.to_owned(),
                description: entry.description.to_owned(),
                parameters: parse_schema(entry),
            })
            .collect()
    }

    /// Every row the SERVER lists, in the order `GET /tools` emits them. The four
    /// `listed: false` runtime rows are withheld exactly as `tools/descriptors.ts:290-328`
    /// withholds them from `RUNTIME_DESCRIPTORS`; they stay resolvable through
    /// [`catalogue::find`] for the session toolbox and the execution dispatch.
    fn summaries(&self) -> Vec<ToolSummary> {
        catalogue::listed_entries()
            .map(|entry| self.summary_of(entry))
            .collect()
    }

    /// Every row of the generated table, which is what a SESSION toolbox walks: TS's
    /// `sessionToolRowsOf` reads the live harness registry (`api-reads.ts:831-836`), and that
    /// registry holds the four `listed: false` capability rows.
    fn catalogue_summaries(&self) -> Vec<ToolSummary> {
        ENTRIES.iter().map(|entry| self.summary_of(entry)).collect()
    }

    /// One row plus the two fields only the identified read carries: the argument schema
    /// and the execution gate (`harness-api.ts:654-665`).
    ///
    /// An unlisted row is `None` here, exactly like TS's `getToolOne`, which looks the name up in
    /// `coreEntries` (the listed runtime set) `?? catalogue.find(name)` and therefore 404s
    /// `threadRead` (`api.ts:1847`).
    fn detail(&self, name: &str) -> Option<ToolDetail> {
        if !catalogue::is_listed(name) {
            return None;
        }

        let entry = catalogue::find(name)?;

        Some(ToolDetail {
            summary: self.summary_of(entry),
            parameters: parse_schema(entry),
            execution: ToolExecutionScope {
                scope: if entry.session_bound {
                    ToolExecutionScopeKind::SessionRequired
                } else {
                    ToolExecutionScopeKind::Global
                },
                // Manual execution is the documented surface for every live entry
                // (`api-reads.ts:1206-1211`).
                manual: true,
            },
        })
    }

    /// Runs one call, timed from entry to exit.
    fn execute(&self, ctx: ToolExecCtx) -> BoxFuture<'static, ToolExecOutcome> {
        // Only owned values cross the `await`: the registry itself is shared, so the
        // future must not borrow it. The declared set is computed HERE, while `self` is
        // still in scope, because `toolList`/`toolRequest` answer from it.
        let client = self.client.clone();
        let options = self.options.clone();
        let declared = self.declared_names(&ctx.policy);

        Box::pin(async move {
            let started = Instant::now();
            let name = ctx.name.clone();
            let outcome = dispatch(&client, &options, &declared, ctx).await;

            match outcome {
                Ok(value) => result::completed_outcome(value, started),
                Err(ToolFailure::Rejected(message)) => result::rejected_outcome(&message),
                Err(ToolFailure::Failed(message)) => {
                    result::failed_outcome(&name, &message, started)
                }
            }
        })
    }
}

/// Parses one entry's schema text. A generated row that failed to parse would be a build
/// bug, and the panic message names the tool so `cargo test` says which row to fix.
fn parse_schema(entry: &Entry) -> Option<Value> {
    Some(
        serde_json::from_str(entry.parameters_json).unwrap_or_else(|error| {
            panic!(
                "the generated schema for \"{}\" is not valid JSON: {error}",
                entry.name
            )
        }),
    )
}

/// The one call path: validate, resolve, dispatch, and classify every failure.
async fn dispatch(
    client: &reqwest::Client,
    options: &Options,
    declared: &[String],
    ctx: ToolExecCtx,
) -> Result<Value, ToolFailure> {
    let rejected = |message: String| Err(ToolFailure::Rejected(message));

    // ARGUMENTS: the runtime hands over parsed JSON; a non-object is a shape failure the
    // model can fix, and the schema is the thing to point at.
    if !(ctx.arguments.is_object() || ctx.arguments.is_null()) {
        return rejected(format!(
            "Tool \"{}\" received arguments that are not a JSON object ({}).",
            ctx.name,
            result::type_of(&ctx.arguments)
        ));
    }

    let entry = catalogue::find(&ctx.name)
        .ok_or_else(|| ToolFailure::Rejected(unknown_tool_message(&ctx.name, &ctx.policy)))?;

    // A listed capability with no implementation in this build. This is checked BEFORE the
    // argument shape: a missing capability is not something the model can fix by re-sending
    // better arguments, and the answer must name the capability every time.
    if let Some(capability) = MISSING_CAPABILITY
        .iter()
        .find(|(name, _)| *name == ctx.name)
        .map(|(_, capability)| *capability)
    {
        return Err(ToolFailure::Failed(format!(
            "this server does not support {capability}, so \"{name}\" cannot run here. The capability is on the port list; use the tools that are executable to make progress.",
            name = ctx.name
        )));
    }

    // A top-level required-property check, which is the schema rule that actually fires
    // in practice (a full JSON-Schema validator is not a dependency of this crate).
    if let Some(missing) = missing_required(entry, &ctx.arguments) {
        return rejected(format!(
            "Tool \"{}\" received arguments that do not match its schema: missing required property \"{missing}\".",
            ctx.name
        ));
    }

    let cwd: PathBuf = ctx.cwd.clone();
    let arguments = ctx.arguments.clone();
    let signal = ctx.signal.clone();

    match ctx.name.as_str() {
        // The file tools.
        "read" => files::read(&arguments, &cwd).map_err(ToolFailure::Failed),
        "write" => files::write(&arguments, &cwd).map_err(ToolFailure::Failed),
        "edit" => files::edit(&arguments, &cwd).map_err(ToolFailure::Failed),
        "files" => files::file_list(&arguments, &cwd).map_err(ToolFailure::Failed),
        "glob" => files::glob(&arguments, &cwd).map_err(ToolFailure::Failed),
        "grep" => files::grep(&arguments, &cwd).map_err(ToolFailure::Failed),

        // The command tools, run through the platform shell.
        "shell" => command::shell(&arguments, &cwd, &signal)
            .await
            .map_err(ToolFailure::Failed),
        "gitDiff" => command::git_diff(&arguments, &cwd, &signal)
            .await
            .map_err(ToolFailure::Failed),

        // The Python half runs a child process, so it takes the turn's cancellation token like the
        // command and network tools do; the other halves never yield.
        "syntax" => syntax::check(&arguments, &signal)
            .await
            .map_err(ToolFailure::Failed),

        // The app tools.
        "timeNow" => text::time_now(&arguments).map_err(ToolFailure::Failed),
        "textCount" => text::text_count(&arguments).map_err(ToolFailure::Failed),
        "diceRoll" => text::dice_roll(&arguments).map_err(ToolFailure::Failed),
        "noteSearch" => text::note_search(&arguments).map_err(ToolFailure::Failed),
        "textSummarize" => text::text_summarize(&arguments).map_err(ToolFailure::Failed),

        // The discovery pair, answered from the catalogue this registry owns.
        "toolList" => Ok(tool_list(declared)),
        "toolRequest" => Ok(tool_request(declared, &arguments)),

        // The network tools.
        "fetch" => {
            if options.fetch_enabled {
                web::fetch(client, &arguments, &options.web_keys, &signal)
                    .await
                    .map_err(ToolFailure::Failed)
            } else {
                Err(ToolFailure::Failed(WEBFETCH_UNCONFIGURED_HINT.to_owned()))
            }
        }
        "search" => web::search(client, &arguments, &options.web_keys, &signal)
            .await
            .map_err(ToolFailure::Failed),
        "download" => web::download(client, &arguments, &cwd, &signal)
            .await
            .map_err(ToolFailure::Failed),

        // The session-shared todo list: a pass-through of the host's own JSON answer.
        "tasks" => {
            let Some(host) = ctx.task_host.as_ref() else {
                return Err(ToolFailure::Failed(
                    "task needs this session's task board, and this run has none.".to_owned(),
                ));
            };

            host.execute(arguments).map_err(ToolFailure::Failed)
        }

        // The owner question: the host records the event and answers it.
        "actionRequest" => {
            let Some(host) = ctx.action_host.as_ref() else {
                return Err(ToolFailure::Failed(
                    "actionRequest needs an owner-interaction host, and this run has none."
                        .to_owned(),
                ));
            };

            let request = validated_action_request(&arguments)?;

            host.request(request).await.map_err(ToolFailure::Failed)
        }

        // Every other catalogued name was handled above or is a catalogue/runtime gap.
        other => Err(ToolFailure::Failed(format!(
            "tool \"{other}\" is catalogued but has no implementation in this build."
        ))),
    }
}

/// `launchToolCall`'s unknown-tool sentence (`harness.ts:5170-5177`), with the declared
/// set of THIS call rather than the whole catalogue.
fn unknown_tool_message(name: &str, policy: &ToolPolicy) -> String {
    let mut available: Vec<&str> = policy
        .declared
        .iter()
        .map(String::as_str)
        .chain(policy.enabled.iter().map(String::as_str))
        .chain(policy.created.iter().map(String::as_str))
        .collect();

    available.sort_unstable();
    available.dedup();

    let list = if available.is_empty() {
        ENTRIES
            .iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        available.join(", ")
    };

    format!(
        "Tool \"{name}\" is not registered. Available tools: {list}. Call toolRequest with capability tags to search the catalogue."
    )
}

/// The only schema rule applied before a body runs: the documented required properties.
///
/// A full JSON-Schema validator is not a dependency here, and the TS failure mode that
/// matters to a model is a missing property (AJV reports exactly that). Everything else is
/// the body's own validation, which carries the entry's precise wording.
fn missing_required(entry: &Entry, arguments: &Value) -> Option<String> {
    let Ok(schema) = serde_json::from_str::<Value>(entry.parameters_json) else {
        return None;
    };

    let required = schema.get("required")?.as_array()?;

    for key in required.iter().filter_map(Value::as_str) {
        match arguments.get(key) {
            None | Some(Value::Null) => return Some(key.to_owned()),
            Some(_) => {}
        }
    }

    None
}

/// Builds the owner question the host records (`core/action.ts`'s bounds, mirrored so a
/// malformed directive from the model becomes a readable tool result instead of a parked
/// question).
///
/// `requestId` is deliberately ABSENT: the host mints its own and replaces a submitted one
/// (`knowledge.md`, "the action host mints its own requestId").
fn validated_action_request(arguments: &Value) -> Result<Value, ToolFailure> {
    let question = arguments
        .get("question")
        .and_then(Value::as_str)
        .ok_or_else(|| ToolFailure::Rejected(
            "actionRequest received arguments that do not match its schema: missing required property \"question\".".to_owned(),
        ))?;

    if question.trim().is_empty() || question.encode_utf16().count() > 4_000 {
        return Err(ToolFailure::Rejected(
            "actionRequest question must be a non-blank string of at most 4000 characters."
                .to_owned(),
        ));
    }

    let recommended = arguments
        .get("recommended")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ToolFailure::Rejected(
                "actionRequest recommended must name one of the supplied option ids.".to_owned(),
            )
        })?;

    let options = arguments
        .get("options")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    if options.len() < 2 || options.len() > 8 {
        return Err(ToolFailure::Rejected(format!(
            "actionRequest options must hold between 2 and 8 entries; received {}.",
            options.len()
        )));
    }

    let mut ids: Vec<&str> = Vec::with_capacity(options.len());

    for option in &options {
        let Some(id) = option.get("id").and_then(Value::as_str) else {
            return Err(ToolFailure::Rejected(
                "actionRequest options[].id must be a string of letters, digits, \"_\" or \"-\"."
                    .to_owned(),
            ));
        };

        if option
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or("")
            .is_empty()
        {
            return Err(ToolFailure::Rejected(format!(
                "actionRequest options[].label must be a non-empty string for \"{id}\"."
            )));
        }

        ids.push(id);
    }

    if !ids.contains(&recommended) {
        return Err(ToolFailure::Rejected(format!(
            "actionRequest recommended \"{recommended}\" is not one of the supplied option ids."
        )));
    }

    Ok(json!({
        "question": question,
        "recommended": recommended,
        "options": options,
        "allowCustom": true,
    }))
}

/// `toolList` (`harness.ts:1462` `listToolsFor`): the session's live toolbox as
/// `{name, description}` entries - metadata only, never a schema.
fn tool_list(declared: &[String]) -> Value {
    Value::Array(
        declared
            .iter()
            .filter_map(|name| catalogue::find(name))
            .map(|entry| json!({ "name": entry.name, "description": entry.description }))
            .collect(),
    )
}

/// `toolRequest` (`core/discovery.ts` `searchTools` + `selectTools`), reported in the
/// document the model was told about: the union of `names`, `regex` and `tags`, the
/// enabled names, the exact match count, and the honest `remaining`.
///
/// The searchable pool is the catalogue MINUS the always-declared core set, which is what
/// "core tools are reported, never enabled" means (`discovery.ts:42-52`).
fn tool_request(declared: &[String], arguments: &Value) -> Value {
    let names: Vec<String> = result::string_list(arguments, "names").unwrap_or_default();
    let tags: Vec<String> = result::string_list(arguments, "tags").unwrap_or_default();
    let pattern = result::optional_text(arguments, "regex");
    let limit = result::bounded_integer(arguments, "limit", 25, 1, 100).unwrap_or(25);

    // `/source/flags` is an accepted spelling (`toolRequest`'s schema says "bare source or
    // /source/flags"), so strip the delimiters before compiling.
    let regex = pattern.as_deref().and_then(compiled_regex);
    let lowered_tags: Vec<String> = tags.iter().map(|tag| tag.to_lowercase()).collect();

    // TS ranks the UNION by each row's strongest criterion: an exact requested name (in
    // the ORDER THE CALLER LISTED them), then a name-regex hit, then a tag-only hit
    // (`discovery.ts:373-378`). A row that matches several criteria is ranked once, by its
    // strongest group, and never repeated.
    struct Ranked<'a> {
        entry: &'a Entry,
        /// 2 exact name, 1 regex name, 0 tag-only (`discovery.ts:429` `RankedSelection`).
        group: u8,
        /// The caller's position for an exact name, and the maximum otherwise, so the
        /// exact-name group sorts by request order without a second scan.
        name_order: usize,
        /// How many requested tags this row answers: the tiebreak inside the tag group.
        ///
        /// TS also has `scoreTool`'s fuzzy point ladder across name/tag/description
        /// (`discovery.ts:174`); this port matches tags case-insensitively and keeps the
        /// breadth count, so a row answering three requested tags outranks one answering
        /// one, and the omitted ladder is stated in `knowledge.md`.
        tag_hits: usize,
    }

    let mut matches: Vec<Ranked<'_>> = Vec::new();

    for entry in ENTRIES.iter() {
        let name_order = names.iter().position(|name| name == entry.name);
        let by_regex = regex
            .as_ref()
            .is_some_and(|regex| regex.is_match(entry.name));
        let tag_hits = lowered_tags
            .iter()
            .filter(|tag| {
                entry
                    .tags
                    .iter()
                    .any(|candidate| &candidate.to_lowercase() == *tag)
            })
            .count();

        let group = if name_order.is_some() {
            2
        } else if by_regex {
            1
        } else if tag_hits > 0 {
            0
        } else {
            continue;
        };

        matches.push(Ranked {
            entry,
            group,
            name_order: name_order.unwrap_or(usize::MAX),
            tag_hits,
        });
    }

    matches.sort_by(|left, right| {
        right
            .group
            .cmp(&left.group)
            .then(left.name_order.cmp(&right.name_order))
            .then(right.tag_hits.cmp(&left.tag_hits))
            .then_with(|| left.entry.name.cmp(right.entry.name))
    });

    // A row the run ALREADY has is `alreadyAvailable`, not `enabled`, and it never consumes
    // a slot in the page: TS keeps declared core tools out of the ranked pool entirely
    // (`discovery.ts:275-279` and the `excludeNames` note at `:402-411`), so slicing BEFORE
    // that split would let a core row shrink the answer to fewer tools than the caller asked
    // for. Both halves keep the ranking above.
    let (available, selectable): (Vec<Ranked<'_>>, Vec<Ranked<'_>>) = matches
        .into_iter()
        .partition(|ranked| declared.iter().any(|name| name == ranked.entry.name));

    let matched = available.len() + selectable.len();
    let page: Vec<&Entry> = selectable
        .into_iter()
        .take(usize::try_from(limit).unwrap_or(25))
        .map(|ranked| ranked.entry)
        .collect();

    let metadata_of = |entry: &&Entry| {
        json!({
            "name": entry.name,
            "description": entry.description,
            "tags": entry.tags,
        })
    };

    let enabled: Vec<String> = page.iter().map(|entry| entry.name.to_owned()).collect();
    let tools: Vec<Value> = page.iter().map(metadata_of).collect();
    let already_available: Vec<Value> = available
        .iter()
        .map(|ranked| &ranked.entry)
        .map(metadata_of)
        .collect();

    // `remaining` counts only further SELECTABLE matches, so it reaches 0 exactly when the
    // pool is exhausted and an already-available row never inflates it.
    let remaining = matched.saturating_sub(page.len() + available.len());

    let mut result = json!({
        "enabled": enabled,
        "matched": matched,
        "tools": tools,
        "remaining": remaining,
        "availableTags": available_tags(),
    });

    if remaining > 0 {
        result["truncated"] = json!(true);
    }

    if !already_available.is_empty() {
        result["alreadyAvailable"] = json!(already_available);
    }

    result
}

/// The tag vocabulary a discovery answer advertises (`discovery.ts:217` `vocabulary`).
fn available_tags() -> Vec<String> {
    let mut tags: Vec<String> = ENTRIES
        .iter()
        .flat_map(|entry| entry.tags.iter().map(|tag| (*tag).to_owned()))
        .collect();

    tags.sort();
    tags.dedup();
    tags
}

/// Accepts `source` or `/source/flags`, and answers `None` for a source the regex crate
/// refuses (a JS-only construct), which discovery reports as "no regex match" rather than
/// failing the whole call.
fn compiled_regex(pattern: &str) -> Option<regex::Regex> {
    let body = if pattern.len() > 2 && pattern.starts_with('/') {
        pattern[1..pattern.rfind('/')?].to_owned()
    } else {
        pattern.to_owned()
    };

    regex::Regex::new(&body).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalogue_is_the_documented_shape_in_the_documented_order() {
        let registry = ToolRegistry::with_options(Options {
            web_keys: WebKeys::default(),
            fetch_enabled: true,
        });
        let summaries = registry.summaries();

        // The LISTED set is the TS page: 36 rows minus the four the manifest marks
        // `listed: false` (`tool-manifest.ts:1002, 1046, 1692, 1811`).
        assert_eq!(summaries.len(), ENTRIES.len() - UNLISTED_EXPECTED.len());
        assert_eq!(ENTRIES.len(), 36);
        assert_eq!(summaries[0].name, "toolRegistry");
        assert_eq!(summaries[0].group, "Core tools");

        // Wave 1: the manifest's renamed rows are the ONLY spelling this catalogue carries, and the
        // record-store row (`records`, `tool-manifest.ts` between `toolCreate` and `toolPatch`) is
        // listed like every other read-side row.
        for name in [
            "files",
            "tasks",
            "profiles",
            "chapters",
            "locations",
            "records",
        ] {
            assert!(
                summaries.iter().any(|row| row.name == name),
                "`{name}` is a current manifest row name and must be listed by GET /tools"
            );
        }

        for stale in [
            "fileList",
            "task",
            "profileManage",
            "chapterManage",
            "locationManage",
        ] {
            assert!(
                !summaries.iter().any(|row| row.name == stale),
                "`{stale}` was renamed in the manifest; a row still calling it that would advertise \
                 a name no preset, context pack or client sends anymore"
            );
        }
        assert_eq!(
            summaries[0].availability,
            Some(ToolAvailability {
                state: ToolAvailabilityState::Available,
                environment: None,
                hint: Some(CORE_TOOL_SESSION_HINT.to_owned()),
            })
        );

        let rows: Vec<(String, String)> = summaries
            .iter()
            .map(|row| (row.name.clone(), row.group.clone()))
            .collect();

        let index = |name: &str| rows.iter().position(|(row, _)| row == name).expect("a row");

        // The group boundaries the TypeScript route states (`api.test.ts:1134-1150`,
        // `server.test.ts:219`, `websearch.test.ts:112`). `threadFork` is the last LISTED runtime
        // row; `threadList` is no longer in this page at all (asserted below).
        assert!(index("threadFork") < index("syntax"));
        assert!(index("download") < index("timeNow"));
        assert!(index("textSummarize") < index("search"));
        assert_eq!(rows[index("search")].1.as_str(), "Web search");
        assert_eq!(rows[index("fetch")].1.as_str(), "Web fetch");
        assert_eq!(rows[index("read")].1, "Package presets");
        assert_eq!(rows[index("timeNow")].1, "Harness tools");

        // Names are unique, and every row the SHARED toolbox advertises is present.
        let mut names: Vec<&str> = summaries.iter().map(|row| row.name.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), summaries.len());

        for name in crate::registry::presets::SHARED_TOOL_NAMES {
            assert!(names.contains(&name), "{name} must stay catalogued");
        }

        // T7b: the four unlisted runtime rows are absent from the GLOBAL listing and its identified
        // read, while the session toolbox and the execution dispatch still resolve them.
        for name in UNLISTED_EXPECTED {
            assert!(
                !names.contains(&name),
                "{name} is `listed: false` in the manifest and must not appear in GET /tools"
            );
            assert!(
                registry.detail(name).is_none(),
                "{name} must not resolve on GET /tools/{{name}} either"
            );
            assert!(
                registry
                    .catalogue_summaries()
                    .iter()
                    .any(|row| row.name == *name),
                "{name} stays in the session toolbox, which reads the live registry"
            );
        }

        assert_eq!(registry.catalogue_summaries().len(), ENTRIES.len());
    }

    /// The manifest's `listed: false` runtime rows, restated here so the test names the same set
    /// `catalogue::UNLISTED` does (a drift between the two is a catalogue bug worth failing on).
    const UNLISTED_EXPECTED: [&str; 4] = ["toolCancel", "threadSend", "threadRead", "threadList"];

    #[test]
    fn search_states_its_environment_and_fetch_omits_nothing() {
        let keyless = ToolRegistry::with_options(Options {
            web_keys: WebKeys::default(),
            fetch_enabled: true,
        });
        let rows = keyless
            .summaries()
            .into_iter()
            .map(|row| (row.name, row.availability))
            .collect::<Vec<_>>();
        let search = rows
            .iter()
            .find(|(name, _)| name == "search")
            .expect("a search row")
            .1
            .clone()
            .expect("search always states availability");

        assert_eq!(search.state, ToolAvailabilityState::Unconfigured);
        assert_eq!(
            search.environment,
            Some(vec![
                "EXA_API_KEY".to_owned(),
                "TAVILY_API_KEY".to_owned(),
                "LINKUP_API_KEY".to_owned()
            ])
        );
        assert_eq!(search.hint.as_deref(), Some(WEBSEARCH_UNCONFIGURED_HINT));

        let fetch = rows
            .iter()
            .find(|(name, _)| name == "fetch")
            .expect("a fetch row")
            .1
            .clone()
            .expect("fetch always states availability");

        assert_eq!(fetch.state, ToolAvailabilityState::Available);
        assert_eq!(
            fetch.environment,
            Some(vec!["EXA_API_KEY".to_owned(), "LINKUP_API_KEY".to_owned()])
        );

        let with_key = ToolRegistry::with_options(Options {
            web_keys: WebKeys {
                exa: Some("secret".to_owned()),
                tavily: None,
                linkup: None,
                // The published provider endpoints; only the keys are under test here.
                endpoint_base: None,
            },
            fetch_enabled: true,
        });

        assert_eq!(
            with_key
                .summaries()
                .iter()
                .find(|row| row.name == "search")
                .expect("a search row")
                .availability
                .as_ref()
                .expect("availability")
                .state,
            ToolAvailabilityState::Available
        );

        // The credential never rides the row.
        let serialized = serde_json::to_string(&with_key.summaries()).expect("rows serialize");
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn the_detail_read_adds_the_schema_and_the_execution_gate() {
        let registry = ToolRegistry::new();
        let detail = registry.detail("timeNow").expect("a catalogued row");

        assert_eq!(detail.summary.group, "Harness tools");
        assert_eq!(detail.execution.scope, ToolExecutionScopeKind::Global);
        assert!(detail.execution.manual);

        let schema = detail.parameters.expect("a schema");

        assert_eq!(schema["properties"]["timezone"]["maxLength"], json!(64));
        assert_eq!(schema["required"], json!(["timezone"]));

        let core = registry.detail("tasks").expect("a catalogued row");
        assert_eq!(
            core.execution.scope,
            ToolExecutionScopeKind::SessionRequired
        );
        assert!(registry.detail("nope").is_none());
    }

    #[test]
    fn declarations_are_catalogue_ordered_and_hold_the_session_set() {
        let registry = ToolRegistry::new();
        let policy = ToolPolicy {
            enabled: vec!["shell".to_owned()],
            declared: vec!["read".to_owned(), "grep".to_owned()],
            created: vec![],
            subagents: vec!["summarizer".to_owned()],
        };

        let names: Vec<String> = registry
            .declarations(&policy)
            .iter()
            .map(|decl| decl.name.clone())
            .collect();

        assert!(names.contains(&"shell".to_owned()));
        assert!(names.contains(&"read".to_owned()));
        assert!(names.contains(&"toolRequest".to_owned()));
        assert!(names.contains(&"tasks".to_owned()));

        // T7b: `delegate` is catalogued-only in this build (no nested-run runtime), so it is NOT
        // offered even when the harness has subagents. The same rule covers every name in
        // `MISSING_CAPABILITY`; an explicitly enabled one is the case that used to leak through
        // `policy.enabled`, and it is what `toolRequest` used to be able to surface.
        assert!(
            !names.iter().any(|name| name == "delegate"),
            "a catalogued-only name is never declared"
        );

        let enabled_only = ToolPolicy {
            enabled: vec!["profiles".to_owned(), "threadRead".to_owned()],
            ..ToolPolicy::default()
        };
        let offered: Vec<String> = registry
            .declarations(&enabled_only)
            .iter()
            .map(|decl| decl.name.clone())
            .collect();

        assert!(
            !offered
                .iter()
                .any(|name| name == "profiles" || name == "threadRead"),
            "an enabled-but-unexecutable tool is offered to neither the model nor toolRequest: {offered:?}"
        );
        // The genuinely executable core pair is still offered, so the filter removed the two
        // names and not the whole list.
        assert!(offered.iter().any(|name| name == "toolRequest"));

        // Catalogue order, not policy order.
        let position = |name: &str| names.iter().position(|row| row == name).expect("declared");
        assert!(position("read") < position("shell"));
        assert!(position("toolRequest") < position("tasks"));

        // Every declaration carries its schema.
        assert!(
            registry
                .declarations(&policy)
                .iter()
                .all(|decl| decl.parameters.is_some())
        );

        // Without subagents, `delegate` is not offered.
        let alone = registry.declarations(&ToolPolicy::default());
        assert!(
            !alone
                .iter()
                .any(|decl| decl.name == "delegate" || decl.name == "files")
        );
        assert!(alone.iter().any(|decl| decl.name == "toolRequest"));
    }

    #[test]
    fn an_unknown_tool_is_rejected_with_the_discovery_pointer() {
        let message = unknown_tool_message("nope", &ToolPolicy::default());

        assert!(message.starts_with("Tool \"nope\" is not registered. Available tools: "));
        assert!(message.contains("Call toolRequest with capability tags to search the catalogue."));
        assert!(message.contains("read"));

        let with_policy = unknown_tool_message(
            "nope",
            &ToolPolicy {
                declared: vec!["read".to_owned()],
                ..ToolPolicy::default()
            },
        );

        assert!(with_policy.contains("Available tools: read."));
    }
}
