//! Slash commands: the parser, the built-in registry and the dispatcher.
//!
//! Ported from `underload/harness/src/core/command.ts` (parser + registry + `executeCommand`) and
//! `core/command-builtins.ts` (the five shipped commands). The store owns this because a command never
//! reaches the provider: it settles INSIDE the turn that carried it, which is what makes its result a
//! `tool` transcript entry instead of a model message.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::contract::{HarnessMessage, HarnessToolCall, HarnessToolOutcome};
use crate::store::read::{TURN_PREVIEW_CHARS, preview};
use crate::store::{SessionState, Store, commands, now_ms, tools};

/// The `tool` name a command result rides on (`command.ts:22 COMMAND_TOOL_NAME`).
pub const COMMAND_TOOL_NAME: &str = "command";

/// What one call may hand back to the model (`core/result-budget.ts MODEL_TOOL_RESULT_BUDGET_BYTES`).
pub const COMMAND_RESULT_BUDGET_BYTES: usize = crate::contract::MODEL_TOOL_RESULT_BUDGET_BYTES;

/// The marker that replaces the tail of an over-budget result.
pub const RESULT_CAP_MARKER: &str = "[... trimmed ...]";

/// One registered command (`command.ts:34 CommandDescriptor`).
#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub name: String,
    pub description: String,
    pub usage: Option<String>,
}

/// The parse of one submitted prompt (`command.ts:112 parseCommandCall`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedCommand {
    /// Not a command at all — a normal task.
    None,
    /// A bare `/name`, with no arguments.
    Help { name: String },
    /// `/name rest`, where `rest` may be empty after trimming.
    Call { name: String, args: String },
}

/// `command.ts:104-126`: a command is a prompt that starts with `/`, whose first whitespace-delimited
/// token matches `[a-z][a-z0-9_-]*`, and whose remainder is the argument string.
#[must_use]
pub fn parse_command_line(input: &str) -> ParsedCommand {
    let trimmed = input.trim();
    let Some(rest) = trimmed.strip_prefix('/') else {
        return ParsedCommand::None;
    };
    let Some((name, args)) = rest.split_once(char::is_whitespace) else {
        // A bare token still has to look like a command name; `/Hello` is prose.
        return if is_command_name(rest) {
            ParsedCommand::Help {
                name: rest.to_owned(),
            }
        } else {
            ParsedCommand::None
        };
    };
    let name = name.trim();
    if !is_command_name(name) {
        return ParsedCommand::None;
    }
    ParsedCommand::Call {
        name: name.to_owned(),
        args: args.trim().to_owned(),
    }
}

/// The name grammar `command.ts:109` uses (`/^[a-z][a-z0-9_-]*$/`).
#[must_use]
fn is_command_name(value: &str) -> bool {
    let Some(first) = value.chars().next() else {
        return false;
    };
    value.len() <= 32
        && first.is_ascii_lowercase()
        && value.chars().all(|glyph| {
            glyph.is_ascii_lowercase() || glyph.is_ascii_digit() || matches!(glyph, '_' | '-')
        })
}

/// The five shipped commands (`command-builtins.ts:20-144`).
///
/// Descriptions and usage strings are TS's, byte for byte: `/help` prints them and the composer lists
/// them, so they are user-visible text, not internal labels.
#[must_use]
pub fn builtin_commands() -> Vec<CommandSpec> {
    vec![
        CommandSpec {
            name: "help".into(),
            description: "List available commands. `/help` lists all; `/help <command>` explains one.".into(),
            usage: Some("/help [command]".into()),
        },
        CommandSpec {
            name: "tools".into(),
            description: "List enabled tools, search the catalogue (`/tools <query>`), or inspect one tool (`/tools <name>`).".into(),
            usage: Some("/tools [query|tool-name]".into()),
        },
        CommandSpec {
            name: "task".into(),
            description: "Manage the session task list: list, create, get, update, or delete tasks.".into(),
            usage: Some("/task list | /task create <subject> [-d <description>] [-b \"blocked by\"] | /task get <id> | /task update <id> [-s <status>] [-o <owner>] [-m key=value ...] | /task delete <id>".into()),
        },
        CommandSpec {
            name: "websearch".into(),
            description: "Search the web from chat: /websearch <query> [context]. Returns numbered snippets.".into(),
            usage: Some("/websearch <query> [context]".into()),
        },
        CommandSpec {
            name: "webfetch".into(),
            description: "Fetch web content from chat: /webfetch <url> [instruction]. Returns readable markdown.".into(),
            usage: Some("/webfetch <url> [instruction]".into()),
        },
    ]
}

/// What one command call settled with (`command.ts:188 CommandResult`).
#[derive(Debug, Clone)]
pub struct CommandOutcome {
    pub name: String,
    pub args: String,
    pub content: String,
    pub is_error: bool,
    pub panels: Option<Value>,
}

/// `command.ts:247 runCommand`: parse, dispatch, and shape the result the transcript records.
///
/// An unknown command is a FAILED result, never an error and never a task: TS answers
/// `unknown command: /<name>` inside the turn (`:122-127`), because a typo in a composer is data the
/// user should see rather than a 4xx.
pub fn dispatch(state: &mut SessionState, store: &Store, prompt: &str) -> CommandOutcome {
    match parse_command_line(prompt) {
        ParsedCommand::None => CommandOutcome {
            name: String::new(),
            args: String::new(),
            content: String::new(),
            is_error: false,
            panels: None,
        },
        ParsedCommand::Help { name } => {
            let mut outcome = CommandOutcome {
                name: name.clone(),
                args: String::new(),
                content: String::new(),
                is_error: false,
                panels: None,
            };
            if !known_command(&name) {
                outcome.content = unknown_message(&name);
                outcome.is_error = true;
                return outcome;
            }
            // A bare `/name` is the usage card (`command-builtins.ts:26-34`).
            outcome.content = usage_card(&name);
            outcome
        }
        ParsedCommand::Call { name, args } => {
            let mut outcome = CommandOutcome {
                name: name.clone(),
                args: args.clone(),
                content: String::new(),
                is_error: false,
                panels: None,
            };
            if !known_command(&name) {
                outcome.content = unknown_message(&name);
                outcome.is_error = true;
                return outcome;
            }
            outcome.content = match name.as_str() {
                "help" => help_text(&args),
                "tools" => tools_text(store, state, &args),
                "task" => crate::store::actions::command_task(store, state, &args),
                // The web commands need the agent loop's tool runtime, which lives behind the port and
                // is executed ASYNC. The store therefore answers with the tool's own contract text and
                // lets the integrated runtime replace it (T7); the model reads the same names either
                // way. See knowledge.md "Decisions".
                "websearch" | "webfetch" => command_tool_note(&name, &args),
                other => unknown_message(other),
            };
            outcome.is_error = outcome.content.starts_with("unknown command:");
            outcome
        }
    }
}

/// `command.ts:130-140 isCommandPrompt`, widened to the whole registry the way
/// `sessions.ts:2490 shouldTreatAsCommand` does (the five built-ins ARE commands here, unlike TS's
/// host-gated `isCommandPrompt`, because this build registers them unconditionally).
#[must_use]
pub fn is_command_prompt(prompt: &str) -> bool {
    matches!(
        parse_command_line(prompt),
        ParsedCommand::Help { .. } | ParsedCommand::Call { .. }
    )
}

/// Whether one name is in this session's registry (`command.ts:143 hasCommand`).
#[must_use]
pub fn known_command(name: &str) -> bool {
    builtin_commands()
        .iter()
        .any(|command| command.name == name)
}

/// The registry as a name -> spec map, for the usage card.
#[must_use]
pub fn command_registry() -> BTreeMap<String, CommandSpec> {
    builtin_commands()
        .into_iter()
        .map(|command| (command.name.clone(), command))
        .collect()
}

/// `command-builtins.ts:120-126`: the unknown-command sentence, verbatim.
#[must_use]
pub fn unknown_message(name: &str) -> String {
    format!("unknown command: /{name}")
}

/// The usage card of one command (`/help <command>` and a bare `/command`).
#[must_use]
pub fn usage_card(name: &str) -> String {
    match command_registry().get(name) {
        Some(spec) => format!(
            "/{name} - {}\nUsage: {}",
            spec.description,
            spec.usage.clone().unwrap_or_else(|| format!("/{name}"))
        ),
        None => unknown_message(name),
    }
}

/// `/help [command]` (`command-builtins.ts:20-39`): the command list, or one command's card.
#[must_use]
pub fn help_text(args: &str) -> String {
    let arg = args.trim();
    if !arg.is_empty() {
        let name = arg.trim_start_matches('/');
        return usage_card(name);
    }
    let lines: Vec<String> = builtin_commands()
        .into_iter()
        .map(|spec| {
            format!(
                "/{:<12} - {}",
                spec.name,
                spec.usage.clone().unwrap_or_default()
            )
        })
        .collect();
    format!("Available commands:\n{}", lines.join("\n"))
}

/// `/tools [query]` (`command-builtins.ts:41-66`): the enabled list, a catalogue search, or one tool's
/// schema card.
#[must_use]
pub fn tools_text(store: &Store, state: &SessionState, args: &str) -> String {
    let query = args.trim();
    let declarations = store.declarations_for(state);
    let enabled: Vec<String> = declarations.iter().map(|decl| decl.name.clone()).collect();

    if query.is_empty() {
        let names = store.tools_for(state);
        if names.is_empty() {
            return "No tools are available in this session.".to_owned();
        }
        let listing = format_tool_lines(&names);
        return format!(
            "Enabled tools ({}):\n{listing}\nSearch the catalogue with `/tools <query>`; inspect one with `/tools <name>`.",
            names.len()
        );
    }

    // An exact name wins over a fuzzy match (`command-builtins.ts:55-58`).
    if let Some(decl) = declarations.iter().find(|decl| decl.name == query) {
        let parameters = decl
            .parameters
            .as_ref()
            .map(|schema| serde_json::to_string(schema).unwrap_or_else(|_| "{}".into()))
            .unwrap_or_else(|| "{}".into());
        let availability = if enabled.contains(&decl.name) {
            "enabled"
        } else {
            "not enabled for this session"
        };
        return format!(
            "{name}\nDescription: {description}\nStatus: {availability}\nInputSchema: {parameters}",
            name = decl.name,
            description = decl.description,
        );
    }

    let needle = query.to_lowercase();
    let matches: Vec<String> = declarations
        .iter()
        .filter(|decl| {
            decl.name.to_lowercase().contains(&needle)
                || decl.description.to_lowercase().contains(&needle)
        })
        .map(|decl| decl.name.clone())
        .collect();

    if matches.is_empty() {
        return format!("No tools matched '{query}'.");
    }

    format!(
        "Found {count} matching tools (use /toolRequest to enable): {list}",
        count = matches.len(),
        list = matches.join(", ")
    )
}

/// `/tools`' compact two-per-line listing (`command-builtins.ts:45-50`).
#[must_use]
fn format_tool_lines(names: &[String]) -> String {
    let mut lines: Vec<String> = Vec::new();
    for pair in names.chunks(2) {
        lines.push(format!("  {}", pair.join("  ")));
    }
    lines.join("\n")
}

/// The bridge text for a command that must run a real tool body (`/websearch`, `/webfetch`).
#[must_use]
fn command_tool_note(name: &str, args: &str) -> String {
    format!(
        "/{name} {args}\nThis build routes {name} through the integrated {name} tool; enable it with toolRequest and call it directly.",
        name = name
    )
}

/// The budget cap of a command result (`core/result-budget.ts capResult`).
///
/// Character-counted rather than byte-counted: the brief's cap is 64 KiB of TEXT, and a UTF-8 count
/// would let a CJK result reach triple that on the wire.
#[must_use]
pub fn cap_text(text: &str) -> String {
    if text.len() <= COMMAND_RESULT_BUDGET_BYTES {
        return text.to_owned();
    }
    let mut keep = COMMAND_RESULT_BUDGET_BYTES;
    while keep > 0 && !text.is_char_boundary(keep) {
        keep -= 1;
    }
    format!("{}{RESULT_CAP_MARKER}", &text[..keep])
}

impl CommandOutcome {
    /// The `tool` transcript entry this result becomes (`sessions.ts:3678-3690`).
    #[must_use]
    pub fn tool_message(&self, call_id: &str) -> HarnessMessage {
        HarnessMessage::Tool {
            content: cap_text(&self.content),
            tool_call_id: call_id.to_owned(),
            name: COMMAND_TOOL_NAME.to_owned(),
            attachments: Vec::new(),
        }
    }

    /// The `toolCall` entry that pairs with [`CommandOutcome::tool_message`].
    #[must_use]
    pub fn call(&self) -> HarnessToolCall {
        HarnessToolCall {
            id: format!("cmd-{}", now_ms()),
            name: COMMAND_TOOL_NAME.to_owned(),
            arguments: serde_json::to_string(&Map::from_iter([
                (
                    "command".to_owned(),
                    Value::String(format!("/{}", self.name)),
                ),
                ("args".to_owned(), Value::String(self.args.clone())),
            ]))
            .unwrap_or_else(|_| "{}".into()),
        }
    }

    /// Why the call ended: a failed command is `failed`, never `rejected`
    /// (`command.ts:216-224 outcomeFromResult`).
    #[must_use]
    pub fn outcome(&self) -> HarnessToolOutcome {
        if self.is_error {
            HarnessToolOutcome::Failed
        } else {
            HarnessToolOutcome::Completed
        }
    }
}

/// A slash command as the SESSION keeps it: the parse plus the settled result.
///
/// The brief lists the command as part of the turn record; `SessionTurn` (T1) has no field for it, and
/// the transcript already carries the `toolCall`/`tool` pair, so this is the audit copy the reads need
/// (`turn.blocks[].steps[].name == "command"`).
#[derive(Debug, Clone)]
pub struct CommandRecord {
    pub name: String,
    pub args: String,
    pub content: String,
    pub is_error: bool,
    pub recorded_at: u64,
}

/// The bounded preview a command row prints.
#[must_use]
pub fn command_preview(content: &str) -> String {
    preview(content, TURN_PREVIEW_CHARS)
}

// Cross-references kept for the next agent.
#[allow(dead_code)]
type _Refs = (tools::CreatedTool, commands::CommandSpec);
