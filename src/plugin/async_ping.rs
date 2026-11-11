//! Port of `underload/harness/src/plugins/async-ping.plugin.ts` — "Background
//! Ping", the recurring wake for background tool work.
//!
//! Before this plugin existed, the wake was a hardcoded board-wide `setInterval`
//! in the scheduler (`core/scheduler.ts`). It lives HERE now: ONE self-re-arming
//! one-shot timer per scheduler identity, so a burst of ten calls stays one
//! timer, and a board that drains between ticks is never rescheduled.
//!
//! Two things this port pins:
//!
//! 1. The pure decision rules, which are deterministic and unit-tested here:
//!    [`interval_of`], [`arm_decision`], [`tick_decision`], [`thread_timer_key`].
//! 2. The reserved-`enabled` interaction. The framework gate already stops this
//!    plugin's HOOKS while the switch is off (`plugin/host.ts`, `plugin/hooks.ts`
//!    rule 9), but a heartbeat armed BEFORE the switch was flipped is a timer
//!    handle the bus has no claim on: it would keep firing, keep waking the
//!    conversation, and keep re-arming forever. So the two places that decide to
//!    WAKE or to CONTINUE read the same reserved key (async-ping.plugin.ts:124-133,
//!    193-196, 220). A disable therefore costs at most one interval, after which
//!    the plugin holds no timer and emits no wake — genuinely stopped, not merely
//!    unheard. `harness.dispose` is delivered even while disabled, so a teardown
//!    still clears whatever survived the switch.
//!
//! WHERE THE PORT CANNOT BE LITERAL
//! --------------------------------
//! Node's `setTimeout(...).unref()` exists so a heartbeat never holds the process
//! open (async-ping.plugin.ts:59-63). A `tokio::spawn`ed task has that property by
//! default, so the handle here is a join handle and `clear` is an `abort`. The one
//! real difference: a synchronous `fire()` dispatch can reach this handler with no
//! tokio runtime current, where `spawn` would PANIC — and this crate builds with
//! `panic = "abort"`, so a panic inside a hook is fatal rather than fail-open.
//! [`spawn_heartbeat`] therefore checks [`tokio::runtime::Handle::try_current`]
//! and declines to arm when there is no clock to fire on. A host with no async
//! runtime has no heartbeat in either language; Rust just says so.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::plugin::context::PluginRuntime;
use crate::plugin::event::{AsyncLifecycle, Contribution, LifecycleEvent, Payload};
use crate::plugin::host::{HostError, Plugin, PluginApi};
use crate::plugin::hooks::{HandlerCall, HandlerResult, HookOptions};
use crate::plugins::{self, PluginMetadata};

/// The documented heartbeat, used when neither the setting nor the host config
/// gives a usable number (async-ping.plugin.ts:77): a corrupt host document must
/// not turn the ping into a 0 ms spin.
pub const DEFAULT_PING_INTERVAL_MS: f64 = 600_000.0;

pub const PLUGIN_NAME: &str = "async-ping";

/// The declared hooks (async-ping.plugin.ts:90-97), in declaration order. The
/// plugin registers exactly one handler per entry, in this order, so a setup
/// async-ping's bus `live()` surface equals this list — which is what the Plugins
/// pane's "subscribed" column reads (it is the SUBSCRIBED surface, not the
/// declared one; see `server/plugins.ts`).
pub const HOOKS: [&str; 6] = [
    "async.pending",
    "async.drained",
    "run.failed",
    "thread.stopped",
    "thread.deleted",
    "harness.dispose",
];

/// Resolve the interval, mirroring `intervalOf` (async-ping.plugin.ts:141-153).
///
/// `api.settings` re-reads the provider on every call, so the value is resolved
/// at ARM time and a settings change applies to the next ping instead of
/// requiring a restart; the chain stored setting → declared default →
/// `api.config.wakeIntervalMs` → DEFAULT happens inside settings resolution, so
/// all that reaches this function is a candidate number. `settings.number`
/// coerces by TYPE only, so an operator-stored `0`, `-5` or `0.4` gets here, and
/// anything that cannot drive a sane repeat falls back rather than spinning the
/// board with a zero-length interval. `min`/`step` stay UI hints and are never
/// enforced.
#[must_use]
pub fn interval_of(configured: f64) -> f64 {
    if configured.is_finite() && configured >= 1.0 {
        configured
    } else {
        DEFAULT_PING_INTERVAL_MS
    }
}

/// Whether `arm` starts a heartbeat for an identity (async-ping.plugin.ts:186-208).
///
/// - [`ArmDecision::Ignore`] — an empty/unknown identity cannot address a board
///   (ignore rather than invent a shared `''` timer that would wake unrelated
///   agents), OR the transition announced no work (a ping would report an empty
///   list and cost a model turn for nothing).
/// - [`ArmDecision::Release`] — switched off: release the key instead of arming,
///   so a re-enable starts a clean heartbeat rather than resuming one whose board
///   may have drained meanwhile.
/// - [`ArmDecision::Keep`] — one timer per identity: an identity that already has
///   a live handle is left alone.
/// - [`ArmDecision::Arm`] — install a fresh one-shot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmDecision {
    Ignore,
    Release,
    Keep,
    Arm,
}

#[must_use]
pub fn arm_decision(
    agent: Option<&str>,
    has_work: bool,
    enabled: bool,
    already_armed: bool,
) -> ArmDecision {
    // Checked FIRST, so an unknown identity never reaches the table at all, and a
    // disabled plugin is never asked to release a key it never held.
    if !has_work || agent.is_none_or(|name| name.is_empty()) {
        return ArmDecision::Ignore;
    }
    if !enabled {
        return ArmDecision::Release;
    }
    if already_armed {
        return ArmDecision::Keep;
    }
    ArmDecision::Arm
}

/// What one heartbeat tick does (async-ping.plugin.ts:214-237).
///
/// The entry is dropped BEFORE anything else happens, so the re-arm at the end
/// installs a fresh handle instead of being skipped by `arm`'s already-armed
/// guard, and so a wake whose delivery drains the board (which emits
/// `async.drained` → clear) can never leave a dead handle registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickDecision {
    /// Switched off between ticks: the entry is already dropped, so nothing is
    /// re-armed and no wake is emitted — the conversation is not woken for a
    /// plugin the operator turned off, and no handle is left behind.
    StopDisabled,
    /// The board drained between ticks. This is also the last-resort cleanup for a
    /// board released WITHOUT an `async.drained` event (`toolCancel` of an agent's
    /// last call, or a scope released by a run teardown): such a handle fires once,
    /// reports nothing, and is not rescheduled, so the observable contract — "no
    /// wake for an empty board" — holds even in that window.
    StopDrained,
    /// The board still has work: emit the wake and keep pinging until it drains.
    WakeAndRearm,
}

#[must_use]
pub fn tick_decision(enabled: bool, pending_after_drop: usize) -> TickDecision {
    if !enabled {
        return TickDecision::StopDisabled;
    }
    if pending_after_drop == 0 {
        return TickDecision::StopDrained;
    }
    TickDecision::WakeAndRearm
}

/// The scheduling identity of a host thread board: a thread is scheduled as
/// `thread:<id>`, while an agent's DEFAULT conversation is keyed by its display
/// name — which other boards may legitimately share, so a thread teardown clears
/// only the `thread:` key and sweeps the rest once they are provably empty
/// (async-ping.plugin.ts:251-260).
#[must_use]
pub fn thread_timer_key(thread_id: &str) -> String {
    format!("thread:{thread_id}")
}

/// Scheduler identity → its pending one-shot handle. Absent = not pinging
/// (async-ping.plugin.ts:113-122).
///
/// `Mutex`, not a lock-free cell: handlers can run on different threads and the
/// table must never tear. A poisoned lock reads as "no timers", never as a panic,
/// because a panic inside a hook is the one thing this fail-open plugin must not
/// do (and `panic = "abort"` would end the process over it).
type Timers = Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>;

/// The Background Ping plugin.
///
/// Cloning shares the timer table (`Arc`), which is what the host relies on: the
/// plugin is registered once and its handles outlive any single dispatch.
#[derive(Debug, Clone, Default)]
pub struct AsyncPingPlugin {
    timers: Timers,
}

impl AsyncPingPlugin {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every identity with a live heartbeat, sorted. Diagnostic surface: the host
    /// exposes it so a test (and an operator) can see the timers the bus has no
    /// claim on.
    #[must_use]
    pub fn armed(&self) -> Vec<String> {
        let mut names: Vec<String> = match self.timers.lock() {
            Ok(table) => table.keys().cloned().collect(),
            Err(_) => return Vec::new(),
        };
        names.sort();
        names
    }
}

impl Plugin for AsyncPingPlugin {
    fn metadata(&self) -> &'static PluginMetadata {
        // The static catalogue row is the single source of truth for a built-in's
        // declared metadata; this must not be a second copy of it.
        plugins::metadata_of(PLUGIN_NAME).expect("async-ping ships in the catalogue")
    }

    fn setup(&self, api: &PluginApi) -> Result<(), HostError> {
        let timers = self.timers.clone();

        // `async.pending` → start (or keep) that identity's heartbeat
        // (async-ping.plugin.ts:242-244). The payload list is the transition's OWN
        // claim: this hook fires before `scheduler.schedule()` puts the task on the
        // board, so the live board must not be what decides — a single-call turn
        // would never arm.
        let handle = timers.clone();
        let scope = api.clone();
        api.on(
            HOOKS[0],
            move |call: &HandlerCall| -> HandlerResult {
                let AsyncLifecycle { agent, pending } = match &call.payload {
                    Payload::AsyncPending(lifecycle) => lifecycle.clone(),
                    // A registration only ever sees its own event; anything else
                    // announces nothing and is ignored rather than guessed at.
                    _ => return Ok(Contribution::nothing()),
                };
                let agent = Some(agent);
                let has_work = !pending.is_empty();

                // The reserved switch, read LIVE (`isEnabled`, async-ping.plugin.ts:133).
                let enabled = scope.is_enabled();
                let already_armed = agent.as_ref().is_some_and(|name| {
                    handle
                        .lock()
                        .is_ok_and(|table| table.contains_key(name.as_str()))
                });

                match arm_decision(agent.as_deref(), has_work, enabled, already_armed) {
                    ArmDecision::Ignore | ArmDecision::Keep => Ok(Contribution::nothing()),
                    ArmDecision::Release => {
                        if let Some(name) = agent {
                            if let Ok(mut table) = handle.lock() {
                                if let Some(task) = table.remove(&name) {
                                    task.abort();
                                }
                            }
                        }
                        Ok(Contribution::nothing())
                    }
                    ArmDecision::Arm => {
                        let name = agent.unwrap_or_default();
                        // Resolved at ARM time, never at setup, so a settings change
                        // applies to the next ping (async-ping.plugin.ts:135-145).
                        let fallback = scope
                            .runtime()
                            .wake_interval_ms()
                            .unwrap_or(DEFAULT_PING_INTERVAL_MS);
                        let configured = scope
                            .settings()
                            .and_then(|settings| settings.number("intervalMs", fallback));
                        let interval_ms = interval_of(configured.unwrap_or(fallback));
                        let next =
                            spawn_heartbeat(handle.clone(), name.clone(), interval_ms, scope.clone());
                        if let Some(next) = next {
                            if let Ok(mut table) = handle.lock() {
                                table.insert(name, next);
                            }
                        }
                        Ok(Contribution::nothing())
                    }
                }
            },
            HookOptions::default(),
        )?;

        // `async.drained` → stop pinging that board (async-ping.plugin.ts:247-249).
        let handle = timers.clone();
        api.on(
            HOOKS[1],
            move |call: &HandlerCall| -> HandlerResult {
                let agent = match &call.payload {
                    Payload::AsyncDrained(lifecycle) => lifecycle.agent.clone(),
                    _ => return Ok(Contribution::nothing()),
                };
                if let Ok(mut table) = handle.lock() {
                    if let Some(task) = table.remove(&agent) {
                        task.abort();
                    }
                }
                Ok(Contribution::nothing())
            },
            HookOptions::default(),
        )?;

        // `run.failed` → drop every timer whose board no longer holds anything
        // (async-ping.plugin.ts:263-269). A run abort releases its whole scope
        // WITHOUT a delivery (`abandonScope` notifies nobody), so no
        // `async.drained` fires for it; the sweep removes only the timers that are
        // provably pointless, and live boards are deliberately untouched.
        let handle = timers.clone();
        let sweep = api.clone();
        api.on(
            HOOKS[2],
            move |_call: &HandlerCall| -> HandlerResult {
                sweep_drained(&handle, sweep.runtime());
                Ok(Contribution::nothing())
            },
            HookOptions::default(),
        )?;

        // `thread.stopped` / `thread.deleted` → release that thread's own
        // `thread:<id>` timer, then sweep (async-ping.plugin.ts:271-279). Both
        // events go through the bus's SYNCHRONOUS `fire()` — a handle method cannot
        // await — so reacting here costs no extra microtask boundary.
        let handle = timers.clone();
        let sweep = api.clone();
        api.on(
            HOOKS[3],
            move |call: &HandlerCall| -> HandlerResult {
                release_thread(&handle, call, sweep.runtime());
                Ok(Contribution::nothing())
            },
            HookOptions::default(),
        )?;

        let handle = timers.clone();
        let sweep = api.clone();
        api.on(
            HOOKS[4],
            move |call: &HandlerCall| -> HandlerResult {
                release_thread(&handle, call, sweep.runtime());
                Ok(Contribution::nothing())
            },
            HookOptions::default(),
        )?;

        // `harness.dispose` → clear everything (async-ping.plugin.ts:281-287): the
        // real end of the process, delivered even while disabled, so a teardown
        // still clears whatever survived a disable.
        let handle = timers.clone();
        api.on(
            HOOKS[5],
            move |_call: &HandlerCall| -> HandlerResult {
                if let Ok(mut table) = handle.lock() {
                    for (_, task) in table.drain() {
                        task.abort();
                    }
                }
                Ok(Contribution::nothing())
            },
            HookOptions::default(),
        )?;

        Ok(())
    }
}

/// The thread id one of the two teardown payloads carries. `thread.deleted` has
/// no record left, which is why the payload union spells the id out as a field;
/// both names land on the same scheduler key.
fn thread_id_of(call: &HandlerCall) -> Option<String> {
    match &call.payload {
        Payload::Thread(record) => Some(record.id.clone()),
        Payload::ThreadDeleted { thread_id, .. } => Some(thread_id.clone()),
        _ => None,
    }
}

/// Clear one thread's board key and sweep the rest (async-ping.plugin.ts:257-260).
fn release_thread(timers: &Timers, call: &HandlerCall, runtime: &dyn PluginRuntime) {
    let event = call.event;
    if matches!(event, LifecycleEvent::ThreadStopped | LifecycleEvent::ThreadDeleted) {
        if let Some(thread_id) = thread_id_of(call) {
            if let Ok(mut table) = timers.lock() {
                if let Some(task) = table.remove(&thread_timer_key(&thread_id)) {
                    task.abort();
                }
            }
        }
    }
    sweep_drained(timers, runtime);
}

/// Drop every timer whose board no longer holds anything (async-ping.plugin.ts:173-178).
fn sweep_drained(timers: &Timers, runtime: &dyn PluginRuntime) {
    let names: Vec<String> = match timers.lock() {
        Ok(table) => table.keys().cloned().collect(),
        Err(_) => return,
    };
    for name in names {
        if !runtime.async_pending_for(Some(&name)).is_empty() {
            continue;
        }
        if let Ok(mut table) = timers.lock() {
            if let Some(task) = table.remove(&name) {
                task.abort();
            }
        }
    }
}

/// One heartbeat: a one-shot sleep that re-arms itself, deliberately NOT a
/// repeating interval, so the tick decides whether the cadence continues and a
/// `clear` cannot race a still-firing repeating handle into a double-ping
/// (async-ping.plugin.ts:118-122, 202-207).
///
/// Returns `None` when no tokio runtime is current — see the module header for
/// why that must not be a panic.
fn spawn_heartbeat(
    timers: Timers,
    agent: String,
    interval_ms: f64,
    scope: PluginApi,
) -> Option<tokio::task::JoinHandle<()>> {
    if tokio::runtime::Handle::try_current().is_err() {
        return None;
    }
    Some(tokio::spawn(async move {
        let every = std::time::Duration::from_millis(interval_ms.max(1.0) as u64);
        tokio::time::sleep(every).await;

        // The entry is dropped BEFORE anything else happens. A missing entry means
        // `clear()` already released this identity — a drained board, a teardown, or
        // a disable — and a dead heartbeat must not be resurrected.
        let dropped = match timers.lock() {
            Ok(mut table) => table.remove(&agent).is_some(),
            Err(_) => false,
        };
        if !dropped {
            return;
        }

        // Both reads are LIVE: work may have finished between ticks, and a wake
        // would then claim pending calls that no longer exist.
        let enabled = scope.is_enabled();
        let pending = scope.runtime().async_pending_for(Some(&agent)).len();
        if tick_decision(enabled, pending) != TickDecision::WakeAndRearm {
            return;
        }

        scope.runtime().async_wake(Some(&agent));
        if let Some(next) = spawn_heartbeat(timers.clone(), agent.clone(), interval_ms, scope) {
            if let Ok(mut table) = timers.lock() {
                table.insert(agent, next);
            }
        }
    }))
}
