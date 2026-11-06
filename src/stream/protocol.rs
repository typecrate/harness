//! The wire layer of the harness update stream: the closed vocabularies, the per-scope tables, the
//! opaque cursor codec, and the client-frame validator the conversation runs FIRST.
//!
//! TypeScript origin: `packages/underload/harness/src/server/stream/protocol.ts` (574 lines) on top
//! of the generated `src/shared/harness-stream.ts` vocabulary, which this crate already carries in
//! `contract.rs` (the scopes, kinds, codes and the 18 limit numbers). The frame TYPES stay JSON here
//! rather than becoming serde models for one reason: the contract's union is discriminated twice
//! (`type`, then `scope`/`kind`) with per-variant required/forbidden fields, and `broker.rs` forwards
//! a payload it never interprets (`payload` is whatever the mutation published). A `Value` therefore
//! keeps the payload honest-by-construction while this module still owns every field name the wire
//! contract pins (all snake_case, per `knowledge.md` "Naming and serde shape").
//!
//! THE VALIDATION PRECEDENCE IS THE CONTRACT (`harness.swagger.yaml:2389-2413`). Exactly one
//! condition is reported per frame, in this order:
//!
//! ```text
//! (1) UTF-8 JSON text object within `max_client_frame_bytes`  -> parse_client_frame (frame_too_large)
//! (2) validates against the client union for its `type`       -> parse_client_frame (invalid_payload)
//! (3) `hello` only as the first frame                         -> ./connection.rs (late_hello)
//! (4) `subscription_id` known for ack/unsubscribe             -> ./connection.rs + ./broker.rs
//! (5) required resource id exists                             -> ./broker.rs (unknown_resource)
//! (6) filter published for that scope                         -> ./broker.rs (unsupported_filter)
//! (7) budgets and rates                                       -> ./connection.rs + ./broker.rs
//! ```
//!
//! Step (2) is a FULL schema check here (closed field sets, per-scope id pairs, enums) rather than a
//! loose tag test, which is what lets step (5) and (7) stay silent about a bad `scope`: a scope
//! outside the closed four is a schema failure and can never also be a resource or budget problem.
//!
//! Cross-references:
//!   * `contract.rs:2672-2803` - the generated vocabulary and `ws_stream_limits()` (the one source
//!     of every number; nothing in this module restates one).
//!   * `events.rs` - the epoch and the counters the baseline/resync frames quote.
//!   * `origin.rs:47,140` - the 403 that runs BEFORE the upgrade.
//!   * `harness.swagger.yaml:2326-2575` - the `x-websocket` extension every table below implements.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use serde_json::{Map, Value};

use crate::contract::{self, WsStreamLimits};

// ---------------------------------------------------------------------------
// Close codes (`protocol.ts:52-65`, swagger `x-websocket.close_codes`)
//
// Every entry is documented in the spec, so a wrong code is a contract break rather than a style
// choice: no call site in this crate may spell a bare number.
// ---------------------------------------------------------------------------

/// The close codes this conversation may use.
pub mod close_codes {
    /// A clean goodbye, sent by `bye` or by a client closing.
    pub const NORMAL: u16 = 1000;
    /// Host shutdown while background work drains.
    pub const GOING_AWAY: u16 = 1001;
    /// A frame the conversation cannot honour.
    pub const PROTOCOL_ERROR: u16 = 1002;
    /// A non-text frame: this conversation is JSON text only.
    pub const UNSUPPORTED_DATA: u16 = 1003;
    /// Three consecutive frames failed the client union.
    pub const INVALID_PAYLOAD: u16 = 1007;
    /// The Origin gate refused this upgrade peer.
    pub const POLICY: u16 = 1008;
    /// A client frame above the advertised `max_client_frame_bytes`.
    pub const MESSAGE_TOO_BIG: u16 = 1009;
    /// The server epoch changed; every retained cursor is dead.
    pub const RESTART: u16 = 1012;
    /// Send queue stalled beyond the advertised `sustained_stall_ms`.
    pub const TRY_AGAIN_LATER: u16 = 1013;
    /// The advertised `subscriptions_per_connection` budget is full.
    pub const TOO_MANY_SUBSCRIPTIONS: u16 = 4401;
    /// No `pong` for the advertised `heartbeat_timeout_intervals`.
    pub const HEARTBEAT_TIMEOUT: u16 = 4408;
    /// The advertised `client_messages_per_second` budget is exceeded.
    pub const RATE_LIMITED: u16 = 4429;
}

// ---------------------------------------------------------------------------
// The numbers in force (`protocol.ts:297-327`)
// ---------------------------------------------------------------------------

/// TS `HARNESS_WS_STREAM_LIMITS` re-exported as `DEFAULT_STREAM_LIMITS`: the contract's own
/// numbers, taken from `contract::ws_stream_limits` so the REST advertisement and the stream's
/// `welcome` frame cannot drift.
#[must_use]
pub fn default_ws_limits() -> WsStreamLimits {
    contract::ws_stream_limits()
}

/// One declared bound for a `deployment_tunable_within_declared_bounds` number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsTunableBound {
    pub min: u64,
    pub max: u64,
}

/// `HARNESS_WS_TUNABLE_BOUNDS` (`protocol.ts:297-304`), copied from the `WsStreamLimits` property
/// schemas. A `fixed_by_contract` value is NOT in this table and is therefore never overridable, so
/// a host cannot retune the replay window or the heartbeat cadence.
#[must_use]
pub fn ws_tunable_bounds() -> BTreeMap<&'static str, WsTunableBound> {
    BTreeMap::from([
        (
            "subscriptions_per_connection",
            WsTunableBound { min: 1, max: 256 },
        ),
        (
            "client_messages_per_second",
            WsTunableBound { min: 1, max: 100 },
        ),
        ("send_queue_frames", WsTunableBound { min: 1, max: 4096 }),
        (
            "send_queue_bytes",
            WsTunableBound {
                min: 65_536,
                max: 16_777_216,
            },
        ),
        (
            "sustained_stall_ms",
            WsTunableBound {
                min: 1_000,
                max: 60_000,
            },
        ),
        (
            "ack_timeout_ms",
            WsTunableBound {
                min: 1_000,
                max: 60_000,
            },
        ),
    ])
}

/// `clamped` (`protocol.ts:306-309`) for a value the deployment DID supply: a `u64` cannot be
/// non-finite, so the only rule left is the declared range. An absent override keeps the shipped
/// default, which is the `fallback` arm of the TS original and is handled by the caller.
fn clamped(value: u64, bound: WsTunableBound) -> u64 {
    value.clamp(bound.min, bound.max)
}

/// `resolveStreamLimits` (`protocol.ts:316-327`): the numbers IN FORCE for one host. The contract
/// defaults are the shipped values; a deployment may retune only the six tunable entries, and only
/// inside the declared bounds, and the result is what `welcome.limits` delivers - which is the only
/// source a client may rely on.
///
/// An absent key means "keep the default", which is TS's `Partial<WsStreamLimits>` arm; a key that
/// is not tunable is ignored, exactly as `heartbeat_timeout_close_code` is re-stamped below.
#[must_use]
pub fn resolve_stream_limits(overrides: &BTreeMap<String, u64>) -> WsStreamLimits {
    let mut limits = default_ws_limits();
    let bounds = ws_tunable_bounds();

    for (key, bound) in bounds {
        let Some(value) = overrides.get(key).copied() else {
            continue;
        };

        match key {
            "subscriptions_per_connection" => {
                limits.subscriptions_per_connection = clamped(value, bound);
            }
            "client_messages_per_second" => {
                limits.client_messages_per_second = clamped(value, bound);
            }
            "send_queue_frames" => limits.send_queue_frames = clamped(value, bound),
            "send_queue_bytes" => limits.send_queue_bytes = clamped(value, bound),
            "sustained_stall_ms" => limits.sustained_stall_ms = clamped(value, bound),
            "ack_timeout_ms" => limits.ack_timeout_ms = clamped(value, bound),
            // Every other number is `fixed_by_contract`, so the arm above is the whole rule; the
            // heartbeat close code has a single-member enum and is restamped below for good measure.
            _ => {}
        }
    }

    limits.heartbeat_timeout_close_code = default_ws_limits().heartbeat_timeout_close_code;

    limits
}

// ---------------------------------------------------------------------------
// Per-scope tables (`protocol.ts:72-155`)
// ---------------------------------------------------------------------------

/// `HARNESS_WS_SCOPE_PUBLISHES`: which update kinds each scope publishes. A frame never reports a
/// kind outside its scope's set, which is what keeps a revision baseline small and makes
/// `publishes` in `subscription_state` checkable.
#[must_use]
pub fn scope_publishes(scope: &str) -> &'static [&'static str] {
    match scope {
        "global" => &["catalogue.updated", "config.updated"],
        "session" => &[
            "session.updated",
            "turn.upsert",
            "turn.removed",
            "action.opened",
            "action.closed",
            "usage.updated",
            "thread.upsert",
            "thread.removed",
            "tool.registry_updated",
            "agent.registry_updated",
            "run.event",
        ],
        "thread" => &["transcript.updated", "thread.upsert"],
        "turn" => &["display.ops", "usage.updated", "turn.upsert"],
        _ => &[],
    }
}

/// `HARNESS_WS_SCOPE_IDS.required`: the id a scope must name.
#[must_use]
pub fn scope_ids_required(scope: &str) -> &'static [&'static str] {
    match scope {
        "global" => &[],
        "session" => &["session_id"],
        "thread" => &["session_id", "thread_id"],
        "turn" => &["session_id", "turn_id"],
        _ => &[],
    }
}

/// `HARNESS_WS_SCOPE_IDS.forbidden`: an id outside the pair is `invalid_payload` at schema level
/// (step 2), while a well-formed required id that does not exist is `unknown_resource` (step 5).
#[must_use]
pub fn scope_ids_forbidden(scope: &str) -> &'static [&'static str] {
    match scope {
        "global" => &["session_id", "thread_id", "turn_id"],
        "session" => &["thread_id", "turn_id"],
        "thread" => &["turn_id"],
        "turn" => &["thread_id"],
        _ => &[],
    }
}

/// `HARNESS_WS_SCOPE_FILTERS`: filters a scope answers for. Anything else is `unsupported_filter`
/// for that subscription only (step 6).
#[must_use]
pub fn scope_filters(scope: &str) -> &'static [&'static str] {
    match scope {
        "global" => &[],
        "session" => &["status", "thread_id", "events"],
        "thread" => &[],
        "turn" => &["sections", "context"],
        _ => &[],
    }
}

/// `HARNESS_WS_SCOPE_BASELINES`: the revision-baseline set per scope, verbatim from the
/// `StreamResourceKind` description. Every set is within `revision_records_per_state` (20).
#[must_use]
pub fn scope_baselines(scope: &str) -> &'static [&'static str] {
    match scope {
        "global" => &["harnesses", "agents", "tools", "config"],
        "session" => &[
            "session",
            "definition",
            "turns",
            "queue",
            "actions",
            "usage",
            "threads",
            "session_agents",
            "session_tools",
            "commands",
        ],
        "thread" => &["thread", "transcript"],
        "turn" => &["turn", "turn_display", "turn_events"],
        _ => &[],
    }
}

/// `HARNESS_WS_KIND_RESOURCES`: which counters one update kind moves, in-epoch. Two kinds are
/// DERIVED and mint no counter of their own (`turn_display`/`turn_events` carry the owning turn's
/// revision, `transcript_messages` the owning transcript's), so `display.ops` bumps `turn` and
/// `transcript.updated` bumps `transcript`. `run.event` is opt-in raw traffic and moves nothing.
///
/// This table is the TS broker's own bump list. In this port the counters belong to the hub and the
/// PUBLISHER moves them (`events.rs` "REPORT-ONLY, by contract", `knowledge.md` "T5b"), so the
/// broker reads it only to know which counters a `resync_required` should name when the event itself
/// declares none.
#[must_use]
pub fn kind_resources(kind: &str) -> &'static [&'static str] {
    match kind {
        "session.updated" => &["session"],
        "turn.upsert" | "turn.removed" => &["turns"],
        "display.ops" => &["turn"],
        "usage.updated" => &["usage"],
        "action.opened" | "action.closed" => &["actions"],
        "thread.upsert" => &["threads", "thread"],
        "thread.removed" => &["threads"],
        "transcript.updated" => &["transcript"],
        "tool.registry_updated" => &["session_tools"],
        "agent.registry_updated" => &["session_agents"],
        "config.updated" => &["config"],
        // `run.event` and `catalogue.updated` mint no counter of their own from this table: the
        // catalogue names its collection inside the payload (`broker.rs::catalogue_resource`).
        _ => &[],
    }
}

/// `isKindPublishedTo` (`protocol.ts:158`): the routing check's first gate.
#[must_use]
pub fn is_kind_published_to(scope: &str, kind: &str) -> bool {
    scope_publishes(scope).contains(&kind)
}

/// The ids one subscription addresses; `global` uses none of them (`StreamScopeIds`, `broker.ts:78`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamScopeIds {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
}

impl StreamScopeIds {
    /// Reads one id field by its contract name.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        match key {
            "session_id" => self.session_id.as_deref(),
            "thread_id" => self.thread_id.as_deref(),
            "turn_id" => self.turn_id.as_deref(),
            _ => None,
        }
    }

    /// Sets one id field by its contract name (the frame parser's only writer).
    pub fn set(&mut self, key: &str, value: String) {
        match key {
            "session_id" => self.session_id = Some(value),
            "thread_id" => self.thread_id = Some(value),
            "turn_id" => self.turn_id = Some(value),
            _ => {}
        }
    }

    /// `subscriptionKey` (`broker.ts:251-252`): the identity a parked replay window is stored under,
    /// i.e. the scope plus the ids it addresses, so a reconnect with a new `subscription_id` still
    /// finds the window.
    #[must_use]
    pub fn window_key(&self, scope: &str) -> String {
        format!(
            "{scope}:{}:{}:{}",
            self.session_id.clone().unwrap_or_default(),
            self.thread_id.clone().unwrap_or_default(),
            self.turn_id.clone().unwrap_or_default(),
        )
    }

    /// The hub counter key of one resource kind under these ids (`knowledge.md` "Event hub": the hub
    /// spells a child-scoped counter `session:child`, which is what makes a stream baseline read the
    /// very record the REST write stamped).
    #[must_use]
    pub fn hub_resource_id(&self, resource: &str) -> Option<String> {
        match resource {
            "thread" | "transcript" | "transcript_messages" => {
                match (&self.session_id, &self.thread_id) {
                    (Some(session), Some(thread)) => Some(format!("{session}:{thread}")),
                    (None, Some(thread)) => Some(thread.clone()),
                    _ => self.thread_id.clone(),
                }
            }
            "turn" | "turn_display" | "turn_events" => match (&self.session_id, &self.turn_id) {
                (Some(session), Some(turn)) => Some(format!("{session}:{turn}")),
                (None, Some(turn)) => Some(turn.clone()),
                _ => self.turn_id.clone(),
            },
            // `events.rs:42` `HOST_RESOURCE_KINDS` normalizes these to "no id" anyway.
            "harnesses" | "agents" | "tools" | "config" => None,
            _ => self.session_id.clone(),
        }
    }
}

/// `streamResourceIdFor` (`protocol.ts:166-190`): the id a baseline counter belongs to per kind -
/// the session id for session-scoped kinds, the thread id for `thread`/`transcript`, the turn id for
/// the turn kinds, and `null` for a host-wide resource.
///
/// Kept verbatim because the contract states it that way. The FRAMES this crate emits carry
/// [`StreamScopeIds::hub_resource_id`] instead, because the hub keys a child counter by
/// `session:child`; both spellings agree on every session-scoped and host-wide kind, and the
/// composite is what a REST read's `X-Harness-Revision` was stamped with.
#[must_use]
pub fn stream_resource_id_for(resource: &str, ids: &StreamScopeIds) -> Option<String> {
    match resource {
        "thread" | "transcript" => ids.thread_id.clone(),
        "turn" | "turn_display" | "turn_events" => ids.turn_id.clone(),
        "harnesses" | "agents" | "tools" | "config" => None,
        // Every remaining kind is session-scoped; `transcript_messages` is a LINK target only and
        // falls through to the session id, which is the id its link is addressed by.
        _ => ids.session_id.clone(),
    }
}

/// `defaultStreamLink` (`protocol.ts:197-249`), built from this crate's own mount so the path is the
/// one `server::router` actually serves. The page size is repeated INSIDE the link text, which is
/// the rule that keeps recovery one bounded page rather than a whole-session read.
#[must_use]
pub fn default_stream_link(resource: &str, ids: &StreamScopeIds) -> String {
    let session = ids.session_id.clone().unwrap_or_default();
    let thread = ids.thread_id.clone().unwrap_or_default();
    let turn = ids.turn_id.clone().unwrap_or_default();
    let root = contract::HARNESS_API_ROOT;
    // The contract's own default page size, the same one `events::baseline_link` prints.
    let page = contract::BASELINE_PAGE_LIMIT;

    match resource {
        "session" => format!("{root}/sessions/{session}"),
        "definition" => format!("{root}/sessions/{session}/definition"),
        "turns" => format!("{root}/sessions/{session}/turns?limit={page}"),
        "turn" => format!("{root}/sessions/{session}/turns/{turn}"),
        "turn_display" => format!("{root}/sessions/{session}/turns/{turn}/display?limit={page}"),
        "turn_events" => format!("{root}/sessions/{session}/turns/{turn}/events?limit={page}"),
        "threads" => format!("{root}/sessions/{session}/threads?limit={page}"),
        "thread" => format!("{root}/sessions/{session}/threads/{thread}"),
        "transcript" => format!("{root}/sessions/{session}/threads/{thread}/transcript"),
        "transcript_messages" => {
            format!("{root}/sessions/{session}/threads/{thread}/transcript/messages?limit={page}")
        }
        "actions" => format!("{root}/sessions/{session}/actions?limit={page}"),
        "usage" => format!("{root}/sessions/{session}/usage"),
        "queue" => format!("{root}/sessions/{session}/queue?limit={page}"),
        "session_agents" => format!("{root}/sessions/{session}/agents?limit={page}"),
        "session_tools" => format!("{root}/sessions/{session}/tools?limit={page}"),
        "harnesses" => format!("{root}/harnesses?limit={page}"),
        "agents" => format!("{root}/agents?limit={page}"),
        "tools" => format!("{root}/tools?limit={page}"),
        "config" => format!("{root}/config"),
        "commands" => format!("{root}/sessions/{session}/commands?limit={page}"),
        // An unknown kind still gets a valid link (the root), never a crash.
        _ => root.to_string(),
    }
}

/// The closed key set of `WsSubscriptionFilters` (`protocol.ts:357`); an unknown key is
/// `invalid_payload` at schema level, while a published key the SCOPE does not answer for is
/// `unsupported_filter` - two different conditions.
pub const FILTER_KEYS: [&str; 5] = ["status", "thread_id", "events", "sections", "context"];

// ---------------------------------------------------------------------------
// Cursors (`protocol.ts:258-286`)
//
// A cursor is `base64url(epoch:sequence)` and is NEVER parsed by a client. It is opaque to the wire
// contract but must round-trip exactly on the server, because replay compares against it.
// ---------------------------------------------------------------------------

/// A decoded cursor: the epoch it was minted in and the sequence it stands at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCursor {
    pub epoch: String,
    pub sequence: u64,
}

/// `encodeStreamCursor` (`protocol.ts:260`).
#[must_use]
pub fn encode_stream_cursor(epoch: &str, sequence: u64) -> String {
    URL_SAFE_NO_PAD.encode(format!("{epoch}:{sequence}"))
}

/// `decodeStreamCursor` (`protocol.ts:264-286`): `None` for anything that is not a well-formed
/// cursor of this epoch's shape, which the conversation treats as "no resume state".
///
/// Three deliberate differences from Node's lenient `Buffer.from(_, 'base64url')`, all of them in the
/// same direction (a cursor this server did not mint is "no resume state"):
///   * a padded or standard-alphabet cursor is still accepted (the decoders are tried in turn), so a
///     client that re-encoded our cursor with its own base64 dialect still resumes;
///   * a sequence tail that is not a plain non-negative decimal (`""`, `" 12 "`, `"12abc"`) is
///     refused, where JavaScript's `Number()` would coerce it;
///   * bytes that are not valid UTF-8 decode to `None`.
#[must_use]
pub fn decode_stream_cursor(cursor: Option<&str>) -> Option<StreamCursor> {
    let raw = cursor.filter(|value| !value.is_empty())?;

    let text = [URL_SAFE_NO_PAD, URL_SAFE, STANDARD]
        .iter()
        .find_map(|engine| engine.decode(raw).ok())
        .and_then(|bytes| String::from_utf8(bytes).ok())?;

    // The epoch may itself contain a colon in principle, so the SPLIT is on the LAST one.
    let separator = text.rfind(':')?;
    if separator == 0 {
        return None;
    }

    let epoch = &text[..separator];
    let sequence: u64 = text[separator + 1..].parse().ok()?;

    if epoch.is_empty() {
        return None;
    }

    Some(StreamCursor {
        epoch: epoch.to_owned(),
        sequence,
    })
}

/// Same, for a caller holding an owned cursor.
#[must_use]
pub fn decode_owned_cursor(cursor: &Option<String>) -> Option<StreamCursor> {
    decode_stream_cursor(cursor.as_deref())
}

// ---------------------------------------------------------------------------
// Client frame validation (precedence steps 1 and 2) - `protocol.ts:334-509`
// ---------------------------------------------------------------------------

/// A protocol condition, shaped exactly like `WsProtocolError`'s informative fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFailure {
    /// One of `contract::HARNESS_WS_PROTOCOL_CODES`.
    pub code: &'static str,
    pub message: String,
    /// When present, the server closes with it immediately after the `error` frame (the contract's
    /// "close immediately after this frame" instruction).
    pub close_code: Option<u16>,
    pub subscription_id: Option<String>,
}

impl StreamFailure {
    #[must_use]
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            close_code: None,
            subscription_id: None,
        }
    }

    /// `failure(code, message, close_code)` (`protocol.ts:361-363`).
    #[must_use]
    pub fn with_close(code: &'static str, message: impl Into<String>, close_code: u16) -> Self {
        Self {
            code,
            message: message.into(),
            close_code: Some(close_code),
            subscription_id: None,
        }
    }

    #[must_use]
    pub fn for_subscription(mut self, subscription_id: impl Into<String>) -> Self {
        self.subscription_id = Some(subscription_id.into());
        self
    }

    /// `sendError`'s rule (`connection.ts:86-99`): only `rate_limited` and a client bug are worth
    /// resending; an internal failure is the server's problem and the client cannot fix it by
    /// retrying the same frame.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.code == "rate_limited" || self.code == "internal"
    }
}

/// `StreamPace`: how a subscription wants backpressure spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamPace {
    /// Send everything, in order, unacknowledged.
    #[default]
    Ordered,
    /// Stop and wait: the server withholds frames until `ack` names the highest sequence.
    Ack,
}

impl StreamPace {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Ack => "ack",
        }
    }
}

/// A `subscribe` frame is the same shape the client sends; this is the broker's input name
/// (`StreamSubscribeRequest` / `StreamSubscribeFrame`, `broker.ts:141-148`, `:918`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub subscription_id: String,
    pub scope: String,
    pub ids: StreamScopeIds,
    /// The filter object as sent, kept as JSON so `filters_applied` echoes it back verbatim.
    pub filters: Option<Value>,
    pub pace: StreamPace,
    pub cursor: Option<String>,
}

/// One validated client frame (`WsClientMessage`, discriminated by `type`).
#[derive(Debug, Clone, PartialEq)]
pub enum ClientFrame {
    Hello {
        protocol: Option<String>,
        epoch: Option<String>,
        cursor: Option<String>,
    },
    Subscribe(SubscribeRequest),
    Unsubscribe {
        subscription_id: String,
    },
    Ack {
        subscription_id: String,
        sequence: u64,
    },
    Ping {
        at: Option<f64>,
    },
    Bye {
        reason: Option<String>,
    },
}

/// What [`parse_client_frame`] answers: a frame, or exactly one failure. Never both.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseOutcome {
    Frame(ClientFrame),
    Failure(StreamFailure),
}

/// `CLIENT_FRAME_FIELDS` (`protocol.ts:344-354`): `required` must be present, and nothing outside
/// the two lists may be (`additionalProperties: false` on every frame schema).
#[must_use]
fn client_frame_fields(
    frame_type: &str,
) -> Option<(&'static [&'static str], &'static [&'static str])> {
    match frame_type {
        "hello" => Some((&[], &["protocol", "epoch", "cursor"])),
        "subscribe" => Some((
            &["subscription_id", "scope"],
            &[
                "session_id",
                "thread_id",
                "turn_id",
                "filters",
                "pace",
                "cursor",
            ],
        )),
        "unsubscribe" => Some((&["subscription_id"], &[])),
        "ack" => Some((&["subscription_id", "sequence"], &[])),
        "ping" => Some((&[], &["at"])),
        "bye" => Some((&[], &["reason"])),
        _ => None,
    }
}

/// `isNonEmptyString` (`protocol.ts:359`).
fn non_empty_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

/// `String(value)` for the messages that quote a rejected value (`protocol.ts:410`, `:445`).
///
/// JavaScript's `String()` on a JSON value: `null` -> `"null"`, a number -> its decimal form, an
/// array -> its comma-joined items, an object -> `"[object Object]"`. Reproduced here because the
/// refusal sentence is part of the contract's observable output.
fn display_value(value: Option<&Value>) -> String {
    match value {
        // TS reaches this arm with `undefined` (an absent field) or with JSON `null`, and the two
        // print differently, so the distinction is kept.
        None => "undefined".to_owned(),
        Some(Value::Null) => "null".to_owned(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Bool(flag)) => flag.to_string(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| display_value(Some(item)))
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".to_owned(),
    }
}

/// `failure(code, message)` (`protocol.ts:361`).
fn invalid_payload(message: impl Into<String>) -> ParseOutcome {
    ParseOutcome::Failure(StreamFailure::new("invalid_payload", message))
}

/// Validate one inbound text frame (`parseClientFrame`, `protocol.ts:383-509`).
///
/// The byte size is measured on the UTF-8 encoding, which is what the wire counts; `raw.len()` IS
/// that count in Rust.
#[must_use]
pub fn parse_client_frame(raw: &str) -> ParseOutcome {
    let limits = default_ws_limits();

    // Step 1a: size, measured on bytes rather than characters.
    if raw.len() as u64 > limits.max_client_frame_bytes {
        return ParseOutcome::Failure(StreamFailure::with_close(
            "frame_too_large",
            format!(
                "A client frame cannot exceed {} bytes.",
                limits.max_client_frame_bytes
            ),
            close_codes::MESSAGE_TOO_BIG,
        ));
    }

    // Step 1b: must be a JSON text OBJECT. Note TS's two arms (unparsable text, and a parsed value
    // that is not an object) share one sentence and one close code.
    let parsed: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => {
            return ParseOutcome::Failure(StreamFailure::with_close(
                "frame_too_large",
                "A client frame must be a JSON text object.",
                close_codes::UNSUPPORTED_DATA,
            ));
        }
    };

    let fields = match parsed {
        Value::Object(fields) => fields,
        _ => {
            return ParseOutcome::Failure(StreamFailure::with_close(
                "frame_too_large",
                "A client frame must be a JSON text object.",
                close_codes::UNSUPPORTED_DATA,
            ));
        }
    };

    let Some(frame_type) = fields.get("type").and_then(Value::as_str) else {
        return invalid_payload(format!(
            "Unknown client frame type: {}.",
            display_value(fields.get("type"))
        ));
    };

    if !contract::HARNESS_WS_CLIENT_MESSAGE_TYPES.contains(&frame_type) {
        return invalid_payload(format!("Unknown client frame type: {frame_type}."));
    }

    let Some((required, optional)) = client_frame_fields(frame_type) else {
        return invalid_payload(format!("Unknown client frame type: {frame_type}."));
    };

    for key in required {
        if !fields.contains_key(*key) {
            return invalid_payload(format!("A {frame_type} frame requires {key}."));
        }
    }

    for key in fields.keys() {
        if key == "type" || optional.contains(&key.as_str()) || required.contains(&key.as_str()) {
            continue;
        }

        return invalid_payload(format!("A {frame_type} frame does not accept {key}."));
    }

    // Type-specific narrowing beyond presence: these are all schema-level enum/format rules.
    match frame_type {
        "hello" => {
            if let Some(protocol) = fields.get("protocol")
                && protocol.as_str() != Some(contract::HARNESS_WS_SUBPROTOCOL)
            {
                return invalid_payload(format!(
                    "The only supported protocol is {}.",
                    contract::HARNESS_WS_SUBPROTOCOL
                ));
            }

            if fields.contains_key("epoch") && non_empty_string(fields.get("epoch")).is_none() {
                return invalid_payload("hello.epoch must be a non-empty string.");
            }

            if fields.contains_key("cursor") && non_empty_string(fields.get("cursor")).is_none() {
                return invalid_payload("hello.cursor must be a non-empty string.");
            }

            ParseOutcome::Frame(ClientFrame::Hello {
                protocol: fields
                    .get("protocol")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                epoch: fields
                    .get("epoch")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                cursor: fields
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        }
        "subscribe" => parse_subscribe(fields),
        "unsubscribe" => match fields
            .get("subscription_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        {
            // The contract declares `subscription_id: string`; TS's hand-rolled validator only tests
            // presence, so a non-string value is reported with the same required-field sentence.
            Some(subscription_id) => {
                ParseOutcome::Frame(ClientFrame::Unsubscribe { subscription_id })
            }
            None => invalid_payload("A unsubscribe frame requires subscription_id."),
        },
        "ack" => {
            let sequence = fields
                .get("sequence")
                .and_then(Value::as_f64)
                .filter(|value| value.fract() == 0.0 && *value >= 0.0);

            let Some(sequence) = sequence else {
                return invalid_payload("ack.sequence must be a non-negative integer.");
            };

            match fields
                .get("subscription_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
            {
                Some(subscription_id) => ParseOutcome::Frame(ClientFrame::Ack {
                    subscription_id,
                    sequence: sequence as u64,
                }),
                None => invalid_payload("An ack frame requires subscription_id."),
            }
        }
        "ping" => {
            if let Some(at) = fields.get("at")
                && !at.as_f64().is_some_and(f64::is_finite)
            {
                return invalid_payload("ping.at must be a number.");
            }

            ParseOutcome::Frame(ClientFrame::Ping {
                at: fields.get("at").and_then(Value::as_f64),
            })
        }
        "bye" => {
            if let Some(reason) = fields.get("reason")
                && reason.as_str().is_none()
            {
                return invalid_payload("bye.reason must be a string.");
            }

            ParseOutcome::Frame(ClientFrame::Bye {
                reason: fields
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        }
        other => invalid_payload(format!("Unknown client frame type: {other}.")),
    }
}

/// The `subscribe` arm of the narrowing (`protocol.ts:440-482`).
fn parse_subscribe(fields: Map<String, Value>) -> ParseOutcome {
    // A scope outside the closed four is a SCHEMA failure, never a resource failure, which is why
    // the check runs here rather than in the broker.
    let Some(scope) = fields.get("scope").and_then(Value::as_str) else {
        return invalid_payload(format!(
            "Unknown subscription scope: {}.",
            display_value(fields.get("scope"))
        ));
    };

    if !contract::HARNESS_WS_SCOPES.contains(&scope) {
        return invalid_payload(format!("Unknown subscription scope: {scope}."));
    }

    let mut ids = StreamScopeIds::default();

    for key in scope_ids_required(scope) {
        let Some(value) = non_empty_string(fields.get(*key)) else {
            return invalid_payload(format!("A {scope} subscription requires {key}."));
        };

        ids.set(key, value.to_owned());
    }

    for key in scope_ids_forbidden(scope) {
        if fields.contains_key(*key) {
            return invalid_payload(format!("A {scope} subscription does not accept {key}."));
        }
    }

    for key in ["session_id", "thread_id", "turn_id"] {
        if let Some(value) = non_empty_string(fields.get(key)) {
            ids.set(key, value.to_owned());
        }
    }

    let pace = match fields.get("pace") {
        None => StreamPace::Ordered,
        Some(value) if value.as_str() == Some("ordered") => StreamPace::Ordered,
        Some(value) if value.as_str() == Some("ack") => StreamPace::Ack,
        Some(_) => {
            return invalid_payload("subscribe.pace must be \"ordered\" or \"ack\".");
        }
    };

    if fields.contains_key("cursor") && non_empty_string(fields.get("cursor")).is_none() {
        return invalid_payload("subscribe.cursor must be a non-empty string.");
    }

    if let Some(filters) = fields.get("filters") {
        let Value::Object(filters) = filters else {
            return invalid_payload("subscribe.filters must be an object.");
        };

        for key in filters.keys() {
            if !FILTER_KEYS.contains(&key.as_str()) {
                return invalid_payload(format!("subscribe.filters does not define {key}."));
            }
        }
    }

    ParseOutcome::Frame(ClientFrame::Subscribe(SubscribeRequest {
        // `subscription_id` is client-chosen and unique per connection; the schema declares a string,
        // and a non-string arrives with the required-field sentence (see `unsubscribe`'s note).
        subscription_id: match fields.get("subscription_id").and_then(Value::as_str) {
            Some(subscription_id) => subscription_id.to_owned(),
            None => return invalid_payload("A subscribe frame requires subscription_id."),
        },
        scope: scope.to_owned(),
        ids,
        filters: fields
            .get("filters")
            .filter(|value| matches!(value, Value::Object(_)))
            .cloned(),
        pace,
        cursor: fields
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }))
}

// ---------------------------------------------------------------------------
// Server frame builders (`protocol.ts:548-574` plus the shapes in harness-stream.ts)
//
// Field order is the TS spread order, and every name is snake_case. `serde_json::Map` preserves
// insertion order in this crate (`preserve_order`), so a frame reads the way TS serializes it.
// ---------------------------------------------------------------------------

/// `streamFrameBytes` (`protocol.ts:516`): the UTF-8 byte length of a serialized frame, the unit
/// `max_server_frame_bytes` is stated in.
#[must_use]
pub fn stream_frame_bytes(frame: &Value) -> usize {
    serde_json::to_string(frame).unwrap_or_default().len()
}

/// `WsWelcome`.
#[must_use]
pub fn welcome_frame(epoch: &str, at: u64, limits: &WsStreamLimits) -> Value {
    let mut frame = Map::new();

    frame.insert("type".to_owned(), Value::String("welcome".to_owned()));
    frame.insert(
        "protocol".to_owned(),
        Value::String(contract::HARNESS_WS_SUBPROTOCOL.to_owned()),
    );
    frame.insert("epoch".to_owned(), Value::String(epoch.to_owned()));
    frame.insert("at".to_owned(), Value::from(at));
    frame.insert(
        "limits".to_owned(),
        serde_json::to_value(limits).unwrap_or(Value::Null),
    );

    Value::Object(frame)
}

/// `WsProtocolError`, with `sendError`'s derived members (`connection.ts:86-99`): `retryable` is
/// computed from the code, and the optional members are ABSENT rather than null when unset.
#[must_use]
pub fn error_frame(failure: &StreamFailure) -> Value {
    let mut frame = Map::new();

    frame.insert("type".to_owned(), Value::String("error".to_owned()));
    frame.insert("code".to_owned(), Value::String(failure.code.to_owned()));
    frame.insert("message".to_owned(), Value::String(failure.message.clone()));

    if let Some(subscription_id) = &failure.subscription_id {
        frame.insert(
            "subscription_id".to_owned(),
            Value::String(subscription_id.clone()),
        );
    }

    frame.insert("retryable".to_owned(), Value::Bool(failure.is_retryable()));

    if let Some(close_code) = failure.close_code {
        frame.insert("close_code".to_owned(), Value::from(u64::from(close_code)));
    }

    Value::Object(frame)
}

/// `WsHeartbeat`.
#[must_use]
pub fn heartbeat_frame(at: u64) -> Value {
    let mut frame = Map::new();

    frame.insert("type".to_owned(), Value::String("heartbeat".to_owned()));
    frame.insert("at".to_owned(), Value::from(at));

    Value::Object(frame)
}

/// `WsPong`, echoing the `at` the client sent (`connection.ts:331-334`).
#[must_use]
pub fn pong_frame(at: f64) -> Value {
    let mut frame = Map::new();

    frame.insert("type".to_owned(), Value::String("pong".to_owned()));
    frame.insert(
        "at".to_owned(),
        Value::from(if at.fract() == 0.0 && at.abs() < u64::MAX as f64 {
            serde_json::Number::from(at as u64)
        } else {
            serde_json::Number::from_f64(at).unwrap_or_else(|| serde_json::Number::from(0))
        }),
    );

    Value::Object(frame)
}

/// The metadata every `WsUpdate` variant carries, identical across kinds
/// (`StreamUpdateEnvelope`, `protocol.ts:549-555`).
#[must_use]
pub fn stream_update_envelope(
    subscription_id: &str,
    kind: &str,
    sequence: u64,
    cursor: &str,
    at: u64,
) -> Map<String, Value> {
    let mut envelope = Map::new();

    envelope.insert("type".to_owned(), Value::String("update".to_owned()));
    envelope.insert(
        "subscription_id".to_owned(),
        Value::String(subscription_id.to_owned()),
    );
    envelope.insert("sequence".to_owned(), Value::from(sequence));
    envelope.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
    envelope.insert("kind".to_owned(), Value::String(kind.to_owned()));
    envelope.insert("at".to_owned(), Value::from(at));

    envelope
}

/// One complete `WsUpdate` frame: the envelope plus the variant's payload fields.
///
/// The envelope (`subscription_id`, `sequence`, `cursor`) is minted by the broker, never by the
/// publisher - `broker.ts:454-462` is the only place that knows which subscription a frame belongs to
/// and where its sequence stands.
#[must_use]
pub fn update_frame(
    subscription_id: &str,
    kind: &str,
    sequence: u64,
    cursor: &str,
    at: u64,
    payload: &Value,
) -> Value {
    let mut frame = stream_update_envelope(subscription_id, kind, sequence, cursor, at);

    if let Value::Object(payload) = payload {
        for (key, value) in payload {
            // The envelope wins on a collision: a payload that carried its own `sequence` (a hub
            // event's `at`, say) must not overwrite the transport's stamping.
            frame.entry(key.clone()).or_insert(value.clone());
        }
    }

    Value::Object(frame)
}

/// The input half of `WsSubscriptionState` - eight contract fields, so one named struct beats an
/// eight-parameter call (`clippy::too_many_arguments` is not the reason; a caller that reads the
/// field names is).
#[derive(Debug, Clone)]
pub struct SubscriptionStateFrame<'state> {
    pub subscription_id: &'state str,
    pub scope: &'state str,
    pub epoch: &'state str,
    pub sequence: u64,
    pub cursor: &'state str,
    pub revisions: Vec<Value>,
    pub filters_applied: Option<Value>,
    pub replayed_frames: Option<u64>,
    pub publishes: &'static [&'static str],
}

/// `WsSubscriptionState`, the reconciliation anchor for one subscription (`broker.ts:787-798`).
#[must_use]
pub fn subscription_state_frame(input: &SubscriptionStateFrame<'_>) -> Value {
    let mut frame = Map::new();

    frame.insert(
        "type".to_owned(),
        Value::String("subscription_state".to_owned()),
    );
    frame.insert(
        "subscription_id".to_owned(),
        Value::String(input.subscription_id.to_owned()),
    );
    frame.insert("scope".to_owned(), Value::String(input.scope.to_owned()));
    frame.insert("epoch".to_owned(), Value::String(input.epoch.to_owned()));
    frame.insert("sequence".to_owned(), Value::from(input.sequence));
    frame.insert("cursor".to_owned(), Value::String(input.cursor.to_owned()));
    frame.insert(
        "revisions".to_owned(),
        Value::Array(input.revisions.clone()),
    );

    if let Some(filters) = &input.filters_applied {
        frame.insert("filters_applied".to_owned(), filters.clone());
    }

    if let Some(replayed) = input.replayed_frames {
        frame.insert("replayed_frames".to_owned(), Value::from(replayed));
    }

    frame.insert(
        "publishes".to_owned(),
        Value::Array(
            input
                .publishes
                .iter()
                .map(|kind| Value::String((*kind).to_owned()))
                .collect(),
        ),
    );

    Value::Object(frame)
}

/// `WsResyncRequired` (`broker.ts:383-400`): the only recovery frame, and the only place a client is
/// told to re-read. It never carries a session, a snapshot, an event array, or a display tree.
#[must_use]
pub fn resync_frame(
    subscription_id: &str,
    reason: &str,
    epoch: &str,
    cursor: &str,
    lost_frames: Option<u64>,
    resources: Vec<Value>,
) -> Value {
    let mut frame = Map::new();

    frame.insert(
        "type".to_owned(),
        Value::String("resync_required".to_owned()),
    );
    frame.insert(
        "subscription_id".to_owned(),
        Value::String(subscription_id.to_owned()),
    );
    frame.insert("reason".to_owned(), Value::String(reason.to_owned()));
    frame.insert("epoch".to_owned(), Value::String(epoch.to_owned()));
    frame.insert("cursor".to_owned(), Value::String(cursor.to_owned()));

    // `lost_frames` is omitted unless there is something to report (`broker.ts:394`).
    if let Some(lost) = lost_frames.filter(|value| *value > 0) {
        frame.insert("lost_frames".to_owned(), Value::from(lost));
    }

    frame.insert("resources".to_owned(), Value::Array(resources));

    Value::Object(frame)
}

/// `WsResourceLink`: a pointer to the REST read that restores one resource, never the resource.
#[must_use]
pub fn resource_link(
    resource: &str,
    id: Option<&str>,
    revision: u64,
    epoch: &str,
    link: &str,
) -> Value {
    let mut entry = Map::new();

    entry.insert("resource".to_owned(), Value::String(resource.to_owned()));
    // `id` is always present and `null` for a host-wide kind, because TS declares
    // `id: string | null` on a closed object.
    entry.insert(
        "id".to_owned(),
        id.map_or(Value::Null, |value| Value::String(value.to_owned())),
    );
    entry.insert("revision".to_owned(), Value::from(revision));
    entry.insert("epoch".to_owned(), Value::String(epoch.to_owned()));
    entry.insert("link".to_owned(), Value::String(link.to_owned()));

    Value::Object(entry)
}

/// `chunkStreamPayload` (`protocol.ts:525-546`).
///
/// Split a payload array so no frame exceeds `per_frame` entries AND the frame stays under
/// `max_bytes`: the contract's promise is "the server never emits a larger frame", so an oversize
/// list becomes MORE frames, never a bigger one. A single item that is by itself over the cap stays
/// alone in its own frame - dropping it would silently lose a display op.
#[must_use]
pub fn chunk_stream_payload<T>(
    items: &[T],
    per_frame: usize,
    max_bytes: usize,
    size_of: impl Fn(&T) -> usize,
) -> Vec<Vec<T>>
where
    T: Clone,
{
    let per_frame = per_frame.max(1);
    let mut chunks: Vec<Vec<T>> = Vec::new();
    let mut chunk: Vec<T> = Vec::new();
    let mut bytes = 0_usize;

    for item in items {
        let size = size_of(item);

        if !chunk.is_empty() && (chunk.len() >= per_frame || bytes + size > max_bytes) {
            chunks.push(std::mem::take(&mut chunk));
            bytes = 0;
        }

        chunk.push(item.clone());
        bytes += size;
    }

    if !chunk.is_empty() {
        chunks.push(chunk);
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// base64url of a raw cursor body, so a test can name a malformed cursor by what it decodes to.
    fn encode_probe(body: &str) -> String {
        URL_SAFE_NO_PAD.encode(body)
    }

    /// The cursor codec must round-trip exactly, because replay compares against it
    /// (`protocol.ts:254-256`).
    #[test]
    fn a_cursor_round_trips_through_the_codec() {
        let cursor = encode_stream_cursor("abc123-xyz", 7);

        assert_eq!(
            decode_stream_cursor(Some(&cursor)),
            Some(StreamCursor {
                epoch: "abc123-xyz".to_owned(),
                sequence: 7,
            })
        );
    }

    /// A cursor is split on the LAST colon, so an epoch containing one still decodes.
    #[test]
    fn the_sequence_is_split_on_the_last_colon() {
        let cursor = encode_stream_cursor("epoch:with:colons", 42);

        assert_eq!(
            decode_stream_cursor(Some(&cursor)),
            Some(StreamCursor {
                epoch: "epoch:with:colons".to_owned(),
                sequence: 42,
            })
        );
    }

    /// Anything this server did not mint is "no resume state", never a partially-decoded cursor.
    #[test]
    fn a_foreign_cursor_is_no_resume_state() {
        let empty_epoch = encode_probe(":3");
        let empty_tail = encode_probe("abc:");
        let cases = [
            "".to_owned(),
            "not-base64-%^&".to_owned(),
            ":".to_owned(),
            empty_epoch,
            empty_tail,
        ];

        for raw in &cases {
            assert_eq!(
                decode_stream_cursor(Some(raw)),
                None,
                "`{raw}` is not a cursor of this shape"
            );
        }
    }

    /// The empty epoch is refused the same way TS refuses it (`protocol.ts:283`).
    #[test]
    fn an_empty_epoch_is_refused() {
        let cursor = encode_probe(":9");

        assert_eq!(decode_stream_cursor(Some(&cursor)), None);
    }

    /// `resolveStreamLimits` clamps into the declared bounds and never moves a fixed number.
    #[test]
    fn only_the_six_tunable_numbers_move_and_only_inside_their_bounds() {
        let defaults = default_ws_limits();
        let overrides = BTreeMap::from([
            ("subscriptions_per_connection".to_owned(), 5_000),
            ("ack_timeout_ms".to_owned(), 10),
            ("replay_frames".to_owned(), 4_000),
            ("heartbeat_interval_ms".to_owned(), 1),
        ]);
        let limits = resolve_stream_limits(&overrides);

        assert_eq!(limits.subscriptions_per_connection, 256);
        assert_eq!(limits.ack_timeout_ms, 1_000);
        // The two fixed numbers in the same map are ignored outright.
        assert_eq!(limits.replay_frames, defaults.replay_frames);
        assert_eq!(limits.heartbeat_interval_ms, defaults.heartbeat_interval_ms);
        assert_eq!(limits.heartbeat_timeout_close_code, 4_408);
        // Every number the map did not name is still the contract's.
        assert_eq!(limits.client_messages_per_second, 32);
        assert_eq!(limits.send_queue_frames, 256);
        assert_eq!(limits.max_server_frame_bytes, 65_536);
    }

    /// A frame over `max_client_frame_bytes` is `frame_too_large` with close 1009, before any schema
    /// question is asked (precedence step 1).
    #[test]
    fn an_oversize_frame_is_frame_too_large() {
        let raw = format!("{{\"type\":\"ping\",\"at\":{}}}", "1".repeat(17_000));
        let outcome = parse_client_frame(&raw);

        assert_eq!(
            outcome,
            ParseOutcome::Failure(StreamFailure::with_close(
                "frame_too_large",
                "A client frame cannot exceed 16384 bytes.",
                close_codes::MESSAGE_TOO_BIG,
            ))
        );
    }

    /// Non-JSON text and a JSON array are the same condition with the same close code (1003).
    #[test]
    fn a_frame_must_be_a_json_object() {
        for raw in ["not json", "[1,2]", "\"text\"", "42"] {
            assert_eq!(
                parse_client_frame(raw),
                ParseOutcome::Failure(StreamFailure::with_close(
                    "frame_too_large",
                    "A client frame must be a JSON text object.",
                    close_codes::UNSUPPORTED_DATA,
                )),
                "`{raw}` is not a JSON object"
            );
        }
    }

    /// An unknown tag is a schema failure with no close code (the connection stays open).
    #[test]
    fn an_unknown_frame_type_is_invalid_payload() {
        assert_eq!(
            parse_client_frame(r#"{"type":"nope"}"#),
            invalid_payload("Unknown client frame type: nope.")
        );
        assert_eq!(
            parse_client_frame(r#"{"type":null}"#),
            invalid_payload("Unknown client frame type: null.")
        );
    }

    /// Every frame schema is `additionalProperties: false`, so a stray field is refused by name.
    #[test]
    fn a_stray_field_is_refused_by_name() {
        assert_eq!(
            parse_client_frame(r#"{"type":"pong"}"#),
            invalid_payload("Unknown client frame type: pong.")
        );
        assert_eq!(
            parse_client_frame(r#"{"type":"ping","at":1,"extra":2}"#),
            invalid_payload("A ping frame does not accept extra.")
        );
    }

    /// The per-scope id contract is enforced at schema level, in the contract's own wording.
    #[test]
    fn the_scope_id_contract_is_enforced() {
        assert_eq!(
            parse_client_frame(
                r#"{"type":"subscribe","subscription_id":"s","scope":"thread","session_id":"a"}"#
            ),
            invalid_payload("A thread subscription requires thread_id.")
        );
        assert_eq!(
            parse_client_frame(
                r#"{"type":"subscribe","subscription_id":"s","scope":"thread","session_id":"a","thread_id":"b","turn_id":"c"}"#
            ),
            invalid_payload("A thread subscription does not accept turn_id.")
        );
        assert_eq!(
            parse_client_frame(
                r#"{"type":"subscribe","subscription_id":"s","scope":"nope","session_id":"a"}"#
            ),
            invalid_payload("Unknown subscription scope: nope.")
        );
        assert_eq!(
            parse_client_frame(
                r#"{"type":"subscribe","subscription_id":"s","scope":"session","session_id":"a","pace":"burst"}"#
            ),
            invalid_payload("subscribe.pace must be \"ordered\" or \"ack\".")
        );
        assert_eq!(
            parse_client_frame(
                r#"{"type":"subscribe","subscription_id":"s","scope":"session","session_id":"a","filters":{"nope":1}}"#
            ),
            invalid_payload("subscribe.filters does not define nope.")
        );
    }

    /// The narrowing rules that carry a value rather than a presence test.
    #[test]
    fn per_type_narrowing_reports_its_own_sentence() {
        assert_eq!(
            parse_client_frame(r#"{"type":"hello","protocol":"harness.v2"}"#),
            invalid_payload("The only supported protocol is harness.v1.")
        );
        assert_eq!(
            parse_client_frame(r#"{"type":"hello","cursor":""}"#),
            invalid_payload("hello.cursor must be a non-empty string.")
        );
        assert_eq!(
            parse_client_frame(r#"{"type":"ack","subscription_id":"s","sequence":-1}"#),
            invalid_payload("ack.sequence must be a non-negative integer.")
        );
        assert_eq!(
            parse_client_frame(r#"{"type":"ack","subscription_id":"s","sequence":1.5}"#),
            invalid_payload("ack.sequence must be a non-negative integer.")
        );
        assert_eq!(
            parse_client_frame(r#"{"type":"ping","at":"now"}"#),
            invalid_payload("ping.at must be a number.")
        );
        assert_eq!(
            parse_client_frame(r#"{"type":"bye","reason":7}"#),
            invalid_payload("bye.reason must be a string.")
        );
    }

    /// A well-formed subscribe becomes the broker's request shape.
    #[test]
    fn a_valid_subscribe_parses_into_the_request() {
        let ParseOutcome::Frame(ClientFrame::Subscribe(request)) = parse_client_frame(
            r#"{"type":"subscribe","subscription_id":"s1","scope":"turn","session_id":"a","turn_id":"t","pace":"ack","cursor":"abc","filters":{"sections":false}}"#,
        ) else {
            panic!("the frame is valid");
        };

        assert_eq!(request.scope, "turn");
        assert_eq!(request.pace, StreamPace::Ack);
        assert_eq!(request.cursor.as_deref(), Some("abc"));
        assert_eq!(request.ids.session_id.as_deref(), Some("a"));
        assert_eq!(request.ids.turn_id.as_deref(), Some("t"));
        assert_eq!(request.ids.thread_id, None);
        assert_eq!(request.ids.window_key("turn"), "turn:a::t");
    }

    /// The baseline sets are the contract's, in the contract's order, and every one of them is
    /// within `revision_records_per_state`.
    #[test]
    fn the_scope_tables_match_the_contract() {
        assert_eq!(
            scope_baselines("session"),
            [
                "session",
                "definition",
                "turns",
                "queue",
                "actions",
                "usage",
                "threads",
                "session_agents",
                "session_tools",
                "commands"
            ]
        );
        assert_eq!(
            scope_publishes("global"),
            ["catalogue.updated", "config.updated"]
        );
        assert_eq!(scope_filters("turn"), ["sections", "context"]);
        assert_eq!(scope_ids_forbidden("session"), ["thread_id", "turn_id"]);
        assert_eq!(kind_resources("thread.upsert"), ["threads", "thread"]);
        assert!(kind_resources("run.event").is_empty());
        assert!(is_kind_published_to("session", "run.event"));
        assert!(!is_kind_published_to("turn", "run.event"));
        for scope in contract::HARNESS_WS_SCOPES {
            assert!(
                (scope_baselines(scope).len() as u64)
                    <= default_ws_limits().revision_records_per_state
            );
        }
    }

    /// `defaultStreamLink` is bounded by construction and matches the mount this router serves.
    #[test]
    fn the_default_links_quote_the_mount_and_a_page_size() {
        let ids = StreamScopeIds {
            session_id: Some("s-1".to_owned()),
            thread_id: Some("t-1".to_owned()),
            turn_id: Some("u-1".to_owned()),
        };

        assert_eq!(
            default_stream_link("session", &ids),
            "/v1/harness/sessions/s-1"
        );
        assert_eq!(
            default_stream_link("turns", &ids),
            "/v1/harness/sessions/s-1/turns?limit=25"
        );
        assert_eq!(
            default_stream_link("turn_events", &ids),
            "/v1/harness/sessions/s-1/turns/u-1/events?limit=25"
        );
        assert_eq!(
            default_stream_link("transcript_messages", &ids),
            "/v1/harness/sessions/s-1/threads/t-1/transcript/messages?limit=25"
        );
        assert_eq!(default_stream_link("config", &ids), "/v1/harness/config");
        assert_eq!(
            default_stream_link("unknown-kind", &ids),
            "/v1/harness",
            "an unknown kind still gets a valid link, never a crash"
        );

        assert_eq!(stream_resource_id_for("harnesses", &ids), None);
        assert_eq!(
            stream_resource_id_for("thread", &ids).as_deref(),
            Some("t-1")
        );
        assert_eq!(
            ids.hub_resource_id("thread").as_deref(),
            Some("s-1:t-1"),
            "the hub keys a child counter by `session:child`"
        );
    }

    /// The update envelope is the shared metadata every variant carries.
    #[test]
    fn the_update_envelope_carries_the_shared_metadata() {
        let frame = update_frame(
            "s1",
            "session.updated",
            3,
            "cursor",
            1_700_000_000_000,
            &serde_json::json!({ "session": { "id": "s-1", "revision": 4 } }),
        );
        let frame = frame.as_object().expect("an object frame").clone();

        assert_eq!(
            frame,
            serde_json::json!({
                "type": "update",
                "subscription_id": "s1",
                "sequence": 3,
                "cursor": "cursor",
                "kind": "session.updated",
                "at": 1_700_000_000_000_u64,
                "session": { "id": "s-1", "revision": 4 },
            })
            .as_object()
            .expect("the expected object")
            .clone()
        );
    }

    /// Chunking turns a big list into more frames, never a bigger one.
    #[test]
    fn chunking_caps_both_count_and_bytes() {
        let items: Vec<u8> = (0..100).collect();

        assert_eq!(
            chunk_stream_payload(&items, 32, usize::MAX, |_| 1),
            vec![
                (0..32).collect::<Vec<u8>>(),
                (32..64).collect::<Vec<u8>>(),
                (64..96).collect::<Vec<u8>>(),
                (96..100).collect::<Vec<u8>>(),
            ]
        );

        // A byte cap that only fits two items per chunk turns 100 items into 50 frames, and no item
        // is ever dropped.
        assert_eq!(
            chunk_stream_payload(&items, 32, 4, |_| 2),
            (0..50)
                .map(|chunk| vec![
                    u8::try_from(chunk * 2).expect("in range"),
                    u8::try_from(chunk * 2 + 1).expect("in range")
                ])
                .collect::<Vec<Vec<u8>>>()
        );

        // One item larger than the whole cap stays alone in its own frame: dropping it would silently
        // lose a display op.
        assert_eq!(
            chunk_stream_payload(&items, 32, 1, |_| 3),
            items
                .iter()
                .map(|item| vec![*item])
                .collect::<Vec<Vec<u8>>>()
        );
    }

    /// `welcome` carries the protocol, the epoch and every number in force (`WsWelcome`).
    #[test]
    fn the_welcome_frame_is_the_closed_object_the_contract_declares() {
        let frame = welcome_frame("epoch-1", 1_700_000_000_000, &default_ws_limits());
        let keys: Vec<&str> = frame
            .as_object()
            .expect("an object")
            .keys()
            .map(String::as_str)
            .collect();

        assert_eq!(keys, ["type", "protocol", "epoch", "at", "limits"]);
        assert_eq!(frame["protocol"], serde_json::json!("harness.v1"));
        assert_eq!(frame["limits"]["replay_frames"], serde_json::json!(512));
        assert_eq!(
            frame["limits"]["heartbeat_timeout_close_code"],
            serde_json::json!(4408)
        );
    }

    /// `error` omits the optional members and derives `retryable`.
    #[test]
    fn the_error_frame_omits_what_is_absent() {
        let frame = error_frame(&StreamFailure::for_subscription(
            StreamFailure::new("unknown_resource", "No session matches s-9."),
            "s1",
        ));

        assert_eq!(
            frame,
            serde_json::json!({
                "type": "error",
                "code": "unknown_resource",
                "message": "No session matches s-9.",
                "subscription_id": "s1",
                "retryable": false,
            })
        );

        let rate_limited = error_frame(&StreamFailure::with_close(
            "rate_limited",
            "A client may send at most 32 messages per second.",
            close_codes::RATE_LIMITED,
        ));

        assert_eq!(rate_limited["retryable"], serde_json::json!(true));
        assert_eq!(rate_limited["close_code"], serde_json::json!(4429));
        assert!(
            !rate_limited
                .as_object()
                .expect("an object")
                .contains_key("subscription_id")
        );
    }

    /// `resync_required` reports `lost_frames` only when there is a loss to report.
    #[test]
    fn the_resync_frame_omits_a_zero_loss() {
        let frame = resync_frame("s1", "epoch_mismatch", "epoch-1", "cur", None, Vec::new());
        let object = frame.as_object().expect("an object");

        assert!(!object.contains_key("lost_frames"));
        assert_eq!(frame["reason"], serde_json::json!("epoch_mismatch"));

        let dropped = resync_frame(
            "s1",
            "frames_dropped",
            "epoch-1",
            "cur",
            Some(4),
            Vec::new(),
        );

        assert_eq!(dropped["lost_frames"], serde_json::json!(4));
    }
}
