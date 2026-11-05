//! The server-side event hub: per-process EPOCH + RESOURCE REVISIONS, in one place.
//!
//! TypeScript origin: `packages/underload/harness/src/server/api-events.ts` (T13). Read that
//! file's header before changing anything here; the summary below is the part the Rust port
//! has to keep true.
//!
//! WHY this module exists (`api-events.ts:5-38`): every collection page, every resource
//! read, and every stream frame carries a revision counter that is only comparable WITHIN one
//! server epoch, and BOTH transport owners need the exact same numbers - REST stamps
//! `PageMeta.revision`, `SessionResource.revision`, `TurnResource.revision`, receipts, and the
//! `X-Harness-Epoch` / `X-Harness-Revision` headers, while the stream answers
//! `hello`/`welcome` with the epoch, resolves `subscribe` baselines, and refuses stale cursors
//! with `resync_required`. One process, one counter set, so a REST read and a stream frame for
//! the same resource can never disagree.
//!
//! The counters are IN-MEMORY on purpose: the epoch changes on every boot, so a durable
//! counter would only ever add a restart-vs-restart comparison the contract forbids.
//!
//! KEY SPACE (`api-events.ts:24-30`, verbatim): a counter is keyed by `(resource, id)` where
//! `id` is the instance id or `None` for a host-wide resource. The flat Map key is
//! `"{kind}:{id}"` with an EMPTY id segment, so a host-wide kind and an omitted id collide by
//! design ([`EventHub::resource_key`]).
//!
//! SEMANTICS (`api-events.ts:299-339`): the PUBLISHER owns its counters. A write calls
//! [`EventHub::bump`] for every resource it moves FIRST and then calls [`EventHub::publish`],
//! which only REPORTS the revisions as they now stand and fans the change out. That order is
//! what makes a `subscription_state` frame and a `PageMeta.revision` agree, and it is why
//! `publish` must never advance a counter itself: publishing would otherwise double-count every
//! write that bumped first.

use serde::Serialize;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::contract::{BASELINE_PAGE_LIMIT, HARNESS_API_ROOT};

/// The kinds whose counter is host-wide, so any supplied id collapses to `None`
/// (`api-events.ts:99` `HOST_KINDS`).
pub const HOST_RESOURCE_KINDS: [&str; 4] = ["harnesses", "agents", "tools", "config"];

/// The revision key of one resource instance (`api-events.ts:93-96` `HarnessResourceKey`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ResourceRef {
    /// A `StreamResourceKind` value; kept as a string so the hub is not coupled to one enum
    /// spelling and the TS vocabulary can be passed through verbatim.
    pub resource: String,
    /// `None` for a host-wide kind. ALWAYS serialized as `id: null`, because TS declares
    /// `id: string | null` on a closed object (`api-events.ts:95`); omitting the key would be
    /// a contract violation, not a shorter encoding.
    #[serde(default)]
    pub id: Option<String>,
}

impl ResourceRef {
    #[must_use]
    pub fn new(resource: impl Into<String>, id: Option<impl Into<String>>) -> Self {
        Self {
            resource: resource.into(),
            id: id.map(Into::into),
        }
    }

    /// A host-wide reference (no instance id at all).
    #[must_use]
    pub fn host(resource: impl Into<String>) -> Self {
        Self::new(resource, None::<String>)
    }

    /// The `(resource, id)` pair with `undefined`/`''`/host-kind normalized
    /// (`api-events.ts:102-105` `resourceKeyOf`).
    #[must_use]
    pub fn normalized(&self) -> Self {
        Self {
            resource: self.resource.clone(),
            id: EventHub::normalize_id(&self.resource, self.id.as_deref()),
        }
    }
}

/// One settled counter, i.e. TS `WsRevisionRecord` (`api-events.ts:315`): the realtime owner
/// hands these straight to `welcome` and `subscription_state`, so `id` is ALWAYS present and
/// `null` for a host-wide kind (no `skip_serializing_if` below may be added).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct RevisionRecord {
    pub resource: String,
    pub id: Option<String>,
    pub revision: u64,
    pub epoch: String,
}

/// One published change (`api-events.ts:112-122` `HarnessPublishedUpdate` plus the epoch,
/// timestamp and settled revisions a listener receives, i.e. `HarnessStreamEvent` at `:125`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HubEvent {
    /// A `WsUpdateKind` value, e.g. `turn.upsert`.
    pub kind: String,
    /// The resources this change moved. Their counters were bumped by the PUBLISHER before
    /// this event was built; `publish` only reports them (`api-events.ts:114`, `:302-306`).
    pub resources: Vec<ResourceRef>,
    /// Routing ids the update body addresses. TS carries these inside a composite
    /// `session:child` target id; the Rust hub keeps them separate so no stage has to split
    /// and re-join a string to route a frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    /// The update body. `seq`/`revision` are the transport's to stamp, so they are absent here
    /// (`api-events.ts:118`).
    pub payload: serde_json::Value,
    /// Compiled display ops for one turn, when this change folds the display tree
    /// (`api-events.ts:120-121` `ops?: TurnDisplayOp[]`). Kept as JSON because the display op
    /// union is the display stage's model; the hub only carries it to the transport.
    ///
    /// Empty means "no ops", which TS spells by ABSENTING the optional member, so the wire
    /// skips it; the Rust field is always present so a listener never has to unwrap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ops: Vec<serde_json::Value>,
    /// Wall-clock milliseconds, the same clock the pages stamp `observed_at` with.
    pub at: u64,
    pub epoch: String,
    /// One record per touched resource, carrying the revision the publisher's bumps produced.
    pub revisions: Vec<RevisionRecord>,
}

/// One attached listener: the id is what makes removal exact rather than positional, which TS
/// gets for free from a `Set` of function references.
type ListenerEntry = (u64, Arc<dyn Fn(&HubEvent) + Send + Sync>);

/// The hub's mutable state, behind one `Arc` so a [`HubSubscription`] can detach itself after
/// the hub it came from is dropped by its last owner.
struct HubInner {
    epoch: String,
    counters: Mutex<HashMap<String, u64>>,
    /// Insertion-ordered listeners.
    listeners: Mutex<Vec<ListenerEntry>>,
    next_listener_id: AtomicU64,
}

/// The per-process epoch + revision plane.
#[derive(Clone)]
pub struct EventHub {
    inner: Arc<HubInner>,
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHub {
    /// Build one hub, minting the process epoch (`api-events.ts:261`).
    ///
    /// `createHarnessApi` owns exactly one per API instance - which is one per process in
    /// every real deployment - and the stream reaches the SAME object, so REST and WS can
    /// never disagree about a revision. A test that wants independent counters (e.g. for two
    /// epochs) builds a second hub.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HubInner {
                // THE process epoch, not a per-hub one: the contract's one-comparison rule
                // requires that every surface of this process reports the same string
                // (`api-events.ts:82-83`, `stream/mount.ts:304`).
                epoch: process_epoch().to_owned(),
                counters: Mutex::new(HashMap::new()),
                listeners: Mutex::new(Vec::new()),
                next_listener_id: AtomicU64::new(1),
            }),
        }
    }

    /// This process's epoch, verbatim and never derived from a session (`api-events.ts:282`).
    #[must_use]
    pub fn epoch(&self) -> &str {
        &self.inner.epoch
    }

    /// Normalize an id so `''` and a host-wide kind collapse to the same key
    /// (`api-events.ts:102-105`).
    #[must_use]
    pub fn normalize_id(resource: &str, id: Option<&str>) -> Option<String> {
        if HOST_RESOURCE_KINDS.contains(&resource) {
            return None;
        }

        id.filter(|value| !value.is_empty()).map(str::to_owned)
    }

    /// The flat Map key of a normalized reference (`api-events.ts:267` `keyOf`): a `None` id
    /// is the EMPTY segment, so a host-wide kind and an omitted id collide by design.
    #[must_use]
    pub fn resource_key(resource: &str, id: Option<&str>) -> String {
        format!(
            "{resource}:{}",
            Self::normalize_id(resource, id).unwrap_or_default()
        )
    }

    /// The current revision of one resource instance; untracked resources read as 0, meaning
    /// "never changed" (`api-events.ts:269` `read`).
    #[must_use]
    pub fn revision(&self, resource: &str, id: Option<&str>) -> u64 {
        let key = Self::resource_key(resource, id);
        let counters = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        counters.get(&key).copied().unwrap_or(0)
    }

    /// Advance one resource's counter and return the new value (`api-events.ts:271-278`
    /// `bumpRevision`). This is the ONLY way a counter moves: every write bumps each resource
    /// it touches and THEN publishes, because `publish` merely reports (`:302-306`).
    #[must_use]
    pub fn bump(&self, resource: &str, id: Option<&str>) -> u64 {
        let key = Self::resource_key(resource, id);
        let mut counters = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let next = counters.get(&key).copied().unwrap_or(0) + 1;
        counters.insert(key, next);

        next
    }

    /// `RevisionRecord`s for the given references, in the order given, for
    /// `welcome`/`subscription_state` (`api-events.ts:287-297`).
    #[must_use]
    pub fn revisions(&self, resources: &[ResourceRef]) -> Vec<RevisionRecord> {
        resources
            .iter()
            .map(|reference| self.revision_record(&reference.normalized()))
            .collect()
    }

    /// Publish one change: report the settled revision of every listed resource, then fan out
    /// synchronously (`api-events.ts:299-339`).
    ///
    /// REPORT-ONLY, by contract: the PUBLISHER owns its counters and must have called
    /// [`EventHub::bump`] for each listed resource BEFORE this call (`api-events.ts:302-306`).
    /// Because publishing never advances a counter, a write that bumps and then publishes
    /// moves each counter exactly once, and the numbers reported here are the same numbers a
    /// `subscription_state` frame and a `PageMeta.revision` carry.
    #[must_use]
    pub fn publish(
        &self,
        kind: &str,
        resources: &[ResourceRef],
        session_id: Option<&str>,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        payload: serde_json::Value,
    ) -> HubEvent {
        // TS `publish` is one function with an optional `ops`; the Rust split keeps the 90%
        // call sites short and is still report-only in both forms.
        self.publish_with_ops(
            kind,
            resources,
            session_id,
            thread_id,
            turn_id,
            payload,
            Vec::new(),
        )
    }

    /// [`EventHub::publish`] plus the compiled display ops a change may carry next to its
    /// payload (`api-events.ts:120-121` `ops?: TurnDisplayOp[]`, consumed by `display.ops`).
    /// An empty `ops` is TS's "absent", so the wire skips it (see [`HubEvent::ops`]).
    #[must_use]
    // The hub stays call-shaped like TS `publish(update)` rather than inventing a builder
    // type; the routing ids are three separate values on purpose (see `HubEvent`).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::missing_panics_doc)] // every lock below recovers from poisoning; see `revision`.
    pub fn publish_with_ops(
        &self,
        kind: &str,
        resources: &[ResourceRef],
        session_id: Option<&str>,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        payload: serde_json::Value,
        ops: Vec<serde_json::Value>,
    ) -> HubEvent {
        let at = now_millis();

        // Normalized ONCE, so the event's `resources` and its `revisions` name exactly the
        // keys the counters are stored under (`api-events.ts:307-316`).
        let normalized: Vec<ResourceRef> = resources.iter().map(ResourceRef::normalized).collect();
        let revisions = self.read_records(&normalized);

        let event = HubEvent {
            kind: kind.to_owned(),
            resources: normalized,
            session_id: session_id.map(str::to_owned),
            thread_id: thread_id.map(str::to_owned),
            turn_id: turn_id.map(str::to_owned),
            payload,
            ops,
            at,
            epoch: self.inner.epoch.clone(),
            revisions,
        };

        self.fan_out(&event);

        event
    }

    /// Reads each listed key WITHOUT advancing it, so the event and its revision records
    /// describe the same instant. This is TS `read(normalized)` inside `publish`
    /// (`api-events.ts:313`) - never a bump.
    fn read_records(&self, references: &[ResourceRef]) -> Vec<RevisionRecord> {
        let counters = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        references
            .iter()
            .map(|reference| {
                let key = Self::resource_key(&reference.resource, reference.id.as_deref());

                RevisionRecord {
                    resource: reference.resource.clone(),
                    id: reference.id.clone(),
                    revision: counters.get(&key).copied().unwrap_or(0),
                    epoch: self.inner.epoch.clone(),
                }
            })
            .collect()
    }

    /// Synchronous, ISOLATED fan-out (`api-events.ts:325-336`): one listener panicking must
    /// not lose the change for the others, and the publisher (an HTTP write) never waits on a
    /// slow transport.
    ///
    /// The listener list is cloned first so a listener may subscribe, unsubscribe, or publish
    /// without dead-locking this hub. A panicking listener is dropped rather than retried,
    /// because nothing here can rewind a mutation that already committed.
    ///
    /// NOTE: panic isolation needs unwinding. The release profile of this crate is
    /// `panic = "abort"` (a process that loses a listener's panic is a broken deployment, not
    /// a recoverable one), so in release a panicking listener still takes the process down and
    /// the isolation below is the debug/test behaviour.
    fn fan_out(&self, event: &HubEvent) {
        let live: Vec<ListenerEntry> = {
            let listeners = self
                .inner
                .listeners
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            listeners.clone()
        };

        let mut panicked: Vec<u64> = Vec::new();

        for (id, listener) in &live {
            let result = catch_unwind(AssertUnwindSafe(|| listener(event)));
            if result.is_err() {
                panicked.push(*id);
            }
        }

        if panicked.is_empty() {
            return;
        }

        let mut listeners = self
            .inner
            .listeners
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        listeners.retain(|(id, _)| !panicked.contains(id));
    }

    /// Attach a listener; the returned [`HubSubscription`] detaches it exactly once
    /// (`api-events.ts:341-352`).
    #[must_use]
    pub fn subscribe(&self, listener: Arc<dyn Fn(&HubEvent) + Send + Sync>) -> HubSubscription {
        let id = self.inner.next_listener_id.fetch_add(1, Ordering::Relaxed);

        {
            let mut listeners = self
                .inner
                .listeners
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            listeners.push((id, Arc::clone(&listener)));
        }

        HubSubscription {
            inner: Arc::clone(&self.inner),
            id,
        }
    }

    /// How many listeners are attached (diagnostics/tests only, `api-events.ts:366`).
    #[must_use]
    pub fn listener_count(&self) -> usize {
        self.inner
            .listeners
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    /// The bounded REST read that reconstructs one resource (`api-events.ts:354-364`
    /// `baseline`), i.e. TS `WsResourceLink`.
    #[must_use]
    pub fn baseline(&self, resource: &str, id: Option<&str>) -> serde_json::Value {
        let reference = ResourceRef::new(resource, id).normalized();
        let revision = self.revision(&reference.resource, reference.id.as_deref());

        serde_json::json!({
            "resource": reference.resource,
            "id": reference.id,
            "revision": revision,
            "epoch": self.inner.epoch,
            "link": baseline_link(resource, id),
        })
    }

    fn revision_record(&self, reference: &ResourceRef) -> RevisionRecord {
        RevisionRecord {
            resource: reference.resource.clone(),
            id: reference.id.clone(),
            revision: self.revision(&reference.resource, reference.id.as_deref()),
            epoch: self.inner.epoch.clone(),
        }
    }
}

/// A live listener; dropping it detaches exactly like the TS unsubscribe closure.
pub struct HubSubscription {
    inner: Arc<HubInner>,
    id: u64,
}

impl HubSubscription {
    /// Detaches the listener eagerly and consumes the guard (TS returns the detach closure;
    /// here the same effect is a method on the token).
    pub fn detach(self) {
        drop(self);
    }

    /// The listener id, for diagnostics.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for HubSubscription {
    fn drop(&mut self) {
        let mut listeners = self
            .inner
            .listeners
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        listeners.retain(|(id, _)| *id != self.id);
    }
}

/// Baseline + link table (`api-events.ts:174-239` `resourceBaselineLink`).
///
/// `id` follows the hub's composite spelling for the child-scoped kinds
/// (`session:turn`, `session:thread`), which is what makes a stream baseline read the very
/// record the REST write stamped. The page size is repeated INSIDE the link text exactly as
/// the spec requires ("a concrete path including its page size"), and it is the contract's own
/// default so a resync lands on one bounded first page rather than an unbounded aggregate.
#[must_use]
pub fn baseline_link(resource: &str, id: Option<&str>) -> String {
    let reference = ResourceRef::new(resource, id).normalized();
    let key = reference.id.unwrap_or_default();
    let page = BASELINE_PAGE_LIMIT;

    match resource {
        "session" => format!("{HARNESS_API_ROOT}/sessions/{key}"),
        "definition" => format!("{HARNESS_API_ROOT}/sessions/{key}/definition"),
        "turns" => format!("{HARNESS_API_ROOT}/sessions/{key}/turns?limit={page}"),
        "turn" | "turn_display" | "turn_events" => {
            // A turn key carries `session:turn`; both halves are needed to build the path.
            let (session_id, turn_id) = split_composite(&key);

            match resource {
                "turn" => format!("{HARNESS_API_ROOT}/sessions/{session_id}/turns/{turn_id}"),
                "turn_display" => {
                    format!("{HARNESS_API_ROOT}/sessions/{session_id}/turns/{turn_id}/display")
                }
                _ => format!(
                    "{HARNESS_API_ROOT}/sessions/{session_id}/turns/{turn_id}/events?limit={page}"
                ),
            }
        }
        "threads" => format!("{HARNESS_API_ROOT}/sessions/{key}/threads?limit={page}"),
        "thread" => {
            let (session_id, thread_id) = split_composite(&key);

            format!("{HARNESS_API_ROOT}/sessions/{session_id}/threads/{thread_id}")
        }
        "transcript" | "transcript_messages" => {
            let (session_id, thread_id) = split_composite(&key);

            if resource == "transcript" {
                format!("{HARNESS_API_ROOT}/sessions/{session_id}/threads/{thread_id}/transcript")
            } else {
                format!(
                    "{HARNESS_API_ROOT}/sessions/{session_id}/threads/{thread_id}/transcript/messages?limit={page}"
                )
            }
        }
        "actions" => format!("{HARNESS_API_ROOT}/sessions/{key}/actions"),
        "usage" => format!("{HARNESS_API_ROOT}/sessions/{key}/usage"),
        "queue" => format!("{HARNESS_API_ROOT}/sessions/{key}/queue?limit={page}"),
        "session_agents" => format!("{HARNESS_API_ROOT}/sessions/{key}/agents"),
        "session_tools" => format!("{HARNESS_API_ROOT}/sessions/{key}/tools?limit={page}"),
        "commands" => format!("{HARNESS_API_ROOT}/sessions/{key}/commands?limit={page}"),
        "harnesses" => format!("{HARNESS_API_ROOT}/harnesses?limit={page}"),
        "agents" => format!("{HARNESS_API_ROOT}/agents?limit={page}"),
        "tools" => format!("{HARNESS_API_ROOT}/tools?limit={page}"),
        "config" => format!("{HARNESS_API_ROOT}/config"),
        // An unknown kind still gets a valid link (the root), never a crash: the realtime
        // owner treats an unresolvable baseline as "restart from REST" (`api-events.ts:234`).
        _ => HARNESS_API_ROOT.to_string(),
    }
}

/// Split the `parent:child` id a turn/thread-scoped kind carries (`api-events.ts:242-250`).
///
/// A value with no separator has an EMPTY parent and the whole value as the child - the TS
/// default does the same, so a bare id still produces a valid path instead of a panic.
fn split_composite(value: &str) -> (String, String) {
    match value.split_once(':') {
        Some((parent, child)) => (parent.to_owned(), child.to_owned()),
        None => (String::new(), value.to_owned()),
    }
}

/// Convenience for the common "one session moved" publish (`api-events.ts:371-374`), kept so
/// the handlers stay readable.
#[must_use]
pub fn session_resources(session_id: &str, kinds: &[&str]) -> Vec<ResourceRef> {
    kinds
        .iter()
        .map(|kind| ResourceRef::new(*kind, Some(session_id)).normalized())
        .collect()
}

/// The opaque per-process epoch (`api-events.ts:65-83`).
///
/// Format: base36 timestamp + a random suffix, minted once per process and never re-minted, so
/// every read, header, receipt, welcome frame, and cursor in this process's lifetime carries
/// this exact string. It is OPAQUE to clients (the contract only requires a string that
/// changes on restart) and short, because it rides every cursor.
///
/// TS takes 6 random bytes and renders each as 2 base36 digits, zero-padded; the Rust port
/// does exactly the same and uses `uuid`'s v4 allocator as the byte source, because this crate
/// declares no `rand`. The suffix is therefore `[0-9a-z]{12}`, the same length and the same
/// 48 bits of entropy as TS.
#[must_use]
pub fn process_epoch() -> &'static str {
    static EPOCH: OnceLock<String> = OnceLock::new();

    EPOCH.get_or_init(mint_epoch)
}

/// Mints one epoch token (`api-events.ts:65-83`).
fn mint_epoch() -> String {
    // 6 random bytes, each rendered as exactly 2 base36 digits (`byte.toString(36).padStart(2,
    // '0')`), so `ff` -> `b3` and `00` -> `00`: always 12 characters from `[0-9a-z]`.
    let bytes = uuid::Uuid::new_v4();
    let suffix: String = bytes
        .as_bytes()
        .iter()
        .take(6)
        .map(|byte| format!("{:0>2}", to_base36(u64::from(*byte))))
        .collect();

    format!("{}-{suffix}", to_base36(now_millis()))
}

/// base36 rendering of a value, the same alphabet `Date.now().toString(36)` and
/// `byte.toString(36)` use (`api-events.ts:76`, `:83`).
fn to_base36(value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

    if value == 0 {
        return "0".to_string();
    }

    let mut remainder = value;
    let mut digits: Vec<u8> = Vec::new();

    while remainder > 0 {
        digits.push(DIGITS[(remainder % 36) as usize]);
        remainder /= 36;
    }

    digits.reverse();

    // Safety: every pushed byte comes from the 36-character ASCII table above.
    String::from_utf8(digits).unwrap_or_default()
}

/// Wall-clock milliseconds since the Unix epoch, the same clock TS's `Date.now()` reports.
#[must_use]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}
