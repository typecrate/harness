//! The stream broker: the one place the update conversation's subscriptions, sequences, cursors and
//! replay windows live. It is transport-agnostic on purpose - it never touches a socket, and the REST
//! side can drive it through plain function calls.
//!
//! TypeScript origin: `packages/underload/harness/src/server/stream/broker.ts` (918 lines).
//!
//! WHAT IS SHARED AND WHAT IS NOT (`knowledge.md` "Event hub", "T5b")
//! ------------------------------------------------------------------
//! TS's broker owns its own counter `Map` (`createStreamBroker`'s `counters`) and moves them inside
//! `publish`. This crate cannot: the counters ARE the hub's (`events::EventHub`), because a stream
//! revision has to be the same number a REST read stamps in `X-Harness-Revision`, and the PUBLISHER
//! owns the bump (`api-events.ts:302-306`, restated in `events.rs`: "REPORT-ONLY, by contract"). So:
//!
//!   * [`StreamBroker::publish`] takes a [`HubEvent`] that already carries its settled revisions and
//!     never bumps anything - the TS broker's bump step happens in `Store::commit` instead;
//!   * `subscription_state.revisions` and `resync_required.resources` are counter READS against that
//!     same hub, so the baseline a client reconciles against is the baseline the REST surface prints;
//!   * the epoch is the hub's, i.e. the process epoch (`events::process_epoch`): one broker per
//!     database root, one epoch per process, which is the contract's rule (`harness-stream.ts:15-24`).
//!
//! THE RECONCILIATION-WITHOUT-A-SNAPSHOT RULE (swagger 2234-2272)
//! --------------------------------------------------------------
//! Registering a subscription and capturing the revision baseline are the SAME instant, so a client
//! may drop every frame at or below `subscription_state.sequence` and apply everything above it.
//! `revisions` is a short per-scope list of counters (at most `revision_records_per_state` records),
//! never a resource body, and `replayed_frames` counts what the retained window re-sent. A cursor is
//! `base64url(epoch:sequence)`: a foreign epoch answers `epoch_mismatch`, a cursor older than the
//! retained window answers `cursor_expired`, and frames lost on a live connection answer
//! `frames_dropped` - the closed three-member reason set, and `frames_dropped` is the only one that
//! comes from a live wire rather than from a resume.
//!
//! WHY THE EXISTENCE CHECK RUNS BEFORE THE LOCK
//! --------------------------------------------
//! A REST mutation publishes while it STILL HOLDS the store lock (`Store::commit` -> `hub.publish` ->
//! `fan_out` -> [`StreamBroker::publish`]), so the broker's lock is always the inner one. The
//! existence check reaches the store, and reaching it under the broker lock would invert that order
//! and deadlock a publish against a subscribe. TS checks the duplicate id first (`broker.ts:662`);
//! this port hoists only that one store read, which changes the answer for exactly one input - a frame
//! that is BOTH a duplicate `subscription_id` AND names an unknown resource, where TS reports
//! `invalid_payload` and this reports `unknown_resource`. Every other condition keeps TS's precedence.
//!
//! Cross-references:
//!   * `./protocol.rs` - the tables this module routes with, plus the cursor codec.
//!   * `./connection.rs` - the per-connection conversation that calls `attach`/`subscribe`.
//!   * `runtime.rs` - `stream_broker`, where the hub listener is registered.
//!   * `ports.rs:174` - `StreamExists`, the store seam behind the `unknown_resource` check.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::contract::WsStreamLimits;
use crate::events::{EventHub, HubEvent, ResourceRef, now_millis};
use crate::ports::StreamExists;

use super::protocol::{
    self, StreamFailure, StreamPace, StreamScopeIds, SubscribeRequest, close_codes,
};

/// How a frame left the broker: it belongs to one connection's queue (`StreamDelivery`,
/// `broker.ts:112-114`).
///
/// The sink is the connection (`connection.rs`), which owns the socket and therefore owns the answer:
/// a frame here is queued, written, or evicted under the backpressure rule.
#[derive(Debug, Clone, PartialEq)]
pub struct Outbound {
    /// The connection this frame is addressed to.
    pub conn_id: u64,
    /// The frame, already serialized-shaped: every field name is the contract's.
    pub frame: Value,
}

impl Outbound {
    /// The frame's `type` tag.
    #[must_use]
    pub fn frame_type(&self) -> &str {
        self.frame.get("type").and_then(Value::as_str).unwrap_or("")
    }

    /// The subscription the frame belongs to, when it names one.
    #[must_use]
    pub fn subscription_id(&self) -> Option<&str> {
        self.frame.get("subscription_id").and_then(Value::as_str)
    }

    /// Only an `update` is droppable: `welcome`, `subscription_state`, `resync_required`, `error`,
    /// `heartbeat` and `pong` never move (swagger `backpressure.policy`, `connection.ts:133`), and the
    /// evictor depends on this test being exact.
    #[must_use]
    pub fn is_droppable(&self) -> bool {
        self.frame_type() == "update"
    }

    /// An `error` frame carrying `close_code` is the contract's "close immediately after this frame"
    /// instruction (`connection.ts:158-162`): 4401 too_many_subscriptions, 4429 rate_limited, and the
    /// host's own shutdown/restart codes from [`StreamBroker::close_all`].
    #[must_use]
    pub fn close_code(&self) -> Option<u16> {
        if self.frame_type() != "error" {
            return None;
        }

        self.frame
            .get("close_code")
            .and_then(Value::as_u64)
            .and_then(|code| u16::try_from(code).ok())
    }

    /// The message an `error` frame carries, reused as the WS close reason (`connection.ts:162`).
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.frame.get("message").and_then(Value::as_str)
    }

    /// Serialized size in bytes, the unit `send_queue_bytes` is stated in.
    #[must_use]
    pub fn bytes(&self) -> usize {
        protocol::stream_frame_bytes(&self.frame)
    }
}

/// What a `subscribe` resolves to (`StreamSubscribeOutcome`, `broker.ts:151-154`).
///
/// `Ok` carries the state frame FIRST and then any replay frames; `Resync` and `Rejected` are
/// mutually exclusive with both.
#[derive(Debug, Clone, PartialEq)]
pub enum SubscribeOutcome {
    /// Registered: `subscription_state` first, then the replayed updates.
    Ok { state: Value, replay: Vec<Value> },
    /// The client's cursor cannot be honoured: re-read from REST, never resume from a snapshot.
    Resync { resync: Value },
    /// Exactly one precedence condition, reported for its own `subscription_id`.
    Rejected { failure: StreamFailure },
}

/// One retained frame inside a replay window (`RetainedFrame`, `broker.ts:198`).
#[derive(Debug, Clone)]
struct RetainedFrame {
    sequence: u64,
    at: u64,
    frame: Value,
}

/// One live subscription (`Subscription`, `broker.ts:200-217`).
struct Subscription {
    scope: String,
    ids: StreamScopeIds,
    filters: Option<Value>,
    pace: StreamPace,
    sequence: u64,
    cursor: String,
    /// The bounded replay window: the last `replay_frames` frames within `replay_seconds`.
    retained: VecDeque<RetainedFrame>,
    /// `pace: ack` frames minted while the client owes an acknowledgement.
    hold: Vec<Value>,
    awaiting_ack: bool,
    /// Generation stamp of the armed ack timer, so a stale timer cannot fire twice.
    ack_token: u64,
    closed: bool,
}

/// One attached connection: its sink plus the subscriptions it carries.
///
/// The subscriptions live HERE rather than in one flat table because the budget check is per
/// connection (`subscriptions_per_connection`) and the fan-out walks them per connection anyway.
struct Connection {
    sink: mpsc::UnboundedSender<Outbound>,
    subscriptions: BTreeMap<String, Subscription>,
    /// Connection-level resume state from `hello`; a `subscribe` cursor overrides it for itself
    /// (`broker.ts:159-160`, swagger `resume.rules`).
    resume_cursor: Option<String>,
}

/// A replay window kept after its connection went away (`ParkedWindow`, `broker.ts:230-239`).
///
/// This is what makes the contract's single resume input work at all: a client that drops the socket
/// and reconnects inside the retention window resumes from its cursor, and the copy - not a new read -
/// is what answers its replay.
#[derive(Debug, Clone)]
struct ParkedWindow {
    sequence: u64,
    retained: VecDeque<RetainedFrame>,
    /// Wall-clock milliseconds after which the window is dead. TS holds a `setTimeout` per window;
    /// this port stamps the deadline and prunes it at the next lookup - the same observable rule, with
    /// one less task per parked subscription.
    expires_at: u64,
}

/// The broker's mutable state, behind one lock.
struct State {
    connections: BTreeMap<u64, Connection>,
    parked: BTreeMap<String, ParkedWindow>,
}

/// The process/runtime-scoped stream broker: the only thing a live connection and a REST publish
/// share.
pub struct StreamBroker {
    state: Mutex<State>,
    hub: Arc<EventHub>,
    exists: Arc<dyn StreamExists>,
    limits: WsStreamLimits,
    epoch: String,
    /// A `Weak` handle to this broker, installed by [`StreamBroker::new`], so a spawned ack timer can
    /// reach the broker without keeping it alive.
    this: OnceLock<Weak<StreamBroker>>,
    /// The hub listener, kept alive here: dropping it is what detaches the stream from the hub.
    hub_listener: Mutex<Option<crate::events::HubSubscription>>,
}

impl StreamBroker {
    /// Builds a broker over the hub that owns this runtime's epoch and counters, and the store seam
    /// that answers `unknown_resource`.
    #[must_use]
    pub fn new(hub: Arc<EventHub>, exists: Arc<dyn StreamExists>) -> Arc<Self> {
        // The epoch is the hub's, so a cursor minted by the stream and a revision read over REST name
        // the same epoch (`api-events.ts:282`, `stream/mount.ts:304`).
        let epoch = hub.epoch().to_owned();

        let broker = Arc::new(Self {
            state: Mutex::new(State {
                connections: BTreeMap::new(),
                parked: BTreeMap::new(),
            }),
            hub,
            exists,
            limits: protocol::default_ws_limits(),
            epoch,
            this: OnceLock::new(),
            hub_listener: Mutex::new(None),
        });

        // The `Arc` only exists after the literal, so the self-handle is installed here.
        let _ = broker.this.set(Arc::downgrade(&broker));

        broker
    }

    /// [`StreamBroker::new`] plus the hub listener that fans every published change out to the live
    /// subscriptions. This is the shape `runtime.rs` builds: one broker per database root, attached to
    /// the same hub the REST surface publishes through, so REST and WS cannot disagree about a
    /// revision.
    #[must_use]
    pub fn with_hub_listener(hub: Arc<EventHub>, exists: Arc<dyn StreamExists>) -> Arc<Self> {
        let broker = Self::new(Arc::clone(&hub), exists);
        let weak = broker.weak_self();

        let subscription = hub.subscribe(Arc::new(move |event: &HubEvent| {
            // A broker that is gone has no subscriptions to answer for. The hub drops a listener that
            // panics, so this upgrade is the whole lifetime rule. The returned frame list is the
            // embedder's convenience (`publish` is also callable directly), and a fan-out driven by
            // the hub has no caller to hand it to.
            if let Some(active) = weak.upgrade() {
                let _delivered = active.publish(event);
            }
        }));

        *broker
            .hub_listener
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(subscription);

        broker
    }

    /// This stream's epoch - the same string every REST response carries (`epoch()`, `broker.ts:890`).
    #[must_use]
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// The numbers IN FORCE, i.e. what `welcome.limits` advertises (`limits()`, `broker.ts:891`).
    #[must_use]
    pub fn limits(&self) -> &WsStreamLimits {
        &self.limits
    }

    /// Live subscription total, for diagnostics and the REST capability surface
    /// (`broker.ts:899-903`).
    #[must_use]
    pub fn subscriptions(&self) -> usize {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        state
            .connections
            .values()
            .map(|connection| connection.subscriptions.len())
            .sum()
    }

    /// Registers one connection's sink and returns its receiving end (`open`, `broker.ts:642-652`).
    ///
    /// TS hands the caller a `StreamSessionHandle` closing over the session; the Rust equivalent is
    /// the channel the broker pushes into, because a connection here is a task rather than an object.
    /// Dropping the receiver ends the stream; [`StreamBroker::detach`] is what releases the
    /// subscriptions and parks their windows.
    #[must_use]
    pub fn attach(&self, conn_id: u64) -> mpsc::UnboundedReceiver<Outbound> {
        let (sender, receiver) = mpsc::unbounded_channel();

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        state.connections.insert(
            conn_id,
            Connection {
                sink: sender,
                subscriptions: BTreeMap::new(),
                resume_cursor: None,
            },
        );

        receiver
    }

    /// `setResume` (`broker.ts:860-862`): the connection-level cursor a `hello` carried.
    pub fn resume(&self, conn_id: u64, cursor: Option<String>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(connection) = state.connections.get_mut(&conn_id) {
            connection.resume_cursor = cursor;
        }
    }

    /// Registers one subscription (`subscribe`, `broker.ts:654-803`), in TS's precedence order:
    /// connection closed -> duplicate id -> unknown resource -> unsupported filter -> budget ->
    /// cursor.
    ///
    /// (The store read that answers `unknown_resource` is taken BEFORE the lock, for the deadlock rule
    /// stated in this module's header.)
    pub fn subscribe(&self, conn_id: u64, request: SubscribeRequest) -> SubscribeOutcome {
        // Precedence step 5, answered without the broker lock held.
        let unknown = self.unknown_resource(&request);

        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Disjoint field borrows: `connections` needs `&mut` for the insert, and `parked` is read and
        // then conditionally pruned. One `&mut State` would not let both live at once.
        let State {
            connections,
            parked,
        } = &mut *guard;

        let Some(connection) = connections.get_mut(&conn_id) else {
            return SubscribeOutcome::Rejected {
                failure: StreamFailure::for_subscription(
                    StreamFailure::new("internal", "The connection is closed."),
                    request.subscription_id.clone(),
                ),
            };
        };

        // `subscription_id` is client-chosen and unique per connection; the contract states the
        // uniqueness rule without naming a code, so the closest closed-vocabulary condition is a
        // schema failure (the frame cannot be honoured as sent).
        if connection
            .subscriptions
            .contains_key(&request.subscription_id)
        {
            return SubscribeOutcome::Rejected {
                failure: StreamFailure::for_subscription(
                    StreamFailure::new(
                        "invalid_payload",
                        format!(
                            "subscription_id {} is already in use on this connection.",
                            request.subscription_id
                        ),
                    ),
                    request.subscription_id.clone(),
                ),
            };
        }

        if let Some((resource, id)) = unknown {
            return SubscribeOutcome::Rejected {
                failure: StreamFailure::for_subscription(
                    StreamFailure::new("unknown_resource", format!("No {resource} matches {id}.")),
                    request.subscription_id.clone(),
                ),
            };
        }

        // Precedence step 6: a filter the scope does not publish answers for THAT subscription only;
        // the connection and its other subscriptions keep working.
        let published = protocol::scope_filters(&request.scope);
        let unsupported: Vec<String> = request
            .filters
            .as_ref()
            .and_then(Value::as_object)
            .map(|filters| {
                filters
                    .keys()
                    .filter(|key| !published.contains(&key.as_str()))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        if !unsupported.is_empty() {
            return SubscribeOutcome::Rejected {
                failure: StreamFailure::for_subscription(
                    StreamFailure::new(
                        "unsupported_filter",
                        format!(
                            "{} does not publish {}.",
                            request.scope,
                            unsupported.join(", ")
                        ),
                    ),
                    request.subscription_id.clone(),
                ),
            };
        }

        // Precedence step 7: the per-connection budget (`broker.ts:708-718`).
        if u64::try_from(connection.subscriptions.len()).unwrap_or(u64::MAX)
            >= self.limits.subscriptions_per_connection
        {
            return SubscribeOutcome::Rejected {
                failure: StreamFailure::for_subscription(
                    StreamFailure::with_close(
                        "too_many_subscriptions",
                        format!(
                            "A connection carries at most {} subscriptions.",
                            self.limits.subscriptions_per_connection
                        ),
                        close_codes::TOO_MANY_SUBSCRIPTIONS,
                    ),
                    request.subscription_id.clone(),
                ),
            };
        }

        // A `subscribe` cursor is authoritative for its own subscription, ahead of any `hello` cursor
        // (swagger `resume.rules`: "the scopes of the two inputs do not overlap").
        let key = request.ids.window_key(&request.scope);
        let parked_window = peek_parked(parked, &key, now_millis());
        let cursor = request.cursor.clone().or_else(|| {
            connection
                .resume_cursor
                .clone()
                .filter(|value| !value.is_empty())
        });

        let decoded = protocol::decode_stream_cursor(cursor.as_deref());

        let mut subscription = Subscription {
            scope: request.scope.clone(),
            ids: request.ids.clone(),
            filters: request.filters.clone(),
            pace: request.pace,
            sequence: 0,
            cursor: protocol::encode_stream_cursor(&self.epoch, 0),
            retained: VecDeque::new(),
            hold: Vec::new(),
            awaiting_ack: false,
            ack_token: 0,
            closed: false,
        };

        // Epoch policy: a cursor from another process is dead on arrival, and the answer names the
        // bounded reads to re-issue. Never a snapshot. The client's own position is echoed back so the
        // frame is comparable with what the client sent (`broker.ts:744-747`).
        if let Some(decoded) = decoded.as_ref()
            && decoded.epoch != self.epoch
        {
            let resync = self.resync_frame(
                &request.subscription_id,
                &request.scope,
                &request.ids,
                "epoch_mismatch",
                &protocol::encode_stream_cursor(&decoded.epoch, decoded.sequence),
                None,
            );

            return SubscribeOutcome::Resync { resync };
        }

        // A cursor below the retained floor (or older than the age bound) cannot be answered from
        // memory: the client re-reads the resources it cares about.
        if let (Some(decoded), Some(window)) = (decoded.as_ref(), parked_window.as_ref())
            && let Some(oldest) = window.retained.front()
            && (decoded.sequence + 1 < oldest.sequence
                || now_millis().saturating_sub(oldest.at) > self.limits.replay_seconds * 1_000)
        {
            let resync = self.resync_frame(
                &request.subscription_id,
                &request.scope,
                &request.ids,
                "cursor_expired",
                cursor.as_deref().unwrap_or(&subscription.cursor),
                None,
            );

            return SubscribeOutcome::Resync { resync };
        }

        // Resume only when the client actually sent a cursor AND a window answers it; with neither, the
        // subscription starts fresh at the server's own current position.
        let replay: Vec<Value> = match (decoded.as_ref(), parked_window.as_ref()) {
            (Some(decoded), Some(window)) if !window.retained.is_empty() => {
                subscription.sequence = window.sequence;
                subscription.cursor = protocol::encode_stream_cursor(&self.epoch, window.sequence);
                subscription.retained = window.retained.clone();

                // The window is consumed by the resume that read it (`dropParked`, `broker.ts:767`).
                parked.remove(&key);

                window
                    .retained
                    .iter()
                    .filter(|entry| entry.sequence > decoded.sequence)
                    .map(|entry| entry.frame.clone())
                    .collect()
            }
            _ => Vec::new(),
        };

        // Registration and the baseline are one instant: the state frame reports the sequence the
        // client is reconciled against, and every frame ABOVE it is applied by the client.
        let state_sequence = replay
            .last()
            .and_then(|frame| frame.get("sequence"))
            .and_then(Value::as_u64)
            .unwrap_or(subscription.sequence);

        // The replayed frames stay in the window, so a second reconnect can resume again
        // (`broker.ts:800`).
        for frame in &replay {
            let sequence = frame
                .get("sequence")
                .and_then(Value::as_u64)
                .unwrap_or(state_sequence);
            let at = frame
                .get("at")
                .and_then(Value::as_u64)
                .unwrap_or_else(now_millis);

            retain(
                &mut subscription.retained,
                RetainedFrame {
                    sequence,
                    at,
                    frame: frame.clone(),
                },
                &self.limits,
            );
        }

        let state_cursor = protocol::encode_stream_cursor(&self.epoch, state_sequence);
        let state_frame = protocol::subscription_state_frame(&protocol::SubscriptionStateFrame {
            subscription_id: &request.subscription_id,
            scope: &request.scope,
            epoch: &self.epoch,
            sequence: state_sequence,
            cursor: &state_cursor,
            revisions: self.baseline_records(&request.scope, &request.ids),
            filters_applied: request.filters.clone(),
            replayed_frames: (!replay.is_empty()).then_some(replay.len() as u64),
            publishes: protocol::scope_publishes(&request.scope),
        });

        connection
            .subscriptions
            .insert(request.subscription_id.clone(), subscription);

        SubscribeOutcome::Ok {
            state: state_frame,
            replay,
        }
    }

    /// Ends one subscription and releases its replay buffer INTO a parked window (`unsubscribe`,
    /// `broker.ts:807-818`). No reply frame goes out on success (`connection.ts:321-325`).
    pub fn unsubscribe(&self, conn_id: u64, subscription_id: &str) -> Result<(), StreamFailure> {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let State {
            connections,
            parked,
        } = &mut *guard;

        let Some(connection) = connections.get_mut(&conn_id) else {
            return Err(unknown_subscription(subscription_id));
        };

        let Some(subscription) = connection.subscriptions.remove(subscription_id) else {
            return Err(unknown_subscription(subscription_id));
        };

        park(parked, &subscription, &self.limits);

        Ok(())
    }

    /// Confirms the highest sequence the client has APPLIED (`acknowledge`, `broker.ts:820-838`).
    ///
    /// Acking a subscription that did not ask for pacing is IGNORED rather than an error, so a client
    /// may always ack - which is also why `sequence` is accepted and then not read: the contract's
    /// stop-and-wait releases the whole hold, and a stale ack cannot rewind one.
    pub fn acknowledge(
        &self,
        conn_id: u64,
        subscription_id: &str,
        sequence: u64,
    ) -> Result<(), StreamFailure> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let Some(connection) = state.connections.get_mut(&conn_id) else {
            return Err(unknown_subscription(subscription_id));
        };

        // The sink is cloned first so the borrow of `subscriptions` below is the only one live.
        let sink = connection.sink.clone();

        let Some(subscription) = connection.subscriptions.get_mut(subscription_id) else {
            return Err(unknown_subscription(subscription_id));
        };

        if subscription.pace != StreamPace::Ack {
            let _ = sequence;

            return Ok(());
        }

        // Stop the armed timer by moving its generation on, then release the backlog in sequence order
        // (`broker.ts:831-834`).
        subscription.ack_token = subscription.ack_token.wrapping_add(1);
        subscription.awaiting_ack = true;

        let mut frames = std::mem::take(&mut subscription.hold);
        frames.sort_by_key(|frame| {
            frame
                .get("sequence")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX)
        });

        for frame in frames {
            let _ = sink.send(Outbound { conn_id, frame });
        }

        Ok(())
    }

    /// Fans one published change out to every subscription that wants it (`publish`,
    /// `broker.ts:585-628`) and returns the frames that reached a connection queue.
    ///
    /// One event can become SEVERAL frames: `ops_per_update_frame`, `events_per_update_frame` and
    /// `ids_per_invalidation_frame` all say "more items become more frames, never a bigger one".
    /// Frames for one mutation keep the mutation's `at`, which is what lets a client order across
    /// subscriptions (`ordering.monotonic_rule`).
    #[must_use]
    pub fn publish(&self, event: &HubEvent) -> Vec<Outbound> {
        let payloads = frames_for(event, &self.limits);

        if payloads.is_empty() {
            return Vec::new();
        }

        let at = event.at;
        let mut sent: Vec<Outbound> = Vec::new();
        let mut armed: Vec<(u64, String)> = Vec::new();

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        for (conn_id, connection) in state.connections.iter_mut() {
            let mut minted: Vec<Value> = Vec::new();

            for (subscription_id, subscription) in connection.subscriptions.iter_mut() {
                if subscription.closed || !matches(subscription, event) {
                    continue;
                }

                for payload in &payloads {
                    let Some(narrowed) = narrow(subscription, &event.kind, payload) else {
                        // Every op of this frame was filtered out, so the frame is not sent at all -
                        // an empty ops array would be a lie about order (`broker.ts:534`).
                        continue;
                    };

                    deliver(
                        subscription,
                        subscription_id,
                        &event.kind,
                        &narrowed,
                        at,
                        &self.epoch,
                        &self.limits,
                        &mut minted,
                    );
                }

                // `armAckTimer` runs once per burst, not once per held frame: the timer is guarded by
                // `ack_token`, and one deadline per subscription is what TS's single `ackTimer` field
                // expresses.
                if subscription.pace == StreamPace::Ack && !subscription.hold.is_empty() {
                    armed.push((*conn_id, subscription_id.clone()));
                }
            }

            for frame in minted {
                let outbound = Outbound {
                    conn_id: *conn_id,
                    frame,
                };

                // A failed send means the connection is gone; its subscriptions are released by
                // `detach`, which the connection task always reaches.
                if connection.sink.send(outbound.clone()).is_ok() {
                    sent.push(outbound);
                }
            }
        }

        drop(state);

        // Spawned after the lock is released: the timer callback takes the same lock.
        for (conn_id, subscription_id) in armed {
            let token = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .connections
                .get(&conn_id)
                .and_then(|connection| connection.subscriptions.get(&subscription_id))
                .map(|subscription| subscription.ack_token)
                .unwrap_or(0);

            self.arm_ack_timer(conn_id, subscription_id, token);
        }

        sent
    }

    /// Reports frames LOST ON THE WIRE for one subscription (`requestResync`, `broker.ts:849-853`).
    ///
    /// This is the only path that turns a delivery failure into a contract frame, so the transport
    /// never invents one itself: the reason is `frames_dropped`, the closed-set spelling of "frames
    /// lost while the connection stayed open" (the prose word "backpressure" is NOT a member of
    /// `WsResyncRequired.reason`).
    #[must_use]
    pub fn report_dropped(
        &self,
        conn_id: u64,
        subscription_id: &str,
        lost_frames: u64,
    ) -> Vec<Outbound> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let Some(connection) = state.connections.get(&conn_id) else {
            return Vec::new();
        };

        let Some(subscription) = connection.subscriptions.get(subscription_id) else {
            return Vec::new();
        };

        if subscription.closed {
            return Vec::new();
        }

        let frame = self.resync_frame(
            subscription_id,
            &subscription.scope,
            &subscription.ids,
            "frames_dropped",
            &subscription.cursor,
            Some(lost_frames),
        );

        // A `resync_required` consumes no sequence and is never retained: it is an out-of-band
        // instruction tied to the moment it was sent, and consuming a sequence would look like a gap to
        // a client doing arithmetic on `update` sequences (`broker.ts:402-407`).
        vec![Outbound { conn_id, frame }]
    }

    /// Releases every subscription of one connection, keeping a parked replay window for each
    /// (`release`, `broker.ts:840-847`).
    ///
    /// Closing the connection cancels NOTHING above the stream: a run keeps executing
    /// (`connection.ts:386-394`, "Closing the connection cancels nothing").
    pub fn detach(&self, conn_id: u64) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let State {
            connections,
            parked,
        } = &mut *guard;

        let Some(connection) = connections.remove(&conn_id) else {
            return;
        };

        for subscription in connection.subscriptions.values() {
            park(parked, subscription, &self.limits);
        }
    }

    /// Closes every live connection with `code` and drops all stream state (`close`,
    /// `broker.ts:871-887`): host shutdown answers 1001, an epoch change answers 1012.
    ///
    /// The frame is the contract's - an `error` carrying a `close_code`, which the connection turns
    /// into a WS close frame immediately after sending it. The counters are the HUB's and are NOT
    /// cleared: they outlive the stream in this port, because REST reads quote them.
    pub fn close_all(&self, code: u16) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        for (conn_id, connection) in state.connections.iter() {
            let frame = protocol::error_frame(&StreamFailure::with_close(
                "internal",
                format!("The stream is closing (code {code})."),
                code,
            ));

            let _ = connection.sink.send(Outbound {
                conn_id: *conn_id,
                frame,
            });
        }

        state.connections.clear();
        state.parked.clear();
    }

    /// The `unknown_resource` decision for one subscribe (precedence step 5), answered from the store
    /// seam WITHOUT the broker lock held.
    fn unknown_resource(&self, request: &SubscribeRequest) -> Option<(&'static str, String)> {
        if let Some(session_id) = request.ids.session_id.as_deref()
            && !self.exists.has_session(session_id)
        {
            return Some(("session", session_id.to_owned()));
        }

        if let Some(thread_id) = request.ids.thread_id.as_deref() {
            let session_id = request.ids.session_id.clone().unwrap_or_default();

            if !self.exists.has_thread(&session_id, thread_id) {
                return Some(("thread", thread_id.to_owned()));
            }
        }

        if let Some(turn_id) = request.ids.turn_id.as_deref() {
            let session_id = request.ids.session_id.clone().unwrap_or_default();

            if !self.exists.has_turn(&session_id, turn_id) {
                return Some(("turn", turn_id.to_owned()));
            }
        }

        None
    }

    /// The per-scope revision baseline for `subscription_state.revisions`: counter READS against the
    /// hub only - never a compiled view, never a session snapshot - and never more than
    /// `revision_records_per_state` records (`baselineRecords`, `broker.ts:311-326`).
    #[must_use]
    fn baseline_records(&self, scope: &str, ids: &StreamScopeIds) -> Vec<Value> {
        let cap = usize::try_from(self.limits.revision_records_per_state).unwrap_or(usize::MAX);

        let references: Vec<ResourceRef> = protocol::scope_baselines(scope)
            .iter()
            .take(cap)
            .map(|resource| ResourceRef::new(*resource, ids.hub_resource_id(resource)).normalized())
            .collect();

        self.hub
            .revisions(&references)
            .into_iter()
            .map(|record| serde_json::to_value(record).unwrap_or(Value::Null))
            .collect()
    }

    /// `resyncFrame` (`broker.ts:383-400`): the reason, the current epoch and cursor to resume from,
    /// and the bounded reads that restore this subscription - exactly its baseline kinds, capped at the
    /// 16 links `WsResyncRequired` allows.
    ///
    /// The link comes from `protocol::default_stream_link` (the contract's `defaultStreamLink`, which
    /// quotes `?limit=25` on the paged kinds); the revision comes from the hub, keyed the way the REST
    /// write stamped it.
    #[must_use]
    fn resync_frame(
        &self,
        subscription_id: &str,
        scope: &str,
        ids: &StreamScopeIds,
        reason: &str,
        cursor: &str,
        lost_frames: Option<u64>,
    ) -> Value {
        let resources: Vec<Value> = protocol::scope_baselines(scope)
            .iter()
            .take(16)
            .map(|resource| {
                let id = ids.hub_resource_id(resource);

                protocol::resource_link(
                    resource,
                    id.as_deref(),
                    self.hub.revision(resource, id.as_deref()),
                    &self.epoch,
                    &protocol::default_stream_link(resource, ids),
                )
            })
            .collect();

        protocol::resync_frame(
            subscription_id,
            reason,
            &self.epoch,
            cursor,
            lost_frames,
            resources,
        )
    }

    /// `armAckTimer` (`broker.ts:418-431`), spelled as a task because this port has no event-loop
    /// timer object.
    ///
    /// The ack timeout only guards a BACKLOG: a stop-and-wait subscription that was acknowledged into
    /// silence has nothing queued, so a quiet client is never punished with a spurious resync (the rule
    /// exists so the server does not "queue without bound"). Outside a tokio runtime there is no clock
    /// to arm, and the hold then waits for the `ack` - the same outcome as TS with an unref'd timer in
    /// a process on its way out.
    fn arm_ack_timer(&self, conn_id: u64, subscription_id: String, token: u64) {
        let Some(broker) = self.weak_self().upgrade() else {
            return;
        };

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(
                broker.limits.ack_timeout_ms,
            ))
            .await;

            broker.expire_ack(conn_id, &subscription_id, token);
        });
    }

    /// The ack deadline arm (`broker.ts:421-428`): drop the hold, release the stop-and-wait latch, and
    /// answer `resync_required{ frames_dropped, lost_frames }`.
    fn expire_ack(&self, conn_id: u64, subscription_id: &str, token: u64) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let Some(connection) = guard.connections.get_mut(&conn_id) else {
            return;
        };

        let sink = connection.sink.clone();

        let Some(subscription) = connection.subscriptions.get_mut(subscription_id) else {
            return;
        };

        // A stale timer (an ack already arrived, or the hold was replaced) does nothing.
        if subscription.closed || subscription.ack_token != token || subscription.hold.is_empty() {
            return;
        }

        let lost = subscription.hold.len() as u64;

        subscription.hold.clear();
        subscription.awaiting_ack = false;

        let frame = self.resync_frame(
            subscription_id,
            &subscription.scope,
            &subscription.ids,
            "frames_dropped",
            &subscription.cursor,
            Some(lost),
        );

        drop(guard);

        let _ = sink.send(Outbound { conn_id, frame });
    }

    /// The `Weak` handle installed by [`StreamBroker::new`].
    #[must_use]
    fn weak_self(&self) -> Weak<StreamBroker> {
        self.this.get().cloned().unwrap_or_else(Weak::new)
    }
}

/// `unknownSubscription` (`broker.ts:812`, `:824`): an unknown or already-closed id is precedence
/// step 4.
fn unknown_subscription(subscription_id: &str) -> StreamFailure {
    StreamFailure::for_subscription(
        StreamFailure::new(
            "unknown_subscription",
            format!("No subscription {subscription_id} is open on this connection."),
        ),
        subscription_id,
    )
}

/// `retain` (`broker.ts:335-346`).
///
/// Retention is whichever of the two bounds ends FIRST, so old frames are evicted by age as well as by
/// count - and the `at` of the OLDEST retained frame is what makes a cursor "too old".
fn retain(retained: &mut VecDeque<RetainedFrame>, frame: RetainedFrame, limits: &WsStreamLimits) {
    retained.push_back(frame);

    let replay_frames = usize::try_from(limits.replay_frames).unwrap_or(usize::MAX);
    let oldest_allowed = now_millis().saturating_sub(limits.replay_seconds * 1_000);

    while retained.len() > replay_frames
        || retained
            .front()
            .is_some_and(|entry| entry.at < oldest_allowed)
    {
        retained.pop_front();
    }
}

/// `park` (`broker.ts:348-370`): copy the window out so a reconnect can resume from it. A window with
/// nothing in it cannot answer a resume, so it is not worth keeping.
fn park(
    parked: &mut BTreeMap<String, ParkedWindow>,
    subscription: &Subscription,
    limits: &WsStreamLimits,
) {
    if subscription.retained.is_empty() {
        return;
    }

    let key = subscription.ids.window_key(&subscription.scope);

    parked.insert(
        key,
        ParkedWindow {
            sequence: subscription.sequence,
            retained: subscription.retained.clone(),
            expires_at: now_millis().saturating_add(limits.replay_seconds * 1_000),
        },
    );
}

/// Reads a parked window WITHOUT consuming it, treating an elapsed one as absent
/// (`ParkedWindow.expires` - the deadline arm of TS's `setTimeout`).
///
/// The window survives a `cursor_expired`/`epoch_mismatch` answer for exactly the reason TS's
/// `dropParked` sits inside the resume arm: a refused resume must not destroy the window a later
/// subscribe could still answer from.
fn peek_parked(
    parked: &BTreeMap<String, ParkedWindow>,
    key: &str,
    at: u64,
) -> Option<ParkedWindow> {
    let window = parked.get(key)?;

    (at <= window.expires_at).then(|| window.clone())
}

/// `matches` (`broker.ts:486-515`).
///
/// Scope publishes first (a kind outside the scope's set never reaches a client), then the ids the
/// subscription addresses, then its filters. Narrowing only ever REMOVES frames - it never widens a
/// payload.
fn matches(subscription: &Subscription, event: &HubEvent) -> bool {
    if !protocol::is_kind_published_to(&subscription.scope, &event.kind) {
        return false;
    }

    if subscription.scope != "global" {
        let Some(session_id) = subscription.ids.session_id.as_deref() else {
            return false;
        };

        if event.session_id.as_deref() != Some(session_id) {
            return false;
        }

        if subscription.scope == "thread"
            && subscription.ids.thread_id.as_deref() != event.thread_id.as_deref()
        {
            return false;
        }

        if subscription.scope == "turn"
            && subscription.ids.turn_id.as_deref() != event.turn_id.as_deref()
        {
            return false;
        }
    }

    // `run.event` is opt-in debug traffic: the `events` filter defaults to false, and a scope that
    // never asked for it must not pay for raw event bytes.
    if event.kind == "run.event" && !filter_is_true(subscription, "events") {
        return false;
    }

    let Some(filters) = subscription.filters.as_ref().and_then(Value::as_object) else {
        return true;
    };

    // `status` narrows the turn rows published on a session scope. A row that names no status is not
    // narrowed away, which is TS's rule (`broker.ts:503-506`).
    if let Some(status) = filters.get("status").and_then(Value::as_str)
        && event.kind == "turn.upsert"
    {
        let row_status = event
            .payload
            .get("turn")
            .and_then(|turn| turn.get("status"))
            .and_then(Value::as_str);

        if row_status.is_some_and(|row_status| row_status != status) {
            return false;
        }
    }

    // `thread_id` narrows the conversation whose rows and ops are published. An event with no thread
    // association (a session patch, a usage figure, a queue change) is conversation-independent and
    // therefore always stays, which is what keeps the filter from silently starving a pane that is
    // showing a thread.
    if let Some(thread_id) = filters.get("thread_id").and_then(Value::as_str)
        && event
            .thread_id
            .as_deref()
            .is_some_and(|event_thread| event_thread != thread_id)
    {
        return false;
    }

    true
}

/// One boolean filter read, `false` when absent (`broker.ts:497`).
fn filter_is_true(subscription: &Subscription, key: &str) -> bool {
    subscription
        .filters
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|filters| filters.get(key))
        .and_then(Value::as_bool)
        == Some(true)
}

/// `narrow` (`broker.ts:520-546`): payload narrowing for the `turn` scope's two filters.
///
/// `sections: false` keeps the frame and strips the node/step bodies; `context: false` drops the
/// `context:*` ops outright.
fn narrow(subscription: &Subscription, kind: &str, payload: &Value) -> Option<Value> {
    if kind != "display.ops" {
        return Some(payload.clone());
    }

    let (sections, context) = match subscription.filters.as_ref().and_then(Value::as_object) {
        Some(filters) => (
            filters.get("sections").and_then(Value::as_bool),
            filters.get("context").and_then(Value::as_bool),
        ),
        None => (None, None),
    };

    if sections.is_none() && context.is_none() {
        return Some(payload.clone());
    }

    let Value::Object(source) = payload else {
        return Some(payload.clone());
    };

    let Some(ops) = source.get("ops").and_then(Value::as_array) else {
        return Some(payload.clone());
    };

    let mut ops = ops.clone();

    if context == Some(false) {
        ops.retain(|op| {
            op.get("op")
                .and_then(Value::as_str)
                .map(|name| !name.starts_with("context:"))
                .unwrap_or(true)
        });
    }

    if ops.is_empty() {
        return None;
    }

    if sections == Some(false) {
        ops = ops
            .iter()
            .map(|op| match op.as_object() {
                Some(op) => {
                    let mut stripped = op.clone();
                    stripped.remove("node");
                    stripped.remove("step");

                    Value::Object(stripped)
                }
                None => op.clone(),
            })
            .collect();
    }

    let mut narrowed = source.clone();
    narrowed.insert("ops".to_owned(), Value::Array(ops));

    Some(Value::Object(narrowed))
}

/// `framesFor` (`broker.ts:551-583`): the payload objects one event becomes, BEFORE the envelope is
/// stamped per subscription.
///
/// Two adaptations to this crate's hub shape are stated rather than hidden:
///   * `display.ops` carries its ops in `HubEvent::ops` here (`events.rs:118-125`), so they are folded
///     into the payload the contract's `WsUpdateDisplayOps` variant names;
///   * `run.event` carries ONE recorded row as its payload (`store/turns.rs:1424-1429`), while the
///     contract names `turn_id` plus an `events` ARRAY (`WsUpdateRunEvent`) - so the row is wrapped,
///     and the row's own fields never leak to the top of the frame.
#[must_use]
fn frames_for(event: &HubEvent, limits: &WsStreamLimits) -> Vec<Value> {
    let published: Map<String, Value> = match &event.payload {
        Value::Object(map) => map.clone(),
        other => {
            // A payload that is not an object has no contract-named field to carry, so it travels
            // under the envelope's own name rather than being dropped.
            if other.is_null() {
                Map::new()
            } else {
                let mut single = Map::new();
                single.insert("payload".to_owned(), other.clone());

                single
            }
        }
    };

    // `max_server_frame_bytes - 512` is TS's allowance for the envelope and the frame's own punctuation
    // (`broker.ts:558`).
    let budget = usize::try_from(limits.max_server_frame_bytes)
        .unwrap_or(usize::MAX)
        .saturating_sub(512);

    if event.kind == "display.ops" {
        let turn_id = published
            .get("turn_id")
            .cloned()
            .or_else(|| {
                event
                    .turn_id
                    .as_deref()
                    .map(|turn_id| Value::String(turn_id.to_owned()))
            })
            .unwrap_or(Value::Null);

        let ops: Vec<Value> = if published.contains_key("ops") {
            published
                .get("ops")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        } else {
            event.ops.clone()
        };

        return protocol::chunk_stream_payload(
            &ops,
            usize::try_from(limits.ops_per_update_frame).unwrap_or(usize::MAX),
            budget,
            protocol::stream_frame_bytes,
        )
        .into_iter()
        .map(|chunk| {
            let mut payload = Map::new();

            payload.insert("turn_id".to_owned(), turn_id.clone());
            payload.insert("ops".to_owned(), Value::Array(chunk));

            Value::Object(payload)
        })
        .collect();
    }

    if event.kind == "run.event" {
        let rows = vec![event.payload.clone()];

        return protocol::chunk_stream_payload(
            &rows,
            usize::try_from(limits.events_per_update_frame).unwrap_or(usize::MAX),
            budget,
            protocol::stream_frame_bytes,
        )
        .into_iter()
        .map(|chunk| {
            let mut payload = Map::new();

            payload.insert(
                "turn_id".to_owned(),
                event
                    .turn_id
                    .as_deref()
                    .map_or(Value::Null, |turn_id| Value::String(turn_id.to_owned())),
            );
            payload.insert("events".to_owned(), Value::Array(chunk));

            Value::Object(payload)
        })
        .collect();
    }

    if matches!(
        event.kind.as_str(),
        "tool.registry_updated" | "agent.registry_updated" | "catalogue.updated"
    ) {
        let ids: Vec<Value> = published
            .get("ids")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        // `+ 2` per id is TS's allowance for the quote and the comma of the enclosing array
        // (`broker.ts:576`).
        let chunks = protocol::chunk_stream_payload(
            &ids,
            usize::try_from(limits.ids_per_invalidation_frame).unwrap_or(usize::MAX),
            usize::try_from(limits.max_server_frame_bytes).unwrap_or(usize::MAX),
            |id| protocol::stream_frame_bytes(id) + 2,
        );

        if chunks.len() <= 1 {
            return vec![Value::Object(published)];
        }

        return chunks
            .into_iter()
            .map(|chunk| {
                let mut payload = published.clone();

                payload.insert("ids".to_owned(), Value::Array(chunk));

                Value::Object(payload)
            })
            .collect();
    }

    vec![Value::Object(published)]
}

/// `deliver` (`broker.ts:450-477`): stamp the envelope, retain the frame, then send it or hold it.
///
/// The envelope (`subscription_id`, `sequence`, `cursor`) is minted HERE, never by the publisher: a
/// caller hands over a kind plus a payload, and this is the only place that knows which subscription a
/// frame belongs to and where its sequence stands.
///
/// The eight parameters are TS's closure environment made explicit: `deliver` closes over `epoch` and
/// the limits in `createStreamBroker`, and this port passes them in so the function stays a pure
/// transform of `(subscription, payload)` rather than a method on a shared-mutable broker.
#[allow(clippy::too_many_arguments)]
fn deliver(
    subscription: &mut Subscription,
    subscription_id: &str,
    kind: &str,
    payload: &Value,
    at: u64,
    epoch: &str,
    limits: &WsStreamLimits,
    sent: &mut Vec<Value>,
) {
    subscription.sequence += 1;
    subscription.cursor = protocol::encode_stream_cursor(epoch, subscription.sequence);

    let stamped = protocol::update_frame(
        subscription_id,
        kind,
        subscription.sequence,
        &subscription.cursor,
        at,
        payload,
    );

    retain(
        &mut subscription.retained,
        RetainedFrame {
            sequence: subscription.sequence,
            at,
            frame: stamped.clone(),
        },
        limits,
    );

    // `pace: ack` stops and waits: one frame goes out, the rest are held (`broker.ts:466-473`).
    if subscription.pace == StreamPace::Ack && subscription.awaiting_ack {
        subscription.hold.push(stamped);

        return;
    }

    if subscription.pace == StreamPace::Ack {
        subscription.awaiting_ack = true;
    }

    sent.push(stamped);
}
