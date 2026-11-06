//! The per-connection harness stream conversation: what happens between the `101` and the close.
//!
//! TypeScript origin: `packages/underload/harness/src/server/stream/connection.ts` (418 lines), with
//! the parts of `transport.ts` that are NOT the RFC 6455 codec folded in (the binary-frame refusal).
//!
//! This is the ONLY place a client frame changes broker state, and it enforces the order the contract
//! states in `harness.swagger.yaml:2389-2413`:
//!
//! ```text
//! (1) UTF-8 JSON text object within `max_client_frame_bytes`  -> axum/tungstenite + protocol.rs
//! (2) client-union schema                                      -> ./protocol.rs `parse_client_frame`
//! (3) `hello` only as the first frame                          -> here (`late_hello`)
//! (4) known `subscription_id` for ack/unsubscribe              -> here + ./broker.rs
//! (5) required resource id exists                              -> ./broker.rs (`unknown_resource`)
//! (6) filter published for the scope                           -> ./broker.rs (`unsupported_filter`)
//! (7) budgets and rates                                        -> here (`rate_limited`) + broker
//! ```
//!
//! WHAT THE CONNECTION OWNS AND THE BROKER CANNOT SEE
//! --------------------------------------------------
//! The `welcome` frame, the heartbeat budget, the per-second message budget, the invalid-frame streak,
//! the send queue (and therefore the drop-oldest-droppable backpressure rule), and the close codes.
//! Everything else - sequences, cursors, replay windows, revisions - is the broker's, so two
//! connections on one runtime cannot disagree about a number.
//!
//! WHAT TS DOES WITH CALLBACKS, THIS DOES WITH A CHANNEL
//! -----------------------------------------------------
//! TS's `startStreamConnection` registers listeners on a `HarnessSocket` it does not own. Here the
//! socket IS the loop: one task selects over the socket, the broker's frame channel, and the heartbeat
//! ticker, and every write goes through one queue so the backpressure rule has a single owner. The
//! observable contract is unchanged: `welcome` is still first, `heartbeat` still every
//! `heartbeat_interval_ms`, silence still closes 4408, and a frame the queue could not hold still
//! produces exactly one `resync_required{ frames_dropped }` per affected subscription.
//!
//! Cross-references:
//!   * `./protocol.rs`     - `parse_client_frame`, `close_codes`, the frame builders.
//!   * `./broker.rs`       - subscriptions, sequences, replay, and the `resync_required` frames.
//!   * `./mod.rs`          - the handshake that produced this socket.
//!   * swagger 2486-2509   - the heartbeat and backpressure rules implemented verbatim below.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::StreamExt as _;
use tokio::time::{Interval, MissedTickBehavior, timeout};

use crate::contract::WsStreamLimits;
use crate::events::now_millis;
use crate::runtime::HarnessRuntime;

use super::broker::{Outbound, StreamBroker, SubscribeOutcome};
use super::protocol::{self, ClientFrame, ParseOutcome, StreamFailure, close_codes};

/// The one connection counter, so two connections on one runtime never share an id (TS mints `c1`,
/// `c2`, ... in `broker.ts:643-648`; the number here plays the same role and appears in no frame).
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// The longest a single socket write may block before the connection is called stalled (the contract's
/// `sustained_stall_ms` arm of `backpressure`).
const CLOSE_HANDSHAKE_BUDGET: Duration = Duration::from_secs(2);

/// Runs one accepted stream connection (`startStreamConnection`, `connection.ts:58-415`).
///
/// `runtime` is the harness runtime the upgrade resolved, and its epoch is the one `welcome` advertises
/// - the SAME value the REST surface reports, which is the only way the contract's "compare a revision
///   within one epoch" rule can hold across both transports.
///
/// On the way out the connection ALWAYS releases its subscriptions through
/// [`StreamBroker::detach`], which parks a replay window for each one; a run is never cancelled,
/// because closing the stream cancels nothing (swagger 2271, `connection.ts:386-394`).
pub async fn serve(socket: WebSocket, runtime: Arc<HarnessRuntime>, broker: Arc<StreamBroker>) {
    let socket = &mut { socket };
    let conn_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let limits = *broker.limits();
    let epoch = runtime.epoch();
    let mut inbound = broker.attach(conn_id);
    let mut state = ConnectionState::new(conn_id, limits);

    // First frame on the wire is always `welcome`, before any `subscription_state`, so a client learns
    // the epoch and the numbers in force before it sends a single subscribe (`connection.ts:396-405`).
    // It is not droppable, and a failure to write it means the peer is already gone.
    state.push(Outbound {
        conn_id,
        frame: protocol::welcome_frame(&epoch, now_millis(), &limits),
    });

    loop {
        // 1. Everything the broker minted while this task was elsewhere enters the queue here, under
        //    the backpressure rule (`connection.ts:130-165`).
        while let Ok(outbound) = inbound.try_recv() {
            state.push(outbound);
        }

        // 2. Evictions are reported through the broker, and a drop can be caused by a REST publish
        //    with no inbound frame in flight, so the report is flushed on every pass - always AFTER
        //    the frames that displaced it entered the queue, which is what keeps the client's view in
        //    order (`connection.ts:167-191`).
        for (subscription_id, lost) in state.take_lost() {
            for outbound in broker.report_dropped(conn_id, &subscription_id, lost) {
                state.push(outbound);
            }
        }

        // 3. One write per pass. Awaiting the write here - rather than draining the whole queue - is
        //    what keeps the read side and the heartbeat alive while a slow client digests a burst.
        if let Some(outbound) = state.pop() {
            match write(socket, &outbound, &limits).await {
                Written::Sent => {
                    // An `error` frame with a `close_code` is the contract's "close immediately after
                    // this frame" instruction, so the reason the client reads is the frame's own
                    // message (`connection.ts:158-162`).
                    if let Some(code) = outbound.close_code() {
                        state
                            .request_close(code, outbound.message().unwrap_or_default().to_owned());
                    }
                }
                Written::Stalled => {
                    state.request_close(close_codes::TRY_AGAIN_LATER, "The send queue stalled.");
                }
                Written::Gone => break,
            }

            continue;
        }

        // 4. A close was asked for and everything still owed has been written: put the close frame on
        //    the wire and finish (`close`, `connection.ts:197-209`).
        if let Some((code, reason)) = state.closing.take() {
            let frame = CloseFrame {
                code,
                reason: truncated_reason(&reason).into(),
            };

            // The peer's echo is not waited for: tungstenite finishes the handshake on its own, and a
            // half-open client must not keep this task - or a parked replay window - alive.
            let _ = timeout(
                CLOSE_HANDSHAKE_BUDGET,
                socket.send(Message::Close(Some(frame))),
            )
            .await;

            break;
        }

        // 5. Nothing owed: wait for the next event.
        tokio::select! {
            biased;

            // Any inbound socket event is the liveness signal the heartbeat budget measures
            // (`socket.onActivity`, `connection.ts:377-379`).
            maybe = socket.next() => {
                state.last_activity = Instant::now();

                match maybe {
                    Some(Ok(Message::Text(text))) => {
                        state.handle_text(&text, &broker);
                    }
                    Some(Ok(Message::Binary(_))) => {
                        // The conversation is JSON text only, so a binary frame is a framing refusal,
                        // not a protocol error: 1003 with the transport's own sentence, no frame first
                        // (`transport.ts:449`).
                        state.request_close(
                            close_codes::UNSUPPORTED_DATA,
                            "This conversation carries JSON text frames only",
                        );
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    // A protocol-level ping is answered by tungstenite itself; a pong is the client's
                    // reply to our heartbeat and only ever refreshes the liveness stamp.
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                }
            }

            Some(outbound) = inbound.recv() => {
                state.push(outbound);
            }

            _ = state.heartbeat.tick() => {
                if state.silent_too_long() {
                    // Silence for `heartbeat_timeout_intervals` intervals means the peer is gone: this
                    // is how a half-open browser tab stops holding a subscription open.
                    state.request_close(
                        u16::try_from(limits.heartbeat_timeout_close_code)
                            .unwrap_or(close_codes::HEARTBEAT_TIMEOUT),
                        "No heartbeat reply.",
                    );
                } else {
                    state.push(Outbound {
                        conn_id,
                        frame: protocol::heartbeat_frame(now_millis()),
                    });
                }
            }
        }
    }

    // The peer is gone, or we closed it: release the subscriptions (each keeps a parked replay window
    // for a resume) and stop every timer. The run itself is NOT cancelled.
    broker.detach(conn_id);
}

/// The connection's client-side health, its send queue, and its frame budget - everything the broker
/// cannot see.
struct ConnectionState {
    conn_id: u64,
    limits: WsStreamLimits,
    /// The queue waiting for the socket, oldest first (`transport.ts`'s pending list).
    queue: VecDeque<Outbound>,
    /// Bytes currently queued, so `send_queue_bytes` is measured the way the contract states it.
    queued_bytes: usize,
    /// Frames lost while the queue was over budget, collected per subscription so ONE
    /// `resync_required` covers a burst instead of one frame per loss (`connection.ts:66`).
    lost: BTreeMap<String, u64>,
    /// Consecutive frames that failed the client union; a valid frame resets it (`connection.ts:291`).
    invalid_streak: u32,
    /// `hello` is honoured once per connection; a repeat changes nothing (`connection.ts:219-224`).
    hello_seen: bool,
    /// The first `subscribe` closes the `hello` window (`connection.ts:213-216`).
    first_subscribe_sent: bool,
    /// Messages counted in the current one-second window (`connection.ts:296-310`).
    messages_this_window: u32,
    window_started: Instant,
    last_activity: Instant,
    /// When the queue first went over budget and stayed there; the stall clock measures a queue that
    /// STAYS full, which is a property of the queue and not of whether an eviction was possible.
    stall_since: Option<Instant>,
    /// The close this connection owes, once everything already queued has been written. The first
    /// request wins, exactly as TS's `closing` guard does.
    closing: Option<(u16, String)>,
    heartbeat: Interval,
}

impl ConnectionState {
    fn new(conn_id: u64, limits: WsStreamLimits) -> Self {
        let interval = Duration::from_millis(limits.heartbeat_interval_ms);

        let mut heartbeat =
            tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        // A server that was descheduled for two intervals must not fire twice and close a healthy
        // client with 4408 (`MissedTickBehavior::Delay` restrikes from the missed tick).
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self {
            conn_id,
            invalid_streak: 0,
            hello_seen: false,
            first_subscribe_sent: false,
            messages_this_window: 0,
            queued_bytes: 0,
            queue: VecDeque::new(),
            lost: BTreeMap::new(),
            window_started: Instant::now(),
            last_activity: Instant::now(),
            stall_since: None,
            closing: None,
            limits,
            heartbeat,
        }
    }

    /// The queue depth the contract counts: everything waiting for the socket. The broker channel is
    /// drained before the first eviction check of each pass (`serve`'s step 1), so the local queue IS
    /// the backlog the contract's `send_queue_frames` refers to.
    #[must_use]
    fn queued_frames(&self) -> usize {
        self.queue.len()
    }

    /// `overBudget` (`connection.ts:111-114`), measured with the caller's next frame included.
    #[must_use]
    fn over_budget(&self, extra_bytes: usize) -> bool {
        self.queued_frames() >= usize::try_from(self.limits.send_queue_frames).unwrap_or(usize::MAX)
            || self.queued_bytes.saturating_add(extra_bytes)
                > usize::try_from(self.limits.send_queue_bytes).unwrap_or(usize::MAX)
    }

    /// Queues one frame, evicting the OLDEST droppable `update` while an `update` will not fit
    /// (`deliver`, `connection.ts:130-165`).
    ///
    /// Never a `welcome`, `subscription_state`, `resync_required`, `error`, `heartbeat` or `pong`, and
    /// never a frame that would leave a display op stream half-applied inside one frame - which the
    /// "drop whole frames only" rule gives us for free.
    fn push(&mut self, outbound: Outbound) {
        // A close has been decided: nothing else goes on the wire. TS's `send` refuses once `closing`
        // is set, and its transport flushes what is already queued (`connection.ts:79-84`).
        if self.closing.is_some() {
            return;
        }

        let extra_bytes = outbound.bytes();

        if outbound.is_droppable() {
            while self.over_budget(extra_bytes) {
                match self.evict_oldest_droppable() {
                    Some(subscription_id) => {
                        *self.lost.entry(subscription_id).or_insert(0) += 1;
                    }
                    None => break,
                }
            }

            // The stall clock: a queue that is STILL full after eviction is stalled, whether or not an
            // eviction was possible - a backlog of only non-droppable frames is just as stalled, and
            // 1013 is the honest answer for it (`connection.ts:150-153`).
            if self.over_budget(extra_bytes) {
                let stalled_for = self
                    .stall_since
                    .map(|since| {
                        since.elapsed() >= Duration::from_millis(self.limits.sustained_stall_ms)
                    })
                    .unwrap_or(false);

                if self.stall_since.is_none() {
                    self.stall_since = Some(Instant::now());
                } else if stalled_for {
                    self.request_close(close_codes::TRY_AGAIN_LATER, "The send queue stalled.");

                    return;
                }
            } else {
                self.stall_since = None;
            }
        }

        self.queued_bytes += extra_bytes;
        self.queue.push_back(outbound);
    }

    /// Removes and returns the subscription id of the OLDEST queued `update`
    /// (`evictOldestDroppable`, `transport.ts:68`).
    fn evict_oldest_droppable(&mut self) -> Option<String> {
        let index = self.queue.iter().position(Outbound::is_droppable)?;
        let evicted = self.queue.remove(index)?;

        self.queued_bytes = self.queued_bytes.saturating_sub(evicted.bytes());

        Some(evicted.subscription_id().unwrap_or_default().to_owned())
    }

    /// Takes the accumulated losses, so the caller can report each subscription exactly once
    /// (`flushLossReports`, `connection.ts:184-191`).
    fn take_lost(&mut self) -> Vec<(String, u64)> {
        std::mem::take(&mut self.lost).into_iter().collect()
    }

    /// The next frame owed to the socket.
    fn pop(&mut self) -> Option<Outbound> {
        let outbound = self.queue.pop_front()?;

        self.queued_bytes = self.queued_bytes.saturating_sub(outbound.bytes());

        if !self.over_budget(0) {
            self.stall_since = None;
        }

        Some(outbound)
    }

    /// Records the close this connection owes. The first request wins, because TS's `close` is guarded
    /// by `closing` (`connection.ts:197-199`) and the first decision is always the one the contract
    /// reached for.
    fn request_close(&mut self, code: u16, reason: impl Into<String>) {
        self.closing.get_or_insert((code, reason.into()));
    }

    /// `clock() - lastActivityAt >= heartbeat_interval_ms * heartbeat_timeout_intervals`
    /// (`connection.ts:365`).
    #[must_use]
    fn silent_too_long(&self) -> bool {
        let budget = Duration::from_millis(self.limits.heartbeat_interval_ms)
            * u32::try_from(self.limits.heartbeat_timeout_intervals).unwrap_or(3);

        self.last_activity.elapsed() >= budget
    }

    /// The per-second message budget (`connection.ts:296-310`), checked AFTER the schema question has
    /// had its turn for this frame - a malformed flood is reported as `invalid_payload` first.
    fn allow_rate(&mut self) -> bool {
        let now = Instant::now();

        if now.duration_since(self.window_started) >= Duration::from_millis(1_000) {
            self.window_started = now;
            self.messages_this_window = 0;
        }

        self.messages_this_window += 1;

        self.messages_this_window
            <= u32::try_from(self.limits.client_messages_per_second).unwrap_or(u32::MAX)
    }

    /// The rate-limit refusal, in the contract's own wording and with its close code
    /// (`connection.ts:305-309`).
    fn rate_limited(&self) -> StreamFailure {
        StreamFailure::with_close(
            "rate_limited",
            format!(
                "A client may send at most {} messages per second.",
                self.limits.client_messages_per_second
            ),
            close_codes::RATE_LIMITED,
        )
    }

    /// One inbound text frame: parse, then dispatch (`handleMessage`, `connection.ts:276-345`).
    fn handle_text(&mut self, text: &str, broker: &StreamBroker) {
        let outcome = protocol::parse_client_frame(text);

        let frame = match outcome {
            ParseOutcome::Failure(failure) => {
                self.invalid_streak += 1;
                self.push(Outbound {
                    conn_id: self.conn_id,
                    frame: protocol::error_frame(&failure),
                });

                // `frame_too_large` arrives with a close code of its own (1009 or 1003), and TS closes
                // as soon as that frame is on the wire. Three consecutive union failures close with
                // 1007; a single bad frame never kills a live UI.
                if let Some(code) = failure.close_code {
                    self.request_close(code, failure.message);
                } else if self.invalid_streak >= 3 {
                    self.request_close(close_codes::INVALID_PAYLOAD, "Repeated invalid frames.");
                }

                return;
            }
            ParseOutcome::Frame(frame) => {
                // A frame that validates resets the streak: the contract counts CONSECUTIVE failures.
                self.invalid_streak = 0;

                if !self.allow_rate() {
                    let failure = self.rate_limited();

                    self.push(Outbound {
                        conn_id: self.conn_id,
                        frame: protocol::error_frame(&failure),
                    });
                    self.request_close(close_codes::RATE_LIMITED, failure.message);

                    return;
                }

                frame
            }
        };

        match frame {
            ClientFrame::Hello { cursor, .. } => self.handle_hello(cursor, broker),
            ClientFrame::Subscribe(request) => {
                self.first_subscribe_sent = true;

                match broker.subscribe(self.conn_id, request) {
                    SubscribeOutcome::Rejected { failure } => {
                        let close_code = failure.close_code;
                        let message = failure.message.clone();

                        self.push(Outbound {
                            conn_id: self.conn_id,
                            frame: protocol::error_frame(&failure),
                        });

                        // `too_many_subscriptions` closes with 4401 after the frame (the precedence
                        // mapping), and it is the only rejection with a close code attached.
                        if let Some(code) = close_code {
                            self.request_close(code, message);
                        }
                    }
                    SubscribeOutcome::Resync { resync } => {
                        self.push(Outbound {
                            conn_id: self.conn_id,
                            frame: resync,
                        });
                    }
                    SubscribeOutcome::Ok { state, replay } => {
                        // `subscription_state` is the reconciliation anchor and is therefore never
                        // droppable; the replayed updates that follow it are, because a later resync
                        // can always recover them (`connection.ts:269-273`).
                        self.push(Outbound {
                            conn_id: self.conn_id,
                            frame: state,
                        });

                        for frame in replay {
                            self.push(Outbound {
                                conn_id: self.conn_id,
                                frame,
                            });
                        }
                    }
                }
            }
            ClientFrame::Unsubscribe { subscription_id } => {
                if let Err(failure) = broker.unsubscribe(self.conn_id, &subscription_id) {
                    self.push(Outbound {
                        conn_id: self.conn_id,
                        frame: protocol::error_frame(&failure),
                    });
                }
            }
            ClientFrame::Ack {
                subscription_id,
                sequence,
            } => {
                if let Err(failure) = broker.acknowledge(self.conn_id, &subscription_id, sequence) {
                    self.push(Outbound {
                        conn_id: self.conn_id,
                        frame: protocol::error_frame(&failure),
                    });
                }
            }
            ClientFrame::Ping { at } => {
                // `pong` echoes the `at` the client sent, or the server clock when it sent none.
                self.push(Outbound {
                    conn_id: self.conn_id,
                    frame: protocol::pong_frame(at.unwrap_or_else(|| now_millis() as f64)),
                });
            }
            ClientFrame::Bye { reason } => {
                // A clean goodbye: close 1000, which is how a client releases its subscriptions without
                // looking like a dropped connection in the server's diagnostics.
                self.request_close(close_codes::NORMAL, reason.unwrap_or_default());
            }
        }
    }

    /// `hello` is the ONLY resume input, and it is honoured only as the first frame. A late `hello`
    /// answers `late_hello` and changes nothing; the connection stays usable (spec `resume.rules`).
    fn handle_hello(&mut self, cursor: Option<String>, broker: &StreamBroker) {
        if self.first_subscribe_sent {
            self.push(Outbound {
                conn_id: self.conn_id,
                frame: protocol::error_frame(&StreamFailure::new(
                    "late_hello",
                    "hello must arrive before the first subscribe.",
                )),
            });

            return;
        }

        if self.hello_seen {
            // A repeated hello is not a contract condition; ignoring the later one is the only reading
            // that keeps "the first frame" meaningful without inventing an error code.
            return;
        }

        self.hello_seen = true;

        // `session.setResume` (`connection.ts:226`): the cursor every later `subscribe` falls back to,
        // unless it carries one of its own.
        broker.resume(self.conn_id, cursor);
    }
}

/// Writes one frame to the socket.
enum Written {
    Sent,
    /// The write blocked past `sustained_stall_ms`, which the contract answers with 1013.
    Stalled,
    /// The socket is gone; nothing else is readable, and TS's transport closes the same way.
    Gone,
}

async fn write(socket: &mut WebSocket, outbound: &Outbound, limits: &WsStreamLimits) -> Written {
    let text = serde_json::to_string(&outbound.frame).unwrap_or_else(|_| "{}".to_owned());

    // `sustained_stall_ms` is the contract's bound on how long a write may block before the queue is
    // called stalled; TS measures the queue, this measures the write that cannot drain it, which is the
    // same fact on a socket the peer has stopped reading.
    let budget = Duration::from_millis(limits.sustained_stall_ms);

    match timeout(budget, socket.send(Message::Text(text.into()))).await {
        Err(_) => Written::Stalled,
        Ok(Err(_)) => Written::Gone,
        Ok(Ok(())) => Written::Sent,
    }
}

/// A WS close reason must fit in 123 bytes (RFC 6455 5.5.1), so an over-long message is cut at a
/// character boundary rather than being sent as a frame the peer will refuse.
fn truncated_reason(reason: &str) -> String {
    if reason.len() <= 123 {
        return reason.to_owned();
    }

    let mut keep = 123;

    while keep > 0 && !reason.is_char_boundary(keep) {
        keep -= 1;
    }

    reason[..keep].to_owned()
}
