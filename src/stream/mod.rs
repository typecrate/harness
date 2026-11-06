//! The `/v1/harness/stream` update conversation: the handshake, the broker, and the frames.
//!
//! TypeScript origin: `packages/underload/harness/src/server/stream/` - `protocol.ts` (the wire
//! vocabulary, this module's `protocol`), `broker.ts` (counters, subscriptions, sequences, replay
//! windows, this module's `broker`), `connection.ts` (the per-connection conversation, this module's
//! `connection`), `transport.ts` (the handshake gates, folded into [`upgrade`] below) and `mount.ts`
//! (the origin policy plus the store bridge, folded into [`upgrade`] and `runtime.rs`).
//!
//! WHY THE TRANSPORT MODULE DISAPPEARS HERE
//! ----------------------------------------
//! TypeScript hand-rolls an RFC 6455 transport (`stream/transport.ts`, 677 lines) because
//! `@underload/harness` ships with two runtime dependencies and cannot take `ws`. This crate already
//! depends on axum with the `ws` feature, so hyper + tokio-tungstenite own the frame codec, the
//! masking rules, the fragmentation and the close handshake. What is left - and what this module
//! owns - is the contract: the gate order and the exact refusal sentences, the subprotocol echo, the
//! `X-Harness-Epoch` header on the 101, and everything above the wire.
//!
//! THE GATE ORDER IS THE CONTRACT (`transport.ts:603-645`, `api.ts:3906-3912`).
//! A refusal happens BEFORE the upgrade, so it is an HTTP status with the `{error:{message}}`
//! envelope, never a WS close frame. `mount.ts:12-16` states why the origin gate runs first: it is a
//! policy refusal, not an authentication challenge, and it must answer before any per-root state is
//! touched.
//!
//! Cross-references:
//!   * `server.rs:53-70` - the route that reaches [`upgrade`] (`ANY {root}/stream`).
//!   * `api::handle_stream` - the one-line delegation T5a left as a stub.
//!   * `runtime.rs` - `stream_broker`, the per-runtime broker every connection shares, so REST and WS
//!     read one epoch and one set of counters.
//!   * `origin.rs:47,140` - the 403 sentence and the rule behind it.

pub mod broker;
pub mod connection;
pub mod protocol;

pub use broker::{Outbound, StreamBroker, SubscribeOutcome};
pub use protocol::{StreamFailure, StreamScopeIds, SubscribeRequest};

use std::path::PathBuf;

use axum::extract::FromRequestParts as _;
use axum::extract::Request;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use serde_json::json;

use crate::api;
use crate::contract;
use crate::events;
use crate::origin;
use crate::runtime;

use connection::serve;

/// The refusal sentences the handshake may print, verbatim from `transport.ts:613-644`.
///
/// They are constants rather than inline literals because `tests/stream.rs` asserts each one, and a
/// typo in a refusal is a contract break the test suite should catch by name.
pub mod gate {
    /// `transport.ts:618` - the upgrade is a GET.
    pub const METHOD: &str = "The stream upgrade is a GET.";
    /// `transport.ts:623` - the upgrade tokens are missing.
    pub const NOT_AN_UPGRADE: &str = "The request is not a WebSocket upgrade.";
    /// `transport.ts:630` - no key, so there is nothing to sign.
    pub const MISSING_KEY: &str = "A WebSocket upgrade requires Sec-WebSocket-Key.";
    /// `transport.ts:638` - the honest answer to a client speaking another version.
    pub const VERSION: &str = "This stream speaks WebSocket protocol version 13.";
}

/// `isWebSocketUpgrade` (`transport.ts:86-98`).
///
/// `Connection` may be a token list (`keep-alive, Upgrade`) and proxies fold case, so the test is a
/// case-insensitive TOKEN match rather than a string equality test; `Upgrade` is compared as a whole
/// lower-cased value.
#[must_use]
pub fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let connection = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect::<Vec<_>>()
        .join(",");

    if !connection
        .to_ascii_lowercase()
        .split(',')
        .any(|token| token.trim() == "upgrade")
    {
        return false;
    }

    headers
        .get_all(header::UPGRADE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|value| value.eq_ignore_ascii_case("websocket"))
}

/// Pathname of a request target, without touching the query string (`pathnameOf`, `transport.ts:114-123`).
///
/// A raw request target such as `//x` is matched LITERALLY, which is what the loader's route matching
/// does too - normalising here would create a second notion of "same path" and let
/// `/v1/harness/stream/` slip past an exact-path gate.
#[must_use]
pub fn pathname_of(uri: Option<&str>) -> String {
    let value = uri.unwrap_or("/");
    let marker = value.find('?').unwrap_or(value.len());

    value[..marker].to_owned()
}

/// The stream mount (`api.ts:3906-3912` plus `stream/{transport,mount}.ts`).
///
/// T5a answered every request with TypeScript's own honest non-upgrade 400; this is the upgrade body
/// that replaces it. The gates run in TS's order, and each refusal is the TS sentence:
///
/// 1. the ORIGIN gate (`mount.ts:68-81`) - a policy refusal, 403, before any state exists;
/// 2. the runtime for this root (needed for the epoch header every JSON answer carries);
/// 3. the capability advertisement (`api.ts:3907-3911`): a deployment that withholds the stream
///    refuses rather than accepting an upgrade it will not answer;
/// 4. the method (`transport.ts:617-620`), 405;
/// 5. the upgrade tokens (`transport.ts:622-625`), 400 - the case `api.ts:3911` answers with its own
///    sentence, because a request that never asked for an upgrade is the dispatcher's case;
/// 6. `Sec-WebSocket-Key` (`:627-632`), 400;
/// 7. `Sec-WebSocket-Version` (`:634-640`), 426 plus the version header;
/// 8. the exact path (`:642-645`), 404 naming the path the client used;
/// 9. the upgrade itself, with `harness.v1` offered back only when the client offered it, and
///    `X-Harness-Epoch: <epoch>` on the 101 (`mount.ts:146`).
///
/// A non-upgrade `GET` therefore still receives the TS 400 sentence, which is what `api.ts:3911`,
/// `queue.test.ts:1090` and `view.test.ts:779` pin.
pub async fn upgrade(root: PathBuf, request: Request) -> Response {
    let (mut parts, body) = request.into_parts();
    let headers = parts.headers.clone();
    let target = parts.uri.path().to_owned();
    let method = parts.method.clone();

    // 1. The origin gate runs before anything exists, so its refusal quotes the PROCESS epoch - the
    // same string a runtime for this root would quote (`events::process_epoch`).
    if let Err(error) = origin::check_stream_origin(
        header_text(&headers, "origin").as_deref(),
        header_text(&headers, "host").as_deref(),
    ) {
        return json_error(error.status, &error.message, events::process_epoch(), None);
    }

    // 2. The runtime for this root: its epoch is the 101's header, and its broker is the connection.
    let runtime_for_request = match runtime::runtime_for(&root) {
        Ok(active) => active,
        Err(error) => {
            return json_error(error.status, &error.message, events::process_epoch(), None);
        }
    };

    let epoch = runtime_for_request.epoch();

    // 3. A deployment that withholds `capabilities.websocket_stream` never accepts the upgrade, which
    // is the same refusal `api::root::stream_route` prints for the non-upgrade answer.
    if !contract::service_capabilities().websocket_stream {
        let error = api::capability_denied(
            &format!("GET {}/stream", contract::HARNESS_API_ROOT),
            "a live stream on this deployment",
        );

        return json_error(error.status, &error.message, &epoch, None);
    }

    // 4. `transport.ts:617`. Hyper has already parsed the verb, so this is the same test on the same
    // header set.
    if method != Method::GET {
        return json_error(
            StatusCode::METHOD_NOT_ALLOWED.as_u16(),
            gate::METHOD,
            &epoch,
            None,
        );
    }

    // 5. The upgrade tokens. A request that carries neither is the case `api.ts:3906-3912` answers,
    // and its sentence is the one the REST suite pins, so the honest mapping is: tokens present but
    // wrong -> `transport.ts:623`, tokens absent entirely -> the dispatcher's 400.
    if !is_websocket_upgrade(&headers) {
        let asked_but_wrong = headers.contains_key(header::UPGRADE)
            || headers
                .get_all(header::CONNECTION)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .any(|value| {
                    value
                        .to_ascii_lowercase()
                        .split(',')
                        .any(|token| token.trim() == "upgrade")
                });

        return json_error(
            StatusCode::BAD_REQUEST.as_u16(),
            if asked_but_wrong {
                gate::NOT_AN_UPGRADE
            } else {
                STREAM_UPGRADE_SENTENCE
            },
            &epoch,
            None,
        );
    }

    // 6. `transport.ts:627-632`.
    let key = headers
        .get(header::SEC_WEBSOCKET_KEY)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();

    if key.is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST.as_u16(),
            gate::MISSING_KEY,
            &epoch,
            None,
        );
    }

    // 7. `transport.ts:634-640`: 426 plus the version header is the RFC's answer to a client
    // speaking another version.
    let version = headers
        .get(header::SEC_WEBSOCKET_VERSION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();

    if version != "13" {
        return json_error(
            StatusCode::UPGRADE_REQUIRED.as_u16(),
            gate::VERSION,
            &epoch,
            Some(("Sec-WebSocket-Version", "13")),
        );
    }

    // 8. `transport.ts:642-645` - the exact path, query string stripped.
    let pathname = pathname_of(Some(&target));

    if pathname != contract::HARNESS_WS_STREAM_PATH {
        return json_error(
            StatusCode::NOT_FOUND.as_u16(),
            &format!("No stream is mounted at {pathname}."),
            &epoch,
            None,
        );
    }

    // 9. The upgrade. Every gate above has already decided the questions this extractor re-asks
    // (method, tokens, key, version), so its remaining rejection is the one this port cannot see
    // from a header: hyper did not mark the connection upgradable. That is still "a GET that asked
    // for an upgrade it did not send", which is the dispatcher's 400.
    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(upgrade) => upgrade,
        Err(_) => {
            drop(body);

            return json_error(
                StatusCode::BAD_REQUEST.as_u16(),
                STREAM_UPGRADE_SENTENCE,
                &epoch,
                None,
            );
        }
    };

    // The body is the handshake's, and a GET carries none; dropping it is what lets hyper finish the
    // request before the socket is handed over.
    drop(body);

    let broker = runtime_for_request.stream_broker();

    let mut response = upgrade
        .protocols([contract::HARNESS_WS_SUBPROTOCOL])
        .on_upgrade(move |socket| serve(socket, runtime_for_request, broker));

    // `mount.ts:146` - the epoch rides the 101 so a client learns it is on a different server before
    // it sends a single frame.
    response.headers_mut().insert(
        contract::HARNESS_EPOCH_HEADER,
        HeaderValue::from_str(&epoch).unwrap_or_else(|_| HeaderValue::from_static("")),
    );

    response
}

/// The TS dispatcher's own sentence for this mount (`api.ts:3911`), kept as one constant because both
/// the plain-GET answer and a failed extractor print it.
const STREAM_UPGRADE_SENTENCE: &str = "GET /stream expects a WebSocket upgrade (Sec-WebSocket-Key/Version) for the harness.v1 subprotocol.";

/// One handshake refusal: the `{error:{message}}` envelope with the headers every JSON answer carries
/// (`api.ts:4030-4064`), plus the one extra header the 426 arm needs.
///
/// This mirrors `api::write_error`, which is private to the dispatcher; the stream cannot import it
/// without making `api/mod.rs` wider than this stage owns, so the shape is restated here and pinned
/// by `tests/stream.rs`.
#[must_use]
fn json_error(status: u16, message: &str, epoch: &str, extra: Option<(&str, &str)>) -> Response {
    let body = json!({ "error": { "message": message } });
    let text = serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string());

    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        )
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .header(contract::HARNESS_EPOCH_HEADER, epoch)
        .header(header::CONTENT_LENGTH, text.len().to_string());

    if let Some((name, value)) = extra {
        builder = builder.header(name, value);
    }

    builder
        .body(axum::body::Body::from(text))
        // The builder only fails on a header name/value that is not representable on the wire, and
        // every value here is ASCII or a number. The honest answer when it does happen is a bare 500,
        // because the generated headers are the thing that failed.
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap_or_default()
        })
}

/// One header value as text, `None` when absent or not UTF-8 (the dispatcher's own `header_text`).
#[must_use]
fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
