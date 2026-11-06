//! The `/v1/harness` dispatcher: route table, request context, body policy, response envelope.
//!
//! TypeScript origin: `src/server/api.ts` - `compilePattern`/`matchRoute` (`:616-687`),
//! `relativeSegments` (`:689-705`), `bodyLimitFor` (`:707-723`), the route table
//! (`:3925-3974`), `dispatch` (`:3980-4028`), the error envelope (`:4030-4064`),
//! `withHeaders`/`json` (`:1224-1233`), and the body/param helpers `queryOf`, `paramOf`,
//! `enumParamOf`, `requireKnownFields`, `requireObjectBody`, `readEmptyBody` (`:455-531`) - plus
//! the transport-side body reading of `src/server/handler.ts` (`readJson`, `:97-137`) and
//! `src/server/service.ts` (`readBoundedBody`, `:357-396`, and the declared-length fast refusal at
//! `:343-349`), because the Rust port has ONE adapter (axum) instead of two.
//!
//! WHAT THIS FILE IS THE SINGLE OWNER OF
//! -------------------------------------
//! 1. **The route table.** `HARNESS_PATH_TEMPLATES` (`shared/harness-api.ts:54-100`, 45 entries)
//!    crossed with the 64 operations `api.ts:3925-3974` declares. `api.ts:623-625` states why the
//!    table is compiled from the GENERATED template strings rather than typed out by hand: "a
//!    route cannot drift from the contract by being typed out by hand here". The same reason holds
//!    here, so `ROUTES` below carries each template verbatim next to its operations.
//! 2. **`Ctx`.** Every handler - this stage's host handlers and T5b's session handlers - has the
//!    one shape `async fn(&mut Ctx) -> ApiResult`. The route table maps a path + verb onto one of
//!    those functions, so adding an operation never changes the dispatch mechanism.
//! 3. **The body policy.** Bodies are read LAZILY, per route, under the route's own cap
//!    (`bodyLimitFor`), because "a route that rejects before looking at the body (an unknown id)
//!    never consumes it" (`api.ts:4022-4024`) - and because a 405 must not drain a body an adapter
//!    may still want (`api.ts:4009`).
//! 4. **The response envelope.** `{"error":{"message": ...}}` for every failure, and the contract
//!    headers on every JSON response (`api.ts:1224-1230`, `harness-api.ts:15-18`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use http_body_util::BodyExt;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::contract;
use crate::error::ApiError;
use crate::events;
use crate::origin;
use crate::paging;
use crate::ports::Query;
use crate::runtime::{self, HarnessRuntime};
use crate::stream;

pub mod actions;
pub mod agents;
pub mod config;
pub mod harnesses;
pub mod models;
pub mod queue;
pub mod root;
pub mod session_agents;
pub mod session_tools;
pub mod sessions;
pub mod threads;
pub mod tools;
pub mod transcript;
pub mod turns;
pub mod usage;

// ---------------------------------------------------------------------------
// The result shape
// ---------------------------------------------------------------------------

/// One successful dispatch result: the status, the JSON body, and the resource revision a
/// `X-Harness-Revision` header quotes when there is one (`HarnessApiResult`'s `kind: 'json'` arm,
/// `api.ts:384-386`, with the header values `withHeaders` puts on `headers` at `:1224-1230`).
#[derive(Debug, Clone)]
pub struct ApiResponse {
    pub status: u16,
    pub body: Value,
    /// `Some(n)` stamps `X-Harness-Revision`. `None` means the contract's "a receipt without a
    /// revision simply carries none" (`api.ts:1640-1643`).
    pub revision: Option<u64>,
}

/// What every handler returns. `ApiError` is this crate's one refusal type, and its `status` is
/// the whole story of the error path, so a handler never builds an error response itself.
pub type ApiResult = Result<ApiResponse, ApiError>;

// ---------------------------------------------------------------------------
// The request context
// ---------------------------------------------------------------------------

/// Everything a handler may read about one request (`Ctx`, `api.ts:1239-1248`).
///
/// `mount` is absent on purpose: the mount is always [`contract::HARNESS_API_ROOT`] on this
/// adapter (`api.ts:3987-3989` refuses every other mount, so a handler can never observe one), and
/// the two messages that quote the mount build it from the request path instead.
pub struct Ctx {
    /// The runtime for this request's database root.
    pub runtime: std::sync::Arc<HarnessRuntime>,
    /// Upper-cased verb (`api.ts:3981`).
    pub method: String,
    /// The path the client asked for, INCLUDING the mount prefix, because "an error message can
    /// quote the URL the caller actually used" (`api.ts:351-352`).
    pub pathname: String,
    /// The parsed query string (`queryOf`, `api.ts:455-479`). A repeated key keeps its LAST value
    /// here, where TS's `URLSearchParams.get` keeps the FIRST; no route in the contract reads a
    /// repeated parameter, so the two agree on every documented request.
    pub query: Query,
    /// The path parameters the matched template captured, keyed by the TEMPLATE name
    /// (`session_id`, not `:0`) so "a handler reads a stable key regardless of the value in the
    /// path" (`api.ts:670-673`).
    pub params: BTreeMap<String, String>,
    /// The request headers (the origin gate and the body's content-type rule read these).
    pub headers: HeaderMap,
    /// The lazily-read body.
    pub body: BodyReader,
}

impl Ctx {
    /// The parsed JSON body, or `{}` when the request carried none (`readJson`'s empty-chunks arm,
    /// `handler.ts:120-123`). Reads at most the route's cap, and caches the value so a second read
    /// costs nothing (TS's `readBody` is called by one route at most, but `body_object` and
    /// `body_value` both route through here).
    pub async fn body_value(&mut self) -> Result<Value, ApiError> {
        self.body.read().await
    }

    /// A body that must be a JSON object (`requireObjectBody`, `api.ts:512-518`): an array, a
    /// scalar, or `null` is the same 400, whichever route asked.
    pub async fn body_object(&mut self) -> Result<Map<String, Value>, ApiError> {
        let value = self.body.read().await?;

        contract::require_object(&value).cloned()
    }

    /// A body decoded into a typed request (`decode_body`, `contract.rs:3003`), so a shape or type
    /// mismatch is a 400 naming what serde reported, never a 500 from an unwrap.
    pub async fn body_typed<T>(&mut self) -> Result<T, ApiError>
    where
        T: serde::de::DeserializeOwned,
    {
        let value = self.body.read().await?;

        contract::decode_body(value)
    }

    /// A route documented as body-less (`readEmptyBody`, `api.ts:520-531`): the body IS still read,
    /// because a malformed one must be refused rather than ignored; anything other than an absent
    /// body, `null`, or `{}` is then a 400.
    ///
    /// NOT used by any route today: the TypeScript server never calls `readEmptyBody` (it is defined
    /// and left uncalled), and its GET/DELETE handlers never touch `ctx.body()` - so a body on those
    /// verbs is ignored rather than refused. See [`Ctx::ignore_body`] and `knowledge.md` "T7b".
    pub async fn require_empty_body(&mut self) -> Result<(), ApiError> {
        let value = self.body.read().await?;

        contract::require_empty_body(&value)
    }

    /// The GET/DELETE/reset rule: TypeScript never reads the body on these verbs, so a body is
    /// IGNORED - not parsed (a malformed one stays a success) and not refused.
    ///
    /// The stream is still drained, which is the adapter's own requirement rather than the
    /// contract's: the answer is flushed the moment the handler returns, and a client still writing
    /// its upload would read a connection reset instead of the JSON document
    /// (`drain_unread_body`'s note). Awaiting here also keeps every handler's signature uniform, so
    /// the dispatcher's table stays data.
    pub async fn ignore_body(&mut self) {
        self.body.drain_pending().await;
    }

    /// One raw query value, untrimmed (`ctx.query.get(name)`, which the paging routes hand straight
    /// to `resolveLimit`/`decodeCursor`; those two do their own trimming).
    #[must_use]
    pub fn query_str(&self, key: &str) -> Option<&str> {
        self.query.get(key).map(String::as_str)
    }

    /// `resolveLimit(ctx.query.get('limit'))` (`api.ts:1489` and every other paginated route).
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] `Query parameter "limit" must be an integer between 1 and 100.`
    pub fn limit(&self) -> Result<u64, ApiError> {
        paging::resolve_limit(self.query_str("limit"))
    }

    /// `decodeCursor(ctx.query.get('cursor'))` (`api.ts:1490`), stamped with THIS runtime's epoch,
    /// which is what makes a cursor from a previous process a refusal rather than an off-by-epoch
    /// page.
    ///
    /// # Errors
    ///
    /// [`ApiError::bad_request`] for a token this server did not issue, or a stale/foreign one.
    pub fn cursor(&self) -> Result<u64, ApiError> {
        paging::decode_cursor(self.runtime.hub.epoch(), self.query_str("cursor"))
    }

    /// One captured path parameter.
    #[must_use]
    pub fn param(&self, name: &str) -> &str {
        self.params.get(name).map_or("", String::as_str)
    }

    /// The current counter of one resource (`events.getRevision`, which every identified read
    /// stamps on its response header).
    #[must_use]
    pub fn revision(&self, resource: &str, id: Option<&str>) -> u64 {
        self.runtime.hub.revision(resource, id)
    }
}

// ---------------------------------------------------------------------------
// The bounded body reader
// ---------------------------------------------------------------------------

/// The request body, read at most once, under the route's cap.
///
/// Three states because the read is lazy AND its outcome is sticky: a route that refuses before
/// touching the body must leave the stream alone (`api.ts:4009`, `:4022-4024`), and a route that
/// reads a rejected body twice must report the SAME refusal rather than an empty second read.
pub enum BodyReader {
    /// Not read yet: the stream, the cap from [`body_limit_for`], the declared content type, and
    /// the declared `content-length` (which [`service.ts:504-509`] refuses before a byte is read).
    Pending {
        body: Option<Body>,
        limit: usize,
        content_type: Option<String>,
        declared: Option<u64>,
    },
    /// Read and parsed (or genuinely absent, which is `{}`).
    Ready(Value),
    /// Refused: 413, 415 or 400, replayed on every later read.
    Failed(ApiError),
}

impl BodyReader {
    /// A body that has not been read yet.
    #[must_use]
    pub fn pending(body: Body, limit: usize, headers: &HeaderMap) -> Self {
        Self::Pending {
            body: Some(body),
            limit,
            content_type: headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            declared: declared_length(headers),
        }
    }
}

impl BodyReader {
    /// Throws away an unread body WITHOUT parsing it: no cap check, no media-type check, no JSON
    /// decode. This is what a body-less verb does with a body the contract never documents
    /// ([`Ctx::ignore_body`]), and it is also the dispatcher's sweep after a handler that refused
    /// before looking at the stream.
    ///
    /// The state becomes `Ready({})` so a later sweep does not read the same (now empty) stream
    /// twice; the parsed value is deliberately never produced, because the whole point is that these
    /// bytes are not interpreted.
    pub async fn drain_pending(&mut self) {
        if matches!(self, Self::Pending { .. }) {
            drain(pending_stream(self), BODY_DRAIN_GRACE_BYTES).await;

            *self = Self::Ready(Value::Object(serde_json::Map::new()));
        }
    }

    /// Reads, caps, type-checks and parses the stream (`readJson` + `readBoundedBody`).
    pub async fn read(&mut self) -> Result<Value, ApiError> {
        let outcome = match self {
            Self::Ready(value) => return Ok(value.clone()),
            Self::Failed(error) => return Err(error.clone()),
            Self::Pending {
                body,
                limit,
                content_type,
                declared,
            } => {
                // Everything the read needs is copied out first, so no borrow of `self` is held
                // across the await while the state is still `Pending`.
                let stream = body.take().unwrap_or_else(Body::empty);
                let limit = *limit;
                let declared = *declared;
                let content_type = content_type.clone();

                read_bounded(stream, limit, content_type.as_deref(), declared).await
            }
        };

        match outcome {
            Ok(value) => {
                *self = Self::Ready(value.clone());
                Ok(value)
            }
            Err(error) => {
                *self = Self::Failed(error.clone());
                Err(error)
            }
        }
    }
}

/// The refusal for one oversized body (`handler.ts:109`, `service.ts:315`): the cap is stated in
/// bytes, and it is the ROUTE's cap, so the message tells the client which limit it hit.
#[must_use]
fn too_large(limit: usize) -> ApiError {
    ApiError::payload_too_large(format!("A request body cannot exceed {limit} bytes."))
}

/// The declared `content-length`, when the client stated one and it parses (`service.ts:343-345`).
///
/// The check is deliberately about the CLAIM, not the bytes: an honest length is refused at the door
/// for free, and a liar is caught by the running total inside [`read_bounded`].
#[must_use]
fn declared_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

/// Bytes the adapter is willing to DISCARD after an early body refusal, so the refusal reaches the
/// caller as a 413 rather than as a reset connection.
///
/// Hyper ends the connection once the handler answers with the request body still arriving, and a
/// client still writing into that socket gets `ConnectionReset` instead of the contract's envelope.
/// TypeScript's gateway cancels the reader and lets node absorb it (`service.ts:378-381`, "stop the
/// transfer rather than drain a body nobody will read"); the equivalent here is to keep reading and
/// throw the bytes away, bounded. Every body the contract ALLOWS fits inside this grace, so a client
/// that respects the cap always receives its 413/415/400, and only a client that streams an
/// arbitrary amount past the cap is dropped - which is exactly what TS's cancel does to it.
const BODY_DRAIN_GRACE_BYTES: usize = 32 * 1024 * 1024;

/// Reads and discards the rest of `body`, up to `budget` bytes, then gives up.
async fn drain(mut body: Body, mut budget: usize) {
    while budget > 0 {
        match body.frame().await {
            None => return,
            Some(Err(_)) => return,
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    budget = budget.saturating_sub(data.len());
                }
            }
        }
    }
}

/// Streams `body` under `limit`, then parses it as JSON.
///
/// * a declared `content-length` above the cap is refused BEFORE the first byte is read
///   (`service.ts:504-509`), which costs nothing and stops an honest client at the door;
/// * the running total is checked per frame, so an oversized submit is rejected before the whole
///   payload is buffered (`handler.ts:103-111`);
/// * a body of zero bytes (or whitespace only) is `{}` - the value TS resolves for a request that
///   carried no chunks, which is what keeps the "object body" rules unchanged for a GET
///   (`handler.ts:120-123`, `service.ts:361`);
/// * a non-empty body must declare `application/json` (the contract's 415; TS's Node adapter
///   inherits the check from its loader and the gateway from its pre-parse, so the sentence is
///   this port's, stated in the same voice as the two messages above);
/// * text that is not JSON is the 400 `handler.ts:128` and `service.ts:393` both print.
async fn read_bounded(
    mut body: Body,
    limit: usize,
    content_type: Option<&str>,
    declared: Option<u64>,
) -> Result<Value, ApiError> {
    if let Some(declared) = declared
        && declared > limit as u64
    {
        drain(body, BODY_DRAIN_GRACE_BYTES).await;

        return Err(too_large(limit));
    }

    let mut bytes: Vec<u8> = Vec::new();
    let mut over_cap = false;

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            ApiError::internal(format!("The request body could not be read: {error}"))
        })?;

        if let Some(data) = frame.data_ref() {
            if bytes.len() + data.len() > limit {
                // Refuse at the cap rather than after the whole payload has been allocated and
                // JSON-parsed (`handler.ts:103-111`).
                over_cap = true;
                break;
            }

            bytes.extend_from_slice(data);
        }
    }

    if over_cap {
        // The bytes still in flight are discarded, not interpreted, so the client finishes its write
        // and reads the 413 instead of a reset (`service.ts:379` cancels the reader instead).
        drain(body, BODY_DRAIN_GRACE_BYTES).await;

        return Err(too_large(limit));
    }

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Object(Map::new()));
    }

    require_json_content_type(content_type)?;

    serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::bad_request("The request body must be valid JSON."))
}

/// The media-type rule for a non-empty body: the contract documents 415 for a request body, and a
/// missing declaration is accepted exactly as TS accepts it (its adapters hand over already-parsed
/// JSON, which never says what it was).
fn require_json_content_type(content_type: Option<&str>) -> Result<(), ApiError> {
    let Some(raw) = content_type else {
        return Ok(());
    };

    if raw.to_ascii_lowercase().contains("application/json") {
        return Ok(());
    }

    Err(ApiError::unsupported_media_type(format!(
        "A request body must be application/json (content-type \"{raw}\" is not supported)."
    )))
}

/// The cap for one route (`bodyLimitFor`, `api.ts:707-723`).
///
/// Only `POST {root}/sessions/{id}/tasks` gets the 16 MiB submit cap, because only that route
/// carries raw base64 media inside JSON; every other route keeps 1 MB. The path test is the same
/// structural one TS uses - three segments under the mount, `sessions` first, `tasks` last - so a
/// nested path cannot pick up the larger limit by accident.
#[must_use]
pub fn body_limit_for(method: &str, pathname: &str) -> usize {
    if method.to_ascii_uppercase() != Method::POST.as_str() {
        return contract::MAX_BODY_BYTES;
    }

    let Some(segments) = mount_segments(pathname) else {
        return contract::MAX_BODY_BYTES;
    };

    if segments.len() == 3 && segments[0] == "sessions" && segments[2] == "tasks" {
        return contract::MAX_SUBMIT_BODY_BYTES;
    }

    contract::MAX_BODY_BYTES
}

// ---------------------------------------------------------------------------
// Path matching
// ---------------------------------------------------------------------------

/// The mount a request addressed. `api.ts:3987-3989` refuses a mount other than the canonical root
/// by NAME - "an alias would keep the old surface alive behind a flag nobody can see" - so a stale
/// `/api/...` client is told which root this server actually serves. On this adapter the mount is
/// the first path segment when the path is not already under the canonical root (`/api/x` names
/// `/api`, and `/` names `/`).
#[must_use]
pub fn requested_mount(pathname: &str) -> String {
    if pathname == contract::HARNESS_API_ROOT
        || pathname.starts_with(&format!("{}/", contract::HARNESS_API_ROOT))
    {
        return contract::HARNESS_API_ROOT.to_owned();
    }

    let first = pathname
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or_default();

    if first.is_empty() {
        "/".to_owned()
    } else {
        format!("/{first}")
    }
}

/// The mount-relative segments of a path, percent-decoded, or `None` when the path is not below the
/// canonical root at all (`relativeSegments`, `api.ts:689-705`).
///
/// A segment that fails to decode is kept verbatim, which is TS's `try { decodeURIComponent }
/// catch { segment }`.
#[must_use]
pub fn mount_segments(pathname: &str) -> Option<Vec<String>> {
    let root = contract::HARNESS_API_ROOT;

    if pathname != root && !pathname.starts_with(&format!("{root}/")) {
        return None;
    }

    let rest = if pathname == root {
        ""
    } else {
        &pathname[root.len()..]
    };

    Some(
        rest.split('/')
            .filter(|segment| !segment.is_empty())
            .map(percent_decode)
            .collect(),
    )
}

/// Minimal `decodeURIComponent`: `%XX` runs and `+` (a query value only, so the caller decides).
/// A malformed escape is kept verbatim rather than refused, exactly like the TS fallbacks.
#[must_use]
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && bytes[index + 1].is_ascii_hexdigit()
            && bytes[index + 2].is_ascii_hexdigit()
        {
            let hex = &raw[index + 1..index + 3];

            // A `%` that is not followed by two hex digits is emitted verbatim, which is what TS's
            // `catch { segment }` does for the whole segment.
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }

        out.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(out).unwrap_or_else(|_| raw.to_owned())
}

/// One route: its generated template plus the operations the contract declares for it.
///
/// `operations` is in the TS table's DECLARATION ORDER (`api.ts:3925-3974`), because that order is
/// what the 405 sentence prints (`Object.keys(table)`, `api.ts:4007`).
pub struct Route {
    /// The generated route id (`HARNESS_PATH_TEMPLATES`'s key).
    pub id: &'static str,
    /// The generated template, verbatim, including the `{root}` prefix.
    pub template: &'static str,
    /// `(METHOD, operation)` pairs, in declaration order. An empty list means the path exists in
    /// the template table but this build declares no verb for it, which still answers 405 with
    /// `Use GET for ...` (`api.ts:4007-4013`).
    pub operations: &'static [(&'static str, Operation)],
}

/// The 64 operations of `api.ts:3925-3974`, one variant each.
///
/// An enum rather than 64 function pointers so the table stays `static` (an `async fn`'s future
/// type cannot be named) while the call site stays one exhaustive `match` that cannot silently miss
/// an operation when a variant is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    // ---- host: root / health / config ---------------------------------------
    GetRoot,
    GetHealth,
    GetConfig,
    PatchConfig,
    // ---- host: harness definitions ------------------------------------------
    GetHarnesses,
    PostHarness,
    GetHarness,
    PatchHarness,
    DeleteHarness,
    GetHarnessInstructions,
    GetHarnessInstructionText,
    // ---- host: global agents -------------------------------------------------
    GetAgents,
    GetAgent,
    PatchAgent,
    DeleteAgent,
    // ---- host: tool catalogue ------------------------------------------------
    GetTools,
    GetTool,
    PostToolExecution,
    // ---- host: model discovery -----------------------------------------------
    PostModels,
    // ---- sessions -------------------------------------------------------------
    GetSessions,
    PostSession,
    GetSession,
    PatchSession,
    DeleteSession,
    GetSessionDefinition,
    GetSessionUsage,
    GetSessionCommands,
    GetSessionQueue,
    PostSessionTask,
    PostSessionReset,
    PostSessionCancel,
    // ---- actions --------------------------------------------------------------
    GetSessionActions,
    PostActionAnswer,
    // ---- session agents -------------------------------------------------------
    GetSessionAgents,
    PostSessionAgent,
    GetSessionAgent,
    PatchSessionAgent,
    DeleteSessionAgent,
    GetSessionAgentInstruction,
    PatchSessionAgentInstruction,
    // ---- session tools --------------------------------------------------------
    GetSessionTools,
    PostSessionTool,
    GetSessionTool,
    PatchSessionTool,
    DeleteSessionTool,
    GetSessionToolSource,
    PostSessionToolExecution,
    // ---- threads --------------------------------------------------------------
    GetSessionThreads,
    PostSessionThread,
    GetSessionThread,
    DeleteSessionThread,
    PostThreadCancel,
    // ---- transcripts ----------------------------------------------------------
    GetTranscriptMetadata,
    PutTranscript,
    GetTranscriptMessages,
    GetTranscriptDocument,
    // ---- turns ----------------------------------------------------------------
    GetSessionTurns,
    GetSessionTurn,
    GetSessionTurnDisplay,
    GetSessionTurnEvents,
    GetSessionTurnStep,
    GetSessionTurnBlock,
    GetSessionTurnAttachment,
    // ---- stream ---------------------------------------------------------------
    StreamRoute,
}

/// `(METHOD, operation)` in one place per route, so a template and its verbs cannot drift apart
/// (which is what `api.ts:613-615` protects against).
const fn operations(
    items: &'static [(&'static str, Operation)],
) -> &'static [(&'static str, Operation)] {
    items
}

/// The route table: all 45 generated templates and all 64 declared operations.
///
/// ORDER IS LOAD-BEARING: `matchRoute` (`api.ts:657-687`) returns the FIRST pattern whose segment
/// count and literal segments fit, in template-table order, so an ambiguous pair resolves the way
/// the generated table lists it. The table below is in that exact order.
pub static ROUTES: [Route; 45] = [
    Route {
        id: "root",
        template: contract::HARNESS_API_ROOT,
        operations: operations(&[("GET", Operation::GetRoot)]),
    },
    Route {
        id: "health",
        template: "/v1/harness/health",
        operations: operations(&[("GET", Operation::GetHealth)]),
    },
    Route {
        id: "config",
        template: "/v1/harness/config",
        operations: operations(&[
            ("GET", Operation::GetConfig),
            ("PATCH", Operation::PatchConfig),
        ]),
    },
    Route {
        id: "harnesses",
        template: "/v1/harness/harnesses",
        operations: operations(&[
            ("GET", Operation::GetHarnesses),
            ("POST", Operation::PostHarness),
        ]),
    },
    Route {
        id: "harnessesByHarnessName",
        template: "/v1/harness/harnesses/{harness_name}",
        operations: operations(&[
            ("GET", Operation::GetHarness),
            ("PATCH", Operation::PatchHarness),
            ("DELETE", Operation::DeleteHarness),
        ]),
    },
    Route {
        id: "harnessesByHarnessNameInstructions",
        template: "/v1/harness/harnesses/{harness_name}/instructions",
        operations: operations(&[("GET", Operation::GetHarnessInstructions)]),
    },
    Route {
        id: "harnessesByHarnessNameInstructionsByAgentId",
        template: "/v1/harness/harnesses/{harness_name}/instructions/{agent_id}",
        operations: operations(&[("GET", Operation::GetHarnessInstructionText)]),
    },
    Route {
        id: "agents",
        template: "/v1/harness/agents",
        operations: operations(&[("GET", Operation::GetAgents)]),
    },
    Route {
        id: "agentsByAgentId",
        template: "/v1/harness/agents/{agent_id}",
        operations: operations(&[
            ("GET", Operation::GetAgent),
            ("PATCH", Operation::PatchAgent),
            ("DELETE", Operation::DeleteAgent),
        ]),
    },
    Route {
        id: "tools",
        template: "/v1/harness/tools",
        operations: operations(&[("GET", Operation::GetTools)]),
    },
    Route {
        id: "toolsByToolId",
        template: "/v1/harness/tools/{tool_id}",
        operations: operations(&[("GET", Operation::GetTool)]),
    },
    Route {
        id: "toolsByToolIdExecutions",
        template: "/v1/harness/tools/{tool_id}/executions",
        operations: operations(&[("POST", Operation::PostToolExecution)]),
    },
    Route {
        id: "models",
        template: "/v1/harness/models",
        operations: operations(&[("POST", Operation::PostModels)]),
    },
    Route {
        id: "sessions",
        template: "/v1/harness/sessions",
        operations: operations(&[
            ("GET", Operation::GetSessions),
            ("POST", Operation::PostSession),
        ]),
    },
    Route {
        id: "sessionsBySessionId",
        template: "/v1/harness/sessions/{session_id}",
        operations: operations(&[
            ("GET", Operation::GetSession),
            ("PATCH", Operation::PatchSession),
            ("DELETE", Operation::DeleteSession),
        ]),
    },
    Route {
        id: "sessionsBySessionIdDefinition",
        template: "/v1/harness/sessions/{session_id}/definition",
        operations: operations(&[("GET", Operation::GetSessionDefinition)]),
    },
    Route {
        id: "sessionsBySessionIdUsage",
        template: "/v1/harness/sessions/{session_id}/usage",
        operations: operations(&[("GET", Operation::GetSessionUsage)]),
    },
    Route {
        id: "sessionsBySessionIdCommands",
        template: "/v1/harness/sessions/{session_id}/commands",
        operations: operations(&[("GET", Operation::GetSessionCommands)]),
    },
    Route {
        id: "sessionsBySessionIdQueue",
        template: "/v1/harness/sessions/{session_id}/queue",
        operations: operations(&[("GET", Operation::GetSessionQueue)]),
    },
    Route {
        id: "sessionsBySessionIdTasks",
        template: "/v1/harness/sessions/{session_id}/tasks",
        operations: operations(&[("POST", Operation::PostSessionTask)]),
    },
    Route {
        id: "sessionsBySessionIdReset",
        template: "/v1/harness/sessions/{session_id}/reset",
        operations: operations(&[("POST", Operation::PostSessionReset)]),
    },
    Route {
        id: "sessionsBySessionIdCancel",
        template: "/v1/harness/sessions/{session_id}/cancel",
        operations: operations(&[("POST", Operation::PostSessionCancel)]),
    },
    Route {
        id: "sessionsBySessionIdActions",
        template: "/v1/harness/sessions/{session_id}/actions",
        operations: operations(&[("GET", Operation::GetSessionActions)]),
    },
    Route {
        id: "sessionsBySessionIdActionsByRequestIdAnswer",
        template: "/v1/harness/sessions/{session_id}/actions/{request_id}/answer",
        operations: operations(&[("POST", Operation::PostActionAnswer)]),
    },
    Route {
        id: "sessionsBySessionIdAgents",
        template: "/v1/harness/sessions/{session_id}/agents",
        operations: operations(&[
            ("GET", Operation::GetSessionAgents),
            ("POST", Operation::PostSessionAgent),
        ]),
    },
    Route {
        id: "sessionsBySessionIdAgentsByAgentId",
        template: "/v1/harness/sessions/{session_id}/agents/{agent_id}",
        operations: operations(&[
            ("GET", Operation::GetSessionAgent),
            ("PATCH", Operation::PatchSessionAgent),
            ("DELETE", Operation::DeleteSessionAgent),
        ]),
    },
    Route {
        id: "sessionsBySessionIdAgentsByAgentIdInstruction",
        template: "/v1/harness/sessions/{session_id}/agents/{agent_id}/instruction",
        operations: operations(&[
            ("GET", Operation::GetSessionAgentInstruction),
            ("PATCH", Operation::PatchSessionAgentInstruction),
        ]),
    },
    Route {
        id: "sessionsBySessionIdTools",
        template: "/v1/harness/sessions/{session_id}/tools",
        operations: operations(&[
            ("GET", Operation::GetSessionTools),
            ("POST", Operation::PostSessionTool),
        ]),
    },
    Route {
        id: "sessionsBySessionIdToolsByToolId",
        template: "/v1/harness/sessions/{session_id}/tools/{tool_id}",
        operations: operations(&[
            ("GET", Operation::GetSessionTool),
            ("PATCH", Operation::PatchSessionTool),
            ("DELETE", Operation::DeleteSessionTool),
        ]),
    },
    Route {
        id: "sessionsBySessionIdToolsByToolIdSource",
        template: "/v1/harness/sessions/{session_id}/tools/{tool_id}/source",
        operations: operations(&[("GET", Operation::GetSessionToolSource)]),
    },
    Route {
        id: "sessionsBySessionIdToolsByToolIdExecutions",
        template: "/v1/harness/sessions/{session_id}/tools/{tool_id}/executions",
        operations: operations(&[("POST", Operation::PostSessionToolExecution)]),
    },
    Route {
        id: "sessionsBySessionIdThreads",
        template: "/v1/harness/sessions/{session_id}/threads",
        operations: operations(&[
            ("GET", Operation::GetSessionThreads),
            ("POST", Operation::PostSessionThread),
        ]),
    },
    Route {
        id: "sessionsBySessionIdThreadsByThreadId",
        template: "/v1/harness/sessions/{session_id}/threads/{thread_id}",
        operations: operations(&[
            ("GET", Operation::GetSessionThread),
            ("DELETE", Operation::DeleteSessionThread),
        ]),
    },
    Route {
        id: "sessionsBySessionIdThreadsByThreadIdCancel",
        template: "/v1/harness/sessions/{session_id}/threads/{thread_id}/cancel",
        operations: operations(&[("POST", Operation::PostThreadCancel)]),
    },
    Route {
        id: "sessionsBySessionIdThreadsByThreadIdTranscript",
        template: "/v1/harness/sessions/{session_id}/threads/{thread_id}/transcript",
        operations: operations(&[
            ("GET", Operation::GetTranscriptMetadata),
            ("PUT", Operation::PutTranscript),
        ]),
    },
    Route {
        id: "sessionsBySessionIdThreadsByThreadIdTranscriptMessages",
        template: "/v1/harness/sessions/{session_id}/threads/{thread_id}/transcript/messages",
        operations: operations(&[("GET", Operation::GetTranscriptMessages)]),
    },
    Route {
        id: "sessionsBySessionIdThreadsByThreadIdTranscriptDocument",
        template: "/v1/harness/sessions/{session_id}/threads/{thread_id}/transcript/document",
        operations: operations(&[("GET", Operation::GetTranscriptDocument)]),
    },
    Route {
        id: "sessionsBySessionIdTurns",
        template: "/v1/harness/sessions/{session_id}/turns",
        operations: operations(&[("GET", Operation::GetSessionTurns)]),
    },
    Route {
        id: "sessionsBySessionIdTurnsByTurnId",
        template: "/v1/harness/sessions/{session_id}/turns/{turn_id}",
        operations: operations(&[("GET", Operation::GetSessionTurn)]),
    },
    Route {
        id: "sessionsBySessionIdTurnsByTurnIdDisplay",
        template: "/v1/harness/sessions/{session_id}/turns/{turn_id}/display",
        operations: operations(&[("GET", Operation::GetSessionTurnDisplay)]),
    },
    Route {
        id: "sessionsBySessionIdTurnsByTurnIdEvents",
        template: "/v1/harness/sessions/{session_id}/turns/{turn_id}/events",
        operations: operations(&[("GET", Operation::GetSessionTurnEvents)]),
    },
    Route {
        id: "sessionsBySessionIdTurnsByTurnIdStepsByStepId",
        template: "/v1/harness/sessions/{session_id}/turns/{turn_id}/steps/{step_id}",
        operations: operations(&[("GET", Operation::GetSessionTurnStep)]),
    },
    Route {
        id: "sessionsBySessionIdTurnsByTurnIdBlocksByBlockId",
        template: "/v1/harness/sessions/{session_id}/turns/{turn_id}/blocks/{block_id}",
        operations: operations(&[("GET", Operation::GetSessionTurnBlock)]),
    },
    Route {
        id: "sessionsBySessionIdTurnsByTurnIdAttachmentsByAttachmentId",
        template: "/v1/harness/sessions/{session_id}/turns/{turn_id}/attachments/{attachment_id}",
        operations: operations(&[("GET", Operation::GetSessionTurnAttachment)]),
    },
    Route {
        id: "stream",
        template: "/v1/harness/stream",
        operations: operations(&[("GET", Operation::StreamRoute)]),
    },
];

/// `{param}` is the ONLY matching syntax (`api.ts:623-625`), so a literal segment is compared
/// exactly and a braced segment captures under its own name.
#[must_use]
fn segment_matches(expected: &str, actual: &str) -> Option<Option<String>> {
    if let Some(name) = expected
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    {
        return Some(Some(name.to_owned()));
    }

    if expected == actual {
        return Some(None);
    }

    None
}

/// A matched route: the entry plus the parameters its template captured (`matchRoute`).
pub struct Matched<'a> {
    pub route: &'a Route,
    pub params: BTreeMap<String, String>,
}

/// Matches mount-relative segments against the table, first match wins.
#[must_use]
pub fn match_route(segments: &[String]) -> Option<Matched<'_>> {
    for route in ROUTES.iter() {
        let template = &route.template[contract::HARNESS_API_ROOT.len()..];
        let expected: Vec<&str> = template
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();

        if expected.len() != segments.len() {
            continue;
        }

        let mut params = BTreeMap::new();
        let mut matched = true;

        for (index, pattern) in expected.iter().enumerate() {
            match segment_matches(pattern, &segments[index]) {
                // A literal segment that agrees captures nothing.
                Some(None) => {}
                Some(Some(name)) => {
                    params.insert(name, segments[index].clone());
                }
                None => {
                    matched = false;
                    break;
                }
            }
        }

        if matched {
            return Some(Matched { route, params });
        }
    }

    None
}

/// The operations one route declares, in table order - the list the 405 sentence prints.
#[must_use]
fn allowed_methods(route: &Route) -> Vec<&'static str> {
    route.operations.iter().map(|(method, _)| *method).collect()
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// A JSON success with no revision header (`json(status, body)`, `api.ts:1232-1233`).
#[must_use]
pub fn json(status: u16, body: Value) -> ApiResponse {
    ApiResponse {
        status,
        body,
        revision: None,
    }
}

/// A JSON success that also stamps `X-Harness-Revision` (`json(status, body, revision)`, which
/// every identified read and every page uses).
#[must_use]
pub fn with_revision(status: u16, body: Value, revision: u64) -> ApiResponse {
    ApiResponse {
        status,
        body,
        revision: Some(revision),
    }
}

/// The 404 for one missing subresource (`missing`, `api.ts:605-607`), worded so a client can tell
/// WHICH id it got wrong.
#[must_use]
pub fn missing(what: &str, id: &str) -> ApiError {
    ApiError::not_found(format!("Unknown {what} \"{id}\"."))
}

/// The same 404 when the id lives inside a session (`api.ts:607`'s third arm).
#[must_use]
pub fn missing_in(what: &str, id: &str, session_id: &str) -> ApiError {
    ApiError::not_found(format!(
        "Unknown {what} \"{id}\" in session \"{session_id}\"."
    ))
}

/// A trimmed query value, absent when blank (`paramOf`, `api.ts:481-488`). The empty-is-absent
/// rule is load-bearing: `?q=` must not filter on an empty substring.
#[must_use]
pub fn param_of<'q>(query: &'q Query, key: &str) -> Option<&'q str> {
    query
        .get(key)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// An enum-valued filter (`enumParamOf`, `api.ts:489-501`): an unrecognized value is a 400 naming
/// the parameter and every accepted spelling.
///
/// # Errors
///
/// [`ApiError::bad_request`] `Query parameter "<key>" must be one of: <list>.`
pub fn enum_param<'q>(
    query: &'q Query,
    key: &str,
    allowed: &[&'q str],
) -> Result<Option<&'q str>, ApiError> {
    let Some(value) = param_of(query, key) else {
        return Ok(None);
    };

    if allowed.contains(&value) {
        return Ok(Some(value));
    }

    Err(ApiError::bad_request(format!(
        "Query parameter \"{key}\" must be one of: {}.",
        allowed.join(", ")
    )))
}

/// The current counter of one resource, for a handler that stamps its own header
/// (`events.getRevision(resource, id)`).
#[must_use]
pub fn revision_of(ctx: &Ctx, resource: &str, id: Option<&str>) -> u64 {
    ctx.runtime.hub.revision(resource, id)
}

/// 403 that states the capability that is off, so a refusal answers WHY
/// (`capabilityDenied`, `api-errors.ts:21-23`):
/// `<operation> is not available: this deployment does not support <capability>.`
#[must_use]
pub fn capability_denied(operation: &str, capability: &str) -> ApiError {
    ApiError::forbidden(format!(
        "{operation} is not available: this deployment does not support {capability}."
    ))
}

/// `requireKnownFields` over an already-parsed object map (`api.ts:504-510`), so a handler that
/// pulled the object out of the body with [`Ctx::body_object`] can still name its own allowed set
/// and print the contract's sentence.
///
/// This is the map-shaped twin of [`contract::require_known_fields`], which takes a `Value`; the
/// two produce the same message because both report the FIRST unknown key in document order
/// (`serde_json`'s `preserve_order` is on, so that is the client's own key order - see
/// `knowledge.md` "T2").
pub fn require_body_fields(fields: &Map<String, Value>, allowed: &[&str]) -> Result<(), ApiError> {
    for key in fields.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ApiError::bad_request(format!(
                "Unknown field \"{key}\". Allowed fields: {}.",
                allowed.join(", ")
            )));
        }
    }

    Ok(())
}

/// The same check for a nested block that must itself be an object; `where` is the path the
/// contract names in its refusal (`api.ts:534-542`, ``"<where>.settings" must be an object.``).
pub fn require_block<'v>(value: &'v Value, path: &str) -> Result<&'v Map<String, Value>, ApiError> {
    match value {
        Value::Object(fields) => Ok(fields),
        _ => Err(ApiError::bad_request(format!(
            "\"{path}\" must be an object."
        ))),
    }
}

/// A 200 page body with the collection revision on the header (`pageMeta` + `json(200, result,
/// result.revision)`, `api.ts:1488-1510`, `:1696-1704`).
///
/// `limit` echoes the EFFECTIVE page size and `next_cursor` is `null` at the end of the collection,
/// both of which `paging` already guarantees.
#[must_use]
pub fn page_response<T: Serialize>(
    ctx: &Ctx,
    revision: u64,
    slice: paging::PageSlice<T>,
) -> ApiResponse {
    let body = paging::page_slice_body(
        ctx.runtime.hub.epoch(),
        slice,
        revision,
        events::now_millis(),
    );

    with_revision(StatusCode::OK.as_u16(), body, revision)
}

/// Turns a response header map into the wire response: `content-type: application/json`,
/// `cache-control: no-store`, and the epoch/revision headers `withHeaders` stamps
/// (`api.ts:1224-1230`; `handler.ts:86` is where the content type is set).
#[must_use]
fn write_response(response: &ApiResponse, epoch: &str) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        )
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .header(contract::HARNESS_EPOCH_HEADER, epoch);

    if let Some(revision) = response.revision {
        builder = builder.header(contract::HARNESS_REVISION_HEADER, revision.to_string());
    }

    let body = serde_json::to_string(&response.body).unwrap_or_else(|_| "{}".to_string());

    builder
        .header(header::CONTENT_LENGTH, body.len().to_string())
        .body(Body::from(body))
        // The builder only fails on a header name/value that is not representable on the wire, and
        // every value here is generated from ASCII or a number - so this arm is unreachable in
        // practice. The honest answer when it does happen is a status-only 500, because the
        // generated body is the thing that failed.
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// The single error envelope, with the same headers as a success (`api.ts:4030-4064` +
/// `handler.ts:284-286`).
///
/// TS's adapter sends an error body WITHOUT the contract headers, because `withHeaders` is applied
/// by the dispatcher's `json()` and the catch arm returns a bare result. This port stamps the epoch
/// on every JSON response, error included (the stage contract): the value is the process epoch
/// either way, since `EventHub::new()` takes `events::process_epoch()` and two hubs in one process
/// therefore share it (`knowledge.md` "Event hub").
#[must_use]
fn write_error(error: &ApiError, epoch: &str) -> Response {
    let response = json(
        error.status,
        serde_json::json!({ "error": { "message": error.message } }),
    );

    write_response(&response, epoch)
}

// ---------------------------------------------------------------------------
// The dispatcher
// ---------------------------------------------------------------------------

/// Dispatches one REST request (`api` + `dispatch`, `api.ts:3980-4064`).
///
/// The order is TS's, and every step of it is a contract rule rather than an implementation detail:
///
/// 1. the ORIGIN gate, before any runtime work (`service.ts:529-539`);
/// 2. the per-root runtime, created on first use for this database root (`service.ts:515-528`);
/// 3. the mount guard, so a stale prefix names the canonical root (`api.ts:3987-3989`);
/// 4. the route match (`:3991-4001`), then the verb (`:4003-4014`);
/// 5. the handler, with the body resolved under the route's cap ONLY if the handler asks for it.
pub async fn handle_request(root: PathBuf, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let pathname = parts.uri.path().to_owned();
    let query = parse_query(parts.uri.query());
    let method = parts.method.as_str().to_ascii_uppercase();

    // 1. Access boundary, before anything else exists. A refusal here quotes the PROCESS epoch,
    // because no runtime was needed to answer it - and the epoch is the same value a runtime for
    // this root would quote, since every hub in the process takes `events::process_epoch()`.
    if let Err(error) = origin::check_rest_origin(
        header_text(&headers, "origin").as_deref(),
        header_text(&headers, "host").as_deref(),
    ) {
        return write_error(&error, events::process_epoch());
    }

    // 2. The runtime for this root.
    let runtime = match runtime::runtime_for(&root) {
        Ok(runtime) => runtime,
        Err(error) => return write_error(&error, events::process_epoch()),
    };

    let epoch = runtime.epoch();
    let mount = requested_mount(&pathname);

    // 3. T13 removed the second mount: an adapter addressed at anything else is a 404 that NAMES
    // the canonical root (`api.ts:3987-3989`).
    if mount != contract::HARNESS_API_ROOT {
        return write_error(
            &ApiError::not_found(format!(
                "This server serves {}; \"{mount}\" is not mounted.",
                contract::HARNESS_API_ROOT
            )),
            &epoch,
        );
    }

    let Some(segments) = mount_segments(&pathname) else {
        return write_error(
            &ApiError::not_found(format!("Unknown API route: {method} {pathname}")),
            &epoch,
        );
    };

    // 4. The path, then the verb.
    let Some(matched) = match_route(&segments) else {
        return write_error(
            &ApiError::not_found(format!("Unknown API route: {method} {pathname}")),
            &epoch,
        );
    };

    let operation = matched
        .route
        .operations
        .iter()
        .find(|(declared, _)| *declared == method)
        .map(|(_, operation)| *operation);

    let Some(operation) = operation else {
        let allowed = allowed_methods(matched.route);
        let verb = if allowed.is_empty() {
            "GET".to_owned()
        } else {
            allowed.join(", ")
        };

        // The path is re-joined from the REQUEST segments, not from the template, exactly as
        // `api.ts:4012` prints `mount` + the caller's own path.
        let joined = if segments.is_empty() {
            mount.clone()
        } else {
            format!("{mount}/{}", segments.join("/"))
        };

        // The contract's laziness rule says a wrong verb does not PARSE the body
        // (`api.ts:4009`), and that is what this arm honours: no cap check, no media-type check and no
        // JSON decode happens here. The stream is still drained, because a 405 written over a body that
        // is mid-flight makes hyper reset the socket and the client would read a connection error
        // instead of the contract's sentence - node's adapter gets that drain for free, this one takes
        // it deliberately, and in both cases the bytes are discarded rather than interpreted.
        drain(body, BODY_DRAIN_GRACE_BYTES).await;

        return write_error(
            &ApiError::method_not_allowed(format!("Use {verb} for {joined}.")),
            &epoch,
        );
    };

    // 5. The declared-length fast refusal (`service.ts:502-506`): a client that TELLS the truth about
    // an oversize body is refused before a single byte is read, under the ROUTE's cap. TypeScript runs
    // this in the gateway, before the dispatcher, which is why an unknown session id under a 20 MB
    // submit is a 413 rather than a 404 - the body was never the handler's to read. This adapter runs
    // it after the route and verb have resolved and immediately before the handler, because its 405
    // arm must not look at a body at all (`api.ts:4009`); the two orders differ only for a wrong verb
    // carrying an oversize claim, and there the port keeps its own documented laziness.
    let limit = body_limit_for(&method, &pathname);

    if let Some(declared) = declared_length(&headers)
        && declared > limit as u64
    {
        drain(body, BODY_DRAIN_GRACE_BYTES).await;

        return write_error(&too_large(limit), &epoch);
    }

    let reader = BodyReader::pending(body, limit, &headers);
    let mut ctx = Ctx {
        runtime,
        method,
        pathname,
        query,
        params: matched.params,
        headers,
        body: reader,
    };

    let outcome = dispatch_operation(operation, &mut ctx).await;

    // The drain runs BEFORE the answer is flushed, which is the whole point of it.
    drain_unread_body(&mut ctx.body).await;

    outcome
        .map(|response| write_response(&response, &ctx_epoch(&ctx)))
        .unwrap_or_else(|error| write_error(&error, &ctx_epoch(&ctx)))
}

/// The stream still sitting in a reader nobody consumed.
fn pending_stream(reader: &mut BodyReader) -> Body {
    match reader {
        BodyReader::Pending { body, .. } => body.take().unwrap_or_else(Body::empty),
        BodyReader::Ready(_) | BodyReader::Failed(_) => Body::empty(),
    }
}

/// Drains a body a handler refused WITHOUT parsing it.
///
/// The contract's laziness rule (`api.ts:4022-4024`, `:4009`) says a route that refuses before
/// looking at the body - an unknown id, a withheld capability - must not consume it, and this port
/// honours that literally: no cap check, no media-type check, no JSON decode. TypeScript never sees
/// the problem because its Node adapter hands the handler an ALREADY-parsed body and the gateway
/// cancels the reader otherwise (`service.ts:378-381`, "stop the transfer rather than drain a body
/// nobody will read"). Hyper has no such luxury: the answer is flushed the moment the handler
/// returns, and a client still writing its upload reads `ConnectionReset` instead of the envelope -
/// which is exactly what the 16 MiB submit boundary proof hit when a session-domain stub answered a
/// 2 MB body. The bytes are therefore read and thrown away, bounded by the same grace the 405 arm
/// uses, so a compliant client always receives its 400/404/405/415.
async fn drain_unread_body(reader: &mut BodyReader) {
    reader.drain_pending().await;
}

fn ctx_epoch(ctx: &Ctx) -> String {
    ctx.runtime.epoch()
}

/// The stream mount (`api.ts:3906-3912` + `handler.ts:245-250` + `stream/{transport,mount}.ts`).
///
/// T5c owns the whole upgrade now, so the body is one delegation: the gates, the subprotocol echo, the
/// `X-Harness-Epoch` header on the 101 and the frame conversation all live in [`crate::stream`]. The
/// honest non-upgrade answer is still TypeScript's own 400 sentence, which `crate::stream::upgrade`
/// prints for a GET that never sent the upgrade tokens - the one `queue.test.ts:1090` and
/// `view.test.ts:779` pin.
pub async fn handle_stream(root: PathBuf, request: Request) -> Response {
    stream::upgrade(root, request).await
}

/// Routes one matched operation to its handler. This is the only place a handler is named, so the
/// table above stays data.
async fn dispatch_operation(operation: Operation, ctx: &mut Ctx) -> ApiResult {
    match operation {
        Operation::GetRoot => self::root::get_root(ctx).await,
        Operation::GetHealth => self::root::get_health(ctx).await,
        Operation::GetConfig => self::config::get_config(ctx).await,
        Operation::PatchConfig => self::config::patch_config(ctx).await,

        Operation::GetHarnesses => self::harnesses::get_harnesses(ctx).await,
        Operation::PostHarness => self::harnesses::post_harness(ctx).await,
        Operation::GetHarness => self::harnesses::get_harness(ctx).await,
        Operation::PatchHarness => self::harnesses::patch_harness(ctx).await,
        Operation::DeleteHarness => self::harnesses::delete_harness(ctx).await,
        Operation::GetHarnessInstructions => self::harnesses::get_harness_instructions(ctx).await,
        Operation::GetHarnessInstructionText => {
            self::harnesses::get_harness_instruction_text(ctx).await
        }

        Operation::GetAgents => self::agents::get_agents(ctx).await,
        Operation::GetAgent => self::agents::get_agent(ctx).await,
        Operation::PatchAgent => self::agents::patch_agent(ctx).await,
        Operation::DeleteAgent => self::agents::delete_agent(ctx).await,

        Operation::GetTools => self::tools::get_tools(ctx).await,
        Operation::GetTool => self::tools::get_tool(ctx).await,
        Operation::PostToolExecution => self::tools::post_tool_execution(ctx).await,

        Operation::PostModels => self::models::post_models(ctx).await,

        Operation::GetSessions => self::sessions::get_sessions(ctx).await,
        Operation::PostSession => self::sessions::post_session(ctx).await,
        Operation::GetSession => self::sessions::get_session(ctx).await,
        Operation::PatchSession => self::sessions::patch_session(ctx).await,
        Operation::DeleteSession => self::sessions::delete_session(ctx).await,
        Operation::GetSessionDefinition => self::sessions::get_session_definition(ctx).await,
        Operation::GetSessionUsage => self::sessions::get_session_usage(ctx).await,
        Operation::GetSessionCommands => self::sessions::get_session_commands(ctx).await,
        Operation::GetSessionQueue => self::sessions::get_session_queue(ctx).await,
        Operation::PostSessionTask => self::sessions::post_session_task(ctx).await,
        Operation::PostSessionReset => self::sessions::post_session_reset(ctx).await,
        Operation::PostSessionCancel => self::sessions::post_session_cancel(ctx).await,

        Operation::GetSessionActions => self::actions::get_session_actions(ctx).await,
        Operation::PostActionAnswer => self::actions::post_action_answer(ctx).await,

        Operation::GetSessionAgents => self::session_agents::get_session_agents(ctx).await,
        Operation::PostSessionAgent => self::session_agents::post_session_agent(ctx).await,
        Operation::GetSessionAgent => self::session_agents::get_session_agent(ctx).await,
        Operation::PatchSessionAgent => self::session_agents::patch_session_agent(ctx).await,
        Operation::DeleteSessionAgent => self::session_agents::delete_session_agent(ctx).await,
        Operation::GetSessionAgentInstruction => {
            self::session_agents::get_session_agent_instruction(ctx).await
        }
        Operation::PatchSessionAgentInstruction => {
            self::session_agents::patch_session_agent_instruction(ctx).await
        }

        Operation::GetSessionTools => self::session_tools::get_session_tools(ctx).await,
        Operation::PostSessionTool => self::session_tools::post_session_tool(ctx).await,
        Operation::GetSessionTool => self::session_tools::get_session_tool(ctx).await,
        Operation::PatchSessionTool => self::session_tools::patch_session_tool(ctx).await,
        Operation::DeleteSessionTool => self::session_tools::delete_session_tool(ctx).await,
        Operation::GetSessionToolSource => self::session_tools::get_session_tool_source(ctx).await,
        Operation::PostSessionToolExecution => {
            self::session_tools::post_session_tool_execution(ctx).await
        }

        Operation::GetSessionThreads => self::threads::get_session_threads(ctx).await,
        Operation::PostSessionThread => self::threads::post_session_thread(ctx).await,
        Operation::GetSessionThread => self::threads::get_session_thread(ctx).await,
        Operation::DeleteSessionThread => self::threads::delete_session_thread(ctx).await,
        Operation::PostThreadCancel => self::threads::post_thread_cancel(ctx).await,

        Operation::GetTranscriptMetadata => self::transcript::get_transcript_metadata(ctx).await,
        Operation::PutTranscript => self::transcript::put_transcript(ctx).await,
        Operation::GetTranscriptMessages => self::transcript::get_transcript_messages(ctx).await,
        Operation::GetTranscriptDocument => self::transcript::get_transcript_document(ctx).await,

        Operation::GetSessionTurns => self::turns::get_session_turns(ctx).await,
        Operation::GetSessionTurn => self::turns::get_session_turn(ctx).await,
        Operation::GetSessionTurnDisplay => self::turns::get_session_turn_display(ctx).await,
        Operation::GetSessionTurnEvents => self::turns::get_session_turn_events(ctx).await,
        Operation::GetSessionTurnStep => self::turns::get_session_turn_step(ctx).await,
        Operation::GetSessionTurnBlock => self::turns::get_session_turn_block(ctx).await,
        Operation::GetSessionTurnAttachment => self::turns::get_session_turn_attachment(ctx).await,

        Operation::StreamRoute => self::root::stream_route(ctx).await,
    }
}

/// Compile-time proof that the dispatcher's future is `Send` - the bound axum's `Handler` impl
/// requires, and the reason no async helper may hold a `&Ctx` across an await: `Ctx` is `Send` but NOT
/// `Sync` (its body reader is a boxed `HttpBody`, which is `Send`-only), so `&Ctx` is not `Send`.
/// Every session-domain helper therefore either takes `&mut Ctx` or awaits nothing.
#[cfg(test)]
#[allow(dead_code)]
fn assert_dispatch_future_is_send(operation: Operation, ctx: &mut Ctx) {
    fn requires_send<F: Send>(_: &F) {}

    requires_send(&dispatch_operation(operation, ctx));
}

// ---------------------------------------------------------------------------
// Query parsing
// ---------------------------------------------------------------------------

/// `queryOf` (`api.ts:455-479`) for an axum request: the raw query string, split on `&`/`=` and
/// percent-decoded. An empty key or value survives as the empty string, which is what the
/// `param_of`/`resolve_limit` layers then treat as absent - the same shape `URLSearchParams` gives
/// `?limit=` and `?q=`.
#[must_use]
pub fn parse_query(raw: Option<&str>) -> Query {
    let mut query = Query::new();

    let Some(raw) = raw.filter(|value| !value.is_empty()) else {
        return query;
    };

    for pair in raw.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_component(key, true);

        if key.is_empty() {
            continue;
        }

        query.insert(key, decode_component(value, true));
    }

    query
}

/// Percent-decoding for one query component; `+` is a space in a query string only
/// (`URLSearchParams` semantics, which the contract's `?q=a+b` filter depends on).
fn decode_component(raw: &str, plus_as_space: bool) -> String {
    let replaced = if plus_as_space {
        raw.replace('+', " ")
    } else {
        raw.to_owned()
    };

    percent_decode(&replaced)
}

/// One header value as text, `None` when absent or not UTF-8.
#[must_use]
fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
