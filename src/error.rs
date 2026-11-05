//! The single HTTP-shaped error every layer raises.
//!
//! TypeScript origin: `src/server/api-errors.ts` (`HttpError`) plus the three
//! sibling error classes that all carry an HTTP status (`config.ts` `SettingsError`
//! = 400, `sessions.ts` `SessionError`, `harnesses/registry.ts`
//! `HarnessRegistryError`). In TypeScript those four types exist only so each layer
//! can name its own failure; the dispatcher maps ALL of them to one envelope, so the
//! Rust port collapses them into one value type:
//!
//! ```text
//! { "error": { "message": "<message>" } }
//! ```
//!
//! That envelope is `contract::ErrorBody` and is the ONLY error shape any route
//! returns (see `src/shared/harness-api.ts` `ErrorBody`), so the status + message
//! pair in here is everything a later stage needs.
//!
//! Design notes for the next agent:
//!   * `Clone + PartialEq` are deliberate: handlers build these in helpers that are
//!     reused, and the tests compare an error against the exact expected value rather
//!     than string-matching a rendered message.
//!   * `status` is a bare `u16` (not an enum) because the TS surface uses arbitrary
//!     statuses (`502`, `413`, `426`) and axum needs the raw number anyway.
//!   * There is no `From<serde_json::Error>`: a body that fails to parse is answered
//!     with a route-specific message, and silently mapping the parser's text into the
//!     envelope would put axum's wording on the wire where the contract pins its own.

use std::fmt;

/// A refusal: the HTTP status plus the message the envelope carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// HTTP status code (400/403/404/405/409/413/415/429/415/500/502...).
    pub status: u16,
    /// Human-readable reason, sent verbatim as `error.message`.
    pub message: String,
}

impl ApiError {
    /// The general form: any status with any message.
    #[must_use]
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    /// 400 - a malformed body, query, or settings value (`HttpError`/`SettingsError`).
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, message)
    }

    /// 403 - a policy refusal, used by the origin gate (`service.ts:461`).
    #[must_use]
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(403, message)
    }

    /// 404 - an unknown route or an unknown instance id.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, message)
    }

    /// 405 - a method that is not mounted on an existing path.
    #[must_use]
    pub fn method_not_allowed(message: impl Into<String>) -> Self {
        Self::new(405, message)
    }

    /// 409 - a state conflict (a revision mismatch, an existing name, a busy thread).
    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(409, message)
    }

    /// 413 - an oversized body or attachment (`MAX_BODY_BYTES`, `MAX_SUBMIT_BODY_BYTES`).
    #[must_use]
    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(413, message)
    }

    /// 415 - an unsupported media type.
    #[must_use]
    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self::new(415, message)
    }

    /// 429 - a rate or queue budget refusal (`MAX_PENDING_TASKS`).
    #[must_use]
    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self::new(429, message)
    }

    /// 502 - the provider upstream failed (`transport/models.ts` maps provider faults here).
    #[must_use]
    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(502, message)
    }

    /// 500 - an unexpected internal failure.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(500, message)
    }
}

/// The message text is what the envelope prints, so `Display` == `error.message`
/// (no prefix): a later stage can `format!("{error}")` into a log line without
/// inventing a second spelling of the refusal.
impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ApiError {}

/// The name the session-domain modules use for the crate's one error type. `ApiError` IS the harness
/// error (T2): the transport maps its `status` onto the wire, so a store refusal states its status here
/// and no second error type exists to convert between.
pub type HarnessError = ApiError;

/// The store's result shape under the name the session-domain modules use (`StoreResult` in
/// `store/mod.rs`), kept as an alias so a route can name either.
pub type HarnessResult<T> = std::result::Result<T, ApiError>;
