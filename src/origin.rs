//! The browser-origin policy of the HTTP boundary.
//!
//! TypeScript origins (one definition, several callers that must not drift):
//!   * `packages/underload/harness/src/shared/origin.ts` - the loopback set and the GitHub
//!     Pages host test;
//!   * `src/server/service.ts:165-180` (`isAllowedOrigin`) + `:456-464` - the inbound REST
//!     gate, which answers 403 `Cross-origin requests are not allowed.`;
//!   * `src/server/stream/mount.ts:46-81` (`isStreamOriginAllowed` / `policyFor`) - the same
//!     rule at the upgrade, answering 403 `Cross-origin stream upgrades are not allowed.`
//!
//! ORIGIN POLICY IS NOT AUTHENTICATION (`stream/mount.ts:12-16`): the handshake has no
//! credential, so this is a policy refusal, not a challenge. A client with NO `Origin` header
//! (curl, a native app, server-to-server) is not a cross-origin page and passes; an
//! unparseable `Origin: null` (a sandboxed iframe or a `file://` page) proves nothing and
//! fails.
//!
//! Loopback is matched by HOSTNAME, never by string prefix (`origin.ts:15-19`): values such as
//! `http://127.0.0.1.evil.test` or `http://localhost.attacker.test` must not pass. The `github.io`
//! zone is operated by GitHub, so a user cannot register `github.io.attacker.test` the way they
//! can register `localhost.attacker.test` (`:73-75`), and the apex `github.io` is deliberately
//! excluded because it is GitHub's own host, not a Pages site.
//!
//! NO `url` CRATE: this crate's dependency set is fixed, so [`parse_url`] is a small parser
//! for exactly the forms the policy needs - `scheme://host[:port]/path?query#fragment` -
//! including the WHATWG behaviours the TS comparisons depend on: a lowercased host, a default
//! port that is dropped from `host`, and IPv6 brackets that are KEPT in the hostname.

use crate::error::ApiError;

/// Hostnames that denote this machine (`origin.ts:27-32`), EXACTLY the TS set:
/// `{localhost, 127.0.0.1, [::1], ::1}`.
///
/// TS asserts `isLoopbackHostname('0.0.0.0') === false` (`origin.test.ts:38`), i.e. the bind
/// address is NOT treated as loopback, and `host.docker.internal` is not in the set either. A
/// local deployment that dials those hosts reaches the gate through the SAME-HOST rule (the
/// `Origin` and the `Host` agree) rather than through the loopback list, so widening the list
/// here would only loosen the policy.
pub const LOOPBACK_HOSTNAMES: [&str; 4] = ["localhost", "127.0.0.1", "[::1]", "::1"];

/// The GitHub Pages zone (`origin.ts:76`).
pub const PAGES_HOST_SUFFIX: &str = "github.io";

/// The refusal the REST mount prints (`service.ts:461`).
pub const REST_ORIGIN_MESSAGE: &str = "Cross-origin requests are not allowed.";

/// The refusal the stream upgrade prints (`stream/mount.ts:80`).
pub const STREAM_ORIGIN_MESSAGE: &str = "Cross-origin stream upgrades are not allowed.";

/// True when `host` denotes this machine. The caller has already normalized the hostname
/// (`origin.ts:36-37`); the comparison is case-insensitive.
#[must_use]
pub fn is_loopback_hostname(host: &str) -> bool {
    let normalized = host.trim().to_lowercase();

    LOOPBACK_HOSTNAMES.contains(&normalized.as_str())
}

/// Hostname test for a Pages host, ANCHORED ON A LABEL BOUNDARY (`origin.ts:81-82`): every
/// `<user>.github.io` subdomain passes, while `github.io.attacker.test` does not.
#[must_use]
pub fn is_pages_hostname(host: &str) -> bool {
    host.trim()
        .to_lowercase()
        .ends_with(&format!(".{PAGES_HOST_SUFFIX}"))
}

/// True for an HTTPS GitHub Pages ORIGIN (`origin.ts:90-102`). HTTPS is required: Pages serves
/// the deployed UI over https only, so an `http://<user>.github.io` origin is a redirect
/// artifact or a spoofed header, and the gate must not widen for it.
#[must_use]
pub fn is_pages_origin(origin: &str) -> bool {
    parse_url(origin).is_some_and(|url| url.is_https() && is_pages_hostname(&url.hostname))
}

/// The REST gate's rule (`service.ts:165-180`): same host (including port, case-insensitive),
/// OR a loopback hostname, OR this package's deployed GitHub Pages UI.
#[must_use]
pub fn is_trusted_runtime_origin(origin: &str, request_host: Option<&str>) -> bool {
    matches_origin(origin, request_host, true)
}

/// The stream gate's rule (`stream/mount.ts:46-60`): same host, OR a loopback hostname, OR
/// this package's deployed GitHub Pages UI - deliberately IDENTICAL to the REST rule.
///
/// `mount.ts:41-44` says it outright: "Deliberately identical to `src/server/service.ts`'s HTTP
/// rule; it lives here (rather than being imported from there) because service.ts imports THIS
/// module for the upgrade, and a back-import would make that a cycle." `:59` ends with
/// `return isPagesOrigin(origin)`, so a Pages page may upgrade exactly as it may call REST.
#[must_use]
pub fn is_stream_origin_allowed(origin: &str, request_host: Option<&str>) -> bool {
    matches_origin(origin, request_host, true)
}

/// The one host comparison both gates share, so "which origins count as local" cannot drift
/// between the REST mount and the upgrade. `allow_pages` is the ONLY degree of freedom and both
/// callers pass `true`, which is the TS posture (`service.ts:176-179` and `mount.ts:56-59` run
/// the same three tests); the flag is kept so a host that narrows one gate does it here and not
/// by duplicating the rule.
fn matches_origin(origin: &str, request_host: Option<&str>, allow_pages: bool) -> bool {
    let Some(url) = parse_url(origin) else {
        // `Origin: null`, a scheme with no authority, and garbage all prove nothing.
        return false;
    };

    // Only web origins can host a harness API root: `data:` / `file:` have no meaningful origin
    // and must never be trusted (`origin.ts:56-58`).
    if !(url.is_http() || url.is_https()) {
        return false;
    }

    if let Some(host) = request_host.filter(|value| !value.trim().is_empty())
        && url.host.eq_ignore_ascii_case(host.trim())
    {
        return true;
    }

    if is_loopback_hostname(&url.hostname) {
        return true;
    }

    allow_pages && url.is_https() && is_pages_hostname(&url.hostname)
}

/// The REST access boundary, applied before any handler work (`service.ts:454-464`).
///
/// # Errors
///
/// [`ApiError::forbidden`] with [`REST_ORIGIN_MESSAGE`] when an `Origin` is present and not
/// trusted.
pub fn check_rest_origin(origin: Option<&str>, request_host: Option<&str>) -> Result<(), ApiError> {
    check_origin(origin, request_host, REST_ORIGIN_MESSAGE)
}

/// The stream access boundary, applied before the upgrade (`stream/mount.ts:68-81`).
///
/// # Errors
///
/// [`ApiError::forbidden`] with [`STREAM_ORIGIN_MESSAGE`] when an `Origin` is present and not
/// trusted.
pub fn check_stream_origin(
    origin: Option<&str>,
    request_host: Option<&str>,
) -> Result<(), ApiError> {
    check_origin(origin, request_host, STREAM_ORIGIN_MESSAGE)
}

/// The shared "absent or empty means not a browser, pass" rule (`service.ts:456`,
/// `mount.ts:73`).
fn check_origin(
    origin: Option<&str>,
    request_host: Option<&str>,
    message: &str,
) -> Result<(), ApiError> {
    let Some(value) = origin.filter(|value| !value.is_empty()) else {
        return Ok(());
    };

    // The two gates now decide identically (`mount.ts:41-44`); the only thing that differs is
    // the refusal sentence each one prints, so the dispatch below is about the message.
    let trusted = if message == STREAM_ORIGIN_MESSAGE {
        is_stream_origin_allowed(value, request_host)
    } else {
        is_trusted_runtime_origin(value, request_host)
    };

    if trusted {
        return Ok(());
    }

    Err(ApiError::forbidden(message))
}

// ---------------------------------------------------------------------------
// The minimal URL reader the policy depends on
// ---------------------------------------------------------------------------

/// A parsed `scheme://host/path?query#fragment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedUrl {
    /// Lowercased scheme WITHOUT the colon, e.g. `https`.
    pub(crate) scheme: String,
    /// Lowercased authority WITH the port, minus a default port - the WHATWG `host` getter,
    /// which is what `service.ts:175` compares against the request `Host` header.
    pub(crate) host: String,
    /// Authority minus the port. IPv6 brackets are KEPT, which is why `LOOPBACK_HOSTNAMES`
    /// spells `[::1]` (`origin.ts:18-19`).
    pub(crate) hostname: String,
    /// Path beginning at `/` (empty when the URL has no path).
    pub(crate) pathname: String,
    /// `?...` INCLUDING the leading `?`, or empty.
    pub(crate) search: String,
    /// `#...` INCLUDING the leading `#`, or empty.
    pub(crate) hash: String,
}

impl ParsedUrl {
    #[must_use]
    pub(crate) fn is_http(&self) -> bool {
        self.scheme == "http"
    }

    #[must_use]
    pub(crate) fn is_https(&self) -> bool {
        self.scheme == "https"
    }
}

/// Parses the forms the origin policy and the provider-URL comparison need.
///
/// `None` means "not an absolute URL with an authority", which every caller treats as
/// "untrusted" / "fall back to text" - the same posture `origin.ts:46` documents
/// ("Throwing on a malformed candidate is deliberately avoided").
pub(crate) fn parse_url(value: &str) -> Option<ParsedUrl> {
    let trimmed = value.trim();

    // A scheme is `[A-Za-z][A-Za-z0-9+.-]*:`; anything else (`//host`, `evil.example`,
    // `javascript:alert(1)`) has no authority to compare.
    let (scheme, rest) = trimmed.split_once(':')?;
    if scheme.is_empty()
        || !scheme
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic())
        || !scheme
            .chars()
            .all(|glyph| glyph.is_ascii_alphanumeric() || matches!(glyph, '+' | '-' | '.'))
    {
        return None;
    }

    let remainder = rest.strip_prefix("//")?;

    // The authority ends at the first path/query/fragment delimiter.
    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let (authority, tail) = remainder.split_at(authority_end);
    if authority.is_empty() {
        return None;
    }

    let (raw_hostname, port) = split_authority(authority);
    // The host is lowercased ONCE here, so every comparison downstream (`service.ts:175`'s
    // case-insensitive host match included) sees the same normalized form `URL` produces.
    let hostname = raw_hostname.to_lowercase();
    let scheme = scheme.to_lowercase();

    // WHATWG drops a default port from `host`, which is why `http://Provider.test:80/v1/` and
    // `http://provider.test/v1` compare equal (see the comment above `normalizeProviderURL` in
    // `config.ts:167-175`).
    let is_default_port = match (scheme.as_str(), port) {
        ("http", Some(p)) => p == "80",
        ("https", Some(p)) => p == "443",
        _ => false,
    };
    let host = if hostname.is_empty() {
        return None;
    } else if is_default_port {
        hostname.clone()
    } else {
        match port {
            Some(p) => format!("{hostname}:{p}"),
            None => hostname.clone(),
        }
    };

    let (path_and_query, hash) = split_once_or_empty(tail, '#');
    let (pathname, search) = split_once_or_empty(path_and_query, '?');

    Some(ParsedUrl {
        scheme,
        host,
        hostname,
        pathname: pathname.to_owned(),
        search: search.to_owned(),
        hash: hash.to_owned(),
    })
}

/// Splits `host:port` from an authority, keeping IPv6 brackets in the hostname.
fn split_authority(authority: &str) -> (&str, Option<&str>) {
    // Strip any userinfo: credentials never participate in an origin comparison (and the
    // transport refuses them outright, see `types/index.ts:1672`).
    let host_and_port = match authority.rsplit_once('@') {
        Some((_, rest)) => rest,
        None => authority,
    };

    if host_and_port.starts_with('[') {
        let Some(end) = host_and_port.find(']') else {
            return (host_and_port, None);
        };
        let hostname = &host_and_port[..=end];
        let remainder = &host_and_port[end + 1..];

        return match remainder.strip_prefix(':') {
            Some(port) if !port.is_empty() => (hostname, Some(port)),
            _ => (hostname, None),
        };
    }

    match host_and_port.rsplit_once(':') {
        // A colon that is not followed by digits is not a port (a bare IPv6 literal without
        // brackets, for instance); the whole value stays the hostname then.
        Some((head, port))
            if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            (head, Some(port))
        }
        _ => (host_and_port, None),
    }
}

/// Splits on `separator` and returns `(before, INCLUDING-separator-after)`, so `search` and
/// `hash` keep their leading marker exactly as `URL.search` / `URL.hash` do.
fn split_once_or_empty(value: &str, separator: char) -> (&str, &str) {
    match value.find(separator) {
        Some(index) => (&value[..index], &value[index..]),
        None => (value, ""),
    }
}
