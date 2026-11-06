//! The one paging helper every paginated canonical route shares.
//!
//! TypeScript origin: `packages/underload/harness/src/server/api-paging.ts` (T13), a direct
//! port of `components.parameters.Limit` / `.Cursor` in `harness.swagger.yaml:2887`.
//!
//! WHY one clamp (`api-paging.ts:3-11`): the SAME default (25) and maximum (100) apply to
//! every paginated route, so one helper is the contract rather than a per-route invention;
//! a page that returns exactly `limit` items ALWAYS carries `next_cursor`, "possibly to an
//! empty page", which is how the end of a collection is detected WITHOUT a total - and when the
//! total IS known (`api-paging.ts:130-136`) an exactly-full page that has REACHED that total
//! ends the collection instead, which is what every real route does; and a cursor is OPAQUE,
//! issued by the route that accepts it, so an unrecognised or stale one answers 400 NAMING the
//! parameter and the client restarts from the first page.
//!
//! CURSOR SHAPE (`api-paging.ts:13-21`, server-private and never a published format):
//! `base64url("<epoch>.<ordinal>")`. The epoch half makes staleness DECIDABLE for free: a
//! cursor carried across a restart is refused with the required 400 instead of silently
//! re-reading page one, which is what the contract's "a revision/cursor is only comparable
//! inside one epoch" rule requires. The ordinal half is the number of entries already
//! served, which every collection here can supply without a total (turn index, roster
//! sequence, message position, event index, catalogue insertion order).

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;

use crate::contract::{PAGE_LIMIT_DEFAULT, PAGE_LIMIT_MAX, PAGE_LIMIT_MIN};
use crate::error::ApiError;

/// The longest cursor this server accepts: the spec caps it there, so a longer token was not
/// issued here (`api-paging.ts:59-63`).
const MAX_CURSOR_LENGTH: usize = 256;

/// Issue the cursor that addresses "everything before `ordinal` has been served".
///
/// The epoch is passed IN rather than read from a process global, because the hub
/// ([`crate::events::EventHub`]) owns the epoch and a route must not be able to stamp a
/// cursor with a different string than the one it reports in `X-Harness-Epoch`.
#[must_use]
pub fn encode_cursor(epoch: &str, ordinal: u64) -> String {
    // `epoch` is base36 + hex (`api-events.ts:83`), so it never contains `.`; the decoder
    // still splits on the LAST dot, which is what makes an epoch that somehow grew one safe.
    URL_SAFE_NO_PAD.encode(format!("{epoch}.{ordinal}"))
}

/// Resolve a caller-supplied cursor into an ordinal (`api-paging.ts:54-87`).
///
/// An absent (or empty) cursor means the first page, i.e. `0`. Anything the server did not
/// issue in THIS epoch is a 400 naming `cursor`, exactly as the spec requires: a silent
/// restart on page one would make a client's `next_cursor` loop forever without ever saying
/// why.
///
/// # Errors
///
/// * cursor longer than 256 characters, or not decodable base64url/UTF-8 ->
///   `Query parameter "cursor" is not a cursor this server issued.`
/// * a foreign or previous-epoch token, or an ordinal that is not a non-negative integer ->
///   `Query parameter "cursor" is stale or foreign; restart from the first page.`
pub fn decode_cursor(epoch: &str, cursor: Option<&str>) -> Result<u64, ApiError> {
    // `undefined`/`null`/empty are all "the first page" in TS (`:55`, `:60`); an HTTP query
    // parameter has no other absent form, and a non-string cursor cannot exist because the
    // extractor already typed it as text.
    let Some(token) = cursor.filter(|value| !value.is_empty()) else {
        return Ok(0);
    };

    if token.len() > MAX_CURSOR_LENGTH {
        return Err(ApiError::bad_request(
            "Query parameter \"cursor\" is not a cursor this server issued.",
        ));
    }

    let text = decode_token(token).ok_or_else(|| {
        ApiError::bad_request("Query parameter \"cursor\" is not a cursor this server issued.")
    })?;

    // The separator split is on the LAST dot (`:71`), so an epoch containing a dot still
    // leaves the ordinal intact.
    let Some(separator) = text.rfind('.') else {
        return Err(stale_cursor());
    };
    let (token_epoch, ordinal_text) = text.split_at(separator);
    let ordinal_text = &ordinal_text[1..];

    // The ordinal is whatever `Number()` can coerce (`api-paging.ts:74`): this server issues digits,
    // but a client that re-encoded its own cursor (`+5`, `1e3`, `0x10`) is still describing an
    // ordinal, and TS accepts it. A token whose ordinal half cannot be coerced, or that is not a safe
    // non-negative integer, is by definition not one this server issued.
    let ordinal = match number_value(ordinal_text) {
        Some(value) if is_safe_integer(value) && value >= 0.0 => value as u64,
        _ => return Err(stale_cursor()),
    };

    // The epoch half is a strict string compare (`:79`), so a coerced ordinal from a FOREIGN epoch is
    // still the stale/foreign refusal.
    if token_epoch != epoch {
        return Err(stale_cursor());
    }

    Ok(ordinal)
}

/// The ECMAScript `Number(text)` coercion (`api-paging.ts:74`, `:101`) for exactly the shapes a query
/// string can carry: trim, then an optional sign, then a radix literal (`0x`/`0o`/`0b`), a decimal
/// with an optional fraction, or a decimal with an optional exponent.
///
/// `None` is every value TS would reject through `Number.isSafeInteger` anyway: `NaN` (any spelling
/// JS does not recognise, including `1.5.2` and `1_000` - numeric separators are a SOURCE form, not a
/// string coercion) and `Infinity`/`-Infinity` (which includes `1e400`). An empty or whitespace-only
/// text is `0`, because `Number("")` and `Number("  ")` are both `0` - that is why `?limit=` is a
/// bounds refusal (`0 < min`) while an ABSENT `limit` is the default.
fn number_value(text: &str) -> Option<f64> {
    let text = text.trim();

    if text.is_empty() {
        return Some(0.0);
    }

    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };

    let magnitude = |value: f64| if negative { -value } else { value };

    // The radix literals: digits only after the prefix, no sign, no fraction and no exponent
    // (`Number("0x+5")` and `Number("0x")` are both `NaN`).
    for (prefix, radix) in [("0x", 16u32), ("0o", 8), ("0b", 2)] {
        if rest.len() > prefix.len()
            && rest.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
        {
            let parsed = i64::from_str_radix(&rest[prefix.len()..], radix).ok()?;

            return Some(magnitude(parsed as f64));
        }
    }

    // The decimal form, scanned rather than trusted: Rust's `f64::from_str` also accepts `inf`,
    // `infinity`, `NaN` and a bare `.5`/`5.` tail, and JS's grammar for those is different.
    let is_digit = |glyph: char| glyph.is_ascii_digit();
    let bytes = rest.as_bytes();
    let mut index = 0;
    let mut seen_digits = false;

    while index < bytes.len() && is_digit(bytes[index] as char) {
        index += 1;
        seen_digits = true;
    }

    if index < bytes.len() && bytes[index] == b'.' {
        index += 1;

        while index < bytes.len() && is_digit(bytes[index] as char) {
            index += 1;
            seen_digits = true;
        }
    }

    if !seen_digits {
        return None;
    }

    if index < bytes.len() && (bytes[index] | 0x20) == b'e' {
        index += 1;

        if index < bytes.len() && (bytes[index] == b'+' || bytes[index] == b'-') {
            index += 1;
        }

        let exponent_start = index;

        while index < bytes.len() && is_digit(bytes[index] as char) {
            index += 1;
        }

        // An `e` with no digits after it is `NaN` in JS.
        if exponent_start == index {
            return None;
        }
    }

    if index != bytes.len() {
        return None;
    }

    rest.parse::<f64>().ok().map(magnitude)
}

/// `Number.isSafeInteger` (`:80`, `:103`): a finite, integral value inside the exact `f64` integer
/// range.
#[must_use]
fn is_safe_integer(value: f64) -> bool {
    value.is_finite() && value.fract() == 0.0 && value.abs() <= (1i64 << 53) as f64 - 1.0
}

/// The refusal every unusable cursor gets (`api-paging.ts:83`).
fn stale_cursor() -> ApiError {
    ApiError::bad_request(
        "Query parameter \"cursor\" is stale or foreign; restart from the first page.",
    )
}

/// base64url -> UTF-8 text, `None` when either step fails.
///
/// A trailing `=` is tolerated before decoding so a client that re-padded our unpadded token
/// still gets an answer rather than a refusal (`Buffer.from(x, 'base64url')` is lenient the
/// same way).
fn decode_token(token: &str) -> Option<String> {
    let unpadded = token.trim_end_matches('=');
    let bytes = URL_SAFE_NO_PAD.decode(unpadded).ok()?;

    String::from_utf8(bytes).ok()
}

/// Resolve `limit` for any paginated route (`api-paging.ts:94-108`).
///
/// An absent limit is the contract default (25); a non-integer, zero, or over-max value is a
/// 400 naming the parameter (the schema bounds it at 1..=100, and a schema violation is a 400
/// on every other surface too).
///
/// # Errors
///
/// [`ApiError::bad_request`] with
/// `Query parameter "limit" must be an integer between 1 and 100.`
pub fn resolve_limit(limit: Option<&str>) -> Result<u64, ApiError> {
    const LIMIT_MESSAGE: &str = "Query parameter \"limit\" must be an integer between 1 and 100.";

    // An ABSENT `limit` is the default. `?limit=` is NOT absent: the TS route passes the RAW
    // `URLSearchParams.get` value to `resolveLimit` (`api.ts:1489`), and `Number("")` is `0`, which
    // the bounds check then refuses. (An earlier revision of this port treated empty as absent
    // because the OTHER filters do; T7b follows the paging helper itself.)
    let Some(raw) = limit else {
        return Ok(PAGE_LIMIT_DEFAULT);
    };

    // `const value = Number(limit)` + `Number.isSafeInteger` (`api-paging.ts:101-103`), so `+25`,
    // `25.0`, `1e2` and `0x19` all resolve, while `1.5`, `abc`, `1e400` and an over-2^53 digit run
    // do not.
    let Some(value) = number_value(raw).filter(|value| is_safe_integer(*value)) else {
        return Err(ApiError::bad_request(LIMIT_MESSAGE));
    };

    // `value < HARNESS_PAGE_LIMIT.min || value > HARNESS_PAGE_LIMIT.max` (`:103`). The comparison is
    // on the coerced number because that is what TS bounds, and a negative zero is inside the range
    // exactly as it is there.
    if value < PAGE_LIMIT_MIN as f64 || value > PAGE_LIMIT_MAX as f64 {
        return Err(ApiError::bad_request(LIMIT_MESSAGE));
    }

    Ok(value as u64)
}

/// A bounded slice: the entries for this page plus whether a next page exists
/// (`api-paging.ts:111-115`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PageSlice<T> {
    pub items: Vec<T>,
    /// Ordinal of the first entry of the NEXT page, or `None` when this page ended the
    /// collection.
    pub next: Option<u64>,
}

/// The brief-facing name for the same helper shape; kept as an alias so a caller may spell it
/// either way without a second type.
pub type Page<T> = PageSlice<T>;

impl<T> PageSlice<T> {
    /// Maps the page's items while preserving the cursor (`api-paging.ts` keeps the slice and
    /// the projection separate).
    #[must_use]
    pub fn map<U>(self, transform: impl Fn(T) -> U) -> PageSlice<U> {
        PageSlice {
            items: self.items.into_iter().map(transform).collect(),
            next: self.next,
        }
    }
}

/// Slice `[cursor, cursor + limit)` out of an ordered collection (`api-paging.ts:124-139`).
///
/// `total` mirrors TS's `total: number = -1`:
/// * `None` = the total is UNKNOWN (TS `-1`), so the contract's end-of-collection rule is the
///   only one available and an exactly-full page ALWAYS advertises a next page, "possibly to an
///   empty page" (`api-paging.ts:136`).
/// * `Some(n)` = the route KNOWS the total, which is what every real canonical route passes.
///   Then an exactly-full page that has reached `n` ENDS the collection (`next_cursor: null`),
///   and `n` - not `entries.len()` - is the bound, because a paged store legitimately hands
///   over only the materialized window.
#[must_use]
pub fn slice_page<T>(
    entries: Vec<T>,
    cursor: u64,
    limit: u64,
    total: Option<usize>,
) -> PageSlice<T> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let start = usize::try_from(cursor).unwrap_or(usize::MAX);

    // TS `entries.length` (`:130`), captured before the owning `skip`/`take` consumes the Vec.
    // It only ever matters in the unknown-total branch as a readable fallback, because the
    // `total < 0` test short-circuits first.
    let materialized = entries.len();

    // `skip`/`take` on the owning chain: the collection is consumed exactly `cursor + limit`
    // entries deep and no further, which is what keeps a 256-conversation roster page cheap
    // even when the caller hands over a Vec.
    let items: Vec<T> = entries.into_iter().skip(start).take(limit).collect();

    // `known` is TS's `total >= 0 ? total : entries.length` (`:130`): the declared total wins
    // when the route states one, and the materialized length is only the fallback for the
    // unknown case.
    let known = total.unwrap_or(materialized);
    let served = cursor + items.len() as u64;
    let served_known = u64::try_from(known).unwrap_or(u64::MAX);

    // Exactly-full always continues (the contract's rule, `api-paging.ts:136`), UNLESS the
    // route declared a total and this page has already reached it. `limit` here is the clamped
    // `usize` above, so `items.len() == limit` is TS's `items.length === limit`.
    let has_more = items.len() == limit && (total.is_none() || served < served_known);

    PageSlice {
        items,
        next: has_more.then_some(served),
    }
}

/// The `PageMeta` every page carries, stamped from the hub revision of that collection
/// (`api-paging.ts:142-153`). Kept separate from `contract::PageMeta` so a route can build a
/// page body without allocating the typed model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageBodyMeta {
    /// The EFFECTIVE page size - what this page actually carries.
    pub limit: u64,
    pub next_cursor: Option<String>,
    pub observed_at: u64,
    pub revision: u64,
}

impl PageBodyMeta {
    /// The meta of one served slice, encoding the cursor with the hub's epoch
    /// (`api-paging.ts:145-152`).
    #[must_use]
    pub fn of<T>(
        epoch: &str,
        items: &[T],
        next: Option<u64>,
        revision: u64,
        observed_at: u64,
    ) -> Self {
        Self {
            limit: items.len() as u64,
            next_cursor: next.map(|ordinal| encode_cursor(epoch, ordinal)),
            observed_at,
            revision,
        }
    }

    /// The five wire members of a page body: the four meta fields plus `items`.
    ///
    /// Key order is the map's (alphabetical, because serde_json's `preserve_order` feature is
    /// off for this crate). JSON objects are unordered, so this changes only the byte layout
    /// of a response, never its meaning - see knowledge.md "Decisions".
    #[must_use]
    pub fn into_body<T>(self, items: T) -> serde_json::Value
    where
        T: Serialize,
    {
        let mut body = serde_json::Map::with_capacity(5);
        body.insert("limit".to_owned(), serde_json::Value::from(self.limit));
        body.insert(
            "next_cursor".to_owned(),
            match self.next_cursor {
                Some(cursor) => serde_json::Value::String(cursor),
                None => serde_json::Value::Null,
            },
        );
        body.insert(
            "observed_at".to_owned(),
            serde_json::Value::from(self.observed_at),
        );
        body.insert(
            "revision".to_owned(),
            serde_json::Value::from(self.revision),
        );
        body.insert(
            "items".to_owned(),
            serde_json::to_value(items).unwrap_or(serde_json::Value::Null),
        );

        serde_json::Value::Object(body)
    }
}

/// Assemble one page response: `PageMeta` plus `items` (`api-paging.ts:142-153`).
///
/// `limit` echoes the EFFECTIVE page size, and `next_cursor` is JSON `null` at the end of the
/// collection - both are contract requirements, not conveniences.
#[must_use]
pub fn page_body<T: Serialize>(
    epoch: &str,
    items: Vec<T>,
    next: Option<u64>,
    revision: u64,
    observed_at: u64,
) -> serde_json::Value {
    let meta = PageBodyMeta::of(epoch, &items, next, revision, observed_at);

    meta.into_body(items)
}

/// The same body from a [`PageSlice`], which is what a handler that just called
/// [`slice_page`] wants.
#[must_use]
pub fn page_slice_body<T: Serialize>(
    epoch: &str,
    slice: PageSlice<T>,
    revision: u64,
    observed_at: u64,
) -> serde_json::Value {
    page_body(epoch, slice.items, slice.next, revision, observed_at)
}
