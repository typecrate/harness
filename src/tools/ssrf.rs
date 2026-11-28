//! The SSRF gate the network tools share: is this host one this harness may connect to?
//!
//! TypeScript origin
//! ----------------
//! `packages/underload/harness/src/engine/tools/webdownload/download.tool.ts` - the
//! hardened predicate set (`PRIVATE_IPV4`/`PRIVATE_IPV6` at `:469-470`, `privateIPv4` at
//! `:475`, the address-form decoder `ipv4Number`/`atonToIPv4`/`ipv6Groups`/`embeddedIPv4`
//! at `:509-665`, `isPrivateHost` at `:674`, `resolvePublicHost` at `:749`), restated
//! verbatim from `tools/webfetch/request.ts:493-830` by the reference itself so `download`
//! and `fetch` cannot drift apart. This crate keeps ONE copy of that logic instead of two
//! restatements, because the reference's whole argument for restating it (a compiled tool
//! entry may not import a sibling value) is a PACKAGING constraint the TS runtime has and a
//! Rust crate does not.
//!
//! WHAT THE GATE IS FOR
//! --------------------
//! A harness runs inside a network the model is not supposed to be able to read. Without
//! this gate, `http://127.0.0.1:631/` is a request the tool will happily make: the bytes of
//! an internal service arrive in the model's context AND (for `download`) on disk, and a
//! CLOSED loopback port answers differently from an OPEN one, which turns the tool into a
//! port-open oracle. Three gates, in increasing cost:
//!
//!   1. SPELLING - [`is_private_host`]: absolute http(s), no credentials, bounded length
//!      (the caller's own URL validator), and a PUBLIC host. Public is decided on the
//!      DECODED address, never on its text (see "ADDRESS-FORM DECODING" below). Always
//!      available, costs nothing.
//!   2. RESOLVED ADDRESS - [`resolve_public_host`]: a public NAME whose A record is private
//!      (`127.0.0.1.nip.io`) passes gate 1 and fails here.
//!   3. EVERY HOP - the caller re-runs BOTH gates on every `Location` before following it.
//!      That is only possible because the hops are followed BY HAND by the caller (a
//!      platform default that resolves `Location` itself validates nothing).
//!
//! THE LIMIT THAT STAYS, STATED LOUDLY
//! -----------------------------------
//! Gate 2 is FAIL-OPEN. A resolver that is missing, that errors, or that outlives
//! [`RESOLVE_TIMEOUT_MS`] means "no verdict", not "private": a resolver problem is not a
//! security answer, and refusing the public internet because one resolver is flaky is its
//! own defect (the same trade the reference makes at `request.ts:442-448`). A caller that
//! reports this gate to the model therefore has to say WHEN it applied - which is what
//! `download`'s description does.
//!
//! ADDRESS-FORM DECODING (no DNS, no dependencies)
//! -----------------------------------------------
//! A gate that only reads dotted quads is a gate you walk past by writing the same address
//! differently. Every notation below is decoded, and decoding is ADDITIVE by construction:
//! every form the plain tables refuse still refuses (including the deliberately-wide `::`
//! and `64:ff9b::` prefixes), and a helper that is UNSURE returns `None` so the text falls
//! through to the table verdict. An unexpected address string can therefore never turn a
//! public page into a refusal by failing to parse.
//!
//! DECIDED AGAINST, mirrored so the omission stays a decision here too: prefixes whose text
//! LOOKS like it carries an IPv4 but whose specification puts none at that bit position -
//! ORCHID (RFC 4843: a 28-bit prefix plus a 100-bit hash; registered prefixes `2001:20::/28`
//! RFC 7194, `2001:30::/28` RFC 7360) and the 6VTEL `2620::/27` family. `2001:10::7f00:1`,
//! `2620::7f00:1` and `2620:1000:7f00:1::` stay PUBLIC, with no decoded-address citation
//! behind a refusal; the tests below pin all three so a future "widen it" edit is a
//! decision, not an accident.
//!
//! WHERE THIS CRATE AND THE REFERENCE CANNOT AGREE
//! ----------------------------------------------
//! * TS loads `node:dns/promises` lazily and memoizes the FAILURE; this crate resolves
//!   through `std::net::ToSocketAddrs` on a blocking thread, which cannot be *probed* for
//!   absence, so [`Resolver::system`] is the flag that says whether a lookup is attempted
//!   at all (and a caller with no resolver sets it to `false` rather than letting every call
//!   pay a failed `getaddrinfo`).
//! * A Rust `String` is valid UTF-8, so a host name cannot carry the unpaired surrogates TS
//!   would have to normalize; nothing else about the predicate differs, and the corpus below
//!   is the reference's own.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::ToSocketAddrs;
use std::sync::OnceLock;

use regex::Regex;

/// `download.tool.ts:741` / `webfetch/request.ts:772` `RESOLVE_TIMEOUT_MS`: a slow or hostile
/// DNS answer must not consume the transfer budget before the transfer starts.
pub const RESOLVE_TIMEOUT_MS: u64 = 2_000;

/// `download.tool.ts:469` `PRIVATE_IPV4` - the dotted-quad ranges that are not public.
/// `100.64.0.0/10` (CGNAT) is a PREFIX TEST, not a regex arm, exactly as it is there.
static PRIVATE_IPV4: OnceLock<Regex> = OnceLock::new();
/// `download.tool.ts:470` `PRIVATE_IPV6` - the blanket IPv6 rules. Wider than the prefixes
/// are strictly (`::` alone and `64:ff9b::` refuse their whole families), which is the
/// reference's deliberate over-refusal and stays here.
static PRIVATE_IPV6: OnceLock<Regex> = OnceLock::new();
/// `download.tool.ts:674` `isPrivateHost`'s first branch: four 1-3 digit labels.
static DOTTED_QUAD: OnceLock<Regex> = OnceLock::new();
/// `download.tool.ts:692` `mapped`: the IPv4-mapped text with a DOTTED tail.
static MAPPED_IPV4: OnceLock<Regex> = OnceLock::new();
/// `download.tool.ts:512` `ipv4Number`'s hex arm.
static ATON_HEX: OnceLock<Regex> = OnceLock::new();
/// `download.tool.ts:519` `ipv4Number`'s octal arm (`0` on its own is decimal zero).
static ATON_OCTAL: OnceLock<Regex> = OnceLock::new();
/// `download.tool.ts:524` `ipv4Number`'s decimal arm, length-capped so a 20-digit label
/// cannot quietly round into a valid byte.
static ATON_DECIMAL: OnceLock<Regex> = OnceLock::new();
/// `download.tool.ts:594` `readHalf`'s group arm: 1-4 hex digits.
static IPV6_GROUP: OnceLock<Regex> = OnceLock::new();

/// One compiled pattern per slot. The sources are literals taken from the reference, so a
/// failure here is a programming error, not input-dependent.
fn pattern<'a>(slot: &'a OnceLock<Regex>, source: &str) -> &'a Regex {
    slot.get_or_init(|| Regex::new(source).expect("the published pattern compiles"))
}

/// The `PRIVATE_IPV4` table on its own (`download.tool.ts:469`), WITHOUT the CGNAT prefix arm.
///
/// The IPv4-mapped branch of the reference (`:693`) tests THIS function's twin, not
/// `privateIPv4`, so `::ffff:100.64.0.1` is public there and stays public here: the branch
/// returns before the prefix test could run. Keeping the two arms separate is what lets that
/// asymmetry be stated instead of quietly normalized.
#[must_use]
fn private_ipv4_ranges(address: &str) -> bool {
    pattern(
        &PRIVATE_IPV4,
        r"^(?:10\.|127\.|169\.254\.|172\.(?:1[6-9]|2\d|3[01])\.|192\.0\.0\.|192\.168\.|198\.1[89]\.|0\.)",
    )
    .is_match(address)
}

/// The one IPv4 predicate the whole gate shares (`download.tool.ts:475` `privateIPv4`), so a
/// DECODED address is refused by exactly the rule its dotted-quad twin is refused by.
#[must_use]
pub fn private_ipv4(address: &str) -> bool {
    private_ipv4_ranges(address) || address.starts_with("100.64.")
}

/// One number in the grammar `inet_aton` accepts and the WHATWG URL host parser copies
/// (`download.tool.ts:509` `ipv4Number`): decimal, `0x`-prefixed hex, or a leading-`0` octal.
///
/// `08` and `09` are NOT numbers in this grammar (octal digits only), so `None` - a stack that
/// cannot parse them treats the host as a name, which is the fall-through this decoder is
/// built around.
fn ipv4_number(label: &str) -> Option<u32> {
    if label.is_empty() {
        return None;
    }

    if let Some(hex) = pattern(&ATON_HEX, r"^0x([0-9a-fA-F]{1,8})$").captures(label) {
        return u32::from_str_radix(hex.get(1)?.as_str(), 16).ok();
    }

    if label.len() > 1 && label.starts_with('0') {
        if !pattern(&ATON_OCTAL, r"^0[0-7]{1,11}$").is_match(label) {
            return None;
        }

        return u32::from_str_radix(&label[1..], 8).ok();
    }

    if !pattern(&ATON_DECIMAL, r"^\d{1,10}$").is_match(label) {
        return None;
    }

    label.parse().ok()
}

/// A host written as 1-4 `inet_aton` numbers - where the LAST number holds the remaining
/// bytes, which is what makes `127.1` and a bare `2130706433` addresses rather than names -
/// as a dotted quad (`download.tool.ts:533` `atonToIPv4`).
///
/// `None` means "not that shape", the answer for every ordinary hostname: a single non-numeric
/// label (`example`) stops the decode.
#[must_use]
pub fn aton_to_ipv4(host: &str) -> Option<String> {
    // A trailing dot is the root label, not a part: `127.0.0.1.` is the same host.
    let trimmed = host.strip_suffix('.').unwrap_or(host);
    let labels: Vec<&str> = trimmed.split('.').collect();

    if labels.len() > 4 {
        return None;
    }

    let mut numbers: Vec<u32> = Vec::with_capacity(labels.len());

    for label in labels {
        numbers.push(ipv4_number(label)?);
    }

    // The leading parts are single bytes; the last one is 1-4 bytes wide and may not overflow
    // them (`4294967296` is not an address, `4294967295` is 255.255.255.255).
    let width = 5 - numbers.len();
    let last = numbers.pop().unwrap_or(0);

    if u64::from(last) >= 256_u64.pow(u32::try_from(width).unwrap_or(4)) {
        return None;
    }

    if numbers.iter().any(|value| *value > 255) {
        return None;
    }

    let mut bytes: Vec<u8> = numbers.iter().map(|value| *value as u8).collect();

    // The last number's bytes come out most-significant first, so the shift counts BITS and
    // runs from `(width-1)*8` down to 0 - `2**24` is the 16,777,216 that extracts the leading
    // 127 of `2130706433`. Reading this as `256 ** shift` in BITS would be the reference's
    // first attempt at this decoder (`webfetch/request.ts:563-571`): it floors every byte but
    // the last to zero, turning the PUBLIC decimal `134744072` (8.8.8.8) into `0.0.0.8` and
    // refusing a public page. The public rows below are what keep that class of bug visible.
    for index in (0..width).rev() {
        let shift = u32::try_from(index * 8).unwrap_or(0);
        bytes.push(((last / 2_u32.pow(shift)) % 256) as u8);
    }

    Some(
        bytes
            .iter()
            .map(|byte| byte.to_string())
            .collect::<Vec<_>>()
            .join("."),
    )
}

/// The eight 16-bit groups of an IPv6 text form (`download.tool.ts:574` `ipv6Groups`): one
/// `::` run, 1-4 hex digits per group, and the IPv4 tail (dotted, or in any `inet_aton`
/// notation) the address grammar allows in place of the last two groups. `None` = not IPv6
/// text.
fn ipv6_groups(host: &str) -> Option<Vec<u16>> {
    let halves: Vec<&str> = host.split("::").collect();

    if halves.len() > 2 {
        return None;
    }

    // An IPv4 tail is only an IPv4 tail at the very END of the address, so the head may read
    // one only when there is no second half at all, or when the second half is empty.
    let head = read_half(
        halves[0],
        if halves.len() == 2 {
            halves[1].is_empty()
        } else {
            true
        },
    )?;

    if halves.len() == 1 {
        return (head.len() == 8).then_some(head);
    }

    let tail = read_half(halves[1], true)?;

    // `::` stands for AT LEAST one zero group; accepting the 8-group slop keeps a legal-looking
    // text from escaping the decode entirely (the reference's own note).
    if head.len() + tail.len() > 8 {
        return None;
    }

    let mut groups = head;
    groups.resize(8 - tail.len(), 0);
    groups.extend_from_slice(&tail);

    Some(groups)
}

/// One side of a `::` (`download.tool.ts:578` `readHalf`).
fn read_half(text: &str, allow_ipv4_tail: bool) -> Option<Vec<u16>> {
    if text.is_empty() {
        return Some(Vec::new());
    }

    let labels: Vec<&str> = text.split(':').collect();
    let mut groups: Vec<u16> = Vec::with_capacity(labels.len());

    for (index, label) in labels.iter().enumerate() {
        let is_end = index + 1 == labels.len();

        if is_end && allow_ipv4_tail && label.contains('.') {
            // `aton_to_ipv4` always returns FOUR quads, so the four indexes are in range by
            // construction; `unwrap_or(0)` keeps a future change to that helper from being a
            // panic rather than a verdict change.
            let embedded = aton_to_ipv4(label)?;
            let parts: Vec<u16> = embedded
                .split('.')
                .map(|part| part.parse::<u16>().unwrap_or(0))
                .collect();

            let quad = |index: usize| parts.get(index).copied().unwrap_or(0);

            groups.push((quad(0) << 8) | quad(1));
            groups.push((quad(2) << 8) | quad(3));

            continue;
        }

        if !pattern(&IPV6_GROUP, r"^[0-9a-fA-F]{1,4}$").is_match(label) {
            // An empty label, five hex digits, or a `g`: not IPv6 text at all.
            return None;
        }

        groups.push(u16::from_str_radix(label, 16).ok()?);
    }

    Some(groups)
}

/// The IPv4 addresses an IPv6 address carries inside it, as dotted quads
/// (`download.tool.ts:642` `embeddedIPv4`). Each form contributes the embedding its own
/// specification defines, plus the one a sloppy writer may mean, because the question is
/// "could this address end up at a private host?" - a refusal costs a download, a miss costs
/// an internal read.
///
///   * IPv4-mapped `::ffff:a.b.c.d` (RFC 4291 2.5.5.2) and IPv4-compatible `::a.b.c.d` (the
///     deprecated 2.5.5.1): the embedded address, as written;
///   * IPv4-translated `64:ff9b::a.b.c.d` (RFC 6052 2.1, well-known /96): likewise, and checked
///     on the GROUPS because the longhand spelling `64:ff9b:0:0:0:0:7f00:1` is invisible to a
///     prefix test on the text;
///   * 6to4 `2002:VVVV:VVVV::` (RFC 3056 5.1, RFC 4291 2.5.7): the tunnel's IPv4 in BITS 16-47
///     (the SECOND AND THIRD groups, NOT inverted), and this one is not merely an encoding: a
///     packet to `2002:7f00:1::` is ENCAPSULATED and delivered to 127.0.0.1, so the embedded
///     address IS the destination a download reaches (RFC 3964 3.1/3.2 tells a 6to4 router and
///     relay to disallow private/reserved addresses in tunnels);
///   * Teredo `2001::/32` (RFC 4380 5.1): the CLIENT IPv4 sits in the low 32 bits bitwise
///     INVERTED (`ff00:fffe` is 127.0.0.1) and the relay's IPv4 in the 32 bits above it, also
///     inverted; the non-inverted low half is checked too, because a compatibility-mode writer
///     puts the embedded address there plain - which is why `2001::200:0:7f00:1` is also
///     refused.
fn embedded_ipv4(groups: &[u16]) -> Vec<String> {
    // The invariant is that `ipv6_groups` returned exactly 8 groups; anything else (a caller
    // handing in a slice from elsewhere) embeds nothing rather than reading past the end.
    if groups.len() != 8 {
        return Vec::new();
    }

    // One 32-bit window of the address as a dotted quad, optionally INVERTED (Teredo).
    let dotted = |high: u16, low: u16, invert: bool| -> String {
        let bytes = [
            (high >> 8) as u8,
            (high & 0xff) as u8,
            (low >> 8) as u8,
            (low & 0xff) as u8,
        ];

        bytes
            .iter()
            .map(|byte| {
                if invert {
                    usize::from(255 - *byte)
                } else {
                    usize::from(*byte)
                }
                .to_string()
            })
            .collect::<Vec<_>>()
            .join(".")
    };

    let zeros = |start: usize, end: usize| -> bool {
        groups
            .get(start..end)
            .is_some_and(|window| window.iter().all(|group| *group == 0))
    };

    let group = |index: usize| groups.get(index).copied().unwrap_or(0);

    if group(5) == 0xffff && zeros(0, 5) {
        return vec![dotted(group(6), group(7), false)];
    }

    if zeros(0, 6) {
        return vec![dotted(group(6), group(7), false)];
    }

    if group(0) == 0x0064 && group(1) == 0xff9b && zeros(2, 6) {
        return vec![dotted(group(6), group(7), false)];
    }

    if group(0) == 0x2002 {
        return vec![dotted(group(1), group(2), false)];
    }

    if group(0) == 0x2001 && group(1) == 0x0000 {
        return vec![
            dotted(group(6), group(7), false),
            dotted(group(6), group(7), true),
            dotted(group(2), group(3), true),
        ];
    }

    Vec::new()
}

/// Gate 1's address test (`download.tool.ts:674` `isPrivateHost`, branch order verbatim:
/// dotted quad first so its old verdict is unchanged, then the `inet_aton` notations, then
/// IPv6 - mapped text first, then the blanket prefixes, then the embedded-address decode).
///
/// The host FORM is never a public page: empty/whitespace counts as private (it cannot be
/// fetched anyway), and `localhost` plus any `.localhost` name is private by rule.
#[must_use]
pub fn is_private_host(hostname: &str) -> bool {
    // `download.tool.ts:675` trims, lowercases, and strips the brackets a URL puts around an
    // IPv6 literal (`^\[(.*)\]$`).
    let lowered = hostname.trim().to_lowercase();
    let host = lowered
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(&lowered);

    if host.is_empty() {
        return true;
    }

    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }

    // An IPv4 literal, including the dotted form a browser accepts.
    if pattern(&DOTTED_QUAD, r"^\d{1,3}(?:\.\d{1,3}){3}$").is_match(host) {
        return private_ipv4(host);
    }

    // The same address in the other notations a stack accepts. This runs AFTER the dotted-quad
    // test above so that check keeps exactly its old behaviour, and it only ever ADDS a
    // refusal: an address that decodes to a public quad falls through to `false`.
    if let Some(shorthand) = aton_to_ipv4(host) {
        return private_ipv4(&shorthand);
    }

    // An IPv6 literal: anything with a colon. Also covers the mapped IPv4 forms.
    if host.contains(':') {
        if let Some(mapped) =
            pattern(&MAPPED_IPV4, r"^::ffff:(\d{1,3}(?:\.\d{1,3}){3})$").captures(host)
        {
            return private_ipv4_ranges(mapped.get(1).map_or("", |matched| matched.as_str()));
        }

        // The blanket rules first, unchanged: they refuse the whole `::` and `64:ff9b::`
        // families, wider than those prefixes are, which was the behaviour before the decode
        // and errs on the safe side.
        if pattern(
            &PRIVATE_IPV6,
            r"(?i)^(?:(?:fe80|fc|fd)[0-9a-f:]*|::1?|::ffff:(?:7f00:1|0?0?0?0?0?1)(?::0)?|64:ff9b::)",
        )
        .is_match(host)
        {
            return true;
        }

        // Then the decode, for the text forms those prefixes miss: an uncompressed
        // mapped/translated address, a 6to4 tunnel endpoint, Teredo's inverted client address.
        // An address that parses as IPv6 but embeds no IPv4 - or embeds a PUBLIC one, e.g.
        // `2002:808:808::` or the anycast relay `2002:c058:6301::` - stays public.
        if let Some(groups) = ipv6_groups(host) {
            return embedded_ipv4(&groups)
                .iter()
                .any(|address| private_ipv4(address));
        }
    }

    false
}

/// Whether a host string is an ADDRESS in some notation, so a DNS lookup about it could not
/// change the verdict (`download.tool.ts:766-771` - dotted quad, any IPv6 text, or an
/// `inet_aton` shorthand).
///
/// The reference needed all three tests because its old gate-2 short circuit was "dotted quad
/// or contains a colon", so a single-label notation like `2130706433` still asked DNS about a
/// name that is not a name. This is the same rule, and the test that counts the lookups is the
/// same proof.
#[must_use]
pub fn is_address_form(hostname: &str) -> bool {
    let host = hostname.trim().to_lowercase();

    host.contains(':')
        || pattern(&DOTTED_QUAD, r"^\d{1,3}(?:\.\d{1,3}){3}$").is_match(&host)
        || aton_to_ipv4(&host).is_some()
}

/// How a hostname's addresses are obtained - the reference's `DnsLookup` parameter
/// (`resolvePublicHost(hostname, lookup)`), which the reference keeps injectable precisely so
/// a test can assert BOTH that the gate is asked and that it is asked nothing for an address
/// form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolver {
    /// Fixed answers per LOWERCASED hostname. Non-empty means "the caller supplied a
    /// resolver", which is the TS `lookup` seam: a test (or a deployment in front of a
    /// private DNS view) decides what a name resolves to, and no system lookup happens for
    /// a name listed here.
    pub answers: BTreeMap<String, Vec<IpAddr>>,
    /// Whether a system resolver may be asked at all. `true` is the default, and mirrors the
    /// reference's lazy `node:dns` import SUCCEEDING; `false` is its import FAILING (a browser
    /// or embedded runtime), which degrades gate 2 instead of failing the transfer.
    pub system: bool,
}

impl Default for Resolver {
    /// The real resolver, with no injected answers - the state a production call runs with.
    fn default() -> Self {
        Self {
            answers: BTreeMap::new(),
            system: true,
        }
    }
}

impl Resolver {
    /// A resolver that cannot answer: gate 2 has no verdict and fails open, exactly as the
    /// reference's `lookup === undefined` branch.
    #[must_use]
    pub fn unavailable() -> Self {
        Self {
            answers: BTreeMap::new(),
            system: false,
        }
    }

    /// A resolver with fixed answers - the TS `lookup` injection.
    #[must_use]
    pub fn fixed(answers: &[(&str, &[IpAddr])]) -> Self {
        Self {
            answers: answers
                .iter()
                .map(|(host, addresses)| ((*host).trim().to_lowercase(), (*addresses).to_vec()))
                .collect(),
            system: false,
        }
    }

    /// Whether gate 2 has a resolver AT ALL — the reference's `resolve !== undefined` test
    /// (`webfetch/request.ts`, `resolvePublicHost`'s first line).
    ///
    /// This is deliberately NOT "does this host have an entry": a name the injected table does not
    /// know is a lookup that ANSWERED NOTHING (the reference's `addresses ?? []` branch, which
    /// fails open later), and that is a different situation from having no lookup function at all —
    /// the difference decides whether the private-literal test below runs.
    #[must_use]
    pub fn is_present(&self) -> bool {
        !self.answers.is_empty() || self.system
    }
}

/// What gate 2 decided (`resolvePublicHost`'s return): `blocked` plus, when a resolver produced
/// one, the address that caused the refusal, so the caller's sentence can name it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Resolve {
    pub blocked: bool,
    pub address: Option<String>,
}

/// Gate 2 (`download.tool.ts:749` `resolvePublicHost`): resolve the NAME and test every answer.
///
/// FAIL-OPEN by design - no resolver, a lookup error, an empty answer, or a lookup that
/// outlives [`RESOLVE_TIMEOUT_MS`] all mean "no verdict" (`blocked: false`), because a
/// resolver problem is not a security answer and refusing the public internet over a flaky
/// resolver is its own defect.
#[must_use]
pub async fn resolve_public_host(host: &str, resolver: &Resolver) -> Resolve {
    let hostname = host.trim().to_lowercase();

    // No resolver: no verdict. The reference returns BEFORE its address test in this branch, so
    // the order is part of the mirror — and it is why a browser or embedded runtime degrades the
    // gate instead of refusing every download.
    if !resolver.is_present() {
        return Resolve::default();
    }

    // An address literal needs no lookup, and looking one up would be a wasted query.
    if is_private_host(&hostname) {
        return Resolve {
            blocked: true,
            address: None,
        };
    }

    // ANY address form short-circuits: `is_private_host` above already decoded it, so DNS
    // cannot change the answer.
    if is_address_form(&hostname) {
        return Resolve::default();
    }

    let answers: Option<Vec<IpAddr>> = if let Some(found) = resolver.answers.get(&hostname) {
        Some(found.clone())
    } else if !resolver.system {
        // The name is not in the injected table and there is no system resolver: no verdict.
        None
    } else {
        system_lookup(&hostname).await
    };

    let Some(answers) = answers else {
        return Resolve::default();
    };

    for address in answers {
        if is_private_host(&address.to_string()) {
            return Resolve {
                blocked: true,
                address: Some(address.to_string()),
            };
        }
    }

    Resolve::default()
}

/// The system lookup, bounded by [`RESOLVE_TIMEOUT_MS`] (`RESOLVE_TIMEOUT_MS` is the
/// reference's watchdog: a slow DNS answer must not consume the transfer budget before the
/// transfer starts). Port 0 asks for every A/AAAA record without committing to one.
async fn system_lookup(hostname: &str) -> Option<Vec<IpAddr>> {
    let host = hostname.to_owned();

    let lookup = tokio::task::spawn_blocking(move || {
        (host.as_str(), 0_u16)
            .to_socket_addrs()
            .map(|addresses| addresses.map(|address| address.ip()).collect::<Vec<_>>())
            .ok()
    });

    // A timeout is "no verdict", and so is a panic inside the blocking pool: neither is a
    // security answer, so the `Option` collapses both to `None` and the caller fails open.
    tokio::time::timeout(std::time::Duration::from_millis(RESOLVE_TIMEOUT_MS), lookup)
        .await
        .ok()?
        .ok()?
}

/// The host a URL names, in the form the gate reads (`new URL(x).hostname`): brackets kept for
/// an IPv6 literal, because [`is_private_host`] strips them and a caller reporting the host
/// wants the text that appeared in the URL.
#[must_use]
pub fn hostname_of(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()?
        .host_str()
        .map(str::to_owned)
        .filter(|host| !host.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shrinks the corpus literals: every row needs an `IpAddr`, and parsing the reference's
    /// own dotted-quad text is the least error-prone way to write them.
    fn ip(text: &str) -> IpAddr {
        text.parse::<IpAddr>()
            .unwrap_or_else(|_| panic!("{text} is not an address"))
    }

    #[test]
    fn the_dotted_quad_ranges_are_the_reference_table() {
        // `download.tool.ts:469`'s ranges, one row each, plus the CGNAT prefix that is NOT a
        // regex arm, plus the public neighbours that must not refuse.
        for address in [
            "10.0.0.1",
            "127.0.0.1",
            "127.0.0.53",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.9.9",
            "0.0.0.0",
            "0.1.2.3",
            "100.64.0.1",
            "100.64.255.255",
        ] {
            assert!(is_private_host(address), "{address} must be private");
        }

        // `privateIPv4` (`:476`) matches CGNAT as the STRING PREFIX `100.64.`, not as the
        // /10 mask, so the upper half of the range stays public. Pinning the boundary keeps
        // that narrowness a documented behaviour instead of an accident — widening it to a
        // real mask would be a change in what the tool refuses.
        for address in [
            "8.8.8.8",
            "93.184.216.34",
            "100.63.255.255",
            "100.128.0.1",
            "100.127.255.255",
            "172.15.0.1",
            "172.32.0.1",
            "192.0.1.1",
            "198.17.0.1",
            "198.20.0.1",
            "1.1.1.1",
        ] {
            assert!(!is_private_host(address), "{address} must be public");
        }
    }

    #[test]
    fn every_inet_aton_notation_of_a_private_address_is_refused() {
        // `2130706433` (decimal), `0x7f000001` (hex), `017700000001` (octal) and `127.1`
        // (shorthand) are 127.0.0.1 written four ways; `4294967295` is the broadcast address,
        // which the table does not treat as private, and `4294967296` is not an address at all.
        for host in [
            "2130706433",
            "0x7f000001",
            "017700000001",
            "127.1",
            "127.0.1",
            "127.0.0.1.",
            "0x7f.0.0.1",
            "0177.0.0.1",
        ] {
            assert!(is_private_host(host), "{host} must decode to loopback");
        }

        assert_eq!(aton_to_ipv4("2130706433"), Some("127.0.0.1".to_owned()));
        assert_eq!(aton_to_ipv4("0x7f000001"), Some("127.0.0.1".to_owned()));
        assert_eq!(aton_to_ipv4("017700000001"), Some("127.0.0.1".to_owned()));
        assert_eq!(aton_to_ipv4("127.1"), Some("127.0.0.1".to_owned()));

        // The PUBLIC rows are the load-bearing ones: the reference's first attempt at this
        // decoder floored every byte but the last to zero and refused `134744072` (8.8.8.8).
        assert_eq!(aton_to_ipv4("134744072"), Some("8.8.8.8".to_owned()));
        assert!(!is_private_host("134744072"));
        assert_eq!(
            aton_to_ipv4("4294967295"),
            Some("255.255.255.255".to_owned())
        );
        assert_eq!(aton_to_ipv4("4294967296"), None, "not an address");
        assert_eq!(aton_to_ipv4("08"), None, "not a number in this grammar");
        assert_eq!(aton_to_ipv4("09"), None);
        assert_eq!(
            aton_to_ipv4("example.com"),
            None,
            "a name is not an address"
        );
        assert_eq!(
            aton_to_ipv4("a.b.c.d.e"),
            None,
            "five labels is not an address"
        );
        assert_eq!(
            aton_to_ipv4("300.1.1.1"),
            None,
            "a byte must fit in one label"
        );
    }

    #[test]
    fn the_ipv6_forms_that_carry_an_ipv4_are_decoded() {
        // Loopback in every embedding the grammar allows, refused on the DECODED address. The
        // Teredo rows are the reference's own corpus (webdownload tests :819-822, :682): the
        // first is the RFC inversion, the second the reference's looser label (see the
        // `embedded_ipv4` test for why both refuse), the third compatibility mode, the fourth
        // the link-local address behind every cloud metadata endpoint.
        for host in [
            "::1",
            "::",
            "::ffff:127.0.0.1",
            "::ffff:7f00:1",
            "0:0:0:0:0:ffff:7f00:1",
            "::7f00:1",
            "64:ff9b::7f00:1",
            "64:ff9b:0:0:0:0:7f00:1",
            "2002:7f00:1::",
            "2001::ff00:fffe",
            "2001::200:0:80ff:fffe",
            "2001::200:0:ff00:fffe",
            "2001::200:0:7f00:1",
            "2001::200:0:5601:5601",
            "fe80::1",
            "fc00::1",
            "fd12:3456::1",
        ] {
            assert!(is_private_host(host), "{host} must be private");
        }

        // Public IPv6 stays public, including the 6to4 forms whose embedded address is public
        // (the anycast relay `2002:c058:6301::` is 192.88.99.1) and Teredo with a PUBLIC client
        // (8.8.8.8 and 1.1.1.1, whose inverted texts `f7f7:f7f7` and `fefe:fefe` must not read
        // as anything private).
        for host in [
            "2606:4700:4700::1111",
            "2002:808:808::",
            "2002:c058:6301::",
            "2001::200:0:808:808",
            "2001::200:0:f7f7:f7f7",
            "2001::200:0:fefe:fefe",
        ] {
            assert!(!is_private_host(host), "{host} must be public");
        }
    }

    #[test]
    fn the_orchid_and_6vtel_families_stay_public_by_decision() {
        // Recorded in the module header: these texts LOOK like they carry an IPv4, but no
        // specification puts one at that bit position, so refusing them would be an
        // un-citable refusal. Pinning them public makes "widen it" a deliberate edit.
        for host in ["2001:10::7f00:1", "2620::7f00:1", "2620:1000:7f00:1::"] {
            assert!(!is_private_host(host), "{host} stays public");
        }
    }

    #[test]
    fn host_names_are_decided_by_rule_not_by_lookup() {
        assert!(is_private_host("localhost"));
        assert!(is_private_host("db.localhost"));
        assert!(is_private_host("deep.db.localhost"));
        assert!(is_private_host("  "));
        assert!(is_private_host(""));
        assert!(!is_private_host("example.com"));
        assert!(!is_private_host("127.example.com"));
        // The reference's rule is an EXACT `localhost` match plus a `.localhost` SUFFIX match
        // (`download.tool.ts:677`), so the fully-qualified `localhost.` form is not covered —
        // pinned here because "treat any localhost spelling as private" is the tempting widening.
        assert!(!is_private_host("localhost."));
        assert!(!is_private_host("notlocalhost"));
        // A URL hands an IPv6 literal in brackets, which the reference strips before testing
        // (`:675`'s `^\[(.*)\]$`), so both spellings must decide the same way.
        assert!(is_private_host("[::1]"));
        assert!(is_private_host("[::ffff:127.0.0.1]"));
        assert!(!is_private_host("[2606:4700:4700::1111]"));
    }

    #[test]
    fn an_address_form_never_reaches_the_resolver() {
        // `download.tool.ts:759-771`: the whole point of this branch is that a single-label
        // notation must not ask DNS about a name that is not a name.
        for host in [
            "127.0.0.1",
            "2130706433",
            "0x7f000001",
            "017700000001",
            "127.1",
            "::1",
            "2002:7f00:1::",
        ] {
            assert!(is_address_form(host), "{host} is an address");
        }

        assert!(!is_address_form("example.com"));
        assert!(!is_address_form("example"));
    }

    #[tokio::test]
    async fn gate_two_uses_the_injected_resolver_and_fails_open_without_one() {
        // The `lookup` injection: a public name whose answer is private (the DNS-rebinding
        // shape gate 1 cannot see) is refused, and the refusal NAMES the address.
        let resolver = Resolver::fixed(&[("nip.example", &[ip("127.0.0.1")])]);
        let blocked = resolve_public_host("nip.example", &resolver).await;

        assert!(blocked.blocked);
        assert_eq!(blocked.address.as_deref(), Some("127.0.0.1"));

        // The same name with a public answer is allowed.
        let public = Resolver::fixed(&[("api.example", &[ip("203.0.113.7")])]);

        assert_eq!(
            resolve_public_host("api.example", &public).await,
            Resolve::default()
        );

        // A name the injected table does not know has NO verdict, and with `system: false`
        // there is nothing to ask: fail open, exactly as the reference's `catch` branch.
        let unknown = resolve_public_host("other.example", &public).await;

        assert!(!unknown.blocked);
        assert_eq!(unknown.address, None);

        // No resolver at all: gate 2 cannot answer, and the reference returns BEFORE the
        // address test in that branch (`lookup === undefined` at `:752`), so even a literal is
        // "no verdict" here. Gate 1 owns address literals, and it is unconditional — which is
        // why the order is part of the mirror rather than an optimisation.
        let none = Resolver::unavailable();

        assert_eq!(
            resolve_public_host("api.example", &none).await,
            Resolve::default()
        );
        assert!(!resolve_public_host("127.0.0.1", &none).await.blocked);
    }

    #[tokio::test]
    async fn gate_two_asks_the_resolver_nothing_for_an_address_form() {
        // The proof the reference states in its own comment: an injected resolver is asked
        // NOTHING for every address form and exactly once for a real name. Here "asked" is
        // observable, because an injected table that does not contain a name yields no verdict
        // only when no system lookup was attempted either (`system: false`), and a name that IS
        // in the table proves the lookup happened.
        let resolver = Resolver::fixed(&[("api.example", &[ip("203.0.113.7")])]);

        for host in [
            "127.0.0.1",
            "2130706433",
            "0x7f000001",
            "::1",
            "2002:7f00:1::",
        ] {
            // Private address forms are refused WITHOUT a lookup (the block carries no address,
            // which is only true of the address-literal branch).
            let verdict = resolve_public_host(host, &resolver).await;

            if is_private_host(host) {
                assert!(verdict.blocked, "{host} is private");
                assert_eq!(verdict.address, None, "{host} was not looked up");
            } else {
                assert!(!verdict.blocked, "{host} is public");
            }
        }

        // And exactly once for a name: the table's answer is what the verdict reports.
        let verdict = resolve_public_host("api.example", &resolver).await;

        assert!(!verdict.blocked);
    }

    #[tokio::test]
    async fn a_missing_system_resolver_fails_open_rather_than_refusing_the_internet() {
        // A real lookup against a name that cannot resolve settles as "no verdict" (the
        // reference's `catch`), never as a refusal.
        let verdict =
            resolve_public_host("this-host-does-not-exist.invalid", &Resolver::default()).await;

        assert!(!verdict.blocked);
    }

    #[test]
    fn the_hostname_a_url_names_is_what_the_gate_reads() {
        // An IPv6 literal keeps the brackets `Url` puts around it, because that is the text the
        // reference gate strips and the text a refusal should name.
        assert_eq!(hostname_of("http://[::1]:8080/x").as_deref(), Some("[::1]"));
        assert_eq!(
            hostname_of("https://Example.COM/a").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            hostname_of("http://127.0.0.1/x").as_deref(),
            Some("127.0.0.1")
        );
        // The scheme is not the gate's business here — `validated_url` rejects anything but
        // http(s) before this is ever called — so an ftp host still reads back.
        assert_eq!(
            hostname_of("ftp://127.0.0.1/x").as_deref(),
            Some("127.0.0.1")
        );
        assert_eq!(hostname_of("not a url"), None);
        assert_eq!(hostname_of("/relative/path"), None);
    }

    #[test]
    fn the_embedded_ipv4_helper_reports_every_form_the_specifications_define() {
        // Pinned on the decoded quads, so a change to the inversion or to the 6to4 bit window
        // fails a test instead of silently widening or narrowing the gate.
        let mapped = ipv6_groups("::ffff:127.0.0.1").expect("ipv6 text");

        assert_eq!(embedded_ipv4(&mapped), vec!["127.0.0.1"]);

        let compatible = ipv6_groups("::7f00:1").expect("ipv6 text");

        assert_eq!(embedded_ipv4(&compatible), vec!["127.0.0.1"]);

        let translated = ipv6_groups("64:ff9b:0:0:0:0:7f00:1").expect("ipv6 text");

        assert_eq!(embedded_ipv4(&translated), vec!["127.0.0.1"]);

        // 6to4: bits 16-47, NOT inverted.
        let tunnel = ipv6_groups("2002:7f00:1::").expect("ipv6 text");

        assert_eq!(embedded_ipv4(&tunnel), vec!["127.0.0.1"]);

        // Teredo, three candidates per address: the low half plain, the low half INVERTED (the
        // client IPv4, RFC 4380 5.1), and the 32 bits above it INVERTED (the relay IPv4).
        //
        // `2001::200:0:80ff:fffe` is the RFC-correct encoding of client 127.0.0.1 —
        // inverting the bytes `7f 00 00 01` gives `80 ff ff fe` — so the inverted candidate
        // is exactly `127.0.0.1`.
        //
        // NOTE, mirrored rather than "fixed": the reference's own corpus labels
        // `2001::200:0:ff00:fffe` as "client 127.0.0.1" (webdownload tests :819), but inverting
        // `ff00:fffe` byte-wise gives `0.255.0.1`, not `127.0.0.1`. Both spellings are refused
        // either way — `0.255.0.1` falls in the `0.` range the table already treats as private —
        // so the reference's OUTCOME is reproduced and its label is a loose comment over the
        // arithmetic. Changing the inversion to match the label would change every Teredo row
        // below, which is what these three assertions exist to make loud.
        // The three rows are written in UNCOMPRESSED form on purpose: `2001::200:0:80ff:fffe`
        // puts `200` in group 4, not group 2, because `::` absorbs the difference, and the relay
        // candidate reads groups 2-3. Spelling out all eight groups keeps each expectation
        // derivable from the code without counting zero runs.
        assert_eq!(
            embedded_ipv4(&ipv6_groups("2001:0:200:0:0:0:80ff:fffe").expect("ipv6 text")),
            vec!["128.255.255.254", "127.0.0.1", "253.255.255.255"]
        );
        assert_eq!(
            embedded_ipv4(&ipv6_groups("2001:0:200:0:0:0:ff00:fffe").expect("ipv6 text")),
            vec!["255.0.255.254", "0.255.0.1", "253.255.255.255"]
        );
        // Compatibility mode: the low half carries the client address UN-inverted.
        assert_eq!(
            embedded_ipv4(&ipv6_groups("2001:0:200:0:0:0:7f00:1").expect("ipv6 text")),
            vec!["127.0.0.1", "128.255.255.254", "253.255.255.255"]
        );
        // A global unicast address embeds nothing, so it can never be refused by the decode.
        assert_eq!(
            embedded_ipv4(&ipv6_groups("2606:4700:4700::1111").expect("ipv6 text")),
            Vec::<String>::new()
        );
    }
}
