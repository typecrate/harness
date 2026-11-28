//! The network tools: `fetch`, `search`, `download`.
//!
//! TypeScript origins
//! ------------------
//!   * `tools/webfetch/index.ts` + `request.ts` - `WebFetchResult`
//!     (`{provider, url, contentType, format, characters, truncated, content, title?}`),
//!     the 404/401/403/429 status sentences, and "the native provider needs no key";
//!   * `tools/websearch/index.ts` + `exa.ts` / `tavily.ts` / `linkup.ts` - the provider
//!     preference order (`WEBSEARCH_ENVIRONMENT = [EXA_API_KEY, TAVILY_API_KEY,
//!     LINKUP_API_KEY]`) and `WebSearchResult`
//!     (`{provider, query, maxResults, results: [{title, url, snippet, published?}]}`);
//!   * `tools/webdownload/download.tool.ts` - the streamed, byte-capped transfer, the MIME ->
//!     attachment-kind table, and the `toolMedia(summary, [attachment])` envelope that keeps the
//!     bytes OUT of the text the model reads.
//!
//! The reference writes into a HOST-supplied session downloads folder
//! (`webdownload/download.tool.ts:294-311`; it no longer has a `<cwd>/.underload/downloads/`
//! default), and so does this port: the folder is `<sessions dir>/<session id>/downloads`, supplied
//! through [`super::Options::downloads_root`] and wired once at `runtime.rs:560` from
//! [`crate::persistence::sessions_dir`] - the same per-session directory
//! [`crate::store`] removes on `delete_session`, which is why a session's downloaded bytes cannot
//! outlive it. With no root (a library embedding, no server session) the tool REFUSES before the
//! transfer rather than falling back to a folder the caller never named; `tests/web_surface.rs`
//! pins that both halves hold.
//!
//! WHAT THIS PORT DOES NOT RE-IMPLEMENT
//! ------------------------------------
//! `fetch` re-implementation covers the OBSERVABLE contract of `webfetch/index.ts` in full:
//! validation order, the URL/extension/MIME/size gates, both budgets, the paging keys, the
//! Cloudflare retry, the section + proximity selection, and the redaction pass. Five pieces of
//! the reference pipeline are deliberately not ported, and each one is a claim this module
//! could otherwise make falsely:
//!
//!   * **HTML -> markdown conversion** (`native.ts:240-330`). The reference renders headings
//!     (`#`), lists (`-`), blockquotes (`>`), fences, links and images into markdown; this port
//!     strips tags and collapses whitespace instead, so a `<h1>` arrives as its text, not as
//!     `# text`. The result still says `format: "markdown"`, which is the one place where the
//!     published shape is a promise this build does not fully keep.
//!   * **Charset decoding** (`request.ts` `charsetFrom`/`decoderFor`). The gate reads the MIME
//!     only, so a declared `utf-16`/`windows-1252` body is decoded as UTF-8 rather than
//!     converted.
//!   * **The SSRF gate** (`request.ts` `safeFetchUrl` + `resolvePublicHost`): a DNS resolution
//!     check that refuses private/loopback/link-local targets before connecting. This module
//!     ports it once, in [`ssrf`], and applies it to DOWNLOAD at every hop (the reference
//!     restates the same gate in `webdownload/download.tool.ts:1229-1354`; one implementation
//!     serves both). FETCH is the deliberate exception: its existing coverage — and the
//!     reference tool's own allow-list — run against loopback origins, so fetch keeps the
//!     format-only rule (`assertPublicUrlHostFormat`, webfetch/request.ts:373-392) and reqwest's
//!     own connect policy. A deployment that wants fetch gated should route it through
//!     [`ssrf::resolve_public_host`] the way download does.
//!   * **`redirects` for fetch**: the reference counts hops. `RequestBuilder` in this reqwest
//!     version has no per-request redirect policy, so fetch follows the default policy and
//!     refuses a response that lands on a different authority than the endpoint (`network error`
//!     sentence) instead of reporting a hop count. DOWNLOAD does count hops: it builds its own
//!     client with `Policy::none()` and follows them itself, gated each time.
//!   * **The hosted providers** (`webfetch/exa.ts`, `webfetch/linkup.ts`). `fetch` answers with
//!     `available = ["native"]`, so asking for `exa`/`linkup` gets the published refusal rather
//!     than a silently different renderer.
//!
//! `search` calls a real provider and normalizes its rows to the shared match shape, and
//! `download` streams a byte-capped transfer into the session's own downloads folder. Both state
//! their own `provider` field, so a caller is never told a richer pipeline ran than did.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::redirect::Policy;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use crate::contract::{AttachmentKind, HarnessAttachment};
use crate::payload;
use crate::tools::result::TOOL_MEDIA_BRAND;
use crate::tools::ssrf;

/// `MAX_URL_CHARS` (`webdownload/download.tool.ts:182`, and the same 2 048 in
/// `webfetch/request.ts:105` / `websearch/request.ts:76`): a URL longer than this is refused
/// before it reaches the network — never sliced, because a cut URL is a different resource.
pub const MAX_URL_CHARS: usize = 2_048;
/// `MAX_DOWNLOAD_ATTACHMENT_BYTES` (`shared/attachments.ts:88`): 20 MiB decoded.
pub const MAX_DOWNLOAD_BYTES: u64 = 20 * 1024 * 1024;
/// The provider key set, in the documented preference order
/// (`harnesses/websearch.ts:33` `WEBSEARCH_ENVIRONMENT`).
pub const SEARCH_ENVIRONMENT: [&str; 3] = ["EXA_API_KEY", "TAVILY_API_KEY", "LINKUP_API_KEY"];
/// The search row's hint when no key exists (`harnesses/websearch.ts:38`).
pub const WEBSEARCH_UNCONFIGURED_HINT: &str = "Web search is not configured on this server. Set EXA_API_KEY, TAVILY_API_KEY or LINKUP_API_KEY in packages/underload/harness/.env (or the process environment) and restart the harness server.";

/// The credentials a web capability may use, resolved ONCE when the registry is built
/// (`harnesses/websearch.ts:63-69` reads the environment at catalogue-construction time,
/// never at import, because the server loads `.env` after its imports).
#[derive(Clone, Debug, Default)]
pub struct WebKeys {
    pub exa: Option<String>,
    pub tavily: Option<String>,
    pub linkup: Option<String>,
    /// The base URL every provider endpoint is resolved against. `None` keeps the published
    /// endpoints (`api.exa.ai` and friends); a value replaces only the AUTHORITY, keeping each
    /// provider's path. The reference stubs `globalThis.fetch` for this; a Rust deployment
    /// (and this crate's tests) points the same seam at a proxy or a stub server, which is why
    /// it rides on the keys rather than on a process-global environment read at call time.
    pub endpoint_base: Option<String>,
    /// Resolution answers the **download** SSRF gate consults instead of the system resolver,
    /// keyed by host name — the mirror of the reference tool's injectable `lookup`
    /// (webdownload/download.tool.ts:1303, 1322). It is a TEST seam, because the gate has to be
    /// provable against a host name that resolves to a private address without depending on
    /// public DNS; entries are consulted before the system resolver, so leaving the map empty
    /// (production) changes nothing. Download only: `fetch` and `search` never resolve.
    pub dns_answers: BTreeMap<String, Vec<IpAddr>>,
    /// Host names reqwest must connect to an explicit address, used to point a PUBLIC host name
    /// at a loopback stub so the real HTTP leg stays testable behind the gate
    /// (`ClientBuilder::resolve_to_addrs`). Absent entries resolve normally, and like
    /// `dns_answers` this serves download only.
    pub connect_to: BTreeMap<String, Vec<SocketAddr>>,
}

impl WebKeys {
    /// Reads the three documented variables.
    #[must_use]
    pub fn from_environment() -> Self {
        let read = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };

        Self {
            exa: read("EXA_API_KEY"),
            tavily: read("TAVILY_API_KEY"),
            linkup: read("LINKUP_API_KEY"),
            endpoint_base: read("TYPECRATE_WEB_ENDPOINT_BASE"),
            // Both download seams are process-local overrides: production always runs with the
            // real resolver and no pinned addresses.
            ..Self::default()
        }
    }

    /// True when at least one search provider can be called.
    #[must_use]
    pub fn has_search_key(&self) -> bool {
        self.exa.is_some() || self.tavily.is_some() || self.linkup.is_some()
    }

    /// The key of one named provider, by the provider id used on the wire
    /// (`exa` / `tavily` / `linkup`).
    #[must_use]
    pub fn key_of(&self, provider: &str) -> Option<&String> {
        match provider {
            "exa" => self.exa.as_ref(),
            "tavily" => self.tavily.as_ref(),
            "linkup" => self.linkup.as_ref(),
            _ => None,
        }
    }
}

/// Validates one absolute http(s) URL, rejecting embedded credentials
/// (`download.tool.ts:293-306`: the tool refuses so a secret cannot leak into a log).
///
/// # Errors
///
/// The shared refusal sentence naming the reason.
pub fn validated_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();

    if trimmed.is_empty() || trimmed.len() > MAX_URL_CHARS {
        return Err("url must be a non-empty http(s) URL of at most 4096 characters.".to_owned());
    }

    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(format!(
            "url must be an absolute http(s) URL; received \"{trimmed}\"."
        ));
    }

    // `user:pass@host` is refused rather than redacted: the tool documents credentials in
    // the URL as an error, because the credential would travel to the redirect target too.
    let authority = trimmed
        .split("://")
        .nth(1)
        .unwrap_or_default()
        .split('/')
        .next()
        .unwrap_or_default();

    if authority.contains('@') {
        return Err("url must not carry credentials; pass the public URL.".to_owned());
    }

    Ok(trimmed.to_owned())
}

/// The sanitized status sentences `webfetch/request.ts:147-154` states per class.
fn status_sentence(status: u16) -> String {
    match status {
        404 => "fetch could not find the page (HTTP 404).".to_owned(),
        401 | 403 => format!("fetch was denied access to the page (HTTP {status})."),
        429 => "fetch is rate limited by the host (HTTP 429). Try again later.".to_owned(),
        other => format!("fetch failed (HTTP {other})."),
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// fetch
// ---------------------------------------------------------------------------
//
// The reference `fetch` is a five-file pipeline (`tools/webfetch/{index,request,native,
// sections}.ts`). This section ports its OBSERVABLE contract in full: the argument
// validation order, the URL and content-type gates, the byte and character budgets, the
// paging keys, the Cloudflare retry, and the section/proximity selection a `query`
// answers with. What it does NOT port is listed at the bottom of this header.
//
// WHAT IS PORTED
//   * validation: `fetchableUrl`, `readFormat`, `readProvider`, `readQuery`, `readOffset`
//     and the order they run in (`index.ts:186-271`);
//   * the request: browser UA + format-negotiated `Accept`, `accept-language`, 15 s budget,
//     1 MiB streamed body with a `content-length` pre-check, one honest-UA retry when
//     Cloudflare answers `cf-mitigated: challenge`, the binary-extension gate, the readable
//     MIME gate, and one fixed sentence per failure class (`request.ts:147-731`);
//   * the two content budgets, which are DIFFERENT limits: `MAX_CONTENT_CHARS` bounds a page
//     across its pages, and `fit_result` keeps ONE result inside the harness's model-facing
//     cap so the generic result-budget envelope never replaces the tool's own keys
//     (`index.ts:273-299`);
//   * paging: `offset`, `nextOffset`, `offsetClamped` and its `note`, `pageCharacters`
//     (`index.ts:554-673`);
//   * `query` mode: markdown heading sections, the 20-block cap, the proximity fallback for
//     a flat document, and the `matchMode` that tells the caller which of the two it got
//     (`sections.ts`, `index.ts:576-631`).
//
// WHAT IS NOT PORTED (each one is reported, not hidden)
//   * `native.ts`'s markdown converter (structural passes, `extractPreBlocks`, link/list
//     shape) and `convertHtmlToText`: this build converts HTML to readable text with
//     [`html_to_text`] and decodes entities, so heading/list markers can differ;
//   * the SSRF gate (`safeFetchUrl` + `resolvePublicHost`, `request.ts:341-410`): no address
//     resolution happens here, so a private or loopback address is fetched rather than
//     refused. `decode_entities` covers the named/numeric entities a page actually uses,
//     not the full WHATWG named-character table;
//   * `redirects`: the shared reqwest client follows redirects, and the hop count is not
//     observable through it, so the (optional) `redirects` key is never emitted;
//   * the `exa` / `linkup` fetch providers (`webfetch/{exa,linkup}.ts`): this build serves
//     the `native` provider only, so a model that names another provider gets the published
//     `fetch provider "…" is not configured. Configured: native.` refusal rather than a
//     silent downgrade.

use std::sync::OnceLock;

/// `webfetch/request.ts:52` `FETCH_TIMEOUT_MS`.
pub const FETCH_TIMEOUT_MS: u64 = 15_000;
/// `webfetch/request.ts:71` `MAX_RESPONSE_BYTES`: one mebibyte of body per page.
pub const FETCH_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// `webfetch/request.ts:75` `MAX_CONTENT_CHARS` — 240 000 characters of a page, measured on
/// the converted text.
pub const MAX_CONTENT_CHARS: usize = 240_000;
/// `webfetch/request.ts:107` `MAX_RESULT_BYTES` = `MODEL_TOOL_RESULT_BUDGET_BYTES`
/// (underload/agent/src/protocol.ts:512), restated at `:85` as `MAX_RESULT_BYTES`. It is the
/// HARNESS budget for one model-facing tool result — 32 KiB — and it is the ceiling the
/// REFUSED-result rule has to fit inside, so it is the same number
/// [`crate::tools::result::MODEL_TOOL_RESULT_BUDGET_BYTES`] names. Keeping them as one
/// published constant is what stops a fetch result growing past the point where the envelope
/// would replace the tool's own keys.
pub const FETCH_MAX_RESULT_BYTES: usize = 32 * 1024;
/// `webfetch/request.ts:118` `RESULT_OVERHEAD_BYTES`: the room a fetch result's OWN keys cost,
/// measured by `measureResultOverheadBytes` (`:122`) over a 12-key skeleton rather than guessed.
pub const FETCH_RESULT_OVERHEAD_BYTES: usize = 2_048;
/// `webfetch/request.ts:136` `RESULT_FIT_SAFETY_BYTES`: room for the escape growth the fitter
/// cannot model — it prices strings through the UTF-8 currency and a clipped string may still
/// need no escaping, but the reverse (needing MORE than measured) is what the margin covers.
pub const FETCH_RESULT_FIT_SAFETY_BYTES: usize = 64;
/// `webfetch/request.ts:141` `MAX_RESULT_CONTENT_BYTES`.
pub const FETCH_MAX_RESULT_CONTENT_BYTES: usize =
    FETCH_MAX_RESULT_BYTES - FETCH_RESULT_OVERHEAD_BYTES;
/// `webfetch/request.ts:102` `MAX_TITLE_CHARS`.
pub const FETCH_MAX_TITLE_CHARS: usize = 300;
/// `webfetch/request.ts:105` `MAX_URL_CHARS` for a fetch target (the download tool documents
/// its own, larger 4096).
pub const FETCH_MAX_URL_CHARS: usize = 2_048;
/// `webfetch/request.ts:109` `MAX_CONTENT_TYPE_CHARS`.
pub const FETCH_MAX_CONTENT_TYPE_CHARS: usize = 200;
/// `webfetch/request.ts:113` `MAX_REDIRECTS`.
pub const FETCH_MAX_REDIRECTS: usize = 5;
/// `webfetch/request.ts:117` / `:121` — the query phrase bound and the block cap that stops a
/// one-word query from smuggling the whole document back.
pub const FETCH_MAX_QUERY_CHARS: usize = 200;
pub const FETCH_MAX_QUERY_BLOCKS: usize = 20;
/// `webfetch/request.ts:127` `BROWSER_USER_AGENT`: many hosts 403 an unidentified client.
pub const FETCH_BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36";
/// `webfetch/request.ts:129` `HONEST_USER_AGENT`: tried once when Cloudflare challenges the
/// impersonation, because a bot with a fake UA and a mismatched TLS fingerprint is exactly
/// what `cf-mitigated: challenge` answers.
pub const FETCH_HONEST_USER_AGENT: &str = "underload-harness";
/// `webfetch/sections.ts:52` `FLAT_DOCUMENT_SECTIONS`: at or below this many sections a
/// document has no structure to filter, so `query` falls back to a proximity window.
pub const FETCH_FLAT_DOCUMENT_SECTIONS: usize = 2;
/// `webfetch/sections.ts:56` `PROXIMITY_WINDOW_CHARS`.
pub const FETCH_PROXIMITY_WINDOW_CHARS: usize = 6_000;
/// `webfetch/sections.ts:219` `PROXIMITY_LINE_SNAP_CHARS`: how far a proximity window's edge may
/// walk to reach a line boundary. The window's LENGTH is a bound, not an intention — the snap that
/// rounds the edges may add AT MOST `2 * PROXIMITY_LINE_SNAP_CHARS` to `window_chars` (F3). Both
/// edges used to snap without limit, and on a newline-free page the close edge fell through to the
/// end of the document, which turned the "window" back into the whole rest of the page — the exact
/// cost this mode exists to avoid.
pub const FETCH_PROXIMITY_LINE_SNAP_CHARS: usize = 200;
/// `webfetch/index.ts:511` `PROXIMITY_NOTE_CUT` — the sentence a proximity answer carries when the
/// budget cut the window and a `nextOffset` exists to resume from. It names the argument that has
/// to GO AWAY, because `offset` is ignored while `query` is set: the unconditional sentence this
/// replaced told every headingless caller to "use offset", which the tool cannot honour and the
/// model paid a turn discovering.
pub const PROXIMITY_NOTE_CUT: &str = "This page has no heading structure, so \"query\" returned a text window around the first match instead of a section; the window was cut by the result size limit. Re-fetch WITHOUT \"query\" and pass the reported \"nextOffset\" as \"offset\" to read on — \"offset\" does nothing while \"query\" is set.";
/// `webfetch/index.ts:514` `PROXIMITY_NOTE_WHOLE` — the same answer when NOTHING was cut: the
/// remedy above does not exist, so the sentence says so and offers the two that do.
pub const PROXIMITY_NOTE_WHOLE: &str = "This page has no heading structure, so \"query\" returned a text window around the first match instead of a section. No \"nextOffset\" is reported, so there is nothing to resume: to read other parts of the page, ask a different \"query\" phrase, or re-fetch without \"query\" and page with \"offset\".";
/// `webfetch/request.ts:483` `TEXT_IMAGE_MIME` — the one image that IS text.
const FETCH_TEXT_IMAGE_MIME: &str = "image/svg+xml";
/// `webfetch/request.ts:475` `IMAGE_MIMES`.
const FETCH_IMAGE_MIMES: &str = r"(?i)^(image|audio|video)/";
/// `webfetch/request.ts:217-232` `BINARY_EXTENSIONS`: extensions whose bytes are never
/// readable as text. Deliberately absent: `.md .txt .json .jsonl .yaml .yml .xml .csv .tsv
/// .html .htm .svg .tex .rtf` — all text.
const FETCH_BINARY_EXTENSIONS: &[&str] = &[
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".apng", ".bmp", ".dib", ".tif", ".tiff",
    ".ico", ".cur", ".heic", ".heif", ".psd", ".xcf", ".mp4", ".m4v", ".mov", ".avi", ".mkv",
    ".webm", ".mpg", ".mpeg", ".wmv", ".flv", ".3gp", ".mp3", ".m4a", ".aac", ".ogg", ".oga",
    ".opus", ".wav", ".flac", ".aiff", ".mid", ".midi", ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".odt", ".ods", ".odp", ".epub", ".zip", ".gz", ".tgz", ".bz2", ".xz", ".zst",
    ".7z", ".rar", ".tar", ".cab", ".iso", ".dmg", ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".exe", ".dll", ".so", ".dylib", ".bin", ".dat", ".class", ".jar", ".war", ".pyc", ".o", ".a",
    ".wasm", ".elf", ".msi", ".apk", ".deb", ".rpm",
];

/// `webfetch/request.ts:134` `acceptHeader`: format negotiation, so a host that can serve the
/// requested form skips HTML entirely.
#[must_use]
pub fn accept_header(format: &str) -> &'static str {
    match format {
        "text" => "text/plain;q=1.0, text/markdown;q=0.9, text/html;q=0.8, */*;q=0.1",
        "html" => {
            "text/html;q=1.0, application/xhtml+xml;q=0.9, text/plain;q=0.8, text/markdown;q=0.7, */*;q=0.1"
        }
        _ => {
            "text/markdown;q=1.0, text/x-markdown;q=0.9, text/plain;q=0.8, text/html;q=0.7, */*;q=0.1"
        }
    }
}

/// `webfetch/sections.ts:58-59`: the fence toggles and the ATX heading form.
static SECTION_FENCE: OnceLock<regex::Regex> = OnceLock::new();
static SECTION_HEADING: OnceLock<regex::Regex> = OnceLock::new();
/// `native.ts:179` `extractTitle`'s pattern.
static TITLE_TAG: OnceLock<regex::Regex> = OnceLock::new();
/// Any tag, stripped from a title's inner HTML (`native.ts:182`).
static ANY_TAG: OnceLock<regex::Regex> = OnceLock::new();
/// A numeric or named HTML entity (`native.ts`'s `decodeEntities`).
static ENTITY: OnceLock<regex::Regex> = OnceLock::new();

/// One section of converted content: `[start, end)` lines under `heading`
/// (`webfetch/sections.ts:52` `Section`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Section {
    heading: String,
    start: usize,
    end: usize,
}

/// One selected block (`webfetch/sections.ts` `SectionMatch`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SectionMatch {
    pub heading: String,
    pub text: String,
    pub line: usize,
}

/// `webfetch/sections.ts:63` `splitSections`: split converted content at markdown headings.
/// Content before the first heading is its own section, a fenced block is opaque (a `#` in a
/// code sample is not a heading), and an empty section is dropped.
#[must_use]
fn split_sections(content: &str) -> Vec<Section> {
    let fence = pattern(&SECTION_FENCE, r"^\s*(?:```|~~~|`{4,}|~{4,})");
    let heading = pattern(&SECTION_HEADING, r"^(#{1,6})\s+(.*\S)\s*$");
    let lines: Vec<&str> = content.split('\n').collect();
    let mut sections: Vec<Section> = Vec::new();
    let mut current = String::new();
    let mut start = 0_usize;
    let mut in_fence = false;

    for (index, line) in lines.iter().enumerate() {
        if fence.is_match(line) {
            in_fence = !in_fence;
        }

        if in_fence {
            continue;
        }

        let Some(captures) = heading.captures(line) else {
            continue;
        };

        if index > start || sections.is_empty() {
            sections.push(Section {
                heading: current.clone(),
                start,
                end: index,
            });
        }

        current = captures
            .get(2)
            .map_or_else(String::new, |matched| matched.as_str().to_owned());
        start = index;
    }

    sections.push(Section {
        heading: current,
        start,
        end: lines.len(),
    });

    sections.retain(|section| section.end > section.start);
    sections
}

/// One selection answer, in the shape both selectors return
/// (`webfetch/sections.ts:99` / `:142`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selection {
    pub matches: Vec<SectionMatch>,
    pub total_sections: usize,
    pub total_matches: usize,
    /// `heading` when sections were filtered, `proximity` when the caller got a text window.
    pub match_mode: &'static str,
}

/// `webfetch/sections.ts:99` `selectSections`. Matching is on the WHOLE section text, so a hit
/// anywhere in a block returns that block with its heading; `max_blocks` bounds the answer.
#[must_use]
pub fn select_sections(content: &str, query: &str, max_blocks: usize) -> Selection {
    let sections = split_sections(content);
    let needle = query.to_lowercase();
    let lines: Vec<&str> = content.split('\n').collect();
    let mut matches: Vec<SectionMatch> = Vec::new();
    let mut total_matches = 0_usize;

    for section in &sections {
        let text = lines[section.start..section.end].join("\n");

        if !section.heading.to_lowercase().contains(&needle)
            && !text.to_lowercase().contains(&needle)
        {
            continue;
        }

        total_matches += 1;

        if matches.len() < max_blocks {
            matches.push(SectionMatch {
                heading: section.heading.clone(),
                text: text.trim().to_owned(),
                line: section.start,
            });
        }
    }

    Selection {
        matches,
        total_sections: sections.len(),
        total_matches,
        match_mode: "heading",
    }
}

/// `webfetch/sections.ts:142` `selectProximity`: the fallback for a document with no heading
/// structure — one window centred on the first occurrence, snapped to line boundaries so it
/// never opens or closes mid-word, labelled with the nearest preceding heading.
#[must_use]
pub fn select_proximity(content: &str, query: &str) -> Selection {
    select_proximity_with_window(content, query, FETCH_PROXIMITY_WINDOW_CHARS)
}

/// The window size is a parameter because the reference exposes it as one
/// (`selectProximity(content, query, windowChars = PROXIMITY_WINDOW_CHARS)`).
#[must_use]
pub fn select_proximity_with_window(content: &str, query: &str, window_chars: usize) -> Selection {
    let sections = split_sections(content);
    let needle: Vec<char> = query.to_lowercase().chars().collect();
    let haystack: Vec<char> = content.chars().collect();
    let lowered: Vec<char> = haystack
        .iter()
        .map(|character| character.to_lowercase().next().unwrap_or(*character))
        .collect();

    let find = |scan_from: usize| -> Option<usize> {
        (scan_from + needle.len()..=lowered.len())
            .find(|end| lowered[end - needle.len()..*end] == needle[..])
    };

    let Some(at) = (needle.len() <= lowered.len()).then(|| find(0)).flatten() else {
        return Selection {
            matches: Vec::new(),
            total_sections: sections.len(),
            total_matches: 0,
            match_mode: "proximity",
        };
    };

    // Count every occurrence, so `totalMatches` means "how many hits exist" in both modes
    // rather than silently changing units.
    let mut total_matches = 0_usize;
    let mut scan = at;

    while scan != usize::MAX {
        total_matches += 1;
        scan = find(scan + needle.len()).unwrap_or(usize::MAX);
    }

    // A code-unit window in the reference; a character window here, which is the same thing
    // for the BMP text these pages are made of.
    let start = (at as i64 - (i64::try_from(window_chars / 2).unwrap_or(i64::MAX))).max(0) as usize;
    let end = (start + window_chars).min(haystack.len());

    // Snap to line boundaries so a window never opens or closes mid-word — but ONLY where the
    // boundary is CLOSE (`sections.ts:244-262`, F3). An unbounded snap is the defect this bounds:
    // the open edge used to walk back to the start of its line however long that line was, and the
    // close edge fell through to the end of the document whenever no newline followed `end`, which
    // on a newline-free page (one giant line: minified HTML, a pasted log, a CSV with the CRLF
    // converted away) made the "window" the whole rest of the document.
    //
    // Bounded on BOTH edges, the window can never exceed `window_chars + 2 * snap`, and the match
    // stays inside it: the open edge only moves back from `start`, the close edge only forward from
    // `end`, and both stop at the bound.
    let from = haystack[..start]
        .iter()
        .rposition(|character| *character == '\n')
        .filter(|line| start - line <= FETCH_PROXIMITY_LINE_SNAP_CHARS)
        .map_or(start, |line| line + 1);
    let to = haystack[end..]
        .iter()
        .position(|character| *character == '\n')
        .filter(|line| *line <= FETCH_PROXIMITY_LINE_SNAP_CHARS)
        .map_or(end, |line| end + line);

    let line = haystack[..at]
        .iter()
        .filter(|character| **character == '\n')
        .count();

    // Nearest preceding heading, if the document has any.
    let mut heading = String::new();

    for section in &sections {
        if section.start <= line && !section.heading.is_empty() {
            heading.clone_from(&section.heading);
        }
    }

    let text: String = haystack[from..to].iter().collect();

    Selection {
        matches: vec![SectionMatch {
            heading,
            text: text.trim().to_owned(),
            line,
        }],
        total_sections: sections.len(),
        total_matches,
        match_mode: "proximity",
    }
}

/// The one place the fetch fit learns what a delivered document costs on the wire: the SAME
/// currency [`crate::payload`] defines for the harness's model-facing cap, reached through the
/// crate's own helper rather than a restatement. The reference keeps a private copy of this
/// measure (`webfetch/index.ts:346` `escapedBytes` + `jsonTextBytes`) and its colocated test
/// pins it against `core/modelPayloadBytes` precisely because a silent drift between the two
/// is the F1 defect — a fit that believes it fits.
///
/// `document_wire_bytes` prices a whole JSON document, quotes and braces included, counting
/// every escape the serializer has to write; that is exactly "the raw JSON text of the
/// serialized result", which is what the envelope in [`fit_result`] has to be measured in.
#[must_use]
fn json_text_bytes(value: &Value) -> usize {
    payload::document_wire_bytes(value)
}

/// Measure (2) of the reference (`webfetch/index.ts:417` `resultStringBytes`): what a string
/// costs when it sits INSIDE the serialized document, i.e. escaped once and then escaped again
/// by the enclosing string. The tables differ from [`payload::model_payload_bytes`] by exactly
/// the extra backslash layer, and both are needed: the content pays the nested currency, the
/// rest of the object pays the raw one, and the two are disjoint by construction.
///
/// A lone surrogate cannot exist in a Rust `String`, so the arms that price one
/// (`webfetch/index.ts:394-403`) have no input here; every other code point costs its UTF-8
/// width, which is the 4 the reference charges a well-formed surrogate pair.
#[must_use]
fn result_string_bytes(value: &str) -> usize {
    value.chars().map(result_char_bytes).sum()
}

/// One character in the nested currency (`webfetch/index.ts:346-406`, `nested: true`).
#[must_use]
fn result_char_bytes(character: char) -> usize {
    match character {
        // Both characters of the `\"` / `\\` pair get escaped again by the enclosing string,
        // so 2 + 2.
        '"' | '\\' => 4,
        // The five controls with a one-character escape: the backslash (2 once escaped) plus
        // the letter (1).
        '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 3,
        // `\u00XX` is six characters of JSON text, only the backslash of which the second
        // escaping charges again.
        other if u32::from(other) < 0x20 => 7,
        // Everything else is written literally by BOTH escaping passes.
        other => other.len_utf8(),
    }
}

/// The heavier of two sentences in the currency the fit spends (`index.ts:522-525`). Used for
/// the skeleton of a proximity answer, where WHICH sentence ships depends on a `nextOffset` the
/// fit itself decides — so the skeleton has to cost the worse case, and "worse" is measured,
/// not guessed from character counts, because a sentence made of quotation marks costs more
/// than its length.
#[must_use]
fn heavier<'a>(first: &'a str, second: &'a str) -> &'a str {
    if result_string_bytes(first) >= result_string_bytes(second) {
        first
    } else {
        second
    }
}

/// Where a cut `query` answer should be resumed from — `index.ts:558` `resumeOffsetForCut`, ported
/// block by block.
///
/// `fit_result` cuts the JOINED answer, and a caller can only page the PAGE, so the cut position
/// has to be translated back into an offset in the converted document. Every block
/// `select_sections`/`select_proximity` hands back is a verbatim substring of `content` (both
/// trim, neither rewrites), so locating it from its own starting line gives an exact anchor: the
/// offset returned here is the character immediately AFTER the last character the answer
/// delivered, which is what makes the resume lossless — nothing is skipped, and at most the
/// synthetic `[line N]` header can overlap with what was already shown.
///
/// Returns `None` when no block contains the cut (the answer was not cut) or when a block cannot be
/// found in the page at all — defensively, a caller then gets no `nextOffset` rather than a wrong
/// one. Every figure is in CODE UNITS, the unit `offset`, `nextOffset` and `pageCharacters` are all
/// reported in; bytes are only the intermediate walk.
#[must_use]
fn resume_offset_for_cut(content: &str, matches: &[SectionMatch], kept: usize) -> Option<usize> {
    // Byte offsets of the page's line starts, so a block can be searched for from where it came
    // from rather than from the top of the document (`lineStartOffset`'s job, in bytes).
    let line_starts = |line: usize| -> usize {
        let mut byte = 0_usize;

        for _ in 0..line {
            let Some(next) = content[byte..].find('\n') else {
                return content.len();
            };

            byte += next + 1;
        }

        byte
    };

    // Code units before a byte offset: the unit figure the caller pages with.
    let units_before = |byte: usize| -> usize {
        content
            .get(..byte)
            .unwrap_or(content)
            .chars()
            .map(char::len_utf16)
            .sum()
    };

    let mut cursor = 0_usize;

    for matched in matches {
        // The header the answer actually shows, and therefore the offset within the JOINED answer
        // where this block's text begins (`sectionHeader` + `SECTION_SEPARATOR`, `index.ts:496-499`
        // — the layout has to be reproduced exactly or `nextOffset` points at the wrong character).
        let header = section_header(matched);
        let text_start = cursor + code_units(&header);
        let block_end = text_start + code_units(&matched.text);

        if kept < block_end {
            // The block came from `matched.line`, so search from there; the whole document is the
            // fallback for a block that trimmed away its own line start.
            let hint = line_starts(matched.line);
            let found = content
                .get(hint..)
                .and_then(|rest| rest.find(&matched.text))
                .map(|at| hint + at)
                .or_else(|| content.find(&matched.text))?;

            // A cut that lands in the header or the separator resumes at the block's first real
            // character, and the answer can never point past the end of the page.
            let resume_units = if kept <= text_start {
                units_before(found)
            } else {
                units_before(found) + (kept - text_start)
            };

            return Some(resume_units.min(code_units(content)));
        }

        cursor = block_end + code_units(SECTION_SEPARATOR);
    }

    None
}

/// How a `query` answer is assembled: the label each returned section carries and the blank line
/// between blocks (`index.ts:494-499`). Named because the resume-offset mapping in
/// [`resume_offset_for_cut`] has to reproduce this layout EXACTLY to turn a cut position back into
/// a page offset — if the two drift, `nextOffset` silently points at the wrong character.
pub const SECTION_SEPARATOR: &str = "\n\n";

/// The one-line label a selected section is shown under (`index.ts:497` `sectionHeader`). A
/// heading-less section gets a literal placeholder so the block stays self-describing.
#[must_use]
fn section_header(matched: &SectionMatch) -> String {
    format!(
        "## {} [line {}]\n",
        if matched.heading.is_empty() {
            "(no heading)"
        } else {
            &matched.heading
        },
        matched.line + 1
    )
}

/// `webfetch/index.ts:451` `fitResult`: shrink one content string so the WHOLE result object fits
/// the harness's model-facing cap.
///
/// The harness WOULD bound an oversized result itself, but for a JSON DOCUMENT it does not bound
/// it at all — it refuses the whole value and the delivery site reports the call as FAILED, which
/// drops the url, title, contentType and nextOffset a model needs in order to resume. Doing the
/// cutting here, in the cap's own currency, is what keeps the tool's shape AND keeps the call
/// successful.
///
/// `frame` is the result object as it will be serialized; `content` is added as the empty string.
/// The overhead is MEASURED from it rather than assumed, because the fields around `content` are
/// not fixed: `url` may be 2,048 characters, `title` 300, `contentType` 200, and a clamp or
/// proximity `note` may ride along. `FETCH_RESULT_OVERHEAD_BYTES` still decides where the shrink
/// STARTS (a short-url, no-title result delivers the whole content ceiling), but it can no longer
/// decide whether the result DELIVERS.
#[must_use]
pub fn fit_result(content: &str, frame: &Value) -> String {
    // The envelope, priced in the cap's currency: every key, every value, the braces, AND the 4
    // bytes that delimit `content` — because the skeleton carries `content: ""` and each of those
    // two quotes serializes as `\"`. Measuring them here is what makes the budget below honest;
    // subtracting them a second time is the defect that made every budget 4 bytes too wide
    // (`index.ts:437` `CONTENT_QUOTES_BYTES`).
    let mut skeleton = frame.clone();

    if let Value::Object(map) = &mut skeleton {
        map.insert("content".to_owned(), json!(""));
    }

    let envelope = json_text_bytes(&skeleton);

    // Two limits decide the content budget, and the SMALLER one binds: the per-result content
    // ceiling, and whatever room the envelope leaves under the harness cap. `FETCH_RESULT_FIT_SAFETY_BYTES`
    // is the room for escape growth this function cannot model.
    let budget = FETCH_MAX_RESULT_CONTENT_BYTES.min(
        FETCH_MAX_RESULT_BYTES
            .saturating_sub(envelope)
            .saturating_sub(FETCH_RESULT_FIT_SAFETY_BYTES),
    );

    if result_string_bytes(content) <= budget {
        return content.to_owned();
    }

    // A code-unit slice is an UPPER BOUND on a byte-fitting slice (one code unit is never fewer
    // than one byte in either currency), then step back 5% at a time until the escaped form fits.
    // Bounded by the budget itself, so this terminates.
    //
    // The reference trims a trailing lone high half inside the loop; a Rust `String` cannot hold a
    // surrogate half at all, and `slice_units` cuts on character boundaries, so no candidate can
    // end on half of a pair and the repair has nothing to do here.
    let mut keep = code_units(content).min(budget);
    let mut candidate = slice_units(content, keep).to_owned();

    while keep > 0 && result_string_bytes(&candidate) > budget {
        // `Math.max(1, Math.floor(keep * 0.05))`: five percent, never zero, so the loop always
        // moves.
        keep = keep.saturating_sub((keep / 20).max(1));
        candidate = slice_units(content, keep).to_owned();
    }

    candidate
}

/// `webfetch/native.ts:179` `extractTitle`: read the document title BEFORE any tag stripping
/// destroys it, drop nested tags, decode entities, collapse whitespace.
#[must_use]
pub fn extract_title(html: &str) -> Option<String> {
    let title = pattern(&TITLE_TAG, r"(?is)<title\b[^>]*>(.*?)</title>")
        .captures(html)
        .and_then(|captures| captures.get(1))
        .map(|matched| matched.as_str().to_owned())?;

    let cleaned = decode_entities(&pattern(&ANY_TAG, r"(?s)<[^>]*>").replace_all(&title, ""));
    let cleaned = pattern(&WHITESPACE_RUN, r"\s+")
        .replace_all(cleaned.trim(), " ")
        .to_string();

    (!cleaned.is_empty()).then_some(cleaned)
}

/// `native.ts:39-64` `NAMED_ENTITIES`, entry for entry. The four space entities all decode to
/// a plain U+0020 (NOT a no-break space), which is what lets the surrounding whitespace
/// collapse treat them like ordinary spaces afterwards.
const NAMED_ENTITIES: [(&str, char); 26] = [
    ("amp", '&'),
    ("lt", '<'),
    ("gt", '>'),
    ("quot", '"'),
    ("apos", '\''),
    ("nbsp", ' '),
    ("ensp", ' '),
    ("emsp", ' '),
    ("thinsp", ' '),
    ("copy", '©'),
    ("reg", '®'),
    ("trade", '™'),
    ("hellip", '…'),
    ("mdash", '—'),
    ("ndash", '–'),
    ("lsquo", '\u{2018}'),
    ("rsquo", '\u{2019}'),
    ("ldquo", '\u{201c}'),
    ("rdquo", '\u{201d}'),
    ("bull", '•'),
    ("middot", '·'),
    ("deg", '°'),
    ("laquo", '«'),
    ("raquo", '»'),
    ("times", '×'),
    ("divide", '÷'),
];

/// `webfetch/native.ts`'s `decodeEntities`, reduced to what pages actually emit: numeric
/// references and the six named entities that appear in prose.
#[must_use]
pub fn decode_entities(text: &str) -> String {
    // The semicolon is OPTIONAL in every alternative, because that is what the reference's
    // `/&#x([0-9a-fA-F]+);?/g` does: `&#41` decodes and `&nbsp` decodes too.
    let entity = pattern(&ENTITY, r"(?i)&#x([0-9a-f]+);?|&#(\d+);?|&([a-z]+);?");

    entity
        .replace_all(text, |captures: &regex::Captures<'_>| {
            let decoded = if let Some(hex) = captures.get(1) {
                u32::from_str_radix(hex.as_str(), 16).ok()
            } else if let Some(decimal) = captures.get(2) {
                decimal.as_str().parse().ok()
            } else {
                NAMED_ENTITIES
                    .iter()
                    .find(|(name, _)| {
                        name.eq_ignore_ascii_case(captures.get(3).map_or("", |n| n.as_str()))
                    })
                    .map(|(_, character)| *character as u32)
            };

            match decoded.and_then(|code| {
                // `native.ts:70-79`: codes below 32 collapse to NOTHING rather than emitting
                // an unprintable character. A lone surrogate is the one unavoidable
                // divergence: JS keeps U+D800 in the string (and JSON renders it as U+FFFD
                // downstream), while a Rust `char` cannot hold a surrogate at all, so U+FFFD
                // is the closest renderable equivalent.
                if !(32..=0x10_ffff).contains(&code) {
                    Some(String::new())
                } else if (0xd800..=0xdfff).contains(&code) {
                    Some('\u{fffd}'.to_string())
                } else {
                    char::from_u32(code).map(|character| character.to_string())
                }
            }) {
                Some(value) => value,
                // An unparseable or unknown reference is left exactly as the page wrote it,
                // which is what a browser does with `&#x110000;` and `&nope;`.
                None => captures
                    .get(0)
                    .map_or_else(String::new, |matched| matched.as_str().to_owned()),
            }
        })
        .to_string()
}

/// `webfetch/request.ts:237` `binaryExtension`: the extension of the last path segment.
#[must_use]
pub fn binary_extension(url: &str) -> Option<String> {
    let path = reqwest::Url::parse(url).ok()?.path().to_owned();
    let last = path.rsplit('/').next().unwrap_or_default().to_owned();

    if last.is_empty() {
        return None;
    }

    let dot = last.rfind('.')?;
    let extension = last[dot..].to_lowercase();

    FETCH_BINARY_EXTENSIONS
        .contains(&extension.as_str())
        .then_some(extension)
}

/// `webfetch/request.ts:505` `mimeFrom`.
#[must_use]
pub fn mime_from(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_lowercase()
}

/// `webfetch/request.ts:485` `readableMime`: `fetch` is the STRUCTURED CONTENT path. An
/// image, video, audio, PDF or octet-stream body belongs to `download`.
#[must_use]
pub fn readable_mime(mime: &str) -> bool {
    if mime.is_empty() || mime == FETCH_TEXT_IMAGE_MIME {
        return true;
    }

    if mime == "application/pdf" || mime.starts_with("application/octet-stream") {
        return false;
    }

    if pattern(&IMAGE_MIME_GATE, FETCH_IMAGE_MIMES).is_match(mime) {
        return false;
    }

    mime.starts_with("text/")
        || mime == "application/json"
        || mime.ends_with("+json")
        || mime == "application/xml"
        || mime.ends_with("+xml")
        || mime == "application/xhtml+xml"
        || mime == "application/javascript"
        || mime == "application/x-javascript"
        || mime == "application/typescript"
}

/// The gate behind [`readable_mime`]'s image/audio/video branch.
static IMAGE_MIME_GATE: OnceLock<regex::Regex> = OnceLock::new();

/// `webfetch/request.ts:511` `fetchableUrl`: absolute http(s), no userinfo, bounded length,
/// returned NORMALIZED (the form the request actually uses).
#[must_use]
pub fn fetchable_url(value: Option<&Value>) -> Option<String> {
    let trimmed = value?.as_str()?.trim();

    if trimmed.is_empty() || code_units(trimmed) > FETCH_MAX_URL_CHARS {
        return None;
    }

    let parsed = reqwest::Url::parse(trimmed).ok()?;

    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return None;
    }

    let normalized = parsed.to_string();

    (code_units(&normalized) <= FETCH_MAX_URL_CHARS).then_some(normalized)
}

/// One page, as `webfetch/request.ts:533` `PageFetch` reports it (minus the hop count this
/// build cannot observe).
#[derive(Clone, Debug, PartialEq, Eq)]
struct PageFetch {
    url: String,
    content_type: String,
    body: String,
}

/// `webfetch/request.ts:564` `requestPage`, native path.
///
/// # Errors
///
/// One fixed sentence per failure class; page bodies are deliberately never surfaced.
async fn request_page(
    client: &reqwest::Client,
    url: &str,
    format: &str,
    signal: &CancellationToken,
) -> Result<PageFetch, String> {
    // A page whose extension is a container is a FILE, and refusing it costs neither a 15 s
    // transfer nor a 1 MiB read.
    if let Some(extension) = binary_extension(url) {
        return Err(format!(
            "fetch does not read {extension} files: use the `download` tool for binary content."
        ));
    }

    // `async move` so the returned future owns the UA it stamps: the retry runs the same
    // closure twice, and a borrowed `user_agent` would not outlive the closure body.
    let send = move |user_agent: &'static str| async move {
        let request = client
            .get(url)
            .header("user-agent", user_agent)
            .header("accept", accept_header(format))
            .header("accept-language", "en-US,en;q=0.9")
            .send();

        tokio::select! {
            biased;
            () = signal.cancelled() => Err(CANCELLED_SENTENCE.to_owned()),
            outcome = tokio::time::timeout(
                std::time::Duration::from_millis(FETCH_TIMEOUT_MS),
                request,
            ) => match outcome {
                Err(_) => Err(format!("fetch timed out after {FETCH_TIMEOUT_MS} ms.")),
                Ok(Ok(response)) => Ok(response),
                // fetch rejects with an AbortError for both a signal and some transports, so
                // an unreachable host and a refused connection share one sentence.
                Ok(Err(_)) => Err("fetch could not be reached (network error).".to_owned()),
            },
        }
    };

    let transfer = async {
        let mut response = send(FETCH_BROWSER_USER_AGENT).await?;

        if response.status().as_u16() == 403
            && response
                .headers()
                .get("cf-mitigated")
                .and_then(|value| value.to_str().ok())
                == Some("challenge")
        {
            // A bot with a fake UA and a mismatched TLS fingerprint is exactly what
            // `cf-mitigated: challenge` answers, so the SAME request is tried once with an
            // honest agent.
            response = send(FETCH_HONEST_USER_AGENT).await?;
        }

        if !response.status().is_success() {
            return Err(status_sentence(response.status().as_u16()));
        }

        let declared = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<usize>().ok());

        if declared.is_some_and(|bytes| bytes > FETCH_MAX_RESPONSE_BYTES) {
            return Err(oversized_response_sentence());
        }

        let content_type: String = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .chars()
            .take(FETCH_MAX_CONTENT_TYPE_CHARS)
            .collect();
        let mime = mime_from(&content_type);

        if !readable_mime(&mime) {
            return Err(format!(
                "fetch does not support this content type ({}): use the `download` tool for binary content.",
                if mime.is_empty() { "unknown" } else { &mime }
            ));
        }

        let mut received: Vec<u8> = Vec::new();
        let mut streamed = response;

        while let Some(chunk) = streamed
            .chunk()
            .await
            .map_err(|_| "fetch could not be reached (network error).".to_owned())?
        {
            received.extend_from_slice(&chunk);

            if received.len() > FETCH_MAX_RESPONSE_BYTES {
                return Err(oversized_response_sentence());
            }
        }

        let final_url = streamed.url().to_string();

        Ok(PageFetch {
            url: final_url,
            content_type,
            // A declared `charset` is not decoded here (see the module header): a non-UTF-8
            // page arrives with U+FFFD where the bytes were not valid UTF-8.
            body: String::from_utf8_lossy(&received).into_owned(),
        })
    };

    tokio::select! {
        biased;
        () = signal.cancelled() => Err(CANCELLED_SENTENCE.to_owned()),
        outcome = transfer => outcome,
    }
}

/// `webfetch/request.ts:165` `oversizedResponse`: the cap is a deliberate design, so the
/// sentence names the remedy — a page that will not fit is a file, not an article.
fn oversized_response_sentence() -> String {
    format!(
        "fetch received a response larger than {FETCH_MAX_RESPONSE_BYTES} bytes. Try the plain-text or raw source form of the page if the host offers one (for example .txt, or a \"?action=raw\" / print view), ask for fewer characters with \"query\", or use the `download` tool to save the bytes and read them with grep and read."
    )
}

/// The harness-level cancellation sentence this crate already reports from every tool.
const CANCELLED_SENTENCE: &str = "the request was cancelled.";

/// One validated fetch call (`webfetch/index.ts:175` `Invocation`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchInvocation {
    pub url: String,
    pub format: &'static str,
    pub provider: &'static str,
    pub query: Option<String>,
    pub offset: usize,
}

/// `webfetch/index.ts:186` `readFormat`.
fn read_format(input: Option<&Value>) -> Result<&'static str, String> {
    match input {
        None => Ok("markdown"),
        Some(value) if value.as_str() == Some("markdown") => Ok("markdown"),
        Some(value) if value.as_str() == Some("text") => Ok("text"),
        Some(value) if value.as_str() == Some("html") => Ok("html"),
        Some(_) => Err("fetch format must be \"markdown\", \"text\", or \"html\".".to_owned()),
    }
}

/// `webfetch/index.ts:201` `readProvider`: omitted means `native` no matter which keys exist,
/// because a hosted crawler costs credits and is never a silent upgrade.
fn read_fetch_provider(
    input: Option<&Value>,
    available: &[&'static str],
) -> Result<&'static str, String> {
    match input {
        None => Ok("native"),
        Some(value) => available
            .iter()
            .find(|provider| value.as_str() == Some(**provider))
            .copied()
            .ok_or_else(|| {
                format!(
                    "fetch provider \"{}\" is not configured. Configured: {}.",
                    display_value(value),
                    available.join(", ")
                )
            }),
    }
}

/// `webfetch/index.ts:218` `readQuery`: a plain phrase, matched case-insensitively and
/// deliberately NOT a regex, because the argument is something a model types from memory.
fn read_query(input: Option<&Value>) -> Result<Option<String>, String> {
    let Some(value) = input else {
        return Ok(None);
    };

    let Some(text) = value.as_str() else {
        return Err("fetch query must be a string.".to_owned());
    };

    let trimmed = text.trim();

    if trimmed.is_empty() {
        return Ok(None);
    }

    if code_units(trimmed) > FETCH_MAX_QUERY_CHARS {
        return Err(format!(
            "fetch query must be at most {FETCH_MAX_QUERY_CHARS} characters."
        ));
    }

    Ok(Some(trimmed.to_owned()))
}

/// `webfetch/index.ts:240` `readOffset`: clamped later rather than rejected now, but a
/// negative or non-finite offset means the model has lost track and is refused.
fn read_offset(input: Option<&Value>) -> Result<usize, String> {
    let Some(value) = input else {
        return Ok(0);
    };

    let Some(number) = value.as_f64() else {
        return Err("fetch offset must be a non-negative number.".to_owned());
    };

    if !number.is_finite() || number < 0.0 {
        return Err("fetch offset must be a non-negative number.".to_owned());
    }

    Ok(number.floor() as usize)
}

/// `webfetch/index.ts:250` `readInvocation`.
fn read_fetch_invocation(
    arguments: &Value,
    available: &[&'static str],
) -> Result<FetchInvocation, String> {
    // Direct callers may pass the bare URL (`fetch('https://example.com')`).
    let input = match arguments {
        Value::String(text) => json!({ "url": text }),
        other => other.clone(),
    };

    let url = fetchable_url(input.get("url"))
        .ok_or_else(|| "fetch requires an absolute http(s) URL without credentials.".to_owned())?;

    Ok(FetchInvocation {
        format: read_format(input.get("format"))?,
        provider: read_fetch_provider(input.get("provider"), available)?,
        query: read_query(input.get("query"))?,
        offset: read_offset(input.get("offset"))?,
        url,
    })
}

/// `fetch`: `{url, format?, provider?, query?, offset?}` -> the `WebFetchResult` document
/// (`webfetch/index.ts:487-684`), native provider.
///
/// # Errors
///
/// A validation refusal, the URL/MIME/size refusal for a target `fetch` does not serve, or a
/// transport sentence.
pub(crate) async fn fetch(
    client: &reqwest::Client,
    arguments: &Value,
    keys: &WebKeys,
    signal: &CancellationToken,
) -> Result<Value, String> {
    // `native` first, then the hosted providers whose key resolved — and this build serves
    // `native` alone, which is the list the refusal sentence quotes.
    let available: Vec<&'static str> = vec!["native"];
    let invocation = read_fetch_invocation(arguments, &available)?;

    // Redact every configured key before bounding: a page built from the account, or a
    // provider error echoing the request, could repeat one of them.
    let redactor = Redactor::new(
        ["exa", "linkup"]
            .into_iter()
            .filter_map(|provider| keys.key_of(provider))
            .cloned(),
    );

    let page = request_page(client, &invocation.url, invocation.format, signal).await?;

    // The content type decides the conversion, not the requested format: a markdown request
    // against a JSON endpoint returns the JSON (`native.ts:333-347`).
    let is_html = page.content_type.contains("text/html")
        || page.content_type.contains("application/xhtml+xml");
    let title = if is_html {
        extract_title(&page.body)
    } else {
        None
    };
    let converted = match (is_html, invocation.format) {
        (true, "html") => page.body.clone(),
        (true, _) => decode_entities(&html_to_text(&page.body)),
        (false, _) => page.body.clone(),
    };

    // Redact FIRST, bound second: bounding first can cut a credential in half and leave a
    // secret prefix at the cut.
    let safe_content = redactor.redact(&converted);
    let page_characters = code_units(&safe_content);

    // The window the caller asked for. An offset past the end is a model reaching slightly
    // too far, so it is CLAMPED and the clamp is REPORTED: an empty string with
    // `truncated: false` used to be indistinguishable from "this page has no content".
    let start_offset = invocation.offset.min(page_characters);
    let offset_clamped = start_offset != invocation.offset;
    let window = slice_units_from(&safe_content, start_offset);

    // Optional section selection (fetch + grep). No match is reported as NO match: it never
    // silently degrades into the whole page, because a model that believed it had filtered and
    // received the full text would pay the context cost and not know it.
    if let Some(query) = &invocation.query {
        let sectioned = select_sections(&safe_content, query, FETCH_MAX_QUERY_BLOCKS);
        // `totalMatches` is the COUNT FROM SELECTION, not from whatever mode answered
        // (`index.ts:999`): in proximity mode the answer is one window while this figure counts
        // OCCURRENCES, and that asymmetry is what `capCut` is measured against. Read before the
        // fallback below can take `sectioned` away.
        let total_matches = sectioned.total_matches;
        let flat = sectioned.total_sections <= FETCH_FLAT_DOCUMENT_SECTIONS;
        let answered = if flat && total_matches > 0 {
            select_proximity(&safe_content, query)
        } else {
            sectioned
        };

        if answered.matches.is_empty() {
            return Err(format!(
                "fetch found no section matching \"{query}\" in a {page_characters}-character page. Re-fetch without \"query\" to read the whole page, or use a shorter phrase."
            ));
        }

        // The blocks are joined through `section_header`/`SECTION_SEPARATOR` rather than an inline
        // format string, because the resume mapping below reconstructs this exact layout to turn a
        // cut position back into a page offset.
        let joined = answered
            .matches
            .iter()
            .map(|matched| format!("{}{}", section_header(matched), matched.text))
            .collect::<Vec<_>>()
            .join(SECTION_SEPARATOR);

        // Everything a query answer carries that does NOT depend on the fit, named because the
        // measurement below and the delivered object must agree key for key — a key in one and
        // not the other is how a result sized to its ceiling arrives over the cap
        // (`index.ts:991-1013`). Note there is NO `offset` echo in this branch: selection ignores
        // `offset`, and echoing a number would read as "your offset was applied".
        let safe_title = title
            .as_deref()
            .map(|text| safe_title(&redactor, text))
            .filter(|text| !text.is_empty());
        let mut frame = json!({
            "provider": invocation.provider,
            "url": page.url,
            "contentType": slice_units(&redactor.redact(&page.content_type), FETCH_MAX_CONTENT_TYPE_CHARS),
            "format": invocation.format,
            "query": query,
            "matches": answered.matches.len(),
            "totalSections": answered.total_sections,
            // The COUNT FROM SELECTION, not from whatever mode answered (`index.ts:999`): in
            // proximity mode `matches` is one window and `totalMatches` is the number of
            // OCCURRENCES in the page, and that asymmetry is what makes `capCut` provable.
            "totalMatches": total_matches,
            "matchMode": answered.match_mode,
            "pageCharacters": page_characters,
            "characters": FETCH_MAX_RESULT_CONTENT_BYTES,
            "truncated": false,
            "stopReason": "contentBudget",
            "nextOffset": page_characters,
        });

        // A proximity answer is costed with whichever of its two sentences is HEAVIER, because
        // which one ships depends on the `nextOffset` the fit itself decides
        // (`index.ts:522-525`, `PROXIMITY_NOTE_SKELETON`).
        if answered.match_mode == "proximity" {
            frame["note"] = json!(heavier(PROXIMITY_NOTE_CUT, PROXIMITY_NOTE_WHOLE));
        }

        if let Some(text) = &safe_title {
            frame["title"] = json!(text);
        }

        let bounded_sections = fit_result(&joined, &frame);
        let bounded_length = code_units(&bounded_sections);

        // Two different limits share the one `truncated` flag and need OPPOSITE responses
        // (`index.ts:1043-1076`):
        //   - the result BYTE BUDGET cut the text: more of the already-selected sections exists,
        //     and paging the page from where the cut landed is the remedy, so `nextOffset` ships;
        //   - MAX_QUERY_BLOCKS capped the SECTION LIST: the withheld sections are not one page
        //     away — selection runs on the whole document — so paging cannot reach them and the
        //     caller has to narrow the query.
        // `stopReason` names which one closed the answer; when both do, the budget wins the name
        // because the budget is the one that ships a resume pointer.
        let budget_cut = bounded_length < code_units(&joined);
        // The block cap is a HEADING-mode limit, so it is compared against the constant that
        // imposes it — NOT `totalMatches > matches.length`, which in proximity mode counts phrase
        // occurrences against a one-window answer and reports `resultCap` while withholding
        // nothing (`index.ts:1053-1064`).
        let cap_cut = answered.match_mode == "heading" && total_matches > FETCH_MAX_QUERY_BLOCKS;
        let stop_reason = if budget_cut {
            "contentBudget"
        } else if cap_cut {
            "resultCap"
        } else {
            ""
        };

        // Where a budget-cut answer resumes: an offset into the converted PAGE, to be passed back
        // as `offset` WITHOUT `query`. Absent when the cap rather than the budget caused the
        // truncation, because there is no page position that reaches the withheld sections
        // (`index.ts:1070-1072`).
        let next_offset = if budget_cut {
            resume_offset_for_cut(&safe_content, &answered.matches, bounded_length)
        } else {
            None
        };

        let mut result = Map::new();

        for (key, value) in [
            ("provider", json!(invocation.provider)),
            ("url", json!(page.url)),
            (
                "contentType",
                Value::String(
                    slice_units(
                        &redactor.redact(&page.content_type),
                        FETCH_MAX_CONTENT_TYPE_CHARS,
                    )
                    .to_owned(),
                ),
            ),
            ("format", json!(invocation.format)),
            ("query", json!(query)),
            ("matches", json!(answered.matches.len())),
            ("totalSections", json!(answered.total_sections)),
            ("totalMatches", json!(total_matches)),
            ("matchMode", json!(answered.match_mode)),
            ("pageCharacters", json!(page_characters)),
        ] {
            result.insert(key.to_owned(), value);
        }

        if let Some(text) = safe_title {
            result.insert("title".to_owned(), Value::String(text));
        }

        result.insert("characters".to_owned(), json!(bounded_length));
        // True when the returned sections were cut, or when matching sections were dropped by the
        // block cap — either way, more exists. Which kind it is comes from `stopReason`, because
        // the two have different remedies and one flag alone sent the caller paging for content
        // paging cannot reach.
        result.insert("truncated".to_owned(), json!(budget_cut || cap_cut));
        result.insert("content".to_owned(), json!(bounded_sections));

        if !stop_reason.is_empty() {
            result.insert("stopReason".to_owned(), json!(stop_reason));
        }

        // A proximity hit is an EXCERPT, not a section, and the two are not equally authoritative
        // — so the mode that ran is spelled out. The sentence is chosen from the resume pointer
        // itself: the remedy it names ("use offset") only exists when the pointer does
        // (`index.ts:1077-1086`).
        if answered.match_mode == "proximity" {
            result.insert(
                "note".to_owned(),
                json!(match next_offset {
                    None => PROXIMITY_NOTE_WHOLE,
                    Some(_) => PROXIMITY_NOTE_CUT,
                }),
            );
        }

        if let Some(offset) = next_offset {
            result.insert("nextOffset".to_owned(), json!(offset));
        }

        return Ok(Value::Object(result));
    }

    // The character bound and the RESULT bound are two different limits: MAX_CONTENT_CHARS is
    // what a page may yield across all of its pages, and `fit_result` then keeps ONE result
    // inside the model-facing cap, measured in that cap's own escaped-byte currency, so the
    // result is never REFUSED by the harness's result-budget envelope — a refused document loses
    // the url, the `truncated` flag and the `nextOffset` a model needs in order to resume
    // (`index.ts:1120-1131`).
    let window = slice_units(window, MAX_CONTENT_CHARS);

    // Which disclosure this window ships, decided BEFORE the fit because the sentence has to be
    // costed in the skeleton (`index.ts:1141-1152`). The clamp outranks every other note: a caller
    // that overshot needs to know which window it actually received.
    let note = if offset_clamped {
        format!(
            "Requested offset {} is past the end of this {page_characters}-character page; showing what remains from {start_offset}. The page is fully read once \"content\" is empty.",
            invocation.offset
        )
    } else {
        String::new()
    };

    // A title is part of the RESULT, so it has to be in the skeleton the content fit measures
    // (`index.ts:916-924`): it used to be computed after the fit, in the object literal, which is
    // one of the ways a result sized exactly to its ceiling arrived over the harness cap — the 300
    // characters of title and the key that carries them were never budgeted.
    let safe_title = title
        .as_deref()
        .map(|text| safe_title(&redactor, text))
        .filter(|text| !text.is_empty());

    // The fields that do NOT depend on the fit are costed here and delivered below, and every
    // fit-dependent figure is placed at its WIDEST possible value, so the overhead can only be
    // over-counted: `characters` can never exceed the content ceiling, `nextOffset` can never pass
    // the end of the page, and an optional key is present when it may be absent (`index.ts:1154-1179`).
    let mut frame = json!({
        "provider": invocation.provider,
        "url": page.url,
        "contentType": slice_units(&redactor.redact(&page.content_type), FETCH_MAX_CONTENT_TYPE_CHARS),
        "format": invocation.format,
        "pageCharacters": page_characters,
        // The offset this window ACTUALLY started at, echoed so a caller paging through a document
        // always knows where it is, even when its own offset was out of range.
        "offset": start_offset,
        "characters": FETCH_MAX_RESULT_CONTENT_BYTES,
        // `false` is the WIDER spelling of this boolean ("true" is shorter), and the true case adds
        // only the `nextOffset` key the skeleton already costs.
        "truncated": false,
        "nextOffset": page_characters,
    });

    if offset_clamped {
        frame["offsetClamped"] = json!(true);
    }

    if !note.is_empty() {
        frame["note"] = json!(note.clone());
    }

    if let Some(text) = &safe_title {
        frame["title"] = json!(text);
    }

    let output = fit_result(window, &frame);
    let output_length = code_units(&output);

    // True only when bytes exist BEYOND what is returned here: the end of the last window is not
    // truncation. `truncated` is computed from the SOURCE length, which is what fixes the
    // off-by-one where a page cut exactly at the budget claimed to be whole.
    let truncated = start_offset + output_length < page_characters;

    let mut result = Map::new();

    for (key, value) in [
        ("provider", json!(invocation.provider)),
        ("url", json!(page.url)),
        (
            "contentType",
            Value::String(
                slice_units(
                    &redactor.redact(&page.content_type),
                    FETCH_MAX_CONTENT_TYPE_CHARS,
                )
                .to_owned(),
            ),
        ),
        ("format", json!(invocation.format)),
        ("pageCharacters", json!(page_characters)),
        ("offset", json!(start_offset)),
    ] {
        result.insert(key.to_owned(), value);
    }

    if !note.is_empty() {
        result.insert("note".to_owned(), json!(note));
    }

    if offset_clamped {
        result.insert("offsetClamped".to_owned(), json!(true));
    }

    result.insert("characters".to_owned(), json!(output_length));
    result.insert("truncated".to_owned(), json!(truncated));
    result.insert("content".to_owned(), json!(output));

    // Where the NEXT window starts, present only when more exists. Every other content tool in
    // the harness pages; fetch used to be the one that silently lost the rest. This branch keeps
    // the plain `startOffset + content.length` pointer: with no `query` there is no section list
    // to snap against, and the byte cut lands wherever the budget ended.
    if truncated {
        result.insert("nextOffset".to_owned(), json!(start_offset + output_length));
    }

    if let Some(text) = safe_title {
        result.insert("title".to_owned(), Value::String(text));
    }

    Ok(Value::Object(result))
}

/// The title's published form: redacted, collapsed, bounded, and absent when nothing is left
/// (`index.ts:675-678`).
fn safe_title(redactor: &Redactor, title: &str) -> String {
    let collapsed = pattern(&WHITESPACE_RUN, r"\s+")
        .replace_all(redactor.redact(title).trim(), " ")
        .to_string();

    slice_units(&collapsed, FETCH_MAX_TITLE_CHARS).to_owned()
}

/// The slice from a code-unit offset to the end.
fn slice_units_from(content: &str, offset_units: usize) -> &str {
    let mut units = 0_usize;
    let mut start = content.len();

    for (byte, character) in content.char_indices() {
        if units >= offset_units {
            start = byte;
            break;
        }

        units += character.len_utf16();
        start = byte + character.len_utf8();
    }

    &content[start..]
}

/// Strips scripts, styles, comments and tags, and normalizes whitespace
/// (`webfetch/native.ts`'s conversion, reduced to its observable effect).
fn html_to_text(html: &str) -> String {
    let mut text = String::with_capacity(html.len() / 2);
    let mut rest = html;
    // Block-ish tags become a newline so a document keeps readable lines.
    const BLOCKS: [&str; 8] = ["p", "div", "br", "li", "h1", "h2", "h3", "tr"];

    loop {
        let lower = rest.to_lowercase();

        // Drop the two executable/style regions whole.
        let skipped = {
            let mut cursor = rest;

            for tag in ["script", "style"] {
                let cursor_lower = cursor.to_lowercase();

                if let Some(open) = cursor_lower.find(&format!("<{tag}")) {
                    let close_from = open + tag.len();

                    let close = cursor_lower[close_from..]
                        .find(&format!("</{tag}"))
                        .map(|found| close_from + found + tag.len() + 3)
                        .unwrap_or(cursor.len());

                    cursor = &cursor[close.min(cursor.len())..];
                }
            }

            cursor
        };

        rest = skipped;
        let rest_lower = rest.to_lowercase();

        let Some(open) = rest_lower.find('<') else {
            text.push_str(rest);
            break;
        };

        text.push_str(&rest[..open]);

        let Some(close) = rest_lower[open..].find('>') else {
            break;
        };

        let tag = &rest[open + 1..open + close];
        let name: String = tag
            .trim_start_matches('/')
            .chars()
            .take_while(|character| character.is_alphabetic())
            .collect();

        if BLOCKS.contains(&name.as_str()) {
            text.push('\n');
        }

        rest = &rest[open + close + 1..];
        let _ = &lower;
    }

    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_owned()
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------
//
// Every constant, sentence and field bound in this section is the reference's
// (`tools/websearch/request.ts` + `tools/websearch/index.ts`), because the whole
// point of that module is that a provider is UNTRUSTED DATA: a row is dropped
// rather than truncated, every string is redacted before it is bounded, and a
// provider body is never surfaced (it can echo the key).

/// `websearch/request.ts:53` `DEFAULT_MAX_RESULTS`.
pub const SEARCH_DEFAULT_MAX_RESULTS: usize = 5;
/// `websearch/request.ts:57` `MAX_RESULTS_LIMIT` — a larger request is CLAMPED, not
/// rejected, so a model that asks for 100 still gets a useful answer.
pub const SEARCH_MAX_RESULTS_LIMIT: usize = 20;
/// `websearch/request.ts:60` `SEARCH_TIMEOUT_MS`. No retry: a timeout is reported once.
pub const SEARCH_TIMEOUT_MS: u64 = 15_000;
/// `websearch/request.ts:65` `MAX_RESPONSE_BYTES` (512 KiB), checked on `content-length`
/// AND while streaming. This is what the PROVIDER may send; it is 16x the number the model
/// ever sees, because the transport cap and the delivery cap answer different questions —
/// the first is "may I read this body", the second is "may I put this in a prompt".
pub const SEARCH_MAX_RESPONSE_BYTES: usize = 512 * 1024;
/// `websearch/request.ts:127` `MAX_SEARCH_RESULT_BYTES`, kept EQUAL to the harness's
/// `MODEL_TOOL_RESULT_BUDGET_BYTES` (`src/tools/result.rs`) for the same reason the reference
/// keeps it equal to `MAX_MODEL_TOOL_RESULT_BYTES`: this is the figure the delivery fit spends,
/// and a tool that spends a different number than the transport charges is a tool whose results
/// get JSON-refused.
pub const SEARCH_MAX_RESULT_BYTES: usize = 32 * 1024;
/// `websearch/request.ts:67` `MAX_TITLE_CHARS`.
pub const SEARCH_MAX_TITLE_CHARS: usize = 300;
/// `websearch/request.ts:68` `MAX_SNIPPET_CHARS`.
pub const SEARCH_MAX_SNIPPET_CHARS: usize = 1_000;
/// `websearch/request.ts:72` `MAX_PUBLISHED_CHARS` — a date longer than this is a provider
/// dumping text into a label field, so it is dropped rather than emitted.
pub const SEARCH_MAX_PUBLISHED_CHARS: usize = 64;
/// `websearch/request.ts:76` `MAX_URL_CHARS`. A URL cannot be truncated without corrupting
/// it, so an over-long row is DROPPED (`SEARCH_MAX_URL_CHARS` is the row rule, distinct from
/// the 4096-character `MAX_URL_CHARS` the download tool documents).
pub const SEARCH_MAX_URL_CHARS: usize = 2_048;
/// `websearch/request.ts:80` `REDACTED`. Fixed text: the replacement must not hint at the
/// value it replaced.
pub const SEARCH_REDACTED: &str = "[redacted]";
/// `websearch/index.ts:210-211` — the domain filter bound and the per-entry length.
const SEARCH_MAX_DOMAINS: usize = 20;
const SEARCH_MAX_DOMAIN_CHARS: usize = 253;
/// `websearch/index.ts:89` `PROVIDERS` — the preference order, and therefore the provider a
/// call falls back to when it does not name one.
pub const SEARCH_PROVIDERS: [&str; 3] = ["exa", "tavily", "linkup"];

/// `websearch/index.ts:216` `DOMAIN_ENTRY`: hostname, optional `*.` prefix, optional port,
/// optional opaque path prefix. Permissive about the path (providers treat it as a prefix),
/// strict about the authority, which is where typos land.
static DOMAIN_ENTRY: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
/// `websearch/index.ts:259` `ISO_DATE`: an ISO-8601 date, or a datetime with an optional
/// zone. Validated rather than passed through, because Exa answers a malformed one with a
/// 400 that names no field.
static ISO_DATE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
/// The whitespace run collapses `websearch/request.ts:160` performs before bounding a field.
static WHITESPACE_RUN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();

/// The compiled form of one of the three patterns above.
fn pattern<'a>(slot: &'a std::sync::OnceLock<regex::Regex>, source: &str) -> &'a regex::Regex {
    slot.get_or_init(|| regex::Regex::new(source).expect("the published pattern compiles"))
}

/// UTF-16 code units, the unit every published character bound in this tool is expressed in
/// (`value.length` in the reference). Rust's `chars()` would count an astral character once
/// where the reference counts two, so the ports below count units.
fn code_units(value: &str) -> usize {
    value.encode_utf16().count()
}

/// `value.slice(0, limit)` in code units. A cut that lands between the two units of an astral
/// character keeps the WHOLE character in Rust (a Rust string cannot hold half of one), which
/// is the only place the two ports can differ by one character on an emoji-bearing row.
fn slice_units(value: &str, limit: usize) -> &str {
    let mut units = 0_usize;
    let mut end = 0_usize;

    for (byte, character) in value.char_indices() {
        let width = character.len_utf16();

        if units + width > limit {
            break;
        }

        units += width;
        end = byte + character.len_utf8();
    }

    &value[..end]
}

/// `websearch/request.ts:139` `createRedactor`, over EVERY configured key.
///
/// Every non-empty key is replaced, however short: an explicit or test credential can be a
/// handful of characters, and skipping it would leave the whole value in a provider excerpt.
/// Longer keys go first so a key containing another key is removed whole. Duplicates are
/// dropped, so a shared key is scanned once.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Redactor {
    targets: Vec<String>,
}

impl Redactor {
    #[must_use]
    pub fn new(secrets: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        let mut targets: Vec<String> = Vec::new();

        for secret in secrets {
            let secret = secret.as_ref();

            if !secret.is_empty() && !targets.iter().any(|seen| seen == secret) {
                targets.push(secret.to_owned());
            }
        }

        // Longest first. `sort_by` is a stable sort, so equal lengths keep first-seen order,
        // matching the reference's `Set` + `sort`.
        targets.sort_by_key(|target| std::cmp::Reverse(code_units(target)));

        Self { targets }
    }

    /// Every occurrence of every key, replaced by `SEARCH_REDACTED`. Split/join rather than a
    /// regex: keys are opaque and may carry regex metacharacters.
    #[must_use]
    pub fn redact(&self, text: &str) -> String {
        let mut output = text.to_owned();

        for secret in &self.targets {
            output = output.replace(secret.as_str(), SEARCH_REDACTED);
        }

        output
    }
}

/// `websearch/request.ts:157` `text`: collapse whitespace, trim, bound. Titles and snippets
/// arrive with newlines and runs of spaces; one line is what a tool result wants.
fn collapse_text(value: &str, max: usize) -> String {
    let collapsed = pattern(&WHITESPACE_RUN, r"\s+").replace_all(value, " ");

    slice_units(collapsed.trim(), max).to_owned()
}

/// `websearch/request.ts:168` `bounded`: redact the RAW string, THEN collapse and bound.
/// Redacting after truncation can leave the first characters of a key at the cut.
fn bounded(value: Option<&str>, max: usize, redactor: &Redactor) -> String {
    value.map_or_else(String::new, |raw| collapse_text(&redactor.redact(raw), max))
}

/// `websearch/request.ts:180` `boundedFlagged`: `bounded` plus the truth of whether it cut
/// anything, reported as `snippetTruncated` only when true so an uncut result keeps its exact
/// existing shape.
fn bounded_flagged(value: Option<&str>, max: usize, redactor: &Redactor) -> (String, bool) {
    let Some(raw) = value else {
        return (String::new(), false);
    };

    let collapsed = collapse_text(&redactor.redact(raw), usize::MAX);
    let truncated = code_units(&collapsed) > max;

    (slice_units(&collapsed, max).to_owned(), truncated)
}

/// `websearch/request.ts:193` `excerpt`: a snippet source may be a string (Tavily `content`)
/// or a list of strings (Exa `highlights`).
fn excerpt(value: Option<&Value>) -> String {
    match value {
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" "),
        Some(Value::String(text)) => text.clone(),
        _ => String::new(),
    }
}

/// `websearch/request.ts:203` `date`: provider date fields are strings when present;
/// anything else is ignored rather than stringified into a misleading value.
fn provider_date(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// `websearch/request.ts:215` `validUrl`: absolute http(s) only, no userinfo, at most
/// `SEARCH_MAX_URL_CHARS` before AND after normalization. A provider is data, so a
/// `javascript:` or relative value is dropped from the result instead of handed to the model.
fn valid_url(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();

    if trimmed.is_empty() || code_units(trimmed) > SEARCH_MAX_URL_CHARS {
        return None;
    }

    let parsed = reqwest::Url::parse(trimmed).ok()?;

    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return None;
    }

    // `URL#toString()` normalizes (`http://x` -> `http://x/`); `Url::to_string()` is the
    // equivalent here, and the normalized form is re-measured for the same bound.
    let normalized = parsed.to_string();

    (code_units(&normalized) <= SEARCH_MAX_URL_CHARS).then_some(normalized)
}

/// One provider row mapped to the stable field names, still unvalidated
/// (`websearch/request.ts:114` `MatchFields`).
struct MatchFields {
    title: Option<String>,
    url: Option<String>,
    snippet: String,
    published: Option<String>,
}

/// Which provider's row layout to read (`websearch/{exa,tavily,linkup}.ts`'s readers). An
/// enum rather than a closure: the three layouts differ only in field names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowReader {
    Exa,
    Tavily,
    Linkup,
}

impl RowReader {
    fn read(self, row: &serde_json::Map<String, Value>) -> MatchFields {
        match self {
            // Exa answers a list of highlight fragments; join them into one line.
            Self::Exa => MatchFields {
                title: row.get("title").and_then(Value::as_str).map(str::to_owned),
                url: row.get("url").and_then(Value::as_str).map(str::to_owned),
                snippet: excerpt(row.get("highlights")),
                published: provider_date(row.get("publishedDate")),
            },
            Self::Tavily => MatchFields {
                title: row.get("title").and_then(Value::as_str).map(str::to_owned),
                url: row.get("url").and_then(Value::as_str).map(str::to_owned),
                snippet: excerpt(row.get("content")),
                published: provider_date(row.get("published_date")),
            },
            // Linkup names the title `name` and answers no date at all.
            Self::Linkup => MatchFields {
                title: row.get("name").and_then(Value::as_str).map(str::to_owned),
                url: row.get("url").and_then(Value::as_str).map(str::to_owned),
                snippet: excerpt(row.get("content")),
                published: None,
            },
        }
    }
}

/// `websearch/request.ts:240` `normalizeMatches`.
///
/// Order per field: validate raw -> redact -> bound. Redaction runs on the RAW string so a
/// credential can never be cut at a truncation boundary. Invalid rows are SKIPPED rather than
/// failing the whole search: one bad row is not a reason to lose the good ones.
#[must_use]
fn normalize_matches(
    rows: &[Value],
    max_results: usize,
    redactor: &Redactor,
    reader: RowReader,
) -> (Vec<Value>, usize) {
    let mut matches: Vec<Value> = Vec::new();
    let mut dropped = 0_usize;

    for row in rows {
        // Stop on the VALID count, never on the row index: the caller's quota is what this
        // loop spends, and a provider that pads its answer with junk must not spend it.
        if matches.len() >= max_results {
            break;
        }

        let Value::Object(row) = row else {
            dropped += 1;
            continue;
        };

        let fields = reader.read(row);

        let Some(url) = valid_url(fields.url.as_deref()) else {
            dropped += 1;
            continue;
        };

        // Redaction can LENGTHEN a URL (`[redacted]` is longer than a short key), and a URL
        // that no longer fits is dropped rather than truncated.
        let safe_url = redactor.redact(&url);

        if code_units(&safe_url) > SEARCH_MAX_URL_CHARS {
            dropped += 1;
            continue;
        }

        let (snippet, snippet_truncated) =
            bounded_flagged(Some(&fields.snippet), SEARCH_MAX_SNIPPET_CHARS, redactor);

        // Key order is the published contract: title, url, snippet, then the two optional
        // keys. The title falls back to the URL so a result never has an empty title.
        let title = bounded(fields.title.as_deref(), SEARCH_MAX_TITLE_CHARS, redactor);

        let mut entry = Map::new();
        entry.insert(
            "title".to_owned(),
            Value::String(if title.is_empty() {
                safe_url.clone()
            } else {
                title
            }),
        );
        entry.insert("url".to_owned(), Value::String(safe_url.clone()));
        entry.insert("snippet".to_owned(), Value::String(snippet));

        if snippet_truncated {
            entry.insert("snippetTruncated".to_owned(), json!(true));
        }

        let published = bounded(
            fields.published.as_deref(),
            SEARCH_MAX_PUBLISHED_CHARS,
            redactor,
        );

        if !published.is_empty() {
            entry.insert("published".to_owned(), Value::String(published));
        }

        matches.push(Value::Object(entry));
    }

    (matches, dropped)
}

/// The display form of an argument inside a refusal sentence, matching JS `String(value)`
/// (`search provider "null" is not configured.`). A third private copy of this helper; the
/// canonical one is `stream/protocol.rs:672`, which is not reachable from here.
fn display_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        Value::Array(values) => values
            .iter()
            .map(display_value)
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

/// `websearch/index.ts:291` `readMaxResults`: absent is the default, a non-number is a
/// refusal, an out-of-range number is CLAMPED.
fn read_max_results(input: Option<&Value>) -> Result<usize, String> {
    let Some(value) = input else {
        return Ok(SEARCH_DEFAULT_MAX_RESULTS);
    };

    let Some(number) = value.as_f64().filter(|value| value.is_finite()) else {
        return Err(format!(
            "search maxResults must be a number between 1 and {SEARCH_MAX_RESULTS_LIMIT}."
        ));
    };

    Ok(SEARCH_MAX_RESULTS_LIMIT.min(1.max(number.floor() as usize)))
}

/// `websearch/index.ts:305` `readProvider`: an omitted provider is the first configured one,
/// and a named provider the harness cannot run lists what IS configured so the model can
/// correct itself on the next turn instead of repeating the call.
fn read_provider(
    input: Option<&Value>,
    configured: &[&'static str],
) -> Result<&'static str, String> {
    let Some(value) = input else {
        return Ok(configured[0]);
    };

    configured
        .iter()
        .find(|provider| value.as_str() == Some(**provider))
        .copied()
        .ok_or_else(|| {
            format!(
                "search provider \"{}\" is not configured. Configured: {}.",
                display_value(value),
                configured.join(", ")
            )
        })
}

/// `websearch/index.ts:218` `readDomains`: bounded hard (an unbounded array from a model is
/// a context and cost bomb) and grammar-checked, because the provider answers a typo with a
/// bare 400 that names no field.
fn read_domains(input: Option<&Value>, name: &str) -> Result<Option<Vec<String>>, String> {
    let Some(value) = input else {
        return Ok(None);
    };

    let Some(entries) = value.as_array() else {
        return Err(format!(
            "search {name} must be an array of 1-{SEARCH_MAX_DOMAINS} domains."
        ));
    };

    if entries.is_empty() || entries.len() > SEARCH_MAX_DOMAINS {
        return Err(format!(
            "search {name} must be an array of 1-{SEARCH_MAX_DOMAINS} domains."
        ));
    }

    let domains = pattern(
        &DOMAIN_ENTRY,
        r"^(?:\*\.)?(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?::\d{1,5})?(?:/\S*)?$",
    );

    let mut normalized: Vec<String> = Vec::with_capacity(entries.len());

    for entry in entries {
        let Some(entry) = entry.as_str() else {
            return Err(format!("search {name} entries must be strings."));
        };

        // Trim, lowercase, and drop a scheme prefix or a trailing slash: a stray space or
        // case difference must not silently widen or narrow a filter.
        let lowered = entry.trim().to_lowercase();
        let without_scheme = lowered
            .strip_prefix("http://")
            .or_else(|| lowered.strip_prefix("https://"))
            .unwrap_or(&lowered);
        let trimmed = without_scheme.trim_end_matches('/').to_owned();

        if trimmed.is_empty() || code_units(&trimmed) > SEARCH_MAX_DOMAIN_CHARS {
            return Err(format!(
                "search {name} entries must be 1-{SEARCH_MAX_DOMAIN_CHARS} characters."
            ));
        }

        if !domains.is_match(&trimmed) {
            return Err(format!(
                "search {name} entry \"{}\" is not a domain. Use a hostname (example.com), a path prefix (example.com/docs), or a wildcard subdomain (*.example.com).",
                entry.trim()
            ));
        }

        normalized.push(trimmed);
    }

    Ok((!normalized.is_empty()).then_some(normalized))
}

/// `websearch/index.ts:261` `readDate`.
fn read_date(input: Option<&Value>, name: &str) -> Result<Option<String>, String> {
    let Some(value) = input else {
        return Ok(None);
    };

    let trimmed = value.as_str().unwrap_or_default().trim().to_owned();

    if value.as_str().is_none()
        || !pattern(
            &ISO_DATE,
            r"^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$",
        )
        .is_match(&trimmed)
    {
        return Err(format!(
            "search {name} must be an ISO-8601 date, for example \"2026-01-01\"."
        ));
    }

    Ok(Some(trimmed))
}

/// `websearch/index.ts:271` `readCategory`.
fn read_category(input: Option<&Value>) -> Result<Option<String>, String> {
    let Some(value) = input else {
        return Ok(None);
    };

    let trimmed = value.as_str().unwrap_or_default().trim();

    if value.as_str().is_none() || trimmed.is_empty() || code_units(trimmed) > 64 {
        return Err(
            "search category must be a short string, for example \"news\" or \"company\"."
                .to_owned(),
        );
    }

    Ok(Some(trimmed.to_lowercase()))
}

/// `websearch/index.ts:281` `readSpeed`: `'fast'` asks the provider for its low-latency mode
/// when it has one.
fn read_speed(input: Option<&Value>) -> Result<&'static str, String> {
    match input {
        None => Ok("balanced"),
        Some(value) if value.as_str() == Some("balanced") => Ok("balanced"),
        Some(value) if value.as_str() == Some("fast") => Ok("fast"),
        Some(_) => Err("search speed must be \"balanced\" or \"fast\".".to_owned()),
    }
}

/// One validated call (`websearch/index.ts:193` `Invocation`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchInvocation {
    pub query: String,
    pub max_results: usize,
    pub provider: &'static str,
    pub include_domains: Option<Vec<String>>,
    pub exclude_domains: Option<Vec<String>>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub category: Option<String>,
    pub speed: &'static str,
}

/// `websearch/index.ts:321` `readInvocation`, in the reference's order: query, `since`,
/// `until`, the `until < since` cross-check, then `maxResults`, `provider`, the domain lists,
/// `category`, `speed`. The order is observable, because the first refusal is the one the
/// model reads.
fn read_invocation(
    arguments: &Value,
    configured: &[&'static str],
) -> Result<SearchInvocation, String> {
    // Direct callers may pass the bare query (`search('cats')`); a model always passes an
    // object, because the schema has more than one property.
    let input = match arguments {
        Value::String(text) => json!({ "query": text }),
        other => other.clone(),
    };

    let query = input
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();

    if query.is_empty() {
        return Err("search requires a non-empty query string.".to_owned());
    }

    let since = read_date(input.get("since"), "since")?;
    let until = read_date(input.get("until"), "until")?;

    // A plain string comparison is correct for the ISO-8601 shapes the validator admits.
    if let (Some(since), Some(until)) = (&since, &until)
        && until < since
    {
        return Err("search \"until\" must not be earlier than \"since\".".to_owned());
    }

    // Every remaining field is read here, in the order the reference evaluates them (maxResults,
    // provider, includeDomains, excludeDomains, category, speed): a caller that passes both a bad
    // field AND a bad combination still gets the precise FIELD error first, which is the
    // precedence the existing filter tests pin.
    let max_results = read_max_results(input.get("maxResults"))?;
    let provider = read_provider(input.get("provider"), configured)?;
    let include_domains = read_domains(input.get("includeDomains"), "includeDomains")?;
    let exclude_domains = read_domains(input.get("excludeDomains"), "excludeDomains")?;
    let category = read_category(input.get("category"))?;
    let speed = read_speed(input.get("speed"))?;

    // The combination rules the model-facing description states, enforced HERE so a caller that
    // read the warning does not pay a provider round trip for `Exa search failed (HTTP 400).` — a
    // message naming NO field, because provider bodies are deliberately never surfaced. Same policy
    // as the domain grammar and the ISO date check above.
    //
    // F11 — these are EXA's rules, so they are gated on the RESOLVED provider. They used to run
    // before dispatch, against every provider, and only Exa receives `category` at all: Tavily maps
    // the dates to `start_date`/`end_date` and simply has no category field, so the call was refused
    // for a provider that was never going to answer 400. A refusal the resolved provider cannot
    // justify is not a guard, it is a lost query. Only the combinations verified against the live
    // provider are refused — `company` + excludeDomains and `news` + date filters both work
    // remotely and must not be over-rejected.
    let dated = since.is_some() || until.is_some();

    if provider == "exa" && category.as_deref() == Some("people") && dated {
        return Err(
            "search category \"people\" does not support date filters. Drop \"since\"/\"until\", or use another category."
                .to_owned(),
        );
    }

    if provider == "exa" && category.as_deref() == Some("company") && dated {
        return Err(
            "search category \"company\" does not support date filters. Drop \"since\"/\"until\", or use another category."
                .to_owned(),
        );
    }

    if provider == "exa" && category.as_deref() == Some("people") && exclude_domains.is_some() {
        return Err(
            "search category \"people\" does not support \"excludeDomains\". Drop the category or the domain filter."
                .to_owned(),
        );
    }

    Ok(SearchInvocation {
        max_results,
        provider,
        include_domains,
        exclude_domains,
        since,
        until,
        category,
        speed,
        query,
    })
}

/// `websearch/request.ts:282` `statusFailure`. Provider bodies are deliberately absent: they
/// can contain the request (and therefore the key), an account name, or HTML.
fn status_failure(label: &str, status: u16) -> String {
    if status == 401 || status == 403 {
        return format!(
            "{label} search rejected the API key (HTTP {status}). Check the configured key and account."
        );
    }

    // 402 is Exa's payment-required; 432/433 are Tavily's plan/quota errors.
    if status == 402 || status == 432 || status == 433 {
        return format!(
            "{label} search reports no remaining credits or quota (HTTP {status}). Check the provider account."
        );
    }

    if status == 429 {
        return format!("{label} search is rate limited (HTTP {status}). Try again later.");
    }

    format!("{label} search failed (HTTP {status}).")
}

/// `websearch/request.ts:305` `oversizedResponse` — one sentence for both the declared-size
/// pre-check and the streaming budget.
fn oversized_response(label: &str) -> String {
    format!("{label} search returned a response larger than {SEARCH_MAX_RESPONSE_BYTES} bytes.")
}

/// `websearch/request.ts:416` `networkFailure`: the abort flag is the only way to tell our
/// own timeout from a transport refusal.
fn network_failure(label: &str, timed_out: bool) -> String {
    if timed_out {
        return format!("{label} search timed out after {SEARCH_TIMEOUT_MS} ms.");
    }

    format!("{label} search could not be reached (network error).")
}

/// `websearch/request.ts:475` `resultsArray`: every provider answers with `results`, so an
/// unexpected body (an HTML error page, truncated JSON) becomes the same readable error for
/// any adapter.
fn results_array<'a>(payload: &'a Value, label: &str) -> Result<&'a [Value], String> {
    let Value::Object(body) = payload else {
        return Err(format!("{label} search returned an unexpected response."));
    };

    body.get("results")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("{label} search returned a response without a \"results\" list."))
}

/// The endpoint a call actually uses: the published URL, with its authority replaced by
/// [`WebKeys::endpoint_base`] when the caller supplied one.
fn endpoint(base: Option<&str>, default: &str) -> String {
    let Some(base) = base.map(str::trim).filter(|base| !base.is_empty()) else {
        return default.to_owned();
    };

    // Only the AUTHORITY is replaced. Each provider's path is part of its published API and
    // has to survive the hop to a proxy or a stub server.
    let tail = reqwest::Url::parse(default).map_or_else(
        |_| String::new(),
        |parsed| {
            format!(
                "{}{}",
                parsed.path(),
                parsed
                    .query()
                    .map_or_else(String::new, |query| format!("?{query}"))
            )
        },
    );

    format!("{}{tail}", base.trim_end_matches('/'))
}

/// `websearch/request.ts:410` `requestSearch`: the one place a network request is made.
/// 15 s budget, `redirect: 'error'`, a declared-size pre-check, a streaming size budget, and
/// a fixed sentence per failure class.
async fn request_search(
    client: &reqwest::Client,
    label: &str,
    url: &str,
    headers: &[(&str, String)],
    body: Value,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let transfer = async {
        let mut request = client
            .post(url)
            .header("content-type", "application/json")
            .json(&body);

        for (name, value) in headers {
            request = request.header(*name, value);
        }

        let mut response = request
            .send()
            .await
            .map_err(|_| network_failure(label, false))?;

        if response.status().is_redirection() {
            return Err(network_failure(label, false));
        }

        if response.url().as_str() != url {
            // `redirect: 'error'` in the reference (`websearch/request.ts:432`): a redirect
            // would move the request somewhere the fixed endpoint policy did not choose.
            // reqwest follows redirects for us, so a final URL that differs from the endpoint
            // IS the redirect, and it becomes the same network error the reference throws.
            return Err(network_failure(label, false));
        }

        if !response.status().is_success() {
            return Err(status_failure(label, response.status().as_u16()));
        }

        // Reject an oversized DECLARED body before reading it; the streaming budget below still
        // applies when the header is absent or wrong.
        let declared = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<usize>().ok());

        if declared.is_some_and(|bytes| bytes > SEARCH_MAX_RESPONSE_BYTES) {
            return Err(oversized_response(label));
        }

        // `response.text()` would allocate the whole body first, so a provider could force a
        // huge allocation before any check ran. `chunk()` is the streaming equivalent.
        let mut received: Vec<u8> = Vec::new();

        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| network_failure(label, false))?
        {
            received.extend_from_slice(&chunk);

            if received.len() > SEARCH_MAX_RESPONSE_BYTES {
                return Err(oversized_response(label));
            }
        }

        serde_json::from_slice(&received)
            .map_err(|_| format!("{label} search returned a response that is not JSON."))
    };

    tokio::select! {
        biased;
        () = signal.cancelled() => Err("the request was cancelled.".to_owned()),
        outcome = tokio::time::timeout(std::time::Duration::from_millis(SEARCH_TIMEOUT_MS), transfer) => {
            match outcome {
                Err(_) => Err(network_failure(label, true)),
                Ok(value) => value,
            }
        }
    }
}

/// `websearch/exa.ts`: the key travels in a HEADER — not logged in the URL, not embedded in a
/// body the provider may echo, not present in any redirect target.
#[allow(
    clippy::too_many_arguments,
    reason = "one provider adapter's own argument list"
)]
async fn exa_search(
    client: &reqwest::Client,
    invocation: &SearchInvocation,
    api_key: &str,
    base: Option<&str>,
    redactor: &Redactor,
    signal: &CancellationToken,
) -> Result<(Vec<Value>, usize), String> {
    const LABEL: &str = "Exa";

    // `lite` is the documented low-latency mode that answers from Exa's index; `auto` is Exa's
    // balanced default. Only these two: the tool never upgrades a call on its own.
    let mut body = json!({
        "query": invocation.query,
        "type": if invocation.speed == "fast" { "lite" } else { "auto" },
        // Set explicitly: the provider must not choose its own default count, because the
        // tool's bound is what keeps the model's context small.
        "numResults": invocation.max_results,
    });

    // Sent only when the caller asked: an empty array is not the same as an absent key, and a
    // stray `category: undefined` is wire noise.
    attach(
        &mut body,
        "includeDomains",
        invocation.include_domains.clone().map(Value::from),
    );
    attach(
        &mut body,
        "excludeDomains",
        invocation.exclude_domains.clone().map(Value::from),
    );
    attach(
        &mut body,
        "startPublishedDate",
        invocation.since.clone().map(Value::from),
    );
    attach(
        &mut body,
        "endPublishedDate",
        invocation.until.clone().map(Value::from),
    );
    attach(
        &mut body,
        "category",
        invocation.category.clone().map(Value::from),
    );
    body["contents"] = json!({ "highlights": true });

    let payload = request_search(
        client,
        LABEL,
        &endpoint(base, "https://api.exa.ai/search"),
        &[("x-api-key", api_key.to_owned())],
        body,
        signal,
    )
    .await?;

    Ok(normalize_matches(
        results_array(&payload, LABEL)?,
        invocation.max_results,
        redactor,
        RowReader::Exa,
    ))
}

/// `websearch/tavily.ts`.
async fn tavily_search(
    client: &reqwest::Client,
    invocation: &SearchInvocation,
    api_key: &str,
    base: Option<&str>,
    redactor: &Redactor,
    signal: &CancellationToken,
) -> Result<(Vec<Value>, usize), String> {
    const LABEL: &str = "Tavily";

    let mut body = json!({
        "query": invocation.query,
        "max_results": invocation.max_results,
        "search_depth": "basic",
        "include_answer": false,
        "include_raw_content": false,
    });

    attach(
        &mut body,
        "include_domains",
        invocation.include_domains.clone().map(Value::from),
    );
    attach(
        &mut body,
        "exclude_domains",
        invocation.exclude_domains.clone().map(Value::from),
    );
    attach(
        &mut body,
        "start_date",
        invocation.since.clone().map(Value::from),
    );
    attach(
        &mut body,
        "end_date",
        invocation.until.clone().map(Value::from),
    );

    let payload = request_search(
        client,
        LABEL,
        &endpoint(base, "https://api.tavily.com/search"),
        &[("authorization", format!("Bearer {api_key}"))],
        body,
        signal,
    )
    .await?;

    Ok(normalize_matches(
        results_array(&payload, LABEL)?,
        invocation.max_results,
        redactor,
        RowReader::Tavily,
    ))
}

/// `websearch/linkup.ts`. The adapter receives every filter and sends what its provider has;
/// Linkup has no domain, date or category filter, and its low-latency depth is the only mode
/// this tool uses.
async fn linkup_search(
    client: &reqwest::Client,
    invocation: &SearchInvocation,
    api_key: &str,
    base: Option<&str>,
    redactor: &Redactor,
    signal: &CancellationToken,
) -> Result<(Vec<Value>, usize), String> {
    const LABEL: &str = "Linkup";

    let payload = request_search(
        client,
        LABEL,
        &endpoint(base, "https://api.linkup.so/v1/search"),
        &[("authorization", format!("Bearer {api_key}"))],
        json!({
            "q": invocation.query,
            "depth": "fast",
            "outputType": "searchResults",
            "maxResults": invocation.max_results,
        }),
        signal,
    )
    .await?;

    Ok(normalize_matches(
        results_array(&payload, LABEL)?,
        invocation.max_results,
        redactor,
        RowReader::Linkup,
    ))
}

/// Inserts a provider body key only when the caller asked for it.
fn attach(body: &mut Value, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        body[key] = value;
    }
}

/// `websearch/index.ts:447` `unusableRowsNote`: the F5 sentence, and the only place the
/// reference blames the PROVIDER rather than the query. `%s` is the provider label.
#[must_use]
pub fn unusable_rows_note(provider: &str, dropped: usize) -> String {
    format!(
        "{label} returned {dropped} result {} with no usable http(s) URL, so none of them could be shown. The provider did answer: the query is not necessarily at fault.",
        if dropped == 1 { "row" } else { "rows" },
        label = search_label(provider),
    )
}

/// `websearch/index.ts:453` `sizeNote`: which rows went, and the two levers that bring detail
/// back. Naming `maxResults` and "narrow the query" matters, because the model is the one
/// holding both knobs — a truncation it cannot attribute is a truncation it cannot work around.
#[must_use]
pub fn size_note(omitted: usize, total: usize, clipped: bool) -> String {
    let budget = format!("the {SEARCH_MAX_RESULT_BYTES}-byte result budget");
    let rows = format!(
        "{omitted} of {total} result {}",
        if total == 1 { "row" } else { "rows" }
    );

    if omitted == 0 {
        // Only the clip step can reach here: every row is present, just with a shorter
        // excerpt than the provider sent.
        return format!(
            "The single result was too large for {budget}: its excerpt was shortened. Narrow the query or lower maxResults for more per-row detail."
        );
    }

    if omitted == total {
        return format!(
            "All {total} result {} too large to deliver within {budget}, so none are shown. Lower maxResults or narrow the query.",
            if total == 1 { "row was" } else { "rows were" }
        );
    }

    format!(
        "{rows} {} omitted to fit {budget}{}. Narrow the query or lower maxResults for more per-row detail.",
        if total == 1 { "was" } else { "were" },
        if clipped {
            ", and the excerpt that remains was shortened"
        } else {
            ""
        }
    )
}

/// The provider label used in failure and note sentences (`websearch/index.ts:104` `LABELS`).
#[must_use]
fn search_label(provider: &str) -> &'static str {
    match provider {
        "tavily" => "Tavily",
        "linkup" => "Linkup",
        _ => "Exa",
    }
}

/// `websearch/request.ts:425` `fitRowsToResultBudget`: drop TRAILING rows until the delivered
/// document fits (the provider's order IS its ranking, so a prefix keeps what the caller was
/// most likely to cite), then clip the single remaining row's excerpt by bisection.
///
/// `measure` is supplied by the caller because the number that matters is the WHOLE result —
/// rows plus echoed filters plus the `resultsTruncated`/`note` this function makes the caller
/// add. A fit that priced the rows alone lands back over the cap once the explanation is
/// attached, which is the exact failure this replaced.
///
/// INVARIANT: the returned rows satisfy `measure(rows) <= budget`, or the rows are empty.
fn fit_rows_to_result_budget(
    rows: &[Value],
    budget: usize,
    measure: &dyn Fn(&[Value]) -> usize,
) -> (Vec<Value>, bool, bool) {
    let mut fitted = rows.to_vec();

    // The common path, and the shape every default call takes: an untouched search costs one
    // measurement and adds no flag and no note.
    if fitted.is_empty() || measure(&fitted) <= budget {
        return (fitted, false, false);
    }

    // Drop from the END, keeping at least one row for the clip step below.
    while fitted.len() > 1 && measure(&fitted) > budget {
        fitted.pop();
    }

    let mut clipped = false;

    // One row still over the budget is CLIPPED rather than thrown away: a short excerpt with a
    // real URL beats "no results".
    if fitted.len() == 1 && measure(&fitted) > budget {
        let original = fitted[0].clone();
        let snippet = original
            .get("snippet")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let units = code_units(&snippet);

        // The clip is a row, not a string: `snippetTruncated` claims "this excerpt does not
        // reach the source's own end", and that flag is PAYLOAD too. Bisecting without it
        // lands ~23 bytes over the cap and the row then gets dropped for nothing.
        let clip_to = |kept: usize| -> Value {
            let mut row = original.clone();

            if kept == 0 {
                row["snippet"] = json!("");
                row.as_object_mut()
                    .expect("an object row")
                    .remove("snippetTruncated");
            } else {
                row["snippet"] = json!(slice_units(&snippet, kept));
                row["snippetTruncated"] = json!(true);
            }

            row
        };

        // Escaping is per character, so `measure` is monotone in the snippet and the largest
        // fitting prefix falls out of a bisection over code units (at most ~10 measures for a
        // 1 000-character excerpt). Guessing a character count instead would be wrong by the
        // escaped width of every quote and non-ASCII letter in the row.
        let mut low = 0_usize;
        let mut high = units;

        while low < high {
            // Upper midpoint: `mid` is always > `low`, so the loop cannot stall when the
            // answer is `low + 1`.
            let mid = (low + high).div_ceil(2);

            if measure(&[clip_to(mid)]) <= budget {
                low = mid;
            } else {
                high = mid - 1;
            }
        }

        fitted[0] = clip_to(low);
        clipped = true;

        // A row whose TITLE and URL alone exceed the budget cannot be rescued by clipping its
        // excerpt; dropping it still beats a result the harness refuses.
        if measure(&fitted) > budget {
            fitted.clear();
        }
    }

    (fitted, true, clipped)
}

/// `websearch/index.ts:475` `fitToDeliveryBudget`: hand back a result the harness cannot
/// refuse. `render` is the ONE place the delivered object is built once trimming starts, so a
/// measurement can never price a shape other than the one actually returned.
fn fit_to_delivery_budget(result: Value) -> Value {
    let rows = result
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    // Measured exactly as it will be delivered, so an untouched result costs one stringify.
    if payload::document_wire_bytes(&result) <= SEARCH_MAX_RESULT_BYTES {
        return result;
    }

    let total = rows.len();
    let original_note = result
        .get("note")
        .and_then(Value::as_str)
        .filter(|note| !note.is_empty())
        .map(str::to_owned);

    let render = |candidate: &[Value], clipped: bool| -> Value {
        let omitted = total - candidate.len();
        let mut notes: Vec<String> = Vec::new();

        if let Some(note) = &original_note {
            notes.push(note.clone());
        }

        if omitted > 0 || clipped {
            notes.push(size_note(omitted, total, clipped));
        }

        let mut rendered = result.clone();
        rendered["results"] = Value::Array(candidate.to_vec());
        rendered["resultsTruncated"] = json!(true);

        if !notes.is_empty() {
            rendered["note"] = json!(notes.join(" "));
        } else {
            rendered
                .as_object_mut()
                .expect("an object result")
                .remove("note");
        }

        rendered
    };

    let (fitted, truncated, clipped) =
        fit_rows_to_result_budget(&rows, SEARCH_MAX_RESULT_BYTES, &|candidate| {
            // `clipped` is unknown while rows are still being dropped, and passing `true` is
            // the conservative direction: it can only cost the note a few bytes, and the final
            // render below is the one the caller sees.
            payload::document_wire_bytes(&render(candidate, true))
        });

    if !truncated {
        return result;
    }

    render(&fitted, clipped)
}

/// `websearch/index.ts:487` `createWebSearchTool().execute`.
///
/// # Errors
///
/// The unconfigured hint when no key exists, a validation refusal, or one of the reference's
/// fixed provider sentences.
pub(crate) async fn search(
    client: &reqwest::Client,
    arguments: &Value,
    keys: &WebKeys,
    signal: &CancellationToken,
) -> Result<Value, String> {
    // The providers with a usable key, in the published preference order. A provider the
    // operator configured but this build has no key for is not "available": opting in without
    // a key is the same as being off.
    let configured: Vec<&'static str> = SEARCH_PROVIDERS
        .into_iter()
        .filter(|provider| keys.key_of(provider).is_some())
        .collect();

    if configured.is_empty() {
        return Err(WEBSEARCH_UNCONFIGURED_HINT.to_owned());
    }

    let invocation = read_invocation(arguments, &configured)?;
    let api_key = keys
        .key_of(invocation.provider)
        .ok_or_else(|| {
            format!(
                "search provider \"{}\" is not configured.",
                invocation.provider
            )
        })?
        .to_owned();

    // Redact EVERY configured key, not only the one being called: a provider, or a page it
    // quotes, could repeat the other key.
    let redactor = Redactor::new(
        configured
            .iter()
            .filter_map(|provider| keys.key_of(provider))
            .cloned(),
    );

    let base = keys.endpoint_base.as_deref();
    let (results, dropped) = if invocation.provider == "exa" {
        exa_search(client, &invocation, &api_key, base, &redactor, signal).await?
    } else if invocation.provider == "tavily" {
        tavily_search(client, &invocation, &api_key, base, &redactor, signal).await?
    } else {
        linkup_search(client, &invocation, &api_key, base, &redactor, signal).await?
    };

    // Echo the filters that were applied, so a model can tell "no results exist" from "my
    // filter excluded everything" — the difference between retrying wider and giving up.
    // `speed` is echoed only when it is not the default, which is what `balanced` costs
    // nothing on the wire.
    let mut result = json!({
        "provider": invocation.provider,
        "query": invocation.query,
        "maxResults": invocation.max_results,
        "results": results,
    });

    // F5 — the count of provider rows this call could not use, surfaced ONLY when non-zero so
    // the common result keeps its exact previous shape, and the sentence for the case where
    // that cost the caller everything: `results: []` from a provider that answered is a
    // different situation from an empty index, and the model can only act on the difference if
    // the result states it.
    if dropped > 0 {
        result["droppedResults"] = json!(dropped);
    }

    if results.is_empty() && dropped > 0 {
        result["note"] = json!(unusable_rows_note(invocation.provider, dropped));
    }

    attach(
        &mut result,
        "includeDomains",
        invocation.include_domains.map(Value::from),
    );
    attach(
        &mut result,
        "excludeDomains",
        invocation.exclude_domains.map(Value::from),
    );
    attach(&mut result, "since", invocation.since.map(Value::from));
    attach(&mut result, "until", invocation.until.map(Value::from));
    attach(
        &mut result,
        "category",
        invocation.category.map(Value::from),
    );

    if invocation.speed != "balanced" {
        result["speed"] = json!(invocation.speed);
    }

    // F2 — the LAST thing that touches the result: it runs after redaction (rows leave whole,
    // so a credential can never be cut at a fit boundary) and after every filter echo (so
    // those bytes are counted too).
    Ok(fit_to_delivery_budget(result))
}

// ---------------------------------------------------------------------------
// download
// ---------------------------------------------------------------------------

/// The MIME -> attachment-kind table (`download.tool.ts:322-327`, via the shared
/// `MIME_BY_KIND` lists in `shared/attachments.ts:110-116`).
#[must_use]
pub fn kind_for_mime(mime: &str) -> AttachmentKind {
    let base = mime.split(';').next().unwrap_or(mime).trim().to_lowercase();

    if base.starts_with("image/") {
        return AttachmentKind::Image;
    }

    if base.starts_with("video/") {
        return AttachmentKind::Video;
    }

    if base.starts_with("audio/") {
        return AttachmentKind::Audio;
    }

    if base == "application/pdf" {
        return AttachmentKind::Pdf;
    }

    AttachmentKind::File
}

/// The `filename*` parameter pattern (`EXTENDED_NAME`, `download.tool.ts:976`).
static DISPOSITION_EXTENDED: OnceLock<regex::Regex> = OnceLock::new();
/// The `filename` parameter pattern (`PLAIN_NAME`, `download.tool.ts:977`).
static DISPOSITION_PLAIN: OnceLock<regex::Regex> = OnceLock::new();
/// The `UTF-8''` prefix of an RFC 5987 value (`:985`).
static EXTENDED_PREFIX: OnceLock<regex::Regex> = OnceLock::new();
/// The drive-colon form a `path` argument may not use (`:332`).
static DRIVE_PREFIX: OnceLock<regex::Regex> = OnceLock::new();

/// `download.tool.ts:172` `DOWNLOAD_TIMEOUT_MS`: 60 s for the WHOLE transfer, because a 20 MiB
/// file over an ordinary link is a legitimate minute. A slow start, a slow body, and a slow DNS
/// answer all spend from this one budget; the resolve watchdog inside it is
/// [`ssrf::RESOLVE_TIMEOUT_MS`].
pub const DOWNLOAD_TIMEOUT_MS: u64 = 60_000;
/// `download.tool.ts:178` `MAX_REDIRECT_HOPS`, the same cap `webfetch/request.ts` puts on fetch:
/// a redirect loop must not pin the session. Five hops are FOLLOWED; the sixth is refused.
pub const MAX_REDIRECT_HOPS: usize = 5;
/// `download.tool.ts:190` `MAX_NAME_CHARS`.
pub const DOWNLOAD_MAX_NAME_CHARS: usize = 200;
/// `download.tool.ts:265` `MAX_PATH_CHARS`: a `path` argument is bounded like every other
/// model-supplied string here.
pub const DOWNLOAD_MAX_PATH_CHARS: usize = 512;
/// `download.tool.ts:281` `GREP_MAX_FILE_BYTES`, restated as a literal because the `searchable`
/// flag CLAIMS what `grep` will do with the file, and a claim about a sibling tool has to name
/// that tool's own bound.
pub const DOWNLOAD_GREP_MAX_FILE_BYTES: u64 = 1024 * 1024;
/// `download.tool.ts:386` — the `-1`, `-2`, … suffix budget before the unique-name refusal.
pub const DOWNLOAD_UNIQUE_ATTEMPTS: usize = 50;
/// `download.tool.ts:259` `FALLBACK_MIME`: a missing content type is a real MIME, never the empty
/// string (the attachment validator requires one).
pub const DOWNLOAD_FALLBACK_MIME: &str = "application/octet-stream";
/// The per-session subfolder inside the session directory. The TypeScript host composes the same
/// path in `harnessSessionDownloadsDirectoryOf`; here the session directory is
/// `persistence::sessions_dir(root)/<session_id>` (`src/journal.rs:354`), which is the directory
/// `delete_session` removes (`src/store/session.rs:483`) — and that is the whole point: a session
/// owns its bytes, so removing the session removes the downloads with it.
pub const HARNESS_SESSION_DOWNLOADS_DIRECTORY: &str = "downloads";
/// The fail-closed sentence (`download.tool.ts:302-304`). A folder chosen by the TOOL is a folder
/// nobody owns, so with no session folder there is no owner and the call refuses BEFORE a byte is
/// transferred, rather than writing somewhere the caller never named.
pub const DOWNLOAD_SESSION_REQUIRED: &str = "download needs a session: this harness has no session downloads folder, so there is nowhere to save the file. Run it inside a server session, or build the harness with `downloadsDirectory`.";
/// The URL shape refusal (`download.tool.ts:1291`).
pub const DOWNLOAD_URL_INVALID: &str =
    "download requires an absolute http(s) URL without credentials.";
/// `download.tool.ts:1370` — the one network sentence. The platform's own error text is never
/// echoed: a signed URL's query may carry a token, and the message must not repeat it.
pub const DOWNLOAD_UNREACHABLE: &str = "download could not be reached (network error).";
/// `download.tool.ts:1044` `oversize`.
#[must_use]
pub fn download_oversize() -> String {
    format!("download rejects files larger than {MAX_DOWNLOAD_BYTES} bytes (20 MiB).")
}

/// `download.tool.ts:1031-1039` `statusFailure`: one fixed sentence per status class, and the
/// response BODY is never echoed (an error page is HTML noise).
#[must_use]
pub fn download_status_failure(status: u16) -> String {
    match status {
        404 => "download could not find the file (HTTP 404).".to_owned(),
        401 | 403 => format!("download was denied access to the file (HTTP {status})."),
        429 => "download is rate limited by the host (HTTP 429). Try again later.".to_owned(),
        other => format!("download failed (HTTP {other})."),
    }
}

/// `download.tool.ts:847` `refusedUrl`. `detail` is the host or address gate 2 blamed, and it is
/// carried because "the URL you gave me" and "the address that name answered" are different facts.
#[must_use]
pub fn refused_url(detail: Option<&str>) -> String {
    format!(
        "download refuses this URL: it is not a public absolute http(s) address (loopback and private addresses are not downloaded){}.",
        match detail {
            None => String::new(),
            Some(value) => format!(" — {value} resolves to a non-public address"),
        }
    )
}

/// `download.tool.ts:857` `refusedHop`: its own sentence, because the model's argument and a
/// third-party host's redirect behaviour are things the caller can act on separately.
#[must_use]
pub fn refused_hop(status: u16, address: Option<&str>) -> String {
    format!(
        "download refused a redirect to a non-public address (HTTP {status}){}.",
        match address {
            None => String::new(),
            Some(value) => format!(" — {value}"),
        }
    )
}

/// Gates 1 and 2 on one URL (`safeDownloadUrl` + `resolvePublicHost`, `download.tool.ts:736-745`
/// and `:749`), reported with the caller's own sentence.
///
/// GATE 1 RUNS FIRST AND IS UNCONDITIONAL: it costs nothing, needs no resolver, and it is the only
/// gate that can see an address LITERAL (gate 2 has no verdict when no resolver answers — see the
/// fail-open rule in [`ssrf`]).
async fn gate_download_url(url: &str, resolver: &ssrf::Resolver) -> Result<(), Option<String>> {
    let host = ssrf::hostname_of(url).ok_or(None)?;

    if ssrf::is_private_host(&host) {
        return Err(None);
    }

    let verdict = ssrf::resolve_public_host(&host, resolver).await;

    if verdict.blocked {
        // The address LIST the resolver answered with, not the hostname: `refusedUrl` is passed
        // `firstHop.address ?? hostname` (download.tool.ts:1347), because the actionable fact is
        // what the name pointed at.
        return Err(Some(verdict.address.unwrap_or(host)));
    }

    Ok(())
}

/// A `Location` header resolved against the URL that produced it (`redirectLocation`,
/// `download.tool.ts:865-873`). A relative `Location` is legal and common, so it is RESOLVED rather
/// than rejected; the result still has to pass both gates before anything is asked for it.
#[must_use]
fn redirect_location(location: Option<&str>, from: &str) -> Option<String> {
    let location = location?.trim();

    if location.is_empty() {
        return None;
    }

    reqwest::Url::parse(from)
        .ok()?
        .join(location)
        .ok()
        .map(|target| target.to_string())
}

/// The client for one download. `Policy::none()` is not a style choice
/// (`download.tool.ts:1349-1355`): a platform that follows redirects itself resolves `Location` and
/// connects to the new host BEFORE this function regains control, so one 302 from a public URL into
/// loopback delivers internal bytes to the model and to disk. Taking the hops here is what keeps
/// every destination inside the gate.
///
/// This is also why `fetch` and `download` cannot share one client: `RequestBuilder` in this reqwest
/// version has no per-request redirect policy, so the no-follow policy needs a client of its own.
fn download_client(keys: &WebKeys) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_millis(DOWNLOAD_TIMEOUT_MS))
        .user_agent(FETCH_BROWSER_USER_AGENT);

    // The test seam: a PUBLIC host name pinned to a stub address, so the HTTP leg stays testable
    // behind a gate that refuses loopback names. Production leaves the map empty, and an empty
    // `resolve_to_addrs` list changes nothing.
    for (host, addresses) in &keys.connect_to {
        if !addresses.is_empty() {
            builder = builder.resolve_to_addrs(host, addresses);
        }
    }

    builder.build().map_err(|_| DOWNLOAD_UNREACHABLE.to_owned())
}

/// `isSearchableText` (`download.tool.ts:269-277`): the content types `grep` can actually open.
/// Reported so the model is told WHICH affordance it just got — a searchable text file, or an
/// addressable blob.
#[must_use]
pub fn is_searchable_text(mime: &str) -> bool {
    mime.starts_with("text/")
        || mime == "application/json"
        || mime.ends_with("+json")
        || mime == "application/xml"
        || mime.ends_with("+xml")
        || mime == "image/svg+xml"
        || mime == "application/javascript"
        || mime == "application/x-javascript"
}

/// `noteFor` (`download.tool.ts:1057-1071`), decided by exactly two facts: whether the bytes
/// reached the disk, and whether `grep` can search what is there.
///
/// WHY IT EXISTS: the file lives in the harness SESSION's folder, which is usually NOT under the
/// working directory the file tools default to, so "there is a file at X" is not enough — a model
/// that runs `glob` from the working directory gets an empty answer and concludes the download
/// failed. What makes it reachable is NAMING the absolute path explicitly: `glob` and `grep` read
/// `.gitignore` at or below the root they are GIVEN, and both treat an explicitly named root as
/// walked even when an enclosing ignore file would skip it.
#[must_use]
pub fn note_for(saved: bool, searchable: bool, save_error: Option<&str>) -> String {
    if !saved {
        return format!(
            "Nothing was written to disk{}: these bytes exist only in this message, so no file tool can reach them.",
            match save_error {
                None => String::new(),
                Some(reason) => format!(" ({reason})"),
            }
        );
    }

    if searchable {
        return "Saved in this session's downloads folder. Point read, grep or glob at the absolute `path` above: a path you name explicitly is searched even when a .gitignore above it ignores that folder, and this file is text under 1 MiB so grep can search it.".to_owned();
    }

    "Saved in this session's downloads folder. Point read or glob at the absolute `path` above: a path you name explicitly is searched even when a .gitignore above it ignores that folder. grep skips this file (it opens only text files under 1 MiB), so use the bytes in this message or read the file.".to_owned()
}

/// `MIME_EXTENSIONS` (`download.tool.ts:232-256`). Providers key document handling off the file
/// EXTENSION, so `report` served as `application/pdf` has to arrive as `report.pdf`.
#[must_use]
fn mime_extension(mime: &str) -> Option<&'static str> {
    match mime {
        "image/png" => Some(".png"),
        "image/jpeg" | "image/jpg" => Some(".jpg"),
        "image/webp" => Some(".webp"),
        "image/gif" => Some(".gif"),
        "image/svg+xml" => Some(".svg"),
        "application/pdf" => Some(".pdf"),
        "audio/mpeg" | "audio/mp3" => Some(".mp3"),
        "audio/wav" | "audio/x-wav" | "audio/wave" => Some(".wav"),
        "audio/ogg" => Some(".ogg"),
        "video/mp4" => Some(".mp4"),
        "video/webm" => Some(".webm"),
        "video/quicktime" => Some(".mov"),
        "video/ogg" => Some(".ogg"),
        "text/plain" => Some(".txt"),
        "text/markdown" => Some(".md"),
        "text/csv" => Some(".csv"),
        "text/html" => Some(".html"),
        "application/json" => Some(".json"),
        "application/zip" => Some(".zip"),
        "application/octet-stream" => Some(".bin"),
        _ => None,
    }
}

/// `unquote` (`download.tool.ts:963`): the `\"` and `\\` a quoted-string may contain (RFC 7230
/// §3.2.6). Every OTHER backslash survives to [`sanitize_name`], which strips the characters a
/// label cannot hold anyway.
#[must_use]
fn unquote(value: &str) -> String {
    let characters: Vec<char> = value.chars().collect();
    let mut name = String::with_capacity(value.len());
    let mut index = 0_usize;

    while index < characters.len() {
        if characters[index] == '\\'
            && index + 1 < characters.len()
            && (characters[index + 1] == '"' || characters[index + 1] == '\\')
        {
            name.push(characters[index + 1]);
            index += 2;
            continue;
        }

        name.push(characters[index]);
        index += 1;
    }

    name
}

/// `decodeURIComponent` for a percent-escaped `filename*` value. `None` means "malformed escape",
/// which is the reference's `catch` branch: a broken `%zz` is not fatal, and the raw header value
/// is used instead.
#[must_use]
fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0_usize;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = std::str::from_utf8(bytes.get(index + 1..index + 3)?).ok()?;

            out.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
            continue;
        }

        out.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(out).ok()
}

/// A plain file name out of an untrusted string (`download.tool.ts:900-926` `sanitizeName`):
/// percent-decoded, BASENAME only on either separator, stripped of every character a plain file
/// label cannot hold, never a dot-only label, and at most [`DOWNLOAD_MAX_NAME_CHARS`] characters.
///
/// This is the blast-radius bound for the whole name pipeline: the disposition capture below is
/// deliberately generous, and what keeps a hostile header from naming something surprising is that
/// separators, the drive colon and control characters are removed HERE.
#[must_use]
pub fn sanitize_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();

    if trimmed.is_empty() {
        return None;
    }

    // A header value may percent-encode the name (RFC 5987 `filename*`). The quotes are dropped
    // BEFORE decoding, because a `"` inside a captured value is a delimiter the header grammar
    // already removed, and leaving it would make `%22` and a literal quote behave differently.
    let stripped: String = trimmed
        .chars()
        .filter(|character| *character != '"' && *character != '\'')
        .collect();
    let candidate = match percent_decode(&stripped) {
        Some(decoded) if !decoded.is_empty() => decoded,
        // A malformed escape is not fatal: the RAW value is used, minus the characters the label
        // grammar forbids anyway (which the filter below does).
        _ => trimmed.to_owned(),
    };

    // Basename only, on either separator, then remove every character a plain file name cannot
    // hold. The list is the reference's: NUL..0x1f, DEL, and `< > : " / \ | ? *`.
    let leaf_raw = candidate.rsplit(['/', '\\']).next().unwrap_or_default();
    let leaf: String = leaf_raw
        .chars()
        .filter(|character| {
            let code = u32::from(*character);

            code > 0x1f
                && code != 0x7f
                && !matches!(
                    character,
                    '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
                )
        })
        .collect();

    let leaf = leaf.trim();

    // A label that collapses to a dot-prefix or nothing is not a name: `..` would climb out of the
    // session folder, and `.` names the folder itself.
    if leaf.is_empty() || leaf.chars().all(|character| character == '.') {
        return None;
    }

    Some(leaf.chars().take(DOWNLOAD_MAX_NAME_CHARS).collect())
}

/// The `filename` / `filename*` parameter of a `content-disposition` header
/// (`download.tool.ts:975-995`). `filename*` wins because it carries the charset explicitly.
///
/// QUOTE-AWARE, and that is the whole fix (F12). The pattern this entry used to carry —
/// `/filename\s*=\s*([^;]+)/i` — cut the value at the FIRST `;` even inside quotes, so the common
/// honest header `attachment; filename="report; final.png"` produced `report.png`: the tail the
/// user sees in a browser was silently gone, and the file on disk was named after a different
/// document. RFC 6266 permits `;` inside a quoted-string, so a quoted value is read as a
/// quoted-string (with the `\"` escape RFC 7230 §3.2.6 defines) and only an UNQUOTED value may run
/// to the next parameter separator.
///
/// The alternatives are ordered quoted-double, quoted-single, bare, and the scan is leftmost-first,
/// so the FIRST parameter wins: `filename="a; filename=b.png"` names `a; filename=b.png` (the inner
/// words are part of the name), and `filename="b.png"; filename="a.png"` names `b.png` — which is
/// also what a browser does. An UNTERMINATED quote falls through to the bare branch, i.e. the
/// pre-fix behaviour, so a malformed header cannot lose the name it did yield before.
fn disposition_name(header: Option<&str>) -> Option<String> {
    let header = header?.trim();

    if header.is_empty() {
        return None;
    }

    if let Some(captures) = pattern(
        &DISPOSITION_EXTENDED,
        r#"(?i)filename\*\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^;]*))"#,
    )
    .captures(header)
    {
        // `UTF-8''name` — drop the charset/language prefix, keep the value.
        let value = captures
            .get(1)
            .or(captures.get(2))
            .map_or("", |matched| matched.as_str())
            .trim();
        let value = pattern(&EXTENDED_PREFIX, r"^[A-Za-z0-9-]*''")
            .replace(value, "")
            .to_string();

        let name = sanitize_name(&unquote(&value));

        if name.is_some() {
            return name;
        }
    }

    if let Some(captures) = pattern(
        &DISPOSITION_PLAIN,
        r#"(?i)filename\s*=\s*(?:"((?:[^"\\]|\\.)*)"|'([^']*)'|([^;]*))"#,
    )
    .captures(header)
    {
        let value = captures
            .get(1)
            .or(captures.get(2))
            .or(captures.get(3))
            .map_or("", |matched| matched.as_str())
            .trim();

        return sanitize_name(&unquote(value));
    }

    None
}

/// The URL's own basename (`urlName`, `download.tool.ts:981-989`), used when the host named
/// nothing. The PATHNAME only: a query string's `?file=x` is not a name, and neither is a fragment.
fn url_name(url: &str) -> Option<String> {
    let pathname = reqwest::Url::parse(url).ok()?.path().to_owned();

    pathname
        .split('/')
        .rfind(|segment| !segment.is_empty())
        .and_then(sanitize_name)
}

/// Final name: sanitized name + a MIME-derived extension when the name does not already carry one
/// (`resolveName`, `download.tool.ts:992-1009`). The 200-character budget is kept by SHORTENING THE
/// BASE, never the extension — a truncated `.pd` is a file nothing will open.
#[must_use]
pub fn resolve_name(
    requested: Option<&str>,
    url: &str,
    mime: &str,
    header: Option<&str>,
) -> String {
    let chosen = requested
        .and_then(sanitize_name)
        .or_else(|| disposition_name(header))
        .or_else(|| url_name(url))
        .unwrap_or_else(|| "download".to_owned());
    let extension = mime_extension(mime).unwrap_or("");

    if extension.is_empty() || chosen.to_lowercase().ends_with(extension) {
        return chosen;
    }

    let extension_length = extension.chars().count();
    let base_max = DOWNLOAD_MAX_NAME_CHARS.saturating_sub(extension_length);
    let base: String = if chosen.chars().count() > base_max {
        chosen.chars().take(base_max.max(1)).collect()
    } else {
        chosen.clone()
    };

    format!("{base}{extension}")
}

/// The `path` argument, checked BEFORE the transfer is paid for (`pathArgument`,
/// `download.tool.ts:322-339`).
///
/// A bad destination is an ARGUMENT error: degrading it into `saved: false` would let a model
/// believe it chose a location and got the default folder, and paying 20 MiB of transfer to
/// discover the argument was wrong wastes the very budget this tool exists to protect. Absolute
/// paths, a drive colon and any `..` segment are REFUSED rather than normalized, because the tool
/// documents a session folder and silently writing to `/etc` because a model guessed an absolute
/// path is worse than an error.
fn path_argument(requested: Option<&str>) -> Result<Option<String>, String> {
    let Some(raw) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let candidate = raw.replace('\\', "/");

    if candidate.chars().count() > DOWNLOAD_MAX_PATH_CHARS {
        return Err(format!(
            "download path must be at most {DOWNLOAD_MAX_PATH_CHARS} characters."
        ));
    }

    let escapes = candidate.starts_with('/')
        || pattern(&DRIVE_PREFIX, r"^[A-Za-z]:").is_match(&candidate)
        || candidate.split('/').any(|segment| segment == "..");

    if escapes {
        return Err(
            "download path must be relative to the session downloads folder, without \"..\"."
                .to_owned(),
        );
    }

    Ok(Some(candidate))
}

/// The per-session downloads directory: `<sessions_dir>/<session_id>/downloads`.
///
/// This is the crate's shape of the reference's host-supplied `downloadsDirectory`, and it is the
/// SAME directory the journal writes into (`src/journal.rs:354`), so `delete_session` — which
/// removes that directory (`src/store/session.rs:483`) — removes the downloads with it. That
/// coupling is the property F7 asked for: a model reading `saved: true` can trust that the path it
/// was given is inside something it can delete.
#[must_use]
pub fn session_downloads_directory(sessions_root: &Path, session_id: &str) -> PathBuf {
    sessions_root
        .join(session_id)
        .join(HARNESS_SESSION_DOWNLOADS_DIRECTORY)
}

/// The folder this call may write into, fail closed (`downloadsDirectoryOf`,
/// `download.tool.ts:294-311`). RESOLVED once here: every later containment test compares against
/// this value, and a relative host-supplied root would otherwise be resolved against a working
/// directory the caller does not control.
fn downloads_directory_of(
    sessions_root: Option<&Path>,
    session_id: Option<&str>,
) -> Result<PathBuf, String> {
    let id = session_id.unwrap_or("").trim();

    let root = sessions_root.filter(|root| !root.as_os_str().is_empty());

    if root.is_none() || id.is_empty() {
        return Err(DOWNLOAD_SESSION_REQUIRED.to_owned());
    }

    Ok(lexical_normalize(&session_downloads_directory(
        root.expect("checked above"),
        id,
    )))
}

/// `path.resolve`'s LEXICAL half, without touching the filesystem: `.` segments dropped and `..`
/// segments popped.
///
/// WHY IT IS NEEDED: the reference resolves the folder and compares STRINGS against the resolved
/// value; a Rust `Path::join` keeps `..` literally, which would let `a/../../etc` pass a
/// `starts_with` test while naming something outside the folder. Popping lexically is what makes
/// the containment check mean what it says. (Symlinks are the one thing no lexical check can see,
/// and the reference cannot see them either.)
#[must_use]
fn lexical_normalize(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    };

    let mut resolved = PathBuf::new();

    for segment in absolute.components() {
        match segment {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            other => {
                resolved.push(other.as_os_str());
            }
        }
    }

    resolved
}

/// The destination: the validated `path` inside the session folder, or the sanitized name directly
/// under it (`destinationOf`, `download.tool.ts:345-355`).
///
/// The name is already separator-free (see [`sanitize_name`]), so joining it cannot escape; the
/// `path` guard refuses `..`, and the containment test here is DEFENCE IN DEPTH against a separator
/// a platform spells differently — the write must stay inside the folder that is deleted with the
/// session.
fn destination_of(
    directory: &Path,
    name: &str,
    candidate: Option<&str>,
) -> Result<PathBuf, String> {
    let target = lexical_normalize(&directory.join(candidate.unwrap_or(name)));

    if target != directory && !target.starts_with(directory) {
        return Err("download path must stay inside the session downloads folder.".to_owned());
    }

    Ok(target)
}

/// The one-word reason the summary reports (`error.code` in the reference: `ENOENT`, `EEXIST`, …).
/// A stack trace and an absolute path are host noise the model has no use for; what it needs is
/// that the file is not on disk.
#[must_use]
fn io_error_code(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::NotFound => "ENOENT",
        ErrorKind::AlreadyExists => "EEXIST",
        ErrorKind::PermissionDenied => "EACCES",
        ErrorKind::NotADirectory => "ENOTDIR",
        ErrorKind::IsADirectory => "EISDIR",
        ErrorKind::StorageFull => "ENOSPC",
        ErrorKind::TimedOut => "ETIMEDOUT",
        _ => "write failed",
    }
}

/// Write the bytes WITHOUT CLOBBERING (`writeUnique`, `download.tool.ts:370-401`). Two downloads of
/// the same name would otherwise silently replace each other, and the model would read the wrong
/// bytes believing they were fresh.
///
/// `create_new(true)` is the existence check AND the reservation in one call, so there is no window
/// between testing and writing, and the real bytes are written once (a separate probe write would
/// leave an empty file behind if the payload write then failed).
///
/// The folder created is the destination's OWN directory, not merely the session folder: a `path`
/// argument may carry a subpath (`raw/report.csv`), and creating the leaf's parent is what makes
/// that subpath exist instead of failing after the whole transfer was paid for.
///
/// Returns the ABSOLUTE path written (the suffixing loop may have moved it): a relative path was
/// only ever meaningful inside the working directory this process happened to start in, and the
/// session folder is usually outside it.
async fn write_unique(absolute: &Path, bytes: &[u8]) -> Result<PathBuf, String> {
    use tokio::io::AsyncWriteExt;

    let parent = absolute.parent().unwrap_or(Path::new("."));

    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| io_error_code(error.kind()).to_owned())?;

    // The suffix is derived from the ORIGINAL LEAF name, never from the previous attempt and never
    // from the whole path: compounding produced `pixel-1-2-3.png` after a few downloads of the same
    // URL, which is unreadable and makes the name depend on run order. The LEAF matters now the base
    // is absolute — a dot in the SESSION path (`~/.local/…`) would otherwise read as the start of
    // the extension.
    let leaf = absolute
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".to_owned());
    let dot = leaf.rfind('.');
    let (stem, extension) = match dot {
        Some(at) if at > 0 => (leaf[..at].to_owned(), leaf[at..].to_owned()),
        _ => (leaf.clone(), String::new()),
    };

    for attempt in 0..=DOWNLOAD_UNIQUE_ATTEMPTS {
        let target = if attempt == 0 {
            absolute.to_path_buf()
        } else {
            parent.join(format!("{stem}-{attempt}{extension}"))
        };

        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .await
        {
            Ok(mut file) => {
                // `write_all` then `flush`: a partial write has to be an error, not a short file
                // the summary reports as saved.
                if let Err(error) = file.write_all(bytes).await {
                    return Err(io_error_code(error.kind()).to_owned());
                }

                if let Err(error) = file.flush().await {
                    return Err(io_error_code(error.kind()).to_owned());
                }

                return Ok(target);
            }
            // The one error this loop is waiting for: try the next suffix.
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error_code(error.kind()).to_owned()),
        }
    }

    // DEVIATION, stated: the reference swallows this sentence and reports the bare `write failed`
    // code, because its catch reads `error.code` and a HarnessError has none. The reason survives
    // here instead — same `saved: false`, more to act on.
    Err("download could not reserve a unique file name in the session downloads folder.".to_owned())
}

/// One download attempt's outcome, after the redirect chain and the byte cap.
struct DownloadTransfer {
    /// The URL the FINAL response answers for — the last hop, never the original.
    final_url: String,
    /// The MIME alone, with any `; charset=` parameter dropped.
    mime: String,
    /// The raw `content-disposition` value, kept verbatim because the name rule is a parsing rule.
    disposition: Option<String>,
    /// What the host DECLARED, reported so a caller can tell a lie about the size from an honest
    /// answer about a big file. `None` when the header was absent or not a number.
    declared: Option<u64>,
    bytes: Vec<u8>,
}

/// The header read, with the same rule the fetch pipeline uses: a value that is not valid UTF-8 is
/// absent, never guessed.
fn header_text(response: &reqwest::Response, name: reqwest::header::HeaderName) -> Option<String> {
    response
        .headers()
        .get(name)?
        .to_str()
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// The transfer, redirects followed BY HAND (gate 3). Every hop passes gates 1 and 2 BEFORE the
/// request is made, the status classes get their own sentences, the declared size is checked before
/// the body is read, and the cap is enforced while streaming — so an oversized or internal body
/// never reaches disk.
async fn download_transfer(
    url: &str,
    keys: &WebKeys,
    signal: &CancellationToken,
) -> Result<DownloadTransfer, String> {
    let client = download_client(keys)?;
    // The gate's resolver: injected answers first (the test seam, and a deployment in front of a
    // private DNS view), then the system resolver. `system: true` is the production state.
    let resolver = ssrf::Resolver {
        answers: keys.dns_answers.clone(),
        system: true,
    };

    // GATE 1 + GATE 2 on the first hop. Gate 2's refusal NAMES the address it resolved to
    // (`refusedUrl(firstHop.address ?? hostname)`, `download.tool.ts:1347`): "not public" without
    // the address cannot be acted on, since the caller already believes the name is public.
    if let Err(detail) = gate_download_url(url, &resolver).await {
        return Err(refused_url(detail.as_deref()));
    }

    let mut target = url.to_owned();
    let mut redirects = 0_usize;

    loop {
        let response = tokio::select! {
            biased;
            () = signal.cancelled() => return Err(CANCELLED_SENTENCE.to_owned()),
            outcome = client.get(&target).send() => outcome
                .map_err(|_| DOWNLOAD_UNREACHABLE.to_owned())?,
        };

        let status = response.status().as_u16();

        // A 3xx is a HOP, not an answer: `Location` is resolved, then gated, then asked for.
        if (300..400).contains(&status) {
            let location = header_text(&response, reqwest::header::LOCATION);
            let Some(next) = redirect_location(location.as_deref(), &target) else {
                return Err(refused_hop(status, None));
            };

            // Gate 1 + gate 2 on the NEW address: a public host may redirect into loopback, and the
            // address the first hop answered for says nothing about this one. The hop sentence
            // carries the address the same way the first-hop one does (`refusedHop(response.status,
            // hop.address)`, `download.tool.ts:1397`).
            if let Err(detail) = gate_download_url(&next, &resolver).await {
                return Err(refused_hop(status, detail.as_deref()));
            }

            redirects += 1;

            if redirects > MAX_REDIRECT_HOPS {
                return Err(format!(
                    "download stopped after {MAX_REDIRECT_HOPS} redirects."
                ));
            }

            target = next;
            continue;
        }

        if !(200..300).contains(&status) {
            return Err(download_status_failure(status));
        }

        let declared = header_text(&response, reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.parse::<u64>().ok());

        // The DECLARED size is checked before the body is read, so a host that announces 4 GiB does
        // not get to stream it at us. It is a header a hostile host can lie about, so it is a
        // PRE-CHECK and never the bound — the streaming loop below is the bound.
        if declared.is_some_and(|at| at > MAX_DOWNLOAD_BYTES) {
            return Err(download_oversize());
        }

        let mime = header_text(&response, reqwest::header::CONTENT_TYPE)
            .map(|value| value.split(';').next().unwrap_or("").trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DOWNLOAD_FALLBACK_MIME.to_owned());
        let disposition = header_text(&response, reqwest::header::CONTENT_DISPOSITION);
        let final_url = response.url().to_string();

        let mut bytes: Vec<u8> = Vec::new();
        let mut response = response;

        loop {
            let chunk = tokio::select! {
                biased;
                () = signal.cancelled() => return Err(CANCELLED_SENTENCE.to_owned()),
                outcome = response.chunk() => outcome
                    .map_err(|_| DOWNLOAD_UNREACHABLE.to_owned())?,
            };

            let Some(chunk) = chunk else {
                break;
            };

            // The bound is a `u64` byte count; the running total is a `usize` buffer length, so the
            // sum is WIDENED rather than the limit narrowed (a 32-bit host cannot overflow past the
            // limit unnoticed this way).
            if u64::try_from(bytes.len() + chunk.len()).unwrap_or(u64::MAX) > MAX_DOWNLOAD_BYTES {
                // Dropping the response body here cancels the stream, so the socket is not left
                // draining a payload nobody will read.
                return Err(download_oversize());
            }

            bytes.extend_from_slice(&chunk);
        }

        return Ok(DownloadTransfer {
            final_url,
            mime,
            disposition,
            declared,
            bytes,
        });
    }
}

/// Where one download may write, and who supplied that place.
///
/// `sessions_root` is the crate's sessions directory and `session_id` comes from the call, so the
/// folder is the session's OWN directory — the one `delete_session` removes — which is the property
/// the reference's F7 asked for. Both are `Option`s because the tool can be reached from a global
/// catalogue call with no session, and that case refuses BEFORE a byte is transferred
/// ([`DOWNLOAD_SESSION_REQUIRED`]).
pub struct DownloadContext<'a> {
    pub keys: &'a WebKeys,
    pub sessions_root: Option<&'a Path>,
    pub session_id: Option<&'a str>,
}

/// `download`: `{url, name?, path?}` -> the `toolMedia(summary, [attachment])` envelope.
///
/// The bytes travel ONCE, on the attachment; the model reads only the summary, and the event carries
/// descriptors. The cap is enforced against the STREAMED body, so an oversized page never lands on
/// disk, and the destination is proven before the transfer is paid for.
///
/// # Errors
///
/// A bad URL, the session/`path` argument refusals, an SSRF refusal on any hop, a status sentence,
/// the size refusal, or the timeout. A failed WRITE is not an error: it is `saved: false` plus
/// `saveError`, because the bytes still reached the model.
pub(crate) async fn download(
    arguments: &Value,
    context: &DownloadContext<'_>,
    signal: &CancellationToken,
) -> Result<Value, String> {
    // ---- proven BEFORE the transfer -------------------------------------------------
    let requested = super::result::required_text(arguments, "url", "download")?;
    let url = validated_url(&requested).map_err(|_| DOWNLOAD_URL_INVALID.to_owned())?;

    // The destination is proven before the transfer is paid for. A harness with no session folder,
    // or a `path` that would escape it, is a configuration/argument error: refusing after 20 MiB has
    // arrived wastes the very budget this tool exists to protect, and writing somewhere the caller
    // never named is the defect that rule closed.
    let directory = downloads_directory_of(context.sessions_root, context.session_id)?;
    let subpath = path_argument(
        super::result::optional_text(arguments, "path")
            .as_deref()
            .map(str::to_owned)
            .as_deref(),
    )?;

    // ---- the transfer, gated and capped ---------------------------------------------
    // The controller's own budget: 60 s for the whole call, so a slow start, a slow body and a slow
    // resolve all spend from one figure. A caller cancellation wins over the deadline (the `select!`
    // inside the transfer reports the cancellation as a cancellation, never as a timeout).
    let transfer = tokio::time::timeout(
        Duration::from_millis(DOWNLOAD_TIMEOUT_MS),
        download_transfer(&url, context.keys, signal),
    )
    .await
    .map_err(|_| format!("download timed out after {DOWNLOAD_TIMEOUT_MS} ms."))??;

    // `response.url` after a hand-followed chain is the LAST hop, which is the honest answer for
    // where the bytes came from. An empty one (a minimal runtime, a hand-built response) falls back
    // to the last URL we asked for — never the original.
    let final_url = if transfer.final_url.is_empty() {
        url.clone()
    } else {
        transfer.final_url.clone()
    };
    let mime = transfer.mime.clone();
    let kind = kind_for_mime(&mime);
    let name = resolve_name(
        super::result::optional_text(arguments, "name")
            .as_deref()
            .map(str::to_owned)
            .as_deref(),
        &final_url,
        &mime,
        transfer.disposition.as_deref(),
    );
    let size = u64::try_from(transfer.bytes.len()).unwrap_or(u64::MAX);

    // `[Image 1]`-style markers come from the attachment kind, the same rule the composer uses
    // (`shared/attachments.ts` `attachmentMarker`). `dl-1` is deterministic: one attachment per
    // call, and the transport re-suffixes the id only when one block aggregates several downloads.
    let attachment = HarnessAttachment {
        id: "dl-1".to_owned(),
        kind,
        marker: format!(
            "[{} 1]",
            match kind {
                AttachmentKind::Image => "Image",
                AttachmentKind::Video => "Video",
                AttachmentKind::Audio => "Audio",
                AttachmentKind::Pdf => "PDF",
                AttachmentKind::File => "File",
            }
        ),
        name: name.clone(),
        mime: mime.clone(),
        size,
        data: BASE64.encode(&transfer.bytes),
    };

    // ---- the on-disk copy, deliberately AFTER the attachment and never fatal ---------
    // A host with no writable filesystem still gets its bytes, and says so. Only the I/O may fail
    // softly: a bad `path` SHAPE was refused above, and the containment check below is loud too,
    // because degrading it into `saved: false` would let a model believe it chose a location and got
    // the default folder.
    let wanted = destination_of(&directory, &name, subpath.as_deref())?;
    let (saved, saved_path, save_error) = match write_unique(&wanted, &transfer.bytes).await {
        Ok(path) => (true, Some(path), None),
        Err(reason) => (false, None, Some(reason)),
    };

    // Honest affordance: `grep` skips binaries and files over 1 MiB, so a saved JPEG is addressable
    // but NOT searchable, and telling the model which one it has saves a wasted grep turn.
    let searchable = is_searchable_text(&mime) && size <= DOWNLOAD_GREP_MAX_FILE_BYTES;
    // `noteFor` (`download.tool.ts:1105-1131`) runs BEFORE the summary is assembled, so the one
    // sentence the model reads is derived from the same three facts the summary reports.
    let note = note_for(saved, searchable, save_error.as_deref());

    let mut summary = Map::new();

    for (key, value) in [
        ("url", json!(url)),
        ("finalUrl", json!(final_url)),
        ("contentType", json!(mime)),
        ("kind", json!(kind)),
        ("name", json!(name)),
        ("bytes", json!(transfer.bytes.len())),
        (
            "declaredBytes",
            transfer.declared.map_or(Value::Null, |at| json!(at)),
        ),
        ("media", json!("attached")),
        ("mediaId", json!(attachment.id)),
        ("saved", json!(saved)),
        (
            "path",
            saved_path.map_or(Value::Null, |path| json!(path.to_string_lossy())),
        ),
        (
            "saveError",
            save_error
                .as_ref()
                .map_or(Value::Null, |reason| json!(reason)),
        ),
        ("searchable", json!(searchable)),
        ("note", json!(note)),
    ] {
        // A key whose value is `Null` here is an ABSENT key in the published shape
        // (`...(condition ? { key: value } : {})` in the reference), and the difference matters:
        // `path: null` would read as "there is a path and it is empty".
        if value.is_null() {
            continue;
        }

        summary.insert(key.to_owned(), value);
    }

    // The envelope is what makes the bytes model input: the transport validates against the 20 MiB
    // download profile and the harness stores the result on this call's tool message.
    Ok(json!({
        TOOL_MEDIA_BRAND: [attachment],
        "content": Value::Object(summary),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_url_is_validated_before_the_network_sees_it() {
        assert_eq!(
            validated_url("ftp://x/y").expect_err("refused"),
            "url must be an absolute http(s) URL; received \"ftp://x/y\"."
        );
        assert_eq!(
            validated_url("https://user:pw@host/x").expect_err("refused"),
            "url must not carry credentials; pass the public URL."
        );
        assert_eq!(
            validated_url("  https://host/x  ").expect("accepted"),
            "https://host/x".to_owned()
        );
    }

    #[test]
    fn mime_decides_the_attachment_kind() {
        assert_eq!(kind_for_mime("image/png; charset=x"), AttachmentKind::Image);
        assert_eq!(kind_for_mime("application/pdf"), AttachmentKind::Pdf);
        assert_eq!(kind_for_mime("text/plain"), AttachmentKind::File);
    }

    #[test]
    fn a_file_name_is_stripped_of_path_and_control_text() {
        // Basename FIRST (either separator), then the characters a file label cannot hold. The
        // old port deleted the separators and kept `c:d?`; the reference keeps only the last
        // segment and drops `:`, `?`, `<`, `>`, `|` and `*` from it.
        assert_eq!(sanitize_name("a/b\\c:d?").as_deref(), Some("cd"));
        // A space is legal in a label, so it survives — the reference only trims the ends.
        assert_eq!(
            sanitize_name("  report final.pdf  ").as_deref(),
            Some("report final.pdf")
        );
        // A name that is only dots is not a name, and a control character is not a character.
        assert!(sanitize_name("...").is_none());
        assert_eq!(sanitize_name("a\u{0}b.txt").as_deref(), Some("ab.txt"));
        assert!(
            sanitize_name("  ").is_none(),
            "a name that is only whitespace is no name at all"
        );

        // `decodeURIComponent` runs BEFORE sanitizing (`:1006`), so a percent-escape is a
        // path separator first and a slash-separated name has only its LEAF kept.
        assert_eq!(
            url_name("https://h/a/b/shot%2Epng?v=1").as_deref(),
            Some("shot.png")
        );
        assert_eq!(
            url_name("https://h/a/b/shot.png?v=1").as_deref(),
            Some("shot.png")
        );
    }

    #[test]
    fn html_becomes_readable_text_and_keeps_its_title() {
        let html = "<!doctype html><html><head><title>  A   Title </title>\
                     <style>body{color:red}</style></head>\
                     <body><script>var a = \"<b>hi</b>\";</script><p>One   two</p><div>three</div></body></html>";

        assert_eq!(extract_title(html).as_deref(), Some("A Title"));
        assert_eq!(html_to_text(html), "One two three");
    }

    #[test]
    fn status_sentences_match_the_typescript_wording() {
        assert_eq!(
            status_sentence(404),
            "fetch could not find the page (HTTP 404)."
        );
        assert_eq!(
            status_sentence(403),
            "fetch was denied access to the page (HTTP 403)."
        );
        assert_eq!(
            status_sentence(429),
            "fetch is rate limited by the host (HTTP 429). Try again later."
        );
        assert_eq!(status_sentence(500), "fetch failed (HTTP 500).");
    }
    // ── search (`tools/websearch/websearch.test.ts`, ported row for row) ────────────────
    //
    #[test]
    fn entities_decode_the_way_native_ts_declares() {
        // `native.ts:39-64`: every one of the 26 named entries decodes, and the four space
        // entities are a plain U+0020 so a later whitespace collapse treats them as spaces.
        assert_eq!(
            decode_entities("A &amp; B &lt;b&gt; &quot;q&quot; &apos;x&apos;"),
            "A & B <b> \"q\" 'x'"
        );
        // Decoding does NOT collapse whitespace: `html_to_text` owns collapsing, so the four
        // distinct space entities really do leave four distinct spaces behind.
        assert_eq!(
            decode_entities("Three&nbsp;four &ensp;gap&emsp;&thinsp;"),
            "Three four  gap  "
        );
        assert_eq!(
            decode_entities(
                "&copy;&reg;&trade;&hellip;&mdash;&ndash;&lsquo;&rsquo;&ldquo;&rdquo;&bull;&middot;&deg;&laquo;&raquo;&times;&divide;"
            ),
            "\u{a9}\u{ae}\u{2122}\u{2026}\u{2014}\u{2013}\u{2018}\u{2019}\u{201c}\u{201d}\u{2022}\u{b7}\u{b0}\u{ab}\u{bb}\u{d7}\u{f7}"
        );

        // Numeric first, then named, and the trailing semicolon is OPTIONAL in all three
        // forms. Control codes collapse to nothing, a lone surrogate becomes U+FFFD, and a
        // reference the table does not know stays exactly as the page wrote it.
        assert_eq!(decode_entities("&#65;&#x42;&#x4;A&#xD800;"), "ABA\u{fffd}");
        // The dropped semicolon leaves its trailing space behind, so three entities with
        // no semicolons really do yield three extra spaces.
        assert_eq!(decode_entities("&#65 &#x42 &nbsp x"), "A B   x");
        // Only a reference the table does not KNOW survives; a numeric one outside
        // the scalar range is dropped, exactly as `native.ts:74` returns `''` for it.
        assert_eq!(decode_entities("&nope; &#x110000;"), "&nope; ");

        // An SVG body stays readable text, which is why `image/svg+xml` is not in the image
        // refusal list (`webfetch.test.ts:543` asserts `<svg><text>readable</text></svg>`).
        assert!(readable_mime("image/svg+xml"));
        assert!(!readable_mime("image/png"));
        // The gate reads the MIME, never the charset: `charsetFrom`/`decoderFor` are the part
        // of `request.ts` this port does not implement, so a declared `utf-16` body is still
        // accepted and read as UTF-8 rather than refused.
        assert!(readable_mime("text/plain; charset=utf-8"));
        assert!(readable_mime("text/plain; charset=utf-16"));
    }

    #[tokio::test]
    async fn a_page_becomes_readable_text_under_the_published_result() {
        let body = "<html><head><title>A &amp; B <span>Title</span></title><style>p{color:red}</style>\
                    </head><body><script>var a = \"<b>hi</b>\";</script><h1>Heading</h1>\
                    <p>One   two</p><p>Three&nbsp;four</p></body></html>";
        let (base, _seen, server) =
            fetch_server(&[fetch_reply("text/html; charset=utf-8", body)]).await;
        let outcome = fetch(
            &reqwest::Client::new(),
            &json!({"url": format!("{base}/page")}),
            &fetch_keys(&base),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        // The published key set for a plain read (`webfetch/index.ts:647-678`): the page
        // reports what it is (`contentType`), how much it holds (`pageCharacters`), and where
        // this window starts (`offset`). `title` is the <title> BEFORE tag stripping, with
        // nested tags dropped, entities decoded and whitespace collapsed; `content` is the
        // same conversion the unit test pins (`One two three`).
        assert_eq!(
            outcome,
            json!({
                "provider": "native",
                "url": format!("{base}/page"),
                "contentType": "text/html; charset=utf-8",
                "format": "markdown",
                "characters": 26,
                "truncated": false,
                // The converter is noobscript's own reduction of `native.ts`'s markdown
                // conversion (documented in the module header): tags collapse to spaces, not
                // to markdown syntax, and `&nbsp;` is a plain space like the reference's
                // table says.
                "content": "Heading One two Three four",
                "pageCharacters": 26,
                "offset": 0,
                "title": "A & B Title",
            })
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_query_selects_the_matching_section_and_reports_the_heading_mode() {
        let body =
            "# Setup\n\ninstall the crate\n\n# Usage\n\nrun the tool\n\n# Notes\n\nnothing else";
        let (base, _seen, server) =
            fetch_server(&[fetch_reply("text/plain; charset=utf-8", body)]).await;

        // Three headings is one more than `FETCH_FLAT_DOCUMENT_SECTIONS`, so the filter is a
        // real filter and `matchMode` may claim `heading`. A section's text includes its
        // heading line (`sections.ts:107` slices from `section.start`), which is why the
        // joined block repeats it under the `## heading [line N]` label.
        assert_eq!(
            fetch(
                &reqwest::Client::new(),
                &json!({"url": format!("{base}/doc"), "query": "usage"}),
                &fetch_keys(&base),
                &CancellationToken::new(),
            )
            .await
            .unwrap(),
            json!({
                "provider": "native",
                "url": format!("{base}/doc"),
                "contentType": "text/plain; charset=utf-8",
                "format": "markdown",
                "characters": 39,
                "truncated": false,
                "content": "## Usage [line 5]\n# Usage\n\nrun the tool",
                "query": "usage",
                "matches": 1,
                "totalSections": 3,
                "totalMatches": 1,
                "matchMode": "heading",
                "pageCharacters": 72,
            })
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_flat_page_answers_a_query_with_a_proximity_window_and_says_so() {
        let body = "alpha one\nbeta two needle three\ngamma four";
        let (base, _seen, server) = fetch_server(&[fetch_reply("text/plain", body)]).await;

        // No heading at all: one pseudo-section, so `selectSections` would return the whole
        // page while claiming `matches: 1`. The window mode answers the same question and the
        // NOTE is what tells the caller it did not get a section.
        assert_eq!(
            fetch(
                &reqwest::Client::new(),
                &json!({"url": format!("{base}/flat"), "query": "needle"}),
                &fetch_keys(&base),
                &CancellationToken::new(),
            )
            .await
            .unwrap(),
            json!({
                "provider": "native",
                "url": format!("{base}/flat"),
                "contentType": "text/plain",
                "format": "markdown",
                "characters": 67,
                "truncated": false,
                "content": "## (no heading) [line 2]\nalpha one\nbeta two needle three\ngamma four",
                "query": "needle",
                "matches": 1,
                "totalSections": 1,
                "totalMatches": 1,
                "matchMode": "proximity",
                // The window was NOT cut by the size limit, so this is `PROXIMITY_NOTE_WHOLE`:
                // no `nextOffset` is reported, and the sentence says so instead of sending the
                // model to an `offset` that `query` ignores.
                "note": "This page has no heading structure, so \"query\" returned a text window around the first match instead of a section. No \"nextOffset\" is reported, so there is nothing to resume: to read other parts of the page, ask a different \"query\" phrase, or re-fetch without \"query\" and page with \"offset\".",
                "pageCharacters": 42,
            })
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_query_with_no_match_refuses_instead_of_returning_the_whole_page() {
        let body =
            "# Setup\n\ninstall the crate\n\n# Usage\n\nrun the tool\n\n# Notes\n\nnothing else";
        let (base, _seen, server) = fetch_server(&[fetch_reply("text/plain", body)]).await;

        // The refusal quotes the page size the caller paged with, because "no match" on a
        // 72-character page is a different answer from "no match" on a 240 000 one.
        assert_eq!(
            fetch(
                &reqwest::Client::new(),
                &json!({"url": format!("{base}/doc"), "query": "deploy"}),
                &fetch_keys(&base),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err(),
            "fetch found no section matching \"deploy\" in a 72-character page. Re-fetch without \"query\" to read the whole page, or use a shorter phrase."
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_long_page_pages_by_character_offsets_and_fits_the_result_budget() {
        // 70 000 characters: inside the 1 MiB response budget, past the 63 488-byte
        // model-facing content cap, and under the 240 000-character page cap, so this single
        // document exercises all three limits.
        let body = "a".repeat(70_000);
        let (base, _seen, server) = fetch_server(&[fetch_reply("text/plain", &body)]).await;
        let client = reqwest::Client::new();
        let keys = fetch_keys(&base);
        let signal = CancellationToken::new();

        let first = fetch(
            &client,
            &json!({"url": format!("{base}/long")}),
            &keys,
            &signal,
        )
        .await
        .unwrap();
        assert_eq!(first["characters"], json!(FETCH_MAX_RESULT_CONTENT_BYTES));
        assert_eq!(
            first["content"].as_str().unwrap().len(),
            FETCH_MAX_RESULT_CONTENT_BYTES
        );
        assert_eq!(first["truncated"], json!(true));
        assert_eq!(first["offset"], json!(0));
        assert_eq!(first["nextOffset"], json!(FETCH_MAX_RESULT_CONTENT_BYTES));
        assert_eq!(first["pageCharacters"], json!(70_000));
        assert!(first.get("offsetClamped").is_none());

        // The delivery ceiling is the point of this test, so it is asserted rather than
        // implied: the WHOLE document — every key, not just `content` — stays inside the cap the
        // harness refuses above, which is the number `bound_tool_result` charges.
        assert!(
            payload::document_wire_bytes(&first) <= FETCH_MAX_RESULT_BYTES,
            "a fetch result must never be JSON-refused by the delivery cap"
        );

        // Paging from `nextOffset` returns the NEXT window, not the rest of the document: at
        // 30 720 bytes of content a 70 000-character page takes THREE calls, and the middle one
        // is truncated too. The figures below are exact because the body is ASCII, where one
        // character is one byte; `offset` and `nextOffset` count code units, while the budget the
        // content was cut against counts escaped bytes, and only a non-ASCII page can separate
        // them (see the two-byte case further down).
        let second = fetch(
            &client,
            &json!({"url": format!("{base}/long"), "offset": FETCH_MAX_RESULT_CONTENT_BYTES}),
            &keys,
            &signal,
        )
        .await
        .unwrap();
        assert_eq!(second["characters"], json!(FETCH_MAX_RESULT_CONTENT_BYTES));
        assert_eq!(second["truncated"], json!(true));
        assert_eq!(second["offset"], json!(FETCH_MAX_RESULT_CONTENT_BYTES));
        assert_eq!(
            second["nextOffset"],
            json!(2 * FETCH_MAX_RESULT_CONTENT_BYTES)
        );

        // The last window ends EXACTLY at the end of the document, and that is NOT truncation:
        // `truncated` is computed from the source length, which is what fixes the off-by-one
        // where a page cut exactly at the budget claimed to be whole.
        let third = fetch(
            &client,
            &json!({"url": format!("{base}/long"), "offset": 2 * FETCH_MAX_RESULT_CONTENT_BYTES}),
            &keys,
            &signal,
        )
        .await
        .unwrap();
        assert_eq!(
            third["characters"],
            json!(70_000 - (2 * FETCH_MAX_RESULT_CONTENT_BYTES))
        );
        assert_eq!(third["truncated"], json!(false));
        assert_eq!(third["offset"], json!(2 * FETCH_MAX_RESULT_CONTENT_BYTES));
        assert!(third.get("nextOffset").is_none());

        // A page of two-byte characters is where the two currencies separate: the budget spends
        // BYTES, `characters` reports code units, and the two must not be confused. 40 000 bytes
        // of `é` is 20 000 characters, and the first window costs 30 720 bytes — 15 360
        // characters — while `pageCharacters` still counts characters.
        let accents = "\u{e9}".repeat(20_000);
        let (wide_base, _wide_seen, wide_server) =
            fetch_server(&[fetch_reply("text/plain", &accents)]).await;
        let wide = fetch(
            &client,
            &json!({"url": format!("{wide_base}/wide")}),
            &fetch_keys(&wide_base),
            &signal,
        )
        .await
        .unwrap();

        // The fit steps back in 5% of `keep`, so the landing point is exact arithmetic, not an
        // approximation: 20 000 -> 19 000 -> 18 050 -> 17 148 -> 16 291 -> 15 477 -> 14 704, and
        // 15 477 characters is 30 954 bytes, one step too many over the 30 720-byte budget.
        assert_eq!(wide["pageCharacters"], json!(20_000));
        assert_eq!(wide["characters"], json!(14_704));
        assert_eq!(wide["content"].as_str().expect("text").len(), 29_408);
        assert_eq!(wide["truncated"], json!(true));
        assert!(
            payload::document_wire_bytes(&wide) <= FETCH_MAX_RESULT_BYTES,
            "a multi-byte page has to fit the same cap as an ASCII one"
        );
        wide_server.abort();

        // An offset past the end is a model reaching slightly too far: it is clamped, and the
        // clamp is REPORTED, because an empty string with `truncated: false` used to be
        // indistinguishable from "this page has no content".
        let clamped = fetch(
            &client,
            &json!({"url": format!("{base}/long"), "offset": 80_000}),
            &keys,
            &signal,
        )
        .await
        .unwrap();
        assert_eq!(clamped["offset"], json!(70_000));
        assert_eq!(clamped["offsetClamped"], json!(true));
        assert_eq!(clamped["characters"], json!(0));
        assert_eq!(clamped["content"], json!(""));
        assert_eq!(clamped["truncated"], json!(false));
        assert_eq!(
            clamped["note"],
            json!(
                "Requested offset 80000 is past the end of this 70000-character page; showing what remains from 70000. The page is fully read once \"content\" is empty."
            )
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_cloudflare_challenge_is_retried_once_with_the_honest_agent() {
        // `request.ts:529-531`: 403 plus `cf-mitigated: challenge` is the only signature that
        // earns a retry, and the retry is the SAME GET with an honest UA. A plain 403 keeps
        // its status sentence (see the status test), so this test needs the header.
        let (base, seen, server) = fetch_server(&[
            "HTTP/1.1 403 Forbidden\r\ncf-mitigated: challenge\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned(),
            fetch_reply("text/html", "<html><body>recovered</body></html>"),
        ])
        .await;

        assert_eq!(
            fetch(
                &reqwest::Client::new(),
                &json!({"url": format!("{base}/guard")}),
                &fetch_keys(&base),
                &CancellationToken::new(),
            )
            .await
            .unwrap()["content"],
            json!("recovered")
        );

        let requests = seen.lock().expect("an uncontended lock").clone();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].contains(&format!("user-agent: {FETCH_BROWSER_USER_AGENT}")));
        assert!(requests[1].contains(&format!("user-agent: {FETCH_HONEST_USER_AGENT}")));
        server.abort();
    }

    #[tokio::test]
    async fn a_status_page_names_its_class_and_an_unreadable_body_names_its_type() {
        let client = reqwest::Client::new();
        let signal = CancellationToken::new();

        for (status, expected) in [
            ("404 Not Found", "fetch could not find the page (HTTP 404)."),
            (
                "429 Too Many Requests",
                "fetch is rate limited by the host (HTTP 429). Try again later.",
            ),
            ("500 Internal Server Error", "fetch failed (HTTP 500)."),
        ] {
            let (base, _seen, server) = fetch_server(&[format!(
                "HTTP/1.1 {status}\r\ncontent-type: text/html\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )])
            .await;
            assert_eq!(
                fetch(
                    &client,
                    &json!({"url": format!("{base}/gone")}),
                    &fetch_keys(&base),
                    &signal
                )
                .await
                .unwrap_err(),
                expected
            );
            server.abort();
        }

        // An unreadable body is refused BY NAME (`request.ts:717`), charset stripped, because
        // "unsupported type" gives the model nothing to act on. The charset is also what makes
        // `text/plain; charset=utf-16` unreadable rather than silently mis-decoded.
        let (base, _seen, server) = fetch_server(&[
            "HTTP/1.1 200 OK\r\ncontent-type: image/png\r\ncontent-length: 3\r\nconnection: close\r\n\r\nabc".to_owned()
        ])
        .await;
        assert_eq!(
            fetch(
                &client,
                &json!({"url": format!("{base}/x")}),
                &fetch_keys(&base),
                &signal
            )
            .await
            .unwrap_err(),
            "fetch does not support this content type (image/png): use the `download` tool for binary content."
        );
        server.abort();
    }

    #[tokio::test]
    async fn an_oversized_page_is_refused_with_the_reference_sentence() {
        // Declared up front, so the refusal costs no body at all: the sentence names the size
        // AND the remedies, because a page that will not fit is a file, not an article.
        let body = "a".repeat(FETCH_MAX_RESPONSE_BYTES + 1);
        let (base, _seen, server) = fetch_server(&[fetch_reply("text/plain", &body)]).await;
        assert_eq!(
            fetch(
                &reqwest::Client::new(),
                &json!({"url": format!("{base}/big")}),
                &fetch_keys(&base),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err(),
            format!(
                "fetch received a response larger than {FETCH_MAX_RESPONSE_BYTES} bytes. Try the plain-text or raw source form of the page if the host offers one (for example .txt, or a \"?action=raw\" / print view), ask for fewer characters with \"query\", or use the `download` tool to save the bytes and read them with grep and read."
            )
        );
        server.abort();
    }

    #[tokio::test]
    async fn the_fetch_gates_refuse_before_the_network_with_the_reference_sentences() {
        // An unreachable origin proves every one of these is a pre-flight refusal.
        let base = "http://127.0.0.1:1";
        let client = reqwest::Client::new();
        let keys = fetch_keys(base);
        let signal = CancellationToken::new();

        let cases: Vec<(Value, &str)> = vec![
            (
                json!({}),
                "fetch requires an absolute http(s) URL without credentials.",
            ),
            (
                json!({"url": ""}),
                "fetch requires an absolute http(s) URL without credentials.",
            ),
            (
                json!({"url": "not-a-url"}),
                "fetch requires an absolute http(s) URL without credentials.",
            ),
            (
                json!({"url": "https://user:pw@x.test/a"}),
                "fetch requires an absolute http(s) URL without credentials.",
            ),
            (
                json!({"url": "https://x.test/a.zip"}),
                "fetch does not read .zip files: use the `download` tool for binary content.",
            ),
            (
                json!({"url": "https://x.test/a", "format": "plain"}),
                "fetch format must be \"markdown\", \"text\", or \"html\".",
            ),
            (
                json!({"url": "https://x.test/a", "provider": "exa"}),
                "fetch provider \"exa\" is not configured. Configured: native.",
            ),
            (
                json!({"url": "https://x.test/a", "query": 3}),
                "fetch query must be a string.",
            ),
            (
                json!({"url": "https://x.test/a", "query": "y".repeat(201)}),
                "fetch query must be at most 200 characters.",
            ),
            (
                json!({"url": "https://x.test/a", "offset": -1}),
                "fetch offset must be a non-negative number.",
            ),
            (
                json!({"url": "https://x.test/a", "offset": "x"}),
                "fetch offset must be a non-negative number.",
            ),
        ];

        for (arguments, expected) in cases {
            assert_eq!(
                fetch(&client, &arguments, &keys, &signal)
                    .await
                    .unwrap_err(),
                expected,
                "{arguments}"
            );
        }

        // A blank query is not an error, it is "no query": the whole page comes back. Proving
        // that needs a live origin, so it is asserted separately below.
        let (base, _seen, server) = fetch_server(&[fetch_reply("text/plain", "just text")]).await;
        let keys = fetch_keys(&base);
        let outcome = fetch(
            &client,
            &json!({"url": format!("{base}/blank"), "query": "   "}),
            &keys,
            &signal,
        )
        .await
        .unwrap();
        assert_eq!(outcome["content"], json!("just text"));
        assert!(outcome.get("query").is_none());
        assert!(outcome.get("matchMode").is_none());
        server.abort();
    }

    /// Points the fetch seam at a stub origin, the same way `endpoint_base` redirects a
    /// search provider.
    fn fetch_keys(base: &str) -> WebKeys {
        WebKeys {
            endpoint_base: Some(base.to_owned()),
            ..WebKeys::default()
        }
    }

    /// One 200 with `body` under `content_type`. `connection: close` is load-bearing: without
    /// it a pooled client reuses the socket and a later request never reaches the next reply.
    fn fetch_reply(content_type: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// A multi-request HTTP/1.1 origin for the fetch tool: `replies` is served in order, one
    /// per connection, and cycles, so one test can stage a Cloudflare challenge followed by
    /// the real page. Every request's head line and headers are recorded for UA assertions.
    async fn fetch_server(
        replies: &[String],
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port");
        let address = listener.local_addr().expect("a bound address");
        let replies = replies.to_vec();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let task = {
            let seen = std::sync::Arc::clone(&seen);

            tokio::spawn(async move {
                let mut index = 0_usize;

                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };

                    let mut request: Vec<u8> = Vec::new();
                    let mut prefix = [0_u8; 4_096];

                    // A GET carries no body, so the header block is the whole request.
                    loop {
                        let read = socket.read(&mut prefix).await.unwrap_or(0);

                        if read == 0 {
                            break;
                        }

                        request.extend_from_slice(&prefix[..read]);

                        if String::from_utf8_lossy(&request).contains("\r\n\r\n") {
                            break;
                        }
                    }

                    if let Ok(mut guard) = seen.lock() {
                        guard.push(String::from_utf8_lossy(&request).into_owned());
                    }

                    let _ = socket
                        .write_all(replies[index % replies.len()].as_bytes())
                        .await;
                    let _ = socket.shutdown().await;
                    index += 1;
                }
            })
        };

        (format!("http://{address}"), seen, task)
    }

    // The reference stubs `globalThis.fetch`; this port drives the same requests against a
    // one-shot HTTP/1.1 origin on an ephemeral port, reached through `WebKeys::endpoint_base`.
    // Every assertion names the exact published value, never a range.

    /// Starts a stub provider origin that answers exactly one request with `reply` and sends
    /// back the request line, headers and body it received.
    /// One-shot origin: the base URL, the request the stub saw, and its accept TASK. The task is
    /// part of the return because a dropped handle keeps the listener alive: a test that runs
    /// several provider calls in one process has to `abort` it, or the next call's connection can
    /// be accepted by the previous test's stub.
    #[allow(clippy::type_complexity)]
    async fn stub_origin(
        reply: &str,
    ) -> (
        String,
        tokio::sync::oneshot::Receiver<String>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port");
        let address = listener.local_addr().expect("a bound address");
        let reply = reply.to_owned();

        let (sender, receiver) = tokio::sync::oneshot::channel();

        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("one connection");
            let mut seen: Vec<u8> = Vec::new();
            let mut prefix = [0_u8; 4_096];

            // Read until the headers are complete, then exactly `content-length` more bytes,
            // so the request is reported whole without waiting for a close that a pooled
            // client does not send.
            loop {
                let read = socket.read(&mut prefix).await.unwrap_or(0);

                if read == 0 {
                    break;
                }

                seen.extend_from_slice(&prefix[..read]);

                let text = String::from_utf8_lossy(&seen).into_owned();

                if let Some(head) = text.find("\r\n\r\n") {
                    let headers = text[..head].to_lowercase();
                    let declared: usize = headers
                        .lines()
                        .find(|line| line.starts_with("content-length:"))
                        .and_then(|line| line.split(':').nth(1))
                        .and_then(|value| value.trim().parse().ok())
                        .unwrap_or(0);

                    if seen.len() >= head + 4 + declared {
                        break;
                    }
                }
            }

            let _ = socket.write_all(reply.as_bytes()).await;
            let _ = socket.shutdown().await;
            let _ = sender.send(String::from_utf8_lossy(&seen).into_owned());
        });

        // The accept task is returned so a test that runs SEVERAL provider calls in one process
        // does not leave each listener accepting forever: `abort` closes the socket, and a
        // dangling accept task would hold the next call's connection.
        (format!("http://{address}"), receiver, task)
    }

    /// The published 200 for a provider answer.
    fn reply_with(payload: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
            payload.len()
        )
    }

    /// Keys for one provider, pointed at a stub origin.
    fn stubbed_keys(provider: &str, key: &str, base: &str) -> WebKeys {
        WebKeys {
            exa: (provider == "exa").then(|| key.to_owned()),
            tavily: (provider == "tavily").then(|| key.to_owned()),
            linkup: (provider == "linkup").then(|| key.to_owned()),
            endpoint_base: Some(base.to_owned()),
            // Search never resolves, so the download-only SSRF seams stay empty here.
            ..WebKeys::default()
        }
    }

    /// Runs `search` against a stub and returns the published value plus the raw request.
    async fn stubbed_search(
        provider: &str,
        key: &str,
        payload: &str,
        arguments: Value,
    ) -> (Value, String) {
        let (base, receiver, _server) = stub_origin(&reply_with(payload)).await;
        let client = reqwest::Client::new();
        let keys = stubbed_keys(provider, key, &base);

        let outcome = search(
            &client,
            &arguments,
            &keys,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|error| panic!("the stub call must succeed: {error}"));

        let request = tokio::time::timeout(std::time::Duration::from_secs(5), receiver)
            .await
            .expect("the stub sees one request")
            .expect("the request is reported");

        (outcome, request)
    }

    /// The JSON body of a captured request.
    fn request_body(request: &str) -> Value {
        let head = request.find("\r\n\r\n").expect("a complete request");

        serde_json::from_str(&request[head + 4..]).expect("a JSON body")
    }

    /// One captured header, lowercased key.
    fn request_header(request: &str, name: &str) -> Option<String> {
        request
            .lines()
            .find(|line| line.to_lowercase().starts_with(&format!("{name}:")))
            .map(|line| {
                line.split_once(':')
                    .map_or("", |(_, value)| value.trim())
                    .to_owned()
            })
    }

    /// `websearch.test.ts:264-301`: whitespace collapses, highlights join with a space, the
    /// publication date is carried, and the envelope echoes provider/query/maxResults.
    #[tokio::test]
    async fn one_provider_row_per_rule_becomes_the_published_shape() {
        let (outcome, request) = stubbed_search(
            "exa",
            "exa-secret-key",
            r#"{"results":[
                {"title":"  First   result ","url":"https://example.com/a","highlights":["one","two"],"publishedDate":"2026-01-02"},
                {"title":"Second","url":"https://example.com/b","highlights":["x"]}
            ]}"#,
            json!({ "query": "harness tools" }),
        )
        .await;

        assert_eq!(
            outcome,
            json!({
                "provider": "exa",
                "query": "harness tools",
                "maxResults": 5,
                "results": [
                    {
                        "title": "First result",
                        "url": "https://example.com/a",
                        "snippet": "one two",
                        "published": "2026-01-02",
                    },
                    { "title": "Second", "url": "https://example.com/b", "snippet": "x" },
                ],
            })
        );

        // The key travels in a header, and the count is sent explicitly rather than left to
        // the provider's own default.
        assert_eq!(
            request_header(&request, "x-api-key").as_deref(),
            Some("exa-secret-key")
        );
        assert_eq!(
            request_header(&request, "content-type").as_deref(),
            Some("application/json")
        );
        assert_eq!(
            request_body(&request),
            json!({
                "query": "harness tools",
                "type": "auto",
                "numResults": 5,
                "contents": { "highlights": true },
            })
        );
    }

    /// `websearch.test.ts:303-322`: a `javascript:` row and a relative row are DROPPED, and
    /// the row with no title falls back to its own URL.
    #[tokio::test]
    async fn a_row_without_a_valid_url_is_dropped_and_a_blank_title_falls_back_to_it() {
        let (outcome, _) = stubbed_search(
            "exa",
            "exa-secret-key",
            r#"{"results":[
                {"title":"JS","url":"javascript:alert(1)","highlights":["x"]},
                {"title":"Relative","url":"/somewhere","highlights":["x"]},
                {"title":"","url":"https://example.com/kept","highlights":["x"]}
            ]}"#,
            json!({ "query": "q" }),
        )
        .await;

        assert_eq!(
            outcome["results"],
            json!([
                { "title": "https://example.com/kept", "url": "https://example.com/kept", "snippet": "x" },
            ])
        );
    }

    /// `websearch.test.ts:324-359`: redaction covers EVERY configured key, including the key
    /// of a provider this call never touched.
    #[tokio::test]
    async fn every_configured_key_is_redacted_not_only_the_one_being_called() {
        let (base, receiver, _server) = stub_origin(&reply_with(
            r#"{"results":[{"title":"leak exa-secret-key","url":"https://example.com/exa-secret-key","highlights":["body exa-secret-key and tavily-secret-key"],"publishedDate":"2026-01-02"}]}"#,
        ))
        .await;
        let client = reqwest::Client::new();
        let keys = WebKeys {
            exa: Some("exa-secret-key".to_owned()),
            tavily: Some("tavily-secret-key".to_owned()),
            linkup: None,
            endpoint_base: Some(base),
            ..WebKeys::default()
        };

        let outcome = search(
            &client,
            &json!({ "query": "q" }),
            &keys,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("the stub call must succeed");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), receiver)
            .await
            .expect("one request");

        assert_eq!(
            outcome["results"][0],
            json!({
                "title": "leak [redacted]",
                "url": "https://example.com/[redacted]",
                "snippet": "body [redacted] and [redacted]",
                "published": "2026-01-02",
            })
        );
    }

    /// `websearch.test.ts:361-398`: a two-character key is redacted rather than skipped, even
    /// though over-redacting ordinary text is the price.
    #[tokio::test]
    async fn a_short_configured_key_is_redacted_rather_than_skipped() {
        // Two-character keys on both providers, redacted in every field.
        let (base, receiver, _server) = stub_origin(&reply_with(
            r#"{"results":[{"title":"keys k1 and t2","url":"https://example.com/k1?ref=t2","highlights":["k1 then t2"],"publishedDate":"k1"}]}"#,
        ))
        .await;
        let client = reqwest::Client::new();

        let outcome = search(
            &client,
            &json!({ "query": "q" }),
            &WebKeys {
                exa: Some("k1".to_owned()),
                tavily: Some("t2".to_owned()),
                linkup: None,
                endpoint_base: Some(base),
                ..WebKeys::default()
            },
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("the stub call must succeed");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), receiver)
            .await
            .expect("one request");

        assert_eq!(
            outcome["results"][0],
            json!({
                "title": "keys [redacted] and [redacted]",
                "url": "https://example.com/[redacted]?ref=[redacted]",
                "snippet": "[redacted] then [redacted]",
                "published": "[redacted]",
            })
        );
    }

    /// `websearch/request.ts:180-189`: a cut snippet says so; an uncut one stays byte-identical
    /// to the old shape, so the flag never costs a byte on the common path.
    #[test]
    fn a_cut_snippet_says_so_and_an_uncut_one_stays_the_old_shape() {
        let redactor = Redactor::new(Vec::<String>::new());
        let rows = vec![
            json!({ "title": "Long", "url": "https://example.com/long", "highlights": ["x".repeat(1_200)] }),
            json!({ "title": "Short", "url": "https://example.com/short", "highlights": ["exactly a line"] }),
        ];
        let (matches, dropped) = normalize_matches(&rows, 5, &redactor, RowReader::Exa);

        assert_eq!(dropped, 0);
        assert_eq!(matches[0]["snippet"], "x".repeat(1_000));
        assert_eq!(matches[0]["snippetTruncated"], json!(true));
        assert_eq!(
            matches[0]
                .as_object()
                .expect("an object")
                .keys()
                .collect::<Vec<_>>(),
            vec!["title", "url", "snippet", "snippetTruncated"]
        );
        assert_eq!(matches[1]["snippet"], "exactly a line");
        assert!(matches[1].get("snippetTruncated").is_none());
    }

    /// `websearch/request.ts:271-272`: `published` runs through `bounded`, so a provider that
    /// dumps prose into the date field gets the 64-character PREFIX (the module comment says
    /// "dropped", the code cuts — and the published value is what the reference tests pin).
    #[test]
    fn a_published_label_longer_than_64_characters_is_dropped() {
        let redactor = Redactor::new(Vec::<String>::new());
        let rows = vec![json!({
            "title": "Dated",
            "url": "https://example.com/dated",
            "highlights": ["s"],
            "publishedDate": "2026-01-02T00:00:00.000Z plus a great deal of commentary that pushes the label past its bound",
        })];

        let (matches, _dropped) = normalize_matches(&rows, 5, &redactor, RowReader::Exa);

        assert_eq!(
            matches[0]["published"],
            "2026-01-02T00:00:00.000Z plus a great deal of commentary that pu"
        );
        assert_eq!(
            matches[0]["published"].as_str().expect("text").len(),
            SEARCH_MAX_PUBLISHED_CHARS
        );

        // A label inside the bound is still carried whole.
        let rows = vec![json!({
            "title": "Dated",
            "url": "https://example.com/dated",
            "highlights": ["s"],
            "publishedDate": "2026-01-02",
        })];

        assert_eq!(
            normalize_matches(&rows, 5, &redactor, RowReader::Exa).0[0]["published"],
            "2026-01-02"
        );
    }

    /// `websearch/request.ts:210-231`: a URL is a link the model cites, so an over-long one is
    /// dropped rather than sliced, and a userinfo URL is dropped outright.
    #[test]
    fn an_over_long_or_credential_bearing_url_drops_its_row() {
        let redactor = Redactor::new(Vec::<String>::new());
        let rows = vec![
            json!({ "title": "Long", "url": format!("https://example.com/{}", "x".repeat(2_100)), "highlights": ["x"] }),
            json!({ "title": "Secret", "url": "https://user:pw@example.com/x", "highlights": ["x"] }),
            json!({ "title": "Host only", "url": "https://example.com", "highlights": ["x"] }),
        ];
        let (matches, dropped) = normalize_matches(&rows, 5, &redactor, RowReader::Exa);

        // Only the last row survives, and BOTH refusals are counted: the count is what tells
        // the caller the provider answered at all.
        assert_eq!(dropped, 2);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["url"], "https://example.com/");
    }

    /// `websearch/request.ts:139-153`: longest key first, duplicates dropped, every occurrence
    /// replaced, so a key containing another key is removed whole.
    #[test]
    fn the_redactor_replaces_longer_keys_first_and_never_skips_a_short_one() {
        let redactor = Redactor::new(["ab", "abcdef", "ab", "", "z"]);

        assert_eq!(
            redactor.redact("abcdef ab z"),
            "[redacted] [redacted] [redacted]"
        );
        assert_eq!(redactor.redact("nothing here"), "nothing here");
    }

    /// `websearch/request.ts:163-169`: redaction runs on the RAW string. A key that sits at the
    /// cut must not survive as its own prefix.
    #[test]
    fn redaction_runs_before_the_bound_not_after_it() {
        let redactor = Redactor::new(["secret-value"]);
        // The bound is 20 characters; the key starts at 15 and would be cut in half if the
        // bound ran first.
        let padded = format!("{}secret-value", "y".repeat(15));
        let (value, truncated) = bounded_flagged(Some(&padded), 20, &redactor);

        assert_eq!(value, "yyyyyyyyyyyyyyy[reda");
        assert!(truncated);
        assert!(value.contains("[r"), "a marker may be cut; a key may not");
        assert!(!value.contains("secret"));
    }

    /// `websearch/index.ts:291-303`: an out-of-range count is CLAMPED, a fractional one is
    /// floored, and the default is 5.
    #[test]
    fn max_results_defaults_to_five_and_clamps_instead_of_refusing() {
        let configured = ["exa", "tavily", "linkup"];
        let read = |arguments: Value| {
            read_invocation(&arguments, &configured).map(|invocation| invocation.max_results)
        };

        assert_eq!(
            read(json!({ "query": "q" })),
            Ok(SEARCH_DEFAULT_MAX_RESULTS)
        );
        assert_eq!(read(json!({ "query": "q", "maxResults": 3.7 })), Ok(3));
        assert_eq!(
            read(json!({ "query": "q", "maxResults": 100 })),
            Ok(SEARCH_MAX_RESULTS_LIMIT)
        );
        assert_eq!(read(json!({ "query": "q", "maxResults": 0 })), Ok(1));
        assert_eq!(
            read(json!({ "query": "q", "maxResults": "5" })),
            Err("search maxResults must be a number between 1 and 20.".to_owned())
        );
        assert_eq!(
            read(json!({ "query": "q", "maxResults": null })),
            Err("search maxResults must be a number between 1 and 20.".to_owned())
        );
    }

    /// `websearch/index.ts:321-355`: the first refusal is the sentence the model reads, so the
    /// order and the wording are both the contract.
    #[test]
    fn every_search_refusal_is_the_published_sentence() {
        let configured = ["exa", "tavily", "linkup"];

        assert_eq!(
            read_invocation(&json!({ "query": "   " }), &configured).unwrap_err(),
            "search requires a non-empty query string."
        );
        assert_eq!(
            read_invocation(&json!({ "query": "q", "speed": "turbo" }), &configured).unwrap_err(),
            "search speed must be \"balanced\" or \"fast\"."
        );
        assert_eq!(
            read_invocation(
                &json!({
                    "query": "q",
                    "category": "a rather long category that will not fit inside the sixty four character allowance"
                }),
                &configured
            )
            .unwrap_err(),
            "search category must be a short string, for example \"news\" or \"company\"."
        );
        assert_eq!(
            read_invocation(&json!({ "query": "q", "since": "not-a-date" }), &configured)
                .unwrap_err(),
            "search since must be an ISO-8601 date, for example \"2026-01-01\"."
        );
        assert_eq!(
            read_invocation(
                &json!({ "query": "q", "since": "2026-03-01", "until": "2026-01-01" }),
                &configured
            )
            .unwrap_err(),
            "search \"until\" must not be earlier than \"since\"."
        );
        assert_eq!(
            read_invocation(&json!({ "query": "q", "includeDomains": [] }), &configured)
                .unwrap_err(),
            "search includeDomains must be an array of 1-20 domains."
        );
        assert_eq!(
            read_invocation(
                &json!({ "query": "q", "excludeDomains": ["not a domain!!"] }),
                &configured
            )
            .unwrap_err(),
            "search excludeDomains entry \"not a domain!!\" is not a domain. Use a hostname (example.com), a path prefix (example.com/docs), or a wildcard subdomain (*.example.com)."
        );
        // A bare query string is a valid call, exactly as the reference allows for direct
        // callers, and a provider the keys do not cover is named in the refusal.
        assert_eq!(
            read_invocation(&json!("cats"), &configured)
                .expect("a bare query")
                .query,
            "cats"
        );
        assert_eq!(
            read_invocation(&json!({ "query": "q", "provider": "serper" }), &["exa"]).unwrap_err(),
            "search provider \"serper\" is not configured. Configured: exa."
        );
        assert_eq!(
            read_invocation(&json!({ "query": "q", "provider": null }), &["exa"]).unwrap_err(),
            "search provider \"null\" is not configured. Configured: exa."
        );
    }

    /// `websearch/index.ts:213-250`: a hostname, a path prefix and a wildcard subdomain are
    /// all legal, and each is normalized (lowercased, scheme and trailing slash removed).
    #[test]
    fn a_domain_filter_normalizes_the_three_forms_it_accepts() {
        let configured = ["exa"];
        let invocation = read_invocation(
            &json!({
                "query": "q",
                "includeDomains": ["HTTPS://Example.COM/docs/", "*.news.example.com", "example.com:8443"],
                "excludeDomains": ["other.example.org"],
            }),
            &configured,
        )
        .expect("every entry is a domain");

        assert_eq!(
            invocation.include_domains,
            Some(vec![
                "example.com/docs".to_owned(),
                "*.news.example.com".to_owned(),
                "example.com:8443".to_owned(),
            ])
        );
        assert_eq!(
            invocation.exclude_domains,
            Some(vec!["other.example.org".to_owned()])
        );
    }

    /// `websearch/{tavily,linkup}.ts`: each provider gets its own body and header vocabulary,
    /// and Linkup reads its title from `name` and answers no date at all.
    #[tokio::test]
    async fn each_provider_gets_its_own_body_and_header() {
        let (outcome, request) = stubbed_search(
            "tavily",
            "tavily-secret-key",
            r#"{"results":[{"title":"Tavily result","url":"https://example.com/t","content":"a snippet","published_date":"2026-03-04"}]}"#,
            json!({ "query": "harness tools", "maxResults": 3, "speed": "fast" }),
        )
        .await;

        assert_eq!(
            request_header(&request, "authorization").as_deref(),
            Some("Bearer tavily-secret-key")
        );
        assert_eq!(
            request_body(&request),
            json!({
                "query": "harness tools",
                "max_results": 3,
                "search_depth": "basic",
                "include_answer": false,
                "include_raw_content": false,
            })
        );
        assert_eq!(
            outcome["results"][0],
            json!({
                "title": "Tavily result",
                "url": "https://example.com/t",
                "snippet": "a snippet",
                "published": "2026-03-04",
            })
        );

        let (outcome, request) = stubbed_search(
            "linkup",
            "linkup-secret-key",
            r#"{"results":[{"name":"First result","url":"https://example.com/a","content":"a snippet"}]}"#,
            json!({ "query": "harness tools", "maxResults": 3 }),
        )
        .await;

        assert_eq!(
            request_header(&request, "authorization").as_deref(),
            Some("Bearer linkup-secret-key")
        );
        assert_eq!(
            request_body(&request),
            json!({ "q": "harness tools", "depth": "fast", "outputType": "searchResults", "maxResults": 3 })
        );
        assert_eq!(outcome["provider"], "linkup");
        assert_eq!(
            outcome["results"][0],
            json!({ "title": "First result", "url": "https://example.com/a", "snippet": "a snippet" })
        );
        assert!(outcome["results"][0].get("published").is_none());

        // Exa's `fast` mode is the only request that asks for `lite`.
        let (_, request) = stubbed_search(
            "exa",
            "k",
            r#"{"results":[]}"#,
            json!({ "query": "q", "speed": "fast" }),
        )
        .await;

        assert_eq!(request_body(&request)["type"], "lite");
    }

    /// `websearch/index.ts:548-562`: the filters a call applied come back on the result, so a
    /// model can tell "nothing exists" from "my filter excluded everything".
    #[tokio::test]
    async fn the_filters_a_call_applied_are_echoed_back() {
        let (outcome, _) = stubbed_search(
            "exa",
            "k",
            r#"{"results":[]}"#,
            json!({
                "query": "q",
                "includeDomains": ["example.com"],
                "since": "2026-01-01",
                "until": "2026-02-01",
                "category": "News",
                "speed": "fast",
            }),
        )
        .await;

        assert_eq!(
            outcome,
            json!({
                "provider": "exa",
                "query": "q",
                "maxResults": 5,
                "results": [],
                "includeDomains": ["example.com"],
                "since": "2026-01-01",
                "until": "2026-02-01",
                "category": "news",
                "speed": "fast",
            })
        );
        assert_eq!(
            outcome
                .as_object()
                .expect("an object")
                .get("excludeDomains"),
            None
        );
    }

    /// `websearch/request.ts:282-301`: one fixed sentence per failure class, and never a
    /// provider body, because a body can echo the key.
    #[tokio::test]
    async fn a_provider_status_answers_the_published_sentence() {
        let cases = [
            (
                401,
                "Exa search rejected the API key (HTTP 401). Check the configured key and account.",
            ),
            (
                403,
                "Exa search rejected the API key (HTTP 403). Check the configured key and account.",
            ),
            (
                402,
                "Exa search reports no remaining credits or quota (HTTP 402). Check the provider account.",
            ),
            (
                429,
                "Exa search is rate limited (HTTP 429). Try again later.",
            ),
            (500, "Exa search failed (HTTP 500)."),
        ];

        for (status, sentence) in cases {
            let (base, receiver, _server) = stub_origin(&format!(
                "HTTP/1.1 {status} No\r\ncontent-length: 9\r\nconnection: close\r\n\r\nnot json"
            ))
            .await;
            let client = reqwest::Client::new();

            let error = search(
                &client,
                &json!({ "query": "q" }),
                &stubbed_keys("exa", "k", &base),
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
            .expect_err("a failing status is a tool error");

            assert_eq!(error, sentence, "HTTP {status}");
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), receiver).await;
        }
    }

    /// The two non-JSON shapes: a body that is not JSON at all, and JSON without `results`.
    #[tokio::test]
    async fn json_without_a_results_list_is_named_by_the_field() {
        let (base, receiver, _server) = stub_origin(&reply_with(r#"{"data":[]}"#)).await;
        let client = reqwest::Client::new();

        let error = search(
            &client,
            &json!({ "query": "q" }),
            &stubbed_keys("exa", "k", &base),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("a response without results is an error");

        assert_eq!(
            error,
            "Exa search returned a response without a \"results\" list."
        );
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), receiver).await;

        let (base, receiver, _server) = stub_origin(&reply_with("<html>nope</html>")).await;

        let error = search(
            &client,
            &json!({ "query": "q" }),
            &stubbed_keys("exa", "k", &base),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("a body that is not JSON is an error");

        assert_eq!(error, "Exa search returned a response that is not JSON.");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), receiver).await;
    }

    /// `websearch/request.ts:446-455`: a DECLARED size over the budget is refused before a
    /// single byte is read.
    #[tokio::test]
    async fn an_oversized_declared_response_is_refused_before_it_is_read() {
        let (base, receiver, _server) = stub_origin(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 900000\r\nconnection: close\r\n\r\n{}",
        )
        .await;
        let client = reqwest::Client::new();

        let error = search(
            &client,
            &json!({ "query": "q" }),
            &stubbed_keys("exa", "k", &base),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("a declared size over the budget is an error");

        assert_eq!(
            error,
            format!(
                "Exa search returned a response larger than {SEARCH_MAX_RESPONSE_BYTES} bytes."
            )
        );
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), receiver).await;
    }

    /// A cancelled turn stops waiting on the provider immediately.
    #[tokio::test]
    async fn a_cancelled_turn_stops_waiting_on_the_provider() {
        use tokio::io::AsyncReadExt as _;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port");
        let address = listener.local_addr().expect("a bound address");

        // The stub accepts the connection and never answers.
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("one connection");
            let mut sink = [0_u8; 512];
            let _ = socket.read(&mut sink).await;
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        });

        let token = tokio_util::sync::CancellationToken::new();
        let client = reqwest::Client::new();
        let driving = token.clone();

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            driving.cancel();
        });

        let error = search(
            &client,
            &json!({ "query": "q" }),
            &stubbed_keys("exa", "k", &format!("http://{address}")),
            &token,
        )
        .await
        .expect_err("a cancelled call does not produce a result");

        assert_eq!(error, "the request was cancelled.");
    }

    // ---- download ------------------------------------------------------------------
    // The download seam is the pair `dns_answers` + `connect_to`: the URL carries a PUBLIC name
    // (gate 1 refuses a loopback name before anything else runs), the injected DNS answer is what
    // gate 2 reads, and `connect_to` is where the socket actually goes. Every figure comes from
    // the stub's own bound port, so no test depends on a resolver or on a free port.

    /// One reply, with the headers the name, size and redirect rules read. `connection: close` is
    /// load-bearing for the same reason `fetch_reply` states it: a pooled socket would swallow the
    /// next hop's reply. `content-length` is passed explicitly because several tests lie about it
    /// on purpose.
    fn download_reply(status: u16, reason: &str, headers: &[(&str, &str)], body: &str) -> String {
        let mut head = format!("HTTP/1.1 {status} {reason}\r\n");

        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }

        head.push_str("connection: close\r\n\r\n");
        head.push_str(body);
        head
    }

    /// The stub origin under a public name. The injected answer is TEST-NET-3 (`203.0.113.7`), a
    /// documented-public address, so gate 2 says "go" while `connect_to` lands the socket on the
    /// local stub — which is the only way a gated download can be tested at all.
    fn download_keys(base: &str) -> WebKeys {
        let port = base
            .rsplit(':')
            .next()
            .and_then(|tail| tail.parse::<u16>().ok())
            .unwrap_or(80);

        WebKeys {
            dns_answers: BTreeMap::from([(
                "dl.example".to_owned(),
                vec![IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, 7))],
            )]),
            connect_to: BTreeMap::from([(
                "dl.example".to_owned(),
                vec![SocketAddr::from(([127, 0, 0, 1], port))],
            )]),
            ..WebKeys::default()
        }
    }

    /// The same origin addressed by its public name: `http://127.0.0.1:PORT/x` becomes
    /// `http://dl.example:PORT/x`.
    fn public_url(base: &str, path: &str) -> String {
        format!(
            "http://dl.example{}{path}",
            base.strip_prefix("http://127.0.0.1").unwrap_or("")
        )
    }

    /// A scratch sessions directory that removes itself, so a test can name the exact folder the
    /// file landed in without leaving bytes behind.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new() -> Self {
            let directory =
                std::env::temp_dir().join(format!("r1-download-{}", uuid::Uuid::new_v4()));

            std::fs::create_dir_all(&directory).expect("a scratch directory is creatable");
            Self(directory)
        }

        /// The folder one session owns, exactly as `downloads_directory_of` composes it.
        fn folder(&self) -> std::path::PathBuf {
            self.0.join("session-1").join("downloads")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn a_public_download_lands_in_the_session_folder_and_reports_every_fact() {
        let body = "pixel-bytes";
        let (base, seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[
                ("content-type", "image/png"),
                ("content-disposition", "attachment; filename=\"pixel.png\""),
                ("content-length", "11"),
            ],
            body,
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();
        let url = public_url(&base, "/file");

        let outcome = download(
            &json!({ "url": url }),
            &DownloadContext {
                keys: &keys,
                sessions_root: Some(&scratch.0),
                session_id: Some("session-1"),
            },
            &CancellationToken::new(),
        )
        .await
        .expect("a public host behind the pinned seam must download");

        // The published summary, key for key. There is no `saveError` key, because an optional key
        // that did not fire is ABSENT rather than null: `path: null` would read as "there is a
        // path, and it is empty".
        let path = scratch.folder().join("pixel.png");

        assert_eq!(
            outcome["content"],
            json!({
                "url": url,
                "finalUrl": url,
                "contentType": "image/png",
                "kind": "image",
                "name": "pixel.png",
                "bytes": 11,
                "declaredBytes": 11,
                "media": "attached",
                "mediaId": "dl-1",
                "saved": true,
                "path": path.to_string_lossy(),
                "searchable": false,
                // An image is addressable but NOT searchable: `grep` opens text only, and the note
                // has to say which affordance the model just got.
                "note": "Saved in this session's downloads folder. Point read or glob at the absolute `path` above: a path you name explicitly is searched even when a .gitignore above it ignores that folder. grep skips this file (it opens only text files under 1 MiB), so use the bytes in this message or read the file.",
            })
        );
        assert_eq!(
            std::fs::read(&path).expect("the downloaded bytes are on disk"),
            body.as_bytes()
        );
        // The envelope is what makes the bytes model input: the marker comes from the attachment
        // kind, and `dl-1` is deterministic because one call carries exactly one download.
        assert_eq!(
            outcome[TOOL_MEDIA_BRAND],
            json!([{
                "id": "dl-1",
                "kind": "image",
                "marker": "[Image 1]",
                "name": "pixel.png",
                "mime": "image/png",
                "size": 11,
                "data": BASE64.encode(body),
            }])
        );
        assert_eq!(seen.lock().expect("the request log").len(), 1);
        // The summary is the model-facing half, and it has to fit the delivery cap on its own.
        assert!(payload::document_wire_bytes(&outcome["content"]) <= FETCH_MAX_RESULT_BYTES);
        server.abort();
    }

    #[tokio::test]
    async fn every_private_address_form_is_refused_before_the_socket_opens() {
        let (base, seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[("content-type", "text/plain"), ("content-length", "2")],
            "hi",
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();
        let port = base.rsplit(':').next().expect("a port").to_owned();

        // Gate 1, in every notation the reference decodes: the dotted quad, the `inet_aton`
        // shorthands, IPv6 loopback, link-local, the unique-local range, and the v4-in-v6 tunnels.
        for url in [
            format!("http://127.0.0.1:{port}/x"),
            "http://2130706433/x".to_owned(),
            "http://0x7f000001/x".to_owned(),
            "http://10.0.0.5/x".to_owned(),
            "http://192.168.1.1/x".to_owned(),
            "http://169.254.169.254/x".to_owned(),
            "http://[::1]/x".to_owned(),
            "http://[fe80::1]/x".to_owned(),
            "http://[fc00::1]/x".to_owned(),
            "http://[::ffff:127.0.0.1]/x".to_owned(),
            "http://[2002:7f00:1::]/x".to_owned(),
        ] {
            assert_eq!(
                download(
                    &json!({ "url": url }),
                    &DownloadContext {
                        keys: &keys,
                        sessions_root: Some(&scratch.0),
                        session_id: Some("session-1"),
                    },
                    &CancellationToken::new(),
                )
                .await
                .expect_err("a private destination has to be refused"),
                "download refuses this URL: it is not a public absolute http(s) address (loopback and private addresses are not downloaded).",
                "{url} is not a public address"
            );
        }

        // Nothing was asked of the origin: the gate is PROVEN by the absence of a request, and no
        // folder was created for a transfer that never started.
        assert!(
            seen.lock().expect("the request log").is_empty(),
            "a refused URL must not open a socket"
        );
        assert!(
            !scratch.folder().exists(),
            "a refused download must not create the session folder"
        );
        server.abort();
    }

    /// THE PROOF THAT GATE 2 IS ON THE PATH: the NAME is public, the ANSWER is not, and the
    /// refusal NAMES the address the resolver found.
    ///
    /// The clause is ` — <addresses> resolves to a non-public address` INSIDE the final period:
    /// `refusedUrl` interpolates it before the closing `.` (`download.tool.ts:847-852`) and the
    /// call site passes the resolved list, not the hostname
    /// (`refusedUrl(firstHop.address ?? new URL(target).hostname)`, `:1347`) — the caller already
    /// knows the name, the actionable fact is what it pointed at.
    #[tokio::test]
    async fn a_name_that_answers_with_loopback_is_refused_by_the_resolver() {
        // No `connect_to` entry is needed: the verdict arrives before any socket is built, and a
        // real lookup of `internal.example` would otherwise make this test depend on the network.
        let (base, seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[("content-type", "text/plain"), ("content-length", "2")],
            "hi",
        )])
        .await;
        let port = base.rsplit(':').next().expect("a port").to_owned();
        let keys = WebKeys {
            dns_answers: BTreeMap::from([(
                "internal.example".to_owned(),
                vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            )]),
            ..WebKeys::default()
        };
        let scratch = Scratch::new();

        assert_eq!(
            download(
                &json!({ "url": format!("http://internal.example:{port}/x") }),
                &DownloadContext {
                    keys: &keys,
                    sessions_root: Some(&scratch.0),
                    session_id: Some("session-1"),
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("a name that answers with loopback has to be refused"),
            "download refuses this URL: it is not a public absolute http(s) address (loopback and \
             private addresses are not downloaded) — 127.0.0.1 resolves to a non-public address."
        );
        assert!(
            seen.lock().expect("the request log").is_empty(),
            "a name that resolves into loopback must not open a socket"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_redirect_into_loopback_is_refused_at_the_hop() {
        // The Location names loopback on port 1 and is never asked for: the gate decides on the
        // ADDRESS, and a hand-followed hop is the only place that decision can be made before the
        // connect. A platform that followed redirects itself would have delivered internal bytes.
        let (base, seen, server) = fetch_server(&[download_reply(
            302,
            "Found",
            &[
                ("location", "http://127.0.0.1:1/inner"),
                ("content-length", "0"),
            ],
            "",
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();

        assert_eq!(
            download(
                &json!({ "url": public_url(&base, "/start") }),
                &DownloadContext {
                    keys: &keys,
                    sessions_root: Some(&scratch.0),
                    session_id: Some("session-1"),
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("a public URL may not hand the session an internal page"),
            "download refused a redirect to a non-public address (HTTP 302)."
        );
        assert_eq!(
            seen.lock().expect("the request log").len(),
            1,
            "the hop is refused before it is requested"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_public_redirect_chain_is_followed_five_hops_and_then_stops() {
        // A relative Location is legal and common, so it is RESOLVED against the hop that sent it
        // and stays on the public name. The chain cycles through the stub forever; the cap is what
        // ends it. Five hops are FOLLOWED (requests 1-5 answer 302) and the sixth 302 is the one
        // the tool refuses — which is why SIX requests are recorded.
        let (base, seen, server) = fetch_server(&[download_reply(
            302,
            "Found",
            &[("location", "/next"), ("content-length", "0")],
            "",
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();

        assert_eq!(
            download(
                &json!({ "url": public_url(&base, "/start") }),
                &DownloadContext {
                    keys: &keys,
                    sessions_root: Some(&scratch.0),
                    session_id: Some("session-1"),
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("a redirect loop must not pin the session"),
            "download stopped after 5 redirects."
        );
        assert_eq!(seen.lock().expect("the request log").len(), 6);
        server.abort();
    }

    #[tokio::test]
    async fn a_starred_disposition_name_is_decoded_and_beats_the_plain_one() {
        let (base, _seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[
                ("content-type", "image/png"),
                (
                    "content-disposition",
                    "attachment; filename=\"report; plain.png\"; filename*=UTF-8''%E2%82%AC%20euro.png",
                ),
                ("content-length", "1"),
            ],
            "x",
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();

        let outcome = download(
            &json!({ "url": public_url(&base, "/blob") }),
            &DownloadContext {
                keys: &keys,
                sessions_root: Some(&scratch.0),
                session_id: Some("session-1"),
            },
            &CancellationToken::new(),
        )
        .await
        .expect("the extended form is parseable");

        // `filename*` outranks `filename` — and a parser that split on `;` would have ended the
        // plain name at `report` and lost the rest of it anyway.
        assert_eq!(outcome["content"]["name"], json!("€ euro.png"));
        server.abort();
    }

    #[tokio::test]
    async fn a_quoted_plain_name_keeps_the_semicolon_inside_it() {
        let (base, _seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[
                ("content-type", "image/png"),
                (
                    "content-disposition",
                    "attachment; filename=\"report; final.png\"",
                ),
                ("content-length", "1"),
            ],
            "x",
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();

        let outcome = download(
            &json!({ "url": public_url(&base, "/blob") }),
            &DownloadContext {
                keys: &keys,
                sessions_root: Some(&scratch.0),
                session_id: Some("session-1"),
            },
            &CancellationToken::new(),
        )
        .await
        .expect("the plain form is parseable");

        // A naive split on `;` ends the name at `report` and loses the rest of the file's name.
        assert_eq!(outcome["content"]["name"], json!("report; final.png"));
        server.abort();
    }

    #[tokio::test]
    async fn a_declared_size_over_the_cap_refuses_before_the_body_arrives() {
        // 21 MiB declared, two bytes sent: the declaration is what the tool trusts enough to
        // refuse on, and refusing on it is what keeps 20 MiB from being read for nothing.
        let (base, seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[
                ("content-type", "application/octet-stream"),
                ("content-length", "22020096"),
            ],
            "hi",
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();

        assert_eq!(
            download(
                &json!({ "url": public_url(&base, "/big") }),
                &DownloadContext {
                    keys: &keys,
                    sessions_root: Some(&scratch.0),
                    session_id: Some("session-1"),
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("an oversized declaration is a refusal"),
            "download rejects files larger than 20971520 bytes (20 MiB)."
        );
        assert_eq!(seen.lock().expect("the request log").len(), 1);
        assert!(
            !scratch.folder().exists(),
            "a refused transfer writes nothing"
        );
        server.abort();
    }

    /// WHAT THE TWO SIZE KEYS MEAN, given that a real socket cannot make them disagree.
    ///
    /// The reference builds its `Response` by hand, so its test can declare 4 bytes and deliver 10
    /// (`download.tool.ts:1425` reads the header, `:1444` reads the stream). Over a connection,
    /// content-length FRAMING decides: the client stops at the declared number, so both keys report
    /// the same figure and a lie about the size is invisible. What IS reachable — and what the
    /// reference's summary contract turns on — is a response with NO declaration: `declaredBytes`
    /// is then ABSENT, never a fabricated 0, because the summary spreads it only when
    /// `Number.isFinite(declared)` (`:1495`).
    #[tokio::test]
    async fn the_declaration_and_the_delivered_bytes_are_reported_separately() {
        let (base, _seen, server) = fetch_server(&[
            // A declaration: framing truncates the 10-byte body to 4, and `declaredBytes` carries
            // the same number the transport honoured.
            download_reply(
                200,
                "OK",
                &[("content-type", "text/plain"), ("content-length", "4")],
                "0123456789",
            ),
            // No declaration at all: `connection: close` makes the body EOF-delimited, so all 10
            // bytes are real, and there is nothing to report as declared.
            download_reply(200, "OK", &[("content-type", "text/plain")], "0123456789"),
        ])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();
        let context = DownloadContext {
            keys: &keys,
            sessions_root: Some(&scratch.0),
            session_id: Some("session-1"),
        };

        let declared = download(
            &json!({ "url": public_url(&base, "/declared") }),
            &context,
            &CancellationToken::new(),
        )
        .await
        .expect("a declared transfer still downloads");

        assert_eq!(declared["content"]["bytes"], json!(4));
        assert_eq!(declared["content"]["declaredBytes"], json!(4));

        let undeclared = download(
            &json!({ "url": public_url(&base, "/undeclared") }),
            &context,
            &CancellationToken::new(),
        )
        .await
        .expect("a transfer with no content-length still downloads");

        assert_eq!(undeclared["content"]["bytes"], json!(10));
        assert!(
            undeclared["content"].get("declaredBytes").is_none(),
            "a missing declaration must stay absent: {}",
            undeclared["content"]
        );
        // The extension still comes from the MIME, and the bytes on disk are the bytes reported.
        assert_eq!(undeclared["content"]["name"], json!("undeclared.txt"));
        assert_eq!(
            std::fs::read(scratch.folder().join("undeclared.txt")).expect("the written file"),
            b"0123456789"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_sessionless_download_and_an_escaping_path_refuse_before_the_transfer() {
        let (base, seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[("content-type", "text/plain"), ("content-length", "2")],
            "hi",
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();

        // No folder means no owner for the bytes: the reference refuses BEFORE the transfer rather
        // than writing somewhere the caller never named.
        assert_eq!(
            download(
                &json!({ "url": public_url(&base, "/x") }),
                &DownloadContext {
                    keys: &keys,
                    sessions_root: None,
                    session_id: Some("session-1"),
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("no folder, no download"),
            "download needs a session: this harness has no session downloads folder, so there is nowhere to save the file. Run it inside a server session, or build the harness with `downloadsDirectory`."
        );
        // A `path` that walks out of the session folder is an ARGUMENT error, proven before the
        // transfer is paid for.
        assert_eq!(
            download(
                &json!({ "url": public_url(&base, "/x"), "path": "../escape.png" }),
                &DownloadContext {
                    keys: &keys,
                    sessions_root: Some(&scratch.0),
                    session_id: Some("session-1"),
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("an escaping path is refused"),
            "download path must be relative to the session downloads folder, without \"..\"."
        );
        // An absolute path and a drive prefix escape the same way.
        assert_eq!(
            download(
                &json!({ "url": public_url(&base, "/x"), "path": "/etc/passwd" }),
                &DownloadContext {
                    keys: &keys,
                    sessions_root: Some(&scratch.0),
                    session_id: Some("session-1"),
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("an absolute path is refused"),
            "download path must be relative to the session downloads folder, without \"..\"."
        );
        assert!(seen.lock().expect("the request log").is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn a_path_argument_lands_under_the_folder_and_a_second_copy_is_numbered() {
        let body = "note";
        let (base, _seen, server) = fetch_server(&[download_reply(
            200,
            "OK",
            &[
                ("content-type", "text/plain"),
                ("content-disposition", "attachment; filename=\"note.txt\""),
                ("content-length", "4"),
            ],
            body,
        )])
        .await;
        let keys = download_keys(&base);
        let scratch = Scratch::new();
        let context = DownloadContext {
            keys: &keys,
            sessions_root: Some(&scratch.0),
            session_id: Some("session-1"),
        };
        let arguments = json!({ "url": public_url(&base, "/note"), "path": "sub/note.txt" });

        let first = download(&arguments, &context, &CancellationToken::new())
            .await
            .expect("a relative path inside the folder is honoured");
        let first_path = scratch.folder().join("sub").join("note.txt");

        assert_eq!(
            first["content"]["path"].as_str().expect("a path"),
            first_path.to_string_lossy()
        );
        assert!(first_path.exists());

        // A second call for the same name does not overwrite the first. The suffix goes on the
        // LEAF, so the name stays readable however often the same file is downloaded.
        let second = download(&arguments, &context, &CancellationToken::new())
            .await
            .expect("the unique name is still a success");

        assert_eq!(
            second["content"]["path"].as_str().expect("a path"),
            scratch
                .folder()
                .join("sub")
                .join("note-1.txt")
                .to_string_lossy()
        );
        assert_eq!(
            std::fs::read(first_path).expect("the first copy survives"),
            body.as_bytes()
        );
        server.abort();
    }

    // ---- search: the fit report and the delivery cap --------------------------------

    #[tokio::test]
    async fn provider_rows_that_cannot_be_used_are_counted_and_named() {
        // Two rows, neither with a usable URL: `results: []` on its own would read as "the index
        // has nothing", and the caller would retry a query that was fine.
        let (base, seen, server) = stub_origin(&reply_with(
            r#"{"results":[{"title":"ftp","url":"ftp://example.com/a"},{"title":"no url","highlights":"x"}]}"#,
        ))
        .await;
        let outcome = search(
            &reqwest::Client::new(),
            &json!({ "query": "q" }),
            &stubbed_keys("exa", "k", &base),
            &CancellationToken::new(),
        )
        .await
        .expect("junk rows are a result, not a failure");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), seen)
            .await
            .expect("one request");

        assert_eq!(
            outcome,
            json!({
                "provider": "exa",
                "query": "q",
                "maxResults": 5,
                "results": [],
                "droppedResults": 2,
                "note": "Exa returned 2 result rows with no usable http(s) URL, so none of them could be shown. The provider did answer: the query is not necessarily at fault.",
            })
        );
        server.abort();
    }

    #[tokio::test]
    async fn junk_rows_do_not_spend_the_callers_quota() {
        // `maxResults: 2` behind three junk rows used to answer ZERO results. The scan runs until
        // two USABLE rows are collected, and the junk is reported as a count instead.
        let (base, seen, server) = stub_origin(&reply_with(
            r#"{"results":[
                {"title":"junk 1","url":"ftp://example.com/1","highlights":"x"},
                {"title":"junk 2","url":"not a url","highlights":"x"},
                {"title":"junk 3","url":"mailto:a@b","highlights":"x"},
                {"title":"Good one","url":"https://example.com/one","highlights":"first"},
                {"title":"Good two","url":"https://example.com/two","highlights":"second"},
                {"title":"Never reached","url":"https://example.com/three","highlights":"third"}
            ]}"#,
        ))
        .await;
        let outcome = search(
            &reqwest::Client::new(),
            &json!({ "query": "q", "maxResults": 2 }),
            &stubbed_keys("exa", "k", &base),
            &CancellationToken::new(),
        )
        .await
        .expect("the quota is filled from the usable rows");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), seen)
            .await
            .expect("one request");

        let rows = outcome["results"].as_array().expect("rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["title"], json!("Good one"));
        assert_eq!(rows[1]["title"], json!("Good two"));
        assert_eq!(outcome["droppedResults"], json!(3));
        assert!(
            outcome.get("note").is_none(),
            "a partial answer is not the empty-answer case"
        );
        assert!(outcome.get("resultsTruncated").is_none());
        server.abort();
    }

    #[tokio::test]
    async fn an_oversized_result_drops_trailing_rows_and_says_so() {
        // Twenty rows whose excerpt is 1 000 quotation marks. Each quote serializes as `\"`, so one
        // excerpt alone costs 2 000 payload bytes and the answer cannot be delivered; each row is
        // the same size, so the count that fits is exact arithmetic, not an observation:
        //     row   = 58 framing + 2 000 excerpt + 2 closing  = 2 062 wire bytes
        //     seven = 14 434 + the 6 commas                      = 14 440
        //     the result document around them, with `resultsTruncated` and the note, lands at
        //     28 797 while EIGHT rows would land at 30 880+2 185 — past the 32 768-byte cap.
        let rows: Vec<Value> = (0..20)
            .map(|index| {
                json!({
                    "title": format!("Row {index}"),
                    "url": format!("https://example.com/{index}"),
                    "highlights": "\"".repeat(1_000),
                })
            })
            .collect();
        let (base, seen, server) =
            stub_origin(&reply_with(&json!({ "results": rows }).to_string())).await;
        let outcome = search(
            &reqwest::Client::new(),
            &json!({ "query": "q", "maxResults": 20 }),
            &stubbed_keys("exa", "k", &base),
            &CancellationToken::new(),
        )
        .await
        .expect("an oversized answer is trimmed, not refused");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), seen)
            .await
            .expect("one request");
        let kept = outcome["results"].as_array().expect("rows");

        // The ceiling the harness refuses above, asserted on the document that is actually
        // returned — the fit's own flag and note are inside this figure.
        assert!(payload::document_wire_bytes(&outcome) <= SEARCH_MAX_RESULT_BYTES);
        assert_eq!(outcome["resultsTruncated"], json!(true));
        assert_eq!(kept.len(), 7);
        assert_eq!(kept[0]["title"], json!("Row 0"));
        assert_eq!(kept[6]["title"], json!("Row 6"));
        assert_eq!(
            outcome["note"],
            json!(
                "13 of 20 result rows were omitted to fit the 32768-byte result budget. Narrow the query or lower maxResults for more per-row detail."
            )
        );
        // A row that survives is a row that is WHOLE: redaction and bounding ran before the fit,
        // so no excerpt was cut at a credential boundary and no surviving row was clipped.
        assert_eq!(
            kept[0]["snippet"].as_str().expect("text").chars().count(),
            1_000
        );
        assert!(kept[0].get("snippetTruncated").is_none());
        server.abort();
    }

    #[test]
    fn a_single_row_is_clipped_rather_than_thrown_away() {
        // `search()` cannot reach the clip step — the per-field caps keep one row near 3 KiB — so
        // the fit is driven directly, at a budget one row overflows. The clip prices its own
        // `snippetTruncated` flag: bisecting without it lands over the cap and drops the row.
        //
        // The figures are the same currency as above: the row's framing (`title` 18 + `url` 39 +
        // the `snippet` key 11 + braces/colons) is 70 bytes, and each kept quote costs 2 more, so a
        // 150-byte budget admits 13 of the 100 quotes and the flag is what makes 14 miss.
        let rows = vec![json!({
            "title": "T",
            "url": "https://example.com/a",
            "snippet": "\"".repeat(100),
        })];
        let budget = 150_usize;
        let measure =
            |candidate: &[Value]| payload::document_wire_bytes(&Value::Array(candidate.to_vec()));

        let (fitted, truncated, clipped) = fit_rows_to_result_budget(&rows, budget, &measure);

        assert!(truncated);
        assert!(clipped);
        assert_eq!(fitted.len(), 1);
        assert_eq!(
            fitted[0]["snippet"].as_str().expect("text").chars().count(),
            13
        );
        assert_eq!(fitted[0]["snippetTruncated"], json!(true));
        assert!(measure(&fitted) <= budget);

        // A row whose title and URL ALONE overflow the budget cannot be rescued by clipping its
        // excerpt: the safety net drops it, which still beats a result the harness refuses.
        let (dropped, truncated, clipped) = fit_rows_to_result_budget(&rows, 40, &measure);

        assert!(truncated);
        assert!(clipped);
        assert!(dropped.is_empty());
    }

    #[tokio::test]
    async fn the_category_combination_rules_apply_only_to_the_provider_that_has_them() {
        // F11: only Exa receives `category`, so a Tavily or Linkup call that Exa would refuse still
        // runs — a refusal the resolved provider cannot justify is a lost query, not a guard.
        let body = r#"{"results":[{"title":"T","url":"https://example.com/t","content":"c"}]}"#;

        for (provider, arguments, expected) in [
            (
                "tavily",
                json!({ "query": "harness tools", "category": "people", "since": "2026-01-01" }),
                None,
            ),
            (
                "linkup",
                json!({ "query": "harness tools", "category": "company", "until": "2026-02-01" }),
                None,
            ),
            (
                "exa",
                json!({ "query": "harness tools", "category": "people", "since": "2026-01-01" }),
                Some(
                    "search category \"people\" does not support date filters. Drop \"since\"/\"until\", or use another category.",
                ),
            ),
            (
                "exa",
                json!({ "query": "harness tools", "category": "company", "until": "2026-02-01" }),
                Some(
                    "search category \"company\" does not support date filters. Drop \"since\"/\"until\", or use another category.",
                ),
            ),
            (
                "exa",
                json!({
                    "query": "harness tools",
                    "category": "people",
                    "excludeDomains": ["spam.example"]
                }),
                Some(
                    "search category \"people\" does not support \"excludeDomains\". Drop the category or the domain filter.",
                ),
            ),
            // The combinations verified against the live provider must NOT be over-rejected.
            (
                "exa",
                json!({ "query": "harness tools", "category": "news", "since": "2026-01-01" }),
                None,
            ),
            (
                "exa",
                json!({
                    "query": "harness tools",
                    "category": "company",
                    "excludeDomains": ["spam.example"]
                }),
                None,
            ),
        ] {
            let (base, seen, server) = stub_origin(&reply_with(body)).await;

            match expected {
                None => {
                    search(
                        &reqwest::Client::new(),
                        &arguments,
                        &stubbed_keys(provider, "k", &base),
                        &CancellationToken::new(),
                    )
                    .await
                    .unwrap_or_else(|error| {
                        panic!("{provider} {arguments} must run, got: {error}")
                    });
                }
                Some(sentence) => {
                    assert_eq!(
                        search(
                            &reqwest::Client::new(),
                            &arguments,
                            &stubbed_keys(provider, "k", &base),
                            &CancellationToken::new(),
                        )
                        .await
                        .expect_err("the combination is refused locally"),
                        sentence,
                        "{provider} {arguments}"
                    );
                    let _ = tokio::time::timeout(std::time::Duration::from_millis(250), seen)
                        .await
                        .expect_err("a local refusal must not reach the provider");
                }
            }

            server.abort();
        }
    }
}
