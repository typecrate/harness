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
//!   * `tools/webdownload/download.tool.ts` - the streamed, byte-capped transfer into
//!     `<cwd>/.underload/downloads/`, the MIME -> attachment-kind table, and the
//!     `toolMedia(summary, [attachment])` envelope that keeps the bytes OUT of the text
//!     the model reads.
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
//!     check that refuses private/loopback/link-local targets before connecting. A Rust caller
//!     gets reqwest's own policy plus the redirect-landing refusal below.
//!   * **`redirects`**: the reference counts hops. `RequestBuilder` in this reqwest version has
//!     no per-request redirect policy, so the port follows the default policy and refuses a
//!     response that lands on a different authority than the endpoint (`network error`
//!     sentence) instead of reporting a hop count.
//!   * **The hosted providers** (`webfetch/exa.ts`, `webfetch/linkup.ts`). `fetch` answers with
//!     `available = ["native"]`, so asking for `exa`/`linkup` gets the published refusal rather
//!     than a silently different renderer.
//!
//! `search` calls a real provider and normalizes its rows to the shared match shape, and
//! `download` streams a byte-capped transfer into `.underload/downloads/`. Both state their own
//! `provider` field, so a caller is never told a richer pipeline ran than did.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use crate::contract::{AttachmentKind, HarnessAttachment};
use crate::tools::result::TOOL_MEDIA_BRAND;

/// `MAX_URL_CHARS` (`webdownload/download.tool.ts:299`): a URL longer than this is refused
/// before it reaches the network.
pub const MAX_URL_CHARS: usize = 4_096;
/// `MAX_DOWNLOAD_ATTACHMENT_BYTES` (`shared/attachments.ts:88`): 20 MiB decoded.
pub const MAX_DOWNLOAD_BYTES: u64 = 20 * 1024 * 1024;
/// The default destination folder (`download.tool.ts`'s `.underload/downloads/`).
pub const DOWNLOAD_FOLDER: &str = ".underload/downloads";
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

/// Performs one GET, racing the caller's cancellation token so an abort closes the
/// response stream instead of waiting for the body.
///
/// # Errors
///
/// The status sentence for a non-2xx response, or the transport failure text.
async fn get(
    client: &reqwest::Client,
    url: &str,
    signal: &CancellationToken,
) -> Result<reqwest::Response, String> {
    let request = client
        .get(url)
        .header(
            "user-agent",
            "typecrate-harness/1.0 (+https://github.com/noobscript)",
        )
        .send();

    tokio::select! {
        biased;
        () = signal.cancelled() => Err("the request was cancelled.".to_owned()),
        outcome = request => match outcome {
            Ok(response) if response.status().is_success() => Ok(response),
            Ok(response) => Err(status_sentence(response.status().as_u16())),
            Err(error) => Err(format!("request to {url} failed: {error}")),
        },
    }
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
/// `webfetch/request.ts:85` `MAX_RESULT_BYTES` and `:91` `RESULT_OVERHEAD_BYTES`: the room a
/// fetch result's own keys cost, measured rather than guessed.
pub const FETCH_MAX_RESULT_BYTES: usize = 64 * 1024;
/// `webfetch/request.ts:91`.
pub const FETCH_RESULT_OVERHEAD_BYTES: usize = 2_048;
/// `webfetch/request.ts:99` `MAX_RESULT_CONTENT_BYTES` = 65 536 − 2 048 = 63 488 bytes of
/// `content` per result.
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

    // Snap to line boundaries. A page with no newlines at all has nothing to snap to, so the
    // raw window is used rather than collapsing to an empty block.
    let from = haystack[..start]
        .iter()
        .rposition(|character| *character == '\n')
        .map_or(start, |line| line + 1);
    let to = haystack[end..]
        .iter()
        .position(|character| *character == '\n')
        .map_or(haystack.len(), |line| end + line);

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

/// `webfetch/index.ts:282` `fitResult`: shrink the string until its JSON-serialized form fits
/// the harness's model-facing budget, leaving room for the rest of the result object. The
/// harness WOULD do this itself, but its envelope replaces the whole result and would drop the
/// `url`/`title`/`nextOffset` a model needs to resume.
#[must_use]
pub fn fit_result(content: &str) -> String {
    if content.len() <= FETCH_MAX_RESULT_CONTENT_BYTES {
        return content.to_owned();
    }

    // A code-unit slice is an upper bound on a byte-fitting slice; the step is
    // `Math.max(1, Math.floor(keep * 0.05))`, i.e. one twentieth, never zero.
    let mut keep = code_units(content).min(FETCH_MAX_RESULT_CONTENT_BYTES);
    let mut candidate = slice_units(content, keep).to_owned();

    while keep > 0 && candidate.len() > FETCH_MAX_RESULT_CONTENT_BYTES {
        keep = keep.saturating_sub(keep.max(1) / 20);
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
        let flat = sectioned.total_sections <= FETCH_FLAT_DOCUMENT_SECTIONS;
        let answered = if flat && sectioned.total_matches > 0 {
            select_proximity(&safe_content, query)
        } else {
            sectioned
        };

        if answered.matches.is_empty() {
            return Err(format!(
                "fetch found no section matching \"{query}\" in a {page_characters}-character page. Re-fetch without \"query\" to read the whole page, or use a shorter phrase."
            ));
        }

        let joined = answered
            .matches
            .iter()
            .map(|matched| {
                format!(
                    "## {} [line {}]\n{}",
                    if matched.heading.is_empty() {
                        "(no heading)"
                    } else {
                        &matched.heading
                    },
                    matched.line + 1,
                    matched.text
                )
            })
            .collect::<Vec<_>>()
            .join("\n\n");

        let bounded_sections = fit_result(&joined);
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
            ("characters", json!(code_units(&bounded_sections))),
            (
                "truncated",
                json!(
                    code_units(&bounded_sections) < code_units(&joined)
                        || answered.total_matches > answered.matches.len()
                ),
            ),
            ("content", json!(bounded_sections)),
            ("query", json!(query)),
            ("matches", json!(answered.matches.len())),
            ("totalSections", json!(answered.total_sections)),
            ("totalMatches", json!(answered.total_matches)),
            ("matchMode", json!(answered.match_mode)),
        ] {
            result.insert(key.to_owned(), value);
        }

        // A proximity hit is an EXCERPT, not a section, and the two are not equally
        // authoritative — so the mode that ran is spelled out.
        if answered.match_mode == "proximity" {
            result.insert("note".to_owned(), json!("This page has no heading structure, so \"query\" returned a text window around the first match instead of a section. Use \"offset\" to read further."));
        }

        result.insert("pageCharacters".to_owned(), json!(page_characters));

        if let Some(text) = title
            .as_deref()
            .map(|text| safe_title(&redactor, text))
            .filter(|text| !text.is_empty())
        {
            result.insert("title".to_owned(), Value::String(text));
        }

        return Ok(Value::Object(result));
    }

    // The character bound and the RESULT bound are two different limits: MAX_CONTENT_CHARS is
    // what a page may yield across all of its pages, and `fit_result` then keeps ONE result
    // inside the model-facing cap.
    let window = slice_units(window, MAX_CONTENT_CHARS);
    let output = fit_result(window);
    let output_length = code_units(&output);

    // True only when bytes exist BEYOND what is returned here: the end of the last window is
    // not truncation.
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
        ("characters", json!(output_length)),
        ("truncated", json!(truncated)),
        ("content", json!(output)),
        ("pageCharacters", json!(page_characters)),
        ("offset", json!(start_offset)),
    ] {
        result.insert(key.to_owned(), value);
    }

    // Where the NEXT window starts, present only when more exists. Every other content tool in
    // the harness pages; fetch used to be the one that silently lost the rest.
    if truncated {
        result.insert("nextOffset".to_owned(), json!(start_offset + output_length));
    }

    if offset_clamped {
        result.insert("offsetClamped".to_owned(), json!(true));
        result.insert(
            "note".to_owned(),
            json!(format!(
                "Requested offset {} is past the end of this {page_characters}-character page; showing what remains from {start_offset}. The page is fully read once \"content\" is empty.",
                invocation.offset
            )),
        );
    }

    if let Some(text) = title
        .as_deref()
        .map(|text| safe_title(&redactor, text))
        .filter(|text| !text.is_empty())
    {
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
/// AND while streaming.
pub const SEARCH_MAX_RESPONSE_BYTES: usize = 512 * 1024;
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
) -> Vec<Value> {
    let mut matches: Vec<Value> = Vec::new();

    for row in rows.iter().take(max_results) {
        let Value::Object(row) = row else { continue };

        let fields = reader.read(row);

        let Some(url) = valid_url(fields.url.as_deref()) else {
            continue;
        };

        // Redaction can LENGTHEN a URL (`[redacted]` is longer than a short key), and a URL
        // that no longer fits is dropped rather than truncated.
        let safe_url = redactor.redact(&url);

        if code_units(&safe_url) > SEARCH_MAX_URL_CHARS {
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

    matches
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

    Ok(SearchInvocation {
        max_results: read_max_results(input.get("maxResults"))?,
        provider: read_provider(input.get("provider"), configured)?,
        include_domains: read_domains(input.get("includeDomains"), "includeDomains")?,
        exclude_domains: read_domains(input.get("excludeDomains"), "excludeDomains")?,
        since,
        until,
        category: read_category(input.get("category"))?,
        speed: read_speed(input.get("speed"))?,
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
) -> Result<Vec<Value>, String> {
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
) -> Result<Vec<Value>, String> {
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
) -> Result<Vec<Value>, String> {
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
    let results = if invocation.provider == "exa" {
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

    Ok(result)
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

/// A plain file name out of an untrusted string: no path, no separators, no control
/// characters, at most 120 characters (`download.tool.ts`'s sanitizer).
#[must_use]
pub fn sanitize_name(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|character| !character.is_control() && !matches!(character, '/' | '\\' | ':' | '?'))
        .collect();

    let cleaned = cleaned.trim().replace(' ', "-");

    if cleaned.is_empty() {
        return None;
    }

    Some(cleaned.chars().take(120).collect())
}

/// The name a download takes when the caller did not supply one: the URL's last path
/// segment, or a fixed fallback.
fn name_from_url(url: &str) -> String {
    let tail = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned();

    sanitize_name(&tail).unwrap_or_else(|| "download".to_owned())
}

/// `download`: `{url, name?, path?}` -> the `toolMedia(summary, [attachment])` envelope.
///
/// The bytes travel ONCE, on the attachment; the model reads only the summary, and the
/// event carries descriptors. The cap is enforced against the streamed body, so an
/// oversized page never lands on disk.
///
/// # Errors
///
/// A bad URL, a status sentence, the size refusal, or a write failure.
pub(crate) async fn download(
    client: &reqwest::Client,
    arguments: &Value,
    cwd: &std::path::Path,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let requested = super::result::required_text(arguments, "url", "download")?;
    let url = validated_url(&requested)?;

    let mut response = get(client, &url, signal).await?;
    let final_url = response.url().to_string();
    let mime = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_owned();

    // The cap is applied while reading: `content_length()` alone would trust a header a
    // hostile host can lie about.
    let mut bytes: Vec<u8> = Vec::new();

    loop {
        let chunk = tokio::select! {
            biased;
            () = signal.cancelled() => return Err("the download was cancelled.".to_owned()),
            outcome = response.chunk() => outcome
                .map_err(|error| format!("download could not read the response body: {error}"))?,
        };

        let Some(chunk) = chunk else {
            break;
        };

        // The bound is a `u64` byte count; the running total is a `usize` buffer length, so the
        // sum is widened rather than the limit narrowed (a 32-bit host cannot overflow past the
        // limit unnoticed this way).
        if u64::try_from(bytes.len() + chunk.len()).unwrap_or(u64::MAX) > MAX_DOWNLOAD_BYTES {
            return Err(format!(
                "download refused the response: it is larger than the {}-byte limit.",
                MAX_DOWNLOAD_BYTES
            ));
        }

        bytes.extend_from_slice(&chunk);
    }

    let name = super::result::optional_text(arguments, "name")
        .as_deref()
        .and_then(sanitize_name)
        .unwrap_or_else(|| name_from_url(&final_url));

    let folder = match super::result::optional_text(arguments, "path") {
        Some(raw) if !raw.is_empty() => {
            if raw.contains("..") || raw.contains('\0') {
                return Err("download path must be a destination relative to the working directory without \"..\".".to_owned());
            }

            cwd.join(raw)
        }
        _ => cwd.join(DOWNLOAD_FOLDER),
    };

    tokio::fs::create_dir_all(&folder).await.map_err(|error| {
        format!(
            "download could not create \"{}\": {error}",
            folder.display()
        )
    })?;

    let destination = folder.join(&name);

    tokio::fs::write(&destination, &bytes)
        .await
        .map_err(|error| {
            format!(
                "download could not write \"{}\": {error}",
                destination.display()
            )
        })?;

    let kind = kind_for_mime(&mime);
    let relative = destination
        .strip_prefix(cwd)
        .unwrap_or(&destination)
        .to_string_lossy()
        .replace('\\', "/");

    // `[Image 1]`-style markers come from the attachment kind, the same rule the composer
    // uses (`shared/attachments.ts` `attachmentMarker`).
    let marker = format!(
        "[{} 1]",
        match kind {
            AttachmentKind::Image => "Image",
            AttachmentKind::Video => "Video",
            AttachmentKind::Audio => "Audio",
            AttachmentKind::Pdf => "PDF",
            AttachmentKind::File => "File",
        }
    );

    let attachment = HarnessAttachment {
        id: "dl-1".to_owned(),
        kind,
        marker,
        name: name.clone(),
        mime: mime.clone(),
        size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        data: BASE64.encode(&bytes),
    };

    let summary = json!({
        "name": name,
        "path": relative,
        "mime": mime,
        "kind": kind,
        "size": bytes.len(),
        "url": final_url,
        "saved": true,
    });

    Ok(json!({
        TOOL_MEDIA_BRAND: [attachment],
        "content": summary,
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
        assert_eq!(sanitize_name("a/b\\c:d?").as_deref(), Some("abcd"));
        assert!(
            sanitize_name("  ").is_none(),
            "a name that is only whitespace is no name at all"
        );
        assert_eq!(
            sanitize_name("two words.png").as_deref(),
            Some("two-words.png")
        );
        assert_eq!(
            name_from_url("https://h/a/b/shot.PNG?v=1"),
            "shot.PNG".to_owned()
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
                "note": "This page has no heading structure, so \"query\" returned a text window around the first match instead of a section. Use \"offset\" to read further.",
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

        // Paging from `nextOffset` returns the rest, and a window that ends exactly at the
        // end of the document is NOT truncation: `truncated` is computed from the source
        // length, which is what fixes the off-by-one where a page cut exactly at the budget
        // claimed to be whole.
        let second = fetch(
            &client,
            &json!({"url": format!("{base}/long"), "offset": FETCH_MAX_RESULT_CONTENT_BYTES}),
            &keys,
            &signal,
        )
        .await
        .unwrap();
        assert_eq!(
            second["characters"],
            json!(70_000 - FETCH_MAX_RESULT_CONTENT_BYTES)
        );
        assert_eq!(second["truncated"], json!(false));
        assert_eq!(second["offset"], json!(FETCH_MAX_RESULT_CONTENT_BYTES));
        assert!(second.get("nextOffset").is_none());

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
    async fn stub_origin(reply: &str) -> (String, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port");
        let address = listener.local_addr().expect("a bound address");
        let reply = reply.to_owned();

        let (sender, receiver) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
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

        (format!("http://{address}"), receiver)
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
        }
    }

    /// Runs `search` against a stub and returns the published value plus the raw request.
    async fn stubbed_search(
        provider: &str,
        key: &str,
        payload: &str,
        arguments: Value,
    ) -> (Value, String) {
        let (base, receiver) = stub_origin(&reply_with(payload)).await;
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
        let (base, receiver) = stub_origin(&reply_with(
            r#"{"results":[{"title":"leak exa-secret-key","url":"https://example.com/exa-secret-key","highlights":["body exa-secret-key and tavily-secret-key"],"publishedDate":"2026-01-02"}]}"#,
        ))
        .await;
        let client = reqwest::Client::new();
        let keys = WebKeys {
            exa: Some("exa-secret-key".to_owned()),
            tavily: Some("tavily-secret-key".to_owned()),
            linkup: None,
            endpoint_base: Some(base),
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
        let (base, receiver) = stub_origin(&reply_with(
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
        let matches = normalize_matches(&rows, 5, &redactor, RowReader::Exa);

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

        let matches = normalize_matches(&rows, 5, &redactor, RowReader::Exa);

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
            normalize_matches(&rows, 5, &redactor, RowReader::Exa)[0]["published"],
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
        let matches = normalize_matches(&rows, 5, &redactor, RowReader::Exa);

        // Only the last row survives, and the URL is NORMALIZED the way `new URL()` does:
        // a bare origin gains its trailing slash.
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
            let (base, receiver) = stub_origin(&format!(
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
        let (base, receiver) = stub_origin(&reply_with(r#"{"data":[]}"#)).await;
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

        let (base, receiver) = stub_origin(&reply_with("<html>nope</html>")).await;

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
        let (base, receiver) = stub_origin(
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
}
