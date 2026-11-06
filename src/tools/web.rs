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
//! `webfetch/` has a five-provider dispatcher (native HTML->markdown conversion, Exa
//! livecrawl, Linkup, a text extractor and a redaction pass). The Rust `fetch` is the
//! NATIVE provider only: it fetches, strips markup to readable text, caps at
//! [`MAX_CONTENT_CHARS`], and reports the same result document. `search` calls a real
//! provider and normalizes its rows to the shared match shape. Both state their own
//! `provider` field, so a caller is never told a richer pipeline ran than did.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::contract::{AttachmentKind, HarnessAttachment};
use crate::tools::result::TOOL_MEDIA_BRAND;

/// The webfetch content cap, in characters (`webfetch/index.ts`'s `MAX_CONTENT_CHARS` is
/// the same 64 KiB order of magnitude; the stage contract states 64 KiB).
pub const MAX_CONTENT_CHARS: usize = 64 * 1024;
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
        }
    }

    /// True when at least one search provider can be called.
    #[must_use]
    pub fn has_search_key(&self) -> bool {
        self.exa.is_some() || self.tavily.is_some() || self.linkup.is_some()
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
// fetch
// ---------------------------------------------------------------------------

/// `fetch`: `{url}` -> the `WebFetchResult` document, native provider only.
///
/// The body is converted to readable text by dropping `<script>`/`<style>` content and
/// every tag, then collapsing whitespace. Untrusted page text is data, never an
/// instruction, which is what the surrounding harness prompt rule already states.
///
/// # Errors
///
/// A malformed URL, a provider status sentence, or a transport failure.
pub(crate) async fn fetch(
    client: &reqwest::Client,
    arguments: &Value,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let requested = super::result::required_text(arguments, "url", "fetch")?;
    let url = validated_url(&requested)?;
    let response = get(client, &url, signal).await?;

    let final_url = response.url().to_string();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();

    let bytes = tokio::select! {
        biased;
        () = signal.cancelled() => return Err("the request was cancelled.".to_owned()),
        outcome = response.bytes() => outcome.map_err(|error| format!("fetch could not read the response body: {error}"))?,
    };

    let raw = String::from_utf8_lossy(&bytes).into_owned();
    let converted = if content_type.contains("html") {
        html_to_text(&raw)
    } else {
        raw.trim().to_owned()
    };

    let title = content_type
        .contains("html")
        .then(|| extract_title(&raw))
        .flatten();
    let truncated = converted.chars().count() > MAX_CONTENT_CHARS;
    let content: String = converted.chars().take(MAX_CONTENT_CHARS).collect();
    let characters = content.chars().count();

    let mut result = json!({
        "provider": "native",
        "url": final_url,
        "contentType": content_type,
        "format": if content_type.contains("html") { "markdown" } else { "text" },
        "characters": characters,
        "truncated": truncated,
        "content": content,
    });

    // `title` rides only when a title was actually found (an empty string is not one).
    if let Some(text) = title.filter(|text| !text.is_empty()) {
        result["title"] = json!(text);
    }

    Ok(result)
}

/// Pulls the `<title>` text out of an HTML document.
fn extract_title(html: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let open = lower.find("<title")?;
    let start = lower[open..].find('>')? + open + 1;
    let end = start + lower[start..].find("<")?;

    Some(
        html[start..end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(200)
            .collect(),
    )
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

/// `search`: `{query, maxResults?}` -> the provider-normalized `WebSearchResult`
/// (`websearch/index.ts`'s dispatch, one provider per available key in the documented
/// preference order).
///
/// # Errors
///
/// The unconfigured hint when no key exists, or the provider's own failure sentence.
pub(crate) async fn search(
    client: &reqwest::Client,
    arguments: &Value,
    keys: &WebKeys,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let query = super::result::required_text(arguments, "query", "search")?;
    let max_results =
        super::result::bounded_integer(arguments, "maxResults", 8, 1, 25).unwrap_or(8);

    if !keys.has_search_key() {
        return Err(WEBSEARCH_UNCONFIGURED_HINT.to_owned());
    }

    let (provider, outcome) = if let Some(key) = &keys.exa {
        (
            "exa",
            exa_search(client, &query, max_results, key, signal).await?,
        )
    } else if let Some(key) = &keys.tavily {
        (
            "tavily",
            tavily_search(client, &query, max_results, key, signal).await?,
        )
    } else {
        (
            "linkup",
            linkup_search(
                client,
                &query,
                max_results,
                keys.linkup.as_deref().unwrap_or_default(),
                signal,
            )
            .await?,
        )
    };

    Ok(json!({
        "provider": provider,
        "query": query,
        "maxResults": max_results,
        "results": outcome,
    }))
}

/// One provider call's shared plumbing: POST, bearer/x-api-key header, JSON answer.
async fn provider_post(
    client: &reqwest::Client,
    endpoint: &str,
    label: &str,
    headers: &[(&str, String)],
    body: Value,
    signal: &CancellationToken,
) -> Result<Value, String> {
    let mut request = client.post(endpoint).json(&body);

    for (name, value) in headers {
        request = request.header(*name, value);
    }

    let response = tokio::select! {
        biased;
        () = signal.cancelled() => return Err("the request was cancelled.".to_owned()),
        outcome = request.send() => outcome
            .map_err(|error| format!("{label} request failed: {error}"))?,
    };

    if !response.status().is_success() {
        return Err(format!(
            "{label} returned HTTP {}.",
            response.status().as_u16()
        ));
    }

    response
        .json::<Value>()
        .await
        .map_err(|error| format!("{label} returned an unreadable answer: {error}"))
}

/// Normalizes provider rows into `{title, url, snippet, published?}` and applies the
/// count bound (`websearch/request.ts` `normalizeMatches`).
fn normalize_rows(rows: &[Value], max_results: i64) -> Vec<Value> {
    rows.iter()
        .filter_map(|row| {
            let url = row.get("url").and_then(Value::as_str)?;
            let title = row.get("title").and_then(Value::as_str).unwrap_or("");

            let snippet = row
                .get("highlights")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .or_else(|| {
                    row.get("content")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .or_else(|| {
                    row.get("snippet")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_default();

            let mut entry = json!({
                "title": title,
                "url": url,
                "snippet": snippet.chars().take(600).collect::<String>(),
            });

            if let Some(published) = row
                .get("publishedDate")
                .or_else(|| row.get("published_date"))
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                entry["published"] = json!(published);
            }

            Some(entry)
        })
        .take(usize::try_from(max_results).unwrap_or(8))
        .collect()
}

/// `websearch/exa.ts`: `POST https://api.exa.ai/search`, key in `x-api-key`.
async fn exa_search(
    client: &reqwest::Client,
    query: &str,
    max_results: i64,
    key: &str,
    signal: &CancellationToken,
) -> Result<Vec<Value>, String> {
    let payload = provider_post(
        client,
        "https://api.exa.ai/search",
        "Exa",
        &[("x-api-key", key.to_owned())],
        json!({
            "query": query,
            "type": "auto",
            "numResults": max_results,
            "contents": { "highlights": true },
        }),
        signal,
    )
    .await?;

    Ok(normalize_rows(
        payload
            .get("results")
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice),
        max_results,
    ))
}

/// `websearch/tavily.ts`: `POST https://api.tavily.com/search`, bearer key.
async fn tavily_search(
    client: &reqwest::Client,
    query: &str,
    max_results: i64,
    key: &str,
    signal: &CancellationToken,
) -> Result<Vec<Value>, String> {
    let payload = provider_post(
        client,
        "https://api.tavily.com/search",
        "Tavily",
        &[("authorization", format!("Bearer {key}"))],
        json!({ "query": query, "max_results": max_results, "include_answer": false }),
        signal,
    )
    .await?;

    Ok(normalize_rows(
        payload
            .get("results")
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice),
        max_results,
    ))
}

/// `websearch/linkup.ts`: `POST https://api.linkup.so/v1/search`, `x-api-key`.
async fn linkup_search(
    client: &reqwest::Client,
    query: &str,
    max_results: i64,
    key: &str,
    signal: &CancellationToken,
) -> Result<Vec<Value>, String> {
    let payload = provider_post(
        client,
        "https://api.linkup.so/v1/search",
        "Linkup",
        &[("x-api-key", key.to_owned())],
        json!({ "q": query, "depth": "standard", "outputFormat": "markdown" }),
        signal,
    )
    .await?;

    Ok(normalize_rows(
        payload
            .get("data")
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice),
        max_results,
    ))
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
}
