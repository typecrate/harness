//! The static mirror: the built UI served beside the API mount, with the reference's cache headers.
//!
//! TypeScript origin: `packages/underload/harness/src/server/static.ts` (101 lines), the ONE static
//! handler the reference ships (`createStaticHandler`). Its owner is `src/server/start.ts`, which
//! wires the request in exactly the order this crate now copies (`start.ts:404-414`):
//!
//! ```text
//!   api(request, response)      -> if it answered, done
//!   serveStatic(request, ...)   -> if it answered, done
//!   sendNotFound(response)      -> 404 text/plain "Not found"
//! ```
//!
//! THE ORDER IS THE CONTRACT, and two of its consequences are easy to get wrong:
//!
//!   1. the API adapter takes EVERY path under `/v1/harness` and nothing else -
//!      `handler.ts:277` is literally `if (!url.pathname.startsWith(STANDALONE_MOUNT)) return false`
//!      with the comment "the static handler owns everything outside the API mount". So the mirror
//!      is the NON-API namespace (`/`, `/assets/*`, `/style.css`, a SPA route such as `/sessions/abc`),
//!      never `/v1/harness/*`;
//!   2. a mirror that declines (unknown file, non-GET verb, traversal) falls through to
//!      `sendNotFound` (`start.ts:165-174`), which is a PLAIN-TEXT 404 that does NOT name the mount.
//!      The JSON 404 that names `/v1/harness` is the DISPATCHER's answer, and it only ever appears
//!      for a path inside the mount (`api/mod.rs`'s mount sentence).
//!
//! The cache rule is therefore also the reference's, not a invented one: `static.ts:45-50` stamps
//! `public, max-age=31536000, immutable` on a file whose RESOLVED path contains an `assets`
//! DIRECTORY (the hashed, content-addressed output a bundling build emits) and `no-cache` on
//! everything else, html included. There is no separate "html is special" branch: html falls out of
//! the same rule because it never lives under `assets/`.
//!
//! WHAT IS DELIBERATELY NOT HERE
//! -----------------------------
//!   * No default root. `start.ts:396-402` states it outright - "the deployable UI build is owned by
//!     `@loadscript/harness`, so this server no longer carries a default" - and `src/server/dev.ts:123`
//!     passes `staticRoot: null`. So the Rust mirror is `Option`-based and OFF unless a host or an
//!     operator names a directory; nothing in this crate duplicates or commits build output.
//!   * No streaming and no `Range`/`ETag`/gzip handling, because `static.ts` has none: it sets
//!     `Content-Length` from `stat` and pipes the bytes. The reference's own `.br`/`.gz` siblings in
//!     `packages/loadscript/harness/src/frontend/dist` are served by the hosting proxy, not here.
//!     The one honest deviation is the read: this handler reads the whole file into memory
//!     (`std::fs::read`) instead of streaming it, because an axum handler returns one `Response`
//!     body anyway and the mirror's largest asset is a few megabytes.
//!   * No websocket/SSE special case: `/v1/harness/stream` is inside the mount, so the dispatcher
//!     answers it (`handler.ts:279-284` refuses a plain `GET` there, and this crate's
//!     `api::handle_stream` says the same thing).

use std::fs;
use std::path::{Component, Path, PathBuf};

use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::package_root;

/// `start.ts:172-173` - the content type of the terminal 404 the standalone listener answers when
/// neither the dispatcher nor the mirror took the request.
pub const NOT_FOUND_TYPE: &str = "text/plain; charset=utf-8";

/// `start.ts:173` - that 404's whole body, byte for byte. It does NOT name `/v1/harness`; the JSON
/// sentence that does is the dispatcher's, and the two must not be merged.
pub const NOT_FOUND_BODY: &str = "Not found";

/// `static.ts:48` - the cache header for a content-hashed asset.
pub const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// `static.ts:49` - the cache header for everything else, including `index.html`.
pub const CACHE_NO_CACHE: &str = "no-cache";

/// The directory whose files are considered immutable (`static.ts:47`'s `assets` segment).
pub const ASSETS_DIRECTORY: &str = "assets";

/// The extension -> media-type table, `static.ts:13-24`, in the reference's own declaration order.
/// Anything outside this table is `application/octet-stream` (`static.ts:27`).
pub const MIME_TYPES: [(&str, &str); 11] = [
    (".css", "text/css; charset=utf-8"),
    (".html", "text/html; charset=utf-8"),
    (".ico", "image/x-icon"),
    (".jpeg", "image/jpeg"),
    (".jpg", "image/jpeg"),
    (".js", "text/javascript; charset=utf-8"),
    (".json", "application/json; charset=utf-8"),
    (".map", "application/json; charset=utf-8"),
    (".png", "image/png"),
    (".svg", "image/svg+xml"),
    (".webp", "image/webp"),
];

/// The media type of `file_path`: `contentType` (`static.ts:26-27`), i.e. the lowercased extension
/// looked up in [`MIME_TYPES`], defaulting to octet-stream.
///
/// The lookup keys on the FILE, not on the request path: a request for `/app.JSON` that resolves to
/// `app.JSON` reports the `.json` type, and a request for an extensionless SPA route that falls back
/// to `index.html` reports html - which is exactly what makes the SPA fallback readable as JSON-free
/// markup instead of octet-stream.
#[must_use]
pub fn content_type_of(file_path: &Path) -> &'static str {
    // `path.extname` is the dot plus everything after the LAST dot of the base name, and it is EMPTY
    // for a base name that starts with a dot (`.gitignore` has no extension in Node's model, and
    // therefore no media type of its own).
    let name = file_path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();

    match extension_of(&name) {
        Some(extension) => MIME_TYPES
            .iter()
            .find(|(key, _)| *key == extension)
            .map(|(_, value)| *value)
            .unwrap_or("application/octet-stream"),
        None => "application/octet-stream",
    }
}

/// Node's `path.extname` for one base name: the `.`-prefixed suffix after the LAST dot, or `None`
/// when the name holds no dot or its only dot opens the name (`.gitignore` has no extension).
/// Lowercased, because `static.ts:27` lowercases before the lookup.
///
/// A trailing dot is kept as `"."`, which is what Node returns for `app.`; it matches no table entry,
/// so the media type is octet-stream while the name still COUNTS as having an extension - the
/// difference matters for the SPA fallback, which serves `index.html` only for a name with NO
/// extension at all (`static.ts:95`).
pub fn extension_of(name: &str) -> Option<String> {
    let index = name.rfind('.')?;

    // A leading dot is part of the name, and an all-dots name (`.`/`..`) has no extension either.
    if index == 0 || name.bytes().all(|byte| byte == b'.') {
        return None;
    }

    Some(name[index..].to_lowercase())
}

/// The cache header for one resolved file: `static.ts:45-50`.
///
/// The test is `filePath.includes(`${sep}assets${sep}`)`, i.e. an `assets` DIRECTORY component in
/// the absolute path - not a file called `assets`, and not a path that merely mentions it. Walking
/// the parent components is the platform-correct spelling of the same rule.
#[must_use]
pub fn cache_control_for(file_path: &Path) -> &'static str {
    let in_assets = file_path
        .parent()
        .into_iter()
        .flat_map(|parent| parent.components())
        .any(|component| matches!(component, Component::Normal(name) if name == ASSETS_DIRECTORY));

    if in_assets {
        CACHE_IMMUTABLE
    } else {
        CACHE_NO_CACHE
    }
}

/// One file the mirror decided to send: the three headers `static.ts` sets plus the bytes.
///
/// `body` is EMPTY for a `HEAD` request while `content_length` still carries the file size, which is
/// what `static.ts:51-56` does (`Content-Length` is stamped, then `response.end()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticReply {
    pub status: u16,
    pub content_type: &'static str,
    pub cache_control: &'static str,
    /// The bytes to send. For a `HEAD` request this is empty by design.
    pub body: Vec<u8>,
    /// The `Content-Length` to stamp, which is the FILE size even on `HEAD`.
    pub content_length: u64,
}

impl StaticReply {
    /// `static.ts:43-51`: 200, the media type, the cache header, and the size.
    #[must_use]
    pub fn file(file_path: &Path, body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: content_type_of(file_path),
            cache_control: cache_control_for(file_path),
            content_length: body.len() as u64,
            body,
        }
    }

    /// The `HEAD` form: identical headers, no bytes (`static.ts:53-56`).
    #[must_use]
    pub fn head(file_path: &Path, content_length: u64) -> Self {
        Self {
            status: 200,
            content_type: content_type_of(file_path),
            cache_control: cache_control_for(file_path),
            content_length,
            body: Vec::new(),
        }
    }
}

impl IntoResponse for StaticReply {
    fn into_response(self) -> Response {
        // `build()` only fails on an invalid header name or value, and every value here comes from a
        // literal table or from a `u64`, so an invalid one would be a bug in this file rather than a
        // condition a caller can recover from.
        axum::response::Response::builder()
            .status(StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
            .header(header::CONTENT_TYPE, self.content_type)
            .header(header::CACHE_CONTROL, self.cache_control)
            .header(header::CONTENT_LENGTH, self.content_length)
            .body(Body::from(self.body))
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .expect("an empty 500 builds")
            })
    }
}

/// The mirror over one built UI directory (`createStaticHandler(root)`).
///
/// The root is normalised at construction exactly like `static.ts:32` (`path.resolve(root)`), so the
/// traversal test below compares two lexically-clean paths and cannot be fooled by `.` or `..` in
/// the directory the operator passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticMirror {
    root: PathBuf,
}

impl StaticMirror {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: package_root::normalize_lexically(&root.into()),
        }
    }

    /// The directory this mirror serves, normalised (`static.ts:32`'s `absoluteRoot`).
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The mirror's answer for one request, or `None` when it declines and the caller must fall
    /// through - the `Promise<boolean>` of `StaticHandler` (`static.ts:71-99`), read as an `Option`.
    ///
    /// `target` is the request's PATH (a query string is tolerated and dropped, because
    /// `new URL(...)` separates it in the reference before anything else happens).
    pub fn handle(&self, method: &Method, target: &str) -> Option<StaticReply> {
        // `static.ts:72` - only GET and HEAD are ever served. A POST/PUT/PATCH/DELETE to a mirror path
        // is NOT a 405 here: it falls through, and since `start.ts` puts the dispatcher first and the
        // dispatcher never answers an unmounted path, the terminal answer is the plain 404. That is
        // the reference's behaviour, and it is the one a browser probing a wrong verb should get.
        if method != Method::GET && method != Method::HEAD {
            return None;
        }

        // `static.ts:74-81` - the URL parse keeps the pathname, and a malformed percent escape makes
        // `decodeURIComponent` THROW, which the reference treats as "not mine".
        let decoded = percent_decode(pathname_of(target))?;

        // `static.ts:83` - `/` is the entry document, not a directory listing.
        let requested = if decoded == "/" {
            "/index.html".to_owned()
        } else {
            decoded
        };

        let file_path = self.resolve(&requested)?;

        // `static.ts:91` - a real file wins immediately.
        if let Some(reply) = self.send(method, &file_path) {
            return Some(reply);
        }

        // `static.ts:93-97` - the single-page-app fallback: an EXTENSIONLESS route renders `index.html`
        // so a client-side router can own `/sessions/<id>`, while a missing asset with an extension
        // stays a 404 rather than becoming HTML. A page full of JS at a 404 URL is the bug this rule
        // exists to prevent.
        let base_name = requested.rsplit('/').next().unwrap_or_default();

        if extension_of(base_name).is_none() {
            return self.send(method, &self.root.join("index.html"));
        }

        None
    }

    /// Resolves one decoded request path under the root, rejecting every traversal (`static.ts:84-89`),
    /// including an encoded one: `%2e%2e` decodes to `..` BEFORE the resolve, so `path.resolve`
    /// collapses it and the containment test sees the result.
    pub fn resolve(&self, requested: &str) -> Option<PathBuf> {
        if !requested.starts_with('/') {
            return None;
        }

        // `path.resolve(absoluteRoot, `.${requested}`)`: the `.` keeps the join anchored to the root,
        // so an absolute-looking request segment can never replace it.
        let joined = self.root.join(format!(".{requested}"));
        let resolved = package_root::normalize_lexically(&joined);

        if resolved != self.root && !resolved.starts_with(&self.root) {
            return None;
        }

        Some(resolved)
    }

    /// Reads one file and produces the reply, or `None` when the path is not a readable REGULAR file
    /// (`static.ts:40-41`'s `isFile()` test plus its `catch` arm, which both mean "not mine").
    #[must_use]
    fn send(&self, method: &Method, file_path: &Path) -> Option<StaticReply> {
        let metadata = fs::metadata(file_path).ok()?;

        if !metadata.is_file() {
            return None;
        }

        if method == Method::HEAD {
            return Some(StaticReply::head(file_path, metadata.len()));
        }

        let body = fs::read(file_path).ok()?;

        Some(StaticReply::file(file_path, body))
    }
}

/// Keeps the pathname out of a request target: the query starts at the first `?` and a fragment at
/// the first `#` (`new URL(raw, base).pathname`, `static.ts:74`).
fn pathname_of(target: &str) -> &str {
    let without_query = target.split('?').next().unwrap_or(target);

    without_query.split('#').next().unwrap_or(without_query)
}

/// `decodeURIComponent` (`static.ts:78`): every `%XX` becomes its byte, and a `%` that is NOT followed
/// by two hex digits is a THROW - which this reports as `None`, the same thing the reference's
/// `try`/`catch` does. The decoded bytes must also be valid UTF-8, or the filename is not
/// representable here.
pub fn percent_decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let hex = bytes.get(index + 1..index + 3)?;
                let value = (hex_value(hex[0])? << 4) | hex_value(hex[1])?;

                out.push(value);
                index += 3;
            }
            other => {
                out.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(out).ok()
}

/// One hex digit's value; anything else is a malformed escape (`decodeURIComponent`'s URIError).
fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The media-type table is the reference's, asserted as a whole so a line added on one side
    /// without the other moves this pin (`static.ts:13-24`).
    #[test]
    fn the_media_type_table_is_the_references_eleven_entries() {
        assert_eq!(
            MIME_TYPES
                .iter()
                .map(|(extension, media_type)| format!("{extension}={media_type}"))
                .collect::<Vec<String>>(),
            vec![
                ".css=text/css; charset=utf-8",
                ".html=text/html; charset=utf-8",
                ".ico=image/x-icon",
                ".jpeg=image/jpeg",
                ".jpg=image/jpeg",
                ".js=text/javascript; charset=utf-8",
                ".json=application/json; charset=utf-8",
                ".map=application/json; charset=utf-8",
                ".png=image/png",
                ".svg=image/svg+xml",
                ".webp=image/webp",
            ]
        );
    }

    #[test]
    fn an_unknown_or_absent_extension_is_octet_stream() {
        assert_eq!(
            content_type_of(Path::new("/mirror/app.wasm")),
            "application/octet-stream"
        );
        assert_eq!(
            content_type_of(Path::new("/mirror/index")),
            "application/octet-stream"
        );
        // Node's `path.extname`: a leading dot is not an extension.
        assert_eq!(
            content_type_of(Path::new("/mirror/.gitignore")),
            "application/octet-stream"
        );
        // The LAST dot wins, and the extension is matched case-insensitively (`static.ts:27`).
        assert_eq!(
            content_type_of(Path::new("/mirror/vendor.min.JS")),
            "text/javascript; charset=utf-8"
        );
    }

    /// `static.ts:47`: an `assets` DIRECTORY is immutable, everything else is revalidated.
    #[test]
    fn only_a_file_under_an_assets_directory_is_immutable() {
        assert_eq!(
            cache_control_for(Path::new("/mirror/assets/index-BY5JC9k_.js")),
            CACHE_IMMUTABLE
        );
        assert_eq!(
            cache_control_for(Path::new("/mirror/assets/deep/nested/chunk-9f.js")),
            CACHE_IMMUTABLE
        );
        assert_eq!(
            cache_control_for(Path::new("/mirror/index.html")),
            CACHE_NO_CACHE
        );
        // A FILE named `assets` is not an assets directory: `includes("/assets/")` is false for it.
        assert_eq!(
            cache_control_for(Path::new("/mirror/assets")),
            CACHE_NO_CACHE
        );
        // And the segment has to be a directory of the path, not a prefix of a name.
        assert_eq!(
            cache_control_for(Path::new("/mirror/assets-extra/app.js")),
            CACHE_NO_CACHE
        );
    }

    /// Node's `path.extname`, which drives BOTH the media type and the SPA-fallback decision.
    #[test]
    fn an_extension_is_the_suffix_after_the_last_dot_never_a_leading_one() {
        assert_eq!(extension_of("index.html").as_deref(), Some(".html"));
        assert_eq!(extension_of("index-8Kq2.js.map").as_deref(), Some(".map"));
        assert_eq!(extension_of("index.HTML").as_deref(), Some(".html"));
        assert_eq!(extension_of(".gitignore"), None);
        assert_eq!(extension_of("index"), None);
        assert_eq!(extension_of(""), None);
        // Node returns `.` for a trailing dot: no media type, but the name is NOT extensionless, so
        // the SPA fallback must not serve HTML for it (`static.ts:95`).
        assert_eq!(extension_of("index.").as_deref(), Some("."));
        assert_eq!(extension_of(".."), None);
    }

    #[test]
    fn a_malformed_escape_is_refused_as_a_fall_through() {
        assert_eq!(percent_decode("/a%20b"), Some("/a b".to_owned()));
        assert_eq!(percent_decode("/a%2Fb"), Some("/a/b".to_owned()));
        assert_eq!(percent_decode("/a%zzb"), None);
        assert_eq!(percent_decode("/a%2"), None);
        // `%c3%28` is a valid escape pair and invalid UTF-8, which is the other half of the same refusal.
        assert_eq!(percent_decode("/a%c3%28b"), None);
    }

    /// A mirror over a directory that need not exist: only the containment decision is under test.
    fn mirror(root: &str) -> StaticMirror {
        StaticMirror::new(PathBuf::from(root))
    }

    #[test]
    fn traversal_outside_the_root_is_rejected_encoded_or_not() {
        let base = mirror("/srv/mirror");

        assert_eq!(
            base.resolve("/assets/app.js"),
            Some(PathBuf::from("/srv/mirror/assets/app.js"))
        );
        assert_eq!(base.resolve("/../secret.txt"), None);
        // `%2e%2e` reaches `..` only after decoding, which is the case the reference's comment calls
        // out: `handle` decodes FIRST and only then resolves, so the decoded form is what the
        // containment test sees. Pinning both halves pins the chain.
        assert_eq!(
            percent_decode("/%2e%2e/secret.txt"),
            Some("/../secret.txt".to_owned())
        );
        assert_eq!(base.resolve("/../secret.txt"), None);
        assert_eq!(
            base.resolve("/./index.html"),
            Some(PathBuf::from("/srv/mirror/index.html"))
        );
        // A nested traversal that stays inside is fine.
        assert_eq!(
            base.resolve("/assets/../index.html"),
            Some(PathBuf::from("/srv/mirror/index.html"))
        );
        // The root itself is a legal target, and a path that is not absolute is not.
        assert_eq!(base.resolve("/"), Some(PathBuf::from("/srv/mirror")));
        assert_eq!(base.resolve("assets/app.js"), None);
        // A sibling directory whose name merely starts with the root's name is NOT inside it.
        assert_eq!(mirror("/srv/mirror").resolve("/../mirror-extra/x.js"), None);
    }

    /// The method gate is `static.ts:72`, and it is a fall-through rather than an answer.
    #[test]
    fn a_write_verb_is_left_for_the_caller() {
        let base = mirror("/srv/mirror");

        assert!(base.handle(&Method::POST, "/index.html").is_none());
        assert!(base.handle(&Method::PUT, "/index.html").is_none());
        assert!(base.handle(&Method::PATCH, "/index.html").is_none());
        assert!(base.handle(&Method::DELETE, "/index.html").is_none());
        // GET and HEAD are the only verbs the mirror ever takes, and a missing file is still a
        // fall-through, not a 404 of its own.
        assert!(base.handle(&Method::GET, "/nope.html").is_none());
        assert!(base.handle(&Method::HEAD, "/nope.html").is_none());
    }

    #[test]
    fn a_query_and_a_fragment_never_reach_the_filesystem() {
        let base = mirror("/srv/mirror");

        assert_eq!(
            base.resolve(pathname_of("/index.html?v=7#top")),
            Some(PathBuf::from("/srv/mirror/index.html"))
        );
        assert_eq!(pathname_of("/index.html?v=7#top"), "/index.html");
        assert_eq!(pathname_of("/index.html"), "/index.html");
    }
}
