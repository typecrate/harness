//! Attachment validation - the SINGLE gate a submit's media passes through.
//!
//! TypeScript origin: `packages/underload/harness/src/shared/attachments.ts` (the validator, the
//! UPLOAD trust profile, the marker/MIME/name/id grammars, the canonical-base64 reader, and the
//! magic-signature table) plus the HTTP mapping in
//! `packages/underload/harness/src/server/sessions.ts:1345-1375` (`ATTACHMENT_TOO_LARGE`,
//! `ATTACHMENT_MEDIA_TYPE`, `attachmentErrorStatus`, `validateAttachmentsForSubmit`).
//!
//! WHAT THIS PORT COVERS
//! ---------------------
//! `attachments.ts` carries TWO trust profiles. This module ports the UPLOAD profile only - the
//! four-file / 4 MiB / 8 MiB composer-and-submit contract the brief names - because the download
//! profile (20 MiB, tool-fetched media) belongs to the tool stage, and its only difference is the
//! three byte/count gates (`:12-17`: "grammar, MIME tables, magic signatures and canonical base64
//! are common"). The grammar constants are therefore shared here, and a later caller that wants
//! the download profile passes different limits rather than a second validator.
//!
//! THE STATUS MAPPING IS PORTED AS TWO REGEXES, NOT AS A RULE TABLE
//! ---------------------------------------------------------------
//! TS throws plain `Error`s from the validator and maps them onto 413/415/400 by TESTING THE
//! MESSAGE TEXT (`sessions.ts:1353-1361`), because the validator is shared with the browser and
//! has no HTTP type to throw. Porting the two regexes verbatim keeps that behaviour identical by
//! construction - including the two places where the regex and the prose disagree:
//!   * `attachments may contain at most 4 entries; received 5.` matches NEITHER regex, so the
//!     per-turn COUNT refusal is a 400 in TS. The stage brief calls that case 413; TS wins
//!     (see `knowledge.md`), and the message is the load-bearing half either way.
//!   * `attachments[N].size is S, but its data decodes to D bytes.` also matches neither, so a
//!     size/length disagreement is a 400, not a 413.
//!
//! `data` is RAW canonical base64 (no `data:` prefix, no whitespace), and the `isCanonicalBase64`
//! rules are ported exactly: whole 4-character groups, at most two trailing `=`, and the DISCARDED
//! tail bits zero - so `AB==` and `AA==` are NOT both legal spellings of one payload
//! (`:213-218`: two encodings for one payload would break byte-stable snapshots and dedupe).

use base64::Engine as _;
use regex::Regex;
use serde_json::Value;

use crate::contract::{
    AttachmentDescriptor, AttachmentKind, HarnessAttachment, MAX_ATTACHMENT_BYTES,
    MAX_ATTACHMENTS_PER_TURN, MAX_TOTAL_ATTACHMENT_BYTES,
};
use crate::error::ApiError;

/// The inline window of the compact display row (`turn-display.ts:169`
/// `DISPLAY_ATTACHMENT_INLINE_CHARS`): 96 KiB of BASE64 TEXT (`turn-projector.ts:2769-2783`
/// measures `attachment.data.length`, i.e. characters, not decoded bytes), and only for an image.
/// Chosen below the 1 MiB route cap so an attachment-heavy turn list stays small.
pub const DISPLAY_ATTACHMENT_INLINE_CHARS: usize = 96 * 1024;

/// Marker label per kind (`attachments.ts:101-107`). PDF also accepts `File`, so a composer that
/// treats a document as a generic file still produces a marker the validator accepts.
const MARKER_LABELS: [(AttachmentKind, &[&str]); 5] = [
    (AttachmentKind::Image, &["Image"]),
    (AttachmentKind::Video, &["Video"]),
    (AttachmentKind::Audio, &["Audio"]),
    (AttachmentKind::Pdf, &["PDF", "File"]),
    (AttachmentKind::File, &["File"]),
];

/// `attachments.ts:136-137` - pre-regex bounds, checked BEFORE any pattern test or interpolated
/// error, so a hostile multi-kilobyte MIME/marker cannot be scanned without bound and can never
/// make an error message huge enough to become its own payload.
const MAX_MIME_LENGTH: usize = 128;
const MAX_MARKER_LENGTH: usize = 32;

/// The standard base64 alphabet (`attachments.ts:197`); URL-safe characters are intentionally
/// absent, because the wire format and the snapshots use canonical base64 only.
const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// `ATTACHMENT_TOO_LARGE` (`sessions.ts:1353`), ported verbatim as a case-insensitive regex.
const TOO_LARGE_PATTERN: &str =
    r"(?i)(?:exceeds|above) the .*byte|byte (?:attachment|per-turn) limit";

/// `ATTACHMENT_MEDIA_TYPE` (`sessions.ts:1354-1355`), ported verbatim.
const MEDIA_TYPE_PATTERN: &str =
    r"(?i)\.mime\b|magic signature|does not match its declared MIME|not valid for kind";

/// The marker grammar (`attachments.ts:110`), checked after the label/kind agreement.
const MARKER_PATTERN: &str = r"^\[(Image|Video|Audio|PDF|File) ([1-9][0-9]*)\]$";

/// A MIME type without parameters (`attachments.ts:124`).
const MIME_PATTERN: &str = r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$";

/// A filename, never a path or URL: no separators, no drive colon, no scheme
/// (`attachments.ts:127`). The character class excludes C0 controls, DEL, `/`, `\` and `:`.
const NAME_PATTERN: &str = r"^[^\u{0}-\u{1f}\u{7f}/\\:]{1,200}$";

/// An opaque id: printable ASCII without spaces, bounded so a snapshot stays small
/// (`attachments.ts:130`).
const ID_PATTERN: &str = r"^[\x21-\x7e]{1,100}$";

/// MIME allowlist per kind (`attachments.ts:114-120`). Unknown `file` entries deliberately have
/// no list: they may be represented, but the transport refuses to send them.
#[must_use]
pub fn allowed_mime_for(kind: AttachmentKind) -> &'static [&'static str] {
    match kind {
        AttachmentKind::Image => &[
            "image/png",
            "image/jpeg",
            "image/jpg",
            "image/webp",
            "image/gif",
        ],
        AttachmentKind::Video => &["video/mp4", "video/webm", "video/ogg", "video/quicktime"],
        AttachmentKind::Audio => &[
            "audio/wav",
            "audio/x-wav",
            "audio/wave",
            "audio/mpeg",
            "audio/mp3",
        ],
        AttachmentKind::Pdf => &["application/pdf"],
        // An empty list means "no MIME contract", which the validator reads as "accept the
        // grammar, check nothing against a kind table" (`attachments.ts:463`).
        AttachmentKind::File => &[],
    }
}

/// The kinds the MIME table can CLAIM a payload for, in table order (`attachments.ts:174`
/// `CLASSIFIABLE_KINDS`). `file` is excluded because its list is empty by design.
#[must_use]
pub fn attachment_kind_of_mime(mime: &str) -> Option<AttachmentKind> {
    [
        AttachmentKind::Image,
        AttachmentKind::Video,
        AttachmentKind::Audio,
        AttachmentKind::Pdf,
    ]
    .into_iter()
    .find(|kind| allowed_mime_for(*kind).contains(&mime))
}

/// Validate one submit's attachment list and return DETACHED copies.
///
/// Every rule of the UPLOAD profile runs, in TS's order per entry (`readAttachment`,
/// `attachments.ts:384-508`), then the cross-entry rules (`validateAttachments`, `:534-557`):
/// unique ids, unique markers, and the per-turn total. An error is an `ApiError` whose status
/// comes from TS's two status regexes applied to its own message, so a byte-bound problem is a
/// 413, a media-type/signature problem is a 415, and everything structural is a 400.
///
/// The list itself (an absent `attachments`, a non-array, an over-count) is checked here too,
/// because the store calls this on EVERY submit (`sessions.ts:1363-1368` says the store re-validates
/// so no path can reach a turn with unvalidated media).
///
/// # Errors
///
/// [`ApiError`] with the TS message verbatim and the status `attachmentErrorStatus` would give it.
pub fn validate_attachments(
    attachments: &[HarnessAttachment],
) -> Result<Vec<HarnessAttachment>, ApiError> {
    // `attachments.length > limits.count` (`attachments.ts:530-532`). See the module header for
    // why this is a 400 in TS while the stage brief calls it 413.
    if attachments.len() > MAX_ATTACHMENTS_PER_TURN {
        return Err(attachment_error(format!(
            "attachments may contain at most {MAX_ATTACHMENTS_PER_TURN} entries; received {}.",
            attachments.len()
        )));
    }

    let mut validated: Vec<HarnessAttachment> = Vec::with_capacity(attachments.len());

    for (index, attachment) in attachments.iter().enumerate() {
        validated.push(read_attachment(attachment, index)?);
    }

    // Unique ids and markers WITHIN one turn (`:539-551`), in list order so the refusal names the
    // SECOND occurrence exactly like TS does.
    let mut seen_ids: Vec<&str> = Vec::with_capacity(validated.len());
    let mut seen_markers: Vec<&str> = Vec::with_capacity(validated.len());
    let mut total_bytes: u64 = 0;

    for attachment in &validated {
        if seen_ids.contains(&attachment.id.as_str()) {
            return Err(attachment_error(format!(
                "attachments contain the duplicate id \"{}\".",
                attachment.id
            )));
        }

        if seen_markers.contains(&attachment.marker.as_str()) {
            return Err(attachment_error(format!(
                "attachments contain the duplicate marker \"{}\".",
                attachment.marker
            )));
        }

        seen_ids.push(&attachment.id);
        seen_markers.push(&attachment.marker);
        total_bytes += attachment.size;
    }

    // The per-turn bound is checked AFTER the per-entry rules, i.e. against the SUM of the sizes
    // that were each individually acceptable (`:553-557`).
    let total_limit = u64::try_from(MAX_TOTAL_ATTACHMENT_BYTES).unwrap_or(u64::MAX);

    if total_bytes > total_limit {
        return Err(attachment_error(format!(
            "attachments total {total_bytes} bytes, above the {MAX_TOTAL_ATTACHMENT_BYTES}-byte per-turn limit."
        )));
    }

    Ok(validated)
}

/// The byte-free projection that rides on a turn row (`attachmentDescriptorOf`,
/// `api-reads.ts:414-422`). `has_bytes` is literally "the recorded entry carries text in `data`",
/// which is the fact the download route's 200-vs-404 decision rests on.
#[must_use]
pub fn attachment_descriptor(attachment: &HarnessAttachment) -> AttachmentDescriptor {
    AttachmentDescriptor {
        id: attachment.id.clone(),
        kind: attachment.kind,
        marker: attachment.marker.clone(),
        name: attachment.name.clone(),
        mime: attachment.mime.clone(),
        size: attachment.size,
        has_bytes: !attachment.data.is_empty(),
    }
}

/// One composer chip on the compact display wire (`turn-projector.ts:2767-2784`, the
/// `DisplayAttachmentSummary` member of `harness-api.ts:1702`).
///
/// camelCase, because this is a DISPLAY member, not a REST resource (the REST projection above is
/// the snake_case one). `data` rides ONLY for an image whose base64 text fits the 96 KiB inline
/// window; every other attachment says `hasBytes: true` so the renderer knows a fetch can still
/// show it.
#[must_use]
pub fn inline_display_attachment(attachment: &HarnessAttachment) -> Value {
    // `attachment.kind === 'image' && typeof data === 'string' && data.length > 0 && data.length
    // <= DISPLAY_ATTACHMENT_INLINE_CHARS` (`:2769-2773`) - measured in base64 CHARACTERS.
    let inlinable = attachment.kind == AttachmentKind::Image
        && !attachment.data.is_empty()
        && attachment.data.chars().count() <= DISPLAY_ATTACHMENT_INLINE_CHARS;

    let mut summary = serde_json::Map::new();
    summary.insert("id".to_owned(), Value::String(attachment.id.clone()));
    summary.insert(
        "marker".to_owned(),
        Value::String(attachment.marker.clone()),
    );
    summary.insert("name".to_owned(), Value::String(attachment.name.clone()));
    summary.insert(
        "kind".to_owned(),
        serde_json::to_value(attachment.kind).unwrap_or(Value::Null),
    );
    summary.insert("mime".to_owned(), Value::String(attachment.mime.clone()));
    summary.insert("size".to_owned(), Value::Number(attachment.size.into()));

    if inlinable {
        summary.insert("data".to_owned(), Value::String(attachment.data.clone()));
    }

    // `hasBytes: data.length > 0` (`:2783`) - a byte-less record is the stale-shape case, and it
    // must not advertise a fetch that would 404.
    summary.insert(
        "hasBytes".to_owned(),
        Value::Bool(!attachment.data.is_empty()),
    );

    Value::Object(summary)
}

// ---------------------------------------------------------------------------
// Per-entry validation (`readAttachment`, `attachments.ts:384-508`)
// ---------------------------------------------------------------------------

/// One entry, checked completely, before anything is stored. Field order and message wording
/// follow the TS reader exactly, because a caller that reads the message must be able to match on
/// it the same way the browser composer's does.
fn read_attachment(
    attachment: &HarnessAttachment,
    index: usize,
) -> Result<HarnessAttachment, ApiError> {
    let label = format!("attachments[{index}]");

    // `ID_PATTERN` (`:411-413`).
    if !regex_match(ID_PATTERN, &attachment.id) {
        return Err(attachment_error(format!(
            "{label}.id must be a non-empty printable id of at most 100 characters."
        )));
    }

    // Marker: bound, grammar, safe-integer index, then label/kind agreement (`:420-443`). The
    // ORDER matters - an over-long marker is refused before any regex runs on it.
    if attachment.marker.chars().count() > MAX_MARKER_LENGTH {
        return Err(attachment_error(format!(
            "{label}.marker must be at most {MAX_MARKER_LENGTH} characters."
        )));
    }

    let marker_caps = regex_capture(MARKER_PATTERN, &attachment.marker, 2);

    let Some(marker_parts) = marker_caps else {
        return Err(attachment_error(format!(
            "{label}.marker must match the pattern \"[Image 1]\" (also Video, Audio, PDF, File)."
        )));
    };

    let (marker_label, marker_index) = (marker_parts[0].clone(), marker_parts[1].clone());

    // `Number.isSafeInteger(Number(markerMatch[2]))` (`:436-438`): a 17+ digit index is beyond a
    // safe integer, and checking it here means nothing downstream can see a lossy value.
    if marker_index.parse::<u64>().is_err() {
        return Err(attachment_error(format!(
            "{label}.marker index is too large."
        )));
    }

    let labels = MARKER_LABELS
        .into_iter()
        .find(|(kind, _)| *kind == attachment.kind)
        .map_or(&[][..], |(_, labels)| labels);

    if !labels.contains(&marker_label.as_str()) {
        return Err(attachment_error(format!(
            "{label}.marker \"{}\" does not match kind \"{}\".",
            attachment.marker,
            kind_name(attachment.kind)
        )));
    }

    // `NAME_PATTERN` (`:445-448`).
    if !matches_pattern(NAME_PATTERN, &attachment.name) {
        return Err(attachment_error(format!(
            "{label}.name must be a plain file name (no path separators or URL scheme)."
        )));
    }

    // MIME: bound first, then the grammar, then the kind table (`:450-466`).
    if attachment.mime.chars().count() > MAX_MIME_LENGTH {
        return Err(attachment_error(format!(
            "{label}.mime must be at most {MAX_MIME_LENGTH} characters."
        )));
    }

    if !matches_pattern(MIME_PATTERN, &attachment.mime) {
        return Err(attachment_error(format!(
            "{label}.mime must be a lowercase MIME type without parameters."
        )));
    }

    let allowed = allowed_mime_for(attachment.kind);

    if !allowed.is_empty() && !allowed.contains(&attachment.mime.as_str()) {
        return Err(attachment_error(format!(
            "{label}.mime \"{}\" is not valid for kind \"{}\".",
            attachment.mime,
            kind_name(attachment.kind)
        )));
    }

    // `size` is a positive safe integer (`:468-471`): `0` is refused, because an empty attachment
    // is a caller mistake rather than a representable empty file.
    if attachment.size < 1 {
        return Err(attachment_error(format!(
            "{label}.size must be a positive safe integer (decoded bytes)."
        )));
    }

    if attachment.data.is_empty() {
        return Err(attachment_error(format!(
            "{label}.data must be non-empty raw base64 text."
        )));
    }

    // Cheap CEILING before the canonical check, derived from the profile's DECODED-byte cap, so a
    // hostile multi-megabyte string cannot spend time matching beyond anything that could be
    // accepted (`:483-485`).
    if attachment.data.len() > MAX_ATTACHMENT_BYTES.div_ceil(3) * 4 + 4 {
        return Err(attachment_error(format!(
            "{label}.data exceeds the {MAX_ATTACHMENT_BYTES}-byte attachment limit."
        )));
    }

    if !is_canonical_base64(&attachment.data) {
        return Err(attachment_error(format!(
            "{label}.data must be canonical base64 (no data: prefix or whitespace)."
        )));
    }

    let decoded = decoded_bytes(&attachment.data);

    if decoded == 0 {
        return Err(attachment_error(format!(
            "{label}.data must decode to at least one byte."
        )));
    }

    if decoded > MAX_ATTACHMENT_BYTES {
        return Err(attachment_error(format!(
            "{label}.data is {decoded} bytes, above the {MAX_ATTACHMENT_BYTES}-byte attachment limit."
        )));
    }

    // The declared length must equal the DECODED length (`:500-502`): a client that ships a stale
    // `size` would otherwise reserve the wrong budget in every later model request.
    if usize::try_from(attachment.size).unwrap_or(usize::MAX) != decoded {
        return Err(attachment_error(format!(
            "{label}.size is {}, but its data decodes to {decoded} bytes.",
            attachment.size
        )));
    }

    // Signature check needs only the first 16 bytes, because the deepest offset any table entry
    // reads is `ftyp` at 4 plus 4 (`:504-505`).
    verify_signature(
        &attachment.mime,
        &decode_base64_prefix(&attachment.data, 16),
        &label,
    )
    .map_err(attachment_error)?;

    Ok(attachment.clone())
}

// ---------------------------------------------------------------------------
// Base64 (`attachments.ts:197-281`)
// ---------------------------------------------------------------------------

/// Canonical base64: whole 4-character groups plus at most two trailing `=`, with the DISCARDED
/// tail bits zero. Linear scan, not regex, exactly like TS (`:221-249`).
#[must_use]
pub fn is_canonical_base64(value: &str) -> bool {
    let bytes = value.as_bytes();

    if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
        return false;
    }

    // Trailing padding run.
    let mut end = bytes.len();
    let mut padding = 0usize;

    while end > 0 && bytes[end - 1] == b'=' {
        padding += 1;
        end -= 1;
    }

    if padding > 2 {
        return false;
    }

    // Every data character must be in the alphabet (`:234-237`); a non-ASCII byte is outside
    // canonical base64 by definition, which `as_bytes` already gives us.
    for byte in &bytes[..end] {
        if base64_value(*byte).is_none() {
            return false;
        }
    }

    // A trailing group must be one of the legal lengths for its padding (`:240`).
    if end % 4 != if padding == 0 { 0 } else { 4 - padding } {
        return false;
    }

    if padding == 0 {
        return true;
    }

    // Padding 2: the second data char carries one byte, so its low 4 bits must be zero. Padding 1:
    // the third data char carries the last two bits of byte 2 plus 2 discarded bits, so its low 2
    // bits must be zero (`:244-248`).
    let last_value = base64_value(bytes[end - 1]).unwrap_or(0);

    if padding == 2 {
        last_value & 0x0f == 0
    } else {
        last_value & 0x03 == 0
    }
}

/// The 6-bit value of one base64 character, or `None` when it is not in the alphabet
/// (`BASE64_VALUES`, `:203-212`).
#[must_use]
fn base64_value(byte: u8) -> Option<u32> {
    BASE64_ALPHABET
        .iter()
        .position(|candidate| *candidate == byte)
        .map(|position| position as u32)
}

/// Decoded byte length of canonical base64, without allocating the bytes (`:278-281`).
#[must_use]
fn decoded_bytes(value: &str) -> usize {
    let padding = usize::from(value.ends_with("==")) * 2
        + usize::from(!value.ends_with("==") && value.ends_with('='));

    (value.len() / 4) * 3 - padding
}

/// Decodes just enough leading bytes to check a media signature (`:253-275`): a full decode is
/// never needed at validation time.
#[must_use]
fn decode_base64_prefix(value: &str, max_bytes: usize) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;

    for character in value.chars() {
        // TS's loop reads `ALPHABET.indexOf(character)`, so anything outside the alphabet (the
        // padding `=` first of all, and every non-ASCII byte) ENDS the prefix rather than
        // contributing a partial group.
        let index = match character {
            '0'..='9' => u32::from(character as u8 - b'0') + 52,
            'A'..='Z' => u32::from(character as u8 - b'A'),
            'a'..='z' => u32::from(character as u8 - b'a') + 26,
            '+' => 62,
            '/' => 63,
            _ => break,
        };

        buffer = (buffer << 6) | index;
        bits += 6;

        if bits >= 8 {
            bits -= 8;

            if out.len() < max_bytes {
                out.push(((buffer >> bits) & 0xff) as u8);
            }

            if out.len() >= max_bytes {
                break;
            }
        }
    }

    out.truncate(max_bytes);
    out
}

// ---------------------------------------------------------------------------
// Magic signatures (`attachments.ts:290-373`)
// ---------------------------------------------------------------------------

/// Verifies a known media payload's magic bytes (`:309-373`). A MIME that claims png but carries a
/// jpeg body is a real mismatch (a renamed file, a mislabelled upload), so it is refused rather
/// than forwarded to a provider that would fail less clearly.
fn verify_signature(mime: &str, bytes: &[u8], label: &str) -> Result<(), String> {
    // The one failure sentence of this function (`:310-312`).
    let fail = || {
        format!(
            "{label}.data does not match its declared MIME \"{mime}\" (magic signature mismatch)."
        )
    };

    match mime {
        "image/png" => {
            if !starts_with(bytes, &[0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]) {
                return Err(fail());
            }
        }
        "image/jpeg" | "image/jpg" => {
            if !starts_with(bytes, &[0xff, 0xd8, 0xff]) {
                return Err(fail());
            }
        }
        "image/gif" => {
            if !starts_with(bytes, b"GIF87a") && !starts_with(bytes, b"GIF89a") {
                return Err(fail());
            }
        }
        "image/webp" => {
            // RIFF container with the WEBP fourcc at offset 8.
            if !starts_with(bytes, b"RIFF") || !matches_at(bytes, 8, b"WEBP") {
                return Err(fail());
            }
        }
        "application/pdf" => {
            // The header may carry a little leading junk in the wild, but a clean upload starts
            // with `%PDF-`; requiring it keeps a text blob from posing as a document.
            if !starts_with(bytes, b"%PDF-") {
                return Err(fail());
            }
        }
        "audio/wav" | "audio/x-wav" | "audio/wave" => {
            if !starts_with(bytes, b"RIFF") || !matches_at(bytes, 8, b"WAVE") {
                return Err(fail());
            }
        }
        "audio/mpeg" | "audio/mp3" => {
            // MP3 is either an ID3-tagged file or a bare frame sync (0xFF Ex).
            let bare_sync = bytes.first().is_some_and(|byte| *byte == 0xff)
                && bytes.get(1).is_some_and(|byte| byte & 0xe0 == 0xe0);

            if !starts_with(bytes, b"ID3") && !bare_sync {
                return Err(fail());
            }
        }
        "video/mp4" | "video/quicktime" => {
            // ISO base media: a `ftyp` box begins at offset 4.
            if !matches_at(bytes, 4, b"ftyp") {
                return Err(fail());
            }
        }
        "video/webm" => {
            if !starts_with(bytes, &[0x1a, 0x45, 0xdf, 0xa3]) {
                return Err(fail());
            }
        }
        "video/ogg" if !starts_with(bytes, b"OggS") => {
            return Err(fail());
        }
        // `file` has no signature contract; the transport refuses it anyway (`:369-371`).
        _ => {}
    }

    Ok(())
}

/// `startsWith` (`:286-287`): every signature byte must be present at the same index.
#[must_use]
fn starts_with(bytes: &[u8], signature: &[u8]) -> bool {
    signature
        .iter()
        .enumerate()
        .all(|(index, byte)| bytes.get(index) == Some(byte))
}

/// `matchesAt` (`:303-304`): every signature byte must be present from `offset`.
#[must_use]
fn matches_at(bytes: &[u8], offset: usize, signature: &[u8]) -> bool {
    signature
        .iter()
        .enumerate()
        .all(|(index, byte)| bytes.get(offset + index) == Some(byte))
}

// ---------------------------------------------------------------------------
// Status mapping and helpers
// ---------------------------------------------------------------------------

/// `attachmentErrorStatus` (`sessions.ts:1357-1361`): 413 for a byte bound, 415 for a media type
/// or signature problem, 400 for anything else. The match is intentionally narrow, and unknown
/// validator wording falls back to 400, which is still a clear client error.
#[must_use]
pub fn attachment_error_status(message: &str) -> u16 {
    if regex_match(TOO_LARGE_PATTERN, message) {
        return 413;
    }

    if regex_match(MEDIA_TYPE_PATTERN, message) {
        return 415;
    }

    400
}

/// `validateAttachmentsForSubmit` (`sessions.ts:1368-1375`): the validator's message, wrapped with
/// the status the message text decides.
#[must_use]
pub fn attachment_error(message: impl Into<String>) -> ApiError {
    let message = message.into();
    let status = attachment_error_status(&message);

    ApiError::new(status, message)
}

/// The `kind` spelling TS interpolates into a refusal (`attachments.ts:417`, `:442`); the domain
/// union is lowercase, which is also the wire spelling.
#[must_use]
fn kind_name(kind: AttachmentKind) -> &'static str {
    match kind {
        AttachmentKind::Image => "image",
        AttachmentKind::Video => "video",
        AttachmentKind::Audio => "audio",
        AttachmentKind::Pdf => "pdf",
        AttachmentKind::File => "file",
    }
}

/// One compiled-regex cache per pattern, so a validation loop never recompiles. `OnceLock` keeps
/// this allocation-free after the first call and free of any global mutable state.
fn matches_pattern(pattern: &str, value: &str) -> bool {
    regex_match(pattern, value)
}

/// Captures up to `count` groups of `pattern` against `value`, trimmed of nothing (TS reads
/// `markerMatch[1]` / `[2]` verbatim).
fn regex_capture(pattern: &str, value: &str, count: usize) -> Option<Vec<String>> {
    let expression = compiled(pattern)?;
    let captures = expression.captures(value)?;

    let found: Vec<String> = (1..=count)
        .filter_map(|index| {
            captures
                .get(index)
                .map(|matched| matched.as_str().to_owned())
        })
        .collect();

    (found.len() == count).then_some(found)
}

/// `Regex::new` once per pattern; a compile failure is impossible for the literals above, and the
/// answer is `false`/`None` rather than a panic if the crate ever changes.
fn compiled(pattern: &str) -> Option<&'static Regex> {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static CACHE: OnceLock<Mutex<HashMap<String, &'static Regex>>> = OnceLock::new();

    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(expression) = guard.get(pattern) {
        return Some(*expression);
    }

    let expression: &'static Regex = Box::leak(Box::new(Regex::new(pattern).ok()?));
    guard.insert(pattern.to_owned(), expression);

    Some(expression)
}

/// `pattern.test(value)` (`sessions.ts:1357-1360`): a compile failure reads as "no match", which
/// falls through to the 400 the TS comment already calls the safe default.
fn regex_match(pattern: &str, value: &str) -> bool {
    compiled(pattern).is_some_and(|expression| expression.is_match(value))
}

/// The base64 crate is a dependency of this crate for cursors; the attachment reader above is the
/// authority for what canonical base64 IS, so the only use here is the decoder a caller may want
/// for the recorded bytes (kept re-exported for the tool stage).
#[must_use]
pub fn decode_canonical_base64(value: &str) -> Option<Vec<u8>> {
    if !is_canonical_base64(value) {
        return None;
    }

    base64::engine::general_purpose::STANDARD.decode(value).ok()
}
