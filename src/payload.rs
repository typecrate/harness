//! The ONE byte measure every model-facing budget has to use (`core/payload-bytes.ts`).
//!
//! TypeScript origin: `packages/underload/harness/src/engine/core/payload-bytes.ts`
//! (`utf8ByteLength` at `:49-53`, `modelPayloadBytes` at `:72-132`), plus the per-entry
//! charge the file tools derive from it (`tools/files/grep.tool.ts:235` `entryWireBytes`,
//! `tools/files/glob.tool.ts:161`, `tools/files/files.tool.ts:454`, and
//! `tools/files/read.tool.ts:1638` `contentWireBytes`). Ported 2026-11-28 to close the
//! currency divergence the crate recorded under `MODEL_TOOL_RESULT_BUDGET_BYTES` in
//! `src/contract.rs`, where the note now states what is CLOSED and what is left.
//!
//! WHY THIS EXISTS
//! ---------------
//! A budget is only a budget if it counts the thing the provider charges for. Two
//! different counts exist for the same tool result and the reference enforces the dearer
//! one:
//!
//!   * the TEXT's own UTF-8 bytes ([`crate::payload::utf8_byte_length`]);
//!   * the bytes that text occupies INSIDE the request body, i.e. after JSON escaping
//!     ([`crate::payload::model_payload_bytes`]).
//!
//! A tool result becomes the `content` of a transcript message, and `content` is
//! serialized as a JSON string, so the second number is the one that is billed. They
//! differ by exactly the escapes JSON adds: `"` and `\` double, every other control
//! character becomes `\u00XX` (6 bytes), and a lone surrogate becomes `\udXXX` (6 bytes).
//! Measured on the run the reference module was added for (`payload-bytes.ts:16-20`): a
//! 245,616-byte `read` transcript message reached the wire at 257,074 bytes (+4.6%), and a
//! quote/backslash-heavy file cost +4.8%. A cap measured on the FIRST number therefore
//! overshot the wire by exactly that much.
//!
//! WHAT IS DELIBERATELY NOT COUNTED
//! --------------------------------
//! The two DELIMITING QUOTES of the JSON string literal. They are framing the enclosing
//! key pays, not payload, and every budget in this crate is a payload figure. That is also
//! why this function IS the reference's `contentWireBytes` (`read.tool.ts:1638`): TS builds
//! that figure as `modelPayloadBytes(JSON.stringify(text)) - 2`, i.e. the escaped width with
//! the wrapping quotes removed, which is what this walk produces directly.
//!
//! WHERE THIS CRATE AND THE REFERENCE CANNOT AGREE (stated, not hidden)
//! -------------------------------------------------------------------
//! 1. **Lone surrogates are unrepresentable here.** A Rust `String` is valid UTF-8, so a
//!    surrogate half arriving in a JSON string (TS writes `"\ud800"`) is decoded by
//!    `serde_json` into U+FFFD (3 bytes) before this crate ever sees it, and it is priced at
//!    3 rather than at the reference's 6. No text this build produces can hold a lone
//!    surrogate, so the pricing never diverges on the crate's own output; it diverges only
//!    against a TS-written document that carried one. [`crate::payload::model_payload_bytes_units`]
//!    still
//!    ports the surrogate arms verbatim, because the reference's contract is written in
//!    UTF-16 code units and that twin is where those arms live (and what its vectors are
//!    pinned against).
//! 2. **The crate measures its own serializer.** [`crate::payload::document_wire_bytes`] /
//!    [`crate::payload::entry_wire_bytes`] price a value's `serde_json` form, which is what actually goes on
//!    this server's wire. `serde_json` and `JSON.stringify` agree on every character class
//!    this surface carries (see the oracle test), so the two figures stay equal in practice;
//!    where a future serializer escapes a class JSON leaves literal, THIS measure is the
//!    honest one for this build because it follows the bytes being written.

use serde_json::Value;

/// UTF-8 byte length (`payload-bytes.ts:49` `utf8ByteLength`): the unit the numbers an
/// envelope QUOTES are written in ("cut 64071 of 300000 bytes" is a statement about the
/// tool's text, not about a JSON encoding the reader never sees). It is never the unit a
/// budget is gated in - that is [`model_payload_bytes`].
///
/// `str::len` is exact: Rust strings are stored as UTF-8.
#[must_use]
pub fn utf8_byte_length(value: &str) -> usize {
    value.len()
}

/// The bytes `value` occupies as a JSON string value: its UTF-8 bytes, with every character
/// JSON escaping counted at the ESCAPED width (`payload-bytes.ts:72` `modelPayloadBytes`).
///
/// The table is the reference's, one character at a time:
///   * `"` and `\`      -> 2 (`\"` `\\`)
///   * `\b \f \n \r \t` -> 2 (`\b \f \n \r \t`)
///   * other `< 0x20`   -> 6 (`\u00XX`)
///   * everything else  -> its UTF-8 length. U+2028/U+2029 included: `JSON.stringify`
///     leaves them literal, so charging the raw length is exact (the reference's own note
///     at `payload-bytes.ts:70-71`), and so are the C1 controls `0x80..=0x9f` and DEL.
///
/// Walking `chars()` rather than UTF-16 code units is not a narrowing: a Rust `char` is
/// never a surrogate half, and an astral character's `len_utf8()` is the 4 the reference
/// charges a well-formed pair. See the module header for the one input that cannot be
/// represented here at all.
#[must_use]
pub fn model_payload_bytes(value: &str) -> usize {
    // Escaping is PER CHARACTER in both JSON and this table, which is what makes the sum exact
    // and what lets a tool that streams one character at a time charge the same figure
    // ([`char_payload_bytes`], the reference's `escapedCharBytes`).
    value.chars().map(char_payload_bytes).sum()
}

/// The escaped width of ONE character: the table [`model_payload_bytes` applies], written the
/// way the reference's per-character twin writes it (`read.tool.ts:1605` `escapedCharBytes`).
/// A tool that appends to a bounded buffer one character at a time charges with this and stays
/// in the same currency as every cap above it.
#[must_use]
pub fn char_payload_bytes(character: char) -> usize {
    match character {
        // The two characters that need a backslash in every JSON string, plus the five
        // control characters that have a one-character escape (`\b` 0x08, `\t` 0x09,
        // `\n` 0x0a, `\f` 0x0c, `\r` 0x0d).
        '"' | '\\' | '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 2,
        // Every other control character becomes a six-byte `\u00XX` literal.
        other if u32::from(other) < 0x20 => 6,
        // Everything else is written literally, so it costs exactly its UTF-8 width.
        other => other.len_utf8(),
    }
}

/// The reference's walk, over UTF-16 CODE UNITS (`payload-bytes.ts:72-132` verbatim,
/// including the surrogate arms the `&str` walk cannot reach).
///
/// WHY THIS IS KEPT. The reference counts code units precisely so a LONE surrogate is
/// priced at the six-byte `\udXXX` literal ES2019 `JSON.stringify` writes for it; anything
/// less under-counts a payload about to be truncated. A Rust `&str` cannot hold one (see
/// the module header), so this twin is the executable statement of that half of the
/// contract, and the place the reference's surrogate vectors are pinned. It is not on the
/// turn path.
#[must_use]
pub fn model_payload_bytes_units(units: &[u16]) -> usize {
    let mut bytes = 0_usize;
    let mut index = 0_usize;

    while index < units.len() {
        let code = units[index];

        // The two characters that need a backslash in every JSON string (`"` 0x22, `\` 0x5c) and the
        // five control characters that own a one-character escape (`\b` `\t` `\n` `\f` `\r`) all
        // cost 2 - the reference tests them as two branches and both add 2, so they are ONE branch
        // here (clippy's `if_same_then_else` would otherwise reject the transcription). Every other
        // control becomes a six-byte `\u00XX`.
        if code == 0x22 || code == 0x5c || matches!(code, 0x08 | 0x09 | 0x0a | 0x0c | 0x0d) {
            bytes += 2;
        } else if code < 0x20 {
            bytes += 6;
        } else if code < 0x80 {
            bytes += 1;
        } else if code < 0x800 {
            bytes += 2;
        } else if (0xd800..=0xdbff).contains(&code) {
            // A high surrogate: with a low one right behind it, the PAIR encodes to exactly
            // 4 UTF-8 bytes and JSON leaves the pair literal; on its own it is a `\udXXX`
            // escape, charged per code unit.
            let next = usize::from(units.get(index + 1).copied().unwrap_or(0));

            if (0xdc00..=0xdfff).contains(&next) {
                bytes += 4;
                index += 1;
            } else {
                bytes += 6;
            }
        } else if (0xdc00..=0xdfff).contains(&code) {
            // A lone LOW surrogate: also `\udXXX`.
            bytes += 6;
        } else {
            bytes += 3;
        }

        index += 1;
    }

    bytes
}

/// The widest value any of the reference's counters can hold, and therefore the width it
/// PRICES every numeric field at when it measures a result's own scaffolding
/// (`grep.tool.ts:262` `MAX_FIELD_VALUE = Number.MAX_SAFE_INTEGER`). Used the same way
/// here: an upper bound, so an allowance over-reserves a few hundred bytes rather than
/// under-reserving one.
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// The request-body cost of a whole JSON document: `modelPayloadBytes(JSON.stringify(x))`
/// (`grep.tool.ts:303` `grepResultOverhead`, `glob.tool.ts:224` `globPageAllowance`'s
/// inner call, `files.tool.ts:1061` `filesResultOverhead`).
///
/// This is what a tool has to measure BEFORE it can promise that a full page survives the
/// harness cap: the document's keys, its counters and its framing are payload too, and a
/// budget spent only on the entries leaves the scaffolding unbudgeted.
#[must_use]
pub fn document_wire_bytes(value: &Value) -> usize {
    // `Value::to_string` uses `JSON.stringify`'s default (compact) separators, which is what
    // makes the entry charges below ADD UP to this figure instead of drifting from it.
    model_payload_bytes(&value.to_string())
}

/// The cost of ONE array entry as the serialized document sees it, plus the comma that
/// separates it (`entryWireBytes`, `grep.tool.ts:235` / `glob.tool.ts:161` /
/// `files.tool.ts:454`, all three written as
/// `modelPayloadBytes(JSON.stringify(value)) + 1`).
///
/// WHY THE `+ 1`, AND WHY THE OVER-PRICE IS DELIBERATE: measuring an entry's OWN serialized
/// form charges the quotes around every string 2 bytes each (`\"`) that the enclosing array
/// pays only once per boundary, so an entry is priced slightly HIGH. An over-price can only
/// end a page one entry earlier - `truncated: true` with a cursor - while an UNDER-price is
/// the failure this charge replaced: `grep.tool.ts:199-204` records a page charging 65,380
/// of its own 65,536 reaching the wire at 105,746 payload bytes, i.e. JSON-REFUSED by a cap
/// the tool believed it had filled exactly.
#[must_use]
pub fn entry_wire_bytes(value: &Value) -> usize {
    document_wire_bytes(value) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The oracle the reference pins its measure against (`payload-bytes.test.ts:36`
    /// `wireBytes`): the byte length of the string as JSON puts it on the wire, minus its
    /// two wrapping quotes. Here that is `serde_json`'s own rendering, which is what this
    /// server actually writes.
    fn wire_bytes(value: &str) -> usize {
        serde_json::to_string(value)
            .expect("a string always serializes")
            .len()
            .saturating_sub(2)
    }

    #[test]
    fn utf8_length_counts_bytes_never_code_units_or_code_points() {
        // Exact per class: 1 byte ASCII, 2 bytes Latin-1 supplement, 3 bytes CJK, 4 bytes
        // for an astral character, and control characters cost 1 (they are only inflated by
        // JSON escaping, which is the other measure's job).
        assert_eq!(utf8_byte_length(""), 0);
        assert_eq!(utf8_byte_length("abc"), 3);
        assert_eq!(utf8_byte_length("é"), 2);
        assert_eq!(utf8_byte_length("世"), 3);
        assert_eq!(utf8_byte_length("😀"), 4);
        assert_eq!(utf8_byte_length("\n"), 1);
        assert_eq!(utf8_byte_length("\""), 1);
        assert_eq!(utf8_byte_length("\\"), 1);

        // The realistic mixed window `payload-bytes.test.ts:60` pins.
        assert_eq!(
            utf8_byte_length("path/to/file.ts\nconst x = \"a\\nb\";\n"),
            34
        );
    }

    #[test]
    fn payload_bytes_equals_the_serialized_width_for_every_escape_class() {
        // The reference's corpus, minus the entries a Rust `String` cannot hold
        // (`'\ud800'`, `'a\ud800b\udc00c'`), which are pinned on the code-unit twin below.
        let corpus = [
            "",
            "hello world 123",
            "!@#$%^&*()[]{}<>/?:;,.=-+_~`|",
            "say \"hi\" and 'bye'",
            "C:\\tmp\\n",
            "a\nb\r\nc\td",
            "a\u{7}b\u{8}c\u{c}d",
            "\u{0}\u{1}\u{1f} x",
            "a\u{7f}b",
            "\u{80}\u{9f}",
            "é",
            "世界",
            "مرحبا שלום",
            "😀",
            "👨\u{200d}👩\u{200d}👧",
            "a\u{2028}b\u{2029}c",
            "a\u{FEFF}b\u{A0}c",
            "path/to/file.ts\nconst x = \"a\\nb\";\n",
        ];

        for value in corpus {
            assert_eq!(
                model_payload_bytes(value),
                wire_bytes(value),
                "the walk and the serializer disagree about {value:?}"
            );
        }
    }

    #[test]
    fn payload_bytes_charges_each_escape_at_its_own_width() {
        // Exact numbers per rule, so a change to one rule cannot hide behind a matching
        // oracle over a corpus that happens not to cover it (`payload-bytes.test.ts:64-77`).
        assert_eq!(model_payload_bytes(""), 0);
        assert_eq!(model_payload_bytes("hello"), 5);
        assert_eq!(model_payload_bytes("say \"hi\" and 'bye'"), 20); // two `"` -> `\"`
        assert_eq!(model_payload_bytes("C:\\tmp\\n"), 10); // two `\` -> `\\`
        assert_eq!(model_payload_bytes("a\nb\r\nc\td"), 12); // \n \r \t are 2 bytes each
        assert_eq!(model_payload_bytes("a\u{7}b\u{8}c\u{c}d"), 14); // \u0007, \b, \f
        assert_eq!(model_payload_bytes("\u{0}\u{1}\u{1f} x"), 20); // \u00XX = 6 bytes
    }

    #[test]
    fn one_character_at_a_time_prices_the_same_table_as_the_whole_string() {
        // `char_payload_bytes` is the reference's `escapedCharBytes` (`read.tool.ts:1605`): the
        // primitive a bounded buffer charges with as it appends, which is how `wire_cut_prefix`
        // (`src/tools/files.rs`) decides where a `read` window stops. Every arm is pinned at its
        // exact width, then against the serializer, then as the sum of a whole string - because the
        // property the page cut depends on is that a per-character charge and the whole-string
        // measure run out at the SAME character.
        let cases: [(char, usize); 16] = [
            ('"', 2),
            ('\\', 2),
            ('\u{8}', 2),
            ('\t', 2),
            ('\n', 2),
            ('\u{c}', 2),
            ('\r', 2),
            ('\u{0}', 6),
            ('\u{1}', 6),
            ('\u{b}', 6),
            ('\u{1f}', 6),
            (' ', 1),
            ('\u{7f}', 1),
            ('\u{a0}', 2),
            ('\u{2028}', 3),
            ('\u{1f600}', 4),
        ];

        for (character, expected) in cases {
            assert_eq!(char_payload_bytes(character), expected, "{character:?}");
            // The serializer stays the oracle even at one character, so an arm that drifts away
            // from what `serde_json` writes is caught here rather than in a page budget.
            assert_eq!(
                wire_bytes(&character.to_string()),
                expected,
                "{character:?} is priced away from how it is written"
            );
        }

        let joined: String = cases.iter().map(|(character, _)| *character).collect();

        assert_eq!(
            model_payload_bytes(&joined),
            cases.iter().map(|(_, width)| *width).sum::<usize>(),
            "the sum of the parts is the price of the whole"
        );
        assert_eq!(model_payload_bytes(&joined), wire_bytes(&joined));
    }

    #[test]
    fn payload_bytes_never_inflates_non_ascii_which_json_leaves_literal() {
        // The over-counting trap: `é` is NOT serialized as `\u00e9`. If this regresses to a
        // per-character upper bound, every multibyte tool result is charged 3x what it costs
        // and the model loses real window (`payload-bytes.test.ts:79-92`).
        assert_eq!(model_payload_bytes("é"), 2);
        assert_eq!(model_payload_bytes("世界"), 6);
        assert_eq!(model_payload_bytes("مرحبا שלום"), 19);
        assert_eq!(model_payload_bytes("😀"), 4);
        assert_eq!(model_payload_bytes("👨\u{200d}👩\u{200d}👧"), 18);
        assert_eq!(model_payload_bytes("a\u{2028}b\u{2029}c"), 9);
        assert_eq!(model_payload_bytes("a\u{FEFF}b\u{A0}c"), 8);
        assert_eq!(model_payload_bytes("\u{80}\u{9f}"), 4);
        assert_eq!(model_payload_bytes("a\u{7f}b"), 3);
    }

    #[test]
    fn payload_bytes_charges_a_lone_surrogate_as_the_escape_it_is_written_as() {
        // The reference's vectors, on the code-unit twin (`payload-bytes.ts:55-61`,
        // `payload-bytes.test.ts:94-103`). A lone surrogate has no UTF-8 encoding, so JSON
        // writes it as `\ud800`: 6 wire bytes for the 3 `utf8ByteLength` counts.
        // Written as code units, because a Rust `&str` literal cannot hold a surrogate half
        // at all - which is exactly the divergence the module header records.
        let lone_high: Vec<u16> = vec![0x61, 0xd800, 0x62]; // "a\ud800b"
        let lone_low: Vec<u16> = vec![0x61, 0xdc00, 0x62]; // "a\udc00b"
        let pair: Vec<u16> = "😀".encode_utf16().collect();
        let mixed: Vec<u16> = vec![
            0x61, 0xd800, 0x62, 0xdc00, 0x63, // "a\ud800b\udc00c"
        ];

        assert_eq!(model_payload_bytes_units(&lone_high), 1 + 6 + 1);
        assert_eq!(model_payload_bytes_units(&lone_low), 1 + 6 + 1);
        // A well-formed pair is FOUR bytes, not 3 + 6: the reason the walk resolves one pair
        // at a time.
        assert_eq!(pair.len(), 2);
        assert_eq!(model_payload_bytes_units(&pair), 4);
        assert_eq!(model_payload_bytes_units(&mixed), 15);
    }

    #[test]
    fn payload_bytes_is_never_below_the_raw_length_and_above_it_exactly_when_escaping_fires() {
        // `payload-bytes.test.ts:105-115`.
        let plain = "plain ascii only";
        let escaped = "a \"quoted\" line\nand a backslash \\ here";

        assert_eq!(model_payload_bytes(plain), utf8_byte_length(plain));
        assert_eq!(model_payload_bytes(escaped), 42);
        assert_eq!(utf8_byte_length(escaped), 38);
        assert_eq!(model_payload_bytes(&"é".repeat(200)), 400);
        assert_eq!(utf8_byte_length(&"é".repeat(200)), 400);
    }

    #[test]
    fn payload_bytes_is_exact_on_the_mixed_content_a_real_tool_result_carries() {
        // The reference's realistic window (`payload-bytes.test.ts:117-128`): 5000 ascii +
        // 500 quotes + 500 newlines + one of each multibyte class and a NUL. 6,013 raw bytes
        // reach the provider as 7,018 - a 1.167x factor on ORDINARY content, which is the
        // drift a budget measured on the first number never sees. The reference's figure is
        // 6,016 / 7,024 because the same corpus also carries a lone surrogate (3 raw / 6
        // escaped), which a Rust `String` cannot hold; that leg is pinned on the code-unit
        // twin above.
        let text = format!(
            "{}{}{}é世界😀\u{0}",
            "x".repeat(5_000),
            "\"".repeat(500),
            "\n".repeat(500)
        );

        assert_eq!(utf8_byte_length(&text), 6_013);
        assert_eq!(model_payload_bytes(&text), 7_018);
        assert_eq!(model_payload_bytes(&text), wire_bytes(&text));

        // The same identity at the scale the post-mortem measured (`payload-bytes.ts:16-20`):
        // a 245,616-byte `read` transcript message reaching the wire at 257,074 (+4.6%). That
        // is 11,458 of the 245,616 characters escaping at one extra byte each, so a document
        // of that shape reproduces BOTH cited figures exactly - and the quote/backslash-heavy
        // file's +4.8% is the same arithmetic on a smaller document.
        let heavy = format!("{}\n{}", "q".repeat(234_158), "\n".repeat(11_457));

        assert_eq!(utf8_byte_length(&heavy), 245_616);
        assert_eq!(model_payload_bytes(&heavy), 257_074);

        let quotes = format!("{}{}", "a".repeat(9_520), "\"".repeat(480));

        assert_eq!(utf8_byte_length(&quotes), 10_000);
        assert_eq!(model_payload_bytes(&quotes), 10_480); // +4.8%
    }

    #[test]
    fn an_entry_charge_is_its_document_width_plus_the_separator() {
        // `entryWireBytes` (`grep.tool.ts:235`): one comma and the entry's own serialized
        // form, quoted strings at `\"` - the deliberate one-byte-per-entry over-price.
        assert_eq!(entry_wire_bytes(&serde_json::json!("a/b.ts")), 11); // `\"a/b.ts\"` + comma
        assert_eq!(entry_wire_bytes(&serde_json::json!({"path": "a"})), 17);
        assert_eq!(document_wire_bytes(&serde_json::json!({"path": "a"})), 16);

        // The additive property the page allowances depend on: a sum of entry charges IS the
        // document's cost at compact separators, up to the array's own brackets.
        let entries = [
            serde_json::json!("a/b.ts"),
            serde_json::json!("c/d.ts"),
            serde_json::json!("e/f.ts"),
        ];
        let array = serde_json::Value::Array(entries.to_vec());

        assert_eq!(
            entries.iter().map(entry_wire_bytes).sum::<usize>(),
            document_wire_bytes(&array) - 2 + 1,
            "the entries plus their commas are the array minus its brackets"
        );
    }
}
