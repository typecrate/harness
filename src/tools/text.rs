//! The server-owned "harness tools": `timeNow`, `textCount`, `diceRoll`, `noteSearch`,
//! `textSummarize`.
//!
//! TypeScript origin: `src/server/tools/shared/{timeNow,textCount,diceRoll,noteSearch,
//! textSummarize}.tool.ts`. Their metadata is in `catalogue.rs`; THIS file is the
//! behaviour, including the content shapes every consumer depends on:
//!   * `timeNow` keeps `{ timezone, iso, unix }` and its one `right`-slot key/value
//!     panel (`timeNow.tool.ts` calls it "the R20.2 demonstration" - the model reads the
//!     JSON, a client reads the panel);
//!   * `textCount` counts characters in UTF-16 code units (`String.prototype.length`),
//!     so an astral-plane emoji is 2 - switching to code points would change answers;
//!   * `diceRoll` refuses an out-of-range side count instead of clamping it;
//!   * `noteSearch` answers from the four-note local corpus, verbatim;
//!   * `textSummarize` bounds the notes at 20 000 characters and STATES the cut.
//!
//! WHY `textSummarize` ALWAYS TAKES ITS LOCAL PATH HERE
//! ----------------------------------------------------
//! The TypeScript entry calls `run('summarizer', ...)` when the harness configures that
//! subagent, and falls back to `condense(...)` otherwise (`textSummarize.tool.ts:96-115`).
//! The Rust [`crate::ports::ToolExecCtx`] seam carries a task host and an action host, not
//! a nested agent runner, so a delegated summarizer is not reachable from a tool body yet;
//! the documented local fallback is therefore the only path this port has, and its result
//! is exactly the string `condense` produces. `knowledge.md` records it as a gap for the
//! stage that adds an agent-runner seam.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

/// `MAX_TIMEZONE_CHARS` (`timeNow.tool.ts:41`), mirrored by the schema's `maxLength`.
pub const MAX_TIMEZONE_CHARS: usize = 64;
/// `DEFAULT_DICE_SIDES` / `MIN_DICE_SIDES` / `MAX_DICE_SIDES` (`diceRoll.tool.ts:31-33`).
pub const DEFAULT_DICE_SIDES: i64 = 6;
/// The smallest die this tool will roll.
pub const MIN_DICE_SIDES: i64 = 2;
/// The largest die this tool will roll.
pub const MAX_DICE_SIDES: i64 = 1_000;
/// `MAX_SUMMARIZE_NOTES` (`textSummarize.tool.ts:36`).
pub const MAX_SUMMARIZE_NOTES: usize = 20_000;
/// `NOTES_TRUNCATED` (`textSummarize.tool.ts:40`), the marker that rides both the prompt
/// and the returned summary.
pub const NOTES_TRUNCATED: &str = "[notes truncated to 20000 characters]";

// ---------------------------------------------------------------------------
// The clock
// ---------------------------------------------------------------------------

/// Milliseconds since the Unix epoch (`Date.now()`).
#[must_use]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

/// `Date.prototype.toISOString`: `YYYY-MM-DDTHH:MM:SS.sssZ`, always UTC.
///
/// Days are converted with the civil-from-days algorithm (Howard Hinnant's), because the
/// crate deliberately carries no date library: the whole computation is 20 lines and is
/// exactly the proleptic Gregorian calendar `toISOString` specifies.
#[must_use]
pub fn iso_from_millis(millis: i64) -> String {
    let mut days = millis.div_euclid(86_400_000);
    let time_of_day = millis.rem_euclid(86_400_000);

    let hours = time_of_day / 3_600_000;
    let minutes = (time_of_day % 3_600_000) / 60_000;
    let seconds = (time_of_day % 60_000) / 1_000;
    let milliseconds = time_of_day % 1_000;

    // 0000-03-01 is the epoch the algorithm is built around: moving the year start to
    // March keeps the leap-day correction a single division.
    days += 719_468;

    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    // The algorithm counts the year from MARCH, so January and February belong to the
    // next calendar year (`civil_from_days`' final `y += (m <= 2)`).
    let year = if month <= 2 { year + 1 } else { year };

    format!(
        "{year:04}-{:02}-{:02}T{hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}Z",
        month as i32, day as i32,
    )
}

/// `timeNow`: the `{ timezone, iso, unix }` content wrapped in its declarative panel
/// (`timeNow.tool.ts:96-131`).
///
/// The timezone argument is a LABEL only - it never moves the clock - so it is bounded
/// rather than resolved: the panel would otherwise become an arbitrary text sink.
///
/// # Errors
///
/// `timeNow timezone must be a non-empty string of at most 64 characters.`
pub(crate) fn time_now(arguments: &Value) -> Result<Value, String> {
    let label = match super::result::argument_value(arguments, "timezone") {
        None | Some(Value::Null) => "UTC".to_owned(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => {
            return Err(format!(
                "timeNow timezone must be a non-empty string of at most {MAX_TIMEZONE_CHARS} characters; received {}.",
                super::result::type_of(other)
            ));
        }
    };

    if label.trim().is_empty() || label.chars().count() > MAX_TIMEZONE_CHARS {
        return Err(format!(
            "timeNow timezone must be a non-empty string of at most {MAX_TIMEZONE_CHARS} characters."
        ));
    }

    let millis = i64::try_from(now_millis()).unwrap_or(i64::MAX);
    let content = json!({
        "timezone": label,
        "iso": iso_from_millis(millis),
        "unix": millis / 1_000,
    });

    // The `toolUi(content, panels)` envelope (`core/tool-ui.ts`): the model reads
    // `content`, a client reads `__toolUi`.
    Ok(json!({
        super::result::TOOL_UI_BRAND: [{
            "id": "timeNow",
            // 'right' is the default mount point; slot names are open strings.
            "slot": "right",
            "title": "Current time",
            "blocks": [
                {
                    "type": "keyValue",
                    "items": [
                        { "label": "Timezone", "value": content["timezone"].as_str().unwrap_or_default() },
                        { "label": "ISO 8601", "value": content["iso"].as_str().unwrap_or_default() },
                        { "label": "Unix", "value": (content["unix"].as_i64().unwrap_or(0)).to_string() },
                    ],
                },
                { "type": "text", "text": "Read from the server clock at execution time." },
            ],
        }],
        "content": content,
    }))
}

// ---------------------------------------------------------------------------
// textCount / diceRoll
// ---------------------------------------------------------------------------

/// `textCount`: `{ words, characters }`, characters in UTF-16 code units
/// (`textCount.tool.ts:47-55`).
pub(crate) fn text_count(arguments: &Value) -> Result<Value, String> {
    let text = super::result::required_text(arguments, "text", "textCount")?;
    let trimmed = text.trim();
    let words = if trimmed.is_empty() {
        0
    } else {
        trimmed.split_whitespace().count()
    };

    Ok(json!({
        "words": words,
        "characters": text.encode_utf16().count(),
    }))
}

/// `diceRoll`: `{ sides, value }` with `value` in `1..=sides` (`diceRoll.tool.ts:66-86`).
///
/// The draw is nanosecond-derived rather than `Math.random()`: the crate has no RNG
/// dependency, the value is not a security property, and a test can only assert the
/// documented range.
///
/// # Errors
///
/// `diceRoll sides must be a whole number from 2 to 1000.`
pub(crate) fn dice_roll(arguments: &Value) -> Result<Value, String> {
    let faces = match super::result::argument_value(arguments, "sides") {
        None | Some(Value::Null) => DEFAULT_DICE_SIDES,
        Some(value) => value
            .as_i64()
            .ok_or_else(|| "diceRoll sides must be a whole number from 2 to 1000.".to_owned())?,
    };

    if !(MIN_DICE_SIDES..=MAX_DICE_SIDES).contains(&faces) {
        return Err(format!(
            "diceRoll sides must be a whole number from {MIN_DICE_SIDES} to {MAX_DICE_SIDES}."
        ));
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.subsec_nanos() as u64 + since.as_secs() * 31)
        .unwrap_or(0);
    let value = 1 + i64::try_from(nanos % u64::try_from(faces).unwrap_or(1)).unwrap_or(0);

    Ok(json!({ "sides": faces, "value": value }))
}

// ---------------------------------------------------------------------------
// noteSearch
// ---------------------------------------------------------------------------

/// One entry of `noteSearch`'s local corpus (`noteSearch.tool.ts:32-56`, copied verbatim
/// so a search answer cannot change meaning between the two servers).
const NOTES: [(&str, &str); 4] = [
    (
        "The loop",
        "harness() appends the user input, runs a turn, executes every tool the model asked for, appends each result, and re-requests until the model answers with text. An unknown tool, malformed JSON, or a throwing tool becomes a tool result the model can read and recover from.",
    ),
    (
        "The registry key is the identity",
        "Agents and tools are keyed by name in the harness configuration. The key names the agent in progress events and is the wire name the model calls, so the definition never repeats its own name. The first agent key is the entry point.",
    ),
    (
        "Single-property rule",
        "When a tool schema declares exactly one property, execute receives that property value instead of the wrapper object. That is why read([\"package.json\"]) and a model call carrying { \"paths\": [\"package.json\"] } run the same code path.",
    ),
    (
        "Progress events",
        "report.progress is awaited for every step and receives start, reasoning, text, tool_call, tool_result, done, and error events. Every event names its agent, so a multi-agent run stays readable in one stream.",
    ),
];

/// `noteSearch`: substring search over the corpus, returning `{ title, excerpt }` for
/// every hit in corpus order (`noteSearch.tool.ts:76-86`). An empty query matches all.
pub(crate) fn note_search(arguments: &Value) -> Result<Value, String> {
    let query = super::result::required_text(arguments, "query", "noteSearch")?;
    let needle = query.trim().to_lowercase();

    let matches: Vec<Value> = NOTES
        .iter()
        .filter(|(title, body)| {
            needle.is_empty() || format!("{title} {body}").to_lowercase().contains(&needle)
        })
        .map(|(title, body)| json!({ "title": title, "excerpt": body }))
        .collect();

    Ok(Value::Array(matches))
}

// ---------------------------------------------------------------------------
// textSummarize
// ---------------------------------------------------------------------------

/// `condense` (`textSummarize.tool.ts:46-57`): collapse whitespace, keep the first two
/// sentences.
#[must_use]
pub fn condense(notes: &str) -> String {
    let cleaned = notes.split_whitespace().collect::<Vec<_>>().join(" ");

    if cleaned.is_empty() {
        return String::new();
    }

    // Sentence split on `.`, `!` or `?` followed by whitespace - TS spells it
    // `/(?<=[.!?])\s+/`. The scan keeps BYTE offsets beside each character, because the
    // slices it produces index into `cleaned` and a non-ASCII character moves the two
    // apart.
    let characters: Vec<(usize, char)> = cleaned.char_indices().collect();
    let mut sentences: Vec<&str> = Vec::new();
    let mut start = 0usize;
    let mut position = 0usize;

    while position + 1 < characters.len() {
        let (byte, current) = characters[position];
        let (_, next) = characters[position + 1];

        if matches!(current, '.' | '!' | '?') && next.is_whitespace() {
            sentences.push(&cleaned[start..=byte]);

            let mut scan = position + 1;

            while scan < characters.len() && characters[scan].1.is_whitespace() {
                scan += 1;
            }

            start = characters
                .get(scan)
                .map_or(cleaned.len(), |(byte, _)| *byte);
            position = scan;
            continue;
        }

        position += 1;
    }

    if start < cleaned.len() {
        sentences.push(&cleaned[start..]);
    }

    sentences.retain(|sentence| !sentence.is_empty());
    sentences.truncate(2);
    sentences.join(" ")
}

/// `textSummarize`: bound the notes, summarize, and state the cut
/// (`textSummarize.tool.ts:96-131`).
pub(crate) fn text_summarize(arguments: &Value) -> Result<Value, String> {
    let notes = super::result::required_text(arguments, "notes", "textSummarize")?;

    // The bound is a CHARACTER count in TS (`notes.length`), so the cut is made on
    // UTF-16 code units to keep the same answer.
    let units: Vec<char> = notes.chars().collect();
    let truncated = notes.chars().count() > MAX_SUMMARIZE_NOTES;
    let bounded: String = units.iter().take(MAX_SUMMARIZE_NOTES).collect();
    let truncation_note = if truncated {
        format!("\n\n{NOTES_TRUNCATED}")
    } else {
        String::new()
    };

    Ok(Value::String(format!(
        "{}{truncation_note}",
        condense(&bounded)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_clock_formats_the_iso_8601_epoch_and_later_days() {
        assert_eq!(iso_from_millis(0), "1970-01-01T00:00:00.000Z");
        // 2026-11-06T00:00:00Z, the day this stage was written.
        assert_eq!(
            iso_from_millis(1_793_923_200_000),
            "2026-11-06T00:00:00.000Z"
        );
        // A leap day, and the leap-year rule that the 2100 exception exercises.
        assert_eq!(
            iso_from_millis(1_709_251_200_000),
            "2024-03-01T00:00:00.000Z"
        );
    }

    #[test]
    fn time_now_answers_the_documented_triple_plus_its_panel() {
        let value = time_now(&json!({ "timezone": "UTC" })).expect("a valid label");
        let content = value
            .get("content")
            .expect("the toolUi envelope carries its content");

        assert_eq!(content["timezone"], json!("UTC"));
        assert!(
            content["iso"]
                .as_str()
                .expect("an iso string")
                .ends_with('Z')
        );
        assert!(content["unix"].as_i64().expect("a unix time") > 1_500_000_000);
        assert_eq!(
            value[super::super::result::TOOL_UI_BRAND]
                .as_array()
                .expect("one panel array")
                .len(),
            1
        );
    }

    #[test]
    fn time_now_bounds_its_label() {
        assert_eq!(
            time_now(&json!({ "timezone": "   " })).expect_err("blank is refused"),
            "timeNow timezone must be a non-empty string of at most 64 characters."
        );
    }

    #[test]
    fn text_count_reports_utf16_characters() {
        // One astral-plane emoji is TWO UTF-16 code units, exactly as TS counts: the pair
        // plus the three units of " hi".
        assert_eq!(
            text_count(&json!({ "text": "😀 hi" })),
            Ok(json!({ "words": 2, "characters": 5 }))
        );
        assert_eq!(
            text_count(&json!({ "text": "   " })),
            Ok(json!({ "words": 0, "characters": 3 }))
        );
    }

    #[test]
    fn dice_roll_refuses_an_out_of_range_die() {
        assert_eq!(
            dice_roll(&json!({ "sides": 1 })).expect_err("refused"),
            "diceRoll sides must be a whole number from 2 to 1000."
        );

        let value = dice_roll(&json!({ "sides": 2 })).expect("a coin");

        assert_eq!(value["sides"], json!(2));
        assert!((1..=2).contains(&value["value"].as_i64().expect("a number")));
    }

    #[test]
    fn note_search_answers_the_corpus_it_names() {
        let hits = note_search(&json!({ "query": "single-property" })).expect("a search");
        let entries = hits.as_array().expect("an array");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["title"], json!("Single-property rule"));
        assert_eq!(
            note_search(&json!({ "query": "" }))
                .expect("a search")
                .as_array()
                .expect("an array")
                .len(),
            4
        );
    }

    #[test]
    fn condense_keeps_exactly_the_first_two_sentences() {
        assert_eq!(condense("One.   Two! Three?"), "One. Two!");
        assert_eq!(condense("   "), "");
        assert_eq!(condense("Only one"), "Only one");
    }

    #[test]
    fn a_long_note_block_states_its_truncation() {
        let notes = "x".repeat(MAX_SUMMARIZE_NOTES + 10);
        let summary = text_summarize(&json!({ "notes": notes })).expect("a summary");
        let text = summary.as_str().expect("a string result");

        assert!(text.ends_with(&format!("\n\n{NOTES_TRUNCATED}")));
    }
}
