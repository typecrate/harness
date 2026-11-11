//! Transcript reads and the whole-document write.
//!
//! Ported from `underload/harness/src/server/api-reads.ts:626-683` (metadata, signal, unreadable
//! placeholders) and `server/api.ts:1096-1196` (the four transcript routes).
//!
//! THE ONE RULE THIS MODULE EXISTS TO ENFORCE
//! ------------------------------------------
//! A transcript is user-editable JSON. A structurally broken value is therefore DATA, not a bad
//! request: the store keeps the raw text in `record.invalid_threads`, installs a VALID placeholder in
//! the live transcript so the next run still has something to read, and reports the outcome in the
//! receipt's `accepted_as`/`invalid` pair (`harness-api.ts:1541-1560`). Only a malformed body or a
//! revision conflict is a 4xx.

use serde_json::{Value, json};

use crate::contract::{
    HarnessMessage, PageMeta, TranscriptAcceptedAs, TranscriptDocumentResponse,
    TranscriptInvalidDetail, TranscriptMessagePage, TranscriptMetadata, TranscriptState,
    TranscriptWriteLinks, TranscriptWriteReceipt, UpdateThreadTranscriptRequest,
};
use crate::error::{ApiError, HarnessResult};
use crate::persistence::{HarnessInvalidThreadRecord, HarnessThreadRecord};
use crate::ports::Query;
use crate::store::read;
use crate::store::{SessionState, Store, StoreInner, now_ms, threads};

/// The role filter of `GET /sessions/:id/transcript/:thread` (`api.ts:1120`).
///
/// An unknown role is treated as "no filter", exactly like TS's `paramOf(...) ?? undefined` followed by a
/// membership test on the four roles: the route never refuses a bad role, it just reads every entry.
#[must_use]
pub fn parse_role_filter(query: &Query) -> Option<&str> {
    query
        .get("role")
        .map(|role| role.trim())
        .filter(|role| !role.is_empty())
        .filter(|role| matches!(*role, "system" | "user" | "assistant" | "tool"))
}

/// The live transcript of one conversation, `None` when the conversation is unknown.
impl SessionState {
    #[must_use]
    pub fn transcript_messages(&self, thread_id: &str) -> Option<Vec<HarnessMessage>> {
        self.threads
            .get(thread_id)
            .map(|thread| thread.messages.clone())
    }

    /// Whether one conversation's stored document failed validation (`record.invalidThreads`).
    #[must_use]
    pub fn transcript_is_unreadable(&self, thread_id: &str) -> bool {
        self.invalid_threads.contains_key(thread_id)
    }

    /// The invalid record of one conversation, if it has one.
    #[must_use]
    pub fn transcript_invalid(&self, thread_id: &str) -> Option<&HarnessInvalidThreadRecord> {
        self.invalid_threads.get(thread_id)
    }

    /// `sessions.ts:806-817 unreadableTranscript`: the placeholder a broken document gets in the LIVE
    /// transcript.
    ///
    /// A `system` entry is the only role with no structural partners to satisfy (no tool-call pairing,
    /// no attachment validation), so the placeholder can never itself become the invalid-transcript
    /// problem — and a run of the conversation still gets a readable explanation.
    #[must_use]
    pub fn unreadable_transcript(error: &str) -> Vec<HarnessMessage> {
        vec![HarnessMessage::System {
            content: format!("{}{error}]", TRANSCRIPT_UNREADABLE_MARKER),
            included: None,
            message_id: None,
        }]
    }

    /// `sessions.ts:820-830 isUnreadableTranscript`: is this live transcript still a placeholder?
    ///
    /// Tolerant restore uses this to tell "the operator repaired the document by hand" (the persisted
    /// raw entry is now stale) from "this conversation is still broken" (the raw entry is the ONLY copy
    /// of what the user wrote).
    #[must_use]
    pub fn is_unreadable_transcript(messages: &[HarnessMessage]) -> bool {
        let Some(HarnessMessage::System { content, .. }) = messages.first() else {
            return false;
        };
        messages.len() == 1 && content.starts_with(TRANSCRIPT_UNREADABLE_MARKER)
    }

    /// Drops the transcripts of destroyed conversations, keeping the roster honest.
    pub fn discard_transcripts(&mut self, thread_ids: &[String]) {
        for thread_id in thread_ids {
            self.threads.remove(thread_id);
            self.thread_order.retain(|id| id != thread_id);
            self.invalid_threads.remove(thread_id);
            self.thread_labels.remove(thread_id);
            self.record
                .thread_labels
                .as_mut()
                .map(|labels| labels.remove(thread_id));
            self.record
                .invalid_threads
                .as_mut()
                .and_then(|invalids| invalids.remove(thread_id));
            self.steer_mailbox.remove(thread_id);
            self.lanes.remove(thread_id);
        }
        self.active_thread
            .take_if(|active| !self.threads.contains_key(active));
    }

    /// Replaces one conversation's transcript, keeping `updated_at` honest.
    pub fn set_transcript(&mut self, thread_id: &str, messages: Vec<HarnessMessage>) {
        if let Some(thread) = self.threads.get_mut(thread_id) {
            thread.messages = messages;
            thread.transcript_revision = Some(thread.transcript_revision.unwrap_or(0) + 1);
        }
        self.record.updated_at = now_ms();
    }
}

impl Store {
    /// `GET /sessions/:id/transcript-meta/:thread` (`api-reads.ts:626-658`): size, counts, revision
    /// and validity — no content at all.
    #[must_use]
    pub fn transcript_metadata(
        &self,
        state: &SessionState,
        thread_id: &str,
        agent: &str,
    ) -> TranscriptMetadata {
        let messages = state.transcript_messages(thread_id).unwrap_or_default();
        read::transcript_metadata_of(state, thread_id, agent, &messages, &self.hub)
    }

    /// `GET /sessions/:id/transcript/:thread` (`api.ts:1120-1140`): paged entries, with `total` the
    /// entry count of the WHOLE transcript, independent of the role filter.
    #[must_use]
    pub fn transcript_page(
        &self,
        state: &SessionState,
        thread_id: &str,
        agent: &str,
        query: &Query,
        limit: u64,
        cursor: u64,
    ) -> TranscriptMessagePage {
        let messages = state.transcript_messages(thread_id).unwrap_or_default();
        let role = parse_role_filter(query);
        let total = u64::try_from(messages.len()).unwrap_or(u64::MAX);

        let filtered: Vec<HarnessMessage> = messages
            .into_iter()
            .filter(|entry| role.is_none_or(|want| read::message_role_name(entry) == want))
            .collect();

        let start = usize::try_from(cursor).unwrap_or(usize::MAX);
        let take = usize::try_from(limit).unwrap_or(usize::MAX);
        let items: Vec<HarnessMessage> = filtered.into_iter().skip(start).take(take).collect();
        let served = start + items.len();

        // `next` is issued ONLY for an exactly-full page that has not reached the end of the document
        // (`api.ts:3605`: `served < messages.length && rows.length === limit`). Minting a cursor after
        // a SHORT page is what made a role filter loop forever: the filtered list is shorter than
        // `total`, so the old test re-issued the same cursor with nothing left to serve.
        let next = if items.len() == take && served < usize::try_from(total).unwrap_or(usize::MAX) {
            Some(served as u64)
        } else {
            None
        };

        // The paged read reports the `transcript_messages` counter of the conversation, which is the
        // figure a client watching `transcript_messages` compares against (`api.ts:3605`).
        let revision = self.hub.revision(
            "transcript_messages",
            Some(&crate::store::thread_key(state.id(), thread_id)),
        );

        TranscriptMessagePage {
            meta: self.page_meta(revision, next, items.len()),
            thread_id: thread_id.to_owned(),
            agent: agent.to_owned(),
            transcript_state: if state.transcript_is_unreadable(thread_id) {
                TranscriptState::Unreadable
            } else {
                TranscriptState::Ok
            },
            total,
            items,
        }
    }

    /// `GET /sessions/:id/transcript-document/:thread` (`api-reads.ts:661-670`): the whole transcript
    /// as one JSON TEXT document, which is the editor's and the export surface.
    ///
    /// An UNREADABLE conversation answers with its raw stored value, not with the placeholder: the
    /// editor must show the user what they wrote, not what the store made of it.
    #[must_use]
    pub fn transcript_document(
        &self,
        state: &SessionState,
        thread_id: &str,
        agent: &str,
    ) -> TranscriptDocumentResponse {
        let invalid = state.transcript_invalid(thread_id);
        let document = match invalid {
            Some(entry) => entry.messages.to_string(),
            None => {
                serde_json::to_string(&state.transcript_messages(thread_id).unwrap_or_default())
                    .unwrap_or_else(|_| "[]".to_owned())
            }
        };

        TranscriptDocumentResponse {
            session_id: state.id().to_owned(),
            thread_id: thread_id.to_owned(),
            agent: agent.to_owned(),
            revision: state
                .threads
                .get(thread_id)
                .and_then(|thread| thread.transcript_revision)
                .unwrap_or(0),
            transcript_state: if invalid.is_some() {
                TranscriptState::Unreadable
            } else {
                TranscriptState::Ok
            },
            document,
            content_type: Some("application/json".to_owned()),
        }
    }

    /// `PUT /sessions/:id/transcript/:thread` (`api.ts:1142-1196`): a whole-document write.
    ///
    /// The order is load-bearing: the transcript (or the invalid record) is installed, THEN the counters
    /// move, THEN the stream sees it, and the document is written before any of that is reported —
    /// because a failed write must leave the previous transcript in place, exactly as a failed task
    /// submit leaves no turn.
    pub fn put_transcript(
        &self,
        inner: &mut StoreInner,
        session_id: &str,
        thread_id: &str,
        body: &UpdateThreadTranscriptRequest,
    ) -> HarnessResult<(TranscriptWriteReceipt, CommitOutcome)> {
        let state = inner
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| self.unknown_session(session_id))?;

        let thread = state
            .threads
            .get(thread_id)
            .cloned()
            .ok_or_else(|| threads::unknown_thread(thread_id))?;

        let current = thread.transcript_revision.unwrap_or(0);

        // `if_revision` is an optimistic-concurrency guard: a stale edit must not silently overwrite a
        // newer one (`api.ts:1150-1158`).
        if let Some(expected) = body.if_revision
            && expected != current
        {
            return Err(ApiError::conflict(format!(
                "This transcript is at revision {current}; the write expected {expected}. Re-read before writing."
            )));
        }

        // A transcript is validated as data. `serde_json::from_value` is this port's `readHarnessMessages`:
        // the message union's serde impl IS the contract's shape, so a structural failure is exactly the
        // condition TS reports.
        let (accepted, error) =
            match serde_json::from_value::<Vec<HarnessMessage>>(body.messages.clone()) {
                Ok(messages) => (messages, None),
                Err(error) => (
                    SessionState::unreadable_transcript(&error.to_string()),
                    Some(error.to_string()),
                ),
            };

        let revision = current + 1;
        let accepted_as = if error.is_none() {
            TranscriptAcceptedAs::Transcript
        } else {
            TranscriptAcceptedAs::Raw
        };

        let message_count = u64::try_from(accepted.len()).unwrap_or(u64::MAX);

        // Install: live transcript always, plus either a cleared or a written invalid record.
        state.set_transcript(thread_id, accepted);
        if let Some(thread) = state.threads.get_mut(thread_id) {
            thread.transcript_revision = Some(revision);
        }
        match error.as_ref() {
            Some(error) => {
                // The invalid record keeps the RAW value and the identity of the conversation it belongs
                // to (`protocol.ts:153` `invalidThreads[threadId] = {error, messages, label?, agent?}`).
                state.invalid_threads.insert(
                    thread_id.to_owned(),
                    HarnessInvalidThreadRecord {
                        error: error.clone(),
                        messages: body.messages.clone(),
                        label: thread.label.clone(),
                        agent: Some(thread.agent.clone()),
                        extra: serde_json::Map::new(),
                    },
                );
            }
            None => {
                // A readable write replaces the broken document, so the placeholder record goes with it.
                state.invalid_threads.remove(thread_id);
            }
        }
        state.record.invalid_threads = (!state.invalid_threads.is_empty()).then(|| {
            state
                .invalid_threads
                .iter()
                .map(|(id, entry)| (id.clone(), entry.clone()))
                .collect()
        });
        state.touch_idle(now_ms(), self.config.idle_ms);

        // The receipt, then the publish recipe (`api.ts:1180-1195`): `transcript.updated` for the
        // conversation, `session.updated` for the aggregate.
        let receipt = TranscriptWriteReceipt {
            session_id: session_id.to_owned(),
            thread_id: thread_id.to_owned(),
            accepted_as,
            message_count,
            revision,
            invalid: error.as_ref().map(|error| TranscriptInvalidDetail {
                error: error.clone(),
                stored_type: read::stored_type_of(&body.messages),
            }),
            links: Some(transcript_links(session_id, thread_id)),
        };

        let signal = read::transcript_signal_of(
            thread_id,
            revision,
            &state.transcript_messages(thread_id).unwrap_or_default(),
            error.is_some(),
        );

        // The hub keys a conversation as `session:thread` (`api.ts` `threadKey`), and one transcript
        // write moves BOTH of its counters: `transcript` (the figure the metadata read and the thread
        // resource report) and `transcript_messages` (the figure the paged message read reports) -
        // TS bumps both in the same publish (`api.ts:1176-1185`).
        let key = crate::store::thread_key(session_id, thread_id);

        let outcome = CommitOutcome::touch(session_id)
            .event(
                "transcript.updated",
                &[
                    ("session", Some(session_id)),
                    ("transcript", Some(key.as_str())),
                    ("transcript_messages", Some(key.as_str())),
                    ("thread", Some(key.as_str())),
                ],
                Some(thread_id),
                None,
                serde_json::to_value(&signal).unwrap_or(Value::Null),
            )
            .event(
                "session.updated",
                &[("session", Some(session_id))],
                Some(thread_id),
                None,
                json!({ "session_id": session_id, "revision": current }),
            );

        Ok((receipt, outcome))
    }

    /// The three transcript reads of one conversation, as the receipt prints them
    /// (`harness-api.ts:1562`).
    #[must_use]
    pub fn transcript_links(session_id: &str, thread_id: &str) -> TranscriptWriteLinks {
        transcript_links(session_id, thread_id)
    }

    /// The revision the THREAD RECORD committed for one conversation.
    ///
    /// This is NOT [`Store::transcript_revision_of`], which reads the HUB counter a page reports: the two
    /// disagree whenever a write failed after the record advanced, and the receipt must state the number
    /// the next `if_revision` guard compares against (`api.ts:1150-1158`).
    #[must_use]
    pub fn stored_transcript_revision_of(&self, state: &SessionState, thread_id: &str) -> u64 {
        state
            .threads
            .get(thread_id)
            .and_then(|thread| thread.transcript_revision)
            .unwrap_or(0)
    }
}

/// The link set of one conversation's three reads.
#[must_use]
fn transcript_links(session_id: &str, thread_id: &str) -> TranscriptWriteLinks {
    TranscriptWriteLinks {
        transcript: Some(format!(
            "{}/transcript/{thread_id}",
            session_path(session_id)
        )),
        messages: Some(format!(
            "{}/transcript/{thread_id}",
            session_path(session_id)
        )),
        document: Some(format!(
            "{}/transcript-document/{thread_id}",
            session_path(session_id)
        )),
    }
}

/// The prefix every unreadable placeholder starts with (`sessions.ts:804`), kept as a constant so the
/// detector and the constructor cannot disagree.
const TRANSCRIPT_UNREADABLE_MARKER: &str = "[transcript unreadable: ";

/// Cross-references for the next agent.
#[allow(dead_code)]
type _Refs = (PageMeta, HarnessThreadRecord);

/// The mounted path of one session, which is the prefix every transcript link carries (HARNESS_API_ROOT).
#[must_use]
fn session_path(session_id: &str) -> String {
    format!(
        "{}/sessions/{session_id}",
        crate::contract::HARNESS_API_ROOT
    )
}

use crate::store::CommitOutcome;
