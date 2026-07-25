use std::io;
use std::sync::Arc;

use crate::session::types::Session;

/// Merge messages from a live runner snapshot into an already-durable
/// transcript without ever removing or rewriting a durable message.
///
/// Runtime sessions are append-oriented, and every newly-created message has a
/// stable id.  A runner may nevertheless be holding a snapshot that predates a
/// concurrent append (for example, an injected child-completion message).  A
/// terminal/error checkpoint must not full-save that stale snapshot: doing so
/// would shrink the transcript.  Keep the durable ordering and append only the
/// live messages whose ids are not durable yet.
pub fn append_missing_runtime_messages(session: &mut Session, durable: &Session) -> usize {
    let mut seen = durable
        .messages
        .iter()
        .map(|message| message.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let missing = session
        .messages
        .iter()
        .filter(|message| seen.insert(message.id.clone()))
        .cloned()
        .collect::<Vec<_>>();
    let appended = missing.len();
    session.messages = durable.messages.iter().cloned().chain(missing).collect();
    appended
}

/// Merge the durable SessionInbox admitted-id cursor into a writer snapshot.
///
/// Runtime writers can hold a session clone from before another run admitted
/// an inbox message. No later full save may erase that durable dedupe state.
pub fn merge_session_inbox_admission(session: &mut Session, durable: &Session) {
    let Some(durable_state) = durable.session_inbox_admission().cloned() else {
        return;
    };
    session
        .session_inbox_admission_mut()
        .merge_from(&durable_state);
}

/// Restore durable provider messages identified by their typed
/// `metadata.session_message` marker into a stale writer without preserving
/// unrelated durable suffixes. The bounded cursor is only a fast recent index;
/// the transcript marker is the unbounded source of truth after cursor
/// eviction.
///
/// Insertion follows durable transcript neighbors so an admitted user/runtime
/// message remains ahead of any later assistant output held by the stale
/// runner. This is narrower than [`append_missing_runtime_messages`], retaining
/// the historical shrink semantics for unrelated concurrent messages while
/// making a cursor/tombstone incapable of outliving its transcript entry.
pub fn restore_missing_admitted_inbox_messages(session: &mut Session, durable: &Session) -> usize {
    let admission = durable.session_inbox_admission();
    let mut restored = 0;
    for (durable_index, message) in durable.messages.iter().enumerate() {
        let typed_marker = message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("session_message"))
            .is_some_and(|marker| {
                marker.get("id").and_then(serde_json::Value::as_str) == Some(message.id.as_str())
                    && marker
                        .get("target_session_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(durable.id.as_str())
                    && crate::SessionMessageId::parse(message.id.clone()).is_ok()
            });
        let recent_cursor = admission.is_some_and(|state| state.contains_str(&message.id));
        if !(typed_marker || recent_cursor)
            || session
                .messages
                .iter()
                .any(|current| current.id == message.id)
        {
            continue;
        }

        let insertion = durable.messages[..durable_index]
            .iter()
            .rev()
            .find_map(|predecessor| {
                session
                    .messages
                    .iter()
                    .position(|current| current.id == predecessor.id)
                    .map(|index| index + 1)
            })
            .or_else(|| {
                durable.messages[durable_index + 1..]
                    .iter()
                    .find_map(|successor| {
                        session
                            .messages
                            .iter()
                            .position(|current| current.id == successor.id)
                    })
            })
            .unwrap_or(session.messages.len());
        session.messages.insert(insertion, message.clone());
        restored += 1;
    }
    restored
}

/// Port for runtime (non-authoritative) session persistence.
///
/// Implementors must:
/// - Serialize concurrent saves per session ID.
/// - Merge on-disk authoritative metadata (`title`, `pinned`, `title_version`,
///   `metadata_version`) before writing, so UI edits are never clobbered.
#[async_trait::async_trait]
pub trait RuntimeSessionPersistence: Send + Sync {
    /// Persist the session, merging any newer authoritative metadata from disk.
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()>;

    /// Append-safe checkpoint used at the shared engine execute boundary.
    ///
    /// Unlike [`save_runtime_session`](Self::save_runtime_session), this must
    /// preserve messages that were appended durably by a concurrent writer
    /// after the runner loaded its snapshot.  Implementations that can provide
    /// a per-session transaction should override this method and perform the
    /// load/merge/save under one lock.  The default still reconciles against a
    /// latest snapshot for lightweight/custom SDK persisters; the built-in
    /// storage implementation supplies the atomic variant.
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        if let Some(durable) = self.load_runtime_session(&session.id).await? {
            append_missing_runtime_messages(session, &durable);
            merge_session_inbox_admission(session, &durable);
        }
        self.save_runtime_session(session).await
    }

    /// Load the latest runtime-visible session snapshot when the persistence
    /// implementation can coordinate reads. Tools may update a repository-owned
    /// clone while an agent loop holds its own live Session; the loop uses this
    /// hook to merge narrowly-scoped tool side effects before its next save.
    async fn load_runtime_session(&self, _session_id: &str) -> io::Result<Option<Session>> {
        Ok(None)
    }

    /// Clear the bounded compatibility queue iff it still equals the entries
    /// that were durably copied into SessionInbox. Implementations with a
    /// per-session transaction should override this method.
    async fn clear_legacy_pending_messages(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
    ) -> io::Result<bool> {
        let Some(mut latest) = self.load_runtime_session(session_id).await? else {
            return Ok(false);
        };
        if latest.pending_injected_messages().as_deref() != Some(expected) {
            return Ok(false);
        }
        latest.clear_pending_injected_messages();
        self.save_runtime_session(&mut latest).await?;
        Ok(true)
    }

    /// Append one JSON-line analysis record to the session's append-only
    /// token-usage log (see [`Storage::append_token_usage_record`]). Defaults to
    /// a no-op so non-file-backed persisters are unaffected.
    ///
    /// [`Storage::append_token_usage_record`]: crate::storage::Storage::append_token_usage_record
    async fn append_token_usage_record(&self, session_id: &str, json_line: &str) -> io::Result<()> {
        let _ = (session_id, json_line);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: RuntimeSessionPersistence + ?Sized> RuntimeSessionPersistence for Arc<T> {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        (**self).save_runtime_session(session).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        (**self).checkpoint_runtime_session(session).await
    }

    async fn load_runtime_session(&self, session_id: &str) -> io::Result<Option<Session>> {
        (**self).load_runtime_session(session_id).await
    }

    async fn clear_legacy_pending_messages(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
    ) -> io::Result<bool> {
        (**self)
            .clear_legacy_pending_messages(session_id, expected)
            .await
    }

    async fn append_token_usage_record(&self, session_id: &str, json_line: &str) -> io::Result<()> {
        (**self)
            .append_token_usage_record(session_id, json_line)
            .await
    }
}
