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

    async fn append_token_usage_record(&self, session_id: &str, json_line: &str) -> io::Result<()> {
        (**self)
            .append_token_usage_record(session_id, json_line)
            .await
    }
}
