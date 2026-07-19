use std::io;
use std::sync::Arc;

use crate::session::types::Session;

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

    async fn load_runtime_session(&self, session_id: &str) -> io::Result<Option<Session>> {
        (**self).load_runtime_session(session_id).await
    }

    async fn append_token_usage_record(&self, session_id: &str, json_line: &str) -> io::Result<()> {
        (**self)
            .append_token_usage_record(session_id, json_line)
            .await
    }
}
