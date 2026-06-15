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

    /// Append one JSON-line analysis record to the session's append-only
    /// token-usage log (see [`Storage::append_token_usage_record`]). Defaults to
    /// a no-op so non-file-backed persisters are unaffected.
    ///
    /// [`Storage::append_token_usage_record`]: crate::storage::Storage::append_token_usage_record
    async fn append_token_usage_record(
        &self,
        session_id: &str,
        json_line: &str,
    ) -> io::Result<()> {
        let _ = (session_id, json_line);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: RuntimeSessionPersistence + ?Sized> RuntimeSessionPersistence for Arc<T> {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        (**self).save_runtime_session(session).await
    }

    async fn append_token_usage_record(
        &self,
        session_id: &str,
        json_line: &str,
    ) -> io::Result<()> {
        (**self)
            .append_token_usage_record(session_id, json_line)
            .await
    }
}
