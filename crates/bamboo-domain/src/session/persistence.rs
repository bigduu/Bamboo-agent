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
}

#[async_trait::async_trait]
impl<T: RuntimeSessionPersistence + ?Sized> RuntimeSessionPersistence for Arc<T> {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        (**self).save_runtime_session(session).await
    }
}
