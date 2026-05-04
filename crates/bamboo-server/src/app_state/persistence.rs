use super::*;

impl AppState {
    /// Save a complete session to persistent storage.
    ///
    /// Uses [`bamboo_infrastructure::LockedSessionStore::merge_save_runtime`]
    /// so any concurrent UI edits to the authoritative metadata group (`title`,
    /// `title_version`, `pinned`, `metadata_version`) are preserved when our
    /// runtime-side `metadata_version` does not strictly exceed disk's.
    ///
    /// # Arguments
    ///
    /// * `session` - Session object to save (mutated in-place to reflect the
    ///   merged authoritative metadata values).
    pub async fn save_session(&self, session: &mut bamboo_agent_core::Session) {
        if let Err(error) = self.persistence.merge_save_runtime(session).await {
            tracing::warn!("[{}] Failed to save session: {}", session.id, error);
        }
    }
}
