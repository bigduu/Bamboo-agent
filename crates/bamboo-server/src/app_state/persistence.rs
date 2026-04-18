use super::*;

impl AppState {
    /// Save a complete session to persistent storage
    ///
    /// Writes the session metadata to the storage backend.
    ///
    /// # Arguments
    ///
    /// * `session` - Session object to save
    pub async fn save_session(&self, session: &bamboo_application_agent::Session) {
        let _ = self.storage.save_session(session).await;
    }
}
