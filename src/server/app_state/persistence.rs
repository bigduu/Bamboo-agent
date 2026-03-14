use super::*;

impl AppState {
    /// Save an agent event to persistent storage
    ///
    /// Appends the event to the session's event log in JSONL format.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    /// * `event` - Event to save
    #[allow(dead_code)]
    pub async fn save_event(&self, session_id: &str, event: &AgentEvent) {
        let _ = self.storage.append_event(session_id, event).await;
    }

    /// Save a complete session to persistent storage
    ///
    /// Writes the session metadata and all events to the storage backend.
    ///
    /// # Arguments
    ///
    /// * `session` - Session object to save
    pub async fn save_session(&self, session: &crate::agent::core::Session) {
        let _ = self.storage.save_session(session).await;
    }
}
