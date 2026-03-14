use super::*;

impl AppState {
    /// Get (or create) a long-lived session event sender for a session id.
    ///
    /// This stream is intended for UI consumption and background activity; it should remain
    /// available even when no agent execution is running.
    pub async fn get_session_event_sender(
        &self,
        session_id: &str,
    ) -> broadcast::Sender<AgentEvent> {
        let mut senders = self.session_event_senders.write().await;
        if let Some(existing) = senders.get(session_id) {
            return existing.clone();
        }
        let (tx, _) = broadcast::channel(1000);
        senders.insert(session_id.to_string(), tx.clone());
        tx
    }
}
