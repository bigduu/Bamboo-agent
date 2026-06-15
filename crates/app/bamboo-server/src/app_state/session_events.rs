//! Session event sender management.
//!
//! Re-exports the shared `get_or_create_event_sender` function from the runtime crate.
//! The `impl AppState` method delegates to the re-exported function.

pub use bamboo_engine::execution::session_events::get_or_create_event_sender;

impl super::AppState {
    /// Get (or create) a long-lived session event sender for a session id.
    ///
    /// This stream is intended for UI consumption and background activity; it should remain
    /// available even when no agent execution is running.
    pub async fn get_session_event_sender(
        &self,
        session_id: &str,
    ) -> tokio::sync::broadcast::Sender<bamboo_agent_core::AgentEvent> {
        get_or_create_event_sender(&self.session_event_senders, session_id).await
    }

    /// Ensure a notification relay task is running for `session_id`.
    ///
    /// The relay subscribes to the session's event broadcast, runs the backend
    /// notification policy on each event, and re-broadcasts any resulting
    /// `AgentEvent::Notification` onto the same channel so connected SSE clients
    /// receive it. Idempotent: at most one relay per session (tracked by the
    /// notification service). The relay does not hold a `Sender` clone, so the
    /// channel still closes naturally when the session is torn down.
    pub fn ensure_notification_relay(
        &self,
        session_id: &str,
        sender: tokio::sync::broadcast::Sender<bamboo_agent_core::AgentEvent>,
    ) {
        if !self.notification_service.try_begin_relay(session_id) {
            return;
        }
        let service = self.notification_service.clone();
        let senders = self.session_event_senders.clone();
        let sid = session_id.to_string();
        let mut rx = sender.subscribe();
        drop(sender);
        tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if let Some(notification) = service.notify(&sid, &event) {
                            let tx = senders.read().await.get(&sid).cloned();
                            if let Some(tx) = tx {
                                let _ = tx.send(notification);
                            }
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
            service.end_relay(&sid);
        });
    }
}
