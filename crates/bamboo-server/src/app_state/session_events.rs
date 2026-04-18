//! Session event sender management.
//!
//! Re-exports the shared `get_or_create_event_sender` function from the runtime crate.
//! The `impl AppState` method delegates to the re-exported function.

pub use bamboo_application_runtime::execution::session_events::get_or_create_event_sender;

impl super::AppState {
    /// Get (or create) a long-lived session event sender for a session id.
    ///
    /// This stream is intended for UI consumption and background activity; it should remain
    /// available even when no agent execution is running.
    pub async fn get_session_event_sender(
        &self,
        session_id: &str,
    ) -> tokio::sync::broadcast::Sender<bamboo_application_agent::AgentEvent> {
        get_or_create_event_sender(&self.session_event_senders, session_id).await
    }
}
