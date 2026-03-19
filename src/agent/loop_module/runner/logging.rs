pub(super) struct DebugLogger {
    pub(super) enabled: bool,
}

impl DebugLogger {
    pub(super) fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    pub(super) fn log_event(&self, session_id: &str, event_type: &str, details: serde_json::Value) {
        if !self.enabled {
            return;
        }

        tracing::debug!("[{}] {}: {}", session_id, event_type, details);
    }
}
