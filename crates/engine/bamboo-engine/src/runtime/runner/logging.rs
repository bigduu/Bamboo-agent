pub(crate) struct DebugLogger {
    pub(crate) enabled: bool,
}

impl DebugLogger {
    pub(crate) fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    pub(crate) fn log_event(&self, session_id: &str, event_type: &str, details: serde_json::Value) {
        if !self.enabled {
            return;
        }

        tracing::debug!("[{}] {}: {}", session_id, event_type, details);
    }
}
