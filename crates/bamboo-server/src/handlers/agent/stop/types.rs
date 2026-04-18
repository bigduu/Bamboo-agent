use serde::Serialize;

/// Response for stop request.
#[derive(Serialize)]
pub(super) struct StopResponse {
    /// Whether the stop operation succeeded
    pub(super) success: bool,
    /// Human-readable status message
    pub(super) message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stop_response_serialization() {
        let response = StopResponse {
            success: true,
            message: "Session stopped successfully".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"message\":\"Session stopped successfully\""));
    }

    #[test]
    fn test_stop_response_failure() {
        let response = StopResponse {
            success: false,
            message: "Session not found".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":false"));
    }

    #[test]
    fn test_stop_response_empty_message() {
        let response = StopResponse {
            success: true,
            message: String::new(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"message\":\"\""));
    }
}
