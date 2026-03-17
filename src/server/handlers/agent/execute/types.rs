use crate::core::ReasoningEffort;
use serde::{Deserialize, Serialize};

/// Response returned after triggering agent execution.
///
/// # Fields
///
/// * `session_id` - The session identifier
/// * `status` - Execution status ("started", "completed", "already_running")
/// * `events_url` - URL endpoint to subscribe to agent events (SSE)
#[derive(Serialize)]
pub struct ExecuteResponse {
    /// Session identifier for tracking this execution
    pub session_id: String,
    /// Current execution status
    pub status: String,
    /// SSE endpoint URL for receiving real-time events
    pub events_url: String,
}

/// Request payload for agent execution.
///
/// # Fields
///
/// * `model` - Required model identifier for execution
///
/// # Note
///
/// The `model` parameter is **required** and must be provided in every request.
/// It is not read from the session. This ensures explicit model selection
/// for each execution.
///
/// # Examples
///
/// ```json
/// {
///   "model": "claude-3-opus"
/// }
/// ```
#[derive(Deserialize)]
pub struct ExecuteRequest {
    /// Model to use for execution (required)
    pub model: String,
    /// Optional reasoning effort override for this execution.
    ///
    /// When omitted, the active provider default from config is used.
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_response_serialization() {
        let response = ExecuteResponse {
            session_id: "session-123".to_string(),
            status: "started".to_string(),
            events_url: "/api/v1/execute/session-123/events".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("session-123"));
        assert!(json.contains("started"));
        assert!(json.contains("events"));
    }

    #[test]
    fn test_execute_response_completed() {
        let response = ExecuteResponse {
            session_id: "sess-456".to_string(),
            status: "completed".to_string(),
            events_url: "/events/sess-456".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("completed"));
    }

    #[test]
    fn test_execute_request_deserialization() {
        let json = r#"{"model":"claude-3-opus"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "claude-3-opus");
        assert!(req.reasoning_effort.is_none());
    }

    #[test]
    fn test_execute_request_with_reasoning_effort() {
        let json = r#"{"model":"claude-3-opus","reasoning_effort":"high"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "claude-3-opus");
        assert!(req.reasoning_effort.is_some());
    }

    #[test]
    fn test_execute_request_gpt4() {
        let json = r#"{"model":"gpt-4"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "gpt-4");
    }

    #[test]
    fn test_execute_request_empty_model() {
        let json = r#"{"model":""}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "");
    }

    #[test]
    fn test_execute_request_special_characters_in_model() {
        let json = r#"{"model":"claude-3-opus-20240229"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "claude-3-opus-20240229");
    }
}
