use crate::core::ReasoningEffort;
use serde::{Deserialize, Serialize};

/// Stable reasons explaining why the frontend must resynchronize before execute.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteSyncReason {
    MessageCountMismatch,
    LastMessageIdMismatch,
    PendingQuestionMismatch,
}

impl ExecuteSyncReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MessageCountMismatch => "message_count_mismatch",
            Self::LastMessageIdMismatch => "last_message_id_mismatch",
            Self::PendingQuestionMismatch => "pending_question_mismatch",
        }
    }
}

/// Client waterline used to detect stale frontend state before execution starts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecuteClientSync {
    /// Number of messages the client last confirmed from the server.
    pub client_message_count: usize,
    /// Last confirmed backend message id known by the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_last_message_id: Option<String>,
    /// Whether the client currently believes the session is waiting for a question response.
    #[serde(default)]
    pub client_has_pending_question: bool,
    /// Tool call id for the currently pending question, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_pending_question_tool_call_id: Option<String>,
}

/// Server snapshot describing whether the frontend must reload state before execute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecuteSyncInfo {
    /// Whether the client must resynchronize before a new execution can start.
    pub need_sync: bool,
    /// Specific mismatch reason when `need_sync == true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<ExecuteSyncReason>,
    /// Current number of persisted messages on the server.
    pub server_message_count: usize,
    /// Current last persisted message id on the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_last_message_id: Option<String>,
    /// Whether the server is waiting for a pending conclusion_with_options-style response.
    pub has_pending_question: bool,
    /// Tool call id for the pending question on the server, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_question_tool_call_id: Option<String>,
    /// Whether the server sees resumable pending user work for execute.
    pub has_pending_user_message: bool,
}

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
    /// Optional sync snapshot allowing the frontend to reconcile with server state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync: Option<ExecuteSyncInfo>,
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
    /// Optional per-execution skill mode override (for example: "code", "ask").
    ///
    /// When provided, skill discovery prefers `skills-<mode>` directories.
    #[serde(default)]
    pub skill_mode: Option<String>,
    /// Optional reasoning effort override for this execution.
    ///
    /// When omitted, the active provider default from config is used.
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional server-confirmed client cursor used for pre-execution sync checks.
    #[serde(default)]
    pub client_sync: Option<ExecuteClientSync>,
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
            sync: None,
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
            sync: Some(ExecuteSyncInfo {
                need_sync: false,
                reason: None,
                server_message_count: 3,
                server_last_message_id: Some("msg-3".to_string()),
                has_pending_question: false,
                pending_question_tool_call_id: None,
                has_pending_user_message: false,
            }),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("completed"));
        assert!(json.contains("server_message_count"));
    }

    #[test]
    fn test_execute_request_deserialization() {
        let json = r#"{"model":"claude-3-opus"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "claude-3-opus");
        assert!(req.skill_mode.is_none());
        assert!(req.reasoning_effort.is_none());
        assert!(req.client_sync.is_none());
    }

    #[test]
    fn test_execute_request_with_reasoning_effort() {
        let json = r#"{"model":"claude-3-opus","reasoning_effort":"high"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "claude-3-opus");
        assert!(req.skill_mode.is_none());
        assert!(req.reasoning_effort.is_some());
        assert!(req.client_sync.is_none());
    }

    #[test]
    fn test_execute_request_with_skill_mode() {
        let json = r#"{"model":"claude-3-opus","skill_mode":"code"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.model, "claude-3-opus");
        assert_eq!(req.skill_mode.as_deref(), Some("code"));
    }

    #[test]
    fn test_execute_request_with_client_sync() {
        let json = r#"{
            "model":"claude-3-opus",
            "client_sync":{
                "client_message_count":12,
                "client_last_message_id":"msg-12",
                "client_has_pending_question":true,
                "client_pending_question_tool_call_id":"toolu_123"
            }
        }"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();

        let client_sync = req.client_sync.expect("client sync should deserialize");
        assert_eq!(client_sync.client_message_count, 12);
        assert_eq!(
            client_sync.client_last_message_id.as_deref(),
            Some("msg-12")
        );
        assert!(client_sync.client_has_pending_question);
        assert_eq!(
            client_sync.client_pending_question_tool_call_id.as_deref(),
            Some("toolu_123")
        );
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

    #[test]
    fn test_execute_sync_reason_serializes_as_stable_string() {
        let json = serde_json::to_string(&ExecuteSyncReason::PendingQuestionMismatch).unwrap();
        assert_eq!(json, "\"pending_question_mismatch\"");
    }
}
