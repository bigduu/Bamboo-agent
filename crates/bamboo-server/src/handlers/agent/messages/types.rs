use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PatchMessageRequest {
    pub content: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TruncateRequest {
    /// Truncate all messages *after* the last user message.
    ///
    /// This is useful for "retry/regenerate" flows: keep the last user message
    /// but drop any assistant/tool tail so `POST /execute/{session_id}` can run again.
    AfterLastUser,
    /// Preserve message history and mark the session for error retry.
    ///
    /// This is useful when a run failed transiently and we want to re-execute
    /// from the existing context (including prior successful tool calls).
    ErrorRetry,
}

#[derive(Debug, Deserialize)]
pub struct RestoreSessionRequest {
    pub target_message_id: String,
    #[serde(default)]
    pub restore_files: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_patch_message_request_deserialization() {
        let json = r#"{"content":"Hello, world!"}"#;
        let req: PatchMessageRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.content, "Hello, world!");
    }

    #[test]
    fn test_patch_message_request_debug() {
        let req = PatchMessageRequest {
            content: "Test message".to_string(),
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("PatchMessageRequest"));
    }

    #[test]
    fn test_truncate_request_after_last_user() {
        let json = r#"{"mode":"after_last_user"}"#;
        let req: TruncateRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, TruncateRequest::AfterLastUser));
    }

    #[test]
    fn test_truncate_request_error_retry() {
        let json = r#"{"mode":"error_retry"}"#;
        let req: TruncateRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, TruncateRequest::ErrorRetry));
    }

    #[test]
    fn test_truncate_request_debug() {
        let req = TruncateRequest::AfterLastUser;
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("AfterLastUser"));
    }

    #[test]
    fn test_restore_session_request_basic() {
        let json = r#"{"target_message_id":"msg-123"}"#;
        let req: RestoreSessionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.target_message_id, "msg-123");
        assert!(!req.restore_files); // default false
    }

    #[test]
    fn test_restore_session_request_with_files() {
        let json = r#"{"target_message_id":"msg-456","restore_files":true}"#;
        let req: RestoreSessionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.target_message_id, "msg-456");
        assert!(req.restore_files);
    }

    #[test]
    fn test_restore_session_request_debug() {
        let req = RestoreSessionRequest {
            target_message_id: "test-id".to_string(),
            restore_files: true,
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("RestoreSessionRequest"));
    }
}
