use serde::Deserialize;

/// Request body for creating a new project.
#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    /// File system path to the project directory.
    pub path: String,
}

/// Request body for saving Claude settings.
#[derive(Debug, Deserialize)]
pub struct SaveSettingsRequest {
    /// Settings data as JSON.
    pub settings: serde_json::Value,
}

/// Request body for saving system prompt.
#[derive(Debug, Deserialize)]
pub struct SaveSystemPromptRequest {
    /// System prompt content (markdown).
    pub content: String,
}

/// Request body for executing Claude code.
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    /// Project directory path.
    pub project_path: String,
    /// User prompt to execute.
    pub prompt: String,
    /// Optional session ID to resume.
    pub session_id: Option<String>,
    /// Optional override for Claude's Anthropic base URL.
    ///
    /// If omitted, Bamboo defaults to `http://127.0.0.1:{port}/anthropic` so the
    /// Claude Code CLI talks to Bamboo's embedded Anthropic-compatible API.
    pub anthropic_base_url: Option<String>,
    /// Optional JSON schema for structured output (passed to `claude --json-schema`).
    pub json_schema: Option<String>,
    /// If omitted, defaults to `true` (skip Claude's user confirmation prompts).
    pub dangerously_skip_permissions: Option<bool>,
    /// If omitted, defaults to `true` (better streaming UX).
    pub include_partial_messages: Option<bool>,
}

/// Request body for canceling execution.
#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    /// Session ID to cancel.
    pub session_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_project_request_deserialization() {
        let json = r#"{"path":"/test/project"}"#;
        let req: CreateProjectRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.path, "/test/project");
    }

    #[test]
    fn test_create_project_request_debug() {
        let req = CreateProjectRequest {
            path: "/test".to_string(),
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("CreateProjectRequest"));
        assert!(debug_str.contains("/test"));
    }

    #[test]
    fn test_save_settings_request_deserialization() {
        let json = r#"{"settings":{"key":"value"}}"#;
        let req: SaveSettingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.settings["key"], "value");
    }

    #[test]
    fn test_save_settings_request_with_complex_json() {
        let json = r#"{"settings":{"nested":{"array":[1,2,3]}}}"#;
        let req: SaveSettingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.settings["nested"]["array"][1], 2);
    }

    #[test]
    fn test_save_settings_request_debug() {
        let req = SaveSettingsRequest {
            settings: serde_json::json!({"test": "data"}),
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("SaveSettingsRequest"));
    }

    #[test]
    fn test_save_system_prompt_request_deserialization() {
        let json = "{\"content\":\"# My Prompt\\n\\nThis is a test.\"}";
        let req: SaveSystemPromptRequest = serde_json::from_str(json).unwrap();
        assert!(req.content.contains("My Prompt"));
    }

    #[test]
    fn test_save_system_prompt_request_debug() {
        let req = SaveSystemPromptRequest {
            content: "Test prompt".to_string(),
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("SaveSystemPromptRequest"));
    }

    #[test]
    fn test_execute_request_minimal() {
        let json = r#"{"project_path":"/proj","prompt":"test"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.project_path, "/proj");
        assert_eq!(req.prompt, "test");
        assert!(req.session_id.is_none());
        assert!(req.anthropic_base_url.is_none());
        assert!(req.json_schema.is_none());
        assert!(req.dangerously_skip_permissions.is_none());
        assert!(req.include_partial_messages.is_none());
    }

    #[test]
    fn test_execute_request_with_all_fields() {
        let json = r#"{
            "project_path":"/proj",
            "prompt":"test",
            "session_id":"sess-123",
            "anthropic_base_url":"http://custom:8080",
            "json_schema":"{\"type\":\"object\"}",
            "dangerously_skip_permissions":false,
            "include_partial_messages":false
        }"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.session_id, Some("sess-123".to_string()));
        assert_eq!(
            req.anthropic_base_url,
            Some("http://custom:8080".to_string())
        );
        assert_eq!(req.json_schema, Some("{\"type\":\"object\"}".to_string()));
        assert_eq!(req.dangerously_skip_permissions, Some(false));
        assert_eq!(req.include_partial_messages, Some(false));
    }

    #[test]
    fn test_execute_request_with_session_id() {
        let json = r#"{"project_path":"/p","prompt":"hi","session_id":"abc"}"#;
        let req: ExecuteRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.session_id, Some("abc".to_string()));
    }

    #[test]
    fn test_execute_request_debug() {
        let req = ExecuteRequest {
            project_path: "/test".to_string(),
            prompt: "hello".to_string(),
            session_id: None,
            anthropic_base_url: None,
            json_schema: None,
            dangerously_skip_permissions: None,
            include_partial_messages: None,
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("ExecuteRequest"));
    }

    #[test]
    fn test_cancel_request_deserialization() {
        let json = r#"{"session_id":"sess-to-cancel"}"#;
        let req: CancelRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.session_id, "sess-to-cancel");
    }

    #[test]
    fn test_cancel_request_debug() {
        let req = CancelRequest {
            session_id: "test-id".to_string(),
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("CancelRequest"));
        assert!(debug_str.contains("test-id"));
    }
}
