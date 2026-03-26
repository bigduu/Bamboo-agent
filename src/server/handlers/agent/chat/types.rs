use serde::{Deserialize, Serialize};

/// Request payload for creating a new chat message.
///
/// # Fields
///
/// * `message` - The user's message content
/// * `session_id` - Optional session ID. If not provided, a new UUID will be generated
/// * `system_prompt` - Optional custom system prompt. If empty, uses the default
/// * `enhance_prompt` - Optional additional prompt instructions appended to the system prompt
/// * `copilot_ask_user_enhancement_enabled` - Optional flag for enabling copilot ask-user conclusion flow
/// * `workspace_path` - Optional workspace path to include in the system prompt
/// * `selected_skill_ids` - Optional explicit skill IDs selected for this request
/// * `model` - Required model identifier (e.g., "gpt-4o-mini", "claude-3-opus")
#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub message: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub enhance_prompt: Option<String>,
    #[serde(default)]
    pub copilot_ask_user_enhancement_enabled: Option<bool>,
    #[serde(default)]
    pub workspace_path: Option<String>,
    #[serde(default)]
    pub selected_skill_ids: Option<Vec<String>>,
    /// Optional image attachments (data URLs) associated with this message.
    #[serde(default)]
    pub images: Option<Vec<ChatImage>>,
    pub model: String,
}

#[derive(Debug, Deserialize)]
pub struct ChatImage {
    pub base64: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default, rename = "type")]
    pub mime_type: Option<String>,
}

/// Response returned after successfully creating a chat message.
///
/// # Fields
///
/// * `session_id` - The session identifier for subsequent API calls
/// * `stream_url` - URL endpoint to stream agent events (SSE)
/// * `status` - Current status of the chat session
#[derive(Debug, Serialize)]
pub struct ChatResponse {
    /// Unique session identifier for this conversation
    pub session_id: String,
    /// SSE endpoint URL to receive real-time agent events
    pub stream_url: String,
    /// Current session status (e.g., "streaming")
    pub status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chat_request_deserialization_minimal() {
        let json = r#"{"message":"Hello","model":"gpt-4"}"#;
        let req: ChatRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.message, "Hello");
        assert_eq!(req.model, "gpt-4");
        assert!(req.session_id.is_none());
        assert!(req.system_prompt.is_none());
        assert!(req.copilot_ask_user_enhancement_enabled.is_none());
        assert!(req.images.is_none());
    }

    #[test]
    fn test_chat_request_deserialization_full() {
        let json = r#"{
            "message":"Hello",
            "session_id":"sess-123",
            "system_prompt":"Be helpful",
            "enhance_prompt":"Be concise",
            "copilot_ask_user_enhancement_enabled":true,
            "workspace_path":"/home/user",
            "selected_skill_ids":["pdf","skill-creator"],
            "model":"claude-3"
        }"#;
        let req: ChatRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.message, "Hello");
        assert_eq!(req.session_id, Some("sess-123".to_string()));
        assert_eq!(req.system_prompt, Some("Be helpful".to_string()));
        assert_eq!(req.enhance_prompt, Some("Be concise".to_string()));
        assert_eq!(req.copilot_ask_user_enhancement_enabled, Some(true));
        assert_eq!(req.workspace_path, Some("/home/user".to_string()));
        assert_eq!(
            req.selected_skill_ids,
            Some(vec!["pdf".to_string(), "skill-creator".to_string()])
        );
        assert_eq!(req.model, "claude-3");
    }

    #[test]
    fn test_chat_request_with_images() {
        let json = r#"{
            "message":"Check this",
            "model":"gpt-4",
            "images":[{"base64":"aGVsbG8=","name":"test.png","size":1024,"type":"image/png"}]
        }"#;
        let req: ChatRequest = serde_json::from_str(json).unwrap();
        assert!(req.images.is_some());
        let images = req.images.unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].base64, "aGVsbG8=");
        assert_eq!(images[0].name, Some("test.png".to_string()));
        assert_eq!(images[0].size, Some(1024));
        assert_eq!(images[0].mime_type, Some("image/png".to_string()));
    }

    #[test]
    fn test_chat_image_deserialization_minimal() {
        let json = r#"{"base64":"YWJj"}"#;
        let img: ChatImage = serde_json::from_str(json).unwrap();
        assert_eq!(img.base64, "YWJj");
        assert!(img.name.is_none());
        assert!(img.size.is_none());
        assert!(img.mime_type.is_none());
    }

    #[test]
    fn test_chat_request_debug() {
        let req = ChatRequest {
            message: "Test".to_string(),
            session_id: None,
            system_prompt: None,
            enhance_prompt: None,
            copilot_ask_user_enhancement_enabled: None,
            workspace_path: None,
            selected_skill_ids: None,
            images: None,
            model: "gpt-4".to_string(),
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("ChatRequest"));
        assert!(debug_str.contains("Test"));
    }

    #[test]
    fn test_chat_image_debug() {
        let img = ChatImage {
            base64: "test".to_string(),
            name: Some("image.png".to_string()),
            size: Some(2048),
            mime_type: Some("image/png".to_string()),
        };
        let debug_str = format!("{:?}", img);
        assert!(debug_str.contains("ChatImage"));
    }

    #[test]
    fn test_chat_response_serialization() {
        let resp = ChatResponse {
            session_id: "sess-456".to_string(),
            stream_url: "/stream/sess-456".to_string(),
            status: "streaming".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("sess-456"));
        assert!(json.contains("/stream/sess-456"));
        assert!(json.contains("streaming"));
    }

    #[test]
    fn test_chat_response_debug() {
        let resp = ChatResponse {
            session_id: "test".to_string(),
            stream_url: "/stream".to_string(),
            status: "active".to_string(),
        };
        let debug_str = format!("{:?}", resp);
        assert!(debug_str.contains("ChatResponse"));
    }

    #[test]
    fn test_chat_request_empty_message() {
        let json = r#"{"message":"","model":"gpt-4"}"#;
        let req: ChatRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.message, "");
    }

    #[test]
    fn test_chat_request_special_characters() {
        let json = r#"{"message":"Hello\nWorld\t!","model":"gpt-4"}"#;
        let req: ChatRequest = serde_json::from_str(json).unwrap();
        assert!(req.message.contains('\n'));
        assert!(req.message.contains('\t'));
    }
}
