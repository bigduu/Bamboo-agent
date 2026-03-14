use serde::{Deserialize, Serialize};

/// Request payload for creating a new chat message.
///
/// # Fields
///
/// * `message` - The user's message content
/// * `session_id` - Optional session ID. If not provided, a new UUID will be generated
/// * `system_prompt` - Optional custom system prompt. If empty, uses the default
/// * `enhance_prompt` - Optional additional prompt instructions appended to the system prompt
/// * `workspace_path` - Optional workspace path to include in the system prompt
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
    pub workspace_path: Option<String>,
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
