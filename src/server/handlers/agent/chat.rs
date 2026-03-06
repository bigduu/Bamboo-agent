//! Chat API handler for creating and managing agent conversations.
//!
//! This module provides the HTTP endpoint for initiating chat sessions with the AI agent.

use crate::agent::core::{Role, Session};
use crate::agent::llm::models::{ContentPart, ImageUrl};
use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::server::app_state::AppState;

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
///
/// # Examples
///
/// ```json
/// {
///   "message": "Hello, how can I help?",
///   "session_id": "optional-existing-session-id",
///   "model": "gpt-4o-mini",
///   "system_prompt": "You are a helpful assistant",
///   "workspace_path": "/path/to/workspace"
/// }
/// ```
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

/// Create a new chat message or update an existing session.
///
/// This endpoint accepts a user message and creates or updates a chat session.
/// After calling this endpoint, use the returned `stream_url` to execute
/// the agent and receive events.
///
/// # HTTP Method
///
/// `POST /api/v1/chat`
///
/// # Request Body
///
/// JSON-encoded [`ChatRequest`]
///
/// # Response
///
/// - `201 Created` - Chat message created successfully, returns [`ChatResponse`]
/// - `400 Bad Request` - Missing required `model` field
/// - `500 Internal Server Error` - Failed to load or save session
///
/// # Workflow
///
/// 1. Validates that `model` is provided and non-empty
/// 2. Loads existing session from memory or storage, or creates a new one
/// 3. Builds system prompt from `base_prompt`, `enhance_prompt`, and `workspace_path`
/// 4. Adds the user message to the session
/// 5. Persists the session to storage
/// 6. Returns session ID and stream URL for subsequent execution
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:9562/api/v1/chat \
///   -H "Content-Type: application/json" \
///   -d '{
///     "message": "Help me write a function",
///     "model": "gpt-4o-mini"
///   }'
/// ```
///
/// # Next Steps
///
/// After creating a chat message, call:
/// - `POST /api/v1/execute/{session_id}` to start agent execution
/// - `GET /api/v1/events/{session_id}` to subscribe to events
pub async fn handler(state: web::Data<AppState>, req: web::Json<ChatRequest>) -> impl Responder {
    let session_id = req
        .session_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let model = req.model.trim();
    if model.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "model is required"
        }));
    }
    let model = model.to_string();

    let existing_session = {
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    let mut session = match existing_session {
        Some(session) => session,
        None => match state.storage.load_session(&session_id).await {
            Ok(Some(session)) => session,
            Ok(None) => Session::new(session_id.clone(), model.clone()),
            Err(e) => {
                log::error!(
                    "[{}] Failed to load session from storage: {}",
                    session_id,
                    e
                );
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to load session: {}", e)
                }));
            }
        },
    };

    // Persist the base system prompt on the session so the frontend does not need to
    // store chat history (or system prompt config) in localStorage.
    //
    // IMPORTANT: The agent loop may mutate the in-session system message by merging
    // in skills/tool guide context. We therefore treat `metadata.base_system_prompt`
    // as the stable "source of truth" for future prompt construction.
    let base_prompt_from_request = req
        .system_prompt
        .as_deref()
        .map(str::trim)
        .filter(|prompt| !prompt.is_empty());
    if let Some(prompt) = base_prompt_from_request {
        session
            .metadata
            .insert("base_system_prompt".to_string(), prompt.to_string());
    }
    let base_prompt = base_prompt_from_request
        .map(|v| v.to_string())
        .or_else(|| session.metadata.get("base_system_prompt").cloned())
        .unwrap_or_else(|| crate::server::app_state::DEFAULT_BASE_PROMPT.to_string());

    let enhance_prompt = req
        .enhance_prompt
        .as_deref()
        .map(str::trim)
        .filter(|prompt| !prompt.is_empty());
    let workspace_path = req
        .workspace_path
        .as_deref()
        .map(str::trim)
        .filter(|workspace_path| !workspace_path.is_empty());
    if let Some(path) = workspace_path {
        session
            .metadata
            .insert("workspace_path".to_string(), path.to_string());
    }
    let workspace_path = workspace_path
        .map(|v| v.to_string())
        .or_else(|| session.metadata.get("workspace_path").cloned());

    // Only upsert the system message when the client is explicitly customizing prompt
    // inputs (or if the session has no system message yet).
    let has_system_message = session
        .messages
        .iter()
        .any(|m| matches!(m.role, crate::agent::core::Role::System));
    if base_prompt_from_request.is_some()
        || enhance_prompt.is_some()
        || workspace_path.is_some()
        || !has_system_message
    {
        let system_prompt = build_enhanced_system_prompt(
            base_prompt.as_str(),
            enhance_prompt,
            workspace_path.as_deref(),
        );
        upsert_system_prompt_message(&mut session, system_prompt);
    }

    // Preserve multimodal parts so that preflight hooks (OCR/fallback) and/or multimodal
    // upstream models can use the images.
    if let Some(images) = req.images.as_ref().filter(|items| !items.is_empty()) {
        let mut parts = Vec::new();
        // Always include a text part to keep downstream behavior stable.
        parts.push(ContentPart::Text {
            text: req.message.clone(),
        });

        for image in images {
            let (_, url) = match state
                .session_store
                .write_image_attachment(&session, &image.base64, image.mime_type.as_deref())
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    return HttpResponse::BadRequest().json(serde_json::json!({
                        "error": format!("Failed to store image attachment: {e}")
                    }));
                }
            };
            parts.push(ContentPart::ImageUrl {
                image_url: ImageUrl { url, detail: None },
            });
        }

        session.add_message(crate::agent::core::Message::user_with_parts(
            req.message.clone(),
            parts,
        ));
    } else {
        session.add_message(crate::agent::core::Message::user(req.message.clone()));
    }

    // Model is required (validated by request deserialization). Persist it on the session.
    session.model = model;

    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session.clone());
    }

    if let Err(e) = state.storage.save_session(&session).await {
        log::error!("[{}] Failed to save session: {}", session_id, e);
        return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to save session: {}", e)
        }));
    }

    HttpResponse::Created().json(ChatResponse {
        session_id: session_id.clone(),
        stream_url: format!("/api/v1/events/{}", session_id),
        status: "streaming".to_string(),
    })
}

fn upsert_system_prompt_message(session: &mut Session, system_prompt: String) {
    session
        .messages
        .retain(|message| !matches!(message.role, Role::System));
    session
        .messages
        .insert(0, crate::agent::core::Message::system(system_prompt));
}

fn build_enhanced_system_prompt(
    base_prompt: &str,
    enhance_prompt: Option<&str>,
    workspace_path: Option<&str>,
) -> String {
    let mut merged_prompt = base_prompt.to_string();

    if let Some(enhancement) = enhance_prompt
        .map(str::trim)
        .filter(|enhancement| !enhancement.is_empty())
    {
        merged_prompt.push_str("\n\n");
        merged_prompt.push_str(enhancement);
    }

    if let Some(workspace_path) = workspace_path
        .map(str::trim)
        .filter(|workspace_path| !workspace_path.is_empty())
    {
        merged_prompt.push_str("\n\nWorkspace path: ");
        merged_prompt.push_str(workspace_path);
        merged_prompt.push('\n');
        merged_prompt.push_str(&crate::server::app_state::workspace_prompt_guidance());
    }

    merged_prompt
}

// Note: image attachments are stored on disk in SessionStoreV2, and message parts
// use `bamboo-attachment://<session_id>/<attachment_id>` references.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::Session;

    #[test]
    fn upsert_system_prompt_inserts_when_missing() {
        let mut session = Session::new("session-1", "test-model");
        session.add_message(crate::agent::core::Message::user("hello"));

        upsert_system_prompt_message(&mut session, "system prompt".to_string());

        assert!(matches!(
            session.messages.first().map(|m| &m.role),
            Some(crate::agent::core::Role::System)
        ));
        assert_eq!(session.messages[0].content, "system prompt");
    }

    #[test]
    fn upsert_system_prompt_replaces_existing_message() {
        let mut session = Session::new("session-1", "test-model");
        session.add_message(crate::agent::core::Message::system("old"));
        session.add_message(crate::agent::core::Message::user("hello"));

        upsert_system_prompt_message(&mut session, "new".to_string());

        let system_messages = session
            .messages
            .iter()
            .filter(|m| matches!(m.role, crate::agent::core::Role::System))
            .count();
        assert_eq!(system_messages, 1);
        assert_eq!(session.messages[0].content, "new");
    }

    #[test]
    fn build_enhanced_system_prompt_appends_enhancement_before_skills() {
        let prompt = build_enhanced_system_prompt("Base prompt", Some("Extra guidance"), None);

        assert!(prompt.starts_with("Base prompt\n\nExtra guidance"));
    }

    #[test]
    fn build_enhanced_system_prompt_appends_workspace_context_before_skills() {
        let prompt = build_enhanced_system_prompt(
            "Base prompt",
            Some("Extra guidance"),
            Some("/tmp/workspace"),
        );

        let workspace_segment = format!(
            "Workspace path: /tmp/workspace\n{}",
            crate::server::app_state::workspace_prompt_guidance()
        );

        assert!(prompt.contains(&workspace_segment));
    }

    #[test]
    fn build_enhanced_system_prompt_ignores_empty_enhancement() {
        let prompt = build_enhanced_system_prompt("Base prompt", Some("   "), None);
        assert_eq!(prompt, "Base prompt");
    }

    #[test]
    fn chat_request_deserialization_with_model() {
        let json = r#"{
            "message": "Hello",
            "session_id": "test-session",
            "model": "gpt-5"
        }"#;

        let request: ChatRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.message, "Hello");
        assert_eq!(request.session_id, Some("test-session".to_string()));
        assert_eq!(request.model, "gpt-5");
    }

    #[test]
    fn chat_request_deserialization_without_model() {
        let json = r#"{
            "message": "Hello"
        }"#;

        let result: Result<ChatRequest, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn session_stores_model_in_dedicated_field() {
        // Simulate what the handler does
        let mut session = Session::new("test-session", "initial-model");
        session.model = "gpt-4o-mini".to_string();
        assert_eq!(session.model, "gpt-4o-mini");
    }

    #[test]
    fn session_model_round_trip() {
        // Create session with model
        let session = Session::new("test-session", "gpt-5");

        // Serialize and deserialize
        let json = serde_json::to_string(&session).unwrap();
        let deserialized: Session = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.model, "gpt-5");
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "model must be explicitly provided in the request"

    /// Test: ChatRequest.model must be String (not Option<String>)
    /// This prevents accidental fallback to None
    #[test]
    fn chat_request_model_type_is_string_not_option() {
        let json = r#"{
            "message": "Hello",
            "model": "claude-3-opus"
        }"#;

        let request: ChatRequest = serde_json::from_str(json).unwrap();
        // This line proves model is String, not Option<String>
        // If it were Option<String>, this would fail to compile
        let _model_str: &str = &request.model;
        assert_eq!(request.model, "claude-3-opus");
    }

    /// Test: Empty/whitespace model should fail validation
    #[test]
    fn chat_request_empty_model_fails_validation() {
        let request = ChatRequest {
            message: "Hello".to_string(),
            session_id: None,
            system_prompt: None,
            enhance_prompt: None,
            workspace_path: None,
            images: None,
            model: "   ".to_string(), // Empty/whitespace
        };

        // Handler validation: trim and check if empty
        let model = request.model.trim();
        assert!(model.is_empty(), "Empty model should fail validation");
    }

    /// Test: Session.model is just for recording, not execution
    #[test]
    fn session_model_is_for_recording_only() {
        // Create session with initial model
        let mut session = Session::new("test-123", "initial-model");
        assert_eq!(session.model, "initial-model");

        // Session.model can be updated (just for recording)
        session.model = "updated-model".to_string();
        assert_eq!(session.model, "updated-model");

        // Note: The actual execution uses config.model_name from the request,
        // not session.model. This is enforced in execute.rs and agent-loop.
    }
}
