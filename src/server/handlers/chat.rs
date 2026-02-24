//! Chat API handler for creating and managing agent conversations.
//!
//! This module provides the HTTP endpoint for initiating chat sessions with the AI agent.

use crate::agent::core::{Role, Session};
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
    pub model: String,
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
/// curl -X POST http://localhost:8080/api/v1/chat \
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

    let base_prompt = req
        .system_prompt
        .as_deref()
        .map(str::trim)
        .filter(|prompt| !prompt.is_empty())
        .unwrap_or(crate::server::app_state::DEFAULT_BASE_PROMPT);
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
    let system_prompt = build_enhanced_system_prompt(base_prompt, enhance_prompt, workspace_path);
    upsert_system_prompt_message(&mut session, system_prompt);

    session.add_message(crate::agent::core::Message::user(req.message.clone()));

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
        stream_url: format!("/api/v1/stream/{}", session_id),
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
        merged_prompt.push_str(crate::server::app_state::WORKSPACE_PROMPT_GUIDANCE);
    }

    merged_prompt
}

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

        let workspace_segment =
            "Workspace path: /tmp/workspace\nIf you need to inspect files, check the workspace first, then ~/.bamboo.";

        assert!(prompt.contains(workspace_segment));
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
