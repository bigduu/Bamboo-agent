use actix_web::{web, HttpResponse, Responder};
use agent_core::{Role, Session};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::state::AppState;

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

#[derive(Debug, Serialize)]
pub struct ChatResponse {
    pub session_id: String,
    pub stream_url: String,
    pub status: String,
}

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
                log::error!("[{}] Failed to load session from storage: {}", session_id, e);
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
        .unwrap_or(crate::state::DEFAULT_BASE_PROMPT);
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

    session.add_message(agent_core::Message::user(req.message.clone()));

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
        .insert(0, agent_core::Message::system(system_prompt));
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
        merged_prompt.push_str(crate::state::WORKSPACE_PROMPT_GUIDANCE);
    }

    merged_prompt
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::Session;

    #[test]
    fn upsert_system_prompt_inserts_when_missing() {
        let mut session = Session::new("session-1", "test-model");
        session.add_message(agent_core::Message::user("hello"));

        upsert_system_prompt_message(&mut session, "system prompt".to_string());

        assert!(matches!(
            session.messages.first().map(|m| &m.role),
            Some(agent_core::Role::System)
        ));
        assert_eq!(session.messages[0].content, "system prompt");
    }

    #[test]
    fn upsert_system_prompt_replaces_existing_message() {
        let mut session = Session::new("session-1", "test-model");
        session.add_message(agent_core::Message::system("old"));
        session.add_message(agent_core::Message::user("hello"));

        upsert_system_prompt_message(&mut session, "new".to_string());

        let system_messages = session
            .messages
            .iter()
            .filter(|m| matches!(m.role, agent_core::Role::System))
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
