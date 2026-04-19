use actix_web::{web, HttpResponse, Responder};

use super::{ChatRequest, ChatResponse};
use crate::app_state::AppState;

mod images;
mod request;

// Sync runtime workspace so tools can resolve the working directory.
fn sync_runtime_workspace(session_id: &str, workspace_path: Option<&str>) {
    let preferred = workspace_path
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .and_then(|path| std::fs::canonicalize(&path).ok().or(Some(path)))
        .filter(|path| path.is_dir());
    let _ = bamboo_tools::tools::workspace_state::ensure_session_workspace(session_id, preferred);
}

#[cfg(test)]
mod tests;

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
pub async fn handler(state: web::Data<AppState>, req: web::Json<ChatRequest>) -> impl Responder {
    let session_id = request::resolve_session_id(req.session_id.as_deref());
    let model = match request::validate_and_normalize_model(req.model.as_str()) {
        Ok(model) => model,
        Err(response) => return response,
    };

    let global_default_prompt =
        crate::prompt_defaults::read_global_default_system_prompt_template();
    let builtin_fallback_prompt = crate::app_state::DEFAULT_BASE_PROMPT;

    let workspace_path = request::optional_non_empty(req.workspace_path.as_deref());
    let data_dir = Some(state.app_data_dir.clone());

    // Sync runtime workspace early so the directory exists before prompt building.
    sync_runtime_workspace(
        &session_id,
        workspace_path.map(str::trim).filter(|s| !s.is_empty()),
    );

    let input = crate::session_app::types::ChatTurnInput {
        session_id: session_id.clone(),
        model: model.clone(),
        message: req.message.clone(),
        system_prompt: request::optional_non_empty(req.system_prompt.as_deref()).map(String::from),
        enhance_prompt: request::optional_non_empty(req.enhance_prompt.as_deref())
            .map(String::from),
        workspace_path: workspace_path.map(String::from),
        selected_skill_ids: req.selected_skill_ids.clone(),
        copilot_conclusion_with_options_enhancement_enabled: req
            .copilot_conclusion_with_options_enhancement_enabled,
        data_dir,
    };

    let mut session = match crate::session_app::chat::prepare_chat_turn(
        state.as_ref(),
        input,
        global_default_prompt.as_str(),
        builtin_fallback_prompt,
    )
    .await
    {
        Ok(session) => session,
        Err(error) => {
            tracing::error!("Chat turn preparation failed: {error}");
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to prepare chat: {error}")
            }));
        }
    };

    // Image handling stays in the handler layer (depends on AppState attachment reader).
    if let Err(response) =
        images::append_user_message(&state, &mut session, &req.message, req.images.as_deref()).await
    {
        return response;
    }

    // Re-save to persist image attachments (if any).
    state.save_and_cache_session(&session).await;

    HttpResponse::Created().json(ChatResponse {
        session_id: session_id.clone(),
        stream_url: format!("/api/v1/events/{}", session_id),
        status: "streaming".to_string(),
    })
}

// Note: image attachments are stored on disk in SessionStoreV2, and message parts
// use `bamboo-attachment://<session_id>/<attachment_id>` references.
