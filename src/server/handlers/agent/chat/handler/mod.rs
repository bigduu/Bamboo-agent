use actix_web::{web, HttpResponse, Responder};

use super::prompt::{build_enhanced_system_prompt_with_profile, upsert_system_prompt_message};
use super::{ChatRequest, ChatResponse};
use crate::server::app_state::AppState;

mod images;
mod request;
mod session;

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

    let mut session = match session::load_or_create_session(&state, &session_id, &model).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let global_default_prompt =
        crate::server::prompt_defaults::read_global_default_system_prompt_template();

    let base_prompt = session::resolve_base_prompt(
        &mut session,
        request::optional_non_empty(req.system_prompt.as_deref()),
        global_default_prompt.as_str(),
    );
    let enhance_prompt = session::resolve_enhance_prompt(
        &mut session,
        request::optional_non_empty(req.enhance_prompt.as_deref()),
    );
    session::resolve_copilot_ask_user_enhancement_enabled(
        &mut session,
        req.copilot_ask_user_enhancement_enabled,
    );
    let workspace_path = session::resolve_workspace_path(
        &mut session,
        request::optional_non_empty(req.workspace_path.as_deref()),
    );
    session::resolve_selected_skill_ids(
        &mut session,
        req.selected_skill_ids.as_deref(),
        req.message.as_str(),
    );
    session::clear_skill_runtime_state(&mut session);

    // Always refresh the persisted system prompt so existing sessions don't
    // keep stale tool instructions after backend upgrades.
    let (system_prompt, prompt_profile) = build_enhanced_system_prompt_with_profile(
        base_prompt.as_str(),
        enhance_prompt.as_deref(),
        workspace_path.as_deref(),
    );
    session.metadata.insert(
        "prompt_composer_version".to_string(),
        prompt_profile.version.to_string(),
    );
    session.metadata.insert(
        "prompt_fingerprint".to_string(),
        prompt_profile.fingerprint.clone(),
    );
    session.metadata.insert(
        "prompt_component_flags".to_string(),
        prompt_profile.component_flags_value(),
    );
    session.metadata.insert(
        "prompt_component_lengths".to_string(),
        prompt_profile.component_lengths_value(),
    );
    upsert_system_prompt_message(&mut session, system_prompt);

    if let Err(response) =
        images::append_user_message(&state, &mut session, &req.message, req.images.as_deref()).await
    {
        return response;
    }

    // Model is required (validated by request deserialization). Persist it on the session.
    session.model = model;

    if let Err(response) = session::cache_and_save_session(&state, &session_id, session).await {
        return response;
    }

    HttpResponse::Created().json(ChatResponse {
        session_id: session_id.clone(),
        stream_url: format!("/api/v1/events/{}", session_id),
        status: "streaming".to_string(),
    })
}

// Note: image attachments are stored on disk in SessionStoreV2, and message parts
// use `bamboo-attachment://<session_id>/<attachment_id>` references.
