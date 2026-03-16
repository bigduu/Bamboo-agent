use actix_web::{web, HttpResponse, Result};

use super::shared::{ensure_session_not_running, load_session_or_404, save_and_cache_session};
use super::types::PatchMessageRequest;
use crate::agent::core::Role;
use crate::server::app_state::AppState;

/// `PATCH /api/v1/sessions/{session_id}/messages/{message_id}`
pub async fn patch_message(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    req: web::Json<PatchMessageRequest>,
) -> Result<HttpResponse> {
    let (session_id, message_id) = path.into_inner();
    let PatchMessageRequest { content } = req.into_inner();

    if content.trim().is_empty() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "content must not be empty",
            "session_id": session_id,
            "message_id": message_id,
        })));
    }

    if let Some(response) = ensure_session_not_running(&state, &session_id).await {
        return Ok(response);
    }

    let Some(mut session) = load_session_or_404(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let Some(message) = session
        .messages
        .iter_mut()
        .find(|message| message.id == message_id)
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found",
            "session_id": session_id,
            "message_id": message_id,
        })));
    };

    let has_tool_calls = message
        .tool_calls
        .as_ref()
        .map(|calls| !calls.is_empty())
        .unwrap_or(false);

    if !matches!(message.role, Role::Assistant) || has_tool_calls {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Only assistant text messages can be updated",
            "session_id": session_id,
            "message_id": message_id,
        })));
    }

    message.content = content;

    // Editing history invalidates derived context state.
    session.token_usage = None;
    session.conversation_summary = None;
    let message_count = session.messages.len();
    save_and_cache_session(&state, &session_id, session).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "message_id": message_id,
        "message_count": message_count,
    })))
}

