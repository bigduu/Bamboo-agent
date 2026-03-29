use actix_web::{web, HttpResponse, Result};

use super::shared::{
    clear_derived_context_state, ensure_session_not_running, load_session_or_404,
    save_and_cache_session,
};
use crate::server::app_state::AppState;

/// `DELETE /api/v1/sessions/{session_id}/messages/{message_id}`
pub async fn delete_message(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let (session_id, message_id) = path.into_inner();

    if let Some(response) = ensure_session_not_running(&state, &session_id).await {
        return Ok(response);
    }

    let Some(mut session) = load_session_or_404(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let before = session.messages.len();
    session.messages.retain(|message| message.id != message_id);
    let after = session.messages.len();

    if before == after {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found",
            "session_id": session_id,
            "message_id": message_id,
        })));
    }

    // Deleting history invalidates derived context state.
    clear_derived_context_state(&mut session);
    save_and_cache_session(&state, &session_id, session).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "message_id": message_id,
        "message_count": after,
    })))
}
