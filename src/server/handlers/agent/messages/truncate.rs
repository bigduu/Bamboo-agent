use actix_web::{web, HttpResponse, Result};

use super::shared::{ensure_session_not_running, load_session_or_404, save_and_cache_session};
use super::types::TruncateRequest;
use crate::agent::core::agent::Role;
use crate::server::app_state::AppState;

/// `POST /api/v1/sessions/{session_id}/messages/truncate`
pub async fn truncate_messages(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<TruncateRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    if let Some(response) = ensure_session_not_running(&state, &session_id).await {
        return Ok(response);
    }

    let Some(mut session) = load_session_or_404(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let (removed, new_len) = match req.into_inner() {
        TruncateRequest::AfterLastUser => {
            let last_user_idx = session
                .messages
                .iter()
                .rposition(|message| matches!(message.role, Role::User));

            let Some(idx) = last_user_idx else {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "No user message found to truncate after",
                    "session_id": session_id
                })));
            };

            let keep_len = idx + 1;
            let removed = session.messages.len().saturating_sub(keep_len);
            session.messages.truncate(keep_len);
            (removed, keep_len)
        }
    };

    if removed > 0 {
        // Truncation invalidates derived context state.
        session.token_usage = None;
        session.conversation_summary = None;
        save_and_cache_session(&state, &session_id, session).await?;
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "messages_removed": removed,
        "message_count": new_len,
    })))
}
