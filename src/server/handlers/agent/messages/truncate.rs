use actix_web::{web, HttpResponse, Result};

use super::shared::{ensure_session_not_running, load_session_or_404, save_and_cache_session};
use super::types::TruncateRequest;
use crate::agent::core::agent::Role;
use crate::server::app_state::AppState;

const RETRY_RESUME_PENDING_KEY: &str = "retry_resume_pending";
const RETRY_RESUME_REASON_KEY: &str = "retry_resume_reason";

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

    let (removed, new_len, should_clear_derived_state, should_persist) = match req.into_inner() {
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
            if removed > 0 {
                session.messages.truncate(keep_len);
            }

            let cleared_pending_flag = session.metadata.remove(RETRY_RESUME_PENDING_KEY).is_some();
            let cleared_reason_flag = session.metadata.remove(RETRY_RESUME_REASON_KEY).is_some();
            let cleared_retry_flags = cleared_pending_flag || cleared_reason_flag;
            (
                removed,
                session.messages.len(),
                removed > 0,
                removed > 0 || cleared_retry_flags,
            )
        }
        TruncateRequest::ErrorRetry => {
            session
                .metadata
                .insert(RETRY_RESUME_PENDING_KEY.to_string(), "true".to_string());
            session.metadata.insert(
                RETRY_RESUME_REASON_KEY.to_string(),
                "error_retry".to_string(),
            );
            (0, session.messages.len(), false, true)
        }
    };

    if should_clear_derived_state {
        // Truncation invalidates derived context state.
        session.token_usage = None;
        session.conversation_summary = None;
    }

    if should_persist {
        save_and_cache_session(&state, &session_id, session).await?;
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "messages_removed": removed,
        "message_count": new_len,
    })))
}
