use actix_web::{web, HttpResponse, Result};

use super::shared::{
    clear_derived_context_state, ensure_session_not_running, load_session_or_404,
    save_and_cache_session,
};
use super::types::TruncateRequest;
use crate::app_state::AppState;
use crate::session_app::truncation::{
    sanitize_malformed_tool_chains, truncate_after_last_user, truncate_for_unresolved_tool_calls,
    unresolved_tool_call_ids,
};

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
            let Some(removed) = truncate_after_last_user(&mut session) else {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "No user message found to truncate after",
                    "session_id": session_id
                })));
            };

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
            let unresolved_before = unresolved_tool_call_ids(&session.messages);
            if unresolved_before.is_empty() {
                session
                    .metadata
                    .insert(RETRY_RESUME_PENDING_KEY.to_string(), "true".to_string());
                session.metadata.insert(
                    RETRY_RESUME_REASON_KEY.to_string(),
                    "error_retry".to_string(),
                );
                (0, session.messages.len(), false, true)
            } else {
                let removed_via_sanitization = sanitize_malformed_tool_chains(&mut session);
                let unresolved_after_sanitization = unresolved_tool_call_ids(&session.messages);
                if unresolved_after_sanitization.is_empty() {
                    tracing::warn!(
                        "[{}] error_retry recovered by sanitizing malformed tool chain data: unresolved_before={}, removed_entries={}",
                        session_id,
                        unresolved_before.len(),
                        removed_via_sanitization
                    );
                    session
                        .metadata
                        .insert(RETRY_RESUME_PENDING_KEY.to_string(), "true".to_string());
                    session.metadata.insert(
                        RETRY_RESUME_REASON_KEY.to_string(),
                        "error_retry".to_string(),
                    );
                    (
                        removed_via_sanitization,
                        session.messages.len(),
                        removed_via_sanitization > 0,
                        true,
                    )
                } else {
                    tracing::warn!(
                        "[{}] error_retry unresolved tool calls remain after sanitization (before={}, after={}); falling back to truncation",
                        session_id,
                        unresolved_before.len(),
                        unresolved_after_sanitization.len()
                    );
                    let Some(removed) = truncate_for_unresolved_tool_calls(&mut session) else {
                        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                            "error": "Found unresolved tool calls but no prior user message to retry from",
                            "session_id": session_id
                        })));
                    };

                    let cleared_pending_flag =
                        session.metadata.remove(RETRY_RESUME_PENDING_KEY).is_some();
                    let cleared_reason_flag =
                        session.metadata.remove(RETRY_RESUME_REASON_KEY).is_some();
                    let cleared_retry_flags = cleared_pending_flag || cleared_reason_flag;

                    (
                        removed,
                        session.messages.len(),
                        removed > 0,
                        removed > 0 || cleared_retry_flags,
                    )
                }
            }
        }
    };

    if should_clear_derived_state {
        // Truncation invalidates derived context state.
        clear_derived_context_state(&mut session);
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
