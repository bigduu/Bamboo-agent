use actix_web::{web, HttpResponse, Result};

use super::shared::{
    clear_derived_context_state, ensure_session_not_running, load_session_or_404,
    save_and_cache_session,
};
use super::types::TruncateRequest;
use crate::app_state::AppState;
use bamboo_engine::session_app::execute::{clear_startup_handoff, mark_startup_handoff};
use bamboo_engine::session_app::truncation::{
    sanitize_malformed_tool_chains, truncate_after_last_user, truncate_for_unresolved_tool_calls,
    unresolved_tool_call_ids,
};

#[path = "truncate_retry.rs"]
mod retry;

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
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    };

    if matches!(&*req, TruncateRequest::ErrorRetry)
        && session.kind == bamboo_domain::SessionKind::Root
        && session.authority_identity.is_ordinary()
    {
        return retry::prepare(&state, session).await;
    }

    let (removed, should_persist) = match req.into_inner() {
        TruncateRequest::AfterLastUser => {
            let Some(removed) = truncate_after_last_user(&mut session) else {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value("No user message found to truncate after"),
                    "session_id": session_id
                })));
            };

            let cleared_pending_flag = session.metadata.remove(RETRY_RESUME_PENDING_KEY).is_some();
            let cleared_reason_flag = session.metadata.remove(RETRY_RESUME_REASON_KEY).is_some();
            let cleared_retry_flags = cleared_pending_flag || cleared_reason_flag;
            if cleared_retry_flags {
                clear_startup_handoff(&mut session);
            }
            if removed > 0 {
                clear_derived_context_state(&mut session);
            }
            (removed, removed > 0 || cleared_retry_flags)
        }
        TruncateRequest::ErrorRetry => match prepare_error_retry(&mut session) {
            Ok(prepared) => prepared,
            Err(message) => {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value(message), "session_id": session_id,
                })))
            }
        },
    };

    let new_len = session.messages.len();
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

// Shared transformation only. An activated Root must commit it through its
// exact Actor writer; deriving another run's input or tool grant is forbidden.
fn prepare_error_retry(
    session: &mut bamboo_agent_core::Session,
) -> std::result::Result<(usize, bool), &'static str> {
    let session_id = session.id.clone();
    let (removed, should_persist) = {
        let unresolved_before = unresolved_tool_call_ids(&session.messages);
        if unresolved_before.is_empty() {
            session
                .metadata
                .insert(RETRY_RESUME_PENDING_KEY.to_string(), "true".to_string());
            session.metadata.insert(
                RETRY_RESUME_REASON_KEY.to_string(),
                "error_retry".to_string(),
            );
            mark_startup_handoff(session);
            (0, true)
        } else {
            let removed_via_sanitization = sanitize_malformed_tool_chains(session);
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
                mark_startup_handoff(session);
                (removed_via_sanitization, true)
            } else {
                tracing::warn!(
                    "[{}] error_retry unresolved tool calls remain after sanitization (before={}, after={}); falling back to truncation",
                    session_id,
                    unresolved_before.len(),
                    unresolved_after_sanitization.len()
                );
                let Some(removed) = truncate_for_unresolved_tool_calls(session) else {
                    return Err(
                        "Found unresolved tool calls without a safe active user anchor to retry from",
                    );
                };

                let cleared_pending_flag =
                    session.metadata.remove(RETRY_RESUME_PENDING_KEY).is_some();
                let cleared_reason_flag =
                    session.metadata.remove(RETRY_RESUME_REASON_KEY).is_some();
                let cleared_retry_flags = cleared_pending_flag || cleared_reason_flag;
                if cleared_retry_flags {
                    clear_startup_handoff(session);
                }

                (removed, removed > 0 || cleared_retry_flags)
            }
        }
    };
    if removed > 0 {
        if session.messages.iter().any(|message| message.compressed) {
            // Repairing active history invalidates request state while the
            // archived messages and their summaries still describe the same prefix.
            session.token_usage = None;
            session.metadata.remove("responses.previous_response_id");
            session.reset_model_context_epoch(
                bamboo_domain::ModelContextResetReason::ExplicitHistoryRewrite,
            );
        } else {
            clear_derived_context_state(session);
        }
    }
    Ok((removed, should_persist))
}
