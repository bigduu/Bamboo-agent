//! Message management endpoints (delete/truncate).
//!
//! These endpoints mutate a session's persisted message history.

use actix_web::{web, HttpResponse, Result};
use chrono::Utc;
use serde::Deserialize;

use crate::agent::core::agent::Role;
use crate::server::app_state::{AgentStatus, AppState};

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TruncateRequest {
    /// Truncate all messages *after* the last user message.
    ///
    /// This is useful for "retry/regenerate" flows: keep the last user message
    /// but drop any assistant/tool tail so `POST /execute/{session_id}` can run again.
    AfterLastUser,
}

/// `POST /api/v1/sessions/{session_id}/messages/truncate`
pub async fn truncate_messages(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<TruncateRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    // Avoid corrupting history while the agent is running.
    {
        let runners = state.agent_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": "Session is currently running",
                    "session_id": session_id,
                })));
            }
        }
    }

    let Some(mut session) = state.storage.load_session(&session_id).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to load session: {e}"))
    })?
    else {
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
                .rposition(|m| matches!(m.role, Role::User));

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
        session.updated_at = Utc::now();

        state.storage.save_session(&session).await.map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to save session: {e}"))
        })?;

        // Best-effort update in-memory cache too.
        {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.clone(), session);
        }
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "messages_removed": removed,
        "message_count": new_len,
    })))
}

/// `DELETE /api/v1/sessions/{session_id}/messages/{message_id}`
pub async fn delete_message(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let (session_id, message_id) = path.into_inner();

    // Avoid corrupting history while the agent is running.
    {
        let runners = state.agent_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": "Session is currently running",
                    "session_id": session_id,
                })));
            }
        }
    }

    let Some(mut session) = state.storage.load_session(&session_id).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to load session: {e}"))
    })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let before = session.messages.len();
    session.messages.retain(|m| m.id != message_id);
    let after = session.messages.len();

    if before == after {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found",
            "session_id": session_id,
            "message_id": message_id,
        })));
    }

    // Deleting history invalidates derived context state.
    session.token_usage = None;
    session.conversation_summary = None;
    session.updated_at = Utc::now();

    state.storage.save_session(&session).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to save session: {e}"))
    })?;

    // Best-effort update in-memory cache too.
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "message_id": message_id,
        "message_count": after,
    })))
}
