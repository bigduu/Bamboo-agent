//! Session deletion API handler.
//!
//! This module provides the HTTP endpoint for deleting chat sessions
//! and cancelling in-flight agent executions.

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

/// Delete a chat session and cancel any running agent execution.
///
/// This endpoint removes the session from both memory and persistent storage,
/// and cancels any in-flight agent execution for that session.
///
/// # HTTP Method
///
/// `DELETE /api/v1/sessions/{session_id}`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier to delete
///
/// # Response
///
/// - `200 OK` - Session deleted successfully (no body)
/// - `404 Not Found` - Session does not exist
/// - `500 Internal Server Error` - Failed to delete from storage
///
/// # Side Effects
///
/// When a session is deleted:
/// 1. Session is removed from persistent storage (if exists)
/// 2. Session is removed from in-memory cache
/// 3. Any running agent execution is cancelled
/// 4. Associated cancellation tokens are cleaned up
///
/// # Idempotency
///
/// This endpoint is idempotent. Calling it multiple times with the same
/// session ID will return `404 Not Found` after the first successful deletion.
///
/// # Example
///
/// ```bash
/// curl -X DELETE http://localhost:9562/api/v1/sessions/session-123
/// ```
pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    // Best-effort pre-cancellation of the session (and its children if this is a root session).
    let ids_to_cancel: Vec<String> = match state.session_store.get_index_entry(&session_id).await {
        Some(entry) if matches!(entry.kind, bamboo_agent_core::SessionKind::Root) => state
            .session_store
            .list_index_entries()
            .await
            .into_iter()
            .filter(|e| e.root_session_id == session_id)
            .map(|e| e.id)
            .collect(),
        Some(_) => vec![session_id.clone()],
        None => vec![session_id.clone()],
    };

    let cancelled_runner = {
        let mut runners = state.agent_runners.write().await;
        let mut cancelled = false;
        for id in ids_to_cancel.iter() {
            if let Some(runner) = runners.remove(id) {
                runner.cancel_token.cancel();
                cancelled = true;
            }
        }
        cancelled
    };

    let deleted_from_storage = match state.storage.delete_session(&session_id).await {
        Ok(deleted) => deleted,
        Err(error) => {
            tracing::error!(
                "[{}] Failed to delete session from storage: {}",
                session_id,
                error
            );
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to delete session"
            })));
        }
    };

    let removed_from_memory = {
        let mut sessions = state.sessions.write().await;
        let mut removed = false;
        for id in ids_to_cancel.iter() {
            removed |= sessions.remove(id).is_some();
        }
        removed
    };

    {
        let mut senders = state.session_event_senders.write().await;
        for id in ids_to_cancel.iter() {
            senders.remove(id);
        }
    }

    let cancelled_in_flight = {
        let mut tokens = state.cancel_tokens.write().await;
        let mut cancelled = false;
        for id in ids_to_cancel.iter() {
            if let Some(token) = tokens.remove(id) {
                token.cancel();
                cancelled = true;
            }
        }
        cancelled
    };

    if deleted_from_storage || removed_from_memory || cancelled_in_flight || cancelled_runner {
        tracing::info!(
            "[{}] Session deleted successfully (storage: {}, memory: {}, cancelled: {}, runner_cancelled: {})",
            session_id,
            deleted_from_storage,
            removed_from_memory,
            cancelled_in_flight,
            cancelled_runner
        );
        return Ok(HttpResponse::Ok().finish());
    }

    Ok(HttpResponse::NotFound().json(serde_json::json!({
        "error": "Session not found"
    })))
}
