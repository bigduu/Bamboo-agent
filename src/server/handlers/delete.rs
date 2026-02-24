//! Session deletion API handler.
//!
//! This module provides the HTTP endpoint for deleting chat sessions
//! and cancelling in-flight agent executions.

use actix_web::{web, HttpResponse, Result};

use crate::server::app_state::AppState;

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
/// curl -X DELETE http://localhost:8080/api/v1/sessions/session-123
/// ```
pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let deleted_from_storage = match state.storage.delete_session(&session_id).await {
        Ok(deleted) => deleted,
        Err(error) => {
            log::error!(
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
        sessions.remove(&session_id).is_some()
    };

    let cancelled_in_flight = {
        let mut tokens = state.cancel_tokens.write().await;
        if let Some(token) = tokens.remove(&session_id) {
            token.cancel();
            true
        } else {
            false
        }
    };

    if deleted_from_storage || removed_from_memory || cancelled_in_flight {
        log::info!(
            "[{}] Session deleted successfully (storage: {}, memory: {}, cancelled: {})",
            session_id,
            deleted_from_storage,
            removed_from_memory,
            cancelled_in_flight
        );
        return Ok(HttpResponse::Ok().finish());
    }

    Ok(HttpResponse::NotFound().json(serde_json::json!({
        "error": "Session not found"
    })))
}
