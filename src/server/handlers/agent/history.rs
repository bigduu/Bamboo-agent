//! Session history API handler.
//!
//! This module provides the HTTP endpoint for retrieving chat session history.

use actix_web::{web, HttpResponse, Responder};

use crate::server::app_state::AppState;

/// Retrieve message history for a chat session.
///
/// # HTTP Method
///
/// `GET /api/v1/sessions/{session_id}/history`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier
///
/// # Response
///
/// Returns a JSON object containing the session ID and message history.
///
/// # Response Format
///
/// ```json
/// {
///   "session_id": "session-123",
///   "messages": []
/// }
/// ```
///
/// # Note
///
/// Currently returns an empty messages array. Full history retrieval
/// is planned for a future release.
///
/// # Example
///
/// ```bash
/// curl http://localhost:9562/api/v1/sessions/session-123/history
/// ```
pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let session_id = path.into_inner();

    // When an agent runner is active the in-memory session cache (`state.sessions`)
    // may lag behind disk because the loop works with a local `&mut Session` and only
    // writes back to the cache after `run_agent_loop` returns.  The agent *does* persist
    // to disk after significant changes (ask_user, compaction, finalize), so reading
    // from disk gives the frontend the freshest snapshot during execution.
    let runner_active = {
        let runners = state.agent_runners.read().await;
        runners
            .get(&session_id)
            .map_or(false, |r| r.completed_at.is_none())
    };

    let mut session = if runner_active {
        // Prefer disk – the agent loop may have persisted messages that
        // are not yet in the memory cache.
        match state.storage.load_session(&session_id).await {
            Ok(Some(s)) => Some(s),
            Ok(None) => {
                // Fallback to memory (shouldn't happen but be defensive).
                let sessions = state.sessions.read().await;
                sessions.get(&session_id).cloned()
            }
            Err(e) => {
                tracing::warn!(
                    "[{}] Disk read failed during active execution, falling back to memory: {}",
                    session_id,
                    e
                );
                let sessions = state.sessions.read().await;
                sessions.get(&session_id).cloned()
            }
        }
    } else {
        // No active runner – memory cache is authoritative.
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    if session.is_none() {
        match state.storage.load_session(&session_id).await {
            Ok(Some(s)) => session = Some(s),
            Ok(None) => {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": session_id
                }));
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to load session: {e}"),
                    "session_id": session_id
                }));
            }
        }
    }

    let Some(session) = session else {
        return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Session load unexpectedly returned no data",
            "session_id": session_id
        }));
    };
    HttpResponse::Ok().json(serde_json::json!({
        "session_id": session_id,
        "messages": session.messages,
        "compression_events": session.compression_events
    }))
}
