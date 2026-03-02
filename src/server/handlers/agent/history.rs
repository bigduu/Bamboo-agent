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

    // Try memory first.
    let mut session = {
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

    let session = session.unwrap();
    HttpResponse::Ok().json(serde_json::json!({
        "session_id": session_id,
        "messages": session.messages
    }))
}
