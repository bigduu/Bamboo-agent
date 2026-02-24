//! Session history API handler.
//!
//! This module provides the HTTP endpoint for retrieving chat session history.

use actix_web::{web, HttpResponse, Responder};

use crate::agent::server::state::AppState;

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
/// curl http://localhost:8080/api/v1/sessions/session-123/history
/// ```
pub async fn handler(_state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "session_id": path.into_inner(),
        "messages": []
    }))
}
