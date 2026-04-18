use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

/// Get the pending question for a session (if any).
///
/// This endpoint retrieves the current pending question that the agent
/// is waiting for the user to answer.
///
/// # HTTP Method
///
/// `GET /api/v1/sessions/{session_id}/question`
pub async fn get_pending_question(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();

    let Some(session) = state.load_session_merged(&session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found"
        })));
    };

    match session.pending_question {
        Some(pending) => Ok(HttpResponse::Ok().json(serde_json::json!({
            "has_pending_question": true,
            "question": pending.question,
            "options": pending.options,
            "allow_custom": pending.allow_custom,
            "tool_call_id": pending.tool_call_id
        }))),
        None => Ok(HttpResponse::Ok().json(serde_json::json!({
            "has_pending_question": false
        }))),
    }
}
