use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use crate::title_gen;

/// `POST /api/v1/sessions/{session_id}/regenerate-title`
///
/// Triggers backend auto-title regeneration regardless of current title state.
/// Returns `202 Accepted` immediately; the new title is delivered via the
/// `SessionTitleUpdated` SSE event.
pub async fn regenerate_session_title(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    // Verify session exists; 404 otherwise.
    let exists = state
        .storage
        .load_session(&session_id)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to load session: {error}"))
        })?
        .is_some();

    if !exists {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    }

    // Spawn the regen task. `web::Data::clone` is `Arc::clone`, then
    // `into_inner()` returns the underlying `Arc<AppState>`.
    title_gen::spawn_title_generation_force(state.clone().into_inner(), session_id.clone());

    // SSE will deliver the new title via `SessionTitleUpdated`; a confirmation
    // body is sufficient here.
    Ok(HttpResponse::Accepted().json(serde_json::json!({
        "status": "accepted",
        "session_id": session_id
    })))
}
