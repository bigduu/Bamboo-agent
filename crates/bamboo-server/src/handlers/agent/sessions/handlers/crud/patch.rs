use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

use super::super::super::types::PatchSessionRequest;
use super::query::get_session;

/// `PATCH /api/v1/sessions/{session_id}`
pub async fn patch_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<PatchSessionRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let Some(mut session) = state
        .storage
        .load_session(&session_id)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to load session: {error}"))
        })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    if let Some(title) = req.title.as_ref() {
        session.title = title.trim().to_string();
    }
    if let Some(pinned) = req.pinned {
        session.pinned = pinned;
    }
    if let Some(model) = req.model.as_ref() {
        let trimmed = model.trim();
        if !trimmed.is_empty() {
            session.model = trimmed.to_string();
        }
    }
    if req.clear_reasoning_effort.unwrap_or(false) {
        session.reasoning_effort = None;
    } else if let Some(reasoning_effort) = req.reasoning_effort {
        session.reasoning_effort = Some(reasoning_effort);
    }
    session.updated_at = chrono::Utc::now();

    state
        .storage
        .save_session(&session)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to save session: {error}"))
        })?;

    // Update in-memory cache too.
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    // Return updated summary (from index).
    get_session(state, web::Path::from(session_id)).await
}
