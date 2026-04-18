use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

use super::super::super::types::{GetSessionResponse, ListSessionsResponse, SessionSummary};
use super::running::{is_session_running, running_session_ids};

/// `GET /api/v1/sessions`
pub async fn list_sessions(state: web::Data<AppState>) -> Result<HttpResponse> {
    let running = running_session_ids(&state).await;
    let entries = state.session_store.list_index_entries().await;

    Ok(HttpResponse::Ok().json(ListSessionsResponse {
        sessions: entries
            .into_iter()
            .map(|entry| {
                let is_running = running.contains(&entry.id);
                SessionSummary::from_entry(entry, is_running)
            })
            .collect(),
    }))
}

/// `GET /api/v1/sessions/{session_id}`
pub async fn get_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    match state.session_store.get_index_entry(&session_id).await {
        Some(entry) => {
            let is_running = is_session_running(&state, &session_id).await;
            Ok(HttpResponse::Ok().json(GetSessionResponse {
                session: SessionSummary::from_entry(entry, is_running),
            }))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        }))),
    }
}
