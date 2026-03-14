use actix_web::{error::ErrorInternalServerError, web, HttpResponse, Result};
use chrono::Utc;

use crate::agent::core::Session;
use crate::server::app_state::{AgentStatus, AppState};

pub(super) async fn ensure_session_not_running(
    state: &web::Data<AppState>,
    session_id: &str,
) -> Option<HttpResponse> {
    let runners = state.agent_runners.read().await;
    if let Some(runner) = runners.get(session_id) {
        if matches!(runner.status, AgentStatus::Running) {
            return Some(HttpResponse::Conflict().json(serde_json::json!({
                "error": "Session is currently running",
                "session_id": session_id,
            })));
        }
    }

    None
}

pub(super) async fn load_session_or_404(
    state: &web::Data<AppState>,
    session_id: &str,
) -> Result<Option<Session>> {
    state
        .storage
        .load_session(session_id)
        .await
        .map_err(|e| ErrorInternalServerError(format!("Failed to load session: {e}")))
}

pub(super) async fn save_and_cache_session(
    state: &web::Data<AppState>,
    session_id: &str,
    mut session: Session,
) -> Result<()> {
    session.updated_at = Utc::now();
    state
        .storage
        .save_session(&session)
        .await
        .map_err(|e| ErrorInternalServerError(format!("Failed to save session: {e}")))?;

    let mut sessions = state.sessions.write().await;
    sessions.insert(session_id.to_string(), session);
    Ok(())
}
