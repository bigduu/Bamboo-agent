use actix_web::{error::ErrorInternalServerError, web, HttpResponse, Result};
use chrono::Utc;

use crate::app_state::{AgentStatus, AppState};
use bamboo_agent_core::Session;

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

pub(super) fn clear_derived_context_state(session: &mut Session) {
    session.clear_derived_context_state();
}

pub(super) async fn save_and_cache_session(
    state: &web::Data<AppState>,
    session_id: &str,
    mut session: Session,
) -> Result<()> {
    session.updated_at = Utc::now();
    state
        .persistence
        .merge_save_runtime(&mut session)
        .await
        .map_err(|e| ErrorInternalServerError(format!("Failed to save session: {e}")))?;

    state.sessions.insert(
        session_id.to_string(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
    Ok(())
}
