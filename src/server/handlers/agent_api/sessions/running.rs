use actix_web::{web, HttpResponse};

use crate::server::app_state::AppState;
use crate::server::error::AppError;

/// Lists currently running Claude Code sessions.
pub async fn list_running_claude_sessions() -> Result<HttpResponse, AppError> {
    Ok(HttpResponse::Ok().json(Vec::<serde_json::Value>::new()))
}

/// Lists currently running Claude Code sessions (stateful).
pub async fn list_running_claude_sessions_stateful(
    state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let sessions = state
        .process_registry
        .get_running_claude_sessions()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    Ok(HttpResponse::Ok().json(sessions))
}
