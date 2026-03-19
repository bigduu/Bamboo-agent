use actix_web::{web, HttpResponse};

use crate::agent::core::Session;
use crate::server::app_state::AppState;

pub(super) async fn load_session(
    state: &web::Data<AppState>,
    session_id: &str,
) -> Result<Session, HttpResponse> {
    let mut session = {
        let sessions = state.sessions.read().await;
        sessions.get(session_id).cloned()
    };

    if session.is_none() {
        match state.storage.load_session(session_id).await {
            Ok(Some(restored)) => session = Some(restored),
            Ok(None) => {
                tracing::warn!("[{}] Session not found", session_id);
                return Err(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": session_id
                })));
            }
            Err(error) => {
                tracing::error!("[{}] Failed to load session: {}", session_id, error);
                return Err(HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to load session: {}", error)
                })));
            }
        }
    }

    let Some(session) = session else {
        tracing::error!(
            "[{}] Session load completed without data after successful checks",
            session_id
        );
        return Err(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Session load unexpectedly returned no data",
            "session_id": session_id
        })));
    };

    Ok(session)
}
