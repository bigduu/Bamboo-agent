//! Development-only endpoints.

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

/// `POST /api/v1/dev/reset`
///
/// Greenfield reset: deletes all sessions (including pinned/child) and resets the index.
pub async fn reset(state: web::Data<AppState>) -> Result<HttpResponse> {
    state
        .session_store
        .dev_reset()
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(format!("Reset failed: {e}")))?;

    // Clear in-memory cache too.
    {
        let mut sessions = state.sessions.write().await;
        sessions.clear();
    }
    {
        let mut runners = state.agent_runners.write().await;
        for (_, runner) in runners.drain() {
            runner.cancel_token.cancel();
        }
    }
    {
        let mut tokens = state.cancel_tokens.write().await;
        for (_, token) in tokens.drain() {
            token.cancel();
        }
    }
    {
        let mut senders = state.session_event_senders.write().await;
        senders.clear();
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true
    })))
}
