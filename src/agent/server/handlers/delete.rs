use actix_web::{web, HttpResponse, Result};

use crate::state::AppState;

pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let deleted_from_storage = match state.storage.delete_session(&session_id).await {
        Ok(deleted) => deleted,
        Err(error) => {
            log::error!(
                "[{}] Failed to delete session from storage: {}",
                session_id,
                error
            );
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to delete session"
            })));
        }
    };

    let removed_from_memory = {
        let mut sessions = state.sessions.write().await;
        sessions.remove(&session_id).is_some()
    };

    let cancelled_in_flight = {
        let mut tokens = state.cancel_tokens.write().await;
        if let Some(token) = tokens.remove(&session_id) {
            token.cancel();
            true
        } else {
            false
        }
    };

    if deleted_from_storage || removed_from_memory || cancelled_in_flight {
        log::info!(
            "[{}] Session deleted successfully (storage: {}, memory: {}, cancelled: {})",
            session_id,
            deleted_from_storage,
            removed_from_memory,
            cancelled_in_flight
        );
        return Ok(HttpResponse::Ok().finish());
    }

    Ok(HttpResponse::NotFound().json(serde_json::json!({
        "error": "Session not found"
    })))
}
