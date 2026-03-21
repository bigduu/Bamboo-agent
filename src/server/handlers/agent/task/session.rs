use actix_web::web;

use crate::agent::core::Session;
use crate::server::app_state::AppState;

pub(super) async fn load_session_from_memory_or_storage(
    state: &web::Data<AppState>,
    session_id: &str,
) -> Option<Session> {
    // Try memory first.
    {
        let sessions = state.sessions.read().await;
        if let Some(session) = sessions.get(session_id) {
            return Some(session.clone());
        }
    }

    // Fallback to storage and warm memory cache.
    match state.storage.load_session(session_id).await {
        Ok(Some(session)) => {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.to_string(), session.clone());
            Some(session)
        }
        _ => None,
    }
}
