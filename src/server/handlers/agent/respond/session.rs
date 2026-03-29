use actix_web::web;

use crate::agent::core::Session;
use crate::server::app_state::AppState;

fn should_prefer_storage(memory_session: &Session, storage_session: &Session) -> bool {
    if memory_session.pending_question.is_none() && storage_session.pending_question.is_some() {
        return true;
    }

    storage_session.updated_at > memory_session.updated_at
}

pub(super) async fn load_session_from_memory_or_storage(
    state: &web::Data<AppState>,
    session_id: &str,
) -> Option<Session> {
    let memory_session = {
        let sessions = state.sessions.read().await;
        sessions.get(session_id).cloned()
    };

    let storage_session = match state.storage.load_session(session_id).await {
        Ok(session) => session,
        Err(_) => None,
    };

    match (memory_session, storage_session) {
        (Some(memory_session), Some(storage_session)) => {
            let chosen = if should_prefer_storage(&memory_session, &storage_session) {
                storage_session
            } else {
                memory_session
            };

            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.to_string(), chosen.clone());
            Some(chosen)
        }
        (Some(memory_session), None) => Some(memory_session),
        (None, Some(storage_session)) => {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.to_string(), storage_session.clone());
            Some(storage_session)
        }
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::app_state::AppState;

    #[tokio::test]
    async fn prefers_storage_when_memory_lacks_pending_question() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = web::Data::new(state);

        let session_id = "session-stale-memory";
        let memory_session = Session::new(session_id, "test-model");
        let mut storage_session = memory_session.clone();
        storage_session.set_pending_question(
            "tool-call-1".to_string(),
            "Need confirmation?".to_string(),
            vec!["OK".to_string()],
            true,
        );

        {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.to_string(), memory_session);
        }
        state
            .storage
            .save_session(&storage_session)
            .await
            .expect("storage save should succeed");

        let loaded = load_session_from_memory_or_storage(&state, session_id)
            .await
            .expect("session should load");

        assert!(loaded.pending_question.is_some());
        assert_eq!(
            loaded
                .pending_question
                .as_ref()
                .map(|pending| pending.tool_call_id.as_str()),
            Some("tool-call-1")
        );

        let cached = {
            let sessions = state.sessions.read().await;
            sessions
                .get(session_id)
                .cloned()
                .expect("cache should be warmed")
        };
        assert!(cached.pending_question.is_some());
    }
}
