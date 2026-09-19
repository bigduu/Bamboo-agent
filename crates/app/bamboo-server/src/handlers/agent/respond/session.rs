//! Session loading helpers for the respond handler.
//!
//! The main `load_session_from_memory_or_storage` has been migrated to
//! `AppState::load_session_merged` in `crate::app_state::session_loader`.

#[cfg(test)]
mod tests {
    use crate::app_state::AppState;
    use actix_web::web;
    use bamboo_agent_core::Session;

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
            "ConclusionWithOptions".to_string(),
            "Need confirmation?".to_string(),
            vec!["OK".to_string()],
            true,
        );

        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(memory_session)),
        );
        state
            .storage
            .save_session(&storage_session)
            .await
            .expect("storage save should succeed");

        let loaded = state
            .load_session_merged(session_id)
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

        let cached = state
            .sessions
            .get(session_id)
            .map(|e| e.value().clone())
            .map(|arc| arc.read().clone())
            .expect("cache should be warmed");
        assert!(cached.pending_question.is_some());
    }
}
