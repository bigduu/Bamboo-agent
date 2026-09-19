//! Unified session loading helpers on AppState.
//!
//! Consolidates the three session loading patterns previously duplicated across handlers:
//!
//! - **`load_session`** (strict): memory → storage, returns `Option`
//! - **`load_or_create_session`**: memory → storage → create new
//! - **`load_session_merged`**: merges memory + storage with `should_prefer_storage` heuristic
//! - **`save_and_cache_session`**: dual write (persist + memory cache)
//!
//! Also provides the `SessionAccess` trait implementation for `AppState`,
//! bridging the application-layer use cases to the server infrastructure.

use super::*;

use bamboo_agent_core::Session;
use bamboo_engine::session_app::errors::{RespondError, SessionLoadError, SessionSaveError};
use bamboo_engine::session_app::repository::SessionAccess;

// `SessionAccess` for `AppState` is a pure forward to the framework-owned
// `session_repo` (the canonical `SessionAccess` impl lives on
// `bamboo_engine::SessionRepository`). The coordination + error mapping is
// defined once there, not duplicated here.
#[async_trait::async_trait]
impl SessionAccess for AppState {
    async fn load_session(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        SessionAccess::load_session(&self.session_repo, id).await
    }

    async fn load_or_create(&self, id: &str, model: &str) -> Result<Session, SessionLoadError> {
        SessionAccess::load_or_create(&self.session_repo, id, model).await
    }

    async fn load_merged(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        SessionAccess::load_merged(&self.session_repo, id).await
    }

    async fn save_session(&self, session: &mut Session) -> Result<(), SessionSaveError> {
        SessionAccess::save_session(&self.session_repo, session).await
    }

    async fn save_and_cache(&self, session: &mut Session) -> Result<(), SessionSaveError> {
        SessionAccess::save_and_cache(&self.session_repo, session).await
    }

    async fn inspect_for_response(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        SessionAccess::inspect_for_response(&self.session_repo, id).await
    }

    async fn mutate_for_response(
        &self,
        id: &str,
        mutate: Box<
            dyn for<'session> FnOnce(&'session mut Session) -> Result<(), RespondError>
                + Send
                + 'static,
        >,
    ) -> Result<Option<Session>, RespondError> {
        SessionAccess::mutate_for_response(&self.session_repo, id, mutate).await
    }
}

// The canonical load/save coordination now lives in
// `bamboo_engine::SessionRepository`. These inherent methods are kept as thin
// delegations so the ~hundreds of `state.load_session(...)` call sites stay
// unchanged; new code can take a `&SessionRepository` directly.
impl AppState {
    /// Load a session from memory cache, falling back to persistent storage.
    pub async fn load_session(&self, session_id: &str) -> Option<bamboo_agent_core::Session> {
        self.session_repo.load(session_id).await
    }

    /// Load a session, creating a new one if it doesn't exist.
    pub async fn load_or_create_session(
        &self,
        session_id: &str,
        model: &str,
    ) -> bamboo_agent_core::Session {
        self.session_repo.load_or_create(session_id, model).await
    }

    /// Load a session, merging memory and storage using a preference heuristic.
    pub async fn load_session_merged(
        &self,
        session_id: &str,
    ) -> Option<bamboo_agent_core::Session> {
        self.session_repo.load_merged(session_id).await
    }

    /// Persist session to storage and update the in-memory cache.
    pub async fn save_and_cache_session(&self, session: &mut bamboo_agent_core::Session) {
        self.session_repo.save_and_cache(session).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn load_session_returns_from_memory_first() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");

        let session_id = "session-memory-first";
        let session = bamboo_agent_core::Session::new(session_id.to_string(), "test-model");

        // Seed memory cache.
        state.sessions.insert(
            session_id.to_string(),
            Arc::new(bamboo_engine::SessionSnapshot::new(session.clone())),
        );

        let loaded = state.load_session(session_id).await;
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().id, session_id);
    }

    #[tokio::test]
    async fn load_session_falls_back_to_storage() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");

        let session_id = "session-storage-fallback";
        let session = bamboo_agent_core::Session::new(session_id.to_string(), "test-model");

        // Seed storage only.
        state
            .storage
            .save_session(&session)
            .await
            .expect("save session");

        let loaded = state.load_session(session_id).await;
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().id, session_id);
    }

    #[tokio::test]
    async fn load_session_returns_none_when_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");

        let loaded = state.load_session("nonexistent").await;
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn load_or_create_creates_new_when_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");

        let session = state.load_or_create_session("new-session", "gpt-4").await;
        assert_eq!(session.id, "new-session");
        assert_eq!(session.model, "gpt-4");
    }

    #[tokio::test]
    async fn load_session_merged_prefers_storage_with_pending_question() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");

        let session_id = "session-merge-pending";
        let memory_session = bamboo_agent_core::Session::new(session_id.to_string(), "test-model");
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
            Arc::new(bamboo_engine::SessionSnapshot::new(memory_session)),
        );
        state
            .storage
            .save_session(&storage_session)
            .await
            .expect("save session");

        let loaded = state.load_session_merged(session_id).await;
        assert!(loaded.is_some());
        assert!(loaded.unwrap().pending_question.is_some());
    }

    #[tokio::test]
    async fn save_and_cache_session_writes_both() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");

        let session_id = "session-save-cache";
        let mut session = bamboo_agent_core::Session::new(session_id.to_string(), "test-model");
        session.title = "test-title".to_string();

        state.save_and_cache_session(&mut session).await;

        // Verify memory cache.
        let cached = { bamboo_engine::read_cached_session(&state.sessions, session_id) };
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().title, "test-title");

        // Verify storage.
        let loaded = state.storage.load_session(session_id).await;
        assert!(loaded.is_ok());
        assert_eq!(loaded.unwrap().unwrap().title, "test-title");
    }
}
