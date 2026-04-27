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

#[async_trait::async_trait]
impl crate::session_app::repository::SessionAccess for AppState {
    async fn load_session(
        &self,
        id: &str,
    ) -> Result<Option<bamboo_agent_core::Session>, crate::session_app::errors::SessionLoadError>
    {
        match AppState::load_session(self, id).await {
            Some(session) => Ok(Some(session)),
            None => Err(crate::session_app::errors::SessionLoadError::NotFound(
                id.to_string(),
            )),
        }
    }

    async fn load_or_create(
        &self,
        id: &str,
        model: &str,
    ) -> Result<bamboo_agent_core::Session, crate::session_app::errors::SessionLoadError> {
        Ok(AppState::load_or_create_session(self, id, model).await)
    }

    async fn save_session(
        &self,
        session: &bamboo_agent_core::Session,
    ) -> Result<(), crate::session_app::errors::SessionSaveError> {
        self.storage
            .save_session(session)
            .await
            .map_err(|e| crate::session_app::errors::SessionSaveError::StorageError(e.to_string()))
    }

    async fn save_and_cache(
        &self,
        session: &bamboo_agent_core::Session,
    ) -> Result<(), crate::session_app::errors::SessionSaveError> {
        AppState::save_and_cache_session(self, session).await;
        Ok(())
    }

    async fn load_merged(
        &self,
        id: &str,
    ) -> Result<Option<bamboo_agent_core::Session>, crate::session_app::errors::SessionLoadError>
    {
        Ok(AppState::load_session_merged(self, id).await)
    }
}

impl AppState {
    /// Load a session from memory cache, falling back to persistent storage.
    ///
    /// Returns `None` if the session does not exist in either tier.
    pub async fn load_session(&self, session_id: &str) -> Option<bamboo_agent_core::Session> {
        let memory_session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        if let Some(session) = memory_session {
            return Some(session);
        }

        match self.storage.load_session(session_id).await {
            Ok(Some(session)) => {
                let mut sessions = self.sessions.write().await;
                sessions.insert(session_id.to_string(), session.clone());
                Some(session)
            }
            _ => None,
        }
    }

    /// Load a session, creating a new one if it doesn't exist.
    ///
    /// Memory cache → storage → new `Session::new(session_id, model)`.
    pub async fn load_or_create_session(
        &self,
        session_id: &str,
        model: &str,
    ) -> bamboo_agent_core::Session {
        if let Some(session) = self.load_session(session_id).await {
            return session;
        }
        bamboo_agent_core::Session::new(session_id.to_string(), model.to_string())
    }

    /// Load a session, merging memory and storage using a preference heuristic.
    ///
    /// Prefers the storage version when:
    /// - memory lacks a `pending_question` but storage has one
    /// - storage session has a newer `updated_at`
    pub async fn load_session_merged(
        &self,
        session_id: &str,
    ) -> Option<bamboo_agent_core::Session> {
        let memory_session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        let storage_session = self
            .storage
            .load_session(session_id)
            .await
            .unwrap_or_default();

        match (memory_session, storage_session) {
            (Some(memory), Some(storage)) => {
                let chosen = if should_prefer_storage(&memory, &storage) {
                    storage
                } else {
                    memory
                };
                let mut sessions = self.sessions.write().await;
                sessions.insert(session_id.to_string(), chosen.clone());
                Some(chosen)
            }
            (Some(memory), None) => Some(memory),
            (None, Some(storage)) => {
                let mut sessions = self.sessions.write().await;
                sessions.insert(session_id.to_string(), storage.clone());
                Some(storage)
            }
            (None, None) => None,
        }
    }

    /// Persist session to storage and update the in-memory cache.
    pub async fn save_and_cache_session(&self, session: &bamboo_agent_core::Session) {
        if let Err(error) = self.storage.save_session(session).await {
            tracing::warn!("[{}] Failed to save session: {}", session.id, error);
        }
        let mut sessions = self.sessions.write().await;
        sessions.insert(session.id.clone(), session.clone());
    }
}

fn should_prefer_storage(
    memory_session: &bamboo_agent_core::Session,
    storage_session: &bamboo_agent_core::Session,
) -> bool {
    if memory_session.pending_question.is_none() && storage_session.pending_question.is_some() {
        return true;
    }
    storage_session.updated_at > memory_session.updated_at
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
        {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.to_string(), session.clone());
        }

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
        state.storage.save_session(&session).await;

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

        {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.to_string(), memory_session);
        }
        state.storage.save_session(&storage_session).await;

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

        state.save_and_cache_session(&session).await;

        // Verify memory cache.
        let cached = {
            let sessions = state.sessions.read().await;
            sessions.get(session_id).cloned()
        };
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().title, "test-title");

        // Verify storage.
        let loaded = state.storage.load_session(session_id).await;
        assert!(loaded.is_ok());
        assert_eq!(loaded.unwrap().unwrap().title, "test-title");
    }
}
