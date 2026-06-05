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
impl bamboo_engine::session_app::repository::SessionAccess for AppState {
    async fn load_session(
        &self,
        id: &str,
    ) -> Result<Option<bamboo_agent_core::Session>, bamboo_engine::session_app::errors::SessionLoadError>
    {
        match AppState::load_session(self, id).await {
            Some(session) => Ok(Some(session)),
            None => Err(bamboo_engine::session_app::errors::SessionLoadError::NotFound(
                id.to_string(),
            )),
        }
    }

    async fn load_or_create(
        &self,
        id: &str,
        model: &str,
    ) -> Result<bamboo_agent_core::Session, bamboo_engine::session_app::errors::SessionLoadError> {
        Ok(AppState::load_or_create_session(self, id, model).await)
    }

    async fn save_session(
        &self,
        session: &mut bamboo_agent_core::Session,
    ) -> Result<(), bamboo_engine::session_app::errors::SessionSaveError> {
        self.persistence
            .merge_save_runtime(session)
            .await
            .map_err(|e| bamboo_engine::session_app::errors::SessionSaveError::StorageError(e.to_string()))
    }

    async fn save_and_cache(
        &self,
        session: &mut bamboo_agent_core::Session,
    ) -> Result<(), bamboo_engine::session_app::errors::SessionSaveError> {
        AppState::save_and_cache_session(self, session).await;
        Ok(())
    }

    async fn load_merged(
        &self,
        id: &str,
    ) -> Result<Option<bamboo_agent_core::Session>, bamboo_engine::session_app::errors::SessionLoadError>
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
            let arc = self.sessions.get(session_id).map(|e| e.value().clone());
            arc.map(|a| a.read().clone())
        };

        if let Some(session) = memory_session {
            return Some(session);
        }

        match self.storage.load_session(session_id).await {
            Ok(Some(session)) => {
                self.sessions.insert(
                    session_id.to_string(),
                    Arc::new(parking_lot::RwLock::new(session.clone())),
                );
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
            let arc = self.sessions.get(session_id).map(|e| e.value().clone());
            arc.map(|a| a.read().clone())
        };

        let storage_session = self
            .storage
            .load_session(session_id)
            .await
            .unwrap_or_default();

        match (memory_session, storage_session) {
            (Some(memory), Some(storage)) => {
                let prefer_storage = should_prefer_storage(&memory, &storage);
                // The vast majority of merges are no-op agreements (same length,
                // memory wins). Only log when the two sources actually diverge —
                // i.e. storage is preferred, or the message counts differ — since
                // that is the only case worth investigating. Full detail at trace.
                let diverged = prefer_storage || memory.messages.len() != storage.messages.len();
                let chosen_len = if prefer_storage {
                    storage.messages.len()
                } else {
                    memory.messages.len()
                };
                macro_rules! merged_log {
                    ($level:ident) => {
                        tracing::$level!(
                            "[{}] load_session_merged: memory={} msgs (updated_at={}), storage={} msgs (updated_at={}), prefer_storage={} -> chose {} msgs",
                            session_id,
                            memory.messages.len(),
                            memory.updated_at,
                            storage.messages.len(),
                            storage.updated_at,
                            prefer_storage,
                            chosen_len,
                        )
                    };
                }
                if diverged {
                    merged_log!(debug);
                } else {
                    merged_log!(trace);
                }
                let chosen = if prefer_storage { storage } else { memory };
                self.sessions.insert(
                    session_id.to_string(),
                    Arc::new(parking_lot::RwLock::new(chosen.clone())),
                );
                Some(chosen)
            }
            (Some(memory), None) => Some(memory),
            (None, Some(storage)) => {
                self.sessions.insert(
                    session_id.to_string(),
                    Arc::new(parking_lot::RwLock::new(storage.clone())),
                );
                Some(storage)
            }
            (None, None) => None,
        }
    }

    /// Persist session to storage and update the in-memory cache.
    ///
    /// Uses [`bamboo_infrastructure::LockedSessionStore::merge_save_runtime`]
    /// so concurrent UI edits to the authoritative metadata group are preserved.
    /// The in-memory cache is updated with the merged session (post-merge fields).
    pub async fn save_and_cache_session(&self, session: &mut bamboo_agent_core::Session) {
        if let Err(error) = self.persistence.merge_save_runtime(session).await {
            tracing::warn!("[{}] Failed to save session: {}", session.id, error);
        }
        self.sessions.insert(
            session.id.clone(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
        );
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
        state.sessions.insert(
            session_id.to_string(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
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
            Arc::new(parking_lot::RwLock::new(memory_session)),
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
        let cached = {
            let arc = state.sessions.get(session_id).map(|e| e.value().clone());
            arc.map(|a| a.read().clone())
        };
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().title, "test-title");

        // Verify storage.
        let loaded = state.storage.load_session(session_id).await;
        assert!(loaded.is_ok());
        assert_eq!(loaded.unwrap().unwrap().title, "test-title");
    }
}
