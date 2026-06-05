//! Session access trait for decoupling use cases from server infrastructure.

use async_trait::async_trait;
use bamboo_domain::Session;

use super::errors::{SessionLoadError, SessionSaveError};
use crate::SessionRepository;

/// Trait for loading and persisting sessions.
///
/// The canonical implementation is [`SessionRepository`] (the framework-owned
/// coordinator). The server's `AppState` also implements it by delegating to
/// its `session_repo`. Use cases depend on this trait rather than concrete
/// server types.
#[async_trait]
pub trait SessionAccess: Send + Sync {
    /// Load a session by ID (from cache or storage).
    async fn load_session(&self, id: &str) -> Result<Option<Session>, SessionLoadError>;

    /// Load an existing session or create a new one with the given model.
    async fn load_or_create(&self, id: &str, model: &str) -> Result<Session, SessionLoadError>;

    /// Load a session, merging memory and storage using a preference heuristic.
    ///
    /// Prefers storage when it has a pending question or newer `updated_at`.
    async fn load_merged(&self, id: &str) -> Result<Option<Session>, SessionLoadError>;

    /// Save a session to persistent storage only.
    ///
    /// Implementations may merge concurrent UI edits to title/pinned/title_version
    /// from disk back into `session` (which is why this takes `&mut`).
    async fn save_session(&self, session: &mut Session) -> Result<(), SessionSaveError>;

    /// Save a session to persistent storage and update the in-memory cache.
    ///
    /// Implementations may merge concurrent UI edits to title/pinned/title_version
    /// from disk back into `session` (which is why this takes `&mut`).
    async fn save_and_cache(&self, session: &mut Session) -> Result<(), SessionSaveError>;
}

/// The framework-owned [`SessionRepository`] is the canonical `SessionAccess`.
/// Server `AppState` delegates to its `session_repo`; SDK / in-process callers
/// can use a `SessionRepository` directly as a `SessionAccess`.
#[async_trait]
impl SessionAccess for SessionRepository {
    async fn load_session(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        // Historical contract: absence is an error, not Ok(None).
        match SessionRepository::load(self, id).await {
            Some(session) => Ok(Some(session)),
            None => Err(SessionLoadError::NotFound(id.to_string())),
        }
    }

    async fn load_or_create(&self, id: &str, model: &str) -> Result<Session, SessionLoadError> {
        Ok(SessionRepository::load_or_create(self, id, model).await)
    }

    async fn load_merged(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        Ok(SessionRepository::load_merged(self, id).await)
    }

    async fn save_session(&self, session: &mut Session) -> Result<(), SessionSaveError> {
        // Storage-only persist (no cache write), matching the trait contract.
        self.persistence()
            .merge_save_runtime(session)
            .await
            .map_err(|e| SessionSaveError::StorageError(e.to_string()))
    }

    async fn save_and_cache(&self, session: &mut Session) -> Result<(), SessionSaveError> {
        SessionRepository::save_and_cache(self, session).await;
        Ok(())
    }
}
