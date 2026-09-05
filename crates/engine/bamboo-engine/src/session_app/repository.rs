//! Session access trait for decoupling use cases from server infrastructure.

use async_trait::async_trait;
use bamboo_domain::Session;

use super::errors::{RespondError, SessionLoadError, SessionSaveError};
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
    /// Implementations may merge concurrent UI edits to
    /// title/title_generated/pinned/title_version
    /// from disk back into `session` (which is why this takes `&mut`).
    async fn save_session(&self, session: &mut Session) -> Result<(), SessionSaveError>;

    /// Save a session to persistent storage and update the in-memory cache.
    ///
    /// Implementations may merge concurrent UI edits to
    /// title/title_generated/pinned/title_version
    /// from disk back into `session` (which is why this takes `&mut`).
    async fn save_and_cache(&self, session: &mut Session) -> Result<(), SessionSaveError>;

    /// Inspect the authoritative snapshot used by a pending-response
    /// transaction, without mutating it. Canonical repositories override this
    /// so durable consumed-response state wins over a stale cache candidate.
    async fn inspect_for_response(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        self.load_merged(id).await
    }

    /// Compare/mutate/persist the latest session for a pending response.
    /// Implementations backed by a canonical per-session lock override this so
    /// the whole operation is atomic. The default preserves compatibility for
    /// lightweight/in-memory adapters; the respond use case also serializes
    /// its entrypoints so those adapters cannot double-consume concurrently.
    async fn mutate_for_response(
        &self,
        id: &str,
        mutate: Box<
            dyn for<'session> FnOnce(&'session mut Session) -> Result<(), RespondError>
                + Send
                + 'static,
        >,
    ) -> Result<Option<Session>, RespondError> {
        let Some(mut session) = self.load_merged(id).await? else {
            return Ok(None);
        };
        mutate(&mut session)?;
        self.save_and_cache(&mut session).await?;
        Ok(Some(session))
    }
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
        SessionRepository::load_merged_checked(self, id)
            .await
            .map_err(|error| SessionLoadError::StorageError(error.to_string()))
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

    async fn inspect_for_response(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        let cache = self.cache().clone();
        let session_id = id.to_string();
        self.persistence()
            .inspect_runtime_session_for_response(id, move || {
                crate::read_cached_session(&cache, &session_id)
            })
            .await
            .map_err(|error| SessionLoadError::StorageError(error.to_string()))
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
        let cache_for_load = self.cache().clone();
        let publish_cache = self.cache().clone();
        let session_id = id.to_string();
        match self
            .persistence()
            .mutate_runtime_session_and_publish(
                id,
                move || crate::read_cached_session(&cache_for_load, &session_id),
                move |session| mutate(session),
                move |saved| {
                    publish_cache.insert(
                        saved.id.clone(),
                        std::sync::Arc::new(crate::SessionSnapshot::new(saved.clone())),
                    );
                },
            )
            .await
        {
            Ok(Ok(session)) => Ok(session),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(RespondError::SaveFailed(SessionSaveError::StorageError(
                error.to_string(),
            ))),
        }
    }
}
