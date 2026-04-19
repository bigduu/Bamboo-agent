//! Session access trait for decoupling use cases from server infrastructure.

use async_trait::async_trait;
use bamboo_domain::Session;

use super::errors::{SessionLoadError, SessionSaveError};

/// Trait for loading and persisting sessions.
///
/// Implemented by `AppState` in the server layer. Use cases depend on this
/// trait rather than concrete server types.
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
    async fn save_session(&self, session: &Session) -> Result<(), SessionSaveError>;

    /// Save a session to persistent storage and update the in-memory cache.
    async fn save_and_cache(&self, session: &Session) -> Result<(), SessionSaveError>;
}
