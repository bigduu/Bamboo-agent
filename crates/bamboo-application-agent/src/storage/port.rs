//! Storage port definitions — abstract interfaces for session persistence.

use crate::agent::Session;

/// Trait for session storage backends.
///
/// Provides an abstract interface for persisting and retrieving session data.
/// Implementations can use different storage backends
/// (e.g., JSONL files, databases, cloud storage).
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    /// Saves a session's metadata.
    async fn save_session(&self, session: &Session) -> std::io::Result<()>;

    /// Loads a session by ID, returns None if not found.
    async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>>;

    /// Deletes a session, returns true if anything was deleted.
    async fn delete_session(&self, session_id: &str) -> std::io::Result<bool>;
}

/// Attachment reader for `bamboo-attachment://<session_id>/<attachment_id>` references.
///
/// This is used to keep session storage free of base64 while still allowing the
/// agent loop to send data URLs upstream (most providers expect either HTTP(S)
/// URLs or `data:` URLs for images).
#[async_trait::async_trait]
pub trait AttachmentReader: Send + Sync {
    async fn read_attachment(
        &self,
        session_id: &str,
        attachment_id: &str,
    ) -> std::io::Result<Option<(Vec<u8>, String)>>;
}
