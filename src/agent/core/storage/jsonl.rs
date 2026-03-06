//! JSONL-based session storage implementation.
//!
//! This module provides persistent storage for sessions and events using
//! JSONL (JSON Lines) format for event logs and JSON for session metadata.
//!
//! # Storage Layout
//!
//! ```text
//! base_path/
//! ├── {session_id}.json    # Session metadata
//! └── {session_id}.jsonl   # Event stream (one JSON per line)
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use bamboo_agent::agent::core::storage::jsonl::*;
//!
//! let storage = JsonlStorage::new("/path/to/bamboo-data-dir/sessions");
//! storage.init().await?;
//!
//! // Save session
//! storage.save_session(&session).await?;
//!
//! // Append events
//! storage.append_event(&session_id, &event).await?;
//!
//! // Load session
//! let session = storage.load_session(&session_id).await?;
//!
//! // Load all events
//! let events = storage.load_events(&session_id).await?;
//! ```

use crate::agent::core::agent::{AgentEvent, Session};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// JSONL-based session storage.
///
/// Stores session metadata as JSON and events as JSONL (one JSON object per line).
///
/// # Fields
///
/// * `base_path` - Base directory for storing session files
///
/// # Example
///
/// ```rust,ignore
/// let storage = JsonlStorage::new("/path/to/bamboo-data-dir/sessions");
/// storage.init().await?;
///
/// storage.save_session(&session).await?;
/// let events = storage.load_events(&session_id).await?;
/// ```
#[derive(Debug, Clone)]
pub struct JsonlStorage {
    /// Base directory for session files
    base_path: PathBuf,
}

impl JsonlStorage {
    /// Create a new JSONL storage instance.
    ///
    /// # Arguments
    ///
    /// * `base_path` - Directory to store session files
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let storage = JsonlStorage::new("/path/to/bamboo-data-dir/sessions");
    /// ```
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    pub async fn init(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.base_path).await
    }

    pub async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        let path = self.session_path(&session.id);
        let json = serde_json::to_string(session)?;
        fs::write(path, json).await
    }

    pub async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        let path = self.session_path(session_id);
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(path).await?;
        let session = serde_json::from_str(&content)?;
        Ok(Some(session))
    }

    pub async fn append_event(&self, session_id: &str, event: &AgentEvent) -> std::io::Result<()> {
        let path = self.events_path(session_id);
        let json = serde_json::to_string(event)?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        file.write_all(json.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await
    }

    pub async fn load_events(&self, session_id: &str) -> std::io::Result<Vec<AgentEvent>> {
        let path = self.events_path(session_id);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = fs::File::open(path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut events = Vec::new();

        while let Some(line) = lines.next_line().await? {
            if let Ok(event) = serde_json::from_str(&line) {
                events.push(event);
            }
        }

        Ok(events)
    }

    pub async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
        let session_path = self.session_path(session_id);
        let events_path = self.events_path(session_id);
        let mut deleted_any = false;

        for path in [session_path, events_path] {
            match fs::remove_file(&path).await {
                Ok(()) => {
                    deleted_any = true;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }

        Ok(deleted_any)
    }

    fn session_path(&self, session_id: &str) -> PathBuf {
        self.base_path.join(format!("{}.json", session_id))
    }

    fn events_path(&self, session_id: &str) -> PathBuf {
        self.base_path.join(format!("{}.jsonl", session_id))
    }
}

/// Trait for session and event storage backends.
///
/// Provides an abstract interface for persisting and retrieving session data
/// and event streams. Implementations can use different storage backends
/// (e.g., JSONL files, databases, cloud storage).
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    /// Saves a session's metadata.
    async fn save_session(&self, session: &Session) -> std::io::Result<()>;

    /// Loads a session by ID, returns None if not found.
    async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>>;

    /// Appends an event to the session's event stream.
    async fn append_event(&self, session_id: &str, event: &AgentEvent) -> std::io::Result<()>;

    /// Loads all events for a session.
    async fn load_events(&self, session_id: &str) -> std::io::Result<Vec<AgentEvent>>;

    /// Deletes a session and its events, returns true if anything was deleted.
    async fn delete_session(&self, session_id: &str) -> std::io::Result<bool>;
}

#[async_trait::async_trait]
impl Storage for JsonlStorage {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        JsonlStorage::save_session(self, session).await
    }

    async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        JsonlStorage::load_session(self, session_id).await
    }

    async fn append_event(&self, session_id: &str, event: &AgentEvent) -> std::io::Result<()> {
        JsonlStorage::append_event(self, session_id, event).await
    }

    async fn load_events(&self, session_id: &str) -> std::io::Result<Vec<AgentEvent>> {
        JsonlStorage::load_events(self, session_id).await
    }

    async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
        JsonlStorage::delete_session(self, session_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use uuid::Uuid;

    async fn create_temp_storage() -> io::Result<(JsonlStorage, PathBuf)> {
        let temp_dir = std::env::temp_dir().join(format!("jsonl-storage-test-{}", Uuid::new_v4()));
        let storage = JsonlStorage::new(&temp_dir);
        storage.init().await?;
        Ok((storage, temp_dir))
    }

    #[tokio::test]
    async fn delete_session_removes_metadata_and_events_files() -> io::Result<()> {
        let (storage, temp_dir) = create_temp_storage().await?;
        let session = Session::new("session-1", "test-model");

        storage.save_session(&session).await?;
        storage
            .append_event(
                &session.id,
                &AgentEvent::Token {
                    content: "token".to_string(),
                },
            )
            .await?;

        assert!(storage.session_path(&session.id).exists());
        assert!(storage.events_path(&session.id).exists());

        let deleted = storage.delete_session(&session.id).await?;

        assert!(deleted);
        assert!(!storage.session_path(&session.id).exists());
        assert!(!storage.events_path(&session.id).exists());

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_session_returns_false_when_files_do_not_exist() -> io::Result<()> {
        let (storage, temp_dir) = create_temp_storage().await?;

        let deleted = storage.delete_session("missing-session").await?;

        assert!(!deleted);

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }
}
