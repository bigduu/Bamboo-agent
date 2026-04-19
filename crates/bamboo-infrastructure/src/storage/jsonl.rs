//! JSONL-based session storage implementation.
//!
//! This module provides persistent storage for sessions using JSON format.
//!
//! # Storage Layout
//!
//! ```text
//! base_path/
//! └── {session_id}.json    # Session metadata
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
//! // Load session
//! let session = storage.load_session(&session_id).await?;
//! ```

use bamboo_domain::Session;
use bamboo_domain::Storage;
use std::path::{Path, PathBuf};
use tokio::fs;

/// JSONL-based session storage.
///
/// Stores session metadata as JSON.
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

    pub async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
        let session_path = self.session_path(session_id);
        let mut deleted_any = false;

        for path in [session_path] {
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
}

#[async_trait::async_trait]
impl Storage for JsonlStorage {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        JsonlStorage::save_session(self, session).await
    }

    async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        JsonlStorage::load_session(self, session_id).await
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
    async fn test_init_creates_directory() -> io::Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("jsonl-init-test-{}", Uuid::new_v4()));
        let storage = JsonlStorage::new(&temp_dir);

        assert!(!temp_dir.exists());
        storage.init().await?;
        assert!(temp_dir.exists());

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_save_and_load_session() -> io::Result<()> {
        let (storage, temp_dir) = create_temp_storage().await?;
        let session = Session::new("session-1", "test-model");

        storage.save_session(&session).await?;
        let loaded = storage.load_session(&session.id).await?;

        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, session.id);
        assert_eq!(loaded.model, session.model);

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_load_session_returns_none_when_not_found() -> io::Result<()> {
        let (storage, temp_dir) = create_temp_storage().await?;

        let loaded = storage.load_session("nonexistent").await?;
        assert!(loaded.is_none());

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_session_removes_metadata_file() -> io::Result<()> {
        let (storage, temp_dir) = create_temp_storage().await?;
        let session = Session::new("session-1", "test-model");

        storage.save_session(&session).await?;

        assert!(storage.session_path(&session.id).exists());

        let deleted = storage.delete_session(&session.id).await?;

        assert!(deleted);
        assert!(!storage.session_path(&session.id).exists());

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

    #[tokio::test]
    async fn test_session_path_format() -> io::Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("jsonl-path-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp_dir).await?;
        let storage = JsonlStorage::new(&temp_dir);

        let session_path = storage.session_path("test-123");

        assert_eq!(session_path.file_name().unwrap(), "test-123.json");

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_existing_session() -> io::Result<()> {
        let (storage, temp_dir) = create_temp_storage().await?;

        let session1 = Session::new("session-1", "model-1");
        storage.save_session(&session1).await?;

        let session2 = Session::new("session-1", "model-2");
        storage.save_session(&session2).await?;

        let loaded = storage.load_session("session-1").await?.unwrap();
        assert_eq!(loaded.model, "model-2");

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_trait_implementation() -> io::Result<()> {
        let (storage, temp_dir) = create_temp_storage().await?;
        let trait_obj: &dyn Storage = &storage;

        let session = Session::new("session-1", "test-model");
        trait_obj.save_session(&session).await?;

        let loaded = trait_obj.load_session(&session.id).await?;
        assert!(loaded.is_some());

        let deleted = trait_obj.delete_session(&session.id).await?;
        assert!(deleted);

        fs::remove_dir_all(temp_dir).await?;
        Ok(())
    }
}
