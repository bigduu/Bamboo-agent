//! Plan file persistence for plan mode sessions.
//!
//! Stores plan markdown files in `${BAMBOO_DATA_DIR}/plans/{session_slug}.md`.

use std::path::{Path, PathBuf};

/// Error type for plan store operations.
#[derive(Debug, thiserror::Error)]
pub enum PlanStoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Plan directory not accessible: {0}")]
    DirectoryNotAccessible(String),
}

/// Stores and retrieves plan files for sessions.
#[derive(Debug, Clone)]
pub struct PlanStore {
    plans_dir: PathBuf,
}

impl PlanStore {
    /// Create a new plan store with the given base data directory.
    ///
    /// Plans are stored in `{data_dir}/plans/`.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, PlanStoreError> {
        let plans_dir = data_dir.as_ref().join("plans");
        std::fs::create_dir_all(&plans_dir).map_err(|e| {
            PlanStoreError::DirectoryNotAccessible(format!(
                "Failed to create plans directory at {}: {}",
                plans_dir.display(),
                e
            ))
        })?;
        Ok(Self { plans_dir })
    }

    /// Generate a slug from a session ID suitable for use as a filename.
    ///
    /// Uses the first 8 characters of the session ID plus a hash of the remainder
    /// to produce a short, deterministic, filesystem-safe identifier.
    fn session_slug(session_id: &str) -> String {
        let clean = session_id
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect::<String>();

        if clean.len() <= 16 {
            return clean;
        }

        let prefix = &clean[..8];
        let suffix = &clean[clean.len() - 8..];
        format!(
            "{}-{}-{:x}",
            prefix,
            suffix,
            seahash::hash(clean.as_bytes())
        )
    }

    /// Path to the plan file for a given session.
    pub fn plan_file_path(&self, session_id: &str) -> PathBuf {
        let slug = Self::session_slug(session_id);
        self.plans_dir.join(format!("{}.md", slug))
    }

    /// Write a plan for the given session.
    ///
    /// Overwrites any existing plan for this session.
    pub fn write_plan(
        &self,
        session_id: &str,
        content: impl AsRef<str>,
    ) -> Result<PathBuf, PlanStoreError> {
        let path = self.plan_file_path(session_id);
        std::fs::write(&path, content.as_ref())?;
        Ok(path)
    }

    /// Read the plan for the given session, if it exists.
    pub fn read_plan(&self, session_id: &str) -> Option<String> {
        let path = self.plan_file_path(session_id);
        std::fs::read_to_string(&path).ok()
    }

    /// Check whether a plan exists for the given session.
    pub fn plan_exists(&self, session_id: &str) -> bool {
        self.plan_file_path(session_id).exists()
    }

    /// Delete the plan for the given session.
    pub fn delete_plan(&self, session_id: &str) -> Result<(), PlanStoreError> {
        let path = self.plan_file_path(session_id);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Get the storage directory path.
    pub fn plans_dir(&self) -> &Path {
        &self.plans_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> PlanStore {
        let temp_dir = std::env::temp_dir().join(format!("plan_store_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        PlanStore::new(&temp_dir).unwrap()
    }

    #[test]
    fn session_slug_produces_short_identifier() {
        let id = "sess-abc123-def456-ghi789";
        let slug = PlanStore::session_slug(id);
        assert!(!slug.is_empty());
        assert!(!slug.contains('/'));
        assert!(!slug.contains('\\'));
    }

    #[test]
    fn write_and_read_plan() {
        let store = temp_store();
        let session_id = "test-session-001";
        let content = "# Implementation Plan\n\n1. Step one\n2. Step two\n";

        let path = store.write_plan(session_id, content).unwrap();
        assert!(path.exists());
        assert!(store.plan_exists(session_id));

        let read = store.read_plan(session_id).unwrap();
        assert_eq!(read, content);
    }

    #[test]
    fn read_nonexistent_plan_returns_none() {
        let store = temp_store();
        assert!(store.read_plan("nonexistent-session").is_none());
        assert!(!store.plan_exists("nonexistent-session"));
    }

    #[test]
    fn write_plan_overwrites_existing() {
        let store = temp_store();
        let session_id = "test-session-002";

        store.write_plan(session_id, "Plan v1").unwrap();
        store.write_plan(session_id, "Plan v2").unwrap();

        let read = store.read_plan(session_id).unwrap();
        assert_eq!(read, "Plan v2");
    }

    #[test]
    fn delete_plan_removes_file() {
        let store = temp_store();
        let session_id = "test-session-003";

        store.write_plan(session_id, "Plan to delete").unwrap();
        assert!(store.plan_exists(session_id));

        store.delete_plan(session_id).unwrap();
        assert!(!store.plan_exists(session_id));
    }

    #[test]
    fn delete_nonexistent_plan_is_noop() {
        let store = temp_store();
        // Should not error
        store.delete_plan("never-created").unwrap();
    }

    #[test]
    fn plan_file_path_is_under_plans_dir() {
        let store = temp_store();
        let path = store.plan_file_path("some-session");
        assert!(path.starts_with(&store.plans_dir));
        assert_eq!(path.extension().unwrap(), "md");
    }

    #[test]
    fn session_slug_handles_short_id() {
        let id = "short";
        let slug = PlanStore::session_slug(id);
        assert_eq!(slug, "short");
    }

    #[test]
    fn session_slug_strips_special_chars() {
        let id = "sess/abc\\def:ghi";
        let slug = PlanStore::session_slug(id);
        assert!(!slug.contains('/'));
        assert!(!slug.contains('\\'));
        assert!(!slug.contains(':'));
    }
}
