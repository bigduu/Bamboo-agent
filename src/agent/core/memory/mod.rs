//! External memory and note-taking for conversation context persistence.
//!
//! Provides a way to store and retrieve session-related notes that can
//! persist across conversations and be retrieved when resuming sessions.
//!
//! ## Storage Layout (v2 — multi-topic)
//!
//! ```text
//! ~/.bamboo/notes/{session_id}/{topic}.md
//! ```
//!
//! The default topic is `"default"`. Legacy single-file notes
//! (`{session_id}.md`) are auto-migrated on first access.

use std::io;
use std::path::PathBuf;

/// Default topic name when none is specified.
pub const DEFAULT_TOPIC: &str = "default";

/// Maximum allowed topic name length.
const MAX_TOPIC_LEN: usize = 50;

/// External memory manager for storing and retrieving session notes.
#[derive(Debug)]
pub struct ExternalMemory {
    /// Directory to store notes
    notes_dir: PathBuf,
}

impl ExternalMemory {
    /// Create a new external memory manager.
    pub fn new(notes_dir: impl Into<PathBuf>) -> Self {
        Self {
            notes_dir: notes_dir.into(),
        }
    }

    /// Create with default settings.
    ///
    /// Uses the Bamboo data directory's `notes/` folder as the storage directory.
    pub fn with_defaults() -> Self {
        let notes_dir = crate::core::paths::bamboo_dir().join("notes");
        Self::new(notes_dir)
    }

    // ── Validation ──────────────────────────────────────────────────────

    fn validate_session_id(session_id: &str) -> io::Result<()> {
        let trimmed = session_id.trim();
        if trimmed.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session_id cannot be empty",
            ));
        }
        if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains("..") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session_id contains invalid path characters",
            ));
        }
        if !trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session_id contains unsupported characters",
            ));
        }
        Ok(())
    }

    fn validate_topic(topic: &str) -> io::Result<()> {
        let trimmed = topic.trim();
        if trimmed.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "topic cannot be empty",
            ));
        }
        if trimmed.len() > MAX_TOPIC_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "topic name too long (max {} chars, got {})",
                    MAX_TOPIC_LEN,
                    trimmed.len()
                ),
            ));
        }
        if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains("..") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "topic contains invalid path characters",
            ));
        }
        if !trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "topic must contain only alphanumeric, dash, or underscore characters",
            ));
        }
        Ok(())
    }

    // ── Path resolution ─────────────────────────────────────────────────

    /// Returns the session directory: `{notes_dir}/{session_id}/`
    fn session_dir(&self, session_id: &str) -> io::Result<PathBuf> {
        Self::validate_session_id(session_id)?;
        Ok(self.notes_dir.join(session_id.trim()))
    }

    /// Returns the topic file path: `{notes_dir}/{session_id}/{topic}.md`
    fn topic_path(&self, session_id: &str, topic: &str) -> io::Result<PathBuf> {
        let dir = self.session_dir(session_id)?;
        Self::validate_topic(topic)?;
        Ok(dir.join(format!("{}.md", topic.trim())))
    }

    /// Legacy single-file path: `{notes_dir}/{session_id}.md`
    fn legacy_note_path(&self, session_id: &str) -> io::Result<PathBuf> {
        Self::validate_session_id(session_id)?;
        Ok(self.notes_dir.join(format!("{}.md", session_id.trim())))
    }

    // ── Legacy migration ────────────────────────────────────────────────

    /// Auto-migrate `{session_id}.md` → `{session_id}/default.md` if needed.
    async fn maybe_migrate_legacy(&self, session_id: &str) -> io::Result<()> {
        let legacy = self.legacy_note_path(session_id)?;
        if !legacy.exists() {
            return Ok(());
        }

        let new_dir = self.session_dir(session_id)?;
        let new_path = new_dir.join(format!("{DEFAULT_TOPIC}.md"));

        // Only migrate if the new path doesn't already exist
        if new_path.exists() {
            // Both exist — delete legacy to avoid confusion
            let _ = tokio::fs::remove_file(&legacy).await;
            return Ok(());
        }

        tokio::fs::create_dir_all(&new_dir).await?;
        tokio::fs::rename(&legacy, &new_path).await?;

        tracing::info!(
            "Migrated legacy note {} -> {}",
            legacy.display(),
            new_path.display()
        );
        Ok(())
    }

    // ── Topic-aware API (primary) ───────────────────────────────────────

    /// Save a note for a specific topic.
    pub async fn save_topic(
        &self,
        session_id: &str,
        topic: &str,
        note: &str,
    ) -> io::Result<PathBuf> {
        self.maybe_migrate_legacy(session_id).await?;
        let path = self.topic_path(session_id, topic)?;
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        tokio::fs::write(&path, note).await?;
        Ok(path)
    }

    /// Read a note for a specific topic.
    pub async fn read_topic(&self, session_id: &str, topic: &str) -> io::Result<Option<String>> {
        self.maybe_migrate_legacy(session_id).await?;
        let path = self.topic_path(session_id, topic)?;
        if !path.exists() {
            return Ok(None);
        }
        let content = tokio::fs::read_to_string(&path).await?;
        Ok(Some(content))
    }

    /// Delete a note for a specific topic.
    pub async fn delete_topic(&self, session_id: &str, topic: &str) -> io::Result<bool> {
        self.maybe_migrate_legacy(session_id).await?;
        let path = self.topic_path(session_id, topic)?;
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Append to an existing topic note, or create a new one.
    pub async fn append_topic(
        &self,
        session_id: &str,
        topic: &str,
        content: &str,
    ) -> io::Result<PathBuf> {
        let existing = self.read_topic(session_id, topic).await?;
        let note = match existing {
            Some(mut prev) => {
                prev.push_str("\n\n");
                prev.push_str(content);
                prev
            }
            None => content.to_string(),
        };
        self.save_topic(session_id, topic, &note).await
    }

    /// List all topics for a session.
    pub async fn list_topics(&self, session_id: &str) -> io::Result<Vec<String>> {
        self.maybe_migrate_legacy(session_id).await?;
        let dir = self.session_dir(session_id)?;
        let mut topics = Vec::new();

        if !dir.exists() {
            return Ok(topics);
        }

        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "md") {
                if let Some(stem) = path.file_stem() {
                    topics.push(stem.to_string_lossy().to_string());
                }
            }
        }

        topics.sort();
        Ok(topics)
    }

    // ── Backward-compatible API (uses DEFAULT_TOPIC) ────────────────────

    /// Save a note for a session (default topic).
    pub async fn save_note(&self, session_id: &str, note: &str) -> io::Result<PathBuf> {
        self.save_topic(session_id, DEFAULT_TOPIC, note).await
    }

    /// Read a note for a session (default topic).
    pub async fn read_note(&self, session_id: &str) -> io::Result<Option<String>> {
        self.read_topic(session_id, DEFAULT_TOPIC).await
    }

    /// Delete a note for a session (default topic).
    pub async fn delete_note(&self, session_id: &str) -> io::Result<bool> {
        self.delete_topic(session_id, DEFAULT_TOPIC).await
    }

    /// Append to an existing note (default topic).
    pub async fn append_note(&self, session_id: &str, content: &str) -> io::Result<PathBuf> {
        self.append_topic(session_id, DEFAULT_TOPIC, content).await
    }

    /// List all session IDs that have notes.
    pub async fn list_sessions_with_notes(&self) -> io::Result<Vec<String>> {
        let mut sessions = Vec::new();

        if !self.notes_dir.exists() {
            return Ok(sessions);
        }

        let mut entries = tokio::fs::read_dir(&self.notes_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    sessions.push(name.to_string_lossy().to_string());
                }
            }
        }

        Ok(sessions)
    }

    /// Get the path to the default notes file for a session.
    pub fn get_note_path(&self, session_id: &str) -> PathBuf {
        self.topic_path(session_id, DEFAULT_TOPIC)
            .unwrap_or_else(|_| self.notes_dir.join("invalid-session-id.md"))
    }

    /// Check if a note exists for a session (default topic).
    pub async fn has_note(&self, session_id: &str) -> bool {
        self.get_note_path(session_id).exists()
    }
}

/// Format a conversation summary as a note for external memory.
pub fn format_summary_as_note(summary: &str, message_count: usize, token_count: u32) -> String {
    let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");

    format!(
        r#"# Conversation Summary

**Generated:** {timestamp}
**Messages Summarized:** {message_count}
**Token Count:** {token_count}

{summary}
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn save_and_read_note() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        let note = "This is a test note.";
        memory.save_note("session-1", note).await.unwrap();

        let read = memory.read_note("session-1").await.unwrap();
        assert_eq!(read, Some(note.to_string()));
    }

    #[tokio::test]
    async fn read_nonexistent_note() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        let read = memory.read_note("nonexistent").await.unwrap();
        assert!(read.is_none());
    }

    #[tokio::test]
    async fn delete_note() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        memory.save_note("session-1", "Note").await.unwrap();
        let deleted = memory.delete_note("session-1").await.unwrap();
        assert!(deleted);

        let deleted_again = memory.delete_note("session-1").await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn append_to_note() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        memory.save_note("session-1", "First part").await.unwrap();
        memory
            .append_note("session-1", "Second part")
            .await
            .unwrap();

        let read = memory.read_note("session-1").await.unwrap();
        assert_eq!(read, Some("First part\n\nSecond part".to_string()));
    }

    #[tokio::test]
    async fn list_sessions() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        memory.save_note("session-1", "Note 1").await.unwrap();
        memory.save_note("session-2", "Note 2").await.unwrap();

        let sessions = memory.list_sessions_with_notes().await.unwrap();
        assert_eq!(sessions.len(), 2);
        assert!(sessions.contains(&"session-1".to_string()));
        assert!(sessions.contains(&"session-2".to_string()));
    }

    #[tokio::test]
    async fn rejects_invalid_session_id_characters() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        let save = memory.save_note("../escape", "bad").await;
        assert!(save.is_err());

        let read = memory.read_note("bad/name").await;
        assert!(read.is_err());
    }

    #[test]
    fn format_summary_creates_markdown() {
        let summary = "User asked about Rust. Assistant explained.";
        let note = format_summary_as_note(summary, 10, 500);

        assert!(note.contains("# Conversation Summary"));
        assert!(note.contains("**Messages Summarized:** 10"));
        assert!(note.contains("**Token Count:** 500"));
        assert!(note.contains(summary));
    }

    // ── Multi-topic tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn multi_topic_read_write() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        memory
            .save_topic("s1", "project-a", "Project A notes")
            .await
            .unwrap();
        memory
            .save_topic("s1", "project-b", "Project B notes")
            .await
            .unwrap();

        assert_eq!(
            memory.read_topic("s1", "project-a").await.unwrap(),
            Some("Project A notes".to_string())
        );
        assert_eq!(
            memory.read_topic("s1", "project-b").await.unwrap(),
            Some("Project B notes".to_string())
        );
    }

    #[tokio::test]
    async fn list_topics_returns_sorted() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        memory.save_topic("s1", "zebra", "z").await.unwrap();
        memory.save_topic("s1", "alpha", "a").await.unwrap();
        memory.save_topic("s1", "mid", "m").await.unwrap();

        let topics = memory.list_topics("s1").await.unwrap();
        assert_eq!(topics, vec!["alpha", "mid", "zebra"]);
    }

    #[tokio::test]
    async fn legacy_migration_moves_file() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        // Simulate legacy: create {notes_dir}/session-1.md directly
        let legacy_path = dir.path().join("session-1.md");
        tokio::fs::write(&legacy_path, "legacy content")
            .await
            .unwrap();

        // Reading should trigger migration
        let content = memory.read_note("session-1").await.unwrap();
        assert_eq!(content, Some("legacy content".to_string()));

        // Legacy file should be gone
        assert!(!legacy_path.exists());

        // New file should exist at session-1/default.md
        let new_path = dir.path().join("session-1").join("default.md");
        assert!(new_path.exists());
    }

    #[tokio::test]
    async fn legacy_migration_preserves_new_over_legacy() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        // Create new-style note first
        memory
            .save_topic("session-1", "default", "new content")
            .await
            .unwrap();

        // Simulate legacy file appearing (e.g., from an older version)
        let legacy_path = dir.path().join("session-1.md");
        tokio::fs::write(&legacy_path, "old content").await.unwrap();

        // Reading should prefer new content and delete legacy
        let content = memory.read_note("session-1").await.unwrap();
        assert_eq!(content, Some("new content".to_string()));
        assert!(!legacy_path.exists());
    }

    #[tokio::test]
    async fn delete_specific_topic() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        memory.save_topic("s1", "keep", "keep me").await.unwrap();
        memory
            .save_topic("s1", "remove", "delete me")
            .await
            .unwrap();

        memory.delete_topic("s1", "remove").await.unwrap();

        assert_eq!(
            memory.read_topic("s1", "keep").await.unwrap(),
            Some("keep me".to_string())
        );
        assert_eq!(memory.read_topic("s1", "remove").await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_invalid_topic_names() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        assert!(memory.save_topic("s1", "../escape", "bad").await.is_err());
        assert!(memory.save_topic("s1", "has space", "bad").await.is_err());
        assert!(memory.save_topic("s1", "", "bad").await.is_err());

        let long_name = "a".repeat(MAX_TOPIC_LEN + 1);
        assert!(memory.save_topic("s1", &long_name, "bad").await.is_err());
    }

    #[tokio::test]
    async fn append_to_topic() {
        let dir = tempdir().unwrap();
        let memory = ExternalMemory::new(dir.path());

        memory.save_topic("s1", "notes", "first").await.unwrap();
        memory.append_topic("s1", "notes", "second").await.unwrap();

        let content = memory.read_topic("s1", "notes").await.unwrap();
        assert_eq!(content, Some("first\n\nsecond".to_string()));
    }
}
