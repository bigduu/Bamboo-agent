//! External memory and note-taking compatibility layer for session-scoped context.
//!
//! Provides a backward-compatible API for storing and retrieving session-related
//! notes that persist across conversations.
//!
//! ## Storage Layout (v2 — multi-topic session notes)
//!
//! ```text
//! ${BAMBOO_DATA_DIR}/memory/v1/sessions/{session_id}/note/{topic}.md
//! ```
//!
//! The default topic is `"default"`. Legacy single-file notes
//! (`{session_id}.md`) are auto-migrated on first access.
//!
//! Dream notebook data is **not** part of this session-note API's canonical scope.
//! Dream is handled separately as a derived view (`views/DREAM_NOTEBOOK.md`) by
//! `MemoryStore` and `auto_dream`.

use std::io;
use std::path::PathBuf;

use crate::agent::core::memory_store::{MemoryStore, DEFAULT_SESSION_TOPIC};

/// Default topic name when none is specified.
pub const DEFAULT_TOPIC: &str = DEFAULT_SESSION_TOPIC;

/// External memory manager for storing and retrieving session notes.
#[derive(Debug, Clone)]
pub struct ExternalMemory {
    store: MemoryStore,
}

impl ExternalMemory {
    /// Create a new external memory manager.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            store: MemoryStore::new(data_dir),
        }
    }

    /// Create with default settings.
    ///
    /// Uses the Bamboo data directory's session memory store.
    pub fn with_defaults() -> Self {
        Self {
            store: MemoryStore::with_defaults(),
        }
    }

    pub fn store(&self) -> &MemoryStore {
        &self.store
    }

    // ── Validation ──────────────────────────────────────────────────────

    fn validate_session_id(session_id: &str) -> io::Result<()> {
        crate::agent::core::memory_store::validate_session_id(session_id).map(|_| ())
    }

    fn validate_topic(topic: &str) -> io::Result<()> {
        crate::agent::core::memory_store::validate_session_topic(topic).map(|_| ())
    }

    // ── Path resolution ─────────────────────────────────────────────────

    /// Returns the topic file path: `{memory/v1/sessions}/{session_id}/note/{topic}.md`
    fn topic_path(&self, session_id: &str, topic: &str) -> io::Result<PathBuf> {
        Self::validate_session_id(session_id)?;
        Self::validate_topic(topic)?;
        Ok(self.store.resolver().session_topic_path(session_id, topic))
    }

    // ── Topic-aware API (primary) ───────────────────────────────────────

    /// Save a note for a specific topic.
    pub async fn save_topic(
        &self,
        session_id: &str,
        topic: &str,
        note: &str,
    ) -> io::Result<PathBuf> {
        self.store
            .write_session_topic(session_id, topic, note)
            .await
    }

    /// Read a note for a specific topic.
    pub async fn read_topic(&self, session_id: &str, topic: &str) -> io::Result<Option<String>> {
        self.store.read_session_topic(session_id, topic).await
    }

    /// Delete a note for a specific topic.
    pub async fn delete_topic(&self, session_id: &str, topic: &str) -> io::Result<bool> {
        self.store.delete_session_topic(session_id, topic).await
    }

    /// Append to an existing topic note, or create a new one.
    pub async fn append_topic(
        &self,
        session_id: &str,
        topic: &str,
        content: &str,
    ) -> io::Result<PathBuf> {
        self.store
            .append_session_topic(session_id, topic, content)
            .await
    }

    /// List all topics for a session.
    pub async fn list_topics(&self, session_id: &str) -> io::Result<Vec<String>> {
        self.store.list_session_topics(session_id).await
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
        let root = self.store.resolver().sessions_root();
        let mut sessions = Vec::new();

        if !root.exists() {
            return Ok(sessions);
        }

        let mut entries = tokio::fs::read_dir(root).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                let note_dir = path.join("note");
                if note_dir.exists() {
                    if let Some(name) = path.file_name() {
                        sessions.push(name.to_string_lossy().to_string());
                    }
                }
            }
        }

        sessions.sort();
        Ok(sessions)
    }

    /// Get the path to the default notes file for a session.
    pub fn get_note_path(&self, session_id: &str) -> PathBuf {
        self.topic_path(session_id, DEFAULT_TOPIC)
            .unwrap_or_else(|_| {
                self.store
                    .resolver()
                    .sessions_root()
                    .join("invalid-session-id.md")
            })
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

        // Simulate legacy: create {data_dir}/notes/session-1.md directly
        let legacy_notes_dir = dir.path().join("notes");
        tokio::fs::create_dir_all(&legacy_notes_dir).await.unwrap();
        let legacy_path = legacy_notes_dir.join("session-1.md");
        tokio::fs::write(&legacy_path, "legacy content")
            .await
            .unwrap();

        // Reading should trigger migration
        let content = memory.read_note("session-1").await.unwrap();
        assert_eq!(content, Some("legacy content".to_string()));

        // Legacy file should be gone
        assert!(!legacy_path.exists());

        // New file should exist at memory/v1/sessions/session-1/note/default.md
        let new_path = dir
            .path()
            .join("memory")
            .join("v1")
            .join("sessions")
            .join("session-1")
            .join("note")
            .join("default.md");
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
        let legacy_notes_dir = dir.path().join("notes");
        tokio::fs::create_dir_all(&legacy_notes_dir).await.unwrap();
        let legacy_path = legacy_notes_dir.join("session-1.md");
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

        let long_name = "a".repeat(crate::agent::core::memory_store::MAX_SESSION_TOPIC_LEN + 1);
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
