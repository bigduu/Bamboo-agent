//! External memory and note-taking for conversation context persistence.
//!
//! Provides a way to store and retrieve session-related notes that can
//! persist across conversations and be retrieved when resuming sessions.

use std::io;
use std::path::PathBuf;

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
    /// Uses `~/.bamboo/notes` as the storage directory.
    pub fn with_defaults() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let notes_dir = home.join(".bamboo").join("notes");
        Self::new(notes_dir)
    }

    /// Save a note for a session.
    ///
    /// The note is stored as a markdown file named `{session_id}.md`.
    pub async fn save_note(&self, session_id: &str, note: &str) -> io::Result<PathBuf> {
        // Ensure notes directory exists
        tokio::fs::create_dir_all(&self.notes_dir).await?;

        let note_path = self.notes_dir.join(format!("{}.md", session_id));
        tokio::fs::write(&note_path, note).await?;

        Ok(note_path)
    }

    /// Read a note for a session.
    ///
    /// Returns None if no note exists for the session.
    pub async fn read_note(&self, session_id: &str) -> io::Result<Option<String>> {
        let note_path = self.notes_dir.join(format!("{}.md", session_id));

        if !note_path.exists() {
            return Ok(None);
        }

        let content = tokio::fs::read_to_string(&note_path).await?;
        Ok(Some(content))
    }

    /// Delete a note for a session.
    ///
    /// Returns true if a note was deleted, false if no note existed.
    pub async fn delete_note(&self, session_id: &str) -> io::Result<bool> {
        let note_path = self.notes_dir.join(format!("{}.md", session_id));

        if note_path.exists() {
            tokio::fs::remove_file(&note_path).await?;
            Ok(true)
        } else {
            Ok(false)
        }
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
            if path.extension().is_some_and(|ext| ext == "md") {
                if let Some(stem) = path.file_stem() {
                    sessions.push(stem.to_string_lossy().to_string());
                }
            }
        }

        Ok(sessions)
    }

    /// Append to an existing note, or create a new one if it doesn't exist.
    pub async fn append_note(&self, session_id: &str, content: &str) -> io::Result<PathBuf> {
        let existing = self.read_note(session_id).await?;

        let note = match existing {
            Some(mut prev) => {
                prev.push_str("\n\n");
                prev.push_str(content);
                prev
            }
            None => content.to_string(),
        };

        self.save_note(session_id, &note).await
    }

    /// Get the path to the notes file for a session.
    pub fn get_note_path(&self, session_id: &str) -> PathBuf {
        self.notes_dir.join(format!("{}.md", session_id))
    }

    /// Check if a note exists for a session.
    pub async fn has_note(&self, session_id: &str) -> bool {
        self.get_note_path(session_id).exists()
    }
}

/// Format a conversation summary as a note for external memory.
pub fn format_summary_as_note(
    summary: &str,
    message_count: usize,
    token_count: u32,
) -> String {
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
        memory.append_note("session-1", "Second part").await.unwrap();

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

    #[test]
    fn format_summary_creates_markdown() {
        let summary = "User asked about Rust. Assistant explained.";
        let note = format_summary_as_note(summary, 10, 500);

        assert!(note.contains("# Conversation Summary"));
        // Format includes markdown bold markers (**)
        assert!(note.contains("**Messages Summarized:** 10"));
        assert!(note.contains("**Token Count:** 500"));
        assert!(note.contains(summary));
    }
}
