//! Storage port definitions — abstract interfaces for session persistence.
//!
//! These traits define the boundary between the domain layer and storage
//! implementations. Concrete implementations live in infrastructure crates.

use crate::session::types::Session;

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

    /// Persist ONLY the runtime control-plane (everything except the potentially
    /// large `messages` history) for an already-existing session.
    ///
    /// Backends that keep a small runtime sidecar use this to make frequent
    /// runtime-state updates (e.g. registering a parent's wait for spawned
    /// children) O(1) in conversation length instead of rewriting the whole
    /// message history. Backends without a sidecar fall back to a full
    /// [`save_session`](Self::save_session), so this is always safe to call.
    async fn save_runtime_state(&self, session: &Session) -> std::io::Result<()> {
        self.save_session(session).await
    }

    /// Load only the runtime control-plane snapshot — a [`Session`] whose
    /// `messages` are left empty — when the backend keeps one.
    ///
    /// Used to merge authoritative metadata before a runtime-only save without
    /// paying to deserialize the full message history. Backends without a
    /// sidecar fall back to a full [`load_session`](Self::load_session).
    async fn load_runtime_control_plane(
        &self,
        session_id: &str,
    ) -> std::io::Result<Option<Session>> {
        self.load_session(session_id).await
    }

    /// List `(child_session_id, last_run_status)` for every direct child of the
    /// given parent session, sourced from the index/metadata the backend keeps.
    ///
    /// This is the single source of truth for the parent→child relationship and
    /// each child's status; callers reconstruct active/completed child sets from
    /// it instead of reading a denormalized copy out of the parent file. Backends
    /// without a child-aware index return an empty list by default.
    async fn list_child_run_statuses(
        &self,
        parent_session_id: &str,
    ) -> std::io::Result<Vec<(String, Option<String>)>> {
        let _ = parent_session_id;
        Ok(Vec::new())
    }

    /// Append one analysis record — a single JSON line — to the session's
    /// dedicated, append-only token-usage log, stored alongside the session's
    /// other files in its per-session directory.
    ///
    /// One line is written per LLM call so the full per-round history (cache
    /// read/creation, output, budget breakdown) survives for offline cost/cache
    /// analysis — unlike `session.json`, which only keeps the latest overwritten
    /// usage snapshot. Backends without a per-session directory keep the default
    /// no-op, so this is always safe to call.
    async fn append_token_usage_record(
        &self,
        session_id: &str,
        json_line: &str,
    ) -> std::io::Result<()> {
        let _ = (session_id, json_line);
        Ok(())
    }
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
