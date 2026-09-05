//! Storage port definitions — abstract interfaces for session persistence.
//!
//! These traits define the boundary between the domain layer and storage
//! implementations. Concrete implementations live in infrastructure crates.

use crate::session::types::Session;
use crate::SupervisorBootstrapReceipt;

/// Trait for session storage backends.
///
/// Provides an abstract interface for persisting and retrieving session data.
/// Implementations can use different storage backends
/// (e.g., JSONL files, databases, cloud storage).
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    /// Trusted host bootstrap for one stable default Supervisor Root. Only the
    /// initial model is caller supplied and is used on first creation only.
    /// Implementations must publish the complete identity atomically, protect it
    /// from ordinary writers, and return a receipt rather than a partial Session.
    async fn get_or_create_default_supervisor(
        &self,
        initial_model: &str,
    ) -> std::io::Result<SupervisorBootstrapReceipt> {
        let _ = initial_model;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support trusted Supervisor bootstrap",
        ))
    }

    /// Strict canonical Root control-plane read for authority decisions.
    /// `None` means absent; partial/corrupt/mismatched published authority is an
    /// error, never a fallback to stale session.json. Returned messages are empty;
    /// this observation must not replace a full Session in a history cache.
    async fn load_root_authority(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        let _ = session_id;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support strict Root authority reads",
        ))
    }

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

    /// Recover an interrupted two-session Task control-plane transaction for
    /// this exact, lexically ordered session pair before either snapshot is
    /// read. Backends without a recovery journal have nothing to do.
    ///
    /// Implementations that retain an undo journal after a failed rollback must
    /// fail closed on ordinary control-plane reads/writes until this operation
    /// succeeds; otherwise callers could continue from a permanently divergent
    /// child/root Task generation in the same process.
    async fn recover_task_control_plane_transaction(
        &self,
        first_session_id: &str,
        second_session_id: &str,
    ) -> std::io::Result<()> {
        let _ = (first_session_id, second_session_id);
        Ok(())
    }

    /// Final-CAS one Task-owned control-plane snapshot. The backend must
    /// re-read the durable Task list/generation under the same lock as its
    /// atomic sidecar replacement, compare them with `original`, and build the
    /// physical write from that current snapshot while patching only Task-owned
    /// fields from `updated`.
    ///
    /// `Ok(true)` commits, `Ok(false)` reports a stale/missing target without
    /// writing, and unsupported backends must fail before mutation. This port
    /// prevents independent [`crate::RuntimeSessionPersistence`] wrappers from
    /// both publishing candidates staged from the same generation.
    async fn save_task_control_plane_if_matches(
        &self,
        original: &Session,
        updated: &Session,
    ) -> std::io::Result<bool> {
        let _ = (original, updated);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support atomic Task control-plane CAS",
        ))
    }

    /// Commit two already-existing runtime control planes as one recoverable
    /// Task transaction. Arguments must be ordered lexically by session id and
    /// each updated snapshot must have the same id as its original snapshot.
    ///
    /// `Ok(true)` is the commit point: both Task lists/generations are durable
    /// and no recovery journal may remain that could later undo them. The
    /// backend must revalidate both Task-owned original snapshots while holding
    /// its final transaction lock; `Ok(false)` reports a stale-generation
    /// conflict and must not write either target or publish a journal. `Err`
    /// requires both originals to remain durable, or a retained recovery
    /// journal plus fail-closed access until recovery restores them.
    /// Implementations unable to provide that contract must return
    /// `Unsupported` before writing.
    async fn save_task_control_planes_atomically(
        &self,
        first_original: &Session,
        first_updated: &Session,
        second_original: &Session,
        second_updated: &Session,
    ) -> std::io::Result<bool> {
        let _ = (
            first_original,
            first_updated,
            second_original,
            second_updated,
        );
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support recoverable paired Task control-plane writes",
        ))
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

    /// List `(session_id, parent_session_id)` for every session whose
    /// `last_run_status` equals `status`, sourced from the backend's index.
    ///
    /// The child-wait watchdog (issue #546) uses this to cheaply enumerate
    /// candidates — sessions suspended on children (`status == "suspended"`)
    /// and orphaned children left `"running"` by a process restart — without
    /// loading every session. Backends without an index return an empty list
    /// by default, which degrades the watchdog to a no-op (never an error).
    async fn list_sessions_by_run_status(
        &self,
        status: &str,
    ) -> std::io::Result<Vec<(String, Option<String>)>> {
        let _ = status;
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
