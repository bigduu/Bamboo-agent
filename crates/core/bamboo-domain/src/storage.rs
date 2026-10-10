//! Storage port definitions — abstract interfaces for session persistence.
//!
//! These traits define the boundary between the domain layer and storage
//! implementations. Concrete implementations live in infrastructure crates.

use crate::session::types::Session;
use crate::session::{RootModeOperationDecision, RootModeOperationRequest};
use crate::{
    SupervisorBootstrapReceipt, SupervisorLinkObservation, SupervisorManagementReceipt,
    SupervisorManagementRequest, SupervisorReference, SupervisorScopeObservation,
};

/// A concrete ordinary Root execution's write capability. This is never read
/// from Session metadata or an HTTP request.
#[derive(Debug, Clone)]
pub struct RootActorRuntimeWrite {
    pub fence: crate::ActorActivationFence,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Synchronous cache publication at the storage commit boundary. Implementors
/// retain the same physical guards and revalidate the owner before invoking it.
pub type RootActorRuntimePublisher = std::sync::Arc<dyn Fn(&Session) + Send + Sync>;

/// The final sink receives a fresh owner check while the physical authority
/// guards remain owned by the publication job. Check immediately before each
/// final effect, including after any journal scan performed by the sink.
pub type RootActorRuntimeEventPublisher =
    Box<dyn FnOnce(&dyn Fn() -> std::io::Result<()>) -> std::io::Result<()> + Send>;

/// Trait for session storage backends.
///
/// Provides an abstract interface for persisting and retrieving session data.
/// Implementations can use different storage backends
/// (e.g., JSONL files, databases, cloud storage).
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    /// Persist an explicit manual-title mutation. Actor-aware backends must
    /// compare it with current canonical state under the full writer guards,
    /// allowing only the title fields and exact version advances. Generic
    /// Session saves (for example resident frame resets) are a separate lane.
    /// Backends without Actor observations retain their ordinary save behavior.
    async fn save_manual_title(&self, session: &Session) -> std::io::Result<()> {
        self.save_session(session).await
    }

    /// Validate a manual title before reporting success, including a no-op.
    /// Backends with Actor metadata observations must reread canonical state
    /// under their writer guards and reject stale or incomplete observations.
    /// This must not initialize, refresh or repair any authority. The default
    /// preserves title behavior for stores without Actor observations.
    async fn validate_title_observations(&self, expected: &Session) -> std::io::Result<()> {
        let _ = expected;
        Ok(())
    }

    /// Bind an Inbox to this exact Root execution before opting into owned
    /// claims. Unsupported/custom queues fail closed, without a legacy claim.
    fn bind_root_actor_inbox(
        &self,
        owner: &RootActorRuntimeWrite,
        inbox: std::sync::Arc<dyn crate::SessionInboxPort>,
    ) -> std::io::Result<std::sync::Arc<dyn crate::SessionInboxPort>> {
        let _ = (owner, inbox);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support owned Root Inbox admission",
        ))
    }

    /// Commit the typed message and cursor through the existing full writer,
    /// then ACK while retaining the same Root and Inbox physical guards.
    async fn save_root_actor_input(
        &self,
        owner: &RootActorRuntimeWrite,
        session: &Session,
        inbox: std::sync::Arc<dyn crate::SessionInboxPort>,
        claim: &crate::SessionInboxOwnedClaim,
        publish: RootActorRuntimePublisher,
    ) -> std::io::Result<()> {
        let _ = (owner, session, inbox, claim, publish);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support fenced Root input checkpoints",
        ))
    }

    /// Probe before claiming an Actor; unsupported backends must not leave a
    /// claimed execution whose writes fall back to an ordinary snapshot save.
    fn supports_root_actor_runtime_write(&self) -> bool {
        false
    }

    /// Publish a synchronous runtime event through the same current-owner
    /// boundary as canonical writes. The callback must be the actual sink,
    /// rather than an async queue that could publish after a replacement.
    async fn publish_root_actor_runtime_event(
        &self,
        owner: &RootActorRuntimeWrite,
        publish: RootActorRuntimeEventPublisher,
    ) -> std::io::Result<()> {
        let _ = (owner, publish);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support fenced Root runtime events",
        ))
    }

    /// Reuse the existing full/runtime publication protocol under a current
    /// ordinary Root fence. Validate birth/owner/expiry immediately before each
    /// canonical replacement and publish only a confirmed snapshot while the
    /// physical guards are still held. Never fall back to `save_session`.
    async fn save_root_actor_runtime(
        &self,
        owner: &RootActorRuntimeWrite,
        session: &Session,
        runtime_only: bool,
        publish: RootActorRuntimePublisher,
    ) -> std::io::Result<()> {
        let _ = (owner, session, runtime_only, publish);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support fenced Root runtime writes",
        ))
    }

    /// Re-read and reconcile an execution's inherited child wait under the
    /// physical session lock, retain birth/fence checks, then save and publish.
    async fn save_inherited_child_wait_finalized(
        &self,
        session: &mut Session,
        inherited: &crate::session::runtime_state::WaitingForChildrenState,
        root_writer: Option<(RootActorRuntimeWrite, RootActorRuntimePublisher)>,
    ) -> std::io::Result<()> {
        let _ = (session, inherited, root_writer);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            crate::SessionAuthorityConflict(
                "atomic inherited child wait finalization is unsupported".into(),
            ),
        ))
    }

    /// Reconcile only the execution's exact inherited wait at the final
    /// physical writer lock, preserving live run status and unrelated fields.
    /// Root input saves retain the same owner, transcript and ACK protocol.
    async fn save_runtime_with_inherited_child_wait(
        &self,
        session: &mut Session,
        inherited: &crate::InheritedChildWait,
        runtime_only: bool,
        root_writer: Option<(RootActorRuntimeWrite, RootActorRuntimePublisher)>,
        input: Option<(
            std::sync::Arc<dyn crate::SessionInboxPort>,
            crate::SessionInboxOwnedClaim,
        )>,
    ) -> std::io::Result<()> {
        let _ = (session, inherited, runtime_only, root_writer, input);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            crate::SessionAuthorityConflict(
                "atomic inherited child wait runtime persistence is unsupported".into(),
            ),
        ))
    }

    /// Whether this backend owns atomic child-wait mutations. Legacy backends
    /// retain LockedSessionStore's process-local serialized compatibility path.
    fn supports_atomic_child_wait_control_plane(&self) -> bool {
        false
    }

    /// Read one waited-for Child through its parent's canonical tree. Durable
    /// linkage must be checked before loading transcript content.
    async fn load_child_wait_session(
        &self,
        parent: &Session,
        child_id: &str,
        full: bool,
    ) -> std::io::Result<Option<Session>> {
        let control = self.load_runtime_control_plane(child_id).await?;
        let owned = control.filter(|child| {
            child.kind == crate::SessionKind::Child
                && child.parent_session_id.as_deref() == Some(&parent.id)
        });
        if !full || owned.is_none() {
            return Ok(owned);
        }
        Ok(self.load_session(child_id).await?.filter(|child| {
            child.kind == crate::SessionKind::Child
                && child.parent_session_id.as_deref() == Some(&parent.id)
        }))
    }

    /// Merge a child wait into the latest control plane under the physical
    /// session writer lock. Terminal filtering uses fresh durable child state,
    /// not a per-instance index. Explicit waits check terminality; pre-launch
    /// arms retain prior terminal generations until the new launch is queued.
    /// Zero means the requested explicit policy is satisfied.
    async fn register_child_wait_control_plane(
        &self,
        expected: &Session,
        batch: &[(String, Option<String>)],
        policy: crate::ChildWaitPolicy,
        check_terminal: bool,
        publish: RootActorRuntimePublisher,
    ) -> std::io::Result<(Session, usize)> {
        let _ = (expected, batch, policy, check_terminal, publish);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "atomic child wait registration is unsupported",
        ))
    }

    /// Commit a child completion only if the full observed wait and Session
    /// incarnation still match. A conflict performs no write or publication.
    async fn compare_exchange_child_wait_control_plane(
        &self,
        expected: &Session,
        updated: &mut Session,
        runtime_only: bool,
        publish: RootActorRuntimePublisher,
    ) -> std::io::Result<bool> {
        let _ = (expected, updated, runtime_only, publish);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "atomic child wait completion is unsupported",
        ))
    }

    /// Durable Root-mode CAS and terminal recovery at the storage writer lock.
    /// A backend without this authority protocol fails closed.
    async fn root_mode_operation(
        &self,
        request: &RootModeOperationRequest,
    ) -> std::io::Result<RootModeOperationDecision> {
        let _ = request;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support recoverable Root mode operations",
        ))
    }

    /// Trusted explicit recreation of a previously deleted Ordinary Root ID.
    /// The backend constructs a blank Root and assigns a fresh birth marker;
    /// callers cannot supply an old snapshot or choose its lifetime. Retrying
    /// after a complete publication returns that surviving lifetime unchanged.
    /// This is a host/SDK port, never a model-callable creation capability.
    async fn recreate_root_session(
        &self,
        session_id: &str,
        initial_model: &str,
    ) -> std::io::Result<Session> {
        let _ = (session_id, initial_model);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support trusted Root recreation",
        ))
    }

    /// Trusted host bootstrap for one stable default Supervisor Root. Only the
    /// initial model is caller supplied and is used on first creation only.
    /// It may be empty before provider setup; bootstrap does not execute a run.
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

    /// Bounded trusted host scope observation, never a Session directory.
    async fn inspect_supervisor_scope(
        &self,
        supervisor: &SupervisorReference,
    ) -> std::io::Result<SupervisorScopeObservation> {
        let _ = supervisor;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support Supervisor management",
        ))
    }

    /// Trusted management CAS at the final durable boundary. Implementations
    /// must revalidate identity/revision and any attach target under lifecycle,
    /// Task and ordered Session locks retained through publication. A stale
    /// revision returns WouldBlock; the caller must reload before a fresh retry.
    async fn mutate_supervisor_management(
        &self,
        request: &SupervisorManagementRequest,
    ) -> std::io::Result<SupervisorManagementReceipt> {
        let _ = request;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support Supervisor management",
        ))
    }

    /// Strict single-link observation under the same authority locks as CAS.
    /// Returned authorization expires when those locks are released. A later
    /// command needs an integrated final authorization + durable admission fence.
    async fn inspect_supervisor_link(
        &self,
        supervisor: &SupervisorReference,
        target_session_id: &str,
    ) -> std::io::Result<SupervisorLinkObservation> {
        let _ = (supervisor, target_session_id);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage backend does not support Supervisor management",
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

    /// Saves a session's metadata. A backend may allow initial creation for a
    /// never-deleted ID, but an ordinary snapshot must not recreate a deleted
    /// lifetime. Use the trusted recreation port for an explicitly reused ID.
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
