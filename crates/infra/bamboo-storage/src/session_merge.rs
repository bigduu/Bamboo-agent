//! Merge-aware session save helper.
//!
//! Provides [`merge_save_session`], which preserves any concurrent UI edits to
//! the authoritative metadata group (`title`, `title_version`,
//! `title_generated`, `pinned`, `metadata_version`) before writing the
//! runtime-modified session to storage.
//! Re-reads the latest persisted copy and only takes in-memory values when the
//! caller's `metadata_version` strictly exceeds disk's.
//!
//! ## Field-by-field merge policy
//!
//! All authoritative metadata fields are grouped under `metadata_version`:
//! when `disk.metadata_version >= session.metadata_version`, the on-disk
//! `title`, `title_version`, `title_generated`, `pinned`, and
//! `metadata_version` overwrite the in-memory values before writing.
//! Authoritative writers bump
//! `metadata_version` (and `title_version` for title edits) before calling so
//! their values survive the merge; non-authoritative writers don't bump and so
//! are overwritten by any later disk changes.
//!
//! ## Two save primitives
//!
//! - **`merge_save_session`** — stateless merge+save. Still works for
//!   non-authoritative writers that hold `Arc<dyn Storage>` directly.
//! - **`LockedSessionStore::merge_save_runtime`** — per-session-locked variant
//!   that additionally serializes writes for the same session. Prefer this for
//!   server-side paths where an authoritative writer may race with a runtime
//!   save.
//! - **`LockedSessionStore::commit_metadata`** — plain save inside a per-session
//!   lock. For authoritative writers that have already performed
//!   load→mutate→bump inside the lock; no merge needed (they hold the latest).
//!
//! Bare [`Storage::save_session`] is reserved for first-write paths (e.g. new
//! session creation) where there is no prior on-disk copy to merge against.

use std::sync::Arc;

use bamboo_domain::session::types::Session;
use bamboo_domain::storage::Storage;
use bamboo_domain::{PermissionAuditSeed, PermissionAuditSnapshot, RuntimeSessionPersistence};
use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

const AUTHORITATIVE_METADATA_KEYS: &[&str] = &["gold_config", "workflow.run_ids.v1"];
const TASK_CONTROL_PLANE_CONFLICT_PREFIX: &str = "Task control-plane changed while saving session ";
const MAX_TASK_CONTROL_PLANE_REBASE_RETRIES: usize = 3;

fn is_task_control_plane_save_conflict(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock
        && error
            .to_string()
            .starts_with(TASK_CONTROL_PLANE_CONFLICT_PREFIX)
}

fn adopt_durable_task_control_plane(session: &mut Session, durable: &Session) {
    session.task_list = durable.task_list.clone();
    session
        .metadata
        .remove(bamboo_domain::session::runtime_metadata::keys::TASK_LIST_VERSION);
    if let Some(runtime_metadata) = session.runtime_metadata.as_mut() {
        runtime_metadata.task_list_version = None;
    }
    if session
        .runtime_metadata
        .as_ref()
        .is_some_and(bamboo_domain::session::SessionRuntimeMetadata::is_empty)
    {
        session.runtime_metadata = None;
    }
    if let Some(version) = durable.task_list_version_meta() {
        session.set_task_list_version_meta(version);
    }
}

fn task_list_snapshot_matches(
    session: &Session,
    expected_task_list: &bamboo_domain::TaskList,
) -> std::io::Result<bool> {
    Ok(serde_json::to_value(&session.task_list)
        .map_err(|error| std::io::Error::other(error.to_string()))?
        == serde_json::to_value(Some(expected_task_list))
            .map_err(|error| std::io::Error::other(error.to_string()))?)
}

fn unconditional_task_patch_would_regress(
    durable: &Session,
    incoming_task_list: &bamboo_domain::TaskList,
    incoming_version: &str,
) -> std::io::Result<bool> {
    let same_list = task_list_snapshot_matches(durable, incoming_task_list)?;
    let Some(durable_version) = durable.task_list_version_meta() else {
        return Ok(false);
    };
    match (
        incoming_version.parse::<u64>(),
        durable_version.parse::<u64>(),
    ) {
        (Ok(incoming), Ok(durable)) => {
            Ok(incoming < durable || (incoming == durable && !same_list))
        }
        _ => Ok(incoming_version != durable_version || !same_list),
    }
}

// ── LockedSessionStore ────────────────────────────────────────────────

/// Wraps a [`Storage`] implementation with per-session write serialization.
///
/// Under the hood it maintains a `DashMap<String, Arc<Mutex<()>>>` so that
/// only writes targeting the *same* session are serialised; different
/// sessions proceed concurrently.
pub struct LockedSessionStore {
    storage: Arc<dyn Storage>,
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    /// Serializes recoverable child/root Task transactions. Per-session locks
    /// still provide the data isolation; this gate ensures a retained recovery
    /// journal is resolved before another pair attempts to commit.
    task_pair_transaction_lock: Arc<Mutex<()>>,
}

/// Self-cleaning guard returned by [`LockedSessionStore::acquire_lock`].
///
/// Holds the `OwnedMutexGuard` for the session's serialization mutex. On drop it
/// releases the mutex **first** (so this guard's `Arc` clone is gone before the
/// count is read) and then removes the map entry iff `Arc::strong_count == 1` —
/// i.e. only the map's own reference remains, no other task holds or is waiting
/// on this session's lock.
///
/// ## Race freedom
///
/// The strong-count check and the removal execute atomically under DashMap's
/// per-shard lock via [`DashMap::remove_if`]. A waiter that clones the `Arc`
/// (through `acquire_lock`'s `entry()`) does so under the same shard lock, so it
/// either:
/// - clones **before** our `remove_if` → `strong_count >= 2` → we skip removal,
///   the waiter keeps a live, map-resident lock; or
/// - clones **after** our `remove_if` → the entry is gone → it inserts a fresh
///   `Arc<Mutex<()>>`; since our guard had already been released, the two tasks
///   never overlapped and needed no mutual exclusion.
///
/// There is therefore no interleaving in which a waiter observes a lock that we
/// then delete out from under it.
pub struct SessionLockGuard {
    /// `Option` so `Drop` can release the mutex before evaluating strong-count.
    guard: Option<OwnedMutexGuard<()>>,
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    session_id: String,
}

impl Drop for SessionLockGuard {
    fn drop(&mut self) {
        // Release the mutex (drops this guard's `Arc` clone) BEFORE reading the
        // strong count, otherwise the count can never reach 1.
        self.guard.take();
        self.locks
            .remove_if(&self.session_id, |_, arc| Arc::strong_count(arc) == 1);
    }
}

impl LockedSessionStore {
    /// Wrap an existing storage backend.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            locks: Arc::new(DashMap::new()),
            task_pair_transaction_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Borrow the inner storage for read-only access.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Acquire a per-session serialization guard.
    ///
    /// Only writes for the **same** session are serialised; writes for
    /// different sessions can proceed concurrently.
    ///
    /// The returned [`SessionLockGuard`] is **self-cleaning**: when it drops it
    /// releases the mutex and then removes the map entry iff no other holder
    /// remains. Without this the `locks` map grew by one entry for every session
    /// id ever written and never shrank (issue #346), so a long-lived server
    /// leaked one `Arc<Mutex<()>>` per session-ever-persisted. See
    /// [`SessionLockGuard`] for the race-freedom argument.
    pub async fn acquire_lock(&self, session_id: &str) -> SessionLockGuard {
        // `entry().or_insert_with().clone()` releases the DashMap shard lock at
        // the end of THIS statement, before the `.await` below — never hold a
        // shard lock across the async lock acquisition (it would deadlock the
        // self-cleaning `remove_if` on drop, which also takes the shard lock).
        let lock = self
            .locks
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = lock.lock_owned().await;
        SessionLockGuard {
            guard: Some(guard),
            locks: self.locks.clone(),
            session_id: session_id.to_string(),
        }
    }

    /// Save a full snapshot while preserving Task generations advanced by an
    /// independent store instance. V2 rejects such a stale write before any
    /// mutation; reloading and adopting only Task-owned fields makes the retry,
    /// caller snapshot, and subsequent cache publication agree exactly.
    async fn save_session_rebasing_task_conflicts(
        &self,
        session: &mut Session,
    ) -> std::io::Result<()> {
        for attempt in 0..=MAX_TASK_CONTROL_PLANE_REBASE_RETRIES {
            match self.storage.save_session(session).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_task_control_plane_save_conflict(&error)
                        && attempt < MAX_TASK_CONTROL_PLANE_REBASE_RETRIES =>
                {
                    let Some(durable) =
                        self.storage.load_runtime_control_plane(&session.id).await?
                    else {
                        return Err(error);
                    };
                    adopt_durable_task_control_plane(session, &durable);
                }
                Err(error) => {
                    if is_task_control_plane_save_conflict(&error) {
                        if let Some(durable) =
                            self.storage.load_runtime_control_plane(&session.id).await?
                        {
                            adopt_durable_task_control_plane(session, &durable);
                        }
                    }
                    return Err(error);
                }
            }
        }
        unreachable!("bounded Task conflict retry loop always returns")
    }

    /// Sidecar-only counterpart of
    /// [`Self::save_session_rebasing_task_conflicts`].
    async fn save_runtime_state_rebasing_task_conflicts(
        &self,
        session: &mut Session,
    ) -> std::io::Result<()> {
        for attempt in 0..=MAX_TASK_CONTROL_PLANE_REBASE_RETRIES {
            match self.storage.save_runtime_state(session).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_task_control_plane_save_conflict(&error)
                        && attempt < MAX_TASK_CONTROL_PLANE_REBASE_RETRIES =>
                {
                    let Some(durable) =
                        self.storage.load_runtime_control_plane(&session.id).await?
                    else {
                        return Err(error);
                    };
                    adopt_durable_task_control_plane(session, &durable);
                }
                Err(error) => {
                    if is_task_control_plane_save_conflict(&error) {
                        if let Some(durable) =
                            self.storage.load_runtime_control_plane(&session.id).await?
                        {
                            adopt_durable_task_control_plane(session, &durable);
                        }
                    }
                    return Err(error);
                }
            }
        }
        unreachable!("bounded Task conflict retry loop always returns")
    }

    /// Runtime-only save: persist the control-plane (`agent_runtime_state`,
    /// metadata, …) without rewriting the message history.
    ///
    /// This is the fast path for runtime-state mutations that do NOT change
    /// `messages` — e.g. registering a parent's wait for spawned children. It
    /// delegates to [`Storage::save_runtime_state`], which writes a small
    /// sidecar (or falls back to a full save on backends without one).
    ///
    /// Like [`Self::merge_save_runtime`], it merges newer authoritative metadata
    /// from disk so a concurrent UI title/pin edit is never clobbered — but it
    /// reads only the lightweight control-plane snapshot (no message history) to
    /// do so.
    ///
    /// Callers MUST NOT use this when they have appended messages: the in-memory
    /// `messages` are ignored by the sidecar and would not be persisted. They
    /// also MUST NOT use it to author `model_context_state`; the durable ledger
    /// loaded under the lock always wins this narrow save.
    pub async fn save_runtime_only(&self, session: &mut Session) -> std::io::Result<()> {
        self.save_runtime_only_and_publish(session, |_| {}).await
    }

    /// Save the runtime control-plane and synchronously publish the committed
    /// snapshot before releasing this session's serialization lock.
    ///
    /// `publish` must remain a short, non-blocking operation. Its synchronous
    /// shape intentionally prevents callers from holding an in-memory cache
    /// guard across an await. The callback also runs when the durable save
    /// fails, preserving [`RuntimeSessionPersistence::save_runtime_control_plane`]
    /// implementations that publish current runtime authorization state while
    /// still returning the storage error.
    pub async fn save_runtime_only_and_publish<F>(
        &self,
        session: &mut Session,
        publish: F,
    ) -> std::io::Result<()>
    where
        F: FnOnce(&Session) + Send,
    {
        let _guard = self.acquire_lock(&session.id).await;
        if let Some(latest) = self.storage.load_runtime_control_plane(&session.id).await? {
            apply_authoritative_metadata(session, &latest);
            // The control-plane sidecar carries `agent_runtime_state`, so a
            // concurrent mid-run bypass flip is here too — don't revert it. #540.
            adopt_fresher_disk_permission_posture(session, &latest);
            // Runtime-only callers own narrow control-plane fields (Task list,
            // parent wait state, …), never the model-context ledger. The sidecar
            // load and save share this lock, so disk is unconditionally
            // authoritative for the ledger and the published snapshot.
            adopt_durable_model_context_state(session, &latest);
        }
        let result = self
            .save_runtime_state_rebasing_task_conflicts(session)
            .await;
        publish(session);
        result
    }

    /// Atomically patch Task-owned control-plane fields and publish the saved
    /// value before releasing this session's serialization lock.
    ///
    /// This couples durable commit order to cache publication order for
    /// repository callers. The synchronous callback may take a cache guard but
    /// cannot hold one across an await.
    pub async fn update_task_list_control_plane_and_publish<F>(
        &self,
        session_id: &str,
        task_list: &bamboo_domain::TaskList,
        version: &str,
        publish: F,
    ) -> std::io::Result<bool>
    where
        F: FnOnce(&Session) + Send,
    {
        let _guard = self.acquire_lock(session_id).await;
        let Some(mut latest) = self.storage.load_runtime_control_plane(session_id).await? else {
            return Ok(false);
        };
        if unconditional_task_patch_would_regress(&latest, task_list, version)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!("Task control-plane changed while patching session {session_id}"),
            ));
        }
        let original = latest.clone();
        latest.task_list = Some(task_list.clone());
        latest.set_task_list_version_meta(version.to_string());
        if !self
            .storage
            .save_task_control_plane_if_matches(&original, &latest)
            .await?
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!("Task control-plane changed while patching session {session_id}"),
            ));
        }
        publish(&latest);
        Ok(true)
    }

    /// Compare-and-patch variant used by asynchronous evaluators. The durable
    /// generation check, narrow Task mutation, save, and cache publication all
    /// occur under the same per-session lock.
    pub async fn update_task_list_control_plane_if_version_and_publish<F>(
        &self,
        session_id: &str,
        expected_version: &str,
        expected_task_list: &bamboo_domain::TaskList,
        task_list: &bamboo_domain::TaskList,
        version: &str,
        publish: F,
    ) -> std::io::Result<bool>
    where
        F: FnOnce(&Session) + Send,
    {
        let _guard = self.acquire_lock(session_id).await;
        let Some(mut latest) = self.storage.load_runtime_control_plane(session_id).await? else {
            return Ok(false);
        };
        if latest.task_list_version_meta().as_deref() != Some(expected_version)
            || !task_list_snapshot_matches(&latest, expected_task_list)?
        {
            return Ok(false);
        }
        let original = latest.clone();
        latest.task_list = Some(task_list.clone());
        latest.set_task_list_version_meta(version.to_string());
        if !self
            .storage
            .save_task_control_plane_if_matches(&original, &latest)
            .await?
        {
            return Ok(false);
        }
        publish(&latest);
        Ok(true)
    }

    /// Recoverably validate and narrowly patch both the executing session and
    /// its shared root. Locks are acquired in lexical id order to prevent two
    /// child evaluators from deadlocking while sharing a root. The underlying
    /// storage transaction must either commit both Task generations or retain
    /// an undo journal and fail closed until both originals are restored.
    pub async fn update_task_list_control_planes_if_version_and_publish<F>(
        &self,
        session_id: &str,
        shared_session_id: &str,
        expected_version: &str,
        expected_task_list: &bamboo_domain::TaskList,
        task_list: &bamboo_domain::TaskList,
        version: &str,
        publish: F,
    ) -> std::io::Result<bool>
    where
        F: FnOnce(&Session, &Session) + Send,
    {
        if session_id == shared_session_id {
            return self
                .update_task_list_control_plane_if_version_and_publish(
                    session_id,
                    expected_version,
                    expected_task_list,
                    task_list,
                    version,
                    |session| publish(session, session),
                )
                .await;
        }

        let (first_id, second_id) = if session_id < shared_session_id {
            (session_id, shared_session_id)
        } else {
            (shared_session_id, session_id)
        };
        let _transaction_guard = self.task_pair_transaction_lock.lock().await;
        let _first_guard = self.acquire_lock(first_id).await;
        let _second_guard = self.acquire_lock(second_id).await;

        // A prior rollback may have failed after one sidecar was published.
        // Recover under the same lexical pair locks before observing either
        // generation; an unresolved journal fails this access closed.
        self.storage
            .recover_task_control_plane_transaction(first_id, second_id)
            .await?;

        let Some(mut local) = self.storage.load_runtime_control_plane(session_id).await? else {
            return Ok(false);
        };
        let Some(mut shared) = self
            .storage
            .load_runtime_control_plane(shared_session_id)
            .await?
        else {
            return Ok(false);
        };
        if local.task_list_version_meta().as_deref() != Some(expected_version)
            || shared.task_list_version_meta().as_deref() != Some(expected_version)
            || !task_list_snapshot_matches(&local, expected_task_list)?
            || !task_list_snapshot_matches(&shared, expected_task_list)?
        {
            return Ok(false);
        }

        let local_original = local.clone();
        let shared_original = shared.clone();
        // The paired persistence port owns only Task list/generation. Keep the
        // surrounding session snapshot byte-stable so its minimal undo journal
        // never needs transcript or unrelated metadata.
        local.task_list = Some(task_list.clone());
        local.set_task_list_version_meta(version.to_string());
        shared.task_list = Some(task_list.clone());
        shared.set_task_list_version_meta(version.to_string());
        let (first_original, first_updated, second_original, second_updated) =
            if session_id < shared_session_id {
                (&local_original, &local, &shared_original, &shared)
            } else {
                (&shared_original, &shared, &local_original, &local)
            };
        let committed = self
            .storage
            .save_task_control_planes_atomically(
                first_original,
                first_updated,
                second_original,
                second_updated,
            )
            .await?;
        if !committed {
            return Ok(false);
        }
        publish(&local, &shared);
        Ok(true)
    }

    /// Authoritative metadata commit.
    ///
    /// The caller must have already loaded the latest session, mutated the
    /// metadata fields, and bumped `metadata_version` (and `title_version` if
    /// applicable).  This method simply acquires the per-session lock and
    /// performs a plain `storage.save_session`.
    ///
    /// The lock guarantees that no other write for this session interleaves
    /// between the caller's load and this save, so merge is unnecessary.
    pub async fn commit_metadata(&self, session: &Session) -> std::io::Result<()> {
        let _guard = self.acquire_lock(&session.id).await;
        let mut committed = session.clone();
        if let Some(latest) = self.storage.load_runtime_control_plane(&session.id).await? {
            adopt_durable_model_context_state(&mut committed, &latest);
        }
        self.save_session_rebasing_task_conflicts(&mut committed)
            .await
    }

    /// Runtime / non-authoritative save with per-session lock.
    ///
    /// Inside the lock: reload disk, merge the authoritative metadata group
    /// (`title`, `title_version`, `title_generated`, `pinned`, `metadata_version`) from disk into
    /// the in-memory copy if disk's `metadata_version >= session.metadata_version`,
    /// then save.
    ///
    /// This is the locked equivalent of [`merge_save_session`]; prefer it for
    /// server-side paths where an authoritative write may race with this save.
    ///
    /// Adopts the on-disk typed permission mode so a running loop's save can't
    /// revert a concurrent `PATCH /sessions` transition (#540/#770). Callers
    /// that are themselves the authoritative writer of that posture — the
    /// parent seeding a child's mode (#74) — must use
    /// [`Self::save_runtime_authoritative_flags`] instead, which persists the
    /// in-memory mode as-is.
    pub async fn merge_save_runtime(&self, session: &mut Session) -> std::io::Result<()> {
        self.merge_save_runtime_and_publish(session, |_, _| {})
            .await
    }

    /// Merge-save a runtime session and synchronously publish the resulting
    /// snapshot before releasing its per-session serialization lock.
    ///
    /// The callback receives whether the durable save committed. It always runs
    /// after the save attempt so repository callers can preserve their existing
    /// cache-on-failure policy without reopening a durable-to-cache race.
    pub async fn merge_save_runtime_and_publish<F>(
        &self,
        session: &mut Session,
        publish: F,
    ) -> std::io::Result<()>
    where
        F: FnOnce(&Session, bool) + Send,
    {
        self.merge_save_runtime_inner_and_publish(session, true, publish)
            .await
    }

    /// Persist an execute-boundary transcript checkpoint without allowing a
    /// stale runner snapshot to shrink or rewrite the durable message log.
    ///
    /// The latest load, append-only message reconciliation, metadata merge and
    /// save all happen while holding the same per-session lock.  Loading is
    /// deliberately fail-closed: falling back to a blind full save when the
    /// latest transcript cannot be read would reintroduce the SHRINK hazard
    /// this checkpoint exists to prevent.
    pub async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.checkpoint_runtime_session_and_publish(session, |_, _| {})
            .await
    }

    /// Checkpoint a runtime session and publish its reconciled snapshot before
    /// releasing the same per-session serialization lock.
    ///
    /// The callback runs after the durable save attempt and receives its commit
    /// status. A load failure returns before publication, matching the
    /// checkpoint's fail-closed behavior.
    pub async fn checkpoint_runtime_session_and_publish<F>(
        &self,
        session: &mut Session,
        publish: F,
    ) -> std::io::Result<()>
    where
        F: FnOnce(&Session, bool) + Send,
    {
        let _guard = self.acquire_lock(&session.id).await;
        let latest = self.storage.load_session(&session.id).await?;

        if let Some(latest) = latest.as_ref() {
            ensure_model_context_checkpoint_is_current(session, latest)?;
            let incoming_count = session.messages.len();
            let durable_count = latest.messages.len();
            let appended = bamboo_domain::append_missing_runtime_messages(session, latest);
            bamboo_domain::merge_session_inbox_admission(session, latest);
            tracing::debug!(
                "[{}] append-safe runtime checkpoint: durable={}, incoming={}, appended={}, saved={}",
                session.id,
                durable_count,
                incoming_count,
                appended,
                session.messages.len(),
            );
            apply_authoritative_metadata(session, latest);
            adopt_fresher_disk_permission_posture(session, latest);
        }

        let result = self.save_session_rebasing_task_conflicts(session).await;
        publish(session, result.is_ok());
        result
    }

    /// Like [`Self::merge_save_runtime`] but does NOT adopt the on-disk
    /// permission mode — the caller's in-memory value is authoritative and
    /// persists as-is.
    ///
    /// For parent-side control writes to a child session (e.g. the #74
    /// resident-reuse posture re-seed), which set the flag deliberately and must
    /// not be reverted by the disk-wins protection meant for a running loop's
    /// own stale saves. Still merges the authoritative metadata group.
    pub async fn save_runtime_authoritative_flags(
        &self,
        session: &mut Session,
    ) -> std::io::Result<()> {
        self.merge_save_runtime_inner_and_publish(session, false, |_, _| {})
            .await
    }

    async fn merge_save_runtime_inner_and_publish<F>(
        &self,
        session: &mut Session,
        adopt_bypass: bool,
        publish: F,
    ) -> std::io::Result<()>
    where
        F: FnOnce(&Session, bool) + Send,
    {
        let _guard = self.acquire_lock(&session.id).await;

        // Single disk read serves BOTH the SHRINK diagnostic and the
        // authoritative-metadata merge below. Previously this path loaded the
        // session twice (once here, once inside the merge helper); on a parent
        // session carrying the full conversation history that doubled the
        // deserialization cost of every runtime save, which is the hot path
        // during sub-agent spawn.
        let latest = self.storage.load_session(&session.id).await?;

        // DIAGNOSTIC: merge_save_runtime overwrites the whole `messages` array
        // (it only merges authoritative metadata, not messages). If the incoming
        // session is stale (fewer messages than what is already on disk), this save
        // silently reverts a concurrent append (e.g. a just-persisted user message).
        // Log a SHRINK warning so we can identify the stale writer.
        let existing_message_count = latest.as_ref().map(|s| s.messages.len());
        let incoming_message_count = session.messages.len();
        if existing_message_count.is_some_and(|existing| existing > incoming_message_count) {
            tracing::warn!(
                "[{}] merge_save_runtime SHRINK: disk has {:?} messages, saving {} (last_role={:?}, updated_at={}); a stale writer is reverting a concurrent append",
                session.id,
                existing_message_count,
                incoming_message_count,
                session.messages.last().map(|m| format!("{:?}", m.role)),
                session.updated_at,
            );
        } else {
            tracing::debug!(
                "[{}] merge_save_runtime: disk={:?} messages, saving {} (updated_at={})",
                session.id,
                existing_message_count,
                incoming_message_count,
                session.updated_at,
            );
        }

        if let Some(latest) = latest.as_ref() {
            apply_authoritative_metadata(session, latest);
            let restored = bamboo_domain::restore_missing_admitted_inbox_messages(session, latest);
            if restored > 0 {
                tracing::warn!(
                    session_id = %session.id,
                    restored,
                    "restored durable SessionInbox transcript messages into stale runtime save"
                );
            }
            bamboo_domain::merge_session_inbox_admission(session, latest);
            // Never let a running loop's save revert a concurrent mid-run
            // `PATCH /sessions {permission_mode|bypass_permissions}` transition.
            // #540/#770. Skipped for
            // authoritative flag writers (`save_runtime_authoritative_flags`).
            if adopt_bypass {
                adopt_fresher_disk_permission_posture(session, latest);
            }
            adopt_fresher_durable_model_context_state(session, latest);
        }
        let result = self.save_session_rebasing_task_conflicts(session).await;
        publish(session, result.is_ok());
        result
    }

    /// Persist one validated RunSpec activation as the exact authority for the
    /// worker's requested posture and complete audit record.
    ///
    /// Warm workers reuse a durable session id. An ordinary runtime save is
    /// intentionally disk-adopting, so using it here would let the previous
    /// activation's posture stick. This dedicated transaction preserves only
    /// durable UI metadata and SessionInbox admission/transcript proof, then
    /// writes the incoming posture with an audit revision above the durable
    /// floor while holding the same per-session lock.
    pub async fn seed_runtime_activation_and_publish<F>(
        &self,
        session: &mut Session,
        publish: F,
    ) -> std::io::Result<()>
    where
        F: FnOnce(&Session, bool) + Send,
    {
        let _guard = self.acquire_lock(&session.id).await;
        let mut incoming_audit = PermissionAuditSnapshot::from_metadata(&session.metadata)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "activation seed requires a complete permission audit record",
                )
            })?;

        if let Some(latest) = self.storage.load_session(&session.id).await? {
            apply_authoritative_metadata(session, &latest);
            bamboo_domain::restore_missing_admitted_inbox_messages(session, &latest);
            bamboo_domain::merge_session_inbox_admission(session, &latest);
            adopt_fresher_durable_model_context_state(session, &latest);

            let durable_audit = PermissionAuditSnapshot::from_metadata(&latest.metadata);
            let durable_floor = durable_audit
                .as_ref()
                .map(|snapshot| snapshot.audit_revision)
                .unwrap_or_default();
            if let Some(durable_audit) = durable_audit {
                if durable_audit.resolution == incoming_audit.resolution {
                    incoming_audit.transitioned_at = durable_audit.transitioned_at;
                }
            }
            incoming_audit.audit_revision = bamboo_domain::next_permission_audit_revision_after(
                durable_floor.max(incoming_audit.audit_revision),
            )
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
            })?;
        }

        session
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .set_permission_mode(incoming_audit.resolution.requested);
        incoming_audit.write_to(&mut session.metadata);

        let result = self.save_session_rebasing_task_conflicts(session).await;
        publish(session, result.is_ok());
        result
    }

    /// Atomically re-seed a resident child from its parent posture.
    ///
    /// The latest session load, typed-mode comparison, complete audit refresh,
    /// metadata CAS bump (only for a true typed transition), narrow companion
    /// mutation, save, and cache publication share one session lock.
    pub async fn update_authoritative_permission_posture_and_publish<M, P>(
        &self,
        session_id: &str,
        seed: &PermissionAuditSeed,
        mutate: M,
        publish: P,
    ) -> std::io::Result<Option<Session>>
    where
        M: FnOnce(&mut Session),
        P: FnOnce(&Session),
    {
        let _guard = self.acquire_lock(session_id).await;
        let Some(mut latest) = self.storage.load_session(session_id).await? else {
            return Ok(None);
        };
        let previous_mode = latest
            .agent_runtime_state
            .as_ref()
            .map(|state| state.effective_permission_mode())
            .unwrap_or_default();
        let previous_resolution = PermissionAuditSnapshot::from_metadata(&latest.metadata)
            .map(|snapshot| snapshot.resolution);
        mutate(&mut latest);
        latest
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .set_permission_mode(seed.resolution.requested);
        let mode_changed = previous_mode != seed.resolution.requested;
        let posture_changed = previous_resolution != Some(seed.resolution);
        let transitioned_at = posture_changed.then(|| chrono::Utc::now().to_rfc3339());
        bamboo_domain::record_permission_audit(
            &mut latest.metadata,
            seed,
            transitioned_at.as_deref(),
        )
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
        if mode_changed {
            latest.metadata_version = latest.metadata_version.saturating_add(1);
        }
        self.save_session_rebasing_task_conflicts(&mut latest)
            .await?;
        publish(&latest);
        Ok(Some(latest))
    }

    /// Persist a worker's bounded executor mapping only when the exact host
    /// posture observed before dispatch is still current. The remote event does
    /// not contribute an audit revision or transition timestamp: both are
    /// allocated from the latest durable record while this session lock is held.
    pub async fn record_permission_posture_activation_and_publish<P>(
        &self,
        session_id: &str,
        expected_audit_revision: Option<u64>,
        seed: &PermissionAuditSeed,
        publish: P,
    ) -> std::io::Result<Option<Session>>
    where
        P: FnOnce(&Session),
    {
        let _guard = self.acquire_lock(session_id).await;
        let Some(mut latest) = self.storage.load_session(session_id).await? else {
            return Ok(None);
        };
        let durable_audit = PermissionAuditSnapshot::from_metadata(&latest.metadata);
        let durable_revision = durable_audit
            .as_ref()
            .map(|snapshot| snapshot.audit_revision);
        if durable_revision != expected_audit_revision {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stale permission posture activation: durable audit changed after dispatch",
            ));
        }
        let durable_requested = latest
            .agent_runtime_state
            .as_ref()
            .map(|state| state.effective_permission_mode())
            .unwrap_or_default();
        if durable_requested != seed.resolution.requested || !seed.resolution.is_consistent() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stale or inconsistent permission posture activation",
            ));
        }
        bamboo_domain::record_permission_audit(&mut latest.metadata, seed, None).map_err(
            |error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
        )?;
        self.save_session_rebasing_task_conflicts(&mut latest)
            .await?;
        publish(&latest);
        Ok(Some(latest))
    }

    /// Apply a config-only mutation to a session without ever clobbering its
    /// `messages` (or other concurrently-written state).
    ///
    /// Unlike [`Self::merge_save_runtime`], the caller does NOT pass a session
    /// snapshot. Instead this loads the **latest** session from storage *inside*
    /// the per-session lock, applies `mutate` (intended for small config fields
    /// like `model_ref` / `reasoning_effort`), and saves. Because the load and
    /// save both happen under the lock, a concurrent append (e.g. `POST /chat`
    /// adding a user message) can never be reverted by this write.
    ///
    /// Returns the saved session, or `None` if it does not exist.
    pub async fn update_runtime_config<F>(
        &self,
        session_id: &str,
        mutate: F,
    ) -> std::io::Result<Option<Session>>
    where
        F: FnOnce(&mut Session),
    {
        self.update_runtime_config_and_publish(session_id, mutate, |_| {})
            .await
    }

    /// Apply a config-only mutation and synchronously publish the saved
    /// snapshot before releasing the session lock.
    pub async fn update_runtime_config_and_publish<M, P>(
        &self,
        session_id: &str,
        mutate: M,
        publish: P,
    ) -> std::io::Result<Option<Session>>
    where
        M: FnOnce(&mut Session),
        P: FnOnce(&Session),
    {
        let _guard = self.acquire_lock(session_id).await;
        let Some(mut session) = self.storage.load_session(session_id).await? else {
            return Ok(None);
        };
        mutate(&mut session);
        self.save_session_rebasing_task_conflicts(&mut session)
            .await?;
        publish(&session);
        Ok(Some(session))
    }

    /// Clear the legacy compatibility queue using durable CAS and publish the
    /// saved full snapshot before releasing the same session lock.
    pub async fn clear_legacy_pending_messages_and_publish<F>(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
        publish: F,
    ) -> std::io::Result<bool>
    where
        F: FnOnce(&Session) + Send,
    {
        let _guard = self.acquire_lock(session_id).await;
        let Some(mut latest) = self.storage.load_session(session_id).await? else {
            return Ok(false);
        };
        if latest.pending_injected_messages().as_deref() != Some(expected) {
            return Ok(false);
        }
        latest.clear_pending_injected_messages();
        self.save_runtime_state_rebasing_task_conflicts(&mut latest)
            .await?;
        publish(&latest);
        Ok(true)
    }
}

/// Infrastructure implementation of the domain runtime-persistence port.
/// Server should assemble this as `Arc<dyn RuntimeSessionPersistence>` and must
/// not define a separate adapter layer for the same behavior.
#[async_trait::async_trait]
impl RuntimeSessionPersistence for LockedSessionStore {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.merge_save_runtime(session).await
    }

    async fn seed_runtime_activation(&self, session: &mut Session) -> std::io::Result<()> {
        self.seed_runtime_activation_and_publish(session, |_, _| {})
            .await
    }

    async fn record_permission_posture_activation(
        &self,
        session_id: &str,
        expected_audit_revision: Option<u64>,
        seed: &PermissionAuditSeed,
    ) -> std::io::Result<Option<Session>> {
        self.record_permission_posture_activation_and_publish(
            session_id,
            expected_audit_revision,
            seed,
            |_| {},
        )
        .await
    }

    async fn save_runtime_control_plane(&self, session: &mut Session) -> std::io::Result<()> {
        self.save_runtime_only(session).await
    }

    async fn load_runtime_control_plane(
        &self,
        session_id: &str,
    ) -> std::io::Result<Option<Session>> {
        self.storage.load_runtime_control_plane(session_id).await
    }

    async fn update_task_list_control_plane(
        &self,
        session_id: &str,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> std::io::Result<bool> {
        self.update_task_list_control_plane_and_publish(session_id, task_list, version, |_| {})
            .await
    }

    async fn update_task_list_control_plane_if_version(
        &self,
        session_id: &str,
        expected_version: &str,
        expected_task_list: &bamboo_domain::TaskList,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> std::io::Result<bool> {
        self.update_task_list_control_plane_if_version_and_publish(
            session_id,
            expected_version,
            expected_task_list,
            task_list,
            version,
            |_| {},
        )
        .await
    }

    async fn update_task_list_control_planes_if_version(
        &self,
        session_id: &str,
        shared_session_id: &str,
        expected_version: &str,
        expected_task_list: &bamboo_domain::TaskList,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> std::io::Result<bool> {
        self.update_task_list_control_planes_if_version_and_publish(
            session_id,
            shared_session_id,
            expected_version,
            expected_task_list,
            task_list,
            version,
            |_, _| {},
        )
        .await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        LockedSessionStore::checkpoint_runtime_session(self, session).await
    }

    async fn load_runtime_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        self.storage.load_session(session_id).await
    }

    async fn clear_legacy_pending_messages(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
    ) -> std::io::Result<bool> {
        self.clear_legacy_pending_messages_and_publish(session_id, expected, |_| {})
            .await
    }
}

// ── Internal merge helper ─────────────────────────────────────────────

/// Re-read the on-disk session and, when the disk copy carries a
/// `metadata_version >= session.metadata_version`, overwrite the in-memory
/// authoritative metadata fields with the disk values.
///
/// This is the core staleness-correction: non-authoritative writers call it
/// before saving so they don't accidentally revert a concurrent UI edit.
async fn merge_authoritative_metadata_into_stale(
    storage: &Arc<dyn Storage>,
    session: &mut Session,
) -> std::io::Result<()> {
    if let Some(latest) = storage.load_session(&session.id).await? {
        apply_authoritative_metadata(session, &latest);
        bamboo_domain::restore_missing_admitted_inbox_messages(session, &latest);
        bamboo_domain::merge_session_inbox_admission(session, &latest);
        adopt_fresher_disk_permission_posture(session, &latest);
        adopt_fresher_durable_model_context_state(session, &latest);
    }
    Ok(())
}

/// Runtime-only writes never own the ledger, so the snapshot loaded under the
/// session lock is authoritative even if the caller happens to carry a larger
/// revision. This keeps both the sidecar and synchronous cache publication from
/// regressing after a concurrent engine checkpoint.
fn adopt_durable_model_context_state(session: &mut Session, latest: &Session) {
    session
        .model_context_state
        .clone_from(&latest.model_context_state);
}

/// Merge policy for ordinary full runtime saves. Legitimate ledger writers
/// (compression/rollback/reconciliation) advance `state_revision`; stale or
/// conflicting non-ledger writers adopt the already-committed state. Equal
/// revisions with different bytes are concurrent children of the same base, so
/// durable state wins instead of allowing last-writer-wins corruption.
fn adopt_fresher_durable_model_context_state(session: &mut Session, latest: &Session) {
    let adopt = match (
        session.model_context_state.as_ref(),
        latest.model_context_state.as_ref(),
    ) {
        (None, Some(_)) => true,
        (Some(incoming), Some(durable)) => {
            durable.state_revision > incoming.state_revision
                || (durable.state_revision == incoming.state_revision && durable != incoming)
        }
        _ => false,
    };
    if adopt {
        adopt_durable_model_context_state(session, latest);
    }
}

/// A provider-bound ledger checkpoint must never silently substitute a newer or
/// conflicting disk ledger after the request body was prepared. Reject it so
/// the engine can roll back the in-memory candidate and retry from a fresh
/// session; only a strictly newer incoming revision may replace disk.
fn ensure_model_context_checkpoint_is_current(
    session: &Session,
    latest: &Session,
) -> std::io::Result<()> {
    let stale_or_conflicting = match (
        session.model_context_state.as_ref(),
        latest.model_context_state.as_ref(),
    ) {
        (None, Some(_)) => true,
        (Some(incoming), Some(durable)) => {
            durable.state_revision > incoming.state_revision
                || (durable.state_revision == incoming.state_revision && durable != incoming)
        }
        _ => false,
    };
    if stale_or_conflicting {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "stale or conflicting model-context ledger checkpoint",
        ));
    }
    Ok(())
}

/// Adopt the on-disk typed permission posture into the session about to be
/// saved when the durable posture is semantically fresher.
///
/// `PATCH /sessions {permission_mode|bypass_permissions}` is the authoritative
/// writer of this posture (a running loop only carries it forward from run
/// start). Without this, a runtime save from an in-flight run — which holds the
/// run-start value — silently reverts a concurrent mid-run transition on disk.
/// A true typed-mode difference always represents an authoritative durable
/// transition. When the modes are equal, the complete audit revision is the
/// ordering fence: an older/missing disk audit must never delete a newer
/// run-start policy/mapping refresh. #540/#770.
fn adopt_fresher_disk_permission_posture(session: &mut Session, latest: &Session) {
    // A disk copy with NO runtime state at all carries no authoritative mode
    // value — treat it as "unknown" and leave the in-memory flag untouched,
    // rather than forcing it OFF (which would silently disable a legitimately
    // bypassed run on any backend/path that doesn't round-trip the field). #540.
    let Some(disk_mode) = latest
        .agent_runtime_state
        .as_ref()
        .map(|state| state.effective_permission_mode())
    else {
        return;
    };
    let current_mode = session
        .agent_runtime_state
        .as_ref()
        .map(|state| state.effective_permission_mode())
        .unwrap_or_default();
    let Some(disk_audit) = bamboo_domain::fresher_disk_permission_audit(
        current_mode,
        &session.metadata,
        disk_mode,
        &latest.metadata,
    ) else {
        return;
    };

    match session.agent_runtime_state.as_mut() {
        Some(state) => state.set_permission_mode(disk_mode),
        // No runtime state in memory and disk says "off" → nothing to adopt;
        // avoid allocating a default state just to store `false`.
        None if disk_mode != bamboo_domain::SessionPermissionMode::Default => {
            let state = session
                .agent_runtime_state
                .get_or_insert_with(bamboo_domain::AgentRuntimeState::default);
            state.set_permission_mode(disk_mode);
        }
        None => {}
    }

    // The typed posture and its complete bounded audit record move together.
    disk_audit.write_to(&mut session.metadata);
}

/// Pure merge step: given a freshly-loaded on-disk copy, overwrite the
/// in-memory authoritative metadata group when disk's `metadata_version` is at
/// least the in-memory one. Split out so callers that have already loaded the
/// disk copy (e.g. [`LockedSessionStore::merge_save_runtime`]) don't pay for a
/// second read.
fn apply_authoritative_metadata(session: &mut Session, latest: &Session) {
    if latest.metadata_version >= session.metadata_version {
        session.title = latest.title.clone();
        session.title_version = latest.title_version;
        session.title_generated = latest.title_generated;
        session.pinned = latest.pinned;
        for key in AUTHORITATIVE_METADATA_KEYS {
            if let Some(value) = latest.metadata.get(*key) {
                session.metadata.insert((*key).to_string(), value.clone());
            } else {
                session.metadata.remove(*key);
            }
        }
        session.metadata_version = latest.metadata_version;
    }
}

// ── Free merge-save function ──────────────────────────────────────────

/// Save a session while preserving any concurrent UI edits to the
/// authoritative metadata group.
///
/// Behaviour: if the on-disk session has `metadata_version >=
/// session.metadata_version`, the on-disk `title`, `title_version`, `title_generated`, `pinned`
/// and `metadata_version` overwrite the in-memory values before writing.
///
/// This is the stateless variant (no per-session lock). Prefer
/// [`LockedSessionStore::merge_save_runtime`] for server-side paths where an
/// authoritative writer may race with this save.
pub async fn merge_save_session(
    storage: &Arc<dyn Storage>,
    session: &mut Session,
) -> std::io::Result<()> {
    merge_authoritative_metadata_into_stale(storage, session).await?;
    storage.save_session(session).await
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{RuntimeTaskTransactionFault, SessionStoreV2};
    use bamboo_domain::{session::types::Session, PermissionMode};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct CountingControlPlaneStorage {
        inner: Arc<SessionStoreV2>,
        control_plane_loads: AtomicUsize,
        full_saves: AtomicUsize,
        runtime_state_saves: AtomicUsize,
    }

    struct PairCommitBarrierStorage {
        inner: Arc<SessionStoreV2>,
        before_commit: Arc<tokio::sync::Barrier>,
    }

    struct SingleCommitBarrierStorage {
        inner: Arc<SessionStoreV2>,
        before_commit: Arc<tokio::sync::Barrier>,
    }

    struct SingleCommitPauseStorage {
        inner: Arc<SessionStoreV2>,
        commit_reached: Arc<tokio::sync::Barrier>,
        release_commit: Arc<tokio::sync::Barrier>,
    }

    #[async_trait::async_trait]
    impl Storage for SingleCommitPauseStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.inner.save_session(session).await
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            self.inner.load_session(session_id).await
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            self.inner.delete_session(session_id).await
        }

        async fn load_runtime_control_plane(
            &self,
            session_id: &str,
        ) -> std::io::Result<Option<Session>> {
            self.inner.load_runtime_control_plane(session_id).await
        }

        async fn save_task_control_plane_if_matches(
            &self,
            original: &Session,
            updated: &Session,
        ) -> std::io::Result<bool> {
            self.commit_reached.wait().await;
            self.release_commit.wait().await;
            self.inner
                .save_task_control_plane_if_matches(original, updated)
                .await
        }
    }

    #[async_trait::async_trait]
    impl Storage for SingleCommitBarrierStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.inner.save_session(session).await
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            self.inner.load_session(session_id).await
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            self.inner.delete_session(session_id).await
        }

        async fn load_runtime_control_plane(
            &self,
            session_id: &str,
        ) -> std::io::Result<Option<Session>> {
            self.inner.load_runtime_control_plane(session_id).await
        }

        async fn save_task_control_plane_if_matches(
            &self,
            original: &Session,
            updated: &Session,
        ) -> std::io::Result<bool> {
            self.before_commit.wait().await;
            self.inner
                .save_task_control_plane_if_matches(original, updated)
                .await
        }
    }

    #[async_trait::async_trait]
    impl Storage for PairCommitBarrierStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.inner.save_session(session).await
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            self.inner.load_session(session_id).await
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            self.inner.delete_session(session_id).await
        }

        async fn save_runtime_state(&self, session: &Session) -> std::io::Result<()> {
            self.inner.save_runtime_state(session).await
        }

        async fn load_runtime_control_plane(
            &self,
            session_id: &str,
        ) -> std::io::Result<Option<Session>> {
            self.inner.load_runtime_control_plane(session_id).await
        }

        async fn recover_task_control_plane_transaction(
            &self,
            first_session_id: &str,
            second_session_id: &str,
        ) -> std::io::Result<()> {
            self.inner
                .recover_task_control_plane_transaction(first_session_id, second_session_id)
                .await
        }

        async fn save_task_control_planes_atomically(
            &self,
            first_original: &Session,
            first_updated: &Session,
            second_original: &Session,
            second_updated: &Session,
        ) -> std::io::Result<bool> {
            // Both independent LockedSessionStores have already recovered,
            // loaded, and validated v1 when they meet here. Their per-instance
            // lexical locks cannot serialize each other; the V2 commit-point
            // revalidation must select exactly one winner.
            self.before_commit.wait().await;
            self.inner
                .save_task_control_planes_atomically(
                    first_original,
                    first_updated,
                    second_original,
                    second_updated,
                )
                .await
        }
    }

    #[async_trait::async_trait]
    impl Storage for CountingControlPlaneStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.full_saves.fetch_add(1, Ordering::SeqCst);
            self.inner.save_session(session).await
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            self.inner.load_session(session_id).await
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            self.inner.delete_session(session_id).await
        }

        async fn save_runtime_state(&self, session: &Session) -> std::io::Result<()> {
            self.runtime_state_saves.fetch_add(1, Ordering::SeqCst);
            self.inner.save_runtime_state(session).await
        }

        async fn load_runtime_control_plane(
            &self,
            session_id: &str,
        ) -> std::io::Result<Option<Session>> {
            self.control_plane_loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_runtime_control_plane(session_id).await
        }

        async fn recover_task_control_plane_transaction(
            &self,
            first_session_id: &str,
            second_session_id: &str,
        ) -> std::io::Result<()> {
            self.inner
                .recover_task_control_plane_transaction(first_session_id, second_session_id)
                .await
        }

        async fn save_task_control_plane_if_matches(
            &self,
            original: &Session,
            updated: &Session,
        ) -> std::io::Result<bool> {
            let committed = self
                .inner
                .save_task_control_plane_if_matches(original, updated)
                .await?;
            if committed {
                self.runtime_state_saves.fetch_add(1, Ordering::SeqCst);
            }
            Ok(committed)
        }

        async fn save_task_control_planes_atomically(
            &self,
            first_original: &Session,
            first_updated: &Session,
            second_original: &Session,
            second_updated: &Session,
        ) -> std::io::Result<bool> {
            let committed = self
                .inner
                .save_task_control_planes_atomically(
                    first_original,
                    first_updated,
                    second_original,
                    second_updated,
                )
                .await?;
            if committed {
                self.runtime_state_saves.fetch_add(2, Ordering::SeqCst);
            }
            Ok(committed)
        }
    }

    async fn make_storage() -> (tempfile::TempDir, Arc<dyn Storage>) {
        let temp = tempfile::tempdir().unwrap();
        let storage = SessionStoreV2::new(temp.path().to_path_buf())
            .await
            .expect("storage init");
        (temp, Arc::new(storage) as Arc<dyn Storage>)
    }

    fn fresh(id: &str) -> Session {
        Session::new(id.to_string(), "test-model".to_string())
    }

    fn ledger_state(state_revision: u64, marker: &str) -> bamboo_domain::ModelContextState {
        bamboo_domain::ModelContextState {
            state_revision,
            prefix_epoch: state_revision,
            cache_scope_sha256: Some("scope".to_string()),
            transcript_item_sha256: vec![marker.to_string()],
            ..bamboo_domain::ModelContextState::default()
        }
    }

    fn set_permission_audit(
        session: &mut Session,
        requested: bamboo_domain::SessionPermissionMode,
        policy_revision: u64,
        mapping: &str,
        transitioned_at: &str,
    ) -> u64 {
        let resolution = bamboo_domain::resolve_permission_mode(requested, PermissionMode::Default);
        session
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .set_permission_mode(requested);
        bamboo_domain::record_permission_audit(
            &mut session.metadata,
            &PermissionAuditSeed::new(policy_revision, resolution, mapping),
            Some(transitioned_at),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn same_mode_newer_run_start_audit_survives_every_runtime_save_path() {
        for path in ["merge", "checkpoint", "control-plane"] {
            let (_temp, storage) = make_storage().await;
            let store = LockedSessionStore::new(storage.clone());
            let session_id = format!("same-mode-newer-{path}");
            let mut durable = fresh(&session_id);
            set_permission_audit(
                &mut durable,
                bamboo_domain::SessionPermissionMode::Default,
                1,
                "bamboo_runtime:old-policy",
                "2026-07-31T12:00:00Z",
            );
            storage.save_session(&durable).await.unwrap();

            let mut run_start = durable.clone();
            let old_revision = PermissionAuditSnapshot::from_metadata(&durable.metadata)
                .unwrap()
                .audit_revision;
            let new_revision = set_permission_audit(
                &mut run_start,
                bamboo_domain::SessionPermissionMode::Default,
                2,
                "bamboo_runtime:new-policy",
                "2026-07-31T12:00:00Z",
            );
            assert!(new_revision > old_revision);

            match path {
                "merge" => store.merge_save_runtime(&mut run_start).await.unwrap(),
                "checkpoint" => store
                    .checkpoint_runtime_session(&mut run_start)
                    .await
                    .unwrap(),
                "control-plane" => store.save_runtime_only(&mut run_start).await.unwrap(),
                _ => unreachable!(),
            }

            let saved = storage.load_session(&session_id).await.unwrap().unwrap();
            let audit = PermissionAuditSnapshot::from_metadata(&saved.metadata).unwrap();
            assert_eq!(audit.audit_revision, new_revision, "path={path}");
            assert_eq!(audit.policy_revision, 2, "path={path}");
            assert_eq!(audit.executor_mapping, "bamboo_runtime:new-policy");
        }
    }

    #[tokio::test]
    async fn newer_disk_transition_wins_after_mode_cycles_back() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "permission-cycle-back";
        let mut baseline = fresh(session_id);
        let stale_revision = set_permission_audit(
            &mut baseline,
            bamboo_domain::SessionPermissionMode::Default,
            1,
            "bamboo_runtime:initial",
            "2026-07-31T12:00:00Z",
        );
        storage.save_session(&baseline).await.unwrap();
        let mut stale_runtime = baseline.clone();

        let mut durable = baseline;
        set_permission_audit(
            &mut durable,
            bamboo_domain::SessionPermissionMode::Auto,
            2,
            "bamboo_runtime:auto",
            "2026-07-31T12:01:00Z",
        );
        let durable_revision = set_permission_audit(
            &mut durable,
            bamboo_domain::SessionPermissionMode::Default,
            3,
            "bamboo_runtime:cycled-default",
            "2026-07-31T12:02:00Z",
        );
        assert!(durable_revision > stale_revision);
        storage.save_session(&durable).await.unwrap();

        store.merge_save_runtime(&mut stale_runtime).await.unwrap();
        let saved = storage.load_session(session_id).await.unwrap().unwrap();
        let audit = PermissionAuditSnapshot::from_metadata(&saved.metadata).unwrap();
        assert_eq!(audit.audit_revision, durable_revision);
        assert_eq!(audit.policy_revision, 3);
        assert_eq!(audit.executor_mapping, "bamboo_runtime:cycled-default");
    }

    #[tokio::test]
    async fn authoritative_activation_seed_replaces_every_warm_worker_posture() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "warm-permission-matrix";
        let cases = [
            (
                bamboo_domain::SessionPermissionMode::Auto,
                PermissionMode::Default,
                PermissionMode::Auto,
            ),
            (
                bamboo_domain::SessionPermissionMode::Default,
                PermissionMode::Default,
                PermissionMode::Default,
            ),
            (
                bamboo_domain::SessionPermissionMode::Auto,
                PermissionMode::Default,
                PermissionMode::Auto,
            ),
            (
                bamboo_domain::SessionPermissionMode::Bypass,
                PermissionMode::Auto,
                PermissionMode::BypassPermissions,
            ),
        ];
        let mut previous_revision = 0;

        for (index, (requested, configured, expected_effective)) in cases.into_iter().enumerate() {
            let mut activation = fresh(session_id);
            activation
                .agent_runtime_state
                .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
                .set_permission_mode(requested);
            let resolution = bamboo_domain::resolve_permission_mode(requested, configured);
            bamboo_domain::record_permission_audit(
                &mut activation.metadata,
                &PermissionAuditSeed::new(
                    index as u64 + 1,
                    resolution,
                    format!("bamboo_worker:{}", resolution.effective.as_str()),
                ),
                Some("2026-07-31T12:00:00Z"),
            )
            .unwrap();

            RuntimeSessionPersistence::seed_runtime_activation(&store, &mut activation)
                .await
                .unwrap();
            let durable = storage.load_session(session_id).await.unwrap().unwrap();
            assert_eq!(
                durable
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .effective_permission_mode(),
                requested,
                "activation {index}"
            );
            let audit = PermissionAuditSnapshot::from_metadata(&durable.metadata).unwrap();
            assert_eq!(audit.resolution.requested, requested);
            assert_eq!(audit.resolution.effective, expected_effective);
            assert!(audit.audit_revision > previous_revision);
            previous_revision = audit.audit_revision;
        }
    }

    #[tokio::test]
    async fn resident_reseed_bumps_etag_only_for_typed_transition() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "resident-atomic-permission";
        let mut baseline = fresh(session_id);
        baseline.metadata_version = 7;
        set_permission_audit(
            &mut baseline,
            bamboo_domain::SessionPermissionMode::Auto,
            1,
            "bamboo_runtime:auto",
            "2026-07-31T12:00:00Z",
        );
        storage.save_session(&baseline).await.unwrap();
        let initial_audit = PermissionAuditSnapshot::from_metadata(&baseline.metadata).unwrap();

        let same_mode_seed = PermissionAuditSeed::bamboo_runtime(
            2,
            bamboo_domain::resolve_permission_mode(
                bamboo_domain::SessionPermissionMode::Auto,
                PermissionMode::Default,
            ),
        );
        let refreshed = store
            .update_authoritative_permission_posture_and_publish(
                session_id,
                &same_mode_seed,
                |session| {
                    session
                        .metadata
                        .insert("resident.marker".to_string(), "same-mode".to_string());
                },
                |_| {},
            )
            .await
            .unwrap()
            .unwrap();
        let refreshed_audit = PermissionAuditSnapshot::from_metadata(&refreshed.metadata).unwrap();
        assert_eq!(refreshed.metadata_version, 7);
        assert!(refreshed_audit.audit_revision > initial_audit.audit_revision);
        assert_eq!(refreshed_audit.policy_revision, 2);

        let transition_seed = PermissionAuditSeed::bamboo_runtime(
            3,
            bamboo_domain::resolve_permission_mode(
                bamboo_domain::SessionPermissionMode::Default,
                PermissionMode::Default,
            ),
        );
        let transitioned = store
            .update_authoritative_permission_posture_and_publish(
                session_id,
                &transition_seed,
                |session| {
                    session
                        .metadata
                        .insert("resident.marker".to_string(), "transition".to_string());
                },
                |_| {},
            )
            .await
            .unwrap()
            .unwrap();
        let transitioned_audit =
            PermissionAuditSnapshot::from_metadata(&transitioned.metadata).unwrap();
        assert_eq!(transitioned.metadata_version, 8, "old ETag must be invalid");
        assert_eq!(
            transitioned
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .effective_permission_mode(),
            bamboo_domain::SessionPermissionMode::Default
        );
        assert_eq!(
            transitioned_audit.resolution.requested,
            bamboo_domain::SessionPermissionMode::Default
        );
        assert!(transitioned_audit.audit_revision > refreshed_audit.audit_revision);
        assert_eq!(
            transitioned
                .metadata
                .get("resident.marker")
                .map(String::as_str),
            Some("transition")
        );
    }

    #[tokio::test]
    async fn worker_activation_cas_cannot_overwrite_concurrent_permission_patch() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "permission-activation-cas";
        let mut baseline = fresh(session_id);
        set_permission_audit(
            &mut baseline,
            bamboo_domain::SessionPermissionMode::Default,
            1,
            "bamboo_runtime:default",
            "2026-07-31T12:00:00Z",
        );
        storage.save_session(&baseline).await.unwrap();
        let dispatched_revision = PermissionAuditSnapshot::from_metadata(&baseline.metadata)
            .unwrap()
            .audit_revision;

        let patched_resolution = bamboo_domain::resolve_permission_mode(
            bamboo_domain::SessionPermissionMode::Auto,
            PermissionMode::Default,
        );
        let patched = store
            .update_authoritative_permission_posture_and_publish(
                session_id,
                &PermissionAuditSeed::new(2, patched_resolution, "patch:auto"),
                |_| {},
                |_| {},
            )
            .await
            .unwrap()
            .unwrap();
        let patched_audit = PermissionAuditSnapshot::from_metadata(&patched.metadata).unwrap();
        assert!(patched_audit.audit_revision > dispatched_revision);

        let stale_worker_seed = PermissionAuditSeed::new(
            1,
            bamboo_domain::resolve_permission_mode(
                bamboo_domain::SessionPermissionMode::Default,
                PermissionMode::Default,
            ),
            "worker:stale-default",
        );
        let error = store
            .record_permission_posture_activation_and_publish(
                session_id,
                Some(dispatched_revision),
                &stale_worker_seed,
                |_| {},
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("durable audit changed"));

        let durable = storage.load_session(session_id).await.unwrap().unwrap();
        let durable_audit = PermissionAuditSnapshot::from_metadata(&durable.metadata).unwrap();
        assert_eq!(durable_audit, patched_audit);
        assert_eq!(durable_audit.executor_mapping, "patch:auto");
    }

    // ── update_runtime_config: config patches must never clobber messages ──

    #[tokio::test]
    async fn update_runtime_config_preserves_concurrently_appended_messages() {
        use bamboo_domain::session::types::Message;
        use bamboo_domain::ReasoningEffort;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "cfg-preserve";

        // Persisted baseline: one user + one assistant turn.
        let mut initial = fresh(session_id);
        initial.add_message(Message::user("hello"));
        initial.add_message(Message::assistant("hi", None));
        storage.save_session(&initial).await.unwrap();

        // Simulate `POST /chat` appending a new user message to disk.
        let mut after_chat = storage.load_session(session_id).await.unwrap().unwrap();
        after_chat.add_message(Message::user("second question"));
        storage.save_session(&after_chat).await.unwrap();
        assert_eq!(after_chat.messages.len(), 3);

        // A config-only patch must load the freshest session and preserve the
        // appended message (this is the regression that broke message sending on
        // existing sessions).
        let updated = store
            .update_runtime_config(session_id, |s| {
                s.reasoning_effort = Some(ReasoningEffort::Max);
            })
            .await
            .unwrap()
            .expect("session exists");

        assert_eq!(updated.reasoning_effort, Some(ReasoningEffort::Max));
        assert_eq!(
            updated.messages.len(),
            3,
            "config patch must not revert a concurrently-appended message"
        );

        let on_disk = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(on_disk.messages.len(), 3);
        assert_eq!(on_disk.reasoning_effort, Some(ReasoningEffort::Max));
    }

    #[tokio::test]
    async fn update_runtime_config_returns_none_for_missing_session() {
        use bamboo_domain::ReasoningEffort;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage);
        let result = store
            .update_runtime_config("does-not-exist", |s| {
                s.reasoning_effort = Some(ReasoningEffort::Low);
            })
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn merge_save_runtime_overwrites_messages_from_stale_snapshot() {
        // Characterization of the bug that motivated `update_runtime_config`:
        // `merge_save_runtime` writes the caller's `messages` verbatim, so a
        // stale snapshot reverts a concurrent append. Config-only writers must
        // therefore use `update_runtime_config`, never `merge_save_runtime`.
        use bamboo_domain::session::types::Message;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "stale-clobber";

        // A handler loads the session (1 message) …
        let mut baseline = fresh(session_id);
        baseline.add_message(Message::user("hello"));
        storage.save_session(&baseline).await.unwrap();
        let mut stale_snapshot = storage.load_session(session_id).await.unwrap().unwrap();

        // … then `POST /chat` appends a second message to disk …
        let mut after_chat = storage.load_session(session_id).await.unwrap().unwrap();
        after_chat.add_message(Message::user("second"));
        storage.save_session(&after_chat).await.unwrap();
        assert_eq!(
            storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap()
                .messages
                .len(),
            2
        );

        // … and the stale handler saves via merge_save_runtime -> append reverted.
        store.merge_save_runtime(&mut stale_snapshot).await.unwrap();
        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(
            after.messages.len(),
            1,
            "merge_save_runtime clobbers concurrent appends — this is why config patches must use update_runtime_config"
        );
    }

    #[tokio::test]
    async fn stale_runtime_save_cannot_remove_admitted_inbox_transcript() {
        use bamboo_domain::session::types::Message;
        use bamboo_domain::SessionMessageId;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "stale-inbox-preserve";

        let mut baseline = fresh(session_id);
        let mut base = Message::user("base");
        base.id = "base".to_string();
        baseline.add_message(base);
        storage.save_session(&baseline).await.unwrap();
        let mut stale = baseline.clone();
        let mut later_assistant = Message::assistant("runner output", None);
        later_assistant.id = "later-assistant".to_string();
        stale.add_message(later_assistant);

        let mut durable = baseline;
        let inbox_id = SessionMessageId::parse("durable-inbox-id").unwrap();
        let mut admitted = Message::user("durable inbox message");
        admitted.id = inbox_id.as_str().to_string();
        durable.add_message(admitted);
        durable
            .session_inbox_admission_mut()
            .record(inbox_id.clone(), 7);
        storage.save_session(&durable).await.unwrap();

        store.merge_save_runtime(&mut stale).await.unwrap();
        let saved = storage.load_session(session_id).await.unwrap().unwrap();
        let ids = saved
            .messages
            .iter()
            .map(|message| message.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["base", "durable-inbox-id", "later-assistant"]);
        assert_eq!(ids.iter().filter(|id| **id == inbox_id.as_str()).count(), 1);
        assert!(saved
            .session_inbox_admission()
            .is_some_and(|state| state.contains(&inbox_id)));
    }

    #[tokio::test]
    async fn stale_runtime_save_preserves_typed_inbox_message_after_cursor_eviction() {
        use bamboo_domain::{
            SessionMessageEnvelope, SessionMessageId, SESSION_INBOX_ADMITTED_CAPACITY,
        };

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "evicted-inbox-preserve";
        let mut durable = fresh(session_id);
        let mut envelope = SessionMessageEnvelope::user_input(session_id, "old durable inbox");
        envelope.id = SessionMessageId::parse("old-inbox-id").unwrap();
        durable.add_message(envelope.to_provider_message().unwrap());
        durable
            .session_inbox_admission_mut()
            .record(envelope.id.clone(), 1);
        for sequence in 2..=(SESSION_INBOX_ADMITTED_CAPACITY as u64 + 1) {
            durable.session_inbox_admission_mut().record(
                SessionMessageId::parse(format!("newer-{sequence}")).unwrap(),
                sequence,
            );
        }
        assert!(!durable
            .session_inbox_admission()
            .unwrap()
            .contains(&envelope.id));
        storage.save_session(&durable).await.unwrap();

        let mut stale = fresh(session_id);
        store.merge_save_runtime(&mut stale).await.unwrap();
        let saved = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(
            saved
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn checkpoint_runtime_session_preserves_disk_suffix_and_appends_live_messages() {
        use bamboo_domain::session::types::Message;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "checkpoint-no-shrink";

        let mut baseline = fresh(session_id);
        baseline.add_message(Message::user("base"));
        storage.save_session(&baseline).await.unwrap();
        let mut runner_snapshot = baseline.clone();

        let mut durable = baseline;
        let mut disk_only = Message::user("concurrent injected message");
        disk_only.id = "disk-only".to_string();
        durable.add_message(disk_only);
        storage.save_session(&durable).await.unwrap();

        let mut live_only = Message::assistant("partial runner output", None);
        live_only.id = "live-only".to_string();
        runner_snapshot.add_message(live_only);

        store
            .checkpoint_runtime_session(&mut runner_snapshot)
            .await
            .unwrap();

        let saved = storage.load_session(session_id).await.unwrap().unwrap();
        let ids = saved
            .messages
            .iter()
            .map(|message| message.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![durable.messages[0].id.as_str(), "disk-only", "live-only"]
        );
        assert_eq!(runner_snapshot.messages.len(), saved.messages.len());
        assert_eq!(runner_snapshot.messages[1].id, saved.messages[1].id);
        assert_eq!(runner_snapshot.messages[2].id, saved.messages[2].id);
        assert_eq!(saved.messages[1].content, "concurrent injected message");
        assert_eq!(saved.messages[2].content, "partial runner output");
    }

    #[tokio::test]
    async fn runtime_only_save_preserves_checkpointed_ledger_and_publishes_merged_state() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "runtime-only-ledger-race";
        let baseline = fresh(session_id);
        storage.save_session(&baseline).await.unwrap();
        let mut stale_control = storage.load_session(session_id).await.unwrap().unwrap();

        let mut runner = baseline;
        runner.model_context_state = Some(ledger_state(1, "runner-l1"));
        store.checkpoint_runtime_session(&mut runner).await.unwrap();

        stale_control.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
        let published = std::sync::Arc::new(std::sync::Mutex::new(None));
        let published_clone = published.clone();
        store
            .save_runtime_only_and_publish(&mut stale_control, move |saved| {
                *published_clone.lock().unwrap() = Some(saved.clone());
            })
            .await
            .unwrap();

        let expected = runner.model_context_state.clone();
        assert_eq!(stale_control.model_context_state, expected);
        assert_eq!(
            published
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .model_context_state,
            expected
        );
        let sidecar = storage
            .load_runtime_control_plane(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sidecar.model_context_state, expected);
        assert_eq!(
            sidecar
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("waiting_for_children")
        );
        let reloaded = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(reloaded.model_context_state, expected);
    }

    #[tokio::test]
    async fn full_runtime_save_preserves_newer_ledger_but_commits_control_mutation() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "full-save-ledger-race";
        let baseline = fresh(session_id);
        storage.save_session(&baseline).await.unwrap();
        let mut stale = storage.load_session(session_id).await.unwrap().unwrap();

        let mut runner = baseline;
        runner.model_context_state = Some(ledger_state(1, "runner-l1"));
        store.checkpoint_runtime_session(&mut runner).await.unwrap();

        stale
            .metadata
            .insert("activated_tools".to_string(), "[\"search\"]".to_string());
        let published = std::sync::Arc::new(std::sync::Mutex::new(None));
        let published_clone = published.clone();
        store
            .merge_save_runtime_and_publish(&mut stale, move |saved, committed| {
                assert!(committed);
                *published_clone.lock().unwrap() = Some(saved.clone());
            })
            .await
            .unwrap();

        let expected = runner.model_context_state.clone();
        assert_eq!(stale.model_context_state, expected);
        assert_eq!(
            published
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .model_context_state,
            expected
        );
        let sidecar = storage
            .load_runtime_control_plane(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sidecar.model_context_state, expected);
        let reloaded = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(reloaded.model_context_state, expected);
        assert_eq!(
            reloaded.metadata.get("activated_tools").map(String::as_str),
            Some("[\"search\"]")
        );
    }

    #[tokio::test]
    async fn newer_explicit_epoch_reset_wins_an_ordinary_full_runtime_save() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "full-save-ledger-reset";
        let mut durable = fresh(session_id);
        durable.model_context_state = Some(ledger_state(1, "runner-l1"));
        storage.save_session(&durable).await.unwrap();

        let mut compression = storage.load_session(session_id).await.unwrap().unwrap();
        compression.reset_model_context_epoch(bamboo_domain::ModelContextResetReason::Compression);
        let reset = compression.model_context_state.clone();
        assert_eq!(reset.as_ref().unwrap().state_revision, 2);
        store.merge_save_runtime(&mut compression).await.unwrap();

        let reloaded = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(reloaded.model_context_state, reset);
        assert_eq!(
            reloaded
                .model_context_state
                .as_ref()
                .and_then(|state| state.last_reset_reason),
            Some(bamboo_domain::ModelContextResetReason::Compression)
        );
    }

    #[tokio::test]
    async fn checkpoint_rejects_equal_revision_divergence_without_overwriting_disk() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "ledger-checkpoint-cas";
        let mut baseline = fresh(session_id);
        baseline.model_context_state = Some(ledger_state(1, "runner-l1"));
        storage.save_session(&baseline).await.unwrap();

        let mut first = baseline.clone();
        first.reset_model_context_epoch(bamboo_domain::ModelContextResetReason::Compression);
        let mut conflicting = baseline;
        conflicting.reset_model_context_epoch(bamboo_domain::ModelContextResetReason::Rollback);
        assert_eq!(
            first.model_context_state.as_ref().unwrap().state_revision,
            conflicting
                .model_context_state
                .as_ref()
                .unwrap()
                .state_revision
        );

        store.checkpoint_runtime_session(&mut first).await.unwrap();
        let error = store
            .checkpoint_runtime_session(&mut conflicting)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        let reloaded = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(reloaded.model_context_state, first.model_context_state);
    }

    #[tokio::test]
    async fn activation_checkpoint_clears_presentation_without_shrinking_concurrent_turn() {
        use bamboo_domain::session::runtime_state::{
            AgentRuntimeState, AgentStatusState, WaitingForChildrenState,
        };
        use bamboo_domain::session::types::Message;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "activation-no-shrink";
        let mut baseline = fresh(session_id);
        baseline.add_message(Message::user("base"));
        let mut state = AgentRuntimeState::new("activation-run");
        state.status = AgentStatusState::Suspended;
        state.waiting_for_children = Some(WaitingForChildrenState::for_children(
            vec!["child-1".to_string()],
            bamboo_domain::session::runtime_state::ChildWaitPolicy::All,
            chrono::Utc::now(),
        ));
        baseline.agent_runtime_state = Some(state);
        baseline.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
        storage.save_session(&baseline).await.unwrap();
        let mut activation_snapshot = baseline.clone();

        let mut concurrent = baseline;
        let mut normal = Message::assistant("normal concurrent answer", None);
        normal.id = "normal-concurrent".to_string();
        concurrent.add_message(normal);
        storage.save_session(&concurrent).await.unwrap();

        let state = activation_snapshot.agent_runtime_state.as_mut().unwrap();
        state.status = AgentStatusState::Idle;
        state.suspension = None;
        activation_snapshot
            .metadata
            .remove("runtime.suspend_reason");
        store
            .checkpoint_runtime_session(&mut activation_snapshot)
            .await
            .unwrap();

        let saved = storage.load_session(session_id).await.unwrap().unwrap();
        assert!(saved
            .messages
            .iter()
            .any(|message| message.id == "normal-concurrent"));
        let state = saved.agent_runtime_state.unwrap();
        assert_eq!(state.status, AgentStatusState::Idle);
        assert!(state.waiting_for_children.is_some());
        assert!(!saved.metadata.contains_key("runtime.suspend_reason"));
    }

    #[tokio::test]
    async fn merge_save_runtime_preserves_disk_authoritative_metadata_with_single_load() {
        // Regression guard for the single-load refactor of `merge_save_runtime`:
        // it must STILL pull the authoritative metadata group (title / pinned /
        // metadata_version) from the freshest on-disk copy when disk's
        // metadata_version >= the in-memory one, even though it now reads disk
        // only once.
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "runtime-merge-meta";

        // Baseline persisted by a runtime writer (metadata_version 0).
        let mut baseline = fresh(session_id);
        baseline.title = "Auto Title".to_string();
        baseline.metadata_version = 0;
        storage.save_session(&baseline).await.unwrap();

        // A stale runtime snapshot (still metadata_version 0, old title).
        let mut stale_snapshot = storage.load_session(session_id).await.unwrap().unwrap();

        // An authoritative UI rename bumps metadata_version on disk.
        let mut renamed = storage.load_session(session_id).await.unwrap().unwrap();
        renamed.title = "User Renamed".to_string();
        renamed.title_version = 1;
        renamed.pinned = true;
        renamed.metadata_version = 1;
        store.commit_metadata(&renamed).await.unwrap();

        // The stale runtime writer saves: it must adopt the disk title/pinned.
        stale_snapshot.title = "Auto Title".to_string();
        store.merge_save_runtime(&mut stale_snapshot).await.unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(after.title, "User Renamed");
        assert!(after.pinned);
        assert_eq!(after.metadata_version, 1);
        // And the in-memory copy was corrected by the merge too.
        assert_eq!(stale_snapshot.title, "User Renamed");
        assert_eq!(stale_snapshot.metadata_version, 1);
    }

    #[tokio::test]
    async fn merge_save_runtime_preserves_durable_workflow_run_index_from_stale_runner() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "runtime-workflow-run-index";

        let baseline = fresh(session_id);
        storage.save_session(&baseline).await.unwrap();
        let mut stale_runner = storage.load_session(session_id).await.unwrap().unwrap();

        store
            .update_runtime_config(session_id, |session| {
                session.metadata.insert(
                    "workflow.run_ids.v1".to_string(),
                    r#"["http-started-run"]"#.to_string(),
                );
            })
            .await
            .unwrap()
            .expect("session exists");

        store.merge_save_runtime(&mut stale_runner).await.unwrap();

        assert_eq!(
            stale_runner
                .metadata
                .get("workflow.run_ids.v1")
                .map(String::as_str),
            Some(r#"["http-started-run"]"#)
        );
        let durable = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(
            durable
                .metadata
                .get("workflow.run_ids.v1")
                .map(String::as_str),
            Some(r#"["http-started-run"]"#)
        );
    }

    // #540: a running loop's `merge_save_runtime` (carrying the run-start bypass
    // value) must NOT revert a concurrent mid-run `PATCH /sessions
    // {bypass_permissions}` write on disk — disk is the authoritative writer.
    #[tokio::test]
    async fn merge_save_runtime_adopts_disk_bypass_permissions() {
        use bamboo_domain::AgentRuntimeState;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "runtime-bypass";

        // Baseline persisted with bypass OFF.
        let baseline = fresh(session_id);
        storage.save_session(&baseline).await.unwrap();

        // The running loop holds a snapshot with bypass OFF (run-start value).
        let mut loop_snapshot = storage.load_session(session_id).await.unwrap().unwrap();
        loop_snapshot.agent_runtime_state = Some(AgentRuntimeState::default());

        // A concurrent PATCH flips bypass ON on disk (via update_runtime_config).
        store
            .update_runtime_config(session_id, |s| {
                s.agent_runtime_state
                    .get_or_insert_with(AgentRuntimeState::default)
                    .bypass_permissions = true;
            })
            .await
            .unwrap()
            .expect("session exists");

        // The loop saves its stale snapshot: it must adopt disk's ON value, not
        // revert to OFF.
        store.merge_save_runtime(&mut loop_snapshot).await.unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert!(
            after
                .agent_runtime_state
                .as_ref()
                .is_some_and(|s| s.bypass_permissions),
            "disk bypass=ON must survive a stale runtime save (#540)"
        );
        // The in-memory copy is corrected too.
        assert!(loop_snapshot
            .agent_runtime_state
            .as_ref()
            .is_some_and(|s| s.bypass_permissions));
    }

    // #770: the generalized disk-wins path must preserve Auto as a distinct
    // typed mode rather than collapsing it into the legacy bypass boolean.
    #[tokio::test]
    async fn merge_save_runtime_adopts_disk_auto_permission_mode() {
        use bamboo_domain::{AgentRuntimeState, SessionPermissionMode};

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "runtime-auto";

        storage.save_session(&fresh(session_id)).await.unwrap();
        let mut loop_snapshot = storage.load_session(session_id).await.unwrap().unwrap();
        loop_snapshot.agent_runtime_state = Some(AgentRuntimeState::default());
        loop_snapshot.metadata.insert(
            "permission.requested_mode".to_string(),
            "default".to_string(),
        );
        loop_snapshot.metadata.insert(
            "permission.effective_mode".to_string(),
            "default".to_string(),
        );
        loop_snapshot.metadata.insert(
            "permission.executor_mapping".to_string(),
            "bamboo_runtime:default".to_string(),
        );

        store
            .update_runtime_config(session_id, |session| {
                session
                    .agent_runtime_state
                    .get_or_insert_with(AgentRuntimeState::default)
                    .set_permission_mode(SessionPermissionMode::Auto);
                session
                    .metadata
                    .insert("permission.policy_revision".to_string(), "12".to_string());
                session
                    .metadata
                    .insert("permission.requested_mode".to_string(), "auto".to_string());
                session
                    .metadata
                    .insert("permission.effective_mode".to_string(), "auto".to_string());
                session.metadata.insert(
                    "permission.executor_mapping".to_string(),
                    "bamboo_runtime:auto".to_string(),
                );
                session.metadata.insert(
                    "permission.transitioned_at".to_string(),
                    "2026-07-31T12:00:00Z".to_string(),
                );
                session.metadata_version = session.metadata_version.saturating_add(1);
            })
            .await
            .unwrap()
            .expect("session exists");

        store.merge_save_runtime(&mut loop_snapshot).await.unwrap();

        let durable = storage.load_session(session_id).await.unwrap().unwrap();
        for state in [
            durable.agent_runtime_state.as_ref(),
            loop_snapshot.agent_runtime_state.as_ref(),
        ] {
            assert_eq!(
                state.map(AgentRuntimeState::effective_permission_mode),
                Some(SessionPermissionMode::Auto)
            );
        }
        for session in [&durable, &loop_snapshot] {
            assert_eq!(
                session.metadata.get("permission.policy_revision"),
                Some(&"12".to_string())
            );
            assert_eq!(
                session.metadata.get("permission.requested_mode"),
                Some(&"auto".to_string())
            );
            assert_eq!(
                session.metadata.get("permission.effective_mode"),
                Some(&"auto".to_string())
            );
            assert_eq!(
                session.metadata.get("permission.executor_mapping"),
                Some(&"bamboo_runtime:auto".to_string())
            );
            assert_eq!(
                session.metadata.get("permission.transitioned_at"),
                Some(&"2026-07-31T12:00:00Z".to_string())
            );
        }
    }

    // The reverse direction: a PATCH turning bypass OFF must also stick against
    // a stale loop snapshot that still has it ON.
    #[tokio::test]
    async fn merge_save_runtime_adopts_disk_bypass_off() {
        use bamboo_domain::AgentRuntimeState;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "runtime-bypass-off";

        // Baseline persisted with bypass ON.
        let mut baseline = fresh(session_id);
        let on_state = AgentRuntimeState {
            bypass_permissions: true,
            ..AgentRuntimeState::default()
        };
        baseline.agent_runtime_state = Some(on_state);
        storage.save_session(&baseline).await.unwrap();

        // Loop snapshot still ON.
        let mut loop_snapshot = storage.load_session(session_id).await.unwrap().unwrap();

        // PATCH flips OFF on disk.
        store
            .update_runtime_config(session_id, |s| {
                s.agent_runtime_state
                    .get_or_insert_with(AgentRuntimeState::default)
                    .bypass_permissions = false;
            })
            .await
            .unwrap()
            .expect("session exists");

        store.merge_save_runtime(&mut loop_snapshot).await.unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert!(
            !after
                .agent_runtime_state
                .as_ref()
                .is_some_and(|s| s.bypass_permissions),
            "disk bypass=OFF must survive a stale runtime save (#540)"
        );
    }

    // #540 review: the authoritative flag writer (#74 child-reseed) must NOT be
    // reverted by the disk-wins protection — its in-memory value persists as-is.
    #[tokio::test]
    async fn save_runtime_authoritative_flags_persists_in_memory_posture_and_audit() {
        use bamboo_domain::AgentRuntimeState;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "child-reseed";

        // Child on disk has bypass ON (created under a bypassed parent).
        let mut baseline = fresh(session_id);
        let on_state = AgentRuntimeState {
            bypass_permissions: true,
            ..AgentRuntimeState::default()
        };
        baseline.agent_runtime_state = Some(on_state);
        for (key, value) in [
            ("permission.policy_revision", "12"),
            ("permission.requested_mode", "bypass"),
            ("permission.effective_mode", "bypass"),
            ("permission.executor_mapping", "bamboo_runtime:bypass"),
            ("permission.transitioned_at", "2026-07-31T12:00:00Z"),
        ] {
            baseline.metadata.insert(key.to_string(), value.to_string());
        }
        storage.save_session(&baseline).await.unwrap();

        // Parent re-seeds the reused child to OFF (parent flipped bypass off),
        // loading the child then setting the flag in memory.
        let mut child = storage.load_session(session_id).await.unwrap().unwrap();
        child
            .agent_runtime_state
            .get_or_insert_with(AgentRuntimeState::default)
            .bypass_permissions = false;
        for (key, value) in [
            ("permission.policy_revision", "13"),
            ("permission.requested_mode", "default"),
            ("permission.effective_mode", "default"),
            ("permission.executor_mapping", "bamboo_runtime:default"),
            ("permission.transitioned_at", "2026-07-31T12:01:00Z"),
        ] {
            child.metadata.insert(key.to_string(), value.to_string());
        }

        // Authoritative write must persist OFF, not adopt the disk's stale ON.
        store
            .save_runtime_authoritative_flags(&mut child)
            .await
            .unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert!(
            !after
                .agent_runtime_state
                .as_ref()
                .is_some_and(|s| s.bypass_permissions),
            "authoritative re-seed of bypass=OFF must persist, not be reverted (#540/#74)"
        );
        for (key, value) in [
            ("permission.policy_revision", "13"),
            ("permission.requested_mode", "default"),
            ("permission.effective_mode", "default"),
            ("permission.executor_mapping", "bamboo_runtime:default"),
            ("permission.transitioned_at", "2026-07-31T12:01:00Z"),
        ] {
            assert_eq!(after.metadata.get(key).map(String::as_str), Some(value));
        }
    }

    // A disk copy lacking runtime state must not force the in-memory bypass OFF.
    #[tokio::test]
    async fn merge_save_runtime_leaves_bypass_when_disk_has_no_runtime_state() {
        use bamboo_domain::AgentRuntimeState;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "no-runtime-state";

        // Disk copy with NO agent_runtime_state.
        let baseline = fresh(session_id);
        assert!(baseline.agent_runtime_state.is_none());
        storage.save_session(&baseline).await.unwrap();

        // A running loop legitimately carries bypass ON in memory.
        let mut running = storage.load_session(session_id).await.unwrap().unwrap();
        let on_state = AgentRuntimeState {
            bypass_permissions: true,
            ..AgentRuntimeState::default()
        };
        running.agent_runtime_state = Some(on_state);

        store.merge_save_runtime(&mut running).await.unwrap();

        assert!(
            running
                .agent_runtime_state
                .as_ref()
                .is_some_and(|s| s.bypass_permissions),
            "a runtime-state-less disk copy must not force bypass OFF (#540)"
        );
    }

    // ── Free-function merge tests (updated for metadata-group) ──────

    #[tokio::test]
    async fn merge_preserves_disk_title_when_versions_equal() {
        let (_temp, storage) = make_storage().await;
        let session_id = "merge-equal";

        let mut on_disk = fresh(session_id);
        on_disk.title = "User Set This".to_string();
        on_disk.title_version = 0;
        on_disk.title_generated = true;
        on_disk.metadata_version = 0;
        storage.save_session(&on_disk).await.unwrap();

        let mut runtime_copy = fresh(session_id);
        runtime_copy.title = "Stale Default".to_string();
        runtime_copy.title_version = 0;
        runtime_copy.title_generated = false;
        runtime_copy.metadata_version = 0;
        runtime_copy.messages = vec![];

        merge_save_session(&storage, &mut runtime_copy)
            .await
            .unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(after.title, "User Set This");
        assert_eq!(after.title_version, 0);
        assert!(after.title_generated);
        assert_eq!(runtime_copy.title, "User Set This");
        assert!(runtime_copy.title_generated);
    }

    #[tokio::test]
    async fn merge_preserves_disk_when_disk_version_higher() {
        let (_temp, storage) = make_storage().await;
        let session_id = "merge-higher";

        let mut on_disk = fresh(session_id);
        on_disk.title = "User Title v3".to_string();
        on_disk.title_version = 3;
        on_disk.metadata_version = 5;
        storage.save_session(&on_disk).await.unwrap();

        let mut runtime_copy = fresh(session_id);
        runtime_copy.title = "Stale".to_string();
        runtime_copy.title_version = 1;
        runtime_copy.metadata_version = 0;

        merge_save_session(&storage, &mut runtime_copy)
            .await
            .unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(after.title, "User Title v3");
        assert_eq!(after.title_version, 3);
        assert_eq!(after.metadata_version, 5);
    }

    #[tokio::test]
    async fn merge_now_preserves_disk_pinned_in_metadata_group() {
        let (_temp, storage) = make_storage().await;
        let session_id = "pinned-merge";

        let mut on_disk = fresh(session_id);
        on_disk.pinned = true;
        on_disk.metadata_version = 2;
        storage.save_session(&on_disk).await.unwrap();

        let mut runtime_copy = fresh(session_id);
        runtime_copy.pinned = false;
        runtime_copy.metadata_version = 0;

        merge_save_session(&storage, &mut runtime_copy)
            .await
            .unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert!(
            after.pinned,
            "disk pinned=true should win over runtime false"
        );
        assert_eq!(after.metadata_version, 2);
    }

    #[tokio::test]
    async fn merge_keeps_in_memory_when_session_version_higher() {
        let (_temp, storage) = make_storage().await;
        let session_id = "merge-bumped";

        let mut on_disk = fresh(session_id);
        on_disk.title = "Old".to_string();
        on_disk.title_version = 1;
        on_disk.metadata_version = 3;
        storage.save_session(&on_disk).await.unwrap();

        let mut authoritative_copy = fresh(session_id);
        authoritative_copy.title = "New Authoritative".to_string();
        authoritative_copy.title_version = 2;
        authoritative_copy.metadata_version = 4;
        authoritative_copy.pinned = true;

        merge_save_session(&storage, &mut authoritative_copy)
            .await
            .unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(after.title, "New Authoritative");
        assert_eq!(after.title_version, 2);
        assert_eq!(after.metadata_version, 4);
        assert!(after.pinned);
    }

    #[tokio::test]
    async fn merge_keeps_runtime_messages_when_disk_only_changed_metadata() {
        let (_temp, storage) = make_storage().await;
        let session_id = "merge-messages";

        let mut on_disk = fresh(session_id);
        on_disk.title = "Fresh Title".to_string();
        on_disk.title_version = 2;
        on_disk.metadata_version = 5;
        storage.save_session(&on_disk).await.unwrap();

        let mut runtime_copy = fresh(session_id);
        runtime_copy.title = "Stale".to_string();
        runtime_copy.metadata_version = 0;
        runtime_copy.messages = vec![bamboo_domain::session::types::Message {
            role: bamboo_domain::session::types::Role::User,
            content: "keep me".to_string(),
            id: "msg-1".to_string(),
            created_at: chrono::Utc::now(),
            reasoning: None,
            reasoning_signature: None,
            content_parts: None,
            image_ocr: None,
            phase: None,
            tool_calls: None,
            tool_call_id: None,
            tool_success: None,
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            metadata: None,
        }];

        merge_save_session(&storage, &mut runtime_copy)
            .await
            .unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(after.title, "Fresh Title");
        assert_eq!(after.metadata_version, 5);
        assert_eq!(after.messages.len(), 1);
        assert_eq!(after.messages[0].content, "keep me");
    }

    #[tokio::test]
    async fn runtime_control_plane_port_uses_sidecar_without_rewriting_messages() {
        use bamboo_domain::session::types::Message;

        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "runtime-control-plane";

        let mut durable = fresh(session_id);
        durable.add_message(Message::user("durable transcript"));
        storage.save_session(&durable).await.unwrap();

        let mut runtime = durable.clone();
        runtime.model = "updated-control-plane-model".to_string();
        runtime.add_message(Message::assistant("uncheckpointed runtime message", None));
        RuntimeSessionPersistence::save_runtime_control_plane(&store, &mut runtime)
            .await
            .unwrap();

        let control_plane =
            RuntimeSessionPersistence::load_runtime_control_plane(&store, session_id)
                .await
                .unwrap()
                .expect("control-plane exists");
        assert!(
            control_plane.messages.is_empty(),
            "LockedSessionStore must expose its message-free sidecar"
        );
        assert_eq!(control_plane.model, "updated-control-plane-model");

        let reloaded = storage
            .load_session(session_id)
            .await
            .unwrap()
            .expect("session exists");
        assert_eq!(reloaded.model, "updated-control-plane-model");
        assert_eq!(
            reloaded.messages.len(),
            1,
            "control-plane save must not write the uncheckpointed message"
        );
        assert_eq!(reloaded.messages[0].content, "durable transcript");
    }

    #[tokio::test]
    async fn atomic_task_patch_loads_inside_lock_and_preserves_interleaved_runtime_state() {
        let temp = tempfile::tempdir().unwrap();
        let inner = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("storage init"),
        );
        let session_id = "atomic-task-patch";
        inner
            .save_session(&fresh(session_id))
            .await
            .expect("seed session");

        let counted = Arc::new(CountingControlPlaneStorage {
            inner: inner.clone(),
            control_plane_loads: AtomicUsize::new(0),
            full_saves: AtomicUsize::new(0),
            runtime_state_saves: AtomicUsize::new(0),
        });
        let storage: Arc<dyn Storage> = counted.clone();
        let store = Arc::new(LockedSessionStore::new(storage));
        let guard = store.acquire_lock(session_id).await;
        let now = chrono::Utc::now();
        let task_list = bamboo_domain::TaskList {
            session_id: session_id.to_string(),
            title: "Atomic Task patch".to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let patch_store = store.clone();
        let patch = tokio::spawn(async move {
            let _ = started_tx.send(());
            RuntimeSessionPersistence::update_task_list_control_plane(
                patch_store.as_ref(),
                session_id,
                &task_list,
                "9",
            )
            .await
        });
        started_rx.await.expect("patch task started");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            counted.control_plane_loads.load(Ordering::SeqCst),
            0,
            "Task patch must acquire the session lock before loading its snapshot"
        );

        // Publish a newer unrelated runtime transition while the Task patch is
        // queued on the same session lock. Once the guard releases, the patch
        // must load this latest snapshot and change only Task-owned fields.
        let mut latest = inner
            .load_runtime_control_plane(session_id)
            .await
            .expect("load latest control-plane")
            .expect("control-plane exists");
        latest.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("latest-run"));
        latest
            .metadata
            .insert("concurrent.runtime".to_string(), "preserve".to_string());
        inner
            .save_runtime_state(&latest)
            .await
            .expect("publish concurrent runtime transition");
        drop(guard);

        assert!(
            patch.await.expect("patch join").expect("patch succeeds"),
            "existing root must be patched"
        );
        assert_eq!(counted.control_plane_loads.load(Ordering::SeqCst), 1);
        let reloaded = inner
            .load_session(session_id)
            .await
            .expect("reload")
            .expect("session exists");
        assert_eq!(
            reloaded
                .agent_runtime_state
                .as_ref()
                .map(|state| state.run_id.as_str()),
            Some("latest-run")
        );
        assert_eq!(
            reloaded
                .metadata
                .get("concurrent.runtime")
                .map(String::as_str),
            Some("preserve")
        );
        assert_eq!(reloaded.task_list_version_meta().as_deref(), Some("9"));
        assert_eq!(
            reloaded.task_list.as_ref().map(|list| list.title.as_str()),
            Some("Atomic Task patch")
        );
    }

    #[tokio::test]
    async fn paired_task_cas_conflict_cannot_overwrite_newer_root_or_child_state() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let root_id = "task-cas-root";
        let child_id = "task-cas-child";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };

        let mut root = fresh(root_id);
        root.set_task_list(task_list("newer root"));
        root.set_task_list_version_meta("2");
        storage.save_session(&root).await.expect("seed root");
        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.set_task_list(task_list("current child"));
        child.set_task_list_version_meta("1");
        storage.save_session(&child).await.expect("seed child");

        let updated = RuntimeSessionPersistence::update_task_list_control_planes_if_version(
            &store,
            child_id,
            root_id,
            "1",
            &task_list("current child"),
            &task_list("stale evaluator"),
            "3",
        )
        .await
        .expect("CAS returns clean conflict");
        assert!(
            !updated,
            "mismatched root generation must reject both writes"
        );

        let durable_root = storage
            .load_session(root_id)
            .await
            .expect("load root")
            .expect("root exists");
        let durable_child = storage
            .load_session(child_id)
            .await
            .expect("load child")
            .expect("child exists");
        assert_eq!(durable_root.task_list_version_meta().as_deref(), Some("2"));
        assert_eq!(
            durable_root
                .task_list
                .as_ref()
                .map(|list| list.title.as_str()),
            Some("newer root")
        );
        assert_eq!(durable_child.task_list_version_meta().as_deref(), Some("1"));
        assert_eq!(
            durable_child
                .task_list
                .as_ref()
                .map(|list| list.title.as_str()),
            Some("current child")
        );
    }

    #[tokio::test]
    async fn paired_task_cas_success_uses_only_targeted_saves_and_preserves_both_transcripts() {
        use bamboo_domain::session::types::Message;

        let temp = tempfile::tempdir().unwrap();
        let inner = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("storage init"),
        );
        let root_id = "task-cas-success-root";
        let child_id = "task-cas-success-child";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };

        let mut root = fresh(root_id);
        root.add_message(Message::user("root transcript"));
        root.metadata
            .insert("unrelated.root".to_string(), "preserve".to_string());
        root.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("root-run"));
        root.set_task_list(task_list("old shared"));
        root.set_task_list_version_meta("1");
        inner.save_session(&root).await.expect("seed root");

        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.add_message(Message::user("child transcript"));
        child
            .metadata
            .insert("unrelated.child".to_string(), "preserve".to_string());
        child.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("child-run"));
        child.set_task_list(task_list("old shared"));
        child.set_task_list_version_meta("1");
        inner.save_session(&child).await.expect("seed child");

        let counted = Arc::new(CountingControlPlaneStorage {
            inner: inner.clone(),
            control_plane_loads: AtomicUsize::new(0),
            full_saves: AtomicUsize::new(0),
            runtime_state_saves: AtomicUsize::new(0),
        });
        let storage: Arc<dyn Storage> = counted.clone();
        let store = LockedSessionStore::new(storage);
        assert!(
            RuntimeSessionPersistence::update_task_list_control_planes_if_version(
                &store,
                child_id,
                root_id,
                "1",
                &task_list("old shared"),
                &task_list("evaluated"),
                "2",
            )
            .await
            .expect("paired CAS succeeds")
        );
        assert_eq!(counted.control_plane_loads.load(Ordering::SeqCst), 2);
        assert_eq!(counted.runtime_state_saves.load(Ordering::SeqCst), 2);
        assert_eq!(
            counted.full_saves.load(Ordering::SeqCst),
            0,
            "evaluation CAS must not call full save_session for child or root"
        );

        let durable_root = inner.load_session(root_id).await.unwrap().unwrap();
        let durable_child = inner.load_session(child_id).await.unwrap().unwrap();
        for (session, transcript, metadata_key, run_id) in [
            (
                &durable_root,
                "root transcript",
                "unrelated.root",
                "root-run",
            ),
            (
                &durable_child,
                "child transcript",
                "unrelated.child",
                "child-run",
            ),
        ] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("evaluated")
            );
            assert_eq!(session.messages.len(), 1);
            assert_eq!(session.messages[0].content, transcript);
            assert_eq!(
                session.metadata.get(metadata_key).map(String::as_str),
                Some("preserve")
            );
            assert_eq!(
                session
                    .agent_runtime_state
                    .as_ref()
                    .map(|state| state.run_id.as_str()),
                Some(run_id)
            );
        }
    }

    #[tokio::test]
    async fn locked_runtime_and_full_saves_adopt_task_conflicts_before_publish() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let first_storage = Arc::new(
            SessionStoreV2::new(home.clone())
                .await
                .expect("first storage init"),
        );
        let second_storage = Arc::new(
            SessionStoreV2::new(home)
                .await
                .expect("second storage init"),
        );
        let now = chrono::Utc::now();
        let task_list = |session_id: &str, title: &str| bamboo_domain::TaskList {
            session_id: session_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };

        let runtime_id = "ordinary-task-retry-runtime";
        let full_id = "ordinary-task-retry-full";
        let mut runtime_initial = fresh(runtime_id);
        runtime_initial.set_task_list(task_list(runtime_id, "runtime v1"));
        runtime_initial.set_task_list_version_meta("1");
        first_storage
            .save_session(&runtime_initial)
            .await
            .expect("seed runtime session");
        let mut full_initial = fresh(full_id);
        full_initial.set_task_list(task_list(full_id, "full v1"));
        full_initial.set_task_list_version_meta("1");
        first_storage
            .save_session(&full_initial)
            .await
            .expect("seed full session");

        let mut runtime_advanced = runtime_initial.clone();
        runtime_advanced.set_task_list(task_list(runtime_id, "runtime v2"));
        runtime_advanced.set_task_list_version_meta("2");
        second_storage
            .save_runtime_state(&runtime_advanced)
            .await
            .expect("advance runtime Task generation");
        let mut full_advanced = full_initial.clone();
        full_advanced.set_task_list(task_list(full_id, "full v2"));
        full_advanced.set_task_list_version_meta("2");
        second_storage
            .save_runtime_state(&full_advanced)
            .await
            .expect("advance full Task generation");

        let storage: Arc<dyn Storage> = first_storage.clone();
        let store = LockedSessionStore::new(storage);
        let runtime_published = Arc::new(std::sync::Mutex::new(None));
        let runtime_callback = runtime_published.clone();
        let mut runtime_stale = runtime_initial;
        runtime_stale
            .metadata
            .insert("runtime.non-task".to_string(), "preserved".to_string());
        store
            .save_runtime_only_and_publish(&mut runtime_stale, move |saved| {
                *runtime_callback.lock().expect("runtime publish lock") = Some(saved.clone());
            })
            .await
            .expect("locked runtime save rebases and retries");

        let full_published = Arc::new(std::sync::Mutex::new(None));
        let full_callback = full_published.clone();
        let mut full_stale = full_initial;
        full_stale
            .metadata
            .insert("full.non-task".to_string(), "preserved".to_string());
        store
            .merge_save_runtime_and_publish(&mut full_stale, move |saved, committed| {
                assert!(committed);
                *full_callback.lock().expect("full publish lock") = Some(saved.clone());
            })
            .await
            .expect("locked full save rebases and retries");

        let durable_runtime = first_storage
            .load_session(runtime_id)
            .await
            .unwrap()
            .expect("durable runtime session");
        let durable_full = first_storage
            .load_session(full_id)
            .await
            .unwrap()
            .expect("durable full session");
        let published_runtime = runtime_published
            .lock()
            .expect("runtime publish lock")
            .clone()
            .expect("runtime published snapshot");
        let published_full = full_published
            .lock()
            .expect("full publish lock")
            .clone()
            .expect("full published snapshot");

        for (session, expected_title, metadata_key) in [
            (&runtime_stale, "runtime v2", "runtime.non-task"),
            (&durable_runtime, "runtime v2", "runtime.non-task"),
            (&published_runtime, "runtime v2", "runtime.non-task"),
            (&full_stale, "full v2", "full.non-task"),
            (&durable_full, "full v2", "full.non-task"),
            (&published_full, "full v2", "full.non-task"),
        ] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some(expected_title)
            );
            assert_eq!(
                session.metadata.get(metadata_key).map(String::as_str),
                Some("preserved")
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_task_cas_rejects_same_version_divergent_snapshot_without_callback() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let session_id = "single-task-exact-snapshot";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: session_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        let durable_winner = task_list("durable winner");
        let stale_snapshot = task_list("stale same-version snapshot");
        let mut session = fresh(session_id);
        session.set_task_list(durable_winner.clone());
        session.set_task_list_version_meta("1");
        storage.save_session(&session).await.expect("seed session");

        let published = Arc::new(AtomicBool::new(false));
        let callback = published.clone();
        assert!(!store
            .update_task_list_control_plane_if_version_and_publish(
                session_id,
                "1",
                &stale_snapshot,
                &task_list("stale evaluation"),
                "2",
                move |_| callback.store(true, Ordering::SeqCst),
            )
            .await
            .expect("same-version divergence is a clean stale result"));
        assert!(!published.load(Ordering::SeqCst));
        let durable = storage
            .load_session(session_id)
            .await
            .unwrap()
            .expect("session remains");
        assert_eq!(durable.task_list_version_meta().as_deref(), Some("1"));
        assert_eq!(
            serde_json::to_value(&durable.task_list).expect("serialize durable Task list"),
            serde_json::to_value(Some(&durable_winner)).expect("serialize expected Task list")
        );
    }

    #[tokio::test]
    async fn paired_task_cas_rejects_same_version_divergent_snapshot_without_callback() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage.clone());
        let root_id = "paired-task-exact-snapshot-root";
        let child_id = "paired-task-exact-snapshot-child";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        let durable_winner = task_list("durable winner");
        let stale_snapshot = task_list("stale same-version snapshot");
        let mut root = fresh(root_id);
        root.set_task_list(durable_winner.clone());
        root.set_task_list_version_meta("1");
        storage.save_session(&root).await.expect("seed root");
        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.set_task_list(durable_winner.clone());
        child.set_task_list_version_meta("1");
        storage.save_session(&child).await.expect("seed child");

        let published = Arc::new(AtomicBool::new(false));
        let callback = published.clone();
        assert!(!store
            .update_task_list_control_planes_if_version_and_publish(
                child_id,
                root_id,
                "1",
                &stale_snapshot,
                &task_list("stale evaluation"),
                "2",
                move |_, _| callback.store(true, Ordering::SeqCst),
            )
            .await
            .expect("same-version divergence is a clean stale result"));
        assert!(!published.load(Ordering::SeqCst));
        for id in [child_id, root_id] {
            let durable = storage
                .load_session(id)
                .await
                .unwrap()
                .expect("session remains");
            assert_eq!(durable.task_list_version_meta().as_deref(), Some("1"));
            assert_eq!(
                serde_json::to_value(&durable.task_list).expect("serialize durable Task list"),
                serde_json::to_value(Some(&durable_winner)).expect("serialize expected Task list")
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unconditional_root_task_patch_reports_final_cas_conflict_without_publishing() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let first_inner = Arc::new(
            SessionStoreV2::new(home.clone())
                .await
                .expect("first storage init"),
        );
        let root_id = "single-task-unconditional-loser";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        let mut root = fresh(root_id);
        root.task_list = Some(task_list("original"));
        root.set_task_list_version_meta("1");
        first_inner.save_session(&root).await.expect("seed root");
        let second_inner = Arc::new(
            SessionStoreV2::new(home)
                .await
                .expect("second storage init"),
        );
        let commit_reached = Arc::new(tokio::sync::Barrier::new(2));
        let release_commit = Arc::new(tokio::sync::Barrier::new(2));
        let storage: Arc<dyn Storage> = Arc::new(SingleCommitPauseStorage {
            inner: first_inner.clone(),
            commit_reached: commit_reached.clone(),
            release_commit: release_commit.clone(),
        });
        let store = LockedSessionStore::new(storage);
        let published = Arc::new(AtomicBool::new(false));
        let callback = published.clone();
        let loser_candidate = task_list("loser");

        let loser = store.update_task_list_control_plane_and_publish(
            root_id,
            &loser_candidate,
            "2",
            move |_| callback.store(true, Ordering::SeqCst),
        );
        let winner = async {
            commit_reached.wait().await;
            let original = second_inner
                .load_runtime_control_plane(root_id)
                .await
                .expect("load winner original")
                .expect("winner original exists");
            let mut updated = original.clone();
            updated.task_list = Some(task_list("winner"));
            updated.set_task_list_version_meta("2");
            assert!(second_inner
                .save_task_control_plane_if_matches(&original, &updated)
                .await
                .expect("commit winner"));
            release_commit.wait().await;
        };
        let (loser_result, ()) = tokio::join!(loser, winner);
        let error = loser_result.expect_err("unconditional loser must be an explicit conflict");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert!(!published.load(Ordering::SeqCst));
        let durable = first_inner
            .load_session(root_id)
            .await
            .unwrap()
            .expect("durable root");
        assert_eq!(durable.task_list_version_meta().as_deref(), Some("2"));
        assert_eq!(
            durable.task_list.as_ref().map(|list| list.title.as_str()),
            Some("winner")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_root_task_patches_have_one_final_cas_winner() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let first_inner = Arc::new(
            SessionStoreV2::new(home.clone())
                .await
                .expect("first storage init"),
        );
        let root_id = "single-task-cas-race-root";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        let mut root = fresh(root_id);
        root.set_task_list(task_list("original"));
        root.set_task_list_version_meta("1");
        first_inner.save_session(&root).await.expect("seed root");
        let second_inner = Arc::new(
            SessionStoreV2::new(home)
                .await
                .expect("second storage init"),
        );
        let before_commit = Arc::new(tokio::sync::Barrier::new(2));
        let first_storage: Arc<dyn Storage> = Arc::new(SingleCommitBarrierStorage {
            inner: first_inner.clone(),
            before_commit: before_commit.clone(),
        });
        let second_storage: Arc<dyn Storage> = Arc::new(SingleCommitBarrierStorage {
            inner: second_inner.clone(),
            before_commit,
        });
        let first_store = LockedSessionStore::new(first_storage);
        let second_store = LockedSessionStore::new(second_storage);
        let first_published = Arc::new(AtomicBool::new(false));
        let second_published = Arc::new(AtomicBool::new(false));
        let first_callback = first_published.clone();
        let second_callback = second_published.clone();
        let expected = task_list("original");
        let first_candidate = task_list("candidate one");
        let second_candidate = task_list("candidate two");

        // Two expected-version patches stage from v1, but the storage final-CAS
        // admits only one regardless of process-local lock ownership.
        let first = first_store.update_task_list_control_plane_if_version_and_publish(
            root_id,
            "1",
            &expected,
            &first_candidate,
            "2",
            move |_| first_callback.store(true, Ordering::SeqCst),
        );
        let second = second_store.update_task_list_control_plane_if_version_and_publish(
            root_id,
            "1",
            &expected,
            &second_candidate,
            "2",
            move |_| second_callback.store(true, Ordering::SeqCst),
        );
        let (first_result, second_result) = tokio::join!(first, second);
        let first_won = first_result.expect("first root result");
        let second_won = second_result.expect("second root result");
        assert_ne!(first_won, second_won, "exactly one root candidate wins");
        assert_eq!(first_published.load(Ordering::SeqCst), first_won);
        assert_eq!(second_published.load(Ordering::SeqCst), second_won);

        let durable = first_inner
            .load_session(root_id)
            .await
            .unwrap()
            .expect("root");
        assert_eq!(durable.task_list_version_meta().as_deref(), Some("2"));
        assert_eq!(
            durable.task_list.as_ref().map(|list| list.title.as_str()),
            Some(if first_won {
                "candidate one"
            } else {
                "candidate two"
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_locked_stores_revalidate_pair_cas_at_storage_commit_point() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let first_inner = Arc::new(
            SessionStoreV2::new(home.clone())
                .await
                .expect("first storage init"),
        );
        let root_id = "task-cas-race-root";
        let child_id = "task-cas-race-child";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };

        let mut root = fresh(root_id);
        root.set_task_list(task_list("original shared"));
        root.set_task_list_version_meta("1");
        first_inner.save_session(&root).await.expect("seed root");
        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.set_task_list(task_list("original shared"));
        child.set_task_list_version_meta("1");
        first_inner.save_session(&child).await.expect("seed child");

        let second_inner = Arc::new(
            SessionStoreV2::new(home)
                .await
                .expect("second storage init"),
        );
        let before_commit = Arc::new(tokio::sync::Barrier::new(2));
        let first_storage: Arc<dyn Storage> = Arc::new(PairCommitBarrierStorage {
            inner: first_inner.clone(),
            before_commit: before_commit.clone(),
        });
        let second_storage: Arc<dyn Storage> = Arc::new(PairCommitBarrierStorage {
            inner: second_inner.clone(),
            before_commit,
        });
        let first_store = LockedSessionStore::new(first_storage);
        let second_store = LockedSessionStore::new(second_storage);
        let first_published = Arc::new(AtomicBool::new(false));
        let second_published = Arc::new(AtomicBool::new(false));
        let first_published_callback = first_published.clone();
        let second_published_callback = second_published.clone();
        let expected = task_list("original shared");
        let first_candidate = task_list("candidate one");
        let second_candidate = task_list("candidate two");

        let first = first_store.update_task_list_control_planes_if_version_and_publish(
            child_id,
            root_id,
            "1",
            &expected,
            &first_candidate,
            "2",
            move |_, _| first_published_callback.store(true, Ordering::SeqCst),
        );
        let second = second_store.update_task_list_control_planes_if_version_and_publish(
            child_id,
            root_id,
            "1",
            &expected,
            &second_candidate,
            "2",
            move |_, _| second_published_callback.store(true, Ordering::SeqCst),
        );
        let (first_result, second_result) = tokio::join!(first, second);
        let first_won = first_result.expect("first CAS result");
        let second_won = second_result.expect("second CAS result");
        assert_ne!(first_won, second_won, "exactly one staged v1 CAS may win");
        assert_eq!(first_published.load(Ordering::SeqCst), first_won);
        assert_eq!(second_published.load(Ordering::SeqCst), second_won);

        let expected_title = if first_won {
            "candidate one"
        } else {
            "candidate two"
        };
        let durable_child = first_inner
            .load_session(child_id)
            .await
            .unwrap()
            .expect("child");
        let durable_root = second_inner
            .load_session(root_id)
            .await
            .unwrap()
            .expect("root");
        for session in [&durable_child, &durable_root] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some(expected_title)
            );
        }
    }

    #[tokio::test]
    async fn paired_task_second_write_failure_rolls_back_and_skips_publish_callback() {
        use bamboo_domain::session::types::Message;

        let temp = tempfile::tempdir().unwrap();
        let inner = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("storage init"),
        );
        let root_id = "task-cas-failure-root";
        let child_id = "task-cas-failure-child";
        let now = chrono::Utc::now();
        let task_list = |title: &str| bamboo_domain::TaskList {
            session_id: root_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        };

        let mut root = fresh(root_id);
        root.add_message(Message::user("root transcript"));
        root.metadata
            .insert("unrelated.root".to_string(), "preserve".to_string());
        root.set_task_list(task_list("old shared"));
        root.set_task_list_version_meta("1");
        inner.save_session(&root).await.expect("seed root");

        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.add_message(Message::user("child transcript"));
        child
            .metadata
            .insert("unrelated.child".to_string(), "preserve".to_string());
        child.set_task_list(task_list("old shared"));
        child.set_task_list_version_meta("1");
        inner.save_session(&child).await.expect("seed child");

        inner
            .inject_runtime_task_transaction_fault(RuntimeTaskTransactionFault::SecondUpdatedWrite);
        let storage: Arc<dyn Storage> = inner.clone();
        let store = LockedSessionStore::new(storage);
        let published = Arc::new(AtomicBool::new(false));
        let published_for_callback = published.clone();
        let error = store
            .update_task_list_control_planes_if_version_and_publish(
                child_id,
                root_id,
                "1",
                &task_list("old shared"),
                &task_list("must roll back"),
                "2",
                move |_, _| published_for_callback.store(true, Ordering::SeqCst),
            )
            .await
            .expect_err("injected second write fails");
        assert!(error.to_string().contains("rolled back"), "{error}");
        assert!(
            !published.load(Ordering::SeqCst),
            "durable failure must not publish either cache snapshot"
        );

        let durable_root = inner.load_session(root_id).await.unwrap().unwrap();
        let durable_child = inner.load_session(child_id).await.unwrap().unwrap();
        for (session, title, transcript, metadata_key) in [
            (
                &durable_root,
                "old shared",
                "root transcript",
                "unrelated.root",
            ),
            (
                &durable_child,
                "old shared",
                "child transcript",
                "unrelated.child",
            ),
        ] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("1"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some(title)
            );
            assert_eq!(session.messages[0].content, transcript);
            assert_eq!(
                session.metadata.get(metadata_key).map(String::as_str),
                Some("preserve")
            );
        }
    }

    // ── LockedSessionStore tests ────────────────────────────────────

    #[tokio::test]
    async fn locked_merge_save_runtime_serialises_concurrent_writes() {
        let (_temp, storage) = make_storage().await;
        let store = Arc::new(LockedSessionStore::new(storage));
        let session_id = "lock-serial".to_string();

        // Seed with base version.
        let base = fresh(&session_id);
        store.storage().save_session(&base).await.unwrap();

        // Two concurrent authorised writers each bump and commit.
        // We'll simulate via clone-and-bump-then-commit.
        let store_a = store.clone();
        let store_b = store.clone();
        let sid_a = session_id.clone();
        let sid_b = session_id.clone();

        let a = tokio::spawn(async move {
            let _guard = store_a.acquire_lock(&sid_a).await;
            let mut s = store_a
                .storage()
                .load_session(&sid_a)
                .await
                .unwrap()
                .unwrap();
            s.title = "Writer A".to_string();
            s.title_version = s.title_version.saturating_add(1);
            s.metadata_version = s.metadata_version.saturating_add(1);
            s.updated_at = chrono::Utc::now();
            store_a.storage().save_session(&s).await.unwrap();
            s.title_version
        });

        // Tiny yield so A goes first.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let b = tokio::spawn(async move {
            let _guard = store_b.acquire_lock(&sid_b).await;
            let mut s = store_b
                .storage()
                .load_session(&sid_b)
                .await
                .unwrap()
                .unwrap();
            s.title = "Writer B".to_string();
            s.title_version = s.title_version.saturating_add(1);
            s.metadata_version = s.metadata_version.saturating_add(1);
            s.updated_at = chrono::Utc::now();
            store_b.storage().save_session(&s).await.unwrap();
            s.title_version
        });

        let (ver_a, ver_b) = tokio::join!(a, b);
        let final_s = store
            .storage()
            .load_session(&session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            ver_a.unwrap() != ver_b.unwrap(),
            "concurrent writers must produce distinct versions"
        );
        assert_eq!(final_s.metadata_version, 2);
    }

    #[tokio::test]
    async fn commit_metadata_is_plain_save_inside_lock() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage);
        let session_id = "commit-plain";

        let mut s = fresh(session_id);
        s.title = "Committed".to_string();
        s.metadata_version = 1;
        s.title_version = 2;

        store.commit_metadata(&s).await.unwrap();

        let after = store
            .storage()
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.title, "Committed");
        assert_eq!(after.metadata_version, 1);
        assert_eq!(after.title_version, 2);
    }

    // ── Self-cleaning per-session lock (issue #346) ─────────────────

    #[tokio::test]
    async fn acquire_lock_self_evicts_when_no_other_holder() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage);

        {
            let _guard = store.acquire_lock("solo").await;
            assert_eq!(store.locks.len(), 1, "entry present while the lock is held");
        }
        // Dropping the guard runs the self-cleaning `remove_if`. Without the
        // eviction logic this stays at 1 forever (the pre-#346 leak).
        assert_eq!(
            store.locks.len(),
            0,
            "lock entry must be evicted once released with no other holder"
        );
    }

    #[tokio::test]
    async fn acquire_lock_many_distinct_ids_do_not_accumulate() {
        let (_temp, storage) = make_storage().await;
        let store = LockedSessionStore::new(storage);

        // Serially acquire+release for 100 distinct session ids.
        for i in 0..100 {
            let _guard = store.acquire_lock(&format!("sess-{i}")).await;
        }
        assert_eq!(
            store.locks.len(),
            0,
            "acquiring locks for many distinct ids must not grow the map"
        );
    }

    #[tokio::test]
    async fn acquire_lock_concurrent_waiter_keeps_valid_lock_and_map_drains() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (_temp, storage) = make_storage().await;
        let store = Arc::new(LockedSessionStore::new(storage));

        // Tracks concurrent holders of the SAME session lock; must never exceed 1.
        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            let active = active.clone();
            let max_seen = max_seen.clone();
            handles.push(tokio::spawn(async move {
                let _guard = store.acquire_lock("contended").await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                // Hold briefly so the other tasks actually queue on the mutex.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Mutual exclusion must hold: a self-cleaning removal that raced (removed
        // the entry a waiter had already cloned, letting a later task create and
        // lock a *second* mutex for the same id) would show 2 concurrent holders.
        // `remove_if`'s atomic strong-count check under the shard lock prevents it.
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "at most one holder of a given session lock at a time"
        );
        assert_eq!(
            store.locks.len(),
            0,
            "after all holders release, the contended entry must be fully evicted"
        );
    }
}
