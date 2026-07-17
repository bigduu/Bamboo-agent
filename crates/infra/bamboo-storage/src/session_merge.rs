//! Merge-aware session save helper.
//!
//! Provides [`merge_save_session`], which preserves any concurrent UI edits to
//! the authoritative metadata group (`title`, `title_version`, `pinned`,
//! `metadata_version`) before writing the runtime-modified session to storage.
//! Re-reads the latest persisted copy and only takes in-memory values when the
//! caller's `metadata_version` strictly exceeds disk's.
//!
//! ## Field-by-field merge policy
//!
//! All authoritative metadata fields are grouped under `metadata_version`:
//! when `disk.metadata_version >= session.metadata_version`, the on-disk
//! `title`, `title_version`, `pinned`, and `metadata_version` overwrite the
//! in-memory values before writing. Authoritative writers bump
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
use bamboo_domain::RuntimeSessionPersistence;
use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

const AUTHORITATIVE_METADATA_KEYS: &[&str] = &["gold_config"];

// ── LockedSessionStore ────────────────────────────────────────────────

/// Wraps a [`Storage`] implementation with per-session write serialization.
///
/// Under the hood it maintains a `DashMap<String, Arc<Mutex<()>>>` so that
/// only writes targeting the *same* session are serialised; different
/// sessions proceed concurrently.
pub struct LockedSessionStore {
    storage: Arc<dyn Storage>,
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
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
    /// `messages` are ignored by the sidecar and would not be persisted.
    pub async fn save_runtime_only(&self, session: &mut Session) -> std::io::Result<()> {
        let _guard = self.acquire_lock(&session.id).await;
        if let Ok(Some(latest)) = self.storage.load_runtime_control_plane(&session.id).await {
            apply_authoritative_metadata(session, &latest);
            // The control-plane sidecar carries `agent_runtime_state`, so a
            // concurrent mid-run bypass flip is here too — don't revert it. #540.
            adopt_disk_bypass_permissions(session, &latest);
        }
        self.storage.save_runtime_state(session).await
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
        self.storage.save_session(session).await
    }

    /// Runtime / non-authoritative save with per-session lock.
    ///
    /// Inside the lock: reload disk, merge the authoritative metadata group
    /// (`title`, `title_version`, `pinned`, `metadata_version`) from disk into
    /// the in-memory copy if disk's `metadata_version >= session.metadata_version`,
    /// then save.
    ///
    /// This is the locked equivalent of [`merge_save_session`]; prefer it for
    /// server-side paths where an authoritative write may race with this save.
    pub async fn merge_save_runtime(&self, session: &mut Session) -> std::io::Result<()> {
        let _guard = self.acquire_lock(&session.id).await;

        // Single disk read serves BOTH the SHRINK diagnostic and the
        // authoritative-metadata merge below. Previously this path loaded the
        // session twice (once here, once inside the merge helper); on a parent
        // session carrying the full conversation history that doubled the
        // deserialization cost of every runtime save, which is the hot path
        // during sub-agent spawn.
        let latest = self.storage.load_session(&session.id).await.ok().flatten();

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
            // Never let a running loop's save revert a concurrent mid-run
            // `PATCH /sessions {bypass_permissions}` flip. #540.
            adopt_disk_bypass_permissions(session, latest);
        }
        self.storage.save_session(session).await
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
        let _guard = self.acquire_lock(session_id).await;
        let Some(mut session) = self.storage.load_session(session_id).await? else {
            return Ok(None);
        };
        mutate(&mut session);
        self.storage.save_session(&session).await?;
        Ok(Some(session))
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
) {
    if let Ok(Some(latest)) = storage.load_session(&session.id).await {
        apply_authoritative_metadata(session, &latest);
        adopt_disk_bypass_permissions(session, &latest);
    }
}

/// Adopt the on-disk `agent_runtime_state.bypass_permissions` into the session
/// about to be saved.
///
/// `PATCH /sessions {bypass_permissions}` is the SOLE authoritative writer of
/// this flag (a running loop only carries it forward from run start). Without
/// this, a runtime save from an in-flight run — which holds the run-start value
/// — silently reverts a concurrent mid-run flip on disk. Unlike the metadata
/// group this is NOT version-gated: the PATCH writes via `update_runtime_config`,
/// which does not bump `metadata_version`. #540.
fn adopt_disk_bypass_permissions(session: &mut Session, latest: &Session) {
    let disk_bypass = latest
        .agent_runtime_state
        .as_ref()
        .is_some_and(|state| state.bypass_permissions);
    match session.agent_runtime_state.as_mut() {
        Some(state) => state.bypass_permissions = disk_bypass,
        // No runtime state in memory and disk says "off" → nothing to adopt;
        // avoid allocating a default state just to store `false`.
        None if disk_bypass => {
            session
                .agent_runtime_state
                .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
                .bypass_permissions = true;
        }
        None => {}
    }
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
/// session.metadata_version`, the on-disk `title`, `title_version`, `pinned`
/// and `metadata_version` overwrite the in-memory values before writing.
///
/// This is the stateless variant (no per-session lock). Prefer
/// [`LockedSessionStore::merge_save_runtime`] for server-side paths where an
/// authoritative writer may race with this save.
pub async fn merge_save_session(
    storage: &Arc<dyn Storage>,
    session: &mut Session,
) -> std::io::Result<()> {
    merge_authoritative_metadata_into_stale(storage, session).await;
    storage.save_session(session).await
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::SessionStoreV2;
    use bamboo_domain::session::types::Session;

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
        let mut on_state = AgentRuntimeState::default();
        on_state.bypass_permissions = true;
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

    // ── Free-function merge tests (updated for metadata-group) ──────

    #[tokio::test]
    async fn merge_preserves_disk_title_when_versions_equal() {
        let (_temp, storage) = make_storage().await;
        let session_id = "merge-equal";

        let mut on_disk = fresh(session_id);
        on_disk.title = "User Set This".to_string();
        on_disk.title_version = 0;
        on_disk.metadata_version = 0;
        storage.save_session(&on_disk).await.unwrap();

        let mut runtime_copy = fresh(session_id);
        runtime_copy.title = "Stale Default".to_string();
        runtime_copy.title_version = 0;
        runtime_copy.metadata_version = 0;
        runtime_copy.messages = vec![];

        merge_save_session(&storage, &mut runtime_copy)
            .await
            .unwrap();

        let after = storage.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(after.title, "User Set This");
        assert_eq!(after.title_version, 0);
        assert_eq!(runtime_copy.title, "User Set This");
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
