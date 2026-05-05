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
    pub async fn acquire_lock(&self, session_id: &str) -> OwnedMutexGuard<()> {
        let lock = self
            .locks
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        lock.lock_owned().await
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
        merge_authoritative_metadata_into_stale(&self.storage, session).await;
        self.storage.save_session(session).await
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
        if latest.metadata_version >= session.metadata_version {
            session.title = latest.title;
            session.title_version = latest.title_version;
            session.pinned = latest.pinned;
            session.metadata_version = latest.metadata_version;
        }
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
    use crate::storage::v2::SessionStoreV2;
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
}
