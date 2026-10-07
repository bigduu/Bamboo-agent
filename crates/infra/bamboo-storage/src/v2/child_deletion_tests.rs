use super::*;
use std::sync::{Condvar, Mutex as StdMutex};

const ROOT: &str = "delete-guard-root";
const CHILD: &str = "delete-guard-child";
const SIBLING: &str = "delete-guard-sibling";

#[derive(Debug, Default)]
struct State {
    entered: bool,
    released: bool,
    finished: bool,
}

#[derive(Debug)]
pub(super) struct ChildDeleteHook {
    state: StdMutex<State>,
    wake: Condvar,
}

impl ChildDeleteHook {
    fn install(store: &SessionStoreV2) -> Arc<Self> {
        let hook = Arc::new(Self {
            state: StdMutex::new(State::default()),
            wake: Condvar::new(),
        });
        *store.child_delete_hook.lock().unwrap() = Some(hook.clone());
        hook
    }

    pub(super) fn enter(&self) -> Finished<'_> {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.wake.notify_all();
        while !state.released {
            state = self.wake.wait(state).unwrap();
        }
        Finished(self)
    }

    fn wait_for(&self, predicate: impl Fn(&State) -> bool) {
        let state = self.state.lock().unwrap();
        // Deadline is a hang guard, never evidence of exclusion or ordering.
        let (state, _) = self
            .wake
            .wait_timeout_while(state, Duration::from_secs(10), |state| !predicate(state))
            .unwrap();
        assert!(predicate(&state), "Child deletion barrier was not reached");
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.wake.notify_all();
    }
}

pub(super) struct Finished<'a>(&'a ChildDeleteHook);
impl Drop for Finished<'_> {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().finished = true;
        self.0.wake.notify_all();
    }
}

// Release before unwinding the runtime, including on an old-code failure.
struct Release(Arc<ChildDeleteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
        // The job has reached its barrier before any exclusion assertion.
        // Drain it on failure too, before the TempDir is removed.
        let state = self.0.state.lock().unwrap();
        let _ = self
            .0
            .wake
            .wait_timeout_while(state, Duration::from_secs(10), |state| {
                state.entered && !state.finished
            });
    }
}

#[derive(Clone, Copy)]
enum Delete {
    Single,
    Cleanup,
}

async fn delete(store: &SessionStoreV2, operation: Delete) -> io::Result<()> {
    match operation {
        Delete::Single => assert!(store.delete_session_recursive(CHILD, false).await?),
        Delete::Cleanup => {
            let result = store.cleanup(CleanupMode::Children, true).await?;
            assert_eq!(result.deleted_session_ids, [CHILD]);
        }
    }
    Ok(())
}

fn assert_delete_locks_held(store: &SessionStoreV2) {
    assert!(
        store.session_lifecycle_lock.try_read().is_err(),
        "caller cancellation released the lifecycle gate before physical deletion finished"
    );
    assert!(
        store.runtime_task_transaction_gate.try_read().is_err(),
        "caller cancellation released the Task gate before physical deletion finished"
    );
    let tree_lock = store
        .bamboo_home_dir
        .join(ACTOR_TREE_LOCK_DIR)
        .join(format!("{:x}.lock", Sha256::digest(ROOT.as_bytes())));
    for path in [
        store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        tree_lock,
    ] {
        // Independent file descriptions model the physical exclusion used by
        // another store/process, without scheduling or elapsed-time inference.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let result = FileExt::try_lock_exclusive(&file);
        if result.is_ok() {
            FileExt::unlock(&file).unwrap();
        }
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "physical deletion released {}",
            path.display()
        );
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

fn cancellation_keeps_started_child_deletion_locked(operation: Delete, shutdown: bool) {
    let home = tempfile::tempdir().unwrap();
    let mut deleting_runtime = Some(runtime());
    let (first, second, mut root) = deleting_runtime.as_ref().unwrap().block_on(async {
        let first = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let root = Session::new(ROOT, "model");
        first.save_session(&root).await.unwrap();
        let child = Session::new_child_of(CHILD, &root, "model", "child");
        first.save_session(&child).await.unwrap();
        let mut sibling = Session::new_child_of(SIBLING, &root, "model", "sibling");
        sibling.pinned = true;
        first.save_session(&sibling).await.unwrap();
        first.flush_search_index().await;
        let second = SessionStoreV2::new(home.path().into()).await.unwrap();
        (first, second, root)
    });
    let hook = ChildDeleteHook::install(&first);
    let _release = Release(hook.clone());
    let job = {
        let first = first.clone();
        deleting_runtime
            .as_ref()
            .unwrap()
            .spawn(async move { delete(&first, operation).await })
    };
    hook.wait_for(|state| state.entered);
    let directory = home
        .path()
        .join("sessions")
        .join(ROOT)
        .join("children")
        .join(CHILD);
    assert!(directory.join("session.json").is_file());
    assert_delete_locks_held(&first);
    if shutdown {
        // Shutdown cancels the async waiter but cannot stop the started
        // blocking deletion. Joining below proves that cancellation completed.
        deleting_runtime.take().unwrap().shutdown_background();
    } else {
        job.abort();
    }
    let successor_runtime = runtime();
    assert!(successor_runtime.block_on(job).unwrap_err().is_cancelled());
    assert_delete_locks_held(&first);
    assert!(directory.join("session.json").is_file());
    hook.release();
    hook.wait_for(|state| state.finished);
    root.title = "writer admitted after deletion".into();
    successor_runtime.block_on(async {
        second.save_session(&root).await.unwrap();
        assert_eq!(
            second.load_session(ROOT).await.unwrap().unwrap().title,
            root.title
        );
        assert!(second.load_session(SIBLING).await.unwrap().unwrap().pinned);
    });
    assert!(!directory.exists());
    assert!(first.session_lifecycle_lock.try_read().is_ok());
    assert!(first.runtime_task_transaction_gate.try_read().is_ok());
}

#[test]
fn caller_abort_keeps_started_child_deletion_locked() {
    cancellation_keeps_started_child_deletion_locked(Delete::Single, false);
}

#[test]
fn caller_abort_keeps_started_child_cleanup_locked() {
    cancellation_keeps_started_child_deletion_locked(Delete::Cleanup, false);
}

#[test]
fn runtime_shutdown_keeps_started_child_deletion_locked() {
    cancellation_keeps_started_child_deletion_locked(Delete::Single, true);
}

#[test]
fn runtime_shutdown_keeps_started_child_cleanup_locked() {
    cancellation_keeps_started_child_deletion_locked(Delete::Cleanup, true);
}

async fn fixture(home: &Path) -> (SessionStoreV2, Session, Session) {
    let store = SessionStoreV2::new(home.into()).await.unwrap();
    let root = Session::new(ROOT, "model");
    store.save_session(&root).await.unwrap();
    let child = Session::new_child_of(CHILD, &root, "model", "child");
    store.save_session(&child).await.unwrap();
    (store, root, child)
}

#[tokio::test]
async fn child_deletion_preserves_pin_force_and_missing_results() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, mut child) = fixture(home.path()).await;
    child.pinned = true;
    store.save_session(&child).await.unwrap();
    let error = store
        .delete_session_recursive(CHILD, false)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "refusing to delete pinned session without force"
    );
    assert!(store.load_session(CHILD).await.unwrap().unwrap().pinned);
    assert!(store.delete_session_recursive(CHILD, true).await.unwrap());
    assert!(store.get_index_entry(CHILD).await.is_none());
    assert!(store.load_session(CHILD).await.unwrap().is_none());
    assert!(!store.delete_session_recursive(CHILD, true).await.unwrap());
    assert!(store.load_session(ROOT).await.unwrap().is_some());
}

#[tokio::test]
async fn child_cleanup_preserves_pinned_sibling_and_root_delete_still_removes_tree() {
    let home = tempfile::tempdir().unwrap();
    let (store, root, _) = fixture(home.path()).await;
    let mut sibling = Session::new_child_of(SIBLING, &root, "model", "sibling");
    sibling.pinned = true;
    store.save_session(&sibling).await.unwrap();
    let result = store.cleanup(CleanupMode::Children, true).await.unwrap();
    assert_eq!(result.deleted_count, 1);
    assert_eq!(result.deleted_session_ids, [CHILD]);
    assert!(store.load_session(CHILD).await.unwrap().is_none());
    assert!(store.load_session(SIBLING).await.unwrap().unwrap().pinned);
    assert!(store.load_session(ROOT).await.unwrap().is_some());
    let result = store.cleanup(CleanupMode::All, true).await.unwrap();
    assert_eq!(result.deleted_count, 0);
    assert!(store.delete_session_recursive(ROOT, true).await.unwrap());
    assert!(!home.path().join("sessions").join(ROOT).exists());
    assert!(store.get_index_entry(ROOT).await.is_none());
    assert!(store.get_index_entry(SIBLING).await.is_none());
}

#[tokio::test]
async fn child_deletion_preserves_existing_ignored_removal_error() {
    let home = tempfile::tempdir().unwrap();
    let (store, _, _) = fixture(home.path()).await;
    store.flush_search_index().await;
    let path = home
        .path()
        .join("sessions")
        .join(ROOT)
        .join("children")
        .join(CHILD);
    std::fs::remove_dir_all(&path).unwrap();
    std::fs::write(&path, b"not a directory").unwrap();
    // The preexisting Child branch removes its index entry even when the
    // physical remove fails. Guard ownership must not change that policy.
    assert!(store.delete_session_recursive(CHILD, false).await.unwrap());
    assert!(store.get_index_entry(CHILD).await.is_none());
    assert_eq!(std::fs::read(&path).unwrap(), b"not a directory");
    assert!(store.session_lifecycle_lock.try_read().is_ok());
    assert!(store.runtime_task_transaction_gate.try_read().is_ok());
}

const GRANDCHILD: &str = "delete-guard-grandchild";

fn descendant_claim(id: &str) -> bamboo_domain::ActorActivationClaim {
    let now = Utc::now();
    bamboo_domain::ActorActivationClaim {
        actor_id: id.into(),
        run_id: format!("run-{id}"),
        lease_owner: "deletion-test".into(),
        lease_expires_at: now + chrono::Duration::minutes(5),
        inbox_generation: 0,
        placement_ref: None,
        now,
    }
}

async fn active_descendant_fixture(
    home: &Path,
) -> (
    Arc<SessionStoreV2>,
    Session,
    Session,
    bamboo_domain::ActorActivation,
) {
    use bamboo_domain::ActorDirectoryPort;
    let (store, root, child) = fixture(home).await;
    let mut grandchild = Session::new_child_of(GRANDCHILD, &child, "model", "grandchild");
    grandchild.pinned = true;
    store.save_session(&grandchild).await.unwrap();
    let activation = store
        .claim_activation(&descendant_claim(GRANDCHILD))
        .await
        .unwrap();
    store
        .start_activation(&activation.fence(), Utc::now())
        .await
        .unwrap();
    (Arc::new(store), root, child, activation)
}

fn descendant_row(home: &Path, root: &str, id: &str) -> bamboo_domain::ActorDirectoryEntry {
    serde_json::from_slice(
        &std::fs::read(
            home.join("sessions")
                .join(root)
                .join("children")
                .join(id)
                .join("actor-authority.json"),
        )
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn descendant_cancellation_covers_deeper_rows_and_preserves_other_scopes() {
    use bamboo_domain::{
        ActorActivationStatus, ActorDirectoryError, ActorDirectoryPort, ActorLogicalState,
    };
    let home = tempfile::tempdir().unwrap();
    let (store, root, child, old) = active_descendant_fixture(home.path()).await;
    let grandchild = store.load_session(GRANDCHILD).await.unwrap().unwrap();
    let deep = Session::new_child_of("delete-deep", &grandchild, "model", "deep");
    let sibling = Session::new_child_of(SIBLING, &root, "model", "sibling");
    let foreign_root = Session::new("delete-foreign-root", "model");
    let foreign = Session::new_child_of("delete-foreign-child", &foreign_root, "model", "foreign");
    for session in [&deep, &sibling, &foreign_root, &foreign] {
        store.save_session(session).await.unwrap();
    }
    let deep_owner = store
        .claim_activation(&descendant_claim(&deep.id))
        .await
        .unwrap();
    let sibling_owner = store
        .claim_activation(&descendant_claim(SIBLING))
        .await
        .unwrap();
    let foreign_owner = store
        .claim_activation(&descendant_claim(&foreign.id))
        .await
        .unwrap();
    let before_sibling = descendant_row(home.path(), ROOT, SIBLING);
    let before_foreign = descendant_row(home.path(), &foreign_root.id, &foreign.id);
    let main_path = home
        .path()
        .join("sessions")
        .join(ROOT)
        .join("children")
        .join(GRANDCHILD)
        .join("session.json");
    let main_before = std::fs::read(&main_path).unwrap();
    assert!(store.delete_session_recursive(CHILD, false).await.unwrap());
    assert_eq!(std::fs::read(main_path).unwrap(), main_before);
    for (id, owner) in [(GRANDCHILD, &old), (deep.id.as_str(), &deep_owner)] {
        let row = descendant_row(home.path(), ROOT, id);
        assert_eq!(row.actor.state, ActorLogicalState::Cold);
        let activation = row.activation.unwrap();
        assert_eq!(activation.status, ActorActivationStatus::Cancelled);
        assert_eq!(activation.lease_epoch, owner.lease_epoch + 1);
    }
    assert_eq!(descendant_row(home.path(), ROOT, SIBLING), before_sibling);
    assert_eq!(
        descendant_row(home.path(), &foreign_root.id, &foreign.id),
        before_foreign
    );
    store.save_session(&child).await.unwrap();
    let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
    for owner in [&old, &deep_owner] {
        assert_eq!(
            reopened
                .validate_fence(&owner.fence(), Utc::now())
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
    }
    reopened
        .validate_fence(&sibling_owner.fence(), Utc::now())
        .await
        .unwrap();
    reopened
        .validate_fence(&foreign_owner.fence(), Utc::now())
        .await
        .unwrap();
}

#[tokio::test]
async fn child_deletion_preserves_legacy_and_interrupted_inert_actor_initialization() {
    use bamboo_domain::{ActorDirectoryEntry, ActorDirectoryPort, ActorSession};
    let home = tempfile::tempdir().unwrap();
    let (store, root, child, _) = active_descendant_fixture(home.path()).await;
    let grandchild = store.load_session(GRANDCHILD).await.unwrap().unwrap();
    let inert = Session::new_child_of("delete-inert", &grandchild, "model", "inert");
    let legacy = Session::new_child_of(SIBLING, &root, "model", "legacy");
    store.save_session(&inert).await.unwrap();
    store.save_session(&legacy).await.unwrap();
    let mut actor = ActorSession::from_session(&inert).unwrap();
    actor.ancestor_observations = store.validate_actor_lineage(&actor).await.unwrap();
    let bytes = serde_json::to_vec_pretty(&ActorDirectoryEntry::new(actor)).unwrap();
    let children = home.path().join("sessions").join(ROOT).join("children");
    let row_path = children.join(&inert.id).join("actor-authority.json");
    let marker_path = children
        .join(&inert.id)
        .join("actor-authority.initialized.json");
    std::fs::write(&row_path, &bytes).unwrap();
    assert!(store.delete_session_recursive(CHILD, false).await.unwrap());
    assert_eq!(std::fs::read(&row_path).unwrap(), bytes);
    assert!(!marker_path.exists());
    assert!(!children.join(SIBLING).join("actor-authority.json").exists());
    assert!(!children
        .join(SIBLING)
        .join("actor-authority.initialized.json")
        .exists());
    store.save_session(&child).await.unwrap();
    let fresh = store
        .claim_activation(&descendant_claim(&inert.id))
        .await
        .unwrap();
    store
        .validate_fence(&fresh.fence(), Utc::now())
        .await
        .unwrap();
    assert_eq!(fresh.attempt, 1);
    assert!(marker_path.is_file());
}

#[tokio::test]
async fn descendant_preflight_failure_keeps_ancestor_and_all_live_rows() {
    use bamboo_domain::ActorDirectoryPort;
    for failure in ["missing-marker", "epoch-overflow", "revision-overflow"] {
        let home = tempfile::tempdir().unwrap();
        let (store, _, _, _) = active_descendant_fixture(home.path()).await;
        let grandchild = store.load_session(GRANDCHILD).await.unwrap().unwrap();
        let deep = Session::new_child_of("delete-preflight-deep", &grandchild, "model", "deep");
        store.save_session(&deep).await.unwrap();
        store
            .claim_activation(&descendant_claim(&deep.id))
            .await
            .unwrap();
        let directory = home
            .path()
            .join("sessions")
            .join(ROOT)
            .join("children")
            .join(GRANDCHILD);
        match failure {
            "missing-marker" => {
                std::fs::remove_file(directory.join("actor-authority.initialized.json")).unwrap();
            }
            _ => {
                let mut row = descendant_row(home.path(), ROOT, GRANDCHILD);
                if failure == "epoch-overflow" {
                    row.activation.as_mut().unwrap().lease_epoch = u64::MAX;
                } else {
                    row.revision = u64::MAX;
                }
                std::fs::write(
                    directory.join("actor-authority.json"),
                    serde_json::to_vec_pretty(&row).unwrap(),
                )
                .unwrap();
            }
        }
        let before = descendant_row(home.path(), ROOT, GRANDCHILD);
        let before_deep = descendant_row(home.path(), ROOT, &deep.id);
        let error = store
            .delete_session_recursive(CHILD, false)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{failure}");
        assert!(store.load_session(CHILD).await.unwrap().is_some());
        assert_eq!(descendant_row(home.path(), ROOT, GRANDCHILD), before);
        assert_eq!(descendant_row(home.path(), ROOT, &deep.id), before_deep);
    }
}

#[tokio::test]
async fn pinned_refusal_leaves_descendant_live_and_forced_delete_cancels_it() {
    use bamboo_domain::{ActorDirectoryError, ActorDirectoryPort};
    let home = tempfile::tempdir().unwrap();
    let (store, _, mut child, old) = active_descendant_fixture(home.path()).await;
    child.pinned = true;
    store.save_session(&child).await.unwrap();
    let before = descendant_row(home.path(), ROOT, GRANDCHILD);
    assert!(store.delete_session_recursive(CHILD, false).await.is_err());
    assert_eq!(descendant_row(home.path(), ROOT, GRANDCHILD), before);
    store
        .validate_fence(&old.fence(), Utc::now())
        .await
        .unwrap();
    assert!(!store
        .delete_session_recursive("missing-child", false)
        .await
        .unwrap());
    assert_eq!(descendant_row(home.path(), ROOT, GRANDCHILD), before);
    assert!(store.delete_session_recursive(CHILD, true).await.unwrap());
    store.save_session(&child).await.unwrap();
    assert_eq!(
        store
            .validate_fence(&old.fence(), Utc::now())
            .await
            .unwrap_err(),
        ActorDirectoryError::StaleFence
    );
}

#[tokio::test]
async fn cleanup_cancels_surviving_pinned_descendant_before_ancestor_removal() {
    use bamboo_domain::{ActorDirectoryError, ActorDirectoryPort};
    let home = tempfile::tempdir().unwrap();
    let (store, root, child, old) = active_descendant_fixture(home.path()).await;
    let mut sibling = Session::new_child_of(SIBLING, &root, "model", "sibling");
    sibling.pinned = true;
    store.save_session(&sibling).await.unwrap();
    let result = store.cleanup(CleanupMode::Children, true).await.unwrap();
    assert_eq!(result.deleted_session_ids, [CHILD]);
    assert!(
        store
            .load_session(GRANDCHILD)
            .await
            .unwrap()
            .unwrap()
            .pinned
    );
    assert!(store.load_session(SIBLING).await.unwrap().unwrap().pinned);
    store.save_session(&child).await.unwrap();
    assert_eq!(
        store
            .validate_fence(&old.fence(), Utc::now())
            .await
            .unwrap_err(),
        ActorDirectoryError::StaleFence
    );
}

struct ReleaseActorHook(Arc<super::actor_directory_lifetime_tests::ActorWriteHook>);
impl Drop for ReleaseActorHook {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn cancelled_descendant_invalidation_retains_guards(shutdown: bool) {
    use super::actor_directory_lifetime_tests::ActorWriteHook;
    use bamboo_domain::{ActorDirectoryError, ActorDirectoryPort};
    let home = tempfile::tempdir().unwrap();
    let mut deleting_runtime = Some(runtime());
    let (first, second, mut root, old) = deleting_runtime.as_ref().unwrap().block_on(async {
        let (first, root, _, old) = active_descendant_fixture(home.path()).await;
        let second = SessionStoreV2::new(home.path().into()).await.unwrap();
        (first, second, root, old)
    });
    let hook = ActorWriteHook::install(
        &first,
        "actor-authority.json",
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = ReleaseActorHook(hook.clone());
    let job = {
        let first = first.clone();
        deleting_runtime
            .as_ref()
            .unwrap()
            .spawn(async move { first.delete_session_recursive(CHILD, false).await })
    };
    hook.wait_entered();
    assert_delete_locks_held(&first);
    if shutdown {
        deleting_runtime.take().unwrap().shutdown_background();
    } else {
        job.abort();
    }
    let successor_runtime = runtime();
    assert!(successor_runtime.block_on(job).unwrap_err().is_cancelled());
    assert_delete_locks_held(&first);
    hook.release();
    root.title = "independent writer after descendant cancellation".into();
    successor_runtime.block_on(async {
        second.save_session(&root).await.unwrap();
        // Caller cancellation prevented the later physical removal from starting.
        assert!(second.load_session(CHILD).await.unwrap().is_some());
        assert_eq!(
            second
                .validate_fence(&old.fence(), Utc::now())
                .await
                .unwrap_err(),
            ActorDirectoryError::StaleFence
        );
        let fresh = second
            .claim_activation(&descendant_claim(GRANDCHILD))
            .await
            .unwrap();
        second
            .start_activation(&fresh.fence(), Utc::now())
            .await
            .unwrap();
        second
            .validate_fence(&fresh.fence(), Utc::now())
            .await
            .unwrap();
    });
}

#[test]
fn caller_abort_keeps_started_descendant_invalidation_locked() {
    cancelled_descendant_invalidation_retains_guards(false);
}

#[test]
fn runtime_shutdown_keeps_started_descendant_invalidation_locked() {
    cancelled_descendant_invalidation_retains_guards(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descendant_publication_error_keeps_ancestor_and_reports_actual_row_state() {
    use super::actor_directory_lifetime_tests::ActorWriteHook;
    use bamboo_domain::{ActorDirectoryError, ActorDirectoryPort};
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir().unwrap();
        let (store, _, _, old) = active_descendant_fixture(home.path()).await;
        let before = descendant_row(home.path(), ROOT, GRANDCHILD);
        let hook = ActorWriteHook::install(&store, "actor-authority.json", phase, true);
        let _release = ReleaseActorHook(hook.clone());
        let job = {
            let store = store.clone();
            tokio::spawn(async move { store.delete_session_recursive(CHILD, false).await })
        };
        hook.wait_entered();
        assert_delete_locks_held(&store);
        hook.release();
        assert!(job.await.unwrap().is_err());
        assert!(store.load_session(CHILD).await.unwrap().is_some());
        if phase == DurableWritePhase::BeforeReplace {
            assert_eq!(descendant_row(home.path(), ROOT, GRANDCHILD), before);
            store
                .validate_fence(&old.fence(), Utc::now())
                .await
                .unwrap();
        } else {
            assert_eq!(
                store
                    .validate_fence(&old.fence(), Utc::now())
                    .await
                    .unwrap_err(),
                ActorDirectoryError::StaleFence
            );
        }
    }
}
