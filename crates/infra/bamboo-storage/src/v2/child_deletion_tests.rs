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
