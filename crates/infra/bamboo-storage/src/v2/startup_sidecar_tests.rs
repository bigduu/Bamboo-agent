//! Actual startup entrypoint, final candidate checks and physical job lifetime.
use super::default_actor_context_tests::DefaultWriteHook;
use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryEntry, ActorDirectoryPort,
    ConversationSummary, Message, SessionAuthorityConflict,
};
use chrono::Duration as ChronoDuration;

const ID: &str = "startup-child";
const DEADLINE: Duration = Duration::from_secs(10);
const RECORD: &str = "actor-authority.json";
const MARKER: &str = "actor-authority.initialized.json";

#[derive(Clone, Debug)]
pub(super) struct ScanPause {
    pub(super) id: String,
    pub(super) reached: Arc<tokio::sync::Barrier>,
    pub(super) release: Arc<tokio::sync::Barrier>,
}
fn pause(store: &SessionStoreV2) -> ScanPause {
    let pause = ScanPause {
        id: ID.into(),
        reached: Arc::new(tokio::sync::Barrier::new(2)),
        release: Arc::new(tokio::sync::Barrier::new(2)),
    };
    *store.migration_scan_pause.lock().unwrap() = Some(pause.clone());
    pause
}
fn claim() -> ActorActivationClaim {
    let now = Utc::now();
    ActorActivationClaim {
        actor_id: ID.into(),
        run_id: "run".into(),
        lease_owner: "owner".into(),
        lease_expires_at: now + ChronoDuration::minutes(5),
        inbox_generation: 0,
        placement_ref: None,
        now,
    }
}
fn directory(home: &Path) -> PathBuf {
    home.join("sessions/startup-root/children").join(ID)
}
fn snapshot(path: &Path) -> Vec<Option<Vec<u8>>> {
    ["session.json", RUNTIME_SIDECAR_FILE, RECORD, MARKER]
        .map(|file| std::fs::read(path.join(file)).ok())
        .into()
}
fn rejected(error: io::Error) {
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert!(error
        .get_ref()
        .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()));
}
async fn setup(home: &Path, cold: bool) -> (Arc<SessionStoreV2>, Arc<SessionStoreV2>, Session) {
    let first = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    let root = Session::new("startup-root", "model");
    first.save_session(&root).await.unwrap();
    let mut child = Session::new_child_of(ID, &root, "model", "child");
    child.add_message(Message::user("retained history"));
    child.conversation_summary = Some(ConversationSummary::new("legacy summary", 1, 10));
    first.save_session(&child).await.unwrap();
    if cold {
        first.ensure_actor(ID).await.unwrap();
    }
    // Constructor recovery has other locks; create the contender before parking.
    let second = Arc::new(SessionStoreV2::new(home.into()).await.unwrap());
    (first, second, child)
}
fn remove_runtime(home: &Path) {
    std::fs::remove_file(directory(home).join(RUNTIME_SIDECAR_FILE)).unwrap();
}
fn no_marker(home: &Path) {
    assert!(!home.join(RUNTIME_SIDECAR_MIGRATION_MARKER).exists());
}

#[tokio::test]
async fn startup_sidecar_allows_legacy_and_inert_cold_without_changing_main_or_actor() {
    for cold in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let (first, _, child) = setup(home.path(), cold).await;
        remove_runtime(home.path());
        let dir = directory(home.path());
        let before = snapshot(&dir);
        assert_eq!(first.migrate_runtime_sidecars().await.unwrap(), 1);
        let after = snapshot(&dir);
        for index in [0, 2, 3] {
            assert_eq!(before[index], after[index]);
        }
        let side: Session = serde_json::from_slice(after[1].as_ref().unwrap()).unwrap();
        assert!(side.messages.is_empty());
        assert!(side.provider_transcript.is_empty());
        assert_eq!(side.created_at, child.created_at);
        assert_eq!(
            serde_json::to_value(&side.conversation_summary).unwrap(),
            serde_json::to_value(&child.conversation_summary).unwrap()
        );
        assert_eq!(
            serde_json::to_value(first.load_session(ID).await.unwrap().unwrap().messages).unwrap(),
            serde_json::to_value(&child.messages).unwrap()
        );
        assert_eq!(first.migrate_runtime_sidecars().await.unwrap(), 0);
        assert!(home.path().join(RUNTIME_SIDECAR_MIGRATION_MARKER).exists());
    }
}

#[tokio::test]
async fn startup_sidecar_rejects_activated_terminal_and_unknown_authority_without_repair() {
    for case in [
        "reserved",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "retired",
        "retired-zero",
        "record-only",
        "marker-only",
        "bad-record",
        "bad-marker",
        "schema",
        "record-birth",
        "marker-birth",
        "lineage",
        "nonregular",
    ] {
        let home = tempfile::tempdir().unwrap();
        let (first, _, _) = setup(home.path(), true).await;
        let dir = directory(home.path());
        match case {
            "reserved" | "running" | "succeeded" | "failed" | "cancelled" | "retired" => {
                let activation = first.claim_activation(&claim()).await.unwrap();
                if case != "reserved" {
                    first
                        .start_activation(&activation.fence(), Utc::now())
                        .await
                        .unwrap();
                }
                match case {
                    "succeeded" | "failed" | "cancelled" => {
                        let finish = match case {
                            "succeeded" => ActorActivationFinish::Succeeded,
                            "failed" => ActorActivationFinish::Failed,
                            _ => ActorActivationFinish::Cancelled,
                        };
                        first
                            .finish_activation(&activation.fence(), Utc::now(), finish)
                            .await
                            .unwrap();
                    }
                    "retired" => {
                        first.retire_actor(ID, Utc::now()).await.unwrap();
                    }
                    _ => {}
                }
            }
            "retired-zero" => {
                first.retire_actor(ID, Utc::now()).await.unwrap();
            }
            "record-only" => std::fs::remove_file(dir.join(MARKER)).unwrap(),
            "marker-only" => std::fs::remove_file(dir.join(RECORD)).unwrap(),
            "bad-record" => std::fs::write(dir.join(RECORD), b"{").unwrap(),
            "bad-marker" => std::fs::write(dir.join(MARKER), b"{").unwrap(),
            "nonregular" => {
                std::fs::remove_file(dir.join(RECORD)).unwrap();
                std::fs::create_dir(dir.join(RECORD)).unwrap();
            }
            "marker-birth" => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(dir.join(MARKER)).unwrap()).unwrap();
                value["session_created_at"] =
                    serde_json::json!(Utc::now() + ChronoDuration::days(1));
                std::fs::write(dir.join(MARKER), serde_json::to_vec(&value).unwrap()).unwrap();
            }
            _ => {
                let mut record: ActorDirectoryEntry =
                    serde_json::from_slice(&std::fs::read(dir.join(RECORD)).unwrap()).unwrap();
                match case {
                    "schema" => record.schema_version += 1,
                    "record-birth" => record.actor.session_created_at += ChronoDuration::days(1),
                    "lineage" => record.actor.parent_actor_id = Some("foreign-parent".into()),
                    _ => unreachable!(),
                }
                std::fs::write(dir.join(RECORD), serde_json::to_vec(&record).unwrap()).unwrap();
            }
        }
        remove_runtime(home.path());
        let before = snapshot(&dir);
        rejected(first.migrate_runtime_sidecars().await.unwrap_err());
        assert_eq!(snapshot(&dir), before, "case {case}");
        no_marker(home.path());
        if case == "nonregular" {
            assert!(dir.join(RECORD).is_dir());
        }
    }
}

#[tokio::test]
async fn startup_sidecar_final_check_rejects_wrong_hint_main_and_nonregular_targets() {
    for case in [
        "hint-escape",
        "hint-kind",
        "hint-birth",
        "main-id",
        "main-parent",
        "main-depth",
        "main-birth",
        "side-directory",
        "main-directory",
        "missing-main",
        "malformed-main",
    ] {
        let home = tempfile::tempdir().unwrap();
        let (first, _, mut child) = setup(home.path(), false).await;
        remove_runtime(home.path());
        let dir = directory(home.path());
        match case {
            "hint-escape" | "hint-kind" | "hint-birth" => {
                let mut index = first.index.write().await;
                let entry = index.sessions.get_mut(ID).unwrap();
                match case {
                    "hint-escape" => entry.rel_path = "../foreign".into(),
                    "hint-kind" => entry.kind = SessionKind::Root,
                    _ => entry.created_at += ChronoDuration::days(1),
                }
            }
            "side-directory" => std::fs::create_dir(dir.join(RUNTIME_SIDECAR_FILE)).unwrap(),
            "main-directory" => {
                std::fs::remove_file(dir.join("session.json")).unwrap();
                std::fs::create_dir(dir.join("session.json")).unwrap();
            }
            "missing-main" => std::fs::remove_file(dir.join("session.json")).unwrap(),
            "malformed-main" => std::fs::write(dir.join("session.json"), b"{").unwrap(),
            _ => {
                match case {
                    "main-id" => child.id = "foreign".into(),
                    "main-parent" => child.parent_session_id = Some("foreign".into()),
                    "main-depth" => child.spawn_depth += 1,
                    "main-birth" => child.created_at += ChronoDuration::days(1),
                    _ => unreachable!(),
                }
                std::fs::write(
                    dir.join("session.json"),
                    serde_json::to_vec(&child).unwrap(),
                )
                .unwrap();
            }
        }
        let before = snapshot(&dir);
        let result = first.migrate_runtime_sidecars().await;
        if matches!(case, "missing-main" | "malformed-main") {
            assert_eq!(result.unwrap(), 0);
        } else {
            assert!(result.is_err(), "case {case}");
            no_marker(home.path());
        }
        assert_eq!(snapshot(&dir), before, "case {case}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_sidecar_final_reread_skips_concurrent_runtime_and_observes_current_main() {
    for publish_runtime in [true, false] {
        let home = tempfile::tempdir().unwrap();
        let (first, second, mut current) = setup(home.path(), false).await;
        remove_runtime(home.path());
        let gate = pause(&first);
        let job = {
            let first = first.clone();
            tokio::spawn(async move { first.migrate_runtime_sidecars().await })
        };
        gate.reached.wait().await;
        current.conversation_summary.as_mut().unwrap().content = "current main".into();
        let path = directory(home.path());
        // Trusted legacy fixture updates the actual main under the exact
        // Session boundary. Default full-save correctly rejects a missing
        // Child runtime; the other migration is the legitimate backfill writer.
        {
            let _guard = second.acquire_session_maintenance_lock(ID).await.unwrap();
            std::fs::write(
                path.join("session.json"),
                serde_json::to_vec(&current).unwrap(),
            )
            .unwrap();
        }
        let expected = if publish_runtime {
            assert_eq!(second.migrate_runtime_sidecars().await.unwrap(), 1);
            std::fs::read(path.join(RUNTIME_SIDECAR_FILE)).unwrap()
        } else {
            serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&current)).unwrap()
        };
        gate.release.wait().await;
        assert_eq!(job.await.unwrap().unwrap(), usize::from(!publish_runtime));
        assert_eq!(
            std::fs::read(path.join(RUNTIME_SIDECAR_FILE)).unwrap(),
            expected
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_sidecar_claim_after_scan_rejects_before_publication() {
    let home = tempfile::tempdir().unwrap();
    let (first, second, _) = setup(home.path(), true).await;
    remove_runtime(home.path());
    let gate = pause(&first);
    let job = {
        let first = first.clone();
        tokio::spawn(async move { first.migrate_runtime_sidecars().await })
    };
    gate.reached.wait().await;
    second.claim_activation(&claim()).await.unwrap();
    let before = snapshot(&directory(home.path()));
    gate.release.wait().await;
    rejected(job.await.unwrap().unwrap_err());
    assert_eq!(snapshot(&directory(home.path())), before);
    no_marker(home.path());
}

struct Release(Arc<DefaultWriteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Cancelled(std::sync::mpsc::Sender<()>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
fn physical_locks_held(store: &SessionStoreV2) {
    for path in [
        store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        store.session_write_lock_path(ID),
    ] {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let result = FileExt::try_lock_exclusive(&file);
        if result.is_ok() {
            FileExt::unlock(&file).unwrap();
        }
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }
}
async fn verify_after_successor(
    home: &Path,
    expected: &Session,
    activation: bamboo_domain::ActorActivation,
) {
    let store = SessionStoreV2::new(home.into()).await.unwrap();
    let before = snapshot(&directory(home));
    assert_eq!(
        store.inspect_actor(ID).await.unwrap().activation,
        Some(activation)
    );
    let loaded = store.load_session(ID).await.unwrap().unwrap();
    assert_eq!(loaded.created_at, expected.created_at);
    assert_eq!(
        serde_json::to_value(&loaded.conversation_summary).unwrap(),
        serde_json::to_value(&expected.conversation_summary).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&loaded.messages).unwrap(),
        serde_json::to_value(&expected.messages).unwrap()
    );
    assert_eq!(snapshot(&directory(home)), before);
    no_marker(home);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_sidecar_caller_abort_retains_all_physical_guards_until_successor() {
    let home = tempfile::tempdir().unwrap();
    let (first, second, child) = setup(home.path(), true).await;
    remove_runtime(home.path());
    let hook = DefaultWriteHook::install(
        &first,
        RUNTIME_SIDECAR_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let job = {
        let first = first.clone();
        tokio::spawn(async move { first.migrate_runtime_sidecars().await })
    };
    hook.wait();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    physical_locks_held(&first);
    let successor = {
        let second = second.clone();
        tokio::spawn(async move { second.claim_activation(&claim()).await })
    };
    assert!(tokio::time::timeout(Duration::from_millis(50), async {
        while !successor.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_err());
    physical_locks_held(&first);
    hook.release();
    let activation = tokio::time::timeout(DEADLINE, successor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    verify_after_successor(home.path(), &child, activation).await;
}

#[test]
fn startup_sidecar_runtime_shutdown_retains_started_job_guards() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (first, second, child) = runtime.block_on(setup(home.path(), true));
    remove_runtime(home.path());
    let hook = DefaultWriteHook::install(
        &first,
        RUNTIME_SIDECAR_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let (tx, rx) = std::sync::mpsc::channel();
    let job = {
        let first = first.clone();
        runtime.spawn(async move {
            let _cancelled = Cancelled(tx);
            first.migrate_runtime_sidecars().await
        })
    };
    hook.wait();
    runtime.shutdown_timeout(Duration::from_millis(20));
    rx.recv_timeout(DEADLINE).unwrap();
    physical_locks_held(&first);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let successor = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            assert!(job.await.unwrap_err().is_cancelled());
            started_tx.send(()).unwrap();
            done_tx
                .send(second.claim_activation(&claim()).await.unwrap())
                .unwrap();
        });
    });
    started_rx.recv_timeout(DEADLINE).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
    physical_locks_held(&first);
    hook.release();
    let activation = done_rx.recv_timeout(DEADLINE).unwrap();
    successor.join().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(verify_after_successor(home.path(), &child, activation));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_sidecar_replace_errors_cleanup_and_partial_rerun_preserve_confirmation_limits() {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir().unwrap();
        let (first, second, child) = setup(home.path(), true).await;
        let root = first.load_session("startup-root").await.unwrap().unwrap();
        let other = Session::new_child_of("other-startup-child", &root, "model", "other");
        first.save_session(&other).await.unwrap();
        remove_runtime(home.path());
        let other_dir = home
            .path()
            .join("sessions/startup-root/children/other-startup-child");
        std::fs::remove_file(other_dir.join(RUNTIME_SIDECAR_FILE)).unwrap();
        let hook = DefaultWriteHook::install(&first, RUNTIME_SIDECAR_FILE, phase, true);
        let _release = Release(hook.clone());
        let job = {
            let first = first.clone();
            tokio::spawn(async move { first.migrate_runtime_sidecars().await })
        };
        hook.wait();
        hook.release();
        assert!(job.await.unwrap().is_err());
        no_marker(home.path());
        let count = [directory(home.path()), other_dir.clone()]
            .iter()
            .filter(|dir| dir.join(RUNTIME_SIDECAR_FILE).exists())
            .count();
        assert_eq!(count, usize::from(phase == DurableWritePhase::AfterReplace));
        for dir in [directory(home.path()), other_dir] {
            assert!(std::fs::read_dir(dir).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("durable.tmp.")));
        }
        assert_eq!(first.migrate_runtime_sidecars().await.unwrap(), 2 - count);
        assert_eq!(first.migrate_runtime_sidecars().await.unwrap(), 0);
        assert_eq!(second.claim_activation(&claim()).await.unwrap().attempt, 1);
        assert_eq!(
            first.load_session(ID).await.unwrap().unwrap().created_at,
            child.created_at
        );
    }
}
