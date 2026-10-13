//! Actual management publication jobs and independent Store/file-lock barriers.

use std::sync::{Condvar, Mutex as StdMutex};

use super::*;
use bamboo_domain::{
    CompressionEvent, CompressionTriggerType, ConversationSummary, Message,
    SupervisorManagementMutation as Mutation, SupervisorManagementRequest, SupervisorReference,
};
use supervisor_proof::{SupervisorProofFault as Stage, SUPERVISOR_PROOF_FILE};

const TARGET: &str = "a-managed-root";
const DEADLINE: Duration = Duration::from_secs(10);
const STAGES: [Stage; 3] = [Stage::Prepared, Stage::Runtime, Stage::Committed];
type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, Default)]
struct Latch {
    entered: bool,
    released: bool,
}

#[derive(Debug)]
pub(super) struct ManagementWriteHook {
    stage: Stage,
    phase: DurableWritePhase,
    fail: bool,
    latch: StdMutex<Latch>,
    wake: Condvar,
}

impl ManagementWriteHook {
    fn install(
        store: &SessionStoreV2,
        stage: Stage,
        phase: DurableWritePhase,
        fail: bool,
    ) -> Arc<Self> {
        let hook = Arc::new(Self {
            stage,
            phase,
            fail,
            latch: StdMutex::new(Latch::default()),
            wake: Condvar::new(),
        });
        *store.management_write_hook.lock().unwrap() = Some(Arc::clone(&hook));
        hook
    }

    pub(super) fn visit(
        &self,
        stage: Stage,
        path: &Path,
        phase: DurableWritePhase,
    ) -> io::Result<()> {
        if stage != self.stage || phase != self.phase {
            return Ok(());
        }
        let file = if stage == Stage::Runtime {
            RUNTIME_SIDECAR_FILE
        } else {
            SUPERVISOR_PROOF_FILE
        };
        assert_eq!(path.file_name().unwrap(), file);
        let mut latch = self.latch.lock().unwrap();
        if latch.entered {
            return Ok(());
        }
        latch.entered = true;
        self.wake.notify_all();
        while !latch.released {
            latch = self.wake.wait(latch).unwrap();
        }
        if self.fail {
            Err(io::Error::other("injected management publication failure"))
        } else {
            Ok(())
        }
    }

    fn wait_entered(&self) {
        let latch = self.latch.lock().unwrap();
        let (latch, timeout) = self
            .wake
            .wait_timeout_while(latch, DEADLINE, |l| !l.entered)
            .unwrap();
        assert!(
            latch.entered && !timeout.timed_out(),
            "management job never entered barrier"
        );
    }

    fn release(&self) {
        self.latch.lock().unwrap().released = true;
        self.wake.notify_all();
    }
}

struct ReleaseOnDrop(Arc<ManagementWriteHook>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Files {
    main: Vec<u8>,
    runtime: Vec<u8>,
    proof: Vec<u8>,
    target_main: Option<Vec<u8>>,
    target_runtime: Option<Vec<u8>>,
}

struct Fixture {
    home: tempfile::TempDir,
    first: Arc<SessionStoreV2>,
    second: Arc<SessionStoreV2>,
    reference: SupervisorReference,
    request: SupervisorManagementRequest,
    before: Files,
    locks_target: bool,
}

fn scope(project: &str) -> Mutation {
    Mutation::ConfigureProjectScope {
        allowed_projects: [project.parse().unwrap()].into(),
    }
}

async fn change(
    store: &SessionStoreV2,
    reference: &SupervisorReference,
    expected_state_revision: u64,
    mutation: Mutation,
) -> io::Result<bamboo_domain::SupervisorManagementReceipt> {
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision,
            mutation,
        })
        .await
}

fn directory(home: &Path) -> PathBuf {
    home.join("sessions").join(DEFAULT_SUPERVISOR_SESSION_ID)
}

fn files(home: &Path) -> Files {
    let d = directory(home);
    let target = home.join("sessions").join(TARGET);
    Files {
        main: std::fs::read(d.join("session.json")).unwrap(),
        runtime: std::fs::read(d.join(RUNTIME_SIDECAR_FILE)).unwrap(),
        proof: std::fs::read(d.join(SUPERVISOR_PROOF_FILE)).unwrap(),
        target_main: std::fs::read(target.join("session.json")).ok(),
        target_runtime: std::fs::read(target.join(RUNTIME_SIDECAR_FILE)).ok(),
    }
}

async fn fixture(attach: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let first = Arc::new(SessionStoreV2::new(home.path().to_path_buf()).await?);
    let supervisor = first.get_or_create_default_supervisor("model").await?;
    let reference = SupervisorReference::from(&supervisor);
    let mut current = first.load_session(&reference.session_id).await?.unwrap();
    current.add_message(Message::user("retain the complete current context"));
    current.conversation_summary = Some(ConversationSummary::new("current summary", 2, 10));
    current.compression_events.push(CompressionEvent::new(
        2,
        1,
        80.0,
        40.0,
        10,
        CompressionTriggerType::Auto,
        0.5,
        None,
        0,
    ));
    current.model_context_state = Some(bamboo_domain::ModelContextState {
        state_revision: 7,
        ..Default::default()
    });
    first.save_session(&current).await?;
    let mut target = Session::new(TARGET, "independent-model");
    target.set_project_id_meta("project-a");
    target.metadata_version = 7;
    target.add_message(Message::user("independent target history"));
    first.save_session(&target).await?;
    change(&first, &reference, 0, scope("project-a")).await?;
    // Constructor recovery has exclusive gates: precreate before any barrier.
    let second = Arc::new(SessionStoreV2::new(home.path().to_path_buf()).await?);
    let request = SupervisorManagementRequest {
        supervisor: reference.clone(),
        expected_state_revision: 1,
        mutation: if attach {
            Mutation::Attach {
                target_session_id: TARGET.into(),
            }
        } else {
            scope("project-b")
        },
    };
    let before = files(home.path());
    Ok(Fixture {
        home,
        first,
        second,
        reference,
        request,
        before,
        locks_target: attach,
    })
}

fn lock_paths(f: &Fixture) -> Vec<PathBuf> {
    let mut paths = vec![
        f.first.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        f.first
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        f.first
            .session_write_lock_path(DEFAULT_SUPERVISOR_SESSION_ID),
    ];
    if f.locks_target {
        paths.push(f.first.session_write_lock_path(TARGET));
    }
    paths
}

fn exclusive_available(path: &Path) -> bool {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            FileExt::unlock(&file).unwrap();
            true
        }
        Err(e) => {
            assert_eq!(e.kind(), io::ErrorKind::WouldBlock);
            false
        }
    }
}

fn held(f: &Fixture) {
    for path in lock_paths(f) {
        assert!(
            !exclusive_available(&path),
            "job released {}",
            path.display()
        );
    }
}

fn wait_released(f: &Fixture) {
    let deadline = Instant::now() + DEADLINE;
    while !lock_paths(f).iter().all(|p| exclusive_available(p)) {
        assert!(
            Instant::now() < deadline,
            "management job did not release guards"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn value(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
}

fn without(mut value: serde_json::Value, fields: &[&str]) -> serde_json::Value {
    for field in fields {
        value.as_object_mut().unwrap().remove(*field);
    }
    value
}

fn preserved(f: &Fixture) -> Files {
    let after = files(f.home.path());
    assert_eq!(after.main, f.before.main);
    assert_eq!(after.target_main, f.before.target_main);
    assert_eq!(after.target_runtime, f.before.target_runtime);
    // All sidecar context and identity, not merely selected summary fields.
    assert_eq!(
        without(value(&after.runtime), &["supervisor_management"]),
        without(value(&f.before.runtime), &["supervisor_management"]),
    );
    assert_eq!(
        without(value(&after.proof), &["state", "supervisor_management"]),
        without(value(&f.before.proof), &["state", "supervisor_management"]),
    );
    for path in std::fs::read_dir(directory(f.home.path())).unwrap() {
        assert!(!path
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("durable.tmp."));
    }
    after
}

async fn outcome(f: &Fixture, stage: Stage, committed: bool) -> TestResult {
    let after = preserved(f);
    let proof = value(&after.proof);
    let runtime = value(&after.runtime);
    assert_eq!(
        proof["state"],
        if committed { "committed" } else { "prepared" }
    );
    assert_eq!(
        proof["supervisor_management"]["revision"],
        f.request.expected_state_revision + 1
    );
    if stage == Stage::Prepared {
        assert_eq!(after.runtime, f.before.runtime);
    } else {
        assert_eq!(
            runtime["supervisor_management"]["revision"],
            f.request.expected_state_revision + 1
        );
        assert_eq!(
            runtime["supervisor_management"],
            proof["supervisor_management"]
        );
    }
    let reopened = SessionStoreV2::new(f.home.path().to_path_buf()).await?;
    if committed {
        let observed = reopened.inspect_supervisor_scope(&f.reference).await?;
        assert_eq!(
            observed.state_revision,
            f.request.expected_state_revision + 1
        );
        // Exact old revision rejects; a real later owner can publish a new proof.
        assert_eq!(
            reopened
                .mutate_supervisor_management(&f.request)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mutation = if f.locks_target {
            Mutation::Detach {
                target_session_id: TARGET.into(),
            }
        } else {
            scope("project-a")
        };
        let receipt = change(&f.second, &f.reference, observed.state_revision, mutation).await?;
        assert!(receipt.changed);
        assert_eq!(receipt.state_revision, observed.state_revision + 1);
        if f.locks_target {
            assert_eq!(receipt.link_revision, Some(2));
        }
        let later = preserved(f);
        let cold = SessionStoreV2::new(f.home.path().to_path_buf()).await?;
        assert_eq!(
            cold.inspect_supervisor_scope(&f.reference)
                .await?
                .state_revision,
            receipt.state_revision
        );
        assert_eq!(
            files(f.home.path()),
            later,
            "no old job can overwrite the later owner"
        );
    } else {
        assert_eq!(
            reopened
                .inspect_supervisor_scope(&f.reference)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(reopened
            .load_root_authority(&f.reference.session_id)
            .await
            .is_err());
        assert!(f
            .second
            .mutate_supervisor_management(&f.request)
            .await
            .is_err());
        assert_eq!(
            files(f.home.path()),
            after,
            "pending proof must never be repaired"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_abort_retains_attach_lock_set_for_each_started_management_stage() -> TestResult {
    for stage in STAGES {
        let f = fixture(true).await?;
        let hook =
            ManagementWriteHook::install(&f.first, stage, DurableWritePhase::BeforeReplace, false);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let writer = {
            let first = Arc::clone(&f.first);
            let request = f.request.clone();
            tokio::spawn(async move { first.mutate_supervisor_management(&request).await })
        };
        hook.wait_entered();
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        held(&f);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let successor = {
            let second = Arc::clone(&f.second);
            let reference = f.reference.clone();
            tokio::spawn(async move {
                let _ = started_tx.send(());
                second.inspect_supervisor_scope(&reference).await
            })
        };
        started_rx.await?;
        assert!(!successor.is_finished());
        held(&f);
        hook.release();
        let observed = tokio::time::timeout(DEADLINE, successor).await??;
        assert_eq!(observed.is_ok(), stage == Stage::Committed);
        wait_released(&f);
        outcome(&f, stage, stage == Stage::Committed).await?;
    }
    Ok(())
}

struct Cancelled(std::sync::mpsc::Sender<()>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_after_runtime_replace_keeps_job_locked_and_pending_proof_unrepaired() -> TestResult
{
    let f = fixture(true).await?;
    let hook = ManagementWriteHook::install(
        &f.first,
        Stage::Runtime,
        DurableWritePhase::AfterReplace,
        false,
    );
    let _release = ReleaseOnDrop(Arc::clone(&hook));
    let (begin_timeout_tx, begin_timeout_rx) = tokio::sync::oneshot::channel();
    let writer = {
        let first = Arc::clone(&f.first);
        let request = f.request.clone();
        tokio::spawn(async move {
            let mutation = first.mutate_supervisor_management(&request);
            tokio::pin!(mutation);
            tokio::select! {
                result = &mut mutation => Ok(result),
                _ = begin_timeout_rx => {
                    // Start the real caller timeout only after the std job is
                    // parked, so slow stage preparation cannot bypass the test.
                    tokio::time::timeout(Duration::from_millis(20), &mut mutation).await
                }
            }
        })
    };
    hook.wait_entered();
    begin_timeout_tx.send(()).unwrap();
    assert!(
        writer.await?.is_err(),
        "caller timeout must drop the awaited mutation"
    );
    held(&f);
    let parked = files(f.home.path());
    assert_eq!(value(&parked.proof)["state"], "prepared");
    assert_eq!(
        value(&parked.runtime)["supervisor_management"]["revision"],
        2
    );
    hook.release();
    wait_released(&f);
    outcome(&f, Stage::Runtime, false).await?;
    Ok(())
}

#[test]
fn whole_runtime_shutdown_retains_each_started_management_job() -> TestResult {
    for stage in STAGES {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let f = runtime.block_on(fixture(false))?;
        let hook =
            ManagementWriteHook::install(&f.first, stage, DurableWritePhase::BeforeReplace, false);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let (cancelled_tx, cancelled_rx) = std::sync::mpsc::channel();
        let writer = {
            let first = Arc::clone(&f.first);
            let request = f.request.clone();
            runtime.spawn(async move {
                let _cancelled = Cancelled(cancelled_tx);
                first.mutate_supervisor_management(&request).await
            })
        };
        hook.wait_entered();
        runtime.shutdown_timeout(Duration::from_millis(20));
        cancelled_rx.recv_timeout(DEADLINE)?;
        held(&f);
        assert!(
            exclusive_available(&f.first.session_write_lock_path(TARGET)),
            "Configure must not lock target"
        );
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let second = Arc::clone(&f.second);
        let reference = f.reference.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                assert!(writer.await.unwrap_err().is_cancelled());
                let _ = started_tx.send(());
                let _ = done_tx.send(second.inspect_supervisor_scope(&reference).await);
            });
        });
        started_rx.recv_timeout(DEADLINE)?;
        assert!(matches!(
            done_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        held(&f);
        hook.release();
        let observed = done_rx.recv_timeout(DEADLINE)?;
        thread.join().unwrap();
        assert_eq!(observed.is_ok(), stage == Stage::Committed);
        wait_released(&f);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(outcome(&f, stage, stage == Stage::Committed))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn management_job_errors_preserve_stage_boundary_and_finish_temp_cleanup() -> TestResult {
    for stage in STAGES {
        for phase in [
            DurableWritePhase::BeforeReplace,
            DurableWritePhase::AfterReplace,
        ] {
            let f = fixture(true).await?;
            let hook = ManagementWriteHook::install(&f.first, stage, phase, true);
            let _release = ReleaseOnDrop(Arc::clone(&hook));
            let writer = {
                let first = Arc::clone(&f.first);
                let request = f.request.clone();
                tokio::spawn(async move { first.mutate_supervisor_management(&request).await })
            };
            hook.wait_entered();
            held(&f);
            hook.release();
            let error = tokio::time::timeout(DEADLINE, writer).await??.unwrap_err();
            assert!(error
                .to_string()
                .contains("injected management publication failure"));
            wait_released(&f);
            if stage == Stage::Prepared && phase == DurableWritePhase::BeforeReplace {
                assert_eq!(files(f.home.path()), f.before);
                assert_eq!(
                    f.second
                        .inspect_supervisor_scope(&f.reference)
                        .await?
                        .state_revision,
                    1
                );
            } else {
                // Runtime before-replace still has the old sidecar, but Prepared proof.
                let published_stage =
                    if stage == Stage::Runtime && phase == DurableWritePhase::BeforeReplace {
                        Stage::Prepared
                    } else {
                        stage
                    };
                outcome(
                    &f,
                    published_stage,
                    stage == Stage::Committed && phase == DurableWritePhase::AfterReplace,
                )
                .await?;
            }
            preserved(&f);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detach_publication_never_needs_deleted_target_or_its_held_session_guard() -> TestResult {
    let mut f = fixture(true).await?;
    f.first.mutate_supervisor_management(&f.request).await?;
    f.first.delete_session(TARGET).await?;
    f.request.expected_state_revision = 2;
    f.request.mutation = Mutation::Detach {
        target_session_id: TARGET.into(),
    };
    f.locks_target = false;
    f.before = files(f.home.path());
    let _target_guard = f.second.acquire_session_maintenance_lock(TARGET).await?;
    let hook = ManagementWriteHook::install(
        &f.first,
        Stage::Committed,
        DurableWritePhase::AfterReplace,
        false,
    );
    let _release = ReleaseOnDrop(Arc::clone(&hook));
    let writer = {
        let first = Arc::clone(&f.first);
        let request = f.request.clone();
        tokio::spawn(async move { first.mutate_supervisor_management(&request).await })
    };
    hook.wait_entered();
    held(&f);
    assert!(!exclusive_available(
        &f.first.session_write_lock_path(TARGET)
    ));
    hook.release();
    let receipt = tokio::time::timeout(DEADLINE, writer).await???;
    assert!(receipt.changed);
    assert_eq!(
        (receipt.state_revision, receipt.link_revision),
        (3, Some(2))
    );
    preserved(&f);
    let loaded = f
        .second
        .load_root_authority(&f.reference.session_id)
        .await?
        .unwrap();
    let link = &loaded.supervisor_management.unwrap().links[TARGET];
    assert!(!link.enabled);
    assert_eq!(link.revision, 2);
    Ok(())
}
