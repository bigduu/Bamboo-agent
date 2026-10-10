//! Barriers inside actual complete std replacements, with independent FileExt probes.
use std::sync::{Condvar, Mutex as StdMutex, OnceLock};

use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryPort, CompressionEvent, CompressionTriggerType,
    ConversationSummary,
};
use chrono::Duration as ChronoDuration;

const CHILD: &str = "tx-child";
const ROOT: &str = "tx-root";
const DEADLINE: Duration = Duration::from_secs(10);
type Pair = (Session, Session, Session, Session);
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Default)]
struct Latch {
    visits: usize,
    entered: bool,
    released: bool,
}

#[derive(Debug)]
pub(super) struct TaskWriteHook {
    target: &'static str,
    phase: DurableWritePhase,
    skip: usize,
    fail: bool,
    error_at: Option<DurableWritePhase>,
    state: StdMutex<Latch>,
    wake: Condvar,
}

impl TaskWriteHook {
    fn new(target: &'static str, phase: DurableWritePhase, skip: usize, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            target,
            phase,
            skip,
            fail,
            error_at: None,
            state: StdMutex::new(Latch::default()),
            wake: Condvar::new(),
        })
    }

    fn install(store: &SessionStoreV2, hook: &Arc<Self>) {
        *store.task_write_hook.lock().unwrap() = Some(Arc::clone(hook));
    }

    pub(super) fn visit(&self, path: &Path, phase: DurableWritePhase) -> io::Result<()> {
        if path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            != Some(self.target)
        {
            return Ok(());
        }
        if self.error_at == Some(phase) {
            return Err(io::Error::other(
                "injected Task runtime publication failure",
            ));
        }
        if phase != self.phase {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        let visit = state.visits;
        state.visits += 1;
        if visit != self.skip {
            return Ok(());
        }
        state.entered = true;
        self.wake.notify_all();
        while !state.released {
            state = self.wake.wait(state).unwrap();
        }
        if self.fail {
            Err(io::Error::other(
                "injected Task runtime publication failure",
            ))
        } else {
            Ok(())
        }
    }

    fn wait_entered(&self) {
        let (state, timeout) = self
            .wake
            .wait_timeout_while(self.state.lock().unwrap(), DEADLINE, |state| !state.entered)
            .unwrap();
        let entered = state.entered && !timeout.timed_out();
        drop(state);
        assert!(entered, "actual std replacement not reached");
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.wake.notify_all();
    }
}

struct ReleaseOnDrop(Arc<TaskWriteHook>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

// Only the selected unique test home receives a hook during constructor recovery.
// There is no production hook registry or public constructor option.
fn constructor_hooks() -> &'static StdMutex<HashMap<PathBuf, Arc<TaskWriteHook>>> {
    static HOOKS: OnceLock<StdMutex<HashMap<PathBuf, Arc<TaskWriteHook>>>> = OnceLock::new();
    HOOKS.get_or_init(|| StdMutex::new(HashMap::new()))
}

pub(super) fn constructor_hook(home: &Path) -> Option<Arc<TaskWriteHook>> {
    constructor_hooks().lock().unwrap().get(home).cloned()
}

struct ConstructorHook(PathBuf);
impl ConstructorHook {
    fn install(home: &Path, hook: &Arc<TaskWriteHook>) -> Self {
        assert!(constructor_hooks()
            .lock()
            .unwrap()
            .insert(home.to_path_buf(), Arc::clone(hook))
            .is_none());
        Self(home.to_path_buf())
    }
}
impl Drop for ConstructorHook {
    fn drop(&mut self) {
        constructor_hooks().lock().unwrap().remove(&self.0);
    }
}

fn set_context(session: &mut Session, label: &str, revision: u64) {
    session.conversation_summary = Some(ConversationSummary::new(label, 2, 10));
    session.compression_events = vec![CompressionEvent::new(
        revision as usize,
        1,
        80.0,
        40.0,
        10,
        CompressionTriggerType::Auto,
        0.5,
        None,
        0,
    )];
    let state = session
        .model_context_state
        .get_or_insert_with(Default::default);
    state.state_revision = revision;
}

fn published_context(session: &Session) -> serde_json::Value {
    serde_json::json!({
        "summary": session.conversation_summary,
        "compression_events": session.compression_events,
        "model_context_state": session.model_context_state,
    })
}

async fn stores(home: &Path) -> io::Result<(Arc<SessionStoreV2>, Arc<SessionStoreV2>, Pair)> {
    let first = Arc::new(SessionStoreV2::new(home.to_path_buf()).await?);
    tests::seed_runtime_task_transaction_pair(&first).await?;
    for id in [ROOT, CHILD] {
        let mut session = first.load_session(id).await?.unwrap();
        set_context(&mut session, "context-A", 11);
        first.save_session(&session).await?;
    }
    let child = first.load_runtime_control_plane(CHILD).await?.unwrap();
    let root = first.load_runtime_control_plane(ROOT).await?.unwrap();
    let mut updated_child = child.clone();
    let mut updated_root = root.clone();
    let evaluated = tests::transaction_task_list(ROOT, "evaluated");
    updated_child.task_list = Some(evaluated.clone());
    updated_root.task_list = Some(evaluated);
    updated_child.set_task_list_version_meta("2");
    updated_root.set_task_list_version_meta("2");
    // Constructor recovery also takes Task exclusive; precreate the contender.
    let second = Arc::new(SessionStoreV2::new(home.to_path_buf()).await?);
    Ok((first, second, (child, updated_child, root, updated_root)))
}

fn probe(store: &SessionStoreV2) -> bool {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(
            store
                .bamboo_home_dir
                .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        )
        .unwrap();
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            FileExt::unlock(&file).unwrap();
            true
        }
        Err(error) => {
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            false
        }
    }
}

fn wait_released(store: &SessionStoreV2) {
    let deadline = Instant::now() + DEADLINE;
    while !probe(store) {
        assert!(
            Instant::now() < deadline,
            "started job retained Task lock forever"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn assert_no_temps(home: &Path) {
    for directory in [
        home.join("sessions").join(ROOT),
        home.join("sessions")
            .join(ROOT)
            .join("children")
            .join(CHILD),
    ] {
        for entry in std::fs::read_dir(directory).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().contains("durable.tmp"),
                "error cleanup left temp file"
            );
        }
    }
}

async fn publish_successor_and_claim(store: &SessionStoreV2) -> Result {
    // Adopt the now-current generation. Never publish a stale v1 snapshot after
    // a single CAS, or pass ordinary access through a pending pair journal.
    for id in [ROOT, CHILD] {
        let mut current = store.load_session(id).await?.unwrap();
        let history = serde_json::to_value(&current.messages)?;
        assert_eq!(
            current.messages[0].content,
            if id == ROOT {
                "root transcript secret"
            } else {
                "child transcript secret"
            }
        );
        set_context(&mut current, "context-B", 22);
        let expected = published_context(&current);
        let generation = current.task_list_version_meta();
        store.save_session(&current).await?;
        let now = Utc::now();
        let activation = store
            .claim_activation(&ActorActivationClaim {
                actor_id: id.into(),
                run_id: format!("successor-{id}"),
                lease_owner: "successor".into(),
                lease_expires_at: now + ChronoDuration::minutes(5),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await?;
        assert_eq!(activation.attempt, 1);
        let loaded = store.load_session(id).await?.unwrap();
        assert_eq!(published_context(&loaded), expected);
        assert_eq!(serde_json::to_value(&loaded.messages)?, history);
        assert_eq!(loaded.task_list_version_meta(), generation);
        let inspected = store.inspect_actor(id).await?;
        assert_eq!(inspected.activation.as_ref(), Some(&activation));
        assert_eq!(inspected.actor.session_created_at, current.created_at);
    }
    let reopened = SessionStoreV2::new(store.bamboo_home_dir.clone()).await?;
    for id in [ROOT, CHILD] {
        let loaded = reopened.load_session(id).await?.unwrap();
        assert_eq!(loaded.conversation_summary.unwrap().content, "context-B");
        assert_eq!(loaded.model_context_state.unwrap().state_revision, 22);
        assert_eq!(loaded.compression_events[0].messages_compressed, 22);
        let directory = store
            .bamboo_home_dir
            .join("sessions")
            .join(ROOT)
            .join(if id == CHILD { "children/tx-child" } else { "" });
        let main: Session =
            serde_json::from_slice(&std::fs::read(directory.join("session.json"))?)?;
        assert_eq!(main.conversation_summary.unwrap().content, "context-B");
        assert_eq!(main.compression_events[0].messages_compressed, 22);
        assert_eq!(main.model_context_state.unwrap().state_revision, 22);
        assert_eq!(
            main.messages[0].content,
            if id == ROOT {
                "root transcript secret"
            } else {
                "child transcript secret"
            }
        );
        assert_eq!(reopened.inspect_actor(id).await?.actor.current_attempt, 1);
        let runtime = std::fs::read(store.bamboo_home_dir.join("sessions").join(ROOT).join(
            if id == CHILD {
                "children/tx-child/runtime.json"
            } else {
                "runtime.json"
            },
        ))?;
        assert_eq!(
            serde_json::from_slice::<Session>(&runtime)?
                .model_context_state
                .unwrap()
                .state_revision,
            22
        );
    }
    assert_no_temps(&store.bamboo_home_dir);
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum WriteCase {
    Single,
    First,
    Second,
    FirstUndo,
    SecondUndo,
    ExplicitRecovery,
    ConstructorRecovery,
}
impl WriteCase {
    fn target(self) -> &'static str {
        match self {
            Self::Second | Self::SecondUndo => ROOT,
            _ => CHILD,
        }
    }
    fn skip(self) -> usize {
        usize::from(matches!(self, Self::FirstUndo))
    }
    fn pending(self) -> bool {
        !matches!(self, Self::Single)
    }
}

async fn seed_pending(store: &SessionStoreV2, pair: &Pair) -> io::Result<()> {
    let guard = store.lock_runtime_task_transaction_exclusive().await?;
    let journal = RuntimeTaskTransactionJournal {
        version: RUNTIME_TASK_TRANSACTION_VERSION,
        transaction_id: Uuid::new_v4().to_string(),
        first: TaskControlPlaneUndo {
            session_id: CHILD.into(),
            task_list: pair.0.task_list.clone(),
            task_list_version: "1".into(),
        },
        second: TaskControlPlaneUndo {
            session_id: ROOT.into(),
            task_list: pair.2.task_list.clone(),
            task_list_version: "1".into(),
        },
    };
    store.write_runtime_task_journal(&journal).await?;
    store
        .write_existing_runtime_sidecar_durable_unchecked(
            &pair.1,
            RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            &guard,
        )
        .await?;
    store
        .write_existing_runtime_sidecar_durable_unchecked(
            &pair.3,
            RuntimeTaskDurabilityEvent::SecondUpdatedSidecarPublished,
            &guard,
        )
        .await?;
    store
        .runtime_task_recovery_required
        .store(true, Ordering::Release);
    Ok(())
}

async fn prepare(case: WriteCase, first: &SessionStoreV2, pair: &Pair) -> io::Result<()> {
    if matches!(
        case,
        WriteCase::ExplicitRecovery | WriteCase::ConstructorRecovery
    ) {
        seed_pending(first, pair).await?;
    }
    if matches!(case, WriteCase::FirstUndo | WriteCase::SecondUndo) {
        first
            .runtime_task_faults
            .lock()
            .unwrap()
            .push(RuntimeTaskTransactionFault::SecondUpdatedWrite);
    }
    Ok(())
}

async fn write(case: WriteCase, first: &SessionStoreV2, pair: &Pair) -> io::Result<()> {
    match case {
        WriteCase::Single => assert!(
            first
                .save_task_control_plane_if_matches(&pair.0, &pair.1)
                .await?
        ),
        WriteCase::ExplicitRecovery => {
            first
                .recover_task_control_plane_transaction(CHILD, ROOT)
                .await?
        }
        WriteCase::ConstructorRecovery => {
            SessionStoreV2::new(first.bamboo_home_dir.clone()).await?;
        }
        _ => {
            first
                .save_task_control_planes_atomically(&pair.0, &pair.1, &pair.2, &pair.3)
                .await?;
        }
    }
    Ok(())
}

const CASES: [WriteCase; 7] = [
    WriteCase::Single,
    WriteCase::First,
    WriteCase::Second,
    WriteCase::FirstUndo,
    WriteCase::SecondUndo,
    WriteCase::ExplicitRecovery,
    WriteCase::ConstructorRecovery,
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_matrix_retains_actual_task_lock_and_cannot_publish_after_successor_claim() -> Result
{
    for case in CASES {
        let home = tempfile::tempdir()?;
        let (first, second, pair) = stores(home.path()).await?;
        prepare(case, &first, &pair).await?;
        let hook = TaskWriteHook::new(
            case.target(),
            DurableWritePhase::BeforeReplace,
            case.skip(),
            false,
        );
        TaskWriteHook::install(&first, &hook);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let _constructor = if matches!(case, WriteCase::ConstructorRecovery) {
            Some(ConstructorHook::install(home.path(), &hook))
        } else {
            None
        };
        let writer = {
            let first = Arc::clone(&first);
            tokio::spawn(async move { write(case, &first, &pair).await })
        };
        hook.wait_entered();
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled(), "{case:?}");
        assert!(
            !probe(&first),
            "abort released physical Task ownership: {case:?}"
        );
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let contender = {
            let second = Arc::clone(&second);
            tokio::spawn(async move {
                let _ = started_tx.send(());
                second.load_runtime_control_plane(CHILD).await
            })
        };
        started_rx.await?;
        assert!(!probe(&first));
        assert!(!contender.is_finished());
        hook.release();
        let result = tokio::time::timeout(DEADLINE, contender).await??;
        if case.pending() {
            assert!(
                result.unwrap_err().to_string().contains("recovery"),
                "{case:?}"
            );
            second
                .recover_task_control_plane_transaction(CHILD, ROOT)
                .await?;
        } else {
            assert_eq!(
                result?.unwrap().task_list_version_meta().as_deref(),
                Some("2")
            );
        }
        assert!(second.runtime_task_journal_paths().await?.is_empty());
        publish_successor_and_claim(&second).await?;
    }
    Ok(())
}

struct Cancelled(std::sync::mpsc::Sender<()>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn runtime_shutdown_matrix_retains_started_task_jobs() -> Result {
    for (index, case) in CASES.into_iter().enumerate() {
        let home = tempfile::tempdir()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let (first, second, pair) = runtime.block_on(stores(home.path()))?;
        runtime.block_on(prepare(case, &first, &pair))?;
        let hook = TaskWriteHook::new(
            case.target(),
            DurableWritePhase::AfterReplace,
            case.skip(),
            false,
        );
        TaskWriteHook::install(&first, &hook);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let _constructor = if matches!(case, WriteCase::ConstructorRecovery) {
            Some(ConstructorHook::install(home.path(), &hook))
        } else {
            None
        };
        let (cancelled_tx, cancelled_rx) = std::sync::mpsc::channel();
        let writer = {
            let first = Arc::clone(&first);
            runtime.spawn(async move {
                let _cancelled = Cancelled(cancelled_tx);
                write(case, &first, &pair).await
            })
        };
        hook.wait_entered();
        if index % 2 == 0 {
            runtime.shutdown_background();
        } else {
            runtime.shutdown_timeout(Duration::from_millis(20));
        }
        cancelled_rx.recv_timeout(DEADLINE)?;
        assert!(
            !probe(&first),
            "runtime loss released Task ownership: {case:?}"
        );
        hook.release();
        wait_released(&first);
        drop(writer);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            if case.pending() {
                assert!(second.load_runtime_control_plane(CHILD).await.is_err());
                second
                    .recover_task_control_plane_transaction(CHILD, ROOT)
                    .await?;
            }
            // Recreate the search worker on this runtime after using the
            // precreated independent Store for the physical-lock/recovery proof.
            let fresh = SessionStoreV2::new(home.path().to_path_buf()).await?;
            publish_successor_and_claim(&fresh).await
        })?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn before_and_after_replace_errors_preserve_distinct_publication_and_cleanup() -> Result {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir()?;
        let (first, second, pair) = stores(home.path()).await?;
        let hook = TaskWriteHook::new(CHILD, phase, 0, true);
        TaskWriteHook::install(&first, &hook);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let old_context = published_context(&pair.0);
        let writer = {
            let first = Arc::clone(&first);
            tokio::spawn(async move {
                first
                    .save_task_control_plane_if_matches(&pair.0, &pair.1)
                    .await
            })
        };
        hook.wait_entered();
        assert!(!probe(&first));
        hook.release();
        let error = tokio::time::timeout(DEADLINE, writer).await??.unwrap_err();
        assert!(error
            .to_string()
            .contains("injected Task runtime publication failure"));
        assert!(probe(&first));
        assert_no_temps(home.path());
        let current = second.load_runtime_control_plane(CHILD).await?.unwrap();
        assert_eq!(published_context(&current), old_context);
        let expected = if phase == DurableWritePhase::BeforeReplace {
            "1"
        } else {
            "2"
        };
        assert_eq!(current.task_list_version_meta().as_deref(), Some(expected));
        assert!(first
            .runtime_task_durability_events
            .lock()
            .unwrap()
            .is_empty());
        publish_successor_and_claim(&second).await?;
    }
    Ok(())
}

// Inject the original write failure before entering a second native barrier in
// its cleanup path. Caller/runtime loss at this point must retain the lock too.
fn cleanup_hook(error_at: DurableWritePhase) -> Arc<TaskWriteHook> {
    Arc::new(TaskWriteHook {
        target: CHILD,
        phase: DurableWritePhase::BeforeErrorCleanup,
        skip: 0,
        fail: false,
        error_at: Some(error_at),
        state: StdMutex::new(Latch::default()),
        wake: Condvar::new(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_at_error_cleanup_keeps_task_lock_until_temp_cleanup_terminates() -> Result {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir()?;
        let (first, second, pair) = stores(home.path()).await?;
        let hook = cleanup_hook(phase);
        TaskWriteHook::install(&first, &hook);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let writer = {
            let first = Arc::clone(&first);
            tokio::spawn(async move {
                first
                    .save_task_control_plane_if_matches(&pair.0, &pair.1)
                    .await
            })
        };
        hook.wait_entered();
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert!(!probe(&first));
        hook.release();
        wait_released(&first);
        assert_no_temps(home.path());
        let current = second.load_runtime_control_plane(CHILD).await?.unwrap();
        let expected = if phase == DurableWritePhase::BeforeReplace {
            "1"
        } else {
            "2"
        };
        assert_eq!(current.task_list_version_meta().as_deref(), Some(expected));
        publish_successor_and_claim(&second).await?;
    }
    Ok(())
}

#[test]
fn runtime_shutdown_at_error_cleanup_retains_physical_task_ownership() -> Result {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let (first, second, pair) = runtime.block_on(stores(home.path()))?;
        let hook = cleanup_hook(phase);
        TaskWriteHook::install(&first, &hook);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let (cancelled_tx, cancelled_rx) = std::sync::mpsc::channel();
        let writer = {
            let first = Arc::clone(&first);
            runtime.spawn(async move {
                let _cancelled = Cancelled(cancelled_tx);
                first
                    .save_task_control_plane_if_matches(&pair.0, &pair.1)
                    .await
            })
        };
        hook.wait_entered();
        runtime.shutdown_timeout(Duration::from_millis(20));
        cancelled_rx.recv_timeout(DEADLINE)?;
        assert!(!probe(&first));
        hook.release();
        wait_released(&first);
        drop(writer);
        assert_no_temps(home.path());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let current = second.load_runtime_control_plane(CHILD).await?.unwrap();
            let expected = if phase == DurableWritePhase::BeforeReplace {
                "1"
            } else {
                "2"
            };
            assert_eq!(current.task_list_version_meta().as_deref(), Some(expected));
            let fresh = SessionStoreV2::new(home.path().to_path_buf()).await?;
            publish_successor_and_claim(&fresh).await
        })?;
    }
    Ok(())
}
