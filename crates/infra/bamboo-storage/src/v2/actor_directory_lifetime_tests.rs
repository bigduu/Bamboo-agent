//! Native filesystem barriers, not a pause around an awaited filesystem call.

use std::sync::{Condvar, Mutex as StdMutex};

use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryEntry, ActorDirectoryError, ActorDirectoryPort,
    ActorLogicalState, Session, Storage,
};
use chrono::{Duration as ChronoDuration, Utc};

use super::*;

const ACTOR: &str = "filesystem-lifetime";
const ENTRY: &str = "actor-authority.json";
const MARKER: &str = "actor-authority.initialized.json";
const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct LatchState {
    entered: bool,
    released: bool,
}

#[derive(Debug)]
pub(super) struct ActorWriteHook {
    file: &'static str,
    phase: DurableWritePhase,
    fail: bool,
    state: StdMutex<LatchState>,
    wake: Condvar,
}

impl ActorWriteHook {
    fn install(
        store: &SessionStoreV2,
        file: &'static str,
        phase: DurableWritePhase,
        fail: bool,
    ) -> Arc<Self> {
        let hook = Arc::new(Self {
            file,
            phase,
            fail,
            state: StdMutex::new(LatchState::default()),
            wake: Condvar::new(),
        });
        *store.actor_write_hook.lock().unwrap() = Some(Arc::clone(&hook));
        hook
    }

    pub(super) fn visit(&self, path: &Path, phase: DurableWritePhase) -> io::Result<()> {
        if path.file_name().and_then(|name| name.to_str()) != Some(self.file) || phase != self.phase
        {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        if state.entered {
            return Ok(());
        }
        state.entered = true;
        self.wake.notify_all();
        while !state.released {
            state = self.wake.wait(state).unwrap();
        }
        if self.fail {
            Err(io::Error::other("injected actor publication failure"))
        } else {
            Ok(())
        }
    }

    fn wait_entered(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .wake
            .wait_timeout_while(state, DEADLINE, |state| !state.entered)
            .unwrap();
        assert!(
            state.entered && !timeout.timed_out(),
            "blocking job never reached actual publication barrier"
        );
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.wake.notify_all();
    }
}

// Panic cleanup must never leave a running filesystem job parked forever.
struct ReleaseOnDrop(Arc<ActorWriteHook>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn claim(run: &str, now: chrono::DateTime<Utc>) -> ActorActivationClaim {
    ActorActivationClaim {
        actor_id: ACTOR.into(),
        run_id: run.into(),
        lease_owner: run.into(),
        lease_expires_at: now + ChronoDuration::minutes(5),
        inbox_generation: 0,
        placement_ref: None,
        now,
    }
}

async fn stores(home: &Path) -> io::Result<(Arc<SessionStoreV2>, Arc<SessionStoreV2>)> {
    let first = Arc::new(SessionStoreV2::new(home.to_path_buf()).await?);
    first.save_session(&Session::new(ACTOR, "model")).await?;
    // Constructor recovery itself takes exclusive gates, so construct the
    // contender before the filesystem barrier to exercise its real actor CAS.
    let second = Arc::new(SessionStoreV2::new(home.to_path_buf()).await?);
    Ok((first, second))
}

fn entry(home: &Path) -> ActorDirectoryEntry {
    serde_json::from_slice(&std::fs::read(home.join("sessions").join(ACTOR).join(ENTRY)).unwrap())
        .unwrap()
}

fn marker(home: &Path) -> PathBuf {
    home.join("sessions").join(ACTOR).join(MARKER)
}

fn lock_paths(store: &SessionStoreV2) -> [PathBuf; 3] {
    [
        store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        store.session_write_lock_path(ACTOR),
    ]
}

fn assert_physical_locks_held(store: &SessionStoreV2) {
    for path in lock_paths(store) {
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
            "guard released while filesystem job is running: {}",
            path.display()
        );
    }
}

fn wait_physical_locks_released(store: &SessionStoreV2) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let unlocked = lock_paths(store).iter().all(|path| {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            if FileExt::try_lock_exclusive(&file).is_ok() {
                FileExt::unlock(&file).unwrap();
                true
            } else {
                false
            }
        });
        if unlocked {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "filesystem job did not release its guards"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_abort_cannot_release_started_cas_job_or_roll_back_successor(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let (first, second) = stores(home.path()).await?;
    let now = Utc::now();
    let old = first.claim_activation(&claim("old", now)).await?;
    let previous = entry(home.path());
    let hook = ActorWriteHook::install(&first, ENTRY, DurableWritePhase::BeforeReplace, false);
    let _release = ReleaseOnDrop(Arc::clone(&hook));
    let writer = {
        let first = Arc::clone(&first);
        let fence = old.fence();
        tokio::spawn(async move {
            first
                .start_activation(&fence, now + ChronoDuration::seconds(1))
                .await
        })
    };
    hook.wait_entered();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    assert_physical_locks_held(&first);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let successor_at = old.lease_expires_at + ChronoDuration::seconds(1);
    let successor = {
        let second = Arc::clone(&second);
        tokio::spawn(async move {
            let _ = started_tx.send(());
            second
                .claim_activation(&claim("successor", successor_at))
                .await
        })
    };
    started_rx.await?;
    assert_physical_locks_held(&first);
    assert!(!successor.is_finished());
    assert_eq!(entry(home.path()), previous);
    hook.release();
    let next = tokio::time::timeout(DEADLINE, successor).await???;
    assert_eq!((next.attempt, next.lease_epoch), (2, 2));
    let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
    let durable = reopened.inspect_actor(ACTOR).await?;
    assert_eq!(durable.activation.as_ref(), Some(&next));
    assert_eq!(durable.revision, previous.revision + 2);
    assert_eq!(
        durable.actor.session_created_at,
        previous.actor.session_created_at
    );
    assert!(matches!(
        reopened.validate_fence(&old.fence(), now).await,
        Err(ActorDirectoryError::StaleFence)
    ));
    Ok(())
}

struct Cancelled(std::sync::mpsc::Sender<()>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

fn shutdown_initialization(
    file: &'static str,
    background: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let (first, second) = runtime.block_on(stores(home.path()))?;
    let hook = ActorWriteHook::install(&first, file, DurableWritePhase::BeforeReplace, false);
    let _release = ReleaseOnDrop(Arc::clone(&hook));
    let (cancelled_tx, cancelled_rx) = std::sync::mpsc::channel();
    let writer = {
        let first = Arc::clone(&first);
        runtime.spawn(async move {
            let _cancelled = Cancelled(cancelled_tx);
            first
                .claim_activation(&claim("cancelled", Utc::now()))
                .await
        })
    };
    hook.wait_entered();
    if background {
        runtime.shutdown_background();
    } else {
        runtime.shutdown_timeout(Duration::from_millis(20));
    }
    cancelled_rx.recv_timeout(DEADLINE)?;
    assert_physical_locks_held(&first);
    assert!(!marker(home.path()).exists());
    hook.release();
    wait_physical_locks_released(&first);
    let cold = entry(home.path());
    assert_eq!(cold.actor.state, ActorLogicalState::Cold);
    assert_eq!(cold.actor.current_attempt, 0);
    assert!(cold.activation.is_none());
    assert_eq!(marker(home.path()).exists(), file == MARKER);
    let independent_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    independent_runtime.block_on(async {
        assert!(writer.await.unwrap_err().is_cancelled());
        let repaired = second.ensure_actor(ACTOR).await?;
        assert_eq!(repaired, cold);
        assert!(marker(home.path()).exists());
        let next = second.claim_activation(&claim("next", Utc::now())).await?;
        assert_eq!((next.attempt, next.lease_epoch), (1, 1));
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let durable = reopened.inspect_actor(ACTOR).await?;
        assert_eq!(durable.activation.as_ref(), Some(&next));
        assert_eq!(durable.revision, cold.revision + 1);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn runtime_shutdown_keeps_cold_job_locked_and_repairs_marker_that_never_started(
) -> Result<(), Box<dyn std::error::Error>> {
    shutdown_initialization(ENTRY, false)
}

#[test]
fn runtime_shutdown_background_keeps_separate_marker_job_locked(
) -> Result<(), Box<dyn std::error::Error>> {
    shutdown_initialization(MARKER, true)
}

#[test]
fn shutdown_started_cas_blocks_independent_runtime_successor_until_publication(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let (first, second) = runtime.block_on(stores(home.path()))?;
    let now = Utc::now();
    let old = runtime.block_on(first.claim_activation(&claim("old", now)))?;
    let previous = entry(home.path());
    let hook = ActorWriteHook::install(&first, ENTRY, DurableWritePhase::BeforeReplace, false);
    let _release = ReleaseOnDrop(Arc::clone(&hook));
    let (cancelled_tx, cancelled_rx) = std::sync::mpsc::channel();
    let writer = {
        let first = Arc::clone(&first);
        let fence = old.fence();
        runtime.spawn(async move {
            let _cancelled = Cancelled(cancelled_tx);
            first
                .start_activation(&fence, now + ChronoDuration::seconds(1))
                .await
        })
    };
    hook.wait_entered();
    runtime.shutdown_timeout(Duration::from_millis(20));
    cancelled_rx.recv_timeout(DEADLINE)?;
    assert_physical_locks_held(&first);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let successor_at = old.lease_expires_at + ChronoDuration::seconds(1);
    let successor_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            assert!(writer.await.unwrap_err().is_cancelled());
            started_tx.send(()).unwrap();
            let result = second
                .claim_activation(&claim("successor", successor_at))
                .await;
            completed_tx.send(result).unwrap();
        });
    });
    started_rx.recv_timeout(DEADLINE)?;
    assert_physical_locks_held(&first);
    assert!(matches!(
        completed_rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_eq!(entry(home.path()), previous);
    hook.release();
    let next = completed_rx.recv_timeout(DEADLINE)??;
    successor_thread.join().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let durable = reopened.inspect_actor(ACTOR).await?;
        assert_eq!(durable.activation.as_ref(), Some(&next));
        assert_eq!((next.attempt, next.lease_epoch), (2, 2));
        assert_eq!(durable.revision, previous.revision + 2);
        assert_eq!(
            durable.actor.session_created_at,
            previous.actor.session_created_at
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn marker_repair_after_abort_retains_physical_guards(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let (first, second) = stores(home.path()).await?;
    let cold = first.ensure_actor(ACTOR).await?;
    std::fs::remove_file(marker(home.path()))?;
    let hook = ActorWriteHook::install(&first, MARKER, DurableWritePhase::BeforeReplace, false);
    let _release = ReleaseOnDrop(Arc::clone(&hook));
    let writer = {
        let first = Arc::clone(&first);
        tokio::spawn(async move { first.ensure_actor(ACTOR).await })
    };
    hook.wait_entered();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    assert_physical_locks_held(&first);
    hook.release();
    let actual = tokio::time::timeout(DEADLINE, second.inspect_actor(ACTOR)).await??;
    assert_eq!(actual, cold);
    assert!(marker(home.path()).exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observation_refresh_can_commit_without_cancelled_following_cas(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let (first, second) = stores(home.path()).await?;
    let cold = first.ensure_actor(ACTOR).await?;
    let mut session = first.load_session(ACTOR).await?.unwrap();
    session.title = "updated observation".into();
    session.metadata_version += 1;
    first.save_session(&session).await?;
    let hook = ActorWriteHook::install(&first, ENTRY, DurableWritePhase::BeforeReplace, false);
    let _release = ReleaseOnDrop(Arc::clone(&hook));
    let writer = {
        let first = Arc::clone(&first);
        tokio::spawn(async move {
            first
                .claim_activation(&claim("cancelled", Utc::now()))
                .await
        })
    };
    hook.wait_entered();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    assert_physical_locks_held(&first);
    hook.release();
    let actual = tokio::time::timeout(DEADLINE, second.inspect_actor(ACTOR)).await??;
    assert_eq!(actual.revision, cold.revision + 1);
    assert_eq!(actual.actor.current_attempt, 0);
    assert!(actual.activation.is_none());
    assert!(actual.actor.observed_metadata_version > cold.actor.observed_metadata_version);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_errors_release_guards_and_reopen_actual_state(
) -> Result<(), Box<dyn std::error::Error>> {
    for phase in [
        DurableWritePhase::BeforeReplace,
        DurableWritePhase::AfterReplace,
    ] {
        let home = tempfile::tempdir()?;
        let (first, second) = stores(home.path()).await?;
        let cold = first.ensure_actor(ACTOR).await?;
        let now = Utc::now();
        let hook = ActorWriteHook::install(&first, ENTRY, phase, true);
        let _release = ReleaseOnDrop(Arc::clone(&hook));
        let writer = {
            let first = Arc::clone(&first);
            tokio::spawn(async move { first.claim_activation(&claim("unconfirmed", now)).await })
        };
        hook.wait_entered();
        assert_physical_locks_held(&first);
        hook.release();
        assert!(matches!(
            tokio::time::timeout(DEADLINE, writer).await??,
            Err(ActorDirectoryError::Storage(_))
        ));
        let reopened = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let actual = reopened.inspect_actor(ACTOR).await?;
        if phase == DurableWritePhase::BeforeReplace {
            assert_eq!(actual, cold);
        } else {
            assert_eq!(actual.actor.current_attempt, 1);
            assert_eq!(actual.revision, cold.revision + 1);
            assert_eq!(actual.activation.as_ref().unwrap().run_id, "unconfirmed");
            assert!(matches!(
                second.claim_activation(&claim("too-soon", now)).await,
                Err(ActorDirectoryError::Busy)
            ));
        }
        let next = second
            .claim_activation(&claim("next", now + ChronoDuration::minutes(6)))
            .await?;
        assert_eq!(
            next.attempt,
            if phase == DurableWritePhase::BeforeReplace {
                1
            } else {
                2
            }
        );
        assert_eq!(
            reopened.inspect_actor(ACTOR).await?.activation.as_ref(),
            Some(&next)
        );
        for file in std::fs::read_dir(home.path().join("sessions").join(ACTOR))? {
            assert!(!file?.file_name().to_string_lossy().contains("durable.tmp."));
        }
    }
    Ok(())
}
