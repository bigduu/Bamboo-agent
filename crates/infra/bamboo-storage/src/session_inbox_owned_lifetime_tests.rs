//! Actual started std jobs, independent physical locks and inner async-scope Drop.
use super::*;
use bamboo_domain::{
    Session, SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxOwnedClaim, Storage,
};
use chrono::Duration as LeaseDuration;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Condvar,
};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const DEADLINE: Duration = Duration::from_secs(10);

pub(super) struct Fixture {
    pub(super) _temp: TempDir,
    pub(super) runtime: Runtime,
    pub(super) store: Arc<SessionStoreV2>,
    pub(super) other_store: Arc<SessionStoreV2>,
    pub(super) inbox: FileSessionInbox,
    pub(super) other: FileSessionInbox,
    pub(super) dir: PathBuf,
    pub(super) now: chrono::DateTime<Utc>,
}
impl Fixture {
    pub(super) fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let runtime = Runtime::new().unwrap();
        let (store, other_store, dir) = runtime.block_on(async {
            let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
            store
                .save_session(&Session::new("target", "model"))
                .await
                .unwrap();
            // Constructors take lifecycle exclusive: both exist before parking any job.
            let other = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
            let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
            let dir = inbox.inbox_dir("target").await.unwrap();
            (store, other, dir)
        });
        Self {
            _temp: temp,
            runtime,
            inbox: FileSessionInbox::new(store.clone(), SessionInboxLimits::default()),
            other: FileSessionInbox::new(other_store.clone(), SessionInboxLimits::default()),
            store,
            other_store,
            dir,
            now: Utc::now(),
        }
    }
    pub(super) fn request(&self, seconds: i64) -> SessionInboxLeaseRequest {
        SessionInboxLeaseRequest {
            consumer: SessionInboxConsumerId::new(),
            now: self.now + LeaseDuration::seconds(seconds),
            duration: LeaseDuration::seconds(10),
        }
    }
    pub(super) fn deliver(&self) -> SessionMessageEnvelope {
        let envelope = SessionMessageEnvelope::user_input("target", "exact durable input");
        self.runtime
            .block_on(self.inbox.deliver_with_activation_intent(
                &envelope,
                SessionActivationPolicy::RespectSpecificWait,
                None,
            ))
            .unwrap();
        envelope
    }
    pub(super) fn claim(&self, request: &SessionInboxLeaseRequest) -> SessionInboxOwnedClaim {
        self.runtime
            .block_on(self.inbox.claim_owned("target", 1, None, request))
            .unwrap()
            .remove(0)
    }
    pub(super) fn checkpoint(&self, claim: &SessionInboxOwnedClaim) {
        self.runtime.block_on(async {
            let mut session = self.store.load_session("target").await.unwrap().unwrap();
            session.add_message(claim.claim.envelope.to_provider_message().unwrap());
            session
                .session_inbox_admission_mut()
                .record(claim.claim.envelope.id.clone(), claim.claim.generation);
            self.store.save_session(&session).await.unwrap();
        });
    }
    pub(super) fn assert_physical_locks(&self, inbox: bool) {
        assert!(!probe(
            &self.store.bamboo_home_dir().join(".session-lifecycle.lock")
        ));
        if inbox {
            assert!(!probe(&self.dir.join(OPERATION_LOCK_FILE)));
        }
    }
    pub(super) fn successor(
        &self,
        seconds: i64,
    ) -> tokio::task::JoinHandle<Vec<SessionInboxOwnedClaim>> {
        let other = self.other.clone();
        let request = self.request(seconds);
        self.runtime.spawn(async move {
            other
                .claim_owned("target", 1, None, &request)
                .await
                .unwrap()
        })
    }
    pub(super) fn process_waiter(&self) -> tokio::task::JoinHandle<()> {
        let inbox = self.inbox.clone();
        let dir = self.dir.clone();
        self.runtime.spawn(async move {
            let _guard = inbox.lock_process(&dir).await;
        })
    }
    pub(super) fn assert_waiting<T>(&self, waiter: &tokio::task::JoinHandle<T>) {
        self.runtime
            .block_on(async { tokio::time::sleep(Duration::from_millis(40)).await });
        assert!(
            !waiter.is_finished(),
            "successor passed a still-running physical job"
        );
    }
    pub(super) fn finish<T>(&self, waiter: tokio::task::JoinHandle<T>) -> T {
        self.runtime.block_on(async {
            tokio::time::timeout(DEADLINE, waiter)
                .await
                .unwrap()
                .unwrap()
        })
    }
}

pub(super) fn probe(path: &Path) -> bool {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            FileExt::unlock(&file).unwrap();
            true
        }
        Err(error) => {
            assert_eq!(error.kind(), ErrorKind::WouldBlock);
            false
        }
    }
}

#[derive(Default)]
struct Latch {
    entered: bool,
    released: bool,
}
#[derive(Default)]
pub(super) struct Barrier {
    latch: Mutex<Latch>,
    wake: Condvar,
}
impl Barrier {
    pub(super) fn park(&self) {
        let mut state = self.latch.lock().unwrap();
        if state.entered {
            return;
        }
        state.entered = true;
        self.wake.notify_all();
        while !state.released {
            state = self.wake.wait(state).unwrap();
        }
    }
    pub(super) fn entered(&self) {
        let (state, timeout) = self
            .wake
            .wait_timeout_while(self.latch.lock().unwrap(), DEADLINE, |s| !s.entered)
            .unwrap();
        let entered = state.entered && !timeout.timed_out();
        drop(state);
        assert!(entered, "selected actual std job was not reached");
    }
    pub(super) fn release(&self) {
        self.latch.lock().unwrap().released = true;
        self.wake.notify_all();
    }
}

pub(super) struct Parked {
    pub(super) barrier: Arc<Barrier>,
    pub(super) dropped: Arc<AtomicBool>,
    pub(super) runtime: Option<Runtime>,
    pub(super) caller: tokio::task::JoinHandle<Result<(), SessionInboxError>>,
}
impl Parked {
    pub(super) fn start<F, J>(
        mut inbox: FileSessionInbox,
        event: &'static str,
        path: PathBuf,
        job: J,
    ) -> Self
    where
        F: std::future::Future<Output = Result<(), SessionInboxError>> + Send + 'static,
        J: FnOnce(FileSessionInbox) -> F,
    {
        let barrier = Arc::new(Barrier::default());
        let selected = barrier.clone();
        inbox.owned_fs_hook = Some(Arc::new(move |phase, actual| {
            if phase == event && actual == path {
                selected.park();
            }
            Ok(())
        }));
        Self::with_hook(inbox, barrier, job)
    }
    pub(super) fn with_hook<F, J>(
        mut inbox: FileSessionInbox,
        barrier: Arc<Barrier>,
        job: J,
    ) -> Self
    where
        F: std::future::Future<Output = Result<(), SessionInboxError>> + Send + 'static,
        J: FnOnce(FileSessionInbox) -> F,
    {
        let dropped = Arc::new(AtomicBool::new(false));
        inbox.owned_scope_drop = Some(dropped.clone());
        let runtime = Runtime::new().unwrap();
        let caller = runtime.spawn(job(inbox));
        let parked = Self {
            barrier,
            dropped,
            runtime: Some(runtime),
            caller,
        };
        parked.barrier.entered();
        parked
    }
    pub(super) fn shutdown(&mut self) {
        self.caller.abort();
        self.runtime
            .take()
            .unwrap()
            .shutdown_timeout(Duration::from_millis(40));
        // This is the implementation's INNER scope, not the aborted public wrapper.
        let deadline = Instant::now() + DEADLINE;
        while !self.dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(
            self.dropped.load(Ordering::SeqCst),
            "inner complete_owned scope survived shutdown"
        );
    }
    pub(super) fn result(&mut self) -> Result<(), SessionInboxError> {
        self.barrier.release();
        self.runtime.as_ref().unwrap().block_on(async {
            tokio::time::timeout(DEADLINE, &mut self.caller)
                .await
                .unwrap()
                .unwrap()
        })
    }
}
impl Drop for Parked {
    fn drop(&mut self) {
        self.barrier.release();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_millis(40));
        }
    }
}

#[test]
fn renewal_job_outlives_inner_shutdown_and_actual_epoch2_cannot_be_resurrected() {
    for phase in ["replace", "after_replace"] {
        let fixture = Fixture::new();
        let envelope = fixture.deliver();
        let first = fixture.request(0);
        let claim = fixture.claim(&first);
        let path = fixture.dir.join("cur").join(&claim.claim.claim_id);
        let mut renewal = first.clone();
        renewal.now += LeaseDuration::seconds(5); // expiry now15
        let current = claim.clone();
        let mut parked = Parked::start(
            fixture.inbox.clone(),
            phase,
            path.clone(),
            move |inbox| async move {
                inbox
                    .renew_owned("target", &current, &renewal)
                    .await
                    .map(|_| ())
            },
        );
        parked.shutdown();
        fixture.assert_physical_locks(true);
        let process = fixture.process_waiter();
        let successor = fixture.successor(16); // genuinely expired AFTER the renewal
        fixture.assert_waiting(&process);
        fixture.assert_waiting(&successor);
        parked.barrier.release();
        fixture.finish(process);
        let next = fixture.finish(successor).remove(0);
        assert_eq!(next.lease.epoch, 2);
        assert_eq!(next.claim.envelope, envelope);
        assert_eq!(next.claim.generation, claim.claim.generation);
        assert_eq!(next.claim.activation_policy, claim.claim.activation_policy);
        assert!(!path.exists());
        assert!(fixture.dir.join("cur").join(&next.claim.claim_id).exists());
        fixture.runtime.block_on(async {
            assert!(fixture
                .inbox
                .ack_owned("target", &claim, fixture.now + LeaseDuration::seconds(16))
                .await
                .is_err());
            assert!(fixture
                .inbox
                .renew_owned("target", &claim, &first)
                .await
                .is_err());
        });
        assert_eq!(
            std::fs::read_dir(fixture.dir.join("cur")).unwrap().count(),
            1
        );
    }
}

#[test]
fn caller_abort_preserves_detached_transaction_but_is_not_shutdown_evidence() {
    let fixture = Fixture::new();
    fixture.deliver();
    let first = fixture.request(0);
    let claim = fixture.claim(&first);
    let path = fixture.dir.join("cur").join(&claim.claim.claim_id);
    let mut request = first.clone();
    request.now += LeaseDuration::seconds(5);
    let parked = Parked::start(
        fixture.inbox.clone(),
        "replace",
        path,
        move |inbox| async move {
            inbox
                .renew_owned("target", &claim, &request)
                .await
                .map(|_| ())
        },
    );
    parked.caller.abort();
    fixture
        .runtime
        .block_on(async { tokio::time::sleep(Duration::from_millis(40)).await });
    assert!(!parked.dropped.load(Ordering::SeqCst));
    fixture.assert_physical_locks(true);
    let successor = fixture.successor(16);
    fixture.assert_waiting(&successor);
    parked.barrier.release();
    assert_eq!(fixture.finish(successor)[0].lease.epoch, 2);
    let deadline = Instant::now() + DEADLINE;
    while !parked.dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(parked.dropped.load(Ordering::SeqCst));
}

#[test]
fn ack_jobs_keep_locks_and_only_published_receipts_are_terminal_across_shutdown() {
    for phase in ["mkdir", "replace", "remove"] {
        let fixture = Fixture::new();
        fixture.deliver();
        let first = fixture.request(0);
        let claim = fixture.claim(&first);
        fixture.checkpoint(&claim);
        let path = if phase == "mkdir" {
            fixture.dir.join(ADMITTED_DIR)
        } else if phase == "replace" {
            FileSessionInbox::admitted_path(&fixture.dir, &claim.claim.envelope.id)
        } else {
            fixture.dir.join("cur").join(&claim.claim.claim_id)
        };
        let current = claim.clone();
        let now = fixture.now;
        let mut parked = Parked::start(
            fixture.inbox.clone(),
            phase,
            path,
            move |inbox| async move { inbox.ack_owned("target", &current, now).await },
        );
        parked.shutdown();
        fixture.assert_physical_locks(true);
        let successor = fixture.successor(11);
        fixture.assert_waiting(&successor);
        parked.barrier.release();
        let next = fixture.finish(successor);
        if phase == "mkdir" {
            // Shutdown can stop the NOT-YET-STARTED receipt job. The input
            // checkpoint exists, but no receipt was published: normal E2 reclaim.
            assert_eq!(next[0].lease.epoch, 2);
            assert!(
                !FileSessionInbox::admitted_path(&fixture.dir, &claim.claim.envelope.id).exists()
            );
            assert!(fixture
                .runtime
                .block_on(fixture.inbox.ack_owned(
                    "target",
                    &claim,
                    now + LeaseDuration::seconds(20)
                ))
                .is_err());
            continue;
        }
        assert!(next.is_empty(), "terminal receipt must forbid epoch2");
        fixture.runtime.block_on(async {
            let reopened = FileSessionInbox::new(
                Arc::new(
                    SessionStoreV2::new(fixture._temp.path().into())
                        .await
                        .unwrap(),
                ),
                SessionInboxLimits::default(),
            );
            reopened
                .ack_owned("target", &claim, now + LeaseDuration::seconds(20))
                .await
                .unwrap();
            let mut forged = claim.clone();
            forged.lease.epoch += 1;
            assert!(reopened
                .ack_owned("target", &forged, now + LeaseDuration::seconds(20))
                .await
                .is_err());
            let durable = fixture.store.load_session("target").await.unwrap().unwrap();
            assert_eq!(
                durable
                    .messages
                    .iter()
                    .filter(|m| m.id == claim.claim.envelope.id.as_str())
                    .count(),
                1
            );
        });
        assert!(!fixture.dir.join("cur").join(&claim.claim.claim_id).exists());
    }
}

#[test]
fn epoch2_first_old_ack_has_zero_publication_and_preserves_exact_successor() {
    let fixture = Fixture::new();
    fixture.deliver();
    let first = fixture.request(0);
    let old = fixture.claim(&first);
    let next = fixture.finish(fixture.successor(11)).remove(0);
    assert_eq!(next.lease.epoch, 2);
    let path = fixture.dir.join("cur").join(&next.claim.claim_id);
    let before = std::fs::read(&path).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut inbox = fixture.inbox.clone();
    inbox.owned_fs_hook = Some(Arc::new(move |phase, _| {
        if phase != "acquire" {
            observed.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }));
    assert!(fixture
        .runtime
        .block_on(inbox.ack_owned("target", &old, fixture.now + LeaseDuration::seconds(11)))
        .is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert!(!FileSessionInbox::admitted_path(&fixture.dir, &old.claim.envelope.id).exists());
}

#[test]
fn claim_headers_wrapper_and_rotation_keep_guards_with_existing_partial_recovery() {
    for selected in [
        "new",
        "cur",
        "corrupt",
        ACTIVATION_GENERATION_FILE,
        INTERRUPT_GENERATION_FILE,
        "wrapper",
        "rotation",
    ] {
        let fixture = Fixture::new();
        let envelope = fixture.deliver();
        let original = std::fs::read_dir(fixture.dir.join("new"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let phase = if matches!(selected, "new" | "cur" | "corrupt") {
            "mkdir"
        } else if selected == "rotation" {
            "rotate"
        } else {
            "replace"
        };
        let path = if matches!(selected, "wrapper" | "rotation") {
            original.clone()
        } else {
            fixture.dir.join(selected)
        };
        let request = fixture.request(0);
        let same_owner = request.clone();
        let mut parked = Parked::start(
            fixture.inbox.clone(),
            phase,
            path,
            move |inbox| async move {
                inbox
                    .claim_owned("target", 1, None, &request)
                    .await
                    .map(|_| ())
            },
        );
        parked.shutdown();
        fixture.assert_physical_locks(true);
        let successor = fixture.successor(11);
        fixture.assert_waiting(&successor);
        parked.barrier.release();
        let next = fixture.finish(successor).remove(0);
        assert_eq!(
            next.lease.epoch,
            if matches!(selected, "wrapper" | "rotation") {
                2
            } else {
                1
            }
        );
        assert_eq!(next.claim.envelope, envelope);
        assert!(!original.exists());
        fixture.runtime.block_on(async {
            assert!(FileSessionInbox::owned_enabled(&fixture.dir).await.unwrap());
            assert!(fixture.inbox.claim("target", 1).await.is_err());
            assert!(fixture
                .inbox
                .claim_owned("target", 1, None, &same_owner)
                .await
                .unwrap()
                .is_empty());
        });
    }
}

#[test]
fn terminal_cleanup_retry_and_owned_inspection_quarantine_have_real_job_ownership() {
    for action in ["terminal_claim", "terminal_ack", "inspect", "claim_scan"] {
        let fixture = Fixture::new();
        fixture.deliver();
        let first = fixture.request(0);
        let claim = fixture.claim(&first);
        let (phase, path) = if action.starts_with("terminal") {
            fixture.checkpoint(&claim);
            let mut fault = fixture.inbox.clone();
            fault.owned_ack_after_receipt_failure = true;
            assert!(fixture
                .runtime
                .block_on(fault.ack_owned("target", &claim, fixture.now))
                .is_err());
            (
                "remove",
                fixture.dir.join("cur").join(&claim.claim.claim_id),
            )
        } else {
            let path = fixture.dir.join("new").join("malformed.json");
            std::fs::write(&path, b"malformed").unwrap();
            ("quarantine", path)
        };
        let current = claim.clone();
        let now = fixture.now;
        let request = first.clone();
        let mut parked = Parked::start(
            fixture.inbox.clone(),
            phase,
            path.clone(),
            move |inbox| async move {
                match action {
                    "terminal_ack" => {
                        inbox
                            .ack_owned("target", &current, now + LeaseDuration::seconds(20))
                            .await
                    }
                    "inspect" => inbox
                        .inspect_owned_leases("target", 0, now)
                        .await
                        .map(|_| ()),
                    _ => inbox
                        .claim_owned("target", 1, None, &request)
                        .await
                        .map(|_| ()),
                }
            },
        );
        parked.shutdown();
        fixture.assert_physical_locks(true);
        let successor = fixture.successor(11);
        fixture.assert_waiting(&successor);
        parked.barrier.release();
        let result = fixture.finish(successor);
        if action.starts_with("terminal") {
            assert!(result.is_empty());
        } else {
            assert_eq!(result[0].lease.epoch, 2);
            assert!(fixture.dir.join("corrupt").join("malformed.json").exists());
        }
        assert!(!path.exists());
    }
}

#[test]
fn actual_write_error_cleanup_keeps_guards_and_primary_error_without_publication() {
    for shutdown in [false, true] {
        let fixture = Fixture::new();
        fixture.deliver();
        let first = fixture.request(0);
        let claim = fixture.claim(&first);
        let path = fixture.dir.join("cur").join(&claim.claim.claim_id);
        let before = std::fs::read(&path).unwrap();
        let barrier = Arc::new(Barrier::default());
        let selected = barrier.clone();
        let mut inbox = fixture.inbox.clone();
        let observed_path = path.clone();
        inbox.owned_fs_hook = Some(Arc::new(move |phase, actual| {
            if actual == observed_path {
                if phase == "write_temp" {
                    return Err(std::io::Error::other("primary write failure"));
                }
                if phase == "write_cleanup" {
                    selected.park();
                    return Err(std::io::Error::other("secondary observer failure"));
                }
            }
            Ok(())
        }));
        let mut request = first.clone();
        request.now += LeaseDuration::seconds(5);
        let mut parked = Parked::with_hook(inbox, barrier, move |inbox| async move {
            inbox
                .renew_owned("target", &claim, &request)
                .await
                .map(|_| ())
        });
        if shutdown {
            parked.shutdown();
        }
        fixture.assert_physical_locks(true);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let successor = fixture.successor(11);
        fixture.assert_waiting(&successor);
        if shutdown {
            parked.barrier.release();
        } else {
            let error = parked.result().unwrap_err().to_string();
            assert!(error.contains("primary write failure"));
            assert!(!error.contains("secondary"));
        }
        let next = fixture.finish(successor).remove(0);
        assert_eq!(next.lease.epoch, 2);
        assert!(!path.exists());
        assert!(!before.is_empty());
        assert!(!std::fs::read_dir(fixture.dir.join("cur"))
            .unwrap()
            .any(|e| e.unwrap().file_name().to_string_lossy().contains("tmp.")));
    }
}

#[test]
fn failed_replace_rotation_and_remove_keep_existing_residue_and_terminal_contracts() {
    for failure in ["write", "replace", "after_replace", "rotate", "remove"] {
        let fixture = Fixture::new();
        fixture.deliver();
        let first = fixture.request(0);
        let existing = (failure != "rotate").then(|| fixture.claim(&first));
        let path = existing
            .as_ref()
            .map(|claim| fixture.dir.join("cur").join(&claim.claim.claim_id))
            .unwrap_or_else(|| {
                std::fs::read_dir(fixture.dir.join("new"))
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .path()
            });
        if failure == "remove" {
            fixture.checkpoint(existing.as_ref().unwrap());
        }
        let barrier = Arc::new(Barrier::default());
        let selected = barrier.clone();
        let mut inbox = fixture.inbox.clone();
        let selected_path = path.clone();
        inbox.owned_fs_hook = Some(Arc::new(move |phase, actual| {
            if phase == failure && actual == selected_path {
                selected.park();
                return Err(std::io::Error::other("selected filesystem failure"));
            }
            Ok(())
        }));
        let mut request = first.clone();
        request.now += LeaseDuration::seconds(5);
        let current = existing.clone();
        let now = fixture.now;
        let mut parked = Parked::with_hook(inbox, barrier, move |inbox| async move {
            match current {
                None => inbox
                    .claim_owned("target", 1, None, &request)
                    .await
                    .map(|_| ()),
                Some(claim) if failure == "remove" => inbox.ack_owned("target", &claim, now).await,
                Some(claim) => inbox
                    .renew_owned("target", &claim, &request)
                    .await
                    .map(|_| ()),
            }
        });
        fixture.assert_physical_locks(true);
        let process = fixture.process_waiter();
        fixture.assert_waiting(&process);
        assert!(parked
            .result()
            .unwrap_err()
            .to_string()
            .contains("selected filesystem failure"));
        fixture.finish(process);
        if failure == "remove" {
            assert!(path.exists());
            assert!(fixture.finish(fixture.successor(20)).is_empty());
            assert!(!path.exists());
        } else if failure == "rotate" {
            let retry = fixture.claim(&request_at(&first, 5));
            assert_eq!(retry.lease.epoch, 1);
            assert!(!path.exists());
        } else {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            let expiry: chrono::DateTime<Utc> =
                serde_json::from_value(value[owned::LEASE_KEY]["token"]["expires_at"].clone())
                    .unwrap();
            assert_eq!(
                expiry,
                first.now
                    + LeaseDuration::seconds(if failure == "after_replace" { 15 } else { 10 })
            );
            let temps = std::fs::read_dir(path.parent().unwrap())
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .contains("tmp.")
                })
                .count();
            assert_eq!(temps, usize::from(failure == "replace")); // existing rename-failure residue
        }
    }
}
fn request_at(first: &SessionInboxLeaseRequest, seconds: i64) -> SessionInboxLeaseRequest {
    let mut request = first.clone();
    request.now += LeaseDuration::seconds(seconds);
    request
}

#[test]
fn acquisition_setup_keeps_preceding_scope_until_deleted_target_can_pass() {
    let fixture = Fixture::new();
    assert!(!fixture.dir.exists());
    let request = fixture.request(0);
    let dir = fixture.dir.clone();
    let mut parked = Parked::start(
        fixture.inbox.clone(),
        "acquire",
        dir.clone(),
        move |inbox| async move {
            inbox
                .claim_owned("target", 1, None, &request)
                .await
                .map(|_| ())
        },
    );
    parked.shutdown();
    fixture.assert_physical_locks(false);
    let process = fixture.process_waiter();
    fixture.assert_waiting(&process);
    let store = fixture.other_store.clone();
    let delete = fixture
        .runtime
        .spawn(async move { store.delete_session("target").await.unwrap() });
    fixture.assert_waiting(&delete);
    parked.barrier.release();
    // This same-adapter waiter probes the original process mutex directly;
    // the independent actual delete must wait on the retained lifecycle FD.
    fixture.finish(process);
    assert!(fixture.finish(delete));
    assert!(!dir.parent().unwrap().exists());
    assert!(probe(
        &fixture
            .store
            .bamboo_home_dir()
            .join(".session-lifecycle.lock")
    ));
}

#[test]
fn actual_os_replace_error_retains_scope_and_leaves_original_temp_residue() {
    let fixture = Fixture::new();
    fixture.deliver();
    let first = fixture.request(0);
    let claim = fixture.claim(&first);
    let path = fixture.dir.join("cur").join(&claim.claim.claim_id);
    let before = std::fs::read(&path).unwrap();
    let barrier = Arc::new(Barrier::default());
    let selected = barrier.clone();
    let mut inbox = fixture.inbox.clone();
    let selected_path = path.clone();
    inbox.owned_fs_hook = Some(Arc::new(move |phase, actual| {
        if phase == "replace" && actual == selected_path {
            // Fault setup inside the held job makes the ACTUAL OS replace fail.
            std::fs::remove_file(actual)?;
            std::fs::create_dir(actual)?;
            selected.park();
        }
        Ok(())
    }));
    let renewal = request_at(&first, 5);
    let mut parked = Parked::with_hook(inbox, barrier, move |inbox| async move {
        inbox
            .renew_owned("target", &claim, &renewal)
            .await
            .map(|_| ())
    });
    fixture.assert_physical_locks(true);
    let process = fixture.process_waiter();
    fixture.assert_waiting(&process);
    let error = parked.result().unwrap_err().to_string();
    assert!(!error.contains("injected"));
    fixture.finish(process);
    assert!(path.is_dir());
    let temps = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("tmp.")
        })
        .count();
    assert_eq!(temps, 1);
    // Restore only this fixture's injected directory fault, not production repair.
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, before).unwrap();
    let retry = fixture.claim(&first);
    let renewed = fixture
        .runtime
        .block_on(
            fixture
                .inbox
                .renew_owned("target", &retry, &request_at(&first, 5)),
        )
        .unwrap();
    assert_eq!(
        renewed.lease.expires_at,
        first.now + LeaseDuration::seconds(15)
    );
    assert_eq!(fixture.finish(fixture.successor(16))[0].lease.epoch, 2);
    assert!(!path.exists());
}
