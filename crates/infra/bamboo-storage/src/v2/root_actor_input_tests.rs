//! Root-bound owned input uses the actual full writer, ACK, and physical locks.
//! Production capability issuance is proved separately by the Host fixtures.

use super::default_actor_context_tests::DefaultWriteHook;
use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryPort, Message, RootActorRuntimeWrite,
    SessionActivationPolicy, SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxLimits,
    SessionInboxOwnedClaim, SessionInboxPort, SessionMessageEnvelope, Storage,
};
use chrono::Duration as LeaseDuration;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex as StdMutex,
};

const ROOT: &str = "owned-root-input";
const WAIT: Duration = Duration::from_secs(8);

struct Release(Arc<DefaultWriteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Fixture {
    a: Arc<SessionStoreV2>,
    b: Arc<SessionStoreV2>,
    raw: Arc<crate::FileSessionInbox>,
    bound: Arc<dyn SessionInboxPort>,
    base: Session,
    owner: RootActorRuntimeWrite,
    root_expires_at: DateTime<Utc>,
    input: SessionMessageEnvelope,
    publications: Arc<StdMutex<Vec<Session>>>,
    _temp: tempfile::TempDir,
}

impl Fixture {
    async fn new(root_duration: LeaseDuration) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let a = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let mut base = Session::new(ROOT, "controlled-model");
        base.add_message(Message::user("original Root transcript"));
        a.save_session(&base).await.unwrap();
        let b = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let raw = Arc::new(crate::FileSessionInbox::new(
            a.clone(),
            SessionInboxLimits::default(),
        ));
        let input = SessionMessageEnvelope::user_input(ROOT, "exact owned Root input");
        raw.deliver_with_activation_intent(
            &input,
            SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .unwrap();
        let now = Utc::now();
        let root_expires_at = now + root_duration;
        let activation = a
            .claim_activation(&ActorActivationClaim {
                actor_id: ROOT.into(),
                run_id: "run-a".into(),
                lease_owner: "fixture-root-a".into(),
                lease_expires_at: root_expires_at,
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        a.start_activation(&activation.fence(), Utc::now())
            .await
            .unwrap();
        let owner = RootActorRuntimeWrite {
            fence: activation.fence(),
            created_at: base.created_at,
        };
        let bound = a.bind_root_actor_inbox(&owner, raw.clone()).unwrap();
        Self {
            a,
            b,
            raw,
            bound,
            base,
            owner,
            root_expires_at,
            input,
            publications: Arc::default(),
            _temp: temp,
        }
    }

    fn directory(&self) -> PathBuf {
        self.a.bamboo_home_dir.join("sessions").join(ROOT)
    }

    fn publisher(&self) -> bamboo_domain::storage::RootActorRuntimePublisher {
        let publications = self.publications.clone();
        Arc::new(move |session| publications.lock().unwrap().push(session.clone()))
    }

    async fn claim(&self, duration: LeaseDuration) -> SessionInboxOwnedClaim {
        let now = Utc::now();
        let duration = duration.min(self.root_expires_at - now - LeaseDuration::milliseconds(100));
        let request = SessionInboxLeaseRequest {
            consumer: SessionInboxConsumerId::new(),
            now,
            duration,
        };
        let mut claims = self
            .bound
            .claim_owned(ROOT, 1, Some("run-a"), &request)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        let claim = claims.remove(0);
        assert_eq!(claim.claim.envelope, self.input);
        assert!(claim.lease.expires_at <= self.root_expires_at);
        claim
    }

    fn candidate(&self, claim: &SessionInboxOwnedClaim) -> Session {
        let mut candidate = self.base.clone();
        candidate.add_message(claim.claim.envelope.to_provider_message().unwrap());
        candidate
            .session_inbox_admission_mut()
            .record(claim.claim.envelope.id.clone(), claim.claim.generation);
        candidate.updated_at = Utc::now();
        candidate
    }

    async fn checkpoint(&self, claim: &SessionInboxOwnedClaim) -> io::Result<()> {
        self.a
            .save_root_actor_input(
                &self.owner,
                &self.candidate(claim),
                self.bound.clone(),
                claim,
                self.publisher(),
            )
            .await
    }

    async fn assert_unadmitted(&self) {
        assert!(!self.raw.was_admitted(ROOT, &self.input.id).await.unwrap());
        let main: Session =
            serde_json::from_slice(&std::fs::read(self.directory().join("session.json")).unwrap())
                .unwrap();
        assert!(!main
            .messages
            .iter()
            .any(|message| message.id == self.input.id.as_str()));
        assert!(main
            .session_inbox_admission()
            .is_none_or(|a| !a.contains(&self.input.id)));
        assert!(self.publications.lock().unwrap().is_empty());
    }
}

async fn expire(deadline: DateTime<Utc>) {
    tokio::time::timeout(WAIT, async {
        while Utc::now() <= deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("lease expires in actual wall time");
}

fn held(f: &Fixture) {
    for path in [
        f.a.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        f.a.bamboo_home_dir.join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        f.a.session_write_lock_path(ROOT),
        f.directory().join("inbox/.session-inbox.lock"),
    ] {
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
            "actual Root/Input guard was released: {}",
            path.display()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_owned_full_checkpoint_commits_typed_input_cursor_and_ack_before_return() {
    let f = Fixture::new(LeaseDuration::seconds(30)).await;
    let claim = f.claim(LeaseDuration::seconds(5)).await;
    f.checkpoint(&claim).await.unwrap();
    assert!(f.raw.was_admitted(ROOT, &f.input.id).await.unwrap());
    assert!(!f
        .directory()
        .join("inbox/cur")
        .join(&claim.claim.claim_id)
        .exists());
    let durable = f.b.load_session(ROOT).await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|message| bamboo_domain::is_matching_session_message(message, &f.input))
            .count(),
        1
    );
    assert_eq!(durable.messages.len(), f.base.messages.len() + 1);
    let admission = durable.session_inbox_admission().unwrap();
    assert!(admission.contains(&f.input.id));
    assert_eq!(admission.last_admitted_sequence, claim.claim.generation);
    let side: Session =
        serde_json::from_slice(&std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap())
            .unwrap();
    assert!(side.messages.is_empty());
    assert!(
        side.session_inbox_admission().is_none(),
        "Main alone owns message+admission atomicity"
    );
    let publications = f.publications.lock().unwrap();
    assert_eq!(publications.len(), 1);
    assert_eq!(
        serde_json::to_value(&publications[0].messages).unwrap(),
        serde_json::to_value(&durable.messages).unwrap()
    );
    drop(publications);
    let redelivery = f
        .raw
        .deliver_with_activation_intent(
            &f.input,
            SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .unwrap();
    assert_eq!(redelivery.generation, claim.claim.generation);
    assert!(
        f.bound.release_owned(ROOT, &claim).await.is_err(),
        "release never reverses a committed ACK"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_owned_renew_fences_old_token_and_release_preserves_input_without_failure() {
    let f = Fixture::new(LeaseDuration::seconds(30)).await;
    let first = f.claim(LeaseDuration::seconds(2)).await;
    let renewal = SessionInboxLeaseRequest {
        consumer: first.lease.consumer.clone(),
        now: Utc::now(),
        duration: LeaseDuration::seconds(5),
    };
    let renewed = f.bound.renew_owned(ROOT, &first, &renewal).await.unwrap();
    assert_eq!(renewed.claim, first.claim);
    assert!(renewed.lease.expires_at > first.lease.expires_at);
    assert!(renewed.lease.expires_at <= f.root_expires_at);
    assert!(f.checkpoint(&first).await.is_err());
    assert!(f.bound.ack_owned(ROOT, &first, Utc::now()).await.is_err());
    assert!(f.bound.release_owned(ROOT, &first).await.is_err());
    f.assert_unadmitted().await;
    f.bound.release_owned(ROOT, &renewed).await.unwrap();
    let retry = f.claim(LeaseDuration::seconds(5)).await;
    assert_eq!(retry.claim.envelope, renewed.claim.envelope);
    assert_eq!(retry.claim.generation, renewed.claim.generation);
    assert_eq!(
        retry.claim.activation_policy,
        renewed.claim.activation_policy
    );
    assert!(retry.lease.epoch > renewed.lease.epoch);
    let path = f.directory().join("inbox/cur").join(&retry.claim.claim_id);
    let before = std::fs::read(&path).unwrap();
    assert!(f.bound.release_owned(ROOT, &renewed).await.is_err());
    assert_eq!(
        std::fs::read(path).unwrap(),
        before,
        "old token release cannot change the live successor incarnation"
    );
    let leases = f
        .raw
        .inspect_owned_leases(ROOT, 1, Utc::now())
        .await
        .unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].failure_count, 0);
    assert!(leases[0].last_error_code.is_none());
    assert!(leases[0].retry_after.is_none());
    f.checkpoint(&retry).await.unwrap();
    assert!(f.raw.was_admitted(ROOT, &f.input.id).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_owned_final_full_write_rejects_real_root_or_input_expiry() {
    for file in [RUNTIME_SIDECAR_FILE, "session.json"] {
        for root_expiry in [false, true] {
            let f = Arc::new(
                Fixture::new(if root_expiry {
                    LeaseDuration::seconds(2)
                } else {
                    LeaseDuration::seconds(30)
                })
                .await,
            );
            let claim = f
                .claim(if root_expiry {
                    LeaseDuration::seconds(5)
                } else {
                    LeaseDuration::milliseconds(250)
                })
                .await;
            let deadline = if root_expiry {
                f.root_expires_at
            } else {
                claim.lease.expires_at
            };
            let before_main = std::fs::read(f.directory().join("session.json")).unwrap();
            let before_runtime = std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap();
            let hook =
                DefaultWriteHook::install(&f.a, file, DurableWritePhase::BeforeReplace, false);
            let _release = Release(hook.clone());
            let run = f.clone();
            let current = claim.clone();
            let writer = tokio::spawn(async move { run.checkpoint(&current).await });
            hook.wait();
            held(&f);
            expire(deadline).await;
            hook.release();
            assert!(tokio::time::timeout(WAIT, writer)
                .await
                .unwrap()
                .unwrap()
                .is_err());
            assert_eq!(
                std::fs::read(f.directory().join("session.json")).unwrap(),
                before_main
            );
            if file == RUNTIME_SIDECAR_FILE {
                assert_eq!(
                    std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap(),
                    before_runtime
                );
            }
            // At Main BeforeReplace a preceding runtime write was legal. Main
            // input/cursor, shared publication and ACK must still be absent.
            f.assert_unadmitted().await;
            assert!(f
                .directory()
                .join("inbox/cur")
                .join(&claim.claim.claim_id)
                .exists());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_root_input_job_retains_real_joint_guards_and_rejects_after_reclaim_deadline() {
    let f = Arc::new(Fixture::new(LeaseDuration::seconds(2)).await);
    let claim = f.claim(LeaseDuration::seconds(5)).await;
    let hook = DefaultWriteHook::install(
        &f.a,
        "session.json",
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let run = f.clone();
    let current = claim.clone();
    let writer = tokio::spawn(async move { run.checkpoint(&current).await });
    hook.wait();
    held(&f);
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    held(&f);
    expire(f.root_expires_at).await;
    let other = f.b.clone();
    let claimant = tokio::spawn(async move {
        let now = Utc::now();
        other
            .claim_activation(&ActorActivationClaim {
                actor_id: ROOT.into(),
                run_id: "run-b".into(),
                lease_owner: "fixture-root-b".into(),
                lease_expires_at: now + LeaseDuration::seconds(30),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !claimant.is_finished(),
        "B cannot reclaim through a still-running physical checkpoint"
    );
    held(&f);
    hook.release();
    let activation = tokio::time::timeout(WAIT, claimant)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.assert_unadmitted().await;
    checkpoint_successor_once(&f, &claim, &activation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_root_input_without_ack_is_reclaimed_once_by_successor() {
    let f = Arc::new(Fixture::new(LeaseDuration::seconds(2)).await);
    let claim = f.claim(LeaseDuration::seconds(5)).await;
    let hook =
        DefaultWriteHook::install(&f.a, "session.json", DurableWritePhase::AfterReplace, false);
    let _release = Release(hook.clone());
    let run = f.clone();
    let current = claim.clone();
    let writer = tokio::spawn(async move { run.checkpoint(&current).await });
    hook.wait();
    held(&f);
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    held(&f);
    expire(f.root_expires_at).await;
    let other = f.b.clone();
    let claimant = tokio::spawn(async move {
        let now = Utc::now();
        other
            .claim_activation(&ActorActivationClaim {
                actor_id: ROOT.into(),
                run_id: "run-b".into(),
                lease_owner: "fixture-root-b".into(),
                lease_expires_at: now + LeaseDuration::seconds(30),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !claimant.is_finished(),
        "B cannot reclaim through a still-running physical checkpoint"
    );
    held(&f);
    hook.release();
    let activation = tokio::time::timeout(WAIT, claimant)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Main replacement finished in the physical job before caller abort.
    // ACK never ran, so the exact typed input remains recoverable in cur.
    assert!(!f.raw.was_admitted(ROOT, &f.input.id).await.unwrap());
    assert!(f.publications.lock().unwrap().is_empty());
    let canonical = f.b.load_session(ROOT).await.unwrap().unwrap();
    assert_eq!(
        canonical
            .messages
            .iter()
            .filter(|message| bamboo_domain::is_matching_session_message(message, &f.input))
            .count(),
        1
    );
    assert!(canonical
        .session_inbox_admission()
        .unwrap()
        .contains(&f.input.id));
    checkpoint_successor_once(&f, &claim, &activation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_ack_job_rechecks_expiry_after_wait_and_retains_joint_guards_on_abort() {
    let f = Arc::new(Fixture::new(LeaseDuration::seconds(2)).await);
    let claim = f.claim(LeaseDuration::seconds(5)).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let pause = Arc::new((StdMutex::new(false), std::sync::Condvar::new()));
    struct ReleaseAck(Arc<(StdMutex<bool>, std::sync::Condvar)>);
    impl Drop for ReleaseAck {
        fn drop(&mut self) {
            *self.0 .0.lock().unwrap() = true;
            self.0 .1.notify_all();
        }
    }
    let release = ReleaseAck(pause.clone());
    let signal = entered.clone();
    let wait = pause.clone();
    let hooked = f
        .raw
        .with_owned_filesystem_hook_for_test(Arc::new(move |event, path| {
            if event == "replace"
                && path
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "admitted")
            {
                signal.notify_one();
                let (lock, cv) = &*wait;
                let (released, timeout) = cv
                    .wait_timeout_while(lock.lock().unwrap(), WAIT, |released| !*released)
                    .unwrap();
                if timeout.timed_out() && !*released {
                    return Err(io::Error::other("ACK fixture release timed out"));
                }
            }
            Ok(())
        }));
    let bound =
        f.a.bind_root_actor_inbox(&f.owner, Arc::new(hooked))
            .unwrap();
    let run = f.clone();
    let current = claim.clone();
    let writer = tokio::spawn(async move {
        run.a
            .save_root_actor_input(
                &run.owner,
                &run.candidate(&current),
                bound,
                &current,
                run.publisher(),
            )
            .await
    });
    tokio::time::timeout(WAIT, entered.notified())
        .await
        .unwrap();
    // This is the actual receipt BeforeReplace, after Main/cache publication.
    held(&f);
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    held(&f);
    expire(f.root_expires_at).await;
    let other = f.b.clone();
    let claimant = tokio::spawn(async move {
        let now = Utc::now();
        other
            .claim_activation(&ActorActivationClaim {
                actor_id: ROOT.into(),
                run_id: "run-b".into(),
                lease_owner: "fixture-root-b".into(),
                lease_expires_at: now + LeaseDuration::seconds(30),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !claimant.is_finished(),
        "B cannot reclaim through a still-running physical checkpoint"
    );
    held(&f);
    drop(release);
    let activation = tokio::time::timeout(WAIT, claimant)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Main finished, but the actual expired ACK job must not mint a receipt.
    // The exact typed input remains recoverable in cur.
    assert!(!f.raw.was_admitted(ROOT, &f.input.id).await.unwrap());
    assert_eq!(
        f.publications.lock().unwrap().len(),
        1,
        "cache publication was legal before ACK expiry"
    );
    let canonical = f.b.load_session(ROOT).await.unwrap().unwrap();
    assert_eq!(
        canonical
            .messages
            .iter()
            .filter(|message| bamboo_domain::is_matching_session_message(message, &f.input))
            .count(),
        1
    );
    assert!(canonical
        .session_inbox_admission()
        .unwrap()
        .contains(&f.input.id));
    checkpoint_successor_once(&f, &claim, &activation).await;
}

async fn checkpoint_successor_once(
    f: &Fixture,
    claim: &SessionInboxOwnedClaim,
    activation: &bamboo_domain::ActorActivation,
) {
    assert!(activation.attempt > f.owner.fence.attempt);
    assert!(activation.lease_epoch > f.owner.fence.lease_epoch);
    f.b.start_activation(&activation.fence(), Utc::now())
        .await
        .unwrap();
    let b_owner = RootActorRuntimeWrite {
        fence: activation.fence(),
        created_at: f.base.created_at,
    };
    let raw_b = Arc::new(crate::FileSessionInbox::new(
        f.b.clone(),
        SessionInboxLimits::default(),
    ));
    let b_inbox = f.b.bind_root_actor_inbox(&b_owner, raw_b.clone()).unwrap();
    let request = SessionInboxLeaseRequest {
        consumer: SessionInboxConsumerId::new(),
        now: Utc::now(),
        duration: LeaseDuration::seconds(5),
    };
    let b_claim = b_inbox
        .claim_owned(ROOT, 1, Some("run-b"), &request)
        .await
        .unwrap()
        .remove(0);
    assert!(b_claim.lease.epoch > claim.lease.epoch);
    let b_path = f
        .directory()
        .join("inbox/cur")
        .join(&b_claim.claim.claim_id);
    let before = std::fs::read(&b_path).unwrap();
    assert!(f.bound.release_owned(ROOT, &claim).await.is_err());
    assert!(f.bound.renew_owned(ROOT, &claim, &request).await.is_err());
    assert!(f.bound.ack_owned(ROOT, &claim, Utc::now()).await.is_err());
    assert_eq!(std::fs::read(b_path).unwrap(), before);
    let published = Arc::new(AtomicUsize::new(0));
    let mark = published.clone();
    f.b.save_root_actor_input(
        &b_owner,
        &f.candidate(&b_claim),
        b_inbox,
        &b_claim,
        Arc::new(move |_| {
            mark.fetch_add(1, Ordering::SeqCst);
        }),
    )
    .await
    .unwrap();
    assert_eq!(published.load(Ordering::SeqCst), 1);
    assert!(raw_b.was_admitted(ROOT, &f.input.id).await.unwrap());
    let durable = f.b.load_session(ROOT).await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|message| bamboo_domain::is_matching_session_message(message, &f.input))
            .count(),
        1
    );
}
