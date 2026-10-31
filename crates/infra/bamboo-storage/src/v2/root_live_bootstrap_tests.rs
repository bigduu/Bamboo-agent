//! Controlled V2 checkpoint/confirm seam. No provider, Host or network.
use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, AdmissionGate, Message,
    RootActorRuntimeEventPublisher, RootActorRuntimePublisher, RootActorRuntimeWrite,
    SessionInboxLimits, SessionInboxPort, SessionMessageEnvelope, Storage,
};
use std::sync::atomic::{AtomicUsize, Ordering};

const ROOT: &str = "live-bootstrap-root";

struct Fixture {
    store: Arc<SessionStoreV2>,
    inbox: Arc<crate::FileSessionInbox>,
    owner: RootActorRuntimeWrite,
    staged: Session,
    _temp: tempfile::TempDir,
}

impl Fixture {
    async fn new(lease: chrono::Duration) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let mut original = Session::new(ROOT, "mock-model");
        original.add_message(Message::user("original message"));
        store.save_session(&original).await.unwrap();
        let now = Utc::now();
        let activation = store
            .claim_activation(&ActorActivationClaim {
                actor_id: ROOT.into(),
                run_id: "live-run".into(),
                lease_owner: "fixture".into(),
                lease_expires_at: now + lease,
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        store
            .start_activation(&activation.fence(), Utc::now())
            .await
            .unwrap();
        let owner = RootActorRuntimeWrite {
            fence: activation.fence(),
            created_at: original.created_at,
            conversation_generation: original.conversation_generation_observation(),
        };
        let inbox = Arc::new(crate::FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let mut staged = store.load_session(ROOT).await.unwrap().unwrap();
        staged.add_message(Message::tool_result("test-tool-call", "already executed"));
        Self {
            store,
            inbox,
            owner,
            staged,
            _temp: temp,
        }
    }

    fn publish() -> RootActorRuntimePublisher {
        Arc::new(|_| {})
    }

    async fn capture(&self, gate: AdmissionGate) -> io::Result<bamboo_domain::LiveBootstrapProof> {
        self.store
            .checkpoint_root_live_bootstrap(
                &self.owner,
                &self.staged,
                self.inbox.clone(),
                gate,
                Self::publish(),
            )
            .await
    }

    async fn confirm(
        &self,
        proof: &bamboo_domain::LiveBootstrapProof,
        counter: Arc<AtomicUsize>,
    ) -> io::Result<()> {
        self.store
            .confirm_root_live_bootstrap(
                &self.owner,
                proof,
                self.inbox.clone(),
                count_callback(counter),
            )
            .await
    }
}

fn count_callback(counter: Arc<AtomicUsize>) -> RootActorRuntimeEventPublisher {
    Box::new(move |check| {
        check()?;
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
}

#[tokio::test]
async fn capture_persists_tool_result_and_binds_the_exact_gate() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let gate = AdmissionGate::default();
    let proof = f.capture(gate.clone()).await.unwrap();
    assert!(proof.matches_gate(&gate));
    assert!(!proof.matches_gate(&AdmissionGate::default()));
    let durable = f.store.load_session(ROOT).await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|m| m.tool_call_id.as_deref() == Some("test-tool-call"))
            .count(),
        1
    );
}

#[tokio::test]
async fn changed_canonical_session_and_pending_inbox_refuse() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = f.capture(AdmissionGate::default()).await.unwrap();
    let mut changed = f.store.load_session(ROOT).await.unwrap().unwrap();
    changed.add_message(Message::user("arrived later"));
    f.store
        .save_root_actor_runtime(&f.owner, &changed, false, Fixture::publish())
        .await
        .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    assert!(f.confirm(&proof, count.clone()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);

    let g = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = g.capture(AdmissionGate::default()).await.unwrap();
    let message = SessionMessageEnvelope::user_input(ROOT, "queued while waiting");
    g.inbox.deliver(&message).await.unwrap();
    assert!(g.confirm(&proof, count.clone()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancellation_before_capture_or_confirmation_never_invokes_callback() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let cancelled = AdmissionGate::default();
    cancelled.cancel_if_pending();
    assert!(f.capture(cancelled).await.is_err());
    let durable = f.store.load_session(ROOT).await.unwrap().unwrap();
    assert!(!durable
        .messages
        .iter()
        .any(|m| m.tool_call_id.as_deref() == Some("test-tool-call")));
    let gate = AdmissionGate::default();
    let proof = f.capture(gate.clone()).await.unwrap();
    proof.cancel_if_pending();
    assert!(proof.is_cancelled());
    assert!(gate.is_cancelled());
    let count = Arc::new(AtomicUsize::new(0));
    assert!(f.confirm(&proof, count.clone()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn expired_or_retired_actor_cannot_confirm() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = f.capture(AdmissionGate::default()).await.unwrap();
    // Deterministic local fixture fault: expire the real stored activation.
    let path = f.store.sessions_dir.join(ROOT).join("actor-authority.json");
    let mut entry: bamboo_domain::ActorDirectoryEntry =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    entry.activation.as_mut().unwrap().lease_expires_at = Utc::now() - chrono::Duration::seconds(1);
    std::fs::write(&path, serde_json::to_vec(&entry).unwrap()).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    assert!(f.confirm(&proof, count.clone()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);

    let g = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = g.capture(AdmissionGate::default()).await.unwrap();
    g.store
        .finish_activation(&g.owner.fence, Utc::now(), ActorActivationFinish::Cancelled)
        .await
        .unwrap();
    assert!(g.confirm(&proof, count.clone()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn competing_confirms_consume_only_once_even_if_cloned() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = f.capture(AdmissionGate::default()).await.unwrap();
    let second = proof.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let (a, b) = tokio::join!(
        f.confirm(&proof, count.clone()),
        f.confirm(&second, count.clone())
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(proof.is_committed());
}

#[tokio::test]
async fn callback_error_after_side_effect_burns_proof() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = f.capture(AdmissionGate::default()).await.unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let output = count.clone();
    let callback: RootActorRuntimeEventPublisher = Box::new(move |check| {
        check()?;
        output.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::other("simulated lost acknowledgment"))
    });
    assert!(f
        .store
        .confirm_root_live_bootstrap(&f.owner, &proof, f.inbox.clone(), callback)
        .await
        .is_err());
    assert!(f.confirm(&proof, count.clone()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(!proof.is_committed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_of_failed_callback_cannot_observe_committed_gate() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = f.capture(AdmissionGate::default()).await.unwrap();
    let (sent, received) = std::sync::mpsc::channel::<()>();
    let receiver_proof = proof.clone();
    let receiver = std::thread::spawn(move || {
        received.recv().unwrap();
        // The gate mutex remains locked until the callback has returned.
        receiver_proof.is_committed()
    });
    let callback: RootActorRuntimeEventPublisher = Box::new(move |check| {
        check()?;
        sent.send(()).map_err(io::Error::other)?;
        Err(io::Error::other("fault after synthetic send"))
    });
    assert!(f
        .store
        .confirm_root_live_bootstrap(&f.owner, &proof, f.inbox.clone(), callback)
        .await
        .is_err());
    assert!(!receiver.join().unwrap());
}

#[tokio::test]
async fn stale_prepared_session_is_rejected_before_tool_result_write() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let mut changed = f.store.load_session(ROOT).await.unwrap().unwrap();
    changed.add_message(Message::user("new durable input"));
    f.store
        .save_root_actor_runtime(&f.owner, &changed, false, Fixture::publish())
        .await
        .unwrap();
    assert!(f.capture(AdmissionGate::default()).await.is_err());
    let durable = f.store.load_session(ROOT).await.unwrap().unwrap();
    assert!(durable
        .messages
        .iter()
        .any(|m| m.content == "new durable input"));
    assert!(!durable
        .messages
        .iter()
        .any(|m| m.tool_call_id.as_deref() == Some("test-tool-call")));
}

#[tokio::test]
async fn newer_durable_summary_is_never_overwritten_by_stale_capture() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    assert!(f.staged.conversation_summary.is_none());
    let mut newer = f.store.load_session(ROOT).await.unwrap().unwrap();
    newer.conversation_summary = Some(bamboo_domain::ConversationSummary::new(
        "newer canonical summary",
        1,
        5,
    ));
    f.store
        .save_root_actor_runtime(&f.owner, &newer, true, Fixture::publish())
        .await
        .unwrap();

    assert!(f.capture(AdmissionGate::default()).await.is_err());
    let durable = f.store.load_session(ROOT).await.unwrap().unwrap();
    assert_eq!(
        durable.conversation_summary.unwrap().content,
        "newer canonical summary"
    );
    assert!(!durable
        .messages
        .iter()
        .any(|m| m.tool_call_id.as_deref() == Some("test-tool-call")));
}

#[tokio::test]
async fn new_user_message_is_not_an_allowed_uncommitted_tool_suffix() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let mut candidate = f.staged.clone();
    candidate.add_message(Message::user("unexpected uncommitted input"));
    assert!(f
        .store
        .checkpoint_root_live_bootstrap(
            &f.owner,
            &candidate,
            f.inbox.clone(),
            AdmissionGate::default(),
            Fixture::publish(),
        )
        .await
        .is_err());
    let durable = f.store.load_session(ROOT).await.unwrap().unwrap();
    assert!(!durable
        .messages
        .iter()
        .any(|m| m.content == "unexpected uncommitted input"));
}

#[tokio::test]
async fn inbox_enqueue_then_cancel_cannot_restore_old_generation() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = f.capture(AdmissionGate::default()).await.unwrap();
    let mut message = SessionMessageEnvelope::user_input(ROOT, "guidance that will be cancelled");
    message.correlation_id = Some("session-guidance".into());
    f.inbox.deliver(&message).await.unwrap();
    assert!(f.inbox.cancel_guidance(ROOT, &message.id).await.unwrap());
    let backlog = f.inbox.inspect(ROOT).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (0, 0));
    assert!(backlog.generation > 0);
    let count = Arc::new(AtomicUsize::new(0));
    assert!(f.confirm(&proof, count.clone()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn saturated_generation_and_unknown_queue_entries_fail_closed() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let dir = f.store.sessions_dir.join(ROOT).join("inbox");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("generation"), u64::MAX.to_string()).unwrap();
    assert!(f.capture(AdmissionGate::default()).await.is_err());
    std::fs::write(dir.join("generation"), "0").unwrap();
    std::fs::create_dir_all(dir.join("new")).unwrap();
    std::fs::write(dir.join("new/.unexpected"), "unknown").unwrap();
    assert!(f.capture(AdmissionGate::default()).await.is_err());
}

#[tokio::test]
async fn oversized_recovery_snapshot_refuses_before_persisting() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let mut oversized = f.staged.clone();
    oversized.add_message(Message::user(
        "x".repeat(crate::session_merge::LIVE_BOOTSTRAP_SESSION_JSON_LIMIT),
    ));
    assert!(f
        .store
        .checkpoint_root_live_bootstrap(
            &f.owner,
            &oversized,
            f.inbox.clone(),
            AdmissionGate::default(),
            Fixture::publish(),
        )
        .await
        .is_err());
    let durable = f.store.load_session(ROOT).await.unwrap().unwrap();
    assert!(!durable
        .messages
        .iter()
        .any(|m| m.tool_call_id.as_deref() == Some("test-tool-call")));

    let mut oversized_skipped = f.staged.clone();
    oversized_skipped.force_manual_compression =
        Some("x".repeat(crate::session_merge::LIVE_BOOTSTRAP_SESSION_JSON_LIMIT));
    assert!(f
        .store
        .checkpoint_root_live_bootstrap(
            &f.owner,
            &oversized_skipped,
            f.inbox.clone(),
            AdmissionGate::default(),
            Fixture::publish(),
        )
        .await
        .is_err());
}

#[tokio::test]
async fn oversized_canonical_file_refuses_before_actor_decode() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let path = f.store.sessions_dir.join(ROOT).join("session.json");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.resize(
        crate::session_merge::LIVE_BOOTSTRAP_SESSION_JSON_LIMIT + 1,
        b' ',
    );
    std::fs::write(&path, bytes).unwrap();
    let error = match f.capture(AdmissionGate::default()).await {
        Ok(_) => panic!("oversized canonical Session unexpectedly accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn forged_opaque_value_and_cancel_while_waiting_for_locks_refuse() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let count = Arc::new(AtomicUsize::new(0));
    let forged = bamboo_domain::LiveBootstrapProof::from_storage(123_u64, AdmissionGate::default());
    assert!(f.confirm(&forged, count.clone()).await.is_err());

    let gate = AdmissionGate::default();
    let proof = f.capture(gate.clone()).await.unwrap();
    let (_, held) = f
        .store
        .root_owned_inbox_filesystem(&f.inbox, ROOT, &f.owner)
        .await
        .unwrap();
    let store = f.store.clone();
    let owner = f.owner.clone();
    let inbox = f.inbox.clone();
    let output = count.clone();
    let mut pending = tokio::spawn(async move {
        store
            .confirm_root_live_bootstrap(&owner, &proof, inbox, count_callback(output))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    gate.cancel_if_pending();
    drop(held);
    assert!(pending.await.unwrap().is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropped_confirmation_waiter_cancels_the_original_gate() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let gate = AdmissionGate::default();
    let proof = f.capture(gate.clone()).await.unwrap();
    let (_, held) = f
        .store
        .root_owned_inbox_filesystem(&f.inbox, ROOT, &f.owner)
        .await
        .unwrap();
    let store = f.store.clone();
    let owner = f.owner.clone();
    let inbox = f.inbox.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let output = count.clone();
    let mut pending = tokio::spawn(async move {
        store
            .confirm_root_live_bootstrap(&owner, &proof, inbox, count_callback(output))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    assert!(gate.is_cancelled());
    drop(held);
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn independent_inbox_adapter_cannot_deliver_through_held_physical_lock() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let second_store = Arc::new(SessionStoreV2::new(f._temp.path().into()).await.unwrap());
    let independent = Arc::new(crate::FileSessionInbox::new(
        second_store,
        SessionInboxLimits::default(),
    ));
    let (_, held) = f
        .store
        .root_owned_inbox_filesystem(&f.inbox, ROOT, &f.owner)
        .await
        .unwrap();
    let mut send = tokio::spawn(async move {
        let message = SessionMessageEnvelope::user_input(ROOT, "separate adapter");
        independent.deliver(&message).await
    });
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut send)
        .await
        .is_err());
    drop(held);
    tokio::time::timeout(Duration::from_secs(5), send)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn unbound_runtime_persister_cannot_checkpoint() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let unbound = crate::LockedSessionStore::new(f.store.clone());
    assert!(unbound
        .checkpoint_live_bootstrap(&f.staged, f.inbox.clone(), AdmissionGate::default())
        .await
        .is_err());
}

#[tokio::test]
async fn dropped_outer_lock_wait_cancels_proof_gate() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let proof = f.capture(AdmissionGate::default()).await.unwrap();
    let bound = Arc::new(
        crate::LockedSessionStore::new(f.store.clone())
            .bind_root_actor_writer(f.owner.clone(), Fixture::publish())
            .unwrap(),
    );
    let held = bound.acquire_lock(ROOT).await;
    let store = bound.clone();
    let pending_proof = proof.clone();
    let inbox = f.inbox.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let output = count.clone();
    let mut pending = tokio::spawn(async move {
        store
            .confirm_live_bootstrap(&pending_proof, inbox, count_callback(output))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    assert!(proof.is_cancelled());
    drop(held);
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bound_runtime_port_forwards_live_checkpoint_and_one_shot_confirmation() {
    let f = Fixture::new(chrono::Duration::seconds(30)).await;
    let bound = crate::LockedSessionStore::new(f.store.clone())
        .bind_root_actor_writer(f.owner.clone(), Fixture::publish())
        .unwrap();
    let port: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = Arc::new(bound);
    let proof = port
        .checkpoint_live_bootstrap(&f.staged, f.inbox.clone(), AdmissionGate::default())
        .await
        .expect("bound runtime trait object must forward the checkpoint");
    let count = Arc::new(AtomicUsize::new(0));
    port.confirm_live_bootstrap(&proof, f.inbox.clone(), count_callback(count.clone()))
        .await
        .expect("bound runtime trait object must forward confirmation");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(port
        .confirm_live_bootstrap(&proof, f.inbox.clone(), count_callback(count.clone()))
        .await
        .is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);

    let unbound: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
        Arc::new(crate::LockedSessionStore::new(f.store.clone()));
    assert!(unbound
        .checkpoint_live_bootstrap(&f.staged, f.inbox.clone(), AdmissionGate::default())
        .await
        .is_err());
}
