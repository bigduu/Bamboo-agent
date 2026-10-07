//! Named parent-outcome writer boundaries over real V2 and repository bindings.
//! These exercise the existing CAS port, not the server's ParentRequest decision
//! semantics or production provider claim, which have separate Host fixtures.

use std::{io, sync::Arc, time::Duration};

use bamboo_agent_core::tools::context::{root_actor_tool_writer, with_root_actor_tool_writer};
use bamboo_domain::{
    ActorActivation, ActorActivationFinish, ActorActivationStatus, ActorDirectoryPort, Message,
    Role, RootActorExecutionBinding, RuntimeSessionPersistence, Session, SessionActivationPolicy,
    SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxLimits, SessionInboxPort,
    SessionMessageEnvelope, Storage,
};
use bamboo_engine::{read_cached_session, SessionRepository, SessionSnapshot};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use chrono::Utc;
use tokio::sync::oneshot;

const ROOT: &str = "named-parent-outcome-root";
const WAIT: Duration = Duration::from_secs(10);

struct Fixture {
    a: Arc<SessionStoreV2>,
    b: Arc<SessionStoreV2>,
    host_a: SessionRepository,
    host_b: SessionRepository,
    root: Session,
    _temp: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let a = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let mut root = Session::new(ROOT, "controlled-parent-model");
        root.add_message(Message::user("original parent transcript"));
        a.save_session(&root).await.unwrap();
        // Initialize the second observer after the canonical Root is indexed.
        let b = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let host_a = repository(a.clone()).with_root_actor_directory(a.clone());
        let host_b = repository(b.clone()).with_root_actor_directory(b.clone());
        Self {
            a,
            b,
            host_a,
            host_b,
            root,
            _temp: temp,
        }
    }

    async fn bind(&self, host: &SessionRepository, run: &str) -> RootActorExecutionBinding {
        tokio::time::timeout(WAIT, host.bind_root_actor_execution(&self.root, run))
            .await
            .expect("actual repository claim/start completes")
            .unwrap()
            .expect("persisted ordinary Root receives its execution capability")
    }

    async fn current(&self) -> ActorActivation {
        self.a
            .inspect_actor(ROOT)
            .await
            .unwrap()
            .activation
            .unwrap()
    }

    fn main_bytes(&self) -> Vec<u8> {
        std::fs::read(
            self._temp
                .path()
                .join("sessions")
                .join(ROOT)
                .join("session.json"),
        )
        .unwrap()
    }

    async fn assert_marker(&self, host: &SessionRepository, marker: &str) {
        let canonical = self.a.load_session(ROOT).await.unwrap().unwrap();
        let cached = read_cached_session(host.cache(), ROOT).expect("actual Host cache published");
        assert_eq!(count(&canonical, marker), 1);
        assert_eq!(count(&cached, marker), 1);
        assert_eq!(
            serde_json::to_value(&canonical.messages).unwrap(),
            serde_json::to_value(&cached.messages).unwrap()
        );
    }
}

fn repository(store: Arc<SessionStoreV2>) -> SessionRepository {
    SessionRepository::new(
        Arc::default(),
        store.clone(),
        Arc::new(LockedSessionStore::new(store)),
    )
}

fn count(session: &Session, marker: &str) -> usize {
    session
        .messages
        .iter()
        .filter(|message| message.content == marker)
        .count()
}

fn cache_value(host: &SessionRepository) -> serde_json::Value {
    serde_json::to_value(read_cached_session(host.cache(), ROOT)).unwrap()
}

async fn commit(host: &SessionRepository, marker: &str) -> io::Result<()> {
    let marker = marker.to_owned();
    let cache = host.cache().clone();
    let result = host
        .persistence()
        .mutate_runtime_session_and_publish(
            ROOT,
            || None,
            move |session| {
                session.add_message(Message::assistant(marker, None));
                Ok::<_, ()>(())
            },
            move |saved| {
                cache.insert(
                    saved.id.clone(),
                    Arc::new(SessionSnapshot::new(saved.clone())),
                );
            },
        )
        .await?;
    match result {
        Ok(Some(_)) => Ok(()),
        _ => Err(io::Error::other(
            "parent-outcome CAS did not confirm a snapshot",
        )),
    }
}

async fn finish(mut binding: RootActorExecutionBinding) {
    binding
        .directory
        .finish_activation(
            &binding.owner.fence,
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    binding.disarm_abandonment();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_parent_outcome_borrows_actual_root_capability_without_finishing_provider_owner() {
    let f = Fixture::new().await;
    let binding = f.bind(&f.host_a, "provider-a").await;
    let before = f.current().await;
    assert_eq!(before.status, ActorActivationStatus::Running);
    assert!(root_actor_tool_writer().is_none());

    let writer = f.host_a.bind_parent_outcome_writer(ROOT).await.unwrap();
    let borrowed = writer.repository().root_actor_writer().unwrap();
    assert_eq!(borrowed.fence, binding.owner.fence);
    assert_eq!(borrowed.created_at, binding.owner.created_at);
    commit(writer.repository(), "HOST_PARENT_CURRENT")
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let after = f.current().await;
    assert_eq!(after.fence(), before.fence());
    assert_eq!(after.status, ActorActivationStatus::Running);
    assert_eq!(after.lease_expires_at, before.lease_expires_at);
    assert_eq!(after.finished_at, None);
    f.assert_marker(&f.host_a, "HOST_PARENT_CURRENT").await;
    finish(binding).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn surviving_old_runtime_scope_cannot_borrow_successor_or_publish_main_and_cache() {
    let f = Fixture::new().await;
    let binding_a = f.bind(&f.host_a, "provider-a").await;
    let owner_a = binding_a.persistence.root_actor_writer().unwrap();
    assert_eq!(owner_a.fence, binding_a.owner.fence);
    let route = f.host_a.downgrade_parent_outcome_route();
    // Separate V2, LockedSessionStore and cache, but the same Host routing
    // registry: B's real bind will replace the registry's pointer to A.
    let host_b = route.rebind(f.host_b.clone()).unwrap();
    let host_a = f.host_a.clone();
    let old_candidate = f.root.clone();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let old = tokio::spawn(with_root_actor_tool_writer(
        Some(owner_a.clone()),
        async move {
            let captured = host_a.bind_parent_outcome_writer(ROOT).await.unwrap();
            commit(captured.repository(), "A_BEFORE_RECLAIM")
                .await
                .unwrap();
            let private_before = cache_value(captured.repository());
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            let origin = root_actor_tool_writer().unwrap().unwrap();
            assert_eq!(origin.fence, binding_a.owner.fence);

            // Lookup may fail early or return A's immutable capability. It must
            // never turn this old runtime into a Host job or return B's capability.
            if let Ok(fresh) = host_a.bind_parent_outcome_writer(ROOT).await {
                assert_eq!(
                    fresh.repository().root_actor_writer().unwrap().fence,
                    origin.fence
                );
                let private_fresh = cache_value(fresh.repository());
                assert!(commit(fresh.repository(), "STALE_FRESH_LOOKUP")
                    .await
                    .is_err());
                assert_eq!(cache_value(fresh.repository()), private_fresh);
                fresh.finish().await.unwrap();
            }
            // A writer captured while A was valid must reach the actual final
            // persistence boundary and be rejected there after B has committed.
            assert!(commit(captured.repository(), "STALE_CAPTURED_PARENT")
                .await
                .is_err());
            assert_eq!(cache_value(captured.repository()), private_before);
            let mut old_candidate = old_candidate;
            old_candidate.add_message(Message::assistant("STALE_RUNTIME_CHECKPOINT", None));
            assert!(binding_a
                .persistence
                .save_runtime_session(&mut old_candidate)
                .await
                .is_err());
            captured.finish().await.unwrap(); // borrowed finish cannot finish B
        },
    ));
    tokio::time::timeout(WAIT, ready_rx).await.unwrap().unwrap();

    // The task still owns binding A at the barrier. Observe its actual expiry
    // through the directory; no old owner has been killed or completed.
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let current = f.current().await;
            assert_eq!(current.fence(), owner_a.fence);
            if current.lease_expires_at <= Utc::now() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("A's real lease expires while its task survives");
    assert!(!old.is_finished());
    let binding_b = f.bind(&host_b, "provider-b").await;
    assert!(binding_b.owner.fence.attempt > owner_a.fence.attempt);
    assert!(binding_b.owner.fence.lease_epoch > owner_a.fence.lease_epoch);
    let writer_b = host_b.bind_parent_outcome_writer(ROOT).await.unwrap();
    assert_eq!(
        writer_b.repository().root_actor_writer().unwrap().fence,
        binding_b.owner.fence
    );
    commit(writer_b.repository(), "B_CURRENT_PARENT")
        .await
        .unwrap();
    writer_b.finish().await.unwrap();
    f.assert_marker(&host_b, "B_CURRENT_PARENT").await;
    let main_before = f.main_bytes();
    let cache_a_before = cache_value(&f.host_a);
    let cache_b_before = cache_value(&host_b);

    release_tx.send(()).unwrap();
    tokio::time::timeout(WAIT, old).await.unwrap().unwrap();
    assert_eq!(f.main_bytes(), main_before);
    assert_eq!(cache_value(&f.host_a), cache_a_before);
    assert_eq!(cache_value(&host_b), cache_b_before);
    let current = f.current().await;
    assert_eq!(current.fence(), binding_b.owner.fence);
    assert_eq!(current.status, ActorActivationStatus::Running);
    finish(binding_b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_parent_control_commits_and_finishes_while_unbound_runtime_cannot_mint() {
    let f = Fixture::new().await;
    let binding = f.bind(&f.host_a, "initial-provider").await;
    let original_fence = binding.owner.fence.clone();
    finish(binding).await;
    let before = f.current().await;
    let bytes = f.main_bytes();
    let cache = cache_value(&f.host_a);
    with_root_actor_tool_writer(None, async {
        assert!(matches!(root_actor_tool_writer(), Some(None)));
        assert!(f.host_a.bind_parent_outcome_writer(ROOT).await.is_err());
    })
    .await;
    assert_eq!(f.current().await.fence(), before.fence());
    assert_eq!(f.main_bytes(), bytes);
    assert_eq!(cache_value(&f.host_a), cache);

    let writer = f.host_a.bind_parent_outcome_writer(ROOT).await.unwrap();
    let owner = writer.repository().root_actor_writer().unwrap();
    assert!(owner.fence.attempt > original_fence.attempt);
    assert_eq!(f.current().await.fence(), owner.fence);
    assert_eq!(f.current().await.status, ActorActivationStatus::Running);
    commit(writer.repository(), "IDLE_PARENT_TERMINAL")
        .await
        .unwrap();
    writer.finish().await.unwrap();
    f.assert_marker(&f.host_a, "IDLE_PARENT_TERMINAL").await;
    let terminal = f.current().await;
    assert_eq!(terminal.fence(), owner.fence);
    assert_eq!(terminal.status, ActorActivationStatus::Succeeded);
    assert!(terminal.finished_at.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn weak_host_route_rebinds_activated_parent_deadline_cas_without_keeping_host_alive() {
    let f = Fixture::new().await;
    let binding = f.bind(&f.host_a, "initial-provider").await;
    let route = f.host_a.downgrade_parent_outcome_route();
    finish(binding).await;

    // This mirrors the deadline job's fresh repository: no inherited owner,
    // directory or writer registry until the original weak route is rebound.
    let fresh = repository(f.b.clone());
    assert!(!fresh.root_actor_execution_required(&f.root));
    let rebound = route
        .rebind(fresh)
        .expect("live Host route restores authority routing");
    assert!(rebound.root_actor_execution_required(&f.root));
    let writer = rebound.bind_parent_outcome_writer(ROOT).await.unwrap();
    let control = writer.repository().root_actor_writer().unwrap();
    // Decision semantics remain the existing caller's CAS; this fixture only
    // proves that its full Main/cache publication uses the named Root writer.
    commit(writer.repository(), "DEADLINE_DENY_TERMINAL")
        .await
        .unwrap();
    writer.finish().await.unwrap();
    f.assert_marker(&rebound, "DEADLINE_DENY_TERMINAL").await;
    assert_eq!(f.current().await.fence(), control.fence);
    assert_eq!(f.current().await.status, ActorActivationStatus::Succeeded);

    // Drop both borrowed/control bindings and every route-bearing repository.
    // The fixture's store Arc may remain; it must not keep the Host registry.
    drop(rebound);
    drop(f.host_a);
    assert!(route.rebind(repository(f.b.clone())).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_input_ack_recovery_preserves_a_later_owned_prompt_checkpoint() {
    let f = Fixture::new().await;
    let binding = f.bind(&f.host_a, "prompt-recovery").await;
    let raw = Arc::new(bamboo_storage::FileSessionInbox::new(
        f.a.clone(),
        SessionInboxLimits::default(),
    ));
    let envelope = SessionMessageEnvelope::user_input(ROOT, "prompt turn")
        .with_root_chat_prompt("PROMPT_A".into())
        .unwrap();
    raw.deliver_with_activation_intent(
        &envelope,
        SessionActivationPolicy::InterruptSpecificWait,
        None,
    )
    .await
    .unwrap();
    let inbox =
        f.a.bind_root_actor_inbox(&binding.owner, raw.clone())
            .unwrap();
    let claim = inbox
        .claim_owned(
            ROOT,
            1,
            Some("prompt-recovery"),
            &SessionInboxLeaseRequest {
                consumer: SessionInboxConsumerId::new(),
                now: Utc::now(),
                duration: chrono::Duration::seconds(2),
            },
        )
        .await
        .unwrap()
        .remove(0);

    // A full actual Root checkpoint reached Main before its exact input ACK.
    let mut committed = f.a.load_session(ROOT).await.unwrap().unwrap();
    committed.messages.insert(0, Message::system("PROMPT_A"));
    committed.add_message(envelope.to_provider_message().unwrap());
    committed
        .session_inbox_admission_mut()
        .record(envelope.id.clone(), claim.claim.generation);
    f.a.save_root_actor_runtime(&binding.owner, &committed, false, Arc::new(|_| {}))
        .await
        .unwrap();
    assert!(!raw.was_admitted(ROOT, &envelope.id).await.unwrap());

    // Another owned context update is committed before the lost ACK recovers.
    committed
        .messages
        .retain(|message| message.role != Role::System);
    committed.messages.insert(0, Message::system("PROMPT_B"));
    f.a.save_root_actor_runtime(&binding.owner, &committed, false, Arc::new(|_| {}))
        .await
        .unwrap();
    while Utc::now() < claim.lease.expires_at {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut recovered = f.a.load_session(ROOT).await.unwrap().unwrap();
    let admission = binding
        .persistence
        .admit_root_inbox(&mut recovered, raw.clone(), Some("prompt-recovery"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.merged, 0);
    assert!(admission.admission_error.is_none(), "{admission:?}");
    assert!(raw.was_admitted(ROOT, &envelope.id).await.unwrap());
    let canonical = f.a.load_session(ROOT).await.unwrap().unwrap();
    let cached = read_cached_session(f.host_a.cache(), ROOT).unwrap();
    for session in [&recovered, &canonical, &cached] {
        assert_eq!(count(session, "PROMPT_A"), 0);
        assert_eq!(count(session, "PROMPT_B"), 1);
        assert_eq!(count(session, "prompt turn"), 1);
        assert_eq!(session.messages[0].content, "PROMPT_B");
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.role == Role::System)
                .count(),
            1
        );
    }
    finish(binding).await;
}
