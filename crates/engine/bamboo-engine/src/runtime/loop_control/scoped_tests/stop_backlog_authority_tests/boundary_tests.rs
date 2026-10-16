//! Deterministic ordering around the real scoped checkpoint, not fake I/O.
use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
enum GatePhase {
    BeforeCheckpoint,
    AfterCheckpoint,
}

struct GateStopCheckpoint {
    inner: Arc<dyn RuntimeSessionPersistence>,
    phase: GatePhase,
    armed: AtomicBool,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl RuntimeSessionPersistence for GateStopCheckpoint {
    fn root_actor_writer(&self) -> Option<bamboo_domain::RootActorRuntimeWrite> {
        self.inner.root_actor_writer()
    }

    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.checkpoint_runtime_session(session).await
    }

    async fn claim_scoped_loop_controls(
        &self,
        session: &Session,
        inbox: Arc<dyn SessionInboxPort>,
        active_run_id: &str,
        limit: usize,
    ) -> std::io::Result<Vec<bamboo_domain::ScopedLoopControlClaim>> {
        self.inner
            .claim_scoped_loop_controls(session, inbox, active_run_id, limit)
            .await
    }

    async fn checkpoint_scoped_loop_control(
        &self,
        session: &mut Session,
        bindings: &[ScopedLoopControlBinding],
        activation: Arc<dyn SessionActivationPort>,
        input: Option<(
            Arc<dyn SessionInboxPort>,
            bamboo_domain::ScopedLoopControlClaim,
        )>,
    ) -> std::io::Result<()> {
        let stop = input.as_ref().is_some_and(|(_, claim)| {
            bamboo_domain::scoped_loop_control_input(claim.envelope())
                .ok()
                .flatten()
                .is_some_and(|input| matches!(input.command, LoopControlRequest::Stop { .. }))
        });
        let gated = stop && self.armed.swap(false, Ordering::SeqCst);
        if gated && matches!(self.phase, GatePhase::BeforeCheckpoint) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let result = self
            .inner
            .checkpoint_scoped_loop_control(session, bindings, activation, input)
            .await;
        if gated && result.is_ok() && matches!(self.phase, GatePhase::AfterCheckpoint) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        result
    }

    async fn load_loop_control_checkpoint(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_loop_control_checkpoint(id).await
    }
}

fn install_gate(h: &mut Fixture, phase: GatePhase) -> Arc<GateStopCheckpoint> {
    let gate = Arc::new(GateStopCheckpoint {
        inner: h.config.persistence.take().unwrap(),
        phase,
        armed: AtomicBool::new(true),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    });
    h.config.persistence = Some(gate.clone());
    gate
}

#[tokio::test]
async fn scoped_native_root_stop_closes_new_carriers_on_both_sides_of_real_checkpoint() {
    for phase in [GatePhase::BeforeCheckpoint, GatePhase::AfterCheckpoint] {
        let (mut h, mut binding) = native_fixture().await;
        h.start_and_admit().await;
        let stop = stop(&h);
        let queued_stop = h.send(&stop).await.unwrap();
        let tail = h.update("Only the producer before real Stop may enqueue");
        let tail_operation = operation_id(&tail).unwrap().to_owned();
        let gate = install_gate(&mut h, phase);
        let messenger = h.messenger();
        let source = h.supervisor.clone();
        let link = h.link_revision;
        let control = gate.clone();
        let producer = async move {
            control.entered.notified().await;
            // This is a real authority+Maildir delivery. The gate never
            // fabricates its result or holds the backend locks for the test.
            let result = messenger
                .supervisor_loop_control(&source, TARGET, Some(link), &tail, None)
                .await;
            control.release.notify_one();
            result
        };
        let (drained, delivered) =
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::join!(h.drain(1), producer)
            })
            .await
            .expect("real Stop and producer must cross their deterministic checkpoint gate");
        assert!(
            !gate.armed.load(Ordering::SeqCst),
            "Stop gate must really fire"
        );
        drained.unwrap();
        match phase {
            GatePhase::BeforeCheckpoint => {
                assert_eq!(delivered.unwrap()["configured"], false);
            }
            GatePhase::AfterCheckpoint => {
                assert!(
                    delivered.is_err(),
                    "a producer after real Stop must not enqueue: {delivered:?}"
                );
            }
        }
        assert!(!h.admit().await);
        let durable = h.durable().await;
        assert_eq!(h.state().status, LoopStatus::Stopped);
        let cursor = durable.session_inbox_admission().unwrap();
        assert_eq!(
            cursor.last_admitted_sequence,
            queued_stop["generation"].as_u64().unwrap()
        );
        assert_eq!(cursor.last_observed_sequence, cursor.last_admitted_sequence);
        assert!(consumption(&durable, &stop).configured);
        assert_eq!(scoped_loop_consumptions(&durable).unwrap().len(), 2);
        let backlog = h.inbox.inspect(TARGET).await.unwrap();
        assert_eq!((backlog.pending, backlog.claimed), (0, 0));
        let status = h.status(Some(&tail_operation)).await.unwrap();
        assert_eq!(status["configured"], false);
        assert!(status["operation_receipt"].is_null());
        close_native(&mut h, &mut binding).await;
    }
}

#[tokio::test]
async fn scoped_native_root_revoke_before_stop_checkpoint_keeps_original_authority_failure() {
    let (mut h, mut binding) = native_fixture().await;
    let start = h.start_and_admit().await;
    let start_proof = consumption(&h.durable().await, &start);
    let command = stop(&h);
    let queued_stop = h.send(&command).await.unwrap();
    let tail = h.update("Revoked source cannot retire or apply this transport");
    let queued_tail = h.send(&tail).await.unwrap();
    let stop_id =
        bamboo_domain::SessionMessageId::parse(queued_stop["receipt_id"].as_str().unwrap())
            .unwrap();
    let tail_id =
        bamboo_domain::SessionMessageId::parse(queued_tail["receipt_id"].as_str().unwrap())
            .unwrap();
    let gate = install_gate(&mut h, GatePhase::BeforeCheckpoint);
    let router = h.router.clone();
    let grant_id = h.policy().supervisor_grant_id().unwrap();
    let control = gate.clone();
    let revoke = async move {
        control.entered.notified().await;
        let revoked = router
            .revoke_supervisor_loop_control(TARGET, &grant_id)
            .await;
        control.release.notify_one();
        revoked
    };
    let (result, revoked) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(h.drain(1), revoke)
    })
    .await
    .expect("revoke must cross the real post-claim/pre-checkpoint boundary");
    assert!(revoked);
    assert!(!gate.armed.load(Ordering::SeqCst));
    assert!(
        result.is_err(),
        "retirement must not grant rights after explicit revoke"
    );
    let durable = h.durable().await;
    assert_eq!(scoped_loop_consumptions(&durable).unwrap().len(), 1);
    assert_eq!(consumption(&durable, &start), start_proof);
    let cursor = durable.session_inbox_admission().unwrap();
    assert!(!cursor.contains(&stop_id));
    assert!(!cursor.contains(&tail_id));
    assert!(!h.inbox.was_admitted(TARGET, &stop_id).await.unwrap());
    assert!(!h.inbox.was_admitted(TARGET, &tail_id).await.unwrap());
    let backlog = h.inbox.inspect(TARGET).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (1, 1));
    assert!(
        !h.cancel.is_cancelled(),
        "revocation must not invent user cancellation"
    );
    close_native(&mut h, &mut binding).await;
}

mod selector_guard_tests;
