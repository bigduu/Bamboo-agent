//! Tests-only fragment for loop_control/tests/suspension_admission_tests.rs.
//! Parent adds only `mod suspension_admission_tests;`; the existing Harness
//! supplies real ownership, canonical Storage, parser and checkpoint behavior.
//! Compatible with the pre-fix source: no may_suspend/new production API use.

use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

struct UnknownFutureControlHook {
    point: bamboo_domain::AgentHookPoint,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl bamboo_agent_core::AgentHook for UnknownFutureControlHook {
    fn point(&self) -> bamboo_domain::AgentHookPoint {
        self.point
    }

    // Deliberately no may_suspend override. A host has made no non-suspending
    // declaration, so Start must conservatively reject before running it.
    async fn run(
        &self,
        _: bamboo_domain::AgentHookPoint,
        _: &bamboo_domain::HookPayload,
        _: &Session,
    ) -> bamboo_domain::HookResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        bamboo_domain::HookResult::Suspend {
            reason: "a future control seam may suspend".into(),
        }
    }
}

async fn assert_admission_has_no_generation(h: &Harness) {
    assert!(counted_loop::read_loop_state(&h.session).unwrap().is_none());
    assert!(h.policy().generation().is_none());
    assert!(h.policy().active_counted().is_none());
    let durable = h
        .storage
        .load_session(&h.session.id)
        .await
        .unwrap()
        .unwrap();
    assert!(counted_loop::read_loop_state(&durable).unwrap().is_none());
    assert!(!durable.messages.iter().any(|message| message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("runtime_kind"))
        .and_then(serde_json::Value::as_str)
        == Some("counted_loop_iteration")));
}

async fn assert_existing_wait_refuses_start(h: &mut Harness) {
    // Capture status before Start. Querying only afterwards would be vacuous:
    // even the defective source reports can_start=false after reserving a loop.
    let observed = h.command(&LoopControlRequest::Status, 0).await;
    assert!(observed.success);
    let result = h.command(&start("finite", 2, 8), 0).await;
    assert!(
        !result.success,
        "Start must refuse an incompatible wait: {}",
        result.result
    );
    assert!(value(&result)["error"]
        .as_str()
        .is_some_and(|reason| !reason.is_empty()));
    assert_eq!(value(&result)["applied"], false);
    assert_eq!(value(&observed)["can_start"], false);
    assert_admission_has_no_generation(h).await;
}

#[tokio::test]
async fn self_control_start_refuses_all_five_unknown_future_control_hook_points() {
    for point in [
        bamboo_domain::AgentHookPoint::BeforeRound,
        bamboo_domain::AgentHookPoint::AfterRound,
        bamboo_domain::AgentHookPoint::BeforeToolExecution,
        bamboo_domain::AgentHookPoint::AfterToolExecution,
        bamboo_domain::AgentHookPoint::BeforeFinalize,
    ] {
        let mut h = Harness::new().await;
        let hook = Arc::new(UnknownFutureControlHook {
            point,
            calls: AtomicUsize::new(0),
        });
        let mut runner = crate::runtime::HookRunner::new();
        runner.register(hook.clone());
        h.config.hook_runner = Arc::new(runner);
        assert_existing_wait_refuses_start(&mut h).await;
        assert_eq!(
            hook.calls.load(Ordering::SeqCst),
            0,
            "classification must not run an unknown handler at Start: {point:?}"
        );
    }
}

#[tokio::test]
async fn self_control_start_refuses_canonical_active_children_without_spawning_work() {
    for status in [None, Some("running"), Some("suspended")] {
        let mut h = Harness::new().await;
        let mut child = Session::new_child_of(
            "existing-active-child",
            &h.session,
            "synthetic-only",
            "worker",
        );
        if let Some(status) = status {
            child.set_last_run_status(status);
        }
        h.storage.save_session(&child).await.unwrap();
        let child_before = h.storage.load_session(&child.id).await.unwrap().unwrap();
        let indexed = h
            .storage
            .list_child_run_statuses(&h.session.id)
            .await
            .unwrap();
        assert!(
            indexed
                .iter()
                .any(|(id, observed)| id == &child.id && observed.as_deref() == status),
            "the real child index must expose the blocker, status={status:?}"
        );
        assert_existing_wait_refuses_start(&mut h).await;
        let unchanged = h.storage.load_session(&child.id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(unchanged).unwrap(),
            serde_json::to_value(child_before).unwrap()
        );
    }
}

struct NeverSpawnGuardian(AtomicUsize);

#[async_trait::async_trait]
impl crate::runtime::config::GuardianSpawner for NeverSpawnGuardian {
    async fn spawn_guardian_review(
        &self,
        _: &Session,
        _: String,
        _: String,
        _: Option<std::collections::BTreeSet<String>>,
    ) -> Result<String, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err("admission test must never launch a reviewer".into())
    }
}

#[tokio::test]
async fn self_control_start_refuses_pending_question_and_live_guardian_configuration() {
    let mut h = Harness::new().await;
    h.session.set_pending_question(
        "existing-question".into(),
        "user_question".into(),
        "Await the real user's answer".into(),
        vec!["continue".into(), "stop".into()],
        true,
    );
    h.storage.save_session(&h.session).await.unwrap();
    let pending = serde_json::to_value(h.session.pending_question.as_ref().unwrap()).unwrap();
    assert_existing_wait_refuses_start(&mut h).await;
    assert_eq!(
        serde_json::to_value(h.session.pending_question.as_ref().unwrap()).unwrap(),
        pending
    );
    let durable = h
        .storage
        .load_session(&h.session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(durable.pending_question.unwrap()).unwrap(),
        pending
    );

    let mut h = Harness::new().await;
    let guardian = Arc::new(NeverSpawnGuardian(AtomicUsize::new(0)));
    h.config.guardian_config = Some(crate::runtime::config::GuardianConfig {
        enabled: true,
        ..Default::default()
    });
    h.config.guardian_spawner = Some(guardian.clone());
    assert!(
        h.config.guardian_active(),
        "test a live gate, not an inert enabled flag"
    );
    assert_existing_wait_refuses_start(&mut h).await;
    assert_eq!(guardian.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn self_control_completed_child_does_not_disable_finite_start() {
    let mut h = Harness::new().await;
    let mut child = Session::new_child_of(
        "existing-completed-child",
        &h.session,
        "synthetic-only",
        "worker",
    );
    child.set_last_run_status("completed");
    h.storage.save_session(&child).await.unwrap();
    let indexed = h
        .storage
        .list_child_run_statuses(&h.session.id)
        .await
        .unwrap();
    assert!(indexed
        .iter()
        .any(|(id, status)| id == &child.id && status.as_deref() == Some("completed")));
    let status = h.command(&LoopControlRequest::Status, 0).await;
    assert!(status.success);
    assert_eq!(value(&status)["can_start"], true);
    let accepted = h.command(&start("finite", 2, 8), 0).await;
    assert!(
        accepted.success,
        "completed child is not a wait: {}",
        accepted.result
    );
    let receipt: LoopControlReceipt = serde_json::from_str(&accepted.result).unwrap();
    let state = h.state();
    assert_eq!(state.status, LoopStatus::Running);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (0, 0, 1)
    );
    assert_eq!(state.generation, receipt.generation);
    assert_eq!(state.control.as_ref().unwrap().operations.len(), 1);
    assert!(h.policy().active_counted().is_some());
    let durable = h
        .storage
        .load_session(&h.session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        counted_loop::read_loop_state(&durable).unwrap(),
        Some(state)
    );
}

struct ChildListCancellationBarrier {
    inner: Arc<dyn Storage>,
    entered: Notify,
    release: Notify,
    reads: AtomicUsize,
}

#[async_trait::async_trait]
impl Storage for ChildListCancellationBarrier {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.inner.save_session(session).await
    }
    async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_session(id).await
    }
    async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
        self.inner.delete_session(id).await
    }
    async fn list_child_run_statuses(
        &self,
        id: &str,
    ) -> std::io::Result<Vec<(String, Option<String>)>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        self.inner.list_child_run_statuses(id).await
    }
}

#[tokio::test]
async fn self_control_user_cancel_during_child_observation_precedes_start_side_effects() {
    let mut h = Harness::new().await;
    let durable_before = serde_json::to_value(
        h.storage
            .load_session(&h.session.id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let barrier = Arc::new(ChildListCancellationBarrier {
        inner: h.storage.clone(),
        entered: Notify::new(),
        release: Notify::new(),
        reads: AtomicUsize::new(0),
    });
    h.config.storage = Some(barrier.clone());
    let cancel = h.cancel.clone();
    let command = start("finite", 2, 8);
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let command = h.command_with_flags(&command, 0, false, false);
        tokio::pin!(command);
        tokio::select! {
            // Old source returns without inspecting children. This completes
            // immediately and the read assertion below provides the real RED.
            result = &mut command => result,
            _ = barrier.entered.notified() => {
                cancel.cancel();
                barrier.release.notify_one();
                command.await
            }
        }
    })
    .await
    .expect("one bounded admission read must finish after cancellation");
    assert_eq!(
        barrier.reads.load(Ordering::SeqCst),
        1,
        "Start must actually inspect existing children before accepting"
    );
    assert!(cancel.is_cancelled());
    assert!(matches!(result, Err(AgentError::Cancelled)), "{result:?}");
    assert_admission_has_no_generation(&h).await;
    assert!(
        !h.session
            .messages
            .iter()
            .any(|message| message.role == Role::Tool),
        "cancel before acceptance must not fabricate an operation receipt"
    );
    assert_eq!(
        serde_json::to_value(
            h.storage
                .load_session(&h.session.id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        durable_before,
        "no checkpoint or command effect may precede this cancellation"
    );
}
