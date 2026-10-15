use super::*;
use async_trait::async_trait;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_domain::{Role, RuntimeSessionPersistence};
use std::sync::atomic::{AtomicBool, Ordering};

struct Harness {
    _dir: tempfile::TempDir,
    registration: crate::session_activation::SessionRunRegistration,
    router: Arc<crate::session_activation::SessionActivationRouter>,
    session: Session,
    config: AgentLoopConfig,
    storage: Arc<dyn Storage>,
    cancel: CancellationToken,
    next_call: usize,
}

impl Harness {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let session = Session::new("self-control-service", "synthetic-only");
        let storage: Arc<dyn Storage> = Arc::new(
            bamboo_storage::SessionStoreV2::new(dir.path().into())
                .await
                .unwrap(),
        );
        storage.save_session(&session).await.unwrap();
        let router = crate::session_activation::SessionActivationRouter::new();
        let registration = router
            .register_run(&session.id, "registered-owner")
            .await
            .unwrap();
        let mut policy = LoopControlConfig::new(10, 100);
        policy.bind_owner(registration.ownership(), &session);
        let cancel = CancellationToken::new();
        policy
            .bind_execution("actual-execution", cancel.clone())
            .unwrap();
        let mut config = AgentLoopConfig {
            loop_control: Some(policy),
            storage: Some(storage.clone()),
            persistence: Some(Arc::new(bamboo_storage::LockedSessionStore::new(
                storage.clone(),
            ))),
            ..Default::default()
        };
        config.run_budget.max_rounds = Some(100);
        Self {
            _dir: dir,
            registration,
            router,
            session,
            config,
            storage,
            cancel,
            next_call: 0,
        }
    }

    fn policy(&self) -> &LoopControlConfig {
        self.config.loop_control.as_ref().unwrap()
    }

    fn state(&self) -> LoopState {
        counted_loop::read_loop_state(&self.session)
            .unwrap()
            .unwrap()
    }

    async fn command(&mut self, command: &LoopControlRequest, round: usize) -> ToolResult {
        self.command_with_flags(command, round, false, false)
            .await
            .unwrap()
    }

    async fn command_with_flags(
        &mut self,
        command: &LoopControlRequest,
        round: usize,
        replay: bool,
        plan: bool,
    ) -> Result<ToolResult, AgentError> {
        // Use the public parser even in service tests; never feed an invalid
        // constructed DTO past the actual tool's static validation boundary.
        let json = serde_json::to_string(command).unwrap();
        let command = bamboo_tools::tools::loop_control::parse_loop_control_request(&json).unwrap();
        self.next_call += 1;
        let call_id = format!("self-control-{}", self.next_call);
        self.session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: call_id.clone(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "loop_control".into(),
                    arguments: json,
                },
            }]),
        ));
        apply_current(
            &mut self.session,
            &self.config,
            &command,
            &call_id,
            round,
            replay,
            plan,
        )
        .await
    }

    async fn admit(&mut self) {
        assert!(
            counted_loop::before_round(&mut self.session, &self.config, "actual-execution")
                .await
                .unwrap()
        );
    }

    fn update(
        &self,
        objective: Option<String>,
        iterations: Option<u32>,
        rounds: Option<u32>,
    ) -> LoopControlRequest {
        let state = self.state();
        LoopControlRequest::Update {
            operation_id: uuid::Uuid::new_v4().to_string(),
            generation: state.generation,
            expected_revision: state.control.unwrap().control_revision,
            objective,
            max_iterations: iterations,
            max_total_rounds: rounds,
        }
    }

    fn stop(&self) -> LoopControlRequest {
        let state = self.state();
        LoopControlRequest::Stop {
            operation_id: uuid::Uuid::new_v4().to_string(),
            generation: state.generation,
            expected_revision: state.control.unwrap().control_revision,
        }
    }
}

fn start(objective: &str, iterations: u32, rounds: u32) -> LoopControlRequest {
    LoopControlRequest::Start {
        operation_id: uuid::Uuid::new_v4().to_string(),
        expected_generation: None,
        expected_revision: 0,
        objective: objective.into(),
        max_iterations: iterations,
        max_total_rounds: rounds,
    }
}

fn value(result: &ToolResult) -> serde_json::Value {
    serde_json::from_str(&result.result).unwrap()
}

fn denied(result: &ToolResult, code: &str) {
    assert!(!result.success, "unexpected success: {}", result.result);
    assert_eq!(value(result)["error"], code);
    assert_eq!(value(result)["applied"], false);
}

#[tokio::test]
async fn self_control_starts_only_on_command_and_charges_all_previous_rounds() {
    let mut h = Harness::new().await;
    assert!(h.policy().active_counted().is_none());
    assert!(counted_loop::read_loop_state(&h.session).unwrap().is_none());
    let status = h.command(&LoopControlRequest::Status, 4).await;
    assert_eq!(value(&status)["activation_rounds_spent"], 5);
    assert_eq!(value(&status)["host_remaining_rounds"], 95);
    let result = h.command(&start("New objective", 2, 10), 4).await;
    assert!(result.success);
    assert_eq!(value(&result)["applied"], false);
    assert_eq!(value(&result)["effective_iteration"], 1);
    let state = h.state();
    assert_eq!((state.iterations_started, state.rounds_started), (0, 5));
    assert_eq!(state.owner_execution_id, "actual-execution");
    assert!(!h.session.messages.iter().any(|m| m.role == Role::User));
    h.admit().await;
    let state = h.state();
    assert_eq!((state.iterations_started, state.rounds_started), (1, 6));
    assert_eq!(state.control.unwrap().applied_revision, 1);
    let durable = h
        .storage
        .load_session(&h.session.id)
        .await
        .unwrap()
        .unwrap();
    assert!(durable
        .messages
        .iter()
        .any(|m| m.role == Role::User && m.content == "New objective"));
}

#[tokio::test]
async fn self_control_duplicate_precedes_cas_but_payload_changes_conflict() {
    let mut h = Harness::new().await;
    let command = start("Keep context", 3, 20);
    let first = h.command(&command, 0).await;
    h.admit().await;
    let state = h.state();
    let duplicate = h.command(&command, 1).await;
    assert_eq!(
        first.result, duplicate.result,
        "original accepted receipt is stable"
    );
    assert_eq!(
        h.state(),
        state,
        "duplicate cannot reset counters or revisions"
    );
    let mut changed = command.clone();
    if let LoopControlRequest::Start { objective, .. } = &mut changed {
        *objective = "Changed".into();
    }
    denied(&h.command(&changed, 1).await, "operation_payload_conflict");
    assert_eq!(h.state(), state);
}

#[tokio::test]
async fn self_control_cas_ignores_checkpoint_revision_and_applies_goal_only_next_iteration() {
    let mut h = Harness::new().await;
    h.command(&start("A", 3, 20), 0).await;
    h.admit().await;
    let status = value(&h.command(&LoopControlRequest::Status, 1).await);
    let revision = status["loop"]["control_revision"].as_u64().unwrap();
    let earlier_state_revision = status["loop"]["state_revision"].as_u64().unwrap();
    h.admit().await;
    assert!(h.state().revision > earlier_state_revision);
    let command = LoopControlRequest::Update {
        operation_id: uuid::Uuid::new_v4().to_string(),
        generation: h.state().generation,
        expected_revision: revision,
        objective: Some("B".into()),
        max_iterations: None,
        max_total_rounds: None,
    };
    let result = h.command(&command, 2).await;
    assert!(result.success, "{}", result.result);
    assert_eq!(value(&result)["accepted_revision"], 2);
    assert_eq!(value(&result)["applied_revision_at_acceptance"], 1);
    assert_eq!(value(&result)["effective_iteration"], 2);
    let state = h.state();
    assert_eq!(state.objective, "A");
    assert_eq!(state.objective_revision, 1);
    assert_eq!(
        state.control.as_ref().unwrap().iteration_objective_revision,
        1
    );
    let pending = h.update(Some("C".into()), None, None);
    denied(&h.command(&pending, 2).await, "objective_update_pending");
    let stale = LoopControlRequest::Stop {
        operation_id: uuid::Uuid::new_v4().to_string(),
        generation: state.generation,
        expected_revision: revision,
    };
    denied(&h.command(&stale, 2).await, "stale_generation_or_revision");
    assert!(matches!(
        counted_loop::terminal(&mut h.session, &h.config)
            .await
            .unwrap(),
        counted_loop::TerminalDecision::Continue
    ));
    assert_eq!(h.state().iterations_completed, 1);
    assert_eq!(h.state().objective, "A");
    h.admit().await;
    let state = h.state();
    assert_eq!(
        (state.objective.as_str(), state.objective_revision),
        ("B", 2)
    );
    assert_eq!(
        (state.iterations_started, state.iterations_completed),
        (2, 1)
    );
    let control = state.control.unwrap();
    assert_eq!(
        (
            control.control_revision,
            control.applied_revision,
            control.iteration_objective_revision
        ),
        (2, 2, 2)
    );
    assert!(control.pending_objective.is_none());
}

#[tokio::test]
async fn self_control_bounds_never_grow_reset_or_go_below_consumption() {
    let mut h = Harness::new().await;
    denied(
        &h.command(&start("A", 2, 101), 0).await,
        "host_or_remaining_budget_exceeded",
    );
    assert!(h.policy().generation().is_none());
    denied(
        &h.command(&start("A", 2, 6), 4).await,
        "host_or_remaining_budget_exceeded",
    );
    assert!(h.policy().generation().is_none());
    h.command(&start("A", 3, 20), 4).await;
    h.admit().await;
    for (iterations, rounds) in [(Some(4), None), (None, Some(21)), (None, Some(5))] {
        let before = h.state();
        let command = h.update(None, iterations, rounds);
        denied(
            &h.command(&command, 5).await,
            "bounds_may_only_tighten_without_reset",
        );
        assert_eq!(h.state(), before);
    }
    let command = h.update(None, Some(2), Some(6));
    assert!(h.command(&command, 5).await.success);
    let state = h.state();
    assert_eq!(
        (
            state.max_iterations,
            state.max_total_rounds,
            state.rounds_started
        ),
        (2, 6, 6)
    );
    assert!(
        !counted_loop::before_round(&mut h.session, &h.config, "actual-execution")
            .await
            .unwrap()
    );
    assert_eq!(h.state().status, LoopStatus::BudgetExhausted);
}

#[tokio::test]
async fn self_control_stop_dominates_both_update_orders_and_forbids_restart() {
    for update_first in [false, true] {
        let mut h = Harness::new().await;
        h.command(&start("A", 3, 20), 0).await;
        h.admit().await;
        if update_first {
            let command = h.update(Some("B".into()), None, None);
            assert!(h.command(&command, 1).await.success);
        }
        let stop = h.stop();
        let result = h.command(&stop, 1).await;
        assert!(result.success);
        assert_eq!(value(&result)["applied"], true);
        assert_eq!(h.command(&stop, 1).await.result, result.result);
        let stopped = h.state();
        assert!(stopped.control.as_ref().unwrap().stop_requested);
        assert!(stopped
            .control
            .as_ref()
            .unwrap()
            .pending_objective
            .is_none());
        let update = h.update(Some("C".into()), None, None);
        denied(&h.command(&update, 1).await, "stop_is_terminal");
        assert_eq!(h.state(), stopped);
        assert!(
            !counted_loop::before_round(&mut h.session, &h.config, "actual-execution")
                .await
                .unwrap()
        );
        let mut restart = start("D", 1, 10);
        if let LoopControlRequest::Start {
            expected_generation,
            expected_revision,
            ..
        } = &mut restart
        {
            *expected_generation = Some(h.state().generation);
            *expected_revision = h.state().control.unwrap().control_revision;
        }
        denied(
            &h.command(&restart, 1).await,
            "generation_already_started_this_activation",
        );
        assert_eq!(h.state().status, LoopStatus::Stopped);
    }
}

#[tokio::test]
async fn self_control_escaped_objective_update_rejects_before_corrupting_state() {
    let mut h = Harness::new().await;
    let escaped = "\0".repeat(MAX_LOOP_PROMPT_BYTES);
    assert!(h.command(&start(&escaped, 3, 20), 0).await.success);
    h.admit().await;
    let before = h.state();
    let other = format!("{}x", "\0".repeat(MAX_LOOP_PROMPT_BYTES - 1));
    let command = h.update(Some(other), None, None);
    denied(
        &h.command(&command, 1).await,
        "state_checkpoint_bound_exceeded",
    );
    assert_eq!(
        h.state(),
        before,
        "failed edit must leave a readable active state"
    );
    let durable = h
        .storage
        .load_session(&h.session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        counted_loop::read_loop_state(&durable).unwrap().unwrap(),
        before
    );
}

#[tokio::test]
async fn self_control_receipts_are_bounded_and_duplicates_survive_the_limit() {
    let mut h = Harness::new().await;
    let command = start(&"\0".repeat(MAX_LOOP_PROMPT_BYTES), 3, 20);
    let first = h.command(&command, 0).await;
    h.admit().await;
    for _ in 1..MAX_OPERATIONS {
        let command = h.update(None, Some(3), None);
        assert!(h.command(&command, 1).await.success);
    }
    let state = h.state();
    assert_eq!(
        state.control.as_ref().unwrap().operations.len(),
        MAX_OPERATIONS
    );
    assert_eq!(h.command(&command, 1).await.result, first.result);
    let stop = h.stop();
    denied(&h.command(&stop, 1).await, "operation_limit_exhausted");
    assert_eq!(h.state(), state);
    h.policy().stop();
    assert!(
        !counted_loop::before_round(&mut h.session, &h.config, "actual-execution")
            .await
            .unwrap()
    );
    assert_eq!(
        h.state().status,
        LoopStatus::Stopped,
        "host revocation cannot be exhausted by receipt quota"
    );
}

#[tokio::test]
async fn self_control_plan_and_approved_replay_cannot_mutate_but_service_status_is_readonly() {
    let mut h = Harness::new().await;
    let command = start("A", 1, 3);
    denied(
        &h.command_with_flags(&command, 0, false, true)
            .await
            .unwrap(),
        "plan_read_only",
    );
    denied(
        &h.command_with_flags(&command, 0, true, false)
            .await
            .unwrap(),
        "fresh_owner_command_required",
    );
    assert!(h.policy().generation().is_none());
    assert!(
        h.command_with_flags(&LoopControlRequest::Status, 0, true, true)
            .await
            .unwrap()
            .success
    );
    assert!(counted_loop::read_loop_state(&h.session).unwrap().is_none());
}

#[tokio::test]
async fn self_control_cancel_and_host_revocation_remain_sticky() {
    let mut h = Harness::new().await;
    h.policy().stop();
    denied(
        &h.command(&start("A", 1, 3), 0).await,
        "host_stop_requested",
    );
    assert!(h.policy().generation().is_none());
    h.cancel.cancel();
    assert!(h
        .command_with_flags(&LoopControlRequest::Status, 0, false, false)
        .await
        .unwrap_err()
        .is_cancelled());
    assert!(h
        .command_with_flags(&start("A", 1, 3), 0, false, false)
        .await
        .unwrap_err()
        .is_cancelled());
    assert!(counted_loop::read_loop_state(&h.session).unwrap().is_none());
}

#[tokio::test]
async fn self_control_old_generation_receipts_are_stale_for_a_new_activation() {
    let mut h = Harness::new().await;
    let command = start("A", 1, 3);
    h.command(&command, 0).await;
    h.admit().await;
    assert!(matches!(
        counted_loop::terminal(&mut h.session, &h.config)
            .await
            .unwrap(),
        counted_loop::TerminalDecision::Finalize
    ));
    let generation = h.state().generation;
    counted_loop::finish(&mut h.session, &h.config, Some(&generation), &Ok(()))
        .await
        .unwrap();
    let before = h.state();
    let mut new_policy = LoopControlConfig::new(10, 100);
    new_policy.bind_owner(h.registration.ownership(), &h.session);
    new_policy
        .bind_execution("new-execution", CancellationToken::new())
        .unwrap();
    h.config.loop_control = Some(new_policy);
    denied(
        &h.command(&command, 0).await,
        "generation_not_current_activation",
    );
    assert_eq!(h.state(), before);
    assert!(h.policy().generation().is_none());
}

#[tokio::test]
async fn self_control_missing_expired_replaced_and_child_owners_fail_before_checkpoint() {
    for mode in 0..4 {
        let mut h = Harness::new().await;
        match mode {
            0 => h.config.loop_control = Some(LoopControlConfig::new(10, 100)),
            1 => {
                h.router
                    .finish_finalization(&h.session.id, "registered-owner", 0)
                    .await
                    .unwrap();
            }
            2 => h.session.created_at += chrono::Duration::seconds(1),
            3 => {
                h.session.kind = bamboo_domain::SessionKind::Child;
                h.session.parent_session_id = Some("parent".into());
            }
            _ => unreachable!(),
        }
        let durable_before = h
            .storage
            .load_session(&h.session.id)
            .await
            .unwrap()
            .unwrap();
        assert!(h
            .command_with_flags(&start("A", 1, 3), 0, false, false)
            .await
            .is_err());
        assert!(counted_loop::read_loop_state(&h.session).unwrap().is_none());
        let durable_after = h
            .storage
            .load_session(&h.session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(durable_after).unwrap(),
            serde_json::to_value(durable_before).unwrap()
        );
    }
}

struct FailCommandCheckpoint {
    inner: Arc<dyn RuntimeSessionPersistence>,
    commit_before_error: bool,
    fired: AtomicBool,
}

#[async_trait]
impl RuntimeSessionPersistence for FailCommandCheckpoint {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        if !self.fired.swap(true, Ordering::SeqCst) {
            if self.commit_before_error {
                self.inner.checkpoint_runtime_session(session).await?;
            }
            return Err(std::io::Error::other(
                "synthetic command checkpoint failure",
            ));
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

#[tokio::test]
async fn self_control_uncertain_command_checkpoint_retains_pair_and_fences_the_generation() {
    for commit_before_error in [false, true] {
        let mut h = Harness::new().await;
        let fault = Arc::new(FailCommandCheckpoint {
            inner: h.config.persistence.clone().unwrap(),
            commit_before_error,
            fired: AtomicBool::new(false),
        });
        h.config.persistence = Some(fault.clone());
        let error = h
            .command_with_flags(&start("A", 2, 5), 0, false, false)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Session checkpoint failed"));
        assert!(fault.fired.load(Ordering::SeqCst));
        let state = h.state();
        assert_eq!(h.policy().generation(), Some(state.generation.as_str()));
        assert_eq!(state.control.as_ref().unwrap().operations.len(), 1);
        assert_eq!(
            h.session
                .messages
                .iter()
                .filter(|m| m.tool_call_id.as_deref() == Some("self-control-1"))
                .count(),
            1
        );
        let result = Err(error);
        counted_loop::finish(&mut h.session, &h.config, Some(&state.generation), &result)
            .await
            .unwrap();
        assert_eq!(h.state().status, LoopStatus::Failed);
        let durable = h
            .storage
            .load_session(&h.session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            counted_loop::read_loop_state(&durable)
                .unwrap()
                .unwrap()
                .status,
            LoopStatus::Failed
        );
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|m| m.tool_call_id.as_deref() == Some("self-control-1"))
                .count(),
            1
        );
        assert!(
            counted_loop::reject_recovery(&h.session).is_ok(),
            "no objective iteration was ever admitted"
        );
    }
}

#[tokio::test]
async fn self_control_malformed_schema_two_and_oversized_writes_fail_closed() {
    let mut h = Harness::new().await;
    h.command(&start("A", 2, 10), 0).await;
    let state = h.state();
    for mutation in 0..5 {
        let mut bad = state.clone();
        match mutation {
            0 => bad.control.as_mut().unwrap().control_revision = 0,
            1 => bad.control.as_mut().unwrap().operations.clear(),
            2 => bad.control.as_mut().unwrap().operations[0].payload_sha256 = "bad".into(),
            3 => bad.control.as_mut().unwrap().applied_revision = 2,
            4 => {
                bad.objective = "\0".repeat(MAX_LOOP_PROMPT_BYTES);
                bad.control.as_mut().unwrap().pending_objective = Some(PendingLoopObjective {
                    objective: "\0".repeat(MAX_LOOP_PROMPT_BYTES),
                    objective_revision: 2,
                });
            }
            _ => unreachable!(),
        }
        let before = h.session.metadata.clone();
        assert!(counted_loop::stage_state(&mut h.session, bad).is_err());
        assert_eq!(
            h.session.metadata, before,
            "failed staging cannot publish unreadable bytes"
        );
    }
}
