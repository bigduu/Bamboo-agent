//! Real live-owner delivery/checkpoint contract tests. No provider is invoked.
//! Public runner/provider wiring is covered separately by the owner.

use super::*;
use crate::runtime::config::PromptMemoryFlags;
use crate::session_activation::{
    SessionActivationReserveOutcome, SessionActivationRouter, SessionActivationSpawner,
    SessionRunRegistration,
};
use crate::session_app::supervisor::SupervisorSessionService;
use crate::{SessionMessenger, SessionMessengerError, SessionRepository};
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_domain::{
    scoped_loop_consumptions, AgentRuntimeState, AgentStatusState, LoopControlAction, Role,
    ScopedLoopControlBinding, ScopedLoopControlConsumption, ScopedLoopControlInput,
    SessionActivationDisposition, SessionActivationError, SessionActivationPort, SessionInboxError,
    SessionInboxLimits, SessionInboxPort, SessionRunConflict, SessionRunDeliveryGuard, Storage,
    SupervisorReference,
};
use bamboo_storage::{FileSessionInbox, LockedSessionStore, SessionStoreV2};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::sync::atomic::AtomicUsize;

const TARGET: &str = "scoped-loop-target";
const OWNER: &str = "registered-scoped-loop-owner";
const EXECUTION: &str = "scoped-loop-pipeline-execution";
const ORIGINAL_REQUEST: &str = "Preserve this independently owned task and context.";

#[derive(Default)]
struct NoLaunch {
    reservations: AtomicUsize,
}

#[async_trait]
impl SessionActivationSpawner for NoLaunch {
    async fn reserve_activation(
        &self,
        _target_session_id: &str,
        _inbox_generation: u64,
    ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        Err(SessionActivationError::Internal(
            "scoped control must never reserve a successor".into(),
        ))
    }
}

struct RecordingActivation {
    router: Arc<SessionActivationRouter>,
    requests: AtomicUsize,
}

#[async_trait]
impl SessionActivationPort for RecordingActivation {
    fn retain_expected_run_blocking(
        &self,
        target: &str,
        birth: DateTime<Utc>,
        expected_run: &str,
    ) -> Result<Arc<dyn SessionRunDeliveryGuard>, SessionRunConflict> {
        self.router
            .retain_expected_run_blocking(target, birth, expected_run)
    }

    async fn observe_supervisor_loop(
        &self,
        supervisor: &SupervisorReference,
        target: &str,
        expected_link_revision: Option<u64>,
        action: LoopControlAction,
    ) -> Result<ScopedLoopControlBinding, SessionInboxError> {
        self.router
            .observe_supervisor_loop(supervisor, target, expected_link_revision, action)
            .await
    }

    fn retain_supervisor_loop_blocking(
        &self,
        binding: &ScopedLoopControlBinding,
    ) -> Result<Arc<dyn SessionRunDeliveryGuard>, SessionRunConflict> {
        self.router.retain_supervisor_loop_blocking(binding)
    }

    async fn notify_active_delivery(&self, target: &str, generation: u64) -> bool {
        self.router.notify_active_delivery(target, generation).await
    }

    async fn request_activation(
        &self,
        target: &str,
        generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.router.request_activation(target, generation).await
    }
}

struct Fixture {
    registration: Option<SessionRunRegistration>,
    router: Arc<SessionActivationRouter>,
    activation: Arc<RecordingActivation>,
    spawner: Arc<NoLaunch>,
    storage: Arc<dyn Storage>,
    inbox: Arc<FileSessionInbox>,
    supervisor_service: SupervisorSessionService,
    supervisor: SupervisorReference,
    link_revision: u64,
    session: Session,
    original_messages: Value,
    config: AgentLoopConfig,
    cancel: CancellationToken,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn new(granted: bool) -> Self {
        Self::new_with_limits(granted, SessionInboxLimits::default()).await
    }

    async fn new_with_limits(granted: bool, limits: SessionInboxLimits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let concrete = Arc::new(
            SessionStoreV2::new(dir.path().join("sessions"))
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = concrete.clone();
        let supervisor_service = SupervisorSessionService::new(storage.clone());
        let supervisor: SupervisorReference = (&supervisor_service
            .get_or_create_default("synthetic-supervisor")
            .await
            .unwrap())
            .into();
        let mut session = Session::new(TARGET, "synthetic-target");
        session.set_project_id_meta("project-a");
        session.metadata_version = 3;
        session.add_message(Message::system("Original independent Root context."));
        session.add_message(Message::user(ORIGINAL_REQUEST));
        session.add_message(Message::assistant("Original retained fact.", None));
        // Match the actual owner's durable runtime identity before installing
        // its live grant. This seeds no LoopState, receipt or consumption.
        let mut runtime = AgentRuntimeState::new(OWNER);
        runtime.execution_id = Some(EXECUTION.into());
        runtime.status = AgentStatusState::Running;
        runtime.round.max_rounds = Some(30);
        session.agent_runtime_state = Some(runtime);
        storage.save_session(&session).await.unwrap();
        supervisor_service
            .configure_project_scope(
                &supervisor,
                0,
                ["project-a".parse().unwrap()].into_iter().collect(),
            )
            .await
            .unwrap();
        let attached = supervisor_service
            .attach(&supervisor, 1, TARGET)
            .await
            .unwrap();
        let link_revision = attached.link_revision.unwrap();
        let executing_supervisor = storage
            .load_session(&supervisor.session_id)
            .await
            .unwrap()
            .unwrap();

        let inbox = Arc::new(FileSessionInbox::new(
            concrete,
            limits,
        ));
        let router = SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let spawner = Arc::new(NoLaunch::default());
        router.set_spawner(spawner.clone()).await;
        let registration = router.register_session_run(&session, OWNER).await.unwrap();
        let activation = Arc::new(RecordingActivation {
            router: router.clone(),
            requests: AtomicUsize::new(0),
        });

        let mut policy = LoopControlConfig::new(5, 30);
        if granted {
            let grant = SupervisorLoopControlGrant::new(
                supervisor.clone(),
                executing_supervisor.root_tool_authority_revision,
                link_revision,
                [
                    LoopControlAction::Status,
                    LoopControlAction::Start,
                    LoopControlAction::Update,
                    LoopControlAction::Stop,
                ],
                3,
                12,
            )
            .unwrap();
            policy = policy.supervisor_control(grant).unwrap();
        }
        policy.bind_owner(registration.ownership(), &session);
        let cancel = CancellationToken::new();
        policy.bind_execution(EXECUTION, cancel.clone()).unwrap();
        if granted {
            policy
                .register_supervisor_control(&session, &storage)
                .await
                .unwrap();
        }

        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        let repository = Arc::new(SessionRepository::new(
            Arc::new(Default::default()),
            storage.clone(),
            persistence,
        ));
        let mut config = AgentLoopConfig {
            loop_control: Some(policy),
            storage: Some(storage.clone()),
            persistence: Some(repository),
            session_inbox: Some(inbox.clone()),
            guidance_active_run_id: Some(OWNER.into()),
            permission_mode: Some(bamboo_config::PermissionMode::Default),
            prompt_memory_flags: PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            ..Default::default()
        };
        config.run_budget.max_rounds = Some(30);
        let original_messages = serde_json::to_value(&session.messages).unwrap();
        Self {
            registration: Some(registration),
            router,
            activation,
            spawner,
            storage,
            inbox,
            supervisor_service,
            supervisor,
            link_revision,
            session,
            original_messages,
            config,
            cancel,
            _dir: dir,
        }
    }

    fn messenger(&self) -> SessionMessenger {
        SessionMessenger::new(
            self.storage.clone(),
            self.inbox.clone(),
            self.activation.clone(),
        )
    }

    fn policy(&self) -> &LoopControlConfig {
        self.config.loop_control.as_ref().unwrap()
    }

    async fn send(&self, command: &LoopControlRequest) -> Result<Value, SessionMessengerError> {
        self.messenger()
            .supervisor_loop_control(
                &self.supervisor,
                TARGET,
                Some(self.link_revision),
                command,
                None,
            )
            .await
    }

    async fn status(&self, operation: Option<&str>) -> Result<Value, SessionMessengerError> {
        // Bootstrap is a real service read with the entire canonical/live
        // authority path, not a guessed revision or a test-only grant.
        self.messenger()
            .supervisor_loop_control(
                &self.supervisor,
                TARGET,
                None,
                &LoopControlRequest::Status,
                operation,
            )
            .await
    }

    async fn drain(&mut self, spent: u32) -> Result<(), AgentError> {
        scoped::drain(&mut self.session, &self.config, spent).await
    }

    async fn admit(&mut self) -> bool {
        counted_loop::before_round(&mut self.session, &self.config, EXECUTION)
            .await
            .unwrap()
    }

    fn state(&self) -> LoopState {
        counted_loop::read_loop_state(&self.session)
            .unwrap()
            .unwrap()
    }

    async fn durable(&self) -> Session {
        self.storage.load_session(TARGET).await.unwrap().unwrap()
    }

    fn assert_no_activation(&self) {
        assert_eq!(self.activation.requests.load(Ordering::SeqCst), 0);
        assert_eq!(self.spawner.reservations.load(Ordering::SeqCst), 0);
    }

    async fn close(&mut self) {
        if let Some(mut registration) = self.registration.take() {
            registration.begin_finalization().await;
            let generation = self.inbox.inspect(TARGET).await.unwrap().generation;
            assert_eq!(registration.finish(generation).await.unwrap(), None);
        }
        self.assert_no_activation();
    }

    async fn detach(&self) {
        let revision = self
            .supervisor_service
            .inspect_scope(&self.supervisor)
            .await
            .unwrap()
            .state_revision;
        self.supervisor_service
            .detach(&self.supervisor, revision, TARGET)
            .await
            .unwrap();
    }

    async fn revoke_grant(&self) {
        assert!(
            self.router
                .revoke_supervisor_loop_control(
                    TARGET,
                    &self.policy().supervisor_grant_id().unwrap(),
                )
                .await
        );
    }

    async fn start_and_admit(&mut self) -> LoopControlRequest {
        let command = start("Remote A", 3, 12);
        let queued = self.send(&command).await.unwrap();
        assert_eq!(queued["admitted"], true);
        assert_eq!(queued["configured"], false);
        self.drain(0).await.unwrap();
        assert!(self.admit().await);
        command
    }

    async fn local_start(&mut self, objective: &str, iterations: u32, rounds: u32) {
        let command = start(objective, iterations, rounds);
        let arguments = serde_json::to_string(&command).unwrap();
        let command = bamboo_domain::parse_loop_control_request(&arguments).unwrap();
        self.session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "local-host-authorized-start".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "loop_control".into(),
                    arguments,
                },
            }]),
        ));
        let result = apply_current(
            &mut self.session,
            &self.config,
            &command,
            "local-host-authorized-start",
            0,
            false,
            false,
            0,
        )
        .await
        .unwrap();
        assert!(result.success, "{}", result.result);
    }

    fn update(&self, objective: &str) -> LoopControlRequest {
        let state = self.state();
        LoopControlRequest::Update {
            operation_id: uuid::Uuid::new_v4().to_string(),
            generation: state.generation,
            expected_revision: state.control.unwrap().control_revision,
            objective: Some(objective.into()),
            max_iterations: None,
            max_total_rounds: None,
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

fn stored_receipt(state: &LoopState, command: &LoopControlRequest) -> LoopControlReceipt {
    state
        .control
        .as_ref()
        .unwrap()
        .operations
        .iter()
        .find(|stored| Some(stored.receipt.operation_id.as_str()) == operation_id(command))
        .unwrap()
        .receipt
        .clone()
}

fn consumption(session: &Session, command: &LoopControlRequest) -> ScopedLoopControlConsumption {
    let records = scoped_loop_consumptions(session).unwrap();
    let matching: Vec<_> = records
        .into_iter()
        .filter(|record| Some(record.operation_id.as_str()) == operation_id(command))
        .collect();
    assert_eq!(matching.len(), 1, "one durable consumption per operation");
    matching.into_iter().next().unwrap()
}

#[tokio::test]
async fn scoped_start_distinguishes_delivery_checkpoint_and_actual_iteration_including_zero_spent()
{
    for (iterations, rounds) in [(1, 1), (3, 12)] {
        let mut h = Fixture::new(true).await;
        let command = start("Remote objective", iterations, rounds);
        let op = operation_id(&command).unwrap();
        let initial = h.status(None).await.unwrap();
        assert_eq!(initial["configured"], false);
        assert_eq!(initial["objective_applied_actual"], false);
        assert!(initial["operation_receipt"].is_null());
        let queued = h.send(&command).await.unwrap();
        assert_eq!(queued["admitted"], true);
        assert_eq!(queued["configured"], false);
        assert_ne!(queued["applied"], true);
        let binding = h
            .activation
            .observe_supervisor_loop(
                &h.supervisor,
                TARGET,
                Some(h.link_revision),
                LoopControlAction::Start,
            )
            .await
            .unwrap();
        let carrier = ScopedLoopControlInput {
            binding,
            command: command.clone(),
        }
        .envelope()
        .unwrap();
        assert_eq!(queued["receipt_id"], carrier.id.as_str());
        assert!(
            carrier.to_provider_message().is_err(),
            "the typed control is never a User message"
        );
        let durable = h.durable().await;
        assert!(counted_loop::read_loop_state(&durable).unwrap().is_none());
        assert_eq!(
            serde_json::to_value(&durable.messages).unwrap(),
            h.original_messages
        );
        assert_eq!(h.inbox.inspect(TARGET).await.unwrap().pending, 1);
        assert!(!h.inbox.inspect(TARGET).await.unwrap().activation_pending());
        assert_eq!(h.status(Some(op)).await.unwrap()["configured"], false);

        h.drain(0).await.unwrap();
        let configured = h.durable().await;
        let state = counted_loop::read_loop_state(&configured).unwrap().unwrap();
        assert_eq!((state.iterations_started, state.rounds_started), (0, 0));
        assert_eq!(
            serde_json::to_value(&configured.messages).unwrap(),
            h.original_messages
        );
        assert_eq!(h.inbox.inspect(TARGET).await.unwrap().pending, 0);
        let proof = consumption(&configured, &command);
        assert!(proof.configured);
        assert!(proof.rejection.is_none());
        assert_eq!(
            proof.receipt,
            Some(serde_json::to_value(stored_receipt(&state, &command)).unwrap())
        );
        let status = h.status(Some(op)).await.unwrap();
        assert_eq!(status["configured"], true);
        assert_eq!(status["objective_applied_actual"], false);
        assert_eq!(
            status["operation_receipt"],
            serde_json::to_value(&proof).unwrap()
        );

        assert!(
            h.admit().await,
            "the first remote iteration consumes no phantom round"
        );
        assert_eq!(
            (h.state().iterations_started, h.state().rounds_started),
            (1, 1)
        );
        let durable = h.durable().await;
        assert_eq!(
            durable.messages.len(),
            h.original_messages.as_array().unwrap().len() + 1
        );
        let appended = durable.messages.last().unwrap();
        assert_eq!(appended.role, Role::User);
        assert_eq!(appended.content, "Remote objective");
        assert_eq!(
            appended.metadata.as_ref().unwrap()["runtime_kind"],
            "counted_loop_iteration"
        );
        let status = h.status(Some(op)).await.unwrap();
        assert_eq!(status["objective_applied_actual"], true);
        assert_eq!(
            status["operation_receipt"],
            serde_json::to_value(&proof).unwrap(),
            "acceptance receipt is immutable"
        );
        assert!(!h.cancel.is_cancelled());
        h.close().await;
    }
}

#[tokio::test]
async fn scoped_update_retry_is_exact_and_pending_goal_applies_only_after_current_iteration() {
    let mut h = Fixture::new(true).await;
    let original_start = h.start_and_admit().await;
    let command = h.update("Remote B");
    let op = operation_id(&command).unwrap();
    let first = h.send(&command).await.unwrap();
    let retry = h.send(&command).await.unwrap();
    assert_eq!(retry["receipt_id"], first["receipt_id"]);
    assert_eq!(retry["generation"], first["generation"]);
    assert_eq!(h.inbox.inspect(TARGET).await.unwrap().pending, 1);
    let mut conflicting = command.clone();
    if let LoopControlRequest::Update { objective, .. } = &mut conflicting {
        *objective = Some("Conflicting C".into());
    }
    assert!(h.send(&conflicting).await.is_err());
    h.drain(1).await.unwrap();
    let state = h.state();
    assert_eq!(state.objective, "Remote A");
    assert_eq!(
        state
            .control
            .as_ref()
            .unwrap()
            .pending_objective
            .as_ref()
            .unwrap()
            .objective,
        "Remote B"
    );
    assert_eq!(state.control.as_ref().unwrap().control_revision, 2);
    assert_eq!(state.control.as_ref().unwrap().applied_revision, 1);
    let proof = consumption(&h.durable().await, &command);
    assert!(proof.configured);
    assert_eq!(
        proof.receipt,
        Some(serde_json::to_value(stored_receipt(&state, &command)).unwrap())
    );
    let status = h.status(Some(op)).await.unwrap();
    assert_eq!(status["configured"], true);
    assert_eq!(status["objective_applied_actual"], false);
    assert_eq!(
        status["operation_receipt"],
        serde_json::to_value(&proof).unwrap()
    );
    assert_eq!(h.send(&command).await.unwrap()["configured"], true);
    h.drain(1).await.unwrap();
    assert_eq!(
        h.state(),
        state,
        "an ACKed retry cannot advance control or budget"
    );
    assert!(matches!(
        counted_loop::terminal(&mut h.session, &h.config)
            .await
            .unwrap(),
        counted_loop::TerminalDecision::Continue
    ));
    h.drain(1).await.unwrap();
    assert!(h.admit().await);
    let state = h.state();
    assert_eq!(
        (state.objective.as_str(), state.objective_revision),
        ("Remote B", 2)
    );
    assert_eq!(state.control.as_ref().unwrap().applied_revision, 2);
    assert_eq!(
        h.status(Some(op)).await.unwrap()["objective_applied_actual"],
        true
    );
    assert_eq!(consumption(&h.durable().await, &command), proof);
    assert_eq!(stored_receipt(&state, &original_start).accepted_revision, 1);
    assert_eq!(state.control.as_ref().unwrap().operations.len(), 2);
    h.close().await;
}

#[tokio::test]
async fn scoped_stop_preserves_context_and_admits_no_pending_goal_or_successor() {
    let mut h = Fixture::new(true).await;
    h.start_and_admit().await;
    let pending = h.update("Never admit B");
    h.send(&pending).await.unwrap();
    h.drain(1).await.unwrap();
    let before_messages = serde_json::to_value(&h.session.messages).unwrap();
    let state = h.state();
    let command = LoopControlRequest::Stop {
        operation_id: uuid::Uuid::new_v4().to_string(),
        generation: state.generation,
        expected_revision: state.control.unwrap().control_revision,
    };
    assert_eq!(h.send(&command).await.unwrap()["configured"], false);
    let trailing = h.update("Never consume after Stop");
    h.send(&trailing).await.unwrap();
    h.drain(1).await.unwrap();
    assert!(consumption(&h.durable().await, &command).configured);
    assert!(!scoped_loop_consumptions(&h.durable().await)
        .unwrap()
        .iter()
        .any(|record| record.operation_id
            == match &trailing {
                LoopControlRequest::Update { operation_id, .. } => operation_id.as_str(),
                _ => unreachable!(),
            }));
    let operation = operation_id(&command).unwrap();
    let status = h.status(Some(operation)).await.unwrap();
    assert_eq!(status["configured"], true);
    assert_eq!(status["loop"]["stop_requested"], true);
    assert_eq!(
        status["operation_receipt"],
        serde_json::to_value(consumption(&h.durable().await, &command)).unwrap()
    );
    let generation = h.inbox.inspect(TARGET).await.unwrap().generation;
    let retry = h.send(&command).await.unwrap();
    assert_eq!(retry["configured"], true);
    assert_eq!(
        h.inbox.inspect(TARGET).await.unwrap().generation,
        generation
    );
    assert!(!h.admit().await);
    assert_eq!(h.state().status, LoopStatus::Stopped);
    let stopped = h.status(Some(operation)).await.unwrap();
    assert_eq!(stopped["loop"]["status"], "stopped");
    assert_eq!(stopped["operation_receipt"], status["operation_receipt"]);
    assert_eq!(
        serde_json::to_value(&h.durable().await.messages).unwrap(),
        before_messages
    );
    assert!(
        !h.cancel.is_cancelled(),
        "stop is independent from user cancellation"
    );
    h.close().await;
}

#[tokio::test]
async fn scoped_status_and_mutations_require_an_extra_live_host_grant() {
    let mut h = Fixture::new(false).await;
    let command = start("No implicit authority", 1, 1);
    assert!(
        h.status(None).await.is_err(),
        "attachment and self opt-in do not grant remote status"
    );
    assert!(h.send(&command).await.is_err());
    assert!(counted_loop::read_loop_state(&h.durable().await)
        .unwrap()
        .is_none());
    assert_eq!(h.inbox.inspect(TARGET).await.unwrap().generation, 0);
    assert_eq!(
        serde_json::to_value(&h.durable().await.messages).unwrap(),
        h.original_messages
    );
    h.close().await;
}

#[tokio::test]
async fn scoped_queued_command_cannot_migrate_to_a_new_or_missing_owner() {
    for replacement in [false, true] {
        let mut h = Fixture::new(true).await;
        let command = start("Old owner only", 1, 2);
        h.send(&command).await.unwrap();
        h.close().await;
        if replacement {
            h.registration = Some(
                h.router
                    .register_session_run(&h.session, "replacement-owner")
                    .await
                    .unwrap(),
            );
        }
        assert!(h.status(None).await.is_err());
        assert!(h
            .send(&start("No successor permission", 1, 2))
            .await
            .is_err());
        assert!(
            h.drain(0).await.is_err(),
            "the old owner proof must never be reconstructed from target id"
        );
        let durable = h.durable().await;
        assert!(counted_loop::read_loop_state(&durable).unwrap().is_none());
        assert!(scoped_loop_consumptions(&durable).unwrap().is_empty());
        assert_eq!(
            serde_json::to_value(&durable.messages).unwrap(),
            h.original_messages
        );
        h.close().await;
    }
}

#[tokio::test]
async fn scoped_detach_or_grant_revocation_before_drain_cannot_publish_a_configuration_proof() {
    for detach in [false, true] {
        let mut h = Fixture::new(true).await;
        h.send(&start("Must remain unconfigured", 1, 2))
            .await
            .unwrap();
        if detach {
            h.detach().await;
        } else {
            h.revoke_grant().await;
        }
        assert!(h.drain(0).await.is_err());
        let durable = h.durable().await;
        assert!(counted_loop::read_loop_state(&durable).unwrap().is_none());
        assert!(scoped_loop_consumptions(&durable).unwrap().is_empty());
        assert_eq!(
            serde_json::to_value(&durable.messages).unwrap(),
            h.original_messages
        );
        assert!(
            h.status(None).await.is_err(),
            "status bootstrap is not authorization"
        );
        h.close().await;
    }
}

#[tokio::test]
async fn scoped_pending_remote_objective_keeps_its_authority_until_future_admission() {
    for detach in [false, true] {
        let mut h = Fixture::new(true).await;
        h.start_and_admit().await;
        let command = h.update("Future B needs a live grant");
        h.send(&command).await.unwrap();
        h.drain(1).await.unwrap();
        assert!(matches!(
            counted_loop::terminal(&mut h.session, &h.config)
                .await
                .unwrap(),
            counted_loop::TerminalDecision::Continue
        ));
        let before = h.durable().await;
        let state = counted_loop::read_loop_state(&before).unwrap().unwrap();
        if detach {
            h.detach().await;
        } else {
            h.revoke_grant().await;
        }
        let admission = async {
            h.drain(1).await?;
            counted_loop::before_round(&mut h.session, &h.config, EXECUTION).await
        }
        .await;
        assert!(
            admission.is_err(),
            "the complete safe boundary must recheck authority before a new goal is admitted"
        );
        let after = h.durable().await;
        assert_eq!(
            counted_loop::read_loop_state(&after).unwrap().unwrap(),
            state
        );
        assert_eq!(
            serde_json::to_value(&after.messages).unwrap(),
            serde_json::to_value(&before.messages).unwrap()
        );
        assert!(!after
            .messages
            .iter()
            .any(|message| message.role == Role::User
                && message.content == "Future B needs a live grant"));
        assert_eq!(
            consumption(&after, &command),
            consumption(&before, &command)
        );
        h.close().await;
    }
}

#[tokio::test]
async fn scoped_update_checks_effective_omitted_bounds_and_cas_without_consuming_loop_budget() {
    let mut h = Fixture::new(true).await;
    // Self-control is independently host-authorized to wider bounds; it does
    // not transfer those bounds to the stricter remote grant (3 / 12).
    h.local_start("Wider local A", 4, 20).await;
    assert!(h.admit().await);
    let original = h.state();
    assert_eq!(original.rounds_started, 2);
    for stale_cas in [false, true] {
        let mut command = h.update("Remote B");
        if stale_cas {
            if let LoopControlRequest::Update {
                expected_revision,
                max_iterations,
                max_total_rounds,
                ..
            } = &mut command
            {
                *expected_revision = 0;
                *max_iterations = Some(3);
                *max_total_rounds = Some(12);
            }
        }
        assert_eq!(h.send(&command).await.unwrap()["configured"], false);
        h.drain(2).await.unwrap();
        let durable = h.durable().await;
        assert_eq!(
            counted_loop::read_loop_state(&durable).unwrap().unwrap(),
            original
        );
        let proof = consumption(&durable, &command);
        assert!(!proof.configured);
        assert!(proof.receipt.is_none());
        assert!(proof
            .rejection
            .as_deref()
            .is_some_and(|reason| !reason.is_empty()));
        let status = h.status(operation_id(&command)).await.unwrap();
        assert_eq!(status["configured"], false);
        assert_eq!(status["objective_applied_actual"], false);
        assert_eq!(
            status["operation_receipt"],
            serde_json::to_value(&proof).unwrap()
        );
        assert_eq!(h.inbox.inspect(TARGET).await.unwrap().pending, 0);
    }
    let mut bounded = h.update("Remote B");
    if let LoopControlRequest::Update {
        max_iterations,
        max_total_rounds,
        ..
    } = &mut bounded
    {
        *max_iterations = Some(3);
        *max_total_rounds = Some(12);
    }
    h.send(&bounded).await.unwrap();
    h.drain(2).await.unwrap();
    assert!(consumption(&h.durable().await, &bounded).configured);
    let state = h.state();
    assert_eq!((state.max_iterations, state.max_total_rounds), (3, 12));
    assert_eq!(state.rounds_started, original.rounds_started);
    assert_eq!(state.iterations_started, original.iterations_started);
    assert_eq!(state.objective, "Wider local A");
    assert_eq!(
        state
            .control
            .as_ref()
            .unwrap()
            .pending_objective
            .as_ref()
            .unwrap()
            .objective,
        "Remote B"
    );
    assert_eq!(state.control.as_ref().unwrap().control_revision, 2);
    h.close().await;
}

// Only the claim return is parked. All actual queue and checkpoint operations
// delegate to the real persistence already attached to the live-owner Fixture.
struct PauseScopedClaimAfterSuccess {
    inner: Arc<dyn bamboo_domain::RuntimeSessionPersistence>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    claims_observed: Arc<AtomicUsize>,
}

#[async_trait]
impl bamboo_domain::RuntimeSessionPersistence for PauseScopedClaimAfterSuccess {
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
        let claims = self
            .inner
            .claim_scoped_loop_controls(session, inbox, active_run_id, limit)
            .await?;
        if !claims.is_empty() {
            self.claims_observed
                .fetch_add(claims.len(), Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(claims)
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
        self.inner
            .checkpoint_scoped_loop_control(session, bindings, activation, input)
            .await
    }

    async fn load_loop_control_checkpoint(
        &self,
        session_id: &str,
    ) -> std::io::Result<Option<Session>> {
        self.inner.load_loop_control_checkpoint(session_id).await
    }
}

#[tokio::test]
async fn scoped_host_stop_during_successful_claim_leaves_update_unconsumed_and_stops_normally() {
    let mut h = Fixture::new(true).await;
    let start = h.start_and_admit().await;
    let before = h.state();
    let before_messages = serde_json::to_value(&h.session.messages).unwrap();
    let update = h.update("Must not apply after host stop");
    let queued = h.send(&update).await.unwrap();
    assert_eq!(queued["admitted"], true);
    assert_eq!(queued["configured"], false);
    let update_id =
        bamboo_domain::SessionMessageId::parse(queued["receipt_id"].as_str().unwrap()).unwrap();
    let update_operation = operation_id(&update).unwrap().to_owned();
    let start_consumption = consumption(&h.durable().await, &start);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let claims_observed = Arc::new(AtomicUsize::new(0));
    h.config.persistence = Some(Arc::new(PauseScopedClaimAfterSuccess {
        inner: h.config.persistence.take().unwrap(),
        entered: entered.clone(),
        release: release.clone(),
        claims_observed: claims_observed.clone(),
    }));
    let policy = h.policy().clone();
    let stop_after_claim = async move {
        entered.notified().await;
        policy.stop();
        release.notify_one();
    };
    let (drained, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(h.drain(1), stop_after_claim)
    })
    .await
    .expect("real control claim did not reach its deterministic stop gate");
    assert_eq!(claims_observed.load(Ordering::SeqCst), 1);
    assert!(
        drained.is_ok(),
        "host stop after a successful claim must not become an authority failure: {drained:?}"
    );
    assert!(!h.admit().await);
    let durable = h.durable().await;
    let stopped = counted_loop::read_loop_state(&durable).unwrap().unwrap();
    assert_eq!(stopped.status, LoopStatus::Stopped);
    assert_eq!(stopped.objective, before.objective);
    assert_eq!(
        (
            stopped.iterations_started,
            stopped.iterations_completed,
            stopped.rounds_started,
        ),
        (
            before.iterations_started,
            before.iterations_completed,
            before.rounds_started,
        )
    );
    assert!(stopped
        .control
        .as_ref()
        .unwrap()
        .pending_objective
        .is_none());
    assert!(!scoped_loop_consumptions(&durable)
        .unwrap()
        .iter()
        .any(|record| record.operation_id == update_operation));
    assert!(!h.inbox.was_admitted(TARGET, &update_id).await.unwrap());
    assert_eq!(consumption(&durable, &start), start_consumption);
    assert_eq!(
        serde_json::to_value(&durable.messages).unwrap(),
        before_messages
    );
    assert!(
        !h.cancel.is_cancelled(),
        "the real host stop is independent from user cancellation"
    );
    h.close().await;
}

mod stop_backlog_tests;
