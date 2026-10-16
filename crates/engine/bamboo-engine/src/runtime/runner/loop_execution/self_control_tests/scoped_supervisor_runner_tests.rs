//! Public Agent entry with a distinct, host-authorized Supervisor producer.
//! The provider and every persistence/resource dependency are fixture-local.

use super::*;
use crate::runtime::loop_control::SupervisorLoopControlGrant;
use crate::session_activation::{SessionActivationReserveOutcome, SessionActivationSpawner};
use crate::session_app::supervisor::SupervisorSessionService;
use crate::{SessionMessenger, SessionRepository};
use bamboo_domain::{
    scoped_loop_consumptions, LoopControlAction, LoopControlRequest, RuntimeSessionPersistence,
    ScopedLoopControlBinding, ScopedLoopControlConsumption, SessionActivationDisposition,
    SessionActivationError, SessionActivationPort, SessionInboxError, SessionInboxLimits,
    SessionInboxPort, SessionRunConflict, SessionRunDeliveryGuard, SupervisorReference,
};
use chrono::{DateTime, Utc};

const TARGET_ID: &str = "public-scoped-loop-target";
const TARGET_MODEL: &str = "synthetic-scoped-target";
const START_ID: &str = "17b358f5-d63e-4977-9c84-1d6ff9511ff1";
const UPDATE_ID: &str = "28a0d504-157a-4e74-9461-503852d2cf22";
const STOP_ID: &str = "3911b615-268b-4f85-a572-614963e3d033";
const GOAL_A: &str = "First objective supplied by the authorized Supervisor.";
const GOAL_B: &str = "Next objective supplied by the same authorized Supervisor.";

// The engine intentionally consumes the existing redacted registry seam.
// Keep its Project resolver and workspace validation real while supplying a
// fixture-local authoritative descriptor; no registry persistence is cloned.
struct FixtureProjectSource(crate::project_context::ProjectDescriptor);

#[async_trait]
impl crate::project_context::ProjectContextSource for FixtureProjectSource {
    async fn find_project(
        &self,
        id: &bamboo_domain::ProjectId,
    ) -> Result<
        Option<crate::project_context::ProjectDescriptor>,
        crate::project_context::ProjectContextError,
    > {
        Ok((&self.0.id == id).then(|| self.0.clone()))
    }
    async fn list_projects(
        &self,
    ) -> Result<
        Vec<crate::project_context::ProjectDescriptor>,
        crate::project_context::ProjectContextError,
    > {
        Ok(vec![self.0.clone()])
    }
    async fn find_workspace_owner(
        &self,
        workspace: &std::path::Path,
    ) -> Result<Option<bamboo_domain::ProjectId>, crate::project_context::ProjectContextError> {
        Ok(self
            .0
            .project_path
            .as_ref()
            .filter(|path| workspace.starts_with(path))
            .map(|_| self.0.id.clone()))
    }
}

#[derive(Default)]
struct Stage {
    entered: Notify,
    release: Notify,
}

struct ScopedProvider {
    storage: Arc<dyn Storage>,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
    stages: [Stage; 3],
    seen: Mutex<Vec<(Vec<Message>, Option<LoopState>)>>,
}

impl ScopedProvider {
    fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            stages: std::array::from_fn(|_| Stage::default()),
            seen: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl LLMProvider for ScopedProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;

        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            call <= 3,
            "Stop must prevent a fourth provider/summary request"
        );
        assert_eq!(model, TARGET_MODEL);
        assert!(tools
            .iter()
            .any(|schema| schema.function.name == "loop_control"));
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let guard = ActiveStream(self.active.clone());
        assert_eq!(active, 1);
        let durable = self
            .storage
            .load_execution_admission_snapshot(TARGET_ID)
            .await
            .unwrap()
            .unwrap();
        let state = read_loop_state(&durable).unwrap();
        match call {
            1 => assert!(
                state.is_none(),
                "a host grant alone must not start the target loop"
            ),
            2 => {
                let state = state.as_ref().unwrap();
                assert_eq!(
                    (
                        state.iterations_started,
                        state.iterations_completed,
                        state.rounds_started
                    ),
                    (1, 0, 2)
                );
                assert_eq!(admitted_objectives(messages), vec![(GOAL_A, 1)]);
            }
            3 => {
                let state = state.as_ref().unwrap();
                assert_eq!(
                    (
                        state.iterations_started,
                        state.iterations_completed,
                        state.rounds_started
                    ),
                    (2, 1, 3)
                );
                assert_eq!(
                    admitted_objectives(messages),
                    vec![(GOAL_A, 1), (GOAL_B, 2)]
                );
            }
            _ => unreachable!(),
        }
        self.seen.lock().unwrap().push((messages.to_vec(), state));
        self.stages[call - 1].entered.notify_one();
        self.stages[call - 1].release.notified().await;
        let response = match call {
            1 => control_call("scoped-prestart-status", json!({"action":"status"})),
            2 => LLMChunk::Token("The original remote objective is complete.".into()),
            3 => control_call("scoped-before-stop-status", json!({"action":"status"})),
            _ => unreachable!(),
        };
        let stream = futures::stream::iter([Ok(response), Ok(LLMChunk::Done)]).map(move |chunk| {
            let _keep_stream_alive = &guard;
            chunk
        });
        Ok(Box::pin(stream))
    }
}

#[derive(Default)]
struct NoSuccessor {
    reservations: AtomicUsize,
}

#[async_trait]
impl SessionActivationSpawner for NoSuccessor {
    async fn reserve_activation(
        &self,
        _target: &str,
        _generation: u64,
    ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        Err(SessionActivationError::Internal(
            "unexpected successor for live scoped control".into(),
        ))
    }
}

struct RecordedActivation {
    router: Arc<SessionActivationRouter>,
    requests: AtomicUsize,
}

#[async_trait]
impl SessionActivationPort for RecordedActivation {
    fn retain_expected_run_blocking(
        &self,
        target: &str,
        birth: DateTime<Utc>,
        run: &str,
    ) -> Result<Arc<dyn SessionRunDeliveryGuard>, SessionRunConflict> {
        self.router.retain_expected_run_blocking(target, birth, run)
    }

    async fn observe_supervisor_loop(
        &self,
        supervisor: &SupervisorReference,
        target: &str,
        revision: Option<u64>,
        action: LoopControlAction,
    ) -> Result<ScopedLoopControlBinding, SessionInboxError> {
        self.router
            .observe_supervisor_loop(supervisor, target, revision, action)
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

struct ScopedPublicFixture {
    session: Session,
    agent: Arc<crate::runtime::Agent>,
    provider: Arc<ScopedProvider>,
    storage: Arc<dyn Storage>,
    repository: Arc<SessionRepository>,
    inbox: Arc<bamboo_storage::FileSessionInbox>,
    router: Arc<SessionActivationRouter>,
    activation: Arc<RecordedActivation>,
    spawner: Arc<NoSuccessor>,
    messenger: Arc<SessionMessenger>,
    supervisor: SupervisorReference,
    supervisor_messages: Value,
    link_revision: u64,
    policy: LoopControlConfig,
    workspace: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

impl ScopedPublicFixture {
    async fn new() -> Self {
        Self::new_with_root_actor_directory(false).await
    }

    async fn new_with_root_actor_directory(native_root: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let config_dir = dir.path().join("config");
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        let mut config = bamboo_llm::Config::from_data_dir_without_env(Some(config_dir));
        *config.memory_mut() = Some(bamboo_config::MemoryConfig {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda_injection: false,
            auto_dream_enabled: false,
            ledger_gardener_enabled: false,
            ledger_distillation_enabled: false,
            gardener_enabled: false,
            dedup_gardener_enabled: false,
            granularity_freshness_gardener_enabled: false,
            ..Default::default()
        });
        config.features.dynamic_model_routing = false;
        config.features.final_evidence_check = false;
        let flags = PromptMemoryFlags::from(config.memory().as_ref().unwrap());
        assert!(
            !flags.project_prompt_injection
                && !flags.relevant_recall
                && !flags.relevant_recall_rerank
                && !flags.project_first_dream
                && !flags.ledger_agenda
        );
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(dir.path().join("sessions"))
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let repository = SessionRepository::new(
            Arc::new(Default::default()),
            storage.clone(),
            Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone())),
        );
        let repository = Arc::new(if native_root {
            repository.with_root_actor_directory(store.clone())
        } else {
            repository
        });
        let supervisor_service = SupervisorSessionService::new(storage.clone());
        let supervisor: SupervisorReference = (&supervisor_service
            .get_or_create_default("synthetic-supervisor")
            .await
            .unwrap())
            .into();
        let mut session = Session::new(TARGET_ID, TARGET_MODEL);
        session.set_project_id_meta("project-a");
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        session.metadata_version = 3;
        session.add_message(Message::system(ORIGINAL_CONTEXT));
        session.add_message(Message::assistant(ORIGINAL_FACT, None));
        storage.save_session(&session).await.unwrap();
        supervisor_service
            .configure_project_scope(
                &supervisor,
                0,
                ["project-a".parse().unwrap()].into_iter().collect(),
            )
            .await
            .unwrap();
        let link_revision = supervisor_service
            .attach(&supervisor, 1, TARGET_ID)
            .await
            .unwrap()
            .link_revision
            .unwrap();
        let source = storage
            .load_session(&supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        let supervisor_messages = serde_json::to_value(&source.messages).unwrap();
        let grant = SupervisorLoopControlGrant::new(
            supervisor.clone(),
            source.root_tool_authority_revision,
            link_revision,
            [
                LoopControlAction::Status,
                LoopControlAction::Start,
                LoopControlAction::Update,
                LoopControlAction::Stop,
            ],
            3,
            8,
        )
        .unwrap();
        let policy = LoopControlConfig::new(3, 8)
            .supervisor_control(grant)
            .unwrap();
        let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let router = SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let spawner = Arc::new(NoSuccessor::default());
        router.set_spawner(spawner.clone()).await;
        let activation = Arc::new(RecordedActivation {
            router: router.clone(),
            requests: AtomicUsize::new(0),
        });
        let messenger = Arc::new(SessionMessenger::new(
            storage.clone(),
            inbox.clone(),
            activation.clone(),
        ));
        let project_id: bamboo_domain::ProjectId = "project-a".parse().unwrap();
        let project_home = dir.path().join("projects/project-a");
        tokio::fs::create_dir_all(&project_home).await.unwrap();
        let workspace_root = dir.path().join("workspaces");
        tokio::fs::create_dir_all(&workspace_root).await.unwrap();
        let resolver = Arc::new(
            crate::project_context::ProjectContextResolver::new_with_workspace_resolver(
                Arc::new(FixtureProjectSource(
                    crate::project_context::ProjectDescriptor {
                        id: project_id.clone(),
                        name: "Scoped fixture Project".into(),
                        project_path: Some(workspace.canonicalize().unwrap()),
                        home: project_home,
                        workspace_bindings: Vec::new(),
                        resources: bamboo_domain::ProjectResourceSummary {
                            project_id,
                            resource_revision: 1,
                            resources: Vec::new(),
                        },
                    },
                )),
                bamboo_agent_core::workspace_state::WorkspaceResolver::new(
                    || None,
                    move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
                        root: workspace_root.clone(),
                        confine: false,
                    },
                ),
            ),
        );
        let agent = crate::runtime::Agent::builder()
            .project_context_resolver(resolver)
            .storage(storage.clone())
            .persistence(repository.clone())
            .session_inbox(inbox.clone())
            .session_messenger(messenger.clone())
            .activation_router(router.clone())
            .attachment_reader(store)
            .skill_manager(Arc::new(bamboo_skills::SkillManager::with_config(
                bamboo_skills::SkillStoreConfig {
                    skills_dir: dir.path().join("skills"),
                    project_dir: Some(workspace.clone()),
                    active_mode: None,
                },
            )))
            .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    dir.path().join("metrics.db"),
                )),
                7,
            ))
            .config(Arc::new(tokio::sync::RwLock::new(config)))
            .memory_store(bamboo_memory::memory_store::MemoryStore::new(
                dir.path().join("jiandu"),
            ))
            .default_tools(Arc::new(bamboo_tools::BuiltinToolExecutor::new()));
        // Keep this exact provider observable and shared with the public host.
        let provider = Arc::new(ScopedProvider::new(storage.clone()));
        let agent = Arc::new(agent.provider(provider.clone()).build().unwrap());
        Self {
            session,
            agent,
            provider,
            storage,
            repository,
            inbox,
            router,
            activation,
            spawner,
            messenger,
            supervisor,
            supervisor_messages,
            link_revision,
            policy,
            workspace,
            _dir: dir,
        }
    }

    fn request(&self, tx: mpsc::Sender<AgentEvent>) -> crate::runtime::ExecuteRequest {
        crate::runtime::ExecuteRequestBuilder::new(INITIAL_PROMPT, tx, CancellationToken::new())
            .model(TARGET_MODEL)
            .app_data_dir(self._dir.path().join("app"))
            .run_budget(bamboo_config::RunBudgetConfig {
                max_rounds: Some(8),
                ..Default::default()
            })
            .loop_control(self.policy.clone())
            .build()
    }

    fn assert_no_successor(&self) {
        assert_eq!(self.activation.requests.load(Ordering::SeqCst), 0);
        assert_eq!(self.spawner.reservations.load(Ordering::SeqCst), 0);
    }
}

fn actual_receipt(
    status: &Value,
    operation_id: &str,
    objective: &str,
) -> ScopedLoopControlConsumption {
    assert_eq!(status["configured"], true);
    assert_eq!(status["objective_applied_actual"], true);
    assert_eq!(status["checkpoint_observation"], "confirmed");
    assert_eq!(status["loop"]["objective"], objective);
    let record: ScopedLoopControlConsumption =
        serde_json::from_value(status["operation_receipt"].clone()).unwrap();
    assert_eq!(record.operation_id, operation_id);
    assert!(record.configured);
    assert!(record.rejection.is_none());
    let receipt: LoopControlReceipt =
        serde_json::from_value(record.receipt.clone().unwrap()).unwrap();
    assert_eq!(receipt.operation_id, operation_id);
    assert!(
        !receipt.applied,
        "receipt retains acceptance-time truth despite actual application"
    );
    record
}

#[tokio::test]
async fn scoped_supervisor_public_direct_orders_start_update_stop_in_one_existing_owner() {
    assert_scoped_public_start_update_stop(false).await;
}

#[tokio::test]
async fn scoped_supervisor_native_root_direct_consumes_real_goals_with_owned_control_claims() {
    // The real repository-issued lease lasts 15 seconds. Keep the whole
    // fixture-local scenario below that bound; no renewal scheduler is added.
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        assert_scoped_public_start_update_stop(true),
    )
    .await
    .expect("native Root control and exact owner finish stay within the real lease");
}

async fn assert_scoped_public_start_update_stop(native_root: bool) {
    let mut f = ScopedPublicFixture::new_with_root_actor_directory(native_root).await;
    assert_ne!(f.supervisor.session_id, f.session.id);
    let birth = f.session.created_at;
    let (tx, mut rx) = mpsc::channel(1024);
    let request = f.request(tx);
    let (lease, mut root_binding) = if native_root {
        let lease = f.agent.begin_direct_execution(TARGET_ID).await.unwrap();
        let owner_run_id = f.router.current_run_id(TARGET_ID).await.unwrap();
        let binding = f
            .repository
            .bind_root_actor_execution(&f.session, &owner_run_id)
            .await
            .unwrap()
            .expect("real repository must issue the native Root capability");
        assert_eq!(binding.owner.fence.actor_id, TARGET_ID);
        assert_eq!(binding.owner.fence.run_id, owner_run_id);
        assert_eq!(binding.owner.created_at, birth);
        let writer = binding
            .persistence
            .root_actor_writer()
            .expect("bound persistence retains the repository-issued writer");
        assert_eq!(writer.fence, binding.owner.fence);
        assert_eq!(writer.created_at, binding.owner.created_at);
        let current = binding
            .directory
            .inspect_actor(TARGET_ID)
            .await
            .unwrap()
            .activation
            .unwrap();
        assert_eq!(current.fence(), binding.owner.fence);
        assert_eq!(
            current.status,
            bamboo_domain::ActorActivationStatus::Running
        );
        (Some(lease), Some(binding))
    } else {
        (None, None)
    };
    let agent = match root_binding.as_ref() {
        Some(binding) => Arc::new(
            f.agent
                .with_execution_persistence(binding.persistence.clone()),
        ),
        None => f.agent.clone(),
    };
    let provider = f.provider.clone();
    let messenger = f.messenger.clone();
    let storage = f.storage.clone();
    let supervisor = f.supervisor.clone();
    let link_revision = f.link_revision;
    let start = LoopControlRequest::Start {
        operation_id: START_ID.into(),
        expected_generation: None,
        expected_revision: 0,
        objective: GOAL_A.into(),
        max_iterations: 2,
        max_total_rounds: 6,
    };
    let mut start_proof = None;
    let mut update_proof = None;
    let (result, events) = {
        let run = async {
            match lease {
                Some(lease) => {
                    agent
                        .execute_direct_registered(&mut f.session, request, lease)
                        .await
                }
                None => agent.execute_direct(&mut f.session, request).await,
            }
        };
        tokio::pin!(run);
        let mut events = Vec::new();
        for stage in 0..3 {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    tokio::select! {
                        result = &mut run => panic!("public runner exited before stage {stage}: {result:?}"),
                        _ = provider.stages[stage].entered.notified() => break,
                        event = rx.recv() => if let Some(event) = event { events.push(event); },
                    }
                }
            }).await.expect("bounded provider stage");
            let command = match stage {
                0 => start.clone(),
                1 => {
                    let status = messenger
                        .supervisor_loop_control(
                            &supervisor,
                            TARGET_ID,
                            None,
                            &LoopControlRequest::Status,
                            Some(START_ID),
                        )
                        .await
                        .unwrap();
                    let record = actual_receipt(&status, START_ID, GOAL_A);
                    let canonical = storage
                        .load_execution_admission_snapshot(TARGET_ID)
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(canonical
                        .session_inbox_admission()
                        .unwrap()
                        .contains(&record.input_id));
                    assert_eq!(admitted_objectives(&canonical.messages), vec![(GOAL_A, 1)]);
                    start_proof = Some(record);
                    LoopControlRequest::Update {
                        operation_id: UPDATE_ID.into(),
                        generation: status["loop"]["generation"].as_str().unwrap().into(),
                        expected_revision: status["loop"]["control_revision"].as_u64().unwrap(),
                        objective: Some(GOAL_B.into()),
                        max_iterations: None,
                        max_total_rounds: None,
                    }
                }
                2 => {
                    let status = messenger
                        .supervisor_loop_control(
                            &supervisor,
                            TARGET_ID,
                            None,
                            &LoopControlRequest::Status,
                            Some(UPDATE_ID),
                        )
                        .await
                        .unwrap();
                    let record = actual_receipt(&status, UPDATE_ID, GOAL_B);
                    let canonical = storage
                        .load_execution_admission_snapshot(TARGET_ID)
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(canonical
                        .session_inbox_admission()
                        .unwrap()
                        .contains(&record.input_id));
                    assert_eq!(
                        admitted_objectives(&canonical.messages),
                        vec![(GOAL_A, 1), (GOAL_B, 2)]
                    );
                    update_proof = Some(record);
                    LoopControlRequest::Stop {
                        operation_id: STOP_ID.into(),
                        generation: status["loop"]["generation"].as_str().unwrap().into(),
                        expected_revision: status["loop"]["control_revision"].as_u64().unwrap(),
                    }
                }
                _ => unreachable!(),
            };
            let queued = messenger
                .supervisor_loop_control(
                    &supervisor,
                    TARGET_ID,
                    Some(link_revision),
                    &command,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(queued["admitted"], true);
            assert_eq!(queued["configured"], false);
            assert_eq!(queued["objective_applied_actual"], false);
            provider.stages[stage].release.notify_one();
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut run => break result,
                    event = rx.recv() => if let Some(event) = event { events.push(event); },
                }
            }
        })
        .await
        .expect("Stop must finish the public runner without another provider");
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        (result, events)
    };
    result.unwrap();
    assert_eq!(f.provider.calls.load(Ordering::SeqCst), 3);
    assert_eq!(f.provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    {
        let seen = f.provider.seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        for (messages, _) in seen.iter() {
            assert!(messages
                .iter()
                .any(|message| message.content.contains(ORIGINAL_CONTEXT)));
            assert!(messages
                .iter()
                .any(|message| message.content == ORIGINAL_FACT));
        }
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        1
    );
    for id in ["scoped-prestart-status", "scoped-before-stop-status"] {
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event,
            AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == id))
                .count(),
            1
        );
        assert_eq!(
            f.session
                .messages
                .iter()
                .filter(|message| message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(id))
                .count(),
            1
        );
    }
    let durable = f
        .storage
        .load_execution_admission_snapshot(TARGET_ID)
        .await
        .unwrap()
        .unwrap();
    let state = read_loop_state(&durable).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Stopped);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (2, 1, 3)
    );
    assert_eq!((state.max_iterations, state.max_total_rounds), (2, 6));
    assert_eq!(state.control.as_ref().unwrap().control_revision, 3);
    assert!(state.control.as_ref().unwrap().stop_requested);
    assert!(state.control.as_ref().unwrap().pending_objective.is_none());
    assert_eq!(read_loop_state(&f.session).unwrap(), Some(state.clone()));
    assert_eq!(
        admitted_objectives(&durable.messages),
        vec![(GOAL_A, 1), (GOAL_B, 2)]
    );
    assert!(!durable
        .messages
        .iter()
        .any(|message| message.role == Role::User
            && message
                .content
                .contains(bamboo_domain::SCOPED_LOOP_INSTRUCTION)));
    let records = scoped_loop_consumptions(&durable).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(
        records
            .iter()
            .find(|record| record.operation_id == START_ID),
        start_proof.as_ref()
    );
    assert_eq!(
        records
            .iter()
            .find(|record| record.operation_id == UPDATE_ID),
        update_proof.as_ref()
    );
    let stop = records
        .iter()
        .find(|record| record.operation_id == STOP_ID)
        .unwrap();
    assert!(stop.configured);
    assert!(stop.rejection.is_none());
    assert!(durable
        .session_inbox_admission()
        .unwrap()
        .contains(&stop.input_id));
    let receipt: LoopControlReceipt =
        serde_json::from_value(stop.receipt.clone().unwrap()).unwrap();
    assert!(receipt.applied);
    assert_eq!(receipt.accepted_revision, 3);
    for record in &records {
        if let Some(binding) = root_binding.as_ref() {
            assert_eq!(record.binding.owner_run_id, binding.owner.fence.run_id);
        }
        assert_eq!(record.binding.supervisor, f.supervisor);
        assert_eq!(record.binding.target_session_id, TARGET_ID);
        assert_eq!(record.binding.execution_id, state.owner_execution_id);
        assert_eq!(record.binding.link_revision, f.link_revision);
        assert_eq!(
            state
                .control
                .as_ref()
                .unwrap()
                .operations
                .iter()
                .find(|operation| operation.receipt.operation_id == record.operation_id)
                .map(|operation| serde_json::to_value(&operation.receipt).unwrap()),
            record.receipt
        );
    }
    assert_eq!(
        durable
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .execution_id
            .as_deref(),
        Some(state.owner_execution_id.as_str())
    );
    assert_eq!(durable.created_at, birth);
    assert_eq!(durable.model, TARGET_MODEL);
    assert_eq!(
        durable.workspace_path_meta().as_deref(),
        f.workspace.to_str()
    );
    assert!(durable
        .messages
        .iter()
        .any(|message| message.role == Role::System && message.content.contains(ORIGINAL_CONTEXT)));
    assert!(durable
        .messages
        .iter()
        .any(|message| message.role == Role::Assistant && message.content == ORIGINAL_FACT));
    let source = f
        .storage
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&source.messages).unwrap(),
        f.supervisor_messages
    );
    assert_eq!(f.inbox.inspect(TARGET_ID).await.unwrap().pending, 0);
    assert!(f.router.current_run_id(TARGET_ID).await.is_none());
    if let Some(binding) = root_binding.as_mut() {
        // The direct adapter released its real router owner, while the
        // independently issued physical Root lease stays ours until finish.
        let current = binding
            .directory
            .inspect_actor(TARGET_ID)
            .await
            .unwrap()
            .activation
            .unwrap();
        assert_eq!(current.fence(), binding.owner.fence);
        assert_eq!(
            current.status,
            bamboo_domain::ActorActivationStatus::Running
        );
        assert_eq!(
            serde_json::to_value(
                binding
                    .persistence
                    .load_runtime_admission_snapshot(TARGET_ID)
                    .await
                    .unwrap()
                    .unwrap()
                    .messages
            )
            .unwrap(),
            serde_json::to_value(&durable.messages).unwrap()
        );
        binding
            .directory
            .finish_activation(
                &binding.owner.fence,
                Utc::now(),
                bamboo_domain::ActorActivationFinish::Succeeded,
            )
            .await
            .unwrap();
        binding.disarm_abandonment();
        assert!(binding
            .persistence
            .load_runtime_admission_snapshot(TARGET_ID)
            .await
            .is_err());
    }
    assert!(f
        .messenger
        .supervisor_loop_control(
            &f.supervisor,
            TARGET_ID,
            None,
            &LoopControlRequest::Status,
            Some(STOP_ID),
        )
        .await
        .is_err());
    f.assert_no_successor();
}

#[tokio::test]
async fn scoped_public_registration_failure_terminalizes_runtime_before_any_generation_or_provider()
{
    let mut f = ScopedPublicFixture::new().await;
    // A real revoked host token reaches the new registration boundary after
    // initial runtime setup/checkpoint. No fake executor or LoopState is used.
    f.policy.stop();
    let (tx, mut rx) = mpsc::channel(1024);
    let request = f.request(tx);
    let error = f
        .agent
        .execute_direct(&mut f.session, request)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Supervisor Loop owner binding failed"),
        "{error}"
    );
    assert_eq!(f.provider.calls.load(Ordering::SeqCst), 0);
    assert!(read_loop_state(&f.session).unwrap().is_none());
    assert!(scoped_loop_consumptions(&f.session).unwrap().is_empty());
    let runtime = f.session.agent_runtime_state.as_ref().unwrap();
    assert!(runtime.execution_id.is_some());
    assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Failed);
    assert!(!std::iter::from_fn(|| rx.try_recv().ok())
        .any(|event| matches!(event, AgentEvent::Complete { .. })));
    // Exercise the host's existing conservative error-save path too.
    f.repository
        .save_finalized_runtime_session(&mut f.session)
        .await
        .unwrap();
    let durable = f
        .storage
        .load_execution_admission_snapshot(TARGET_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        durable.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Failed
    );
    assert!(read_loop_state(&durable).unwrap().is_none());
    assert_eq!(f.inbox.inspect(TARGET_ID).await.unwrap().generation, 0);
    assert!(f.router.current_run_id(TARGET_ID).await.is_none());
    f.assert_no_successor();
}

mod terminal_retirement_tests;
