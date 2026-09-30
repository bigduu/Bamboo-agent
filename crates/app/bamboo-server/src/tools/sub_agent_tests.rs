use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};

use serde_json::json;
use uuid::Uuid;

use bamboo_agent_core::tools::{
    Tool, ToolCtx, ToolError, ToolExecutionContext, ToolOutcome, ToolResult,
};
use bamboo_domain::session::runtime_state::ChildWaitPolicy;
use bamboo_domain::{
    ActorDirectoryPort, ActorLogicalState, SessionActivationDisposition, SessionActivationError,
    SessionActivationPort, SessionInboxPort,
};
use bamboo_engine::session_app::child_session;
use bamboo_engine::session_app::child_session::{
    ChildRunnerInfo, ChildSessionEntry, ChildSessionError, ChildSessionPort, DeleteChildResult,
};

use crate::app_state::{AgentRunner, AgentStatus};
use crate::tools::{ChildSessionAdapter, PlanTool, SubAgentTool};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
use bamboo_agent_core::{AgentEvent, Message, Role, Session};
use bamboo_engine::execution::spawn::{SpawnContext, SpawnScheduler};
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use bamboo_metrics::collector::MetricsCollector;
use bamboo_metrics::storage::SqliteMetricsStorage;
use bamboo_skills::SkillManager;
use bamboo_storage::SessionStoreV2;

/// Invoke the `SubAgent` tool (a `Tool`, not an executor) and unwrap the
/// synchronous `Completed` outcome these tests expect, yielding the same
/// `ToolResult`/`ToolError` the pre-rewrite `execute_with_context` returned so
/// existing `.expect`/`.unwrap_err`/`is_ok`/`{:?}` assertions keep working.
async fn invoke_completed(
    tool: &SubAgentTool,
    args: serde_json::Value,
    ctx: ToolCtx,
) -> Result<ToolResult, ToolError> {
    match tool.invoke(args, ctx).await {
        Ok(ToolOutcome::Completed(result)) => Ok(result),
        Ok(_) => panic!("expected a Completed outcome"),
        Err(e) => Err(e),
    }
}

async fn invoke_plan_completed(
    tool: &PlanTool,
    args: serde_json::Value,
    ctx: ToolCtx,
) -> Result<ToolResult, ToolError> {
    match tool.invoke(args, ctx).await {
        Ok(ToolOutcome::Completed(result)) => Ok(result),
        Ok(_) => panic!("expected a Completed outcome"),
        Err(error) => Err(error),
    }
}

#[tokio::test]
async fn host_local_deploy_uses_durable_child_actor_and_canonical_inbox() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let store = harness.adapter.session_store.clone();
    let registry = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let deploy = crate::tools::DeployAgentTool::new(
        "ws://127.0.0.1:1",
        "unused",
        "/absent-bamboo",
        registry.clone(),
        Arc::new(RwLock::new(bamboo_config::Config::default())),
    )
    .with_actor_store(store.clone())
    .with_child_port(harness.adapter.clone());
    let ToolOutcome::Completed(created) = deploy
        .invoke(
            json!({
                "action": "deploy",
                "env": "local",
                "role": "worker",
                "workspace": harness.workspace_path,
            }),
            subagent_test_ctx(&harness.parent_session_id, "deploy-local-actor"),
        )
        .await
        .expect("local deploy creates a logical Child")
    else {
        panic!("deploy must complete synchronously");
    };
    let created: serde_json::Value = serde_json::from_str(&created.result).unwrap();
    let actor_id = created["id"].as_str().unwrap();
    assert!(actor_id.starts_with("actor-"));
    assert_eq!(created["status"], "cold");
    let child = store.load_session(actor_id).await.unwrap().unwrap();
    let actor = store.inspect_actor(actor_id).await.unwrap().actor;
    assert_eq!(actor.actor_id, child.id);
    assert_eq!(
        actor.parent_actor_id.as_deref(),
        Some(harness.parent_session_id.as_str())
    );
    assert_eq!(actor.state, ActorLogicalState::Cold);
    assert_eq!(
        child.metadata.get("deployment_kind").map(String::as_str),
        Some("legacy_logical")
    );
    assert!(
        registry.lock().await.is_empty(),
        "no private broker worker was launched"
    );

    let ToolOutcome::Completed(listed) = deploy
        .invoke(
            json!({"action": "list"}),
            subagent_test_ctx(&harness.parent_session_id, "list-local-actor"),
        )
        .await
        .unwrap()
    else {
        panic!("list must complete synchronously");
    };
    let listed: serde_json::Value = serde_json::from_str(&listed.result).unwrap();
    assert_eq!(
        listed["agents"].as_array().unwrap().len(),
        1,
        "ordinary SubAgent Child must stay out of deploy list"
    );
    assert_eq!(listed["agents"][0]["id"], actor_id);

    // Use a recording activation port so this verifies durable admission
    // without launching the test harness's NoopProvider worker.
    let messenger = Arc::new(bamboo_engine::SessionMessenger::new(
        harness.storage.clone(),
        harness.session_inbox.clone(),
        Arc::new(RecordingActivation::default()),
    ));
    let ask = crate::tools::AskAgentTool::new("ws://127.0.0.1:1", "unused")
        .with_deployments(registry, store.clone())
        .with_messenger(messenger);
    let ToolOutcome::Completed(steered) = ask
        .invoke(
            json!({"target": actor_id, "mode": "steer", "question": "Handle only the assigned file"}),
            subagent_test_ctx(&harness.parent_session_id, "steer-local-actor"),
        )
        .await
        .expect("Root can steer its durable Child")
    else {
        panic!("steer must complete synchronously");
    };
    let steered: serde_json::Value = serde_json::from_str(&steered.result).unwrap();
    assert_eq!(steered["admitted"], true);
    assert_eq!(
        harness
            .session_inbox
            .inspect(actor_id)
            .await
            .unwrap()
            .pending,
        1
    );

    let ToolOutcome::Completed(stopped) = deploy
        .invoke(
            json!({"action": "stop", "id": actor_id}),
            subagent_test_ctx(&harness.parent_session_id, "stop-local-actor"),
        )
        .await
        .expect("Root can retire its Child")
    else {
        panic!("stop must complete synchronously");
    };
    let stopped: serde_json::Value = serde_json::from_str(&stopped.result).unwrap();
    assert_eq!(stopped["status"], "stopped");
    assert_eq!(
        store.inspect_actor(actor_id).await.unwrap().actor.state,
        ActorLogicalState::Retired
    );
    assert!(ask
        .invoke(
            json!({"target": actor_id, "mode": "steer", "question": "Another task"}),
            subagent_test_ctx(&harness.parent_session_id, "steer-retired-actor"),
        )
        .await
        .is_err());
}

#[tokio::test]
async fn retired_root_cannot_deploy_or_steer_durable_child() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let store = harness.adapter.session_store.clone();
    let registry = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let deploy = crate::tools::DeployAgentTool::new(
        "ws://127.0.0.1:1",
        "unused",
        "/absent-bamboo",
        registry.clone(),
        Arc::new(RwLock::new(bamboo_config::Config::default())),
    )
    .with_actor_store(store.clone())
    .with_child_port(harness.adapter.clone());
    let ToolOutcome::Completed(created) = deploy
        .invoke(
            json!({
                "action": "deploy",
                "env": "local",
                "role": "worker",
                "workspace": harness.workspace_path,
            }),
            subagent_test_ctx(&harness.parent_session_id, "deploy-before-root-retire"),
        )
        .await
        .expect("live Root creates a Child")
    else {
        panic!("deploy must complete synchronously");
    };
    let created: serde_json::Value = serde_json::from_str(&created.result).unwrap();
    let actor_id = created["id"].as_str().unwrap();
    store
        .retire_actor(&harness.parent_session_id, chrono::Utc::now())
        .await
        .expect("Root retirement persists");

    let messenger = Arc::new(bamboo_engine::SessionMessenger::new(
        harness.storage.clone(),
        harness.session_inbox.clone(),
        Arc::new(RecordingActivation::default()),
    ));
    let ask = crate::tools::AskAgentTool::new("ws://127.0.0.1:1", "unused")
        .with_deployments(registry, store.clone())
        .with_messenger(messenger);
    assert!(ask
        .invoke(
            json!({"target": actor_id, "mode": "steer", "question": "Work after retirement"}),
            subagent_test_ctx(&harness.parent_session_id, "steer-after-root-retire"),
        )
        .await
        .is_err());
    assert_eq!(
        harness
            .session_inbox
            .inspect(actor_id)
            .await
            .unwrap()
            .pending,
        0,
        "retired Root must not enqueue a Child message"
    );
    assert!(deploy
        .invoke(
            json!({"action": "deploy", "env": "local", "role": "worker", "workspace": harness.workspace_path}),
            subagent_test_ctx(&harness.parent_session_id, "deploy-after-root-retire"),
        )
        .await
        .is_err());
}

struct QuestionHostBound;

#[async_trait::async_trait]
impl bamboo_server_tools::ParentRequestReplyPort for QuestionHostBound {
    async fn resolve(
        &self,
        _: &str,
        _: &str,
        _: bamboo_domain::ParentRequestOption,
    ) -> Result<bamboo_server_tools::ParentRequestReplyReceipt, String> {
        Err("test question binding never grants permission".into())
    }
}

#[tokio::test]
async fn ask_parent_requires_host_bound_child_and_returns_agentic_clarification() {
    let harness = build_test_harness().await;
    let ask = json!({"intent":"ask_parent","message":"Which option?"});
    assert!(
        invoke_completed(
            &harness.tool,
            ask.clone(),
            subagent_test_ctx(&harness.child_session_id, "ask-parent-local"),
        )
        .await
        .is_err(),
        "unbound local tool cannot park a Child"
    );
    let tool = SubAgentTool::new(harness.adapter.clone(), harness.adapter.clone())
        .with_parent_request_replies(Arc::new(QuestionHostBound));
    assert!(
        invoke_completed(
            &tool,
            ask.clone(),
            subagent_test_ctx(&harness.parent_session_id, "ask-parent-root"),
        )
        .await
        .is_err(),
        "Root cannot ask itself"
    );
    let result = invoke_completed(
        &tool,
        ask.clone(),
        subagent_test_ctx(&harness.child_session_id, "ask-parent-child"),
    )
    .await
    .expect("Host-bound direct Child can ask");
    assert!(matches!(
        bamboo_agent_core::tools::try_parse_agentic_result(&result),
        Some(bamboo_agent_core::AgenticToolResult::NeedClarification { question, options })
            if question == "Which option?" && options.is_none()
    ));
    let mut child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    child.set_subagent_type("planner");
    harness.storage.save_session(&child).await.unwrap();
    assert!(
        invoke_completed(
            &tool,
            ask.clone(),
            subagent_test_ctx(&harness.child_session_id, "ask-parent-planner"),
        )
        .await
        .is_err(),
        "planner Child cannot bypass its role through ask_parent"
    );
    child.set_subagent_type("guardian");
    harness.storage.save_session(&child).await.unwrap();
    assert!(
        invoke_completed(
            &tool,
            ask.clone(),
            subagent_test_ctx(&harness.child_session_id, "ask-parent-guardian"),
        )
        .await
        .is_err(),
        "guardian Child cannot bypass its role through ask_parent"
    );
    child.set_subagent_type("general");
    child.set_project_id_meta(bamboo_domain::ProjectId::new().to_string());
    harness.storage.save_session(&child).await.unwrap();
    assert!(
        invoke_completed(
            &tool,
            ask.clone(),
            subagent_test_ctx(&harness.child_session_id, "ask-parent-project-changed"),
        )
        .await
        .is_err(),
        "Child cannot ask after its Project diverges from the direct parent"
    );
    child.clear_project_id_meta();
    let other_parent = Session::new("different-root", "gpt-5");
    harness.storage.save_session(&other_parent).await.unwrap();
    child.parent_session_id = Some(other_parent.id.clone());
    harness.storage.save_session(&child).await.unwrap();
    assert!(
        invoke_completed(
            &tool,
            ask,
            subagent_test_ctx(&harness.child_session_id, "ask-parent-lineage-changed"),
        )
        .await
        .is_err(),
        "Child cannot ask after its direct-parent identity diverges from its Root"
    );
}

fn subagent_test_ctx(session_id: &str, tool_call_id: &str) -> ToolCtx {
    ToolExecutionContext {
        executing_supervisor: None,
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id,
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    }
    .to_tool_ctx()
}

struct NoopProvider;

#[async_trait::async_trait]
impl LLMProvider for NoopProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        Err(LLMError::Api("noop".to_string()))
    }
}

struct NoopToolExecutor;

#[async_trait::async_trait]
impl ToolExecutor for NoopToolExecutor {
    async fn execute(&self, _call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        Err(ToolError::NotFound("noop".to_string()))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        Vec::new()
    }
}

#[derive(Default)]
struct RecordingActivation {
    calls: AtomicUsize,
    failures_remaining: AtomicUsize,
    delegate: StdRwLock<Option<Arc<dyn SessionActivationPort>>>,
    forced_disposition: StdRwLock<Option<SessionActivationDisposition>>,
    pause_next: AtomicBool,
    activation_entered: tokio::sync::Notify,
    release_activation: tokio::sync::Notify,
}

impl RecordingActivation {
    fn fail_next(&self) {
        self.failures_remaining.fetch_add(1, Ordering::SeqCst);
    }

    fn set_delegate(&self, delegate: Arc<dyn SessionActivationPort>) {
        *self.delegate.write().unwrap() = Some(delegate);
    }

    fn force_disposition(&self, disposition: SessionActivationDisposition) {
        *self.forced_disposition.write().unwrap() = Some(disposition);
    }
}

#[async_trait::async_trait]
impl SessionActivationPort for RecordingActivation {
    async fn request_activation(
        &self,
        target_session_id: &str,
        inbox_generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.pause_next.swap(false, Ordering::SeqCst) {
            self.activation_entered.notify_one();
            self.release_activation.notified().await;
        }
        if self
            .failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(SessionActivationError::Internal(
                "injected activation failure".to_string(),
            ));
        }
        if let Some(disposition) = *self.forced_disposition.read().unwrap() {
            return Ok(disposition);
        }
        let delegate = self.delegate.read().unwrap().clone();
        match delegate {
            Some(delegate) => {
                delegate
                    .request_activation(target_session_id, inbox_generation)
                    .await
            }
            None => Ok(SessionActivationDisposition::ActiveNotified),
        }
    }
}

/// No-op child runner for these tool/adapter-level tests. Sub-agents always run
/// as actors now (the in-process spawn path was removed), but these tests
/// exercise the SubAgent tool + adapter + scheduler bookkeeping — event
/// emission, wait registration, queueing, manually-seeded runner state — not
/// real child execution. Background runs resolve immediately as "completed".
struct NoopChildRunner;

#[async_trait::async_trait]
impl bamboo_engine::execution::spawn::ExternalChildRunner for NoopChildRunner {
    async fn should_handle(&self, _session: &Session) -> bool {
        true
    }

    async fn execute_external_child(
        &self,
        _session: &mut Session,
        _job: &bamboo_engine::execution::spawn::SpawnJob,
        _event_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        _cancel_token: tokio_util::sync::CancellationToken,
    ) -> bamboo_engine::runner::Result<()> {
        Ok(())
    }
}

struct QueueBoundaryHook {
    before_child: String,
    before_once: AtomicBool,
    before_entered: Arc<tokio::sync::Semaphore>,
    before_release: Arc<tokio::sync::Semaphore>,
    during_child: String,
    during_once: AtomicBool,
    during_entered: Arc<tokio::sync::Semaphore>,
    during_release: Arc<tokio::sync::Semaphore>,
}

impl bamboo_engine::execution::ChildRunLaunchHook for QueueBoundaryHook {
    fn before_child_launch(
        &self,
        _job: &bamboo_engine::execution::spawn::SpawnJob,
        _child_events: broadcast::Sender<AgentEvent>,
    ) {
    }

    fn before_queued_dequeue(
        &self,
        job: &bamboo_engine::execution::spawn::SpawnJob,
    ) -> futures::future::BoxFuture<'static, ()> {
        let matches = job.child_session_id == self.before_child
            && self.before_once.swap(false, Ordering::SeqCst);
        let entered = self.before_entered.clone();
        let release = self.before_release.clone();
        Box::pin(async move {
            if matches {
                entered.add_permits(1);
                release
                    .acquire()
                    .await
                    .expect("release queue barrier")
                    .forget();
            }
        })
    }

    fn before_queued_reservation(
        &self,
        job: &bamboo_engine::execution::spawn::SpawnJob,
    ) -> futures::future::BoxFuture<'static, ()> {
        let matches = job.child_session_id == self.during_child
            && self.during_once.swap(false, Ordering::SeqCst);
        let entered = self.during_entered.clone();
        let release = self.during_release.clone();
        Box::pin(async move {
            if matches {
                entered.add_permits(1);
                release
                    .acquire()
                    .await
                    .expect("release reservation barrier")
                    .forget();
            }
        })
    }
}

fn make_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
}

struct TestHarness {
    tool: SubAgentTool,
    adapter: Arc<ChildSessionAdapter>,
    storage: Arc<dyn Storage>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    parent_session_id: String,
    child_session_id: String,
    parent_rx: broadcast::Receiver<AgentEvent>,
    notification_service: Arc<bamboo_notification::NotificationService>,
    session_inbox: Arc<dyn SessionInboxPort>,
    activation: Arc<RecordingActivation>,
    activation_router: Arc<bamboo_engine::SessionActivationRouter>,
    project_store: Arc<bamboo_projects::ProjectStore>,
    workspace_path: PathBuf,
}

async fn build_test_harness() -> TestHarness {
    build_test_harness_with_resolver(None).await
}

async fn build_running_child_harness() -> TestHarness {
    build_test_harness_with_hook(None, None, true, None, true).await
}

async fn build_test_harness_with_resolver(
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
) -> TestHarness {
    build_test_harness_with_options(subagent_model_resolver, None).await
}

async fn build_test_harness_with_options(
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    workspace_resolver: Option<bamboo_agent_core::workspace_state::WorkspaceResolver>,
) -> TestHarness {
    build_test_harness_with_storage(subagent_model_resolver, workspace_resolver, false).await
}

async fn build_test_harness_with_storage(
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    workspace_resolver: Option<bamboo_agent_core::workspace_state::WorkspaceResolver>,
    use_v2_storage: bool,
) -> TestHarness {
    build_test_harness_with_hook(
        subagent_model_resolver,
        workspace_resolver,
        use_v2_storage,
        None,
        false,
    )
    .await
}

async fn build_test_harness_with_hook(
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    workspace_resolver: Option<bamboo_agent_core::workspace_state::WorkspaceResolver>,
    use_v2_storage: bool,
    launch_hook: Option<Arc<dyn bamboo_engine::execution::ChildRunLaunchHook>>,
    initial_child_running: bool,
) -> TestHarness {
    let bamboo_home = make_temp_dir("bamboo-sub-agent-test");
    tokio::fs::create_dir_all(&bamboo_home).await.unwrap();
    // The named-profile reader rejects symlinked path components. On macOS,
    // std::env::temp_dir() can start with /var, which aliases /private/var.
    let bamboo_home = tokio::fs::canonicalize(&bamboo_home).await.unwrap();
    let workspace_path = bamboo_home.join("workspace");
    tokio::fs::create_dir_all(&workspace_path).await.unwrap();
    let workspace_path = tokio::fs::canonicalize(workspace_path).await.unwrap();

    let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
    let project_store =
        Arc::new(bamboo_projects::ProjectStore::open(&bamboo_home).expect("Project store"));
    let storage: Arc<dyn Storage> = if use_v2_storage {
        session_store.clone()
    } else {
        let storage_dir = bamboo_home.join("storage");
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        let jsonl = bamboo_storage::JsonlStorage::new(&storage_dir);
        jsonl.init().await.unwrap();
        Arc::new(jsonl)
    };
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

    let metrics_storage = Arc::new(SqliteMetricsStorage::new(bamboo_home.join("metrics.db")));
    let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

    let sessions_cache: bamboo_engine::SessionCache = Arc::default();
    let agent_runners = Arc::new(RwLock::new(HashMap::new()));
    let session_event_senders = Arc::new(RwLock::new(HashMap::<
        String,
        broadcast::Sender<AgentEvent>,
    >::new()));

    let parent_session_id = "root-session".to_string();
    let child_session_id = "child-session".to_string();
    let (parent_tx, parent_rx) = broadcast::channel(1000);
    {
        let mut senders = session_event_senders.write().await;
        senders.insert(parent_session_id.clone(), parent_tx);
    }

    let mut parent = Session::new(parent_session_id.clone(), "gpt-5");
    parent.title = "Root".to_string();
    storage.save_session(&parent).await.unwrap();
    session_store.save_session(&parent).await.unwrap();

    let mut child = Session::new_child(
        child_session_id.clone(),
        parent_session_id.clone(),
        "gpt-5",
        "Child session",
    );
    child.metadata.insert(
        "last_run_status".to_string(),
        if initial_child_running {
            "running"
        } else {
            "completed"
        }
        .to_string(),
    );
    child.add_message(Message::system("child system"));
    child.add_message(Message::user("initial assignment"));
    child.add_message(Message::assistant("initial answer", None));
    storage.save_session(&child).await.unwrap();
    session_store.save_session(&child).await.unwrap();

    let session_inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
        session_store.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    ));
    let activation_router = bamboo_engine::SessionActivationRouter::new();
    let activation = Arc::new(RecordingActivation::default());
    let activation_port: Arc<dyn SessionActivationPort> = activation.clone();
    let session_messenger = Arc::new(bamboo_engine::SessionMessenger::new(
        storage.clone(),
        session_inbox.clone(),
        activation_port,
    ));

    let config = Arc::new(RwLock::new(bamboo_llm::Config::default()));
    let provider: Arc<dyn LLMProvider> = Arc::new(NoopProvider);
    let mut providers = HashMap::new();
    providers.insert("test".to_string(), provider.clone());
    let provider_registry = Arc::new(bamboo_llm::ProviderRegistry::new(
        providers,
        "test".to_string(),
    ));
    let provider_router = Arc::new(bamboo_llm::ProviderModelRouter::new(
        provider_registry.clone(),
    ));
    let agent_runtime = Arc::new(
        bamboo_engine::Agent::builder()
            .storage(storage.clone())
            .persistence(persistence.clone())
            .session_inbox(session_inbox.clone())
            .activation_router(activation_router.clone())
            .session_messenger(session_messenger.clone())
            .attachment_reader(session_store.clone())
            .skill_manager(Arc::new(SkillManager::new()))
            .metrics_collector(metrics_collector)
            .config(config.clone())
            .provider(provider)
            .default_tools(Arc::new(NoopToolExecutor))
            .build()
            .expect("test agent should be fully configured"),
    );

    // Real notification service + relay deps (not stubbed): the scheduler's
    // canonical launch hook owns observer setup for both queued child creates
    // and reserved idle SessionInbox activation.
    let notification_service = Arc::new(bamboo_notification::NotificationService::new(
        bamboo_home.join("notification_preferences.json"),
    ));
    let notification_relay_deps = crate::app_state::session_events::NotificationRelayDeps {
        notification_service: notification_service.clone(),
        session_event_senders: session_event_senders.clone(),
        session_watchers: crate::app_state::watchers::SessionWatchers::new(),
        config: config.clone(),
    };

    let completion_coordinator = Arc::new(bamboo_engine::ChildCompletionCoordinator::new(
        storage.clone(),
        persistence.clone(),
        sessions_cache.clone(),
        agent_runners.clone(),
        session_event_senders.clone(),
        agent_runtime.clone(),
        config.clone(),
        provider_registry,
        provider_router.clone(),
        bamboo_home.clone(),
        None,
    ));
    activation_router
        .set_spawner(completion_coordinator.clone())
        .await;
    let scheduler = Arc::new(SpawnScheduler::new(SpawnContext {
        agent: agent_runtime,
        tools: Arc::new(NoopToolExecutor),
        sessions_cache: sessions_cache.clone(),
        agent_runners: agent_runners.clone(),
        session_event_senders: session_event_senders.clone(),
        external_child_runner: Arc::new(NoopChildRunner),
        provider_router: Some(provider_router),
        app_data_dir: Some(bamboo_home.clone()),
        completion_handler: Some(completion_coordinator.clone()),
        child_run_launch_hook: launch_hook.or_else(|| {
            Some(Arc::new(
                crate::app_state::session_events::NotificationRelayLaunchHook::new(
                    notification_relay_deps,
                ),
            ))
        }),
        account_feed_inbox: None,
    }));
    completion_coordinator.set_spawn_scheduler(&scheduler).await;
    activation.set_delegate(activation_router.clone());

    let adapter = Arc::new(ChildSessionAdapter {
        session_store,
        storage: storage.clone(),
        persistence,
        session_messenger: Some(session_messenger),
        scheduler,
        sessions_cache,
        agent_runners: agent_runners.clone(),
        session_event_senders,
        subagent_model_resolver,
        config,
        project_store: Some(project_store.clone()),
        workspace_resolver: workspace_resolver.unwrap_or_else(
            bamboo_agent_core::workspace_state::WorkspaceResolver::from_process_globals,
        ),
        parent_wait_slots: Arc::new(dashmap::DashMap::new()),
        recovered_launches: Arc::new(dashmap::DashMap::new()),
    });
    let tool = SubAgentTool::new(adapter.clone(), adapter.clone());

    TestHarness {
        tool,
        adapter,
        storage,
        agent_runners,
        parent_session_id,
        child_session_id,
        parent_rx,
        notification_service,
        session_inbox,
        activation,
        activation_router,
        project_store,
        workspace_path,
    }
}

/// A scheduler/SessionInbox gate that makes the fast-completion ordering
/// deterministic: a child is allowed to activate only after its parent's
/// wait can already be read from durable storage. It can also fail a launch
/// at that exact boundary to exercise per-child wait compensation.
struct WaitOrderPort {
    inner: Arc<ChildSessionAdapter>,
    storage: Arc<dyn Storage>,
    fail_launch: AtomicBool,
    checked_launches: AtomicUsize,
    hold_first_enqueue: AtomicBool,
    hold_before_admission: AtomicBool,
    hold_message_delivery: AtomicBool,
    hold_running_probe: AtomicBool,
    hold_after_interrupt: AtomicBool,
    expect_wait_on_enqueue: AtomicBool,
    hold_wait_after_persist: AtomicBool,
    fail_wait_after_persist: AtomicBool,
    first_enqueue_entered: tokio::sync::Notify,
    release_first_enqueue: tokio::sync::Notify,
    message_delivery_entered: tokio::sync::Notify,
    release_message_delivery: tokio::sync::Notify,
    running_probe_entered: tokio::sync::Notify,
    release_running_probe: tokio::sync::Notify,
    interrupt_completed: tokio::sync::Notify,
    release_interrupt: tokio::sync::Notify,
    wait_persisted: tokio::sync::Notify,
    release_wait: tokio::sync::Notify,
    last_wait_child_id: StdRwLock<Option<String>>,
    last_admit_child_id: StdRwLock<Option<String>>,
    skip_successful_enqueue: AtomicBool,
    parent_load_count: AtomicUsize,
    clear_wait_after_second_parent_load: AtomicBool,
}

impl WaitOrderPort {
    fn new(inner: Arc<ChildSessionAdapter>, storage: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            storage,
            fail_launch: AtomicBool::new(false),
            checked_launches: AtomicUsize::new(0),
            hold_first_enqueue: AtomicBool::new(false),
            hold_before_admission: AtomicBool::new(false),
            hold_message_delivery: AtomicBool::new(false),
            hold_running_probe: AtomicBool::new(false),
            hold_after_interrupt: AtomicBool::new(false),
            expect_wait_on_enqueue: AtomicBool::new(true),
            hold_wait_after_persist: AtomicBool::new(false),
            fail_wait_after_persist: AtomicBool::new(false),
            first_enqueue_entered: tokio::sync::Notify::new(),
            release_first_enqueue: tokio::sync::Notify::new(),
            message_delivery_entered: tokio::sync::Notify::new(),
            release_message_delivery: tokio::sync::Notify::new(),
            running_probe_entered: tokio::sync::Notify::new(),
            release_running_probe: tokio::sync::Notify::new(),
            interrupt_completed: tokio::sync::Notify::new(),
            release_interrupt: tokio::sync::Notify::new(),
            wait_persisted: tokio::sync::Notify::new(),
            release_wait: tokio::sync::Notify::new(),
            last_wait_child_id: StdRwLock::new(None),
            last_admit_child_id: StdRwLock::new(None),
            skip_successful_enqueue: AtomicBool::new(false),
            parent_load_count: AtomicUsize::new(0),
            clear_wait_after_second_parent_load: AtomicBool::new(false),
        }
    }

    async fn assert_wait_armed(&self, parent_id: &str, child_id: &str) {
        let parent = self
            .storage
            .load_session(parent_id)
            .await
            .expect("load durable parent")
            .expect("parent exists");
        let wait = parent
            .agent_runtime_state
            .as_ref()
            .and_then(|state| state.waiting_for_children.as_ref())
            .expect("wait must be durable before child activation");
        assert!(
            wait.child_session_ids.iter().any(|id| id == child_id),
            "child {child_id} could complete before its parent wait was armed"
        );
        self.checked_launches.fetch_add(1, Ordering::SeqCst);
    }

    fn maybe_fail_launch(&self) -> Result<(), ChildSessionError> {
        if self.fail_launch.load(Ordering::SeqCst) {
            Err(ChildSessionError::Execution(
                "injected launch failure".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl ChildSessionPort for WaitOrderPort {
    async fn resolve_named_profile(
        &self,
        parent: &Session,
        name: &str,
    ) -> Result<Option<child_session::named_profile::ResolvedChildProfile>, ChildSessionError> {
        self.inner.resolve_named_profile(parent, name).await
    }

    async fn validate_required_child_context_route(
        &self,
        runtime_metadata: &HashMap<String, String>,
        subagent_type: &str,
    ) -> Result<(), ChildSessionError> {
        self.inner
            .validate_required_child_context_route(runtime_metadata, subagent_type)
            .await
    }

    async fn validate_child_workspace(
        &self,
        project_id: Option<&bamboo_domain::ProjectId>,
        requested_workspace: &str,
    ) -> Result<String, ChildSessionError> {
        self.inner
            .validate_child_workspace(project_id, requested_workspace)
            .await
    }

    fn publish_child_workspace(
        &self,
        session_id: &str,
        workspace: PathBuf,
        source: &str,
    ) -> PathBuf {
        self.inner
            .publish_child_workspace(session_id, workspace, source)
    }

    async fn load_root_session(&self, root_id: &str) -> Result<Session, ChildSessionError> {
        let parent = self.inner.load_root_session(root_id).await?;
        if self
            .clear_wait_after_second_parent_load
            .load(Ordering::SeqCst)
            && self.parent_load_count.fetch_add(1, Ordering::SeqCst) == 1
        {
            let child_id = parent
                .agent_runtime_state
                .as_ref()
                .and_then(|runtime| runtime.waiting_for_children.as_ref())
                .and_then(|wait| wait.child_session_ids.first())
                .cloned()
                .expect("test parent has a pre-existing child wait");
            self.inner
                .rollback_parent_wait_for_child(root_id, &child_id)
                .await?;
        }
        Ok(parent)
    }

    async fn load_child_for_parent(
        &self,
        parent_id: &str,
        child_id: &str,
    ) -> Result<Session, ChildSessionError> {
        self.inner.load_child_for_parent(parent_id, child_id).await
    }

    async fn save_child_session(&self, child: &mut Session) -> Result<(), ChildSessionError> {
        self.inner.save_child_session(child).await
    }

    async fn update_child_session(
        &self,
        parent_id: &str,
        child_id: &str,
        update: child_session::ChildSessionUpdate,
    ) -> Result<(Session, usize), ChildSessionError> {
        self.inner
            .update_child_session(parent_id, child_id, update)
            .await
    }

    async fn append_draft_child_message(
        &self,
        parent_id: &str,
        child_id: &str,
        message: &str,
    ) -> Result<Session, ChildSessionError> {
        self.inner
            .append_draft_child_message(parent_id, child_id, message)
            .await
    }

    async fn save_child_session_authoritative_flags(
        &self,
        child: &mut Session,
    ) -> Result<(), ChildSessionError> {
        self.inner
            .save_child_session_authoritative_flags(child)
            .await
    }

    async fn send_session_message(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        message: &str,
        idempotency_key: Option<&str>,
    ) -> Result<child_session::ChildSessionMessageDelivery, ChildSessionError> {
        self.send_session_message_with_gate(
            source_session_id,
            target_session_id,
            message,
            idempotency_key,
            None,
        )
        .await
    }

    async fn send_session_message_with_gate(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        message: &str,
        idempotency_key: Option<&str>,
        gate: Option<&bamboo_domain::AdmissionGate>,
    ) -> Result<child_session::ChildSessionMessageDelivery, ChildSessionError> {
        self.assert_wait_armed(source_session_id, target_session_id)
            .await;
        if self.hold_message_delivery.swap(false, Ordering::SeqCst) {
            self.message_delivery_entered.notify_one();
            self.release_message_delivery.notified().await;
        }
        self.maybe_fail_launch()?;
        self.inner
            .send_session_message_with_gate(
                source_session_id,
                target_session_id,
                message,
                idempotency_key,
                gate,
            )
            .await
    }

    async fn is_child_running(&self, child_id: &str) -> bool {
        let running = self.inner.is_child_running(child_id).await;
        if self.hold_running_probe.swap(false, Ordering::SeqCst) {
            self.running_probe_entered.notify_one();
            self.release_running_probe.notified().await;
        }
        running
    }

    async fn list_children(&self, parent_id: &str) -> Vec<ChildSessionEntry> {
        self.inner.list_children(parent_id).await
    }

    async fn enqueue_child_run(
        &self,
        parent: &Session,
        child: &Session,
    ) -> Result<(), ChildSessionError> {
        self.admit_child_run(parent, child, None).await.map(|_| ())
    }

    async fn admit_child_run(
        &self,
        parent: &Session,
        child: &Session,
        gate: Option<&bamboo_domain::AdmissionGate>,
    ) -> Result<bamboo_domain::AdmissionCommit<()>, ChildSessionError> {
        *self.last_admit_child_id.write().unwrap() = Some(child.id.clone());
        if self.expect_wait_on_enqueue.load(Ordering::SeqCst) {
            self.assert_wait_armed(&parent.id, &child.id).await;
        }
        if self.hold_before_admission.swap(false, Ordering::SeqCst) {
            self.first_enqueue_entered.notify_one();
            self.release_first_enqueue.notified().await;
        }
        if self.hold_first_enqueue.swap(false, Ordering::SeqCst) {
            self.first_enqueue_entered.notify_one();
            self.release_first_enqueue.notified().await;
            return Err(ChildSessionError::Execution(
                "injected first launch failure".to_string(),
            ));
        }
        self.maybe_fail_launch()?;
        if self.skip_successful_enqueue.load(Ordering::SeqCst) {
            return Ok(bamboo_domain::AdmissionCommit::Committed(()));
        }
        self.inner.admit_child_run(parent, child, gate).await
    }

    async fn cancel_child_run_and_wait(&self, child_id: &str) -> Result<(), ChildSessionError> {
        self.inner.cancel_child_run_and_wait(child_id).await?;
        if self.hold_after_interrupt.swap(false, Ordering::SeqCst) {
            self.interrupt_completed.notify_one();
            self.release_interrupt.notified().await;
        }
        Ok(())
    }

    async fn delete_child_session(
        &self,
        parent_id: &str,
        child_id: &str,
    ) -> Result<DeleteChildResult, ChildSessionError> {
        self.inner.delete_child_session(parent_id, child_id).await
    }

    async fn get_child_runner_info(&self, child_id: &str) -> Option<ChildRunnerInfo> {
        self.inner.get_child_runner_info(child_id).await
    }

    async fn register_parent_wait_for_child(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        tool_call_id: Option<&str>,
    ) -> Result<(), ChildSessionError> {
        *self.last_wait_child_id.write().unwrap() = Some(child_session_id.to_string());
        self.inner
            .register_parent_wait_for_child(parent_session_id, child_session_id, tool_call_id)
            .await?;
        if self.hold_wait_after_persist.swap(false, Ordering::SeqCst) {
            self.wait_persisted.notify_one();
            self.release_wait.notified().await;
        }
        if self.fail_wait_after_persist.swap(false, Ordering::SeqCst) {
            return Err(ChildSessionError::Execution(
                "injected parent wait acknowledgement failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn rollback_parent_wait_for_child(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
    ) -> Result<(), ChildSessionError> {
        self.inner
            .rollback_parent_wait_for_child(parent_session_id, child_session_id)
            .await
    }

    async fn register_parent_wait_for_children(
        &self,
        parent_session_id: &str,
        child_session_ids: &[String],
        policy: ChildWaitPolicy,
    ) -> Result<usize, ChildSessionError> {
        self.inner
            .register_parent_wait_for_children(parent_session_id, child_session_ids, policy)
            .await
    }

    async fn register_parent_wait_for_children_tagged(
        &self,
        parent_session_id: &str,
        child_session_ids: &[String],
        policy: ChildWaitPolicy,
        tool_call_id: &str,
    ) -> Result<usize, ChildSessionError> {
        self.inner
            .register_parent_wait_for_children_tagged(
                parent_session_id,
                child_session_ids,
                policy,
                tool_call_id,
            )
            .await
    }

    async fn active_child_ids(&self, parent_session_id: &str) -> Vec<String> {
        self.inner.active_child_ids(parent_session_id).await
    }

    async fn terminal_child_ids(
        &self,
        parent_session_id: &str,
        candidates: &[String],
    ) -> Vec<(String, String)> {
        self.inner
            .terminal_child_ids(parent_session_id, candidates)
            .await
    }

    async fn find_resident_child(
        &self,
        root_session_id: &str,
        resident_name: &str,
    ) -> Option<String> {
        self.inner
            .find_resident_child(root_session_id, resident_name)
            .await
    }

    async fn ensure_child_indexed(&self, child_session_id: &str) {
        self.inner.ensure_child_indexed(child_session_id).await;
    }
}

fn synchronous_launch_args(action: &str, child_id: &str, workspace: &str) -> serde_json::Value {
    match action {
        "create" => json!({
            "action": "create",
            "title": "Fast child",
            "responsibility": "Return immediately",
            "prompt": "Return immediately",
            "workspace": workspace,
            "wait": true,
        }),
        "update" => json!({
            "action": "update",
            "child_session_id": child_id,
            "responsibility": "Return immediately",
            "subagent_type": "worker",
            "prompt": "Return immediately",
            "auto_run": true,
        }),
        "run" => json!({"action": "run", "child_session_id": child_id}),
        "send_message" => json!({
            "action": "send_message",
            "child_session_id": child_id,
            "message": "Return immediately",
        }),
        other => panic!("unexpected synchronous action: {other}"),
    }
}

#[tokio::test]
async fn synchronous_subagent_paths_arm_wait_before_a_fast_child_can_activate() {
    for action in ["create", "update", "run", "send_message"] {
        let harness = build_test_harness_with_storage(None, None, true).await;
        let port = Arc::new(WaitOrderPort::new(
            harness.adapter.clone(),
            harness.storage.clone(),
        ));
        let tool = SubAgentTool::new(port.clone(), harness.adapter.clone());
        let args = synchronous_launch_args(
            action,
            &harness.child_session_id,
            &harness.workspace_path.to_string_lossy(),
        );
        let result = invoke_completed(
            &tool,
            args,
            subagent_test_ctx(&harness.parent_session_id, action),
        )
        .await
        .unwrap_or_else(|error| panic!("{action} launch failed: {error}"));
        assert_eq!(
            result.display_preference.as_deref(),
            Some("runtime_control:waiting_for_children"),
            "{action} must suspend after launch"
        );
        assert_eq!(
            port.checked_launches.load(Ordering::SeqCst),
            1,
            "{action} must cross the checked activation gate exactly once"
        );
    }
}

#[tokio::test]
async fn fast_synchronous_child_completion_clears_wait_and_queues_resume() {
    let mut harness = build_test_harness().await;
    let result = invoke_completed(
        &harness.tool,
        synchronous_launch_args(
            "create",
            &harness.child_session_id,
            &harness.workspace_path.to_string_lossy(),
        ),
        subagent_test_ctx(&harness.parent_session_id, "fast-completion"),
    )
    .await
    .expect("launch fast synchronous child");
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    let child_id = payload["child_session_id"].as_str().unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match harness.parent_rx.recv().await {
                Ok(AgentEvent::SubAgentCompleted {
                    child_session_id, ..
                }) if child_session_id == child_id => break,
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => panic!("parent stream closed"),
            }
        }
    })
    .await
    .expect("fast child must complete before the watchdog interval");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let parent = harness
                .storage
                .load_session(&harness.parent_session_id)
                .await
                .unwrap()
                .unwrap();
            if parent
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.waiting_for_children.as_ref())
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fast child completion must clear parent wait without watchdog");
    let resume_backlog = harness
        .session_inbox
        .inspect(&harness.parent_session_id)
        .await
        .expect("inspect parent resume inbox");
    assert!(
        resume_backlog.generation > 0,
        "completion must admit a parent resume message"
    );
}

#[tokio::test]
async fn concurrent_same_child_run_failure_cannot_rollback_successor_wait() {
    let harness = build_test_harness().await;
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.hold_first_enqueue.store(true, Ordering::SeqCst);
    port.skip_successful_enqueue.store(true, Ordering::SeqCst);
    let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
    let parent_id = harness.parent_session_id.clone();
    let child_id = harness.child_session_id.clone();
    let workspace = harness.workspace_path.to_string_lossy().to_string();

    let first_entered = port.first_enqueue_entered.notified();
    let first_tool = tool.clone();
    let first_child = child_id.clone();
    let first_workspace = workspace.clone();
    let first_parent = parent_id.clone();
    let first = tokio::spawn(async move {
        invoke_completed(
            &first_tool,
            synchronous_launch_args("run", &first_child, &first_workspace),
            subagent_test_ctx(&first_parent, "same-child-first"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), first_entered)
        .await
        .expect("first run reaches enqueue after arming wait");

    let second_tool = tool;
    let second_child = child_id.clone();
    let second_started = Arc::new(tokio::sync::Notify::new());
    let second_started_wait = second_started.notified();
    let second_started_signal = second_started.clone();
    let second = tokio::spawn(async move {
        second_started_signal.notify_one();
        invoke_completed(
            &second_tool,
            synchronous_launch_args("run", &second_child, &workspace),
            subagent_test_ctx(&parent_id, "same-child-second"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), second_started_wait)
        .await
        .expect("second run task starts while first enqueue is held");
    let reached_second_enqueue = tokio::time::timeout(Duration::from_millis(150), async {
        loop {
            if port.checked_launches.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        reached_second_enqueue.is_err(),
        "second same-child run must wait until first launch compensates"
    );

    port.release_first_enqueue.notify_one();
    let first_error = first.await.unwrap().unwrap_err();
    assert!(format!("{first_error}").contains("injected first launch failure"));
    let second_result = second
        .await
        .unwrap()
        .expect("second run queues after rollback");
    assert_eq!(
        second_result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );
    let durable = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(durable
        .agent_runtime_state
        .as_ref()
        .and_then(|runtime| runtime.waiting_for_children.as_ref())
        .is_some_and(|wait| wait.child_session_ids.contains(&child_id)));
}

#[tokio::test]
async fn run_rechecks_terminal_child_after_already_running_race() {
    let harness = build_test_harness().await;
    // Simulate the narrow race where the runner is still published as running
    // while the durable child status has already become terminal.
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    harness
        .agent_runners
        .write()
        .await
        .insert(harness.child_session_id.clone(), runner);

    let result = invoke_completed(
        &harness.tool,
        json!({"action": "run", "child_session_id": harness.child_session_id}),
        subagent_test_ctx(&harness.parent_session_id, "already-running-race"),
    )
    .await
    .expect("already-running run should return");
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"], "already_terminal");
    assert_eq!(payload["last_run_status"], "completed");
    assert_ne!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .is_none());
}

#[tokio::test]
async fn run_terminal_recheck_preserves_an_existing_wait_for_the_same_child() {
    let harness = build_test_harness().await;
    harness
        .adapter
        .register_parent_wait_for_child(
            &harness.parent_session_id,
            &harness.child_session_id,
            Some("original-wait"),
        )
        .await
        .expect("seed an existing child wait");

    // The runner is still published as active, while the durable child status
    // has become terminal. This Run call must not remove the earlier wait.
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    harness
        .agent_runners
        .write()
        .await
        .insert(harness.child_session_id.clone(), runner);

    let result = invoke_completed(
        &harness.tool,
        json!({"action": "run", "child_session_id": harness.child_session_id}),
        subagent_test_ctx(&harness.parent_session_id, "second-run"),
    )
    .await
    .expect("already-running run should return");
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"], "already_terminal");
    assert_ne!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );

    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let wait = parent
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .expect("earlier child wait must remain armed");
    assert_eq!(wait.child_session_ids, [harness.child_session_id]);
    assert_eq!(
        wait.registered_by_tool_call_id.as_deref(),
        Some("original-wait")
    );
}

#[tokio::test]
async fn run_does_not_rearm_wait_cleared_after_parent_snapshot() {
    let harness = build_test_harness().await;
    harness
        .adapter
        .register_parent_wait_for_child(
            &harness.parent_session_id,
            &harness.child_session_id,
            Some("original-wait"),
        )
        .await
        .unwrap();
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    harness
        .agent_runners
        .write()
        .await
        .insert(harness.child_session_id.clone(), runner);

    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.clear_wait_after_second_parent_load
        .store(true, Ordering::SeqCst);
    let tool = SubAgentTool::new(port, harness.adapter.clone());
    let result = invoke_completed(
        &tool,
        json!({"action": "run", "child_session_id": harness.child_session_id}),
        subagent_test_ctx(
            &harness.parent_session_id,
            "cleared-between-snapshot-register",
        ),
    )
    .await
    .expect("already-running run returns after completion");
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"], "already_terminal");
    assert_ne!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .and_then(|runtime| runtime.waiting_for_children.as_ref())
        .is_none());
}

#[tokio::test]
async fn failed_synchronous_launch_rolls_back_only_its_child_wait() {
    for action in ["create", "update", "run", "send_message"] {
        let harness = build_test_harness_with_storage(None, None, true).await;
        harness
            .adapter
            .register_parent_wait_for_child(&harness.parent_session_id, "sibling", None)
            .await
            .expect("seed sibling wait");
        let port = Arc::new(WaitOrderPort::new(
            harness.adapter.clone(),
            harness.storage.clone(),
        ));
        port.fail_launch.store(true, Ordering::SeqCst);
        let tool = SubAgentTool::new(port.clone(), harness.adapter.clone());
        let args = synchronous_launch_args(
            action,
            &harness.child_session_id,
            &harness.workspace_path.to_string_lossy(),
        );
        let error = invoke_completed(
            &tool,
            args,
            subagent_test_ctx(&harness.parent_session_id, action),
        )
        .await
        .expect_err("injected launch failure must fail tool");
        assert!(error.to_string().contains("injected launch failure"));
        assert_eq!(port.checked_launches.load(Ordering::SeqCst), 1);
        assert!(
            harness
                .adapter
                .active_child_ids(&harness.parent_session_id)
                .await
                .is_empty(),
            "{action} failed launch must not be rediscovered by the end-of-turn auto-wait"
        );
        let parent = harness
            .storage
            .load_session(&harness.parent_session_id)
            .await
            .expect("load parent")
            .expect("parent exists");
        let wait = parent
            .agent_runtime_state
            .as_ref()
            .and_then(|state| state.waiting_for_children.as_ref())
            .expect("sibling wait survives rollback");
        assert_eq!(
            wait.child_session_ids,
            ["sibling"],
            "{action} must roll back only its own child wait"
        );
    }
}

#[tokio::test]
async fn cancelled_synchronous_launch_still_compensates_a_rejected_enqueue() {
    for action in ["create", "update", "run"] {
        let harness = build_test_harness_with_storage(None, None, true).await;
        let port = Arc::new(WaitOrderPort::new(
            harness.adapter.clone(),
            harness.storage.clone(),
        ));
        port.hold_first_enqueue.store(true, Ordering::SeqCst);
        let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
        let args = synchronous_launch_args(
            action,
            &harness.child_session_id,
            &harness.workspace_path.to_string_lossy(),
        );
        let parent_id = harness.parent_session_id.clone();
        let entered = port.first_enqueue_entered.notified();
        let outer = tokio::spawn(async move {
            invoke_completed(&tool, args, subagent_test_ctx(&parent_id, action)).await
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .expect("launch owner must reach enqueue with durable wait armed");
        outer.abort();
        assert!(outer.await.unwrap_err().is_cancelled());
        port.release_first_enqueue.notify_one();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let parent = harness
                    .storage
                    .load_session(&harness.parent_session_id)
                    .await
                    .unwrap()
                    .unwrap();
                let wait_cleared = parent
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|state| state.waiting_for_children.as_ref())
                    .is_none();
                let active = harness
                    .adapter
                    .active_child_ids(&harness.parent_session_id)
                    .await;
                if wait_cleared && active.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{action} cancellation stranded a pending child or wait"));
        assert_eq!(port.checked_launches.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancelled_run_clears_wait_without_releasing_precommit_scheduler_block() {
    let harness = build_test_harness().await;
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.hold_before_admission.store(true, Ordering::SeqCst);
    let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
    let parent_id = harness.parent_session_id.clone();
    let child_id = harness.child_session_id.clone();
    let entered = port.first_enqueue_entered.notified();
    let outer = tokio::spawn(async move {
        invoke_completed(
            &tool,
            json!({"action": "run", "child_session_id": child_id}),
            subagent_test_ctx(&parent_id, "run-precommit-cancel"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("scheduler must reach a pre-commit block after arming the wait");
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let parent = harness
                .storage
                .load_session(&harness.parent_session_id)
                .await
                .unwrap()
                .unwrap();
            let wait_cleared = parent
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.waiting_for_children.as_ref())
                .is_none();
            let active = harness
                .adapter
                .active_child_ids(&harness.parent_session_id)
                .await;
            if wait_cleared && active.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancellation must clear the wait without releasing scheduler");
}

#[tokio::test]
async fn compact_chat_cancellation_clears_wait_without_releasing_precommit_inbox_block() {
    let harness = build_test_harness().await;
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.hold_message_delivery.store(true, Ordering::SeqCst);
    let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
    let parent_id = harness.parent_session_id.clone();
    let child_id = harness.child_session_id.clone();
    let entered = port.message_delivery_entered.notified();
    let outer = tokio::spawn(async move {
        invoke_completed(
            &tool,
            json!({
                "target": child_id,
                "message": "Do this only if admitted",
            }),
            subagent_test_ctx(&parent_id, "send-message-precommit-cancel"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("delivery must reach a pre-commit block after arming the wait");
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let parent = harness
                .storage
                .load_session(&harness.parent_session_id)
                .await
                .unwrap()
                .unwrap();
            if parent
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.waiting_for_children.as_ref())
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancellation must clear the parent wait without releasing Inbox");
    let backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
}

#[tokio::test]
async fn cancelled_background_create_cannot_admit_a_late_child_job() {
    let mut harness = build_test_harness().await;
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.expect_wait_on_enqueue.store(false, Ordering::SeqCst);
    port.hold_before_admission.store(true, Ordering::SeqCst);
    let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
    let parent_id = harness.parent_session_id.clone();
    let workspace = harness.workspace_path.to_string_lossy().to_string();
    let entered = port.first_enqueue_entered.notified();
    let outer = tokio::spawn(async move {
        invoke_completed(
            &tool,
            json!({
                "action": "create",
                "title": "Cancelled background child",
                "responsibility": "Return immediately",
                "prompt": "Return immediately",
                "workspace": workspace,
                "wait": false,
            }),
            subagent_test_ctx(&parent_id, "cancelled-background-create"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("prepared child must reach the scheduler admission boundary");
    let child_id = port.last_admit_child_id.read().unwrap().clone().unwrap();
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());
    port.release_first_enqueue.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let child = harness
                .storage
                .load_session(&child_id)
                .await
                .unwrap()
                .unwrap();
            if child.last_run_status().as_deref() == Some("error") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled background child must become terminal");
    assert!(harness
        .adapter
        .active_child_ids(&harness.parent_session_id)
        .await
        .is_empty());
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .is_none());
    while let Ok(event) = harness.parent_rx.try_recv() {
        assert!(
            !matches!(
                event,
                AgentEvent::SubAgentStarted { child_session_id, .. }
                    if child_session_id == child_id
            ),
            "a cancelled pre-admission child must not publish SubAgentStarted"
        );
    }
}

#[tokio::test]
async fn cancelled_before_delivery_rolls_back_wait_without_sending() {
    for action in ["create", "update", "run", "send_message"] {
        let harness = build_test_harness_with_storage(None, None, true).await;
        let port = Arc::new(WaitOrderPort::new(
            harness.adapter.clone(),
            harness.storage.clone(),
        ));
        port.hold_wait_after_persist.store(true, Ordering::SeqCst);
        let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
        let args = synchronous_launch_args(
            action,
            &harness.child_session_id,
            &harness.workspace_path.to_string_lossy(),
        );
        let parent_id = harness.parent_session_id.clone();
        let wait_persisted = port.wait_persisted.notified();
        let mut outer = tokio::spawn(async move {
            invoke_completed(&tool, args, subagent_test_ctx(&parent_id, action)).await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = wait_persisted => {}
                outcome = &mut outer => panic!("{action} ended before persisting its parent wait: {outcome:?}"),
            }
        })
            .await
            .unwrap_or_else(|_| panic!("{action} parent wait must be persisted before cancellation"));
        outer.abort();
        assert!(outer.await.unwrap_err().is_cancelled());
        port.release_wait.notify_one();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let parent = harness
                    .storage
                    .load_session(&harness.parent_session_id)
                    .await
                    .unwrap()
                    .unwrap();
                let wait_cleared = parent
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|state| state.waiting_for_children.as_ref())
                    .is_none();
                if wait_cleared {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{action} cancellation did not clear its durable wait"));
        assert_eq!(port.checked_launches.load(Ordering::SeqCst), 0);
        assert!(harness
            .adapter
            .active_child_ids(&harness.parent_session_id)
            .await
            .is_empty());
        if action != "send_message" {
            let child_id = port.last_wait_child_id.read().unwrap().clone().unwrap();
            let child = harness
                .storage
                .load_session(&child_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                child.last_run_status().as_deref(),
                Some("error"),
                "{action} left its prepared child non-terminal"
            );
        }
        let child_backlog = harness
            .session_inbox
            .inspect(&harness.child_session_id)
            .await
            .unwrap();
        assert_eq!(child_backlog.generation, 0, "{action} delivered a message");
        let parent_backlog = harness
            .session_inbox
            .inspect(&harness.parent_session_id)
            .await
            .unwrap();
        assert_eq!(parent_backlog.generation, 0, "{action} enqueued a child");
    }
}

#[tokio::test]
async fn failed_parent_wait_acknowledgement_marks_prepared_child_terminal() {
    let harness = build_test_harness().await;
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.fail_wait_after_persist.store(true, Ordering::SeqCst);
    let tool = SubAgentTool::new(port.clone(), harness.adapter.clone());
    let error = invoke_completed(
        &tool,
        synchronous_launch_args(
            "create",
            &harness.child_session_id,
            &harness.workspace_path.to_string_lossy(),
        ),
        subagent_test_ctx(&harness.parent_session_id, "failed-wait-ack"),
    )
    .await
    .expect_err("uncertain wait acknowledgement must not launch child");
    assert!(error
        .to_string()
        .contains("injected parent wait acknowledgement failure"));
    assert_eq!(port.checked_launches.load(Ordering::SeqCst), 0);
    assert!(harness
        .adapter
        .active_child_ids(&harness.parent_session_id)
        .await
        .is_empty());
    let child_id = port.last_wait_child_id.read().unwrap().clone().unwrap();
    let child = harness
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.last_run_status().as_deref(), Some("error"));
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .is_none());
}

#[tokio::test]
async fn cancelled_send_message_finishes_activation_failure_after_inbox_admission() {
    let harness = build_test_harness().await;
    harness.activation.pause_next.store(true, Ordering::SeqCst);
    harness.activation.fail_next();
    let tool = Arc::new(SubAgentTool::new(
        harness.adapter.clone(),
        harness.adapter.clone(),
    ));
    let parent_id = harness.parent_session_id.clone();
    let child_id = harness.child_session_id.clone();
    let entered = harness.activation.activation_entered.notified();
    let outer = tokio::spawn(async move {
        invoke_completed(
            &tool,
            json!({
                "action": "send_message",
                "child_session_id": child_id,
                "message": "retry after the activation failure"
            }),
            subagent_test_ctx(&parent_id, "cancelled-send-message"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("message must reach activation after durable inbox admission");
    let admitted = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(admitted.generation, 1);
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());
    harness.activation.release_activation.notify_one();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let parent = harness
                .storage
                .load_session(&harness.parent_session_id)
                .await
                .unwrap()
                .unwrap();
            if parent
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.waiting_for_children.as_ref())
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("activation failure after caller cancellation must clear unsatisfiable wait");
    let backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.generation, 1);
    assert_eq!(backlog.pending + backlog.claimed, 1);
}

// -----------------------------------------------------------------------
// ④ Batched parent-wait registration
// -----------------------------------------------------------------------

#[tokio::test]
async fn plan_workspace_selection_uses_durable_metadata_and_current_project_default() {
    use bamboo_engine::project_context::{WorkspaceSource, WORKSPACE_SOURCE_METADATA_KEY};
    let harness = build_test_harness().await;
    let mut parent = harness
        .adapter
        .load_root_session(&harness.parent_session_id)
        .await
        .unwrap();
    let selected = tempfile::tempdir().unwrap();
    let selected_path = selected
        .path()
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    parent.workspace = Some(harness.workspace_path.to_string_lossy().into_owned());
    parent.set_workspace_path_meta(&selected_path);
    // A prior publication cache and legacy field cannot beat durable metadata.
    bamboo_agent_core::workspace_state::publish_resolved_workspace(
        &parent.id,
        harness.workspace_path.clone(),
    );
    let (path, source) = harness
        .adapter
        .resolve_child_workspace(&parent, None)
        .await
        .unwrap();
    assert_eq!(path, selected_path);
    assert_eq!(source, WorkspaceSource::Session);
    let (path, source) = harness
        .adapter
        .resolve_child_workspace(&parent, Some(harness.workspace_path.to_str().unwrap()))
        .await
        .unwrap();
    assert_eq!(path, harness.workspace_path.to_string_lossy());
    assert_eq!(source, WorkspaceSource::Explicit);
    parent.metadata.remove("workspace_path");
    parent.runtime_metadata.as_mut().unwrap().workspace_path = None;
    let (path, source) = harness
        .adapter
        .resolve_child_workspace(&parent, None)
        .await
        .unwrap();
    assert_eq!(path, harness.workspace_path.to_string_lossy());
    assert_eq!(source, WorkspaceSource::Session);

    let project = harness
        .project_store
        .create_with_project_path("Plan Project", None, selected_path.as_str(), Vec::new())
        .unwrap();
    parent.set_project_id_meta(project.id.to_string());
    parent.set_workspace_path_meta(&selected_path);
    parent.metadata.insert(
        WORKSPACE_SOURCE_METADATA_KEY.into(),
        "project_default".into(),
    );
    let moved = tempfile::tempdir().unwrap();
    let project = harness
        .project_store
        .update_with_project_path(
            &project.id,
            project.revision,
            moved.path().to_str().unwrap(),
            |_| Ok(()),
        )
        .unwrap();
    let (path, source) = harness
        .adapter
        .resolve_child_workspace(&parent, None)
        .await
        .unwrap();
    assert_eq!(Some(path.as_str()), project.project_path.as_deref());
    assert_eq!(source, WorkspaceSource::ProjectDefault);
    // Assigned parents without durable metadata also use the current Project,
    // even if an older legacy workspace field remains populated.
    parent.metadata.remove("workspace_path");
    parent.metadata.remove(WORKSPACE_SOURCE_METADATA_KEY);
    parent.runtime_metadata.as_mut().unwrap().workspace_path = None;
    let (path, source) = harness
        .adapter
        .resolve_child_workspace(&parent, None)
        .await
        .unwrap();
    assert_eq!(Some(path.as_str()), project.project_path.as_deref());
    assert_eq!(source, WorkspaceSource::ProjectDefault);
}

#[tokio::test]
async fn plan_rejects_missing_invalid_and_foreign_workspace_before_child_persistence() {
    let harness = build_test_harness_with_storage(None, None, false).await;
    let tool = PlanTool::new(harness.adapter.clone(), harness.adapter.clone());
    let mut parent = harness
        .adapter
        .load_root_session(&harness.parent_session_id)
        .await
        .unwrap();
    parent.workspace = None;
    // Deliberately leave a cached fallback present: it supplies no authority.
    bamboo_agent_core::workspace_state::publish_resolved_workspace(
        &parent.id,
        harness.workspace_path.clone(),
    );
    let storage_dir = harness.workspace_path.parent().unwrap().join("storage");
    let before = std::fs::read_dir(&storage_dir).unwrap().count();
    let invalid = tempfile::NamedTempFile::new().unwrap();
    let foreign = tempfile::tempdir().unwrap();
    harness
        .project_store
        .create_with_project_path(
            "Foreign Plan Project",
            None,
            foreign.path().to_string_lossy(),
            Vec::new(),
        )
        .unwrap();
    for workspace in [None, Some(invalid.path()), Some(foreign.path())] {
        if let Some(path) = workspace {
            parent.set_workspace_path_meta(path.to_string_lossy());
        } else {
            parent.metadata.remove("workspace_path");
            if let Some(metadata) = parent.runtime_metadata.as_mut() {
                metadata.workspace_path = None;
            }
        }
        harness.storage.save_session(&parent).await.unwrap();
        let error = invoke_plan_completed(
            &tool,
            json!({"task":"Inspect without writes"}),
            subagent_test_ctx(&parent.id, "tc_invalid_plan_workspace"),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)), "{error:?}");
        assert_eq!(
            std::fs::read_dir(&storage_dir).unwrap().count(),
            before,
            "invalid default must not persist a Child"
        );
        assert!(!harness
            .storage
            .load_session(&parent.id)
            .await
            .unwrap()
            .unwrap()
            .agent_runtime_state
            .as_ref()
            .is_some_and(|runtime| runtime.waiting_for_children.is_some()));
    }
    // Explicit override remains subject to the same foreign ownership check.
    let error = invoke_plan_completed(
        &tool,
        json!({"task":"Inspect", "workspace":foreign.path()}),
        subagent_test_ctx(&parent.id, "tc_explicit_foreign_plan"),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ToolError::InvalidArguments(_)));
    assert_eq!(std::fs::read_dir(&storage_dir).unwrap().count(), before);

    // An assigned legacy path is not a canonical Project selection. Neither
    // a relative cwd nor an older valid path repairs an unconfigured Project.
    let unconfigured = harness
        .project_store
        .create("Unconfigured Plan Project", None)
        .unwrap();
    parent.set_project_id_meta(unconfigured.id.to_string());
    parent.metadata.remove("workspace_path");
    parent.runtime_metadata.as_mut().unwrap().workspace_path = None;
    for legacy in [".", harness.workspace_path.to_str().unwrap()] {
        parent.workspace = Some(legacy.into());
        harness.storage.save_session(&parent).await.unwrap();
        let error = invoke_plan_completed(
            &tool,
            json!({"task":"Inspect without writes"}),
            subagent_test_ctx(&parent.id, "tc_assigned_legacy_plan"),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)), "{error:?}");
        assert!(error.to_string().contains("project_path"), "{error}");
        assert_eq!(std::fs::read_dir(&storage_dir).unwrap().count(), before);
    }
}

#[tokio::test]
async fn plan_creates_one_typed_read_only_child_and_registers_a_noninteractive_wait() {
    let resolver: crate::tools::SubagentModelResolver = Arc::new(|subagent_type: String| {
        Box::pin(async move {
            assert_eq!(subagent_type, "planner");
            Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "gpt-planner",
            ))
        })
    });
    let harness = build_test_harness_with_storage(Some(resolver), None, false).await;

    // A permissive root is useful regression pressure: Plan must preserve the
    // root's posture while the child receives an independent read-only overlay.
    let mut root = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    root.agent_runtime_state
        .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
        .set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
    harness.storage.save_session(&root).await.unwrap();
    harness
        .adapter
        .session_store
        .save_session(&root)
        .await
        .unwrap();

    let tool = PlanTool::new(harness.adapter.clone(), harness.adapter.clone());
    let result = invoke_plan_completed(
        &tool,
        json!({
            "task": "Inspect the session execution path and design a safe migration.",
            "title": "Plan session migration",
            "workspace": harness.workspace_path.to_string_lossy(),
            "fork_last_messages": 2
        }),
        subagent_test_ctx(&harness.parent_session_id, "tc_plan_delegate"),
    )
    .await
    .expect("Plan should delegate to one child");

    assert_eq!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"], "waiting_for_planner");
    assert_eq!(payload["runtime_control"], "waiting_for_children");
    assert_eq!(payload["wait_for"], "all");
    assert_eq!(payload["subagent_type"], "planner");
    assert_eq!(payload["model"], "gpt-planner");
    assert_eq!(payload["read_only"], true);
    assert!(
        payload.get("awaiting_user_input").is_none(),
        "delegated planning must never request a mode-switch response"
    );
    assert!(payload["note"]
        .as_str()
        .is_some_and(|note| note.contains("resume automatically")));
    let child_id = payload["child_session_id"]
        .as_str()
        .expect("planner child id");

    let child = harness
        .storage
        .load_session(child_id)
        .await
        .unwrap()
        .expect("planner child persisted");
    assert_eq!(child.subagent_type().as_deref(), Some("planner"));
    assert_eq!(
        child.metadata.get("runtime.kind").map(String::as_str),
        Some("external")
    );
    assert_eq!(
        child.metadata.get("external.protocol").map(String::as_str),
        Some("actor")
    );
    assert_eq!(
        child.metadata.get("external.agent_id").map(String::as_str),
        Some(bamboo_engine::external_agents::config::LOCAL_ACTOR_AGENT_ID)
    );
    let child_runtime = child
        .agent_runtime_state
        .as_ref()
        .expect("typed planner runtime state");
    assert!(child_runtime.read_only);
    assert_eq!(
        child_runtime.effective_permission_mode(),
        bamboo_domain::SessionPermissionMode::Auto,
        "requested mode remains auditable even though read-only wins effectively"
    );
    assert_eq!(
        bamboo_domain::PermissionAuditSnapshot::from_metadata(&child.metadata)
            .expect("planner permission audit")
            .resolution
            .effective,
        bamboo_domain::PermissionMode::Plan
    );
    assert!(
        !child.metadata.contains_key("disabled_tools"),
        "Plan caller must not be the authority that supplies its own denylist"
    );
    assert!(child
        .metadata
        .get("assignment_prompt")
        .is_some_and(|prompt| prompt.contains("do not implement")));

    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let root_runtime = parent
        .agent_runtime_state
        .as_ref()
        .expect("root runtime state");
    assert!(!root_runtime.read_only);
    assert!(root_runtime.plan_mode.is_none());
    assert_eq!(
        root_runtime.effective_permission_mode(),
        bamboo_domain::SessionPermissionMode::Auto
    );
    let wait = root_runtime
        .waiting_for_children
        .as_ref()
        .expect("Plan registered a durable child wait");
    assert_eq!(wait.child_session_ids, vec![child_id.to_string()]);
    assert_eq!(
        wait.registered_by_tool_call_id.as_deref(),
        Some("tc_plan_delegate")
    );

    // This harness intentionally persists tool actions through JsonlStorage,
    // so the independent SessionStoreV2 index does not receive the new child.
    // The adapter's publication cache does, and lets us prove there was no
    // hidden second planner creation in the same call.
    let planner_children = harness
        .adapter
        .sessions_cache
        .iter()
        .filter(|entry| {
            let session = entry.value().read();
            session.parent_session_id.as_deref() == Some(harness.parent_session_id.as_str())
                && session.subagent_type().as_deref() == Some("planner")
        })
        .count();
    assert_eq!(
        planner_children, 1,
        "one Plan call creates exactly one child"
    );
}

#[tokio::test]
async fn child_publication_uses_the_validating_instance_workspace_root() {
    let instance_root = tempfile::tempdir().expect("instance workspace root");
    let canonical_instance_root = instance_root
        .path()
        .canonicalize()
        .expect("canonical instance workspace root");
    let foreign_workspace = tempfile::tempdir().expect("foreign workspace");
    let resolver = bamboo_agent_core::workspace_state::WorkspaceResolver::new(|| None, {
        let root = instance_root.path().to_path_buf();
        move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
            root: root.clone(),
            confine: true,
        }
    });
    let harness = build_test_harness_with_options(None, Some(resolver)).await;
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .expect("load parent")
        .expect("parent");
    let child_id = "instance-confined-child".to_string();

    child_session::create_child_action(
        harness.adapter.as_ref(),
        child_session::CreateChildInput {
            parent_session: parent,
            child_id: child_id.clone(),
            title: "Confined child".to_string(),
            responsibility: "Inspect".to_string(),
            assignment_prompt: "Inspect".to_string(),
            subagent_type: "workspace-probe".to_string(),
            workspace: foreign_workspace.path().to_string_lossy().into_owned(),
            workspace_source: bamboo_engine::project_context::WorkspaceSource::Explicit,
            model_override: None,
            model_ref_override: None,
            runtime_metadata: HashMap::new(),
            read_only: false,
            auto_run: false,
            reasoning_effort: None,
            lifecycle: None,
            resident_name: None,
            resident_context: None,
            disabled_tools: None,
            context_fork: None,
        },
    )
    .await
    .expect("instance-confined child");

    let published =
        bamboo_agent_core::workspace_state::get_workspace(&child_id).expect("published workspace");
    assert!(published.starts_with(&canonical_instance_root));
    assert!(
        published.is_dir(),
        "the same instance resolver that validated the relocated target must materialize it"
    );
}

#[tokio::test]
async fn supervisor_common_child_constructor_keeps_ordinary_identity_for_all_role_labels() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let store = &harness.adapter.session_store;
    let receipt = store
        .get_or_create_default_supervisor("test-model")
        .await
        .unwrap();
    let root = store
        .load_session(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    let mut nested_parent: Option<Session> = None;
    // This matrix exercises common child construction, including cosmetic role
    // labels; it does not invoke the separate GuardianSpawner entry point.
    for (role, lifecycle, name) in [
        ("worker", None, None),
        ("resident", Some("resident"), Some("authority-resident")),
        ("guardian", None, None),
        ("nested", None, None),
    ] {
        let parent = if role == "nested" {
            nested_parent.clone().unwrap()
        } else {
            root.clone()
        };
        let child_id = format!("ordinary-{role}-{}", Uuid::new_v4());
        child_session::create_child_action(
            harness.adapter.as_ref(),
            child_session::CreateChildInput {
                parent_session: parent,
                child_id: child_id.clone(),
                title: role.into(),
                responsibility: "Inspect".into(),
                assignment_prompt: "Inspect".into(),
                subagent_type: role.into(),
                workspace: harness.workspace_path.to_string_lossy().into_owned(),
                workspace_source: bamboo_engine::project_context::WorkspaceSource::Explicit,
                model_override: None,
                model_ref_override: None,
                runtime_metadata: HashMap::from([
                    ("authority_identity".into(), "supervisor".into()),
                    ("role".into(), "supervisor".into()),
                ]),
                read_only: false,
                auto_run: false,
                reasoning_effort: None,
                lifecycle: lifecycle.map(str::to_string),
                resident_name: name.map(str::to_string),
                resident_context: None,
                disabled_tools: None,
                context_fork: None,
            },
        )
        .await
        .unwrap();
        let child = harness
            .storage
            .load_session(&child_id)
            .await
            .unwrap()
            .unwrap();
        assert!(child.authority_identity.is_ordinary(), "{role}");
        assert_eq!(child.root_session_id, receipt.session_id);
        assert_eq!(child.project_id_meta(), root.project_id_meta());
        if role == "worker" {
            nested_parent = Some(child);
        }
    }
    assert_eq!(
        store
            .load_root_authority(&receipt.session_id)
            .await
            .unwrap()
            .unwrap()
            .authority_identity,
        root.authority_identity
    );
}

#[tokio::test]
async fn child_resident_and_guardian_reject_cross_project_workspace_without_side_effects() {
    let harness = build_test_harness().await;
    let workspace = tempfile::tempdir().expect("workspace");
    let parent_project = harness
        .project_store
        .create("Parent Project", None)
        .expect("Parent Project");
    let _workspace_owner = harness
        .project_store
        .create_with_bindings(
            "Workspace Owner",
            None,
            vec![bamboo_domain::WorkspaceBinding {
                path: workspace.path().to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .expect("Workspace Owner");
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .expect("load parent")
        .expect("parent");
    parent.set_project_id_meta(parent_project.id.to_string());
    harness
        .storage
        .save_session(&parent)
        .await
        .expect("save parent");

    for (role, lifecycle, resident_name) in [
        ("workspace-probe", None, None),
        ("resident", Some("resident"), Some("stable")),
        ("guardian", None, None),
    ] {
        let child_id = format!("cross-project-{role}");
        let error = child_session::create_child_action(
            harness.adapter.as_ref(),
            child_session::CreateChildInput {
                parent_session: parent.clone(),
                child_id: child_id.clone(),
                title: format!("{role} child"),
                responsibility: "Inspect".to_string(),
                assignment_prompt: "Inspect".to_string(),
                subagent_type: role.to_string(),
                workspace: workspace.path().to_string_lossy().into_owned(),
                workspace_source: bamboo_engine::project_context::WorkspaceSource::Explicit,
                model_override: None,
                model_ref_override: None,
                runtime_metadata: HashMap::new(),
                read_only: false,
                auto_run: false,
                reasoning_effort: None,
                lifecycle: lifecycle.map(str::to_string),
                resident_name: resident_name.map(str::to_string),
                resident_context: None,
                disabled_tools: None,
                context_fork: None,
            },
        )
        .await
        .expect_err("cross-Project child workspace must fail closed");
        assert!(error.to_string().contains("belongs to Project"));
        assert!(
            harness
                .storage
                .load_session(&child_id)
                .await
                .expect("load child")
                .is_none(),
            "{role} conflict must not persist a child"
        );
        assert!(
            harness
                .adapter
                .session_store
                .get_index_entry(&child_id)
                .await
                .is_none(),
            "{role} conflict must not index a child"
        );
        assert!(
            bamboo_agent_core::workspace_state::get_workspace(&child_id).is_none(),
            "{role} conflict must not mutate runtime workspace state"
        );
    }
}

#[tokio::test]
async fn unassigned_child_rejects_stale_bound_workspace_without_persistence() {
    let harness = build_test_harness().await;
    let stale_workspace = tempfile::tempdir().expect("stale workspace");
    let stale_path = stale_workspace.path().to_path_buf();
    harness
        .project_store
        .create_with_bindings(
            "Former Workspace Owner",
            None,
            vec![bamboo_domain::WorkspaceBinding {
                path: stale_path.to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .expect("bind workspace while it exists");
    stale_workspace.close().expect("remove bound workspace");
    assert!(!stale_path.exists());

    let index_before = harness
        .adapter
        .session_store
        .list_index_entries()
        .await
        .into_iter()
        .map(|entry| entry.id)
        .collect::<Vec<_>>();
    let cache_len_before = harness.adapter.sessions_cache.len();
    let runners_before = harness.agent_runners.read().await.len();
    let parent_before = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .expect("load parent")
        .expect("parent");

    let error = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Must not be created",
            "responsibility": "Inspect",
            "prompt": "Inspect the stale workspace.",
            "workspace": stale_path,
            "auto_run": false
        }),
        ctx_for(&harness.parent_session_id, "stale-workspace").to_tool_ctx(),
    )
    .await
    .expect_err("server adapter must always use authoritative workspace validation");
    assert!(
        matches!(error, ToolError::InvalidArguments(ref message) if message.contains("does not exist"))
    );

    assert_eq!(
        harness
            .adapter
            .session_store
            .list_index_entries()
            .await
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        index_before,
        "rejected child must not be indexed"
    );
    assert_eq!(harness.adapter.sessions_cache.len(), cache_len_before);
    assert_eq!(harness.agent_runners.read().await.len(), runners_before);
    let parent_after = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .expect("reload parent")
        .expect("parent");
    assert_eq!(
        serde_json::to_value(parent_after).expect("parent after JSON"),
        serde_json::to_value(parent_before).expect("parent before JSON"),
        "rejected child must not mutate or persist its parent"
    );
}

#[tokio::test]
async fn concurrent_parent_wait_registrations_all_land_in_wait_set() {
    let harness = build_test_harness().await;
    let adapter = harness.adapter.clone();
    let parent_id = harness.parent_session_id.clone();

    // Fire several registrations for the same parent concurrently, exactly as
    // a round of parallel `SubAgent.create` calls would.
    let child_ids: Vec<String> = (0..6).map(|i| format!("c-{i}")).collect();
    let mut handles = Vec::new();
    for id in &child_ids {
        let adapter = adapter.clone();
        let parent_id = parent_id.clone();
        let id = id.clone();
        handles.push(tokio::spawn(async move {
            adapter
                .register_parent_wait_for_child(&parent_id, &id, Some("tc-1"))
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().expect("registration should succeed");
    }

    // Every child must be durably present in the parent's wait set, with no
    // duplicates — regardless of how the concurrent calls coalesced.
    let parent = harness
        .storage
        .load_session(&parent_id)
        .await
        .unwrap()
        .unwrap();
    let wait = parent
        .agent_runtime_state
        .expect("runtime state persisted")
        .waiting_for_children
        .expect("wait state persisted");
    let mut got = wait.child_session_ids.clone();
    got.sort();
    assert_eq!(
        got, child_ids,
        "all children must be registered exactly once"
    );
    assert_eq!(
        parent
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str),
        Some("waiting_for_children")
    );
}

#[tokio::test]
async fn repeated_registration_of_same_child_is_idempotent() {
    let harness = build_test_harness().await;
    let adapter = harness.adapter.clone();
    let parent_id = harness.parent_session_id.clone();

    for _ in 0..3 {
        adapter
            .register_parent_wait_for_child(&parent_id, "dup-child", None)
            .await
            .unwrap();
    }

    let parent = harness
        .storage
        .load_session(&parent_id)
        .await
        .unwrap()
        .unwrap();
    let wait = parent
        .agent_runtime_state
        .unwrap()
        .waiting_for_children
        .unwrap();
    assert_eq!(wait.child_session_ids, vec!["dup-child".to_string()]);
}

#[tokio::test]
async fn failed_launch_rollback_removes_only_its_child_from_the_parent_wait() {
    let harness = build_test_harness().await;
    let adapter = harness.adapter.clone();
    let parent_id = harness.parent_session_id.clone();

    adapter
        .register_parent_wait_for_child(&parent_id, "failed-child", Some("tc-failed"))
        .await
        .unwrap();
    adapter
        .register_parent_wait_for_child(&parent_id, "live-sibling", Some("tc-live"))
        .await
        .unwrap();

    adapter
        .rollback_parent_wait_for_child(&parent_id, "failed-child")
        .await
        .expect("rollback should preserve the live sibling");

    let parent = harness
        .storage
        .load_session(&parent_id)
        .await
        .unwrap()
        .unwrap();
    let wait = parent
        .agent_runtime_state
        .expect("runtime state")
        .waiting_for_children
        .expect("sibling wait must remain armed");
    assert_eq!(wait.child_session_ids, vec!["live-sibling".to_string()]);
    assert_eq!(
        parent
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str),
        Some("waiting_for_children")
    );

    adapter
        .rollback_parent_wait_for_child(&parent_id, "live-sibling")
        .await
        .expect("last-child rollback should clear the wait");
    let parent = harness
        .storage
        .load_session(&parent_id)
        .await
        .unwrap()
        .unwrap();
    assert!(parent
        .agent_runtime_state
        .and_then(|state| state.waiting_for_children)
        .is_none());
    assert!(!parent.metadata.contains_key("runtime.suspend_reason"));
}

#[tokio::test]
async fn parent_wait_slot_is_evicted_after_flush_drains() {
    // Issue #346: the per-parent coalescing slot must not linger in
    // `parent_wait_slots` after its pending queue drains, otherwise the map
    // grows by one entry per parent-that-ever-spawned and never shrinks.
    let harness = build_test_harness().await;
    let adapter = harness.adapter.clone();
    let parent_id = harness.parent_session_id.clone();

    adapter
        .register_parent_wait_for_child(&parent_id, "one-child", None)
        .await
        .unwrap();

    assert!(
        adapter.parent_wait_slots.is_empty(),
        "coalescing slot must be evicted once the batch is persisted and pending drains"
    );
}

#[tokio::test]
async fn parent_wait_slots_drain_after_concurrent_registrations() {
    let harness = build_test_harness().await;
    let adapter = harness.adapter.clone();
    let parent_id = harness.parent_session_id.clone();

    let mut handles = Vec::new();
    for i in 0..6 {
        let adapter = adapter.clone();
        let parent_id = parent_id.clone();
        handles.push(tokio::spawn(async move {
            adapter
                .register_parent_wait_for_child(&parent_id, &format!("c-{i}"), None)
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert!(
        adapter.parent_wait_slots.is_empty(),
        "no coalescing slot should linger once all concurrent registrations drain"
    );
}

// -----------------------------------------------------------------------
// Decoupled create + explicit SubAgent.wait
// -----------------------------------------------------------------------

fn ctx_for<'a>(session_id: &'a str, tool_call_id: &'static str) -> ToolExecutionContext<'a> {
    ToolExecutionContext {
        executing_supervisor: None,
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id,
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    }
}

#[tokio::test]
async fn create_without_subagent_type_defaults_to_worker_label() {
    let harness = build_test_harness().await;
    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "No Label Child",
            "responsibility": "Do work",
            "prompt": "Do the work",
            "workspace": harness.workspace_path.to_string_lossy()
            // subagent_type intentionally omitted
        }),
        ctx_for(&harness.parent_session_id, "tc_no_label").to_tool_ctx(),
    )
    .await
    .expect("create must succeed without subagent_type");

    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["subagent_type"].as_str(), Some("worker"));
}

#[tokio::test]
async fn create_refused_at_max_spawn_depth() {
    // Phase 6: an agent at the depth cap cannot create more sub-agents (bounds
    // worker→worker→… recursion). Use a real nested Child at the cap.
    let harness = build_test_harness_with_storage(None, None, true).await;
    let parent =
        nested_parent_at_depth(&harness, bamboo_server_tools::DEFAULT_MAX_SPAWN_DEPTH).await;

    let err = invoke_completed(
        &harness.tool,
            json!({"action":"create","title":"X","responsibility":"Y","prompt":"Z","workspace":harness.workspace_path.to_string_lossy()}),
            ctx_for(&parent.id, "tc_depth_cap").to_tool_ctx(),
        )
        .await
        .expect_err("create at the depth cap must be refused");
    assert!(
        matches!(err, bamboo_agent_core::tools::ToolError::InvalidArguments(ref m) if m.contains("depth limit")),
        "expected a depth-limit InvalidArguments, got {err:?}"
    );
}

#[tokio::test]
async fn create_allowed_just_below_max_spawn_depth() {
    // One level below the cap, create proceeds (depth gate does not fire).
    let harness = build_test_harness_with_storage(None, None, true).await;
    let parent =
        nested_parent_at_depth(&harness, bamboo_server_tools::DEFAULT_MAX_SPAWN_DEPTH - 1).await;

    let result = invoke_completed(
        &harness.tool,
            json!({"action":"create","title":"X","responsibility":"Y","prompt":"Z","workspace":harness.workspace_path.to_string_lossy()}),
            ctx_for(&parent.id, "tc_depth_ok").to_tool_ctx(),
        )
        .await;
    assert!(
        result.is_ok(),
        "create just below the cap should proceed, got {result:?}"
    );
}

async fn nested_parent_at_depth(harness: &TestHarness, depth: u32) -> Session {
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    for level in 1..=depth {
        let child = Session::new_child_of(
            format!("depth-{level}"),
            &parent,
            "gpt-5",
            format!("Depth {level}"),
        );
        harness.storage.save_session(&child).await.unwrap();
        parent = child;
    }
    parent
}

#[tokio::test]
async fn create_with_wait_true_suspends_and_registers_wait() {
    let harness = build_test_harness().await;
    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Blocking Child",
            "responsibility": "Do one thing",
            "prompt": "Do it",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy(),
            "wait": true
        }),
        ctx_for(&harness.parent_session_id, "tc_create_wait").to_tool_ctx(),
    )
    .await
    .expect("create should succeed");

    assert_eq!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children"),
        "create wait=true must suspend the parent"
    );

    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let wait = parent
        .agent_runtime_state
        .expect("runtime state")
        .waiting_for_children
        .expect("wait registered");
    assert_eq!(wait.child_session_ids.len(), 1);
}

#[tokio::test]
async fn wait_action_with_explicit_children_suspends_and_registers() {
    let harness = build_test_harness().await;
    // The jsonl-backed harness has no child index, so nothing is positively
    // reported terminal and every requested id must be KEPT (issue #546:
    // unknown ≠ finished — only index-confirmed terminal ids are dropped; a
    // truly bogus id is rescued by the child-wait watchdog at runtime).
    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "wait",
            "child_session_ids": ["k1", "k2", "k3"],
            "wait_for": "any"
        }),
        ctx_for(&harness.parent_session_id, "tc_wait").to_tool_ctx(),
    )
    .await
    .expect("wait should succeed");

    assert_eq!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children"),
        "wait must suspend the parent"
    );
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"].as_str(), Some("waiting"));
    assert_eq!(payload["wait_for"].as_str(), Some("any"));
    assert_eq!(
        payload["already_terminal_child_ids"]
            .as_array()
            .map(Vec::len),
        Some(0),
        "nothing may be dropped when the index reports no terminal children: {payload}"
    );

    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let mut stale_runner = parent.clone();
    let wait = parent
        .agent_runtime_state
        .unwrap()
        .waiting_for_children
        .unwrap();
    assert_eq!(
        wait.child_session_ids,
        vec!["k1".to_string(), "k2".to_string(), "k3".to_string()]
    );
    assert_eq!(wait.wait_for, ChildWaitPolicy::Any);
    assert_eq!(wait.registered_by_tool_call_id.as_deref(), Some("tc_wait"));

    // Completion after the runner's pipeline read must still win at the final
    // merge-save boundary for this explicit tool wait.
    let mut completed = stale_runner.clone();
    let runtime = completed.agent_runtime_state.as_mut().unwrap();
    runtime.waiting_for_children = None;
    runtime.status = bamboo_domain::session::runtime_state::AgentStatusState::Idle;
    runtime.suspension = None;
    completed.metadata.remove("runtime.suspend_reason");
    harness.storage.save_session(&completed).await.unwrap();
    harness
        .adapter
        .persistence
        .merge_save_finalized_runtime(&mut stale_runner)
        .await
        .unwrap();
    let finalized = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(finalized
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    assert!(!finalized.metadata.contains_key("runtime.suspend_reason"));
}

#[tokio::test]
async fn wait_action_is_noop_when_no_active_children() {
    let harness = build_test_harness().await;
    // No explicit ids and (in the jsonl-backed harness) no derivable active
    // children → must NOT suspend, and must NOT register an empty wait.
    let result = invoke_completed(
        &harness.tool,
        json!({ "action": "wait" }),
        ctx_for(&harness.parent_session_id, "tc_wait_noop").to_tool_ctx(),
    )
    .await
    .expect("wait should succeed");

    assert_ne!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children"),
        "wait with no active children must not suspend"
    );
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"].as_str(), Some("no_active_children"));

    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(parent
        .agent_runtime_state
        .and_then(|s| s.waiting_for_children)
        .is_none());
}

// (Pure `normalize_title` unit tests live with the helper in
// `bamboo-server-tools` `sub_agent.rs`.)

// -----------------------------------------------------------------------
// Create action tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn create_requires_session_id_in_tool_context() {
    let harness = build_test_harness().await;

    let err = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "demo task",
            "responsibility": "do something",
            "prompt": "do something",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy()
        }),
        ToolCtx::none("tool_call"),
    )
    .await
    .unwrap_err();

    match err {
        ToolError::Execution(msg) => {
            assert!(msg.contains("SubAgent requires a session_id in tool context"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn create_publishes_started_before_fast_completion_and_caches_latest() {
    let mut harness = build_test_harness().await;
    let mut parent_runner = AgentRunner::new();
    parent_runner.status = bamboo_engine::AgentStatus::Running;
    harness
        .agent_runners
        .write()
        .await
        .insert(harness.parent_session_id.clone(), parent_runner);

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Child A",
            "responsibility": "Investigate one module",
            "prompt": "Read module and summarize",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy()
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_1",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("SubAgent should enqueue a child session");

    let parsed_result: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    let child_session_id = parsed_result
        .get("child_session_id")
        .and_then(|v| v.as_str())
        .expect("tool result should include child_session_id")
        .to_string();

    let lifecycle = tokio::time::timeout(Duration::from_secs(2), async {
        let mut lifecycle = Vec::new();
        while lifecycle.len() < 2 {
            match harness.parent_rx.recv().await {
                Ok(AgentEvent::SubAgentStarted {
                    parent_session_id: pid,
                    child_session_id: cid,
                    ..
                }) if cid == child_session_id => lifecycle.push(("started", pid, cid)),
                Ok(AgentEvent::SubAgentCompleted {
                    parent_session_id: pid,
                    child_session_id: cid,
                    ..
                }) if cid == child_session_id => lifecycle.push(("completed", pid, cid)),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    panic!("parent stream closed before child lifecycle completed")
                }
            }
        }
        lifecycle
    })
    .await
    .expect("should receive the fast child lifecycle quickly");

    assert_eq!(lifecycle[0].0, "started");
    assert_eq!(lifecycle[1].0, "completed");
    assert!(lifecycle.iter().all(|(_, parent_id, child_id)| parent_id
        == &harness.parent_session_id
        && child_id == &child_session_id));

    let runners = harness.agent_runners.read().await;
    let parent_runner = runners
        .get(&harness.parent_session_id)
        .expect("parent runner retained");
    let cached_child_lifecycle = parent_runner
        .last_critical_events
        .iter()
        .filter(|event| match event {
            AgentEvent::SubAgentStarted {
                child_session_id: event_child_id,
                ..
            }
            | AgentEvent::SubAgentCompleted {
                child_session_id: event_child_id,
                ..
            } => event_child_id == &child_session_id,
            _ => false,
        })
        .collect::<Vec<_>>();
    assert_eq!(cached_child_lifecycle.len(), 1);
    assert!(matches!(
        cached_child_lifecycle[0],
        AgentEvent::SubAgentCompleted {
            child_session_id: cached_child_id,
            ..
        } if cached_child_id == &child_session_id
    ));
}

#[tokio::test]
async fn create_uses_async_subagent_model_resolver() {
    let resolver: crate::tools::SubagentModelResolver = Arc::new(|subagent_type: String| {
        Box::pin(async move {
            assert_eq!(subagent_type, "coder");
            Some(
                bamboo_domain::ProviderModelRef::new("openai", "gpt-resolved-coder")
                    .with_reasoning_effort(bamboo_domain::ReasoningEffort::Low),
            )
        })
    });
    let harness = build_test_harness_with_resolver(Some(resolver)).await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Coder Child",
            "responsibility": "Implement a focused change",
            "prompt": "Patch one file",
            "subagent_type": "coder",
            "workspace": harness.workspace_path.to_string_lossy(),
            "auto_run": false
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_async_resolver",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("SubAgent should create a child using async model resolver");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["model"], "gpt-resolved-coder");
    assert_eq!(payload["reasoning_effort"], "low");

    let child_id = payload["child_session_id"]
        .as_str()
        .expect("child_session_id should be present");
    let child = harness
        .storage
        .load_session(child_id)
        .await
        .unwrap()
        .expect("child session should exist");
    assert_eq!(child.model, "gpt-resolved-coder");
    assert_eq!(
        child.model_ref,
        Some(
            bamboo_domain::ProviderModelRef::new("openai", "gpt-resolved-coder")
                .with_reasoning_effort(bamboo_domain::ReasoningEffort::Low),
        )
    );
    assert_eq!(
        child.reasoning_effort,
        Some(bamboo_domain::ReasoningEffort::Low)
    );
    assert_eq!(
        child.metadata.get("provider_name").map(String::as_str),
        Some("openai")
    );
}

#[tokio::test]
async fn supervisor_resident_reset_and_accumulate_stay_ordinary_in_canonical_v2() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let canonical: Arc<dyn Storage> = harness.adapter.session_store.clone();
    assert!(Arc::ptr_eq(&harness.storage, &canonical));
    let receipt = harness
        .storage
        .get_or_create_default_supervisor("gpt-5")
        .await
        .unwrap();
    let expected_identity = bamboo_domain::SessionAuthorityIdentity::Supervisor {
        incarnation_id: receipt.incarnation_id,
    };
    assert_eq!(
        harness
            .storage
            .load_session(&receipt.session_id)
            .await
            .unwrap()
            .unwrap()
            .authority_identity,
        expected_identity
    );

    let first_brief = "First Supervisor resident assignment";
    let reset_brief = "Replacement Supervisor resident assignment";
    let accumulated_brief = "Additional Supervisor resident assignment";
    let mut resident_id = None;
    for (step, context, brief) in [
        (0, "reset", first_brief),
        (1, "reset", reset_brief),
        (2, "accumulate", accumulated_brief),
    ] {
        let result = invoke_completed(
            &harness.tool,
            json!({
                "action": "create",
                "lifecycle": "resident",
                "name": "supervisor-resident",
                "context": context,
                "title": format!("Supervisor resident task {step}"),
                "responsibility": "Inspect one bounded task",
                "prompt": brief,
                "workspace": harness.workspace_path.to_string_lossy(),
                "auto_run": false
            }),
            subagent_test_ctx(&receipt.session_id, &format!("supervisor-resident-{step}")),
        )
        .await
        .expect("real SubAgent create or reuse must succeed");
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let id = payload["child_session_id"].as_str().unwrap().to_string();
        assert_eq!(payload["reused"], json!(step != 0));
        if let Some(previous_id) = resident_id.as_ref() {
            assert_eq!(
                &id, previous_id,
                "reset and accumulate must reuse the resident"
            );
        } else {
            resident_id = Some(id.clone());
        }
        // No manual index mirroring: the production V2 save must make the
        // resident discoverable by the next real SubAgent invocation.
        let child = harness.storage.load_session(&id).await.unwrap().unwrap();
        assert!(child.authority_identity.is_ordinary(), "step {step}");
        assert_eq!(
            child.parent_session_id.as_deref(),
            Some(receipt.session_id.as_str())
        );
        assert_eq!(child.root_session_id, receipt.session_id);
        assert!(child.messages.last().unwrap().content.contains(brief));
        if step > 0 {
            assert!(!child
                .messages
                .iter()
                .any(|message| message.content.contains(first_brief)));
            assert_eq!(
                child.metadata.get("assignment_prompt").map(String::as_str),
                Some(reset_brief)
            );
        }
        if step == 2 {
            assert!(child
                .messages
                .iter()
                .any(|message| message.content.contains(reset_brief)));
        }
        assert_eq!(
            harness
                .storage
                .load_root_authority(&receipt.session_id)
                .await
                .unwrap()
                .unwrap()
                .authority_identity,
            expected_identity,
            "resident provisioning must preserve the parent incarnation"
        );
    }
}

#[tokio::test]
async fn resident_create_reuses_same_child_session() {
    let harness = build_test_harness().await;
    let workspace = tempfile::tempdir().expect("workspace");
    let ctx = |tcid: &'static str| ToolExecutionContext {
        executing_supervisor: None,
        session_id: Some(harness.parent_session_id.as_str()),
        root_session_id: None,
        tool_call_id: tcid,
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let create = |name_task: &'static str, prompt: &'static str| {
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "essayist",
            "title": name_task,
            "responsibility": "Write a short essay",
            "prompt": prompt,
            "workspace": workspace.path(),
            "auto_run": false
        })
    };

    // First resident create: spins up the essayist.
    let r1 = invoke_completed(
        &harness.tool,
        create("Essay: 溪流", "Write ~150 words about 溪流."),
        ctx("tc1").to_tool_ctx(),
    )
    .await
    .expect("first resident create");
    let p1: serde_json::Value = serde_json::from_str(&r1.result).unwrap();
    let id1 = p1["child_session_id"].as_str().unwrap().to_string();
    assert_eq!(p1["reused"], json!(false));
    assert_eq!(p1["lifecycle"], "resident");

    // In production storage and the session index are the SAME SessionStoreV2,
    // so a child save auto-indexes (find_resident_child reads that index). This
    // harness uses a separate index store, so mirror the production effect by
    // indexing the freshly-created resident explicitly.
    let child1 = harness
        .storage
        .load_session(&id1)
        .await
        .unwrap()
        .expect("child1 saved");
    harness
        .adapter
        .session_store
        .save_session(&child1)
        .await
        .unwrap();

    // Second resident create with the SAME name: reuses the same session.
    let r2 = invoke_completed(
        &harness.tool,
        create("Essay: 山峰", "Write ~150 words about 山峰."),
        ctx("tc2").to_tool_ctx(),
    )
    .await
    .expect("second resident create");
    let p2: serde_json::Value = serde_json::from_str(&r2.result).unwrap();
    assert_eq!(
        p2["child_session_id"].as_str().unwrap(),
        id1,
        "resident reuse must return the same child session"
    );
    assert_eq!(p2["reused"], json!(true));

    // The reused child carries the resident metadata tags.
    let child = harness
        .storage
        .load_session(&id1)
        .await
        .unwrap()
        .expect("child exists");
    assert!(child.authority_identity.is_ordinary());
    assert_eq!(
        child.metadata.get("lifecycle").map(String::as_str),
        Some("resident")
    );
    assert_eq!(
        child.metadata.get("resident_name").map(String::as_str),
        Some("essayist")
    );

    // A one-shot create makes a DIFFERENT session.
    let r3 = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "OneShot",
            "responsibility": "Independent task",
            "prompt": "Do something unrelated.",
            "workspace": workspace.path(),
            "auto_run": false
        }),
        ctx("tc3").to_tool_ctx(),
    )
    .await
    .expect("oneshot create");
    let p3: serde_json::Value = serde_json::from_str(&r3.result).unwrap();
    assert_ne!(
        p3["child_session_id"].as_str().unwrap(),
        id1,
        "one-shot create must be a new session"
    );
}

#[tokio::test]
async fn root_stays_contract_free_while_oneshot_and_resident_children_get_it_once() {
    let harness = build_test_harness().await;
    let mut child_prompts = Vec::new();

    for (title, lifecycle, name, call_id) in [
        ("One-shot contract", None, None, "contract-oneshot"),
        (
            "Resident contract",
            Some("resident"),
            Some("contract-resident"),
            "contract-resident",
        ),
    ] {
        let mut request = json!({
            "action": "create",
            "title": title,
            "responsibility": "Inspect one bounded path",
            "prompt": "Read one file and report evidence.",
            "subagent_type": "contract-probe",
            "workspace": harness.workspace_path.to_string_lossy(),
            "auto_run": false
        });
        if let Some(lifecycle) = lifecycle {
            request["lifecycle"] = json!(lifecycle);
        }
        if let Some(name) = name {
            request["name"] = json!(name);
        }

        let result = invoke_completed(
            &harness.tool,
            request,
            subagent_test_ctx(&harness.parent_session_id, call_id),
        )
        .await
        .expect("child create with delegation contract");
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let child = harness
            .storage
            .load_session(payload["child_session_id"].as_str().unwrap())
            .await
            .unwrap()
            .expect("created child");
        let base = child
            .metadata
            .get("base_system_prompt")
            .expect("child base prompt")
            .clone();
        assert_eq!(
            base.matches(child_session::SUBAGENT_DELEGATION_CONTRACT_VERSION)
                .count(),
            1
        );
        assert_eq!(
            base.matches(child_session::SUBAGENT_DELEGATION_CONTRACT_START_MARKER)
                .count(),
            1
        );
        assert_eq!(
            child
                .messages
                .iter()
                .filter(|message| matches!(message.role, Role::System))
                .count(),
            1
        );
        assert_eq!(
            child.messages[0].content, base,
            "persisted base and child system message must be identical"
        );
        let assignment = child.messages.last().expect("assignment message");
        assert!(matches!(assignment.role, Role::User));
        assert!(assignment.content.starts_with("Delegated child assignment"));
        assert_eq!(assignment.content.matches("## ").count(), 6);
        assert!(!assignment
            .content
            .contains(child_session::SUBAGENT_DELEGATION_CONTRACT_VERSION));
        child_prompts.push(base);
    }

    assert_eq!(
        child_prompts[0], child_prompts[1],
        "one-shot and resident children must receive the same child contract"
    );
    let root = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .expect("root session");
    assert!(!root
        .metadata
        .values()
        .any(|value| value.contains(child_session::SUBAGENT_DELEGATION_CONTRACT_VERSION)));
    assert!(!root.messages.iter().any(|message| message
        .content
        .contains(child_session::SUBAGENT_DELEGATION_CONTRACT_VERSION)));
}

#[tokio::test]
async fn resident_reuse_caller_cancel_after_stop_still_queues_replacement_task() {
    let mut harness = build_test_harness_with_storage(None, None, true).await;
    let first = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "steady-worker",
            "title": "First task",
            "responsibility": "Handle one task",
            "prompt": "Original task",
            "workspace": harness.workspace_path.to_string_lossy(),
            "auto_run": false,
        }),
        subagent_test_ctx(&harness.parent_session_id, "resident-initial"),
    )
    .await
    .unwrap();
    let first_payload: serde_json::Value = serde_json::from_str(&first.result).unwrap();
    let child_id = first_payload["child_session_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        harness
            .adapter
            .find_resident_child(&harness.parent_session_id, "steady-worker")
            .await
            .as_deref(),
        Some(child_id.as_str())
    );
    let cancel_token = {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let cancel_token = runner.cancel_token.clone();
        runners.insert(child_id.clone(), runner);
        cancel_token
    };
    let runners_for_status = harness.agent_runners.clone();
    let child_id_for_status = child_id.clone();
    let waiter = tokio::spawn(async move {
        cancel_token.cancelled().await;
        let mut runners = runners_for_status.write().await;
        if let Some(runner) = runners.get_mut(&child_id_for_status) {
            runner.status = AgentStatus::Cancelled;
        }
    });
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.expect_wait_on_enqueue.store(false, Ordering::SeqCst);
    port.hold_after_interrupt.store(true, Ordering::SeqCst);
    let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
    let parent_id = harness.parent_session_id.clone();
    let workspace = harness.workspace_path.to_string_lossy().to_string();
    let entered = port.interrupt_completed.notified();
    let mut outer = tokio::spawn(async move {
        invoke_completed(
            &tool,
            json!({
                "action": "create",
                "lifecycle": "resident",
                "name": "steady-worker",
                "context": "reset",
                "title": "Replacement task",
                "responsibility": "Handle replacement",
                "prompt": "Replacement task after stop",
                "workspace": workspace,
                "auto_run": true,
                "wait": false,
            }),
            subagent_test_ctx(&parent_id, "resident-reuse-after-stop"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = entered => {}
            outcome = &mut outer => panic!("resident reuse ended before stopping old run: {outcome:?}"),
        }
    })
    .await
    .expect("resident old run must stop before caller cancellation");
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());
    port.release_interrupt.notify_one();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("resident old run cancellation must signal the waiter")
        .expect("waiter task should finish");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match harness.parent_rx.recv().await {
                Ok(AgentEvent::SubAgentStarted {
                    child_session_id, ..
                }) if child_session_id == child_id => break,
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(error) => panic!("parent event stream closed: {error}"),
            }
        }
    })
    .await
    .expect("replacement resident task must be queued after caller abort");
    let child = harness
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child.metadata.get("assignment_prompt").map(String::as_str),
        Some("Replacement task after stop")
    );
}

#[tokio::test]
async fn resident_create_reset_and_accumulate_share_complete_background_aware_frame() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .expect("parent");
    parent.add_message(Message::system("root-only secret policy"));
    parent.add_message(Message::user("initial parent input"));
    parent.add_message(Message::assistant("initial parent response", None));
    harness.storage.save_session(&parent).await.unwrap();

    let first_brief = "First complete brief.\nAcceptance: show first evidence.";
    let first = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "contract-lifecycle",
            "context": "reset",
            "title": "First resident task",
            "responsibility": "Inspect first path",
            "prompt": first_brief,
            "subagent_type": "researcher",
            "workspace": harness.workspace_path.to_string_lossy(),
            "fork_last_messages": 3,
            "auto_run": false
        }),
        subagent_test_ctx(&harness.parent_session_id, "resident-contract-first"),
    )
    .await
    .expect("first resident create");
    let first_payload: serde_json::Value = serde_json::from_str(&first.result).unwrap();
    let resident_id = first_payload["child_session_id"]
        .as_str()
        .unwrap()
        .to_string();
    let first_child = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .expect("first resident");
    let first_background = child_session::render_forked_parent_context(&parent, 3).unwrap();
    let first_expected = child_session::format_child_assignment_with_background(
        "First resident task",
        "Inspect first path",
        "researcher",
        first_brief,
        Some(&first_background),
    );
    assert_eq!(first_child.messages.last().unwrap().content, first_expected);
    assert!(!first_expected.contains("root-only secret policy"));
    assert!(first_expected.contains("initial parent input"));
    assert!(first_expected.contains("initial parent response"));
    assert!(
        first_expected.find("</forked-parent-background>").unwrap()
            < first_expected
                .find("## 3. Allowed actions and mutation scope")
                .unwrap()
    );
    assert_eq!(
        first_child
            .metadata
            .get("assignment_prompt")
            .map(String::as_str),
        Some(first_brief)
    );
    harness
        .adapter
        .session_store
        .save_session(&first_child)
        .await
        .unwrap();

    parent.add_message(Message::user("reset-only parent background"));
    harness.storage.save_session(&parent).await.unwrap();
    let reset_brief = "Reset complete brief.\nAcceptance: show reset evidence.";
    let reset = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "contract-lifecycle",
            "context": "reset",
            "title": "Reset resident task",
            "responsibility": "Inspect reset path",
            "prompt": reset_brief,
            "subagent_type": "audit-worker",
            "workspace": harness.workspace_path.to_string_lossy(),
            "fork_last_messages": 1,
            "auto_run": false
        }),
        subagent_test_ctx(&harness.parent_session_id, "resident-contract-reset"),
    )
    .await
    .expect("resident reset reuse");
    let reset_payload: serde_json::Value = serde_json::from_str(&reset.result).unwrap();
    assert_eq!(reset_payload["child_session_id"], resident_id);
    let reset_child = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .expect("reset resident");
    let reset_background = child_session::render_forked_parent_context(&parent, 1).unwrap();
    let reset_expected = child_session::format_child_assignment_with_background(
        "Reset resident task",
        "Inspect reset path",
        "audit-worker",
        reset_brief,
        Some(&reset_background),
    );
    assert_eq!(reset_child.messages.last().unwrap().content, reset_expected);
    assert!(!reset_child
        .messages
        .last()
        .unwrap()
        .content
        .contains(first_brief));
    assert_eq!(
        reset_child
            .metadata
            .get("assignment_prompt")
            .map(String::as_str),
        Some(reset_brief)
    );
    assert!(!reset_child
        .metadata
        .get("assignment_prompt")
        .unwrap()
        .contains("forked-parent-background"));

    parent.add_message(Message::user("accumulate-only parent background"));
    harness.storage.save_session(&parent).await.unwrap();
    let accumulate_brief = "Accumulated complete brief.\nAcceptance: show accumulated evidence.";
    let accumulated = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "contract-lifecycle",
            "context": "accumulate",
            "title": "Accumulated resident task",
            "responsibility": "Inspect accumulated path",
            "prompt": accumulate_brief,
            "subagent_type": "audit-worker",
            "workspace": harness.workspace_path.to_string_lossy(),
            "fork_last_messages": 1,
            "auto_run": false
        }),
        subagent_test_ctx(&harness.parent_session_id, "resident-contract-accumulate"),
    )
    .await
    .expect("resident accumulate reuse");
    let accumulated_payload: serde_json::Value = serde_json::from_str(&accumulated.result).unwrap();
    assert_eq!(accumulated_payload["child_session_id"], resident_id);
    let accumulated_child = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .expect("accumulated resident");
    let accumulated_background = child_session::render_forked_parent_context(&parent, 1).unwrap();
    let accumulated_expected = child_session::format_child_assignment_with_background(
        "Accumulated resident task",
        "Inspect accumulated path",
        "audit-worker",
        accumulate_brief,
        Some(&accumulated_background),
    );
    assert_eq!(
        accumulated_child.messages.last().unwrap().content,
        accumulated_expected
    );
    assert!(accumulated_child
        .messages
        .iter()
        .any(|message| message.content == reset_expected));
    assert_eq!(
        accumulated_child
            .metadata
            .get("assignment_prompt")
            .map(String::as_str),
        Some(reset_brief),
        "accumulate is an appended task message and must not rewrite canonical reset metadata"
    );
    assert!(!accumulated_child
        .metadata
        .get("assignment_prompt")
        .unwrap()
        .contains("forked-parent-background"));
}

#[tokio::test]
async fn resident_reuse_rejects_cross_project_workspace_before_mutating_resident() {
    let harness = build_test_harness().await;
    let workspace_a = tempfile::tempdir().expect("workspace A");
    let workspace_b = tempfile::tempdir().expect("workspace B");
    let project_a = harness
        .project_store
        .create_with_bindings(
            "Project A",
            None,
            vec![bamboo_domain::WorkspaceBinding {
                path: workspace_a.path().to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .expect("Project A");
    let project_b = harness
        .project_store
        .create_with_bindings(
            "Project B",
            None,
            vec![bamboo_domain::WorkspaceBinding {
                path: workspace_b.path().to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .expect("Project B");
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .expect("load parent")
        .expect("parent");
    parent.set_project_id_meta(project_a.id.to_string());
    parent.set_workspace_path_meta(workspace_a.path().to_string_lossy().into_owned());
    harness
        .storage
        .save_session(&parent)
        .await
        .expect("save parent");
    let ctx = |tool_call_id: &'static str| {
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    };
    let create_args = |workspace: &std::path::Path, title: &str| {
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "stable-reviewer",
            "title": title,
            "responsibility": "Review safely",
            "prompt": "Inspect the assigned workspace.",
            "workspace": workspace,
            "auto_run": false
        })
    };

    let created = invoke_completed(
        &harness.tool,
        create_args(workspace_a.path(), "Initial review"),
        ctx("resident-create"),
    )
    .await
    .expect("create resident in Project A");
    let created: serde_json::Value = serde_json::from_str(&created.result).unwrap();
    let resident_id = created["child_session_id"]
        .as_str()
        .expect("resident id")
        .to_string();
    let resident_before = harness
        .storage
        .load_session(&resident_id)
        .await
        .expect("load resident")
        .expect("resident");
    harness
        .adapter
        .session_store
        .save_session(&resident_before)
        .await
        .expect("index resident");
    let runtime_before = bamboo_agent_core::workspace_state::get_workspace(&resident_id);

    let error = invoke_completed(
        &harness.tool,
        create_args(workspace_b.path(), "Must not replace title"),
        ctx("resident-reuse-conflict"),
    )
    .await
    .expect_err("resident reuse must validate workspace before lookup/mutation");
    assert!(
        matches!(error, ToolError::InvalidArguments(ref message) if message.contains("belongs to Project"))
    );
    let resident_after = harness
        .storage
        .load_session(&resident_id)
        .await
        .expect("reload resident")
        .expect("resident");
    assert_eq!(resident_after.title, resident_before.title);
    assert_eq!(
        serde_json::to_value(&resident_after.messages).expect("after messages JSON"),
        serde_json::to_value(&resident_before.messages).expect("before messages JSON")
    );
    assert_eq!(resident_after.metadata, resident_before.metadata);
    assert_eq!(
        resident_after.metadata_version,
        resident_before.metadata_version
    );
    assert_eq!(
        bamboo_engine::project_context::ProjectContextResolver::project_id_from_session(
            &resident_after
        )
        .as_ref(),
        Some(&project_a.id)
    );
    assert_ne!(
        bamboo_engine::project_context::ProjectContextResolver::project_id_from_session(
            &resident_after
        )
        .as_ref(),
        Some(&project_b.id)
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::get_workspace(&resident_id),
        runtime_before
    );
}

#[tokio::test]
async fn resident_reuse_rejects_stale_project_after_root_reassignment_without_mutation() {
    let harness = build_test_harness().await;
    let workspace_a = tempfile::tempdir().expect("workspace A");
    let workspace_b = tempfile::tempdir().expect("workspace B");
    let project_a = harness
        .project_store
        .create_with_bindings(
            "Project A",
            None,
            vec![bamboo_domain::WorkspaceBinding {
                path: workspace_a.path().to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .expect("Project A");
    let project_b = harness
        .project_store
        .create_with_bindings(
            "Project B",
            None,
            vec![bamboo_domain::WorkspaceBinding {
                path: workspace_b.path().to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .expect("Project B");
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    parent.set_project_id_meta(project_a.id.to_string());
    parent.set_workspace_path_meta(workspace_a.path().to_string_lossy().into_owned());
    parent.workspace = Some(workspace_a.path().to_string_lossy().into_owned());
    harness.storage.save_session(&parent).await.unwrap();
    let context = |tool_call_id: &'static str| {
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    };
    let args = |workspace: &std::path::Path, title: &str| {
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "project-stable-resident",
            "title": title,
            "responsibility": "Stay inside the parent Project",
            "prompt": "Inspect the workspace.",
            "workspace": workspace,
            "auto_run": false
        })
    };

    let created = invoke_completed(
        &harness.tool,
        args(workspace_a.path(), "Project A resident"),
        context("resident-project-a"),
    )
    .await
    .expect("create Project A resident");
    let created: serde_json::Value = serde_json::from_str(&created.result).unwrap();
    let resident_id = created["child_session_id"].as_str().unwrap().to_string();
    let before = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .unwrap();
    harness
        .adapter
        .session_store
        .save_session(&before)
        .await
        .expect("index resident");
    let runtime_before = bamboo_agent_core::workspace_state::peek_workspace(&resident_id);

    parent.set_project_id_meta(project_b.id.to_string());
    parent.set_workspace_path_meta(workspace_b.path().to_string_lossy().into_owned());
    parent.workspace = Some(workspace_b.path().to_string_lossy().into_owned());
    harness.storage.save_session(&parent).await.unwrap();

    let error = invoke_completed(
        &harness.tool,
        args(workspace_b.path(), "Must not cross Project"),
        context("resident-project-b"),
    )
    .await
    .expect_err("stale resident must not be silently reassigned");
    assert!(
        matches!(error, ToolError::InvalidArguments(ref message) if message.contains("resident_project_scope_conflict"))
    );
    let after = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&resident_id),
        runtime_before
    );
}

#[tokio::test]
async fn same_project_resident_reuse_persists_and_publishes_changed_workspace() {
    let harness = build_test_harness().await;
    let workspace_a = tempfile::tempdir().expect("workspace A");
    let workspace_b = tempfile::tempdir().expect("workspace B");
    let project = harness
        .project_store
        .create_with_bindings(
            "Shared Project",
            None,
            vec![
                bamboo_domain::WorkspaceBinding {
                    path: workspace_a.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                },
                bamboo_domain::WorkspaceBinding {
                    path: workspace_b.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                },
            ],
        )
        .expect("Project");
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    parent.set_project_id_meta(project.id.to_string());
    parent.set_workspace_path_meta(workspace_a.path().to_string_lossy().into_owned());
    parent.workspace = Some(workspace_a.path().to_string_lossy().into_owned());
    harness.storage.save_session(&parent).await.unwrap();
    let context = |tool_call_id: &'static str| {
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    };
    let args = |workspace: &std::path::Path, title: &str| {
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "workspace-switcher",
            "title": title,
            "responsibility": "Inspect a Project workspace",
            "prompt": "Inspect the requested workspace.",
            "workspace": workspace,
            "auto_run": false
        })
    };

    let created = invoke_completed(
        &harness.tool,
        args(workspace_a.path(), "Workspace A"),
        context("resident-workspace-a"),
    )
    .await
    .expect("create resident");
    let created: serde_json::Value = serde_json::from_str(&created.result).unwrap();
    let resident_id = created["child_session_id"].as_str().unwrap().to_string();
    let initial = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .unwrap();
    harness
        .adapter
        .session_store
        .save_session(&initial)
        .await
        .expect("index resident");

    let reused = invoke_completed(
        &harness.tool,
        args(workspace_b.path(), "Workspace B"),
        context("resident-workspace-b"),
    )
    .await
    .expect("reuse resident");
    let reused: serde_json::Value = serde_json::from_str(&reused.result).unwrap();
    assert_eq!(reused["child_session_id"], resident_id);
    assert_eq!(reused["reused"], true);

    let child = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .unwrap();
    let workspace_b = workspace_b.path().canonicalize().unwrap();
    let workspace_b_display = bamboo_config::paths::path_to_display_string(&workspace_b);
    assert_eq!(
        child.workspace.as_deref(),
        Some(workspace_b_display.as_str())
    );
    assert_eq!(
        child.workspace_path_meta().as_deref(),
        Some(workspace_b_display.as_str())
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&resident_id).as_deref(),
        Some(workspace_b.as_path())
    );
    assert_eq!(
        bamboo_engine::project_context::ProjectContextResolver::project_id_from_session(&child)
            .as_ref(),
        Some(&project.id)
    );
}

#[tokio::test]
async fn resident_reuse_publication_uses_the_validating_instance_workspace_root() {
    let instance_root = tempfile::tempdir().expect("instance workspace root");
    let canonical_instance_root = instance_root
        .path()
        .canonicalize()
        .expect("canonical instance workspace root");
    let resolver = bamboo_agent_core::workspace_state::WorkspaceResolver::new(|| None, {
        let root = canonical_instance_root.clone();
        move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
            root: root.clone(),
            confine: true,
        }
    });
    let harness = build_test_harness_with_options(None, Some(resolver.clone())).await;
    let workspace_a = canonical_instance_root.join("workspace-a");
    std::fs::create_dir_all(&workspace_a).expect("workspace A");
    let workspace_b = tempfile::tempdir().expect("foreign workspace B");
    let context = |tool_call_id: &'static str| {
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    };
    let args = |workspace: &std::path::Path, title: &str| {
        json!({
            "action": "create",
            "lifecycle": "resident",
            "name": "instance-confined-resident",
            "title": title,
            "responsibility": "Inspect a confined workspace",
            "prompt": "Inspect the requested workspace.",
            "workspace": workspace,
            "auto_run": false
        })
    };

    let created = invoke_completed(
        &harness.tool,
        args(&workspace_a, "Workspace A"),
        context("instance-resident-create"),
    )
    .await
    .expect("create instance-confined resident");
    let created: serde_json::Value = serde_json::from_str(&created.result).unwrap();
    let resident_id = created["child_session_id"].as_str().unwrap().to_string();
    let initial = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .unwrap();
    harness
        .adapter
        .session_store
        .save_session(&initial)
        .await
        .expect("index resident");

    let reused = invoke_completed(
        &harness.tool,
        args(workspace_b.path(), "Workspace B"),
        context("instance-resident-reuse"),
    )
    .await
    .expect("reuse instance-confined resident");
    let reused: serde_json::Value = serde_json::from_str(&reused.result).unwrap();
    assert_eq!(reused["child_session_id"], resident_id);
    assert_eq!(reused["reused"], true);

    let canonical_workspace_b = workspace_b.path().canonicalize().unwrap();
    let expected = resolver.preview_workspace_path(canonical_workspace_b);
    let expected_display = bamboo_config::paths::path_to_display_string(&expected);
    let child = harness
        .storage
        .load_session(&resident_id)
        .await
        .unwrap()
        .unwrap();
    assert!(expected.starts_with(&canonical_instance_root));
    assert!(
        expected.is_dir(),
        "resident reuse must materialize the instance resolver's relocated target"
    );
    assert_eq!(child.workspace.as_deref(), Some(expected_display.as_str()));
    assert_eq!(
        child.workspace_path_meta().as_deref(),
        Some(expected_display.as_str())
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&resident_id).as_deref(),
        Some(expected.as_path())
    );
}

#[tokio::test]
async fn backward_compat_legacy_subagent_call_without_action_defaults_to_create() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "title": "Legacy Child",
            "responsibility": "Test backward compat",
            "prompt": "Do something",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy()
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_legacy",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("legacy SubAgent call without action should default to create");

    assert!(result.success);
    let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert!(parsed.get("child_session_id").is_some());
}

// -----------------------------------------------------------------------
// Management action tests for the unified SubAgent tool
// -----------------------------------------------------------------------

#[tokio::test]
async fn direct_update_uses_the_canonical_complete_assignment_frame() {
    let harness = build_test_harness().await;
    let task_brief = "Updated complete brief.\nAcceptance: retain both lines.";
    let mut seeded_child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .expect("seed child");
    seeded_child.metadata.insert(
        "responsibility".to_string(),
        "Inspect the existing path".to_string(),
    );
    seeded_child
        .metadata
        .insert("subagent_type".to_string(), "reviewer".to_string());
    seeded_child.metadata.insert(
        "assignment_prompt".to_string(),
        "Original complete brief.".to_string(),
    );
    harness.storage.save_session(&seeded_child).await.unwrap();

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "update",
            "child_session_id": harness.child_session_id,
            "prompt": task_brief,
            "reset_after_update": true,
            "auto_run": false
        }),
        subagent_test_ctx(&harness.parent_session_id, "direct-update-contract"),
    )
    .await
    .expect("direct child update");
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["messages_removed"], 1);

    let child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .expect("updated child");
    assert_eq!(
        child.messages.last().unwrap().content,
        child_session::format_child_assignment(
            "Child session",
            "Inspect the existing path",
            "reviewer",
            task_brief,
        )
    );
    assert_eq!(child.messages.len(), 2);
    assert!(!child
        .messages
        .iter()
        .any(|message| matches!(message.role, Role::Assistant)));
    assert_eq!(
        child.metadata.get("assignment_prompt").map(String::as_str),
        Some(task_brief)
    );
}

#[tokio::test]
async fn send_message_appends_follow_up_without_replacing_history() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let raw_message = "\n  continue with the failing parser path  \n";

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": harness.child_session_id,
            "message": raw_message,
            "auto_run": false
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_send_message",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("send_message should succeed");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["status"], "pending");

    let child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .expect("child session should exist");
    assert_eq!(child.messages.len(), 4);
    assert!(matches!(child.messages[2].role, Role::Assistant));
    assert!(matches!(child.messages[3].role, Role::User));
    assert_eq!(child.messages[3].content, raw_message);
    assert!(!child.messages[3]
        .content
        .contains("Delegated child assignment"));
    assert!(!child.messages[3]
        .content
        .contains(child_session::SUBAGENT_DELEGATION_CONTRACT_VERSION));
    assert_eq!(
        child.metadata.get("last_run_status").map(String::as_str),
        Some("pending")
    );
    let backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(
        backlog.pending + backlog.claimed,
        0,
        "auto_run=false on an idle child must remain a draft and not activate"
    );
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn update_rejects_active_assignment_but_allows_transcript_safe_title() {
    let harness = build_test_harness().await;
    let mut active = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    active.set_last_run_status("running");
    harness.storage.save_session(&active).await.unwrap();
    let original_messages = serde_json::to_value(&active.messages).unwrap();
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    harness
        .agent_runners
        .write()
        .await
        .insert(active.id.clone(), runner);

    let rejected = invoke_completed(
        &harness.tool,
        json!({
            "action": "update",
            "child_session_id": harness.child_session_id,
            "prompt": "replace the task",
            "auto_run": false,
        }),
        subagent_test_ctx(&harness.parent_session_id, "active-assignment-update"),
    )
    .await
    .expect_err("active assignment replacement must fail");
    assert!(
        rejected.to_string().contains("send_message"),
        "error must tell the parent how to steer the live child: {rejected}"
    );

    invoke_completed(
        &harness.tool,
        json!({
            "action": "update",
            "child_session_id": harness.child_session_id,
            "title": "Current analysis",
            "auto_run": false,
        }),
        subagent_test_ctx(&harness.parent_session_id, "active-title-update"),
    )
    .await
    .expect("title-only update is safe while running");
    let saved = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.title, "Current analysis");
    assert_eq!(saved.title_version, active.title_version + 1);
    assert_eq!(saved.metadata_version, active.metadata_version + 1);
    assert_eq!(
        serde_json::to_value(&saved.messages).unwrap(),
        original_messages
    );
    assert_eq!(saved.last_run_status().as_deref(), Some("running"));
}

#[tokio::test]
async fn title_update_and_draft_append_preserve_both_changes() {
    let harness = build_test_harness().await;
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();

    let update = child_session::update_child_action(
        harness.adapter.as_ref(),
        &parent.id,
        harness.child_session_id.clone(),
        Some("Renamed draft".into()),
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    );
    let append = child_session::send_message_to_child_action(
        harness.adapter.as_ref(),
        &parent,
        harness.child_session_id.clone(),
        "Keep this follow-up".into(),
        Some(false),
        None,
        None,
        false,
    );
    let (updated, appended) = tokio::join!(update, append);
    updated.expect("title update");
    appended.expect("draft append");

    let saved = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.title, "Renamed draft");
    assert!(saved
        .messages
        .iter()
        .any(|message| message.content == "Keep this follow-up"));
    assert_eq!(saved.messages.len(), 4);
}

#[tokio::test]
async fn queued_child_update_waits_for_launch_fence_and_rejects_assignment_change() {
    let harness = build_test_harness().await;
    let mut queued = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    queued.set_last_run_status("pending");
    assert_eq!(queued.advance_child_launch_generation(), Some(1));
    queued.mark_child_auto_run_launch_intent();
    harness.storage.save_session(&queued).await.unwrap();
    let original_messages = serde_json::to_value(&queued.messages).unwrap();

    let launch_guard = harness
        .adapter
        .scheduler
        .lock_child_launch(&harness.child_session_id)
        .await;
    let mut update = Box::pin(child_session::update_child_action(
        harness.adapter.as_ref(),
        &harness.parent_session_id,
        harness.child_session_id.clone(),
        None,
        None,
        Some("replace queued assignment".into()),
        None,
        None,
        None,
        None,
        false,
    ));
    assert!(matches!(
        futures::poll!(update.as_mut()),
        std::task::Poll::Pending
    ));
    drop(launch_guard);
    let error = update
        .await
        .expect_err("queued generation must reject update");
    assert!(error.to_string().contains("send_message"));

    child_session::update_child_action(
        harness.adapter.as_ref(),
        &harness.parent_session_id,
        harness.child_session_id.clone(),
        Some("Queued analysis".into()),
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .expect("title-only metadata edit must remain available");
    let saved = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.title, "Queued analysis");
    assert_eq!(saved.recoverable_child_launch_generation(), Some(1));
    assert_eq!(
        serde_json::to_value(&saved.messages).unwrap(),
        original_messages
    );
}

#[tokio::test]
async fn send_message_blank_unknown_child_preserves_not_found_priority() {
    let harness = build_test_harness().await;
    let unknown_child_id = Uuid::new_v4().to_string();

    let error = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": unknown_child_id,
            "message": " \n\t  ",
            "auto_run": false,
            "interrupt_running": true
        }),
        subagent_test_ctx(&harness.parent_session_id, "blank-unknown-child"),
    )
    .await
    .expect_err("unknown child must win over blank-message validation");

    assert!(matches!(error, ToolError::Execution(message)
            if message.contains("session not found") && message.contains(&unknown_child_id)));
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 0);
    assert!(!harness
        .agent_runners
        .read()
        .await
        .contains_key(&unknown_child_id));
}

#[tokio::test]
async fn send_message_rejects_whitespace_only_without_mutating_the_child() {
    let harness = build_test_harness().await;
    let before = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .expect("child before invalid message");

    let error = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": harness.child_session_id,
            "message": " \n\t  ",
            "auto_run": false
        }),
        subagent_test_ctx(&harness.parent_session_id, "whitespace-only-message"),
    )
    .await
    .expect_err("whitespace-only message must fail");
    assert!(
        matches!(error, ToolError::InvalidArguments(message) if message.contains("message must be non-empty"))
    );

    let after = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .expect("child after invalid message");
    assert_eq!(
        serde_json::to_value(after.messages).unwrap(),
        serde_json::to_value(before.messages).unwrap()
    );
    assert_eq!(after.metadata, before.metadata);
}

#[tokio::test]
async fn send_message_queues_on_running_child_without_interrupt() {
    let harness = build_test_harness().await;
    let raw_message = "  continue\nwith exact whitespace  \n";
    let run_id = {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let run_id = runner.run_id.clone();
        runners.insert(harness.child_session_id.clone(), runner);
        run_id
    };
    // The production execution core publishes the same logical owner into the
    // activation router after reserving this exact shared runner slot.
    let _owner_registration = harness
        .activation_router
        .register_run(&harness.child_session_id, &run_id)
        .await
        .unwrap();

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": harness.child_session_id,
            "message": raw_message
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_running",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("send_message should queue message on running child");

    assert!(result.success);
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"], "message_delivered_live");
    assert_eq!(payload["auto_run"], false);
    assert_eq!(payload["message"], raw_message);
    assert!(!payload["message"]
        .as_str()
        .unwrap()
        .contains("Delegated child assignment"));
    assert!(!payload["message"]
        .as_str()
        .unwrap()
        .contains(child_session::SUBAGENT_DELEGATION_CONTRACT_VERSION));

    let child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .expect("child session should exist");
    // The running snapshot is untouched. The typed envelope lives in the
    // canonical durable SessionInbox and the active owner is notified once.
    assert_eq!(child.messages.len(), 3);
    assert!(!child.has_pending_injected_messages());
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 1);
    let claims = harness
        .session_inbox
        .claim(&harness.child_session_id, 1)
        .await
        .expect("typed SessionInbox claim");
    assert_eq!(claims.len(), 1);
    let envelope = &claims[0].envelope;
    assert_eq!(envelope.target_session_id, harness.child_session_id);
    assert_eq!(
        envelope.source,
        bamboo_domain::SessionMessageSource::Session {
            session_id: harness.parent_session_id.clone()
        }
    );
    assert_eq!(
        envelope.kind,
        bamboo_domain::SessionMessageKind::PeerMessage
    );
    assert_eq!(
        envelope.body.clone(),
        bamboo_domain::SessionMessageBody::Content(bamboo_domain::SessionMessageContent::text(
            raw_message
        ))
    );
}

#[tokio::test]
async fn send_message_can_interrupt_running_child() {
    let harness = build_running_child_harness().await;
    let cancel_token = {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let cancel_token = runner.cancel_token.clone();
        runners.insert(harness.child_session_id.clone(), runner);
        cancel_token
    };

    let runners_for_status = harness.agent_runners.clone();
    let storage_for_status = harness.storage.clone();
    let child_id_for_status = harness.child_session_id.clone();
    let waiter = tokio::spawn(async move {
        cancel_token.cancelled().await;
        let mut child = storage_for_status
            .load_session(&child_id_for_status)
            .await
            .unwrap()
            .unwrap();
        child.set_last_run_status("cancelled");
        storage_for_status.save_session(&child).await.unwrap();
        let mut runners = runners_for_status.write().await;
        if let Some(runner) = runners.get_mut(&child_id_for_status) {
            runner.status = AgentStatus::Cancelled;
        }
    });

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": harness.child_session_id,
            "message": "continue from latest state",
            "auto_run": false,
            "interrupt_running": true
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_interrupt_running",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("send_message should interrupt running child");

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("running Child cancellation must signal the waiter")
        .expect("waiter task should finish");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["status"], "pending");
    assert_eq!(payload["auto_run"], false);

    let child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .expect("child session should exist");
    assert!(matches!(
        child.messages.last().map(|m| &m.role),
        Some(Role::User)
    ));
    assert_eq!(
        child.messages.last().map(|m| m.content.as_str()),
        Some("continue from latest state")
    );
    assert_eq!(
        child.metadata.get("last_run_status").map(String::as_str),
        Some("pending")
    );
    let backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(
        backlog.pending + backlog.claimed,
        0,
        "interrupt=true + auto_run=false must remain a draft and not activate"
    );
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn interrupt_running_cancel_before_stop_leaves_old_run_and_inbox_untouched() {
    let harness = build_test_harness().await;
    let cancel_token = {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let cancel_token = runner.cancel_token.clone();
        runners.insert(harness.child_session_id.clone(), runner);
        cancel_token
    };
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.hold_running_probe.store(true, Ordering::SeqCst);
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let child_id = harness.child_session_id.clone();
    let gate = Arc::new(bamboo_domain::AdmissionGate::default());
    let entered = port.running_probe_entered.notified();
    let task = {
        let gate = gate.clone();
        let port = port.clone();
        tokio::spawn(async move {
            child_session::send_message_to_child_action_with_gate(
                port.as_ref(),
                &parent,
                child_id,
                "new task".to_string(),
                Some(true),
                Some(true),
                Some("interrupt-before-stop"),
                true,
                Some(&gate),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("running probe must pause before stop ownership commits");
    gate.cancel_if_pending();
    port.release_running_probe.notify_one();
    let error = task.await.unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("cancelled before child interruption"));
    assert!(!cancel_token.is_cancelled());
    let backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
}

#[tokio::test]
async fn interrupt_running_caller_cancel_after_stop_still_delivers_new_inbox_message() {
    let harness = build_running_child_harness().await;
    let cancel_token = {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let cancel_token = runner.cancel_token.clone();
        runners.insert(harness.child_session_id.clone(), runner);
        cancel_token
    };
    let runners_for_status = harness.agent_runners.clone();
    let storage_for_status = harness.storage.clone();
    let child_id_for_status = harness.child_session_id.clone();
    let waiter = tokio::spawn(async move {
        cancel_token.cancelled().await;
        let mut child = storage_for_status
            .load_session(&child_id_for_status)
            .await
            .unwrap()
            .unwrap();
        child.set_last_run_status("cancelled");
        storage_for_status.save_session(&child).await.unwrap();
        let mut runners = runners_for_status.write().await;
        if let Some(runner) = runners.get_mut(&child_id_for_status) {
            runner.status = AgentStatus::Cancelled;
        }
    });
    let port = Arc::new(WaitOrderPort::new(
        harness.adapter.clone(),
        harness.storage.clone(),
    ));
    port.hold_after_interrupt.store(true, Ordering::SeqCst);
    let tool = Arc::new(SubAgentTool::new(port.clone(), harness.adapter.clone()));
    let parent_id = harness.parent_session_id.clone();
    let child_id = harness.child_session_id.clone();
    let entered = port.interrupt_completed.notified();
    let outer = tokio::spawn(async move {
        invoke_completed(
            &tool,
            json!({
                "action": "send_message",
                "child_session_id": child_id,
                "message": "deliver after stop",
                "interrupt_running": true,
            }),
            subagent_test_ctx(&parent_id, "interrupt-after-stop-cancel"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("old run must stop before caller cancellation");
    outer.abort();
    assert!(outer.await.unwrap_err().is_cancelled());
    port.release_interrupt.notify_one();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("running Child cancellation must signal the waiter")
        .expect("waiter task should finish");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let backlog = harness
                .session_inbox
                .inspect(&harness.child_session_id)
                .await
                .unwrap();
            if backlog.pending + backlog.claimed == 1
                && harness.activation.calls.load(Ordering::SeqCst) == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached owner must deliver after stopping the old run");
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn send_message_can_queue_child_immediately() {
    let mut harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": harness.child_session_id,
            "message": "retry with a narrower scope"
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_queue",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("send_message should queue the child");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["status"], "queued");
    assert_eq!(payload["auto_run"], true);
    assert_eq!(payload["inbox_generation"], 1);
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 1);

    let started_event = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match harness.parent_rx.recv().await {
                Ok(AgentEvent::SubAgentStarted {
                    parent_session_id,
                    child_session_id,
                    ..
                }) => break (parent_session_id, child_session_id),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    panic!("parent stream closed before start event")
                }
            }
        }
    })
    .await
    .expect("should receive SubAgentStarted event");

    assert_eq!(started_event.0, harness.parent_session_id);
    assert_eq!(started_event.1, harness.child_session_id);
    assert!(
        !harness
            .notification_service
            .try_begin_relay(&harness.child_session_id),
        "reserved idle SessionInbox activation should start the child relay"
    );
}

#[tokio::test]
async fn send_message_same_tool_call_retries_activation_without_duplicate_delivery() {
    let harness = build_test_harness().await;
    harness.activation.fail_next();

    let args = json!({
        "action": "send_message",
        "child_session_id": harness.child_session_id,
        "message": "retry the exact durable follow-up"
    });
    let context = || {
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_activation_retry",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    };

    let first = invoke_completed(&harness.tool, args.clone(), context())
        .await
        .expect("durable delivery must be reported even when activation fails");
    let first_payload: serde_json::Value =
        serde_json::from_str(&first.result).expect("first tool result should be JSON");
    assert_eq!(first_payload["status"], "activation_pending");
    assert_eq!(first_payload["inbox_generation"], 1);
    let parent_after_pending = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        parent_after_pending
            .agent_runtime_state
            .as_ref()
            .and_then(|state| state.waiting_for_children.as_ref())
            .is_none(),
        "a durable message with failed activation must not leave an unsatisfiable parent wait"
    );
    assert_eq!(
        first_payload["activation_error"],
        "session activation failed: injected activation failure"
    );
    let message_id = first_payload["message_id"]
        .as_str()
        .expect("stable message id")
        .to_string();
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 1);
    let first_backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .expect("inspect first durable delivery");
    assert_eq!(first_backlog.pending + first_backlog.claimed, 1);
    assert_eq!(first_backlog.generation, 1);
    assert_eq!(first_backlog.activation_generation, 1);

    let second = invoke_completed(&harness.tool, args, context())
        .await
        .expect("same tool call/body must retry activation");
    let second_payload: serde_json::Value =
        serde_json::from_str(&second.result).expect("second tool result should be JSON");
    assert_eq!(second_payload["status"], "queued");
    assert_eq!(second_payload["inbox_generation"], 1);
    assert_eq!(second_payload["message_id"], message_id);
    assert_eq!(second_payload["activation_error"], serde_json::Value::Null);
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 2);
    let second_backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .expect("inspect idempotent retry");
    assert_eq!(second_backlog.generation, 1);
    assert_eq!(second_backlog.activation_generation, 1);
    let message_id =
        bamboo_domain::SessionMessageId::parse(message_id).expect("valid stable message id");
    let second_admitted = harness
        .session_inbox
        .was_admitted(&harness.child_session_id, &message_id)
        .await
        .expect("inspect exact admission receipt");
    assert!(
        (second_backlog.pending + second_backlog.claimed == 1 && !second_admitted)
            || (second_backlog.pending + second_backlog.claimed == 0 && second_admitted),
        "the real activation may still own the one durable claim or may have \
         permanently admitted it, but must not duplicate or lose it: \
         backlog={second_backlog:?}, admitted={second_admitted}"
    );

    let conflicting = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": harness.child_session_id,
            "message": "different follow-up under the same tool-call id"
        }),
        context(),
    )
    .await
    .expect_err("same tool-call id with different body must fail closed");
    assert!(
        conflicting
            .to_string()
            .contains("reused with different delivery semantics"),
        "unexpected error: {conflicting}"
    );
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 2);
    let final_backlog = harness
        .session_inbox
        .inspect(&harness.child_session_id)
        .await
        .expect("inspect after conflicting retry");
    assert_eq!(final_backlog.generation, 1);
    let final_admitted = harness
        .session_inbox
        .was_admitted(&harness.child_session_id, &message_id)
        .await
        .expect("inspect exact admission receipt after conflict");
    assert!(
        (final_backlog.pending + final_backlog.claimed == 1 && !final_admitted)
            || (final_backlog.pending + final_backlog.claimed == 0 && final_admitted),
        "conflicting retry must neither duplicate nor lose the original \
         delivery: backlog={final_backlog:?}, admitted={final_admitted}"
    );
}

#[tokio::test]
async fn send_message_coalesced_retry_preserves_prior_child_wait() {
    let harness = build_test_harness().await;
    harness
        .activation
        .force_disposition(SessionActivationDisposition::ActivationReserved);
    let args = json!({
        "action": "send_message",
        "child_session_id": harness.child_session_id,
        "message": "same durable correction"
    });
    let first = invoke_completed(
        &harness.tool,
        args.clone(),
        subagent_test_ctx(&harness.parent_session_id, "coalesced-retry"),
    )
    .await
    .expect("first delivery queues child");
    let first_payload: serde_json::Value = serde_json::from_str(&first.result).unwrap();
    assert_eq!(first_payload["status"], "queued");

    harness
        .activation
        .force_disposition(SessionActivationDisposition::ActivationCoalesced);
    let retry = invoke_completed(
        &harness.tool,
        args,
        subagent_test_ctx(&harness.parent_session_id, "coalesced-retry"),
    )
    .await
    .expect("idempotent retry is coalesced");
    let retry_payload: serde_json::Value = serde_json::from_str(&retry.result).unwrap();
    assert_eq!(retry_payload["status"], "message_queued");

    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let wait = parent
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .expect("first queued delivery still owns the parent wait");
    assert_eq!(wait.child_session_ids, [harness.child_session_id]);
}

#[tokio::test]
async fn resident_accumulate_coalesced_retry_keeps_synchronous_wait_result() {
    let harness = build_test_harness().await;
    let initial = json!({
        "action": "create",
        "title": "Resident",
        "responsibility": "Handle recurring work",
        "prompt": "First task",
        "workspace": harness.workspace_path.to_string_lossy(),
        "lifecycle": "resident",
        "name": "resident-retry",
        "context": "accumulate",
        "auto_run": false,
    });
    invoke_completed(
        &harness.tool,
        initial,
        subagent_test_ctx(&harness.parent_session_id, "resident-initial"),
    )
    .await
    .expect("create idle resident");

    let retry_args = json!({
        "action": "create",
        "title": "Resident",
        "responsibility": "Handle recurring work",
        "prompt": "Second task",
        "workspace": harness.workspace_path.to_string_lossy(),
        "lifecycle": "resident",
        "name": "resident-retry",
        "context": "accumulate",
        "wait": true,
    });
    harness
        .activation
        .force_disposition(SessionActivationDisposition::ActivationReserved);
    let first = invoke_completed(
        &harness.tool,
        retry_args.clone(),
        subagent_test_ctx(&harness.parent_session_id, "resident-wait-retry"),
    )
    .await
    .expect("first resident activation");
    assert_eq!(
        first.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );

    harness
        .activation
        .force_disposition(SessionActivationDisposition::ActivationCoalesced);
    let retry = invoke_completed(
        &harness.tool,
        retry_args,
        subagent_test_ctx(&harness.parent_session_id, "resident-wait-retry"),
    )
    .await
    .expect("coalesced resident retry");
    assert_eq!(
        retry.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children"),
        "a preserved wait must still suspend the retried resident call"
    );
}

#[tokio::test]
async fn enqueue_child_run_starts_the_notification_relay_for_the_child() {
    // A headless child (nobody subscribed to its own SSE/WS stream) must still
    // get the always-on notification relay started for ITS session id — not
    // just the parent's — so events that only ever appear on the child's own
    // stream (e.g. a background Bash finishing, or critical context pressure
    // inside the child) are classified instead of silently dropped. See the
    // scheduler-owned child launch hook.
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Relay child",
            "responsibility": "Exercise ordinary queued child launch",
            "prompt": "Finish immediately",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy()
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_relay",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("create should enqueue the child and start its relay");
    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("create result should be JSON");
    let child_session_id = payload["child_session_id"]
        .as_str()
        .expect("create result child id");

    // `try_begin_relay` only returns `true` the FIRST time it claims a
    // session id; a second call returning `false` proves a relay is already
    // running for the child — the same technique
    // `session_events::ensure_notification_relay_is_idempotent_and_classifies_events`
    // uses.
    assert!(
        !harness
            .notification_service
            .try_begin_relay(child_session_id),
        "enqueue_child_run should have started a relay for the child session"
    );
}

#[tokio::test]
async fn cancel_stops_running_child() {
    let harness = build_test_harness().await;
    // A genuinely RUNNING child carries status "running"/"pending" (every
    // (re)enqueue path resets it before the run; the terminal status is
    // only written when the run ends). The fixture's stale "completed"
    // would otherwise trip the natural-terminal guard in
    // cancel_child_action, which deliberately refuses to overwrite a
    // completed/error outcome that landed while the cancel was in flight.
    {
        let mut child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .unwrap();
        child.set_last_run_status("running");
        harness.storage.save_session(&child).await.unwrap();
    }
    let cancel_token = {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let token = runner.cancel_token.clone();
        runners.insert(harness.child_session_id.clone(), runner);
        token
    };

    let runners_for_wait = harness.agent_runners.clone();
    let child_id_for_wait = harness.child_session_id.clone();
    let waiter = tokio::spawn(async move {
        cancel_token.cancelled().await;
        let mut runners = runners_for_wait.write().await;
        if let Some(runner) = runners.get_mut(&child_id_for_wait) {
            runner.status = AgentStatus::Cancelled;
        }
    });

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "cancel",
            "child_session_id": harness.child_session_id
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_cancel",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("cancel should succeed");

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("running Child cancellation must signal the waiter")
        .expect("waiter should finish");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["status"], "cancelled");
    assert_eq!(payload["child_session_id"], harness.child_session_id);
}

#[tokio::test]
async fn queued_child_cancel_fences_duplicate_deliveries_and_explicit_retry() {
    let before_entered = Arc::new(tokio::sync::Semaphore::new(0));
    let before_release = Arc::new(tokio::sync::Semaphore::new(0));
    let during_entered = Arc::new(tokio::sync::Semaphore::new(0));
    let during_release = Arc::new(tokio::sync::Semaphore::new(0));
    let hook = Arc::new(QueueBoundaryHook {
        before_child: "queued-target".into(),
        before_once: AtomicBool::new(true),
        before_entered: before_entered.clone(),
        before_release: before_release.clone(),
        during_child: "race-target".into(),
        during_once: AtomicBool::new(true),
        during_entered: during_entered.clone(),
        during_release: during_release.clone(),
    });
    let harness = build_test_harness_with_hook(None, None, true, Some(hook), false).await;
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();

    for id in ["queued-target", "queue-sentinel", "race-target"] {
        let mut child = Session::new_child(id, parent.id.clone(), "gpt-5", id);
        child.add_message(Message::system("child system"));
        child.add_message(Message::user("finish the bounded task"));
        child.set_last_run_status("pending");
        assert_eq!(child.advance_child_launch_generation(), Some(1));
        harness.storage.save_session(&child).await.unwrap();
    }
    let target = harness
        .storage
        .load_session("queued-target")
        .await
        .unwrap()
        .unwrap();
    // Duplicate delivery of one generation must never create a second run.
    harness
        .adapter
        .enqueue_child_run(&parent, &target)
        .await
        .unwrap();
    before_entered.acquire().await.unwrap().forget();
    harness
        .adapter
        .enqueue_child_run(&parent, &target)
        .await
        .unwrap();
    let cancelled = invoke_completed(
        &harness.tool,
        json!({"action":"cancel", "child_session_id":"queued-target"}),
        subagent_test_ctx(&parent.id, "cancel-before-dequeue"),
    )
    .await
    .expect("cancel queued generation");
    let cancelled: serde_json::Value = serde_json::from_str(&cancelled.result).unwrap();
    assert_eq!(cancelled["status"], "cancelled");
    let durable = harness
        .storage
        .load_session("queued-target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.child_launch_generation(), 1);
    assert!(durable.is_child_launch_cancelled(1));
    assert_eq!(durable.last_run_status().as_deref(), Some("cancelled"));

    // A later sentinel completing proves both stale queue copies were
    // dequeued. Neither copy may reserve the target's runner.
    let sentinel = harness
        .storage
        .load_session("queue-sentinel")
        .await
        .unwrap()
        .unwrap();
    harness
        .adapter
        .enqueue_child_run(&parent, &sentinel)
        .await
        .unwrap();
    before_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if harness
                .storage
                .load_session("queue-sentinel")
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref()
                == Some("completed")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sentinel must finish after stale queue entries");
    assert!(harness
        .agent_runners
        .read()
        .await
        .get("queued-target")
        .is_none());

    // A parent's explicit retry prepares a fresh durable generation. It may
    // run even though the old generation remains cancelled.
    child_session::run_child_action(
        harness.adapter.as_ref(),
        &parent,
        "queued-target".into(),
        None,
    )
    .await
    .expect("prepare explicit retry");
    let retry = harness
        .storage
        .load_session("queued-target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.child_launch_generation(), 2);
    assert!(!retry.is_child_launch_cancelled(2));
    harness
        .adapter
        .enqueue_child_run(&parent, &retry)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if harness
                .storage
                .load_session("queued-target")
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref()
                == Some("completed")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("explicit retry must complete");

    // The dequeue hook now stops a second generation after eligibility was
    // checked while the launch guard is held. Cancellation must wait for the
    // reservation handoff, then stop the runner or retain its real terminal.
    let racing = harness
        .storage
        .load_session("race-target")
        .await
        .unwrap()
        .unwrap();
    harness
        .adapter
        .enqueue_child_run(&parent, &racing)
        .await
        .unwrap();
    during_entered.acquire().await.unwrap().forget();
    let adapter = harness.adapter.clone();
    let cancel_race =
        tokio::spawn(async move { adapter.cancel_child_run_and_wait("race-target").await });
    tokio::task::yield_now().await;
    assert!(
        !cancel_race.is_finished(),
        "cancel must wait for dequeue guard"
    );
    during_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), cancel_race)
        .await
        .expect("racing cancellation must finish")
        .unwrap()
        .unwrap();
    let racing = harness
        .storage
        .load_session("race-target")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        racing.last_run_status().as_deref(),
        Some("cancelled" | "completed")
    ));
}

#[tokio::test]
async fn startup_recovers_only_matching_pending_auto_run_intents() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    // Durable snapshots model a crash at the save-before-enqueue boundary.
    // A draft beside the accepted auto-run must remain inert.
    for (id, auto_run) in [("recover-saved", true), ("recover-draft", false)] {
        let mut child = Session::new_child(id, parent.id.clone(), "gpt-5", id);
        child.add_message(Message::system("child system"));
        child.add_message(Message::user("bounded task"));
        child.set_last_run_status("pending");
        child.advance_child_launch_generation().unwrap();
        if auto_run {
            child.mark_child_auto_run_launch_intent();
        }
        harness.storage.save_session(&child).await.unwrap();
    }
    for (id, status, stale, cancelled) in [
        ("recover-running", "running", false, false),
        ("recover-terminal", "completed", false, false),
        ("recover-stale", "pending", true, false),
        ("recover-cancelled", "pending", false, true),
    ] {
        let mut child = Session::new_child(id, parent.id.clone(), "gpt-5", id);
        child.add_message(Message::system("child system"));
        child.add_message(Message::user("bounded task"));
        child.set_last_run_status(status);
        child.advance_child_launch_generation().unwrap();
        child.mark_child_auto_run_launch_intent();
        if stale {
            child.advance_child_launch_generation().unwrap();
        }
        if cancelled {
            child.cancel_child_launch_generation();
        }
        harness.storage.save_session(&child).await.unwrap();
    }
    let saved = harness
        .storage
        .load_session("recover-saved")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.recoverable_child_launch_generation(), Some(1));
    let draft = harness
        .storage
        .load_session("recover-draft")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(draft.recoverable_child_launch_generation(), None);

    let reopened_store = Arc::new(
        SessionStoreV2::new(harness.workspace_path.parent().unwrap().to_path_buf())
            .await
            .unwrap(),
    );
    let reopened_storage: Arc<dyn Storage> = reopened_store.clone();
    let reopened_adapter = ChildSessionAdapter::new(
        reopened_store,
        reopened_storage.clone(),
        Arc::new(bamboo_storage::LockedSessionStore::new(reopened_storage)),
        harness.adapter.scheduler.clone(),
        harness.adapter.sessions_cache.clone(),
        harness.agent_runners.clone(),
        harness.adapter.session_event_senders.clone(),
        harness.adapter.session_messenger.clone(),
        None,
        harness.adapter.config.clone(),
    );
    assert_eq!(
        reopened_adapter
            .reconcile_pending_child_launches()
            .await
            .unwrap(),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if harness
                .storage
                .load_session("recover-saved")
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref()
                == Some("completed")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("saved launch intent should finish after restart");
    assert_eq!(
        reopened_adapter
            .reconcile_pending_child_launches()
            .await
            .unwrap(),
        0
    );
    for (id, status) in [
        ("recover-draft", "pending"),
        ("recover-running", "running"),
        ("recover-terminal", "completed"),
        ("recover-stale", "pending"),
        ("recover-cancelled", "pending"),
    ] {
        assert_eq!(
            harness
                .storage
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref(),
            Some(status),
            "{id} must not launch"
        );
        assert!(harness.agent_runners.read().await.get(id).is_none());
    }
}

#[tokio::test]
async fn repeated_reconcile_during_queued_launch_admits_one_recovery() {
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let hook = Arc::new(QueueBoundaryHook {
        before_child: "recover-queued".into(),
        before_once: AtomicBool::new(true),
        before_entered: entered.clone(),
        before_release: release.clone(),
        during_child: "unused".into(),
        during_once: AtomicBool::new(false),
        during_entered: Arc::new(tokio::sync::Semaphore::new(0)),
        during_release: Arc::new(tokio::sync::Semaphore::new(0)),
    });
    let harness = build_test_harness_with_hook(None, None, true, Some(hook), false).await;
    let mut child = Session::new_child(
        "recover-queued",
        harness.parent_session_id.clone(),
        "gpt-5",
        "recover-queued",
    );
    child.add_message(Message::system("child system"));
    child.add_message(Message::user("bounded task"));
    child.set_last_run_status("pending");
    child.advance_child_launch_generation().unwrap();
    harness.storage.save_session(&child).await.unwrap();

    let parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    // The admission boundary itself must persist the launch promise before
    // acknowledging a queue entry (Plan promotes a draft this way).
    harness
        .adapter
        .enqueue_child_run(&parent, &child)
        .await
        .unwrap();
    assert_eq!(
        harness
            .storage
            .load_session("recover-queued")
            .await
            .unwrap()
            .unwrap()
            .recoverable_child_launch_generation(),
        Some(1)
    );
    entered.acquire().await.unwrap().forget();
    // Model a fresh reconcile while the original admitted job has not run.
    assert_eq!(
        harness
            .adapter
            .reconcile_pending_child_launches()
            .await
            .unwrap(),
        1
    );
    // The first job is admitted but has not reserved a runner. Repeated boot
    // reconciliation must not add another copy to the process queue.
    assert_eq!(
        harness
            .adapter
            .reconcile_pending_child_launches()
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        harness
            .adapter
            .reconcile_pending_child_launches()
            .await
            .unwrap(),
        0
    );
    assert!(harness
        .agent_runners
        .read()
        .await
        .get("recover-queued")
        .is_none());
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if harness
                .storage
                .load_session("recover-queued")
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref()
                == Some("completed")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued launch should finish once released");
    assert_eq!(
        harness
            .adapter
            .reconcile_pending_child_launches()
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn list_returns_children() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({"action": "list"}),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_list",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("list should succeed");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    let children = payload["children"]
        .as_array()
        .expect("list result should have children array");
    assert_eq!(children.len(), 1);
    assert_eq!(children[0]["child_session_id"], harness.child_session_id);
    assert_eq!(payload["count"], 1);
}

#[tokio::test]
async fn get_returns_runner_diagnostics() {
    let harness = build_test_harness().await;

    // Set up a running runner with diagnostic fields populated.
    {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        runner.last_tool_name = Some("Read".to_string());
        runner.last_tool_phase = Some("begin".to_string());
        runner.round_count = 3;
        runners.insert(harness.child_session_id.clone(), runner);
    }

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "get",
            "child_session_id": harness.child_session_id
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_get_diagnostics",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("get should succeed");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["child_session_id"], harness.child_session_id);
    assert_eq!(payload["is_running"], true);
    assert_eq!(payload["last_tool_name"], "Read");
    assert_eq!(payload["last_tool_phase"], "begin");
    assert_eq!(payload["round_count"], 3);
    assert!(payload["runner_started_at"].is_string());
    assert!(payload.get("guidance").is_some());
}

fn child_inspection_ctx(parent_session_id: &str) -> ToolCtx {
    ToolExecutionContext {
        executing_supervisor: None,
        session_id: Some(parent_session_id),
        root_session_id: None,
        tool_call_id: "tool_call_inspect_child",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    }
    .to_tool_ctx()
}

async fn inspect_child(
    tool: &SubAgentTool,
    parent_session_id: &str,
    args: serde_json::Value,
) -> serde_json::Value {
    let result = invoke_completed(tool, args, child_inspection_ctx(parent_session_id))
        .await
        .expect("authorized child inspection");
    serde_json::from_str(&result.result).expect("inspection JSON")
}

#[tokio::test]
async fn child_inspection_pages_long_utf8_result_after_storage_restart() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let answer = "汉字🙂résumé\n".repeat(1600);
    let mut child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    child.add_message(Message::assistant(answer.clone(), None));
    child
        .metadata
        .insert("assignment_prompt".into(), "任务".repeat(4000));
    harness.storage.save_session(&child).await.unwrap();

    let overview = inspect_child(
        &harness.tool,
        &harness.parent_session_id,
        json!({"action": "get", "child_session_id": harness.child_session_id}),
    )
    .await;
    assert!(overview["prompt"].as_str().unwrap().len() <= 2048);
    assert!(overview["truncated_fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "prompt"));
    assert!(overview["inspection_hint"]
        .as_str()
        .unwrap()
        .contains("metadata"));

    let messages = inspect_child(
        &harness.tool,
        &harness.parent_session_id,
        json!({
            "action": "get",
            "child_session_id": harness.child_session_id,
            "view": "messages",
            "limit": 2,
        }),
    )
    .await;
    assert_eq!(messages["messages"].as_array().unwrap().len(), 2);
    assert!(messages["next_cursor"].is_string());
    assert!(
        messages["messages"][0]["content_preview"]
            .as_str()
            .unwrap()
            .len()
            <= 512
    );

    let first_result = inspect_child(
        &harness.tool,
        &harness.parent_session_id,
        json!({
            "action": "get",
            "child_session_id": harness.child_session_id,
            "view": "result",
            "max_bytes": 113,
        }),
    )
    .await;
    assert_eq!(first_result["available"], true);
    assert!(first_result["next_cursor"].is_string());
    assert!(first_result["text"].as_str().unwrap().len() <= 113);

    // Re-open the durable V2 store with a fresh adapter/tool, as a server
    // restart would. The old in-memory cache cannot supply any next slice.
    let bamboo_home = harness.workspace_path.parent().unwrap().to_path_buf();
    let reopened_store = Arc::new(SessionStoreV2::new(bamboo_home).await.unwrap());
    let reopened_storage: Arc<dyn Storage> = reopened_store.clone();
    let reopened_adapter = Arc::new(ChildSessionAdapter {
        session_store: reopened_store,
        storage: reopened_storage.clone(),
        persistence: Arc::new(bamboo_storage::LockedSessionStore::new(reopened_storage)),
        session_messenger: None,
        scheduler: harness.adapter.scheduler.clone(),
        sessions_cache: Arc::default(),
        agent_runners: Arc::new(RwLock::new(HashMap::new())),
        session_event_senders: Arc::new(RwLock::new(HashMap::new())),
        subagent_model_resolver: None,
        config: harness.adapter.config.clone(),
        project_store: harness.adapter.project_store.clone(),
        workspace_resolver: harness.adapter.workspace_resolver.clone(),
        parent_wait_slots: Arc::new(dashmap::DashMap::new()),
        recovered_launches: Arc::new(dashmap::DashMap::new()),
    });
    let reopened_tool = SubAgentTool::new(reopened_adapter.clone(), reopened_adapter);
    let message_next = inspect_child(
        &reopened_tool,
        &harness.parent_session_id,
        json!({
            "action": "get",
            "child_session_id": harness.child_session_id,
            "view": "messages",
            "cursor": messages["next_cursor"],
            "limit": 2,
        }),
    )
    .await;
    assert_eq!(message_next["messages"][0]["index"], 2);

    let mut reassembled = first_result["text"].as_str().unwrap().to_string();
    let mut cursor = first_result["next_cursor"].as_str().map(str::to_string);
    while let Some(next) = cursor {
        let page = inspect_child(
            &reopened_tool,
            &harness.parent_session_id,
            json!({
                "action": "get",
                "child_session_id": harness.child_session_id,
                "view": "result",
                "cursor": next,
                "max_bytes": 113,
            }),
        )
        .await;
        assert!(page["text"].as_str().unwrap().len() <= 113);
        reassembled.push_str(page["text"].as_str().unwrap());
        cursor = page["next_cursor"].as_str().map(str::to_string);
    }
    assert_eq!(reassembled, answer);
}

#[tokio::test]
async fn child_inspection_allows_verified_ancestors_but_not_other_roots() {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let root = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let nested_parent = Session::new_child_of("nested-parent", &root, "gpt-5", "Nested parent");
    let mut grandchild = Session::new_child_of("grandchild", &nested_parent, "gpt-5", "Grandchild");
    grandchild.add_message(Message::assistant("nested answer", None));
    harness.storage.save_session(&nested_parent).await.unwrap();
    harness.storage.save_session(&grandchild).await.unwrap();

    let root_result = invoke_completed(
        &harness.tool,
        json!({"action": "get", "child_session_id": grandchild.id, "view": "result"}),
        child_inspection_ctx(&harness.parent_session_id),
    )
    .await
    .unwrap();
    let root_result: serde_json::Value = serde_json::from_str(&root_result.result).unwrap();
    assert_eq!(root_result["text"], "nested answer");

    let outsider = Session::new("unrelated-root", "gpt-5");
    harness.storage.save_session(&outsider).await.unwrap();
    assert!(invoke_completed(
        &harness.tool,
        json!({"action": "get", "child_session_id": grandchild.id, "view": "result"}),
        child_inspection_ctx(&outsider.id),
    )
    .await
    .is_err());

    let nested = inspect_child(
        &harness.tool,
        &nested_parent.id,
        json!({"action": "get", "child_session_id": grandchild.id, "view": "result"}),
    )
    .await;
    assert_eq!(nested["text"], "nested answer");

    let wrong_cursor = inspect_child(
        &harness.tool,
        &harness.parent_session_id,
        json!({
            "action": "get",
            "child_session_id": harness.child_session_id,
            "view": "messages",
            "limit": 1,
        }),
    )
    .await;
    let cursor_error = invoke_completed(
        &harness.tool,
        json!({
            "action": "get",
            "child_session_id": grandchild.id,
            "view": "messages",
            "cursor": wrong_cursor["next_cursor"],
        }),
        child_inspection_ctx(&nested_parent.id),
    )
    .await
    .unwrap_err();
    assert!(cursor_error.to_string().contains("invalid or stale"));
}

#[tokio::test]
async fn create_returns_duration_hint() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Test Child",
            "responsibility": "Do something",
            "prompt": "Do something useful",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy()
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_create_hint",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("create should succeed");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    let note = payload["note"].as_str().expect("note should be present");
    assert!(
        note.contains("30-120 seconds"),
        "note should contain estimated duration hint: {note}"
    );
    assert!(
        note.contains("send_message"),
        "note should mention send_message: {note}"
    );
    // Default create now runs in the background and does NOT suspend the
    // parent: the result must not carry the waiting_for_children control.
    assert_ne!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children"),
        "default create must not suspend the parent"
    );
    assert_eq!(payload["status"].as_str(), Some("running_in_background"));
}

#[tokio::test]
async fn create_persists_explicit_reasoning_effort_to_child_session() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Reasoning Child",
            "responsibility": "Investigate hard problem",
            "prompt": "Think carefully step by step",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy(),
            "auto_run": false,
            "reasoning_effort": "high"
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_create_with_effort",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("create should succeed");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(
        payload["reasoning_effort"].as_str(),
        Some("high"),
        "tool result should echo the resolved reasoning_effort"
    );

    let child_id = payload["child_session_id"]
        .as_str()
        .expect("child_session_id present")
        .to_string();
    let child = harness
        .storage
        .load_session(&child_id)
        .await
        .expect("child should be persisted")
        .expect("child session should exist");
    assert_eq!(
        child.reasoning_effort,
        Some(bamboo_domain::ReasoningEffort::High),
        "child.reasoning_effort should reflect the explicit override"
    );
}

#[tokio::test]
async fn create_without_reasoning_effort_leaves_child_at_provider_default() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Default Child",
            "responsibility": "Quick lookup",
            "prompt": "Read a file and summarise",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy(),
            "auto_run": false
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_create_default_effort",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("create should succeed");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert!(
        payload["reasoning_effort"].is_null(),
        "tool result should report null reasoning_effort when omitted, got {:?}",
        payload["reasoning_effort"]
    );

    let child_id = payload["child_session_id"]
        .as_str()
        .expect("child_session_id present")
        .to_string();
    let child = harness
        .storage
        .load_session(&child_id)
        .await
        .expect("child should be persisted")
        .expect("child session should exist");
    assert_eq!(
        child.reasoning_effort, None,
        "child.reasoning_effort should stay at None (provider default) when caller omits it; \
             children must NOT inherit the parent's reasoning_effort"
    );
}

#[tokio::test]
async fn update_can_change_reasoning_effort_on_existing_child() {
    let harness = build_test_harness().await;

    // Pre-condition: the seeded child has reasoning_effort = None.
    let seeded = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .expect("seeded child should load")
        .expect("seeded child exists");
    assert_eq!(seeded.reasoning_effort, None);

    let _ = invoke_completed(
        &harness.tool,
        json!({
            "action": "update",
            "child_session_id": harness.child_session_id,
            "reasoning_effort": "max"
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_update_effort",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("update should succeed");

    let updated = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .expect("updated child should load")
        .expect("child still exists");
    assert_eq!(
        updated.reasoning_effort,
        Some(bamboo_domain::ReasoningEffort::Max),
        "update should persist the new reasoning_effort"
    );
}

#[tokio::test]
async fn delete_removes_child() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "delete",
            "child_session_id": harness.child_session_id
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_delete",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("delete should succeed");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["deleted"], true);

    let child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap();
    assert!(child.is_none());
}

#[tokio::test]
async fn create_requires_workspace() {
    let harness = build_test_harness().await;

    let err = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "No Workspace Child",
            "responsibility": "Test workspace validation",
            "prompt": "Do something",
            "subagent_type": "general-purpose"
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_no_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .unwrap_err();

    match err {
        ToolError::InvalidArguments(msg) => {
            assert!(
                msg.contains("workspace"),
                "error should mention workspace: {msg}"
            );
        }
        other => panic!("expected InvalidArguments error, got: {other:?}"),
    }
}

#[tokio::test]
async fn assigned_child_without_parent_workspace_uses_project_path() {
    let harness = build_test_harness().await;
    let project_path = tempfile::tempdir().expect("Project path");
    let project = harness
        .project_store
        .create_with_project_path(
            "Child Project",
            None,
            project_path.path().to_string_lossy(),
            Vec::new(),
        )
        .expect("Project");
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .expect("load parent")
        .expect("parent");
    parent.set_project_id_meta(project.id.to_string());
    parent.workspace = None;
    harness
        .storage
        .save_session(&parent)
        .await
        .expect("save parent");

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Project Default Child",
            "responsibility": "Verify Project fallback",
            "prompt": "Inspect the Project.",
            "auto_run": false
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_project_default_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("assigned child should use Project path");
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    let child_id = payload["child_session_id"].as_str().unwrap();
    let child = harness
        .storage
        .load_session(child_id)
        .await
        .expect("load child")
        .expect("child");
    let expected =
        bamboo_config::paths::path_to_display_string(&project_path.path().canonicalize().unwrap());
    assert_eq!(
        child.workspace_path_meta().as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        child.project_id_meta().as_deref(),
        Some(project.id.as_str())
    );
    assert_eq!(
        child
            .metadata
            .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
            .map(String::as_str),
        Some("project_default")
    );

    let moved_project_path = tempfile::tempdir().expect("Moved Project path");
    let updated = harness
        .project_store
        .update_with_project_path(
            &project.id,
            project.revision,
            moved_project_path.path().to_string_lossy().as_ref(),
            |_| Ok(()),
        )
        .expect("move Project path");
    parent.workspace = Some(expected.clone());
    parent.set_workspace_path_meta(expected);
    parent.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
        bamboo_engine::project_context::WorkspaceSource::ProjectDefault
            .as_str()
            .to_string(),
    );
    harness
        .storage
        .save_session(&parent)
        .await
        .expect("save stale default-derived parent");
    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Moved Project Default Child",
            "responsibility": "Verify current Project fallback",
            "prompt": "Inspect the moved Project.",
            "auto_run": false
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_moved_project_default_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("child should follow Project path CAS");
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    let moved_child = harness
        .storage
        .load_session(payload["child_session_id"].as_str().unwrap())
        .await
        .expect("load moved child")
        .expect("moved child");
    assert_eq!(
        moved_child.workspace_path_meta().as_deref(),
        updated.project_path.as_deref()
    );
}

#[tokio::test]
async fn create_sets_child_workspace() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "create",
            "title": "Workspace Child",
            "responsibility": "Test workspace propagation",
            "prompt": "Do something",
            "subagent_type": "general-purpose",
            "workspace": harness.workspace_path.to_string_lossy(),
            "auto_run": false
        }),
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some(harness.parent_session_id.as_str()),
            root_session_id: None,
            tool_call_id: "tool_call_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("create should succeed with workspace");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    let child_id = payload["child_session_id"]
        .as_str()
        .expect("child_session_id should be present")
        .to_string();

    let child = harness
        .storage
        .load_session(&child_id)
        .await
        .expect("child should be persisted")
        .expect("child session should exist");
    assert_eq!(
        child.workspace,
        Some(harness.workspace_path.to_string_lossy().into_owned()),
        "child workspace should be set from create args"
    );
}

fn child_report_fixture() -> serde_json::Value {
    json!({"version":1,"outcome":"blocked","summary":"Reported claim 🪷",
        "reported_evidence":[{"description":"Not host verified","reference":"/unreadable/reported/path","sha256":null}],
        "reported_verification":[{"check":"focused check","reported_status":"unknown","details":""}],
        "proposals":[],"blockers":["Need a decision"],"open_decisions":[]})
}

#[tokio::test]
async fn compact_chat_creates_a_durable_child_with_the_complete_message() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let mut parent = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let workspace = h.workspace_path.to_string_lossy().into_owned();
    parent.workspace = None;
    parent.set_workspace_path_meta(workspace);
    parent.metadata_version += 1;
    h.storage.save_session(&parent).await.unwrap();
    let port = Arc::new(WaitOrderPort::new(h.adapter.clone(), h.storage.clone()));
    port.expect_wait_on_enqueue.store(false, Ordering::SeqCst);
    port.skip_successful_enqueue.store(true, Ordering::SeqCst);
    let tool = SubAgentTool::new(port.clone(), h.adapter.clone());
    let message = "  Analyze this complete task 🪷\n\nPreserve every instruction and the trailing whitespace.  ";
    let result = invoke_completed(
        &tool,
        json!({"message":message}),
        subagent_test_ctx(&h.parent_session_id, "compact-create"),
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    let child_id = payload["actor_id"].as_str().unwrap();
    assert_eq!(payload["observed_status"], "running_in_background");
    assert_eq!(
        port.last_admit_child_id.read().unwrap().as_deref(),
        Some(child_id)
    );
    let child = h.storage.load_session(child_id).await.unwrap().unwrap();
    assert_eq!(
        child.parent_session_id.as_deref(),
        Some(h.parent_session_id.as_str())
    );
    assert_eq!(child.metadata["assignment_prompt"], message);
    assert!(child
        .messages
        .iter()
        .any(|m| m.role == Role::User && m.content.contains(message)));
    assert_eq!(
        child.workspace.as_deref(),
        Some(h.workspace_path.to_str().unwrap())
    );
    assert!(serde_json::to_vec(&result).unwrap().len() <= 8192);
    assert!(payload.get("child_session_id").is_none());
    assert!(payload.get("runtime_kind").is_none());
    let schema = tool.parameters_schema();
    assert_eq!(schema["properties"].as_object().unwrap().len(), 5);
    assert!(schema["properties"].get("action").is_none());
}

#[tokio::test]
async fn compact_owned_inspection_correction_and_control_keep_one_logical_child() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let mut child = h
        .storage
        .load_session(&h.child_session_id)
        .await
        .unwrap()
        .unwrap();
    child.metadata.insert(
        "external.agent_id".into(),
        "physical-worker-sentinel".into(),
    );
    child.set_last_run_error("physical-endpoint-sentinel");
    child.metadata_version += 1;
    h.storage.save_session(&child).await.unwrap();
    for query in [
        "overview".to_string(),
        "diagnostics".to_string(),
        "messages".to_string(),
        "result".to_string(),
        "error".to_string(),
    ] {
        let result = invoke_completed(
            &h.tool,
            json!({"intent":"inspect", "target":h.child_session_id, "message":query}),
            subagent_test_ctx(&h.parent_session_id, "compact-inspect"),
        )
        .await
        .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["actor_id"], h.child_session_id);
        assert!(!result.result.contains("physical-worker-sentinel"));
        assert!(!result.result.contains("physical-endpoint-sentinel"));
        assert!(serde_json::to_vec(&result).unwrap().len() <= 8192);
        if query == "diagnostics" {
            assert_eq!(payload["permission"]["available"], true);
            assert_eq!(payload["permission"]["status"], "none");
        }
        if query == "messages" {
            assert_eq!(payload["messages"].as_array().unwrap().len(), 1);
            let cursor = payload["next_cursor"].as_str().unwrap();
            let page = invoke_completed(
                &h.tool,
                json!({"intent":"inspect", "target":h.child_session_id,
                "message":json!({"view":"messages", "cursor":cursor}).to_string()}),
                subagent_test_ctx(&h.parent_session_id, "compact-page"),
            )
            .await
            .unwrap();
            let page: serde_json::Value = serde_json::from_str(&page.result).unwrap();
            assert_eq!(page["messages"][0]["index"], 1);
            assert_eq!(page["messages"][0]["content_preview"], "initial assignment");
        }
        if query == "result" {
            assert_eq!(payload["text"], "initial answer");
        }
    }
    let legacy = invoke_completed(
        &h.tool,
        json!({"action":"get", "child_session_id":h.child_session_id}),
        subagent_test_ctx(&h.parent_session_id, "legacy-inspect"),
    )
    .await
    .unwrap();
    let legacy: serde_json::Value = serde_json::from_str(&legacy.result).unwrap();
    assert_eq!(legacy["child_session_id"], h.child_session_id);
    assert_eq!(legacy["external_agent_id"], "physical-worker-sentinel");

    let foreign = Session::new("other-root", "gpt-5");
    h.storage.save_session(&foreign).await.unwrap();
    assert!(invoke_completed(
        &h.tool,
        json!({"intent":"inspect", "target":h.child_session_id}),
        subagent_test_ctx(&foreign.id, "foreign-inspect")
    )
    .await
    .is_err());
    let before = h
        .storage
        .load_session(&h.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(invoke_completed(
        &h.tool,
        json!({"intent":"control", "target":h.child_session_id, "message":"cancel"}),
        subagent_test_ctx(&foreign.id, "foreign-cancel")
    )
    .await
    .is_err());
    let after = h
        .storage
        .load_session(&h.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    let correction = "  Keep the same child 🪷\nDo not broaden the task.  ";
    h.activation
        .force_disposition(SessionActivationDisposition::ActiveNotified);
    let delivered = invoke_completed(
        &h.tool,
        json!({"target":h.child_session_id, "message":correction}),
        subagent_test_ctx(&h.parent_session_id, "compact-correction"),
    )
    .await
    .unwrap();
    let delivered: serde_json::Value = serde_json::from_str(&delivered.result).unwrap();
    assert_eq!(delivered["actor_id"], h.child_session_id);
    assert_eq!(delivered["observed_status"], "message_delivered_live");
    let claims = h.session_inbox.claim(&h.child_session_id, 1).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0].envelope.id.as_str(),
        delivered["delivery_message_id"].as_str().unwrap()
    );
    assert_eq!(
        claims[0].envelope.body,
        bamboo_domain::SessionMessageBody::Content(bamboo_domain::SessionMessageContent::text(
            correction
        ))
    );

    let cancelled = invoke_completed(
        &h.tool,
        json!({"intent":"control", "target":h.child_session_id, "message":"cancel"}),
        subagent_test_ctx(&h.parent_session_id, "compact-cancel"),
    )
    .await
    .unwrap();
    let cancelled: serde_json::Value = serde_json::from_str(&cancelled.result).unwrap();
    assert_eq!(cancelled["actor_id"], h.child_session_id);
    assert_eq!(cancelled["observed_status"], "completed");
    let port = Arc::new(WaitOrderPort::new(h.adapter.clone(), h.storage.clone()));
    port.skip_successful_enqueue.store(true, Ordering::SeqCst);
    let retry_tool = SubAgentTool::new(port.clone(), h.adapter.clone());
    let retried = invoke_completed(
        &retry_tool,
        json!({"intent":"control", "target":h.child_session_id, "message":"retry"}),
        subagent_test_ctx(&h.parent_session_id, "compact-retry"),
    )
    .await
    .unwrap();
    let retried: serde_json::Value = serde_json::from_str(&retried.result).unwrap();
    assert_eq!(retried["actor_id"], h.child_session_id);
    assert_eq!(retried["runtime_control"], "waiting_for_children");
    assert_eq!(
        port.last_admit_child_id.read().unwrap().as_deref(),
        Some(h.child_session_id.as_str())
    );
    assert_eq!(h.adapter.list_children(&h.parent_session_id).await.len(), 1);
    assert_eq!(
        h.storage
            .load_session(&h.child_session_id)
            .await
            .unwrap()
            .unwrap()
            .created_at,
        child.created_at
    );
}

#[tokio::test]
async fn compact_tree_bounds_owned_observations_without_physical_identity() {
    let h = build_test_harness_with_storage(None, None, true).await;
    for n in 0..35 {
        let child = Session::new_child(
            format!("tree-child-{n}"),
            h.parent_session_id.clone(),
            "gpt-5",
            "Tree child",
        );
        h.storage.save_session(&child).await.unwrap();
    }
    let foreign = Session::new("foreign-root", "gpt-5");
    h.storage.save_session(&foreign).await.unwrap();
    let foreign_child = Session::new_child("foreign-child", foreign.id, "gpt-5", "Not owned");
    h.storage.save_session(&foreign_child).await.unwrap();
    let result = invoke_completed(
        &h.tool,
        json!({"intent":"inspect"}),
        subagent_test_ctx(&h.parent_session_id, "compact-tree"),
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["actor_id"], h.parent_session_id);
    assert_eq!(payload["truncated"], true);
    let nodes = payload["nodes"].as_array().unwrap();
    assert!(nodes.len() <= 32);
    assert_eq!(nodes[0]["actor_id"], h.parent_session_id);
    assert!(nodes
        .iter()
        .skip(1)
        .all(|node| node["parent_actor_id"] == h.parent_session_id));
    assert!(nodes
        .iter()
        .all(|node| node.get("parent_actor_id").is_some()));
    assert!(!result.result.contains("foreign-child"));
    assert!(serde_json::to_vec(&result).unwrap().len() <= 8192);
}

#[tokio::test]
async fn compact_child_tree_reads_only_its_durable_owned_subtree() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let child = h
        .storage
        .load_session(&h.child_session_id)
        .await
        .unwrap()
        .unwrap();
    let sibling = Session::new_child("tree-sibling", &h.parent_session_id, "gpt-5", "Sibling");
    let grandchild = Session::new_child_of("tree-grandchild", &child, "gpt-5", "Grandchild");
    let great_grandchild = Session::new_child_of(
        "tree-great-grandchild",
        &grandchild,
        "gpt-5",
        "Great grandchild",
    );
    for session in [&sibling, &grandchild, &great_grandchild] {
        h.storage.save_session(session).await.unwrap();
    }

    let result = invoke_completed(
        &h.tool,
        json!({"intent":"inspect"}),
        subagent_test_ctx(&child.id, "child-owned-tree"),
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["actor_id"], child.id);
    let nodes = payload["nodes"].as_array().unwrap();
    let actor_ids = nodes
        .iter()
        .map(|node| node["actor_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actor_ids,
        ["child-session", "tree-grandchild", "tree-great-grandchild"]
    );
    assert_eq!(nodes[0]["parent_actor_id"], serde_json::Value::Null);
    assert_eq!(nodes[1]["parent_actor_id"], child.id);
    assert_eq!(nodes[2]["depth"], 2);
    assert!(!result.result.contains("tree-sibling"));
    assert!(!result.result.contains(&h.parent_session_id));

    let root_result = invoke_completed(
        &h.tool,
        json!({"intent":"inspect"}),
        subagent_test_ctx(&h.parent_session_id, "root-full-tree"),
    )
    .await
    .unwrap();
    let root_payload: serde_json::Value = serde_json::from_str(&root_result.result).unwrap();
    let root_ids = root_payload["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node["actor_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(root_ids.contains(&"tree-sibling"));
    assert!(root_ids.contains(&"tree-great-grandchild"));
}

async fn write_tree_runtime_fixture(h: &TestHarness, session: &Session) {
    // A forged or stale index/runtime pair cannot be produced by a normal
    // save: Storage rejects immutable Child lineage and Project changes. Put
    // the bad durable runtime snapshot on disk to exercise the read boundary.
    let entry = h
        .adapter
        .session_store
        .get_index_entry(&session.id)
        .await
        .unwrap();
    let path = h
        .adapter
        .session_store
        .bamboo_home_dir()
        .join(entry.rel_path)
        .join("runtime.json");
    tokio::fs::write(path, serde_json::to_vec(session).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn compact_child_tree_rejects_project_and_root_spoofing() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let mut root = Session::new("tree-project-root", "gpt-5");
    root.set_project_id_meta("project-a");
    h.storage.save_session(&root).await.unwrap();
    let mut child = Session::new_child_of("tree-project-parent", &root, "gpt-5", "Scoped");
    child.set_project_id_meta("project-a");
    h.storage.save_session(&child).await.unwrap();
    let mut grandchild = Session::new_child_of("tree-project-child", &child, "gpt-5", "Scoped");
    grandchild.set_project_id_meta("project-a");
    h.storage.save_session(&grandchild).await.unwrap();
    let child_id = child.id.clone();
    let inspect = || {
        invoke_completed(
            &h.tool,
            json!({"intent":"inspect"}),
            subagent_test_ctx(&child_id, "child-tree-scope"),
        )
    };
    assert!(inspect().await.is_ok());

    grandchild.set_project_id_meta("project-b");
    write_tree_runtime_fixture(&h, &grandchild).await;
    assert!(inspect().await.is_err());
    grandchild.set_project_id_meta("project-a");
    grandchild.root_session_id = "foreign-root".into();
    write_tree_runtime_fixture(&h, &grandchild).await;
    assert!(inspect().await.is_err());
    grandchild.root_session_id = root.id.clone();
    write_tree_runtime_fixture(&h, &grandchild).await;

    child.set_project_id_meta("project-b");
    write_tree_runtime_fixture(&h, &child).await;
    assert!(inspect().await.is_err());
    child.set_project_id_meta("project-a");
    write_tree_runtime_fixture(&h, &child).await;
    child
        .metadata
        .insert("project_id".into(), "project-b".into());
    write_tree_runtime_fixture(&h, &child).await;
    assert!(inspect().await.is_err());
}

#[tokio::test]
async fn compact_child_tree_cursor_rejects_recreated_descendant() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let child = h
        .storage
        .load_session(&h.child_session_id)
        .await
        .unwrap()
        .unwrap();
    let mut descendants = Vec::new();
    for index in 0..35 {
        let grandchild = Session::new_child_of(
            format!("tree-page-{index:02}"),
            &child,
            "gpt-5",
            "Same title",
        );
        h.storage.save_session(&grandchild).await.unwrap();
        descendants.push(grandchild);
    }
    let first = invoke_completed(
        &h.tool,
        json!({"intent":"inspect"}),
        subagent_test_ctx(&child.id, "child-tree-first-page"),
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&first.result).unwrap();
    assert!(payload["nodes"].as_array().unwrap().len() <= 32);
    let cursor = payload["next_cursor"].as_str().unwrap().to_owned();
    let page = |cursor: &str| {
        invoke_completed(
            &h.tool,
            json!({"intent":"inspect", "message":json!({"view":"tree", "cursor":cursor}).to_string()}),
            subagent_test_ctx(&child.id, "child-tree-next-page"),
        )
    };
    assert!(page(&cursor).await.is_ok());

    descendants[0].created_at += chrono::Duration::seconds(1);
    write_tree_runtime_fixture(&h, &descendants[0]).await;
    assert!(page(&cursor).await.is_err());
}

async fn required_result_harness() -> (
    TestHarness,
    Session,
    Session,
    bamboo_domain::ChildContextBinding,
) {
    let harness = build_test_harness_with_storage(None, None, true).await;
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let mut user = Message::user("Do not expand scope");
    user.id = "required-source".into();
    let mut background = Message::assistant("optional facts", None);
    background.id = "optional-source".into();
    parent.messages.extend([user, background]);
    harness.storage.save_session(&parent).await.unwrap();
    let packet = bamboo_domain::ChildContextPacket {
        version: 1,
        objective: "Report bounded result".into(),
        constraints: vec![],
        acceptance: vec!["Return the strict report".into()],
        non_goals: vec![],
        necessary_user_instructions: vec![],
        recorded_decisions: vec![],
        source_user_message_ids: vec!["required-source".into()],
        background_message_ids: vec!["optional-source".into()],
    };
    let mut child = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    let resolved = packet.resolve(&parent, "Produce report").unwrap();
    let binding = bamboo_domain::ChildContextBinding::new(
        &parent,
        &child.id,
        resolved.required_brief.clone(),
        resolved,
    )
    .unwrap();
    binding.install(&mut child).unwrap();
    child.messages = vec![
        Message::system("Child"),
        binding.assignment_message(),
        Message::assistant(child_report_fixture().to_string(), None),
    ];
    child.set_last_run_status("completed");
    harness.storage.save_session(&child).await.unwrap();
    (harness, parent, child, binding)
}
fn result_args(child: &Session, binding: &bamboo_domain::ChildContextBinding) -> serde_json::Value {
    json!({"action":"get","child_session_id":child.id,"view":"typed_result",
        "expected_child_created_at":child.created_at,"expected_assignment_sha256":binding.assignment_sha256})
}
fn result_files(directory: &std::path::Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn visit(path: &std::path::Path, result: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, result);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                result.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
    }
    let mut result = Default::default();
    visit(directory, &mut result);
    result
}
async fn assert_result_unavailable(h: &TestHarness, args: serde_json::Value, reason: &str) {
    let directory = h.workspace_path.parent().unwrap().join("sessions");
    let before = result_files(&directory);
    let result = invoke_completed(&h.tool, args, child_inspection_ctx(&h.parent_session_id))
        .await
        .unwrap();
    assert!(serde_json::to_vec(&result).unwrap().len() <= 8192);
    let value: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(value["available"], false);
    assert_eq!(value["reason"], reason);
    assert!(value.get("child_report").is_none());
    assert_eq!(result_files(&directory), before);
}

#[tokio::test]
async fn typed_child_result_rejects_latest_nonfinal_without_older_winner() {
    let (h, _, child, binding) = required_result_harness().await;
    let args = result_args(&child, &binding);
    let value = inspect_child(&h.tool, &h.parent_session_id, args.clone()).await;
    assert_eq!(value["child_report"]["outcome"], "blocked");
    assert_eq!(value["host_observation"]["last_run_status"], "completed");
    for variant in 0..10 {
        let mut changed = child.clone();
        match variant {
            0 => changed.add_message(Message::assistant("Newest malformed report", None)),
            1 => changed.add_message(Message::user("Later task")),
            2 => {
                changed.messages.last_mut().unwrap().phase =
                    Some(bamboo_domain::MessagePhase::Commentary)
            }
            3 => changed.messages.last_mut().unwrap().compressed = true,
            4 => changed.messages.last_mut().unwrap().compressed_by_event_id = Some("event".into()),
            5 => changed.messages.last_mut().unwrap().compression_level = 1,
            6 => changed.set_last_run_status("running"),
            7 => changed.set_last_run_status("error"),
            8 => {
                changed.messages.last_mut().unwrap().content_parts =
                    Some(vec![serde_json::from_value(
                        json!({"type":"text","text":"extra"}),
                    )
                    .unwrap()])
            }
            _ => {
                changed.messages.last_mut().unwrap().tool_calls =
                    Some(vec![bamboo_agent_core::tools::ToolCall {
                        id: "call".into(),
                        tool_type: "function".into(),
                        function: bamboo_agent_core::tools::FunctionCall {
                            name: "Read".into(),
                            arguments: "{}".into(),
                        },
                    }])
            }
        }
        h.storage.save_session(&changed).await.unwrap();
        assert_result_unavailable(
            &h,
            args.clone(),
            if variant == 0 {
                "report_malformed"
            } else {
                "report_not_current_final"
            },
        )
        .await;
    }
    let mut changed = child.clone();
    changed.messages.retain(|m| m.role != Role::Assistant);
    h.storage.save_session(&changed).await.unwrap();
    assert_result_unavailable(&h, args, "report_absent").await;
}

#[tokio::test]
async fn typed_child_result_checks_authority_selectors_and_durable_context() {
    let (h, parent, child, binding) = required_result_harness().await;
    let args = result_args(&child, &binding);
    for view in ["result_binding", "typed_result"] {
        let foreign = invoke_completed(
            &h.tool,
            json!({"action":"get","child_session_id":child.id,"view":view,
            "expected_assignment_sha256":"malformed"}),
            child_inspection_ctx("foreign-root"),
        )
        .await
        .unwrap_err();
        assert!(foreign.to_string().contains("does not belong to parent"));
    }
    let nested = Session::new_child_of("result-nested", &parent, "model", "nested");
    let grandchild = Session::new_child_of("result-grandchild", &nested, "model", "grandchild");
    h.storage.save_session(&nested).await.unwrap();
    h.storage.save_session(&grandchild).await.unwrap();
    assert!(invoke_completed(
        &h.tool,
        json!({"action":"get","child_session_id":grandchild.id,"view":"typed_result",
        "expected_child_created_at":"bad"}),
        child_inspection_ctx(&parent.id)
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("does not belong to parent"));
    for bad in [
        json!({}),
        json!({"expected_child_created_at":"bad","expected_assignment_sha256":"a".repeat(64)}),
        json!({"expected_child_created_at":child.created_at,"expected_assignment_sha256":"A".repeat(64)}),
        json!({"expected_child_created_at":child.created_at,"expected_assignment_sha256":binding.assignment_sha256,"cursor":null}),
        json!({"expected_child_created_at":child.created_at,"expected_assignment_sha256":binding.assignment_sha256,"parent_session_id":parent.id}),
    ] {
        let mut input = json!({"action":"get","child_session_id":child.id,"view":"typed_result"});
        input
            .as_object_mut()
            .unwrap()
            .extend(bad.as_object().unwrap().clone());
        assert!(
            invoke_completed(&h.tool, input, child_inspection_ctx(&parent.id))
                .await
                .is_err()
        );
    }
    let mut wrong = args.clone();
    wrong["expected_child_created_at"] = json!(child.created_at + chrono::Duration::nanoseconds(1));
    assert_result_unavailable(&h, wrong, "stale_result_selector").await;
    wrong = args.clone();
    wrong["expected_assignment_sha256"] = json!("a".repeat(64));
    assert_result_unavailable(&h, wrong, "stale_result_selector").await;
    let discovered = inspect_child(
        &h.tool,
        &parent.id,
        json!({"action":"get","child_session_id":child.id,"view":"result_binding"}),
    )
    .await;
    assert_eq!(discovered["assignment_sha256"], binding.assignment_sha256);
    assert_eq!(discovered["child_created_at"], json!(child.created_at));
    // Explicit trusted Store fault injection. Inspect must never repair it.
    let runtime = h
        .workspace_path
        .parent()
        .unwrap()
        .join("sessions")
        .join(&parent.id)
        .join("children")
        .join(&child.id)
        .join("runtime.json");
    let original = std::fs::read(&runtime).unwrap();
    for (key, value, reason) in [
        (
            "child.context_packet.binding.v1",
            json!("corrupt"),
            "result_binding_invalid",
        ),
        ("lifecycle", json!("resident"), "typed_result_unsupported"),
        ("project_id", json!("bad/path"), "stale_parent_context"),
        ("project_id", json!("other-project"), "stale_parent_context"),
    ] {
        let mut raw: serde_json::Value = serde_json::from_slice(&original).unwrap();
        raw["metadata"][key] = value;
        std::fs::write(&runtime, serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_result_unavailable(&h, args.clone(), reason).await;
    }
    std::fs::write(&runtime, &original).unwrap();
    for variant in 0..6 {
        let mut changed = parent.clone();
        match variant {
            0 | 1 => changed.messages[variant].content.push('!'),
            2 | 3 => {
                changed.messages.remove(variant - 2);
            }
            _ => changed.messages.push(parent.messages[variant - 4].clone()),
        }
        h.storage.save_session(&changed).await.unwrap();
        assert_result_unavailable(&h, args.clone(), "stale_parent_context").await;
    }
    h.storage.save_session(&parent).await.unwrap();
    let mut raw: serde_json::Value = serde_json::from_slice(&original).unwrap();
    raw["created_at"] = json!(child.created_at + chrono::Duration::nanoseconds(1));
    std::fs::write(&runtime, serde_json::to_vec(&raw).unwrap()).unwrap();
    assert_result_unavailable(&h, args.clone(), "stale_result_selector").await;
    for key in ["spawn_depth", "root_session_id"] {
        let mut raw: serde_json::Value = serde_json::from_slice(&original).unwrap();
        raw[key] = if key == "spawn_depth" {
            json!(child.spawn_depth + 1)
        } else {
            json!("foreign-root")
        };
        std::fs::write(&runtime, serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_result_unavailable(&h, args.clone(), "stale_parent_context").await;
    }
    std::fs::write(&runtime, &original).unwrap();
    // A coherent trusted replacement binding is still stale against the actual parent.
    for variant in 0..2 {
        use sha2::{Digest, Sha256};
        let mut wrong_binding = binding.clone();
        if variant == 0 {
            wrong_binding.payload.parent_created_at += chrono::Duration::nanoseconds(1);
        } else {
            wrong_binding
                .payload
                .sources
                .push(binding.payload.sources[0].clone());
        }
        let mut digest = Sha256::new();
        digest.update(b"bamboo/immutable-child-assignment/v1\0");
        digest.update(serde_json::to_vec(&wrong_binding.payload).unwrap());
        wrong_binding.assignment_sha256 = hex::encode(digest.finalize());
        let mut changed = child.clone();
        wrong_binding.install(&mut changed).unwrap();
        changed.messages[1] = wrong_binding.assignment_message();
        h.storage.save_session(&changed).await.unwrap();
        assert_result_unavailable(
            &h,
            result_args(&changed, &wrong_binding),
            "stale_parent_context",
        )
        .await;
    }
    h.storage.save_session(&child).await.unwrap();
    let mut changed = child.clone();
    let assignment = changed
        .messages
        .iter_mut()
        .find(|m| m.id == binding.assignment_message().id)
        .unwrap();
    assignment.content.push('!');
    h.storage.save_session(&changed).await.unwrap();
    assert_result_unavailable(&h, args.clone(), "result_binding_invalid").await;
    let main = runtime.with_file_name("session.json");
    let original_main = std::fs::read(&main).unwrap();
    std::fs::write(&main, b"not JSON").unwrap();
    assert!(
        invoke_completed(&h.tool, args.clone(), child_inspection_ctx(&parent.id))
            .await
            .unwrap_err()
            .to_string()
            .contains("failed to load child")
    );
    std::fs::write(&main, &original_main).unwrap();
    // A real same-ID recreation has coherent Main/Runtime birth, but cannot win an old selector.
    h.storage.delete_session(&child.id).await.unwrap();
    let mut replacement = Session::new_child_of(&child.id, &parent, "gpt-5", "replacement");
    assert_ne!(replacement.created_at, child.created_at);
    binding.install(&mut replacement).unwrap();
    replacement.messages = vec![
        binding.assignment_message(),
        Message::assistant(child_report_fixture().to_string(), None),
    ];
    replacement.set_last_run_status("completed");
    h.storage.save_session(&replacement).await.unwrap();
    assert_result_unavailable(&h, args.clone(), "stale_result_selector").await;
    h.storage.delete_session(&parent.id).await.unwrap();
    h.storage
        .recreate_root_session(&parent.id, "model")
        .await
        .unwrap();
    assert!(
        invoke_completed(&h.tool, args, child_inspection_ctx(&parent.id))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typed_child_result_enforces_actual_double_escaped_tool_budget() {
    let (h, _, child, binding) = required_result_harness().await;
    let args = result_args(&child, &binding);
    let base = inspect_child(&h.tool, &h.parent_session_id, args.clone()).await;
    let mut last_valid = 0;
    let mut double_escaped_rejected = false;
    for count in (1..1001).step_by(16) {
        let mut report = child_report_fixture();
        report["proposals"] = json!(vec!["\"".repeat(count); 3]);
        assert!(report.to_string().len() <= 8192);
        let mut changed = child.clone();
        changed.messages.last_mut().unwrap().content = report.to_string();
        h.storage.save_session(&changed).await.unwrap();
        let result = invoke_completed(
            &h.tool,
            args.clone(),
            child_inspection_ctx(&h.parent_session_id),
        )
        .await
        .unwrap();
        let bytes = serde_json::to_vec(&result).unwrap();
        assert!(bytes.len() <= 8192);
        let value: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        if value["available"] == true {
            last_valid = bytes.len();
        } else {
            assert_eq!(value["reason"], "result_budget_exceeded");
            let mut candidate = base.clone();
            candidate["child_report"] = report;
            assert!(
                candidate.to_string().len() <= 8192,
                "first compact JSON layer fits"
            );
            let unbounded = ToolResult {
                success: true,
                result: candidate.to_string(),
                display_preference: Some("Collapsible".into()),
                images: vec![],
            };
            assert!(serde_json::to_vec(&unbounded).unwrap().len() > 8192);
            double_escaped_rejected = true;
            break;
        }
    }
    assert!(
        last_valid >= 7900,
        "near-boundary valid actual ToolResult: {last_valid}"
    );
    assert!(double_escaped_rejected);
    let h = build_test_harness_with_storage(None, None, true).await;
    assert_result_unavailable(
        &h,
        json!({"action":"get","child_session_id":h.child_session_id,"view":"result_binding"}),
        "typed_result_unsupported",
    )
    .await;
    for view in ["overview", "messages", "message", "result", "error"] {
        assert!(invoke_completed(
            &h.tool,
            json!({"action":"get","child_session_id":h.child_session_id,"view":view,
            "expected_assignment_sha256":null}),
            child_inspection_ctx(&h.parent_session_id)
        )
        .await
        .is_err());
    }
}
