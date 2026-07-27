use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    SessionActivationDisposition, SessionActivationError, SessionActivationPort, SessionInboxPort,
};
use bamboo_engine::session_app::child_session;

use crate::app_state::{AgentRunner, AgentStatus};
use crate::tools::{ChildSessionAdapter, SubAgentTool};
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
}

impl RecordingActivation {
    fn fail_next(&self) {
        self.failures_remaining.fetch_add(1, Ordering::SeqCst);
    }

    fn set_delegate(&self, delegate: Arc<dyn SessionActivationPort>) {
        *self.delegate.write().unwrap() = Some(delegate);
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

async fn build_test_harness_with_resolver(
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
) -> TestHarness {
    build_test_harness_with_options(subagent_model_resolver, None).await
}

async fn build_test_harness_with_options(
    subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    workspace_resolver: Option<bamboo_agent_core::workspace_state::WorkspaceResolver>,
) -> TestHarness {
    let bamboo_home = make_temp_dir("bamboo-sub-agent-test");
    tokio::fs::create_dir_all(&bamboo_home).await.unwrap();
    let workspace_path = bamboo_home.join("workspace");
    tokio::fs::create_dir_all(&workspace_path).await.unwrap();
    let workspace_path = tokio::fs::canonicalize(workspace_path).await.unwrap();

    let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
    let project_store =
        Arc::new(bamboo_projects::ProjectStore::open(&bamboo_home).expect("Project store"));
    let storage_dir = bamboo_home.join("storage");
    tokio::fs::create_dir_all(&storage_dir).await.unwrap();
    let jsonl = bamboo_storage::JsonlStorage::new(&storage_dir);
    jsonl.init().await.unwrap();
    let storage: Arc<dyn Storage> = Arc::new(jsonl);
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

    let metrics_storage = Arc::new(SqliteMetricsStorage::new(bamboo_home.join("metrics.db")));
    let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

    let sessions_cache: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());
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
    child
        .metadata
        .insert("last_run_status".to_string(), "completed".to_string());
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
        child_run_launch_hook: Some(Arc::new(
            crate::app_state::session_events::NotificationRelayLaunchHook::new(
                notification_relay_deps,
            ),
        )),
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

// -----------------------------------------------------------------------
// ④ Batched parent-wait registration
// -----------------------------------------------------------------------

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
            subagent_type: "explorer".to_string(),
            workspace: foreign_workspace.path().to_string_lossy().into_owned(),
            workspace_source: bamboo_engine::project_context::WorkspaceSource::Explicit,
            model_override: None,
            model_ref_override: None,
            runtime_metadata: HashMap::new(),
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
        ("explorer", None, None),
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
        session_id: Some(session_id),
        tool_call_id,
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
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
    // worker→worker→… recursion). Put the parent run session at the cap.
    let harness = build_test_harness().await;
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    parent.spawn_depth = bamboo_server_tools::DEFAULT_MAX_SPAWN_DEPTH;
    harness.storage.save_session(&parent).await.unwrap();

    let err = invoke_completed(
        &harness.tool,
            json!({"action":"create","title":"X","responsibility":"Y","prompt":"Z","workspace":harness.workspace_path.to_string_lossy()}),
            ctx_for(&harness.parent_session_id, "tc_depth_cap").to_tool_ctx(),
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
    let harness = build_test_harness().await;
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    parent.spawn_depth = bamboo_server_tools::DEFAULT_MAX_SPAWN_DEPTH - 1;
    harness.storage.save_session(&parent).await.unwrap();

    let result = invoke_completed(
        &harness.tool,
            json!({"action":"create","title":"X","responsibility":"Y","prompt":"Z","workspace":harness.workspace_path.to_string_lossy()}),
            ctx_for(&harness.parent_session_id, "tc_depth_ok").to_tool_ctx(),
        )
        .await;
    assert!(
        result.is_ok(),
        "create just below the cap should proceed, got {result:?}"
    );
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
async fn create_emits_sub_agent_started_event_after_queueing() {
    let mut harness = build_test_harness().await;

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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_1",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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

    let started_event = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match harness.parent_rx.recv().await {
                Ok(AgentEvent::SubAgentStarted {
                    parent_session_id: pid,
                    child_session_id: cid,
                    ..
                }) => break (pid, cid),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    panic!("parent stream closed before start event")
                }
            }
        }
    })
    .await
    .expect("should receive SubAgentStarted event quickly");

    assert_eq!(started_event.0, harness.parent_session_id);
    assert_eq!(started_event.1, child_session_id);
}

#[tokio::test]
async fn create_uses_async_subagent_model_resolver() {
    let resolver: crate::tools::SubagentModelResolver = Arc::new(|subagent_type: String| {
        Box::pin(async move {
            assert_eq!(subagent_type, "coder");
            Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "gpt-resolved-coder",
            ))
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_async_resolver",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
        Some(bamboo_domain::ProviderModelRef::new(
            "openai",
            "gpt-resolved-coder",
        ))
    );
    assert_eq!(
        child.metadata.get("provider_name").map(String::as_str),
        Some("openai")
    );
}

#[tokio::test]
async fn resident_create_reuses_same_child_session() {
    let harness = build_test_harness().await;
    let workspace = tempfile::tempdir().expect("workspace");
    let ctx = |tcid: &'static str| ToolExecutionContext {
        session_id: Some(harness.parent_session_id.as_str()),
        tool_call_id: tcid,
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_legacy",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
async fn send_message_appends_follow_up_without_replacing_history() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({
            "action": "send_message",
            "child_session_id": harness.child_session_id,
            "message": "continue with the failing parser path",
            "auto_run": false
        }),
        ToolExecutionContext {
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_send_message",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
    assert_eq!(
        child.messages[3].content,
        "continue with the failing parser path"
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
        "auto_run=false on an idle child must remain a draft and not activate"
    );
    assert_eq!(harness.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn send_message_queues_on_running_child_without_interrupt() {
    let harness = build_test_harness().await;
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
            "message": "continue"
        }),
        ToolExecutionContext {
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_running",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
    assert_eq!(payload["message"], "continue");

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
            "continue"
        ))
    );
}

#[tokio::test]
async fn send_message_can_interrupt_running_child() {
    let harness = build_test_harness().await;
    let cancel_token = {
        let mut runners = harness.agent_runners.write().await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let cancel_token = runner.cancel_token.clone();
        runners.insert(harness.child_session_id.clone(), runner);
        cancel_token
    };

    let runners_for_status = harness.agent_runners.clone();
    let child_id_for_status = harness.child_session_id.clone();
    let waiter = tokio::spawn(async move {
        cancel_token.cancelled().await;
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_interrupt_running",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("send_message should interrupt running child");

    waiter.await.expect("waiter task should finish");

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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_queue",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_activation_retry",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_relay",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_cancel",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx(),
    )
    .await
    .expect("cancel should succeed");

    waiter.await.expect("waiter should finish");

    let payload: serde_json::Value =
        serde_json::from_str(&result.result).expect("tool result should be JSON");
    assert_eq!(payload["status"], "cancelled");
    assert_eq!(payload["child_session_id"], harness.child_session_id);
}

#[tokio::test]
async fn list_returns_children() {
    let harness = build_test_harness().await;

    let result = invoke_completed(
        &harness.tool,
        json!({"action": "list"}),
        ToolExecutionContext {
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_list",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_get_diagnostics",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_create_hint",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_create_with_effort",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_create_default_effort",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_update_effort",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_delete",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_no_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_project_default_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_moved_project_default_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
            session_id: Some(harness.parent_session_id.as_str()),
            tool_call_id: "tool_call_workspace",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
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
