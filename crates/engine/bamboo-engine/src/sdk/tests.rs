//! Phase 2 SDK runner tests (S-T2.1 .. S-T2.5).
//!
//! These exercise the canonical spawn core (`run_child_spawn`) and the ergonomic
//! `ChildRunner` facade directly at the engine level, using a mock LLM provider
//! to avoid network I/O.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;
use tokio::sync::{broadcast, Notify, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{ToolCall, ToolError, ToolResult, ToolSchema};
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_domain::session::runtime_state::{
    AgentRuntimeState, AgentStatusState, ChildWaitPolicy, SuspensionState, WaitingForChildrenState,
};
use bamboo_domain::{
    SessionActivationError, SessionActivationPort, SessionInboxPort, SessionMessageEnvelope,
    SessionMessageId,
};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};

use crate::runtime::execution::spawn::{ExternalChildRunner, SpawnContext, SpawnJob};
use crate::sdk::runner::{ChildRunner, RunChildInput};
use crate::sdk::spawn::run_child_spawn;
use bamboo_metrics::MetricsCollector;
use bamboo_skills::SkillManager;

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

/// Provider that returns a single token + Done, so the child loop completes.
struct CompletedProvider;

#[async_trait]
impl LLMProvider for CompletedProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let items: Vec<bamboo_llm::provider::Result<LLMChunk>> =
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)];
        Ok(Box::pin(stream::iter(items)))
    }
}

/// Holds a resumed activation inside its provider boundary so registration
/// races can inspect the published router owner and raw runner deterministically.
struct BlockingCompletedProvider {
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl LLMProvider for BlockingCompletedProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        let items: Vec<bamboo_llm::provider::Result<LLMChunk>> =
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)];
        Ok(Box::pin(stream::iter(items)))
    }
}

/// Records the second production run's provider context. The terminal-race
/// envelope must be absent from the first call and present exactly once in the
/// successor call.
struct SuccessorRecordingProvider {
    calls: Arc<AtomicUsize>,
    expected_message_id: String,
    successor_seen: Arc<Notify>,
    successor_match_count: Arc<AtomicUsize>,
}

#[async_trait]
impl LLMProvider for SuccessorRecordingProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let matches = messages
            .iter()
            .filter(|message| message.id == self.expected_message_id)
            .count();
        if call == 1 {
            assert_eq!(
                matches, 0,
                "terminal-race message must not reach the first provider run"
            );
        } else if call == 2 {
            self.successor_match_count.store(matches, Ordering::SeqCst);
            self.successor_seen.notify_one();
        }
        let items: Vec<bamboo_llm::provider::Result<LLMChunk>> =
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)];
        Ok(Box::pin(stream::iter(items)))
    }
}

/// Provider whose stream drips tokens forever without ever sending `Done`, so the
/// child runner never finishes on its own and the watchdog must terminate it.
///
/// The stream MUST keep yielding (rather than `stream::pending()`): the stream
/// consumer only checks the cancel token between chunks, mirroring real LLM
/// streams that emit periodic chunks.
struct HangingProvider;

#[async_trait]
impl LLMProvider for HangingProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        // A deliberately slow but *finite* stream: a handful of tokens spaced out,
        // then `Done`. Each LLM call (including any auxiliary calls that do not
        // thread the cancel token) terminates, but the main agent round takes long
        // enough that the child watchdog fires and cancels the run first — the
        // round-boundary cancel check then yields `AgentError::Cancelled`, which the
        // spawn core maps to a `timeout` terminal status (because `timeout_reason`
        // was set by the watchdog).
        let chunks: Vec<bamboo_llm::provider::Result<LLMChunk>> = vec![
            Ok(LLMChunk::Token("slow-1".to_string())),
            Ok(LLMChunk::Token("slow-2".to_string())),
            Ok(LLMChunk::Done),
        ];
        // Each chunk is paced well beyond the watchdog's total-timeout window, so
        // the watchdog deterministically fires (and cancels) while the run is still
        // streaming — never racing a fast natural completion.
        let s = stream::unfold(chunks.into_iter(), |mut it| async move {
            let next = it.next()?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            Some((next, it))
        });
        Ok(Box::pin(s))
    }
}

/// Tool executor exposing a fixed catalog of tool names.
struct CatalogToolExecutor {
    names: Vec<String>,
}

#[async_trait]
impl bamboo_agent_core::tools::ToolExecutor for CatalogToolExecutor {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
        Err(ToolError::NotFound("noop".to_string()))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.names
            .iter()
            .map(|name| ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionSchema {
                    name: name.clone(),
                    description: String::new(),
                    parameters: serde_json::json!({}),
                },
            })
            .collect()
    }
}

/// Test-only child runner that drives the child loop in-process. The
/// production in-process spawn path was removed (sub-agents always run as
/// actors), but `run_child_spawn`'s orchestration — event ordering, watchdog,
/// completion/status mapping, persistence — is runner-agnostic. This stand-in
/// keeps that orchestration under fast, network-free unit test without spinning
/// up a real actor worker subprocess.
struct InProcessTestRunner {
    agent: Arc<crate::Agent>,
    tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
}

#[async_trait]
impl ExternalChildRunner for InProcessTestRunner {
    async fn should_handle(&self, _session: &Session) -> bool {
        true
    }

    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        let mut builder =
            crate::runtime::ExecuteRequestBuilder::new(String::new(), event_tx, cancel_token)
                .tools(self.tools.clone())
                .model_roster(crate::runtime::ModelRoster {
                    model: Some(job.model.clone()),
                    provider_name: None,
                    provider_type: None,
                    fast: None,
                    background: None,
                    summarization: None,
                });
        if let Some(disabled) = &job.disabled_tools {
            builder = builder.disabled_tools(disabled.iter().cloned().collect());
        }
        self.agent.execute(session, builder.build()).await
    }
}

struct TerminalBarrierRunner {
    inner: InProcessTestRunner,
    completed_runs: AtomicUsize,
    first_execute_returned: Arc<Notify>,
    release_first_terminal: Arc<Notify>,
}

#[async_trait]
impl ExternalChildRunner for TerminalBarrierRunner {
    async fn should_handle(&self, session: &Session) -> bool {
        self.inner.should_handle(session).await
    }

    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        let result = self
            .inner
            .execute_external_child(session, job, event_tx, cancel_token)
            .await;
        let completed = self.completed_runs.fetch_add(1, Ordering::SeqCst);
        if completed == 0 {
            // The final provider boundary has completed, while run_child_spawn's
            // real terminal block has not begun.
            self.first_execute_returned.notify_one();
            self.release_first_terminal.notified().await;
        }
        result
    }
}

struct GatedCoordinatorSpawner {
    inner: Arc<crate::ChildCompletionCoordinator>,
    reservations: Arc<AtomicUsize>,
    reservation_entered: Arc<Notify>,
    allow_reservation: Arc<Notify>,
}

#[async_trait]
impl crate::SessionActivationSpawner for GatedCoordinatorSpawner {
    async fn reserve_activation(
        &self,
        target_session_id: &str,
        inbox_generation: u64,
    ) -> Result<crate::SessionActivationReserveOutcome, SessionActivationError> {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        self.reservation_entered.notify_one();
        self.allow_reservation.notified().await;
        crate::SessionActivationSpawner::reserve_activation(
            self.inner.as_ref(),
            target_session_id,
            inbox_generation,
        )
        .await
    }
}

/// Fails exactly the first transcript checkpoint that contains the selected
/// typed inbox message. Legacy queue cleanup and terminal persistence still
/// use the real locked store, reproducing the boundary failure where the
/// compatibility source has already been CAS-cleared but the typed claim has
/// not yet been admitted.
struct FailFirstInboxCheckpointPersistence {
    inner: Arc<bamboo_storage::LockedSessionStore>,
    message_id: String,
    failures: AtomicUsize,
}

#[async_trait]
impl bamboo_domain::RuntimeSessionPersistence for FailFirstInboxCheckpointPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        bamboo_domain::RuntimeSessionPersistence::save_runtime_session(self.inner.as_ref(), session)
            .await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        if session
            .messages
            .iter()
            .any(|message| message.id == self.message_id)
            && self.failures.fetch_add(1, Ordering::SeqCst) == 0
        {
            return Err(std::io::Error::other(
                "injected first SessionInbox transcript checkpoint failure",
            ));
        }
        bamboo_domain::RuntimeSessionPersistence::checkpoint_runtime_session(
            self.inner.as_ref(),
            session,
        )
        .await
    }

    async fn load_runtime_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        bamboo_domain::RuntimeSessionPersistence::load_runtime_session(
            self.inner.as_ref(),
            session_id,
        )
        .await
    }

    async fn clear_legacy_pending_messages(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
    ) -> std::io::Result<bool> {
        bamboo_domain::RuntimeSessionPersistence::clear_legacy_pending_messages(
            self.inner.as_ref(),
            session_id,
            expected,
        )
        .await
    }

    async fn append_token_usage_record(
        &self,
        session_id: &str,
        json_line: &str,
    ) -> std::io::Result<()> {
        bamboo_domain::RuntimeSessionPersistence::append_token_usage_record(
            self.inner.as_ref(),
            session_id,
            json_line,
        )
        .await
    }
}

struct Harness {
    ctx: SpawnContext,
    storage: Arc<dyn Storage>,
    parent_session_id: String,
    child_session_id: String,
    parent_rx: broadcast::Receiver<AgentEvent>,
    parent_tx: broadcast::Sender<AgentEvent>,
}

struct DeliveryHarness {
    ctx: SpawnContext,
    storage: Arc<dyn Storage>,
    inbox: Arc<dyn SessionInboxPort>,
    messenger: Arc<crate::SessionMessenger>,
    coordinator: Arc<crate::ChildCompletionCoordinator>,
    parent_session_id: String,
    child_session_id: String,
    parent_rx: broadcast::Receiver<AgentEvent>,
    agent_runners: Arc<RwLock<HashMap<String, crate::execution::AgentRunner>>>,
    first_execute_returned: Arc<Notify>,
    release_first_terminal: Arc<Notify>,
    reservation_entered: Arc<Notify>,
    allow_reservation: Arc<Notify>,
    reservations: Arc<AtomicUsize>,
    _spawn_scheduler: Arc<crate::runtime::execution::spawn::SpawnScheduler>,
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()))
}

async fn build_harness(
    provider: Arc<dyn LLMProvider>,
    tool_names: Vec<String>,
    child_metadata: &[(&str, &str)],
) -> Harness {
    let home = temp_dir("bamboo-sdk-test");
    tokio::fs::create_dir_all(&home).await.unwrap();

    let session_store = Arc::new(
        bamboo_storage::SessionStoreV2::new(home.clone())
            .await
            .unwrap(),
    );
    let storage_dir = home.join("storage");
    tokio::fs::create_dir_all(&storage_dir).await.unwrap();
    let jsonl = bamboo_storage::JsonlStorage::new(&storage_dir);
    jsonl.init().await.unwrap();
    let storage: Arc<dyn Storage> = Arc::new(jsonl);

    let metrics_storage = Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
        home.join("metrics.db"),
    ));
    let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

    let sessions_cache: crate::SessionCache = Arc::new(dashmap::DashMap::new());
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
        senders.insert(parent_session_id.clone(), parent_tx.clone());
    }

    let mut parent = Session::new(parent_session_id.clone(), "gpt-5");
    parent.title = "Root".to_string();
    storage.save_session(&parent).await.unwrap();

    let mut child = Session::new_child(
        child_session_id.clone(),
        parent_session_id.clone(),
        "gpt-5",
        "Child session",
    );
    child
        .metadata
        .insert("last_run_status".to_string(), "queued".to_string());
    for (k, v) in child_metadata {
        child.metadata.insert(k.to_string(), v.to_string());
    }
    child.add_message(Message::system("child system"));
    child.add_message(Message::user("do the task"));
    storage.save_session(&child).await.unwrap();

    let agent = Arc::new(
        crate::Agent::builder()
            .storage(storage.clone())
            .persistence(Arc::new(bamboo_storage::LockedSessionStore::new(
                storage.clone(),
            )))
            .attachment_reader(session_store.clone())
            .skill_manager(Arc::new(SkillManager::new()))
            .metrics_collector(metrics_collector)
            .config(Arc::new(RwLock::new(bamboo_llm::Config::default())))
            .provider(provider)
            .default_tools(Arc::new(CatalogToolExecutor {
                names: tool_names.clone(),
            }))
            .build()
            .expect("test agent should build"),
    );

    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(CatalogToolExecutor { names: tool_names });

    let ctx = SpawnContext {
        agent: agent.clone(),
        tools: tools.clone(),
        sessions_cache,
        agent_runners,
        session_event_senders,
        external_child_runner: Arc::new(InProcessTestRunner { agent, tools }),
        provider_router: None,
        app_data_dir: None,
        completion_handler: None,
        child_run_launch_hook: None,
        account_feed_inbox: None,
    };

    Harness {
        ctx,
        storage,
        parent_session_id,
        child_session_id,
        parent_rx,
        parent_tx,
    }
}

async fn build_terminal_delivery_harness(
    provider: Arc<dyn LLMProvider>,
    legacy_pending: Option<serde_json::Value>,
    fail_first_checkpoint_for: Option<String>,
) -> DeliveryHarness {
    let home = temp_dir("bamboo-sdk-terminal-inbox-test");
    tokio::fs::create_dir_all(&home).await.unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(home.clone())
            .await
            .unwrap(),
    );
    let storage: Arc<dyn Storage> = store.clone();
    let locked = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
        match fail_first_checkpoint_for {
            Some(message_id) => Arc::new(FailFirstInboxCheckpointPersistence {
                inner: locked.clone(),
                message_id,
                failures: AtomicUsize::new(0),
            }),
            None => locked.clone(),
        };
    let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
        store.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    ));
    let router = crate::SessionActivationRouter::new();
    let activation: Arc<dyn SessionActivationPort> = router.clone();
    let messenger = Arc::new(crate::SessionMessenger::new(
        storage.clone(),
        inbox.clone(),
        activation,
    ));

    let metrics_storage = Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
        home.join("metrics.db"),
    ));
    let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(CatalogToolExecutor { names: Vec::new() });
    let mut config_value = bamboo_llm::Config::default();
    config_value.provider = "test".to_string();
    let config = Arc::new(RwLock::new(config_value));
    let mut providers = HashMap::new();
    providers.insert("test".to_string(), provider.clone());
    let provider_registry = Arc::new(bamboo_llm::ProviderRegistry::new(
        providers,
        "test".to_string(),
    ));
    let provider_router = Arc::new(bamboo_llm::ProviderModelRouter::new(
        provider_registry.clone(),
    ));
    let agent = Arc::new(
        crate::Agent::builder()
            .storage(storage.clone())
            .persistence(persistence)
            .session_inbox(inbox.clone())
            .activation_router(router.clone())
            .session_messenger(messenger.clone())
            .attachment_reader(store)
            .skill_manager(Arc::new(SkillManager::new()))
            .metrics_collector(metrics_collector)
            .config(config.clone())
            .provider(provider)
            .default_tools(tools.clone())
            .build()
            .expect("delivery test agent"),
    );

    let sessions_cache: crate::SessionCache = Arc::new(dashmap::DashMap::new());
    let agent_runners = Arc::new(RwLock::new(HashMap::new()));
    let session_event_senders = Arc::new(RwLock::new(HashMap::<
        String,
        broadcast::Sender<AgentEvent>,
    >::new()));
    let parent_session_id = "terminal-root".to_string();
    let child_session_id = "terminal-child".to_string();
    let (parent_tx, parent_rx) = broadcast::channel(1000);
    session_event_senders
        .write()
        .await
        .insert(parent_session_id.clone(), parent_tx);

    let parent = Session::new(&parent_session_id, "gpt-5");
    storage.save_session(&parent).await.unwrap();
    let mut child = Session::new_child(
        &child_session_id,
        &parent_session_id,
        "gpt-5",
        "Terminal child",
    );
    child.add_message(Message::system("child system"));
    child.add_message(Message::user("first task"));
    if let Some(message) = legacy_pending {
        child.set_pending_injected_messages(vec![message]);
    }
    storage.save_session(&child).await.unwrap();

    let first_execute_returned = Arc::new(Notify::new());
    let release_first_terminal = Arc::new(Notify::new());
    let external_child_runner: Arc<dyn ExternalChildRunner> = Arc::new(TerminalBarrierRunner {
        inner: InProcessTestRunner {
            agent: agent.clone(),
            tools: tools.clone(),
        },
        completed_runs: AtomicUsize::new(0),
        first_execute_returned: first_execute_returned.clone(),
        release_first_terminal: release_first_terminal.clone(),
    });
    let ctx = SpawnContext {
        agent: agent.clone(),
        tools: tools.clone(),
        sessions_cache: sessions_cache.clone(),
        agent_runners: agent_runners.clone(),
        session_event_senders: session_event_senders.clone(),
        external_child_runner,
        provider_router: None,
        app_data_dir: None,
        completion_handler: None,
        child_run_launch_hook: None,
        account_feed_inbox: None,
    };
    let reservations = Arc::new(AtomicUsize::new(0));
    let reservation_entered = Arc::new(Notify::new());
    let allow_reservation = Arc::new(Notify::new());
    let coordinator = Arc::new(crate::ChildCompletionCoordinator::new(
        storage.clone(),
        locked,
        sessions_cache,
        agent_runners,
        session_event_senders,
        agent,
        config,
        provider_registry,
        provider_router,
        home,
        None,
    ));
    coordinator.set_root_tools(tools).await;
    let spawn_scheduler = Arc::new(crate::runtime::execution::spawn::SpawnScheduler::new(
        ctx.clone(),
    ));
    coordinator.set_spawn_scheduler(&spawn_scheduler).await;
    router
        .set_spawner(Arc::new(GatedCoordinatorSpawner {
            inner: coordinator.clone(),
            reservations: reservations.clone(),
            reservation_entered: reservation_entered.clone(),
            allow_reservation: allow_reservation.clone(),
        }))
        .await;

    let delivery_runners = ctx.agent_runners.clone();
    DeliveryHarness {
        ctx,
        storage,
        inbox,
        messenger,
        coordinator,
        parent_session_id,
        child_session_id,
        parent_rx,
        agent_runners: delivery_runners,
        first_execute_returned,
        release_first_terminal,
        reservation_entered,
        allow_reservation,
        reservations,
        _spawn_scheduler: spawn_scheduler,
    }
}

/// Collect parent events until a `SubAgentCompleted` is observed (or timeout).
async fn collect_until_completed(rx: &mut broadcast::Receiver<AgentEvent>) -> Vec<AgentEvent> {
    collect_until_completed_with_budget(rx, Duration::from_secs(15)).await
}

/// Like [`collect_until_completed`] but with a caller-supplied budget. The
/// watchdog timeout path uses wall-clock time, which can stretch under heavy
/// parallel test load, so the timeout test grants a wider budget.
async fn collect_until_completed_with_budget(
    rx: &mut broadcast::Receiver<AgentEvent>,
    budget: Duration,
) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for SubAgentCompleted; saw: {events:?}");
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(event)) => {
                let done = matches!(event, AgentEvent::SubAgentCompleted { .. });
                events.push(event);
                if done {
                    return events;
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                panic!("parent channel closed before completion; saw: {events:?}")
            }
            Err(_) => panic!("timed out waiting for SubAgentCompleted; saw: {events:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// S-T2.1 — run_child_spawn integration: ordering + persisted status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn distinct_router_collision_is_a_terminal_setup_error_not_a_phantom_run() {
    let mut harness =
        build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let workspace_before =
        bamboo_agent_core::workspace_state::peek_workspace(&harness.child_session_id);
    let rejected_workspace =
        temp_dir("bamboo-rejected-child-workspace").join("must-not-materialize");
    let mut child_with_rejected_workspace = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    child_with_rejected_workspace.workspace =
        Some(rejected_workspace.to_string_lossy().to_string());
    harness
        .storage
        .save_session(&child_with_rejected_workspace)
        .await
        .unwrap();

    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    let mut existing_owner = router
        .register_run(&harness.child_session_id, "independent-owner")
        .await
        .unwrap();

    let error = run_child_spawn(
        harness.ctx.clone(),
        SpawnJob {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            model: "gpt-5".to_string(),
            disabled_tools: None,
        },
    )
    .await
    .expect_err("a distinct logical-session owner must reject child setup");
    assert!(error.contains("owner collision"), "{error}");
    assert!(
        router
            .owns_run(&harness.child_session_id, "independent-owner")
            .await
    );

    let completion = tokio::time::timeout(Duration::from_secs(2), harness.parent_rx.recv())
        .await
        .expect("the waiting parent must be unblocked immediately")
        .expect("parent event channel remains open");
    match completion {
        AgentEvent::SubAgentCompleted {
            child_session_id,
            status,
            error: Some(completion_error),
            ..
        } => {
            assert_eq!(child_session_id, harness.child_session_id);
            assert_eq!(status, "error");
            assert!(completion_error.contains("owner collision"));
        }
        other => panic!("expected terminal collision completion, got {other:?}"),
    }

    let persisted = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    let persisted_status = persisted.last_run_status();
    assert_ne!(
        persisted_status.as_deref(),
        Some("running"),
        "rejected setup must not persist a phantom running marker"
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&harness.child_session_id),
        workspace_before,
        "rejected setup must not overwrite process-global workspace ownership"
    );
    assert!(
        !rejected_workspace.exists(),
        "rejected setup must not materialize its proposed workspace"
    );
    assert!(matches!(
        harness
            .agent_runners
            .read()
            .await
            .get(&harness.child_session_id)
            .map(|runner| &runner.status),
        Some(crate::execution::AgentStatus::Error(_))
    ));

    existing_owner.begin_finalization().await;
    assert_eq!(existing_owner.finish(0).await.unwrap(), None);
}

#[tokio::test]
async fn s_t2_1_run_child_spawn_emits_started_event_completed_in_order() {
    let mut harness = build_harness(Arc::new(CompletedProvider), Vec::new(), &[]).await;

    // The adapter emits SubAgentStarted before enqueue; simulate that here so the
    // full ordering can be asserted.
    harness
        .parent_tx
        .send(AgentEvent::SubAgentStarted {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            title: Some("Child session".to_string()),
        })
        .unwrap();

    run_child_spawn(
        harness.ctx.clone(),
        SpawnJob {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            model: "gpt-5".to_string(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();

    let events = collect_until_completed(&mut harness.parent_rx).await;

    let started_idx = events
        .iter()
        .position(|e| matches!(e, AgentEvent::SubAgentStarted { .. }))
        .expect("SubAgentStarted present");
    let completed_idx = events
        .iter()
        .position(|e| matches!(e, AgentEvent::SubAgentCompleted { .. }))
        .expect("SubAgentCompleted present");
    let event_idx = events
        .iter()
        .position(|e| matches!(e, AgentEvent::SubAgentEvent { .. }));

    assert!(
        started_idx < completed_idx,
        "Started must precede Completed: {events:?}"
    );
    if let Some(event_idx) = event_idx {
        assert!(
            started_idx < event_idx,
            "Started must precede SubAgentEvent"
        );
        assert!(
            event_idx < completed_idx,
            "SubAgentEvent must precede Completed"
        );
    }

    match events.last().unwrap() {
        AgentEvent::SubAgentCompleted { status, .. } => {
            assert_eq!(status, "completed", "child should finish completed");
        }
        other => panic!("last event must be SubAgentCompleted, got {other:?}"),
    }

    // Persisted child status reflects completion.
    let persisted = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted
            .metadata
            .get("last_run_status")
            .map(String::as_str),
        Some("completed")
    );
}

#[tokio::test]
async fn activation_watermark_wait_gap_defers_then_reserves_same_generation_once() {
    let harness = build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let now = chrono::Utc::now();
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let mut runtime = AgentRuntimeState::new("watermark-wait-gap");
    runtime.status = AgentStatusState::Suspended;
    runtime.suspension = Some(SuspensionState {
        reason: "waiting_for_children".to_string(),
        suspended_at: now,
        resumable: true,
        hook_point: None,
    });
    runtime.waiting_for_children = Some(WaitingForChildrenState::for_children(
        vec!["still-running-child".to_string()],
        ChildWaitPolicy::All,
        now,
    ));
    parent.agent_runtime_state = Some(runtime.clone());
    parent.metadata.insert(
        "agent.runtime.state".to_string(),
        serde_json::to_string(&runtime).unwrap(),
    );
    parent.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "waiting_for_children".to_string(),
    );
    harness.storage.save_session(&parent).await.unwrap();

    let admission = harness
        .messenger
        .admit(SessionMessageEnvelope::user_input(
            &harness.parent_session_id,
            "durable work waiting behind a child barrier",
        ))
        .await
        .unwrap();
    harness
        .messenger
        .prepare_activation(&admission)
        .await
        .unwrap();
    let generation = admission.delivery.generation;

    let deferred = crate::SessionActivationSpawner::reserve_activation(
        harness.coordinator.as_ref(),
        &harness.parent_session_id,
        generation,
    )
    .await
    .unwrap();
    assert!(matches!(
        deferred,
        crate::SessionActivationReserveOutcome::NoWork
    ));
    assert!(!harness
        .agent_runners
        .read()
        .await
        .contains_key(&harness.parent_session_id));
    let still_waiting = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap()
        .agent_runtime_state
        .unwrap();
    assert!(still_waiting.waiting_for_children.is_some());
    assert_eq!(still_waiting.status, AgentStatusState::Suspended);

    let mut cleared = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let mut cleared_runtime = cleared.agent_runtime_state.clone().unwrap();
    cleared_runtime.waiting_for_children = None;
    cleared_runtime.status = AgentStatusState::Idle;
    cleared_runtime.suspension = None;
    cleared.agent_runtime_state = Some(cleared_runtime.clone());
    cleared.metadata.insert(
        "agent.runtime.state".to_string(),
        serde_json::to_string(&cleared_runtime).unwrap(),
    );
    cleared.metadata.remove("runtime.suspend_reason");
    harness.storage.save_session(&cleared).await.unwrap();

    let reserved = crate::SessionActivationSpawner::reserve_activation(
        harness.coordinator.as_ref(),
        &harness.parent_session_id,
        generation,
    )
    .await
    .unwrap();
    let crate::SessionActivationReserveOutcome::Reserved(launch) = reserved else {
        panic!("cleared wait must reserve the same durable generation");
    };
    let run_id = launch.run_id.clone();
    assert_eq!(
        harness
            .agent_runners
            .read()
            .await
            .get(&harness.parent_session_id)
            .map(|runner| runner.run_id.as_str()),
        Some(run_id.as_str())
    );

    let duplicate = crate::SessionActivationSpawner::reserve_activation(
        harness.coordinator.as_ref(),
        &harness.parent_session_id,
        generation,
    )
    .await
    .unwrap();
    assert!(matches!(
        duplicate,
        crate::SessionActivationReserveOutcome::AlreadyRunning {
            run_id: duplicate_run_id
        } if duplicate_run_id == run_id
    ));

    drop(launch);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !harness
                .agent_runners
                .read()
                .await
                .contains_key(&harness.parent_session_id)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping an unpublished activation must roll back its reservation");
}

#[tokio::test]
async fn activation_already_running_never_replaces_live_session_cache_arc() {
    let harness = build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let admission = harness
        .messenger
        .admit(SessionMessageEnvelope::user_input(
            &harness.parent_session_id,
            "activation racing a manual owner",
        ))
        .await
        .unwrap();
    harness
        .messenger
        .prepare_activation(&admission)
        .await
        .unwrap();

    let mut live = Session::new(&harness.parent_session_id, "live-cache-model");
    live.metadata
        .insert("cache_owner".to_string(), "manual-run".to_string());
    let live_arc = Arc::new(parking_lot::RwLock::new(live));
    harness
        .ctx
        .sessions_cache
        .insert(harness.parent_session_id.clone(), live_arc.clone());
    let event_sender = crate::runtime::execution::session_events::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
    )
    .await;
    let existing_run_id = match crate::runtime::execution::runner_lifecycle::reserve_runner_core(
        &harness.agent_runners,
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
        &event_sender,
    )
    .await
    {
        crate::runtime::execution::runner_lifecycle::ReserveOutcome::Reserved(reservation) => {
            reservation.run_id
        }
        crate::runtime::execution::runner_lifecycle::ReserveOutcome::AlreadyRunning(_) => {
            panic!("test must establish the manual owner")
        }
    };

    let outcome = crate::SessionActivationSpawner::reserve_activation(
        harness.coordinator.as_ref(),
        &harness.parent_session_id,
        admission.delivery.generation,
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        crate::SessionActivationReserveOutcome::AlreadyRunning { run_id }
            if run_id == existing_run_id
    ));
    let cached = harness
        .ctx
        .sessions_cache
        .get(&harness.parent_session_id)
        .unwrap()
        .value()
        .clone();
    assert!(
        Arc::ptr_eq(&cached, &live_arc),
        "losing activation must not publish its detached prepared snapshot"
    );
    assert_eq!(
        cached
            .read()
            .metadata
            .get("cache_owner")
            .map(String::as_str),
        Some("manual-run")
    );
}

#[tokio::test]
async fn direct_registration_waits_for_activation_without_creating_a_phantom_owner() {
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let provider_entered = Arc::new(Notify::new());
    let provider_release = Arc::new(Notify::new());
    let harness = build_terminal_delivery_harness(
        Arc::new(BlockingCompletedProvider {
            calls: provider_calls.clone(),
            entered: provider_entered.clone(),
            release: provider_release.clone(),
        }),
        None,
        None,
    )
    .await;
    let admission = harness
        .messenger
        .admit(SessionMessageEnvelope::user_input(
            &harness.parent_session_id,
            "activation wins before direct registration",
        ))
        .await
        .unwrap();
    harness
        .messenger
        .prepare_activation(&admission)
        .await
        .unwrap();

    let messenger = harness.messenger.clone();
    let activation = tokio::spawn(async move { messenger.activate_prepared(&admission).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        harness.reservation_entered.notified(),
    )
    .await
    .expect("router must enter the gated runtime reservation");

    let mut live = Session::new(&harness.parent_session_id, "manual-owner-model");
    live.metadata
        .insert("cache_owner".to_string(), "manual-router-owner".to_string());
    let live_arc = Arc::new(parking_lot::RwLock::new(live));
    harness
        .ctx
        .sessions_cache
        .insert(harness.parent_session_id.clone(), live_arc.clone());

    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    let direct_router = router.clone();
    let direct_session_id = harness.parent_session_id.clone();
    let direct = tokio::spawn(async move {
        direct_router
            .register_run(&direct_session_id, "manual-router-owner")
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !direct.is_finished(),
        "direct registration must remain pending while activation owns the router token"
    );
    assert!(
        !router
            .owns_run(&harness.parent_session_id, "manual-router-owner")
            .await
    );
    let cached_before_launch = harness
        .ctx
        .sessions_cache
        .get(&harness.parent_session_id)
        .unwrap()
        .value()
        .clone();
    assert!(
        Arc::ptr_eq(&cached_before_launch, &live_arc),
        "prepared activation cache must remain unpublished before router ownership commits"
    );

    harness.allow_reservation.notify_one();

    let disposition = tokio::time::timeout(Duration::from_secs(5), activation)
        .await
        .expect("serialized activation must settle")
        .expect("activation task must not panic")
        .expect("durable activation remains accepted");
    assert_eq!(
        disposition.activation,
        bamboo_domain::SessionActivationDisposition::ActivationReserved
    );
    tokio::time::timeout(Duration::from_secs(5), provider_entered.notified())
        .await
        .expect("the published activation must reach one real provider execution");

    let collision = match tokio::time::timeout(Duration::from_secs(5), direct)
        .await
        .expect("direct registration must wake after activation publication")
        .expect("direct registration task must not panic")
    {
        Ok(_) => panic!("direct registration must not replace the activation owner"),
        Err(error) => error,
    };
    let activation_run_id = collision.existing_run_id().to_string();
    assert_ne!(activation_run_id, "manual-router-owner");
    assert!(
        router
            .owns_run(&harness.parent_session_id, &activation_run_id)
            .await
    );
    assert!(
        !router
            .owns_run(&harness.parent_session_id, "manual-router-owner")
            .await
    );
    {
        let runners = harness.agent_runners.read().await;
        let runner = runners
            .get(&harness.parent_session_id)
            .expect("the activation must retain its exact raw runner");
        assert_eq!(runner.run_id, activation_run_id);
        assert!(matches!(
            runner.status,
            crate::execution::AgentStatus::Running
        ));
    }

    let cached = harness
        .ctx
        .sessions_cache
        .get(&harness.parent_session_id)
        .unwrap()
        .value()
        .clone();
    assert!(
        !Arc::ptr_eq(&cached, &live_arc),
        "the winning activation publishes its prepared snapshot only after owner commit"
    );
    assert!(
        !cached.read().metadata.contains_key("cache_owner"),
        "the stale manual cache snapshot must not masquerade as the winning execution"
    );

    provider_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let runner_settled = harness
                .agent_runners
                .read()
                .await
                .get(&harness.parent_session_id)
                .is_some_and(|runner| {
                    runner.run_id == activation_run_id
                        && !matches!(runner.status, crate::execution::AgentStatus::Running)
                });
            let owner_released = !router
                .owns_run(&harness.parent_session_id, &activation_run_id)
                .await;
            if runner_settled && owner_released {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the one real activation must settle and release its exact router owner");
    assert!(
        !router
            .owns_run(&harness.parent_session_id, "manual-router-owner")
            .await,
        "the rejected direct run must never leave a phantom router owner"
    );
    assert_eq!(harness.reservations.load(Ordering::SeqCst), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
    let backlog = harness
        .inbox
        .inspect(&harness.parent_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
}

#[tokio::test]
async fn cancelled_manual_reservation_observed_by_activation_recovers_one_real_successor() {
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let provider_entered = Arc::new(Notify::new());
    let provider_release = Arc::new(Notify::new());
    let harness = build_terminal_delivery_harness(
        Arc::new(BlockingCompletedProvider {
            calls: provider_calls.clone(),
            entered: provider_entered.clone(),
            release: provider_release.clone(),
        }),
        None,
        None,
    )
    .await;
    let admission = harness
        .messenger
        .admit(SessionMessageEnvelope::user_input(
            &harness.parent_session_id,
            "recover an activation that observed a cancelled manual runner",
        ))
        .await
        .unwrap();
    harness
        .messenger
        .prepare_activation(&admission)
        .await
        .unwrap();

    let messenger = harness.messenger.clone();
    let activation = tokio::spawn(async move { messenger.activate_prepared(&admission).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        harness.reservation_entered.notified(),
    )
    .await
    .expect("activation must hold its router token before checking the raw registry");

    let sender = crate::execution::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
    )
    .await;
    let manual = {
        let agent = harness.ctx.agent.clone();
        let runners = harness.agent_runners.clone();
        let senders = harness.ctx.session_event_senders.clone();
        let session_id = harness.parent_session_id.clone();
        let sender = sender.clone();
        tokio::spawn(async move {
            crate::execution::reserve_session_execution(
                &agent,
                &runners,
                &senders,
                &session_id,
                &sender,
            )
            .await
        })
    };
    let cancelled_run_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(run_id) = harness
                .agent_runners
                .read()
                .await
                .get(&harness.parent_session_id)
                .map(|runner| runner.run_id.clone())
            {
                break run_id;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("manual entry must reserve a raw runner before waiting on the router token");

    manual.abort();
    match manual.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("manual reservation unexpectedly completed before cancellation"),
    }
    assert!(matches!(
        harness
            .agent_runners
            .read()
            .await
            .get(&harness.parent_session_id)
            .map(|runner| &runner.status),
        Some(crate::execution::AgentStatus::Running)
    ));
    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    assert!(
        !router
            .owns_run(&harness.parent_session_id, &cancelled_run_id)
            .await
    );

    // The activation observes the still-protected raw runner and publishes its
    // exact placeholder. Pending-registration Drop then adopts and abandons it,
    // which releases both planes before requesting one bounded successor.
    harness.allow_reservation.notify_one();
    let disposition = tokio::time::timeout(Duration::from_secs(5), activation)
        .await
        .expect("the first activation attempt must settle")
        .expect("activation task must not panic")
        .expect("durable activation remains accepted");
    assert_eq!(
        disposition.activation,
        bamboo_domain::SessionActivationDisposition::ActiveNotified
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        harness.reservation_entered.notified(),
    )
    .await
    .expect("abandoned exact placeholder must request one bounded successor");
    assert_eq!(harness.reservations.load(Ordering::SeqCst), 2);
    assert!(
        !router
            .owns_run(&harness.parent_session_id, &cancelled_run_id)
            .await,
        "the cancelled manual run must not remain as a phantom router owner"
    );
    assert!(matches!(
        harness
            .agent_runners
            .read()
            .await
            .get(&harness.parent_session_id)
            .filter(|runner| runner.run_id == cancelled_run_id)
            .map(|runner| &runner.status),
        Some(crate::execution::AgentStatus::Cancelled)
    ));

    harness.allow_reservation.notify_one();
    tokio::time::timeout(Duration::from_secs(5), provider_entered.notified())
        .await
        .expect("the bounded successor must reach one real provider execution");
    let successor_run_id = {
        let runners = harness.agent_runners.read().await;
        let runner = runners
            .get(&harness.parent_session_id)
            .expect("successor raw runner must be present");
        assert_ne!(runner.run_id, cancelled_run_id);
        assert!(matches!(
            runner.status,
            crate::execution::AgentStatus::Running
        ));
        runner.run_id.clone()
    };
    assert!(
        router
            .owns_run(&harness.parent_session_id, &successor_run_id)
            .await
    );
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);

    provider_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let settled = harness
                .agent_runners
                .read()
                .await
                .get(&harness.parent_session_id)
                .is_some_and(|runner| {
                    runner.run_id == successor_run_id
                        && !matches!(runner.status, crate::execution::AgentStatus::Running)
                });
            let owner_released = !router
                .owns_run(&harness.parent_session_id, &successor_run_id)
                .await;
            if settled && owner_released {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real successor must settle without a phantom owner");
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
    let backlog = harness
        .inbox
        .inspect(&harness.parent_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
}

#[tokio::test]
async fn shared_execution_reservation_collides_before_entry_side_effects() {
    let harness = build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    let mut sdk_owner = router
        .register_run(&harness.parent_session_id, "sdk-owner")
        .await
        .unwrap();
    let sender = crate::execution::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
    )
    .await;

    let outcome = crate::execution::reserve_session_execution(
        &harness.ctx.agent,
        &harness.agent_runners,
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
        &sender,
    )
    .await;
    assert!(matches!(
        outcome,
        crate::execution::SessionExecutionReserveOutcome::AlreadyRunning { run_id }
            if run_id == "sdk-owner"
    ));
    assert!(matches!(
        harness
            .agent_runners
            .read()
            .await
            .get(&harness.parent_session_id)
            .map(|runner| &runner.status),
        Some(crate::execution::AgentStatus::Error(_))
    ));
    assert!(
        router
            .owns_run(&harness.parent_session_id, "sdk-owner")
            .await
    );

    sdk_owner.begin_finalization().await;
    assert_eq!(sdk_owner.finish(0).await.unwrap(), None);
}

#[tokio::test]
async fn dropped_shared_execution_reservation_releases_runner_and_router() {
    let harness = build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let sender = crate::execution::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
    )
    .await;
    let reservation = match crate::execution::reserve_session_execution(
        &harness.ctx.agent,
        &harness.agent_runners,
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
        &sender,
    )
    .await
    {
        crate::execution::SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        crate::execution::SessionExecutionReserveOutcome::AlreadyRunning { run_id } => {
            panic!("unexpected owner {run_id}")
        }
    };
    let run_id = reservation.run_id().to_string();
    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    assert!(router.owns_run(&harness.parent_session_id, &run_id).await);
    drop(reservation);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let owns = router.owns_run(&harness.parent_session_id, &run_id).await;
            let cancelled = matches!(
                harness
                    .agent_runners
                    .read()
                    .await
                    .get(&harness.parent_session_id)
                    .map(|runner| &runner.status),
                Some(crate::execution::AgentStatus::Cancelled)
            );
            if !owns && cancelled {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Drop must clean both exact ownership planes");
}

#[tokio::test]
async fn cancelled_explicit_reservation_abandon_still_cleans_both_ownership_planes() {
    let harness = build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let sender = crate::execution::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
    )
    .await;
    let reservation = match crate::execution::reserve_session_execution(
        &harness.ctx.agent,
        &harness.agent_runners,
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
        &sender,
    )
    .await
    {
        crate::execution::SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        crate::execution::SessionExecutionReserveOutcome::AlreadyRunning { run_id } => {
            panic!("unexpected owner {run_id}")
        }
    };
    let run_id = reservation.run_id().to_string();
    let cancel_token = reservation.cancel_token().clone();
    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    let runners_guard = harness.agent_runners.write().await;
    let abandon = tokio::spawn(reservation.abandon());
    tokio::time::timeout(Duration::from_secs(5), cancel_token.cancelled())
        .await
        .expect("explicit abandon must cancel its exact runner token");
    assert!(matches!(
        runners_guard.get(&harness.parent_session_id).map(|runner| {
            assert_eq!(runner.run_id, run_id);
            &runner.status
        }),
        Some(crate::execution::AgentStatus::Running)
    ));

    abandon.abort();
    assert!(abandon.await.unwrap_err().is_cancelled());
    drop(runners_guard);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cancelled = matches!(
                harness
                    .agent_runners
                    .read()
                    .await
                    .get(&harness.parent_session_id)
                    .map(|runner| &runner.status),
                Some(crate::execution::AgentStatus::Cancelled)
            );
            let owner_released = !router.owns_run(&harness.parent_session_id, &run_id).await;
            if cancelled && owner_released {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("caller cancellation must not cancel detached reservation cleanup");
}

#[tokio::test]
async fn cancelled_pending_registration_cleans_after_router_lock_releases() {
    let harness = build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    let lock_entered = Arc::new(Notify::new());
    let release_lock = Arc::new(Notify::new());
    let lock_holder = {
        let router = router.clone();
        let lock_entered = lock_entered.clone();
        let release_lock = release_lock.clone();
        tokio::spawn(async move {
            router
                .hold_state_lock_for_test(lock_entered, release_lock)
                .await;
        })
    };
    lock_entered.notified().await;

    let sender = crate::execution::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
    )
    .await;
    let reservation_task = {
        let agent = harness.ctx.agent.clone();
        let runners = harness.agent_runners.clone();
        let senders = harness.ctx.session_event_senders.clone();
        let session_id = harness.parent_session_id.clone();
        let sender = sender.clone();
        tokio::spawn(async move {
            crate::execution::reserve_session_execution(
                &agent,
                &runners,
                &senders,
                &session_id,
                &sender,
            )
            .await
        })
    };
    let run_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(run_id) = harness
                .agent_runners
                .read()
                .await
                .get(&harness.parent_session_id)
                .map(|runner| runner.run_id.clone())
            {
                break run_id;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("raw runner reservation must become visible");

    reservation_task.abort();
    match reservation_task.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("reservation task unexpectedly completed before abort"),
    }
    assert!(matches!(
        harness
            .agent_runners
            .read()
            .await
            .get(&harness.parent_session_id)
            .map(|runner| &runner.status),
        Some(crate::execution::AgentStatus::Running)
    ));

    release_lock.notify_one();
    lock_holder.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cancelled = matches!(
                harness
                    .agent_runners
                    .read()
                    .await
                    .get(&harness.parent_session_id)
                    .map(|runner| &runner.status),
                Some(crate::execution::AgentStatus::Cancelled)
            );
            let owner_released = !router.owns_run(&harness.parent_session_id, &run_id).await;
            if cancelled && owner_released {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled pending registration must clean both ownership planes");
}

#[tokio::test]
async fn cancelled_published_placeholder_is_adopted_and_abandoned_by_raii() {
    let harness = build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    let sender = crate::execution::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
    )
    .await;
    let raw = match crate::execution::reserve_runner_core(
        &harness.agent_runners,
        &harness.ctx.session_event_senders,
        &harness.parent_session_id,
        &sender,
    )
    .await
    {
        crate::execution::ReserveOutcome::Reserved(reservation) => reservation,
        crate::execution::ReserveOutcome::AlreadyRunning(run_id) => {
            panic!("unexpected owner {run_id}")
        }
    };
    let run_id = raw.run_id.clone();
    router
        .install_owner_placeholder_for_test(&harness.parent_session_id, &run_id)
        .await;
    let mut reservation =
        crate::execution::SessionExecutionReservation::from_activation_placeholder(
            harness.parent_session_id.clone(),
            raw,
            router.clone(),
            harness.agent_runners.clone(),
        );
    reservation.mark_activation_published();

    let lock_entered = Arc::new(Notify::new());
    let release_lock = Arc::new(Notify::new());
    let lock_holder = {
        let router = router.clone();
        let lock_entered = lock_entered.clone();
        let release_lock = release_lock.clone();
        tokio::spawn(async move {
            router
                .hold_state_lock_for_test(lock_entered, release_lock)
                .await;
        })
    };
    lock_entered.notified().await;
    let adoption = tokio::spawn(async move { reservation.ensure_registered().await });
    tokio::task::yield_now().await;
    adoption.abort();
    assert!(adoption.await.unwrap_err().is_cancelled());

    release_lock.notify_one();
    lock_holder.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let owns = router.owns_run(&harness.parent_session_id, &run_id).await;
            let cancelled = matches!(
                harness
                    .agent_runners
                    .read()
                    .await
                    .get(&harness.parent_session_id)
                    .map(|runner| &runner.status),
                Some(crate::execution::AgentStatus::Cancelled)
            );
            if !owns && cancelled {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled launched placeholder must clean both ownership planes");
}

#[tokio::test]
async fn aborted_reserved_child_setup_releases_placeholder_and_runs_one_successor() {
    let mut harness =
        build_terminal_delivery_harness(Arc::new(CompletedProvider), None, None).await;
    let admission = harness
        .messenger
        .admit(SessionMessageEnvelope::user_input(
            &harness.child_session_id,
            "recover child work after its reserved setup task is aborted",
        ))
        .await
        .unwrap();
    harness
        .messenger
        .prepare_activation(&admission)
        .await
        .unwrap();
    let router = harness
        .ctx
        .agent
        .activation_router()
        .expect("delivery harness has an activation router")
        .clone();
    let child_sender = crate::execution::get_or_create_event_sender(
        &harness.ctx.session_event_senders,
        &harness.child_session_id,
    )
    .await;
    let raw = match crate::execution::reserve_runner_core(
        &harness.agent_runners,
        &harness.ctx.session_event_senders,
        &harness.child_session_id,
        &child_sender,
    )
    .await
    {
        crate::execution::ReserveOutcome::Reserved(reservation) => reservation,
        crate::execution::ReserveOutcome::AlreadyRunning(run_id) => {
            panic!("unexpected child owner {run_id}")
        }
    };
    let aborted_run_id = raw.run_id.clone();
    router
        .install_owner_placeholder_for_test(&harness.child_session_id, &aborted_run_id)
        .await;
    let mut reservation =
        crate::execution::SessionExecutionReservation::from_activation_placeholder(
            harness.child_session_id.clone(),
            raw,
            router.clone(),
            harness.agent_runners.clone(),
        );
    reservation.mark_activation_published();

    let lock_entered = Arc::new(Notify::new());
    let release_lock = Arc::new(Notify::new());
    let lock_holder = {
        let router = router.clone();
        let lock_entered = lock_entered.clone();
        let release_lock = release_lock.clone();
        tokio::spawn(async move {
            router
                .hold_state_lock_for_test(lock_entered, release_lock)
                .await;
        })
    };
    lock_entered.notified().await;
    let job = SpawnJob {
        parent_session_id: harness.parent_session_id.clone(),
        child_session_id: harness.child_session_id.clone(),
        model: "gpt-5".to_string(),
        disabled_tools: None,
    };
    let setup = harness._spawn_scheduler.launch_reserved(job, reservation);
    let expected_child_id = harness.child_session_id.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match harness.parent_rx.recv().await {
                Ok(AgentEvent::SubAgentStarted {
                    child_session_id, ..
                }) if child_session_id == expected_child_id => break,
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(error) => panic!("parent event stream closed during child setup: {error}"),
            }
        }
    })
    .await
    .expect("reserved child setup must become runnable before cancellation");
    setup.abort();
    match setup.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(()) => panic!("reserved child setup unexpectedly completed before cancellation"),
    }

    release_lock.notify_one();
    lock_holder.await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        harness.reservation_entered.notified(),
    )
    .await
    .expect("aborted child placeholder must request one bounded successor");
    assert_eq!(harness.reservations.load(Ordering::SeqCst), 1);
    assert!(
        !router
            .owns_run(&harness.child_session_id, &aborted_run_id)
            .await
    );
    assert!(matches!(
        harness
            .agent_runners
            .read()
            .await
            .get(&harness.child_session_id)
            .filter(|runner| runner.run_id == aborted_run_id)
            .map(|runner| &runner.status),
        Some(crate::execution::AgentStatus::Cancelled)
    ));

    harness.allow_reservation.notify_one();
    tokio::time::timeout(
        Duration::from_secs(10),
        harness.first_execute_returned.notified(),
    )
    .await
    .expect("one real child successor must complete its provider boundary");
    let successor_run_id = {
        let runners = harness.agent_runners.read().await;
        let runner = runners
            .get(&harness.child_session_id)
            .expect("successor child runner must be present");
        assert_ne!(runner.run_id, aborted_run_id);
        assert!(matches!(
            runner.status,
            crate::execution::AgentStatus::Running
        ));
        runner.run_id.clone()
    };
    assert!(
        router
            .owns_run(&harness.child_session_id, &successor_run_id)
            .await
    );

    harness.release_first_terminal.notify_one();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let settled = harness
                .agent_runners
                .read()
                .await
                .get(&harness.child_session_id)
                .is_some_and(|runner| {
                    runner.run_id == successor_run_id
                        && !matches!(runner.status, crate::execution::AgentStatus::Running)
                });
            let owner_released = !router
                .owns_run(&harness.child_session_id, &successor_run_id)
                .await;
            if settled && owner_released {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("child successor must settle without a phantom owner");
    let backlog = harness
        .inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
    assert_eq!(harness.reservations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn terminal_delivery_runs_only_in_one_real_successor_execution() {
    let message_id = "terminal-production-race".to_string();
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let successor_seen = Arc::new(Notify::new());
    let successor_match_count = Arc::new(AtomicUsize::new(0));
    let provider: Arc<dyn LLMProvider> = Arc::new(SuccessorRecordingProvider {
        calls: provider_calls.clone(),
        expected_message_id: message_id.clone(),
        successor_seen: successor_seen.clone(),
        successor_match_count: successor_match_count.clone(),
    });
    let harness = build_terminal_delivery_harness(provider, None, None).await;
    let job = SpawnJob {
        parent_session_id: harness.parent_session_id.clone(),
        child_session_id: harness.child_session_id.clone(),
        model: "gpt-5".to_string(),
        disabled_tools: None,
    };

    run_child_spawn(harness.ctx.clone(), job).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        harness.first_execute_returned.notified(),
    )
    .await
    .expect("first production run must reach terminal barrier");

    let mut envelope =
        SessionMessageEnvelope::user_input(&harness.child_session_id, "after final provider round");
    envelope.id = SessionMessageId::parse(message_id.clone()).unwrap();
    let receipt = harness.messenger.send(envelope.clone()).await.unwrap();
    assert_eq!(
        receipt.activation,
        bamboo_domain::SessionActivationDisposition::ActiveNotified
    );
    harness.release_first_terminal.notify_one();

    // The real sdk::spawn terminal block has entered finish_finalization and
    // asked the real coordinator for a successor. Hold that reservation long
    // enough to prove terminal cleanup did not claim or acknowledge typed work.
    tokio::time::timeout(
        Duration::from_secs(10),
        harness.reservation_entered.notified(),
    )
    .await
    .expect("terminal finalization must request a successor");
    let backlog = harness
        .inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending, 1);
    assert_eq!(backlog.claimed, 0);
    assert!(!harness
        .inbox
        .was_admitted(&harness.child_session_id, &envelope.id)
        .await
        .unwrap());
    let first_terminal_snapshot = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!first_terminal_snapshot
        .messages
        .iter()
        .any(|message| message.id == message_id));
    assert_eq!(harness.reservations.load(Ordering::SeqCst), 1);

    harness.allow_reservation.notify_one();
    tokio::time::timeout(Duration::from_secs(10), successor_seen.notified())
        .await
        .expect("real successor execution must reach its provider");
    assert_eq!(successor_match_count.load(Ordering::SeqCst), 1);

    // Wait for the successor's real terminal block to finalize its runner.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let terminal = harness
                .agent_runners
                .read()
                .await
                .get(&harness.child_session_id)
                .is_some_and(|runner| {
                    !matches!(runner.status, crate::execution::AgentStatus::Running)
                });
            if terminal {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real successor terminal block must finalize its runner");
    assert_eq!(
        provider_calls.load(Ordering::SeqCst),
        2,
        "exactly the first run and one successor may invoke the provider"
    );
    let settled = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        settled
            .messages
            .iter()
            .filter(|message| message.id == message_id)
            .count(),
        1
    );
    let backlog = harness
        .inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
    assert!(harness
        .inbox
        .was_admitted(&harness.child_session_id, &envelope.id)
        .await
        .unwrap());
}

#[tokio::test]
async fn legacy_boundary_checkpoint_failure_still_runs_one_successor() {
    let legacy_value =
        serde_json::json!({ "content": "legacy message whose first checkpoint fails" });
    let legacy_id = SessionMessageId::legacy("terminal-child", 0, &legacy_value).to_string();
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let successor_seen = Arc::new(Notify::new());
    let successor_match_count = Arc::new(AtomicUsize::new(0));
    let provider: Arc<dyn LLMProvider> = Arc::new(SuccessorRecordingProvider {
        calls: provider_calls.clone(),
        expected_message_id: legacy_id.clone(),
        successor_seen: successor_seen.clone(),
        successor_match_count: successor_match_count.clone(),
    });
    let harness =
        build_terminal_delivery_harness(provider, Some(legacy_value), Some(legacy_id.clone()))
            .await;
    let job = SpawnJob {
        parent_session_id: harness.parent_session_id.clone(),
        child_session_id: harness.child_session_id.clone(),
        model: "gpt-5".to_string(),
        disabled_tools: None,
    };

    run_child_spawn(harness.ctx.clone(), job).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        harness.first_execute_returned.notified(),
    )
    .await
    .expect("first run must complete after the injected checkpoint failure");
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);

    // The compatibility source is already CAS-cleared, while the typed claim
    // remains the sole recoverable copy. The boundary's observed generation
    // must therefore survive on the current run until terminal finalization.
    let first_snapshot = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!first_snapshot.has_pending_injected_messages());
    assert!(!first_snapshot
        .messages
        .iter()
        .any(|message| message.id == legacy_id));
    let backlog = harness
        .inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending, 0);
    assert_eq!(backlog.claimed, 1);

    harness.release_first_terminal.notify_one();
    tokio::time::timeout(
        Duration::from_secs(10),
        harness.reservation_entered.notified(),
    )
    .await
    .expect("normal-boundary generation must reach terminal activation");
    assert_eq!(harness.reservations.load(Ordering::SeqCst), 1);
    assert!(!harness
        .inbox
        .was_admitted(
            &harness.child_session_id,
            &SessionMessageId::parse(legacy_id.clone()).unwrap(),
        )
        .await
        .unwrap());

    harness.allow_reservation.notify_one();
    tokio::time::timeout(Duration::from_secs(10), successor_seen.notified())
        .await
        .expect("one real successor must retry and admit the legacy envelope");
    assert_eq!(successor_match_count.load(Ordering::SeqCst), 1);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let terminal = harness
                .agent_runners
                .read()
                .await
                .get(&harness.child_session_id)
                .is_some_and(|runner| {
                    !matches!(runner.status, crate::execution::AgentStatus::Running)
                });
            if terminal {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("successor must finalize");
    assert_eq!(provider_calls.load(Ordering::SeqCst), 2);
    assert_eq!(harness.reservations.load(Ordering::SeqCst), 1);
    let settled = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        settled
            .messages
            .iter()
            .filter(|message| message.id == legacy_id)
            .count(),
        1
    );
    let backlog = harness
        .inbox
        .inspect(&harness.child_session_id)
        .await
        .unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
}

// ---------------------------------------------------------------------------
// S-T2.2 — Sub-agents are full agents: ChildRunner never trims tools.
//
// There are no per-role tool policies anymore; the runner always emits
// `disabled_tools: None` regardless of the live tool catalog.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s_t2_2_run_child_never_disables_tools() {
    let tool_names = vec![
        "Read".to_string(),
        "Grep".to_string(),
        "Edit".to_string(),
        "Write".to_string(),
    ];
    let harness = build_harness(Arc::new(CompletedProvider), tool_names, &[]).await;
    let runner = ChildRunner::new(harness.ctx.clone());

    let input = RunChildInput {
        child_session_id: harness.child_session_id.clone(),
        parent_session_id: harness.parent_session_id.clone(),
        model: "gpt-5".to_string(),
    };
    let job = runner.build_job(&input);
    assert!(
        job.disabled_tools.is_none(),
        "sub-agents are full agents; the runner must never trim tools"
    );
}

// ---------------------------------------------------------------------------
// S-T2.4 — model override honored over the persisted session model
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s_t2_4_run_child_model_override_is_persisted() {
    let harness = build_harness(Arc::new(CompletedProvider), Vec::new(), &[]).await;
    let runner = ChildRunner::new(harness.ctx.clone());

    // Child session was persisted with model "gpt-5"; run with a different model.
    let override_model = "claude-3-7-sonnet";
    let mut parent_rx = harness.parent_rx.resubscribe();
    runner
        .run_child(RunChildInput {
            child_session_id: harness.child_session_id.clone(),
            parent_session_id: harness.parent_session_id.clone(),
            model: override_model.to_string(),
        })
        .await
        .unwrap();

    let _ = collect_until_completed(&mut parent_rx).await;

    let persisted = harness
        .storage
        .load_session(&harness.child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.model, override_model,
        "child session model must reflect the runner override"
    );
}

// ---------------------------------------------------------------------------
// S-T2.5 — watchdog timeout → SubAgentCompleted status=timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s_t2_5_watchdog_timeout_completes_with_timeout_status() {
    // Tight watchdog: 1s total cap, checked every 1s.
    let harness = build_harness(
        Arc::new(HangingProvider),
        Vec::new(),
        // The watchdog skips its immediate tick, so the first real check lands at
        // `check_interval_secs` (1s). By then `total_secs` already meets the 1s cap,
        // so the timeout fires on that first check — well before the slow stream
        // (first chunk at +2s) could finish, making the outcome deterministic.
        &[
            ("child_watchdog.max_total_secs", "1"),
            ("child_watchdog.max_idle_secs", "1"),
            ("child_watchdog.check_interval_secs", "1"),
        ],
    )
    .await;

    let mut parent_rx = harness.parent_rx.resubscribe();
    run_child_spawn(
        harness.ctx.clone(),
        SpawnJob {
            parent_session_id: harness.parent_session_id.clone(),
            child_session_id: harness.child_session_id.clone(),
            model: "gpt-5".to_string(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();

    let events = collect_until_completed_with_budget(&mut parent_rx, Duration::from_secs(45)).await;
    match events.last().unwrap() {
        AgentEvent::SubAgentCompleted { status, .. } => {
            assert_eq!(status, "timeout", "watchdog must yield timeout status");
        }
        other => panic!("expected SubAgentCompleted, got {other:?}"),
    }
}
