//! Exercise the native backend through the production spawn, Agent, Inbox and
//! canonical Actor writer. Only the provider is controlled by the test; no
//! alternate Child runner, Worker executable or broker participates.
use super::*;
use bamboo_agent_core::tools::{ToolCall, ToolError, ToolResult, ToolSchema};
use bamboo_agent_core::{Message, Role};
use bamboo_domain::{
    ActorActivationStatus, ActorLogicalState, ActorSnapshotLimits, ActorSnapshotPort,
    ActorSnapshotPrincipal, PublicLocalTaskReason, PublicLocalTaskState, SessionActivationPort,
    SessionInboxPort, SessionMessageEnvelope,
};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use std::sync::atomic::AtomicUsize;
use tokio::sync::{broadcast, oneshot, RwLock};

const MODEL: &str = "local-test-model";
const WAIT: Duration = Duration::from_secs(15);

#[cfg(unix)]
mod owned_bash_publication;

struct EmptyTools;
#[async_trait]
impl ToolExecutor for EmptyTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        Err(ToolError::NotFound(call.function.name.clone()))
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        Vec::new()
    }
}

struct EmptyCeiling;
#[async_trait]
impl NativeToolCeilingSource for EmptyCeiling {
    async fn observe(&self, _: &Session) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }
}

#[derive(Debug)]
enum ProviderReply {
    Complete(String),
    Panic,
    Error,
}

#[derive(Debug)]
struct ProviderCall {
    process_id: u32,
    messages: Vec<Message>,
    reply: oneshot::Sender<ProviderReply>,
}
impl ProviderCall {
    fn complete(self, text: &str) {
        assert!(self
            .reply
            .send(ProviderReply::Complete(text.to_string()))
            .is_ok());
    }
    fn sees(&self, text: &str) -> bool {
        self.messages.iter().any(|message| message.content == text)
    }
}

struct ActiveCall(Arc<AtomicUsize>);
impl Drop for ActiveCall {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ControlledProvider {
    calls: mpsc::UnboundedSender<ProviderCall>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
#[async_trait]
impl LLMProvider for ControlledProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let guard = ActiveCall(self.active.clone());
        let (reply, receive) = oneshot::channel();
        self.calls
            .send(ProviderCall {
                process_id: std::process::id(),
                messages: messages.to_vec(),
                reply,
            })
            .expect("provider observation receiver");
        match receive.await.expect("test releases provider") {
            ProviderReply::Complete(text) => {
                // Retain the active-call guard until the stream is consumed or
                // cancelled, rather than counting only request construction.
                let chunks = vec![Ok(LLMChunk::Token(text)), Ok(LLMChunk::Done)];
                Ok(Box::pin(futures::stream::unfold(
                    (chunks.into_iter(), guard),
                    |(mut chunks, guard)| async move {
                        chunks.next().map(|chunk| (chunk, (chunks, guard)))
                    },
                )))
            }
            ProviderReply::Panic => panic!("controlled local provider panic"),
            ProviderReply::Error => Err(LLMError::Auth(
                "controlled permanent provider failure".into(),
            )),
        }
    }
}

struct ProviderControl {
    provider: Arc<ControlledProvider>,
    calls: mpsc::UnboundedReceiver<ProviderCall>,
}
impl ProviderControl {
    fn new() -> Self {
        let (calls, receive) = mpsc::unbounded_channel();
        Self {
            provider: Arc::new(ControlledProvider {
                calls,
                active: Arc::new(AtomicUsize::new(0)),
                peak: Arc::new(AtomicUsize::new(0)),
            }),
            calls: receive,
        }
    }
    async fn next(&mut self) -> ProviderCall {
        tokio::time::timeout(WAIT, self.calls.recv())
            .await
            .expect("production local Agent must reach provider")
            .expect("provider observation channel stays open")
    }
}

struct Harness {
    home: PathBuf,
    parent: Session,
    store: Arc<bamboo_storage::SessionStoreV2>,
    inbox: Arc<dyn SessionInboxPort>,
    router: Arc<crate::SessionActivationRouter>,
    messenger: Arc<crate::SessionMessenger>,
    runner: Arc<LocalTaskChildRunner>,
    ctx: crate::execution::SpawnContext,
    parent_events: broadcast::Receiver<AgentEvent>,
    coordinator: Arc<crate::ChildCompletionCoordinator>,
    _scheduler: Arc<crate::execution::SpawnScheduler>,
}
impl Harness {
    async fn open(
        home: &std::path::Path,
        parent_id: &str,
        provider: Arc<dyn LLMProvider>,
        max_concurrent: usize,
    ) -> Self {
        Self::open_with_tools(
            home,
            parent_id,
            provider,
            max_concurrent,
            Arc::new(EmptyTools),
            Arc::new(EmptyCeiling),
        )
        .await
    }

    async fn open_with_tools(
        home: &std::path::Path,
        parent_id: &str,
        provider: Arc<dyn LLMProvider>,
        max_concurrent: usize,
        tools: Arc<dyn ToolExecutor>,
        ceiling: Arc<dyn NativeToolCeilingSource>,
    ) -> Self {
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let locked = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let router = crate::SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let activation: Arc<dyn SessionActivationPort> = router.clone();
        let messenger = Arc::new(crate::SessionMessenger::new(
            storage.clone(),
            inbox.clone(),
            activation,
        ));
        let mut config_value = Config::default();
        config_value.provider = "local-test".into();
        config_value.subagents_mut().max_concurrent = Some(max_concurrent);
        // A stale discovery path must not implicitly start Worker IPC.
        config_value.subagents_mut().fabric_dir = Some(
            home.join("forbidden-worker-fabric")
                .to_string_lossy()
                .into(),
        );
        assert!(!super::super::runtime::requires_legacy_child_runtime(
            &config_value
        ));
        let runner = Arc::new(LocalTaskChildRunner::new(
            &config_value,
            Some(ceiling),
            None,
        ));
        let config = Arc::new(RwLock::new(config_value));
        let registry = Arc::new(bamboo_llm::ProviderRegistry::new(
            HashMap::from([("local-test".to_string(), provider.clone())]),
            "local-test".to_string(),
        ));
        let provider_router = Arc::new(ProviderModelRouter::new(registry.clone()));
        let metrics = bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                home.join("metrics.db"),
            )),
            7,
        );
        let agent = Arc::new(
            Agent::builder()
                .storage(storage.clone())
                .persistence(locked.clone())
                .session_inbox(inbox.clone())
                .activation_router(router.clone())
                .session_messenger(messenger.clone())
                .attachment_reader(store.clone())
                .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
                .metrics_collector(metrics)
                .config(config.clone())
                .provider(provider)
                .default_tools(tools.clone())
                .build()
                .unwrap(),
        );
        let sessions_cache: crate::SessionCache = Arc::default();
        runner.set_local_task_runtime(Some(LocalTaskRuntime {
            agent: agent.clone(),
            tools: tools.clone(),
            provider_router: Some(provider_router.clone()),
            app_data_dir: Some(home.to_path_buf()),
            sessions_cache: sessions_cache.clone(),
        }));
        runner.set_actor_directory_store(Some(store.clone()));
        runner.set_session_inbox_runtime(Some(SessionInboxRuntimeBinding {
            router: router.clone(),
            inbox: inbox.clone(),
            storage: storage.clone(),
            persistence: locked.clone(),
            parent_question_lock: Some(locked.clone()),
        }));
        let parent = match store.load_session(parent_id).await.unwrap() {
            Some(parent) => parent,
            None => {
                let parent = Session::new(parent_id, MODEL);
                store.save_session(&parent).await.unwrap();
                parent
            }
        };
        let (parent_tx, parent_events) = broadcast::channel(256);
        let ctx = crate::execution::SpawnContext {
            agent: agent.clone(),
            tools: tools.clone(),
            sessions_cache,
            agent_runners: Arc::new(RwLock::new(HashMap::new())),
            session_event_senders: Arc::new(RwLock::new(HashMap::from([(
                parent_id.to_string(),
                parent_tx,
            )]))),
            external_child_runner: runner.clone(),
            provider_router: Some(provider_router.clone()),
            app_data_dir: Some(home.to_path_buf()),
            completion_handler: None,
            child_run_launch_hook: None,
            account_feed_inbox: None,
        };
        let coordinator = Arc::new(crate::ChildCompletionCoordinator::new(
            storage,
            locked,
            ctx.sessions_cache.clone(),
            ctx.agent_runners.clone(),
            ctx.session_event_senders.clone(),
            agent,
            config,
            registry,
            provider_router,
            home.to_path_buf(),
            None,
        ));
        coordinator.set_root_tools(tools).await;
        let scheduler = Arc::new(crate::execution::SpawnScheduler::new(ctx.clone()));
        coordinator.set_spawn_scheduler(&scheduler).await;
        router.set_spawner(coordinator.clone()).await;
        Self {
            home: home.to_path_buf(),
            parent,
            store,
            inbox,
            router,
            messenger,
            runner,
            ctx,
            parent_events,
            coordinator,
            _scheduler: scheduler,
        }
    }

    async fn child(&self, suffix: &str, prompt: &str) -> Session {
        let mut child = Session::new_child_of(
            format!("{}-{suffix}", self.parent.id),
            &self.parent,
            MODEL,
            suffix,
        );
        child.set_last_run_status("queued");
        child.add_message(Message::system("Complete the current task briefly."));
        child.add_message(Message::user(prompt));
        self.store.save_session(&child).await.unwrap();
        child
    }

    async fn launch(&self, child: &Session) -> Result<(), String> {
        crate::sdk::spawn::run_child_spawn(
            self.ctx.clone(),
            SpawnJob {
                parent_session_id: self.parent.id.clone(),
                child_session_id: child.id.clone(),
                model: MODEL.into(),
                disabled_tools: None,
            },
        )
        .await
    }

    async fn child_events(&self, child: &Session) -> broadcast::Receiver<AgentEvent> {
        crate::execution::get_or_create_event_sender(&self.ctx.session_event_senders, &child.id)
            .await
            .subscribe()
    }

    async fn completion(&mut self) -> (String, String, Option<String>) {
        tokio::time::timeout(WAIT, async {
            loop {
                match self.parent_events.recv().await.unwrap() {
                    AgentEvent::SubAgentCompleted {
                        parent_session_id,
                        child_session_id,
                        status,
                        error,
                    } => {
                        assert_eq!(parent_session_id, self.parent.id);
                        return (child_session_id, status, error);
                    }
                    AgentEvent::SubAgentEvent { .. } => {
                        panic!("native Child tokens must not be copied into Root history")
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("production spawn must publish a durable completion")
    }

    async fn assert_terminal(&self, child: &Session, status: &str) -> Session {
        // Reopen the canonical store to avoid accepting an in-memory cache as
        // persistence or a synthesized lifecycle event as actual completion.
        let reopened = bamboo_storage::SessionStoreV2::new(self.home.clone())
            .await
            .unwrap();
        let saved = reopened.load_session(&child.id).await.unwrap().unwrap();
        assert_eq!(saved.created_at, child.created_at);
        assert_eq!(saved.parent_created_at, Some(self.parent.created_at));
        assert_eq!(
            saved.parent_session_id.as_deref(),
            Some(self.parent.id.as_str())
        );
        assert_eq!(saved.root_session_id, self.parent.id);
        assert_eq!(saved.last_run_status().as_deref(), Some(status));
        assert_eq!(
            saved
                .metadata
                .get("runtime.local_task.state")
                .map(String::as_str),
            Some("stopped")
        );
        let instance = &saved.metadata["runtime.local_task.instance"];
        uuid::Uuid::parse_str(instance)
            .expect("only a joined task/resource drain stamps a stop instance");
        assert_eq!(
            saved.metadata["runtime.local_task.child_birth"],
            child.created_at.to_rfc3339()
        );
        chrono::DateTime::parse_from_rfc3339(&saved.metadata["runtime.local_task.observed_at"])
            .unwrap();
        assert!(!saved.metadata.contains_key("runtime.local_task.reason"));
        let actor = reopened.inspect_actor(&child.id).await.unwrap();
        assert!(actor.actor.matches_session(&saved));
        assert_eq!(actor.actor.ancestor_observations.len(), 1);
        assert_eq!(
            actor.actor.ancestor_observations[0].session_created_at,
            self.parent.created_at
        );
        assert_eq!(
            actor.actor.placement_intent,
            Some(bamboo_domain::ActorPlacementIntent::Local)
        );
        let activation = actor.activation.unwrap();
        let expected = match status {
            "completed" => ActorActivationStatus::Succeeded,
            "cancelled" => ActorActivationStatus::Cancelled,
            "error" => ActorActivationStatus::Failed,
            other => panic!("unsupported expected terminal {other}"),
        };
        assert_eq!(activation.status, expected);
        assert!(activation.started_at.is_some());
        assert!(activation.finished_at.is_some());
        assert!(
            activation.placement_ref.is_none(),
            "a Tokio task has no Worker placement lease"
        );
        assert_eq!(
            actor.actor.state,
            if status == "error" {
                ActorLogicalState::Failed
            } else {
                ActorLogicalState::Cold
            }
        );
        let source = crate::execution::ChildCompletionSource::from_committed_session(&saved)
            .expect("terminal history must carry its committed, read-back completion source");
        assert_eq!(source.activation_run_id, activation.run_id);
        assert_eq!(source.child_created_at, child.created_at);
        assert_eq!(source.status, status);
        assert!(self.router.current_run_id(&child.id).await.is_none());
        assert!(!self
            .ctx
            .agent_runners
            .read()
            .await
            .get(&child.id)
            .is_some_and(|runner| matches!(runner.status, crate::execution::AgentStatus::Running)));
        assert!(lock(&self.runner.executions).get(&child.id).is_none());
        self.assert_no_worker_artifacts();
        saved
    }

    fn assert_no_worker_artifacts(&self) {
        for name in [
            "broker",
            "broker.json",
            "forbidden-worker-fabric",
            ".broker-maildir-identity-v1",
        ] {
            assert!(
                !self.home.join(name).exists(),
                "native execution started forbidden {name}"
            );
        }
    }
}

fn root_id() -> String {
    format!("local-task-test-{}", uuid::Uuid::new_v4())
}

#[tokio::test]
async fn production_local_task_completes_in_host_and_commits_history_before_parent_end() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 2).await;
    let child = harness.child("single", "single native task").await;
    // A common observation bus records the actual publication order, rather
    // than comparing two receivers after asynchronous storage reads. Neither
    // the production publishers nor the canonical persistence are replaced.
    let mut events = {
        let mut senders = harness.ctx.session_event_senders.write().await;
        let sender = senders[&harness.parent.id].clone();
        senders.insert(child.id.clone(), sender.clone());
        sender.subscribe()
    };
    harness.launch(&child).await.unwrap();
    let call = control.next().await;
    assert_eq!(call.process_id, std::process::id());
    assert!(call.sees("single native task"));
    let running = harness.store.inspect_actor(&child.id).await.unwrap();
    assert_eq!(running.actor.state, ActorLogicalState::Active);
    assert_eq!(
        running.activation.unwrap().status,
        ActorActivationStatus::Running
    );
    call.complete("native result");
    tokio::time::timeout(WAIT, async {
        let mut complete = false;
        let mut committed = false;
        loop {
            match events.recv().await.unwrap() {
                AgentEvent::Complete { .. } => complete = true,
                AgentEvent::SessionHistoryCommitted { session_id } => {
                    assert_eq!(session_id, child.id);
                    assert!(complete, "Agent end must precede durable history publication");
                    committed = true;
                }
                AgentEvent::SubAgentCompleted { child_session_id, .. } => {
                    assert_eq!(child_session_id, child.id);
                    assert!(committed, "history commit barrier must precede parent completion on the same publication sequence");
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("ordered production completion publication");
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "completed", "{error:?}");
    let saved = harness.assert_terminal(&child, "completed").await;
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant && m.content == "native result")
            .count(),
        1
    );
    let source = crate::execution::ChildCompletionSource::from_committed_session(&saved).unwrap();
    assert_eq!(
        source.result(&saved, &harness.parent).as_deref(),
        Some("native result")
    );
    let root = harness
        .store
        .load_session(&harness.parent.id)
        .await
        .unwrap()
        .unwrap();
    assert!(root
        .messages
        .iter()
        .all(|message| message.content != "native result"));
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_tasks_are_isolated_and_enforce_configured_concurrency() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 2).await;
    let a = harness.child("a", "private input a").await;
    let b = harness.child("b", "private input b").await;
    let c = harness.child("c", "private input c").await;
    harness.launch(&a).await.unwrap();
    let first = control.next().await;
    harness.launch(&b).await.unwrap();
    let second = control.next().await;
    assert!(first.sees("private input a") && !first.sees("private input b"));
    assert!(second.sees("private input b") && !second.sees("private input a"));
    harness.launch(&c).await.unwrap();
    assert_eq!(harness.runner.concurrency.available_permits(), 0);
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    first.complete("private result a");
    let third = control.next().await;
    assert!(third.sees("private input c"));
    assert!(!third.sees("private input a") && !third.sees("private input b"));
    second.complete("private result b");
    third.complete("private result c");
    let mut completed = std::collections::BTreeSet::new();
    for _ in 0..3 {
        let (id, status, error) = harness.completion().await;
        assert_eq!(status, "completed", "{error:?}");
        assert!(completed.insert(id), "duplicate terminal completion");
    }
    assert_eq!(
        completed,
        [a.id.clone(), b.id.clone(), c.id.clone()]
            .into_iter()
            .collect()
    );
    for (child, own, foreign) in [
        (&a, "private result a", "private result b"),
        (&b, "private result b", "private result c"),
        (&c, "private result c", "private result a"),
    ] {
        let saved = harness.assert_terminal(child, "completed").await;
        assert!(saved.messages.iter().any(|message| message.content == own));
        assert!(saved
            .messages
            .iter()
            .all(|message| message.content != foreign));
    }
    assert_eq!(control.provider.peak.load(Ordering::SeqCst), 2);
    assert_eq!(control.provider.active.load(Ordering::SeqCst), 0);
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_task_cancellation_waits_for_owned_execution_and_persists_cancelled() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness.child("cancel", "cancel this native task").await;
    let mut events = harness.child_events(&child).await;
    harness.launch(&child).await.unwrap();
    let call = control.next().await;
    let cancel = harness.ctx.agent_runners.read().await[&child.id]
        .cancel_token
        .clone();
    cancel.cancel();
    // Allow the controlled provider boundary to unwind. A cancel request alone
    // is never asserted to be the durable proof that a task has stopped.
    let _ = call
        .reply
        .send(ProviderReply::Complete("late response".into()));
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "cancelled", "{error:?}");
    let stopped = harness.assert_terminal(&child, "cancelled").await;
    assert!(stopped
        .messages
        .iter()
        .all(|message| !message.content.contains("late response")));
    let root = harness
        .store
        .load_session(&harness.parent.id)
        .await
        .unwrap()
        .unwrap();
    assert!(root
        .messages
        .iter()
        .all(|message| !message.content.contains("late response")));
    while let Ok(event) = events.try_recv() {
        assert!(
            !serde_json::to_string(&event)
                .unwrap()
                .contains("late response"),
            "cancelled provider output must not leak into observable Child events"
        );
    }
    assert_eq!(control.provider.active.load(Ordering::SeqCst), 0);
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_task_natural_completion_wins_late_cancel_without_poisoning_successor() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness
        .child("completion-wins", "finish before late cancellation")
        .await;
    let mut events = harness.child_events(&child).await;
    harness.launch(&child).await.unwrap();
    let call = control.next().await;
    let run_id = harness.router.current_run_id(&child.id).await.unwrap();
    let execution = harness.runner.execution(&child, &run_id).unwrap();
    let cancel = harness.ctx.agent_runners.read().await[&child.id]
        .cancel_token
        .clone();

    // Hold a real owned task at the drain boundary. This makes publication
    // remain pending after the actual Agent Complete event without sleeping,
    // replacing the runner, or manufacturing any stop/completion proof.
    let (release_drain, drain_gate) = oneshot::channel();
    let drained_task = execution
        .resources
        .spawn_owned(async move {
            drain_gate.await.unwrap();
        })
        .unwrap();
    call.complete("natural completion won");
    tokio::time::timeout(WAIT, async {
        loop {
            if matches!(events.recv().await.unwrap(), AgentEvent::Complete { .. }) {
                break;
            }
        }
    })
    .await
    .expect("real Agent completion boundary");
    assert_eq!(
        harness.router.current_run_id(&child.id).await.as_deref(),
        Some(run_id.as_str())
    );
    assert!(
        lock(&execution.stop).is_none(),
        "publication still awaits the owned drain task"
    );
    cancel.cancel();
    while let Ok(event) = harness.parent_events.try_recv() {
        assert!(
            !matches!(event, AgentEvent::SubAgentCompleted { .. }),
            "late cancel cannot publish before the actual owned resource joins"
        );
    }
    release_drain.send(()).unwrap();
    tokio::time::timeout(WAIT, drained_task)
        .await
        .unwrap()
        .unwrap();
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "completed", "{error:?}");
    let first = harness.assert_terminal(&child, "completed").await;
    assert_eq!(
        first
            .messages
            .iter()
            .filter(|m| m.content == "natural completion won")
            .count(),
        1
    );
    let first_source =
        crate::execution::ChildCompletionSource::from_committed_session(&first).unwrap();
    assert_eq!(first_source.activation_run_id, run_id);
    assert_eq!(
        first_source.result(&first, &harness.parent).as_deref(),
        Some("natural completion won")
    );

    // A subsequent real Inbox activation of the same logical Child must get a
    // fresh cancellation domain, preserve its previous answer, and finish once.
    let envelope =
        SessionMessageEnvelope::user_input(&child.id, "successor after late cancellation");
    let admission = harness.messenger.admit(envelope.clone()).await.unwrap();
    harness
        .messenger
        .prepare_activation(&admission)
        .await
        .unwrap();
    harness
        .router
        .reconcile_wake(&child.id, chrono::Utc::now())
        .await
        .unwrap();
    let successor = control.next().await;
    assert!(successor.sees("natural completion won"));
    let running = harness
        .store
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.metadata["runtime.local_task.state"], "running");
    assert_ne!(running.metadata["runtime.local_task.run_id"], run_id);
    assert_eq!(
        running.metadata["runtime.local_task.child_birth"],
        child.created_at.to_rfc3339()
    );
    assert!(
        !running.metadata.contains_key("runtime.local_task.instance"),
        "a successor must not inherit an older stop proof"
    );
    assert!(!running.metadata.contains_key("runtime.local_task.reason"));
    assert_eq!(
        successor
            .messages
            .iter()
            .filter(|m| m.id == envelope.id.as_str())
            .count(),
        1
    );
    cancel.cancel();
    successor.complete("successor unaffected");
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "completed", "{error:?}");
    let final_child = harness.assert_terminal(&child, "completed").await;
    for text in ["natural completion won", "successor unaffected"] {
        assert_eq!(
            final_child
                .messages
                .iter()
                .filter(|m| m.content == text)
                .count(),
            1
        );
    }
    let source =
        crate::execution::ChildCompletionSource::from_committed_session(&final_child).unwrap();
    assert_ne!(source.activation_run_id, first_source.activation_run_id);
    assert_eq!(
        source.result(&final_child, &harness.parent).as_deref(),
        Some("successor unaffected")
    );
    assert_eq!(
        harness
            .store
            .inspect_actor(&child.id)
            .await
            .unwrap()
            .actor
            .current_attempt,
        2
    );
    assert_eq!(harness.inbox.inspect(&child.id).await.unwrap().pending, 0);
    while let Ok(event) = harness.parent_events.try_recv() {
        assert!(
            !matches!(event, AgentEvent::SubAgentCompleted { .. }),
            "no duplicate terminal publication"
        );
    }
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_task_panic_quarantines_unknown_stop_and_preserves_healthy_sibling() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 2).await;
    let child = harness.child("panic", "panic inside native task").await;
    let admission_before = harness.runner.admission.available_permits();
    let mut child_events = harness.child_events(&child).await;
    harness.launch(&child).await.unwrap();
    let call = control.next().await;
    let execution = lock(&harness.runner.executions)
        .get(&child.id)
        .unwrap()
        .clone();
    let run_id = execution.owner.fence.run_id.clone();
    assert!(call.reply.send(ProviderReply::Panic).is_ok());
    // The coordinator genuinely exits, but a generic panic cannot prove that
    // every filesystem operation started by the Agent has stopped.
    tokio::time::timeout(WAIT, async {
        loop {
            let finished = {
                let coordinators = lock(&harness.runner.coordinators);
                !coordinators.is_empty() && coordinators.iter().all(|task| task.is_finished())
            };
            if finished {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("panic must reach the production quarantine boundary");
    let saved = harness
        .store
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.last_run_status().as_deref(), Some("error"));
    assert!(saved
        .last_run_error()
        .as_deref()
        .is_some_and(|error| error.contains("unknown")));
    assert!(crate::execution::ChildCompletionSource::from_committed_session(&saved).is_none());
    assert!(!saved.metadata.contains_key("runtime.local_task.instance"));
    assert_eq!(
        saved
            .metadata
            .get("runtime.local_task.state")
            .map(String::as_str),
        Some("stop_unconfirmed")
    );
    assert_eq!(saved.metadata["runtime.local_task.run_id"], run_id);
    assert_eq!(
        saved.metadata["runtime.local_task.child_birth"],
        child.created_at.to_rfc3339()
    );
    assert_eq!(
        saved.metadata["runtime.local_task.reason"],
        "execution_stop_unconfirmed"
    );
    chrono::DateTime::parse_from_rfc3339(&saved.metadata["runtime.local_task.observed_at"])
        .unwrap();
    let reopened = bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let cold = reopened.load_session(&child.id).await.unwrap().unwrap();
    for key in LOCAL_TASK_OBSERVATION_KEYS {
        assert_eq!(
            saved.metadata.get(key),
            cold.metadata.get(key),
            "diagnostic must survive a cold canonical read"
        );
    }
    let snapshot = reopened
        .actor_subtree_snapshot_v2(
            ActorSnapshotPrincipal::host_owner(),
            &harness.parent.id,
            &harness.parent.id,
            ActorSnapshotLimits::default(),
        )
        .await
        .unwrap();
    let node = snapshot
        .nodes
        .iter()
        .find(|node| node.actor.actor_id == child.id)
        .unwrap();
    let diagnostic = node
        .local_task
        .as_ref()
        .expect("the production panic must reach the cold read-only projection");
    assert_eq!(diagnostic.state, PublicLocalTaskState::StopUnconfirmed);
    assert_eq!(
        diagnostic.reason,
        Some(PublicLocalTaskReason::ExecutionStopUnconfirmed)
    );
    assert_eq!(
        diagnostic.observed_at.unwrap().to_rfc3339(),
        saved.metadata["runtime.local_task.observed_at"]
    );
    assert_eq!(
        node.actor.activation.as_ref().unwrap().status,
        ActorActivationStatus::Running
    );
    assert!(
        diagnostic.lease_expires_at.unwrap() > chrono::Utc::now(),
        "explicit unknown is visible even while its exact Actor lease remains valid"
    );
    let repeated = execution.persist_stop_unconfirmed().await.unwrap();
    assert_eq!(
        repeated.metadata["runtime.local_task.observed_at"],
        saved.metadata["runtime.local_task.observed_at"],
        "replay must retain the first evidence timestamp"
    );
    assert_eq!(
        harness.runner.admission.available_permits(),
        admission_before - 1,
        "a quarantined coordinator exiting must not release admission"
    );
    assert!(lock(&execution.stop).is_none());
    assert_eq!(
        harness.router.current_run_id(&child.id).await.as_deref(),
        Some(run_id.as_str())
    );
    assert!(harness
        .router
        .register_run(&child.id, "must-not-replace-unconfirmed")
        .await
        .is_err());
    let actor = harness.store.inspect_actor(&child.id).await.unwrap();
    assert_eq!(
        actor.activation.unwrap().status,
        ActorActivationStatus::Running
    );
    assert_eq!(
        harness.runner.concurrency.available_permits(),
        1,
        "unknown-stop task must retain its concurrency capacity"
    );
    while let Ok(event) = harness.parent_events.try_recv() {
        assert!(
            !matches!(event, AgentEvent::SubAgentCompleted { .. }),
            "unconfirmed panic must not publish a terminal Child result"
        );
    }
    while let Ok(event) = child_events.try_recv() {
        assert!(
            !matches!(event, AgentEvent::SessionHistoryCommitted { .. }),
            "unconfirmed panic must not publish a final history barrier"
        );
    }
    let healthy = harness
        .child("after-panic", "healthy task after sibling panic")
        .await;
    harness.launch(&healthy).await.unwrap();
    control.next().await.complete("Host is still healthy");
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, healthy.id);
    assert_eq!(status, "completed", "{error:?}");
    harness.assert_terminal(&healthy, "completed").await;
    assert_eq!(harness.runner.concurrency.available_permits(), 1);
    assert_eq!(
        harness.router.current_run_id(&child.id).await.as_deref(),
        Some(run_id.as_str())
    );
    assert!(harness
        .router
        .register_run(&child.id, "must-not-replace-unconfirmed")
        .await
        .is_err());
    assert!(harness
        .runner
        .shutdown()
        .await
        .unwrap_err()
        .contains("unconfirmed"));
}

#[tokio::test]
async fn production_local_task_provider_error_confirms_stop_and_releases_slot() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness.child("error", "ordinary provider failure").await;
    harness.launch(&child).await.unwrap();
    assert!(control
        .next()
        .await
        .reply
        .send(ProviderReply::Error)
        .is_ok());
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "error");
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("controlled permanent provider failure")),
        "{error:?}"
    );
    harness.assert_terminal(&child, "error").await;
    assert_eq!(control.provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(harness.runner.concurrency.available_permits(), 1);
    let healthy = harness
        .child("after-error", "healthy task after provider error")
        .await;
    harness.launch(&healthy).await.unwrap();
    control.next().await.complete("ordinary error recovered");
    assert_eq!(harness.completion().await.1, "completed");
    harness.assert_terminal(&healthy, "completed").await;
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_restart_replays_canonical_inbox_once_after_confirmed_stop() {
    let home = tempfile::tempdir().unwrap();
    let parent_id = root_id();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &parent_id, control.provider.clone(), 1).await;
    let child = harness.child("replay", "initial task before restart").await;
    harness.launch(&child).await.unwrap();
    control.next().await.complete("first result");
    assert_eq!(harness.completion().await.1, "completed");
    let first = harness.assert_terminal(&child, "completed").await;
    let first_source =
        crate::execution::ChildCompletionSource::from_committed_session(&first).unwrap();
    let first_actor = harness.store.inspect_actor(&child.id).await.unwrap();
    let envelope =
        SessionMessageEnvelope::user_input(&child.id, "durable continuation across restart");
    let admission = harness.messenger.admit(envelope.clone()).await.unwrap();
    harness
        .messenger
        .prepare_activation(&admission)
        .await
        .unwrap();
    assert_eq!(harness.inbox.inspect(&child.id).await.unwrap().pending, 1);
    harness.runner.shutdown().await.unwrap();
    drop(harness);

    let mut reopened = Harness::open(home.path(), &parent_id, control.provider.clone(), 1).await;
    reopened
        .router
        .reconcile_wake(&child.id, chrono::Utc::now())
        .await
        .unwrap();
    let successor = control.next().await;
    assert_eq!(
        successor
            .messages
            .iter()
            .filter(|message| message.id == envelope.id.as_str())
            .count(),
        1
    );
    assert!(successor.sees("first result"));
    successor.complete("resumed once");
    let (id, status, error) = reopened.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "completed", "{error:?}");
    let saved = reopened.assert_terminal(&child, "completed").await;
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|message| message.id == envelope.id.as_str())
            .count(),
        1
    );
    let source = crate::execution::ChildCompletionSource::from_committed_session(&saved).unwrap();
    assert_ne!(source.activation_run_id, first_source.activation_run_id);
    let actor = reopened.store.inspect_actor(&child.id).await.unwrap();
    assert_eq!(
        actor.actor.current_attempt,
        first_actor.actor.current_attempt + 1
    );
    let backlog = reopened.inbox.inspect(&child.id).await.unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
    assert!(reopened
        .inbox
        .was_admitted(&child.id, &envelope.id)
        .await
        .unwrap());
    reopened
        .router
        .reconcile_wake(&child.id, chrono::Utc::now())
        .await
        .unwrap();
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    reopened.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_restart_rejects_unknown_stop_without_relaunch_or_false_terminal() {
    let home = tempfile::tempdir().unwrap();
    let parent_id = root_id();
    let mut control = ProviderControl::new();
    let original = Harness::open(home.path(), &parent_id, control.provider.clone(), 1).await;
    let mut child = original
        .child("unknown", "must not replay an unproven task")
        .await;
    let child_birth = child.created_at;
    record_local_task_observation(
        &mut child,
        "unproven-before-restart",
        child_birth,
        "running",
    );
    child.set_last_run_status("running");
    original.store.save_session(&child).await.unwrap();
    original.store.ensure_actor(&child.id).await.unwrap();
    let old_now = chrono::Utc::now() - chrono::Duration::minutes(1);
    let expired = original
        .store
        .claim_activation(&ActorActivationClaim {
            actor_id: child.id.clone(),
            run_id: "unproven-before-restart".into(),
            lease_owner: "lost-local-host".into(),
            lease_expires_at: old_now + chrono::Duration::seconds(10),
            inbox_generation: 0,
            placement_ref: None,
            now: old_now,
        })
        .await
        .unwrap();
    original
        .store
        .start_activation(&expired.fence(), old_now)
        .await
        .unwrap();
    let unproven_actor = original.store.inspect_actor(&child.id).await.unwrap();
    assert!(unproven_actor.activation.as_ref().unwrap().lease_expires_at < chrono::Utc::now());
    original.runner.shutdown().await.unwrap();
    drop(original);

    let reopened = Harness::open(home.path(), &parent_id, control.provider.clone(), 1).await;
    let error = reopened.launch(&child).await.unwrap_err();
    assert!(
        error.contains("confirmed stop") && error.contains("blocked"),
        "{error}"
    );
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let saved = reopened
        .store
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.last_run_status().as_deref(), Some("running"));
    assert_eq!(
        serde_json::to_value(&saved.messages).unwrap(),
        serde_json::to_value(&child.messages).unwrap()
    );
    assert!(!saved.metadata.contains_key("runtime.local_task.instance"));
    assert!(crate::execution::ChildCompletionSource::from_committed_session(&saved).is_none());
    assert!(lock(&reopened.runner.executions).is_empty());
    assert_eq!(
        reopened.store.inspect_actor(&child.id).await.unwrap(),
        unproven_actor,
        "lease expiry alone must neither authorize replacement nor fabricate a stop"
    );
    reopened.assert_no_worker_artifacts();
    reopened.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_task_queued_cancellation_does_not_enter_provider_or_block_sibling() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let active = harness
        .child("active", "active child holds only slot")
        .await;
    let queued = harness
        .child("queued", "cancel while waiting for slot")
        .await;
    harness.launch(&active).await.unwrap();
    let active_call = control.next().await;
    harness.launch(&queued).await.unwrap();
    assert_eq!(harness.runner.concurrency.available_permits(), 0);
    harness.ctx.agent_runners.read().await[&queued.id]
        .cancel_token
        .cancel();
    let (id, status, error) = harness.completion().await;
    assert_eq!(
        id, queued.id,
        "queued cancellation must not wait for sibling slot"
    );
    assert_eq!(status, "cancelled", "{error:?}");
    harness.assert_terminal(&queued, "cancelled").await;
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(control.provider.active.load(Ordering::SeqCst), 1);
    active_call.complete("active sibling finishes independently");
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, active.id);
    assert_eq!(status, "completed", "{error:?}");
    harness.assert_terminal(&active, "completed").await;
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn production_local_child_completion_resumes_waiting_parent_through_canonical_inbox() {
    use bamboo_domain::{
        AgentRuntimeState, AgentStatusState, ChildWaitPolicy, SuspensionState,
        WaitingForChildrenState,
    };

    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness
        .child("parent-wait", "private native child assignment")
        .await;
    let mut parent = harness.parent.clone();
    let now = chrono::Utc::now();
    let mut runtime = AgentRuntimeState::new("root-awaiting-native-child");
    runtime.status = AgentStatusState::Suspended;
    runtime.suspension = Some(SuspensionState {
        reason: "waiting_for_children".into(),
        suspended_at: now,
        resumable: true,
        hook_point: None,
    });
    let mut wait =
        WaitingForChildrenState::for_children(vec![child.id.clone()], ChildWaitPolicy::All, now);
    wait.registered_by_tool_call_id = Some("native-child-launch".into());
    runtime.waiting_for_children = Some(wait);
    parent.agent_runtime_state = Some(runtime);
    parent.metadata.insert(
        "runtime.suspend_reason".into(),
        "waiting_for_children".into(),
    );
    parent.set_last_run_status("suspended");
    harness.store.save_session(&parent).await.unwrap();
    harness.ctx.completion_handler = Some(harness.coordinator.clone());

    // The actual local Child, rather than a fabricated completion event,
    // produces the result consumed by the real coordinator and Root Agent.
    harness.launch(&child).await.unwrap();
    let child_call = control.next().await;
    assert!(child_call.sees("private native child assignment"));
    child_call.complete("native child result for the parent");
    let (id, status, error) = harness.completion().await;
    assert_eq!(id, child.id);
    assert_eq!(status, "completed", "{error:?}");

    let parent_call = control.next().await;
    assert_eq!(parent_call.process_id, std::process::id());
    assert!(!parent_call.sees("private native child assignment"));
    assert_eq!(
        parent_call
            .messages
            .iter()
            .filter(|message| {
                message
                    .content
                    .contains("Child final response:\nnative child result for the parent")
                    && message
                        .metadata
                        .as_ref()
                        .and_then(|metadata| metadata.get("runtime_kind"))
                        .and_then(serde_json::Value::as_str)
                        == Some("child_completion_resume")
            })
            .count(),
        1,
        "Root must reason over exactly one canonical Child result"
    );
    parent_call.complete("parent resumed from the native child");

    tokio::time::timeout(WAIT, async {
        loop {
            let terminal = harness
                .ctx
                .agent_runners
                .read()
                .await
                .get(&parent.id)
                .is_some_and(|runner| {
                    !matches!(runner.status, crate::execution::AgentStatus::Running)
                });
            if terminal && harness.router.current_run_id(&parent.id).await.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real Root successor must durably finish");
    let saved = harness
        .store
        .load_session(&parent.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.last_run_status().as_deref(), Some("completed"));
    assert!(saved
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .is_none());
    assert!(!saved.metadata.contains_key("runtime.suspend_reason"));
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|message| message.content == "parent resumed from the native child")
            .count(),
        1
    );
    let backlog = harness.inbox.inspect(&parent.id).await.unwrap();
    assert_eq!(backlog.pending + backlog.claimed, 0);
    assert!(saved
        .session_inbox_admission()
        .is_some_and(|admission| admission.last_admitted_sequence > 0));
    harness.assert_terminal(&child, "completed").await;
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    harness.runner.shutdown().await.unwrap();
}

/// These lifecycle tests use a genuinely accepted Actor setup and the real
/// no-work cancellation/drain path. They do not manufacture a task-stop proof:
/// no Agent is launched, and stop_unstarted permanently closes that launch.
async fn checkpoint_unstarted_cancellation(
    runner: &LocalTaskChildRunner,
    execution: &Execution,
    mut child: Session,
    run_id: &str,
) -> Session {
    execution.cancel.cancel();
    runner
        .stop_unstarted(execution, &mut child, run_id)
        .await
        .unwrap();
    child.set_last_run_status("cancelled");
    child.set_last_run_error("cancelled before Agent launch in lifecycle regression");
    execution
        .persistence
        .checkpoint_runtime_session(&mut child)
        .await
        .unwrap();
    child
}

#[tokio::test]
async fn local_admission_stays_occupied_through_publication_tail_then_releases_without_harvest() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness
        .child("publication-tail", "unused unstarted Child input")
        .await;
    let run_id = "actual-publication-tail-coordinator";
    let registration = harness
        .router
        .register_run(&child.id, run_id)
        .await
        .unwrap();
    let before = harness.runner.admission.available_permits();
    harness
        .runner
        .prepare_child_execution(&child, run_id, CancellationToken::new())
        .await
        .unwrap();
    let execution = harness.runner.execution(&child, run_id).unwrap();
    assert_eq!(harness.runner.admission.available_permits(), before - 1);

    let (release_finish, finish_released) = oneshot::channel();
    let (finished, finish_observed) = oneshot::channel();
    let (release_tail, tail_released) = oneshot::channel();
    let coordinator = {
        let runner = harness.runner.clone();
        let execution = execution.clone();
        let child = child.clone();
        tokio::spawn(async move {
            finish_released.await.unwrap();
            let stopped =
                checkpoint_unstarted_cancellation(&runner, &execution, child, run_id).await;
            runner
                .finish_child_execution(&stopped, run_id, true)
                .await
                .unwrap();
            finished.send(()).unwrap();
            // This real task remains alive after backend finalization, just as
            // SDK publication may still be writing Root completion delivery.
            tail_released.await.unwrap();
        })
    };
    assert!(harness
        .runner
        .track_child_task(&child.id, run_id, coordinator)
        .is_none());
    assert_eq!(
        harness.runner.admission.available_permits(),
        before - 1,
        "tracking must transfer admission to the actual coordinator, not release it"
    );
    release_finish.send(()).unwrap();
    tokio::time::timeout(WAIT, finish_observed)
        .await
        .unwrap()
        .unwrap();
    assert!(lock(&harness.runner.executions).get(&child.id).is_none());
    assert_eq!(
        harness
            .store
            .inspect_actor(&child.id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status,
        ActorActivationStatus::Cancelled
    );
    assert_eq!(
        harness.runner.admission.available_permits(),
        before - 1,
        "finish must not release capacity while the publication tail is alive"
    );
    release_tail.send(()).unwrap();
    tokio::time::timeout(WAIT, async {
        while harness.runner.admission.available_permits() != before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("coordinator join must return capacity without another prepare/track harvest");
    assert!(
        !lock(&harness.runner.coordinators).is_empty(),
        "the no-harvest assertion must observe a retained supervisor handle"
    );
    assert!(matches!(
        control.calls.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    registration.finish(0).await.unwrap();
    harness.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_shutdown_keeps_harvested_panic_and_wakes_both_finish_and_shutdown_handoff_waiters() {
    let home = tempfile::tempdir().unwrap();
    let control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness
        .child("shutdown-handoff", "unused accepted setup input")
        .await;
    let run_id = "actual-shutdown-handoff-coordinator";
    let registration = harness
        .router
        .register_run(&child.id, run_id)
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    harness
        .runner
        .prepare_child_execution(&child, run_id, cancel.clone())
        .await
        .unwrap();
    let execution = harness.runner.execution(&child, run_id).unwrap();

    // A real already-finished task failure must survive being harvested by a
    // later accepted handoff while shutdown is waiting for that handoff.
    let failed = tokio::spawn(async {
        panic!("actual coordinator panic retained during shutdown handoff");
        #[allow(unreachable_code)]
        Ok::<(), String>(())
    });
    tokio::time::timeout(WAIT, async {
        while !failed.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("injected real coordinator task must finish panicking");
    lock(&harness.runner.coordinators).push(failed);
    let shutdown = {
        let runner = harness.runner.clone();
        tokio::spawn(async move { runner.shutdown().await })
    };
    tokio::time::timeout(WAIT, cancel.cancelled())
        .await
        .unwrap();
    // Cancellation is sent after shutdown snapshots accepted execution. Wait
    // for its persistent watch subscription before creating the other waiter.
    tokio::time::timeout(WAIT, async {
        while execution.launch_handoff.receiver_count() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown must park on the accepted setup handoff");
    assert!(!shutdown.is_finished());

    let (enter_finish, finish_entered) = oneshot::channel();
    let coordinator = {
        let runner = harness.runner.clone();
        let execution = execution.clone();
        let child = child.clone();
        tokio::spawn(async move {
            let stopped =
                checkpoint_unstarted_cancellation(&runner, &execution, child, run_id).await;
            enter_finish.send(()).unwrap();
            runner
                .finish_child_execution(&stopped, run_id, true)
                .await
                .unwrap();
        })
    };
    tokio::time::timeout(WAIT, finish_entered)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(WAIT, async {
        while execution.launch_handoff.receiver_count() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("finish and shutdown must both be waiting before the single handoff");
    assert!(harness
        .runner
        .track_child_task(&child.id, run_id, coordinator)
        .is_none());
    let result = tokio::time::timeout(WAIT, shutdown)
        .await
        .expect("one published handoff must wake both finish and shutdown")
        .expect("shutdown task itself must join");
    let error =
        result.expect_err("harvest during shutdown must not turn a coordinator panic into success");
    assert!(error.contains("panic"), "{error}");
    assert!(
        lock(&execution.stop).is_some(),
        "no-work cancellation really drained its resource domain"
    );
    assert!(lock(&harness.runner.executions).get(&child.id).is_none());
    assert_eq!(
        harness
            .store
            .inspect_actor(&child.id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status,
        ActorActivationStatus::Cancelled
    );
    registration.finish(0).await.unwrap();
}

#[tokio::test]
async fn local_unstarted_cleanup_failure_records_durable_unknown_stop() {
    let home = tempfile::tempdir().unwrap();
    let control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness
        .child("unstarted-unknown", "never enters Agent")
        .await;
    let run_id = "accepted-unstarted-cleanup-failure";
    let registration = harness
        .router
        .register_run(&child.id, run_id)
        .await
        .unwrap();
    harness
        .runner
        .prepare_child_execution(&child, run_id, CancellationToken::new())
        .await
        .unwrap();
    let execution = harness.runner.execution(&child, run_id).unwrap();
    execution
        .resources
        .record_unconfirmed("owned setup resource stop cannot be confirmed");
    assert!(harness
        .runner
        .abandon_child_execution(&child, run_id)
        .await
        .is_err());
    let cold = bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let saved = cold.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(
        saved.metadata["runtime.local_task.state"],
        "stop_unconfirmed"
    );
    assert_eq!(saved.metadata["runtime.local_task.run_id"], run_id);
    assert_eq!(
        saved.metadata["runtime.local_task.child_birth"],
        child.created_at.to_rfc3339()
    );
    assert_eq!(
        saved.metadata["runtime.local_task.reason"],
        "execution_stop_unconfirmed"
    );
    assert!(!saved.metadata.contains_key("runtime.local_task.instance"));
    assert!(lock(&execution.stop).is_none());
    assert!(execution.task.lock().await.is_none());
    registration.quarantine().await;
    assert!(harness
        .runner
        .shutdown()
        .await
        .unwrap_err()
        .contains("unconfirmed"));
}

#[tokio::test]
async fn local_coordinator_abort_joins_agent_and_late_writer_before_unknown_observation() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let mut child = harness
        .child("coordinator-abort", "enter the real Agent provider")
        .await;
    let run_id = "actual-coordinator-aborted-during-agent";
    let registration = harness
        .router
        .register_run(&child.id, run_id)
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    harness
        .runner
        .prepare_child_execution(&child, run_id, cancel.clone())
        .await
        .unwrap();
    let execution = harness.runner.execution(&child, run_id).unwrap();
    child
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .run_id = run_id.into();
    execution
        .persistence
        .checkpoint_runtime_session(&mut child)
        .await
        .unwrap();
    let admission_during = harness.runner.admission.available_permits();
    let (events, mut event_rx) = mpsc::channel(256);
    let event_drain = tokio::spawn(async move { while event_rx.recv().await.is_some() {} });
    let coordinator = {
        let runner = harness.runner.clone();
        let mut child = child.clone();
        let job = SpawnJob {
            parent_session_id: harness.parent.id.clone(),
            child_session_id: child.id.clone(),
            model: MODEL.into(),
            disabled_tools: None,
        };
        tokio::spawn(async move {
            let _ = runner
                .execute_external_child(&mut child, &job, events, cancel)
                .await;
        })
    };
    let abort = coordinator.abort_handle();
    harness
        .runner
        .track_child_task(&child.id, run_id, coordinator);
    let _actual_provider_call = control.next().await;
    let mut stale = execution
        .persistence
        .load_runtime_control_plane(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stale.metadata["runtime.local_task.state"], "running");
    let (release_write, write_released) = oneshot::channel();
    let persistence = execution.persistence.clone();
    let late_write = execution
        .resources
        .spawn_owned(async move {
            write_released.await.unwrap();
            // A real registered resource still owns the old Running control plane.
            // It must finish writing before the supervisor's final observation.
            persistence
                .save_runtime_control_plane(&mut stale)
                .await
                .unwrap();
        })
        .unwrap();
    abort.abort();
    tokio::time::timeout(WAIT, execution.cancel.cancelled())
        .await
        .unwrap();
    // The coordinator is already joined before it cancels this token. The
    // supervisor must now own the actual Agent join mutex while the gated
    // resource keeps that Agent unfinished. An implementation that only
    // signals cancellation never acquires this ownership and fails here.
    tokio::time::timeout(WAIT, async {
        loop {
            if execution.task.try_lock().is_err() {
                break;
            }
            assert!(
                lock(&harness.runner.coordinators)
                    .iter()
                    .any(|task| !task.is_finished()),
                "supervisor exited without joining the surviving Agent writer"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("supervisor must take ownership of the Agent join before recording unknown");
    let before_release = execution
        .persistence
        .load_runtime_control_plane(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before_release.metadata["runtime.local_task.state"], "running",
        "unknown cannot be finalized while the old writer remains active"
    );
    assert!(
        lock(&harness.runner.coordinators)
            .iter()
            .any(|task| !task.is_finished()),
        "cancel is not evidence that the separate Agent/resource writer has exited"
    );
    assert_eq!(
        harness.runner.admission.available_permits(),
        admission_during
    );
    assert!(lock(&execution.stop).is_none());
    release_write.send(()).unwrap();
    tokio::time::timeout(WAIT, late_write)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(WAIT, async {
        while !lock(&harness.runner.coordinators)
            .iter()
            .all(|task| task.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(WAIT, event_drain)
        .await
        .unwrap()
        .unwrap();
    let cold = bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let saved = cold.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(
        saved.metadata["runtime.local_task.state"],
        "stop_unconfirmed"
    );
    assert_eq!(saved.metadata["runtime.local_task.run_id"], run_id);
    assert!(!saved.metadata.contains_key("runtime.local_task.instance"));
    assert!(crate::execution::ChildCompletionSource::from_committed_session(&saved).is_none());
    assert!(execution.task.lock().await.is_none());
    assert!(lock(&execution.stop).is_none());
    assert_eq!(
        harness.runner.admission.available_permits(),
        admission_during
    );
    registration.quarantine().await;
    assert!(harness.runner.shutdown().await.is_err());
}

#[tokio::test]
async fn local_confirmed_stop_without_publication_keeps_admission_after_coordinator_exit() {
    let home = tempfile::tempdir().unwrap();
    let control = ProviderControl::new();
    let harness = Harness::open(home.path(), &root_id(), control.provider.clone(), 1).await;
    let child = harness
        .child("unpublished-stop", "unused accepted setup input")
        .await;
    let run_id = "stopped-unpublished-coordinator";
    let registration = harness
        .router
        .register_run(&child.id, run_id)
        .await
        .unwrap();
    let before = harness.runner.admission.available_permits();
    harness
        .runner
        .prepare_child_execution(&child, run_id, CancellationToken::new())
        .await
        .unwrap();
    let execution = harness.runner.execution(&child, run_id).unwrap();
    let coordinator = {
        let runner = harness.runner.clone();
        let execution = execution.clone();
        let child = child.clone();
        tokio::spawn(async move {
            let stopped =
                checkpoint_unstarted_cancellation(&runner, &execution, child, run_id).await;
            assert!(runner
                .finish_child_execution(&stopped, run_id, false)
                .await
                .is_err());
        })
    };
    assert!(harness
        .runner
        .track_child_task(&child.id, run_id, coordinator)
        .is_none());
    tokio::time::timeout(WAIT, async {
        while !lock(&harness.runner.coordinators)
            .iter()
            .all(|task| task.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        lock(&execution.stop).is_some(),
        "the no-work execution actually drained"
    );
    assert!(!execution.finalized.load(Ordering::Acquire));
    assert_eq!(harness.runner.admission.available_permits(), before - 1);
    let saved = harness
        .store
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        saved.metadata["runtime.local_task.state"], "stopped",
        "failed publication must not erase confirmed physical stop"
    );
    assert!(!saved.metadata.contains_key("runtime.local_task.reason"));
    assert!(crate::execution::ChildCompletionSource::from_committed_session(&saved).is_none());
    registration.quarantine().await;
    harness.runner.shutdown().await.unwrap();
}
