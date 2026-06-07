
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{broadcast, RwLock};

    use serde_json::json;
    use uuid::Uuid;

    use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
    use bamboo_domain::session::runtime_state::ChildWaitPolicy;
    use bamboo_engine::session_app::child_session;

    use crate::app_state::{AgentRunner, AgentStatus};
    use crate::tools::{ChildSessionAdapter, SubAgentTool};
    use bamboo_engine::execution::spawn::{SpawnContext, SpawnScheduler};
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
    use bamboo_agent_core::{AgentEvent, Message, Role, Session};
    use bamboo_metrics::storage::SqliteMetricsStorage;
    use bamboo_metrics::collector::MetricsCollector;
    use bamboo_skills::SkillManager;
    use bamboo_storage::SessionStoreV2;
    use bamboo_llm::{LLMError, LLMProvider, LLMStream};

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
    }

    async fn build_test_harness() -> TestHarness {
        build_test_harness_with_resolver(None).await
    }

    async fn build_test_harness_with_resolver(
        subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    ) -> TestHarness {
        let bamboo_home = make_temp_dir("bamboo-sub-agent-test");
        tokio::fs::create_dir_all(&bamboo_home).await.unwrap();

        let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
        let storage_dir = bamboo_home.join("storage");
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        let jsonl = bamboo_storage::JsonlStorage::new(&storage_dir);
        jsonl.init().await.unwrap();
        let storage: Arc<dyn Storage> = Arc::new(jsonl);

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

        let agent_runtime = Arc::new(
            bamboo_engine::Agent::builder()
                .storage(storage.clone())
                .persistence(Arc::new(bamboo_storage::LockedSessionStore::new(
                    storage.clone(),
                )))
                .attachment_reader(session_store.clone())
                .skill_manager(Arc::new(SkillManager::new()))
                .metrics_collector(metrics_collector)
                .config(Arc::new(RwLock::new(
                    bamboo_llm::Config::default(),
                )))
                .provider(Arc::new(NoopProvider))
                .default_tools(Arc::new(NoopToolExecutor))
                .build()
                .expect("test agent should be fully configured"),
        );

        let scheduler = Arc::new(SpawnScheduler::new(SpawnContext {
            agent: agent_runtime,
            tools: Arc::new(NoopToolExecutor),
            sessions_cache: sessions_cache.clone(),
            agent_runners: agent_runners.clone(),
            session_event_senders: session_event_senders.clone(),
            external_child_runner: None,
            provider_router: None,
            app_data_dir: None,
            completion_handler: None,
            account_feed_inbox: None,
        }));

        let test_profiles = std::sync::Arc::new(
            bamboo_domain::subagent::SubagentProfileRegistry::builder()
                .extend(bamboo_engine::profiles::builtin::builtin_profiles())
                .build()
                .expect("builtin subagent profiles must build"),
        );
        let adapter = Arc::new(ChildSessionAdapter {
            session_store,
            storage: storage.clone(),
            persistence: Arc::new(bamboo_storage::LockedSessionStore::new(
                storage.clone(),
            )),
            scheduler,
            sessions_cache,
            agent_runners: agent_runners.clone(),
            session_event_senders,
            subagent_model_resolver,
            config: Arc::new(RwLock::new(bamboo_llm::Config::default())),
            subagent_profiles: test_profiles.clone(),
            tool_names: Vec::new(),
            parent_wait_slots: Arc::new(dashmap::DashMap::new()),
        });
        let tool = SubAgentTool::new(adapter.clone(), adapter.clone(), test_profiles);

        TestHarness {
            tool,
            adapter,
            storage,
            agent_runners,
            parent_session_id,
            child_session_id,
            parent_rx,
        }
    }

    // -----------------------------------------------------------------------
    // ④ Batched parent-wait registration
    // -----------------------------------------------------------------------

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
        assert_eq!(got, child_ids, "all children must be registered exactly once");
        assert_eq!(
            parent.metadata.get("runtime.suspend_reason").map(String::as_str),
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

    // -----------------------------------------------------------------------
    // Decoupled create + explicit SubAgent.wait
    // -----------------------------------------------------------------------

    fn ctx_for<'a>(session_id: &'a str, tool_call_id: &'static str) -> ToolExecutionContext<'a> {
        ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
        }
    }

    #[tokio::test]
    async fn create_without_subagent_type_defaults_to_general_purpose() {
        let harness = build_test_harness().await;
        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "No Role Child",
                    "responsibility": "Do work",
                    "prompt": "Do the work",
                    "workspace": "/tmp/ws"
                    // subagent_type intentionally omitted
                }),
                ctx_for(&harness.parent_session_id, "tc_no_role"),
            )
            .await
            .expect("create must succeed without subagent_type");

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["subagent_type"].as_str(), Some("general-purpose"));
    }

    #[tokio::test]
    async fn create_with_wait_true_suspends_and_registers_wait() {
        let harness = build_test_harness().await;
        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Blocking Child",
                    "responsibility": "Do one thing",
                    "prompt": "Do it",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/ws",
                    "wait": true
                }),
                ctx_for(&harness.parent_session_id, "tc_create_wait"),
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
        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "wait",
                    "child_session_ids": ["k1", "k2", "k3"],
                    "wait_for": "any"
                }),
                ctx_for(&harness.parent_session_id, "tc_wait"),
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
        let result = harness
            .tool
            .execute_with_context(
                json!({ "action": "wait" }),
                ctx_for(&harness.parent_session_id, "tc_wait_noop"),
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

        let err = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "demo task",
                    "responsibility": "do something",
                    "prompt": "do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext::none("tool_call"),
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Child A",
                    "responsibility": "Investigate one module",
                    "prompt": "Read module and summarize",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Coder Child",
                    "responsibility": "Implement a focused change",
                    "prompt": "Patch one file",
                    "subagent_type": "coder",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_async_resolver",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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
    async fn backward_compat_legacy_subagent_call_without_action_defaults_to_create() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "title": "Legacy Child",
                    "responsibility": "Test backward compat",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_legacy",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let result = harness
            .tool
            .execute_with_context(
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
                },
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
    }

    #[tokio::test]
    async fn send_message_queues_on_running_child_without_interrupt() {
        let harness = build_test_harness().await;
        {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            runners.insert(harness.child_session_id.clone(), runner);
        }

        let result = harness
            .tool
            .execute_with_context(
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
                },
            )
            .await
            .expect("send_message should queue message on running child");

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "message_queued");
        assert_eq!(payload["message"], "continue");

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .expect("child session should exist");
        // Message is NOT appended to messages array while child is running;
        // it is stored in metadata for the agent loop to merge at turn boundaries.
        assert_eq!(child.messages.len(), 3);
        let pending_raw = child
            .metadata
            .get("pending_injected_messages")
            .expect("pending_injected_messages should be set");
        let pending: Vec<child_session::QueuedInjectedMessage> =
            serde_json::from_str(pending_raw).expect("should parse queued messages");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].content, "continue");
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

        let result = harness
            .tool
            .execute_with_context(
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
                },
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
    }

    #[tokio::test]
    async fn send_message_can_queue_child_immediately() {
        let mut harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
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
                },
            )
            .await
            .expect("send_message should queue the child");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "queued");
        assert_eq!(payload["auto_run"], true);

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
    }

    #[tokio::test]
    async fn cancel_stops_running_child() {
        let harness = build_test_harness().await;
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "cancel",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_cancel",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list"}),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_list",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "get",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_get_diagnostics",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Test Child",
                    "responsibility": "Do something",
                    "prompt": "Do something useful",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_hint",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Reasoning Child",
                    "responsibility": "Investigate hard problem",
                    "prompt": "Think carefully step by step",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false,
                    "reasoning_effort": "high"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_with_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Default Child",
                    "responsibility": "Quick lookup",
                    "prompt": "Read a file and summarise",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_default_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

        let _ = harness
            .tool
            .execute_with_context(
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
                },
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

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "delete",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_delete",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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

    /// `action=list_profiles` returns every built-in profile (without
    /// the `system_prompt` body), reports the registry's fallback id,
    /// and uses the registry's stable insertion order. The shape of
    /// this payload is a public contract — UI / LLM rely on it.
    #[tokio::test]
    async fn list_profiles_returns_builtin_catalog() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list_profiles"}),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_list_profiles",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list_profiles should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");

        // Top-level shape.
        let profiles = payload["profiles"]
            .as_array()
            .expect("list_profiles must return a `profiles` array");
        assert!(
            profiles.len() >= 6,
            "expected at least 6 built-in profiles, got {}",
            profiles.len()
        );
        assert_eq!(payload["count"], profiles.len());
        assert_eq!(payload["fallback_id"], "general-purpose");

        // Required fields per profile, and explicit guarantee that we
        // do NOT leak `system_prompt` (could be very large).
        for entry in profiles {
            assert!(entry.get("id").and_then(|v| v.as_str()).is_some());
            assert!(entry.get("display_name").and_then(|v| v.as_str()).is_some());
            assert!(entry.get("tools").is_some());
            assert!(
                entry.get("system_prompt").is_none(),
                "system_prompt must NOT be returned by list_profiles",
            );
        }

        // Built-in catalogue must include the documented baseline ids
        // so the LLM can rely on them being present.
        let ids: Vec<&str> = profiles
            .iter()
            .map(|p| p["id"].as_str().unwrap_or(""))
            .collect();
        for required in [
            "general-purpose",
            "plan",
            "researcher",
            "coder",
            "reviewer",
            "tester",
        ] {
            assert!(
                ids.contains(&required),
                "built-in profile `{required}` missing from list_profiles output (got: {ids:?})"
            );
        }
    }

    /// `list_profiles` is read-only and must not require a real,
    /// loadable parent session. We pass a non-existent session_id and
    /// expect success (registry is consulted directly, no session
    /// lookup is performed).
    #[tokio::test]
    async fn list_profiles_does_not_load_parent_session() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list_profiles"}),
                ToolExecutionContext {
                    session_id: Some("non-existent-session-id"),
                    tool_call_id: "tool_call_list_profiles_no_session",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list_profiles should succeed even when the parent session id is unknown");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert!(payload["profiles"].as_array().is_some());
    }

    #[tokio::test]
    async fn create_requires_workspace() {
        let harness = build_test_harness().await;

        let err = harness
            .tool
            .execute_with_context(
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
                },
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
    async fn create_sets_child_workspace() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Workspace Child",
                    "responsibility": "Test workspace propagation",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_workspace",
                    event_tx: None,
                    available_tool_schemas: None,
                },
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
            Some("/tmp/test-workspace".to_string()),
            "child workspace should be set from create args"
        );
    }

    // -----------------------------------------------------------------------
    // S-T5.2 — End-to-end policy enforcement for a read-only allowlist child.
    //
    // The `researcher` builtin profile is a read-only Allowlist. A researcher
    // child must have mutating tools (Edit/Write) enforced by BOTH halves of
    // the tool-policy machinery (TD-7):
    //   1. Discovery (schema): the profile's `disabled_tools` — computed by
    //      `disabled_tools_for_profile` over the real builtin tool surface —
    //      contains Edit and Write, so they are absent from the advertised
    //      schema for the child.
    //   2. Execution (safety net): `PolicyAwareToolExecutor` rejects an Edit /
    //      Write call from a `subagent_type=researcher` child at execute time,
    //      while still permitting allowlisted tools (Read).
    //
    // This pins the anti-fork invariant: the same canonical profile drives both
    // layers; there is no forked policy definition.
    // -----------------------------------------------------------------------

    /// A recording executor used to prove the runtime safety net forwards vs
    /// blocks calls. Forwards always succeed; blocked calls never reach it.
    struct PolicyRecordingExecutor {
        executed: Arc<RwLock<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for PolicyRecordingExecutor {
        async fn execute(
            &self,
            call: &ToolCall,
        ) -> std::result::Result<
            bamboo_agent_core::tools::ToolResult,
            bamboo_agent_core::tools::ToolError,
        > {
            self.executed.write().await.push(call.function.name.clone());
            Ok(bamboo_agent_core::tools::ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }

        async fn execute_with_context(
            &self,
            call: &ToolCall,
            _ctx: bamboo_agent_core::tools::ToolExecutionContext<'_>,
        ) -> std::result::Result<
            bamboo_agent_core::tools::ToolResult,
            bamboo_agent_core::tools::ToolError,
        > {
            self.executed.write().await.push(call.function.name.clone());
            Ok(bamboo_agent_core::tools::ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    fn researcher_builtin_profile() -> bamboo_domain::subagent::SubagentProfile {
        bamboo_engine::profiles::builtin::builtin_profiles()
            .into_iter()
            .find(|p| p.id == "researcher")
            .expect("researcher builtin profile must exist")
    }

    fn make_tool_call(name: &str) -> ToolCall {
        ToolCall {
            id: "policy_call".to_string(),
            tool_type: "function".to_string(),
            function: bamboo_agent_core::tools::FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    #[test]
    fn researcher_disabled_tools_block_edit_and_write_in_schema() {
        // Layer 1 (discovery): the schema-level disabled set excludes mutating
        // tools for the read-only researcher allowlist.
        let researcher = researcher_builtin_profile();
        let all_tool_names: Vec<String> = bamboo_domain::tool_names::BUILTIN_TOOL_NAMES
            .iter()
            .map(|s| s.to_string())
            .collect();

        let disabled =
            bamboo_domain::subagent::disabled_tools_for_profile(&researcher.tools, &all_tool_names);

        assert!(
            disabled.iter().any(|t| t == "Edit"),
            "researcher schema must disable Edit; disabled={disabled:?}"
        );
        assert!(
            disabled.iter().any(|t| t == "Write"),
            "researcher schema must disable Write; disabled={disabled:?}"
        );
        // Sanity: an allowlisted read-only tool stays enabled.
        assert!(
            !disabled.iter().any(|t| t == "Read"),
            "researcher schema must keep Read enabled; disabled={disabled:?}"
        );
    }

    #[tokio::test]
    async fn researcher_child_blocks_edit_and_write_at_execute() {
        // Layer 2 (execution safety net): a researcher child has Edit / Write
        // rejected at execute time by PolicyAwareToolExecutor, while Read is
        // still forwarded. Uses the canonical builtin registry + the new
        // `bamboo_tools` path via the server's re-export shim.
        let executed = Arc::new(RwLock::new(Vec::<String>::new()));
        let inner: Arc<dyn ToolExecutor> = Arc::new(PolicyRecordingExecutor {
            executed: executed.clone(),
        });

        let registry = Arc::new(
            bamboo_domain::subagent::SubagentProfileRegistry::builder()
                .extend(bamboo_engine::profiles::builtin::builtin_profiles())
                .build()
                .expect("builtin subagent profiles must build"),
        );

        let mut child =
            Session::new_child("researcher-child", "root", "test-model", "Research child");
        child
            .metadata
            .insert("subagent_type".to_string(), "researcher".to_string());
        let sessions: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());
        sessions.insert(
            "researcher-child".to_string(),
            Arc::new(parking_lot::RwLock::new(child)),
        );

        let executor = crate::tools::PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = bamboo_agent_core::tools::ToolExecutionContext {
            session_id: Some("researcher-child"),
            tool_call_id: "policy_call",
            event_tx: None,
            available_tool_schemas: None,
        };

        // Edit blocked at execute.
        let edit_err = executor
            .execute_with_context(&make_tool_call("Edit"), ctx)
            .await
            .expect_err("Edit must be blocked for a researcher child");
        match edit_err {
            bamboo_agent_core::tools::ToolError::Execution(msg) => {
                assert!(msg.contains("Edit"), "error should name the tool: {msg}");
                assert!(
                    msg.contains("researcher"),
                    "error should name the subagent_type: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }

        // Write blocked at execute.
        let write_err = executor
            .execute_with_context(&make_tool_call("Write"), ctx)
            .await
            .expect_err("Write must be blocked for a researcher child");
        assert!(
            matches!(
                write_err,
                bamboo_agent_core::tools::ToolError::Execution(ref msg) if msg.contains("Write")
            ),
            "Write must be rejected with an Execution error naming the tool"
        );

        // Allowlisted Read still forwarded to the inner executor.
        executor
            .execute_with_context(&make_tool_call("Read"), ctx)
            .await
            .expect("Read is allowlisted and must be forwarded");

        // Only Read reached the inner executor; Edit/Write were stopped above it.
        assert_eq!(
            executed.read().await.as_slice(),
            &["Read".to_string()],
            "only the allowlisted Read should reach the inner executor"
        );
    }
