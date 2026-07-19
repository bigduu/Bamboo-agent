use std::sync::Arc;

use async_trait::async_trait;
use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
use chrono::Utc;
use futures::stream;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{FunctionCall, Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_agent_core::{Message, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use bamboo_tools::BuiltinToolExecutorBuilder;

fn task_list_with_in_progress_item(session_id: &str, description: &str) -> TaskList {
    TaskList {
        session_id: session_id.to_string(),
        title: "Agent Tasks".to_string(),
        items: vec![TaskItem {
            id: "task-1".to_string(),
            description: description.to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

/// Regression test: tool calls executed inside the agent loop MUST receive a ToolExecutionContext
/// with `session_id=Some(...)`. This is required by server-only tools like `spawn_session`.
#[tokio::test]
async fn agent_loop_passes_session_id_into_tool_execution_context() {
    struct QueueProvider {
        // Each `chat_stream` call pops one pre-baked stream.
        queue: Mutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
    }

    #[async_trait]
    impl LLMProvider for QueueProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            let mut guard = self.queue.lock().await;
            if guard.is_empty() {
                panic!("test provider queue exhausted");
            }
            let items = guard.remove(0);
            Ok(Box::pin(stream::iter(items)))
        }
    }

    struct SessionIdRequiredTool {
        seen_session_id: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl Tool for SessionIdRequiredTool {
        fn name(&self) -> &str {
            // Use the exact name we rely on in production.
            "spawn_session"
        }

        fn description(&self) -> &str {
            "test tool that requires session_id in ToolExecutionContext"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({
                "type": "object",
                "properties": {
                    "goal": { "type": "string" }
                },
                "required": ["goal"]
            })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            let Some(session_id) = ctx.session_id else {
                return Err(ToolError::Execution(
                    "missing session_id in tool context".to_string(),
                ));
            };

            *self.seen_session_id.lock().await = Some(session_id.to_string());

            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    let seen_session_id = Arc::new(Mutex::new(None));
    let tools = BuiltinToolExecutorBuilder::new()
        .with_tool(SessionIdRequiredTool {
            seen_session_id: seen_session_id.clone(),
        })
        .expect("register test tool")
        .build();

    let tool_call = bamboo_agent_core::tools::ToolCall {
        id: "call_spawn".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "spawn_session".to_string(),
            arguments: r#"{"goal":"do it"}"#.to_string(),
        },
    };

    let provider = Arc::new(QueueProvider {
        queue: Mutex::new(vec![
            vec![Ok(LLMChunk::ToolCalls(vec![tool_call])), Ok(LLMChunk::Done)],
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)],
        ]),
    });

    let mut session = Session::new("session-ctx-test", "ignored");

    let (event_tx, _event_rx) = mpsc::channel(64);
    let config = AgentLoopConfig {
        max_rounds: 3,
        system_prompt: Some("sys".to_string()),
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    super::run_agent_loop_with_config(
        &mut session,
        "hello".to_string(),
        event_tx,
        provider,
        Arc::new(tools),
        CancellationToken::new(),
        config,
    )
    .await
    .expect("agent loop should succeed");

    assert_eq!(
        seen_session_id.lock().await.clone(),
        Some("session-ctx-test".to_string())
    );
}

#[tokio::test]
async fn agent_loop_uses_refreshed_fast_model_for_between_round_task_evaluation() {
    struct RecordingRoundProvider {
        queue: Mutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
        fast_models: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl LLMProvider for RecordingRoundProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            if model.starts_with("fast-") {
                self.fast_models.lock().await.push(model.to_string());
                return Err(LLMError::Api("intentional fast-model failure".to_string()));
            }

            // When the second chat round starts, the first task evaluator has
            // already been spawned but may not have received executor time yet.
            // Wait for that background request to enter the provider so this
            // test observes the between-round refresh deterministically without
            // relying on the finalize path to drain it.
            if self.queue.lock().await.len() == 2 {
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    while self.fast_models.lock().await.is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("first between-round task evaluation should start");
            }

            let mut guard = self.queue.lock().await;
            if guard.is_empty() {
                panic!("test provider queue exhausted");
            }
            let items = guard.remove(0);
            Ok(Box::pin(stream::iter(items)))
        }
    }

    struct NoopTool;

    #[async_trait]
    impl Tool for NoopTool {
        fn name(&self) -> &str {
            "noop_tool"
        }

        fn description(&self) -> &str {
            "no-op tool for round boundary testing"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    let tools = BuiltinToolExecutorBuilder::new()
        .with_tool(NoopTool)
        .expect("register test tool")
        .with_command_tool("Task")
        .expect("register Task tool")
        .build();

    // Task evaluation fires only on Task-tool writes. Two writes also exercise
    // coalescing while the first auxiliary request is in flight; normal
    // finalization intentionally cancels the queued final-round snapshot (#593).
    let tool_call = |id: &str| bamboo_agent_core::tools::ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Task".to_string(),
            arguments: serde_json::json!({
                "tasks": [{
                    "content": "Verify auxiliary refresh",
                    "status": "in_progress",
                    "activeForm": "Verifying auxiliary refresh"
                }]
            })
            .to_string(),
        },
    };

    let provider = Arc::new(RecordingRoundProvider {
        queue: Mutex::new(vec![
            vec![
                Ok(LLMChunk::ToolCalls(vec![tool_call("call-1")])),
                Ok(LLMChunk::Done),
            ],
            vec![
                Ok(LLMChunk::ToolCalls(vec![tool_call("call-2")])),
                Ok(LLMChunk::Done),
            ],
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)],
        ]),
        fast_models: Mutex::new(Vec::new()),
    });

    let mut session = Session::new("session-fast-refresh", "sticky-chat-model");
    session.set_task_list(task_list_with_in_progress_item(
        &session.id,
        "Verify auxiliary refresh",
    ));

    let fast_counter = Arc::new(std::sync::Mutex::new(0usize));
    let fast_counter_for_resolver = fast_counter.clone();

    let (event_tx, _event_rx) = mpsc::channel(64);
    let config = AgentLoopConfig {
        max_rounds: 5,
        system_prompt: Some("sys".to_string()),
        model_name: Some("sticky-chat-model".to_string()),
        auxiliary_model_resolver: Some(Arc::new(move || {
            let mut guard = fast_counter_for_resolver.lock().expect("fast counter lock");
            *guard += 1;
            crate::runtime::config::AuxiliaryModelConfig {
                fast_model_name: Some(format!("fast-{}", *guard)),
                ..Default::default()
            }
        })),
        ..Default::default()
    };

    super::run_agent_loop_with_config(
        &mut session,
        "hello".to_string(),
        event_tx,
        provider.clone(),
        Arc::new(tools),
        CancellationToken::new(),
        config,
    )
    .await
    .expect("agent loop should succeed");

    let fast_models = provider.fast_models.lock().await.clone();
    // `fast-1` was resolved at startup; the between-round refresh must select
    // `fast-2` for the first evaluation. Depending on scheduling, that request
    // may finish before the next round polls it, allowing the second write to
    // launch with `fast-3`; otherwise it remains queued and is cancelled at
    // finalization. Both are valid, and neither requires a finalize-time drain.
    assert!(
        matches!(
            fast_models.as_slice(),
            [first] if first == "fast-2"
        ) || matches!(
            fast_models.as_slice(),
            [first, second] if first == "fast-2" && second == "fast-3"
        ),
        "between-round evaluations must use refreshed fast models in order, got {fast_models:?}"
    );
}

#[tokio::test]
async fn cancelled_pipeline_releases_activation_without_false_completion() {
    struct NeverCalledProvider;

    #[async_trait]
    impl LLMProvider for NeverCalledProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("pre-cancelled pipeline must not call the provider")
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let skill_root = directory.path().join("skills/cancel-demo");
    tokio::fs::create_dir_all(&skill_root)
        .await
        .expect("skill root");
    tokio::fs::write(
        skill_root.join("SKILL.md"),
        "---\nname: cancel-demo\ndescription: cancel\n---\ncancel\n",
    )
    .await
    .expect("skill");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    manager
        .store()
        .pin_current_activation("cancelled-activation", &["cancel-demo".to_string()], None)
        .await
        .expect("pin activation");
    let mut session = Session::new("cancelled-activation", "model");
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "auto".to_string(),
    );
    let config = AgentLoopConfig {
        skill_manager: Some(manager.clone()),
        model_name: Some("model".to_string()),
        ..Default::default()
    };
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let result = super::run_agent_loop_with_config(
        &mut session,
        "cancel".to_string(),
        event_tx,
        Arc::new(NeverCalledProvider),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        cancel,
        config,
    )
    .await;

    assert!(matches!(
        result,
        Err(bamboo_agent_core::AgentError::Cancelled)
    ));
    assert!(manager
        .store()
        .activation_descriptor("cancelled-activation")
        .await
        .is_none());
    assert!(!session.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Completed)
    }));
    while let Ok(event) = event_rx.try_recv() {
        assert!(
            !matches!(event, bamboo_agent_core::AgentEvent::Complete { .. }),
            "cancelled pipeline must not emit Complete"
        );
    }
}

#[tokio::test]
async fn suspended_restart_without_retained_snapshot_continues_degraded() {
    struct CalledProvider {
        called: std::sync::atomic::AtomicBool,
        messages: std::sync::Mutex<Vec<Message>>,
    }

    #[async_trait]
    impl LLMProvider for CalledProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            self.messages
                .lock()
                .expect("provider messages lock")
                .extend_from_slice(messages);
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("continued without workflow".to_string())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let skill_root = directory.path().join("skills/restart-demo");
    tokio::fs::create_dir_all(&skill_root)
        .await
        .expect("skill root");
    tokio::fs::write(
        skill_root.join("SKILL.md"),
        "---\nname: restart-demo\ndescription: restart\n---\nrestart N+1\n",
    )
    .await
    .expect("skill");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");

    let mut session = Session::new("restart-missing-pin", "model");
    let mut suspended = bamboo_domain::AgentRuntimeState::new("restart-missing-pin");
    suspended.status = bamboo_domain::AgentStatusState::Suspended;
    session.agent_runtime_state = Some(suspended);
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
        "7".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "explicit".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
        r#"{"restart-demo":3}"#.to_string(),
    );
    let config = AgentLoopConfig {
        skill_manager: Some(manager.clone()),
        selected_skill_ids: Some(vec!["restart-demo".to_string()]),
        model_name: Some("model".to_string()),
        ..Default::default()
    };
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let provider = Arc::new(CalledProvider {
        called: std::sync::atomic::AtomicBool::new(false),
        messages: std::sync::Mutex::new(Vec::new()),
    });
    let result = super::run_agent_loop_with_config(
        &mut session,
        "continue".to_string(),
        event_tx,
        provider.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        CancellationToken::new(),
        config,
    )
    .await;

    result.expect("missing LKG degrades workflow without killing the main session");
    assert!(provider.called.load(std::sync::atomic::Ordering::SeqCst));
    let (has_degraded_diagnostic, has_workflow_runtime) = {
        let provider_messages = provider.messages.lock().expect("provider messages lock");
        (
            provider_messages
                .iter()
                .any(|message| message.content.contains("Workflow Activation Degraded")),
            provider_messages
                .iter()
                .any(|message| message.content.contains("context_type: workflow_runtime")),
        )
    };
    assert!(has_degraded_diagnostic);
    assert!(!has_workflow_runtime);
    assert!(session
        .metadata
        .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY)
        .is_some_and(|message| message.contains("snapshot")));
    assert!(manager
        .store()
        .activation_descriptor("restart-missing-pin")
        .await
        .is_none());
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
    while let Ok(event) = event_rx.try_recv() {
        assert!(!matches!(
            event,
            bamboo_agent_core::AgentEvent::WorkflowActivated { .. }
        ));
    }
}

#[tokio::test]
async fn suspended_restart_with_empty_selection_does_not_require_snapshot() {
    struct CalledProvider(std::sync::atomic::AtomicBool);

    #[async_trait]
    impl LLMProvider for CalledProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("continued".to_string())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("restart-empty-selection", "model");
    let mut suspended = bamboo_domain::AgentRuntimeState::new("restart-empty-selection");
    suspended.status = bamboo_domain::AgentStatusState::Suspended;
    session.agent_runtime_state = Some(suspended);
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
        "7".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
        "{}".to_string(),
    );
    let provider = Arc::new(CalledProvider(std::sync::atomic::AtomicBool::new(false)));
    let (event_tx, _event_rx) = mpsc::channel(8);
    super::run_agent_loop_with_config(
        &mut session,
        "continue".to_string(),
        event_tx,
        provider.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager),
            selected_skill_ids: Some(Vec::new()),
            model_name: Some("model".to_string()),
            max_rounds: 1,
            ..Default::default()
        },
    )
    .await
    .expect("empty selection continuation");
    assert!(provider.0.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn activation_metadata_persistence_failure_releases_pin_before_provider_call() {
    struct FailingPersistence;
    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for FailingPersistence {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            Err(std::io::Error::other("injected persistence failure"))
        }
    }
    struct NeverCalledProvider;
    #[async_trait]
    impl LLMProvider for NeverCalledProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("persistence failure must stop before provider execution")
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("persistence-failure", "model");
    let (event_tx, _event_rx) = mpsc::channel(4);
    let result = super::run_agent_loop_with_config(
        &mut session,
        "review".to_string(),
        event_tx,
        Arc::new(NeverCalledProvider),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["review".to_string()]),
            persistence: Some(Arc::new(FailingPersistence)),
            model_name: Some("model".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert!(result
        .expect_err("setup must fail closed")
        .to_string()
        .contains("could not be published before tool/model execution"));
    assert!(manager
        .store()
        .activation_descriptor("persistence-failure")
        .await
        .is_none());
}

#[tokio::test]
async fn post_preload_persistence_failure_stops_before_provider_and_releases_pin() {
    struct FailSecondSave(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for FailSecondSave {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            let call = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call == 0 {
                Ok(())
            } else {
                Err(std::io::Error::other("second save failed"))
            }
        }
    }
    struct SuccessfulLoad;
    #[async_trait]
    impl bamboo_agent_core::tools::ToolExecutor for SuccessfulLoad {
        async fn execute(
            &self,
            _call: &bamboo_agent_core::tools::ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            Ok(ToolResult {
                success: true,
                result: serde_json::json!({
                    "skill_id": "review",
                    "revision": 1,
                    "instructions": "pinned instructions",
                    "dynamic_context": [],
                    "activation_status": "active"
                })
                .to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }
        fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
            vec![bamboo_agent_core::tools::ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionSchema {
                    name: "load_skill".to_string(),
                    description: "load".to_string(),
                    parameters: serde_json::json!({"type":"object"}),
                },
            }]
        }
    }
    struct NeverCalled;
    #[async_trait]
    impl LLMProvider for NeverCalled {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("second persistence failure must stop before provider")
        }
    }
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("post-preload-save-failure", "model");
    let save_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (event_tx, _event_rx) = mpsc::channel(4);
    let result = super::run_agent_loop_with_config(
        &mut session,
        "review".to_string(),
        event_tx,
        Arc::new(NeverCalled),
        Arc::new(SuccessfulLoad),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["review".to_string()]),
            persistence: Some(Arc::new(FailSecondSave(save_count.clone()))),
            model_name: Some("model".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert!(result
        .expect_err("post preload save must fail")
        .to_string()
        .contains("loaded-state could not be published"));
    assert_eq!(save_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(manager
        .store()
        .activation_descriptor("post-preload-save-failure")
        .await
        .is_none());
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
}

#[tokio::test]
async fn unsuccessful_explicit_preload_stops_before_provider_and_releases_pin() {
    struct UnsuccessfulLoad;
    #[async_trait]
    impl bamboo_agent_core::tools::ToolExecutor for UnsuccessfulLoad {
        async fn execute(
            &self,
            _call: &bamboo_agent_core::tools::ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            Ok(ToolResult {
                success: false,
                result: "injected preload failure".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }
        fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
            vec![bamboo_agent_core::tools::ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionSchema {
                    name: "load_skill".to_string(),
                    description: "load".to_string(),
                    parameters: serde_json::json!({"type":"object"}),
                },
            }]
        }
    }
    struct NeverCalled;
    #[async_trait]
    impl LLMProvider for NeverCalled {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("unsuccessful explicit preload must stop before provider")
        }
    }
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("unsuccessful-preload", "model");
    let (event_tx, _event_rx) = mpsc::channel(4);
    let result = super::run_agent_loop_with_config(
        &mut session,
        "review".to_string(),
        event_tx,
        Arc::new(NeverCalled),
        Arc::new(UnsuccessfulLoad),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["review".to_string()]),
            model_name: Some("model".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert!(result
        .expect_err("preload must fail closed")
        .to_string()
        .contains("preload was unsuccessful"));
    assert!(manager
        .store()
        .activation_descriptor("unsuccessful-preload")
        .await
        .is_none());
}
