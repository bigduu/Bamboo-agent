use std::sync::Arc;

use async_trait::async_trait;
use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
use chrono::Utc;
use futures::stream;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{FunctionCall, Tool, ToolError, ToolExecutionContext, ToolResult};
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

        async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
            // This tool is expected to be executed via `execute_with_context`.
            Err(ToolError::Execution(
                "spawn_session test tool must be executed with context".to_string(),
            ))
        }

        async fn execute_with_context(
            &self,
            _args: serde_json::Value,
            ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            let Some(session_id) = ctx.session_id else {
                return Err(ToolError::Execution(
                    "missing session_id in tool context".to_string(),
                ));
            };

            *self.seen_session_id.lock().await = Some(session_id.to_string());

            Ok(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
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
async fn agent_loop_refreshes_fast_model_between_rounds_for_task_evaluation() {
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

        async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }
    }

    let tools = BuiltinToolExecutorBuilder::new()
        .with_tool(NoopTool)
        .expect("register test tool")
        .build();

    let tool_call = |id: &str| bamboo_agent_core::tools::ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "noop_tool".to_string(),
            arguments: "{}".to_string(),
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
    assert_eq!(
        fast_models,
        vec!["fast-2".to_string(), "fast-3".to_string()]
    );
}
