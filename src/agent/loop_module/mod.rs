pub mod config;
pub mod runner;
pub mod stream;
pub mod todo_context;
pub mod todo_evaluation;

pub use config::AgentLoopConfig;
pub use runner::{run_agent_loop, run_agent_loop_with_config};
pub use todo_context::TodoLoopContext;
pub use todo_evaluation::{evaluate_todo_progress, TodoEvaluationResult};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agent_core::composition::CompositionExecutor;
    use agent_core::tools::{
        execute_tool_call, handle_tool_result_with_agentic_support, AgenticToolResult,
        FunctionCall, ToolCall, ToolExecutor, ToolHandlingOutcome, ToolRegistry, ToolResult,
    };
    use agent_core::{AgentEvent, Session};
    use agent_tools::BuiltinToolExecutor;
    use tokio::sync::mpsc;

    use crate::config::AgentLoopConfig;

    fn make_tool_call(id: &str, name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn agent_loop_config_default() {
        let config = AgentLoopConfig::default();
        assert_eq!(config.max_rounds, 50);
        assert!(config.system_prompt.is_none());
        assert!(config.additional_tool_schemas.is_empty());
        assert!(config.tool_registry.is_empty());
        assert!(config.composition_executor.is_none());
        assert!(config.skill_manager.is_none());
        assert!(!config.skip_initial_user_message);
    }

    #[test]
    fn skip_initial_message_flag() {
        let config = AgentLoopConfig {
            skip_initial_user_message: true,
            ..Default::default()
        };
        assert!(config.skip_initial_user_message);
    }

    #[tokio::test]
    async fn need_clarification_sends_event() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
        let mut session = Session::new("s1", "test-model");
        let tool_call = make_tool_call("call_parent", "smart_tool", "{}");
        let result = ToolResult {
            success: true,
            result: serde_json::to_string(&AgenticToolResult::NeedClarification {
                question: "Which file should I inspect?".to_string(),
                options: Some(vec!["src/main.rs".to_string(), "src/lib.rs".to_string()]),
            })
            .unwrap(),
            display_preference: None,
        };

        let outcome = handle_tool_result_with_agentic_support(
            &result,
            &tool_call,
            &event_tx,
            &mut session,
            tools.as_ref(),
            None,
        )
        .await;

        assert_eq!(outcome, ToolHandlingOutcome::AwaitingClarification);

        let event = event_rx.recv().await.expect("missing clarification event");
        match event {
            AgentEvent::NeedClarification { question, options } => {
                assert_eq!(question, "Which file should I inspect?");
                assert_eq!(
                    options,
                    Some(vec!["src/main.rs".to_string(), "src/lib.rs".to_string()])
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn need_more_actions_executes_sub_actions() {
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
        let mut session = Session::new("s2", "test-model");
        let sub_action = make_tool_call("call_sub", "get_current_dir", "{}");
        let parent_call = make_tool_call("call_parent", "smart_tool", "{}");
        let result = ToolResult {
            success: true,
            result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
                actions: vec![sub_action.clone()],
                reason: "Need workspace context".to_string(),
            })
            .unwrap(),
            display_preference: None,
        };

        let outcome = handle_tool_result_with_agentic_support(
            &result,
            &parent_call,
            &event_tx,
            &mut session,
            tools.as_ref(),
            None,
        )
        .await;

        assert_eq!(outcome, ToolHandlingOutcome::Continue);
        assert!(session
            .messages
            .iter()
            .any(
                |message| message.tool_call_id.as_deref() == Some("call_sub")
                    && !message.content.is_empty()
            ));

        let mut saw_sub_start = false;
        let mut saw_sub_complete = false;

        while let Ok(event) = event_rx.try_recv() {
            match event {
                AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == "call_sub" => {
                    saw_sub_start = true;
                }
                AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == "call_sub" => {
                    saw_sub_complete = true;
                }
                _ => {}
            }
        }

        assert!(saw_sub_start);
        assert!(saw_sub_complete);
    }

    #[tokio::test]
    async fn execute_tool_call_falls_back_when_composition_misses_tool() {
        let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
        let composition_executor =
            Arc::new(CompositionExecutor::new(Arc::new(ToolRegistry::new())));
        let tool_call = make_tool_call("call_sub", "get_current_dir", "{}");

        let result = execute_tool_call(&tool_call, tools.as_ref(), Some(composition_executor))
            .await
            .expect("fallback execution should succeed");

        assert!(result.success);
        assert!(!result.result.is_empty());
    }
}
