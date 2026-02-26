use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::composition::CompositionExecutor;
use crate::agent::core::tools::executor::execute_tool_call_with_context;
use crate::agent::core::tools::{
    convert_from_standard_result, AgenticToolResult, ToolCall, ToolError, ToolExecutionContext,
    ToolExecutor, ToolResult,
};
use crate::agent::core::{AgentEvent, Message, Session};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolHandlingOutcome {
    Continue,
    AwaitingClarification,
}

pub const MAX_SUB_ACTIONS: usize = 64;

pub fn parse_tool_args(arguments: &str) -> std::result::Result<serde_json::Value, ToolError> {
    let args_raw = arguments.trim();

    if args_raw.is_empty() {
        return Ok(serde_json::json!({}));
    }

    serde_json::from_str(args_raw)
        .map_err(|error| ToolError::InvalidArguments(format!("Invalid JSON arguments: {error}")))
}

pub fn try_parse_agentic_result(result: &ToolResult) -> Option<AgenticToolResult> {
    if result.result.trim_start().starts_with('{') {
        if let Ok(parsed) = serde_json::from_str::<AgenticToolResult>(&result.result) {
            return Some(parsed);
        }
    }

    match result.display_preference.as_deref() {
        Some("clarification") | Some("actions_needed") => {
            Some(convert_from_standard_result(result.clone()))
        }
        _ => None,
    }
}

pub async fn handle_tool_result_with_agentic_support(
    result: &ToolResult,
    tool_call: &ToolCall,
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut Session,
    tools: &dyn ToolExecutor,
    composition_executor: Option<Arc<CompositionExecutor>>,
) -> ToolHandlingOutcome {
    let Some(agentic_result) = try_parse_agentic_result(result) else {
        session.add_message(Message::tool_result(
            tool_call.id.clone(),
            result.result.clone(),
        ));
        return ToolHandlingOutcome::Continue;
    };

    match agentic_result {
        AgenticToolResult::Success { result } => {
            session.add_message(Message::tool_result(tool_call.id.clone(), result));
            ToolHandlingOutcome::Continue
        }
        AgenticToolResult::Error { error } => {
            let _ = event_tx
                .send(AgentEvent::ToolError {
                    tool_call_id: tool_call.id.clone(),
                    error: error.clone(),
                })
                .await;

            session.add_message(Message::tool_result(
                tool_call.id.clone(),
                format!("Error: {error}"),
            ));

            ToolHandlingOutcome::Continue
        }
        AgenticToolResult::NeedClarification { question, options } => {
            send_clarification_request(event_tx, question.clone(), options).await;

            session.add_message(Message::tool_result(
                tool_call.id.clone(),
                format!("Clarification needed: {question}"),
            ));

            ToolHandlingOutcome::AwaitingClarification
        }
        AgenticToolResult::NeedMoreActions { actions, reason } => {
            session.add_message(Message::tool_result(
                tool_call.id.clone(),
                format!(
                    "Need more actions: {reason} ({} actions pending)",
                    actions.len()
                ),
            ));

            execute_sub_actions(&actions, event_tx, session, tools, composition_executor).await
        }
    }
}

pub async fn send_clarification_request(
    event_tx: &mpsc::Sender<AgentEvent>,
    question: String,
    options: Option<Vec<String>>,
) {
    let _ = event_tx
        .send(AgentEvent::NeedClarification { question, options })
        .await;
}

pub async fn execute_sub_actions(
    actions: &[ToolCall],
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut Session,
    tools: &dyn ToolExecutor,
    composition_executor: Option<Arc<CompositionExecutor>>,
) -> ToolHandlingOutcome {
    let mut pending: VecDeque<ToolCall> = actions.iter().cloned().collect();
    let mut processed = 0usize;

    while let Some(action) = pending.pop_front() {
        if processed >= MAX_SUB_ACTIONS {
            let error = format!("Reached max sub-action limit ({MAX_SUB_ACTIONS})");
            let _ = event_tx
                .send(AgentEvent::ToolError {
                    tool_call_id: action.id.clone(),
                    error: error.clone(),
                })
                .await;
            session.add_message(Message::tool_result(action.id.clone(), error));
            return ToolHandlingOutcome::Continue;
        }

        processed += 1;

        let args =
            parse_tool_args(&action.function.arguments).unwrap_or_else(|_| serde_json::json!({}));

        let _ = event_tx
            .send(AgentEvent::ToolStart {
                tool_call_id: action.id.clone(),
                tool_name: action.function.name.clone(),
                arguments: args,
            })
            .await;

        let tool_ctx = ToolExecutionContext {
            session_id: Some(&session.id),
            tool_call_id: &action.id,
            event_tx: Some(event_tx),
        };

        match execute_tool_call_with_context(&action, tools, composition_executor.clone(), tool_ctx)
            .await
        {
            Ok(result) => {
                let _ = event_tx
                    .send(AgentEvent::ToolComplete {
                        tool_call_id: action.id.clone(),
                        result: result.clone(),
                    })
                    .await;

                match try_parse_agentic_result(&result) {
                    Some(AgenticToolResult::Success { result }) => {
                        session.add_message(Message::tool_result(action.id.clone(), result));
                    }
                    Some(AgenticToolResult::Error { error }) => {
                        let _ = event_tx
                            .send(AgentEvent::ToolError {
                                tool_call_id: action.id.clone(),
                                error: error.clone(),
                            })
                            .await;
                        session.add_message(Message::tool_result(
                            action.id.clone(),
                            format!("Error: {error}"),
                        ));
                    }
                    Some(AgenticToolResult::NeedClarification { question, options }) => {
                        send_clarification_request(event_tx, question.clone(), options).await;
                        session.add_message(Message::tool_result(
                            action.id.clone(),
                            format!("Clarification needed: {question}"),
                        ));
                        return ToolHandlingOutcome::AwaitingClarification;
                    }
                    Some(AgenticToolResult::NeedMoreActions {
                        actions: next_actions,
                        reason,
                    }) => {
                        session.add_message(Message::tool_result(
                            action.id.clone(),
                            format!(
                                "Need more actions: {reason} ({} actions pending)",
                                next_actions.len()
                            ),
                        ));
                        pending.extend(next_actions);
                    }
                    None => {
                        session.add_message(Message::tool_result(
                            action.id.clone(),
                            result.result.clone(),
                        ));
                    }
                }
            }
            Err(error) => {
                let error_msg = error.to_string();
                let _ = event_tx
                    .send(AgentEvent::ToolError {
                        tool_call_id: action.id.clone(),
                        error: error_msg.clone(),
                    })
                    .await;
                session.add_message(Message::tool_result(
                    action.id.clone(),
                    format!("Error: {error_msg}"),
                ));
            }
        }
    }

    ToolHandlingOutcome::Continue
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    use crate::agent::core::tools::{FunctionCall, ToolSchema};

    use super::*;

    struct StaticExecutor {
        results: HashMap<String, ToolResult>,
    }

    impl StaticExecutor {
        fn new(results: HashMap<String, ToolResult>) -> Self {
            Self { results }
        }
    }

    #[async_trait]
    impl ToolExecutor for StaticExecutor {
        async fn execute(
            &self,
            call: &ToolCall,
        ) -> crate::agent::core::tools::executor::Result<ToolResult> {
            self.results
                .get(&call.function.name)
                .cloned()
                .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

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

    #[tokio::test]
    async fn need_clarification_sends_event() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let tools: Arc<dyn ToolExecutor> = Arc::new(StaticExecutor::new(HashMap::new()));
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
        let sub_action = make_tool_call("call_sub", "sub_tool", "{}");
        let parent_call = make_tool_call("call_parent", "smart_tool", "{}");

        let mut results = HashMap::new();
        results.insert(
            "sub_tool".to_string(),
            ToolResult {
                success: true,
                result: "sub-action-done".to_string(),
                display_preference: None,
            },
        );
        let tools: Arc<dyn ToolExecutor> = Arc::new(StaticExecutor::new(results));
        let mut session = Session::new("s2", "test-model");

        let result = ToolResult {
            success: true,
            result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
                actions: vec![sub_action],
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
                    && message.content == "sub-action-done"
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

    #[test]
    fn parse_tool_args_rejects_invalid_json() {
        let error = parse_tool_args("not-json").expect_err("invalid json should fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }
}
