use tokio::sync::mpsc;

use super::maybe_handle_taskwrite;
use bamboo_application_agent::tools::{FunctionCall, ToolCall, ToolResult};
use bamboo_application_agent::AgentEvent;
use bamboo_application_agent::Session;
use bamboo_domain_session::TaskItemStatus;
use crate::config::AgentLoopConfig;
use crate::task_context::TaskLoopContext;

#[tokio::test]
async fn maybe_handle_taskwrite_handles_task_tool_updates_session_and_context() {
    let tool_call = ToolCall {
        id: "task-call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Task".to_string(),
            arguments: serde_json::json!({
                "tasks": [{
                    "content": "Refactor module",
                    "status": "in_progress",
                    "activeForm": "Refactoring module"
                }]
            })
            .to_string(),
        },
    };
    let result = ToolResult {
        success: true,
        result: "ok".to_string(),
        display_preference: None,
    };

    let mut session = Session::new("session-1", "model");
    let mut task_context: Option<TaskLoopContext> = None;
    let (tx, mut rx) = mpsc::channel(4);

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut session,
        "session-1",
        &tx,
        &AgentLoopConfig::default(),
        &mut task_context,
    )
    .await;

    let task_list = session.task_list.as_ref().expect("task list should be set");
    assert_eq!(task_list.items.len(), 1);
    assert_eq!(task_list.items[0].status, TaskItemStatus::InProgress);
    assert!(task_context.is_some());

    let event = rx.recv().await.expect("task update event");
    match event {
        AgentEvent::TaskListUpdated { task_list } => {
            assert_eq!(task_list.items.len(), 1);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn maybe_handle_taskwrite_ignores_non_task_calls() {
    let tool_call = ToolCall {
        id: "read-call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Read".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let result = ToolResult {
        success: true,
        result: "ok".to_string(),
        display_preference: None,
    };

    let mut session = Session::new("session-1", "model");
    let mut task_context: Option<TaskLoopContext> = None;
    let (tx, mut rx) = mpsc::channel(4);

    maybe_handle_taskwrite(
        &tool_call,
        &result,
        &mut session,
        "session-1",
        &tx,
        &AgentLoopConfig::default(),
        &mut task_context,
    )
    .await;

    assert!(session.task_list.is_none());
    assert!(task_context.is_none());
    assert!(rx.try_recv().is_err());
}
