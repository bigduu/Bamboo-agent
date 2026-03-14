use tokio::sync::mpsc;

use super::maybe_handle_todowrite;
use crate::agent::core::tools::{FunctionCall, ToolCall, ToolResult};
use crate::agent::core::{AgentEvent, Session, TodoItemStatus};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;

#[tokio::test]
async fn maybe_handle_todowrite_updates_session_and_context() {
    let tool_call = ToolCall {
        id: "todo-call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "TodoWrite".to_string(),
            arguments: serde_json::json!({
                "todos": [{
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
    let mut todo_context: Option<TodoLoopContext> = None;
    let (tx, mut rx) = mpsc::channel(4);

    maybe_handle_todowrite(
        &tool_call,
        &result,
        &mut session,
        "session-1",
        &tx,
        &AgentLoopConfig::default(),
        &mut todo_context,
    )
    .await;

    let todo_list = session.todo_list.as_ref().expect("todo list should be set");
    assert_eq!(todo_list.items.len(), 1);
    assert_eq!(todo_list.items[0].status, TodoItemStatus::InProgress);
    assert!(todo_context.is_some());

    let event = rx.recv().await.expect("todo update event");
    match event {
        AgentEvent::TodoListUpdated { todo_list } => {
            assert_eq!(todo_list.items.len(), 1);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn maybe_handle_todowrite_ignores_non_todowrite_calls() {
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
    let mut todo_context: Option<TodoLoopContext> = None;
    let (tx, mut rx) = mpsc::channel(4);

    maybe_handle_todowrite(
        &tool_call,
        &result,
        &mut session,
        "session-1",
        &tx,
        &AgentLoopConfig::default(),
        &mut todo_context,
    )
    .await;

    assert!(session.todo_list.is_none());
    assert!(todo_context.is_none());
    assert!(rx.try_recv().is_err());
}
