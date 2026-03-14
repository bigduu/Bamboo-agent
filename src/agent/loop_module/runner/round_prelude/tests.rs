use chrono::Utc;
use tokio_util::sync::CancellationToken;

use super::prepare_round;
use crate::agent::core::todo::{TodoItem, TodoItemStatus, TodoList};
use crate::agent::core::{AgentError, Role, Session};
use crate::agent::loop_module::todo_context::TodoLoopContext;

fn sample_todo_list(session_id: &str, status: TodoItemStatus) -> TodoList {
    TodoList {
        session_id: session_id.to_string(),
        title: "Tasks".to_string(),
        items: vec![TodoItem {
            id: "item-1".to_string(),
            description: "Test item".to_string(),
            status,
            depends_on: Vec::new(),
            notes: String::new(),
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn prepare_round_updates_todo_context_and_returns_round_id() {
    let mut session = Session::new("session-prelude", "test-model");
    session.set_todo_list(sample_todo_list("session-prelude", TodoItemStatus::Pending));
    let mut todo_context = TodoLoopContext::from_session(&session);

    let round_id = prepare_round(
        &mut session,
        &mut todo_context,
        2,
        7,
        &CancellationToken::new(),
        None,
        "session-prelude",
        "test-model",
        false,
    )
    .await
    .expect("round should prepare");

    assert_eq!(round_id, "session-prelude-round-3");
    let ctx = todo_context.expect("todo context should exist");
    assert_eq!(ctx.current_round, 2);
    assert_eq!(ctx.max_rounds, 7);
    assert!(session
        .messages
        .iter()
        .any(|msg| matches!(msg.role, Role::System) && msg.content.contains("Current Task List")));
}

#[tokio::test]
async fn prepare_round_returns_cancelled_error_when_token_cancelled() {
    let mut session = Session::new("session-cancelled", "test-model");
    let mut todo_context = None;
    let cancel_token = CancellationToken::new();
    cancel_token.cancel();

    let result = prepare_round(
        &mut session,
        &mut todo_context,
        0,
        5,
        &cancel_token,
        None,
        "session-cancelled",
        "test-model",
        false,
    )
    .await;

    assert!(matches!(result, Err(AgentError::Cancelled)));
}
