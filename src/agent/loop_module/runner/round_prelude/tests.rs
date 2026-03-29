use chrono::Utc;
use tokio_util::sync::CancellationToken;

use super::prepare_round;
use crate::agent::core::{AgentError, Role, Session};
use crate::agent::core::{TaskItem, TaskItemStatus, TaskList};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::tools::BuiltinToolExecutor;

fn sample_task_list(session_id: &str, status: TaskItemStatus) -> TaskList {
    TaskList {
        session_id: session_id.to_string(),
        title: "Tasks".to_string(),
        items: vec![TaskItem {
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
async fn prepare_round_updates_task_context_and_returns_round_id() {
    let mut session = Session::new("session-prelude", "test-model");
    session.set_task_list(sample_task_list("session-prelude", TaskItemStatus::Pending));
    let mut task_context = TaskLoopContext::from_session(&session);
    let config = AgentLoopConfig::default();
    let tools = BuiltinToolExecutor::new();

    let round_id = prepare_round(
        &mut session,
        &mut task_context,
        2,
        7,
        &CancellationToken::new(),
        None,
        "session-prelude",
        "test-model",
        false,
        &config,
        &tools,
    )
    .await
    .expect("round should prepare");

    assert_eq!(round_id, "session-prelude-round-3");
    let ctx = task_context.expect("task context should exist");
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
    let mut task_context = None;
    let cancel_token = CancellationToken::new();
    let config = AgentLoopConfig::default();
    let tools = BuiltinToolExecutor::new();
    cancel_token.cancel();

    let result = prepare_round(
        &mut session,
        &mut task_context,
        0,
        5,
        &cancel_token,
        None,
        "session-cancelled",
        "test-model",
        false,
        &config,
        &tools,
    )
    .await;

    assert!(matches!(result, Err(AgentError::Cancelled)));
}
