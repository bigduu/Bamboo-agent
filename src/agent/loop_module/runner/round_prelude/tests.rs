use chrono::Utc;
use tokio_util::sync::CancellationToken;

use super::prepare_round;
use crate::agent::core::{AgentError, Message, Role, Session};
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
            ..TaskItem::default()
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
async fn prepare_round_refreshes_prompt_metadata_for_round_sections() {
    let mut session = Session::new("session-round-metadata", "test-model");
    session.add_message(Message::system("Base prompt"));
    session.set_task_list(sample_task_list(
        "session-round-metadata",
        TaskItemStatus::InProgress,
    ));
    let mut task_context = TaskLoopContext::from_session(&session);
    let config = AgentLoopConfig::default();
    let tools = BuiltinToolExecutor::new();

    let _round_id = prepare_round(
        &mut session,
        &mut task_context,
        0,
        5,
        &CancellationToken::new(),
        None,
        "session-round-metadata",
        "test-model",
        false,
        &config,
        &tools,
    )
    .await
    .expect("round should prepare");

    let flags = session
        .metadata
        .get("runtime_prompt_component_flags")
        .map(String::as_str)
        .unwrap_or_default();
    let lengths = session
        .metadata
        .get("runtime_prompt_component_lengths")
        .map(String::as_str)
        .unwrap_or_default();
    let layout = session
        .metadata
        .get("runtime_prompt_section_layout")
        .map(String::as_str)
        .unwrap_or_default();

    assert!(flags.contains("workspace=0"));
    assert!(flags.contains("external_memory="));
    assert!(flags.contains("task_list=1"));
    assert!(lengths.contains("external_memory="));
    assert!(lengths.contains("task_list="));
    assert!(lengths.contains("final="));
    assert!(layout.contains("round_base_prompt:core_static:static:1:"));
    assert!(layout.contains("task_list:environment_workspace:dynamic:1:"));
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
