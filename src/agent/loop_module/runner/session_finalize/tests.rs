use chrono::Utc;
use tokio::sync::mpsc;

use super::finalize_session;
use crate::agent::core::todo::{TodoItem, TodoItemStatus, TodoList};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;

fn completed_todo_list(session_id: &str) -> TodoList {
    TodoList {
        session_id: session_id.to_string(),
        title: "Done".to_string(),
        items: vec![TodoItem {
            id: "done-1".to_string(),
            description: "Completed task".to_string(),
            status: TodoItemStatus::Completed,
            depends_on: Vec::new(),
            notes: String::new(),
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn finalize_session_emits_complete_when_not_sent_yet() {
    let mut session = Session::new("finalize-session-1", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel(8);

    finalize_session(
        None,
        &mut session,
        &event_tx,
        "finalize-session-1",
        &AgentLoopConfig::default(),
        None,
        false,
    )
    .await;

    let event = event_rx.recv().await.expect("complete event expected");
    match event {
        AgentEvent::Complete { usage } => {
            assert_eq!(usage.prompt_tokens, 0);
            assert_eq!(usage.completion_tokens, 0);
            assert_eq!(usage.total_tokens, 0);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn finalize_session_skips_complete_when_already_sent() {
    let mut session = Session::new("finalize-session-2", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel(8);

    finalize_session(
        None,
        &mut session,
        &event_tx,
        "finalize-session-2",
        &AgentLoopConfig::default(),
        None,
        true,
    )
    .await;

    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn finalize_session_syncs_todo_context_and_emits_todo_completed() {
    let mut session = Session::new("finalize-session-3", "test-model");
    session.set_todo_list(completed_todo_list("finalize-session-3"));
    let mut todo_context =
        TodoLoopContext::from_session(&session).expect("todo context should exist");
    todo_context.current_round = 3;

    let (event_tx, mut event_rx) = mpsc::channel(8);

    finalize_session(
        Some(todo_context),
        &mut session,
        &event_tx,
        "finalize-session-3",
        &AgentLoopConfig::default(),
        None,
        true,
    )
    .await;

    let event = event_rx
        .recv()
        .await
        .expect("todo completed event expected");
    match event {
        AgentEvent::TodoListCompleted {
            session_id,
            total_rounds,
            ..
        } => {
            assert_eq!(session_id, "finalize-session-3");
            assert_eq!(total_rounds, 4);
        }
        other => panic!("unexpected event: {other:?}"),
    }

    assert!(session.todo_list.is_some());
    assert_eq!(
        session
            .metadata
            .get("todo_list_version")
            .map(String::as_str),
        Some("0")
    );
    assert!(event_rx.try_recv().is_err());
}
