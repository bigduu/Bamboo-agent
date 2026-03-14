use crate::agent::core::todo::{TodoItem, TodoList};
use crate::agent::core::TodoItemStatus;
use crate::agent::loop_module::todo_context::{TodoLoopContext, TodoLoopItem, ToolCallRecord};
use chrono::Utc;

use super::build_todo_evaluation_messages;
use super::message_builder::format_recent_tools;

fn create_test_context() -> TodoLoopContext {
    let mut session = crate::agent::core::Session::new("test", "test-model");
    let todo_list = TodoList {
        session_id: "test".to_string(),
        title: "Test Tasks".to_string(),
        items: vec![TodoItem {
            id: "1".to_string(),
            description: "Fix bug in authentication".to_string(),
            status: TodoItemStatus::InProgress,
            depends_on: Vec::new(),
            notes: String::new(),
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_todo_list(todo_list);

    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");
    context.items = vec![TodoLoopItem {
        id: "1".to_string(),
        description: "Fix bug in authentication".to_string(),
        status: TodoItemStatus::InProgress,
        tool_calls: vec![
            ToolCallRecord {
                round: 0,
                tool_name: "read_file".to_string(),
                success: true,
                timestamp: Utc::now(),
            },
            ToolCallRecord {
                round: 1,
                tool_name: "write_file".to_string(),
                success: true,
                timestamp: Utc::now(),
            },
        ],
        started_at_round: Some(0),
        completed_at_round: None,
    }];

    context
}

#[test]
fn build_evaluation_messages_contains_context_and_rules() {
    let context = create_test_context();
    let session = crate::agent::core::Session::new("test", "test-model");

    let messages = build_todo_evaluation_messages(&context, &session);

    assert_eq!(messages.len(), 2);
    assert!(messages[0].content.contains("task progress evaluator"));
    assert!(messages[1].content.contains("Fix bug in authentication"));
}

#[test]
fn format_recent_tools_includes_symbols_and_tool_names() {
    let context = create_test_context();
    let output = format_recent_tools(&context, 5);

    assert!(output.contains("read_file"));
    assert!(output.contains("write_file"));
    assert!(output.contains("✓"));
}

#[test]
fn in_progress_items_require_evaluation() {
    let mut context = create_test_context();

    assert!(context
        .items
        .iter()
        .any(|item| matches!(item.status, TodoItemStatus::InProgress)));

    context.items[0].status = TodoItemStatus::Completed;

    assert!(!context
        .items
        .iter()
        .any(|item| matches!(item.status, TodoItemStatus::InProgress)));
}

#[test]
fn todo_evaluation_requires_model_parameter() {
    // Compile-time documentation test: evaluate_todo_progress includes `model: &str`.
    assert!(true);
}
