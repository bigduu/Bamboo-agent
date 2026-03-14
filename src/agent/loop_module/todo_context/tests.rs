use crate::agent::core::todo::TodoItemStatus;
use crate::agent::core::todo::{TodoItem, TodoList};
use crate::agent::core::tools::ToolResult;
use chrono::Utc;

use super::TodoLoopContext;

fn create_test_session() -> crate::agent::core::Session {
    let mut session = crate::agent::core::Session::new("test-session", "test-model");
    let todo_list = TodoList {
        session_id: "test-session".to_string(),
        title: "Test Tasks".to_string(),
        items: vec![
            TodoItem {
                id: "task-1".to_string(),
                description: "Read configuration file".to_string(),
                status: TodoItemStatus::Pending,
                depends_on: Vec::new(),
                notes: String::new(),
            },
            TodoItem {
                id: "task-2".to_string(),
                description: "Run tests".to_string(),
                status: TodoItemStatus::Pending,
                depends_on: Vec::new(),
                notes: String::new(),
            },
        ],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_todo_list(todo_list);
    session
}

#[test]
fn from_session_initializes_loop_state() {
    let session = create_test_session();
    let context = TodoLoopContext::from_session(&session).expect("todo context should initialize");

    assert_eq!(context.session_id, "test-session");
    assert_eq!(context.items.len(), 2);
    assert_eq!(context.items[0].id, "task-1");
    assert!(context.items[0].tool_calls.is_empty());
}

#[test]
fn track_tool_execution_appends_record_for_active_item() {
    let session = create_test_session();
    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");

    context.set_active_item("task-1");

    let result = ToolResult {
        success: true,
        result: "OK".to_string(),
        display_preference: None,
    };
    context.track_tool_execution("read_file", &result, 1);

    assert_eq!(context.items[0].tool_calls.len(), 1);
    assert_eq!(context.items[0].tool_calls[0].tool_name, "read_file");
    assert!(context.items[0].tool_calls[0].success);
    assert_eq!(context.version, 2);
}

#[test]
fn set_active_item_marks_previous_completed() {
    let session = create_test_session();
    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");

    context.set_active_item("task-1");
    context.current_round = 3;
    context.set_active_item("task-2");

    assert_eq!(context.active_item_id.as_deref(), Some("task-2"));
    assert_eq!(context.items[0].status, TodoItemStatus::Completed);
    assert_eq!(context.items[0].completed_at_round, Some(3));
    assert_eq!(context.items[1].status, TodoItemStatus::InProgress);
}

#[test]
fn is_all_completed_requires_non_empty_and_all_completed() {
    let session = create_test_session();
    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");

    assert!(!context.is_all_completed());
    context.items[0].status = TodoItemStatus::Completed;
    context.items[1].status = TodoItemStatus::Completed;
    assert!(context.is_all_completed());
}

#[test]
fn format_for_prompt_includes_round_and_items() {
    let session = create_test_session();
    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");
    context.current_round = 2;
    context.max_rounds = 10;

    let prompt = context.format_for_prompt();

    assert!(prompt.contains("Round 3/10"));
    assert!(prompt.contains("task-1"));
    assert!(prompt.contains("task-2"));
}

#[test]
fn auto_match_tool_to_item_uses_keyword_heuristic() {
    let session = create_test_session();
    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");

    context.auto_match_tool_to_item("read_file");

    assert_eq!(context.active_item_id.as_deref(), Some("task-1"));
}

#[test]
fn auto_update_status_marks_completed_after_success_threshold() {
    let session = create_test_session();
    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");

    context.set_active_item("task-1");
    context.current_round = 1;

    let success = ToolResult {
        success: true,
        result: "OK".to_string(),
        display_preference: None,
    };

    context.track_tool_execution("read_file", &success, 1);
    context.auto_update_status("read_file", &success);
    assert_eq!(context.items[0].status, TodoItemStatus::InProgress);

    context.track_tool_execution("read_file", &success, 2);
    context.auto_update_status("read_file", &success);
    assert_eq!(context.items[0].status, TodoItemStatus::InProgress);

    context.track_tool_execution("read_file", &success, 3);
    context.auto_update_status("read_file", &success);

    assert_eq!(context.items[0].status, TodoItemStatus::Completed);
    assert!(context.active_item_id.is_none());
}

#[test]
fn auto_update_status_marks_blocked_after_two_failures() {
    let session = create_test_session();
    let mut context =
        TodoLoopContext::from_session(&session).expect("todo context should initialize");

    context.set_active_item("task-1");
    context.current_round = 1;

    let failure = ToolResult {
        success: false,
        result: "Error".to_string(),
        display_preference: None,
    };

    context.track_tool_execution("read_file", &failure, 1);
    context.auto_update_status("read_file", &failure);
    assert_eq!(context.items[0].status, TodoItemStatus::InProgress);

    context.track_tool_execution("read_file", &failure, 2);
    context.auto_update_status("read_file", &failure);

    assert_eq!(context.items[0].status, TodoItemStatus::Blocked);
}

#[test]
fn into_todo_list_preserves_core_items() {
    let session = create_test_session();
    let context = TodoLoopContext::from_session(&session).expect("todo context should initialize");

    let todo_list = context.into_todo_list();

    assert_eq!(todo_list.session_id, "test-session");
    assert_eq!(todo_list.items.len(), 2);
}
