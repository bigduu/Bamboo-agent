use crate::agent::core::tools::ToolResult;
use crate::agent::core::TaskItemStatus;
use crate::agent::core::{TaskItem, TaskList};
use chrono::Utc;

use super::TaskLoopContext;

fn create_test_session() -> crate::agent::core::Session {
    let mut session = crate::agent::core::Session::new("test-session", "test-model");
    let task_list = TaskList {
        session_id: "test-session".to_string(),
        title: "Test Tasks".to_string(),
        items: vec![
            TaskItem {
                id: "task-1".to_string(),
                description: "Read configuration file".to_string(),
                status: TaskItemStatus::Pending,
                depends_on: Vec::new(),
                notes: String::new(),
            },
            TaskItem {
                id: "task-2".to_string(),
                description: "Run tests".to_string(),
                status: TaskItemStatus::Pending,
                depends_on: Vec::new(),
                notes: String::new(),
            },
        ],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);
    session
}

#[test]
fn from_session_initializes_loop_state() {
    let session = create_test_session();
    let context = TaskLoopContext::from_session(&session).expect("task context should initialize");

    assert_eq!(context.session_id, "test-session");
    assert_eq!(context.items.len(), 2);
    assert_eq!(context.items[0].id, "task-1");
    assert!(context.items[0].tool_calls.is_empty());
}

#[test]
fn track_tool_execution_appends_record_for_active_item() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

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
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    context.set_active_item("task-1");
    context.current_round = 3;
    context.set_active_item("task-2");

    assert_eq!(context.active_item_id.as_deref(), Some("task-2"));
    assert_eq!(context.items[0].status, TaskItemStatus::Completed);
    assert_eq!(context.items[0].completed_at_round, Some(3));
    assert_eq!(context.items[1].status, TaskItemStatus::InProgress);
}

#[test]
fn is_all_completed_requires_non_empty_and_all_completed() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    assert!(!context.is_all_completed());
    context.items[0].status = TaskItemStatus::Completed;
    context.items[1].status = TaskItemStatus::Completed;
    assert!(context.is_all_completed());
}

#[test]
fn format_for_prompt_includes_round_and_items() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");
    context.current_round = 2;
    context.max_rounds = 10;

    let prompt = context.format_for_prompt();

    assert!(prompt.contains("Round 3/10"));
    assert!(prompt.contains("task-1"));
    assert!(prompt.contains("task-2"));
}

#[test]
fn format_for_prompt_returns_empty_for_no_items() {
    let session = crate::agent::core::Session::new("test-session", "test-model");
    let context = TaskLoopContext::from_session(&session);

    // Session without task list should return None or empty context
    if let Some(ctx) = context {
        assert!(ctx.items.is_empty());
        assert!(ctx.format_for_prompt().is_empty());
    }
}

#[test]
fn format_for_prompt_shows_correct_status_icons() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    // Test Pending status
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("[ ] task-1"));
    assert!(prompt.contains("[ ] task-2"));

    // Test InProgress status
    context.items[0].status = TaskItemStatus::InProgress;
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("[/] task-1"));
    assert!(prompt.contains("[ ] task-2"));

    // Test Completed status
    context.items[0].status = TaskItemStatus::Completed;
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("[x] task-1"));
    assert!(prompt.contains("[ ] task-2"));

    // Test Blocked status
    context.items[1].status = TaskItemStatus::Blocked;
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("[x] task-1"));
    assert!(prompt.contains("[!] task-2"));
}

#[test]
fn format_for_prompt_shows_tool_call_count() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    context.set_active_item("task-1");

    let result = ToolResult {
        success: true,
        result: "OK".to_string(),
        display_preference: None,
    };

    context.track_tool_execution("read_file", &result, 1);
    context.track_tool_execution("write_file", &result, 2);

    let prompt = context.format_for_prompt();

    assert!(prompt.contains("(2 tool calls)"));
    assert!(prompt.contains("[/] task-1: Read configuration file (2 tool calls)"));
}

#[test]
fn format_for_prompt_shows_progress_for_various_completion_states() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    // 0/2 completed
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("Progress: 0/2 tasks completed"));

    // 1/2 completed
    context.items[0].status = TaskItemStatus::Completed;
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("Progress: 1/2 tasks completed"));

    // 2/2 completed
    context.items[1].status = TaskItemStatus::Completed;
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("Progress: 2/2 tasks completed"));
}

#[test]
fn format_for_prompt_includes_task_descriptions() {
    let session = create_test_session();
    let context = TaskLoopContext::from_session(&session).expect("task context should initialize");

    let prompt = context.format_for_prompt();

    assert!(prompt.contains("Read configuration file"));
    assert!(prompt.contains("Run tests"));
}

#[test]
fn format_for_prompt_with_single_item() {
    let mut session = crate::agent::core::Session::new("test-session", "test-model");
    let task_list = TaskList {
        session_id: "test-session".to_string(),
        title: "Single Task".to_string(),
        items: vec![TaskItem {
            id: "only-task".to_string(),
            description: "Single task test".to_string(),
            status: TaskItemStatus::InProgress,
            depends_on: Vec::new(),
            notes: String::new(),
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");
    context.current_round = 0;
    context.max_rounds = 5;

    let prompt = context.format_for_prompt();

    assert!(prompt.contains("Round 1/5"));
    assert!(prompt.contains("[/] only-task: Single task test"));
    assert!(prompt.contains("Progress: 0/1 tasks completed"));
}

#[test]
fn format_for_prompt_with_many_items() {
    let mut session = crate::agent::core::Session::new("test-session", "test-model");
    let items: Vec<TaskItem> = (1..=5)
        .map(|i| TaskItem {
            id: format!("task-{}", i),
            description: format!("Task number {}", i),
            status: if i <= 3 {
                TaskItemStatus::Completed
            } else {
                TaskItemStatus::Pending
            },
            depends_on: Vec::new(),
            notes: String::new(),
        })
        .collect();

    let task_list = TaskList {
        session_id: "test-session".to_string(),
        title: "Many Tasks".to_string(),
        items,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let context = TaskLoopContext::from_session(&session).expect("task context should initialize");

    let prompt = context.format_for_prompt();

    assert!(prompt.contains("[x] task-1"));
    assert!(prompt.contains("[x] task-2"));
    assert!(prompt.contains("[x] task-3"));
    assert!(prompt.contains("[ ] task-4"));
    assert!(prompt.contains("[ ] task-5"));
    assert!(prompt.contains("Progress: 3/5 tasks completed"));
}

#[test]
fn format_for_prompt_round_display_is_one_indexed() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    // Round 0 should display as "Round 1/10"
    context.current_round = 0;
    context.max_rounds = 10;
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("Round 1/10"));

    // Round 4 should display as "Round 5/10"
    context.current_round = 4;
    let prompt = context.format_for_prompt();
    assert!(prompt.contains("Round 5/10"));
}

#[test]
fn format_for_prompt_mixed_statuses_with_tool_calls() {
    let mut session = crate::agent::core::Session::new("test-session", "test-model");
    let task_list = TaskList {
        session_id: "test-session".to_string(),
        title: "Mixed Tasks".to_string(),
        items: vec![
            TaskItem {
                id: "completed-task".to_string(),
                description: "Already done".to_string(),
                status: TaskItemStatus::Completed,
                depends_on: Vec::new(),
                notes: String::new(),
            },
            TaskItem {
                id: "active-task".to_string(),
                description: "Working on it".to_string(),
                status: TaskItemStatus::InProgress,
                depends_on: Vec::new(),
                notes: String::new(),
            },
            TaskItem {
                id: "blocked-task".to_string(),
                description: "Cannot proceed".to_string(),
                status: TaskItemStatus::Blocked,
                depends_on: Vec::new(),
                notes: String::new(),
            },
        ],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");
    context.set_active_item("active-task");

    let result = ToolResult {
        success: true,
        result: "OK".to_string(),
        display_preference: None,
    };
    context.track_tool_execution("test_tool", &result, 1);

    let prompt = context.format_for_prompt();

    assert!(prompt.contains("[x] completed-task: Already done"));
    assert!(prompt.contains("[/] active-task: Working on it (1 tool calls)"));
    assert!(prompt.contains("[!] blocked-task: Cannot proceed"));
    assert!(prompt.contains("Progress: 1/3 tasks completed"));
}

#[test]
fn auto_match_tool_to_item_uses_keyword_heuristic() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    context.auto_match_tool_to_item("read_file");

    assert_eq!(context.active_item_id.as_deref(), Some("task-1"));
}

#[test]
fn auto_update_status_marks_completed_after_success_threshold() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    context.set_active_item("task-1");
    context.current_round = 1;

    let success = ToolResult {
        success: true,
        result: "OK".to_string(),
        display_preference: None,
    };

    context.track_tool_execution("read_file", &success, 1);
    context.auto_update_status("read_file", &success);
    assert_eq!(context.items[0].status, TaskItemStatus::InProgress);

    context.track_tool_execution("read_file", &success, 2);
    context.auto_update_status("read_file", &success);
    assert_eq!(context.items[0].status, TaskItemStatus::InProgress);

    context.track_tool_execution("read_file", &success, 3);
    context.auto_update_status("read_file", &success);

    assert_eq!(context.items[0].status, TaskItemStatus::Completed);
    assert!(context.active_item_id.is_none());
}

#[test]
fn auto_update_status_marks_blocked_after_two_failures() {
    let session = create_test_session();
    let mut context =
        TaskLoopContext::from_session(&session).expect("task context should initialize");

    context.set_active_item("task-1");
    context.current_round = 1;

    let failure = ToolResult {
        success: false,
        result: "Error".to_string(),
        display_preference: None,
    };

    context.track_tool_execution("read_file", &failure, 1);
    context.auto_update_status("read_file", &failure);
    assert_eq!(context.items[0].status, TaskItemStatus::InProgress);

    context.track_tool_execution("read_file", &failure, 2);
    context.auto_update_status("read_file", &failure);

    assert_eq!(context.items[0].status, TaskItemStatus::Blocked);
}

#[test]
fn into_task_list_preserves_core_items() {
    let session = create_test_session();
    let context = TaskLoopContext::from_session(&session).expect("task context should initialize");

    let task_list = context.into_task_list();

    assert_eq!(task_list.session_id, "test-session");
    assert_eq!(task_list.items.len(), 2);
}
