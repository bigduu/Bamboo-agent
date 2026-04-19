//! Comprehensive tests for agent core types
//!
//! Tests cover:
//! - Role enum serialization
//! - Message constructors and methods
//! - Session lifecycle and management
//! - Tool message truncation
//! - Conversation summaries

use bamboo_agent::agent::{Message, Role, Session};
use bamboo_agent_core::agent::types::{CompressionEvent, ConversationSummary, SessionKind};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
use chrono::Utc;

#[test]
fn test_role_serialization() {
    // Test System role
    let role = Role::System;
    let json = serde_json::to_string(&role).unwrap();
    assert_eq!(json, r#""system""#);
    let decoded: Role = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, Role::System);

    // Test User role
    let role = Role::User;
    let json = serde_json::to_string(&role).unwrap();
    assert_eq!(json, r#""user""#);
    let decoded: Role = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, Role::User);

    // Test Assistant role
    let role = Role::Assistant;
    let json = serde_json::to_string(&role).unwrap();
    assert_eq!(json, r#""assistant""#);
    let decoded: Role = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, Role::Assistant);

    // Test Tool role
    let role = Role::Tool;
    let json = serde_json::to_string(&role).unwrap();
    assert_eq!(json, r#""tool""#);
    let decoded: Role = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, Role::Tool);
}

#[test]
fn test_message_user_constructor() {
    let msg = Message::user("Hello, assistant!");

    assert_eq!(msg.role, Role::User);
    assert_eq!(msg.content, "Hello, assistant!");
    assert!(msg.tool_calls.is_none());
    assert!(msg.tool_call_id.is_none());
    assert!(msg.reasoning.is_none());
    assert!(!msg.compressed);
    assert!(!msg.id.is_empty());
}

#[test]
fn test_message_assistant_constructor() {
    let msg = Message::assistant("Hello!", None);

    assert_eq!(msg.role, Role::Assistant);
    assert_eq!(msg.content, "Hello!");
    assert!(msg.tool_calls.is_none());
    assert!(msg.tool_call_id.is_none());
    assert!(!msg.compressed);
}

#[test]
fn test_message_assistant_with_tool_calls() {
    let tool_call = ToolCall {
        id: "call-123".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "test_tool".to_string(),
            arguments: r#"{"arg": "value"}"#.to_string(),
        },
    };

    let msg = Message::assistant("Using tool", Some(vec![tool_call.clone()]));

    assert_eq!(msg.role, Role::Assistant);
    assert_eq!(msg.content, "Using tool");
    assert!(msg.tool_calls.is_some());
    let tool_calls = msg.tool_calls.unwrap();
    assert_eq!(tool_calls.len(), 1);
    assert_eq!(tool_calls[0].id, "call-123");
}

#[test]
fn test_message_assistant_with_reasoning() {
    let reasoning = "Let me think about this...".to_string();
    let msg = Message::assistant_with_reasoning("Response", None, Some(reasoning.clone()));

    assert_eq!(msg.role, Role::Assistant);
    assert_eq!(msg.content, "Response");
    assert_eq!(msg.reasoning, Some(reasoning));
}

#[test]
fn test_message_tool_result_constructor() {
    let msg = Message::tool_result("call-123", "Tool output here");

    assert_eq!(msg.role, Role::Tool);
    assert_eq!(msg.content, "Tool output here");
    assert_eq!(msg.tool_call_id, Some("call-123".to_string()));
    assert!(msg.tool_calls.is_none());
}

#[test]
fn test_message_system_constructor() {
    let msg = Message::system("You are a helpful assistant");

    assert_eq!(msg.role, Role::System);
    assert_eq!(msg.content, "You are a helpful assistant");
    assert!(msg.tool_calls.is_none());
    assert!(msg.tool_call_id.is_none());
}

#[test]
fn test_message_id_generation() {
    let msg1 = Message::user("First");
    let msg2 = Message::user("Second");

    // IDs should be unique
    assert_ne!(msg1.id, msg2.id);
    // IDs should not be empty
    assert!(!msg1.id.is_empty());
    assert!(!msg2.id.is_empty());
}

#[test]
fn test_session_new() {
    let session = Session::new("session-123", "gpt-4o-mini");

    assert_eq!(session.id, "session-123");
    assert_eq!(session.model, "gpt-4o-mini");
    assert_eq!(session.title, "New Session");
    assert!(!session.pinned);
    assert_eq!(session.kind, SessionKind::Root);
    assert!(session.parent_session_id.is_none());
    assert_eq!(session.root_session_id, "session-123");
    assert_eq!(session.spawn_depth, 0);
    assert!(session.messages.is_empty());
    assert!(session.task_list.is_none());
    assert!(session.pending_question.is_none());
    assert!(session.token_budget.is_none());
    assert!(session.token_usage.is_none());
    assert!(session.conversation_summary.is_none());
    assert!(session.compression_events.is_empty());
}

#[test]
fn test_session_new_child() {
    let child = Session::new_child(
        "child-123",
        "root-456",
        "gpt-4o-mini",
        "Child Session Title",
    );

    assert_eq!(child.id, "child-123");
    assert_eq!(child.model, "gpt-4o-mini");
    assert_eq!(child.title, "Child Session Title");
    assert!(!child.pinned);
    assert_eq!(child.kind, SessionKind::Child);
    assert_eq!(child.parent_session_id, Some("root-456".to_string()));
    assert_eq!(child.root_session_id, "root-456");
    assert_eq!(child.spawn_depth, 1);
}

#[test]
fn test_session_add_message() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    let msg = Message::user("Hello");
    session.add_message(msg);

    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].role, Role::User);
    assert_eq!(session.messages[0].content, "Hello");

    // Updated_at should be later than created_at
    assert!(session.updated_at >= session.created_at);
}

#[test]
fn test_session_add_multiple_messages() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    session.add_message(Message::system("System prompt"));
    session.add_message(Message::user("User message"));
    session.add_message(Message::assistant("Assistant response", None));

    assert_eq!(session.messages.len(), 3);
    assert_eq!(session.messages[0].role, Role::System);
    assert_eq!(session.messages[1].role, Role::User);
    assert_eq!(session.messages[2].role, Role::Assistant);
}

#[test]
fn test_session_set_task_list() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    let task_list = TaskList {
        session_id: "session-1".to_string(),
        title: "Task List".to_string(),
        items: vec![TaskItem {
            id: "item-1".to_string(),
            description: "Task 1".to_string(),
            status: TaskItemStatus::Pending,
            depends_on: vec![],
            notes: String::new(),
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    session.set_task_list(task_list);

    assert!(session.task_list.is_some());
    let list = session.task_list.unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].description, "Task 1");
}

#[test]
fn test_session_update_task_item() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    // Set initial task list
    let task_list = TaskList {
        session_id: "session-1".to_string(),
        title: "Task List".to_string(),
        items: vec![TaskItem {
            id: "item-1".to_string(),
            description: "Task 1".to_string(),
            status: TaskItemStatus::Pending,
            depends_on: vec![],
            notes: String::new(),
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    // Update the task item
    let result = session.update_task_item("item-1", TaskItemStatus::Completed, Some("Done!"), None);

    assert!(result.is_ok());
    let list = session.task_list.unwrap();
    assert_eq!(list.items[0].status, TaskItemStatus::Completed);
    assert_eq!(list.items[0].notes, "Done!");
    assert_eq!(list.items[0].transitions.len(), 1);
    assert_eq!(
        list.items[0].transitions[0].from_status,
        TaskItemStatus::Pending
    );
    assert_eq!(
        list.items[0].transitions[0].to_status,
        TaskItemStatus::Completed
    );
    assert_eq!(
        list.items[0].transitions[0].reason.as_deref(),
        Some("Done!")
    );
}

#[test]
fn test_session_update_task_item_same_status_does_not_create_transition() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    let task_list = TaskList {
        session_id: "session-1".to_string(),
        title: "Task List".to_string(),
        items: vec![TaskItem {
            id: "item-1".to_string(),
            description: "Task 1".to_string(),
            status: TaskItemStatus::Pending,
            depends_on: vec![],
            notes: String::new(),
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let result = session.update_task_item(
        "item-1",
        TaskItemStatus::Pending,
        Some("Still pending"),
        None,
    );

    assert!(result.is_ok());
    let list = session.task_list.unwrap();
    assert_eq!(list.items[0].status, TaskItemStatus::Pending);
    assert_eq!(list.items[0].notes, "Still pending");
    assert!(list.items[0].transitions.is_empty());
}

#[test]
fn test_session_update_task_item_enforces_completion_criteria_gate() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    let task_list = TaskList {
        session_id: "session-1".to_string(),
        title: "Task List".to_string(),
        items: vec![TaskItem {
            id: "item-1".to_string(),
            description: "Task with criteria".to_string(),
            status: TaskItemStatus::InProgress,
            depends_on: vec![],
            notes: String::new(),
            completion_criteria: vec!["All tests pass".to_string(), "No lint errors".to_string()],
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let incomplete = vec!["c1".to_string()];
    let result = session.update_task_item(
        "item-1",
        TaskItemStatus::Completed,
        Some("Attempt completion"),
        Some(&incomplete),
    );

    assert!(result.is_ok());
    let list = session.task_list.as_ref().expect("task list should exist");
    assert_eq!(list.items[0].status, TaskItemStatus::InProgress);
    assert!(list.items[0]
        .notes
        .contains("Completion criteria not fully met"));

    let complete = vec!["c1".to_string(), "c2".to_string()];
    let result = session.update_task_item(
        "item-1",
        TaskItemStatus::Completed,
        Some("All done"),
        Some(&complete),
    );

    assert!(result.is_ok());
    let list = session.task_list.as_ref().expect("task list should exist");
    assert_eq!(list.items[0].status, TaskItemStatus::Completed);
}

#[test]
fn test_session_update_task_item_completion_gate_blocks_when_criteria_not_provided() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    let task_list = TaskList {
        session_id: "session-1".to_string(),
        title: "Task List".to_string(),
        items: vec![TaskItem {
            id: "item-1".to_string(),
            description: "Task with criteria".to_string(),
            status: TaskItemStatus::InProgress,
            depends_on: vec![],
            notes: String::new(),
            completion_criteria: vec!["All tests pass".to_string(), "No lint errors".to_string()],
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let result = session.update_task_item("item-1", TaskItemStatus::Completed, Some("Done"), None);
    assert!(result.is_ok());

    let list = session.task_list.as_ref().expect("task list should exist");
    assert_eq!(list.items[0].status, TaskItemStatus::InProgress);
    assert!(list.items[0]
        .notes
        .contains("Completion criteria not fully met"));
}

#[test]
fn test_session_update_task_item_not_found() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    let task_list = TaskList {
        session_id: "session-1".to_string(),
        title: "Task List".to_string(),
        items: vec![],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let result = session.update_task_item("nonexistent", TaskItemStatus::Completed, None, None);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}

#[test]
fn test_session_update_task_item_no_list() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    let result = session.update_task_item("item-1", TaskItemStatus::Completed, None, None);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No task list exists"));
}

#[test]
fn test_session_set_pending_question() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    session.set_pending_question(
        "call-123".to_string(),
        "Which language?".to_string(),
        vec!["Rust".to_string(), "Python".to_string()],
        false,
    );

    assert!(session.has_pending_question());

    let pending = session.pending_question.unwrap();
    assert_eq!(pending.tool_call_id, "call-123");
    assert_eq!(pending.question, "Which language?");
    assert_eq!(pending.options.len(), 2);
    assert!(!pending.allow_custom);
}

#[test]
fn test_session_clear_pending_question() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    session.set_pending_question(
        "call-123".to_string(),
        "Question?".to_string(),
        vec![],
        true,
    );

    assert!(session.has_pending_question());

    session.clear_pending_question();

    assert!(!session.has_pending_question());
    assert!(session.pending_question.is_none());
}

#[test]
fn test_session_has_pending_question() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    assert!(!session.has_pending_question());

    session.set_pending_question(
        "call-123".to_string(),
        "Question?".to_string(),
        vec![],
        true,
    );

    assert!(session.has_pending_question());
}

#[test]
fn test_conversation_summary_new() {
    let summary = ConversationSummary::new("Discussion about Rust", 10, 50);

    assert_eq!(summary.content, "Discussion about Rust");
    assert_eq!(summary.message_count, 10);
    assert_eq!(summary.token_count, 50);
    assert_eq!(summary.created_at, summary.updated_at);
}

#[test]
fn test_conversation_summary_update() {
    let mut summary = ConversationSummary::new("Initial summary", 5, 25);

    std::thread::sleep(std::time::Duration::from_millis(1));

    summary.update("Updated summary", 10, 50);

    assert_eq!(summary.content, "Updated summary");
    assert_eq!(summary.message_count, 10);
    assert_eq!(summary.token_count, 50);
    assert!(summary.updated_at > summary.created_at);
}

#[test]
fn test_compression_event_new() {
    let event = CompressionEvent::new(15, 3, 96.2, 61.4, 128);

    assert!(!event.id.is_empty());
    assert_eq!(event.messages_compressed, 15);
    assert_eq!(event.segments_removed, 3);
    assert!((event.usage_before_percent - 96.2).abs() < f64::EPSILON);
    assert!((event.usage_after_percent - 61.4).abs() < f64::EPSILON);
    assert_eq!(event.summary_tokens, 128);
}

#[test]
fn test_tool_message_truncation_small() {
    let small_content = "Small tool output";
    let msg = Message::tool_result("call-123", small_content);

    // Small messages should not be truncated
    assert_eq!(msg.content, small_content);
}

#[test]
fn test_tool_message_truncation_large() {
    // Create a large tool message (> 256KB)
    let large_content = "x".repeat(300 * 1024);
    let mut session = Session::new("session-1", "gpt-4o-mini");

    session.add_message(Message::tool_result("call-123", large_content.clone()));

    // Tool message should be truncated when added to session
    assert!(session.messages[0].content.len() < large_content.len());
    assert!(session.messages[0].content.contains("truncated"));
}

#[test]
fn test_tool_message_no_truncation_for_other_roles() {
    // Large user message should not be truncated
    let large_content = "x".repeat(300 * 1024);
    let mut session = Session::new("session-1", "gpt-4o-mini");

    session.add_message(Message::user(large_content.clone()));

    // User message should not be truncated
    assert_eq!(session.messages[0].content.len(), large_content.len());
}

#[test]
fn test_session_format_task_list_for_prompt() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    // Without task list
    assert!(session.format_task_list_for_prompt().is_empty());

    // With task list
    let task_list = TaskList {
        session_id: "session-1".to_string(),
        title: "Task List".to_string(),
        items: vec![TaskItem {
            id: "item-1".to_string(),
            description: "Task 1".to_string(),
            status: TaskItemStatus::Pending,
            depends_on: vec![],
            notes: String::new(),
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    session.set_task_list(task_list);

    let formatted = session.format_task_list_for_prompt();
    assert!(!formatted.is_empty());
    assert!(formatted.contains("Task 1"));
}

#[test]
fn test_session_compact_oversized_tool_messages() {
    let mut session = Session::new("session-1", "gpt-4o-mini");

    // Add regular message
    session.add_message(Message::user("User message"));

    // Add large tool message (already truncated when added)
    let large_content = "x".repeat(300 * 1024);
    session.add_message(Message::tool_result("call-123", large_content));

    // Compact should return 0 since message was already truncated on add
    let compacted = session.compact_oversized_tool_messages();
    assert_eq!(compacted, 0);
}

#[test]
fn test_session_serialization() {
    let mut session = Session::new("session-123", "gpt-4o-mini");
    session.add_message(Message::user("Hello"));
    session.add_message(Message::assistant("Hi there!", None));

    // Serialize to JSON
    let json = serde_json::to_string(&session).unwrap();

    // Deserialize back
    let decoded: Session = serde_json::from_str(&json).unwrap();

    assert_eq!(decoded.id, session.id);
    assert_eq!(decoded.model, session.model);
    assert_eq!(decoded.messages.len(), 2);
    assert_eq!(decoded.messages[0].content, "Hello");
    assert_eq!(decoded.messages[1].content, "Hi there!");
}

#[test]
fn test_message_serialization() {
    let msg = Message::user("Test message");

    let json = serde_json::to_string(&msg).unwrap();
    let decoded: Message = serde_json::from_str(&json).unwrap();

    assert_eq!(decoded.role, Role::User);
    assert_eq!(decoded.content, "Test message");
}

#[test]
fn test_empty_session() {
    let session = Session::new("session-1", "gpt-4o-mini");

    assert!(session.messages.is_empty());
    assert!(session.task_list.is_none());
    assert!(session.pending_question.is_none());
    assert!(session.conversation_summary.is_none());
}

#[test]
fn test_session_kind_default() {
    assert_eq!(SessionKind::default(), SessionKind::Root);
}

#[test]
fn test_session_kind_serialization() {
    let root = SessionKind::Root;
    let json = serde_json::to_string(&root).unwrap();
    assert_eq!(json, r#""root""#);

    let child = SessionKind::Child;
    let json = serde_json::to_string(&child).unwrap();
    assert_eq!(json, r#""child""#);
}
