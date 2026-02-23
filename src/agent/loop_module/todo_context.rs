//! TodoList context for Agent Loop integration
//!
//! This module provides TodoLoopContext which integrates TodoList
//! as a first-class citizen in the Agent Loop, similar to Token Budget.

use agent_core::todo::{TodoItem, TodoItemStatus, TodoList};
use agent_core::tools::ToolResult;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// TodoList context for Agent Loop
///
/// Acts as a first-class citizen in the agent loop, tracking
/// task progress throughout the entire conversation lifecycle.
#[derive(Debug, Clone)]
pub struct TodoLoopContext {
    /// Session ID
    pub session_id: String,

    /// Todo items with execution tracking
    pub items: Vec<TodoLoopItem>,

    /// Currently active todo item ID
    pub active_item_id: Option<String>,

    /// Current round number
    pub current_round: u32,

    /// Maximum rounds allowed
    pub max_rounds: u32,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Version number for conflict detection
    pub version: u64,
}

/// Todo item with execution tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoLoopItem {
    /// Item ID
    pub id: String,

    /// Item description
    pub description: String,

    /// Item status
    pub status: TodoItemStatus,

    /// Tool call history (tracks execution process)
    pub tool_calls: Vec<ToolCallRecord>,

    /// Round when item was started
    pub started_at_round: Option<u32>,

    /// Round when item was completed
    pub completed_at_round: Option<u32>,
}

/// Record of a tool call execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRecord {
    /// Round number
    pub round: u32,

    /// Tool name
    pub tool_name: String,

    /// Whether the call succeeded
    pub success: bool,

    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

impl TodoLoopContext {
    /// Create TodoLoopContext from Session's TodoList
    pub fn from_session(session: &agent_core::Session) -> Option<Self> {
        session.todo_list.as_ref().map(|todo_list| {
            // Preserve version from existing todo_list metadata if available
            // This prevents version reset across multiple executions
            let existing_version = session.metadata.get("todo_list_version")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);

            Self {
                session_id: todo_list.session_id.clone(),
                items: todo_list
                    .items
                    .iter()
                    .map(|item| TodoLoopItem {
                        id: item.id.clone(),
                        description: item.description.clone(),
                        status: item.status.clone(),
                        tool_calls: Vec::new(),
                        started_at_round: None,
                        completed_at_round: None,
                    })
                    .collect(),
                active_item_id: None,
                current_round: 0,
                max_rounds: 50,
                created_at: todo_list.created_at,
                updated_at: todo_list.updated_at,
                version: existing_version,
            }
        })
    }

    /// Track tool execution
    ///
    /// Records a tool call and associates it with the active todo item.
    pub fn track_tool_execution(
        &mut self,
        tool_name: &str,
        result: &ToolResult,
        round: u32,
    ) {
        self.current_round = round;

        // Record tool call
        let record = ToolCallRecord {
            round,
            tool_name: tool_name.to_string(),
            success: result.success,
            timestamp: Utc::now(),
        };

        // Associate with active item if exists
        if let Some(ref active_id) = self.active_item_id {
            if let Some(item) = self.items.iter_mut().find(|i| &i.id == active_id) {
                item.tool_calls.push(record);
                self.updated_at = Utc::now();
                self.version += 1;
            }
        }
    }

    /// Set active todo item
    ///
    /// Marks the previous active item as completed and activates a new item.
    pub fn set_active_item(&mut self, item_id: &str) {
        // Complete previous active item
        if let Some(ref prev_id) = self.active_item_id {
            if let Some(item) = self.items.iter_mut().find(|i| &i.id == prev_id) {
                item.status = TodoItemStatus::Completed;
                item.completed_at_round = Some(self.current_round);
            }
        }

        // Set new active item
        self.active_item_id = Some(item_id.to_string());
        if let Some(item) = self.items.iter_mut().find(|i| i.id == item_id) {
            item.status = TodoItemStatus::InProgress;
            item.started_at_round = Some(self.current_round);
        }

        self.updated_at = Utc::now();
        self.version += 1;
    }

    /// Update item status manually
    pub fn update_item_status(&mut self, item_id: &str, status: TodoItemStatus) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == item_id) {
            item.status = status.clone();

            match &status {
                TodoItemStatus::InProgress => {
                    item.started_at_round = Some(self.current_round);
                    self.active_item_id = Some(item_id.to_string());
                }
                TodoItemStatus::Completed => {
                    item.completed_at_round = Some(self.current_round);
                    if self.active_item_id.as_deref() == Some(item_id) {
                        self.active_item_id = None;
                    }
                }
                _ => {}
            }

            self.updated_at = Utc::now();
            self.version += 1;
        }
    }

    /// Check if all items are completed
    pub fn is_all_completed(&self) -> bool {
        !self.items.is_empty()
            && self
                .items
                .iter()
                .all(|item| matches!(item.status, TodoItemStatus::Completed))
    }

    /// Generate context for prompt injection
    pub fn format_for_prompt(&self) -> String {
        if self.items.is_empty() {
            return String::new();
        }

        let mut output = format!(
            "\n\n## Current Task List (Round {}/{})\n",
            self.current_round + 1,
            self.max_rounds
        );

        for item in &self.items {
            let status_icon = match item.status {
                TodoItemStatus::Pending => "[ ]",
                TodoItemStatus::InProgress => "[/]",
                TodoItemStatus::Completed => "[x]",
                TodoItemStatus::Blocked => "[!]",
            };

            output.push_str(&format!(
                "\n{} {}: {}",
                status_icon, item.id, item.description
            ));

            if !item.tool_calls.is_empty() {
                output.push_str(&format!(" ({} tool calls)", item.tool_calls.len()));
            }
        }

        let completed = self
            .items
            .iter()
            .filter(|i| matches!(i.status, TodoItemStatus::Completed))
            .count();
        output.push_str(&format!(
            "\n\nProgress: {}/{} tasks completed",
            completed,
            self.items.len()
        ));

        output
    }

    /// Convert back to TodoList for persistence
    pub fn into_todo_list(self) -> TodoList {
        TodoList {
            session_id: self.session_id,
            title: "Agent Tasks".to_string(),
            items: self
                .items
                .into_iter()
                .map(|loop_item| TodoItem {
                    id: loop_item.id,
                    description: loop_item.description,
                    status: loop_item.status,
                    depends_on: Vec::new(),
                    notes: String::new(),
                })
                .collect(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    /// Auto-match tool to todo item based on keywords
    pub fn auto_match_tool_to_item(&mut self, tool_name: &str) {
        if self.active_item_id.is_some() {
            return; // Already have an active item
        }

        // Strategy: Keyword matching
        let tool_lower = tool_name.to_lowercase();
        let matching_item_id = self.items.iter().find(|item| {
            let desc_lower = item.description.to_lowercase();
            // Simple heuristic: tool name appears in description
            desc_lower.contains(&tool_lower) ||
            // Or common tool patterns
            (tool_lower.contains("file") && desc_lower.contains("file")) ||
            (tool_lower.contains("command") && (desc_lower.contains("run") || desc_lower.contains("execute")))
        }).map(|item| item.id.clone());

        if let Some(item_id) = matching_item_id {
            self.set_active_item(&item_id);
        }
    }

    /// Auto-update status based on tool execution result
    pub fn auto_update_status(&mut self, tool_name: &str, result: &ToolResult) {
        // If no active item, try to auto-match
        if self.active_item_id.is_none() {
            self.auto_match_tool_to_item(tool_name);
        }

        if let Some(ref active_id) = self.active_item_id.clone() {
            // First, determine what action to take (avoid borrow issues)
            let action = self.items.iter().find(|i| &i.id == active_id).and_then(|item| {
                if result.success {
                    if self.should_mark_completed(item) {
                        Some(TodoItemStatus::Completed)
                    } else {
                        None
                    }
                } else if self.should_mark_blocked(item) {
                    Some(TodoItemStatus::Blocked)
                } else {
                    None
                }
            });

            // Then apply the action
            if let Some(new_status) = action {
                if let Some(item) = self.items.iter_mut().find(|i| &i.id == active_id) {
                    item.status = new_status.clone();
                    if matches!(new_status, TodoItemStatus::Completed) {
                        item.completed_at_round = Some(self.current_round);
                        self.active_item_id = None;
                    }
                    self.version += 1;
                    self.updated_at = Utc::now(); // IMPORTANT: Update timestamp
                }
            }
        }
    }

    /// Determine if item should be marked as completed
    fn should_mark_completed(&self, item: &TodoLoopItem) -> bool {
        // Simple strategy: 3 successful tool calls
        let success_count = item.tool_calls.iter().filter(|r| r.success).count();
        success_count >= 3
    }

    /// Determine if item should be marked as blocked
    fn should_mark_blocked(&self, item: &TodoLoopItem) -> bool {
        // Simple strategy: 2 consecutive failures
        let recent_failures = item
            .tool_calls
            .iter()
            .rev()
            .take(2)
            .filter(|r| !r.success)
            .count();
        recent_failures >= 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_todo_list() -> agent_core::Session {
        let mut session = agent_core::Session::new("test-session", "test-model");
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
    fn test_from_session() {
        let session = create_test_todo_list();
        let ctx = TodoLoopContext::from_session(&session).unwrap();

        assert_eq!(ctx.session_id, "test-session");
        assert_eq!(ctx.items.len(), 2);
        assert_eq!(ctx.items[0].id, "task-1");
        assert_eq!(ctx.items[0].tool_calls.len(), 0);
    }

    #[test]
    fn test_track_tool_execution() {
        let session = create_test_todo_list();
        let mut ctx = TodoLoopContext::from_session(&session).unwrap();

        // Set active item
        ctx.set_active_item("task-1");

        // Track tool execution
        let result = ToolResult {
            success: true,
            result: "OK".to_string(),
            display_preference: None,
        };
        ctx.track_tool_execution("read_file", &result, 1);

        assert_eq!(ctx.items[0].tool_calls.len(), 1);
        assert_eq!(ctx.items[0].tool_calls[0].tool_name, "read_file");
        assert!(ctx.items[0].tool_calls[0].success);
        assert_eq!(ctx.version, 2); // set_active_item + track_tool_execution
    }

    #[test]
    fn test_set_active_item() {
        let session = create_test_todo_list();
        let mut ctx = TodoLoopContext::from_session(&session).unwrap();

        ctx.set_active_item("task-1");

        assert_eq!(ctx.active_item_id, Some("task-1".to_string()));
        assert_eq!(ctx.items[0].status, TodoItemStatus::InProgress);
        assert_eq!(ctx.items[0].started_at_round, Some(0));
    }

    #[test]
    fn test_is_all_completed() {
        let session = create_test_todo_list();
        let mut ctx = TodoLoopContext::from_session(&session).unwrap();

        assert!(!ctx.is_all_completed());

        ctx.items[0].status = TodoItemStatus::Completed;
        ctx.items[1].status = TodoItemStatus::Completed;

        assert!(ctx.is_all_completed());
    }

    #[test]
    fn test_format_for_prompt() {
        let session = create_test_todo_list();
        let mut ctx = TodoLoopContext::from_session(&session).unwrap();
        ctx.current_round = 2;
        ctx.max_rounds = 10;

        let prompt = ctx.format_for_prompt();

        assert!(prompt.contains("Round 3/10"));
        assert!(prompt.contains("task-1"));
        assert!(prompt.contains("task-2"));
    }

    #[test]
    fn test_auto_match_tool_to_item() {
        let session = create_test_todo_list();
        let mut ctx = TodoLoopContext::from_session(&session).unwrap();

        // Test keyword matching
        ctx.auto_match_tool_to_item("read_file");

        // Should match task-1 (Read configuration file)
        assert_eq!(ctx.active_item_id, Some("task-1".to_string()));
    }

    #[test]
    fn test_auto_update_status_completed() {
        let session = create_test_todo_list();
        let mut ctx = TodoLoopContext::from_session(&session).unwrap();

        ctx.set_active_item("task-1");
        ctx.current_round = 1;

        // Add 3 successful tool calls
        let result = ToolResult {
            success: true,
            result: "OK".to_string(),
            display_preference: None,
        };

        ctx.track_tool_execution("read_file", &result, 1);
        ctx.auto_update_status("read_file", &result);
        assert_eq!(ctx.items[0].status, TodoItemStatus::InProgress);

        ctx.track_tool_execution("read_file", &result, 2);
        ctx.auto_update_status("read_file", &result);
        assert_eq!(ctx.items[0].status, TodoItemStatus::InProgress);

        ctx.track_tool_execution("read_file", &result, 3);
        ctx.auto_update_status("read_file", &result);

        // After 3 successful calls, should be completed
        assert_eq!(ctx.items[0].status, TodoItemStatus::Completed);
        assert_eq!(ctx.active_item_id, None);
    }

    #[test]
    fn test_auto_update_status_blocked() {
        let session = create_test_todo_list();
        let mut ctx = TodoLoopContext::from_session(&session).unwrap();

        ctx.set_active_item("task-1");
        ctx.current_round = 1;

        // Add 2 failed tool calls
        let fail_result = ToolResult {
            success: false,
            result: "Error".to_string(),
            display_preference: None,
        };

        ctx.track_tool_execution("read_file", &fail_result, 1);
        ctx.auto_update_status("read_file", &fail_result);
        assert_eq!(ctx.items[0].status, TodoItemStatus::InProgress);

        ctx.track_tool_execution("read_file", &fail_result, 2);
        ctx.auto_update_status("read_file", &fail_result);

        // After 2 consecutive failures, should be blocked
        assert_eq!(ctx.items[0].status, TodoItemStatus::Blocked);
    }

    #[test]
    fn test_into_todo_list() {
        let session = create_test_todo_list();
        let ctx = TodoLoopContext::from_session(&session).unwrap();

        let todo_list = ctx.into_todo_list();

        assert_eq!(todo_list.session_id, "test-session");
        assert_eq!(todo_list.items.len(), 2);
    }
}
