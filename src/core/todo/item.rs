//! TodoItem - Universal execution unit
//!
//! Everything in the system becomes a TodoItem:
//! - Chat responses (streaming)
//! - Tool calls (blocking)
//! - Built-in tool calls (blocking)
//! - Workflow steps (blocking)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::execution::{TodoExecution, TodoStatus};

/// Universal execution unit - everything becomes a TodoItem
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TodoItem {
    /// Unique identifier
    pub id: Uuid,

    /// Type of todo item - determines execution strategy
    pub item_type: TodoItemType,

    /// Human-readable description
    pub description: String,

    /// Current status
    pub status: TodoStatus,

    /// Order within the list (0-indexed)
    pub order: u32,

    /// When this item was created
    pub created_at: DateTime<Utc>,

    /// When execution started
    pub started_at: Option<DateTime<Utc>>,

    /// When execution completed
    pub completed_at: Option<DateTime<Utc>>,

    /// Execution details (result, error, timing)
    pub execution: TodoExecution,

    /// Nested items (for workflows with sub-steps)
    pub children: Vec<TodoItem>,
}

impl TodoItem {
    /// Create a new TodoItem with the given type and description
    pub fn new(item_type: TodoItemType, description: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            item_type,
            description: description.into(),
            status: TodoStatus::Pending,
            order: 0,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            execution: TodoExecution::default(),
            children: Vec::new(),
        }
    }

    /// Check if this item requires blocking execution
    pub fn is_blocking(&self) -> bool {
        self.item_type.is_blocking()
    }

    /// Check if this item is streaming/async
    pub fn is_async(&self) -> bool {
        self.item_type.is_async()
    }

    /// Mark as started
    pub fn start(&mut self) {
        self.status = TodoStatus::InProgress;
        self.started_at = Some(Utc::now());
    }

    /// Mark as completed with result
    pub fn complete(&mut self, result: Option<serde_json::Value>) {
        self.status = TodoStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.execution.result = result;

        if let (Some(start), Some(end)) = (self.started_at, self.completed_at) {
            self.execution.duration_ms = Some((end - start).num_milliseconds() as u64);
        }
    }

    /// Mark as failed with error
    pub fn fail(&mut self, error: impl Into<String>) {
        self.status = TodoStatus::Failed {
            error: error.into(),
        };
        self.completed_at = Some(Utc::now());
    }
}

/// Type of todo item - determines execution strategy
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TodoItemType {
    /// Streaming LLM response (summary, analysis, explanation)
    /// Execution: Streaming
    Chat {
        /// ID of the message being streamed (set when streaming starts)
        streaming_message_id: Option<Uuid>,
    },

    /// Tool execution
    /// Execution: Blocking
    ToolCall {
        /// Name of the tool to execute
        tool_name: String,
        /// Arguments to pass to the tool
        arguments: serde_json::Value,
    },

    /// Workflow step execution
    /// Execution: Blocking
    WorkflowStep {
        /// Name of the workflow
        workflow_name: String,
        /// Step index (0-based)
        step_index: usize,
        /// Description of this step
        step_description: String,
    },
}

impl TodoItemType {
    /// Returns true if this item type requires blocking execution
    /// Tool calls and workflow steps are blocking
    pub fn is_blocking(&self) -> bool {
        matches!(self, Self::ToolCall { .. } | Self::WorkflowStep { .. })
    }

    /// Returns true if this item is streaming/async
    /// Chat is async
    pub fn is_async(&self) -> bool {
        matches!(self, Self::Chat { .. })
    }

    /// Get a short label for display
    pub fn label(&self) -> &str {
        match self {
            Self::Chat { .. } => "Chat",
            Self::ToolCall { tool_name, .. } => tool_name,
            Self::WorkflowStep { workflow_name, .. } => workflow_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_todo_item_creation() {
        let item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "read_file".to_string(),
                arguments: serde_json::json!({"path": "/test.txt"}),
            },
            "Read test file",
        );

        assert!(item.is_blocking());
        assert!(!item.is_async());
        assert!(matches!(item.status, TodoStatus::Pending));
    }

    #[test]
    fn test_chat_is_async() {
        let item = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Generate summary",
        );

        assert!(!item.is_blocking());
        assert!(item.is_async());
    }

    #[test]
    fn test_todo_item_start() {
        let mut item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "test_tool".to_string(),
                arguments: serde_json::json!({}),
            },
            "Test task",
        );

        assert!(matches!(item.status, TodoStatus::Pending));
        assert!(item.started_at.is_none());

        item.start();

        assert!(matches!(item.status, TodoStatus::InProgress));
        assert!(item.started_at.is_some());
    }

    #[test]
    fn test_todo_item_complete() {
        let mut item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "test_tool".to_string(),
                arguments: serde_json::json!({}),
            },
            "Test task",
        );

        item.start();
        let result = Some(serde_json::json!({"output": "success"}));
        item.complete(result.clone());

        assert!(matches!(item.status, TodoStatus::Completed));
        assert!(item.completed_at.is_some());
        assert_eq!(item.execution.result, result);
        assert!(item.execution.duration_ms.is_some());
    }

    #[test]
    fn test_todo_item_fail() {
        let mut item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "test_tool".to_string(),
                arguments: serde_json::json!({}),
            },
            "Test task",
        );

        item.start();
        item.fail("Something went wrong");

        match item.status {
            TodoStatus::Failed { error } => {
                assert_eq!(error, "Something went wrong");
            }
            _ => panic!("Expected Failed status"),
        }
        assert!(item.completed_at.is_some());
    }

    #[test]
    fn test_workflow_step_is_blocking() {
        let item = TodoItem::new(
            TodoItemType::WorkflowStep {
                workflow_name: "test_workflow".to_string(),
                step_index: 0,
                step_description: "First step".to_string(),
            },
            "Execute workflow step",
        );

        assert!(item.is_blocking());
        assert!(!item.is_async());
    }

    #[test]
    fn test_todo_item_type_label() {
        let chat_type = TodoItemType::Chat {
            streaming_message_id: None,
        };
        assert_eq!(chat_type.label(), "Chat");

        let tool_type = TodoItemType::ToolCall {
            tool_name: "read_file".to_string(),
            arguments: serde_json::json!({}),
        };
        assert_eq!(tool_type.label(), "read_file");

        let workflow_type = TodoItemType::WorkflowStep {
            workflow_name: "deployment".to_string(),
            step_index: 0,
            step_description: "Deploy".to_string(),
        };
        assert_eq!(workflow_type.label(), "deployment");
    }

    #[test]
    fn test_todo_item_serialization() {
        let item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "test".to_string(),
                arguments: serde_json::json!({"arg": "value"}),
            },
            "Test description",
        );

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("Test description"));
    }

    #[test]
    fn test_todo_item_with_children() {
        let mut parent = TodoItem::new(
            TodoItemType::WorkflowStep {
                workflow_name: "parent".to_string(),
                step_index: 0,
                step_description: "Parent step".to_string(),
            },
            "Parent task",
        );

        let child = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "child_tool".to_string(),
                arguments: serde_json::json!({}),
            },
            "Child task",
        );

        parent.children.push(child);
        assert_eq!(parent.children.len(), 1);
    }

    #[test]
    fn test_chat_with_streaming_message_id() {
        let message_id = Uuid::new_v4();
        let item = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: Some(message_id),
            },
            "Streaming chat",
        );

        assert!(item.is_async());
        if let TodoItemType::Chat {
            streaming_message_id,
        } = item.item_type
        {
            assert_eq!(streaming_message_id, Some(message_id));
        } else {
            panic!("Expected Chat type");
        }
    }

    #[test]
    fn test_todo_item_complete_without_start() {
        let mut item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "test".to_string(),
                arguments: serde_json::json!({}),
            },
            "Test",
        );

        // Complete without starting - duration should be None
        item.complete(Some(serde_json::json!("result")));
        assert!(item.execution.duration_ms.is_none());
    }

    #[test]
    fn test_todo_item_order() {
        let mut item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "test".to_string(),
                arguments: serde_json::json!({}),
            },
            "Test",
        );

        assert_eq!(item.order, 0);
        item.order = 5;
        assert_eq!(item.order, 5);
    }
}
