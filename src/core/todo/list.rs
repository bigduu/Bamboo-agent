//! TodoList - Container for TodoItems
//!
//! Groups related TodoItems for a task or workflow.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::execution::TodoStatus;
use super::item::TodoItem;

/// Container for related todo items
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TodoList {
    /// Unique identifier
    pub id: Uuid,

    /// Title of this todo list
    pub title: String,

    /// Optional longer description
    pub description: Option<String>,

    /// Items in this list
    pub items: Vec<TodoItem>,

    /// Overall status of the list
    pub status: TodoListStatus,

    /// When this list was created
    pub created_at: DateTime<Utc>,

    /// When this list was completed (all items done)
    pub completed_at: Option<DateTime<Utc>>,

    /// ID of the message that created this list
    pub source_message_id: Option<Uuid>,

    /// Context ID this list belongs to
    pub context_id: Uuid,
}

impl TodoList {
    /// Create a new empty TodoList
    pub fn new(title: impl Into<String>, context_id: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            title: title.into(),
            description: None,
            items: Vec::new(),
            status: TodoListStatus::Active,
            created_at: Utc::now(),
            completed_at: None,
            source_message_id: None,
            context_id,
        }
    }

    /// Add an item to the list
    pub fn add_item(&mut self, mut item: TodoItem) {
        item.order = self.items.len() as u32;
        self.items.push(item);
    }

    /// Get item by ID
    pub fn get_item(&self, id: Uuid) -> Option<&TodoItem> {
        self.items.iter().find(|i| i.id == id)
    }

    /// Get mutable item by ID
    pub fn get_item_mut(&mut self, id: Uuid) -> Option<&mut TodoItem> {
        self.items.iter_mut().find(|i| i.id == id)
    }

    /// Count of pending items
    pub fn pending_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| matches!(i.status, TodoStatus::Pending))
            .count()
    }

    /// Count of completed items
    pub fn completed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| matches!(i.status, TodoStatus::Completed))
            .count()
    }

    /// Count of failed items
    pub fn failed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| matches!(i.status, TodoStatus::Failed { .. }))
            .count()
    }

    /// Progress as percentage (0.0 - 1.0)
    pub fn progress(&self) -> f64 {
        if self.items.is_empty() {
            return 1.0;
        }
        let terminal = self.items.iter().filter(|i| i.status.is_terminal()).count();
        terminal as f64 / self.items.len() as f64
    }

    /// Check if all items are completed
    pub fn is_all_completed(&self) -> bool {
        !self.items.is_empty() && self.items.iter().all(|i| i.status.is_terminal())
    }

    /// Get the next pending item
    pub fn next_pending(&self) -> Option<&TodoItem> {
        self.items
            .iter()
            .find(|i| matches!(i.status, TodoStatus::Pending))
    }

    /// Mark the list as completed
    pub fn complete(&mut self) {
        self.status = TodoListStatus::Completed;
        self.completed_at = Some(Utc::now());
    }

    /// Mark the list as abandoned
    pub fn abandon(&mut self) {
        self.status = TodoListStatus::Abandoned;
        self.completed_at = Some(Utc::now());
    }
}

/// Overall status of a TodoList
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum TodoListStatus {
    /// List is active, items being processed
    #[default]
    Active,

    /// All items completed successfully
    Completed,

    /// List was abandoned before completion
    Abandoned,

    /// List is paused (waiting for user action)
    Paused,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::todo::TodoItemType;

    #[test]
    fn test_todo_list_progress() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test List", context_id);

        let mut item1 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 1",
        );
        item1.complete(None);

        let item2 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 2",
        );

        list.add_item(item1);
        list.add_item(item2);

        assert_eq!(list.progress(), 0.5);
        assert_eq!(list.completed_count(), 1);
        assert_eq!(list.pending_count(), 1);
    }

    #[test]
    fn test_todo_list_new() {
        let context_id = Uuid::new_v4();
        let list = TodoList::new("Test Title", context_id);

        assert!(list.title == "Test Title");
        assert!(list.description.is_none());
        assert!(list.items.is_empty());
        assert!(matches!(list.status, TodoListStatus::Active));
        assert!(list.completed_at.is_none());
        assert!(list.source_message_id.is_none());
        assert_eq!(list.context_id, context_id);
    }

    #[test]
    fn test_todo_list_add_item() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        let item = TodoItem::new(
            TodoItemType::ToolCall {
                tool_name: "test".to_string(),
                arguments: serde_json::json!({}),
            },
            "Test item",
        );

        list.add_item(item);
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].order, 0);

        let item2 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Second item",
        );
        list.add_item(item2);
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[1].order, 1);
    }

    #[test]
    fn test_todo_list_get_item() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        let item = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Test item",
        );
        let item_id = item.id;

        list.add_item(item);

        let found = list.get_item(item_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().description, "Test item");

        let not_found = list.get_item(Uuid::new_v4());
        assert!(not_found.is_none());
    }

    #[test]
    fn test_todo_list_get_item_mut() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        let item = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Test item",
        );
        let item_id = item.id;

        list.add_item(item);

        let found = list.get_item_mut(item_id);
        assert!(found.is_some());
        found.unwrap().description = "Modified".to_string();

        assert_eq!(list.items[0].description, "Modified");
    }

    #[test]
    fn test_todo_list_counts() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        let mut item1 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 1",
        );
        item1.complete(None);

        let mut item2 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 2",
        );
        item2.fail("Error");

        let item3 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 3",
        );

        list.add_item(item1);
        list.add_item(item2);
        list.add_item(item3);

        assert_eq!(list.completed_count(), 1);
        assert_eq!(list.failed_count(), 1);
        assert_eq!(list.pending_count(), 1);
    }

    #[test]
    fn test_todo_list_progress_empty() {
        let context_id = Uuid::new_v4();
        let list = TodoList::new("Test", context_id);
        assert_eq!(list.progress(), 1.0);
    }

    #[test]
    fn test_todo_list_progress_all_completed() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        let mut item1 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 1",
        );
        item1.complete(None);

        let mut item2 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 2",
        );
        item2.fail("Error");

        list.add_item(item1);
        list.add_item(item2);

        assert_eq!(list.progress(), 1.0);
    }

    #[test]
    fn test_todo_list_is_all_completed() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        let mut item1 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 1",
        );
        item1.complete(None);

        let mut item2 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 2",
        );
        item2.fail("Error");

        list.add_item(item1);
        list.add_item(item2);

        assert!(list.is_all_completed());

        let item3 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 3",
        );
        list.add_item(item3);
        assert!(!list.is_all_completed());
    }

    #[test]
    fn test_todo_list_next_pending() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        let mut item1 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 1",
        );
        item1.complete(None);

        let item2 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 2",
        );
        let item2_id = item2.id;

        list.add_item(item1);
        list.add_item(item2);

        let next = list.next_pending();
        assert!(next.is_some());
        assert_eq!(next.unwrap().id, item2_id);

        let mut item3 = TodoItem::new(
            TodoItemType::Chat {
                streaming_message_id: None,
            },
            "Item 3",
        );
        item3.complete(None);
        list.add_item(item3);

        assert!(list.next_pending().is_some());
    }

    #[test]
    fn test_todo_list_complete() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        list.complete();
        assert!(matches!(list.status, TodoListStatus::Completed));
        assert!(list.completed_at.is_some());
    }

    #[test]
    fn test_todo_list_abandon() {
        let context_id = Uuid::new_v4();
        let mut list = TodoList::new("Test", context_id);

        list.abandon();
        assert!(matches!(list.status, TodoListStatus::Abandoned));
        assert!(list.completed_at.is_some());
    }

    #[test]
    fn test_todo_list_status_default() {
        let status = TodoListStatus::default();
        assert!(matches!(status, TodoListStatus::Active));
    }

    #[test]
    fn test_todo_list_status_serialization() {
        let status = TodoListStatus::Completed;
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("completed"));
    }

    #[test]
    fn test_todo_list_status_clone() {
        let status = TodoListStatus::Paused;
        let cloned = status.clone();
        assert!(matches!(cloned, TodoListStatus::Paused));
    }

    #[test]
    fn test_todo_list_status_debug() {
        let status = TodoListStatus::Active;
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("Active"));
    }

    #[test]
    fn test_todo_list_serialization() {
        let context_id = Uuid::new_v4();
        let list = TodoList::new("Test List", context_id);
        let json = serde_json::to_string(&list).unwrap();
        assert!(json.contains("Test List"));
    }

    #[test]
    fn test_todo_list_clone() {
        let context_id = Uuid::new_v4();
        let list = TodoList::new("Test", context_id);
        let cloned = list.clone();
        assert_eq!(list.title, cloned.title);
    }

    #[test]
    fn test_todo_list_debug() {
        let context_id = Uuid::new_v4();
        let list = TodoList::new("Test", context_id);
        let debug_str = format!("{:?}", list);
        assert!(debug_str.contains("TodoList"));
    }
}
