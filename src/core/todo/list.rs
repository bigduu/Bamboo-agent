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
}
