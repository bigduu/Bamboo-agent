//! Todo list types for task tracking in sessions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Todo item status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(Default)]
pub enum TodoItemStatus {
    #[serde(rename = "pending")]
    #[default]
    Pending,
    #[serde(rename = "in_progress")]
    InProgress,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "blocked")]
    Blocked,
}


/// Todo item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: String,
    pub description: String,
    pub status: TodoItemStatus,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub notes: String,
}

/// Todo list for a session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoList {
    pub session_id: String,
    pub title: String,
    pub items: Vec<TodoItem>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TodoList {
    /// Format todo list for display in system prompt
    pub fn format_for_prompt(&self) -> String {
        let mut output = format!("\n\n## Current Task List: {}\n", self.title);

        for item in &self.items {
            let status_icon = match item.status {
                TodoItemStatus::Pending => "[ ]",
                TodoItemStatus::InProgress => "[/]",
                TodoItemStatus::Completed => "[x]",
                TodoItemStatus::Blocked => "[!]",
            };

            output.push_str(&format!("\n{} {}: {}", status_icon, item.id, item.description));

            if !item.depends_on.is_empty() {
                output.push_str(&format!(" (depends on: {})", item.depends_on.join(", ")));
            }

            if !item.notes.is_empty() {
                output.push_str(&format!("\n    Notes: {}", item.notes.replace('\n', "\n    ")));
            }
        }

        let completed = self.items.iter().filter(|i| i.status == TodoItemStatus::Completed).count();
        let total = self.items.len();
        output.push_str(&format!("\n\nProgress: {}/{} tasks completed", completed, total));

        output
    }

    /// Update a todo item status
    pub fn update_item(&mut self, item_id: &str, status: TodoItemStatus, notes: Option<&str>) -> Result<String, String> {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == item_id) {
            item.status = status;
            if let Some(n) = notes {
                if !item.notes.is_empty() {
                    item.notes.push('\n');
                }
                item.notes.push_str(n);
            }
            self.updated_at = Utc::now();
            Ok(format!("Updated item '{}'", item_id))
        } else {
            Err(format!("Todo item '{}' not found", item_id))
        }
    }
}
