//! Task list types for task tracking in sessions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Task item status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum TaskItemStatus {
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

/// Task item for task tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskItem {
    /// Unique identifier for the task item.
    pub id: String,
    /// Human-readable description of the task.
    pub description: String,
    /// Current status of the item.
    pub status: TaskItemStatus,
    /// IDs of other items this item depends on.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Additional notes or context.
    #[serde(default)]
    pub notes: String,
}

/// Task list for a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskList {
    /// Session ID this task list belongs to.
    pub session_id: String,
    /// Title of the task list.
    pub title: String,
    /// List of task items.
    pub items: Vec<TaskItem>,
    /// When the list was created.
    pub created_at: DateTime<Utc>,
    /// When the list was last updated.
    pub updated_at: DateTime<Utc>,
}

impl TaskList {
    /// Format task list for display in system prompt.
    pub fn format_for_prompt(&self) -> String {
        let mut output = format!("\n\n## Current Task List: {}\n", self.title);

        for item in &self.items {
            let status_icon = match item.status {
                TaskItemStatus::Pending => "[ ]",
                TaskItemStatus::InProgress => "[/]",
                TaskItemStatus::Completed => "[x]",
                TaskItemStatus::Blocked => "[!]",
            };

            output.push_str(&format!(
                "\n{} {}: {}",
                status_icon, item.id, item.description
            ));

            if !item.depends_on.is_empty() {
                output.push_str(&format!(" (depends on: {})", item.depends_on.join(", ")));
            }

            if !item.notes.is_empty() {
                output.push_str(&format!(
                    "\n    Notes: {}",
                    item.notes.replace('\n', "\n    ")
                ));
            }
        }

        let completed = self
            .items
            .iter()
            .filter(|i| i.status == TaskItemStatus::Completed)
            .count();
        let total = self.items.len();
        output.push_str(&format!(
            "\n\nProgress: {}/{} tasks completed",
            completed, total
        ));

        output
    }

    /// Update a task item status.
    pub fn update_item(
        &mut self,
        item_id: &str,
        status: TaskItemStatus,
        notes: Option<&str>,
    ) -> Result<String, String> {
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
            Err(format!("Task item '{}' not found", item_id))
        }
    }
}
