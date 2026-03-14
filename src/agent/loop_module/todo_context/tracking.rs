use crate::agent::core::todo::TodoItemStatus;
use crate::agent::core::tools::ToolResult;
use chrono::Utc;

use super::{TodoLoopContext, ToolCallRecord};

impl TodoLoopContext {
    /// Track tool execution
    ///
    /// Records a tool call and associates it with the active todo item.
    pub fn track_tool_execution(&mut self, tool_name: &str, result: &ToolResult, round: u32) {
        self.current_round = round;

        let record = ToolCallRecord {
            round,
            tool_name: tool_name.to_string(),
            success: result.success,
            timestamp: Utc::now(),
        };

        if let Some(ref active_id) = self.active_item_id {
            if let Some(item) = self.items.iter_mut().find(|item| &item.id == active_id) {
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
        if let Some(ref previous_id) = self.active_item_id {
            if let Some(item) = self.items.iter_mut().find(|item| &item.id == previous_id) {
                item.status = TodoItemStatus::Completed;
                item.completed_at_round = Some(self.current_round);
            }
        }

        self.active_item_id = Some(item_id.to_string());
        if let Some(item) = self.items.iter_mut().find(|item| item.id == item_id) {
            item.status = TodoItemStatus::InProgress;
            item.started_at_round = Some(self.current_round);
        }

        self.updated_at = Utc::now();
        self.version += 1;
    }

    /// Update item status manually
    pub fn update_item_status(&mut self, item_id: &str, status: TodoItemStatus) {
        if let Some(item) = self.items.iter_mut().find(|item| item.id == item_id) {
            item.status = status.clone();

            match status {
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
                TodoItemStatus::Pending | TodoItemStatus::Blocked => {}
            }

            self.updated_at = Utc::now();
            self.version += 1;
        }
    }
}
