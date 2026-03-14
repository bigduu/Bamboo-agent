use crate::agent::core::todo::TodoItemStatus;
use crate::agent::core::tools::ToolResult;
use chrono::Utc;

use super::{TodoLoopContext, TodoLoopItem};

impl TodoLoopContext {
    /// Auto-match tool to todo item based on keywords
    pub fn auto_match_tool_to_item(&mut self, tool_name: &str) {
        if self.active_item_id.is_some() {
            return;
        }

        let tool_lower = tool_name.to_lowercase();
        let matching_item_id = self
            .items
            .iter()
            .find(|item| {
                let desc_lower = item.description.to_lowercase();
                desc_lower.contains(&tool_lower)
                    || (tool_lower.contains("file") && desc_lower.contains("file"))
                    || (tool_lower.contains("command")
                        && (desc_lower.contains("run") || desc_lower.contains("execute")))
            })
            .map(|item| item.id.clone());

        if let Some(item_id) = matching_item_id {
            self.set_active_item(&item_id);
        }
    }

    /// Auto-update status based on tool execution result
    pub fn auto_update_status(&mut self, tool_name: &str, result: &ToolResult) {
        if self.active_item_id.is_none() {
            self.auto_match_tool_to_item(tool_name);
        }

        if let Some(ref active_id) = self.active_item_id.clone() {
            let action = self
                .items
                .iter()
                .find(|item| &item.id == active_id)
                .and_then(|item| {
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

            if let Some(new_status) = action {
                if let Some(item) = self.items.iter_mut().find(|item| &item.id == active_id) {
                    item.status = new_status.clone();
                    if matches!(new_status, TodoItemStatus::Completed) {
                        item.completed_at_round = Some(self.current_round);
                        self.active_item_id = None;
                    }
                    self.version += 1;
                    self.updated_at = Utc::now();
                }
            }
        }
    }

    /// Determine if item should be marked as completed.
    fn should_mark_completed(&self, item: &TodoLoopItem) -> bool {
        let success_count = item
            .tool_calls
            .iter()
            .filter(|record| record.success)
            .count();
        success_count >= 3
    }

    /// Determine if item should be marked as blocked.
    fn should_mark_blocked(&self, item: &TodoLoopItem) -> bool {
        let recent_failures = item
            .tool_calls
            .iter()
            .rev()
            .take(2)
            .filter(|record| !record.success)
            .count();
        recent_failures >= 2
    }
}
