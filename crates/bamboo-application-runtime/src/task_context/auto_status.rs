use bamboo_domain_session::task::{TaskBlocker, TaskBlockerKind};
use bamboo_application_agent::tools::ToolResult;
use bamboo_domain_session::TaskItemStatus;
use chrono::Utc;

use super::{TaskLoopContext, TaskLoopItem};

impl TaskLoopContext {
    /// Auto-match tool to task item based on keywords
    pub fn auto_match_tool_to_item(&mut self, tool_name: &str) {
        if self.active_item_id.is_some() {
            return;
        }

        let tool_lower = tool_name.to_lowercase();
        let matching_item_id = self
            .items
            .iter()
            .find(|item| {
                if matches!(item.status, TaskItemStatus::Completed) {
                    return false;
                }
                if !self.unresolved_dependencies(&item.depends_on).is_empty() {
                    return false;
                }
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
                            Some(TaskItemStatus::Completed)
                        } else {
                            None
                        }
                    } else if self.should_mark_blocked(item) {
                        Some(TaskItemStatus::Blocked)
                    } else {
                        None
                    }
                });

            if let Some(new_status) = action {
                if let Some(item) = self.items.iter_mut().find(|item| &item.id == active_id) {
                    if matches!(new_status, TaskItemStatus::Blocked) {
                        let mut summary = format!("Tool `{}` failed repeatedly", tool_name);
                        let detail = result.result.trim();
                        if !detail.is_empty() {
                            let clipped: String = detail.chars().take(120).collect();
                            if detail.chars().count() > 120 {
                                summary.push_str(&format!(": {}…", clipped.trim_end()));
                            } else {
                                summary.push_str(&format!(": {clipped}"));
                            }
                        }
                        TaskLoopContext::add_item_blocker(
                            item,
                            TaskBlocker {
                                kind: TaskBlockerKind::ToolFailure,
                                summary,
                                waiting_on: None,
                            },
                        );
                    }

                    let reason = if result.success {
                        Some("Auto-updated after sufficient successful tool calls")
                    } else {
                        Some("Auto-updated after repeated tool failures")
                    };
                    let changed = TaskLoopContext::transition_item(
                        item,
                        new_status.clone(),
                        reason,
                        Some(self.current_round),
                    );

                    if changed && matches!(new_status, TaskItemStatus::Completed) {
                        item.completed_at_round = Some(self.current_round);
                        self.active_item_id = None;
                    }
                    if changed {
                        self.version += 1;
                        self.updated_at = Utc::now();
                    }
                }
            }
        }
    }

    /// Determine if item should be marked as completed.
    fn should_mark_completed(&self, item: &TaskLoopItem) -> bool {
        if !item.completion_criteria.is_empty() {
            return false;
        }
        let success_count = item
            .tool_calls
            .iter()
            .filter(|record| record.success)
            .count();
        success_count >= 3
    }

    /// Determine if item should be marked as blocked.
    fn should_mark_blocked(&self, item: &TaskLoopItem) -> bool {
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
