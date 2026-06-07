use bamboo_agent_core::tools::ToolResult;
use bamboo_domain::task::{TaskBlocker, TaskBlockerKind, TaskEvidence, TaskEvidenceKind};
use bamboo_domain::TaskItemStatus;
use chrono::Utc;

use super::{TaskLoopContext, ToolCallRecord};

fn summarize_tool_result(tool_name: &str, result: &ToolResult, max_chars: usize) -> String {
    let status_text = if result.success {
        "succeeded"
    } else {
        "failed"
    };
    let mut summary = format!("Tool `{tool_name}` {status_text}");
    let detail = result.result.trim();
    if !detail.is_empty() {
        let clipped: String = detail.chars().take(max_chars).collect();
        if detail.chars().count() > max_chars {
            summary.push_str(&format!(": {}…", clipped.trim_end()));
        } else {
            summary.push_str(&format!(": {clipped}"));
        }
    }
    summary
}

impl TaskLoopContext {
    /// Track tool execution
    ///
    /// Records a tool call and associates it with the active task item.
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
                Self::push_item_evidence(
                    item,
                    TaskEvidence {
                        kind: TaskEvidenceKind::ToolCall,
                        summary: summarize_tool_result(tool_name, result, 160),
                        reference: None,
                        tool_name: Some(tool_name.to_string()),
                        tool_call_id: None,
                        round: Some(round),
                        success: Some(result.success),
                    },
                );
                self.updated_at = Utc::now();
                self.version += 1;
            }
        }
    }

    /// Set active task item
    ///
    /// Moves the previous active item back to pending and activates a new item.
    pub fn set_active_item(&mut self, item_id: &str) {
        if self.active_item_id.as_deref() == Some(item_id) {
            return;
        }

        let Some(target_index) = self.items.iter().position(|item| item.id == item_id) else {
            return;
        };

        let unmet_dependencies = {
            let target = &self.items[target_index];
            self.unresolved_dependencies(&target.depends_on)
        };

        if !unmet_dependencies.is_empty() {
            let mut changed = false;
            if let Some(item) = self.items.get_mut(target_index) {
                let summary = if unmet_dependencies.len() == 1 {
                    format!(
                        "Waiting for dependency `{}` to complete",
                        unmet_dependencies[0]
                    )
                } else {
                    format!(
                        "Waiting for dependencies to complete: {}",
                        unmet_dependencies.join(", ")
                    )
                };
                let waiting_on = if unmet_dependencies.len() == 1 {
                    Some(unmet_dependencies[0].clone())
                } else {
                    None
                };
                let blocker_count = item.blockers.len();
                Self::add_item_blocker(
                    item,
                    TaskBlocker {
                        kind: TaskBlockerKind::Dependency,
                        summary,
                        waiting_on,
                    },
                );
                if item.blockers.len() > blocker_count {
                    changed = true;
                }
                changed |= Self::transition_item(
                    item,
                    TaskItemStatus::Blocked,
                    Some("Cannot start task before dependencies are completed"),
                    Some(self.current_round),
                );
            }
            if changed {
                self.updated_at = Utc::now();
                self.version += 1;
            }
            return;
        }

        if let Some(ref previous_id) = self.active_item_id.clone() {
            if let Some(item) = self.items.iter_mut().find(|item| &item.id == previous_id) {
                Self::transition_item(
                    item,
                    TaskItemStatus::Pending,
                    Some("Task focus switched to another item"),
                    Some(self.current_round),
                );
            }
        }

        self.active_item_id = Some(item_id.to_string());
        if let Some(item) = self.items.get_mut(target_index) {
            Self::transition_item(
                item,
                TaskItemStatus::InProgress,
                Some("Task marked as active"),
                Some(self.current_round),
            );
            item.started_at_round = Some(self.current_round);
        }

        self.updated_at = Utc::now();
        self.version += 1;
    }

    /// Update item status manually
    pub fn update_item_status(&mut self, item_id: &str, status: TaskItemStatus) {
        if let Some(item) = self.items.iter_mut().find(|item| item.id == item_id) {
            let changed =
                Self::transition_item(item, status.clone(), None, Some(self.current_round));

            match status {
                TaskItemStatus::InProgress => {
                    item.started_at_round = Some(self.current_round);
                    self.active_item_id = Some(item_id.to_string());
                }
                TaskItemStatus::Completed => {
                    item.completed_at_round = Some(self.current_round);
                    if self.active_item_id.as_deref() == Some(item_id) {
                        self.active_item_id = None;
                    }
                }
                TaskItemStatus::Pending | TaskItemStatus::Blocked => {
                    if self.active_item_id.as_deref() == Some(item_id) {
                        self.active_item_id = None;
                    }
                }
            }

            if changed {
                self.updated_at = Utc::now();
                self.version += 1;
            }
        }
    }

    /// Apply an evaluated task update and report whether it changed the context.
    pub fn apply_evaluated_update(
        &mut self,
        item_id: &str,
        status: TaskItemStatus,
        notes: Option<&str>,
        evidence: Option<&str>,
        blocker: Option<&str>,
    ) -> bool {
        let previous_version = self.version;
        self.update_item_status(item_id, status);
        if let Some(note) = notes.map(str::trim).filter(|note| !note.is_empty()) {
            if let Some(item) = self.items.iter_mut().find(|item| item.id == item_id) {
                let previous_notes = item.notes.clone();
                Self::append_item_notes(item, note);
                if item.notes != previous_notes {
                    self.updated_at = Utc::now();
                    self.version += 1;
                }
            }
        }
        self.append_structured_feedback(item_id, evidence, blocker);
        self.version != previous_version
    }

    pub fn append_structured_feedback(
        &mut self,
        item_id: &str,
        evidence: Option<&str>,
        blocker: Option<&str>,
    ) {
        let mut changed = false;
        if let Some(item) = self.items.iter_mut().find(|item| item.id == item_id) {
            if let Some(summary) = evidence
                .map(str::trim)
                .filter(|summary| !summary.is_empty())
            {
                Self::push_item_evidence(
                    item,
                    TaskEvidence {
                        kind: TaskEvidenceKind::Observation,
                        summary: summary.to_string(),
                        reference: None,
                        tool_name: None,
                        tool_call_id: None,
                        round: Some(self.current_round),
                        success: None,
                    },
                );
                changed = true;
            }
            if let Some(summary) = blocker.map(str::trim).filter(|summary| !summary.is_empty()) {
                Self::add_item_blocker(
                    item,
                    TaskBlocker {
                        kind: TaskBlockerKind::Unknown,
                        summary: summary.to_string(),
                        waiting_on: None,
                    },
                );
                changed = true;
            }
        }
        if changed {
            self.updated_at = Utc::now();
            self.version += 1;
        }
    }
}
