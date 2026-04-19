use bamboo_domain::task::{
    task_evidence_kind_label, task_phase_label, task_priority_label, TaskPhase, TaskPriority,
};
use bamboo_domain::TaskItemStatus;

use super::TaskLoopContext;

impl TaskLoopContext {
    fn truncate_prompt_value(value: &str, max_chars: usize) -> String {
        let trimmed = value.trim().replace('\n', " ");
        let char_count = trimmed.chars().count();
        if char_count <= max_chars {
            return trimmed;
        }

        let truncated: String = trimmed.chars().take(max_chars).collect();
        format!("{}…", truncated.trim_end())
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
                TaskItemStatus::Pending => "[ ]",
                TaskItemStatus::InProgress => "[/]",
                TaskItemStatus::Completed => "[x]",
                TaskItemStatus::Blocked => "[!]",
            };

            output.push_str(&format!(
                "\n{} {}: {}",
                status_icon, item.id, item.description
            ));

            if !item.tool_calls.is_empty() {
                output.push_str(&format!(" ({} tool calls)", item.tool_calls.len()));
            }

            let mut tags = Vec::new();
            if item.phase != TaskPhase::Execution {
                tags.push(format!("phase={}", task_phase_label(&item.phase)));
            }
            if item.priority != TaskPriority::Medium {
                tags.push(format!("priority={}", task_priority_label(&item.priority)));
            }
            if let Some(parent_id) = item.parent_id.as_deref().filter(|value| !value.is_empty()) {
                tags.push(format!("parent={parent_id}"));
            }
            if !item.depends_on.is_empty() {
                tags.push(format!("depends_on={}", item.depends_on.join(", ")));
            }
            if !tags.is_empty() {
                output.push_str(&format!(" [{}]", tags.join(" | ")));
            }

            if matches!(item.status, TaskItemStatus::InProgress) {
                if let Some(active_form) = item
                    .active_form
                    .as_deref()
                    .filter(|value| !value.is_empty())
                {
                    output.push_str(&format!(
                        "\n    Active: {}",
                        Self::truncate_prompt_value(active_form, 140)
                    ));
                }
            }

            if !item.completion_criteria.is_empty() {
                let criteria = item
                    .completion_criteria
                    .iter()
                    .enumerate()
                    .map(|(index, criterion)| format!("c{}: {}", index + 1, criterion.trim()))
                    .collect::<Vec<_>>()
                    .join(" | ");
                output.push_str(&format!("\n    Completion Criteria: {criteria}"));
            }

            if let Some(blocker) = item.blockers.last() {
                let mut blocker_line = Self::truncate_prompt_value(&blocker.summary, 140);
                if let Some(waiting_on) = blocker
                    .waiting_on
                    .as_deref()
                    .filter(|value| !value.is_empty())
                {
                    blocker_line.push_str(&format!(
                        " (waiting_on: {})",
                        Self::truncate_prompt_value(waiting_on, 60)
                    ));
                }
                output.push_str(&format!("\n    Blocked by: {blocker_line}"));
            }

            if let Some(evidence) = item.evidence.last() {
                let mut evidence_line = Self::truncate_prompt_value(&evidence.summary, 140);
                if let Some(reference) = evidence
                    .reference
                    .as_deref()
                    .filter(|value| !value.is_empty())
                {
                    evidence_line.push_str(&format!(
                        " [ref: {}]",
                        Self::truncate_prompt_value(reference, 60)
                    ));
                }
                output.push_str(&format!(
                    "\n    Latest evidence [{}]: {}",
                    task_evidence_kind_label(&evidence.kind),
                    evidence_line
                ));
            }

            let notes = item.notes.trim();
            let active_form = item.active_form.as_deref().map(str::trim);
            if !notes.is_empty() && Some(notes) != active_form {
                output.push_str(&format!(
                    "\n    Notes: {}",
                    Self::truncate_prompt_value(notes, 160)
                ));
            }
        }

        let completed = self
            .items
            .iter()
            .filter(|item| matches!(item.status, TaskItemStatus::Completed))
            .count();

        output.push_str(&format!(
            "\n\nProgress: {}/{} tasks completed",
            completed,
            self.items.len()
        ));

        output
    }
}
