use crate::agent::core::todo::TodoItemStatus;

use super::TodoLoopContext;

impl TodoLoopContext {
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
                TodoItemStatus::Pending => "[ ]",
                TodoItemStatus::InProgress => "[/]",
                TodoItemStatus::Completed => "[x]",
                TodoItemStatus::Blocked => "[!]",
            };

            output.push_str(&format!(
                "\n{} {}: {}",
                status_icon, item.id, item.description
            ));

            if !item.tool_calls.is_empty() {
                output.push_str(&format!(" ({} tool calls)", item.tool_calls.len()));
            }
        }

        let completed = self
            .items
            .iter()
            .filter(|item| matches!(item.status, TodoItemStatus::Completed))
            .count();

        output.push_str(&format!(
            "\n\nProgress: {}/{} tasks completed",
            completed,
            self.items.len()
        ));

        output
    }
}
