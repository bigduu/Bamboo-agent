use crate::agent::core::{Message, Session};

use super::system_sections::strip_existing_prompt_block;

const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";
const TASK_LIST_END_MARKER: &str = "<!-- BAMBOO_TASK_LIST_END -->";
const LEGACY_TODO_LIST_START_MARKER: &str = "<!-- BAMBOO_TODO_LIST_START -->";
const LEGACY_TODO_LIST_END_MARKER: &str = "<!-- BAMBOO_TODO_LIST_END -->";

pub(super) fn inject_task_list_into_system_message(session: &mut Session) {
    let task_context = session.format_task_list_for_prompt();

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let base_prompt = strip_existing_task_list(&system_message.content);

        if !task_context.is_empty() {
            if base_prompt.trim().is_empty() {
                system_message.content = format!(
                    "{TASK_LIST_START_MARKER}\n{}\n{TASK_LIST_END_MARKER}",
                    task_context.trim()
                );
            } else {
                system_message.content = format!(
                    "{}\n\n{TASK_LIST_START_MARKER}\n{}\n{TASK_LIST_END_MARKER}",
                    base_prompt.trim_end(),
                    task_context.trim(),
                );
            }
            tracing::info!(
                "Injected task list into system message ({} chars)",
                task_context.len()
            );
        } else {
            system_message.content = base_prompt;
        }
    } else if !task_context.is_empty() {
        // No system message exists but we have task context.
        session.messages.insert(
            0,
            Message::system(format!(
                "{TASK_LIST_START_MARKER}\n{}\n{TASK_LIST_END_MARKER}",
                task_context.trim()
            )),
        );
        tracing::info!(
            "Created system message with task list ({} chars)",
            task_context.len()
        );
    }
}

pub(super) fn strip_existing_task_list(prompt: &str) -> String {
    let prompt = strip_existing_prompt_block(prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER);
    strip_existing_prompt_block(
        &prompt,
        LEGACY_TODO_LIST_START_MARKER,
        LEGACY_TODO_LIST_END_MARKER,
    )
}
