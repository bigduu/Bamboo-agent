use crate::agent::core::{Message, Session};

use super::system_sections::strip_existing_prompt_block;

const TODO_LIST_START_MARKER: &str = "<!-- BAMBOO_TODO_LIST_START -->";
const TODO_LIST_END_MARKER: &str = "<!-- BAMBOO_TODO_LIST_END -->";

pub(super) fn inject_todo_list_into_system_message(session: &mut Session) {
    let todo_context = session.format_todo_list_for_prompt();

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let base_prompt = strip_existing_todo_list(&system_message.content);

        if !todo_context.is_empty() {
            if base_prompt.trim().is_empty() {
                system_message.content = format!(
                    "{TODO_LIST_START_MARKER}\n{}\n{TODO_LIST_END_MARKER}",
                    todo_context.trim()
                );
            } else {
                system_message.content = format!(
                    "{}\n\n{TODO_LIST_START_MARKER}\n{}\n{TODO_LIST_END_MARKER}",
                    base_prompt.trim_end(),
                    todo_context.trim(),
                );
            }
            tracing::info!(
                "Injected todo list into system message ({} chars)",
                todo_context.len()
            );
        } else {
            system_message.content = base_prompt;
        }
    } else if !todo_context.is_empty() {
        // No system message exists but we have todo context
        session.messages.insert(
            0,
            Message::system(format!(
                "{TODO_LIST_START_MARKER}\n{}\n{TODO_LIST_END_MARKER}",
                todo_context.trim()
            )),
        );
        tracing::info!(
            "Created system message with todo list ({} chars)",
            todo_context.len()
        );
    }
}

pub(super) fn strip_existing_todo_list(prompt: &str) -> String {
    strip_existing_prompt_block(prompt, TODO_LIST_START_MARKER, TODO_LIST_END_MARKER)
}
