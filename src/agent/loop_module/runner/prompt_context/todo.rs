use crate::agent::core::{Message, Session};

use super::system_sections::strip_existing_prompt_section;

const TODO_LIST_MARKER: &str = "\n\n## Current Task List:";

pub(super) fn inject_todo_list_into_system_message(session: &mut Session) {
    let todo_context = session.format_todo_list_for_prompt();

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let base_prompt = strip_existing_todo_list(&system_message.content);

        if !todo_context.is_empty() {
            system_message.content = format!("{}\n{}", base_prompt, todo_context);
            log::info!(
                "Injected todo list into system message ({} chars)",
                todo_context.len()
            );
        } else {
            system_message.content = base_prompt;
        }
    } else if !todo_context.is_empty() {
        // No system message exists but we have todo context
        session
            .messages
            .insert(0, Message::system(todo_context.clone()));
        log::info!(
            "Created system message with todo list ({} chars)",
            todo_context.len()
        );
    }
}

pub(super) fn strip_existing_todo_list(prompt: &str) -> String {
    strip_existing_prompt_section(prompt, TODO_LIST_MARKER)
}
