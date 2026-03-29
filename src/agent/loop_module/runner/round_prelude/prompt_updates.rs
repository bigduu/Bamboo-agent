use crate::agent::core::{Role, Session};

use super::super::prompt_context::{
    inject_external_memory_into_system_message, inject_task_list_into_system_message,
};

const CONTEXT_COMPRESSION_PROMPT_START: &str = "<!-- BAMBOO_CONTEXT_COMPRESSION_TOOL_START -->";
const CONTEXT_COMPRESSION_PROMPT_END: &str = "<!-- BAMBOO_CONTEXT_COMPRESSION_TOOL_END -->";

pub(super) async fn refresh_round_prompt_context(session: &mut Session) {
    // Load/refresh persistent memory note for this round.
    inject_external_memory_into_system_message(session).await;

    // Inject task list into system message at the start of each round.
    inject_task_list_into_system_message(session);

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))
    {
        system_message.content = strip_context_compression_prompt(&system_message.content);
    }
}

fn strip_context_compression_prompt(prompt: &str) -> String {
    let mut current = prompt.to_string();
    loop {
        let Some(start_idx) = current.find(CONTEXT_COMPRESSION_PROMPT_START) else {
            break;
        };
        let search_from = start_idx + CONTEXT_COMPRESSION_PROMPT_START.len();
        let Some(end_rel_idx) = current[search_from..].find(CONTEXT_COMPRESSION_PROMPT_END) else {
            break;
        };
        let end_idx = search_from + end_rel_idx + CONTEXT_COMPRESSION_PROMPT_END.len();

        let before = current[..start_idx].trim_end();
        let after = current[end_idx..].trim_start();
        current = match (before.is_empty(), after.is_empty()) {
            (true, true) => String::new(),
            (true, false) => after.to_string(),
            (false, true) => before.to_string(),
            (false, false) => format!("{before}\n\n{after}"),
        };
    }
    current
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_context_compression_prompt_removes_embedded_block_cleanly() {
        let prompt = format!(
            "Header\n\n{}\ncompression hint\n{}\n\nFooter",
            CONTEXT_COMPRESSION_PROMPT_START, CONTEXT_COMPRESSION_PROMPT_END
        );
        let stripped = strip_context_compression_prompt(&prompt);
        assert_eq!(stripped, "Header\n\nFooter");
    }

    #[test]
    fn strip_context_compression_prompt_removes_multiple_blocks() {
        let prompt = format!(
            "A\n\n{}\none\n{}\n\nB\n\n{}\ntwo\n{}\n\nC",
            CONTEXT_COMPRESSION_PROMPT_START,
            CONTEXT_COMPRESSION_PROMPT_END,
            CONTEXT_COMPRESSION_PROMPT_START,
            CONTEXT_COMPRESSION_PROMPT_END
        );
        let stripped = strip_context_compression_prompt(&prompt);
        assert_eq!(stripped, "A\n\nB\n\nC");
    }

    #[test]
    fn strip_context_compression_prompt_keeps_plain_prompt_unchanged() {
        let prompt = "You are Bamboo.";
        let stripped = strip_context_compression_prompt(prompt);
        assert_eq!(stripped, prompt);
    }
}
