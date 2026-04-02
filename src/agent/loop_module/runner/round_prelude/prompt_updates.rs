use crate::agent::core::{Role, Session};

use super::super::prompt_context::{
    inject_external_memory_into_system_message, inject_task_list_into_system_message,
};
use super::super::session_setup::prompt_setup::PromptAssemblyReport;

const CONTEXT_COMPRESSION_PROMPT_START: &str = "<!-- BAMBOO_CONTEXT_COMPRESSION_TOOL_START -->";
const CONTEXT_COMPRESSION_PROMPT_END: &str = "<!-- BAMBOO_CONTEXT_COMPRESSION_TOOL_END -->";
const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";
const TASK_LIST_END_MARKER: &str = "<!-- BAMBOO_TASK_LIST_END -->";
const RUNTIME_PROMPT_FLAGS_KEY: &str = "runtime_prompt_component_flags";
const RUNTIME_PROMPT_LENGTHS_KEY: &str = "runtime_prompt_component_lengths";
const RUNTIME_PROMPT_SECTION_LAYOUT_KEY: &str = "runtime_prompt_section_layout";

pub(super) async fn refresh_round_prompt_context(session: &mut Session) {
    // Load/refresh persistent memory note for this round.
    inject_external_memory_into_system_message(session).await;

    // Inject task list into system message at the start of each round.
    inject_task_list_into_system_message(session);

    let session_id = session.id.clone();
    let prompt_for_metadata = if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))
    {
        system_message.content = strip_context_compression_prompt(&system_message.content);
        Some(system_message.content.clone())
    } else {
        None
    };

    if let Some(prompt) = prompt_for_metadata {
        persist_round_prompt_metadata(session, &prompt);
        log_round_prompt_refresh_summary(session_id.as_str(), &prompt);
    }
}

fn persist_round_prompt_metadata(session: &mut Session, prompt: &str) {
    let sections = build_round_prompt_sections(prompt);
    let report = PromptAssemblyReport::from_sections(sections, prompt);
    session.metadata.insert(
        RUNTIME_PROMPT_FLAGS_KEY.to_string(),
        report.component_flags_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_LENGTHS_KEY.to_string(),
        report.component_lengths_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_SECTION_LAYOUT_KEY.to_string(),
        report.section_layout_value(),
    );
}

fn build_round_prompt_sections(
    prompt: &str,
) -> Vec<crate::agent::loop_module::runner::session_setup::prompt_setup::PromptSection> {
    use crate::agent::loop_module::runner::session_setup::prompt_setup::{
        PromptLayer, PromptSection,
    };

    let external_memory = extract_wrapped_section(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
    .unwrap_or_default();
    let task_list = extract_wrapped_section(prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER)
        .unwrap_or_default();

    vec![
        PromptSection::new("round_base_prompt", PromptLayer::CoreStatic, false, prompt),
        PromptSection::new(
            "external_memory",
            PromptLayer::EnvironmentWorkspace,
            true,
            external_memory,
        ),
        PromptSection::new(
            "task_list",
            PromptLayer::EnvironmentWorkspace,
            true,
            task_list,
        ),
    ]
}

fn extract_wrapped_section(prompt: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start_idx = prompt.find(start_marker)?;
    let section_start = start_idx + start_marker.len();
    let end_rel_idx = prompt[section_start..].find(end_marker)?;
    let section_end = section_start + end_rel_idx;
    let section = prompt[start_idx..section_end + end_marker.len()].trim();
    (!section.is_empty()).then(|| section.to_string())
}

fn log_round_prompt_refresh_summary(session_id: &str, prompt: &str) {
    let external_memory_len = wrapped_section_len(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    );
    let task_list_len = wrapped_section_len(prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER);

    tracing::info!(
        "[{}] Round prompt refresh summary: effective_len={} chars, has_external_memory={}, external_memory_len={}, has_task_list={}, task_list_len={}",
        session_id,
        prompt.len(),
        external_memory_len > 0,
        external_memory_len,
        task_list_len > 0,
        task_list_len,
    );

    tracing::debug!(
        "[{}] ========== EFFECTIVE MODEL SYSTEM PROMPT AFTER ROUND REFRESH ==========",
        session_id
    );
    tracing::debug!("[{}] {}", session_id, prompt);
    tracing::debug!(
        "[{}] ========== END EFFECTIVE MODEL SYSTEM PROMPT AFTER ROUND REFRESH ==========",
        session_id
    );
}

fn wrapped_section_len(prompt: &str, start_marker: &str, end_marker: &str) -> usize {
    let Some(start_idx) = prompt.find(start_marker) else {
        return 0;
    };
    let section_start = start_idx + start_marker.len();
    let Some(end_rel_idx) = prompt[section_start..].find(end_marker) else {
        return 0;
    };
    prompt[section_start..section_start + end_rel_idx]
        .trim()
        .len()
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
