use crate::agent::core::{ExternalMemory, Session};

use super::system_sections::strip_existing_prompt_block;

const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
const EXTERNAL_MEMORY_PROMPT_MAX_CHARS: usize = 4_000;
const EXTERNAL_MEMORY_TOOL_NAME: &str = "memory_note";

pub(super) fn strip_existing_external_memory(prompt: &str) -> String {
    strip_existing_prompt_block(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
}

fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    for (count, ch) in value.chars().enumerate() {
        if count >= max_chars {
            return (out, true);
        }
        out.push(ch);
    }
    (out, false)
}

pub(super) async fn inject_external_memory_into_system_message(session: &mut Session) {
    let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    else {
        return;
    };

    // Remove any previously injected memory block, then re-append a fresh
    // memory section for this round.
    let base_prompt = strip_existing_external_memory(&system_message.content);

    let memory = ExternalMemory::with_defaults();
    let note = match memory.read_note(session.id.as_str()).await {
        Ok(Some(content)) => content,
        Ok(None) => String::new(),
        Err(error) => {
            log::warn!(
                "[{}] Failed to read external memory note: {}",
                session.id,
                error
            );
            String::new()
        }
    };

    let note = note.trim().to_string();
    let note_len = note.chars().count();
    let (note_snippet, truncated) = truncate_chars(&note, EXTERNAL_MEMORY_PROMPT_MAX_CHARS);
    let note_for_prompt = if note_snippet.trim().is_empty() {
        "_(empty)_".to_string()
    } else {
        note_snippet
    };

    let mut section = String::new();
    section.push_str("\n\n");
    section.push_str(EXTERNAL_MEMORY_START_MARKER);
    section.push('\n');
    section.push_str("## External Memory (Persistent)\n\n");
    section.push_str("You have access to a persistent per-session memory note.\n");
    section.push_str("- If you learn durable information that will help later (preferences, key decisions, constraints, environment details), update the note using the tool.\n");
    section.push_str("- Do NOT store secrets/tokens.\n");
    section.push_str("- Keep the note concise and factual. If it gets too long, compress it (rewrite a shorter version) and replace it.\n\n");
    section.push_str("Tool usage:\n");
    section.push_str(&format!(
        "- Append: call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"append\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Replace (for compression): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"replace\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Read full note (if truncated): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"read\"}}`\n\n"
    ));

    section.push_str("### Current Note (markdown)\n");
    // Use a 4-backtick fence to reduce collisions with markdown code fences inside the note itself.
    section.push_str("````md\n");
    section.push_str(&note_for_prompt);
    section.push_str("\n````\n");

    if truncated {
        section.push_str(&format!(
            "\nNote is truncated in the system prompt (showing first {} chars of {}). Use `{}` action=read to view it and then action=replace to compress it.\n",
            EXTERNAL_MEMORY_PROMPT_MAX_CHARS, note_len, EXTERNAL_MEMORY_TOOL_NAME
        ));
    }
    section.push('\n');
    section.push_str(EXTERNAL_MEMORY_END_MARKER);

    system_message.content = format!("{}{}", base_prompt.trim_end(), section);
}
