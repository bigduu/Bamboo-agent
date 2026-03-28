use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{Message, Role, Session};
use crate::agent::loop_module::config::AgentLoopConfig;

use super::super::prompt_context::{
    inject_external_memory_into_system_message, inject_task_list_into_system_message,
};
use super::super::session_setup::tool_schemas::resolve_available_tool_schemas_for_session;

const CONTEXT_COMPRESSION_PROMPT_START: &str = "<!-- BAMBOO_CONTEXT_COMPRESSION_TOOL_START -->";
const CONTEXT_COMPRESSION_PROMPT_END: &str = "<!-- BAMBOO_CONTEXT_COMPRESSION_TOOL_END -->";
const CONTEXT_COMPRESSION_ENABLED_KEY: &str = "context_compression_tool_enabled";
const CONTEXT_COMPRESSION_TRIGGER_PCT_KEY: &str = "context_compression_tool_trigger_pct";
const CONTEXT_COMPRESSION_USAGE_PCT_KEY: &str = "context_compression_tool_usage_pct";

pub(super) async fn refresh_round_prompt_context(
    session: &mut Session,
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
    session_id: &str,
) {
    // Load/refresh persistent memory note for this round.
    inject_external_memory_into_system_message(session).await;

    // Inject task list into system message at the start of each round.
    inject_task_list_into_system_message(session);

    let _ = session_id;
    let _ = resolve_available_tool_schemas_for_session(config, tools, Some(session));

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))
    {
        let updated = upsert_context_compression_prompt(
            &system_message.content,
            session
                .metadata
                .get(CONTEXT_COMPRESSION_ENABLED_KEY)
                .map(|value| value == "true")
                .unwrap_or(false),
            session
                .metadata
                .get(CONTEXT_COMPRESSION_USAGE_PCT_KEY)
                .map(String::as_str),
            session
                .metadata
                .get(CONTEXT_COMPRESSION_TRIGGER_PCT_KEY)
                .map(String::as_str),
        );
        system_message.content = updated;
    } else if session
        .metadata
        .get(CONTEXT_COMPRESSION_ENABLED_KEY)
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        session.messages.insert(
            0,
            Message::system(build_context_compression_prompt(
                session
                    .metadata
                    .get(CONTEXT_COMPRESSION_USAGE_PCT_KEY)
                    .map(String::as_str),
                session
                    .metadata
                    .get(CONTEXT_COMPRESSION_TRIGGER_PCT_KEY)
                    .map(String::as_str),
            )),
        );
    }
}

fn upsert_context_compression_prompt(
    prompt: &str,
    enabled: bool,
    usage_pct: Option<&str>,
    trigger_pct: Option<&str>,
) -> String {
    let base = strip_context_compression_prompt(prompt);
    if !enabled {
        return base;
    }

    let section = build_context_compression_prompt(usage_pct, trigger_pct);
    if base.trim().is_empty() {
        section
    } else {
        format!("{}\n\n{}", base.trim_end(), section)
    }
}

fn build_context_compression_prompt(usage_pct: Option<&str>, trigger_pct: Option<&str>) -> String {
    let usage = usage_pct.unwrap_or("?");
    let trigger = trigger_pct.unwrap_or("?");
    let critical_usage = usage_pct
        .and_then(|value| value.parse::<f64>().ok())
        .is_some_and(|value| value >= 95.0);
    let critical_line = if critical_usage {
        "- Active context usage is critically high (>=95%). Call `compress_context` now before continuing with additional multi-step analysis or tool calls.\n"
    } else {
        ""
    };
    format!(
        "{CONTEXT_COMPRESSION_PROMPT_START}\n## Context Compression Tool\n- The tool `compress_context` is available in this round because active context usage is around {usage}% and the compression trigger is {trigger}%.\n{critical_line}- Call `compress_context` only when you have reached a stable checkpoint: for example after finishing a subtask, after collecting enough evidence, or before a long next phase that needs more context budget.\n- Do not call it immediately after every message. Prefer to compress older history once you can preserve the important state in a summary.\n- Host fallback: when active context usage reaches 98% or higher, compression will be forced automatically. Prefer calling `compress_context` proactively in the 90%-97% range at a stable checkpoint.\n- After calling it, continue using the refreshed compact context.\n{CONTEXT_COMPRESSION_PROMPT_END}"
    )
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
    fn upsert_context_compression_prompt_does_not_inject_when_disabled() {
        let base = "You are Bamboo.";
        let updated = upsert_context_compression_prompt(base, false, Some("83"), Some("80"));
        assert_eq!(updated, base);
        assert!(!updated.contains(CONTEXT_COMPRESSION_PROMPT_START));
        assert!(!updated.contains("compress_context"));
    }

    #[test]
    fn upsert_context_compression_prompt_injects_guidance_when_enabled() {
        let base = "You are Bamboo.";
        let updated = upsert_context_compression_prompt(base, true, Some("83"), Some("80"));
        assert!(updated.contains(CONTEXT_COMPRESSION_PROMPT_START));
        assert!(updated.contains(CONTEXT_COMPRESSION_PROMPT_END));
        assert!(updated.contains("compress_context"));
        assert!(updated.contains("83%"));
        assert!(updated.contains("80%"));
        assert!(updated.contains("stable checkpoint"));
        assert!(updated.contains("98%"));
    }

    #[test]
    fn upsert_context_compression_prompt_adds_critical_line_at_high_usage() {
        let base = "You are Bamboo.";
        let updated = upsert_context_compression_prompt(base, true, Some("96.4"), Some("80"));
        assert!(updated.contains("critically high"));
        assert!(updated.contains("Call `compress_context` now"));
    }

    #[test]
    fn upsert_context_compression_prompt_replaces_existing_block_instead_of_stacking() {
        let existing = format!(
            "System base.\n\n{}\nold guidance\n{}",
            CONTEXT_COMPRESSION_PROMPT_START, CONTEXT_COMPRESSION_PROMPT_END
        );
        let updated = upsert_context_compression_prompt(&existing, true, Some("91"), Some("80"));

        assert_eq!(updated.matches(CONTEXT_COMPRESSION_PROMPT_START).count(), 1);
        assert_eq!(updated.matches(CONTEXT_COMPRESSION_PROMPT_END).count(), 1);
        assert!(updated.contains("91%"));
        assert!(!updated.contains("old guidance"));
    }

    #[test]
    fn strip_context_compression_prompt_removes_embedded_block_cleanly() {
        let prompt = format!(
            "Header\n\n{}\ncompression hint\n{}\n\nFooter",
            CONTEXT_COMPRESSION_PROMPT_START, CONTEXT_COMPRESSION_PROMPT_END
        );
        let stripped = strip_context_compression_prompt(&prompt);
        assert_eq!(stripped, "Header\n\nFooter");
    }
}
