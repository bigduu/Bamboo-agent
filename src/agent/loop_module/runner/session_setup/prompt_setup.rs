use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{Message, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::tools::guide::{context::GuideBuildContext, EnhancedPromptBuilder};

use super::super::prompt_context::merge_system_prompt_with_contexts;

pub(super) fn resolve_base_prompt_for_language<'a>(
    config: &'a AgentLoopConfig,
    session: &'a Session,
) -> &'a str {
    config
        .system_prompt
        .as_deref()
        .or_else(|| {
            session
                .messages
                .iter()
                .find(|message| matches!(message.role, crate::agent::core::Role::System))
                .map(|message| message.content.as_str())
        })
        .unwrap_or_default()
}

pub(super) fn build_tool_guide_context(
    config: &AgentLoopConfig,
    tool_schemas: &[ToolSchema],
    base_prompt_for_language: &str,
    session_id: &str,
) -> String {
    let guide_context = GuideBuildContext::from_system_prompt(base_prompt_for_language);
    let tool_guide_context = EnhancedPromptBuilder::build(
        Some(config.tool_registry.as_ref()),
        tool_schemas,
        &guide_context,
    );
    log::info!(
        "[{}] Tool guide context built, length: {} chars",
        session_id,
        tool_guide_context.len()
    );
    tool_guide_context
}

pub(super) fn apply_system_prompt_contexts(
    session: &mut Session,
    config: &AgentLoopConfig,
    skill_context: &str,
    tool_guide_context: &str,
) {
    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let base_prompt = config
            .system_prompt
            .as_deref()
            .unwrap_or(&system_message.content);
        system_message.content =
            merge_system_prompt_with_contexts(base_prompt, skill_context, tool_guide_context);
    } else {
        let base_prompt = config.system_prompt.as_deref().unwrap_or_default();
        let merged_prompt =
            merge_system_prompt_with_contexts(base_prompt, skill_context, tool_guide_context);
        if !merged_prompt.is_empty() {
            session.messages.insert(0, Message::system(merged_prompt));
        }
    }
}
