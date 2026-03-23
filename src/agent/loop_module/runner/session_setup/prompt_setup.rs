use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{Message, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::tools::guide::{context::GuideBuildContext, EnhancedPromptBuilder};
use sha2::{Digest, Sha256};

use super::super::prompt_context::merge_system_prompt_with_contexts;

const RUNTIME_PROMPT_COMPOSER_VERSION: &str = "bamboo.runtime-system-prompt.v1";
const RUNTIME_PROMPT_VERSION_KEY: &str = "runtime_prompt_composer_version";
const RUNTIME_PROMPT_FINGERPRINT_KEY: &str = "runtime_prompt_fingerprint";
const RUNTIME_PROMPT_FLAGS_KEY: &str = "runtime_prompt_component_flags";
const RUNTIME_PROMPT_LENGTHS_KEY: &str = "runtime_prompt_component_lengths";

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
    tracing::info!(
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
    let (base_prompt, merged_prompt) = if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let base_prompt = config
            .system_prompt
            .as_deref()
            .unwrap_or(&system_message.content)
            .to_string();
        let merged_prompt =
            merge_system_prompt_with_contexts(&base_prompt, skill_context, tool_guide_context);
        system_message.content = merged_prompt.clone();
        (base_prompt, merged_prompt)
    } else {
        let base_prompt = config
            .system_prompt
            .as_deref()
            .unwrap_or_default()
            .to_string();
        let merged_prompt =
            merge_system_prompt_with_contexts(&base_prompt, skill_context, tool_guide_context);
        if !merged_prompt.is_empty() {
            session
                .messages
                .insert(0, Message::system(merged_prompt.clone()));
        }
        (base_prompt, merged_prompt)
    };

    persist_runtime_prompt_metadata(
        session,
        base_prompt.as_str(),
        skill_context,
        tool_guide_context,
        merged_prompt.as_str(),
    );
}

fn build_runtime_prompt_fingerprint(
    base_prompt: &str,
    skill_context: &str,
    tool_guide_context: &str,
    final_prompt: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(RUNTIME_PROMPT_COMPOSER_VERSION.as_bytes());
    hasher.update([0u8]);
    hasher.update(base_prompt.as_bytes());
    hasher.update([0u8]);
    hasher.update(skill_context.as_bytes());
    hasher.update([0u8]);
    hasher.update(tool_guide_context.as_bytes());
    hasher.update([0u8]);
    hasher.update(final_prompt.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn persist_runtime_prompt_metadata(
    session: &mut Session,
    base_prompt: &str,
    skill_context: &str,
    tool_guide_context: &str,
    final_prompt: &str,
) {
    session.metadata.insert(
        RUNTIME_PROMPT_VERSION_KEY.to_string(),
        RUNTIME_PROMPT_COMPOSER_VERSION.to_string(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_FINGERPRINT_KEY.to_string(),
        build_runtime_prompt_fingerprint(
            base_prompt,
            skill_context,
            tool_guide_context,
            final_prompt,
        ),
    );

    session.metadata.insert(
        RUNTIME_PROMPT_FLAGS_KEY.to_string(),
        format!(
            "skill={};tool_guide={}",
            (!skill_context.trim().is_empty()) as u8,
            (!tool_guide_context.trim().is_empty()) as u8
        ),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_LENGTHS_KEY.to_string(),
        format!(
            "base={};skill={};tool_guide={};final={}",
            base_prompt.len(),
            skill_context.len(),
            tool_guide_context.len(),
            final_prompt.len()
        ),
    );
}
