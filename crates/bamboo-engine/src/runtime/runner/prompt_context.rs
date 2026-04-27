//! System prompt context helpers used by the agent loop runner.

mod external_memory;
mod plan_mode;
mod system_sections;
mod task;

pub(crate) use external_memory::{PromptMemoryRuntimeContext, PROMPT_MEMORY_OBSERVABILITY_KEY};

pub(crate) async fn inject_external_memory_into_system_message(
    session: &mut bamboo_agent_core::Session,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) {
    external_memory::inject_external_memory_into_system_message(
        session,
        prompt_memory_flags,
        runtime_context,
    )
    .await;
}

#[cfg(test)]
pub(super) async fn inject_external_memory_into_system_message_with_store(
    session: &mut bamboo_agent_core::Session,
    memory: &bamboo_memory::memory_store::MemoryStore,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) {
    external_memory::inject_external_memory_into_system_message_with_store(
        session,
        memory,
        prompt_memory_flags,
        runtime_context,
    )
    .await;
}

pub(super) fn strip_existing_external_memory(prompt: &str) -> String {
    external_memory::strip_existing_external_memory(prompt)
}

pub(super) fn merge_system_prompt_with_contexts(
    base_prompt: &str,
    skill_context: &str,
    tool_guide_context: &str,
) -> String {
    system_sections::merge_system_prompt_with_contexts(
        base_prompt,
        skill_context,
        tool_guide_context,
    )
}

pub(super) fn strip_existing_skill_context(prompt: &str) -> String {
    system_sections::strip_existing_skill_context(prompt)
}

pub(super) fn strip_existing_tool_guide_context(prompt: &str) -> String {
    system_sections::strip_existing_tool_guide_context(prompt)
}

pub(super) fn strip_existing_env_context(prompt: &str) -> String {
    system_sections::strip_existing_env_context(prompt)
}

pub(crate) fn inject_task_list_into_system_message(session: &mut bamboo_agent_core::Session) {
    task::inject_task_list_into_system_message(session);
}

pub(super) fn strip_existing_task_list(prompt: &str) -> String {
    task::strip_existing_task_list(prompt)
}

pub(crate) fn inject_plan_mode_instructions(session: &mut bamboo_agent_core::Session) {
    plan_mode::inject_plan_mode_instructions(session);
}

#[cfg(test)]
mod tests;
