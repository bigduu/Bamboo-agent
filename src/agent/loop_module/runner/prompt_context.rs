//! System prompt context helpers used by the agent loop runner.

mod external_memory;
mod system_sections;
mod task;

pub(crate) use external_memory::{PromptMemoryRuntimeContext, PROMPT_MEMORY_OBSERVABILITY_KEY};

pub(super) async fn inject_external_memory_into_system_message(
    session: &mut crate::agent::core::Session,
    prompt_memory_flags: crate::agent::loop_module::config::PromptMemoryFlags,
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
    session: &mut crate::agent::core::Session,
    memory: &crate::agent::core::memory_store::MemoryStore,
    prompt_memory_flags: crate::agent::loop_module::config::PromptMemoryFlags,
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

pub(super) fn inject_task_list_into_system_message(session: &mut crate::agent::core::Session) {
    task::inject_task_list_into_system_message(session);
}

pub(super) fn strip_existing_task_list(prompt: &str) -> String {
    task::strip_existing_task_list(prompt)
}

#[cfg(test)]
mod tests;
