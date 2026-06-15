//! System prompt context helpers used by the agent loop runner.

mod external_memory;
mod goal;
mod plan_mode;
mod plan_runtime;
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

pub(super) fn append_core_agent_directives(base_prompt: &str, directives: &str) -> String {
    system_sections::append_core_agent_directives(base_prompt, directives)
}

pub(super) fn strip_existing_core_directives(prompt: &str) -> String {
    system_sections::strip_existing_core_directives(prompt)
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

pub(crate) fn inject_goal_into_system_message(
    session: &mut bamboo_agent_core::Session,
    goal: Option<&str>,
) {
    goal::inject_goal_into_system_message(session, goal);
}

pub(super) fn strip_existing_task_list(prompt: &str) -> String {
    task::strip_existing_task_list(prompt)
}

pub(super) fn strip_existing_plan_mode_instructions(prompt: &str) -> String {
    plan_mode::strip_existing_plan_mode_instructions(prompt)
}

pub(super) fn strip_existing_plan_runtime_context(prompt: &str) -> String {
    plan_runtime::strip_existing_plan_runtime_context(prompt)
}

pub(crate) fn inject_plan_mode_instructions(session: &mut bamboo_agent_core::Session) {
    plan_mode::inject_plan_mode_instructions(session);
}

pub(crate) fn inject_plan_runtime_context_into_system_message(
    session: &mut bamboo_agent_core::Session,
    app_data_dir: Option<&std::path::Path>,
) {
    plan_runtime::inject_plan_runtime_context_into_system_message(session, app_data_dir);
}

#[cfg(test)]
mod tests;
