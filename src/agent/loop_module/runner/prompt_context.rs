//! System prompt context helpers used by the agent loop runner.

mod external_memory;
mod system_sections;
mod task;

pub(super) async fn inject_external_memory_into_system_message(
    session: &mut crate::agent::core::Session,
) {
    external_memory::inject_external_memory_into_system_message(session).await;
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

pub(super) fn inject_task_list_into_system_message(session: &mut crate::agent::core::Session) {
    task::inject_task_list_into_system_message(session);
}

pub(super) fn strip_existing_task_list(prompt: &str) -> String {
    task::strip_existing_task_list(prompt)
}

#[cfg(test)]
mod tests;
