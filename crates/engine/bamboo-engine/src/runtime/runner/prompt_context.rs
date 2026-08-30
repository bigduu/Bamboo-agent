//! System prompt context helpers used by the agent loop runner.

mod external_memory;
mod goal;
mod memory_rerank;
mod plan_mode;
mod plan_runtime;
mod system_sections;
mod task;

pub(crate) use external_memory::{PromptMemoryRuntimeContext, PROMPT_MEMORY_OBSERVABILITY_KEY};
// Only tests reference this through the `prompt_context` re-export, so gate it to
// `cfg(test)` — otherwise the lib-only clippy check flags it as an unused import.
#[cfg(test)]
pub(crate) use external_memory::EXTERNAL_MEMORY_RENDERED_KEY;

pub(crate) async fn refresh_external_memory_context(
    session: &mut bamboo_agent_core::Session,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) {
    external_memory::refresh_external_memory_context(
        session,
        prompt_memory_flags,
        runtime_context,
        project_context_resolver,
    )
    .await;
}

#[cfg(test)]
pub(super) async fn refresh_external_memory_context_with_store(
    session: &mut bamboo_agent_core::Session,
    memory: &bamboo_memory::memory_store::MemoryStore,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) {
    external_memory::refresh_external_memory_context_with_store(
        session,
        memory,
        prompt_memory_flags,
        runtime_context,
    )
    .await;
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) async fn refresh_external_memory_context_with_store_and_resolver(
    session: &mut bamboo_agent_core::Session,
    memory: &bamboo_memory::memory_store::MemoryStore,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) {
    external_memory::refresh_external_memory_context_with_store_and_resolver(
        session,
        memory,
        prompt_memory_flags,
        runtime_context,
        project_context_resolver,
    )
    .await;
}

pub(super) fn strip_existing_external_memory(prompt: &str) -> String {
    external_memory::strip_existing_external_memory(prompt)
}

/// The rendered external-memory section for this round, read from the session
/// field the async refresh populates (`None` when there is none). Built into a
/// volatile `ExternalMemory` block by the request assembler.
pub(crate) fn render_external_memory_section(
    session: &bamboo_agent_core::Session,
) -> Option<String> {
    external_memory::rendered_external_memory_section(session)
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

/// Render the session-goal section body (for the volatile [`GoalState`] block).
pub(crate) fn render_goal_section(objective: &str) -> String {
    goal::render_goal_section(objective)
}

/// Strip a legacy goal block from a (possibly persisted) system prompt.
pub(super) fn strip_existing_goal(prompt: &str) -> String {
    goal::strip_existing_goal(prompt)
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

/// Render the plan-mode section text from session state (`None` when inactive).
/// The agent loop builds this into a volatile `PlanModeState` block rather than
/// injecting it into the system message.
pub(crate) fn render_plan_mode_section(session: &bamboo_agent_core::Session) -> Option<String> {
    plan_mode::render_plan_mode_section(session)
}

/// Render the durable plan-execution context text from session state plus
/// persisted plan artifacts (`None` when plan mode is inactive). Built into a
/// volatile `PlanRuntimeState` block.
pub(crate) fn render_plan_runtime_section(
    session: &bamboo_agent_core::Session,
    app_data_dir: Option<&std::path::Path>,
) -> Option<String> {
    plan_runtime::build_plan_runtime_context(session, app_data_dir)
}

#[cfg(test)]
mod tests;
