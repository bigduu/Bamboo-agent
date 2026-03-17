//! Session setup helpers for the agent loop runner.

use chrono::Utc;

use crate::agent::core::tools::{ToolExecutor, ToolSchema};
use crate::agent::core::{Message, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::metrics::MetricsCollector;

mod compaction;
mod prompt_setup;
mod skill_context;
mod tool_schemas;

pub(super) async fn prepare_session_for_loop(
    session: &mut Session,
    initial_message: &str,
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
) -> Option<TodoLoopContext> {
    let skill_context = skill_context::load_skill_context(config, session_id).await;

    let base_prompt_for_language = prompt_setup::resolve_base_prompt_for_language(config, session);
    let tool_schemas = resolve_available_tool_schemas(config, tools);
    let tool_guide_context = prompt_setup::build_tool_guide_context(
        config,
        &tool_schemas,
        base_prompt_for_language,
        session_id,
    );

    prompt_setup::apply_system_prompt_contexts(
        session,
        config,
        &skill_context,
        &tool_guide_context,
    );

    if !config.skip_initial_user_message {
        session.add_message(Message::user(initial_message.to_string()));
        if let Some(metrics) = metrics_collector {
            metrics.session_message_count(
                session_id.to_string(),
                session.messages.len() as u32,
                Utc::now(),
            );
        }
    }

    compaction::compact_oversized_tool_messages(session, config, session_id).await;

    let todo_context = TodoLoopContext::from_session(session);
    if todo_context.is_some() {
        log::debug!("[{}] TodoLoopContext initialized", session_id);
    }
    todo_context
}

pub(super) fn resolve_available_tool_schemas(
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
) -> Vec<ToolSchema> {
    tool_schemas::resolve_available_tool_schemas(config, tools)
}

#[cfg(test)]
mod tests;
