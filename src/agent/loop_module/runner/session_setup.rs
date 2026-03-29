//! Session setup helpers for the agent loop runner.

use chrono::Utc;

use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{Message, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::metrics::MetricsCollector;

mod compaction;
pub(super) mod prompt_setup;
mod skill_context;
pub(super) mod tool_schemas;

pub(super) async fn prepare_session_for_loop(
    session: &mut Session,
    initial_message: &str,
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
) -> Option<TaskLoopContext> {
    let skill_context =
        skill_context::load_skill_context(config, session_id, initial_message).await;

    let tool_schemas =
        tool_schemas::resolve_available_tool_schemas_for_session(config, tools, session);
    let base_prompt_for_language =
        prompt_setup::resolve_base_prompt_for_language(config, session).to_string();
    let tool_guide_context = prompt_setup::build_tool_guide_context(
        config,
        &tool_schemas,
        &base_prompt_for_language,
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

    let task_context = TaskLoopContext::from_session(session);
    if task_context.is_some() {
        tracing::debug!("[{}] TaskLoopContext initialized", session_id);
    }
    task_context
}

#[cfg(test)]
mod tests;
