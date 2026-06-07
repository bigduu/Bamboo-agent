//! Session setup helpers for the agent loop runner.

use chrono::Utc;

use bamboo_metrics::MetricsCollector;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_skills::runtime_metadata::{
    SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY, SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY,
    SKILL_RUNTIME_SELECTION_COUNT_KEY, SKILL_RUNTIME_SELECTION_SOURCE_KEY,
    SKILL_RUNTIME_SELECTION_TRACE_KEY,
};
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{Message, PromptSnapshot, Session};
use bamboo_tools::exposure::activated_discoverable_tools;

use super::logging::DebugLogger;

pub(crate) mod compaction;
pub(crate) mod prompt_envelope;
pub(crate) mod prompt_setup;
pub(crate) mod skill_context;
pub(crate) mod tool_schemas;

pub fn read_prompt_snapshot(session: &Session) -> Option<PromptSnapshot> {
    prompt_setup::read_prompt_snapshot_metadata(session)
}

pub fn refresh_prompt_snapshot(session: &mut Session) {
    prompt_setup::refresh_prompt_snapshot_from_session(session)
}

pub(crate) async fn prepare_session_for_loop(
    session: &mut Session,
    initial_message: &str,
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    debug_logger: &DebugLogger,
) -> Option<TaskLoopContext> {
    let skill_result = skill_context::load_skill_context(config, session_id, initial_message).await;
    let skill_context = skill_result.context.clone();

    if let Some(source) = skill_result.selection_source.as_deref() {
        debug_logger.log_event(
            session_id,
            "skill_selection_runtime_state",
            serde_json::json!({
                "source": source,
                "selected_skill_ids": skill_result.selected_skill_ids,
                "selected_skill_mode": skill_result.selected_skill_mode,
                "request_hint_present": skill_result.request_hint_present
            }),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
            source.to_string(),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_COUNT_KEY.to_string(),
            skill_result.selected_skill_ids.len().to_string(),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
            serde_json::to_string(&skill_result.selected_skill_ids).unwrap_or("[]".to_string()),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_TRACE_KEY.to_string(),
            serde_json::json!({
                "source": source,
                "selected_skill_ids": skill_result.selected_skill_ids,
                "selected_skill_mode": skill_result.selected_skill_mode,
                "request_hint_present": skill_result.request_hint_present
            })
            .to_string(),
        );
        if let Some(mode) = skill_result.selected_skill_mode.as_ref() {
            session.metadata.insert(
                SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY.to_string(),
                mode.clone(),
            );
        }
    }

    let tool_schemas =
        tool_schemas::resolve_available_tool_schemas_for_session(config, tools, session);
    let base_prompt_for_language =
        prompt_setup::resolve_base_prompt_for_language(config, session).to_string();
    let activated = activated_discoverable_tools(session);
    let tool_guide_context = prompt_setup::build_tool_guide_context(
        config,
        &tool_schemas,
        &base_prompt_for_language,
        session_id,
        &activated,
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
