//! Session setup helpers for the agent loop runner.

use chrono::Utc;

use super::logging::DebugLogger;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Message, PromptSnapshot, Session};
use bamboo_metrics::MetricsCollector;

pub(crate) mod compaction;
pub(crate) mod legacy_instruction;
pub(crate) mod legacy_skill_history;
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

pub(crate) fn migrate_legacy_workspace_prompt(session: &mut Session) -> bool {
    prompt_setup::migrate_legacy_workspace_prompt(session)
}

async fn publish_pending_workflow_lifecycle_event(
    session: &mut Session,
    config: &AgentLoopConfig,
    event_tx: &tokio::sync::mpsc::Sender<AgentEvent>,
) -> super::Result<()> {
    let Some(event) = session
        .metadata
        .get(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
    else {
        return Ok(());
    };
    // Early #579 builds persisted degradation diagnostics in the lifecycle
    // outbox even though that outbox only has activated/deactivated delivery
    // semantics. Acknowledge that legacy shape without treating the next run as
    // malformed; the structured diagnostic remains durable under activation_error.
    if event["type"].as_str() == Some("workflow.degraded") {
        session
            .metadata
            .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);
        if let Some(persistence) = config.persistence.as_ref() {
            persistence
                .save_runtime_session(session)
                .await
                .map_err(|error| {
                    AgentError::Tool(format!(
                        "workflow degradation acknowledgement could not be persisted: {error}"
                    ))
                })?;
        }
        return Ok(());
    }
    let workflow_id = event["workflow_id"]
        .as_str()
        .ok_or_else(|| {
            AgentError::Tool("pending workflow lifecycle event is malformed".to_string())
        })?
        .to_string();
    let revision = event["revision"].as_u64().ok_or_else(|| {
        AgentError::Tool("pending workflow lifecycle event is malformed".to_string())
    })?;
    let lifecycle_event = match event["type"].as_str() {
        Some("workflow.activated") => AgentEvent::WorkflowActivated {
            event_id: bamboo_skills::workflow_lifecycle_event_id(&session.id, &event),
            session_id: session.id.clone(),
            workflow_id,
            revision,
            invoked_by: event["invoked_by"]
                .as_str()
                .unwrap_or("unknown")
                .to_string(),
        },
        Some("workflow.deactivated") => AgentEvent::WorkflowDeactivated {
            event_id: bamboo_skills::workflow_lifecycle_event_id(&session.id, &event),
            session_id: session.id.clone(),
            workflow_id,
            revision,
        },
        _ => {
            return Err(AgentError::Tool(
                "pending workflow lifecycle event is malformed".to_string(),
            ));
        }
    };
    event_tx.send(lifecycle_event).await.map_err(|_| {
        AgentError::Tool("workflow lifecycle event channel closed before publication".to_string())
    })?;
    session
        .metadata
        .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);
    if let Some(persistence) = config.persistence.as_ref() {
        persistence
            .save_runtime_session(session)
            .await
            .map_err(|error| {
                AgentError::Tool(format!(
                    "workflow lifecycle event acknowledgement could not be persisted: {error}"
                ))
            })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_session_for_loop(
    session: &mut Session,
    initial_message: &str,
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    debug_logger: &DebugLogger,
    must_resume_pinned_activation: bool,
    event_tx: &tokio::sync::mpsc::Sender<AgentEvent>,
) -> super::Result<Option<TaskLoopContext>> {
    // Resume compatibility: recover metadata before any workspace-scoped skill
    // or instruction lookup, then permanently remove the legacy prompt marker.
    migrate_legacy_workspace_prompt(session);
    if config.sdk_skill_execution_host.is_some() {
        // Validate and convert old Instruction history before any old outbox,
        // without resolving a live workflow or changing WorkflowRun state.
        let plan =
            legacy_skill_history::plan_legacy_skill_history(&session.messages, &session.metadata)
                .map_err(AgentError::Tool)?;
        if plan.unsupported.is_none()
            && (plan.message.is_some() || !plan.remove_metadata.is_empty())
        {
            if let (Some(after), Some(message)) = (plan.insert_after, plan.message) {
                session.messages.insert(after + 1, message);
            }
            for key in plan.remove_metadata {
                session.metadata.remove(&key);
            }
            let persistence = config.persistence.as_ref().ok_or_else(|| {
                AgentError::Tool(
                    "SDK legacy history conversion requires runtime persistence".into(),
                )
            })?;
            persistence
                .checkpoint_runtime_session(session)
                .await
                .map_err(|error| {
                    AgentError::Tool(format!(
                        "SDK legacy history conversion checkpoint failed: {error}"
                    ))
                })?;
        }
        // WorkflowRun / Orchestration retains its existing lifecycle outbox.
        let orchestration = session
            .metadata
            .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw).ok())
            .is_some_and(|active| active.kind == bamboo_skills::WorkflowKind::Orchestration);
        if orchestration {
            publish_pending_workflow_lifecycle_event(session, config, event_tx).await?;
        }
    } else {
        publish_pending_workflow_lifecycle_event(session, config, event_tx).await?;
    }
    let skill_context = legacy_instruction::prepare_context(
        session,
        initial_message,
        config,
        session_id,
        debug_logger,
        must_resume_pinned_activation,
    )
    .await?;

    let tool_schemas = tool_schemas::resolve_tool_schemas_for_round(config, tools, session);
    let base_prompt_for_language =
        prompt_setup::resolve_base_prompt_for_language(config, session).to_string();
    let activated = tool_schemas::effective_guide_activation(config, session);
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
    Ok(task_context)
}

#[cfg(test)]
mod tests;
