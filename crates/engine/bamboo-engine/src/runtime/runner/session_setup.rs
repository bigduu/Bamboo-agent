//! Session setup helpers for the agent loop runner.

use chrono::Utc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Message, PromptSnapshot, Session};
use bamboo_metrics::MetricsCollector;
use bamboo_skills::runtime_metadata::{
    SKILL_RUNTIME_ACTIVATION_ERROR_KEY, SKILL_RUNTIME_ACTIVATION_GENERATION_KEY,
    SKILL_RUNTIME_PINNED_SNAPSHOT_KEY, SKILL_RUNTIME_SELECTED_CATALOG_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY, SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY, SKILL_RUNTIME_SELECTION_COUNT_KEY,
    SKILL_RUNTIME_SELECTION_SOURCE_KEY, SKILL_RUNTIME_SELECTION_TRACE_KEY,
};
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
    publish_pending_workflow_lifecycle_event(session, config, event_tx).await?;
    let skill_result = match skill_context::load_skill_context(
        config,
        session,
        session_id,
        initial_message,
        must_resume_pinned_activation,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            session.metadata.insert(
                SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
                error.clone(),
            );
            if let Some(persistence) = config.persistence.as_ref() {
                if let Err(save_error) = persistence.save_runtime_session(session).await {
                    tracing::warn!(
                        "[{}] Failed to persist workflow activation setup error: {}",
                        session_id,
                        save_error
                    );
                }
            }
            return Err(AgentError::Tool(format!(
                "Workflow activation failed before model execution: {error}"
            )));
        }
    };
    session.metadata.remove(SKILL_RUNTIME_ACTIVATION_ERROR_KEY);
    let explicit_activation_loaded = skill_result.restored_active_context
        || skill_context::selection_matches_loaded_activation(session, &skill_result);
    let skill_context = if explicit_activation_loaded {
        format!(
            "\n\n## Explicit Workflow Already Activated\n\
The `{skill_id}` workflow was loaded successfully earlier in this session. Continue following the existing `load_skill` result and its workflow instructions. Do not call `load_skill` again solely because execution resumed.\n",
            skill_id = skill_result.selected_skill_ids[0],
        )
    } else if skill_result.selection_source.as_deref() == Some("explicit")
        && skill_result.selected_skill_ids.len() == 1
    {
        format!(
            "\n\n## Required Explicit Workflow Activation\n\
The user explicitly selected `{skill_id}`. Your first response step MUST be exactly one `load_skill` call for `{skill_id}`. Do not emit commentary, an answer, or any other tool call before it completes. If the tool reports `activation_status: degraded`, do not retry it; continue without workflow instructions. Otherwise, follow the loaded workflow instructions.\n{context}",
            skill_id = skill_result.selected_skill_ids[0],
            context = skill_result.context.as_str(),
        )
    } else {
        skill_result.context.clone()
    };

    if let Some(diagnostic) = skill_result.activation_diagnostic.as_ref() {
        session.metadata.insert(
            SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
            serde_json::to_string(diagnostic).unwrap_or_else(|_| diagnostic.message.clone()),
        );
        session
            .metadata
            .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);
        if let Some(persistence) = config.persistence.as_ref() {
            persistence
                .save_runtime_session(session)
                .await
                .map_err(|error| {
                    AgentError::Tool(format!(
                        "Workflow degraded state could not be persisted: {error}"
                    ))
                })?;
        }
    }

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
            SKILL_RUNTIME_SELECTED_CATALOG_KEY.to_string(),
            serde_json::to_string(&skill_result.catalog_entries)
                .unwrap_or_else(|_| "[]".to_string()),
        );
        session.metadata.insert(
            bamboo_skills::WORKFLOW_CATALOG_DIAGNOSTIC_METADATA_KEY.to_string(),
            serde_json::to_string(&skill_result.catalog_diagnostic)
                .unwrap_or_else(|_| "null".to_string()),
        );
        if let Some(snapshot) = skill_result.durable_snapshot.as_ref() {
            session.metadata.insert(
                SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(),
                serde_json::to_string(snapshot).map_err(|_| {
                    AgentError::Tool(
                        "Workflow activation snapshot could not be serialized".to_string(),
                    )
                })?,
            );
        } else {
            // A metadata-only automatic catalog must not inherit an older
            // candidate resource snapshot from a prior selection.
            session.metadata.remove(SKILL_RUNTIME_PINNED_SNAPSHOT_KEY);
        }
        session.metadata.insert(
            SKILL_RUNTIME_SELECTION_COUNT_KEY.to_string(),
            skill_result.selected_skill_ids.len().to_string(),
        );
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
            serde_json::to_string(&skill_result.selected_skill_ids).unwrap_or("[]".to_string()),
        );
        if let Some(revision) = skill_result.catalog_revision {
            session.metadata.insert(
                SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
                revision.to_string(),
            );
        } else {
            session
                .metadata
                .remove(SKILL_RUNTIME_ACTIVATION_GENERATION_KEY);
        }
        session.metadata.insert(
            SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
            serde_json::to_string(&skill_result.skill_revisions).unwrap_or("{}".to_string()),
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
        } else {
            session
                .metadata
                .remove(SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY);
        }

        if !skill_result.restored_active_context {
            skill_context::reset_activation_state_for_new_selection(session, &skill_result);
        }

        // Runtime tools authorize skill loads through the shared session repository.
        // Publish this run's resolved IDs before the first model/tool call so they
        // never observe a missing or previous-run allowlist from the cache.
        if let Some(persistence) = config.persistence.as_ref() {
            if let Err(error) = persistence.save_runtime_session(session).await {
                if let Some(skill_manager) = config.skill_manager.as_ref() {
                    let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
                    let _ = skill_manager
                        .release_activation_for_workspace(session_id, workspace.as_deref())
                        .await;
                }
                return Err(AgentError::Tool(format!(
                    "Workflow activation metadata could not be published before tool/model execution: {error}"
                )));
            }
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
    Ok(task_context)
}

#[cfg(test)]
mod tests;
