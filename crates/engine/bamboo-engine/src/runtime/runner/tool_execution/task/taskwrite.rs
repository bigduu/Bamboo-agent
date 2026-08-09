use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_tools::TaskTool;

pub(super) async fn maybe_handle_taskwrite(
    tool_call: &ToolCall,
    result: &ToolResult,
    session: &mut Session,
    session_id: &str,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    task_context: &mut Option<TaskLoopContext>,
) {
    if tool_call.function.name != "Task" || !result.success {
        return;
    }

    let Ok(args) = serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments) else {
        return;
    };
    let shared_session_id = match session.kind {
        SessionKind::Child => session.root_session_id.clone(),
        SessionKind::Root => session.id.clone(),
    };

    let existing_task_list = session.task_list.as_ref();
    let is_plan_mode = session
        .agent_runtime_state
        .as_ref()
        .and_then(|s| s.plan_mode.as_ref())
        .is_some();
    let default_phase = is_plan_mode.then_some(bamboo_domain::TaskPhase::Planning);

    let Ok(task_list) = TaskTool::task_list_from_args_with_existing(
        &args,
        &shared_session_id,
        existing_task_list,
        default_phase,
    ) else {
        return;
    };

    // Keep the executing session in sync so this run's prompt context remains coherent.
    session.set_task_list(task_list.clone());
    let next_version = task_context
        .as_ref()
        .map(|ctx| ctx.version.saturating_add(1))
        .or_else(|| {
            session
                .task_list_version_meta()
                .and_then(|value| value.parse::<u64>().ok())
                .map(|value| value.saturating_add(1))
        })
        .unwrap_or(1);
    session.set_task_list_version_meta(next_version.to_string());
    tracing::info!(
        "[{}] Task updated shared task list '{}' with {} items",
        session_id,
        task_list.title,
        task_list.items.len()
    );

    persist_shared_task_list(config, session, &shared_session_id, session_id, &task_list).await;

    let _ = event_tx
        .send(AgentEvent::TaskListUpdated {
            task_list: task_list.clone(),
        })
        .await;

    reinitialize_task_context(task_context, session, session_id);
}

async fn persist_shared_task_list(
    config: &AgentLoopConfig,
    session: &mut Session,
    shared_session_id: &str,
    session_id: &str,
    task_list: &bamboo_domain::TaskList,
) {
    let Some(ref persistence) = config.persistence else {
        return;
    };

    if let Err(error) = persistence.save_runtime_control_plane(session).await {
        tracing::warn!(
            "[{}] Failed to save session control-plane after Task update: {}",
            session_id,
            error
        );
    } else {
        tracing::debug!(
            "[{}] Session control-plane saved after Task update",
            session_id
        );
    }

    if shared_session_id == session.id {
        return;
    }

    let Some(version) = session.task_list_version_meta() else {
        tracing::warn!(
            "[{}] Shared root Task update on {} has no task-list version",
            session_id,
            shared_session_id
        );
        return;
    };

    match persistence
        .update_task_list_control_plane(shared_session_id, task_list, &version)
        .await
    {
        Ok(true) => {
            tracing::debug!(
                "[{}] Shared root Task control-plane patched on {}",
                session_id,
                shared_session_id
            );
        }
        Ok(false) => {
            // Save-only legacy/custom persisters leave the default load hook at
            // `None`, so the default atomic patch cannot find the root. Retain
            // source-compatible behavior with an explicitly full snapshot and
            // full save; this path must never feed a message-free sidecar
            // snapshot into a full-save fallback.
            let Some(ref storage) = config.storage else {
                tracing::warn!(
                    "[{}] Root session {} unavailable through persistence and no storage fallback is configured",
                    session_id,
                    shared_session_id
                );
                return;
            };
            let mut root_session = match storage.load_session(shared_session_id).await {
                Ok(Some(root_session)) => root_session,
                Ok(None) => {
                    tracing::warn!(
                        "[{}] Root session {} not found while syncing shared task list",
                        session_id,
                        shared_session_id
                    );
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        "[{}] Failed to load root session {} through storage fallback: {}",
                        session_id,
                        shared_session_id,
                        error
                    );
                    return;
                }
            };
            root_session.set_task_list(task_list.clone());
            root_session.set_task_list_version_meta(version);
            if let Err(error) = persistence.save_runtime_session(&mut root_session).await {
                tracing::warn!(
                    "[{}] Failed to full-save shared root Task fallback on {}: {}",
                    session_id,
                    shared_session_id,
                    error
                );
            } else {
                tracing::debug!(
                    "[{}] Shared root Task full-save fallback completed on {}",
                    session_id,
                    shared_session_id
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                "[{}] Failed to patch shared root Task control-plane on {}: {}",
                session_id,
                shared_session_id,
                error
            );
        }
    }
}

fn reinitialize_task_context(
    task_context: &mut Option<TaskLoopContext>,
    session: &Session,
    session_id: &str,
) {
    // IMPORTANT: Re-initialize TaskLoopContext from session.
    *task_context = TaskLoopContext::from_session(session);
    if let Some(ctx) = task_context.as_mut() {
        // Mark the list dirty so the end-of-turn gate spawns exactly one
        // evaluation for this Task-tool write. This is the only place the flag is
        // set; it is cleared when the evaluation is spawned.
        ctx.task_list_dirty = true;
        tracing::debug!("[{}] TaskLoopContext re-initialized after Task", session_id);
    }
}
