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
                .metadata
                .get("task_list_version")
                .and_then(|value| value.parse::<u64>().ok())
                .map(|value| value.saturating_add(1))
        })
        .unwrap_or(1);
    session
        .metadata
        .insert("task_list_version".to_string(), next_version.to_string());
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

pub(in crate::runtime::runner) async fn persist_shared_task_list(
    config: &AgentLoopConfig,
    session: &mut Session,
    shared_session_id: &str,
    session_id: &str,
    task_list: &bamboo_domain::TaskList,
) {
    if let Some(ref storage) = config.storage {
        if let Some(ref persistence) = config.persistence {
            if let Err(error) = persistence.save_runtime_session(session).await {
                tracing::warn!(
                    "[{}] Failed to save session after Task update: {}",
                    session_id,
                    error
                );
            } else {
                tracing::debug!("[{}] Session saved after Task update", session_id);
            }

            if shared_session_id != session.id {
                match storage.load_session(shared_session_id).await {
                    Ok(Some(mut root_session)) => {
                        root_session.set_task_list(task_list.clone());
                        if let Some(version) = session.metadata.get("task_list_version") {
                            root_session
                                .metadata
                                .insert("task_list_version".to_string(), version.clone());
                        }
                        if let Err(error) =
                            persistence.save_runtime_session(&mut root_session).await
                        {
                            tracing::warn!(
                                "[{}] Failed to save shared root task list on {}: {}",
                                session_id,
                                shared_session_id,
                                error
                            );
                        } else {
                            tracing::debug!(
                                "[{}] Shared root task list saved on {}",
                                session_id,
                                shared_session_id
                            );
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "[{}] Root session {} not found while syncing shared task list",
                            session_id,
                            shared_session_id
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            "[{}] Failed to load root session {} while syncing shared task list: {}",
                            session_id,
                            shared_session_id,
                            error
                        );
                    }
                }
            }
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
    if task_context.is_some() {
        tracing::debug!("[{}] TaskLoopContext re-initialized after Task", session_id);
    }
}
