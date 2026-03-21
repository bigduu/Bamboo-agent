use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolResult};
use crate::agent::core::{AgentEvent, Session, SessionKind};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::tools::TaskTool;

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

    let Ok(task_list) = TaskTool::task_list_from_args(&args, &shared_session_id) else {
        return;
    };

    // Keep the executing session in sync so this run's prompt context remains coherent.
    session.set_task_list(task_list.clone());
    tracing::info!(
        "[{}] Task updated shared task list '{}' with {} items",
        session_id,
        task_list.title,
        task_list.items.len()
    );

    persist_task_list(config, session, &shared_session_id, session_id, &task_list).await;

    let _ = event_tx
        .send(AgentEvent::TaskListUpdated {
            task_list: task_list.clone(),
        })
        .await;

    reinitialize_task_context(task_context, session, session_id);
}

async fn persist_task_list(
    config: &AgentLoopConfig,
    session: &Session,
    shared_session_id: &str,
    session_id: &str,
    task_list: &crate::agent::core::TaskList,
) {
    if let Some(ref storage) = config.storage {
        if let Err(error) = storage.save_session(session).await {
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
                    if let Err(error) = storage.save_session(&root_session).await {
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
