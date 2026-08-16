use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_tools::TaskTool;

enum TaskPersistenceOutcome {
    Publish(bamboo_domain::TaskList),
    Suppress,
}

fn task_list_matches(
    current: Option<&bamboo_domain::TaskList>,
    expected: &bamboo_domain::TaskList,
) -> bool {
    match (
        current.map(serde_json::to_value),
        serde_json::to_value(expected),
    ) {
        (Some(Ok(current)), Ok(expected)) => current == expected,
        _ => false,
    }
}

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

    match persist_shared_task_list(config, session, &shared_session_id, session_id).await {
        TaskPersistenceOutcome::Publish(authoritative_task_list) => {
            // Persistence may rebase a stale child candidate onto a newer
            // shared-root generation. Publish the generation paired with the
            // authoritative snapshot, not the pre-save candidate version.
            let authoritative_version = session
                .task_list_version_meta()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(next_version);
            let _ = event_tx
                .send(AgentEvent::TaskListUpdated {
                    task_list: authoritative_task_list,
                    version: Some(authoritative_version),
                })
                .await;
            reinitialize_task_context(task_context, session, session_id, true);
        }
        TaskPersistenceOutcome::Suppress => {
            // A failed reconciliation must not emit or mark the losing staged
            // Task list dirty. Keep the live context aligned with whichever
            // durable child snapshot the reconciliation helper could reload.
            reinitialize_task_context(task_context, session, session_id, false);
        }
    }
}

async fn persist_shared_task_list(
    config: &AgentLoopConfig,
    session: &mut Session,
    shared_session_id: &str,
    session_id: &str,
) -> TaskPersistenceOutcome {
    let Some(ref persistence) = config.persistence else {
        return session
            .task_list
            .clone()
            .map(TaskPersistenceOutcome::Publish)
            .unwrap_or(TaskPersistenceOutcome::Suppress);
    };

    if let Err(error) = persistence.save_runtime_control_plane(session).await {
        tracing::warn!(
            "[{}] Failed to save session control-plane after Task update: {}",
            session_id,
            error
        );
        return TaskPersistenceOutcome::Suppress;
    }
    tracing::debug!(
        "[{}] Session control-plane saved after Task update",
        session_id
    );

    if shared_session_id == session.id {
        return session
            .task_list
            .clone()
            .map(TaskPersistenceOutcome::Publish)
            .unwrap_or(TaskPersistenceOutcome::Suppress);
    }

    // The local save may have rebased a stale same-generation tool candidate
    // onto a newer durable Task evaluation. Read both list and generation only
    // after that save, from the same rewritten session snapshot, so a child can
    // never overwrite its shared root with the candidate that just lost.
    let Some(task_list) = session.task_list.clone() else {
        tracing::warn!(
            "[{}] Shared root Task update on {} has no authoritative task list",
            session_id,
            shared_session_id
        );
        return TaskPersistenceOutcome::Suppress;
    };
    let Some(version) = session.task_list_version_meta() else {
        tracing::warn!(
            "[{}] Shared root Task update on {} has no task-list version",
            session_id,
            shared_session_id
        );
        return TaskPersistenceOutcome::Suppress;
    };

    match persistence
        .update_task_list_control_plane(shared_session_id, &task_list, &version)
        .await
    {
        Ok(true) => {
            tracing::debug!(
                "[{}] Shared root Task control-plane patched on {}",
                session_id,
                shared_session_id
            );
            TaskPersistenceOutcome::Publish(task_list)
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
                return TaskPersistenceOutcome::Suppress;
            };
            let mut root_session = match storage.load_session(shared_session_id).await {
                Ok(Some(root_session)) => root_session,
                Ok(None) => {
                    tracing::warn!(
                        "[{}] Root session {} not found while syncing shared task list",
                        session_id,
                        shared_session_id
                    );
                    return TaskPersistenceOutcome::Suppress;
                }
                Err(error) => {
                    tracing::warn!(
                        "[{}] Failed to load root session {} through storage fallback: {}",
                        session_id,
                        shared_session_id,
                        error
                    );
                    return TaskPersistenceOutcome::Suppress;
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
                TaskPersistenceOutcome::Suppress
            } else {
                tracing::debug!(
                    "[{}] Shared root Task full-save fallback completed on {}",
                    session_id,
                    shared_session_id
                );
                TaskPersistenceOutcome::Publish(task_list)
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            tracing::warn!(
                "[{}] Shared root Task control-plane on {} changed concurrently: {}",
                session_id,
                shared_session_id,
                error
            );
            reconcile_child_with_shared_root(persistence, session, shared_session_id, session_id)
                .await
        }
        Err(error) => {
            tracing::warn!(
                "[{}] Failed to patch shared root Task control-plane on {}: {}",
                session_id,
                shared_session_id,
                error
            );
            TaskPersistenceOutcome::Suppress
        }
    }
}

async fn reconcile_child_with_shared_root(
    persistence: &std::sync::Arc<dyn bamboo_domain::RuntimeSessionPersistence>,
    session: &mut Session,
    shared_session_id: &str,
    session_id: &str,
) -> TaskPersistenceOutcome {
    let Some(child_task_list) = session.task_list.clone() else {
        return TaskPersistenceOutcome::Suppress;
    };
    let Some(child_version) = session.task_list_version_meta() else {
        return TaskPersistenceOutcome::Suppress;
    };
    let root = match persistence
        .load_runtime_control_plane(shared_session_id)
        .await
    {
        Ok(Some(root)) => root,
        Ok(None) => return TaskPersistenceOutcome::Suppress,
        Err(error) => {
            tracing::warn!(
                "[{}] Failed to load shared root {} for Task reconciliation: {}",
                session_id,
                shared_session_id,
                error
            );
            return TaskPersistenceOutcome::Suppress;
        }
    };
    let (Some(root_task_list), Some(root_version)) =
        (root.task_list.clone(), root.task_list_version_meta())
    else {
        return TaskPersistenceOutcome::Suppress;
    };

    let reconciled = persistence
        .update_task_list_control_plane_if_version(
            &session.id,
            &child_version,
            &child_task_list,
            &root_task_list,
            &root_version,
        )
        .await;
    if matches!(reconciled, Ok(true)) {
        // Re-read the root after the child CAS. If it advanced again, suppress
        // publication rather than claiming a cross-session snapshot that was
        // never simultaneously authoritative.
        let root_still_matches = persistence
            .load_runtime_control_plane(shared_session_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|latest| {
                latest.task_list_version_meta().as_deref() == Some(root_version.as_str())
                    && task_list_matches(latest.task_list.as_ref(), &root_task_list)
            });
        session.task_list = Some(root_task_list.clone());
        session.set_task_list_version_meta(root_version);
        if root_still_matches {
            return TaskPersistenceOutcome::Publish(root_task_list);
        }
    } else if let Err(error) = reconciled {
        tracing::warn!(
            "[{}] Failed to reconcile child Task control-plane with root {}: {}",
            session_id,
            shared_session_id,
            error
        );
    }

    if let Ok(Some(durable_child)) = persistence.load_runtime_control_plane(&session.id).await {
        let version = durable_child.task_list_version_meta();
        if let (Some(task_list), Some(version)) = (durable_child.task_list, version) {
            session.task_list = Some(task_list);
            session.set_task_list_version_meta(version);
        }
    }
    TaskPersistenceOutcome::Suppress
}

fn reinitialize_task_context(
    task_context: &mut Option<TaskLoopContext>,
    session: &Session,
    session_id: &str,
    mark_dirty: bool,
) {
    // IMPORTANT: Re-initialize TaskLoopContext from session.
    *task_context = TaskLoopContext::from_session(session);
    if mark_dirty {
        if let Some(ctx) = task_context.as_mut() {
            // Mark the list dirty so the end-of-turn gate spawns exactly one
            // evaluation for this Task-tool write. This is the only place the flag is
            // set; it is cleared when the evaluation is spawned.
            ctx.task_list_dirty = true;
            tracing::debug!("[{}] TaskLoopContext re-initialized after Task", session_id);
        }
    }
}
