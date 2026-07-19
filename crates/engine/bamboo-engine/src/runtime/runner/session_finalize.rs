//! Session finalization helpers for the agent loop runner.

use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::AgentStatusState;
use bamboo_metrics::MetricsCollector;

mod completion_event;
mod completion_metrics;

use completion_event::send_complete_event_if_needed;
use completion_metrics::record_session_resolution;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_session(
    task_context: Option<TaskLoopContext>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    config: &AgentLoopConfig,
    metrics_collector: Option<&MetricsCollector>,
    sent_complete: bool,
    runtime_state: &mut bamboo_domain::AgentRuntimeState,
) {
    super::task_lifecycle::finalize_task_context(
        task_context,
        session,
        event_tx,
        session_id,
        config,
    )
    .await;

    send_complete_event_if_needed(event_tx, sent_complete).await;

    record_session_resolution(metrics_collector, session_id, session, runtime_state);

    if !matches!(runtime_state.status, AgentStatusState::Suspended) {
        if let Some(skill_manager) = config.skill_manager.as_ref() {
            let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
            if let Err(error) = skill_manager
                .release_activation_for_workspace(session_id, workspace.as_deref())
                .await
            {
                tracing::warn!(
                    "[{}] Failed to release completed workflow activation snapshot: {}",
                    session_id,
                    error
                );
            }
        }
        runtime_state.status = AgentStatusState::Completed;
    }
    super::state_bridge::write_runtime_state(session, runtime_state);
}

#[cfg(test)]
mod tests;
