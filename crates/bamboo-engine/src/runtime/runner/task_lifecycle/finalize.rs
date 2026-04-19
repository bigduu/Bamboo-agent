use chrono::Utc;
use tokio::sync::mpsc;

use bamboo_agent_core::{AgentEvent, Session};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;

pub(super) async fn finalize_task_context(
    task_context: Option<TaskLoopContext>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    config: &AgentLoopConfig,
) {
    let Some(ctx) = task_context else {
        return;
    };

    if ctx.is_all_completed() {
        tracing::info!("[{}] All task items completed", session_id);

        let _ = event_tx
            .send(AgentEvent::TaskListCompleted {
                session_id: session_id.to_string(),
                completed_at: Utc::now(),
                total_rounds: ctx.current_round + 1, // Convert 0-indexed to 1-indexed for display.
                total_tool_calls: ctx.items.iter().map(|item| item.tool_calls.len()).sum(),
            })
            .await;
    }

    let version = ctx.version;
    session
        .metadata
        .insert("task_list_version".to_string(), version.to_string());
    session.task_list = Some(ctx.into_task_list());
    session.updated_at = Utc::now();

    tracing::debug!(
        "[{}] Synced TaskLoopContext to session, version={}",
        session_id,
        version
    );

    if let Some(ref storage) = config.storage {
        if let Err(error) = storage.save_session(session).await {
            tracing::warn!(
                "[{}] Failed to save session after agent loop: {}",
                session_id,
                error
            );
        } else {
            tracing::debug!("[{}] Session saved with updated task list", session_id);
        }
    }
}
