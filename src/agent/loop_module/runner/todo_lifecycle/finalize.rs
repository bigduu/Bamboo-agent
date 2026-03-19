use chrono::Utc;
use tokio::sync::mpsc;

use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;

pub(super) async fn finalize_todo_context(
    todo_context: Option<TodoLoopContext>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    config: &AgentLoopConfig,
) {
    let Some(ctx) = todo_context else {
        return;
    };

    if ctx.is_all_completed() {
        tracing::info!("[{}] All todo items completed", session_id);

        let _ = event_tx
            .send(AgentEvent::TodoListCompleted {
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
        .insert("todo_list_version".to_string(), version.to_string());
    session.todo_list = Some(ctx.into_todo_list());
    session.updated_at = Utc::now();

    tracing::debug!(
        "[{}] Synced TodoLoopContext to session, version={}",
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
            tracing::debug!("[{}] Session saved with updated todo list", session_id);
        }
    }
}
