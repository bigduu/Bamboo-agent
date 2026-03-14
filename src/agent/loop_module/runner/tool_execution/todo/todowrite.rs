use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolResult};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::tools::TodoWriteTool;

pub(super) async fn maybe_handle_todowrite(
    tool_call: &ToolCall,
    result: &ToolResult,
    session: &mut Session,
    session_id: &str,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    todo_context: &mut Option<TodoLoopContext>,
) {
    if tool_call.function.name != "TodoWrite" || !result.success {
        return;
    }

    let Ok(args) = serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments) else {
        return;
    };
    let Ok(todo_list) = TodoWriteTool::todo_list_from_args(&args, session_id) else {
        return;
    };

    session.set_todo_list(todo_list.clone());
    log::info!(
        "[{}] TodoWrite updated todo list '{}' with {} items",
        session_id,
        todo_list.title,
        todo_list.items.len()
    );

    persist_todo_list(config, session, session_id).await;

    let _ = event_tx
        .send(AgentEvent::TodoListUpdated {
            todo_list: todo_list.clone(),
        })
        .await;

    reinitialize_todo_context(todo_context, session, session_id);
}

async fn persist_todo_list(config: &AgentLoopConfig, session: &Session, session_id: &str) {
    if let Some(ref storage) = config.storage {
        if let Err(error) = storage.save_session(session).await {
            log::warn!(
                "[{}] Failed to save session after todo list creation: {}",
                session_id,
                error
            );
        } else {
            log::debug!("[{}] Session saved after TodoWrite update", session_id);
        }
    }
}

fn reinitialize_todo_context(
    todo_context: &mut Option<TodoLoopContext>,
    session: &Session,
    session_id: &str,
) {
    // IMPORTANT: Re-initialize TodoLoopContext from session.
    *todo_context = TodoLoopContext::from_session(session);
    if todo_context.is_some() {
        log::debug!(
            "[{}] TodoLoopContext re-initialized after TodoWrite",
            session_id
        );
    }
}
