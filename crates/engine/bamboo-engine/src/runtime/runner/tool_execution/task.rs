use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Session};

mod progress;
mod taskwrite;

pub(super) async fn maybe_apply_ticket_task(
    tool_call: &ToolCall,
    result: &ToolResult,
    session: &mut Session,
    config: &AgentLoopConfig,
) -> Option<ToolResult> {
    if tool_call.function.name != "Task" || !result.success {
        return None;
    }
    if !session
        .metadata
        .contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY)
        && config.ticket_worker_plan.is_none()
    {
        return None;
    }
    let outcome = async {
        let plan = config.ticket_worker_plan.as_ref().ok_or_else(|| {
            bamboo_tickets::Error::ScopeDenied(
                "Ticket Worker requires a trusted LocalPlan permit".into(),
            )
        })?;
        let args = serde_json::from_str(&tool_call.function.arguments)?;
        plan.apply_task(session, &tool_call.id, &args).await
    }
    .await;
    let mut resolved = result.clone();
    if let Err(error) = outcome {
        resolved.success = false;
        resolved.result = error.to_string();
    } else if let Some(request) = session
        .metadata
        .get(crate::ticket_worker_plan::TICKET_QUESTION_YIELD_KEY)
    {
        resolved.result = serde_json::json!({"status":"waiting_for_answer","request":serde_json::from_str::<serde_json::Value>(request).unwrap_or_default()}).to_string();
    }
    Some(resolved)
}

pub(super) async fn track_task_progress(
    task_context: &mut Option<TaskLoopContext>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    tool_call: &ToolCall,
    result: &ToolResult,
    round: usize,
) {
    progress::track_task_progress(task_context, event_tx, session_id, tool_call, result, round)
        .await;
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
    taskwrite::maybe_handle_taskwrite(
        tool_call,
        result,
        session,
        session_id,
        event_tx,
        config,
        task_context,
    )
    .await;
}

#[cfg(test)]
mod tests;
