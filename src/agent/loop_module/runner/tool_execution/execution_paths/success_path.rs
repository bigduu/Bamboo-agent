use std::sync::Arc;

use crate::agent::core::tools::{handle_tool_result_with_agentic_support, ToolHandlingOutcome};
use crate::agent::core::AgentEvent;

use super::super::{clarification, events, todo};
use super::{workspace, SuccessPathContext};

pub(super) async fn handle_successful_tool_result(ctx: SuccessPathContext<'_>) -> bool {
    todo::track_todo_progress(
        ctx.todo_context,
        ctx.event_tx,
        ctx.session_id,
        ctx.tool_call,
        ctx.result,
        ctx.round,
    )
    .await;

    todo::maybe_handle_todowrite(
        ctx.tool_call,
        ctx.result,
        ctx.session,
        ctx.session_id,
        ctx.event_tx,
        ctx.config,
        ctx.todo_context,
    )
    .await;

    workspace::maybe_apply_workspace_update(ctx.session, ctx.tool_call, ctx.result, ctx.session_id);

    if clarification::maybe_handle_user_question_tool(
        ctx.tool_call,
        ctx.result,
        ctx.session,
        ctx.event_tx,
        ctx.metrics_collector,
        ctx.session_id,
        ctx.round_id,
        ctx.config,
    )
    .await
    {
        ctx.state.mark_awaiting_clarification();
        return true;
    }

    events::send_event_with_metrics(
        ctx.event_tx,
        ctx.metrics_collector,
        ctx.session_id,
        ctx.round_id,
        AgentEvent::ToolComplete {
            tool_call_id: ctx.tool_call.id.clone(),
            result: ctx.result.clone(),
        },
    )
    .await;

    if !ctx.result.success {
        ctx.state
            .mark_unsuccessful_tool(&ctx.tool_call.function.name);
    }

    log::debug!(
        "[{}] tool_complete: {}",
        ctx.session_id,
        serde_json::json!({
            "tool_name": ctx.tool_call.function.name,
            "tool_call_id": ctx.tool_call.id,
            "duration_ms": ctx.tool_timer.elapsed().as_millis(),
            "success": ctx.result.success,
        })
    );

    let outcome = handle_tool_result_with_agentic_support(
        ctx.result,
        ctx.tool_call,
        ctx.event_tx,
        ctx.session,
        ctx.tools.as_ref(),
        ctx.config.composition_executor.as_ref().map(Arc::clone),
    )
    .await;

    if outcome == ToolHandlingOutcome::AwaitingClarification {
        ctx.state.mark_awaiting_clarification();
        return true;
    }

    false
}
