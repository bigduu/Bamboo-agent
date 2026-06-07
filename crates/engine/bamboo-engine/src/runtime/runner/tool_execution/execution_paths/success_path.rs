use std::sync::Arc;

use bamboo_agent_core::tools::{handle_tool_result_with_agentic_support, ToolHandlingOutcome};
use bamboo_agent_core::AgentEvent;

use super::super::{clarification, events, task, tool_error_collector};
use super::{workspace, SuccessPathContext};

pub(super) async fn handle_successful_tool_result(ctx: SuccessPathContext<'_>) -> bool {
    task::track_task_progress(
        ctx.task_context,
        ctx.event_tx,
        ctx.session_id,
        ctx.tool_call,
        ctx.result,
        ctx.round,
    )
    .await;

    task::maybe_handle_taskwrite(
        ctx.tool_call,
        ctx.result,
        ctx.session,
        ctx.session_id,
        ctx.event_tx,
        ctx.config,
        ctx.task_context,
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

        // Fire-and-forget: persist soft failure for offline analysis.
        tool_error_collector::append_tool_error(tool_error_collector::soft_failure_record(
            ctx.session_id,
            ctx.round,
            &ctx.tool_call.function.name,
            &ctx.tool_call.id,
            &ctx.tool_call.function.arguments,
            &ctx.result.result,
        ))
        .await;
    }

    tracing::debug!(
        "[{}] tool_complete: {}",
        ctx.session_id,
        serde_json::json!({
            "tool_name": ctx.tool_call.function.name,
            "tool_call_id": ctx.tool_call.id,
            "duration_ms": ctx.tool_duration.as_millis(),
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

    match outcome {
        ToolHandlingOutcome::AwaitingClarification => {
            ctx.state.mark_awaiting_clarification();
            true
        }
        ToolHandlingOutcome::WaitingForChildren => {
            ctx.state.mark_waiting_for_children();
            // Do NOT break — remaining tool calls in this round may spawn
            // additional children. The pipeline will suspend the root agent
            // after ALL tool calls finish, once `waiting_for_children` is set.
            false
        }
        ToolHandlingOutcome::Continue => false,
    }
}
