use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::tools::{
    parse_tool_args_best_effort, ToolCall, ToolExecutionContext, ToolExecutor, ToolResult,
};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::metrics::MetricsCollector;

use super::execution_paths;
use super::loop_state::RoundExecutionState;
use super::policy;

fn preview_for_log(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut preview = String::new();
    for _ in 0..max_chars {
        match iter.next() {
            Some(ch) => preview.push(ch),
            None => break,
        }
    }
    if iter.next().is_some() {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n").replace('\r', "\\r")
}

pub(super) struct ToolExecutionOnlyContext<'a> {
    pub tool_call: &'a ToolCall,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub round: usize,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub config: &'a AgentLoopConfig,
}

pub(super) struct ToolExecutionApplyContext<'a> {
    pub tool_call: &'a ToolCall,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub round: usize,
    pub session: &'a mut Session,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub config: &'a AgentLoopConfig,
    pub task_context: &'a mut Option<TaskLoopContext>,
    pub state: &'a mut RoundExecutionState,
}

pub(super) struct ToolExecutionOutcome {
    pub result: Result<ToolResult, String>,
    pub tool_duration: std::time::Duration,
}

pub(super) async fn execute_tool_call_only(
    ctx: ToolExecutionOnlyContext<'_>,
) -> ToolExecutionOutcome {
    if let Err(policy_error) = policy::validate_tool_call_arguments(ctx.tool_call) {
        tracing::warn!(
            "[{}][round:{}] Tool call blocked by strict argument policy before ToolStart: tool_call_id={}, tool_name={}, error={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            policy_error
        );
        return ToolExecutionOutcome {
            result: Err(policy_error),
            tool_duration: std::time::Duration::ZERO,
        };
    }

    let raw_arguments = ctx.tool_call.function.arguments.trim();
    let (args, parse_warning) = parse_tool_args_best_effort(&ctx.tool_call.function.arguments);
    if let Some(warning) = parse_warning {
        tracing::warn!(
            "[{}][round:{}] Tool call arguments required fallback before ToolStart: tool_call_id={}, tool_name={}, args_len={}, args_preview=\"{}\", warning={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            raw_arguments.len(),
            preview_for_log(raw_arguments, 180),
            warning
        );
    }

    tracing::debug!(
        "[{}][round:{}] Starting tool execution: tool_call_id={}, tool_name={}, raw_args_len={}",
        ctx.session_id,
        ctx.round,
        ctx.tool_call.id,
        ctx.tool_call.function.name,
        raw_arguments.len()
    );

    super::events::send_event_with_metrics(
        ctx.event_tx,
        ctx.metrics_collector,
        ctx.session_id,
        ctx.round_id,
        AgentEvent::ToolStart {
            tool_call_id: ctx.tool_call.id.clone(),
            tool_name: ctx.tool_call.function.name.clone(),
            arguments: args,
        },
    )
    .await;

    let tool_timer = std::time::Instant::now();
    let tool_ctx = ToolExecutionContext {
        session_id: Some(ctx.session_id),
        tool_call_id: &ctx.tool_call.id,
        event_tx: Some(ctx.event_tx),
    };

    let result = crate::agent::core::tools::executor::execute_tool_call_with_context(
        ctx.tool_call,
        ctx.tools.as_ref(),
        ctx.config.composition_executor.as_ref().map(Arc::clone),
        tool_ctx,
    )
    .await;

    ToolExecutionOutcome {
        result: result.map_err(|error| error.to_string()),
        tool_duration: tool_timer.elapsed(),
    }
}

pub(super) async fn apply_tool_execution_outcome(
    ctx: ToolExecutionApplyContext<'_>,
    outcome: ToolExecutionOutcome,
) -> bool {
    // ── Output compression pipeline ────────────────────────────────────
    // Compress the tool result before it enters the session message list.
    let outcome = super::output_compressor::maybe_compress(
        &ctx.tool_call.function.name,
        &ctx.tool_call.function.arguments,
        ctx.session_id,
        outcome,
    )
    .await;

    match outcome.result {
        Ok(result) => {
            execution_paths::handle_successful_tool_result(execution_paths::SuccessPathContext {
                tool_call: ctx.tool_call,
                result: &result,
                event_tx: ctx.event_tx,
                metrics_collector: ctx.metrics_collector,
                session_id: ctx.session_id,
                round_id: ctx.round_id,
                round: ctx.round,
                session: ctx.session,
                tools: ctx.tools,
                config: ctx.config,
                task_context: ctx.task_context,
                state: ctx.state,
                tool_duration: outcome.tool_duration,
            })
            .await
        }
        Err(error_message) => {
            execution_paths::handle_tool_execution_error(
                ctx.tool_call,
                &error_message,
                ctx.event_tx,
                ctx.metrics_collector,
                ctx.session_id,
                ctx.round_id,
                ctx.round,
                ctx.session,
                ctx.state,
            )
            .await;
            false
        }
    }
}
