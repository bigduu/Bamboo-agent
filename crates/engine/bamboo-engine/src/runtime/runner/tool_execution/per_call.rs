use std::sync::Arc;

use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{
    parse_tool_args_best_effort, ToolCall, ToolExecutionContext, ToolExecutor, ToolResult,
};
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_metrics::MetricsCollector;

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

    // ── ToolEmitter: track lifecycle events ─────────────────────────────
    let tool_name = ctx.tool_call.function.name.trim();
    let is_mutating = bamboo_tools::orchestrator::classify_tool(tool_name)
        == bamboo_tools::orchestrator::ToolMutability::Mutating;
    let mut emitter =
        bamboo_tools::events::ToolEmitter::new(&ctx.tool_call.id, tool_name, is_mutating);
    emitter.set_auto_approved(!is_mutating);
    let begin_event = emitter.begin().clone();
    // Push lifecycle "begin" through the AgentEvent channel for UI visibility
    if let Err(e) = ctx.event_tx.send(begin_event.into_agent_event()).await {
        tracing::warn!(
            "[{}] tool lifecycle begin event send failed: {}",
            ctx.session_id,
            e
        );
    }

    let tool_timer = std::time::Instant::now();
    let available_tool_schemas = ctx.tools.list_tools();
    let tool_ctx = ToolExecutionContext {
        session_id: Some(ctx.session_id),
        tool_call_id: &ctx.tool_call.id,
        event_tx: Some(ctx.event_tx),
        available_tool_schemas: Some(available_tool_schemas.as_slice()),
    };

    let result = bamboo_agent_core::tools::executor::execute_tool_call_with_context(
        ctx.tool_call,
        ctx.tools.as_ref(),
        ctx.config.composition_executor.as_ref().map(Arc::clone),
        tool_ctx,
    )
    .await;

    let tool_duration = tool_timer.elapsed();

    // Emit lifecycle event based on result and push through AgentEvent channel
    let end_event = match &result {
        Ok(_) => emitter
            .finish(Some(format!("completed in {:?}", tool_duration)))
            .clone(),
        Err(err) => emitter.error(format!("{}", err)).clone(),
    };
    if let Err(e) = ctx.event_tx.send(end_event.into_agent_event()).await {
        tracing::warn!(
            "[{}] tool lifecycle end event send failed: {}",
            ctx.session_id,
            e
        );
    }

    tracing::trace!(
        "[{}][round:{}] ToolEmitter: call_id={}, tool={}, events={}",
        ctx.session_id,
        ctx.round,
        ctx.tool_call.id,
        tool_name,
        emitter.events().len()
    );

    ToolExecutionOutcome {
        result: result.map_err(|error| error.to_string()),
        tool_duration,
    }
}

pub(super) async fn apply_tool_execution_outcome(
    ctx: ToolExecutionApplyContext<'_>,
    outcome: ToolExecutionOutcome,
) -> bool {
    // Capture tool lifecycle metadata before the borrow-splitting match.
    let tool_name_for_meta = ctx.tool_call.function.name.clone();
    let tool_call_id_for_meta = ctx.tool_call.id.clone();
    let tool_duration_ms = outcome.tool_duration.as_millis() as u64;
    let is_success = outcome.result.is_ok();

    let is_mutating = bamboo_tools::orchestrator::classify_tool(&tool_name_for_meta)
        == bamboo_tools::orchestrator::ToolMutability::Mutating;

    let result = match outcome.result {
        Ok(result) => {
            let r = execution_paths::handle_successful_tool_result(
                execution_paths::SuccessPathContext {
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
                },
            )
            .await;
            r
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
    };

    // ── Persist lifecycle metadata on the tool result message ──────────
    // Find the last tool-result message matching this tool_call_id and
    // attach execution metadata so it is persisted in session.json and
    // available when the frontend reloads the session later.
    let metadata_value = serde_json::json!({
        "elapsed_ms": tool_duration_ms,
        "is_mutating": is_mutating,
        "auto_approved": !is_mutating,
        "tool_name": tool_name_for_meta,
        "success": is_success,
    });
    if let Some(msg) = ctx
        .session
        .messages
        .iter_mut()
        .rev()
        .find(|m| m.tool_call_id.as_deref() == Some(&tool_call_id_for_meta))
    {
        msg.metadata = Some(metadata_value);
    }

    result
}
