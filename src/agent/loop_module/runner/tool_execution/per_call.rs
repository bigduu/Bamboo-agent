use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::tools::{parse_tool_args, ToolCall, ToolExecutionContext, ToolExecutor};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::metrics::MetricsCollector;

use super::execution_paths;
use super::loop_state::RoundExecutionState;

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

pub(super) struct PerToolExecutionContext<'a> {
    pub tool_call: &'a ToolCall,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub round: usize,
    pub session: &'a mut Session,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub config: &'a AgentLoopConfig,
    pub todo_context: &'a mut Option<TodoLoopContext>,
    pub state: &'a mut RoundExecutionState,
}

pub(super) async fn execute_single_tool_call(ctx: PerToolExecutionContext<'_>) -> bool {
    let raw_arguments = ctx.tool_call.function.arguments.trim();
    let args = match parse_tool_args(&ctx.tool_call.function.arguments) {
        Ok(args) => args,
        Err(error) => {
            log::warn!(
                "[{}][round:{}] Failed to parse tool call arguments before ToolStart event; using empty object: tool_call_id={}, tool_name={}, args_len={}, args_preview=\"{}\", error={}",
                ctx.session_id,
                ctx.round,
                ctx.tool_call.id,
                ctx.tool_call.function.name,
                raw_arguments.len(),
                preview_for_log(raw_arguments, 180),
                error
            );
            serde_json::json!({})
        }
    };

    log::debug!(
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

    match crate::agent::core::tools::executor::execute_tool_call_with_context(
        ctx.tool_call,
        ctx.tools.as_ref(),
        ctx.config.composition_executor.as_ref().map(Arc::clone),
        tool_ctx,
    )
    .await
    {
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
                todo_context: ctx.todo_context,
                state: ctx.state,
                tool_timer,
            })
            .await
        }
        Err(error) => {
            execution_paths::handle_tool_execution_error(
                ctx.tool_call,
                &error.to_string(),
                ctx.event_tx,
                ctx.metrics_collector,
                ctx.session_id,
                ctx.round_id,
                ctx.session,
                ctx.state,
            )
            .await;
            false
        }
    }
}
