use std::sync::Arc;

use tokio::sync::mpsc;

use crate::metrics::{MetricsCollector, TokenUsage};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::session_setup::tool_schemas::resolve_available_tool_schemas_for_session;
use crate::runtime::stream::handler::StreamHandlingOutput;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Message, Session};
use bamboo_infrastructure::LLMProvider;

use super::super::{task_lifecycle, tool_execution};
use super::{RoundFlowContext, RoundFlowOutcome};

mod round_state;
mod usage;

use round_state::ToolCallsRoundState;
use usage::accumulate_round_usage;

pub(super) async fn handle_tool_calls_path(
    context: RoundFlowContext<'_>,
    stream_output: StreamHandlingOutput,
    mut round_usage: TokenUsage,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    tools: &Arc<dyn ToolExecutor>,
    config: &AgentLoopConfig,
    task_context: &mut Option<TaskLoopContext>,
    llm: Arc<dyn LLMProvider>,
) -> Result<RoundFlowOutcome, AgentError> {
    let reasoning = (!stream_output.reasoning_content.trim().is_empty())
        .then_some(stream_output.reasoning_content);
    session.add_message(Message::assistant_with_reasoning(
        stream_output.content,
        Some(stream_output.tool_calls.clone()),
        reasoning,
    ));

    let compression_model = config
        .model_name
        .clone()
        .or_else(|| (!session.model.trim().is_empty()).then_some(session.model.trim().to_string()));
    if compression_model.is_none() {
        tracing::warn!(
            "[{}] Skipping mid-turn context compression after tool execution: missing model name",
            context.session_id
        );
    }
    let tool_schemas = resolve_available_tool_schemas_for_session(config, tools.as_ref(), session);

    let mut state = ToolCallsRoundState::default();
    let tool_execution = tool_execution::execute_round_tool_calls(
        &stream_output.tool_calls,
        event_tx,
        metrics_collector,
        context.session_id,
        context.round_id,
        context.round,
        session,
        tools,
        config,
        task_context,
        &llm,
        compression_model.as_deref(),
        &tool_schemas,
    )
    .await?;
    state.apply_tool_execution_result(tool_execution);

    if state.awaiting_clarification() {
        state.record_round_completion(metrics_collector, &context, session, round_usage);
        return Ok(RoundFlowOutcome {
            should_break: true,
            sent_complete: false,
        });
    }

    state.log_round_complete_if_debug(&context, session.messages.len());

    // Use fast_model for task evaluation when available (lightweight task).
    let eval_model = config
        .fast_model_name
        .as_deref()
        .or(config.model_name.as_deref());
    let task_evaluation_usage = task_lifecycle::evaluate_round_task_progress(
        task_context,
        session,
        llm,
        event_tx,
        context.session_id,
        context.round + 1,
        eval_model,
        config.reasoning_effort,
    )
    .await?;
    accumulate_round_usage(&mut round_usage, task_evaluation_usage);

    state.record_round_completion(metrics_collector, &context, session, round_usage);

    Ok(RoundFlowOutcome {
        should_break: false,
        sent_complete: false,
    })
}
