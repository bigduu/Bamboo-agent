//! Round post-LLM flow helpers for the agent loop runner.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::metrics::MetricsCollector;

use super::round_lifecycle::RoundLlmExecutionOutput;

mod no_tool_calls;
mod tool_calls;

pub(super) struct RoundFlowOutcome {
    pub should_break: bool,
    pub sent_complete: bool,
}

pub(super) struct RoundFlowContext<'a> {
    pub round: usize,
    pub round_id: &'a str,
    pub session_id: &'a str,
    pub debug_enabled: bool,
}

pub(super) async fn handle_round_post_llm(
    context: RoundFlowContext<'_>,
    round_llm_output: RoundLlmExecutionOutput,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    tools: &Arc<dyn ToolExecutor>,
    config: &AgentLoopConfig,
    todo_context: &mut Option<TodoLoopContext>,
    llm: Arc<dyn LLMProvider>,
) -> Result<RoundFlowOutcome, AgentError> {
    let stream_output = round_llm_output.stream_output;

    if stream_output.tool_calls.is_empty() {
        let reasoning = (!stream_output.reasoning_content.trim().is_empty())
            .then_some(stream_output.reasoning_content);
        return Ok(no_tool_calls::handle_no_tool_calls(
            stream_output.content,
            reasoning,
            round_llm_output.prompt_tokens,
            round_llm_output.completion_tokens,
            round_llm_output.round_usage,
            session,
            event_tx,
            metrics_collector,
            context.round_id,
            context.session_id,
        )
        .await);
    }

    tool_calls::handle_tool_calls_path(
        context,
        stream_output,
        round_llm_output.round_usage,
        session,
        event_tx,
        metrics_collector,
        tools,
        config,
        todo_context,
        llm,
    )
    .await
}
