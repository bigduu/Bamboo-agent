use tokio::sync::mpsc;

use crate::agent::core::AgentEvent;
use crate::agent::loop_module::stream::handler::StreamHandlingOutput;

use super::super::token_estimation::estimate_completion_tokens;
use super::super::update_parsing::{parse_item_updates_from_tool_calls, summarize_updates};
use super::super::TaskEvaluationResult;

pub(super) async fn build_success_result(
    stream_output: StreamHandlingOutput,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    prompt_tokens: u64,
) -> TaskEvaluationResult {
    tracing::info!(
        "[{}] Task evaluation completed: {} tokens, {} tool calls",
        session_id,
        stream_output.token_count,
        stream_output.tool_calls.len()
    );

    let updates = parse_item_updates_from_tool_calls(&stream_output.tool_calls);
    let reasoning = summarize_updates(&updates);

    if !stream_output.content.trim().is_empty() {
        tracing::debug!(
            "[{}] Task evaluation raw reasoning suppressed ({} chars)",
            session_id,
            stream_output.content.len()
        );
    }

    let completion_tokens =
        estimate_completion_tokens(&stream_output.content, &stream_output.tool_calls);

    let _ = event_tx
        .send(AgentEvent::TaskEvaluationCompleted {
            session_id: session_id.to_string(),
            updates_count: updates.len(),
            reasoning: reasoning.clone(),
        })
        .await;

    TaskEvaluationResult {
        needs_evaluation: true,
        updates,
        reasoning,
        prompt_tokens,
        completion_tokens,
    }
}
