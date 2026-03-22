use tokio::sync::mpsc;

use crate::agent::core::tools::ToolCall;
use crate::agent::core::{AgentEvent, Message, Session};
use crate::agent::metrics::MetricsCollector;

use super::super::events;
use super::super::loop_state::RoundExecutionState;
use super::super::tool_error_collector;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_tool_execution_error(
    tool_call: &ToolCall,
    error_message: &str,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    round: usize,
    session: &mut Session,
    state: &mut RoundExecutionState,
) {
    state.mark_tool_execution_error(error_message.to_string());

    events::send_event_with_metrics(
        event_tx,
        metrics_collector,
        session_id,
        round_id,
        AgentEvent::ToolError {
            tool_call_id: tool_call.id.clone(),
            error: error_message.to_string(),
        },
    )
    .await;

    session.add_message(Message::tool_result_with_status(
        tool_call.id.clone(),
        format!("Error: {error_message}"),
        false,
    ));

    // Fire-and-forget: persist hard error for offline analysis.
    tool_error_collector::append_tool_error(tool_error_collector::hard_error_record(
        session_id,
        round,
        &tool_call.function.name,
        &tool_call.id,
        &tool_call.function.arguments,
        error_message,
    ))
    .await;
}
