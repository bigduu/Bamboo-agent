use tokio::sync::mpsc;

use crate::agent::core::tools::ToolCall;
use crate::agent::core::{AgentEvent, Message, Session};
use crate::agent::metrics::MetricsCollector;

use super::super::events;
use super::super::loop_state::RoundExecutionState;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_tool_execution_error(
    tool_call: &ToolCall,
    error_message: &str,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
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

    session.add_message(Message::tool_result(
        tool_call.id.clone(),
        format!("Error: {error_message}"),
    ));
}
