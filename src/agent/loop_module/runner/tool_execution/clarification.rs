use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolResult};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::metrics::MetricsCollector;

use super::events::send_event_with_metrics;

mod payload;
mod session_effects;

use payload::{parse_user_question_payload, should_handle_user_question_tool};
use session_effects::{
    append_waiting_tool_result_message, emit_need_clarification_event,
    persist_session_after_question,
};

pub(super) async fn maybe_handle_user_question_tool(
    tool_call: &ToolCall,
    result: &ToolResult,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    config: &AgentLoopConfig,
) -> bool {
    if !should_handle_user_question_tool(tool_call, result) {
        return false;
    }

    let Some(question_payload) = parse_user_question_payload(&result.result) else {
        return false;
    };

    log::info!(
        "[{}] {} called, awaiting user response",
        session_id,
        tool_call.function.name
    );

    append_waiting_tool_result_message(session, tool_call, &result.result, session_id);

    send_event_with_metrics(
        event_tx,
        metrics_collector,
        session_id,
        round_id,
        AgentEvent::ToolComplete {
            tool_call_id: tool_call.id.clone(),
            result: result.clone(),
        },
    )
    .await;

    emit_need_clarification_event(event_tx, &question_payload).await;

    session.set_pending_question(
        tool_call.id.clone(),
        question_payload.question,
        question_payload.options,
        question_payload.allow_custom,
    );

    persist_session_after_question(config, session, session_id).await;

    true
}

#[cfg(test)]
mod tests;
