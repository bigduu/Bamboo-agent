use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::ToolCall;
use bamboo_agent_core::{AgentEvent, PendingQuestionSource, Session};

use super::payload::UserQuestionPayload;

pub(super) fn append_waiting_tool_result_message(
    session: &mut Session,
    tool_call: &ToolCall,
    tool_result_payload: &str,
    session_id: &str,
) {
    let tool_result_msg = bamboo_agent_core::Message::tool_result(
        tool_call.id.clone(),
        tool_result_payload.to_string(),
    );
    tracing::debug!(
        "[{}] Adding tool result message for {}, tool_call_id: {}, message_id: {}",
        session_id,
        tool_call.function.name,
        tool_call.id,
        tool_result_msg.id
    );
    session.add_message(tool_result_msg);
}

pub(super) async fn emit_need_clarification_event(
    event_tx: &mpsc::Sender<AgentEvent>,
    payload: &UserQuestionPayload,
    tool_call_id: &str,
    tool_name: &str,
    source: PendingQuestionSource,
) {
    let _ = event_tx
        .send(AgentEvent::NeedClarification {
            question: payload.question.clone(),
            options: if payload.options.is_empty() {
                None
            } else {
                Some(payload.options.clone())
            },
            tool_call_id: Some(tool_call_id.to_string()),
            tool_name: Some(tool_name.to_string()),
            allow_custom: payload.allow_custom,
            source: Some(source),
        })
        .await;
}

pub(super) async fn persist_session_after_question(
    config: &AgentLoopConfig,
    session: &mut Session,
    _session_id: &str,
) -> std::io::Result<()> {
    if let Some(ref persistence) = config.persistence {
        persistence.save_runtime_session(session).await?;
    }
    Ok(())
}

pub(super) async fn emit_clarification_persistence_error(
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    error: &std::io::Error,
) {
    tracing::error!(
        session_id,
        %error,
        "Failed to persist clarification; suppressing NeedClarification"
    );
    let _ = event_tx
        .send(AgentEvent::Error {
            message: format!(
                "Clarification could not be saved; retry after session storage recovers: {error}"
            ),
        })
        .await;
}
