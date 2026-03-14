use tokio::sync::mpsc;

use crate::agent::core::tools::ToolCall;
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;

use super::payload::UserQuestionPayload;

pub(super) fn append_waiting_tool_result_message(
    session: &mut Session,
    tool_call: &ToolCall,
    question: &str,
    session_id: &str,
) {
    let tool_result_msg = crate::agent::core::Message::tool_result(
        tool_call.id.clone(),
        format!("Waiting for user response to: {}", question),
    );
    log::debug!(
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
) {
    let _ = event_tx
        .send(AgentEvent::NeedClarification {
            question: payload.question.clone(),
            options: if payload.options.is_empty() {
                None
            } else {
                Some(payload.options.clone())
            },
        })
        .await;
}

pub(super) async fn persist_session_after_question(
    config: &AgentLoopConfig,
    session: &Session,
    session_id: &str,
) {
    if let Some(ref storage) = config.storage {
        if let Err(error) = storage.save_session(session).await {
            log::warn!(
                "[{}] Failed to save session after user-question tool: {}",
                session_id,
                error
            );
        }
    }
}
