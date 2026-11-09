use std::collections::HashSet;

use super::{
    conversation_compaction_pending_rejection, is_system_resume_message,
    ConversationResetRejection, Role, Session,
};

#[derive(Debug, thiserror::Error)]
pub enum MessageSummaryToolProtocolError {
    #[error("the current tool batch is waiting for results")]
    PendingResults,
    #[error("{0}")]
    Invalid(&'static str),
}

/// Pending tool work can still add results to the history being considered.
/// Ordinary running rounds can compact after their current batch completes.
pub fn message_summary_history_recovery_blocked(session: &Session) -> bool {
    session.has_runtime_wait()
        || session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|runtime| runtime.tools.active_tool_calls > 0)
        || matches!(
            conversation_compaction_pending_rejection(session),
            Some(
                ConversationResetRejection::PendingApproval
                    | ConversationResetRejection::PendingWait
                    | ConversationResetRejection::UncertainOperation
                    | ConversationResetRejection::LegacyRuntimeState
            )
        )
}

/// Check the tool rows that a message summary would archive in their entirety.
/// Returns true when the archive includes unanswered calls superseded by a
/// later user request. Those rows stay intact in durable history.
pub fn validate_message_summary_tool_protocol(
    session: &Session,
) -> Result<bool, MessageSummaryToolProtocolError> {
    use MessageSummaryToolProtocolError::{Invalid, PendingResults};

    let latest_request = if message_summary_history_recovery_blocked(session) {
        None
    } else {
        session.messages.iter().rposition(|message| {
            !message.compressed && message.role == Role::User && !is_system_resume_message(message)
        })
    };
    let mut pending_calls = HashSet::new();
    let mut archives_unanswered_calls = false;
    for (index, message) in session.messages.iter().enumerate() {
        if message.compressed {
            continue;
        }
        let calls = message.tool_calls.as_deref().unwrap_or_default();
        let has_tool_protocol =
            !calls.is_empty() || message.tool_call_id.is_some() || message.role == Role::Tool;
        if message.never_compress && has_tool_protocol {
            return Err(Invalid("a tool-chain record is marked never_compress"));
        }

        // Tool call IDs belong to one assistant batch. A missing result can
        // be left in the raw transcript only once a newer user turn supersedes
        // the batch. A synthetic runtime resume is part of the same turn.
        if matches!(message.role, Role::Assistant | Role::User) && !pending_calls.is_empty() {
            if latest_request.is_none_or(|request| index > request) {
                return Err(Invalid("a previous tool batch is missing results"));
            }
            pending_calls.clear();
            archives_unanswered_calls = true;
        }
        if !calls.is_empty() {
            if message.role != Role::Assistant {
                return Err(Invalid("tool calls have a non-assistant role"));
            }
            for call in calls {
                if call.id.is_empty() || !pending_calls.insert(call.id.as_str()) {
                    return Err(Invalid("tool calls have an empty or duplicate identifier"));
                }
            }
        }
        if message.role == Role::Tool {
            let id = message
                .tool_call_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or(Invalid("a tool result has no call identifier"))?;
            if !pending_calls.remove(id) {
                return Err(Invalid("a tool result has no pending call in its batch"));
            }
        } else if message.tool_call_id.is_some() {
            return Err(Invalid("a tool result identifier has a non-tool role"));
        }
    }
    if !pending_calls.is_empty() {
        return Err(PendingResults);
    }
    Ok(archives_unanswered_calls)
}
