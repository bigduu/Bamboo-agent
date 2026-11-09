use std::collections::HashSet;

use super::{is_system_resume_message, message_summary_tool_work_pending, Message, Role, Session};

#[derive(Debug, thiserror::Error)]
pub enum MessageSummaryToolProtocolError {
    #[error("the session has pending tool work")]
    PendingResults,
    #[error("{0}")]
    Invalid(&'static str),
}

fn is_new_user_request(message: &Message) -> bool {
    message.role == Role::User
        && !is_system_resume_message(message)
        && !matches!(
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("runtime_kind"))
                .and_then(serde_json::Value::as_str),
            Some(
                "stop_hook_continuation"
                    | "run_budget_summary"
                    | "max_rounds_summary"
                    | "counted_loop_iteration"
            )
        )
}

/// Check the tool rows that a message summary would archive in their entirety.
/// Unanswered calls superseded by a later user request stay in durable history.
pub fn validate_message_summary_tool_protocol(
    session: &Session,
) -> Result<(), MessageSummaryToolProtocolError> {
    use MessageSummaryToolProtocolError::{Invalid, PendingResults};

    if message_summary_tool_work_pending(session) {
        return Err(PendingResults);
    }
    let latest_request = session
        .messages
        .iter()
        .rposition(|message| !message.compressed && is_new_user_request(message));
    let mut pending_calls = HashSet::new();
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
    Ok(())
}
