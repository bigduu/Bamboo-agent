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

/// Check the active tool exchange before projecting a message summary.
pub fn validate_message_summary_tool_protocol(
    session: &Session,
) -> Result<(), MessageSummaryToolProtocolError> {
    pinned_tool_batch_rows(session).map(|_| ())
}

/// Derive the exact eligible archive set for planning and durable commit.
/// Retain the latest user message, system rows, and explicit pins. A pin
/// anywhere in a complete tool batch protects every call and result in it.
pub fn message_summary_archive_message_ids(
    session: &Session,
) -> Result<HashSet<&str>, MessageSummaryToolProtocolError> {
    let pinned_rows = pinned_tool_batch_rows(session)?;
    let latest_user = session
        .messages
        .iter()
        .rposition(|message| !message.compressed && message.role == Role::User);
    let mut archive = HashSet::new();
    let mut retained = HashSet::new();
    for (index, message) in session.messages.iter().enumerate() {
        if message.compressed {
            continue;
        }
        let ids = if message.role == Role::System
            || Some(index) == latest_user
            || pinned_rows.contains(&index)
            || (message.never_compress && !is_post_compaction_recovery(message))
        {
            &mut retained
        } else {
            &mut archive
        };
        ids.insert(message.id.as_str());
    }
    if !archive.is_disjoint(&retained) {
        return Err(MessageSummaryToolProtocolError::Invalid(
            "a message identifier is shared by archived and retained rows",
        ));
    }
    Ok(archive)
}

/// Legacy generated recovery prose is retired when rebuilding a message summary.
pub fn is_post_compaction_recovery(message: &Message) -> bool {
    message.role == Role::Assistant && message.content.starts_with("[post-compaction-recovery]\n")
}

fn pinned_tool_batch_rows(
    session: &Session,
) -> Result<HashSet<usize>, MessageSummaryToolProtocolError> {
    use MessageSummaryToolProtocolError::{Invalid, PendingResults};

    if message_summary_tool_work_pending(session) {
        return Err(PendingResults);
    }
    let latest_request = session
        .messages
        .iter()
        .rposition(|message| !message.compressed && is_new_user_request(message));
    let mut pending_calls = HashSet::new();
    let mut batch = Vec::new();
    let mut pinned_rows = HashSet::new();
    let batch_is_pinned = |rows: &[usize]| {
        rows.iter()
            .any(|&index| session.messages[index].never_compress)
    };
    for (index, message) in session.messages.iter().enumerate() {
        if message.compressed {
            continue;
        }
        let calls = message.tool_calls.as_deref().unwrap_or_default();

        // Tool call IDs belong to one assistant batch. A missing result can
        // be left in the raw transcript only once a newer user turn supersedes
        // the batch. A synthetic runtime resume is part of the same turn.
        if matches!(message.role, Role::Assistant | Role::User) && !pending_calls.is_empty() {
            if latest_request.is_none_or(|request| index > request) {
                return Err(Invalid("a previous tool batch is missing results"));
            }
            if batch_is_pinned(&batch) {
                return Err(Invalid("a pinned tool batch is missing results"));
            }
            pending_calls.clear();
            batch.clear();
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
        if !calls.is_empty() || message.role == Role::Tool {
            batch.push(index);
            if pending_calls.is_empty() {
                if batch_is_pinned(&batch) {
                    pinned_rows.extend(batch.iter().copied());
                }
                batch.clear();
            }
        }
    }
    if !pending_calls.is_empty() {
        return Err(PendingResults);
    }
    Ok(pinned_rows)
}
