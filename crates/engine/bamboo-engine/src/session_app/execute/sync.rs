//! Client/server sync evaluation and the server execute snapshot.

use bamboo_agent_core::Message;
use bamboo_domain::Session;

use super::super::types::{ExecuteClientSync, ExecuteSyncReason, ServerExecuteSnapshot};
use super::resume_markers::has_pending_user_message;

pub fn evaluate_client_sync(
    client_sync: Option<&ExecuteClientSync>,
    server_snapshot: &ServerExecuteSnapshot,
) -> Option<ExecuteSyncReason> {
    let client_sync = client_sync?;

    let client_pending_question_tool_call_id = client_sync
        .client_pending_question_tool_call_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let server_pending_question_tool_call_id = server_snapshot
        .pending_question_tool_call_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if client_sync.client_has_pending_question != server_snapshot.has_pending_question {
        return Some(ExecuteSyncReason::PendingQuestionMismatch);
    }

    if client_sync.client_has_pending_question
        && client_pending_question_tool_call_id.is_some()
        && client_pending_question_tool_call_id != server_pending_question_tool_call_id
    {
        return Some(ExecuteSyncReason::PendingQuestionMismatch);
    }

    if client_sync.client_message_count != server_snapshot.message_count {
        return Some(ExecuteSyncReason::MessageCountMismatch);
    }

    let client_last_message_id = client_sync
        .client_last_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let server_last_message_id = server_snapshot
        .last_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if client_last_message_id != server_last_message_id {
        return Some(ExecuteSyncReason::LastMessageIdMismatch);
    }

    None
}

/// Returns true if the message is flagged `metadata.hidden_from_ui == true`.
///
/// Such messages are runtime-injected (e.g. child-completion resume, retry
/// resume, conclusion-with-options resume) and are filtered out of any
/// client-facing view. Both `GET /history` and the execute sync snapshot
/// must use this exact predicate so the client and server agree on the
/// visible message_count and last_message_id.
pub fn is_hidden_from_ui(message: &Message) -> bool {
    message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("hidden_from_ui"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

impl ServerExecuteSnapshot {
    pub fn from_session(session: &Session) -> Self {
        // Mirror the GET /history filter so the client (which only ever sees
        // visible messages) and the server agree on message_count /
        // last_message_id. Hidden runtime resume messages stay internal and
        // do not leak into the sync protocol.
        let visible: Vec<&Message> = session
            .messages
            .iter()
            .filter(|message| !is_hidden_from_ui(message))
            .collect();
        Self {
            message_count: visible.len(),
            last_message_id: visible.last().map(|message| message.id.clone()),
            has_pending_question: session.pending_question.is_some(),
            pending_question_tool_call_id: session
                .pending_question
                .as_ref()
                .map(|pending| pending.tool_call_id.clone()),
            has_pending_user_message: has_pending_user_message(session),
        }
    }
}
