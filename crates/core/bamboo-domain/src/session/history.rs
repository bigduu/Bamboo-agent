//! The visible transcript and its page boundaries, shared by history and execution sync.

use std::{borrow::Borrow, collections::HashMap};

use chrono::{DateTime, Utc};

use super::types::{CompressionEvent, Message, Role, Session};

pub const RECENT_HISTORY_MESSAGE_LIMIT: usize = 100;
pub const APPROVAL_HANDOFF_SUBSYSTEM: &str = "supervisor_approval_handoff";
pub const APPROVAL_HANDOFF_INSTRUCTION: &str = "supervisor_approval_handoff_v1";

/// Internal runtime records still participate in the model's transcript.
pub fn is_hidden_from_ui(message: &Message) -> bool {
    message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("hidden_from_ui"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        || is_internal_handoff(message)
}

/// The canonical Inbox envelope identifies runtime notices, including old
/// receipts. A human message that quotes the same text is still visible.
pub fn is_internal_handoff(message: &Message) -> bool {
    let Some(marker) = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("session_message"))
    else {
        return false;
    };
    message.role == Role::User
        && marker["id"].as_str() == Some(message.id.as_str())
        && marker["source"]["type"].as_str() == Some("runtime")
        && marker["kind"].as_str() == Some("runtime_instruction")
        && marker["body"]["type"].as_str() == Some("runtime_instruction")
        && matches!(
            (
                marker["source"]["subsystem"].as_str(),
                marker["body"]["instruction"].as_str(),
            ),
            (
                Some("supervised_root_outcome"),
                Some("supervised_root_outcome_v1" | "managed_root_blocker_v1")
            ) | (
                Some(APPROVAL_HANDOFF_SUBSYSTEM),
                Some(APPROVAL_HANDOFF_INSTRUCTION)
            )
        )
}

/// Keep an assistant tool batch together when the nominal cut falls inside
/// its results or intervening System records. A new User/Assistant record
/// starts a different batch. This also keeps an unfinished batch together as
/// further results are appended.
pub fn history_page_start<T: Borrow<Message>>(messages: &[T], end: usize, limit: usize) -> usize {
    let start = end.saturating_sub(limit);
    if start == 0
        || start == end
        || !matches!(messages[start].borrow().role, Role::Tool | Role::System)
    {
        return start;
    }
    let batch = messages[..start]
        .iter()
        .rposition(|message| matches!(message.borrow().role, Role::User | Role::Assistant))
        .filter(|&index| {
            let message = messages[index].borrow();
            message.role == Role::Assistant
                && message
                    .tool_calls
                    .as_ref()
                    .is_some_and(|calls| !calls.is_empty())
        });
    batch.unwrap_or(start)
}

/// The lifetime and reset revision of a transcript being read.
#[derive(Clone, Copy)]
pub struct HistoryScope {
    pub created_at: DateTime<Utc>,
    pub conversation_revision: u64,
}

impl From<&Session> for HistoryScope {
    fn from(session: &Session) -> Self {
        Self {
            created_at: session.created_at,
            conversation_revision: session.conversation_revision(),
        }
    }
}

/// A presentation read with the count of the complete visible transcript.
/// Keeping a distinct type prevents a page being used as a writable Session.
pub struct SessionHistoryWindow {
    pub scope: HistoryScope,
    pub messages: Vec<Message>,
    pub total_message_count: usize,
    pub compression_events: Vec<CompressionEvent>,
    pub metadata: HashMap<String, String>,
}

impl SessionHistoryWindow {
    pub fn from_session(mut session: Session, limit: usize) -> Self {
        session
            .messages
            .retain(|message| !is_hidden_from_ui(message));
        let total_message_count = session.messages.len();
        let start = history_page_start(&session.messages, total_message_count, limit);
        Self {
            scope: HistoryScope::from(&session),
            messages: session.messages.split_off(start),
            total_message_count,
            compression_events: session.compression_events,
            metadata: session.metadata,
        }
    }
}
