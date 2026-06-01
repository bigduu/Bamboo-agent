//! Message truncation use cases.
//!
//! Pure functions for removing messages from a session to support
//! error recovery and cleanup.

use std::collections::HashSet;

use bamboo_agent_core::{Message, Role, Session};

/// Find tool call IDs that have no corresponding tool result.
pub fn unresolved_tool_call_ids(messages: &[Message]) -> HashSet<String> {
    let mut pending_ids = HashSet::new();

    for message in messages {
        if let Some(tool_calls) = &message.tool_calls {
            for call in tool_calls {
                if !call.id.trim().is_empty() {
                    pending_ids.insert(call.id.clone());
                }
            }
        }

        if matches!(message.role, Role::Tool) {
            if let Some(tool_call_id) = &message.tool_call_id {
                pending_ids.remove(tool_call_id);
            }
        }
    }

    pending_ids
}

/// Remove assistant tool calls that have no result, and tool results that have no call.
///
/// Returns the total number of messages and tool calls removed.
pub fn sanitize_malformed_tool_chains(session: &mut Session) -> usize {
    let resolved_tool_result_ids: HashSet<String> = session
        .messages
        .iter()
        .filter(|message| matches!(message.role, Role::Tool))
        .filter_map(|message| {
            message
                .tool_call_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
        })
        .collect();

    let mut removed_assistant_calls = 0usize;
    for message in session.messages.iter_mut() {
        if !matches!(message.role, Role::Assistant) {
            continue;
        }
        let Some(tool_calls) = message.tool_calls.take() else {
            continue;
        };
        let original_len = tool_calls.len();
        let kept_calls = tool_calls
            .into_iter()
            .filter(|call| {
                let id = call.id.trim();
                !id.is_empty() && resolved_tool_result_ids.contains(id)
            })
            .collect::<Vec<_>>();
        removed_assistant_calls += original_len.saturating_sub(kept_calls.len());
        message.tool_calls = if kept_calls.is_empty() {
            None
        } else {
            Some(kept_calls)
        };
    }

    let valid_tool_call_ids: HashSet<String> = session
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_ref())
        .flatten()
        .filter_map(|call| {
            let id = call.id.trim();
            if id.is_empty() {
                None
            } else {
                Some(id.to_string())
            }
        })
        .collect();

    let before_tool_results = session
        .messages
        .iter()
        .filter(|message| matches!(message.role, Role::Tool))
        .count();
    session.messages.retain(|message| {
        if !matches!(message.role, Role::Tool) {
            return true;
        }
        message
            .tool_call_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .is_some_and(|id| valid_tool_call_ids.contains(id))
    });
    let after_tool_results = session
        .messages
        .iter()
        .filter(|message| matches!(message.role, Role::Tool))
        .count();
    let removed_tool_results = before_tool_results.saturating_sub(after_tool_results);

    removed_assistant_calls + removed_tool_results
}

/// Truncate all messages after the last user message.
///
/// Returns `None` if there is no user message.
pub fn truncate_after_last_user(session: &mut Session) -> Option<usize> {
    let last_user_idx = session
        .messages
        .iter()
        .rposition(|message| matches!(message.role, Role::User))?;

    let keep_len = last_user_idx + 1;
    let removed = session.messages.len().saturating_sub(keep_len);
    if removed > 0 {
        session.messages.truncate(keep_len);
    }
    Some(removed)
}

/// Truncate at the user message preceding the first unresolved tool call.
///
/// Returns `None` if there are no unresolved calls or no prior user anchor.
pub fn truncate_for_unresolved_tool_calls(session: &mut Session) -> Option<usize> {
    let unresolved = unresolved_tool_call_ids(&session.messages);
    if unresolved.is_empty() {
        return Some(0);
    }

    let first_unresolved_call_idx = session.messages.iter().position(|message| {
        message
            .tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|call| unresolved.contains(&call.id)))
    })?;

    let retry_anchor_idx = session.messages[..first_unresolved_call_idx]
        .iter()
        .rposition(|message| matches!(message.role, Role::User))?;

    let keep_len = retry_anchor_idx + 1;
    let removed = session.messages.len().saturating_sub(keep_len);
    if removed > 0 {
        session.messages.truncate(keep_len);
    }
    Some(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tool_call(id: &str) -> bamboo_agent_core::tools::ToolCall {
        use bamboo_agent_core::tools::{FunctionCall, ToolCall};
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "test_tool".to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    #[test]
    fn unresolved_tool_call_ids_tracks_only_missing_outputs() {
        let messages = vec![
            Message::user("question"),
            Message::assistant("", Some(vec![make_tool_call("call_1")])),
            Message::tool_result("call_1", "ok"),
            Message::assistant("", Some(vec![make_tool_call("call_2")])),
        ];

        let unresolved = unresolved_tool_call_ids(&messages);
        assert_eq!(unresolved.len(), 1);
        assert!(unresolved.contains("call_2"));
    }

    #[test]
    fn truncate_for_unresolved_tool_calls_keeps_latest_safe_user_turn() {
        let mut session = Session::new("session-1", "gpt-5");
        session.add_message(Message::system("system"));
        session.add_message(Message::user("task-1"));
        session.add_message(Message::assistant("", Some(vec![make_tool_call("call_1")])));
        session.add_message(Message::tool_result("call_1", "done"));
        session.add_message(Message::assistant("done", None));
        session.add_message(Message::user("task-2"));
        session.add_message(Message::assistant("", Some(vec![make_tool_call("call_2")])));

        let removed =
            truncate_for_unresolved_tool_calls(&mut session).expect("truncate should succeed");
        assert_eq!(removed, 1);
        assert!(matches!(
            session.messages.last().map(|message| &message.role),
            Some(Role::User)
        ));
        assert_eq!(
            session
                .messages
                .last()
                .map(|message| message.content.as_str()),
            Some("task-2")
        );
    }

    #[test]
    fn truncate_for_unresolved_tool_calls_requires_prior_user_anchor() {
        let mut session = Session::new("session-1", "gpt-5");
        session.add_message(Message::assistant("", Some(vec![make_tool_call("call_1")])));

        assert!(truncate_for_unresolved_tool_calls(&mut session).is_none());
    }

    #[test]
    fn truncate_after_last_user_truncates_tail_messages() {
        let mut session = Session::new("session-1", "gpt-5");
        session.add_message(Message::system("system"));
        session.add_message(Message::user("question"));
        session.add_message(Message::assistant("answer", None));

        let removed = truncate_after_last_user(&mut session).expect("user anchor exists");
        assert_eq!(removed, 1);
        assert_eq!(session.messages.len(), 2);
    }

    #[test]
    fn sanitize_malformed_tool_chains_prunes_orphan_results_and_unresolved_calls() {
        let mut session = Session::new("session-1", "gpt-5");
        session.add_message(Message::user("task-1"));
        session.add_message(Message::assistant(
            "tool call without result",
            Some(vec![make_tool_call("call_missing")]),
        ));
        session.add_message(Message::tool_result("call_orphan", "error"));
        session.add_message(Message::user("continue"));

        let removed = sanitize_malformed_tool_chains(&mut session);
        assert_eq!(removed, 2);
        assert!(unresolved_tool_call_ids(&session.messages).is_empty());
        assert!(session
            .messages
            .iter()
            .all(|message| !matches!(message.role, Role::Tool)));
        assert!(session.messages.iter().any(|message| {
            matches!(message.role, Role::Assistant)
                && message.content == "tool call without result"
                && message.tool_calls.is_none()
        }));
    }
}
