use actix_web::{web, HttpResponse, Result};
use std::collections::HashSet;

use super::shared::{ensure_session_not_running, load_session_or_404, save_and_cache_session};
use super::types::TruncateRequest;
use crate::agent::core::agent::Role;
use crate::agent::core::{Message, Session};
use crate::server::app_state::AppState;

const RETRY_RESUME_PENDING_KEY: &str = "retry_resume_pending";
const RETRY_RESUME_REASON_KEY: &str = "retry_resume_reason";

fn unresolved_tool_call_ids(messages: &[Message]) -> HashSet<String> {
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

fn truncate_after_last_user(session: &mut Session) -> Option<usize> {
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

fn truncate_for_unresolved_tool_calls(session: &mut Session) -> Option<usize> {
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

/// `POST /api/v1/sessions/{session_id}/messages/truncate`
pub async fn truncate_messages(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<TruncateRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    if let Some(response) = ensure_session_not_running(&state, &session_id).await {
        return Ok(response);
    }

    let Some(mut session) = load_session_or_404(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let (removed, new_len, should_clear_derived_state, should_persist) = match req.into_inner() {
        TruncateRequest::AfterLastUser => {
            let Some(removed) = truncate_after_last_user(&mut session) else {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "No user message found to truncate after",
                    "session_id": session_id
                })));
            };

            let cleared_pending_flag = session.metadata.remove(RETRY_RESUME_PENDING_KEY).is_some();
            let cleared_reason_flag = session.metadata.remove(RETRY_RESUME_REASON_KEY).is_some();
            let cleared_retry_flags = cleared_pending_flag || cleared_reason_flag;
            (
                removed,
                session.messages.len(),
                removed > 0,
                removed > 0 || cleared_retry_flags,
            )
        }
        TruncateRequest::ErrorRetry => {
            if unresolved_tool_call_ids(&session.messages).is_empty() {
                session
                    .metadata
                    .insert(RETRY_RESUME_PENDING_KEY.to_string(), "true".to_string());
                session.metadata.insert(
                    RETRY_RESUME_REASON_KEY.to_string(),
                    "error_retry".to_string(),
                );
                (0, session.messages.len(), false, true)
            } else {
                tracing::warn!(
                    "[{}] error_retry requested with unresolved tool calls; falling back to truncation",
                    session_id
                );
                let Some(removed) = truncate_for_unresolved_tool_calls(&mut session) else {
                    return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                        "error": "Found unresolved tool calls but no prior user message to retry from",
                        "session_id": session_id
                    })));
                };

                let cleared_pending_flag =
                    session.metadata.remove(RETRY_RESUME_PENDING_KEY).is_some();
                let cleared_reason_flag =
                    session.metadata.remove(RETRY_RESUME_REASON_KEY).is_some();
                let cleared_retry_flags = cleared_pending_flag || cleared_reason_flag;

                (
                    removed,
                    session.messages.len(),
                    removed > 0,
                    removed > 0 || cleared_retry_flags,
                )
            }
        }
    };

    if should_clear_derived_state {
        // Truncation invalidates derived context state.
        session.token_usage = None;
        session.conversation_summary = None;
    }

    if should_persist {
        save_and_cache_session(&state, &session_id, session).await?;
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "messages_removed": removed,
        "message_count": new_len,
    })))
}

#[cfg(test)]
mod tests {
    use super::{
        truncate_after_last_user, truncate_for_unresolved_tool_calls, unresolved_tool_call_ids,
    };
    use crate::agent::core::tools::{FunctionCall, ToolCall};
    use crate::agent::core::{Message, Session};

    fn make_tool_call(id: &str) -> ToolCall {
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
            Some(crate::agent::core::agent::Role::User)
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
}
