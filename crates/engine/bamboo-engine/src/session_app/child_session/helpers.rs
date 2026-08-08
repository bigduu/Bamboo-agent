//! Pure helpers for child session management.

use bamboo_domain::{ModelContextResetReason, Session};
use serde_json::json;

use super::{ChildRunnerInfo, ChildSessionEntry, ChildSessionError};

pub fn normalize_non_empty_optional(
    value: Option<String>,
    field_name: &str,
) -> Result<Option<String>, ChildSessionError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ChildSessionError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    }
    Ok(Some(trimmed.to_string()))
}

pub fn normalize_required_text(
    value: Option<String>,
    field_name: &str,
) -> Result<String, ChildSessionError> {
    let Some(value) = value else {
        return Err(ChildSessionError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ChildSessionError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    }
    Ok(trimmed.to_string())
}

pub fn metadata_text(session: &Session, key: &str) -> Option<String> {
    session
        .metadata
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn format_child_assignment(
    title: &str,
    responsibility: &str,
    subagent_type: &str,
    prompt: &str,
) -> String {
    format!(
        "Sub-session title: {}\nResponsibility: {}\nSubagent type: {}\n\nTask brief:\n{}",
        title, responsibility, subagent_type, prompt
    )
}

/// Render the last `n` non-system messages of `parent` into a compact
/// "forked context" block for seeding a child sub-session (Phase 3
/// model-controllable context fork). Returns `None` when `n == 0` or there is no
/// non-system content. Rendered as `role: content` lines (content trimmed to a
/// sane length) — a single text block, NOT spliced raw messages, so it can be
/// safely prepended to the child's task brief without breaking role structure.
pub fn render_forked_parent_context(parent: &Session, n: usize) -> Option<String> {
    use bamboo_agent_core::Role;
    if n == 0 {
        return None;
    }
    let mut recent: Vec<&bamboo_agent_core::Message> = parent
        .messages
        .iter()
        .filter(|m| !matches!(m.role, Role::System))
        .rev()
        .take(n)
        .collect();
    recent.reverse();

    let rendered: Vec<String> = recent
        .into_iter()
        .filter_map(|m| {
            let content = m.content.trim();
            if content.is_empty() {
                return None;
            }
            let role = match m.role {
                Role::User => "user",
                Role::Assistant => "assistant",
                _ => "context",
            };
            let snippet = if content.chars().count() > 2000 {
                let truncated: String = content.chars().take(2000).collect();
                format!("{truncated}…")
            } else {
                content.to_string()
            };
            Some(format!("{role}: {snippet}"))
        })
        .collect();

    if rendered.is_empty() {
        None
    } else {
        Some(format!(
            "## Forked context from parent (last {} message(s))\n{}",
            rendered.len(),
            rendered.join("\n")
        ))
    }
}

#[cfg(test)]
mod fork_context_tests {
    use super::render_forked_parent_context;
    use bamboo_agent_core::{Message, Session};

    #[test]
    fn renders_recent_non_system_messages() {
        let mut parent = Session::new("p", "model");
        parent.add_message(Message::system("you are root"));
        parent.add_message(Message::user("first user msg"));
        parent.add_message(Message::assistant("assistant reply", None));
        parent.add_message(Message::user("latest ask"));

        let forked = render_forked_parent_context(&parent, 2).expect("renders");
        assert!(forked.contains("Forked context from parent"));
        // Last 2 non-system messages, oldest-first.
        assert!(forked.contains("assistant: assistant reply"));
        assert!(forked.contains("user: latest ask"));
        // The older user msg + the system msg are excluded by n=2 / system filter.
        assert!(!forked.contains("first user msg"));
        assert!(!forked.contains("you are root"));
    }

    #[test]
    fn none_when_zero_or_empty() {
        let mut parent = Session::new("p", "model");
        parent.add_message(Message::user("hi"));
        assert!(render_forked_parent_context(&parent, 0).is_none());
        let empty = Session::new("p2", "model");
        assert!(render_forked_parent_context(&empty, 5).is_none());
    }
}

pub fn replace_or_append_last_user_message(session: &mut Session, content: String) -> usize {
    use bamboo_agent_core::Role;

    if let Some(index) = session
        .messages
        .iter()
        .rposition(|message| matches!(message.role, Role::User))
    {
        let rewrites_visible_history =
            session.messages[index].content != content && session.model_context_state.is_some();
        session.messages[index].content = content;
        if rewrites_visible_history {
            session.reset_model_context_epoch(ModelContextResetReason::Rollback);
        }
        return index;
    }

    session.add_message(bamboo_agent_core::Message::user(content));
    session.messages.len().saturating_sub(1)
}

pub fn truncate_after_index(session: &mut Session, keep_last_index: usize) -> usize {
    let keep_len = keep_last_index.saturating_add(1);
    let removed = session.messages.len().saturating_sub(keep_len);
    if removed > 0 {
        session.messages.truncate(keep_len);
        session.token_usage = None;
        session.conversation_summary = None;
        session.reset_model_context_epoch(ModelContextResetReason::Rollback);
    }
    removed
}

pub fn truncate_after_last_user(session: &mut Session) -> Result<usize, ChildSessionError> {
    use bamboo_agent_core::Role;

    let Some(last_user_idx) = session
        .messages
        .iter()
        .rposition(|message| matches!(message.role, Role::User))
    else {
        return Err(ChildSessionError::Execution(
            "No user message found to retry from".to_string(),
        ));
    };

    Ok(truncate_after_index(session, last_user_idx))
}

pub fn map_child_entry(entry: &ChildSessionEntry) -> serde_json::Value {
    json!({
        "child_session_id": entry.child_session_id,
        "title": entry.title,
        "pinned": entry.pinned,
        "message_count": entry.message_count,
        "updated_at": entry.updated_at,
        "last_run_status": entry.last_run_status,
        "last_run_error": entry.last_run_error,
    })
}

/// Generate contextual guidance for the root LLM based on child status and runner info.
pub fn compute_status_guidance(
    status: Option<&str>,
    runner_info: Option<&ChildRunnerInfo>,
    has_pending_messages: bool,
) -> String {
    match status {
        Some("running") => {
            let mut parts = vec!["Child is active.".to_string()];
            if let Some(info) = runner_info {
                if let Some(ref tool_name) = info.last_tool_name {
                    if info.last_tool_phase.as_deref() == Some("begin") {
                        parts.push(format!("Currently executing tool: {tool_name}. Wait for completion."));
                    } else {
                        parts.push(format!("Last tool: {tool_name} ({}).", info.last_tool_phase.as_deref().unwrap_or("unknown")));
                    }
                }
                if let Some(last_event) = info.last_event_at {
                    let elapsed = chrono::Utc::now().signed_duration_since(last_event);
                    let secs = elapsed.num_seconds();
                    if secs < 30 {
                        parts.push("Progress event received very recently. Do not create a replacement; wait 30-60s.".to_string());
                    } else if secs > 120 {
                        parts.push("No progress event for 120s. Consider send_message or cancel if stalled.".to_string());
                    }
                }
            }
            if has_pending_messages {
                parts.push("A follow-up message is already queued and will be picked up at the next turn boundary.".to_string());
            } else {
                parts.push("Use send_message with interrupt_running=false to queue a follow-up, or interrupt_running=true to cancel and restart.".to_string());
            }
            parts.join(" ")
        }
        Some("error") => "Child failed. Use send_message with corrected instructions to retry in place, or create a new child only if the approach needs to change completely.".to_string(),
        Some("completed") => "Child finished. Use get to read results, or send_message for follow-up work.".to_string(),
        Some("pending") => "Child is waiting to run. Use action=run to start execution.".to_string(),
        Some("cancelled") => "Child was cancelled. Use send_message to resume, or action=run to restart.".to_string(),
        Some("skipped") => "Child had no pending message. Use send_message to add work, then action=run.".to_string(),
        _ => "Use action=get to inspect progress, send_message to redirect, or create only if a new delegation is needed.".to_string(),
    }
}
