//! Pure helpers for child session management.

use bamboo_domain::Session;
use serde_json::json;

use super::{
    ChildRunnerInfo, ChildSessionEntry, ChildSessionError, CHILD_SYSTEM_PROMPT,
    PLAN_AGENT_SYSTEM_PROMPT,
};

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

/// Resolve the system prompt for a child session.
///
/// - When `override_prompt` is `Some`, that value is used verbatim. Callers
///   resolve this from the [`SubagentProfileRegistry`] before invoking
///   `create_child_action`.
/// - When `override_prompt` is `None`, falls back to the legacy hard-coded
///   prompts: [`PLAN_AGENT_SYSTEM_PROMPT`] when `subagent_type == "plan"`
///   (case-insensitive, surrounding whitespace ignored), and
///   [`CHILD_SYSTEM_PROMPT`] otherwise. This keeps unwired call paths
///   byte-for-byte equivalent to pre-PR-3 behaviour.
pub fn resolve_system_prompt<'a>(
    subagent_type: &str,
    override_prompt: Option<&'a str>,
) -> std::borrow::Cow<'a, str> {
    if let Some(prompt) = override_prompt {
        std::borrow::Cow::Borrowed(prompt)
    } else if subagent_type.trim().eq_ignore_ascii_case("plan") {
        std::borrow::Cow::Borrowed(PLAN_AGENT_SYSTEM_PROMPT)
    } else {
        std::borrow::Cow::Borrowed(CHILD_SYSTEM_PROMPT)
    }
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

pub fn replace_or_append_last_user_message(session: &mut Session, content: String) -> usize {
    use bamboo_agent_core::Role;

    if let Some(index) = session
        .messages
        .iter()
        .rposition(|message| matches!(message.role, Role::User))
    {
        session.messages[index].content = content;
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
