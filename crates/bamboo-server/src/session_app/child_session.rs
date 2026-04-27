//! Child session management use cases.
//!
//! Provides the application-layer logic for managing child sessions within
//! a root session. The server layer implements `ChildSessionPort` to supply
//! the infrastructure operations (load, save, schedule, cancel).

use async_trait::async_trait;
use bamboo_domain::Session;
use chrono::Utc;
use serde_json::json;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ChildSessionError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("session is not a root session: {0}")]
    NotRootSession(String),
    #[error("session is not a child session: {0}")]
    NotChildSession(String),
    #[error("child session {child_id} does not belong to parent {parent_id}")]
    NotChildOfParent { child_id: String, parent_id: String },
    #[error("{0}")]
    InvalidArguments(String),
    #[error("{0}")]
    Execution(String),
}

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/// Summary of a child session for listing.
#[derive(Debug, Clone)]
pub struct ChildSessionEntry {
    pub child_session_id: String,
    pub title: String,
    pub pinned: bool,
    pub message_count: usize,
    pub updated_at: String,
    pub last_run_status: Option<String>,
    pub last_run_error: Option<String>,
}

/// Result of deleting a child session.
#[derive(Debug, Clone)]
pub struct DeleteChildResult {
    pub deleted: bool,
    pub cancelled_running_child: bool,
}

/// Diagnostic snapshot of a running child session runner.
#[derive(Debug, Clone)]
pub struct ChildRunnerInfo {
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_tool_name: Option<String>,
    pub last_tool_phase: Option<String>,
    pub last_event_at: Option<chrono::DateTime<chrono::Utc>>,
    pub round_count: u32,
}

/// Default system prompt for child sessions.
pub const CHILD_SYSTEM_PROMPT: &str = r#"You are a **Child Session**, delegated by a parent session.

Requirements:
- Focus only on the assigned task and avoid unrelated conversation.
- You may use tools to complete the task.
- Do not create or trigger any additional child sessions (no recursive spawn).
- Keep output concise: provide the conclusion first, then only necessary evidence or steps.
"#;

/// System prompt for plan-mode exploration child sessions.
pub const PLAN_AGENT_SYSTEM_PROMPT: &str = r#"You are a **Plan Agent**, a read-only exploration specialist delegated by a parent session.

Your role is EXCLUSIVELY to explore the codebase and gather information to help design an implementation plan. You MUST NOT modify anything.

=== CRITICAL: READ-ONLY MODE — NO FILE MODIFICATIONS ===

You are FORBIDDEN from using these tools:
- Write — do not create new files
- Edit — do not modify existing files
- NotebookEdit — do not edit notebooks
- Bash — do not execute shell commands
- BashOutput — do not execute shell commands
- KillShell — do not manage processes
- SubSession — do not spawn further child sessions

You MAY use these read-only tools:
- Read — read file contents
- Glob — list files matching patterns
- Grep — search code for patterns
- GetFileInfo — get file metadata
- WebFetch — fetch web content
- WebSearch — search the web
- MemoryNote — write observations to session memory

Requirements:
- Focus only on the assigned exploration task.
- Provide clear, structured findings: what you discovered, where the relevant code is, and what it does.
- Keep output concise but thorough — the parent session needs enough detail to design a plan.
- If you cannot find something after reasonable searching, say so clearly.
"#;

/// Input for creating a child session.
#[derive(Debug, Clone)]
pub struct CreateChildInput {
    pub parent_session: Session,
    pub child_id: String,
    pub title: String,
    pub responsibility: String,
    pub assignment_prompt: String,
    pub subagent_type: String,
    /// Optional model override resolved from subagent_type routing.
    /// When `None`, the child inherits the parent session's model.
    pub model_override: Option<String>,
    /// Optional provider+model override resolved from subagent routing.
    /// When present, this preserves cross-provider routing for child execution.
    pub model_ref_override: Option<bamboo_domain::ProviderModelRef>,
    /// Runtime metadata resolved from subagent routing (e.g. external agent config).
    pub runtime_metadata: std::collections::HashMap<String, String>,
    /// Whether to immediately enqueue the child for execution.
    /// Defaults to `true`.
    pub auto_run: bool,
}

/// Result of creating a child session.
#[derive(Debug, Clone)]
pub struct CreateChildResult {
    pub child_session_id: String,
    pub model: String,
}

// ---------------------------------------------------------------------------
// Port trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ChildSessionPort: Send + Sync {
    async fn load_root_session(&self, root_id: &str) -> Result<Session, ChildSessionError>;
    async fn load_child_for_parent(
        &self,
        parent_id: &str,
        child_id: &str,
    ) -> Result<Session, ChildSessionError>;
    async fn save_child_session(&self, child: &Session) -> Result<(), ChildSessionError>;
    async fn is_child_running(&self, child_id: &str) -> bool;
    async fn list_children(&self, parent_id: &str) -> Vec<ChildSessionEntry>;
    async fn enqueue_child_run(
        &self,
        parent: &Session,
        child: &Session,
    ) -> Result<(), ChildSessionError>;
    async fn cancel_child_run_and_wait(&self, child_id: &str) -> Result<(), ChildSessionError>;
    async fn delete_child_session(
        &self,
        parent_id: &str,
        child_id: &str,
    ) -> Result<DeleteChildResult, ChildSessionError>;
    /// Return live diagnostic info for a running child session, if available.
    async fn get_child_runner_info(&self, child_id: &str) -> Option<ChildRunnerInfo>;
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Action functions
// ---------------------------------------------------------------------------

pub async fn create_child_action(
    port: &dyn ChildSessionPort,
    input: CreateChildInput,
) -> Result<CreateChildResult, ChildSessionError> {
    use bamboo_agent_core::Message;
    use bamboo_engine::runner::refresh_prompt_snapshot;

    let mut child = Session::new_child(
        input.child_id.clone(),
        input.parent_session.id.clone(),
        input
            .model_ref_override
            .as_ref()
            .map(|model_ref| model_ref.model.clone())
            .or_else(|| input.model_override.clone())
            .unwrap_or_else(|| input.parent_session.model.clone()),
        input.title.clone(),
    );

    if let Some(model_ref) = input.model_ref_override.clone() {
        child.model_ref = Some(model_ref.clone());
        child
            .metadata
            .insert("provider_name".to_string(), model_ref.provider);
    } else if let Some(parent_model_ref) = input.parent_session.model_ref.clone() {
        child.model_ref = Some(parent_model_ref.clone());
        child
            .metadata
            .insert("provider_name".to_string(), parent_model_ref.provider);
    } else if let Some(parent_provider) =
        input.parent_session.metadata.get("provider_name").cloned()
    {
        child
            .metadata
            .insert("provider_name".to_string(), parent_provider);
    }

    child
        .metadata
        .insert("spawned_by".to_string(), "SubSession".to_string());
    child
        .metadata
        .insert("subagent_type".to_string(), input.subagent_type.clone());
    child
        .metadata
        .insert("responsibility".to_string(), input.responsibility.clone());
    child.metadata.insert(
        "assignment_prompt".to_string(),
        input.assignment_prompt.clone(),
    );
    child
        .metadata
        .insert("last_run_status".to_string(), "pending".to_string());
    child.metadata.remove("last_run_error");

    // Apply runtime metadata (e.g. external agent routing).
    for (key, value) in input.runtime_metadata {
        child.metadata.insert(key, value);
    }

    let system_prompt = if input.subagent_type.trim().eq_ignore_ascii_case("plan") {
        PLAN_AGENT_SYSTEM_PROMPT
    } else {
        CHILD_SYSTEM_PROMPT
    };

    child
        .metadata
        .insert("base_system_prompt".to_string(), system_prompt.to_string());

    child.add_message(Message::system(system_prompt));

    // Child sessions get more aggressive compression: trigger at 70% instead
    // of the default 85%, target 35% instead of 40%. This prevents long child
    // tasks from exhausting the context window before the parent can intervene.
    if let Some(ref parent_budget) = input.parent_session.token_budget {
        let mut child_budget = parent_budget.clone();
        child_budget.compression_trigger_percent = 70;
        child_budget.compression_target_percent = 35;
        child.token_budget = Some(child_budget);
    }

    refresh_prompt_snapshot(&mut child);
    let assignment = format_child_assignment(
        &input.title,
        &input.responsibility,
        &input.subagent_type,
        &input.assignment_prompt,
    );
    child.add_message(Message::user(assignment));

    if let Some(parent_task_list) = input.parent_session.task_list.clone() {
        child.set_task_list(parent_task_list);
    }

    let model = child.model.clone();
    port.save_child_session(&child).await?;
    if input.auto_run {
        port.enqueue_child_run(&input.parent_session, &child)
            .await?;
    }

    Ok(CreateChildResult {
        child_session_id: child.id,
        model,
    })
}

pub async fn list_children_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
) -> serde_json::Value {
    let children = port.list_children(parent_id).await;
    json!({
        "parent_session_id": parent_id,
        "children": children.iter().map(map_child_entry).collect::<Vec<_>>(),
        "count": children.len(),
    })
}

pub async fn get_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
) -> Result<serde_json::Value, ChildSessionError> {
    let child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;

    let status = metadata_text(&child, "last_run_status");
    let runner_info = port.get_child_runner_info(&child.id).await;

    Ok(json!({
        "child_session_id": child.id,
        "title": child.title,
        "model": child.model,
        "pinned": child.pinned,
        "message_count": child.messages.len(),
        "is_running": port.is_child_running(&child.id).await,
        "last_run_status": status,
        "last_run_error": metadata_text(&child, "last_run_error"),
        "responsibility": metadata_text(&child, "responsibility"),
        "subagent_type": metadata_text(&child, "subagent_type"),
        "prompt": metadata_text(&child, "assignment_prompt"),
        "latest_user_message": child
            .messages
            .iter()
            .rposition(|message| matches!(message.role, bamboo_agent_core::Role::User))
            .and_then(|idx| child.messages.get(idx))
            .map(|message| message.content.clone()),
        "runtime_kind": metadata_text(&child, "runtime.kind"),
        "external_protocol": metadata_text(&child, "external.protocol"),
        "external_agent_id": metadata_text(&child, "external.agent_id"),
        "a2a_context_id": metadata_text(&child, "a2a.context_id"),
        "a2a_latest_task_id": metadata_text(&child, "a2a.latest_task_id"),
        "a2a_last_state": metadata_text(&child, "a2a.last_state"),
        "runner_started_at": runner_info.as_ref().and_then(|r| r.started_at.map(|t| t.to_rfc3339())),
        "runner_completed_at": runner_info.as_ref().and_then(|r| r.completed_at.map(|t| t.to_rfc3339())),
        "last_tool_name": runner_info.as_ref().and_then(|r| r.last_tool_name.clone()),
        "last_tool_phase": runner_info.as_ref().and_then(|r| r.last_tool_phase.clone()),
        "last_event_at": runner_info.as_ref().and_then(|r| r.last_event_at.map(|t| t.to_rfc3339())),
        "round_count": runner_info.as_ref().map(|r| r.round_count).unwrap_or(0),
        "has_pending_injected_messages": child.metadata.contains_key("pending_injected_messages"),
        "guidance": compute_status_guidance(status.as_deref(), runner_info.as_ref(), child.metadata.contains_key("pending_injected_messages")),
    }))
}

pub async fn update_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
    title: Option<String>,
    responsibility: Option<String>,
    prompt: Option<String>,
    subagent_type: Option<String>,
    reset_after_update: Option<bool>,
) -> Result<serde_json::Value, ChildSessionError> {
    let mut child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;

    let title = normalize_non_empty_optional(title, "title")?;
    let responsibility = normalize_non_empty_optional(responsibility, "responsibility")?;
    let prompt = normalize_non_empty_optional(prompt, "prompt")?;
    let subagent_type = normalize_non_empty_optional(subagent_type, "subagent_type")?;

    let should_refresh_assignment =
        responsibility.is_some() || prompt.is_some() || subagent_type.is_some();

    if title.is_none() && !should_refresh_assignment {
        return Err(ChildSessionError::InvalidArguments(
            "update requires at least one field: title/responsibility/prompt/subagent_type"
                .to_string(),
        ));
    }

    if let Some(title) = title {
        child.title = title;
    }

    let mut messages_removed = 0usize;

    if should_refresh_assignment {
        let effective_responsibility = normalize_required_text(
            responsibility.or_else(|| metadata_text(&child, "responsibility")),
            "responsibility",
        )?;
        let effective_subagent_type = normalize_required_text(
            subagent_type.or_else(|| metadata_text(&child, "subagent_type")),
            "subagent_type",
        )?;
        let effective_prompt = normalize_required_text(
            prompt.or_else(|| metadata_text(&child, "assignment_prompt")),
            "prompt",
        )?;

        child.metadata.insert(
            "responsibility".to_string(),
            effective_responsibility.clone(),
        );
        child
            .metadata
            .insert("subagent_type".to_string(), effective_subagent_type.clone());
        child
            .metadata
            .insert("assignment_prompt".to_string(), effective_prompt.clone());
        child
            .metadata
            .insert("last_run_status".to_string(), "pending".to_string());
        child.metadata.remove("last_run_error");

        let assignment = format_child_assignment(
            &child.title,
            &effective_responsibility,
            &effective_subagent_type,
            &effective_prompt,
        );
        let user_index = replace_or_append_last_user_message(&mut child, assignment);

        if reset_after_update.unwrap_or(true) {
            messages_removed = truncate_after_index(&mut child, user_index);
        }
    }

    child.updated_at = Utc::now();
    port.save_child_session(&child).await?;

    Ok(json!({
        "child_session_id": child.id,
        "title": child.title,
        "messages_removed": messages_removed,
        "last_run_status": metadata_text(&child, "last_run_status"),
        "note": "Child session updated in place. Use action=run to execute the same child session.",
    }))
}

pub async fn run_child_action(
    port: &dyn ChildSessionPort,
    parent: &Session,
    child_session_id: String,
    reset_to_last_user: Option<bool>,
) -> Result<serde_json::Value, ChildSessionError> {
    let mut child = port
        .load_child_for_parent(&parent.id, &child_session_id)
        .await?;

    if port.is_child_running(&child.id).await {
        return Ok(json!({
            "child_session_id": child.id,
            "status": "already_running",
            "note": "Child session is already running.",
        }));
    }

    let mut messages_removed = 0usize;
    if reset_to_last_user.unwrap_or(true) {
        messages_removed = truncate_after_last_user(&mut child)?;
    }

    child
        .metadata
        .insert("last_run_status".to_string(), "pending".to_string());
    child.metadata.remove("last_run_error");
    child.updated_at = Utc::now();
    port.save_child_session(&child).await?;

    port.enqueue_child_run(parent, &child).await?;

    Ok(json!({
        "child_session_id": child.id,
        "status": "queued",
        "messages_removed": messages_removed,
        "note": "Queued existing child session for retry in place.",
    }))
}

/// A queued follow-up message stored in session metadata for later injection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueuedInjectedMessage {
    pub content: String,
    #[serde(default)]
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn send_message_to_child_action(
    port: &dyn ChildSessionPort,
    parent: &Session,
    child_session_id: String,
    message: String,
    auto_run: Option<bool>,
    interrupt_running: Option<bool>,
) -> Result<serde_json::Value, ChildSessionError> {
    let mut child = port
        .load_child_for_parent(&parent.id, &child_session_id)
        .await?;

    let is_running = port.is_child_running(&child.id).await;
    let should_interrupt = interrupt_running.unwrap_or(false);

    if is_running && should_interrupt {
        port.cancel_child_run_and_wait(&child.id).await?;
        child = port
            .load_child_for_parent(&parent.id, &child_session_id)
            .await?;
    }

    let message = normalize_required_text(Some(message), "message")?;

    if is_running && !should_interrupt {
        // Store the message in session metadata so the running agent loop
        // can merge it at the next turn boundary without canceling progress.
        let mut pending: Vec<QueuedInjectedMessage> = child
            .metadata
            .get("pending_injected_messages")
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        pending.push(QueuedInjectedMessage {
            content: message.clone(),
            created_at: Some(chrono::Utc::now()),
        });
        child.metadata.insert(
            "pending_injected_messages".to_string(),
            serde_json::to_string(&pending).unwrap_or_default(),
        );
        port.save_child_session(&child).await?;
        return Ok(json!({
            "child_session_id": child.id,
            "status": "message_queued",
            "auto_run": false,
            "message": message,
            "message_count": child.messages.len(),
            "note": "Message queued for the child session. It will be picked up at the next turn boundary without canceling current progress.",
        }));
    }

    child.add_message(bamboo_agent_core::Message::user(message.clone()));
    child
        .metadata
        .insert("last_run_status".to_string(), "pending".to_string());
    child.metadata.remove("last_run_error");
    port.save_child_session(&child).await?;

    let should_auto_run = auto_run.unwrap_or(true);
    if should_auto_run {
        port.enqueue_child_run(parent, &child).await?;
    }

    Ok(json!({
        "child_session_id": child.id,
        "status": if should_auto_run { "queued" } else { "pending" },
        "auto_run": should_auto_run,
        "message": message,
        "message_count": child.messages.len(),
        "note": if should_auto_run {
            "Follow-up message appended and child session queued."
        } else {
            "Follow-up message appended. Use action=run to execute the child session."
        },
    }))
}

pub async fn cancel_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
) -> Result<serde_json::Value, ChildSessionError> {
    let mut child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;
    port.cancel_child_run_and_wait(&child_session_id).await?;
    child
        .metadata
        .insert("last_run_status".to_string(), "cancelled".to_string());
    child
        .metadata
        .insert("last_run_error".to_string(), "Cancelled by parent".to_string());
    port.save_child_session(&child).await?;
    Ok(json!({
        "child_session_id": child_session_id,
        "status": "cancelled",
    }))
}

pub async fn delete_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
) -> Result<serde_json::Value, ChildSessionError> {
    // Load child first to get its ID (port.delete_child_session handles cancellation + cleanup)
    let child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;
    let result = port.delete_child_session(parent_id, &child.id).await?;

    if !result.deleted {
        return Err(ChildSessionError::Execution(format!(
            "child session was not deleted: {}",
            child.id
        )));
    }

    Ok(json!({
        "child_session_id": child.id,
        "deleted": true,
        "cancelled_running_child": result.cancelled_running_child,
    }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_after_last_user_removes_assistant_tail() {
        let mut session = Session::new_child("child", "root", "test-model", "Child");
        session.add_message(bamboo_agent_core::Message::system("system"));
        session.add_message(bamboo_agent_core::Message::user("task"));
        session.add_message(bamboo_agent_core::Message::assistant("done", None));

        let removed = truncate_after_last_user(&mut session).expect("truncate should work");

        assert_eq!(removed, 1);
        assert_eq!(session.messages.len(), 2);
        assert!(matches!(
            session.messages[1].role,
            bamboo_agent_core::Role::User
        ));
    }

    #[test]
    fn replace_or_append_last_user_message_replaces_existing() {
        let mut session = Session::new_child("child", "root", "test-model", "Child");
        session.add_message(bamboo_agent_core::Message::user("old"));
        session.add_message(bamboo_agent_core::Message::assistant("tail", None));

        let idx = replace_or_append_last_user_message(&mut session, "new".to_string());

        assert_eq!(idx, 0);
        assert_eq!(session.messages[0].content, "new");
        assert_eq!(session.messages.len(), 2);
    }

    #[test]
    fn normalize_non_empty_optional_rejects_blank_strings() {
        let err = normalize_non_empty_optional(Some("  ".to_string()), "prompt")
            .expect_err("blank should be rejected");
        assert!(matches!(err, ChildSessionError::InvalidArguments(msg) if msg.contains("prompt")));
    }

    #[test]
    fn format_child_assignment_builds_expected_string() {
        let result = format_child_assignment("Title", "Responsibility", "Type", "Task brief");
        assert!(result.contains("Title"));
        assert!(result.contains("Responsibility"));
        assert!(result.contains("Type"));
        assert!(result.contains("Task brief"));
    }
}
