use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration, Instant};

use crate::agent::core::storage::{SessionIndexEntry, SessionStoreV2, Storage};
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::{Message, Role, Session, SessionKind};
use crate::server::app_state::{AgentRunner, AgentStatus};
use crate::server::spawn_scheduler::{SpawnJob, SpawnScheduler};

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SubSessionManagerArgs {
    List,
    Get {
        child_session_id: String,
    },
    Update {
        child_session_id: String,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        responsibility: Option<String>,
        #[serde(default)]
        prompt: Option<String>,
        #[serde(default)]
        subagent_type: Option<String>,
        #[serde(default)]
        reset_after_update: Option<bool>,
    },
    Run {
        child_session_id: String,
        #[serde(default)]
        reset_to_last_user: Option<bool>,
    },
    SendMessage {
        child_session_id: String,
        message: String,
        #[serde(default)]
        auto_run: Option<bool>,
        #[serde(default)]
        interrupt_running: Option<bool>,
    },
    Delete {
        child_session_id: String,
    },
}

fn normalize_non_empty_optional(
    value: Option<String>,
    field_name: &str,
) -> Result<Option<String>, ToolError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    }
    Ok(Some(trimmed.to_string()))
}

fn normalize_required_text(value: Option<String>, field_name: &str) -> Result<String, ToolError> {
    let Some(value) = value else {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    }
    Ok(trimmed.to_string())
}

fn metadata_text(session: &Session, key: &str) -> Option<String> {
    session
        .metadata
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn format_child_assignment(
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

fn replace_or_append_last_user_message(session: &mut Session, content: String) -> usize {
    if let Some(index) = session
        .messages
        .iter()
        .rposition(|message| matches!(message.role, Role::User))
    {
        session.messages[index].content = content;
        return index;
    }

    session.add_message(Message::user(content));
    session.messages.len().saturating_sub(1)
}

fn truncate_after_index(session: &mut Session, keep_last_index: usize) -> usize {
    let keep_len = keep_last_index.saturating_add(1);
    let removed = session.messages.len().saturating_sub(keep_len);
    if removed > 0 {
        session.messages.truncate(keep_len);
        session.token_usage = None;
        session.conversation_summary = None;
    }
    removed
}

fn truncate_after_last_user(session: &mut Session) -> Result<usize, ToolError> {
    let Some(last_user_idx) = session
        .messages
        .iter()
        .rposition(|message| matches!(message.role, Role::User))
    else {
        return Err(ToolError::Execution(
            "No user message found to retry from".to_string(),
        ));
    };

    Ok(truncate_after_index(session, last_user_idx))
}

fn map_child_entry(entry: &SessionIndexEntry) -> serde_json::Value {
    json!({
        "child_session_id": entry.id,
        "title": entry.title,
        "pinned": entry.pinned,
        "message_count": entry.message_count,
        "updated_at": entry.updated_at,
        "last_run_status": entry.last_run_status,
        "last_run_error": entry.last_run_error,
    })
}

pub struct SubSessionManagerTool {
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    scheduler: Arc<SpawnScheduler>,
    sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders:
        Arc<RwLock<HashMap<String, broadcast::Sender<crate::agent::core::AgentEvent>>>>,
}

impl SubSessionManagerTool {
    pub fn new(
        session_store: Arc<SessionStoreV2>,
        storage: Arc<dyn Storage>,
        scheduler: Arc<SpawnScheduler>,
        sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
        agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        session_event_senders: Arc<
            RwLock<HashMap<String, broadcast::Sender<crate::agent::core::AgentEvent>>>,
        >,
    ) -> Self {
        Self {
            session_store,
            storage,
            scheduler,
            sessions_cache,
            agent_runners,
            session_event_senders,
        }
    }

    async fn load_root_session(&self, root_session_id: &str) -> Result<Session, ToolError> {
        let Some(session) = self
            .storage
            .load_session(root_session_id)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("failed to load session {root_session_id}: {error}"))
            })?
        else {
            return Err(ToolError::Execution(format!(
                "session not found: {root_session_id}"
            )));
        };

        if session.kind != SessionKind::Root {
            return Err(ToolError::Execution(
                "sub_session_manager is only allowed in root sessions".to_string(),
            ));
        }

        Ok(session)
    }

    async fn load_child_for_parent(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
    ) -> Result<Session, ToolError> {
        let Some(child) = self
            .storage
            .load_session(child_session_id)
            .await
            .map_err(|error| {
                ToolError::Execution(format!(
                    "failed to load child session {child_session_id}: {error}"
                ))
            })?
        else {
            return Err(ToolError::Execution(format!(
                "child session not found: {child_session_id}"
            )));
        };

        if child.kind != SessionKind::Child {
            return Err(ToolError::Execution(format!(
                "session is not a child session: {child_session_id}"
            )));
        }

        if child.parent_session_id.as_deref() != Some(parent_session_id) {
            return Err(ToolError::Execution(format!(
                "child session {child_session_id} does not belong to parent {parent_session_id}"
            )));
        }

        Ok(child)
    }

    async fn save_child_session(&self, child: &Session) -> Result<(), ToolError> {
        self.storage.save_session(child).await.map_err(|error| {
            ToolError::Execution(format!("failed to save child session: {error}"))
        })?;

        let mut sessions = self.sessions_cache.write().await;
        sessions.insert(child.id.clone(), child.clone());

        Ok(())
    }

    async fn get_or_create_sender(
        &self,
        session_id: &str,
    ) -> broadcast::Sender<crate::agent::core::AgentEvent> {
        let mut senders = self.session_event_senders.write().await;
        if let Some(existing) = senders.get(session_id) {
            return existing.clone();
        }
        let (tx, _) = broadcast::channel(1000);
        senders.insert(session_id.to_string(), tx.clone());
        tx
    }

    async fn is_child_running(&self, child_session_id: &str) -> bool {
        let runners = self.agent_runners.read().await;
        runners
            .get(child_session_id)
            .is_some_and(|runner| matches!(runner.status, AgentStatus::Running))
    }

    async fn list_children(&self, parent_session_id: &str) -> Vec<serde_json::Value> {
        self.session_store
            .list_index_entries()
            .await
            .into_iter()
            .filter(|entry| {
                entry.kind == SessionKind::Child
                    && entry.parent_session_id.as_deref() == Some(parent_session_id)
            })
            .map(|entry| map_child_entry(&entry))
            .collect()
    }

    async fn get_action(
        &self,
        parent_session_id: &str,
        child_session_id: String,
    ) -> Result<ToolResult, ToolError> {
        let child = self
            .load_child_for_parent(parent_session_id, &child_session_id)
            .await?;

        let result = json!({
            "child_session_id": child.id,
            "title": child.title,
            "model": child.model,
            "pinned": child.pinned,
            "message_count": child.messages.len(),
            "is_running": self.is_child_running(&child.id).await,
            "last_run_status": metadata_text(&child, "last_run_status"),
            "last_run_error": metadata_text(&child, "last_run_error"),
            "responsibility": metadata_text(&child, "responsibility"),
            "subagent_type": metadata_text(&child, "subagent_type"),
            "prompt": metadata_text(&child, "assignment_prompt"),
            "latest_user_message": child
                .messages
                .iter()
                .rposition(|message| matches!(message.role, Role::User))
                .and_then(|idx| child.messages.get(idx))
                .map(|message| message.content.clone()),
        });

        Ok(ToolResult {
            success: true,
            result: result.to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }

    async fn update_action(
        &self,
        parent_session_id: &str,
        child_session_id: String,
        title: Option<String>,
        responsibility: Option<String>,
        prompt: Option<String>,
        subagent_type: Option<String>,
        reset_after_update: Option<bool>,
    ) -> Result<ToolResult, ToolError> {
        let mut child = self
            .load_child_for_parent(parent_session_id, &child_session_id)
            .await?;

        let title = normalize_non_empty_optional(title, "title")?;
        let responsibility = normalize_non_empty_optional(responsibility, "responsibility")?;
        let prompt = normalize_non_empty_optional(prompt, "prompt")?;
        let subagent_type = normalize_non_empty_optional(subagent_type, "subagent_type")?;

        let should_refresh_assignment =
            responsibility.is_some() || prompt.is_some() || subagent_type.is_some();

        if title.is_none() && !should_refresh_assignment {
            return Err(ToolError::InvalidArguments(
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
        self.save_child_session(&child).await?;

        Ok(ToolResult {
            success: true,
            result: json!({
                "child_session_id": child.id,
                "title": child.title,
                "messages_removed": messages_removed,
                "last_run_status": metadata_text(&child, "last_run_status"),
                "note": "Child session updated in place. Use action=run to execute the same child session.",
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }

    async fn run_action(
        &self,
        parent: &Session,
        child_session_id: String,
        reset_to_last_user: Option<bool>,
    ) -> Result<ToolResult, ToolError> {
        let mut child = self
            .load_child_for_parent(&parent.id, &child_session_id)
            .await?;

        if self.is_child_running(&child.id).await {
            return Ok(ToolResult {
                success: true,
                result: json!({
                    "child_session_id": child.id,
                    "status": "already_running",
                    "note": "Child session is already running.",
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
            });
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
        self.save_child_session(&child).await?;

        self.enqueue_child_run(parent, &child).await?;

        Ok(ToolResult {
            success: true,
            result: json!({
                "child_session_id": child.id,
                "status": "queued",
                "messages_removed": messages_removed,
                "note": "Queued existing child session for retry in place.",
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }

    async fn send_message_action(
        &self,
        parent: &Session,
        child_session_id: String,
        message: String,
        auto_run: Option<bool>,
        interrupt_running: Option<bool>,
    ) -> Result<ToolResult, ToolError> {
        let mut child = self
            .load_child_for_parent(&parent.id, &child_session_id)
            .await?;

        if self.is_child_running(&child.id).await {
            if !interrupt_running.unwrap_or(false) {
                return Err(ToolError::Execution(
                    "cannot send a follow-up message while the child session is running; set interrupt_running=true to cancel and continue".to_string(),
                ));
            }

            self.cancel_child_run_and_wait(&child.id).await?;
            child = self
                .load_child_for_parent(&parent.id, &child_session_id)
                .await?;
        }

        let message = normalize_required_text(Some(message), "message")?;
        child.add_message(Message::user(message.clone()));
        child
            .metadata
            .insert("last_run_status".to_string(), "pending".to_string());
        child.metadata.remove("last_run_error");
        self.save_child_session(&child).await?;

        let should_auto_run = auto_run.unwrap_or(true);
        if should_auto_run {
            self.enqueue_child_run(parent, &child).await?;
        }

        Ok(ToolResult {
            success: true,
            result: json!({
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
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }

    async fn cancel_child_run_and_wait(&self, child_session_id: &str) -> Result<(), ToolError> {
        let cancelled = {
            let mut runners = self.agent_runners.write().await;
            if let Some(runner) = runners.get_mut(child_session_id) {
                if matches!(runner.status, AgentStatus::Running) {
                    runner.cancel_token.cancel();
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };

        if !cancelled {
            return Ok(());
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let still_running = {
                let runners = self.agent_runners.read().await;
                runners
                    .get(child_session_id)
                    .is_some_and(|runner| matches!(runner.status, AgentStatus::Running))
            };
            if !still_running {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ToolError::Execution(format!(
                    "timed out waiting for child session {child_session_id} to stop after cancellation"
                )));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn enqueue_child_run(&self, parent: &Session, child: &Session) -> Result<(), ToolError> {
        let model = if child.model.trim().is_empty() {
            parent.model.clone()
        } else {
            child.model.clone()
        };
        if model.trim().is_empty() {
            return Err(ToolError::Execution(
                "child model is empty and parent model is unavailable".to_string(),
            ));
        }

        self.scheduler
            .enqueue(SpawnJob {
                parent_session_id: parent.id.clone(),
                child_session_id: child.id.clone(),
                model,
            })
            .await
            .map_err(ToolError::Execution)?;

        let parent_tx = self.get_or_create_sender(&parent.id).await;
        let _ = parent_tx.send(crate::agent::core::AgentEvent::SubSessionStarted {
            parent_session_id: parent.id.clone(),
            child_session_id: child.id.clone(),
            title: Some(child.title.clone()),
        });

        Ok(())
    }

    async fn delete_action(
        &self,
        parent_session_id: &str,
        child_session_id: String,
    ) -> Result<ToolResult, ToolError> {
        let child = self
            .load_child_for_parent(parent_session_id, &child_session_id)
            .await?;

        let cancelled_running_child = {
            let mut runners = self.agent_runners.write().await;
            if let Some(runner) = runners.remove(&child.id) {
                runner.cancel_token.cancel();
                true
            } else {
                false
            }
        };

        let deleted = self
            .storage
            .delete_session(&child.id)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("failed to delete child session: {error}"))
            })?;

        if !deleted {
            return Err(ToolError::Execution(format!(
                "child session was not deleted: {}",
                child.id
            )));
        }

        {
            let mut sessions = self.sessions_cache.write().await;
            sessions.remove(&child.id);
        }
        {
            let mut senders = self.session_event_senders.write().await;
            senders.remove(&child.id);
            if cancelled_running_child {
                if let Some(parent_tx) = senders.get(parent_session_id) {
                    let _ = parent_tx.send(crate::agent::core::AgentEvent::SubSessionCompleted {
                        parent_session_id: parent_session_id.to_string(),
                        child_session_id: child.id.clone(),
                        status: "cancelled".to_string(),
                        error: Some("Child session deleted while running".to_string()),
                    });
                }
            }
        }

        Ok(ToolResult {
            success: true,
            result: json!({
                "child_session_id": child.id,
                "deleted": true,
                "cancelled_running_child": cancelled_running_child,
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[async_trait]
impl Tool for SubSessionManagerTool {
    fn name(&self) -> &str {
        "sub_session_manager"
    }

    fn description(&self) -> &str {
        "Manage existing child sessions under the current root session. Supports list/get/update/run/send_message/delete so retries and follow-up instructions can happen in place instead of creating new child sessions."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "get", "update", "run", "send_message", "delete"],
                    "description": "Operation to perform on child sessions of the current root session."
                },
                "child_session_id": { "type": "string", "description": "Existing child session id to manage." },
                "title": { "type": "string", "description": "Updated child title (update)." },
                "responsibility": { "type": "string", "description": "Updated child responsibility (update)." },
                "prompt": { "type": "string", "description": "Updated child task brief (update)." },
                "subagent_type": { "type": "string", "description": "Updated subagent profile (update)." },
                "reset_after_update": { "type": "boolean", "description": "Whether to truncate messages after refreshed assignment on update (default true)." },
                "reset_to_last_user": { "type": "boolean", "description": "Whether to truncate messages after the last user message before run (default true)." },
                "message": { "type": "string", "description": "Follow-up instruction to append as a new user message on the child session (send_message)." },
                "auto_run": { "type": "boolean", "description": "Whether send_message should immediately queue the child session again (default true)." },
                "interrupt_running": { "type": "boolean", "description": "If true, send_message cancels a currently running child session and waits until it stops before appending the follow-up message." }
            },
            "required": ["action"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parent_session_id = ctx.session_id.ok_or_else(|| {
            ToolError::Execution(
                "sub_session_manager requires a session_id in tool context".to_string(),
            )
        })?;
        let parent = self.load_root_session(parent_session_id).await?;

        let parsed: SubSessionManagerArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid sub_session_manager args: {error}"))
        })?;

        match parsed {
            SubSessionManagerArgs::List => {
                let children = self.list_children(&parent.id).await;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "parent_session_id": parent.id,
                        "children": children,
                        "count": children.len(),
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }
            SubSessionManagerArgs::Get { child_session_id } => {
                self.get_action(&parent.id, child_session_id).await
            }
            SubSessionManagerArgs::Update {
                child_session_id,
                title,
                responsibility,
                prompt,
                subagent_type,
                reset_after_update,
            } => {
                self.update_action(
                    &parent.id,
                    child_session_id,
                    title,
                    responsibility,
                    prompt,
                    subagent_type,
                    reset_after_update,
                )
                .await
            }
            SubSessionManagerArgs::Run {
                child_session_id,
                reset_to_last_user,
            } => {
                self.run_action(&parent, child_session_id, reset_to_last_user)
                    .await
            }
            SubSessionManagerArgs::SendMessage {
                child_session_id,
                message,
                auto_run,
                interrupt_running,
            } => {
                self.send_message_action(
                    &parent,
                    child_session_id,
                    message,
                    auto_run,
                    interrupt_running,
                )
                .await
            }
            SubSessionManagerArgs::Delete { child_session_id } => {
                self.delete_action(&parent.id, child_session_id).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::storage::{JsonlStorage, SessionStoreV2, Storage};
    use crate::agent::core::tools::{ToolExecutionContext, ToolExecutor, ToolSchema};
    use crate::agent::core::{AgentEvent, Session};
    use crate::agent::llm::{LLMError, LLMProvider, LLMStream};
    use crate::agent::metrics::{MetricsCollector, SqliteMetricsStorage};
    use crate::agent::skill::SkillManager;
    use crate::server::spawn_scheduler::SpawnContext;
    use std::path::PathBuf;
    use tokio::time::Duration;
    use uuid::Uuid;

    struct NoopProvider;

    #[async_trait::async_trait]
    impl LLMProvider for NoopProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Err(LLMError::Api("noop".to_string()))
        }
    }

    struct NoopToolExecutor;

    #[async_trait::async_trait]
    impl ToolExecutor for NoopToolExecutor {
        async fn execute(
            &self,
            _call: &crate::agent::core::tools::ToolCall,
        ) -> std::result::Result<ToolResult, ToolError> {
            Err(ToolError::NotFound("noop".to_string()))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    fn make_temp_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
    }

    struct TestHarness {
        tool: SubSessionManagerTool,
        storage: Arc<dyn Storage>,
        agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        parent_session_id: String,
        child_session_id: String,
        parent_rx: broadcast::Receiver<AgentEvent>,
    }

    async fn build_test_harness() -> TestHarness {
        let bamboo_home = make_temp_dir("bamboo-sub-session-manager-test");
        tokio::fs::create_dir_all(&bamboo_home).await.unwrap();

        let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
        let storage_dir = bamboo_home.join("storage");
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        let jsonl = JsonlStorage::new(&storage_dir);
        jsonl.init().await.unwrap();
        let storage: Arc<dyn Storage> = Arc::new(jsonl);

        let metrics_storage = Arc::new(SqliteMetricsStorage::new(bamboo_home.join("metrics.db")));
        let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

        let sessions_cache = Arc::new(RwLock::new(HashMap::new()));
        let agent_runners = Arc::new(RwLock::new(HashMap::new()));
        let session_event_senders = Arc::new(RwLock::new(HashMap::<
            String,
            broadcast::Sender<AgentEvent>,
        >::new()));

        let parent_session_id = "root-session".to_string();
        let child_session_id = "child-session".to_string();
        let (parent_tx, parent_rx) = broadcast::channel(1000);
        {
            let mut senders = session_event_senders.write().await;
            senders.insert(parent_session_id.clone(), parent_tx);
        }

        let mut parent = Session::new(parent_session_id.clone(), "gpt-5");
        parent.title = "Root".to_string();
        storage.save_session(&parent).await.unwrap();

        let mut child = Session::new_child(
            child_session_id.clone(),
            parent_session_id.clone(),
            "gpt-5",
            "Child session",
        );
        child
            .metadata
            .insert("last_run_status".to_string(), "completed".to_string());
        child.add_message(Message::system("child system"));
        child.add_message(Message::user("initial assignment"));
        child.add_message(Message::assistant("initial answer", None));
        storage.save_session(&child).await.unwrap();

        let scheduler = Arc::new(SpawnScheduler::new(SpawnContext {
            session_store: session_store.clone(),
            storage: storage.clone(),
            provider: Arc::new(NoopProvider),
            tools: Arc::new(NoopToolExecutor),
            config: Arc::new(RwLock::new(crate::core::Config::default())),
            skill_manager: Arc::new(SkillManager::new()),
            metrics_collector,
            sessions_cache: sessions_cache.clone(),
            agent_runners: agent_runners.clone(),
            session_event_senders: session_event_senders.clone(),
        }));

        let tool = SubSessionManagerTool::new(
            session_store,
            storage.clone(),
            scheduler,
            sessions_cache,
            agent_runners.clone(),
            session_event_senders,
        );

        TestHarness {
            tool,
            storage,
            agent_runners,
            parent_session_id,
            child_session_id,
            parent_rx,
        }
    }

    #[test]
    fn truncate_after_last_user_removes_assistant_tail() {
        let mut session = Session::new_child("child", "root", "test-model", "Child");
        session.add_message(Message::system("system"));
        session.add_message(Message::user("task"));
        session.add_message(Message::assistant("done", None));

        let removed = truncate_after_last_user(&mut session).expect("truncate should work");

        assert_eq!(removed, 1);
        assert_eq!(session.messages.len(), 2);
        assert!(matches!(session.messages[1].role, Role::User));
    }

    #[test]
    fn replace_or_append_last_user_message_replaces_existing() {
        let mut session = Session::new_child("child", "root", "test-model", "Child");
        session.add_message(Message::user("old"));
        session.add_message(Message::assistant("tail", None));

        let idx = replace_or_append_last_user_message(&mut session, "new".to_string());

        assert_eq!(idx, 0);
        assert_eq!(session.messages[0].content, "new");
        assert_eq!(session.messages.len(), 2);
    }

    #[test]
    fn normalize_non_empty_optional_rejects_blank_strings() {
        let err = normalize_non_empty_optional(Some("  ".to_string()), "prompt")
            .expect_err("blank should be rejected");
        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("prompt")));
    }

    #[tokio::test]
    async fn send_message_appends_follow_up_without_replacing_history() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "continue with the failing parser path",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_send_message",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("send_message should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "pending");

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .expect("child session should exist");
        assert_eq!(child.messages.len(), 4);
        assert!(matches!(child.messages[2].role, Role::Assistant));
        assert!(matches!(child.messages[3].role, Role::User));
        assert_eq!(
            child.messages[3].content,
            "continue with the failing parser path"
        );
        assert_eq!(
            child.metadata.get("last_run_status").map(String::as_str),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn send_message_rejects_running_child() {
        let harness = build_test_harness().await;
        {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            runners.insert(harness.child_session_id.clone(), runner);
        }

        let err = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "continue"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_running",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect_err("send_message should reject a running child");

        assert!(
            matches!(err, ToolError::Execution(msg) if msg.contains("while the child session is running"))
        );
    }

    #[tokio::test]
    async fn send_message_can_interrupt_running_child() {
        let harness = build_test_harness().await;
        let cancel_token = {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            let cancel_token = runner.cancel_token.clone();
            runners.insert(harness.child_session_id.clone(), runner);
            cancel_token
        };

        let runners_for_status = harness.agent_runners.clone();
        let child_id_for_status = harness.child_session_id.clone();
        let waiter = tokio::spawn(async move {
            cancel_token.cancelled().await;
            let mut runners = runners_for_status.write().await;
            if let Some(runner) = runners.get_mut(&child_id_for_status) {
                runner.status = AgentStatus::Cancelled;
            }
        });

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "continue from latest state",
                    "auto_run": false,
                    "interrupt_running": true
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_interrupt_running",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("send_message should interrupt running child");

        waiter.await.expect("waiter task should finish");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "pending");
        assert_eq!(payload["auto_run"], false);

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .expect("child session should exist");
        assert!(matches!(
            child.messages.last().map(|m| &m.role),
            Some(Role::User)
        ));
        assert_eq!(
            child.messages.last().map(|m| m.content.as_str()),
            Some("continue from latest state")
        );
        assert_eq!(
            child.metadata.get("last_run_status").map(String::as_str),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn send_message_can_queue_child_immediately() {
        let mut harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "retry with a narrower scope"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_queue",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("send_message should queue the child");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "queued");
        assert_eq!(payload["auto_run"], true);

        let started_event = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match harness.parent_rx.recv().await {
                    Ok(AgentEvent::SubSessionStarted {
                        parent_session_id,
                        child_session_id,
                        ..
                    }) => break (parent_session_id, child_session_id),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("parent stream closed before start event")
                    }
                }
            }
        })
        .await
        .expect("should receive SubSessionStarted event");

        assert_eq!(started_event.0, harness.parent_session_id);
        assert_eq!(started_event.1, harness.child_session_id);
    }
}
