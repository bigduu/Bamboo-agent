use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

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
        self.save_child_session(&child).await?;

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
        "Manage existing child sessions under the current root session. Supports list/get/update/run/delete so retries can happen in-place instead of creating new child sessions."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "get", "update", "run", "delete"],
                    "description": "Operation to perform on child sessions of the current root session."
                },
                "child_session_id": { "type": "string", "description": "Existing child session id to manage." },
                "title": { "type": "string", "description": "Updated child title (update)." },
                "responsibility": { "type": "string", "description": "Updated child responsibility (update)." },
                "prompt": { "type": "string", "description": "Updated child task brief (update)." },
                "subagent_type": { "type": "string", "description": "Updated subagent profile (update)." },
                "reset_after_update": { "type": "boolean", "description": "Whether to truncate messages after refreshed assignment on update (default true)." },
                "reset_to_last_user": { "type": "boolean", "description": "Whether to truncate messages after the last user message before run (default true)." }
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
            SubSessionManagerArgs::Delete { child_session_id } => {
                self.delete_action(&parent.id, child_session_id).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
