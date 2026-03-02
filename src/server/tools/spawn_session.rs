use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use crate::agent::core::storage::{SessionStoreV2, Storage};
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::{Message, Session, SessionKind};
use crate::server::spawn_scheduler::{SpawnJob, SpawnScheduler};

const CHILD_SYSTEM_PROMPT: &str = r#"你是一个 **Child Session（子会话）**，由主会话委派任务。

要求：
- 只专注完成当前任务，避免无关对话。
- 允许使用工具来完成任务。
- 不允许创建/触发新的子会话（禁止递归 spawn）。
- 输出尽量简洁：先给结论，再给必要依据/步骤。
"#;

#[derive(Debug, serde::Deserialize)]
struct SpawnSessionArgs {
    /// Task goal / instructions for the child session.
    goal: String,
    /// Optional display title.
    #[serde(default)]
    title: Option<String>,
    /// Optional model override. Defaults to the parent session's model.
    #[serde(default)]
    model: Option<String>,
}

/// Server-only tool: spawn a child session and run it asynchronously.
///
/// The tool returns immediately; the UI can observe progress via the parent session event stream
/// (`sub_session_*` events), and can open the child session to inspect full history.
pub struct SpawnSessionTool {
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
    scheduler: Arc<SpawnScheduler>,
}

impl SpawnSessionTool {
    pub fn new(
        session_store: Arc<SessionStoreV2>,
        storage: Arc<dyn Storage>,
        scheduler: Arc<SpawnScheduler>,
    ) -> Self {
        Self {
            session_store,
            storage,
            scheduler,
        }
    }

    async fn load_parent_session(&self, session_id: &str) -> Result<Session, ToolError> {
        match self.storage.load_session(session_id).await {
            Ok(Some(session)) => Ok(session),
            Ok(None) => Err(ToolError::Execution(format!(
                "session not found: {session_id}"
            ))),
            Err(e) => Err(ToolError::Execution(format!(
                "failed to load session {session_id}: {e}"
            ))),
        }
    }
}

#[async_trait]
impl Tool for SpawnSessionTool {
    fn name(&self) -> &str {
        "spawn_session"
    }

    fn description(&self) -> &str {
        "Create a child session to handle a sub-task asynchronously (returns immediately). Progress is forwarded to the parent session event stream; do not use http_request to poll localhost. Child sessions cannot spawn further sessions."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "goal": { "type": "string", "description": "Sub-task instructions for the child session." },
                "title": { "type": "string", "description": "Optional child session title." },
                "model": { "type": "string", "description": "Optional model override (defaults to parent session model)." }
            },
            "required": ["goal"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call")).await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parent_session_id = ctx.session_id.ok_or_else(|| {
            ToolError::Execution("spawn_session requires a session_id in tool context".to_string())
        })?;

        let parsed: SpawnSessionArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid spawn_session args: {e}"))
        })?;
        let goal = parsed.goal.trim();
        if goal.is_empty() {
            return Err(ToolError::InvalidArguments(
                "goal must be a non-empty string".to_string(),
            ));
        }

        let parent = self.load_parent_session(parent_session_id).await?;
        if parent.kind != SessionKind::Root {
            return Err(ToolError::Execution(
                "spawn_session is not allowed inside child sessions".to_string(),
            ));
        }

        // Use parent model by default. This is set by /execute (per-request model).
        let model = parsed
            .model
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
            .unwrap_or_else(|| parent.model.clone());
        if model.trim().is_empty() {
            return Err(ToolError::Execution(
                "parent session model is empty; pass `model` explicitly".to_string(),
            ));
        }

        let child_id = Uuid::new_v4().to_string();
        let title = parsed
            .title
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "Child Session".to_string());

        let mut child = Session::new_child(child_id.clone(), parent.id.clone(), model.clone(), title);
        child.metadata.insert(
            "spawned_by".to_string(),
            "spawn_session".to_string(),
        );
        child.metadata.insert(
            "base_system_prompt".to_string(),
            CHILD_SYSTEM_PROMPT.to_string(),
        );

        // Keep the child prompt minimal; do NOT copy the parent's full system prompt.
        child.add_message(Message::system(CHILD_SYSTEM_PROMPT));
        child.add_message(Message::user(goal.to_string()));

        // Persist child session + index entry.
        self.storage
            .save_session(&child)
            .await
            .map_err(|e| ToolError::Execution(format!("failed to save child session: {e}")))?;

        // Ensure index entry is visible immediately (best-effort).
        let _ = self.session_store.get_index_entry(&child_id).await;

        // Schedule background run.
        self.scheduler
            .enqueue(SpawnJob {
                parent_session_id: parent.id.clone(),
                child_session_id: child_id.clone(),
                model: model.clone(),
            })
            .await
            .map_err(ToolError::Execution)?;

        ctx.emit_tool_token(format!("Spawned child session: {child_id}")).await;

        Ok(ToolResult {
            success: true,
            result: json!({
                "child_session_id": child_id,
                "parent_session_id": parent.id,
                "model": model,
                "note": "Child session runs in background. UI can observe progress via the parent session event stream (sub_session_* events) and open the child session for full history."
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}
