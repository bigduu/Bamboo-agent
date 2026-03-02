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

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;
    use tokio::sync::{broadcast, RwLock};

    use crate::agent::core::tools::{ToolCall, ToolExecutor, ToolSchema};
    use crate::agent::llm::{LLMError, LLMProvider, LLMStream};
    use crate::agent::metrics::storage::SqliteMetricsStorage;
    use crate::agent::metrics::MetricsCollector;
    use crate::agent::skill::SkillManager;

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
            _call: &ToolCall,
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

    #[tokio::test]
    async fn spawn_session_requires_session_id_in_tool_context() {
        // This should fail fast before any disk IO or scheduler enqueues happen.
        let bamboo_home = make_temp_dir("bamboo-spawn-session-tool-test");
        tokio::fs::create_dir_all(&bamboo_home).await.unwrap();

        let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
        let storage_dir = bamboo_home.join("storage");
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        let jsonl = crate::agent::core::storage::JsonlStorage::new(&storage_dir);
        jsonl.init().await.unwrap();
        let storage: Arc<dyn Storage> = Arc::new(jsonl);

        let metrics_storage = Arc::new(SqliteMetricsStorage::new(bamboo_home.join("metrics.db")));
        let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

        let ctx = crate::server::spawn_scheduler::SpawnContext {
            session_store: session_store.clone(),
            storage: storage.clone(),
            provider: Arc::new(NoopProvider),
            tools: Arc::new(NoopToolExecutor),
            skill_manager: Arc::new(SkillManager::new()),
            metrics_collector,
            sessions_cache: Arc::new(RwLock::new(HashMap::new())),
            agent_runners: Arc::new(RwLock::new(HashMap::new())),
            session_event_senders: Arc::new(RwLock::new(HashMap::<String, broadcast::Sender<crate::agent::core::AgentEvent>>::new())),
        };
        let scheduler = Arc::new(SpawnScheduler::new(ctx));

        let tool = SpawnSessionTool::new(session_store, storage, scheduler);

        let err = tool
            .execute_with_context(
                json!({ "goal": "do something" }),
                ToolExecutionContext::none("tool_call"),
            )
            .await
            .unwrap_err();

        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("spawn_session requires a session_id in tool context"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
