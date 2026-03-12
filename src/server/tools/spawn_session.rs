use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use crate::agent::core::storage::{SessionStoreV2, Storage};
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::{Message, Session, SessionKind};
use crate::server::spawn_scheduler::{SpawnJob, SpawnScheduler};

const CHILD_SYSTEM_PROMPT: &str = r#"You are a **Child Session**, delegated by a parent session.

Requirements:
- Focus only on the assigned task and avoid unrelated conversation.
- You may use tools to complete the task.
- Do not create or trigger any additional child sessions (no recursive spawn).
- Keep output concise: provide the conclusion first, then only necessary evidence or steps.
"#;

#[derive(Debug, serde::Deserialize)]
struct SpawnSessionArgsRaw {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: String,
    #[serde(default)]
    responsibility: Option<String>,
    prompt: String,
    subagent_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpawnSessionArgs {
    title: String,
    responsibility: String,
    prompt: String,
    subagent_type: String,
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

fn normalize_title(title: Option<String>, legacy_description: String) -> Result<String, ToolError> {
    let title = title.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let legacy_description = {
        let trimmed = legacy_description.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    };
    normalize_required_text(title.or(legacy_description), "title")
}

fn normalize_spawn_session_args(raw: SpawnSessionArgsRaw) -> Result<SpawnSessionArgs, ToolError> {
    let title = normalize_title(raw.title, raw.description)?;
    let responsibility = normalize_required_text(raw.responsibility, "responsibility")?;
    let prompt = normalize_required_text(Some(raw.prompt), "prompt")?;
    let subagent_type = normalize_required_text(Some(raw.subagent_type), "subagent_type")?;

    Ok(SpawnSessionArgs {
        title,
        responsibility,
        prompt,
        subagent_type,
    })
}

fn format_child_assignment(args: &SpawnSessionArgs) -> String {
    format!(
        "Sub-session title: {}\nResponsibility: {}\nSubagent type: {}\n\nTask brief:\n{}",
        args.title, args.responsibility, args.subagent_type, args.prompt
    )
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
        "Task"
    }

    fn description(&self) -> &str {
        "Delegate a sub-session (sub task/team agent/parallel worker) to run asynchronously. Always provide a clear title and responsibility."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "title": { "type": "string", "description": "Short title for the child session. This is displayed in the Child Sessions panel." },
                "description": { "type": "string", "description": "Legacy alias of title; prefer title." },
                "responsibility": { "type": "string", "description": "Single, explicit responsibility for this child session." },
                "prompt": { "type": "string", "description": "Detailed task instructions and expected output for the child session." },
                "subagent_type": { "type": "string", "description": "Specialized agent profile to use (for example: general-purpose, researcher, coder)." }
            },
            "oneOf": [
                { "required": ["title", "responsibility", "prompt", "subagent_type"] },
                { "required": ["description", "responsibility", "prompt", "subagent_type"] }
            ],
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
            ToolError::Execution("Task requires a session_id in tool context".to_string())
        })?;

        let parsed: SpawnSessionArgsRaw = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Task args: {e}")))?;
        let parsed = normalize_spawn_session_args(parsed)?;

        let parent = self.load_parent_session(parent_session_id).await?;
        if parent.kind != SessionKind::Root {
            return Err(ToolError::Execution(
                "Task is not allowed inside child sessions".to_string(),
            ));
        }

        let model = parent.model.clone();
        if model.trim().is_empty() {
            return Err(ToolError::Execution(
                "parent session model is empty".to_string(),
            ));
        }

        let child_id = Uuid::new_v4().to_string();
        let title = parsed.title.clone();

        let mut child =
            Session::new_child(child_id.clone(), parent.id.clone(), model.clone(), title);
        child
            .metadata
            .insert("spawned_by".to_string(), "Task".to_string());
        child
            .metadata
            .insert("subagent_type".to_string(), parsed.subagent_type.clone());
        child
            .metadata
            .insert("responsibility".to_string(), parsed.responsibility.clone());
        child
            .metadata
            .insert("last_run_status".to_string(), "pending".to_string());
        child.metadata.remove("last_run_error");
        child.metadata.insert(
            "base_system_prompt".to_string(),
            CHILD_SYSTEM_PROMPT.to_string(),
        );

        // Keep the child prompt minimal; do NOT copy the parent's full system prompt.
        child.add_message(Message::system(CHILD_SYSTEM_PROMPT));
        child.add_message(Message::user(format_child_assignment(&parsed)));

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

        ctx.emit_tool_token(format!("Spawned child session: {child_id}"))
            .await;
        let result_title = parsed.title.clone();
        let result_responsibility = parsed.responsibility.clone();
        let result_prompt = parsed.prompt.clone();
        let result_subagent_type = parsed.subagent_type.clone();

        Ok(ToolResult {
            success: true,
            result: json!({
                "title": result_title.clone(),
                "description": result_title,
                "responsibility": result_responsibility,
                "prompt": result_prompt,
                "subagent_type": result_subagent_type,
                "child_session_id": child_id,
                "parent_session_id": parent.id,
                "model": model,
                "note": "Child session runs in background. Observe via sub_session_* events."
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

    #[test]
    fn normalize_spawn_session_args_accepts_legacy_description() {
        let parsed = normalize_spawn_session_args(SpawnSessionArgsRaw {
            title: None,
            description: "Search refs".to_string(),
            responsibility: Some("Inspect parser modules and summarize entrypoints".to_string()),
            prompt: "Read parser-related files and report key functions.".to_string(),
            subagent_type: "general-purpose".to_string(),
        })
        .expect("legacy description should map to title");

        assert_eq!(parsed.title, "Search refs");
        assert_eq!(
            parsed.responsibility,
            "Inspect parser modules and summarize entrypoints"
        );
    }

    #[test]
    fn normalize_spawn_session_args_rejects_missing_responsibility() {
        let err = normalize_spawn_session_args(SpawnSessionArgsRaw {
            title: Some("Search refs".to_string()),
            description: String::new(),
            responsibility: None,
            prompt: "Read parser-related files and report key functions.".to_string(),
            subagent_type: "general-purpose".to_string(),
        })
        .expect_err("responsibility should be required");

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("responsibility")));
    }

    #[test]
    fn normalize_spawn_session_args_rejects_missing_title_and_description() {
        let err = normalize_spawn_session_args(SpawnSessionArgsRaw {
            title: None,
            description: String::new(),
            responsibility: Some("Inspect parser modules and summarize entrypoints".to_string()),
            prompt: "Read parser-related files and report key functions.".to_string(),
            subagent_type: "general-purpose".to_string(),
        })
        .expect_err("title should be required when legacy description is also missing");

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("title")));
    }

    #[test]
    fn normalize_spawn_session_args_uses_legacy_description_when_title_is_blank() {
        let parsed = normalize_spawn_session_args(SpawnSessionArgsRaw {
            title: Some("   ".to_string()),
            description: "Legacy title".to_string(),
            responsibility: Some("Inspect parser modules and summarize entrypoints".to_string()),
            prompt: "Read parser-related files and report key functions.".to_string(),
            subagent_type: "general-purpose".to_string(),
        })
        .expect("blank title should fall back to legacy description");

        assert_eq!(parsed.title, "Legacy title");
    }

    #[test]
    fn format_child_assignment_includes_title_and_responsibility() {
        let content = format_child_assignment(&SpawnSessionArgs {
            title: "Find parser entrypoints".to_string(),
            responsibility: "Locate parser entrypoints and summarize call graph".to_string(),
            prompt: "Scan src/parser and produce a short report.".to_string(),
            subagent_type: "general-purpose".to_string(),
        });

        assert!(content.contains("Sub-session title: Find parser entrypoints"));
        assert!(
            content.contains("Responsibility: Locate parser entrypoints and summarize call graph")
        );
        assert!(content.contains("Task brief:"));
    }

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
        async fn execute(&self, _call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
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
    async fn task_requires_session_id_in_tool_context() {
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
            session_event_senders: Arc::new(RwLock::new(HashMap::<
                String,
                broadcast::Sender<crate::agent::core::AgentEvent>,
            >::new())),
        };
        let scheduler = Arc::new(SpawnScheduler::new(ctx));

        let tool = SpawnSessionTool::new(session_store, storage, scheduler);

        let err = tool
            .execute_with_context(
                json!({
                    "description": "demo task",
                    "prompt": "do something",
                    "subagent_type": "general-purpose"
                }),
                ToolExecutionContext::none("tool_call"),
            )
            .await
            .unwrap_err();

        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("Task requires a session_id in tool context"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
