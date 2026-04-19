use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use crate::session_app::child_session::{create_child_action, ChildSessionPort, CreateChildInput};
use crate::tools::child_session_adapter::{tool_error_from_child_session, ChildSessionAdapter};
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};

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

/// Server-only tool: spawn a child session and run it asynchronously.
///
/// The tool returns immediately; the UI can observe progress via the parent session event stream
/// (`sub_session_*` events), and can open the child session to inspect full history.
pub struct SpawnSessionTool {
    adapter: Arc<ChildSessionAdapter>,
}

impl SpawnSessionTool {
    pub fn new(adapter: Arc<ChildSessionAdapter>) -> Self {
        Self { adapter }
    }
}

#[async_trait]
impl Tool for SpawnSessionTool {
    fn name(&self) -> &str {
        "SubSession"
    }

    fn description(&self) -> &str {
        "Create and run a child session asynchronously when the user explicitly requests delegated/parallel sub-agent work. Always provide a clear title and responsibility. After creation, use sub_session_manager to inspect, retry, or send follow-up messages to the same child session."
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
            "required": ["responsibility", "prompt", "subagent_type"],
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
            ToolError::Execution("SubSession requires a session_id in tool context".to_string())
        })?;

        let parsed: SpawnSessionArgsRaw = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid SubSession args: {e}")))?;
        let parsed = normalize_spawn_session_args(parsed)?;

        let parent = self
            .adapter
            .as_ref()
            .load_root_session(parent_session_id)
            .await
            .map_err(tool_error_from_child_session)?;

        if parent.model.trim().is_empty() {
            return Err(ToolError::Execution(
                "parent session model is empty".to_string(),
            ));
        }

        let child_id = Uuid::new_v4().to_string();

        let result = create_child_action(
            self.adapter.as_ref(),
            CreateChildInput {
                parent_session: parent.clone(),
                child_id: child_id.clone(),
                title: parsed.title.clone(),
                responsibility: parsed.responsibility.clone(),
                assignment_prompt: parsed.prompt.clone(),
                subagent_type: parsed.subagent_type.clone(),
            },
        )
        .await
        .map_err(tool_error_from_child_session)?;

        // Ensure index entry is visible immediately (best-effort).
        let _ = self
            .adapter
            .session_store
            .get_index_entry(&result.child_session_id)
            .await;

        ctx.emit_tool_token(format!(
            "Spawned child session: {}",
            result.child_session_id
        ))
        .await;

        Ok(ToolResult {
            success: true,
            result: json!({
                "title": parsed.title.clone(),
                "description": parsed.title,
                "responsibility": parsed.responsibility,
                "prompt": parsed.prompt,
                "subagent_type": parsed.subagent_type,
                "child_session_id": result.child_session_id,
                "parent_session_id": parent_session_id,
                "model": result.model,
                "note": "Child session runs in background. Observe via sub_session_* events, then use sub_session_manager action=send_message/run/update to continue the same child session."
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
    use std::time::Duration;
    use tokio::sync::{broadcast, RwLock};

    use crate::session_app::child_session::format_child_assignment;
    use crate::spawn_scheduler::{SpawnContext, SpawnScheduler};
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
    use bamboo_agent_core::{AgentEvent, Message, Session};
    use bamboo_engine::metrics::storage::SqliteMetricsStorage;
    use bamboo_engine::MetricsCollector;
    use bamboo_engine::SkillManager;
    use bamboo_infrastructure::SessionStoreV2;
    use bamboo_infrastructure::{LLMError, LLMProvider, LLMStream};

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
        let content = format_child_assignment(
            "Find parser entrypoints",
            "Locate parser entrypoints and summarize call graph",
            "general-purpose",
            "Scan src/parser and produce a short report.",
        );

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
        let jsonl = bamboo_infrastructure::JsonlStorage::new(&storage_dir);
        jsonl.init().await.unwrap();
        let storage: Arc<dyn Storage> = Arc::new(jsonl);

        let metrics_storage = Arc::new(SqliteMetricsStorage::new(bamboo_home.join("metrics.db")));
        let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);
        let session_event_senders = Arc::new(RwLock::new(HashMap::<
            String,
            broadcast::Sender<bamboo_agent_core::AgentEvent>,
        >::new()));

        let agent_runtime = Arc::new(
            bamboo_engine::Agent::builder()
                .storage(storage.clone())
                .attachment_reader(session_store.clone())
                .skill_manager(Arc::new(SkillManager::new()))
                .metrics_collector(metrics_collector)
                .config(Arc::new(RwLock::new(
                    bamboo_infrastructure::Config::default(),
                )))
                .provider(Arc::new(NoopProvider))
                .default_tools(Arc::new(NoopToolExecutor))
                .build()
                .expect("test agent should be fully configured"),
        );

        let ctx = crate::spawn_scheduler::SpawnContext {
            agent: agent_runtime,
            tools: Arc::new(NoopToolExecutor),
            sessions_cache: Arc::new(RwLock::new(HashMap::new())),
            agent_runners: Arc::new(RwLock::new(HashMap::new())),
            session_event_senders: session_event_senders.clone(),
        };
        let scheduler = Arc::new(SpawnScheduler::new(ctx));
        let adapter = Arc::new(ChildSessionAdapter {
            session_store,
            storage,
            scheduler,
            sessions_cache: Arc::new(RwLock::new(HashMap::new())),
            agent_runners: Arc::new(RwLock::new(HashMap::new())),
            session_event_senders: session_event_senders.clone(),
        });
        let tool = SpawnSessionTool::new(adapter);

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
                assert!(msg.contains("SubSession requires a session_id in tool context"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_emits_sub_session_started_event_after_queueing() {
        let bamboo_home = make_temp_dir("bamboo-spawn-session-started-event-test");
        tokio::fs::create_dir_all(&bamboo_home).await.unwrap();

        let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
        let storage_dir = bamboo_home.join("storage");
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        let jsonl = bamboo_infrastructure::JsonlStorage::new(&storage_dir);
        jsonl.init().await.unwrap();
        let storage: Arc<dyn Storage> = Arc::new(jsonl);

        let mut parent = Session::new("root-session", "gpt-5");
        parent.title = "Root".to_string();
        storage.save_session(&parent).await.unwrap();
        let parent_session_id = parent.id.clone();

        let metrics_storage = Arc::new(SqliteMetricsStorage::new(bamboo_home.join("metrics.db")));
        let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

        let (parent_tx, mut parent_rx) = broadcast::channel(1000);
        let session_event_senders = Arc::new(RwLock::new(HashMap::<
            String,
            broadcast::Sender<bamboo_agent_core::AgentEvent>,
        >::new()));
        {
            let mut senders = session_event_senders.write().await;
            senders.insert(parent_session_id.clone(), parent_tx);
        }

        let agent_runtime = Arc::new(
            bamboo_engine::Agent::builder()
                .storage(storage.clone())
                .attachment_reader(session_store.clone())
                .skill_manager(Arc::new(SkillManager::new()))
                .metrics_collector(metrics_collector)
                .config(Arc::new(RwLock::new(
                    bamboo_infrastructure::Config::default(),
                )))
                .provider(Arc::new(NoopProvider))
                .default_tools(Arc::new(NoopToolExecutor))
                .build()
                .expect("test agent should be fully configured"),
        );

        let ctx = crate::spawn_scheduler::SpawnContext {
            agent: agent_runtime,
            tools: Arc::new(NoopToolExecutor),
            sessions_cache: Arc::new(RwLock::new(HashMap::new())),
            agent_runners: Arc::new(RwLock::new(HashMap::new())),
            session_event_senders: session_event_senders.clone(),
        };
        let scheduler = Arc::new(SpawnScheduler::new(ctx));
        let adapter = Arc::new(ChildSessionAdapter {
            session_store,
            storage,
            scheduler,
            sessions_cache: Arc::new(RwLock::new(HashMap::new())),
            agent_runners: Arc::new(RwLock::new(HashMap::new())),
            session_event_senders: session_event_senders.clone(),
        });
        let tool = SpawnSessionTool::new(adapter);

        let result = tool
            .execute_with_context(
                json!({
                    "title": "Child A",
                    "responsibility": "Investigate one module",
                    "prompt": "Read module and summarize",
                    "subagent_type": "general-purpose"
                }),
                ToolExecutionContext {
                    session_id: Some(parent_session_id.as_str()),
                    tool_call_id: "tool_call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("SubSession should enqueue a child session");

        let parsed_result: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let child_session_id = parsed_result
            .get("child_session_id")
            .and_then(|value| value.as_str())
            .expect("tool result should include child_session_id")
            .to_string();

        let started_event = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match parent_rx.recv().await {
                    Ok(AgentEvent::SubSessionStarted {
                        parent_session_id: pid,
                        child_session_id: cid,
                        ..
                    }) => break (pid, cid),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("parent stream closed before start event")
                    }
                }
            }
        })
        .await
        .expect("should receive SubSessionStarted event quickly");

        assert_eq!(started_event.0, parent_session_id);
        assert_eq!(started_event.1, child_session_id);
    }
}
