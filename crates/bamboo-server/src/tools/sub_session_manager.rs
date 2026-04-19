use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::session_app::child_session::{self, ChildSessionPort};
use bamboo_agent_core::tools::{Tool, ToolError, ToolResult, ToolExecutionContext};
use crate::tools::child_session_adapter::{
    ChildSessionAdapter, tool_error_from_child_session,
};

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

pub struct SubSessionManagerTool {
    adapter: Arc<ChildSessionAdapter>,
}

fn tool_result(value: serde_json::Value) -> Result<ToolResult, ToolError> {
    Ok(ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("Collapsible".to_string()),
    })
}

impl SubSessionManagerTool {
    pub fn new(adapter: Arc<ChildSessionAdapter>) -> Self {
        Self { adapter }
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
        let parent = self.adapter.as_ref().load_root_session(parent_session_id).await.map_err(tool_error_from_child_session)?;

        let parsed: SubSessionManagerArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid sub_session_manager args: {error}"))
        })?;

        match parsed {
            SubSessionManagerArgs::List => {
                let result = child_session::list_children_action(self.adapter.as_ref(), &parent.id).await;
                tool_result(result)
            }
            SubSessionManagerArgs::Get { child_session_id } => {
                let result = child_session::get_child_action(self.adapter.as_ref(), &parent.id, child_session_id)
                    .await
                    .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubSessionManagerArgs::Update {
                child_session_id,
                title,
                responsibility,
                prompt,
                subagent_type,
                reset_after_update,
            } => {
                let result = child_session::update_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                    title,
                    responsibility,
                    prompt,
                    subagent_type,
                    reset_after_update,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubSessionManagerArgs::Run {
                child_session_id,
                reset_to_last_user,
            } => {
                let result = child_session::run_child_action(
                    self.adapter.as_ref(),
                    &parent,
                    child_session_id,
                    reset_to_last_user,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubSessionManagerArgs::SendMessage {
                child_session_id,
                message,
                auto_run,
                interrupt_running,
            } => {
                let result = child_session::send_message_to_child_action(
                    self.adapter.as_ref(),
                    &parent,
                    child_session_id,
                    message,
                    auto_run,
                    interrupt_running,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubSessionManagerArgs::Delete { child_session_id } => {
                let result = child_session::delete_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tokio::sync::{broadcast, RwLock};
    use tokio::time::Duration;
    use uuid::Uuid;

    use bamboo_infrastructure::{JsonlStorage, SessionStoreV2};
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolExecutionContext, ToolExecutor, ToolSchema};
    use bamboo_agent_core::{AgentEvent, Message, Role, Session};
    use bamboo_infrastructure::{LLMError, LLMProvider, LLMStream};
    use bamboo_engine::{MetricsCollector, SqliteMetricsStorage};
    use bamboo_engine::SkillManager;
    use crate::app_state::{AgentRunner, AgentStatus};
    use crate::spawn_scheduler::{SpawnContext, SpawnScheduler};

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
            _call: &bamboo_agent_core::tools::ToolCall,
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

        let agent_runtime = Arc::new(
            bamboo_engine::Agent::builder()
                .storage(storage.clone())
                .attachment_reader(session_store.clone())
                .skill_manager(Arc::new(SkillManager::new()))
                .metrics_collector(metrics_collector)
                .config(Arc::new(RwLock::new(bamboo_infrastructure::Config::default())))
                .provider(Arc::new(NoopProvider))
                .default_tools(Arc::new(NoopToolExecutor))
                .build()
                .expect("test agent should be fully configured"),
        );

        let scheduler = Arc::new(SpawnScheduler::new(SpawnContext {
            agent: agent_runtime,
            tools: Arc::new(NoopToolExecutor),
            sessions_cache: sessions_cache.clone(),
            agent_runners: agent_runners.clone(),
            session_event_senders: session_event_senders.clone(),
        }));

        let adapter = Arc::new(ChildSessionAdapter {
            session_store,
            storage: storage.clone(),
            scheduler,
            sessions_cache,
            agent_runners: agent_runners.clone(),
            session_event_senders,
        });
        let tool = SubSessionManagerTool::new(adapter);

        TestHarness {
            tool,
            storage,
            agent_runners,
            parent_session_id,
            child_session_id,
            parent_rx,
        }
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
