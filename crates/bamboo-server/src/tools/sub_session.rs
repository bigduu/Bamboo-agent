use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use crate::session_app::child_session::{self, ChildSessionPort, CreateChildInput};
use crate::tools::child_session_adapter::{tool_error_from_child_session, ChildSessionAdapter};
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};

// ---------------------------------------------------------------------------
// Args enum
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SubSessionArgs {
    Create {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        description: String,
        #[serde(default)]
        responsibility: Option<String>,
        prompt: String,
        subagent_type: String,
        #[serde(default)]
        auto_run: Option<bool>,
    },
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
        #[serde(default)]
        auto_run: Option<bool>,
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
    Cancel {
        child_session_id: String,
    },
    Delete {
        child_session_id: String,
    },
}

// ---------------------------------------------------------------------------
// Normalization helpers (ported from legacy SpawnSessionTool)
// ---------------------------------------------------------------------------

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

fn tool_result(value: serde_json::Value) -> Result<ToolResult, ToolError> {
    Ok(ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("Collapsible".to_string()),
    })
}

// ---------------------------------------------------------------------------
// Tool struct
// ---------------------------------------------------------------------------

pub struct SubSessionTool {
    adapter: Arc<ChildSessionAdapter>,
}

impl SubSessionTool {
    pub fn new(adapter: Arc<ChildSessionAdapter>) -> Self {
        Self { adapter }
    }
}

#[async_trait]
impl Tool for SubSessionTool {
    fn name(&self) -> &str {
        "SubSession"
    }

    fn description(&self) -> &str {
        "Create, inspect, and manage child sessions for explicitly requested delegated, parallel, or sub-agent work. A child session runs independently under the current root session with its own conversation context, can use a specialized subagent profile, streams progress back to the parent via sub_session_* events, and can be reopened from the Sub-sessions panel. Use action=create for a new delegated task; use list/get to inspect existing children; use update/run/send_message/cancel/delete to manage existing children. Use only when the user explicitly asks for delegation/parallelism or when a side task would otherwise flood the main context. Do not use for simple one-step tasks. Child sessions cannot spawn nested child sessions. IMPORTANT: When a child fails or needs redirection, prefer send_message over creating a duplicate child. Use list before create to avoid spawning redundant children."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["create", "list", "get", "update", "run", "send_message", "cancel", "delete"],
                    "description": "Sub-session lifecycle operation. Use create to delegate a new independent child session; use list/get to inspect; use update/run/send_message/cancel/delete to manage existing child sessions."
                },
                "child_session_id": {
                    "type": "string",
                    "description": "Existing child session id. Required for get/update/run/send_message/cancel/delete."
                },
                "title": {
                    "type": "string",
                    "description": "Short title for a new or updated child session. Required for create. Displayed in the Sub-sessions panel."
                },
                "description": {
                    "type": "string",
                    "description": "Legacy alias of title; prefer title."
                },
                "responsibility": {
                    "type": "string",
                    "description": "Single explicit responsibility for the child session. Required for create. Keep this narrow and non-overlapping with other child sessions."
                },
                "prompt": {
                    "type": "string",
                    "description": "Detailed task instructions, context, constraints, and expected output for the child session. Required for create; optional for update."
                },
                "subagent_type": {
                    "type": "string",
                    "description": "Specialized child agent profile, e.g. general-purpose, researcher, coder, plan. Use plan/researcher for read-only exploration and coder/general-purpose for implementation when allowed."
                },
                "auto_run": {
                    "type": "boolean",
                    "description": "For create/send_message/update: whether to enqueue the child session immediately. Defaults to true for create/send_message and false for update."
                },
                "reset_after_update": {
                    "type": "boolean",
                    "description": "For update: whether to truncate messages after refreshed assignment. Defaults to true."
                },
                "reset_to_last_user": {
                    "type": "boolean",
                    "description": "For run: whether to truncate messages after the last user message before rerun. Defaults to true."
                },
                "message": {
                    "type": "string",
                    "description": "Follow-up instruction to append as a new user message for send_message. Required for send_message."
                },
                "interrupt_running": {
                    "type": "boolean",
                    "description": "For send_message/cancel: if true, cancel a currently running child session before appending or returning. Defaults to false for send_message. When false on a running child, the message is queued and will be picked up at the next turn boundary without canceling progress."
                }
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
            ToolError::Execution("SubSession requires a session_id in tool context".to_string())
        })?;

        // Backward compatibility: legacy SubSession calls did not include an
        // "action" field and always meant "create". If action is missing,
        // default to "create" before deserializing the tagged enum.
        let mut args = args;
        if args.get("action").is_none() {
            args["action"] = json!("create");
        }

        let parsed: SubSessionArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid SubSession args: {error}"))
        })?;

        let parent = self
            .adapter
            .as_ref()
            .load_root_session(parent_session_id)
            .await
            .map_err(tool_error_from_child_session)?;

        match parsed {
            SubSessionArgs::Create {
                title,
                description,
                responsibility,
                prompt,
                subagent_type,
                auto_run,
            } => {
                let title = normalize_title(title, description)?;
                let responsibility = normalize_required_text(responsibility, "responsibility")?;
                let prompt = normalize_required_text(Some(prompt), "prompt")?;
                let subagent_type = normalize_required_text(Some(subagent_type), "subagent_type")?;

                if parent.model.trim().is_empty() {
                    return Err(ToolError::Execution(
                        "parent session model is empty".to_string(),
                    ));
                }

                let child_id = Uuid::new_v4().to_string();
                let model_ref_override = self.adapter.resolve_subagent_model(&subagent_type).await;
                let model_override = model_ref_override
                    .as_ref()
                    .map(|model_ref| model_ref.model.clone());
                let runtime_metadata = self.adapter.resolve_runtime_metadata(&subagent_type).await;
                let system_prompt_override =
                    Some(self.adapter.resolve_subagent_prompt(&subagent_type));

                let result = child_session::create_child_action(
                    self.adapter.as_ref(),
                    CreateChildInput {
                        parent_session: parent.clone(),
                        child_id: child_id.clone(),
                        title: title.clone(),
                        responsibility: responsibility.clone(),
                        assignment_prompt: prompt.clone(),
                        subagent_type: subagent_type.clone(),
                        model_override,
                        model_ref_override,
                        runtime_metadata,
                        system_prompt_override,
                        auto_run: auto_run.unwrap_or(true),
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

                tool_result(json!({
                    "title": title.clone(),
                    "description": title,
                    "responsibility": responsibility,
                    "prompt": prompt,
                    "subagent_type": subagent_type,
                    "child_session_id": result.child_session_id,
                    "parent_session_id": parent_session_id,
                    "model": result.model,
                    "note": "Child session created. Typical execution time: 30-120 seconds. Use action=get to check progress. Wait at least 30 seconds before polling. If the child fails or needs correction, use send_message (not create) to retry in place."
                }))
            }
            SubSessionArgs::List => {
                let result =
                    child_session::list_children_action(self.adapter.as_ref(), &parent.id).await;
                tool_result(result)
            }
            SubSessionArgs::Get { child_session_id } => {
                let result = child_session::get_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubSessionArgs::Update {
                child_session_id,
                title,
                responsibility,
                prompt,
                subagent_type,
                reset_after_update,
                auto_run,
            } => {
                let result = child_session::update_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id.clone(),
                    title,
                    responsibility,
                    prompt,
                    subagent_type,
                    reset_after_update,
                )
                .await
                .map_err(tool_error_from_child_session)?;

                if auto_run.unwrap_or(false) {
                    let child = self
                        .adapter
                        .load_child_for_parent(&parent.id, &child_session_id)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    self.adapter
                        .enqueue_child_run(&parent, &child)
                        .await
                        .map_err(tool_error_from_child_session)?;
                }

                tool_result(result)
            }
            SubSessionArgs::Run {
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
            SubSessionArgs::SendMessage {
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
            SubSessionArgs::Cancel { child_session_id } => {
                let result = child_session::cancel_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubSessionArgs::Delete { child_session_id } => {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::sync::{broadcast, RwLock};

    use crate::app_state::{AgentRunner, AgentStatus};
    use crate::spawn_scheduler::{SpawnContext, SpawnScheduler};
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
    use bamboo_agent_core::{AgentEvent, Message, Role, Session};
    use bamboo_engine::metrics::storage::SqliteMetricsStorage;
    use bamboo_engine::MetricsCollector;
    use bamboo_engine::SkillManager;
    use bamboo_infrastructure::SessionStoreV2;
    use bamboo_infrastructure::{LLMError, LLMProvider, LLMStream};

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

    struct TestHarness {
        tool: SubSessionTool,
        storage: Arc<dyn Storage>,
        agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        parent_session_id: String,
        child_session_id: String,
        parent_rx: broadcast::Receiver<AgentEvent>,
    }

    async fn build_test_harness() -> TestHarness {
        build_test_harness_with_resolver(None).await
    }

    async fn build_test_harness_with_resolver(
        subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    ) -> TestHarness {
        let bamboo_home = make_temp_dir("bamboo-sub-session-test");
        tokio::fs::create_dir_all(&bamboo_home).await.unwrap();

        let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
        let storage_dir = bamboo_home.join("storage");
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        let jsonl = bamboo_infrastructure::JsonlStorage::new(&storage_dir);
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
        session_store.save_session(&parent).await.unwrap();

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
        session_store.save_session(&child).await.unwrap();

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

        let scheduler = Arc::new(SpawnScheduler::new(SpawnContext {
            agent: agent_runtime,
            tools: Arc::new(NoopToolExecutor),
            sessions_cache: sessions_cache.clone(),
            agent_runners: agent_runners.clone(),
            session_event_senders: session_event_senders.clone(),
            external_child_runner: None,
            provider_router: None,
        }));

        let adapter = Arc::new(ChildSessionAdapter {
            session_store,
            storage: storage.clone(),
            scheduler,
            sessions_cache,
            agent_runners: agent_runners.clone(),
            session_event_senders,
            subagent_model_resolver,
            config: Arc::new(RwLock::new(bamboo_infrastructure::Config::default())),
            subagent_profiles: std::sync::Arc::new(
                bamboo_domain::subagent::SubagentProfileRegistry::builder()
                    .extend(crate::subagent_profiles::builtin::builtin_profiles())
                    .build()
                    .expect("builtin subagent profiles must build"),
            ),
        });
        let tool = SubSessionTool::new(adapter);

        TestHarness {
            tool,
            storage,
            agent_runners,
            parent_session_id,
            child_session_id,
            parent_rx,
        }
    }

    // -----------------------------------------------------------------------
    // Normalization tests
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_title_accepts_legacy_description() {
        let title = normalize_title(None, "Search refs".to_string()).unwrap();
        assert_eq!(title, "Search refs");
    }

    #[test]
    fn normalize_title_prefers_title_over_description() {
        let title =
            normalize_title(Some("Real title".to_string()), "Legacy desc".to_string()).unwrap();
        assert_eq!(title, "Real title");
    }

    #[test]
    fn normalize_title_rejects_both_empty() {
        let err = normalize_title(None, "".to_string()).unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("title")));
    }

    // -----------------------------------------------------------------------
    // Create action tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn create_requires_session_id_in_tool_context() {
        let harness = build_test_harness().await;

        let err = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "demo task",
                    "responsibility": "do something",
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
    async fn create_emits_sub_session_started_event_after_queueing() {
        let mut harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Child A",
                    "responsibility": "Investigate one module",
                    "prompt": "Read module and summarize",
                    "subagent_type": "general-purpose"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
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
            .and_then(|v| v.as_str())
            .expect("tool result should include child_session_id")
            .to_string();

        let started_event = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match harness.parent_rx.recv().await {
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

        assert_eq!(started_event.0, harness.parent_session_id);
        assert_eq!(started_event.1, child_session_id);
    }

    #[tokio::test]
    async fn create_uses_async_subagent_model_resolver() {
        let resolver: crate::tools::SubagentModelResolver = Arc::new(|subagent_type: String| {
            Box::pin(async move {
                assert_eq!(subagent_type, "coder");
                Some(bamboo_domain::ProviderModelRef::new(
                    "openai",
                    "gpt-resolved-coder",
                ))
            })
        });
        let harness = build_test_harness_with_resolver(Some(resolver)).await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Coder Child",
                    "responsibility": "Implement a focused change",
                    "prompt": "Patch one file",
                    "subagent_type": "coder",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_async_resolver",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("SubSession should create a child using async model resolver");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["model"], "gpt-resolved-coder");

        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id should be present");
        let child = harness
            .storage
            .load_session(child_id)
            .await
            .unwrap()
            .expect("child session should exist");
        assert_eq!(child.model, "gpt-resolved-coder");
        assert_eq!(
            child.model_ref,
            Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "gpt-resolved-coder",
            ))
        );
        assert_eq!(
            child.metadata.get("provider_name").map(String::as_str),
            Some("openai")
        );
    }

    #[tokio::test]
    async fn backward_compat_legacy_subsession_call_without_action_defaults_to_create() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "title": "Legacy Child",
                    "responsibility": "Test backward compat",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_legacy",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("legacy SubSession call without action should default to create");

        assert!(result.success);
        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert!(parsed.get("child_session_id").is_some());
    }

    // -----------------------------------------------------------------------
    // Management action tests for the unified SubSession tool
    // -----------------------------------------------------------------------

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
    async fn send_message_queues_on_running_child_without_interrupt() {
        let harness = build_test_harness().await;
        {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            runners.insert(harness.child_session_id.clone(), runner);
        }

        let result = harness
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
            .expect("send_message should queue message on running child");

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "message_queued");
        assert_eq!(payload["message"], "continue");

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .expect("child session should exist");
        // Message is NOT appended to messages array while child is running;
        // it is stored in metadata for the agent loop to merge at turn boundaries.
        assert_eq!(child.messages.len(), 3);
        let pending_raw = child
            .metadata
            .get("pending_injected_messages")
            .expect("pending_injected_messages should be set");
        let pending: Vec<child_session::QueuedInjectedMessage> =
            serde_json::from_str(pending_raw).expect("should parse queued messages");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].content, "continue");
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

    #[tokio::test]
    async fn cancel_stops_running_child() {
        let harness = build_test_harness().await;
        let cancel_token = {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            let token = runner.cancel_token.clone();
            runners.insert(harness.child_session_id.clone(), runner);
            token
        };

        let runners_for_wait = harness.agent_runners.clone();
        let child_id_for_wait = harness.child_session_id.clone();
        let waiter = tokio::spawn(async move {
            cancel_token.cancelled().await;
            let mut runners = runners_for_wait.write().await;
            if let Some(runner) = runners.get_mut(&child_id_for_wait) {
                runner.status = AgentStatus::Cancelled;
            }
        });

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "cancel",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_cancel",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("cancel should succeed");

        waiter.await.expect("waiter should finish");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "cancelled");
        assert_eq!(payload["child_session_id"], harness.child_session_id);
    }

    #[tokio::test]
    async fn list_returns_children() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list"}),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_list",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let children = payload["children"]
            .as_array()
            .expect("list result should have children array");
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["child_session_id"], harness.child_session_id);
        assert_eq!(payload["count"], 1);
    }

    #[tokio::test]
    async fn get_returns_runner_diagnostics() {
        let harness = build_test_harness().await;

        // Set up a running runner with diagnostic fields populated.
        {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            runner.last_tool_name = Some("Read".to_string());
            runner.last_tool_phase = Some("begin".to_string());
            runner.round_count = 3;
            runners.insert(harness.child_session_id.clone(), runner);
        }

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "get",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_get_diagnostics",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("get should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["child_session_id"], harness.child_session_id);
        assert_eq!(payload["is_running"], true);
        assert_eq!(payload["last_tool_name"], "Read");
        assert_eq!(payload["last_tool_phase"], "begin");
        assert_eq!(payload["round_count"], 3);
        assert!(payload["runner_started_at"].is_string());
        assert!(payload.get("guidance").is_some());
    }

    #[tokio::test]
    async fn create_returns_duration_hint() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Test Child",
                    "responsibility": "Do something",
                    "prompt": "Do something useful",
                    "subagent_type": "general-purpose",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_hint",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let note = payload["note"].as_str().expect("note should be present");
        assert!(
            note.contains("30-120 seconds"),
            "note should contain estimated duration hint: {note}"
        );
        assert!(
            note.contains("send_message"),
            "note should mention send_message: {note}"
        );
    }

    #[tokio::test]
    async fn delete_removes_child() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "delete",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_delete",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("delete should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["deleted"], true);

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap();
        assert!(child.is_none());
    }
}
