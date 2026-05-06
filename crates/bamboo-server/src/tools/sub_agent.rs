use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use crate::session_app::child_session::{self, ChildSessionPort, CreateChildInput};
use crate::tools::child_session_adapter::{tool_error_from_child_session, ChildSessionAdapter};
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_domain::subagent::SubagentProfileRegistry;
use bamboo_domain::ReasoningEffort;

// ---------------------------------------------------------------------------
// Args enum
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SubAgentArgs {
    Create {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        description: String,
        #[serde(default)]
        responsibility: Option<String>,
        prompt: String,
        subagent_type: String,
        workspace: String,
        #[serde(default)]
        auto_run: Option<bool>,
        /// Optional reasoning effort for the child session. When omitted,
        /// the child stays at `None` so the provider's default applies
        /// (it does NOT inherit the parent's reasoning_effort). The LLM
        /// should pass an explicit value (e.g. `"low"` for cheap fan-outs,
        /// `"high"`/`"max"` for hard reasoning) when it has a preference.
        #[serde(default)]
        reasoning_effort: Option<ReasoningEffort>,
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
        /// Optional reasoning effort to apply to the existing child session.
        /// `Some(level)` overrides the current value; `None` (the default)
        /// leaves it unchanged.
        #[serde(default)]
        reasoning_effort: Option<ReasoningEffort>,
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
    /// Enumerate the available subagent profiles (built-ins plus any
    /// user/project overrides). Read-only; does not touch any session.
    /// Useful both for the LLM (to discover roles before calling
    /// `create`) and for the frontend (to populate a role dropdown).
    ListProfiles,
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

fn waiting_for_children_tool_result(mut value: serde_json::Value) -> Result<ToolResult, ToolError> {
    if let Some(object) = value.as_object_mut() {
        object.insert("runtime_control".to_string(), json!("waiting_for_children"));
        object.insert("wait_for".to_string(), json!("all"));
        object.insert(
            "note".to_string(),
            json!("Child session queued. The parent run is suspended and will resume automatically when the child finishes or times out."),
        );
    }

    Ok(ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("runtime_control:waiting_for_children".to_string()),
    })
}

// ---------------------------------------------------------------------------
// Tool struct
// ---------------------------------------------------------------------------

pub struct SubAgentTool {
    adapter: Arc<ChildSessionAdapter>,
    /// Registry consulted by `action=list_profiles`. Held as `Arc` so the
    /// tool stays cheap to clone and share across executors.
    profiles: Arc<SubagentProfileRegistry>,
}

impl SubAgentTool {
    pub fn new(adapter: Arc<ChildSessionAdapter>, profiles: Arc<SubagentProfileRegistry>) -> Self {
        Self { adapter, profiles }
    }
}

#[async_trait]
impl Tool for SubAgentTool {
    fn name(&self) -> &str {
        "SubAgent"
    }

    fn description(&self) -> &str {
        "Create, inspect, and manage child sessions for explicitly requested delegated, parallel, or sub-agent work. A child session runs independently under the current root session with its own conversation context, can use a specialized subagent profile, streams progress back to the parent via sub_agent_* events, and can be reopened from the Sub-agents panel. Use action=create for a new delegated task; use list/get to inspect existing children; use update/run/send_message/cancel/delete to manage existing children. Use only when the user explicitly asks for delegation/parallelism or when a side task would otherwise flood the main context. Do not use for simple one-step tasks. Child sessions cannot spawn nested child sessions. IMPORTANT: When a child fails or needs redirection, prefer send_message over creating a duplicate child. Use list before create to avoid spawning redundant children."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["create", "list", "get", "update", "run", "send_message", "cancel", "delete", "list_profiles"],
                    "description": "Sub-agent lifecycle operation. Use create to delegate a new independent child session; use list/get to inspect; use update/run/send_message/cancel/delete to manage existing child sessions. Use list_profiles to enumerate available subagent roles before deciding which subagent_type to pass to create."
                },
                "child_session_id": {
                    "type": "string",
                    "description": "Existing child session id. Required for get/update/run/send_message/cancel/delete."
                },
                "title": {
                    "type": "string",
                    "description": "Short title for a new or updated child session. Required for create. Displayed in the Sub-agents panel."
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
                "workspace": {
                    "type": "string",
                    "description": "Absolute path to the working directory for the child session. The child will use this as its workspace for file operations."
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
                },
                "reasoning_effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh", "max"],
                    "description": "For create/update: reasoning effort level applied to the child session's own LLM calls. Use \"low\" for trivial fan-outs (e.g. simple lookups), \"medium\"/\"high\" for normal coding/analysis, \"xhigh\"/\"max\" for deep reasoning tasks. Omit to leave at provider default; the child does NOT inherit the parent's reasoning_effort."
                }
            },
            "required": ["action", "workspace"],
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
            ToolError::Execution("SubAgent requires a session_id in tool context".to_string())
        })?;

        // Backward compatibility: legacy SubAgent calls did not include an
        // "action" field and always meant "create". If action is missing,
        // default to "create" before deserializing the tagged enum.
        let mut args = args;
        if args.get("action").is_none() {
            args["action"] = json!("create");
        }

        let parsed: SubAgentArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid SubAgent args: {error}"))
        })?;

        // `list_profiles` is read-only and operates purely on the
        // in-memory profile registry, so we short-circuit before doing
        // any session lookup. This also lets the LLM call `list_profiles`
        // safely from any context (root or otherwise).
        if let SubAgentArgs::ListProfiles = parsed {
            return tool_result(self.list_profiles_payload());
        }

        let parent = self
            .adapter
            .as_ref()
            .load_root_session(parent_session_id)
            .await
            .map_err(tool_error_from_child_session)?;

        match parsed {
            SubAgentArgs::Create {
                title,
                description,
                responsibility,
                prompt,
                subagent_type,
                workspace,
                auto_run,
                reasoning_effort,
            } => {
                let title = normalize_title(title, description)?;
                let responsibility = normalize_required_text(responsibility, "responsibility")?;
                let prompt = normalize_required_text(Some(prompt), "prompt")?;
                let subagent_type = normalize_required_text(Some(subagent_type), "subagent_type")?;
                let workspace = normalize_required_text(Some(workspace), "workspace")?;

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

                let should_auto_run = auto_run.unwrap_or(true);
                let result = child_session::create_child_action(
                    self.adapter.as_ref(),
                    CreateChildInput {
                        parent_session: parent.clone(),
                        child_id: child_id.clone(),
                        title: title.clone(),
                        responsibility: responsibility.clone(),
                        assignment_prompt: prompt.clone(),
                        subagent_type: subagent_type.clone(),
                        workspace: workspace.clone(),
                        model_override,
                        model_ref_override,
                        runtime_metadata,
                        system_prompt_override,
                        auto_run: should_auto_run,
                        reasoning_effort,
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

                let payload = json!({
                    "title": title.clone(),
                    "description": title,
                    "responsibility": responsibility,
                    "prompt": prompt,
                    "subagent_type": subagent_type,
                    "child_session_id": result.child_session_id,
                    "parent_session_id": parent_session_id,
                    "model": result.model,
                    "reasoning_effort": reasoning_effort.map(|effort| effort.as_str()),
                    "status": if should_auto_run { "queued" } else { "created" },
                    "note": "Child session created. Typical execution time: 30-120 seconds. If the child fails or needs correction, use send_message (not create) to retry in place."
                });
                if should_auto_run {
                    waiting_for_children_tool_result(payload)
                } else {
                    tool_result(payload)
                }
            }
            SubAgentArgs::List => {
                let result =
                    child_session::list_children_action(self.adapter.as_ref(), &parent.id).await;
                tool_result(result)
            }
            SubAgentArgs::Get { child_session_id } => {
                let result = child_session::get_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubAgentArgs::Update {
                child_session_id,
                title,
                responsibility,
                prompt,
                subagent_type,
                reset_after_update,
                auto_run,
                reasoning_effort,
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
                    reasoning_effort,
                )
                .await
                .map_err(tool_error_from_child_session)?;

                let should_auto_run = auto_run.unwrap_or(false);
                if should_auto_run {
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

                if should_auto_run {
                    waiting_for_children_tool_result(result)
                } else {
                    tool_result(result)
                }
            }
            SubAgentArgs::Run {
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
                waiting_for_children_tool_result(result)
            }
            SubAgentArgs::SendMessage {
                child_session_id,
                message,
                auto_run,
                interrupt_running,
            } => {
                let should_auto_run = auto_run.unwrap_or(true);
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
                if should_auto_run
                    && result
                        .get("status")
                        .and_then(|value| value.as_str())
                        .is_some_and(|status| status == "queued")
                {
                    waiting_for_children_tool_result(result)
                } else {
                    tool_result(result)
                }
            }
            SubAgentArgs::Cancel { child_session_id } => {
                let result = child_session::cancel_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubAgentArgs::Delete { child_session_id } => {
                let result = child_session::delete_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            // Already short-circuited above; kept here so the match stays
            // exhaustive without a wildcard.
            SubAgentArgs::ListProfiles => tool_result(self.list_profiles_payload()),
        }
    }
}

impl SubAgentTool {
    /// Build the JSON payload returned by `action=list_profiles`.
    ///
    /// Shape (kept stable as a public contract for the frontend and for
    /// the LLM):
    ///
    /// ```jsonc
    /// {
    ///   "profiles": [
    ///     {
    ///       "id": "researcher",
    ///       "display_name": "Researcher",
    ///       "description": "...",
    ///       "tools": { "mode": "allowlist", "allow": ["Read", "Grep"] },
    ///       "model_hint": null,
    ///       "default_responsibility": null,
    ///       "ui": { "icon": "🔎", "color": "blue" }
    ///       // NOTE: `system_prompt` is intentionally omitted from the
    ///       // listing — it can be lengthy and is not needed for UI/LLM
    ///       // selection. Use `action=get` on a child to inspect the
    ///       // resolved prompt of an active session.
    ///     }
    ///   ],
    ///   "fallback_id": "general-purpose",
    ///   "count": 6
    /// }
    /// ```
    fn list_profiles_payload(&self) -> serde_json::Value {
        // Project each profile into a UI-friendly shape that excludes the
        // (potentially large) `system_prompt`. This keeps the payload
        // small for both the LLM context window and the frontend list.
        let profiles: Vec<serde_json::Value> = self
            .profiles
            .iter()
            .map(|p| {
                json!({
                    "id": p.id,
                    "display_name": p.display_name,
                    "description": p.description,
                    "tools": p.tools,
                    "model_hint": p.model_hint,
                    "default_responsibility": p.default_responsibility,
                    "ui": p.ui,
                })
            })
            .collect();
        json!({
            "profiles": profiles,
            "fallback_id": self.profiles.fallback_id(),
            "count": self.profiles.len(),
        })
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
        tool: SubAgentTool,
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
        let bamboo_home = make_temp_dir("bamboo-sub-agent-test");
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
                .persistence(Arc::new(bamboo_infrastructure::LockedSessionStore::new(
                    storage.clone(),
                )))
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
            completion_handler: None,
        }));

        let test_profiles = std::sync::Arc::new(
            bamboo_domain::subagent::SubagentProfileRegistry::builder()
                .extend(crate::subagent_profiles::builtin::builtin_profiles())
                .build()
                .expect("builtin subagent profiles must build"),
        );
        let adapter = Arc::new(ChildSessionAdapter {
            session_store,
            storage: storage.clone(),
            persistence: Arc::new(bamboo_infrastructure::LockedSessionStore::new(
                storage.clone(),
            )),
            scheduler,
            sessions_cache,
            agent_runners: agent_runners.clone(),
            session_event_senders,
            subagent_model_resolver,
            config: Arc::new(RwLock::new(bamboo_infrastructure::Config::default())),
            subagent_profiles: test_profiles.clone(),
            tool_names: Vec::new(),
        });
        let tool = SubAgentTool::new(adapter, test_profiles);

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
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext::none("tool_call"),
            )
            .await
            .unwrap_err();

        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("SubAgent requires a session_id in tool context"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_emits_sub_agent_started_event_after_queueing() {
        let mut harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Child A",
                    "responsibility": "Investigate one module",
                    "prompt": "Read module and summarize",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("SubAgent should enqueue a child session");

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
                    Ok(AgentEvent::SubAgentStarted {
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
        .expect("should receive SubAgentStarted event quickly");

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
                    "workspace": "/tmp/test-workspace",
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
            .expect("SubAgent should create a child using async model resolver");

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
    async fn backward_compat_legacy_subagent_call_without_action_defaults_to_create() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "title": "Legacy Child",
                    "responsibility": "Test backward compat",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_legacy",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("legacy SubAgent call without action should default to create");

        assert!(result.success);
        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert!(parsed.get("child_session_id").is_some());
    }

    // -----------------------------------------------------------------------
    // Management action tests for the unified SubAgent tool
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
                    Ok(AgentEvent::SubAgentStarted {
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
        .expect("should receive SubAgentStarted event");

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
                    "workspace": "/tmp/test-workspace",
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
    async fn create_persists_explicit_reasoning_effort_to_child_session() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Reasoning Child",
                    "responsibility": "Investigate hard problem",
                    "prompt": "Think carefully step by step",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false,
                    "reasoning_effort": "high"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_with_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(
            payload["reasoning_effort"].as_str(),
            Some("high"),
            "tool result should echo the resolved reasoning_effort"
        );

        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id present")
            .to_string();
        let child = harness
            .storage
            .load_session(&child_id)
            .await
            .expect("child should be persisted")
            .expect("child session should exist");
        assert_eq!(
            child.reasoning_effort,
            Some(bamboo_domain::ReasoningEffort::High),
            "child.reasoning_effort should reflect the explicit override"
        );
    }

    #[tokio::test]
    async fn create_without_reasoning_effort_leaves_child_at_provider_default() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Default Child",
                    "responsibility": "Quick lookup",
                    "prompt": "Read a file and summarise",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_default_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert!(
            payload["reasoning_effort"].is_null(),
            "tool result should report null reasoning_effort when omitted, got {:?}",
            payload["reasoning_effort"]
        );

        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id present")
            .to_string();
        let child = harness
            .storage
            .load_session(&child_id)
            .await
            .expect("child should be persisted")
            .expect("child session should exist");
        assert_eq!(
            child.reasoning_effort, None,
            "child.reasoning_effort should stay at None (provider default) when caller omits it; \
             children must NOT inherit the parent's reasoning_effort"
        );
    }

    #[tokio::test]
    async fn update_can_change_reasoning_effort_on_existing_child() {
        let harness = build_test_harness().await;

        // Pre-condition: the seeded child has reasoning_effort = None.
        let seeded = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .expect("seeded child should load")
            .expect("seeded child exists");
        assert_eq!(seeded.reasoning_effort, None);

        let _ = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "update",
                    "child_session_id": harness.child_session_id,
                    "reasoning_effort": "max"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_update_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("update should succeed");

        let updated = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .expect("updated child should load")
            .expect("child still exists");
        assert_eq!(
            updated.reasoning_effort,
            Some(bamboo_domain::ReasoningEffort::Max),
            "update should persist the new reasoning_effort"
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

    /// `action=list_profiles` returns every built-in profile (without
    /// the `system_prompt` body), reports the registry's fallback id,
    /// and uses the registry's stable insertion order. The shape of
    /// this payload is a public contract — UI / LLM rely on it.
    #[tokio::test]
    async fn list_profiles_returns_builtin_catalog() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list_profiles"}),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_list_profiles",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list_profiles should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");

        // Top-level shape.
        let profiles = payload["profiles"]
            .as_array()
            .expect("list_profiles must return a `profiles` array");
        assert!(
            profiles.len() >= 6,
            "expected at least 6 built-in profiles, got {}",
            profiles.len()
        );
        assert_eq!(payload["count"], profiles.len());
        assert_eq!(payload["fallback_id"], "general-purpose");

        // Required fields per profile, and explicit guarantee that we
        // do NOT leak `system_prompt` (could be very large).
        for entry in profiles {
            assert!(entry.get("id").and_then(|v| v.as_str()).is_some());
            assert!(entry.get("display_name").and_then(|v| v.as_str()).is_some());
            assert!(entry.get("tools").is_some());
            assert!(
                entry.get("system_prompt").is_none(),
                "system_prompt must NOT be returned by list_profiles",
            );
        }

        // Built-in catalogue must include the documented baseline ids
        // so the LLM can rely on them being present.
        let ids: Vec<&str> = profiles
            .iter()
            .map(|p| p["id"].as_str().unwrap_or(""))
            .collect();
        for required in [
            "general-purpose",
            "plan",
            "researcher",
            "coder",
            "reviewer",
            "tester",
        ] {
            assert!(
                ids.contains(&required),
                "built-in profile `{required}` missing from list_profiles output (got: {ids:?})"
            );
        }
    }

    /// `list_profiles` is read-only and must not require a real,
    /// loadable parent session. We pass a non-existent session_id and
    /// expect success (registry is consulted directly, no session
    /// lookup is performed).
    #[tokio::test]
    async fn list_profiles_does_not_load_parent_session() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list_profiles"}),
                ToolExecutionContext {
                    session_id: Some("non-existent-session-id"),
                    tool_call_id: "tool_call_list_profiles_no_session",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list_profiles should succeed even when the parent session id is unknown");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert!(payload["profiles"].as_array().is_some());
    }

    #[tokio::test]
    async fn create_requires_workspace() {
        let harness = build_test_harness().await;

        let err = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "No Workspace Child",
                    "responsibility": "Test workspace validation",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_no_workspace",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .unwrap_err();

        match err {
            ToolError::InvalidArguments(msg) => {
                assert!(msg.contains("workspace"), "error should mention workspace: {msg}");
            }
            other => panic!("expected InvalidArguments error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_sets_child_workspace() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Workspace Child",
                    "responsibility": "Test workspace propagation",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_workspace",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed with workspace");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id should be present")
            .to_string();

        let child = harness
            .storage
            .load_session(&child_id)
            .await
            .expect("child should be persisted")
            .expect("child session should exist");
        assert_eq!(
            child.workspace,
            Some("/tmp/test-workspace".to_string()),
            "child workspace should be set from create args"
        );
    }
}
