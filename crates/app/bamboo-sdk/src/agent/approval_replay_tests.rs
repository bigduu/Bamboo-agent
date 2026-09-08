use super::*;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{
    FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolExecutionContext,
    ToolExecutionSessionFlags, ToolExecutor, ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_domain::{AgentRuntimeState, PendingQuestionSource, RuntimeSessionPersistence};
use bamboo_engine::session_app::respond::{
    acquire_pending_response_guard, submit_pending_permission_response_checked_guarded,
    PERMISSION_REEXECUTE_GENERATION_METADATA_KEY,
};
use bamboo_engine::{SessionCache, SessionRepository};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use bamboo_tools::permission::{
    current_permission_replay_generation, ConfigPermissionChecker, PermissionConfig,
    PermissionDecision, PermissionDecisionKind, PermissionDecisionReceipt, PermissionReasonCode,
    PermissionRequest, RiskLevel,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

const CALL: &str = "reused-call";
const CURRENT: &str = "generation-a";
const SECOND: &str = "generation-b";

#[derive(Debug)]
struct Invocation {
    name: String,
    arguments: Value,
    generation: Option<String>,
    flags: ToolExecutionSessionFlags,
    supervisor_absent: bool,
}

#[derive(Default)]
struct Probe {
    calls: Mutex<Vec<Invocation>>,
    actions: AtomicUsize,
    provider_calls: AtomicUsize,
    fail_execution: AtomicBool,
    next_call: Mutex<Option<ToolCall>>,
}

#[async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        self.provider_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(call) = self.next_call.lock().unwrap().take() {
            return Ok(Box::pin(futures::stream::iter([
                Ok(bamboo_llm::LLMChunk::ToolCalls(vec![call])),
                Ok(bamboo_llm::LLMChunk::Done),
            ])));
        }
        Ok(Box::pin(futures::stream::iter([
            Ok(bamboo_llm::LLMChunk::Token("done".into())),
            Ok(bamboo_llm::LLMChunk::Done),
        ])))
    }
}

struct RecordingExecutor {
    probe: Arc<Probe>,
    config: Arc<PermissionConfig>,
    typed: bool,
    second_context: bool,
    action_log: std::path::PathBuf,
}

#[async_trait]
impl ToolExecutor for RecordingExecutor {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("replay must supply its execution context")
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let session_id = ctx.session_id.expect("session identity");
        let generation = current_permission_replay_generation(session_id, &call.id);
        self.probe.calls.lock().unwrap().push(Invocation {
            name: call.function.name.clone(),
            arguments: serde_json::from_str(&call.function.arguments).unwrap(),
            generation: generation.clone(),
            flags: ToolExecutionSessionFlags {
                bypass_permissions: ctx.bypass_permissions,
                auto_approve_permissions: ctx.auto_approve_permissions,
                plan_read_only: ctx.plan_read_only,
            },
            supervisor_absent: ctx.executing_supervisor.is_none(),
        });
        if self.typed {
            let generation =
                generation.ok_or_else(|| ToolError::Execution("missing generation".into()))?;
            for (other_session, other_generation, other_resource) in [
                ("another-session", generation.as_str(), "current-command"),
                (session_id, "another-generation", "current-command"),
                (session_id, generation.as_str(), "another-command"),
            ] {
                assert!(!self.config.consume_once_for_generation(
                    other_session,
                    CALL,
                    other_generation,
                    PermissionType::ExecuteCommand,
                    other_resource,
                ));
            }
            if !self.config.consume_once_for_generation(
                session_id,
                CALL,
                &generation,
                PermissionType::ExecuteCommand,
                "current-command",
            ) {
                return Err(ToolError::Execution("missing exact A authorization".into()));
            }
            if self.second_context
                && !self.config.consume_once_for_generation(
                    session_id,
                    CALL,
                    &generation,
                    PermissionType::WriteFile,
                    "second-resource",
                )
            {
                return Ok(waiting_result(&request(
                    session_id,
                    SECOND,
                    "second-resource",
                    PermissionType::WriteFile,
                )));
            }
        }
        if self.probe.fail_execution.load(Ordering::SeqCst) {
            return Err(ToolError::Execution("intentional execution failure".into()));
        }
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.action_log)
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        std::io::Write::write_all(&mut log, b"physical action\n")
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        self.probe.actions.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::text(
            true,
            format!("REAL OUTPUT {}", call.function.arguments),
        ))
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        vec![serde_json::from_value(json!({
            "type": "function", "function": { "name": "Bash", "description": "record execution",
            "parameters": { "type": "object", "properties": { "command": { "type": "string" } } } }
        }))
        .unwrap()]
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    persistence: Arc<LockedSessionStore>,
    repo: SessionRepository,
    probe: Arc<Probe>,
}

struct RegisteredProbeTool {
    name: String,
    probe: Arc<Probe>,
    config: Arc<PermissionConfig>,
}

#[async_trait]
impl Tool for RegisteredProbeTool {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        "record the selected exact tool"
    }
    fn parameters_schema(&self) -> Value {
        json!({ "type": "object", "properties": { "command": { "type": "string" } }, "required": ["command"] })
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let session_id = ctx.session_id.as_deref().unwrap();
        // Bash is gated by the real BuiltinToolExecutor. The exact custom
        // registration requests its own permission through the same public
        // evaluator, so the engine creates both pending structures itself.
        if self.name == "execute_command" {
            match self
                .config
                .evaluate(bamboo_tools::permission::PermissionEvaluation {
                    request_id: ctx.tool_call_id.to_string(),
                    session_id: session_id.into(),
                    workspace_path: None,
                    tool_name: self.name.clone(),
                    tool_args: args.clone(),
                    permission_type: PermissionType::ExecuteCommand,
                    resource: args["command"].as_str().unwrap().into(),
                    operation_summary: "custom operation".into(),
                    risk_level: RiskLevel::High,
                    bypass_requested: ctx.bypass_permissions,
                    auto_approve_requested: ctx.auto_approve_permissions,
                    platform_hard_deny: None,
                    consume_once: true,
                    supported_decisions: PermissionRequest::forced_decisions(),
                }) {
                bamboo_tools::permission::PermissionOutcome::Ask(request) => {
                    self.config.register_pending_request(request.clone());
                    let mut result = waiting_result(&request);
                    // The normal runner's legacy prompt adapter accepts only
                    // successful gate synths, matching BuiltinToolExecutor.
                    result.success = true;
                    return Ok(ToolOutcome::Completed(result));
                }
                bamboo_tools::permission::PermissionOutcome::Allow { .. } => {}
                bamboo_tools::permission::PermissionOutcome::Deny { reason, .. } => {
                    return Err(ToolError::Execution(reason.message));
                }
            }
        }
        self.probe.calls.lock().unwrap().push(Invocation {
            name: self.name.clone(),
            arguments: args.clone(),
            generation: current_permission_replay_generation(session_id, &ctx.tool_call_id),
            flags: ToolExecutionSessionFlags {
                bypass_permissions: ctx.bypass_permissions,
                auto_approve_permissions: ctx.auto_approve_permissions,
                plan_read_only: ctx.plan_read_only,
            },
            supervisor_absent: ctx.executing_supervisor.is_none(),
        });
        self.probe.actions.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            format!("REAL OUTPUT {args}"),
        )))
    }
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(directory.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        let repo = SessionRepository::new(SessionCache::default(), storage, persistence.clone());
        Self {
            directory,
            store,
            persistence,
            repo,
            probe: Arc::new(Probe::default()),
        }
    }

    fn agent(
        &self,
        config: Option<Arc<PermissionConfig>>,
        typed: bool,
        second_context: bool,
        persistence: Option<Arc<dyn RuntimeSessionPersistence>>,
    ) -> Agent {
        let executor = Arc::new(RecordingExecutor {
            probe: self.probe.clone(),
            config: config.clone().unwrap_or_default(),
            typed,
            second_context,
            action_log: self.directory.path().join("actions.log"),
        });
        self.agent_with_executor(config, executor, persistence)
    }

    fn agent_with_executor(
        &self,
        config: Option<Arc<PermissionConfig>>,
        executor: Arc<dyn ToolExecutor>,
        persistence: Option<Arc<dyn RuntimeSessionPersistence>>,
    ) -> Agent {
        let metrics = bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                self.directory.path().join("metrics.db"),
            )),
            7,
        );
        let runtime = RuntimeAgent::builder()
            .storage(self.store.clone())
            .persistence(persistence.unwrap_or_else(|| self.persistence.clone()))
            .attachment_reader(self.store.clone())
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(metrics)
            .config(Arc::new(tokio::sync::RwLock::new(
                bamboo_llm::Config::default(),
            )))
            .provider(self.probe.clone())
            .default_tools(executor)
            .build()
            .unwrap();
        Agent::from_runtime_with_config(
            runtime,
            Some("configured System".into()),
            Some("test-model".into()),
            None,
            None,
            None,
            config.map(|config| {
                Arc::new(ConfigPermissionChecker::new(config)) as Arc<dyn PermissionChecker>
            }),
            PermissionMode::Default,
        )
    }

    fn registered_agent(&self, config: Arc<PermissionConfig>, names: &[&str]) -> Agent {
        let registry = bamboo_tools::ToolRegistry::new();
        for name in names {
            registry
                .register(RegisteredProbeTool {
                    name: (*name).into(),
                    probe: self.probe.clone(),
                    config: config.clone(),
                })
                .unwrap();
        }
        let executor = bamboo_tools::executor::BuiltinToolExecutor::with_registry_and_permissions(
            registry,
            Arc::new(ConfigPermissionChecker::new(config.clone())),
        );
        self.agent_with_executor(Some(config), Arc::new(executor), None)
    }

    async fn reload(&self, id: &str) -> Session {
        let durable = self.store.load_session(id).await.unwrap().unwrap();
        serde_json::from_slice(&serde_json::to_vec(&durable).unwrap()).unwrap()
    }

    async fn reopen_store(&mut self) {
        let store = Arc::new(
            SessionStoreV2::new(self.directory.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        self.repo = SessionRepository::new(SessionCache::default(), storage, persistence.clone());
        self.persistence = persistence;
        self.store = store;
    }

    async fn approve(&self, request: &PermissionRequest) -> Session {
        let guard = acquire_pending_response_guard(&request.session_id).await;
        submit_pending_permission_response_checked_guarded(
            &self.repo,
            RespondInput {
                session_id: request.session_id.clone(),
                user_response: "Approve".into(),
                model: None,
                model_ref: None,
                provider: None,
                reasoning_effort: None,
            },
            Some(CALL.into()),
            PermissionDecisionReceipt {
                session_id: request.session_id.clone(),
                decision: PermissionDecision {
                    request_id: CALL.into(),
                    request_generation: request.request_generation.clone(),
                    decision: PermissionDecisionKind::AllowOnce,
                    matcher_id: None,
                    expected_policy_revision: Some(request.policy_revision),
                    confirm_global: false,
                },
                decided_at: std::time::SystemTime::now().into(),
            },
            &guard,
        )
        .await
        .unwrap()
        .0
    }
}

fn request(
    id: &str,
    generation: &str,
    resource: &str,
    permission_type: PermissionType,
) -> PermissionRequest {
    PermissionRequest {
        request_id: CALL.into(),
        request_generation: generation.into(),
        session_id: id.into(),
        workspace_path: None,
        tool_name: "Bash".into(),
        permission_type,
        resource: resource.into(),
        operation_summary: format!("operate on {resource}"),
        risk_level: RiskLevel::High,
        reason_code: PermissionReasonCode::RiskThreshold,
        effective_mode: PermissionMode::Default,
        bypass_requested: false,
        auto_approve_requested: false,
        policy_revision: 4,
        matched_rule: None,
        allowed_decisions: vec![
            PermissionDecisionKind::AllowOnce,
            PermissionDecisionKind::DenyOnce,
        ],
        suggested_matchers: vec![],
    }
}

fn waiting_result(request: &PermissionRequest) -> ToolResult {
    ToolResult { success: false, result: json!({
        "status": "awaiting_permission_approval", "question": "Permission required", "options": ["Approve", "Deny"],
        "allow_custom": false, "permission_type": request.permission_type, "resource": request.resource,
        "permission_request": request,
    }).to_string(), display_preference: Some("request_permissions".into()), images: vec![] }
}

fn parked(id: &str, typed: bool) -> (Session, PermissionRequest) {
    let mut session = Session::new(id, "test-model");
    session.agent_runtime_state = Some(AgentRuntimeState::new(id));
    session.add_message(Message::system("caller System"));
    session.add_message(Message::user("perform the operation"));
    let current = request(
        id,
        CURRENT,
        "current-command",
        PermissionType::ExecuteCommand,
    );
    for (name, request) in [
        (
            "old",
            request(
                id,
                "generation-old",
                "old-command",
                PermissionType::ExecuteCommand,
            ),
        ),
        ("current", current.clone()),
    ] {
        session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: CALL.into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Bash".into(),
                    arguments: json!({"command": request.resource}).to_string(),
                },
            }]),
        ));
        let mut payload: Value = serde_json::from_str(&waiting_result(&request).result).unwrap();
        if !typed {
            payload
                .as_object_mut()
                .unwrap()
                .remove("permission_request");
        }
        let mut result = Message::tool_result_with_status(CALL, payload.to_string(), false);
        result.id = format!("result-{name}");
        session.add_message(result);
    }
    session.set_pending_question_with_source(
        CALL.into(),
        "Bash".into(),
        "Permission required".into(),
        vec!["Approve".into(), "Deny".into()],
        false,
        PendingQuestionSource::PauseTool,
    );
    session.metadata.insert(
        "runtime.suspend_reason".into(),
        "awaiting_clarification".into(),
    );
    (session, current)
}

fn result_message(session: &Session, id: &str) -> Value {
    serde_json::to_value(
        session
            .messages
            .iter()
            .find(|message| message.id == id)
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn legacy_public_answer_run_replays_only_the_latest_occurrence() {
    let fixture = Fixture::new().await;
    let (mut session, _) = parked("legacy-duplicate", false);
    let old = result_message(&session, "result-old");
    fixture.repo.save(&mut session).await.unwrap();
    let agent = fixture.agent(None, false, false, None);
    let mut session = agent.answer(&session.id, "Approve").await.unwrap().session;
    agent.run(&mut session, "continue").await.unwrap();
    let calls = fixture.probe.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].arguments, json!({"command": "current-command"}));
    assert!(calls[0].generation.is_none());
    assert_eq!(result_message(&session, "result-old"), old);
    assert!(result_message(&session, "result-current")["content"]
        .as_str()
        .unwrap()
        .starts_with("REAL OUTPUT"));
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn typed_public_resume_restores_exact_once_after_storage_and_serde_reload() {
    let fixture = Fixture::new().await;
    let (mut session, request) = parked("typed-restart", true);
    let old = result_message(&session, "result-old");
    fixture.repo.save(&mut session).await.unwrap();
    fixture.approve(&request).await;
    let mut session = fixture.reload(&session.id).await;
    let config = Arc::new(PermissionConfig::new());
    let agent = fixture.agent(Some(config.clone()), true, false, None);
    agent.resume(&mut session).await.unwrap();
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
    let calls = fixture.probe.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].arguments, json!({"command": "current-command"}));
    assert_eq!(calls[0].generation.as_deref(), Some(CURRENT));
    assert!(calls[0].supervisor_absent);
    assert_eq!(calls[0].flags, ToolExecutionSessionFlags::default());
    assert!(!config.consume_once_for_generation(
        &session.id,
        CALL,
        CURRENT,
        PermissionType::ExecuteCommand,
        "current-command"
    ));
    assert_eq!(result_message(&session, "result-old"), old);
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
}

async fn events(mut receiver: mpsc::Receiver<AgentEvent>) -> Vec<AgentEvent> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async move {
        let mut events = Vec::new();
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .expect("SDK stream must terminate")
}

fn replay_state(session: &Session) -> Value {
    json!({
        "pending": session.pending_question,
        "call": session.metadata.get(PERMISSION_REEXECUTE_METADATA_KEY),
        "generation": session.metadata.get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY),
        "suspend_reason": session.metadata.get("runtime.suspend_reason"),
        "current": result_message(session, "result-current"),
        "old": result_message(session, "result-old"),
    })
}

fn alias_call() -> ToolCall {
    ToolCall {
        id: CALL.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "execute_command".into(),
            arguments: json!({"command": "current-command"}).to_string(),
        },
    }
}

fn current_request(session: &Session) -> PermissionRequest {
    let result = session
        .messages
        .iter()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some(CALL))
        .unwrap();
    let payload: Value = serde_json::from_str(&result.content).unwrap();
    serde_json::from_value(payload["permission_request"].clone()).unwrap()
}

fn alias_config() -> Arc<PermissionConfig> {
    let config = Arc::new(PermissionConfig::new());
    config.set_ask_rules(["Bash(current-command)".into(), "execute_command".into()]);
    config
}

async fn alias_pending_fixture() -> (Fixture, Session, PermissionRequest) {
    let fixture = Fixture::new().await;
    let agent = fixture.registered_agent(alias_config(), &["Bash"]);
    *fixture.probe.next_call.lock().unwrap() = Some(alias_call());
    let initial_events =
        events(agent.run_stream(Session::new("alias-pending", "test-model"), "operate")).await;
    assert!(initial_events
        .iter()
        .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
    let pending = agent.load_session("alias-pending").await.unwrap().unwrap();
    assert_eq!(
        pending.pending_question.as_ref().unwrap().tool_name,
        "execute_command"
    );
    assert_eq!(
        pending.pending_question.as_ref().unwrap().source,
        PendingQuestionSource::PauseTool
    );
    let request = current_request(&pending);
    assert_eq!(request.tool_name, "Bash");
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    (fixture, pending, request)
}

#[tokio::test]
async fn typed_alias_pending_retains_the_real_original_question_identity() {
    let (fixture, pending, _) = Box::pin(alias_pending_fixture()).await;
    let agent = fixture.registered_agent(alias_config(), &["Bash"]);
    let calls_before = fixture.probe.provider_calls.load(Ordering::SeqCst);
    let waiting = events(agent.resume_stream(pending)).await;
    assert_eq!(waiting.len(), 1);
    assert!(
        matches!(&waiting[0], AgentEvent::NeedClarification { tool_name: Some(name), .. } if name == "execute_command")
    );
    assert_eq!(
        fixture.probe.provider_calls.load(Ordering::SeqCst),
        calls_before
    );
    assert!(fixture.probe.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn typed_alias_approved_replay_reaches_the_registered_bash_owner() {
    let (mut fixture, _, request) = Box::pin(alias_pending_fixture()).await;
    fixture.approve(&request).await;
    fixture.reopen_store().await;
    let agent = fixture.registered_agent(alias_config(), &["Bash"]);
    let session = agent
        .load_session(&request.session_id)
        .await
        .unwrap()
        .unwrap();
    let calls_before = fixture.probe.provider_calls.load(Ordering::SeqCst);
    let completed = events(agent.resume_stream(session)).await;
    assert!(!completed.iter().any(|event| matches!(
        event,
        AgentEvent::Error { .. } | AgentEvent::NeedClarification { .. }
    )));
    let calls = fixture.probe.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "Bash");
    assert_eq!(calls[0].arguments, json!({"command": "current-command"}));
    assert_eq!(
        calls[0].generation.as_deref(),
        Some(request.request_generation.as_str())
    );
    assert!(calls[0].supervisor_absent);
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > calls_before);
}

#[tokio::test]
async fn typed_alias_exact_custom_owner_cannot_borrow_bash_approval() {
    let (fixture, mut pending, request) = Box::pin(alias_pending_fixture()).await;
    let shadow = fixture.registered_agent(alias_config(), &["Bash", "execute_command"]);
    let before = fixture.probe.provider_calls.load(Ordering::SeqCst);
    assert!(Box::pin(shadow.resume(&mut pending)).await.is_err());
    let mut approved = fixture.approve(&request).await;
    assert!(Box::pin(shadow.resume(&mut approved)).await.is_err());
    assert!(fixture.probe.calls.lock().unwrap().is_empty());
    assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), before);

    // A fresh operation actually resolved to the exact custom registration
    // receives that custom tool's own typed request and can resume normally.
    *fixture.probe.next_call.lock().unwrap() = Some(alias_call());
    let initial =
        events(shadow.run_stream(Session::new("custom-control", "test-model"), "operate")).await;
    let pending = shadow
        .load_session("custom-control")
        .await
        .unwrap()
        .unwrap();
    assert!(
        pending.pending_question.is_some(),
        "custom tool must request its own permission: {initial:?}; calls={:?}",
        fixture.probe.calls.lock().unwrap()
    );
    assert_eq!(
        pending.pending_question.as_ref().unwrap().tool_name,
        "execute_command"
    );
    let request = current_request(&pending);
    assert_eq!(request.tool_name, "execute_command");
    let approved = fixture.approve(&request).await;
    let completed = events(shadow.resume_stream(approved)).await;
    assert!(!completed.iter().any(|event| matches!(
        event,
        AgentEvent::Error { .. } | AgentEvent::NeedClarification { .. }
    )));
    let calls = fixture.probe.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "execute_command");
    assert_eq!(
        calls[0].generation.as_deref(),
        Some(request.request_generation.as_str())
    );
}

#[tokio::test]
async fn second_context_stays_waiting_across_unanswered_public_entries_and_restart() {
    let mut fixture = Fixture::new().await;
    let (mut session, request_a) = parked("multiple-contexts", true);
    let old = result_message(&session, "result-old");
    fixture.repo.save(&mut session).await.unwrap();
    let session = fixture.approve(&request_a).await;
    let agent = fixture.agent(Some(Arc::new(PermissionConfig::new())), true, true, None);
    let first_events = events(agent.resume_stream(session)).await;
    drop(agent);
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert!(!fixture.directory.path().join("actions.log").exists());
    assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
    assert!(first_events.iter().any(|event| matches!(event, AgentEvent::NeedClarification { tool_call_id: Some(id), .. } if id == CALL)));
    assert!(!first_events.iter().any(|event| matches!(
        event,
        AgentEvent::Error { .. } | AgentEvent::Complete { .. }
    )));
    let pending = fixture.reload(&request_a.session_id).await;
    let saved = replay_state(&pending);
    assert!(pending.pending_question.is_some());
    assert!(saved["call"].is_null() && saved["generation"].is_null());
    let result = result_message(&pending, "result-current");
    assert_eq!(
        result["metadata"]["permission_request"]["request_generation"],
        SECOND
    );
    assert_eq!(
        result["metadata"]["permission.replay_approvals.v1"][0]["request"]["request_generation"],
        CURRENT
    );
    assert!(result["metadata"]
        .get("permission_decision_receipt")
        .is_none());
    assert_eq!(result_message(&pending, "result-old"), old);

    // Box each test phase to bound the orchestration frame. Calls within each
    // phase remain direct and consecutive, with no spawn or scheduler yield.
    Box::pin(assert_wait_releases_direct_ownership(&fixture, &pending)).await;
    Box::pin(assert_contradictory_wait_is_rejected(&fixture, &pending)).await;
    Box::pin(assert_unanswered_public_entries_stay_waiting(
        &fixture, &pending,
    ))
    .await;
    Box::pin(approve_second_context_and_resume(&mut fixture, &pending)).await;
}

async fn assert_wait_releases_direct_ownership(fixture: &Fixture, pending: &Session) {
    let saved = replay_state(pending);
    std::fs::write(fixture.directory.path().join("config.json"), r#"{
        "provider": "anthropic", "providers": { "anthropic": { "api_key": "test-key", "model": "test-model" } }
    }"#).unwrap();
    let delivery_builder = AgentBuilder::new()
        .model("test-model")
        .provider(fixture.probe.clone())
        .session_delivery(SessionActivationRouter::new());
    let delivery = Box::pin(
        delivery_builder.with_defaults_for_data_dir(fixture.directory.path().to_path_buf()),
    )
    .await
    .unwrap()
    .build()
    .unwrap();
    let mut repeated = delivery.load_session(&pending.id).await.unwrap().unwrap();
    assert_eq!(replay_state(&repeated), saved);
    Box::pin(delivery.resume(&mut repeated)).await.unwrap();
    // No yield or retry: a handled wait must release its logical execution
    // ownership before returning to the caller.
    Box::pin(delivery.run_session(&mut repeated)).await.unwrap();
    assert_eq!(replay_state(&repeated), saved);
    let mut orphaned = repeated.clone();
    orphaned.metadata.insert(
        PERMISSION_REEXECUTE_GENERATION_METADATA_KEY.into(),
        CURRENT.into(),
    );
    assert!(Box::pin(delivery.resume(&mut orphaned)).await.is_err());
    Box::pin(delivery.resume(&mut repeated)).await.unwrap();
    assert_eq!(replay_state(&repeated), saved);
}

async fn assert_contradictory_wait_is_rejected(fixture: &Fixture, pending: &Session) {
    for case in [
        "old-markers",
        "conflicting-request",
        "malformed-payload-request",
        "unexpected-receipt",
    ] {
        let mut contradictory = pending.clone();
        let message = contradictory
            .messages
            .iter_mut()
            .find(|message| message.id == "result-current")
            .unwrap();
        let mut payload: Value = serde_json::from_str(&message.content).unwrap();
        match case {
            "old-markers" => {
                contradictory
                    .metadata
                    .insert(PERMISSION_REEXECUTE_METADATA_KEY.into(), CALL.into());
                contradictory.metadata.insert(
                    PERMISSION_REEXECUTE_GENERATION_METADATA_KEY.into(),
                    CURRENT.into(),
                );
            }
            "conflicting-request" => {
                payload["permission_request"]["resource"] = json!("different-resource")
            }
            "malformed-payload-request" => payload["permission_request"] = Value::Null,
            "unexpected-receipt" => payload["permission_decision_receipt"] = Value::Null,
            _ => unreachable!(),
        }
        message.content = payload.to_string();
        let before = replay_state(&contradictory);
        let fresh = fixture.agent(Some(Arc::new(PermissionConfig::new())), true, true, None);
        assert!(
            Box::pin(fresh.resume(&mut contradictory)).await.is_err(),
            "must reject {case}"
        );
        assert_eq!(replay_state(&contradictory), before);
        assert_eq!(fixture.probe.calls.lock().unwrap().len(), 1);
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
    }
}

async fn assert_unanswered_public_entries_stay_waiting(fixture: &Fixture, pending: &Session) {
    let saved = replay_state(pending);
    for entry in ["resume", "run_session", "continue", "Approve", "stream"] {
        let mut reloaded = fixture.reload(&pending.id).await;
        let messages_before = serde_json::to_value(&reloaded.messages).unwrap();
        let fresh = fixture.agent(Some(Arc::new(PermissionConfig::new())), true, true, None);
        match entry {
            "resume" => Box::pin(fresh.resume(&mut reloaded)).await.unwrap(),
            "run_session" => Box::pin(fresh.run_session(&mut reloaded)).await.unwrap(),
            "stream" => {
                let waiting_events = events(fresh.run_stream(reloaded.clone(), "Approve")).await;
                assert_eq!(waiting_events.iter().filter(|event| matches!(event, AgentEvent::NeedClarification {
                    tool_call_id: Some(id), question, ..
                } if id == CALL && question == &pending.pending_question.as_ref().unwrap().question)).count(), 1);
                assert!(!waiting_events.iter().any(|event| matches!(
                    event,
                    AgentEvent::ToolComplete { .. }
                        | AgentEvent::Error { .. }
                        | AgentEvent::ToolLifecycle { .. }
                        | AgentEvent::Complete { .. }
                )));
            }
            input => Box::pin(fresh.run(&mut reloaded, input)).await.unwrap(),
        }
        assert_eq!(replay_state(&reloaded), saved, "unanswered {entry}");
        if matches!(entry, "continue" | "Approve") {
            let last = reloaded.messages.pop().unwrap();
            assert!(matches!(last.role, Role::User));
            assert_eq!(last.content, entry);
        }
        assert_eq!(
            serde_json::to_value(&reloaded.messages).unwrap(),
            messages_before
        );
        assert_eq!(fixture.probe.calls.lock().unwrap().len(), 1);
        assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(replay_state(&fixture.reload(&pending.id).await), saved);
    }
}

async fn approve_second_context_and_resume(fixture: &mut Fixture, pending: &Session) {
    let result = result_message(pending, "result-current");
    let request_b: PermissionRequest =
        serde_json::from_value(result["metadata"]["permission_request"].clone()).unwrap();
    let approved = fixture.approve(&request_b).await;
    let old_store = fixture.store.clone();
    let old_persistence = fixture.persistence.clone();
    fixture.reopen_store().await;
    assert!(!Arc::ptr_eq(&old_store, &fixture.store));
    assert!(!Arc::ptr_eq(&old_persistence, &fixture.persistence));
    drop((old_store, old_persistence));
    let final_config = Arc::new(PermissionConfig::new());
    let fresh = fixture.agent(Some(final_config.clone()), true, true, None);
    let loaded = fresh.load_session(&pending.id).await.unwrap().unwrap();
    let mut session: Session =
        serde_json::from_slice(&serde_json::to_vec(&loaded).unwrap()).unwrap();
    assert_eq!(replay_state(&session), replay_state(&approved));
    Box::pin(fresh.resume(&mut session)).await.unwrap();
    let calls = fixture.probe.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].generation.as_deref(), Some(CURRENT));
    assert_eq!(calls[1].generation.as_deref(), Some(SECOND));
    assert!(calls
        .iter()
        .all(|call| call.supervisor_absent
            && call.arguments == json!({"command": "current-command"})));
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read_to_string(fixture.directory.path().join("actions.log")).unwrap(),
        "physical action\n"
    );
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        result_message(&session, "result-old"),
        result_message(pending, "result-old")
    );
    for (kind, resource) in [
        (PermissionType::ExecuteCommand, "current-command"),
        (PermissionType::WriteFile, "second-resource"),
    ] {
        assert!(!final_config.consume_once_for_generation(
            &session.id,
            CALL,
            SECOND,
            kind,
            resource
        ));
    }
}

#[tokio::test]
async fn invalid_typed_replay_is_rejected_before_executor_or_provider() {
    for case in [
        "orphan",
        "empty-marker",
        "unknown",
        "old-still-present",
        "missing-marker",
        "missing-receipt",
        "wrong-session",
        "wrong-decision-generation",
        "missing-config",
        "malformed-request",
        "blank-request-generation",
        "receipt-without-request",
        "wrong-tool",
        "denied-receipt",
        "non-tool-result",
    ] {
        let fixture = Fixture::new().await;
        let (mut session, request) = parked(&format!("invalid-{case}"), true);
        fixture.repo.save(&mut session).await.unwrap();
        let mut session = fixture.approve(&request).await;
        let result = session
            .messages
            .iter_mut()
            .find(|message| message.id == "result-current")
            .unwrap();
        let metadata = result.metadata.as_mut().unwrap();
        match case {
            "orphan" => {
                session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
            }
            "empty-marker" | "unknown" | "old-still-present" => {
                session.metadata.insert(
                    PERMISSION_REEXECUTE_GENERATION_METADATA_KEY.into(),
                    match case {
                        "empty-marker" => "",
                        "unknown" => "unknown",
                        _ => "generation-old",
                    }
                    .into(),
                );
            }
            "missing-marker" => {
                session
                    .metadata
                    .remove(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);
            }
            "missing-receipt" => {
                metadata
                    .as_object_mut()
                    .unwrap()
                    .remove("permission_decision_receipt");
            }
            "wrong-session" => {
                metadata["permission_decision_receipt"]["session_id"] = json!("another-session")
            }
            "wrong-decision-generation" => {
                metadata["permission_decision_receipt"]["decision"]["request_generation"] =
                    json!("another-generation")
            }
            "malformed-request" => metadata["permission_request"] = Value::Null,
            "blank-request-generation" => {
                metadata["permission_request"]["request_generation"] = json!(" ")
            }
            "receipt-without-request" => {
                metadata
                    .as_object_mut()
                    .unwrap()
                    .remove("permission_request");
            }
            "wrong-tool" => metadata["permission_request"]["tool_name"] = json!("Write"),
            "denied-receipt" => {
                metadata["permission_decision_receipt"]["decision"]["decision"] =
                    json!(PermissionDecisionKind::DenyOnce)
            }
            "non-tool-result" => result.role = Role::User,
            "missing-config" => {}
            _ => unreachable!(),
        }
        fixture.repo.save(&mut session).await.unwrap();
        let before = replay_state(&session);
        let config = (case != "missing-config").then(|| Arc::new(PermissionConfig::new()));
        let agent = fixture.agent(config, true, false, None);
        assert!(
            agent.resume(&mut session).await.is_err(),
            "must reject {case}"
        );
        assert_eq!(replay_state(&session), before, "preserve rejected {case}");
        assert_eq!(
            fixture.probe.calls.lock().unwrap().len(),
            0,
            "executor {case}"
        );
        assert_eq!(
            fixture.probe.provider_calls.load(Ordering::SeqCst),
            0,
            "provider {case}"
        );
        assert!(!fixture.directory.path().join("actions.log").exists());
    }
}

#[tokio::test]
async fn typed_text_answer_cannot_downgrade_to_legacy_replay() {
    let fixture = Fixture::new().await;
    let (mut session, _) = parked("typed-text-answer", true);
    fixture.repo.save(&mut session).await.unwrap();
    let agent = fixture.agent(Some(Arc::new(PermissionConfig::new())), true, false, None);
    let answered = agent.answer(&session.id, "Approve").await.unwrap();
    assert!(answered.session.pending_question.is_none());
    assert!(!answered.permission_grants.is_empty());
    assert!(answered
        .session
        .metadata
        .get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY)
        .is_none());
    let result = result_message(&answered.session, "result-current");
    assert!(result["metadata"]["permission_request"].is_object());
    assert!(result["metadata"]
        .get("permission_decision_receipt")
        .is_none());
    let before = replay_state(&answered.session);
    let rejected = events(agent.resume_stream(answered.session)).await;
    assert_eq!(
        rejected
            .iter()
            .filter(|event| matches!(event, AgentEvent::Error { .. }))
            .count(),
        1
    );
    assert!(!rejected.iter().any(|event| matches!(
        event,
        AgentEvent::ToolLifecycle { .. }
            | AgentEvent::ToolComplete { .. }
            | AgentEvent::NeedClarification { .. }
            | AgentEvent::Complete { .. }
    )));
    assert_eq!(fixture.probe.calls.lock().unwrap().len(), 0);
    assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_state(&fixture.reload(&session.id).await), before);
}

#[tokio::test]
async fn admitted_tool_errors_are_saved_before_normal_model_recovery() {
    for typed in [false, true] {
        let fixture = Fixture::new().await;
        let (mut session, request) = parked("execution-error", typed);
        fixture.repo.save(&mut session).await.unwrap();
        let agent = fixture.agent(Some(Arc::new(PermissionConfig::new())), typed, false, None);
        let session = if typed {
            fixture.approve(&request).await
        } else {
            agent.answer(&session.id, "Approve").await.unwrap().session
        };
        let old = result_message(&session, "result-old");
        fixture.probe.fail_execution.store(true, Ordering::SeqCst);
        let completed = events(agent.resume_stream(session)).await;
        assert_eq!(completed.iter().filter(|event| matches!(event, AgentEvent::ToolLifecycle { phase, .. } if phase == "error")).count(), 1);
        assert!(!completed.iter().any(|event| matches!(
            event,
            AgentEvent::Error { .. } | AgentEvent::ToolComplete { .. }
        )));
        assert_eq!(fixture.probe.calls.lock().unwrap().len(), 1);
        assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
        assert!(!fixture.directory.path().join("actions.log").exists());
        let saved = fixture.reload(&request.session_id).await;
        let result = result_message(&saved, "result-current");
        assert_eq!(result["tool_success"], false);
        assert!(result["content"]
            .as_str()
            .unwrap()
            .contains("intentional execution failure"));
        assert_eq!(result_message(&saved, "result-old"), old);
        assert!(!saved
            .metadata
            .contains_key(PERMISSION_REEXECUTE_METADATA_KEY));
        assert!(!saved
            .metadata
            .contains_key(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY));
    }
}

#[tokio::test]
async fn legacy_pending_question_does_not_inherit_an_old_typed_wait() {
    let fixture = Fixture::new().await;
    let (mut session, _) = parked("ordinary-pending", false);
    let old = session
        .messages
        .iter_mut()
        .find(|message| message.id == "result-old")
        .unwrap();
    old.metadata = Some(
        json!({"permission_request": request(&session.id, "old-typed", "old-command", PermissionType::ExecuteCommand)}),
    );
    session.pending_question.as_mut().unwrap().source = PendingQuestionSource::AgenticClarification;
    fixture.repo.save(&mut session).await.unwrap();
    let agent = fixture.agent(None, false, false, None);
    Box::pin(agent.run_session(&mut session)).await.unwrap();
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
    assert!(fixture.probe.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn latest_plan_blocks_current_occurrence_before_public_provider_continuation() {
    let fixture = Fixture::new().await;
    let (mut session, request) = parked("typed-plan", true);
    fixture.repo.save(&mut session).await.unwrap();
    let mut stale = fixture.approve(&request).await;
    let old = result_message(&stale, "result-old");
    stale
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
    let mut latest = fixture.reload(&stale.id).await;
    latest.agent_runtime_state.as_mut().unwrap().plan_mode = Some(serde_json::from_value(json!({
        "entered_at": "2026-07-31T00:00:00Z", "pre_permission_mode": "default", "status": "exploring",
    })).unwrap());
    fixture.repo.save(&mut latest).await.unwrap();
    let agent = fixture.agent(Some(Arc::new(PermissionConfig::new())), true, false, None);
    let completed = events(agent.resume_stream(stale)).await;
    assert!(!completed.iter().any(|event| matches!(
        event,
        AgentEvent::ToolLifecycle { .. }
            | AgentEvent::ToolComplete { .. }
            | AgentEvent::Error { .. }
    )));
    assert_eq!(fixture.probe.calls.lock().unwrap().len(), 0);
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
    let saved = fixture.reload(&session.id).await;
    assert!(!saved
        .metadata
        .contains_key(PERMISSION_REEXECUTE_METADATA_KEY));
    assert!(!saved
        .metadata
        .contains_key(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY));
    assert_eq!(result_message(&saved, "result-old"), old);
    let result = result_message(&saved, "result-current");
    assert_eq!(result["tool_success"], false);
    assert!(result["content"]
        .as_str()
        .unwrap()
        .contains("Plan mode blocked"));
}

struct FailReplaySave {
    inner: Arc<LockedSessionStore>,
    attempts: AtomicUsize,
}

#[async_trait]
impl RuntimeSessionPersistence for FailReplaySave {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        let result = result_message(session, "result-current");
        let content = result["content"].as_str().unwrap();
        if content.starts_with("REAL OUTPUT")
            || content.starts_with("Plan mode blocked")
            || result["metadata"]["permission_request"]["request_generation"] == SECOND
        {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(std::io::Error::other("injected replay persistence failure"));
        }
        self.inner.save_runtime_session(session).await
    }

    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
}

#[tokio::test]
async fn output_repark_and_plan_save_failures_stop_provider_and_success_events() {
    for (scenario, stream) in [
        ("output", false),
        ("output", true),
        ("repark", true),
        ("plan", false),
        ("plan", true),
    ] {
        let second_context = scenario == "repark";
        let plan = scenario == "plan";
        let fixture = Fixture::new().await;
        let (mut session, request) = parked("save-failure", true);
        fixture.repo.save(&mut session).await.unwrap();
        let mut session = fixture.approve(&request).await;
        if plan {
            session
                .agent_runtime_state
                .as_mut()
                .unwrap()
                .set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
            let mut latest = fixture.reload(&request.session_id).await;
            latest.agent_runtime_state.as_mut().unwrap().plan_mode = Some(
                serde_json::from_value(json!({
                    "entered_at": "2026-07-31T00:00:00Z",
                    "pre_permission_mode": "default", "status": "exploring",
                }))
                .unwrap(),
            );
            fixture.repo.save(&mut latest).await.unwrap();
        }
        let durable_before = fixture.reload(&request.session_id).await;
        let before = replay_state(&durable_before);
        let runtime_before = serde_json::to_value(&durable_before.agent_runtime_state).unwrap();
        let persistence = Arc::new(FailReplaySave {
            inner: fixture.persistence.clone(),
            attempts: AtomicUsize::new(0),
        });
        let agent = fixture.agent(
            Some(Arc::new(PermissionConfig::new())),
            true,
            second_context,
            Some(persistence.clone()),
        );
        if stream {
            let failed = events(agent.resume_stream(session)).await;
            assert_eq!(
                failed
                    .iter()
                    .filter(|event| matches!(event, AgentEvent::Error { .. }))
                    .count(),
                1
            );
            assert!(failed.iter().any(|event| matches!(event, AgentEvent::Error { message } if message.contains("injected replay persistence failure"))));
            assert_eq!(failed.iter().filter(
                |event| matches!(event, AgentEvent::ToolLifecycle { phase, .. } if phase == "error")
            ).count(), usize::from(!plan));
            if plan {
                assert!(!failed
                    .iter()
                    .any(|event| matches!(event, AgentEvent::ToolLifecycle { .. })));
            }
            assert!(!failed.iter().any(|event| matches!(
                event,
                AgentEvent::ToolComplete { .. }
                    | AgentEvent::NeedClarification { .. }
                    | AgentEvent::Complete { .. }
            )));
            assert!(!failed.iter().any(|event| matches!(event, AgentEvent::ToolLifecycle { phase, .. } if phase == "finished")));
        } else {
            let error = agent.resume(&mut session).await.unwrap_err();
            assert!(error
                .to_string()
                .contains("injected replay persistence failure"));
        }
        assert_eq!(persistence.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.probe.calls.lock().unwrap().len(),
            usize::from(!plan)
        );
        assert_eq!(
            fixture.probe.actions.load(Ordering::SeqCst),
            usize::from(!second_context && !plan)
        );
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        let durable_after = fixture.reload(&request.session_id).await;
        assert_eq!(replay_state(&durable_after), before);
        assert_eq!(
            serde_json::to_value(&durable_after.agent_runtime_state).unwrap(),
            runtime_before
        );
    }
}
