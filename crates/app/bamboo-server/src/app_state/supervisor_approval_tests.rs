//! Real Server/Connect dispatch and HTTP decision coverage for host authority.

use super::*;
use actix_web::{test, web, App};
use bamboo_agent_core::tools::{
    ExecutingSupervisorObservation, FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome,
    ToolResult, ToolSchema,
};
use bamboo_agent_core::{Message, Session};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_llm::{Config, LLMProvider, ProviderModelRouter, ProviderRegistry};
use bamboo_tools::permission::{PermissionDecision, PermissionDecisionKind, PermissionRequest};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) const CALL: &str = "transport-call";
const TARGET: &str = "transport-target";
const AUTHORITY: &str = ExecutingSupervisorObservation::PERMISSION_REPLAY_METADATA_KEY;

#[derive(Default)]
struct Probe {
    next_call: AtomicBool,
    shell_call: AtomicBool,
    shell_entries: AtomicUsize,
    actions: AtomicUsize,
    provider_calls: AtomicUsize,
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
        let output = if self.next_call.swap(false, Ordering::SeqCst) {
            bamboo_llm::LLMChunk::ToolCalls(vec![ToolCall {
                id: CALL.into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: if self.shell_call.load(Ordering::SeqCst) {
                        "default::exec_command"
                    } else {
                        "default::session_control"
                    }
                    .into(),
                    arguments: if self.shell_call.load(Ordering::SeqCst) {
                        json!({"cmd":"current-command"})
                    } else {
                        json!({"action":"followup", "target_session_id":TARGET,
                            "operation_id":"transport-operation", "message":"continue the authorized work"})
                    }
                    .to_string(),
                },
            }])
        } else {
            bamboo_llm::LLMChunk::Token("done".into())
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(output),
            Ok(bamboo_llm::LLMChunk::Done),
        ])))
    }
}

struct ProtectedTool {
    service: SupervisorSessionService,
    probe: Arc<Probe>,
}

#[async_trait]
impl Tool for ProtectedTool {
    fn name(&self) -> &str {
        "session_control"
    }
    fn description(&self) -> &str {
        "Exercise strict Supervisor management after the real permission gate"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"action":{"type":"string"},"target_session_id":{"type":"string"},"operation_id":{"type":"string"},"message":{"type":"string"}},"required":["action","target_session_id","operation_id","message"]})
    }
    async fn invoke(&self, _args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        assert!(!ctx.bypass_permissions && !ctx.auto_approve_permissions && !ctx.plan_read_only);
        let original = ctx
            .executing_supervisor_for(ctx.session_id.as_deref().unwrap())
            .ok_or_else(|| {
                ToolError::Execution("original Supervisor observation missing".into())
            })?;
        // As in the SDK fixture, this strict durable link write probes the
        // transported identity. It is not the production followup operation.
        self.service
            .attach(&original.supervisor_reference(), 1, TARGET)
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        self.probe.actions.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            "strict write saved",
        )))
    }
}

struct ShellProbeTool(Arc<Probe>);

#[async_trait]
impl Tool for ShellProbeTool {
    fn name(&self) -> &str {
        "exec_command"
    }
    fn description(&self) -> &str {
        "Record entry into the shell fixture without executing a command"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]})
    }
    async fn invoke(&self, _args: Value, _ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        self.0.shell_entries.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            "shell probe entered",
        )))
    }
}

pub(crate) struct Fixture {
    directory: tempfile::TempDir,
    pub(crate) state: web::Data<AppState>,
    pub(crate) original: bamboo_domain::SupervisorReference,
    session_id: String,
    ordinary_root: bool,
    replay_generation: Mutex<Option<String>>,
    corrupted_authority: Mutex<Option<Value>>,
    probe: Arc<Probe>,
}

impl Fixture {
    async fn open(path: &std::path::Path, probe: Arc<Probe>) -> web::Data<AppState> {
        let mut config = Config::from_data_dir(Some(path.to_path_buf()));
        config.provider = "test-provider".into();
        let mut state = AppState::new_with_provider(path.to_path_buf(), config, probe.clone())
            .await
            .unwrap();
        state.provider_registry = Arc::new(ProviderRegistry::new(
            HashMap::from([(
                "test-provider".into(),
                probe.clone() as Arc<dyn LLMProvider>,
            )]),
            "test-provider".into(),
        ));
        state.provider_router = Arc::new(ProviderModelRouter::new(state.provider_registry.clone()));
        state
            .permission_checker
            .permission_config()
            .unwrap()
            .set_ask_rules([
                "session_control".into(),
                "default::session_control".into(),
                "Bash(current-command)".into(),
            ]);
        let registry = bamboo_tools::ToolRegistry::new();
        registry
            .register(ProtectedTool {
                service: SupervisorSessionService::new(state.session_store.clone()),
                probe: probe.clone(),
            })
            .unwrap();
        registry.register(ShellProbeTool(probe)).unwrap();
        let executor = Arc::new(
            bamboo_tools::executor::BuiltinToolExecutor::with_registry_and_permissions(
                registry,
                state.permission_checker.clone(),
            ),
        );
        state.tool_factory =
            crate::tools::ToolSurfaceFactory::new(executor.clone(), executor.clone(), executor);
        web::Data::new(state)
    }

    pub(crate) fn pending() -> Pin<Box<impl Future<Output = Self>>> {
        Box::pin(Self::pending_inner(false))
    }

    pub(crate) fn ordinary_root_pending() -> Pin<Box<impl Future<Output = Self>>> {
        Box::pin(Self::pending_inner(true))
    }

    async fn unparked(ordinary_root: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::default());
        let state = Self::open(directory.path(), probe.clone()).await;
        let service = SupervisorSessionService::new(state.session_store.clone());
        let original: bamboo_domain::SupervisorReference =
            (&service.get_or_create_default("test-model").await.unwrap()).into();
        let mut target = Session::new(TARGET, "target-model");
        target.set_project_id_meta("transport-project");
        target.add_message(Message::user("independent target history"));
        state.storage.save_session(&target).await.unwrap();
        service
            .configure_project_scope(&original, 0, ["transport-project".parse().unwrap()].into())
            .await
            .unwrap();
        let session_id = if ordinary_root {
            let root = Session::new("ordinary-transport-root", "test-model");
            assert!(root.supervisor_incarnation_id().is_none());
            state.storage.save_session(&root).await.unwrap();
            root.id
        } else {
            original.session_id.clone()
        };
        probe.shell_call.store(ordinary_root, Ordering::SeqCst);
        Self {
            directory,
            state,
            original,
            session_id,
            ordinary_root,
            replay_generation: Mutex::new(None),
            corrupted_authority: Mutex::new(None),
            probe,
        }
    }

    async fn pending_inner(ordinary_root: bool) -> Self {
        let mut fixture = Self::unparked(ordinary_root).await;
        fixture.park().await;
        // Fresh stores, cache and permission configuration cannot inherit an
        // in-memory pending request, decision receipt or AllowOnce grant.
        fixture.state.shutdown().await;
        fixture.state = Self::open(fixture.directory.path(), fixture.probe.clone()).await;
        fixture
    }

    pub(crate) async fn prepare_workspace_catalog(&self) {
        async fn bytes_if_present(path: &std::path::Path) -> Option<Vec<u8>> {
            match tokio::fs::read(path).await {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("failed to read {}: {error}", path.display()),
            }
        }
        let session = self.reload().await;
        assert!(session.pending_question.is_some());
        assert_eq!(self.probe.actions.load(Ordering::SeqCst), 0);
        assert_eq!(self.probe.provider_calls.load(Ordering::SeqCst), 1);
        let workspace = session.workspace_path_meta().unwrap();
        let session_dir = self.directory.path().join("sessions").join(&session.id);
        let paths = [
            session_dir.join("session.json"),
            session_dir.join("runtime.json"),
            self.directory.path().join("permissions.json"),
        ];
        let mut before = Vec::new();
        for path in &paths {
            before.push(bytes_if_present(path).await);
        }
        assert!(before[0].is_some() && before[1].is_some());
        // Native catalog watcher registration can be slow under concurrent
        // test load. Prepare this fresh instance before timing approval replay.
        self.state
            .skill_manager
            .store_for_workspace(Some(std::path::Path::new(&workspace)))
            .await
            .unwrap();
        for (path, bytes) in paths.iter().zip(before) {
            assert_eq!(
                bytes_if_present(path).await,
                bytes,
                "catalog preparation changed {}",
                path.display()
            );
        }
        assert_eq!(self.probe.actions.load(Ordering::SeqCst), 0);
        assert_eq!(self.probe.provider_calls.load(Ordering::SeqCst), 1);
    }

    fn park(&self) -> Pin<Box<impl Future<Output = ()> + '_>> {
        Box::pin(self.park_inner())
    }

    async fn park_inner(&self) {
        assert!(self.run_call().await);
        let pending = self.reload().await;
        assert!(pending.pending_question.is_some());
        let result = pending
            .messages
            .iter()
            .rev()
            .find(|m| m.tool_call_id.as_deref() == Some(CALL))
            .unwrap();
        if self.ordinary_root {
            assert!(result
                .metadata
                .as_ref()
                .is_none_or(|metadata| metadata.get(AUTHORITY).is_none()));
            let payload: Value = serde_json::from_str(&result.content).unwrap();
            assert!(payload.get(AUTHORITY).is_none());
        } else {
            let authority = &result.metadata.as_ref().unwrap()[AUTHORITY];
            assert_eq!(
                authority["incarnation_id"],
                self.original.incarnation_id.to_string()
            );
            assert_eq!(authority["result_message_id"], result.id);
        }
        assert_eq!(self.probe.actions.load(Ordering::SeqCst), 0);
        assert_eq!(self.probe.shell_entries.load(Ordering::SeqCst), 0);
    }

    async fn run_call(&self) -> bool {
        self.probe.next_call.store(true, Ordering::SeqCst);
        let mut session = self.reload().await;
        let workspace = self.directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        session.set_workspace_path_meta(workspace.to_string_lossy());
        if self.ordinary_root {
            return self.run_root_call(session).await;
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let drain = tokio::spawn(async move {
            let mut parked = false;
            while let Some(event) = rx.recv().await {
                parked |= matches!(event, AgentEvent::NeedClarification { .. });
            }
            parked
        });
        self.state
            .agent
            .execute_direct(
                &mut session,
                bamboo_engine::ExecuteRequestBuilder::new(
                    "attach the permitted Root",
                    tx,
                    tokio_util::sync::CancellationToken::new(),
                )
                .tools(self.state.tools_for(crate::tools::ToolSurface::Root))
                .provider_override(self.probe.clone())
                .model("test-model")
                .build(),
            )
            .await
            .unwrap();
        drain.await.unwrap()
    }

    async fn run_root_call(&self, mut session: Session) -> bool {
        use crate::handlers::agent::execute::runtime::{
            spawn_agent_execution, spawn_event_forwarder_with_root_actor, SpawnAgentExecution,
        };
        use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};

        // Use the existing hosted Root path: the real reservation, Actor
        // writer and history handoff must all exist before the provider runs.
        session.add_message(Message::user("execute the permitted command"));
        self.state
            .agent
            .prepare_external_session_for_execution(&mut session)
            .await
            .unwrap();
        self.state.storage.save_session(&session).await.unwrap();
        let sender = self.state.get_session_event_sender(&self.session_id).await;
        let mut events = sender.subscribe();
        let mut reservation = match reserve_session_execution(
            &self.state.agent,
            &self.state.agent_runners,
            &self.state.session_event_senders,
            &self.session_id,
            &sender,
        )
        .await
        {
            SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
            SessionExecutionReserveOutcome::AlreadyRunning { run_id } => {
                panic!("fresh ordinary Root already has a run: {run_id}")
            }
        };
        reservation
            .bind_root_actor(&self.state.agent, &session)
            .await
            .unwrap();
        let writer = reservation
            .root_actor_writer()
            .expect("ordinary Root requires its real Actor writer");
        assert!(reservation.execution_persistence().is_some());
        let (mpsc_tx, mpsc_rx) = tokio::sync::mpsc::channel(100);
        let history_commit_barrier = spawn_event_forwarder_with_root_actor(
            self.state.clone(),
            self.session_id.clone(),
            reservation.run_id().to_string(),
            mpsc_rx,
            sender,
            None,
            Some(writer),
        );
        spawn_agent_execution(SpawnAgentExecution {
            state: self.state.clone(),
            session_id: self.session_id.clone(),
            session,
            execution_reservation: reservation,
            is_child_session: false,
            provider_name: "test-provider".into(),
            provider_override: Some(self.probe.clone()),
            model_roster: bamboo_engine::ModelRoster {
                model: Some("test-model".into()),
                ..Default::default()
            },
            reasoning_effort: None,
            reasoning_effort_source: "test".into(),
            disabled_tools: Default::default(),
            disabled_skill_ids: Default::default(),
            mpsc_tx,
            history_commit_barrier,
            image_fallback: None,
            gold_config: None,
            app_data_dir: Some(self.state.app_data_dir.clone()),
            run_budget: None,
        });
        let parked = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let mut parked = false;
            loop {
                match events.recv().await.unwrap() {
                    AgentEvent::NeedClarification { .. } => parked = true,
                    AgentEvent::SessionHistoryCommitted { session_id } => {
                        assert_eq!(session_id, self.session_id);
                        break parked;
                    }
                    AgentEvent::Error { message } => {
                        panic!("hosted Root fixture failed: {message}")
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("hosted Root must commit its pending approval history");
        self.wait_for_idle().await;
        parked
    }

    pub(crate) async fn reload(&self) -> Session {
        self.state
            .storage
            .load_session(&self.session_id)
            .await
            .unwrap()
            .unwrap()
    }

    pub(crate) async fn decision(&self) -> PermissionDecision {
        let session = self.reload().await;
        let result = session
            .messages
            .iter()
            .rev()
            .find(|m| m.tool_call_id.as_deref() == Some(CALL))
            .unwrap();
        // Match the native HTTP persisted_permission_request reader. The real
        // Root Actor persists the waiting payload in content; a later typed
        // response retains it in metadata before replacing that content.
        let request = result
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("permission_request"))
            .cloned()
            .or_else(|| {
                serde_json::from_str::<Value>(&result.content)
                    .ok()?
                    .get("permission_request")
                    .cloned()
            })
            .expect("the real parked call must retain its native permission request");
        let request: PermissionRequest = serde_json::from_value(request).unwrap();
        assert_eq!(request.session_id, self.session_id);
        assert_eq!(request.request_id, CALL);
        assert!(!request.request_generation.trim().is_empty());
        PermissionDecision {
            request_id: CALL.into(),
            request_generation: request.request_generation,
            decision: PermissionDecisionKind::AllowOnce,
            matcher_id: None,
            expected_policy_revision: Some(request.policy_revision),
            confirm_global: false,
        }
    }

    pub(crate) async fn corrupt(&self) {
        let mut session = self.reload().await;
        let result = session
            .messages
            .iter_mut()
            .rev()
            .find(|m| m.tool_call_id.as_deref() == Some(CALL))
            .unwrap();
        result.metadata.as_mut().unwrap()[AUTHORITY]["version"] = json!(99);
        *self.corrupted_authority.lock().unwrap() =
            Some(result.metadata.as_ref().unwrap()[AUTHORITY].clone());
        self.state.session_repo.save(&mut session).await.unwrap();
    }

    pub(crate) async fn formal_approve(&self) {
        let decision = self.decision().await;
        *self.replay_generation.lock().unwrap() = Some(decision.request_generation.clone());
        let guard =
            bamboo_engine::session_app::respond::acquire_pending_response_guard(&self.session_id)
                .await;
        bamboo_engine::session_app::respond::submit_pending_permission_response_checked_guarded(
            &self.state.session_repo,
            bamboo_engine::session_app::types::RespondInput {
                session_id: self.session_id.clone(),
                user_response: "Approve".into(),
                model: None,
                model_ref: None,
                provider: None,
                reasoning_effort: None,
            },
            Some(CALL.into()),
            bamboo_tools::permission::PermissionDecisionReceipt {
                session_id: self.session_id.clone(),
                decision,
                decided_at: std::time::SystemTime::now().into(),
            },
            &guard,
        )
        .await
        .unwrap();
    }

    async fn wait_for_idle(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let active = self
                    .state
                    .agent_runners
                    .read()
                    .await
                    .get(&self.session_id)
                    .is_some_and(|runner| {
                        matches!(
                            runner.status,
                            crate::app_state::AgentStatus::Pending
                                | crate::app_state::AgentStatus::Running
                        )
                    });
                if !active
                    && self
                        .state
                        .session_activation_router
                        .current_run_id(&self.session_id)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("adapter must finish or retire its reserved execution");
    }

    pub(crate) async fn settled(&self, writes: usize, replay_retained: bool) {
        self.wait_for_idle().await;
        self.state.shutdown().await;
        assert_eq!(self.probe.actions.load(Ordering::SeqCst), writes);
        assert_eq!(self.probe.shell_entries.load(Ordering::SeqCst), 0);
        let scope = SupervisorSessionService::new(self.state.session_store.clone())
            .inspect_scope(&self.original)
            .await
            .unwrap();
        assert_eq!(scope.state_revision, 1 + writes as u64);
        let session = self.reload().await;
        if writes == 1 {
            assert_eq!(session.last_run_status().as_deref(), Some("completed"));
            let last = session.messages.last().unwrap();
            assert!(matches!(last.role, bamboo_agent_core::Role::Assistant));
            assert_eq!(last.content, "done");
        }
        assert_eq!(
            session
                .metadata
                .contains_key(PERMISSION_REEXECUTE_METADATA_KEY),
            replay_retained
        );
        assert!(session.pending_question.is_none());
        if replay_retained {
            assert_eq!(
                session
                    .metadata
                    .get(PERMISSION_REEXECUTE_METADATA_KEY)
                    .map(String::as_str),
                Some(CALL)
            );
            assert_eq!(session.metadata.get(
                bamboo_engine::session_app::respond::PERMISSION_REEXECUTE_GENERATION_METADATA_KEY,
            ), self.replay_generation.lock().unwrap().as_ref());
        } else {
            assert!(!session.metadata.contains_key(
                bamboo_engine::session_app::respond::PERMISSION_REEXECUTE_GENERATION_METADATA_KEY,
            ));
        }
        if let Some(expected) = self.corrupted_authority.lock().unwrap().as_ref() {
            let result = session
                .messages
                .iter()
                .rev()
                .find(|message| message.tool_call_id.as_deref() == Some(CALL))
                .unwrap();
            assert_eq!(
                result.metadata.as_ref().unwrap().get(AUTHORITY),
                Some(expected)
            );
        }
        if self.ordinary_root {
            assert!(session.supervisor_incarnation_id().is_none());
            let decision = self.decision().await;
            assert!(!self
                .state
                .permission_checker
                .permission_config()
                .unwrap()
                .consume_once_for_generation(
                    &self.session_id,
                    CALL,
                    &decision.request_generation,
                    bamboo_tools::permission::PermissionType::ExecuteCommand,
                    "current-command"
                ));
            let supervisor = self
                .state
                .storage
                .load_session(&self.original.session_id)
                .await
                .unwrap()
                .unwrap();
            assert!(!supervisor.root_orchestration_only_enabled());
            assert_eq!(
                supervisor.supervisor_incarnation_id(),
                Some(self.original.incarnation_id)
            );
        }
        let target = self
            .state
            .storage
            .load_session(TARGET)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.messages.len(), 1);
        assert_eq!(target.messages[0].content, "independent target history");
    }
}

#[actix_web::test]
async fn server_supervisor_shell_never_enters_approval_or_protected_write_in_either_mode() {
    let body = std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            actix_web::rt::System::new().block_on(async {
                for ultra in [false, true] {
                    let fixture = Fixture::unparked(false).await;
                    let mut supervisor = fixture.reload().await;
                    supervisor.set_root_orchestration_only(ultra).unwrap();
                    fixture
                        .state
                        .storage
                        .save_session(&supervisor)
                        .await
                        .unwrap();
                    fixture.probe.shell_call.store(true, Ordering::SeqCst);
                    assert!(!fixture.run_call().await);
                    let saved = fixture.reload().await;
                    assert!(saved.pending_question.is_none());
                    let result = saved
                        .messages
                        .iter()
                        .rev()
                        .find(|message| message.tool_call_id.as_deref() == Some(CALL))
                        .unwrap();
                    assert_eq!(result.tool_success, Some(false));
                    assert!(result
                        .metadata
                        .as_ref()
                        .is_none_or(|metadata| metadata.get(AUTHORITY).is_none()));
                    fixture.settled(0, false).await;
                }
            });
        })
        .expect("spawn test thread");
    body.join().expect("test thread completed");
}

#[actix_web::test]
async fn server_native_typed_supervisor_approval_survives_policy_refresh_and_rejects_corruption() {
    // The permission replay fixture drives deeply nested async state machines
    // that overflow the default 2 MiB libtest worker stack on Linux CI (fine
    // on macOS's 8 MiB main thread). Run the body on a dedicated thread with
    // an 8 MiB stack, in its own actix System like #[actix_web::test] does,
    // so the test is platform-stable.
    let body = std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            actix_web::rt::System::new()
                .block_on(server_native_typed_supervisor_approval_fixture_replay_body());
        })
        .expect("spawn test thread");
    body.join().expect("test thread completed");
}

async fn server_native_typed_supervisor_approval_fixture_replay_body() {
    for corrupt in [false, true] {
        let fixture = Box::pin(Fixture::pending()).await;
        if corrupt {
            fixture.corrupt().await;
        }
        let mut decision = fixture.decision().await;
        let app = test::init_service(App::new().app_data(fixture.state.clone()).route(
            "/sessions/{session_id}/permission-decisions",
            web::post().to(crate::handlers::agent::respond::submit_permission_decision),
        ))
        .await;
        let uri = format!(
            "/sessions/{}/permission-decisions",
            fixture.original.session_id
        );
        let mut policy = fixture
            .state
            .permission_section
            .snapshot()
            .data
            .as_ref()
            .clone();
        policy.ask_rules = vec!["session_control".into(), "default::session_control".into()];
        fixture.state.permission_section.commit(0, policy).unwrap();
        let policy = fixture.state.permission_section.snapshot();
        let permissions = fixture
            .state
            .permission_checker
            .permission_config()
            .unwrap();
        permissions.publish_persistent_policy(policy.revision, policy.data.as_ref());
        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&uri)
                .set_json(&decision)
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);
        let refreshed = permissions
            .pending_request(&fixture.original.session_id, CALL)
            .unwrap();
        assert_eq!(refreshed.policy_revision, 1);
        assert_eq!(refreshed.request_generation, decision.request_generation);
        assert_eq!(fixture.decision().await.expected_policy_revision, Some(0));
        decision.expected_policy_revision = Some(refreshed.policy_revision);
        *fixture.replay_generation.lock().unwrap() = Some(decision.request_generation.clone());
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&uri)
                .set_json(&decision)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["replayed"], false);
        assert_eq!(body["auto_resume_status"], "started");
        assert_eq!(body["resume"]["auto_resume_status"], "started");
        assert_eq!(
            body["receipt"]["decision"]["request_generation"],
            decision.request_generation
        );
        // The HTTP reply only confirms that auto-resume started. A concurrent
        // Supervisor save may briefly hold a Prepared proof, which readers
        // correctly reject until the final runtime/proof commit completes.
        fixture.settled(usize::from(!corrupt), corrupt).await;
        let completed = fixture.reload().await;
        let result = completed
            .messages
            .iter()
            .rev()
            .find(|m| m.tool_call_id.as_deref() == Some(CALL))
            .unwrap();
        assert_eq!(
            result.metadata.as_ref().unwrap()["permission_decision_receipt"]["decision"]
                ["request_generation"],
            decision.request_generation
        );
        assert_eq!(
            result.metadata.as_ref().unwrap()["permission_request"]["policy_revision"],
            0
        );
        assert_eq!(
            result.metadata.as_ref().unwrap()["permission_decision_receipt"]["decision"]
                ["expected_policy_revision"],
            1
        );
    }
}

#[actix_web::test]
async fn server_resume_consumes_approved_call_after_root_tool_tightening() {
    let body = std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            actix_web::rt::System::new().block_on(async {
                let fixture = Box::pin(Fixture::ordinary_root_pending()).await;
                fixture.prepare_workspace_catalog().await;
                fixture.formal_approve().await;
                let mut selected = fixture.reload().await;
                selected.set_root_orchestration_only(true).unwrap();
                fixture.state.storage.save_session(&selected).await.unwrap();
                let session = fixture.reload().await;
                let config = bamboo_engine::session_app::resolution::resolve_resume_config_snapshot(
                    &*fixture.state.config.read().await,
                    &fixture.state.provider_registry,
                    &session,
                    None,
                );
                let outcome = bamboo_engine::session_app::resume::resume_session_execution(
                    &AppStateResumeRef(fixture.state.clone()),
                    &session.id,
                    config,
                )
                .await;
                assert!(matches!(
                    outcome,
                    bamboo_engine::session_app::types::ResumeOutcome::Started { .. }
                ));
                fixture.settled(0, false).await;
                let saved = fixture.reload().await;
                let result = saved
                    .messages
                    .iter()
                    .rev()
                    .find(|message| message.tool_call_id.as_deref() == Some(CALL))
                    .unwrap();
                assert_eq!(result.tool_success, Some(false));
                assert!(result.content.contains("Root orchestration policy blocked"));
            });
        })
        .expect("spawn test thread");
    body.join().expect("test thread completed");
}

#[actix_web::test]
async fn server_old_decision_cannot_answer_a_recreated_supervisor() {
    // Same 2 MiB libtest worker stack concern as the fixture-replay test
    // above; run on a dedicated 8 MiB-stack thread.
    let body = std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            actix_web::rt::System::new()
                .block_on(server_old_decision_cannot_answer_a_recreated_supervisor_body());
        })
        .expect("spawn test thread");
    body.join().expect("test thread completed");
}

async fn server_old_decision_cannot_answer_a_recreated_supervisor_body() {
    let mut fixture = Box::pin(Fixture::pending()).await;
    let old_decision = fixture.decision().await;
    let old = fixture.original.clone();
    assert!(fixture
        .state
        .storage
        .delete_session(&old.session_id)
        .await
        .unwrap());
    let service = SupervisorSessionService::new(fixture.state.session_store.clone());
    fixture.original = (&service.get_or_create_default("replacement").await.unwrap()).into();
    assert_ne!(fixture.original.incarnation_id, old.incarnation_id);
    service
        .configure_project_scope(
            &fixture.original,
            0,
            ["transport-project".parse().unwrap()].into(),
        )
        .await
        .unwrap();
    Box::pin(fixture.park()).await;
    let replacement_pending = fixture.reload().await;
    assert_ne!(
        fixture.decision().await.request_generation,
        old_decision.request_generation
    );
    let app = test::init_service(App::new().app_data(fixture.state.clone()).route(
        "/sessions/{session_id}/permission-decisions",
        web::post().to(crate::handlers::agent::respond::submit_permission_decision),
    ))
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri(&format!(
                "/sessions/{}/permission-decisions",
                old.session_id
            ))
            .set_json(&old_decision)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
    assert_eq!(
        serde_json::to_value(fixture.reload().await).unwrap(),
        serde_json::to_value(replacement_pending).unwrap()
    );
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert_eq!(
        service
            .inspect_scope(&fixture.original)
            .await
            .unwrap()
            .state_revision,
        1
    );
    fixture.state.shutdown().await;
}
