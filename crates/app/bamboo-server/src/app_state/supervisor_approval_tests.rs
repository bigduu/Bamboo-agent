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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) const CALL: &str = "transport-call";
const TARGET: &str = "transport-target";
const AUTHORITY: &str = ExecutingSupervisorObservation::PERMISSION_REPLAY_METADATA_KEY;

#[derive(Default)]
struct Probe {
    next_call: AtomicBool,
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
                    name: "execute_command".into(),
                    arguments: json!({"command":"current-command"}).to_string(),
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
        "Bash"
    }
    fn description(&self) -> &str {
        "Exercise a strict Supervisor write after approval"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"]})
    }
    async fn invoke(&self, _args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        assert!(!ctx.bypass_permissions && !ctx.auto_approve_permissions && !ctx.plan_read_only);
        let original = ctx
            .executing_supervisor_for(ctx.session_id.as_deref().unwrap())
            .ok_or_else(|| {
                ToolError::Execution("original Supervisor observation missing".into())
            })?;
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

pub(crate) struct Fixture {
    directory: tempfile::TempDir,
    pub(crate) state: web::Data<AppState>,
    pub(crate) original: bamboo_domain::SupervisorReference,
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
            .set_ask_rules(["Bash(current-command)".into()]);
        let registry = bamboo_tools::ToolRegistry::new();
        registry
            .register(ProtectedTool {
                service: SupervisorSessionService::new(state.session_store.clone()),
                probe,
            })
            .unwrap();
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

    pub(crate) async fn pending() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::default());
        let state = Self::open(directory.path(), probe.clone()).await;
        let service = SupervisorSessionService::new(state.session_store.clone());
        let original = (&service.get_or_create_default("test-model").await.unwrap()).into();
        let mut target = Session::new(TARGET, "target-model");
        target.set_project_id_meta("transport-project");
        target.add_message(Message::user("independent target history"));
        state.storage.save_session(&target).await.unwrap();
        service
            .configure_project_scope(&original, 0, ["transport-project".parse().unwrap()].into())
            .await
            .unwrap();
        let mut fixture = Self {
            directory: directory,
            state,
            original,
            probe,
        };
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

    async fn park(&self) {
        self.probe.next_call.store(true, Ordering::SeqCst);
        let mut session = self.reload().await;
        let workspace = self.directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        session.set_workspace_path_meta(workspace.to_string_lossy());
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
        assert!(drain.await.unwrap());
        let pending = self.reload().await;
        assert!(pending.pending_question.is_some());
        let result = pending
            .messages
            .iter()
            .rev()
            .find(|m| m.tool_call_id.as_deref() == Some(CALL))
            .unwrap();
        let authority = &result.metadata.as_ref().unwrap()[AUTHORITY];
        assert_eq!(
            authority["incarnation_id"],
            self.original.incarnation_id.to_string()
        );
        assert_eq!(authority["result_message_id"], result.id);
        assert_eq!(self.probe.actions.load(Ordering::SeqCst), 0);
    }

    pub(crate) async fn reload(&self) -> Session {
        self.state
            .storage
            .load_session(&self.original.session_id)
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
        let request: PermissionRequest =
            serde_json::from_value(result.metadata.as_ref().unwrap()["permission_request"].clone())
                .unwrap();
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
        self.state.session_repo.save(&mut session).await.unwrap();
    }

    pub(crate) async fn formal_approve(&self) {
        let decision = self.decision().await;
        let guard = bamboo_engine::session_app::respond::acquire_pending_response_guard(
            &self.original.session_id,
        )
        .await;
        bamboo_engine::session_app::respond::submit_pending_permission_response_checked_guarded(
            &self.state.session_repo,
            bamboo_engine::session_app::types::RespondInput {
                session_id: self.original.session_id.clone(),
                user_response: "Approve".into(),
                model: None,
                model_ref: None,
                provider: None,
                reasoning_effort: None,
            },
            Some(CALL.into()),
            bamboo_tools::permission::PermissionDecisionReceipt {
                session_id: self.original.session_id.clone(),
                decision,
                decided_at: std::time::SystemTime::now().into(),
            },
            &guard,
        )
        .await
        .unwrap();
    }

    pub(crate) async fn settled(&self, writes: usize, replay_retained: bool) {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let active = self
                    .state
                    .agent_runners
                    .read()
                    .await
                    .get(&self.original.session_id)
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
                        .current_run_id(&self.original.session_id)
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
        self.state.shutdown().await;
        assert_eq!(self.probe.actions.load(Ordering::SeqCst), writes);
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
async fn server_native_typed_supervisor_approval_survives_policy_refresh_and_rejects_corruption() {
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
        policy.ask_rules = vec!["Bash(current-command)".into()];
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
        fixture.settled(usize::from(!corrupt), corrupt).await;
    }
}

#[actix_web::test]
async fn server_old_decision_cannot_answer_a_recreated_supervisor() {
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
