use super::*;
use actix_web::{http::StatusCode, test, web, App};
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext, ToolExecutor};
use bamboo_agent_core::Session;
use bamboo_server_tools::project_tools::WorkspaceScopeGuard;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Semaphore;

struct NoProvider(Arc<AtomicUsize>);

#[async_trait]
impl bamboo_llm::LLMProvider for NoProvider {
    async fn chat_stream(
        &self,
        _messages: &[bamboo_agent_core::Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("scope mutation fixtures must not invoke a model")
    }
}

struct ScopeWriterFixture {
    state: web::Data<crate::app_state::AppState>,
    workspace_tools: Arc<dyn ToolExecutor>,
    session: Session,
    workspace: PathBuf,
    next_workspace: PathBuf,
    release: Arc<Semaphore>,
    run_id: Option<String>,
    provider_calls: Arc<AtomicUsize>,
    _home: tempfile::TempDir,
    _workspace: tempfile::TempDir,
    _next_workspace: tempfile::TempDir,
}

impl ScopeWriterFixture {
    async fn new(blocking_run: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let physical_home = home.path().canonicalize().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let next_workspace = tempfile::tempdir().unwrap();
        let workspace_path = workspace.path().canonicalize().unwrap();
        let next_path = next_workspace.path().canonicalize().unwrap();
        let bundle = physical_home.join("skills/scope-writer-fence");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(
            bundle.join("SKILL.md"),
            "---\nname: scope-writer-fence\ndescription: Scope writer fixture\n---\nRead only.\n",
        )
        .unwrap();
        std::fs::write(bundle.join("workflow.yaml"), "workflow_schema: 1\nid: scope-writer-fence\nrevision: 1\nsteps:\n  - id: inspect\n    type: tool\n    tool: Read\n    args: {}\n    capabilities: [read]\nplan: {type: step, step: inspect}\n").unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let mut state = crate::app_state::AppState::new_with_provider(
            physical_home.clone(),
            bamboo_llm::Config::default(),
            Arc::new(NoProvider(provider_calls.clone())),
        )
        .await
        .unwrap();
        let project = state
            .project_store
            .create_with_project_path(
                "Scope writer Project",
                None,
                workspace_path.to_string_lossy(),
                vec![bamboo_domain::WorkspaceBinding {
                    path: next_path.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let mut session = Session::new(
            format!("workflow-scope-writer-{}", uuid::Uuid::new_v4()),
            "test-model",
        );
        session.set_project_id_meta(project.id.to_string());
        session.set_workspace_path_meta(workspace_path.to_string_lossy());
        session.metadata.insert(
            bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
            "explicit".to_string(),
        );
        state.storage.save_session(&session).await.unwrap();
        state.workspace_resolver.publish_resolved_workspace(
            &session.id,
            workspace_path.clone(),
            "scope_writer_fixture",
        );
        let release = Arc::new(Semaphore::new(0));
        let entered = Arc::new(Semaphore::new(0));
        if blocking_run {
            state.workflow_runs = WorkflowRunAccess::new(
                &physical_home,
                Arc::new(super::tests::BlockingWorkflowReadTool {
                    entered: entered.clone(),
                    release: Some(release.clone()),
                }),
                state.skill_manager.clone(),
                state.session_repo.clone(),
            )
            .await
            .unwrap()
            .with_project_scope(state.project_store.clone(), state.workspace_resolver.clone());
        }
        let root_tools = state.tools_for(bamboo_server_tools::ToolSurface::Root);
        let root_tools = root_tools.for_session(&session).unwrap_or(root_tools);
        let workspace_tools = if blocking_run {
            // This fixture replaces the Workflow engine with a controlled real
            // Tool. Bind the actual Workspace implementation to that same owner;
            // retain the assembled Root executor's original permission gates.
            let guard = Arc::new(WorkflowWorkspaceScopeGuard::default());
            guard.bind(&state.workflow_runs).unwrap();
            Arc::new(bamboo_server_tools::OverlayToolExecutor::new(
                root_tools,
                Arc::new(
                    bamboo_server_tools::ProjectWorkspaceTool::new_with_workspace_resolver(
                        state.session_repo.clone(),
                        state.project_store.clone(),
                        state.workspace_resolver.clone(),
                    )
                    .with_scope_guard(guard),
                ),
            )) as Arc<dyn ToolExecutor>
        } else {
            // Exercise the production builder's original Workspace overlay.
            root_tools
        };
        let run_id = if blocking_run {
            let started = state
                .workflow_runs
                .start(&session.id, "scope-writer-fence", 1, json!({}), None)
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            assert!(state.workflow_runs.scope_change_is_blocked(&session.id));
            Some(started.run_id)
        } else {
            None
        };
        Self {
            state: web::Data::new(state),
            workspace_tools,
            session,
            workspace: workspace_path,
            next_workspace: next_path,
            release,
            run_id,
            provider_calls,
            _home: home,
            _workspace: workspace,
            _next_workspace: next_workspace,
        }
    }

    async fn durable(&self) -> Session {
        self.state
            .storage
            .load_session(&self.session.id)
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_scope_unchanged(&self, before: &Session) {
        assert_eq!(
            serde_json::to_value(self.durable().await).unwrap(),
            serde_json::to_value(before).unwrap(),
            "a rejected request must not save a speculative Session"
        );
        let cached = self
            .state
            .session_repo
            .try_load(&self.session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached.project_id_meta(), before.project_id_meta());
        assert_eq!(cached.workspace_path_meta(), before.workspace_path_meta());
        assert_eq!(cached.metadata_version, before.metadata_version);
        assert_eq!(
            bamboo_agent_core::workspace_state::peek_workspace(&self.session.id),
            Some(self.workspace.clone())
        );
        assert_eq!(self.provider_calls.load(Ordering::SeqCst), 0);
    }

    async fn finish_and_drain(&self) {
        self.release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while self.state.workflow_runs.scope_change_is_blocked(&self.session.id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the controlled physical Tool must finish before scope changes");
    }

    async fn workspace_call(&self, path: Option<&Path>) -> ToolResult {
        let call = ToolCall {
            id: format!("scope-workspace-{}", uuid::Uuid::new_v4()),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Workspace".to_string(),
                arguments: path.map_or_else(|| json!({}), |path| json!({"path":path})).to_string(),
            },
        };
        let mut context = ToolExecutionContext::none(&call.id);
        context.session_id = Some(&self.session.id);
        assert!(!context.bypass_permissions && !context.auto_approve_permissions);
        self.workspace_tools
            .execute_with_context(&call, context)
            .await
            .expect("the ordinary authorized Workspace call must reach its scope check")
    }
}

#[actix_web::test]
async fn workflow_chat_rejects_scope_change_until_physical_drain() {
    let fixture = ScopeWriterFixture::new(true).await;
    let app = test::init_service(
        App::new()
            .app_data(fixture.state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    assert!(fixture.state.agent_runners.read().await.get(&fixture.session.id).is_none());
    let plain = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(json!({"session_id":fixture.session.id,"model":"test-model","message":"Same-scope follow-up"}))
            .to_request(),
    )
    .await;
    let status = plain.status();
    let bytes = test::read_body(plain).await;
    if status != StatusCode::CREATED {
        fixture.finish_and_drain().await;
    }
    assert_eq!(status, StatusCode::CREATED, "{}", String::from_utf8_lossy(&bytes));
    for draining in [false, true] {
        if draining {
            fixture.state.workflow_runs.cancel_for_session(
                &fixture.session.id,
                fixture.run_id.as_deref().unwrap(),
            ).await.unwrap();
        }
        assert!(fixture.state.workflow_runs.scope_change_is_blocked(&fixture.session.id));
        let before = fixture.durable().await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(json!({"session_id":fixture.session.id,"model":"test-model","message":"Change workspace","workspace_path":fixture.next_workspace}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let bytes = test::read_body(response).await;
        if status != StatusCode::CONFLICT {
            fixture.finish_and_drain().await;
        }
        assert_eq!(status, StatusCode::CONFLICT, "draining={draining}: {}", String::from_utf8_lossy(&bytes));
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "session_project_running_conflict");
        fixture.assert_scope_unchanged(&before).await;
    }
    fixture.finish_and_drain().await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(json!({"session_id":fixture.session.id,"model":"test-model","message":"Change after drain","workspace_path":fixture.next_workspace}))
            .to_request(),
    )
    .await;
    let status = response.status();
    let bytes = test::read_body(response).await;
    assert_eq!(status, StatusCode::CREATED, "{}", String::from_utf8_lossy(&bytes));
    assert_eq!(fixture.durable().await.workspace_path_meta(), Some(fixture.next_workspace.to_string_lossy().into_owned()));
    assert_eq!(fixture.provider_calls.load(Ordering::SeqCst), 0);
}

#[actix_web::test]
async fn workflow_workspace_tool_rejects_scope_change_until_physical_drain() {
    let fixture = ScopeWriterFixture::new(true).await;
    for draining in [false, true] {
        if draining {
            fixture.state.workflow_runs.cancel_for_session(
                &fixture.session.id,
                fixture.run_id.as_deref().unwrap(),
            ).await.unwrap();
        }
        assert!(fixture.state.workflow_runs.scope_change_is_blocked(&fixture.session.id));
        let before = fixture.durable().await;
        let get = fixture.workspace_call(None).await;
        assert!(get.success, "{}", get.result);
        let result = fixture.workspace_call(Some(&fixture.next_workspace)).await;
        if result.success {
            fixture.finish_and_drain().await;
        }
        assert!(!result.success, "draining={draining}: {}", result.result);
        let body: Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(body["code"], "session_project_running_conflict");
        fixture.assert_scope_unchanged(&before).await;
    }
    fixture.finish_and_drain().await;
    let result = fixture.workspace_call(Some(&fixture.next_workspace)).await;
    assert!(result.success, "{}", result.result);
    assert_eq!(fixture.durable().await.workspace_path_meta(), Some(fixture.next_workspace.to_string_lossy().into_owned()));
    assert_eq!(fixture.provider_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn workspace_guard_is_fail_closed_and_retains_no_workflow_owner() {
    let guard = WorkflowWorkspaceScopeGuard::default();
    assert!(guard.scope_change_is_blocked("idle").is_err());
    let (access, _, _directory) = super::tests::workflow_test_access().await;
    let engine = Arc::downgrade(&access.engine);
    let admissions = Arc::downgrade(&access.scope_admissions);
    let owners = Arc::strong_count(&access.engine);
    guard.bind(&access).unwrap();
    assert_eq!(Arc::strong_count(&access.engine), owners);
    assert!(guard.bind(&access).is_err(), "an installed owner cannot be rebound");
    assert!(!guard.scope_change_is_blocked("idle").unwrap());
    drop(access);
    assert!(engine.upgrade().is_none());
    assert!(admissions.upgrade().is_none());
    assert!(guard.scope_change_is_blocked("idle").is_err());
}

#[actix_web::test]
async fn production_workspace_overlay_observes_owned_workflow_admission() {
    let fixture = ScopeWriterFixture::new(false).await;
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let access = fixture.state.workflow_runs.clone();
    let id = fixture.session.id.clone();
    let worker_entered = entered.clone();
    let worker_release = release.clone();
    let request = tokio::spawn(async move {
        access.with_scope_admission(&id, move |_| async move {
            worker_entered.add_permits(1);
            let _permit = worker_release.acquire().await.unwrap();
            Err(WorkflowRunError::Preflight("controlled rejection".to_string()))
        }).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await.unwrap().unwrap().forget();
    let before = fixture.durable().await;
    assert!(fixture.state.workflow_runs.scope_change_is_blocked(&fixture.session.id));
    let result = fixture.workspace_call(Some(&fixture.next_workspace)).await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(fixture.state.workflow_runs.scope_change_is_blocked(&fixture.session.id));
    release.add_permits(1);
    fixture.finish_and_drain().await;
    assert!(!result.success, "{}", result.result);
    let body: Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(body["code"], "session_project_running_conflict");
    fixture.assert_scope_unchanged(&before).await;
    let result = fixture.workspace_call(Some(&fixture.next_workspace)).await;
    assert!(result.success, "{}", result.result);
    assert_eq!(fixture.provider_calls.load(Ordering::SeqCst), 0);
}
