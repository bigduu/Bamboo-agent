//! Host-wired model entry: an independent Root is not a SubAgent child.

use std::{sync::Arc, time::Duration};

use actix_web::{http::StatusCode, test, web, App};

use bamboo_agent_core::tools::{
    with_executing_session_caller, ExecutingSupervisorObservation, FunctionCall, ToolCall,
    ToolError, ToolExecutionContext, ToolExecutionSessionFlags, ToolOutcome,
};
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryPort, RootModeOperationAction, RootModeOperationRequest,
    RootThinkingMode, SessionCallerReference, SessionInboxPort, SessionKind, SessionMessageId,
    SessionPermissionMode, SupervisorReference,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};
use serde_json::{json, Value};
use tokio::{sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;

use super::AppState;

struct Fixture {
    home: tempfile::TempDir,
    state: web::Data<AppState>,
    supervisor: SupervisorReference,
    project_id: bamboo_domain::ProjectId,
    workspace: String,
}

impl Fixture {
    async fn new(ultra: bool) -> Self {
        Self::with_provider(
            ultra,
            Config::default(),
            Arc::new(super::UnconfiguredProvider {
                message: "no external model is available in this fixture".into(),
            }),
        )
        .await
    }

    async fn with_provider(ultra: bool, config: Config, provider: Arc<dyn LLMProvider>) -> Self {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            Box::pin(AppState::new_with_provider(
                home.path().to_path_buf(),
                config,
                provider,
            ))
            .await
            .unwrap(),
        );
        let workspace = home.path().join("workspaces").join("explicit-worker");
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = workspace
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let project = state
            .project_store
            .create_with_project_path("Explicit worker Project", None, &workspace, Vec::new())
            .unwrap();
        let service = SupervisorSessionService::new(state.storage.clone());
        let original = service
            .get_or_create_default("supervisor-private-model")
            .await
            .unwrap();
        let supervisor = SupervisorReference::from(&original);
        // The real HTTP read exposes the default domain without a privileged
        // authorizer or a scope PUT. Per-operation tool approval below remains
        // independent; no Project ACL is injected into this fixture.
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/supervisor/default/scope")
                .peer_addr("127.0.0.1:12345".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let receipt: Value = test::read_body_json(response).await;
        assert_eq!(receipt["scope"]["state_revision"], 0);
        assert_eq!(receipt["management_access"]["default_all_projects"], true);
        assert_eq!(
            receipt["management_access"]["requires_project_authorization"],
            false
        );
        drop(app);
        let original = state
            .storage
            .load_root_authority(&supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        state
            .storage
            .root_mode_operation(&RootModeOperationRequest {
                session_id: original.id.clone(),
                operation_id: format!(
                    "{}:{}",
                    original.root_mode_transition_epoch,
                    uuid::Uuid::new_v4()
                ),
                birth_token: original.root_mode_birth_token(),
                expected_epoch: original.root_mode_transition_epoch,
                requested_enabled: ultra,
                action: RootModeOperationAction::Select,
            })
            .await
            .unwrap();
        let original = state
            .storage
            .load_root_authority(&supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.root_orchestration_only_enabled(), ultra);
        // An exact native session grant for the fixture operation. No bypass,
        // auto-approve, identity override, or permissions on a worker are used.
        state
            .permission_checker
            .permission_config()
            .unwrap()
            .grant_typed_scoped_session_permission(
                &supervisor.session_id,
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "explicit-worker-creation".into(),
                    kind: PermissionMatcherKind::ExactResource,
                    value: format!("session_control create {} {workspace}", project.id),
                },
            )
            .unwrap();
        Self {
            home,
            state,
            supervisor,
            project_id: project.id,
            workspace,
        }
    }

    fn request(&self, operation_id: uuid::Uuid) -> Value {
        json!({"action":"create", "request": {
            "operation_id": operation_id,
            "expected_state_revision": 0,
            "project_id": self.project_id,
            "workspace_path": self.workspace,
            "title": "Independent implementation task",
            "message": "Inspect the explicitly assigned workspace and report evidence."
        }})
    }

    async fn invoke(&self, args: Value) -> Value {
        self.invoke_named("session_control", args).await
    }

    async fn invoke_named(&self, name: &str, args: Value) -> Value {
        let result = self
            .dispatch_named(name, args, true)
            .await
            .expect("the Host must expose the independent Root creation operation");
        let ToolOutcome::Completed(result) = result else {
            panic!("expected bounded creation/delivery receipt");
        };
        assert!(result.success, "{}", result.result);
        serde_json::from_str(&result.result).unwrap()
    }

    async fn set_supervisor_permission_mode(
        &self,
        mode: SessionPermissionMode,
        auto_dispatch: bool,
    ) {
        let supervisor = self
            .state
            .storage
            .load_root_authority(&self.supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(self.state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let mut payload = json!({"permission_mode": mode});
        if auto_dispatch {
            payload["supervisor_auto_dispatch"] = json!(true);
        }
        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{}", supervisor.id))
                .peer_addr("127.0.0.1:12345".parse().unwrap())
                .insert_header((
                    actix_web::http::header::IF_MATCH,
                    format!("\"{}\"", supervisor.metadata_version),
                ))
                .set_json(payload)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response
            .headers()
            .get(actix_web::http::header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .trim_matches('"')
            .parse::<u64>()
            .unwrap();
        let result: Value = test::read_body_json(response).await;
        assert_eq!(result["session"]["permission_mode"], json!(mode));
        assert_eq!(result["session"]["supervisor_auto_dispatch"], auto_dispatch);
        assert_eq!(
            self.state
                .session_store
                .get_index_entry(&self.supervisor.session_id)
                .await
                .unwrap()
                .supervisor_profile_revision,
            Some(etag)
        );
    }

    async fn dispatch_named(
        &self,
        name: &str,
        args: Value,
        observe_original_caller: bool,
    ) -> Result<ToolOutcome, ToolError> {
        let executing = self
            .state
            .storage
            .load_root_authority(&self.supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        let call = ToolCall {
            id: "supervised-root-create".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: args.to_string(),
            },
        };
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&executing.id);
        ctx.root_session_id = Some(&executing.id);
        let flags = ToolExecutionSessionFlags::from_session(&executing);
        ctx.bypass_permissions = flags.bypass_permissions;
        ctx.auto_approve_permissions = flags.auto_approve_permissions;
        ctx.plan_read_only = flags.plan_read_only;
        let ctx = ctx.with_executing_supervisor(
            ExecutingSupervisorObservation::capture_from_executing_session(&executing),
        );
        let tools = self.state.tools_for(crate::tools::ToolSurface::Root);
        // The model loop binds this immutable registration contract immediately
        // after canonical Root authority admission; mirror that real boundary.
        let tools = tools.for_session(&executing).unwrap_or(tools);
        // Real dispatch captures this original caller before invoking a tool;
        // a Supervisor observation alone cannot supply Root caller identity.
        let caller =
            observe_original_caller.then(|| Arc::new(SessionCallerReference::from(&executing)));
        with_executing_session_caller(caller, tools.execute_with_context_outcome(&call, ctx)).await
    }
}

async fn assert_create_is_independent_and_exact_retry(ultra: bool) {
    let f = Fixture::new(ultra).await;
    let before = f
        .state
        .storage
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    let scope = f.invoke(json!({"action":"scope"})).await;
    assert_eq!(scope["state_revision"], 0);
    assert_eq!(scope["allowed_projects"], json!([]));
    assert_eq!(scope["requires_project_authorization"], false);
    assert!(scope["projects"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["id"].as_str() == Some(f.project_id.as_str())));
    assert_eq!(scope["project_scope_configured"], true);
    let operation_id = uuid::Uuid::new_v4();
    let response = f.invoke(f.request(operation_id)).await;
    assert_eq!(response["action"], "create");
    assert_eq!(response["admitted"], true);
    let id = response["target_session_id"].as_str().unwrap();
    let root = f.state.storage.load_session(id).await.unwrap().unwrap();
    assert_eq!(root.kind, SessionKind::Root);
    assert_eq!(root.root_session_id, id);
    assert!(root.parent_session_id.is_none());
    assert!(root.authority_identity.is_ordinary());
    assert_eq!(
        root.project_id_meta().as_deref(),
        Some(f.project_id.as_str())
    );
    assert_eq!(
        root.workspace_path_meta().as_deref(),
        Some(f.workspace.as_str())
    );
    assert_eq!(root.supervisor_incarnation_id(), None);
    assert!(!root.root_orchestration_only_enabled());
    assert_ne!(root.model, "supervisor-private-model");
    let link = f
        .state
        .storage
        .inspect_supervisor_link(&f.supervisor, id)
        .await
        .unwrap();
    assert!(link.authorized);
    assert!(
        link.link.is_none(),
        "default management must not populate a legacy attachment"
    );
    let replay = f.invoke(f.request(operation_id)).await;
    assert_eq!(replay["target_session_id"], id);
    assert_eq!(replay["receipt_id"], response["receipt_id"]);
    assert_eq!(replay["generation"], response["generation"]);
    assert_eq!(
        f.state.session_inbox.inspect(id).await.unwrap().generation,
        1
    );
    let after = f
        .state
        .storage
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(after.authority_identity, before.authority_identity);
    assert_eq!(
        serde_json::to_value(&after.messages).unwrap(),
        serde_json::to_value(&before.messages).unwrap()
    );
    assert_eq!(after.model, before.model);
    assert_eq!(after.root_orchestration_only_enabled(), ultra);
    assert!(f.home.path().is_dir());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_standard_creates_independent_root_and_retries_once() {
    assert_create_is_independent_and_exact_retry(false).await;
}

#[actix_web::test]
async fn supervisor_ultra_creates_independent_root_and_retries_once() {
    assert_create_is_independent_and_exact_retry(true).await;
}

const PERMISSION_PROBES: [(&str, &str); 4] = [
    ("shell-first", "echo first-worker-check"),
    ("shell-second", "echo second-worker-check"),
    ("shell-third", "echo third-worker-check"),
    ("shell-denied", "echo denied-worker-check"),
];
const SHELL_PROBES_DONE: &str = "Worker permission checks finished.";

#[actix_web::test]
async fn supervisor_dispatch_reservation_orders_with_consent_revocation() {
    let f = Fixture::new(false).await;
    f.set_supervisor_permission_mode(SessionPermissionMode::Auto, true)
        .await;
    let store = f.state.session_store.clone();
    let current = f
        .state
        .storage
        .load_execution_admission_snapshot(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    let caller = SessionCallerReference::from(&current);
    let reserving_caller = caller.clone();
    let (entered, held) = tokio::sync::oneshot::channel();
    let (release, proceed) = tokio::sync::oneshot::channel();
    let reservation = tokio::spawn(async move {
        store
            .with_supervisor_dispatch_consent(&reserving_caller, |allowed| async move {
                entered.send(allowed).unwrap();
                proceed.await.unwrap();
                Ok(())
            })
            .await
            .unwrap()
    });
    assert!(held.await.unwrap());
    let revoke = f
        .state
        .session_store
        .update_supervisor_execution_profile(&current, |session| {
            session.set_supervisor_auto_dispatch(false);
            session.metadata_version += 1;
            Ok::<_, std::io::Error>(())
        });
    tokio::pin!(revoke);
    assert!(timeout(Duration::from_millis(100), &mut revoke)
        .await
        .is_err());
    release.send(()).unwrap();
    reservation.await.unwrap();
    revoke.await.unwrap().unwrap();
    let mut stale_workspace_update = current.clone();
    stale_workspace_update.metadata_version += 1;
    stale_workspace_update.set_workspace_path_meta(&f.workspace);
    let stale_error = f
        .state
        .storage
        .save_session(&stale_workspace_update)
        .await
        .unwrap_err();
    assert!(stale_error
        .to_string()
        .contains("Supervisor dispatch consent"));
    assert!(!f
        .state
        .session_store
        .with_supervisor_dispatch_consent(&caller, |allowed| async move { Ok(allowed) })
        .await
        .unwrap());
    f.state.shutdown().await;
}

struct WorkerPermissionProvider;

#[async_trait::async_trait]
impl LLMProvider for WorkerPermissionProvider {
    async fn chat_stream(
        &self,
        messages: &[bamboo_agent_core::Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let next = PERMISSION_PROBES.iter().find(|(id, _)| {
            !messages
                .iter()
                .any(|message| message.tool_call_id.as_deref() == Some(*id))
        });
        let response = if let Some((id, command)) = next {
            LLMChunk::ToolCalls(vec![ToolCall {
                id: (*id).into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "exec_command".into(),
                    arguments: json!({"cmd": command}).to_string(),
                },
            }])
        } else {
            LLMChunk::Token(SHELL_PROBES_DONE.into())
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(response),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
async fn supervisor_auto_dispatch_runs_three_commands_without_approval() {
    use bamboo_tools::permission::{
        DurablePermissionRule, PermissionRuleEffect, PermissionRuleScope, PermissionRuleSource,
    };

    for ultra in [false, true] {
        let mut config = Config::default();
        config.provider = "openai".into();
        config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            model: Some("worker-permission-fixture".into()),
            ..Default::default()
        });
        config
            .memory_mut()
            .get_or_insert_with(Default::default)
            .auto_dream_enabled = false;
        let provider: Arc<dyn LLMProvider> = Arc::new(WorkerPermissionProvider);
        let f = Fixture::with_provider(ultra, config, provider.clone()).await;
        f.state
            .provider_registry
            .replace_with(ProviderRegistry::new(
                std::collections::HashMap::from([("openai".into(), provider)]),
                "openai".into(),
            ));
        f.set_supervisor_permission_mode(SessionPermissionMode::Auto, true)
            .await;
        let permissions = f.state.permission_checker.permission_config().unwrap();
        permissions.set_ask_rules(["Bash".into()]);
        permissions
            .add_durable_rule(DurablePermissionRule {
                id: "worker-shell-deny".into(),
                permission_type: PermissionType::ExecuteCommand,
                effect: PermissionRuleEffect::Deny,
                scope: PermissionRuleScope::Global,
                workspace_path: None,
                matcher: PermissionMatcher {
                    id: "worker-shell-deny".into(),
                    kind: PermissionMatcherKind::ExactResource,
                    value: PERMISSION_PROBES[3].1.into(),
                },
                source: PermissionRuleSource::User,
                expires_at: None,
            })
            .unwrap();

        let mut request = f.request(uuid::Uuid::new_v4());
        if !ultra {
            request = request["request"].clone();
            request.as_object_mut().unwrap().remove("project_id");
            request.as_object_mut().unwrap().remove("workspace_path");
        }
        let route = if ultra {
            "session_control"
        } else {
            "spawn_session"
        };
        let created = f.invoke_named(route, request.clone()).await;
        let id = created["target_session_id"].as_str().unwrap();
        let root = timeout(Duration::from_secs(20), async {
            loop {
                let root = f.state.storage.load_session(id).await.unwrap().unwrap();
                assert!(
                    root.pending_question.is_none(),
                    "Auto worker asked for approval"
                );
                if root
                    .messages
                    .iter()
                    .any(|message| message.content == SHELL_PROBES_DONE)
                {
                    break root;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the new worker executes its first input and answers");
        assert_eq!(
            root.agent_runtime_state
                .as_ref()
                .unwrap()
                .effective_permission_mode(),
            SessionPermissionMode::Auto
        );
        for (index, (call_id, command)) in PERMISSION_PROBES.iter().enumerate() {
            let result = root
                .messages
                .iter()
                .find(|message| message.tool_call_id.as_deref() == Some(*call_id))
                .unwrap();
            if index == 3 {
                assert!(
                    result.content.contains("worker-shell-deny"),
                    "{}",
                    result.content
                );
            } else {
                assert!(
                    result.content.contains(command.trim_start_matches("echo ")),
                    "{}",
                    result.content
                );
            }
        }

        f.set_supervisor_permission_mode(SessionPermissionMode::Auto, false)
            .await;
        let replay = f.invoke_named(route, request).await;
        assert_eq!(replay["target_session_id"], id);
        assert_eq!(replay["receipt_id"], created["receipt_id"]);
        assert_eq!(
            f.state
                .storage
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .agent_runtime_state
                .unwrap()
                .effective_permission_mode(),
            SessionPermissionMode::Auto
        );
        let next = f.invoke(f.request(uuid::Uuid::new_v4())).await;
        let without_dispatch_auto = f
            .state
            .storage
            .load_root_authority(next["target_session_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            without_dispatch_auto
                .agent_runtime_state
                .unwrap()
                .effective_permission_mode(),
            SessionPermissionMode::Bypass
        );
        f.state.shutdown().await;
    }
}

const PARALLEL_ROOT_INPUTS: [&str; 3] = [
    "Check dispatch alpha.",
    "Check dispatch beta.",
    "Check dispatch gamma.",
];

struct ParallelRootProvider {
    calls: mpsc::UnboundedSender<String>,
    release_siblings: CancellationToken,
}

#[async_trait::async_trait]
impl LLMProvider for ParallelRootProvider {
    async fn chat_stream(
        &self,
        messages: &[bamboo_agent_core::Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let mut inputs = messages.iter().filter(|message| {
            message.role == bamboo_agent_core::Role::User
                && PARALLEL_ROOT_INPUTS
                    .iter()
                    .any(|text| message.content.contains(*text))
        });
        let input = inputs
            .next()
            .expect("the provider receives the dispatched input");
        assert!(inputs.next().is_none(), "one input per Root provider call");
        self.calls.send(input.id.clone()).unwrap();
        if !input.content.contains(PARALLEL_ROOT_INPUTS[2]) {
            self.release_siblings.cancelled().await;
        }
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("Dispatch checked.".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
async fn supervisor_parallel_dispatch_survives_first_input_ack_cleanup_expiry() {
    let (sent, mut calls) = mpsc::unbounded_channel();
    let release_siblings = CancellationToken::new();
    let _release_on_drop = release_siblings.clone().drop_guard();
    let mut config = Config::default();
    config.provider = "openai".into();
    config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
        model: Some("dispatch-fixture".into()),
        ..Default::default()
    });
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let provider: Arc<dyn LLMProvider> = Arc::new(ParallelRootProvider {
        calls: sent,
        release_siblings: release_siblings.clone(),
    });
    let f = Fixture::with_provider(false, config, provider.clone()).await;
    f.state
        .provider_registry
        .replace_with(ProviderRegistry::new(
            std::collections::HashMap::from([("openai".into(), provider)]),
            "openai".into(),
        ));
    let count = f.state.session_store.list_index_entries().await.len();
    let mut roots = Vec::new();
    let wait = Duration::from_secs(20);
    for input in &PARALLEL_ROOT_INPUTS[..2] {
        let mut request = f.request(uuid::Uuid::new_v4());
        request["request"]["message"] = json!(input);
        roots.push(f.invoke(request).await);
    }
    let mut siblings = timeout(wait, async {
        [calls.recv().await.unwrap(), calls.recv().await.unwrap()]
    })
    .await
    .expect("both sibling Roots enter the provider before the third dispatch");
    siblings.sort_unstable();
    let mut expected = roots
        .iter()
        .map(|root| root["receipt_id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(siblings.as_slice(), expected.as_slice());

    let inbox = f
        .state
        .session_inbox
        .as_any()
        .unwrap()
        .downcast_ref::<bamboo_storage::FileSessionInbox>()
        .unwrap();
    let expired = inbox.expire_next_root_ack_cleanup_for_testing();
    let mut request = f.request(uuid::Uuid::new_v4());
    request["request"]["message"] = json!(PARALLEL_ROOT_INPUTS[2]);
    let third = f.invoke(request.clone()).await;
    let id = third["target_session_id"].as_str().unwrap();
    let receipt = third["receipt_id"].as_str().unwrap();
    timeout(wait, expired.notified())
        .await
        .expect("the real ACK crosses the Inbox lease boundary");
    assert_eq!(
        timeout(wait, calls.recv())
            .await
            .expect("the third Root reaches the provider"),
        Some(receipt.to_owned())
    );
    let replay = f.invoke(request).await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["target_session_id"], id);
    assert_eq!(replay["receipt_id"], receipt);
    roots.push(third);
    release_siblings.cancel();

    for root in &roots {
        let id = root["target_session_id"].as_str().unwrap();
        let receipt = root["receipt_id"].as_str().unwrap();
        let run = f.state.agent_runners.read().await[id].run_id.clone();
        let actor = timeout(wait, async {
            loop {
                let actor = f
                    .state
                    .session_store
                    .inspect_actor(id)
                    .await
                    .unwrap()
                    .activation
                    .unwrap();
                if actor.finished_at.is_some()
                    && f.state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_none()
                {
                    break actor;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the original Root retires its durable activation");
        assert_eq!(actor.run_id, run);
        assert_eq!(actor.status, ActorActivationStatus::Succeeded);
        assert_eq!(actor.attempt, 1, "one activation for the original Root");
        let durable = f.state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|message| message.id == receipt)
                .count(),
            1
        );
        assert!(inbox
            .was_admitted(id, &SessionMessageId::parse(receipt).unwrap())
            .await
            .unwrap());
        let backlog = inbox.inspect(id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
    }
    assert_eq!(
        f.state.session_store.list_index_entries().await.len(),
        count + 3
    );
    assert!(
        calls.try_recv().is_err(),
        "each dispatch reaches the provider once"
    );
    f.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_creates_and_continues_standalone_roots_without_inheriting_a_project_workspace()
{
    for ultra in [false, true] {
        let f = Fixture::new(ultra).await;
        f.state.config.write().await.default_work_area =
            Some(bamboo_config::DefaultWorkAreaConfig {
                path: Some(f.workspace.clone()),
            });
        let scope = f.invoke(json!({"action":"scope"})).await;
        assert_eq!(scope["create_unassigned"], true);
        let operation = uuid::Uuid::new_v4();
        let permissions = f.state.permission_checker.permission_config().unwrap();
        permissions
            .grant_typed_scoped_session_permission(
                &f.supervisor.session_id,
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "standalone-creation".into(),
                    kind: PermissionMatcherKind::ExactResource,
                    value: format!("session_control create [unassigned] {operation}"),
                },
            )
            .unwrap();
        let mut request = json!({
            "operation_id":operation, "expected_state_revision":scope["state_revision"],
            "message":"Check the installation instructions for a local utility.",
            "title":"Check local utility installation",
            "notify_on_material_event":scope["create_material_notifications"]
        });
        let count = f.state.session_store.list_index_entries().await.len();
        let created = if ultra {
            f.invoke(json!({"action":"create", "request":request.clone()}))
                .await
        } else {
            f.invoke_named("spawn_session", request.clone()).await
        };
        assert_eq!(created["admitted"], true);
        assert!(created["project_id"].is_null());
        let id = created["target_session_id"].as_str().unwrap();
        let root = f
            .state
            .storage
            .load_root_authority(id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root.kind, SessionKind::Root);
        assert!(root.project_id_meta().is_none());
        assert!(root.parent_session_id.is_none());
        assert!(root.authority_identity.is_ordinary());
        assert_eq!(
            root.agent_runtime_state
                .as_ref()
                .unwrap()
                .effective_permission_mode(),
            bamboo_domain::SessionPermissionMode::Bypass
        );
        let workspace = f
            .home
            .path()
            .join("workspaces")
            .join(id)
            .canonicalize()
            .unwrap();
        let workspace = bamboo_config::paths::path_to_display_string(&workspace);
        assert_eq!(
            root.workspace_path_meta().as_deref(),
            Some(workspace.as_str())
        );
        assert_ne!(workspace, f.workspace);

        let directory = f
            .invoke(json!({"action":"directory", "query":"Check local utility installation"}))
            .await;
        let entries = directory["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["session_id"], id);
        assert_eq!(entries[0]["origin"], "supervisor_created");
        assert_eq!(entries[0]["project_id"], Value::Null);

        request["project_id"] = Value::Null;
        request["workspace_path"] = Value::Null;
        let replay = if ultra {
            f.invoke_named("spawn_session", request).await
        } else {
            f.invoke(json!({"action":"create", "request":request}))
                .await
        };
        assert_eq!(replay["target_session_id"], id);
        assert_eq!(replay["receipt_id"], created["receipt_id"]);
        assert_eq!(
            f.state.session_inbox.inspect(id).await.unwrap().generation,
            1
        );

        let ToolOutcome::Completed(conflict) = f
            .dispatch_named("session_control", f.request(operation), true)
            .await
            .unwrap()
        else {
            panic!("expected exact retry conflict");
        };
        assert!(!conflict.success);
        let conflict: Value = serde_json::from_str(&conflict.result).unwrap();
        assert_eq!(conflict["code"], "idempotency_conflict");

        let current = f
            .state
            .storage
            .load_root_authority(id)
            .await
            .unwrap()
            .unwrap();
        let target = bamboo_domain::SupervisorCurrentUserTarget::capture(&current).unwrap();
        permissions
            .grant_typed_scoped_session_permission(
                &f.supervisor.session_id,
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: "standalone-followup".into(),
                    kind: PermissionMatcherKind::ExactResource,
                    value: format!("session_control followup {id}"),
                },
            )
            .unwrap();
        let followup = f
            .invoke(json!({
                "action":"followup", "target_session_id":id, "target_ref":target,
                "operation_id":uuid::Uuid::new_v4(), "message":"Continue the same check."
            }))
            .await;
        assert_eq!(followup["target_session_id"], id);
        assert_eq!(followup["admitted"], true);
        assert_eq!(
            f.state.session_store.list_index_entries().await.len(),
            count + 1
        );
        f.state.shutdown().await;
    }
}

struct UltraWorkerProvider(mpsc::UnboundedSender<Vec<String>>);

#[async_trait::async_trait]
impl LLMProvider for UltraWorkerProvider {
    async fn chat_stream(
        &self,
        _messages: &[bamboo_agent_core::Message],
        tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0
            .send(
                tools
                    .iter()
                    .map(|tool| tool.function.name.clone())
                    .collect(),
            )
            .unwrap();
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("Ultra assignment checked.".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
async fn supervisor_can_create_an_ordinary_ultra_worker_before_its_first_turn() {
    for standalone in [false, true] {
        let mut config = Config::default();
        config.provider = "openai".into();
        config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            model: Some("ultra-worker-fixture".into()),
            ..Default::default()
        });
        config
            .memory_mut()
            .get_or_insert_with(Default::default)
            .auto_dream_enabled = false;
        let (sent, mut calls) = mpsc::unbounded_channel();
        let provider: Arc<dyn LLMProvider> = Arc::new(UltraWorkerProvider(sent));
        let f = Fixture::with_provider(false, config, provider.clone()).await;
        f.state
            .provider_registry
            .replace_with(ProviderRegistry::new(
                std::collections::HashMap::from([("openai".into(), provider)]),
                "openai".into(),
            ));
        let operation = uuid::Uuid::new_v4();
        let mut request = f.request(operation);
        request["request"]["thinking_mode"] = json!("ultra");
        let route = if standalone {
            f.state
                .permission_checker
                .permission_config()
                .unwrap()
                .grant_typed_scoped_session_permission(
                    &f.supervisor.session_id,
                    PermissionType::ExecuteCommand,
                    PermissionMatcher {
                        id: "standalone-ultra-create".into(),
                        kind: PermissionMatcherKind::ExactResource,
                        value: format!("session_control create [unassigned] {operation}"),
                    },
                )
                .unwrap();
            request = request["request"].clone();
            request.as_object_mut().unwrap().remove("project_id");
            request.as_object_mut().unwrap().remove("workspace_path");
            "spawn_session"
        } else {
            "session_control"
        };
        let receipt = f.invoke_named(route, request.clone()).await;
        assert_eq!(receipt["admitted"], true);
        let id = receipt["target_session_id"].as_str().unwrap();
        let catalog = timeout(Duration::from_secs(20), calls.recv())
            .await
            .expect("the worker's first turn reached the provider")
            .unwrap();
        for name in ["Read", "Write", "Edit", "SubAgent"] {
            assert!(catalog.iter().any(|tool| tool == name), "{catalog:?}");
        }
        for name in ["exec_command", "session_control"] {
            assert!(!catalog.iter().any(|tool| tool == name), "{catalog:?}");
        }
        let root = f
            .state
            .storage
            .load_root_authority(id)
            .await
            .unwrap()
            .unwrap();
        assert!(root.authority_identity.is_ordinary());
        assert_eq!(root.root_thinking_mode(), RootThinkingMode::Ultra);
        assert_eq!(root.root_session_id, id);
        assert!(root.parent_session_id.is_none());
        let replay = f.invoke_named(route, request.clone()).await;
        assert_eq!(replay["target_session_id"], id);
        assert_eq!(replay["receipt_id"], receipt["receipt_id"]);
        if standalone {
            request["thinking_mode"] = json!("standard");
        } else {
            request["request"]["thinking_mode"] = json!("standard");
        }
        let ToolOutcome::Completed(conflict) =
            f.dispatch_named(route, request, true).await.unwrap()
        else {
            panic!("changing the reserved mode must conflict");
        };
        assert!(!conflict.success);
        let result: Value = serde_json::from_str(&conflict.result).unwrap();
        assert_eq!(result["code"], "idempotency_conflict");
        assert_eq!(
            f.state.session_inbox.inspect(id).await.unwrap().generation,
            1
        );
        f.state.shutdown().await;
    }
}

async fn assert_v2_spawn_uses_root_port_in_both_modes(ultra: bool) {
    let f = Fixture::new(ultra).await;
    let request = f.request(uuid::Uuid::new_v4())["request"].clone();
    let before = f.state.session_store.list_index_entries().await.len();
    let Err(ToolError::Execution(reason)) = f
        .dispatch_named("spawn_session", request.clone(), false)
        .await
    else {
        panic!("Supervisor identity without the original Host caller must be refused");
    };
    assert_eq!(
        reason,
        "spawn_session requires the original Host-observed caller identity"
    );
    assert_eq!(
        f.state.session_store.list_index_entries().await.len(),
        before
    );
    let created = f.invoke_named("spawn_session", request.clone()).await;
    assert_eq!(created["admitted"], true);
    let id = created["target_session_id"].as_str().unwrap();
    let root = f.state.storage.load_session(id).await.unwrap().unwrap();
    assert_eq!(root.kind, SessionKind::Root);
    assert_eq!(root.session_version, Some(2));
    assert_eq!(root.root_session_id, id);
    assert!(root.parent_session_id.is_none() && root.authority_identity.is_ordinary());
    assert_eq!(
        root.workspace_path_meta().as_deref(),
        Some(f.workspace.as_str())
    );
    let old_entry_retry = f
        .invoke(json!({"action":"create", "request":request.clone()}))
        .await;
    assert_eq!(
        old_entry_retry["target_session_id"],
        created["target_session_id"]
    );
    assert_eq!(old_entry_retry["receipt_id"], created["receipt_id"]);
    let replay = f.invoke_named("default::spawnSession", request).await;
    assert_eq!(replay["target_session_id"], created["target_session_id"]);
    assert_eq!(replay["receipt_id"], created["receipt_id"]);
    assert_eq!(
        f.state.session_inbox.inspect(id).await.unwrap().generation,
        1
    );
    assert_eq!(
        f.state.session_store.list_index_entries().await.len(),
        before + 1,
        "denied caller and exact retries cannot create another Root"
    );
    let canonical = f
        .state
        .storage
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    let bound = f
        .state
        .tools_for(crate::tools::ToolSurface::Root)
        .for_session(&canonical)
        .unwrap();
    assert!(bound.owns_exact_tool("spawn_session"));
    assert!(bound.owns_exact_tool("SubAgent"));
    assert_eq!(
        bound
            .resolve_reference_for_session(&canonical, "default::spawnSession")
            .as_deref(),
        Some("spawn_session")
    );
    assert!(
        f.state
            .storage
            .inspect_supervisor_link(&f.supervisor, id)
            .await
            .unwrap()
            .authorized
    );
    f.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_v2_standard_spawn_uses_real_root_creation_and_exact_retry() {
    assert_v2_spawn_uses_root_port_in_both_modes(false).await;
}

#[actix_web::test]
async fn supervisor_v2_ultra_spawn_uses_real_root_creation_and_exact_retry() {
    assert_v2_spawn_uses_root_port_in_both_modes(true).await;
}

// These use the existing real AppState/Host operation fixture. The temporary
// Work feature opt-in never changes installed/user security configuration.
#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn work_bound_supervisor_chat_case(standard: bool, stable_message_id: bool) {
    let f = Fixture::new(true).await;
    f.state.config.write().await.features.ticket_mutation = true;
    let observed = f.state.tickets.status().await;
    let payload = observed["initialization"].clone();
    assert!(
        payload.is_object(),
        "fresh Work scope must expose its initializer"
    );
    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let initialized = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/initialize")
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .set_json(payload)
            .to_request(),
    )
    .await;
    assert_eq!(
        initialized.status(),
        StatusCode::OK,
        "normal owner Work initialization must succeed"
    );
    let _: Value = test::read_body_json(initialized).await;
    let service = f.state.tickets.service().unwrap();
    let original = f
        .state
        .storage
        .load_root_authority(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    if standard {
        f.state
            .storage
            .root_mode_operation(&RootModeOperationRequest {
                session_id: original.id.clone(),
                operation_id: format!(
                    "{}:{}",
                    original.root_mode_transition_epoch,
                    uuid::Uuid::new_v4()
                ),
                birth_token: original.root_mode_birth_token(),
                expected_epoch: original.root_mode_transition_epoch,
                requested_enabled: false,
                action: RootModeOperationAction::Select,
            })
            .await
            .unwrap();
        f.state.sessions.remove(&original.id);
    }
    let scope = f.state.tickets.status().await;
    assert_eq!(scope["availability"]["kind"], "ready");
    if standard {
        assert_eq!(scope["availability"]["reason_code"], "scope_ready");
        let input_before = f.state.session_inbox.inspect(&original.id).await.unwrap();
        let message_id = uuid::Uuid::new_v4().to_string();
        let threaded = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .peer_addr("127.0.0.1:12345".parse().unwrap())
                .set_json(json!({
                    "session_id": original.id,
                    "message_id": message_id,
                    "message": "Record this authenticated Work input",
                    "model": "supervisor-private-model",
                    "thread_id": "work-only-thread"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(threaded.status(), StatusCode::CREATED);
        let threaded_receipt: Value = test::read_body_json(threaded).await;
        assert_eq!(threaded_receipt["message_id"], message_id);
        let published = service.published().unwrap();
        let ingress = published.1.resolutions[&message_id]
            .ingress
            .as_ref()
            .unwrap();
        assert_eq!(ingress.thread_id.as_deref(), Some("work-only-thread"));
        assert_eq!(ingress.text, "Record this authenticated Work input");
        assert_eq!(
            f.state
                .session_inbox
                .inspect(&original.id)
                .await
                .unwrap()
                .generation,
            input_before.generation + 1
        );
    }
    let before = service.published().unwrap();
    let mut chat = json!({
        "session_id": original.id,
        "message": "Coordinate an independent Root without submitting Work",
        "model": "supervisor-private-model"
    });
    if stable_message_id {
        chat["message_id"] = json!(uuid::Uuid::new_v4().to_string());
    }
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/chat")
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .set_json(chat.clone())
            .to_request(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "plain Supervisor chat must retain its independent Root ingress"
    );
    let receipt: Value = test::read_body_json(response).await;
    if stable_message_id {
        assert_eq!(receipt["message_id"], chat["message_id"]);
    }
    assert!(
        receipt["message_id"].is_string() && receipt["ingress_seq"].as_u64().is_some_and(|n| n > 0)
    );
    if standard {
        assert_eq!(
            service.published().unwrap().0,
            before.0,
            "ordinary Root ingress must not create a formal Work intent"
        );
        assert!(bamboo_engine::ticket_runtime::verified_scope_binding(
            f.state.storage.as_ref(),
            &original.id
        )
        .await
        .is_ok());
    } else {
        assert!(
            service.published().unwrap().1.resolutions.len() > before.1.resolutions.len(),
            "Ultra Human ingress must still record its formal Work intent"
        );
        assert!(bamboo_engine::ticket_runtime::verified_scope_binding(
            f.state.storage.as_ref(),
            &original.id
        )
        .await
        .is_ok());
    }
    drop(app);
    let operation = uuid::Uuid::new_v4();
    let created = f.invoke(f.request(operation)).await;
    assert_eq!(created["admitted"], true);
    let id = created["target_session_id"].as_str().unwrap();
    let root = f
        .state
        .storage
        .load_root_authority(id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.kind, SessionKind::Root);
    assert!(root.parent_session_id.is_none() && root.authority_identity.is_ordinary());
    assert_eq!(root.supervisor_incarnation_id(), None);
    assert!(!root.root_orchestration_only_enabled());
    assert_eq!(
        root.workspace_path_meta().as_deref(),
        Some(f.workspace.as_str())
    );
    let replay = f.invoke(f.request(operation)).await;
    assert_eq!(replay["target_session_id"], created["target_session_id"]);
    assert_eq!(replay["receipt_id"], created["receipt_id"]);
    assert_eq!(
        f.state.session_inbox.inspect(id).await.unwrap().generation,
        1
    );
    f.state.shutdown().await;
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[actix_web::test]
async fn work_bound_standard_supervisor_distinguishes_plain_chat_from_formal_ingress() {
    work_bound_supervisor_chat_case(true, false).await;
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[actix_web::test]
async fn work_bound_ultra_supervisor_retains_formal_ingress_and_independent_roots() {
    work_bound_supervisor_chat_case(false, false).await;
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[actix_web::test]
async fn work_bound_standard_supervisor_keeps_native_message_identity_for_plain_chat() {
    work_bound_supervisor_chat_case(true, true).await;
}
