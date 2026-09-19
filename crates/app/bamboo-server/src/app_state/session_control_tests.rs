//! Actual root tools, FileInbox, activation spawner and human-response transport.
use super::AppState;
use crate::tools::ToolSurface;
use actix_web::{test, web, App};
use bamboo_agent_core::tools::{
    ExecutingSupervisorObservation, FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome,
    ToolSchema,
};
use bamboo_agent_core::{Message, Session, ToolExecutionContext};
use bamboo_domain::{SessionActivationPolicy, SessionMessageEnvelope, SupervisorReference};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_llm::{Config, LLMProvider, ProviderRegistry};
use bamboo_tools::permission::{PermissionDecision, PermissionDecisionKind, PermissionRequest};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const TARGET: &str = "followup-runtime-target";
const CALL: &str = "target-human-question";
const STEER: &str = "supervisor followup unique content";

// The compatibility conclusion tool is deliberately HostOnly. As in the
// existing runner NeedsHuman regression, expose a fixture-only question name
// while delegating the actual question behavior to the production tool.
struct FixtureClarification;

#[async_trait::async_trait]
impl Tool for FixtureClarification {
    fn name(&self) -> &str {
        "fixture_clarification"
    }
    fn description(&self) -> &str {
        "Ask the original target question"
    }
    fn parameters_schema(&self) -> Value {
        bamboo_tools::tools::ConclusionWithOptionsTool::new().parameters_schema()
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        bamboo_tools::tools::ConclusionWithOptionsTool::new()
            .invoke(args, ctx)
            .await
    }
}

#[derive(Default)]
struct Probe {
    next: Mutex<Option<ToolCall>>,
    blocked: std::sync::atomic::AtomicBool,
    entered: Notify,
    release: Notify,
    requests: Mutex<Vec<(String, Vec<Message>)>>,
}

#[async_trait::async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        self.requests
            .lock()
            .unwrap()
            .push((model.into(), messages.to_vec()));
        let output = self.next.lock().unwrap().take();
        if self
            .blocked
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let mut chunks = Vec::new();
        if output.is_some() {
            // The actual conclusion tool requires an assistant explanation
            // before its question. Exercise that policy instead of disabling it.
            chunks.push(Ok(bamboo_llm::LLMChunk::Token(
                "The original task needs your decision before continuing.".into(),
            )));
        }
        let output = output
            .map(|call| bamboo_llm::LLMChunk::ToolCalls(vec![call]))
            .unwrap_or_else(|| {
                bamboo_llm::LLMChunk::Token("original Root completed followup".into())
            });
        chunks.push(Ok(output));
        chunks.push(Ok(bamboo_llm::LLMChunk::Done));
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}

struct Fixture {
    _home: tempfile::TempDir,
    state: web::Data<AppState>,
    probe: Arc<Probe>,
    reference: SupervisorReference,
    original: Session,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::default());
        let mut config = Config::from_data_dir(Some(home.path().to_path_buf()));
        config.provider = "followup-provider".into();
        let mut state =
            AppState::new_with_provider(home.path().to_path_buf(), config, probe.clone())
                .await
                .unwrap();
        let root = Arc::new(crate::tools::OverlayToolExecutor::new(
            state.tools_for(ToolSurface::Root),
            Arc::new(FixtureClarification),
        ));
        state
            .child_completion_coordinator
            .set_root_tools(root.clone())
            .await;
        state.tool_factory = crate::tools::ToolSurfaceFactory::new(
            state.tools_for(ToolSurface::Base),
            state.tools_for(ToolSurface::WithTask),
            root,
        );
        // Mutate the shared registry through its supported hot-reload seam so
        // the real coordinator and HTTP resume adapter use this same provider.
        state.provider_registry.replace_with(ProviderRegistry::new(
            HashMap::from([(
                "followup-provider".into(),
                probe.clone() as Arc<dyn LLMProvider>,
            )]),
            "followup-provider".into(),
        ));
        let project = state
            .project_store
            .create("Original target Project", None)
            .unwrap();
        let workspace = home.path().join("original-workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = std::fs::canonicalize(workspace).unwrap();
        let mut original = Session::new(TARGET, "original-target-model");
        original.set_project_id_meta(project.id.to_string());
        original.set_workspace_path_meta(workspace.to_string_lossy());
        original
            .metadata
            .insert("provider".into(), "followup-provider".into());
        original.add_message(Message::user("original target context marker"));
        original.add_message(Message::assistant("original target history prefix", None));
        state.storage.save_session(&original).await.unwrap();
        let service = SupervisorSessionService::new(state.session_store.clone());
        let reference: SupervisorReference = (&service
            .get_or_create_default("supervisor-model")
            .await
            .unwrap())
            .into();
        service
            .configure_project_scope(&reference, 0, [project.id].into())
            .await
            .unwrap();
        service.attach(&reference, 1, TARGET).await.unwrap();
        // This fixture's Supervisor has approval only for this one target
        // operation. Target permissions and every bypass flag remain unchanged.
        state
            .permission_checker
            .permission_config()
            .unwrap()
            .grant_typed_scoped_session_permission(
                &reference.session_id,
                bamboo_tools::permission::PermissionType::ExecuteCommand,
                bamboo_tools::permission::PermissionMatcher {
                    id: "approved-followup-target".into(),
                    kind: bamboo_tools::permission::PermissionMatcherKind::ExactResource,
                    value: format!("session_control followup {TARGET}"),
                },
            )
            .unwrap();
        // Prewarm existing catalog watchers before bounded runtime assertions.
        state
            .skill_manager
            .store_for_workspace(Some(&workspace))
            .await
            .unwrap();
        Self {
            _home: home,
            state: web::Data::new(state),
            probe,
            reference,
            original,
        }
    }

    fn arm_question(&self, permission: bool, block: bool) {
        *self.probe.next.lock().unwrap() = Some(ToolCall {
            id: CALL.into(), tool_type: "function".into(),
            function: FunctionCall {
                name: if permission { "Bash" } else { "fixture_clarification" }.into(),
                arguments: if permission { json!({"command":"echo followup-permission-probe"}) } else {
                    json!({"question":"Which original option?", "options":["A","B"], "conclusion":{"summary":"Need original user choice", "mermaid":{"graph":"graph TD\nA-->B"}}})
                }.to_string(),
            },
        });
        self.probe
            .blocked
            .store(block, std::sync::atomic::Ordering::SeqCst);
        self.state
            .permission_checker
            .permission_config()
            .unwrap()
            .set_ask_rules(["Bash(echo followup-permission-probe)".into()]);
    }

    async fn call(
        &self,
        operation: &str,
        message: &str,
    ) -> Result<ToolOutcome, bamboo_agent_core::tools::ToolError> {
        let supervisor = self
            .state
            .storage
            .load_session(&self.reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let call = ToolCall { id: format!("call-{operation}"), tool_type: "function".into(), function: FunctionCall {
            name: "session_control".into(), arguments: json!({"action":"followup", "target_session_id":TARGET, "operation_id":operation, "message":message}).to_string(),
        }};
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&supervisor.id);
        ctx.root_session_id = Some(&supervisor.id);
        ctx = ctx.with_executing_supervisor(
            ExecutingSupervisorObservation::capture_from_executing_session(&supervisor),
        );
        self.state
            .tools_for(ToolSurface::Root)
            .execute_with_context_outcome(&call, ctx)
            .await
    }

    async fn followup(&self, operation: &str, message: &str) -> Value {
        let ToolOutcome::Completed(result) = self.call(operation, message).await.unwrap() else {
            panic!("followup receipt required")
        };
        assert!(result.success, "{}", result.result);
        serde_json::from_str(&result.result).unwrap()
    }

    async fn reload(&self) -> Session {
        self.state
            .storage
            .load_session(TARGET)
            .await
            .unwrap()
            .unwrap()
    }

    async fn settled(&self, calls: usize) {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let calls_ready = self.probe.requests.lock().unwrap().len() >= calls;
                let active = self
                    .state
                    .agent_runners
                    .read()
                    .await
                    .get(TARGET)
                    .is_some_and(|runner| {
                        matches!(
                            runner.status,
                            crate::app_state::AgentStatus::Pending
                                | crate::app_state::AgentStatus::Running
                        )
                    });
                if calls_ready
                    && !active
                    && self
                        .state
                        .session_activation_router
                        .current_run_id(TARGET)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual Root runner must settle");
    }

    async fn answer(&self, permission: bool) {
        let app = test::init_service(
            App::new()
                .app_data(self.state.clone())
                .route(
                    "/sessions/{session_id}/respond/pending",
                    web::get().to(crate::handlers::agent::respond::get_pending_question),
                )
                .route(
                    "/sessions/{session_id}/permission-decisions",
                    web::post().to(crate::handlers::agent::respond::submit_permission_decision),
                )
                .route(
                    "/sessions/{session_id}/respond",
                    web::post().to(crate::handlers::agent::respond::submit_response),
                ),
        )
        .await;
        let pending_response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/sessions/{TARGET}/respond/pending"))
                .to_request(),
        )
        .await;
        assert!(pending_response.status().is_success());
        let pending: Value = test::read_body_json(pending_response).await;
        assert_eq!(
            pending["interaction_kind"],
            if permission {
                "permission"
            } else {
                "clarification"
            }
        );
        let request = if permission {
            let request: PermissionRequest =
                serde_json::from_value(pending["permission_request"].clone()).unwrap();
            test::TestRequest::post()
                .uri(&format!("/sessions/{TARGET}/permission-decisions"))
                .set_json(PermissionDecision {
                    request_id: CALL.into(),
                    request_generation: request.request_generation,
                    decision: PermissionDecisionKind::DenyOnce,
                    matcher_id: None,
                    expected_policy_revision: Some(request.policy_revision),
                    confirm_global: false,
                })
        } else {
            test::TestRequest::post().uri(&format!("/sessions/{TARGET}/respond")).set_json(json!({"response":"A", "expected_tool_call_id":CALL, "model":"original-target-model", "provider":"followup-provider"}))
        };
        let response = test::call_service(&app, request.to_request()).await;
        assert!(
            response.status().is_success(),
            "{}",
            String::from_utf8_lossy(&test::read_body(response).await)
        );
    }

    fn assert_context(&self, session: &Session) {
        assert_eq!(session.project_id_meta(), self.original.project_id_meta());
        assert_eq!(
            session.workspace_path_meta(),
            self.original.workspace_path_meta()
        );
        assert_eq!(session.model, self.original.model);
        assert_eq!(
            session.metadata.get("provider"),
            self.original.metadata.get("provider")
        );
        for original in &self.original.messages {
            assert!(session
                .messages
                .iter()
                .any(|message| message.id == original.id && message.content == original.content));
        }
        assert!(self
            .probe
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(model, messages)| model == "original-target-model"
                && messages
                    .iter()
                    .any(|message| message.content.contains("original target context marker"))));
    }
}

#[actix_web::test]
async fn supervisor_followup_real_root_surface_gates_execution_and_idle_retry_runs_once() {
    let f = Box::pin(Fixture::new()).await;
    f.state
        .permission_checker
        .permission_config()
        .unwrap()
        .set_ask_rules(["session_control".into()]);
    let denied = f.call("permission-gate", STEER).await.unwrap_err();
    assert!(denied.to_string().contains("Permission approval required"));
    assert_eq!(
        f.state
            .session_inbox
            .inspect(TARGET)
            .await
            .unwrap()
            .generation,
        0
    );
    assert!(f.probe.requests.lock().unwrap().is_empty());
    f.state
        .permission_checker
        .permission_config()
        .unwrap()
        .set_ask_rules(Vec::<String>::new());
    let receipt = f.followup("idle-operation", STEER).await;
    f.settled(1).await;
    let after = f.reload().await;
    f.assert_context(&after);
    assert_eq!(
        after
            .messages
            .iter()
            .filter(|message| message.id == receipt["receipt_id"].as_str().unwrap())
            .count(),
        1
    );
    let retry = f.followup("idle-operation", STEER).await;
    assert_eq!(retry["receipt_id"], receipt["receipt_id"]);
    f.settled(1).await;
    assert_eq!(f.probe.requests.lock().unwrap().len(), 1);
    assert_eq!(
        f.state.session_inbox.inspect(TARGET).await.unwrap().pending,
        0
    );
    f.state.shutdown().await;
}

#[actix_web::test]
async fn supervisor_followup_real_spawner_preserves_human_wait_with_older_user_interrupt() {
    for permission in [false, true] {
        let f = Box::pin(Fixture::new()).await;
        f.arm_question(permission, false);
        let original_receipt = f
            .followup("start-question", "continue until original human question")
            .await;
        f.settled(1).await;
        let before = f.reload().await;
        assert_eq!(before.pending_question.as_ref().unwrap().tool_call_id, CALL);
        let retry = f
            .followup("start-question", "continue until original human question")
            .await;
        assert_eq!(retry["receipt_id"], original_receipt["receipt_id"]);
        assert_eq!(retry["generation"], original_receipt["generation"]);
        assert_eq!(f.probe.requests.lock().unwrap().len(), 1);
        assert_eq!(
            f.state.session_inbox.inspect(TARGET).await.unwrap().pending,
            0
        );
        let policy_before =
            serde_json::to_value(f.state.permission_section.snapshot().data.as_ref()).unwrap();
        // An older genuine User admission already raised the interrupt watermark.
        // A later Session followup must not borrow that watermark to clear a question.
        let user = SessionMessageEnvelope::user_input(TARGET, "queued ordinary user input");
        let admitted = f.state.session_messenger.admit(user).await.unwrap();
        f.state
            .session_inbox
            .mark_activation_eligible(
                TARGET,
                admitted.delivery.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let receipt = f.followup("waiting-operation", STEER).await;
        f.settled(1).await;
        let waiting = f.reload().await;
        assert_eq!(
            serde_json::to_value(&waiting.pending_question).unwrap(),
            serde_json::to_value(&before.pending_question).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&waiting.agent_runtime_state).unwrap(),
            serde_json::to_value(&before.agent_runtime_state).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&waiting.messages).unwrap(),
            serde_json::to_value(&before.messages).unwrap()
        );
        assert_eq!(
            serde_json::to_value(f.state.permission_section.snapshot().data.as_ref()).unwrap(),
            policy_before
        );
        assert_eq!(f.probe.requests.lock().unwrap().len(), 1);
        assert_eq!(
            f.state.session_inbox.inspect(TARGET).await.unwrap().pending,
            2
        );
        f.answer(permission).await;
        f.settled(2).await;
        let after = f.reload().await;
        assert!(after.pending_question.is_none());
        f.assert_context(&after);
        let followup_id = receipt["receipt_id"].as_str().unwrap();
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|message| message.id == followup_id)
                .count(),
            1
        );
        let user_pos = after
            .messages
            .iter()
            .position(|message| message.id == admitted.delivery.id.as_str())
            .unwrap();
        let followup_pos = after
            .messages
            .iter()
            .position(|message| message.id == followup_id)
            .unwrap();
        assert!(
            user_pos < followup_pos,
            "canonical generation order retained"
        );
        {
            let requests = f.probe.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[1]
                    .1
                    .iter()
                    .filter(|message| message.id == followup_id)
                    .count(),
                1
            );
        }
        assert_eq!(
            f.state.session_inbox.inspect(TARGET).await.unwrap().pending,
            0
        );
        f.state.shutdown().await;
    }
}

#[actix_web::test]
async fn supervisor_followup_active_to_human_wait_keeps_queued_followup_until_real_response() {
    for permission in [false, true] {
        let f = Box::pin(Fixture::new()).await;
        f.arm_question(permission, true);
        f.followup("active-start", "start active original task")
            .await;
        tokio::time::timeout(Duration::from_secs(20), f.probe.entered.notified())
            .await
            .unwrap();
        assert!(f
            .state
            .session_activation_router
            .current_run_id(TARGET)
            .await
            .is_some());
        let receipt = f.followup("active-followup", STEER).await;
        assert_eq!(receipt["activation"], "active_notified");
        assert_eq!(f.probe.requests.lock().unwrap().len(), 1);
        f.probe.release.notify_one();
        f.settled(1).await;
        let waiting = f.reload().await;
        assert!(
            waiting.pending_question.is_some(),
            "permission={permission}; calls={}; messages={:?}",
            f.probe.requests.lock().unwrap().len(),
            waiting
                .messages
                .iter()
                .map(|message| (&message.role, &message.content))
                .collect::<Vec<_>>()
        );
        assert_eq!(f.probe.requests.lock().unwrap().len(), 1);
        assert_eq!(
            f.state.session_inbox.inspect(TARGET).await.unwrap().pending,
            1
        );
        f.answer(permission).await;
        f.settled(2).await;
        let after = f.reload().await;
        f.assert_context(&after);
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|message| message.id == receipt["receipt_id"].as_str().unwrap())
                .count(),
            1
        );
        assert_eq!(
            f.state.session_inbox.inspect(TARGET).await.unwrap().pending,
            0
        );
        f.state.shutdown().await;
    }
}

#[actix_web::test]
async fn supervisor_followup_real_activation_failures_keep_receipt_and_retry_original_input() {
    for failure in ["watermark", "spawner"] {
        let f = Box::pin(Fixture::new()).await;
        let watermark = f
            .state
            .app_data_dir
            .join("sessions")
            .join(TARGET)
            .join("inbox/activation-generation");
        if failure == "watermark" {
            tokio::fs::create_dir_all(&watermark).await.unwrap();
        } else {
            // The production spawner reports its real uninitialized-root-tool
            // error; do not replace activation with a successful mock counter.
            let uninitialized = bamboo_engine::ChildCompletionCoordinator::new(
                f.state.storage.clone(),
                f.state.persistence.clone(),
                f.state.sessions.clone(),
                f.state.agent_runners.clone(),
                f.state.session_event_senders.clone(),
                f.state.agent.clone(),
                f.state.config.clone(),
                f.state.provider_registry.clone(),
                f.state.provider_router.clone(),
                f.state.app_data_dir.clone(),
                None,
            );
            f.state
                .session_activation_router
                .set_spawner(Arc::new(uninitialized))
                .await;
        }
        let receipt = f.followup("failed-activation", STEER).await;
        assert_eq!(receipt["admitted"], true);
        assert_eq!(receipt["generation"], 1);
        assert_eq!(receipt["activation"], "activation_pending");
        let error = receipt["activation_error"].as_str().unwrap();
        assert!(
            error.contains(if failure == "watermark" {
                "watermark"
            } else {
                "root tool surface"
            }),
            "{error}"
        );
        assert!(f.probe.requests.lock().unwrap().is_empty());
        if failure == "watermark" {
            tokio::fs::remove_dir(&watermark).await.unwrap();
        } else {
            f.state
                .session_activation_router
                .set_spawner(f.state.child_completion_coordinator.clone())
                .await;
        }
        let retry = f.followup("failed-activation", STEER).await;
        assert_eq!(retry["receipt_id"], receipt["receipt_id"]);
        assert_eq!(retry["generation"], receipt["generation"]);
        f.settled(1).await;
        let after = f.reload().await;
        f.assert_context(&after);
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|message| message.id == receipt["receipt_id"].as_str().unwrap())
                .count(),
            1
        );
        assert_eq!(f.probe.requests.lock().unwrap().len(), 1);
        f.state.shutdown().await;
    }
}
