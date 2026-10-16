use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    ExecutingSupervisorObservation, Tool, ToolClass, ToolCtx, ToolOutcome,
};
use bamboo_domain::{
    Message, Session, SessionActivationDisposition, SessionActivationError, SessionActivationPort,
    SessionInboxLimits, SessionInboxPort, SessionMessageBody, SessionMessageContent,
    SessionMessageKind, SessionMessageSource, Storage, SupervisorReference,
};
use bamboo_engine::{session_app::supervisor::SupervisorSessionService, SessionMessenger};
use bamboo_server_tools::{SessionControlTool, SessionInspectorTool};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use serde_json::{json, Value};

#[derive(Default)]
struct Activation {
    fail: AtomicBool,
    calls: tokio::sync::Mutex<Vec<(String, u64)>>,
}

#[async_trait]
impl SessionActivationPort for Activation {
    async fn request_activation(
        &self,
        target: &str,
        generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        self.calls.lock().await.push((target.into(), generation));
        if self.fail.load(Ordering::SeqCst) {
            Err(SessionActivationError::Internal(
                "test activation unavailable".into(),
            ))
        } else {
            Ok(SessionActivationDisposition::ActivationReserved)
        }
    }
}

struct Fixture {
    home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    inbox: Arc<FileSessionInbox>,
    activation: Arc<Activation>,
    messenger: Arc<SessionMessenger>,
    tool: SessionControlTool,
    service: SupervisorSessionService,
    supervisor: SupervisorReference,
    ctx: ToolCtx,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let service = SupervisorSessionService::new(store.clone());
        let supervisor: SupervisorReference = (&service
            .get_or_create_default("supervisor-model")
            .await
            .unwrap())
            .into();
        for (id, project) in [("a", "project-a"), ("b", "project-b"), ("c", "project-c")] {
            let mut target = Session::new(id, format!("{id}-original-model"));
            target.set_project_id_meta(project);
            target.set_workspace_path_meta(format!("/original/{id}"));
            target.metadata_version = 3;
            target.add_message(Message::user(format!("{id} original context")));
            store.save_session(&target).await.unwrap();
        }
        service
            .configure_project_scope(
                &supervisor,
                0,
                ["project-a", "project-b"]
                    .into_iter()
                    .map(|p| p.parse().unwrap())
                    .collect(),
            )
            .await
            .unwrap();
        service.attach(&supervisor, 1, "a").await.unwrap();
        service.attach(&supervisor, 2, "b").await.unwrap();
        let executing = store
            .load_session(&supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        let mut ctx = ToolCtx::none("followup-call");
        ctx.session_id = Some(Arc::from(executing.id.as_str()));
        ctx.executing_supervisor =
            ExecutingSupervisorObservation::capture_from_executing_session(&executing);
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let activation = Arc::new(Activation::default());
        let messenger = Arc::new(SessionMessenger::new(
            store.clone(),
            inbox.clone(),
            activation.clone(),
        ));
        let tool = SessionControlTool::new(messenger.clone());
        Self {
            home,
            store,
            inbox,
            activation,
            messenger,
            tool,
            service,
            supervisor,
            ctx,
        }
    }

    async fn invoke(&self, target: &str, operation: &str, message: &str) -> Value {
        completed(
            self.tool
                .invoke(args(target, operation, message), self.ctx.clone())
                .await
                .unwrap(),
        )
    }
}

fn args(target: &str, operation: &str, message: &str) -> Value {
    json!({"action":"followup", "target_session_id":target, "operation_id":operation, "message":message})
}

fn completed(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("expected synchronous receipt")
    };
    assert!(result.success, "{}", result.result);
    serde_json::from_str(&result.result).unwrap()
}

#[tokio::test]
async fn supervisor_followup_preserves_two_roots_and_rejects_unattached_c() {
    let f = Fixture::new().await;
    assert_eq!(
        f.tool.classify(&args("a", "op", "continue")),
        ToolClass::MUTATING_SERIAL
    );
    let schema = f.tool.parameters_schema();
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"].as_object().unwrap().len(), 7);
    assert!(bamboo_domain::ClassifiedToolIdentity::from_schema_name("session_control").is_some());
    for id in ["a", "b"] {
        let before =
            serde_json::to_value(f.store.load_session(id).await.unwrap().unwrap()).unwrap();
        let result = f
            .invoke(id, "operation-1", "Continue your original task")
            .await;
        assert_eq!(result["admitted"], true);
        assert_eq!(result["generation"], 1);
        let claims = f.inbox.claim(id, 10).await.unwrap();
        assert_eq!(claims.len(), 1);
        let envelope = &claims[0].envelope;
        assert_eq!(
            envelope.source,
            SessionMessageSource::Session {
                session_id: f.supervisor.session_id.clone()
            }
        );
        assert_eq!(envelope.kind, SessionMessageKind::PeerMessage);
        assert_eq!(
            envelope.body,
            SessionMessageBody::Content(SessionMessageContent::text("Continue your original task"))
        );
        assert_eq!(
            before,
            serde_json::to_value(f.store.load_session(id).await.unwrap().unwrap()).unwrap()
        );
        assert_eq!(f.inbox.inspect(id).await.unwrap().interrupt_generation, 0);
    }
    assert!(f
        .tool
        .invoke(args("c", "operation-1", "not authorized"), f.ctx.clone())
        .await
        .is_err());
    assert_eq!(f.inbox.inspect("c").await.unwrap().generation, 0);
    assert_eq!(f.activation.calls.lock().await.len(), 2);
}

#[tokio::test]
async fn supervisor_followup_exact_retry_survives_activation_failure_reopen_and_ack() {
    let f = Fixture::new().await;
    f.activation.fail.store(true, Ordering::SeqCst);
    let pending = f.invoke("a", "operation-1", "Continue").await;
    assert_eq!(pending["admitted"], true);
    assert_eq!(pending["activation"], "activation_pending");
    assert_eq!(f.inbox.inspect("a").await.unwrap().pending, 1);
    f.activation.fail.store(false, Ordering::SeqCst);
    let store = Arc::new(
        SessionStoreV2::new(f.home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let inbox = Arc::new(FileSessionInbox::new(
        store.clone(),
        SessionInboxLimits::default(),
    ));
    let tool = SessionControlTool::new(Arc::new(SessionMessenger::new(
        store.clone(),
        inbox.clone(),
        f.activation.clone(),
    )));
    let retry = completed(
        tool.invoke(args("a", "operation-1", "Continue"), f.ctx.clone())
            .await
            .unwrap(),
    );
    assert_eq!(retry["receipt_id"], pending["receipt_id"]);
    assert_eq!(retry["generation"], pending["generation"]);
    let claim = inbox.claim("a", 10).await.unwrap().remove(0);
    let mut target = store.load_session("a").await.unwrap().unwrap();
    target.add_message(claim.envelope.to_provider_message().unwrap());
    store.save_session(&target).await.unwrap();
    inbox.ack("a", &claim).await.unwrap();
    let after_ack = completed(
        tool.invoke(args("a", "operation-1", "Continue"), f.ctx.clone())
            .await
            .unwrap(),
    );
    assert_eq!(after_ack["receipt_id"], pending["receipt_id"]);
    assert_eq!(inbox.inspect("a").await.unwrap().pending, 0);
    assert!(tool
        .invoke(args("a", "operation-1", "Different payload"), f.ctx.clone())
        .await
        .is_err());
    let history = SessionInspectorTool::new(store.clone(), store.clone());
    let result = history
        .invoke(
            json!({"action":"read_messages", "session_id":"a", "limit":10, "from_end":true}),
            f.ctx.clone(),
        )
        .await
        .unwrap()
        .into_tool_result();
    assert!(result.success);
    assert!(result.result.contains("a original context"));
    assert_eq!(
        store
            .load_session("a")
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.id == claim.envelope.id.as_str())
            .count(),
        1
    );
}

#[tokio::test]
async fn supervisor_followup_duplicate_calls_admit_one_semantic_message() {
    let f = Fixture::new().await;
    let call = args("a", "same-operation", "Continue");
    let (first, second) = tokio::join!(
        f.tool.invoke(call.clone(), f.ctx.clone()),
        f.tool.invoke(call, f.ctx.clone())
    );
    assert_eq!(
        completed(first.unwrap())["receipt_id"],
        completed(second.unwrap())["receipt_id"]
    );
    assert_eq!(f.inbox.inspect("a").await.unwrap().pending, 1);
    assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 1);
}

#[tokio::test]
async fn supervisor_followup_rejects_forged_missing_ordinary_child_plan_and_stale_callers() {
    let f = Fixture::new().await;
    let valid = args("a", "op", "Continue");
    for field in [
        "source_session_id",
        "supervisor",
        "incarnation_id",
        "bypass_permissions",
    ] {
        let mut forged = valid.clone();
        forged[field] = json!("forged");
        assert!(
            f.tool.invoke(forged, f.ctx.clone()).await.is_err(),
            "{field}"
        );
    }
    for caller in [None, Some("a"), Some("child-a")] {
        let mut ctx = f.ctx.clone();
        ctx.session_id = caller.map(Arc::from);
        assert!(f.tool.invoke(valid.clone(), ctx).await.is_err());
    }
    let mut missing = f.ctx.clone();
    missing.executing_supervisor = None;
    assert!(f.tool.invoke(valid.clone(), missing).await.is_err());
    for bypass in [false, true] {
        let mut plan = f.ctx.clone();
        plan.plan_read_only = true;
        plan.bypass_permissions = bypass;
        plan.auto_approve_permissions = bypass;
        assert!(f.tool.invoke(valid.clone(), plan).await.is_err());
    }
    f.store
        .delete_session(&f.supervisor.session_id)
        .await
        .unwrap();
    f.service
        .get_or_create_default("replacement")
        .await
        .unwrap();
    assert!(f.tool.invoke(valid, f.ctx.clone()).await.is_err());
    assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
    assert!(f.activation.calls.lock().await.is_empty());
}

#[tokio::test]
async fn supervisor_followup_validates_target_operation_and_utf8_bounds() {
    let f = Fixture::new().await;
    for operation in ["", " padded", "path/key", "..", &"x".repeat(257)] {
        assert!(f
            .tool
            .invoke(args("a", operation, "Continue"), f.ctx.clone())
            .await
            .is_err());
    }
    for message in [" ".to_string(), "界".repeat(6000)] {
        assert!(f
            .tool
            .invoke(args("a", "op", &message), f.ctx.clone())
            .await
            .is_err());
    }
    for target in ["", "../a", &f.supervisor.session_id] {
        assert!(f
            .tool
            .invoke(args(target, "op", "Continue"), f.ctx.clone())
            .await
            .is_err());
    }
    assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
    // Ordinary messenger admission retains its existing cross-Root rejection.
    let mut peer = bamboo_domain::SessionMessageEnvelope::user_input("a", "Continue");
    peer.source = SessionMessageSource::Session {
        session_id: f.supervisor.session_id.clone(),
    };
    peer.kind = SessionMessageKind::PeerMessage;
    assert!(f.messenger.send(peer).await.is_err());
}

mod loop_adapter_tests {
    use super::*;
    use bamboo_agent_core::tools::ToolError;
    use bamboo_tools::permission::{permission_replay_active, with_permission_replay_generation};

    const OPERATION: &str = "a4b85b5b-ba44-488d-9900-20dd7e016e88";
    const GENERATION: &str = "b23bc011-5424-4b5e-8fc9-3c861b89874b";

    fn status() -> Value {
        json!({"action":"loop","target_session_id":"a","command":{"action":"status"}})
    }

    fn start() -> Value {
        json!({
            "action":"loop", "target_session_id":"a", "expected_link_revision":2,
            "command":{"action":"start", "operation_id":OPERATION,
                "expected_revision":0, "objective":"  target objective  ",
                "max_iterations":2, "max_total_rounds":6}
        })
    }

    fn stop() -> Value {
        json!({
            "action":"loop", "target_session_id":"a", "expected_link_revision":2,
            "command":{"action":"stop", "operation_id":OPERATION,
                "generation":GENERATION, "expected_revision":1}
        })
    }

    fn update() -> Value {
        json!({
            "action":"loop", "target_session_id":"a", "expected_link_revision":2,
            "command":{"action":"update", "operation_id":OPERATION,
                "generation":GENERATION, "expected_revision":1, "max_total_rounds":4}
        })
    }

    #[tokio::test]
    async fn loop_schema_reuses_strict_bounded_command_and_keeps_every_action_serial() {
        let f = Fixture::new().await;
        let schema = f.tool.parameters_schema();
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["action", "target_session_id"]));
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!(["followup", "loop"])
        );
        assert_eq!(schema["properties"].as_object().unwrap().len(), 7);
        let command = &schema["properties"]["command"];
        assert_eq!(command["additionalProperties"], false);
        assert_eq!(command["required"], json!(["action"]));
        assert_eq!(command["properties"]["max_iterations"]["maximum"], 100);
        assert_eq!(command["properties"]["max_total_rounds"]["maximum"], 1000);
        assert!(schema.get("oneOf").is_none());
        assert!(command.get("oneOf").is_none());
        assert!(command["properties"].get("target_session_id").is_none());
        assert!(command["properties"].get("session_id").is_none());
        assert_eq!(schema["properties"]["expected_link_revision"]["minimum"], 1);
        assert_eq!(schema["properties"]["message"]["maxLength"], 16384);
        for arguments in [
            status(),
            start(),
            update(),
            stop(),
            json!({"action":"loop"}),
            args("a", "legacy-op", "Continue"),
        ] {
            assert_eq!(f.tool.classify(&arguments), ToolClass::MUTATING_SERIAL);
        }
    }

    #[tokio::test]
    async fn loop_rejects_unknown_fields_invalid_links_and_nested_commands_before_delivery() {
        let f = Fixture::new().await;
        let mut invalid = Vec::new();
        for field in [
            "supervisor",
            "source_session_id",
            "incarnation_id",
            "host_grant",
            "project_id",
            "operation_id",
            "message",
        ] {
            let mut arguments = status();
            arguments[field] = json!("untrusted");
            invalid.push(arguments);
        }
        for field in ["target_session_id", "caller", "host_grant"] {
            let mut arguments = status();
            arguments["command"][field] = json!("untrusted");
            invalid.push(arguments);
        }
        for target in ["", "../a", bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID] {
            let mut arguments = status();
            arguments["target_session_id"] = json!(target);
            invalid.push(arguments);
        }
        for mut missing_link in [start(), update(), stop()] {
            missing_link
                .as_object_mut()
                .unwrap()
                .remove("expected_link_revision");
            invalid.push(missing_link);
        }
        for mut arguments in [status(), start(), update(), stop()] {
            arguments["expected_link_revision"] = json!(0);
            invalid.push(arguments);
        }
        let mut invalid_uuid = start();
        invalid_uuid["command"]["operation_id"] = json!("not-a-uuid");
        invalid.push(invalid_uuid);
        let mut invalid_bound = start();
        invalid_bound["command"]["max_iterations"] = json!(101);
        invalid.push(invalid_bound);
        let mut invalid_objective = start();
        invalid_objective["command"]["objective"] = json!("界".repeat(5500));
        invalid.push(invalid_objective);
        let mut receipt_on_mutation = stop();
        receipt_on_mutation["receipt_operation_id"] = json!(OPERATION);
        invalid.push(receipt_on_mutation);
        let mut malformed_receipt = status();
        malformed_receipt["receipt_operation_id"] = json!("not-a-uuid");
        invalid.push(malformed_receipt);
        let mut followup_with_loop_field = args("a", "legacy-op", "Continue");
        followup_with_loop_field["command"] = json!({"action":"status"});
        invalid.push(followup_with_loop_field);
        for arguments in invalid {
            assert!(
                matches!(
                    f.tool.invoke(arguments.clone(), f.ctx.clone()).await,
                    Err(ToolError::InvalidArguments(_))
                ),
                "{arguments}"
            );
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn loop_original_identity_and_plan_denials_survive_bypass_and_auto() {
        let f = Fixture::new().await;
        let ordinary = Session::new("a", "model");
        let child = Session::new_child("child-a", "a", "model", "child");
        let impostor = Session::new(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID, "model");
        let mut ultra = Session::new("ordinary-ultra", "model");
        ultra.set_root_orchestration_only(true).unwrap();
        for session in [ordinary, child, impostor, ultra] {
            let mut ctx = ToolCtx::none("loop-role-check");
            ctx.session_id = Some(Arc::from(session.id.as_str()));
            ctx.executing_supervisor =
                ExecutingSupervisorObservation::capture_from_executing_session(&session);
            ctx.bypass_permissions = true;
            ctx.auto_approve_permissions = true;
            assert!(matches!(f.tool.invoke(status(), ctx).await,
                Err(ToolError::Execution(ref error)) if error.contains("original executing Supervisor")));
        }
        let mut wrong_caller = f.ctx.clone();
        wrong_caller.session_id = Some(Arc::from("a"));
        assert!(f.tool.invoke(status(), wrong_caller).await.is_err());
        assert!(f
            .tool
            .invoke(status(), ToolCtx::none("missing"))
            .await
            .is_err());
        for arguments in [status(), start(), update(), stop()] {
            let mut ctx = f.ctx.clone();
            ctx.plan_read_only = true;
            ctx.bypass_permissions = true;
            ctx.auto_approve_permissions = true;
            assert!(matches!(f.tool.invoke(arguments, ctx).await,
                Err(ToolError::Execution(ref error)) if error.contains("Plan mode")));
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn loop_rejects_typed_and_legacy_replay_while_followup_keeps_its_existing_contract() {
        let f = Fixture::new().await;
        let caller = f.ctx.session_id().unwrap();
        let call_id = &f.ctx.tool_call_id;
        for (index, generation) in [Some("generation-1"), None].into_iter().enumerate() {
            with_permission_replay_generation(caller, call_id, generation, async {
                assert!(permission_replay_active(caller, call_id));
                for arguments in [status(), start(), update(), stop()] {
                    assert!(matches!(f.tool.invoke(arguments, f.ctx.clone()).await,
                        Err(ToolError::Execution(ref error)) if error.contains("approval replay")));
                }
            })
            .await;
            assert!(!permission_replay_active(caller, call_id));
            assert_eq!(f.inbox.inspect("a").await.unwrap().generation, index as u64);
            let operation = format!("unchanged-followup-replay-{index}");
            let response = with_permission_replay_generation(
                caller,
                call_id,
                generation,
                f.tool.invoke(
                    args("a", &operation, "Continue your original task"),
                    f.ctx.clone(),
                ),
            )
            .await
            .unwrap();
            let response = completed(response);
            assert_eq!(response["action"], "followup");
            assert_eq!(response["admitted"], true);
            assert_eq!(response["operation_id"], operation);
            assert!(response.get("applied").is_none());
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 2);
        assert_eq!(f.activation.calls.lock().await.len(), 2);
    }
}
