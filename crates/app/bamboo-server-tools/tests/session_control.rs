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
    assert_eq!(schema["properties"].as_object().unwrap().len(), 4);
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
