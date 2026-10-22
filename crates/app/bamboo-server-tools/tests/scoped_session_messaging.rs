//! Supported complete-send routes over temporary canonical Inbox data only.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    ExecutingSupervisorObservation, Tool, ToolClass, ToolCtx, ToolExecutionContext, ToolExecutor,
    ToolOutcome,
};
use bamboo_agent_core::{FunctionCall, Session, Storage, ToolCall};
use bamboo_domain::{
    SessionActivationDisposition, SessionActivationError, SessionActivationPort,
    SessionCallerReference, SessionInboxLimits, SessionInboxPort, SessionMessageDeliveryIntent,
    SessionMessageSource, SupervisorManagementMutation as Mutation, SupervisorManagementRequest,
    SupervisorReference,
};
use bamboo_engine::{SessionMessenger, SessionPeerDeliveryReceipt};
use bamboo_server_tools::scoped_session_messaging::{
    CanonicalSessionSendAdapter, ScopedSessionSendError, ScopedSessionSendPort,
    ScopedSessionSendRequest,
};
use bamboo_server_tools::{OverlayToolExecutor, SessionMessageTool};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use bamboo_tools::permission::{
    ConfigPermissionChecker, PermissionConfig, PermissionRule, PermissionType,
};
use serde_json::{json, Value};

#[derive(Default)]
struct Activation {
    fail: AtomicBool,
    requests: tokio::sync::Mutex<Vec<(String, u64)>>,
}

#[async_trait]
impl SessionActivationPort for Activation {
    async fn request_activation(
        &self,
        id: &str,
        generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        self.requests.lock().await.push((id.into(), generation));
        if self.fail.load(Ordering::SeqCst) {
            Err(SessionActivationError::Internal(
                "private provider address must remain private".into(),
            ))
        } else {
            Ok(SessionActivationDisposition::ActiveNotified)
        }
    }
}

struct Fixture {
    _home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    inbox: Arc<FileSessionInbox>,
    activation: Arc<Activation>,
    messenger: Arc<SessionMessenger>,
    root: Session,
    sender: Session,
    recipient: Session,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let mut root = Session::new("same-tree-root", "model");
        root.set_project_id_meta("project-a");
        let sender = Session::new_child_of("same-tree-sender", &root, "model", "sender");
        let recipient = Session::new_child_of("same-tree-recipient", &root, "model", "recipient");
        for session in [&root, &sender, &recipient] {
            store.save_session(session).await.unwrap();
        }
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
        Self {
            _home: home,
            store,
            inbox,
            activation,
            messenger,
            root,
            sender,
            recipient,
        }
    }

    fn ctx(session: &Session) -> ToolCtx {
        let mut ctx = ToolCtx::none("scoped-send-fixture");
        ctx.session_id = Some(Arc::from(session.id.as_str()));
        ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(session)));
        ctx.executing_supervisor =
            ExecutingSupervisorObservation::capture_from_executing_session(session);
        ctx
    }

    fn tool(&self) -> SessionMessageTool {
        SessionMessageTool::new(self.messenger.clone()).with_scoped_sender(Arc::new(
            CanonicalSessionSendAdapter::new(self.store.clone(), self.messenger.clone()),
        ))
    }

    async fn new_root(&self, id: &str) -> Session {
        let mut target = Session::new(id, "model");
        target.set_project_id_meta("project-a");
        self.store.save_session(&target).await.unwrap();
        target
    }

    async fn supervisor(&self, targets: &[&str]) -> Session {
        let receipt = self
            .store
            .get_or_create_default_supervisor("fixture-model")
            .await
            .unwrap();
        let reference: SupervisorReference = (&receipt).into();
        let mut mutations = vec![Mutation::ConfigureProjectScope {
            allowed_projects: ["project-a".parse().unwrap()].into(),
        }];
        mutations.extend(targets.iter().map(|id| Mutation::Attach {
            target_session_id: (*id).into(),
        }));
        for (revision, mutation) in mutations.into_iter().enumerate() {
            self.store
                .mutate_supervisor_management(&SupervisorManagementRequest {
                    supervisor: reference.clone(),
                    expected_state_revision: revision as u64,
                    mutation,
                })
                .await
                .unwrap();
        }
        self.store
            .load_session(&receipt.session_id)
            .await
            .unwrap()
            .unwrap()
    }
}

fn send(target: &str, operation: &str, intent: Option<&str>) -> Value {
    let mut args = json!({"action":"send","target_session_id":target,"operation_id":operation,"message":"one bounded instruction"});
    if let Some(intent) = intent {
        args["delivery_intent"] = json!(intent);
    }
    args
}

fn result(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("send is an admission receipt")
    };
    serde_json::from_str(&result.result).unwrap()
}

#[tokio::test]
async fn observed_peer_entry_rejects_stale_original_before_canonical_admission() {
    let fixture = Fixture::new().await;
    let mut stale = SessionCallerReference::from(&fixture.sender);
    stale.created_at -= chrono::Duration::seconds(1);
    assert!(fixture
        .messenger
        .send_peer_observed(
            &stale,
            &fixture.recipient.id,
            "operation",
            "text",
            Some(SessionMessageDeliveryIntent::TriggerTurn)
        )
        .await
        .is_err());
    assert_eq!(
        fixture
            .inbox
            .inspect(&fixture.recipient.id)
            .await
            .unwrap()
            .generation,
        0
    );
    assert!(fixture.activation.requests.lock().await.is_empty());
    let current = SessionCallerReference::from(&fixture.sender);
    let receipt = fixture
        .messenger
        .send_peer_observed(
            &current,
            &fixture.recipient.id,
            "operation",
            "text",
            Some(SessionMessageDeliveryIntent::TriggerTurn),
        )
        .await
        .unwrap();
    assert_eq!(receipt.delivery.generation, 1);
    assert_eq!(
        fixture
            .inbox
            .claim(&fixture.recipient.id, 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn scoped_peer_send_preserves_intent_exact_receipt_and_original_source() {
    for intent in [None, Some("queue_only"), Some("trigger_turn")] {
        let fixture = Fixture::new().await;
        let tool = fixture.tool();
        let args = send(&fixture.recipient.id, "operation", intent);
        let first = result(
            tool.invoke(args.clone(), Fixture::ctx(&fixture.sender))
                .await
                .unwrap(),
        );
        let retry = result(
            tool.invoke(args.clone(), Fixture::ctx(&fixture.sender))
                .await
                .unwrap(),
        );
        assert_eq!(first["admitted"], true);
        assert_eq!(first["terminal"], "not_observed");
        assert_eq!(first["message_id"], retry["message_id"]);
        assert_eq!(first["inbox_generation"], retry["inbox_generation"]);
        if intent == Some("queue_only") {
            assert_eq!(first["activation"], "not_requested");
            assert!(fixture.activation.requests.lock().await.is_empty());
        }
        let mut changed = args.clone();
        changed["message"] = json!("changed payload");
        assert!(tool
            .invoke(changed, Fixture::ctx(&fixture.sender))
            .await
            .is_err());
        let mut changed = args;
        changed["delivery_intent"] = json!(if intent == Some("queue_only") {
            "trigger_turn"
        } else {
            "queue_only"
        });
        assert!(tool
            .invoke(changed, Fixture::ctx(&fixture.sender))
            .await
            .is_err());
        let claims = fixture
            .inbox
            .claim(&fixture.recipient.id, 10)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(
            claims[0].envelope.source,
            SessionMessageSource::Session {
                session_id: fixture.sender.id.clone()
            }
        );
        assert_eq!(claims[0].envelope.target_session_id, fixture.recipient.id);
    }
}

#[tokio::test]
async fn original_supervisor_send_uses_its_existing_scoped_followup_writer() {
    let fixture = Fixture::new().await;
    let target = fixture.new_root("independent-worker").await;
    let supervisor = fixture.supervisor(&[&target.id]).await;
    let tool = fixture.tool();
    for intent in [None, Some("queue_only")] {
        let error = tool
            .invoke(
                send(&target.id, "followup-operation", intent),
                Fixture::ctx(&supervisor),
            )
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("scoped_session_send_intent_unsupported"));
        assert_eq!(
            fixture.inbox.inspect(&target.id).await.unwrap().generation,
            0
        );
    }
    let before =
        serde_json::to_value(fixture.store.load_session(&target.id).await.unwrap()).unwrap();
    let args = send(&target.id, "followup-operation", Some("trigger_turn"));
    let first = result(
        tool.invoke(args.clone(), Fixture::ctx(&supervisor))
            .await
            .unwrap(),
    );
    let retry = result(tool.invoke(args, Fixture::ctx(&supervisor)).await.unwrap());
    assert_eq!(first["admitted"], true);
    assert_eq!(first["message_id"], retry["message_id"]);
    assert_eq!(first["terminal"], "not_observed");
    let claims = fixture.inbox.claim(&target.id, 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0].envelope.source,
        SessionMessageSource::Session {
            session_id: supervisor.id.clone()
        }
    );
    assert_eq!(
        fixture
            .inbox
            .inspect(&target.id)
            .await
            .unwrap()
            .interrupt_generation,
        0
    );
    assert_eq!(
        serde_json::to_value(fixture.store.load_session(&target.id).await.unwrap()).unwrap(),
        before
    );
}

#[tokio::test]
async fn same_project_and_being_supervised_do_not_grant_an_ordinary_root_cross_root_send() {
    let fixture = Fixture::new().await;
    let other = fixture.new_root("other-independent-root").await;
    let supervisor = fixture.supervisor(&[&fixture.root.id, &other.id]).await;
    let tool = fixture.tool();
    let args = send(&other.id, "not-authorized", Some("trigger_turn"));
    assert!(tool
        .invoke(args.clone(), Fixture::ctx(&fixture.root))
        .await
        .is_err());
    let mut forged = Fixture::ctx(&fixture.root);
    forged.executing_supervisor =
        ExecutingSupervisorObservation::capture_from_executing_session(&supervisor);
    assert!(tool.invoke(args.clone(), forged).await.is_err());
    let mut impersonation = args;
    impersonation["source_session_id"] = json!(supervisor.id);
    assert!(tool
        .invoke(impersonation, Fixture::ctx(&fixture.root))
        .await
        .is_err());
    assert_eq!(
        fixture.inbox.inspect(&other.id).await.unwrap().generation,
        0
    );
    assert!(fixture.activation.requests.lock().await.is_empty());
}

#[tokio::test]
async fn replaced_caller_and_changed_tool_revision_do_not_reconstruct_original_authority() {
    let fixture = Fixture::new().await;
    let old = Fixture::ctx(&fixture.root);
    let mut changed = fixture.root.clone();
    changed.set_root_orchestration_only(true).unwrap();
    fixture.store.save_session(&changed).await.unwrap();
    let error = fixture
        .tool()
        .invoke(
            send(&fixture.recipient.id, "old-root", Some("trigger_turn")),
            old,
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("scoped_session_send_caller_changed"));
    let old = Fixture::ctx(&fixture.sender);
    fixture
        .store
        .delete_session(&fixture.sender.id)
        .await
        .unwrap();
    let mut replacement =
        Session::new_child_of(fixture.sender.id.clone(), &changed, "model", "replacement");
    replacement.created_at = fixture.sender.created_at + chrono::Duration::seconds(1);
    fixture.store.save_session(&replacement).await.unwrap();
    let error = fixture
        .tool()
        .invoke(
            send(&fixture.recipient.id, "old-child", Some("trigger_turn")),
            old,
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("scoped_session_send_caller_changed"));
    assert_eq!(
        fixture
            .inbox
            .inspect(&fixture.recipient.id)
            .await
            .unwrap()
            .generation,
        0
    );
}

struct RejectingHost {
    calls: AtomicUsize,
    uncertain: bool,
}

#[async_trait]
impl ScopedSessionSendPort for RejectingHost {
    async fn send(
        &self,
        _ctx: &ToolCtx,
        _request: ScopedSessionSendRequest,
    ) -> Result<SessionPeerDeliveryReceipt, ScopedSessionSendError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(if self.uncertain {
            ScopedSessionSendError::OutcomeUncertain
        } else {
            ScopedSessionSendError::AuthorityUnavailable
        })
    }
}

#[tokio::test]
async fn missing_original_or_denied_host_never_falls_back_to_legacy_peer_delivery() {
    let fixture = Fixture::new().await;
    let host = Arc::new(RejectingHost {
        calls: AtomicUsize::new(0),
        uncertain: false,
    });
    let tool = SessionMessageTool::new(fixture.messenger.clone()).with_scoped_sender(host.clone());
    let args = send(&fixture.recipient.id, "operation", Some("trigger_turn"));
    let mut missing = Fixture::ctx(&fixture.sender);
    missing.executing_caller = None;
    assert!(tool.invoke(args.clone(), missing).await.is_err());
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
    let error = tool
        .invoke(args, Fixture::ctx(&fixture.sender))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("scoped_session_send_authority_unavailable"));
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .inbox
            .inspect(&fixture.recipient.id)
            .await
            .unwrap()
            .generation,
        0
    );
    assert!(fixture.activation.requests.lock().await.is_empty());
}

#[tokio::test]
async fn scoped_send_preserves_durable_receipt_when_canonical_activation_fails() {
    let fixture = Fixture::new().await;
    fixture.activation.fail.store(true, Ordering::SeqCst);
    let tool = fixture.tool();
    let args = send(&fixture.recipient.id, "operation", Some("trigger_turn"));
    let first = result(
        tool.invoke(args.clone(), Fixture::ctx(&fixture.sender))
            .await
            .unwrap(),
    );
    assert_eq!(first["accepted"], true);
    assert_eq!(first["admitted"], true);
    assert_eq!(first["activation"], "activation_pending");
    assert_eq!(first["terminal"], "not_observed");
    assert!(!first.to_string().contains("private provider address"));
    assert_eq!(
        fixture
            .inbox
            .inspect(&fixture.recipient.id)
            .await
            .unwrap()
            .pending,
        1
    );
    fixture.activation.fail.store(false, Ordering::SeqCst);
    let retry = result(
        tool.invoke(args, Fixture::ctx(&fixture.sender))
            .await
            .unwrap(),
    );
    assert_eq!(first["message_id"], retry["message_id"]);
    assert_eq!(first["inbox_generation"], retry["inbox_generation"]);
    assert_eq!(
        fixture
            .inbox
            .inspect(&fixture.recipient.id)
            .await
            .unwrap()
            .pending,
        1
    );
}

#[tokio::test]
async fn scoped_send_remains_mutating_permission_gated_and_plan_refused() {
    let fixture = Fixture::new().await;
    let host = Arc::new(RejectingHost {
        calls: AtomicUsize::new(0),
        uncertain: false,
    });
    let tool = Arc::new(
        SessionMessageTool::new(fixture.messenger.clone()).with_scoped_sender(host.clone()),
    );
    let args = send(&fixture.recipient.id, "operation", Some("trigger_turn"));
    assert_eq!(tool.classify(&args), ToolClass::MUTATING_SERIAL);
    let permissions = bamboo_tools::permission::check_permissions("session_message", &args)
        .unwrap()
        .unwrap();
    assert_eq!(
        permissions[0].permission_type,
        PermissionType::ExecuteCommand
    );
    assert_eq!(permissions[0].resource, "session_message send");
    let mut plan = Fixture::ctx(&fixture.sender);
    plan.plan_read_only = true;
    assert!(tool.invoke(args.clone(), plan).await.is_err());
    let config = Arc::new(PermissionConfig::new());
    config.add_rule(PermissionRule::new(
        PermissionType::ExecuteCommand,
        "session_message send",
        false,
    ));
    let overlay = OverlayToolExecutor::new(
        Arc::new(bamboo_tools::BuiltinToolExecutor::new_with_permissions(
            Arc::new(ConfigPermissionChecker::new(config)),
        )),
        tool.clone(),
    );
    let call = ToolCall {
        id: "denied-send".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "session_message".into(),
            arguments: args.to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&fixture.sender.id);
    assert!(overlay.execute_with_context(&call, ctx).await.is_err());
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .inbox
            .inspect(&fixture.recipient.id)
            .await
            .unwrap()
            .generation,
        0
    );
    let schema = tool.parameters_schema();
    let normalized =
        bamboo_llm::providers::common::tool_schema::sanitize_openai_function_parameters_schema(
            &schema,
        );
    assert_eq!(normalized["properties"], schema["properties"]);
    assert_eq!(normalized["properties"]["message"]["maxLength"], 16384);
}

#[tokio::test]
async fn unknown_outcome_and_invalid_request_do_not_mint_admission_or_a_new_operation() {
    let fixture = Fixture::new().await;
    let host = Arc::new(RejectingHost {
        calls: AtomicUsize::new(0),
        uncertain: true,
    });
    let tool = SessionMessageTool::new(fixture.messenger.clone()).with_scoped_sender(host.clone());
    let args = send(
        &fixture.recipient.id,
        "same-operation",
        Some("trigger_turn"),
    );
    let error = tool
        .invoke(args.clone(), Fixture::ctx(&fixture.sender))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("scoped_session_send_outcome_uncertain"));
    assert!(!error.to_string().contains("admitted=false"));
    assert_eq!(args["operation_id"], "same-operation");
    for (field, value) in [
        ("message", json!("x".repeat(16385))),
        ("message", json!("nul\0text")),
        ("target_session_id", json!("../escape")),
        ("operation_id", json!("")),
    ] {
        let mut invalid = args.clone();
        invalid[field] = value;
        assert!(tool
            .invoke(invalid, Fixture::ctx(&fixture.sender))
            .await
            .is_err());
    }
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .inbox
            .inspect(&fixture.recipient.id)
            .await
            .unwrap()
            .generation,
        0
    );
}
