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
        // Advertised per-action acceptance is validated below, not inferred from shape.
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

    fn advertised_validator(fixture: &Fixture) -> jsonschema::Validator {
        let registry = bamboo_agent_core::tools::registry::ToolRegistry::new();
        registry
            .register(SessionControlTool::new(fixture.messenger.clone()))
            .unwrap();
        let advertised = registry
            .list_tools()
            .into_iter()
            .find(|schema| schema.function.name == "session_control")
            .unwrap();
        assert_eq!(
            advertised.function.parameters,
            fixture.tool.parameters_schema()
        );
        jsonschema::validator_for(&advertised.function.parameters).unwrap()
    }

    #[tokio::test]
    async fn advertised_followup_schema_requires_its_fields_and_rejects_loop_fields() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let valid = args("a", "schema-followup", "Continue your original task");
        assert!(validator.validate(&valid).is_ok());
        assert_eq!(f.tool.classify(&valid), ToolClass::MUTATING_SERIAL);

        for field in ["action", "target_session_id", "operation_id", "message"] {
            let mut invalid = valid.clone();
            invalid.as_object_mut().unwrap().remove(field);
            assert!(
                validator.validate(&invalid).is_err(),
                "advertised followup accepted missing {field}: {invalid}"
            );
            assert_eq!(f.tool.classify(&invalid), ToolClass::MUTATING_SERIAL);
            assert!(matches!(
                f.tool.invoke(invalid, f.ctx.clone()).await,
                Err(ToolError::InvalidArguments(_))
            ));
        }
        for (field, value) in [
            ("command", json!({"action":"status"})),
            ("expected_link_revision", json!(2)),
            ("receipt_operation_id", json!(OPERATION)),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(
                validator.validate(&invalid).is_err(),
                "advertised followup accepted loop-only {field}: {invalid}"
            );
            assert_eq!(f.tool.classify(&invalid), ToolClass::MUTATING_SERIAL);
            assert!(matches!(
                f.tool.invoke(invalid, f.ctx.clone()).await,
                Err(ToolError::InvalidArguments(_))
            ));
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn advertised_loop_schema_requires_its_fields_and_rejects_followup_fields() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let mut receipt_status = status();
        receipt_status["expected_link_revision"] = json!(2);
        receipt_status["receipt_operation_id"] = json!(OPERATION);
        for valid in [status(), receipt_status, start(), update(), stop()] {
            assert!(
                validator.validate(&valid).is_ok(),
                "advertised schema rejected legal loop structure: {valid}"
            );
            assert_eq!(f.tool.classify(&valid), ToolClass::MUTATING_SERIAL);
        }

        for field in ["action", "target_session_id", "command"] {
            let mut invalid = status();
            invalid.as_object_mut().unwrap().remove(field);
            assert!(
                validator.validate(&invalid).is_err(),
                "advertised loop accepted missing {field}: {invalid}"
            );
            assert_eq!(f.tool.classify(&invalid), ToolClass::MUTATING_SERIAL);
            assert!(matches!(
                f.tool.invoke(invalid, f.ctx.clone()).await,
                Err(ToolError::InvalidArguments(_))
            ));
        }
        for (field, value) in [
            ("operation_id", json!("schema-followup")),
            ("message", json!("Continue your original task")),
        ] {
            let mut invalid = status();
            invalid[field] = value;
            assert!(
                validator.validate(&invalid).is_err(),
                "advertised loop accepted followup-only {field}: {invalid}"
            );
            assert_eq!(f.tool.classify(&invalid), ToolClass::MUTATING_SERIAL);
            assert!(matches!(
                f.tool.invoke(invalid, f.ctx.clone()).await,
                Err(ToolError::InvalidArguments(_))
            ));
        }

        let mut unknown_outer = status();
        unknown_outer["host_grant"] = json!("forged");
        let mut unknown_nested = status();
        unknown_nested["command"]["target_session_id"] = json!("a");
        for invalid in [unknown_outer, unknown_nested] {
            assert!(validator.validate(&invalid).is_err(), "{invalid}");
            assert!(matches!(
                f.tool.invoke(invalid, f.ctx.clone()).await,
                Err(ToolError::InvalidArguments(_))
            ));
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    fn linked_command(command: Value) -> Value {
        json!({
            "action": "loop", "target_session_id": "a", "expected_link_revision": 2,
            "command": command
        })
    }

    fn command_validates(command: &Value) -> bool {
        serde_json::from_value::<bamboo_domain::LoopControlRequest>(command.clone())
            .is_ok_and(|request| request.validate().is_ok())
    }

    async fn assert_loop_schema_case(
        f: &Fixture,
        validator: &jsonschema::Validator,
        arguments: Value,
        valid: bool,
    ) {
        assert_eq!(f.tool.classify(&arguments), ToolClass::MUTATING_SERIAL);
        if valid {
            let request: bamboo_domain::LoopControlRequest =
                serde_json::from_value(arguments["command"].clone()).unwrap();
            request.validate().unwrap();
            assert!(
                validator.validate(&arguments).is_ok(),
                "advertised schema rejected valid wire fields: {arguments}"
            );
            // Derived Serialize emits None as null; preserve that actual DTO
            // roundtrip instead of narrowing nullable patches in the schema.
            let mut roundtrip = arguments.clone();
            roundtrip["command"] = serde_json::to_value(request).unwrap();
            assert!(
                validator.validate(&roundtrip).is_ok(),
                "advertised schema rejected the typed roundtrip: {roundtrip}"
            );
            // This fixture has no live Loop host grant. A legal wire structure
            // reaches that existing service gate; it does not gain authority.
            assert!(matches!(
                f.tool.invoke(arguments, f.ctx.clone()).await,
                Err(ToolError::Execution(_))
            ));
        } else {
            assert!(
                validator.validate(&arguments).is_err(),
                "advertised schema accepted invalid wire fields: {arguments}"
            );
            assert!(matches!(
                f.tool.invoke(arguments, f.ctx.clone()).await,
                Err(ToolError::InvalidArguments(_))
            ));
        }
    }

    async fn assert_nested_command_rejected(
        f: &Fixture,
        validator: &jsonschema::Validator,
        command: Value,
    ) {
        assert!(
            !command_validates(&command),
            "DTO unexpectedly accepted {command}"
        );
        let arguments = linked_command(command);
        assert_eq!(arguments["expected_link_revision"], 2);
        assert_loop_schema_case(f, validator, arguments, false).await;
    }

    #[tokio::test]
    async fn advertised_nested_loop_command_requires_every_action_field() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        // Keep the outer link valid so the real RED isolates missing nested
        // Start fields, rather than an unrelated missing outer revision.
        assert_nested_command_rejected(&f, &validator, json!({"action": "start"})).await;
        for (arguments, required) in [
            (status(), vec!["action"]),
            (
                start(),
                vec![
                    "action",
                    "operation_id",
                    "expected_revision",
                    "objective",
                    "max_iterations",
                    "max_total_rounds",
                ],
            ),
            (
                update(),
                vec!["action", "operation_id", "generation", "expected_revision"],
            ),
            (
                stop(),
                vec!["action", "operation_id", "generation", "expected_revision"],
            ),
        ] {
            let command = arguments["command"].clone();
            for field in required {
                let mut missing = command.clone();
                assert!(missing.as_object_mut().unwrap().remove(field).is_some());
                assert_nested_command_rejected(&f, &validator, missing).await;
            }
            assert_loop_schema_case(&f, &validator, linked_command(command), true).await;
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn advertised_nested_loop_command_rejects_other_action_fields_even_null() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let fields = [
            ("operation_id", json!(OPERATION)),
            ("expected_generation", json!(GENERATION)),
            ("generation", json!(GENERATION)),
            ("expected_revision", json!(0)),
            ("objective", json!("Future objective")),
            ("max_iterations", json!(2)),
            ("max_total_rounds", json!(6)),
        ];
        for (arguments, allowed) in [
            (status(), vec![]),
            (
                start(),
                vec![
                    "operation_id",
                    "expected_generation",
                    "expected_revision",
                    "objective",
                    "max_iterations",
                    "max_total_rounds",
                ],
            ),
            (
                update(),
                vec![
                    "operation_id",
                    "generation",
                    "expected_revision",
                    "objective",
                    "max_iterations",
                    "max_total_rounds",
                ],
            ),
            (
                stop(),
                vec!["operation_id", "generation", "expected_revision"],
            ),
        ] {
            let command = arguments["command"].clone();
            for (field, value) in &fields {
                if allowed.contains(field) {
                    continue;
                }
                for value in [value.clone(), Value::Null] {
                    let mut extra = command.clone();
                    extra[*field] = value;
                    assert_nested_command_rejected(&f, &validator, extra).await;
                }
            }
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn advertised_nested_loop_update_requires_a_real_patch_and_preserves_dto_roundtrips() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let mut empty = update()["command"].clone();
        empty.as_object_mut().unwrap().remove("max_total_rounds");
        assert_nested_command_rejected(&f, &validator, empty.clone()).await;
        let mut all_null = empty.clone();
        for field in ["objective", "max_iterations", "max_total_rounds"] {
            all_null[field] = Value::Null;
        }
        assert_nested_command_rejected(&f, &validator, all_null.clone()).await;
        for (field, value) in [
            ("objective", json!("  Revised objective  ")),
            ("max_iterations", json!(2)),
            ("max_total_rounds", json!(4)),
        ] {
            let mut patched = all_null.clone();
            patched[field] = value;
            // expected_revision is a stateless u64, including zero; actual
            // generation/control CAS is still checked by the owning runner.
            patched["expected_revision"] = json!(0);
            assert_loop_schema_case(&f, &validator, linked_command(patched), true).await;
        }
        for expected_generation in [Value::Null, json!(GENERATION)] {
            let mut command = start()["command"].clone();
            command["expected_generation"] = expected_generation;
            command["operation_id"] = json!("00000000-0000-1000-8000-000000000001");
            assert_loop_schema_case(&f, &validator, linked_command(command), true).await;
        }
        for field in [
            "operation_id",
            "expected_revision",
            "objective",
            "max_iterations",
            "max_total_rounds",
        ] {
            let mut command = start()["command"].clone();
            command[field] = Value::Null;
            assert_nested_command_rejected(&f, &validator, command).await;
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn advertised_loop_outer_revision_and_receipt_conditions_match_nullable_dto_fields() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        for arguments in [start(), update(), stop()] {
            assert!(command_validates(&arguments["command"]));
            let mut missing_link = arguments.clone();
            missing_link
                .as_object_mut()
                .unwrap()
                .remove("expected_link_revision");
            assert_loop_schema_case(&f, &validator, missing_link, false).await;
            for revision in [Value::Null, json!(0)] {
                let mut invalid_link = arguments.clone();
                invalid_link["expected_link_revision"] = revision;
                assert_loop_schema_case(&f, &validator, invalid_link, false).await;
            }
            let mut query_on_mutation = arguments.clone();
            query_on_mutation["receipt_operation_id"] = json!(OPERATION);
            assert_loop_schema_case(&f, &validator, query_on_mutation, false).await;
            let mut no_query = arguments;
            no_query["receipt_operation_id"] = Value::Null;
            assert_loop_schema_case(&f, &validator, no_query, true).await;
        }
        for revision in [None, Some(Value::Null), Some(json!(2))] {
            for receipt in [None, Some(Value::Null), Some(json!(OPERATION))] {
                let mut arguments = status();
                if let Some(revision) = revision.clone() {
                    arguments["expected_link_revision"] = revision;
                }
                if let Some(receipt) = receipt {
                    arguments["receipt_operation_id"] = receipt;
                }
                assert_loop_schema_case(&f, &validator, arguments, true).await;
            }
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    // Use the registered tool's real full validator, and the shared DTO and
    // adapter, rather than duplicating either validation implementation.
    async fn assert_objective_schema_case(
        f: &Fixture,
        validator: &jsonschema::Validator,
        label: &str,
        objective: &str,
        valid: bool,
    ) {
        for mut arguments in [start(), update()] {
            arguments["command"]["objective"] = json!(objective);
            assert_eq!(arguments["expected_link_revision"], 2);
            assert_eq!(f.tool.classify(&arguments), ToolClass::MUTATING_SERIAL);
            let request: bamboo_domain::LoopControlRequest =
                serde_json::from_value(arguments["command"].clone()).unwrap();
            let action = request.action();
            let dimensions = (objective.len(), objective.chars().count());
            assert_eq!(
                request.validate().is_ok(),
                valid,
                "{label}/{action}: DTO acceptance differs, (bytes, chars)={dimensions:?}"
            );
            let outcome = f.tool.invoke(arguments.clone(), f.ctx.clone()).await;
            if valid {
                // The fixture has no live target host grant. Reaching the
                // service gate proves legal wire admission, not application.
                assert!(
                    matches!(outcome, Err(ToolError::Execution(_))),
                    "{label}/{action}: valid wire did not reach the existing service gate"
                );
            } else {
                assert!(
                    matches!(outcome, Err(ToolError::InvalidArguments(_))),
                    "{label}/{action}: invalid objective did not fail before delivery"
                );
            }
            assert_eq!(
                validator.validate(&arguments).is_ok(),
                valid,
                "{label}/{action}: advertised acceptance differs, (bytes, chars)={dimensions:?}"
            );
            if valid {
                let mut roundtrip = arguments;
                roundtrip["command"] = serde_json::to_value(request).unwrap();
                assert_eq!(roundtrip["command"]["objective"].as_str(), Some(objective));
                assert!(
                    validator.validate(&roundtrip).is_ok(),
                    "{label}/{action}: advertised schema rejected the typed roundtrip"
                );
            }
        }
    }

    #[tokio::test]
    async fn advertised_nested_objective_matches_rust_whitespace() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let whitespace = [
            '\u{0009}', '\u{000a}', '\u{000b}', '\u{000c}', '\u{000d}', '\u{0020}', '\u{0085}',
            '\u{00a0}', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}',
            '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200a}', '\u{2028}',
            '\u{2029}', '\u{202f}', '\u{205f}', '\u{3000}',
        ];
        assert!(whitespace.iter().all(|character| character.is_whitespace()));
        for character in whitespace {
            let label = format!("whitespace U+{:04X}", character as u32);
            assert_objective_schema_case(&f, &validator, &label, &character.to_string(), false)
                .await;
        }
        let all_whitespace: String = whitespace.into_iter().collect();
        assert_objective_schema_case(&f, &validator, "empty", "", false).await;
        assert_objective_schema_case(&f, &validator, "all whitespace", &all_whitespace, false)
            .await;
        // Rust White_Space and ECMAScript \s differ at FEFF and NEL.
        // A \S shorthand would reject FEFF, which this DTO preserves.
        assert!(!'\u{feff}'.is_whitespace());
        for (label, objective) in [
            ("FEFF is content", "\u{feff}".to_string()),
            ("NEL around FEFF", "\u{0085}\u{feff}\u{0085}".to_string()),
            (
                "surrounded content",
                format!("{all_whitespace}x{all_whitespace}"),
            ),
        ] {
            assert_objective_schema_case(&f, &validator, label, &objective, true).await;
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn advertised_nested_objective_preserves_update_null() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let mut bound_only = update();
        bound_only["command"]["objective"] = Value::Null;
        assert_loop_schema_case(&f, &validator, bound_only, true).await;
        let mut no_patch = update()["command"].clone();
        for field in ["objective", "max_iterations", "max_total_rounds"] {
            no_patch[field] = Value::Null;
        }
        assert_nested_command_rejected(&f, &validator, no_patch).await;
        let mut invalid_start = start()["command"].clone();
        invalid_start["objective"] = Value::Null;
        assert_nested_command_rejected(&f, &validator, invalid_start).await;
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn advertised_nested_objective_preserves_full_utf8_byte_boundaries() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let limit = bamboo_domain::session::loop_control_request::MAX_OBJECTIVE_BYTES;
        assert_eq!(limit, 16_384);
        for (label, objective, expected_bytes) in [
            ("ASCII at limit", "x".repeat(16_384), 16_384),
            ("5000 CJK", "界".repeat(5000), 15_000),
            ("CJK below limit", "界".repeat(5461), 16_383),
            (
                "CJK plus ASCII at limit",
                format!("{}a", "界".repeat(5461)),
                16_384,
            ),
            ("two-byte at limit", "é".repeat(8192), 16_384),
            ("four-byte at limit", "😀".repeat(4096), 16_384),
            (
                "mixed ASCII and CJK at limit",
                format!("{}{}", "x".repeat(8191), "界".repeat(2731)),
                16_384,
            ),
            (
                "mixed all widths at limit",
                format!("{}abcd", "aé界😀".repeat(1638)),
                16_384,
            ),
        ] {
            assert_eq!(objective.len(), expected_bytes, "{label}");
            assert!(objective.len() <= limit, "{label}");
            assert_objective_schema_case(&f, &validator, label, &objective, true).await;
        }
        assert_eq!(f.inbox.inspect("a").await.unwrap().generation, 0);
        assert!(f.activation.calls.lock().await.is_empty());
    }

    // The advertised schema is a character filter; shared typed validation
    // enforces UTF-8 bytes before delivery. This does not assert byte parity.
    #[tokio::test]
    async fn nested_objective_byte_guard_rejects_oversize_despite_schema_character_filter() {
        let f = Fixture::new().await;
        let validator = advertised_validator(&f);
        let limit = bamboo_domain::session::loop_control_request::MAX_OBJECTIVE_BYTES;
        let before = f.inbox.inspect("a").await.unwrap();
        assert_eq!(
            (before.generation, before.pending, before.claimed),
            (0, 0, 0)
        );
        for (label, objective, advertised_accepts) in [
            ("reported 5500 CJK", "界".repeat(5500), true),
            ("CJK beyond byte limit", "界".repeat(5462), true),
            ("two-byte beyond limit", "é".repeat(8193), true),
            ("four-byte beyond limit", "😀".repeat(4097), true),
            (
                "mixed beyond byte limit",
                format!("{}{}x", "x".repeat(8191), "界".repeat(2731)),
                true,
            ),
            (
                "exact character cap but one extra byte",
                format!("{}é", "x".repeat(16_383)),
                true,
            ),
            (
                "ASCII beyond both limits control",
                "x".repeat(16_385),
                false,
            ),
        ] {
            assert!(!objective.trim().is_empty(), "{label}");
            assert!(objective.len() > limit, "{label}");
            for mut arguments in [start(), update()] {
                arguments["command"]["objective"] = json!(&objective);
                assert_eq!(arguments["expected_link_revision"], 2);
                assert_eq!(f.tool.classify(&arguments), ToolClass::MUTATING_SERIAL);
                let request: bamboo_domain::LoopControlRequest =
                    serde_json::from_value(arguments["command"].clone()).unwrap();
                let action = request.action();
                // This remains the existing registered full validator. The
                // advertised character bound cannot assert a UTF-8 byte bound.
                assert_eq!(
                    validator.validate(&arguments).is_ok(),
                    advertised_accepts,
                    "{label}/{action}: unexpected advertised acceptance"
                );
                assert!(
                    request.validate().is_err(),
                    "{label}/{action}: the shared DTO must enforce its byte bound"
                );
                assert!(matches!(
                    f.tool.invoke(arguments, f.ctx.clone()).await,
                    Err(ToolError::InvalidArguments(_))
                ));
                let after = f.inbox.inspect("a").await.unwrap();
                assert_eq!(
                    (after.generation, after.pending, after.claimed),
                    (before.generation, before.pending, before.claimed),
                    "{label}/{action}: rejected objective changed Inbox admission"
                );
                assert!(f.activation.calls.lock().await.is_empty());
            }
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
