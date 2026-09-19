//! Protected approval transport exercises native parking and actual service writes.

use super::*;
use bamboo_agent_core::tools::ExecutingSupervisorObservation;
use bamboo_engine::session_app::supervisor::SupervisorSessionService;

const TARGET: &str = "transport-target";
const AUTHORITY: &str = ExecutingSupervisorObservation::PERMISSION_REPLAY_METADATA_KEY;
type WriteBarrier = (
    tokio::sync::mpsc::UnboundedSender<ExecutingSupervisorObservation>,
    Arc<tokio::sync::Semaphore>,
);

#[path = "approval_supervisor_process_tests.rs"]
mod process;

struct ProtectedTool {
    name: String,
    service: SupervisorSessionService,
    probe: Arc<Probe>,
    config: Arc<PermissionConfig>,
    second_context: bool,
    barrier: Option<WriteBarrier>,
}

#[async_trait]
impl Tool for ProtectedTool {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        "Exercise strict Supervisor management after the real permission gate"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"]})
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        assert!(!ctx.bypass_permissions && !ctx.auto_approve_permissions && !ctx.plan_read_only);
        let mut gates = Vec::new();
        if self.name == "execute_command" {
            gates.push((PermissionType::ExecuteCommand, "current-command"));
        }
        if self.second_context {
            gates.push((PermissionType::WriteFile, "second-resource"));
        }
        for (permission_type, resource) in gates {
            match self
                .config
                .evaluate(bamboo_tools::permission::PermissionEvaluation {
                    request_id: ctx.tool_call_id.to_string(),
                    session_id: ctx.session_id.as_ref().unwrap().to_string(),
                    workspace_path: None,
                    tool_name: self.name.clone(),
                    tool_args: args.clone(),
                    permission_type,
                    resource: resource.into(),
                    operation_summary: "protected link operation".into(),
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
                    result.success = true;
                    return Ok(ToolOutcome::Completed(result));
                }
                bamboo_tools::permission::PermissionOutcome::Allow { .. } => {}
                bamboo_tools::permission::PermissionOutcome::Deny { reason, .. } => {
                    return Err(ToolError::Execution(reason.message))
                }
            }
        }
        self.probe.calls.lock().unwrap().push(Invocation {
            name: self.name.clone(),
            arguments: args,
            generation: current_permission_replay_generation(
                ctx.session_id.as_deref().unwrap(),
                &ctx.tool_call_id,
            ),
            flags: ToolExecutionSessionFlags::default(),
            supervisor_absent: ctx.executing_supervisor.is_none(),
        });
        let original = ctx
            .executing_supervisor_for(ctx.session_id.as_deref().unwrap())
            .ok_or_else(|| {
                ToolError::Execution("original Supervisor observation missing".into())
            })?;
        if let Some((entered, release)) = &self.barrier {
            entered.send(original).unwrap();
            release.acquire().await.unwrap().forget();
        }
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

fn protected_agent(fixture: &Fixture) -> Agent {
    protected_agent_with(fixture, alias_config(), &["Bash"], false, None)
}

fn protected_agent_with(
    fixture: &Fixture,
    config: Arc<PermissionConfig>,
    names: &[&str],
    second_context: bool,
    barrier: Option<WriteBarrier>,
) -> Agent {
    let registry = bamboo_tools::ToolRegistry::new();
    for name in names {
        registry
            .register(ProtectedTool {
                name: (*name).into(),
                service: SupervisorSessionService::new(fixture.store.clone()),
                probe: fixture.probe.clone(),
                config: config.clone(),
                second_context,
                barrier: barrier.clone(),
            })
            .unwrap();
    }
    let executor = bamboo_tools::executor::BuiltinToolExecutor::with_registry_and_permissions(
        registry,
        Arc::new(ConfigPermissionChecker::new(config.clone())),
    );
    fixture.agent_with_executor(Some(config), Arc::new(executor), None)
}

async fn protected_pending() -> (Fixture, Session, bamboo_domain::SupervisorReference) {
    protected_pending_with(&["Bash"]).await
}

async fn setup_supervisor(fixture: &Fixture) -> bamboo_domain::SupervisorReference {
    let service = SupervisorSessionService::new(fixture.store.clone());
    let bootstrap = service.get_or_create_default("test-model").await.unwrap();
    let original = (&bootstrap).into();
    // Scope is host configuration. The dispatched test tool can only attach an
    // existing independent Root within this pre-authorized Project.
    let mut target = Session::new(TARGET, "target-model");
    target.set_project_id_meta("transport-project");
    target.add_message(Message::user("independent history"));
    fixture.store.save_session(&target).await.unwrap();
    service
        .configure_project_scope(&original, 0, ["transport-project".parse().unwrap()].into())
        .await
        .unwrap();
    original
}

async fn protected_pending_with(
    names: &[&str],
) -> (Fixture, Session, bamboo_domain::SupervisorReference) {
    let fixture = Fixture::new().await;
    let original = setup_supervisor(&fixture).await;
    let session = fixture.reload(&original.session_id).await;
    *fixture.probe.next_call.lock().unwrap() = Some(alias_call());
    let initial = events(
        protected_agent_with(&fixture, alias_config(), names, false, None)
            .run_stream(session, "attach the permitted Root"),
    )
    .await;
    assert!(initial
        .iter()
        .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
    let mut pending = fixture.reload(&original.session_id).await;
    let record = authority(&pending).clone();
    assert_eq!(
        record["incarnation_id"],
        original.incarnation_id.to_string()
    );
    assert_eq!(
        record["result_message_id"],
        pending.messages[result_index(&pending)].id
    );
    fixture.repo.save(&mut pending).await.unwrap();
    assert_eq!(
        *authority(&fixture.reload(&original.session_id).await),
        record,
        "lifecycle and subsequent save must retain the original host record"
    );
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    (fixture, pending, original)
}

fn result_index(session: &Session) -> usize {
    session
        .messages
        .iter()
        .rposition(|message| message.tool_call_id.as_deref() == Some(CALL))
        .unwrap()
}

fn authority(session: &Session) -> &Value {
    &session.messages[result_index(session)]
        .metadata
        .as_ref()
        .unwrap()[AUTHORITY]
}

#[tokio::test]
async fn supervisor_native_approved_replay_uses_original_identity() {
    let (mut fixture, pending, original) = Box::pin(protected_pending()).await;
    fixture.approve(&current_request(&pending)).await;
    fixture.reopen_store().await;
    let approved = fixture.reload(&original.session_id).await;
    let completed = events(protected_agent(&fixture).resume_stream(approved)).await;
    let scope = SupervisorSessionService::new(fixture.store.clone())
        .inspect_scope(&original)
        .await
        .unwrap();
    let final_session = fixture.reload(&original.session_id).await;
    assert_eq!(
        scope.state_revision, 2,
        "approved native operation must perform the strict durable write; events={completed:?}; result={:?}",
        final_session.messages.iter().rev().find(|message| message.tool_call_id.as_deref() == Some(CALL))
    );
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
    assert!(
        SupervisorSessionService::new(fixture.store.clone())
            .inspect_link(&original, TARGET)
            .await
            .unwrap()
            .authorized
    );
    let completed_again = events(protected_agent(&fixture).resume_stream(final_session)).await;
    assert!(!completed_again
        .iter()
        .any(|event| matches!(event, AgentEvent::ToolLifecycle { .. })));
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn supervisor_replay_rejects_corrupted_native_bindings_before_protected_writes() {
    for case in [
        "record_message",
        "record_session",
        "record_call",
        "record_owner",
        "record_generation",
        "unknown_version",
        "malformed",
        "absent",
        "arguments",
        "name",
        "request_owner",
        "receipt_generation",
        "legacy_with_record",
        "moved_occurrence",
        "duplicate_call",
        "ordinary",
        "child",
        "plan_invalid",
    ] {
        let (fixture, pending, original) = Box::pin(protected_pending()).await;
        let target_before = serde_json::to_value(fixture.reload(TARGET).await).unwrap();
        let mut approved = fixture.approve(&current_request(&pending)).await;
        let index = result_index(&approved);
        let call_index = approved.messages[..index]
            .iter()
            .rposition(|message| message.tool_calls.is_some())
            .unwrap();
        match case {
            "record_message" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY]
                    ["result_message_id"] = json!("moved")
            }
            "record_session" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY]["session_id"] =
                    json!("other-session")
            }
            "record_call" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY]["tool_call"]["id"] =
                    json!("another-call")
            }
            "record_owner" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY]["execution_name"] =
                    json!("execute_command")
            }
            "record_generation" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY]
                    ["request_generation"] = json!("another-generation")
            }
            "unknown_version" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY]["version"] =
                    json!(99)
            }
            "malformed" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY] = Value::Null
            }
            "absent" => {
                approved.messages[index]
                    .metadata
                    .as_mut()
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .remove(AUTHORITY);
            }
            "arguments" => approved.messages[call_index].tool_calls.as_mut().unwrap()[0]
                .function
                .arguments
                .push(' '),
            "name" => {
                approved.messages[call_index].tool_calls.as_mut().unwrap()[0]
                    .function
                    .name = "Write".into()
            }
            "request_owner" => {
                approved.messages[index].metadata.as_mut().unwrap()["permission_request"]
                    ["tool_name"] = json!("Write")
            }
            "receipt_generation" => {
                approved.messages[index].metadata.as_mut().unwrap()["permission_decision_receipt"]
                    ["decision"]["request_generation"] = json!("another-generation")
            }
            "legacy_with_record" => {
                let metadata = approved.messages[index]
                    .metadata
                    .as_mut()
                    .unwrap()
                    .as_object_mut()
                    .unwrap();
                metadata.remove("permission_request");
                metadata.remove("permission_decision_receipt");
                approved
                    .metadata
                    .remove(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);
            }
            "moved_occurrence" => {
                let next_call = approved.messages[call_index].clone();
                let mut moved = approved.messages[index].clone();
                moved.id = "another-real-result-id".into();
                approved.add_message(next_call);
                approved.add_message(moved);
            }
            "duplicate_call" => {
                let calls = approved.messages[call_index].tool_calls.as_mut().unwrap();
                calls.push(calls[0].clone());
            }
            "ordinary" => {
                approved.authority_identity = bamboo_domain::SessionAuthorityIdentity::Ordinary
            }
            "child" => approved.kind = bamboo_domain::SessionKind::Child,
            "plan_invalid" => {
                approved.messages[index].metadata.as_mut().unwrap()[AUTHORITY]["version"] =
                    json!(99);
                let mut disk = fixture.reload(&original.session_id).await;
                disk.agent_runtime_state.as_mut().unwrap().plan_mode = Some(serde_json::from_value(json!({
                    "entered_at":"2026-09-09T00:00:00Z", "pre_permission_mode":"default", "status":"exploring"
                })).unwrap());
                fixture.store.save_session(&disk).await.unwrap();
            }
            _ => unreachable!(),
        }
        let completed = events(protected_agent(&fixture).resume_stream(approved)).await;
        assert_eq!(
            fixture.probe.actions.load(Ordering::SeqCst),
            0,
            "case={case}; events={completed:?}"
        );
        assert_eq!(
            SupervisorSessionService::new(fixture.store.clone())
                .inspect_scope(&original)
                .await
                .unwrap()
                .state_revision,
            1,
            "case={case}"
        );
        assert_eq!(
            serde_json::to_value(fixture.reload(TARGET).await).unwrap(),
            target_before,
            "case={case}"
        );
    }
}

#[tokio::test]
async fn supervisor_typed_response_rejects_payload_authority_without_erasing_evidence() {
    let (fixture, mut pending, original) = Box::pin(protected_pending()).await;
    let index = result_index(&pending);
    let request = current_request(&pending);
    let host_record = pending.messages[index]
        .metadata
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove(AUTHORITY)
        .unwrap();
    let mut payload: Value = serde_json::from_str(&pending.messages[index].content).unwrap();
    payload[AUTHORITY] = host_record;
    pending.messages[index].content = payload.to_string();
    fixture.repo.save(&mut pending).await.unwrap();
    let before = serde_json::to_value(fixture.reload(&original.session_id).await).unwrap();
    let guard = acquire_pending_response_guard(&original.session_id).await;
    let result = submit_pending_permission_response_checked_guarded(
        &fixture.repo,
        RespondInput {
            session_id: original.session_id.clone(),
            user_response: "Approve".into(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        },
        Some(CALL.into()),
        PermissionDecisionReceipt {
            session_id: original.session_id.clone(),
            decision: PermissionDecision {
                request_id: CALL.into(),
                request_generation: request.request_generation,
                decision: PermissionDecisionKind::AllowOnce,
                matcher_id: None,
                expected_policy_revision: Some(request.policy_revision),
                confirm_global: false,
            },
            decided_at: std::time::SystemTime::now().into(),
        },
        &guard,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        serde_json::to_value(fixture.reload(&original.session_id).await).unwrap(),
        before
    );
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn supervisor_shared_validation_rejects_an_old_same_id_occurrence_without_grants() {
    use bamboo_engine::session_app::approval_replay::{
        find_permission_replay_target, restore_permission_replay_authorization,
        validate_permission_replay_authority,
    };
    let (fixture, pending, _) = Box::pin(protected_pending()).await;
    let request = current_request(&pending);
    let mut session = fixture.approve(&request).await;
    let old =
        find_permission_replay_target(&session, CALL, Some(&request.request_generation)).unwrap();
    session.add_message(Message::assistant(
        "new operation",
        Some(vec![alias_call()]),
    ));
    let mut newer = Message::tool_result(CALL, "new waiting occurrence");
    newer.metadata = Some(json!({"permission_request":{"request_generation":"new-generation"}}));
    session.add_message(newer);
    assert!(validate_permission_replay_authority(&session, &old, "Bash").is_err());
    let config = alias_config();
    assert!(restore_permission_replay_authorization(&config, &session, &old, "Bash").is_err());
    assert!(!config.consume_once_for_generation(
        &session.id,
        CALL,
        &request.request_generation,
        PermissionType::ExecuteCommand,
        &request.resource
    ));
}

#[tokio::test]
async fn supervisor_exact_custom_owner_and_builtin_alias_remain_distinct() {
    let (fixture, pending, original) = Box::pin(protected_pending()).await;
    let approved = fixture.approve(&current_request(&pending)).await;
    events(
        protected_agent_with(
            &fixture,
            alias_config(),
            &["Bash", "execute_command"],
            false,
            None,
        )
        .resume_stream(approved),
    )
    .await;
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert_eq!(
        SupervisorSessionService::new(fixture.store.clone())
            .inspect_scope(&original)
            .await
            .unwrap()
            .state_revision,
        1
    );

    let (fixture, pending, original) =
        Box::pin(protected_pending_with(&["Bash", "execute_command"])).await;
    assert_eq!(current_request(&pending).tool_name, "execute_command");
    let approved = fixture.approve(&current_request(&pending)).await;
    events(
        protected_agent_with(
            &fixture,
            alias_config(),
            &["Bash", "execute_command"],
            false,
            None,
        )
        .resume_stream(approved),
    )
    .await;
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 1);
    assert!(
        SupervisorSessionService::new(fixture.store.clone())
            .inspect_link(&original, TARGET)
            .await
            .unwrap()
            .authorized
    );
}

#[tokio::test]
async fn supervisor_sdk_text_answer_cannot_mint_a_typed_receipt() {
    let (fixture, pending, original) = Box::pin(protected_pending()).await;
    let agent = protected_agent(&fixture);
    let answered = agent.answer(&pending.id, "Approve").await.unwrap().session;
    assert!(answered.messages[result_index(&answered)]
        .metadata
        .as_ref()
        .unwrap()
        .get("permission_decision_receipt")
        .is_none());
    events(agent.resume_stream(answered)).await;
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert_eq!(
        SupervisorSessionService::new(fixture.store.clone())
            .inspect_scope(&original)
            .await
            .unwrap()
            .state_revision,
        1
    );
}

#[tokio::test]
async fn supervisor_recreated_after_restoration_is_rejected_at_the_final_writer() {
    let (fixture, pending, original) = Box::pin(protected_pending()).await;
    let target_before = serde_json::to_value(fixture.reload(TARGET).await).unwrap();
    let approved = fixture.approve(&current_request(&pending)).await;
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let stream = protected_agent_with(
        &fixture,
        alias_config(),
        &["Bash"],
        false,
        Some((entered_tx, release.clone())),
    )
    .resume_stream(approved);
    let retained = tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.supervisor_reference(), original);
    assert!(fixture
        .store
        .delete_session(&original.session_id)
        .await
        .unwrap());
    let service = SupervisorSessionService::new(fixture.store.clone());
    let replacement = service.get_or_create_default("replacement").await.unwrap();
    assert_ne!(replacement.incarnation_id, original.incarnation_id);
    release.add_permits(1);
    let completed = events(stream).await;
    assert_eq!(
        fixture.probe.actions.load(Ordering::SeqCst),
        0,
        "events={completed:?}"
    );
    assert_eq!(
        service
            .inspect_scope(&(&replacement).into())
            .await
            .unwrap()
            .state_revision,
        0
    );
    assert_eq!(
        serde_json::to_value(fixture.reload(TARGET).await).unwrap(),
        target_before
    );
}

#[tokio::test]
async fn supervisor_waiting_validates_identity_without_requiring_an_answer() {
    let (fixture, pending, _) = Box::pin(protected_pending()).await;
    let initial_calls = fixture.probe.provider_calls.load(Ordering::SeqCst);
    let waiting = events(protected_agent(&fixture).resume_stream(pending.clone())).await;
    assert!(matches!(
        waiting.as_slice(),
        [AgentEvent::NeedClarification { .. }]
    ));
    for remove_request in [false, true] {
        let mut broken = pending.clone();
        let index = result_index(&broken);
        let metadata = broken.messages[index].metadata.as_mut().unwrap();
        if remove_request {
            metadata
                .as_object_mut()
                .unwrap()
                .remove("permission_request");
            let mut payload: Value = serde_json::from_str(&broken.messages[index].content).unwrap();
            payload
                .as_object_mut()
                .unwrap()
                .remove("permission_request");
            broken.messages[index].content = payload.to_string();
        } else {
            metadata[AUTHORITY]["version"] = json!(99);
        }
        let failed = events(protected_agent(&fixture).resume_stream(broken)).await;
        assert!(failed
            .iter()
            .any(|event| matches!(event, AgentEvent::Error { .. })));
        assert!(!failed
            .iter()
            .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
    }
    assert_eq!(
        fixture.probe.provider_calls.load(Ordering::SeqCst),
        initial_calls
    );
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ordinary_child_and_synthetic_callers_do_not_acquire_supervisor_authority() {
    for child in [false, true] {
        let fixture = Fixture::new().await;
        let original = setup_supervisor(&fixture).await;
        let supervisor = fixture.reload(&original.session_id).await;
        let session = if child {
            Session::new_child_of("ordinary-child", &supervisor, "test-model", "child")
        } else {
            Session::new("ordinary-root", "test-model")
        };
        let id = session.id.clone();
        *fixture.probe.next_call.lock().unwrap() = Some(alias_call());
        events(protected_agent(&fixture).run_stream(session, "attempt attach")).await;
        let pending = fixture.reload(&id).await;
        assert!(pending.messages[result_index(&pending)]
            .metadata
            .as_ref()
            .is_none_or(|metadata| metadata.get(AUTHORITY).is_none()));
        let approved = fixture.approve(&current_request(&pending)).await;
        events(protected_agent(&fixture).resume_stream(approved)).await;
        let mut synthetic = ToolCtx::none("synthetic-call");
        synthetic.session_id = Some(original.session_id.as_str().into());
        let result = ProtectedTool {
            name: "Bash".into(),
            service: SupervisorSessionService::new(fixture.store.clone()),
            probe: fixture.probe.clone(),
            config: alias_config(),
            second_context: false,
            barrier: None,
        }
        .invoke(json!({"command":"current-command"}), synthetic)
        .await;
        assert!(result.is_err());
        assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
        assert_eq!(
            SupervisorSessionService::new(fixture.store.clone())
                .inspect_scope(&original)
                .await
                .unwrap()
                .state_revision,
            1
        );
    }
}
