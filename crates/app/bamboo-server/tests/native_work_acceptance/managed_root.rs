//! The production Supervisor tool, Inbox/Actor and Root resource owner.
use super::*;
use bamboo_agent_core::tools::{
    with_executing_session_caller, ExecutingSupervisorObservation, ToolExecutionContext,
    ToolExecutionSessionFlags, ToolOutcome, ToolSchema,
};
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryPort, Message, ReasoningEffort, Session,
    SessionCallerReference, SessionKind,
};
use bamboo_engine::session_activation::{
    SessionActivationReserveOutcome, SessionActivationSpawner,
};
use bamboo_llm::{LLMError, LLMProvider, LLMRequestOptions, LLMStream};
use bamboo_server::{tools::ToolSurface, AppState};
use bamboo_tickets::{AssignmentState, Authority, Command, ExecutionWorkspace, TicketService};

async fn dispatch(state: &AppState, caller: &Session, command: &Command, success: bool) -> Value {
    let args = json!({
        "operation_id": command.operation_id,
        "expected_seq": command.expected_seq,
        "expected_epoch": command.expected_epoch,
        "operations": command.operations,
    });
    let call = ToolCall {
        id: "managed-work-dispatch".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "work_dispatch".into(),
            arguments: args.to_string(),
        },
    };
    let tools = state.tools_for(ToolSurface::Root);
    let schemas = tools.list_tools();
    let (tx, _rx) = mpsc::channel(16);
    let ctx = ToolExecutionContext::for_dispatch(
        &caller.id,
        &caller.root_session_id,
        &call.id,
        &tx,
        &schemas,
        ToolExecutionSessionFlags::from_session(caller),
        false,
        None,
        None,
    )
    .with_executing_supervisor(
        ExecutingSupervisorObservation::capture_from_executing_session(caller),
    );
    let result = with_executing_session_caller(
        Some(Arc::new(SessionCallerReference::from(caller))),
        tools.execute_with_context_outcome(&call, ctx),
    )
    .await
    .unwrap();
    let ToolOutcome::Completed(result) = result else {
        panic!("expected dispatch receipt")
    };
    assert_eq!(result.success, success, "{}", result.result);
    let response: Value = serde_json::from_str(&result.result).unwrap();
    if success {
        assert_eq!(response["errors"], json!([]));
    }
    response
}

fn prepare_start(
    service: &TicketService,
    owner: &Authority,
    supervisor: &Authority,
    label: &str,
    workspace: ExecutionWorkspace,
    constraints: Vec<String>,
) -> (String, Command) {
    let contract = Contract {
        title: "Managed native handoff".into(),
        objective: "Read the assigned fixture and report verified evidence.".into(),
        constraints,
        acceptance: vec!["Read the file and record the Task plan".into()],
        allowed_tools: BTreeSet::from(["Read".into(), "Task".into()]),
        user_acceptance_required: true,
    };
    let created = service
        .prepare_command(
            owner,
            &format!("{label}-create"),
            vec![Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract,
                depends_on: BTreeSet::new(),
            }],
        )
        .unwrap();
    let work = service.work_update(owner, &created).unwrap().ids["work"].clone();
    let ready = service
        .prepare_command(
            owner,
            &format!("{label}-ready"),
            vec![Operation::Ready {
                work_id: work.clone(),
            }],
        )
        .unwrap();
    service.work_update(owner, &ready).unwrap();
    let start = service
        .prepare_command(
            supervisor,
            &format!("{label}-start"),
            vec![Operation::Start {
                work_id: work.clone(),
                temp_id: "assignment".into(),
                workspace: Some(workspace),
            }],
        )
        .unwrap();
    (work, start)
}

#[actix_web::test]
async fn supervisor_work_start_admits_one_independent_root_and_submits_owned_result() {
    managed_work_root_submits_owned_result(false).await;
}

#[actix_web::test]
async fn supervisor_work_ultra_reaches_the_managed_root_and_its_model_requests() {
    managed_work_root_submits_owned_result(true).await;
}

struct AssertEffort {
    provider: Arc<LocalProvider>,
    expected: ReasoningEffort,
}

#[async_trait::async_trait]
impl LLMProvider for AssertEffort {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("managed Work must retain request options");
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(
            options.and_then(|options| options.reasoning_effort),
            Some(self.expected)
        );
        self.provider
            .chat_stream(messages, tools, max_output_tokens, model)
            .await
    }
}

async fn managed_work_root_submits_owned_result(ultra: bool) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let home = tempfile::tempdir().unwrap();
    let path = home.path().canonicalize().unwrap();
    let workspace = execution_workspace(&path);
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 3,
    });
    let routed = Arc::new(AssertEffort {
        provider: provider.clone(),
        expected: if ultra {
            ReasoningEffort::Max
        } else {
            ReasoningEffort::Xhigh
        },
    });
    let state = web::Data::new(host(&path, routed).await);
    // The provider is already installed in the fixture registry. Leave the
    // synthetic plaintext key out of the config being persisted by the API.
    state
        .config
        .write()
        .await
        .providers_mut()
        .openai
        .as_mut()
        .unwrap()
        .api_key
        .clear();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let saved = owner_post!(
        app,
        "/api/v1/bamboo/config",
        &json!({ "features": {
            "work_ultra_by_default": ultra,
            "work_execution": { "reasoning_effort": "xhigh" }
        } }),
        StatusCode::OK
    );
    assert_eq!(saved["features"]["work_ultra_by_default"], ultra);
    let readback: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/bamboo/config")
            .to_request(),
    )
    .await;
    assert_eq!(readback["features"]["work_ultra_by_default"], ultra);
    assert_eq!(
        readback["features"]["work_execution"],
        json!({ "reasoning_effort": "xhigh", "max_parallel": null })
    );
    let owner_principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, owner) = state
        .tickets
        .authority(owner_principal.clone())
        .await
        .unwrap();
    let supervisor_id = service.published().unwrap().1.binding.supervisor_session_id;
    let supervisor = state
        .storage
        .load_execution_admission_snapshot(&supervisor_id)
        .await
        .unwrap()
        .unwrap();
    let project = state.project_store.create("Managed Work", None).unwrap();
    state
        .project_store
        .bind_workspace(
            &project.id,
            project.revision,
            WorkspaceBinding {
                path: workspace.worktree.clone(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    let (_, authority) = state
        .tickets
        .authority(Principal::Supervisor {
            session_id: supervisor_id.clone(),
        })
        .await
        .unwrap();
    let (work, start) = prepare_start(
        &service,
        &owner,
        &authority,
        "managed",
        workspace.clone(),
        Vec::new(),
    );

    let first_dispatch = dispatch(&state, &supervisor, &start, true).await;
    assert!(
        matches!(
            first_dispatch["runtime"][0]["observation"]["status"].as_str(),
            Some("pending" | "admitted")
        ),
        "{first_dispatch}"
    );
    let assignment_id = first_dispatch["receipt"]["ids"]["assignment"]
        .as_str()
        .unwrap();
    let request = match tokio::time::timeout(WAIT, requests.recv()).await {
        Ok(Some(request)) => request,
        _ => {
            let snapshot = service.published().unwrap().1;
            let assignment = &snapshot.assignments[assignment_id];
            let intent = &snapshot.intents[&assignment.dispatch_key];
            let target = intent
                .immutable_spec
                .managed_root
                .as_ref()
                .unwrap()
                .session_id()
                .unwrap();
            let terminal = state
                .storage
                .load_execution_admission_snapshot(&target)
                .await
                .map(|root| root.map(|root| (root.last_run_status(), root.last_run_error())));
            panic!("managed Root did not reach provider: dispatch={first_dispatch}, state={:?}, terminal={terminal:?}", assignment.state);
        }
    };
    let snapshot = service.published().unwrap().1;
    let assignment = &snapshot.assignments[assignment_id];
    let intent = &snapshot.intents[&assignment.dispatch_key];
    let receipt = assignment.runtime.as_ref().unwrap();
    let admission = intent.managed_admission.as_ref().unwrap();
    assert_eq!(assignment.state, AssignmentState::Running);
    assert_eq!(admission.run_id, receipt.run_id);
    assert_eq!(admission.target_session_id, receipt.session_id);
    let root = state
        .storage
        .load_execution_admission_snapshot(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.kind, SessionKind::Root);
    assert_eq!(root.root_thinking_mode().enabled(), ultra);
    assert_eq!(root.reasoning_effort, Some(ReasoningEffort::Xhigh));
    assert_eq!(
        intent
            .immutable_spec
            .managed_root
            .as_ref()
            .unwrap()
            .thinking_mode,
        ultra.then_some(bamboo_domain::RootThinkingMode::Ultra)
    );
    assert_eq!(root.created_at, admission.target_created_at);
    assert_eq!(root.project_id_meta().as_deref(), Some(project.id.as_str()));
    assert_eq!(root.parent_session_id, None);
    assert_eq!(root.root_session_id, root.id);
    assert_eq!(
        request
            .tools
            .iter()
            .map(|tool| tool.function.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["Read", "Task"])
    );
    assert!(request
        .messages
        .iter()
        .any(|message| message.content.contains(&work)));
    if ultra {
        let saved = owner_post!(
            app,
            "/api/v1/bamboo/config",
            &json!({ "features": { "work_ultra_by_default": false } }),
            StatusCode::OK
        );
        assert_eq!(saved["features"]["work_ultra_by_default"], false);
        assert_eq!(
            saved["features"]["work_execution"]["reasoning_effort"],
            "xhigh"
        );
    }
    assert_eq!(
        dispatch(&state, &supervisor, &start, true).await["receipt"],
        first_dispatch["receipt"]
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    let call = |id: &str, name: &str, arguments: Value| ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: arguments.to_string(),
        },
    };
    request
        .reply
        .send(LLMChunk::ToolCalls(vec![
            call(
                "managed-read",
                "Read",
                json!({
                    "file_path": format!("{}/fixture.txt", root.workspace_path_meta().unwrap()),
                }),
            ),
            call(
                "managed-task",
                "Task",
                json!({"tasks":[
                    {"id":"verified","content":"Verify the isolated fixture","status":"completed"}
                ]}),
            ),
        ]))
        .unwrap();
    let second = tokio::time::timeout(WAIT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(second.messages.iter().any(|message| {
        message.role == Role::Tool
            && message.tool_call_id.as_deref() == Some("managed-read")
            && message.tool_success == Some(true)
            && message.content.contains("disposable native acceptance")
    }));
    let task_reply = second
        .messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some("managed-task"))
        .expect("provider saw the actual Task result");
    assert_eq!(
        task_reply.tool_success,
        Some(true),
        "{}",
        task_reply.content
    );
    assert_eq!(
        service.published().unwrap().1.assignments[assignment_id]
            .plan
            .steps[0]
            .status,
        Some(StepStatus::Completed)
    );
    second.reply.send(LLMChunk::Token(RESULT.into())).unwrap();
    let submitted = tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.published().unwrap().1;
            if let Some(id) = &snapshot.tickets[&work].current_submission {
                break id.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("joined Root result should publish its Work Submission");
    let facts = service
        .work_submission_facts(&owner, &work, &submitted, None)
        .unwrap()
        .data;
    assert!(facts.assignment.process_stopped);
    let handoff = ticket_runtime::load_handoff_session(state.storage.as_ref(), &facts)
        .await
        .unwrap()
        .unwrap();
    let intent = facts.managed_intent.as_ref().unwrap();
    let reply = bamboo_orchestration::managed_work::terminal_reply(intent, receipt, &handoff)
        .unwrap()
        .unwrap();
    assert_eq!(reply.content, RESULT);
    assert_eq!(
        facts.submission.artifacts[0].sha256,
        content_hash(RESULT.as_bytes())
    );
    let accept = service
        .prepare_command(
            &owner,
            "managed-accept",
            vec![Operation::Accept {
                work_id: work.clone(),
                submission_id: submitted.clone(),
                evidence: vec!["Verified the exact handoff".into()],
            }],
        )
        .unwrap();
    assert!(service.work_update(&authority, &accept).is_err());
    owner_post!(app, "/api/v1/tickets/update", &accept, StatusCode::OK);
    drop(app);
    state.shutdown().await;
    drop(service);
    drop(state);

    let reopened = host(&path, provider.clone()).await;
    let (service, owner) = reopened
        .tickets
        .authority(owner_principal.clone())
        .await
        .unwrap();
    assert_eq!(
        service.published().unwrap().1.tickets[&work].state,
        WorkState::Accepted
    );
    let recovered = service
        .work_submission_facts(&owner, &work, &submitted, None)
        .unwrap()
        .data;
    assert_eq!(
        recovered.managed_intent.as_ref().unwrap().managed_admission,
        intent.managed_admission
    );
    assert!(reopened
        .storage
        .delete_session(&receipt.session_id)
        .await
        .unwrap());
    let replacement = reopened
        .storage
        .recreate_root_session(&receipt.session_id, native_work::MODEL)
        .await
        .unwrap();
    assert_ne!(replacement.created_at, root.created_at);
    assert!(
        ticket_runtime::load_handoff_session(reopened.storage.as_ref(), &recovered)
            .await
            .unwrap()
            .is_none()
    );
    let supervisor = reopened
        .storage
        .load_execution_admission_snapshot(&supervisor_id)
        .await
        .unwrap()
        .unwrap();
    let replay = dispatch(&reopened, &supervisor, &start, true).await;
    assert_eq!(replay["receipt"], first_dispatch["receipt"]);
    assert_eq!(
        replay["runtime"][0]["observation"]["status"],
        "outcome_unknown"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);

    let (_, authority) = reopened
        .tickets
        .authority(Principal::Supervisor {
            session_id: supervisor_id,
        })
        .await
        .unwrap();
    let (oversized_work, oversized) = prepare_start(
        &service,
        &owner,
        &authority,
        "oversized",
        workspace.clone(),
        vec!["x".repeat(17 * 1024)],
    );
    let refused = dispatch(&reopened, &supervisor, &oversized, false).await;
    assert_eq!(refused["error"], "context_budget_exceeded");
    assert_eq!(
        service.published().unwrap().1.tickets[&oversized_work].state,
        WorkState::Ready
    );

    let (cancel_work, cancel_start) = prepare_start(
        &service,
        &owner,
        &authority,
        "cancel",
        workspace,
        Vec::new(),
    );
    let dispatched = dispatch(&reopened, &supervisor, &cancel_start, true).await;
    let cancelled_id = dispatched["receipt"]["ids"]["assignment"].as_str().unwrap();
    let held = tokio::time::timeout(WAIT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    let cancel = service
        .prepare_command(
            &owner,
            "cancel-exact-root",
            vec![Operation::Cancel {
                work_id: cancel_work.clone(),
            }],
        )
        .unwrap();
    reopened
        .tickets
        .update(owner_principal, &cancel)
        .await
        .unwrap();
    let stopped = tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.published().unwrap().1;
            if snapshot.assignments[cancelled_id].process_stopped {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("exact cancelled Root must close before releasing Work claims");
    drop(held);
    assert_eq!(
        stopped.assignments[cancelled_id].state,
        AssignmentState::Cancelled
    );
    assert!(stopped.tickets[&cancel_work].current_submission.is_none());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    reopened.shutdown().await;
}

struct HeldActivation {
    real: Option<Arc<bamboo_engine::ChildCompletionCoordinator>>,
    abandon_prepared: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl SessionActivationSpawner for HeldActivation {
    async fn reserve_activation(
        &self,
        id: &str,
        generation: u64,
    ) -> std::result::Result<SessionActivationReserveOutcome, bamboo_domain::SessionActivationError>
    {
        let reserved = match &self.real {
            Some(real) => real.reserve_activation(id, generation).await,
            None => Err(bamboo_domain::SessionActivationError::Internal(
                "controlled activation unavailable".into(),
            )),
        };
        if (self.real.is_none()
            || matches!(&reserved, Ok(SessionActivationReserveOutcome::Reserved(_))))
            && self.calls.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.abandon_prepared
            && matches!(&reserved, Ok(SessionActivationReserveOutcome::Reserved(_)))
        {
            // Dropping the actual two-phase launch runs its native Actor
            // rollback. No Root outer task has been attached.
            drop(reserved);
            return Err(bamboo_domain::SessionActivationError::Internal(
                "controlled abandonment of the reserved Root".into(),
            ));
        }
        reserved
    }
}

#[actix_web::test]
async fn supervisor_cancels_managed_root_before_work_admission_and_releases_its_claim() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().canonicalize().unwrap();
    let workspace = execution_workspace(&path);
    let (send, _requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 0,
    });
    let state = host(&path, provider.clone()).await;
    let owner_principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, owner) = state
        .tickets
        .authority(owner_principal.clone())
        .await
        .unwrap();
    let supervisor_id = service.published().unwrap().1.binding.supervisor_session_id;
    let supervisor = state
        .storage
        .load_execution_admission_snapshot(&supervisor_id)
        .await
        .unwrap()
        .unwrap();
    let (_, authority) = state
        .tickets
        .authority(Principal::Supervisor {
            session_id: supervisor_id,
        })
        .await
        .unwrap();
    let project = state.project_store.create("Managed Work", None).unwrap();
    state
        .project_store
        .bind_workspace(
            &project.id,
            project.revision,
            WorkspaceBinding {
                path: workspace.worktree.clone(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    for (label, prepare_actor, abandon_prepared) in [
        ("staged-without-launch", false, false),
        ("abandoned-real-reservation", true, true),
        ("launched-real-run", true, false),
    ] {
        let gate = Arc::new(HeldActivation {
            real: prepare_actor.then(|| state.child_completion_coordinator.clone()),
            abandon_prepared,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            calls: AtomicUsize::new(0),
        });
        state
            .session_activation_router
            .set_spawner(gate.clone())
            .await;
        let (work, start) = prepare_start(
            &service,
            &owner,
            &authority,
            label,
            workspace.clone(),
            Vec::new(),
        );
        let cancel_before_admission = async {
            tokio::time::timeout(WAIT, gate.entered.notified())
                .await
                .unwrap();
            let (_, snapshot) = service.published().unwrap();
            let assignment_id = snapshot.tickets[&work].active_assignment.clone().unwrap();
            let assignment = &snapshot.assignments[&assignment_id];
            let target = snapshot.intents[&assignment.dispatch_key]
                .immutable_spec
                .managed_root
                .as_ref()
                .unwrap()
                .session_id()
                .unwrap();
            let pending = state.session_inbox.inspect(&target).await.unwrap();
            assert_eq!(pending.pending, 1);
            assert!(assignment.runtime.is_none());
            assert!(!assignment.process_stopped);
            let activation = state
                .session_store
                .inspect_actor(&target)
                .await
                .unwrap()
                .activation;
            assert_eq!(
                activation
                    .as_ref()
                    .is_some_and(|actor| actor.status.is_live()),
                prepare_actor
            );
            let cancel = service
                .prepare_command(
                    &owner,
                    &format!("cancel-{label}"),
                    vec![Operation::Cancel {
                        work_id: work.clone(),
                    }],
                )
                .unwrap();
            state
                .tickets
                .update(owner_principal.clone(), &cancel)
                .await
                .unwrap();
            assert!(!service.published().unwrap().1.assignments[&assignment_id].process_stopped);
            gate.release.notify_one();
            (assignment_id, target)
        };
        let (_, (assignment, target)) = tokio::join!(
            dispatch(&state, &supervisor, &start, true),
            cancel_before_admission
        );
        tokio::time::timeout(WAIT, async {
            loop {
                let (_, snapshot) = service.published().unwrap();
                let assignment = &snapshot.assignments[&assignment];
                if assignment.process_stopped {
                    assert_eq!(assignment.state, AssignmentState::Cancelled);
                    assert!(assignment.runtime.is_none());
                    assert!(snapshot.intents[&assignment.dispatch_key]
                        .managed_admission
                        .is_none());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("confirmed unadmitted stop must release the Work claim");
        let activation = state
            .session_store
            .inspect_actor(&target)
            .await
            .unwrap()
            .activation;
        assert_eq!(activation.is_some(), prepare_actor);
        if abandon_prepared {
            assert_eq!(
                activation.as_ref().map(|actor| actor.status),
                Some(ActorActivationStatus::Cancelled)
            );
        }
        assert!(activation.is_none_or(|actor| !actor.status.is_live()));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
    state.shutdown().await;
}

// Use the same executor and verified caller envelope as a normal model turn.
async fn project_tool(
    state: &AppState,
    caller: &Session,
    name: &str,
    args: Value,
    success: bool,
) -> Value {
    let call = ToolCall {
        id: format!("fixture-{name}"),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: args.to_string(),
        },
    };
    let tools = state.tools_for(ToolSurface::Root);
    let schemas = tools.list_tools();
    let (tx, _rx) = mpsc::channel(16);
    let ctx = ToolExecutionContext::for_dispatch(
        &caller.id,
        &caller.root_session_id,
        &call.id,
        &tx,
        &schemas,
        ToolExecutionSessionFlags::from_session(caller),
        false,
        None,
        None,
    )
    .with_executing_supervisor(
        ExecutingSupervisorObservation::capture_from_executing_session(caller),
    );
    let result = with_executing_session_caller(
        Some(Arc::new(SessionCallerReference::from(caller))),
        tools.execute_with_context_outcome(&call, ctx),
    )
    .await
    .unwrap();
    let ToolOutcome::Completed(result) = result else {
        panic!("expected a tool result")
    };
    assert_eq!(result.success, success, "{}", result.result);
    serde_json::from_str(&result.result).unwrap()
}

fn model_tool(request: native_work::ModelRequest, id: &str, name: &str, arguments: Value) {
    assert!(
        request.tools.iter().any(|tool| tool.function.name == name),
        "model is missing {name}"
    );
    request
        .reply
        .send(LLMChunk::ToolCalls(vec![ToolCall {
            id: id.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: arguments.to_string(),
            },
        }]))
        .unwrap();
}

async fn discover_project_tool(
    requests: &mut mpsc::UnboundedReceiver<native_work::ModelRequest>,
    request: native_work::ModelRequest,
    name: &str,
) -> native_work::ModelRequest {
    if request.tools.iter().any(|tool| tool.function.name == name) {
        return request;
    }
    model_tool(
        request,
        "discover-project-tool",
        "discover_capabilities",
        json!({"query": name}),
    );
    tokio::time::timeout(WAIT, requests.recv())
        .await
        .expect("follow-up model request after discovery")
        .unwrap()
}

fn model_tool_result(request: &native_work::ModelRequest, id: &str) -> Value {
    let result = request
        .messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some(id))
        .unwrap_or_else(|| panic!("missing model Tool result for {id}"));
    assert_eq!(result.tool_success, Some(true), "{}", result.content);
    serde_json::from_str(&result.content).unwrap()
}

#[actix_web::test]
async fn supervisor_can_discover_projects_beyond_the_first_page() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let (requests, _received) = mpsc::unbounded_channel();
    let state = Box::pin(host(
        &root,
        Arc::new(LocalProvider {
            requests,
            calls: AtomicUsize::new(0),
            max_requests: 0,
        }),
    ))
    .await;
    for index in 0..33 {
        state
            .project_store
            .create(format!("Route fixture #{index:02}"), None)
            .unwrap();
    }
    let supervisor = state
        .storage
        .load_root_authority(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let first = project_tool(
        &state,
        &supervisor,
        "work_project_manager",
        json!({"action":"projects", "query":"ROUTE FIXTURE"}),
        true,
    )
    .await;
    assert_eq!(first["projects"].as_array().unwrap().len(), 32);
    assert_eq!(first["next_offset"], 32);
    let last = project_tool(
        &state,
        &supervisor,
        "work_project_manager",
        json!({"action":"projects", "query":"ROUTE FIXTURE", "offset":first["next_offset"]}),
        true,
    )
    .await;
    assert_eq!(last["projects"].as_array().unwrap().len(), 1);
    assert!(last["next_offset"].is_null());
    let unseen = &last["projects"][0];
    assert!(!first["projects"].as_array().unwrap().contains(unseen));
    let found = project_tool(
        &state,
        &supervisor,
        "work_project_manager",
        json!({"action":"projects", "query":unseen["name"]}),
        true,
    )
    .await;
    assert_eq!(found["projects"], json!([unseen]));
    state.shutdown().await;
}

#[actix_web::test]
async fn existing_project_work_survives_restart_and_dispatches_into_section() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let home = tempfile::tempdir().unwrap();
    let path = home.path().canonicalize().unwrap();
    let workspace = execution_workspace(&path);
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 8,
    });
    let section = format!("section:{}", uuid::Uuid::new_v4());
    let source_pins = BTreeMap::from([("installation-requirements".into(), "revision-1".into())]);
    let (project_id, manager_id, work_id) = {
        let state = web::Data::new(Box::pin(host(&path, provider.clone())).await);
        let project = state
            .project_store
            .create_with_project_path(
                "Project Manager acceptance",
                Some("Project description fixture for the installation project".into()),
                workspace.repo.clone(),
                vec![WorkspaceBinding {
                    path: workspace.worktree.clone(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        state
            .project_store
            .update(&project.id, project.revision, |project| {
                project.sidebar = Some(bamboo_domain::ProjectSidebar {
                    sections: vec![
                        bamboo_domain::ProjectSidebarSection {
                            id: format!("section:{}", uuid::Uuid::new_v4()),
                            name: "Testing".into(),
                            description: Some("Unrelated Section description fixture".into()),
                        },
                        bamboo_domain::ProjectSidebarSection {
                            id: section.clone(),
                            name: "Documentation".into(),
                            description: Some("Documentation Section description fixture".into()),
                        },
                    ],
                    ..Default::default()
                });
                Ok(())
            })
            .unwrap();
        let (service, owner) = state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let command = service
            .prepare_command(
                &owner,
                "manager-existing-work",
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: Contract {
                            title: "Installation documentation".into(),
                            objective: "Prepare an installation acceptance checklist.".into(),
                            constraints: vec!["Cover a fresh installation on macOS ARM64.".into()],
                            acceptance: vec!["List the proposed installation checks.".into()],
                            allowed_tools: BTreeSet::from(["Task".into()]),
                            user_acceptance_required: true,
                        },
                        depends_on: BTreeSet::new(),
                    },
                    Operation::AssociateProject {
                        ticket_id: "work".into(),
                        project_id: project.id.clone(),
                    },
                ],
            )
            .unwrap();
        let work_id = service.work_update(&owner, &command).unwrap().ids["work"].clone();
        let current_project = state.project_store.get(&project.id).unwrap();
        let work = service.published().unwrap().1.tickets[&work_id].clone();
        let adopt_context = service
            .prepare_command(
                &owner,
                "project-manager-initial-adopted-context",
                vec![Operation::SetProjectContext {
                    work_id: work_id.clone(),
                    context: ProjectContextSnapshot {
                        project_id: project.id.clone(),
                        project_revision: current_project.revision,
                        resource_revision: current_project.resource_revision,
                        workspace_binding: current_project.workspace_bindings[0].clone(),
                        contract_revision: work.contract_revision,
                        contract: work.contract,
                        sources: source_pins.clone(),
                    },
                }],
            )
            .unwrap();
        state
            .tickets
            .update(owner.principal().clone(), &adopt_context)
            .await
            .unwrap();
        let mut ordinary = Session::new("ordinary-project-session", native_work::MODEL);
        ordinary.set_project_id_meta(project.id.to_string());
        state.storage.save_session(&ordinary).await.unwrap();
        let ordinary = state
            .storage
            .load_session(&ordinary.id)
            .await
            .unwrap()
            .unwrap();
        assert!(!ordinary.allows_model_tool_execution("project_manager_work"));
        assert!(!ordinary.allows_model_tool_execution("work_project_manager"));
        project_tool(
            &state,
            &ordinary,
            "project_manager_work",
            json!({"action":"list"}),
            false,
        )
        .await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(bamboo_server::configure_routes),
        )
        .await;
        let manager = owner_post!(
            app,
            &format!("/api/v1/projects/{}/manager", project.id),
            &json!({}),
            StatusCode::OK
        );
        let manager_id = manager["session_id"].as_str().unwrap().to_owned();
        drop(app);
        state.shutdown().await;
        (project.id, manager_id, work_id)
    };
    let state = web::Data::new(Box::pin(host(&path, provider.clone())).await);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let manager = owner_post!(
        app,
        &format!("/api/v1/projects/{project_id}/manager"),
        &json!({}),
        StatusCode::OK
    );
    assert_eq!(manager["session_id"], manager_id);
    let manager = state
        .storage
        .load_session(&manager_id)
        .await
        .unwrap()
        .unwrap();
    assert!(manager.messages.iter().any(|message| message
        .content
        .contains("You are this Project's Project Manager")));
    let view = project_tool(
        &state,
        &manager,
        "project_manager_work",
        json!({"action":"list"}),
        true,
    )
    .await;
    assert_eq!(view["board"]["data"]["rows"][0]["ticket"]["id"], work_id);
    assert_eq!(view["sections"][1]["id"], section);
    let start = json!({
        "action": "start",
        "operation_id": uuid::Uuid::new_v4(),
        "work_id": work_id,
        "section_id": section,
    });
    let mut outside = start.clone();
    outside["work_id"] = json!("another-project-ticket");
    project_tool(&state, &manager, "project_manager_work", outside, false).await;
    let mut ungrouped = start.clone();
    ungrouped["section_id"] = Value::Null;
    let refused = project_tool(&state, &manager, "project_manager_work", ungrouped, false).await;
    assert!(refused["error"]
        .as_str()
        .unwrap()
        .contains("Project Section"));
    owner_post!(
        app,
        "/api/v1/chat",
        &json!({
            "session_id": manager.id,
            "message": format!("Start Work {work_id}. Choose an existing Section based on the card."),
            "model": native_work::MODEL,
        }),
        StatusCode::CREATED
    );
    owner_post!(
        app,
        &format!("/api/v1/execute/{}", manager.id),
        &json!({ "model": native_work::MODEL }),
        StatusCode::ACCEPTED
    );
    let intake = tokio::time::timeout(WAIT, requests.recv())
        .await
        .expect("Project Manager model request")
        .unwrap();
    assert!(intake.messages.iter().any(|message| message
        .content
        .contains("You are this Project's Project Manager")));
    assert!(!intake
        .tools
        .iter()
        .any(|tool| matches!(tool.function.name.as_str(), "Bash" | "work_dispatch")));
    let intake = discover_project_tool(&mut requests, intake, "project_manager_work").await;
    model_tool(
        intake,
        "inspect-project-ticket",
        "project_manager_work",
        json!({"action":"inspect", "work_id":work_id}),
    );
    let routing = tokio::time::timeout(WAIT, requests.recv())
        .await
        .expect("Project Manager model request with the original Work")
        .unwrap();
    let details = model_tool_result(&routing, "inspect-project-ticket");
    assert_eq!(details["ticket"]["id"], work_id);
    assert_eq!(
        details["ticket"]["contract"]["title"],
        "Installation documentation"
    );
    assert!(details["sections"]
        .as_array()
        .unwrap()
        .iter()
        .any(|choice| choice["id"] == section && choice["name"] == "Documentation"));
    model_tool(
        routing,
        "start-project-ticket",
        "project_manager_work",
        start.clone(),
    );
    let first = tokio::time::timeout(WAIT, requests.recv())
        .await
        .expect("first dispatch model request")
        .unwrap();
    let second = tokio::time::timeout(WAIT, requests.recv())
        .await
        .expect("second dispatch model request")
        .unwrap();
    let (routed, worker) = if first
        .messages
        .iter()
        .any(|message| message.tool_call_id.as_deref() == Some("start-project-ticket"))
    {
        (first, second)
    } else {
        (second, first)
    };
    let launch = model_tool_result(&routed, "start-project-ticket");
    assert_eq!(launch["errors"], json!([]));
    routed
        .reply
        .send(LLMChunk::Token("The original Work is dispatched.".into()))
        .unwrap();
    let manager = state
        .storage
        .load_session(&manager_id)
        .await
        .unwrap()
        .unwrap();
    let replay = project_tool(&state, &manager, "project_manager_work", start, true).await;
    assert_eq!(launch["receipt"], replay["receipt"]);
    let assignment_id = launch["receipt"]["ids"]["assignment"].as_str().unwrap();
    let (service, owner) = state
        .tickets
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        worker
            .tools
            .iter()
            .map(|tool| tool.function.name.as_str())
            .collect::<Vec<_>>(),
        ["Task"]
    );
    assert!(!worker.messages.iter().any(|message| message
        .content
        .contains("You are this Project's Project Manager")));
    for expected in [
        "Project description fixture for the installation project",
        "Current Section: Documentation",
        "Documentation Section description fixture",
    ] {
        assert!(
            worker
                .messages
                .iter()
                .any(|message| message.content.contains(expected)),
            "{expected}"
        );
    }
    assert!(!worker.messages.iter().any(|message| message
        .content
        .contains("Unrelated Section description fixture")));
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 1);
    let adopted = &snapshot.project.contexts[&snapshot.project.assignment_contexts[assignment_id]];
    assert_eq!(adopted.sources, source_pins);
    let receipt = snapshot.assignments[assignment_id]
        .runtime
        .as_ref()
        .unwrap();
    let session = state
        .storage
        .load_session(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session.kind, SessionKind::Root);
    assert_eq!(
        session.project_id_meta().as_deref(),
        Some(project_id.as_str())
    );
    let manifest = state.project_store.get(&project_id).unwrap();
    assert_eq!(
        manifest.sidebar.unwrap().session_sections[&session.id].as_deref(),
        Some(section.as_str())
    );
    assert_eq!(
        manifest.sidebar_section_bindings[&session.id].root_birth_token,
        session.root_mode_birth_token()
    );
    worker.reply.send(LLMChunk::ToolCalls(vec![ToolCall {
        id: "project-worker-task".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "Task".into(),
            arguments: json!({"tasks":[{"id":"done","content":"Prepare installation checks","status":"completed"}]}).to_string(),
        },
    }])).unwrap();
    let completion = tokio::time::timeout(WAIT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(completion
        .messages
        .iter()
        .any(
            |message| message.tool_call_id.as_deref() == Some("project-worker-task")
                && message.tool_success == Some(true)
        ));
    completion
        .reply
        .send(LLMChunk::Token(RESULT.into()))
        .unwrap();

    let submission_id = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(id) = &service.published().unwrap().1.tickets[&work_id].current_submission {
                break id.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("result published to the original Work");
    let facts = service
        .work_submission_facts(&owner, &work_id, &submission_id, None)
        .unwrap()
        .data;
    assert_eq!(facts.assignment.id, assignment_id);
    assert_eq!(
        service.published().unwrap().1.tickets[&work_id].state,
        WorkState::Submitted
    );
    assert_eq!(
        facts.submission.artifacts[0].sha256,
        content_hash(RESULT.as_bytes())
    );
    let card = owner_post!(
        app,
        "/api/v1/tickets/inspect",
        &json!({"ids":[work_id],"sections":["submissions"],"depth":0,"budget_bytes":65536,"fixed_commit":null}),
        StatusCode::OK
    );
    assert_eq!(card["data"][0]["ticket"]["id"], work_id);
    assert_eq!(card["data"][0]["submissions"][0]["id"], submission_id);
    let base_contract = service.published().unwrap().1.tickets[&work_id]
        .contract
        .clone();
    let populate = service
        .prepare_command(
            &owner,
            "project-manager-paginated-board",
            (0..24)
                .flat_map(|index| {
                    let temp = format!("paged-{index}");
                    let mut contract = base_contract.clone();
                    contract.title = format!("Paged Work {index}");
                    [
                        Operation::Create {
                            temp_id: temp.clone(),
                            kind: TicketKind::Work,
                            parent: None,
                            contract,
                            depends_on: BTreeSet::new(),
                        },
                        Operation::AssociateProject {
                            ticket_id: temp,
                            project_id: project_id.clone(),
                        },
                    ]
                })
                .collect(),
        )
        .unwrap();
    service.work_update(&owner, &populate).unwrap();
    let first = project_tool(
        &state,
        &manager,
        "project_manager_work",
        json!({"action":"list"}),
        true,
    )
    .await;
    assert_eq!(first["board"]["data"]["rows"].as_array().unwrap().len(), 24);
    let next = project_tool(&state, &manager, "project_manager_work",
        json!({"action":"list", "cursor":first["board"]["next_cursor"], "fixed_commit":first["board"]["snapshot"]["commit"]}), true).await;
    assert_eq!(next["board"]["data"]["rows"].as_array().unwrap().len(), 1);
    assert_eq!(next["board"]["snapshot"], first["board"]["snapshot"]);
    assert!(next["board"]["next_cursor"].is_null());
    let supervisor = state
        .storage
        .load_root_authority(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let inspected = project_tool(
        &state,
        &supervisor,
        "work_project_manager",
        json!({"action":"inspect","project_id":project_id,"cursor":first["board"]["next_cursor"],
             "fixed_commit":first["board"]["snapshot"]["commit"]}),
        true,
    )
    .await;
    assert_eq!(inspected["board"], next["board"]);

    let deleted = test::call_service(
        &app,
        test::TestRequest::delete()
            .uri(&format!("/api/v1/sessions/{manager_id}"))
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .to_request(),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::OK);
    let uri = format!("/api/v1/projects/{project_id}/manager");
    let (left, right) = tokio::join!(
        async { owner_post!(app, &uri, &json!({}), StatusCode::OK) },
        async { owner_post!(app, &uri, &json!({}), StatusCode::OK) },
    );
    assert_eq!(left["session_id"], right["session_id"]);
    assert_ne!(left["session_id"], manager_id);
    assert!(state
        .storage
        .load_root_authority(&manager_id)
        .await
        .unwrap()
        .is_none());
    let replacement = state
        .storage
        .load_root_authority(left["session_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replacement.project_id_meta().as_deref(),
        Some(project_id.as_str())
    );
    drop(app);
    state.shutdown().await;
}
