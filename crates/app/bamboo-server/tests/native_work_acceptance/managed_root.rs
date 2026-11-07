//! The production Supervisor tool, Inbox/Actor and Root resource owner.
use super::*;
use bamboo_agent_core::tools::{
    with_executing_session_caller, ExecutingSupervisorObservation, ToolExecutionContext,
    ToolExecutionSessionFlags, ToolOutcome,
};
use bamboo_domain::{Session, SessionCallerReference, SessionKind};
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
    let state = web::Data::new(host(&path, provider.clone()).await);
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
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
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
