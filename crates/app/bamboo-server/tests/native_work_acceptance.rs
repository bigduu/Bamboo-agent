//! Actual assembled Host execution, followed by an explicit owner HTTP decision.
//! The only model is an in-process gate. No Runtime receipt or Submission is seeded.
use actix_web::{http::StatusCode, test, web, App};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_domain::{Role, WorkspaceBinding};
use bamboo_engine::ticket_runtime;
use bamboo_llm::LLMChunk;
use bamboo_tickets::{
    canonical_bytes, content_hash, Contract, Operation, Principal, ProjectContextSnapshot,
    StepStatus, TicketKind, WorkState,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;

#[macro_use]
#[path = "common/native_work.rs"]
mod native_work;
use native_work::{execution_workspace, host, LocalProvider, CALL, RESULT, WAIT};

#[actix_web::test]
async fn native_task_submission_requires_owner_accept_and_survives_host_restart() {
    let root = tempfile::tempdir().unwrap();
    let physical_root = root.path().canonicalize().unwrap();
    let workspace = execution_workspace(&physical_root);
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 2,
    });

    let (
        work_id,
        assignment_id,
        child_id,
        submission_id,
        accept,
        accepted,
        before_submission,
        artifact,
    ) = {
        let state = web::Data::new(host(&physical_root, provider.clone()).await);
        let (service, owner) = state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let binding = service.published().unwrap().1.binding;
        let supervisor = state
            .storage
            .load_root_authority(&binding.supervisor_session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(supervisor.supervisor_incarnation_id().is_some());
        assert!(
            supervisor.supervisor_orchestration_revision().is_some(),
            "formal Work requires canonical Supervisor business authority in either thinking mode"
        );
        let manifest = state
            .project_store
            .create("Native owner acceptance", None)
            .unwrap();
        let manifest = state
            .project_store
            .bind_workspace(
                &manifest.id,
                manifest.revision,
                WorkspaceBinding {
                    path: workspace.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(bamboo_server::configure_routes),
        )
        .await;
        let contract = Contract {
            title: "Native lifecycle".into(),
            objective: "Finish one bounded Task and submit the actual result for owner review."
                .into(),
            constraints: vec!["Only the authorized Task tool; never self-accept".into()],
            acceptance: vec!["The owner reviews the exact stopped native result".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let create = service
            .prepare_command(
                &owner,
                "native-create",
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract.clone(),
                        depends_on: BTreeSet::new(),
                    },
                    Operation::AssociateProject {
                        ticket_id: "work".into(),
                        project_id: manifest.id.clone(),
                    },
                ],
            )
            .unwrap();
        let created = owner_post!(app, "/api/v1/tickets/update", &create, StatusCode::OK);
        let work_id = created["ids"]["work"].as_str().unwrap().to_owned();
        let snapshot = service.published().unwrap().1;
        let ready = service
            .prepare_command(
                &owner,
                "native-ready",
                vec![
                    Operation::SetProjectContext {
                        work_id: work_id.clone(),
                        context: ProjectContextSnapshot {
                            project_id: manifest.id.clone(),
                            project_revision: manifest.revision,
                            resource_revision: manifest.resource_revision,
                            workspace_binding: manifest.workspace_bindings[0].clone(),
                            contract_revision: snapshot.tickets[&work_id].contract_revision,
                            contract,
                            sources: BTreeMap::new(),
                        },
                    },
                    Operation::Ready {
                        work_id: work_id.clone(),
                    },
                ],
            )
            .unwrap();
        owner_post!(app, "/api/v1/tickets/update", &ready, StatusCode::OK);
        assert_eq!(
            service.published().unwrap().1.tickets[&work_id].state,
            WorkState::Ready
        );
        let start = service
            .prepare_command(
                &owner,
                "native-start",
                vec![Operation::Start {
                    work_id: work_id.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some(workspace.clone()),
                }],
            )
            .unwrap();
        let dispatched = owner_post!(app, "/api/v1/tickets/dispatch", &start, StatusCode::OK);
        assert_eq!(dispatched["status"], "accepted_for_dispatch");
        assert_eq!(dispatched["errors"], json!([]));
        let first = tokio::time::timeout(WAIT, requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(first.tools.iter().any(|tool| tool.function.name == "Task"));
        assert!(!first
            .tools
            .iter()
            .any(|tool| matches!(tool.function.name.as_str(), "work_update" | "work_dispatch")));
        let running = service.published().unwrap().1;
        let assignment_id = dispatched["receipt"]["ids"]["assignment"]
            .as_str()
            .unwrap()
            .to_owned();
        let assignment = &running.assignments[&assignment_id];
        let receipt = assignment.runtime.as_ref().expect("real native admission");
        let child_id = receipt.session_id.clone();
        assert_eq!(running.tickets[&work_id].state, WorkState::Active);
        assert!(!assignment.process_stopped);
        let child = state
            .storage
            .load_session(&child_id)
            .await
            .unwrap()
            .unwrap();
        let dispatch = ticket_runtime::read_dispatch(&child).unwrap().unwrap();
        assert_eq!(dispatch.receipt.as_ref(), Some(receipt));
        assert_eq!(
            child.parent_session_id.as_deref(),
            Some(binding.supervisor_session_id.as_str())
        );
        assert_eq!(
            running.project.assignment_contexts[&assignment_id],
            running.project.work_contexts[&work_id]
        );
        let replay = owner_post!(app, "/api/v1/tickets/dispatch", &start, StatusCode::OK);
        assert_eq!(replay["receipt"], dispatched["receipt"]);
        assert_eq!(replay["errors"], json!([]));
        assert_eq!(service.published().unwrap().1.assignments.len(), 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        first.reply.send(LLMChunk::ToolCalls(vec![ToolCall {
            id: CALL.into(), tool_type: "function".into(), function: FunctionCall {
                name: "Task".into(), arguments: json!({"tasks":[{"id":"native-step","content":"Complete the bounded plan","status":"completed"}]}).to_string(),
            },
        }])).unwrap();
        let second = tokio::time::timeout(WAIT, requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            second
                .messages
                .iter()
                .any(|message| message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(CALL)
                    && message.tool_success == Some(true)),
            "continuation consumes the actual Task result"
        );
        let planned = service.published().unwrap().1;
        assert_eq!(
            planned.assignments[&assignment_id].plan.steps[0].status,
            Some(StepStatus::Completed)
        );
        assert!(planned.tickets[&work_id].accepted_submission.is_none());
        second.reply.send(LLMChunk::Token(RESULT.into())).unwrap();
        tokio::time::timeout(WAIT, async {
            loop {
                let snapshot = service.published().unwrap().1;
                assert_ne!(
                    snapshot.tickets[&work_id].state,
                    WorkState::Accepted,
                    "execution never substitutes for owner acceptance"
                );
                if snapshot.tickets[&work_id].state == WorkState::Submitted
                    && snapshot.assignments[&assignment_id].process_stopped
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real native result and independently confirmed stop");
        let submitted = service.published().unwrap().1;
        assert_eq!(submitted.assignments.len(), 1);
        assert_eq!(submitted.submissions.len(), 1);
        let submission_id = submitted.tickets[&work_id]
            .current_submission
            .clone()
            .unwrap();
        let submission = &submitted.submissions[&submission_id];
        assert_eq!(submission.runtime, *receipt);
        assert_eq!(submission.assignment_id, assignment_id);
        assert_eq!(
            submission.generation,
            submitted.tickets[&work_id].generation
        );
        assert!(!submission.stale);
        let child = state
            .storage
            .load_session(&child_id)
            .await
            .unwrap()
            .unwrap();
        let stop: Value =
            serde_json::from_str(&child.metadata["ticket.runtime.owned_stop.v1"]).unwrap();
        assert_eq!(stop["receipt"], json!(receipt));
        assert!(stop["pid"].is_null());
        assert!(stop["task_instance"].as_str().is_some());
        assert_eq!(stop["completed"], true);
        let before_submission = canonical_bytes(submission).unwrap();
        let artifact = submission.artifacts[0].clone();
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/tickets/artifacts/{}", artifact.sha256))
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = test::read_body(response).await;
        assert_eq!(content_hash(&body), artifact.sha256);
        assert!(String::from_utf8(body.to_vec()).unwrap().contains(RESULT));
        let accept = service
            .prepare_command(
                &owner,
                "native-owner-accept",
                vec![Operation::Accept {
                    work_id: work_id.clone(),
                    submission_id: submission_id.clone(),
                    evidence: vec![
                        "Owner reviewed this exact native result and confirmed-stop receipt".into(),
                    ],
                }],
            )
            .unwrap();
        let mut stale = accept.clone();
        stale.operation_id = "native-stale-owner-accept".into();
        stale.expected_seq -= 1;
        owner_post!(app, "/api/v1/tickets/update", &stale, StatusCode::CONFLICT);
        assert_eq!(
            service.published().unwrap().1.tickets[&work_id].state,
            WorkState::Submitted
        );
        let accepted = owner_post!(app, "/api/v1/tickets/update", &accept, StatusCode::OK);
        assert_eq!(accepted["principal"], "user:host-owner");
        assert_eq!(
            owner_post!(app, "/api/v1/tickets/update", &accept, StatusCode::OK),
            accepted
        );
        let final_snapshot = service.published().unwrap().1;
        assert_eq!(final_snapshot.tickets[&work_id].state, WorkState::Accepted);
        assert_eq!(
            final_snapshot.tickets[&work_id]
                .accepted_submission
                .as_deref(),
            Some(submission_id.as_str())
        );
        assert_eq!(
            canonical_bytes(&final_snapshot.submissions[&submission_id]).unwrap(),
            before_submission
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        state.shutdown().await;
        (
            work_id,
            assignment_id,
            child_id,
            submission_id,
            accept,
            accepted,
            before_submission,
            artifact,
        )
    };

    let state = web::Data::new(host(&physical_root, provider.clone()).await);
    let (service, _) = state
        .tickets
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.tickets[&work_id].state, WorkState::Accepted);
    assert_eq!(
        snapshot.tickets[&work_id].accepted_submission.as_deref(),
        Some(submission_id.as_str())
    );
    assert!(snapshot.assignments[&assignment_id].process_stopped);
    assert_eq!(
        snapshot.assignments[&assignment_id]
            .runtime
            .as_ref()
            .unwrap()
            .session_id,
        child_id
    );
    assert_eq!(
        canonical_bytes(&snapshot.submissions[&submission_id]).unwrap(),
        before_submission
    );
    assert_eq!(
        owner_post!(app, "/api/v1/tickets/update", &accept, StatusCode::OK),
        accepted
    );
    let inspection = owner_post!(
        app,
        "/api/v1/tickets/inspect",
        &json!({"ids":[work_id],"sections":["submissions","controls"],"depth":0,"budget_bytes":65536,"fixed_commit":null}),
        StatusCode::OK
    );
    assert!(inspection.to_string().contains(&submission_id));
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(&format!("/api/v1/tickets/artifacts/{}", artifact.sha256))
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = test::read_body(response).await;
    assert_eq!(content_hash(&bytes), artifact.sha256);
    assert!(String::from_utf8(bytes.to_vec()).unwrap().contains(RESULT));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    state.shutdown().await;
    eprintln!("native_work_acceptance: actual Host Task/Submission/confirmed stop/owner HTTP Accept/replay/restart PASS; local provider=2, external provider=0");
}
