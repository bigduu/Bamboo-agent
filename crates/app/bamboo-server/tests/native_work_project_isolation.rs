//! Two real Project workers and an isolated denied-read probe over existing Host ports.
use actix_web::{http::StatusCode, test, web, App};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_domain::{ProjectId, Role, WorkspaceBinding};
use bamboo_llm::LLMChunk;
use bamboo_server::AppState;
use bamboo_tickets::{
    canonical_bytes, Contract, ExecutionWorkspace, Operation, Principal, ProjectContextSnapshot,
    Submission, TicketKind, WorkState,
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
use native_work::{execution_workspace_at, host, LocalProvider, ModelRequest, WAIT};

struct Work {
    id: String,
    project: ProjectId,
    workspace: ExecutionWorkspace,
    marker: &'static str,
}

impl Work {
    fn path(&self) -> String {
        std::path::Path::new(&self.workspace.worktree)
            .join("fixture.txt")
            .to_str()
            .unwrap()
            .into()
    }
}

macro_rules! prepare_work {
    ($app:expr, $state:expr, $workspace:expr, $project:expr, $label:expr, $marker:expr) => {{
        let (service, owner) = $state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let contract = Contract {
            title: format!("Project Work {}", $label),
            objective: format!("Read only this assigned Project's marked file: {}", $marker),
            constraints: vec!["Never read another Assignment's worktree; never self-accept".into()],
            acceptance: vec!["Explicit owner review of the exact stopped result".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into(), "Read".into()]),
        };
        let create = service
            .prepare_command(
                &owner,
                &format!("create-{}", $label),
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
                        project_id: $project.id.clone(),
                    },
                ],
            )
            .unwrap();
        let created = owner_post!($app, "/api/v1/tickets/update", &create, StatusCode::OK);
        let id = created["ids"]["work"].as_str().unwrap().to_owned();
        let snap = service.published().unwrap().1;
        let ready = service
            .prepare_command(
                &owner,
                &format!("ready-{}", $label),
                vec![
                    Operation::SetProjectContext {
                        work_id: id.clone(),
                        context: ProjectContextSnapshot {
                            project_id: $project.id.clone(),
                            project_revision: $project.revision,
                            resource_revision: $project.resource_revision,
                            workspace_binding: $project.workspace_bindings[0].clone(),
                            contract_revision: snap.tickets[&id].contract_revision,
                            contract,
                            sources: BTreeMap::new(),
                        },
                    },
                    Operation::Ready {
                        work_id: id.clone(),
                    },
                ],
            )
            .unwrap();
        owner_post!($app, "/api/v1/tickets/update", &ready, StatusCode::OK);
        Work {
            id,
            project: $project.id.clone(),
            workspace: $workspace.clone(),
            marker: $marker,
        }
    }};
}

macro_rules! start_work {
    ($app:expr, $state:expr, $work:expr, $label:expr) => {{
        let (service, owner) = $state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let command = service
            .prepare_command(
                &owner,
                &format!("start-{}", $label),
                vec![Operation::Start {
                    work_id: $work.id.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some($work.workspace.clone()),
                }],
            )
            .unwrap();
        let response = owner_post!($app, "/api/v1/tickets/dispatch", &command, StatusCode::OK);
        assert_eq!(response["errors"], json!([]));
        let assignment = response["receipt"]["ids"]["assignment"]
            .as_str()
            .unwrap()
            .to_owned();
        let replay = owner_post!($app, "/api/v1/tickets/dispatch", &command, StatusCode::OK);
        assert_eq!(replay["receipt"], response["receipt"]);
        assignment
    }};
}

async fn next(requests: &mut mpsc::UnboundedReceiver<ModelRequest>) -> ModelRequest {
    tokio::time::timeout(WAIT, requests.recv())
        .await
        .unwrap()
        .unwrap()
}

fn reply_call(request: ModelRequest, id: &str, name: &str, arguments: Value) {
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

fn reply<'a>(request: &'a ModelRequest, id: &str) -> &'a bamboo_domain::Message {
    request
        .messages
        .iter()
        .find(|message| message.role == Role::Tool && message.tool_call_id.as_deref() == Some(id))
        .expect("actual persisted tool result reaches continuation")
}

async fn complete_own_work(
    requests: &mut mpsc::UnboundedReceiver<ModelRequest>,
    work: &Work,
    other: &Work,
    label: &str,
) {
    let first = next(requests).await;
    let messages = serde_json::to_string(&first.messages).unwrap();
    assert!(messages.contains(work.marker));
    assert!(
        !messages.contains(other.marker),
        "foreign Project context must not enter the worker packet"
    );
    let read = first
        .tools
        .iter()
        .find(|tool| tool.function.name == "Read")
        .expect("explicit Assignment Read grant");
    assert_eq!(read.function.parameters["additionalProperties"], false);
    let read_id = format!("read-{label}");
    reply_call(first, &read_id, "Read", json!({"file_path":work.path()}));
    let second = next(requests).await;
    let result = reply(&second, &read_id);
    assert_eq!(
        result.tool_success,
        Some(true),
        "the allowed scoped Read must really work"
    );
    assert!(result.content.contains(work.marker));
    assert!(!result.content.contains(other.marker));
    let task_id = format!("task-{label}");
    reply_call(
        second,
        &task_id,
        "Task",
        json!({"tasks":[{"id":format!("step-{label}"),"content":"Read the owned result; await owner review","status":"completed"}]}),
    );
    let third = next(requests).await;
    assert_eq!(reply(&third, &task_id).tool_success, Some(true));
    third
        .reply
        .send(LLMChunk::Token(format!(
            "Verified own bounded fixture result: {}",
            work.marker
        )))
        .unwrap();
}

async fn submitted(state: &AppState, work: &Work, assignment: &str) -> Submission {
    tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = state.tickets.service().unwrap().published().unwrap().1;
            assert_ne!(snapshot.tickets[&work.id].state, WorkState::Accepted);
            if snapshot.tickets[&work.id].state == WorkState::Submitted
                && snapshot.assignments[assignment].process_stopped
            {
                let id = snapshot.tickets[&work.id]
                    .current_submission
                    .as_ref()
                    .unwrap();
                let submission = snapshot.submissions[id].clone();
                assert_eq!(submission.assignment_id, assignment);
                assert_eq!(snapshot.project.links[&work.id], work.project);
                assert_eq!(
                    snapshot.project.assignment_contexts[assignment],
                    snapshot.project.work_contexts[&work.id]
                );
                return submission;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual native Submission and independently confirmed stop")
}

#[actix_web::test]
async fn two_project_native_results_and_denied_reads_survive_host_restart() {
    let root = tempfile::tempdir().unwrap();
    let physical = root.path().canonicalize().unwrap();
    let marker_a = "ONLY_PROJECT_A_NATIVE_2265";
    let marker_b = "ONLY_PROJECT_B_NATIVE_2265";
    let wa = execution_workspace_at(&physical, "a", marker_a);
    let wb = execution_workspace_at(&physical, "b", marker_b);
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 9,
    });

    let (a, b, probe, first, a_assignment) = {
        let state = web::Data::new(host(&physical, provider.clone()).await);
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(bamboo_server::configure_routes),
        )
        .await;
        let pa = state.project_store.create("Project A", None).unwrap();
        let pa = state
            .project_store
            .bind_workspace(
                &pa.id,
                pa.revision,
                WorkspaceBinding {
                    path: wa.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let pb = state.project_store.create("Project B", None).unwrap();
        let pb = state
            .project_store
            .bind_workspace(
                &pb.id,
                pb.revision,
                WorkspaceBinding {
                    path: wb.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        assert_ne!(pa.id, pb.id);
        let a = prepare_work!(app, state, wa, pa, "a", marker_a);
        let b = prepare_work!(app, state, wb, pb, "b", marker_b);
        let probe = prepare_work!(app, state, wa, pa, "denied-probe", marker_a);
        assert_eq!(std::fs::read_to_string(a.path()).unwrap(), marker_a);
        assert_eq!(std::fs::read_to_string(b.path()).unwrap(), marker_b);
        let assignment = start_work!(app, state, a, "a");
        complete_own_work(&mut requests, &a, &b, "a").await;
        let first = submitted(&state, &a, &assignment).await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
        state.shutdown().await;
        (a, b, probe, first, assignment)
    };

    let (second, b_assignment, accept_a, accept_b, receipt_a, receipt_b) = {
        let state = web::Data::new(host(&physical, provider.clone()).await);
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(bamboo_server::configure_routes),
        )
        .await;
        let (service, owner) = state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let resumed = service.published().unwrap().1;
        assert_eq!(resumed.tickets[&a.id].state, WorkState::Submitted);
        assert_eq!(resumed.tickets[&b.id].state, WorkState::Ready);
        assert_eq!(
            canonical_bytes(&resumed.submissions[&first.id]).unwrap(),
            canonical_bytes(&first).unwrap()
        );
        assert!(resumed.assignments[&a_assignment].process_stopped);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
        let assignment = start_work!(app, state, b, "b");
        complete_own_work(&mut requests, &b, &a, "b").await;
        let second = submitted(&state, &b, &assignment).await;
        assert_ne!(first.id, second.id);
        assert_ne!(first.runtime.session_id, second.runtime.session_id);
        assert_ne!(first.runtime.run_id, second.runtime.run_id);
        let before = service.published().unwrap();
        let foreign = service
            .prepare_command(
                &owner,
                "wrong-project-submission",
                vec![Operation::Accept {
                    work_id: a.id.clone(),
                    submission_id: second.id.clone(),
                    evidence: vec!["Must be rejected: foreign exact Submission".into()],
                }],
            )
            .unwrap();
        owner_post!(
            app,
            "/api/v1/tickets/update",
            &foreign,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(service.published().unwrap().0, before.0);
        let accept_a = service
            .prepare_command(
                &owner,
                "accept-a",
                vec![Operation::Accept {
                    work_id: a.id.clone(),
                    submission_id: first.id.clone(),
                    evidence: vec!["Owner reviewed A's actual result".into()],
                }],
            )
            .unwrap();
        let receipt_a = owner_post!(app, "/api/v1/tickets/update", &accept_a, StatusCode::OK);
        let accept_b = service
            .prepare_command(
                &owner,
                "accept-b",
                vec![Operation::Accept {
                    work_id: b.id.clone(),
                    submission_id: second.id.clone(),
                    evidence: vec!["Owner reviewed B's actual result".into()],
                }],
            )
            .unwrap();
        let receipt_b = owner_post!(app, "/api/v1/tickets/update", &accept_b, StatusCode::OK);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 6);

        // Probe denial separately: a correct safety stop need not continue into success.
        let probe_assignment = start_work!(app, state, probe, "denied-probe");
        let request = next(&mut requests).await;
        assert!(request
            .tools
            .iter()
            .any(|tool| tool.function.name == "Read"));
        assert!(!serde_json::to_string(&request.messages)
            .unwrap()
            .contains(marker_b));
        reply_call(
            request,
            "probe-owned-read",
            "Read",
            json!({"file_path":probe.path()}),
        );
        let request = next(&mut requests).await;
        let owned = reply(&request, "probe-owned-read");
        assert_eq!(
            owned.tool_success,
            Some(true),
            "the probe must retain a working positive Read grant"
        );
        assert!(owned.content.contains(marker_a));
        assert!(!owned.content.contains(marker_b));
        reply_call(
            request,
            "forbidden-project-read",
            "Read",
            json!({"file_path":b.path()}),
        );
        let followup = tokio::time::timeout(WAIT, async {
            loop {
                tokio::select! {
                    request = requests.recv() => return request,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if service.published().unwrap().1.assignments[&probe_assignment].process_stopped { return None; }
                    }
                }
            }
        }).await.expect("denial is observed or the exact native task stops");
        if let Some(followup) = followup {
            let denied = reply(&followup, "forbidden-project-read");
            assert_eq!(denied.tool_success, Some(false));
            assert!(!denied.content.contains(marker_b));
            followup
                .reply
                .send(LLMChunk::Token(
                    "Blocked: another Assignment's file is not authorized.".into(),
                ))
                .unwrap();
        }
        tokio::time::timeout(WAIT, async {
            while !service.published().unwrap().1.assignments[&probe_assignment].process_stopped {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned denied-read probe terminates");
        let done = service.published().unwrap().1;
        let probe_child = done.assignments[&probe_assignment]
            .runtime
            .as_ref()
            .unwrap();
        let child = state
            .storage
            .load_session(&probe_child.session_id)
            .await
            .unwrap()
            .unwrap();
        let denied = child
            .messages
            .iter()
            .find(|m| {
                m.role == Role::Tool && m.tool_call_id.as_deref() == Some("forbidden-project-read")
            })
            .expect("canonical denied Read result retained");
        assert_eq!(denied.tool_success, Some(false));
        assert!(!denied.content.contains(marker_b));
        assert_ne!(done.tickets[&probe.id].state, WorkState::Accepted);
        assert_eq!(std::fs::read_to_string(a.path()).unwrap(), marker_a);
        assert_eq!(std::fs::read_to_string(b.path()).unwrap(), marker_b);
        state.shutdown().await;
        (second, assignment, accept_a, accept_b, receipt_a, receipt_b)
    };

    let state = web::Data::new(host(&physical, provider.clone()).await);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let snapshot = state.tickets.service().unwrap().published().unwrap().1;
    for (work, submission, assignment) in
        [(&a, &first, &a_assignment), (&b, &second, &b_assignment)]
    {
        assert_eq!(snapshot.tickets[&work.id].state, WorkState::Accepted);
        assert_eq!(
            snapshot.tickets[&work.id].accepted_submission.as_deref(),
            Some(submission.id.as_str())
        );
        assert_eq!(snapshot.project.links[&work.id], work.project);
        assert_eq!(
            canonical_bytes(&snapshot.submissions[&submission.id]).unwrap(),
            canonical_bytes(submission).unwrap()
        );
        assert!(snapshot.assignments[assignment].process_stopped);
    }
    assert_eq!(
        owner_post!(app, "/api/v1/tickets/update", &accept_a, StatusCode::OK),
        receipt_a
    );
    assert_eq!(
        owner_post!(app, "/api/v1/tickets/update", &accept_b, StatusCode::OK),
        receipt_b
    );
    let calls = provider.calls.load(Ordering::SeqCst);
    assert!((8..=9).contains(&calls));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    state.shutdown().await;
    eprintln!("cross_project_native: distinct Projects/contexts/reads/submissions, foreign Read and Accept refused, cold replay PASS; local provider={calls}, external=0");
}
