//! Two real Project workers and an isolated denied-read probe over existing Host ports.
use actix_web::{body::MessageBody, http::StatusCode, test, web, App};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::AgentEvent;
use bamboo_domain::{ProjectId, Role, Session, TaskItemStatus, WorkspaceBinding};
use bamboo_engine::ticket_worker_plan::{TicketWorkerPlan, WorkerLocalPlan};
use bamboo_llm::LLMChunk;
use bamboo_server::AppState;
use bamboo_tickets::{
    canonical_bytes, Contract, ExecutionWorkspace, Operation, Principal, ProjectContextSnapshot,
    StepStatus, Submission, TicketKind, WorkState,
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
                            workspace_binding: WorkspaceBinding {
                                path: $workspace.repo.clone(),
                                label: None,
                                git_common_dir: None,
                            },
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
    state: &AppState,
    assignment: &str,
    requests: &mut mpsc::UnboundedReceiver<ModelRequest>,
    work: &Work,
    other: &Work,
    label: &str,
) -> (TicketWorkerPlan, Session) {
    let first = next(requests).await;
    let service = state.tickets.service().unwrap();
    let snapshot = service.published().unwrap().1;
    let receipt = snapshot.assignments[assignment].runtime.as_ref().unwrap();
    let mut child = state
        .storage
        .load_session(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        child.project_id_meta().is_none(),
        "Work must retain its global Supervisor's tree Project"
    );
    assert_eq!(
        child.root_session_id,
        snapshot.binding.supervisor_session_id
    );
    // Mirror Runtime::execute's readonly caller projection, using the native
    // receipt that already reached this provider gate. Never persist test inputs.
    child
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .run_id = receipt.run_id.clone();
    let plan = TicketWorkerPlan::from_runtime_receipt(service, assignment).unwrap();
    let path = Some(std::path::Path::new(&work.workspace.worktree));
    assert!(
        plan.workspace_project_context(&child, path).is_err(),
        "unbound capability grants no workspace"
    );
    plan.bind_session(&mut child).unwrap();
    assert_eq!(
        plan.workspace_project_context(&child, path)
            .unwrap()
            .unwrap()
            .project_id,
        work.project
    );
    let display = bamboo_config::paths::path_to_display_string(std::path::Path::new(
        &work.workspace.worktree,
    ));
    let nested = std::path::Path::new(&display).join("display-form");
    std::fs::create_dir(&nested).unwrap();
    let alias = nested.join("..");
    assert_ne!(alias, std::path::Path::new(&work.workspace.worktree));
    assert_eq!(
        alias.canonicalize().unwrap(),
        std::path::Path::new(&work.workspace.worktree)
    );
    let mut displayed = child.clone();
    displayed.set_workspace_path_meta(alias.to_string_lossy());
    assert_eq!(
        plan.workspace_project_context(&displayed, Some(&alias))
            .unwrap()
            .unwrap()
            .project_id,
        work.project,
        "a valid Host display spelling retains only the same physical workspace grant"
    );
    displayed.set_workspace_path_meta(&other.workspace.worktree);
    assert!(plan.workspace_project_context(&displayed, path).is_err());
    displayed.set_workspace_path_meta(nested.join("missing").to_string_lossy());
    assert!(plan.workspace_project_context(&displayed, path).is_err());
    #[cfg(unix)]
    {
        let selected = std::path::Path::new(&work.workspace.worktree);
        let retained = selected.with_extension("retained");
        let foreign = tempfile::tempdir().unwrap();
        std::fs::rename(selected, &retained).unwrap();
        std::os::unix::fs::symlink(foreign.path(), selected).unwrap();
        let result = plan.workspace_project_context(&child, path);
        std::fs::remove_file(selected).unwrap();
        std::fs::rename(&retained, selected).unwrap();
        assert!(
            result.is_err(),
            "a replaced frozen directory cannot be re-canonicalized into a new grant"
        );
    }
    assert!(plan
        .workspace_project_context(
            &child,
            Some(std::path::Path::new(&other.workspace.worktree))
        )
        .is_err());
    let mut wrong = child.clone();
    wrong.created_at += chrono::Duration::seconds(1);
    assert!(plan.workspace_project_context(&wrong, path).is_err());
    let mut wrong = child.clone();
    wrong.parent_created_at = None;
    assert!(plan.workspace_project_context(&wrong, path).is_err());
    let mut wrong = child.clone();
    wrong.root_session_id = "foreign-root".into();
    assert!(plan.workspace_project_context(&wrong, path).is_err());
    let mut wrong = child.clone();
    wrong.agent_runtime_state.as_mut().unwrap().run_id = "foreign-run".into();
    assert!(plan.workspace_project_context(&wrong, path).is_err());
    let mut wrong = child.clone();
    wrong.set_project_id_meta(other.project.to_string());
    assert!(plan.workspace_project_context(&wrong, path).is_err());
    let mut wrong = child.clone();
    let mut dispatch = bamboo_engine::ticket_runtime::read_dispatch(&wrong)
        .unwrap()
        .unwrap();
    dispatch.spec_hash = "foreign-spec".into();
    wrong.metadata.insert(
        bamboo_engine::ticket_runtime::TICKET_DISPATCH_KEY.into(),
        serde_json::to_string(&dispatch).unwrap(),
    );
    assert!(plan.workspace_project_context(&wrong, path).is_err());
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
    (plan, child)
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
async fn native_work_read_activity_cannot_advance_the_committed_plan() {
    let root = tempfile::tempdir().unwrap();
    let physical = root.path().canonicalize().unwrap();
    let marker = "WORK_READ_ACTIVITY";
    let workspace = execution_workspace_at(&physical, "activity", marker);
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 4,
    });
    let state = web::Data::new(host(&physical, provider.clone()).await);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let project = state
        .project_store
        .create_with_project_path("Work activity", None, &workspace.repo, Vec::new())
        .unwrap();
    let work = prepare_work!(app, state, workspace, project, "activity", marker);
    let assignment = start_work!(app, state, work, "activity");
    let first = next(&mut requests).await;
    let service = state.tickets.service().unwrap();
    let child_id = service.published().unwrap().1.assignments[&assignment]
        .runtime
        .as_ref()
        .unwrap()
        .session_id
        .clone();
    let mut events = state.get_session_event_sender(&child_id).await.subscribe();
    let task = |status| json!({"tasks":[{"id":"read-step","content":"Read the assigned fixture and record evidence","status":status}]});
    reply_call(first, "plan-start", "Task", task("in_progress"));
    let read_round = next(&mut requests).await;
    assert_eq!(reply(&read_round, "plan-start").tool_success, Some(true));
    // The forwarder caches each committed plan before broadcasting it. Seeing
    // this event on the original connection fences the later replay check.
    let initial_plan = tokio::time::timeout(WAIT, async {
        loop {
            if let AgentEvent::TaskListUpdated { task_list, version } = events.recv().await.unwrap()
            {
                assert_eq!(task_list.session_id, child_id);
                break (version, task_list.items[0].status.clone());
            }
        }
    })
    .await
    .expect("original connection receives the first committed plan");
    let reads = (0..4)
        .map(|index| ToolCall {
            id: format!("read-activity-{index}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: json!({"file_path":work.path()}).to_string(),
            },
        })
        .collect::<Vec<_>>();
    let read_ids = reads
        .iter()
        .map(|call| call.id.clone())
        .collect::<BTreeSet<_>>();
    read_round.reply.send(LLMChunk::ToolCalls(reads)).unwrap();
    let finish_plan = next(&mut requests).await;
    for id in &read_ids {
        let result = reply(&finish_plan, id);
        assert_eq!(result.tool_success, Some(true));
        assert!(result.content.contains(marker));
    }
    let before = service.published().unwrap().1;
    let plan = &before.assignments[&assignment].plan;
    assert_eq!(plan.plan_revision, 1);
    assert_eq!(plan.steps[0].status, Some(StepStatus::InProgress));
    let child = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.task_list_version_meta().as_deref(), Some("1"));
    assert_eq!(
        child.task_list.as_ref().unwrap().items[0].status,
        TaskItemStatus::InProgress
    );

    // Start a second client after the tool activity, while the real provider
    // is still held before the explicit completion. It must replay v1.
    let reconnect = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(&format!("/api/v1/events/{child_id}"))
            .to_request(),
    )
    .await;
    assert_eq!(reconnect.status(), StatusCode::OK);
    let mut reconnect = Box::pin(reconnect.into_body());
    let (version, list) = tokio::time::timeout(WAIT, async {
        loop {
            let bytes = std::future::poll_fn(|cx| reconnect.as_mut().poll_next(cx))
                .await
                .expect("reconnect closed before replay")
                .expect("SSE body frame");
            let frame = std::str::from_utf8(&bytes).unwrap();
            for payload in frame.lines().filter_map(|line| line.strip_prefix("data: ")) {
                match serde_json::from_str::<AgentEvent>(payload) {
                    Ok(AgentEvent::TaskListUpdated { version, task_list }) => {
                        return (version, task_list);
                    }
                    Ok(AgentEvent::TaskListItemProgress { .. }) => {
                        panic!("reconnect must not replay speculative progress");
                    }
                    _ => {}
                }
            }
        }
    })
    .await
    .expect("late subscriber receives the committed native Work plan");
    assert_eq!(version, Some(1));
    assert_eq!(list.session_id, child_id);
    assert_eq!(list.items[0].id, "read-step");
    assert_eq!(list.items[0].status, TaskItemStatus::InProgress);
    drop(reconnect);

    reply_call(finish_plan, "plan-finish", "Task", task("completed"));
    let finish_run = next(&mut requests).await;
    assert_eq!(reply(&finish_run, "plan-finish").tool_success, Some(true));
    let mut observed_reads = BTreeSet::new();
    let mut observed_plans = vec![initial_plan];
    let mut speculative = Vec::new();
    tokio::time::timeout(WAIT, async {
        loop {
            match events.recv().await.unwrap() {
                AgentEvent::TaskListUpdated { task_list, version } => {
                    assert_eq!(task_list.session_id, child_id);
                    observed_plans.push((version, task_list.items[0].status.clone()));
                    if version == Some(2) {
                        break;
                    }
                }
                AgentEvent::TaskListItemProgress {
                    version, status, ..
                } => {
                    speculative.push((version, status));
                }
                AgentEvent::ToolComplete {
                    tool_call_id,
                    result,
                } if read_ids.contains(&tool_call_id) => {
                    assert!(result.success);
                    observed_reads.insert(tool_call_id);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("observe both real Task commits and the intervening tool activity");
    assert_eq!(observed_reads, read_ids);
    assert_eq!(
        observed_plans,
        [
            (Some(1), TaskItemStatus::InProgress),
            (Some(2), TaskItemStatus::Completed)
        ],
    );
    finish_run
        .reply
        .send(LLMChunk::Token(format!(
            "Read the scoped evidence: {marker}"
        )))
        .unwrap();
    submitted(&state, &work, &assignment).await;
    let stopped = service.published().unwrap().1;
    assert_eq!(stopped.assignments[&assignment].plan.plan_revision, 2);
    assert_eq!(
        stopped.assignments[&assignment].plan.steps[0].status,
        Some(StepStatus::Completed)
    );
    let child = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.task_list_version_meta().as_deref(), Some("2"));
    assert_eq!(
        child.task_list.as_ref().unwrap().items[0].status,
        TaskItemStatus::Completed
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    assert!(
        speculative.is_empty(),
        "tool activity cannot publish uncommitted Work Task status: {speculative:?}"
    );
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
        let pa = state
            .project_store
            .create_with_project_path("Project A", None, &wa.repo, Vec::new())
            .unwrap();
        let pa = state
            .project_store
            .bind_workspace(
                &pa.id,
                pa.revision,
                WorkspaceBinding {
                    path: wa.worktree.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let pb = state
            .project_store
            .create_with_project_path("Project B", None, &wb.repo, Vec::new())
            .unwrap();
        let pb = state
            .project_store
            .bind_workspace(
                &pb.id,
                pb.revision,
                WorkspaceBinding {
                    path: wb.worktree.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        assert_ne!(pa.id, pb.id);
        for (workspace, project) in [(&wa, &pa), (&wb, &pb)] {
            assert_eq!(
                state
                    .project_store
                    .find_workspace_owner_for_path(&workspace.worktree)
                    .unwrap()
                    .unwrap()
                    .id,
                project.id
            );
        }
        let a = prepare_work!(app, state, wa, pa, "a", marker_a);
        let b = prepare_work!(app, state, wb, pb, "b", marker_b);
        let probe = prepare_work!(app, state, wa, pa, "denied-probe", marker_a);
        assert_eq!(std::fs::read_to_string(a.path()).unwrap(), marker_a);
        assert_eq!(std::fs::read_to_string(b.path()).unwrap(), marker_b);
        let assignment = start_work!(app, state, a, "a");
        let (plan, child) =
            complete_own_work(&state, &assignment, &mut requests, &a, &b, "a").await;
        let first = submitted(&state, &a, &assignment).await;
        assert!(
            plan.workspace_project_context(
                &child,
                Some(std::path::Path::new(&a.workspace.worktree))
            )
            .is_err(),
            "stopped Work cannot reuse a prior workspace capability"
        );
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
        let (plan, child) =
            complete_own_work(&state, &assignment, &mut requests, &b, &a, "b").await;
        let second = submitted(&state, &b, &assignment).await;
        assert!(plan
            .workspace_project_context(&child, Some(std::path::Path::new(&b.workspace.worktree)))
            .is_err());
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
