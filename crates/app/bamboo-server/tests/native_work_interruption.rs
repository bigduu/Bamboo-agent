//! Real owner cancellation after a native Task write, with no synthesized stop/result.
use actix_web::{http::StatusCode, test, web, App};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_domain::{Role, WorkspaceBinding};
use bamboo_engine::ticket_worker_plan::TicketWorkerPlan;
use bamboo_llm::LLMChunk;
use bamboo_tickets::{
    canonical_bytes, AssignmentState, Command, CommandSource, Contract, Error, Operation,
    Principal, ProjectContextSnapshot, StepStatus, WorkState,
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
use native_work::{execution_workspace, host, LocalProvider, ModelRequest, WAIT};

const FIRST: &str = "committed-before-cancel";

macro_rules! prepare_work {
    ($app:expr, $service:expr, $owner:expr, $project:expr, $label:expr) => {{
        let contract = Contract {
            title: format!("Native interruption {}", $label),
            objective: "Commit a bounded Task plan; explicit owner control remains authoritative"
                .into(),
            constraints: vec![
                "Cancellation never grants another attempt or owner acceptance".into(),
            ],
            acceptance: vec!["Exact stopped result requires owner review".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let create = $service
            .prepare_command(
                &$owner,
                &format!("create-{}", $label),
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: bamboo_tickets::TicketKind::Work,
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
        let snapshot = $service.published().unwrap().1;
        let ready = $service
            .prepare_command(
                &$owner,
                &format!("ready-{}", $label),
                vec![
                    Operation::SetProjectContext {
                        work_id: id.clone(),
                        context: ProjectContextSnapshot {
                            project_id: $project.id.clone(),
                            project_revision: $project.revision,
                            resource_revision: $project.resource_revision,
                            workspace_binding: $project.workspace_bindings[0].clone(),
                            contract_revision: snapshot.tickets[&id].contract_revision,
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
        id
    }};
}

macro_rules! start_work {
    ($app:expr, $service:expr, $owner:expr, $work:expr, $workspace:expr, $label:expr) => {{
        let command = $service
            .prepare_command(
                &$owner,
                &format!("start-{}", $label),
                vec![Operation::Start {
                    work_id: $work.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some($workspace.clone()),
                }],
            )
            .unwrap();
        let response = owner_post!($app, "/api/v1/tickets/dispatch", &command, StatusCode::OK);
        assert_eq!(response["errors"], json!([]));
        let assignment = response["receipt"]["ids"]["assignment"]
            .as_str()
            .unwrap()
            .to_owned();
        (assignment, command, response["receipt"].clone())
    }};
}

async fn next(requests: &mut mpsc::UnboundedReceiver<ModelRequest>) -> ModelRequest {
    tokio::time::timeout(WAIT, requests.recv())
        .await
        .unwrap()
        .unwrap()
}

fn task_chunk(id: &str, arguments: &Value) -> LLMChunk {
    LLMChunk::ToolCalls(vec![ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "Task".into(),
            arguments: arguments.to_string(),
        },
    }])
}

fn task_result<'a>(request: &'a ModelRequest, id: &str) -> &'a bamboo_domain::Message {
    request
        .messages
        .iter()
        .find(|message| message.role == Role::Tool && message.tool_call_id.as_deref() == Some(id))
        .expect("real Task result reaches provider continuation")
}

#[actix_web::test]
async fn cancelled_inflight_work_cannot_replay_task_or_regain_authority_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let physical = root.path().canonicalize().unwrap();
    let workspace = execution_workspace(&physical);
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 4,
    });
    let first_args = json!({"tasks":[{
        "id":"first-step", "content":"One canonical side effect before interruption", "status":"completed"
    }]});
    let late_args = json!({"tasks":[{
        "id":"forbidden-late-step", "content":"Must never be committed after cancellation", "status":"completed"
    }]});

    let (
        project_id,
        work,
        assignment,
        start,
        start_receipt,
        cancel,
        cancel_receipt,
        saved_plan,
        task_receipt_id,
        task_receipt,
        stop_receipt,
    ) = {
        let state = web::Data::new(host(&physical, provider.clone()).await);
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(bamboo_server::configure_routes),
        )
        .await;
        let project = state
            .project_store
            .create("Interruption Project", None)
            .unwrap();
        let project = state
            .project_store
            .bind_workspace(
                &project.id,
                project.revision,
                WorkspaceBinding {
                    path: workspace.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let (service, owner) = state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let work = prepare_work!(app, service, owner, project, "inflight");
        let (assignment, start, start_receipt) =
            start_work!(app, service, owner, work, workspace, "inflight");
        let first = next(&mut requests).await;
        assert!(first
            .tools
            .iter()
            .any(|schema| schema.function.name == "Task"));
        let before_revision = service.published().unwrap().1.assignments[&assignment]
            .plan
            .plan_revision;
        first.reply.send(task_chunk(FIRST, &first_args)).unwrap();
        let held = next(&mut requests).await;
        assert_eq!(task_result(&held, FIRST).tool_success, Some(true));

        let running = service.published().unwrap().1;
        assert_eq!(running.tickets[&work].state, WorkState::Active);
        assert!(!running.assignments[&assignment].process_stopped);
        let plan = &running.assignments[&assignment].plan;
        assert_eq!(plan.plan_revision, before_revision + 1);
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].status, Some(StepStatus::Completed));
        let saved_plan = canonical_bytes(plan).unwrap();
        let task_receipt_id = format!(
            "worker-plan/{}/{FIRST}",
            running.assignments[&assignment].dispatch_key
        );
        let task_receipt = canonical_bytes(&running.receipts[&task_receipt_id]).unwrap();
        let task_command: Command =
            serde_json::from_str(&running.receipts[&task_receipt_id].canonical_request).unwrap();
        assert!(
            matches!(task_command.source, Some(CommandSource::WorkerTask { arguments }) if arguments == first_args)
        );
        let runtime = running.assignments[&assignment].runtime.clone().unwrap();
        // Captured only from the real admitted Child/Run. It will be used for a
        // cached reply and negative new-command checks, never to create stop evidence.
        let retained =
            TicketWorkerPlan::from_runtime_receipt(service.clone(), &assignment).unwrap();
        let mut stale_child = state
            .storage
            .load_session(&runtime.session_id)
            .await
            .unwrap()
            .unwrap();
        let cancel = service
            .prepare_command(
                &owner,
                "cancel-inflight",
                vec![Operation::Cancel {
                    work_id: work.clone(),
                }],
            )
            .unwrap();
        let cancel_receipt = owner_post!(app, "/api/v1/tickets/update", &cancel, StatusCode::OK);
        tokio::time::timeout(WAIT, async {
            loop {
                let snapshot = service.published().unwrap().1;
                if snapshot.assignments[&assignment].process_stopped {
                    assert_eq!(snapshot.tickets[&work].state, WorkState::Cancelled);
                    assert_eq!(
                        snapshot.assignments[&assignment].state,
                        AssignmentState::Cancelled
                    );
                    assert!(snapshot.tickets[&work].active_assignment.is_none());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real owner cancellation joins the exact native run");
        assert!(
            held.reply
                .send(task_chunk("late-provider-task", &late_args))
                .is_err(),
            "a confirmed stopped run cannot consume another provider response"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);

        // Exact cached projection is allowed; it must not be confused with a
        // renewed tool grant or replace the current plan with a new command.
        let stopped_revision = service.published().unwrap().0;
        retained
            .apply_task(&mut stale_child, FIRST, &first_args)
            .unwrap();
        assert!(matches!(
            retained.apply_task(&mut stale_child, "new-after-cancel", &late_args),
            Err(Error::ScopeDenied(_))
        ));
        assert_eq!(service.published().unwrap().0, stopped_revision);
        let stopped = service.published().unwrap().1;
        assert_eq!(
            canonical_bytes(&stopped.assignments[&assignment].plan).unwrap(),
            saved_plan
        );
        assert_eq!(
            canonical_bytes(&stopped.receipts[&task_receipt_id]).unwrap(),
            task_receipt
        );
        assert!(!stopped.receipts.contains_key(&format!(
            "worker-plan/{}/new-after-cancel",
            stopped.assignments[&assignment].dispatch_key
        )));
        assert!(stopped.tickets[&work].current_submission.is_none());
        assert!(stopped.tickets[&work].accepted_submission.is_none());
        assert!(stopped
            .submissions
            .values()
            .filter(|s| s.work_id == work)
            .all(|s| s.stale));
        // Ticket stop is committed after the task/resource join, before the
        // SDK's terminal Session publication tail. Observe both boundaries;
        // the first receipt alone does not mean every diagnostic is published.
        let child = tokio::time::timeout(WAIT, async {
            loop {
                let child = state
                    .storage
                    .load_session(&runtime.session_id)
                    .await
                    .unwrap()
                    .unwrap();
                if child
                    .metadata
                    .get("runtime.local_task.state")
                    .map(String::as_str)
                    == Some("stopped")
                    && matches!(
                        child.last_run_status().as_deref(),
                        Some("completed" | "error" | "timeout" | "cancelled" | "skipped")
                    )
                {
                    break child;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("SDK durably publishes the already joined Child's terminal state");
        let stop_receipt: Value =
            serde_json::from_str(&child.metadata["ticket.runtime.owned_stop.v1"]).unwrap();
        assert_eq!(stop_receipt["receipt"], json!(runtime));
        assert_eq!(stop_receipt["completed"], false);
        let instance =
            uuid::Uuid::parse_str(stop_receipt["task_instance"].as_str().unwrap()).unwrap();
        assert!(!instance.is_nil());
        assert_eq!(stop_receipt["child_birth"], json!(child.created_at));
        assert_eq!(
            child.metadata["runtime.local_task.instance"],
            instance.to_string()
        );
        assert_eq!(child.metadata["runtime.local_task.state"], "stopped");
        drop(retained);
        state.shutdown().await;
        (
            project.id,
            work,
            assignment,
            start,
            start_receipt,
            cancel,
            cancel_receipt,
            saved_plan,
            task_receipt_id,
            task_receipt,
            stop_receipt,
        )
    };

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
    let recovered = service.published().unwrap().1;
    assert_eq!(recovered.tickets[&work].state, WorkState::Cancelled);
    assert!(recovered.tickets[&work].current_submission.is_none());
    assert!(recovered.tickets[&work].accepted_submission.is_none());
    assert!(recovered.assignments[&assignment].process_stopped);
    assert_eq!(
        canonical_bytes(&recovered.assignments[&assignment].plan).unwrap(),
        saved_plan
    );
    assert_eq!(
        canonical_bytes(&recovered.receipts[&task_receipt_id]).unwrap(),
        task_receipt
    );
    assert!(matches!(
        TicketWorkerPlan::from_runtime_receipt(service.clone(), &assignment),
        Err(Error::ScopeDenied(_))
    ));
    let child_id = &recovered.assignments[&assignment]
        .runtime
        .as_ref()
        .unwrap()
        .session_id;
    let child = state.storage.load_session(child_id).await.unwrap().unwrap();
    let cold_stop: Value =
        serde_json::from_str(&child.metadata["ticket.runtime.owned_stop.v1"]).unwrap();
    assert_eq!(cold_stop, stop_receipt);
    assert_eq!(
        owner_post!(app, "/api/v1/tickets/update", &cancel, StatusCode::OK),
        cancel_receipt
    );
    let replay = owner_post!(app, "/api/v1/tickets/dispatch", &start, StatusCode::OK);
    assert_eq!(replay["receipt"], start_receipt);
    assert_eq!(service.published().unwrap().1.assignments.len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);

    // Fresh owner authorization is a positive control: cancellation is scoped,
    // not a global disable switch. The old exact runtime still has no new grant.
    let project = state.project_store.get(&project_id).unwrap();
    let fresh = prepare_work!(app, service, owner, project, "fresh");
    let (fresh_assignment, _, _) = start_work!(app, service, owner, fresh, workspace, "fresh");
    let request = next(&mut requests).await;
    request.reply.send(task_chunk("fresh-authorized-task", &json!({"tasks":[{
        "id":"fresh-step", "content":"A separately authorized current Work", "status":"completed"
    }]}))).unwrap();
    let request = next(&mut requests).await;
    assert_eq!(
        task_result(&request, "fresh-authorized-task").tool_success,
        Some(true)
    );
    assert!(matches!(
        TicketWorkerPlan::from_runtime_receipt(service.clone(), &assignment),
        Err(Error::ScopeDenied(_))
    ));
    request
        .reply
        .send(LLMChunk::Token(
            "Fresh Work result still requires explicit owner acceptance.".into(),
        ))
        .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.published().unwrap().1;
            if snapshot.tickets[&fresh].state == WorkState::Submitted
                && snapshot.assignments[&fresh_assignment].process_stopped
            {
                assert_eq!(snapshot.tickets[&work].state, WorkState::Cancelled);
                assert!(snapshot.tickets[&fresh].accepted_submission.is_none());
                assert_ne!(
                    snapshot.assignments[&assignment].runtime,
                    snapshot.assignments[&fresh_assignment].runtime
                );
                assert_eq!(
                    canonical_bytes(&snapshot.assignments[&assignment].plan).unwrap(),
                    saved_plan
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fresh exact authorization still executes and stops normally");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err()
    );
    state.shutdown().await;
    eprintln!("native_inflight_cancel: real Task write, owner Cancel, joined stop, cached acknowledgement only, cold replay and fresh grant PASS; local provider=4 external=0");
}
