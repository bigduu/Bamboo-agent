//! Real native Question -> owned stop -> patrol wait -> process reopen.
//! The same test is its own bounded child-process fixture; only the parent
//! owns/removes the disposable root. No receipt/stop/Actor record is seeded.
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_domain::WorkspaceBinding;
use bamboo_engine::execution::AgentStatus;
use bamboo_llm::LLMChunk;
use bamboo_orchestration::patrol::{
    host as projection, program::ProgramPatrol, CheckpointPort, Clock, ObservationPort, Policy,
    PortError, Reason, Scope, Status, WaitKind,
};
use bamboo_server::AppState;
use bamboo_tickets::{
    Contract, Operation, Principal, ProjectContextSnapshot, RequestStatus, WorkState,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;

#[macro_use]
#[path = "common/native_work.rs"]
mod native_work;
use native_work::{execution_workspace, host, LocalProvider, CALL, RESULT, WAIT};

const SEED: &str = "BAMBOO_PATROL_2469_SEED_ROOT";
const CASE: &str = "native_question_wait_retains_incident_and_reopens_without_mutation";
const OBJECTIVE: &str = "Ask one precise owner question and yield without delivering the Work";

struct At(u64);
impl Clock for At {
    fn now_ms(&self) -> u64 {
        self.0
    }
}
fn now() -> u64 {
    chrono::Utc::now().timestamp_millis().try_into().unwrap()
}
fn user() -> Principal {
    Principal::User {
        user_id: "host-owner".into(),
    }
}

#[derive(Serialize, Deserialize)]
struct SeedReceipt {
    scope: Scope,
    issued: u64,
    until: u64,
    evidence: String,
}

async fn observation(state: &AppState, scope: &Scope) -> bamboo_orchestration::patrol::Observation {
    let mut source = state.tickets.patrol_observation(scope).await.unwrap();
    let sample = source.observation(scope, 0).unwrap();
    assert_eq!(source.observation(scope, 0), Err(PortError::Unavailable));
    assert_eq!(sample.started_at_ms, None);
    assert_eq!(sample.heartbeat_at_ms, None);
    assert_eq!(sample.progress, None);
    sample
}

fn actor_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(root: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>, depth: usize) {
        assert!(depth < 24);
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name() == ".git" {
                continue;
            }
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                collect(&entry.path(), result, depth + 1);
            } else if kind.is_file()
                && matches!(
                    entry.file_name().to_str(),
                    Some("actor-authority.json" | "actor-authority.initialized.json")
                )
            {
                result.insert(entry.path(), std::fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut result = BTreeMap::new();
    collect(root, &mut result, 0);
    result
}

async fn seed(root: &Path) {
    let workspace = execution_workspace(root);
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 1,
    });
    let state = actix_web::web::Data::new(host(root, provider.clone()).await);
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .configure(bamboo_server::routes::configure_routes),
    )
    .await;
    let project = state
        .project_store
        .create("Patrol Question Project", None)
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
    let (service, authority) = state.tickets.authority(user()).await.unwrap();
    // The required native packet contains this Contract twice and has a
    // 2048-byte line cap. Keep fixture input at its known-admitted size.
    let contract = Contract {
        title: "Exact native patrol Question".into(),
        objective: OBJECTIVE.into(),
        constraints: vec!["No model or observer may accept or restart this Work".into()],
        acceptance: vec!["Owner separately reviews an eventual submission".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    };
    let create = service
        .prepare_command(
            &authority,
            "patrol-create",
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
                    project_id: project.id.clone(),
                },
            ],
        )
        .unwrap();
    let created = owner_post!(
        app,
        "/api/v1/tickets/update",
        &create,
        actix_web::http::StatusCode::OK
    );
    let work = created["ids"]["work"].as_str().unwrap().to_owned();
    let ready = service
        .prepare_command(
            &authority,
            "patrol-ready",
            vec![
                Operation::SetProjectContext {
                    work_id: work.clone(),
                    context: ProjectContextSnapshot {
                        project_id: project.id.clone(),
                        project_revision: project.revision,
                        resource_revision: project.resource_revision,
                        workspace_binding: project.workspace_bindings[0].clone(),
                        contract_revision: service.published().unwrap().1.tickets[&work]
                            .contract_revision,
                        contract,
                        sources: BTreeMap::new(),
                    },
                },
                Operation::Ready {
                    work_id: work.clone(),
                },
            ],
        )
        .unwrap();
    state.tickets.update(user(), &ready).await.unwrap();
    let start = service
        .prepare_command(
            &authority,
            "patrol-start",
            vec![Operation::Start {
                work_id: work.clone(),
                temp_id: "attempt".into(),
                workspace: Some(workspace),
            }],
        )
        .unwrap();
    let dispatched = state.tickets.dispatch(user(), &start).await.unwrap();
    assert_eq!(dispatched["errors"], json!([]));
    let request = tokio::time::timeout(WAIT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(request
        .messages
        .iter()
        .any(|message| message.content.contains(OBJECTIVE)));
    assert!(request
        .tools
        .iter()
        .any(|tool| tool.function.name == "Task"));
    let snapshot = service.published().unwrap().1;
    let assignment = snapshot.tickets[&work]
        .active_assignment
        .as_ref()
        .unwrap()
        .clone();
    let scope = state
        .tickets
        .patrol_scope(project.id.as_str(), &work, &assignment)
        .await
        .unwrap();
    let receipt = snapshot.assignments[&assignment].runtime.as_ref().unwrap();
    let child = state
        .storage
        .load_runtime_control_plane(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(WAIT, async {
        while state
            .spawn_scheduler
            .native_task_liveness(&child.id, &receipt.run_id, child.created_at)
            .and_then(|live| live.heartbeat_at_ms())
            .is_none()
        {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the held provider allows a real native renewal before yielding the Question");
    let mut live = state.tickets.patrol_observation(&scope).await.unwrap();
    let entered = live.observation(&scope, 0).unwrap();
    assert!(entered.started_at_ms.is_some());
    assert!(entered.heartbeat_at_ms.is_some());
    assert_eq!(entered.progress, None);
    let mut checkpoints = state.tickets.open_patrol_checkpoints(&scope).await.unwrap();
    let program = ProgramPatrol::new(Policy::default()).unwrap();
    let mut source = state.tickets.patrol_observation(&scope).await.unwrap();
    assert_eq!(
        program
            .tick(&scope, &At(now()), &mut checkpoints, &mut source)
            .unwrap()
            .status,
        Status::Incident
    );
    let mut source = state.tickets.patrol_observation(&scope).await.unwrap();
    program
        .tick(&scope, &At(now()), &mut checkpoints, &mut source)
        .unwrap();
    let original = checkpoints
        .load(&scope)
        .unwrap()
        .unwrap()
        .checkpoint
        .incident
        .unwrap();
    assert_eq!(original.reason, Reason::EvidenceUnknown);
    assert!(original.escalation.is_some());
    request.reply.send(LLMChunk::ToolCalls(vec![ToolCall {
        id: CALL.into(), tool_type: "function".into(),
        function: FunctionCall { name: "Task".into(), arguments: json!({
            "tasks":[{"id":"own-question-step","content":"Await the precise owner answer","status":"blocked"}],
            "question":{"prompt":"Which exact bounded acceptance should this Work use?"}
        }).to_string() },
    }])).unwrap();
    let (snapshot, sample) = tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.published().unwrap().1;
            let attempt = &snapshot.assignments[&assignment];
            if attempt.process_stopped && attempt.awaiting_request.is_some() {
                match state.tickets.patrol_observation(&scope).await {
                    Ok(mut source) => {
                        let sample = source.observation(&scope, 0).unwrap();
                        if sample.expected_wait.is_some() {
                            break (snapshot, sample);
                        }
                    }
                    Err(_) => {}
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual Question/stop/typed runtime must join without fabricated evidence");
    assert_eq!(snapshot.tickets[&work].state, WorkState::Blocked);
    assert!(snapshot.tickets[&work].active_assignment.is_none());
    assert!(snapshot.submissions.is_empty());
    let receipt = snapshot.assignments[&assignment].runtime.as_ref().unwrap();
    // Owned stop precedes the SDK history barrier and final Actor projection.
    // Require the actual exact runner/router finalization before establishing
    // the byte baseline; this is not a delay or an inferred physical-stop proof.
    tokio::time::timeout(WAIT, async {
        loop {
            let runner_done = state
                .agent_runners
                .read()
                .await
                .get(&receipt.session_id)
                .is_some_and(|runner| {
                    runner.run_id == receipt.run_id
                        && matches!(runner.status, AgentStatus::Completed)
                        && runner.completed_at.is_some()
                });
            if runner_done
                && state
                    .session_activation_router
                    .current_run_id(&receipt.session_id)
                    .await
                    .is_none()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("exact native coordinator must complete its history/Actor finalization tail");
    let first_wait = sample.expected_wait.unwrap();
    let wait = observation(&state, &scope).await.expected_wait.unwrap();
    assert_eq!(
        (wait.issued_at_ms, wait.until_ms),
        (first_wait.issued_at_ms, first_wait.until_ms),
        "finalization must not extend the diagnostic allowance"
    );
    assert_eq!(wait.kind, WaitKind::User);
    assert_eq!(wait.progress_sequence, 0);
    assert_eq!(
        wait.until_ms - wait.issued_at_ms,
        Policy::default().max_expected_wait_ms
    );
    let actor_before = actor_files(root);
    assert!(!actor_before.is_empty());
    let actor = actor_before
        .values()
        .filter_map(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
        .find(|entry| entry["actor"]["actor_id"] == receipt.session_id)
        .unwrap();
    assert_eq!(actor["activation"]["run_id"], receipt.run_id);
    assert_eq!(actor["activation"]["status"], "succeeded");
    assert!(actor["activation"]["finished_at"].is_string());
    let child = state
        .storage
        .load_runtime_control_plane(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        actor["actor"]["session_created_at"],
        serde_json::to_value(child.created_at).unwrap()
    );
    let head_before = service.published().unwrap().0;
    for _ in 0..3 {
        assert_eq!(
            observation(&state, &scope).await.expected_wait,
            Some(wait.clone())
        );
    }
    let mut source = state.tickets.patrol_observation(&scope).await.unwrap();
    assert_eq!(
        program
            .tick(&scope, &At(now()), &mut checkpoints, &mut source)
            .unwrap()
            .status,
        Status::ExpectedWait
    );
    assert_eq!(
        checkpoints
            .load(&scope)
            .unwrap()
            .unwrap()
            .checkpoint
            .incident,
        Some(original)
    );
    assert_eq!(service.published().unwrap().0, head_before);
    assert_eq!(
        actor_files(root),
        actor_before,
        "observation must not ensure/refresh Actor authority"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    std::fs::write(
        root.join("2469-seed.json"),
        serde_json::to_vec(&SeedReceipt {
            scope,
            issued: wait.issued_at_ms,
            until: wait.until_ms,
            evidence: wait.evidence_ref,
        })
        .unwrap(),
    )
    .unwrap();
    state.shutdown().await;
}

async fn reopened(root: &Path, seed: SeedReceipt) {
    let (send, _requests) = mpsc::unbounded_channel();
    let provider = Arc::new(LocalProvider {
        requests: send,
        calls: AtomicUsize::new(0),
        max_requests: 0,
    });
    let state = host(root, provider.clone()).await;
    let (service, authority) = state.tickets.authority(user()).await.unwrap();
    let snapshot = service.published().unwrap().1;
    let assignment = &snapshot.assignments[&seed.scope.assignment_id];
    let request_id = assignment.awaiting_request.as_ref().unwrap().clone();
    let receipt = assignment.runtime.as_ref().unwrap().clone();
    let scope = state
        .tickets
        .patrol_scope(
            &seed.scope.project_id,
            &seed.scope.work_id,
            &seed.scope.assignment_id,
        )
        .await
        .unwrap();
    assert_eq!(scope, seed.scope);
    let before = service.published().unwrap().0;
    let actors = actor_files(root);
    let sample = observation(&state, &scope).await;
    let wait = sample.expected_wait.unwrap();
    assert_eq!(
        (wait.issued_at_ms, wait.until_ms, &wait.evidence_ref),
        (seed.issued, seed.until, &seed.evidence)
    );
    let mut bad_scope = scope.clone();
    bad_scope.generation += 1;
    assert!(state.tickets.patrol_observation(&bad_scope).await.is_err());
    bad_scope = scope.clone();
    bad_scope.project_id = bamboo_domain::ProjectId::new().to_string();
    assert!(state.tickets.patrol_observation(&bad_scope).await.is_err());
    let mut forged = projection::RuntimeObservation {
        scope: scope.clone(),
        source_commit: before.clone(),
        receipt,
        wait: projection::RuntimeWait {
            kind: WaitKind::User,
            registered_at_ms: seed.issued,
            timeout_at_ms: None,
            request_id: Some(request_id.clone()),
            evidence_ref: seed.evidence,
        },
    };
    forged.receipt.run_id = uuid::Uuid::new_v4().to_string();
    assert!(matches!(
        projection::read_observation_with_runtime(
            &service,
            &authority,
            &scope,
            &forged,
            &Policy::default()
        ),
        Err(PortError::Denied)
    ));
    let program = ProgramPatrol::new(Policy::default()).unwrap();
    let mut checkpoints = state.tickets.open_patrol_checkpoints(&scope).await.unwrap();
    let original = checkpoints
        .load(&scope)
        .unwrap()
        .unwrap()
        .checkpoint
        .incident;
    let mut source = state.tickets.patrol_observation(&scope).await.unwrap();
    assert_eq!(
        program
            .tick(&scope, &At(now()), &mut checkpoints, &mut source)
            .unwrap()
            .status,
        Status::ExpectedWait
    );
    assert_eq!(
        checkpoints
            .load(&scope)
            .unwrap()
            .unwrap()
            .checkpoint
            .incident,
        original
    );
    // Diagnostic allowance expiry does not expire/answer/cancel the business Question.
    let mut source = state.tickets.patrol_observation(&scope).await.unwrap();
    program
        .tick(&scope, &At(seed.until + 1), &mut checkpoints, &mut source)
        .unwrap();
    assert_eq!(
        checkpoints
            .load(&scope)
            .unwrap()
            .unwrap()
            .checkpoint
            .incident
            .unwrap()
            .reason,
        Reason::WaitExpired
    );
    assert_eq!(service.published().unwrap().0, before);
    assert_eq!(
        service.published().unwrap().1.requests[&request_id].status,
        RequestStatus::Open
    );
    assert_eq!(actor_files(root), actors);
    let answer = service
        .prepare_command(
            &authority,
            "patrol-answer",
            vec![Operation::Answer {
                request_id: request_id.clone(),
                prompt_revision: snapshot.requests[&request_id].prompt_revision,
                answer: format!("The eventual result must be '{RESULT}' and receive owner review."),
            }],
        )
        .unwrap();
    state.tickets.update(user(), &answer).await.unwrap();
    assert_eq!(
        service.published().unwrap().1.requests[&request_id].status,
        RequestStatus::Answered
    );
    assert!(
        state.tickets.patrol_observation(&scope).await.is_err(),
        "answered saved metadata cannot retain an expected User wait"
    );
    assert!(
        projection::current_assignment_id(&service.published().unwrap().1, &scope.work_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(service.published().unwrap().1.assignments.len(), 1);
    state.shutdown().await;
}

#[test]
fn native_question_wait_retains_incident_and_reopens_without_mutation() {
    if let Some(root) = std::env::var_os(SEED) {
        actix_web::rt::System::new().block_on(seed(Path::new(&root)));
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let physical = root.path().canonicalize().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CASE, "--nocapture", "--test-threads=1"])
        .env(SEED, &physical)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "real seed process failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let seed: SeedReceipt =
        serde_json::from_slice(&std::fs::read(physical.join("2469-seed.json")).unwrap()).unwrap();
    actix_web::rt::System::new().block_on(reopened(&physical, seed));
}
