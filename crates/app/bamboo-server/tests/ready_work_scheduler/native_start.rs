//! Live native entry, renewal and verified progress from the real runner.
use super::*;
use bamboo_domain::ActorDirectoryPort;
use bamboo_orchestration::patrol::{Observation, ObservationPort, Scope};

async fn sample(fixture: &Fixture, work: &str, assignment: &str) -> Observation {
    let scope = fixture
        .state
        .tickets
        .patrol_scope(fixture.project.as_str(), work, assignment)
        .await
        .unwrap();
    let mut port = fixture
        .state
        .tickets
        .patrol_observation(&scope)
        .await
        .unwrap();
    port.observation(&scope, 0).unwrap()
}

async fn sample_http(
    fixture: &Fixture,
    scope: &Scope,
    expected_status: actix_web::http::StatusCode,
) -> serde_json::Value {
    use actix_web::{test, App};
    let app = test::init_service(
        App::new()
            .app_data(fixture.state.clone())
            .configure(bamboo_server::routes::configure_routes),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/patrol/observation")
            .peer_addr("127.0.0.1:43210".parse().unwrap())
            .set_json(json!({
                "project_id":scope.project_id,
                "work_id":scope.work_id,
                "assignment_id":scope.assignment_id
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), expected_status);
    if response.status().is_success() {
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    }
    test::read_body_json(response).await
}

fn file_call(request: Request, id: &str, tool: &str, arguments: serde_json::Value) {
    assert!(request
        .tools
        .iter()
        .any(|schema| schema.function.name == tool));
    request
        .reply
        .send(Reply::File(ToolCall {
            id: id.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: tool.into(),
                arguments: arguments.to_string(),
            },
        }))
        .unwrap();
}

#[tokio::test]
async fn live_workspace_progress_requires_an_actual_committed_native_write() {
    use actix_web::http::StatusCode;
    use bamboo_orchestration::patrol::{
        program::ProgramPatrol, CheckpointPort, Clock, Policy, Reason, Status,
    };
    use bamboo_server::app_state::ticket_application::PatrolCheckpointStore;

    struct FixedClock(u64);
    impl Clock for FixedClock {
        fn now_ms(&self) -> u64 {
            self.0
        }
    }
    let clock = || FixedClock(chrono::Utc::now().timestamp_millis().try_into().unwrap());
    let mut fixture = Fixture::with_tools(1, true, &["Task", "Read", "Write"]).await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    let first = fixture.next_request().await;
    let work = fixture.works[0].id.clone();
    let path = Path::new(&fixture.works[0].workspace.worktree).join("native-result.txt");
    let before = fixture.snapshot();
    let assignment = before.tickets[&work].active_assignment.clone().unwrap();
    let receipt = before.assignments[&assignment].runtime.clone().unwrap();
    let initial = sample(&fixture, &work, &assignment).await;
    let entered = initial.started_at_ms.expect("real native Task entered");
    assert_eq!(initial.progress, None);
    assert!(!path.exists());
    let scope = initial.scope;
    let live = sample_http(&fixture, &scope, StatusCode::OK).await;
    assert_eq!(live["scope"], serde_json::to_value(&scope).unwrap());
    assert_eq!(live["state"], "observed");
    assert_eq!(live["started_at_ms"], entered);
    assert!(live["progress"].is_null());
    let program = ProgramPatrol::new(Policy::default()).unwrap();
    let mut journal = PatrolCheckpointStore::open(
        &fixture
            ._root
            .path()
            .canonicalize()
            .unwrap()
            .join("progress-control"),
    )
    .unwrap();
    let mut before_write = fixture
        .state
        .tickets
        .patrol_observation(&scope)
        .await
        .unwrap();
    assert_eq!(
        program
            .tick(&scope, &clock(), &mut journal, &mut before_write)
            .unwrap()
            .status,
        Status::Incident
    );
    assert_eq!(
        journal
            .load(&scope)
            .unwrap()
            .unwrap()
            .checkpoint
            .incident
            .unwrap()
            .reason,
        Reason::EvidenceUnknown
    );

    file_call(
        first,
        "work-progress-write",
        "Write",
        json!({"file_path":path,"content":"verified output"}),
    );
    let after_write = fixture.next_request().await;
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "verified output");
    let observed = sample(&fixture, &work, &assignment).await;
    let progress = observed
        .progress
        .expect("committed artifact was observed while live");
    let snapshot = fixture.snapshot();
    let (effect_id, effect) = snapshot.assignments[&assignment]
        .effects
        .iter()
        .next()
        .unwrap();
    assert_eq!(effect.state, bamboo_tickets::EffectState::Succeeded);
    assert_eq!(
        progress.sequence,
        snapshot.receipts[effect_id].committed_seq
    );
    assert!(progress.at_ms >= entered);
    let service = fixture.state.tickets.service().unwrap();
    let head = service.published().unwrap().0;
    let response = sample_http(&fixture, &scope, StatusCode::OK).await;
    assert_eq!(response["ticket_commit"], head);
    assert_eq!(
        response["progress"],
        json!({"sequence":progress.sequence,"at_ms":progress.at_ms})
    );
    let repeated = sample_http(&fixture, &scope, StatusCode::OK).await;
    assert_eq!(repeated["progress"], response["progress"]);
    assert_eq!(service.published().unwrap().0, head);
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 2);
    let mut after_write_observation = fixture
        .state
        .tickets
        .patrol_observation(&scope)
        .await
        .unwrap();
    assert_eq!(
        program
            .tick(&scope, &clock(), &mut journal, &mut after_write_observation)
            .unwrap()
            .status,
        Status::Resolved
    );
    assert_eq!(
        sample(&fixture, &work, &assignment).await.progress,
        Some(progress)
    );

    file_call(
        after_write,
        "work-progress-read",
        "Read",
        json!({"file_path":path}),
    );
    let after_read = fixture.next_request().await;
    assert!(after_read.messages.iter().any(|message| {
        message.role == Role::Tool && message.content.contains("verified output")
    }));
    assert_eq!(
        sample(&fixture, &work, &assignment).await.progress,
        Some(progress)
    );
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 3);

    after_read.complete();
    fixture.wait_submitted(&work).await;
    wait_native_terminal(&fixture.state, &receipt.session_id).await;
    let stopped = sample(&fixture, &work, &assignment).await;
    assert_eq!(stopped.started_at_ms, None);
    assert_eq!(stopped.progress, None);
    let response = sample_http(&fixture, &scope, StatusCode::OK).await;
    assert_eq!(response["state"], "unknown");
    assert!(response["progress"].is_null());
    assert!(response["started_at_ms"].is_null());

    fixture.state.tickets.revoke_ready_work_policy().await;
    let submission_id = fixture.snapshot().tickets[&work]
        .current_submission
        .clone()
        .unwrap();
    let principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (_, authority) = fixture
        .state
        .tickets
        .authority(principal.clone())
        .await
        .unwrap();
    let reject = service
        .prepare_command(
            &authority,
            "retire-progress-attempt",
            vec![Operation::Reject {
                work_id: work,
                submission_id,
                reason: "fixture rework".into(),
            }],
        )
        .unwrap();
    fixture
        .state
        .tickets
        .update(principal, &reject)
        .await
        .unwrap();
    sample_http(&fixture, &scope, StatusCode::FORBIDDEN).await;
}

#[tokio::test]
async fn owner_http_preserves_canonical_outcome_uncertainty_without_measurements() {
    use actix_web::http::StatusCode;
    let mut fixture = Fixture::new(1).await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    let held = fixture.next_request().await;
    let work = &fixture.works[0].id;
    let assignment = fixture.snapshot().tickets[work]
        .active_assignment
        .clone()
        .unwrap();
    let scope = sample(&fixture, work, &assignment).await.scope;
    assert_eq!(
        sample_http(&fixture, &scope, StatusCode::OK).await["state"],
        "observed"
    );

    let (service, authority) = fixture
        .state
        .tickets
        .authority(Principal::Runtime)
        .await
        .unwrap();
    let command = service
        .prepare_command(
            &authority,
            "fixture-uncertain-outcome",
            vec![Operation::OutcomeUnknown {
                assignment_id: assignment,
                reason: "fixture unconfirmed outcome".into(),
            }],
        )
        .unwrap();
    service.execute(&authority, &command).unwrap();
    let head = service.published().unwrap().0;
    let response = sample_http(&fixture, &scope, StatusCode::OK).await;
    assert_eq!(response["state"], "unknown");
    assert_eq!(response["execution_outcome_unknown"], true);
    for field in [
        "started_at_ms",
        "heartbeat_at_ms",
        "progress",
        "expected_wait",
    ] {
        assert!(response[field].is_null(), "{field}: {response}");
    }
    assert_eq!(service.published().unwrap().0, head);
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
    drop(held);
    fixture.state.shutdown().await;
}

#[tokio::test]
async fn live_start_requires_real_task_entry_and_never_restores_from_stop_or_shutdown() {
    let mut fixture = Fixture::new(3).await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(3))
        .await
        .unwrap();
    start_observation::wait(&fixture).await;
    let first = fixture.next_request().await;
    let second = fixture.next_request().await;
    let first_index = (0..3).find(|i| first.is_work(*i)).unwrap();
    let second_index = (0..3).find(|i| second.is_work(*i)).unwrap();
    assert_ne!(first_index, second_index);
    let queued_index = (0..3)
        .find(|i| *i != first_index && *i != second_index)
        .unwrap();
    let first_work = fixture.works[first_index].id.clone();
    let second_work = fixture.works[second_index].id.clone();
    let queued_work = fixture.works[queued_index].id.clone();
    let before = fixture
        .state
        .tickets
        .service()
        .unwrap()
        .published()
        .unwrap();
    let exact_assignment = |work: &str| {
        before
            .1
            .assignments
            .values()
            .find(|a| a.work_id == work)
            .unwrap()
            .id
            .as_str()
    };
    let first_assignment = exact_assignment(&first_work);
    let second_assignment = exact_assignment(&second_work);
    let queued_assignment_id = exact_assignment(&queued_work);
    let first_sample = sample(&fixture, &first_work, first_assignment).await;
    let started = first_sample
        .started_at_ms
        .expect("actual native Agent execution entered before provider admission");
    assert!(started > 0);
    assert!(sample(&fixture, &second_work, second_assignment)
        .await
        .started_at_ms
        .is_some());
    let queued_sample = sample(&fixture, &queued_work, queued_assignment_id).await;
    assert_eq!(queued_sample.started_at_ms, None);
    assert_eq!(queued_sample.heartbeat_at_ms, None);
    let queued_assignment = before
        .1
        .assignments
        .values()
        .find(|a| a.work_id == queued_work)
        .unwrap();
    let queued_child = ticket_runtime::ticket_child_id(&queued_assignment.dispatch_key);
    let (queued_run, queued_birth) = tokio::time::timeout(WAIT, async {
        loop {
            if let (Some(run), Some(child)) = (
                fixture
                    .state
                    .session_activation_router
                    .current_run_id(&queued_child)
                    .await,
                fixture
                    .state
                    .storage
                    .load_runtime_control_plane(&queued_child)
                    .await
                    .unwrap(),
            ) {
                break (run, child.created_at);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the third native logical Run is prepared behind the two real slots");
    // Inspect the real Actor lease only as a control for the queued renewal.
    // The patrol read path never consults this lease to infer a heartbeat.
    let queued_lease = || async {
        fixture
            .state
            .session_store
            .inspect_actor(&queued_child)
            .await
            .unwrap()
            .activation
            .filter(|activation| activation.run_id == queued_run)
            .map(|activation| activation.lease_expires_at)
    };
    let (before_renewal, after_renewal) = tokio::time::timeout(WAIT, async {
        let mut initial = None;
        loop {
            if let Some(current) = queued_lease().await {
                let first = *initial.get_or_insert(current);
                if current > first {
                    break (first, current);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the existing heartbeat loop renews the third Actor while it is still queued");
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_liveness(&queued_child, &queued_run, queued_birth)
        .is_none());
    let queued_sample = sample(&fixture, &queued_work, queued_assignment_id).await;
    assert_eq!(queued_sample.started_at_ms, None);
    assert_eq!(queued_sample.heartbeat_at_ms, None);
    eprintln!("2525 queued renewal {before_renewal} -> {after_renewal}; no heartbeat observation");
    assert_eq!(
        sample(&fixture, &first_work, first_assignment)
            .await
            .started_at_ms,
        Some(started)
    );
    let assignment = before
        .1
        .assignments
        .values()
        .find(|a| a.work_id == first_work)
        .unwrap();
    let receipt = assignment.runtime.as_ref().unwrap();
    let child = fixture
        .state
        .storage
        .load_runtime_control_plane(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    let live = fixture
        .state
        .spawn_scheduler
        .native_task_start(&child.id, &receipt.run_id, child.created_at)
        .unwrap();
    assert_eq!(live.started_at_ms(), Some(started));
    assert!(!live.instance_id.is_nil());
    let heartbeat = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(at) = fixture
                .state
                .spawn_scheduler
                .native_task_liveness(&child.id, &receipt.run_id, child.created_at)
                .and_then(|live| live.heartbeat_at_ms())
            {
                break at;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the admitted Task reports a successful renewal");
    let observed = sample(&fixture, &first_work, first_assignment).await;
    assert_eq!(observed.progress, None);
    assert!(heartbeat >= started);
    assert!(observed.heartbeat_at_ms.is_some_and(|at| at >= heartbeat));
    let response = sample_http(&fixture, &observed.scope, actix_web::http::StatusCode::OK).await;
    assert!(response["heartbeat_at_ms"]
        .as_u64()
        .is_some_and(|at| at >= heartbeat));
    assert!(response["progress"].is_null());
    assert_eq!(
        fixture
            .state
            .tickets
            .service()
            .unwrap()
            .published()
            .unwrap()
            .0,
        before.0
    );
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 2);
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_liveness(&child.id, "wrong-run", child.created_at)
        .is_none());
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_liveness(
            &child.id,
            &receipt.run_id,
            child.created_at + chrono::Duration::milliseconds(1)
        )
        .is_none());
    let mut wrong = first_sample.scope.clone();
    wrong.generation += 1;
    assert!(fixture
        .state
        .tickets
        .patrol_observation(&wrong)
        .await
        .is_err());
    wrong = first_sample.scope.clone();
    wrong.project_id = "foreign-project".into();
    assert!(fixture
        .state
        .tickets
        .patrol_observation(&wrong)
        .await
        .is_err());

    first.complete();
    let queued = fixture.next_request().await;
    assert!(queued.is_work(queued_index));
    assert!(sample(&fixture, &queued_work, queued_assignment_id)
        .await
        .started_at_ms
        .is_some());
    fixture.wait_submitted(&first_work).await;
    wait_native_terminal(&fixture.state, &child.id).await;
    let stopped = sample(&fixture, &first_work, first_assignment).await;
    assert_eq!(stopped.started_at_ms, None);
    assert_eq!(stopped.heartbeat_at_ms, None);
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_liveness(&child.id, &receipt.run_id, child.created_at)
        .is_none());
    queued.complete();
    second.complete();
    for work in &fixture.works {
        fixture.wait_submitted(&work.id).await;
    }
    fixture.assert_quiet(3).await;
    let finished = fixture.snapshot();
    assert_eq!(finished.assignments.len(), 3);
    assert_eq!(finished.submissions.len(), 3);
    for (id, original) in &before.1.assignments {
        assert_eq!(finished.assignments[id].dispatch_key, original.dispatch_key);
        assert_eq!(finished.assignments[id].generation, original.generation);
        assert_ne!(
            finished.tickets[&original.work_id].state,
            WorkState::Accepted
        );
    }
    fixture.state.tickets.revoke_ready_work_policy().await;
    fixture.state.shutdown().await;
    assert!(fixture
        .state
        .spawn_scheduler
        .native_task_liveness(&child.id, &receipt.run_id, child.created_at)
        .is_none());
}
