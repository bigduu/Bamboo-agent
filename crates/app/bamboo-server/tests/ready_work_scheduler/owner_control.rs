//! Owner routes use the same FileStore, durable Inbox and native Host fixture.
use super::*;
use actix_web::{
    http::{Method, StatusCode},
    test, web, App,
};
use bamboo_tickets::{Authority, WorkTokenLimits};
use serde_json::Value;

const URI: &str = "/api/v1/tickets/ready-policy";
const CAP: u64 = 65_536 + 1_024;

async fn http(
    state: &web::Data<AppState>,
    method: Method,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    http_uri(state, method, URI, body).await
}

async fn http_uri(
    state: &web::Data<AppState>,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::routes::configure_routes),
    )
    .await;
    let mut request = test::TestRequest::default()
        .method(method)
        .uri(uri)
        .peer_addr("127.0.0.1:43210".parse().unwrap());
    if let Some(body) = body {
        request = request.set_json(body);
    }
    let response = test::call_service(&app, request.to_request()).await;
    (response.status(), test::read_body_json(response).await)
}

fn owner() -> Principal {
    Principal::User {
        user_id: "host-owner".into(),
    }
}

async fn configure_budget(fixture: &Fixture, limit: u64) {
    fixture.state.config.write().await.extra.insert("model_limits".into(), json!([{
        "model_pattern":MODEL,"max_context_tokens":65536,"max_output_tokens":1024,"safety_margin":1000,
    }]));
    let budget_uri = "/api/v1/tickets/token-budget";
    let (code, status) = http_uri(&fixture.state, Method::GET, budget_uri, None).await;
    assert_eq!(code, StatusCode::OK, "{status}");
    let request = json!({
        "binding":status["binding"],"authority_epoch":status["observed"]["authority_epoch"],
        "expected_revision":status["budget_revision"],"limits":WorkTokenLimits {
            work_tokens: fixture.works.iter().map(|work| (work.id.clone(), limit)).collect(),
            utc_day_tokens: limit * fixture.works.len() as u64,
            total_tokens: limit * fixture.works.len() as u64,
        },
    });
    let installed = http_uri(&fixture.state, Method::PUT, budget_uri, Some(&request)).await;
    assert_eq!(installed.0, StatusCode::OK, "{}", installed.1);
    assert_eq!(
        http_uri(&fixture.state, Method::PUT, budget_uri, Some(&request)).await,
        installed,
        "budget retry reads one original canonical decision"
    );
}

fn install(
    fixture: &Fixture,
    status: &Value,
    operation: &str,
    capacity: usize,
    lifetime: u32,
) -> Value {
    let (commit, snapshot) = fixture
        .state
        .tickets
        .service()
        .unwrap()
        .published()
        .unwrap();
    json!({
        "process_id":status["process_id"],"expected_revision":status["revision"],
        "operation_id":operation,"action":{"kind":"install","policy":{
            "binding":snapshot.binding,
            "observed":{"commit":commit,"seq":snapshot.seq,"authority_epoch":snapshot.authority_epoch},
            "project_id":fixture.project,"capacity":capacity,"max_rounds":MAX_ROUNDS,
            "lifetime_secs":lifetime,
            "grants":fixture.works.iter().map(|work| json!({
                "work_id":work.id,"contract_revision":work.contract_revision,
                "context_hash":work.context_hash,"workspace":work.workspace,
            })).collect::<Vec<_>>(),
        }},
    })
}

async fn revoke(fixture: &Fixture, operation: &str) -> Value {
    let (code, status) = http(&fixture.state, Method::GET, None).await;
    assert_eq!(code, StatusCode::OK);
    let request = json!({"process_id":status["process_id"],"expected_revision":status["revision"],
        "operation_id":operation,"action":{"kind":"revoke"}});
    let (code, receipt) = http(&fixture.state, Method::PUT, Some(&request)).await;
    assert_eq!(code, StatusCode::OK, "{receipt}");
    let repeated = http(&fixture.state, Method::PUT, Some(&request)).await;
    assert_eq!(repeated, (StatusCode::OK, receipt.clone()));
    receipt
}

#[actix_web::test]
async fn owner_http_explicit_scope_to_native_submission_and_exact_acceptance() {
    let mut fixture = Fixture::with_explicit_scope(1, true).await;
    let (_, status) = http(&fixture.state, Method::GET, None).await;
    assert_eq!(status["active"], false);
    let missing_budget = install(&fixture, &status, "owner-install", 1, 60);
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&missing_budget))
            .await
            .0,
        StatusCode::LOCKED
    );
    fixture.assert_quiet(0).await;
    configure_budget(&fixture, 2 * CAP).await;
    let request = install(&fixture, &status, "owner-install", 1, 60);
    let (first_install, concurrent_retry) = futures::join!(
        http(&fixture.state, Method::PUT, Some(&request)),
        http(&fixture.state, Method::PUT, Some(&request)),
    );
    assert_eq!(first_install, concurrent_retry);
    let (code, receipt) = first_install;
    assert_eq!(code, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["action"], "installed");
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&request)).await,
        (StatusCode::OK, receipt.clone())
    );
    let mut changed = request.clone();
    changed["action"]["policy"]["capacity"] = json!(2);
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&changed)).await.0,
        StatusCode::CONFLICT
    );
    let first = fixture.next_request().await;
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.intents.len(), 1);
    assert_eq!(
        snapshot
            .execution_budget
            .as_ref()
            .unwrap()
            .reservations
            .len(),
        1
    );
    assert_eq!(
        snapshot.execution_budget.as_ref().unwrap().total_charged,
        CAP
    );
    let assignment = snapshot.assignments.values().next().unwrap();
    let child_id = assignment.runtime.as_ref().unwrap().session_id.clone();
    let dispatch_key = assignment.dispatch_key.clone();
    let spec = snapshot.intents[&dispatch_key].immutable_spec.clone();
    assert!(matches!(
        ticket_runtime::query_dispatch(
            fixture.state.storage.as_ref(),
            &fixture.state.tickets.service().unwrap(),
            &dispatch_key,
            &spec
        )
        .await
        .unwrap(),
        ticket_runtime::DispatchObservation::Admitted { .. }
    ));
    first.task();
    let final_request = fixture.next_request().await;
    assert!(final_request
        .messages
        .iter()
        .any(|message| message.role == Role::Tool
            && message.tool_call_id.as_deref() == Some(TASK_CALL)
            && message.tool_success == Some(true)));
    final_request.complete();
    let work_id = fixture.works[0].id.clone();
    fixture.wait_submitted(&work_id).await;
    let child = wait_native_terminal(&fixture.state, &child_id).await;
    assert_eq!(child.kind, SessionKind::Child);
    assert!(child
        .messages
        .iter()
        .any(|message| message.role == Role::Tool
            && message.tool_call_id.as_deref() == Some(TASK_CALL)));
    let service = fixture.state.tickets.service().unwrap();
    let (commit, snapshot) = service.published().unwrap();
    assert_eq!(snapshot.tickets[&work_id].state, WorkState::Submitted);
    assert!(snapshot.tickets[&work_id].accepted_submission.is_none());
    assert_eq!(snapshot.submissions.len(), 1);
    assert_eq!(
        snapshot.execution_budget.as_ref().unwrap().total_charged,
        2 * CAP
    );
    assert_eq!(
        snapshot
            .execution_budget
            .as_ref()
            .unwrap()
            .reservations
            .len(),
        2
    );
    let authority = Authority::from_verified_host(snapshot.binding.clone(), owner());
    let accept = service
        .prepare_command(
            &authority,
            "owner-exact-accept",
            vec![Operation::Accept {
                work_id: work_id.clone(),
                submission_id: snapshot.tickets[&work_id]
                    .current_submission
                    .clone()
                    .unwrap(),
                evidence: vec![
                    "Reviewed the canonical Child Task receipt and exact submitted result".into(),
                ],
            }],
        )
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(fixture.state.clone())
            .configure(bamboo_server::routes::configure_routes),
    )
    .await;
    let accept_body = json!({"operation_id":accept.operation_id,"binding":accept.binding,
        "expected_seq":accept.expected_seq,"expected_epoch":accept.expected_epoch,"operations":accept.operations});
    let mut original = None;
    for _ in 0..2 {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/tickets/update")
                .peer_addr("127.0.0.1:43210".parse().unwrap())
                .set_json(&accept_body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let accepted: Value = test::read_body_json(response).await;
        if let Some(original) = &original {
            assert_eq!(&accepted, original);
        }
        original = Some(accepted);
    }
    assert_ne!(service.published().unwrap().0, commit);
    let accepted = fixture.snapshot();
    assert_eq!(accepted.tickets[&work_id].state, WorkState::Accepted);
    assert_eq!(accepted.assignments.len(), 1);
    assert_eq!(accepted.submissions.len(), 1);
    let accepted_commit = service.published().unwrap().0;
    let second_accept = service
        .prepare_command(
            &authority,
            "different-double-accept",
            accept.operations.clone(),
        )
        .unwrap();
    assert!(matches!(
        fixture.state.tickets.update(owner(), &second_accept).await,
        Err(bamboo_tickets::Error::InvalidTransition(_))
    ));
    assert_eq!(service.published().unwrap().0, accepted_commit);
    revoke(&fixture, "owner-revoke").await;
    // Cached installation is just a receipt. It cannot undo revocation.
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&request)).await,
        (StatusCode::OK, receipt)
    );
    assert_eq!(
        http(&fixture.state, Method::GET, None).await.1["active"],
        false
    );
    fixture.assert_quiet(2).await;
    drop(app);
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn owner_capacity_and_revoke_leave_second_work_ready_without_double_dispatch() {
    let mut fixture = Fixture::with_explicit_scope(2, true).await;
    configure_budget(&fixture, 2 * CAP).await;
    let (_, status) = http(&fixture.state, Method::GET, None).await;
    let request = install(&fixture, &status, "capacity-install", 1, 60);
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&request)).await.0,
        StatusCode::OK
    );
    let first = fixture.next_request().await;
    for _ in 0..3 {
        assert!(matches!(
            fixture.state.tickets.reconcile_ready_work().await.unwrap(),
            ReadyReconcile::CapacityBlocked {
                occupied: 1,
                capacity: 1
            }
        ));
    }
    assert_eq!(fixture.snapshot().assignments.len(), 1);
    fixture.assert_quiet(1).await;
    revoke(&fixture, "capacity-revoke").await;
    let work_id = fixture
        .snapshot()
        .assignments
        .values()
        .next()
        .unwrap()
        .work_id
        .clone();
    first.complete();
    fixture.wait_submitted(&work_id).await;
    fixture.state.tickets.wake_ready_work();
    assert_eq!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert_eq!(fixture.snapshot().assignments.len(), 1);
    assert_eq!(
        fixture
            .snapshot()
            .tickets
            .values()
            .filter(|work| work.state == WorkState::Ready)
            .count(),
        1
    );
    fixture.assert_quiet(1).await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn owner_finite_canonical_budget_blocks_second_physical_provider_call() {
    let mut fixture = Fixture::with_explicit_scope(1, true).await;
    configure_budget(&fixture, CAP).await;
    let (_, status) = http(&fixture.state, Method::GET, None).await;
    let request = install(&fixture, &status, "budget-install", 1, 60);
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&request)).await.0,
        StatusCode::OK
    );
    fixture.next_request().await.task();
    let work_id = fixture.works[0].id.clone();
    tokio::time::timeout(WAIT, async {
        loop {
            if fixture.snapshot().tickets[&work_id].state == WorkState::Blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("budget failure reaches the canonical Work through the real stopped worker");
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(
        snapshot.assignments.values().next().unwrap().state,
        AssignmentState::Failed
    );
    assert!(snapshot.submissions.is_empty());
    assert_eq!(
        snapshot.execution_budget.as_ref().unwrap().total_charged,
        CAP
    );
    assert_eq!(
        snapshot
            .execution_budget
            .as_ref()
            .unwrap()
            .reservations
            .len(),
        1
    );
    assert!(
        snapshot
            .assignments
            .values()
            .next()
            .unwrap()
            .process_stopped
    );
    fixture.assert_quiet(1).await;
    revoke(&fixture, "budget-revoke").await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn owner_stale_snapshot_invalid_context_remote_and_restart_never_install() {
    let mut fixture = Fixture::with_explicit_scope(1, true).await;
    configure_budget(&fixture, 2 * CAP).await;
    let (_, status) = http(&fixture.state, Method::GET, None).await;
    let request = install(&fixture, &status, "invalid-install", 1, 60);
    let app = test::init_service(
        App::new()
            .app_data(fixture.state.clone())
            .configure(bamboo_server::routes::configure_routes),
    )
    .await;
    for method in [Method::GET, Method::PUT] {
        let response = test::call_service(
            &app,
            test::TestRequest::default()
                .method(method)
                .uri(URI)
                .peer_addr("192.0.2.1:43210".parse().unwrap())
                .set_json(&request)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    for changed in [
        {
            let mut value = request.clone();
            value["action"]["policy"]["observed"]["authority_epoch"] = json!(0);
            value
        },
        {
            let mut value = request.clone();
            value["action"]["policy"]["grants"][0]["context_hash"] = json!("f".repeat(64));
            value
        },
        {
            let mut value = request.clone();
            value["action"]["policy"]["capacity"] = json!(4);
            value
        },
    ] {
        assert!(matches!(
            http(&fixture.state, Method::PUT, Some(&changed)).await.0,
            StatusCode::CONFLICT | StatusCode::UNPROCESSABLE_ENTITY
        ));
    }
    assert_eq!(
        http(&fixture.state, Method::GET, None).await.1["revision"],
        0
    );
    fixture.assert_quiet(0).await;
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&request)).await.0,
        StatusCode::OK
    );
    fixture.next_request().await.complete();
    let work_id = fixture.works[0].id.clone();
    fixture.wait_submitted(&work_id).await;
    let assignment = fixture
        .snapshot()
        .assignments
        .values()
        .next()
        .unwrap()
        .clone();
    wait_native_terminal(
        &fixture.state,
        &assignment.runtime.as_ref().unwrap().session_id,
    )
    .await;
    revoke(&fixture, "before-restart").await;
    let original_dispatch = fixture.snapshot().intents[&assignment.dispatch_key]
        .immutable_spec
        .clone();
    let original_runtime = assignment.runtime.clone();
    let root = fixture._root.path().canonicalize().unwrap();
    let storage = fixture.state.storage.clone();
    let config = fixture.state.config.clone();
    fixture.state.shutdown().await;
    // A second real application instance rotates the process identity even if
    // the old frontend retains the exact request and the store is unchanged.
    // Drop the original writer and HTTP router before reopening canonical HEAD.
    drop(app);
    let old_epoch = fixture.snapshot().authority_epoch;
    let provider = fixture.provider.clone();
    drop(fixture.state);
    drop(storage);
    let reopened = web::Data::new(
        Box::pin(AppState::new_with_provider_and_memory_store(
            root.clone(),
            config.read().await.clone(),
            provider,
            bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
        ))
        .await
        .unwrap(),
    );
    assert!(
        reopened
            .tickets
            .service()
            .unwrap()
            .published()
            .unwrap()
            .1
            .authority_epoch
            > old_epoch
    );
    assert_ne!(
        http(&reopened, Method::GET, None).await.1["process_id"],
        status["process_id"]
    );
    assert_eq!(
        http(&reopened, Method::PUT, Some(&request)).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(http(&reopened, Method::GET, None).await.1["active"], false);
    let service = reopened.tickets.service().unwrap();
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.submissions.len(), 1);
    assert_eq!(
        snapshot.assignments[&assignment.id].runtime,
        original_runtime
    );
    assert_eq!(
        snapshot.execution_budget.as_ref().unwrap().total_charged,
        CAP
    );
    assert_eq!(snapshot.tickets[&work_id].state, WorkState::Submitted);
    assert!(
        matches!(ticket_runtime::query_dispatch(reopened.storage.as_ref(), &service,
        &assignment.dispatch_key, &original_dispatch).await.unwrap(),
        ticket_runtime::DispatchObservation::Terminal { receipt, .. }
            if Some(&receipt) == original_runtime.as_ref())
    );
    assert_eq!(
        reopened.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    );
    reopened.shutdown().await;
}

#[actix_web::test]
async fn cached_owner_install_after_expiry_cannot_extend_the_grant() {
    let mut fixture = Fixture::with_explicit_scope(1, true).await;
    configure_budget(&fixture, 2 * CAP).await;
    let (_, status) = http(&fixture.state, Method::GET, None).await;
    let request = install(&fixture, &status, "expiring-install", 1, 60);
    let (code, receipt) = http(&fixture.state, Method::PUT, Some(&request)).await;
    assert_eq!(code, StatusCode::OK);
    let first = fixture.next_request().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&request)).await,
        (StatusCode::OK, receipt)
    );
    assert_eq!(
        http(&fixture.state, Method::GET, None).await.1["active"],
        false
    );
    tokio::time::resume();
    first.complete();
    let work_id = fixture.works[0].id.clone();
    fixture.wait_submitted(&work_id).await;
    fixture.assert_quiet(1).await;
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn owner_status_and_revoke_do_not_wait_for_a_full_real_native_queue() {
    use bamboo_domain::AdmissionCommit;
    let mut fixture = Fixture::with_explicit_scope(1, true).await;
    configure_budget(&fixture, 2 * CAP).await;
    let scheduler = fixture.state.spawn_scheduler.clone();
    let mut held = Vec::new();
    for _ in 0..128 {
        match tokio::time::timeout(WAIT, scheduler.reserve_queue_capacity(None))
            .await
            .unwrap()
            .unwrap()
        {
            AdmissionCommit::Committed(permit) => held.push(permit),
            _ => panic!("must hold actual native queue capacity"),
        }
    }
    let mut probe = Box::pin(scheduler.reserve_queue_capacity(None));
    assert!(futures::poll!(probe.as_mut()).is_pending());
    drop(probe);
    let (_, status) = http(&fixture.state, Method::GET, None).await;
    let request = install(&fixture, &status, "queue-install", 1, 60);
    let (code, receipt) = http(&fixture.state, Method::PUT, Some(&request)).await;
    assert_eq!(code, StatusCode::OK, "{receipt}");
    let (before, prepared) = super::admission::wait_prepared_child(&fixture).await;
    let (code, busy) = tokio::time::timeout(WAIT, http(&fixture.state, Method::GET, None))
        .await
        .expect("busy scheduler must not block owner observation/revocation");
    assert_eq!(code, StatusCode::OK);
    assert!(
        busy["active"].is_null(),
        "busy admission is unknown, never a false running fact"
    );
    let revoke_request = json!({"process_id":busy["process_id"],
        "expected_revision":busy["revision"],"operation_id":"queue-revoke",
        "action":{"kind":"revoke"}});
    let app = test::init_service(
        App::new()
            .app_data(fixture.state.clone())
            .configure(bamboo_server::routes::configure_routes),
    )
    .await;
    let mut waiter = Box::pin(test::call_service(
        &app,
        test::TestRequest::put()
            .uri(URI)
            .peer_addr("127.0.0.1:43210".parse().unwrap())
            .set_json(&revoke_request)
            .to_request(),
    ));
    // The managed pass still owns the real admission lock. First poll records
    // the revoke and cancels that pass, then waits for its drain. Drop this
    // actual HTTP future before the cancelled pass can run its cleanup.
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    drop(waiter);
    let (_, pending) = http(&fixture.state, Method::GET, None).await;
    assert_eq!(pending["pending_revoke_operation"], "queue-revoke");
    assert_eq!(pending["revision"], 2);
    assert!(pending["policy"].is_null());
    assert_eq!(pending["receipt_capacity_remaining"], 126);
    let replacement = install(&fixture, &pending, "blocked-replacement", 1, 60);
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&replacement))
            .await
            .0,
        StatusCode::LOCKED,
        "an unfinished revoke cannot admit a replacement policy"
    );
    let retried = tokio::time::timeout(
        WAIT,
        http(&fixture.state, Method::PUT, Some(&revoke_request)),
    )
    .await
    .expect("the original revoke must finish its original drain");
    assert_eq!(retried.0, StatusCode::OK, "{}", retried.1);
    assert_eq!(retried.1["revision"], 2);
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&revoke_request)).await,
        retried,
        "retry returns one immutable receipt without another revision"
    );
    let (_, drained) = http(&fixture.state, Method::GET, None).await;
    assert!(drained["pending_revoke_operation"].is_null());
    assert_eq!(drained["revision"], 2);
    assert_eq!(drained["receipt_capacity_remaining"], 126);
    assert_eq!(
        http(&fixture.state, Method::PUT, Some(&request)).await,
        (StatusCode::OK, receipt)
    );
    assert_eq!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert_eq!(
        serde_json::to_value(fixture.snapshot()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    let child = fixture
        .state
        .storage
        .load_session(&prepared.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.last_run_status().as_deref(), Some("pending"));
    assert_eq!(
        child.child_launch_generation(),
        prepared.child_launch_generation()
    );
    assert!(fixture
        .snapshot()
        .execution_budget
        .unwrap()
        .reservations
        .is_empty());
    drop(held);
    fixture.assert_quiet(0).await;
    fixture.state.shutdown().await;
}
