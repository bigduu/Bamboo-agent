//! Explicit semantic accounting is independent of opt-in Worker quotas.
use super::*;
use bamboo_tickets::{ExecutionBudgetPurpose, WorkTokenAdmission, WorkTokenRequest};

async fn accounting_request(fixture: &Fixture) -> Value {
    let mut request = fixture.setup(CAP).await;
    request["limits"] = Value::Null;
    request
}

#[actix_web::test]
async fn semantic_accounting_owner_explicit_null_replays_and_missing_limits_never_disables() {
    let fixture = Fixture::new(false).await;
    let setup = accounting_request(&fixture).await;
    let before = fixture.service().published().unwrap().0;
    let mut missing = setup.clone();
    missing.as_object_mut().unwrap().remove("limits");
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&missing))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(fixture.service().published().unwrap().0, before);
    let original = http(&fixture.state, Method::PUT, URI, Some(&setup)).await;
    assert_eq!(
        original.0,
        StatusCode::OK,
        "explicit accounting without quotas: {}",
        original.1
    );
    assert!(original.1["limits"].is_null());
    let committed = fixture.service().published().unwrap().0;
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&setup)).await,
        original
    );
    assert_eq!(fixture.service().published().unwrap().0, committed);
    let (_, status) = http(&fixture.state, Method::GET, URI, None).await;
    assert_eq!(status["configured"], true);
    assert_eq!(status["quotas_enforced"], false);
    assert_eq!(status["accounting"]["reservations"]["total"], 0);
    let mut changed = setup.clone();
    changed["limits"] = fixture.setup(CAP).await["limits"].clone();
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&changed))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(fixture.service().published().unwrap().0, committed);
    assert_eq!(fixture.local.calls.load(Ordering::SeqCst), 0);
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn semantic_accounting_never_enables_worker_quota_or_known_ceiling_admission() {
    let mut fixture = Fixture::new(false).await;
    fixture
        .state
        .config
        .write()
        .await
        .extra
        .remove("model_limits");
    let setup = accounting_request(&fixture).await;
    let configured = http(&fixture.state, Method::PUT, URI, Some(&setup)).await;
    assert_eq!(configured.0, StatusCode::OK, "{}", configured.1);
    fixture.policy().await;
    task(fixture.next().await);
    fixture
        .next()
        .await
        .reply
        .send(LLMChunk::Token("Unrestricted native Worker handoff".into()))
        .unwrap();
    fixture.terminal(true).await;
    assert_eq!(fixture.local.calls.load(Ordering::SeqCst), 2);
    let (_, status) = http(&fixture.state, Method::GET, URI, None).await;
    assert_eq!(status["quotas_enforced"], false);
    assert_eq!(status["accounting"]["total_charged"], 0);
    assert_eq!(status["accounting"]["reservations"]["total"], 0);
    let snapshot = fixture.service().published().unwrap().1;
    assert_eq!(snapshot.tickets[&fixture.work].state, WorkState::Submitted);
    assert!(
        snapshot
            .assignments
            .values()
            .next()
            .unwrap()
            .process_stopped
    );
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn semantic_accounting_reserves_settles_unknown_and_reopens_without_quotas() {
    let fixture = Fixture::new(false).await;
    let setup = accounting_request(&fixture).await;
    let original = http(&fixture.state, Method::PUT, URI, Some(&setup)).await;
    assert_eq!(original.0, StatusCode::OK, "{}", original.1);
    let service = fixture.service();
    let snapshot = service.published().unwrap().1;
    let runtime = Authority::from_verified_host(snapshot.binding, Principal::Runtime);
    let request = WorkTokenRequest {
        operation_id: "semantic-accounting-known".into(),
        project_id: fixture.project.clone(),
        work_id: fixture.work.clone(),
        purpose: ExecutionBudgetPurpose::SupervisorDecision,
        request_hash: bamboo_tickets::content_hash(b"bounded semantic input"),
        observed_seq: snapshot.seq,
        input_cap: 60,
        output_cap: 40,
        attempt: None,
    };
    let WorkTokenAdmission::Reserved(receipt) =
        service.reserve_work_tokens(&runtime, &request, 1).unwrap()
    else {
        panic!("original reserve")
    };
    assert_eq!(
        service
            .published()
            .unwrap()
            .1
            .execution_budget
            .as_ref()
            .unwrap()
            .total_charged,
        100
    );
    let unknown = service
        .settle_work_tokens(
            &runtime,
            &request.operation_id,
            &request.request_hash,
            &receipt.receipt,
            ExecutionTokenUsage::Unknown,
        )
        .unwrap();
    assert_eq!(unknown.usage, Some(ExecutionTokenUsage::Unknown));
    assert_eq!(
        service
            .published()
            .unwrap()
            .1
            .execution_budget
            .as_ref()
            .unwrap()
            .total_charged,
        100
    );
    let known = service
        .settle_work_tokens(
            &runtime,
            &request.operation_id,
            &request.request_hash,
            &receipt.receipt,
            ExecutionTokenUsage::Known {
                input_tokens: 20,
                output_tokens: 10,
            },
        )
        .unwrap();
    assert_eq!(known.receipt, receipt.receipt);
    assert_eq!(
        service
            .published()
            .unwrap()
            .1
            .execution_budget
            .as_ref()
            .unwrap()
            .total_charged,
        30
    );
    let committed = service.published().unwrap().0;
    assert_eq!(
        service
            .settle_work_tokens(
                &runtime,
                &request.operation_id,
                &request.request_hash,
                &receipt.receipt,
                ExecutionTokenUsage::Known {
                    input_tokens: 20,
                    output_tokens: 10
                }
            )
            .unwrap(),
        known
    );
    assert_eq!(service.published().unwrap().0, committed);
    assert_eq!(fixture.local.calls.load(Ordering::SeqCst), 0);
    let Fixture {
        state,
        provider,
        home,
        ..
    } = fixture;
    state.shutdown().await;
    drop(state);
    drop(service);
    let state =
        web::Data::new(Box::pin(host(&home.path().canonicalize().unwrap(), provider)).await);
    assert_eq!(
        http(&state, Method::PUT, URI, Some(&setup)).await.0,
        StatusCode::CONFLICT
    );
    let (_, status) = http(&state, Method::GET, URI, None).await;
    assert_eq!(status["quotas_enforced"], false);
    assert_eq!(status["accounting"]["total_charged"], 30);
    assert_eq!(status["accounting"]["reservations"]["known_usage"], 1);
    let mut fresh = setup;
    fresh["authority_epoch"] = status["observed"]["authority_epoch"].clone();
    assert_eq!(http(&state, Method::PUT, URI, Some(&fresh)).await, original);
    state.shutdown().await;
}
