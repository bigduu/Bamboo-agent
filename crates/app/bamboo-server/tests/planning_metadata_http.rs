use actix_web::{http::StatusCode, test, web, App};
use bamboo_server::AppState;
use serde_json::{json, Value};
use std::sync::Arc;

#[actix_web::test]
async fn owner_planning_route_preserves_cas_receipts_and_board_projection() {
    let root = tempfile::tempdir().unwrap();
    let mut state = AppState::new_with_memory_store(
        root.path().to_path_buf(),
        bamboo_memory::memory_store::MemoryStore::new(root.path().join("jiandu")),
    )
    .await
    .unwrap();
    *state.config.write().await = serde_json::from_value(json!({
        "provider":"openai", "features":{"ticket_mutation":true},
        "providers":{"openai":{"api_key":"fixture", "model":"fixture-model"}}
    }))
    .unwrap();
    state.tickets = Arc::new(
        bamboo_server::app_state::ticket_application::TicketApplication::open(
            root.path(),
            state.storage.clone(),
            state.config.clone(),
        )
        .await,
    );
    let project = state.project_store.create("Owner planning", None).unwrap();
    let service = state.tickets.service().unwrap();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let snapshot = service.published().unwrap().1;
    let seed = json!({"operation_id":"seed", "binding":snapshot.binding, "expected_seq":snapshot.seq, "expected_epoch":snapshot.authority_epoch,
        "operations":[{"op":"create", "temp_id":"w", "kind":"work", "parent":null, "depends_on":[],
        "contract":{"title":"Deadline", "objective":"Keep User intent", "constraints":[], "acceptance":["Review"], "user_acceptance_required":true, "allowed_tools":["Task"]}},
        {"op":"associate_project", "ticket_id":"w", "project_id":project.id}]});
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&seed)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let snapshot = service.published().unwrap().1;
    let work = snapshot.tickets.values().next().unwrap();
    let request = json!({"operation_id":"owner-plan", "binding":snapshot.binding, "expected_seq":snapshot.seq, "expected_epoch":snapshot.authority_epoch,
        "operations":[{"op":"set_planning_metadata", "ticket_id":work.id, "expected_record_revision":work.record_revision, "priority":"high", "expected_completion_at":"2026-12-01T18:00:00+08:00"}]});
    let before = service.published().unwrap().0;
    let denied = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("203.0.113.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(service.published().unwrap().0, before);
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let receipt: Value = test::read_body_json(response).await;
    assert_eq!(receipt["principal"], "user:host-owner");
    let committed = service.published().unwrap().0;
    let replay = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(test::read_body_json::<Value, _>(replay).await, receipt);
    assert_eq!(service.published().unwrap().0, committed);
    for (operation_id, change, status) in [
        ("stale-scope", "scope", StatusCode::CONFLICT),
        ("stale-row", "revision", StatusCode::CONFLICT),
        ("invalid-time", "time", StatusCode::UNPROCESSABLE_ENTITY),
        ("missing-clear", "missing", StatusCode::BAD_REQUEST),
    ] {
        let snapshot = service.published().unwrap().1;
        let mut invalid = request.clone();
        invalid["operation_id"] = operation_id.into();
        invalid["expected_seq"] = snapshot.seq.into();
        invalid["operations"][0]["expected_record_revision"] =
            snapshot.tickets[&work.id].record_revision.into();
        match change {
            "scope" => invalid["expected_seq"] = 0.into(),
            "revision" => invalid["operations"][0]["expected_record_revision"] = 0.into(),
            "time" => {
                invalid["operations"][0]["expected_completion_at"] = "2026-12-01T18:00:00".into()
            }
            "missing" => {
                invalid["operations"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("expected_completion_at");
            }
            _ => unreachable!(),
        }
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/tickets/update")
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .set_json(&invalid)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), status, "{operation_id}");
        assert_eq!(service.published().unwrap().0, committed);
    }
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/project-board")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(json!({"project_id":project.id}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let board: Value = test::read_body_json(response).await;
    let ticket = &board["data"]["rows"][0]["ticket"];
    assert_eq!(ticket["priority"], "high");
    assert_eq!(
        ticket["expected_completion_at"],
        "2026-12-01T18:00:00+08:00"
    );
    assert_eq!(ticket["state"], "draft");
    assert!(service.published().unwrap().1.assignments.is_empty());
}
