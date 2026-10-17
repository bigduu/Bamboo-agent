use actix_web::{http::StatusCode, web};
use bamboo_server::AppState;

#[actix_web::test]
async fn project_board_owner_route_uses_explicit_membership_and_rejects_unknown_project() {
    use actix_web::{test, App};
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap();
    let mut state = AppState::new(root.path().to_path_buf()).await.unwrap();
    *state.config.write().await = serde_json::from_value(serde_json::json!({
        "provider":"openai","features":{"ticket_mutation":true},
        "providers":{"openai":{"api_key":"fixture","model":"fixture-model"}}
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
    let project = state.project_store.create("Board", None).unwrap();
    let service = state.tickets.service().unwrap();
    let state = web::Data::new(state);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let request = serde_json::json!({"project_id":project.id,"limit":10});
    let denied = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/project-board")
            .peer_addr("203.0.113.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let snapshot = service.published().unwrap().1;
    let command = serde_json::json!({"operation_id":"link-project", "binding":snapshot.binding,
        "expected_seq":snapshot.seq,"expected_epoch":snapshot.authority_epoch,
        "operations":[{"op":"create","temp_id":"w","kind":"work","parent":null,
        "depends_on":[],"contract":{"title":"Delivery","objective":"Build","constraints":[],
        "acceptance":["Test"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"associate_project","ticket_id":"w","project_id":project.id}]});
    let updated = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&command)
            .to_request(),
    )
    .await;
    assert_eq!(updated.status(), StatusCode::OK);
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/project-board")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["coverage"], "authoritative_scope");
    assert_eq!(body["data"]["counts"]["todo"], 1);
    assert_eq!(body["data"]["rows"][0]["stage"], "todo");
    assert!(body["data"]["rows"][0]["context"].is_null());
    let snapshot = service.published().unwrap().1;
    let mut invalid = command;
    invalid["operation_id"] = "missing-project".into();
    invalid["expected_seq"] = snapshot.seq.into();
    invalid["operations"][1]["project_id"] = "missing-project".into();
    let before = service.published().unwrap().0;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&invalid)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(service.published().unwrap().0, before);
    // Host validation uses registered Project facts, not client-provided paths
    // or revisions. Context metadata cannot create a grant or Assignment.
    let workspace = tempfile::tempdir().unwrap();
    let manifest = state
        .project_store
        .bind_workspace(
            &project.id,
            project.revision,
            bamboo_domain::WorkspaceBinding {
                path: workspace.path().to_string_lossy().into(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    let snapshot = service.published().unwrap().1;
    let work = snapshot.tickets.values().next().unwrap();
    let context = serde_json::json!({"project_id":project.id,"project_revision":manifest.revision,
        "resource_revision":manifest.resource_revision,"workspace_binding":manifest.workspace_bindings[0],
        "contract_revision":work.contract_revision,"contract":work.contract,"sources":{"spec":"v1"}});
    for (operation_id, supplied, status) in [
        (
            "stale-resource",
            {
                let mut c = context.clone();
                c["resource_revision"] = 0.into();
                c
            },
            StatusCode::CONFLICT,
        ),
        (
            "unregistered-binding",
            {
                let mut c = context.clone();
                c["workspace_binding"]["path"] = "/unregistered".into();
                c
            },
            StatusCode::CONFLICT,
        ),
        ("adopt-context", context, StatusCode::OK),
    ] {
        let request = serde_json::json!({"operation_id":operation_id,"binding":snapshot.binding,
            "expected_seq":snapshot.seq,"expected_epoch":snapshot.authority_epoch,
            "operations":[{"op":"set_project_context","work_id":work.id,"context":supplied}]});
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/tickets/update")
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .set_json(&request)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), status, "{operation_id}");
        if status != StatusCode::OK {
            assert_eq!(service.published().unwrap().0, before);
        }
    }
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.project.work_contexts.len(), 1);
    assert!(snapshot.assignments.is_empty());
}
