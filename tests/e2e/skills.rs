//! E2E tests for /v1/skills endpoints

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::skill;

#[actix_web::test]
async fn test_list_skills_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/skills", web::get().to(skill::list_skills)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/skills").to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_list_skills_returns_json() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/skills", web::get().to(skill::list_skills)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/skills").to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;

    // Should be valid JSON
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have skills array
    assert!(result.is_object());
    assert!(result.get("skills").is_some());
    assert!(result.get("total").is_some());
}

#[actix_web::test]
async fn test_get_skill_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/skills/{id}", web::get().to(skill::get_skill)),
    )
    .await;

    // Test with a non-existent skill ID
    let req = test::TestRequest::get()
        .uri("/v1/skills/non-existent-skill-id")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return 404 for non-existent skill
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn test_get_available_tools_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/skills/available-tools",
        web::get().to(skill::get_available_tools),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/skills/available-tools")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have tools array
    assert!(result.is_object());
    assert!(result.get("tools").is_some());
    assert!(result["tools"].is_array());
}

#[actix_web::test]
async fn test_get_filtered_tools_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/skills/filtered-tools",
        web::get().to(skill::get_filtered_tools),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/skills/filtered-tools")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have tools array
    assert!(result.is_object());
    assert!(result.get("tools").is_some());
    assert!(result["tools"].is_array());
}

#[actix_web::test]
async fn test_get_filtered_tools_with_chat_id() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/skills/filtered-tools",
        web::get().to(skill::get_filtered_tools),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/skills/filtered-tools?chat_id=test-chat-123")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_get_filtered_tools_with_session_id() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/skills/filtered-tools",
        web::get().to(skill::get_filtered_tools),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/skills/filtered-tools?session_id=test-session-123")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_get_available_workflows_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/skills/available-workflows",
        web::get().to(skill::get_available_workflows),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/skills/available-workflows")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have workflows array
    assert!(result.is_object());
    assert!(result.get("workflows").is_some());
    assert!(result["workflows"].is_array());
}

#[actix_web::test]
async fn test_skills_endpoints_with_query_params() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/v1/skills", web::get().to(skill::list_skills))
            .route(
                "/v1/skills/filtered-tools",
                web::get().to(skill::get_filtered_tools),
            ),
    )
    .await;

    // Test list_skills with query params
    let req = test::TestRequest::get()
        .uri("/v1/skills?category=test&search=query&refresh=true")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // Test filtered-tools with query params
    for query in ["chat_id=test-chat", "session_id=test-session"] {
        let req = test::TestRequest::get()
            .uri(&format!("/v1/skills/filtered-tools?{query}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
    }
}
