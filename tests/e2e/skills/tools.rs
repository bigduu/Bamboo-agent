use super::*;

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
