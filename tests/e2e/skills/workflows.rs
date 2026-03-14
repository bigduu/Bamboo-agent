use super::*;

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
