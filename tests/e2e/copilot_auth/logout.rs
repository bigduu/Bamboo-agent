use super::*;

/// Test POST /v1/bamboo/copilot/logout endpoint.
#[actix_web::test]
async fn test_copilot_logout_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/logout",
        web::post().to(copilot_auth::logout_copilot),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/logout")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Verify response structure
    assert!(
        json.get("success").is_some(),
        "Response should contain success field"
    );
    assert_eq!(json["success"].as_bool(), Some(true));
    assert!(
        json.get("message").is_some(),
        "Response should contain message field"
    );
    assert!(
        json["message"].as_str().unwrap().contains("Logged out"),
        "Message should indicate successful logout"
    );
}

/// Test POST /v1/bamboo/copilot/logout idempotency.
#[actix_web::test]
async fn test_copilot_logout_idempotent() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/logout",
        web::post().to(copilot_auth::logout_copilot),
    ))
    .await;

    // First logout
    let req1 = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/logout")
        .to_request();
    let resp1 = test::call_service(&app, req1).await;
    assert!(resp1.status().is_success());

    // Second logout (should still succeed)
    let req2 = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/logout")
        .to_request();
    let resp2 = test::call_service(&app, req2).await;
    assert!(resp2.status().is_success());

    let body = test::read_body(resp2).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");
    assert_eq!(json["success"].as_bool(), Some(true));
}
