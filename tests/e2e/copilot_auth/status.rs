use super::*;

/// Test POST /v1/bamboo/copilot/auth/status endpoint (unauthenticated).
#[actix_web::test]
async fn test_copilot_auth_status_unauthenticated() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/auth/status",
        web::post().to(copilot_auth::get_copilot_auth_status),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/status")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Verify response structure
    assert!(
        json.get("authenticated").is_some(),
        "Response should contain authenticated field"
    );
    assert!(
        json.get("message").is_some(),
        "Response should contain message field"
    );

    // When unauthenticated, should return false
    assert_eq!(json["authenticated"].as_bool(), Some(false));
}

/// Test POST /v1/bamboo/copilot/auth/status endpoint structure.
#[actix_web::test]
async fn test_copilot_auth_status_response_structure() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/auth/status",
        web::post().to(copilot_auth::get_copilot_auth_status),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/status")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Verify response is an object with authenticated boolean
    assert!(json.is_object(), "Response should be a JSON object");
    assert!(
        json["authenticated"].is_boolean(),
        "authenticated field should be a boolean"
    );
    assert!(
        json["message"].is_string() || json["message"].is_null(),
        "message field should be a string or null"
    );
}
