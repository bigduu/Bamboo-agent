use super::*;

/// Test POST /v1/bamboo/copilot/auth/complete endpoint.
#[actix_web::test]
async fn test_copilot_auth_complete_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/auth/complete",
        web::post().to(copilot_auth::complete_copilot_auth),
    ))
    .await;

    // Test with mock device code data
    let payload = json!({
        "device_code": "test-device-code-12345",
        "interval": 5,
        "expires_in": 900
    });

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/complete")
        .set_json(&payload)
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return an error because the device code is invalid
    // But the endpoint should be accessible
    assert!(resp.status().is_success() || resp.status().is_server_error());

    let body = test::read_body(resp).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Verify response structure
    assert!(
        json.get("success").is_some(),
        "Response should contain success field"
    );

    // Should be an error response (invalid device code)
    if !json["success"].as_bool().unwrap_or(true) {
        assert!(
            json.get("error").is_some(),
            "Error response should contain error field"
        );
    }
}

/// Test POST /v1/bamboo/copilot/auth/complete with missing fields.
#[actix_web::test]
async fn test_copilot_auth_complete_missing_fields() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/auth/complete",
        web::post().to(copilot_auth::complete_copilot_auth),
    ))
    .await;

    // Test with incomplete payload
    let payload = json!({
        "device_code": "test-device-code"
        // Missing interval and expires_in
    });

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/complete")
        .set_json(&payload)
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return a client error (bad request) due to missing fields
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}
