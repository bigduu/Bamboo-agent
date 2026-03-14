use super::*;

/// Test POST /v1/bamboo/copilot/auth/start endpoint.
#[actix_web::test]
async fn test_copilot_auth_start_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/auth/start",
        web::post().to(copilot_auth::start_copilot_auth),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/start")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should respond (may be success or error depending on network access)
    assert!(resp.status().is_success() || resp.status().is_server_error());

    // If successful, verify the response structure
    if resp.status().is_success() {
        let body = test::read_body(resp).await;
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("Response should be valid JSON");

        // Verify required fields in device code response
        assert!(
            json.get("device_code").is_some(),
            "Response should contain device_code"
        );
        assert!(
            json.get("user_code").is_some(),
            "Response should contain user_code"
        );
        assert!(
            json.get("verification_uri").is_some(),
            "Response should contain verification_uri"
        );
        assert!(
            json.get("expires_in").is_some(),
            "Response should contain expires_in"
        );
        assert!(
            json.get("interval").is_some(),
            "Response should contain interval"
        );
    }
}
