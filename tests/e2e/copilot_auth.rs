//! E2E tests for /v1/bamboo/copilot/* endpoints
//!
//! These tests cover all 5 Copilot authentication-related endpoints:
//! - POST /v1/bamboo/copilot/auth/start - Start authentication flow
//! - POST /v1/bamboo/copilot/auth/complete - Complete authentication flow
//! - POST /v1/bamboo/copilot/authenticate - Legacy authentication endpoint
//! - POST /v1/bamboo/copilot/auth/status - Check authentication status
//! - POST /v1/bamboo/copilot/logout - Logout and delete tokens

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::copilot_auth;
use serde_json::json;

/// Test POST /v1/bamboo/copilot/auth/start endpoint
///
/// Note: This test verifies the endpoint structure but may fail in CI environments
/// because it requires network access to GitHub's device code endpoint.
/// The test validates that:
/// - Endpoint is accessible
/// - Returns proper JSON structure
/// - Handles errors gracefully
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

/// Test POST /v1/bamboo/copilot/auth/complete endpoint
///
/// This test verifies the complete endpoint structure and error handling.
/// Successful completion requires a valid device code from the start endpoint.
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

/// Test POST /v1/bamboo/copilot/auth/complete with missing fields
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

/// Test POST /v1/bamboo/copilot/authenticate endpoint (legacy)
///
/// This test verifies the legacy authentication endpoint.
/// Default provider is "anthropic", so should return 400 Bad Request.
#[actix_web::test]
async fn test_copilot_authenticate_endpoint_not_copilot() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/copilot/authenticate",
        web::post().to(copilot_auth::authenticate_copilot),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/authenticate")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Default provider is "anthropic", so should return 400 Bad Request
    let status = resp.status();
    assert!(status.is_client_error(),
            "Expected client error (400), got status: {}", status);

    let body = test::read_body(resp).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Verify error response structure
    assert!(json.is_object(), "Response should be a JSON object");
    assert!(json.get("success").is_some());
    assert_eq!(json["success"].as_bool(), Some(false));
    assert!(json.get("error").is_some());
    assert!(
        json["error"].as_str().unwrap().contains("not Copilot"),
        "Error should indicate provider is not Copilot"
    );
}

/// Test POST /v1/bamboo/copilot/auth/status endpoint (unauthenticated)
///
/// This test verifies the auth status endpoint returns proper structure
/// when no token is cached.
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

/// Test POST /v1/bamboo/copilot/auth/status endpoint structure
///
/// This test verifies the response format matches the expected schema.
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

/// Test POST /v1/bamboo/copilot/logout endpoint
///
/// This test verifies the logout endpoint successfully cleans up tokens.
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

/// Test POST /v1/bamboo/copilot/logout idempotency
///
/// Calling logout multiple times should not cause errors.
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

/// Test full authentication flow simulation
///
/// This test simulates the complete flow (start -> status -> logout)
/// without actually authenticating (to avoid external dependencies).
#[actix_web::test]
async fn test_copilot_auth_flow_simulation() {
    let state = crate::e2e::common::create_test_app().await;

    // Configure all routes
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/copilot/auth/start",
                web::post().to(copilot_auth::start_copilot_auth),
            )
            .route(
                "/v1/bamboo/copilot/auth/complete",
                web::post().to(copilot_auth::complete_copilot_auth),
            )
            .route(
                "/v1/bamboo/copilot/auth/status",
                web::post().to(copilot_auth::get_copilot_auth_status),
            )
            .route(
                "/v1/bamboo/copilot/logout",
                web::post().to(copilot_auth::logout_copilot),
            ),
    )
    .await;

    // Step 1: Check initial auth status (should be unauthenticated)
    let status_req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/status")
        .to_request();
    let status_resp = test::call_service(&app, status_req).await;
    assert!(status_resp.status().is_success());
    let status_body = test::read_body(status_resp).await;
    let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
    assert_eq!(status_json["authenticated"].as_bool(), Some(false));

    // Step 2: Attempt to start auth (may fail in CI without network)
    let start_req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/start")
        .to_request();
    let start_resp = test::call_service(&app, start_req).await;
    // Accept either success or error (network-dependent)
    assert!(start_resp.status().is_success() || start_resp.status().is_server_error());

    // Step 3: Logout (should always succeed)
    let logout_req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/logout")
        .to_request();
    let logout_resp = test::call_service(&app, logout_req).await;
    assert!(logout_resp.status().is_success());
    let logout_body = test::read_body(logout_resp).await;
    let logout_json: serde_json::Value = serde_json::from_slice(&logout_body).unwrap();
    assert_eq!(logout_json["success"].as_bool(), Some(true));

    // Step 4: Verify status is still unauthenticated after logout
    let final_status_req = test::TestRequest::post()
        .uri("/v1/bamboo/copilot/auth/status")
        .to_request();
    let final_status_resp = test::call_service(&app, final_status_req).await;
    assert!(final_status_resp.status().is_success());
    let final_status_body = test::read_body(final_status_resp).await;
    let final_status_json: serde_json::Value = serde_json::from_slice(&final_status_body).unwrap();
    assert_eq!(final_status_json["authenticated"].as_bool(), Some(false));
}

/// Test that all Copilot auth endpoints are properly registered
///
/// This test verifies that all 5 endpoints respond to requests (even if with errors).
#[actix_web::test]
async fn test_all_copilot_endpoints_accessible() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/bamboo/copilot/auth/start",
                web::post().to(copilot_auth::start_copilot_auth),
            )
            .route(
                "/v1/bamboo/copilot/auth/complete",
                web::post().to(copilot_auth::complete_copilot_auth),
            )
            .route(
                "/v1/bamboo/copilot/authenticate",
                web::post().to(copilot_auth::authenticate_copilot),
            )
            .route(
                "/v1/bamboo/copilot/auth/status",
                web::post().to(copilot_auth::get_copilot_auth_status),
            )
            .route(
                "/v1/bamboo/copilot/logout",
                web::post().to(copilot_auth::logout_copilot),
            ),
    )
    .await;

    // Test all 5 endpoints
    let endpoints = vec![
        "/v1/bamboo/copilot/auth/start",
        "/v1/bamboo/copilot/auth/complete",
        "/v1/bamboo/copilot/authenticate",
        "/v1/bamboo/copilot/auth/status",
        "/v1/bamboo/copilot/logout",
    ];

    for endpoint in endpoints {
        let req = test::TestRequest::post().uri(endpoint).to_request();
        let resp = test::call_service(&app, req).await;

        // All endpoints should respond (not return 404)
        assert_ne!(
            resp.status(),
            actix_web::http::StatusCode::NOT_FOUND,
            "Endpoint {} should be registered",
            endpoint
        );
    }
}
