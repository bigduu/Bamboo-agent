use super::*;

/// Test full authentication flow simulation.
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

/// Test that all Copilot auth endpoints are properly registered.
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
