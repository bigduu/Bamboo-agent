use super::*;

/// Test POST /v1/bamboo/copilot/authenticate endpoint (legacy).
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
    assert!(
        status.is_client_error(),
        "Expected client error (400), got status: {}",
        status
    );

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
