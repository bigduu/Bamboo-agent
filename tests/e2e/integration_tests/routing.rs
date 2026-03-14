use super::*;

#[actix_web::test]
async fn test_full_api_routing() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).service(create_api_scope())).await;

    // Test health endpoint as basic connectivity check
    let req = test::TestRequest::get().uri("/api/v1/health").to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_all_endpoints_respond() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).service(create_api_scope())).await;

    let session_id = uuid::Uuid::new_v4().to_string();

    // Test a representative set of endpoints to verify routing works
    let endpoints = vec![
        format!("/api/v1/history/{}", session_id),
        format!("/api/v1/todo/{}", session_id),
        format!("/api/v1/todo/{}/exists", session_id),
        format!("/api/v1/respond/{}/pending", session_id),
        "/api/v1/metrics/summary".to_string(),
        "/api/v1/metrics/by-model".to_string(),
        "/api/v1/metrics/sessions".to_string(),
        "/api/v1/metrics/daily".to_string(),
        "/api/v1/metrics/v2/summary".to_string(),
        "/api/v1/metrics/v2/timeline".to_string(),
        "/api/v1/mcp/servers".to_string(),
        "/api/v1/mcp/tools".to_string(),
    ];

    for endpoint in endpoints {
        let req = test::TestRequest::get().uri(&endpoint).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(
            resp.status().is_success() || resp.status().is_client_error(),
            "Endpoint {} should respond",
            endpoint
        );
    }
}
