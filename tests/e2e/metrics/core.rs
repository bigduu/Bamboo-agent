use super::*;

#[actix_web::test]
async fn test_metrics_summary_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/summary",
        web::get().to(handlers::metrics::summary),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/summary")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_by_model_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/by-model",
        web::get().to(handlers::metrics::by_model),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/by-model")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_sessions_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/sessions",
        web::get().to(handlers::metrics::sessions),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/sessions")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_session_detail_endpoint() {
    let state = crate::e2e::common::create_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/sessions/{session_id}",
        web::get().to(handlers::metrics::session_detail),
    ))
    .await;

    let uri = format!("/api/v1/metrics/sessions/{}", session_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return metrics or appropriate error
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_metrics_daily_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/daily",
        web::get().to(handlers::metrics::daily),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/daily")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_persistence_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/persistence",
        web::get().to(handlers::metrics::persistence),
    ))
    .await;

    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/metrics/persistence")
            .to_request(),
    )
    .await;

    assert!(response.status().is_success());
    let body: serde_json::Value = test::read_body_json(response).await;
    assert!(body["full_save"]["lock_wait"]["p95_ms"].is_u64());
    assert!(body["runtime_save"]["lock_hold"]["p95_ms"].is_u64());
    assert!(body["search_index"]["p95_ms"].is_u64());
    assert!(body["create_latency"]["p95_ms"].is_u64());
    assert!(body["pending_search_jobs"].is_u64());
}
