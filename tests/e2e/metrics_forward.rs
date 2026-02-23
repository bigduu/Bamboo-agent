//! E2E tests for /api/v1/metrics/forward/* endpoints

use actix_web::{test, web, App};
use bamboo_agent::agent::server::handlers;
use bamboo_agent::agent::server::state::AppState;

#[actix_web::test]
async fn test_metrics_forward_summary_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/metrics/forward/summary",
                web::get().to(handlers::metrics::forward_summary),
            ),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/forward/summary")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_forward_by_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/metrics/forward/by-endpoint",
                web::get().to(handlers::metrics::forward_by_endpoint),
            ),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/forward/by-endpoint")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_forward_requests() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/metrics/forward/requests",
                web::get().to(handlers::metrics::forward_requests),
            ),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/forward/requests")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}
