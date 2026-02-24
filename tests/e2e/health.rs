//! E2E tests for /api/v1/health endpoint

use actix_web::{test, web, App};
use bamboo_agent::server::app_state::AppState;
use bamboo_agent::server::handlers;

#[actix_web::test]
async fn test_health_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/api/v1/health", web::get().to(handlers::health::handler)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/health").to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_health_returns_ok() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/api/v1/health", web::get().to(handlers::health::handler)),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/v1/health").to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;

    assert_eq!(body, "OK");
}
