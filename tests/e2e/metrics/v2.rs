use super::*;

#[actix_web::test]
async fn test_metrics_v2_summary_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/v2/summary",
        web::get().to(handlers::metrics::v2_unified_summary),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/v2/summary")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_v2_timeline_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/v2/timeline",
        web::get().to(handlers::metrics::v2_unified_timeline),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/v2/timeline")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}
