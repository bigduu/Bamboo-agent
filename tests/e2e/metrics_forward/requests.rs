use super::*;

#[actix_web::test]
async fn test_metrics_forward_requests() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/forward/requests",
        web::get().to(handlers::metrics::forward_requests),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/forward/requests")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}
