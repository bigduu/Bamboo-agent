use super::*;

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
