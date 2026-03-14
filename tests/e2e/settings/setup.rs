use super::*;

#[actix_web::test]
async fn test_get_setup_status() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/setup/status",
        web::get().to(settings::get_setup_status),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/setup/status")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.get("is_complete").is_some());
    assert!(result.get("has_proxy_config").is_some());
    assert!(result.get("has_proxy_env").is_some());
    assert!(result.get("message").is_some());
}

#[actix_web::test]
async fn test_mark_setup_complete() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/setup/complete",
                web::post().to(settings::mark_setup_complete),
            )
            .route(
                "/v1/bamboo/setup/status",
                web::get().to(settings::get_setup_status),
            ),
    )
    .await;

    let complete_req = test::TestRequest::post()
        .uri("/v1/bamboo/setup/complete")
        .to_request();

    let complete_resp = test::call_service(&app, complete_req).await;
    assert!(complete_resp.status().is_success());

    let status_req = test::TestRequest::get()
        .uri("/v1/bamboo/setup/status")
        .to_request();

    let status_resp = test::call_service(&app, status_req).await;
    assert!(status_resp.status().is_success());

    let body = test::read_body(status_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["is_complete"], true);
}
