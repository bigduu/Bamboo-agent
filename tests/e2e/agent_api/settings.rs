use super::*;

#[actix_web::test]
async fn test_get_settings_default() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/settings",
        web::get().to(agent_api::get_claude_settings),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/settings")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let settings: Value = serde_json::from_slice(&body).expect("Failed to parse response");
    assert!(settings.is_object());
}

#[actix_web::test]
async fn test_save_and_get_settings() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/agent/settings",
                web::post().to(agent_api::save_claude_settings),
            )
            .route(
                "/v1/agent/settings",
                web::get().to(agent_api::get_claude_settings),
            ),
    )
    .await;

    let save_req = test::TestRequest::post()
        .uri("/v1/agent/settings")
        .set_json(json!({
            "settings": {
                "model": "claude-3-5-sonnet-20241022",
                "test_key": "test_value"
            }
        }))
        .to_request();

    let save_resp = test::call_service(&app, save_req).await;
    assert!(save_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/agent/settings")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let settings: Value = serde_json::from_slice(&body).expect("Failed to parse response");
    assert!(settings.is_object());
}

#[actix_web::test]
async fn test_save_settings_empty() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/settings",
        web::post().to(agent_api::save_claude_settings),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/settings")
        .set_json(json!({ "settings": {} }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}
