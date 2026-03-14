use super::*;

#[actix_web::test]
async fn test_get_provider_config() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/settings/provider",
        web::get().to(settings::get_provider_config),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/settings/provider")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.get("provider").is_some());
    assert!(result.get("available_providers").is_some());
    assert!(result.get("providers").is_some());
    assert!(result["available_providers"].is_array());
}

#[actix_web::test]
async fn test_update_provider_config() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/settings/provider",
                web::post().to(settings::update_provider_config),
            )
            .route(
                "/v1/bamboo/settings/provider",
                web::get().to(settings::get_provider_config),
            ),
    )
    .await;

    let update_req = test::TestRequest::post()
        .uri("/v1/bamboo/settings/provider")
        .set_json(&json!({
            "provider": "openai",
            "providers": {
                "openai": {
                    "api_key": "sk-test-key-123",
                    "model": "gpt-4"
                }
            }
        }))
        .to_request();

    let update_resp = test::call_service(&app, update_req).await;
    assert!(update_resp.status().is_success());

    let body = test::read_body(update_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");
    assert_eq!(result["success"], true);

    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/settings/provider")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["provider"], "openai");
    assert_eq!(result["providers"]["openai"]["api_key"], "****...****");
    assert_eq!(result["providers"]["openai"]["model"], "gpt-4");
}

#[actix_web::test]
async fn test_provider_config_masks_api_keys() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/settings/provider",
                web::post().to(settings::update_provider_config),
            )
            .route(
                "/v1/bamboo/settings/provider",
                web::get().to(settings::get_provider_config),
            ),
    )
    .await;

    let set_req = test::TestRequest::post()
        .uri("/v1/bamboo/settings/provider")
        .set_json(&json!({
            "provider": "anthropic",
            "providers": {
                "anthropic": {
                    "api_key": "sk-ant-real-secret-key-12345678",
                    "model": "claude-3-5-sonnet-20241022"
                }
            }
        }))
        .to_request();

    let set_resp = test::call_service(&app, set_req).await;
    assert!(set_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/settings/provider")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    let api_key = result["providers"]["anthropic"]["api_key"]
        .as_str()
        .expect("api_key should be string");
    assert!(api_key.contains('*'));
    assert!(!api_key.contains("real-secret-key"));
}
