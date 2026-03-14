use super::*;

#[actix_web::test]
async fn test_get_bamboo_config() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/config",
        web::get().to(settings::get_bamboo_config),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/config")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_object());
}

#[actix_web::test]
async fn test_set_bamboo_config() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/config",
                web::post().to(settings::set_bamboo_config),
            )
            .route(
                "/v1/bamboo/config",
                web::get().to(settings::get_bamboo_config),
            ),
    )
    .await;

    let set_req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(&json!({
            "provider": "openai",
            "http_proxy": "http://proxy:8080",
            "providers": {
                "openai": {
                    "api_key": "sk-test"
                }
            }
        }))
        .to_request();

    let set_resp = test::call_service(&app, set_req).await;
    assert!(set_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/config")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["provider"], "openai");
    assert_eq!(result["http_proxy"], "http://proxy:8080");
}

#[actix_web::test]
async fn test_set_bamboo_config_allows_incomplete_provider_config() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/config",
                web::post().to(settings::set_bamboo_config),
            )
            .route(
                "/v1/bamboo/config",
                web::get().to(settings::get_bamboo_config),
            ),
    )
    .await;

    let set_req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(&json!({
            "provider": "anthropic",
            "http_proxy": "http://proxy:8080"
        }))
        .to_request();

    let set_resp = test::call_service(&app, set_req).await;
    assert!(set_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/config")
        .to_request();
    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");
    assert_eq!(result["provider"], "anthropic");
    assert_eq!(result["http_proxy"], "http://proxy:8080");
}

#[actix_web::test]
async fn test_set_proxy_auth_does_not_fail_when_provider_unconfigured() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/config",
                web::post().to(settings::set_bamboo_config),
            )
            .route(
                "/v1/bamboo/proxy-auth",
                web::post().to(settings::set_proxy_auth),
            ),
    )
    .await;

    let set_req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(&json!({
            "provider": "anthropic"
        }))
        .to_request();
    let set_resp = test::call_service(&app, set_req).await;
    assert!(set_resp.status().is_success());

    let auth_req = test::TestRequest::post()
        .uri("/v1/bamboo/proxy-auth")
        .set_json(&json!({
            "username": "user",
            "password": "pass"
        }))
        .to_request();
    let auth_resp = test::call_service(&app, auth_req).await;
    assert!(auth_resp.status().is_success());
}

#[actix_web::test]
async fn test_update_bamboo_config_merges() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/config",
                web::post().to(settings::set_bamboo_config),
            )
            .route(
                "/v1/bamboo/config",
                web::get().to(settings::get_bamboo_config),
            ),
    )
    .await;

    let set_req1 = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(&json!({
            "provider": "openai",
            "field1": "value1",
            "providers": {
                "openai": {
                    "api_key": "sk-test"
                }
            }
        }))
        .to_request();

    let set_resp1 = test::call_service(&app, set_req1).await;
    assert!(set_resp1.status().is_success());

    let set_req2 = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(&json!({
            "provider": "anthropic",
            "field2": "value2",
            "providers": {
                "anthropic": {
                    "api_key": "sk-ant-test"
                }
            }
        }))
        .to_request();

    let set_resp2 = test::call_service(&app, set_req2).await;
    assert!(set_resp2.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/config")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["provider"], "anthropic");
    assert_eq!(result["field2"], "value2");
}
