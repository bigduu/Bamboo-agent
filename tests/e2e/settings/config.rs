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
async fn test_get_bamboo_tools() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/tools",
        web::get().to(settings::get_bamboo_tools),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/tools")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result["tools"].is_array());
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
        .set_json(json!({
            "provider": "openai",
            "providers": {
                "openai": {
                    "api_key": "sk-test"
                }
            }
        }))
        .to_request();

    let set_resp = test::call_service(&app, set_req).await;
    assert!(set_resp.status().is_success());
    let proxy_req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "http_proxy": "http://proxy:8080"
        }))
        .to_request();
    let proxy_resp = test::call_service(&app, proxy_req).await;
    assert!(proxy_resp.status().is_success());

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
async fn test_lifecycle_hooks_round_trip_persists_and_reloads() {
    let state = crate::e2e::common::create_test_app().await;
    let data_dir = state.app_data_dir.clone();
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
    let lifecycle_hooks = json!({
        "enabled": true,
        "PreToolUse": [{
            "enabled": false,
            "matcher": "^Bash$",
            "hooks": [{"type": "command", "command": "echo checked", "timeout_ms": 2500}]
        }],
        "SessionEnd": [{
            "hooks": [{"type": "command", "command": "echo complete"}]
        }]
    });

    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/v1/bamboo/config")
            .set_json(json!({"lifecycle_hooks": lifecycle_hooks}))
            .to_request(),
    )
    .await;
    assert!(response.status().is_success());

    let persisted: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(data_dir.join("hooks.json")).expect("hooks.json persisted"),
    )
    .unwrap();
    let persisted = persisted.get("data").unwrap_or(&persisted);
    assert_eq!(persisted["lifecycle_hooks"]["enabled"], true);
    assert_eq!(
        persisted["lifecycle_hooks"]["PreToolUse"][0]["enabled"],
        false
    );
    assert_eq!(
        persisted["lifecycle_hooks"]["PreToolUse"][0]["hooks"][0]["command"],
        "echo checked"
    );

    let reloaded = state.reload_config().await;
    assert!(reloaded.lifecycle_hooks.enabled);
    assert!(!reloaded.lifecycle_hooks.pre_tool_use[0].enabled);
    assert_eq!(
        reloaded.lifecycle_hooks.pre_tool_use[0].hooks[0].timeout_ms,
        2_500
    );

    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/v1/bamboo/config")
            .to_request(),
    )
    .await;
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["lifecycle_hooks"]["PreToolUse"][0]["enabled"], false);
    assert_eq!(
        body["lifecycle_hooks"]["SessionEnd"][0]["hooks"][0]["command"],
        "echo complete"
    );
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
        .set_json(json!({
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
        .set_json(json!({
            "provider": "anthropic"
        }))
        .to_request();
    let set_resp = test::call_service(&app, set_req).await;
    assert!(set_resp.status().is_success());

    let auth_req = test::TestRequest::post()
        .uri("/v1/bamboo/proxy-auth")
        .set_json(json!({
            "expected_revision": 0,
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
        .set_json(json!({
            "provider": "openai",
            "providers": {
                "openai": {
                    "api_key": "sk-test"
                }
            }
        }))
        .to_request();

    let set_resp1 = test::call_service(&app, set_req1).await;
    assert!(set_resp1.status().is_success());
    let field_req1 = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({"field1": "value1"}))
        .to_request();
    assert!(test::call_service(&app, field_req1)
        .await
        .status()
        .is_success());

    let set_req2 = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "provider": "anthropic",
            "providers": {
                "anthropic": {
                    "api_key": "sk-ant-test"
                }
            }
        }))
        .to_request();

    let set_resp2 = test::call_service(&app, set_req2).await;
    assert!(set_resp2.status().is_success());
    let field_req2 = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({"field2": "value2"}))
        .to_request();
    assert!(test::call_service(&app, field_req2)
        .await
        .status()
        .is_success());

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
