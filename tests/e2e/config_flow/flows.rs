use super::*;

#[actix_web::test]
async fn test_full_setup_and_provider_flow_does_not_conflict() {
    let state = crate::e2e::common::create_test_app().await;
    let data_dir = state.app_data_dir.clone();
    let config_path = data_dir.join("config.json");
    let providers_path = data_dir.join("providers.json");

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;

    // 1) Setup flow: write proxy config + switch provider to an incomplete provider.
    // This MUST NOT fail with provider validation errors.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "provider": "anthropic",
            "http_proxy": "http://proxy:8080"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 2) Persist proxy auth (optional) and mark setup complete.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/proxy-auth")
        .set_json(json!({
            "username": "user",
            "password": "pass"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // Verify proxy auth is configured via status endpoint (not by comparing encrypted blobs,
    // which are re-encrypted with a random nonce on each save).
    let req = test::TestRequest::get()
        .uri("/v1/bamboo/proxy-auth/status")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["configured"], true);
    assert_eq!(status["username"], "user");

    // Attempt to inject proxy_auth_encrypted via permissive endpoint - must be ignored.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "proxy_auth_encrypted": "deadbeef:deadbeef"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // Proxy auth should remain configured (sanitize must prevent overwriting credentials).
    let req = test::TestRequest::get()
        .uri("/v1/bamboo/proxy-auth/status")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["configured"], true);
    assert_eq!(status["username"], "user");

    // Mark setup complete (writes into Config.extra["setup"]).
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/setup/complete")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 3) Strict provider endpoint: invalid provider config => 400.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/settings/provider")
        .set_json(json!({
            "provider": "openai",
            "providers": { "openai": { "model": "gpt-4" } }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    let body = test::read_body(resp).await;
    let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(err["success"], false);
    // Canonical nested error envelope (#251 finding 2 / #507).
    assert_eq!(err["error"]["type"], "api_error");
    assert!(err["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("Invalid configuration"));

    // 4) Strict provider endpoint: valid provider config => 200 and persists encrypted key.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/settings/provider")
        .set_json(json!({
            "provider": "openai",
            "providers": { "openai": { "api_key": "sk-test-key", "model": "gpt-4" } }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let providers_document = read_config_json(&providers_path);
    let providers = config_document_data(&providers_document);
    let openai_encrypted_before = providers
        .get("openai")
        .and_then(|o| o.get("api_key_encrypted"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    assert!(
        !openai_encrypted_before.is_empty(),
        "expected providers.openai.api_key_encrypted to be persisted"
    );

    // Attempt to inject api_key_encrypted via permissive endpoint - must be ignored.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "providers": { "openai": { "api_key_encrypted": "deadbeef" } }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let providers_document = read_config_json(&providers_path);
    if providers_document.get("data").is_some() {
        assert_eq!(providers_document["schema_version"], 1);
        assert!(
            providers_document["revision"]
                .as_u64()
                .is_some_and(|revision| revision >= 1),
            "revisioned providers envelope must have a positive revision"
        );
    }
    let providers = config_document_data(&providers_document);
    let openai_encrypted_after = providers
        .get("openai")
        .and_then(|o| o.get("api_key_encrypted"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    assert!(
        !openai_encrypted_after.is_empty(),
        "expected providers.openai.api_key_encrypted to remain persisted"
    );
    assert!(
        openai_encrypted_after.contains(':'),
        "expected nonce:ciphertext format"
    );
    assert_ne!(openai_encrypted_after, "deadbeef");

    // Ensure the permissive endpoint merges without clobbering prior provider/setup state.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "https_proxy": "http://proxy:8080",
            "keyword_masking": { "entries": [] }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let cfg = read_config_json(&config_path);
    assert_eq!(cfg["provider"], "openai");
    assert_eq!(cfg["http_proxy"], "http://proxy:8080");
    assert_eq!(cfg["https_proxy"], "http://proxy:8080");
    assert!(cfg.get("setup").is_some(), "expected setup to still exist");
}

#[actix_web::test]
async fn test_bamboo_config_persists_disabled_skills() {
    let state = crate::e2e::common::create_test_app().await;
    let data_dir = state.app_data_dir.clone();
    let config_path = data_dir.join("config.json");

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "skills": {
                "disabled": [" pdf ", "skill-creator", "pdf"]
            }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let cfg = read_config_json(&config_path);
    assert_eq!(cfg["skills"]["disabled"], json!(["pdf", "skill-creator"]));
}
