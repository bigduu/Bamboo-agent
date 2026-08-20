use super::*;

#[actix_web::test]
async fn test_full_setup_and_provider_flow_does_not_conflict() {
    let state = crate::e2e::common::create_test_app().await;
    let data_dir = state.app_data_dir.clone();
    let core_path = data_dir.join("core.json");
    let providers_path = data_dir.join("providers.json");

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;

    // 1) Setup flow: switch provider to an incomplete provider, then update
    // proxy metadata as a separate section commit. Compatibility writes are
    // intentionally single-section under the modular facade.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "provider": "anthropic"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let req = test::TestRequest::get()
        .uri("/v1/bamboo/config/sections/core")
        .to_request();
    let mut core: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    core["data"]["http_proxy"] = json!("http://proxy:8080");
    let req = test::TestRequest::put()
        .uri("/v1/bamboo/config/sections/core")
        .set_json(json!({
            "expected_revision": core["revision"],
            "data": core["data"]
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 2) Persist proxy auth against the Core section revision exposed by the
    // secret-free status endpoint, then mark setup complete.
    let req = test::TestRequest::get()
        .uri("/v1/bamboo/proxy-auth/status")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let proxy_status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(proxy_status["configured"], false);
    let core_revision = proxy_status["revision"].as_u64().unwrap();

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/proxy-auth")
        .set_json(json!({
            "expected_revision": core_revision,
            "action": "replace",
            "username": "proxy-user-name",
            "password": "proxy-pass-value"
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
    assert_eq!(status["credential_ref"], "proxy.default.auth");
    assert!(status.get("username").is_none());
    let root = std::fs::read_to_string(&core_path).unwrap();
    assert!(root.contains("proxy_auth_credential_ref"));
    assert!(!root.contains("proxy_auth_encrypted"));
    assert!(!root.contains("\"username\""));
    assert!(!root.contains("\"password\""));
    let credentials = std::fs::read_to_string(data_dir.join("credentials.json")).unwrap();
    assert!(!credentials.contains("proxy-user-name"));
    assert!(!credentials.contains("proxy-pass-value"));

    // Attempt to inject proxy_auth_encrypted via permissive endpoint - it must
    // fail closed in favor of the revisioned Core/proxy-auth APIs.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "proxy_auth_encrypted": "deadbeef:deadbeef"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    // Proxy auth should remain configured (sanitize must prevent overwriting credentials).
    let req = test::TestRequest::get()
        .uri("/v1/bamboo/proxy-auth/status")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["configured"], true);
    assert_eq!(status["credential_ref"], "proxy.default.auth");
    assert!(status.get("username").is_none());

    // Mark setup complete (writes into Config.extra["setup"]).
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/setup/complete")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 3) The canonical provider-settings endpoint is revisioned and validates
    // instance credentials before committing an enabled default.
    let req = test::TestRequest::get()
        .uri("/v1/bamboo/config/provider-settings")
        .to_request();
    let provider_settings: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    let provider_revision = provider_settings["revision"].as_u64().unwrap();
    let mut provider_data = provider_settings["data"].clone();
    provider_data["provider"] = json!("openai");
    provider_data["providers"] = json!({});
    provider_data["provider_instances"] = json!({
        "openai": {
            "provider_type": "openai",
            "label": "OpenAI",
            "model": "gpt-4",
            "enabled": true
        }
    });
    provider_data["default_provider_instance_id"] = json!("openai");
    provider_data["defaults"] = json!({
        "chat": {"provider": "openai", "model": "gpt-4"}
    });

    let req = test::TestRequest::put()
        .uri("/v1/bamboo/config/provider-settings")
        .set_json(json!({
            "expected_revision": provider_revision,
            "data": provider_data
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    let body = test::read_body(resp).await;
    let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Canonical nested error envelope (#251 finding 2 / #507).
    assert_eq!(err["error"]["type"], "api_error");
    assert!(!err["error"]["message"].as_str().unwrap_or("").is_empty());

    // 4) Supplying an explicit credential action commits the same canonical
    // instance shape and routes the secret to credentials.json while
    // providers.json stores only a stable reference.
    let req = test::TestRequest::put()
        .uri("/v1/bamboo/config/provider-settings")
        .set_json(json!({
            "expected_revision": provider_revision,
            "data": provider_data,
            "credential_changes": {
                "provider_instances": {
                    "openai": {"action": "replace", "value": "sk-test-key"}
                }
            }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
    assert!(!body.contains("sk-test-key"));
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        body["data"]["credential_status"]["provider_instances"]["openai"]["configured"],
        true
    );

    let providers_document = read_config_json(&providers_path);
    let providers = config_document_data(&providers_document);
    let openai_ref_before = providers
        .get("provider_instances")
        .and_then(|instances| instances.get("openai"))
        .and_then(|o| o.get("credential_ref"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    assert_eq!(openai_ref_before, "provider_instance.openai.api_key");
    assert!(providers["provider_instances"]["openai"]
        .get("api_key_encrypted")
        .is_none());
    let providers_raw = std::fs::read_to_string(&providers_path).unwrap();
    let credentials_raw = std::fs::read_to_string(data_dir.join("credentials.json")).unwrap();
    assert!(!providers_raw.contains("sk-test-key"));
    assert!(!credentials_raw.contains("sk-test-key"));
    let reference =
        bamboo_config::credential_ref("provider_instance", "openai", "api_key").unwrap();
    assert_eq!(
        state
            .credential_store
            .resolve(&reference)
            .unwrap()
            .unwrap()
            .expose(),
        "sk-test-key"
    );
    let reloaded = bamboo_config::Config::from_data_dir_without_env(Some(data_dir.clone()));
    assert_eq!(
        reloaded.default_provider_instance.as_deref(),
        Some("openai")
    );
    let reloaded_openai = reloaded
        .provider_instances
        .get("openai")
        .expect("canonical provider instance should persist");
    assert_eq!(reloaded_openai.api_key, "sk-test-key");
    assert_eq!(
        reloaded_openai
            .credential_ref
            .as_ref()
            .map(|reference| reference.as_str()),
        Some("provider_instance.openai.api_key")
    );
    assert!(reloaded.providers().openai.is_none());

    // Attempt to inject api_key_encrypted via the permissive endpoint after
    // the canonical write. It must be ignored without resurrecting the
    // retired type-keyed alias.
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
    let openai_ref_after = providers
        .get("provider_instances")
        .and_then(|instances| instances.get("openai"))
        .and_then(|o| o.get("credential_ref"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    assert_eq!(openai_ref_after, openai_ref_before);
    assert!(providers.get("openai").is_none());
    assert!(providers["provider_instances"]["openai"]
        .get("api_key_encrypted")
        .is_none());

    // Ensure the permissive endpoint merges without clobbering prior provider/setup state.
    let req = test::TestRequest::get()
        .uri("/v1/bamboo/config/sections/core")
        .to_request();
    let mut core: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    core["data"]["https_proxy"] = json!("http://proxy:8080");
    let req = test::TestRequest::put()
        .uri("/v1/bamboo/config/sections/core")
        .set_json(json!({
            "expected_revision": core["revision"],
            "data": core["data"]
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config")
        .set_json(json!({
            "keyword_masking": { "entries": [] }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/config")
        .to_request();
    let cfg: serde_json::Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(cfg["provider"], "openai");
    assert_eq!(cfg["http_proxy"], "http://proxy:8080");
    assert_eq!(cfg["https_proxy"], "http://proxy:8080");
    assert!(cfg.get("setup").is_some(), "expected setup to still exist");
    let providers_document = read_config_json(&providers_path);
    let providers = config_document_data(&providers_document);
    assert_eq!(
        providers["provider_instances"]["openai"]["credential_ref"],
        "provider_instance.openai.api_key",
        "metadata-only updates must preserve the existing credential ref"
    );
    assert!(providers.get("openai").is_none());
    assert!(providers["provider_instances"]["openai"]
        .get("api_key_encrypted")
        .is_none());
}

#[actix_web::test]
async fn test_bamboo_config_persists_disabled_skills() {
    let state = crate::e2e::common::create_test_app().await;
    let data_dir = state.app_data_dir.clone();
    let config_path = data_dir.join("tools-skills.json");

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

    let document = read_config_json(&config_path);
    let cfg = config_document_data(&document);
    assert_eq!(cfg["skills"]["disabled"], json!(["pdf", "skill-creator"]));
}
