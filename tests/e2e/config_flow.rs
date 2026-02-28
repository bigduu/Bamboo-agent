//! End-to-end flow tests for the unified config system.
//!
//! These tests exercise multiple endpoints in sequence to catch regressions where:
//! - config patches clobber other sections (lost updates)
//! - permissive config endpoint incorrectly enforces provider validation
//! - strict provider endpoint returns proper HTTP codes and messages
//! - config patch sanitization prevents encrypted material injection

use actix_web::{test, App};
use bamboo_agent::server::configure_routes;
use serde_json::json;

fn read_config_json(path: &std::path::Path) -> serde_json::Value {
    let raw = std::fs::read_to_string(path).expect("config.json should be readable");
    serde_json::from_str(&raw).expect("config.json should be valid JSON")
}

#[actix_web::test]
async fn test_full_setup_and_provider_flow_does_not_conflict() {
    let state = crate::e2e::common::create_test_app().await;
    let data_dir = state.app_data_dir.clone();
    let config_path = data_dir.join("config.json");

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
        .set_json(&json!({
            "provider": "anthropic",
            "http_proxy": "http://proxy:8080"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // 2) Persist proxy auth (optional) and mark setup complete.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/proxy-auth")
        .set_json(&json!({
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
        .set_json(&json!({
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
        .set_json(&json!({
            "provider": "openai",
            "providers": { "openai": { "model": "gpt-4" } }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

    let body = test::read_body(resp).await;
    let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(err["success"], false);
    assert!(err["error"]
        .as_str()
        .unwrap_or("")
        .contains("Invalid configuration"));

    // 4) Strict provider endpoint: valid provider config => 200 and persists encrypted key.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/settings/provider")
        .set_json(&json!({
            "provider": "openai",
            "providers": { "openai": { "api_key": "sk-test-key", "model": "gpt-4" } }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let cfg = read_config_json(&config_path);
    let openai_encrypted_before = cfg
        .get("providers")
        .and_then(|p| p.get("openai"))
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
        .set_json(&json!({
            "providers": { "openai": { "api_key_encrypted": "deadbeef" } }
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let cfg = read_config_json(&config_path);
    let openai_encrypted_after = cfg
        .get("providers")
        .and_then(|p| p.get("openai"))
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
        .set_json(&json!({
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
async fn test_validate_config_patch_reports_domain_errors() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    // Invalid proxy URL should be reported under proxy domain.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config/validate")
        .set_json(&json!({
            "http_proxy": "http://"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["valid"], false);
    assert!(result["errors"]["proxy"].as_array().unwrap().len() >= 1);

    // Invalid setup shape should be reported under setup domain.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config/validate")
        .set_json(&json!({
            "setup": "nope"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["valid"], false);
    assert!(result["errors"]["setup"].as_array().unwrap().len() >= 1);
}
