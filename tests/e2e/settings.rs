//! E2E tests for /v1/bamboo/* settings endpoints

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::settings;
use serde_json::json;

// ============================================================================
// Workflow Tests
// ============================================================================

#[actix_web::test]
async fn test_list_workflows_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows",
        web::get().to(settings::list_workflows),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_list_workflows_returns_json_array() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows",
        web::get().to(settings::list_workflows),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows")
        .to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;

    // Should be valid JSON array
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.is_array());
}

#[actix_web::test]
async fn test_create_and_get_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/workflows",
                web::post().to(settings::save_workflow),
            )
            .route(
                "/v1/bamboo/workflows/{name}",
                web::get().to(settings::get_workflow),
            ),
    )
    .await;

    // Create a workflow
    let create_req = test::TestRequest::post()
        .uri("/v1/bamboo/workflows")
        .set_json(&json!({
            "name": "test-workflow",
            "content": "# Test Workflow\n\nThis is a test workflow."
        }))
        .to_request();

    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());

    // Get the workflow
    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows/test-workflow")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert_eq!(result["name"], "test-workflow");
    assert!(result["content"]
        .as_str()
        .unwrap()
        .contains("# Test Workflow"));
}

#[actix_web::test]
async fn test_delete_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route(
                "/v1/bamboo/workflows",
                web::post().to(settings::save_workflow),
            )
            .route(
                "/v1/bamboo/workflows/{name}",
                web::delete().to(settings::delete_workflow),
            )
            .route(
                "/v1/bamboo/workflows/{name}",
                web::get().to(settings::get_workflow),
            ),
    )
    .await;

    // Create a workflow
    let create_req = test::TestRequest::post()
        .uri("/v1/bamboo/workflows")
        .set_json(&json!({
            "name": "workflow-to-delete",
            "content": "# Workflow to Delete"
        }))
        .to_request();

    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());

    // Delete the workflow
    let delete_req = test::TestRequest::delete()
        .uri("/v1/bamboo/workflows/workflow-to-delete")
        .to_request();

    let delete_resp = test::call_service(&app, delete_req).await;
    assert!(delete_resp.status().is_success());

    // Try to get the deleted workflow - should return 404
    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows/workflow-to-delete")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert_eq!(get_resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn test_get_nonexistent_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows/{name}",
        web::get().to(settings::get_workflow),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/workflows/nonexistent-workflow")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

// ============================================================================
// Config Tests
// ============================================================================

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

    // Should return an object (empty or with config)
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

    // Set config
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

    // Get config to verify
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

    // Set initial config
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

    // Update with additional field
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

    // Get config to verify update
    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/config")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Provider should be updated
    assert_eq!(result["provider"], "anthropic");
    // New field should exist
    assert_eq!(result["field2"], "value2");
}

// ============================================================================
// Provider Config Tests
// ============================================================================

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

    // Should have required fields
    assert!(result.get("provider").is_some());
    assert!(result.get("available_providers").is_some());
    assert!(result.get("providers").is_some());

    // available_providers should be an array
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

    // Update provider config
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

    // Should have success field
    assert_eq!(result["success"], true);

    // Get provider config to verify update
    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/settings/provider")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Provider should be updated
    assert_eq!(result["provider"], "openai");
    // API key should be masked
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

    // Set provider with API key
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

    // Get config - API key should be masked
    let get_req = test::TestRequest::get()
        .uri("/v1/bamboo/settings/provider")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // API key should be masked, not the real key
    let api_key = result["providers"]["anthropic"]["api_key"]
        .as_str()
        .unwrap();
    assert!(api_key.contains("*"));
    assert!(!api_key.contains("real-secret-key"));
}

// ============================================================================
// Setup Status Tests
// ============================================================================

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

    // Should have required fields
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

    // Mark setup as complete
    let complete_req = test::TestRequest::post()
        .uri("/v1/bamboo/setup/complete")
        .to_request();

    let complete_resp = test::call_service(&app, complete_req).await;
    assert!(complete_resp.status().is_success());

    // Check status
    let status_req = test::TestRequest::get()
        .uri("/v1/bamboo/setup/status")
        .to_request();

    let status_resp = test::call_service(&app, status_req).await;
    assert!(status_resp.status().is_success());

    let body = test::read_body(status_resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Setup should be marked as complete
    assert_eq!(result["is_complete"], true);
}

// ============================================================================
// Keyword Masking Tests
// ============================================================================

#[actix_web::test]
async fn test_get_keyword_masking_config() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/keyword-masking",
        web::get().to(settings::get_keyword_masking_config),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/keyword-masking")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have entries field (array)
    assert!(result.get("entries").is_some());
    assert!(result["entries"].is_array());
}

// ============================================================================
// Invalid Input Tests
// ============================================================================

#[actix_web::test]
async fn test_save_workflow_with_invalid_name() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows",
        web::post().to(settings::save_workflow),
    ))
    .await;

    // Try to create workflow with invalid name (containing path traversal)
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/workflows")
        .set_json(&json!({
            "name": "../../../etc/passwd",
            "content": "malicious content"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should reject invalid workflow name
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_update_provider_with_invalid_provider() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/settings/provider",
        web::post().to(settings::update_provider_config),
    ))
    .await;

    // Try to set invalid provider
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/settings/provider")
        .set_json(&json!({
            "provider": "invalid-provider-name",
            "providers": {}
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should handle invalid provider gracefully
    // The response might be 200 with success: false or an error status
    assert!(resp.status().is_client_error() || resp.status().is_success());
}

#[actix_web::test]
async fn test_delete_nonexistent_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows/{name}",
        web::delete().to(settings::delete_workflow),
    ))
    .await;

    let req = test::TestRequest::delete()
        .uri("/v1/bamboo/workflows/nonexistent-workflow")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return 404 for non-existent workflow
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}
