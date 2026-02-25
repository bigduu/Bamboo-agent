//! E2E tests for Claude Code integration endpoints (/v1/agent/*)
//!
//! This module tests all 11 endpoints for Claude Code integration:
//! - Project management (list, create, get sessions)
//! - Settings management (get, save)
//! - System prompt (get, save)
//! - Session lifecycle (running, execute, cancel, jsonl)

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::agent_api;
use serde_json::{json, Value};
use std::path::PathBuf;

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a temporary project directory for testing
fn create_temp_project() -> PathBuf {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    temp_dir.keep()
}

// ============================================================================
// Project Management Tests (4 tests)
// ============================================================================

#[actix_web::test]
async fn test_list_projects_empty() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects",
        web::get().to(agent_api::list_projects),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/projects")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let projects: Vec<Value> = serde_json::from_slice(&body).expect("Failed to parse response");

    // Should return an array (may be empty if no projects exist)
    // Already parsed as Vec<Value>, which is an array
    drop(projects); // Just verify we can parse it
}

#[actix_web::test]
async fn test_create_project_success() {
    let state = crate::e2e::common::create_test_app().await;
    let temp_project = create_temp_project();
    let project_path = temp_project.to_string_lossy().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects",
        web::post().to(agent_api::create_project),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/projects")
        .set_json(json!({
            "path": project_path
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let project: Value = serde_json::from_slice(&body).expect("Failed to parse response");

    // Verify response structure
    assert!(project["id"].is_string());
    assert!(project["path"].is_string());
    assert!(project["sessions"].is_array());
    assert!(project["created_at"].is_number());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_project);
}

#[actix_web::test]
async fn test_create_project_invalid_path() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects",
        web::post().to(agent_api::create_project),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/projects")
        .set_json(json!({
            "path": "/nonexistent/path/12345"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return error for non-existent path
    assert!(resp.status().is_server_error());
}

#[actix_web::test]
async fn test_get_project_sessions_nonexistent() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/projects/{project_id}/sessions",
        web::get().to(agent_api::get_project_sessions),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/projects/nonexistent-project-12345/sessions")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return error for non-existent project
    assert!(resp.status().is_server_error());
}

// ============================================================================
// Settings Management Tests (3 tests)
// ============================================================================

#[actix_web::test]
async fn test_get_settings_default() {
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

    // Should return settings object (may be empty or contain defaults)
    assert!(settings.is_object());
}

#[actix_web::test]
async fn test_save_and_get_settings() {
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

    // Save settings
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

    // Get settings back
    let get_req = test::TestRequest::get()
        .uri("/v1/agent/settings")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let settings: Value = serde_json::from_slice(&body).expect("Failed to parse response");

    // Verify settings structure
    assert!(settings.is_object());
}

#[actix_web::test]
async fn test_save_settings_empty() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/settings",
        web::post().to(agent_api::save_claude_settings),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/settings")
        .set_json(json!({
            "settings": {}
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should accept empty settings
    assert!(resp.status().is_success());
}

// ============================================================================
// System Prompt Tests (2 tests)
// ============================================================================

#[actix_web::test]
async fn test_get_system_prompt_default() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/system-prompt",
        web::get().to(agent_api::get_system_prompt),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/system-prompt")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let prompt: Value = serde_json::from_slice(&body).expect("Failed to parse response");

    // Should return content field (may be empty)
    assert!(prompt["content"].is_string());
    assert!(prompt["path"].is_string());
}

#[actix_web::test]
async fn test_save_and_get_system_prompt() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/agent/system-prompt",
                web::post().to(agent_api::save_system_prompt),
            )
            .route(
                "/v1/agent/system-prompt",
                web::get().to(agent_api::get_system_prompt),
            ),
    )
    .await;

    // Save system prompt
    let save_req = test::TestRequest::post()
        .uri("/v1/agent/system-prompt")
        .set_json(json!({
            "content": "# Test System Prompt\n\nYou are a helpful assistant."
        }))
        .to_request();

    let save_resp = test::call_service(&app, save_req).await;
    assert!(save_resp.status().is_success());

    // Get system prompt back
    let get_req = test::TestRequest::get()
        .uri("/v1/agent/system-prompt")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let prompt: Value = serde_json::from_slice(&body).expect("Failed to parse response");

    // Verify prompt content
    assert!(prompt["content"].is_string());
    assert!(prompt["content"]
        .as_str()
        .unwrap()
        .contains("Test System Prompt"));
}

// ============================================================================
// Session Lifecycle Tests (5 tests)
// ============================================================================

#[actix_web::test]
async fn test_list_running_sessions() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/running",
        web::get().to(agent_api::list_running_claude_sessions),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/sessions/running")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let sessions: Vec<Value> = serde_json::from_slice(&body).expect("Failed to parse response");

    // Should return an array (currently returns empty array in placeholder)
    // Already parsed as Vec<Value>, which is an array
    drop(sessions); // Just verify we can parse it
}

#[actix_web::test]
async fn test_execute_claude_code() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/execute",
        web::post().to(agent_api::execute_claude_code),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/sessions/execute")
        .set_json(json!({
            "project_path": "/tmp",
            "prompt": "Hello, Claude!",
            "session_id": null
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should accept execution request (placeholder implementation)
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Failed to parse response");

    assert!(result["success"].is_boolean());
}

#[actix_web::test]
async fn test_execute_with_session_id() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/execute",
        web::post().to(agent_api::execute_claude_code),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/sessions/execute")
        .set_json(json!({
            "project_path": "/tmp",
            "prompt": "Continue conversation",
            "session_id": "test-session-123"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should accept execution request with session ID
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_cancel_execution() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/cancel",
        web::post().to(agent_api::cancel_claude_execution),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/agent/sessions/cancel")
        .set_json(json!({
            "session_id": "test-session-to-cancel"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should accept cancellation request
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: Value = serde_json::from_slice(&body).expect("Failed to parse response");

    assert!(result["success"].is_boolean());
    assert_eq!(result["success"], true);
}

#[actix_web::test]
async fn test_get_session_jsonl_missing_project_id() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/{session_id}/jsonl",
        web::get().to(agent_api::get_session_jsonl),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/sessions/test-session/jsonl")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return error when project_id query parameter is missing
    assert!(resp.status().is_server_error());
}

#[actix_web::test]
async fn test_get_session_jsonl_nonexistent() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/sessions/{session_id}/jsonl",
        web::get().to(agent_api::get_session_jsonl),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/sessions/nonexistent-session/jsonl?project_id=nonexistent-project")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return error for non-existent session
    assert!(resp.status().is_server_error());
}

// ============================================================================
// Integration Tests (2 tests)
// ============================================================================

#[actix_web::test]
async fn test_full_project_workflow() {
    let state = crate::e2e::common::create_test_app().await;
    let temp_project = create_temp_project();
    let project_path = temp_project.to_string_lossy().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/agent/projects",
                web::post().to(agent_api::create_project),
            )
            .route(
                "/v1/agent/projects",
                web::get().to(agent_api::list_projects),
            )
            .route(
                "/v1/agent/projects/{project_id}/sessions",
                web::get().to(agent_api::get_project_sessions),
            ),
    )
    .await;

    // Step 1: Create project
    let create_req = test::TestRequest::post()
        .uri("/v1/agent/projects")
        .set_json(json!({
            "path": project_path
        }))
        .to_request();

    let create_resp = test::call_service(&app, create_req).await;
    assert!(create_resp.status().is_success());

    let create_body = test::read_body(create_resp).await;
    let project: Value = serde_json::from_slice(&create_body).expect("Failed to parse response");
    let project_id = project["id"].as_str().expect("Project ID should be string");

    // Step 2: List projects (should contain new project)
    let list_req = test::TestRequest::get()
        .uri("/v1/agent/projects")
        .to_request();

    let list_resp = test::call_service(&app, list_req).await;
    assert!(list_resp.status().is_success());

    // Step 3: Get project sessions (should be empty initially)
    let sessions_uri = format!("/v1/agent/projects/{}/sessions", project_id);
    let sessions_req = test::TestRequest::get().uri(&sessions_uri).to_request();

    let sessions_resp = test::call_service(&app, sessions_req).await;
    assert!(sessions_resp.status().is_success());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_project);
}

#[actix_web::test]
async fn test_settings_and_prompt_integration() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/agent/settings",
                web::post().to(agent_api::save_claude_settings),
            )
            .route(
                "/v1/agent/system-prompt",
                web::post().to(agent_api::save_system_prompt),
            )
            .route(
                "/v1/agent/sessions/execute",
                web::post().to(agent_api::execute_claude_code),
            ),
    )
    .await;

    // Save settings
    let settings_req = test::TestRequest::post()
        .uri("/v1/agent/settings")
        .set_json(json!({
            "settings": {
                "model": "claude-3-5-sonnet-20241022",
                "max_tokens": 4096
            }
        }))
        .to_request();

    let settings_resp = test::call_service(&app, settings_req).await;
    assert!(settings_resp.status().is_success());

    // Save system prompt
    let prompt_req = test::TestRequest::post()
        .uri("/v1/agent/system-prompt")
        .set_json(json!({
            "content": "# Integration Test Prompt\n\nBe helpful and concise."
        }))
        .to_request();

    let prompt_resp = test::call_service(&app, prompt_req).await;
    assert!(prompt_resp.status().is_success());

    // Execute with new settings
    let execute_req = test::TestRequest::post()
        .uri("/v1/agent/sessions/execute")
        .set_json(json!({
            "project_path": "/tmp",
            "prompt": "Test execution with new settings"
        }))
        .to_request();

    let execute_resp = test::call_service(&app, execute_req).await;
    assert!(execute_resp.status().is_success());
}
