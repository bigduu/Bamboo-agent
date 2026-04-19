use crate::{app_state::AppState, error::AppError};
use actix_web::{body::to_bytes, http::StatusCode, web};
use bamboo_tools::BuiltinToolExecutor;

use super::tools::{resolve_session_identifier, select_tools_by_allowlist, to_openai_tools};
use super::types::FilteredToolsQuery;

#[test]
fn resolve_session_identifier_prefers_session_id() {
    let query = FilteredToolsQuery {
        session_id: Some("session-123".to_string()),
        chat_id: Some("chat-123".to_string()),
    };

    assert_eq!(resolve_session_identifier(&query), Some("session-123"));
}

#[test]
fn resolve_session_identifier_falls_back_to_chat_id() {
    let query = FilteredToolsQuery {
        session_id: None,
        chat_id: Some("chat-456".to_string()),
    };

    assert_eq!(resolve_session_identifier(&query), Some("chat-456"));
}

#[test]
fn select_tools_by_allowlist_returns_all_when_allowlist_is_empty() {
    let all_tools = BuiltinToolExecutor::tool_schemas();
    let expected_len = all_tools.len();

    let selected = select_tools_by_allowlist(all_tools, &[]);
    assert_eq!(selected.len(), expected_len);
}

#[test]
fn select_tools_by_allowlist_filters_to_exact_tool_names() {
    let all_tools = BuiltinToolExecutor::tool_schemas();
    let allowed_tool = all_tools
        .first()
        .expect("Builtin tools should not be empty")
        .function
        .name
        .clone();
    let allowlist = vec![allowed_tool.clone()];

    let selected = select_tools_by_allowlist(all_tools, &allowlist);
    assert!(!selected.is_empty());
    assert!(selected
        .iter()
        .all(|tool| tool.function.name == allowed_tool));
}

#[test]
fn to_openai_tools_maps_function_fields() {
    let mut all_tools = BuiltinToolExecutor::tool_schemas();
    let first = all_tools
        .drain(..1)
        .next()
        .expect("Builtin tools should not be empty");
    let expected_name = first.function.name.clone();
    let expected_description = first.function.description.clone();

    let mapped = to_openai_tools(vec![first]);
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0].tool_type, "function");
    assert_eq!(mapped[0].function.name, expected_name);
    assert_eq!(mapped[0].function.description, expected_description);
}

#[actix_web::test]
async fn skill_config_registers_expected_routes() {
    let app = actix_web::test::init_service(actix_web::App::new().configure(super::config)).await;

    for uri in [
        "/skills",
        "/skills/test-id",
        "/skills/available-tools",
        "/skills/filtered-tools",
        "/skills/available-workflows",
    ] {
        let req = actix_web::test::TestRequest::get().uri(uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected skill route to be registered: {uri}"
        );
    }
}

#[tokio::test]
async fn get_available_workflows_returns_sorted_workflow_names() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let app_state = web::Data::new(
        AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize"),
    );
    let workflows_dir = app_state.app_data_dir.join("workflows");

    tokio::fs::create_dir_all(&workflows_dir)
        .await
        .expect("create workflows dir");
    tokio::fs::write(workflows_dir.join("zeta.yaml"), "# zeta")
        .await
        .expect("write zeta workflow");
    tokio::fs::write(workflows_dir.join("alpha.md"), "# alpha")
        .await
        .expect("write alpha workflow");

    let response = super::get_available_workflows(app_state)
        .await
        .expect("handler should return response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body())
        .await
        .expect("serialize response body");
    let payload: serde_json::Value =
        serde_json::from_slice(&body).expect("deserialize workflow response");

    assert_eq!(payload["workflows"], serde_json::json!(["alpha", "zeta"]));
}

#[tokio::test]
async fn get_available_workflows_maps_listing_failures_to_internal_error() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let app_state = web::Data::new(
        AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize"),
    );
    let workflows_path = app_state.app_data_dir.join("workflows");

    if workflows_path.exists() {
        tokio::fs::remove_dir_all(&workflows_path)
            .await
            .expect("remove workflows dir");
    }
    tokio::fs::write(&workflows_path, "not a directory")
        .await
        .expect("write workflows file");

    let error = super::get_available_workflows(app_state)
        .await
        .expect_err("handler should return error when workflows path is invalid");

    match error {
        AppError::InternalError(inner) => assert!(
            inner.to_string().contains("Failed to list workflows"),
            "unexpected error message: {inner}"
        ),
        other => panic!("expected internal error, got {other:?}"),
    }
}
