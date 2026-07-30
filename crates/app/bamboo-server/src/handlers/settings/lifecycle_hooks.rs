use actix_web::{web, HttpResponse};
use bamboo_config::{
    LifecycleHookHandler, DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
    DEFAULT_JAVASCRIPT_HOOK_TIMEOUT_MS, DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
    LIFECYCLE_HOOK_EVENT_NAMES, MAX_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
    MAX_LIFECYCLE_HOOK_TIMEOUT_MS, MIN_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES,
    MIN_LIFECYCLE_HOOK_TIMEOUT_MS,
};
use regex::Regex;
use serde::Deserialize;

use crate::{app_state::AppState, error::AppError};

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum LifecycleHookTestType {
    #[default]
    Command,
    JavaScript,
}

/// One lifecycle handler selected in the settings editor for a dry run.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleHookTestRequest {
    event: String,
    #[serde(default)]
    matcher: Option<String>,
    #[serde(rename = "type", default)]
    hook_type: LifecycleHookTestType,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    memory_limit_bytes: Option<usize>,
}

/// Execute one command against Bamboo's deterministic synthetic lifecycle
/// payload and return raw output. The route is mounted inside the same access-
/// password middleware as config writes; it deliberately never persists the
/// submitted command.
pub async fn test_lifecycle_hook(
    app_state: web::Data<AppState>,
    payload: web::Json<LifecycleHookTestRequest>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    if !LIFECYCLE_HOOK_EVENT_NAMES.contains(&payload.event.as_str()) {
        return Err(AppError::BadRequest(format!(
            "unknown lifecycle hook event '{}'",
            payload.event
        )));
    }
    let timeout_ms = payload.timeout_ms.unwrap_or(match payload.hook_type {
        LifecycleHookTestType::Command => DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
        LifecycleHookTestType::JavaScript => DEFAULT_JAVASCRIPT_HOOK_TIMEOUT_MS,
    });
    if !(MIN_LIFECYCLE_HOOK_TIMEOUT_MS..=MAX_LIFECYCLE_HOOK_TIMEOUT_MS).contains(&timeout_ms) {
        return Err(AppError::BadRequest(format!(
            "timeout_ms must be between {MIN_LIFECYCLE_HOOK_TIMEOUT_MS} and {MAX_LIFECYCLE_HOOK_TIMEOUT_MS}"
        )));
    }
    if let Some(matcher) = payload.matcher.as_deref() {
        Regex::new(matcher).map_err(|error| {
            AppError::BadRequest(format!("invalid lifecycle hook matcher regex: {error}"))
        })?;
    }

    let fallback_cwd = app_state
        .config
        .read()
        .await
        .get_default_work_area_path()
        .or_else(|| Some(app_state.app_data_dir.clone()));
    let handler = match payload.hook_type {
        LifecycleHookTestType::Command => {
            let command = payload.command.unwrap_or_default();
            if command.trim().is_empty() {
                return Err(AppError::BadRequest(
                    "lifecycle hook command must not be empty".to_string(),
                ));
            }
            LifecycleHookHandler::command(command, timeout_ms)
        }
        LifecycleHookTestType::JavaScript => {
            let source = payload.source.unwrap_or_default();
            if source.trim().is_empty() {
                return Err(AppError::BadRequest(
                    "JavaScript lifecycle hook source must not be empty".to_string(),
                ));
            }
            let memory_limit_bytes = payload
                .memory_limit_bytes
                .unwrap_or(DEFAULT_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES);
            if !(MIN_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES..=MAX_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES)
                .contains(&memory_limit_bytes)
            {
                return Err(AppError::BadRequest(format!(
                    "memory_limit_bytes must be between {MIN_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES} and {MAX_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES}"
                )));
            }
            LifecycleHookHandler::javascript(source, timeout_ms, memory_limit_bytes)
        }
    };
    let output = bamboo_engine::test_lifecycle_handler(&payload.event, &handler, fallback_cwd)
        .await
        .map_err(|error| {
            AppError::InternalError(anyhow::anyhow!("lifecycle hook dry run failed: {error}"))
        })?;

    Ok(HttpResponse::Ok().json(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};

    #[actix_web::test]
    async fn dry_run_returns_raw_exit_and_captured_streams() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/hooks/test", web::post().to(test_lifecycle_hook)),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/hooks/test")
                .set_json(serde_json::json!({
                    "event": "PreToolUse",
                    "matcher": "^Bash$",
                    "command": "printf '%s' \"$BAMBOO_HOOK_EVENT\"; printf 'diagnostic' >&2; exit 7",
                    "timeout_ms": 2_000
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["exit_code"], 7);
        assert_eq!(body["stdout"], "PreToolUse");
        assert_eq!(body["stderr"], "diagnostic");
        assert_eq!(body["timed_out"], false);
    }

    #[actix_web::test]
    async fn dry_run_rejects_unknown_event_before_execution() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/hooks/test", web::post().to(test_lifecycle_hook)),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/hooks/test")
                .set_json(serde_json::json!({
                    "event": "Unknown",
                    "command": "exit 99"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn dry_run_executes_javascript_handler() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/hooks/test", web::post().to(test_lifecycle_hook)),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/hooks/test")
                .set_json(serde_json::json!({
                    "event": "PreToolUse",
                    "type": "javascript",
                    "source": "function hook(input) { return { additional_context: input.tool_name }; }"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["exit_code"], serde_json::Value::Null);
        assert_eq!(body["timed_out"], false);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body["stdout"].as_str().unwrap()).unwrap()
                ["additional_context"],
            "Bash"
        );
    }
}
