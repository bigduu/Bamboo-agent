use actix_web::{web, HttpResponse};
use bamboo_config::{
    LifecycleHookCommand, LifecycleHookType, DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
    LIFECYCLE_HOOK_EVENT_NAMES, MAX_LIFECYCLE_HOOK_TIMEOUT_MS, MIN_LIFECYCLE_HOOK_TIMEOUT_MS,
};
use regex::Regex;
use serde::Deserialize;

use crate::{app_state::AppState, error::AppError};

fn default_timeout_ms() -> u64 {
    DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS
}

/// One lifecycle command selected in the settings editor for a dry run.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleHookTestRequest {
    event: String,
    #[serde(default)]
    matcher: Option<String>,
    command: String,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
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
    if payload.command.trim().is_empty() {
        return Err(AppError::BadRequest(
            "lifecycle hook command must not be empty".to_string(),
        ));
    }
    if !(MIN_LIFECYCLE_HOOK_TIMEOUT_MS..=MAX_LIFECYCLE_HOOK_TIMEOUT_MS)
        .contains(&payload.timeout_ms)
    {
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
    let command = LifecycleHookCommand {
        hook_type: LifecycleHookType::Command,
        command: payload.command,
        timeout_ms: payload.timeout_ms,
    };
    let output =
        bamboo_engine::test_lifecycle_shell_command(&payload.event, &command, fallback_cwd)
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
}
