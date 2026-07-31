use actix_web::{web, HttpResponse};
use bamboo_config::{
    lifecycle_script_extension, LifecycleHookHandler, LifecycleScriptRunner,
    DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS, LIFECYCLE_HOOK_EVENT_NAMES, MAX_LIFECYCLE_HOOK_TIMEOUT_MS,
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
    Script,
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
    path: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    runner: Option<LifecycleScriptRunner>,
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
    let timeout_ms = payload
        .timeout_ms
        .unwrap_or(DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS);
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
        LifecycleHookTestType::Script => {
            let path = payload.path.unwrap_or_default();
            let path = path.trim();
            if path.is_empty() {
                return Err(AppError::BadRequest(
                    "lifecycle script path must not be empty".to_string(),
                ));
            }
            if lifecycle_script_extension(path).is_none() {
                return Err(AppError::BadRequest(
                    "lifecycle script path must end in .js, .mjs, .cjs, .py, .sh, .ps1, .bat, or .cmd"
                        .to_string(),
                ));
            }
            let runner = payload.runner.unwrap_or_default();
            if !runner.supports_path(path) {
                return Err(AppError::BadRequest(format!(
                    "lifecycle script runner '{}' is incompatible with path '{path}'",
                    runner.as_str()
                )));
            }
            LifecycleHookHandler::script(path, runner, timeout_ms)
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
    async fn dry_run_executes_external_script_handler() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("hook.js"),
            r#"
let raw = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => raw += chunk);
process.stdin.on("end", () => {
  const input = JSON.parse(raw);
  process.stdout.write(JSON.stringify({additional_context: input.tool_name}));
});
"#,
        )
        .unwrap();
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
                    "type": "script",
                    "path": "hook.js",
                    "runner": "node"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["exit_code"], 0);
        assert_eq!(body["timed_out"], false);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body["stdout"].as_str().unwrap()).unwrap()
                ["additional_context"],
            "Bash"
        );
    }
}
