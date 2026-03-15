use actix_web::{web, HttpResponse};

use crate::server::app_state::AppState;
use crate::server::error::AppError;

use super::super::types::ExecuteRequest;
use super::{resolve_anthropic_base_url, resolve_session_ids};
use project_path::validate_project_path;
use request_flags::resolve_execution_flags;
use runner_state::{insert_running_runner, spawn_runner_status_watcher};

mod project_path;
mod request_flags;
mod runner_state;

fn disabled_cli_response() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "success": false,
        "message": "Claude Code CLI not found; integration disabled"
    }))
}

/// Executes Claude Code in a project directory.
pub async fn execute_claude_code(
    state: web::Data<AppState>,
    req: web::Json<ExecuteRequest>,
) -> Result<HttpResponse, AppError> {
    let claude_path = state.claude_cli_path.read().await.clone();
    let Some(claude_path) = claude_path else {
        log::warn!("Claude Code CLI not available; refusing to execute");
        return Ok(disabled_cli_response());
    };

    let project_path = validate_project_path(&req.project_path)?;

    let (client_session_id, claude_session_id, alias_used) =
        resolve_session_ids(req.session_id.clone());

    if alias_used {
        log::warn!(
            "Non-UUID session_id provided ({}); using generated Claude session UUID ({})",
            client_session_id,
            claude_session_id
        );
        let mut aliases = state.claude_session_aliases.write().await;
        aliases.insert(client_session_id.clone(), claude_session_id.clone());
    }

    let (include_partial_messages, dangerously_skip_permissions) = resolve_execution_flags(&req);
    let port = state.config.read().await.server.port;
    let anthropic_base_url = resolve_anthropic_base_url(&req, port);

    let (event_sender, cancel_token) =
        insert_running_runner(state.claude_runners.clone(), &client_session_id).await;

    let run_id = crate::claude::spawn_claude_code_cli(
        state.process_registry.clone(),
        event_sender.clone(),
        cancel_token,
        crate::claude::ClaudeCodeCliConfig {
            claude_path,
            project_path,
            prompt: req.prompt.clone(),
            session_id: claude_session_id.clone(),
            anthropic_base_url,
            json_schema: req.json_schema.clone(),
            skip_permissions: dangerously_skip_permissions,
            include_partial_messages,
        },
    )
    .await
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;

    spawn_runner_status_watcher(
        state.claude_runners.clone(),
        client_session_id.clone(),
        event_sender,
    );

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": client_session_id,
        "claude_session_id": claude_session_id,
        "run_id": run_id,
        "events_url": format!("/v1/agent/sessions/{}/events", client_session_id),
        "message": "Claude Code execution started"
    })))
}

#[cfg(test)]
mod tests {
    use super::disabled_cli_response;

    #[test]
    fn disabled_cli_response_reports_disabled_integration() {
        let response = disabled_cli_response();
        let status = response.status();

        assert_eq!(status, actix_web::http::StatusCode::OK);
    }
}
