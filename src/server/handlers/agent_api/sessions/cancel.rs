use std::collections::HashMap;

use actix_web::{web, HttpResponse};
use uuid::Uuid;

use crate::server::app_state::AppState;
use crate::server::error::AppError;

use super::super::types::CancelRequest;

fn resolve_claude_session_id(
    session_id: &str,
    aliases: Option<&HashMap<String, String>>,
) -> Option<String> {
    if Uuid::parse_str(session_id).is_ok() {
        return Some(session_id.to_string());
    }

    aliases.and_then(|mapping| mapping.get(session_id).cloned())
}

/// Cancels a running Claude Code execution.
pub async fn cancel_claude_execution(
    state: web::Data<AppState>,
    req: web::Json<CancelRequest>,
) -> Result<HttpResponse, AppError> {
    let session_id = req.session_id.trim().to_string();
    if session_id.is_empty() {
        return Err(AppError::BadRequest("session_id is required".to_string()));
    }

    {
        let runners = state.claude_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            runner.cancel_token.cancel();
        }
    }

    let claude_session_id = if Uuid::parse_str(&session_id).is_ok() {
        Some(session_id.clone())
    } else {
        let aliases = state.claude_session_aliases.read().await;
        resolve_claude_session_id(&session_id, Some(&aliases))
    };

    let run_id = if let Some(ref claude_session_id) = claude_session_id {
        state
            .process_registry
            .get_claude_session_by_id(claude_session_id)
            .await
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
            .map(|info| info.run_id)
    } else {
        None
    };

    if let Some(run_id) = run_id {
        let _ = state
            .process_registry
            .kill_process(run_id)
            .await
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;

        Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "message": "Cancellation request sent",
            "session_id": session_id,
            "claude_session_id": claude_session_id,
            "run_id": run_id
        })))
    } else {
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "message": "Session not found or not running",
            "session_id": session_id,
            "claude_session_id": claude_session_id
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::resolve_claude_session_id;

    #[test]
    fn resolve_claude_session_id_keeps_uuid_input() {
        let session_id = Uuid::new_v4().to_string();
        let resolved = resolve_claude_session_id(&session_id, None);
        assert_eq!(resolved, Some(session_id));
    }

    #[test]
    fn resolve_claude_session_id_uses_alias_mapping_for_non_uuid() {
        let mut aliases = HashMap::new();
        aliases.insert("friendly-name".to_string(), Uuid::new_v4().to_string());

        let resolved = resolve_claude_session_id("friendly-name", Some(&aliases));
        assert_eq!(resolved, aliases.get("friendly-name").cloned());
    }

    #[test]
    fn resolve_claude_session_id_returns_none_for_unknown_alias() {
        let aliases = HashMap::new();
        let resolved = resolve_claude_session_id("missing-alias", Some(&aliases));
        assert_eq!(resolved, None);
    }
}
