use actix_web::{web, HttpResponse};

use crate::agent::core::Session;
use crate::server::app_state::AppState;

pub(super) async fn load_or_create_session(
    state: &web::Data<AppState>,
    session_id: &str,
    model: &str,
) -> Result<Session, HttpResponse> {
    let existing_session = {
        let sessions = state.sessions.read().await;
        sessions.get(session_id).cloned()
    };

    match existing_session {
        Some(session) => Ok(session),
        None => match state.storage.load_session(session_id).await {
            Ok(Some(session)) => Ok(session),
            Ok(None) => Ok(Session::new(session_id.to_string(), model.to_string())),
            Err(error) => {
                log::error!(
                    "[{}] Failed to load session from storage: {}",
                    session_id,
                    error
                );
                Err(HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to load session: {}", error)
                })))
            }
        },
    }
}

pub(super) fn resolve_base_prompt(
    session: &mut Session,
    base_prompt_from_request: Option<&str>,
) -> String {
    // Persist the base system prompt on the session so the frontend does not need to
    // store chat history (or system prompt config) in localStorage.
    //
    // IMPORTANT: The agent loop may mutate the in-session system message by merging
    // in skills/tool guide context. We therefore treat `metadata.base_system_prompt`
    // as the stable "source of truth" for future prompt construction.
    if let Some(prompt) = base_prompt_from_request {
        session
            .metadata
            .insert("base_system_prompt".to_string(), prompt.to_string());
    }

    base_prompt_from_request
        .map(ToString::to_string)
        .or_else(|| session.metadata.get("base_system_prompt").cloned())
        .unwrap_or_else(|| crate::server::app_state::DEFAULT_BASE_PROMPT.to_string())
}

pub(super) fn resolve_workspace_path(
    session: &mut Session,
    workspace_path_from_request: Option<&str>,
) -> Option<String> {
    if let Some(path) = workspace_path_from_request {
        session
            .metadata
            .insert("workspace_path".to_string(), path.to_string());
    }

    workspace_path_from_request
        .map(ToString::to_string)
        .or_else(|| session.metadata.get("workspace_path").cloned())
}

pub(super) async fn cache_and_save_session(
    state: &web::Data<AppState>,
    session_id: &str,
    session: Session,
) -> Result<(), HttpResponse> {
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.to_string(), session.clone());
    }

    if let Err(error) = state.storage.save_session(&session).await {
        log::error!("[{}] Failed to save session: {}", session_id, error);
        return Err(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to save session: {}", error)
        })));
    }

    Ok(())
}
