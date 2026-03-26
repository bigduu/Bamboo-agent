use actix_web::{web, HttpResponse};

use crate::agent::core::{Role, Session};
use crate::server::app_state::AppState;

const SELECTED_SKILL_IDS_METADATA_KEY: &str = "selected_skill_ids";
const LOADED_SKILL_IDS_METADATA_KEY: &str = "skill_runtime_loaded_skill_ids";
const LAST_LOADED_SKILL_ID_METADATA_KEY: &str = "skill_runtime_last_loaded_skill_id";
const COPILOT_ASK_USER_ENHANCEMENT_METADATA_KEY: &str = "copilot_ask_user_enhancement_enabled";

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
                tracing::error!(
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
    global_default_template: &str,
) -> String {
    // Persist the base system prompt on the session so the frontend does not need to
    // store chat history (or system prompt config) in localStorage.
    //
    // IMPORTANT: The agent loop may mutate the in-session system message by merging
    // in skills/tool guide context. We therefore treat `metadata.base_system_prompt`
    // as the stable "source of truth" for future prompt construction.
    let resolved = base_prompt_from_request
        .map(ToString::to_string)
        .or_else(|| {
            session
                .metadata
                .get("base_system_prompt")
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .or_else(|| {
            session
                .messages
                .iter()
                .find(|message| matches!(message.role, Role::System))
                .map(|message| message.content.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| {
            let trimmed = global_default_template.trim();
            if trimmed.is_empty() {
                crate::server::app_state::DEFAULT_BASE_PROMPT.to_string()
            } else {
                trimmed.to_string()
            }
        });

    session
        .metadata
        .insert("base_system_prompt".to_string(), resolved.clone());
    resolved
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

pub(super) fn resolve_enhance_prompt(
    session: &mut Session,
    enhance_prompt_from_request: Option<&str>,
) -> Option<String> {
    if let Some(prompt) = enhance_prompt_from_request {
        session
            .metadata
            .insert("enhance_prompt".to_string(), prompt.to_string());
    } else {
        session.metadata.remove("enhance_prompt");
    }

    enhance_prompt_from_request.map(ToString::to_string)
}

pub(super) fn resolve_copilot_ask_user_enhancement_enabled(
    session: &mut Session,
    enabled_from_request: Option<bool>,
) -> Option<bool> {
    if let Some(enabled) = enabled_from_request {
        session.metadata.insert(
            COPILOT_ASK_USER_ENHANCEMENT_METADATA_KEY.to_string(),
            enabled.to_string(),
        );
    } else {
        session
            .metadata
            .remove(COPILOT_ASK_USER_ENHANCEMENT_METADATA_KEY);
    }

    enabled_from_request
}

pub(super) fn resolve_selected_skill_ids(
    session: &mut Session,
    selected_skill_ids_from_request: Option<&[String]>,
    message: &str,
) -> Option<Vec<String>> {
    if let Some(request_ids) = selected_skill_ids_from_request {
        let normalized = crate::agent::skill::selection::normalize_selected_skill_ids(
            request_ids.iter().cloned(),
        );
        persist_selected_skill_ids_metadata(session, normalized.as_deref());
        return normalized;
    }

    let from_hint = crate::agent::skill::selection::normalize_selected_skill_ids(
        extract_skill_ids_from_hint(message),
    );
    if let Some(ids) = from_hint.as_ref() {
        persist_selected_skill_ids_metadata(session, Some(ids));
        return from_hint;
    }

    // Explicit per-message selection behavior: if this request does not specify
    // or hint any selected skill, clear stale session metadata from prior turns.
    session.metadata.remove(SELECTED_SKILL_IDS_METADATA_KEY);
    None
}

pub(super) fn clear_skill_runtime_state(session: &mut Session) {
    session.metadata.remove(LOADED_SKILL_IDS_METADATA_KEY);
    session.metadata.remove(LAST_LOADED_SKILL_ID_METADATA_KEY);
}

fn persist_selected_skill_ids_metadata(
    session: &mut Session,
    selected_skill_ids: Option<&[String]>,
) {
    match selected_skill_ids {
        Some(ids) if !ids.is_empty() => {
            if let Ok(serialized) = serde_json::to_string(ids) {
                session
                    .metadata
                    .insert(SELECTED_SKILL_IDS_METADATA_KEY.to_string(), serialized);
            } else {
                tracing::warn!("Failed to serialize selected skill IDs; clearing metadata");
                session.metadata.remove(SELECTED_SKILL_IDS_METADATA_KEY);
            }
        }
        _ => {
            session.metadata.remove(SELECTED_SKILL_IDS_METADATA_KEY);
        }
    }
}

fn extract_skill_ids_from_hint(message: &str) -> Vec<String> {
    const HINT_PREFIX: &str = "[User explicitly selected skill:";
    let mut extracted = Vec::new();

    for line in message.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with(HINT_PREFIX) || !trimmed.ends_with(']') {
            continue;
        }

        // Expected format:
        // [User explicitly selected skill: <display label> (ID: <skill-id>)]
        let Some(id_marker_index) = trimmed.rfind("(ID:") else {
            continue;
        };
        let id_segment = &trimmed[id_marker_index + "(ID:".len()..];
        let Some(close_paren_index) = id_segment.find(')') else {
            continue;
        };
        let id = id_segment[..close_paren_index].trim();
        if !id.is_empty() {
            extracted.push(id.to_string());
        }
    }

    extracted
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
        tracing::error!("[{}] Failed to save session: {}", session_id, error);
        return Err(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to save session: {}", error)
        })));
    }

    Ok(())
}
