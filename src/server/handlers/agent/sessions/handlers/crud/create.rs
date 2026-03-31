use actix_web::{web, HttpResponse, Result};
use uuid::Uuid;

use crate::agent::core::{Message, Session};
use crate::core::ReasoningEffort;
use crate::server::app_state::AppState;

use super::super::super::types::{CreateSessionRequest, CreateSessionResponse, SessionSummary};

/// `POST /api/v1/sessions`
pub async fn create_session(
    state: web::Data<AppState>,
    req: web::Json<CreateSessionRequest>,
) -> Result<HttpResponse> {
    let id = Uuid::new_v4().to_string();
    let global_default_prompt =
        crate::server::prompt_defaults::read_global_default_system_prompt_template();
    let config_snapshot = state.config.read().await.clone();
    let session = build_new_session(&id, &req, global_default_prompt.as_str(), &config_snapshot);

    state
        .storage
        .save_session(&session)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to save session: {error}"))
        })?;

    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(id.clone(), session.clone());
    }

    match state.session_store.get_index_entry(&id).await {
        Some(entry) => Ok(HttpResponse::Ok().json(CreateSessionResponse {
            session: SessionSummary::from_entry(entry, false),
        })),
        None => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Session created but missing from index",
            "session_id": id
        }))),
    }
}

pub(super) fn build_new_session(
    id: &str,
    req: &CreateSessionRequest,
    global_default_prompt: &str,
    config: &crate::core::Config,
) -> Session {
    let model = model_from_request(req, config);
    let mut session = Session::new(id.to_string(), model);
    session.reasoning_effort = reasoning_effort_from_request(req, config);

    if let Some(title) = trimmed_non_empty_owned(req.title.as_deref()) {
        session.title = title;
    }
    let explicit_prompt = trimmed_non_empty_owned(req.system_prompt.as_deref());
    let has_explicit_prompt = explicit_prompt.is_some();
    let base_prompt = explicit_prompt.unwrap_or_else(|| {
        let trimmed = global_default_prompt.trim();
        if trimmed.is_empty() {
            crate::server::app_state::DEFAULT_BASE_PROMPT.to_string()
        } else {
            trimmed.to_string()
        }
    });
    session
        .metadata
        .insert("base_system_prompt".to_string(), base_prompt.clone());

    if has_explicit_prompt {
        session.add_message(Message::system(base_prompt));
    }

    session
}

pub(super) fn model_from_request(req: &CreateSessionRequest, config: &crate::core::Config) -> String {
    trimmed_non_empty_owned(req.model.as_deref())
        .or_else(|| config.get_model())
        .unwrap_or_else(|| "unknown".to_string())
}

pub(super) fn reasoning_effort_from_request(
    req: &CreateSessionRequest,
    config: &crate::core::Config,
) -> Option<ReasoningEffort> {
    req.reasoning_effort.or_else(|| config.get_reasoning_effort())
}

fn trimmed_non_empty_owned(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
