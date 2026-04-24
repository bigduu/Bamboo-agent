use actix_web::{web, HttpResponse, Result};
use uuid::Uuid;

use crate::app_state::AppState;
use bamboo_agent_core::Session;

use super::super::super::types::{CreateSessionRequest, CreateSessionResponse, SessionSummary};

/// `POST /api/v1/sessions`
pub async fn create_session(
    state: web::Data<AppState>,
    req: web::Json<CreateSessionRequest>,
) -> Result<HttpResponse> {
    let id = Uuid::new_v4().to_string();
    let global_default_prompt =
        crate::prompt_defaults::read_global_default_system_prompt_template();
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

fn build_new_session(
    id: &str,
    req: &CreateSessionRequest,
    global_default_prompt: &str,
    config: &bamboo_infrastructure::Config,
) -> Session {
    use crate::session_app::session_create::{
        build_new_session as crate_build, CreateSessionConfig, CreateSessionInput,
    };

    let input = CreateSessionInput {
        id: id.to_string(),
        title: req.title.clone(),
        system_prompt: req.system_prompt.clone(),
        model: req.model.clone(),
        model_ref: req.model_ref.clone(),
        reasoning_effort: req.reasoning_effort,
    };
    let create_config = CreateSessionConfig {
        default_model: config.get_model(),
        default_reasoning_effort: config.get_reasoning_effort(),
        global_default_prompt: global_default_prompt.to_string(),
        builtin_fallback_prompt: crate::app_state::DEFAULT_BASE_PROMPT,
    };

    crate_build(&input, &create_config)
}
