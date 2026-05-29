use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use crate::model_config_helper::normalize_gold_config_json;
use crate::session_app::metadata::{MetadataError, SessionMetadataService};
use crate::session_app::provider_model::{
    derive_model_ref, persist_legacy_model_provider, persist_model_ref,
};

use super::super::super::types::PatchSessionRequest;
use super::query::get_session;

/// `PATCH /api/v1/sessions/{session_id}`
///
/// Title and pinned are routed through [`SessionMetadataService`] so they go
/// through the canonical pipeline (load → re-check → bump version
/// → locked save → cache → publish_replayable_session_event). Non-metadata
/// fields (`model_ref`, `reasoning_effort`) are written via the locked
/// metadata-merge path to avoid clobbering concurrent UI edits.
pub async fn patch_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<PatchSessionRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    if let Some(title) = req.title.as_ref() {
        match SessionMetadataService::set_title(state.get_ref(), &session_id, title).await {
            Ok(_) => {}
            Err(MetadataError::NotFound(id)) => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": id
                })));
            }
            Err(err) => {
                return Err(actix_web::error::ErrorInternalServerError(err.to_string()));
            }
        }
    }

    if let Some(pinned) = req.pinned {
        match SessionMetadataService::set_pinned(state.get_ref(), &session_id, pinned).await {
            Ok(_) => {}
            Err(MetadataError::NotFound(id)) => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": id
                })));
            }
            Err(err) => {
                return Err(actix_web::error::ErrorInternalServerError(err.to_string()));
            }
        }
    }

    if req.gold_config.is_some() {
        let gold_config_json = match req
            .gold_config
            .as_ref()
            .map(normalize_gold_config_json)
            .transpose()
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "Invalid gold_config",
                    "message": error.to_string()
                })));
            }
        };
        match SessionMetadataService::set_gold_config_json(
            state.get_ref(),
            &session_id,
            gold_config_json,
        )
        .await
        {
            Ok(_) => {}
            Err(MetadataError::NotFound(id)) => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": id
                })));
            }
            Err(err) => {
                return Err(actix_web::error::ErrorInternalServerError(err.to_string()));
            }
        }
    }

    let touches_non_metadata = req.model_ref.is_some()
        || req.provider.is_some()
        || req.model.is_some()
        || req.reasoning_effort.is_some()
        || req.clear_reasoning_effort.unwrap_or(false);

    if touches_non_metadata {
        let Some(mut session) = state
            .storage
            .load_session(&session_id)
            .await
            .map_err(|error| {
                actix_web::error::ErrorInternalServerError(format!(
                    "Failed to load session: {error}"
                ))
            })?
        else {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": "Session not found",
                "session_id": session_id
            })));
        };

        let request_model_ref = derive_model_ref(
            req.model_ref.as_ref(),
            req.provider.as_deref(),
            req.model.as_deref(),
        );
        if let Some(model_ref) = request_model_ref.as_ref() {
            persist_model_ref(&mut session, model_ref);
        } else {
            persist_legacy_model_provider(
                &mut session,
                req.model.as_deref(),
                req.provider.as_deref(),
            );
        }
        if req.clear_reasoning_effort.unwrap_or(false) {
            session.reasoning_effort = None;
        } else if let Some(reasoning_effort) = req.reasoning_effort {
            session.reasoning_effort = Some(reasoning_effort);
        }
        session.updated_at = chrono::Utc::now();

        state
            .persistence
            .merge_save_runtime(&mut session)
            .await
            .map_err(|error| {
                actix_web::error::ErrorInternalServerError(format!(
                    "Failed to save session: {error}"
                ))
            })?;

        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    get_session(state, web::Path::from(session_id)).await
}
