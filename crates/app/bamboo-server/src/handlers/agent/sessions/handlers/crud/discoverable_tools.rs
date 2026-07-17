use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

use super::super::super::types::{ActivateDiscoverableToolsRequest, DiscoverableToolsResponse};

/// `GET /api/v1/sessions/{session_id}/discoverable-tools`
///
/// List all discoverable tools and indicate which ones are currently
/// activated for the session.
pub async fn list_discoverable_tools(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let session = match state.storage.load_session(&session_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": session_id
            })));
        }
        Err(error) => {
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value(format!("Failed to load session: {error}"))
            })));
        }
    };

    let all_tools = bamboo_tools::exposure::list_discoverable_tools();
    let activated = bamboo_tools::exposure::activated_discoverable_tools(&session);

    let tools: Vec<_> = all_tools
        .into_iter()
        .map(|name| {
            serde_json::json!({
                "name": name,
                "activated": activated.contains(name),
            })
        })
        .collect();

    Ok(HttpResponse::Ok().json(DiscoverableToolsResponse {
        session_id,
        tools,
        activated: activated.into_iter().collect(),
    }))
}

/// `POST /api/v1/sessions/{session_id}/discoverable-tools`
///
/// Activate discoverable tools for the session.
pub async fn activate_discoverable_tools(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<ActivateDiscoverableToolsRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let Some(mut session) = state
        .storage
        .load_session(&session_id)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to load session: {error}"))
        })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    };

    bamboo_tools::exposure::activate_discoverable_tools(&mut session, &req.tools);
    session.updated_at = chrono::Utc::now();

    state
        .persistence
        .merge_save_runtime(&mut session)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to save session: {error}"))
        })?;

    state.sessions.insert(
        session_id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
    );

    let activated = bamboo_tools::exposure::activated_discoverable_tools(&session);
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "session_id": session_id,
        "activated": activated,
    })))
}

/// `DELETE /api/v1/sessions/{session_id}/discoverable-tools`
///
/// Deactivate discoverable tools for the session.
pub async fn deactivate_discoverable_tools(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<ActivateDiscoverableToolsRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let Some(mut session) = state
        .storage
        .load_session(&session_id)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to load session: {error}"))
        })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    };

    bamboo_tools::exposure::deactivate_discoverable_tools(&mut session, &req.tools);
    session.updated_at = chrono::Utc::now();

    state
        .persistence
        .merge_save_runtime(&mut session)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to save session: {error}"))
        })?;

    state.sessions.insert(
        session_id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
    );

    let activated = bamboo_tools::exposure::activated_discoverable_tools(&session);
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "session_id": session_id,
        "activated": activated,
    })))
}
