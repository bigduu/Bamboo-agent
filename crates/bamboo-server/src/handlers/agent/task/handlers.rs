use actix_web::{web, HttpResponse, Result};

use super::types::to_task_list_response;
use bamboo_application_agent::SessionKind;
use crate::app_state::AppState;

/// Get task list for a session.
pub async fn get_task_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let Some(session) = state.load_session(&session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found"
        })));
    };

    let shared_session = if session.kind == SessionKind::Child {
        state.load_session(&session.root_session_id)
            .await
            .unwrap_or_else(|| session.clone())
    } else {
        session.clone()
    };

    let Some(task_list) = shared_session.task_list.as_ref() else {
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "session_id": shared_session.id,
            "title": null,
            "items": [],
            "progress": {
                "completed": 0,
                "total": 0,
                "percentage": 0
            }
        })));
    };

    Ok(HttpResponse::Ok().json(to_task_list_response(task_list)))
}

/// Check if a session has a task list.
pub async fn has_task_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let Some(session) = state.load_session(&session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found"
        })));
    };

    let shared_session = if session.kind == SessionKind::Child {
        state.load_session(&session.root_session_id)
            .await
            .unwrap_or_else(|| session.clone())
    } else {
        session.clone()
    };

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "has_task_list": shared_session.task_list.is_some(),
        "session_id": shared_session.id
    })))
}
