use actix_web::{web, HttpResponse, Result};

use super::session::load_session_from_memory_or_storage;
use super::types::to_todo_list_response;
use crate::server::app_state::AppState;

/// Get todo list for a session.
pub async fn get_todo_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let Some(session) = load_session_from_memory_or_storage(&state, &session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found"
        })));
    };

    let Some(todo_list) = session.todo_list.as_ref() else {
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "session_id": session.id,
            "title": null,
            "items": [],
            "progress": {
                "completed": 0,
                "total": 0,
                "percentage": 0
            }
        })));
    };

    Ok(HttpResponse::Ok().json(to_todo_list_response(todo_list)))
}

/// Check if a session has a todo list.
pub async fn has_todo_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let Some(session) = load_session_from_memory_or_storage(&state, &session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found"
        })));
    };

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "has_todo_list": session.todo_list.is_some(),
        "session_id": session.id
    })))
}
