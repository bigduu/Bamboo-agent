use actix_web::{web, HttpResponse, Result};
use serde::Serialize;

use crate::state::AppState;

/// Todo item response for frontend
#[derive(Serialize)]
pub struct TodoItemResponse {
    pub id: String,
    pub description: String,
    pub status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub notes: String,
}

/// Todo list response for frontend
#[derive(Serialize)]
pub struct TodoListResponse {
    pub session_id: String,
    pub title: String,
    pub items: Vec<TodoItemResponse>,
    pub progress: TodoProgress,
}

/// Progress information
#[derive(Serialize)]
pub struct TodoProgress {
    pub completed: usize,
    pub total: usize,
    pub percentage: u8,
}

/// Get todo list for a session
pub async fn get_todo_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();

    // Try to get session from memory first, then from storage
    let session = {
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    let session = match session {
        Some(s) => s,
        None => match state.storage.load_session(&session_id).await {
            Ok(Some(session)) => {
                // Load into memory for future requests
                let mut sessions = state.sessions.write().await;
                sessions.insert(session_id.clone(), session.clone());
                session
            }
            _ => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found"
                })));
            }
        }
    };

    let todo_list = match &session.todo_list {
        Some(tl) => tl,
        None => {
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
        }
    };

    let items: Vec<TodoItemResponse> = todo_list
        .items
        .iter()
        .map(|item| TodoItemResponse {
            id: item.id.clone(),
            description: item.description.clone(),
            status: match item.status {
                agent_core::TodoItemStatus::Pending => "pending".to_string(),
                agent_core::TodoItemStatus::InProgress => "in_progress".to_string(),
                agent_core::TodoItemStatus::Completed => "completed".to_string(),
                agent_core::TodoItemStatus::Blocked => "blocked".to_string(),
            },
            depends_on: item.depends_on.clone(),
            notes: item.notes.clone(),
        })
        .collect();

    let completed = items
        .iter()
        .filter(|i| i.status == "completed")
        .count();
    let total = items.len();
    let percentage = if total > 0 {
        ((completed as f32 / total as f32) * 100.0) as u8
    } else {
        0
    };

    let response = TodoListResponse {
        session_id: todo_list.session_id.clone(),
        title: todo_list.title.clone(),
        items,
        progress: TodoProgress {
            completed,
            total,
            percentage,
        },
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Check if a session has a todo list
pub async fn has_todo_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();

    // Try to get session from memory first, then from storage
    let session = {
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    let session = match session {
        Some(s) => s,
        None => match state.storage.load_session(&session_id).await {
            Ok(Some(session)) => {
                // Load into memory for future requests
                let mut sessions = state.sessions.write().await;
                sessions.insert(session_id.clone(), session.clone());
                session
            }
            _ => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found"
                })));
            }
        }
    };

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "has_todo_list": session.todo_list.is_some(),
        "session_id": session.id
    })))
}
