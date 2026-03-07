//! Session management endpoints (V2 index-backed).

use actix_web::{web, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

use crate::agent::core::storage::{CleanupMode, CleanupResult, SessionIndexEntry};
use crate::agent::core::{Message, Session};
use crate::server::app_state::AgentStatus;
use crate::server::app_state::AppState;

#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub id: String,
    pub kind: crate::agent::core::SessionKind,
    pub title: String,
    pub pinned: bool,
    pub parent_session_id: Option<String>,
    pub root_session_id: String,
    pub spawn_depth: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_schedule_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_activity_at: chrono::DateTime<chrono::Utc>,
    pub message_count: usize,
    pub has_attachments: bool,
    pub is_running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<crate::agent::core::TokenBudgetUsage>,
}

impl SessionSummary {
    pub(crate) fn from_entry(entry: SessionIndexEntry, is_running: bool) -> Self {
        Self {
            id: entry.id,
            kind: entry.kind,
            title: entry.title,
            pinned: entry.pinned,
            parent_session_id: entry.parent_session_id,
            root_session_id: entry.root_session_id,
            spawn_depth: entry.spawn_depth,
            created_by_schedule_id: entry.created_by_schedule_id,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            last_activity_at: entry.last_activity_at,
            message_count: entry.message_count,
            has_attachments: entry.has_attachments,
            is_running,
            token_usage: entry.token_usage,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ListSessionsResponse {
    pub sessions: Vec<SessionSummary>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateSessionResponse {
    pub session: SessionSummary,
}

/// `POST /api/v1/sessions`
pub async fn create_session(
    state: web::Data<AppState>,
    req: web::Json<CreateSessionRequest>,
) -> Result<HttpResponse> {
    let id = Uuid::new_v4().to_string();
    let model = req
        .model
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("unknown")
        .to_string();

    let mut session = Session::new(id.clone(), model);
    if let Some(title) = req
        .title
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        session.title = title.to_string();
    }
    if let Some(prompt) = req
        .system_prompt
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        session
            .metadata
            .insert("base_system_prompt".to_string(), prompt.to_string());
        session.add_message(Message::system(prompt.to_string()));
    }

    state.storage.save_session(&session).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to save session: {e}"))
    })?;

    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(id.clone(), session);
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

/// `GET /api/v1/sessions`
pub async fn list_sessions(state: web::Data<AppState>) -> Result<HttpResponse> {
    let running: HashSet<String> = {
        let runners = state.agent_runners.read().await;
        runners
            .iter()
            .filter_map(|(sid, runner)| {
                if matches!(runner.status, AgentStatus::Running) {
                    Some(sid.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    let entries = state.session_store.list_index_entries().await;
    Ok(HttpResponse::Ok().json(ListSessionsResponse {
        sessions: entries
            .into_iter()
            .map(|e| {
                let is_running = running.contains(&e.id);
                SessionSummary::from_entry(e, is_running)
            })
            .collect(),
    }))
}

#[derive(Debug, Serialize)]
pub struct GetSessionResponse {
    pub session: SessionSummary,
}

/// `GET /api/v1/sessions/{session_id}`
pub async fn get_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    match state.session_store.get_index_entry(&session_id).await {
        Some(entry) => Ok(HttpResponse::Ok().json(GetSessionResponse {
            session: {
                let runners = state.agent_runners.read().await;
                let is_running = runners
                    .get(&session_id)
                    .map(|r| matches!(r.status, AgentStatus::Running))
                    .unwrap_or(false);
                SessionSummary::from_entry(entry, is_running)
            },
        })),
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        }))),
    }
}

#[derive(Debug, Deserialize)]
pub struct PatchSessionRequest {
    pub title: Option<String>,
    pub pinned: Option<bool>,
}

/// `PATCH /api/v1/sessions/{session_id}`
pub async fn patch_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<PatchSessionRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let Some(mut session) = state.storage.load_session(&session_id).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to load session: {e}"))
    })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    if let Some(title) = req.title.as_ref() {
        session.title = title.trim().to_string();
    }
    if let Some(pinned) = req.pinned {
        session.pinned = pinned;
    }
    session.updated_at = chrono::Utc::now();

    state.storage.save_session(&session).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to save session: {e}"))
    })?;

    // Update in-memory cache too.
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    // Return updated summary (from index).
    get_session(state, web::Path::from(session_id)).await
}

/// `POST /api/v1/sessions/{session_id}/clear`
pub async fn clear_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let cleared = state
        .session_store
        .clear_session(&session_id)
        .await
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to clear session: {e}"))
        })?;

    if !cleared {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id
    })))
}

#[derive(Debug, Deserialize)]
pub struct CleanupRequest {
    pub mode: String,
    #[serde(default)]
    pub keep_pinned: bool,
}

/// `POST /api/v1/sessions/cleanup`
pub async fn cleanup_sessions(
    state: web::Data<AppState>,
    req: web::Json<CleanupRequest>,
) -> Result<HttpResponse> {
    let mode = match req.mode.trim().to_ascii_lowercase().as_str() {
        "all" => CleanupMode::All,
        "empty" => CleanupMode::Empty,
        "children" => CleanupMode::Children,
        other => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Invalid cleanup mode",
                "mode": other
            })));
        }
    };

    let result: CleanupResult = state
        .session_store
        .cleanup(mode, req.keep_pinned)
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(format!("Cleanup failed: {e}")))?;

    if !result.deleted_session_ids.is_empty() {
        // Best-effort cancel any in-flight executions.
        {
            let mut runners = state.agent_runners.write().await;
            for session_id in result.deleted_session_ids.iter() {
                if let Some(runner) = runners.remove(session_id) {
                    runner.cancel_token.cancel();
                }
            }
        }
        {
            let mut tokens = state.cancel_tokens.write().await;
            for session_id in result.deleted_session_ids.iter() {
                if let Some(token) = tokens.remove(session_id) {
                    token.cancel();
                }
            }
        }
        {
            let mut sessions = state.sessions.write().await;
            for session_id in result.deleted_session_ids.iter() {
                sessions.remove(session_id);
            }
        }
        {
            let mut senders = state.session_event_senders.write().await;
            for session_id in result.deleted_session_ids.iter() {
                senders.remove(session_id);
            }
        }
    }

    Ok(HttpResponse::Ok().json(result))
}

/// `GET /api/v1/sessions/{session_id}/attachments/{attachment_id}`
pub async fn get_attachment(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let (session_id, attachment_id) = path.into_inner();
    match state
        .session_store
        .read_attachment(&session_id, &attachment_id)
        .await
    {
        Ok(Some((bytes, mime))) => Ok(HttpResponse::Ok()
            .content_type(mime)
            .append_header(("Cache-Control", "no-store"))
            .body(bytes)),
        Ok(None) => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Attachment not found",
            "session_id": session_id,
            "attachment_id": attachment_id
        }))),
        Err(e) => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to read attachment: {e}"),
            "session_id": session_id,
            "attachment_id": attachment_id
        }))),
    }
}
