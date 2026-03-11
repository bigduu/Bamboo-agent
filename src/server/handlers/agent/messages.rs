//! Message management endpoints (delete/truncate).
//!
//! These endpoints mutate a session's persisted message history.

use actix_web::{web, HttpResponse, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::agent::core::agent::Role;
use crate::server::app_state::{AgentStatus, AppState};

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TruncateRequest {
    /// Truncate all messages *after* the last user message.
    ///
    /// This is useful for "retry/regenerate" flows: keep the last user message
    /// but drop any assistant/tool tail so `POST /execute/{session_id}` can run again.
    AfterLastUser,
}

#[derive(Debug, Deserialize)]
pub struct RestoreSessionRequest {
    pub target_message_id: String,
    #[serde(default)]
    pub restore_files: bool,
}

#[derive(Debug, Deserialize)]
struct FileChangeCheckpoint {
    #[serde(default)]
    created: bool,
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FileChangePayload {
    file_path: String,
    checkpoint: Option<FileChangeCheckpoint>,
}

#[derive(Debug, Serialize)]
struct FileRestoreError {
    file_path: String,
    checkpoint_path: Option<String>,
    error: String,
}

/// `POST /api/v1/sessions/{session_id}/messages/truncate`
pub async fn truncate_messages(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<TruncateRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    // Avoid corrupting history while the agent is running.
    {
        let runners = state.agent_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": "Session is currently running",
                    "session_id": session_id,
                })));
            }
        }
    }

    let Some(mut session) = state.storage.load_session(&session_id).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to load session: {e}"))
    })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let (removed, new_len) = match req.into_inner() {
        TruncateRequest::AfterLastUser => {
            let last_user_idx = session
                .messages
                .iter()
                .rposition(|m| matches!(m.role, Role::User));

            let Some(idx) = last_user_idx else {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": "No user message found to truncate after",
                    "session_id": session_id
                })));
            };

            let keep_len = idx + 1;
            let removed = session.messages.len().saturating_sub(keep_len);
            session.messages.truncate(keep_len);
            (removed, keep_len)
        }
    };

    if removed > 0 {
        // Truncation invalidates derived context state.
        session.token_usage = None;
        session.conversation_summary = None;
        session.updated_at = Utc::now();

        state.storage.save_session(&session).await.map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to save session: {e}"))
        })?;

        // Best-effort update in-memory cache too.
        {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session_id.clone(), session);
        }
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "messages_removed": removed,
        "message_count": new_len,
    })))
}

/// `POST /api/v1/sessions/{session_id}/restore`
///
/// Restore session history to `target_message_id`.
/// When `restore_files` is true, file changes after the target message are reverted
/// by replaying tool checkpoints in reverse order.
pub async fn restore_session_state(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<RestoreSessionRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let target_message_id = req.target_message_id.trim().to_string();
    let restore_files = req.restore_files;

    if target_message_id.is_empty() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "target_message_id is required",
            "session_id": session_id,
        })));
    }

    // Avoid corrupting history while the agent is running.
    {
        let runners = state.agent_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": "Session is currently running",
                    "session_id": session_id,
                })));
            }
        }
    }

    let Some(mut session) = state.storage.load_session(&session_id).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to load session: {e}"))
    })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let Some(target_index) = session
        .messages
        .iter()
        .position(|m| m.id == target_message_id)
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Target message not found",
            "session_id": session_id,
            "target_message_id": target_message_id,
        })));
    };

    let messages_to_remove = session
        .messages
        .len()
        .saturating_sub(target_index.saturating_add(1));
    let tail = session.messages[target_index + 1..].to_vec();

    let mut restored_files = 0usize;
    let mut deleted_files = 0usize;
    let mut file_errors: Vec<FileRestoreError> = Vec::new();

    if restore_files {
        for message in tail.iter().rev() {
            if !matches!(message.role, Role::Tool) {
                continue;
            }

            let Ok(payload) = serde_json::from_str::<FileChangePayload>(&message.content) else {
                continue;
            };

            let file_path = payload.file_path.trim();
            if file_path.is_empty() {
                continue;
            }

            let checkpoint = payload.checkpoint;
            if let Some(checkpoint) = checkpoint {
                if checkpoint.created {
                    let Some(checkpoint_path) = checkpoint.path.as_deref() else {
                        file_errors.push(FileRestoreError {
                            file_path: file_path.to_string(),
                            checkpoint_path: None,
                            error: "Checkpoint path missing".to_string(),
                        });
                        continue;
                    };

                    let checkpoint_path_buf = Path::new(checkpoint_path);
                    let file_path_buf = Path::new(file_path);

                    let bytes = match tokio::fs::read(checkpoint_path_buf).await {
                        Ok(data) => data,
                        Err(error) => {
                            file_errors.push(FileRestoreError {
                                file_path: file_path.to_string(),
                                checkpoint_path: Some(checkpoint_path.to_string()),
                                error: format!("Failed to read checkpoint: {error}"),
                            });
                            continue;
                        }
                    };

                    if let Some(parent) = file_path_buf.parent() {
                        if let Err(error) = tokio::fs::create_dir_all(parent).await {
                            file_errors.push(FileRestoreError {
                                file_path: file_path.to_string(),
                                checkpoint_path: Some(checkpoint_path.to_string()),
                                error: format!("Failed to create parent directory: {error}"),
                            });
                            continue;
                        }
                    }

                    if let Err(error) = tokio::fs::write(file_path_buf, bytes).await {
                        file_errors.push(FileRestoreError {
                            file_path: file_path.to_string(),
                            checkpoint_path: Some(checkpoint_path.to_string()),
                            error: format!("Failed to restore file content: {error}"),
                        });
                        continue;
                    }

                    restored_files += 1;
                    continue;
                }
            }

            // No checkpoint means the file did not exist before the tool call.
            // Remove it to restore pre-change state.
            match tokio::fs::metadata(file_path).await {
                Ok(metadata) => {
                    if metadata.is_file() {
                        if let Err(error) = tokio::fs::remove_file(file_path).await {
                            file_errors.push(FileRestoreError {
                                file_path: file_path.to_string(),
                                checkpoint_path: None,
                                error: format!("Failed to delete file: {error}"),
                            });
                            continue;
                        }
                        deleted_files += 1;
                    } else {
                        file_errors.push(FileRestoreError {
                            file_path: file_path.to_string(),
                            checkpoint_path: None,
                            error: "Path is not a file".to_string(),
                        });
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // Already absent; treat as success.
                }
                Err(error) => {
                    file_errors.push(FileRestoreError {
                        file_path: file_path.to_string(),
                        checkpoint_path: None,
                        error: format!("Failed to inspect file: {error}"),
                    });
                }
            }
        }
    }

    session.messages.truncate(target_index + 1);
    session.token_usage = None;
    session.conversation_summary = None;
    session.updated_at = Utc::now();

    state.storage.save_session(&session).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to save session: {e}"))
    })?;

    // Best-effort update in-memory cache too.
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "target_message_id": target_message_id,
        "restore_files": restore_files,
        "messages_removed": messages_to_remove,
        "message_count": target_index + 1,
        "restored_files": restored_files,
        "deleted_files": deleted_files,
        "file_errors": file_errors,
    })))
}

/// `DELETE /api/v1/sessions/{session_id}/messages/{message_id}`
pub async fn delete_message(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let (session_id, message_id) = path.into_inner();

    // Avoid corrupting history while the agent is running.
    {
        let runners = state.agent_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": "Session is currently running",
                    "session_id": session_id,
                })));
            }
        }
    }

    let Some(mut session) = state.storage.load_session(&session_id).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to load session: {e}"))
    })?
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let before = session.messages.len();
    session.messages.retain(|m| m.id != message_id);
    let after = session.messages.len();

    if before == after {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Message not found",
            "session_id": session_id,
            "message_id": message_id,
        })));
    }

    // Deleting history invalidates derived context state.
    session.token_usage = None;
    session.conversation_summary = None;
    session.updated_at = Utc::now();

    state.storage.save_session(&session).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to save session: {e}"))
    })?;

    // Best-effort update in-memory cache too.
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "message_id": message_id,
        "message_count": after,
    })))
}
