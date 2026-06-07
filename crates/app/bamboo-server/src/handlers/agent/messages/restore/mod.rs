use actix_web::{web, HttpResponse, Result};

use super::shared::{
    clear_derived_context_state, ensure_session_not_running, load_session_or_404,
    save_and_cache_session,
};
use super::types::RestoreSessionRequest;
use crate::app_state::AppState;

mod files;
mod models;
mod planning;

#[cfg(test)]
mod tests;

use files::apply_restore_plan;
use models::FileRestoreOutcome;
use planning::{build_restore_plan, find_target_message_index};

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

    if let Some(response) = ensure_session_not_running(&state, &session_id).await {
        return Ok(response);
    }

    let Some(mut session) = load_session_or_404(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        })));
    };

    let Some(target_index) = find_target_message_index(&session.messages, &target_message_id)
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
    let messages_after_target = session.messages[target_index + 1..].to_vec();

    let restore_outcome = if restore_files {
        let plan = build_restore_plan(&messages_after_target);
        apply_restore_plan(plan).await
    } else {
        FileRestoreOutcome::default()
    };
    let FileRestoreOutcome {
        restored_files,
        deleted_files,
        file_errors,
    } = restore_outcome;

    session.messages.truncate(target_index + 1);
    clear_derived_context_state(&mut session);
    save_and_cache_session(&state, &session_id, session).await?;

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
