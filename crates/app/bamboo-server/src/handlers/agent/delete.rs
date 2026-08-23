//! Session deletion API handler.
//!
//! This module provides the HTTP endpoint for deleting chat sessions
//! and cancelling in-flight agent executions.

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

/// Delete a chat session and cancel any running agent execution.
///
/// This endpoint removes the session from both memory and persistent storage,
/// and cancels any in-flight agent execution for that session.
///
/// # HTTP Method
///
/// `DELETE /api/v1/sessions/{session_id}`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier to delete
///
/// # Response
///
/// - `200 OK` - Session deleted successfully (no body)
/// - `404 Not Found` - Session does not exist
/// - `500 Internal Server Error` - Failed to delete from storage
///
/// # Side Effects
///
/// When a session is deleted:
/// 1. Session is removed from persistent storage (if exists)
/// 2. Session is removed from in-memory cache
/// 3. Any running agent execution is cancelled
/// 4. Associated cancellation tokens are cleaned up
///
/// # Idempotency
///
/// This endpoint is idempotent. Calling it multiple times with the same
/// session ID will return `404 Not Found` after the first successful deletion.
///
/// # Example
///
/// ```bash
/// curl -X DELETE http://localhost:9562/api/v1/sessions/session-123
/// ```
pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let _persistence_guard = state.persistence.acquire_lock(&session_id).await;

    // Best-effort pre-cancellation of the session (and its children if this is a root session).
    let ids_to_cancel: Vec<String> = match state.session_store.get_index_entry(&session_id).await {
        Some(entry) if matches!(entry.kind, bamboo_agent_core::SessionKind::Root) => state
            .session_store
            .list_index_entries()
            .await
            .into_iter()
            .filter(|e| e.root_session_id == session_id)
            .map(|e| e.id)
            .collect(),
        Some(_) => vec![session_id.clone()],
        None => vec![session_id.clone()],
    };

    // Deletion is terminal, unlike a resumable stop/suspension. Release every
    // root/child activation before removing session state so immutable snapshot
    // capacity cannot leak even if a runner never reaches its own finalizer.
    for id in &ids_to_cancel {
        if let Err(error) = state
            .skill_manager
            .release_activation_for_workspace(id, None)
            .await
        {
            tracing::warn!(
                "[{}] Failed to release deleted workflow activation snapshot: {}",
                id,
                error
            );
        }
    }

    let cancelled_runner = {
        let mut runners = state.agent_runners.write().await;
        let mut cancelled = false;
        for id in ids_to_cancel.iter() {
            if let Some(runner) = runners.remove(id) {
                runner.cancel_token.cancel();
                cancelled = true;
            }
        }
        cancelled
    };

    let deleted_from_storage = match state.storage.delete_session(&session_id).await {
        Ok(deleted) => deleted,
        Err(error) => {
            tracing::error!(
                "[{}] Failed to delete session from storage: {}",
                session_id,
                error
            );
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value("Failed to delete session")
            })));
        }
    };

    let removed_from_memory = {
        let mut removed = false;
        for id in ids_to_cancel.iter() {
            removed |= state.sessions.remove(id).is_some();
        }
        removed
    };

    {
        let mut senders = state.session_event_senders.write().await;
        for id in ids_to_cancel.iter() {
            senders.remove(id);
        }
    }

    let cancelled_in_flight = {
        let mut tokens = state.cancel_tokens.write().await;
        let mut cancelled = false;
        for id in ids_to_cancel.iter() {
            if let Some(token) = tokens.remove(id) {
                token.cancel();
                cancelled = true;
            }
        }
        cancelled
    };

    if deleted_from_storage || removed_from_memory || cancelled_in_flight || cancelled_runner {
        // Publish onto the account change feed so other clients drop the
        // deleted session(s) from their list without polling.
        for id in ids_to_cancel.iter() {
            state.account_sink.record(
                Some(id),
                &bamboo_agent_core::AgentEvent::SessionDeleted {
                    session_id: id.clone(),
                },
            );
        }
        tracing::info!(
            "[{}] Session deleted successfully (storage: {}, memory: {}, cancelled: {}, runner_cancelled: {})",
            session_id,
            deleted_from_storage,
            removed_from_memory,
            cancelled_in_flight,
            cancelled_runner
        );
        return Ok(HttpResponse::Ok().finish());
    }

    Ok(HttpResponse::NotFound().json(serde_json::json!({
        "error": crate::error::error_value("Session not found")
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::Session;

    #[tokio::test]
    async fn deleting_root_releases_root_and_child_workflow_activations() {
        let directory = tempfile::tempdir().expect("tempdir");
        let state = AppState::new(directory.path().to_path_buf())
            .await
            .expect("app state");
        let root = Session::new("delete-root", "model");
        let child = Session::new_child_of("delete-child", &root, "model", "child");
        state
            .session_store
            .save_session(&root)
            .await
            .expect("save root index");
        state
            .session_store
            .save_session(&child)
            .await
            .expect("save child index");
        state.sessions.insert(
            root.id.clone(),
            std::sync::Arc::new(parking_lot::RwLock::new(root.clone())),
        );
        state.sessions.insert(
            child.id.clone(),
            std::sync::Arc::new(parking_lot::RwLock::new(child.clone())),
        );
        let ids = vec!["review".to_string()];
        for session_id in [&root.id, &child.id] {
            state
                .skill_manager
                .store()
                .pin_current_activation(session_id, &ids, None)
                .await
                .expect("pin workflow activation");
        }
        let state = web::Data::new(state);

        let response = handler(state.clone(), web::Path::from(root.id.clone()))
            .await
            .expect("delete response");

        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        for session_id in [&root.id, &child.id] {
            assert!(state
                .skill_manager
                .store()
                .activation_descriptor(session_id)
                .await
                .is_none());
        }
    }
}
