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
/// - `500 Internal Server Error` - Storage deletion or browser cleanup failed;
///   cleanup errors include session IDs that can be retried
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
    // Storage's physical deletion can outlive an aborted HTTP waiter. Keep
    // browser cleanup in that same owned operation, rather than leaving an
    // await/cancellation gap between deleting the chat and retiring its host.
    let outcome = tokio::spawn(delete_owned(state, session_id))
        .await
        .map_err(|_| crate::error::json_internal_server_error("Session deletion interrupted"))?;
    Ok(match outcome {
        DeleteOutcome::Deleted => HttpResponse::Ok().finish(),
        DeleteOutcome::Missing => HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found")
        })),
        DeleteOutcome::StorageFailed(ids) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value("Failed to finish session deletion"),
            "storage_deletion_failed": true,
            "browser_cleanup_pending_session_ids": ids
        })),
        DeleteOutcome::BrowserCleanupIncomplete(ids) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value("Session removed; browser data cleanup incomplete. Retry deletion for the listed session IDs."),
            "browser_cleanup_pending_session_ids": ids
        })),
    })
}

enum DeleteOutcome {
    Deleted,
    Missing,
    StorageFailed(Vec<String>),
    BrowserCleanupIncomplete(Vec<String>),
}

async fn delete_owned(state: web::Data<AppState>, session_id: String) -> DeleteOutcome {
    let _persistence_guard = state.persistence.acquire_lock(&session_id).await;

    // Best-effort pre-cancellation of the session (and its children if this is a root session).
    let mut ids_to_cancel: Vec<String> =
        match state.session_store.get_index_entry(&session_id).await {
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
            if let Some(runner) =
                bamboo_engine::runtime::execution::runner_lifecycle::remove_runner_entry(
                    &mut runners,
                    id,
                )
                .await
            {
                runner.cancel_token.cancel();
                cancelled = true;
            }
        }
        cancelled
    };

    let pre_cancelled_ids: std::collections::HashSet<String> =
        ids_to_cancel.iter().cloned().collect();
    let (deleted_from_storage, storage_failed, mut actual_candidates) = match state
        .session_store
        .delete_session_recursive_with_progress(&session_id, true)
        .await
    {
        Ok(result) => (result.deleted, false, result.candidate_session_ids),
        Err(failure) => {
            tracing::error!(
                "[{}] Failed to delete session from storage: {}",
                session_id,
                failure.error
            );
            (false, true, failure.candidate_session_ids)
        }
    };

    // The preflight may miss children added by another writer. Storage returns
    // its actual locked candidates; canonical absence, not a stale index or
    // the error alone, permits terminal browser cleanup.
    actual_candidates.extend(ids_to_cancel);
    actual_candidates.sort();
    actual_candidates.dedup();
    let mut browser_cleanup_pending = Vec::new();
    ids_to_cancel = Vec::new();
    for id in actual_candidates {
        match state.storage.load_session(&id).await {
            Ok(None) => ids_to_cancel.push(id),
            Ok(Some(_)) => {}
            Err(_) => browser_cleanup_pending.push(id),
        }
    }
    for id in &ids_to_cancel {
        // Late children were not in the pre-cancellation snapshot.
        if !pre_cancelled_ids.contains(id) {
            if let Err(error) = state
                .skill_manager
                .release_activation_for_workspace(id, None)
                .await
            {
                tracing::warn!(session_id = %id, %error, "deleted session activation cleanup incomplete");
            }
        }
        if let Err(error) = state.browser.retire(id).await {
            tracing::warn!(session_id = %id, %error, "deleted session browser cleanup incomplete; retry deletion");
            browser_cleanup_pending.push(id.clone());
        }
    }

    let removed_from_memory = {
        let mut removed = false;
        for id in ids_to_cancel.iter() {
            removed |= state.sessions.remove(id).is_some();
        }
        removed
    };

    let cancelled_runner = {
        let mut runners = state.agent_runners.write().await;
        let mut cancelled = cancelled_runner;
        for id in &ids_to_cancel {
            if let Some(runner) =
                bamboo_engine::runtime::execution::runner_lifecycle::remove_runner_entry(
                    &mut runners,
                    id,
                )
                .await
            {
                runner.cancel_token.cancel();
                cancelled = true;
            }
        }
        cancelled
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
    }
    if storage_failed {
        DeleteOutcome::StorageFailed(browser_cleanup_pending)
    } else if !browser_cleanup_pending.is_empty() {
        DeleteOutcome::BrowserCleanupIncomplete(browser_cleanup_pending)
    } else if deleted_from_storage || removed_from_memory || cancelled_in_flight || cancelled_runner
    {
        DeleteOutcome::Deleted
    } else {
        DeleteOutcome::Missing
    }
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
            std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(root.clone())),
        );
        state.sessions.insert(
            child.id.clone(),
            std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(child.clone())),
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
        let profiles = [&root.id, &child.id].map(|id| {
            let profile = crate::browser::test_profile_directory(
                &directory.path().join("browser-profiles"),
                id,
            );
            std::fs::write(profile.join("Cookies"), b"synthetic state").unwrap();
            profile
        });

        let response = handler(state.clone(), web::Path::from(root.id.clone()))
            .await
            .expect("delete response");

        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        assert!(profiles.iter().all(|path| !path.exists()));
        for session_id in [&root.id, &child.id] {
            assert!(state
                .skill_manager
                .store()
                .activation_descriptor(session_id)
                .await
                .is_none());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn logically_deleted_root_still_purges_browser_data_when_physical_storage_removal_fails()
    {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
        let session = Session::new("delete-browser-partial-storage", "model");
        state.session_store.save_session(&session).await.unwrap();
        let profile = crate::browser::test_profile_directory(
            &directory.path().join("browser-profiles"),
            &session.id,
        );
        std::fs::write(profile.join("Cookies"), b"synthetic state").unwrap();
        let storage_directory = directory.path().join("sessions").join(&session.id);
        std::fs::set_permissions(&storage_directory, std::fs::Permissions::from_mode(0o500))
            .unwrap();
        let response = handler(state.clone(), web::Path::from(session.id.clone()))
            .await
            .unwrap();
        if storage_directory.exists() {
            std::fs::set_permissions(&storage_directory, std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        assert_eq!(
            response.status(),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["storage_deletion_failed"], true);
        assert!(state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .is_none());
        assert!(!profile.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_browser_cleanup_is_reported_and_missing_session_deletion_retries_it() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
        let session = Session::new("delete-browser-retry", "model");
        state.session_store.save_session(&session).await.unwrap();
        let profile = crate::browser::test_profile_directory(
            &directory.path().join("browser-profiles"),
            &session.id,
        );
        std::fs::write(profile.join("Cookies"), b"synthetic state").unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o500)).unwrap();
        let response = handler(state.clone(), web::Path::from(session.id.clone()))
            .await
            .unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            response.status(),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["browser_cleanup_pending_session_ids"],
            serde_json::json!([session.id.clone()])
        );
        assert!(state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .is_none());
        assert!(profile.join("Cookies").exists());

        let retried = handler(state, web::Path::from(session.id)).await.unwrap();
        assert_eq!(retried.status(), actix_web::http::StatusCode::NOT_FOUND);
        assert!(!profile.exists());
    }

    #[tokio::test]
    async fn cancelled_http_waiter_does_not_abandon_storage_or_browser_deletion() {
        use std::time::Duration;
        let directory = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
        let session = Session::new("delete-browser-cancelled-waiter", "model");
        state.session_store.save_session(&session).await.unwrap();
        let profile = crate::browser::test_profile_directory(
            &directory.path().join("browser-profiles"),
            &session.id,
        );
        std::fs::write(profile.join("Cookies"), b"synthetic state").unwrap();
        let guard = state.persistence.acquire_lock(&session.id).await;
        assert!(tokio::time::timeout(
            Duration::from_millis(50),
            handler(state.clone(), web::Path::from(session.id.clone()))
        )
        .await
        .is_err());
        assert!(state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .is_some());
        drop(guard);
        tokio::time::timeout(Duration::from_secs(5), async {
            while state
                .storage
                .load_session(&session.id)
                .await
                .unwrap()
                .is_some()
                || profile.exists()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the owned deletion was abandoned when its HTTP waiter disconnected");
    }
}
