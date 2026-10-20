//! Development-only endpoints.

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

/// Greenfield reset of sessions, runtime caches and owned browser site data.
pub async fn reset(state: web::Data<AppState>) -> Result<HttpResponse> {
    // Request cancellation must not abandon a partially completed wipe.
    tokio::spawn(reset_owned(state))
        .await
        .map_err(|_| crate::error::json_internal_server_error("Reset interrupted"))?
        .map_err(|error| {
            crate::error::json_internal_server_error(format!("Reset failed: {error}"))
        })?;
    Ok(HttpResponse::Ok().json(serde_json::json!({"success": true})))
}

async fn reset_owned(state: web::Data<AppState>) -> std::result::Result<(), String> {
    // Keep local and process-shared browser admission fenced through the
    // canonical wipe, terminal markers, orphan purge and runtime eviction.
    let browser_reset = state
        .browser
        .reset_profiles()
        .await
        .map_err(|error| format!("Browser cleanup failed: {error}"))?;
    let (candidates, storage_failure) = match state.session_store.dev_reset_with_progress().await {
        Ok(result) => (result.deleted_session_ids, None),
        Err(failure) => (
            failure.candidate_session_ids,
            Some(failure.error.to_string()),
        ),
    };
    let mut cleanup_failures = Vec::new();
    let mut confirmed_absent = Vec::new();
    for id in candidates {
        match state.session_store.session_is_canonically_absent(&id).await {
            Ok(true) => {
                if let Err(error) = state.browser.retire_deleted_session(&id).await {
                    cleanup_failures.push(format!("{id}: {error}"));
                }
                confirmed_absent.push(id);
            }
            Ok(false) => cleanup_failures.push(format!("{id}: canonical Session still exists")),
            Err(error) => {
                cleanup_failures.push(format!("{id}: canonical state uncertain: {error}"))
            }
        }
    }
    if let Some(error) = storage_failure {
        // Keep still-canonical profiles/caches. Only absence-confirmed IDs
        // above are terminal; a partial reset never authorizes an orphan wipe.
        evict_confirmed_runtime(&state, &confirmed_absent).await;
        return Err(format!(
            "{error}; browser cleanup: {}",
            cleanup_failures.join("; ")
        ));
    }
    if let Err(error) = browser_reset.purge_profiles().await {
        cleanup_failures.push(error.to_string());
    }

    // Clear in-memory cache too.
    state.sessions.clear();
    {
        let mut runners = state.agent_runners.write().await;
        for runner in runners.values() {
            runner.cancel_token.cancel();
            runner.event_publication.retire().await;
        }
        runners.clear();
    }
    {
        let mut tokens = state.cancel_tokens.write().await;
        for (_, token) in tokens.drain() {
            token.cancel();
        }
    }
    {
        let mut senders = state.session_event_senders.write().await;
        senders.clear();
    }

    if !cleanup_failures.is_empty() {
        return Err(format!(
            "browser cleanup incomplete: {}",
            cleanup_failures.join("; ")
        ));
    }
    Ok(())
}

// A failed reset may have revoked or removed only part of the tree. Child
// history can be read directly from cache, so confirmed absence must retire
// its runtime state even when a later Root/index operation failed. Surviving
// and uncertain IDs are deliberately excluded.
async fn evict_confirmed_runtime(state: &AppState, ids: &[String]) {
    for id in ids {
        state.sessions.remove(id);
    }
    {
        let mut runners = state.agent_runners.write().await;
        for id in ids {
            if let Some(runner) =
                bamboo_engine::runtime::execution::runner_lifecycle::remove_runner_entry(
                    &mut runners,
                    id,
                )
                .await
            {
                runner.cancel_token.cancel();
            }
        }
    }
    {
        let mut tokens = state.cancel_tokens.write().await;
        for id in ids {
            if let Some(token) = tokens.remove(id) {
                token.cancel();
            }
        }
    }
    let mut senders = state.session_event_senders.write().await;
    for id in ids {
        senders.remove(id);
    }
}
