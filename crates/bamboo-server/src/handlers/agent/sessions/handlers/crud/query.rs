use std::collections::HashMap;

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

use super::super::super::types::{GetSessionResponse, ListSessionsResponse, SessionSummary};
use super::running::{is_session_running, running_session_ids};

/// `GET /api/v1/sessions`
pub async fn list_sessions(state: web::Data<AppState>) -> Result<HttpResponse> {
    let running = running_session_ids(&state).await;
    let entries = state.session_store.list_index_entries().await;

    // Compute running child counts per parent session.
    let mut running_child_counts: HashMap<String, u32> = HashMap::new();
    for entry in &entries {
        if running.contains(&entry.id) {
            if let Some(parent_id) = &entry.parent_session_id {
                *running_child_counts.entry(parent_id.clone()).or_insert(0) += 1;
            }
        }
    }

    Ok(HttpResponse::Ok().json(ListSessionsResponse {
        sessions: entries
            .into_iter()
            .map(|entry| {
                let is_running = running.contains(&entry.id);
                let mut summary = SessionSummary::from_entry(entry, is_running);
                summary.running_child_count =
                    running_child_counts.get(&summary.id).copied().unwrap_or(0);
                summary
            })
            .collect(),
    }))
}

/// `GET /api/v1/sessions/{session_id}`
pub async fn get_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    match state.session_store.get_index_entry(&session_id).await {
        Some(entry) => {
            let is_running = is_session_running(&state, &session_id).await;
            let running = running_session_ids(&state).await;
            let all_entries = state.session_store.list_index_entries().await;
            let running_child_count = all_entries
                .iter()
                .filter(|e| {
                    e.parent_session_id.as_ref() == Some(&session_id) && running.contains(&e.id)
                })
                .count() as u32;
            let mut summary = SessionSummary::from_entry(entry, is_running);
            summary.running_child_count = running_child_count;

            // Surface the session ETag (`metadata_version`) so clients can send
            // it back as `If-Match` on metadata writes (optimistic concurrency).
            let etag = state
                .storage
                .load_session(&session_id)
                .await
                .ok()
                .flatten()
                .map(|s| s.metadata_version);

            let mut response = HttpResponse::Ok();
            if let Some(version) = etag {
                response.insert_header((
                    actix_web::http::header::ETAG,
                    format!("\"{version}\""),
                ));
            }
            Ok(response.json(GetSessionResponse { session: summary }))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        }))),
    }
}
