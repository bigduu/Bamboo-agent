use std::collections::HashMap;

use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

use super::super::super::types::{
    GetSessionResponse, ListSessionsQuery, ListSessionsResponse, SessionSummary,
};
use super::running::{is_session_running, running_session_ids};

/// Default page size for `GET /api/v1/sessions` when the client omits `limit`.
/// Deliberately generous so a typical client sees all of its recent sessions in
/// one page, while still bounding the response so the list can't grow without
/// limit as session count grows forever (#252).
const DEFAULT_SESSIONS_PAGE: usize = 200;
/// Hard cap on the page size, so a client-supplied `limit` can't force an
/// unbounded read (mirrors the metrics `normalize_limit` clamp). (#252)
const MAX_SESSIONS_PAGE: usize = 1000;

/// Resolve the effective page size: the default when omitted, otherwise clamped
/// to `1..=MAX_SESSIONS_PAGE`. Never unbounded. (#252)
fn clamp_page_size(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(DEFAULT_SESSIONS_PAGE)
        .clamp(1, MAX_SESSIONS_PAGE)
}

/// `GET /api/v1/sessions`
pub async fn list_sessions(
    state: web::Data<AppState>,
    query: web::Query<ListSessionsQuery>,
) -> Result<HttpResponse> {
    let running = running_session_ids(&state).await;
    let entries = state.session_store.list_index_entries().await;

    // Compute running child counts per parent session over the FULL set: a
    // parent's running children may land on a different page, so this count must
    // not be paginated or it would be wrong for parents shown on this page.
    let mut running_child_counts: HashMap<String, u32> = HashMap::new();
    for entry in &entries {
        if running.contains(&entry.id) {
            if let Some(parent_id) = &entry.parent_session_id {
                *running_child_counts.entry(parent_id.clone()).or_insert(0) += 1;
            }
        }
    }

    // Server-enforced pagination bounds the response so the list stays finite as
    // session count grows (#252). `list_index_entries` is already sorted
    // newest-first, giving a deterministic page order.
    let total = entries.len();
    let limit = clamp_page_size(query.limit);
    let offset = query.offset.unwrap_or(0).min(total);

    let sessions: Vec<SessionSummary> = entries
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|entry| {
            let is_running = running.contains(&entry.id);
            let mut summary = SessionSummary::from_entry(entry, is_running);
            summary.running_child_count =
                running_child_counts.get(&summary.id).copied().unwrap_or(0);
            summary
        })
        .collect();

    let end = offset + sessions.len();
    let next_offset = if end < total { Some(end) } else { None };

    Ok(HttpResponse::Ok().json(ListSessionsResponse {
        sessions,
        total,
        limit,
        offset,
        next_offset,
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

            // Load the authoritative session once for both its ETag and the
            // public-safe active Workflow identity. The index deliberately
            // does not mirror this richer lifecycle object.
            let durable_session = state.storage.load_session(&session_id).await.ok().flatten();
            summary.active_workflow = durable_session.as_ref().and_then(|session| {
                session
                    .metadata
                    .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
                    .and_then(|raw| match serde_json::from_str(raw) {
                        Ok(active) => Some(active),
                        Err(error) => {
                            tracing::warn!(
                                %session_id,
                                %error,
                                "ignoring malformed active Workflow metadata in session detail"
                            );
                            None
                        }
                    })
            });
            // Surface the session ETag (`metadata_version`) so clients can send
            // it back as `If-Match` on metadata writes (optimistic concurrency).
            let etag = durable_session.map(|session| session.metadata_version);

            let mut response = HttpResponse::Ok();
            if let Some(version) = etag {
                response.insert_header((actix_web::http::header::ETAG, format!("\"{version}\"")));
            }
            Ok(response.json(GetSessionResponse { session: summary }))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            // Canonical nested error envelope — matches `AppError`'s shape
            // (#251 finding 2), with `session_id` kept as a sibling field.
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        }))),
    }
}

// Pure clamp unit test — kept in its own module that does NOT import
// `actix_web::test`, so the built-in `#[test]` attribute isn't shadowed by the
// actix test-macro re-export.
#[cfg(test)]
mod clamp_tests {
    use super::{clamp_page_size, DEFAULT_SESSIONS_PAGE, MAX_SESSIONS_PAGE};

    #[test]
    fn clamp_page_size_defaults_and_caps() {
        // Omitted → the bounded default, never an unbounded read (#252).
        assert_eq!(clamp_page_size(None), DEFAULT_SESSIONS_PAGE);
        // In-range passes through untouched.
        assert_eq!(clamp_page_size(Some(50)), 50);
        // Over the hard cap is clamped down; zero is clamped up to 1.
        assert_eq!(clamp_page_size(Some(10_000_000)), MAX_SESSIONS_PAGE);
        assert_eq!(clamp_page_size(Some(0)), 1);
    }
}

#[cfg(test)]
mod pagination_http_tests {
    use actix_web::{test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use super::{DEFAULT_SESSIONS_PAGE, MAX_SESSIONS_PAGE};
    use crate::routes::configure_routes;
    use crate::AppState;
    use bamboo_agent_core::Session;

    async fn app_state_with_sessions(n: usize) -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        for i in 0..n {
            let mut session = Session::new(format!("sess-{i:03}"), "model");
            state.save_and_cache_session(&mut session).await;
        }
        state
    }

    /// Omitting `limit` returns the server default page size (bounded), and a
    /// `limit` above the hard max is clamped to the max — mirrors the metrics
    /// `normalize_limit` clamp. Without the pagination fix the response carries
    /// no `limit` field at all, so these assertions fail. (#252)
    #[actix_web::test]
    async fn list_sessions_default_and_max_are_enforced() {
        let state = app_state_with_sessions(3).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // No params → the effective page size is the server default, not unbounded.
        let resp: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        assert_eq!(resp["limit"], DEFAULT_SESSIONS_PAGE as u64);
        assert_eq!(resp["total"], 3);
        assert_eq!(resp["sessions"].as_array().unwrap().len(), 3);
        // Everything fits on the first page, so there is no next page.
        assert!(resp.get("next_offset").is_none() || resp["next_offset"].is_null());

        // A `limit` above the hard cap is clamped down to the max.
        let capped: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions?limit=10000000")
                .to_request(),
        )
        .await;
        assert_eq!(capped["limit"], MAX_SESSIONS_PAGE as u64);
    }

    /// `limit`/`offset` slice the newest-first list and `next_offset` walks the
    /// pages, ending at `None` on the last page. (#252)
    #[actix_web::test]
    async fn list_sessions_pages_with_limit_and_offset() {
        let state = app_state_with_sessions(3).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // First page of 2 of 3 → a next page starts at offset 2.
        let page1: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions?limit=2")
                .to_request(),
        )
        .await;
        assert_eq!(page1["total"], 3);
        assert_eq!(page1["limit"], 2);
        assert_eq!(page1["offset"], 0);
        assert_eq!(page1["sessions"].as_array().unwrap().len(), 2);
        assert_eq!(page1["next_offset"], 2);

        // Second page picks up the remaining 1 and reports no further page.
        let page2: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions?limit=2&offset=2")
                .to_request(),
        )
        .await;
        assert_eq!(page2["offset"], 2);
        assert_eq!(page2["sessions"].as_array().unwrap().len(), 1);
        assert!(page2.get("next_offset").is_none() || page2["next_offset"].is_null());
    }

    /// `GET /api/v1/sessions/{id}` on an unknown id must use the canonical
    /// nested error envelope (`{"error": {"message", "type"}}`, matching
    /// `AppError`), not the old flat `{"error": "<string>"}` shape. #251
    /// (finding 2).
    #[actix_web::test]
    async fn get_session_not_found_uses_canonical_error_envelope() {
        let state = app_state_with_sessions(0).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions/does-not-exist")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["error"]["message"], "Session not found");
        assert_eq!(body["session_id"], "does-not-exist");
    }

    #[actix_web::test]
    async fn session_detail_restores_public_active_workflow_but_list_stays_lightweight() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let mut session = Session::new("workflow-session", "model");
        session.metadata.insert(
            bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY.to_string(),
            serde_json::json!({
                "id": "review",
                "source": "builtin",
                "revision": 7,
                "kind": "instruction",
                "args": {"focus": "security"},
                "invoked_by": "user",
                "activated_at": "2026-08-21T00:00:00Z",
                "status": "active",
                "context_fingerprint": "sha256:test"
            })
            .to_string(),
        );
        state.save_and_cache_session(&mut session).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let list: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        assert!(list["sessions"][0].get("active_workflow").is_none());

        let detail: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions/workflow-session")
                .to_request(),
        )
        .await;
        assert_eq!(detail["session"]["active_workflow"]["id"], "review");
        assert_eq!(detail["session"]["active_workflow"]["source"], "builtin");
        assert_eq!(
            detail["session"]["active_workflow"]["args"]["focus"],
            "security"
        );
        assert!(detail["session"]["active_workflow"].get("prompt").is_none());
    }
}
