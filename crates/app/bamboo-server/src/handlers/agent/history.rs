//! Session history API handler.
//!
//! This module provides the HTTP endpoint for retrieving chat session history,
//! with optional delta retrieval via a `since_message_id` cursor.

use actix_web::{web, HttpResponse, Responder};
use serde::Deserialize;

use crate::app_state::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct HistoryQuery {
    /// When set, return only UI-visible messages appended *after* the message
    /// with this id (a delta). Falls back to the full history if the id is not
    /// found (e.g. the client is far behind, or the message was edited away).
    #[serde(default)]
    pub since_message_id: Option<String>,
}

/// Retrieve message history for a chat session.
///
/// # HTTP Method
///
/// `GET /api/v1/sessions/{session_id}/history`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier
///
/// # Response
///
/// Returns a JSON object containing the session ID and message history.
///
/// # Response Format
///
/// ```json
/// {
///   "session_id": "session-123",
///   "messages": []
/// }
/// ```
///
/// # Note
///
/// Currently returns an empty messages array. Full history retrieval
/// is planned for a future release.
///
/// # Example
///
/// ```bash
/// curl http://localhost:9562/api/v1/sessions/session-123/history
/// ```
pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<HistoryQuery>,
) -> impl Responder {
    let session_id = path.into_inner();

    // When an agent runner is active the in-memory session cache (`state.sessions`)
    // may lag behind disk because the loop works with a local `&mut Session` and only
    // writes back to the cache after `run_agent_loop` returns.  The agent *does* persist
    // to disk after significant changes (conclusion_with_options, compaction, finalize), so reading
    // from disk gives the frontend the freshest snapshot during execution.
    let runner_active = {
        let runners = state.agent_runners.read().await;
        runners
            .get(&session_id)
            .is_some_and(|r| r.completed_at.is_none())
    };

    let mut session = if runner_active {
        // Prefer disk – the agent loop may have persisted messages that
        // are not yet in the memory cache.
        match state.storage.load_session(&session_id).await {
            Ok(Some(s)) => Some(s),
            Ok(None) => {
                // Fallback to memory (shouldn't happen but be defensive).
                bamboo_engine::read_cached_session(&state.sessions, &session_id)
            }
            Err(e) => {
                tracing::warn!(
                    "[{}] Disk read failed during active execution, falling back to memory: {}",
                    session_id,
                    e
                );
                bamboo_engine::read_cached_session(&state.sessions, &session_id)
            }
        }
    } else {
        // No active runner – memory cache is authoritative.
        bamboo_engine::read_cached_session(&state.sessions, &session_id)
    };

    if session.is_none() {
        match state.storage.load_session(&session_id).await {
            Ok(Some(s)) => session = Some(s),
            Ok(None) => {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": session_id
                }));
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to load session: {e}"),
                    "session_id": session_id
                }));
            }
        }
    }

    let Some(session) = session else {
        return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Session load unexpectedly returned no data",
            "session_id": session_id
        }));
    };

    let mut messages: Vec<_> = session
        .messages
        .into_iter()
        .filter(|message| !bamboo_engine::session_app::execute::is_hidden_from_ui(message))
        .collect();

    // Delta mode: if the client supplied a cursor and we can locate it, return
    // only the messages after it. This naturally includes assistant and tool
    // messages (which carry no `MessageAppended` feed event), so it stays
    // correct even though the feed only pings user-message appends. An unknown
    // cursor falls back to the full list (`is_delta = false`).
    let mut is_delta = false;
    if let Some(cursor) = query.since_message_id.as_deref().filter(|c| !c.is_empty()) {
        if let Some(idx) = messages.iter().position(|m| m.id == cursor) {
            messages.drain(..=idx);
            is_delta = true;
        }
    }

    // Include the session-level gold config so the frontend can update its
    // local session summary after sync-recovery without an extra round-trip.
    let gold_config = session
        .metadata
        .get(bamboo_engine::model_config_helper::GOLD_CONFIG_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_engine::config::GoldConfig>(raw).ok());

    let mut response = serde_json::json!({
        "session_id": session_id,
        "messages": messages,
        "is_delta": is_delta,
        "compression_events": session.compression_events
    });

    if let Some(gc) = gold_config {
        response
            .as_object_mut()
            .unwrap()
            .insert("gold_config".to_string(), serde_json::to_value(gc).unwrap());
    }

    HttpResponse::Ok().json(response)
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;
    use bamboo_agent_core::{Message, Session};

    async fn app_state_with_session(messages: Vec<Message>) -> (web::Data<AppState>, String) {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let mut session = Session::new("hist-delta", "model");
        for m in messages {
            session.add_message(m);
        }
        state.save_and_cache_session(&mut session).await;
        (state, "hist-delta".to_string())
    }

    fn seqs(messages: &Value) -> Vec<String> {
        messages
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["content"].as_str().unwrap().to_string())
            .collect()
    }

    #[actix_web::test]
    async fn delta_history_returns_only_messages_after_cursor() {
        let (state, id) = app_state_with_session(vec![
            Message::user("m1"),
            Message::assistant("m2", None),
            Message::user("m3"),
        ])
        .await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // Full history.
        let full: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/history/{id}"))
                .to_request(),
        )
        .await;
        assert_eq!(full["is_delta"], false);
        assert_eq!(seqs(&full["messages"]), vec!["m1", "m2", "m3"]);
        let cursor = full["messages"][0]["id"].as_str().unwrap().to_string();

        // Delta from the first message: should be exactly the tail [m2, m3],
        // including the assistant message which has no MessageAppended event.
        let delta: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/history/{id}?since_message_id={cursor}"))
                .to_request(),
        )
        .await;
        assert_eq!(delta["is_delta"], true);
        assert_eq!(seqs(&delta["messages"]), vec!["m2", "m3"]);
    }

    #[actix_web::test]
    async fn delta_history_unknown_cursor_falls_back_to_full() {
        let (state, id) =
            app_state_with_session(vec![Message::user("a"), Message::user("b")]).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!(
                    "/api/v1/history/{id}?since_message_id=does-not-exist"
                ))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["is_delta"], false);
        assert_eq!(seqs(&body["messages"]), vec!["a", "b"]);
    }
}
