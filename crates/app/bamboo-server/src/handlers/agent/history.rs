//! Session history API handler.
//!
//! This module provides the HTTP endpoint for retrieving chat session history,
//! with optional delta retrieval via a `since_message_id` cursor.

use actix_web::{web, HttpResponse, Responder};
use serde::Deserialize;

use bamboo_agent_core::Role;

use crate::app_state::AppState;

/// Hard cap on the number of UI-visible messages a cold (non-delta) history
/// fetch returns, so a pathological long-running session can't return an
/// unbounded array (#252). The most recent messages are kept — the tail is what
/// a chat UI renders first — and `truncated` is surfaced so a client knows
/// earlier messages were dropped. A delta fetch is already bounded by its cursor
/// and is never trimmed.
const MAX_HISTORY_MESSAGES: usize = 2000;

/// Cold-fetch cap for the history response: when returning a full (non-delta)
/// history that exceeds [`MAX_HISTORY_MESSAGES`], drop the oldest overflow so
/// only the newest `MAX_HISTORY_MESSAGES` remain. Returns whether it trimmed.
///
/// The count-based drop is tool-pair aware: `is_tool_result` reports whether a
/// message is a `tool_result`, and after the count trim any LEADING orphaned
/// tool_result(s) are dropped too. Otherwise a session over the cap could start
/// a cold fetch mid-pair — the assistant `tool_call` was in the dropped overflow
/// but its `tool_result` survived at the head, leaving a dangling result the LLM
/// (and frontend) can't match to a call. A parallel-call turn can leave several
/// consecutive orphaned results, so all leading ones are dropped to the next
/// safe turn boundary. (#422)
fn cap_cold_history<T>(
    messages: &mut Vec<T>,
    is_delta: bool,
    is_tool_result: impl Fn(&T) -> bool,
) -> bool {
    if is_delta || messages.len() <= MAX_HISTORY_MESSAGES {
        return false;
    }
    let drop = messages.len() - MAX_HISTORY_MESSAGES;
    messages.drain(..drop);

    let orphan_head = messages.iter().take_while(|m| is_tool_result(m)).count();
    if orphan_head > 0 {
        messages.drain(..orphan_head);
    }
    true
}

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
                // Canonical nested error envelope — matches `AppError`'s shape
                // (#251 finding 2), with `session_id` kept as a sibling field
                // for callers that already read it off this endpoint.
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": crate::error::error_value("Session not found"),
                    "session_id": session_id
                }));
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": crate::error::error_value(format!("Failed to load session: {e}")),
                    "session_id": session_id
                }));
            }
        }
    }

    let Some(session) = session else {
        return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value("Session load unexpectedly returned no data"),
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

    // Bound a cold fetch: a session that never used the delta cursor would
    // otherwise return its entire (unbounded) message history in one response
    // (#252). The count *before* capping is reported so a client can tell it
    // received a truncated tail.
    let total_message_count = messages.len();
    let truncated = cap_cold_history(&mut messages, is_delta, |m| matches!(m.role, Role::Tool));

    // Include the session-level gold config so the frontend can update its
    // local session summary after sync-recovery without an extra round-trip.
    let gold_config = session
        .metadata
        .get(bamboo_engine::model_config_helper::GOLD_CONFIG_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<bamboo_engine::config::GoldConfig>(raw).ok());

    // Include the runtime goal state (status, continuation count, and the
    // side-channel double-check eval history) so the frontend can show live
    // goal progress, not just the configured objective. Stored as a JSON blob
    // under `goal.state` (see `bamboo_engine::runtime::goal_state`).
    let goal_state = session
        .metadata
        .get("goal.state")
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());

    let mut response = serde_json::json!({
        "session_id": session_id,
        "messages": messages,
        "is_delta": is_delta,
        // Whether the cold fetch dropped older messages to stay under the cap,
        // and the pre-cap UI-visible count so a client can detect the gap (#252).
        "truncated": truncated,
        "total_message_count": total_message_count,
        "compression_events": session.compression_events
    });

    if let Some(gc) = gold_config {
        response
            .as_object_mut()
            .unwrap()
            .insert("gold_config".to_string(), serde_json::to_value(gc).unwrap());
    }

    if let Some(gs) = goal_state {
        response
            .as_object_mut()
            .unwrap()
            .insert("goal_state".to_string(), gs);
    }

    HttpResponse::Ok().json(response)
}

// Pure cold-fetch-cap unit test — kept in its own module that does NOT import
// `actix_web::test`, so the built-in `#[test]` attribute isn't shadowed by the
// actix test-macro re-export.
#[cfg(test)]
mod cold_cap_tests {
    use super::{cap_cold_history, MAX_HISTORY_MESSAGES};

    #[test]
    fn cap_cold_history_trims_cold_fetch_to_newest() {
        // A cold fetch over the cap keeps only the newest MAX_HISTORY_MESSAGES,
        // preserving the tail and dropping the oldest overflow (#252).
        let mut over: Vec<u32> = (0..(MAX_HISTORY_MESSAGES as u32 + 5)).collect();
        let newest = *over.last().unwrap();
        assert!(cap_cold_history(&mut over, false, |_| false));
        assert_eq!(over.len(), MAX_HISTORY_MESSAGES);
        assert_eq!(*over.last().unwrap(), newest, "keeps the newest message");
        assert_eq!(over[0], 5, "drops the oldest overflow");

        // At/under the cap the cold fetch is untouched.
        let mut small: Vec<u32> = (0..10).collect();
        assert!(!cap_cold_history(&mut small, false, |_| false));
        assert_eq!(small.len(), 10);

        // A delta fetch is never trimmed, even when large.
        let mut delta: Vec<u32> = (0..(MAX_HISTORY_MESSAGES as u32 + 5)).collect();
        assert!(!cap_cold_history(&mut delta, true, |_| false));
        assert_eq!(delta.len(), MAX_HISTORY_MESSAGES + 5);
    }

    #[test]
    fn cap_cold_history_drops_leading_orphaned_tool_results_to_safe_boundary() {
        // (value, is_tool_result). The count trim drops the oldest overflow; if
        // that lands the head on a tool_result whose assistant tool_call was
        // dropped, the cap must advance past the leading orphaned result(s) to the
        // next safe turn boundary. (#422)
        let mut msgs: Vec<(u32, bool)> = (0..(MAX_HISTORY_MESSAGES as u32 + 3))
            .map(|i| (i, false))
            .collect();
        // len = MAX+3 → drops the oldest 3 (indices 0,1,2), head becomes index 3.
        // Mark indices 3 and 4 as orphaned tool_results (a parallel-call pair);
        // index 5 is a normal message — the safe boundary.
        msgs[3].1 = true;
        msgs[4].1 = true;
        let is_tool = |m: &(u32, bool)| m.1;

        assert!(cap_cold_history(&mut msgs, false, is_tool));
        assert!(
            !msgs.first().unwrap().1,
            "head must not be a leading orphaned tool_result"
        );
        assert_eq!(
            msgs.first().unwrap().0,
            5,
            "trimmed past the orphaned pair to the next safe boundary"
        );
        // 3 dropped by count + 2 orphaned results → slightly under the cap.
        assert_eq!(msgs.len(), MAX_HISTORY_MESSAGES - 2);
    }

    #[test]
    fn cap_cold_history_keeps_head_when_boundary_already_safe() {
        // If the count trim lands on a non-tool message, nothing extra is dropped.
        let mut msgs: Vec<(u32, bool)> = (0..(MAX_HISTORY_MESSAGES as u32 + 3))
            .map(|i| (i, false))
            .collect();
        let is_tool = |m: &(u32, bool)| m.1;

        assert!(cap_cold_history(&mut msgs, false, is_tool));
        assert_eq!(msgs.len(), MAX_HISTORY_MESSAGES, "no extra orphan trim");
        assert_eq!(
            msgs.first().unwrap().0,
            3,
            "only the count overflow dropped"
        );
    }
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
    async fn history_response_includes_goal_state() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let mut session = Session::new("hist-goal", "model");
        session.add_message(Message::user("do it"));
        // Seed the durable goal state exactly as the engine persists it.
        session.metadata.insert(
            "goal.state".to_string(),
            serde_json::json!({
                "objective": "ship it",
                "status": "complete",
                "continuation_count": 1,
                "eval_history": [{
                    "checkpoint": "terminal",
                    "iteration": 3,
                    "decision": "achieved",
                    "confidence": "high",
                    "reasoning": "verified against current state",
                    "recorded_at": "2026-06-16T00:00:00Z"
                }],
                "created_at": "2026-06-16T00:00:00Z",
                "updated_at": "2026-06-16T00:00:00Z"
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

        let resp: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/history/hist-goal")
                .to_request(),
        )
        .await;

        // The runtime goal state is surfaced so the frontend can show live progress.
        assert_eq!(resp["goal_state"]["status"], "complete");
        assert_eq!(resp["goal_state"]["continuation_count"], 1);
        assert_eq!(
            resp["goal_state"]["eval_history"][0]["decision"],
            "achieved"
        );
        assert_eq!(
            resp["goal_state"]["eval_history"][0]["checkpoint"],
            "terminal"
        );
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

    /// `GET /api/v1/history/{id}` on an unknown session must use the
    /// canonical nested error envelope, not the old flat
    /// `{"error": "<string>"}` shape. #251 (finding 2).
    #[actix_web::test]
    async fn history_not_found_uses_canonical_error_envelope() {
        let (state, _id) = app_state_with_session(vec![]).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/history/does-not-exist")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["error"]["message"], "Session not found");
        assert_eq!(body["session_id"], "does-not-exist");
    }

    /// The same nested history endpoint reachable via its canonical
    /// `/api/v1/sessions/{id}/history` path (#251 finding 4) returns
    /// identical data to the legacy flat `/api/v1/history/{id}` alias.
    #[actix_web::test]
    async fn history_is_reachable_via_canonical_nested_path() {
        let (state, id) = app_state_with_session(vec![Message::user("hi")]).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{id}/history"))
                .to_request(),
        )
        .await;
        assert_eq!(seqs(&resp["messages"]), vec!["hi"]);
    }
}
