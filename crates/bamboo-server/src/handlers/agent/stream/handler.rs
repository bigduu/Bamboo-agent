//! Account-scoped change-feed SSE endpoint (`GET /api/v1/stream`).
//!
//! A single multiplexed feed of durable change events across *all* sessions,
//! resumable via `?since=<seq>` or the `Last-Event-ID` header (header wins, per
//! SSE reconnect semantics). Replaces the per-session polling the frontend used
//! to do for session-index, health, and pending-question state.

use actix_web::{web, HttpRequest, HttpResponse, Responder};
use serde::Deserialize;

use super::response::account_feed_response;
use crate::app_state::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct StreamQuery {
    /// Resume cursor: deliver events with `seq > since`. Absent/0 = from start.
    #[serde(default)]
    pub since: Option<u64>,
}

/// Subscribe to the account-wide change feed via Server-Sent Events.
///
/// `GET /api/v1/stream?since={seq}`
pub async fn handler(
    state: web::Data<AppState>,
    query: web::Query<StreamQuery>,
    req: HttpRequest,
) -> impl Responder {
    // `Last-Event-ID` (set automatically by the browser on reconnect) takes
    // precedence over the query parameter.
    let header_cursor = req
        .headers()
        .get("Last-Event-ID")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok());
    let since = header_cursor.or(query.since).unwrap_or(0);

    // Subscribe FIRST so events written during journal replay are buffered in
    // the ring and delivered in Phase B (no gap in the handoff).
    let receiver = state.account_sink.subscribe();
    let latest_at_start = state.account_sink.latest_seq();
    let events_dir = state.account_sink.events_dir().to_path_buf();

    tracing::debug!("account feed subscription opened (since={since}, latest={latest_at_start})");

    let response: HttpResponse =
        account_feed_response(since, latest_at_start, events_dir, receiver);
    response
}
