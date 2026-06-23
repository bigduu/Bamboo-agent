//! SSE response builder for the account change feed.
//!
//! Implements the subscribe-first replay→live handoff that guarantees a
//! resuming client sees every event after its cursor exactly once:
//!
//! 1. The caller subscribes to the live broadcast *before* this runs, so any
//!    event written during journal replay is already buffered in the ring.
//! 2. Phase A replays the durable journal from the client's `since`.
//! 3. Phase B switches to the live tail, skipping any `seq <= last_replayed`
//!    (the overlap between journal and ring), so nothing is duplicated.
//! 4. On `Lagged` (ring overran during a slow replay) it re-seeks the journal
//!    from `last_replayed` — the durable backstop — then continues live.

use std::path::{Path, PathBuf};
use std::time::Duration;

use actix_web::http::header;
use actix_web::{web, HttpResponse};
use tokio::sync::broadcast;

use bamboo_engine::events::change_feed::ChangeEvent;
use bamboo_engine::events::journal;

/// The events to emit when (re)seeking the journal from a cursor, plus the new
/// `last_replayed` watermark and whether a `feed_reset` was needed.
///
/// Pure and deterministic so the resume handoff can be unit-tested without an
/// HTTP server.
pub(crate) struct ReplayPlan {
    /// If `Some`, the client's cursor predated the retained window; it must
    /// drop local state and full-resync. The contained value is the stale `since`.
    pub reset_from: Option<u64>,
    /// Journaled events with `seq > cursor`, in order.
    pub events: Vec<ChangeEvent>,
    /// The seq watermark after emitting `events` (max seq seen, or the
    /// fast-forward target on reset).
    pub last_replayed: u64,
}

/// Compute the journal replay for a connecting/lagging client.
///
/// * On a retained cursor: returns all events with `seq > since`.
/// * On a cursor below the retained window: returns a reset directive and
///   fast-forwards `last_replayed` to `latest_at_start` (the client resyncs via
///   REST and the live tail serves anything newer).
pub(crate) fn plan_replay(events_dir: &Path, since: u64, latest_at_start: u64) -> ReplayPlan {
    if since > 0 {
        if let Ok(Some(oldest)) = journal::oldest_seq(events_dir) {
            if oldest > since + 1 {
                return ReplayPlan {
                    reset_from: Some(since),
                    events: Vec::new(),
                    last_replayed: latest_at_start,
                };
            }
        }
    }

    let mut events = match journal::read_since(events_dir, since) {
        Ok(events) => events,
        Err(e) => {
            tracing::error!("account feed: journal replay failed from {since}: {e}");
            Vec::new()
        }
    };
    events.retain(|ce| ce.seq > since);
    let last_replayed = events.last().map(|ce| ce.seq).unwrap_or(since);
    ReplayPlan {
        reset_from: None,
        events,
        last_replayed,
    }
}

/// Build the streaming SSE response for `GET /api/v1/stream`.
///
/// * `since` — the client's last-seen seq (0 = from the beginning).
/// * `latest_at_start` — the max seq at connect time, used to fast-forward past
///   a `feed_reset` without replaying the (now-stale) journal.
/// * `events_dir` — journal directory for stateless replay reads.
/// * `receiver` — a broadcast receiver already subscribed by the caller.
pub(super) fn account_feed_response(
    since: u64,
    latest_at_start: u64,
    events_dir: PathBuf,
    mut receiver: broadcast::Receiver<std::sync::Arc<ChangeEvent>>,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream; charset=utf-8"))
        .append_header((header::CACHE_CONTROL, "no-cache, no-transform"))
        .append_header((header::CONNECTION, "keep-alive"))
        .append_header(("X-Accel-Buffering", "no"))
        .streaming(async_stream::stream! {
            // Phase A: replay the durable journal from the cursor (with a
            // feed_reset directive when the cursor predates the retained window).
            let plan = plan_replay(&events_dir, since, latest_at_start);
            if let Some(from) = plan.reset_from {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(reset_frame(from)));
            }
            let mut last_replayed = plan.last_replayed;
            for ce in plan.events {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(change_frame(&ce)));
            }

            // Phase B: live tail with overlap-dedupe and lagged re-seek.
            let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
            heartbeat.tick().await; // skip the immediate tick

            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        yield Ok::<_, actix_web::Error>(web::Bytes::from(keepalive_frame()));
                    }
                    recv = receiver.recv() => {
                        match recv {
                            Ok(ce) => {
                                if ce.seq <= last_replayed {
                                    continue; // dedupe the replay/live overlap
                                }
                                yield Ok::<_, actix_web::Error>(web::Bytes::from(change_frame(&ce)));
                                last_replayed = ce.seq;
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                // Ring overran during a slow consumer; recover the
                                // gap from the durable journal, then continue live.
                                if let Ok(events) = journal::read_since(&events_dir, last_replayed) {
                                    for ce in events {
                                        if ce.seq <= last_replayed {
                                            continue;
                                        }
                                        yield Ok::<_, actix_web::Error>(web::Bytes::from(change_frame(&ce)));
                                        last_replayed = ce.seq;
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        })
}

/// Serialize a change event as an SSE frame with its `id:` (the seq), so the
/// browser `EventSource` auto-tracks `Last-Event-ID` for resume.
fn change_frame(ce: &ChangeEvent) -> String {
    match serde_json::to_string(ce) {
        Ok(json) => format!("id: {}\ndata: {}\n\n", ce.seq, json),
        Err(_) => String::new(),
    }
}

/// Control frame instructing the client to drop local state and full-resync.
fn reset_frame(from_seq: u64) -> String {
    format!("data: {{\"type\":\"feed_reset\",\"from_seq\":{from_seq}}}\n\n")
}

fn keepalive_frame() -> &'static str {
    "data: [KEEPALIVE]\n\n"
}
