use std::time::Duration;

use actix_web::{test, web, App};
use bamboo_agent_core::AgentEvent;

use super::response::plan_replay;
use crate::app_state::AppState;
use bamboo_engine::events::change_feed::ChangeEvent;
use bamboo_engine::events::journal;

fn deletion(id: &str) -> AgentEvent {
    AgentEvent::SessionDeleted {
        session_id: id.to_string(),
    }
}

/// Record `n` durable events into the sink and wait until they are journaled.
async fn seed(state: &AppState, n: u64) {
    for i in 1..=n {
        state
            .account_sink
            .record(Some(&format!("s{i}")), &deletion(&format!("s{i}")));
    }
    // Wait for the single writer task to assign + journal all of them.
    for _ in 0..100 {
        if state.account_sink.latest_seq() >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(state.account_sink.latest_seq(), n);
}

// ── plan_replay (pure resume/dedupe/reset decision) ────────────────────────

#[tokio::test]
async fn plan_replay_returns_events_after_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    seed(&state, 5).await;
    let events_dir = state.account_sink.events_dir();

    let plan = plan_replay(events_dir, 3, 5);
    assert!(plan.reset_from.is_none());
    assert_eq!(
        plan.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert_eq!(plan.last_replayed, 5);
}

#[tokio::test]
async fn plan_replay_from_zero_returns_all() {
    let dir = tempfile::tempdir().unwrap();
    let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    seed(&state, 4).await;

    let plan = plan_replay(state.account_sink.events_dir(), 0, 4);
    assert_eq!(
        plan.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(plan.last_replayed, 4);
}

#[tokio::test]
async fn plan_replay_triggers_feed_reset_when_cursor_predates_retained_window() {
    let dir = tempfile::tempdir().unwrap();
    // One file per event so retention can drop the early ones.
    let (mut j, _) =
        journal::EventJournal::open_with_threshold(dir.path().to_path_buf(), 1).unwrap();
    for seq in 1..=6 {
        j.append(&ChangeEvent {
            seq,
            ts: chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap(),
            session_id: Some(format!("s{seq}")),
            event: AgentEvent::SessionDeleted {
                session_id: format!("s{seq}"),
            },
        })
        .unwrap();
    }
    drop(j);

    // Retention keeps only the two newest files (seq 5, 6).
    journal::prune(dir.path(), 2).unwrap();
    assert_eq!(journal::oldest_seq(dir.path()).unwrap(), Some(5));

    // A cursor of 2 predates the retained window → reset directive, no stale
    // replay, fast-forward to the connect-time head (6).
    let plan = plan_replay(dir.path(), 2, 6);
    assert_eq!(plan.reset_from, Some(2));
    assert!(plan.events.is_empty());
    assert_eq!(plan.last_replayed, 6);
}

#[tokio::test]
async fn plan_replay_at_head_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    seed(&state, 5).await;

    let plan = plan_replay(state.account_sink.events_dir(), 5, 5);
    assert!(plan.reset_from.is_none());
    assert!(plan.events.is_empty());
    assert_eq!(plan.last_replayed, 5);
}

// ── End-to-end SSE resume over the real route ──────────────────────────────

/// Parse the `seq` of every `ChangeEvent` data frame in a collected SSE body,
/// ignoring keepalive/reset sentinels.
fn parse_seqs(body: &str) -> Vec<u64> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|payload| serde_json::from_str::<ChangeEvent>(payload).ok())
        .map(|ce| ce.seq)
        .collect()
}

#[actix_web::test]
async fn stream_resume_delivers_only_events_after_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());

    // 5 durable events, plus an ephemeral one that must never reach the feed.
    seed(&state, 5).await;
    state.account_sink.record(
        Some("s1"),
        &AgentEvent::Token {
            content: "x".into(),
        },
    );

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/stream", web::get().to(super::handler)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/stream?since=3")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream; charset=utf-8")
    );

    // Read the streaming body until we've seen seq 5 (Phase A emits it
    // immediately), bounded by a timeout so the infinite live tail can't hang.
    let mut body = std::pin::pin!(resp.into_body());
    let mut buf = String::new();
    let collect = async {
        loop {
            let chunk = std::future::poll_fn(|cx| {
                actix_web::body::MessageBody::poll_next(body.as_mut(), cx)
            })
            .await;
            match chunk {
                Some(Ok(bytes)) => {
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    if buf.contains("\"seq\":5") {
                        break;
                    }
                }
                _ => break,
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(3), collect).await;

    let seqs = parse_seqs(&buf);
    // Exactly the events after the cursor, in order, no gaps or duplicates.
    assert_eq!(seqs, vec![4, 5], "body was: {buf}");
    // Each frame carried its SSE id for Last-Event-ID resume.
    assert!(buf.contains("id: 4\n"));
    assert!(buf.contains("id: 5\n"));
    // The ephemeral Token (seq would-be) never appears.
    let journaled = journal::read_since(state.account_sink.events_dir(), 0).unwrap();
    assert_eq!(journaled.len(), 5, "ephemeral token must not be journaled");
}
