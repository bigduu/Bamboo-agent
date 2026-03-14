use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tempfile::tempdir;
use tokio::time::sleep;

use super::{
    record_round_and_session_error, record_round_started, record_session_completed_if_resolved,
    record_session_started,
};
use crate::agent::metrics::storage::MetricsStorage;
use crate::agent::metrics::types::{RoundStatus, SessionMetricsFilter, SessionStatus, TokenUsage};
use crate::agent::metrics::{MetricsCollector, SqliteMetricsStorage};

async fn create_collector_with_storage() -> (
    tempfile::TempDir,
    MetricsCollector,
    Arc<SqliteMetricsStorage>,
) {
    let dir = tempdir().expect("temp dir");
    let storage = Arc::new(SqliteMetricsStorage::new(dir.path().join("metrics.db")));
    storage.init().await.expect("init metrics storage");
    let collector = MetricsCollector::spawn(storage.clone(), 7);
    (dir, collector, storage)
}

async fn wait_for_session(
    storage: &SqliteMetricsStorage,
    session_id: &str,
) -> crate::agent::metrics::types::SessionMetrics {
    for _ in 0..100 {
        let sessions = storage
            .sessions(SessionMetricsFilter::default())
            .await
            .expect("sessions query");
        if let Some(session) = sessions.into_iter().find(|s| s.session_id == session_id) {
            return session;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("session not found: {session_id}");
}

async fn wait_for_session_detail(
    storage: &SqliteMetricsStorage,
    session_id: &str,
) -> crate::agent::metrics::types::SessionDetail {
    for _ in 0..100 {
        if let Some(detail) = storage
            .session_detail(session_id)
            .await
            .expect("session detail query")
        {
            if !detail.rounds.is_empty() {
                return detail;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("session detail with rounds not found: {session_id}");
}

async fn wait_for_session_status(
    storage: &SqliteMetricsStorage,
    session_id: &str,
    expected_status: SessionStatus,
) -> crate::agent::metrics::types::SessionMetrics {
    for _ in 0..100 {
        let session = wait_for_session(storage, session_id).await;
        if session.status == expected_status {
            return session;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("session {session_id} did not reach status {expected_status:?}");
}

async fn wait_for_session_state(
    storage: &SqliteMetricsStorage,
    session_id: &str,
    expected_status: SessionStatus,
    expected_message_count: u32,
) -> crate::agent::metrics::types::SessionMetrics {
    for _ in 0..100 {
        let session = wait_for_session(storage, session_id).await;
        if session.status == expected_status && session.message_count == expected_message_count {
            return session;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "session {session_id} did not reach status {expected_status:?} with message_count={expected_message_count}"
    );
}

#[tokio::test]
async fn record_session_started_writes_initial_session_metrics() {
    let (_dir, collector, storage) = create_collector_with_storage().await;
    let started_at = Utc::now();

    record_session_started(Some(&collector), "metrics-s1", "test-model", started_at, 3);

    let session = wait_for_session(storage.as_ref(), "metrics-s1").await;
    assert_eq!(session.model, "test-model");
    assert_eq!(session.message_count, 3);
    assert_eq!(session.status, SessionStatus::Running);
}

#[tokio::test]
async fn record_round_and_session_error_marks_round_and_session_as_error() {
    let (_dir, collector, storage) = create_collector_with_storage().await;
    let started_at = Utc::now();

    record_session_started(Some(&collector), "metrics-s2", "test-model", started_at, 1);
    record_round_started(Some(&collector), "metrics-r2", "metrics-s2", "test-model");
    record_round_and_session_error(
        Some(&collector),
        "metrics-r2",
        "metrics-s2",
        5,
        RoundStatus::Error,
        Some("boom".to_string()),
        SessionStatus::Error,
    );

    let session =
        wait_for_session_status(storage.as_ref(), "metrics-s2", SessionStatus::Error).await;
    assert_eq!(session.message_count, 5);

    let detail = wait_for_session_detail(storage.as_ref(), "metrics-s2").await;
    assert_eq!(detail.session.status, SessionStatus::Error);
    assert_eq!(detail.session.message_count, 5);
    assert_eq!(detail.rounds.len(), 1);
    assert_eq!(detail.rounds[0].status, RoundStatus::Error);
    assert_eq!(detail.rounds[0].error.as_deref(), Some("boom"));
    assert_eq!(detail.rounds[0].token_usage, TokenUsage::default());
}

#[tokio::test]
async fn record_session_completed_if_resolved_respects_pending_question_state() {
    let (_dir, collector, storage) = create_collector_with_storage().await;
    let started_at = Utc::now();

    record_session_started(Some(&collector), "metrics-s3", "test-model", started_at, 1);
    record_session_completed_if_resolved(Some(&collector), "metrics-s3", 9, true);

    let running =
        wait_for_session_state(storage.as_ref(), "metrics-s3", SessionStatus::Running, 9).await;
    assert_eq!(running.status, SessionStatus::Running);
    assert_eq!(running.message_count, 9);

    record_session_completed_if_resolved(Some(&collector), "metrics-s3", 10, false);
    let completed =
        wait_for_session_state(storage.as_ref(), "metrics-s3", SessionStatus::Completed, 10).await;
    assert_eq!(completed.message_count, 10);
}
