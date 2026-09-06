use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::time::{Duration as StdDuration, Instant};

use crate::storage::{MetricsMutation, SqliteMetricsStorage};
use crate::types::{MetricsDateFilter, PromptMemoryExposureItem, PromptMemoryRecallOutcome};

use super::*;

async fn reclaimed(storage: &Weak<SqliteMetricsStorage>, scheduler: &tokio::task::AbortHandle) {
    tokio::time::timeout(StdDuration::from_secs(30), async {
        while storage.strong_count() != 0 || !scheduler.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted local buffers and queued commands must drain before storage is released");
    assert!(storage.upgrade().is_none());
}

#[tokio::test]
async fn final_owner_drop_drains_additive_commands_at_every_segment_boundary() {
    for count in [31, 32, 33, 96] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metrics.db");
        let storage = Arc::new(SqliteMetricsStorage::new(&path));
        let weak = Arc::downgrade(&storage);
        let collector = MetricsCollector::spawn(storage, 90);
        let scheduler = collector._scheduler.task.abort_handle();
        let now = Utc::now();
        // Current-thread runtime: all N ordinary events are ready before init
        // or either spawned task can run. The scheduler is cancelled at Drop.
        for index in 0..count {
            collector.execute_sync_mismatch(
                if index % 2 == 0 { "even" } else { "odd" },
                now + Duration::milliseconds(i64::from(index)),
            );
        }
        drop(collector);
        reclaimed(&weak, &scheduler).await;
        let reopened = SqliteMetricsStorage::new(&path);
        let summary = reopened
            .summary(MetricsDateFilter::default())
            .await
            .unwrap();
        assert_eq!(summary.total_sync_mismatches, count as u64);
        assert_eq!(
            summary.sync_mismatch_breakdown["even"],
            ((count + 1) / 2) as u64
        );
        assert_eq!(summary.sync_mismatch_breakdown["odd"], (count / 2) as u64);
    }
}

fn observation(when: DateTime<Utc>, memory: &str) -> PromptMemoryExposureObservation {
    PromptMemoryExposureObservation {
        schema_version: 1,
        round_id: "reused-round".to_string(),
        session_id: "barriers".to_string(),
        project_id: Some("project".to_string()),
        observed_at: when,
        recall_enabled: true,
        query_present: true,
        recall_outcome: PromptMemoryRecallOutcome::Lexical,
        all_compact_exposed_count: 1,
        project_exposed_count: 1,
        out_of_project_only: false,
        compact_section_chars: 120,
        project_items: vec![PromptMemoryExposureItem {
            memory_id: memory.to_string(),
            scope: "project".to_string(),
            status_at_observation: "active".to_string(),
            rank: 1,
            rendered_chars: 60,
        }],
    }
}

#[tokio::test]
async fn singleton_barriers_preserve_fifo_across_segments_and_retention_cascades() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metrics.db");
    let storage = Arc::new(SqliteMetricsStorage::new(&path));
    storage.init().await.unwrap();
    let audit = rusqlite::Connection::open(&path).unwrap();
    audit
        .execute_batch(
            "CREATE TABLE compression_order_audit (status TEXT NOT NULL);
         CREATE TRIGGER audit_compression_order
         AFTER UPDATE OF compression_count ON round_metrics
         WHEN NEW.round_id = 'reused-round'
         BEGIN INSERT INTO compression_order_audit VALUES (NEW.status); END;",
        )
        .unwrap();
    drop(audit);
    let weak = Arc::downgrade(&storage);
    let collector = MetricsCollector::spawn(storage, 90);
    let scheduler = collector._scheduler.task.abort_handle();
    let now = Utc::now();
    let old = now - Duration::days(40);
    collector.session_started("barriers", "model", old);
    collector.round_started("reused-round", "barriers", "model", old);
    // Place the first exposure exactly after 31 ready ordinary commands.
    for _ in 0..29 {
        collector.execute_sync_mismatch("padding", now);
    }
    collector.prompt_memory_exposure(observation(old, "old-memory"));
    collector
        .tx
        .send(CollectorCommand::Prune {
            cutoff: now - Duration::days(30),
        })
        .unwrap();
    // Reusing the ID makes a deferred/reordered exposure observable: the old
    // snapshot must cascade away before the new round's first observation.
    collector.round_started("reused-round", "barriers", "model", now);
    collector.context_compressed("barriers", "reused-round", 4, 50, 80.0, 40.0, "test", 1);
    collector.round_completed(
        "reused-round",
        now,
        RoundStatus::Success,
        TokenUsage {
            prompt_tokens: 6,
            completion_tokens: 4,
            total_tokens: 10,
        },
        2,
        20,
        None,
    );
    let expected = observation(now, "new-memory");
    collector.prompt_memory_exposure(expected.clone());
    collector.session_message_count("barriers", 42, now);
    collector.session_completed("barriers", SessionStatus::Completed, now);
    drop(collector);
    reclaimed(&weak, &scheduler).await;
    let reopened = SqliteMetricsStorage::new(&path);
    assert_eq!(
        reopened
            .prompt_memory_exposure("reused-round")
            .await
            .unwrap(),
        Some(expected)
    );
    let detail = reopened.session_detail("barriers").await.unwrap().unwrap();
    assert_eq!(detail.rounds.len(), 1);
    assert_eq!(detail.rounds[0].started_at, now);
    assert_eq!(detail.rounds[0].status, RoundStatus::Success);
    assert_eq!(detail.rounds[0].compression_count, 1);
    assert_eq!(detail.rounds[0].tokens_saved, 70);
    assert_eq!(detail.session.total_rounds, 1);
    assert_eq!(detail.session.total_compression_events, 1);
    assert_eq!(detail.session.total_tokens_saved, 70);
    assert_eq!(detail.session.message_count, 42);
    assert_eq!(detail.session.status, SessionStatus::Completed);
    let audit = rusqlite::Connection::open(&path).unwrap();
    let statuses: Vec<String> = audit
        .prepare("SELECT status FROM compression_order_audit")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        statuses,
        ["running"],
        "compression must run exactly once before round completion"
    );
    let updated_at: String = audit
        .query_row(
            "SELECT updated_at FROM session_metrics WHERE session_id = 'barriers'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        updated_at,
        now.to_rfc3339(),
        "final explicit completion remains the last refresh"
    );
    assert_eq!(
        reopened
            .summary(MetricsDateFilter::default())
            .await
            .unwrap()
            .total_sync_mismatches,
        29
    );
}

#[tokio::test]
async fn failed_ordinary_and_special_commands_do_not_discard_accepted_suffix() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metrics.db");
    let storage = Arc::new(SqliteMetricsStorage::new(&path));
    let weak = Arc::downgrade(&storage);
    let collector = MetricsCollector::spawn(storage, 90);
    let scheduler = collector._scheduler.task.abort_handle();
    let now = Utc::now();
    collector.execute_sync_mismatch("kept", now);
    collector.round_completed(
        "missing",
        now,
        RoundStatus::Success,
        TokenUsage::default(),
        0,
        0,
        None,
    );
    collector.execute_sync_mismatch("kept", now);
    collector.prompt_memory_exposure(observation(now, "missing-round"));
    collector.context_compressed("missing", "missing", 1, 10, 80.0, 40.0, "test", 1);
    collector.execute_sync_mismatch("kept", now);
    collector.session_started("suffix", "model", now);
    collector.session_message_count("suffix", 71, now);
    drop(collector);
    reclaimed(&weak, &scheduler).await;
    let reopened = SqliteMetricsStorage::new(&path);
    assert_eq!(
        reopened
            .summary(MetricsDateFilter::default())
            .await
            .unwrap()
            .total_sync_mismatches,
        3
    );
    assert_eq!(
        reopened
            .session_detail("suffix")
            .await
            .unwrap()
            .unwrap()
            .session
            .message_count,
        71
    );
    assert!(reopened
        .prompt_memory_exposure("reused-round")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn burst_of_256_independent_sessions_drains_complete_rounds_and_tools() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metrics.db");
    let storage = Arc::new(SqliteMetricsStorage::new(&path));
    let weak = Arc::downgrade(&storage);
    let collector = MetricsCollector::spawn(storage, 90);
    let scheduler = collector._scheduler.task.abort_handle();
    let now = Utc::now();
    let started = Instant::now();
    for index in 0..256 {
        let session = format!("session-{index}");
        let round = format!("round-{index}");
        let tool = format!("tool-{index}");
        collector.session_started(&session, "model", now);
        collector.round_started(&round, &session, "model", now);
        collector
            .tx
            .send(CollectorCommand::Mutation(MetricsMutation::ToolStarted {
                tool_call_id: tool.clone(),
                round_id: round.clone(),
                session_id: session.clone(),
                tool_name: "test_tool".to_string(),
                started_at: now,
            }))
            .unwrap();
        collector
            .tx
            .send(CollectorCommand::Mutation(MetricsMutation::ToolCompleted {
                tool_call_id: tool,
                completion: ToolCallCompletion {
                    completed_at: now,
                    success: true,
                    error: None,
                },
            }))
            .unwrap();
        collector.round_completed(
            &round,
            now,
            RoundStatus::Success,
            TokenUsage {
                prompt_tokens: 6,
                completion_tokens: 4,
                total_tokens: 10,
            },
            0,
            0,
            None,
        );
        collector.session_completed(&session, SessionStatus::Completed, now);
    }
    drop(collector);
    reclaimed(&weak, &scheduler).await;
    let elapsed = started.elapsed();
    let reopened = SqliteMetricsStorage::new(&path);
    let summary = reopened
        .summary(MetricsDateFilter::default())
        .await
        .unwrap();
    assert_eq!(summary.total_sessions, 256);
    assert_eq!(summary.completed_sessions, 256);
    assert_eq!(summary.total_tokens.total_tokens, 2560);
    assert_eq!(summary.total_tool_calls, 256);
    for index in 0..256 {
        let detail = reopened
            .session_detail(&format!("session-{index}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detail.rounds.len(), 1);
        assert_eq!(detail.rounds[0].round_id, format!("round-{index}"));
        assert_eq!(detail.rounds[0].tool_calls.len(), 1);
        assert_eq!(
            detail.rounds[0].tool_calls[0].tool_call_id,
            format!("tool-{index}")
        );
        assert_eq!(detail.rounds[0].status, RoundStatus::Success);
        assert_eq!(detail.session.status, SessionStatus::Completed);
        assert_eq!(detail.session.tool_call_count, 1);
    }
    eprintln!("256-session collector burst: 1536 ready ordinary commands drained in {elapsed:?}");
}

#[tokio::test]
async fn final_owner_drop_preserves_in_flight_segment_and_channel_suffix() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metrics.db");
    let storage = Arc::new(SqliteMetricsStorage::new(&path));
    storage.init().await.unwrap();
    let now = Utc::now();
    let old = now - Duration::days(100);
    storage
        .upsert_session_start("stale", "model", old)
        .await
        .unwrap();
    storage
        .insert_round_start("stale-round", "stale", "model", old)
        .await
        .unwrap();
    let collector = MetricsCollector::spawn(storage.clone(), 90);
    let scheduler = collector._scheduler.task.abort_handle();
    collector.session_started("ready", "model", now);
    collector.session_message_count("ready", 42, now);
    // Observe both initialization and the scheduler's real initial prune before
    // acquiring the writer. No timer command can precede the tested segment.
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            let ready = storage.session_detail("ready").await.unwrap();
            let stale = storage.session_detail("stale").await.unwrap().unwrap();
            if ready.is_some_and(|detail| detail.session.message_count == 42)
                && stale.rounds.is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let received_before = collector.received_count.load(Ordering::SeqCst);
    for _ in 0..96 {
        collector.execute_sync_mismatch("in-flight", now);
    }
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while collector.received_count.load(Ordering::SeqCst) == received_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("consumer must take a ready segment while the SQLite writer is occupied");
    assert_eq!(
        collector.received_count.load(Ordering::SeqCst) - received_before,
        32,
        "only 32 commands may leave the channel before the blocked segment completes"
    );
    let weak = Arc::downgrade(&storage);
    drop(storage);
    drop(collector);
    tokio::task::yield_now().await;
    assert!(
        weak.upgrade().is_some(),
        "consumer retains storage until in-flight and queued commands finish"
    );
    writer.execute_batch("COMMIT").unwrap();
    drop(writer);
    reclaimed(&weak, &scheduler).await;
    let reopened = SqliteMetricsStorage::new(&path);
    assert_eq!(
        reopened
            .summary(MetricsDateFilter::default())
            .await
            .unwrap()
            .total_sync_mismatches,
        96
    );
    assert_eq!(
        reopened
            .session_detail("ready")
            .await
            .unwrap()
            .unwrap()
            .session
            .message_count,
        42
    );
}
