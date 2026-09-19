//! Public event-path acceptance for session aggregate refreshes.
use std::path::{Path, PathBuf};

use bamboo_infrastructure::metrics::storage::{
    MetricsError, MetricsMutation, MetricsResult, MetricsStorage, SqliteMetricsStorage,
    ToolCallCompletion, MAX_METRICS_BATCH_SIZE,
};
use bamboo_infrastructure::metrics::types::{
    PromptMemoryExposureItem, PromptMemoryExposureObservation, PromptMemoryRecallOutcome,
    RoundStatus, SessionStatus, TokenUsage,
};
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{types::Value, Connection};
use tempfile::TempDir;

fn at(second: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + second, 0).unwrap()
}

fn start(session: &str) -> MetricsMutation {
    MetricsMutation::SessionStarted {
        session_id: session.into(),
        model: format!("model-{session}"),
        started_at: at(-100),
    }
}

fn round(id: &str, session: &str, second: i64) -> MetricsMutation {
    MetricsMutation::RoundStarted {
        round_id: id.into(),
        session_id: session.into(),
        model: "round-model".into(),
        started_at: at(second),
    }
}

fn done(id: &str, tokens: [u64; 3], cached: [u32; 2], second: i64) -> MetricsMutation {
    MetricsMutation::RoundCompleted {
        round_id: id.into(),
        completed_at: at(second),
        status: RoundStatus::Success,
        usage: TokenUsage {
            prompt_tokens: tokens[0],
            completion_tokens: tokens[1],
            total_tokens: tokens[2],
        },
        prompt_cached_tool_outputs: cached[0],
        prompt_cached_tool_tokens_saved: cached[1],
        error: None,
    }
}

fn tool(id: &str, round: &str, session: &str, second: i64) -> MetricsMutation {
    MetricsMutation::ToolStarted {
        tool_call_id: id.into(),
        round_id: round.into(),
        session_id: session.into(),
        tool_name: format!("name-{id}"),
        started_at: at(second),
    }
}

// Invoke the public singleton methods independently of MetricsMutation::apply.
async fn direct(storage: &SqliteMetricsStorage, event: MetricsMutation) -> MetricsResult<()> {
    match event {
        MetricsMutation::SessionStarted {
            session_id,
            model,
            started_at,
        } => {
            storage
                .upsert_session_start(&session_id, &model, started_at)
                .await
        }
        MetricsMutation::SessionCompleted {
            session_id,
            status,
            completed_at,
        } => {
            storage
                .complete_session(&session_id, status, completed_at)
                .await
        }
        MetricsMutation::RoundStarted {
            round_id,
            session_id,
            model,
            started_at,
        } => {
            storage
                .insert_round_start(&round_id, &session_id, &model, started_at)
                .await
        }
        MetricsMutation::RoundCompleted {
            round_id,
            completed_at,
            status,
            usage,
            prompt_cached_tool_outputs,
            prompt_cached_tool_tokens_saved,
            error,
        } => {
            storage
                .complete_round(
                    &round_id,
                    completed_at,
                    status,
                    usage,
                    prompt_cached_tool_outputs,
                    prompt_cached_tool_tokens_saved,
                    error,
                )
                .await
        }
        MetricsMutation::ToolStarted {
            tool_call_id,
            round_id,
            session_id,
            tool_name,
            started_at,
        } => {
            storage
                .insert_tool_start(
                    &tool_call_id,
                    &round_id,
                    &session_id,
                    &tool_name,
                    started_at,
                )
                .await
        }
        MetricsMutation::ToolCompleted {
            tool_call_id,
            completion,
        } => storage.complete_tool_call(&tool_call_id, completion).await,
        other => panic!("unneeded event in aggregate fixture: {other:?}"),
    }
}

fn rows(connection: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| (0..columns).map(|i| row.get(i)).collect())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn snapshot(path: &Path, processing_time_sessions: &[&str]) -> Vec<Vec<Vec<Value>>> {
    let connection = Connection::open(path).unwrap();
    [
        "session_metrics", "round_metrics", "tool_call_metrics",
        "prompt_memory_round_observations", "prompt_memory_project_exposures",
        "forward_request_metrics", "execute_sync_mismatch_metrics",
    ].into_iter().map(|table| {
        let mut data = rows(&connection, &format!("SELECT * FROM {table} ORDER BY rowid"));
        if table == "session_metrics" && !processing_time_sessions.is_empty() {
            let statement = connection.prepare("SELECT * FROM session_metrics").unwrap();
            let updated_at = statement.column_names().iter().position(|name| *name == "updated_at").unwrap();
            for row in &mut data {
                if matches!(&row[0], Value::Text(id) if processing_time_sessions.contains(&id.as_str())) {
                    row[updated_at] = Value::Text("bounded prune processing time".into());
                }
            }
        }
        data
    }).collect()
}

struct Stores {
    _directory: TempDir,
    paths: [PathBuf; 2],
    direct: SqliteMetricsStorage,
    batch: SqliteMetricsStorage,
}

impl Stores {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let paths = [
            directory.path().join("direct.db"),
            directory.path().join("batch.db"),
        ];
        let direct = SqliteMetricsStorage::new(&paths[0]);
        let batch = SqliteMetricsStorage::new(&paths[1]);
        direct.init().await.unwrap();
        batch.init().await.unwrap();
        Self {
            _directory: directory,
            paths,
            direct,
            batch,
        }
    }

    async fn segment(&self, events: Vec<MetricsMutation>) -> Vec<MetricsResult<()>> {
        let mut expected = Vec::new();
        for event in events.iter().cloned() {
            expected.push(direct(&self.direct, event).await);
        }
        let actual = self.batch.apply_batch(events).await.unwrap();
        assert_eq!(actual.len(), expected.len());
        for (left, right) in expected.iter().zip(&actual) {
            assert_eq!(
                left.as_ref().err().map(ToString::to_string),
                right.as_ref().err().map(ToString::to_string)
            );
        }
        self.assert_same(&[]);
        actual
    }

    async fn ok(&self, events: Vec<MetricsMutation>) {
        for result in self.segment(events).await {
            result.unwrap();
        }
    }

    fn assert_same(&self, processing_time_sessions: &[&str]) {
        assert_eq!(
            snapshot(&self.paths[0], processing_time_sessions),
            snapshot(&self.paths[1], processing_time_sessions)
        );
    }

    fn totals(&self, session: &str, expected: [i64; 9]) {
        for path in &self.paths {
            let connection = Connection::open(path).unwrap();
            let actual = connection
                .query_row(
                    "SELECT total_rounds, prompt_tokens, completion_tokens, total_tokens,
                 prompt_cached_tool_outputs, prompt_cached_tool_tokens_saved,
                 total_compression_events, total_tokens_saved, tool_call_count
                 FROM session_metrics WHERE session_id = ?1",
                    [session],
                    |row| {
                        (0..9)
                            .map(|i| row.get::<_, i64>(i))
                            .collect::<Result<Vec<_>, _>>()
                    },
                )
                .unwrap();
            assert_eq!(actual, expected, "session {session}");
        }
    }
}

#[tokio::test]
async fn completion_replay_preserves_compression_detail_order_and_unrelated_session() {
    let stores = Stores::new().await;
    stores
        .ok(vec![start("empty"), start("active"), start("sentinel")])
        .await;
    stores
        .ok(vec![MetricsMutation::SessionCompleted {
            session_id: "empty".into(),
            status: SessionStatus::Completed,
            completed_at: at(0),
        }])
        .await;
    stores.totals("empty", [0; 9]);
    assert!(stores
        .direct
        .session_detail("empty")
        .await
        .unwrap()
        .unwrap()
        .rounds
        .is_empty());
    for path in &stores.paths {
        Connection::open(path)
            .unwrap()
            .execute_batch(
                "UPDATE session_metrics SET total_rounds=41, prompt_tokens=42,
             completion_tokens=43, total_tokens=44, prompt_cached_tool_outputs=45,
             prompt_cached_tool_tokens_saved=46, total_compression_events=47,
             total_tokens_saved=48, tool_call_count=49, message_count=50
             WHERE session_id='sentinel'",
            )
            .unwrap();
    }
    let sentinel = rows(
        &Connection::open(&stores.paths[0]).unwrap(),
        "SELECT * FROM session_metrics WHERE session_id='sentinel'",
    );
    stores
        .ok(vec![
            round("later", "active", 20),
            round("earlier", "active", 10),
        ])
        .await;
    stores.totals("active", [2, 0, 0, 0, 0, 0, 0, 0, 0]);
    stores
        .ok(vec![
            done("later", [20, 5, 25], [1, 2], 100),
            done("earlier", [10, 4, 14], [2, 3], 100),
        ])
        .await;
    stores.totals("active", [2, 30, 9, 39, 3, 5, 0, 5, 0]);
    stores
        .ok(vec![
            tool("tool-later", "earlier", "active", 14),
            tool("tool-earlier", "earlier", "active", 12),
        ])
        .await;
    // Tool starts retain the existing aggregate refresh position.
    stores.totals("active", [2, 30, 9, 39, 3, 5, 0, 5, 0]);
    stores
        .ok(vec![MetricsMutation::ToolCompleted {
            tool_call_id: "tool-later".into(),
            completion: ToolCallCompletion {
                completed_at: at(16),
                success: false,
                error: Some("tool failed".into()),
            },
        }])
        .await;
    stores.totals("active", [2, 30, 9, 39, 3, 5, 0, 5, 2]);
    for storage in [&stores.direct, &stores.batch] {
        storage
            .record_round_compression("earlier", at(110), 5)
            .await
            .unwrap();
    }
    stores.assert_same(&[]);
    stores.totals("active", [2, 30, 9, 39, 3, 5, 1, 10, 2]);
    let replay = done("earlier", [12, 6, 18], [4, 7], 120);
    stores.ok(vec![replay.clone()]).await;
    stores.totals("active", [2, 32, 11, 43, 5, 9, 1, 14, 2]);
    let before_replay = snapshot(&stores.paths[0], &[]);
    stores.ok(vec![replay; MAX_METRICS_BATCH_SIZE + 1]).await;
    assert_eq!(snapshot(&stores.paths[0], &[]), before_replay);
    stores
        .ok(vec![MetricsMutation::SessionCompleted {
            session_id: "active".into(),
            status: SessionStatus::Completed,
            completed_at: at(200),
        }])
        .await;
    stores.totals("active", [2, 32, 11, 43, 5, 9, 1, 14, 2]);
    let detail = stores
        .direct
        .session_detail("active")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        Some(detail.clone()),
        stores.batch.session_detail("active").await.unwrap()
    );
    assert_eq!(detail.session.status, SessionStatus::Completed);
    assert_eq!(detail.session.total_token_usage.total_tokens, 43);
    assert_eq!(
        detail
            .rounds
            .iter()
            .map(|r| r.round_id.as_str())
            .collect::<Vec<_>>(),
        ["earlier", "later"]
    );
    let first = &detail.rounds[0];
    assert_eq!(
        (
            first.prompt_cached_tool_tokens_saved,
            first.compression_count,
            first.tokens_saved
        ),
        (7, 1, 12)
    );
    assert_eq!(
        first
            .tool_calls
            .iter()
            .map(|t| t.tool_call_id.as_str())
            .collect::<Vec<_>>(),
        ["tool-earlier", "tool-later"]
    );
    assert_eq!(first.tool_calls[0].success, None);
    assert_eq!(first.tool_calls[1].success, Some(false));
    assert_eq!(first.tool_calls[1].error.as_deref(), Some("tool failed"));
    assert_eq!(first.tool_calls[1].duration_ms, Some(2_000));
    for path in &stores.paths {
        assert_eq!(
            rows(
                &Connection::open(path).unwrap(),
                "SELECT * FROM session_metrics WHERE session_id='sentinel'"
            ),
            sentinel
        );
    }
}

#[tokio::test]
async fn extreme_tokens_saturate_and_failed_completion_rolls_back_before_suffix_repair() {
    let stores = Stores::new().await;
    stores
        .ok(vec![
            start("s"),
            round("bad", "s", 1),
            round("target", "s", 2),
        ])
        .await;
    stores
        .ok(vec![done(
            "bad",
            [u64::MAX, u64::MAX - 1, u64::MAX - 2],
            [1, 2],
            10,
        )])
        .await;
    stores.ok(vec![done("target", [1, 2, 3], [2, 3], 11)]).await;
    stores.totals("s", [2, i64::MAX, i64::MAX, i64::MAX, 3, 5, 0, 5, 0]);
    for path in &stores.paths {
        let connection = Connection::open(path).unwrap();
        assert_eq!(rows(&connection,
            "SELECT prompt_tokens, completion_tokens, total_tokens FROM round_metrics WHERE round_id='bad'"),
            vec![vec![Value::Integer(i64::MAX); 3]]);
        connection
            .execute(
                "UPDATE round_metrics SET prompt_tokens=-1 WHERE round_id='bad'",
                [],
            )
            .unwrap();
    }
    let before_failure = snapshot(&stores.paths[0], &[]);
    let mut failure = done("target", [100, 200, 300], [4, 7], 200);
    if let MetricsMutation::RoundCompleted { status, error, .. } = &mut failure {
        *status = RoundStatus::Error;
        *error = Some("must roll back".into());
    }
    let failed = stores.segment(vec![failure.clone()]).await;
    assert!(
        matches!(&failed[0], Err(MetricsError::InvalidData(message)) if message.contains("prompt_tokens"))
    );
    for path in &stores.paths {
        // Includes child status, usage, cache savings, error and completion time,
        // plus the parent aggregate and updated_at before the rejected event.
        assert_eq!(snapshot(path, &[]), before_failure);
    }
    let suffix = stores
        .segment(vec![
            failure,
            done("bad", [10, 20, 30], [1, 2], 300),
            done("target", [100, 200, 300], [4, 7], 400),
        ])
        .await;
    assert!(matches!(&suffix[0], Err(MetricsError::InvalidData(_))));
    assert!(suffix[1..].iter().all(Result::is_ok));
    stores.totals("s", [2, 110, 220, 330, 5, 9, 0, 9, 0]);
    for storage in [&stores.direct, &stores.batch] {
        let detail = storage.session_detail("s").await.unwrap().unwrap();
        assert!(detail
            .rounds
            .iter()
            .all(|round| round.status == RoundStatus::Success && round.error.is_none()));
        assert_eq!(detail.rounds[1].completed_at, Some(at(400)));
        assert_eq!(detail.rounds[1].token_usage.total_tokens, 300);
    }
}

fn exposure(round_id: &str, session_id: &str) -> PromptMemoryExposureObservation {
    PromptMemoryExposureObservation {
        schema_version: 1,
        round_id: round_id.into(),
        session_id: session_id.into(),
        project_id: Some("project".into()),
        observed_at: at(50),
        recall_enabled: true,
        query_present: true,
        recall_outcome: PromptMemoryRecallOutcome::Lexical,
        all_compact_exposed_count: 1,
        project_exposed_count: 1,
        out_of_project_only: false,
        compact_section_chars: 20,
        project_items: vec![PromptMemoryExposureItem {
            memory_id: format!("memory-{round_id}"),
            scope: "project".into(),
            status_at_observation: "active".into(),
            rank: 1,
            rendered_chars: 10,
        }],
    }
}

#[tokio::test]
async fn retention_refreshes_surviving_and_empty_sessions_and_cascades_round_children() {
    let stores = Stores::new().await;
    stores
        .ok(vec![
            start("kept"),
            start("emptied"),
            start("untouched"),
            round("old", "kept", -20),
            round("recent", "kept", 20),
            round("only-old", "emptied", -10),
        ])
        .await;
    stores
        .ok(vec![
            done("old", [10, 1, 11], [1, 3], 100),
            done("recent", [20, 2, 22], [1, 4], 100),
            done("only-old", [30, 3, 33], [1, 5], 100),
        ])
        .await;
    stores
        .ok(vec![
            tool("old-tool", "old", "kept", -19),
            tool("recent-tool", "recent", "kept", 21),
            tool("only-old-tool", "only-old", "emptied", -9),
        ])
        .await;
    for (round_id, session_id) in [("old", "kept"), ("recent", "kept"), ("only-old", "emptied")] {
        let observation = exposure(round_id, session_id);
        for storage in [&stores.direct, &stores.batch] {
            storage
                .record_prompt_memory_exposure(&observation)
                .await
                .unwrap();
        }
        stores.assert_same(&[]);
    }
    stores
        .ok(vec![
            MetricsMutation::SessionCompleted {
                session_id: "kept".into(),
                status: SessionStatus::Completed,
                completed_at: at(200),
            },
            MetricsMutation::SessionCompleted {
                session_id: "emptied".into(),
                status: SessionStatus::Completed,
                completed_at: at(200),
            },
        ])
        .await;
    stores.totals("kept", [2, 30, 3, 33, 2, 7, 0, 7, 2]);
    stores.totals("emptied", [1, 30, 3, 33, 1, 5, 0, 5, 1]);
    let untouched = rows(
        &Connection::open(&stores.paths[0]).unwrap(),
        "SELECT * FROM session_metrics WHERE session_id='untouched'",
    );
    for (storage, path) in [
        (&stores.direct, &stores.paths[0]),
        (&stores.batch, &stores.paths[1]),
    ] {
        let before = Utc::now();
        assert_eq!(storage.prune_rounds_before(at(0)).await.unwrap(), 2);
        let after = Utc::now();
        let connection = Connection::open(path).unwrap();
        // Only these two processing-time values are normalized below; every
        // event timestamp and the unaffected session's timestamp remains exact.
        for session in ["kept", "emptied"] {
            let raw: String = connection
                .query_row(
                    "SELECT updated_at FROM session_metrics WHERE session_id=?1",
                    [session],
                    |row| row.get(0),
                )
                .unwrap();
            let actual = DateTime::parse_from_rfc3339(&raw)
                .unwrap()
                .with_timezone(&Utc);
            assert!(before <= actual && actual <= after);
        }
        assert_eq!(
            rows(
                &connection,
                "SELECT * FROM session_metrics WHERE session_id='untouched'"
            ),
            untouched
        );
        for table in [
            "round_metrics",
            "tool_call_metrics",
            "prompt_memory_round_observations",
            "prompt_memory_project_exposures",
        ] {
            assert_eq!(
                rows(&connection, &format!("SELECT round_id FROM {table}")),
                vec![vec![Value::Text("recent".into())]]
            );
        }
        assert_eq!(storage.prompt_memory_exposure("old").await.unwrap(), None);
        assert_eq!(
            storage.prompt_memory_exposure("recent").await.unwrap(),
            Some(exposure("recent", "kept"))
        );
        let before_noop = snapshot(path, &[]);
        assert_eq!(storage.prune_rounds_before(at(0)).await.unwrap(), 0);
        assert_eq!(snapshot(path, &[]), before_noop);
    }
    stores.assert_same(&["kept", "emptied"]);
    stores.totals("kept", [1, 20, 2, 22, 1, 4, 0, 4, 1]);
    stores.totals("emptied", [0; 9]);
    let kept = stores.direct.session_detail("kept").await.unwrap().unwrap();
    assert_eq!(
        Some(kept.clone()),
        stores.batch.session_detail("kept").await.unwrap()
    );
    assert_eq!(kept.rounds.len(), 1);
    assert_eq!(kept.rounds[0].round_id, "recent");
    assert_eq!(kept.rounds[0].tool_calls[0].tool_call_id, "recent-tool");
    for storage in [&stores.direct, &stores.batch] {
        let empty = storage.session_detail("emptied").await.unwrap().unwrap();
        assert!(empty.rounds.is_empty());
        assert_eq!(empty.session.status, SessionStatus::Completed);
        assert_eq!(empty.session.completed_at, Some(at(200)));
    }
}
