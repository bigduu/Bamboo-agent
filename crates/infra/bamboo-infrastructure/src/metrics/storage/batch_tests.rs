use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::TimeZone;
use rusqlite::types::Value;
use tempfile::TempDir;

use super::batch_probe::Probe;
use super::*;

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + seconds, 0).unwrap()
}
fn start(id: &str) -> MetricsMutation {
    MetricsMutation::SessionStarted {
        session_id: id.into(),
        model: format!("model-{id}"),
        started_at: at(0),
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
fn done(id: &str, tokens: u64, saved: u32) -> MetricsMutation {
    MetricsMutation::RoundCompleted {
        round_id: id.into(),
        completed_at: at(10),
        status: RoundStatus::Success,
        usage: TokenUsage {
            prompt_tokens: tokens,
            completion_tokens: 0,
            total_tokens: tokens,
        },
        prompt_cached_tool_outputs: 2,
        prompt_cached_tool_tokens_saved: saved,
        error: None,
    }
}
fn mismatch(reason: &str, second: i64) -> MetricsMutation {
    MetricsMutation::ExecuteSyncMismatch {
        reason: reason.into(),
        occurred_at: at(second),
    }
}
fn tool(round: &str, session: &str) -> MetricsMutation {
    MetricsMutation::ToolStarted {
        tool_call_id: "tool".into(),
        round_id: round.into(),
        session_id: session.into(),
        tool_name: format!("tool-{session}"),
        started_at: at(11),
    }
}

async fn store() -> (TempDir, SqliteMetricsStorage, Arc<Probe>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metrics.db");
    let storage = SqliteMetricsStorage::new(&path);
    storage.init().await.unwrap();
    let probe = Probe::install(&path);
    (dir, storage, probe)
}
fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut statement = conn.prepare(sql).unwrap();
    let count = statement.column_count();
    statement
        .query_map([], |row| (0..count).map(|column| row.get(column)).collect())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn snapshot(storage: &SqliteMetricsStorage) -> Vec<(String, Vec<Vec<Value>>)> {
    let conn = Connection::open(&storage.db_path).unwrap();
    let tables = conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .unwrap().query_map([], |row| row.get::<_, String>(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    tables
        .into_iter()
        .map(|table| {
            let data = rows(&conn, &format!("SELECT * FROM {table} ORDER BY rowid"));
            (table, data)
        })
        .collect()
}
async fn compare(
    serial: &SqliteMetricsStorage,
    batch: &SqliteMetricsStorage,
    items: Vec<MetricsMutation>,
) {
    let mut reference = Vec::new();
    for item in items.iter().cloned() {
        reference.push(item.apply(serial).await);
    }
    let actual = batch.apply_batch(items).await.unwrap();
    assert_eq!(
        actual
            .iter()
            .map(|r| r.as_ref().err().map(std::mem::discriminant))
            .collect::<Vec<_>>(),
        reference
            .iter()
            .map(|r| r.as_ref().err().map(std::mem::discriminant))
            .collect::<Vec<_>>()
    );
    assert_eq!(snapshot(serial), snapshot(batch));
}

#[tokio::test]
async fn serial_oracle_preserves_replay_ownership_aggregate_positions_and_raw_sqlite_conversions() {
    let (_sd, serial, sp) = store().await;
    let (_bd, batch, bp) = store().await;
    let forward_start = MetricsMutation::ForwardStarted {
        forward_id: "forward".into(),
        endpoint: "endpoint".into(),
        model: "fmodel".into(),
        is_stream: true,
        started_at: at(1),
    };
    let forward_done = |usage, token_details| MetricsMutation::ForwardCompleted {
        forward_id: "forward".into(),
        completed_at: at(20),
        status_code: Some(200),
        status: ForwardStatus::Success,
        usage,
        token_details,
        error: None,
    };
    compare(
        &serial,
        &batch,
        vec![
            start("a"),
            start("b"),
            round("ra", "a", 1),
            round("rb", "b", 1),
            done("ra", u64::MAX, 3),
            tool("ra", "a"),
            forward_start.clone(),
            forward_done(
                Some(TokenUsage {
                    prompt_tokens: u64::MAX,
                    completion_tokens: u64::MAX,
                    total_tokens: u64::MAX,
                }),
                Some(ForwardTokenDetails {
                    cache_creation_input_tokens: Some(u64::MAX),
                    cache_read_input_tokens: Some(u64::MAX),
                    cache_write_input_tokens: Some(u64::MAX),
                    reasoning_output_tokens: Some(u64::MAX),
                }),
            ),
            mismatch("additive", 4),
            mismatch("additive", 3),
        ],
    )
    .await;
    let conn = Connection::open(&batch.db_path).unwrap();
    // RoundCompleted clamps, while forward fields intentionally retain their
    // pre-existing signed casts. ToolStarted does not refresh session counters.
    assert_eq!(
        rows(
            &conn,
            "SELECT total_tokens, tool_call_count FROM session_metrics WHERE session_id='a'"
        ),
        vec![vec![Value::Integer(i64::MAX), Value::Integer(0)]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT total_tokens, cache_read_input_tokens FROM forward_request_metrics"
        ),
        vec![vec![Value::Integer(-1), Value::Integer(-1)]]
    );
    assert_eq!(sp.stats().events, bp.stats().events);
    assert_eq!(sp.stats().commits, bp.stats().commits);
    for storage in [&serial, &batch] {
        storage
            .record_round_compression("ra", at(12), 5)
            .await
            .unwrap();
        storage
            .complete_tool_call(
                "tool",
                ToolCallCompletion {
                    completed_at: at(13),
                    success: true,
                    error: Some("preserved".into()),
                },
            )
            .await
            .unwrap();
    }
    compare(
        &serial,
        &batch,
        vec![
            done("ra", 100, 7),
            done("ra", 100, 7),
            tool("rb", "b"),
            round("ra", "b", 30),
            MetricsMutation::SessionMessageCount {
                session_id: "a".into(),
                message_count: 9,
                updated_at: at(-5),
            },
            forward_done(None, None),
            forward_done(
                Some(TokenUsage {
                    prompt_tokens: 2,
                    completion_tokens: 3,
                    total_tokens: 5,
                }),
                Some(ForwardTokenDetails {
                    cache_read_input_tokens: Some(7),
                    ..Default::default()
                }),
            ),
            forward_done(None, None),
        ],
    )
    .await;
    assert_eq!(rows(&conn, "SELECT session_id, tokens_saved, compression_count FROM round_metrics WHERE round_id='ra'"),
        vec![vec![Value::Text("a".into()), Value::Integer(12), Value::Integer(1)]]);
    assert_eq!(
        rows(
            &conn,
            "SELECT session_id, success, error, completed_at FROM tool_call_metrics"
        ),
        vec![vec![
            Value::Text("b".into()),
            Value::Integer(1),
            Value::Text("preserved".into()),
            Value::Text(format_timestamp(at(13)))
        ]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT updated_at FROM session_metrics WHERE session_id='b'"
        ),
        vec![vec![Value::Text(format_timestamp(at(30)))]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT updated_at FROM session_metrics WHERE session_id='a'"
        ),
        vec![vec![Value::Text(format_timestamp(at(-5)))]]
    );
    assert_eq!(rows(&conn, "SELECT total_tokens, cache_read_input_tokens, reasoning_output_tokens FROM forward_request_metrics"), vec![vec![Value::Null; 3]]);
    compare(
        &serial,
        &batch,
        vec![
            forward_start,
            MetricsMutation::SessionCompleted {
                session_id: "a".into(),
                status: SessionStatus::Completed,
                completed_at: at(40),
            },
            start("a"),
        ],
    )
    .await;
    assert_eq!(rows(&conn, "SELECT status, completed_at, status_code, total_tokens, error FROM forward_request_metrics"),
        vec![vec![Value::Text("pending".into()), Value::Null, Value::Null, Value::Null, Value::Null]]);
    assert_eq!(
        rows(
            &conn,
            "SELECT status, completed_at, started_at FROM session_metrics WHERE session_id='a'"
        ),
        vec![vec![
            Value::Text("running".into()),
            Value::Null,
            Value::Text(format_timestamp(at(0)))
        ]]
    );
    assert_eq!(sp.stats().events, bp.stats().events);
}

#[tokio::test]
async fn missing_rows_preserve_existing_noop_and_lookup_error_behavior() {
    let (_sd, serial, _) = store().await;
    let (_bd, batch, _) = store().await;
    let items = vec![
        MetricsMutation::ForwardCompleted {
            forward_id: "missing".into(),
            completed_at: at(1),
            status_code: None,
            status: ForwardStatus::Success,
            usage: None,
            token_details: None,
            error: None,
        },
        MetricsMutation::SessionMessageCount {
            session_id: "missing".into(),
            message_count: 4,
            updated_at: at(1),
        },
        done("missing", 3, 0),
        MetricsMutation::ToolCompleted {
            tool_call_id: "missing".into(),
            completion: ToolCallCompletion {
                completed_at: at(1),
                success: true,
                error: None,
            },
        },
    ];
    compare(&serial, &batch, items.clone()).await;
    let results = batch.apply_batch(items).await.unwrap();
    assert!(results[..2].iter().all(Result::is_ok));
    for result in &results[2..] {
        assert!(matches!(
            result,
            Err(MetricsError::Sqlite(rusqlite::Error::QueryReturnedNoRows))
        ));
    }
    assert_eq!(snapshot(&serial), snapshot(&batch));
}

#[tokio::test]
async fn negative_sibling_error_rolls_back_then_later_items_repair_and_complete() {
    let (_sd, serial, _) = store().await;
    let (_bd, batch, probe) = store().await;
    compare(
        &serial,
        &batch,
        vec![start("s"), round("bad", "s", 1), round("target", "s", 2)],
    )
    .await;
    for storage in [&serial, &batch] {
        Connection::open(&storage.db_path)
            .unwrap()
            .execute(
                "UPDATE round_metrics SET prompt_tokens=-1 WHERE round_id='bad'",
                [],
            )
            .unwrap();
    }
    let items = vec![
        done("target", 100, 0),
        done("bad", 10, 0),
        done("target", 100, 0),
    ];
    let mut reference = Vec::new();
    for item in items.iter().cloned() {
        reference.push(item.apply(&serial).await);
    }
    probe.reset();
    let (entered, release) = probe.pause_after(1);
    let path = batch.db_path.clone();
    let running = tokio::spawn(async move { batch.apply_batch(items).await });
    tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    let conn = Connection::open(&path).unwrap();
    let observed = rows(
        &conn,
        "SELECT completed_at, total_tokens FROM round_metrics WHERE round_id='target'",
    );
    let aggregate = rows(
        &conn,
        "SELECT total_tokens FROM session_metrics WHERE session_id='s'",
    );
    release.send(()).unwrap();
    let actual = running.await.unwrap().unwrap();
    assert_eq!(observed, vec![vec![Value::Null, Value::Integer(0)]]);
    assert_eq!(aggregate, vec![vec![Value::Integer(10)]]);
    assert!(matches!(actual[0], Err(MetricsError::InvalidData(_))));
    assert!(actual[1..].iter().all(Result::is_ok));
    assert_eq!(
        reference.iter().map(Result::is_ok).collect::<Vec<_>>(),
        actual.iter().map(Result::is_ok).collect::<Vec<_>>()
    );
    assert_eq!(
        snapshot(&serial),
        snapshot(&SqliteMetricsStorage::new(path))
    );
    assert_eq!(probe.stats().opens, 2);
    assert_eq!(probe.stats().commits, 2);
}

#[tokio::test]
async fn item_open_sql_and_panic_failures_preserve_suffix_without_retry() {
    let (_dir, storage, probe) = store().await;
    probe.fail_next_open();
    let result = storage
        .apply_batch(vec![mismatch("open-failed", 1), mismatch("counter", 2)])
        .await
        .unwrap();
    assert!(matches!(result[0], Err(MetricsError::Io(_))));
    assert!(result[1].is_ok());
    assert_eq!(
        (
            probe.stats().tasks,
            probe.stats().attempts,
            probe.stats().opens
        ),
        (1, 2, 1)
    );
    probe.reset();
    // Foreign-key failure is a single-statement/autocommit error. It must also
    // discard its connection, rather than only discarding active transactions.
    let result = storage
        .apply_batch(vec![tool("missing", "missing"), mismatch("counter", 3)])
        .await
        .unwrap();
    assert!(matches!(result[0], Err(MetricsError::Sqlite(_))));
    assert!(result[1].is_ok());
    assert_eq!(probe.stats().opens, 2);
    storage
        .apply_batch(vec![
            start("s"),
            round("panic", "s", 1),
            round("good", "s", 2),
        ])
        .await
        .unwrap()
        .into_iter()
        .for_each(|r| r.unwrap());
    probe.reset();
    probe.panic_after_round_update("panic");
    let result = storage
        .apply_batch(vec![
            mismatch("counter", 4),
            done("panic", 99, 1),
            mismatch("counter", 5),
            done("good", 10, 2),
        ])
        .await
        .unwrap();
    assert!(result[0].is_ok());
    assert!(matches!(result[1], Err(MetricsError::Task(_))));
    assert!(result[2..].iter().all(Result::is_ok));
    let conn = Connection::open(&storage.db_path).unwrap();
    assert_eq!(rows(&conn, "SELECT completed_at, total_tokens, tokens_saved FROM round_metrics WHERE round_id='panic'"), vec![vec![Value::Null, Value::Integer(0), Value::Integer(0)]]);
    assert_eq!(
        rows(
            &conn,
            "SELECT reason, count FROM execute_sync_mismatch_metrics"
        ),
        vec![vec![Value::Text("counter".into()), Value::Integer(4)]]
    );
    assert_eq!(
        (
            probe.stats().tasks,
            probe.stats().opens,
            probe.stats().commits
        ),
        (1, 2, 1)
    );
}

#[tokio::test]
async fn successful_segments_reuse_connections_and_cached_statements_with_changed_bindings() {
    let (_dir, storage, probe) = store().await;
    assert!(storage.apply_batch(Vec::new()).await.unwrap().is_empty());
    assert_eq!((probe.stats().tasks, probe.stats().opens), (0, 0));
    for length in [31_usize, 32, 33, 96] {
        probe.reset();
        let items = (0..length)
            .map(|i| mismatch(&format!("key-{length}-{i}"), i as i64))
            .collect();
        assert!(storage
            .apply_batch(items)
            .await
            .unwrap()
            .iter()
            .all(Result::is_ok));
        let segments = length.div_ceil(32);
        let stats = probe.stats();
        assert_eq!(
            (stats.tasks, stats.opens, stats.attempts),
            (segments, segments, segments)
        );
        assert_eq!(
            stats.commits, 0,
            "single statements retain SQLite autocommit"
        );
        assert_eq!(stats.cache_reuses, length - segments);
    }
    let conn = Connection::open(&storage.db_path).unwrap();
    assert_eq!(
        rows(
            &conn,
            "SELECT COUNT(*), SUM(count) FROM execute_sync_mismatch_metrics"
        ),
        vec![vec![Value::Integer(192), Value::Integer(192)]]
    );
    for length in [31_usize, 32, 33, 96] {
        for i in 0..length {
            assert_eq!(
                conn.query_row(
                    "SELECT updated_at FROM execute_sync_mismatch_metrics WHERE reason=?1",
                    [format!("key-{length}-{i}")],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
                format_timestamp(at(i as i64))
            );
        }
    }
}

#[tokio::test]
async fn second_writer_commits_between_items_and_next_aggregate_observes_it() {
    let (_dir, storage, probe) = store().await;
    storage
        .apply_batch(vec![start("s"), round("r", "s", 1)])
        .await
        .unwrap()
        .into_iter()
        .for_each(|r| r.unwrap());
    probe.reset();
    let (entered, release) = probe.pause_after(1);
    let path = storage.db_path.clone();
    let running = tokio::spawn(async move {
        storage
            .apply_batch(vec![
                done("r", 10, 0),
                MetricsMutation::SessionCompleted {
                    session_id: "s".into(),
                    status: SessionStatus::Completed,
                    completed_at: at(40),
                },
            ])
            .await
    });
    tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.busy_timeout(Duration::from_secs(1)).unwrap();
    let external = conn.execute_batch("BEGIN IMMEDIATE; UPDATE round_metrics SET prompt_tokens=42, total_tokens=42 WHERE round_id='r'; COMMIT;");
    release.send(()).unwrap();
    assert!(running.await.unwrap().unwrap().iter().all(Result::is_ok));
    external.unwrap();
    assert_eq!(
        rows(
            &conn,
            "SELECT total_tokens FROM session_metrics WHERE session_id='s'"
        ),
        vec![vec![Value::Integer(42)]]
    );
    assert_eq!(
        (
            probe.stats().tasks,
            probe.stats().opens,
            probe.stats().commits
        ),
        (1, 1, 2)
    );
}

#[tokio::test]
async fn burst_of_256_independent_sessions_preserves_records_and_bounds_resource_use() {
    let (_dir, storage, probe) = store().await;
    let mut items = Vec::new();
    for index in 0..256 {
        let id = format!("s{index}");
        let rid = format!("r{index}");
        let tid = format!("t{index}");
        items.extend([
            start(&id),
            round(&rid, &id, 1),
            done(&rid, index + 1, 3),
            MetricsMutation::ToolStarted {
                tool_call_id: tid.clone(),
                round_id: rid,
                session_id: id.clone(),
                tool_name: format!("tool{index}"),
                started_at: at(11),
            },
            MetricsMutation::ToolCompleted {
                tool_call_id: tid,
                completion: ToolCallCompletion {
                    completed_at: at(12),
                    success: true,
                    error: None,
                },
            },
            MetricsMutation::SessionCompleted {
                session_id: id,
                status: SessionStatus::Completed,
                completed_at: at(13),
            },
        ]);
    }
    let count = items.len();
    let now = Instant::now();
    assert!(storage
        .apply_batch(items)
        .await
        .unwrap()
        .iter()
        .all(Result::is_ok));
    let elapsed = now.elapsed();
    let stats = probe.stats();
    assert_eq!(
        (stats.tasks, stats.opens),
        (count.div_ceil(32), count.div_ceil(32))
    );
    assert_eq!(stats.commits, 256 * 4);
    let conn = Connection::open(&storage.db_path).unwrap();
    assert_eq!(rows(&conn, "SELECT COUNT(*), SUM(total_rounds), SUM(tool_call_count) FROM session_metrics WHERE status='completed'"), vec![vec![Value::Integer(256); 3]]);
    assert_eq!(
        rows(
            &conn,
            "SELECT COUNT(*), SUM(total_tokens) FROM round_metrics WHERE status='success'"
        ),
        vec![vec![Value::Integer(256), Value::Integer(256 * 257 / 2)]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT COUNT(*) FROM tool_call_metrics WHERE success=1"
        ),
        vec![vec![Value::Integer(256)]]
    );
    println!("metrics burst: commands={count}, tasks={}, opens={}, explicit_commits={}, elapsed={elapsed:?}", stats.tasks, stats.opens, stats.commits);
}
