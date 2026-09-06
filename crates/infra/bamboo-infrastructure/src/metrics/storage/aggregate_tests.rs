use chrono::TimeZone;
use rusqlite::types::Value;
use tempfile::TempDir;

use super::*;

// Copied from b539f992 before #1087, independently of the production constant.
// Keep the five scalar round lookups as the compatibility and query-plan oracle.
const SCALAR_REFERENCE_SQL: &str = r#"
    UPDATE session_metrics
    SET
        total_rounds = COALESCE((SELECT COUNT(*) FROM round_metrics WHERE session_id = ?1), 0),
        prompt_tokens = ?2,
        completion_tokens = ?3,
        total_tokens = ?4,
        prompt_cached_tool_outputs = COALESCE((SELECT SUM(prompt_cached_tool_outputs) FROM round_metrics WHERE session_id = ?1), 0),
        prompt_cached_tool_tokens_saved = COALESCE((SELECT SUM(prompt_cached_tool_tokens_saved) FROM round_metrics WHERE session_id = ?1), 0),
        total_compression_events = COALESCE((SELECT SUM(compression_count) FROM round_metrics WHERE session_id = ?1), 0),
        total_tokens_saved = COALESCE((SELECT SUM(tokens_saved) FROM round_metrics WHERE session_id = ?1), 0),
        tool_call_count = COALESCE((SELECT COUNT(*) FROM tool_call_metrics WHERE session_id = ?1), 0),
        updated_at = ?5
    WHERE session_id = ?1
"#;

const NON_TOKEN_COLUMNS: [&str; 4] = [
    "prompt_cached_tool_outputs",
    "prompt_cached_tool_tokens_saved",
    "compression_count",
    "tokens_saved",
];

fn at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 6, 1, 2, 3).unwrap()
}

async fn database() -> (TempDir, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metrics.db");
    SqliteMetricsStorage::new(&path).init().await.unwrap();
    let connection = Connection::open(path).unwrap();
    connection
        .pragma_update(None, "foreign_keys", true)
        .unwrap();
    (dir, connection)
}

fn session(connection: &Connection, id: &str) {
    connection.execute(
        "INSERT INTO session_metrics(session_id,model,started_at,total_rounds,prompt_tokens,completion_tokens,total_tokens,prompt_cached_tool_outputs,prompt_cached_tool_tokens_saved,total_compression_events,total_tokens_saved,tool_call_count,message_count,updated_at) VALUES (?1,'model',?2,91,92,93,94,95,96,97,98,99,7,?2)",
        params![id, format_timestamp(at())],
    ).unwrap();
}

fn round(connection: &Connection, id: &str, session_id: &str, value: i64) {
    connection.execute(
        "INSERT INTO round_metrics(round_id,session_id,model,started_at,prompt_tokens,completion_tokens,total_tokens,prompt_cached_tool_outputs,prompt_cached_tool_tokens_saved,compression_count,tokens_saved) VALUES (?1,?2,'model',?3,?4,?4,?4,?4,?4,?4,?4)",
        params![id, session_id, format_timestamp(at()), value],
    ).unwrap();
}

fn rows(connection: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| (0..columns).map(|index| row.get(index)).collect())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn snapshot(connection: &Connection) -> Vec<Vec<Vec<Value>>> {
    ["session_metrics", "round_metrics", "tool_call_metrics"]
        .into_iter()
        .map(|table| rows(connection, &format!("SELECT * FROM {table} ORDER BY rowid")))
        .collect()
}

fn parent_totals(connection: &Connection) -> Vec<Value> {
    rows(connection, "SELECT total_rounds,prompt_tokens,completion_tokens,total_tokens,prompt_cached_tool_outputs,prompt_cached_tool_tokens_saved,total_compression_events,total_tokens_saved,tool_call_count,message_count FROM session_metrics WHERE session_id='target'")
        .pop().unwrap()
}

fn refresh(
    connection: &Connection,
    session_id: &str,
    reference: bool,
    mark_child: bool,
) -> MetricsResult<()> {
    with_immediate_transaction(connection, || {
        if mark_child {
            // A distinguishable child write in the same transaction must be
            // rolled back if either the token fold or SQLite SUM fails.
            connection.execute(
                "UPDATE round_metrics SET status='error',error='uncommitted-marker',completed_at=?1 WHERE round_id='r1'",
                [format_timestamp(at() + chrono::Duration::hours(1))],
            )?;
        }
        if reference {
            // The token fold and bindings are unchanged by this Issue. Only
            // the independently copied old scalar UPDATE differs here.
            let usage = load_session_token_aggregate(connection, session_id)?;
            connection.execute(
                SCALAR_REFERENCE_SQL,
                params![
                    session_id,
                    durable_token_to_i64(usage.prompt_tokens),
                    durable_token_to_i64(usage.completion_tokens),
                    durable_token_to_i64(usage.total_tokens),
                    format_timestamp(at()),
                ],
            )?;
            Ok(())
        } else {
            refresh_session_aggregates_in_transaction(connection, session_id, at())
        }
    })
}

fn compare_refresh(
    reference: &Connection,
    optimized: &Connection,
    mark_child: bool,
) -> MetricsResult<()> {
    let expected = refresh(reference, "target", true, mark_child);
    let actual = refresh(optimized, "target", false, mark_child);
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    assert_eq!(snapshot(optimized), snapshot(reference));
    assert!(reference.is_autocommit() && optimized.is_autocommit());
    actual
}

#[tokio::test]
async fn empty_and_populated_aggregates_match_scalar_reference_without_touching_other_sessions() {
    let (_rd, reference) = database().await;
    let (_od, optimized) = database().await;
    for connection in [&reference, &optimized] {
        session(connection, "target");
        session(connection, "unrelated");
        round(connection, "other-round", "unrelated", 800);
    }
    let unrelated = rows(
        &optimized,
        "SELECT * FROM session_metrics WHERE session_id='unrelated'",
    );
    compare_refresh(&reference, &optimized, false).unwrap();
    assert_eq!(
        parent_totals(&optimized),
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 7].map(Value::Integer)
    );

    for connection in [&reference, &optimized] {
        round(connection, "r2", "target", 3);
        round(connection, "r1", "target", 1);
        connection.execute(
            "INSERT INTO tool_call_metrics(tool_call_id,round_id,session_id,tool_name,started_at) VALUES ('tool','r1','target','read',?1)",
            [format_timestamp(at())],
        ).unwrap();
    }
    compare_refresh(&reference, &optimized, false).unwrap();
    assert_eq!(
        parent_totals(&optimized),
        [2, 4, 4, 8, 4, 4, 4, 4, 1, 7].map(Value::Integer)
    );
    assert_eq!(
        unrelated,
        rows(
            &optimized,
            "SELECT * FROM session_metrics WHERE session_id='unrelated'"
        )
    );
}

#[tokio::test]
async fn each_non_token_integer_sum_overflow_preserves_error_and_rolls_back_the_child_write() {
    for column in NON_TOKEN_COLUMNS {
        let (_rd, reference) = database().await;
        let (_od, optimized) = database().await;
        for connection in [&reference, &optimized] {
            session(connection, "target");
            round(connection, "r1", "target", 1);
            round(connection, "r2", "target", 1);
            connection
                .execute(
                    &format!("UPDATE round_metrics SET {column}=?1 WHERE round_id='r1'"),
                    [i64::MAX],
                )
                .unwrap();
        }
        let before = snapshot(&optimized);
        let error = compare_refresh(&reference, &optimized, true).unwrap_err();
        match error {
            MetricsError::Sqlite(rusqlite::Error::SqliteFailure(code, Some(message))) => {
                assert_eq!(code.extended_code, rusqlite::ffi::SQLITE_ERROR, "{column}");
                assert_eq!(message, "integer overflow", "{column}");
            }
            other => panic!("{column}: expected SQLite integer SUM overflow, got {other:?}"),
        }
        assert_eq!(
            snapshot(&optimized),
            before,
            "{column}: child and parent roll back"
        );
    }
}

#[tokio::test]
async fn negative_token_validation_precedes_non_token_overflow_and_preserves_rollback() {
    let (_rd, reference) = database().await;
    let (_od, optimized) = database().await;
    for connection in [&reference, &optimized] {
        session(connection, "target");
        round(connection, "r1", "target", 1);
        round(connection, "r2", "target", 1);
        connection
            .execute(
                "UPDATE round_metrics SET tokens_saved=?1 WHERE round_id='r1'",
                [i64::MAX],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE round_metrics SET prompt_tokens=-1 WHERE round_id='r2'",
                [],
            )
            .unwrap();
    }
    let before = snapshot(&optimized);
    assert!(matches!(
        compare_refresh(&reference, &optimized, true),
        Err(MetricsError::InvalidData(message))
            if message == "negative durable token counter in round_metrics.prompt_tokens: -1"
    ));
    assert_eq!(snapshot(&optimized), before);
}

#[tokio::test]
async fn non_token_real_sums_and_sql_addition_overflow_keep_their_sqlite_types() {
    let (_rd, reference) = database().await;
    let (_od, optimized) = database().await;
    for connection in [&reference, &optimized] {
        session(connection, "target");
        round(connection, "r1", "target", 1);
        round(connection, "r2", "target", 1);
        for column in NON_TOKEN_COLUMNS {
            connection.execute(
                &format!("UPDATE round_metrics SET {column}=CASE round_id WHEN 'r1' THEN 1.25 ELSE 2.5 END"),
                [],
            ).unwrap();
        }
    }
    compare_refresh(&reference, &optimized, false).unwrap();
    assert_eq!(&parent_totals(&optimized)[4..8], vec![Value::Real(3.75); 4]);

    for connection in [&reference, &optimized] {
        // Existing SQL addition can create REAL values even in an INTEGER
        // affinity column; the aggregate must not force them through Rust i64.
        connection
            .execute(
                "UPDATE round_metrics SET tokens_saved=?1+1 WHERE round_id='r1'",
                [i64::MAX],
            )
            .unwrap();
    }
    compare_refresh(&reference, &optimized, false).unwrap();
    assert!(matches!(parent_totals(&optimized)[7], Value::Real(value) if value >= i64::MAX as f64));
}

#[tokio::test]
async fn absent_parent_skips_non_token_overflow_but_still_validates_orphan_tokens() {
    let (_rd, reference) = database().await;
    let (_od, optimized) = database().await;
    for connection in [&reference, &optimized] {
        session(connection, "target");
        round(connection, "r1", "target", 1);
        round(connection, "r2", "target", 1);
        connection
            .execute(
                "UPDATE round_metrics SET tokens_saved=?1 WHERE round_id='r1'",
                [i64::MAX],
            )
            .unwrap();
        connection
            .pragma_update(None, "foreign_keys", false)
            .unwrap();
        connection
            .execute("DELETE FROM session_metrics WHERE session_id='target'", [])
            .unwrap();
        connection
            .pragma_update(None, "foreign_keys", true)
            .unwrap();
    }
    let before = snapshot(&optimized);
    compare_refresh(&reference, &optimized, false).unwrap();
    assert_eq!(
        snapshot(&optimized),
        before,
        "the unmatched UPDATE is a no-op"
    );
    for connection in [&reference, &optimized] {
        connection
            .execute(
                "UPDATE round_metrics SET total_tokens=-1 WHERE round_id='r2'",
                [],
            )
            .unwrap();
    }
    let before = snapshot(&optimized);
    assert!(matches!(
        compare_refresh(&reference, &optimized, true),
        Err(MetricsError::InvalidData(message))
            if message == "negative durable token counter in round_metrics.total_tokens: -1"
    ));
    assert_eq!(snapshot(&optimized), before);
}

#[tokio::test]
async fn production_update_uses_one_keyed_round_scan_instead_of_five_scalar_scans() {
    let (_dir, connection) = database().await;
    with_immediate_transaction(&connection, || {
        for id in 0..65 {
            let sid = format!("session-{id}");
            session(&connection, &sid);
            for rid in 0..3 {
                let round_id = format!("round-{id}-{rid}");
                round(&connection, &round_id, &sid, rid + 1);
                connection.execute(
                    "INSERT INTO tool_call_metrics(tool_call_id,round_id,session_id,tool_name,started_at) VALUES (?1,?1,?2,'tool',?3)",
                    params![round_id, sid, format_timestamp(at())],
                )?;
            }
        }
        Ok(())
    }).unwrap();
    connection.execute_batch("ANALYZE").unwrap();
    for (name, query, expected_round_lookups) in [
        ("scalar reference", SCALAR_REFERENCE_SQL, 5),
        ("production", REFRESH_SESSION_AGGREGATES_SQL, 1),
    ] {
        let mut statement = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {query}"))
            .unwrap();
        let plan = statement
            .query_map(
                params!["session-0", 0, 0, 0, format_timestamp(at())],
                |row| row.get::<_, String>(3),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let rounds: Vec<_> = plan
            .iter()
            .filter(|step| step.contains("round_metrics"))
            .collect();
        assert_eq!(rounds.len(), expected_round_lookups, "{name}: {plan:?}");
        assert!(
            rounds
                .iter()
                .all(|step| step.starts_with("SEARCH round_metrics ")
                    && step.contains("session_id=?")),
            "{name}: {plan:?}"
        );
        let tools: Vec<_> = plan
            .iter()
            .filter(|step| step.contains("tool_call_metrics"))
            .collect();
        assert_eq!(tools.len(), 1, "{name}: {plan:?}");
        assert!(
            tools[0].starts_with("SEARCH tool_call_metrics ") && tools[0].contains("session_id=?"),
            "{name}: {plan:?}"
        );
        eprintln!("{name}: {plan:?}");
    }
}
