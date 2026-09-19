use chrono::{DateTime, Duration, TimeZone, Utc};
use rusqlite::Connection;
use tempfile::{tempdir, TempDir};

use super::{open_connection, MetricsStorage, SqliteMetricsStorage};
use crate::metrics::types::{
    PromptMemoryExposureItem, PromptMemoryExposureObservation, PromptMemoryRecallOutcome,
};

fn observed_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 5, 10, 0, 0).unwrap()
}

fn observation(round: &str, project: &str, ids: &[&str]) -> PromptMemoryExposureObservation {
    PromptMemoryExposureObservation {
        schema_version: 1,
        round_id: round.to_string(),
        session_id: "session-a".to_string(),
        project_id: Some(project.to_string()),
        observed_at: observed_at(),
        recall_enabled: true,
        query_present: true,
        recall_outcome: PromptMemoryRecallOutcome::Lexical,
        all_compact_exposed_count: ids.len() as u32,
        project_exposed_count: ids.len() as u32,
        out_of_project_only: false,
        compact_section_chars: 80 + 60 * ids.len() as u32,
        project_items: ids
            .iter()
            .enumerate()
            .map(|(index, id)| PromptMemoryExposureItem {
                memory_id: (*id).to_string(),
                scope: "project".to_string(),
                status_at_observation: "active".to_string(),
                rank: index as u32 + 1,
                rendered_chars: 60,
            })
            .collect(),
    }
}

async fn fixture() -> (TempDir, SqliteMetricsStorage) {
    let dir = tempdir().unwrap();
    let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
    storage.init().await.unwrap();
    storage
        .upsert_session_start("session-a", "test-model", observed_at())
        .await
        .unwrap();
    (dir, storage)
}

async fn start_round(storage: &SqliteMetricsStorage, round: &str, when: DateTime<Utc>) {
    storage
        .insert_round_start(round, "session-a", "test-model", when)
        .await
        .unwrap();
}

fn counts(connection: &Connection) -> (i64, i64) {
    connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM prompt_memory_round_observations),
                    (SELECT COUNT(*) FROM prompt_memory_project_exposures)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

fn assert_no_orphans(connection: &Connection) {
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn prompt_memory_first_snapshot_survives_retries_and_conflicting_owners() {
    let (dir, storage) = fixture().await;
    let round = "session-a-run-execution-one-round-1";
    start_round(&storage, round, observed_at()).await;
    let first = observation(round, "project-a", &["memory-a", "memory-b"]);
    storage.record_prompt_memory_exposure(&first).await.unwrap();
    storage.record_prompt_memory_exposure(&first).await.unwrap();
    let mut retry = observation(round, "project-a", &["memory-c"]);
    retry.observed_at += Duration::minutes(1);
    retry.recall_outcome = PromptMemoryRecallOutcome::RerankFallback;
    storage.record_prompt_memory_exposure(&retry).await.unwrap();
    for owner in [Some("project-b".to_string()), None] {
        retry.project_id = owner;
        retry.project_items.clear();
        retry.project_exposed_count = 0;
        retry.out_of_project_only = true;
        assert!(storage.record_prompt_memory_exposure(&retry).await.is_err());
    }
    retry = first.clone();
    retry.session_id = "session-b".to_string();
    assert!(storage.record_prompt_memory_exposure(&retry).await.is_err());
    assert_eq!(
        storage.prompt_memory_exposure(round).await.unwrap(),
        Some(first)
    );

    let restarted = "session-a-run-execution-two-round-1";
    start_round(&storage, restarted, observed_at()).await;
    let next = observation(restarted, "project-a", &["memory-a"]);
    storage.record_prompt_memory_exposure(&next).await.unwrap();
    let connection = open_connection(&dir.path().join("metrics.db")).unwrap();
    assert_eq!(counts(&connection), (2, 3));
    assert_eq!(
        connection.query_row(
            "SELECT COUNT(DISTINCT round_id) FROM prompt_memory_project_exposures WHERE memory_id = 'memory-a'",
            [], |row| row.get::<_, i64>(0),
        ).unwrap(),
        2
    );
    assert_no_orphans(&connection);
}

#[tokio::test]
async fn prompt_memory_concurrent_same_round_delivery_keeps_one_complete_snapshot() {
    let (dir, storage) = fixture().await;
    start_round(&storage, "round-race", observed_at()).await;
    let one = observation("round-race", "project-a", &["memory-a"]);
    let two = observation("round-race", "project-a", &["memory-b", "memory-c"]);
    let (a, b) = tokio::join!(
        storage.record_prompt_memory_exposure(&one),
        storage.record_prompt_memory_exposure(&two)
    );
    a.unwrap();
    b.unwrap();
    let stored = storage
        .prompt_memory_exposure("round-race")
        .await
        .unwrap()
        .unwrap();
    assert!(
        stored == one || stored == two,
        "membership must never be a union"
    );
    let connection = open_connection(&dir.path().join("metrics.db")).unwrap();
    assert_eq!(
        counts(&connection),
        (1, i64::from(stored.project_exposed_count))
    );
    assert_no_orphans(&connection);
}

#[tokio::test]
async fn prompt_memory_second_item_sql_failure_rolls_back_header_and_first_item() {
    let (dir, storage) = fixture().await;
    start_round(&storage, "round-atomic", observed_at()).await;
    let connection = open_connection(&dir.path().join("metrics.db")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_second_memory BEFORE INSERT ON prompt_memory_project_exposures
         WHEN NEW.memory_id = 'memory-b'
         BEGIN SELECT RAISE(ABORT, 'injected second-item failure'); END;",
        )
        .unwrap();
    let value = observation("round-atomic", "project-a", &["memory-a", "memory-b"]);
    assert!(storage.record_prompt_memory_exposure(&value).await.is_err());
    assert_eq!(counts(&connection), (0, 0));
    assert!(storage
        .prompt_memory_exposure("round-atomic")
        .await
        .unwrap()
        .is_none());
    assert_no_orphans(&connection);
    connection
        .execute_batch("DROP TRIGGER fail_second_memory")
        .unwrap();
    storage.record_prompt_memory_exposure(&value).await.unwrap();
    assert_eq!(counts(&connection), (1, 2));
}

#[tokio::test]
async fn prompt_memory_invalid_or_unowned_observations_never_leave_rows() {
    let (dir, storage) = fixture().await;
    let mut value = observation("round-validation", "project-a", &["memory-a"]);
    assert!(storage.record_prompt_memory_exposure(&value).await.is_err());
    start_round(&storage, &value.round_id, observed_at()).await;
    for kind in 0..14 {
        let mut invalid = value.clone();
        match kind {
            0 => invalid.project_items[0].scope = "global".to_string(),
            1 => invalid.project_exposed_count = 2,
            2 => invalid.project_id = None,
            3 => invalid.project_items[0].rank = 0,
            4 => invalid.project_items[0].rendered_chars = 0,
            5 => invalid.schema_version = 2,
            6 => invalid.session_id = "another-session".to_string(),
            7 => invalid.project_items[0].rank = 99,
            8 => invalid.compact_section_chars = 0,
            9 => invalid.compact_section_chars = 59,
            10 => {
                invalid.project_items[0].status_at_observation = "query/body sentinel".to_string()
            }
            11 => invalid.project_items[0].status_at_observation = " ".to_string(),
            12 => invalid.project_items[0].status_at_observation = "unknown".to_string(),
            _ => {
                invalid.project_items.clear();
                invalid.project_exposed_count = 0;
                invalid.out_of_project_only = true;
                invalid.compact_section_chars = 0;
            }
        }
        let error = storage
            .record_prompt_memory_exposure(&invalid)
            .await
            .expect_err("invalid observations must be rejected");
        assert!(
            !error.to_string().contains("query/body sentinel"),
            "case {kind}: rejected content must not leak into collector error logs"
        );
    }
    value.project_items.push(value.project_items[0].clone());
    value.all_compact_exposed_count = 2;
    value.project_exposed_count = 2;
    assert!(storage.record_prompt_memory_exposure(&value).await.is_err());
    let connection = open_connection(&dir.path().join("metrics.db")).unwrap();
    assert_eq!(counts(&connection), (0, 0));
    assert_no_orphans(&connection);
}

#[tokio::test]
async fn prompt_memory_status_is_a_coarse_lifecycle_in_rust_and_sqlite() {
    let (dir, storage) = fixture().await;
    for status in ["active", "stale", "superseded", "contradicted", "archived"] {
        start_round(&storage, status, observed_at()).await;
        let mut value = observation(status, "project-a", &["memory-a"]);
        value.project_items[0].status_at_observation = status.to_string();
        storage.record_prompt_memory_exposure(&value).await.unwrap();
        assert_eq!(
            storage.prompt_memory_exposure(status).await.unwrap(),
            Some(value)
        );
    }
    let connection = open_connection(&dir.path().join("metrics.db")).unwrap();
    assert!(connection.execute(
        "UPDATE prompt_memory_project_exposures SET status_at_observation = 'query/body sentinel'",
        [],
    ).is_err(), "the database also rejects non-lifecycle text");
    assert_eq!(counts(&connection), (5, 5));
    assert_eq!(
        storage
            .prompt_memory_exposure("active")
            .await
            .unwrap()
            .unwrap()
            .project_items[0]
            .status_at_observation,
        "active"
    );
}

#[tokio::test]
async fn prompt_memory_empty_outcomes_and_global_only_headers_remain_distinguishable() {
    let (dir, storage) = fixture().await;
    for outcome in [
        PromptMemoryRecallOutcome::Disabled,
        PromptMemoryRecallOutcome::NoQuery,
        PromptMemoryRecallOutcome::NoMatch,
        PromptMemoryRecallOutcome::LookupError,
        PromptMemoryRecallOutcome::Lexical,
        PromptMemoryRecallOutcome::Reranked,
        PromptMemoryRecallOutcome::RerankFallback,
    ] {
        let mut value = observation(outcome.as_str(), "project-a", &[]);
        value.recall_outcome = outcome;
        value.recall_enabled = outcome != PromptMemoryRecallOutcome::Disabled;
        value.query_present = outcome != PromptMemoryRecallOutcome::NoQuery;
        value.compact_section_chars = 0;
        start_round(&storage, &value.round_id, observed_at()).await;
        storage.record_prompt_memory_exposure(&value).await.unwrap();
        assert_eq!(
            storage
                .prompt_memory_exposure(&value.round_id)
                .await
                .unwrap(),
            Some(value)
        );
    }
    let mut global = observation("global-only", "project-a", &[]);
    global.all_compact_exposed_count = 2;
    global.out_of_project_only = true;
    start_round(&storage, &global.round_id, observed_at()).await;
    storage
        .record_prompt_memory_exposure(&global)
        .await
        .unwrap();
    assert_eq!(
        storage
            .prompt_memory_exposure(&global.round_id)
            .await
            .unwrap(),
        Some(global)
    );
    let mut mixed = observation("mixed", "project-b", &["project-b-memory"]);
    mixed.all_compact_exposed_count = 3;
    mixed.project_items[0].rank = 2; // A Global item leads the actual compact set.
    start_round(&storage, &mixed.round_id, observed_at()).await;
    storage.record_prompt_memory_exposure(&mixed).await.unwrap();
    assert_eq!(
        storage
            .prompt_memory_exposure(&mixed.round_id)
            .await
            .unwrap(),
        Some(mixed)
    );
    let connection = open_connection(&dir.path().join("metrics.db")).unwrap();
    assert_eq!(counts(&connection), (9, 1));
    assert_no_orphans(&connection);
}

#[tokio::test]
async fn prompt_memory_retention_uses_round_cutoff_and_cascades_both_tables() {
    let (dir, storage) = fixture().await;
    let cutoff = observed_at() - Duration::days(90);
    for (round, started) in [
        ("old", cutoff - Duration::seconds(1)),
        ("boundary", cutoff),
        ("recent", cutoff + Duration::seconds(1)),
    ] {
        start_round(&storage, round, started).await;
        // An old round's late telemetry still expires with its parent round.
        storage
            .record_prompt_memory_exposure(&observation(round, "project-a", &["memory-a"]))
            .await
            .unwrap();
    }
    assert_eq!(storage.prune_rounds_before(cutoff).await.unwrap(), 1);
    assert_eq!(storage.prune_rounds_before(cutoff).await.unwrap(), 0);
    assert!(storage
        .prompt_memory_exposure("old")
        .await
        .unwrap()
        .is_none());
    assert!(storage
        .prompt_memory_exposure("boundary")
        .await
        .unwrap()
        .is_some());
    assert!(storage
        .prompt_memory_exposure("recent")
        .await
        .unwrap()
        .is_some());
    let connection = open_connection(&dir.path().join("metrics.db")).unwrap();
    assert_eq!(counts(&connection), (2, 2));
    assert_no_orphans(&connection);
    connection
        .execute(
            "DELETE FROM session_metrics WHERE session_id='session-a'",
            [],
        )
        .unwrap();
    assert_eq!(counts(&connection), (0, 0));
    assert_no_orphans(&connection);
}

// Independent populated pre-#1077 schema: never derived by creating the new
// schema then dropping its tables, so additions and migrations are exercised.
const PRE_EXPOSURE_SCHEMA: &str = r#"
CREATE TABLE session_metrics (
 session_id TEXT PRIMARY KEY, model TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT,
 status TEXT NOT NULL DEFAULT 'running', total_rounds INTEGER NOT NULL DEFAULT 0,
 prompt_tokens INTEGER NOT NULL DEFAULT 0, completion_tokens INTEGER NOT NULL DEFAULT 0,
 total_tokens INTEGER NOT NULL DEFAULT 0, prompt_cached_tool_outputs INTEGER NOT NULL DEFAULT 0,
 prompt_cached_tool_tokens_saved INTEGER NOT NULL DEFAULT 0, total_compression_events INTEGER NOT NULL DEFAULT 0,
 total_tokens_saved INTEGER NOT NULL DEFAULT 0, tool_call_count INTEGER NOT NULL DEFAULT 0,
 message_count INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL
);
CREATE TABLE round_metrics (
 round_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, model TEXT NOT NULL, started_at TEXT NOT NULL,
 completed_at TEXT, status TEXT NOT NULL DEFAULT 'running', prompt_tokens INTEGER NOT NULL DEFAULT 0,
 completion_tokens INTEGER NOT NULL DEFAULT 0, total_tokens INTEGER NOT NULL DEFAULT 0,
 prompt_cached_tool_outputs INTEGER NOT NULL DEFAULT 0, prompt_cached_tool_tokens_saved INTEGER NOT NULL DEFAULT 0,
 compression_count INTEGER NOT NULL DEFAULT 0, tokens_saved INTEGER NOT NULL DEFAULT 0, error TEXT,
 FOREIGN KEY(session_id) REFERENCES session_metrics(session_id) ON DELETE CASCADE
);
CREATE TABLE tool_call_metrics (
 tool_call_id TEXT PRIMARY KEY, round_id TEXT NOT NULL, session_id TEXT NOT NULL, tool_name TEXT NOT NULL,
 started_at TEXT NOT NULL, completed_at TEXT, success INTEGER, error TEXT,
 FOREIGN KEY(round_id) REFERENCES round_metrics(round_id) ON DELETE CASCADE,
 FOREIGN KEY(session_id) REFERENCES session_metrics(session_id) ON DELETE CASCADE
);
CREATE INDEX idx_round_session_started_at ON round_metrics(session_id, started_at);
CREATE INDEX idx_round_started_at ON round_metrics(started_at);
CREATE INDEX idx_tool_round_started_at ON tool_call_metrics(round_id, started_at);
CREATE INDEX custom_tool_success ON tool_call_metrics(success);
INSERT INTO session_metrics(session_id,model,started_at,updated_at) VALUES ('session-a','legacy','2026-09-05T10:00:00+00:00','2026-09-05T10:00:00+00:00');
INSERT INTO round_metrics(round_id,session_id,model,started_at,total_tokens) VALUES ('legacy-round','session-a','legacy','2026-09-05T10:00:00+00:00',42);
INSERT INTO tool_call_metrics(tool_call_id,round_id,session_id,tool_name,started_at,success) VALUES ('legacy-tool','legacy-round','session-a','fixture','2026-09-05T10:00:00+00:00',1);
"#;

fn legacy_rows(connection: &Connection) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    ["session_metrics", "round_metrics", "tool_call_metrics"]
        .iter()
        .map(|table| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                .unwrap();
            let columns = statement.column_count();
            statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        })
        .collect()
}

#[tokio::test]
async fn prompt_memory_populated_migration_and_restart_preserve_rows_indexes_and_exposure() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metrics.db");
    let connection = open_connection(&path).unwrap();
    connection.execute_batch(PRE_EXPOSURE_SCHEMA).unwrap();
    let before = legacy_rows(&connection);
    drop(connection);
    let value = observation("legacy-round", "project-a", &["memory-a"]);
    for pass in 0..3 {
        let storage = SqliteMetricsStorage::new(&path);
        storage.init().await.unwrap();
        if pass == 0 {
            storage.record_prompt_memory_exposure(&value).await.unwrap();
        }
        assert_eq!(
            storage
                .prompt_memory_exposure(&value.round_id)
                .await
                .unwrap(),
            Some(value.clone())
        );
        let connection = open_connection(&path).unwrap();
        assert_eq!(legacy_rows(&connection), before);
        assert_eq!(counts(&connection), (1, 1));
        for index in [
            "idx_round_session_started_at",
            "idx_round_started_at",
            "idx_tool_round_started_at",
            "custom_tool_success",
        ] {
            assert_eq!(
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name=?1",
                        [index],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1,
                "preserve {index}"
            );
        }
        assert_no_orphans(&connection);
    }
}

#[test]
fn prompt_memory_serialization_contains_only_the_v1_allowlisted_fields() {
    let value = observation("round-serde", "project-a", &["memory-a"]);
    let json = serde_json::to_value(&value).unwrap();
    let mut keys = json
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "all_compact_exposed_count",
            "compact_section_chars",
            "observed_at",
            "out_of_project_only",
            "project_exposed_count",
            "project_id",
            "project_items",
            "query_present",
            "recall_enabled",
            "recall_outcome",
            "round_id",
            "schema_version",
            "session_id"
        ]
    );
    let mut item_keys = json["project_items"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    item_keys.sort_unstable();
    assert_eq!(
        item_keys,
        [
            "memory_id",
            "rank",
            "rendered_chars",
            "scope",
            "status_at_observation"
        ]
    );
    assert_eq!(
        serde_json::from_value::<PromptMemoryExposureObservation>(json).unwrap(),
        value
    );
}
