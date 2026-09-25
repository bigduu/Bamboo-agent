//! DuckDB-backed offline analytics over the per-session token-usage logs.
//!
//! Every agent LLM call appends one JSON line to
//! `{bamboo_home}/sessions/**/token-usage.jsonl` (written by
//! `bamboo_engine::token_usage_log`). DuckDB reads those JSONL files **in place**
//! via `read_json_auto` over a glob — no ingestion, no separate database — so
//! this module is a thin set of curated queries plus typed row structs.
//!
//! This is deliberately its own crate: it links the bundled DuckDB native engine
//! (tens of MB), which only callers that actually want analytics should pay for.
//! It is unrelated to the SQLite FTS5 session-search index in `bamboo-storage`,
//! which remains the source of truth for full-text search.
//!
//! # Cost model
//!
//! Costs are expressed in **base-input-token-equivalents** using Anthropic's
//! prompt-cache multipliers (relative to normal input = `1.0`):
//!
//! - cache **read**: [`CACHE_READ_MULTIPLIER`] (`0.1x`)
//! - cache **write**, 1-hour TTL: [`CACHE_WRITE_1H_MULTIPLIER`] (`2.0x`)
//!
//! Bamboo writes its stable prefix with the 1-hour TTL, so the write premium
//! over normal input is `2.0 - 1.0 = 1.0` per created token, and every read
//! saves `1.0 - 0.1 = 0.9` versus paying full price for those tokens.
//!
//! ```no_run
//! use bamboo_analytics::TokenUsageDb;
//! use std::path::Path;
//!
//! let db = TokenUsageDb::open_home(Path::new("/Users/me/.bamboo"))?;
//! for row in db.session_summary()? {
//!     println!("{}: {} calls, {:.0} tokens served from cache",
//!         row.session_id, row.calls, row.total_cache_read as f64);
//! }
//! # Ok::<(), bamboo_analytics::AnalyticsError>(())
//! ```

use std::collections::HashSet;
use std::path::Path;

use duckdb::Connection;
use rusqlite::{Connection as SqliteConnection, OpenFlags};
use serde::Serialize;

mod retrieval_rollout;

pub use retrieval_rollout::{
    render_retrieval_rollout_report, EvidenceKind, EvidenceSource, QueryClass,
    RetrievalRolloutError, RetrievalRolloutReport, RetrievalRolloutSample, RolloutStrategy,
    RETRIEVAL_ROLLOUT_SCHEMA_VERSION,
};

/// Relative cost of a cache-read token (vs normal input = 1.0).
pub const CACHE_READ_MULTIPLIER: f64 = 0.1;
/// Relative cost of writing a cache token at the 1-hour TTL (vs normal input).
pub const CACHE_WRITE_1H_MULTIPLIER: f64 = 2.0;

/// A pause longer than the default 5-minute prompt-cache TTL. Reads that stay
/// non-zero across a gap larger than this — but within an hour — show the
/// 1-hour extended TTL doing its job.
pub const DEFAULT_TTL_SECONDS: f64 = 300.0;

#[derive(Debug, thiserror::Error)]
pub enum AnalyticsError {
    #[error("duckdb error: {0}")]
    Duck(#[from] duckdb::Error),
    #[error("sqlite metrics error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("no token-usage files matched glob: {0}")]
    NoFiles(String),
}

pub type Result<T> = std::result::Result<T, AnalyticsError>;

fn sql_quote(value: &str) -> String {
    value.replace('\'', "''")
}

/// An in-memory DuckDB session with a `token_usage` view over a glob of
/// `token-usage.jsonl` files.
pub struct TokenUsageDb {
    conn: Connection,
}

impl TokenUsageDb {
    /// The standard glob for a bamboo home directory:
    /// `{home}/sessions/**/token-usage.jsonl` (covers root and child sessions).
    pub fn default_glob(home: &Path) -> String {
        home.join("sessions")
            .join("**")
            .join("token-usage.jsonl")
            .to_string_lossy()
            .into_owned()
    }

    /// Open analytics over the standard layout under a bamboo home directory.
    pub fn open_home(home: &Path) -> Result<Self> {
        Self::open_glob(&Self::default_glob(home))
    }

    /// Open analytics over an explicit glob of newline-delimited JSON files.
    ///
    /// `ignore_errors=true` tolerates a partially-written trailing line (the log
    /// is appended to live), and `union_by_name=true` tolerates schema drift if
    /// the record shape ever changes.
    pub fn open_glob(glob: &str) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let view = format!(
            "CREATE VIEW token_usage_raw AS SELECT * FROM read_json_auto('{}', \
             format='newline_delimited', union_by_name=true, ignore_errors=true, filename=true)",
            sql_quote(glob)
        );
        if let Err(error) = conn.execute_batch(&view) {
            let message = error.to_string();
            if message.contains("No files found") || message.contains("IO Error") {
                return Err(AnalyticsError::NoFiles(glob.to_string()));
            }
            return Err(error.into());
        }
        let mut columns = HashSet::new();
        {
            let mut stmt = conn.prepare("PRAGMA table_info('token_usage_raw')")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            for column in rows {
                columns.insert(column?);
            }
        }
        let optional_columns = [
            ("cache_write_input_tokens", "NULL::BIGINT"),
            ("context_management_strategy", "NULL::VARCHAR"),
            ("model_context_epoch", "NULL::BIGINT"),
            ("model_context_reset_reason", "NULL::VARCHAR"),
            ("retrieval_archive_event_count", "NULL::BIGINT"),
            ("latest_retrieval_archive_event_id", "NULL::VARCHAR"),
            ("latest_retrieval_archive_trigger_type", "NULL::VARCHAR"),
        ];
        let missing = optional_columns
            .into_iter()
            .filter(|(name, _)| !columns.contains(*name))
            .map(|(name, default)| format!("{default} AS {name}"))
            .collect::<Vec<_>>();
        let normalized = if missing.is_empty() {
            "CREATE VIEW token_usage AS SELECT * FROM token_usage_raw".to_string()
        } else {
            format!(
                "CREATE VIEW token_usage AS SELECT token_usage_raw.*, {} FROM token_usage_raw",
                missing.join(", ")
            )
        };
        conn.execute_batch(&normalized)?;
        Ok(Self { conn })
    }

    /// Escape hatch for ad-hoc SQL against the `token_usage` view.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// One row per LLM call, ordered by session then time: cache read/creation,
    /// output tokens, the prompt-size estimate, and the cached fraction.
    pub fn round_cache_health(&self) -> Result<Vec<RoundCacheHealth>> {
        let sql = r#"
            SELECT
                COALESCE(session_id, '')::VARCHAR                 AS session_id,
                COALESCE(ts, '')::VARCHAR                         AS ts,
                COALESCE(message_count, 0)::BIGINT                AS message_count,
                COALESCE(cache_read_input_tokens, 0)::BIGINT     AS cache_read,
                COALESCE(cache_creation_input_tokens, 0)::BIGINT AS cache_creation,
                COALESCE(input_tokens, 0)::BIGINT                AS input_tokens,
                COALESCE(output_tokens, 0)::BIGINT               AS output_tokens,
                COALESCE(total_tokens, 0)::BIGINT                AS prompt_tokens_est,
                -- Precise hit ratio when the server-reported input is present:
                -- read / (input + read + creation). Falls back to the budget
                -- estimate for older records that predate the input_tokens field.
                (CASE
                    WHEN COALESCE(input_tokens,0) + COALESCE(cache_read_input_tokens,0)
                         + COALESCE(cache_creation_input_tokens,0) > 0
                    THEN COALESCE(cache_read_input_tokens,0)::DOUBLE
                         / (COALESCE(input_tokens,0) + COALESCE(cache_read_input_tokens,0)
                            + COALESCE(cache_creation_input_tokens,0))
                    WHEN COALESCE(total_tokens, 0) > 0
                    THEN COALESCE(cache_read_input_tokens,0)::DOUBLE / total_tokens
                    ELSE 0 END)::DOUBLE                           AS cached_fraction,
                COALESCE(segments_removed, 0)::BIGINT            AS segments_removed
            FROM token_usage
            ORDER BY session_id, ts
        "#;
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(RoundCacheHealth {
                session_id: row.get(0)?,
                ts: row.get(1)?,
                message_count: row.get(2)?,
                cache_read: row.get(3)?,
                cache_creation: row.get(4)?,
                input_tokens: row.get(5)?,
                output_tokens: row.get(6)?,
                prompt_tokens_est: row.get(7)?,
                cached_fraction: row.get(8)?,
                segments_removed: row.get(9)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// One row per session: call count, totals, average cached fraction, number
    /// of compaction events, and the relative cost breakdown.
    pub fn session_summary(&self) -> Result<Vec<SessionSummary>> {
        let sql = r#"
            SELECT
                COALESCE(session_id, '')::VARCHAR                     AS session_id,
                COUNT(*)::BIGINT                                      AS calls,
                COALESCE(SUM(cache_read_input_tokens), 0)::BIGINT     AS total_cache_read,
                COALESCE(SUM(cache_creation_input_tokens), 0)::BIGINT AS total_cache_creation,
                COALESCE(SUM(input_tokens), 0)::BIGINT                AS total_input,
                COALESCE(SUM(output_tokens), 0)::BIGINT               AS total_output,
                COALESCE(AVG(CASE
                          WHEN COALESCE(input_tokens,0) + COALESCE(cache_read_input_tokens,0)
                               + COALESCE(cache_creation_input_tokens,0) > 0
                          THEN cache_read_input_tokens::DOUBLE
                               / (COALESCE(input_tokens,0) + COALESCE(cache_read_input_tokens,0)
                                  + COALESCE(cache_creation_input_tokens,0))
                          WHEN COALESCE(total_tokens,0) > 0
                          THEN cache_read_input_tokens::DOUBLE / total_tokens
                          ELSE 0 END), 0)::DOUBLE                     AS avg_cached_fraction,
                COALESCE(SUM(CASE WHEN COALESCE(segments_removed,0) > 0 THEN 1 ELSE 0 END), 0)::BIGINT
                                                                     AS compactions,
                (COALESCE(SUM(cache_read_input_tokens), 0)::DOUBLE * 0.1)::DOUBLE   AS est_read_cost,
                (COALESCE(SUM(cache_read_input_tokens), 0)::DOUBLE * 0.9)::DOUBLE   AS est_savings_vs_uncached,
                (COALESCE(SUM(cache_creation_input_tokens), 0)::DOUBLE * 1.0)::DOUBLE AS est_write_premium
            FROM token_usage
            GROUP BY session_id
            ORDER BY total_cache_read DESC
        "#;
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(SessionSummary {
                session_id: row.get(0)?,
                calls: row.get(1)?,
                total_cache_read: row.get(2)?,
                total_cache_creation: row.get(3)?,
                total_input: row.get(4)?,
                total_output: row.get(5)?,
                avg_cached_fraction: row.get(6)?,
                compactions: row.get(7)?,
                est_read_cost: row.get(8)?,
                est_savings_vs_uncached: row.get(9)?,
                est_write_premium: row.get(10)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Compaction events (`segments_removed > 0`) with the cache read on that
    /// round and on the following round — the post-compaction "cold" round where
    /// the rewritten summary forces a re-read.
    pub fn compaction_events(&self) -> Result<Vec<CompactionEvent>> {
        let sql = r#"
            WITH ordered AS (
                SELECT
                    COALESCE(session_id, '')                     AS session_id,
                    COALESCE(ts, '')                             AS ts,
                    COALESCE(segments_removed, 0)                AS segments_removed,
                    COALESCE(cache_read_input_tokens, 0)         AS cache_read,
                    LEAD(COALESCE(cache_read_input_tokens, 0))
                        OVER (PARTITION BY session_id ORDER BY ts)     AS next_read,
                    LEAD(COALESCE(cache_creation_input_tokens, 0))
                        OVER (PARTITION BY session_id ORDER BY ts)     AS next_creation
                FROM token_usage
            )
            SELECT
                session_id::VARCHAR        AS session_id,
                ts::VARCHAR                AS ts,
                segments_removed::BIGINT   AS segments_removed,
                cache_read::BIGINT         AS cache_read_this_round,
                COALESCE(next_read, 0)::BIGINT     AS cache_read_next_round,
                COALESCE(next_creation, 0)::BIGINT AS cache_creation_next_round
            FROM ordered
            WHERE segments_removed > 0
            ORDER BY session_id, ts
        "#;
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(CompactionEvent {
                session_id: row.get(0)?,
                ts: row.get(1)?,
                segments_removed: row.get(2)?,
                cache_read_this_round: row.get(3)?,
                cache_read_next_round: row.get(4)?,
                cache_creation_next_round: row.get(5)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Rounds that follow a pause longer than `min_gap_seconds` (default the
    /// 5-minute TTL). If `cache_read_after > 0`, the cache survived the gap —
    /// direct evidence the 1-hour extended TTL is working.
    pub fn pause_survival(&self, min_gap_seconds: f64) -> Result<Vec<PauseSurvival>> {
        let sql = r#"
            WITH ordered AS (
                SELECT
                    COALESCE(session_id, '')             AS session_id,
                    COALESCE(ts, '')                     AS ts,
                    COALESCE(cache_read_input_tokens, 0) AS cache_read,
                    LAG(COALESCE(cache_read_input_tokens, 0))
                        OVER (PARTITION BY session_id ORDER BY ts)  AS prev_read,
                    epoch(TRY_CAST(ts AS TIMESTAMP))
                      - epoch(LAG(TRY_CAST(ts AS TIMESTAMP))
                          OVER (PARTITION BY session_id ORDER BY ts)) AS gap_seconds
                FROM token_usage
            )
            SELECT
                session_id::VARCHAR        AS session_id,
                ts::VARCHAR                AS ts,
                COALESCE(gap_seconds, 0)::DOUBLE   AS gap_seconds,
                COALESCE(prev_read, 0)::BIGINT     AS cache_read_before,
                cache_read::BIGINT         AS cache_read_after
            FROM ordered
            WHERE COALESCE(gap_seconds, 0) > ?
            ORDER BY gap_seconds DESC
        "#;
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([min_gap_seconds], |row| {
            Ok(PauseSurvival {
                session_id: row.get(0)?,
                ts: row.get(1)?,
                gap_seconds: row.get(2)?,
                cache_read_before: row.get(3)?,
                cache_read_after: row.get(4)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Aggregate provider-cache behavior by effective context strategy, model,
    /// provider, and model-context epoch. Legacy rows remain visible under the
    /// `unavailable` strategy with a null epoch rather than being mislabelled as
    /// either strategy or epoch zero.
    pub fn strategy_epoch_cache_health(&self) -> Result<Vec<StrategyEpochCacheHealth>> {
        let sql = r#"
            WITH normalized AS (
                SELECT
                    COALESCE(context_management_strategy, 'unavailable')::VARCHAR AS strategy,
                    COALESCE(provider, '')::VARCHAR AS provider,
                    COALESCE(model, '')::VARCHAR AS model,
                    model_context_epoch::BIGINT AS model_context_epoch,
                    COALESCE(session_id, '')::VARCHAR AS session_id,
                    COALESCE(input_tokens, 0)::BIGINT AS input_tokens,
                    COALESCE(output_tokens, 0)::BIGINT AS output_tokens,
                    COALESCE(cache_read_input_tokens, 0)::BIGINT AS cache_read,
                    COALESCE(cache_creation_input_tokens, 0)::BIGINT AS cache_creation,
                    COALESCE(cache_write_input_tokens, 0)::BIGINT AS cache_write,
                    CASE
                        WHEN COALESCE(input_tokens, 0)
                           + COALESCE(cache_read_input_tokens, 0)
                           + COALESCE(cache_creation_input_tokens, 0) > 0
                        THEN COALESCE(input_tokens, 0)
                           + COALESCE(cache_read_input_tokens, 0)
                           + COALESCE(cache_creation_input_tokens, 0)
                        ELSE COALESCE(total_tokens, 0)
                    END::BIGINT AS prompt_input_tokens,
                    COALESCE(truncation_occurred, false) AS truncation_occurred,
                    (COALESCE(budget_limit, 0) > 0
                     AND COALESCE(total_tokens, 0) > COALESCE(budget_limit, 0)) AS budget_overflow
                FROM token_usage
            )
            SELECT
                strategy,
                provider,
                model,
                model_context_epoch,
                COUNT(DISTINCT session_id)::BIGINT AS sessions,
                COUNT(*)::BIGINT AS calls,
                COALESCE(SUM(prompt_input_tokens), 0)::BIGINT AS prompt_input_tokens,
                COALESCE(SUM(input_tokens), 0)::BIGINT AS fresh_input_tokens,
                COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
                COALESCE(SUM(cache_read), 0)::BIGINT AS cache_read_tokens,
                COALESCE(SUM(cache_creation), 0)::BIGINT AS cache_creation_tokens,
                COALESCE(SUM(cache_write), 0)::BIGINT AS cache_write_tokens,
                CASE WHEN COALESCE(SUM(prompt_input_tokens), 0) > 0
                     THEN COALESCE(SUM(cache_read), 0)::DOUBLE
                          / SUM(prompt_input_tokens)::DOUBLE
                     ELSE 0 END::DOUBLE AS cached_fraction,
                COALESCE(SUM(CASE WHEN truncation_occurred THEN 1 ELSE 0 END), 0)::BIGINT
                    AS truncation_calls,
                COALESCE(SUM(CASE WHEN budget_overflow THEN 1 ELSE 0 END), 0)::BIGINT
                    AS budget_overflow_calls,
                COALESCE(quantile_cont(prompt_input_tokens, 0.50), 0)::DOUBLE AS prompt_tokens_p50,
                COALESCE(quantile_cont(prompt_input_tokens, 0.95), 0)::DOUBLE AS prompt_tokens_p95,
                COALESCE(quantile_cont(prompt_input_tokens, 0.99), 0)::DOUBLE AS prompt_tokens_p99
            FROM normalized
            GROUP BY strategy, provider, model, model_context_epoch
            ORDER BY strategy, provider, model, model_context_epoch NULLS FIRST
        "#;
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(StrategyEpochCacheHealth {
                strategy: row.get(0)?,
                provider: row.get(1)?,
                model: row.get(2)?,
                model_context_epoch: row.get(3)?,
                sessions: row.get(4)?,
                calls: row.get(5)?,
                prompt_input_tokens: row.get(6)?,
                fresh_input_tokens: row.get(7)?,
                output_tokens: row.get(8)?,
                cache_read_tokens: row.get(9)?,
                cache_creation_tokens: row.get(10)?,
                cache_write_tokens: row.get(11)?,
                cached_fraction: row.get(12)?,
                truncation_calls: row.get(13)?,
                budget_overflow_calls: row.get(14)?,
                prompt_tokens_p50: row.get(15)?,
                prompt_tokens_p95: row.get(16)?,
                prompt_tokens_p99: row.get(17)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// The first completed provider call observed in each reset epoch, plus the
    /// following call when present. This uses the explicit epoch/reset fields;
    /// it never guesses a boundary from `segments_removed`.
    pub fn context_boundary_calls(&self) -> Result<Vec<ContextBoundaryCall>> {
        let sql = r#"
            WITH calls AS (
                SELECT
                    COALESCE(session_id, '')::VARCHAR AS session_id,
                    COALESCE(ts, '')::VARCHAR AS ts,
                    COALESCE(context_management_strategy, 'unavailable')::VARCHAR AS strategy,
                    model_context_epoch::BIGINT AS model_context_epoch,
                    model_context_reset_reason::VARCHAR AS reset_reason,
                    retrieval_archive_event_count::BIGINT AS retrieval_archive_event_count,
                    latest_retrieval_archive_event_id::VARCHAR AS latest_retrieval_archive_event_id,
                    latest_retrieval_archive_trigger_type::VARCHAR AS latest_retrieval_archive_trigger_type,
                    COALESCE(message_count, 0)::BIGINT AS message_count,
                    COALESCE(filename, '')::VARCHAR AS filename,
                    COALESCE(input_tokens, 0)::BIGINT AS input_tokens,
                    COALESCE(output_tokens, 0)::BIGINT AS output_tokens,
                    COALESCE(cache_read_input_tokens, 0)::BIGINT AS cache_read,
                    COALESCE(cache_creation_input_tokens, 0)::BIGINT AS cache_creation,
                    COALESCE(cache_write_input_tokens, 0)::BIGINT AS cache_write,
                    CASE
                        WHEN COALESCE(input_tokens, 0)
                           + COALESCE(cache_read_input_tokens, 0)
                           + COALESCE(cache_creation_input_tokens, 0) > 0
                        THEN COALESCE(input_tokens, 0)
                           + COALESCE(cache_read_input_tokens, 0)
                           + COALESCE(cache_creation_input_tokens, 0)
                        ELSE COALESCE(total_tokens, 0)
                    END::BIGINT AS prompt_input_tokens,
                    COALESCE(truncation_occurred, false) AS truncation_occurred,
                    (COALESCE(budget_limit, 0) > 0
                     AND COALESCE(total_tokens, 0) > COALESCE(budget_limit, 0)) AS budget_overflow
                FROM token_usage
            ), ordered AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY session_id, model_context_epoch
                        ORDER BY ts, message_count, filename
                    ) AS epoch_call_ordinal,
                    LEAD(cache_read) OVER (
                        PARTITION BY session_id, model_context_epoch
                        ORDER BY ts, message_count, filename
                    ) AS next_cache_read,
                    LEAD(cache_creation) OVER (
                        PARTITION BY session_id, model_context_epoch
                        ORDER BY ts, message_count, filename
                    ) AS next_cache_creation
                FROM calls
            )
            SELECT
                session_id,
                ts,
                strategy,
                model_context_epoch,
                reset_reason,
                retrieval_archive_event_count,
                latest_retrieval_archive_event_id,
                latest_retrieval_archive_trigger_type,
                prompt_input_tokens,
                input_tokens,
                output_tokens,
                cache_read,
                cache_creation,
                cache_write,
                truncation_occurred,
                budget_overflow,
                next_cache_read,
                next_cache_creation
            FROM ordered
            WHERE epoch_call_ordinal = 1
              AND model_context_epoch IS NOT NULL
              AND reset_reason IS NOT NULL
            ORDER BY session_id, model_context_epoch, ts
        "#;
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(ContextBoundaryCall {
                session_id: row.get(0)?,
                ts: row.get(1)?,
                strategy: row.get(2)?,
                model_context_epoch: row.get(3)?,
                reset_reason: row.get(4)?,
                retrieval_archive_event_count: row.get(5)?,
                latest_retrieval_archive_event_id: row.get(6)?,
                latest_retrieval_archive_trigger_type: row.get(7)?,
                prompt_input_tokens: row.get(8)?,
                fresh_input_tokens: row.get(9)?,
                output_tokens: row.get(10)?,
                cache_read_tokens: row.get(11)?,
                cache_creation_tokens: row.get(12)?,
                cache_write_tokens: row.get(13)?,
                truncation_occurred: row.get(14)?,
                budget_overflow: row.get(15)?,
                next_cache_read_tokens: row.get(16)?,
                next_cache_creation_tokens: row.get(17)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

/// Read the existing `metrics.db` tool-call table without adding a second
/// runtime writer. No tool arguments, query text, results, paths, or errors are
/// selected by this report.
pub fn session_history_tool_metrics(metrics_db: &Path) -> Result<HistoryToolMetrics> {
    let conn = SqliteConnection::open_with_flags(metrics_db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut stmt = conn.prepare(
        "SELECT success, \
                CASE WHEN completed_at IS NULL THEN NULL \
                     ELSE CAST(ROUND((julianday(completed_at) - julianday(started_at)) \
                                     * 86400000.0) AS INTEGER) END \
         FROM tool_call_metrics \
         WHERE tool_name = 'session_history_current' \
         ORDER BY started_at, tool_call_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    let mut calls = 0u64;
    let mut succeeded = 0u64;
    let mut failed = 0u64;
    let mut incomplete = 0u64;
    let mut latencies = Vec::new();
    for row in rows {
        let (success, latency_ms) = row?;
        calls += 1;
        match success {
            Some(value) if value > 0 => succeeded += 1,
            Some(_) => failed += 1,
            None => incomplete += 1,
        }
        if let Some(latency_ms) = latency_ms.and_then(|value| u64::try_from(value).ok()) {
            latencies.push(latency_ms);
        }
    }
    latencies.sort_unstable();
    let completed = succeeded + failed;
    Ok(HistoryToolMetrics {
        calls,
        succeeded,
        failed,
        incomplete,
        success_rate: (completed > 0).then(|| succeeded as f64 / completed as f64),
        latency_ms_p50: nearest_rank_u64(&latencies, 0.50),
        latency_ms_p95: nearest_rank_u64(&latencies, 0.95),
        latency_ms_p99: nearest_rank_u64(&latencies, 0.99),
    })
}

fn nearest_rank_u64(sorted: &[u64], percentile: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.saturating_sub(1)).copied()
}

/// One LLM call's cache picture. See [`TokenUsageDb::round_cache_health`].
#[derive(Debug, Clone, Serialize)]
pub struct RoundCacheHealth {
    pub session_id: String,
    pub ts: String,
    pub message_count: i64,
    pub cache_read: i64,
    pub cache_creation: i64,
    /// Server-reported non-cached fresh input tokens (`0` for records that
    /// predate the field).
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// Prompt-side size estimate (the budget snapshot's `total_tokens`), kept
    /// for reference. The exact denominator for `cached_fraction` is
    /// `input_tokens + cache_read + cache_creation` when `input_tokens` is
    /// present; otherwise this estimate is used.
    pub prompt_tokens_est: i64,
    /// `cache_read / (input_tokens + cache_read + cache_creation)` — exact when
    /// `input_tokens` is present.
    pub cached_fraction: f64,
    pub segments_removed: i64,
}

/// Aggregate per session. See [`TokenUsageDb::session_summary`].
#[derive(Debug, Clone, Serialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub calls: i64,
    pub total_cache_read: i64,
    pub total_cache_creation: i64,
    pub total_input: i64,
    pub total_output: i64,
    pub avg_cached_fraction: f64,
    pub compactions: i64,
    /// `read * 0.1` — what the cached reads actually cost.
    pub est_read_cost: f64,
    /// `read * 0.9` — saved versus paying full price for those tokens.
    pub est_savings_vs_uncached: f64,
    /// `creation * 1.0` — premium paid for 1-hour cache writes over normal input.
    pub est_write_premium: f64,
}

/// A compaction event and its post-compaction cold round.
#[derive(Debug, Clone, Serialize)]
pub struct CompactionEvent {
    pub session_id: String,
    pub ts: String,
    pub segments_removed: i64,
    pub cache_read_this_round: i64,
    pub cache_read_next_round: i64,
    pub cache_creation_next_round: i64,
}

/// A round after a pause longer than the TTL. See [`TokenUsageDb::pause_survival`].
#[derive(Debug, Clone, Serialize)]
pub struct PauseSurvival {
    pub session_id: String,
    pub ts: String,
    pub gap_seconds: f64,
    pub cache_read_before: i64,
    pub cache_read_after: i64,
}

/// Provider-cache aggregate for one effective strategy/model/provider/epoch.
#[derive(Debug, Clone, Serialize)]
pub struct StrategyEpochCacheHealth {
    pub strategy: String,
    pub provider: String,
    pub model: String,
    pub model_context_epoch: Option<i64>,
    pub sessions: i64,
    pub calls: i64,
    pub prompt_input_tokens: i64,
    pub fresh_input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_creation_tokens: i64,
    pub cache_write_tokens: i64,
    pub cached_fraction: f64,
    pub truncation_calls: i64,
    pub budget_overflow_calls: i64,
    pub prompt_tokens_p50: f64,
    pub prompt_tokens_p95: f64,
    pub prompt_tokens_p99: f64,
}

/// First completed call in a reset epoch and its immediate warm successor.
#[derive(Debug, Clone, Serialize)]
pub struct ContextBoundaryCall {
    pub session_id: String,
    pub ts: String,
    pub strategy: String,
    pub model_context_epoch: i64,
    pub reset_reason: String,
    pub retrieval_archive_event_count: Option<i64>,
    pub latest_retrieval_archive_event_id: Option<String>,
    pub latest_retrieval_archive_trigger_type: Option<String>,
    pub prompt_input_tokens: i64,
    pub fresh_input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_creation_tokens: i64,
    pub cache_write_tokens: i64,
    pub truncation_occurred: bool,
    pub budget_overflow: bool,
    pub next_cache_read_tokens: Option<i64>,
    pub next_cache_creation_tokens: Option<i64>,
}

/// Aggregate from the existing `metrics.db` `tool_call_metrics` table.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct HistoryToolMetrics {
    pub calls: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub incomplete: u64,
    pub success_rate: Option<f64>,
    pub latency_ms_p50: Option<u64>,
    pub latency_ms_p95: Option<u64>,
    pub latency_ms_p99: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::Path;

    fn write_fixture(home: &Path) {
        let sdir = home.join("sessions").join("s1");
        std::fs::create_dir_all(&sdir).unwrap();
        let mut f = std::fs::File::create(sdir.join("token-usage.jsonl")).unwrap();
        // Round 1: cold (first call writes the cache; whole prompt is fresh).
        writeln!(f, r#"{{"ts":"2026-06-15T00:00:00Z","session_id":"s1","model":"m","provider":"anthropic","message_count":2,"cache_creation_input_tokens":5000,"cache_read_input_tokens":0,"input_tokens":5000,"output_tokens":100,"thinking_tokens":0,"system_tokens":5000,"summary_tokens":0,"window_tokens":0,"total_tokens":10000,"max_context_tokens":200000,"budget_limit":180000,"prompt_cached_tool_outputs":0,"prompt_cached_tool_tokens_saved":0,"truncation_occurred":false,"segments_removed":0}}"#).unwrap();
        // Round 2: warm hit, 10 minutes later (gap > 5min TTL → 1h TTL survived).
        writeln!(f, r#"{{"ts":"2026-06-15T00:10:00Z","session_id":"s1","model":"m","provider":"anthropic","message_count":4,"cache_creation_input_tokens":500,"cache_read_input_tokens":10000,"input_tokens":1000,"output_tokens":120,"thinking_tokens":0,"system_tokens":5000,"summary_tokens":0,"window_tokens":0,"total_tokens":12000,"max_context_tokens":200000,"budget_limit":180000,"prompt_cached_tool_outputs":0,"prompt_cached_tool_tokens_saved":0,"truncation_occurred":false,"segments_removed":0}}"#).unwrap();
        // Round 3: compaction event (segments_removed > 0).
        writeln!(f, r#"{{"ts":"2026-06-15T00:11:00Z","session_id":"s1","model":"m","provider":"anthropic","message_count":6,"cache_creation_input_tokens":3000,"cache_read_input_tokens":4000,"input_tokens":2000,"output_tokens":80,"thinking_tokens":0,"system_tokens":5000,"summary_tokens":2000,"window_tokens":0,"total_tokens":9000,"max_context_tokens":200000,"budget_limit":180000,"prompt_cached_tool_outputs":0,"prompt_cached_tool_tokens_saved":0,"truncation_occurred":false,"segments_removed":12}}"#).unwrap();
        // Round 4: the cold round right after compaction.
        writeln!(f, r#"{{"ts":"2026-06-15T00:12:00Z","session_id":"s1","model":"m","provider":"anthropic","message_count":8,"cache_creation_input_tokens":1000,"cache_read_input_tokens":9000,"input_tokens":500,"output_tokens":90,"thinking_tokens":0,"system_tokens":5000,"summary_tokens":2000,"window_tokens":0,"total_tokens":10000,"max_context_tokens":200000,"budget_limit":180000,"prompt_cached_tool_outputs":0,"prompt_cached_tool_tokens_saved":0,"truncation_occurred":false,"segments_removed":0}}"#).unwrap();
    }

    fn write_mixed_schema_fixture(home: &Path) {
        let sdir = home.join("sessions").join("mixed");
        std::fs::create_dir_all(&sdir).unwrap();
        let mut f = std::fs::File::create(sdir.join("token-usage.jsonl")).unwrap();
        writeln!(f, r#"{{"ts":"2026-06-15T00:00:00Z","session_id":"mixed","model":"m","provider":"anthropic","message_count":2,"cache_creation_input_tokens":500,"cache_read_input_tokens":0,"input_tokens":500,"output_tokens":10,"total_tokens":1000,"budget_limit":900,"truncation_occurred":false,"segments_removed":0}}"#).unwrap();
        writeln!(f, r#"{{"ts":"2026-06-15T00:01:00Z","session_id":"mixed","model":"m","provider":"anthropic","message_count":4,"cache_creation_input_tokens":900,"cache_read_input_tokens":100,"cache_write_input_tokens":40,"input_tokens":1000,"output_tokens":20,"total_tokens":2000,"budget_limit":1800,"truncation_occurred":true,"segments_removed":0,"context_management_strategy":"summary","model_context_epoch":1,"model_context_reset_reason":"compression","retrieval_archive_event_count":0}}"#).unwrap();
        writeln!(f, r#"{{"ts":"2026-06-15T00:02:00Z","session_id":"mixed","model":"m","provider":"anthropic","message_count":6,"cache_creation_input_tokens":100,"cache_read_input_tokens":800,"cache_write_input_tokens":20,"input_tokens":100,"output_tokens":30,"total_tokens":1000,"budget_limit":1800,"truncation_occurred":false,"segments_removed":0,"context_management_strategy":"summary","model_context_epoch":1,"model_context_reset_reason":"compression","retrieval_archive_event_count":0}}"#).unwrap();
        writeln!(f, r#"{{"ts":"2026-06-15T00:03:00Z","session_id":"mixed","model":"m","provider":"anthropic","message_count":8,"cache_creation_input_tokens":700,"cache_read_input_tokens":200,"cache_write_input_tokens":30,"input_tokens":600,"output_tokens":40,"total_tokens":1500,"budget_limit":1800,"truncation_occurred":false,"segments_removed":0,"context_management_strategy":"retrieval_window","model_context_epoch":2,"model_context_reset_reason":"compression","retrieval_archive_event_count":1,"latest_retrieval_archive_event_id":"archive-1","latest_retrieval_archive_trigger_type":"auto"}}"#).unwrap();
        writeln!(f, r#"{{"ts":"2026-06-15T00:04:00Z","session_id":"mixed","model":"m","provider":"anthropic","message_count":10,"cache_creation_input_tokens":100,"cache_read_input_tokens":900,"cache_write_input_tokens":10,"input_tokens":100,"output_tokens":50,"total_tokens":1100,"budget_limit":1800,"truncation_occurred":false,"segments_removed":0,"context_management_strategy":"retrieval_window","model_context_epoch":2,"model_context_reset_reason":"compression","retrieval_archive_event_count":1,"latest_retrieval_archive_event_id":"archive-1","latest_retrieval_archive_trigger_type":"auto"}}"#).unwrap();
    }

    #[test]
    fn round_cache_health_reads_and_computes_fraction() {
        let tmp = tempfile::tempdir().unwrap();
        write_fixture(tmp.path());
        let db = TokenUsageDb::open_home(tmp.path()).unwrap();

        let rows = db.round_cache_health().unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].cache_read, 0);
        assert_eq!(rows[1].cache_read, 10000);
        assert_eq!(rows[1].input_tokens, 1000);
        // Precise ratio: read / (input + read + creation) = 10000 / 11500.
        assert!((rows[1].cached_fraction - (10000.0 / 11500.0)).abs() < 1e-9);
    }

    #[test]
    fn session_summary_aggregates_and_costs() {
        let tmp = tempfile::tempdir().unwrap();
        write_fixture(tmp.path());
        let db = TokenUsageDb::open_home(tmp.path()).unwrap();

        let summary = db.session_summary().unwrap();
        assert_eq!(summary.len(), 1);
        let s = &summary[0];
        assert_eq!(s.calls, 4);
        assert_eq!(s.total_cache_read, 23000); // 0 + 10000 + 4000 + 9000
        assert_eq!(s.compactions, 1);
        assert!((s.est_savings_vs_uncached - 23000.0 * 0.9).abs() < 1e-6);
    }

    #[test]
    fn compaction_events_capture_next_round() {
        let tmp = tempfile::tempdir().unwrap();
        write_fixture(tmp.path());
        let db = TokenUsageDb::open_home(tmp.path()).unwrap();

        let events = db.compaction_events().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].segments_removed, 12);
        assert_eq!(events[0].cache_read_this_round, 4000);
        assert_eq!(events[0].cache_read_next_round, 9000);
    }

    #[test]
    fn pause_survival_flags_gaps_over_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        write_fixture(tmp.path());
        let db = TokenUsageDb::open_home(tmp.path()).unwrap();

        // Only the 00:00 → 00:10 gap (600s) exceeds the 300s default.
        let pauses = db.pause_survival(DEFAULT_TTL_SECONDS).unwrap();
        assert_eq!(pauses.len(), 1);
        assert!((pauses[0].gap_seconds - 600.0).abs() < 1e-6);
        // Cache read stayed non-zero across the pause → 1h TTL survived.
        assert_eq!(pauses[0].cache_read_after, 10000);
    }

    #[test]
    fn legacy_only_strategy_epoch_is_explicitly_unavailable() {
        let tmp = tempfile::tempdir().unwrap();
        write_fixture(tmp.path());
        let db = TokenUsageDb::open_home(tmp.path()).unwrap();

        let rows = db.strategy_epoch_cache_health().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].strategy, "unavailable");
        assert_eq!(rows[0].model_context_epoch, None);
        assert!(db.context_boundary_calls().unwrap().is_empty());
    }

    #[test]
    fn mixed_schema_groups_epochs_and_finds_explicit_boundaries() {
        let tmp = tempfile::tempdir().unwrap();
        write_mixed_schema_fixture(tmp.path());
        let db = TokenUsageDb::open_home(tmp.path()).unwrap();

        let rows = db.strategy_epoch_cache_health().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].strategy, "retrieval_window");
        assert_eq!(rows[0].model_context_epoch, Some(2));
        assert_eq!(rows[0].calls, 2);
        assert_eq!(rows[0].cache_read_tokens, 1100);
        assert_eq!(rows[1].strategy, "summary");
        assert_eq!(rows[1].model_context_epoch, Some(1));
        assert_eq!(rows[1].truncation_calls, 1);
        assert_eq!(rows[1].budget_overflow_calls, 1);
        assert_eq!(rows[2].strategy, "unavailable");
        assert_eq!(rows[2].model_context_epoch, None);

        let boundaries = db.context_boundary_calls().unwrap();
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0].strategy, "summary");
        assert_eq!(boundaries[0].model_context_epoch, 1);
        assert_eq!(boundaries[0].cache_read_tokens, 100);
        assert_eq!(boundaries[0].next_cache_read_tokens, Some(800));
        assert_eq!(boundaries[1].strategy, "retrieval_window");
        assert_eq!(
            boundaries[1].latest_retrieval_archive_event_id.as_deref(),
            Some("archive-1")
        );
        assert_eq!(boundaries[1].next_cache_read_tokens, Some(900));
    }

    #[test]
    fn context_boundary_warm_call_never_crosses_into_the_next_epoch() {
        let tmp = tempfile::tempdir().unwrap();
        let sdir = tmp.path().join("sessions").join("boundary-only");
        std::fs::create_dir_all(&sdir).unwrap();
        let mut file = std::fs::File::create(sdir.join("token-usage.jsonl")).unwrap();
        for row in [
            r#"{"ts":"2026-06-15T00:00:00Z","session_id":"boundary-only","model":"m","provider":"anthropic","message_count":2,"cache_creation_input_tokens":500,"cache_read_input_tokens":0,"input_tokens":500,"output_tokens":10,"total_tokens":1000,"budget_limit":1800,"truncation_occurred":false,"segments_removed":0,"context_management_strategy":"summary","model_context_epoch":1,"model_context_reset_reason":"compression","retrieval_archive_event_count":0}"#,
            r#"{"ts":"2026-06-15T00:01:00Z","session_id":"boundary-only","model":"m","provider":"anthropic","message_count":4,"cache_creation_input_tokens":700,"cache_read_input_tokens":100,"input_tokens":700,"output_tokens":20,"total_tokens":1500,"budget_limit":1800,"truncation_occurred":false,"segments_removed":0,"context_management_strategy":"retrieval_window","model_context_epoch":2,"model_context_reset_reason":"compression","retrieval_archive_event_count":1}"#,
        ] {
            writeln!(file, "{row}").unwrap();
        }

        let db = TokenUsageDb::open_home(tmp.path()).unwrap();
        let boundaries = db.context_boundary_calls().unwrap();
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0].model_context_epoch, 1);
        assert_eq!(boundaries[0].next_cache_read_tokens, None);
        assert_eq!(boundaries[0].next_cache_creation_tokens, None);
    }

    #[test]
    fn existing_history_tool_metrics_are_read_without_tool_content() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("metrics.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE tool_call_metrics (
                tool_call_id TEXT PRIMARY KEY,
                round_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                success INTEGER,
                error TEXT
             );
             INSERT INTO tool_call_metrics VALUES
               ('a','r','s','session_history_current','2026-01-01T00:00:00.000Z','2026-01-01T00:00:00.010Z',1,NULL),
               ('b','r','s','session_history_current','2026-01-01T00:00:01.000Z','2026-01-01T00:00:01.020Z',0,'redacted by query'),
               ('c','r','s','session_history_current','2026-01-01T00:00:02.000Z',NULL,NULL,NULL),
               ('d','r','s','Read','2026-01-01T00:00:03.000Z','2026-01-01T00:00:03.001Z',1,NULL);",
        )
        .unwrap();
        drop(conn);

        let metrics = session_history_tool_metrics(&path).unwrap();
        assert_eq!(metrics.calls, 3);
        assert_eq!(metrics.succeeded, 1);
        assert_eq!(metrics.failed, 1);
        assert_eq!(metrics.incomplete, 1);
        assert_eq!(metrics.success_rate, Some(0.5));
        assert_eq!(metrics.latency_ms_p50, Some(10));
        assert_eq!(metrics.latency_ms_p95, Some(20));
        assert_eq!(metrics.latency_ms_p99, Some(20));
    }

    #[test]
    fn missing_glob_is_a_clear_error() {
        let tmp = tempfile::tempdir().unwrap();
        let result = TokenUsageDb::open_home(tmp.path());
        assert!(
            matches!(result, Err(AnalyticsError::NoFiles(_))),
            "empty home should yield a NoFiles error"
        );
    }
}
