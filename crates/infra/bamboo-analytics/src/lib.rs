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

use std::path::Path;

use duckdb::Connection;
use serde::Serialize;

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
            "CREATE VIEW token_usage AS SELECT * FROM read_json_auto('{}', \
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
    fn missing_glob_is_a_clear_error() {
        let tmp = tempfile::tempdir().unwrap();
        let result = TokenUsageDb::open_home(tmp.path());
        assert!(
            matches!(result, Err(AnalyticsError::NoFiles(_))),
            "empty home should yield a NoFiles error"
        );
    }
}
