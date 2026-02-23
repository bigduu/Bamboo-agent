use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use thiserror::Error;

use crate::types::{
    DailyMetrics, ForwardEndpointMetrics, ForwardMetricsFilter, ForwardMetricsSummary,
    ForwardRequestMetrics, ForwardStatus, MetricsDateFilter, MetricsSummary, ModelMetrics,
    RoundMetrics, RoundStatus, SessionDetail, SessionMetrics, SessionMetricsFilter, SessionStatus,
    TokenUsage, ToolCallMetrics,
};

pub type MetricsResult<T> = Result<T, MetricsError>;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("time parse error: {0}")]
    Chrono(#[from] chrono::ParseError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage task join error: {0}")]
    Task(String),

    #[error("invalid metrics data: {0}")]
    InvalidData(String),
}

#[derive(Debug, Clone)]
pub struct ToolCallCompletion {
    pub completed_at: DateTime<Utc>,
    pub success: bool,
    pub error: Option<String>,
}

#[async_trait]
pub trait MetricsStorage: Send + Sync {
    async fn init(&self) -> MetricsResult<()>;

    async fn upsert_session_start(
        &self,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    async fn update_session_message_count(
        &self,
        session_id: &str,
        message_count: u32,
        updated_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    async fn complete_session(
        &self,
        session_id: &str,
        status: SessionStatus,
        completed_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    async fn insert_round_start(
        &self,
        round_id: &str,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    async fn complete_round(
        &self,
        round_id: &str,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        error: Option<String>,
    ) -> MetricsResult<()>;

    async fn insert_tool_start(
        &self,
        tool_call_id: &str,
        round_id: &str,
        session_id: &str,
        tool_name: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    async fn complete_tool_call(
        &self,
        tool_call_id: &str,
        completion: ToolCallCompletion,
    ) -> MetricsResult<()>;

    // Forward request metrics methods
    async fn insert_forward_start(
        &self,
        forward_id: &str,
        endpoint: &str,
        model: &str,
        is_stream: bool,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    async fn complete_forward(
        &self,
        forward_id: &str,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: ForwardStatus,
        usage: Option<TokenUsage>,
        error: Option<String>,
    ) -> MetricsResult<()>;

    async fn forward_summary(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<ForwardMetricsSummary>;
    async fn forward_by_endpoint(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardEndpointMetrics>>;
    async fn forward_requests(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardRequestMetrics>>;

    async fn forward_daily_metrics(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>>;

    async fn summary(&self, filter: MetricsDateFilter) -> MetricsResult<MetricsSummary>;
    async fn by_model(&self, filter: MetricsDateFilter) -> MetricsResult<Vec<ModelMetrics>>;
    async fn sessions(&self, filter: SessionMetricsFilter) -> MetricsResult<Vec<SessionMetrics>>;
    async fn session_detail(&self, session_id: &str) -> MetricsResult<Option<SessionDetail>>;
    async fn daily_metrics(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>>;

    async fn prune_rounds_before(&self, cutoff: DateTime<Utc>) -> MetricsResult<u64>;
}

#[derive(Debug, Clone)]
pub struct SqliteMetricsStorage {
    db_path: PathBuf,
}

impl SqliteMetricsStorage {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_path_buf(),
        }
    }

    async fn with_connection<T, F>(&self, func: F) -> MetricsResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> MetricsResult<T> + Send + 'static,
    {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&db_path)?;
            func(&connection)
        })
        .await
        .map_err(|error| MetricsError::Task(error.to_string()))?
    }
}

#[async_trait]
impl MetricsStorage for SqliteMetricsStorage {
    async fn init(&self) -> MetricsResult<()> {
        self.with_connection(|connection| {
            connection.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS session_metrics (
                    session_id TEXT PRIMARY KEY,
                    model TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL DEFAULT 'running',
                    total_rounds INTEGER NOT NULL DEFAULT 0,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    tool_call_count INTEGER NOT NULL DEFAULT 0,
                    message_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS round_metrics (
                    round_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    model TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL DEFAULT 'running',
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    FOREIGN KEY(session_id) REFERENCES session_metrics(session_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS tool_call_metrics (
                    tool_call_id TEXT PRIMARY KEY,
                    round_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    success INTEGER,
                    error TEXT,
                    FOREIGN KEY(round_id) REFERENCES round_metrics(round_id) ON DELETE CASCADE,
                    FOREIGN KEY(session_id) REFERENCES session_metrics(session_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_session_started_at ON session_metrics(started_at);
                CREATE INDEX IF NOT EXISTS idx_session_model ON session_metrics(model);
                CREATE INDEX IF NOT EXISTS idx_round_session ON round_metrics(session_id);
                CREATE INDEX IF NOT EXISTS idx_tool_session ON tool_call_metrics(session_id);
                CREATE INDEX IF NOT EXISTS idx_tool_started_at ON tool_call_metrics(started_at);
                CREATE INDEX IF NOT EXISTS idx_tool_name ON tool_call_metrics(tool_name);

                CREATE TABLE IF NOT EXISTS forward_request_metrics (
                    forward_id TEXT PRIMARY KEY,
                    endpoint TEXT NOT NULL,
                    model TEXT NOT NULL,
                    is_stream INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status_code INTEGER,
                    status TEXT,
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    total_tokens INTEGER,
                    error TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_forward_started_at ON forward_request_metrics(started_at);
                CREATE INDEX IF NOT EXISTS idx_forward_endpoint ON forward_request_metrics(endpoint);
                CREATE INDEX IF NOT EXISTS idx_forward_model ON forward_request_metrics(model);
                "#,
            )?;
            Ok(())
        })
        .await
    }

    async fn upsert_session_start(
        &self,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        let session_id = session_id.to_string();
        let model = model.to_string();
        let started_at = format_timestamp(started_at);

        self.with_connection(move |connection| {
            connection.execute(
                r#"
                INSERT INTO session_metrics (
                    session_id, model, started_at, status, updated_at
                ) VALUES (?1, ?2, ?3, 'running', ?3)
                ON CONFLICT(session_id) DO UPDATE SET
                    model = excluded.model,
                    started_at = CASE
                        WHEN session_metrics.started_at <= excluded.started_at THEN session_metrics.started_at
                        ELSE excluded.started_at
                    END,
                    completed_at = NULL,
                    status = 'running',
                    updated_at = excluded.updated_at
                "#,
                params![session_id, model, started_at],
            )?;
            Ok(())
        })
        .await
    }

    async fn update_session_message_count(
        &self,
        session_id: &str,
        message_count: u32,
        updated_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        let session_id = session_id.to_string();
        let updated_at = format_timestamp(updated_at);

        self.with_connection(move |connection| {
            connection.execute(
                "UPDATE session_metrics SET message_count = ?1, updated_at = ?2 WHERE session_id = ?3",
                params![i64::from(message_count), updated_at, session_id],
            )?;
            Ok(())
        })
        .await
    }

    async fn complete_session(
        &self,
        session_id: &str,
        status: SessionStatus,
        completed_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        let session_id = session_id.to_string();
        let completed_at_str = format_timestamp(completed_at);

        self.with_connection(move |connection| {
            refresh_session_aggregates(connection, &session_id, completed_at)?;
            connection.execute(
                "UPDATE session_metrics SET status = ?1, completed_at = ?2, updated_at = ?2 WHERE session_id = ?3",
                params![status.as_str(), completed_at_str, session_id],
            )?;
            Ok(())
        })
        .await
    }

    async fn insert_round_start(
        &self,
        round_id: &str,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        let round_id = round_id.to_string();
        let session_id = session_id.to_string();
        let model = model.to_string();
        let started_at_str = format_timestamp(started_at);

        self.with_connection(move |connection| {
            connection.execute(
                r#"
                INSERT INTO round_metrics (
                    round_id, session_id, model, started_at, status
                ) VALUES (?1, ?2, ?3, ?4, 'running')
                ON CONFLICT(round_id) DO NOTHING
                "#,
                params![round_id, session_id, model, started_at_str],
            )?;
            refresh_session_aggregates(connection, &session_id, started_at)?;
            Ok(())
        })
        .await
    }

    async fn complete_round(
        &self,
        round_id: &str,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        error: Option<String>,
    ) -> MetricsResult<()> {
        let round_id = round_id.to_string();
        let completed_at_str = format_timestamp(completed_at);

        self.with_connection(move |connection| {
            let session_id: String = connection.query_row(
                "SELECT session_id FROM round_metrics WHERE round_id = ?1",
                params![round_id],
                |row| row.get(0),
            )?;

            connection.execute(
                r#"
                UPDATE round_metrics
                SET completed_at = ?1,
                    status = ?2,
                    prompt_tokens = ?3,
                    completion_tokens = ?4,
                    total_tokens = ?5,
                    error = ?6
                WHERE round_id = ?7
                "#,
                params![
                    completed_at_str,
                    status.as_str(),
                    usage.prompt_tokens as i64,
                    usage.completion_tokens as i64,
                    usage.total_tokens as i64,
                    error,
                    round_id,
                ],
            )?;

            refresh_session_aggregates(connection, &session_id, completed_at)?;
            Ok(())
        })
        .await
    }

    async fn insert_tool_start(
        &self,
        tool_call_id: &str,
        round_id: &str,
        session_id: &str,
        tool_name: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        let tool_call_id = tool_call_id.to_string();
        let round_id = round_id.to_string();
        let session_id = session_id.to_string();
        let tool_name = tool_name.to_string();
        let started_at_str = format_timestamp(started_at);

        self.with_connection(move |connection| {
            connection.execute(
                r#"
                INSERT INTO tool_call_metrics (
                    tool_call_id, round_id, session_id, tool_name, started_at
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(tool_call_id) DO UPDATE SET
                    round_id = excluded.round_id,
                    session_id = excluded.session_id,
                    tool_name = excluded.tool_name,
                    started_at = excluded.started_at
                "#,
                params![
                    tool_call_id,
                    round_id,
                    session_id,
                    tool_name,
                    started_at_str
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn complete_tool_call(
        &self,
        tool_call_id: &str,
        completion: ToolCallCompletion,
    ) -> MetricsResult<()> {
        let tool_call_id = tool_call_id.to_string();
        let completed_at = format_timestamp(completion.completed_at);
        let success = if completion.success { 1_i64 } else { 0_i64 };
        let error = completion.error;

        self.with_connection(move |connection| {
            let session_id: String = connection.query_row(
                "SELECT session_id FROM tool_call_metrics WHERE tool_call_id = ?1",
                params![tool_call_id],
                |row| row.get(0),
            )?;

            connection.execute(
                "UPDATE tool_call_metrics SET completed_at = ?1, success = ?2, error = ?3 WHERE tool_call_id = ?4",
                params![completed_at, success, error, tool_call_id],
            )?;

            refresh_session_aggregates(connection, &session_id, completion.completed_at)?;
            Ok(())
        })
        .await
    }

    async fn insert_forward_start(
        &self,
        forward_id: &str,
        endpoint: &str,
        model: &str,
        is_stream: bool,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        let forward_id = forward_id.to_string();
        let endpoint = endpoint.to_string();
        let model = model.to_string();
        let is_stream_int = if is_stream { 1_i64 } else { 0_i64 };
        let started_at_str = format_timestamp(started_at);

        self.with_connection(move |connection| {
            connection.execute(
                r#"
                INSERT INTO forward_request_metrics (
                    forward_id, endpoint, model, is_stream, started_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                ON CONFLICT(forward_id) DO UPDATE SET
                    endpoint = excluded.endpoint,
                    model = excluded.model,
                    is_stream = excluded.is_stream,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at
                "#,
                params![forward_id, endpoint, model, is_stream_int, started_at_str],
            )?;
            Ok(())
        })
        .await
    }

    async fn complete_forward(
        &self,
        forward_id: &str,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: ForwardStatus,
        usage: Option<TokenUsage>,
        error: Option<String>,
    ) -> MetricsResult<()> {
        let forward_id = forward_id.to_string();
        let completed_at_str = format_timestamp(completed_at);
        let status_code_int = status_code.map(|s| s as i64);
        let (prompt, completion, total) = match usage {
            Some(u) => (Some(u.prompt_tokens as i64), Some(u.completion_tokens as i64), Some(u.total_tokens as i64)),
            None => (None, None, None),
        };

        self.with_connection(move |connection| {
            connection.execute(
                r#"
                UPDATE forward_request_metrics
                SET completed_at = ?1,
                    status_code = ?2,
                    status = ?3,
                    prompt_tokens = ?4,
                    completion_tokens = ?5,
                    total_tokens = ?6,
                    error = ?7,
                    updated_at = ?1
                WHERE forward_id = ?8
                "#,
                params![
                    completed_at_str,
                    status_code_int,
                    status.as_str(),
                    prompt,
                    completion,
                    total,
                    error,
                    forward_id,
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn forward_summary(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<ForwardMetricsSummary> {
        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_forward_where_clause(
                filter.start_date,
                filter.end_date,
                filter.endpoint.as_deref(),
                filter.model.as_deref(),
                &mut params_vec,
            );

            let sql = format!(
                "SELECT COUNT(*), \
                 COALESCE(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), 0), \
                 COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 0), \
                 COALESCE(SUM(prompt_tokens), 0), \
                 COALESCE(SUM(completion_tokens), 0), \
                 COALESCE(SUM(total_tokens), 0), \
                 AVG(CASE WHEN completed_at IS NOT NULL THEN \
                     (julianday(completed_at) - julianday(started_at)) * 86400000 END) \
                 FROM forward_request_metrics {}",
                where_clause
            );

            let mut stmt = connection.prepare(&sql)?;
            let summary = stmt.query_row(params_from_iter(params_vec.iter()), |row| {
                let avg_duration: Option<f64> = row.get(6)?;
                Ok(ForwardMetricsSummary {
                    total_requests: row.get::<_, i64>(0)? as u64,
                    successful_requests: row.get::<_, i64>(1)? as u64,
                    failed_requests: row.get::<_, i64>(2)? as u64,
                    total_tokens: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(3)? as u64,
                        completion_tokens: row.get::<_, i64>(4)? as u64,
                        total_tokens: row.get::<_, i64>(5)? as u64,
                    },
                    avg_duration_ms: avg_duration.map(|d| d as u64),
                })
            })?;

            Ok(summary)
        })
        .await
    }

    async fn forward_by_endpoint(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardEndpointMetrics>> {
        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_forward_where_clause(
                filter.start_date,
                filter.end_date,
                None,
                filter.model.as_deref(),
                &mut params_vec,
            );

            let sql = format!(
                "SELECT endpoint, COUNT(*), \
                 COALESCE(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), 0), \
                 COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 0), \
                 COALESCE(SUM(prompt_tokens), 0), \
                 COALESCE(SUM(completion_tokens), 0), \
                 COALESCE(SUM(total_tokens), 0), \
                 AVG(CASE WHEN completed_at IS NOT NULL THEN \
                     (julianday(completed_at) - julianday(started_at)) * 86400000 END) \
                 FROM forward_request_metrics {} \
                 GROUP BY endpoint ORDER BY COUNT(*) DESC",
                where_clause
            );

            let mut stmt = connection.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
            let mut endpoints = Vec::new();

            while let Some(row) = rows.next()? {
                let avg_duration: Option<f64> = row.get(7)?;
                endpoints.push(ForwardEndpointMetrics {
                    endpoint: row.get(0)?,
                    requests: row.get::<_, i64>(1)? as u64,
                    successful: row.get::<_, i64>(2)? as u64,
                    failed: row.get::<_, i64>(3)? as u64,
                    tokens: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(4)? as u64,
                        completion_tokens: row.get::<_, i64>(5)? as u64,
                        total_tokens: row.get::<_, i64>(6)? as u64,
                    },
                    avg_duration_ms: avg_duration.map(|d| d as u64),
                });
            }

            Ok(endpoints)
        })
        .await
    }

    async fn forward_requests(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardRequestMetrics>> {
        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_forward_where_clause(
                filter.start_date,
                filter.end_date,
                filter.endpoint.as_deref(),
                filter.model.as_deref(),
                &mut params_vec,
            );

            let limit = i64::from(filter.limit.unwrap_or(100).min(1_000));
            let sql = format!(
                "SELECT forward_id, endpoint, model, is_stream, started_at, completed_at, \
                 status_code, status, prompt_tokens, completion_tokens, total_tokens, error \
                 FROM forward_request_metrics {} \
                 ORDER BY started_at DESC LIMIT {}",
                where_clause, limit
            );

            let mut stmt = connection.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
            let mut requests = Vec::new();

            while let Some(row) = rows.next()? {
                let started_at = parse_timestamp(row.get::<_, String>(4)?)?;
                let completed_at = parse_optional_timestamp(row.get::<_, Option<String>>(5)?)?;
                let status_raw: Option<String> = row.get(7)?;
                let status = status_raw.and_then(|s| ForwardStatus::from_db(&s));

                let prompt: Option<i64> = row.get(8)?;
                let completion: Option<i64> = row.get(9)?;
                let total: Option<i64> = row.get(10)?;
                let token_usage = match (prompt, completion, total) {
                    (Some(p), Some(c), Some(t)) => Some(TokenUsage {
                        prompt_tokens: p as u64,
                        completion_tokens: c as u64,
                        total_tokens: t as u64,
                    }),
                    _ => None,
                };

                requests.push(ForwardRequestMetrics {
                    forward_id: row.get(0)?,
                    endpoint: row.get(1)?,
                    model: row.get(2)?,
                    is_stream: row.get::<_, i64>(3)? > 0,
                    started_at,
                    completed_at,
                    status_code: row.get::<_, Option<i64>>(6)?.map(|s| s as u16),
                    status,
                    token_usage,
                    error: row.get(11)?,
                    duration_ms: compute_duration_ms(started_at, completed_at),
                });
            }

            Ok(requests)
        })
        .await
    }

    async fn forward_daily_metrics(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        let end_date = end_date.unwrap_or_else(|| Utc::now().date_naive());
        let span = days.max(1) - 1;
        let start_date = end_date - chrono::Duration::days(i64::from(span));

        self.with_connection(move |connection| {
            let mut stmt = connection.prepare(
                r#"
                SELECT
                    date(started_at) AS date_key,
                    COUNT(*) AS total_sessions,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens
                FROM forward_request_metrics
                WHERE date(started_at) BETWEEN date(?1) AND date(?2)
                GROUP BY date_key
                ORDER BY date_key ASC
                "#,
            )?;

            let mut rows = stmt.query(params![start_date.to_string(), end_date.to_string()])?;
            let mut result = Vec::new();

            while let Some(row) = rows.next()? {
                let date = NaiveDate::parse_from_str(&row.get::<_, String>(0)?, "%Y-%m-%d")?;

                result.push(DailyMetrics {
                    date,
                    total_sessions: row.get::<_, i64>(1)? as u32,
                    total_rounds: 0,
                    total_token_usage: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(2)? as u64,
                        completion_tokens: row.get::<_, i64>(3)? as u64,
                        total_tokens: row.get::<_, i64>(4)? as u64,
                    },
                    total_tool_calls: 0,
                    model_breakdown: HashMap::new(),
                    tool_breakdown: HashMap::new(),
                });
            }

            Ok(result)
        })
        .await
    }

    async fn summary(&self, filter: MetricsDateFilter) -> MetricsResult<MetricsSummary> {
        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_session_where_clause(
                filter.start_date,
                filter.end_date,
                None,
                &mut params_vec,
            );

            let summary_sql = format!(
                "SELECT COUNT(*), COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), COALESCE(SUM(total_tokens), 0), COALESCE(SUM(tool_call_count), 0) FROM session_metrics {}",
                where_clause
            );

            let mut stmt = connection.prepare(&summary_sql)?;
            let summary = stmt.query_row(params_from_iter(params_vec.iter()), |row| {
                Ok(MetricsSummary {
                    total_sessions: row.get::<_, i64>(0)? as u64,
                    total_tokens: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(1)? as u64,
                        completion_tokens: row.get::<_, i64>(2)? as u64,
                        total_tokens: row.get::<_, i64>(3)? as u64,
                    },
                    total_tool_calls: row.get::<_, i64>(4)? as u64,
                    active_sessions: 0,
                })
            })?;

            let mut active_params = Vec::new();
            let active_clause = build_session_where_clause(
                filter.start_date,
                filter.end_date,
                Some("running"),
                &mut active_params,
            );
            let active_sql = format!(
                "SELECT COUNT(*) FROM session_metrics {}",
                active_clause
            );
            let mut active_stmt = connection.prepare(&active_sql)?;
            let active_sessions = active_stmt.query_row(params_from_iter(active_params.iter()), |row| {
                row.get::<_, i64>(0)
            })? as u64;

            Ok(MetricsSummary {
                active_sessions,
                ..summary
            })
        })
        .await
    }

    async fn by_model(&self, filter: MetricsDateFilter) -> MetricsResult<Vec<ModelMetrics>> {
        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_session_where_clause(
                filter.start_date,
                filter.end_date,
                None,
                &mut params_vec,
            );

            let sql = format!(
                "SELECT model, COUNT(*), COALESCE(SUM(total_rounds), 0), COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), COALESCE(SUM(total_tokens), 0), COALESCE(SUM(tool_call_count), 0) FROM session_metrics {} GROUP BY model ORDER BY SUM(total_tokens) DESC",
                where_clause
            );

            let mut stmt = connection.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
            let mut models = Vec::new();

            while let Some(row) = rows.next()? {
                models.push(ModelMetrics {
                    model: row.get(0)?,
                    sessions: row.get::<_, i64>(1)? as u64,
                    rounds: row.get::<_, i64>(2)? as u64,
                    tokens: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(3)? as u64,
                        completion_tokens: row.get::<_, i64>(4)? as u64,
                        total_tokens: row.get::<_, i64>(5)? as u64,
                    },
                    tool_calls: row.get::<_, i64>(6)? as u64,
                });
            }

            Ok(models)
        })
        .await
    }

    async fn sessions(&self, filter: SessionMetricsFilter) -> MetricsResult<Vec<SessionMetrics>> {
        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_session_where_clause(
                filter.start_date,
                filter.end_date,
                None,
                &mut params_vec,
            );
            let mut conditions = if where_clause.is_empty() {
                Vec::new()
            } else {
                vec![where_clause.replacen("WHERE ", "", 1)]
            };
            if let Some(model) = filter.model {
                conditions.push("model = ?".to_string());
                params_vec.push(model);
            }

            let where_sql = if conditions.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", conditions.join(" AND "))
            };

            let limit = i64::from(filter.limit.unwrap_or(100).min(1_000));
            let sql = format!(
                "SELECT session_id, model, started_at, completed_at, total_rounds, prompt_tokens, completion_tokens, total_tokens, tool_call_count, status, message_count FROM session_metrics {} ORDER BY started_at DESC LIMIT {}",
                where_sql, limit
            );

            let mut stmt = connection.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
            let mut sessions = Vec::new();

            while let Some(row) = rows.next()? {
                let session_id: String = row.get(0)?;
                let started_at = parse_timestamp(row.get::<_, String>(2)?)?;
                let completed_at = parse_optional_timestamp(row.get::<_, Option<String>>(3)?)?;
                let status_raw: String = row.get(9)?;
                let status = SessionStatus::from_db(&status_raw).ok_or_else(|| {
                    MetricsError::InvalidData(format!("unknown session status: {}", status_raw))
                })?;
                let tool_breakdown = load_tool_breakdown(connection, &session_id)?;

                sessions.push(SessionMetrics {
                    session_id,
                    model: row.get(1)?,
                    started_at,
                    completed_at,
                    total_rounds: row.get::<_, i64>(4)? as u32,
                    total_token_usage: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(5)? as u64,
                        completion_tokens: row.get::<_, i64>(6)? as u64,
                        total_tokens: row.get::<_, i64>(7)? as u64,
                    },
                    tool_call_count: row.get::<_, i64>(8)? as u32,
                    tool_breakdown,
                    status,
                    message_count: row.get::<_, i64>(10)? as u32,
                    duration_ms: compute_duration_ms(started_at, completed_at),
                });
            }

            Ok(sessions)
        })
        .await
    }

    async fn session_detail(&self, session_id: &str) -> MetricsResult<Option<SessionDetail>> {
        let session_id = session_id.to_string();
        self.with_connection(move |connection| {
            let session_sql = "SELECT session_id, model, started_at, completed_at, total_rounds, prompt_tokens, completion_tokens, total_tokens, tool_call_count, status, message_count FROM session_metrics WHERE session_id = ?1";
            let session_row = connection
                .query_row(session_sql, params![session_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, i64>(10)?,
                    ))
                })
                .optional()?;

            let Some((
                session_id,
                model,
                started_at_raw,
                completed_at_raw,
                total_rounds,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                tool_call_count,
                status_raw,
                message_count,
            )) = session_row
            else {
                return Ok(None);
            };

            let started_at = parse_timestamp(started_at_raw)?;
            let completed_at = parse_optional_timestamp(completed_at_raw)?;
            let status = SessionStatus::from_db(&status_raw).ok_or_else(|| {
                MetricsError::InvalidData(format!("unknown session status: {}", status_raw))
            })?;
            let tool_breakdown = load_tool_breakdown(connection, &session_id)?;

            let session = SessionMetrics {
                session_id: session_id.clone(),
                model,
                started_at,
                completed_at,
                total_rounds: total_rounds as u32,
                total_token_usage: TokenUsage {
                    prompt_tokens: prompt_tokens as u64,
                    completion_tokens: completion_tokens as u64,
                    total_tokens: total_tokens as u64,
                },
                tool_call_count: tool_call_count as u32,
                tool_breakdown,
                status,
                message_count: message_count as u32,
                duration_ms: compute_duration_ms(started_at, completed_at),
            };

            let rounds = load_rounds(connection, &session_id)?;
            Ok(Some(SessionDetail { session, rounds }))
        })
        .await
    }

    async fn daily_metrics(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        let end_date = end_date.unwrap_or_else(|| Utc::now().date_naive());
        let span = days.max(1) - 1;
        let start_date = end_date - chrono::Duration::days(i64::from(span));

        self.with_connection(move |connection| {
            let mut stmt = connection.prepare(
                r#"
                SELECT
                    date(started_at) AS date_key,
                    COUNT(*) AS total_sessions,
                    COALESCE(SUM(total_rounds), 0) AS total_rounds,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(tool_call_count), 0) AS total_tool_calls
                FROM session_metrics
                WHERE date(started_at) BETWEEN date(?1) AND date(?2)
                GROUP BY date_key
                ORDER BY date_key ASC
                "#,
            )?;

            let mut rows = stmt.query(params![start_date.to_string(), end_date.to_string()])?;
            let mut result = Vec::new();

            while let Some(row) = rows.next()? {
                let date = NaiveDate::parse_from_str(&row.get::<_, String>(0)?, "%Y-%m-%d")?;
                let model_breakdown = load_daily_model_breakdown(connection, date)?;
                let tool_breakdown = load_daily_tool_breakdown(connection, date)?;

                result.push(DailyMetrics {
                    date,
                    total_sessions: row.get::<_, i64>(1)? as u32,
                    total_rounds: row.get::<_, i64>(2)? as u32,
                    total_token_usage: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(3)? as u64,
                        completion_tokens: row.get::<_, i64>(4)? as u64,
                        total_tokens: row.get::<_, i64>(5)? as u64,
                    },
                    total_tool_calls: row.get::<_, i64>(6)? as u32,
                    model_breakdown,
                    tool_breakdown,
                });
            }

            Ok(result)
        })
        .await
    }

    async fn prune_rounds_before(&self, cutoff: DateTime<Utc>) -> MetricsResult<u64> {
        self.with_connection(move |connection| {
            let cutoff_str = format_timestamp(cutoff);
            let deleted = connection.execute(
                "DELETE FROM round_metrics WHERE started_at < ?1",
                params![cutoff_str],
            )?;

            let mut stmt = connection.prepare("SELECT session_id FROM session_metrics")?;
            let session_ids: Vec<String> = stmt
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<String>, _>>()?;
            for session_id in session_ids {
                refresh_session_aggregates(connection, &session_id, Utc::now())?;
            }

            Ok(deleted as u64)
        })
        .await
    }
}

fn open_connection(path: &Path) -> MetricsResult<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path)?;
    connection.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        PRAGMA synchronous = NORMAL;
        "#,
    )?;
    Ok(connection)
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339()
}

fn parse_timestamp(raw: String) -> MetricsResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&raw)?.with_timezone(&Utc))
}

fn parse_optional_timestamp(raw: Option<String>) -> MetricsResult<Option<DateTime<Utc>>> {
    raw.map(parse_timestamp).transpose()
}

fn compute_duration_ms(
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
) -> Option<u64> {
    completed_at.and_then(|end| {
        end.signed_duration_since(started_at)
            .num_milliseconds()
            .try_into()
            .ok()
    })
}

fn build_session_where_clause(
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    required_status: Option<&str>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_date {
        conditions.push("date(started_at) >= date(?)".to_string());
        params_vec.push(start.to_string());
    }

    if let Some(end) = end_date {
        conditions.push("date(started_at) <= date(?)".to_string());
        params_vec.push(end.to_string());
    }

    if let Some(status) = required_status {
        conditions.push("status = ?".to_string());
        params_vec.push(status.to_string());
    }

    if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    }
}

fn build_forward_where_clause(
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    endpoint: Option<&str>,
    model: Option<&str>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_date {
        conditions.push("date(started_at) >= date(?)".to_string());
        params_vec.push(start.to_string());
    }

    if let Some(end) = end_date {
        conditions.push("date(started_at) <= date(?)".to_string());
        params_vec.push(end.to_string());
    }

    if let Some(ep) = endpoint {
        conditions.push("endpoint = ?".to_string());
        params_vec.push(ep.to_string());
    }

    if let Some(m) = model {
        conditions.push("model = ?".to_string());
        params_vec.push(m.to_string());
    }

    if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    }
}

fn refresh_session_aggregates(
    connection: &Connection,
    session_id: &str,
    updated_at: DateTime<Utc>,
) -> MetricsResult<()> {
    let updated_at = format_timestamp(updated_at);
    connection.execute(
        r#"
        UPDATE session_metrics
        SET
            total_rounds = COALESCE((SELECT COUNT(*) FROM round_metrics WHERE session_id = ?1), 0),
            prompt_tokens = COALESCE((SELECT SUM(prompt_tokens) FROM round_metrics WHERE session_id = ?1), 0),
            completion_tokens = COALESCE((SELECT SUM(completion_tokens) FROM round_metrics WHERE session_id = ?1), 0),
            total_tokens = COALESCE((SELECT SUM(total_tokens) FROM round_metrics WHERE session_id = ?1), 0),
            tool_call_count = COALESCE((SELECT COUNT(*) FROM tool_call_metrics WHERE session_id = ?1), 0),
            updated_at = ?2
        WHERE session_id = ?1
        "#,
        params![session_id, updated_at],
    )?;
    Ok(())
}

fn load_tool_breakdown(
    connection: &Connection,
    session_id: &str,
) -> MetricsResult<HashMap<String, u32>> {
    let mut stmt = connection.prepare(
        "SELECT tool_name, COUNT(*) FROM tool_call_metrics WHERE session_id = ?1 GROUP BY tool_name",
    )?;
    let mut rows = stmt.query(params![session_id])?;
    let mut breakdown = HashMap::new();

    while let Some(row) = rows.next()? {
        let tool: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        breakdown.insert(tool, count as u32);
    }

    Ok(breakdown)
}

fn load_rounds(connection: &Connection, session_id: &str) -> MetricsResult<Vec<RoundMetrics>> {
    let mut stmt = connection.prepare(
        "SELECT round_id, session_id, model, started_at, completed_at, status, prompt_tokens, completion_tokens, total_tokens, error FROM round_metrics WHERE session_id = ?1 ORDER BY started_at ASC",
    )?;
    let mut rows = stmt.query(params![session_id])?;
    let mut rounds = Vec::new();

    while let Some(row) = rows.next()? {
        let round_id: String = row.get(0)?;
        let started_at = parse_timestamp(row.get::<_, String>(3)?)?;
        let completed_at = parse_optional_timestamp(row.get::<_, Option<String>>(4)?)?;
        let status_raw: String = row.get(5)?;
        let status = RoundStatus::from_db(&status_raw).ok_or_else(|| {
            MetricsError::InvalidData(format!("unknown round status: {}", status_raw))
        })?;

        rounds.push(RoundMetrics {
            round_id: round_id.clone(),
            session_id: row.get(1)?,
            model: row.get(2)?,
            started_at,
            completed_at,
            token_usage: TokenUsage {
                prompt_tokens: row.get::<_, i64>(6)? as u64,
                completion_tokens: row.get::<_, i64>(7)? as u64,
                total_tokens: row.get::<_, i64>(8)? as u64,
            },
            tool_calls: load_tool_calls(connection, &round_id)?,
            status,
            error: row.get(9)?,
            duration_ms: compute_duration_ms(started_at, completed_at),
        });
    }

    Ok(rounds)
}

fn load_tool_calls(connection: &Connection, round_id: &str) -> MetricsResult<Vec<ToolCallMetrics>> {
    let mut stmt = connection.prepare(
        "SELECT tool_call_id, tool_name, started_at, completed_at, success, error FROM tool_call_metrics WHERE round_id = ?1 ORDER BY started_at ASC",
    )?;
    let mut rows = stmt.query(params![round_id])?;
    let mut tools = Vec::new();

    while let Some(row) = rows.next()? {
        let started_at = parse_timestamp(row.get::<_, String>(2)?)?;
        let completed_at = parse_optional_timestamp(row.get::<_, Option<String>>(3)?)?;
        let success = row.get::<_, Option<i64>>(4)?.map(|value| value > 0);

        tools.push(ToolCallMetrics {
            tool_call_id: row.get(0)?,
            tool_name: row.get(1)?,
            started_at,
            completed_at,
            success,
            error: row.get(5)?,
            duration_ms: compute_duration_ms(started_at, completed_at),
        });
    }

    Ok(tools)
}

fn load_daily_model_breakdown(
    connection: &Connection,
    date: NaiveDate,
) -> MetricsResult<HashMap<String, TokenUsage>> {
    let mut stmt = connection.prepare(
        r#"
        SELECT model,
               COALESCE(SUM(prompt_tokens), 0),
               COALESCE(SUM(completion_tokens), 0),
               COALESCE(SUM(total_tokens), 0)
        FROM session_metrics
        WHERE date(started_at) = date(?1)
        GROUP BY model
        "#,
    )?;

    let mut rows = stmt.query(params![date.to_string()])?;
    let mut breakdown = HashMap::new();

    while let Some(row) = rows.next()? {
        breakdown.insert(
            row.get::<_, String>(0)?,
            TokenUsage {
                prompt_tokens: row.get::<_, i64>(1)? as u64,
                completion_tokens: row.get::<_, i64>(2)? as u64,
                total_tokens: row.get::<_, i64>(3)? as u64,
            },
        );
    }

    Ok(breakdown)
}

fn load_daily_tool_breakdown(
    connection: &Connection,
    date: NaiveDate,
) -> MetricsResult<HashMap<String, u32>> {
    let mut stmt = connection.prepare(
        r#"
        SELECT tool_name, COUNT(*)
        FROM tool_call_metrics
        WHERE date(started_at) = date(?1)
        GROUP BY tool_name
        "#,
    )?;

    let mut rows = stmt.query(params![date.to_string()])?;
    let mut breakdown = HashMap::new();

    while let Some(row) = rows.next()? {
        breakdown.insert(row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u32);
    }

    Ok(breakdown)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{NaiveDate, TimeZone, Utc};
    use tempfile::tempdir;

    use super::{MetricsStorage, SqliteMetricsStorage, ToolCallCompletion};
    use crate::types::{
        MetricsDateFilter, RoundStatus, SessionMetricsFilter, SessionStatus, TokenUsage,
    };

    #[tokio::test]
    async fn storage_records_session_and_round_data_for_summary_queries() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));

        storage.init().await.expect("init storage");

        let started_at = Utc
            .with_ymd_and_hms(2026, 2, 10, 10, 0, 0)
            .single()
            .expect("valid datetime");
        storage
            .upsert_session_start("session-a", "gpt-4", started_at)
            .await
            .expect("session started");
        storage
            .update_session_message_count("session-a", 7, started_at)
            .await
            .expect("message count update");

        storage
            .insert_round_start("round-a", "session-a", "gpt-4", started_at)
            .await
            .expect("round start");
        storage
            .insert_tool_start("tool-1", "round-a", "session-a", "read_file", started_at)
            .await
            .expect("tool start");
        storage
            .complete_tool_call(
                "tool-1",
                ToolCallCompletion {
                    completed_at: started_at,
                    success: true,
                    error: None,
                },
            )
            .await
            .expect("tool completion");
        storage
            .complete_round(
                "round-a",
                started_at,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 10,
                    completion_tokens: 15,
                    total_tokens: 25,
                },
                None,
            )
            .await
            .expect("round completion");
        storage
            .complete_session("session-a", SessionStatus::Completed, started_at)
            .await
            .expect("session completion");

        let summary = storage
            .summary(MetricsDateFilter::default())
            .await
            .expect("summary query");

        assert_eq!(summary.total_sessions, 1);
        assert_eq!(summary.total_tokens.total_tokens, 25);
        assert_eq!(summary.total_tool_calls, 1);
    }

    #[tokio::test]
    async fn storage_filters_sessions_and_returns_tool_breakdown() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));

        storage.init().await.expect("init storage");

        let day_a = Utc
            .with_ymd_and_hms(2026, 2, 1, 9, 0, 0)
            .single()
            .expect("valid datetime");
        let day_b = Utc
            .with_ymd_and_hms(2026, 2, 5, 9, 0, 0)
            .single()
            .expect("valid datetime");

        storage
            .upsert_session_start("s1", "gpt-4", day_a)
            .await
            .expect("session start");
        storage
            .insert_round_start("r1", "s1", "gpt-4", day_a)
            .await
            .expect("round start");
        storage
            .insert_tool_start("t1", "r1", "s1", "read_file", day_a)
            .await
            .expect("tool start");
        storage
            .complete_tool_call(
                "t1",
                ToolCallCompletion {
                    completed_at: day_a,
                    success: true,
                    error: None,
                },
            )
            .await
            .expect("tool complete");
        storage
            .complete_round(
                "r1",
                day_a,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
                None,
            )
            .await
            .expect("round complete");

        storage
            .upsert_session_start("s2", "claude-3", day_b)
            .await
            .expect("session start");

        let sessions = storage
            .sessions(SessionMetricsFilter {
                start_date: Some(NaiveDate::from_ymd_opt(2026, 2, 1).expect("valid date")),
                end_date: Some(NaiveDate::from_ymd_opt(2026, 2, 3).expect("valid date")),
                model: Some("gpt-4".to_string()),
                limit: Some(100),
            })
            .await
            .expect("sessions query");

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "s1");
        assert_eq!(sessions[0].tool_breakdown.get("read_file"), Some(&1));
    }

    #[tokio::test]
    async fn storage_produces_daily_rollups_with_model_and_tool_breakdowns() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));

        storage.init().await.expect("init storage");

        let now = Utc
            .with_ymd_and_hms(2026, 2, 10, 12, 0, 0)
            .single()
            .expect("valid datetime");
        storage
            .upsert_session_start("daily-1", "gpt-4", now)
            .await
            .expect("session start");
        storage
            .insert_round_start("daily-r1", "daily-1", "gpt-4", now)
            .await
            .expect("round start");
        storage
            .insert_tool_start("daily-t1", "daily-r1", "daily-1", "write_file", now)
            .await
            .expect("tool start");
        storage
            .complete_tool_call(
                "daily-t1",
                ToolCallCompletion {
                    completed_at: now,
                    success: true,
                    error: None,
                },
            )
            .await
            .expect("tool complete");
        storage
            .complete_round(
                "daily-r1",
                now,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 3,
                    completion_tokens: 7,
                    total_tokens: 10,
                },
                None,
            )
            .await
            .expect("round completion");

        let daily = storage
            .daily_metrics(
                7,
                Some(NaiveDate::from_ymd_opt(2026, 2, 10).expect("valid date")),
            )
            .await
            .expect("daily metrics");

        assert_eq!(daily.len(), 1);
        let row = &daily[0];
        assert_eq!(row.total_sessions, 1);
        assert_eq!(row.total_rounds, 1);
        assert_eq!(row.total_tool_calls, 1);
        assert_eq!(
            row.model_breakdown
                .get("gpt-4")
                .map(|usage| usage.total_tokens),
            Some(10)
        );
        assert_eq!(
            row.tool_breakdown,
            HashMap::from([(String::from("write_file"), 1)])
        );
    }
}
