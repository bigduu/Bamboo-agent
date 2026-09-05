//! Metrics Storage System
//!
//! This module provides persistent storage for agent metrics using SQLite as the backend.
//! It implements a comprehensive metrics collection and query system for monitoring
//! agent performance, resource usage, and behavior patterns.
//!
//! # Architecture
//!
//! The storage system is built around the [`MetricsStorage`] trait, which defines
//! the interface for storing and retrieving metrics data. The primary implementation
//! is [`SqliteMetricsStorage`], which uses SQLite with WAL mode for reliable,
//! concurrent access.
//!
//! # Data Model
//!
//! Metrics are organized into three main categories:
//!
//! ## Session Metrics
//! Track complete conversation sessions from start to finish, including:
//! - Total rounds and token usage
//! - Tool call counts and breakdown
//! - Session duration and status
//!
//! ## Round Metrics
//! Track individual request-response cycles within sessions:
//! - Per-round token consumption
//! - Round status and errors
//! - Associated tool calls
//!
//! ## Forward Metrics
//! Track HTTP proxy operations to upstream APIs:
//! - Request/response tracking
//! - Endpoint-specific metrics
//! - Token usage per provider
//!
//! # Storage Schema
//!
//! The SQLite database contains the following tables:
//! - `session_metrics`: Aggregated session-level metrics
//! - `round_metrics`: Individual round metrics linked to sessions
//! - `tool_call_metrics`: Tool invocation details linked to rounds
//! - `forward_request_metrics`: HTTP proxy request tracking
//!
//! # Usage
//!
//! ```rust,ignore
//! use bamboo_agent::agent::metrics::storage::{SqliteMetricsStorage, MetricsStorage};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize storage
//!     let storage = SqliteMetricsStorage::new("metrics.db");
//!     storage.init().await?;
//!
//!     // Record session start
//!     storage.upsert_session_start(
//!         "session-123",
//!         "gpt-4",
//!         chrono::Utc::now()
//!     ).await?;
//!
//!     // Query metrics
//!     let summary = storage.summary(Default::default()).await?;
//!     println!("Total sessions: {}", summary.total_sessions);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Performance
//!
//! The storage system is optimized for:
//! - **Concurrent writes**: Uses WAL mode and spawn_blocking for async compatibility
//! - **Efficient queries**: Indexed by timestamps, models, and endpoints
//! - **Aggregate caching**: Session metrics are pre-aggregated for fast queries
//!
//! # Thread Safety
//!
//! All storage operations are thread-safe and can be called from multiple
//! async tasks concurrently. SQLite connections are opened per-operation
//! to avoid blocking the async runtime.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use thiserror::Error;

use crate::metrics::types::{
    DailyMetrics, ForwardEndpointMetrics, ForwardMetricsFilter, ForwardMetricsSummary,
    ForwardRequestMetrics, ForwardStatus, ForwardTokenDetails, MetricsDateFilter, MetricsSummary,
    ModelMetrics, ModelMetricsDateFilter, RoundMetrics, RoundStatus, SessionDetail, SessionMetrics,
    SessionMetricsFilter, SessionStatus, TokenUsage, ToolCallMetrics,
};

/// Result type for metrics storage operations.
///
/// This is a specialized Result type that uses [`MetricsError`] as the error type,
/// providing a consistent return type across all storage operations.
pub type MetricsResult<T> = Result<T, MetricsError>;

/// Errors that can occur during metrics storage operations.
///
/// This enum covers all the error cases that can arise when working with
/// the metrics storage system, from database errors to data validation issues.
#[derive(Debug, Error)]
pub enum MetricsError {
    /// SQLite database operation failed.
    ///
    /// This can occur due to SQL syntax errors, constraint violations,
    /// database corruption, or connection issues.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Timestamp parsing failed.
    ///
    /// This occurs when reading timestamps from the database that don't
    /// conform to the expected RFC3339 format.
    #[error("time parse error: {0}")]
    Chrono(#[from] chrono::ParseError),

    /// I/O operation failed.
    ///
    /// This can occur when creating the database file, directory, or
    /// during other file system operations.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Async task failed to complete.
    ///
    /// This occurs when a spawned blocking task panics or is cancelled,
    /// typically indicating a serious system issue.
    #[error("storage task join error: {0}")]
    Task(String),

    /// Data validation failed.
    ///
    /// This occurs when retrieved data doesn't match expected constraints,
    /// such as invalid enum values or malformed data.
    #[error("invalid metrics data: {0}")]
    InvalidData(String),
}

/// Information about a completed tool call.
///
/// This structure contains the completion details for a tool invocation,
/// including when it finished, whether it succeeded, and any error information.
///
/// # Fields
///
/// - `completed_at`: Timestamp when the tool finished execution
/// - `success`: Whether the tool executed successfully
/// - `error`: Error message if the tool failed, None on success
///
/// # Example
///
/// ```rust,ignore
/// use bamboo_agent::agent::metrics::storage::ToolCallCompletion;
/// use chrono::Utc;
///
/// let completion = ToolCallCompletion {
///     completed_at: Utc::now(),
///     success: true,
///     error: None,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ToolCallCompletion {
    /// Timestamp when the tool call completed
    pub completed_at: DateTime<Utc>,
    /// Whether the tool execution succeeded
    pub success: bool,
    /// Error message if execution failed, None on success
    pub error: Option<String>,
}

/// Trait defining the interface for metrics storage backends.
///
/// This trait provides an abstract interface for storing and querying metrics data.
/// The primary implementation is [`SqliteMetricsStorage`], but this trait allows
/// for alternative backends (e.g., PostgreSQL, TimescaleDB) to be implemented.
///
/// # Async Operations
///
/// All methods are async to support non-blocking I/O operations. The SQLite
/// implementation uses `spawn_blocking` to avoid blocking the async runtime
/// with database operations.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow sharing across async tasks.
///
/// # Data Consistency
///
/// The storage system maintains referential integrity between:
/// - Sessions → Rounds → Tool Calls
/// - Sessions aggregate data from child entities
/// - Deletions cascade appropriately
///
/// # Example
///
/// ```rust,ignore
/// use bamboo_agent::agent::metrics::storage::{MetricsStorage, SqliteMetricsStorage};
/// use bamboo_agent::agent::metrics::types::{SessionStatus, TokenUsage};
/// use chrono::Utc;
///
/// async fn example(storage: &dyn MetricsStorage) -> Result<(), Box<dyn std::error::Error>> {
///     // Start a session
///     storage.upsert_session_start("s1", "gpt-4", Utc::now()).await?;
///
///     // Add a round
///     storage.insert_round_start("r1", "s1", "gpt-4", Utc::now()).await?;
///
///     // Complete the round
///     storage.complete_round(
///         "r1",
///         Utc::now(),
///         bamboo::agent::metrics::types::RoundStatus::Success,
///         TokenUsage { prompt_tokens: 10, completion_tokens: 20, total_tokens: 30 },
///         None
///     ).await?;
///
///     // Complete the session
///     storage.complete_session("s1", SessionStatus::Completed, Utc::now()).await?;
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait MetricsStorage: Send + Sync {
    /// Initializes the storage backend.
    ///
    /// This must be called before any other storage operations.
    /// For SQLite, this creates the database schema if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be created or initialized.
    async fn init(&self) -> MetricsResult<()>;

    /// Records the start of a new chat session.
    ///
    /// If a session with the same ID already exists, it will be reset to
    /// running status (useful for session recovery scenarios).
    ///
    /// # Arguments
    ///
    /// * `session_id` - Unique identifier for the session
    /// * `model` - AI model being used (e.g., "gpt-4", "claude-3")
    /// * `started_at` - Timestamp when the session started
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// storage.upsert_session_start("session-123", "gpt-4", Utc::now()).await?;
    /// ```
    async fn upsert_session_start(
        &self,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    /// Updates the message count for a session.
    ///
    /// This should be called whenever messages are added to the conversation.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session to update
    /// * `message_count` - New total message count
    /// * `updated_at` - Timestamp of the update
    async fn update_session_message_count(
        &self,
        session_id: &str,
        message_count: u32,
        updated_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    /// Marks a session as completed with a final status.
    ///
    /// This triggers a final aggregation of all session metrics before closing.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session to complete
    /// * `status` - Final session status (completed, failed, or cancelled)
    /// * `completed_at` - Timestamp when the session ended
    async fn complete_session(
        &self,
        session_id: &str,
        status: SessionStatus,
        completed_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    /// Records the start of a new round within a session.
    ///
    /// A round represents a single request-response cycle. This also
    /// triggers an update to the parent session's aggregate counters.
    ///
    /// # Arguments
    ///
    /// * `round_id` - Unique identifier for this round
    /// * `session_id` - Parent session this round belongs to
    /// * `model` - AI model being used for this round
    /// * `started_at` - Timestamp when the round started
    async fn insert_round_start(
        &self,
        round_id: &str,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    /// Completes a round with final metrics and status.
    ///
    /// This records the round's completion and triggers an update to
    /// the parent session's aggregated metrics.
    ///
    /// # Arguments
    ///
    /// * `round_id` - Round to complete
    /// * `completed_at` - Timestamp when the round finished
    /// * `status` - Final round status (success or failed)
    /// * `usage` - Token consumption during this round
    /// * `error` - Error message if the round failed, None on success
    #[allow(clippy::too_many_arguments)]
    async fn complete_round(
        &self,
        round_id: &str,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        prompt_cached_tool_outputs: u32,
        prompt_cached_tool_tokens_saved: u32,
        error: Option<String>,
    ) -> MetricsResult<()>;

    /// Records a context-compression event against a round and refreshes the parent session aggregates.
    async fn record_round_compression(
        &self,
        round_id: &str,
        compressed_at: DateTime<Utc>,
        tokens_saved: u32,
    ) -> MetricsResult<()>;

    /// Records the start of a tool invocation.
    ///
    /// Tools are called during rounds to perform specific actions
    /// (e.g., reading files, executing commands).
    ///
    /// # Arguments
    ///
    /// * `tool_call_id` - Unique identifier for this tool call
    /// * `round_id` - Round this tool call belongs to
    /// * `session_id` - Session this tool call belongs to
    /// * `tool_name` - Name of the tool being invoked
    /// * `started_at` - Timestamp when the tool was invoked
    async fn insert_tool_start(
        &self,
        tool_call_id: &str,
        round_id: &str,
        session_id: &str,
        tool_name: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    /// Records the completion of a tool call.
    ///
    /// This updates the tool call record with completion details and
    /// triggers an update to the parent session's tool call count.
    ///
    /// # Arguments
    ///
    /// * `tool_call_id` - Tool call to complete
    /// * `completion` - Completion details including success status and timing
    async fn complete_tool_call(
        &self,
        tool_call_id: &str,
        completion: ToolCallCompletion,
    ) -> MetricsResult<()>;

    // Forward request metrics methods

    /// Records the start of a forwarded HTTP request to an upstream API.
    ///
    /// This tracks requests proxied to external API providers like OpenAI or Anthropic.
    ///
    /// # Arguments
    ///
    /// * `forward_id` - Unique identifier for this forwarded request
    /// * `endpoint` - API endpoint identifier (e.g., "openai.chat_completions")
    /// * `model` - AI model being requested
    /// * `is_stream` - Whether this is a streaming (SSE) request
    /// * `started_at` - Timestamp when the request was initiated
    async fn insert_forward_start(
        &self,
        forward_id: &str,
        endpoint: &str,
        model: &str,
        is_stream: bool,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    /// Completes a forwarded request with response details.
    ///
    /// This records the response from the upstream API, including status,
    /// token usage, and any errors.
    ///
    /// # Arguments
    ///
    /// * `forward_id` - Forwarded request to complete
    /// * `completed_at` - Timestamp when the response was received
    /// * `status_code` - HTTP status code from the upstream API
    /// * `status` - Classified status (success, error, or timeout)
    /// * `usage` - Token usage if provided in the response
    /// * `token_details` - Provider-specific cache/reasoning dimensions
    /// * `error` - Error message if the request failed
    #[allow(clippy::too_many_arguments)]
    async fn complete_forward(
        &self,
        forward_id: &str,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: ForwardStatus,
        usage: Option<TokenUsage>,
        token_details: Option<ForwardTokenDetails>,
        error: Option<String>,
    ) -> MetricsResult<()>;

    /// Retrieves aggregated summary statistics for forwarded requests.
    ///
    /// Returns counts of total/successful/failed requests, token usage,
    /// and average latency for requests matching the filter criteria.
    ///
    /// # Arguments
    ///
    /// * `filter` - Filter criteria for date range, endpoint, and model
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use bamboo_agent::agent::metrics::types::ForwardMetricsFilter;
    ///
    /// let filter = ForwardMetricsFilter {
    ///     start_date: Some(NaiveDate::from_ymd_opt(2026, 2, 1).unwrap()),
    ///     end_date: Some(NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()),
    ///     endpoint: Some("openai.chat_completions".to_string()),
    ///     model: None,
    ///     limit: None,
    /// };
    ///
    /// let summary = storage.forward_summary(filter).await?;
    /// println!("Total requests: {}", summary.total_requests);
    /// println!("Success rate: {:.2}%",
    ///     (summary.successful_requests as f64 / summary.total_requests as f64) * 100.0);
    /// ```
    async fn forward_summary(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<ForwardMetricsSummary>;

    /// Retrieves metrics grouped by endpoint.
    ///
    /// Returns per-endpoint statistics including request counts,
    /// success rates, token usage, and average latency.
    ///
    /// # Arguments
    ///
    /// * `filter` - Filter criteria (endpoint filter is ignored, grouped by all endpoints)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let endpoints = storage.forward_by_endpoint(filter).await?;
    /// for endpoint in endpoints {
    ///     println!("{}: {} requests, {:.2}ms avg",
    ///         endpoint.endpoint,
    ///         endpoint.requests,
    ///         endpoint.avg_duration_ms.unwrap_or(0) as f64
    ///     );
    /// }
    /// ```
    async fn forward_by_endpoint(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardEndpointMetrics>>;

    /// Retrieves individual forward request records.
    ///
    /// Returns detailed information about each forwarded request,
    /// including timing, status, and token usage.
    ///
    /// # Arguments
    ///
    /// * `filter` - Filter criteria including pagination via `limit`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let filter = ForwardMetricsFilter {
    ///     limit: Some(50),
    ///     ..Default::default()
    /// };
    ///
    /// let requests = storage.forward_requests(filter).await?;
    /// for req in requests {
    ///     println!("{}: {} - {:?}", req.forward_id, req.endpoint, req.status);
    /// }
    /// ```
    async fn forward_requests(
        &self,
        filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardRequestMetrics>>;

    /// Retrieves daily aggregated metrics for forwarded requests.
    ///
    /// Returns per-day statistics for the specified date range,
    /// useful for trend analysis and reporting.
    ///
    /// # Arguments
    ///
    /// * `days` - Number of days to include
    /// * `end_date` - End date for the range (defaults to today)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let daily = storage.forward_daily_metrics(7, None).await?;
    /// for day in daily {
    ///     println!("{}: {} requests, {} tokens",
    ///         day.date,
    ///         day.total_sessions,
    ///         day.total_token_usage.total_tokens
    ///     );
    /// }
    /// ```
    async fn forward_daily_metrics(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>>;

    /// Retrieves daily forwarded-request metrics restricted to one model.
    /// Blank model values have the same meaning as omission.
    async fn forward_daily_metrics_for_model(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        model: Option<String>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        if normalize_filter_value(model).is_none() {
            self.forward_daily_metrics(days, end_date).await
        } else {
            Err(MetricsError::InvalidData(
                "model-filtered forward daily metrics are not supported by this storage"
                    .to_string(),
            ))
        }
    }

    /// Retrieves daily forwarded-request metrics using the same endpoint/model
    /// semantics as the other forward metrics queries.
    async fn forward_daily_metrics_filtered(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        endpoint: Option<String>,
        model: Option<String>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        if normalize_filter_value(endpoint).is_some() {
            return Err(MetricsError::InvalidData(
                "endpoint-filtered forward daily metrics are not supported by this storage"
                    .to_string(),
            ));
        }
        self.forward_daily_metrics_for_model(days, end_date, model)
            .await
    }

    /// Retrieves aggregated summary statistics for chat sessions.
    ///
    /// Session/status counts are filtered by session start. Token/cache/
    /// compression usage is filtered by each round's start, and tool counts by
    /// each tool call's start. This intentionally keeps session lifecycle
    /// dimensions separate from usage occurrence dimensions.
    ///
    /// # Arguments
    ///
    /// * `filter` - Date range filter criteria
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use bamboo_agent::agent::metrics::types::MetricsDateFilter;
    ///
    /// let filter = MetricsDateFilter {
    ///     start_date: Some(NaiveDate::from_ymd_opt(2026, 2, 1).unwrap()),
    ///     end_date: Some(NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()),
    /// };
    ///
    /// let summary = storage.summary(filter).await?;
    /// println!("Active sessions: {}", summary.active_sessions);
    /// println!("Total tokens: {}", summary.total_tokens.total_tokens);
    /// ```
    async fn summary(&self, filter: MetricsDateFilter) -> MetricsResult<MetricsSummary>;

    /// Retrieves aggregate chat metrics with an optional model restriction.
    ///
    /// The default preserves compatibility for existing storage
    /// implementations when no model is selected. Implementations that support
    /// model filtering should override this method.
    async fn summary_filtered(
        &self,
        filter: ModelMetricsDateFilter,
    ) -> MetricsResult<MetricsSummary> {
        let ModelMetricsDateFilter {
            start_date,
            end_date,
            model,
        } = filter;
        if normalize_filter_value(model).is_none() {
            self.summary(MetricsDateFilter {
                start_date,
                end_date,
            })
            .await
        } else {
            Err(MetricsError::InvalidData(
                "model-filtered summary metrics are not supported by this storage".to_string(),
            ))
        }
    }

    /// Retrieves metrics grouped by AI model.
    ///
    /// Session counts use the session row's model and start date. Rounds and
    /// token/cache usage use each round's own model and start date; tool calls
    /// use their own start date and the model of their owning round. Models
    /// present in only one side are retained.
    ///
    /// # Arguments
    ///
    /// * `filter` - Date range filter criteria
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let models = storage.by_model(filter).await?;
    /// for model in models {
    ///     println!("{}: {} sessions, {} tokens",
    ///         model.model,
    ///         model.sessions,
    ///         model.tokens.total_tokens
    ///     );
    /// }
    /// ```
    async fn by_model(&self, filter: MetricsDateFilter) -> MetricsResult<Vec<ModelMetrics>>;

    /// Retrieves grouped model metrics with an optional model restriction.
    ///
    /// The default preserves compatibility for existing storage
    /// implementations when no model is selected. Implementations that support
    /// model filtering should override this method.
    async fn by_model_filtered(
        &self,
        filter: ModelMetricsDateFilter,
    ) -> MetricsResult<Vec<ModelMetrics>> {
        let ModelMetricsDateFilter {
            start_date,
            end_date,
            model,
        } = filter;
        if normalize_filter_value(model).is_none() {
            self.by_model(MetricsDateFilter {
                start_date,
                end_date,
            })
            .await
        } else {
            Err(MetricsError::InvalidData(
                "model-filtered grouped metrics are not supported by this storage".to_string(),
            ))
        }
    }

    /// Retrieves session metrics with filtering and pagination.
    ///
    /// Returns detailed information about sessions matching the filter criteria,
    /// including token usage, tool breakdown, and status.
    ///
    /// # Arguments
    ///
    /// * `filter` - Filter criteria including date range, model, and pagination
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use bamboo_agent::agent::metrics::types::SessionMetricsFilter;
    ///
    /// let filter = SessionMetricsFilter {
    ///     model: Some("gpt-4".to_string()),
    ///     limit: Some(100),
    ///     ..Default::default()
    /// };
    ///
    /// let sessions = storage.sessions(filter).await?;
    /// for session in sessions {
    ///     println!("{}: {} rounds, {} tools",
    ///         session.session_id,
    ///         session.total_rounds,
    ///         session.tool_call_count
    ///     );
    /// }
    /// ```
    async fn sessions(&self, filter: SessionMetricsFilter) -> MetricsResult<Vec<SessionMetrics>>;

    /// Retrieves complete details for a specific session.
    ///
    /// Returns the session metrics along with all associated rounds
    /// and their tool calls for detailed analysis.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session to retrieve
    ///
    /// # Returns
    ///
    /// Returns `Ok(None)` if the session doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(detail) = storage.session_detail("session-123").await? {
    ///     println!("Session: {}", detail.session.session_id);
    ///     for round in detail.rounds {
    ///         println!("  Round {}: {} tokens, {} tools",
    ///             round.round_id,
    ///             round.token_usage.total_tokens,
    ///             round.tool_calls.len()
    ///         );
    ///     }
    /// }
    /// ```
    async fn session_detail(&self, session_id: &str) -> MetricsResult<Option<SessionDetail>>;

    /// Increments the execute sync mismatch counter for a specific stable reason label.
    async fn increment_execute_sync_mismatch(
        &self,
        reason: &str,
        occurred_at: DateTime<Utc>,
    ) -> MetricsResult<()>;

    /// Retrieves daily aggregated metrics for chat sessions.
    ///
    /// Session counts are attributed to the session start date. Round usage and
    /// model breakdown are attributed to each round's start date, including
    /// still-running rounds; tool totals/breakdown use each tool call's start
    /// date. A day with later activity can therefore have zero new sessions.
    ///
    /// # Arguments
    ///
    /// * `days` - Number of days to include
    /// * `end_date` - End date for the range (defaults to today)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let daily = storage.daily_metrics(30, None).await?;
    /// for day in daily {
    ///     println!("{}: {} sessions, {} tokens",
    ///         day.date,
    ///         day.total_sessions,
    ///         day.total_token_usage.total_tokens
    ///     );
    ///
    ///     // Model breakdown
    ///     for (model, usage) in day.model_breakdown {
    ///         println!("  {}: {} tokens", model, usage.total_tokens);
    ///     }
    /// }
    /// ```
    async fn daily_metrics(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>>;

    /// Retrieves daily chat metrics restricted to one model. Session/status
    /// dimensions use the session model, while round/cache/token dimensions
    /// and tool calls use the owning round model. Blank values mean all models.
    async fn daily_metrics_for_model(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        model: Option<String>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        if normalize_filter_value(model).is_none() {
            self.daily_metrics(days, end_date).await
        } else {
            Err(MetricsError::InvalidData(
                "model-filtered daily metrics are not supported by this storage".to_string(),
            ))
        }
    }

    /// Deletes old round records before a cutoff date.
    ///
    /// This is used for data retention and cleanup. After deleting rounds,
    /// it triggers a refresh of affected session aggregates.
    ///
    /// # Arguments
    ///
    /// * `cutoff` - Delete rounds started before this timestamp
    ///
    /// # Returns
    ///
    /// Returns the number of rounds deleted.
    ///
    /// # Warning
    ///
    /// This operation is irreversible. Ensure you have backups if needed.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use chrono::{Duration, Utc};
    ///
    /// // Delete rounds older than 90 days
    /// let cutoff = Utc::now() - Duration::days(90);
    /// let deleted = storage.prune_rounds_before(cutoff).await?;
    /// println!("Deleted {} old rounds", deleted);
    /// ```
    async fn prune_rounds_before(&self, cutoff: DateTime<Utc>) -> MetricsResult<u64>;

    /// Reconciles stale session / round / forward rows using durable runtime hints.
    async fn reconcile_stale_executions(
        &self,
        active_session_ids: &[String],
        awaiting_response_session_ids: &[String],
    ) -> MetricsResult<()>;
}

/// SQLite-based implementation of the MetricsStorage trait.
///
/// This is the primary storage backend for the metrics system, using SQLite
/// with WAL (Write-Ahead Logging) mode for reliable concurrent access.
///
/// # Features
///
/// - **WAL Mode**: Enables concurrent readers with writers
/// - **Foreign Keys**: Enforces referential integrity
/// - **Async Compatible**: Uses `spawn_blocking` to avoid blocking the async runtime
/// - **Automatic Schema Migration**: Creates tables on initialization
///
/// # Database Schema
///
/// The database contains four main tables:
///
/// ## session_metrics
/// Stores aggregated session-level metrics with columns for:
/// - Session identification (session_id, model)
/// - Timing (started_at, completed_at, updated_at)
/// - Aggregates (total_rounds, prompt_tokens, completion_tokens, total_tokens, tool_call_count)
/// - Status and message count
///
/// ## round_metrics
/// Stores individual round metrics with foreign keys to sessions:
/// - Round identification (round_id, session_id, model)
/// - Timing and status
/// - Token usage per round
/// - Error information
///
/// ## tool_call_metrics
/// Stores tool invocation details with foreign keys to rounds and sessions:
/// - Tool identification (tool_call_id, round_id, session_id, tool_name)
/// - Execution timing and success status
/// - Error details
///
/// ## forward_request_metrics
/// Stores HTTP proxy request tracking:
/// - Request identification (forward_id, endpoint, model)
/// - Request type (is_stream)
/// - Response details (status_code, status, token usage)
/// - Error information
///
/// # Example
///
/// ```rust,ignore
/// use bamboo_agent::agent::metrics::storage::SqliteMetricsStorage;
/// use bamboo_agent::agent::metrics::storage::MetricsStorage;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Create storage instance
///     let storage = SqliteMetricsStorage::new("path/to/metrics.db");
///
///     // Initialize database schema
///     storage.init().await?;
///
///     // Now ready to use
///     storage.upsert_session_start("s1", "gpt-4", chrono::Utc::now()).await?;
///
///     Ok(())
/// }
/// ```
///
/// # Thread Safety
///
/// The storage can be safely cloned and shared across threads. Each operation
/// opens its own database connection to avoid blocking and ensure thread safety.
#[derive(Debug, Clone)]
pub struct SqliteMetricsStorage {
    /// Path to the SQLite database file
    db_path: PathBuf,
}

impl SqliteMetricsStorage {
    /// Creates a new SQLite storage instance.
    ///
    /// The database file will be created when [`init`](MetricsStorage::init) is called.
    /// If the file already exists, it will be used as-is.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the SQLite database file (will create parent directories if needed)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use bamboo_agent::agent::metrics::storage::SqliteMetricsStorage;
    ///
    /// let storage = SqliteMetricsStorage::new("metrics.db");
    /// let storage = SqliteMetricsStorage::new("/var/data/bamboo/metrics.db");
    /// ```
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_path_buf(),
        }
    }

    /// Executes a function with a database connection in a blocking context.
    ///
    /// This helper method handles:
    /// 1. Opening a connection to the database
    /// 2. Running the provided function in `spawn_blocking` to avoid blocking async runtime
    /// 3. Proper error handling and task joining
    ///
    /// # Type Parameters
    ///
    /// * `T` - Return type of the function (must be Send + 'static)
    /// * `F` - Function type (must be Send + 'static)
    ///
    /// # Arguments
    ///
    /// * `func` - Function to execute with the database connection
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database connection fails to open
    /// - The function returns an error
    /// - The blocking task fails to complete
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
                    prompt_cached_tool_outputs INTEGER NOT NULL DEFAULT 0,
                    prompt_cached_tool_tokens_saved INTEGER NOT NULL DEFAULT 0,
                    total_compression_events INTEGER NOT NULL DEFAULT 0,
                    total_tokens_saved INTEGER NOT NULL DEFAULT 0,
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
                    prompt_cached_tool_outputs INTEGER NOT NULL DEFAULT 0,
                    prompt_cached_tool_tokens_saved INTEGER NOT NULL DEFAULT 0,
                    compression_count INTEGER NOT NULL DEFAULT 0,
                    tokens_saved INTEGER NOT NULL DEFAULT 0,
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
                CREATE INDEX IF NOT EXISTS idx_round_session_started_at ON round_metrics(session_id, started_at);
                -- Install the replacement before dropping our redundant prefix index.
                -- Repeated or interrupted initialization always retains a session lookup.
                DROP INDEX IF EXISTS idx_round_session;
                CREATE INDEX IF NOT EXISTS idx_round_started_at ON round_metrics(started_at);
                CREATE INDEX IF NOT EXISTS idx_tool_session ON tool_call_metrics(session_id);
                CREATE INDEX IF NOT EXISTS idx_tool_round_started_at ON tool_call_metrics(round_id, started_at);
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
                    status TEXT NOT NULL DEFAULT 'pending',
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    total_tokens INTEGER,
                    cache_creation_input_tokens INTEGER,
                    cache_read_input_tokens INTEGER,
                    cache_write_input_tokens INTEGER,
                    reasoning_output_tokens INTEGER,
                    error TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execute_sync_mismatch_metrics (
                    reason TEXT NOT NULL,
                    mismatch_date TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (reason, mismatch_date)
                );

                CREATE INDEX IF NOT EXISTS idx_forward_started_at ON forward_request_metrics(started_at);
                CREATE INDEX IF NOT EXISTS idx_forward_endpoint ON forward_request_metrics(endpoint);
                CREATE INDEX IF NOT EXISTS idx_forward_model ON forward_request_metrics(model);
                CREATE INDEX IF NOT EXISTS idx_execute_sync_mismatch_date ON execute_sync_mismatch_metrics(mismatch_date);
                CREATE INDEX IF NOT EXISTS idx_execute_sync_mismatch_reason ON execute_sync_mismatch_metrics(reason);
                "#,
            )?;
            ensure_integer_column(
                connection,
                "session_metrics",
                "prompt_cached_tool_outputs",
                0,
            )?;
            ensure_integer_column(connection, "session_metrics", "prompt_cached_tool_tokens_saved", 0)?;
            ensure_integer_column(connection, "session_metrics", "total_compression_events", 0)?;
            ensure_integer_column(connection, "session_metrics", "total_tokens_saved", 0)?;
            ensure_integer_column(connection, "round_metrics", "prompt_cached_tool_outputs", 0)?;
            ensure_integer_column(connection, "round_metrics", "prompt_cached_tool_tokens_saved", 0)?;
            ensure_integer_column(connection, "round_metrics", "compression_count", 0)?;
            ensure_integer_column(connection, "round_metrics", "tokens_saved", 0)?;
            ensure_nullable_integer_column(
                connection,
                "forward_request_metrics",
                "cache_creation_input_tokens",
            )?;
            ensure_nullable_integer_column(
                connection,
                "forward_request_metrics",
                "cache_read_input_tokens",
            )?;
            ensure_nullable_integer_column(
                connection,
                "forward_request_metrics",
                "cache_write_input_tokens",
            )?;
            ensure_nullable_integer_column(
                connection,
                "forward_request_metrics",
                "reasoning_output_tokens",
            )?;
            connection.execute(
                "UPDATE forward_request_metrics SET status = 'pending' WHERE status IS NULL OR trim(status) = ''",
                [],
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
            with_immediate_transaction(connection, || {
                refresh_session_aggregates(connection, &session_id, completed_at)?;
                connection.execute(
                    "UPDATE session_metrics SET status = ?1, completed_at = ?2, updated_at = ?2 WHERE session_id = ?3",
                    params![status.as_str(), completed_at_str, session_id],
                )?;
                Ok(())
            })
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
            with_immediate_transaction(connection, || {
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
        })
        .await
    }

    async fn complete_round(
        &self,
        round_id: &str,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        prompt_cached_tool_outputs: u32,
        prompt_cached_tool_tokens_saved: u32,
        error: Option<String>,
    ) -> MetricsResult<()> {
        let round_id = round_id.to_string();
        let completed_at_str = format_timestamp(completed_at);
        // SQLite INTEGER is signed 64-bit. Normalize before conversion so
        // extreme provider values saturate instead of wrapping negative.
        let usage = usage.clamped_for_durable_metrics();
        let prompt_tokens = durable_token_to_i64(usage.prompt_tokens);
        let completion_tokens = durable_token_to_i64(usage.completion_tokens);
        let total_tokens = durable_token_to_i64(usage.total_tokens);

        self.with_connection(move |connection| {
            with_immediate_transaction(connection, || {
                #[cfg(test)]
                signal_complete_round_transaction_entered(&round_id);

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
                        prompt_cached_tool_outputs = ?6,
                        prompt_cached_tool_tokens_saved = ?7,
                        -- `RoundCompleted` may be replayed. Replace its prompt-
                        -- cache contribution while preserving tokens recorded by
                        -- separate compression events, rather than adding the
                        -- same completion payload again.
                        tokens_saved = MAX(
                            COALESCE(tokens_saved, 0) - COALESCE(prompt_cached_tool_tokens_saved, 0),
                            0
                        ) + ?8,
                        error = ?9
                    WHERE round_id = ?10
                    "#,
                    params![
                        completed_at_str,
                        status.as_str(),
                        prompt_tokens,
                        completion_tokens,
                        total_tokens,
                        i64::from(prompt_cached_tool_outputs),
                        i64::from(prompt_cached_tool_tokens_saved),
                        i64::from(prompt_cached_tool_tokens_saved),
                        error,
                        round_id,
                    ],
                )?;

                refresh_session_aggregates(connection, &session_id, completed_at)?;
                Ok(())
            })
        })
        .await
    }

    async fn record_round_compression(
        &self,
        round_id: &str,
        compressed_at: DateTime<Utc>,
        tokens_saved: u32,
    ) -> MetricsResult<()> {
        let round_id = round_id.to_string();

        self.with_connection(move |connection| {
            with_immediate_transaction(connection, || {
                let session_id: String = connection.query_row(
                    "SELECT session_id FROM round_metrics WHERE round_id = ?1",
                    params![round_id],
                    |row| row.get(0),
                )?;

                connection.execute(
                    r#"
                    UPDATE round_metrics
                    SET compression_count = COALESCE(compression_count, 0) + 1,
                        tokens_saved = COALESCE(tokens_saved, 0) + ?1
                    WHERE round_id = ?2
                    "#,
                    params![i64::from(tokens_saved), round_id],
                )?;

                refresh_session_aggregates(connection, &session_id, compressed_at)?;
                Ok(())
            })
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
            with_immediate_transaction(connection, || {
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
                    forward_id, endpoint, model, is_stream, started_at, status, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?5)
                ON CONFLICT(forward_id) DO UPDATE SET
                    endpoint = excluded.endpoint,
                    model = excluded.model,
                    is_stream = excluded.is_stream,
                    started_at = excluded.started_at,
                    completed_at = NULL,
                    status_code = NULL,
                    status = 'pending',
                    prompt_tokens = NULL,
                    completion_tokens = NULL,
                    total_tokens = NULL,
                    cache_creation_input_tokens = NULL,
                    cache_read_input_tokens = NULL,
                    cache_write_input_tokens = NULL,
                    reasoning_output_tokens = NULL,
                    error = NULL,
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
        token_details: Option<ForwardTokenDetails>,
        error: Option<String>,
    ) -> MetricsResult<()> {
        let forward_id = forward_id.to_string();
        let completed_at_str = format_timestamp(completed_at);
        let status_code_int = status_code.map(|s| s as i64);
        let (prompt, completion, total) = match usage {
            Some(u) => (
                Some(u.prompt_tokens as i64),
                Some(u.completion_tokens as i64),
                Some(u.total_tokens as i64),
            ),
            None => (None, None, None),
        };
        let token_details = token_details.unwrap_or_default();
        let cache_creation = token_details
            .cache_creation_input_tokens
            .map(|value| value as i64);
        let cache_read = token_details
            .cache_read_input_tokens
            .map(|value| value as i64);
        let cache_write = token_details
            .cache_write_input_tokens
            .map(|value| value as i64);
        let reasoning_output = token_details
            .reasoning_output_tokens
            .map(|value| value as i64);

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
                    cache_creation_input_tokens = ?7,
                    cache_read_input_tokens = ?8,
                    cache_write_input_tokens = ?9,
                    reasoning_output_tokens = ?10,
                    error = ?11,
                    updated_at = ?1
                WHERE forward_id = ?12
                "#,
                params![
                    completed_at_str,
                    status_code_int,
                    status.as_str(),
                    prompt,
                    completion,
                    total,
                    cache_creation,
                    cache_read,
                    cache_write,
                    reasoning_output,
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
                 SUM(cache_creation_input_tokens), \
                 SUM(cache_read_input_tokens), \
                 SUM(cache_write_input_tokens), \
                 SUM(reasoning_output_tokens), \
                 AVG(CASE WHEN completed_at IS NOT NULL THEN \
                     (julianday(completed_at) - julianday(started_at)) * 86400000 END) \
                 FROM forward_request_metrics {}",
                where_clause
            );

            let mut stmt = connection.prepare(&sql)?;
            let summary = stmt.query_row(params_from_iter(params_vec.iter()), |row| {
                let avg_duration: Option<f64> = row.get(10)?;
                Ok(ForwardMetricsSummary {
                    total_requests: row.get::<_, i64>(0)? as u64,
                    successful_requests: row.get::<_, i64>(1)? as u64,
                    failed_requests: row.get::<_, i64>(2)? as u64,
                    total_tokens: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(3)? as u64,
                        completion_tokens: row.get::<_, i64>(4)? as u64,
                        total_tokens: row.get::<_, i64>(5)? as u64,
                    },
                    token_details: ForwardTokenDetails {
                        cache_creation_input_tokens: row
                            .get::<_, Option<i64>>(6)?
                            .map(|value| value as u64),
                        cache_read_input_tokens: row
                            .get::<_, Option<i64>>(7)?
                            .map(|value| value as u64),
                        cache_write_input_tokens: row
                            .get::<_, Option<i64>>(8)?
                            .map(|value| value as u64),
                        reasoning_output_tokens: row
                            .get::<_, Option<i64>>(9)?
                            .map(|value| value as u64),
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
                 SUM(cache_creation_input_tokens), \
                 SUM(cache_read_input_tokens), \
                 SUM(cache_write_input_tokens), \
                 SUM(reasoning_output_tokens), \
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
                let avg_duration: Option<f64> = row.get(11)?;
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
                    token_details: ForwardTokenDetails {
                        cache_creation_input_tokens: row
                            .get::<_, Option<i64>>(7)?
                            .map(|value| value as u64),
                        cache_read_input_tokens: row
                            .get::<_, Option<i64>>(8)?
                            .map(|value| value as u64),
                        cache_write_input_tokens: row
                            .get::<_, Option<i64>>(9)?
                            .map(|value| value as u64),
                        reasoning_output_tokens: row
                            .get::<_, Option<i64>>(10)?
                            .map(|value| value as u64),
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
                 status_code, status, prompt_tokens, completion_tokens, total_tokens, \
                 cache_creation_input_tokens, cache_read_input_tokens, \
                 cache_write_input_tokens, reasoning_output_tokens, error \
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
                    token_details: ForwardTokenDetails {
                        cache_creation_input_tokens: row
                            .get::<_, Option<i64>>(11)?
                            .map(|value| value as u64),
                        cache_read_input_tokens: row
                            .get::<_, Option<i64>>(12)?
                            .map(|value| value as u64),
                        cache_write_input_tokens: row
                            .get::<_, Option<i64>>(13)?
                            .map(|value| value as u64),
                        reasoning_output_tokens: row
                            .get::<_, Option<i64>>(14)?
                            .map(|value| value as u64),
                    },
                    error: row.get(15)?,
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
        self.forward_daily_metrics_filtered(days, end_date, None, None)
            .await
    }

    async fn forward_daily_metrics_for_model(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        model: Option<String>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        self.forward_daily_metrics_filtered(days, end_date, None, model)
            .await
    }

    async fn forward_daily_metrics_filtered(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        endpoint: Option<String>,
        model: Option<String>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        let end_date = end_date.unwrap_or_else(|| Utc::now().date_naive());
        let span = days.max(1) - 1;
        let start_date = end_date - chrono::Duration::days(i64::from(span));
        let endpoint = normalize_filter_value(endpoint);
        let model = normalize_filter_value(model);

        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_forward_where_clause(
                Some(start_date),
                Some(end_date),
                endpoint.as_deref(),
                model.as_deref(),
                &mut params_vec,
            );
            let sql = format!(
                r#"
                SELECT
                    date(started_at) AS date_key,
                    COUNT(*) AS total_sessions,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens
                FROM forward_request_metrics
                {}
                GROUP BY date_key
                ORDER BY date_key ASC
                "#,
                where_clause
            );
            let mut stmt = connection.prepare(&sql)?;

            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
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
                    prompt_cached_tool_outputs: 0,
                    model_breakdown: HashMap::new(),
                    tool_breakdown: HashMap::new(),
                });
            }

            Ok(result)
        })
        .await
    }

    async fn summary(&self, filter: MetricsDateFilter) -> MetricsResult<MetricsSummary> {
        self.summary_filtered(filter.into()).await
    }

    async fn summary_filtered(
        &self,
        filter: ModelMetricsDateFilter,
    ) -> MetricsResult<MetricsSummary> {
        let ModelMetricsDateFilter {
            start_date,
            end_date,
            model,
        } = filter;
        let model = normalize_filter_value(model);
        self.with_connection(move |connection| {
            // Lifecycle dimensions belong to the session start date. Do not
            // infer status/counts from rounds: a session may have no rounds, or
            // may continue producing rounds on later days.
            let mut session_params = Vec::new();
            let session_clause = build_session_model_where_clause(
                start_date,
                end_date,
                None,
                model.as_deref(),
                &mut session_params,
            );
            let session_sql = format!(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status = 'awaiting_response' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0) FROM session_metrics {}",
                session_clause
            );
            let session_stats = connection.query_row(
                &session_sql,
                params_from_iter(session_params.iter()),
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )?;

            // Usage dimensions belong to the round occurrence. `started_at`
            // is non-null for both running and completed rows and records when
            // the billed model turn began; nullable `completed_at` is therefore
            // intentionally not used as the attribution key.
            let mut round_params = Vec::new();
            let round_clause = build_started_at_model_where_clause(
                "started_at",
                "model",
                start_date,
                end_date,
                model.as_deref(),
                &mut round_params,
            );
            let round_sql = format!(
                "SELECT COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), COALESCE(SUM(total_tokens), 0), COALESCE(SUM(prompt_cached_tool_outputs), 0), COALESCE(SUM(prompt_cached_tool_tokens_saved), 0), COALESCE(SUM(compression_count), 0), COALESCE(SUM(tokens_saved), 0) FROM round_metrics {}",
                round_clause
            );
            let round_stats = connection.query_row(
                &round_sql,
                params_from_iter(round_params.iter()),
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )?;

            let mut tool_params = Vec::new();
            let tool_clause = if model.is_some() {
                build_started_at_model_where_clause(
                    "tool_call_metrics.started_at",
                    "round_metrics.model",
                    start_date,
                    end_date,
                    model.as_deref(),
                    &mut tool_params,
                )
            } else {
                build_started_at_where_clause(
                    "started_at",
                    start_date,
                    end_date,
                    &mut tool_params,
                )
            };
            let tool_sql = if model.is_some() {
                format!(
                    "SELECT COUNT(*) FROM tool_call_metrics JOIN round_metrics ON round_metrics.round_id = tool_call_metrics.round_id {}",
                    tool_clause
                )
            } else {
                format!("SELECT COUNT(*) FROM tool_call_metrics {}", tool_clause)
            };
            let total_tool_calls = connection.query_row(
                &tool_sql,
                params_from_iter(tool_params.iter()),
                |row| row.get::<_, i64>(0),
            )?;

            let mut summary = MetricsSummary {
                total_sessions: session_stats.0 as u64,
                total_tokens: TokenUsage {
                    prompt_tokens: round_stats.0 as u64,
                    completion_tokens: round_stats.1 as u64,
                    total_tokens: round_stats.2 as u64,
                },
                total_tool_calls: total_tool_calls as u64,
                active_sessions: session_stats.5 as u64,
                prompt_cached_tool_outputs: round_stats.3 as u64,
                tool_context_tokens_saved: round_stats.4 as u64,
                total_compression_events: round_stats.5 as u64,
                total_tokens_saved: round_stats.6 as u64,
                non_tool_compression_tokens_saved: (round_stats.6 - round_stats.4).max(0) as u64,
                completed_sessions: session_stats.1 as u64,
                awaiting_response_sessions: session_stats.2 as u64,
                error_sessions: session_stats.3 as u64,
                cancelled_sessions: session_stats.4 as u64,
                total_sync_mismatches: 0,
                sync_mismatch_breakdown: HashMap::new(),
            };

            // Sync-mismatch rows currently have no model column or stable
            // session/round key. Returning their all-model total in a filtered
            // summary would mix populations, so filtered summaries explicitly
            // leave this non-attributable dimension at zero.
            if model.is_none() {
                let mut mismatch_params = Vec::new();
                let mismatch_clause = build_execute_sync_mismatch_where_clause(
                    start_date,
                    end_date,
                    None,
                    &mut mismatch_params,
                );
                let mismatch_sql = format!(
                    "SELECT COALESCE(SUM(count), 0) FROM execute_sync_mismatch_metrics {}",
                    mismatch_clause
                );
                let mut mismatch_stmt = connection.prepare(&mismatch_sql)?;
                summary.total_sync_mismatches = mismatch_stmt
                    .query_row(params_from_iter(mismatch_params.iter()), |row| {
                        row.get::<_, i64>(0)
                    })? as u64;
                summary.sync_mismatch_breakdown =
                    load_execute_sync_mismatch_breakdown(connection, start_date, end_date)?;
            }

            Ok(summary)
        })
        .await
    }

    async fn by_model(&self, filter: MetricsDateFilter) -> MetricsResult<Vec<ModelMetrics>> {
        self.by_model_filtered(filter.into()).await
    }

    async fn by_model_filtered(
        &self,
        filter: ModelMetricsDateFilter,
    ) -> MetricsResult<Vec<ModelMetrics>> {
        let ModelMetricsDateFilter {
            start_date,
            end_date,
            model,
        } = filter;
        let model = normalize_filter_value(model);
        self.with_connection(move |connection| {
            let mut models: HashMap<String, ModelMetrics> = HashMap::new();

            // Session counts retain the session row's model/start semantics.
            let mut session_params = Vec::new();
            let session_clause = build_session_model_where_clause(
                start_date,
                end_date,
                None,
                model.as_deref(),
                &mut session_params,
            );
            let session_sql = format!(
                "SELECT model, COUNT(*) FROM session_metrics {} GROUP BY model",
                session_clause
            );
            {
                let mut stmt = connection.prepare(&session_sql)?;
                let mut rows = stmt.query(params_from_iter(session_params.iter()))?;
                while let Some(row) = rows.next()? {
                    let model = row.get::<_, String>(0)?;
                    models
                        .entry(model.clone())
                        .or_insert_with(|| empty_model_metrics(model))
                        .sessions = row.get::<_, i64>(1)? as u64;
                }
            }

            // Round count, token usage and cache values follow each round's own
            // model and occurrence date, not the session's current model.
            let mut round_params = Vec::new();
            let round_clause = build_started_at_model_where_clause(
                "started_at",
                "model",
                start_date,
                end_date,
                model.as_deref(),
                &mut round_params,
            );
            let round_sql = format!(
                "SELECT model, COUNT(*), COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), COALESCE(SUM(total_tokens), 0), COALESCE(SUM(prompt_cached_tool_outputs), 0) FROM round_metrics {} GROUP BY model",
                round_clause
            );
            {
                let mut stmt = connection.prepare(&round_sql)?;
                let mut rows = stmt.query(params_from_iter(round_params.iter()))?;
                while let Some(row) = rows.next()? {
                    let model = row.get::<_, String>(0)?;
                    let entry = models
                        .entry(model.clone())
                        .or_insert_with(|| empty_model_metrics(model));
                    entry.rounds = row.get::<_, i64>(1)? as u64;
                    entry.tokens = TokenUsage {
                        prompt_tokens: row.get::<_, i64>(2)? as u64,
                        completion_tokens: row.get::<_, i64>(3)? as u64,
                        total_tokens: row.get::<_, i64>(4)? as u64,
                    };
                    entry.prompt_cached_tool_outputs = row.get::<_, i64>(5)? as u64;
                }
            }

            // A tool call has no model column, so attribute it through its
            // owning round while retaining the tool call's own occurrence date.
            let mut tool_params = Vec::new();
            let tool_clause = build_started_at_model_where_clause(
                "tool_call_metrics.started_at",
                "round_metrics.model",
                start_date,
                end_date,
                model.as_deref(),
                &mut tool_params,
            );
            let tool_sql = format!(
                "SELECT round_metrics.model, COUNT(*) FROM tool_call_metrics JOIN round_metrics ON round_metrics.round_id = tool_call_metrics.round_id {} GROUP BY round_metrics.model",
                tool_clause
            );
            {
                let mut stmt = connection.prepare(&tool_sql)?;
                let mut rows = stmt.query(params_from_iter(tool_params.iter()))?;
                while let Some(row) = rows.next()? {
                    let model = row.get::<_, String>(0)?;
                    models
                        .entry(model.clone())
                        .or_insert_with(|| empty_model_metrics(model))
                        .tool_calls = row.get::<_, i64>(1)? as u64;
                }
            }

            let mut models = models.into_values().collect::<Vec<_>>();
            models.sort_by(|left, right| {
                right
                    .tokens
                    .total_tokens
                    .cmp(&left.tokens.total_tokens)
                    .then_with(|| left.model.cmp(&right.model))
            });
            Ok(models)
        })
        .await
    }

    async fn sessions(&self, filter: SessionMetricsFilter) -> MetricsResult<Vec<SessionMetrics>> {
        self.with_connection(move |connection| {
            let model = normalize_filter_value(filter.model);
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
            if let Some(model) = model {
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
                "SELECT session_id, model, started_at, completed_at, total_rounds, prompt_tokens, completion_tokens, total_tokens, tool_call_count, prompt_cached_tool_outputs, prompt_cached_tool_tokens_saved, total_compression_events, total_tokens_saved, status, message_count FROM session_metrics {} ORDER BY started_at DESC LIMIT {}",
                where_sql, limit
            );

            let mut stmt = connection.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
            let mut sessions = Vec::new();

            while let Some(row) = rows.next()? {
                let session_id: String = row.get(0)?;
                let started_at = parse_timestamp(row.get::<_, String>(2)?)?;
                let completed_at = parse_optional_timestamp(row.get::<_, Option<String>>(3)?)?;
                let status_raw: String = row.get(13)?;
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
                    prompt_cached_tool_outputs: row.get::<_, i64>(9)? as u64,
                    prompt_cached_tool_tokens_saved: row.get::<_, i64>(10)? as u64,
                    total_compression_events: row.get::<_, i64>(11)? as u64,
                    total_tokens_saved: row.get::<_, i64>(12)? as u64,
                    tool_breakdown,
                    status,
                    message_count: row.get::<_, i64>(14)? as u32,
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
            let session_sql = "SELECT session_id, model, started_at, completed_at, total_rounds, prompt_tokens, completion_tokens, total_tokens, tool_call_count, prompt_cached_tool_outputs, prompt_cached_tool_tokens_saved, total_compression_events, total_tokens_saved, status, message_count FROM session_metrics WHERE session_id = ?1";
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
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, i64>(12)?,
                        row.get::<_, String>(13)?,
                        row.get::<_, i64>(14)?,
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
                prompt_cached_tool_outputs,
                prompt_cached_tool_tokens_saved,
                total_compression_events,
                total_tokens_saved,
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
                prompt_cached_tool_outputs: prompt_cached_tool_outputs as u64,
                prompt_cached_tool_tokens_saved: prompt_cached_tool_tokens_saved as u64,
                total_compression_events: total_compression_events as u64,
                total_tokens_saved: total_tokens_saved as u64,
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

    async fn increment_execute_sync_mismatch(
        &self,
        reason: &str,
        occurred_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        let reason = reason.to_string();
        let mismatch_date = occurred_at.date_naive().to_string();
        let updated_at = format_timestamp(occurred_at);

        self.with_connection(move |connection| {
            connection.execute(
                r#"
                INSERT INTO execute_sync_mismatch_metrics (reason, mismatch_date, count, updated_at)
                VALUES (?1, ?2, 1, ?3)
                ON CONFLICT(reason, mismatch_date) DO UPDATE SET
                    count = count + 1,
                    updated_at = excluded.updated_at
                "#,
                params![reason, mismatch_date, updated_at],
            )?;
            Ok(())
        })
        .await
    }

    async fn daily_metrics(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        self.daily_metrics_for_model(days, end_date, None).await
    }

    async fn daily_metrics_for_model(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        model: Option<String>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        let end_date = end_date.unwrap_or_else(|| Utc::now().date_naive());
        let span = days.max(1) - 1;
        let start_date = end_date - chrono::Duration::days(i64::from(span));
        let model = normalize_filter_value(model);

        self.with_connection(move |connection| {
            let start_bound = start_date.to_string();
            let end_bound = next_day_bound(end_date);

            let mut sessions_by_day =
                load_session_counts_by_day(connection, &start_bound, &end_bound, model.as_deref())?;
            let mut rounds_by_day = load_round_aggregates_by_day(
                connection,
                &start_bound,
                &end_bound,
                model.as_deref(),
            )?;
            let mut model_by_day = load_model_breakdown_by_day(
                connection,
                &start_bound,
                &end_bound,
                model.as_deref(),
            )?;
            let mut tool_by_day =
                load_tool_breakdown_by_day(connection, &start_bound, &end_bound, model.as_deref())?;

            // A continuation day may have rounds or tools but no new session.
            // Build the output keyset from all three occurrence dimensions so
            // those later-day rows are never dropped by a session-only driver.
            let mut dates = BTreeSet::new();
            dates.extend(sessions_by_day.keys().copied());
            dates.extend(rounds_by_day.keys().copied());
            dates.extend(tool_by_day.keys().copied());

            let mut result = Vec::with_capacity(dates.len());
            for date in dates {
                let round = rounds_by_day.remove(&date).unwrap_or_default();
                let model_breakdown = model_by_day.remove(&date).unwrap_or_default();
                let tool_breakdown = tool_by_day.remove(&date).unwrap_or_default();
                let total_tool_calls = tool_breakdown
                    .values()
                    .copied()
                    .fold(0u32, u32::saturating_add);

                result.push(DailyMetrics {
                    date,
                    total_sessions: sessions_by_day.remove(&date).unwrap_or(0),
                    total_rounds: round.total_rounds,
                    total_token_usage: round.token_usage,
                    total_tool_calls,
                    prompt_cached_tool_outputs: round.prompt_cached_tool_outputs,
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

            // Capture-then-delete must be atomic w.r.t. other writers: without a
            // write lock, a round with `started_at < cutoff` inserted between the
            // SELECT and the DELETE (backfill/replay/clock skew) would be deleted
            // yet miss re-aggregation, leaving session_metrics overcounting. Wrap
            // the whole select→delete→refresh in a single IMMEDIATE transaction.
            with_immediate_transaction(connection, || {
                // Only sessions that actually lose rounds need re-aggregation
                // (was nine correlated subqueries × ALL sessions per pass).
                let affected_sessions: Vec<String> = {
                    let mut stmt = connection.prepare(
                        "SELECT DISTINCT session_id FROM round_metrics WHERE started_at < ?1",
                    )?;
                    let ids = stmt
                        .query_map(params![cutoff_str], |row| row.get(0))?
                        .collect::<Result<Vec<String>, _>>()?;
                    ids
                };

                let deleted = connection.execute(
                    "DELETE FROM round_metrics WHERE started_at < ?1",
                    params![cutoff_str],
                )?;

                for session_id in affected_sessions {
                    refresh_session_aggregates(connection, &session_id, Utc::now())?;
                }

                Ok(deleted as u64)
            })
        })
        .await
    }

    async fn reconcile_stale_executions(
        &self,
        active_session_ids: &[String],
        awaiting_response_session_ids: &[String],
    ) -> MetricsResult<()> {
        let active_session_ids = active_session_ids.to_vec();
        let awaiting_response_session_ids = awaiting_response_session_ids.to_vec();

        self.with_connection(move |connection| {
            with_immediate_transaction(connection, || {
                let reconciled_at = Utc::now();
                let reconciled_at_str = format_timestamp(reconciled_at);

                let running_session_ids: Vec<String> = {
                    let mut stmt = connection.prepare(
                        "SELECT session_id FROM session_metrics WHERE status = 'running'",
                    )?;
                    let ids = stmt
                        .query_map([], |row| row.get(0))?
                        .collect::<Result<Vec<String>, _>>()?;
                    ids
                };

                for session_id in running_session_ids {
                    if active_session_ids.iter().any(|id| id == &session_id) {
                        continue;
                    }

                    let status = if awaiting_response_session_ids
                        .iter()
                        .any(|id| id == &session_id)
                    {
                        SessionStatus::AwaitingResponse
                    } else {
                        SessionStatus::Completed
                    };

                    connection.execute(
                        "UPDATE session_metrics SET status = ?1, completed_at = COALESCE(completed_at, ?2), updated_at = ?2 WHERE session_id = ?3",
                        params![status.as_str(), reconciled_at_str, session_id],
                    )?;
                    refresh_session_aggregates(connection, &session_id, reconciled_at)?;
                }

                connection.execute(
                    "UPDATE round_metrics SET status = 'error', completed_at = COALESCE(completed_at, ?1), error = COALESCE(error, 'reconciled_stale_round') WHERE status = 'running'",
                    params![reconciled_at_str],
                )?;
                connection.execute(
                    "UPDATE forward_request_metrics SET status = 'error', completed_at = COALESCE(completed_at, ?1), error = COALESCE(error, 'reconciled_stale_forward'), updated_at = ?1 WHERE status = 'pending' AND completed_at IS NULL",
                    params![reconciled_at_str],
                )?;

                Ok(())
            })
        })
        .await
    }
}

/// Opens a connection to the SQLite database with proper configuration.
///
/// This function:
/// 1. Creates parent directories if they don't exist
/// 2. Opens the database file (creates if doesn't exist)
/// 3. Configures optimal SQLite settings:
///    - WAL mode for concurrent access
///    - Foreign key enforcement
///    - Normal synchronous mode for performance
///
/// # Arguments
///
/// * `path` - Path to the SQLite database file
///
/// # Errors
///
/// Returns an error if:
/// - Parent directories cannot be created
/// - Database file cannot be opened
/// - PRAGMA settings fail to apply
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
        -- Block-and-retry for up to 5s instead of failing a contended writer
        -- immediately with SQLITE_BUSY (SQLite's default busy_timeout is 0).
        -- Matters since prune_rounds_before holds the write lock across a
        -- BEGIN IMMEDIATE transaction while live inserts may also write. #357.
        PRAGMA busy_timeout = 5000;
        "#,
    )?;
    Ok(connection)
}

/// Formats a timestamp as RFC3339 string for database storage.
///
/// # Arguments
///
/// * `timestamp` - DateTime to format
///
/// # Returns
///
/// RFC3339 formatted string (e.g., "2026-02-24T12:34:56.789+00:00")
fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339()
}

/// Parses an RFC3339 timestamp string from the database.
///
/// # Arguments
///
/// * `raw` - RFC3339 formatted string
///
/// # Errors
///
/// Returns an error if the string doesn't conform to RFC3339 format.
fn parse_timestamp(raw: String) -> MetricsResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&raw)?.with_timezone(&Utc))
}

/// Parses an optional RFC3339 timestamp string.
///
/// # Arguments
///
/// * `raw` - Optional RFC3339 formatted string
///
/// # Returns
///
/// Returns `Ok(None)` if the input is None, otherwise parses the timestamp.
fn parse_optional_timestamp(raw: Option<String>) -> MetricsResult<Option<DateTime<Utc>>> {
    raw.map(parse_timestamp).transpose()
}

/// Computes the duration in milliseconds between two timestamps.
///
/// # Arguments
///
/// * `started_at` - Start timestamp
/// * `completed_at` - Optional end timestamp
///
/// # Returns
///
/// Returns `None` if `completed_at` is None, otherwise returns the duration in milliseconds.
/// Returns `None` if the duration is negative or too large to fit in u64.
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

/// Builds a SQL WHERE clause for session metrics queries.
///
/// Constructs a WHERE clause based on the provided filter criteria,
/// appending parameters to the params vector in the correct order.
///
/// # Arguments
///
/// * `start_date` - Optional start date filter (inclusive)
/// * `end_date` - Optional end date filter (inclusive)
/// * `required_status` - Optional status filter (e.g., "running", "completed")
/// * `params_vec` - Vector to append SQL parameters to
///
/// # Returns
///
/// Returns an empty string if no filters are applied, otherwise returns
/// a WHERE clause starting with "WHERE ".
/// Half-open upper bound for an inclusive end date: the start of the following
/// day as a `YYYY-MM-DD` string. `started_at < next_day_bound(end)` keeps the
/// whole end date while letting the query seek the index (no `date()` wrap).
/// Saturates at `NaiveDate::MAX` (unreachable in practice).
fn next_day_bound(end: NaiveDate) -> String {
    end.succ_opt().unwrap_or(end).to_string()
}

fn normalize_filter_value(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Builds a half-open UTC date range over an internal `started_at` column.
///
/// Round attribution deliberately uses `round_metrics.started_at`: it is the
/// non-null occurrence timestamp shared by completed and running rows. Tool
/// attribution likewise uses the tool row's own `started_at`. The column name
/// is always a static, code-owned SQL identifier, never user input.
fn build_started_at_where_clause(
    started_at_column: &'static str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_date {
        conditions.push(format!("{started_at_column} >= ?"));
        params_vec.push(start.to_string());
    }

    if let Some(end) = end_date {
        conditions.push(format!("{started_at_column} < ?"));
        params_vec.push(next_day_bound(end));
    }

    if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    }
}

fn build_started_at_model_where_clause(
    started_at_column: &'static str,
    model_column: &'static str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    model: Option<&str>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut where_clause =
        build_started_at_where_clause(started_at_column, start_date, end_date, params_vec);
    if let Some(model) = model {
        if where_clause.is_empty() {
            where_clause = format!("WHERE {model_column} = ?");
        } else {
            where_clause.push_str(&format!(" AND {model_column} = ?"));
        }
        params_vec.push(model.to_string());
    }
    where_clause
}

fn build_session_where_clause(
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    required_status: Option<&str>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_date {
        // Sargable: bare comparison on the RFC3339-lexicographic, indexed column
        // seeks idx_session_started_at; a `date()` wrap would force a full scan.
        conditions.push("started_at >= ?".to_string());
        params_vec.push(start.to_string());
    }

    if let Some(end) = end_date {
        // Inclusive of the whole end date → half-open upper bound at the next day.
        conditions.push("started_at < ?".to_string());
        params_vec.push(next_day_bound(end));
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

fn build_session_model_where_clause(
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    required_status: Option<&str>,
    model: Option<&str>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut where_clause =
        build_session_where_clause(start_date, end_date, required_status, params_vec);
    if let Some(model) = model {
        if where_clause.is_empty() {
            where_clause = "WHERE model = ?".to_string();
        } else {
            where_clause.push_str(" AND model = ?");
        }
        params_vec.push(model.to_string());
    }
    where_clause
}

fn empty_model_metrics(model: String) -> ModelMetrics {
    ModelMetrics {
        model,
        sessions: 0,
        rounds: 0,
        tokens: TokenUsage::default(),
        tool_calls: 0,
        prompt_cached_tool_outputs: 0,
    }
}

/// Builds a SQL WHERE clause for forward request metrics queries.
///
/// Constructs a WHERE clause based on the provided filter criteria,
/// appending parameters to the params vector in the correct order.
///
/// # Arguments
///
/// * `start_date` - Optional start date filter (inclusive)
/// * `end_date` - Optional end date filter (inclusive)
/// * `endpoint` - Optional endpoint filter
/// * `model` - Optional model filter
/// * `params_vec` - Vector to append SQL parameters to
///
/// # Returns
///
/// Returns an empty string if no filters are applied, otherwise returns
/// a WHERE clause starting with "WHERE ".
fn build_forward_where_clause(
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    endpoint: Option<&str>,
    model: Option<&str>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_date {
        // Sargable range on the indexed RFC3339 column (see build_session_where_clause).
        conditions.push("started_at >= ?".to_string());
        params_vec.push(start.to_string());
    }

    if let Some(end) = end_date {
        conditions.push("started_at < ?".to_string());
        params_vec.push(next_day_bound(end));
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

fn build_execute_sync_mismatch_where_clause(
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    reason: Option<&str>,
    params_vec: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_date {
        conditions.push("date(mismatch_date) >= date(?)".to_string());
        params_vec.push(start.to_string());
    }

    if let Some(end) = end_date {
        conditions.push("date(mismatch_date) <= date(?)".to_string());
        params_vec.push(end.to_string());
    }

    if let Some(reason) = reason {
        conditions.push("reason = ?".to_string());
        params_vec.push(reason.to_string());
    }

    if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    }
}

fn ensure_integer_column(
    connection: &Connection,
    table: &str,
    column: &str,
    default_value: i64,
) -> MetricsResult<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = connection.prepare(&pragma)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(());
        }
    }

    let alter =
        format!("ALTER TABLE {table} ADD COLUMN {column} INTEGER NOT NULL DEFAULT {default_value}");
    connection.execute(&alter, [])?;
    Ok(())
}

fn ensure_nullable_integer_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> MetricsResult<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = connection.prepare(&pragma)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(());
        }
    }

    let alter = format!("ALTER TABLE {table} ADD COLUMN {column} INTEGER");
    connection.execute(&alter, [])?;
    Ok(())
}

#[cfg(test)]
struct SessionTokenFoldPause {
    session_id: String,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static SESSION_TOKEN_FOLD_PAUSE: std::sync::OnceLock<
    std::sync::Mutex<Option<SessionTokenFoldPause>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn install_session_token_fold_pause(
    session_id: &str,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut slot = SESSION_TOKEN_FOLD_PAUSE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("session token fold test hook lock");
    assert!(slot.is_none(), "session token fold test hook already set");
    *slot = Some(SessionTokenFoldPause {
        session_id: session_id.to_string(),
        entered: entered_tx,
        release: release_rx,
    });
    (entered_rx, release_tx)
}

#[cfg(test)]
fn pause_after_session_token_fold(session_id: &str) {
    let pause = {
        let mut slot = SESSION_TOKEN_FOLD_PAUSE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("session token fold test hook lock");
        if slot
            .as_ref()
            .is_some_and(|pause| pause.session_id == session_id)
        {
            slot.take()
        } else {
            None
        }
    };

    if let Some(pause) = pause {
        let _ = pause.entered.send(());
        let _ = pause.release.recv();
    }
}

#[cfg(test)]
struct CompleteRoundTransactionEnteredHook {
    round_id: String,
    entered: std::sync::mpsc::Sender<()>,
}

#[cfg(test)]
static COMPLETE_ROUND_TRANSACTION_ENTERED_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<CompleteRoundTransactionEnteredHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn install_complete_round_transaction_entered_hook(
    round_id: &str,
) -> std::sync::mpsc::Receiver<()> {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let mut slot = COMPLETE_ROUND_TRANSACTION_ENTERED_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("complete round transaction test hook lock");
    assert!(
        slot.is_none(),
        "complete round transaction test hook already set"
    );
    *slot = Some(CompleteRoundTransactionEnteredHook {
        round_id: round_id.to_string(),
        entered: entered_tx,
    });
    entered_rx
}

#[cfg(test)]
fn signal_complete_round_transaction_entered(round_id: &str) {
    let hook = {
        let mut slot = COMPLETE_ROUND_TRANSACTION_ENTERED_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("complete round transaction test hook lock");
        if slot.as_ref().is_some_and(|hook| hook.round_id == round_id) {
            slot.take()
        } else {
            None
        }
    };

    if let Some(hook) = hook {
        let _ = hook.entered.send(());
    }
}

/// Runs a metrics mutation under the SQLite write lock unless the caller
/// already owns a transaction.
///
/// Aggregate refreshes read child rows and then update their cached parent
/// row. `BEGIN IMMEDIATE` serializes that read-modify-write sequence across
/// independent connections, while the autocommit check lets larger operations
/// such as pruning reuse their existing transaction.
fn with_immediate_transaction<T>(
    connection: &Connection,
    operation: impl FnOnce() -> MetricsResult<T>,
) -> MetricsResult<T> {
    if !connection.is_autocommit() {
        return operation();
    }

    connection.execute_batch("BEGIN IMMEDIATE")?;
    match operation() {
        Ok(value) => match connection.execute_batch("COMMIT") {
            Ok(()) => Ok(value),
            Err(error) => {
                let _ = connection.execute_batch("ROLLBACK");
                Err(error.into())
            }
        },
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// Refreshes aggregated metrics for a session by recalculating from child entities.
///
/// This function updates the session's aggregate columns by summing values
/// from all associated rounds and counting tool calls. It should be called
/// whenever a round or tool call is added or modified.
///
/// # Updated Columns
///
/// - `total_rounds`: Count of rounds in the session
/// - `prompt_tokens`: Sum of prompt tokens from all rounds
/// - `completion_tokens`: Sum of completion tokens from all rounds
/// - `total_tokens`: Sum of total tokens from all rounds
/// - `prompt_cached_tool_outputs`: Sum of prompt-side cached tool outputs from all rounds
/// - `total_compression_events`: Sum of compression events from all rounds
/// - `total_tokens_saved`: Sum of tokens saved by compression from all rounds
/// - `tool_call_count`: Count of tool calls in the session
/// - `updated_at`: Timestamp of this update
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `session_id` - Session to refresh
/// * `updated_at` - Timestamp for the updated_at column
///
/// # Errors
///
/// Returns an error if the SQL execution fails.
fn refresh_session_aggregates(
    connection: &Connection,
    session_id: &str,
    updated_at: DateTime<Utc>,
) -> MetricsResult<()> {
    with_immediate_transaction(connection, || {
        refresh_session_aggregates_in_transaction(connection, session_id, updated_at)
    })
}

fn refresh_session_aggregates_in_transaction(
    connection: &Connection,
    session_id: &str,
    updated_at: DateTime<Utc>,
) -> MetricsResult<()> {
    // Do not use SQLite SUM for token counters: summing otherwise-valid i64
    // round rows can overflow before an outer MIN/CASE can clamp it. Fold in
    // Rust with the same signed-64 saturation policy used by the runtime.
    let token_usage = load_session_token_aggregate(connection, session_id)?;
    #[cfg(test)]
    pause_after_session_token_fold(session_id);
    let updated_at = format_timestamp(updated_at);
    connection.execute(
        r#"
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
        "#,
        params![
            session_id,
            durable_token_to_i64(token_usage.prompt_tokens),
            durable_token_to_i64(token_usage.completion_tokens),
            durable_token_to_i64(token_usage.total_tokens),
            updated_at,
        ],
    )?;
    Ok(())
}

fn durable_token_to_i64(value: u64) -> i64 {
    i64::try_from(value.min(bamboo_domain::MAX_DURABLE_TOKEN_COUNT))
        .expect("durable token count is clamped to i64::MAX")
}

fn durable_token_from_i64(column: &str, value: i64) -> MetricsResult<u64> {
    u64::try_from(value).map_err(|_| {
        MetricsError::InvalidData(format!(
            "negative durable token counter in {column}: {value}"
        ))
    })
}

fn load_session_token_aggregate(
    connection: &Connection,
    session_id: &str,
) -> MetricsResult<TokenUsage> {
    let mut stmt = connection.prepare(
        "SELECT prompt_tokens, completion_tokens, total_tokens FROM round_metrics WHERE session_id = ?1",
    )?;
    let mut rows = stmt.query(params![session_id])?;
    let mut aggregate = TokenUsage::default();
    while let Some(row) = rows.next()? {
        let prompt = durable_token_from_i64("round_metrics.prompt_tokens", row.get(0)?)?;
        let completion = durable_token_from_i64("round_metrics.completion_tokens", row.get(1)?)?;
        let total = durable_token_from_i64("round_metrics.total_tokens", row.get(2)?)?;
        aggregate.add_assign_durable(TokenUsage {
            prompt_tokens: prompt,
            completion_tokens: completion,
            total_tokens: total,
        });
    }
    Ok(aggregate)
}

/// Loads tool call breakdown (tool_name -> count) for a session.
///
/// Retrieves the count of tool invocations grouped by tool name
/// for the specified session.
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `session_id` - Session to get tool breakdown for
///
/// # Returns
///
/// A HashMap mapping tool names to their invocation counts.
///
/// # Example
///
/// ```rust,ignore
/// let breakdown = load_tool_breakdown(&conn, "session-123")?;
/// // breakdown might be: {"read_file": 5, "execute_command": 2}
/// ```
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

/// Loads all rounds for a session with their associated tool calls.
///
/// Retrieves complete round metrics including token usage, status,
/// and all tool calls made during each round.
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `session_id` - Session to get rounds for
///
/// # Returns
///
/// A vector of RoundMetrics ordered by started_at ascending.
///
/// # Errors
///
/// Returns an error if:
/// - SQL execution fails
/// - Timestamp parsing fails
/// - Status values are invalid
fn load_rounds(connection: &Connection, session_id: &str) -> MetricsResult<Vec<RoundMetrics>> {
    let mut stmt = connection.prepare(LOAD_ROUNDS_SQL)?;
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
            prompt_cached_tool_outputs: row.get::<_, i64>(9)? as u32,
            prompt_cached_tool_tokens_saved: row.get::<_, i64>(10)? as u32,
            compression_count: row.get::<_, i64>(11)? as u32,
            tokens_saved: row.get::<_, i64>(12)? as u32,
            error: row.get(13)?,
            duration_ms: compute_duration_ms(started_at, completed_at),
        });
    }

    Ok(rounds)
}

/// Loads all tool calls for a specific round.
///
/// Retrieves tool invocation details including timing, success status,
/// and error information.
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `round_id` - Round to get tool calls for
///
/// # Returns
///
/// A vector of ToolCallMetrics ordered by started_at ascending.
fn load_tool_calls(connection: &Connection, round_id: &str) -> MetricsResult<Vec<ToolCallMetrics>> {
    let mut stmt = connection.prepare(LOAD_TOOL_CALLS_SQL)?;
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

#[derive(Debug, Default)]
struct DailyRoundAggregate {
    total_rounds: u32,
    token_usage: TokenUsage,
    prompt_cached_tool_outputs: u64,
}

/// Loads new-session counts keyed by the session row's UTC start date.
fn load_session_counts_by_day(
    connection: &Connection,
    start_bound: &str,
    end_bound: &str,
    model: Option<&str>,
) -> MetricsResult<HashMap<NaiveDate, u32>> {
    let model_clause = if model.is_some() {
        " AND model = ?3"
    } else {
        ""
    };
    let sql = format!(
        r#"
        SELECT date(started_at) AS date_key, COUNT(*)
        FROM session_metrics
        WHERE started_at >= ?1 AND started_at < ?2{}
        GROUP BY date_key
        "#,
        model_clause
    );
    let mut params_vec = vec![start_bound.to_string(), end_bound.to_string()];
    if let Some(model) = model {
        params_vec.push(model.to_string());
    }
    let mut stmt = connection.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
    let mut by_day = HashMap::new();

    while let Some(row) = rows.next()? {
        let date = NaiveDate::parse_from_str(&row.get::<_, String>(0)?, "%Y-%m-%d")?;
        by_day.insert(date, row.get::<_, i64>(1)? as u32);
    }

    Ok(by_day)
}

/// Loads round usage keyed by the round row's UTC start date.
///
/// `started_at` is present for both completed and running rows. A running row
/// therefore contributes one round (and its currently persisted, usually-zero
/// usage) without requiring a nullable completion timestamp.
fn load_round_aggregates_by_day(
    connection: &Connection,
    start_bound: &str,
    end_bound: &str,
    model: Option<&str>,
) -> MetricsResult<HashMap<NaiveDate, DailyRoundAggregate>> {
    let model_clause = if model.is_some() {
        " AND model = ?3"
    } else {
        ""
    };
    let sql = format!(
        r#"
        SELECT date(started_at) AS date_key,
               COUNT(*),
               COALESCE(SUM(prompt_tokens), 0),
               COALESCE(SUM(completion_tokens), 0),
               COALESCE(SUM(total_tokens), 0),
               COALESCE(SUM(prompt_cached_tool_outputs), 0)
        FROM round_metrics
        WHERE started_at >= ?1 AND started_at < ?2{}
        GROUP BY date_key
        "#,
        model_clause
    );
    let mut params_vec = vec![start_bound.to_string(), end_bound.to_string()];
    if let Some(model) = model {
        params_vec.push(model.to_string());
    }
    let mut stmt = connection.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
    let mut by_day = HashMap::new();

    while let Some(row) = rows.next()? {
        let date = NaiveDate::parse_from_str(&row.get::<_, String>(0)?, "%Y-%m-%d")?;
        by_day.insert(
            date,
            DailyRoundAggregate {
                total_rounds: row.get::<_, i64>(1)? as u32,
                token_usage: TokenUsage {
                    prompt_tokens: row.get::<_, i64>(2)? as u64,
                    completion_tokens: row.get::<_, i64>(3)? as u64,
                    total_tokens: row.get::<_, i64>(4)? as u64,
                },
                prompt_cached_tool_outputs: row.get::<_, i64>(5)? as u64,
            },
        );
    }

    Ok(by_day)
}

/// Loads per-model token-usage breakdown for a whole date range in ONE grouped
/// query (`GROUP BY date_key, model`), returning it keyed by day so callers can
/// look up each day without a per-day query (avoids the daily-metrics N+1).
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `start_bound` / `end_bound` - Half-open `started_at` range (`>= start_bound
///   AND < end_bound`), the same sargable bounds used by the caller.
///
/// # Returns
///
/// `date -> (model -> total token usage)` for every day with rounds in range.
fn load_model_breakdown_by_day(
    connection: &Connection,
    start_bound: &str,
    end_bound: &str,
    model: Option<&str>,
) -> MetricsResult<HashMap<NaiveDate, HashMap<String, TokenUsage>>> {
    let model_clause = if model.is_some() {
        " AND model = ?3"
    } else {
        ""
    };
    let sql = format!(
        r#"
        SELECT date(started_at) AS date_key,
               model,
               COALESCE(SUM(prompt_tokens), 0),
               COALESCE(SUM(completion_tokens), 0),
               COALESCE(SUM(total_tokens), 0)
        FROM round_metrics
        WHERE started_at >= ?1 AND started_at < ?2{}
        GROUP BY date_key, model
        "#,
        model_clause
    );

    let mut params_vec = vec![start_bound.to_string(), end_bound.to_string()];
    if let Some(model) = model {
        params_vec.push(model.to_string());
    }
    let mut stmt = connection.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
    let mut by_day: HashMap<NaiveDate, HashMap<String, TokenUsage>> = HashMap::new();

    while let Some(row) = rows.next()? {
        let date = NaiveDate::parse_from_str(&row.get::<_, String>(0)?, "%Y-%m-%d")?;
        by_day.entry(date).or_default().insert(
            row.get::<_, String>(1)?,
            TokenUsage {
                prompt_tokens: row.get::<_, i64>(2)? as u64,
                completion_tokens: row.get::<_, i64>(3)? as u64,
                total_tokens: row.get::<_, i64>(4)? as u64,
            },
        );
    }

    Ok(by_day)
}

/// Loads per-tool invocation counts for a whole date range in ONE grouped query
/// (`GROUP BY date_key, tool_name`), keyed by day — the tool-call analogue of
/// [`load_model_breakdown_by_day`], used to avoid the daily-metrics N+1.
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `start_bound` / `end_bound` - Half-open `started_at` range.
///
/// # Returns
///
/// `date -> (tool name -> invocation count)` for every day with tool calls in range.
fn load_tool_breakdown_by_day(
    connection: &Connection,
    start_bound: &str,
    end_bound: &str,
    model: Option<&str>,
) -> MetricsResult<HashMap<NaiveDate, HashMap<String, u32>>> {
    let model_clause = if model.is_some() {
        " AND round_metrics.model = ?3"
    } else {
        ""
    };
    let sql = format!(
        r#"
        SELECT date(tool_call_metrics.started_at) AS date_key, tool_call_metrics.tool_name, COUNT(*)
        FROM tool_call_metrics
        JOIN round_metrics ON round_metrics.round_id = tool_call_metrics.round_id
        WHERE tool_call_metrics.started_at >= ?1 AND tool_call_metrics.started_at < ?2{}
        GROUP BY date_key, tool_name
        "#,
        model_clause
    );

    let mut params_vec = vec![start_bound.to_string(), end_bound.to_string()];
    if let Some(model) = model {
        params_vec.push(model.to_string());
    }
    let mut stmt = connection.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
    let mut by_day: HashMap<NaiveDate, HashMap<String, u32>> = HashMap::new();

    while let Some(row) = rows.next()? {
        let date = NaiveDate::parse_from_str(&row.get::<_, String>(0)?, "%Y-%m-%d")?;
        by_day
            .entry(date)
            .or_default()
            .insert(row.get::<_, String>(1)?, row.get::<_, i64>(2)? as u32);
    }

    Ok(by_day)
}

fn load_execute_sync_mismatch_breakdown(
    connection: &Connection,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> MetricsResult<HashMap<String, u64>> {
    let mut params_vec = Vec::new();
    let where_clause =
        build_execute_sync_mismatch_where_clause(start_date, end_date, None, &mut params_vec);
    let sql = format!(
        "SELECT reason, COALESCE(SUM(count), 0) FROM execute_sync_mismatch_metrics {} GROUP BY reason ORDER BY reason ASC",
        where_clause
    );
    let mut stmt = connection.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
    let mut breakdown = HashMap::new();

    while let Some(row) = rows.next()? {
        breakdown.insert(row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64);
    }

    Ok(breakdown)
}

const LOAD_ROUNDS_SQL: &str = "SELECT round_id, session_id, model, started_at, completed_at, status, prompt_tokens, completion_tokens, total_tokens, prompt_cached_tool_outputs, prompt_cached_tool_tokens_saved, compression_count, tokens_saved, error FROM round_metrics WHERE session_id = ?1 ORDER BY started_at ASC";

const LOAD_TOOL_CALLS_SQL: &str = "SELECT tool_call_id, tool_name, started_at, completed_at, success, error FROM tool_call_metrics WHERE round_id = ?1 ORDER BY started_at ASC";

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{NaiveDate, TimeZone, Utc};
    use tempfile::tempdir;

    use super::{MetricsStorage, SqliteMetricsStorage, ToolCallCompletion};
    use crate::metrics::types::{
        ForwardMetricsFilter, ForwardStatus, ForwardTokenDetails, MetricsDateFilter,
        ModelMetricsDateFilter, RoundStatus, SessionMetricsFilter, SessionStatus, TokenUsage,
    };

    // The pre-#1075 layout is independent of init() so this exercises a real
    // populated database migration rather than deriving the old schema from it.
    const LEGACY_ROUND_SCHEMA: &str = r#"
        CREATE TABLE session_metrics (
            session_id TEXT PRIMARY KEY, model TEXT NOT NULL, started_at TEXT NOT NULL,
            completed_at TEXT, status TEXT NOT NULL DEFAULT 'running',
            total_rounds INTEGER NOT NULL DEFAULT 0, prompt_tokens INTEGER NOT NULL DEFAULT 0,
            completion_tokens INTEGER NOT NULL DEFAULT 0, total_tokens INTEGER NOT NULL DEFAULT 0,
            prompt_cached_tool_outputs INTEGER NOT NULL DEFAULT 0,
            prompt_cached_tool_tokens_saved INTEGER NOT NULL DEFAULT 0,
            total_compression_events INTEGER NOT NULL DEFAULT 0,
            total_tokens_saved INTEGER NOT NULL DEFAULT 0, tool_call_count INTEGER NOT NULL DEFAULT 0,
            message_count INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL
        );
        CREATE TABLE round_metrics (
            round_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, model TEXT NOT NULL,
            started_at TEXT NOT NULL, completed_at TEXT, status TEXT NOT NULL DEFAULT 'running',
            prompt_tokens INTEGER NOT NULL DEFAULT 0, completion_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens INTEGER NOT NULL DEFAULT 0, prompt_cached_tool_outputs INTEGER NOT NULL DEFAULT 0,
            prompt_cached_tool_tokens_saved INTEGER NOT NULL DEFAULT 0,
            compression_count INTEGER NOT NULL DEFAULT 0, tokens_saved INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            FOREIGN KEY(session_id) REFERENCES session_metrics(session_id) ON DELETE CASCADE
        );
        CREATE TABLE tool_call_metrics (
            tool_call_id TEXT PRIMARY KEY, round_id TEXT NOT NULL, session_id TEXT NOT NULL,
            tool_name TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT,
            success INTEGER, error TEXT,
            FOREIGN KEY(round_id) REFERENCES round_metrics(round_id) ON DELETE CASCADE,
            FOREIGN KEY(session_id) REFERENCES session_metrics(session_id) ON DELETE CASCADE
        );
        CREATE INDEX idx_session_started_at ON session_metrics(started_at);
        CREATE INDEX idx_session_model ON session_metrics(model);
        CREATE INDEX idx_round_session ON round_metrics(session_id);
        CREATE INDEX idx_round_started_at ON round_metrics(started_at);
        CREATE INDEX idx_tool_session ON tool_call_metrics(session_id);
        CREATE INDEX idx_tool_started_at ON tool_call_metrics(started_at);
        CREATE INDEX idx_tool_name ON tool_call_metrics(tool_name);
        CREATE INDEX custom_tool_success ON tool_call_metrics(success);
    "#;

    fn seed_round_index_fixture(connection: &rusqlite::Connection) {
        let now = Utc.with_ymd_and_hms(2026, 2, 10, 12, 0, 0).unwrap();
        super::with_immediate_transaction(connection, || {
            // Target plus 64 unrelated sessions, 195 rounds and 585 tools.
            // Reverse insertion order ensures detail ordering comes from SQL.
            for session in 0..65 {
                let sid = format!("index-session-{session}");
                connection.execute(
                    "INSERT INTO session_metrics(session_id,model,started_at,message_count,updated_at) VALUES (?1,'model',?2,7,?2)",
                    rusqlite::params![sid, super::format_timestamp(now)],
                )?;
                for round in (0..3).rev() {
                    let rid = format!("index-round-{session}-{round}");
                    let started = if session == 0 && round == 0 {
                        now - chrono::Duration::days(40)
                    } else {
                        now + chrono::Duration::minutes(round)
                    };
                    connection.execute(
                        "INSERT INTO round_metrics(round_id,session_id,model,started_at,completed_at,status,prompt_tokens,completion_tokens,total_tokens,prompt_cached_tool_outputs,prompt_cached_tool_tokens_saved,tokens_saved) VALUES (?1,?2,'model',?3,?3,'success',10,2,12,1,3,3)",
                        rusqlite::params![rid, sid, super::format_timestamp(started)],
                    )?;
                    for tool in (0..3).rev() {
                        connection.execute(
                            "INSERT INTO tool_call_metrics(tool_call_id,round_id,session_id,tool_name,started_at,completed_at,success) VALUES (?1,?2,?3,'fixture_tool',?4,?4,1)",
                            rusqlite::params![format!("index-tool-{session}-{round}-{tool}"), rid, sid, super::format_timestamp(started + chrono::Duration::seconds(tool))],
                        )?;
                    }
                }
                super::refresh_session_aggregates(connection, &sid, now)?;
            }
            Ok(())
        }).expect("seed populated metrics fixture");
        connection
            .execute_batch("ANALYZE")
            .expect("analyze fixture");
    }

    fn assert_round_lookup_plans(connection: &rusqlite::Connection) {
        for (query, key, table, column) in [
            (
                super::LOAD_TOOL_CALLS_SQL,
                "index-round-0-1",
                "tool_call_metrics",
                "round_id",
            ),
            (
                super::LOAD_ROUNDS_SQL,
                "index-session-0",
                "round_metrics",
                "session_id",
            ),
            (
                "DELETE FROM round_metrics WHERE round_id = ?1",
                "index-round-0-1",
                "tool_call_metrics",
                "round_id",
            ),
        ] {
            let mut statement = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {query}"))
                .unwrap();
            let plan = statement
                .query_map([key], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                plan.iter().any(|step| {
                    step.starts_with(&format!("SEARCH {table} "))
                        && step.contains(&format!("({column}=?"))
                }),
                "lookup must be keyed by {column}: {plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|step| step.starts_with(&format!("SCAN {table}"))),
                "lookup must not scan unrelated {table} rows: {plan:?}"
            );
            assert!(
                !plan.iter().any(|step| step.contains("TEMP B-TREE")),
                "ordered detail must not need a temporary sort: {plan:?}"
            );
        }
    }

    fn assert_round_indexes(connection: &rusqlite::Connection) {
        for (name, expected) in [
            ("idx_round_session_started_at", ["session_id", "started_at"]),
            ("idx_tool_round_started_at", ["round_id", "started_at"]),
        ] {
            let columns = connection
                .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")
                .unwrap()
                .query_map([name], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(columns, expected, "index columns for {name}");
        }
        let old_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name='idx_round_session'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(old_count, 0, "owned redundant prefix index is replaced");
    }

    type MetricRows = Vec<Vec<rusqlite::types::Value>>;

    fn snapshot_round_fixture(
        connection: &rusqlite::Connection,
        unrelated_only: bool,
    ) -> Vec<MetricRows> {
        ["session_metrics", "round_metrics", "tool_call_metrics"]
            .iter()
            .map(|table| {
                let filter = if unrelated_only {
                    "WHERE session_id != 'index-session-0'"
                } else {
                    ""
                };
                let mut statement = connection
                    .prepare(&format!("SELECT * FROM {table} {filter} ORDER BY 1"))
                    .unwrap();
                let columns = statement.column_count();
                statement
                    .query_map([], |row| {
                        (0..columns)
                            .map(|index| row.get(index))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn fresh_round_indexes_use_keyed_lookups_and_preserve_detail_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metrics.db");
        let storage = SqliteMetricsStorage::new(&path);
        storage.init().await.unwrap();
        storage
            .init()
            .await
            .expect("fresh initialization is idempotent");
        let connection = super::open_connection(&path).unwrap();
        assert_round_indexes(&connection);
        seed_round_index_fixture(&connection);
        assert_round_lookup_plans(&connection);
        drop(connection);

        let detail = storage
            .session_detail("index-session-0")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            detail
                .rounds
                .iter()
                .map(|round| round.round_id.as_str())
                .collect::<Vec<_>>(),
            ["index-round-0-0", "index-round-0-1", "index-round-0-2"]
        );
        for (round_index, round) in detail.rounds.iter().enumerate() {
            assert_eq!(
                round
                    .tool_calls
                    .iter()
                    .map(|tool| tool.tool_call_id.clone())
                    .collect::<Vec<_>>(),
                (0..3)
                    .map(|tool| format!("index-tool-0-{round_index}-{tool}"))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn populated_legacy_round_indexes_migrate_without_loss_and_preserve_retention() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metrics.db");
        let connection = super::open_connection(&path).unwrap();
        connection.execute_batch(LEGACY_ROUND_SCHEMA).unwrap();
        seed_round_index_fixture(&connection);
        let before = snapshot_round_fixture(&connection, false);
        let unrelated_before = snapshot_round_fixture(&connection, true);
        drop(connection);

        let storage = SqliteMetricsStorage::new(&path);
        let detail_before = storage
            .session_detail("index-session-0")
            .await
            .unwrap()
            .unwrap();
        for _ in 0..2 {
            storage
                .init()
                .await
                .expect("migrate/reinitialize populated legacy database");
            let connection = super::open_connection(&path).unwrap();
            assert_round_indexes(&connection);
            assert_round_lookup_plans(&connection);
            assert_eq!(
                snapshot_round_fixture(&connection, false),
                before,
                "migration preserves every existing row"
            );
            let unrelated_index: i64 = connection.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name='custom_tool_success'",
                [], |row| row.get(0),
            ).unwrap();
            assert_eq!(unrelated_index, 1, "unrelated indexes are preserved");
        }
        assert_eq!(
            storage
                .session_detail("index-session-0")
                .await
                .unwrap()
                .unwrap(),
            detail_before
        );
        let cutoff = Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap();
        assert_eq!(storage.prune_rounds_before(cutoff).await.unwrap(), 1);
        assert_eq!(storage.prune_rounds_before(cutoff).await.unwrap(), 0);
        let detail = storage
            .session_detail("index-session-0")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detail.rounds, detail_before.rounds[1..]);
        assert_eq!(detail.session.total_rounds, 2);
        assert_eq!(detail.session.total_token_usage.total_tokens, 24);
        assert_eq!(detail.session.tool_call_count, 6);
        assert_eq!(detail.session.prompt_cached_tool_outputs, 2);
        assert_eq!(detail.session.prompt_cached_tool_tokens_saved, 6);
        assert_eq!(detail.session.total_tokens_saved, 6);
        assert_eq!(detail.session.message_count, 7);
        let connection = super::open_connection(&path).unwrap();
        assert_eq!(snapshot_round_fixture(&connection, true), unrelated_before);
        let orphans: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM tool_call_metrics WHERE round_id='index-round-0-0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(orphans, 0, "pruning cascades to the expired round's tools");
        let violations: i64 = connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(violations, 0);
    }

    #[test]
    fn open_connection_sets_busy_timeout() {
        // #357: a contended metrics writer must block-and-retry rather than fail
        // immediately with SQLITE_BUSY (SQLite's default busy_timeout is 0).
        let dir = tempdir().expect("temp dir");
        let conn = super::open_connection(&dir.path().join("metrics.db")).expect("open");
        let timeout: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .expect("read busy_timeout");
        assert_eq!(timeout, 5000);
    }

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
                3,
                0,
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
        assert_eq!(summary.prompt_cached_tool_outputs, 3);

        let detail = storage
            .session_detail("session-a")
            .await
            .expect("session detail query")
            .expect("session detail should exist");
        assert_eq!(detail.session.prompt_cached_tool_outputs, 3);
        assert_eq!(detail.rounds.len(), 1);
        assert_eq!(detail.rounds[0].prompt_cached_tool_outputs, 3);
    }

    #[tokio::test]
    async fn distinct_rounds_aggregate_and_same_event_replay_is_idempotent() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
        storage.init().await.expect("init storage");

        let session_id = "resumed-session";
        let first_started = Utc
            .with_ymd_and_hms(2026, 8, 8, 10, 0, 0)
            .single()
            .expect("valid first timestamp");
        let first_completed = first_started + chrono::Duration::seconds(2);
        let second_started = first_started + chrono::Duration::minutes(1);
        let second_completed = second_started + chrono::Duration::seconds(3);
        let first_round = "resumed-session-run-exec-a-round-1";
        let second_round = "resumed-session-run-exec-b-round-1";

        storage
            .upsert_session_start(session_id, "model-a", first_started)
            .await
            .expect("session start");
        storage
            .insert_round_start(first_round, session_id, "model-a", first_started)
            .await
            .expect("first round start");
        storage
            .insert_tool_start("tool-first", first_round, session_id, "Read", first_started)
            .await
            .expect("first tool start");
        storage
            .complete_tool_call(
                "tool-first",
                ToolCallCompletion {
                    completed_at: first_completed,
                    success: true,
                    error: None,
                },
            )
            .await
            .expect("first tool completion");
        storage
            .record_round_compression(first_round, first_completed, 5)
            .await
            .expect("compression");

        let first_usage = TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 2,
            total_tokens: 12,
        };
        storage
            .complete_round(
                first_round,
                first_completed,
                RoundStatus::Error,
                first_usage,
                1,
                7,
                Some("first failure".to_string()),
            )
            .await
            .expect("first completion");

        // Replay the exact same metrics events. Starts must remain insert-only,
        // completion values must remain replacements (not additive), and the
        // tool must stay linked to this same logical round.
        storage
            .insert_round_start(first_round, session_id, "model-a", first_started)
            .await
            .expect("replayed round start");
        storage
            .insert_tool_start("tool-first", first_round, session_id, "Read", first_started)
            .await
            .expect("replayed tool start");
        storage
            .complete_tool_call(
                "tool-first",
                ToolCallCompletion {
                    completed_at: first_completed,
                    success: true,
                    error: None,
                },
            )
            .await
            .expect("replayed tool completion");
        storage
            .complete_round(
                first_round,
                first_completed,
                RoundStatus::Error,
                first_usage,
                1,
                7,
                Some("first failure".to_string()),
            )
            .await
            .expect("replayed round completion");

        let second_usage = TokenUsage {
            prompt_tokens: 20,
            completion_tokens: 3,
            total_tokens: 23,
        };
        storage
            .insert_round_start(second_round, session_id, "model-b", second_started)
            .await
            .expect("second round start");
        storage
            .complete_round(
                second_round,
                second_completed,
                RoundStatus::Success,
                second_usage,
                2,
                11,
                None,
            )
            .await
            .expect("second completion");

        let detail = storage
            .session_detail(session_id)
            .await
            .expect("session detail query")
            .expect("session detail");
        assert_eq!(detail.rounds.len(), 2);
        assert_eq!(detail.session.total_rounds, 2);
        assert_eq!(
            detail.session.total_token_usage,
            TokenUsage {
                prompt_tokens: 30,
                completion_tokens: 5,
                total_tokens: 35,
            }
        );
        assert_eq!(detail.session.prompt_cached_tool_outputs, 3);
        assert_eq!(detail.session.prompt_cached_tool_tokens_saved, 18);
        assert_eq!(detail.session.total_tokens_saved, 23);
        assert_eq!(detail.session.tool_call_count, 1);

        let first = detail
            .rounds
            .iter()
            .find(|round| round.round_id == first_round)
            .expect("first round remains present");
        assert_eq!(first.status, RoundStatus::Error);
        assert_eq!(first.token_usage, first_usage);
        assert_eq!(first.error.as_deref(), Some("first failure"));
        assert_eq!(first.prompt_cached_tool_tokens_saved, 7);
        assert_eq!(first.compression_count, 1);
        assert_eq!(first.tokens_saved, 12);
        assert_eq!(first.tool_calls.len(), 1);
        assert_eq!(first.tool_calls[0].tool_call_id, "tool-first");

        let second = detail
            .rounds
            .iter()
            .find(|round| round.round_id == second_round)
            .expect("second round remains present");
        assert_eq!(second.status, RoundStatus::Success);
        assert_eq!(second.token_usage, second_usage);
        assert!(second.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn durable_round_write_and_session_sum_saturate_at_signed_storage_boundary() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
        storage.init().await.expect("init storage");
        let now = Utc::now();

        storage
            .upsert_session_start("overflow-session", "model", now)
            .await
            .expect("session start");
        storage
            .insert_round_start("overflow-r1", "overflow-session", "model", now)
            .await
            .expect("round 1 start");
        storage
            .complete_round(
                "overflow-r1",
                now,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: u64::MAX,
                    completion_tokens: 3,
                    total_tokens: 1,
                },
                0,
                0,
                None,
            )
            .await
            .expect("round 1 clamps instead of wrapping");
        storage
            .insert_round_start("overflow-r2", "overflow-session", "model", now)
            .await
            .expect("round 2 start");
        storage
            .complete_round(
                "overflow-r2",
                now,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 7,
                    completion_tokens: 11,
                    total_tokens: 18,
                },
                0,
                0,
                None,
            )
            .await
            .expect("session aggregate saturates instead of SQL SUM overflow");

        let detail = storage
            .session_detail("overflow-session")
            .await
            .expect("session detail query")
            .expect("session detail");
        let max = bamboo_domain::MAX_DURABLE_TOKEN_COUNT;
        assert_eq!(detail.rounds.len(), 2);
        assert_eq!(
            detail.rounds[0].token_usage,
            TokenUsage {
                prompt_tokens: max,
                completion_tokens: 3,
                total_tokens: max,
            },
            "u64 values clamp before the checked SQLite conversion"
        );
        assert_eq!(
            detail.session.total_token_usage,
            TokenUsage {
                prompt_tokens: max,
                completion_tokens: 14,
                total_tokens: max,
            },
            "multi-row aggregation uses the same saturation policy without SQLite SUM overflow"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_round_completions_serialize_fold_and_session_update() {
        let dir = tempdir().expect("temp dir");
        let database_path = dir.path().join("metrics.db");
        let storage_a = SqliteMetricsStorage::new(&database_path);
        let storage_b = SqliteMetricsStorage::new(&database_path);
        storage_a.init().await.expect("init storage");
        let now = Utc::now();

        storage_a
            .upsert_session_start("concurrent-session", "model", now)
            .await
            .expect("session start");
        storage_a
            .insert_round_start("concurrent-r1", "concurrent-session", "model", now)
            .await
            .expect("round 1 start");
        storage_a
            .insert_round_start("concurrent-r2", "concurrent-session", "model", now)
            .await
            .expect("round 2 start");

        // Hold the first writer after it has folded round rows but before it
        // updates session_metrics. A second independent storage/connection must
        // not enter its complete_round transaction until the first commits.
        let (first_folded, release_first) =
            super::install_session_token_fold_pause("concurrent-session");
        let first_storage = storage_a.clone();
        let first = tokio::spawn(async move {
            first_storage
                .complete_round(
                    "concurrent-r1",
                    now,
                    RoundStatus::Success,
                    TokenUsage {
                        prompt_tokens: 10,
                        completion_tokens: 1,
                        total_tokens: 11,
                    },
                    0,
                    0,
                    None,
                )
                .await
        });
        tokio::task::spawn_blocking(move || {
            first_folded.recv_timeout(std::time::Duration::from_secs(2))
        })
        .await
        .expect("wait for first fold task")
        .expect("first completion reached aggregate fold");

        let second_entered =
            super::install_complete_round_transaction_entered_hook("concurrent-r2");
        let second = tokio::spawn(async move {
            storage_b
                .complete_round(
                    "concurrent-r2",
                    now,
                    RoundStatus::Success,
                    TokenUsage {
                        prompt_tokens: 20,
                        completion_tokens: 2,
                        total_tokens: 22,
                    },
                    0,
                    0,
                    None,
                )
                .await
        });

        let (entry_while_first_locked, second_entered) = tokio::task::spawn_blocking(move || {
            let result = second_entered.recv_timeout(std::time::Duration::from_millis(250));
            (result, second_entered)
        })
        .await
        .expect("wait for second transaction attempt");
        let was_serialized = matches!(
            entry_while_first_locked,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        );

        release_first.send(()).expect("release first completion");
        first
            .await
            .expect("first completion task")
            .expect("first completion");
        second
            .await
            .expect("second completion task")
            .expect("second completion");

        if was_serialized {
            tokio::task::spawn_blocking(move || {
                second_entered.recv_timeout(std::time::Duration::from_secs(2))
            })
            .await
            .expect("wait for serialized second transaction")
            .expect("second transaction entered after first committed");
        }
        assert!(
            was_serialized,
            "a competing connection entered between aggregate fold and parent update"
        );

        let detail = storage_a
            .session_detail("concurrent-session")
            .await
            .expect("session detail query")
            .expect("session detail");
        assert_eq!(
            detail.session.total_token_usage,
            TokenUsage {
                prompt_tokens: 30,
                completion_tokens: 3,
                total_tokens: 33,
            },
            "the last session writer must include both completed rounds"
        );
    }

    #[tokio::test]
    async fn complete_round_rolls_back_child_update_when_aggregate_refresh_fails() {
        let dir = tempdir().expect("temp dir");
        let database_path = dir.path().join("metrics.db");
        let storage = SqliteMetricsStorage::new(&database_path);
        storage.init().await.expect("init storage");
        let now = Utc::now();

        storage
            .upsert_session_start("rollback-session", "model", now)
            .await
            .expect("session start");
        storage
            .insert_round_start("rollback-target", "rollback-session", "model", now)
            .await
            .expect("target round start");
        storage
            .insert_round_start("rollback-corrupt", "rollback-session", "model", now)
            .await
            .expect("corrupt round start");

        let connection = super::open_connection(&database_path).expect("open database");
        connection
            .execute(
                "UPDATE round_metrics SET prompt_tokens = -1 WHERE round_id = 'rollback-corrupt'",
                [],
            )
            .expect("inject invalid durable counter");
        drop(connection);

        let error = storage
            .complete_round(
                "rollback-target",
                now,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 5,
                    completion_tokens: 7,
                    total_tokens: 12,
                },
                0,
                0,
                None,
            )
            .await
            .expect_err("aggregate validation should fail");
        assert!(
            matches!(error, super::MetricsError::InvalidData(_)),
            "unexpected completion error: {error}"
        );

        let connection = super::open_connection(&database_path).expect("reopen database");
        let target: (String, Option<String>, i64, i64, i64) = connection
            .query_row(
                "SELECT status, completed_at, prompt_tokens, completion_tokens, total_tokens FROM round_metrics WHERE round_id = 'rollback-target'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .expect("read rolled-back target round");
        assert_eq!(
            target,
            (String::from("running"), None, 0, 0, 0),
            "the round mutation must roll back with the failed parent refresh"
        );
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
                0,
                0,
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
                0,
                0,
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

    #[tokio::test]
    async fn model_filter_scopes_summary_by_model_and_daily_across_all_dimensions() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
        storage.init().await.expect("init storage");
        let now = Utc
            .with_ymd_and_hms(2026, 2, 10, 12, 0, 0)
            .single()
            .expect("valid datetime");

        for (suffix, model, tool, tokens, cached, status) in [
            ("a", "model-a", "Read", 10, 2, SessionStatus::Completed),
            ("b", "model-b", "Write", 20, 4, SessionStatus::Error),
        ] {
            let session_id = format!("filtered-session-{suffix}");
            let round_id = format!("filtered-round-{suffix}");
            let tool_id = format!("filtered-tool-{suffix}");
            storage
                .upsert_session_start(&session_id, model, now)
                .await
                .expect("session start");
            storage
                .insert_round_start(&round_id, &session_id, model, now)
                .await
                .expect("round start");
            storage
                .insert_tool_start(&tool_id, &round_id, &session_id, tool, now)
                .await
                .expect("tool start");
            storage
                .complete_round(
                    &round_id,
                    now,
                    RoundStatus::Success,
                    TokenUsage {
                        prompt_tokens: tokens,
                        completion_tokens: 0,
                        total_tokens: tokens,
                    },
                    cached,
                    cached,
                    None,
                )
                .await
                .expect("round completion");
            storage
                .record_round_compression(&round_id, now, if model == "model-a" { 3 } else { 7 })
                .await
                .expect("round compression");
            storage
                .complete_session(&session_id, status, now)
                .await
                .expect("session completion");
        }
        storage
            .increment_execute_sync_mismatch("global-only", now)
            .await
            .expect("sync mismatch");

        let selected = ModelMetricsDateFilter {
            model: Some("model-a".to_string()),
            ..ModelMetricsDateFilter::default()
        };
        let summary = storage
            .summary_filtered(selected.clone())
            .await
            .expect("filtered summary");
        assert_eq!(summary.total_sessions, 1);
        assert_eq!(summary.completed_sessions, 1);
        assert_eq!(summary.error_sessions, 0);
        assert_eq!(summary.total_tokens.total_tokens, 10);
        assert_eq!(summary.total_tool_calls, 1);
        assert_eq!(summary.prompt_cached_tool_outputs, 2);
        assert_eq!(summary.tool_context_tokens_saved, 2);
        assert_eq!(summary.total_compression_events, 1);
        assert_eq!(summary.total_tokens_saved, 5);
        assert_eq!(summary.non_tool_compression_tokens_saved, 3);
        assert_eq!(summary.total_sync_mismatches, 0);
        assert!(summary.sync_mismatch_breakdown.is_empty());

        let by_model = storage
            .by_model_filtered(selected)
            .await
            .expect("filtered by-model");
        assert_eq!(by_model.len(), 1);
        assert_eq!(by_model[0].model, "model-a");
        assert_eq!(by_model[0].sessions, 1);
        assert_eq!(by_model[0].rounds, 1);
        assert_eq!(by_model[0].tool_calls, 1);
        assert_eq!(by_model[0].tokens.total_tokens, 10);

        let cleared_by_model = storage
            .by_model_filtered(ModelMetricsDateFilter {
                model: Some("\t".to_string()),
                ..ModelMetricsDateFilter::default()
            })
            .await
            .expect("blank model restores all-model by-model metrics");
        assert_eq!(cleared_by_model.len(), 2);
        assert!(cleared_by_model.iter().any(|row| row.model == "model-a"));
        assert!(cleared_by_model.iter().any(|row| row.model == "model-b"));

        let daily = storage
            .daily_metrics_for_model(1, Some(now.date_naive()), Some("model-a".to_string()))
            .await
            .expect("filtered daily");
        assert_eq!(daily.len(), 1);
        assert_eq!(daily[0].total_sessions, 1);
        assert_eq!(daily[0].total_rounds, 1);
        assert_eq!(daily[0].total_tool_calls, 1);
        assert_eq!(daily[0].total_token_usage.total_tokens, 10);
        assert_eq!(
            daily[0].tool_breakdown,
            HashMap::from([("Read".to_string(), 1)])
        );
        assert_eq!(daily[0].model_breakdown.len(), 1);
        assert!(daily[0].model_breakdown.contains_key("model-a"));

        let cleared = storage
            .summary_filtered(ModelMetricsDateFilter {
                model: Some("   ".to_string()),
                ..ModelMetricsDateFilter::default()
            })
            .await
            .expect("blank model restores all-model summary");
        assert_eq!(cleared.total_sessions, 2);
        assert_eq!(cleared.total_tokens.total_tokens, 30);
        assert_eq!(cleared.total_tool_calls, 2);
        assert_eq!(cleared.total_sync_mismatches, 1);

        let cleared_daily = storage
            .daily_metrics_for_model(1, Some(now.date_naive()), Some(" ".to_string()))
            .await
            .expect("blank model restores all-model daily");
        assert_eq!(cleared_daily[0].total_sessions, 2);
        assert_eq!(cleared_daily[0].total_rounds, 2);
        assert_eq!(cleared_daily[0].total_tool_calls, 2);
        assert_eq!(cleared_daily[0].total_token_usage.total_tokens, 30);
        assert_eq!(cleared_daily[0].model_breakdown.len(), 2);
    }

    #[tokio::test]
    async fn forward_daily_model_filter_matches_forward_summary_scope() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
        storage.init().await.expect("init storage");
        let now = Utc
            .with_ymd_and_hms(2026, 2, 10, 13, 0, 0)
            .single()
            .expect("valid datetime");

        for (suffix, model, tokens) in [("a", "model-a", 5), ("b", "model-b", 9)] {
            let id = format!("forward-filtered-{suffix}");
            storage
                .insert_forward_start(&id, "openai.responses", model, false, now)
                .await
                .expect("forward start");
            storage
                .complete_forward(
                    &id,
                    now,
                    Some(200),
                    ForwardStatus::Success,
                    Some(TokenUsage {
                        prompt_tokens: tokens,
                        completion_tokens: 0,
                        total_tokens: tokens,
                    }),
                    None,
                    None,
                )
                .await
                .expect("forward completion");
        }

        let selected_summary = storage
            .forward_summary(ForwardMetricsFilter {
                model: Some("model-a".to_string()),
                ..ForwardMetricsFilter::default()
            })
            .await
            .expect("filtered forward summary");
        let selected_daily = storage
            .forward_daily_metrics_for_model(1, Some(now.date_naive()), Some("model-a".to_string()))
            .await
            .expect("filtered forward daily");
        assert_eq!(selected_summary.total_requests, 1);
        assert_eq!(selected_summary.total_tokens.total_tokens, 5);
        assert_eq!(selected_daily.len(), 1);
        assert_eq!(selected_daily[0].total_sessions, 1);
        assert_eq!(selected_daily[0].total_token_usage.total_tokens, 5);

        let cleared = storage
            .forward_daily_metrics_for_model(1, Some(now.date_naive()), Some("  ".to_string()))
            .await
            .expect("blank forward model restores all-model daily");
        assert_eq!(cleared[0].total_sessions, 2);
        assert_eq!(cleared[0].total_token_usage.total_tokens, 14);
    }

    #[tokio::test]
    async fn round_rollups_follow_round_day_and_model_while_sessions_follow_start() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
        storage.init().await.expect("init storage");

        let day_one = Utc
            .with_ymd_and_hms(2026, 1, 31, 23, 59, 58)
            .single()
            .expect("valid first day");
        let day_two = Utc
            .with_ymd_and_hms(2026, 2, 1, 11, 0, 0)
            .single()
            .expect("valid second day");
        let day_one_date = day_one.date_naive();
        let day_two_date = day_two.date_naive();

        storage
            .upsert_session_start("cross-day", "model-a", day_one)
            .await
            .expect("session start");

        storage
            .insert_round_start("day-one-round", "cross-day", "model-a", day_one)
            .await
            .expect("first round start");
        storage
            .insert_tool_start(
                "day-one-tool",
                "day-one-round",
                "cross-day",
                "Read",
                day_one,
            )
            .await
            .expect("first tool start");
        storage
            .complete_tool_call(
                "day-one-tool",
                ToolCallCompletion {
                    completed_at: day_one + chrono::Duration::seconds(1),
                    success: true,
                    error: None,
                },
            )
            .await
            .expect("first tool complete");
        storage
            .complete_round(
                "day-one-round",
                // Completion crosses midnight, but occurrence attribution is
                // deliberately locked to the round's non-null start timestamp.
                day_one + chrono::Duration::seconds(2),
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 10,
                    completion_tokens: 1,
                    total_tokens: 11,
                },
                1,
                0,
                None,
            )
            .await
            .expect("first round complete");

        storage
            .insert_round_start("day-two-round", "cross-day", "model-b", day_two)
            .await
            .expect("second round start");
        storage
            .insert_tool_start(
                "day-two-tool",
                "day-two-round",
                "cross-day",
                "Write",
                day_two,
            )
            .await
            .expect("second tool start");
        storage
            .complete_tool_call(
                "day-two-tool",
                ToolCallCompletion {
                    completed_at: day_two + chrono::Duration::seconds(1),
                    success: true,
                    error: None,
                },
            )
            .await
            .expect("second tool complete");
        storage
            .complete_round(
                "day-two-round",
                day_two + chrono::Duration::seconds(2),
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 20,
                    completion_tokens: 2,
                    total_tokens: 22,
                },
                2,
                0,
                None,
            )
            .await
            .expect("second round complete");

        // Running rows have no completion timestamp, but are still real round
        // occurrences and must be counted on their non-null start date.
        storage
            .insert_round_start("day-two-running", "cross-day", "model-b", day_two)
            .await
            .expect("running round start");
        storage
            .complete_session(
                "cross-day",
                SessionStatus::Completed,
                day_two + chrono::Duration::minutes(1),
            )
            .await
            .expect("session completion");

        let daily = storage
            .daily_metrics(2, Some(day_two_date))
            .await
            .expect("cross-day daily metrics");
        assert_eq!(daily.len(), 2);
        let first_day = daily
            .iter()
            .find(|day| day.date == day_one_date)
            .expect("session start day");
        assert_eq!(first_day.total_sessions, 1);
        assert_eq!(first_day.total_rounds, 1);
        assert_eq!(first_day.total_token_usage.total_tokens, 11);
        assert_eq!(first_day.total_tool_calls, 1);
        assert_eq!(
            first_day
                .model_breakdown
                .get("model-a")
                .map(|usage| usage.total_tokens),
            Some(11)
        );

        let second_day = daily
            .iter()
            .find(|day| day.date == day_two_date)
            .expect("continuation day");
        assert_eq!(
            second_day.total_sessions, 0,
            "session count remains attributed to session start"
        );
        assert_eq!(second_day.total_rounds, 2);
        assert_eq!(second_day.total_token_usage.total_tokens, 22);
        assert_eq!(second_day.total_tool_calls, 1);
        assert_eq!(second_day.prompt_cached_tool_outputs, 2);
        assert_eq!(
            second_day
                .model_breakdown
                .get("model-b")
                .map(|usage| usage.total_tokens),
            Some(22)
        );
        assert_eq!(
            second_day.tool_breakdown,
            HashMap::from([(String::from("Write"), 1)])
        );

        let day_two_only = storage
            .daily_metrics(1, Some(day_two_date))
            .await
            .expect("day-two-only daily metrics");
        assert_eq!(day_two_only.len(), 1);
        assert_eq!(day_two_only[0].total_sessions, 0);
        assert_eq!(day_two_only[0].total_rounds, 2);
        assert_eq!(day_two_only[0].total_token_usage.total_tokens, 22);

        let by_model = storage
            .by_model(MetricsDateFilter::default())
            .await
            .expect("model rollup");
        let model_a = by_model
            .iter()
            .find(|model| model.model == "model-a")
            .expect("session model");
        assert_eq!(model_a.sessions, 1);
        assert_eq!(model_a.rounds, 1);
        assert_eq!(model_a.tokens.total_tokens, 11);
        assert_eq!(model_a.tool_calls, 1);
        assert_eq!(model_a.prompt_cached_tool_outputs, 1);

        let model_b = by_model
            .iter()
            .find(|model| model.model == "model-b")
            .expect("later round model retained without a session count");
        assert_eq!(model_b.sessions, 0);
        assert_eq!(model_b.rounds, 2);
        assert_eq!(model_b.tokens.total_tokens, 22);
        assert_eq!(model_b.tool_calls, 1);
        assert_eq!(model_b.prompt_cached_tool_outputs, 2);

        let day_two_models = storage
            .by_model(MetricsDateFilter {
                start_date: Some(day_two_date),
                end_date: Some(day_two_date),
            })
            .await
            .expect("day-two model rollup");
        assert_eq!(day_two_models.len(), 1);
        assert_eq!(day_two_models[0].model, "model-b");
        assert_eq!(day_two_models[0].sessions, 0);
        assert_eq!(day_two_models[0].rounds, 2);
        assert_eq!(day_two_models[0].tokens.total_tokens, 22);

        let day_two_summary = storage
            .summary(MetricsDateFilter {
                start_date: Some(day_two_date),
                end_date: Some(day_two_date),
            })
            .await
            .expect("day-two summary");
        assert_eq!(day_two_summary.total_sessions, 0);
        assert_eq!(day_two_summary.completed_sessions, 0);
        assert_eq!(day_two_summary.total_tokens.total_tokens, 22);
        assert_eq!(day_two_summary.total_tool_calls, 1);
        assert_eq!(day_two_summary.prompt_cached_tool_outputs, 2);

        // Every usage surface must equal a direct fold of round rows: session
        // counts are separate and must never multiply the usage contribution.
        let detail = storage
            .session_detail("cross-day")
            .await
            .expect("session detail query")
            .expect("session detail");
        let direct_total = detail
            .rounds
            .iter()
            .map(|round| round.token_usage.total_tokens)
            .sum::<u64>();
        assert_eq!(direct_total, 33);
        assert_eq!(
            daily
                .iter()
                .map(|day| day.total_token_usage.total_tokens)
                .sum::<u64>(),
            direct_total
        );
        assert_eq!(
            by_model
                .iter()
                .map(|model| model.tokens.total_tokens)
                .sum::<u64>(),
            direct_total
        );
        assert_eq!(
            storage
                .summary(MetricsDateFilter::default())
                .await
                .expect("unfiltered summary")
                .total_tokens
                .total_tokens,
            direct_total
        );
    }

    #[tokio::test]
    async fn prune_deletes_old_rounds_and_refreshes_affected_session_aggregate() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
        storage.init().await.expect("init storage");

        let day = Utc
            .with_ymd_and_hms(2026, 2, 10, 12, 0, 0)
            .single()
            .expect("valid datetime");
        let old = day - chrono::Duration::days(40);

        storage
            .upsert_session_start("s", "gpt-4", day)
            .await
            .expect("session start");
        // One old round (will be pruned) and one recent round (retained).
        for (rid, ts) in [("r-old", old), ("r-new", day)] {
            storage
                .insert_round_start(rid, "s", "gpt-4", ts)
                .await
                .expect("round start");
            storage
                .complete_round(
                    rid,
                    ts,
                    RoundStatus::Success,
                    TokenUsage {
                        prompt_tokens: 1,
                        completion_tokens: 1,
                        total_tokens: 2,
                    },
                    0,
                    0,
                    None,
                )
                .await
                .expect("round complete");
        }

        let deleted = storage
            .prune_rounds_before(day - chrono::Duration::days(30))
            .await
            .expect("prune");
        assert_eq!(deleted, 1, "only the 40-day-old round is pruned");

        // The affected session's aggregate is recomputed from the remaining round.
        let daily = storage
            .daily_metrics(
                1,
                Some(NaiveDate::from_ymd_opt(2026, 2, 10).expect("valid date")),
            )
            .await
            .expect("daily metrics");
        assert_eq!(daily.len(), 1);
        assert_eq!(
            daily[0].total_rounds, 1,
            "session round aggregate refreshed to exclude the pruned round"
        );
    }

    #[tokio::test]
    async fn daily_metrics_end_date_is_inclusive_and_next_day_excluded() {
        // Locks the sargable half-open range for both session and round rows: an
        // occurrence at the very end of the end date is kept; one at 00:00 the
        // next day is excluded.
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));
        storage.init().await.expect("init storage");

        let end_of_day = Utc
            .with_ymd_and_hms(2026, 2, 10, 23, 59, 59)
            .single()
            .expect("valid datetime");
        let next_day_midnight = Utc
            .with_ymd_and_hms(2026, 2, 11, 0, 0, 0)
            .single()
            .expect("valid datetime");
        storage
            .upsert_session_start("in-range", "gpt-4", end_of_day)
            .await
            .expect("session start");
        storage
            .upsert_session_start("out-of-range", "gpt-4", next_day_midnight)
            .await
            .expect("session start");
        storage
            .insert_round_start("in-range-round", "in-range", "gpt-4", end_of_day)
            .await
            .expect("in-range round start");
        storage
            .complete_round(
                "in-range-round",
                end_of_day,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
                0,
                0,
                None,
            )
            .await
            .expect("in-range round completion");
        storage
            .insert_round_start(
                "out-of-range-round",
                "out-of-range",
                "gpt-4",
                next_day_midnight,
            )
            .await
            .expect("out-of-range round start");
        storage
            .complete_round(
                "out-of-range-round",
                next_day_midnight,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 2,
                    completion_tokens: 2,
                    total_tokens: 4,
                },
                0,
                0,
                None,
            )
            .await
            .expect("out-of-range round completion");

        let daily = storage
            .daily_metrics(
                1,
                Some(NaiveDate::from_ymd_opt(2026, 2, 10).expect("valid date")),
            )
            .await
            .expect("daily metrics");

        assert_eq!(daily.len(), 1, "only the end date should be in range");
        assert_eq!(daily[0].date, NaiveDate::from_ymd_opt(2026, 2, 10).unwrap());
        assert_eq!(
            daily[0].total_sessions, 1,
            "the 23:59:59 session is in range; the next-day 00:00 session is not"
        );
        assert_eq!(daily[0].total_rounds, 1);
        assert_eq!(daily[0].total_token_usage.total_tokens, 2);
    }

    #[tokio::test]
    async fn forward_metrics_preserve_provider_cache_dimensions_separately() {
        let dir = tempdir().expect("temp dir");
        let database_path = dir.path().join("metrics.db");
        let legacy = rusqlite::Connection::open(&database_path).expect("legacy database");
        legacy
            .execute_batch(
                r#"
                CREATE TABLE forward_request_metrics (
                    forward_id TEXT PRIMARY KEY,
                    endpoint TEXT NOT NULL,
                    model TEXT NOT NULL,
                    is_stream INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status_code INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    total_tokens INTEGER,
                    error TEXT,
                    updated_at TEXT NOT NULL
                );
                "#,
            )
            .expect("legacy forward schema");
        drop(legacy);

        let storage = SqliteMetricsStorage::new(database_path);
        storage.init().await.expect("init storage");
        let now = Utc
            .with_ymd_and_hms(2026, 2, 11, 9, 0, 0)
            .single()
            .expect("valid datetime");

        storage
            .insert_forward_start("forward-cache", "openai.responses", "gpt-5.6", true, now)
            .await
            .expect("forward start");
        storage
            .complete_forward(
                "forward-cache",
                now + chrono::Duration::milliseconds(25),
                Some(200),
                ForwardStatus::Success,
                Some(TokenUsage {
                    prompt_tokens: 80,
                    completion_tokens: 20,
                    total_tokens: 100,
                }),
                Some(ForwardTokenDetails {
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: Some(32),
                    cache_write_input_tokens: Some(48),
                    reasoning_output_tokens: Some(5),
                }),
                None,
            )
            .await
            .expect("forward completion");

        let requests = storage
            .forward_requests(ForwardMetricsFilter::default())
            .await
            .expect("forward requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].token_details.cache_read_input_tokens, Some(32));
        assert_eq!(requests[0].token_details.cache_write_input_tokens, Some(48));
        assert_eq!(requests[0].token_details.cache_creation_input_tokens, None);
        assert_eq!(requests[0].token_details.reasoning_output_tokens, Some(5));

        let summary = storage
            .forward_summary(ForwardMetricsFilter::default())
            .await
            .expect("forward summary");
        assert_eq!(summary.token_details.cache_read_input_tokens, Some(32));
        assert_eq!(summary.token_details.cache_write_input_tokens, Some(48));
        assert_eq!(summary.token_details.cache_creation_input_tokens, None);
        assert_eq!(summary.token_details.reasoning_output_tokens, Some(5));

        let endpoints = storage
            .forward_by_endpoint(ForwardMetricsFilter::default())
            .await
            .expect("forward endpoints");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(
            endpoints[0].token_details.cache_write_input_tokens,
            Some(48)
        );
    }

    #[tokio::test]
    async fn storage_reconciles_stale_running_sessions_rounds_and_forwards() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));

        storage.init().await.expect("init storage");

        let now = Utc
            .with_ymd_and_hms(2026, 2, 12, 9, 0, 0)
            .single()
            .expect("valid datetime");

        storage
            .upsert_session_start("stale-await", "gpt-4", now)
            .await
            .expect("await session start");
        storage
            .insert_round_start("round-await", "stale-await", "gpt-4", now)
            .await
            .expect("await round start");

        storage
            .upsert_session_start("stale-complete", "gpt-4", now)
            .await
            .expect("complete session start");
        storage
            .insert_round_start("round-complete", "stale-complete", "gpt-4", now)
            .await
            .expect("complete round start");

        storage
            .insert_forward_start(
                "forward-pending",
                "/v1/chat/completions",
                "gpt-4",
                false,
                now,
            )
            .await
            .expect("forward start");

        storage
            .reconcile_stale_executions(&[], &[String::from("stale-await")])
            .await
            .expect("reconcile stale executions");

        let sessions = storage
            .sessions(SessionMetricsFilter::default())
            .await
            .expect("sessions query");
        let stale_await = sessions
            .iter()
            .find(|session| session.session_id == "stale-await")
            .expect("stale-await should exist");
        let stale_complete = sessions
            .iter()
            .find(|session| session.session_id == "stale-complete")
            .expect("stale-complete should exist");
        assert_eq!(stale_await.status, SessionStatus::AwaitingResponse);
        assert_eq!(stale_complete.status, SessionStatus::Completed);
        assert!(stale_await.completed_at.is_some());
        assert!(stale_complete.completed_at.is_some());

        let await_detail = storage
            .session_detail("stale-await")
            .await
            .expect("await detail query")
            .expect("await detail exists");
        let complete_detail = storage
            .session_detail("stale-complete")
            .await
            .expect("complete detail query")
            .expect("complete detail exists");
        assert_eq!(await_detail.rounds[0].status, RoundStatus::Error);
        assert_eq!(complete_detail.rounds[0].status, RoundStatus::Error);
        assert_eq!(
            await_detail.rounds[0].error.as_deref(),
            Some("reconciled_stale_round")
        );
        assert_eq!(
            complete_detail.rounds[0].error.as_deref(),
            Some("reconciled_stale_round")
        );

        let forward_requests = storage
            .forward_requests(ForwardMetricsFilter::default())
            .await
            .expect("forward requests query");
        assert_eq!(forward_requests.len(), 1);
        assert_eq!(forward_requests[0].status, Some(ForwardStatus::Error));
        assert_eq!(
            forward_requests[0].error.as_deref(),
            Some("reconciled_stale_forward")
        );

        let forward_summary = storage
            .forward_summary(ForwardMetricsFilter::default())
            .await
            .expect("forward summary query");
        assert_eq!(forward_summary.total_requests, 1);
        assert_eq!(forward_summary.successful_requests, 0);
        assert_eq!(forward_summary.failed_requests, 1);

        let summary = storage
            .summary(MetricsDateFilter::default())
            .await
            .expect("summary query");
        assert_eq!(summary.total_sessions, 2);
        assert_eq!(summary.active_sessions, 0);
        assert_eq!(summary.awaiting_response_sessions, 1);
        assert_eq!(summary.completed_sessions, 1);
        assert_eq!(summary.error_sessions, 0);
        assert_eq!(summary.cancelled_sessions, 0);
    }

    #[tokio::test]
    async fn storage_summarizes_execute_sync_mismatches_by_reason() {
        let dir = tempdir().expect("temp dir");
        let storage = SqliteMetricsStorage::new(dir.path().join("metrics.db"));

        storage.init().await.expect("init storage");

        let day_a = Utc
            .with_ymd_and_hms(2026, 2, 10, 10, 0, 0)
            .single()
            .expect("valid datetime");
        let day_b = Utc
            .with_ymd_and_hms(2026, 2, 11, 10, 0, 0)
            .single()
            .expect("valid datetime");

        storage
            .increment_execute_sync_mismatch("message_count", day_a)
            .await
            .expect("message_count mismatch one");
        storage
            .increment_execute_sync_mismatch("message_count", day_a)
            .await
            .expect("message_count mismatch two");
        storage
            .increment_execute_sync_mismatch("pending_question", day_a)
            .await
            .expect("pending question mismatch");
        storage
            .increment_execute_sync_mismatch("last_message_id", day_b)
            .await
            .expect("last_message_id mismatch");

        let day_a_summary = storage
            .summary(MetricsDateFilter {
                start_date: Some(NaiveDate::from_ymd_opt(2026, 2, 10).expect("valid date")),
                end_date: Some(NaiveDate::from_ymd_opt(2026, 2, 10).expect("valid date")),
            })
            .await
            .expect("day a summary");

        assert_eq!(day_a_summary.total_sync_mismatches, 3);
        assert_eq!(
            day_a_summary.sync_mismatch_breakdown,
            HashMap::from([
                (String::from("message_count"), 2_u64),
                (String::from("pending_question"), 1_u64),
            ])
        );

        let full_summary = storage
            .summary(MetricsDateFilter::default())
            .await
            .expect("full summary");
        assert_eq!(full_summary.total_sync_mismatches, 4);
        assert_eq!(
            full_summary.sync_mismatch_breakdown.get("last_message_id"),
            Some(&1_u64)
        );
    }
}
