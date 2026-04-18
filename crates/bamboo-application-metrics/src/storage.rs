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
    async fn complete_round(
        &self,
        round_id: &str,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        prompt_cached_tool_outputs: u32,
        error: Option<String>,
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
    /// * `error` - Error message if the request failed
    async fn complete_forward(
        &self,
        forward_id: &str,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: ForwardStatus,
        usage: Option<TokenUsage>,
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

    /// Retrieves aggregated summary statistics for chat sessions.
    ///
    /// Returns total sessions, token usage, tool call counts, and
    /// active session count for sessions matching the filter.
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

    /// Retrieves metrics grouped by AI model.
    ///
    /// Returns per-model statistics including session counts,
    /// rounds, token usage, and tool calls.
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
    /// Returns per-day statistics including session counts, token usage,
    /// model breakdown, and tool breakdown for trend analysis.
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
            ensure_integer_column(connection, "round_metrics", "prompt_cached_tool_outputs", 0)?;
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
        prompt_cached_tool_outputs: u32,
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
                    prompt_cached_tool_outputs = ?6,
                    error = ?7
                WHERE round_id = ?8
                "#,
                params![
                    completed_at_str,
                    status.as_str(),
                    usage.prompt_tokens as i64,
                    usage.completion_tokens as i64,
                    usage.total_tokens as i64,
                    i64::from(prompt_cached_tool_outputs),
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
            Some(u) => (
                Some(u.prompt_tokens as i64),
                Some(u.completion_tokens as i64),
                Some(u.total_tokens as i64),
            ),
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
        self.with_connection(move |connection| {
            let mut params_vec = Vec::new();
            let where_clause = build_session_where_clause(
                filter.start_date,
                filter.end_date,
                None,
                &mut params_vec,
            );

            let summary_sql = format!(
                "SELECT COUNT(*), COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), COALESCE(SUM(total_tokens), 0), COALESCE(SUM(tool_call_count), 0), COALESCE(SUM(prompt_cached_tool_outputs), 0) FROM session_metrics {}",
                where_clause
            );

            let mut stmt = connection.prepare(&summary_sql)?;
            let mut summary = stmt.query_row(params_from_iter(params_vec.iter()), |row| {
                Ok(MetricsSummary {
                    total_sessions: row.get::<_, i64>(0)? as u64,
                    total_tokens: TokenUsage {
                        prompt_tokens: row.get::<_, i64>(1)? as u64,
                        completion_tokens: row.get::<_, i64>(2)? as u64,
                        total_tokens: row.get::<_, i64>(3)? as u64,
                    },
                    total_tool_calls: row.get::<_, i64>(4)? as u64,
                    prompt_cached_tool_outputs: row.get::<_, i64>(5)? as u64,
                    total_sync_mismatches: 0,
                    sync_mismatch_breakdown: HashMap::new(),
                    active_sessions: 0,
                })
            })?;

            let mut mismatch_params = Vec::new();
            let mismatch_clause = build_execute_sync_mismatch_where_clause(
                filter.start_date,
                filter.end_date,
                None,
                &mut mismatch_params,
            );
            let mismatch_sql = format!(
                "SELECT COALESCE(SUM(count), 0) FROM execute_sync_mismatch_metrics {}",
                mismatch_clause
            );
            let mut mismatch_stmt = connection.prepare(&mismatch_sql)?;
            summary.total_sync_mismatches = mismatch_stmt
                .query_row(params_from_iter(mismatch_params.iter()), |row| row.get::<_, i64>(0))?
                as u64;
            summary.sync_mismatch_breakdown = load_execute_sync_mismatch_breakdown(
                connection,
                filter.start_date,
                filter.end_date,
            )?;
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
                "SELECT model, COUNT(*), COALESCE(SUM(total_rounds), 0), COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), COALESCE(SUM(total_tokens), 0), COALESCE(SUM(tool_call_count), 0), COALESCE(SUM(prompt_cached_tool_outputs), 0) FROM session_metrics {} GROUP BY model ORDER BY SUM(total_tokens) DESC",
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
                    prompt_cached_tool_outputs: row.get::<_, i64>(7)? as u64,
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
                "SELECT session_id, model, started_at, completed_at, total_rounds, prompt_tokens, completion_tokens, total_tokens, tool_call_count, prompt_cached_tool_outputs, status, message_count FROM session_metrics {} ORDER BY started_at DESC LIMIT {}",
                where_sql, limit
            );

            let mut stmt = connection.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
            let mut sessions = Vec::new();

            while let Some(row) = rows.next()? {
                let session_id: String = row.get(0)?;
                let started_at = parse_timestamp(row.get::<_, String>(2)?)?;
                let completed_at = parse_optional_timestamp(row.get::<_, Option<String>>(3)?)?;
                let status_raw: String = row.get(10)?;
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
                    tool_breakdown,
                    status,
                    message_count: row.get::<_, i64>(11)? as u32,
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
            let session_sql = "SELECT session_id, model, started_at, completed_at, total_rounds, prompt_tokens, completion_tokens, total_tokens, tool_call_count, prompt_cached_tool_outputs, status, message_count FROM session_metrics WHERE session_id = ?1";
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
                        row.get::<_, String>(10)?,
                        row.get::<_, i64>(11)?,
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
                    COALESCE(SUM(tool_call_count), 0) AS total_tool_calls,
                    COALESCE(SUM(prompt_cached_tool_outputs), 0) AS prompt_cached_tool_outputs
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
                    prompt_cached_tool_outputs: row.get::<_, i64>(7)? as u64,
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
    let updated_at = format_timestamp(updated_at);
    connection.execute(
        r#"
        UPDATE session_metrics
        SET
            total_rounds = COALESCE((SELECT COUNT(*) FROM round_metrics WHERE session_id = ?1), 0),
            prompt_tokens = COALESCE((SELECT SUM(prompt_tokens) FROM round_metrics WHERE session_id = ?1), 0),
            completion_tokens = COALESCE((SELECT SUM(completion_tokens) FROM round_metrics WHERE session_id = ?1), 0),
            total_tokens = COALESCE((SELECT SUM(total_tokens) FROM round_metrics WHERE session_id = ?1), 0),
            prompt_cached_tool_outputs = COALESCE((SELECT SUM(prompt_cached_tool_outputs) FROM round_metrics WHERE session_id = ?1), 0),
            tool_call_count = COALESCE((SELECT COUNT(*) FROM tool_call_metrics WHERE session_id = ?1), 0),
            updated_at = ?2
        WHERE session_id = ?1
        "#,
        params![session_id, updated_at],
    )?;
    Ok(())
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
    let mut stmt = connection.prepare(
        "SELECT round_id, session_id, model, started_at, completed_at, status, prompt_tokens, completion_tokens, total_tokens, prompt_cached_tool_outputs, error FROM round_metrics WHERE session_id = ?1 ORDER BY started_at ASC",
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
            prompt_cached_tool_outputs: row.get::<_, i64>(9)? as u32,
            error: row.get(10)?,
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

/// Loads model-level token usage breakdown for a specific date.
///
/// Retrieves aggregated token usage grouped by AI model for all
/// sessions that started on the specified date.
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `date` - Date to get model breakdown for
///
/// # Returns
///
/// A HashMap mapping model names to their total token usage.
///
/// # Example
///
/// ```rust,ignore
/// let breakdown = load_daily_model_breakdown(&conn, NaiveDate::from_ymd_opt(2026, 2, 24).unwrap())?;
/// // breakdown might be: {"gpt-4": TokenUsage{...}, "claude-3": TokenUsage{...}}
/// ```
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

/// Loads tool call count breakdown for a specific date.
///
/// Retrieves the count of tool invocations grouped by tool name
/// for all tool calls that occurred on the specified date.
///
/// # Arguments
///
/// * `connection` - Database connection to use
/// * `date` - Date to get tool breakdown for
///
/// # Returns
///
/// A HashMap mapping tool names to their invocation counts.
///
/// # Example
///
/// ```rust,ignore
/// let breakdown = load_daily_tool_breakdown(&conn, NaiveDate::from_ymd_opt(2026, 2, 24).unwrap())?;
/// // breakdown might be: {"read_file": 10, "write_file": 5, "execute_command": 3}
/// ```
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
                3,
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
