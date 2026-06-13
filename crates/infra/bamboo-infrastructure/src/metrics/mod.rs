//! Metrics persistence layer.
//!
//! Holds the metrics data types and the `MetricsStorage` abstraction plus its
//! SQLite-backed implementation. This is an infrastructure concern (it owns the
//! rusqlite dependency); the live metrics pipeline (bus / collector / worker)
//! lives in the engine and depends on these abstractions.

pub mod storage;
pub mod types;

pub use storage::{
    MetricsError, MetricsResult, MetricsStorage, SqliteMetricsStorage, ToolCallCompletion,
};
pub use types::{
    DailyMetrics, ForwardEndpointMetrics, ForwardMetricsFilter, ForwardMetricsSummary,
    ForwardRequestMetrics, ForwardStatus, MetricsDateFilter, MetricsSummary, ModelMetrics,
    RoundMetrics, RoundStatus, SessionDetail, SessionMetrics, SessionMetricsFilter, SessionStatus,
    TokenUsage, ToolCallMetrics,
};
