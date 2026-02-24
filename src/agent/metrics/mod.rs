//! Metrics collection and aggregation system
//!
//! This module provides comprehensive metrics tracking for the agent system,
//! including token usage, request counts, session statistics, and tool performance.
//!
//! # Components
//!
//! - **Collector**: Collects metrics events from various sources
//! - **Storage**: Persists metrics to SQLite database
//! - **Aggregator**: Aggregates metrics by time periods (daily, weekly, monthly)
//! - **Worker**: Background worker for processing metrics events
//! - **Bus**: Event bus for distributing metrics events
//!
//! # Usage
//!
//! ```rust,ignore
//! use bamboo_agent::agent::metrics::{MetricsCollector, MetricsStorage};
//!
//! let collector = MetricsCollector::new();
//! collector.record_event(event);
//!
//! let storage = SqliteMetricsStorage::new("metrics.db")?;
//! let summary = storage.get_summary(filter)?;
//! ```

pub mod aggregator;
pub mod bus;
pub mod collector;
pub mod events;
pub mod storage;
pub mod types;
pub mod worker;

pub use aggregator::{aggregate_monthly, aggregate_weekly, PeriodMetrics};
pub use bus::MetricsBus;
pub use collector::MetricsCollector;
pub use events::{ChatEvent, EventMeta, ForwardEvent, MetricsEvent, SystemEvent};
pub use storage::{MetricsError, MetricsResult, MetricsStorage, SqliteMetricsStorage};
pub use types::{
    DailyMetrics, ForwardEndpointMetrics, ForwardMetricsFilter, ForwardMetricsSummary,
    ForwardRequestMetrics, ForwardStatus, MetricsDateFilter, MetricsSummary, ModelMetrics,
    RoundMetrics, RoundStatus, SessionDetail, SessionMetrics, SessionMetricsFilter, SessionStatus,
    TokenUsage, ToolCallMetrics,
};
pub use worker::MetricsWorker;
