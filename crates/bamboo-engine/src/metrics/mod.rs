//! Metrics collection and aggregation system.
//!
//! The live pipeline (bus / collector / worker / aggregator / events) lives
//! here in the engine. The persistence layer — metrics `types` and the
//! `MetricsStorage` abstraction with its SQLite implementation — lives in
//! `bamboo_infrastructure::metrics` and is re-exported below so the historical
//! `crate::metrics::{types, storage}::…` paths keep resolving.

pub mod aggregator;
pub mod bus;
pub mod collector;
pub mod events;
pub mod worker;

pub use bamboo_infrastructure::metrics::{storage, types};

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
