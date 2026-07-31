//! Live metrics pipeline for the Bamboo agent framework.
//!
//! Holds the runtime metrics flow — event bus, collector, background worker and
//! aggregation — plus the `MetricsService` facade. The persistence layer
//! (metrics `types` and the `MetricsStorage` abstraction with its SQLite
//! implementation) lives in `bamboo_infrastructure::metrics` and is re-exported
//! here so callers see one cohesive `bamboo_metrics::…` surface.

pub mod aggregator;
pub mod bus;
pub mod collector;
pub mod events;
pub mod metrics_service;
pub mod worker;

pub use bamboo_infrastructure::metrics::{storage, types};

pub use aggregator::{aggregate_monthly, aggregate_weekly, PeriodMetrics};
pub use bus::MetricsBus;
pub use collector::MetricsCollector;
pub use events::{ChatEvent, EventMeta, ForwardEvent, MetricsEvent, SystemEvent};
pub use metrics_service::MetricsService;
pub use storage::{MetricsError, MetricsResult, MetricsStorage, SqliteMetricsStorage};
pub use types::{
    DailyMetrics, ForwardEndpointMetrics, ForwardMetricsFilter, ForwardMetricsSummary,
    ForwardRequestMetrics, ForwardStatus, ForwardTokenDetails, MetricsDateFilter, MetricsSummary,
    ModelMetrics, RoundMetrics, RoundStatus, SessionDetail, SessionMetrics, SessionMetricsFilter,
    SessionStatus, TokenUsage, ToolCallMetrics,
};
pub use worker::MetricsWorker;
