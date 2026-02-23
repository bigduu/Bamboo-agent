use std::path::Path;
use std::sync::Arc;

use agent_metrics::{
    aggregate_monthly, aggregate_weekly, DailyMetrics, ForwardEndpointMetrics,
    ForwardMetricsFilter, ForwardMetricsSummary, ForwardRequestMetrics, MetricsCollector,
    MetricsDateFilter, MetricsError, MetricsStorage, MetricsSummary, ModelMetrics, PeriodMetrics,
    SessionDetail, SessionMetrics, SessionMetricsFilter, SqliteMetricsStorage,
};
use chrono::NaiveDate;

#[derive(Clone)]
pub struct MetricsService {
    storage: Arc<SqliteMetricsStorage>,
    collector: MetricsCollector,
}

impl MetricsService {
    pub async fn new(db_path: impl AsRef<Path>) -> Result<Self, MetricsError> {
        let storage = Arc::new(SqliteMetricsStorage::new(db_path));
        storage.init().await?;

        let storage_trait: Arc<dyn MetricsStorage> = storage.clone();
        let collector = MetricsCollector::spawn(storage_trait, 90);

        Ok(Self { storage, collector })
    }

    pub fn collector(&self) -> MetricsCollector {
        self.collector.clone()
    }

    pub async fn summary(
        &self,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<MetricsSummary, MetricsError> {
        self.storage
            .summary(MetricsDateFilter {
                start_date,
                end_date,
            })
            .await
    }

    pub async fn by_model(
        &self,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<ModelMetrics>, MetricsError> {
        self.storage
            .by_model(MetricsDateFilter {
                start_date,
                end_date,
            })
            .await
    }

    pub async fn sessions(
        &self,
        filter: SessionMetricsFilter,
    ) -> Result<Vec<SessionMetrics>, MetricsError> {
        self.storage.sessions(filter).await
    }

    pub async fn session_detail(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionDetail>, MetricsError> {
        self.storage.session_detail(session_id).await
    }

    pub async fn daily(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<DailyMetrics>, MetricsError> {
        self.storage.daily_metrics(days, end_date).await
    }

    pub async fn weekly(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<PeriodMetrics>, MetricsError> {
        let daily = self.daily(days, end_date).await?;
        Ok(aggregate_weekly(&daily))
    }

    pub async fn monthly(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<PeriodMetrics>, MetricsError> {
        let daily = self.daily(days, end_date).await?;
        Ok(aggregate_monthly(&daily))
    }

    // Forward metrics methods
    pub async fn forward_summary(
        &self,
        filter: ForwardMetricsFilter,
    ) -> Result<ForwardMetricsSummary, MetricsError> {
        self.storage.forward_summary(filter).await
    }

    pub async fn forward_by_endpoint(
        &self,
        filter: ForwardMetricsFilter,
    ) -> Result<Vec<ForwardEndpointMetrics>, MetricsError> {
        self.storage.forward_by_endpoint(filter).await
    }

    pub async fn forward_requests(
        &self,
        filter: ForwardMetricsFilter,
    ) -> Result<Vec<ForwardRequestMetrics>, MetricsError> {
        self.storage.forward_requests(filter).await
    }

    pub async fn forward_daily(
        &self,
        filter: ForwardMetricsFilter,
    ) -> Result<Vec<DailyMetrics>, MetricsError> {
        self.storage
            .forward_daily_metrics(filter.limit.unwrap_or(30), filter.end_date)
            .await
    }
}
