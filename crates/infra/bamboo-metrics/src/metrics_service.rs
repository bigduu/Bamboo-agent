use std::path::Path;
use std::sync::Arc;

use crate::{
    aggregate_monthly, aggregate_weekly, DailyMetrics, ForwardEndpointMetrics,
    ForwardMetricsFilter, ForwardMetricsSummary, ForwardRequestMetrics, MetricsCollector,
    MetricsDateFilter, MetricsError, MetricsStorage, MetricsSummary, ModelMetrics, PeriodMetrics,
    SessionDetail, SessionMetrics, SessionMetricsFilter, SqliteMetricsStorage,
};
use bamboo_agent_core::Session;
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

    pub async fn reconcile_startup_sessions(
        &self,
        sessions: impl IntoIterator<Item = Session>,
        active_session_ids: &[String],
    ) -> Result<(), MetricsError> {
        let awaiting_response_session_ids = sessions
            .into_iter()
            .filter(|session| {
                session.has_pending_question()
                    || session.agent_runtime_state.as_ref().is_some_and(|state| {
                        matches!(state.status, bamboo_domain::AgentStatusState::Suspended)
                    })
            })
            .map(|session| session.id)
            .collect::<Vec<_>>();

        self.storage
            .reconcile_stale_executions(active_session_ids, &awaiting_response_session_ids)
            .await
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

#[cfg(test)]
mod tests {
    use chrono::{Datelike, TimeZone, Utc};
    use tempfile::tempdir;

    use super::*;
    use crate::{RoundStatus, SessionStatus, TokenUsage};

    #[tokio::test]
    async fn weekly_and_monthly_preserve_round_attributed_daily_usage() {
        let directory = tempdir().expect("metrics tempdir");
        let service = MetricsService::new(directory.path().join("metrics.db"))
            .await
            .expect("metrics service");
        // MetricsService starts its real 90-day retention worker, so derive a
        // recent month boundary instead of using fixtures that will age out.
        let current_month_date = Utc::now()
            .date_naive()
            .with_day(1)
            .expect("current month has a first day");
        let previous_month_date = current_month_date
            .pred_opt()
            .expect("current month has a previous day");
        let previous_month = Utc.from_utc_datetime(
            &previous_month_date
                .and_hms_opt(10, 0, 0)
                .expect("valid previous-month timestamp"),
        );
        let current_month = Utc.from_utc_datetime(
            &current_month_date
                .and_hms_opt(10, 0, 0)
                .expect("valid current-month timestamp"),
        );

        service
            .storage
            .upsert_session_start("period-session", "model-a", previous_month)
            .await
            .expect("session start");
        service
            .storage
            .insert_round_start("period-r1", "period-session", "model-a", previous_month)
            .await
            .expect("previous-month round start");
        service
            .storage
            .complete_round(
                "period-r1",
                previous_month,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 4,
                    completion_tokens: 6,
                    total_tokens: 10,
                },
                0,
                0,
                None,
            )
            .await
            .expect("previous-month round completion");
        service
            .storage
            .insert_round_start("period-r2", "period-session", "model-b", current_month)
            .await
            .expect("current-month round start");
        service
            .storage
            .complete_round(
                "period-r2",
                current_month,
                RoundStatus::Success,
                TokenUsage {
                    prompt_tokens: 12,
                    completion_tokens: 8,
                    total_tokens: 20,
                },
                0,
                0,
                None,
            )
            .await
            .expect("current-month round completion");
        service
            .storage
            .complete_session("period-session", SessionStatus::Completed, current_month)
            .await
            .expect("session completion");

        // The weekly rollup must preserve the corrected daily rows without
        // moving later usage to the session's earlier start date. A month
        // boundary can also be a Monday, so assert across all returned weeks.
        let weekly = service
            .weekly(2, Some(current_month_date))
            .await
            .expect("weekly metrics");
        assert_eq!(
            weekly
                .iter()
                .map(|period| period.total_sessions)
                .sum::<u32>(),
            1
        );
        assert_eq!(
            weekly.iter().map(|period| period.total_rounds).sum::<u32>(),
            2
        );
        assert_eq!(
            weekly
                .iter()
                .map(|period| period.total_token_usage.total_tokens)
                .sum::<u64>(),
            30
        );
        assert_eq!(
            weekly
                .iter()
                .filter_map(|period| period.model_breakdown.get("model-a"))
                .map(|usage| usage.total_tokens)
                .sum::<u64>(),
            10
        );
        assert_eq!(
            weekly
                .iter()
                .filter_map(|period| period.model_breakdown.get("model-b"))
                .map(|usage| usage.total_tokens)
                .sum::<u64>(),
            20
        );

        let monthly = service
            .monthly(2, Some(current_month_date))
            .await
            .expect("monthly metrics");
        assert_eq!(monthly.len(), 2);
        let previous_month_rollup = monthly
            .iter()
            .find(|period| {
                period.period_start
                    == previous_month_date
                        .with_day(1)
                        .expect("previous month has a first day")
            })
            .expect("previous-month rollup");
        assert_eq!(previous_month_rollup.total_sessions, 1);
        assert_eq!(previous_month_rollup.total_rounds, 1);
        assert_eq!(previous_month_rollup.total_token_usage.total_tokens, 10);

        let current_month_rollup = monthly
            .iter()
            .find(|period| period.period_start == current_month_date)
            .expect("current-month rollup");
        assert_eq!(
            current_month_rollup.total_sessions, 0,
            "monthly session count remains on the session-start month"
        );
        assert_eq!(current_month_rollup.total_rounds, 1);
        assert_eq!(current_month_rollup.total_token_usage.total_tokens, 20);
    }
}
