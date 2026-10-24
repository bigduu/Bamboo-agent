use std::path::Path;
use std::sync::Arc;

use crate::{
    aggregate_monthly, aggregate_weekly, DailyMetrics, ForwardEndpointMetrics,
    ForwardMetricsFilter, ForwardMetricsSummary, ForwardRequestMetrics, MetricsCollector,
    MetricsDateFilter, MetricsError, MetricsStorage, MetricsSummary, ModelMetrics,
    ModelMetricsDateFilter, PeriodMetrics, SessionDetail, SessionMetrics, SessionMetricsFilter,
    SqliteMetricsStorage,
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

        // The collector also initializes storage and immediately schedules
        // retention. Reconciliation must share that FIFO and await its result,
        // rather than opening a competing startup connection to the same DB.
        self.collector
            .reconcile_startup(active_session_ids, &awaiting_response_session_ids)
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

    pub async fn summary_filtered(
        &self,
        filter: ModelMetricsDateFilter,
    ) -> Result<MetricsSummary, MetricsError> {
        self.storage.summary_filtered(filter).await
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

    pub async fn by_model_filtered(
        &self,
        filter: ModelMetricsDateFilter,
    ) -> Result<Vec<ModelMetrics>, MetricsError> {
        self.storage.by_model_filtered(filter).await
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
        self.daily_for_model(days, end_date, None).await
    }

    pub async fn daily_for_model(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        model: Option<String>,
    ) -> Result<Vec<DailyMetrics>, MetricsError> {
        self.storage
            .daily_metrics_for_model(days, end_date, model)
            .await
    }

    pub async fn weekly(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<PeriodMetrics>, MetricsError> {
        self.weekly_for_model(days, end_date, None).await
    }

    pub async fn weekly_for_model(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        model: Option<String>,
    ) -> Result<Vec<PeriodMetrics>, MetricsError> {
        let daily = self.daily_for_model(days, end_date, model).await?;
        Ok(aggregate_weekly(&daily))
    }

    pub async fn monthly(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
    ) -> Result<Vec<PeriodMetrics>, MetricsError> {
        self.monthly_for_model(days, end_date, None).await
    }

    pub async fn monthly_for_model(
        &self,
        days: u32,
        end_date: Option<NaiveDate>,
        model: Option<String>,
    ) -> Result<Vec<PeriodMetrics>, MetricsError> {
        let daily = self.daily_for_model(days, end_date, model).await?;
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
            .forward_daily_metrics_filtered(
                filter.limit.unwrap_or(30),
                filter.end_date,
                filter.endpoint,
                filter.model,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, TimeZone, Utc};
    use tempfile::tempdir;

    use super::*;
    use crate::{ForwardStatus, RoundStatus, SessionStatus, TokenUsage};

    async fn wait_for_storage_release(storage: &std::sync::Weak<SqliteMetricsStorage>) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while storage.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted collector work must finish before fixture cleanup");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_reconciliation_waits_for_initialization_and_accepted_prefix() {
        let directory = tempdir().expect("metrics tempdir");
        let path = directory.path().join("metrics.db");
        let storage = Arc::new(SqliteMetricsStorage::new(&path));
        let weak = Arc::downgrade(&storage);
        let collector = MetricsCollector::spawn(storage.clone(), 90);
        let now = Utc::now();

        // Queue real observations before the worker can initialize its DB.
        collector.session_started("stale", "model", now);
        collector.round_started("stale-round", "stale", "model", now);
        collector.session_message_count("stale", 7, now);
        collector.session_started("active", "model", now);
        let service = MetricsService { storage, collector };
        service
            .reconcile_startup_sessions(std::iter::empty::<Session>(), &["active".to_owned()])
            .await
            .expect("startup reconciliation receipt");

        drop(service);
        wait_for_storage_release(&weak).await;
        let reopened = SqliteMetricsStorage::new(&path);
        let stale = reopened.session_detail("stale").await.unwrap().unwrap();
        assert_eq!(stale.session.status, SessionStatus::Completed);
        assert_eq!(stale.session.message_count, 7);
        assert_eq!(stale.rounds.len(), 1);
        assert_eq!(stale.rounds[0].status, RoundStatus::Error);
        let active = reopened.session_detail("active").await.unwrap().unwrap();
        assert_eq!(active.session.status, SessionStatus::Running);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_reconciliation_reports_collector_initialization_failure() {
        let directory = tempdir().expect("metrics tempdir");
        let blocked_parent = directory.path().join("blocked-parent");
        let original = b"fixture parent is a file";
        std::fs::write(&blocked_parent, original).unwrap();
        let storage = Arc::new(SqliteMetricsStorage::new(blocked_parent.join("metrics.db")));
        let weak = Arc::downgrade(&storage);
        let collector = MetricsCollector::spawn(storage.clone(), 90);
        let service = MetricsService { storage, collector };
        let error = service
            .reconcile_startup_sessions(std::iter::empty::<Session>(), &[])
            .await
            .expect_err("initialization failure cannot produce a successful receipt");
        assert!(matches!(&error, MetricsError::Task(_)));
        assert!(error
            .to_string()
            .contains("metrics collector initialization failed"));

        drop(service);
        wait_for_storage_release(&weak).await;
        assert_eq!(std::fs::read(blocked_parent).unwrap(), original);
    }

    #[tokio::test]
    async fn forward_daily_preserves_endpoint_and_model_filters() {
        let directory = tempdir().expect("metrics tempdir");
        let service = MetricsService::new(directory.path().join("metrics.db"))
            .await
            .expect("metrics service");
        let now = Utc::now();

        for (suffix, endpoint, model, tokens) in [
            ("a", "openai.responses", "model-a", 5),
            ("b", "openai.responses", "model-b", 9),
            ("c", "anthropic.messages", "model-a", 11),
        ] {
            let forward_id = format!("service-forward-{suffix}");
            service
                .storage
                .insert_forward_start(&forward_id, endpoint, model, false, now)
                .await
                .expect("forward start");
            service
                .storage
                .complete_forward(
                    &forward_id,
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

        let selected = service
            .forward_daily(ForwardMetricsFilter {
                end_date: Some(now.date_naive()),
                endpoint: Some("openai.responses".to_string()),
                model: Some("model-a".to_string()),
                limit: Some(1),
                ..ForwardMetricsFilter::default()
            })
            .await
            .expect("endpoint and model filtered forward daily");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].total_sessions, 1);
        assert_eq!(selected[0].total_token_usage.total_tokens, 5);

        let cleared = service
            .forward_daily(ForwardMetricsFilter {
                end_date: Some(now.date_naive()),
                endpoint: Some("openai.responses".to_string()),
                model: Some("  ".to_string()),
                limit: Some(1),
                ..ForwardMetricsFilter::default()
            })
            .await
            .expect("blank model restores all models for selected endpoint");
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0].total_sessions, 2);
        assert_eq!(cleared[0].total_token_usage.total_tokens, 14);
    }

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

        let legacy_summary = service
            .summary(None, None)
            .await
            .expect("legacy all-model summary");
        assert_eq!(legacy_summary.total_tokens.total_tokens, 30);
        let legacy_by_model = service
            .by_model(None, None)
            .await
            .expect("legacy all-model grouped metrics");
        assert_eq!(legacy_by_model.len(), 2);

        let selected_weekly = service
            .weekly_for_model(2, Some(current_month_date), Some("model-a".to_string()))
            .await
            .expect("model-filtered weekly metrics");
        assert_eq!(
            selected_weekly
                .iter()
                .map(|period| period.total_sessions)
                .sum::<u32>(),
            1
        );
        assert_eq!(
            selected_weekly
                .iter()
                .map(|period| period.total_rounds)
                .sum::<u32>(),
            1
        );
        assert_eq!(
            selected_weekly
                .iter()
                .map(|period| period.total_token_usage.total_tokens)
                .sum::<u64>(),
            10
        );
        assert!(selected_weekly
            .iter()
            .all(|period| !period.model_breakdown.contains_key("model-b")));

        let selected_monthly = service
            .monthly_for_model(2, Some(current_month_date), Some("model-a".to_string()))
            .await
            .expect("model-filtered monthly metrics");
        assert_eq!(selected_monthly.len(), 1);
        assert_eq!(selected_monthly[0].total_sessions, 1);
        assert_eq!(selected_monthly[0].total_rounds, 1);
        assert_eq!(selected_monthly[0].total_token_usage.total_tokens, 10);
    }
}
