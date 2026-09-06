//! An external adapter deliberately implementing only the pre-batch required API.
use async_trait::async_trait;
use bamboo_infrastructure::metrics::storage::{
    MetricsError, MetricsMutation, MetricsResult, MetricsStorage, ToolCallCompletion,
};
use bamboo_infrastructure::metrics::types::*;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct ExistingAdapter {
    calls: Mutex<Vec<MetricsMutation>>,
}
impl ExistingAdapter {
    fn record(&self, mutation: MetricsMutation) -> MetricsResult<()> {
        let fail = matches!(
            &mutation,
            MetricsMutation::SessionMessageCount {
                message_count: 777,
                ..
            }
        );
        self.calls.lock().unwrap().push(mutation);
        if fail {
            Err(MetricsError::InvalidData(
                "one deliberately failed call".into(),
            ))
        } else {
            Ok(())
        }
    }
}

// Neither apply_batch nor record_prompt_memory_exposure is overridden. A new
// required method or a Self: Sized restriction would break this external crate.
#[async_trait]
impl MetricsStorage for ExistingAdapter {
    async fn init(&self) -> MetricsResult<()> {
        Ok(())
    }
    async fn upsert_session_start(
        &self,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::SessionStarted {
            session_id: session_id.to_string(),
            model: model.to_string(),
            started_at,
        })
    }
    async fn update_session_message_count(
        &self,
        session_id: &str,
        message_count: u32,
        updated_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::SessionMessageCount {
            session_id: session_id.to_string(),
            message_count,
            updated_at,
        })
    }
    async fn complete_session(
        &self,
        session_id: &str,
        status: SessionStatus,
        completed_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::SessionCompleted {
            session_id: session_id.to_string(),
            status,
            completed_at,
        })
    }
    async fn insert_round_start(
        &self,
        round_id: &str,
        session_id: &str,
        model: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::RoundStarted {
            round_id: round_id.to_string(),
            session_id: session_id.to_string(),
            model: model.to_string(),
            started_at,
        })
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
        self.record(MetricsMutation::RoundCompleted {
            round_id: round_id.to_string(),
            completed_at,
            status,
            usage,
            prompt_cached_tool_outputs,
            prompt_cached_tool_tokens_saved,
            error,
        })
    }
    async fn record_round_compression(
        &self,
        _round_id: &str,
        _compressed_at: DateTime<Utc>,
        _tokens_saved: u32,
    ) -> MetricsResult<()> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn insert_tool_start(
        &self,
        tool_call_id: &str,
        round_id: &str,
        session_id: &str,
        tool_name: &str,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::ToolStarted {
            tool_call_id: tool_call_id.to_string(),
            round_id: round_id.to_string(),
            session_id: session_id.to_string(),
            tool_name: tool_name.to_string(),
            started_at,
        })
    }
    async fn complete_tool_call(
        &self,
        tool_call_id: &str,
        completion: ToolCallCompletion,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::ToolCompleted {
            tool_call_id: tool_call_id.to_string(),
            completion,
        })
    }
    async fn insert_forward_start(
        &self,
        forward_id: &str,
        endpoint: &str,
        model: &str,
        is_stream: bool,
        started_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::ForwardStarted {
            forward_id: forward_id.to_string(),
            endpoint: endpoint.to_string(),
            model: model.to_string(),
            is_stream,
            started_at,
        })
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
        self.record(MetricsMutation::ForwardCompleted {
            forward_id: forward_id.to_string(),
            completed_at,
            status_code,
            status,
            usage,
            token_details,
            error,
        })
    }
    async fn forward_summary(
        &self,
        _filter: ForwardMetricsFilter,
    ) -> MetricsResult<ForwardMetricsSummary> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn forward_by_endpoint(
        &self,
        _filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardEndpointMetrics>> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn forward_requests(
        &self,
        _filter: ForwardMetricsFilter,
    ) -> MetricsResult<Vec<ForwardRequestMetrics>> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn forward_daily_metrics(
        &self,
        _days: u32,
        _end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn summary(&self, _filter: MetricsDateFilter) -> MetricsResult<MetricsSummary> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn by_model(&self, _filter: MetricsDateFilter) -> MetricsResult<Vec<ModelMetrics>> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn sessions(&self, _filter: SessionMetricsFilter) -> MetricsResult<Vec<SessionMetrics>> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn session_detail(&self, _session_id: &str) -> MetricsResult<Option<SessionDetail>> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn increment_execute_sync_mismatch(
        &self,
        reason: &str,
        occurred_at: DateTime<Utc>,
    ) -> MetricsResult<()> {
        self.record(MetricsMutation::ExecuteSyncMismatch {
            reason: reason.to_string(),
            occurred_at,
        })
    }
    async fn daily_metrics(
        &self,
        _days: u32,
        _end_date: Option<NaiveDate>,
    ) -> MetricsResult<Vec<DailyMetrics>> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn prune_rounds_before(&self, _cutoff: DateTime<Utc>) -> MetricsResult<u64> {
        unreachable!("not part of this write-only external adapter fixture")
    }
    async fn reconcile_stale_executions(
        &self,
        _active_session_ids: &[String],
        _awaiting_response_session_ids: &[String],
    ) -> MetricsResult<()> {
        unreachable!("not part of this write-only external adapter fixture")
    }
}

#[tokio::test]
async fn existing_trait_object_receives_all_raw_payloads_and_continues_after_error() {
    let adapter = Arc::new(ExistingAdapter::default());
    let storage: Arc<dyn MetricsStorage> = adapter.clone();
    storage.init().await.unwrap();
    let when = Utc.with_ymd_and_hms(2026, 9, 5, 10, 0, 0).unwrap();
    let usage = TokenUsage {
        prompt_tokens: u64::MAX,
        completion_tokens: u64::MAX - 1,
        total_tokens: u64::MAX - 2,
    };
    let details = ForwardTokenDetails {
        cache_creation_input_tokens: Some(u64::MAX),
        cache_read_input_tokens: Some(u64::MAX - 1),
        cache_write_input_tokens: Some(u64::MAX - 2),
        reasoning_output_tokens: Some(u64::MAX - 3),
    };
    let mutations = vec![
        MetricsMutation::SessionStarted {
            session_id: "session".into(),
            model: "session-model".into(),
            started_at: when,
        },
        MetricsMutation::SessionMessageCount {
            session_id: "session".into(),
            message_count: 777,
            updated_at: when,
        },
        MetricsMutation::RoundStarted {
            round_id: "round".into(),
            session_id: "session".into(),
            model: "round-model".into(),
            started_at: when,
        },
        MetricsMutation::RoundCompleted {
            round_id: "round".into(),
            completed_at: when,
            status: RoundStatus::Error,
            usage,
            prompt_cached_tool_outputs: u32::MAX,
            prompt_cached_tool_tokens_saved: u32::MAX - 1,
            error: Some("round-error".into()),
        },
        MetricsMutation::ToolStarted {
            tool_call_id: "tool".into(),
            round_id: "round".into(),
            session_id: "session".into(),
            tool_name: "tool-name".into(),
            started_at: when,
        },
        MetricsMutation::ToolCompleted {
            tool_call_id: "tool".into(),
            completion: ToolCallCompletion {
                completed_at: when,
                success: false,
                error: Some("tool-error".into()),
            },
        },
        MetricsMutation::ForwardStarted {
            forward_id: "forward".into(),
            endpoint: "endpoint".into(),
            model: "forward-model".into(),
            is_stream: true,
            started_at: when,
        },
        MetricsMutation::ForwardCompleted {
            forward_id: "forward".into(),
            completed_at: when,
            status_code: Some(503),
            status: ForwardStatus::Error,
            usage: Some(usage),
            token_details: Some(details),
            error: Some("forward-error".into()),
        },
        MetricsMutation::ForwardCompleted {
            forward_id: "empty-forward".into(),
            completed_at: when,
            status_code: None,
            status: ForwardStatus::Success,
            usage: None,
            token_details: None,
            error: None,
        },
        MetricsMutation::ExecuteSyncMismatch {
            reason: "additive".into(),
            occurred_at: when,
        },
        MetricsMutation::ExecuteSyncMismatch {
            reason: "additive".into(),
            occurred_at: when,
        },
        MetricsMutation::SessionCompleted {
            session_id: "session".into(),
            status: SessionStatus::Completed,
            completed_at: when,
        },
    ];
    let results = storage.apply_batch(mutations.clone()).await.unwrap();
    assert_eq!(results.len(), mutations.len());
    for (index, result) in results.iter().enumerate() {
        if index == 1 {
            assert!(matches!(result, Err(MetricsError::InvalidData(_))));
        } else {
            assert!(result.is_ok(), "call {index}: {result:?}");
        }
    }
    // Debug includes every field of this non-Serialize command type. This is
    // an exact payload/order assertion, not a snapshot of a formatting API.
    assert_eq!(
        format!("{:?}", *adapter.calls.lock().unwrap()),
        format!("{mutations:?}")
    );
    assert!(storage.apply_batch(Vec::new()).await.unwrap().is_empty());
    assert_eq!(adapter.calls.lock().unwrap().len(), mutations.len());

    let observation = PromptMemoryExposureObservation {
        schema_version: 1,
        round_id: "round".into(),
        session_id: "session".into(),
        project_id: None,
        observed_at: when,
        recall_enabled: false,
        query_present: false,
        recall_outcome: PromptMemoryRecallOutcome::Disabled,
        all_compact_exposed_count: 0,
        project_exposed_count: 0,
        out_of_project_only: false,
        compact_section_chars: 0,
        project_items: Vec::new(),
    };
    assert!(matches!(
        storage.record_prompt_memory_exposure(&observation).await,
        Err(MetricsError::InvalidData(_))
    ));
    let suffix = storage
        .apply_batch(vec![MetricsMutation::SessionMessageCount {
            session_id: "session".into(),
            message_count: 42,
            updated_at: when,
        }])
        .await
        .unwrap();
    assert_eq!(suffix.len(), 1);
    assert!(suffix[0].is_ok());
    let calls = adapter.calls.lock().unwrap();
    assert_eq!(calls.len(), mutations.len() + 1);
    assert!(matches!(
        calls.last(),
        Some(MetricsMutation::SessionMessageCount {
            message_count: 42,
            ..
        })
    ));
}
