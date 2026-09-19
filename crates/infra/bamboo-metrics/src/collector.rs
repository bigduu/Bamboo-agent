use std::sync::Arc;

use bamboo_agent_core::AgentEvent;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::mpsc;

use crate::storage::{MetricsMutation, MetricsStorage, ToolCallCompletion, MAX_METRICS_BATCH_SIZE};
use crate::types::{
    ForwardTokenDetails, PromptMemoryExposureObservation, RoundStatus, SessionStatus, TokenUsage,
};

#[derive(Debug)]
enum CollectorCommand {
    Mutation(MetricsMutation),
    PromptMemoryExposure {
        observation: PromptMemoryExposureObservation,
    },
    ContextCompressed {
        session_id: String,
        round_id: String,
        messages_compressed: u32,
        tokens_saved: u32,
        usage_before_percent: f64,
        usage_after_percent: f64,
        trigger_type: String,
        latency_ms: u64,
    },
    Prune {
        cutoff: DateTime<Utc>,
    },
}

// Shared only by external collector clones. The scheduler itself holds no
// owner reference, so dropping the last clone cancels its timer without a cycle.
struct PruneScheduler {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for PruneScheduler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
pub struct MetricsCollector {
    tx: mpsc::UnboundedSender<CollectorCommand>,
    _scheduler: Arc<PruneScheduler>,
    #[cfg(test)]
    received_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl MetricsCollector {
    pub fn spawn(storage: Arc<dyn MetricsStorage>, retention_days: u32) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<CollectorCommand>();
        #[cfg(test)]
        let received_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        #[cfg(test)]
        let consumer_received_count = received_count.clone();

        // Do not abort the consumer on owner release: closing the producers
        // lets it finish initialization and drain accepted commands in order.
        let consumer = tokio::spawn(async move {
            if let Err(error) = storage.init().await {
                tracing::error!("metrics storage initialization failed: {}", error);
            }

            let mut buffer = Vec::with_capacity(MAX_METRICS_BATCH_SIZE);
            loop {
                let received = rx.recv_many(&mut buffer, MAX_METRICS_BATCH_SIZE).await;
                if received == 0 {
                    break;
                }
                #[cfg(test)]
                consumer_received_count.fetch_add(received, std::sync::atomic::Ordering::SeqCst);
                let mut pending = Vec::with_capacity(buffer.len());
                for command in buffer.drain(..) {
                    if let CollectorCommand::Mutation(mutation) = command {
                        pending.push(mutation);
                        continue;
                    }
                    Self::flush_mutations(storage.as_ref(), std::mem::take(&mut pending)).await;
                    let outcome = match command {
                        CollectorCommand::PromptMemoryExposure { observation } => {
                            storage.record_prompt_memory_exposure(&observation).await
                        }
                        CollectorCommand::ContextCompressed {
                            session_id,
                            round_id,
                            messages_compressed,
                            tokens_saved,
                            usage_before_percent,
                            usage_after_percent,
                            trigger_type,
                            latency_ms,
                        } => {
                            tracing::info!(
                            "[{}] metrics: context compressed — round={}, messages={}, tokens_saved={}, before={:.1}%, after={:.1}%, trigger={}, latency={}ms",
                            session_id, round_id, messages_compressed, tokens_saved,
                            usage_before_percent, usage_after_percent, trigger_type, latency_ms,
                        );
                            storage
                                .record_round_compression(&round_id, Utc::now(), tokens_saved)
                                .await
                        }
                        CollectorCommand::Prune { cutoff } => {
                            storage.prune_rounds_before(cutoff).await.map(|_| ())
                        }
                        CollectorCommand::Mutation(_) => {
                            unreachable!("ordinary mutations handled above")
                        }
                    };
                    if let Err(error) = outcome {
                        tracing::warn!("metrics collector command failed: {}", error);
                    }
                }
                Self::flush_mutations(storage.as_ref(), pending).await;
            }
        });

        let scheduler = Self::schedule_prune(tx.downgrade(), consumer, retention_days);
        Self {
            tx,
            _scheduler: Arc::new(scheduler),
            #[cfg(test)]
            received_count,
        }
    }

    async fn flush_mutations(storage: &dyn MetricsStorage, mutations: Vec<MetricsMutation>) {
        if mutations.is_empty() {
            return;
        }
        let expected = mutations.len();
        match storage.apply_batch(mutations).await {
            Ok(results) => {
                if results.len() != expected {
                    tracing::error!(
                        expected,
                        actual = results.len(),
                        "metrics backend violated batch result count; no replay is safe"
                    );
                }
                for result in results {
                    if let Err(error) = result {
                        tracing::warn!("metrics collector command failed: {}", error);
                    }
                }
            }
            Err(error) => tracing::warn!(
                "metrics batch task failed; committed prefix cannot be replayed: {}",
                error
            ),
        }
    }

    pub fn session_started(
        &self,
        session_id: impl Into<String>,
        model: impl Into<String>,
        started_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::Mutation(
            MetricsMutation::SessionStarted {
                session_id: session_id.into(),
                model: model.into(),
                started_at,
            },
        ));
    }

    pub fn session_message_count(
        &self,
        session_id: impl Into<String>,
        message_count: u32,
        updated_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::Mutation(
            MetricsMutation::SessionMessageCount {
                session_id: session_id.into(),
                message_count,
                updated_at,
            },
        ));
    }

    pub fn session_completed(
        &self,
        session_id: impl Into<String>,
        status: SessionStatus,
        completed_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::Mutation(
            MetricsMutation::SessionCompleted {
                session_id: session_id.into(),
                status,
                completed_at,
            },
        ));
    }

    pub fn round_started(
        &self,
        round_id: impl Into<String>,
        session_id: impl Into<String>,
        model: impl Into<String>,
        started_at: DateTime<Utc>,
    ) {
        let _ = self
            .tx
            .send(CollectorCommand::Mutation(MetricsMutation::RoundStarted {
                round_id: round_id.into(),
                session_id: session_id.into(),
                model: model.into(),
                started_at,
            }));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn round_completed(
        &self,
        round_id: impl Into<String>,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        prompt_cached_tool_outputs: u32,
        prompt_cached_tool_tokens_saved: u32,
        error: Option<String>,
    ) {
        let _ = self.tx.send(CollectorCommand::Mutation(
            MetricsMutation::RoundCompleted {
                round_id: round_id.into(),
                completed_at,
                status,
                usage,
                prompt_cached_tool_outputs,
                prompt_cached_tool_tokens_saved,
                error,
            },
        ));
    }

    /// Queues a host-observed compact-memory prompt exposure.
    ///
    /// This is intentionally best-effort like the rest of the live metrics
    /// collector. It neither blocks provider execution nor creates a durable
    /// delivery acknowledgement protocol.
    pub fn prompt_memory_exposure(&self, observation: PromptMemoryExposureObservation) {
        let _ = self
            .tx
            .send(CollectorCommand::PromptMemoryExposure { observation });
    }

    pub fn record_agent_event(&self, session_id: &str, round_id: &str, event: &AgentEvent) {
        match event {
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                ..
            } => {
                let _ = self
                    .tx
                    .send(CollectorCommand::Mutation(MetricsMutation::ToolStarted {
                        tool_call_id: tool_call_id.clone(),
                        round_id: round_id.to_string(),
                        session_id: session_id.to_string(),
                        tool_name: tool_name.clone(),
                        started_at: Utc::now(),
                    }));
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => {
                let _ = self
                    .tx
                    .send(CollectorCommand::Mutation(MetricsMutation::ToolCompleted {
                        tool_call_id: tool_call_id.clone(),
                        completion: ToolCallCompletion {
                            completed_at: Utc::now(),
                            success: result.success,
                            error: if result.success {
                                None
                            } else {
                                Some(result.result.clone())
                            },
                        },
                    }));
            }
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => {
                let _ = self
                    .tx
                    .send(CollectorCommand::Mutation(MetricsMutation::ToolCompleted {
                        tool_call_id: tool_call_id.clone(),
                        completion: ToolCallCompletion {
                            completed_at: Utc::now(),
                            success: false,
                            error: Some(error.clone()),
                        },
                    }));
            }
            _ => {}
        }
    }

    pub fn forward_started(
        &self,
        forward_id: impl Into<String>,
        endpoint: impl Into<String>,
        model: impl Into<String>,
        is_stream: bool,
        started_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::Mutation(
            MetricsMutation::ForwardStarted {
                forward_id: forward_id.into(),
                endpoint: endpoint.into(),
                model: model.into(),
                is_stream,
                started_at,
            },
        ));
    }

    pub fn forward_completed(
        &self,
        forward_id: impl Into<String>,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: crate::types::ForwardStatus,
        usage: Option<TokenUsage>,
        error: Option<String>,
    ) {
        self.forward_completed_with_details(
            forward_id,
            completed_at,
            status_code,
            status,
            usage,
            None,
            error,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn forward_completed_with_details(
        &self,
        forward_id: impl Into<String>,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: crate::types::ForwardStatus,
        usage: Option<TokenUsage>,
        token_details: Option<ForwardTokenDetails>,
        error: Option<String>,
    ) {
        let _ = self.tx.send(CollectorCommand::Mutation(
            MetricsMutation::ForwardCompleted {
                forward_id: forward_id.into(),
                completed_at,
                status_code,
                status,
                usage,
                token_details,
                error,
            },
        ));
    }

    pub fn execute_sync_mismatch(&self, reason: impl Into<String>, occurred_at: DateTime<Utc>) {
        let _ = self.tx.send(CollectorCommand::Mutation(
            MetricsMutation::ExecuteSyncMismatch {
                reason: reason.into(),
                occurred_at,
            },
        ));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn context_compressed(
        &self,
        session_id: impl Into<String>,
        round_id: impl Into<String>,
        messages_compressed: u32,
        tokens_saved: u32,
        usage_before_percent: f64,
        usage_after_percent: f64,
        trigger_type: impl Into<String>,
        latency_ms: u64,
    ) {
        let _ = self.tx.send(CollectorCommand::ContextCompressed {
            session_id: session_id.into(),
            round_id: round_id.into(),
            messages_compressed,
            tokens_saved,
            usage_before_percent,
            usage_after_percent,
            trigger_type: trigger_type.into(),
            latency_ms,
        });
    }

    fn schedule_prune(
        sender: mpsc::WeakUnboundedSender<CollectorCommand>,
        mut consumer: tokio::task::JoinHandle<()>,
        retention_days: u32,
    ) -> PruneScheduler {
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 60 * 60));
            loop {
                // Observe consumer exit even between ticks. Aborting this
                // scheduler only drops the JoinHandle, detaching the consumer
                // so accepted work still drains after the final owner drops.
                tokio::select! {
                    _ = &mut consumer => break,
                    _ = interval.tick() => {}
                }
                // This temporary producer is dropped before the next timer
                // wait, allowing the consumer to close after external owners go.
                let Some(tx) = sender.upgrade() else {
                    break;
                };
                let cutoff = Utc::now() - Duration::days(i64::from(retention_days));
                if tx.send(CollectorCommand::Prune { cutoff }).is_err() {
                    break;
                }
            }
        });
        PruneScheduler { task }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;
    use std::time::Duration as StdDuration;

    use crate::storage::SqliteMetricsStorage;

    use super::*;

    async fn wait_for_reclaimed(
        storage: &Weak<SqliteMetricsStorage>,
        scheduler: &tokio::task::AbortHandle,
    ) {
        tokio::time::timeout(StdDuration::from_secs(5), async {
            while storage.strong_count() != 0 || !scheduler.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("collector should drain accepted commands and reclaim its tasks and storage");
        assert!(scheduler.is_finished());
        assert!(storage.upgrade().is_none());
    }

    #[tokio::test]
    async fn final_owner_drop_before_initialization_drains_normal_and_prune_commands_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.db");
        let storage = Arc::new(SqliteMetricsStorage::new(&path));
        let storage_weak = Arc::downgrade(&storage);
        let collector = MetricsCollector::spawn(storage, 90);
        let scheduler = collector._scheduler.task.abort_handle();
        let now = Utc::now();
        let old = now - Duration::days(40);

        // This current-thread test does not yield until after the final drop:
        // initialization is still pending and all these commands are queued.
        collector.session_started("drain", "model", old);
        collector.round_started("old-round", "drain", "model", old);
        collector.round_completed(
            "old-round",
            old,
            RoundStatus::Success,
            TokenUsage {
                prompt_tokens: 6,
                completion_tokens: 4,
                total_tokens: 10,
            },
            0,
            0,
            None,
        );
        collector
            .tx
            .send(CollectorCommand::Prune {
                cutoff: now - Duration::days(30),
            })
            .unwrap();
        collector.session_message_count("drain", 1, now);
        collector.round_started("new-round", "drain", "model", now);
        collector.round_completed(
            "new-round",
            now,
            RoundStatus::Success,
            TokenUsage {
                prompt_tokens: 12,
                completion_tokens: 8,
                total_tokens: 20,
            },
            0,
            0,
            None,
        );
        collector.session_message_count("drain", 42, now);
        collector.session_completed("drain", SessionStatus::Completed, now);
        drop(collector);

        wait_for_reclaimed(&storage_weak, &scheduler).await;
        // Reopen without initializing: the drained consumer had to create the
        // schema and persist the complete FIFO sequence before releasing it.
        let reopened = SqliteMetricsStorage::new(&path);
        let detail = reopened.session_detail("drain").await.unwrap().unwrap();
        assert_eq!(detail.rounds.len(), 1);
        assert_eq!(detail.rounds[0].round_id, "new-round");
        assert_eq!(detail.rounds[0].status, RoundStatus::Success);
        assert_eq!(detail.session.total_rounds, 1);
        assert_eq!(detail.session.total_token_usage.total_tokens, 20);
        assert_eq!(detail.session.message_count, 42);
        assert_eq!(detail.session.status, SessionStatus::Completed);
    }

    #[tokio::test]
    async fn remaining_clone_keeps_collection_and_periodic_retention_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.db");
        let storage = Arc::new(SqliteMetricsStorage::new(&path));
        storage.init().await.unwrap();
        let now = Utc::now();
        let old = now - Duration::days(100);
        storage
            .upsert_session_start("stale", "model", old)
            .await
            .unwrap();
        storage
            .insert_round_start("stale-round", "stale", "model", old)
            .await
            .unwrap();

        let storage_weak = Arc::downgrade(&storage);
        let collector = MetricsCollector::spawn(storage.clone(), 90);
        let remaining = collector.clone();
        let scheduler = collector._scheduler.task.abort_handle();
        drop(collector);
        remaining.session_started("kept", "model", now);
        remaining.session_message_count("kept", 23, now);

        // The interval's first tick is immediate, so observing its real prune
        // needs no clock manipulation or six-hour delay.
        tokio::time::timeout(StdDuration::from_secs(5), async {
            loop {
                let stale = storage.session_detail("stale").await.unwrap().unwrap();
                let kept = storage.session_detail("kept").await.unwrap();
                if stale.rounds.is_empty()
                    && kept.is_some_and(|detail| detail.session.message_count == 23)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remaining collector clone should accept events and retain its scheduler");
        assert!(!scheduler.is_finished());
        assert_eq!(remaining.tx.strong_count(), 1);

        drop(storage);
        drop(remaining);
        wait_for_reclaimed(&storage_weak, &scheduler).await;
    }

    #[tokio::test]
    async fn repeated_collectors_in_one_runtime_release_storage_and_scheduler() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.db");
        for iteration in 0..32 {
            let storage = Arc::new(SqliteMetricsStorage::new(&path));
            let storage_weak = Arc::downgrade(&storage);
            let collector = MetricsCollector::spawn(storage, 90);
            let scheduler = collector._scheduler.task.abort_handle();
            let session_id = format!("session-{iteration}");
            collector.session_started(&session_id, "model", Utc::now());
            collector.session_message_count(&session_id, iteration + 1, Utc::now());
            drop(collector);

            wait_for_reclaimed(&storage_weak, &scheduler).await;
        }

        let reopened = SqliteMetricsStorage::new(&path);
        for iteration in 0..32 {
            let detail = reopened
                .session_detail(&format!("session-{iteration}"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(detail.session.message_count, iteration + 1);
        }
    }

    #[tokio::test]
    async fn scheduler_stops_on_closed_receiver_while_producer_survives() {
        let (tx, rx) = mpsc::unbounded_channel();
        let consumer = tokio::spawn(async move { drop(rx) });
        let scheduler = MetricsCollector::schedule_prune(tx.downgrade(), consumer, 90);

        tokio::time::timeout(StdDuration::from_secs(5), async {
            while !scheduler.task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed receiver should terminate the scheduler");
        assert_eq!(tx.strong_count(), 1);
    }

    #[tokio::test]
    async fn scheduler_stops_between_ticks_when_consumer_exits_with_live_producer() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let first_tick = Arc::new(tokio::sync::Notify::new());
        let finish_consumer = Arc::new(tokio::sync::Notify::new());
        let consumer = tokio::spawn({
            let first_tick = first_tick.clone();
            let finish_consumer = finish_consumer.clone();
            async move {
                assert!(matches!(
                    rx.recv().await,
                    Some(CollectorCommand::Prune { .. })
                ));
                first_tick.notify_one();
                finish_consumer.notified().await;
            }
        });
        let scheduler = MetricsCollector::schedule_prune(tx.downgrade(), consumer, 90);

        tokio::time::timeout(StdDuration::from_secs(5), async {
            first_tick.notified().await;
            assert!(!scheduler.task.is_finished());
            finish_consumer.notify_one();
            while !scheduler.task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("consumer exit should stop the scheduler before its next six-hour tick");
        assert_eq!(tx.strong_count(), 1);
    }
}

#[cfg(test)]
mod batch_tests;
