use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use log::{error, info, warn};
use tokio::sync::mpsc;

use crate::bus::MetricsBus;
use crate::events::{ChatEvent, ForwardEvent, MetricsEvent, SystemEvent};
use crate::storage::{MetricsStorage, ToolCallCompletion};
use crate::types::ForwardStatus;

/// Worker that consumes metrics events from the bus and writes them to storage
pub struct MetricsWorker {
    storage: Arc<dyn MetricsStorage>,
    running: Arc<AtomicBool>,
}

impl MetricsWorker {
    /// Create a new metrics worker with the given storage backend
    pub fn new(storage: Arc<dyn MetricsStorage>) -> Self {
        Self {
            storage,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Spawn the worker task
    ///
    /// Returns a handle to stop the worker
    pub fn spawn(
        &self,
        mut receiver: mpsc::Receiver<MetricsEvent>,
        bus: MetricsBus,
    ) -> Arc<AtomicBool> {
        let storage = Arc::clone(&self.storage);
        let running = Arc::clone(&self.running);
        running.store(true, Ordering::SeqCst);

        let running_clone = Arc::clone(&running);

        tokio::spawn(async move {
            info!("MetricsWorker started");
            bus.emit(MetricsEvent::System(SystemEvent::WorkerStarted));

            while running.load(Ordering::SeqCst) {
                match receiver.recv().await {
                    Some(event) => {
                        if let Err(e) = Self::handle_event(&storage, &event).await {
                            warn!("Failed to handle metrics event: {}", e);
                            bus.emit(MetricsEvent::System(SystemEvent::StorageError {
                                error: e.to_string(),
                                event_type: event_type_name(&event),
                            }));
                        }
                    }
                    None => {
                        info!("MetricsWorker channel closed");
                        break;
                    }
                }
            }

            info!("MetricsWorker stopped");
            bus.emit(MetricsEvent::System(SystemEvent::WorkerStopped));
        });

        running_clone
    }

    /// Handle a single metrics event
    async fn handle_event(storage: &Arc<dyn MetricsStorage>, event: &MetricsEvent) -> anyhow::Result<()> {
        match event {
            MetricsEvent::Chat(chat_event) => Self::handle_chat_event(storage, chat_event).await,
            MetricsEvent::Forward(forward_event) => Self::handle_forward_event(storage, forward_event).await,
            MetricsEvent::System(system_event) => {
                // Just log system events
                match system_event {
                    SystemEvent::WorkerStarted => info!("System: WorkerStarted"),
                    SystemEvent::WorkerStopped => info!("System: WorkerStopped"),
                    SystemEvent::MetricsDropped { count, reason } => {
                        warn!("System: MetricsDropped - {} events, reason: {}", count, reason);
                    }
                    SystemEvent::StorageError { error, event_type } => {
                        error!("System: StorageError for {} - {}", event_type, error);
                    }
                }
                Ok(())
            }
        }
    }

    /// Handle chat-related events
    async fn handle_chat_event(
        storage: &Arc<dyn MetricsStorage>,
        event: &ChatEvent,
    ) -> anyhow::Result<()> {
        match event {
            ChatEvent::SessionStarted {
                session_id,
                model,
                meta,
                ..
            } => {
                storage
                    .upsert_session_start(session_id, model, meta.occurred_at)
                    .await?;
                info!("Chat: SessionStarted - {}", session_id);
            }
            ChatEvent::SessionCompleted {
                session_id,
                status,
                meta,
            } => {
                storage
                    .complete_session(session_id, *status, meta.occurred_at)
                    .await?;
                info!("Chat: SessionCompleted - {} ({:?})", session_id, status);
            }
            ChatEvent::RoundStarted {
                round_id,
                session_id,
                model,
                meta,
            } => {
                storage
                    .insert_round_start(round_id, session_id, model, meta.occurred_at)
                    .await?;
                info!("Chat: RoundStarted - {} in session {}", round_id, session_id);
            }
            ChatEvent::RoundCompleted {
                round_id,
                status,
                usage,
                error,
                meta,
                ..
            } => {
                storage
                    .complete_round(
                        round_id,
                        meta.occurred_at,
                        *status,
                        *usage,
                        error.clone(),
                    )
                    .await?;
                info!(
                    "Chat: RoundCompleted - {} ({:?}) - {} tokens",
                    round_id,
                    status,
                    usage.total_tokens
                );
            }
            ChatEvent::ToolCalled {
                tool_call_id,
                round_id,
                session_id,
                tool_name,
                latency_ms,
                success,
                meta,
            } => {
                // Insert tool start first
                storage
                    .insert_tool_start(
                        tool_call_id,
                        round_id,
                        session_id,
                        tool_name,
                        meta.occurred_at,
                    )
                    .await?;

                // Then complete it
                let completion = ToolCallCompletion {
                    completed_at: meta.occurred_at,
                    success: *success,
                    error: if *success {
                        None
                    } else {
                        Some(format!("Tool failed after {}ms", latency_ms))
                    },
                };
                storage.complete_tool_call(tool_call_id, completion).await?;
                info!(
                    "Chat: ToolCalled - {} ({}) - {}ms",
                    tool_name,
                    if *success { "success" } else { "failed" },
                    latency_ms
                );
            }
            ChatEvent::MessageCountUpdated {
                session_id,
                message_count,
                meta,
            } => {
                storage
                    .update_session_message_count(session_id, *message_count, meta.occurred_at)
                    .await?;
            }
        }
        Ok(())
    }

    /// Handle forward-related events
    async fn handle_forward_event(
        storage: &Arc<dyn MetricsStorage>,
        event: &ForwardEvent,
    ) -> anyhow::Result<()> {
        match event {
            ForwardEvent::RequestStarted {
                request_id,
                endpoint,
                model,
                is_stream,
                meta,
            } => {
                storage
                    .insert_forward_start(request_id, endpoint, model, *is_stream, meta.occurred_at)
                    .await?;
                info!(
                    "Forward: RequestStarted - {} to {} (stream: {})",
                    request_id, endpoint, is_stream
                );
            }
            ForwardEvent::RequestCompleted {
                request_id,
                status_code,
                status,
                usage,
                latency_ms,
                error,
                meta,
            } => {
                storage
                    .complete_forward(
                        request_id,
                        meta.occurred_at,
                        Some(*status_code),
                        *status,
                        *usage,
                        error.clone(),
                    )
                    .await?;
                info!(
                    "Forward: RequestCompleted - {} ({} {}) - {}ms - {} tokens",
                    request_id,
                    status_code,
                    match status {
                        ForwardStatus::Success => "success",
                        ForwardStatus::Error => "error",
                    },
                    latency_ms,
                    usage.as_ref().map(|u| u.total_tokens).unwrap_or(0)
                );
            }
        }
        Ok(())
    }

    /// Stop the worker gracefully
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

fn event_type_name(event: &MetricsEvent) -> String {
    match event {
        MetricsEvent::Chat(e) => match e {
            ChatEvent::SessionStarted { .. } => "Chat::SessionStarted",
            ChatEvent::SessionCompleted { .. } => "Chat::SessionCompleted",
            ChatEvent::RoundStarted { .. } => "Chat::RoundStarted",
            ChatEvent::RoundCompleted { .. } => "Chat::RoundCompleted",
            ChatEvent::ToolCalled { .. } => "Chat::ToolCalled",
            ChatEvent::MessageCountUpdated { .. } => "Chat::MessageCountUpdated",
        }
        .to_string(),
        MetricsEvent::Forward(e) => match e {
            ForwardEvent::RequestStarted { .. } => "Forward::RequestStarted",
            ForwardEvent::RequestCompleted { .. } => "Forward::RequestCompleted",
        }
        .to_string(),
        MetricsEvent::System(e) => match e {
            SystemEvent::WorkerStarted => "System::WorkerStarted",
            SystemEvent::WorkerStopped => "System::WorkerStopped",
            SystemEvent::MetricsDropped { .. } => "System::MetricsDropped",
            SystemEvent::StorageError { .. } => "System::StorageError",
        }
        .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventMeta;
    use crate::types::{RoundStatus, TokenUsage};
    use std::path::PathBuf;
    use tempfile::tempdir;

    async fn create_test_storage() -> (Arc<dyn MetricsStorage>, PathBuf) {
        let dir = tempdir().expect("temp dir");
        let db_path = dir.path().join("metrics.db");
        // Keep temp dir alive for the test
        std::mem::forget(dir);
        let storage = Arc::new(crate::storage::SqliteMetricsStorage::new(&db_path));
        storage.init().await.expect("init storage");
        (storage, db_path)
    }

    #[tokio::test]
    async fn test_worker_handles_chat_events() {
        let (storage, _db_path) = create_test_storage().await;
        let worker = MetricsWorker::new(Arc::clone(&storage));
        let (bus, rx) = MetricsBus::new(100);

        let running = worker.spawn(rx, bus.clone());

        // Emit events
        bus.emit(MetricsEvent::Chat(ChatEvent::SessionStarted {
            meta: EventMeta::new(),
            session_id: "test-session".to_string(),
            model: "gpt-4".to_string(),
        }));

        bus.emit(MetricsEvent::Chat(ChatEvent::RoundStarted {
            meta: EventMeta::new(),
            round_id: "test-round".to_string(),
            session_id: "test-session".to_string(),
            model: "gpt-4".to_string(),
        }));

        bus.emit(MetricsEvent::Chat(ChatEvent::RoundCompleted {
            meta: EventMeta::new(),
            round_id: "test-round".to_string(),
            session_id: "test-session".to_string(),
            status: RoundStatus::Success,
            usage: TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 20,
                total_tokens: 30,
            },
            latency_ms: 1000,
            error: None,
        }));

        // Wait for processing
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify data was written
        let summary = storage
            .summary(crate::types::MetricsDateFilter::default())
            .await
            .expect("get summary");
        assert_eq!(summary.total_sessions, 1);

        // Stop worker
        running.store(false, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn test_worker_handles_forward_events() {
        let (storage, _db_path) = create_test_storage().await;
        let worker = MetricsWorker::new(Arc::clone(&storage));
        let (bus, rx) = MetricsBus::new(100);

        let running = worker.spawn(rx, bus.clone());

        // Emit forward events
        bus.emit(MetricsEvent::Forward(ForwardEvent::RequestStarted {
            meta: EventMeta::new(),
            request_id: "req-123".to_string(),
            endpoint: "openai.chat_completions".to_string(),
            model: "gpt-4".to_string(),
            is_stream: true,
        }));

        bus.emit(MetricsEvent::Forward(ForwardEvent::RequestCompleted {
            meta: EventMeta::new(),
            request_id: "req-123".to_string(),
            status_code: 200,
            status: ForwardStatus::Success,
            usage: Some(TokenUsage {
                prompt_tokens: 50,
                completion_tokens: 100,
                total_tokens: 150,
            }),
            latency_ms: 500,
            error: None,
        }));

        // Wait for processing
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify data was written
        let summary = storage
            .forward_summary(crate::types::ForwardMetricsFilter::default())
            .await
            .expect("get forward summary");
        assert_eq!(summary.total_requests, 1);

        // Stop worker
        running.store(false, Ordering::SeqCst);
    }
}
