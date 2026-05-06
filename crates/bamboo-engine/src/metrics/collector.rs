use std::sync::Arc;

use bamboo_agent_core::AgentEvent;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::mpsc;

use crate::metrics::storage::{MetricsStorage, ToolCallCompletion};
use crate::metrics::types::{RoundStatus, SessionStatus, TokenUsage};

#[derive(Debug)]
enum CollectorCommand {
    SessionStarted {
        session_id: String,
        model: String,
        started_at: DateTime<Utc>,
    },
    SessionMessageCount {
        session_id: String,
        message_count: u32,
        updated_at: DateTime<Utc>,
    },
    SessionCompleted {
        session_id: String,
        status: SessionStatus,
        completed_at: DateTime<Utc>,
    },
    RoundStarted {
        round_id: String,
        session_id: String,
        model: String,
        started_at: DateTime<Utc>,
    },
    RoundCompleted {
        round_id: String,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        prompt_cached_tool_outputs: u32,
        error: Option<String>,
    },
    ToolStarted {
        tool_call_id: String,
        round_id: String,
        session_id: String,
        tool_name: String,
        started_at: DateTime<Utc>,
    },
    ToolCompleted {
        tool_call_id: String,
        completion: ToolCallCompletion,
    },
    ExecuteSyncMismatch {
        reason: String,
        occurred_at: DateTime<Utc>,
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
    ForwardStarted {
        forward_id: String,
        endpoint: String,
        model: String,
        is_stream: bool,
        started_at: DateTime<Utc>,
    },
    ForwardCompleted {
        forward_id: String,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: crate::metrics::types::ForwardStatus,
        usage: Option<TokenUsage>,
        error: Option<String>,
    },
    Prune {
        cutoff: DateTime<Utc>,
    },
}

#[derive(Clone)]
pub struct MetricsCollector {
    tx: mpsc::UnboundedSender<CollectorCommand>,
}

impl MetricsCollector {
    pub fn spawn(storage: Arc<dyn MetricsStorage>, retention_days: u32) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<CollectorCommand>();

        tokio::spawn(async move {
            if let Err(error) = storage.init().await {
                tracing::error!("metrics storage initialization failed: {}", error);
            }

            while let Some(command) = rx.recv().await {
                let outcome = match command {
                    CollectorCommand::SessionStarted {
                        session_id,
                        model,
                        started_at,
                    } => {
                        storage
                            .upsert_session_start(&session_id, &model, started_at)
                            .await
                    }
                    CollectorCommand::SessionMessageCount {
                        session_id,
                        message_count,
                        updated_at,
                    } => {
                        storage
                            .update_session_message_count(&session_id, message_count, updated_at)
                            .await
                    }
                    CollectorCommand::SessionCompleted {
                        session_id,
                        status,
                        completed_at,
                    } => {
                        storage
                            .complete_session(&session_id, status, completed_at)
                            .await
                    }
                    CollectorCommand::RoundStarted {
                        round_id,
                        session_id,
                        model,
                        started_at,
                    } => {
                        storage
                            .insert_round_start(&round_id, &session_id, &model, started_at)
                            .await
                    }
                    CollectorCommand::RoundCompleted {
                        round_id,
                        completed_at,
                        status,
                        usage,
                        prompt_cached_tool_outputs,
                        error,
                    } => {
                        storage
                            .complete_round(
                                &round_id,
                                completed_at,
                                status,
                                usage,
                                prompt_cached_tool_outputs,
                                error,
                            )
                            .await
                    }
                    CollectorCommand::ToolStarted {
                        tool_call_id,
                        round_id,
                        session_id,
                        tool_name,
                        started_at,
                    } => {
                        storage
                            .insert_tool_start(
                                &tool_call_id,
                                &round_id,
                                &session_id,
                                &tool_name,
                                started_at,
                            )
                            .await
                    }
                    CollectorCommand::ToolCompleted {
                        tool_call_id,
                        completion,
                    } => storage.complete_tool_call(&tool_call_id, completion).await,
                    CollectorCommand::ExecuteSyncMismatch {
                        reason,
                        occurred_at,
                    } => {
                        storage
                            .increment_execute_sync_mismatch(&reason, occurred_at)
                            .await
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
                    CollectorCommand::ForwardStarted {
                        forward_id,
                        endpoint,
                        model,
                        is_stream,
                        started_at,
                    } => {
                        storage
                            .insert_forward_start(
                                &forward_id,
                                &endpoint,
                                &model,
                                is_stream,
                                started_at,
                            )
                            .await
                    }
                    CollectorCommand::ForwardCompleted {
                        forward_id,
                        completed_at,
                        status_code,
                        status,
                        usage,
                        error,
                    } => {
                        storage
                            .complete_forward(
                                &forward_id,
                                completed_at,
                                status_code,
                                status,
                                usage,
                                error,
                            )
                            .await
                    }
                    CollectorCommand::Prune { cutoff } => {
                        storage.prune_rounds_before(cutoff).await.map(|_| ())
                    }
                };

                if let Err(error) = outcome {
                    tracing::warn!("metrics collector command failed: {}", error);
                }
            }
        });

        let collector = Self { tx };
        collector.schedule_prune(retention_days);
        collector
    }

    pub fn session_started(
        &self,
        session_id: impl Into<String>,
        model: impl Into<String>,
        started_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::SessionStarted {
            session_id: session_id.into(),
            model: model.into(),
            started_at,
        });
    }

    pub fn session_message_count(
        &self,
        session_id: impl Into<String>,
        message_count: u32,
        updated_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::SessionMessageCount {
            session_id: session_id.into(),
            message_count,
            updated_at,
        });
    }

    pub fn session_completed(
        &self,
        session_id: impl Into<String>,
        status: SessionStatus,
        completed_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::SessionCompleted {
            session_id: session_id.into(),
            status,
            completed_at,
        });
    }

    pub fn round_started(
        &self,
        round_id: impl Into<String>,
        session_id: impl Into<String>,
        model: impl Into<String>,
        started_at: DateTime<Utc>,
    ) {
        let _ = self.tx.send(CollectorCommand::RoundStarted {
            round_id: round_id.into(),
            session_id: session_id.into(),
            model: model.into(),
            started_at,
        });
    }

    pub fn round_completed(
        &self,
        round_id: impl Into<String>,
        completed_at: DateTime<Utc>,
        status: RoundStatus,
        usage: TokenUsage,
        prompt_cached_tool_outputs: u32,
        error: Option<String>,
    ) {
        let _ = self.tx.send(CollectorCommand::RoundCompleted {
            round_id: round_id.into(),
            completed_at,
            status,
            usage,
            prompt_cached_tool_outputs,
            error,
        });
    }

    pub fn record_agent_event(&self, session_id: &str, round_id: &str, event: &AgentEvent) {
        match event {
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                ..
            } => {
                let _ = self.tx.send(CollectorCommand::ToolStarted {
                    tool_call_id: tool_call_id.clone(),
                    round_id: round_id.to_string(),
                    session_id: session_id.to_string(),
                    tool_name: tool_name.clone(),
                    started_at: Utc::now(),
                });
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => {
                let _ = self.tx.send(CollectorCommand::ToolCompleted {
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
                });
            }
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => {
                let _ = self.tx.send(CollectorCommand::ToolCompleted {
                    tool_call_id: tool_call_id.clone(),
                    completion: ToolCallCompletion {
                        completed_at: Utc::now(),
                        success: false,
                        error: Some(error.clone()),
                    },
                });
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
        let _ = self.tx.send(CollectorCommand::ForwardStarted {
            forward_id: forward_id.into(),
            endpoint: endpoint.into(),
            model: model.into(),
            is_stream,
            started_at,
        });
    }

    pub fn forward_completed(
        &self,
        forward_id: impl Into<String>,
        completed_at: DateTime<Utc>,
        status_code: Option<u16>,
        status: crate::metrics::types::ForwardStatus,
        usage: Option<TokenUsage>,
        error: Option<String>,
    ) {
        let _ = self.tx.send(CollectorCommand::ForwardCompleted {
            forward_id: forward_id.into(),
            completed_at,
            status_code,
            status,
            usage,
            error,
        });
    }

    pub fn execute_sync_mismatch(&self, reason: impl Into<String>, occurred_at: DateTime<Utc>) {
        let _ = self.tx.send(CollectorCommand::ExecuteSyncMismatch {
            reason: reason.into(),
            occurred_at,
        });
    }

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

    fn schedule_prune(&self, retention_days: u32) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 60 * 60));
            loop {
                interval.tick().await;
                let cutoff = Utc::now() - Duration::days(i64::from(retention_days));
                let _ = tx.send(CollectorCommand::Prune { cutoff });
            }
        });
    }
}
