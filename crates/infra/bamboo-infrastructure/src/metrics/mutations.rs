use super::storage::{MetricsResult, MetricsStorage, ToolCallCompletion};
use super::types::{ForwardStatus, ForwardTokenDetails, RoundStatus, SessionStatus, TokenUsage};
use chrono::{DateTime, Utc};

/// Maximum ready ordinary mutations dispatched in one connection segment.
pub const MAX_METRICS_BATCH_SIZE: usize = 32;

/// Owned ordinary metrics writes. Payloads remain raw for custom storage backends.
/// Retention, compression and prompt-memory observations retain singleton paths.
#[derive(Debug, Clone)]
pub enum MetricsMutation {
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
        prompt_cached_tool_tokens_saved: u32,
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
        status: ForwardStatus,
        usage: Option<TokenUsage>,
        token_details: Option<ForwardTokenDetails>,
        error: Option<String>,
    },
}

impl MetricsMutation {
    pub(crate) async fn apply<S: MetricsStorage + ?Sized>(self, storage: &S) -> MetricsResult<()> {
        match self {
            Self::SessionStarted {
                session_id,
                model,
                started_at,
            } => {
                storage
                    .upsert_session_start(&session_id, &model, started_at)
                    .await
            }
            Self::SessionMessageCount {
                session_id,
                message_count,
                updated_at,
            } => {
                storage
                    .update_session_message_count(&session_id, message_count, updated_at)
                    .await
            }
            Self::SessionCompleted {
                session_id,
                status,
                completed_at,
            } => {
                storage
                    .complete_session(&session_id, status, completed_at)
                    .await
            }
            Self::RoundStarted {
                round_id,
                session_id,
                model,
                started_at,
            } => {
                storage
                    .insert_round_start(&round_id, &session_id, &model, started_at)
                    .await
            }
            Self::RoundCompleted {
                round_id,
                completed_at,
                status,
                usage,
                prompt_cached_tool_outputs,
                prompt_cached_tool_tokens_saved,
                error,
            } => {
                storage
                    .complete_round(
                        &round_id,
                        completed_at,
                        status,
                        usage,
                        prompt_cached_tool_outputs,
                        prompt_cached_tool_tokens_saved,
                        error,
                    )
                    .await
            }
            Self::ToolStarted {
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
            Self::ToolCompleted {
                tool_call_id,
                completion,
            } => storage.complete_tool_call(&tool_call_id, completion).await,
            Self::ExecuteSyncMismatch {
                reason,
                occurred_at,
            } => {
                storage
                    .increment_execute_sync_mismatch(&reason, occurred_at)
                    .await
            }
            Self::ForwardStarted {
                forward_id,
                endpoint,
                model,
                is_stream,
                started_at,
            } => {
                storage
                    .insert_forward_start(&forward_id, &endpoint, &model, is_stream, started_at)
                    .await
            }
            Self::ForwardCompleted {
                forward_id,
                completed_at,
                status_code,
                status,
                usage,
                token_details,
                error,
            } => {
                storage
                    .complete_forward(
                        &forward_id,
                        completed_at,
                        status_code,
                        status,
                        usage,
                        token_details,
                        error,
                    )
                    .await
            }
        }
    }
}
