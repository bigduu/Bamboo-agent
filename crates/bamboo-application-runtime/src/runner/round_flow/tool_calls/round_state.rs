use bamboo_application_agent::Session;
use bamboo_application_metrics::{MetricsCollector, RoundStatus as MetricsRoundStatus, TokenUsage};

use super::super::super::metrics_lifecycle;
use super::super::RoundFlowContext;

#[derive(Debug)]
pub(super) struct ToolCallsRoundState {
    awaiting_clarification: bool,
    round_status: MetricsRoundStatus,
    round_error: Option<String>,
}

impl Default for ToolCallsRoundState {
    fn default() -> Self {
        Self {
            awaiting_clarification: false,
            round_status: MetricsRoundStatus::Success,
            round_error: None,
        }
    }
}

impl ToolCallsRoundState {
    pub(super) fn apply_tool_execution_result(
        &mut self,
        tool_execution: crate::runner::tool_execution::RoundToolExecutionResult,
    ) {
        if tool_execution.round_status != MetricsRoundStatus::Success {
            self.round_status = tool_execution.round_status;
        }
        if let Some(error) = tool_execution.round_error {
            self.round_error = Some(error);
        }
        if tool_execution.awaiting_clarification {
            self.awaiting_clarification = true;
        }
    }

    pub(super) fn awaiting_clarification(&self) -> bool {
        self.awaiting_clarification
    }

    pub(super) fn log_round_complete_if_debug(
        &self,
        context: &RoundFlowContext<'_>,
        message_count: usize,
    ) {
        if context.debug_enabled {
            tracing::debug!(
                "[{}] round_complete: {}",
                context.session_id,
                serde_json::json!({
                    "round": context.round + 1,
                    "message_count": message_count,
                })
            );
        }
    }

    pub(super) fn record_round_completion(
        &self,
        metrics_collector: Option<&MetricsCollector>,
        context: &RoundFlowContext<'_>,
        session: &Session,
        round_usage: TokenUsage,
    ) {
        metrics_lifecycle::record_round_completed(
            metrics_collector,
            context.round_id,
            context.session_id,
            session.messages.len() as u32,
            self.round_status,
            round_usage,
            session
                .token_usage
                .as_ref()
                .map(|usage| usage.prompt_cached_tool_outputs)
                .unwrap_or(0)
                .min(u32::MAX as usize) as u32,
            self.round_error.clone(),
        );
    }
}
