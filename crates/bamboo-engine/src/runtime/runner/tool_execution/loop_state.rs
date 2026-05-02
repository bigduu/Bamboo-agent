use crate::metrics::RoundStatus as MetricsRoundStatus;

use super::RoundToolExecutionResult;

#[derive(Debug)]
pub(super) struct RoundExecutionState {
    pub awaiting_clarification: bool,
    pub waiting_for_children: bool,
    pub round_status: MetricsRoundStatus,
    pub round_error: Option<String>,
}

impl Default for RoundExecutionState {
    fn default() -> Self {
        Self {
            awaiting_clarification: false,
            waiting_for_children: false,
            round_status: MetricsRoundStatus::Success,
            round_error: None,
        }
    }
}

impl RoundExecutionState {
    pub(super) fn mark_awaiting_clarification(&mut self) {
        self.awaiting_clarification = true;
    }

    pub(super) fn mark_waiting_for_children(&mut self) {
        self.waiting_for_children = true;
    }

    pub(super) fn mark_unsuccessful_tool(&mut self, tool_name: &str) {
        if self.round_error.is_none() {
            self.round_status = MetricsRoundStatus::Error;
            self.round_error = Some(format!(
                "Tool \"{}\" returned an unsuccessful result",
                tool_name
            ));
        }
    }

    pub(super) fn mark_tool_execution_error(&mut self, error_message: String) {
        self.round_status = MetricsRoundStatus::Error;
        self.round_error = Some(error_message);
    }

    pub(super) fn into_result(self) -> RoundToolExecutionResult {
        RoundToolExecutionResult {
            awaiting_clarification: self.awaiting_clarification,
            waiting_for_children: self.waiting_for_children,
            round_status: self.round_status,
            round_error: self.round_error,
        }
    }
}
