use bamboo_metrics::RoundStatus as MetricsRoundStatus;

use super::RoundToolExecutionResult;

#[derive(Debug)]
pub(super) struct RoundExecutionState {
    pub single_tool_call: bool,
    pub quiet_exec_handle: Option<String>,
    pub explicit_exec_waits: Vec<String>,
    pub awaiting_clarification: bool,
    pub waiting_for_children: bool,
    pub yield_supervisor_wait: bool,
    pub round_status: MetricsRoundStatus,
    pub round_error: Option<String>,
}

impl Default for RoundExecutionState {
    fn default() -> Self {
        Self {
            single_tool_call: false,
            quiet_exec_handle: None,
            explicit_exec_waits: Vec::new(),
            awaiting_clarification: false,
            waiting_for_children: false,
            yield_supervisor_wait: false,
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
            quiet_exec_handle: self.quiet_exec_handle,
            explicit_exec_waits: self.explicit_exec_waits,
            awaiting_clarification: self.awaiting_clarification,
            waiting_for_children: self.waiting_for_children,
            yield_supervisor_wait: self.yield_supervisor_wait,
            round_status: self.round_status,
            round_error: self.round_error,
        }
    }
}
