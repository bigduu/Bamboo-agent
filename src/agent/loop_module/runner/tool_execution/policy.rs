use std::collections::HashMap;

use crate::agent::core::tools::{normalize_tool_name, parse_tool_args, ToolCall, ToolResult};

const MAX_TOOL_CALLS_PER_ROUND: usize = 80;
const MAX_CONSECUTIVE_FAILURES_PER_TOOL: usize = 3;
const RESET_POLICY_TOOL_NAME: &str = "ask_user";

const STRICT_ARGUMENT_TOOL_NAMES: [&str; 10] = [
    "Write",
    "Edit",
    "NotebookEdit",
    "apply_patch",
    "Bash",
    "Task",
    "SubSession",
    "schedule_tasks",
    "sub_session_manager",
    "memory_note",
];

fn normalize_tool_for_policy(raw_tool_name: &str) -> String {
    crate::agent::tools::normalize_tool_ref(raw_tool_name)
        .unwrap_or_else(|| normalize_tool_name(raw_tool_name).trim().to_string())
}

pub(super) fn validate_tool_call_arguments(tool_call: &ToolCall) -> Result<(), String> {
    let normalized_tool_name = normalize_tool_for_policy(&tool_call.function.name);
    if !STRICT_ARGUMENT_TOOL_NAMES
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&normalized_tool_name))
    {
        return Ok(());
    }

    parse_tool_args(&tool_call.function.arguments).map_err(|error| {
        format!(
            "Tool policy blocked '{}' due to invalid JSON arguments: {}",
            normalized_tool_name, error
        )
    })?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ToolPolicyPrecheckViolation {
    RoundToolLimit {
        limit: usize,
        tool_name: String,
    },
    ToolCircuitOpen {
        tool_name: String,
        consecutive_failures: usize,
        limit: usize,
    },
}

impl ToolPolicyPrecheckViolation {
    pub(super) fn should_stop_round(&self) -> bool {
        matches!(self, Self::RoundToolLimit { .. })
    }

    pub(super) fn into_message(self) -> String {
        match self {
            Self::RoundToolLimit { limit, tool_name } => format!(
                "Tool policy blocked '{}': per-round tool call limit ({limit}) reached",
                tool_name
            ),
            Self::ToolCircuitOpen {
                tool_name,
                consecutive_failures,
                limit,
            } => format!(
                "Tool policy blocked '{}': {} consecutive failures reached circuit limit ({}) in this round",
                tool_name, consecutive_failures, limit
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ToolPolicyGuard {
    max_tool_calls_per_round: usize,
    max_consecutive_failures_per_tool: usize,
    executed_calls: usize,
    consecutive_failures: HashMap<String, usize>,
}

impl ToolPolicyGuard {
    pub(super) fn check_before_execution(
        &self,
        tool_call: &ToolCall,
        reserved_calls: usize,
    ) -> Result<(), ToolPolicyPrecheckViolation> {
        let normalized_tool_name = normalize_tool_for_policy(&tool_call.function.name);
        let projected_executions = self.executed_calls.saturating_add(reserved_calls);

        if projected_executions >= self.max_tool_calls_per_round {
            return Err(ToolPolicyPrecheckViolation::RoundToolLimit {
                limit: self.max_tool_calls_per_round,
                tool_name: normalized_tool_name,
            });
        }

        let failures = self
            .consecutive_failures
            .get(&normalized_tool_name)
            .copied()
            .unwrap_or(0);
        if failures >= self.max_consecutive_failures_per_tool {
            return Err(ToolPolicyPrecheckViolation::ToolCircuitOpen {
                tool_name: normalized_tool_name,
                consecutive_failures: failures,
                limit: self.max_consecutive_failures_per_tool,
            });
        }

        Ok(())
    }

    pub(super) fn observe_outcome(
        &mut self,
        tool_call: &ToolCall,
        outcome: &Result<ToolResult, String>,
    ) {
        self.executed_calls = self.executed_calls.saturating_add(1);

        let normalized_tool_name = normalize_tool_for_policy(&tool_call.function.name);

        if normalized_tool_name.eq_ignore_ascii_case(RESET_POLICY_TOOL_NAME) {
            self.reset();
            return;
        }

        let succeeded = matches!(outcome, Ok(result) if result.success);

        if succeeded {
            self.consecutive_failures.remove(&normalized_tool_name);
            return;
        }

        *self
            .consecutive_failures
            .entry(normalized_tool_name)
            .or_insert(0) += 1;
    }

    fn reset(&mut self) {
        self.executed_calls = 0;
        self.consecutive_failures.clear();
    }
}

impl Default for ToolPolicyGuard {
    fn default() -> Self {
        Self {
            max_tool_calls_per_round: MAX_TOOL_CALLS_PER_ROUND,
            max_consecutive_failures_per_tool: MAX_CONSECUTIVE_FAILURES_PER_TOOL,
            executed_calls: 0,
            consecutive_failures: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::tools::FunctionCall;

    fn tool_call(name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn strict_tools_reject_invalid_json_arguments() {
        let invalid = tool_call("Write", "{invalid");
        let err = validate_tool_call_arguments(&invalid).expect_err("expected strict rejection");
        assert!(err.contains("Tool policy blocked 'Write'"));
    }

    #[test]
    fn non_strict_tools_allow_invalid_json_for_best_effort_path() {
        let call = tool_call("Read", "{invalid");
        assert!(validate_tool_call_arguments(&call).is_ok());
    }

    #[test]
    fn precheck_blocks_when_round_limit_is_reached() {
        let mut guard = ToolPolicyGuard::default();
        let call = tool_call("Read", "{}");

        for _ in 0..MAX_TOOL_CALLS_PER_ROUND {
            guard.observe_outcome(
                &call,
                &Ok(ToolResult {
                    success: true,
                    result: "ok".to_string(),
                    display_preference: None,
                }),
            );
        }

        let violation = guard
            .check_before_execution(&call, 0)
            .expect_err("expected round limit violation");
        assert!(matches!(
            violation,
            ToolPolicyPrecheckViolation::RoundToolLimit { .. }
        ));
    }

    #[test]
    fn precheck_blocks_after_consecutive_failures() {
        let mut guard = ToolPolicyGuard::default();
        let call = tool_call("Bash", "{}");

        for _ in 0..MAX_CONSECUTIVE_FAILURES_PER_TOOL {
            guard.observe_outcome(&call, &Err("boom".to_string()));
        }

        let violation = guard
            .check_before_execution(&call, 0)
            .expect_err("expected circuit violation");
        assert!(matches!(
            violation,
            ToolPolicyPrecheckViolation::ToolCircuitOpen { .. }
        ));
    }

    #[test]
    fn successful_outcome_resets_failure_streak() {
        let mut guard = ToolPolicyGuard::default();
        let call = tool_call("Task", "{}");

        guard.observe_outcome(&call, &Err("boom".to_string()));
        guard.observe_outcome(
            &call,
            &Ok(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            }),
        );

        assert!(guard.check_before_execution(&call, 0).is_ok());
    }

    #[test]
    fn ask_user_resets_round_limit_counters() {
        let mut guard = ToolPolicyGuard::default();
        let read_call = tool_call("Read", "{}");
        let ask_user_call = tool_call("ask_user", "{}");

        for _ in 0..MAX_TOOL_CALLS_PER_ROUND {
            guard.observe_outcome(
                &read_call,
                &Ok(ToolResult {
                    success: true,
                    result: "ok".to_string(),
                    display_preference: None,
                }),
            );
        }

        assert!(guard.check_before_execution(&read_call, 0).is_err());

        guard.observe_outcome(
            &ask_user_call,
            &Ok(ToolResult {
                success: true,
                result: "ask".to_string(),
                display_preference: None,
            }),
        );

        assert!(guard.check_before_execution(&read_call, 0).is_ok());
    }

    #[test]
    fn ask_user_resets_failure_circuit() {
        let mut guard = ToolPolicyGuard::default();
        let bash_call = tool_call("Bash", "{}");
        let ask_user_call = tool_call("ask_user", "{}");

        for _ in 0..MAX_CONSECUTIVE_FAILURES_PER_TOOL {
            guard.observe_outcome(&bash_call, &Err("boom".to_string()));
        }
        assert!(guard.check_before_execution(&bash_call, 0).is_err());

        guard.observe_outcome(
            &ask_user_call,
            &Ok(ToolResult {
                success: true,
                result: "ask".to_string(),
                display_preference: None,
            }),
        );

        assert!(guard.check_before_execution(&bash_call, 0).is_ok());
    }
}
