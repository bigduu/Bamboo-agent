use std::collections::HashMap;

use crate::agent::core::tools::{
    normalize_tool_name, parse_tool_args, parse_tool_args_best_effort, ToolCall, ToolResult,
};
use crate::agent::core::{Role, Session};

const MAX_TOOL_CALLS_PER_ROUND: usize = 80;
const MAX_CONSECUTIVE_FAILURES_PER_TOOL: usize = 3;
const RESET_POLICY_TOOL_NAME: &str = "conclusion_with_options";
const COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY: &str =
    "copilot_conclusion_with_options_enhancement_enabled";

const STRICT_ARGUMENT_TOOL_NAMES: [&str; 10] = [
    "Write",
    "Edit",
    "NotebookEdit",
    "apply_patch",
    "Bash",
    "Task",
    "SubSession",
    "scheduler",
    "sub_session_manager",
    "memory_note",
];

fn normalize_tool_for_policy(raw_tool_name: &str) -> String {
    crate::agent::tools::normalize_tool_ref(raw_tool_name)
        .unwrap_or_else(|| normalize_tool_name(raw_tool_name).trim().to_string())
}

fn is_copilot_conclusion_with_options_enhancement_enabled(session: &Session) -> bool {
    session
        .metadata
        .get(COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY)
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

fn find_assistant_message_for_tool_call<'a>(
    session: &'a Session,
    tool_call_id: &str,
) -> Option<&'a crate::agent::core::Message> {
    session.messages.iter().rev().find(|message| {
        matches!(message.role, Role::Assistant)
            && message
                .tool_calls
                .as_ref()
                .map(|calls| calls.iter().any(|call| call.id == tool_call_id))
                .unwrap_or(false)
    })
}

fn conclusion_with_options_arguments_include_conclusion(tool_call: &ToolCall) -> bool {
    let (parsed, _) = parse_tool_args_best_effort(&tool_call.function.arguments);
    let Some(args) = parsed.as_object() else {
        return false;
    };

    let Some(conclusion) = args.get("conclusion").and_then(|value| value.as_object()) else {
        return false;
    };
    let has_summary = conclusion
        .get("summary")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.trim().is_empty());
    let has_mermaid_graph = conclusion
        .get("mermaid")
        .and_then(|value| value.as_object())
        .and_then(|value| value.get("graph"))
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.trim().is_empty());

    has_summary && has_mermaid_graph
}

pub(super) fn validate_tool_call_arguments(tool_call: &ToolCall) -> Result<(), String> {
    let normalized_tool_name = normalize_tool_for_policy(&tool_call.function.name);
    if !STRICT_ARGUMENT_TOOL_NAMES
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&normalized_tool_name))
    {
        return Ok(());
    }

    if normalized_tool_name.eq_ignore_ascii_case("memory_note") {
        let (parsed, parse_warning) = parse_tool_args_best_effort(&tool_call.function.arguments);
        if parse_warning.is_some()
            && parsed
                .as_object()
                .map(|map| map.is_empty())
                .unwrap_or(false)
        {
            return Err(
                "Tool policy blocked 'memory_note' due to invalid JSON arguments: unable to recover arguments. Rewrite the memory_note call with a valid JSON object, for example {\"action\":\"read\",\"topic\":\"default\"}.".to_string()
            );
        }
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

pub(super) fn validate_tool_call_context(
    tool_call: &ToolCall,
    session: &Session,
) -> Result<(), String> {
    let normalized_tool_name = normalize_tool_for_policy(&tool_call.function.name);
    let assistant_message = find_assistant_message_for_tool_call(session, &tool_call.id);
    let enhancement_enabled = is_copilot_conclusion_with_options_enhancement_enabled(session);

    if normalized_tool_name.eq_ignore_ascii_case("conclusion_with_options") {
        let has_narration = assistant_message
            .map(|message| !message.content.trim().is_empty())
            .unwrap_or(false);

        if !has_narration {
            return Err(
                "Tool policy blocked 'conclusion_with_options': add a brief assistant text summary before calling conclusion_with_options so the user can understand the conclusion. Then retry conclusion_with_options instead of repeating the same blocked call. For example, first send a short plain-text wrap-up sentence, then call conclusion_with_options in the next assistant action."
                    .to_string(),
            );
        }

        if enhancement_enabled && !conclusion_with_options_arguments_include_conclusion(tool_call) {
            return Err(
                "Tool policy blocked 'conclusion_with_options': when copilot conclusion-with-options enhancement is enabled, include `conclusion.summary` and `conclusion.mermaid.graph` in the conclusion_with_options arguments. Rewrite the tool arguments to include those required fields, then retry conclusion_with_options."
                    .to_string(),
            );
        }

        return Ok(());
    }

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
    use crate::agent::core::{Message, Session};

    fn tool_call(name: &str, arguments: &str) -> ToolCall {
        tool_call_with_id("call-1", name, arguments)
    }

    fn tool_call_with_id(id: &str, name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
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
    fn conclusion_with_options_context_validation_requires_matching_assistant_narration() {
        let call = tool_call("conclusion_with_options", "{}");
        let session = Session::new("session-1", "model");
        let err =
            validate_tool_call_context(&call, &session).expect_err("expected context rejection");
        assert!(err.contains("before calling conclusion_with_options"));
        assert!(err.contains("Then retry conclusion_with_options"));
        assert!(err.contains("short plain-text wrap-up sentence"));
    }

    #[test]
    fn conclusion_with_options_context_validation_rejects_empty_assistant_narration() {
        let call = tool_call("conclusion_with_options", "{}");
        let mut session = Session::new("session-1", "model");
        session.add_message(Message::assistant("   ", Some(vec![call.clone()])));

        let err =
            validate_tool_call_context(&call, &session).expect_err("expected context rejection");
        assert!(err.contains("before calling conclusion_with_options"));
        assert!(err.contains("Then retry conclusion_with_options"));
    }

    #[test]
    fn conclusion_with_options_context_validation_accepts_non_empty_assistant_narration() {
        let call = tool_call("conclusion_with_options", "{}");
        let mut session = Session::new("session-1", "model");
        session.add_message(Message::assistant(
            "Summary: completed requested changes and ready for your confirmation.",
            Some(vec![call.clone()]),
        ));

        assert!(validate_tool_call_context(&call, &session).is_ok());
    }

    #[test]
    fn non_conclusion_with_options_context_validation_is_noop() {
        let call = tool_call("Read", "{}");
        let session = Session::new("session-1", "model");
        assert!(validate_tool_call_context(&call, &session).is_ok());
    }

    #[test]
    fn conclusion_with_options_requires_conclusion_fields_when_enhancement_enabled() {
        let conclusion_with_options_call = tool_call_with_id(
            "call-conclusion-with-options",
            "conclusion_with_options",
            r#"{"question":"Continue?"}"#,
        );
        let mut session = Session::new("session-1", "model");
        session.metadata.insert(
            COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY.to_string(),
            "true".to_string(),
        );
        session.add_message(Message::assistant(
            "Final confirmation request.",
            Some(vec![conclusion_with_options_call.clone()]),
        ));

        let err = validate_tool_call_context(&conclusion_with_options_call, &session)
            .expect_err("expected policy rejection");
        assert!(err.contains("conclusion.summary"));
        assert!(err.contains("conclusion.mermaid.graph"));
        assert!(err.contains("then retry conclusion_with_options"));
    }

    #[test]
    fn conclusion_with_options_with_conclusion_fields_passes_when_enhancement_enabled() {
        let conclusion_with_options_call = tool_call_with_id(
            "call-conclusion-with-options",
            "conclusion_with_options",
            r#"{"question":"Continue?","conclusion":{"summary":"All done.","mermaid":{"graph":"graph TD\nA-->B"}}}"#,
        );
        let mut session = Session::new("session-1", "model");
        session.metadata.insert(
            COPILOT_CONCLUSION_WITH_OPTIONS_ENHANCEMENT_METADATA_KEY.to_string(),
            "true".to_string(),
        );
        session.add_message(Message::assistant(
            "Final summary and confirmation.",
            Some(vec![conclusion_with_options_call.clone()]),
        ));

        assert!(validate_tool_call_context(&conclusion_with_options_call, &session).is_ok());
    }

    #[test]
    fn memory_note_accepts_repairable_json_arguments() {
        let call = tool_call("memory_note", r#"{"action":"read""#);
        assert!(validate_tool_call_arguments(&call).is_ok());
    }

    #[test]
    fn memory_note_rejects_unrecoverable_json_with_retry_guidance() {
        let call = tool_call("memory_note", "not-json");
        let err = validate_tool_call_arguments(&call).expect_err("expected strict rejection");
        assert!(err.contains("Rewrite the memory_note call with a valid JSON object"));
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
    fn conclusion_with_options_resets_round_limit_counters() {
        let mut guard = ToolPolicyGuard::default();
        let read_call = tool_call("Read", "{}");
        let conclusion_with_options_call = tool_call("conclusion_with_options", "{}");

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
            &conclusion_with_options_call,
            &Ok(ToolResult {
                success: true,
                result: "ask".to_string(),
                display_preference: None,
            }),
        );

        assert!(guard.check_before_execution(&read_call, 0).is_ok());
    }

    #[test]
    fn conclusion_with_options_resets_failure_circuit() {
        let mut guard = ToolPolicyGuard::default();
        let bash_call = tool_call("Bash", "{}");
        let conclusion_with_options_call = tool_call("conclusion_with_options", "{}");

        for _ in 0..MAX_CONSECUTIVE_FAILURES_PER_TOOL {
            guard.observe_outcome(&bash_call, &Err("boom".to_string()));
        }
        assert!(guard.check_before_execution(&bash_call, 0).is_err());

        guard.observe_outcome(
            &conclusion_with_options_call,
            &Ok(ToolResult {
                success: true,
                result: "ask".to_string(),
                display_preference: None,
            }),
        );

        assert!(guard.check_before_execution(&bash_call, 0).is_ok());
    }
}
