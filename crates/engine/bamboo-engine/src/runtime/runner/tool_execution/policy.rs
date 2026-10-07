use std::collections::{BTreeSet, HashMap};

use bamboo_agent_core::tools::{
    normalize_tool_name, parse_tool_args, parse_tool_args_best_effort, ToolCall, ToolResult,
};
use sha2::{Digest, Sha256};

const MAX_TOOL_CALLS_PER_ROUND: usize = 80;
const MAX_CONSECUTIVE_FAILURES_PER_TOOL: usize = 3;
const UNCHANGED_OBSERVATION_ROUNDS: usize = 3;
const MAX_TRACKED_OBSERVATIONS: usize = 80;

/// Advisory only: compare complete successful filesystem-observation rounds.
/// Keep fingerprints, not arguments or output, and forget everything at run end.
#[derive(Debug, Clone, Default)]
struct ObservationProgress {
    current: Option<BTreeSet<[u8; 32]>>,
    previous: Option<BTreeSet<[u8; 32]>>,
    unchanged_rounds: usize,
    last_round: Option<usize>,
}

impl ObservationProgress {
    fn begin_round(&mut self, round: usize) {
        if self
            .last_round
            .is_some_and(|last| last.checked_add(1) != Some(round))
        {
            self.previous = None;
            self.unchanged_rounds = 0;
        }
        self.last_round = Some(round);
        self.current = Some(BTreeSet::new());
    }

    fn observe(&mut self, call: &ToolCall, outcome: &Result<ToolResult, String>) {
        let Some(current) = self.current.as_mut() else {
            return;
        };
        // Polling, waits, browser/MCP tools and mutations are deliberately outside
        // this small allowlist. Do not infer observation semantics from a name suffix.
        let Some(name) = ["Read", "GetFileInfo", "Glob", "Grep"]
            .into_iter()
            .find(|name| name.eq_ignore_ascii_case(call.function.name.trim()))
        else {
            self.current = None;
            return;
        };
        let Ok(result) = outcome else {
            self.current = None;
            return;
        };
        if !result.success || !result.images.is_empty() || current.len() >= MAX_TRACKED_OBSERVATIONS
        {
            self.current = None;
            return;
        }
        let Ok(mut arguments) = parse_tool_args(&call.function.arguments) else {
            self.current = None;
            return;
        };
        arguments.sort_all_objects();
        let Ok(arguments) = serde_json::to_vec(&arguments) else {
            self.current = None;
            return;
        };
        let mut fingerprint = Sha256::new();
        fingerprint.update(name.as_bytes());
        fingerprint.update([0u8]);
        fingerprint.update(arguments);
        fingerprint.update([0u8]);
        fingerprint.update(result.result.as_bytes());
        current.insert(fingerprint.finalize().into());
    }

    fn finish_round(&mut self) -> Option<&'static str> {
        let current = self.current.take().filter(|round| !round.is_empty());
        self.unchanged_rounds = match &current {
            Some(round) if self.previous.as_ref() == Some(round) => {
                self.unchanged_rounds.saturating_add(1)
            }
            Some(_) => 1,
            None => 0,
        };
        self.previous = current;
        (self.unchanged_rounds == UNCHANGED_OBSERVATION_ROUNDS).then_some(
            "The same successful file observations returned unchanged information for three consecutive rounds. Use the evidence already collected, change your approach, or explain a concrete blocker. For intentional polling, wait between checks or use the appropriate status tool.",
        )
    }
}

const STRICT_ARGUMENT_TOOL_NAMES: [&str; 11] = [
    "Write",
    "Edit",
    "NotebookEdit",
    "apply_patch",
    "Bash",
    "Task",
    "SubAgent",
    "scheduler",
    "sub_session_manager",
    "session_note",
    "memory_note",
];

fn normalize_tool_for_policy(raw_tool_name: &str) -> String {
    let name = bamboo_tools::normalize_tool_ref(raw_tool_name)
        .unwrap_or_else(|| normalize_tool_name(raw_tool_name).trim().to_string());
    if bamboo_tools::resolve_alias(&name)
        .unwrap_or(&name)
        .eq_ignore_ascii_case("SubAgent")
    {
        "SubAgent".to_string()
    } else {
        name
    }
}

pub(super) fn validate_tool_call_arguments(tool_call: &ToolCall) -> Result<(), String> {
    let normalized_tool_name = normalize_tool_for_policy(&tool_call.function.name);
    if !STRICT_ARGUMENT_TOOL_NAMES
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&normalized_tool_name))
    {
        return Ok(());
    }

    if normalized_tool_name.eq_ignore_ascii_case("session_note")
        || normalized_tool_name.eq_ignore_ascii_case("memory_note")
    {
        let (parsed, parse_warning) = parse_tool_args_best_effort(&tool_call.function.arguments);
        if parse_warning.is_some()
            && parsed
                .as_object()
                .map(|map| map.is_empty())
                .unwrap_or(false)
        {
            return Err(format!(
                "Tool policy blocked '{}' due to invalid JSON arguments: unable to recover arguments. Rewrite the {} call with a valid JSON object, for example {{\"action\":\"read\",\"topic\":\"default\"}}.",
                normalized_tool_name, normalized_tool_name,
            ));
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
            } => {
                let scope = if tool_name == "SubAgent" {
                    "run"
                } else {
                    "round"
                };
                format!(
                    "Tool policy blocked '{}': {} consecutive failures reached circuit limit ({}) in this {}",
                    tool_name, consecutive_failures, limit, scope
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ToolPolicyGuard {
    max_tool_calls_per_round: usize,
    max_consecutive_failures_per_tool: usize,
    executed_calls: usize,
    consecutive_failures: HashMap<String, usize>,
    observation_progress: ObservationProgress,
}

impl ToolPolicyGuard {
    pub(crate) fn new(
        max_tool_calls_per_round: usize,
        max_consecutive_failures_per_tool: usize,
    ) -> Self {
        Self {
            max_tool_calls_per_round,
            max_consecutive_failures_per_tool,
            executed_calls: 0,
            consecutive_failures: HashMap::new(),
            observation_progress: ObservationProgress::default(),
        }
    }

    pub(crate) fn begin_round(
        &mut self,
        max_tool_calls_per_round: usize,
        max_consecutive_failures_per_tool: usize,
    ) {
        self.max_tool_calls_per_round = max_tool_calls_per_round;
        self.max_consecutive_failures_per_tool = max_consecutive_failures_per_tool;
        self.executed_calls = 0;
        // Preserve delegation failures across model turns. Other tools keep
        // their existing per-round circuit behavior.
        self.consecutive_failures
            .retain(|name, _| name == "SubAgent");
    }

    pub(crate) fn delegation_failure_message(&self) -> Option<String> {
        let failures = *self.consecutive_failures.get("SubAgent")?;
        (failures >= self.max_consecutive_failures_per_tool).then(|| format!(
            "SubAgent delegation stopped after {failures} consecutive failures in this run \
             (limit {}). Review the tool errors and correct the call before starting a new run. \
             To create a child use {{\"role\":\"explorer\",\"message\":\"Complete bounded task\"}}; \
             omit unused target and reply_to. Reply to a pending parent request only with \
             its exact reply_to id and message, omitting target and role.",
            self.max_consecutive_failures_per_tool
        ))
    }

    pub(super) fn observe_raw_observation(
        &mut self,
        tool_call: &ToolCall,
        outcome: &Result<ToolResult, String>,
    ) {
        self.observation_progress.observe(tool_call, outcome);
    }

    pub(super) fn begin_observation_round(&mut self, round: usize) {
        self.observation_progress.begin_round(round);
    }

    pub(crate) fn observation_progress_hint(&mut self) -> Option<&'static str> {
        self.observation_progress.finish_round()
    }

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

        let succeeded = matches!(outcome, Ok(result) if result.success);

        // Compression/budget enforcement can turn raw success into failure.
        // Such a round must not produce a successful-observation advisory.
        if !succeeded {
            self.observation_progress.current = None;
        }

        if succeeded {
            self.consecutive_failures.remove(&normalized_tool_name);
            return;
        }

        *self
            .consecutive_failures
            .entry(normalized_tool_name)
            .or_insert(0) += 1;
    }
}

impl Default for ToolPolicyGuard {
    fn default() -> Self {
        Self::new(MAX_TOOL_CALLS_PER_ROUND, MAX_CONSECUTIVE_FAILURES_PER_TOOL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::FunctionCall;

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
    fn session_note_accepts_repairable_json_arguments() {
        let call = tool_call("session_note", r#"{"action":"read""#);
        assert!(validate_tool_call_arguments(&call).is_ok());
    }

    #[test]
    fn session_note_rejects_unrecoverable_json_with_retry_guidance() {
        let call = tool_call("session_note", "not-json");
        let err = validate_tool_call_arguments(&call).expect_err("expected strict rejection");
        assert!(err.contains("Rewrite the session_note call with a valid JSON object"));
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
                    images: Vec::new(),
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
                images: Vec::new(),
            }),
        );

        assert!(guard.check_before_execution(&call, 0).is_ok());
    }

    #[test]
    fn only_delegation_failures_survive_round_boundaries() {
        let mut guard = ToolPolicyGuard::new(2, 3);
        let delegation = tool_call("SubAgent", "{}");
        let read = tool_call("Read", "{}");
        for _ in 0..3 {
            guard.begin_round(2, 3);
            assert!(guard.check_before_execution(&delegation, 0).is_ok());
            assert!(guard.check_before_execution(&read, 0).is_ok());
            guard.observe_outcome(&delegation, &Err("invalid arguments".into()));
            guard.observe_outcome(&read, &Err("missing file".into()));
        }
        guard.begin_round(2, 3);
        assert!(guard.check_before_execution(&delegation, 0).is_err());
        assert!(guard.check_before_execution(&read, 0).is_ok());
        assert!(guard
            .delegation_failure_message()
            .unwrap()
            .contains("3 consecutive failures"));
        assert!(ToolPolicyGuard::new(2, 3)
            .delegation_failure_message()
            .is_none());
    }

    #[test]
    fn corrected_delegation_resets_cross_round_streak() {
        let mut guard = ToolPolicyGuard::default();
        let call = tool_call("SubAgent", "{}");
        for _ in 0..2 {
            guard.begin_round(80, 3);
            guard.observe_outcome(&call, &Err("invalid arguments".into()));
        }
        guard.begin_round(80, 3);
        guard.observe_outcome(&call, &Ok(ToolResult::text(true, "child created")));
        for _ in 0..2 {
            guard.begin_round(80, 3);
            guard.observe_outcome(&call, &Err("invalid arguments".into()));
        }
        assert!(guard.check_before_execution(&call, 0).is_ok());
        assert!(guard.delegation_failure_message().is_none());
    }

    #[test]
    fn delegation_aliases_share_one_failure_streak() {
        let mut guard = ToolPolicyGuard::default();
        for name in ["SubAgent", "server::SubAgent", "SUB_SESSION_MANAGER"] {
            guard.begin_round(80, 3);
            guard.observe_outcome(&tool_call(name, "{}"), &Err("invalid arguments".into()));
        }
        assert!(guard.delegation_failure_message().is_some());
    }

    fn observation_round(
        guard: &mut ToolPolicyGuard,
        round: usize,
        calls: &[(&str, &str, Result<ToolResult, String>)],
    ) -> bool {
        guard.begin_round(80, 3);
        guard.begin_observation_round(round);
        for (name, args, result) in calls {
            let call = tool_call(name, args);
            guard.observe_raw_observation(&call, result);
            guard.observe_outcome(&call, result);
        }
        guard.observation_progress_hint().is_some()
    }

    #[test]
    fn observation_progress_hints_once_per_streak_and_resets_for_new_run() {
        let calls = [(
            "Read",
            r#"{"path":"file"}"#,
            Ok(ToolResult::text(true, "same")),
        )];
        let mut guard = ToolPolicyGuard::default();
        for round in 0..8 {
            assert_eq!(observation_round(&mut guard, round, &calls), round == 2);
            assert!(guard
                .check_before_execution(&tool_call("Read", "{}"), 0)
                .is_ok());
        }
        let mut fresh = ToolPolicyGuard::default();
        assert!(!observation_round(&mut fresh, 0, &calls));
        assert!(!observation_round(&mut fresh, 1, &calls));
        assert!(observation_round(&mut fresh, 2, &calls));
    }

    #[test]
    fn observation_progress_canonicalizes_object_keys_and_counts_batches_once() {
        let mut guard = ToolPolicyGuard::default();
        for round in 0..3 {
            let args = if round == 1 {
                r#"{"range":{"end":3,"start":1},"path":"file"}"#
            } else {
                r#"{"path":"file","range":{"start":1,"end":3}}"#
            };
            let calls = (0..8)
                .map(|_| ("Read", args, Ok(ToolResult::text(true, "same"))))
                .collect::<Vec<_>>();
            assert_eq!(observation_round(&mut guard, round, &calls), round == 2);
        }
    }

    #[test]
    fn observation_progress_changed_evidence_arguments_and_round_gaps_reset_streak() {
        for (before, after, output) in [
            (r#"{"path":"file"}"#, r#"{"path":"file"}"#, "changed"),
            (r#"{"path":"file"}"#, r#"{"path":"other"}"#, "same"),
            (r#"{"paths":["a","b"]}"#, r#"{"paths":["b","a"]}"#, "same"),
        ] {
            let baseline = [("Read", before, Ok(ToolResult::text(true, "same")))];
            let changed = ("Read", after, Ok(ToolResult::text(true, output)));
            let mut guard = ToolPolicyGuard::default();
            assert!(!observation_round(&mut guard, 0, &baseline));
            assert!(!observation_round(&mut guard, 1, &baseline));
            assert!(!observation_round(&mut guard, 2, &[changed]));
            assert!(!observation_round(&mut guard, 3, &baseline));
            assert!(!observation_round(&mut guard, 4, &baseline));
            assert!(observation_round(&mut guard, 5, &baseline));
            assert!(!observation_round(&mut guard, 7, &baseline));
        }
    }

    #[test]
    fn observation_progress_failures_mutations_and_polling_reset_streak() {
        let observation = ("Read", "{}", Ok(ToolResult::text(true, "same")));
        for breaker in [
            ("Read", "{}", Err("missing".to_string())),
            ("Read", "{}", Ok(ToolResult::text(false, "failed"))),
            ("Write", "{}", Ok(ToolResult::text(true, "same"))),
            ("BashOutput", "{}", Ok(ToolResult::text(true, "same"))),
            ("Sleep", "{}", Ok(ToolResult::text(true, "same"))),
            ("mcp::Read", "{}", Ok(ToolResult::text(true, "same"))),
        ] {
            let mut guard = ToolPolicyGuard::default();
            assert!(!observation_round(&mut guard, 0, &[observation.clone()]));
            assert!(!observation_round(&mut guard, 1, &[observation.clone()]));
            assert!(!observation_round(
                &mut guard,
                2,
                &[observation.clone(), breaker]
            ));
            assert!(!observation_round(&mut guard, 3, &[observation.clone()]));
            assert!(!observation_round(&mut guard, 4, &[observation.clone()]));
            assert!(observation_round(&mut guard, 5, &[observation.clone()]));
        }
    }
}
