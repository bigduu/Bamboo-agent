use std::collections::{BTreeSet, HashMap};

use bamboo_agent_core::tools::{
    normalize_tool_name, parse_tool_args, parse_tool_args_best_effort, ToolCall, ToolResult,
};
use sha2::{Digest, Sha256};

const MAX_TOOL_CALLS_PER_ROUND: usize = 80;
const MAX_CONSECUTIVE_FAILURES_PER_TOOL: usize = 3;
const UNCHANGED_OBSERVATION_ROUNDS: usize = 3;
const REPEATED_TOOL_CALLS: usize = 3;
const MAX_TRACKED_OBSERVATIONS: usize = 80;
const SUBAGENT_INSPECTION_LABEL: &str = "SubAgent (inspect)";
pub(super) const REPEATED_TOOL_ADVISORY_PREFIX: &str = "[Runtime tool advisory] ";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum ToolCircuitKey {
    Tool(String),
    SubAgentInspection,
}

impl ToolCircuitKey {
    fn name(&self) -> &str {
        match self {
            Self::Tool(name) => name,
            Self::SubAgentInspection => SUBAGENT_INSPECTION_LABEL,
        }
    }

    fn run_scoped(&self) -> bool {
        matches!(self, Self::SubAgentInspection)
            || matches!(self, Self::Tool(name) if name == "SubAgent")
    }
}

/// Compare complete successful filesystem-observation rounds.
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

/// Track consecutive equivalent calls within one execution.
#[derive(Debug, Clone, Default)]
struct RepeatedToolCalls {
    fingerprint: Option<[u8; 32]>,
    count: usize,
    last_round: Option<usize>,
    pending: BTreeSet<String>,
}

impl RepeatedToolCalls {
    fn begin_round(&mut self, round: usize) {
        if self
            .last_round
            .is_some_and(|last| last.checked_add(1) != Some(round))
        {
            *self = Self::default();
        }
        self.last_round = Some(round);
    }

    fn observe(&mut self, name: &str, fingerprint: Option<[u8; 32]>) {
        if fingerprint.is_some() && self.fingerprint == fingerprint {
            self.count = self.count.saturating_add(1);
        } else {
            self.fingerprint = fingerprint;
            self.count = usize::from(fingerprint.is_some());
        }
        if self.count == REPEATED_TOOL_CALLS {
            // Keep the notice even if further calls in the batch break this streak.
            self.pending.insert(name.chars().take(128).collect());
        }
    }

    fn advisory(&mut self, file_hint: Option<&str>) -> Option<String> {
        if self.pending.is_empty() {
            return file_hint
                .filter(|_| self.count < REPEATED_TOOL_CALLS)
                .map(|hint| format!("{REPEATED_TOOL_ADVISORY_PREFIX}{hint}"));
        }
        let tools = std::mem::take(&mut self.pending)
            .into_iter()
            .map(|name| format!("{name:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!(
            "{REPEATED_TOOL_ADVISORY_PREFIX}The following tools each returned the same result at least {REPEATED_TOOL_CALLS} times in a row with identical arguments ({tools}). Consider using the evidence already collected or trying a different approach. If you are intentionally polling, wait for a useful change or use the appropriate status tool.",
        ))
    }
}

/// Compare real results before compression, using the admitted identity and inputs.
pub(super) fn dispatched_tool_fingerprint(
    call: &ToolCall,
    outcome: &super::per_call::ToolExecutionOutcome,
) -> Option<[u8; 32]> {
    outcome.output_cap?;
    if outcome.needs_human.is_some() {
        return None;
    }
    let tool = outcome.portable_tool.as_ref()?;
    let mut fingerprint = start_tool_fingerprint("dispatch", tool.execution_name(), tool.input())?;
    match &outcome.result {
        Ok(result) => {
            if super::clarification::requires_suspension(call, result)
                || bamboo_agent_core::tools::tool_result_requires_suspension(result)
            {
                return None;
            }
            fingerprint.update([
                1,
                u8::from(result.success),
                u8::from(outcome.post_tool_hook_eligible),
            ]);
            let normalized = normalized_shell_poll(tool.execution_name(), result);
            hash_field(
                &mut fingerprint,
                normalized.as_deref().unwrap_or(result.result.as_bytes()),
            );
            for image in &result.images {
                hash_field(&mut fingerprint, image.mime_type.as_bytes());
                hash_field(&mut fingerprint, image.data.as_bytes());
            }
        }
        Err(error) => {
            fingerprint.update([0]);
            hash_field(&mut fingerprint, error.as_bytes());
        }
    }
    Some(fingerprint.finalize().into())
}

pub(crate) fn discovery_tool_fingerprint(
    name: &str,
    arguments: &serde_json::Value,
    success: bool,
    result: &[u8],
) -> Option<[u8; 32]> {
    let mut fingerprint = start_tool_fingerprint("discovery", name, arguments)?;
    fingerprint.update([u8::from(success)]);
    hash_field(&mut fingerprint, result);
    Some(fingerprint.finalize().into())
}

fn start_tool_fingerprint(
    source: &str,
    name: &str,
    arguments: &serde_json::Value,
) -> Option<Sha256> {
    let mut arguments = arguments.clone();
    arguments.sort_all_objects();
    let mut fingerprint = Sha256::new();
    hash_field(&mut fingerprint, source.as_bytes());
    hash_field(&mut fingerprint, name.as_bytes());
    hash_field(&mut fingerprint, &serde_json::to_vec(&arguments).ok()?);
    Some(fingerprint)
}

/// Shell clocks and advice change while an empty capture still observes the
/// same process state. Preserve the handle, output and exit observations.
fn normalized_shell_poll(name: &str, result: &ToolResult) -> Option<Vec<u8>> {
    if !matches!(name, "exec_command" | "write_stdin") || !result.success {
        return None;
    }
    let mut value: serde_json::Value = serde_json::from_str(&result.result).ok()?;
    if value["status"] != "running"
        || value["session_id"].as_str().is_none()
        || value["output"] != ""
        || value
            .get("output_omitted_bytes")
            .is_some_and(|v| v.as_u64() != Some(0))
    {
        return None;
    }
    let polling = value.get_mut("polling")?.as_object_mut()?;
    for field in [
        "elapsed_ms",
        "output_idle_ms",
        "consecutive_empty_readouts",
        "state",
        "advisory",
    ] {
        polling.remove(field);
    }
    value.sort_all_objects();
    serde_json::to_vec(&value).ok()
}

fn hash_field(fingerprint: &mut Sha256, bytes: &[u8]) {
    fingerprint.update((bytes.len() as u64).to_be_bytes());
    fingerprint.update(bytes);
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

fn normalize_tool_for_policy(raw_tool_name: &str, admitted_execution_name: Option<&str>) -> String {
    if let Some(execution_name) = admitted_execution_name {
        // Admission already selected an exact registered owner. Legacy alias
        // normalization here would charge a Root/custom failure to SubAgent.
        return execution_name.to_owned();
    }
    let selected = raw_tool_name.trim();
    let name = bamboo_tools::normalize_tool_ref(selected)
        .unwrap_or_else(|| normalize_tool_name(selected).trim().to_string());
    if matches!(name.as_str(), "exec_command" | "write_stdin") {
        // Only the admitted exact identity can share a shell circuit. An exact
        // custom vendor::exec_command keeps its ordinary outcome and own key.
        return selected.to_owned();
    }
    if bamboo_tools::resolve_alias(&name)
        .unwrap_or(&name)
        .eq_ignore_ascii_case("SubAgent")
    {
        "SubAgent".to_string()
    } else {
        name
    }
}

/// Separate read-only SubAgent inspection from delegation only when the
/// arguments unambiguously select an inspection route. Ambiguous or malformed
/// requests remain in the delegation circuit, including compact/legacy mixes.
pub(super) fn tool_policy_key(
    tool_call: &ToolCall,
    admitted_execution_name: Option<&str>,
) -> ToolCircuitKey {
    let name = normalize_tool_for_policy(&tool_call.function.name, admitted_execution_name);
    if name != "SubAgent" {
        return ToolCircuitKey::Tool(name);
    }
    let Ok(args) = parse_tool_args(&tool_call.function.arguments) else {
        return ToolCircuitKey::Tool(name);
    };
    let Some(object) = args.as_object() else {
        return ToolCircuitKey::Tool(name);
    };
    let compact_inspection = object.get("intent").and_then(|value| value.as_str())
        == Some("inspect")
        && object.keys().all(|key| {
            matches!(
                key.as_str(),
                "intent" | "target" | "message" | "role" | "reply_to"
            )
        })
        && ["role", "reply_to"].iter().all(|key| {
            object
                .get(*key)
                .is_none_or(|value| value.is_null() || value.as_str() == Some(""))
        });
    let legacy_inspection = matches!(
        object.get("action").and_then(|value| value.as_str()),
        Some("get" | "list_models")
    ) && ["intent", "target", "role", "reply_to"]
        .iter()
        .all(|key| !object.contains_key(*key));
    if compact_inspection || legacy_inspection {
        ToolCircuitKey::SubAgentInspection
    } else {
        ToolCircuitKey::Tool(name)
    }
}

#[cfg(test)]
pub(super) fn validate_tool_call_arguments(tool_call: &ToolCall) -> Result<(), String> {
    validate_tool_call_arguments_for_execution(tool_call, "")
}

pub(super) fn validate_tool_call_arguments_for_execution(
    tool_call: &ToolCall,
    execution_name: &str,
) -> Result<(), String> {
    let exact_shell = matches!(execution_name, "exec_command" | "write_stdin");
    let normalized_tool_name = if exact_shell {
        execution_name.to_owned()
    } else {
        normalize_tool_for_policy(&tool_call.function.name, None)
    };
    if !exact_shell
        && !STRICT_ARGUMENT_TOOL_NAMES
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
    StandaloneSpawnArguments(&'static str),
    RoundToolLimit {
        limit: usize,
        tool_name: String,
    },
    ToolCircuitOpen {
        tool_name: String,
        consecutive_failures: usize,
        limit: usize,
        run_scoped: bool,
    },
}

impl ToolPolicyPrecheckViolation {
    pub(super) fn should_stop_round(&self) -> bool {
        matches!(self, Self::RoundToolLimit { .. })
    }

    pub(super) fn into_message(self) -> String {
        match self {
            Self::StandaloneSpawnArguments(message) => message.into(),
            Self::RoundToolLimit { limit, tool_name } => format!(
                "Tool policy blocked '{}': per-round tool call limit ({limit}) reached",
                tool_name
            ),
            Self::ToolCircuitOpen {
                tool_name,
                consecutive_failures,
                limit,
                run_scoped,
            } => {
                let scope = if run_scoped { "run" } else { "round" };
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
    consecutive_failures: HashMap<ToolCircuitKey, usize>,
    observation_progress: ObservationProgress,
    repeated_tool_calls: RepeatedToolCalls,
    pub(super) standalone_supervisor_spawn: bool,
    pub(super) child_inspections: super::child_inspection::ChildInspectionProgress,
    pub(super) quiet_polls: super::quiet_poll::QuietPollGuard,
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
            repeated_tool_calls: RepeatedToolCalls::default(),
            standalone_supervisor_spawn: false,
            child_inspections: super::child_inspection::ChildInspectionProgress::default(),
            quiet_polls: super::quiet_poll::QuietPollGuard::default(),
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
        // Preserve each SubAgent circuit across model turns. Other tools keep
        // their existing per-round circuit behavior.
        self.consecutive_failures.retain(|key, _| key.run_scoped());
    }

    pub(crate) fn set_supervisor_spawn_route(&mut self, standalone: bool) {
        self.standalone_supervisor_spawn = standalone;
    }

    pub(crate) fn delegation_failure_message(&self) -> Option<String> {
        let failures = *self
            .consecutive_failures
            .get(&ToolCircuitKey::Tool("SubAgent".into()))?;
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
        self.begin_repeated_tool_round(round);
        self.observation_progress.begin_round(round);
        self.child_inspections.begin_round(round);
    }

    pub(crate) fn begin_repeated_tool_round(&mut self, round: usize) {
        self.repeated_tool_calls.begin_round(round);
    }

    pub(crate) fn observe_repeated_call(&mut self, name: &str, fingerprint: Option<[u8; 32]>) {
        self.repeated_tool_calls.observe(name, fingerprint);
    }

    pub(crate) fn unchanged_child_inspection_targets(&mut self) -> Vec<String> {
        self.child_inspections.finish_round()
    }

    #[cfg(test)]
    pub(crate) fn observation_progress_hint(&mut self) -> Option<&'static str> {
        self.observation_progress.finish_round()
    }

    /// Exact trusted handle, before the feedback packet shortens display IDs.
    pub(crate) fn quiet_command_for_background(&self) -> Option<&str> {
        self.quiet_polls.pending_handle()
    }

    /// Only the provider boundary's newly admitted Inbox records are eligible.
    pub(crate) fn observe_background_exec_completion<'a>(
        &mut self,
        session_id: &str,
        message: &'a bamboo_agent_core::Message,
    ) -> Option<&'a str> {
        let envelope = message.metadata.as_ref()?.get("session_message")?;
        let body = &envelope["body"];
        let data = &body["data"];
        let handle = data["exec_session_id"].as_str()?;
        if envelope["source"]["type"] == "runtime"
            && envelope["source"]["subsystem"] == "background_exec"
            && envelope["kind"] == "runtime_instruction"
            && envelope["target_session_id"] == session_id
            && envelope["correlation_id"] == handle
            && body["instruction"] == "background_exec_completed"
            && data["session_id"] == session_id
            && matches!(
                data["status"].as_str(),
                Some("completed" | "killed" | "error")
            )
        {
            self.quiet_polls.forget_handle(handle);
            Some(handle)
        } else {
            None
        }
    }

    /// Emit a quiet-command advisory only when a new group of polls qualifies.
    /// Consuming that evidence also releases suppression for the next batch.
    pub(crate) fn feedback_for_next_round(&mut self, quiet_poll_active: bool) -> Option<String> {
        let file_hint = self.observation_progress.finish_round();
        let quiet_advisory = if quiet_poll_active {
            self.quiet_polls
                .take_feedback_evidence()
                .map(super::progress_feedback::render_quiet_command_advisory)
        } else {
            None
        };
        let repeated_advisory = self.repeated_tool_calls.advisory(file_hint);
        match (repeated_advisory, quiet_advisory) {
            (Some(repeated), Some(quiet)) => Some(format!("{repeated}\n\n{quiet}")),
            (repeated, quiet) => repeated.or(quiet),
        }
    }

    pub(crate) fn reset_observation_progress(&mut self) {
        self.observation_progress = ObservationProgress::default();
        self.repeated_tool_calls = RepeatedToolCalls::default();
        self.child_inspections = super::child_inspection::ChildInspectionProgress::default();
        self.quiet_polls = super::quiet_poll::QuietPollGuard::default();
    }

    #[cfg(test)]
    pub(super) fn check_before_execution(
        &self,
        tool_call: &ToolCall,
        reserved_calls: usize,
    ) -> Result<(), ToolPolicyPrecheckViolation> {
        self.check_before_execution_for_name(tool_call, reserved_calls, None)
    }

    pub(super) fn check_before_execution_for_name(
        &self,
        tool_call: &ToolCall,
        reserved_calls: usize,
        admitted_execution_name: Option<&str>,
    ) -> Result<(), ToolPolicyPrecheckViolation> {
        let key = tool_policy_key(tool_call, admitted_execution_name);
        let projected_executions = self.executed_calls.saturating_add(reserved_calls);

        if projected_executions >= self.max_tool_calls_per_round {
            return Err(ToolPolicyPrecheckViolation::RoundToolLimit {
                limit: self.max_tool_calls_per_round,
                tool_name: key.name().to_owned(),
            });
        }

        let failures = self.consecutive_failures.get(&key).copied().unwrap_or(0);
        if failures >= self.max_consecutive_failures_per_tool {
            return Err(ToolPolicyPrecheckViolation::ToolCircuitOpen {
                tool_name: key.name().to_owned(),
                consecutive_failures: failures,
                limit: self.max_consecutive_failures_per_tool,
                run_scoped: key.run_scoped(),
            });
        }

        if self.standalone_supervisor_spawn && admitted_execution_name == Some("spawn_session") {
            bamboo_orchestration::managed_session::validate_standalone_spawn_arguments(
                &tool_call.function.arguments,
            )
            .map_err(ToolPolicyPrecheckViolation::StandaloneSpawnArguments)?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub(super) fn observe_outcome(
        &mut self,
        tool_call: &ToolCall,
        outcome: &Result<ToolResult, String>,
    ) {
        self.observe_accounted_outcome(
            tool_call,
            outcome,
            None,
            super::per_call::ShellAccountingOutcome::Ordinary,
        );
    }

    pub(super) fn observe_execution_outcome(
        &mut self,
        tool_call: &ToolCall,
        outcome: &super::per_call::ToolExecutionOutcome,
        admitted_execution_name: Option<&str>,
    ) {
        self.observe_accounted_outcome(
            tool_call,
            &outcome.result,
            admitted_execution_name,
            super::per_call::shell_accounting_outcome(outcome, admitted_execution_name),
        );
    }

    fn observe_accounted_outcome(
        &mut self,
        tool_call: &ToolCall,
        outcome: &Result<ToolResult, String>,
        admitted_execution_name: Option<&str>,
        shell_accounting: super::per_call::ShellAccountingOutcome,
    ) {
        self.executed_calls = self.executed_calls.saturating_add(1);

        let key = tool_policy_key(tool_call, admitted_execution_name);

        let succeeded = match shell_accounting {
            super::per_call::ShellAccountingOutcome::Ordinary => {
                matches!(outcome, Ok(result) if result.success)
            }
            super::per_call::ShellAccountingOutcome::Pending => {
                self.observation_progress.current = None;
                return;
            }
            super::per_call::ShellAccountingOutcome::Terminal(success) => success,
        };

        // Compression/budget enforcement can turn raw success into failure.
        // Such a round must not produce a successful-observation advisory.
        if !succeeded {
            self.observation_progress.current = None;
        }

        if succeeded {
            self.consecutive_failures.remove(&key);
            return;
        }

        *self.consecutive_failures.entry(key).or_insert(0) += 1;
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

    fn fingerprint(name: &str, arguments: serde_json::Value, result: &str) -> Option<[u8; 32]> {
        let call = tool_call(name, "{}");
        let tool =
            super::super::per_call::PortableToolExecution::with_input_fixture(name, arguments);
        dispatched_tool_fingerprint(
            &call,
            &super::super::per_call::ToolExecutionOutcome {
                output_cap: Some(None),
                portable_tool: Some(tool),
                permission_replay_origin: None,
                result: Ok(ToolResult::text(true, result)),
                needs_human: None,
                post_tool_hook_eligible: true,
                managed_running_violation: false,
                tool_duration: std::time::Duration::ZERO,
            },
        )
    }

    #[test]
    fn repeated_tool_advisory_rearms_when_tool_arguments_or_result_change() {
        let first = fingerprint(
            "browser_eval",
            serde_json::json!({"code":"document.title","expected_epoch":17}),
            "title",
        );
        let same = fingerprint(
            "browser_eval",
            serde_json::from_str(r#"{"expected_epoch":17,"code":"document.title"}"#).unwrap(),
            "title",
        );
        assert!(first.is_some());
        assert_eq!(first, same);
        for changed in [
            fingerprint(
                "browser_eval",
                serde_json::json!({"code":"document.title","expected_epoch":18}),
                "title",
            ),
            fingerprint(
                "browser_eval",
                serde_json::json!({"code":"document.title","expected_epoch":17}),
                "new title",
            ),
            fingerprint(
                "vendor::browser_eval",
                serde_json::json!({"code":"document.title","expected_epoch":17}),
                "title",
            ),
            None,
        ] {
            assert_ne!(first, changed);
            let mut repeats = RepeatedToolCalls::default();
            for observed in [first, same] {
                repeats.observe("browser_eval", observed);
                assert!(repeats.advisory(None).is_none());
            }
            repeats.observe("browser_eval", first);
            assert!(repeats.advisory(None).unwrap().contains("3 times in a row"));
            repeats.observe("browser_eval", same);
            assert!(repeats.advisory(None).is_none());
            for observed in [changed, first, same] {
                repeats.observe("browser_eval", observed);
                assert!(repeats.advisory(None).is_none());
            }
            repeats.observe("browser_eval", first);
            assert!(repeats.advisory(None).unwrap().contains("3 times in a row"));
        }
    }

    #[test]
    fn shell_poll_comparison_keeps_process_and_output_changes() {
        use serde_json::json;
        let args = json!({"session_id":"process-a","chars":""});
        let before = json!({
            "session_id":"process-a",
            "status":"running",
            "output":"",
            "polling":{
                "terminal_result_ttl_ms":300000,
                "state":"waiting",
                "elapsed_ms":1000,
                "output_idle_ms":500,
                "consecutive_empty_readouts":2,
                "exit_observed":false
            }
        });
        let capture =
            |tool, value: &serde_json::Value| fingerprint(tool, args.clone(), &value.to_string());
        let initial = capture("write_stdin", &before);
        assert!(initial.is_some());
        let mut later = before.clone();
        later["polling"]["elapsed_ms"] = json!(92000);
        later["polling"]["output_idle_ms"] = json!(91500);
        later["polling"]["state"] = json!("needs_diagnosis");
        later["polling"]["consecutive_empty_readouts"] = json!(4);
        later["polling"]["advisory"] = json!("wait for the command or inspect it");
        assert_eq!(initial, capture("write_stdin", &later));
        assert_ne!(
            capture("mcp__terminal__write_stdin", &before),
            capture("mcp__terminal__write_stdin", &later)
        );
        let mut exited = later.clone();
        exited["polling"]["exit_observed"] = json!(true);
        let mut other_process = later.clone();
        other_process["session_id"] = json!("process-b");
        let mut new_output = later;
        new_output["output"] = json!("new bytes");
        for changed in [
            exited,
            other_process,
            new_output,
            json!({"status":"completed","exit_code":0,"output":""}),
        ] {
            assert_ne!(initial, capture("write_stdin", &changed));
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
    fn pending_shell_calls_consume_round_allowance_without_resetting_failure_streaks() {
        let call = tool_call("default::exec_command", "{}");
        let outcome =
            |status: &str, needs_human: bool| super::super::per_call::ToolExecutionOutcome {
                output_cap: None,
                portable_tool: None,
                permission_replay_origin: None,
                result: Ok(ToolResult::text(
                    true,
                    serde_json::json!({"status":status, "exit_code":7, "output":""}).to_string(),
                )),
                needs_human: needs_human.then(|| bamboo_agent_core::PendingQuestion {
                    tool_call_id: call.id.clone(),
                    tool_name: call.function.name.clone(),
                    question: "Approve?".into(),
                    options: vec![],
                    allow_custom: false,
                    source: bamboo_agent_core::PendingQuestionSource::PauseTool,
                }),
                post_tool_hook_eligible: false,
                managed_running_violation: false,
                tool_duration: std::time::Duration::ZERO,
            };
        let mut guard = ToolPolicyGuard::new(4, 3);
        for _ in 0..2 {
            guard.observe_execution_outcome(
                &call,
                &outcome("completed", false),
                Some("exec_command"),
            );
        }
        guard.begin_observation_round(0);
        guard.observe_raw_observation(
            &tool_call("Read", "{}"),
            &Ok(ToolResult::text(true, "same file")),
        );
        assert!(guard.observation_progress.current.is_some());
        guard.observe_execution_outcome(&call, &outcome("running", false), Some("exec_command"));
        assert!(guard.observation_progress.current.is_none());
        guard.observe_execution_outcome(&call, &outcome("completed", true), Some("exec_command"));
        assert_eq!(
            guard
                .consecutive_failures
                .get(&ToolCircuitKey::Tool("exec_command".into())),
            Some(&2)
        );
        assert!(matches!(
            guard.check_before_execution_for_name(&call, 0, Some("exec_command")),
            Err(ToolPolicyPrecheckViolation::RoundToolLimit { .. })
        ));
        let mut error = outcome("completed", false);
        error.result = Err("transport failed before dispatch".into());
        guard.observe_execution_outcome(&call, &error, Some("exec_command"));
        guard.max_tool_calls_per_round = 80;
        assert!(matches!(
            guard.check_before_execution_for_name(&call, 0, Some("exec_command")),
            Err(ToolPolicyPrecheckViolation::ToolCircuitOpen { .. })
        ));
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
    fn inspection_failures_do_not_spend_the_delegation_circuit() {
        let mut guard = ToolPolicyGuard::default();
        let inspect = tool_call(
            "SubAgent",
            r#"{"intent":"inspect","target":"child","message":"result"}"#,
        );
        let create = tool_call("SubAgent", r#"{"role":"implementer","message":"task"}"#);
        for _ in 0..2 {
            guard.begin_round(80, 3);
            guard.observe_outcome(&inspect, &Err("bad cursor".into()));
        }
        guard.begin_round(80, 3);
        guard.observe_outcome(&create, &Err("budget exceeded".into()));
        assert!(guard.delegation_failure_message().is_none());
        assert!(guard.check_before_execution(&create, 0).is_ok());
        assert_eq!(
            guard
                .consecutive_failures
                .get(&ToolCircuitKey::SubAgentInspection),
            Some(&2)
        );
        assert_eq!(
            guard
                .consecutive_failures
                .get(&ToolCircuitKey::Tool("SubAgent".into())),
            Some(&1)
        );

        guard.observe_outcome(&inspect, &Err("bad cursor".into()));
        guard.begin_round(80, 3);
        assert!(matches!(
            guard.check_before_execution(&inspect, 0),
            Err(ToolPolicyPrecheckViolation::ToolCircuitOpen { .. })
        ));
        assert!(guard.check_before_execution(&create, 0).is_ok());
        assert!(guard.delegation_failure_message().is_none());
        guard.observe_outcome(&create, &Ok(ToolResult::text(true, "child created")));
        assert!(guard.delegation_failure_message().is_none());
        assert!(guard.check_before_execution(&inspect, 0).is_err());
    }

    #[test]
    fn inspection_circuit_only_accepts_unambiguous_read_routes() {
        for args in [
            r#"{"intent":"inspect","target":"child","message":"messages"}"#,
            r#"{"intent":"inspect","target":"child","role":null,"reply_to":""}"#,
            r#"{"action":"get","child_session_id":"child"}"#,
            r#"{"intent":"inspect"}"#,
            r#"{"action":"list_models"}"#,
        ] {
            assert_eq!(
                tool_policy_key(&tool_call("SubAgent", args), Some("SubAgent")),
                ToolCircuitKey::SubAgentInspection
            );
        }
        for args in [
            r#"{"intent":"inspect","action":"create","message":"task"}"#,
            r#"{"intent":"inspect","target":"child","reply_to":"request"}"#,
            r#"{"intent":"inspect","role":"implementer"}"#,
            r#"{"intent":"inspect","unknown":"value"}"#,
            r#"{"action":"get","target":"child"}"#,
            r#"{"action":"list"}"#,
            r#"{"target":"child","message":"result"}"#,
            "not-json",
        ] {
            assert_eq!(
                tool_policy_key(&tool_call("SubAgent", args), Some("SubAgent")),
                ToolCircuitKey::Tool("SubAgent".into())
            );
        }
        assert_eq!(
            tool_policy_key(
                &tool_call("vendor::SubAgent", r#"{"intent":"inspect"}"#),
                Some("vendor::SubAgent")
            ),
            ToolCircuitKey::Tool("vendor::SubAgent".into())
        );
        assert_eq!(
            tool_policy_key(
                &tool_call(SUBAGENT_INSPECTION_LABEL, r#"{"intent":"inspect"}"#),
                Some(SUBAGENT_INSPECTION_LABEL)
            ),
            ToolCircuitKey::Tool(SUBAGENT_INSPECTION_LABEL.into())
        );
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
        fresh.reset_observation_progress();
        assert!(!observation_round(&mut fresh, 3, &calls));
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
            assert!(!observation_round(
                &mut guard,
                0,
                std::slice::from_ref(&observation)
            ));
            assert!(!observation_round(
                &mut guard,
                1,
                std::slice::from_ref(&observation)
            ));
            assert!(!observation_round(
                &mut guard,
                2,
                &[observation.clone(), breaker]
            ));
            assert!(!observation_round(
                &mut guard,
                3,
                std::slice::from_ref(&observation)
            ));
            assert!(!observation_round(
                &mut guard,
                4,
                std::slice::from_ref(&observation)
            ));
            assert!(observation_round(
                &mut guard,
                5,
                std::slice::from_ref(&observation)
            ));
        }
    }
    #[test]
    fn versioned_spawn_policy_accounting_keeps_root_and_child_failure_circuits_independent() {
        for (owner, reference) in [
            ("spawn_session", "default::spawnSession"),
            ("vendor::spawn_session", "vendor::spawn_session"),
            ("spawnSession", "spawnSession"),
        ] {
            let call = tool_call(reference, "{}");
            let child = tool_call("SubAgent", "{}");
            let mut guard = ToolPolicyGuard::default();
            for _ in 0..3 {
                guard.begin_round(80, 3);
                guard.observe_accounted_outcome(
                    &call,
                    &Err("correctable Project/workspace input".into()),
                    Some(owner),
                    super::super::per_call::ShellAccountingOutcome::Ordinary,
                );
                assert!(
                    guard
                        .check_before_execution_for_name(&child, 0, Some("SubAgent"))
                        .is_ok(),
                    "{owner}"
                );
            }
            assert!(guard.delegation_failure_message().is_none(), "{owner}");
            guard.begin_round(80, 3);
            assert!(
                guard
                    .check_before_execution_for_name(&call, 0, Some(owner))
                    .is_ok(),
                "{owner}"
            );
            for _ in 0..3 {
                guard.observe_accounted_outcome(
                    &call,
                    &Err("same exact owner failed again".into()),
                    Some(owner),
                    super::super::per_call::ShellAccountingOutcome::Ordinary,
                );
            }
            assert!(guard
                .check_before_execution_for_name(&call, 0, Some(owner))
                .is_err());
            for other_owner in ["spawn_session", "vendor::spawn_session", "spawnSession"] {
                if other_owner != owner {
                    let other = tool_call(other_owner, "{}");
                    assert!(
                        guard.check_before_execution_for_name(&other, 0, Some(other_owner)).is_ok(),
                        "{owner} failures must not open the distinct exact owner {other_owner} circuit"
                    );
                }
            }
            assert!(guard
                .check_before_execution_for_name(&child, 0, Some("SubAgent"))
                .is_ok());
            guard.begin_round(80, 3);
            assert!(
                guard
                    .check_before_execution_for_name(&call, 0, Some(owner))
                    .is_ok(),
                "{owner}"
            );
            for _ in 0..3 {
                guard.observe_accounted_outcome(
                    &child,
                    &Err("actual Child delegation failed".into()),
                    Some("SubAgent"),
                    super::super::per_call::ShellAccountingOutcome::Ordinary,
                );
            }
            guard.begin_round(80, 3);
            assert!(guard
                .check_before_execution_for_name(&child, 0, Some("SubAgent"))
                .is_err());
            assert!(
                guard
                    .check_before_execution_for_name(&call, 0, Some(owner))
                    .is_ok(),
                "{owner}"
            );
            assert!(guard.delegation_failure_message().is_some());
        }
    }
}
