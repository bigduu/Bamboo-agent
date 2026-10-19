//! Pair same-batch redundant polls and rearm real capture after model feedback.
//! Only the admitted shell identity and raw successful results participate.
use std::collections::HashMap;

use bamboo_agent_core::tools::{parse_tool_args, ToolCall, ToolResult};

use super::per_call::ToolExecutionOutcome;

const MAX_HANDLES: usize = 64;
const SILENCE_MS: u64 = 90_000;
// The first qualifying readout already carries the reminder; allow three more.
const READOUTS_AFTER_REMINDER: usize = 4;

#[derive(Debug, Clone, Default)]
pub(super) struct QuietPollGuard {
    readouts: HashMap<String, usize>,
    pending: Option<String>,
    last_quiet: Option<super::progress_feedback::ProgressEvidence>,
    observed_progress: bool,
}

fn poll_args(call: &ToolCall) -> Option<(String, bool)> {
    let args = parse_tool_args(&call.function.arguments).ok()?;
    let object = args.as_object()?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "session_id" | "chars" | "eof" | "yield_time_ms" | "max_output_tokens"
        )
    }) {
        return None;
    }
    if args
        .get("yield_time_ms")
        .is_some_and(|v| !v.is_null() && v.as_u64().is_none())
        || args.get("max_output_tokens").is_some_and(|v| {
            !v.is_null() && v.as_u64().and_then(|n| usize::try_from(n).ok()).is_none()
        })
    {
        return None;
    }
    let id = args.get("session_id")?.as_str()?;
    if id.is_empty() || id.len() > 256 {
        return None;
    }
    let chars = match args.get("chars") {
        None => "",
        Some(value) => value.as_str()?,
    };
    let eof = match args.get("eof") {
        None => false,
        Some(value) => value.as_bool()?,
    };
    Some((id.to_owned(), chars.is_empty() && !eof))
}

impl QuietPollGuard {
    pub(super) fn observe(
        &mut self,
        call: &ToolCall,
        outcome: &ToolExecutionOutcome,
        admitted: Option<&str>,
    ) {
        let identity = outcome
            .portable_tool
            .as_ref()
            .map(|tool| tool.execution_name())
            .or(admitted);
        if identity != Some("write_stdin") || !outcome.post_tool_hook_eligible {
            return;
        }
        let Some((id, empty_poll)) = poll_args(call) else {
            return;
        };
        let raw = outcome
            .result
            .as_ref()
            .ok()
            .filter(|result| result.success && result.images.is_empty())
            .and_then(|result| serde_json::from_str::<serde_json::Value>(&result.result).ok());
        if let Some(value) = raw.as_ref() {
            let owned_result = value["session_id"].as_str() == Some(id.as_str())
                // Terminal exec results intentionally omit their collected handle.
                || (value["status"] == "completed" && value.get("session_id").is_none());
            if owned_result
                && (value["output"]
                    .as_str()
                    .is_some_and(|output| !output.is_empty())
                    || value["output_omitted_bytes"]
                        .as_u64()
                        .is_some_and(|bytes| bytes > 0)
                    || value["status"] == "completed"
                    || value["polling"]["exit_observed"] == true)
            {
                self.observed_progress = true;
            }
        }
        let quiet = empty_poll
            && outcome
                .result
                .as_ref()
                .ok()
                .filter(|r| r.success && r.images.is_empty())
                .and_then(|r| serde_json::from_str::<serde_json::Value>(&r.result).ok())
                .is_some_and(|value| {
                    value["session_id"].as_str() == Some(id.as_str())
                        && value["status"] == "running"
                        && value["polling"]["exit_observed"] == false
                        && value["polling"]["elapsed_ms"].as_u64().is_some()
                        && value["output"] == ""
                        && value
                            .get("output_omitted_bytes")
                            .is_none_or(|v| v.as_u64() == Some(0))
                        && value["polling"]["output_idle_ms"]
                            .as_u64()
                            .is_some_and(|ms| ms >= SILENCE_MS)
                        && value["polling"]["consecutive_empty_readouts"]
                            .as_u64()
                            .is_some_and(|n| n >= 3)
                });
        if !quiet {
            self.readouts.remove(&id);
            if self.pending.as_deref() == Some(id.as_str()) {
                self.pending = None;
                self.last_quiet = None;
            }
            return;
        }
        if self.readouts.len() >= MAX_HANDLES && !self.readouts.contains_key(&id) {
            return;
        }
        let count = self.readouts.entry(id.clone()).or_default();
        *count = count.saturating_add(1);
        if *count >= READOUTS_AFTER_REMINDER {
            let value = raw.as_ref().expect("qualifying quiet result was parsed");
            self.last_quiet = Some(super::progress_feedback::ProgressEvidence::QuietCommand {
                session_id: id.clone(),
                tool_call_id: call.id.chars().take(129).collect(),
                elapsed_ms: value["polling"]["elapsed_ms"].as_u64().unwrap_or(0),
                output_idle_ms: value["polling"]["output_idle_ms"].as_u64().unwrap_or(0),
                consecutive_empty_readouts: value["polling"]["consecutive_empty_readouts"]
                    .as_u64()
                    .unwrap_or(0),
            });
            self.pending = Some(id);
        }
    }

    pub(super) fn pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Once the complete batch is paired, feedback releases its suppression.
    /// The next model-requested read must reach the real owned capture again.
    pub(super) fn take_feedback_evidence(
        &mut self,
    ) -> Option<super::progress_feedback::ProgressEvidence> {
        let id = self.pending.take()?;
        self.readouts.remove(&id);
        self.last_quiet.take()
    }

    pub(super) fn take_observed_progress(&mut self) -> bool {
        std::mem::take(&mut self.observed_progress)
    }

    pub(super) fn retain_owned_handles(&mut self, handles: &[String]) {
        self.readouts.retain(|id, _| handles.contains(id));
        if self
            .pending
            .as_ref()
            .is_some_and(|id| !handles.contains(id))
        {
            self.pending = None;
            self.last_quiet = None;
        }
    }

    pub(super) fn suppress(&self, call: &ToolCall, admitted: Option<&str>) -> Option<ToolResult> {
        if admitted != Some("write_stdin") {
            return None;
        }
        let (id, empty_poll) = poll_args(call)?;
        if !empty_poll || self.pending.as_deref() != Some(id.as_str()) {
            return None;
        }
        // Pair every planned call, without another wait or capture read. This
        // is a policy observation, not a new process/exit-status observation.
        Some(ToolResult::text(
            true,
            serde_json::json!({
                "session_id":id, "status":"running", "output":"",
                "polling":{"state":"diagnosis_pending", "poll_suppressed":true},
            })
            .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::FunctionCall;

    fn call(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "probe".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: args.to_string(),
            },
        }
    }

    fn outcome(tick: u64, output: &str) -> ToolExecutionOutcome {
        ToolExecutionOutcome {
            output_cap: None,
            permission_replay_origin: None,
            portable_tool: None,
            post_tool_hook_eligible: true,
            needs_human: None,
            managed_running_violation: false,
            tool_duration: std::time::Duration::ZERO,
            result: Ok(ToolResult::text(
                true,
                serde_json::json!({
                    "session_id":"handle", "status":"running", "output":output,
                    "polling":{"elapsed_ms":tick, "output_idle_ms":90_000+tick,
                        "consecutive_empty_readouts":3+tick, "exit_observed":false},
                })
                .to_string(),
            )),
        }
    }

    #[test]
    fn timestamps_yield_caps_aliases_and_key_order_cannot_hide_repeated_empty_polls() {
        let mut guard = QuietPollGuard::default();
        for tick in 0..4 {
            let name = if tick % 2 == 0 {
                "write_stdin"
            } else {
                "default::write_stdin"
            };
            let poll = call(
                name,
                serde_json::json!({"session_id":"handle", "chars":"",
                "yield_time_ms":250+tick*1000,"max_output_tokens":tick+1}),
            );
            guard.observe(&poll, &outcome(tick, ""), Some("write_stdin"));
            assert_eq!(guard.pending(), tick == 3);
        }
        let poll = call("write_stdin", serde_json::json!({"session_id":"handle"}));
        let suppressed = guard.suppress(&poll, Some("write_stdin")).unwrap();
        let value: serde_json::Value = serde_json::from_str(&suppressed.result).unwrap();
        assert_eq!(value["status"], "running");
        assert_eq!(value["session_id"], "handle");
        assert_eq!(value["polling"]["poll_suppressed"], true);
        assert!(value.get("exit_code").is_none());
        for invalid in [
            serde_json::json!({"session_id":"handle", "unexpected":true}),
            serde_json::json!({"session_id":"handle", "yield_time_ms":"250"}),
            serde_json::json!({"session_id":"handle", "eof":null}),
        ] {
            assert!(guard
                .suppress(&call("write_stdin", invalid), Some("write_stdin"))
                .is_none());
        }
        assert!(guard.suppress(&poll, Some("vendor::write_stdin")).is_none());
        assert!(guard
            .suppress(
                &call("write_stdin", serde_json::json!({"session_id":"other"})),
                Some("write_stdin")
            )
            .is_none());
        assert!(guard
            .suppress(
                &call("Read", serde_json::json!({"session_id":"handle"})),
                Some("Read")
            )
            .is_none());
        assert!(guard
            .suppress(
                &call(
                    "write_stdin",
                    serde_json::json!({"session_id":"handle","eof":true})
                ),
                Some("write_stdin")
            )
            .is_none());
        // Explicit input remains dispatchable and real progress releases the gate.
        let input = call(
            "write_stdin",
            serde_json::json!({"session_id":"handle","chars":"answer\n"}),
        );
        assert!(guard.suppress(&input, Some("write_stdin")).is_none());
        guard.observe(&input, &outcome(5, ""), Some("write_stdin"));
        assert!(!guard.pending());
        guard.observe(&poll, &outcome(6, "new output"), Some("write_stdin"));
        assert!(!guard.pending());
    }

    #[test]
    fn omitted_output_unknown_metadata_errors_and_foreign_tools_are_not_quiet_progress() {
        let poll = call("write_stdin", serde_json::json!({"session_id":"handle"}));
        for variant in 0..6 {
            let mut guard = QuietPollGuard::default();
            for tick in 0..3 {
                guard.observe(&poll, &outcome(tick, ""), Some("write_stdin"));
            }
            let mut result = outcome(4, "");
            let mut value: serde_json::Value =
                serde_json::from_str(&result.result.as_ref().unwrap().result).unwrap();
            let identity = match variant {
                0 => {
                    value["output_omitted_bytes"] = 1.into();
                    "write_stdin"
                }
                1 => {
                    value.as_object_mut().unwrap().remove("polling");
                    "write_stdin"
                }
                2 => {
                    value["status"] = "completed".into();
                    value["exit_code"] = 7.into();
                    "write_stdin"
                }
                3 => {
                    value["polling"]["output_idle_ms"] = 89_999.into();
                    "write_stdin"
                }
                4 => "vendor::write_stdin",
                _ => "default::write_stdin",
            };
            result.result = Ok(ToolResult::text(true, value.to_string()));
            guard.observe(&poll, &result, Some(identity));
            assert!(!guard.pending());
        }
        let mut guard = QuietPollGuard::default();
        for tick in 0..3 {
            guard.observe(&poll, &outcome(tick, ""), Some("write_stdin"));
        }
        let mut failed = outcome(4, "");
        failed.result = Err("read failed".into());
        guard.observe(&poll, &failed, Some("write_stdin"));
        assert!(!guard.pending());
        guard.observe(&poll, &outcome(5, ""), Some("write_stdin"));
        assert!(!guard.pending());
    }

    #[test]
    fn expired_or_foreign_handles_release_the_bounded_guard_capacity() {
        let mut guard = QuietPollGuard::default();
        let mut handles = Vec::new();
        for index in 0..MAX_HANDLES {
            let id = format!("old-{index}");
            handles.push(id.clone());
            let poll = call("write_stdin", serde_json::json!({"session_id":id}));
            let mut result = outcome(1, "");
            let mut value: serde_json::Value =
                serde_json::from_str(&result.result.as_ref().unwrap().result).unwrap();
            value["session_id"] = id.into();
            result.result = Ok(ToolResult::text(true, value.to_string()));
            for _ in 0..READOUTS_AFTER_REMINDER {
                guard.observe(&poll, &result, Some("write_stdin"));
            }
        }
        guard.retain_owned_handles(&handles);
        assert!(guard.pending());
        let old = call(
            "write_stdin",
            serde_json::json!({"session_id":handles.last().unwrap()}),
        );
        assert!(guard.suppress(&old, Some("write_stdin")).is_some());
        // Cache expiry, terminal consumption, or a different owner snapshot
        // must free policy capacity as well as release any pending suppression.
        guard.retain_owned_handles(&["handle".into()]);
        assert!(!guard.pending());
        assert!(guard.suppress(&old, Some("write_stdin")).is_none());
        let current = call("write_stdin", serde_json::json!({"session_id":"handle"}));
        for tick in 0..READOUTS_AFTER_REMINDER as u64 {
            guard.observe(&current, &outcome(tick, ""), Some("write_stdin"));
        }
        assert!(guard.suppress(&current, Some("write_stdin")).is_some());
        guard.retain_owned_handles(&[]);
        assert!(!guard.pending());
    }

    #[test]
    fn feedback_consumption_rearms_a_real_poll_for_late_output() {
        let mut guard = QuietPollGuard::default();
        let poll = call("write_stdin", serde_json::json!({"session_id":"handle"}));
        for tick in 0..4 {
            guard.observe(&poll, &outcome(tick, ""), Some("write_stdin"));
        }
        assert!(guard.suppress(&poll, Some("write_stdin")).is_some());
        assert!(matches!(
            guard.take_feedback_evidence(),
            Some(super::super::progress_feedback::ProgressEvidence::QuietCommand { .. })
        ));
        assert!(
            guard.suppress(&poll, Some("write_stdin")).is_none(),
            "the next model round must reach the real owned capture"
        );
        guard.observe(&poll, &outcome(4, "late output"), Some("write_stdin"));
        assert!(guard.take_observed_progress());
        assert!(!guard.take_observed_progress());
        assert!(!guard.pending());
    }

    #[test]
    fn observed_exit_is_never_reported_as_a_quiet_no_exit_fact() {
        let mut guard = QuietPollGuard::default();
        let poll = call("write_stdin", serde_json::json!({"session_id":"handle"}));
        let mut result = outcome(4, "");
        let mut raw: serde_json::Value =
            serde_json::from_str(&result.result.as_ref().unwrap().result).unwrap();
        raw["polling"]["exit_observed"] = serde_json::json!(true);
        result.result.as_mut().unwrap().result = raw.to_string();
        for _ in 0..8 {
            guard.observe(&poll, &result, Some("write_stdin"));
        }
        assert!(!guard.pending());
        assert!(guard.take_feedback_evidence().is_none());
        assert!(guard.take_observed_progress());
    }

    #[test]
    fn collected_terminal_result_releases_recovery_even_without_a_numeric_exit_code() {
        let mut guard = QuietPollGuard::default();
        let poll = call("write_stdin", serde_json::json!({"session_id":"handle"}));
        for tick in 0..4 {
            guard.observe(&poll, &outcome(tick, ""), Some("write_stdin"));
        }
        guard.take_feedback_evidence().unwrap();
        let mut terminal = outcome(5, "");
        terminal.result.as_mut().unwrap().result = serde_json::json!({
            "status":"completed", "output":"", "exit_code":null,
            "polling":{"exit_observed":true},
        })
        .to_string();
        guard.observe(&poll, &terminal, Some("write_stdin"));
        assert!(guard.take_observed_progress());
        assert!(!guard.pending());
        assert!(guard.suppress(&poll, Some("write_stdin")).is_none());
    }
}
