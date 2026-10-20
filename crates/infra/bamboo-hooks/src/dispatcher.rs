use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bamboo_agent_core::{AgentHook, Session};
use bamboo_config::LifecycleHooksConfig;
use bamboo_domain::{AgentHookPoint, HookPayload, HookResult};

/// Aggregate control and context output from one lifecycle seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRunOutcome {
    pub decision: HookResult,
    pub injected_contexts: Vec<String>,
    pub plugin_contexts: Vec<crate::portable::PluginContext>,
}

impl Default for HookRunOutcome {
    fn default() -> Self {
        Self {
            decision: HookResult::Continue,
            injected_contexts: Vec::new(),
            plugin_contexts: Vec::new(),
        }
    }
}

/// One completed handler execution, returned to the engine for observability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookExecution {
    pub hook_name: String,
    pub point: AgentHookPoint,
    pub duration_ms: u64,
    pub result: HookResult,
}

/// Complete dispatch result for one lifecycle seam.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HookDispatchReport {
    pub outcome: HookRunOutcome,
    pub executions: Vec<HookExecution>,
}

/// Registry and deterministic dispatcher for programmatic and configured hooks.
#[derive(Clone)]
struct RegisteredHook {
    hook: Arc<dyn AgentHook>,
    may_suspend: bool,
}

#[derive(Clone, Default)]
pub struct HookDispatcher {
    hooks: Vec<RegisteredHook>,
}

impl HookDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a hook. Lower numeric priorities run first.
    pub fn register(&mut self, hook: Arc<dyn AgentHook>) {
        let may_suspend = hook.may_suspend();
        self.hooks.push(RegisteredHook { hook, may_suspend });
        self.hooks
            .sort_by_key(|registered| registered.hook.priority());
    }

    /// Clone this registry and append configured handlers from a frozen config
    /// snapshot. The original registry remains reusable by future runs.
    pub fn with_lifecycle_config(
        &self,
        config: &LifecycleHooksConfig,
        fallback_cwd: Option<PathBuf>,
    ) -> Self {
        let mut dispatcher = self.clone();
        crate::configured::register_configured_hooks(&mut dispatcher, config, fallback_cwd);
        dispatcher
    }

    pub async fn run_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
    ) -> HookDispatchReport {
        self.run_hooks_with_control(point, payload, session, true, None, None)
            .await
    }

    /// Preserve the original native hook payload while allowing a resolved
    /// builtin shell identity and its legacy projection as fallback matches.
    /// Foreign identities are never normalized by this compatibility seam.
    pub async fn run_hooks_with_resolved_shell(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        resolved_tool_name: Option<&str>,
        original_tool_input: Option<&serde_json::Value>,
    ) -> HookDispatchReport {
        self.run_hooks_with_control(
            point,
            payload,
            session,
            true,
            resolved_tool_name,
            original_tool_input,
        )
        .await
    }

    /// Run all matching handlers without allowing a control decision to
    /// suppress later observer handlers.
    pub async fn run_observer_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
    ) -> HookDispatchReport {
        self.run_hooks_with_control(point, payload, session, false, None, None)
            .await
    }

    async fn run_hooks_with_control(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        honor_control_decisions: bool,
        resolved_tool_name: Option<&str>,
        original_tool_input: Option<&serde_json::Value>,
    ) -> HookDispatchReport {
        let mut report = HookDispatchReport::default();
        let resolved_shell = resolved_tool_name
            .filter(|name| matches!(*name, "exec_command" | "write_stdin"))
            .and_then(|name| {
                let mut resolved = payload.clone();
                match &mut resolved {
                    HookPayload::ToolExecution { tool_name, .. }
                    | HookPayload::ToolResult { tool_name, .. } => {
                        *tool_name = name.to_owned();
                        Some(resolved)
                    }
                    _ => None,
                }
            });
        let legacy = crate::shell_compatibility::legacy_shell_projections(
            resolved_shell.as_ref().unwrap_or(payload),
            session,
            original_tool_input,
        );

        let complete_operation_checks = crate::requires_complete_shell_policy_checks(
            point,
            resolved_shell.as_ref().unwrap_or(payload),
            session,
            original_tool_input,
        );
        let mut pending_composite_ask = false;

        for registered in &self.hooks {
            let hook = &registered.hook;
            if hook.point() != point {
                continue;
            }
            // Original/resolved-name and wildcard handlers run once. A legacy-
            // only matcher must evaluate each distinct operation it covers:
            // stdin/kill also reads output, whose policy can independently deny.
            let selected: Vec<_> = if hook.matches(payload) {
                vec![(payload, false)]
            } else if let Some(resolved) = resolved_shell
                .as_ref()
                .filter(|resolved| hook.matches(resolved))
            {
                vec![(resolved, false)]
            } else {
                legacy
                    .iter()
                    .filter(|projected| hook.matches(&projected.payload))
                    .map(|projected| (&projected.payload, projected.restriction_only))
                    .collect()
            };

            let mut deferred_ask = false;
            for (selected_payload, restriction_only) in selected {
                let started = Instant::now();
                let result = hook.run(point, selected_payload, session).await;
                let result = if registered.may_suspend {
                    result
                } else {
                    enforce_non_suspending_hook(result)
                };
                report.executions.push(HookExecution {
                    hook_name: hook.name().to_string(),
                    point,
                    duration_ms: started.elapsed().as_millis() as u64,
                    result: result.clone(),
                });

                let (result, mut contexts) = unwrap_context_result(result);
                report.outcome.injected_contexts.append(&mut contexts);
                let result = if restriction_only && matches!(result, HookResult::Allow) {
                    // An output-only legacy policy may restrict the read side of a
                    // combined continuation, but cannot grant stdin/kill authority.
                    HookResult::Continue
                } else {
                    result
                };

                match &result {
                    HookResult::Abort { .. }
                    | HookResult::Suspend { .. }
                    | HookResult::Deny { .. } => {
                        if honor_control_decisions {
                            report.outcome.decision = result;
                            return report;
                        }
                    }
                    HookResult::Ask => {
                        // Approval of the input side cannot override an unchecked
                        // output denial from this same legacy-only handler.
                        deferred_ask |= honor_control_decisions;
                    }
                    HookResult::InjectContext { text } => {
                        report.outcome.injected_contexts.push(text.clone());
                    }
                    HookResult::Mutated => {
                        if matches!(report.outcome.decision, HookResult::Continue) {
                            report.outcome.decision = HookResult::Mutated;
                        }
                    }
                    HookResult::Allow => report.outcome.decision = HookResult::Allow,
                    HookResult::Continue => {}
                    HookResult::WithContext { .. } => unreachable!("context results are unwrapped"),
                }
            }
            if deferred_ask {
                if complete_operation_checks {
                    // A separate handler may still restrict the output side.
                    pending_composite_ask = true;
                } else {
                    report.outcome.decision = HookResult::Ask;
                    return report;
                }
            }
        }

        if pending_composite_ask {
            report.outcome.decision = HookResult::Ask;
        }
        report
    }

    /// Native control hooks are conservatively resumable unless their host
    /// implementation explicitly declares the non-suspending contract.
    pub fn has_suspending_hooks_for(&self, point: AgentHookPoint) -> bool {
        self.hooks
            .iter()
            .any(|registered| registered.hook.point() == point && registered.may_suspend)
    }

    pub fn has_hooks_for(&self, point: AgentHookPoint) -> bool {
        self.hooks
            .iter()
            .any(|registered| registered.hook.point() == point)
    }

    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

fn unwrap_context_result(mut result: HookResult) -> (HookResult, Vec<String>) {
    let mut contexts = Vec::new();
    while let HookResult::WithContext {
        result: inner,
        text,
    } = result
    {
        if !text.trim().is_empty() {
            contexts.push(text);
        }
        result = *inner;
    }
    (result, contexts)
}

fn enforce_non_suspending_hook(result: HookResult) -> HookResult {
    match result {
        HookResult::Suspend { reason } => HookResult::Abort {
            reason: format!("non-suspending native hook requested suspension: {reason}"),
        },
        HookResult::WithContext { result, text } => HookResult::WithContext {
            result: Box::new(enforce_non_suspending_hook(*result)),
            text,
        },
        result => result,
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn combined_shell_ask_checks_later_native_output_policy() {
        for name in ["write_stdin", "default::write_stdin"] {
            for args in [
                json!({"session_id":"exec", "chars":"yes"}),
                json!({"session_id":"exec", "eof":true}),
                json!({"session_id":"exec", "chars":"\u{3}"}),
            ] {
                for output in [
                    HookResult::Deny {
                        reason: "output denied".into(),
                    },
                    HookResult::Abort {
                        reason: "output aborted".into(),
                    },
                    HookResult::Suspend {
                        reason: "output suspended".into(),
                    },
                    HookResult::Continue,
                    HookResult::Allow,
                    HookResult::Mutated,
                ] {
                    for primary_matcher in ["^(BashInput|KillShell)$", "^write_stdin$", ".*"] {
                        let mut dispatcher = HookDispatcher::new();
                        let seen = Arc::new(Mutex::new(Vec::new()));
                        for (matcher, result) in [
                            (primary_matcher, HookResult::Ask),
                            ("^BashOutput$", output.clone()),
                        ] {
                            dispatcher.register(Arc::new(RecordingHook {
                                point: AgentHookPoint::BeforeToolExecution,
                                matcher: regex::Regex::new(matcher).unwrap(),
                                seen: seen.clone(),
                                result,
                            }));
                        }
                        let expected = if matches!(
                            output,
                            HookResult::Deny { .. }
                                | HookResult::Abort { .. }
                                | HookResult::Suspend { .. }
                        ) {
                            output.clone()
                        } else {
                            HookResult::Ask
                        };
                        let report = dispatcher
                            .run_hooks_with_resolved_shell(
                                AgentHookPoint::BeforeToolExecution,
                                &HookPayload::ToolExecution {
                                    tool_name: name.into(),
                                    tool_call_id: "combined-ask".into(),
                                    parsed_args: args.clone(),
                                },
                                &Session::new("combined-ask", "model"),
                                Some("write_stdin"),
                                Some(&args),
                            )
                            .await;
                        assert_eq!(report.outcome.decision, expected, "{name}: {args}");
                        assert_eq!(
                            report.executions.len(),
                            2,
                            "distinct handlers must both run"
                        );
                        assert!(
                            matches!(&seen.lock().unwrap()[1], HookPayload::ToolExecution { tool_name, .. } if tool_name == "BashOutput")
                        );
                    }
                }
            }
        }
        // These single-operation calls retain the existing Ask short circuit.
        for (name, args) in [
            ("BashInput", json!({"bash_id":"legacy", "input":"yes"})),
            ("KillShell", json!({"bash_id":"legacy"})),
            ("exec_command", json!({"cmd":"echo yes"})),
            ("write_stdin", json!({"session_id":"exec", "chars":""})),
            (
                "vendor::write_stdin",
                json!({"session_id":"foreign", "chars":"yes"}),
            ),
        ] {
            let mut dispatcher = HookDispatcher::new();
            let seen = Arc::new(Mutex::new(Vec::new()));
            for result in [
                HookResult::Ask,
                HookResult::Deny {
                    reason: "must remain skipped".into(),
                },
            ] {
                dispatcher.register(Arc::new(RecordingHook {
                    point: AgentHookPoint::BeforeToolExecution,
                    matcher: regex::Regex::new(".*").unwrap(),
                    seen: seen.clone(),
                    result,
                }));
            }
            let report = dispatcher
                .run_hooks_with_resolved_shell(
                    AgentHookPoint::BeforeToolExecution,
                    &HookPayload::ToolExecution {
                        tool_name: name.into(),
                        tool_call_id: "ordinary-ask".into(),
                        parsed_args: args.clone(),
                    },
                    &Session::new("ordinary-ask", "model"),
                    Some(name),
                    Some(&args),
                )
                .await;
            assert_eq!(report.outcome.decision, HookResult::Ask, "{name}");
            assert_eq!(report.executions.len(), 1, "{name}");
            assert_eq!(seen.lock().unwrap().len(), 1);
        }
        // A combined post-tool payload keeps its prior Ask short circuit.
        for name in ["write_stdin", "default::write_stdin"] {
            let mut dispatcher = HookDispatcher::new();
            let seen = Arc::new(Mutex::new(Vec::new()));
            for result in [
                HookResult::Ask,
                HookResult::Deny {
                    reason: "post-tool skipped".into(),
                },
            ] {
                dispatcher.register(Arc::new(RecordingHook {
                    point: AgentHookPoint::AfterToolExecution,
                    matcher: regex::Regex::new(".*").unwrap(),
                    seen: seen.clone(),
                    result,
                }));
            }
            let report = dispatcher
                .run_hooks_with_resolved_shell(
                    AgentHookPoint::AfterToolExecution,
                    &HookPayload::ToolResult {
                        tool_name: name.into(),
                        tool_call_id: "post-ask".into(),
                        outcome: bamboo_domain::HookToolOutcome {
                            success: true,
                            result: Some("done".into()),
                            error: None,
                            needs_human: false,
                            duration_ms: 1,
                        },
                    },
                    &Session::new("post-ask", "model"),
                    Some("write_stdin"),
                    Some(&json!({"session_id":"exec", "chars":"yes"})),
                )
                .await;
            assert_eq!(report.outcome.decision, HookResult::Ask);
            assert_eq!(report.executions.len(), 1);
        }
    }
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    struct RecordingHook {
        point: AgentHookPoint,
        matcher: regex::Regex,
        seen: Arc<Mutex<Vec<HookPayload>>>,
        result: HookResult,
    }

    #[async_trait]
    impl AgentHook for RecordingHook {
        fn point(&self) -> AgentHookPoint {
            self.point
        }
        fn matches(&self, payload: &HookPayload) -> bool {
            match payload {
                HookPayload::ToolExecution { tool_name, .. }
                | HookPayload::ToolResult { tool_name, .. } => self.matcher.is_match(tool_name),
                _ => false,
            }
        }
        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            self.seen.lock().unwrap().push(payload.clone());
            self.result.clone()
        }
    }

    struct LegacyOperationPolicyHook {
        matcher: regex::Regex,
        seen: Arc<Mutex<Vec<HookPayload>>>,
        output_result: HookResult,
        primary_result: HookResult,
    }

    #[async_trait]
    impl AgentHook for LegacyOperationPolicyHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeToolExecution
        }

        fn matches(&self, payload: &HookPayload) -> bool {
            matches!(payload, HookPayload::ToolExecution { tool_name, .. }
                if self.matcher.is_match(tool_name))
        }

        async fn run(&self, _: AgentHookPoint, payload: &HookPayload, _: &Session) -> HookResult {
            self.seen.lock().unwrap().push(payload.clone());
            if matches!(payload, HookPayload::ToolExecution { tool_name, .. } if tool_name == "BashOutput")
            {
                self.output_result.clone()
            } else {
                self.primary_result.clone()
            }
        }
    }

    #[tokio::test]
    async fn legacy_only_overlap_output_deny_precedes_input_ask() {
        for args in [
            json!({"session_id":"exec", "chars":"yes"}),
            json!({"session_id":"exec", "eof":true}),
            json!({"session_id":"exec", "chars":char::from(3).to_string()}),
        ] {
            for output_result in [
                HookResult::Deny {
                    reason: "output remains denied".into(),
                },
                HookResult::Continue,
                HookResult::Allow,
            ] {
                let mut dispatcher = HookDispatcher::new();
                let seen = Arc::new(Mutex::new(Vec::new()));
                let expected = if matches!(output_result, HookResult::Deny { .. }) {
                    output_result.clone()
                } else {
                    HookResult::Ask
                };
                dispatcher.register(Arc::new(LegacyOperationPolicyHook {
                    matcher: regex::Regex::new("^(BashInput|KillShell|BashOutput)$").unwrap(),
                    seen: seen.clone(),
                    primary_result: HookResult::Ask,
                    output_result,
                }));
                let report = dispatcher
                    .run_hooks_with_resolved_shell(
                        AgentHookPoint::BeforeToolExecution,
                        &HookPayload::ToolExecution {
                            tool_name: "write_stdin".into(),
                            tool_call_id: "ask-before-output".into(),
                            parsed_args: args.clone(),
                        },
                        &Session::new("ask-before-output", "model"),
                        Some("write_stdin"),
                        Some(&args),
                    )
                    .await;
                assert_eq!(report.outcome.decision, expected);
                assert_eq!(report.executions.len(), 2);
                assert!(
                    matches!(&seen.lock().unwrap()[1], HookPayload::ToolExecution {tool_name,..} if tool_name == "BashOutput")
                );
            }
        }
    }

    #[tokio::test]
    async fn legacy_only_overlap_evaluates_output_policy_after_input_eof_or_kill() {
        for (input, primary_name) in [
            (json!({"session_id":"exec", "chars":"yes\n"}), "BashInput"),
            (json!({"session_id":"exec", "eof":true}), "BashInput"),
            (json!({"session_id":"exec", "chars":"\u{3}"}), "KillShell"),
        ] {
            for output_result in [
                HookResult::Deny {
                    reason: "legacy output policy".into(),
                },
                HookResult::Ask,
                HookResult::Allow,
            ] {
                let mut dispatcher = HookDispatcher::new();
                let seen = Arc::new(Mutex::new(Vec::new()));
                for matcher in [
                    "^default::write_stdin$",
                    "^write_stdin$",
                    ".*",
                    "^(write_stdin|BashInput|KillShell|BashOutput)$",
                ] {
                    dispatcher.register(Arc::new(RecordingHook {
                        point: AgentHookPoint::BeforeToolExecution,
                        matcher: regex::Regex::new(matcher).unwrap(),
                        seen: seen.clone(),
                        result: HookResult::Continue,
                    }));
                }
                dispatcher.register(Arc::new(LegacyOperationPolicyHook {
                    matcher: regex::Regex::new("^(BashInput|KillShell|BashOutput)$").unwrap(),
                    seen: seen.clone(),
                    output_result: output_result.clone(),
                    primary_result: HookResult::Continue,
                }));
                let payload = HookPayload::ToolExecution {
                    tool_name: "default::write_stdin".into(),
                    tool_call_id: "overlap-call".into(),
                    parsed_args: input.clone(),
                };
                let report = dispatcher
                    .run_hooks_with_resolved_shell(
                        AgentHookPoint::BeforeToolExecution,
                        &payload,
                        &Session::new("overlap", "model"),
                        Some("write_stdin"),
                        Some(&input),
                    )
                    .await;
                let expected = if matches!(output_result, HookResult::Allow) {
                    HookResult::Continue
                } else {
                    output_result
                };
                assert_eq!(report.outcome.decision, expected);
                assert_eq!(report.executions.len(), 6);
                let seen = seen.lock().unwrap();
                let names: Vec<_> = seen
                    .iter()
                    .map(|payload| match payload {
                        HookPayload::ToolExecution { tool_name, .. } => tool_name.as_str(),
                        _ => unreachable!(),
                    })
                    .collect();
                assert_eq!(
                    names,
                    [
                        "default::write_stdin",
                        "write_stdin",
                        "default::write_stdin",
                        "write_stdin",
                        primary_name,
                        "BashOutput"
                    ]
                );
                assert!(
                    matches!(&seen[5], HookPayload::ToolExecution { parsed_args, .. }
                    if parsed_args == &json!({"bash_id":"exec"}))
                );
            }
        }
    }

    #[tokio::test]
    async fn legacy_shell_policy_applies_without_duplicate_primary_or_wildcard_hooks() {
        for control in [
            HookResult::Deny {
                reason: "legacy policy".into(),
            },
            HookResult::Ask,
        ] {
            let mut dispatcher = HookDispatcher::new();
            let seen = Arc::new(Mutex::new(Vec::new()));
            for (matcher, result) in [
                ("^exec_command$", HookResult::Allow),
                (".*", HookResult::Continue),
                ("^(exec_command|Bash)$", HookResult::Continue),
                ("^Bash$", control.clone()),
            ] {
                dispatcher.register(Arc::new(RecordingHook {
                    point: AgentHookPoint::BeforeToolExecution,
                    matcher: regex::Regex::new(matcher).unwrap(),
                    seen: seen.clone(),
                    result,
                }));
            }
            let payload = HookPayload::ToolExecution {
                tool_name: "exec_command".into(),
                tool_call_id: "exec-call".into(),
                parsed_args: json!({"cmd":"cargo test"}),
            };
            let report = dispatcher
                .run_hooks(
                    AgentHookPoint::BeforeToolExecution,
                    &payload,
                    &Session::new("test", "model"),
                )
                .await;
            assert_eq!(report.outcome.decision, control);
            assert_eq!(report.executions.len(), 4);
            let seen = seen.lock().unwrap();
            assert_eq!(&seen[..3], &[payload.clone(), payload.clone(), payload]);
            assert!(
                matches!(&seen[3], HookPayload::ToolExecution { tool_name, parsed_args, .. }
                if tool_name == "Bash" && parsed_args["command"] == "cargo test" && parsed_args.get("cmd").is_none())
            );
        }
    }

    #[tokio::test]
    async fn resolved_shell_aliases_preserve_primary_spelling_and_run_fallbacks_once() {
        for (alias, resolved, legacy, input) in [
            (
                "default::exec_command",
                "exec_command",
                "Bash",
                json!({"cmd":"cargo test"}),
            ),
            (
                "default::write_stdin",
                "write_stdin",
                "BashInput",
                json!({"session_id":"exec", "chars":"yes\n"}),
            ),
        ] {
            for (point, payload) in [
                (
                    AgentHookPoint::BeforeToolExecution,
                    HookPayload::ToolExecution {
                        tool_name: alias.into(),
                        tool_call_id: "alias-call".into(),
                        parsed_args: input.clone(),
                    },
                ),
                (
                    AgentHookPoint::AfterToolExecution,
                    HookPayload::ToolResult {
                        tool_name: alias.into(),
                        tool_call_id: "alias-call".into(),
                        outcome: bamboo_domain::HookToolOutcome {
                            success: true,
                            result: Some("done".into()),
                            error: None,
                            needs_human: false,
                            duration_ms: 1,
                        },
                    },
                ),
            ] {
                let mut dispatcher = HookDispatcher::new();
                let seen = Arc::new(Mutex::new(Vec::new()));
                for matcher in [
                    format!("^{alias}$"),
                    ".*".into(),
                    format!("^({alias}|{resolved}|{legacy})$"),
                    format!("^{resolved}$"),
                    format!("^{legacy}$"),
                ] {
                    dispatcher.register(Arc::new(RecordingHook {
                        point,
                        matcher: regex::Regex::new(&matcher).unwrap(),
                        seen: seen.clone(),
                        result: HookResult::Continue,
                    }));
                }
                let report = dispatcher
                    .run_hooks_with_resolved_shell(
                        point,
                        &payload,
                        &Session::new("test", "model"),
                        Some(resolved),
                        Some(&input),
                    )
                    .await;
                assert_eq!(report.executions.len(), 5);
                let seen = seen.lock().unwrap();
                assert_eq!(&seen[..3], &[payload.clone(), payload.clone(), payload]);
                let name = |payload: &HookPayload| match payload {
                    HookPayload::ToolExecution { tool_name, .. }
                    | HookPayload::ToolResult { tool_name, .. } => tool_name.clone(),
                    _ => panic!("expected tool payload"),
                };
                assert_eq!(name(&seen[3]), resolved);
                assert_eq!(name(&seen[4]), legacy);
            }
        }
    }

    #[tokio::test]
    async fn combined_continuations_apply_output_restrictions_without_duplicate_primary_hooks() {
        for (input, primary) in [
            (
                json!({"session_id":"exec", "chars":"yes\n", "eof":false}),
                "BashInput",
            ),
            (json!({"session_id":"exec", "eof":true}), "BashInput"),
            (json!({"session_id":"exec", "chars":"\u{3}"}), "KillShell"),
        ] {
            for control in [
                HookResult::Deny {
                    reason: "output policy".into(),
                },
                HookResult::Ask,
            ] {
                for (point, payload) in [
                    (
                        AgentHookPoint::BeforeToolExecution,
                        HookPayload::ToolExecution {
                            tool_name: "default::write_stdin".into(),
                            tool_call_id: "combined-call".into(),
                            parsed_args: input.clone(),
                        },
                    ),
                    (
                        AgentHookPoint::AfterToolExecution,
                        HookPayload::ToolResult {
                            tool_name: "default::write_stdin".into(),
                            tool_call_id: "combined-call".into(),
                            outcome: bamboo_domain::HookToolOutcome {
                                success: true,
                                result: Some("output".into()),
                                error: None,
                                needs_human: false,
                                duration_ms: 1,
                            },
                        },
                    ),
                ] {
                    let mut dispatcher = HookDispatcher::new();
                    let seen = Arc::new(Mutex::new(Vec::new()));
                    for (matcher, result) in [
                        ("^default::write_stdin$", HookResult::Continue),
                        (".*", HookResult::Continue),
                        ("^(BashInput|KillShell|BashOutput)$", HookResult::Continue),
                        ("^write_stdin$", HookResult::Continue),
                        ("^BashOutput$", control.clone()),
                    ] {
                        dispatcher.register(Arc::new(RecordingHook {
                            point,
                            matcher: regex::Regex::new(matcher).unwrap(),
                            seen: seen.clone(),
                            result,
                        }));
                    }
                    let report = dispatcher
                        .run_hooks_with_resolved_shell(
                            point,
                            &payload,
                            &Session::new("test", "model"),
                            Some("write_stdin"),
                            Some(&input),
                        )
                        .await;
                    assert_eq!(report.outcome.decision, control);
                    assert_eq!(report.executions.len(), 6);
                    let seen = seen.lock().unwrap();
                    assert_eq!(&seen[..2], &[payload.clone(), payload]);
                    let name = |payload: &HookPayload| match payload {
                        HookPayload::ToolExecution { tool_name, .. }
                        | HookPayload::ToolResult { tool_name, .. } => tool_name.clone(),
                        _ => unreachable!(),
                    };
                    assert_eq!(name(&seen[2]), primary);
                    assert_eq!(name(&seen[3]), "BashOutput");
                    assert_eq!(name(&seen[4]), "write_stdin");
                    assert_eq!(name(&seen[5]), "BashOutput");
                    if let HookPayload::ToolExecution { parsed_args, .. } = &seen[5] {
                        assert_eq!(parsed_args, &json!({"bash_id":"exec"}));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn secondary_output_allow_preserves_context_without_granting_input_authority() {
        for (matcher, chars, expected) in [
            ("^BashOutput$", "yes\n", HookResult::Continue),
            ("^BashInput$", "yes\n", HookResult::Allow),
            ("^write_stdin$", "yes\n", HookResult::Allow),
            ("^BashOutput$", "", HookResult::Allow),
        ] {
            let mut dispatcher = HookDispatcher::new();
            let seen = Arc::new(Mutex::new(Vec::new()));
            dispatcher.register(Arc::new(RecordingHook {
                point: AgentHookPoint::BeforeToolExecution,
                matcher: regex::Regex::new(matcher).unwrap(),
                seen: seen.clone(),
                result: HookResult::WithContext {
                    result: Box::new(HookResult::Allow),
                    text: "policy context".into(),
                },
            }));
            let payload = HookPayload::ToolExecution {
                tool_name: "write_stdin".into(),
                tool_call_id: "allow-call".into(),
                parsed_args: json!({"session_id":"exec", "chars":chars}),
            };
            let report = dispatcher
                .run_hooks(
                    AgentHookPoint::BeforeToolExecution,
                    &payload,
                    &Session::new("test", "model"),
                )
                .await;
            assert_eq!(report.outcome.decision, expected);
            assert_eq!(report.outcome.injected_contexts, ["policy context"]);
            assert_eq!(seen.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn legacy_post_tool_hooks_resolve_stdin_operation_from_original_call() {
        use bamboo_agent_core::{FunctionCall, Message, ToolCall};
        for (input, legacy_name) in [
            (json!({"session_id":"exec", "chars":""}), "BashOutput"),
            (json!({"session_id":"exec", "chars":"yes\n"}), "BashInput"),
            (
                json!({"session_id":"exec", "chars":"", "eof":true}),
                "BashInput",
            ),
            (json!({"session_id":"exec", "chars":"\u{3}"}), "KillShell"),
        ] {
            let mut session = Session::new("test", "model");
            session.messages.push(Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: "stdin-call".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "write_stdin".into(),
                        arguments: input.to_string(),
                    },
                }]),
            ));
            let payload = HookPayload::ToolResult {
                tool_name: "write_stdin".into(),
                tool_call_id: "stdin-call".into(),
                outcome: bamboo_domain::HookToolOutcome {
                    success: true,
                    result: Some("done".into()),
                    error: None,
                    needs_human: false,
                    duration_ms: 1,
                },
            };
            let seen = Arc::new(Mutex::new(Vec::new()));
            let mut dispatcher = HookDispatcher::new();
            dispatcher.register(Arc::new(RecordingHook {
                point: AgentHookPoint::AfterToolExecution,
                matcher: regex::Regex::new(&format!("^{legacy_name}$")).unwrap(),
                seen: seen.clone(),
                result: HookResult::Ask,
            }));
            let report = dispatcher
                .run_hooks(AgentHookPoint::AfterToolExecution, &payload, &session)
                .await;
            assert_eq!(report.outcome.decision, HookResult::Ask);
            assert!(
                matches!(&seen.lock().unwrap()[0], HookPayload::ToolResult { tool_name, .. } if tool_name == legacy_name)
            );
        }
    }
}

#[cfg(test)]
mod non_suspending_tests;
