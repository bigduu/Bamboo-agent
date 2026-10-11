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
#[derive(Clone, Default)]
pub struct HookDispatcher {
    hooks: Vec<Arc<dyn AgentHook>>,
}

impl HookDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a hook. Lower numeric priorities run first.
    pub fn register(&mut self, hook: Arc<dyn AgentHook>) {
        self.hooks.push(hook);
        self.hooks.sort_by_key(|hook| hook.priority());
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
        let legacy = crate::shell_compatibility::legacy_shell_projection(
            resolved_shell.as_ref().unwrap_or(payload),
            session,
            original_tool_input,
        );

        for hook in &self.hooks {
            if hook.point() != point {
                continue;
            }
            // Original-name and wildcard matchers keep the actual spelling.
            // Each handler runs once, using shell compatibility only when its
            // primary matcher did not match the original call.
            let selected_payload = if hook.matches(payload) {
                payload
            } else if let Some(resolved) = resolved_shell
                .as_ref()
                .filter(|resolved| hook.matches(resolved))
            {
                resolved
            } else if let Some(projected) = legacy
                .as_ref()
                .filter(|legacy| hook.matches(&legacy.payload))
            {
                &projected.payload
            } else {
                continue;
            };

            let started = Instant::now();
            let result = hook.run(point, selected_payload, session).await;
            report.executions.push(HookExecution {
                hook_name: hook.name().to_string(),
                point,
                duration_ms: started.elapsed().as_millis() as u64,
                result: result.clone(),
            });

            let (result, mut contexts) = unwrap_context_result(result);
            report.outcome.injected_contexts.append(&mut contexts);

            match &result {
                HookResult::Abort { .. }
                | HookResult::Suspend { .. }
                | HookResult::Deny { .. }
                | HookResult::Ask => {
                    if honor_control_decisions {
                        report.outcome.decision = result;
                        return report;
                    }
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

        report
    }

    pub fn has_hooks_for(&self, point: AgentHookPoint) -> bool {
        self.hooks.iter().any(|hook| hook.point() == point)
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

#[cfg(test)]
mod tests {
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
