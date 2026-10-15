//! Hook runner — dispatches registered hooks at lifecycle points.

mod local_resources;

use std::sync::Arc;

use bamboo_agent_core::{AgentError, AgentEvent, AgentHook, Message, Session};
use bamboo_domain::{
    AgentHookPoint, AgentRuntimeState, AgentStatusState, HookCheckpoint, HookPayload, HookResult,
    SessionEndStatus, SuspensionState,
};
use chrono::Utc;
use tokio::sync::mpsc;

pub use bamboo_hooks::{
    test_lifecycle_handler, test_lifecycle_shell_command, HookRunOutcome, LifecycleHookEvent,
    LifecycleHookTestOutput, LifecycleScriptRunner, ScriptHook, ShellCommandHook, ShellHookEvent,
    ShellHookTestOutput,
};

/// Engine adapter around the standalone hook dispatcher.
///
/// `bamboo-hooks` owns matching and handler execution. This adapter translates
/// completed executions into engine checkpoints and lifecycle events.
#[derive(Clone)]
pub struct HookRunner {
    dispatcher: bamboo_hooks::HookDispatcher,
    plugin_root: Option<std::path::PathBuf>,
}

impl HookRunner {
    pub fn new() -> Self {
        Self {
            dispatcher: bamboo_hooks::HookDispatcher::new(),
            plugin_root: None,
        }
    }

    pub fn register(&mut self, hook: Arc<dyn AgentHook>) {
        self.dispatcher.register(hook);
    }

    /// Clone this registry and append configured handlers from one frozen config
    /// snapshot. The original registry remains reusable by future runs.
    pub fn with_lifecycle_config(
        &self,
        config: &bamboo_config::LifecycleHooksConfig,
        fallback_cwd: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            plugin_root: fallback_cwd.as_ref().map(|root| root.join("plugins")),
            dispatcher: self.dispatcher.with_lifecycle_config(config, fallback_cwd),
        }
    }

    /// Record the exact accepted prompt from the server submission seam. The
    /// shared loop consumes this once, so server and embedded runs cannot fire
    /// portable UserPromptSubmit twice for the same admitted prompt.
    pub fn mark_user_prompt_prechecked(session: &mut Session, prompt: &str) {
        session.metadata.insert(
            "runtime.plugin_prompt_prechecked".into(),
            prompt_fingerprint(prompt),
        );
    }

    /// Shared CLI/SDK fallback, before session preparation or provider calls.
    /// Native submission hooks keep their existing server-only invocation.
    pub(crate) async fn apply_portable_user_prompt(
        &self,
        session: &mut Session,
        prompt: &str,
    ) -> Result<String, AgentError> {
        let prechecked = session.metadata.remove("runtime.plugin_prompt_prechecked");
        if prechecked.as_deref() == Some(prompt_fingerprint(prompt).as_str()) {
            return Ok(prompt.to_owned());
        }
        let Some(root) = &self.plugin_root else {
            return Ok(prompt.to_owned());
        };
        let report = local_resources::run(bamboo_hooks::portable::run(
            root,
            AgentHookPoint::BeforeSessionSetup,
            &HookPayload::Prompt {
                prompt: prompt.to_owned(),
            },
            session,
        ))
        .await;
        for error in &report.errors {
            tracing::warn!(%error, "plugin prompt hook compatibility failure");
        }
        match report.decision {
            HookResult::Deny { reason } | HookResult::Abort { reason } => {
                return Err(AgentError::Tool(format!(
                    "UserPromptSubmit hook rejected prompt: {reason}"
                )));
            }
            _ => {}
        }
        let contexts = report
            .contexts
            .into_iter()
            .map(|context| context.rendered_text())
            .collect::<Vec<_>>();
        if contexts.is_empty() {
            Ok(prompt.to_owned())
        } else {
            Ok(format!(
                "{prompt}\n\n<user_prompt_submit_context>\n{}\n</user_prompt_submit_context>",
                contexts.join("\n\n---\n\n")
            ))
        }
    }

    /// Run all hooks matching the given point.
    ///
    /// Records checkpoints in `runtime_state`. Returns the first
    /// `Suspend` or `Abort` result, or the aggregate result otherwise.
    pub async fn run_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: Option<&mpsc::Sender<AgentEvent>>,
    ) -> HookRunOutcome {
        self.run_hooks_with_tool_input(point, payload, session, runtime_state, event_tx, None)
            .await
    }

    pub async fn run_hooks_with_tool_input(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: Option<&mpsc::Sender<AgentEvent>>,
        original_tool_input: Option<&serde_json::Value>,
    ) -> HookRunOutcome {
        self.run_hooks_with_inputs(
            point,
            payload,
            session,
            runtime_state,
            event_tx,
            bamboo_hooks::portable::PortableInputs {
                original_tool_input,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn run_hooks_with_inputs(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: Option<&mpsc::Sender<AgentEvent>>,
        inputs: bamboo_hooks::portable::PortableInputs<'_>,
    ) -> HookRunOutcome {
        let report = local_resources::run(self.dispatcher.run_hooks_with_resolved_shell(
            point,
            payload,
            session,
            inputs.resolved_tool_name,
            inputs.original_tool_input,
        ))
        .await;
        let mut outcome = record_dispatch_report(report, runtime_state, event_tx).await;
        if let Some(root) = &self.plugin_root {
            let mut portable_payload = payload.clone();
            if let Some(name) = inputs.resolved_tool_name {
                match &mut portable_payload {
                    HookPayload::ToolExecution { tool_name, .. }
                    | HookPayload::ToolResult { tool_name, .. } => *tool_name = name.to_owned(),
                    _ => {}
                }
            }
            let pending_composite_ask = matches!(outcome.decision, HookResult::Ask)
                && bamboo_hooks::requires_complete_shell_policy_checks(
                    point,
                    &portable_payload,
                    session,
                    inputs.original_tool_input,
                );
            if matches!(
                outcome.decision,
                HookResult::Continue | HookResult::Allow | HookResult::Mutated
            ) || pending_composite_ask
            {
                let plugin = local_resources::run(bamboo_hooks::portable::run_with_inputs(
                    root,
                    point,
                    &portable_payload,
                    session,
                    inputs.original_tool_input,
                    inputs.final_assistant_content,
                ))
                .await;
                for error in plugin.errors {
                    tracing::warn!(%error, "plugin hook compatibility failure");
                    runtime_state.checkpoints.push(HookCheckpoint {
                        hook_point: format!("plugin:{point:?}"),
                        timestamp: Utc::now(),
                        result: error.chars().take(1024).collect(),
                        duration_ms: 0,
                    });
                }
                if !matches!(plugin.decision, HookResult::Continue)
                    && (!pending_composite_ask
                        || matches!(
                            plugin.decision,
                            HookResult::Deny { .. }
                                | HookResult::Abort { .. }
                                | HookResult::Suspend { .. }
                        ))
                {
                    outcome.decision = plugin.decision;
                }
                outcome.plugin_contexts = plugin.contexts;
            }
        }
        outcome
    }

    /// Run every matching hook while recording checkpoints/events, but never
    /// short-circuit on a control decision. Observer/advisory seams such as
    /// `SessionEnd`, `PreCompact`, and `Notification` use this so a command's
    /// control-shaped output cannot suppress later hooks or reverse an
    /// operation that must proceed for correctness.
    pub async fn run_observer_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: Option<&mpsc::Sender<AgentEvent>>,
    ) -> HookRunOutcome {
        let report =
            local_resources::run(self.dispatcher.run_observer_hooks(point, payload, session)).await;
        record_dispatch_report(report, runtime_state, event_tx).await
    }

    pub fn has_hooks_for(&self, point: AgentHookPoint) -> bool {
        self.dispatcher.has_hooks_for(point)
            || (self
                .plugin_root
                .as_ref()
                .is_some_and(|root| bamboo_hooks::portable::has_active_hooks_for(root, point)))
    }

    /// Future control seams in this runner, after the model can issue Start.
    /// Setup is already complete and observer seams never honor suspension.
    pub(crate) fn has_suspending_loop_control_hooks(&self) -> bool {
        [
            AgentHookPoint::BeforeRound,
            AgentHookPoint::AfterRound,
            AgentHookPoint::BeforeToolExecution,
            AgentHookPoint::AfterToolExecution,
            AgentHookPoint::BeforeFinalize,
        ]
        .into_iter()
        .any(|point| self.dispatcher.has_suspending_hooks_for(point))
    }

    pub fn len(&self) -> usize {
        self.dispatcher.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dispatcher.is_empty()
            && !self.plugin_root.as_ref().is_some_and(|root| {
                [
                    AgentHookPoint::BeforeSessionSetup,
                    AgentHookPoint::BeforeToolExecution,
                    AgentHookPoint::AfterToolExecution,
                    AgentHookPoint::BeforeFinalize,
                ]
                .into_iter()
                .any(|point| bamboo_hooks::portable::has_active_hooks_for(root, point))
            })
    }
}

fn prompt_fingerprint(prompt: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(prompt.as_bytes()))
}

async fn record_dispatch_report(
    report: bamboo_hooks::HookDispatchReport,
    runtime_state: &mut AgentRuntimeState,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
) -> HookRunOutcome {
    let bamboo_hooks::HookDispatchReport {
        outcome,
        executions,
    } = report;
    for execution in executions {
        runtime_state.checkpoints.push(HookCheckpoint {
            hook_point: format!("{:?}", execution.point),
            timestamp: Utc::now(),
            result: format!("{:?}", execution.result),
            duration_ms: execution.duration_ms,
        });
        if let Some(event_tx) = event_tx {
            let _ = event_tx
                .send(AgentEvent::HookLifecycle {
                    hook_name: execution.hook_name,
                    point: execution.point,
                    phase: "completed".to_string(),
                    duration_ms: execution.duration_ms,
                    decision: execution.result,
                })
                .await;
        }
    }
    outcome
}

/// Fire cleanup/notification hooks after a terminal run. Decisions and context
/// are intentionally ignored: `SessionEnd` observes a settled outcome and may
/// not reverse it. Suspended runs are non-terminal and do not fire this event.
pub(crate) async fn run_session_end_hooks(
    runner: &HookRunner,
    result: &Result<(), AgentError>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
) {
    let suspended_non_terminal = result.is_ok()
        && session
            .metadata
            .get("runtime.suspend_reason")
            .is_some_and(|reason| !reason.trim().is_empty());
    if suspended_non_terminal || !runner.has_hooks_for(AgentHookPoint::AfterSessionEnd) {
        return;
    }

    let (status, completion_reason) = match result {
        Ok(()) => (
            SessionEndStatus::Completed,
            session
                .metadata
                .get("runtime.completion_reason")
                .cloned()
                .or_else(|| Some("completed".to_string())),
        ),
        Err(error) if error.is_cancelled() => {
            (SessionEndStatus::Cancelled, Some(error.to_string()))
        }
        Err(error) => (SessionEndStatus::Failed, Some(error.to_string())),
    };
    let mut runtime_state = session
        .agent_runtime_state
        .clone()
        .unwrap_or_else(|| AgentRuntimeState::new(&session.id));
    runner
        .run_observer_hooks(
            AgentHookPoint::AfterSessionEnd,
            &HookPayload::SessionEnd {
                status,
                completion_reason,
            },
            session,
            &mut runtime_state,
            Some(event_tx),
        )
        .await;
    session.agent_runtime_state = Some(runtime_state);
}

/// Apply context injections and non-tool control decisions consistently across
/// lifecycle seams.
pub(crate) fn apply_hook_outcome(
    point: AgentHookPoint,
    outcome: HookRunOutcome,
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
) -> Result<(), AgentError> {
    inject_plugin_contexts(session, outcome.plugin_contexts);
    if matches!(point, AgentHookPoint::AfterSessionSetup) {
        runtime_state.hook_contexts.extend(
            outcome
                .injected_contexts
                .into_iter()
                .filter(|text| !text.trim().is_empty()),
        );
    } else {
        inject_contexts(session, point, outcome.injected_contexts);
    }

    match outcome.decision {
        HookResult::Continue
        | HookResult::Mutated
        | HookResult::Allow
        | HookResult::InjectContext { .. } => Ok(()),
        HookResult::Suspend { reason } => {
            let hook_point = format!("{point:?}");
            runtime_state.status = AgentStatusState::Suspended;
            runtime_state.suspension = Some(SuspensionState {
                reason: reason.clone(),
                suspended_at: Utc::now(),
                resumable: true,
                hook_point: Some(hook_point.clone()),
            });
            session.metadata.insert(
                "runtime.suspend_reason".to_string(),
                "hook_suspended".to_string(),
            );
            Err(AgentError::HookSuspended(format!("{hook_point}: {reason}")))
        }
        HookResult::Abort { reason } => Err(AgentError::Tool(format!(
            "hook aborted at {point:?}: {reason}"
        ))),
        HookResult::Deny { reason } => Err(AgentError::Tool(format!(
            "hook denied lifecycle seam {point:?}: {reason}"
        ))),
        HookResult::Ask => Err(AgentError::Tool(format!(
            "hook requested parent approval at non-tool seam {point:?}"
        ))),
        HookResult::WithContext { result, text } => apply_hook_outcome(
            point,
            HookRunOutcome {
                decision: *result,
                injected_contexts: vec![text],
                plugin_contexts: vec![],
            },
            session,
            runtime_state,
        ),
    }
}

/// Stage bounded untrusted snapshots for the existing chronological context
/// ledger. Do not insert messages into an unfinished assistant/tool group.
pub(crate) fn inject_plugin_contexts(
    session: &mut Session,
    contexts: Vec<bamboo_hooks::portable::PluginContext>,
) {
    if contexts.is_empty() {
        return;
    }
    let key = "runtime.plugin_hook_contexts";
    let mut chunks: Vec<String> = session
        .metadata
        .get(key)
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default();
    chunks.extend(contexts.into_iter().map(|context| context.rendered_text()));
    while chunks.iter().map(String::len).sum::<usize>() + chunks.len().saturating_sub(1) * 2
        > bamboo_hooks::portable::PORTABLE_CONTEXT_BYTES
    {
        if chunks.is_empty() {
            break;
        }
        chunks.remove(0);
    }
    session.metadata.insert(
        key.into(),
        serde_json::to_string(&chunks).expect("string serialization"),
    );
}

pub(crate) fn inject_contexts(
    session: &mut Session,
    point: AgentHookPoint,
    injected_contexts: Vec<String>,
) {
    for text in injected_contexts {
        if text.trim().is_empty() {
            continue;
        }
        let block =
            format!("\n\n<agent_hook_context point=\"{point:?}\">\n{text}\n</agent_hook_context>");
        if let Some(system_message) = session
            .messages
            .iter_mut()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        {
            system_message.content.push_str(&block);
            system_message.never_compress = true;
        } else {
            let mut message = Message::system(block.trim().to_string());
            message.never_compress = true;
            message.metadata = Some(serde_json::json!({
                "runtime_kind": "hook_context",
                "hook_point": point,
            }));
            session.add_message(message);
        }
    }
}

/// Merge hook checkpoints produced through a session-local seam (notably
/// compression) into the runner-owned state without losing checkpoints written
/// directly by tool/round seams.
pub(crate) fn merge_session_hook_checkpoints(
    session: &Session,
    runtime_state: &mut AgentRuntimeState,
) {
    let Some(session_state) = session.agent_runtime_state.as_ref() else {
        return;
    };
    for checkpoint in &session_state.checkpoints {
        if !runtime_state.checkpoints.contains(checkpoint) {
            runtime_state.checkpoints.push(checkpoint.clone());
        }
    }
    if matches!(session_state.status, AgentStatusState::Suspended) {
        runtime_state.status = AgentStatusState::Suspended;
        runtime_state.suspension = session_state.suspension.clone();
    }
}

impl Default for HookRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    #[cfg(unix)]
    #[tokio::test]
    async fn combined_shell_ask_checks_trusted_portable_output_policy() {
        use bamboo_plugin::{
            InstalledPlugin, InstalledPlugins, PluginInstallStatus, PluginManifest, PluginSource,
            RegisteredCapabilities,
        };
        use serde_json::json;
        for decision in ["deny", "continue", "allow"] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("plugins");
            let bundle = root.join("combined-ask");
            std::fs::create_dir_all(&bundle).unwrap();
            let manifest: PluginManifest = serde_json::from_value(json!({
                "id":"combined-ask", "name":"Combined Ask", "version":"0.1.0",
                "provides":{"hooks":[{"config":"hooks.json", "scripts":["policy.sh"]}]}
            }))
            .unwrap();
            std::fs::write(
                bundle.join("plugin.json"),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            std::fs::write(bundle.join("policy.sh"), "# reviewed fixture").unwrap();
            let response = if decision == "continue" {
                String::new()
            } else {
                json!({"hookSpecificOutput":{"hookEventName":"PreToolUse", "permissionDecision":decision, "permissionDecisionReason":"portable output denied"}}).to_string()
            };
            let command = format!("touch \"$PLUGIN_DATA/seen\"; printf '%s' '{}'", response);
            std::fs::write(bundle.join("hooks.json"), serde_json::to_vec(&json!({"hooks":{"PreToolUse":[{
                "matcher":"^BashOutput$", "hooks":[{"type":"command", "command":command, "timeout":1}]
            }]}})).unwrap()).unwrap();
            let mut hooks = bamboo_plugin::hooks::registrations(&manifest, &bundle).unwrap();
            let digest = hooks[0].digest.clone();
            hooks[0].confirm_review(&digest).unwrap();
            InstalledPlugins {
                plugins: vec![InstalledPlugin {
                    id: manifest.id,
                    version: manifest.version,
                    source: PluginSource::LocalDir {
                        path: bundle.clone(),
                    },
                    plugin_dir: bundle,
                    installed_at: Utc::now(),
                    status: PluginInstallStatus::Installed,
                    registered: RegisteredCapabilities {
                        hooks,
                        ..Default::default()
                    },
                }],
            }
            .save(&root.join("installed.json"))
            .await
            .unwrap();
            let marker = root.join(".hook-data/combined-ask/seen");
            for name in ["write_stdin", "default::write_stdin"] {
                for (primary, args) in [
                    ("BashInput", json!({"session_id":"exec", "chars":"yes"})),
                    ("BashInput", json!({"session_id":"exec", "eof":true})),
                    ("KillShell", json!({"session_id":"exec", "chars":"\u{3}"})),
                ] {
                    let _ = std::fs::remove_file(&marker);
                    let mut runner = HookRunner::new().with_lifecycle_config(
                        &bamboo_config::LifecycleHooksConfig::default(),
                        Some(temp.path().to_owned()),
                    );
                    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
                    runner.register(Arc::new(ToolNameHook {
                        point: AgentHookPoint::BeforeToolExecution,
                        name: primary.into(),
                        seen: seen.clone(),
                        result: HookResult::Ask,
                    }));
                    let outcome = runner
                        .run_hooks_with_inputs(
                            AgentHookPoint::BeforeToolExecution,
                            &HookPayload::ToolExecution {
                                tool_name: name.into(),
                                tool_call_id: "combined-ask".into(),
                                parsed_args: args.clone(),
                            },
                            &test_session(),
                            &mut AgentRuntimeState::new("combined-ask"),
                            None,
                            bamboo_hooks::portable::PortableInputs {
                                resolved_tool_name: Some("write_stdin"),
                                original_tool_input: Some(&args),
                                ..Default::default()
                            },
                        )
                        .await;
                    let expected = if decision == "deny" {
                        HookResult::Deny {
                            reason: "portable output denied".into(),
                        }
                    } else {
                        HookResult::Ask
                    };
                    assert_eq!(outcome.decision, expected, "{decision}, {name}: {args}");
                    assert_eq!(seen.lock().unwrap().len(), 1);
                    assert!(
                        marker.exists(),
                        "trusted portable output handler must run after native Ask"
                    );
                }
            }
            // Legacy and pure-poll calls retain the native Ask short circuit.
            for (name, args, resolved) in [
                ("BashOutput", json!({"bash_id":"legacy"}), None),
                (
                    "write_stdin",
                    json!({"session_id":"exec", "chars":""}),
                    Some("write_stdin"),
                ),
                (
                    "default::write_stdin",
                    json!({"session_id":"exec", "eof":false}),
                    Some("write_stdin"),
                ),
            ] {
                let _ = std::fs::remove_file(&marker);
                let mut runner = HookRunner::new().with_lifecycle_config(
                    &bamboo_config::LifecycleHooksConfig::default(),
                    Some(temp.path().to_owned()),
                );
                runner.register(Arc::new(ToolNameHook {
                    point: AgentHookPoint::BeforeToolExecution,
                    name: "BashOutput".into(),
                    seen: Arc::new(std::sync::Mutex::new(Vec::new())),
                    result: HookResult::Ask,
                }));
                let outcome = runner
                    .run_hooks_with_inputs(
                        AgentHookPoint::BeforeToolExecution,
                        &HookPayload::ToolExecution {
                            tool_name: name.into(),
                            tool_call_id: "legacy-ask".into(),
                            parsed_args: args.clone(),
                        },
                        &test_session(),
                        &mut AgentRuntimeState::new("legacy-ask"),
                        None,
                        bamboo_hooks::portable::PortableInputs {
                            resolved_tool_name: resolved,
                            original_tool_input: Some(&args),
                            ..Default::default()
                        },
                    )
                    .await;
                assert_eq!(outcome.decision, HookResult::Ask);
                assert!(
                    !marker.exists(),
                    "ordinary legacy Ask must skip portable execution"
                );
            }
        }
    }
    use super::*;

    /// A no-op hook that always returns Continue.
    struct ContinueHook {
        point: AgentHookPoint,
        pri: u32,
        name: String,
    }

    #[async_trait::async_trait]
    impl AgentHook for ContinueHook {
        fn point(&self) -> AgentHookPoint {
            self.point
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            HookResult::Continue
        }

        fn priority(&self) -> u32 {
            self.pri
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    /// A hook that always returns Abort.
    struct AbortHook;

    #[async_trait::async_trait]
    impl AgentHook for AbortHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeLlmCall
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            HookResult::Abort {
                reason: "test abort".to_string(),
            }
        }

        fn name(&self) -> &str {
            "abort_hook"
        }
    }

    fn test_session() -> Session {
        Session::new("test", "test-model")
    }

    struct ToolNameHook {
        point: AgentHookPoint,
        name: String,
        seen: Arc<std::sync::Mutex<Vec<HookPayload>>>,
        result: HookResult,
    }

    #[async_trait::async_trait]
    impl AgentHook for ToolNameHook {
        fn point(&self) -> AgentHookPoint {
            self.point
        }

        fn matches(&self, payload: &HookPayload) -> bool {
            match payload {
                HookPayload::ToolExecution { tool_name, .. }
                | HookPayload::ToolResult { tool_name, .. } => tool_name == &self.name,
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
    async fn resolved_builtin_aliases_keep_native_legacy_shell_policy() {
        use bamboo_agent_core::{FunctionCall, ToolCall};
        use serde_json::json;

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
            let mut session = test_session();
            session.messages.push(Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: "alias-call".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: alias.into(),
                        arguments: input.to_string(),
                    },
                }]),
            ));
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
                let mut runner = HookRunner::new();
                let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
                let deny = HookResult::Deny {
                    reason: "legacy shell policy".into(),
                };
                runner.register(Arc::new(ToolNameHook {
                    point,
                    name: legacy.into(),
                    seen: seen.clone(),
                    result: deny.clone(),
                }));
                let outcome = runner
                    .run_hooks_with_inputs(
                        point,
                        &payload,
                        &session,
                        &mut AgentRuntimeState::new("alias-run"),
                        None,
                        bamboo_hooks::portable::PortableInputs {
                            resolved_tool_name: Some(resolved),
                            original_tool_input: Some(&input),
                            ..Default::default()
                        },
                    )
                    .await;
                assert_eq!(outcome.decision, deny);
                let seen = seen.lock().unwrap();
                assert_eq!(seen.len(), 1);
                match &seen[0] {
                    HookPayload::ToolExecution {
                        tool_name,
                        parsed_args,
                        ..
                    } => {
                        assert_eq!(tool_name, legacy);
                        if resolved == "exec_command" {
                            assert_eq!(parsed_args, &json!({"command":"cargo test"}));
                        } else {
                            assert_eq!(parsed_args["bash_id"], "exec");
                            assert_eq!(parsed_args["input"], "yes\n");
                        }
                    }
                    HookPayload::ToolResult { tool_name, .. } => assert_eq!(tool_name, legacy),
                    _ => panic!("expected tool hook payload"),
                }
            }
        }
    }

    #[tokio::test]
    async fn resolved_non_shell_aliases_preserve_native_matchers_and_payloads() {
        for alias in ["read_file", "default::Read"] {
            for (point, payload) in [
                (
                    AgentHookPoint::BeforeToolExecution,
                    HookPayload::ToolExecution {
                        tool_name: alias.into(),
                        tool_call_id: "read-alias".into(),
                        parsed_args: serde_json::json!({"path":"input.txt"}),
                    },
                ),
                (
                    AgentHookPoint::AfterToolExecution,
                    HookPayload::ToolResult {
                        tool_name: alias.into(),
                        tool_call_id: "read-alias".into(),
                        outcome: bamboo_domain::HookToolOutcome {
                            success: true,
                            result: Some("file contents".into()),
                            error: None,
                            needs_human: false,
                            duration_ms: 1,
                        },
                    },
                ),
            ] {
                let mut runner = HookRunner::new();
                let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
                runner.register(Arc::new(ToolNameHook {
                    point,
                    name: "Read".into(),
                    seen: seen.clone(),
                    result: HookResult::Deny {
                        reason: "must not newly match the resolved name".into(),
                    },
                }));
                let expected = HookResult::Deny {
                    reason: "original alias policy".into(),
                };
                runner.register(Arc::new(ToolNameHook {
                    point,
                    name: alias.into(),
                    seen: seen.clone(),
                    result: expected.clone(),
                }));
                let outcome = runner
                    .run_hooks_with_inputs(
                        point,
                        &payload,
                        &test_session(),
                        &mut AgentRuntimeState::new("read-alias-run"),
                        None,
                        bamboo_hooks::portable::PortableInputs {
                            resolved_tool_name: Some("Read"),
                            ..Default::default()
                        },
                    )
                    .await;
                assert_eq!(outcome.decision, expected);
                assert_eq!(*seen.lock().unwrap(), vec![payload]);
            }
        }
    }

    #[tokio::test]
    async fn foreign_and_unresolved_tool_names_do_not_inherit_native_shell_policy() {
        for (name, resolved) in [
            ("vendor::exec_command", Some("vendor::exec_command")),
            ("vendor::write_stdin", Some("vendor::write_stdin")),
            ("default::exec_command", None),
            ("default::write_stdin", None),
        ] {
            let point = AgentHookPoint::BeforeToolExecution;
            let mut runner = HookRunner::new();
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            for legacy in ["Bash", "BashInput"] {
                runner.register(Arc::new(ToolNameHook {
                    point,
                    name: legacy.into(),
                    seen: seen.clone(),
                    result: HookResult::Deny {
                        reason: "must not govern a foreign tool".into(),
                    },
                }));
            }
            runner.register(Arc::new(ToolNameHook {
                point,
                name: name.into(),
                seen: seen.clone(),
                result: HookResult::Continue,
            }));
            let payload = HookPayload::ToolExecution {
                tool_name: name.into(),
                tool_call_id: "foreign-call".into(),
                parsed_args: serde_json::json!({"cmd":"cargo test", "session_id":"exec", "chars":"yes"}),
            };
            let outcome = runner
                .run_hooks_with_inputs(
                    point,
                    &payload,
                    &test_session(),
                    &mut AgentRuntimeState::new("foreign-run"),
                    None,
                    bamboo_hooks::portable::PortableInputs {
                        resolved_tool_name: resolved,
                        ..Default::default()
                    },
                )
                .await;
            assert_eq!(outcome.decision, HookResult::Continue);
            assert_eq!(*seen.lock().unwrap(), vec![payload]);
        }
    }

    #[tokio::test]
    async fn empty_runner_returns_continue() {
        let runner = HookRunner::new();
        let mut state = AgentRuntimeState::new("run-1");
        let session = test_session();
        let (tx, _rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::BeforeRound,
                &HookPayload::Round { round: 1 },
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert_eq!(result.decision, HookResult::Continue);
        assert!(state.checkpoints.is_empty());
    }

    #[tokio::test]
    async fn hooks_run_in_priority_order() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(ContinueHook {
            point: AgentHookPoint::BeforeRound,
            pri: 200,
            name: "slow".to_string(),
        }));
        runner.register(Arc::new(ContinueHook {
            point: AgentHookPoint::BeforeRound,
            pri: 50,
            name: "fast".to_string(),
        }));

        let mut state = AgentRuntimeState::new("run-2");
        let session = test_session();
        let (tx, mut rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::BeforeRound,
                &HookPayload::Round { round: 1 },
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert_eq!(result.decision, HookResult::Continue);
        assert_eq!(state.checkpoints.len(), 2);
        // Lower priority runs first
        assert!(state.checkpoints[0].result.contains("Continue"));
        assert!(matches!(
            rx.recv().await,
            Some(AgentEvent::HookLifecycle { hook_name, .. }) if hook_name == "fast"
        ));
    }

    #[tokio::test]
    async fn abort_short_circuits() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(AbortHook));

        let mut state = AgentRuntimeState::new("run-3");
        let session = test_session();
        let (tx, _rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::BeforeLlmCall,
                &HookPayload::None,
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert!(matches!(result.decision, HookResult::Abort { .. }));
        assert_eq!(state.checkpoints.len(), 1);
    }

    #[tokio::test]
    async fn wrong_point_hooks_are_skipped() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(AbortHook)); // registered for BeforeLlmCall

        let mut state = AgentRuntimeState::new("run-4");
        let session = test_session();
        let (tx, _rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::AfterRound,
                &HookPayload::Round { round: 1 },
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert_eq!(result.decision, HookResult::Continue);
        assert!(state.checkpoints.is_empty());
    }

    struct RecordingSessionEndHook {
        payloads: Arc<std::sync::Mutex<Vec<HookPayload>>>,
    }

    #[async_trait::async_trait]
    impl AgentHook for RecordingSessionEndHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterSessionEnd
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            self.payloads.lock().unwrap().push(payload.clone());
            // Decisions at SessionEnd are observability-only and must not
            // change the already-settled terminal outcome.
            HookResult::Deny {
                reason: "ignored cleanup decision".to_string(),
            }
        }
    }

    #[tokio::test]
    async fn session_end_fires_for_completed_failed_and_cancelled_and_ignores_decisions() {
        for (result, expected_status) in [
            (Ok(()), SessionEndStatus::Completed),
            (
                Err(AgentError::Tool("terminal failure".to_string())),
                SessionEndStatus::Failed,
            ),
            (Err(AgentError::Cancelled), SessionEndStatus::Cancelled),
        ] {
            let payloads = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut runner = HookRunner::new();
            runner.register(Arc::new(RecordingSessionEndHook {
                payloads: payloads.clone(),
            }));
            runner.register(Arc::new(RecordingSessionEndHook {
                payloads: payloads.clone(),
            }));
            let mut session = test_session();
            let (tx, _rx) = mpsc::channel(4);

            run_session_end_hooks(&runner, &result, &mut session, &tx).await;

            let recorded = payloads.lock().unwrap();
            assert_eq!(
                recorded.len(),
                2,
                "a denied observer must not suppress later cleanup hooks"
            );
            assert!(recorded.iter().all(|payload| matches!(
                payload,
                HookPayload::SessionEnd { status, .. } if *status == expected_status
            )));
            assert_eq!(
                session
                    .agent_runtime_state
                    .as_ref()
                    .map(|state| state.checkpoints.len()),
                Some(2)
            );
        }
    }

    #[tokio::test]
    async fn session_end_skips_suspended_non_terminal_runs() {
        let payloads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut runner = HookRunner::new();
        runner.register(Arc::new(RecordingSessionEndHook {
            payloads: payloads.clone(),
        }));
        let mut session = test_session();
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
        let (tx, _rx) = mpsc::channel(4);

        run_session_end_hooks(&runner, &Ok(()), &mut session, &tx).await;

        assert!(payloads.lock().unwrap().is_empty());
        assert!(session.agent_runtime_state.is_none());
    }
}

#[cfg(test)]
mod portable_context_tests {
    use super::*;
    #[test]
    fn retained_plugin_snapshot_is_bounded_without_damaging_native_context() {
        let mut session = Session::new("fixture", "model");
        let mut state = AgentRuntimeState::new("run");
        state.hook_contexts = vec!["native context".into()];
        session.agent_runtime_state = Some(state);
        for _ in 0..100 {
            inject_plugin_contexts(
                &mut session,
                vec![bamboo_hooks::portable::PluginContext {
                    source: "fixture@1.0.0:hooks.json:PreToolUse".into(),
                    text: "x".repeat(200),
                }],
            );
        }
        let chunks: Vec<String> =
            serde_json::from_str(&session.metadata["runtime.plugin_hook_contexts"]).unwrap();
        assert!(chunks.join("\n\n").len() <= bamboo_hooks::portable::PORTABLE_CONTEXT_BYTES);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.starts_with("Plugin hook supplemental context")));
        assert!(session.messages.is_empty());
        let block =
            crate::runtime::runner::session_setup::prompt_envelope::build_agent_hook_context_block(
                &session,
            )
            .unwrap();
        assert!(block.content.starts_with("native context"));
    }
    #[test]
    fn plugin_context_is_chronological_sourced_and_compressible() {
        let mut session = Session::new("test", "model");
        let mut system = Message::system("host authority");
        system.never_compress = true;
        session.add_message(system);
        session.add_message(Message::user("original prompt"));
        inject_plugin_contexts(
            &mut session,
            vec![bamboo_hooks::portable::PluginContext {
                source: "fixture@0.1.0:hooks.json:PreToolUse".into(),
                text: "plugin content".into(),
            }],
        );
        assert_eq!(session.messages[0].content, "host authority");
        assert!(session.messages[0].never_compress);
        assert_eq!(session.messages.len(), 2);
        let block =
            crate::runtime::runner::session_setup::prompt_envelope::build_agent_hook_context_block(
                &session,
            )
            .unwrap();
        assert!(block.content.contains("fixture@0.1.0"));
        assert!(block.content.contains("untrusted"));
        let event = bamboo_domain::ModelContextEvent {
            id: "fixture".into(),
            epoch: 0,
            sequence: 0,
            anchor_message_id: None,
            block_type: block.block_type,
            revision: 1,
            supersedes_revision: None,
            kind: bamboo_domain::ModelContextEventKind::Snapshot,
            content_sha256: bamboo_domain::model_context_block_sha256(&block),
            rendered_text: block.render_runtime_context_text(),
        };
        let message = event.render_message();
        assert!(matches!(message.role, bamboo_agent_core::Role::User));
        assert!(!message.never_compress);
    }
}
