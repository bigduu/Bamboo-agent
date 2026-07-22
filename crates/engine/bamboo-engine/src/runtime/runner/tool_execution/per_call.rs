use std::sync::Arc;

use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{
    parse_tool_args_best_effort, ToolCall, ToolExecutionContext, ToolExecutionSessionFlags,
    ToolExecutor, ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::{AgentHookPoint, AgentRuntimeState, HookPayload, HookResult, HookToolOutcome};
use bamboo_metrics::MetricsCollector;

use super::execution_paths;
use super::loop_state::RoundExecutionState;
use super::policy;

fn preview_for_log(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut preview = String::new();
    for _ in 0..max_chars {
        match iter.next() {
            Some(ch) => preview.push(ch),
            None => break,
        }
    }
    if iter.next().is_some() {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n").replace('\r', "\\r")
}

pub(super) struct ToolExecutionOnlyContext<'a> {
    pub tool_call: &'a ToolCall,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub round: usize,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub config: &'a AgentLoopConfig,
    /// Present only on the sequential path when BeforeToolExecution hooks are
    /// registered. Parallel-safe tools are forced through that path whenever
    /// such hooks exist, preserving deterministic mutation/control semantics.
    pub hook_session: Option<&'a mut Session>,
    pub hook_runtime_state: Option<&'a mut AgentRuntimeState>,
    /// Per-session execution flags (e.g. bypass permissions), derived from the
    /// session via `ToolExecutionSessionFlags::from_session` at the call site
    /// and threaded through so this (parallel-safe) path can apply them without
    /// borrowing the session.
    pub session_flags: ToolExecutionSessionFlags,
    /// Snapshot of every tool schema the executor exposes for THIS round, built
    /// once per round (in `execute_round_tool_calls`) rather than re-cloned on
    /// every single tool call. Passed straight into the dispatch context's
    /// `available_tool_schemas`. Scoped to the round — never global/static — so
    /// one session's tool set can't leak into another. ASSUMPTION: the
    /// executor's tool set is stable for the duration of a round (the agent loop
    /// only registers hooks mid-round, never tools), so the snapshot equals what
    /// a fresh `list_tools()` would return on every call. It is consumed only by
    /// `for_dispatch`, which threads it into the dispatch context's
    /// `available_tool_schemas` — a metadata field no builtin tool currently
    /// inspects — so even a hypothetical divergence would be unobservable today.
    pub available_tool_schemas: &'a [ToolSchema],
}

pub(super) struct ToolExecutionApplyContext<'a> {
    pub tool_call: &'a ToolCall,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub round: usize,
    pub session: &'a mut Session,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub config: &'a AgentLoopConfig,
    pub runtime_state: &'a mut AgentRuntimeState,
    pub task_context: &'a mut Option<TaskLoopContext>,
    pub state: &'a mut RoundExecutionState,
}

pub(super) struct ToolExecutionOutcome {
    pub result: Result<ToolResult, String>,
    /// Set when the tool returned [`ToolOutcome::NeedsHuman`] — the structured
    /// pending question the loop suspends on (its display `result` is carried in
    /// `result` above, so the compressor/policy path is unchanged). Handled in
    /// [`apply_tool_execution_outcome`] before the normal success path.
    pub needs_human: Option<bamboo_agent_core::PendingQuestion>,
    pub tool_duration: std::time::Duration,
}

pub(super) async fn execute_tool_call_only(
    mut ctx: ToolExecutionOnlyContext<'_>,
) -> Result<ToolExecutionOutcome, AgentError> {
    if let Err(policy_error) = policy::validate_tool_call_arguments(ctx.tool_call) {
        tracing::warn!(
            "[{}][round:{}] Tool call blocked by strict argument policy before ToolStart: tool_call_id={}, tool_name={}, error={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            policy_error
        );
        return Ok(ToolExecutionOutcome {
            needs_human: None,
            result: Err(policy_error),
            tool_duration: std::time::Duration::ZERO,
        });
    }

    let raw_arguments = ctx.tool_call.function.arguments.trim();
    let (args, parse_warning) = parse_tool_args_best_effort(&ctx.tool_call.function.arguments);
    if let Some(warning) = parse_warning {
        tracing::warn!(
            "[{}][round:{}] Tool call arguments required fallback before ToolStart: tool_call_id={}, tool_name={}, args_len={}, args_preview=\"{}\", warning={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            raw_arguments.len(),
            preview_for_log(raw_arguments, 180),
            warning
        );
    }

    tracing::debug!(
        "[{}][round:{}] Starting tool execution: tool_call_id={}, tool_name={}, raw_args_len={}",
        ctx.session_id,
        ctx.round,
        ctx.tool_call.id,
        ctx.tool_call.function.name,
        raw_arguments.len()
    );

    super::events::send_event_with_metrics(
        ctx.event_tx,
        ctx.metrics_collector,
        ctx.session_id,
        ctx.round_id,
        AgentEvent::ToolStart {
            tool_call_id: ctx.tool_call.id.clone(),
            tool_name: ctx.tool_call.function.name.clone(),
            arguments: args.clone(),
        },
    )
    .await;

    // ── ToolEmitter: track lifecycle events ─────────────────────────────
    let tool_name = ctx.tool_call.function.name.trim();
    let is_mutating = bamboo_tools::orchestrator::classify_tool(tool_name)
        == bamboo_tools::orchestrator::ToolMutability::Mutating;
    let mut emitter =
        bamboo_tools::events::ToolEmitter::new(&ctx.tool_call.id, tool_name, is_mutating);
    emitter.set_auto_approved(!is_mutating);
    let begin_event = emitter.begin().clone();
    // Push lifecycle "begin" through the AgentEvent channel for UI visibility
    if let Err(e) = ctx.event_tx.send(begin_event.into_agent_event()).await {
        tracing::warn!(
            "[{}] tool lifecycle begin event send failed: {}",
            ctx.session_id,
            e
        );
    }

    let tool_timer = std::time::Instant::now();

    if ctx
        .config
        .hook_runner
        .has_hooks_for(AgentHookPoint::BeforeToolExecution)
    {
        let session = ctx
            .hook_session
            .as_deref_mut()
            .expect("hooked tool calls must run on the sequential path");
        let runtime_state = ctx
            .hook_runtime_state
            .as_deref_mut()
            .expect("hooked tool calls must carry runtime state");
        let payload = HookPayload::ToolExecution {
            tool_name: ctx.tool_call.function.name.clone(),
            tool_call_id: ctx.tool_call.id.clone(),
            parsed_args: args.clone(),
        };
        let hook_outcome = ctx
            .config
            .hook_runner
            .run_hooks(
                AgentHookPoint::BeforeToolExecution,
                &payload,
                session,
                runtime_state,
                Some(ctx.event_tx),
            )
            .await;

        match hook_outcome.decision.clone() {
            HookResult::Deny { reason } => {
                crate::runtime::hooks::inject_contexts(
                    session,
                    AgentHookPoint::BeforeToolExecution,
                    hook_outcome.injected_contexts,
                );
                let elapsed = tool_timer.elapsed();
                let end_event = emitter.error(reason.clone()).clone();
                let _ = ctx.event_tx.send(end_event.into_agent_event()).await;
                return Ok(ToolExecutionOutcome {
                    result: Err(format!("Tool execution denied by hook: {reason}")),
                    needs_human: None,
                    tool_duration: elapsed,
                });
            }
            HookResult::Ask => {
                crate::runtime::hooks::inject_contexts(
                    session,
                    AgentHookPoint::BeforeToolExecution,
                    hook_outcome.injected_contexts,
                );
                if let Some(outcome) =
                    hook_ask_outcome(ctx.tool_call, ctx.config, session, runtime_state, &args).await
                {
                    let end_event = match &outcome.result {
                        Ok(_) => emitter
                            .finish(Some("waiting for parent review".to_string()))
                            .clone(),
                        Err(error) => emitter.error(error.clone()).clone(),
                    };
                    let _ = ctx.event_tx.send(end_event.into_agent_event()).await;
                    return Ok(outcome);
                }
            }
            _ => {
                if let Err(error) = crate::runtime::hooks::apply_hook_outcome(
                    AgentHookPoint::BeforeToolExecution,
                    hook_outcome,
                    session,
                    runtime_state,
                ) {
                    let end_event = emitter.error(error.to_string()).clone();
                    let _ = ctx.event_tx.send(end_event.into_agent_event()).await;
                    return Err(error);
                }
            }
        }
    }

    // THIS is the live server tool-dispatch path (engine runtime). Build via
    // `for_dispatch` so per-session flags stay in sync with the other loop
    // (bamboo-agent-core's `result_handler.rs`). The schema slice is the
    // per-round snapshot threaded in via `ctx.available_tool_schemas` (built
    // once in `execute_round_tool_calls`) instead of re-cloning every call.
    let tool_ctx = ToolExecutionContext::for_dispatch(
        ctx.session_id,
        &ctx.tool_call.id,
        ctx.event_tx,
        ctx.available_tool_schemas,
        ctx.session_flags,
        // Only let the Bash auto path promote to background when this loop can
        // actually suspend for and self-resume the shell — i.e. a
        // `bash_resume_hook` AND persistence are both wired (issue #84, phase
        // 2d). On hook-less paths (e.g. the schedule loop) this is false, so the
        // auto path stays synchronous and never orphans a promoted shell.
        ctx.config.bash_resume_hook.is_some() && ctx.config.persistence.is_some(),
        // Loop-facing background-Bash completion sink (issue #84 Phase 2b
        // follow-up). Threaded from the loop config so the Bash tool can push a
        // shell's result into this loop on completion. `None` on loops without
        // it wired, leaving the push inert (the poll backstop still runs).
        ctx.config.bash_completion_sink.as_ref(),
        // Reuse the args parsed above (for the `ToolStart` event) instead of
        // re-parsing the raw JSON string downstream in the executor (issue #106).
        // `args` came from `parse_tool_args_best_effort`, the same parser the
        // executor would call, so reuse is byte-for-byte equivalent.
        Some(&args),
    );

    // Outcome-aware dispatch. Extract a NeedsHuman pending question (handled in
    // apply before the success path) and collapse the rest to a ToolResult so the
    // compressor / policy / transcript path is unchanged. Completed -> its result,
    // Running -> its synthetic ack, NeedsHuman -> its rich display result.
    let (needs_human, result) =
        match bamboo_agent_core::tools::executor::execute_tool_call_with_context_outcome(
            ctx.tool_call,
            ctx.tools.as_ref(),
            // Agent execution does not route through the legacy composition
            // runtime; catalog-pinned workflow_run is the sole orchestrator.
            None,
            tool_ctx,
        )
        .await
        {
            Ok(ToolOutcome::NeedsHuman { question, result }) => (Some(question), Ok(result)),
            Ok(other) => (None, Ok(other.into_tool_result())),
            Err(error) => (None, Err(error)),
        };

    let tool_duration = tool_timer.elapsed();

    // Emit lifecycle event based on result and push through AgentEvent channel
    let end_event = match &result {
        Ok(_) => emitter
            .finish(Some(format!("completed in {:?}", tool_duration)))
            .clone(),
        Err(err) => emitter.error(format!("{}", err)).clone(),
    };
    if let Err(e) = ctx.event_tx.send(end_event.into_agent_event()).await {
        tracing::warn!(
            "[{}] tool lifecycle end event send failed: {}",
            ctx.session_id,
            e
        );
    }

    tracing::trace!(
        "[{}][round:{}] ToolEmitter: call_id={}, tool={}, events={}",
        ctx.session_id,
        ctx.round,
        ctx.tool_call.id,
        tool_name,
        emitter.events().len()
    );

    Ok(ToolExecutionOutcome {
        result: result.map_err(|error| error.to_string()),
        needs_human,
        tool_duration,
    })
}

/// Resolve `HookResult::Ask` without ever opening an unowned/manual approval.
/// External workers use their ambient parent proxy inline. Missing or failed
/// parent routes fail closed; this path never reuses the interactive
/// clarification/approval flow.
async fn hook_ask_outcome(
    tool_call: &ToolCall,
    config: &AgentLoopConfig,
    session: &Session,
    runtime_state: &AgentRuntimeState,
    args: &serde_json::Value,
) -> Option<ToolExecutionOutcome> {
    let tool_name = tool_call.function.name.trim().to_string();
    let permission_context = bamboo_tools::permission::check_permissions(&tool_name, args)
        .ok()
        .flatten()
        .and_then(|contexts| contexts.into_iter().next());
    let (permission_type, resource, operation_summary, risk_level) =
        if let Some(permission) = permission_context {
            let risk_level = permission.risk_level();
            (
                permission.permission_type,
                permission.resource,
                permission.operation_description,
                risk_level,
            )
        } else {
            let permission_type = bamboo_tools::permission::PermissionType::ExecuteCommand;
            (
                permission_type,
                args.to_string(),
                format!("Hook-requested review for {tool_name}"),
                permission_type.risk_level(),
            )
        };

    let request = bamboo_tools::permission::PermissionRequest {
        request_id: tool_call.id.clone(),
        session_id: session.id.clone(),
        workspace_path: session.workspace_path_meta(),
        tool_name: tool_name.clone(),
        permission_type,
        resource: resource.clone(),
        operation_summary,
        risk_level,
        reason_code: bamboo_tools::permission::PermissionReasonCode::ConfiguredAlwaysAsk,
        effective_mode: config.permission_mode.unwrap_or_default(),
        bypass_requested: runtime_state.bypass_permissions,
        policy_revision: 0,
        matched_rule: None,
        allowed_decisions: bamboo_tools::permission::PermissionRequest::forced_decisions(),
        suggested_matchers: bamboo_tools::permission::conservative_matchers(
            permission_type,
            &resource,
        ),
    };

    if let Some(proxy) = bamboo_tools::current_approval_proxy() {
        let approved = proxy
            .request_approval(bamboo_tools::ApprovalAsk {
                tool_name,
                permission: permission_type.description().to_string(),
                resource,
                permission_request: Some(request),
            })
            .await;
        return (!approved).then(|| ToolExecutionOutcome {
            result: Err("Tool execution denied by parent agent review".to_string()),
            needs_human: None,
            tool_duration: std::time::Duration::ZERO,
        });
    }

    Some(ToolExecutionOutcome {
        result: Err(
            "Hook requested approval, but no parent-agent reviewer is available; denied"
                .to_string(),
        ),
        needs_human: None,
        tool_duration: std::time::Duration::ZERO,
    })
}

pub(super) async fn apply_tool_execution_outcome(
    ctx: ToolExecutionApplyContext<'_>,
    mut outcome: ToolExecutionOutcome,
) -> Result<bool, AgentError> {
    let mut deferred_contexts = Vec::new();
    let mut deferred_hook_control = None;
    if ctx
        .config
        .hook_runner
        .has_hooks_for(AgentHookPoint::AfterToolExecution)
    {
        let hook_payload = HookPayload::ToolResult {
            tool_name: ctx.tool_call.function.name.clone(),
            tool_call_id: ctx.tool_call.id.clone(),
            outcome: match &outcome.result {
                Ok(result) => HookToolOutcome {
                    success: result.success,
                    result: Some(result.result.clone()),
                    error: None,
                    needs_human: outcome.needs_human.is_some(),
                    duration_ms: outcome.tool_duration.as_millis() as u64,
                },
                Err(error) => HookToolOutcome {
                    success: false,
                    result: None,
                    error: Some(error.clone()),
                    needs_human: false,
                    duration_ms: outcome.tool_duration.as_millis() as u64,
                },
            },
        };
        let mut hook_outcome = ctx
            .config
            .hook_runner
            .run_hooks(
                AgentHookPoint::AfterToolExecution,
                &hook_payload,
                ctx.session,
                ctx.runtime_state,
                Some(ctx.event_tx),
            )
            .await;
        deferred_contexts = std::mem::take(&mut hook_outcome.injected_contexts);
        if let HookResult::Deny { reason } = hook_outcome.decision.clone() {
            outcome.needs_human = None;
            outcome.result = Err(format!("Tool result denied by hook: {reason}"));
            hook_outcome.decision = HookResult::Continue;
        }
        if matches!(
            hook_outcome.decision,
            HookResult::Suspend { .. } | HookResult::Abort { .. } | HookResult::Ask
        ) {
            deferred_hook_control = Some(hook_outcome);
        } else {
            crate::runtime::hooks::apply_hook_outcome(
                AgentHookPoint::AfterToolExecution,
                hook_outcome,
                ctx.session,
                ctx.runtime_state,
            )?;
        }
    }

    // Capture tool lifecycle metadata before the borrow-splitting match.
    let tool_name_for_meta = ctx.tool_call.function.name.clone();
    let tool_call_id_for_meta = ctx.tool_call.id.clone();
    let tool_duration_ms = outcome.tool_duration.as_millis() as u64;
    let is_success = outcome.result.is_ok();

    let is_mutating = bamboo_tools::orchestrator::classify_tool(&tool_name_for_meta)
        == bamboo_tools::orchestrator::ToolMutability::Mutating;

    // The tool asked for a human decision (Phase B): suspend directly on the
    // returned PendingQuestion — no marker sniff. Its rich display result is the
    // `Ok` value in `outcome.result`.
    let result = if let Some(pending_question) = outcome.needs_human {
        let display_result = outcome.result.unwrap_or_else(|_| ToolResult {
            success: true,
            result: String::new(),
            display_preference: None,
            images: Vec::new(),
        });
        // Preserve the per-tool task-progress accounting that the success path
        // runs for every tool. An interactive tool that suspends (e.g.
        // conclusion_with_options) must still record its call against the active
        // task item — parity with the pre-Phase-B Completed+sniff path, which ran
        // handle_successful_tool_result (→ track_task_progress) before the sniff
        // suspended. The other success-path steps (taskwrite/workspace/goal/
        // agentic) are tool-specific no-ops here, and suspend_for_pending_question
        // already emits the ToolComplete event.
        super::task::track_task_progress(
            ctx.task_context,
            ctx.event_tx,
            ctx.session_id,
            ctx.tool_call,
            &display_result,
            ctx.round,
        )
        .await;
        super::clarification::suspend_for_pending_question(
            ctx.tool_call,
            pending_question,
            display_result,
            ctx.session,
            ctx.event_tx,
            ctx.metrics_collector,
            ctx.session_id,
            ctx.round_id,
            ctx.config,
        )
        .await;
        ctx.state.mark_awaiting_clarification();
        true
    } else {
        match outcome.result {
            Ok(result) => {
                let r = execution_paths::handle_successful_tool_result(
                    execution_paths::SuccessPathContext {
                        tool_call: ctx.tool_call,
                        result: &result,
                        event_tx: ctx.event_tx,
                        metrics_collector: ctx.metrics_collector,
                        session_id: ctx.session_id,
                        round_id: ctx.round_id,
                        round: ctx.round,
                        session: ctx.session,
                        tools: ctx.tools,
                        config: ctx.config,
                        task_context: ctx.task_context,
                        state: ctx.state,
                        tool_duration: outcome.tool_duration,
                    },
                )
                .await;
                r
            }
            Err(error_message) => {
                execution_paths::handle_tool_execution_error(
                    ctx.tool_call,
                    &error_message,
                    ctx.event_tx,
                    ctx.metrics_collector,
                    ctx.session_id,
                    ctx.round_id,
                    ctx.round,
                    ctx.session,
                    ctx.state,
                )
                .await;
                false
            }
        }
    };

    // ── Persist lifecycle metadata on the tool result message ──────────
    // Find the last tool-result message matching this tool_call_id and
    // attach execution metadata so it is persisted in session.json and
    // available when the frontend reloads the session later.
    let metadata_value = serde_json::json!({
        "elapsed_ms": tool_duration_ms,
        "is_mutating": is_mutating,
        "auto_approved": !is_mutating,
        "tool_name": tool_name_for_meta,
        "success": is_success,
    });
    if let Some(msg) = ctx
        .session
        .messages
        .iter_mut()
        .rev()
        .find(|m| m.tool_call_id.as_deref() == Some(&tool_call_id_for_meta))
    {
        msg.metadata = Some(metadata_value);
    }

    crate::runtime::hooks::inject_contexts(
        ctx.session,
        AgentHookPoint::AfterToolExecution,
        deferred_contexts,
    );

    if let Some(hook_outcome) = deferred_hook_control {
        crate::runtime::hooks::apply_hook_outcome(
            AgentHookPoint::AfterToolExecution,
            hook_outcome,
            ctx.session,
            ctx.runtime_state,
        )?;
    }

    Ok(result)
}

#[cfg(test)]
mod hook_tests {
    use super::*;
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{FunctionCall, ToolError};
    use bamboo_agent_core::AgentHook;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DenyToolHook;

    #[async_trait]
    impl AgentHook for DenyToolHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeToolExecution
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            assert!(matches!(
                payload,
                HookPayload::ToolExecution {
                    tool_name,
                    parsed_args,
                    ..
                } if tool_name == "probe" && parsed_args["value"] == 7
            ));
            HookResult::Deny {
                reason: "policy hook blocked probe".to_string(),
            }
        }

        fn name(&self) -> &str {
            "deny_probe"
        }
    }

    struct AskToolHook;

    #[async_trait]
    impl AgentHook for AskToolHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeToolExecution
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            HookResult::Ask
        }
    }

    struct RecordingParentReviewer {
        seen: AtomicBool,
        approve: bool,
    }

    #[async_trait]
    impl bamboo_tools::ApprovalProxy for RecordingParentReviewer {
        async fn request_approval(&self, ask: bamboo_tools::ApprovalAsk) -> bool {
            assert_eq!(ask.tool_name, "probe");
            assert_eq!(
                ask.permission_request
                    .as_ref()
                    .map(|request| request.reason_code),
                Some(bamboo_tools::permission::PermissionReasonCode::ConfiguredAlwaysAsk)
            );
            self.seen.store(true, Ordering::SeqCst);
            self.approve
        }
    }

    struct RecordingExecutor(AtomicBool);

    #[async_trait]
    impl ToolExecutor for RecordingExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            self.0.store(true, Ordering::SeqCst);
            Ok(ToolResult {
                success: true,
                result: "executed".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    struct PanicLegacyApprovalDelegate;

    #[async_trait]
    impl crate::runtime::config::ApprovalDelegate for PanicLegacyApprovalDelegate {
        async fn delegate_child_approval(
            &self,
            _request: crate::runtime::config::ChildApprovalRequest,
        ) -> Result<crate::runtime::config::ChildApprovalOutcome, String> {
            panic!("Hook Ask must not enter the legacy interactive approval path")
        }
    }

    #[tokio::test]
    async fn before_tool_hook_denies_without_dispatching_executor() {
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(DenyToolHook));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let tools = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let tool_call = ToolCall {
            id: "call-probe".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "probe".to_string(),
                arguments: serde_json::json!({"value": 7}).to_string(),
            },
        };
        let mut session = Session::new("hook-deny-session", "model");
        let session_flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime_state = AgentRuntimeState::new(&session.id);

        let outcome = execute_tool_call_only(ToolExecutionOnlyContext {
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "hook-deny-session",
            round_id: "round-1",
            round: 0,
            tools: &(tools.clone() as Arc<dyn ToolExecutor>),
            config: &config,
            hook_session: Some(&mut session),
            hook_runtime_state: Some(&mut runtime_state),
            session_flags,
            available_tool_schemas: &[],
        })
        .await
        .expect("deny is a tool outcome, not a runner error");

        assert!(matches!(outcome.result, Err(ref error) if error.contains("policy hook blocked")));
        assert!(!tools.0.load(Ordering::SeqCst));
        assert_eq!(runtime_state.checkpoints.len(), 1);
        let events: Vec<_> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::HookLifecycle { hook_name, decision: HookResult::Deny { .. }, .. }
                if hook_name == "deny_probe"
        )));
    }

    #[tokio::test]
    async fn ask_hook_routes_to_parent_proxy_and_executes_only_after_approval() {
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(AskToolHook));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let tools = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        let tool_executor: Arc<dyn ToolExecutor> = tools.clone();
        let reviewer = Arc::new(RecordingParentReviewer {
            seen: AtomicBool::new(false),
            approve: true,
        });
        let reviewer_proxy: Arc<dyn bamboo_tools::ApprovalProxy> = reviewer.clone();
        let (event_tx, _event_rx) = mpsc::channel(16);
        let tool_call = ToolCall {
            id: "call-probe".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "probe".to_string(),
                arguments: serde_json::json!({"value": 7}).to_string(),
            },
        };
        let mut session = Session::new("hook-ask-session", "model");
        let session_flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime_state = AgentRuntimeState::new(&session.id);

        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy),
            execute_tool_call_only(ToolExecutionOnlyContext {
                tool_call: &tool_call,
                event_tx: &event_tx,
                metrics_collector: None,
                session_id: "hook-ask-session",
                round_id: "round-1",
                round: 0,
                tools: &tool_executor,
                config: &config,
                hook_session: Some(&mut session),
                hook_runtime_state: Some(&mut runtime_state),
                session_flags,
                available_tool_schemas: &[],
            }),
        )
        .await
        .expect("approved parent review should continue dispatch");

        assert!(outcome.result.is_ok());
        assert!(reviewer.seen.load(Ordering::SeqCst));
        assert!(tools.0.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn ask_hook_without_parent_proxy_fails_closed_without_manual_prompt() {
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(AskToolHook));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            approval_delegate: Some(Arc::new(PanicLegacyApprovalDelegate)),
            ..Default::default()
        };
        let tools = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        let tool_executor: Arc<dyn ToolExecutor> = tools.clone();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let tool_call = ToolCall {
            id: "call-probe".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "probe".to_string(),
                arguments: serde_json::json!({"value": 7}).to_string(),
            },
        };
        let mut session = Session::new("hook-ask-no-parent", "model");
        session.parent_session_id = Some("parent-with-legacy-delegate".to_string());
        let session_flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime_state = AgentRuntimeState::new(&session.id);

        let outcome = execute_tool_call_only(ToolExecutionOnlyContext {
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "hook-ask-no-parent",
            round_id: "round-1",
            round: 0,
            tools: &tool_executor,
            config: &config,
            hook_session: Some(&mut session),
            hook_runtime_state: Some(&mut runtime_state),
            session_flags,
            available_tool_schemas: &[],
        })
        .await
        .expect("missing parent is represented as a denied tool outcome");

        assert!(matches!(
            outcome.result,
            Err(ref error) if error.contains("no parent-agent reviewer")
        ));
        assert!(!tools.0.load(Ordering::SeqCst));
        assert!(
            !std::iter::from_fn(|| event_rx.try_recv().ok()).any(|event| matches!(
                event,
                AgentEvent::NeedClarification { .. } | AgentEvent::ChildApprovalRequested { .. }
            ))
        );
    }
}
