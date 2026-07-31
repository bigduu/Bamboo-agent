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
    pub session_flags: ToolExecutionSessionFlags,
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
    /// True only after a terminal executor outcome (`Completed` or an execution
    /// error). Pre-dispatch blocks, `Running`, and `NeedsHuman` do not represent
    /// a completed tool and must not fire `PostToolUse`.
    pub post_tool_hook_eligible: bool,
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
            post_tool_hook_eligible: false,
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
    let mut permission_override = None;

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
                    post_tool_hook_eligible: false,
                    tool_duration: elapsed,
                });
            }
            HookResult::Ask => {
                crate::runtime::hooks::inject_contexts(
                    session,
                    AgentHookPoint::BeforeToolExecution,
                    hook_outcome.injected_contexts,
                );
                if ctx.session_flags.auto_approve_permissions {
                    permission_override = Some(bamboo_tools::HookPermissionOverride::Allow);
                } else if let Some(outcome) =
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
            HookResult::Allow => {
                crate::runtime::hooks::apply_hook_outcome(
                    AgentHookPoint::BeforeToolExecution,
                    hook_outcome,
                    session,
                    runtime_state,
                )?;
                permission_override = Some(bamboo_tools::HookPermissionOverride::Allow);
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
    let dispatch = bamboo_agent_core::tools::executor::execute_tool_call_with_context_outcome(
        ctx.tool_call,
        ctx.tools.as_ref(),
        // Agent execution does not route through the legacy composition
        // runtime; catalog-pinned workflow_run is the sole orchestrator.
        None,
        tool_ctx,
    );
    let (needs_human, result, post_tool_hook_eligible) =
        match bamboo_tools::with_hook_permission_override(
            permission_override,
            &ctx.tool_call.id,
            dispatch,
        )
        .await
        {
            Ok(ToolOutcome::Completed(result)) => (None, Ok(result), true),
            Ok(ToolOutcome::Running(handle)) => (None, Ok(handle.ack), false),
            Ok(ToolOutcome::NeedsHuman { question, result }) => (Some(question), Ok(result), false),
            Err(error) => (None, Err(error), true),
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
        post_tool_hook_eligible,
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

    let requested_mode = runtime_state.effective_permission_mode();
    let resolution = bamboo_domain::resolve_permission_mode(
        requested_mode,
        config.permission_mode.unwrap_or_default(),
    );
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
        effective_mode: resolution.effective,
        bypass_requested: resolution.bypass_permissions(),
        auto_approve_requested: requested_mode == bamboo_domain::SessionPermissionMode::Auto,
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
            post_tool_hook_eligible: false,
            tool_duration: std::time::Duration::ZERO,
        });
    }

    Some(ToolExecutionOutcome {
        result: Err(
            "Hook requested approval, but no parent-agent reviewer is available; denied"
                .to_string(),
        ),
        needs_human: None,
        post_tool_hook_eligible: false,
        tool_duration: std::time::Duration::ZERO,
    })
}

fn append_post_tool_feedback(result: &mut Result<ToolResult, String>, feedback: Vec<String>) {
    let feedback = feedback
        .into_iter()
        .filter_map(|text| {
            let text = text.trim();
            (!text.is_empty()).then(|| text.to_string())
        })
        .collect::<Vec<_>>();
    if feedback.is_empty() {
        return;
    }

    let block = format!(
        "\n\n<post_tool_use_feedback>\n{}\n</post_tool_use_feedback>",
        feedback.join("\n")
    );
    match result {
        Ok(result) => result.result.push_str(&block),
        Err(error) => error.push_str(&block),
    }
}

pub(super) async fn apply_tool_execution_outcome(
    ctx: ToolExecutionApplyContext<'_>,
    mut outcome: ToolExecutionOutcome,
) -> Result<bool, AgentError> {
    let mut post_tool_feedback = Vec::new();
    let mut deferred_hook_control = None;
    if outcome.post_tool_hook_eligible
        && ctx
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
        post_tool_feedback = std::mem::take(&mut hook_outcome.injected_contexts);
        if let HookResult::Deny { reason } = hook_outcome.decision.clone() {
            post_tool_feedback.push(format!("Blocked by PostToolUse hook: {reason}"));
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
    append_post_tool_feedback(&mut outcome.result, post_tool_feedback);

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
                        session_flags: ctx.session_flags,
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
    use bamboo_agent_core::tools::{
        AsyncWaitKind, FunctionCall, RunningCompletion, RunningHandle, ToolError,
    };
    use bamboo_agent_core::AgentHook;
    use bamboo_config::{LifecycleHookGroup, LifecycleHookHandler, LifecycleHooksConfig};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    struct AllowToolHook;

    #[async_trait]
    impl AgentHook for AllowToolHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeToolExecution
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            HookResult::Allow
        }
    }

    struct PostFeedbackHook;

    #[async_trait]
    impl AgentHook for PostFeedbackHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterToolExecution
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            assert!(matches!(
                payload,
                HookPayload::ToolResult {
                    tool_name,
                    outcome: HookToolOutcome { success: true, .. },
                    ..
                } if tool_name == "probe"
            ));
            HookResult::WithContext {
                result: Box::new(HookResult::Deny {
                    reason: "generated output violates policy".to_string(),
                }),
                text: "lint: replace the generated token".to_string(),
            }
        }
    }

    struct ErrorPostFeedbackHook;

    #[async_trait]
    impl AgentHook for ErrorPostFeedbackHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterToolExecution
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            assert!(matches!(
                payload,
                HookPayload::ToolResult {
                    outcome: HookToolOutcome {
                        success: false,
                        error: Some(error),
                        ..
                    },
                    ..
                } if error == "executor exploded"
            ));
            HookResult::InjectContext {
                text: "error diagnostic from PostToolUse".to_string(),
            }
        }
    }

    struct CountingPostHook(Arc<AtomicUsize>);

    #[async_trait]
    impl AgentHook for CountingPostHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterToolExecution
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            self.0.fetch_add(1, Ordering::SeqCst);
            HookResult::Continue
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
            assert_eq!(
                ask.permission_request
                    .as_ref()
                    .map(|request| request.bypass_requested),
                Some(true),
                "hook ask must reach the parent reviewer even under bypass"
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

    enum NonTerminalOutcome {
        Running,
        NeedsHuman,
    }

    struct NonTerminalExecutor(NonTerminalOutcome);

    #[async_trait]
    impl ToolExecutor for NonTerminalExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            unreachable!("the outcome-aware path must be used")
        }

        async fn execute_with_context_outcome(
            &self,
            call: &ToolCall,
            _ctx: bamboo_agent_core::tools::ToolExecutionContext<'_>,
        ) -> Result<ToolOutcome, ToolError> {
            match self.0 {
                NonTerminalOutcome::Running => Ok(ToolOutcome::Running(RunningHandle {
                    tool_call_id: call.id.clone(),
                    ack: ToolResult::text(true, "running"),
                    completion: RunningCompletion::Detached,
                    wait_kind: AsyncWaitKind::AsyncTools,
                    kill: Box::new(|| {}),
                })),
                NonTerminalOutcome::NeedsHuman => Ok(ToolOutcome::NeedsHuman {
                    question: bamboo_agent_core::PendingQuestion {
                        tool_call_id: call.id.clone(),
                        tool_name: call.function.name.clone(),
                        question: "Choose?".to_string(),
                        options: vec!["yes".to_string(), "no".to_string()],
                        allow_custom: false,
                        source: bamboo_agent_core::PendingQuestionSource::PauseTool,
                    },
                    result: ToolResult::text(false, "decision pending"),
                }),
            }
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

    fn probe_call(name: &str) -> ToolCall {
        ToolCall {
            id: format!("call-{name}"),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: serde_json::json!({"value": 7}).to_string(),
            },
        }
    }

    async fn apply_test_outcome(
        config: &AgentLoopConfig,
        tools: &Arc<dyn ToolExecutor>,
        tool_call: &ToolCall,
        session: &mut Session,
        event_tx: &mpsc::Sender<AgentEvent>,
        outcome: ToolExecutionOutcome,
    ) -> Result<bool, AgentError> {
        let session_id = session.id.clone();
        let mut runtime_state = AgentRuntimeState::new(&session.id);
        let mut task_context = None;
        let mut state = RoundExecutionState::default();
        let session_flags = ToolExecutionSessionFlags::from_session_and_configured_mode(
            session,
            config.permission_mode.unwrap_or_default(),
        );
        apply_tool_execution_outcome(
            ToolExecutionApplyContext {
                tool_call,
                event_tx,
                metrics_collector: None,
                session_id: &session_id,
                round_id: "round-1",
                round: 0,
                session,
                tools,
                session_flags,
                config,
                runtime_state: &mut runtime_state,
                task_context: &mut task_context,
                state: &mut state,
            },
            outcome,
        )
        .await
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
    async fn configured_shell_hook_denies_bash_and_persists_synthetic_result() {
        let lifecycle_config = LifecycleHooksConfig {
            enabled: true,
            pre_tool_use: vec![LifecycleHookGroup {
                enabled: true,
                matcher: Some("^bash$".to_string()),
                hooks: vec![LifecycleHookHandler::command(
                    "printf 'configured bash denial' >&2; exit 2",
                    1_000,
                )],
            }],
            ..LifecycleHooksConfig::default()
        };
        let runner =
            crate::runtime::hooks::HookRunner::new().with_lifecycle_config(&lifecycle_config, None);
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let concrete_tools = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        let tools: Arc<dyn ToolExecutor> = concrete_tools.clone();
        let (event_tx, _event_rx) = mpsc::channel(16);
        let tool_call = probe_call("bash");
        let workspace = tempfile::tempdir().unwrap();
        let mut session = Session::new("configured-hook-deny", "model");
        session.workspace = Some(workspace.path().to_string_lossy().into_owned());
        let session_flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime_state = AgentRuntimeState::new(&session.id);

        let outcome = execute_tool_call_only(ToolExecutionOnlyContext {
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "configured-hook-deny",
            round_id: "round-1",
            round: 0,
            tools: &tools,
            config: &config,
            hook_session: Some(&mut session),
            hook_runtime_state: Some(&mut runtime_state),
            session_flags,
            available_tool_schemas: &[],
        })
        .await
        .expect("hook denial is represented as a synthetic tool error");

        assert!(!concrete_tools.0.load(Ordering::SeqCst));
        apply_test_outcome(
            &config,
            &tools,
            &tool_call,
            &mut session,
            &event_tx,
            outcome,
        )
        .await
        .unwrap();
        let tool_message = session
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(&tool_call.id))
            .expect("synthetic denial must be appended as a tool result");
        assert!(tool_message.content.contains("configured bash denial"));
    }

    #[tokio::test]
    async fn allow_hook_skips_exact_configured_ask_in_engine_dispatch() {
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(AllowToolHook));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join("engine-hook-allowed.txt");
        let permission_config = Arc::new(bamboo_tools::permission::PermissionConfig::new());
        permission_config
            .set_ask_rules([format!("Write({}/**)", workspace.path().to_string_lossy())]);
        let checker = Arc::new(bamboo_tools::permission::ConfigPermissionChecker::new(
            permission_config,
        ));
        let tools: Arc<dyn ToolExecutor> = Arc::new(
            bamboo_tools::BuiltinToolExecutorBuilder::new()
                .with_tool(bamboo_tools::WriteTool::new())
                .unwrap()
                .with_permission_checker(checker)
                .build(),
        );
        let (event_tx, _event_rx) = mpsc::channel(16);
        let tool_call = ToolCall {
            id: "call-write-allow".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: serde_json::json!({
                    "file_path": path.to_string_lossy(),
                    "content": "allowed"
                })
                .to_string(),
            },
        };
        let mut session = Session::new("hook-allow-engine", "model");
        session.workspace = Some(workspace.path().to_string_lossy().into_owned());
        let session_flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime_state = AgentRuntimeState::new(&session.id);

        let outcome = execute_tool_call_only(ToolExecutionOnlyContext {
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "hook-allow-engine",
            round_id: "round-1",
            round: 0,
            tools: &tools,
            config: &config,
            hook_session: Some(&mut session),
            hook_runtime_state: Some(&mut runtime_state),
            session_flags,
            available_tool_schemas: &[],
        })
        .await
        .unwrap();

        assert!(outcome.result.is_ok());
        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "allowed");
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
        let mut runtime_state = AgentRuntimeState::new(&session.id);
        runtime_state.bypass_permissions = true;
        session.agent_runtime_state = Some(runtime_state.clone());
        let session_flags = ToolExecutionSessionFlags::from_session(&session);

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
    async fn post_tool_feedback_is_appended_to_persisted_tool_result() {
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(PostFeedbackHook));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let tools: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        let (event_tx, _event_rx) = mpsc::channel(16);
        let tool_call = probe_call("probe");
        let mut session = Session::new("post-feedback", "model");
        let outcome = ToolExecutionOutcome {
            result: Ok(ToolResult::text(true, "raw output")),
            needs_human: None,
            post_tool_hook_eligible: true,
            tool_duration: std::time::Duration::from_millis(7),
        };

        apply_test_outcome(
            &config,
            &tools,
            &tool_call,
            &mut session,
            &event_tx,
            outcome,
        )
        .await
        .unwrap();
        let persisted = serde_json::to_vec(&session).unwrap();
        let restored: Session = serde_json::from_slice(&persisted).unwrap();
        let content = &restored
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(&tool_call.id))
            .expect("tool result must survive persistence")
            .content;
        assert!(content.contains("raw output"));
        assert!(content.contains("lint: replace the generated token"));
        assert!(content.contains("Blocked by PostToolUse hook"));
        assert!(content.contains("generated output violates policy"));
    }

    #[tokio::test]
    async fn post_tool_feedback_runs_for_executor_errors() {
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(ErrorPostFeedbackHook));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let tools: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        let (event_tx, _event_rx) = mpsc::channel(16);
        let tool_call = probe_call("probe");
        let mut session = Session::new("post-error-feedback", "model");
        let outcome = ToolExecutionOutcome {
            result: Err("executor exploded".to_string()),
            needs_human: None,
            post_tool_hook_eligible: true,
            tool_duration: std::time::Duration::from_millis(3),
        };

        apply_test_outcome(
            &config,
            &tools,
            &tool_call,
            &mut session,
            &event_tx,
            outcome,
        )
        .await
        .unwrap();
        let content = &session
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(&tool_call.id))
            .expect("error tool result must be appended")
            .content;
        assert!(content.contains("executor exploded"));
        assert!(content.contains("error diagnostic from PostToolUse"));
    }

    #[tokio::test]
    async fn post_tool_hook_skips_running_and_needs_human_outcomes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(CountingPostHook(calls.clone())));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let (event_tx, _event_rx) = mpsc::channel(32);
        let tool_call = probe_call("probe");

        for (session_id, executor) in [
            (
                "post-skip-running",
                NonTerminalExecutor(NonTerminalOutcome::Running),
            ),
            (
                "post-skip-needs-human",
                NonTerminalExecutor(NonTerminalOutcome::NeedsHuman),
            ),
        ] {
            let tools: Arc<dyn ToolExecutor> = Arc::new(executor);
            let mut session = Session::new(session_id, "model");
            let session_flags = ToolExecutionSessionFlags::from_session(&session);
            let outcome = execute_tool_call_only(ToolExecutionOnlyContext {
                tool_call: &tool_call,
                event_tx: &event_tx,
                metrics_collector: None,
                session_id,
                round_id: "round-1",
                round: 0,
                tools: &tools,
                config: &config,
                hook_session: None,
                hook_runtime_state: None,
                session_flags,
                available_tool_schemas: &[],
            })
            .await
            .unwrap();
            assert!(!outcome.post_tool_hook_eligible);
            apply_test_outcome(
                &config,
                &tools,
                &tool_call,
                &mut session,
                &event_tx,
                outcome,
            )
            .await
            .unwrap();
        }

        assert_eq!(calls.load(Ordering::SeqCst), 0);
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
