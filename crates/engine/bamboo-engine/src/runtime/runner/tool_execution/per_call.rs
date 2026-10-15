use std::sync::Arc;

use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{
    parse_tool_args_best_effort, tool_start_arguments_for_display, ExecutingSupervisorObservation,
    ToolCall, ToolExecutionContext, ToolExecutionSessionFlags, ToolExecutor, ToolOutcome,
    ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::{
    AgentHookPoint, AgentRuntimeState, EffectiveCallableSet, HookPayload, HookResult,
    HookToolOutcome,
};
use bamboo_metrics::MetricsCollector;

use super::execution_paths;
use super::loop_state::RoundExecutionState;
use super::policy;
use crate::session_app::approval_replay::PermissionReplayOrigin;

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

fn parse_warning_log_details<'a>(
    execution_name: &str,
    raw_arguments: &str,
    warning: &'a str,
) -> (String, &'a str) {
    if execution_name.eq_ignore_ascii_case("browser")
        || execution_name.eq_ignore_ascii_case("browser_eval")
        || execution_name.eq_ignore_ascii_case("session_message")
        || [
            "work_overview",
            "work_search",
            "work_inspect",
            "work_changes",
            "work_update",
            "work_dispatch",
        ]
        .iter()
        .any(|name| execution_name.eq_ignore_ascii_case(name))
    {
        ("[redacted]".to_string(), "[redacted]")
    } else {
        (preview_for_log(raw_arguments, 180), warning)
    }
}

fn tool_start_name_for_display(tool_name: &str) -> String {
    if tool_name
        .trim()
        .rsplit("::")
        .next()
        .is_some_and(|name| name.eq_ignore_ascii_case("browser"))
    {
        "browser".to_string()
    } else {
        tool_name.to_string()
    }
}

pub(super) struct ToolExecutionOnlyContext<'a> {
    pub vision_support: bool,
    pub tool_call: &'a ToolCall,
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    /// Immutable parallel-window projection, used only without a hook Session.
    /// Sequential calls reread the actual Session after all continuing hooks.
    pub output_cap: Option<u32>,
    /// Snapshotted from the durable Root authority at this dispatch boundary.
    /// A parallel batch shares one snapshot across its already-admitted calls.
    pub root_orchestration_only: bool,
    /// Root-session identity snapshotted from the executing Session before any
    /// parallel dispatch borrow begins.
    pub root_session_id: &'a str,
    /// Original executing lifetime captured before this call is handed off.
    pub executing_supervisor: Option<ExecutingSupervisorObservation>,
    pub round_id: &'a str,
    pub round: usize,
    pub tools: &'a Arc<dyn ToolExecutor>,
    pub config: &'a AgentLoopConfig,
    /// Always present on the sequential path, including when portable hooks
    /// are enabled after admission. Parallel-safe calls cannot borrow mutable
    /// session state and fail closed if hooks become active before execution.
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

pub(super) struct PortableToolExecution {
    name: String,
    input: serde_json::Value,
}

impl PortableToolExecution {
    pub(super) fn execution_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
impl PortableToolExecution {
    /// Declare an already-selected executor identity for result-contract tests.
    /// Model catalog admission is covered separately by the promotion tests.
    pub(super) fn selected_identity_fixture(execution_name: &str) -> Self {
        Self {
            name: execution_name.to_owned(),
            input: serde_json::json!({}),
        }
    }
}

pub(super) fn portable_tool_for_admitted_call(
    callable: &EffectiveCallableSet,
    call: &ToolCall,
) -> Option<PortableToolExecution> {
    // Timeout synthesis uses the same immutable admission snapshot as dispatch,
    // never a later catalog or a generic name normalizer.
    callable
        .resolve_callable_reference(&call.function.name)
        .map(|name| PortableToolExecution {
            name,
            input: parse_tool_args_best_effort(&call.function.arguments).0,
        })
}

pub(super) struct ToolExecutionOutcome {
    /// Exact dispatch observation for the original compressor. None means no
    /// retained capture (also synthesized timeout), not proof of non-execution.
    /// Some(None) is a real dispatch with an unknown cap; zero remains known.
    pub output_cap: Option<Option<u32>>,
    /// The exact selected executor and arguments supplied to it, carried through
    /// result application without resolving model-controlled aliases a second time.
    pub portable_tool: Option<PortableToolExecution>,
    pub permission_replay_origin: Option<PermissionReplayOrigin>,
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

pub(super) enum ShellAccountingOutcome {
    Ordinary,
    Pending,
    Terminal(bool),
}

pub(super) fn shell_accounting_outcome(
    outcome: &ToolExecutionOutcome,
    admitted_execution_name: Option<&str>,
) -> ShellAccountingOutcome {
    if !matches!(
        outcome
            .portable_tool
            .as_ref()
            .map(|tool| tool.execution_name())
            .or(admitted_execution_name),
        Some("exec_command" | "write_stdin")
    ) {
        return ShellAccountingOutcome::Ordinary;
    }
    if outcome.needs_human.is_some() {
        return ShellAccountingOutcome::Pending;
    }
    let Ok(result) = &outcome.result else {
        // Infrastructure/argument errors retain ordinary failure accounting.
        return ShellAccountingOutcome::Ordinary;
    };
    // The permission gate uses a Completed acknowledgement while the command
    // remains unexecuted. Its pause marker survives output compression, unlike
    // the JSON body; use the same success/marker contract as clarification.
    if result.success && result.display_preference.as_deref() == Some("request_permissions") {
        return ShellAccountingOutcome::Pending;
    }
    let envelope = serde_json::from_str::<serde_json::Value>(&result.result).ok();
    let status = envelope.as_ref().and_then(|value| value["status"].as_str());
    if status == Some("running") {
        return ShellAccountingOutcome::Pending;
    }
    ShellAccountingOutcome::Terminal(
        result.success
            && status == Some("completed")
            && envelope
                .as_ref()
                .and_then(|value| value["exit_code"].as_i64())
                == Some(0),
    )
}

fn task_progress_result_for_outcome(
    outcome: &ToolExecutionOutcome,
) -> execution_paths::TaskProgressResult {
    use execution_paths::TaskProgressResult;

    match shell_accounting_outcome(outcome, None) {
        ShellAccountingOutcome::Ordinary => TaskProgressResult::Ordinary,
        ShellAccountingOutcome::Pending => TaskProgressResult::Skip,
        ShellAccountingOutcome::Terminal(success) => {
            let mut result = outcome
                .result
                .as_ref()
                .expect("terminal shell result")
                .clone();
            result.success = success;
            TaskProgressResult::CommandOutcome(result)
        }
    }
}

/// Admit one model-requested function call before it enters Bamboo's existing
/// tool execution pipeline.
///
/// Capability loading changes availability only: an admitted call retains its
/// original spelling and continues through the exact same argument, event,
/// hook, permission, and executor path below. A rejected call is represented as
/// an ordinary per-call outcome so sibling calls in a parallel batch keep
/// running and the normal result-application path can append the corresponding
/// tool error without emitting `ToolStart` or entering downstream hooks.
pub(super) async fn execute_model_requested_tool_call_only(
    effective_callable_set: &EffectiveCallableSet,
    ctx: ToolExecutionOnlyContext<'_>,
) -> Result<ToolExecutionOutcome, AgentError> {
    let Some(execution_name) =
        effective_callable_set.resolve_callable_reference(&ctx.tool_call.function.name)
    else {
        let message = format!(
            "Tool '{}' is not callable at the current conversation position",
            ctx.tool_call.function.name
        );
        tracing::warn!(
            "[{}][round:{}] Tool call rejected by capability loading before ToolStart: tool_call_id={}, tool_name={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
        );
        return Ok(ToolExecutionOutcome {
            output_cap: None,
            permission_replay_origin: None,
            result: Err(message),
            needs_human: None,
            portable_tool: None,
            post_tool_hook_eligible: false,
            tool_duration: std::time::Duration::ZERO,
        });
    };

    if execution_name == "ViewImage" && !ctx.vision_support {
        return Ok(ToolExecutionOutcome {
            output_cap: None, permission_replay_origin: None,
            result: Err("ViewImage is unavailable: the current model does not support Vision. Select a Vision-capable model or enable supports_vision for this model in provider settings. No image was read or sent.".into()),
            needs_human: None, portable_tool: None, post_tool_hook_eligible: false,
            tool_duration: std::time::Duration::ZERO,
        });
    }
    execute_tool_call_only_with_execution_name(&execution_name, ctx).await
}

#[cfg(test)]
pub(super) async fn execute_tool_call_only(
    ctx: ToolExecutionOnlyContext<'_>,
) -> Result<ToolExecutionOutcome, AgentError> {
    let execution_name = ctx.tool_call.function.name.clone();
    execute_tool_call_only_with_execution_name(&execution_name, ctx).await
}

async fn execute_tool_call_only_with_execution_name(
    execution_name: &str,
    mut ctx: ToolExecutionOnlyContext<'_>,
) -> Result<ToolExecutionOutcome, AgentError> {
    // Recheck the resolved exact executor identity even if the provider cites a
    // stale schema or the round's EffectiveCallableSet predates live tightening.
    // Reject before ToolStart, hooks, replay handling, and executor entry.
    let executing_supervisor = ctx.executing_supervisor.is_some()
        && ctx.session_id == bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID
        && ctx.root_session_id == ctx.session_id;
    // Filtering a registered exact identity out of the model catalog must
    // not turn its spelling into a compatibility alias for another owner.
    let reference = ctx.tool_call.function.name.trim();
    let exact_owner_shadowed = reference != execution_name && ctx.tools.owns_exact_tool(reference);
    if (ctx.root_orchestration_only || executing_supervisor)
        && (exact_owner_shadowed
            || !bamboo_domain::orchestration_only_allows_execution_name_for_supervisor(
                execution_name,
                executing_supervisor,
            ))
    {
        tracing::warn!(
            "[{}][round:{}] Tool call rejected by Root authority before ToolStart: tool_call_id={}, tool_name={}, execution_name={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            execution_name,
        );
        return Ok(ToolExecutionOutcome {
            output_cap: None,
            permission_replay_origin: None,
            needs_human: None,
            portable_tool: None,
            post_tool_hook_eligible: false,
            result: Err(if executing_supervisor {
                format!("supervisor_delegation_required: Tool '{}' is outside Supervisor orchestration authority. Delegate execution to an authorized worker. If unavailable or denied, report the actual recoverable blocker; do not self-execute or retry unchanged delegation indefinitely.", ctx.tool_call.function.name)
            } else {
                format!(
                    "Tool '{}' is outside orchestration-only Root authority",
                    ctx.tool_call.function.name
                )
            }),
            tool_duration: std::time::Duration::ZERO,
        });
    }

    if let Err(policy_error) =
        policy::validate_tool_call_arguments_for_execution(ctx.tool_call, execution_name)
    {
        tracing::warn!(
            "[{}][round:{}] Tool call blocked by strict argument policy before ToolStart: tool_call_id={}, tool_name={}, error={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            policy_error
        );
        return Ok(ToolExecutionOutcome {
            output_cap: None,
            permission_replay_origin: None,
            needs_human: None,
            portable_tool: None,
            post_tool_hook_eligible: false,
            result: Err(policy_error),
            tool_duration: std::time::Duration::ZERO,
        });
    }

    let raw_arguments = ctx.tool_call.function.arguments.trim();
    let (args, parse_warning) = parse_tool_args_best_effort(&ctx.tool_call.function.arguments);
    if let Some(warning) = parse_warning {
        let (args_preview, warning) =
            parse_warning_log_details(execution_name, raw_arguments, &warning);
        tracing::warn!(
            "[{}][round:{}] Tool call arguments required fallback before ToolStart: tool_call_id={}, tool_name={}, args_len={}, args_preview=\"{}\", warning={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            raw_arguments.len(),
            args_preview,
            warning
        );
    }

    tracing::debug!(
        "[{}][round:{}] Starting tool execution: tool_call_id={}, tool_name={}, raw_args_len={}",
        ctx.session_id,
        ctx.round,
        ctx.tool_call.id,
        tool_start_name_for_display(&ctx.tool_call.function.name),
        raw_arguments.len()
    );

    super::events::send_event_with_metrics(
        ctx.event_tx,
        ctx.metrics_collector,
        ctx.session_id,
        ctx.round_id,
        AgentEvent::ToolStart {
            tool_call_id: ctx.tool_call.id.clone(),
            tool_name: tool_start_name_for_display(&ctx.tool_call.function.name),
            arguments: tool_start_arguments_for_display(execution_name, &args),
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
        let (Some(session), Some(runtime_state)) = (
            ctx.hook_session.as_deref_mut(),
            ctx.hook_runtime_state.as_deref_mut(),
        ) else {
            // A review can enable portable hooks after this parallel batch was
            // admitted without mutable hook state. Do not panic or bypass the
            // newly active policy; the next admission can run sequentially.
            let reason = "Tool hooks became active after parallel admission; retry the tool call"
                .to_string();
            let end_event = emitter.error(reason.clone()).clone();
            let _ = ctx.event_tx.send(end_event.into_agent_event()).await;
            return Ok(ToolExecutionOutcome {
                output_cap: None,
                permission_replay_origin: None,
                result: Err(reason),
                needs_human: None,
                portable_tool: None,
                post_tool_hook_eligible: false,
                tool_duration: tool_timer.elapsed(),
            });
        };
        let payload = HookPayload::ToolExecution {
            tool_name: ctx.tool_call.function.name.clone(),
            tool_call_id: ctx.tool_call.id.clone(),
            parsed_args: args.clone(),
        };
        let mut hook_outcome = ctx
            .config
            .hook_runner
            .run_hooks_with_inputs(
                AgentHookPoint::BeforeToolExecution,
                &payload,
                session,
                runtime_state,
                Some(ctx.event_tx),
                bamboo_hooks::portable::PortableInputs {
                    resolved_tool_name: Some(execution_name),
                    original_tool_input: Some(&args),
                    ..Default::default()
                },
            )
            .await;

        crate::runtime::hooks::inject_plugin_contexts(
            session,
            std::mem::take(&mut hook_outcome.plugin_contexts),
        );
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
                    output_cap: None,
                    permission_replay_origin: None,
                    result: Err(format!("Tool execution denied by hook: {reason}")),
                    needs_human: None,
                    portable_tool: None,
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
                } else if let Some(outcome) = hook_ask_outcome(
                    ctx.tool_call,
                    execution_name,
                    ctx.config,
                    session,
                    runtime_state,
                    &args,
                )
                .await
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

    // Hooks take an immutable Session and may inject context/control. Observe
    // its current scalar only after they allow dispatch; a cleared value must
    // not fall back to the earlier parallel-window projection.
    let output_cap = ctx
        .hook_session
        .as_deref()
        .map_or(ctx.output_cap, |session| {
            session
                .effective_token_budget()
                .map(|budget| budget.max_tool_output_tokens)
        });

    // THIS is the live server tool-dispatch path (engine runtime). Build via
    // `for_dispatch` so per-session flags stay in sync with the other loop
    // (bamboo-agent-core's `result_handler.rs`). The schema slice is the
    // per-round snapshot threaded in via `ctx.available_tool_schemas` (built
    // once in `execute_round_tool_calls`) instead of re-cloning every call.
    let tool_ctx = ToolExecutionContext::for_dispatch(
        ctx.session_id,
        ctx.root_session_id,
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
    )
    .with_executing_supervisor(ctx.executing_supervisor);

    // Outcome-aware dispatch. Extract a NeedsHuman pending question (handled in
    // apply before the success path) and collapse the rest to a ToolResult so the
    // compressor / policy / transcript path is unchanged. Completed -> its result,
    // Running -> its synthetic ack, NeedsHuman -> its rich display result.
    // Dispatch through the exact identity already selected by the effective
    // callable-set resolver. Portable policies receive this identity; native
    // hooks, events, and permission handling above keep
    // the model's original call spelling, while the executor cannot re-resolve
    // that spelling to a different (possibly excluded) exact owner.
    let dispatch =
        ctx.tools
            .execute_exact_with_context_outcome(ctx.tool_call, execution_name, tool_ctx);
    let dispatch = bamboo_agent_core::tools::context::with_root_actor_tool_writer(
        ctx.config
            .persistence
            .as_ref()
            .and_then(|persistence| persistence.root_actor_writer()),
        dispatch,
    );
    let dispatch = bamboo_agent_core::tools::scope_tool_output_cap(
        ctx.session_id,
        &ctx.tool_call.id,
        output_cap,
        dispatch,
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
        output_cap: Some(output_cap),
        permission_replay_origin: ctx.executing_supervisor.map(|observation| {
            PermissionReplayOrigin::new(observation, ctx.session_id, ctx.tool_call, execution_name)
        }),
        result: result.map_err(|error| error.to_string()),
        needs_human,
        post_tool_hook_eligible,
        portable_tool: Some(PortableToolExecution {
            name: execution_name.to_owned(),
            input: args,
        }),
        tool_duration,
    })
}

/// Resolve `HookResult::Ask` without ever opening an unowned/manual approval.
/// External workers use their ambient parent proxy inline. Missing or failed
/// parent routes fail closed; this path never reuses the interactive
/// clarification/approval flow.
async fn hook_ask_outcome(
    tool_call: &ToolCall,
    execution_name: &str,
    config: &AgentLoopConfig,
    session: &Session,
    runtime_state: &AgentRuntimeState,
    args: &serde_json::Value,
) -> Option<ToolExecutionOutcome> {
    let tool_name = tool_call.function.name.trim().to_string();
    let browser_execution = execution_name.trim().eq_ignore_ascii_case("browser");
    let focused_browser_input =
        bamboo_tools::permission::is_focused_browser_input(execution_name, args);
    let native_browser_select =
        bamboo_tools::permission::is_native_browser_select(execution_name, args);
    let browser_eval = execution_name.trim().eq_ignore_ascii_case("browser_eval");
    let private_browser_file_input =
        bamboo_tools::permission::is_private_browser_file_input(execution_name, args);
    let private_browser_download = browser_execution
        && args
            .get("action")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|action| action.eq_ignore_ascii_case("download"));
    let private_browser_approval = focused_browser_input
        || native_browser_select
        || browser_eval
        || private_browser_file_input
        || private_browser_download;
    let private_check_error = if browser_eval {
        "Browser page script permission check failed"
    } else if private_browser_download {
        "Browser download permission check failed"
    } else if private_browser_file_input {
        "Browser file input permission check failed"
    } else if native_browser_select {
        "Browser selection permission check failed"
    } else if focused_browser_input {
        "Focused browser input permission check failed"
    } else {
        "Browser permission check failed"
    };
    let permission_context = match bamboo_tools::permission::check_permissions(execution_name, args)
    {
        Ok(contexts) => contexts.and_then(|contexts| contexts.into_iter().next()),
        Err(_) if browser_execution || private_browser_approval => {
            return Some(ToolExecutionOutcome {
                output_cap: None,
                permission_replay_origin: None,
                result: Err(private_check_error.to_string()),
                needs_human: None,
                portable_tool: None,
                post_tool_hook_eligible: false,
                tool_duration: std::time::Duration::ZERO,
            });
        }
        Err(_) => None,
    };
    if private_browser_approval && permission_context.is_none() {
        return Some(ToolExecutionOutcome {
            output_cap: None,
            permission_replay_origin: None,
            result: Err(private_check_error.to_string()),
            needs_human: None,
            portable_tool: None,
            post_tool_hook_eligible: false,
            tool_duration: std::time::Duration::ZERO,
        });
    }
    // Read-only browser actions can legitimately have no permission context.
    // A hook Ask for those actions still reaches the parent, but arbitrary
    // extra fields must never become its fallback approval resource.
    let browser_without_context = browser_execution && permission_context.is_none();
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
                if browser_without_context {
                    "[redacted]".to_string()
                } else {
                    args.to_string()
                },
                format!("Hook-requested review for {tool_name}"),
                permission_type.risk_level(),
            )
        };

    let requested_mode = runtime_state.effective_permission_mode();
    let resolution = bamboo_domain::resolve_permission_mode(
        requested_mode,
        config.permission_mode.unwrap_or_default(),
    );
    let approval_resource = if private_browser_approval || browser_without_context {
        "[redacted]".to_string()
    } else {
        resource.clone()
    };
    // A namespace is model-supplied text. Keep the private download's outward
    // tool identity fixed as well as its resource and matcher.
    let approval_tool_name = if private_browser_download {
        "browser".to_string()
    } else {
        tool_name.clone()
    };
    let request = bamboo_tools::permission::PermissionRequest {
        request_id: tool_call.id.clone(),
        request_generation: bamboo_tools::permission::PermissionRequest::fresh_generation(),
        session_id: session.id.clone(),
        workspace_path: session.workspace_path_meta(),
        tool_name: approval_tool_name.clone(),
        permission_type,
        resource: approval_resource.clone(),
        operation_summary,
        risk_level,
        reason_code: bamboo_tools::permission::PermissionReasonCode::ConfiguredAlwaysAsk,
        effective_mode: resolution.effective,
        bypass_requested: resolution.bypass_permissions(),
        auto_approve_requested: requested_mode == bamboo_domain::SessionPermissionMode::Auto,
        policy_revision: 0,
        matched_rule: None,
        allowed_decisions: bamboo_tools::permission::PermissionRequest::forced_decisions(),
        suggested_matchers: if private_browser_approval || browser_without_context {
            Vec::new()
        } else {
            bamboo_tools::permission::conservative_matchers(permission_type, &resource)
        },
    };

    if let Some(proxy) = bamboo_tools::current_approval_proxy() {
        let approved = proxy
            .request_approval(bamboo_tools::ApprovalAsk {
                tool_name: approval_tool_name,
                permission: permission_type.description().to_string(),
                resource: approval_resource,
                permission_request: Some(request),
            })
            .await;
        return (!approved).then(|| ToolExecutionOutcome {
            output_cap: None,
            permission_replay_origin: None,
            result: Err("Tool execution denied by parent agent review".to_string()),
            needs_human: None,
            portable_tool: None,
            post_tool_hook_eligible: false,
            tool_duration: std::time::Duration::ZERO,
        });
    }

    Some(ToolExecutionOutcome {
        output_cap: None,
        permission_replay_origin: None,
        result: Err(
            "Hook requested approval, but no parent-agent reviewer is available; denied"
                .to_string(),
        ),
        needs_human: None,
        portable_tool: None,
        post_tool_hook_eligible: false,
        tool_duration: std::time::Duration::ZERO,
    })
}

fn append_post_tool_feedback(
    result: &mut Result<ToolResult, String>,
    feedback: Vec<String>,
    execution_name: Option<&str>,
    max_tool_output_tokens: u32,
) {
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

    if matches!(execution_name, Some("exec_command" | "write_stdin")) {
        if let Ok(result) = result {
            if let Ok(mut envelope) = serde_json::from_str::<serde_json::Value>(&result.result) {
                if let Some(object) = envelope.as_object_mut() {
                    // Native hooks have no aggregate context cap. Bound only
                    // this optional shell-result metadata before preserving it.
                    let mut remaining = bamboo_hooks::portable::PORTABLE_CONTEXT_BYTES;
                    let total = feedback.iter().map(String::len).sum::<usize>();
                    let mut bounded = Vec::new();
                    for mut text in feedback {
                        if remaining == 0 {
                            break;
                        }
                        let mut end = text.len().min(remaining);
                        while !text.is_char_boundary(end) {
                            end -= 1;
                        }
                        text.truncate(end);
                        remaining -= end;
                        if !text.is_empty() {
                            bounded.push(text);
                        }
                    }
                    let kept = bounded.iter().map(String::len).sum::<usize>();
                    object.insert("hook_feedback".into(), serde_json::json!(bounded));
                    if total > kept {
                        object.insert(
                            "hook_feedback_omitted_bytes".into(),
                            serde_json::json!(total - kept),
                        );
                    }
                    result.result = serde_json::to_string(&envelope)
                        .expect("shell feedback envelope is serializable");
                    if max_tool_output_tokens > 0 {
                        use bamboo_compression::TokenCounter;
                        let counter = bamboo_compression::TiktokenTokenCounter::default();
                        if counter.count_text(&result.result) > max_tool_output_tokens {
                            if let Some(truncated) =
                                super::output_compressor::truncate_exec_session_output(
                                    &result.result,
                                    max_tool_output_tokens,
                                    &counter,
                                )
                            {
                                result.result = truncated;
                            }
                        }
                    }
                    return;
                }
            }
        }
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
    let mut loop_result_checkpointed = false;
    if outcome.portable_tool.as_ref().is_some_and(|tool| {
        tool.execution_name() == bamboo_tools::tools::loop_control::LOOP_CONTROL_TOOL_NAME
    }) && outcome.result.as_ref().is_ok_and(|result| result.success)
        && outcome.needs_human.is_none()
    {
        let command = bamboo_tools::tools::loop_control::parse_loop_control_request(
            &ctx.tool_call.function.arguments,
        )
        .map_err(AgentError::Tool)?;
        crate::runtime::runner::state_bridge::write_runtime_state(ctx.session, ctx.runtime_state);
        let read_only = ctx.runtime_state.plan_mode.is_some()
            || ctx.config.permission_mode == Some(bamboo_config::PermissionMode::Plan);
        outcome.result = Ok(crate::runtime::loop_control::apply_current(
            ctx.session,
            ctx.config,
            &command,
            &ctx.tool_call.id,
            ctx.round,
            outcome.permission_replay_origin.is_some(),
            read_only,
        )
        .await?);
        loop_result_checkpointed = true;
    }
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
        let original_tool_input = serde_json::from_str(&ctx.tool_call.function.arguments).ok();
        let mut hook_outcome = ctx
            .config
            .hook_runner
            .run_hooks_with_inputs(
                AgentHookPoint::AfterToolExecution,
                &hook_payload,
                ctx.session,
                ctx.runtime_state,
                Some(ctx.event_tx),
                bamboo_hooks::portable::PortableInputs {
                    resolved_tool_name: outcome
                        .portable_tool
                        .as_ref()
                        .map(|tool| tool.name.as_str()),
                    original_tool_input: outcome
                        .portable_tool
                        .as_ref()
                        .map(|tool| &tool.input)
                        .or(original_tool_input.as_ref()),
                    ..Default::default()
                },
            )
            .await;
        crate::runtime::hooks::inject_plugin_contexts(
            ctx.session,
            std::mem::take(&mut hook_outcome.plugin_contexts),
        );
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
    // Read the shell envelope before post-hook feedback appends prose. Only
    // the admitted exact identity selects command-specific task accounting.
    let task_progress_result = task_progress_result_for_outcome(&outcome);
    let feedback_cap = outcome
        .output_cap
        .unwrap_or_else(|| {
            ctx.session
                .effective_token_budget()
                .map(|budget| budget.max_tool_output_tokens)
        })
        .unwrap_or(0);
    append_post_tool_feedback(
        &mut outcome.result,
        post_tool_feedback,
        outcome
            .portable_tool
            .as_ref()
            .map(|tool| tool.execution_name()),
        feedback_cap,
    );

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
        if let Some(progress_result) = task_progress_result.result(&display_result) {
            super::task::track_task_progress(
                ctx.task_context,
                ctx.event_tx,
                ctx.session_id,
                ctx.tool_call,
                progress_result,
                ctx.round,
            )
            .await;
        }
        super::clarification::suspend_for_pending_question(
            ctx.tool_call,
            outcome.permission_replay_origin.as_ref(),
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
                        permission_replay_origin: outcome.permission_replay_origin.as_ref(),
                        result: &result,
                        task_progress_result,
                        loop_result_checkpointed,
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
        let metadata = msg.metadata.get_or_insert_with(|| serde_json::json!({}));
        if let Some(object) = metadata.as_object_mut() {
            object.extend(
                metadata_value
                    .as_object()
                    .expect("lifecycle metadata object")
                    .clone(),
            );
        }
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
mod task_progress_tests {
    use super::*;
    use crate::runtime::task_context::{TaskLoopItem, ToolCallRecord};
    use bamboo_agent_core::tools::FunctionCall;
    use bamboo_domain::TaskItemStatus;
    use chrono::Utc;

    fn active_task(history: &[bool]) -> TaskLoopContext {
        let now = Utc::now();
        TaskLoopContext {
            session_id: "shell-progress".into(),
            items: vec![TaskLoopItem {
                id: "verify".into(),
                description: "Run verification".into(),
                status: TaskItemStatus::InProgress,
                tool_calls: history
                    .iter()
                    .map(|success| ToolCallRecord {
                        round: 0,
                        tool_name: "prior".into(),
                        success: *success,
                        timestamp: now,
                    })
                    .collect(),
                ..Default::default()
            }],
            active_item_id: Some("verify".into()),
            current_round: 0,
            max_rounds: None,
            created_at: now,
            updated_at: now,
            version: 0,
            task_list_dirty: false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_result(
        task: TaskLoopContext,
        requested_name: &str,
        execution_name: &str,
        status: &str,
        exit_code: Option<i32>,
        needs_human: bool,
        round: usize,
    ) -> TaskLoopContext {
        let call = ToolCall {
            id: format!("shell-progress-{round}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: requested_name.into(),
                arguments: "{}".into(),
            },
        };
        let result = ToolResult::text(
            true,
            serde_json::json!({"status":status,"output":"","exit_code":exit_code}).to_string(),
        );
        let expected_result = result.result.clone();
        let outcome = ToolExecutionOutcome {
            output_cap: None,
            portable_tool: Some(PortableToolExecution {
                name: execution_name.into(),
                input: serde_json::json!({}),
            }),
            permission_replay_origin: None,
            result: Ok(result),
            needs_human: needs_human.then(|| bamboo_agent_core::PendingQuestion {
                tool_call_id: call.id.clone(),
                tool_name: requested_name.into(),
                question: "Approve command?".into(),
                options: vec!["yes".into(), "no".into()],
                allow_custom: false,
                source: bamboo_agent_core::PendingQuestionSource::PauseTool,
            }),
            post_tool_hook_eligible: true,
            tool_duration: std::time::Duration::ZERO,
        };
        let mut session = Session::new("shell-progress", "model");
        let flags = ToolExecutionSessionFlags::from_session(&session);
        let tools: Arc<dyn ToolExecutor> = Arc::new(
            bamboo_tools::BuiltinToolExecutor::with_registry(bamboo_tools::ToolRegistry::new()),
        );
        let (event_tx, mut event_rx) = mpsc::channel(64);
        let mut context = Some(task);
        apply_tool_execution_outcome(
            ToolExecutionApplyContext {
                tool_call: &call,
                event_tx: &event_tx,
                metrics_collector: None,
                session_id: "shell-progress",
                round_id: "round",
                round,
                session: &mut session,
                tools: &tools,
                session_flags: flags,
                config: &AgentLoopConfig::default(),
                runtime_state: &mut AgentRuntimeState::new("shell-progress"),
                task_context: &mut context,
                state: &mut RoundExecutionState::default(),
            },
            outcome,
        )
        .await
        .unwrap();
        let completed = std::iter::from_fn(|| event_rx.try_recv().ok())
            .find_map(|event| match event {
                AgentEvent::ToolComplete { result, .. } => Some(result),
                _ => None,
            })
            .expect("tool completion remains visible");
        assert!(
            completed.success,
            "public transport success must be unchanged"
        );
        assert_eq!(completed.result, expected_result);
        context.unwrap()
    }

    #[tokio::test]
    async fn running_shell_polls_neither_complete_nor_block_active_tasks() {
        for execution_name in ["exec_command", "write_stdin"] {
            for history in [&[][..], &[true, true, true][..], &[false, false][..]] {
                let mut task = active_task(history);
                for round in 0..4 {
                    task = apply_result(
                        task,
                        &format!("default::{execution_name}"),
                        execution_name,
                        "running",
                        None,
                        false,
                        round,
                    )
                    .await;
                }
                assert_eq!(task.items[0].status, TaskItemStatus::InProgress);
                assert_eq!(task.items[0].tool_calls.len(), history.len());
                assert!(task.items[0].evidence.is_empty());
                assert_eq!(task.version, 0);
            }
        }
    }

    #[tokio::test]
    async fn terminal_shell_results_follow_existing_task_success_and_failure_rules() {
        for execution_name in ["exec_command", "write_stdin"] {
            for (status, exit_code, expected, successful) in [
                ("completed", Some(0), TaskItemStatus::Completed, true),
                ("completed", Some(7), TaskItemStatus::Blocked, false),
                ("error", None, TaskItemStatus::Blocked, false),
                ("killed", None, TaskItemStatus::Blocked, false),
            ] {
                let mut task = active_task(&[]);
                for round in 0..4 {
                    task = apply_result(
                        task,
                        execution_name,
                        execution_name,
                        status,
                        exit_code,
                        false,
                        round,
                    )
                    .await;
                }
                assert_eq!(task.items[0].status, expected, "{execution_name}:{status}");
                assert!(task.items[0]
                    .tool_calls
                    .iter()
                    .all(|record| record.success == successful));
                assert!(task.items[0]
                    .evidence
                    .iter()
                    .all(|evidence| evidence.success == Some(successful)));
            }
        }
    }

    async fn real_shell_permission_ack_is_neutral(cap: u32) {
        for (requested, execution_name) in [
            ("exec_command", "exec_command"),
            ("default::exec_command", "exec_command"),
            ("write_stdin", "write_stdin"),
            ("default::write_stdin", "write_stdin"),
        ] {
            let home = tempfile::tempdir().unwrap();
            let sentinel = home.path().join("must-not-execute");
            let permission = Arc::new(bamboo_tools::permission::PermissionConfig::new());
            permission.set_ask_rules(["exec_command".into(), "write_stdin".into()]);
            let checker = Arc::new(bamboo_tools::permission::ConfigPermissionChecker::new(
                permission,
            ));
            let tools: Arc<dyn ToolExecutor> = Arc::new(
                bamboo_tools::BuiltinToolExecutor::new_with_permissions(checker),
            );
            let schemas = tools.list_tools();
            let args = if execution_name == "exec_command" {
                serde_json::json!({"cmd":format!("printf forbidden > '{}'", sentinel.display())})
            } else {
                // The gate must pause before the actual tool rejects this unknown handle.
                serde_json::json!({"session_id":"00000000-0000-0000-0000-000000000000", "chars":""})
            };
            let call = ToolCall {
                id: format!("real-pending-{cap}-{requested}"),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: requested.into(),
                    arguments: args.to_string(),
                },
            };
            let mut session = Session::new("shell-progress", "model");
            session.token_budget = Some(bamboo_domain::TokenBudget {
                max_tool_output_tokens: cap,
                ..Default::default()
            });
            let flags = ToolExecutionSessionFlags::from_session(&session);
            assert!(!flags.bypass_permissions && !flags.auto_approve_permissions);
            let config = AgentLoopConfig::default();
            let mut runtime = AgentRuntimeState::new("shell-progress");
            let (event_tx, mut event_rx) = mpsc::channel(64);
            let outcome = bamboo_tools::with_approval_proxy(
                None,
                // Exercise the real gate for an explicitly selected executor;
                // model admission is covered by the strict-argument test.
                execute_tool_call_only_with_execution_name(
                    execution_name,
                    ToolExecutionOnlyContext {
                        vision_support: true,
                        output_cap: None,
                        executing_supervisor: None,
                        tool_call: &call,
                        event_tx: &event_tx,
                        metrics_collector: None,
                        session_id: "shell-progress",
                        root_orchestration_only: false,
                        root_session_id: "shell-progress",
                        round_id: "round",
                        round: 0,
                        tools: &tools,
                        config: &config,
                        hook_session: Some(&mut session),
                        hook_runtime_state: Some(&mut runtime),
                        session_flags: flags,
                        available_tool_schemas: &schemas,
                    },
                ),
            )
            .await
            .unwrap();
            assert!(
                outcome.needs_human.is_none(),
                "real gate uses the legacy Completed adapter"
            );
            let result = outcome.result.as_ref().unwrap();
            assert!(result.success);
            assert_eq!(
                result.display_preference.as_deref(),
                Some("request_permissions")
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&result.result).unwrap()["status"],
                "awaiting_permission_approval"
            );
            assert!(!sentinel.exists());
            assert!(std::iter::from_fn(|| event_rx.try_recv().ok())
                .any(|event| matches!(event, AgentEvent::ToolApprovalRequested { .. })));
            let outcome = super::super::output_compressor::maybe_compress(
                requested,
                &call.function.arguments,
                "shell-progress",
                outcome,
                cap,
                None,
                None,
            )
            .await;
            assert!(outcome.needs_human.is_none());
            assert_eq!(
                outcome.portable_tool.as_ref().unwrap().execution_name(),
                execution_name
            );
            assert_eq!(
                outcome
                    .result
                    .as_ref()
                    .unwrap()
                    .display_preference
                    .as_deref(),
                Some("request_permissions")
            );
            if cap == 1 {
                assert!(outcome
                    .result
                    .as_ref()
                    .unwrap()
                    .result
                    .contains("tool output truncated"));
            }
            assert!(matches!(shell_accounting_outcome(&outcome, Some(execution_name)), ShellAccountingOutcome::Pending),
                "{requested}, cap={cap}: an unexecuted permission acknowledgment is not command failure");

            let mut canonical_call = call.clone();
            canonical_call.function.name = execution_name.into();
            let prior_error = Err("prior command failure".into());
            let mut guard = policy::ToolPolicyGuard::new(80, 3);
            for _ in 0..2 {
                guard.observe_outcome(&canonical_call, &prior_error);
            }
            guard.observe_execution_outcome(&call, &outcome, Some(execution_name));
            assert!(
                guard
                    .check_before_execution_for_name(&call, 0, Some(execution_name))
                    .is_ok(),
                "approval must not add a third failure"
            );
            guard.observe_outcome(&canonical_call, &prior_error);
            assert!(
                matches!(
                    guard.check_before_execution_for_name(&call, 0, Some(execution_name)),
                    Err(policy::ToolPolicyPrecheckViolation::ToolCircuitOpen {
                        consecutive_failures: 3,
                        ..
                    })
                ),
                "approval must not reset the existing two failures"
            );

            let mut task = Some(active_task(&[false, false]));
            apply_tool_execution_outcome(
                ToolExecutionApplyContext {
                    tool_call: &call,
                    event_tx: &event_tx,
                    metrics_collector: None,
                    session_id: "shell-progress",
                    round_id: "round",
                    round: 0,
                    session: &mut session,
                    tools: &tools,
                    session_flags: flags,
                    config: &config,
                    runtime_state: &mut runtime,
                    task_context: &mut task,
                    state: &mut RoundExecutionState::default(),
                },
                outcome,
            )
            .await
            .unwrap();
            if cap == 0 {
                let question = session
                    .pending_question
                    .as_ref()
                    .expect("real gate must pause for approval");
                assert_eq!(question.tool_call_id, call.id);
                assert_eq!(question.tool_name, requested);
            }
            let task = task.unwrap();
            assert_eq!(task.items[0].status, TaskItemStatus::InProgress);
            assert_eq!(task.items[0].tool_calls.len(), 2);
            assert!(task.items[0]
                .tool_calls
                .iter()
                .all(|record| !record.success));
            assert!(task.items[0].evidence.is_empty());
            assert_eq!(task.version, 0);
            assert!(!sentinel.exists());
        }
    }

    #[tokio::test]
    async fn real_shell_permission_gate_completed_ack_does_not_record_command_outcome() {
        real_shell_permission_ack_is_neutral(0).await;
    }

    #[tokio::test]
    async fn compressed_real_shell_permission_ack_remains_neutral_without_json() {
        real_shell_permission_ack_is_neutral(1).await;
    }

    #[tokio::test]
    async fn pending_shell_approval_and_exact_foreign_tools_keep_their_own_semantics() {
        let task = apply_result(
            active_task(&[true, true, true]),
            "default::exec_command",
            "exec_command",
            "completed",
            Some(0),
            true,
            0,
        )
        .await;
        assert_eq!(task.items[0].status, TaskItemStatus::InProgress);
        assert_eq!(task.items[0].tool_calls.len(), 3);
        for name in ["vendor::exec_command", "vendor::write_stdin"] {
            let mut task = active_task(&[]);
            for round in 0..4 {
                task = apply_result(task, name, name, "completed", Some(7), false, round).await;
            }
            assert_eq!(task.items[0].status, TaskItemStatus::Completed);
            assert!(task.items[0].tool_calls.iter().all(|record| record.success));
        }
    }
}

#[cfg(test)]
mod hook_tests {
    use super::*;
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{
        AsyncWaitKind, FunctionCall, FunctionSchema, RunningCompletion, RunningHandle, ToolError,
    };
    use bamboo_agent_core::AgentHook;
    use bamboo_config::{LifecycleHookGroup, LifecycleHookHandler, LifecycleHooksConfig};
    use bamboo_domain::{CapabilityLoadingMode, ClassifiedToolSchema};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct BlockingPendingPersistence {
        saved: std::sync::Mutex<Vec<Session>>,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for BlockingPendingPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.saved
                .lock()
                .expect("pending persistence lock")
                .push(session.clone());
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

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

    #[derive(Default)]
    struct BrowserApprovalRecorder(Mutex<Vec<bamboo_tools::ApprovalAsk>>);

    #[async_trait]
    impl bamboo_tools::ApprovalProxy for BrowserApprovalRecorder {
        async fn request_approval(&self, ask: bamboo_tools::ApprovalAsk) -> bool {
            self.0
                .lock()
                .expect("browser approval record lock")
                .push(ask);
            false
        }
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

    struct NameRecordingExecutor {
        entered: Mutex<Vec<String>>,
        schemas: Vec<ToolSchema>,
    }

    impl NameRecordingExecutor {
        fn new(schema_names: &[&str]) -> Self {
            Self {
                entered: Mutex::new(Vec::new()),
                schemas: schema_names.iter().copied().map(tool_schema).collect(),
            }
        }

        fn entered(&self) -> Vec<String> {
            self.entered
                .lock()
                .expect("capability gate probe lock")
                .clone()
        }
    }

    #[async_trait]
    impl ToolExecutor for NameRecordingExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            self.entered
                .lock()
                .expect("capability gate probe lock")
                .push(call.function.name.clone());
            Ok(ToolResult::text(true, "executed"))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            self.schemas.clone()
        }
    }

    #[derive(Default)]
    struct ExactOwnerRecordingExecutor {
        ordinary_dispatches: Mutex<Vec<String>>,
        exact_dispatches: Mutex<Vec<(String, String)>>,
        output_caps: Mutex<Vec<Option<u32>>>,
    }

    #[async_trait]
    impl ToolExecutor for ExactOwnerRecordingExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            self.ordinary_dispatches
                .lock()
                .expect("ordinary dispatch probe lock")
                .push(call.function.name.clone());
            Ok(ToolResult::text(true, "ordinary dispatch"))
        }

        async fn execute_exact_with_context_outcome(
            &self,
            call: &ToolCall,
            execution_name: &str,
            ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolOutcome, ToolError> {
            let cap = bamboo_agent_core::tools::observed_tool_output_cap(&ctx.to_tool_ctx());
            self.output_caps.lock().unwrap().push(cap);
            tokio::task::yield_now().await;
            assert_eq!(
                bamboo_agent_core::tools::observed_tool_output_cap(&ctx.to_tool_ctx()),
                cap
            );
            self.exact_dispatches
                .lock()
                .expect("exact dispatch probe lock")
                .push((call.function.name.clone(), execution_name.to_string()));
            Ok(ToolOutcome::Completed(ToolResult::text(
                true,
                "exact dispatch",
            )))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            ["Edit", "apply_patch"]
                .into_iter()
                .map(tool_schema)
                .collect()
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
            ctx: bamboo_agent_core::tools::ToolExecutionContext<'_>,
        ) -> Result<ToolOutcome, ToolError> {
            assert_eq!(
                bamboo_agent_core::tools::observed_tool_output_cap(&ctx.to_tool_ctx()),
                Some(97)
            );
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

    struct ShellFeedbackHook(String);

    #[async_trait]
    impl AgentHook for ShellFeedbackHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterToolExecution
        }

        async fn run(&self, _: AgentHookPoint, _: &HookPayload, _: &Session) -> HookResult {
            HookResult::InjectContext {
                text: self.0.clone(),
            }
        }
    }

    #[tokio::test]
    async fn shell_post_hook_feedback_keeps_json_protocol_and_respects_output_caps() {
        use bamboo_compression::{TiktokenTokenCounter, TokenCounter};

        let feedback = "review 界🦀\\\"\n".repeat(1_000);
        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(ShellFeedbackHook(feedback.clone())));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let tools: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        for (requested, execution_name) in [
            ("exec_command", "exec_command"),
            ("default::write_stdin", "write_stdin"),
        ] {
            for status in ["running", "completed"] {
                for cap in [0, 1, 256] {
                    let call = probe_call(requested);
                    let mut payload = serde_json::json!({"status":status,"output":"captured output ".repeat(100)});
                    if status == "running" {
                        payload["session_id"] = "retained-handle".into();
                    } else {
                        payload["exit_code"] = 7.into();
                    }
                    let original_output = payload["output"].as_str().unwrap().to_owned();
                    let outcome = ToolExecutionOutcome {
                        output_cap: Some(Some(cap)),
                        portable_tool: Some(PortableToolExecution {
                            name: execution_name.into(),
                            input: serde_json::json!({}),
                        }),
                        permission_replay_origin: None,
                        result: Ok(ToolResult::text(true, payload.to_string())),
                        needs_human: None,
                        post_tool_hook_eligible: true,
                        tool_duration: std::time::Duration::ZERO,
                    };
                    let (event_tx, _event_rx) = mpsc::channel(64);
                    let mut session = Session::new("shell-feedback", "model");
                    apply_test_outcome(&config, &tools, &call, &mut session, &event_tx, outcome)
                        .await
                        .unwrap();
                    let message = session
                        .messages
                        .iter()
                        .find(|message| message.tool_call_id.as_deref() == Some(call.id.as_str()))
                        .unwrap();
                    assert_eq!(message.tool_success, Some(true));
                    let received: serde_json::Value =
                        serde_json::from_str(&message.content).unwrap();
                    assert_eq!(received["status"], status);
                    if status == "running" {
                        assert_eq!(received["session_id"], "retained-handle");
                    } else {
                        assert_eq!(received["exit_code"], 7);
                    }
                    let retained_feedback = received["hook_feedback"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|text| text.as_str().unwrap().len())
                        .sum::<usize>();
                    assert!(retained_feedback <= bamboo_hooks::portable::PORTABLE_CONTEXT_BYTES);
                    assert_eq!(
                        received["hook_feedback_omitted_bytes"].as_u64().unwrap() as usize
                            + retained_feedback,
                        feedback.trim().len(),
                    );
                    if cap > 0 {
                        assert!(received.get("hook_feedback").is_none());
                        let kept = received["output"].as_str().unwrap();
                        assert_eq!(
                            received["output_omitted_bytes"],
                            original_output.len() - kept.len()
                        );
                        if cap == 1 {
                            assert!(kept.is_empty());
                        } else {
                            assert!(
                                TiktokenTokenCounter::default().count_text(&message.content) <= cap
                            );
                        }
                    }
                }
            }
        }

        for name in ["Read", "vendor::exec_command", "vendor::write_stdin"] {
            let mut result = Ok(ToolResult::text(
                true,
                "{\"status\":\"running\",\"output\":\"\"}",
            ));
            append_post_tool_feedback(&mut result, vec!["ordinary feedback".into()], Some(name), 1);
            let content = result.unwrap().result;
            assert!(content.contains("<post_tool_use_feedback>"));
            assert!(serde_json::from_str::<serde_json::Value>(&content).is_err());
        }
    }

    fn tool_schema(name: &str) -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: name.to_string(),
                description: format!("{name} capability gate probe"),
                parameters: serde_json::json!({"type": "object"}),
            },
        }
    }

    fn effective_callable_set(
        catalog_names: &[&str],
        mode: CapabilityLoadingMode,
        loaded_names: &[&str],
    ) -> EffectiveCallableSet {
        let catalog = catalog_names
            .iter()
            .copied()
            .map(tool_schema)
            .filter_map(ClassifiedToolSchema::new)
            .collect::<Vec<_>>();
        EffectiveCallableSet::from_catalog(&catalog, mode, loaded_names.iter().copied())
    }

    struct OriginalToolNameRecorder(Mutex<Vec<String>>);
    #[async_trait]
    impl AgentHook for OriginalToolNameRecorder {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeToolExecution
        }
        async fn run(&self, _: AgentHookPoint, payload: &HookPayload, _: &Session) -> HookResult {
            if let HookPayload::ToolExecution { tool_name, .. } = payload {
                self.0.lock().unwrap().push(tool_name.clone());
            }
            HookResult::Continue
        }
    }

    async fn portable_alias_policy(
        pre_command: &str,
        canonical: &str,
    ) -> (tempfile::TempDir, AgentLoopConfig) {
        use bamboo_plugin::{
            InstalledPlugin, InstalledPlugins, PluginInstallStatus, PluginManifest, PluginSource,
            RegisteredCapabilities,
        };
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let bundle = root.join("alias-policy");
        std::fs::create_dir_all(&bundle).unwrap();
        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "id":"alias-policy", "name":"Alias policy", "version":"0.1.0",
            "provides":{"hooks":[{"config":"hooks.json","scripts":["policy.sh"]}]}
        }))
        .unwrap();
        std::fs::write(
            bundle.join("plugin.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(bundle.join("policy.sh"), "# reviewed fixture").unwrap();
        let post_command = r#"payload=$(cat); case "$payload" in *'"tool_name":"Bash"'*'"tool_response":"exact dispatch"'*) printf '%s' '{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"resolved Bash result"}}' ;; *) exit 1 ;; esac"#.replace("Bash", canonical);
        std::fs::write(bundle.join("hooks.json"), serde_json::to_vec(&serde_json::json!({"hooks":{
            "PreToolUse":[{"matcher":format!("^{canonical}$"),"hooks":[{"type":"command","command":pre_command,"timeout":1}]}],
            "PostToolUse":[{"matcher":format!("^{canonical}$"),"hooks":[{"type":"command","command":post_command,"timeout":1}]}]
        }})).unwrap()).unwrap();
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
                installed_at: chrono::Utc::now(),
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
        let runner = crate::runtime::hooks::HookRunner::new().with_lifecycle_config(
            &LifecycleHooksConfig::default(),
            Some(temp.path().to_owned()),
        );
        (
            temp,
            AgentLoopConfig {
                hook_runner: Arc::new(runner),
                ..Default::default()
            },
        )
    }

    async fn execute_portable_alias_call(
        config: &AgentLoopConfig,
        tools: &Arc<dyn ToolExecutor>,
        callable: &EffectiveCallableSet,
        call: &ToolCall,
        event_tx: &mpsc::Sender<AgentEvent>,
    ) -> (ToolExecutionOutcome, Session) {
        let mut session = Session::new("portable-alias", "model");
        let flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime = AgentRuntimeState::new(&session.id);
        let outcome = execute_model_requested_tool_call_only(
            callable,
            ToolExecutionOnlyContext {
                vision_support: true,
                output_cap: None,
                executing_supervisor: None,
                tool_call: call,
                event_tx,
                metrics_collector: None,
                session_id: "portable-alias",
                root_session_id: "portable-alias",
                root_orchestration_only: false,
                round_id: "round-1",
                round: 0,
                tools,
                config,
                hook_session: Some(&mut session),
                hook_runtime_state: Some(&mut runtime),
                session_flags: flags,
                available_tool_schemas: &[],
            },
        )
        .await
        .unwrap();
        (outcome, session)
    }

    #[tokio::test]
    async fn portable_hook_denies_resolved_aliases_but_preserves_exact_shadow_and_native_spelling()
    {
        // Legacy Bash policy still governs canonical shell calls; an exact
        // foreign tool with the same suffix keeps its own identity.
        let (_temp, mut config) = portable_alias_policy(r#"printf '%s' '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Bash blocked"}}'"#, "Bash").await;
        let recorder = Arc::new(OriginalToolNameRecorder(Mutex::new(vec![])));
        Arc::make_mut(&mut config.hook_runner).register(recorder.clone());
        let concrete = Arc::new(ExactOwnerRecordingExecutor::default());
        let tools: Arc<dyn ToolExecutor> = concrete.clone();
        let (event_tx, _event_rx) = mpsc::channel(64);
        let callable = effective_callable_set(
            &["exec_command"],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        for name in ["exec_command", "default::exec_command"] {
            let mut call = probe_call(name);
            call.function.arguments = serde_json::json!({"cmd":"printf fixture"}).to_string();
            assert_eq!(
                callable.resolve_callable_reference(name).as_deref(),
                Some("exec_command")
            );
            let (outcome, _) =
                execute_portable_alias_call(&config, &tools, &callable, &call, &event_tx).await;
            assert!(
                matches!(outcome.result, Err(ref error) if error.contains("Bash blocked")),
                "{name}"
            );
            assert!(!outcome.post_tool_hook_eligible);
        }
        assert!(concrete.exact_dispatches.lock().unwrap().is_empty());
        let shadow = effective_callable_set(
            &["exec_command", "vendor::exec_command"],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        let call = probe_call("vendor::exec_command");
        assert_eq!(
            shadow
                .resolve_callable_reference(&call.function.name)
                .as_deref(),
            Some("vendor::exec_command")
        );
        let (outcome, _) =
            execute_portable_alias_call(&config, &tools, &shadow, &call, &event_tx).await;
        assert!(outcome.result.is_ok());
        assert_eq!(
            *concrete.exact_dispatches.lock().unwrap(),
            vec![("vendor::exec_command".into(), "vendor::exec_command".into())]
        );
        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec![
                "exec_command",
                "default::exec_command",
                "vendor::exec_command"
            ]
        );
    }

    #[tokio::test]
    async fn portable_hook_post_uses_selected_identity_and_original_executor_input() {
        let (_temp, config) = portable_alias_policy("true", "Bash").await;
        let concrete = Arc::new(ExactOwnerRecordingExecutor::default());
        let tools: Arc<dyn ToolExecutor> = concrete;
        let (event_tx, _event_rx) = mpsc::channel(64);
        let callable = effective_callable_set(
            &["exec_command"],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        let mut call = probe_call("default::exec_command");
        let input = serde_json::json!({"cmd":"printf fixture"});
        call.function.arguments = input.to_string();
        let (outcome, mut session) =
            execute_portable_alias_call(&config, &tools, &callable, &call, &event_tx).await;
        assert!(outcome.result.is_ok());
        assert_eq!(outcome.portable_tool.as_ref().unwrap().name, "exec_command");
        assert_eq!(outcome.portable_tool.as_ref().unwrap().input, input);
        apply_test_outcome(&config, &tools, &call, &mut session, &event_tx, outcome)
            .await
            .unwrap();
        assert!(session
            .metadata
            .get("runtime.plugin_hook_contexts")
            .unwrap()
            .contains("resolved Bash result"));
    }

    #[tokio::test]
    async fn portable_hook_post_observes_repaired_non_strict_arguments() {
        let (_temp, config) = portable_alias_policy("true", "Read").await;
        let concrete = Arc::new(ExactOwnerRecordingExecutor::default());
        let tools: Arc<dyn ToolExecutor> = concrete;
        let (event_tx, _event_rx) = mpsc::channel(64);
        let callable =
            effective_callable_set(&["Read"], CapabilityLoadingMode::LegacyFullCatalog, &[]);
        let mut call = probe_call("Read");
        call.function.arguments = "{\"file_path\":\"/fixture\"".into();
        assert!(serde_json::from_str::<serde_json::Value>(&call.function.arguments).is_err());
        let (outcome, mut session) =
            execute_portable_alias_call(&config, &tools, &callable, &call, &event_tx).await;
        assert!(outcome.result.is_ok());
        assert_eq!(
            outcome.portable_tool.as_ref().unwrap().input,
            serde_json::json!({"file_path":"/fixture"})
        );
        apply_test_outcome(&config, &tools, &call, &mut session, &event_tx, outcome)
            .await
            .unwrap();
        assert!(session
            .metadata
            .get("runtime.plugin_hook_contexts")
            .unwrap()
            .contains("resolved Read result"));
    }

    async fn check_cap_live_portable_activation(sequential: bool, cap: Option<u32>, allow: bool) {
        let command = if allow {
            r#"printf '%s' '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow","additionalContext":"portable cap truth"}}'"#
        } else {
            r#"printf '%s' '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Bash blocked"}}'"#
        };
        let (temp, config) = portable_alias_policy(command, "Bash").await;
        let registry = temp.path().join("plugins/installed.json");
        let mut store = bamboo_plugin::InstalledPlugins::load(&registry)
            .await
            .unwrap();
        store.plugins[0].registered.hooks[0].enabled = false;
        store.save(&registry).await.unwrap();
        assert!(!config
            .hook_runner
            .has_hooks_for(AgentHookPoint::BeforeToolExecution));

        let concrete = Arc::new(ExactOwnerRecordingExecutor::default());
        let tools: Arc<dyn ToolExecutor> = concrete.clone();
        let callable = effective_callable_set(
            &["exec_command"],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        let mut call = probe_call("exec_command");
        call.function.arguments = serde_json::json!({"cmd":"printf fixture"}).to_string();
        let mut session = Session::new("live-hook-review", "model");
        session.token_budget = cap.map(|n| bamboo_domain::TokenBudget {
            max_tool_output_tokens: n,
            ..Default::default()
        });
        let flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime = AgentRuntimeState::new(&session.id);
        let (event_tx, mut event_rx) = mpsc::channel(1);
        // Hold ToolStart at a deterministic await after admission, without sleeps.
        event_tx
            .send(AgentEvent::Token {
                content: "barrier".into(),
            })
            .await
            .unwrap();
        let outcome = {
            let execution = execute_model_requested_tool_call_only(
                &callable,
                ToolExecutionOnlyContext {
                    vision_support: true,
                    output_cap: cap,
                    executing_supervisor: None,
                    tool_call: &call,
                    event_tx: &event_tx,
                    metrics_collector: None,
                    session_id: "live-hook-review",
                    root_session_id: "live-hook-review",
                    root_orchestration_only: false,
                    round_id: "round-1",
                    round: 0,
                    tools: &tools,
                    config: &config,
                    hook_session: sequential.then_some(&mut session),
                    hook_runtime_state: sequential.then_some(&mut runtime),
                    session_flags: flags,
                    available_tool_schemas: &[],
                },
            );
            tokio::pin!(execution);
            tokio::select! {
                biased;
                _ = &mut execution => panic!("ToolStart must be waiting on the barrier"),
                _ = std::future::ready(()) => {}
            }
            store.plugins[0].registered.hooks[0].enabled = true;
            store.save(&registry).await.unwrap();
            assert!(config
                .hook_runner
                .has_hooks_for(AgentHookPoint::BeforeToolExecution));
            assert!(matches!(
                event_rx.recv().await.unwrap(),
                AgentEvent::Token { .. }
            ));
            let outcome = loop {
                tokio::select! {
                    result = &mut execution => break result.unwrap(),
                    event = event_rx.recv() => { assert!(event.is_some()); }
                }
            };
            outcome
        };
        if sequential && allow {
            assert_eq!(outcome.output_cap, Some(cap));
            assert_eq!(outcome.result.as_ref().unwrap().result, "exact dispatch");
            assert_eq!(outcome.portable_tool.as_ref().unwrap().name, "exec_command");
            assert_eq!(*concrete.output_caps.lock().unwrap(), vec![cap]);
            assert!(session.metadata["runtime.plugin_hook_contexts"].contains("portable cap truth"));
            assert_eq!(
                session
                    .effective_token_budget()
                    .map(|b| b.max_tool_output_tokens),
                cap
            );
            assert!(session.resolved_token_budget.is_none());
            return;
        }
        assert_eq!(outcome.output_cap, None);
        assert!(concrete.output_caps.lock().unwrap().is_empty());
        let expected = if sequential {
            "Bash blocked"
        } else {
            "hooks became active after parallel admission"
        };
        assert!(matches!(outcome.result, Err(ref error) if error.contains(expected)));
        assert!(!outcome.post_tool_hook_eligible);
        assert!(outcome.portable_tool.is_none());
        assert!(concrete.exact_dispatches.lock().unwrap().is_empty());
        let (reply_tx, _reply_rx) = mpsc::channel(64);
        apply_test_outcome(&config, &tools, &call, &mut session, &reply_tx, outcome)
            .await
            .unwrap();
        let reply = session.messages.last().unwrap();
        assert_eq!(reply.tool_call_id.as_deref(), Some(call.id.as_str()));
        assert_eq!(reply.tool_success, Some(false));
        assert!(reply.content.contains(expected));
        if allow {
            // A real fresh sequential retry captures the changed local value;
            // the blocked parallel call is never silently rescheduled.
            let current = Some(cap.unwrap_or(0) + 97);
            session.token_budget = current.map(|n| bamboo_domain::TokenBudget {
                max_tool_output_tokens: n,
                ..Default::default()
            });
            let retry = execute_cap_hook_call(&mut session, &tools, &config, &call)
                .await
                .unwrap();
            assert_eq!(retry.output_cap, Some(current));
            assert_eq!(*concrete.output_caps.lock().unwrap(), vec![current]);
            assert_eq!(retry.result.unwrap().result, "exact dispatch");
        }
    }

    async fn check_live_portable_activation(sequential: bool) {
        check_cap_live_portable_activation(sequential, None, false).await;
    }

    #[tokio::test]
    async fn scoped_cap_late_portable_activation_and_fresh_retry_observe_actual_values() {
        for cap in [None, Some(0), Some(53)] {
            for sequential in [false, true] {
                for allow in [false, true] {
                    check_cap_live_portable_activation(sequential, cap, allow).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn portable_hook_activation_after_parallel_admission_fails_closed() {
        check_live_portable_activation(false).await;
    }

    #[tokio::test]
    async fn portable_hook_activation_during_sequential_tool_start_enforces_policy() {
        check_live_portable_activation(true).await;
    }

    async fn execute_without_hooks(
        effective_callable_set: &EffectiveCallableSet,
        tools: &Arc<dyn ToolExecutor>,
        tool_call: &ToolCall,
        event_tx: &mpsc::Sender<AgentEvent>,
    ) -> ToolExecutionOutcome {
        execute_without_hooks_with_root_authority(
            effective_callable_set,
            tools,
            tool_call,
            event_tx,
            false,
        )
        .await
    }

    async fn execute_without_hooks_with_root_authority(
        effective_callable_set: &EffectiveCallableSet,
        tools: &Arc<dyn ToolExecutor>,
        tool_call: &ToolCall,
        event_tx: &mpsc::Sender<AgentEvent>,
        root_orchestration_only: bool,
    ) -> ToolExecutionOutcome {
        let session = Session::new("capability-gate-session", "model");
        execute_model_requested_tool_call_only(
            effective_callable_set,
            ToolExecutionOnlyContext {
                vision_support: true,
                output_cap: None,
                executing_supervisor: None,
                tool_call,
                event_tx,
                metrics_collector: None,
                session_id: "capability-gate-session",
                root_orchestration_only,
                root_session_id: "capability-gate-session",
                round_id: "round-1",
                round: 0,
                tools,
                config: &AgentLoopConfig::default(),
                hook_session: None,
                hook_runtime_state: None,
                session_flags: ToolExecutionSessionFlags::from_session(&session),
                available_tool_schemas: &[],
            },
        )
        .await
        .expect("capability admission is a per-call outcome")
    }

    #[tokio::test]
    async fn capability_gate_allows_legacy_deferred_and_progressive_core_or_loaded_calls() {
        let concrete_tools = Arc::new(NameRecordingExecutor::new(&["Read", "probe"]));
        let tools: Arc<dyn ToolExecutor> = concrete_tools.clone();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let legacy = effective_callable_set(
            &["Read", "probe"],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        let progressive = effective_callable_set(
            &["Read", "probe"],
            CapabilityLoadingMode::Progressive,
            &["probe"],
        );

        let legacy_deferred =
            execute_without_hooks(&legacy, &tools, &probe_call("probe"), &event_tx).await;
        let progressive_core =
            execute_without_hooks(&progressive, &tools, &probe_call("Read"), &event_tx).await;
        let progressive_loaded =
            execute_without_hooks(&progressive, &tools, &probe_call("probe"), &event_tx).await;

        assert!(legacy_deferred.result.is_ok());
        assert!(progressive_core.result.is_ok());
        assert!(progressive_loaded.result.is_ok());
        assert_eq!(concrete_tools.entered(), ["probe", "Read", "probe"]);
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::ToolStart { .. }))
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn admitted_shells_reject_repairable_json_before_tool_start_or_side_effects() {
        let temp = tempfile::tempdir().unwrap();
        let sentinel = temp.path().join("must-not-exist");
        let tools: Arc<dyn ToolExecutor> = Arc::new(bamboo_tools::BuiltinToolExecutor::new());
        let callable = effective_callable_set(
            &["exec_command", "write_stdin"],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        let (event_tx, mut event_rx) = mpsc::channel(32);
        for (name, args) in [
            (
                "exec_command",
                serde_json::json!({"cmd":format!("touch '{}'", sentinel.display())}),
            ),
            (
                "default::exec_command",
                serde_json::json!({"cmd":format!("touch '{}'", sentinel.display())}),
            ),
            (
                "write_stdin",
                serde_json::json!({"session_id":"not-owned", "chars":"yes\n"}),
            ),
            (
                "default::write_stdin",
                serde_json::json!({"session_id":"not-owned", "eof":true}),
            ),
        ] {
            let mut call = probe_call(name);
            call.function.arguments = args.to_string();
            call.function.arguments.pop();
            assert!(parse_tool_args_best_effort(&call.function.arguments)
                .1
                .is_some());
            let outcome = execute_without_hooks(&callable, &tools, &call, &event_tx).await;
            assert!(
                matches!(outcome.result, Err(ref error) if error.contains("invalid JSON arguments"))
            );
            assert!(!outcome.post_tool_hook_eligible);
            assert!(!sentinel.exists());
        }
        assert!(std::iter::from_fn(|| event_rx.try_recv().ok())
            .all(|event| !matches!(event, AgentEvent::ToolStart { .. })));

        let concrete = Arc::new(NameRecordingExecutor::new(&[
            "vendor::exec_command",
            "vendor::write_stdin",
        ]));
        let tools: Arc<dyn ToolExecutor> = concrete.clone();
        let callable = effective_callable_set(
            &["vendor::exec_command", "vendor::write_stdin"],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        for name in ["vendor::exec_command", "vendor::write_stdin"] {
            let mut call = probe_call(name);
            call.function.arguments = "{\"ordinary\":true".into();
            assert!(execute_without_hooks(&callable, &tools, &call, &event_tx)
                .await
                .result
                .is_ok());
        }
        assert_eq!(
            concrete.entered(),
            ["vendor::exec_command", "vendor::write_stdin"]
        );
    }

    #[tokio::test]
    async fn supervisor_delegation_first_final_gate_rejects_stale_execution_schemas_in_both_modes()
    {
        use bamboo_domain::{SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};
        let names = [
            "Read",
            "SubAgent",
            "Plan",
            "Task",
            "session_control",
            "session_history",
            "loop_control",
            "work_dispatch",
            "exec_command",
            "write_stdin",
            "Edit",
            "load_skill",
            "vendor::Read",
            "default::Read",
        ];
        let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
        supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: uuid::Uuid::new_v4(),
        };
        // The supplied catalog deliberately predates any restriction.
        let callable =
            effective_callable_set(&names, CapabilityLoadingMode::LegacyFullCatalog, &[]);
        for ultra in [false, true] {
            supervisor.set_root_orchestration_only(ultra).unwrap();
            let observation =
                ExecutingSupervisorObservation::capture_from_executing_session(&supervisor);
            let concrete = Arc::new(NameRecordingExecutor::new(&names));
            let tools: Arc<dyn ToolExecutor> = concrete.clone();
            let (tx, mut rx) = mpsc::channel(128);
            for name in names {
                let call = probe_call(name);
                let outcome = execute_model_requested_tool_call_only(
                    &callable,
                    ToolExecutionOnlyContext {
                        vision_support: true,
                        output_cap: None,
                        executing_supervisor: observation,
                        tool_call: &call,
                        event_tx: &tx,
                        metrics_collector: None,
                        session_id: &supervisor.id,
                        root_session_id: &supervisor.root_session_id,
                        root_orchestration_only: ultra,
                        round_id: "delegation-first",
                        round: 0,
                        tools: &tools,
                        config: &AgentLoopConfig::default(),
                        hook_session: None,
                        hook_runtime_state: None,
                        session_flags: ToolExecutionSessionFlags::from_session(&supervisor),
                        available_tool_schemas: &[],
                    },
                )
                .await
                .unwrap();
                let allowed = matches!(
                    name,
                    "Read"
                        | "SubAgent"
                        | "Plan"
                        | "Task"
                        | "session_control"
                        | "session_history"
                        | "work_dispatch"
                );
                assert_eq!(outcome.result.is_ok(), allowed, "ultra={ultra}: {name}");
                if !allowed {
                    assert!(!outcome.post_tool_hook_eligible);
                    assert!(outcome.portable_tool.is_none());
                }
            }
            let expected = vec![
                "Read",
                "SubAgent",
                "Plan",
                "Task",
                "session_control",
                "session_history",
                "work_dispatch",
            ];
            assert_eq!(concrete.entered(), expected);
            assert_eq!(
                std::iter::from_fn(|| rx.try_recv().ok())
                    .filter(|event| matches!(event, AgentEvent::ToolStart { .. }))
                    .count(),
                expected.len()
            );
        }
    }

    #[tokio::test]
    async fn supervisor_ultra_final_gate_uses_original_observation_and_exact_management_tool() {
        use bamboo_domain::{SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};
        let names = [
            "session_control",
            "session_history",
            "exec_command",
            "vendor::session_control",
            "default::session_control",
        ];
        let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
        supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: uuid::Uuid::new_v4(),
        };
        supervisor.set_root_orchestration_only(true).unwrap();
        let observed = ExecutingSupervisorObservation::capture_from_executing_session(&supervisor);
        let callable =
            effective_callable_set(&names, CapabilityLoadingMode::LegacyFullCatalog, &[]);
        for (observation, caller) in [
            (observed, DEFAULT_SUPERVISOR_SESSION_ID),
            (None, DEFAULT_SUPERVISOR_SESSION_ID),
            (observed, "ordinary-root"),
        ] {
            let concrete = Arc::new(NameRecordingExecutor::new(&names));
            let tools: Arc<dyn ToolExecutor> = concrete.clone();
            let (tx, mut rx) = mpsc::channel(64);
            for name in names {
                let call = probe_call(name);
                let outcome = execute_model_requested_tool_call_only(
                    &callable,
                    ToolExecutionOnlyContext {
                        vision_support: true,
                        output_cap: None,
                        executing_supervisor: observation,
                        tool_call: &call,
                        event_tx: &tx,
                        metrics_collector: None,
                        session_id: caller,
                        root_session_id: caller,
                        root_orchestration_only: true,
                        round_id: "round-1",
                        round: 0,
                        tools: &tools,
                        config: &AgentLoopConfig::default(),
                        hook_session: None,
                        hook_runtime_state: None,
                        session_flags: ToolExecutionSessionFlags::from_session(&supervisor),
                        available_tool_schemas: &[],
                    },
                )
                .await
                .unwrap();
                let allowed = observation.is_some()
                    && caller == DEFAULT_SUPERVISOR_SESSION_ID
                    && matches!(name, "session_control" | "session_history");
                assert_eq!(outcome.result.is_ok(), allowed, "{caller}: {name}");
            }
            let expected = if observation.is_some() && caller == DEFAULT_SUPERVISOR_SESSION_ID {
                vec!["session_control", "session_history"]
            } else {
                vec![]
            };
            assert_eq!(concrete.entered(), expected);
            assert_eq!(
                std::iter::from_fn(|| rx.try_recv().ok())
                    .filter(|event| matches!(event, AgentEvent::ToolStart { .. }))
                    .count(),
                expected.len()
            );
        }
    }

    #[tokio::test]
    async fn selected_root_final_gate_rejects_stale_schema_and_exact_alias_before_tool_start() {
        let concrete_tools = Arc::new(NameRecordingExecutor::new(&[
            "Read",
            "exec_command",
            "write_stdin",
            "Edit",
            "apply_patch",
            "default::Read",
        ]));
        let tools: Arc<dyn ToolExecutor> = concrete_tools.clone();
        // Deliberately stale: the loaded callable set still contains names that
        // the live Root authority now denies.
        let stale = effective_callable_set(
            &[
                "Read",
                "exec_command",
                "write_stdin",
                "Edit",
                "apply_patch",
                "default::Read",
            ],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        let (event_tx, mut event_rx) = mpsc::channel(32);
        for name in [
            "exec_command",
            "default::exec_command",
            "write_stdin",
            "Edit",
            "apply_patch",
            "default::Read",
        ] {
            let outcome = execute_without_hooks_with_root_authority(
                &stale,
                &tools,
                &probe_call(name),
                &event_tx,
                true,
            )
            .await;
            assert!(matches!(
                outcome.result,
                Err(ref error) if error.contains("outside orchestration-only Root authority")
            ));
            assert!(!outcome.post_tool_hook_eligible);
        }
        let allowed = execute_without_hooks_with_root_authority(
            &stale,
            &tools,
            &probe_call("read_file"),
            &event_tx,
            true,
        )
        .await;
        assert!(allowed.result.is_ok());
        assert_eq!(concrete_tools.entered(), ["Read"]);
        let starts = std::iter::from_fn(|| event_rx.try_recv().ok())
            .filter(|event| matches!(event, AgentEvent::ToolStart { .. }))
            .count();
        assert_eq!(starts, 1);
    }

    #[tokio::test]
    async fn malformed_namespaced_browser_call_starts_with_fixed_display_name() {
        let concrete_tools = Arc::new(NameRecordingExecutor::new(&["browser"]));
        let tools: Arc<dyn ToolExecutor> = concrete_tools;
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let callable =
            effective_callable_set(&["browser"], CapabilityLoadingMode::LegacyFullCatalog, &[]);
        let mut call = probe_call("private-selector::browser");
        call.function.arguments = "{malformed".to_string();
        let _ = execute_without_hooks(&callable, &tools, &call, &event_tx).await;
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolStart { tool_name, arguments, .. }
                if tool_name == "browser" && !arguments.to_string().contains("private-selector")
        )));
        assert_eq!(call.function.name, "private-selector::browser");
    }

    #[tokio::test]
    async fn capability_gate_rejects_unloaded_host_only_unknown_and_excluded_before_hooks() {
        let concrete_tools = Arc::new(NameRecordingExecutor::new(&[
            "Read",
            "probe",
            "Workspace",
            "excluded",
        ]));
        let tools: Arc<dyn ToolExecutor> = concrete_tools.clone();
        let effective_callable_set = effective_callable_set(
            &["Read", "probe", "Workspace"],
            CapabilityLoadingMode::Progressive,
            &[],
        );
        let (event_tx, mut event_rx) = mpsc::channel(16);

        let mut runner = crate::runtime::hooks::HookRunner::new();
        runner.register(Arc::new(DenyToolHook));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(runner),
            ..Default::default()
        };
        let unloaded_call = probe_call("probe");
        let mut session = Session::new("capability-gate-hook-session", "model");
        let session_flags = ToolExecutionSessionFlags::from_session(&session);
        let mut runtime_state = AgentRuntimeState::new(&session.id);
        let unloaded = execute_model_requested_tool_call_only(
            &effective_callable_set,
            ToolExecutionOnlyContext {
                vision_support: true,
                output_cap: None,
                executing_supervisor: None,
                tool_call: &unloaded_call,
                event_tx: &event_tx,
                metrics_collector: None,
                session_id: "capability-gate-hook-session",
                root_orchestration_only: false,
                root_session_id: "capability-gate-hook-session",
                round_id: "round-1",
                round: 0,
                tools: &tools,
                config: &config,
                hook_session: Some(&mut session),
                hook_runtime_state: Some(&mut runtime_state),
                session_flags,
                available_tool_schemas: &[],
            },
        )
        .await
        .expect("unloaded rejection is a per-call outcome");

        let mut outcomes = vec![("probe", unloaded)];
        for name in ["Workspace", "unknown_tool", "excluded"] {
            outcomes.push((
                name,
                execute_without_hooks(
                    &effective_callable_set,
                    &tools,
                    &probe_call(name),
                    &event_tx,
                )
                .await,
            ));
        }

        for (name, outcome) in outcomes {
            assert!(matches!(
                outcome.result,
                Err(ref error)
                    if error.contains(name)
                        && error.contains("not callable at the current conversation position")
            ));
        }
        assert!(concrete_tools.entered().is_empty());
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(events.iter().all(|event| !matches!(
            event,
            AgentEvent::ToolStart { .. } | AgentEvent::HookLifecycle { .. }
        )));
    }

    #[tokio::test]
    async fn capability_gate_preserves_exact_shadow_without_alias_fallback() {
        let concrete_tools = Arc::new(NameRecordingExecutor::new(&["Edit", "apply_patch"]));
        let tools: Arc<dyn ToolExecutor> = concrete_tools.clone();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let core_only = effective_callable_set(
            &["Edit", "apply_patch"],
            CapabilityLoadingMode::Progressive,
            &[],
        );
        let exact_loaded = effective_callable_set(
            &["Edit", "apply_patch"],
            CapabilityLoadingMode::Progressive,
            &["apply_patch"],
        );
        let exact_call = probe_call("apply_patch");

        let rejected = execute_without_hooks(&core_only, &tools, &exact_call, &event_tx).await;
        let admitted = execute_without_hooks(&exact_loaded, &tools, &exact_call, &event_tx).await;

        assert!(matches!(
            rejected.result,
            Err(ref error) if error.contains("not callable at the current conversation position")
        ));
        assert!(admitted.result.is_ok());
        assert_eq!(concrete_tools.entered(), ["apply_patch"]);
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::ToolStart { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn capability_gate_dispatches_the_resolved_owner_without_reresolving_original_shadow() {
        let concrete_tools = Arc::new(ExactOwnerRecordingExecutor::default());
        let tools: Arc<dyn ToolExecutor> = concrete_tools.clone();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let effective_callable_set =
            effective_callable_set(&["Edit"], CapabilityLoadingMode::LegacyFullCatalog, &[]);
        let call = probe_call("apply_patch");
        assert_eq!(
            effective_callable_set
                .resolve_callable_reference(&call.function.name)
                .as_deref(),
            Some("Edit"),
            "with no exact registration in the eligible catalog, the shared resolver selects the allowed builtin owner"
        );

        let outcome =
            execute_without_hooks(&effective_callable_set, &tools, &call, &event_tx).await;

        assert!(outcome.result.is_ok());
        assert!(concrete_tools
            .ordinary_dispatches
            .lock()
            .expect("ordinary dispatch probe lock")
            .is_empty());
        assert_eq!(
            *concrete_tools
                .exact_dispatches
                .lock()
                .expect("exact dispatch probe lock"),
            vec![("apply_patch".to_string(), "Edit".to_string())]
        );
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolStart { tool_name, .. } if tool_name == "apply_patch"
        )));
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
    async fn needs_human_uses_runner_identity_and_persists_before_clarification_event() {
        let persistence = Arc::new(BlockingPendingPersistence::default());
        let config = AgentLoopConfig {
            persistence: Some(persistence.clone()),
            ..Default::default()
        };
        let tools: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor(AtomicBool::new(false)));
        let tool_call = probe_call("canonical-tool");
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let outcome = ToolExecutionOutcome {
            output_cap: None,
            permission_replay_origin: None,
            result: Ok(ToolResult::text(false, "decision pending")),
            needs_human: Some(bamboo_agent_core::PendingQuestion {
                tool_call_id: "stale-call".to_string(),
                tool_name: "stale-tool".to_string(),
                question: "Choose?".to_string(),
                options: vec!["yes".to_string(), "no".to_string()],
                allow_custom: false,
                source: bamboo_agent_core::PendingQuestionSource::PauseTool,
            }),
            portable_tool: None,
            post_tool_hook_eligible: false,
            tool_duration: std::time::Duration::from_millis(1),
        };

        let apply = tokio::spawn(async move {
            let mut session = Session::new("canonical-pending", "model");
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
            session
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            persistence.entered.notified(),
        )
        .await
        .expect("pending-question persistence should begin");
        let events_before_save: Vec<_> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events_before_save
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolComplete { .. })));
        assert!(!events_before_save
            .iter()
            .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));

        let saved = persistence
            .saved
            .lock()
            .expect("pending persistence lock")
            .last()
            .cloned()
            .expect("pending session snapshot");
        let persisted_question = saved.pending_question.expect("persisted pending question");
        assert_eq!(persisted_question.tool_call_id, "call-canonical-tool");
        assert_eq!(persisted_question.tool_name, "canonical-tool");

        persistence.release.notify_one();
        let session = apply.await.unwrap();
        let pending = session.pending_question.expect("runtime pending question");
        assert_eq!(pending.tool_call_id, "call-canonical-tool");
        assert_eq!(pending.tool_name, "canonical-tool");

        let clarification =
            tokio::time::timeout(std::time::Duration::from_secs(5), event_rx.recv())
                .await
                .expect("clarification event should arrive after persistence")
                .expect("clarification channel should stay open");
        assert!(matches!(
            clarification,
            AgentEvent::NeedClarification {
                tool_call_id: Some(ref id),
                tool_name: Some(ref name),
                ..
            } if id == "call-canonical-tool" && name == "canonical-tool"
        ));
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
            vision_support: true,
            output_cap: None,
            executing_supervisor: None,
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "hook-deny-session",
            root_orchestration_only: false,
            root_session_id: "hook-deny-session",
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
            vision_support: true,
            output_cap: None,
            executing_supervisor: None,
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "configured-hook-deny",
            root_orchestration_only: false,
            root_session_id: "configured-hook-deny",
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
            vision_support: true,
            output_cap: None,
            executing_supervisor: None,
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "hook-allow-engine",
            root_orchestration_only: false,
            root_session_id: "hook-allow-engine",
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
                vision_support: true,
                output_cap: None,
                executing_supervisor: None,
                tool_call: &tool_call,
                event_tx: &event_tx,
                metrics_collector: None,
                session_id: "hook-ask-session",
                root_orchestration_only: false,
                root_session_id: "hook-ask-session",
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
    async fn hook_ask_hides_focused_browser_resource_and_fails_closed_on_check_error() {
        let config = AgentLoopConfig::default();
        let session = Session::new("browser-hook-ask", "model");
        let runtime_state = AgentRuntimeState::new(&session.id);
        let reviewer = Arc::new(BrowserApprovalRecorder::default());
        let reviewer_proxy: Arc<dyn bamboo_tools::ApprovalProxy> = reviewer.clone();
        let key_args = serde_json::json!({
            "action":"key",
            "key":"private-key",
            "expected_epoch":17,
        });
        let key_call = ToolCall {
            id: "focused-key".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "browser".to_string(),
                arguments: key_args.to_string(),
            },
        };
        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy.clone()),
            hook_ask_outcome(
                &key_call,
                "browser",
                &config,
                &session,
                &runtime_state,
                &key_args,
            ),
        )
        .await
        .expect("denied parent review returns a tool outcome");
        assert!(outcome.result.is_err());
        let asks = reviewer.0.lock().expect("browser approval record lock");
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].resource, "[redacted]");
        let request = asks[0]
            .permission_request
            .as_ref()
            .expect("typed review request");
        assert_eq!(request.resource, "[redacted]");
        assert!(request.suggested_matchers.is_empty());
        assert!(!format!("{:?}", asks[0]).contains("private-key"));
        drop(asks);

        let eval_args = serde_json::json!({
            "code":"document.title = 'private-source'",
            "expected_url":"https://example.com/?token=private-query",
            "expected_epoch":17,
        });
        let eval_call = ToolCall {
            id: "browser-eval".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "browser_eval".to_string(),
                arguments: eval_args.to_string(),
            },
        };
        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy.clone()),
            hook_ask_outcome(
                &eval_call,
                "browser_eval",
                &config,
                &session,
                &runtime_state,
                &eval_args,
            ),
        )
        .await
        .expect("denied eval review returns a tool outcome");
        assert!(outcome.result.is_err());
        let asks = reviewer.0.lock().expect("browser approval record lock");
        assert_eq!(asks.len(), 2);
        assert_eq!(asks[1].resource, "[redacted]");
        let eval_request = asks[1].permission_request.as_ref().unwrap();
        assert_eq!(eval_request.resource, "[redacted]");
        assert!(eval_request.suggested_matchers.is_empty());
        for secret in ["private-source", "private-query", "browser_eval:17:"] {
            assert!(!format!("{:?}", asks[1]).contains(secret));
        }
        drop(asks);

        let invalid_type_args = serde_json::json!({
            "action":"type",
            "text":"private browser input",
            "expected_epoch":"invalid",
        });
        let invalid_type_call = ToolCall {
            id: "focused-type-invalid".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "browser".to_string(),
                arguments: invalid_type_args.to_string(),
            },
        };
        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy.clone()),
            hook_ask_outcome(
                &invalid_type_call,
                "browser",
                &config,
                &session,
                &runtime_state,
                &invalid_type_args,
            ),
        )
        .await
        .expect("invalid focused request fails closed");
        assert!(
            matches!(outcome.result, Err(ref error) if error == "Focused browser input permission check failed")
        );
        assert_eq!(
            reviewer
                .0
                .lock()
                .expect("browser approval record lock")
                .len(),
            2
        );

        // A namespaced model call resolves to canonical `browser` before hook
        // Ask. Even malformed actions with private-looking fields must not
        // fall back to a raw-arguments approval resource.
        let execution_name = "browser";
        for malformed_args in [
            serde_json::json!({
                "action":"select_options",
                "selector":"select[data-private='account']",
                "values":["private-option-value"],
            }),
            serde_json::json!({
                "action":null,
                "selector":"select[data-private='account']",
                "values":["private-option-value"],
            }),
        ] {
            let malformed_call = ToolCall {
                id: "select-hook-ask-malformed".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "default::browser".to_string(),
                    arguments: malformed_args.to_string(),
                },
            };
            let outcome = bamboo_tools::with_approval_proxy(
                Some(reviewer_proxy.clone()),
                hook_ask_outcome(
                    &malformed_call,
                    &execution_name,
                    &config,
                    &session,
                    &runtime_state,
                    &malformed_args,
                ),
            )
            .await
            .expect("malformed browser action fails closed");
            assert!(matches!(
                outcome.result,
                Err(ref error) if error == "Browser permission check failed"
            ));
            assert_eq!(
                reviewer
                    .0
                    .lock()
                    .expect("browser approval record lock")
                    .len(),
                2
            );
        }

        let read_only_args = serde_json::json!({
            "action":"snapshot",
            "selector":"select[data-private='account']",
            "values":["private-option-value"],
        });
        let read_only_call = ToolCall {
            id: "browser-hook-ask-read-only".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "default::browser".to_string(),
                arguments: read_only_args.to_string(),
            },
        };
        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy),
            hook_ask_outcome(
                &read_only_call,
                &execution_name,
                &config,
                &session,
                &runtime_state,
                &read_only_args,
            ),
        )
        .await
        .expect("read-only browser hook Ask reaches parent");
        assert!(outcome.result.is_err());
        let asks = reviewer.0.lock().expect("browser approval record lock");
        assert_eq!(asks.len(), 3);
        assert_eq!(asks[2].resource, "[redacted]");
        let request = asks[2]
            .permission_request
            .as_ref()
            .expect("typed read-only review request");
        assert_eq!(request.resource, "[redacted]");
        assert!(request.suggested_matchers.is_empty());
        for private in ["private-option-value", "data-private"] {
            assert!(!format!("{:?}", asks[2]).contains(private));
        }
    }

    #[test]
    fn namespaced_browser_select_hook_ask_redacts_parent_request() {
        let data_dir = tempfile::tempdir().expect("isolated Bamboo data dir");
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .arg("namespaced_browser_select_hook_ask_child")
            .env("BAMBOO_DATA_DIR", data_dir.path())
            .env_remove("BAMBOO_CONFIG_ENCRYPTION_KEY")
            .env("BAMBOO_SELECT_HOOK_ASK_TEST", "1")
            .output()
            .expect("run hook Ask child test");
        assert!(output.status.success(), "isolated hook Ask test failed");
    }

    #[test]
    fn browser_download_hook_ask_redacts_parent_request() {
        let data_dir = tempfile::tempdir().expect("isolated Bamboo data dir");
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .arg("browser_download_hook_ask_child")
            .env("BAMBOO_DATA_DIR", data_dir.path())
            .env_remove("BAMBOO_CONFIG_ENCRYPTION_KEY")
            .env("BAMBOO_DOWNLOAD_HOOK_ASK_TEST", "1")
            .output()
            .expect("run download hook Ask child test");
        assert!(output.status.success(), "isolated hook Ask test failed");
    }

    #[tokio::test]
    async fn browser_download_hook_ask_child() {
        if std::env::var_os("BAMBOO_DOWNLOAD_HOOK_ASK_TEST").is_none() {
            return;
        }
        let config = AgentLoopConfig::default();
        let session = Session::new("browser-download-hook-ask", "model");
        let runtime_state = AgentRuntimeState::new(&session.id);
        let reviewer = Arc::new(BrowserApprovalRecorder::default());
        let reviewer_proxy: Arc<dyn bamboo_tools::ApprovalProxy> = reviewer.clone();
        let callable_set =
            effective_callable_set(&["browser"], CapabilityLoadingMode::LegacyFullCatalog, &[]);
        let secret = "a[data-private='account']";

        for (index, name) in ["browser", "default::browser", "private-namespace::browser"]
            .into_iter()
            .enumerate()
        {
            let args = serde_json::json!({
                "action":"download", "selector":secret, "expected_epoch":17,
            });
            let call = ToolCall {
                id: format!("download-hook-ask-{index}"),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: name.to_string(),
                    arguments: args.to_string(),
                },
            };
            let execution_name = if name == "private-namespace::browser" {
                // Model aliases normally resolve before hook Ask; exercise a
                // private namespace reaching the same canonical execution.
                "browser".to_string()
            } else {
                callable_set
                    .resolve_callable_reference(&call.function.name)
                    .expect("browser resolves to the registered tool")
            };
            assert_eq!(execution_name, "browser");
            let outcome = bamboo_tools::with_approval_proxy(
                Some(reviewer_proxy.clone()),
                hook_ask_outcome(
                    &call,
                    &execution_name,
                    &config,
                    &session,
                    &runtime_state,
                    &args,
                ),
            )
            .await
            .expect("parent review returns an outcome");
            assert!(outcome.result.is_err());
        }

        let asks = reviewer.0.lock().expect("browser approval record lock");
        assert_eq!(asks.len(), 3);
        for ask in asks.iter() {
            assert_eq!(ask.tool_name, "browser");
            assert_eq!(ask.resource, "[redacted]");
            let request = ask.permission_request.as_ref().unwrap();
            assert_eq!(request.tool_name, "browser");
            assert_eq!(request.resource, "[redacted]");
            assert_eq!(
                request.operation_summary,
                "Download from selected browser element"
            );
            assert!(request.suggested_matchers.is_empty());
            let display = format!("{ask:?}");
            assert!(!display.contains(secret));
            assert!(!display.contains("download:css:"));
            assert!(!display.contains("private-namespace"));
        }
        drop(asks);

        let malformed = serde_json::json!({
            "action":"download", "selector":secret, "expected_epoch":17,
            "url":"https://example.test/private-query",
        });
        let call = ToolCall {
            id: "download-hook-ask-malformed".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "default::browser".to_string(),
                arguments: malformed.to_string(),
            },
        };
        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy),
            hook_ask_outcome(
                &call,
                "browser",
                &config,
                &session,
                &runtime_state,
                &malformed,
            ),
        )
        .await
        .expect("malformed download fails closed");
        assert!(matches!(outcome.result,
            Err(ref error) if error == "Browser download permission check failed"));
        assert_eq!(
            reviewer
                .0
                .lock()
                .expect("browser approval record lock")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn namespaced_browser_select_hook_ask_child() {
        if std::env::var_os("BAMBOO_SELECT_HOOK_ASK_TEST").is_none() {
            return;
        }
        let config = AgentLoopConfig::default();
        let session = Session::new("browser-select-hook-ask", "model");
        let runtime_state = AgentRuntimeState::new(&session.id);
        let reviewer = Arc::new(BrowserApprovalRecorder::default());
        let reviewer_proxy: Arc<dyn bamboo_tools::ApprovalProxy> = reviewer.clone();
        let args = serde_json::json!({
            "action":"select_option",
            "selector":"select[data-private='account']",
            "values":["private-option-value"],
            "expected_epoch":17,
        });
        let call = ToolCall {
            id: "select-hook-ask".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "default::browser".to_string(),
                arguments: args.to_string(),
            },
        };
        let callable_set =
            effective_callable_set(&["browser"], CapabilityLoadingMode::LegacyFullCatalog, &[]);
        let execution_name = callable_set
            .resolve_callable_reference(&call.function.name)
            .expect("namespaced browser resolves to the registered tool");
        assert_eq!(execution_name, "browser");
        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy.clone()),
            hook_ask_outcome(
                &call,
                &execution_name,
                &config,
                &session,
                &runtime_state,
                &args,
            ),
        )
        .await
        .expect("parent review returns an outcome");
        assert!(outcome.result.is_err());
        let asks = reviewer.0.lock().expect("browser approval record lock");
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].tool_name, "default::browser");
        assert_eq!(asks[0].resource, "[redacted]");
        let request = asks[0]
            .permission_request
            .as_ref()
            .expect("typed review request");
        assert_eq!(
            request.permission_type,
            bamboo_tools::permission::PermissionType::BrowserInteraction
        );
        assert_eq!(request.resource, "[redacted]");
        assert_eq!(request.operation_summary, "Select native browser options");
        assert!(request.suggested_matchers.is_empty());
        for private in ["private-option-value", "data-private", "options:"] {
            assert!(!format!("{:?}", asks[0]).contains(private));
        }
        drop(asks);

        let invalid_args = serde_json::json!({
            "action":"select_option",
            "selector":"select[data-private='account']",
            "values":["private-option-value"],
            "expected_epoch":"invalid",
        });
        let invalid_call = ToolCall {
            id: "select-hook-ask-invalid".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "default::browser".to_string(),
                arguments: invalid_args.to_string(),
            },
        };
        let outcome = bamboo_tools::with_approval_proxy(
            Some(reviewer_proxy),
            hook_ask_outcome(
                &invalid_call,
                &execution_name,
                &config,
                &session,
                &runtime_state,
                &invalid_args,
            ),
        )
        .await
        .expect("invalid request fails closed");
        assert!(matches!(
            outcome.result,
            Err(ref error) if error == "Browser selection permission check failed"
        ));
        assert_eq!(
            reviewer
                .0
                .lock()
                .expect("browser approval record lock")
                .len(),
            1
        );
    }

    #[test]
    fn malformed_browser_tool_warning_hides_raw_preview_and_repair_message() {
        for (tool_name, raw) in [
            (
                "browser",
                r#"{"action":"type","text":"private browser input"#,
            ),
            ("browser_eval", r#"{"code":"private page source"#),
            (
                "session_message",
                r#"{"action":"send","message":"private peer message"#,
            ),
            (
                "SESSION_MESSAGE",
                r#"{"action":"reply","message":"private peer reply"#,
            ),
            (
                "work_update",
                r#"{"operations":[{"objective":"private work instructions"#,
            ),
        ] {
            let (_, warning) = parse_tool_args_best_effort(raw);
            let warning = warning.expect("malformed JSON warning");
            assert!(warning.contains("private"));
            let (preview, logged_warning) = parse_warning_log_details(tool_name, raw, &warning);
            assert_eq!(preview, "[redacted]");
            assert_eq!(logged_warning, "[redacted]");
            let (ordinary_preview, ordinary_warning) =
                parse_warning_log_details("probe", raw, &warning);
            assert!(ordinary_preview.contains("private"));
            assert_eq!(ordinary_warning, warning);
        }
    }

    #[test]
    fn file_input_tool_start_is_display_only_and_never_carries_file_bytes() {
        let args = serde_json::json!({
            "action":"set_file_input","selector":"#upload","filename":"private.txt",
            "data_base64":"cHJpdmF0ZSBieXRlcw==",
        });
        let original = args.clone();
        assert_eq!(
            tool_start_arguments_for_display("browser", &args),
            serde_json::json!({"action":"set_file_input","file":"[redacted]"})
        );
        assert_eq!(args, original);
        assert_eq!(tool_start_arguments_for_display("other", &args), args);
        assert_eq!(tool_start_name_for_display("default::browser"), "browser");
    }

    #[test]
    fn download_tool_start_is_display_only_and_never_carries_selector_or_extras() {
        let args = serde_json::json!({
            "action":"download","selector":"a[data-secret='private']",
            "expected_epoch":17,"url":"https://private.example/file",
            "nested":{"filename":"private.txt"},
        });
        let original = args.clone();
        let display = serde_json::json!({"action":"download","expected_epoch":17});
        assert_eq!(tool_start_arguments_for_display("browser", &args), display);
        assert_eq!(
            tool_start_arguments_for_display("default::browser", &args),
            display
        );
        let mut poisoned = args.clone();
        poisoned["data_base64"] = serde_json::json!("private-file-bytes");
        assert_eq!(
            tool_start_arguments_for_display("browser", &poisoned),
            display
        );
        assert_eq!(args, original);
        assert_eq!(tool_start_arguments_for_display("other", &args), args);
        assert_eq!(
            tool_start_name_for_display("private-selector::browser"),
            "browser"
        );
        assert_eq!(tool_start_name_for_display("other"), "other");
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
            output_cap: None,
            permission_replay_origin: None,
            result: Ok(ToolResult::text(true, "raw output")),
            needs_human: None,
            portable_tool: None,
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
            output_cap: None,
            permission_replay_origin: None,
            result: Err("executor exploded".to_string()),
            needs_human: None,
            portable_tool: None,
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
                vision_support: true,
                output_cap: Some(97),
                executing_supervisor: None,
                tool_call: &tool_call,
                event_tx: &event_tx,
                metrics_collector: None,
                session_id,
                root_session_id: session_id,
                root_orchestration_only: false,
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
            assert_eq!(outcome.output_cap, Some(Some(97)));
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
            vision_support: true,
            output_cap: None,
            executing_supervisor: None,
            tool_call: &tool_call,
            event_tx: &event_tx,
            metrics_collector: None,
            session_id: "hook-ask-no-parent",
            root_orchestration_only: false,
            root_session_id: "hook-ask-no-parent",
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

    struct WaitingCapHook {
        decision: HookResult,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl AgentHook for WaitingCapHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeToolExecution
        }
        async fn run(
            &self,
            _: AgentHookPoint,
            payload: &HookPayload,
            session: &Session,
        ) -> HookResult {
            let HookPayload::ToolExecution { tool_call_id, .. } = payload else {
                panic!("tool seam")
            };
            let mut ctx = bamboo_agent_core::tools::ToolCtx::none(tool_call_id.clone());
            ctx.session_id = Some(session.id.clone().into());
            assert_eq!(
                bamboo_agent_core::tools::observed_tool_output_cap(&ctx),
                None,
                "BeforeTool is outside the executor observation scope"
            );
            self.entered.notify_one();
            self.release.notified().await;
            self.decision.clone()
        }
    }

    async fn execute_cap_hook_call(
        session: &mut Session,
        tools: &Arc<dyn ToolExecutor>,
        config: &AgentLoopConfig,
        call: &ToolCall,
    ) -> Result<ToolExecutionOutcome, AgentError> {
        let (event_tx, _event_rx) = mpsc::channel(128);
        let flags = ToolExecutionSessionFlags::from_session(session);
        let sid = session.id.clone();
        let mut runtime = session
            .agent_runtime_state
            .clone()
            .unwrap_or_else(|| AgentRuntimeState::new(&sid));
        let callable = effective_callable_set(
            &[call.function.name.as_str()],
            CapabilityLoadingMode::LegacyFullCatalog,
            &[],
        );
        execute_model_requested_tool_call_only(
            &callable,
            ToolExecutionOnlyContext {
                vision_support: true,
                output_cap: None,
                executing_supervisor: None,
                tool_call: call,
                event_tx: &event_tx,
                metrics_collector: None,
                session_id: &sid,
                root_session_id: &sid,
                root_orchestration_only: false,
                round_id: "cap-hook",
                round: 0,
                tools,
                config,
                hook_session: Some(session),
                hook_runtime_state: Some(&mut runtime),
                session_flags: flags,
                available_tool_schemas: &[],
            },
        )
        .await
    }

    #[tokio::test]
    async fn scoped_cap_follows_real_continuing_native_hook_outcomes() {
        for cap in [None, Some(0), Some(53)] {
            for decision in [
                HookResult::Continue,
                HookResult::Allow,
                HookResult::Mutated,
                HookResult::InjectContext {
                    text: "cap-context".into(),
                },
                HookResult::WithContext {
                    result: Box::new(HookResult::Allow),
                    text: "cap-control".into(),
                },
            ] {
                let mut session = Session::new("cap-hook-session", "model");
                session.token_budget = cap.map(|n| bamboo_domain::TokenBudget {
                    max_tool_output_tokens: n,
                    ..Default::default()
                });
                let initial = serde_json::to_value(&session.token_budget).unwrap();
                let entered = Arc::new(tokio::sync::Notify::new());
                let release = Arc::new(tokio::sync::Notify::new());
                let mut runner = crate::runtime::hooks::HookRunner::new();
                runner.register(Arc::new(WaitingCapHook {
                    decision: decision.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                }));
                let config = AgentLoopConfig {
                    hook_runner: Arc::new(runner),
                    ..Default::default()
                };
                let concrete = Arc::new(ExactOwnerRecordingExecutor::default());
                let tools: Arc<dyn ToolExecutor> = concrete.clone();
                let call = probe_call("probe");
                let outcome = {
                    let execution = execute_cap_hook_call(&mut session, &tools, &config, &call);
                    tokio::pin!(execution);
                    tokio::select! {
                        result = &mut execution => panic!("hook must wait: {}", result.is_ok()),
                        _ = entered.notified() => {}
                    }
                    assert!(concrete.exact_dispatches.lock().unwrap().is_empty());
                    release.notify_one();
                    execution.await.unwrap()
                };
                assert_eq!(outcome.output_cap, Some(cap));
                assert_eq!(*concrete.output_caps.lock().unwrap(), vec![cap]);
                assert_eq!(outcome.result.unwrap().result, "exact dispatch");
                assert_eq!(
                    serde_json::to_value(&session.token_budget).unwrap(),
                    initial
                );
                assert!(session.resolved_token_budget.is_none());
                if matches!(
                    decision,
                    HookResult::InjectContext { .. } | HookResult::WithContext { .. }
                ) {
                    assert!(session
                        .messages
                        .iter()
                        .any(|m| m.content.contains("cap-context")
                            || m.content.contains("cap-control")));
                }
            }
        }
    }

    #[tokio::test]
    async fn scoped_cap_is_absent_on_real_native_control_exit_and_cancel() {
        for decision in [
            HookResult::Deny {
                reason: "blocked cap".into(),
            },
            HookResult::Ask,
            HookResult::Suspend {
                reason: "park cap".into(),
            },
            HookResult::Abort {
                reason: "abort cap".into(),
            },
        ] {
            let mut session = Session::new("cap-hook-stop", "model");
            session.token_budget = Some(bamboo_domain::TokenBudget {
                max_tool_output_tokens: 83,
                ..Default::default()
            });
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let mut runner = crate::runtime::hooks::HookRunner::new();
            runner.register(Arc::new(WaitingCapHook {
                decision,
                entered: entered.clone(),
                release: release.clone(),
            }));
            let config = AgentLoopConfig {
                hook_runner: Arc::new(runner),
                ..Default::default()
            };
            let concrete = Arc::new(ExactOwnerRecordingExecutor::default());
            let tools: Arc<dyn ToolExecutor> = concrete.clone();
            let call = probe_call("probe");
            release.notify_one();
            if let Ok(outcome) = execute_cap_hook_call(&mut session, &tools, &config, &call).await {
                assert_eq!(outcome.output_cap, None);
                assert!(outcome.result.is_err());
            }
            assert!(concrete.output_caps.lock().unwrap().is_empty());
            let mut execution =
                Box::pin(execute_cap_hook_call(&mut session, &tools, &config, &call));
            tokio::select! { biased;
                _ = &mut execution => panic!("hook must block"),
                _ = std::future::ready(()) => {}
            }
            drop(execution);
            assert!(concrete.output_caps.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn scoped_cap_native_ask_continuation_is_distinct_from_returned_ask() {
        for approve in [false, true] {
            let mut runner = crate::runtime::hooks::HookRunner::new();
            runner.register(Arc::new(AskToolHook));
            let config = AgentLoopConfig {
                hook_runner: Arc::new(runner),
                ..Default::default()
            };
            let mut session = Session::new("cap-ask", "model");
            session.token_budget = Some(bamboo_domain::TokenBudget {
                max_tool_output_tokens: 4096,
                ..Default::default()
            });
            let mut state = AgentRuntimeState::new(&session.id);
            state.bypass_permissions = true;
            session.agent_runtime_state = Some(state);
            let concrete = Arc::new(ExactOwnerRecordingExecutor::default());
            let tools: Arc<dyn ToolExecutor> = concrete.clone();
            let reviewer = Arc::new(RecordingParentReviewer {
                seen: AtomicBool::new(false),
                approve,
            });
            let outcome = bamboo_tools::with_approval_proxy(
                Some(reviewer.clone()),
                execute_cap_hook_call(&mut session, &tools, &config, &probe_call("probe")),
            )
            .await
            .unwrap();
            assert!(reviewer.seen.load(Ordering::SeqCst));
            assert_eq!(outcome.output_cap, approve.then_some(Some(4096)));
            assert_eq!(outcome.result.is_ok(), approve);
            assert_eq!(
                *concrete.output_caps.lock().unwrap(),
                if approve { vec![Some(4096)] } else { vec![] }
            );
        }
    }
}
