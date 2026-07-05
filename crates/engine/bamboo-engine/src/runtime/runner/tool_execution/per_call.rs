use std::sync::Arc;

use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::{
    parse_tool_args_best_effort, ToolCall, ToolExecutionContext, ToolExecutionSessionFlags,
    ToolExecutor, ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentEvent, Session};
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
    ctx: ToolExecutionOnlyContext<'_>,
) -> ToolExecutionOutcome {
    if let Err(policy_error) = policy::validate_tool_call_arguments(ctx.tool_call) {
        tracing::warn!(
            "[{}][round:{}] Tool call blocked by strict argument policy before ToolStart: tool_call_id={}, tool_name={}, error={}",
            ctx.session_id,
            ctx.round,
            ctx.tool_call.id,
            ctx.tool_call.function.name,
            policy_error
        );
        return ToolExecutionOutcome {
            needs_human: None,
            result: Err(policy_error),
            tool_duration: std::time::Duration::ZERO,
        };
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
            ctx.config.composition_executor.as_ref().map(Arc::clone),
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

    ToolExecutionOutcome {
        result: result.map_err(|error| error.to_string()),
        needs_human,
        tool_duration,
    }
}

pub(super) async fn apply_tool_execution_outcome(
    ctx: ToolExecutionApplyContext<'_>,
    outcome: ToolExecutionOutcome,
) -> bool {
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

    result
}
