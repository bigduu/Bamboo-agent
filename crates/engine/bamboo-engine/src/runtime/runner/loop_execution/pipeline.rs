//! Simplified while(tool_call) pipeline for the agent loop.
//!
//! Replaces the round-based state machine with a flat loop:
//!   loop { call LLM -> if no tool calls break -> execute tools -> repeat }
//!
//! "Round" is kept only as a counter for metrics compatibility.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::guardian_state::{
    ensure_guardian_state, guardian_read_only_disabled_tools, write_guardian_config,
    write_guardian_state, GuardianPhase, GUARDIAN_REVIEW_RUBRIC,
};
use crate::runtime::runner::loop_execution::startup::{
    resolve_auxiliary_models, InFlightTaskEvaluation, LoopRunState,
};
use crate::runtime::runner::prompt_context::PromptMemoryRuntimeContext;
use crate::runtime::runner::session_setup::tool_schemas::resolve_available_tool_schemas_for_session;
use crate::runtime::stream::handler::StreamHandlingOutput;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Message, Session};
use bamboo_domain::session::runtime_state::{
    AgentRuntimeState, AgentStatusState, ChildWaitPolicy, SuspensionState, WaitingForBashState,
    WaitingForChildrenState,
};
use bamboo_llm::LLMProvider;
use bamboo_metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
    TokenUsage as MetricsTokenUsage,
};

use super::super::to_event_token_usage;
use super::gold::{
    apply_completed_gold_evaluation, drain_in_flight_gold_evaluation, evaluate_gold_terminal,
    poll_completed_gold_evaluation, spawn_gold_evaluation_if_needed,
    start_queued_gold_evaluation_if_idle, GoldTerminalDecision,
};
use crate::runtime::runner::state_bridge;

const MAX_LLM_TURN_ATTEMPTS: usize = 3;
const LLM_RETRY_BASE_DELAY_MS: u64 = 400;

// ---- Error classification (from rounds.rs) ----

fn should_retry_turn_error(error: &AgentError) -> bool {
    let AgentError::LLM(message) = error else {
        return false;
    };
    let message = message.trim().to_ascii_lowercase();
    if message.is_empty() {
        return false;
    }
    let non_retryable_patterns = [
        "authentication error",
        "invalid api key",
        "invalid_request_error",
        "unsupported model",
        "model_name is required",
        "http 400",
        "http 401",
        "http 403",
        "http 404",
    ];
    !non_retryable_patterns
        .iter()
        .any(|pattern| message.contains(pattern))
}

fn is_overflow_recoverable(error: &AgentError) -> bool {
    matches!(error, AgentError::LLMOverflow(_))
}

// ---- Turn outcome (replaces RoundFlowOutcome) ----

struct TurnOutcome {
    should_break: bool,
    sent_complete: bool,
}

/// Terminal child run statuses, as mirrored into the session index.
fn is_terminal_child_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "error" | "timeout" | "cancelled" | "skipped"
    )
}

/// Runner primitive: durably suspend `session` to wait on a known set of child
/// sessions, returning the canonical "stop the turn, do not send complete"
/// outcome.
///
/// Centralizes the suspend transaction so every runner-initiated terminal gate
/// (the orphaned-children safety net, the guardian review gate, ...) registers
/// the wait identically: build the durable [`WaitingForChildrenState`], mirror
/// it into the session via [`state_bridge::write_runtime_state`], stamp the
/// `runtime.suspend_reason` metadata — always `"waiting_for_children"`, the
/// discriminant the suspend-finalization keys on — bump `updated_at`, and
/// persist so the completion coordinator can resume this parent and the suspend
/// finalization merges (rather than clobbers) the durable wait.
///
/// The caller owns child *discovery*; `child_session_ids` is assumed already
/// sorted/deduped where order matters.
async fn suspend_to_wait_for_children(
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    child_session_ids: Vec<String>,
    wait_for: ChildWaitPolicy,
) -> TurnOutcome {
    let now = Utc::now();
    let count = child_session_ids.len();
    runtime_state.waiting_for_children = Some(WaitingForChildrenState::for_children(
        child_session_ids,
        wait_for,
        now,
    ));
    state_bridge::write_runtime_state(session, runtime_state);
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "waiting_for_children".to_string(),
    );
    session.updated_at = now;

    if let Some(persistence) = persistence {
        if let Err(error) = persistence.save_runtime_session(session).await {
            tracing::warn!(
                "[{}] suspend-to-wait failed to persist parent wait on {} child(ren): {}",
                session.id,
                count,
                error
            );
        }
    }

    TurnOutcome {
        should_break: true,
        sent_complete: false,
    }
}

/// End-of-turn safety net for the spawn/wait model.
///
/// `SubAgent.create` runs children in the background without suspending, and the
/// model is expected to call `SubAgent.wait` when it wants their results. If the
/// model instead finishes its turn (no tool calls) while children are still
/// running and it never registered a wait, we suspend here on its behalf so
/// background results are never silently dropped.
///
/// Returns `Some` suspend outcome (with the durable wait persisted) when it
/// engages, or `None` to let the run complete normally. No-ops when there is no
/// storage, no active children, or a wait is already registered — so child
/// sessions (which have no children) and explicit-wait flows are unaffected.
async fn maybe_suspend_for_orphaned_children(
    session: &mut Session,
    config: &AgentLoopConfig,
    runtime_state: &mut AgentRuntimeState,
) -> Option<TurnOutcome> {
    if runtime_state.waiting_for_children.is_some() {
        return None;
    }
    let storage = config.storage.as_ref()?;

    let mut active: Vec<String> = storage
        .list_child_run_statuses(&session.id)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, status)| !status.as_deref().is_some_and(is_terminal_child_status))
        .map(|(id, _)| id)
        .collect();
    if active.is_empty() {
        return None;
    }
    active.sort();
    active.dedup();

    tracing::info!(
        "[{}] end-of-turn safety net: suspending to wait for {} orphaned child session(s) the model did not explicitly wait on",
        session.id,
        active.len(),
    );
    Some(
        suspend_to_wait_for_children(
            session,
            runtime_state,
            config.persistence.as_ref(),
            active,
            ChildWaitPolicy::All,
        )
        .await,
    )
}

/// Runner primitive: durably suspend `session` to wait on a known set of still
/// running background Bash shells, returning the canonical "stop the turn, do
/// not send complete" outcome (issue #84 Phase 2b).
///
/// A structural peer to [`suspend_to_wait_for_children`]: build the durable
/// [`WaitingForBashState`], mirror it into the session via
/// [`state_bridge::write_runtime_state`], stamp the `runtime.suspend_reason`
/// metadata — always `"waiting_for_bash"`, the discriminant the suspend
/// finalization keys on — bump `updated_at`, and persist so a future resume
/// coordinator (Phase 2c) can resume this session. The wait policy is fixed
/// ("all bash ids must finish"), so, unlike children, no policy enum is taken.
async fn suspend_to_wait_for_bash(
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    bash_ids: Vec<String>,
) -> TurnOutcome {
    let now = Utc::now();
    let count = bash_ids.len();
    runtime_state.waiting_for_bash = Some(WaitingForBashState::for_bash(bash_ids, now));
    state_bridge::write_runtime_state(session, runtime_state);
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "waiting_for_bash".to_string(),
    );
    session.updated_at = now;

    if let Some(persistence) = persistence {
        if let Err(error) = persistence.save_runtime_session(session).await {
            tracing::warn!(
                "[{}] suspend-to-wait-bash failed to persist bash wait on {} shell(s): {}",
                session.id,
                count,
                error
            );
        }
    }

    TurnOutcome {
        should_break: true,
        sent_complete: false,
    }
}

/// End-of-turn safety net for background Bash shells (issue #84 Phase 2b).
///
/// A background shell (`run_in_background: true`) runs detached from the agent
/// loop, so the model can finish its turn (no tool calls) while the shell is
/// still producing output. To avoid silently dropping that background work, we
/// suspend here on the session's behalf. The opt-in is implicit: only
/// `run_in_background` shells land in the session-aware registry, so the default
/// foreground path never trips this.
///
/// Returns `Some` suspend outcome (with the durable wait persisted AND a
/// self-resume hook arranged) when it engages, or `None` to let the run
/// proceed. No-ops when no background shells are still running, a bash wait is
/// already registered, or durable backing + a resume hook are unavailable
/// (should-fix 1 — mirrors children's durability guard so a session never
/// strands itself without a resume path). This is an independent check from
/// [`maybe_suspend_for_orphaned_children`]; the call site runs the children gate
/// first, so a session already suspending for children never reaches this in the
/// same pass.
async fn maybe_suspend_for_outstanding_bash(
    session: &mut Session,
    config: &AgentLoopConfig,
    runtime_state: &mut AgentRuntimeState,
) -> Option<TurnOutcome> {
    if runtime_state.waiting_for_bash.is_some() {
        return None;
    }

    // Should-fix 1: a suspend without durable backing or a resume hook would
    // strand the session forever — the self-resume task reloads from
    // persistence, and without a wired hook no resume can ever fire.
    config.persistence.as_ref()?;
    let hook = config.bash_resume_hook.as_ref()?;

    let mut bash_ids = bamboo_tools::tools::bash_runtime::running_shells_for_session(&session.id);
    if bash_ids.is_empty() {
        return None;
    }
    bash_ids.sort();
    bash_ids.dedup();

    // Blocker 1: close the snapshot→commit TOCTOU. A shell captured above may
    // finish before we commit the suspend; if ALL did, do not strand the
    // session — let the turn complete normally. The self-resume poll task
    // (arranged below) handles shells that complete AFTER the commit.
    if bamboo_tools::tools::bash_runtime::running_shells_for_session(&session.id).is_empty() {
        tracing::info!(
            "[{}] end-of-turn bash gate: all {} shell(s) finished during the snapshot window; not suspending",
            session.id,
            bash_ids.len(),
        );
        return None;
    }

    tracing::info!(
        "[{}] end-of-turn safety net: suspending to wait for {} background bash shell(s) still running",
        session.id,
        bash_ids.len(),
    );

    // Clone ids for the self-resume hook before moving them into the suspend.
    let hook_ids = bash_ids.clone();
    let outcome = suspend_to_wait_for_bash(
        session,
        runtime_state,
        config.persistence.as_ref(),
        bash_ids,
    )
    .await;

    // Blocker 2: arrange the self-resume safety net so the session is ALWAYS
    // eventually resumed once the captured shells finish. The hook polls the
    // live registry — not the one-shot BashCompleted event — so it is immune to
    // the lost-wakeup: even if a shell completes during the persist above, the
    // poll task's first check will see it as not-running and resume.
    hook.arrange_bash_self_resume(session.id.clone(), hook_ids);

    Some(outcome)
}

/// Build the guardian reviewer's task brief: the static rubric plus the active
/// task's completion criteria, the session goal, and (issue #400) the agent's
/// own final assistant message, when present.
///
/// `final_assistant_content` is READ-ONLY review context: it is folded into
/// the prompt text handed to the spawned reviewer, but the caller must NOT
/// have already persisted it into the session transcript that the reviewer
/// child forks (see [`maybe_spawn_guardian_review`] and
/// `handle_no_tool_calls`'s no-goal-loop deferral) — otherwise the reviewer
/// would see the same content twice. Blank/whitespace-only content is treated
/// as absent so an empty final turn never adds a stray, empty section.
fn build_guardian_review_prompt(
    task_context: &Option<TaskLoopContext>,
    config: &AgentLoopConfig,
    final_assistant_content: Option<&str>,
) -> String {
    let mut prompt = String::from(GUARDIAN_REVIEW_RUBRIC);

    let criteria: Vec<String> = task_context
        .as_ref()
        .and_then(|ctx| {
            ctx.items
                .iter()
                .find(|item| Some(&item.id) == ctx.active_item_id.as_ref())
        })
        .map(|item| item.completion_criteria.clone())
        .unwrap_or_default();
    if !criteria.is_empty() {
        prompt.push_str("\n\n## Completion criteria (verify EACH against real evidence)\n");
        for (idx, criterion) in criteria.iter().enumerate() {
            prompt.push_str(&format!("{}. {}\n", idx + 1, criterion));
        }
    }

    let goal = config.active_goal();
    if let Some(goal) = goal {
        prompt.push_str("\n\n## Session goal\n");
        prompt.push_str(goal);
        prompt.push('\n');
    }

    if criteria.is_empty() && goal.is_none() {
        prompt.push_str(
            "\n\n(No explicit completion criteria or goal were provided; review the diff for correctness, completeness, and obvious bugs.)\n",
        );
    }

    // Issue #400: the agent's own final assistant turn (its summary/handoff)
    // is not always visible in the forked transcript the reviewer child sees
    // — in the no-goal-loop configuration it is intentionally deferred out of
    // the parent session until AFTER the guardian gate, to avoid a resumed
    // turn re-emitting it (see `handle_no_tool_calls`). Fold it in here as
    // plain review context so the reviewer still sees what the agent actually
    // said, without persisting it anywhere.
    if let Some(content) = final_assistant_content {
        let trimmed = content.trim();
        if !trimmed.is_empty() {
            prompt.push_str(
                "\n\n## Agent's final message (context only — not yet part of the session transcript)\n",
            );
            prompt.push_str(trimmed);
            prompt.push('\n');
        }
    }

    prompt
}

/// Terminal gate (peer to [`maybe_suspend_for_orphaned_children`]): before a run
/// completes, spawn a read-only adversarial reviewer child and suspend on its
/// verdict. Returns `Some` suspend outcome when it engages a review, or `None`
/// to let the run complete — guardian inactive, the verdict already accepted the
/// work, the review budget is spent, or a spawn failure that must not strand the
/// run.
///
/// Driven by [`GuardianState`]: `None` → spawn the first review; `Pending` →
/// never double-spawn (a review is in flight, the resume path re-enters with a
/// verdict); `Reviewed` + approve → complete; `Reviewed` + reject → re-review the
/// fix until [`GuardianState::budget_exhausted`]. The budget is the hard bound on
/// the review→fix→review loop, so it always terminates.
///
/// `final_assistant_content` (issue #400) is the agent's own final assistant
/// turn, passed as READ-ONLY review context — folded into the spawned
/// reviewer's prompt via [`build_guardian_review_prompt`] but never appended
/// to `session`'s message transcript here. Callers pass `None` when the
/// content is already present in the transcript the reviewer child forks
/// (e.g. the goal-loop-active case, which adds the message before this gate
/// runs), so the reviewer never sees it twice.
async fn maybe_spawn_guardian_review(
    session: &mut Session,
    config: &AgentLoopConfig,
    task_context: &Option<TaskLoopContext>,
    runtime_state: &mut AgentRuntimeState,
    iteration: u32,
    final_assistant_content: Option<&str>,
) -> Option<TurnOutcome> {
    // Already suspended waiting on a child (orphan gate / explicit wait won).
    if runtime_state.waiting_for_children.is_some() {
        return None;
    }
    if !config.guardian_active() {
        return None;
    }
    let spawner = config.guardian_spawner.as_ref()?;
    let max_reviews = config.guardian_max_reviews();

    let mut guardian_state = ensure_guardian_state(session);
    match guardian_state.phase {
        // A review is in flight (we suspended for it); never double-spawn.
        GuardianPhase::Pending => return None,
        GuardianPhase::Reviewed => {
            if guardian_state.last_approved() {
                // Work accepted — allow completion.
                return None;
            }
            if guardian_state.budget_exhausted(max_reviews) {
                tracing::warn!(
                    "[{}] guardian: review budget ({}) exhausted with unresolved findings; allowing completion",
                    session.id,
                    max_reviews
                );
                return None;
            }
            // Rejected and budget remains → re-review the fix below.
        }
        GuardianPhase::None => {
            if guardian_state.budget_exhausted(max_reviews) {
                return None;
            }
            // First review → spawn below.
        }
    }

    // Persist the guardian config so the resumed run (driven by the completion
    // coordinator, which has no original request) re-injects it and keeps the
    // review → fix → re-review loop active across the suspend/resume boundary.
    if let Some(guardian_config) = config.guardian_config.as_ref() {
        write_guardian_config(session, guardian_config);
    }

    let review_prompt = build_guardian_review_prompt(task_context, config, final_assistant_content);
    let Some(model) = config
        .guardian_model()
        .map(str::to_string)
        .or_else(|| config.model_name.clone())
        .map(|model| model.trim().to_string())
        .filter(|model| !model.is_empty())
    else {
        // No reviewer model resolves — skip the review rather than spawning a
        // child with an empty model id, which would error out and burn the
        // review budget on a reviewer that never actually runs.
        tracing::warn!(
            "[{}] guardian: no reviewer model resolved; skipping review at this terminal",
            session.id
        );
        return None;
    };
    let disabled_tools = Some(guardian_read_only_disabled_tools());

    match spawner
        .spawn_guardian_review(session, review_prompt, model, disabled_tools)
        .await
    {
        Ok(child_id) => {
            guardian_state.record_spawn(&child_id);
            guardian_state.last_reviewed_at_round = iteration;
            let pass = guardian_state.review_count;
            write_guardian_state(session, guardian_state);
            tracing::info!(
                "[{}] guardian: spawned read-only review child {} (pass {}/{}); suspending until verdict",
                session.id,
                child_id,
                pass,
                max_reviews
            );
            Some(
                suspend_to_wait_for_children(
                    session,
                    runtime_state,
                    config.persistence.as_ref(),
                    vec![child_id],
                    ChildWaitPolicy::All,
                )
                .await,
            )
        }
        Err(error) => {
            tracing::warn!(
                "[{}] guardian: failed to spawn review child: {}; allowing completion",
                session.id,
                error
            );
            None
        }
    }
}

// ---- Metrics helpers (from round_error.rs) ----

fn map_turn_error_status(error: &AgentError) -> (MetricsRoundStatus, MetricsSessionStatus) {
    if matches!(error, AgentError::Cancelled) {
        (
            MetricsRoundStatus::Cancelled,
            MetricsSessionStatus::Cancelled,
        )
    } else {
        (MetricsRoundStatus::Error, MetricsSessionStatus::Error)
    }
}

fn record_turn_failure(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    message_count: u32,
    error: &AgentError,
) {
    let (round_status, session_status) = map_turn_error_status(error);
    crate::runtime::runner::metrics_lifecycle::record_round_and_session_error(
        metrics_collector,
        round_id,
        session_id,
        message_count,
        round_status,
        Some(error.to_string()),
        session_status,
    );
}

async fn poll_completed_task_evaluation(state: &mut LoopRunState) {
    let finished = state
        .task_evaluation
        .in_flight
        .as_ref()
        .is_some_and(|in_flight| in_flight.join_handle.is_finished());
    if !finished {
        return;
    }

    let Some(in_flight) = state.task_evaluation.in_flight.take() else {
        return;
    };

    match in_flight.join_handle.await {
        Ok(Some(result)) => {
            state.task_evaluation.completed = Some(result);
        }
        Ok(None) => {
            // The run was cancelled while this task evaluation was in flight; the
            // eval future was dropped before completing, so there is no outcome
            // to apply.
            tracing::debug!(
                "[{}] Async task evaluation cancelled for round {}",
                state.session_id,
                in_flight.request.round_number
            );
        }
        Err(error) => {
            tracing::warn!(
                "[{}] Async task evaluation join failed for round {}: {}",
                state.session_id,
                in_flight.request.round_number,
                error
            );
        }
    }
}

async fn drain_in_flight_task_evaluation(state: &mut LoopRunState) {
    if state.task_evaluation.completed.is_some() {
        return;
    }

    let Some(in_flight) = state.task_evaluation.in_flight.take() else {
        return;
    };

    match in_flight.join_handle.await {
        Ok(Some(result)) => {
            state.task_evaluation.completed = Some(result);
        }
        Ok(None) => {
            tracing::debug!(
                "[{}] Async task evaluation cancelled while draining round {}",
                state.session_id,
                in_flight.request.round_number
            );
        }
        Err(error) => {
            tracing::warn!(
                "[{}] Async task evaluation join failed while draining round {}: {}",
                state.session_id,
                in_flight.request.round_number,
                error
            );
        }
    }
}

async fn apply_completed_task_evaluation(
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
) {
    let Some(result) = state.task_evaluation.completed.take() else {
        return;
    };

    let apply_outcome = crate::runtime::runner::task_lifecycle::apply_task_evaluation_result(
        &mut state.task_context,
        session,
        &state.session_id,
        result.clone(),
    );

    let synthetic_round_id = format!(
        "{}-task-evaluation-round-{}",
        state.session_id, result.round_number
    );
    crate::runtime::runner::metrics_lifecycle::record_round_started(
        state.metrics_collector.as_ref(),
        &synthetic_round_id,
        &state.session_id,
        result.model_name.as_str(),
    );
    crate::runtime::runner::metrics_lifecycle::record_round_completed(
        state.metrics_collector.as_ref(),
        &synthetic_round_id,
        &state.session_id,
        session.messages.len() as u32,
        if apply_outcome.stale {
            MetricsRoundStatus::Cancelled
        } else {
            MetricsRoundStatus::Success
        },
        apply_outcome.usage,
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.prompt_cached_tool_outputs)
            .unwrap_or(0)
            .min(u32::MAX as usize) as u32,
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.prompt_cached_tool_tokens_saved)
            .unwrap_or(0),
        None,
    );

    if !apply_outcome.stale && apply_outcome.applied_updates > 0 {
        if let Some(ref ctx) = state.task_context {
            let task_list_title = result
                .task_list_title
                .or_else(|| {
                    session
                        .task_list
                        .as_ref()
                        .map(|task_list| task_list.title.clone())
                })
                .unwrap_or_else(|| "Agent Tasks".to_string());
            session.set_task_list_version_meta(ctx.version.to_string());
            let task_list = ctx.to_task_list_with_title(task_list_title);
            session.set_task_list(task_list.clone());
            crate::runtime::runner::tool_execution::persist_shared_task_list(
                config,
                session,
                &result.shared_session_id,
                &state.session_id,
                &task_list,
            )
            .await;
            let _ = event_tx
                .send(AgentEvent::TaskListUpdated { task_list })
                .await;
        }
    }
}

fn spawn_task_evaluation_request(
    state: &mut LoopRunState,
    event_tx: &mpsc::Sender<AgentEvent>,
    request: crate::runtime::runner::task_lifecycle::AsyncTaskEvaluationRequest,
    llm: Arc<dyn LLMProvider>,
    cancel_token: CancellationToken,
) {
    let task_round = request.round_number;
    let session_id = state.session_id.clone();
    let event_tx = event_tx.clone();
    let request_for_spawn = request.clone();
    // Thread the run's cancel token into the detached eval so a cancelled run
    // drops the in-flight LLM request future at the first await point (`None`)
    // instead of running the evaluation — and its late `TaskListUpdated` event —
    // to completion (issue #347). `biased` checks cancellation first.
    let join_handle = tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => None,
            result = crate::runtime::runner::task_lifecycle::execute_async_task_evaluation(
                request_for_spawn,
                llm,
                event_tx,
            ) => Some(result),
        }
    });

    tracing::debug!(
        "[{}] Spawned async task evaluation for round {}",
        session_id,
        task_round
    );

    state.task_evaluation.in_flight = Some(InFlightTaskEvaluation {
        request,
        join_handle,
    });
}

/// Abort any in-flight async Gold/Task evaluation and clear its slot.
///
/// Called on EVERY early return from [`run_pipeline`] (cancellation,
/// terminal-error, no-outcome, overflow-recovery failure). The happy path
/// instead *drains* (awaits + applies) these handles after the loop; the early
/// returns skip that drain, so without an explicit abort the `JoinHandle` would
/// simply be dropped — which DETACHES (not aborts) the tokio task, letting a
/// cancelled run keep executing a full LLM request to completion and fire a late
/// event onto the ended stream (issue #347). `abort()` drops the eval future at
/// its next await point, stopping the spend.
fn abort_in_flight_evaluations(state: &mut LoopRunState) {
    if let Some(in_flight) = state.task_evaluation.in_flight.take() {
        in_flight.join_handle.abort();
    }
    if let Some(in_flight) = state.gold_evaluation.in_flight.take() {
        in_flight.join_handle.abort();
    }
}

fn spawn_task_evaluation_if_needed(
    turn: usize,
    session: &Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
    llm: Arc<dyn LLMProvider>,
    cancel_token: CancellationToken,
) -> Result<(), AgentError> {
    // Gate: evaluate only when the Task tool structurally rewrote the list this
    // turn. The flag is set in `maybe_handle_taskwrite`, so an evaluation fires
    // once per Task-tool write rather than every round of tool activity (which
    // bumps `TaskLoopContext::version` without changing the plan). A task list
    // that never went through the Task tool is never auto-evaluated.
    let task_list_dirty = state
        .task_context
        .as_ref()
        .is_some_and(|ctx| ctx.task_list_dirty);
    if !task_list_dirty {
        return Ok(());
    }
    if let Some(ctx) = state.task_context.as_mut() {
        ctx.task_list_dirty = false;
    }

    let eval_model = state
        .auxiliary_models
        .fast_model_name
        .as_deref()
        .or(Some(state.model_name.as_str()));
    let request = crate::runtime::runner::task_lifecycle::build_async_task_evaluation_request(
        &state.task_context,
        session,
        &state.session_id,
        turn + 1,
        eval_model,
        config.reasoning_effort,
    )?;
    let Some(request) = request else {
        return Ok(());
    };

    if state.task_evaluation.in_flight.is_some() {
        state.task_evaluation.queued_request = Some(request);
        tracing::debug!(
            "[{}] Queued latest async task evaluation snapshot for round {} while another evaluation is still in flight",
            state.session_id,
            turn + 1
        );
        return Ok(());
    }

    spawn_task_evaluation_request(state, event_tx, request, llm, cancel_token);
    Ok(())
}

fn refresh_auxiliary_models_for_round(state: &mut LoopRunState, config: &AgentLoopConfig) {
    state.auxiliary_models = resolve_auxiliary_models(config);
    state.runtime_state.llm.fast_model_name = state.auxiliary_models.fast_model_name.clone();
    state.runtime_state.llm.background_model_name =
        state.auxiliary_models.background_model_name.clone();
}

// ---- No-tool-calls path (from round_flow/no_tool_calls.rs) ----

/// Record the terminal `Complete` round metrics for a no-tool-calls turn. Shared
/// by the gold-continue and the completion branches of [`handle_no_tool_calls`].
fn record_no_tool_calls_round_completed(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    session: &Session,
    round_usage: MetricsTokenUsage,
) {
    crate::runtime::runner::metrics_lifecycle::record_round_completed(
        metrics_collector,
        round_id,
        session_id,
        session.messages.len() as u32,
        MetricsRoundStatus::Success,
        round_usage,
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.prompt_cached_tool_outputs)
            .unwrap_or(0)
            .min(u32::MAX as usize) as u32,
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.prompt_cached_tool_tokens_saved)
            .unwrap_or(0),
        None,
    );
}

/// Handle a terminal round where the model emitted NO tool calls.
///
/// Gate ordering (issue #343): the goal-continuation (Gold) gate is evaluated
/// FIRST, before the guardian review gate.
///
/// * When an autonomous goal loop is active and the objective is not yet met, the
///   Gold gate injects a hidden continuation and the run keeps working WITHOUT
///   touching the guardian — so a premature terminal never spends a bounded
///   guardian review (spawn + durable suspend/resume + LLM cost) reviewing an
///   INCOMPLETE state the goal loop already knows is not done.
/// * Only once Gold decides to STOP (the goal is met, or no goal loop is
///   configured) does the guardian review gate run, so the reviewer always sees
///   the genuinely-final state. When the guardian approves (or is inactive / out
///   of budget) the run emits its single terminal `Complete`.
///
/// Preserved cases: with no goal loop configured Gold is a trivial `Stop`, so the
/// guardian runs exactly as before; with no guardian configured the goal loop
/// runs exactly as before.
#[allow(clippy::too_many_arguments)]
async fn handle_no_tool_calls(
    content: String,
    reasoning: Option<String>,
    reasoning_signature: Option<String>,
    prompt_tokens: u64,
    completion_tokens: u64,
    round_usage: MetricsTokenUsage,
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    config: &AgentLoopConfig,
    task_context: &Option<TaskLoopContext>,
    eval_model: &str,
    iteration: u32,
    llm: Arc<dyn LLMProvider>,
) -> TurnOutcome {
    // The Gold judge reads the recent transcript, so when the goal loop is active
    // the assistant's final turn must be in the session BEFORE the gate runs
    // (matching the pre-#343 add-before-gold order). When no goal loop is active
    // the gate is a trivial `Stop` that reads nothing; in that case defer adding
    // the message until the run actually completes, so a guardian suspend on the
    // Stop path does NOT persist a message the resumed turn re-emits — preserving
    // the exact pre-#343 no-goal guardian behavior (the guardian ran before the
    // assistant message was appended).
    let add_message_before_gold = config.goal_loop_active();
    let mut deferred_assistant_message = Some(
        Message::assistant_with_reasoning(content, None, reasoning)
            .with_reasoning_signature(reasoning_signature),
    );
    if add_message_before_gold {
        if let Some(message) = deferred_assistant_message.take() {
            session.add_message(message);
        }
    }

    // Terminal goal gate FIRST (issue #343): when an autonomous goal is active,
    // decide whether to keep working toward it INSTEAD of completing. The agent
    // self-reports completion via `update_goal`, and a side-channel Gold
    // double-check verifies the objective before the run actually stops. Running
    // this inside the loop means the run emits a single terminal `Complete` only
    // when the goal is truly done — keeping `is_running` accurate and the SSE
    // stream open.
    let decision = evaluate_gold_terminal(
        session,
        task_context,
        config,
        eval_model,
        config.reasoning_effort,
        session_id,
        iteration,
        llm,
        event_tx,
    )
    .await;

    if let GoldTerminalDecision::Continue { continuation_count } = decision {
        tracing::info!(
            "[{}] Goal terminal gate: continuing toward goal (continuation {})",
            session_id,
            continuation_count
        );
        record_no_tool_calls_round_completed(
            metrics_collector,
            round_id,
            session_id,
            session,
            round_usage,
        );
        return TurnOutcome {
            should_break: false,
            sent_complete: false,
        };
    }

    // Gold decided STOP: the goal is met, or no goal loop is configured. Only now
    // review the genuinely-final state. Adversarial guardian review: before
    // completing, spawn a read-only reviewer child to verify the work and suspend
    // until its verdict returns. `maybe_spawn_guardian_review` returns `Some` when
    // it engages a review (spawn + suspend); it is inert unless a guardian config
    // + spawner are wired (`config.guardian_active()`).
    //
    // Issue #400: when the assistant message is still deferred (no goal loop —
    // it was never added to `session` above), hand its content to the guardian
    // as read-only review context so the reviewer sees the agent's own final
    // summary/handoff even though the transcript it forks does not contain it
    // yet. When the message WAS already added (goal loop active), pass `None`
    // — it is already in the forked transcript, so adding it again here would
    // duplicate it in the reviewer's context.
    let final_assistant_content_for_guardian = deferred_assistant_message
        .as_ref()
        .map(|message| message.content.as_str());
    if let Some(review) = maybe_spawn_guardian_review(
        session,
        config,
        task_context,
        runtime_state,
        iteration,
        final_assistant_content_for_guardian,
    )
    .await
    {
        // Suspended on the guardian verdict. In the no-goal case the assistant
        // message was intentionally not appended yet (the resumed turn re-emits
        // it), so nothing to roll back here.
        return review;
    }

    // Guardian approved, inactive, or out of budget → complete the run.
    if let Some(message) = deferred_assistant_message.take() {
        session.add_message(message);
    }
    let _ = event_tx
        .send(AgentEvent::Complete {
            usage: to_event_token_usage(prompt_tokens, completion_tokens),
        })
        .await;
    record_no_tool_calls_round_completed(
        metrics_collector,
        round_id,
        session_id,
        session,
        round_usage,
    );
    TurnOutcome {
        should_break: true,
        sent_complete: true,
    }
}

// ---- Tool-calls path (from round_flow/tool_calls.rs) ----

#[allow(clippy::too_many_arguments)]
async fn handle_tool_calls_path(
    frame: &crate::runtime::runner::round_frame::RoundFrame<'_>,
    stream_output: StreamHandlingOutput,
    mut round_usage: MetricsTokenUsage,
    session: &mut Session,
    auxiliary_models: &crate::runtime::config::AuxiliaryModelConfig,
    model_name: &str,
    task_context: &mut Option<TaskLoopContext>,
    cancel_token: &CancellationToken,
) -> Result<TurnOutcome, AgentError> {
    let reasoning_present = !stream_output.reasoning_content.trim().is_empty();
    // Only carry the signature alongside actual reasoning text — a signature
    // with no text would be a nonsensical replay target (#524).
    let reasoning_signature = reasoning_present
        .then_some(stream_output.reasoning_signature.clone())
        .flatten();
    let reasoning = reasoning_present.then_some(stream_output.reasoning_content);
    session.add_message(
        Message::assistant_with_reasoning(
            stream_output.content,
            Some(stream_output.tool_calls.clone()),
            reasoning,
        )
        .with_reasoning_signature(reasoning_signature),
    );

    let compression_model = Some(model_name.to_string())
        .or_else(|| (!session.model.trim().is_empty()).then_some(session.model.trim().to_string()));
    if compression_model.is_none() {
        tracing::warn!(
            "[{}] Skipping mid-turn context compression after tool execution: missing model name",
            frame.session_id
        );
    }
    let tool_schemas =
        resolve_available_tool_schemas_for_session(frame.config, frame.tools.as_ref(), session);

    // Tool execution can block for a long time (up to parallel_batch_timeout_secs,
    // default 300s, and per_tool_timeout_secs for single tools). The loop only
    // polls cancellation BETWEEN rounds, so without this select! a cancel issued
    // *during* tool execution (e.g. a 120s foreground Bash command) would run to
    // completion and the agent would appear unresponsive to cancel for up to
    // minutes.
    //
    // We mirror the LLM stream's biased-cancel pattern (see
    // `stream/handler/consume.rs`): `biased` checks cancellation first so a
    // ready-but-cancelled batch is dropped. On cancel the in-flight tool futures
    // are dropped (true cancellation — foreground Bash is kill_on_drop, so its
    // child is reaped). The per-batch/per-tool `tokio::time::timeout` *inside*
    // `execute_round_tool_calls` is left untouched — cancel is strictly an
    // additional early-exit, the timeout is preserved. (issue #30)
    let tool_execution = tokio::select! {
        biased;
        _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
        result = crate::runtime::runner::tool_execution::execute_round_tool_calls(
            &stream_output.tool_calls,
            frame,
            session,
            task_context,
            compression_model
                .as_deref()
                .or(auxiliary_models.background_model_name.as_deref()),
            auxiliary_models
                .summarization_model_provider
                .as_ref()
                .or(auxiliary_models.background_model_provider.as_ref()),
            &tool_schemas,
        ) => result?,
    };

    // Track round state for metrics
    let mut awaiting_clarification = false;
    let mut waiting_for_children = false;
    let mut round_status = MetricsRoundStatus::Success;
    let mut round_error: Option<String> = None;

    if tool_execution.round_status != MetricsRoundStatus::Success {
        round_status = tool_execution.round_status;
    }
    if let Some(e) = tool_execution.round_error {
        round_error = Some(e);
    }
    if tool_execution.awaiting_clarification {
        awaiting_clarification = true;
    }
    if tool_execution.waiting_for_children {
        waiting_for_children = true;
    }

    if awaiting_clarification || waiting_for_children {
        crate::runtime::runner::metrics_lifecycle::record_round_completed(
            frame.metrics_collector,
            frame.round_id,
            frame.session_id,
            session.messages.len() as u32,
            round_status,
            round_usage,
            session
                .token_usage
                .as_ref()
                .map(|usage| usage.prompt_cached_tool_outputs)
                .unwrap_or(0)
                .min(u32::MAX as usize) as u32,
            session
                .token_usage
                .as_ref()
                .map(|usage| usage.prompt_cached_tool_tokens_saved)
                .unwrap_or(0),
            round_error,
        );
        return Ok(TurnOutcome {
            should_break: true,
            sent_complete: false,
        });
    }

    if frame.debug_enabled {
        tracing::debug!(
            "[{}] round_complete: {}",
            frame.session_id,
            serde_json::json!({
                "round": frame.turn + 1,
                "message_count": session.messages.len(),
            })
        );
    }

    // ---- Dynamic model routing: classify task complexity ----
    // When features.dynamic_model_routing is enabled, evaluate task complexity
    // at the end of each round using the fast model. Store the result in session
    // metadata for downstream consumers (subagents, scheduling, etc.).
    let _complexity = if frame.config.features_dynamic_model_routing {
        // Collect tool call names from this round for classification.
        let round_tool_calls = &stream_output.tool_calls;

        // Use the fast model for classification.
        let classifier_model = auxiliary_models
            .fast_model_name
            .as_deref()
            .or(Some(model_name));
        let _classifier_provider = auxiliary_models
            .fast_model_provider
            .clone()
            .unwrap_or_else(|| frame.llm.clone());

        if let Some(_model) = classifier_model {
            // Heuristic-based classification. For full LLM-backed classification,
            // wire MiniLoopExecutor through the runner (see ComplexityClassifier).
            let complexity = heuristic_complexity(round_tool_calls);
            tracing::info!(
                "[{}] Dynamic model routing: round {} complexity={:?}",
                frame.session_id,
                frame.turn + 1,
                complexity
            );
            session.metadata.insert(
                "last_round_complexity".to_string(),
                format!("{:?}", complexity),
            );
            Some(complexity)
        } else {
            None
        }
    } else {
        None
    };
    round_usage.recompute_total();

    crate::runtime::runner::metrics_lifecycle::record_round_completed(
        frame.metrics_collector,
        frame.round_id,
        frame.session_id,
        session.messages.len() as u32,
        round_status,
        round_usage,
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.prompt_cached_tool_outputs)
            .unwrap_or(0)
            .min(u32::MAX as usize) as u32,
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.prompt_cached_tool_tokens_saved)
            .unwrap_or(0),
        round_error,
    );

    Ok(TurnOutcome {
        should_break: false,
        sent_complete: false,
    })
}

// ---- Core pipeline ----

pub(super) async fn run_pipeline(
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    cancel_token: &CancellationToken,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
) -> super::super::Result<bool> {
    let mut sent_complete = false;
    let mut turn_counter: u32 = 0;
    // One-shot sentinel for the max_rounds summary turn (see the guard at the
    // bottom of the loop). Cleared per-run. We also drop any stale
    // `runtime.completion_reason` carried over from a previous run on this
    // session, so a normal completion is never misread as exhaustion (mirrors
    // how `runtime.suspend_reason` is cleared on resume).
    let mut max_rounds_summary_used = false;
    session.metadata.remove("runtime.completion_reason");

    loop {
        refresh_auxiliary_models_for_round(state, config);
        poll_completed_task_evaluation(state).await;
        apply_completed_task_evaluation(session, event_tx, config, state).await;
        if state.task_evaluation.in_flight.is_none() {
            if let Some(request) = state.task_evaluation.queued_request.take() {
                let eval_provider = state
                    .auxiliary_models
                    .fast_model_provider
                    .clone()
                    .unwrap_or_else(|| llm.clone());
                spawn_task_evaluation_request(
                    state,
                    event_tx,
                    request,
                    eval_provider,
                    cancel_token.clone(),
                );
            }
        }
        poll_completed_gold_evaluation(state).await;
        apply_completed_gold_evaluation(session, config, state).await;
        start_queued_gold_evaluation_if_idle(
            state,
            event_tx,
            state
                .auxiliary_models
                .fast_model_provider
                .clone()
                .unwrap_or_else(|| llm.clone()),
            cancel_token.clone(),
        );

        state.runtime_state.round.current_round = turn_counter;

        let round_id = format!("{}-round-{}", state.session_id, turn_counter + 1);
        state.runtime_state.round.last_round_id = Some(round_id.clone());

        // --- Prompt context refresh ---
        let runtime_context = PromptMemoryRuntimeContext {
            llm: state
                .auxiliary_models
                .background_model_provider
                .clone()
                .unwrap_or_else(|| llm.clone()),
            background_model_name: state.auxiliary_models.background_model_name.clone(),
        };
        crate::runtime::runner::round_prelude::refresh_round_prompt_context(
            session,
            config.prompt_memory_flags,
            Some(&runtime_context),
        )
        .await;

        // --- Task round state ---
        if let Some(ctx) = state.task_context.as_mut() {
            ctx.current_round = turn_counter;
            ctx.max_rounds = config.max_rounds as u32;
        }

        // --- Debug log ---
        if state.debug_logger.enabled {
            tracing::debug!(
                "[{}] round_start: {}",
                state.session_id,
                serde_json::json!({
                    "round": turn_counter + 1,
                    "total_rounds": config.max_rounds,
                    "message_count": session.messages.len(),
                })
            );
        }

        // --- Runner progress event ---
        let _ = event_tx
            .send(AgentEvent::RunnerProgress {
                session_id: state.session_id.clone(),
                round_count: turn_counter,
            })
            .await;

        // --- Merge any queued injected messages from send_message ---
        state_bridge::merge_pending_injected_messages(
            session,
            config.storage.as_ref(),
            config.persistence.as_ref(),
        )
        .await;

        // --- Cancellation check ---
        if cancel_token.is_cancelled() {
            crate::runtime::runner::metrics_lifecycle::record_session_cancelled(
                state.metrics_collector.as_ref(),
                &state.session_id,
                session.messages.len() as u32,
            );
            // Abort any in-flight Gold/Task eval before returning: this early exit
            // skips the post-loop drain, so without this the handle would be
            // dropped (detached, not aborted) and the eval would keep running its
            // LLM request to completion — wasted spend + a late event onto the
            // already-ended stream (issue #347).
            abort_in_flight_evaluations(state);
            return Err(AgentError::Cancelled);
        }

        // --- Metrics: round started ---
        crate::runtime::runner::metrics_lifecycle::record_round_started(
            state.metrics_collector.as_ref(),
            &round_id,
            &state.session_id,
            &state.model_name,
        );

        // --- Resolve tool schemas ---
        let tool_schemas =
            resolve_available_tool_schemas_for_session(config, tools.as_ref(), session);

        // --- LLM call with retry ---
        let mut overflow_recovery_attempted = false;
        let mut turn_outcome: Option<TurnOutcome> = None;
        let mut terminal_error: Option<AgentError> = None;

        for attempt in 1..=MAX_LLM_TURN_ATTEMPTS {
            let llm_output = match crate::runtime::runner::round_lifecycle::execute_llm_round(
                session,
                config,
                &llm,
                event_tx,
                cancel_token,
                &state.session_id,
                &state.model_name,
                &tool_schemas,
            )
            .await
            {
                Ok(output) => output,
                Err(error) => {
                    if is_overflow_recoverable(&error) && !overflow_recovery_attempted {
                        overflow_recovery_attempted = true;
                        if !state.overflow_recovery.can_attempt_recovery() {
                            let breaker_error = AgentError::LLMOverflow(format!(
                                "overflow recovery circuit breaker opened after {} consecutive recoveries",
                                state.overflow_recovery.consecutive_recoveries
                            ));
                            tracing::error!(
                                "[{}] Turn {} overflow recovery skipped by circuit breaker: {}",
                                state.session_id,
                                turn_counter + 1,
                                breaker_error,
                            );
                            terminal_error = Some(breaker_error);
                            break;
                        }

                        tracing::warn!(
                            "[{}] Turn {} detected overflow error (attempt {}/{}): {}. Trying forced overflow recovery.",
                            state.session_id,
                            turn_counter + 1,
                            attempt,
                            MAX_LLM_TURN_ATTEMPTS,
                            error,
                        );
                        let recovered =
                            match crate::runtime::runner::round_lifecycle::force_overflow_context_recovery(
                                session,
                                config,
                                &state.model_name,
                                &state.session_id,
                                &llm,
                                Some(event_tx),
                            )
                            .await
                            {
                                Ok(recovered) => recovered,
                                Err(error) => {
                                    // Early exit before the post-loop drain — abort
                                    // any in-flight eval so it does not detach and
                                    // keep spending (issue #347).
                                    abort_in_flight_evaluations(state);
                                    return Err(error);
                                }
                            };
                        if recovered {
                            state
                                .overflow_recovery
                                .record_recovery(turn_counter as usize);
                            tracing::info!(
                                "[{}] Overflow recovery applied: total_recoveries={}, consecutive_recoveries={}, turn={}",
                                state.session_id,
                                state.overflow_recovery.total_recoveries,
                                state.overflow_recovery.consecutive_recoveries,
                                turn_counter + 1,
                            );
                            let tool_schemas_after_recovery =
                                resolve_available_tool_schemas_for_session(
                                    config,
                                    tools.as_ref(),
                                    session,
                                );
                            match crate::runtime::runner::round_lifecycle::execute_llm_round(
                                session,
                                config,
                                &llm,
                                event_tx,
                                cancel_token,
                                &state.session_id,
                                &state.model_name,
                                &tool_schemas_after_recovery,
                            )
                            .await
                            {
                                Ok(output) => output,
                                Err(recovery_error) => {
                                    tracing::error!(
                                        "[{}] Turn {} overflow recovery retry failed: {}",
                                        state.session_id,
                                        turn_counter + 1,
                                        recovery_error,
                                    );
                                    terminal_error = Some(recovery_error);
                                    break;
                                }
                            }
                        } else {
                            tracing::error!(
                                "[{}] Turn {} overflow recovery was attempted but no compression was applied.",
                                state.session_id,
                                turn_counter + 1,
                            );
                            terminal_error = Some(error);
                            break;
                        }
                    } else if should_retry_turn_error(&error) && attempt < MAX_LLM_TURN_ATTEMPTS {
                        let delay_ms = LLM_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1));
                        tracing::warn!(
                            "[{}] Turn {} LLM call failed (attempt {}/{}): {}. Retrying in {}ms",
                            state.session_id,
                            turn_counter + 1,
                            attempt,
                            MAX_LLM_TURN_ATTEMPTS,
                            error,
                            delay_ms
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    } else {
                        tracing::error!(
                            "[{}] Turn {} LLM call failed terminally (attempt {}/{}): {}",
                            state.session_id,
                            turn_counter + 1,
                            attempt,
                            MAX_LLM_TURN_ATTEMPTS,
                            error,
                        );
                        terminal_error = Some(error);
                        break;
                    }
                }
            };

            // --- Handle LLM output ---
            let stream_output = llm_output.stream_output;

            if stream_output.tool_calls.is_empty() {
                // Safety net: if the model is about to finish but left background
                // children running without waiting on them, suspend instead of
                // completing so their results are collected.
                if let Some(suspend) =
                    maybe_suspend_for_orphaned_children(session, config, &mut state.runtime_state)
                        .await
                {
                    turn_outcome = Some(suspend);
                    break;
                }
                // Safety net (issue #84 Phase 2b): if the model is about to finish
                // but left a `run_in_background` Bash shell still running for this
                // session, suspend instead of completing so background output is not
                // silently dropped. Independent of the children gate; runs only when
                // children did not already suspend this pass.
                if let Some(suspend) =
                    maybe_suspend_for_outstanding_bash(session, config, &mut state.runtime_state)
                        .await
                {
                    turn_outcome = Some(suspend);
                    break;
                }
                // Terminal handling for a no-tool-calls round. The Gold
                // goal-continuation gate is evaluated FIRST inside
                // `handle_no_tool_calls`; the adversarial guardian review gate
                // only runs once Gold decides to STOP, so a premature terminal
                // (goal not met) loops on a continuation without spending a
                // guardian review on incomplete work (issue #343).
                let reasoning_present = !stream_output.reasoning_content.trim().is_empty();
                // Only carry the signature alongside actual reasoning text — a
                // signature with no text would be a nonsensical replay target
                // (#524).
                let reasoning_signature = reasoning_present
                    .then_some(stream_output.reasoning_signature.clone())
                    .flatten();
                let reasoning = reasoning_present.then_some(stream_output.reasoning_content);
                let eval_model = state
                    .auxiliary_models
                    .fast_model_name
                    .clone()
                    .unwrap_or_else(|| state.model_name.clone());
                turn_outcome = Some(
                    handle_no_tool_calls(
                        stream_output.content,
                        reasoning,
                        reasoning_signature,
                        llm_output.prompt_tokens,
                        llm_output.completion_tokens,
                        llm_output.round_usage,
                        session,
                        &mut state.runtime_state,
                        event_tx,
                        state.metrics_collector.as_ref(),
                        &round_id,
                        &state.session_id,
                        config,
                        &state.task_context,
                        &eval_model,
                        turn_counter + 1,
                        llm.clone(),
                    )
                    .await,
                );
                break;
            }

            let frame = crate::runtime::runner::round_frame::RoundFrame {
                session_id: &state.session_id,
                round_id: &round_id,
                turn: turn_counter as usize,
                debug_enabled: state.debug_logger.enabled,
                event_tx,
                metrics_collector: state.metrics_collector.as_ref(),
                config,
                llm: &llm,
                tools: &tools,
            };

            match handle_tool_calls_path(
                &frame,
                stream_output,
                llm_output.round_usage,
                session,
                &state.auxiliary_models,
                &state.model_name,
                &mut state.task_context,
                cancel_token,
            )
            .await
            {
                Ok(outcome) => {
                    turn_outcome = Some(outcome);
                    break;
                }
                Err(error) => {
                    if should_retry_turn_error(&error) && attempt < MAX_LLM_TURN_ATTEMPTS {
                        let delay_ms = LLM_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1));
                        tracing::warn!(
                            "[{}] Turn {} post-LLM handling failed (attempt {}/{}): {}. Retrying in {}ms",
                            state.session_id,
                            turn_counter + 1,
                            attempt,
                            MAX_LLM_TURN_ATTEMPTS,
                            error,
                            delay_ms
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    }

                    tracing::error!(
                        "[{}] Turn {} post-LLM handling failed terminally (attempt {}/{}): {}",
                        state.session_id,
                        turn_counter + 1,
                        attempt,
                        MAX_LLM_TURN_ATTEMPTS,
                        error,
                    );
                    terminal_error = Some(error);
                    break;
                }
            }
        }

        // --- Handle terminal error ---
        if let Some(error) = terminal_error {
            record_turn_failure(
                state.metrics_collector.as_ref(),
                &round_id,
                &state.session_id,
                session.messages.len() as u32,
                &error,
            );
            // Early exit before the post-loop drain — abort in-flight evals so a
            // terminal error does not leave an eval detached and spending (#347).
            abort_in_flight_evaluations(state);
            return Err(error);
        }

        let Some(outcome) = turn_outcome else {
            let error = AgentError::LLM(format!(
                "[{}] turn {} completed without outcome",
                state.session_id,
                turn_counter + 1
            ));
            record_turn_failure(
                state.metrics_collector.as_ref(),
                &round_id,
                &state.session_id,
                session.messages.len() as u32,
                &error,
            );
            // Early exit before the post-loop drain — abort in-flight evals (#347).
            abort_in_flight_evaluations(state);
            return Err(error);
        };

        // --- Overflow recovery state ---
        if !overflow_recovery_attempted {
            state.overflow_recovery.reset_after_stable_round();
        }

        state.runtime_state.memory.overflow_recovery_total =
            state.overflow_recovery.total_recoveries as u32;
        state.runtime_state.memory.overflow_recovery_consecutive =
            state.overflow_recovery.consecutive_recoveries as u32;

        match session
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str)
        {
            Some("awaiting_clarification") => {
                state.runtime_state.status = AgentStatusState::Suspended;
                state.runtime_state.suspension = Some(SuspensionState {
                    reason: "awaiting_clarification".to_string(),
                    suspended_at: Utc::now(),
                    resumable: true,
                    hook_point: Some("AfterToolExecution".to_string()),
                });
            }
            Some("awaiting_parent_approval") => {
                // Phase 2: a CHILD suspended while its gated tool awaits the
                // PARENT's approval. Resumable — the parent's decision sets the
                // re-execute marker and resumes this child via the same path as
                // `awaiting_clarification`.
                state.runtime_state.status = AgentStatusState::Suspended;
                state.runtime_state.suspension = Some(SuspensionState {
                    reason: "awaiting_parent_approval".to_string(),
                    suspended_at: Utc::now(),
                    resumable: true,
                    hook_point: Some("AfterToolExecution".to_string()),
                });
            }
            Some("waiting_for_children") => {
                state.runtime_state.status = AgentStatusState::Suspended;
                state.runtime_state.suspension = Some(SuspensionState {
                    reason: "waiting_for_children".to_string(),
                    suspended_at: Utc::now(),
                    resumable: true,
                    hook_point: Some("AfterToolExecution".to_string()),
                });

                // The SubAgent adapter registers durable wait details against the
                // persisted parent while the runner still owns this local session
                // snapshot. Merge those details before final save so we do not
                // clobber them when this suspended runner tears down.
                if let Some(storage) = config.storage.as_ref() {
                    if let Ok(Some(persisted)) = storage.load_session(&state.session_id).await {
                        if let Some(runtime_state) = persisted.agent_runtime_state {
                            state.runtime_state.waiting_for_children =
                                runtime_state.waiting_for_children;
                        }

                        // If a very fast child completed before this suspended
                        // parent runner finished saving, the coordinator may have
                        // already appended a hidden runtime resume message. Preserve
                        // it so finalization does not overwrite the pending resume.
                        let existing_ids: std::collections::HashSet<String> = session
                            .messages
                            .iter()
                            .map(|message| message.id.clone())
                            .collect();
                        let mut appended = 0usize;
                        for message in persisted.messages {
                            let hidden_runtime_resume = message
                                .metadata
                                .as_ref()
                                .and_then(|metadata| metadata.get("runtime_kind"))
                                .and_then(|value| value.as_str())
                                // Preserve BOTH the generic child-completion resume
                                // and the guardian review resume: a fast guardian
                                // child can append its verdict message before this
                                // suspended runner's final (message-overwriting)
                                // save lands, and the verdict/findings must not be
                                // dropped.
                                .is_some_and(|kind| {
                                    matches!(
                                        kind,
                                        "child_completion_resume" | "guardian_review_resume"
                                    )
                                });
                            if hidden_runtime_resume && !existing_ids.contains(message.id.as_str())
                            {
                                session.messages.push(message);
                                appended += 1;
                            }
                        }
                        if appended > 0 {
                            tracing::info!(
                                "[{}] Preserved {} hidden child-completion resume message(s) during parent suspension save",
                                state.session_id,
                                appended
                            );
                        }
                    }
                }
            }
            Some("waiting_for_bash") => {
                state.runtime_state.status = AgentStatusState::Suspended;
                state.runtime_state.suspension = Some(SuspensionState {
                    reason: "waiting_for_bash".to_string(),
                    suspended_at: Utc::now(),
                    resumable: true,
                    hook_point: Some("AfterToolExecution".to_string()),
                });

                // Defensive mirror of the `waiting_for_children` arm: the bash
                // suspend is single-writer (suspend_to_wait_for_bash already set
                // and persisted `waiting_for_bash`), but load the persisted record
                // so a concurrent/external update is never clobbered, and preserve
                // any hidden runtime resume message the Phase 2c bash coordinator
                // may have appended before this suspended runner's final save.
                if let Some(storage) = config.storage.as_ref() {
                    if let Ok(Some(persisted)) = storage.load_session(&state.session_id).await {
                        if let Some(runtime_state) = persisted.agent_runtime_state {
                            // Nit 1: only merge when the persisted record actually
                            // carries a bash wait — a failed earlier persist can
                            // leave a stale `None`, and overwriting the in-memory
                            // `Some` with it would silently drop the wait.
                            if runtime_state.waiting_for_bash.is_some() {
                                state.runtime_state.waiting_for_bash =
                                    runtime_state.waiting_for_bash;
                            }
                        }

                        let existing_ids: std::collections::HashSet<String> = session
                            .messages
                            .iter()
                            .map(|message| message.id.clone())
                            .collect();
                        let mut appended = 0usize;
                        for message in persisted.messages {
                            let hidden_runtime_resume = message
                                .metadata
                                .as_ref()
                                .and_then(|metadata| metadata.get("runtime_kind"))
                                .and_then(|value| value.as_str())
                                .is_some_and(|kind| {
                                    kind == crate::runtime::config::BASH_COMPLETION_RESUME_KIND
                                });
                            if hidden_runtime_resume && !existing_ids.contains(message.id.as_str())
                            {
                                session.messages.push(message);
                                appended += 1;
                            }
                        }
                        if appended > 0 {
                            tracing::info!(
                                "[{}] Preserved {} hidden bash-completion resume message(s) during suspension save",
                                state.session_id,
                                appended
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        state_bridge::write_runtime_state(session, &state.runtime_state);

        sent_complete = sent_complete || outcome.sent_complete;
        if outcome.should_break {
            break;
        }

        if let Err(error) = spawn_task_evaluation_if_needed(
            turn_counter as usize,
            session,
            event_tx,
            config,
            state,
            state
                .auxiliary_models
                .fast_model_provider
                .clone()
                .unwrap_or_else(|| llm.clone()),
            cancel_token.clone(),
        ) {
            tracing::warn!(
                "[{}] Failed to spawn async task evaluation after round {}: {}",
                state.session_id,
                turn_counter + 1,
                error
            );
        }
        if let Err(error) = spawn_gold_evaluation_if_needed(
            turn_counter as usize,
            session,
            event_tx,
            config,
            state,
            state
                .auxiliary_models
                .fast_model_provider
                .clone()
                .unwrap_or_else(|| llm.clone()),
            cancel_token.clone(),
        ) {
            tracing::warn!(
                "[{}] Failed to spawn async Gold evaluation after round {}: {}",
                state.session_id,
                turn_counter + 1,
                error
            );
        }

        turn_counter += 1;

        // --- Guard against max_rounds (issue #29) ---
        //
        // Hitting the round budget must be DISTINGUISHABLE from a normal
        // completion, not silent. On exhaustion we:
        //   1. stamp `runtime.completion_reason` = "max_rounds_reached"
        //      (mirroring the `runtime.suspend_reason` convention) so the
        //      finalize/Complete path — and the UI reading session metadata —
        //      can tell exhaustion apart from real success;
        //   2. log a tracing::warn!;
        //   3. inject a VISIBLE user-facing notification explaining the stop.
        //
        // We also grant the model EXACTLY ONE final turn to summarize. The
        // local `max_rounds_summary_used` sentinel makes this strictly
        // one-shot: the first guard hit injects the summary prompt and continues
        // for a single extra round; the next time this guard fires we break
        // unconditionally — regardless of what that turn did (including ignoring
        // the instruction and emitting more tool calls). It can therefore never
        // recurse or extend the loop indefinitely.
        if turn_counter >= config.max_rounds as u32 {
            if !max_rounds_summary_used {
                tracing::warn!(
                    "[{}] Reached max rounds ({}) — granting one summary turn before stopping.",
                    state.session_id,
                    config.max_rounds
                );
                session.metadata.insert(
                    "runtime.completion_reason".to_string(),
                    "max_rounds_reached".to_string(),
                );
                // Single visible user turn that both notifies the user WHY the
                // run stopped and prompts the model to summarize. It MUST be one
                // message: two consecutive user messages would violate strict
                // role alternation (Anthropic 400s on it), breaking the summary
                // turn and the next resume. One user turn keeps alternation valid
                // (a preceding Tool message is merged into it by the serializer).
                session.add_message(Message::user(format!(
                    "Reached the maximum of {0} rounds; the task was stopped before \
                     completion. Stop working now and summarize your progress so far \
                     and what remains.",
                    config.max_rounds
                )));
                max_rounds_summary_used = true;
                continue;
            }

            tracing::warn!(
                "[{}] Reached max rounds ({}) — stopping the run before completion.",
                state.session_id,
                config.max_rounds
            );
            break;
        }
    }

    drain_in_flight_task_evaluation(state).await;
    apply_completed_task_evaluation(session, event_tx, config, state).await;
    // A task evaluation may have been queued during the final round but never
    // spawned because the in-flight slot was still busy when the loop ended.
    // Run it now (spawn + drain) so the last round's progress is actually
    // evaluated instead of being silently dropped — this also makes the
    // between-rounds refresh behavior deterministic regardless of scheduling.
    if state.task_evaluation.in_flight.is_none() {
        if let Some(request) = state.task_evaluation.queued_request.take() {
            let eval_provider = state
                .auxiliary_models
                .fast_model_provider
                .clone()
                .unwrap_or_else(|| llm.clone());
            spawn_task_evaluation_request(
                state,
                event_tx,
                request,
                eval_provider,
                cancel_token.clone(),
            );
            drain_in_flight_task_evaluation(state).await;
            apply_completed_task_evaluation(session, event_tx, config, state).await;
        }
    }
    drain_in_flight_gold_evaluation(state).await;
    apply_completed_gold_evaluation(session, config, state).await;

    Ok(sent_complete)
}

/// Heuristic task complexity classification based on tool call names.
///
/// This is used when `features.dynamic_model_routing` is enabled but
/// `MiniLoopExecutor` is not wired through the runner.
fn heuristic_complexity(
    tool_calls: &[bamboo_agent_core::tools::ToolCall],
) -> crate::runtime::complexity_classifier::TaskComplexity {
    use crate::runtime::complexity_classifier::TaskComplexity;

    let simple_tools = ["Read", "Glob", "Grep", "Bash"];
    let complex_tools = ["Agent", "SubAgent", "TodoWrite"];

    let names: Vec<&str> = tool_calls
        .iter()
        .map(|tc| tc.function.name.as_str())
        .collect();

    if names.iter().any(|n| complex_tools.contains(n)) {
        return TaskComplexity::Complex;
    }

    if names.iter().all(|n| simple_tools.contains(n)) && !names.is_empty() {
        return TaskComplexity::Simple;
    }

    TaskComplexity::Standard
}

#[cfg(test)]
mod tests {
    use super::super::startup::OverflowRecoveryState;
    use super::{
        build_guardian_review_prompt, is_overflow_recoverable, is_terminal_child_status,
        map_turn_error_status, maybe_spawn_guardian_review, maybe_suspend_for_orphaned_children,
        maybe_suspend_for_outstanding_bash, should_retry_turn_error, suspend_to_wait_for_bash,
    };
    use crate::runtime::config::{AgentLoopConfig, GuardianConfig, GuardianSpawner};
    use crate::runtime::goal_state::{
        ensure_goal_state, read_goal_state, write_goal_state, GoalDeclaredStatus, GoalRuntimeStatus,
    };
    use crate::runtime::guardian_state::{
        ensure_guardian_state, read_guardian_state, write_guardian_state, GuardianPhase,
        GuardianVerdict,
    };
    use crate::runtime::runner::state_bridge;
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::{AgentError, AgentEvent, Message, Session};
    use bamboo_domain::AgentRuntimeState;
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use bamboo_metrics::{
        RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
        TokenUsage as MetricsTokenUsage,
    };
    use futures::stream;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// A guardian spawner stub that returns a canned child id without touching
    /// any real spawn machinery — lets the gate's state machine be unit-tested.
    struct MockGuardianSpawner {
        child_id: String,
    }
    #[async_trait::async_trait]
    impl GuardianSpawner for MockGuardianSpawner {
        async fn spawn_guardian_review(
            &self,
            _parent_session: &Session,
            _review_prompt: String,
            _model: String,
            _disabled_tools: Option<std::collections::BTreeSet<String>>,
        ) -> Result<String, String> {
            Ok(self.child_id.clone())
        }
    }

    /// An `AgentLoopConfig` with the guardian gate enabled and a mock spawner.
    fn guardian_enabled_config(max_reviews: u32) -> AgentLoopConfig {
        let spawner: Arc<dyn GuardianSpawner> = Arc::new(MockGuardianSpawner {
            child_id: "guardian-child".to_string(),
        });
        AgentLoopConfig {
            guardian_config: Some(GuardianConfig {
                enabled: true,
                model_name: Some("guardian-test-model".to_string()),
                max_reviews,
            }),
            guardian_spawner: Some(spawner),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn guardian_gate_spawns_and_suspends_on_first_terminal() {
        let mut session = Session::new("s1", "model");
        let config = guardian_enabled_config(2);
        let mut runtime_state = AgentRuntimeState::new("s1".to_string());

        let outcome =
            maybe_spawn_guardian_review(&mut session, &config, &None, &mut runtime_state, 1, None)
                .await
                .expect("guardian should engage a review and suspend");

        assert!(outcome.should_break && !outcome.sent_complete);
        assert!(runtime_state.waiting_for_children.is_some());
        let guardian_state = read_guardian_state(&session).expect("guardian state persisted");
        assert_eq!(guardian_state.phase, GuardianPhase::Pending);
        assert_eq!(
            guardian_state.guardian_child_id.as_deref(),
            Some("guardian-child")
        );
        assert_eq!(guardian_state.review_count, 1);
    }

    #[tokio::test]
    async fn guardian_gate_inert_without_config() {
        let mut session = Session::new("s1", "model");
        let config = AgentLoopConfig::default(); // no guardian config / spawner
        let mut runtime_state = AgentRuntimeState::new("s1".to_string());
        assert!(maybe_spawn_guardian_review(
            &mut session,
            &config,
            &None,
            &mut runtime_state,
            1,
            None
        )
        .await
        .is_none());
        assert!(runtime_state.waiting_for_children.is_none());
    }

    #[tokio::test]
    async fn guardian_gate_skips_when_no_model_resolves() {
        // Guardian enabled + spawner wired, but no reviewer model anywhere
        // (guardian_config.model_name None AND AgentLoopConfig.model_name None).
        let spawner: Arc<dyn GuardianSpawner> = Arc::new(MockGuardianSpawner {
            child_id: "guardian-child".to_string(),
        });
        let config = AgentLoopConfig {
            guardian_config: Some(GuardianConfig {
                enabled: true,
                model_name: None,
                max_reviews: 2,
            }),
            guardian_spawner: Some(spawner),
            ..Default::default()
        };
        let mut session = Session::new("s1", "model");
        let mut runtime_state = AgentRuntimeState::new("s1".to_string());
        // Skip the review (no spawn, no suspend) rather than spawning a reviewer
        // with an empty model id; the budget is NOT charged.
        assert!(maybe_spawn_guardian_review(
            &mut session,
            &config,
            &None,
            &mut runtime_state,
            1,
            None
        )
        .await
        .is_none());
        assert!(runtime_state.waiting_for_children.is_none());
        assert!(
            read_guardian_state(&session).is_none(),
            "no guardian review budget should be charged when skipped"
        );
    }

    #[tokio::test]
    async fn guardian_gate_completes_after_approval() {
        let mut session = Session::new("s1", "model");
        let mut guardian_state = ensure_guardian_state(&session);
        guardian_state.record_spawn("guardian-child");
        guardian_state.record_verdict(GuardianVerdict::approved(), 1);
        write_guardian_state(&mut session, guardian_state);

        let config = guardian_enabled_config(2);
        let mut runtime_state = AgentRuntimeState::new("s1".to_string());
        // Reviewed + approved → allow completion (no suspend, no re-spawn).
        assert!(maybe_spawn_guardian_review(
            &mut session,
            &config,
            &None,
            &mut runtime_state,
            2,
            None
        )
        .await
        .is_none());
        assert!(runtime_state.waiting_for_children.is_none());
    }

    #[tokio::test]
    async fn guardian_gate_re_reviews_after_reject_then_completes_on_budget() {
        let mut session = Session::new("s1", "model");
        // One review already done and rejected; budget 2 → a re-review is allowed.
        let mut guardian_state = ensure_guardian_state(&session);
        guardian_state.record_spawn("guardian-child");
        guardian_state.record_verdict(GuardianVerdict::rejected(vec!["bug".to_string()]), 1);
        write_guardian_state(&mut session, guardian_state);

        let config = guardian_enabled_config(2);
        let mut runtime_state = AgentRuntimeState::new("s1".to_string());
        let outcome =
            maybe_spawn_guardian_review(&mut session, &config, &None, &mut runtime_state, 2, None)
                .await
                .expect("rejected within budget → re-review (suspend)");
        assert!(outcome.should_break && !outcome.sent_complete);
        let after = read_guardian_state(&session).expect("state persisted");
        assert_eq!(after.review_count, 2, "second review spawned");
        assert_eq!(after.phase, GuardianPhase::Pending);

        // The second review also rejects, exhausting the budget → completion.
        let mut exhausted = ensure_guardian_state(&session);
        exhausted.record_verdict(GuardianVerdict::rejected(vec!["still".to_string()]), 3);
        write_guardian_state(&mut session, exhausted);
        let mut runtime_state2 = AgentRuntimeState::new("s1".to_string());
        assert!(
            maybe_spawn_guardian_review(&mut session, &config, &None, &mut runtime_state2, 4, None)
                .await
                .is_none(),
            "budget exhausted → allow completion despite unresolved findings"
        );
    }

    /// Minimal provider for terminal-gate tests. Never actually invoked when Gold
    /// is disabled (the gate short-circuits before any LLM call).
    struct StubProvider;

    #[async_trait::async_trait]
    impl LLMProvider for StubProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }
    }

    /// Provider that returns a `report_gold_evaluation` tool call so the terminal
    /// gate inside `handle_no_tool_calls` can be driven end to end.
    struct ScriptedGoldProvider {
        decision: &'static str,
        confidence: &'static str,
    }

    #[async_trait::async_trait]
    impl LLMProvider for ScriptedGoldProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let arguments = format!(
                r#"{{"decision":"{}","confidence":"{}","reasoning":"gate test"}}"#,
                self.decision, self.confidence
            );
            let call = bamboo_agent_core::tools::ToolCall {
                id: "gold-call-1".to_string(),
                tool_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionCall {
                    name: "report_gold_evaluation".to_string(),
                    arguments,
                },
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![call])),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    fn gold_continue_config() -> crate::runtime::config::AgentLoopConfig {
        crate::runtime::config::AgentLoopConfig {
            gold_config: Some(crate::runtime::config::GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("finish the task".to_string()),
                max_auto_continuations: 3,
                ..crate::runtime::config::GoldConfig::default()
            }),
            ..crate::runtime::config::AgentLoopConfig::default()
        }
    }

    fn round_usage() -> MetricsTokenUsage {
        MetricsTokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
        }
    }

    /// THE bug-fix invariant: when Gold decides to continue at the terminal
    /// point, the runner must NOT emit `Complete` (which closes the SSE stream
    /// and locks the frontend). Instead it injects a hidden continuation message
    /// and keeps looping.
    #[tokio::test]
    async fn no_tool_calls_does_not_complete_when_gold_continues() {
        let mut session = Session::new("session-1", "model");
        let mut runtime_state = AgentRuntimeState::new("session-1".to_string());
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        let outcome = super::handle_no_tool_calls(
            "tentative answer".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "session-1",
            &gold_continue_config(),
            &None,
            "model",
            1,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "high",
            }),
        )
        .await;

        // The run keeps going: no break, no terminal Complete.
        assert!(!outcome.should_break);
        assert!(!outcome.sent_complete);

        // Assistant message + hidden gold continuation message were appended.
        assert_eq!(session.messages.len(), 2);
        let last = session.messages.last().unwrap();
        assert!(matches!(last.role, bamboo_agent_core::Role::User));
        let metadata = last.metadata.as_ref().expect("runtime metadata");
        assert_eq!(
            metadata.get("runtime_kind").and_then(|v| v.as_str()),
            Some("goal_continue")
        );

        // Drain events: a Gold evaluation was emitted, but NO Complete.
        drop(tx);
        let mut saw_complete = false;
        while let Some(event) = rx.recv().await {
            if matches!(event, AgentEvent::Complete { .. }) {
                saw_complete = true;
            }
        }
        assert!(
            !saw_complete,
            "Complete must not be emitted on gold continue"
        );
    }

    /// Counterpart: when Gold reports the goal achieved, the run completes
    /// normally with a single terminal `Complete`.
    #[tokio::test]
    async fn no_tool_calls_completes_when_gold_achieved() {
        let mut session = Session::new("session-1", "model");
        let mut runtime_state = AgentRuntimeState::new("session-1".to_string());
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        let outcome = super::handle_no_tool_calls(
            "final answer".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "session-1",
            &gold_continue_config(),
            &None,
            "model",
            1,
            Arc::new(ScriptedGoldProvider {
                decision: "achieved",
                confidence: "high",
            }),
        )
        .await;

        assert!(outcome.should_break);
        assert!(outcome.sent_complete);
        // Only the assistant message — no hidden continuation injected.
        assert_eq!(session.messages.len(), 1);

        drop(tx);
        let mut saw_complete = false;
        while let Some(event) = rx.recv().await {
            if matches!(event, AgentEvent::Complete { .. }) {
                saw_complete = true;
            }
        }
        assert!(
            saw_complete,
            "Complete must be emitted when gold is achieved"
        );
    }

    /// End-to-end goal loop across multiple terminal rounds:
    /// 1. The agent finishes prematurely (no tool calls) without declaring done.
    ///    The side-channel double-check says "continue" → the loop VETOES the
    ///    stop, persists the verdict, and injects the completion-audit prompt.
    /// 2. The agent does the work and declares completion via `update_goal`
    ///    (simulated here through the same `goal_state` API the tool's post-exec
    ///    handler uses).
    /// 3. On the next terminal round the double-check confirms ("achieved") →
    ///    the run stops with exactly one terminal `Complete` and status Complete,
    ///    and both double-check verdicts are persisted in the goal's eval trail.
    #[tokio::test]
    async fn e2e_goal_loop_continue_then_declare_then_complete() {
        let mut session = Session::new("session-e2e", "model");
        let config = gold_continue_config();
        let mut runtime_state = AgentRuntimeState::new("session-e2e".to_string());
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);

        // --- Round 1: premature finish, undeclared, judge says continue ---
        let r1 = super::handle_no_tool_calls(
            "I think that's everything.".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "session-e2e",
            &config,
            &None,
            "model",
            1,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "high",
            }),
        )
        .await;
        assert!(!r1.should_break, "undeclared + continue → keep working");
        assert!(!r1.sent_complete);

        let st = read_goal_state(&session).expect("goal state persisted after round 1");
        assert_eq!(st.continuation_count, 1);
        assert_eq!(st.status, GoalRuntimeStatus::Active);
        assert_eq!(st.eval_history.len(), 1);
        assert!(session
            .messages
            .last()
            .unwrap()
            .content
            .contains("update_goal"));

        // --- Agent declares completion via update_goal (post-exec handler) ---
        let mut st = ensure_goal_state(&session, "finish the task");
        st.declare(GoalDeclaredStatus::Complete, 2);
        write_goal_state(&mut session, st);

        // --- Round 2: declared complete, judge confirms "achieved" → stop ---
        let r2 = super::handle_no_tool_calls(
            "Done — shipped and verified.".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-2",
            "session-e2e",
            &config,
            &None,
            "model",
            2,
            Arc::new(ScriptedGoldProvider {
                decision: "achieved",
                confidence: "high",
            }),
        )
        .await;
        assert!(r2.should_break, "declared complete + achieved → stop");
        assert!(r2.sent_complete);

        let st = read_goal_state(&session).expect("goal state persisted after round 2");
        assert_eq!(st.status, GoalRuntimeStatus::Complete);
        assert_eq!(st.declared_status, None, "declaration cleared after acting");
        assert_eq!(st.eval_history.len(), 2, "both double-checks persisted");

        drop(tx);
        let mut completes = 0;
        while let Some(event) = rx.recv().await {
            if matches!(event, AgentEvent::Complete { .. }) {
                completes += 1;
            }
        }
        assert_eq!(
            completes, 1,
            "exactly one terminal Complete across the whole loop"
        );
    }

    /// The double-check must be able to VETO a premature `update_goal(complete)`:
    /// the agent declared done, but the evaluator confidently says continue.
    #[tokio::test]
    async fn e2e_goal_loop_double_check_vetoes_premature_complete() {
        let mut session = Session::new("session-e2e2", "model");
        let config = gold_continue_config();
        let mut runtime_state = AgentRuntimeState::new("session-e2e2".to_string());
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        // Agent prematurely declares completion.
        let mut st = ensure_goal_state(&session, "finish the task");
        st.declare(GoalDeclaredStatus::Complete, 1);
        write_goal_state(&mut session, st);

        let outcome = super::handle_no_tool_calls(
            "All done!".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "session-e2e2",
            &config,
            &None,
            "model",
            1,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "high",
            }),
        )
        .await;

        assert!(!outcome.should_break, "premature completion vetoed");
        assert!(!outcome.sent_complete);
        let st = read_goal_state(&session).expect("goal state persisted");
        assert_eq!(st.status, GoalRuntimeStatus::Active);
        assert_eq!(
            st.declared_status, None,
            "stale declaration cleared on veto"
        );
        assert_eq!(st.continuation_count, 1);
    }

    // ---- Gold-then-guardian gate ordering (issue #343) ----

    /// An `AgentLoopConfig` with BOTH the autonomous goal loop and the guardian
    /// review gate active — the overlap issue #343 reorders.
    fn guardian_and_gold_config(max_reviews: u32) -> crate::runtime::config::AgentLoopConfig {
        let spawner: Arc<dyn GuardianSpawner> = Arc::new(MockGuardianSpawner {
            child_id: "guardian-child".to_string(),
        });
        crate::runtime::config::AgentLoopConfig {
            gold_config: Some(crate::runtime::config::GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("finish the task".to_string()),
                max_auto_continuations: 3,
                ..crate::runtime::config::GoldConfig::default()
            }),
            guardian_config: Some(GuardianConfig {
                enabled: true,
                model_name: Some("guardian-test-model".to_string()),
                max_reviews,
            }),
            guardian_spawner: Some(spawner),
            ..crate::runtime::config::AgentLoopConfig::default()
        }
    }

    /// THE ordering fix (issue #343): with BOTH a guardian and an autonomous goal
    /// loop configured, a premature terminal — the model stops emitting tool calls
    /// but the goal is NOT met, so Gold decides CONTINUE — must inject a
    /// continuation and keep working WITHOUT spawning a guardian review of the
    /// incomplete state. Before the fix the guardian gate ran first and would have
    /// spawned a review + suspended here, burning its bounded budget (and a
    /// suspend/resume cycle) on work the goal loop already knew was unfinished —
    /// and, once approved, would never re-review the truly-final state.
    #[tokio::test]
    async fn gold_continue_skips_guardian_review() {
        let mut session = Session::new("s343-continue", "model");
        let config = guardian_and_gold_config(2);
        let mut runtime_state = AgentRuntimeState::new("s343-continue".to_string());
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let outcome = super::handle_no_tool_calls(
            "tentative — I think that's everything".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "s343-continue",
            &config,
            &None,
            "model",
            1,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "high",
            }),
        )
        .await;

        // The run keeps working: no break, no terminal Complete.
        assert!(!outcome.should_break);
        assert!(!outcome.sent_complete);

        // The guardian was NOT engaged: no suspend and no review budget charged.
        assert!(
            runtime_state.waiting_for_children.is_none(),
            "a premature terminal must NOT suspend on a guardian review",
        );
        assert!(
            read_guardian_state(&session).is_none(),
            "no guardian review budget may be spent before the goal is met",
        );

        // A hidden continuation was injected after the assistant message.
        assert_eq!(session.messages.len(), 2);
        let last = session.messages.last().unwrap();
        assert_eq!(
            last.metadata
                .as_ref()
                .and_then(|m| m.get("runtime_kind"))
                .and_then(|v| v.as_str()),
            Some("goal_continue"),
        );
    }

    /// Counterpart to [`gold_continue_skips_guardian_review`]: once Gold decides
    /// STOP (the goal is met), the guardian reviews the genuinely-final state —
    /// spawning a reviewer child and suspending the run on its verdict rather than
    /// completing outright.
    #[tokio::test]
    async fn gold_stop_reaches_guardian_review_on_final_state() {
        let mut session = Session::new("s343-stop", "model");
        let config = guardian_and_gold_config(2);
        // The agent declared completion; the double-check confirms "achieved", so
        // the goal gate decides STOP.
        let mut goal = ensure_goal_state(&session, "finish the task");
        goal.declare(GoalDeclaredStatus::Complete, 1);
        write_goal_state(&mut session, goal);
        let mut runtime_state = AgentRuntimeState::new("s343-stop".to_string());
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let outcome = super::handle_no_tool_calls(
            "Done — shipped and verified.".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "s343-stop",
            &config,
            &None,
            "model",
            1,
            Arc::new(ScriptedGoldProvider {
                decision: "achieved",
                confidence: "high",
            }),
        )
        .await;

        // The guardian engaged: the run suspended on the reviewer verdict instead
        // of emitting a terminal Complete.
        assert!(outcome.should_break);
        assert!(
            !outcome.sent_complete,
            "Gold STOP must reach the guardian and suspend, not complete outright",
        );
        assert!(
            runtime_state.waiting_for_children.is_some(),
            "the guardian must review the final state and suspend on its verdict",
        );
        let guardian = read_guardian_state(&session).expect("guardian state persisted");
        assert_eq!(guardian.phase, GuardianPhase::Pending);
        assert_eq!(guardian.review_count, 1);
    }

    /// Full-loop e2e through `run_pipeline`, exercising the REAL wiring:
    /// the model calls the `update_goal` tool (round 1) → it is dispatched by the
    /// builtin executor → the post-exec handler records the declaration into the
    /// durable goal state → on the next terminal round the side-channel
    /// double-check confirms achievement → the run stops as Complete.
    ///
    /// The scripted provider distinguishes main-agent calls (`request_purpose =
    /// "agent_loop"`) from the Gold double-check (`"gold_evaluation"`).
    struct GoalLoopE2eProvider {
        main_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl LLMProvider for GoalLoopE2eProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&bamboo_llm::LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let purpose = options
                .and_then(|o| o.request_purpose.as_deref())
                .unwrap_or("agent_loop");

            if purpose == "gold_evaluation" {
                // The double-check confirms the goal is achieved.
                let call = bamboo_agent_core::tools::ToolCall {
                    id: "gold-1".to_string(),
                    tool_type: "function".to_string(),
                    function: bamboo_agent_core::tools::FunctionCall {
                        name: "report_gold_evaluation".to_string(),
                        arguments: r#"{"decision":"achieved","confidence":"high","reasoning":"objective verified"}"#.to_string(),
                    },
                };
                return Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::ToolCalls(vec![call])),
                    Ok(LLMChunk::Done),
                ])));
            }

            // Main agent rounds.
            let n = self
                .main_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                // Round 1: declare completion via the update_goal tool.
                let call = bamboo_agent_core::tools::ToolCall {
                    id: "ug-1".to_string(),
                    tool_type: "function".to_string(),
                    function: bamboo_agent_core::tools::FunctionCall {
                        name: "update_goal".to_string(),
                        arguments: r#"{"status":"complete"}"#.to_string(),
                    },
                };
                Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::ToolCalls(vec![call])),
                    Ok(LLMChunk::Done),
                ])))
            } else {
                // Round 2: finish with a plain message (no tool calls) → terminal.
                Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::Token("Done — shipped and verified.".to_string())),
                    Ok(LLMChunk::Done),
                ])))
            }
        }
    }

    fn e2e_loop_state(
        session_id: &str,
    ) -> crate::runtime::runner::loop_execution::startup::LoopRunState {
        use crate::runtime::runner::loop_execution::startup::{
            GoldEvaluationState, LoopRunState, OverflowRecoveryState, TaskEvaluationState,
        };
        LoopRunState {
            session_id: session_id.to_string(),
            model_name: "model".to_string(),
            metrics_collector: None,
            debug_logger: crate::runtime::runner::logging::DebugLogger::new(false),
            task_context: None,
            overflow_recovery: OverflowRecoveryState::default(),
            task_evaluation: TaskEvaluationState::default(),
            gold_evaluation: GoldEvaluationState {
                in_flight: None,
                completed: None,
                queued_request: None,
            },
            auxiliary_models: crate::runtime::config::AuxiliaryModelConfig::default(),
            runtime_state: AgentRuntimeState::new(session_id),
        }
    }

    #[tokio::test]
    async fn e2e_full_loop_update_goal_tool_then_double_check_completes() {
        use crate::runtime::config::PromptMemoryFlags;

        let mut session = Session::new("session-full-e2e", "model");
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let llm: Arc<dyn LLMProvider> = Arc::new(GoalLoopE2eProvider {
            main_calls: std::sync::atomic::AtomicUsize::new(0),
        });
        // The real builtin executor — it registers and dispatches `update_goal`.
        let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
            Arc::new(bamboo_tools::BuiltinToolExecutor::new());

        let config = AgentLoopConfig {
            gold_config: Some(crate::runtime::config::GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("ship it".to_string()),
                max_auto_continuations: 3,
                ..crate::runtime::config::GoldConfig::default()
            }),
            // Disable memory/recall injection so the loop makes no auxiliary LLM
            // calls beyond the scripted main + gold ones.
            prompt_memory_flags: PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            model_name: Some("model".to_string()),
            max_rounds: 5,
            ..AgentLoopConfig::default()
        };

        let mut state = e2e_loop_state("session-full-e2e");
        let cancel = tokio_util::sync::CancellationToken::new();

        let sent_complete =
            super::run_pipeline(&mut session, &tx, llm, tools, &cancel, &config, &mut state)
                .await
                .expect("pipeline runs to completion");

        assert!(sent_complete, "the run emits a terminal Complete");

        // The durable goal state reflects the full lifecycle.
        let goal_state = read_goal_state(&session).expect("goal state persisted");
        assert_eq!(goal_state.status, GoalRuntimeStatus::Complete);
        assert_eq!(
            goal_state.declared_status, None,
            "declaration cleared after the terminal gate acted"
        );
        assert!(
            !goal_state.eval_history.is_empty(),
            "the double-check verdict was persisted into the goal's eval trail"
        );

        // Exactly one terminal Complete across the whole loop.
        drop(tx);
        let mut completes = 0;
        while let Some(event) = rx.recv().await {
            if matches!(event, AgentEvent::Complete { .. }) {
                completes += 1;
            }
        }
        assert_eq!(completes, 1, "exactly one terminal Complete");
    }

    /// Always emits a tool call so the loop can never self-terminate — forces the
    /// worst case through the full round budget, including the summary turn.
    struct MaxRoundsProvider {
        main_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl LLMProvider for MaxRoundsProvider {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[bamboo_agent_core::tools::ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }

        async fn chat_stream_with_options(
            &self,
            _: &[Message],
            _: &[bamboo_agent_core::tools::ToolSchema],
            _: Option<u32>,
            _: &str,
            _: Option<&bamboo_llm::LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let n = self
                .main_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let call = bamboo_agent_core::tools::ToolCall {
                id: format!("tool-{n}"),
                tool_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionCall {
                    name: "noop".to_string(),
                    arguments: "{}".to_string(),
                },
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![call])),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    /// Executes any tool call successfully so tool rounds keep progressing.
    struct AlwaysOkExecutor;

    #[async_trait::async_trait]
    impl bamboo_agent_core::tools::ToolExecutor for AlwaysOkExecutor {
        async fn execute(
            &self,
            _call: &bamboo_agent_core::tools::ToolCall,
        ) -> std::result::Result<
            bamboo_agent_core::tools::ToolResult,
            bamboo_agent_core::tools::ToolError,
        > {
            Ok(bamboo_agent_core::tools::ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }

        fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
            Vec::new()
        }
    }

    /// Issue #29: hitting `max_rounds` must be DISTINGUISHABLE, not silent.
    ///
    /// Drives the worst case — a model that keeps emitting tool calls so the loop
    /// can never self-terminate — through a small budget, then asserts:
    ///   (a) the session is stamped `runtime.completion_reason` =
    ///       "max_rounds_reached";
    ///   (b) a visible notification message is appended;
    ///   (c) the model gets EXACTLY ONE summary turn (`max_rounds + 1` total
    ///       model turns) before the loop stops hard — no infinite loop.
    #[tokio::test]
    async fn max_rounds_exhaustion_is_distinguishable_and_runs_one_summary_turn() {
        use crate::runtime::config::PromptMemoryFlags;
        use std::sync::atomic::Ordering;

        const MAX_ROUNDS: usize = 3;
        let mut session = Session::new("session-max-rounds", "model");
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let provider = Arc::new(MaxRoundsProvider {
            main_calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let llm: Arc<dyn LLMProvider> = provider.clone();
        let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = Arc::new(AlwaysOkExecutor);
        let config = AgentLoopConfig {
            max_rounds: MAX_ROUNDS,
            prompt_memory_flags: PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            model_name: Some("model".to_string()),
            ..AgentLoopConfig::default()
        };
        let mut state = e2e_loop_state("session-max-rounds");
        let cancel = tokio_util::sync::CancellationToken::new();

        let sent_complete =
            super::run_pipeline(&mut session, &tx, llm, tools, &cancel, &config, &mut state)
                .await
                .expect("pipeline runs to completion");

        // (a) Distinguishable: session carries the exhaustion reason.
        assert_eq!(
            session
                .metadata
                .get("runtime.completion_reason")
                .map(String::as_str),
            Some("max_rounds_reached"),
            "exhaustion must be stamped in session metadata"
        );
        // (b) Visible notification is present.
        assert!(
            session.messages.iter().any(|m| m.content.contains(
                "Reached the maximum of 3 rounds; the task was stopped before completion."
            )),
            "a visible max_rounds notification message must be appended"
        );
        // (b2) The injected summary turn must NOT create consecutive user
        // messages — that would 400 on strict-alternation providers (Anthropic)
        // and break the very summary turn this feature relies on (#29 review).
        assert!(
            !session
                .messages
                .windows(2)
                .any(|w| w[0].role == bamboo_domain::Role::User
                    && w[1].role == bamboo_domain::Role::User),
            "max_rounds injection must not produce consecutive user messages"
        );
        // (c) Exactly one summary turn, then stops hard (no infinite loop).
        let main_calls = provider.main_calls.load(Ordering::SeqCst);
        assert_eq!(
            main_calls,
            MAX_ROUNDS + 1,
            "exactly one extra summary turn after {MAX_ROUNDS} normal rounds (got {main_calls})"
        );
        // Worst case: the summary turn itself emitted tool calls, so the loop
        // broke via the guard (sent_complete false; finalize emits a zero-token
        // Complete — the exact pre-fix symptom, now made distinguishable above).
        assert!(
            !sent_complete,
            "worst-case summary turn (tool calls) leaves sent_complete false"
        );
        drop(tx);
        let mut completes = 0;
        while let Some(event) = rx.recv().await {
            if matches!(event, AgentEvent::Complete { .. }) {
                completes += 1;
            }
        }
        assert_eq!(
            completes, 0,
            "no Complete emitted during this worst-case run"
        );
    }

    #[derive(Default)]
    struct TestStorage {
        sessions: RwLock<HashMap<String, Session>>,
    }

    #[async_trait::async_trait]
    impl Storage for TestStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.sessions
                .write()
                .await
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.sessions.read().await.get(session_id).cloned())
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            Ok(self.sessions.write().await.remove(session_id).is_some())
        }
    }

    struct TestPersistence(Arc<dyn Storage>);

    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for TestPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.0.save_session(session).await
        }
    }

    #[tokio::test]
    async fn pending_injected_messages_are_merged_once_and_cleared_from_storage() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(TestPersistence(storage.clone()));
        let mut persisted = Session::new_child("child-merge", "parent", "model", "Child");
        persisted.add_message(Message::system("system"));
        persisted.add_message(Message::user("original task"));
        persisted.metadata.insert(
            "pending_injected_messages".to_string(),
            serde_json::json!([
                {
                    "content": "queued correction",
                    "created_at": chrono::Utc::now(),
                }
            ])
            .to_string(),
        );
        storage
            .save_session(&persisted)
            .await
            .expect("persisted child should be saved");

        let mut running = persisted.clone();
        running.metadata.remove("pending_injected_messages");

        state_bridge::merge_pending_injected_messages(
            &mut running,
            Some(&storage),
            Some(&persistence),
        )
        .await;

        assert_eq!(
            running
                .messages
                .last()
                .map(|message| message.content.as_str()),
            Some("queued correction")
        );
        assert!(!running.metadata.contains_key("pending_injected_messages"));
        let saved = storage
            .load_session("child-merge")
            .await
            .expect("load should succeed")
            .expect("session should exist");
        assert!(!saved.metadata.contains_key("pending_injected_messages"));

        let count_after_first_merge = running.messages.len();
        state_bridge::merge_pending_injected_messages(
            &mut running,
            Some(&storage),
            Some(&persistence),
        )
        .await;
        assert_eq!(running.messages.len(), count_after_first_merge);
    }

    // --- Tests from rounds.rs ---

    #[test]
    fn retries_transient_llm_errors() {
        assert!(should_retry_turn_error(&AgentError::LLM(
            "HTTP error: timeout while connecting".to_string(),
        )));
        assert!(should_retry_turn_error(&AgentError::LLM(
            "API error: HTTP 503: Service Unavailable".to_string(),
        )));
        assert!(should_retry_turn_error(&AgentError::LLM(
            "empty assistant response".to_string(),
        )));
    }

    #[test]
    fn retries_reqwest_transport_errors() {
        assert!(should_retry_turn_error(&AgentError::LLM(
            "HTTP error: error sending request for url (https://api.githubcopilot.com/chat/completions)".to_string(),
        )));
    }

    #[test]
    fn retries_stream_decode_transport_errors() {
        assert!(should_retry_turn_error(&AgentError::LLM(
            "Stream error: Transport error: error decoding response body".to_string(),
        )));
    }

    #[test]
    fn retries_unknown_llm_errors_by_default() {
        assert!(should_retry_turn_error(&AgentError::LLM(
            "some completely unknown error".to_string(),
        )));
    }

    #[test]
    fn does_not_retry_non_retryable_llm_errors() {
        assert!(!should_retry_turn_error(&AgentError::LLM(
            "Authentication error: Invalid API key".to_string(),
        )));
        assert!(!should_retry_turn_error(&AgentError::LLM(
            "API error: HTTP 400: invalid request".to_string(),
        )));
    }

    #[test]
    fn does_not_retry_non_llm_errors() {
        assert!(!should_retry_turn_error(&AgentError::Cancelled));
        assert!(!should_retry_turn_error(&AgentError::Tool(
            "tool failed".to_string(),
        )));
        assert!(!should_retry_turn_error(&AgentError::Budget(
            "budget exceeded".to_string(),
        )));
    }

    #[test]
    fn does_not_retry_empty_llm_error() {
        assert!(!should_retry_turn_error(&AgentError::LLM("".to_string())));
        assert!(!should_retry_turn_error(&AgentError::LLM(
            "   ".to_string()
        )));
    }

    #[test]
    fn overflow_errors_use_dedicated_recovery_path() {
        assert!(is_overflow_recoverable(&AgentError::LLMOverflow(
            "prompt too long".to_string(),
        )));
        assert!(!is_overflow_recoverable(&AgentError::LLM(
            "timeout while connecting".to_string(),
        )));
        assert!(!should_retry_turn_error(&AgentError::LLMOverflow(
            "maximum context length exceeded".to_string(),
        )));
    }

    #[test]
    fn overflow_recovery_state_opens_circuit_breaker_after_threshold() {
        let mut state = OverflowRecoveryState::default();
        assert!(state.can_attempt_recovery());
        state.record_recovery(0);
        state.record_recovery(1);
        state.record_recovery(2);
        assert!(!state.can_attempt_recovery());
    }

    // --- Tests from round_error.rs ---

    #[test]
    fn test_map_turn_error_status_cancelled() {
        let error = AgentError::Cancelled;
        let (round_status, session_status) = map_turn_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Cancelled);
        assert_eq!(session_status, MetricsSessionStatus::Cancelled);
    }

    #[test]
    fn test_map_turn_error_status_tool_error() {
        let error = AgentError::Tool("Tool failed".to_string());
        let (round_status, session_status) = map_turn_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_turn_error_status_llm_error() {
        let error = AgentError::LLM("LLM provider error".to_string());
        let (round_status, session_status) = map_turn_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_turn_error_status_session_not_found() {
        let error = AgentError::SessionNotFound("session-123".to_string());
        let (round_status, session_status) = map_turn_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_turn_error_status_budget_error() {
        let error = AgentError::Budget("Budget exceeded".to_string());
        let (round_status, session_status) = map_turn_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_turn_error_status_cancelled_is_distinct() {
        let cancelled_error = AgentError::Cancelled;
        let other_error = AgentError::Tool("Tool error".to_string());

        let (cancelled_round, cancelled_session) = map_turn_error_status(&cancelled_error);
        let (other_round, other_session) = map_turn_error_status(&other_error);

        assert_ne!(cancelled_round, other_round);
        assert_ne!(cancelled_session, other_session);
    }

    #[test]
    fn test_map_turn_error_only_cancelled_gets_cancelled_status() {
        let errors = vec![
            AgentError::LLM("error".to_string()),
            AgentError::Tool("error".to_string()),
            AgentError::SessionNotFound("id".to_string()),
            AgentError::Budget("error".to_string()),
        ];

        for error in errors {
            let (round_status, session_status) = map_turn_error_status(&error);
            assert_eq!(round_status, MetricsRoundStatus::Error);
            assert_eq!(session_status, MetricsSessionStatus::Error);
        }

        let (round_status, session_status) = map_turn_error_status(&AgentError::Cancelled);
        assert_eq!(round_status, MetricsRoundStatus::Cancelled);
        assert_eq!(session_status, MetricsSessionStatus::Cancelled);
    }

    // --- Tests from round_flow/no_tool_calls.rs ---

    #[tokio::test]
    async fn handle_no_tool_calls_emits_complete_and_appends_assistant_message() {
        let mut session = Session::new("session-1", "model");
        let mut runtime_state = AgentRuntimeState::new("session-1".to_string());
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);

        let outcome = super::handle_no_tool_calls(
            "final answer".to_string(),
            Some("reasoning trace".to_string()),
            Some("sig_captured_by_anthropic".to_string()), // reasoning_signature (#524)
            11,
            7,
            MetricsTokenUsage {
                prompt_tokens: 11,
                completion_tokens: 7,
                total_tokens: 18,
            },
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "session-1",
            &crate::runtime::config::AgentLoopConfig::default(),
            &None,
            "model",
            1,
            Arc::new(StubProvider),
        )
        .await;

        assert!(outcome.should_break);
        assert!(outcome.sent_complete);
        assert_eq!(session.messages.len(), 1);
        assert!(matches!(
            session.messages[0].role,
            bamboo_agent_core::Role::Assistant
        ));
        assert_eq!(session.messages[0].content, "final answer");
        assert_eq!(
            session.messages[0].reasoning.as_deref(),
            Some("reasoning trace")
        );
        assert_eq!(
            session.messages[0].reasoning_signature.as_deref(),
            Some("sig_captured_by_anthropic"),
            "captured signature must thread through to the persisted message (#524)"
        );

        let event = rx.recv().await.expect("complete event should be sent");
        match event {
            AgentEvent::Complete { usage } => {
                assert_eq!(usage.prompt_tokens, 11);
                assert_eq!(usage.completion_tokens, 7);
                assert_eq!(usage.total_tokens, 18);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_completed_task_evaluation_updates_task_list_and_emits_event() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(TestPersistence(storage.clone()));
        let mut session = Session::new("session-task-eval", "model");
        session.set_task_list(bamboo_domain::TaskList {
            session_id: "session-task-eval".to_string(),
            title: "Eval Tasks".to_string(),
            items: vec![bamboo_domain::TaskItem {
                id: "task-1".to_string(),
                description: "Do work".to_string(),
                status: bamboo_domain::TaskItemStatus::InProgress,
                ..bamboo_domain::TaskItem::default()
            }],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        });
        session
            .metadata
            .insert("task_list_version".to_string(), "1".to_string());

        let mut state = super::super::startup::LoopRunState {
            session_id: "session-task-eval".to_string(),
            model_name: "model".to_string(),
            metrics_collector: None,
            debug_logger: crate::runtime::runner::logging::DebugLogger::new(false),
            task_context: crate::runtime::task_context::TaskLoopContext::from_session(&session),
            overflow_recovery: super::super::startup::OverflowRecoveryState::default(),
            task_evaluation: super::super::startup::TaskEvaluationState {
                in_flight: None,
                completed: Some(
                    crate::runtime::runner::task_lifecycle::AsyncTaskEvaluationResult {
                        shared_session_id: "session-task-eval".to_string(),
                        round_number: 1,
                        based_on_task_context_version: 1,
                        task_list_title: Some("Eval Tasks".to_string()),
                        model_name: "fast-model".to_string(),
                        evaluation_result: crate::runtime::task_evaluation::TaskEvaluationResult {
                            needs_evaluation: true,
                            updates: vec![crate::runtime::task_evaluation::TaskItemUpdate {
                                item_id: "task-1".to_string(),
                                status: bamboo_domain::TaskItemStatus::Completed,
                                notes: Some("done".to_string()),
                                evidence: Some("verified".to_string()),
                                blocker: None,
                                criteria_met: None,
                            }],
                            reasoning: "complete".to_string(),
                            prompt_tokens: 4,
                            completion_tokens: 2,
                        },
                    },
                ),
                queued_request: None,
            },
            gold_evaluation: super::super::startup::GoldEvaluationState::default(),
            auxiliary_models: crate::runtime::config::AuxiliaryModelConfig::default(),
            runtime_state: AgentRuntimeState::new("session-task-eval"),
        };
        let config = crate::runtime::config::AgentLoopConfig {
            storage: Some(storage.clone()),
            persistence: Some(persistence),
            ..Default::default()
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        super::apply_completed_task_evaluation(&mut session, &tx, &config, &mut state).await;

        assert_eq!(
            session.task_list.as_ref().unwrap().items[0].status,
            bamboo_domain::TaskItemStatus::Completed
        );
        let event = rx
            .recv()
            .await
            .expect("task update event should be emitted");
        match event {
            AgentEvent::TaskListUpdated { task_list } => {
                assert_eq!(
                    task_list.items[0].status,
                    bamboo_domain::TaskItemStatus::Completed
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    // --- Tests from round_prelude/round_state.rs ---

    #[test]
    fn test_build_round_id() {
        let id = format!("{}-round-{}", "session-123", 1);
        assert_eq!(id, "session-123-round-1");

        let id = format!("{}-round-{}", "test", 4 + 1);
        assert_eq!(id, "test-round-5");
    }

    // --- Tests from round_prelude/cancellation.rs ---

    #[tokio::test]
    async fn ensure_not_cancelled_returns_ok_when_not_cancelled() {
        let token = tokio_util::sync::CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_returns_error_when_cancelled() {
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    // --- Tests from round_flow/tool_calls/usage.rs ---

    #[test]
    fn accumulate_round_usage_saturates_components_and_recomputes_total() {
        let mut usage = MetricsTokenUsage {
            prompt_tokens: u64::MAX - 5,
            completion_tokens: u64::MAX - 9,
            total_tokens: 0,
        };
        let delta = MetricsTokenUsage {
            prompt_tokens: 10,
            completion_tokens: 20,
            total_tokens: 30,
        };

        usage.prompt_tokens = usage.prompt_tokens.saturating_add(delta.prompt_tokens);
        usage.completion_tokens = usage
            .completion_tokens
            .saturating_add(delta.completion_tokens);
        usage.recompute_total();

        assert_eq!(usage.prompt_tokens, u64::MAX);
        assert_eq!(usage.completion_tokens, u64::MAX);
        assert_eq!(usage.total_tokens, u64::MAX);
    }

    // ── End-of-turn safety net (auto-wait on orphaned children) ──────────

    #[test]
    fn is_terminal_child_status_classifies_correctly() {
        for s in ["completed", "error", "timeout", "cancelled", "skipped"] {
            assert!(is_terminal_child_status(s), "{s} should be terminal");
        }
        for s in ["running", "pending", "queued", ""] {
            assert!(!is_terminal_child_status(s), "{s} should be active");
        }
    }

    /// Storage whose child index is configurable, for the safety-net tests.
    struct ChildIndexStorage {
        inner: Arc<TestStorage>,
        children: Vec<(String, Option<String>)>,
    }

    #[async_trait::async_trait]
    impl Storage for ChildIndexStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.inner.save_session(session).await
        }
        async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
            self.inner.load_session(id).await
        }
        async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
            self.inner.delete_session(id).await
        }
        async fn list_child_run_statuses(
            &self,
            _parent: &str,
        ) -> std::io::Result<Vec<(String, Option<String>)>> {
            Ok(self.children.clone())
        }
    }

    fn config_with_storage(storage: Arc<dyn Storage>) -> AgentLoopConfig {
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(TestPersistence(storage.clone()));
        AgentLoopConfig {
            storage: Some(storage),
            persistence: Some(persistence),
            ..AgentLoopConfig::default()
        }
    }

    #[tokio::test]
    async fn safety_net_suspends_on_orphaned_active_children() {
        let inner = Arc::new(TestStorage::default());
        let storage: Arc<dyn Storage> = Arc::new(ChildIndexStorage {
            inner: inner.clone(),
            children: vec![
                ("c-run".into(), Some("running".into())),
                ("c-pend".into(), None),
                ("c-done".into(), Some("completed".into())),
            ],
        });
        let config = config_with_storage(storage.clone());
        let mut session = Session::new("parent-orphan", "model");
        let mut runtime_state = AgentRuntimeState::new("parent-orphan");

        let outcome =
            maybe_suspend_for_orphaned_children(&mut session, &config, &mut runtime_state)
                .await
                .expect("must suspend when active children remain");
        assert!(outcome.should_break && !outcome.sent_complete);

        let wait = runtime_state
            .waiting_for_children
            .expect("durable wait registered");
        // Only the non-terminal children, sorted/deduped.
        assert_eq!(
            wait.child_session_ids,
            vec!["c-pend".to_string(), "c-run".to_string()]
        );
        assert_eq!(
            session
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("waiting_for_children")
        );
        // Persisted so the coordinator can resume it.
        let persisted = storage
            .load_session("parent-orphan")
            .await
            .unwrap()
            .unwrap();
        assert!(persisted
            .agent_runtime_state
            .and_then(|s| s.waiting_for_children)
            .is_some());
    }

    #[tokio::test]
    async fn safety_net_noop_when_all_children_terminal() {
        let inner = Arc::new(TestStorage::default());
        let storage: Arc<dyn Storage> = Arc::new(ChildIndexStorage {
            inner,
            children: vec![
                ("a".into(), Some("completed".into())),
                ("b".into(), Some("error".into())),
            ],
        });
        let config = config_with_storage(storage);
        let mut session = Session::new("parent-done", "model");
        let mut runtime_state = AgentRuntimeState::new("parent-done");

        assert!(
            maybe_suspend_for_orphaned_children(&mut session, &config, &mut runtime_state)
                .await
                .is_none(),
            "no active children → must not suspend"
        );
        assert!(runtime_state.waiting_for_children.is_none());
    }

    #[tokio::test]
    async fn safety_net_noop_when_already_waiting() {
        // A model that DID call wait already has waiting_for_children set; the
        // safety net must not touch it (and must not even query storage).
        let storage: Arc<dyn Storage> = Arc::new(ChildIndexStorage {
            inner: Arc::new(TestStorage::default()),
            children: vec![("x".into(), Some("running".into()))],
        });
        let config = config_with_storage(storage);
        let mut session = Session::new("parent-waiting", "model");
        let mut runtime_state = AgentRuntimeState::new("parent-waiting");
        runtime_state.waiting_for_children = Some(super::WaitingForChildrenState {
            child_session_ids: vec!["x".into()],
            wait_for: super::ChildWaitPolicy::All,
            registered_at: chrono::Utc::now(),
            timeout_at: None,
            registered_by_tool_call_id: None,
        });

        assert!(
            maybe_suspend_for_orphaned_children(&mut session, &config, &mut runtime_state)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn suspend_to_wait_for_bash_sets_reason_and_state() {
        // The suspend primitive must register the durable bash wait, stamp the
        // `runtime.suspend_reason` discriminant, and break the turn without
        // sending complete — mirroring suspend_to_wait_for_children. No
        // persistence is exercised here (None), keeping the test harness-free.
        let mut session = Session::new("s-bash", "model");
        let mut runtime_state = AgentRuntimeState::new("s-bash");

        let outcome = suspend_to_wait_for_bash(
            &mut session,
            &mut runtime_state,
            None,
            vec!["bg-1".to_string(), "bg-2".to_string()],
        )
        .await;

        assert!(outcome.should_break, "must break the turn");
        assert!(!outcome.sent_complete, "must not send complete");

        let wait = runtime_state
            .waiting_for_bash
            .expect("durable bash wait should be registered");
        assert_eq!(wait.bash_ids, vec!["bg-1".to_string(), "bg-2".to_string()]);
        assert_eq!(
            session
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("waiting_for_bash"),
            "metadata reason must match the discriminant arm"
        );
    }

    #[tokio::test]
    async fn bash_safety_net_noop_when_already_waiting() {
        // A session already registered a bash wait must not re-suspend (and must
        // not even query the global shell registry), mirroring the children
        // safety net's already-waiting guard. This is the deterministic guard
        // path that does not depend on the process-global registry.
        let config = AgentLoopConfig::default();
        let mut session = Session::new("s-bash-waiting", "model");
        let mut runtime_state = AgentRuntimeState::new("s-bash-waiting");
        runtime_state.waiting_for_bash = Some(super::WaitingForBashState {
            bash_ids: vec!["bg-1".to_string()],
            registered_at: chrono::Utc::now(),
            timeout_at: None,
        });

        assert!(
            maybe_suspend_for_outstanding_bash(&mut session, &config, &mut runtime_state)
                .await
                .is_none(),
            "must not re-suspend when a bash wait is already registered"
        );
    }

    // ── Bash self-resume liveness tests (issue #84 Phase 2b) ──────────────

    struct StubBashPersistence;
    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for StubBashPersistence {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RecordingBashResumeHook {
        calls: Arc<std::sync::Mutex<Vec<(String, Vec<String>)>>>,
    }
    impl crate::runtime::config::BashResumeHook for RecordingBashResumeHook {
        fn arrange_bash_self_resume(&self, session_id: String, bash_ids: Vec<String>) {
            self.calls
                .lock()
                .expect("hook mutex")
                .push((session_id, bash_ids));
        }
    }

    struct NoopBashResumeHook;
    impl crate::runtime::config::BashResumeHook for NoopBashResumeHook {
        fn arrange_bash_self_resume(&self, _: String, _: Vec<String>) {}
    }

    #[tokio::test]
    async fn bash_gate_arranges_self_resume_hook_on_suspend() {
        // Liveness proof (Blocker 2): when the gate suspends for outstanding
        // background bash, it MUST arrange a self-resume hook so the session
        // is always eventually resumed — no suspend-forever.
        let session_id = "s-bash-liveness";
        let mut config = AgentLoopConfig::default();
        config.persistence = Some(Arc::new(StubBashPersistence));
        let hook = RecordingBashResumeHook {
            calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        config.bash_resume_hook = Some(Arc::new(hook.clone()));

        let shell = bamboo_tools::tools::bash_runtime::spawn_background(
            "sleep 5",
            None,
            None,
            Some(session_id.to_string()),
            false,
            None,
        )
        .await
        .expect("spawn");

        let mut session = Session::new(session_id, "model");
        let mut runtime_state = AgentRuntimeState::new(session_id);
        let outcome =
            maybe_suspend_for_outstanding_bash(&mut session, &config, &mut runtime_state).await;
        let _ = shell.kill().await; // clean up first

        assert!(
            outcome.is_some(),
            "gate should suspend with a running shell"
        );
        assert!(
            runtime_state.waiting_for_bash.is_some(),
            "durable wait registered"
        );
        let calls = hook.calls.lock().expect("hook calls");
        assert_eq!(calls.len(), 1, "hook called exactly once");
        assert_eq!(calls[0].0, session_id);
        assert!(!calls[0].1.is_empty(), "hook received bash ids");
    }

    #[tokio::test]
    async fn bash_gate_no_suspend_when_all_shells_finished() {
        // Blocker 1: if all captured shells finish before the gate commits, the
        // gate returns None — no suspend-forever on a lost-wakeup.
        let session_id = "s-bash-toctou";
        let mut config = AgentLoopConfig::default();
        config.persistence = Some(Arc::new(StubBashPersistence));
        config.bash_resume_hook = Some(Arc::new(NoopBashResumeHook));

        let shell = bamboo_tools::tools::bash_runtime::spawn_background(
            "true",
            None,
            None,
            Some(session_id.to_string()),
            false,
            None,
        )
        .await
        .expect("spawn");

        // Wait for the shell to finish (bounded so the test never hangs).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if shell.status() != "running" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("test shell did not finish in 5s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let mut session = Session::new(session_id, "model");
        let mut runtime_state = AgentRuntimeState::new(session_id);
        let outcome =
            maybe_suspend_for_outstanding_bash(&mut session, &config, &mut runtime_state).await;

        assert!(
            outcome.is_none(),
            "must not suspend when no shells are running"
        );
        assert!(
            runtime_state.waiting_for_bash.is_none(),
            "no bash wait registered"
        );
    }

    #[tokio::test]
    async fn bash_suspend_reason_matches_suspended_discriminant() {
        // Should-fix 2: the suspend_reason literal set by the write site
        // (suspend_to_wait_for_bash) MUST resolve to Suspended status in the
        // discriminant match — a future typo in either side is caught here.
        let mut session = Session::new("s-discriminant", "model");
        let mut runtime_state = AgentRuntimeState::new("s-discriminant");
        suspend_to_wait_for_bash(
            &mut session,
            &mut runtime_state,
            None,
            vec!["bg-1".to_string()],
        )
        .await;

        let reason = session
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str);
        assert_eq!(reason, Some("waiting_for_bash"));

        // Mirrors the discriminant arms in run_pipeline. If the write site's
        // literal were changed, the assert_eq! above catches it. If a match arm
        // were renamed, this matches! fails — the reason would fall through to
        // the inert `_ => {}` and the session would wrongly complete.
        let produces_suspended = matches!(
            reason,
            Some("awaiting_clarification")
                | Some("awaiting_parent_approval")
                | Some("waiting_for_children")
                | Some("waiting_for_bash")
        );
        assert!(
            produces_suspended,
            "waiting_for_bash must be Suspended-producing"
        );
    }

    // ── Cancel-during-tool-execution (issue #30) ─────────────────────────
    //
    // The loop previously only checked cancellation BETWEEN rounds, so a cancel
    // issued *during* a long-running tool (up to parallel_batch_timeout_secs =
    // 300s, or per_tool_timeout_secs for a single tool like a 120s Bash command)
    // was ignored until the tool finished — the agent looked unresponsive to
    // cancel for up to minutes. The fix wraps the tool-execution await in
    // `handle_tool_calls_path` with a biased `select!` on the cancel token
    // (mirroring `stream/handler/consume.rs`). On cancel the in-flight tool
    // futures are dropped; the `Cancelled` error reuses the loop's existing
    // cancel classification (`map_turn_error_status`), so no new flow is added.

    use super::handle_tool_calls_path;
    use crate::runtime::runner::round_frame::RoundFrame;
    use crate::runtime::runner::tool_execution::execute_round_tool_calls;
    use crate::runtime::stream::handler::StreamHandlingOutput;
    use crate::runtime::task_context::TaskLoopContext;
    use bamboo_agent_core::tools::{
        FunctionCall, FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    /// Tool-executor probe. When `block` is set it sleeps far longer than any
    /// test will wait, so cancel must race a genuinely in-flight future (not the
    /// pre-execution setup). It flips `started` the instant execution begins so
    /// the test can fire cancel against real, in-progress execution.
    struct CancelProbeToolExecutor {
        block: bool,
        started: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for CancelProbeToolExecutor {
        async fn execute(
            &self,
            _call: &ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            self.started.store(true, Ordering::SeqCst);
            if self.block {
                // Block far longer than the test will wait. When the biased
                // select! in handle_tool_calls_path drops this future on cancel,
                // the sleep is cancelled cooperatively.
                tokio::time::sleep(Duration::from_secs(120)).await;
            }
            Ok(ToolResult {
                success: true,
                result: "tool-result-123".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "Read".to_string(),
                    description: "read tool".to_string(),
                    parameters: serde_json::json!({ "type": "object", "properties": {} }),
                },
            }]
        }
    }

    fn single_read_call() -> ToolCall {
        ToolCall {
            id: "call-read".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Read".to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    fn stream_output_with_tool_call(call: ToolCall) -> StreamHandlingOutput {
        StreamHandlingOutput {
            response_id: None,
            content: String::new(),
            reasoning_content: String::new(),
            reasoning_signature: None,
            token_count: 0,
            tool_calls: vec![call],
            output_tokens: 0,
            thinking_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            input_tokens: 0,
        }
    }

    #[tokio::test]
    async fn tool_execution_cancel_returns_promptly() {
        // A long-running tool must NOT pin the loop after cancel. The probe
        // sleeps 120s; if cancel isn't honored *during* tool execution this test
        // would block until that sleep (or the batch timeout) — the outer
        // tokio::time::timeout turns that into a fast failure instead of a hang.
        let started = Arc::new(AtomicBool::new(false));
        let tools: Arc<dyn ToolExecutor> = Arc::new(CancelProbeToolExecutor {
            block: true,
            started: started.clone(),
        });
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(128);
        let llm: Arc<dyn LLMProvider> = Arc::new(StubProvider);
        let config = AgentLoopConfig::default();
        let mut session = Session::new("s-cancel", "model");
        let frame = RoundFrame {
            session_id: "s-cancel",
            round_id: "r1",
            turn: 0,
            debug_enabled: false,
            event_tx: &event_tx,
            metrics_collector: None,
            config: &config,
            llm: &llm,
            tools: &tools,
        };
        let auxiliary_models = crate::runtime::config::AuxiliaryModelConfig::default();
        let mut task_context: Option<TaskLoopContext> = None;
        let cancel_token = CancellationToken::new();

        // Driver: wait until the tool has ACTUALLY started executing, then cancel
        // — guaranteeing cancel races a live in-flight tool, not pre-exec setup.
        let driver_started = started.clone();
        let driver_token = cancel_token.clone();
        let driver = tokio::spawn(async move {
            for _ in 0..500 {
                if driver_started.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            assert!(
                driver_started.load(Ordering::SeqCst),
                "tool never started executing"
            );
            driver_token.cancel();
        });

        let t0 = std::time::Instant::now();
        let result = tokio::time::timeout(
            // Bounded well below the 120s tool sleep so a regression fails fast.
            Duration::from_secs(5),
            handle_tool_calls_path(
                &frame,
                stream_output_with_tool_call(single_read_call()),
                MetricsTokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
                &mut session,
                &auxiliary_models,
                "model",
                &mut task_context,
                &cancel_token,
            ),
        )
        .await;
        let elapsed = t0.elapsed();
        let _ = driver.await;

        let inner = result.expect(
            "handle_tool_calls_path did not return within 5s — cancel not honored during tool execution",
        );
        assert!(
            matches!(inner, Err(AgentError::Cancelled)),
            "expected Err(AgentError::Cancelled), got {:?}",
            inner.as_ref().err()
        );
        // Cancel must be PROMPT — well under the 120s tool sleep and the 300s
        // batch timeout. `elapsed` is dominated by polling for the tool to start
        // (2ms cadence); cancel propagation itself is sub-millisecond.
        assert!(
            elapsed < Duration::from_secs(2),
            "cancel was not prompt (tool would otherwise block for ~120s): {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn normal_tool_batch_completes_unchanged() {
        // No cancel: the batch must complete normally and record the tool result
        // — byte-identical healthy behavior. Tested at the `execute_round_tool_calls`
        // level (the exact future the select! wraps): its non-cancel arm is
        // literally `result = execute_round_tool_calls(...) => result?`, identical
        // to the previous `.await?`, so a clean healthy completion here proves the
        // wrapper does not perturb the non-cancelled path.
        let tools: Arc<dyn ToolExecutor> = Arc::new(CancelProbeToolExecutor {
            block: false,
            started: Arc::new(AtomicBool::new(false)),
        });
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(128);
        let llm: Arc<dyn LLMProvider> = Arc::new(StubProvider);
        let config = AgentLoopConfig::default();
        let mut session = Session::new("s-normal", "model");
        let frame = RoundFrame {
            session_id: "s-normal",
            round_id: "r1",
            turn: 0,
            debug_enabled: false,
            event_tx: &event_tx,
            metrics_collector: None,
            config: &config,
            llm: &llm,
            tools: &tools,
        };
        let tool_schemas = tools.list_tools();
        let mut task_context: Option<TaskLoopContext> = None;

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_round_tool_calls(
                std::slice::from_ref(&single_read_call()),
                &frame,
                &mut session,
                &mut task_context,
                // No compression model -> mid-turn compression short-circuits, so
                // the healthy path is exercised without any auxiliary LLM call.
                None,
                None,
                &tool_schemas,
            ),
        )
        .await
        .expect("normal tool batch did not complete within 10s");

        let round_result = result.expect("normal batch should return Ok");
        assert!(!round_result.awaiting_clarification);
        assert!(!round_result.waiting_for_children);
        // The tool result must have been recorded as a tool message.
        assert!(
            session
                .messages
                .iter()
                .any(|m| m.role == bamboo_agent_core::Role::Tool
                    && m.content.contains("tool-result-123")),
            "expected a tool-result message, got {} message(s)",
            session.messages.len()
        );
    }

    /// #524: a captured Anthropic signature on the stream output must thread
    /// through `handle_tool_calls_path` onto the persisted assistant message,
    /// alongside the reasoning text it signs.
    #[tokio::test]
    async fn handle_tool_calls_path_persists_reasoning_signature_on_assistant_message() {
        let tools: Arc<dyn ToolExecutor> = Arc::new(CancelProbeToolExecutor {
            block: false,
            started: Arc::new(AtomicBool::new(false)),
        });
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(128);
        let llm: Arc<dyn LLMProvider> = Arc::new(StubProvider);
        let config = AgentLoopConfig::default();
        let mut session = Session::new("s-sig", "model");
        let frame = RoundFrame {
            session_id: "s-sig",
            round_id: "r1",
            turn: 0,
            debug_enabled: false,
            event_tx: &event_tx,
            metrics_collector: None,
            config: &config,
            llm: &llm,
            tools: &tools,
        };
        let auxiliary_models = crate::runtime::config::AuxiliaryModelConfig::default();
        let mut task_context: Option<TaskLoopContext> = None;
        let cancel_token = CancellationToken::new();

        let mut stream_output = stream_output_with_tool_call(single_read_call());
        stream_output.reasoning_content = "Anthropic's own thinking.".to_string();
        stream_output.reasoning_signature = Some("sig_captured_by_anthropic".to_string());

        tokio::time::timeout(
            Duration::from_secs(10),
            handle_tool_calls_path(
                &frame,
                stream_output,
                MetricsTokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
                &mut session,
                &auxiliary_models,
                "model",
                &mut task_context,
                &cancel_token,
            ),
        )
        .await
        .expect("handle_tool_calls_path did not complete within 10s")
        .expect("handle_tool_calls_path should return Ok");

        let assistant_message = session
            .messages
            .iter()
            .find(|m| m.role == bamboo_agent_core::Role::Assistant)
            .expect("assistant message should have been added");
        assert_eq!(
            assistant_message.reasoning.as_deref(),
            Some("Anthropic's own thinking.")
        );
        assert_eq!(
            assistant_message.reasoning_signature.as_deref(),
            Some("sig_captured_by_anthropic"),
            "captured signature must thread through to the persisted message (#524)"
        );
    }

    // ── Mid-turn compression failure is best-effort, not a whole-turn retry (#238)
    //
    // A transient failure in the MID-TURN context-compression summarization call
    // (host summarizer LLM) used to propagate out of `execute_round_tool_calls`
    // via `?`, out of `handle_tool_calls_path`'s `result?`, and into the per-turn
    // retry loop. Because the assistant message (with its `tool_calls`) is
    // appended BEFORE tools run and tools execute one-by-one, that propagation
    // corrupted state: it aborted the not-yet-executed tool calls and — if the
    // error were classified retryable — re-ran the WHOLE turn, appending a SECOND
    // assistant message and re-billing the LLM. The fix makes mid-turn
    // compression infallible (log + degrade): the turn keeps running its
    // remaining tools with the uncompressed context, and the failure never
    // reaches the retry path.

    /// Tool executor that records execution order and forces STRICTLY sequential
    /// scheduling (so the mid-turn compression runs after EACH tool, never
    /// batched — the exact one-by-one path the bug lives in). `compact_context`
    /// is included so its post-execution tool result flips the session's manual
    /// compression flag, deterministically triggering the mid-turn summarization
    /// call without any token-budget arithmetic.
    struct RecordingSequentialExecutor {
        executed: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for RecordingSequentialExecutor {
        async fn execute(
            &self,
            call: &ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            self.executed
                .lock()
                .unwrap()
                .push(call.function.name.clone());
            Ok(ToolResult {
                success: true,
                result: format!("result-of-{}", call.function.name),
                display_preference: None,
                images: Vec::new(),
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            ["compact_context", "tool_b", "tool_c"]
                .iter()
                .map(|name| ToolSchema {
                    schema_type: "function".to_string(),
                    function: FunctionSchema {
                        name: name.to_string(),
                        description: "test tool".to_string(),
                        parameters: serde_json::json!({ "type": "object", "properties": {} }),
                    },
                })
                .collect()
        }

        // Force Sequential scheduling for every tool: Mutating + not
        // concurrency-safe => tools run one-by-one with a compression check
        // interleaved after each, never in a parallel batch.
        fn call_parallel_classification(
            &self,
            _call: &ToolCall,
        ) -> (bamboo_agent_core::tools::ToolMutability, bool) {
            (bamboo_agent_core::tools::ToolMutability::Mutating, false)
        }
    }

    /// Provider whose only job is to FAIL the mid-turn context-compression
    /// summarization call (identified by `request_purpose == "compression"`,
    /// set by `LlmSummarizer`) with a transient upstream error, counting the
    /// attempts. It is never asked to run a main-agent round here
    /// (`handle_tool_calls_path` consumes an already-produced `StreamHandlingOutput`),
    /// so `chat_stream` is a benign stub.
    struct FailingCompressionProvider {
        compression_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl LLMProvider for FailingCompressionProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&bamboo_llm::LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let purpose = options
                .and_then(|o| o.request_purpose.as_deref())
                .unwrap_or("");
            if purpose == "compression" {
                self.compression_calls.fetch_add(1, Ordering::SeqCst);
                // Transient failure: HTTP 500 / rate limit / timeout on the
                // summarization call. This is exactly the class of error the fix
                // downgrades to best-effort.
                return Err(LLMError::Api(
                    "http 500 transient upstream failure (compression summarization)".to_string(),
                ));
            }
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }
    }

    fn tool_call(id: &str, name: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn mid_turn_compression_failure_is_best_effort_and_does_not_retry_turn() {
        let compression_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

        let llm: Arc<dyn LLMProvider> = Arc::new(FailingCompressionProvider {
            compression_calls: compression_calls.clone(),
        });
        let tools: Arc<dyn ToolExecutor> = Arc::new(RecordingSequentialExecutor {
            executed: executed.clone(),
        });
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(128);

        // `background_model_name` set + no explicit summarization provider =>
        // the summarizer runs against `frame.llm` (our failing provider).
        let config = AgentLoopConfig {
            model_name: Some("model".to_string()),
            background_model_name: Some("summarizer".to_string()),
            ..AgentLoopConfig::default()
        };

        let mut session = Session::new("s-compress-fail", "model");
        // Seed enough non-system history that `summary_source_messages` clears the
        // >= 3 message floor once compact_context's result is appended, so the
        // summarization call is genuinely attempted (and fails).
        session.add_message(Message::system("system"));
        session.add_message(Message::user("do the work"));
        session.add_message(Message::assistant("prior assistant turn".to_string(), None));
        session.add_message(Message::user("keep going"));

        let frame = RoundFrame {
            session_id: "s-compress-fail",
            round_id: "r1",
            turn: 0,
            debug_enabled: false,
            event_tx: &event_tx,
            metrics_collector: None,
            config: &config,
            llm: &llm,
            tools: &tools,
        };
        let auxiliary_models = crate::runtime::config::AuxiliaryModelConfig::default();
        let mut task_context: Option<TaskLoopContext> = None;
        let cancel_token = CancellationToken::new();

        // Assistant turn issues three tool calls; the FIRST is `compact_context`,
        // whose post-execution result trips the manual-compression flag so the
        // mid-turn summarization fires right after it — and fails transiently.
        let stream_output = StreamHandlingOutput {
            response_id: None,
            content: String::new(),
            reasoning_content: String::new(),
            reasoning_signature: None,
            token_count: 0,
            tool_calls: vec![
                tool_call("call-compact", "compact_context"),
                tool_call("call-b", "tool_b"),
                tool_call("call-c", "tool_c"),
            ],
            output_tokens: 0,
            thinking_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            input_tokens: 0,
        };

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            handle_tool_calls_path(
                &frame,
                stream_output,
                MetricsTokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
                &mut session,
                &auxiliary_models,
                "model",
                &mut task_context,
                &cancel_token,
            ),
        )
        .await
        .expect("handle_tool_calls_path did not return within 10s");

        // The transient compression failure must NOT surface as a turn error.
        // Without the fix it propagates as Err out of handle_tool_calls_path,
        // which run_pipeline's per-turn retry loop treats as a whole-turn failure
        // (re-appending a duplicate assistant message / re-billing). The retry is
        // gated on this Err, so proving Ok here proves the turn is not retried.
        let _outcome = result.expect(
            "mid-turn compression failure must be best-effort (Ok), not a whole-turn error/retry",
        );

        // The compression path was genuinely exercised and failed — else the test
        // would prove nothing.
        assert!(
            compression_calls.load(Ordering::SeqCst) >= 1,
            "mid-turn compression summarization must have been attempted (and failed)"
        );

        // (a) The turn kept running the REMAINING tools despite the failure — no
        // orphaned tool calls. Without the fix, execution aborts right after
        // compact_context and tool_b / tool_c never run.
        let ran = executed.lock().unwrap().clone();
        assert_eq!(
            ran,
            vec![
                "compact_context".to_string(),
                "tool_b".to_string(),
                "tool_c".to_string(),
            ],
            "all tools must execute in order despite the mid-turn compression failure"
        );

        // (b) Exactly ONE assistant message carries this turn's tool calls — no
        // duplicate from a whole-turn re-run.
        let assistant_turns = session
            .messages
            .iter()
            .filter(|m| {
                m.role == bamboo_agent_core::Role::Assistant
                    && m.tool_calls.as_ref().is_some_and(|calls| {
                        calls.iter().any(|c| c.function.name == "compact_context")
                    })
            })
            .count();
        assert_eq!(
            assistant_turns, 1,
            "exactly one assistant message must exist for the turn (no duplicate)"
        );

        // (c) Each tool produced exactly one tool-result message (no re-execution
        // / no duplicated results from a retried turn).
        for (id, name) in [
            ("call-compact", "compact_context"),
            ("call-b", "tool_b"),
            ("call-c", "tool_c"),
        ] {
            let count = session
                .messages
                .iter()
                .filter(|m| {
                    m.role == bamboo_agent_core::Role::Tool && m.tool_call_id.as_deref() == Some(id)
                })
                .count();
            assert_eq!(count, 1, "tool {name} must have exactly one result message");
        }
    }

    // ── Async Gold/Task eval cancel + abort-on-early-exit (issue #347) ────
    //
    // The runner spawns Gold/Task evaluations as detached tokio tasks and only
    // *drains* (awaits + applies) them on the normal post-loop path. On an early
    // return (cancellation / terminal-error / no-outcome) it used to simply drop
    // the `JoinHandle` — which DETACHES (not aborts) the task, so a run the user
    // cancelled kept running a full LLM eval request to completion (wasted spend)
    // and could fire a late event onto the already-ended stream. The fix threads
    // the run's cancel token into the spawned eval (a `select!` that resolves to
    // `None` on cancel) AND aborts any in-flight handle at every early return.

    /// What the scripted main agent does on its SECOND round, once the Gold
    /// evaluation spawned after round 1 is genuinely in flight.
    #[derive(Clone, Copy)]
    enum SecondRoundBehavior {
        /// Block forever so the runner parks in its cancel-aware LLM stream; the
        /// test then fires `cancel` against a live in-flight eval.
        BlockForever,
        /// Return a non-retryable terminal error so `run_pipeline` takes the
        /// terminal-error early return WITHOUT the cancel token being cancelled —
        /// isolating the `abort_in_flight_evaluations` mechanism (the `select!`
        /// on the cancel token cannot fire here).
        TerminalError,
    }

    /// Round 1 emits a tool call so a tool round runs and, with the Gold loop
    /// enabled, a PostRound Gold evaluation is spawned at the end of the round.
    /// The Gold evaluation flips `gold_started`, then BLOCKS on `release`
    /// (simulating a slow LLM request) and sets `gold_completed` + signals
    /// `finished` ONLY if it is allowed to run past the block — so an aborted /
    /// cancelled eval leaves `gold_completed` false and never signals `finished`.
    struct EvalAbortProbeProvider {
        main_calls: std::sync::atomic::AtomicUsize,
        gold_started: Arc<AtomicBool>,
        gold_completed: Arc<AtomicBool>,
        release: Arc<tokio::sync::Notify>,
        finished: Arc<tokio::sync::Notify>,
        second_round: SecondRoundBehavior,
    }

    #[async_trait::async_trait]
    impl LLMProvider for EvalAbortProbeProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            // The runner dispatches via `chat_stream_ir`, whose default delegates
            // to `chat_stream_with_options`; this plain method is unused here.
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&bamboo_llm::LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let purpose = options
                .and_then(|o| o.request_purpose.as_deref())
                .unwrap_or("agent_loop");

            if purpose == "gold_evaluation" {
                // Genuinely in-flight LLM eval: flag start, then block. On cancel
                // the spawn's `select!` drops this future; on a terminal-error
                // early exit `abort_in_flight_evaluations` aborts the task. Either
                // way the code below `release` never runs.
                self.gold_started.store(true, Ordering::SeqCst);
                self.release.notified().await;
                self.gold_completed.store(true, Ordering::SeqCst);
                self.finished.notify_one();
                let call = bamboo_agent_core::tools::ToolCall {
                    id: "gold-eval-async".to_string(),
                    tool_type: "function".to_string(),
                    function: bamboo_agent_core::tools::FunctionCall {
                        name: "report_gold_evaluation".to_string(),
                        arguments:
                            r#"{"decision":"achieved","confidence":"high","reasoning":"done"}"#
                                .to_string(),
                    },
                };
                return Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::ToolCalls(vec![call])),
                    Ok(LLMChunk::Done),
                ])));
            }

            let n = self
                .main_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                // Round 1: a tool call → a tool round → PostRound Gold eval spawns.
                let call = bamboo_agent_core::tools::ToolCall {
                    id: "noop-1".to_string(),
                    tool_type: "function".to_string(),
                    function: bamboo_agent_core::tools::FunctionCall {
                        name: "noop".to_string(),
                        arguments: "{}".to_string(),
                    },
                };
                return Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::ToolCalls(vec![call])),
                    Ok(LLMChunk::Done),
                ])));
            }

            // Round 2+: wait until the Gold eval is genuinely in flight so the
            // early-exit races a LIVE eval (not an unspawned task), then act.
            for _ in 0..2000 {
                if self.gold_started.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            match self.second_round {
                SecondRoundBehavior::BlockForever => Ok(Box::pin(stream::pending())),
                SecondRoundBehavior::TerminalError => Err(LLMError::Auth(
                    "terminal error injected to exercise #347 abort".to_string(),
                )),
            }
        }
    }

    fn eval_abort_config() -> AgentLoopConfig {
        use crate::runtime::config::PromptMemoryFlags;
        AgentLoopConfig {
            gold_config: Some(crate::runtime::config::GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("ship it".to_string()),
                max_auto_continuations: 3,
                ..crate::runtime::config::GoldConfig::default()
            }),
            prompt_memory_flags: PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            model_name: Some("model".to_string()),
            max_rounds: 5,
            ..AgentLoopConfig::default()
        }
    }

    /// A run the user CANCELS with a Gold evaluation in flight must not run that
    /// eval's LLM request to completion. Drives the real `run_pipeline`: the eval
    /// blocks mid-request, the run is cancelled, and after the pipeline returns
    /// `Cancelled` the eval is released — it must NOT complete (its future was
    /// dropped at the cancel point), so `finished` never fires.
    #[tokio::test]
    async fn cancelled_run_does_not_complete_in_flight_gold_eval() {
        let gold_started = Arc::new(AtomicBool::new(false));
        let gold_completed = Arc::new(AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        let llm: Arc<dyn LLMProvider> = Arc::new(EvalAbortProbeProvider {
            main_calls: std::sync::atomic::AtomicUsize::new(0),
            gold_started: gold_started.clone(),
            gold_completed: gold_completed.clone(),
            release: release.clone(),
            finished: finished.clone(),
            second_round: SecondRoundBehavior::BlockForever,
        });
        let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = Arc::new(AlwaysOkExecutor);
        let config = eval_abort_config();
        let mut session = Session::new("session-eval-cancel", "model");
        let mut state = e2e_loop_state("session-eval-cancel");
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let cancel = CancellationToken::new();

        // Driver: cancel only once the Gold eval is genuinely in flight.
        let driver_started = gold_started.clone();
        let driver_token = cancel.clone();
        let driver = tokio::spawn(async move {
            for _ in 0..2000 {
                if driver_started.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            driver_token.cancel();
        });

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            super::run_pipeline(&mut session, &tx, llm, tools, &cancel, &config, &mut state),
        )
        .await
        .expect("run_pipeline did not return within 5s after cancel");
        let _ = driver.await;

        assert!(
            matches!(result, Err(AgentError::Cancelled)),
            "cancelled run must return Cancelled, got {result:?}"
        );
        assert!(
            gold_started.load(Ordering::SeqCst),
            "the Gold eval must have been genuinely in flight (else nothing was tested)"
        );
        assert!(
            state.gold_evaluation.in_flight.is_none(),
            "the in-flight Gold eval slot must be cleared on the cancel early-exit"
        );

        // Release the eval; a dropped/aborted future can never reach completion,
        // so `finished` must NOT fire.
        release.notify_one();
        let finished_within =
            tokio::time::timeout(Duration::from_millis(500), finished.notified()).await;
        assert!(
            finished_within.is_err(),
            "cancelled Gold eval kept running to completion (spend not stopped)"
        );
        assert!(
            !gold_completed.load(Ordering::SeqCst),
            "cancelled Gold eval must not complete its LLM request"
        );
    }

    /// A run that hits a TERMINAL ERROR with a Gold evaluation in flight must
    /// ABORT that eval on the early return — the cancel token is NOT cancelled
    /// here, so this isolates `abort_in_flight_evaluations` (the `select!` on the
    /// token cannot help). Removing the abort call makes this test fail: the
    /// detached eval would wake on `release` and complete, firing `finished`.
    #[tokio::test]
    async fn terminal_error_aborts_in_flight_gold_eval() {
        let gold_started = Arc::new(AtomicBool::new(false));
        let gold_completed = Arc::new(AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        let llm: Arc<dyn LLMProvider> = Arc::new(EvalAbortProbeProvider {
            main_calls: std::sync::atomic::AtomicUsize::new(0),
            gold_started: gold_started.clone(),
            gold_completed: gold_completed.clone(),
            release: release.clone(),
            finished: finished.clone(),
            second_round: SecondRoundBehavior::TerminalError,
        });
        let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = Arc::new(AlwaysOkExecutor);
        let config = eval_abort_config();
        let mut session = Session::new("session-eval-terminal", "model");
        let mut state = e2e_loop_state("session-eval-terminal");
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        // Never cancelled: the terminal error, not a cancel, drives the early exit.
        let cancel = CancellationToken::new();

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            super::run_pipeline(&mut session, &tx, llm, tools, &cancel, &config, &mut state),
        )
        .await
        .expect("run_pipeline did not return within 5s");

        assert!(
            matches!(result, Err(AgentError::LLM(_))),
            "the injected terminal error must surface as Err(LLM), got {result:?}"
        );
        assert!(
            !cancel.is_cancelled(),
            "this test must NOT rely on cancellation — it isolates the abort path"
        );
        assert!(
            gold_started.load(Ordering::SeqCst),
            "the Gold eval must have been genuinely in flight (else nothing was tested)"
        );
        assert!(
            state.gold_evaluation.in_flight.is_none(),
            "the in-flight Gold eval slot must be aborted+cleared on the terminal early-exit"
        );

        // Release the (aborted) eval and confirm it does NOT complete. Without the
        // abort, the detached eval would wake here and fire `finished`.
        release.notify_one();
        let finished_within =
            tokio::time::timeout(Duration::from_millis(500), finished.notified()).await;
        assert!(
            finished_within.is_err(),
            "in-flight Gold eval was detached, not aborted, on the terminal early-exit (#347)"
        );
        assert!(
            !gold_completed.load(Ordering::SeqCst),
            "aborted Gold eval must not complete its LLM request"
        );
    }

    // ---- Guardian final-message review context (issue #400) ----

    /// A guardian spawner stub that, like [`MockGuardianSpawner`], returns a
    /// canned child id, but also records every review prompt it was handed —
    /// letting tests assert on the guardian's review INPUT (what the reviewer
    /// actually sees) rather than just its spawn/suspend side effects.
    struct RecordingGuardianSpawner {
        child_id: String,
        prompts: Arc<std::sync::Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl GuardianSpawner for RecordingGuardianSpawner {
        async fn spawn_guardian_review(
            &self,
            _parent_session: &Session,
            review_prompt: String,
            _model: String,
            _disabled_tools: Option<std::collections::BTreeSet<String>>,
        ) -> Result<String, String> {
            self.prompts.lock().unwrap().push(review_prompt);
            Ok(self.child_id.clone())
        }
    }

    /// Guardian-only config (NO goal loop) wired to a [`RecordingGuardianSpawner`]
    /// so the test can inspect the prompt the reviewer was actually given.
    fn guardian_only_config_with_recorder(
        max_reviews: u32,
    ) -> (AgentLoopConfig, Arc<std::sync::Mutex<Vec<String>>>) {
        let prompts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let spawner: Arc<dyn GuardianSpawner> = Arc::new(RecordingGuardianSpawner {
            child_id: "guardian-child".to_string(),
            prompts: prompts.clone(),
        });
        let config = AgentLoopConfig {
            guardian_config: Some(GuardianConfig {
                enabled: true,
                model_name: Some("guardian-test-model".to_string()),
                max_reviews,
            }),
            guardian_spawner: Some(spawner),
            ..Default::default()
        };
        (config, prompts)
    }

    /// Guardian + autonomous goal loop, wired to a [`RecordingGuardianSpawner`]
    /// (a peer to [`guardian_and_gold_config`] used elsewhere in this module,
    /// but with a spawner that records prompts instead of just a canned id).
    fn guardian_and_gold_config_with_recorder(
        max_reviews: u32,
    ) -> (
        crate::runtime::config::AgentLoopConfig,
        Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let prompts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let spawner: Arc<dyn GuardianSpawner> = Arc::new(RecordingGuardianSpawner {
            child_id: "guardian-child".to_string(),
            prompts: prompts.clone(),
        });
        let config = crate::runtime::config::AgentLoopConfig {
            gold_config: Some(crate::runtime::config::GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("finish the task".to_string()),
                max_auto_continuations: 3,
                ..crate::runtime::config::GoldConfig::default()
            }),
            guardian_config: Some(GuardianConfig {
                enabled: true,
                model_name: Some("guardian-test-model".to_string()),
                max_reviews,
            }),
            guardian_spawner: Some(spawner),
            ..crate::runtime::config::AgentLoopConfig::default()
        };
        (config, prompts)
    }

    const GUARDIAN_FINAL_MESSAGE_HEADER: &str = "## Agent's final message";

    /// Direct unit coverage of [`build_guardian_review_prompt`]: real content is
    /// folded into the prompt under its own section.
    #[test]
    fn guardian_review_prompt_includes_final_assistant_content() {
        let config = AgentLoopConfig::default();
        let prompt = build_guardian_review_prompt(
            &None,
            &config,
            Some("Final handoff: shipped the fix and ran the tests."),
        );
        assert!(prompt.contains(GUARDIAN_FINAL_MESSAGE_HEADER));
        assert!(prompt.contains("Final handoff: shipped the fix and ran the tests."));
    }

    /// `None` (already-persisted / goal-loop case) adds nothing.
    #[test]
    fn guardian_review_prompt_omits_section_when_content_is_none() {
        let config = AgentLoopConfig::default();
        let prompt = build_guardian_review_prompt(&None, &config, None);
        assert!(!prompt.contains(GUARDIAN_FINAL_MESSAGE_HEADER));
    }

    /// Whitespace-only content must not add a stray, empty context block.
    #[test]
    fn guardian_review_prompt_omits_section_when_content_is_blank() {
        let config = AgentLoopConfig::default();
        let prompt = build_guardian_review_prompt(&None, &config, Some("   \n\t  "));
        assert!(!prompt.contains(GUARDIAN_FINAL_MESSAGE_HEADER));
    }

    /// THE fix (issue #400): in the guardian-only configuration (no goal loop),
    /// the final assistant message is deferred out of the session transcript to
    /// avoid a resumed-turn re-emit (see `handle_no_tool_calls`). Before this
    /// fix the guardian reviewer never saw that content at all. Now it must
    /// reach the reviewer as read-only review context, while the invariant that
    /// motivated the deferral — the message is NOT in the transcript at the
    /// suspend point — must still hold.
    #[tokio::test]
    async fn guardian_only_review_context_includes_final_content_without_persisting_it() {
        let mut session = Session::new("s400-guardian-only", "model");
        let (config, prompts) = guardian_only_config_with_recorder(2);
        let mut runtime_state = AgentRuntimeState::new("s400-guardian-only".to_string());
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let final_text = "Final handoff: implemented the feature and verified with cargo test.";
        let outcome = super::handle_no_tool_calls(
            final_text.to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "s400-guardian-only",
            &config,
            &None,
            "model",
            1,
            Arc::new(StubProvider),
        )
        .await;

        // The guardian engaged: suspended on the reviewer verdict rather than
        // completing outright.
        assert!(outcome.should_break);
        assert!(!outcome.sent_complete);
        assert!(runtime_state.waiting_for_children.is_some());

        // Invariant preserved: with no goal loop active, the final assistant
        // message must NOT be appended to the session transcript before/at the
        // guardian suspend point (this is what avoids the resumed-turn re-emit).
        assert!(
            session.messages.is_empty(),
            "the deferred final message must not be persisted into the transcript \
             at the guardian suspend point, got {:?}",
            session.messages
        );

        // But the guardian's review INPUT must include it as read-only context.
        let recorded = prompts.lock().unwrap();
        assert_eq!(recorded.len(), 1, "exactly one review was spawned");
        assert!(
            recorded[0].contains(GUARDIAN_FINAL_MESSAGE_HEADER),
            "guardian review prompt must include the final-message section:\n{}",
            recorded[0]
        );
        assert!(
            recorded[0].contains(final_text),
            "guardian review prompt must include the agent's actual final content:\n{}",
            recorded[0]
        );
    }

    /// Counterpart: with an autonomous goal loop ALSO active, the final
    /// assistant message is already appended to the session transcript before
    /// the guardian gate runs (see `handle_no_tool_calls`'s
    /// `add_message_before_gold`), so the transcript the reviewer child forks
    /// already contains it. The gate must pass `None` in that case so the
    /// content is not duplicated into the guardian's prompt a second time.
    #[tokio::test]
    async fn goal_loop_active_final_content_not_duplicated_in_guardian_prompt() {
        let mut session = Session::new("s400-goal-loop", "model");
        let (config, prompts) = guardian_and_gold_config_with_recorder(2);
        // Agent declared completion; the double-check confirms "achieved", so the
        // goal gate decides STOP and the guardian gate runs on the final state.
        let mut goal = ensure_goal_state(&session, "finish the task");
        goal.declare(GoalDeclaredStatus::Complete, 1);
        write_goal_state(&mut session, goal);
        let mut runtime_state = AgentRuntimeState::new("s400-goal-loop".to_string());
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let final_text = "Done — shipped and verified.";
        let outcome = super::handle_no_tool_calls(
            final_text.to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "s400-goal-loop",
            &config,
            &None,
            "model",
            1,
            Arc::new(ScriptedGoldProvider {
                decision: "achieved",
                confidence: "high",
            }),
        )
        .await;

        assert!(outcome.should_break);
        assert!(!outcome.sent_complete);
        assert!(runtime_state.waiting_for_children.is_some());

        // The goal-loop path adds the assistant message BEFORE the gate, so it
        // is already in the transcript the reviewer child forks.
        assert!(
            session
                .messages
                .iter()
                .any(|message| message.content == final_text),
            "goal-loop path must add the final assistant message to the transcript"
        );

        // The guardian's prompt must NOT carry a duplicate copy of that content.
        let recorded = prompts.lock().unwrap();
        assert_eq!(recorded.len(), 1, "exactly one review was spawned");
        assert!(
            !recorded[0].contains(GUARDIAN_FINAL_MESSAGE_HEADER),
            "goal-loop case must not duplicate the final message into the guardian prompt \
             (it is already in the forked transcript):\n{}",
            recorded[0]
        );
    }

    /// Empty/whitespace-only final content (e.g. a model turn with no visible
    /// text) must not add a stray, empty context block to the guardian's
    /// prompt.
    #[tokio::test]
    async fn guardian_only_blank_final_content_adds_no_stray_context_block() {
        let mut session = Session::new("s400-blank", "model");
        let (config, prompts) = guardian_only_config_with_recorder(2);
        let mut runtime_state = AgentRuntimeState::new("s400-blank".to_string());
        let (tx, _rx) = tokio::sync::mpsc::channel(16);

        let outcome = super::handle_no_tool_calls(
            "   \n  ".to_string(),
            None,
            None, // reasoning_signature (#524)
            5,
            5,
            round_usage(),
            &mut session,
            &mut runtime_state,
            &tx,
            None,
            "round-1",
            "s400-blank",
            &config,
            &None,
            "model",
            1,
            Arc::new(StubProvider),
        )
        .await;

        assert!(outcome.should_break);
        assert!(!outcome.sent_complete);
        assert!(session.messages.is_empty());

        let recorded = prompts.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert!(
            !recorded[0].contains(GUARDIAN_FINAL_MESSAGE_HEADER),
            "blank final content must not add a stray context block:\n{}",
            recorded[0]
        );
    }
}
