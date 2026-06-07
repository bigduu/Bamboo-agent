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

use bamboo_metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
    TokenUsage as MetricsTokenUsage,
};
use crate::runtime::config::AgentLoopConfig;
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
    AgentRuntimeState, AgentStatusState, ChildWaitPolicy, SuspensionState, WaitingForChildrenState,
};
use bamboo_llm::LLMProvider;

use super::super::to_event_token_usage;
use super::gold::{
    apply_completed_gold_evaluation, apply_gold_terminal_continue, drain_in_flight_gold_evaluation,
    evaluate_gold_terminal, poll_completed_gold_evaluation, spawn_gold_evaluation_if_needed,
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

    let now = Utc::now();
    let count = active.len();
    runtime_state.waiting_for_children = Some(WaitingForChildrenState {
        child_session_ids: active,
        wait_for: ChildWaitPolicy::All,
        registered_at: now,
        timeout_at: Some(now + chrono::Duration::hours(6)),
        registered_by_tool_call_id: None,
    });
    state_bridge::write_runtime_state(session, runtime_state);
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "waiting_for_children".to_string(),
    );
    session.updated_at = now;

    // Persist so the completion coordinator can resume this parent, and so the
    // suspend finalization merges (rather than clobbers) the durable wait.
    if let Some(persistence) = config.persistence.as_ref() {
        if let Err(error) = persistence.save_runtime_session(session).await {
            tracing::warn!(
                "[{}] safety-net auto-wait failed to persist parent wait: {}",
                session.id,
                error
            );
        }
    }
    tracing::info!(
        "[{}] end-of-turn safety net: suspending to wait for {} orphaned child session(s) the model did not explicitly wait on",
        session.id,
        count,
    );
    Some(TurnOutcome {
        should_break: true,
        sent_complete: false,
    })
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
        Ok(result) => {
            state.task_evaluation.completed = Some(result);
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
        Ok(result) => {
            state.task_evaluation.completed = Some(result);
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
) {
    let task_round = request.round_number;
    let session_id = state.session_id.clone();
    let event_tx = event_tx.clone();
    let request_for_spawn = request.clone();
    let join_handle = tokio::spawn(async move {
        crate::runtime::runner::task_lifecycle::execute_async_task_evaluation(
            request_for_spawn,
            llm,
            event_tx,
        )
        .await
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

fn spawn_task_evaluation_if_needed(
    turn: usize,
    session: &Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
    llm: Arc<dyn LLMProvider>,
) -> Result<(), AgentError> {
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

    spawn_task_evaluation_request(state, event_tx, request, llm);
    Ok(())
}

fn refresh_auxiliary_models_for_round(state: &mut LoopRunState, config: &AgentLoopConfig) {
    state.auxiliary_models = resolve_auxiliary_models(config);
    state.runtime_state.llm.fast_model_name = state.auxiliary_models.fast_model_name.clone();
    state.runtime_state.llm.background_model_name =
        state.auxiliary_models.background_model_name.clone();
}

// ---- No-tool-calls path (from round_flow/no_tool_calls.rs) ----

#[allow(clippy::too_many_arguments)]
async fn handle_no_tool_calls(
    content: String,
    reasoning: Option<String>,
    prompt_tokens: u64,
    completion_tokens: u64,
    round_usage: MetricsTokenUsage,
    session: &mut Session,
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
    session.add_message(Message::assistant_with_reasoning(content, None, reasoning));

    // Terminal Gold gate: when a goal is set with auto-continue, decide whether
    // to keep working toward it INSTEAD of completing. Running this inside the
    // loop means the run emits a single terminal `Complete` only when Gold is
    // truly done — keeping `is_running` accurate and the SSE stream open.
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

    let outcome = match decision {
        GoldTerminalDecision::Continue(result) => {
            let next_count = apply_gold_terminal_continue(session, config, &result);
            tracing::info!(
                "[{}] Gold terminal gate: continuing toward goal (continuation {})",
                session_id,
                next_count
            );
            TurnOutcome {
                should_break: false,
                sent_complete: false,
            }
        }
        GoldTerminalDecision::Stop => {
            let _ = event_tx
                .send(AgentEvent::Complete {
                    usage: to_event_token_usage(prompt_tokens, completion_tokens),
                })
                .await;
            TurnOutcome {
                should_break: true,
                sent_complete: true,
            }
        }
    };

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

    outcome
}

// ---- Tool-calls path (from round_flow/tool_calls.rs) ----

async fn handle_tool_calls_path(
    frame: &crate::runtime::runner::round_frame::RoundFrame<'_>,
    stream_output: StreamHandlingOutput,
    mut round_usage: MetricsTokenUsage,
    session: &mut Session,
    auxiliary_models: &crate::runtime::config::AuxiliaryModelConfig,
    model_name: &str,
    task_context: &mut Option<TaskLoopContext>,
) -> Result<TurnOutcome, AgentError> {
    let reasoning = (!stream_output.reasoning_content.trim().is_empty())
        .then_some(stream_output.reasoning_content);
    session.add_message(Message::assistant_with_reasoning(
        stream_output.content,
        Some(stream_output.tool_calls.clone()),
        reasoning,
    ));

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

    let tool_execution = crate::runtime::runner::tool_execution::execute_round_tool_calls(
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
    )
    .await?;

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
                spawn_task_evaluation_request(state, event_tx, request, eval_provider);
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
            config.app_data_dir.as_deref(),
            config.active_goal(),
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
                            crate::runtime::runner::round_lifecycle::force_overflow_context_recovery(
                                session,
                                config,
                                &state.model_name,
                                &state.session_id,
                                &llm,
                                Some(event_tx),
                            )
                            .await?;
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
                let reasoning = (!stream_output.reasoning_content.trim().is_empty())
                    .then_some(stream_output.reasoning_content);
                let eval_model = state
                    .auxiliary_models
                    .fast_model_name
                    .clone()
                    .unwrap_or_else(|| state.model_name.clone());
                turn_outcome = Some(
                    handle_no_tool_calls(
                        stream_output.content,
                        reasoning,
                        llm_output.prompt_tokens,
                        llm_output.completion_tokens,
                        llm_output.round_usage,
                        session,
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
                                .is_some_and(|kind| kind == "child_completion_resume");
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
        ) {
            tracing::warn!(
                "[{}] Failed to spawn async Gold evaluation after round {}: {}",
                state.session_id,
                turn_counter + 1,
                error
            );
        }

        turn_counter += 1;

        // --- Guard against max_rounds ---
        if turn_counter >= config.max_rounds as u32 {
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
            spawn_task_evaluation_request(state, event_tx, request, eval_provider);
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
        is_overflow_recoverable, is_terminal_child_status, map_turn_error_status,
        maybe_suspend_for_orphaned_children, should_retry_turn_error,
    };
    use crate::runtime::config::AgentLoopConfig;
    use bamboo_domain::AgentRuntimeState;
    use bamboo_metrics::{
        RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
        TokenUsage as MetricsTokenUsage,
    };
    use crate::runtime::runner::state_bridge;
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::{AgentError, AgentEvent, Message, Session};
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use futures::stream;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

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
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        let outcome = super::handle_no_tool_calls(
            "tentative answer".to_string(),
            None,
            5,
            5,
            round_usage(),
            &mut session,
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
            Some("gold_continue_resume")
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        let outcome = super::handle_no_tool_calls(
            "final answer".to_string(),
            None,
            5,
            5,
            round_usage(),
            &mut session,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);

        let outcome = super::handle_no_tool_calls(
            "final answer".to_string(),
            Some("reasoning trace".to_string()),
            11,
            7,
            MetricsTokenUsage {
                prompt_tokens: 11,
                completion_tokens: 7,
                total_tokens: 18,
            },
            &mut session,
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

        let outcome = maybe_suspend_for_orphaned_children(&mut session, &config, &mut runtime_state)
            .await
            .expect("must suspend when active children remain");
        assert!(outcome.should_break && !outcome.sent_complete);

        let wait = runtime_state
            .waiting_for_children
            .expect("durable wait registered");
        // Only the non-terminal children, sorted/deduped.
        assert_eq!(wait.child_session_ids, vec!["c-pend".to_string(), "c-run".to_string()]);
        assert_eq!(
            session.metadata.get("runtime.suspend_reason").map(String::as_str),
            Some("waiting_for_children")
        );
        // Persisted so the coordinator can resume it.
        let persisted = storage.load_session("parent-orphan").await.unwrap().unwrap();
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
}
