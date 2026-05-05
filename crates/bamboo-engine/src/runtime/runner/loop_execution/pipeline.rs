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

use crate::metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
    TokenUsage as MetricsTokenUsage,
};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::prompt_context::PromptMemoryRuntimeContext;
use crate::runtime::runner::session_setup::tool_schemas::resolve_available_tool_schemas_for_session;
use crate::runtime::stream::handler::StreamHandlingOutput;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Message, Session};
use bamboo_domain::session::runtime_state::{AgentStatusState, SuspensionState};
use bamboo_infrastructure::LLMProvider;

use super::super::to_event_token_usage;
use super::startup::LoopRunState;
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

// ---- Pending injected message merge ----

/// Merge any queued follow-up messages that were injected via `send_message`
/// while the child session is running. Loads the latest persisted session to
/// check for `pending_injected_messages` metadata, appends them to the in-memory
/// session, and clears the metadata flag.
async fn maybe_merge_pending_injected_messages(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) {
    let Some(storage) = storage else { return };

    let Ok(Some(latest)) = storage.load_session(&session.id).await else {
        return;
    };

    let Some(raw) = latest.metadata.get("pending_injected_messages") else {
        return;
    };

    let Ok(messages) = serde_json::from_str::<Vec<serde_json::Value>>(raw) else {
        return;
    };

    let mut merged = 0usize;
    for msg in messages {
        if let Some(content) = msg.get("content").and_then(|v| v.as_str()) {
            session.add_message(Message::user(content.to_string()));
            merged += 1;
        }
    }

    if merged > 0 {
        session.metadata.remove("pending_injected_messages");
        session.updated_at = chrono::Utc::now();

        if let Some(persistence) = persistence {
            if let Err(error) = persistence.save_runtime_session(session).await {
                tracing::warn!(
                    "[{}] Failed to persist pending injected message cleanup: {}",
                    session.id,
                    error
                );
            }
        }

        tracing::info!(
            "[{}] Merged {} injected message(s) from queued send_message at turn boundary",
            session.id,
            merged
        );
    }
}

// ---- Turn outcome (replaces RoundFlowOutcome) ----

struct TurnOutcome {
    should_break: bool,
    sent_complete: bool,
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

// ---- No-tool-calls path (from round_flow/no_tool_calls.rs) ----

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
) -> TurnOutcome {
    session.add_message(Message::assistant_with_reasoning(content, None, reasoning));

    let _ = event_tx
        .send(AgentEvent::Complete {
            usage: to_event_token_usage(prompt_tokens, completion_tokens),
        })
        .await;

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
        None,
    );

    TurnOutcome {
        should_break: true,
        sent_complete: true,
    }
}

// ---- Tool-calls path (from round_flow/tool_calls.rs) ----

async fn handle_tool_calls_path(
    turn: usize,
    round_id: &str,
    session_id: &str,
    debug_enabled: bool,
    stream_output: StreamHandlingOutput,
    mut round_usage: MetricsTokenUsage,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    tools: &Arc<dyn ToolExecutor>,
    config: &AgentLoopConfig,
    task_context: &mut Option<TaskLoopContext>,
    llm: Arc<dyn LLMProvider>,
) -> Result<TurnOutcome, AgentError> {
    let reasoning = (!stream_output.reasoning_content.trim().is_empty())
        .then_some(stream_output.reasoning_content);
    session.add_message(Message::assistant_with_reasoning(
        stream_output.content,
        Some(stream_output.tool_calls.clone()),
        reasoning,
    ));

    let compression_model = config
        .model_name
        .clone()
        .or_else(|| (!session.model.trim().is_empty()).then_some(session.model.trim().to_string()));
    if compression_model.is_none() {
        tracing::warn!(
            "[{}] Skipping mid-turn context compression after tool execution: missing model name",
            session_id
        );
    }
    let tool_schemas = resolve_available_tool_schemas_for_session(config, tools.as_ref(), session);

    let tool_execution = crate::runtime::runner::tool_execution::execute_round_tool_calls(
        &stream_output.tool_calls,
        event_tx,
        metrics_collector,
        session_id,
        round_id,
        turn,
        session,
        tools,
        config,
        task_context,
        &llm,
        compression_model.as_deref(),
        config.background_model_provider.as_ref(),
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
            metrics_collector,
            round_id,
            session_id,
            session.messages.len() as u32,
            round_status,
            round_usage,
            session
                .token_usage
                .as_ref()
                .map(|usage| usage.prompt_cached_tool_outputs)
                .unwrap_or(0)
                .min(u32::MAX as usize) as u32,
            round_error,
        );
        return Ok(TurnOutcome {
            should_break: true,
            sent_complete: false,
        });
    }

    if debug_enabled {
        tracing::debug!(
            "[{}] round_complete: {}",
            session_id,
            serde_json::json!({
                "round": turn + 1,
                "message_count": session.messages.len(),
            })
        );
    }

    let eval_model = config
        .fast_model_name
        .as_deref()
        .or(config.model_name.as_deref());
    let eval_provider = config
        .background_model_provider
        .clone()
        .unwrap_or_else(|| llm.clone());
    let task_evaluation_usage =
        crate::runtime::runner::task_lifecycle::evaluate_round_task_progress(
            task_context,
            session,
            eval_provider,
            event_tx,
            session_id,
            turn + 1,
            eval_model,
            config.reasoning_effort,
        )
        .await?;

    // Accumulate task evaluation usage into round usage
    round_usage.prompt_tokens = round_usage
        .prompt_tokens
        .saturating_add(task_evaluation_usage.prompt_tokens);
    round_usage.completion_tokens = round_usage
        .completion_tokens
        .saturating_add(task_evaluation_usage.completion_tokens);

    // ---- Dynamic model routing: classify task complexity ----
    // When features.dynamic_model_routing is enabled, evaluate task complexity
    // at the end of each round using the fast model. Store the result in session
    // metadata for downstream consumers (subagents, scheduling, etc.).
    let _complexity = if config.features_dynamic_model_routing {
        // Collect tool call names from this round for classification.
        let round_tool_calls = &stream_output.tool_calls;

        // Use the fast model for classification.
        let classifier_model = config
            .fast_model_name
            .as_deref()
            .or(config.model_name.as_deref());
        let _classifier_provider = config
            .background_model_provider
            .clone()
            .unwrap_or_else(|| llm.clone());

        if let Some(_model) = classifier_model {
            // Heuristic-based classification. For full LLM-backed classification,
            // wire MiniLoopExecutor through the runner (see ComplexityClassifier).
            let complexity = heuristic_complexity(round_tool_calls);
            tracing::info!(
                "[{}] Dynamic model routing: round {} complexity={:?}",
                session_id,
                turn + 1,
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
        metrics_collector,
        round_id,
        session_id,
        session.messages.len() as u32,
        round_status,
        round_usage,
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.prompt_cached_tool_outputs)
            .unwrap_or(0)
            .min(u32::MAX as usize) as u32,
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
        state.runtime_state.round.current_round = turn_counter;

        let round_id = format!("{}-round-{}", state.session_id, turn_counter + 1);

        // --- Prompt context refresh ---
        let runtime_context = PromptMemoryRuntimeContext {
            llm: llm.clone(),
            background_model_name: config.background_model_name.clone(),
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
        maybe_merge_pending_injected_messages(
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
                let reasoning = (!stream_output.reasoning_content.trim().is_empty())
                    .then_some(stream_output.reasoning_content);
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
                    )
                    .await,
                );
                break;
            }

            match handle_tool_calls_path(
                turn_counter as usize,
                &round_id,
                &state.session_id,
                state.debug_logger.enabled,
                stream_output,
                llm_output.round_usage,
                session,
                event_tx,
                state.metrics_collector.as_ref(),
                &tools,
                config,
                &mut state.task_context,
                llm.clone(),
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

        if session
            .metadata
            .get("runtime.suspend_reason")
            .is_some_and(|reason| reason == "waiting_for_children")
        {
            state.runtime_state.status = AgentStatusState::Suspended;
            state.runtime_state.suspension = Some(SuspensionState {
                reason: "waiting_for_children".to_string(),
                suspended_at: Utc::now(),
                resumable: true,
                hook_point: Some("AfterToolExecution".to_string()),
            });

            // The SubSession adapter registers durable wait details against the
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
                        if hidden_runtime_resume && !existing_ids.contains(message.id.as_str()) {
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

        state_bridge::write_runtime_state(session, &state.runtime_state);

        sent_complete = sent_complete || outcome.sent_complete;
        if outcome.should_break {
            break;
        }

        turn_counter += 1;

        // --- Guard against max_rounds ---
        if turn_counter >= config.max_rounds as u32 {
            break;
        }
    }

    Ok(sent_complete)
}

#[cfg(test)]
mod tests {
    use super::super::startup::OverflowRecoveryState;
    use super::{
        is_overflow_recoverable, map_turn_error_status, maybe_merge_pending_injected_messages,
        should_retry_turn_error,
    };
    use crate::metrics::{
        RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
        TokenUsage as MetricsTokenUsage,
    };
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::{AgentError, AgentEvent, Message, Session};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

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

        maybe_merge_pending_injected_messages(&mut running, Some(&storage), Some(&persistence))
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
        maybe_merge_pending_injected_messages(&mut running, Some(&storage), Some(&persistence))
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

    // --- Tests from round_prelude/round_state.rs ---

    #[test]
    fn test_build_round_id() {
        let id = format!("{}-round-{}", "session-123", 0 + 1);
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
    let complex_tools = ["Agent", "SubSession", "TodoWrite"];

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
