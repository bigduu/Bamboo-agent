use std::sync::Arc;

use tokio::sync::mpsc;

use crate::metrics::RoundStatus as MetricsRoundStatus;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::gold_evaluation::{
    apply_gold_evaluation_result, build_async_gold_evaluation_request, evaluate_gold,
    execute_async_gold_evaluation, AsyncGoldEvaluationRequest, GoldEvaluationResult,
};
use crate::runtime::runner::loop_execution::startup::{InFlightGoldEvaluation, LoopRunState};
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::{AgentError, AgentEvent, GoldCheckpoint, GoldDecision, Message, Session};
use bamboo_domain::ReasoningEffort;
use bamboo_infrastructure::LLMProvider;

/// Metadata key tracking how many autonomous Gold continuations have run.
const GOLD_AUTO_CONTINUE_COUNT_KEY: &str = "gold.auto_continue_count";

/// Outcome of the terminal Gold gate evaluated when the agent produces no tool
/// calls (i.e. it would otherwise complete the run).
pub(super) enum GoldTerminalDecision {
    /// Complete the run as normal.
    Stop,
    /// Keep working: inject a hidden continuation message and run another round
    /// WITHOUT emitting `Complete`. Carries the evaluation that justified it.
    Continue(Box<GoldEvaluationResult>),
}

/// Decide, at the terminal point of a round, whether Gold wants the agent to
/// keep working toward the goal instead of completing.
///
/// This runs the continuation decision *inside* the runner loop so the run only
/// ever emits a single terminal `Complete`. That keeps `is_running` accurate and
/// the SSE stream open across continuations — unlike a post-stop resume, which
/// emits a premature `Complete` and leaves the frontend stuck/locked.
#[allow(clippy::too_many_arguments)]
pub(super) async fn evaluate_gold_terminal(
    session: &Session,
    task_context: &Option<TaskLoopContext>,
    config: &AgentLoopConfig,
    eval_model: &str,
    reasoning_effort: Option<ReasoningEffort>,
    session_id: &str,
    iteration: u32,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
) -> GoldTerminalDecision {
    let Some(gold_config) = config.gold_config.as_ref() else {
        return GoldTerminalDecision::Stop;
    };
    if !gold_config.enabled || !gold_config.auto_continue_enabled {
        return GoldTerminalDecision::Stop;
    }

    let continuation_count = gold_auto_continue_count(session);
    if continuation_count >= gold_config.max_auto_continuations {
        tracing::debug!(
            "[{}] Gold terminal gate: auto-continue budget exhausted ({}/{})",
            session_id,
            continuation_count,
            gold_config.max_auto_continuations
        );
        return GoldTerminalDecision::Stop;
    }

    let model_name = gold_config.model_name.as_deref().unwrap_or(eval_model);
    let result = match evaluate_gold(
        session,
        task_context.as_ref(),
        gold_config,
        llm,
        event_tx,
        session_id,
        model_name,
        reasoning_effort,
        GoldCheckpoint::Terminal,
        iteration,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(
                "[{}] Gold terminal gate evaluation failed, completing normally: {}",
                session_id,
                error
            );
            return GoldTerminalDecision::Stop;
        }
    };

    let should_continue = matches!(result.decision, GoldDecision::Continue)
        && result
            .confidence
            .meets(gold_config.min_auto_continue_confidence);

    if should_continue {
        GoldTerminalDecision::Continue(Box::new(result))
    } else {
        GoldTerminalDecision::Stop
    }
}

fn gold_auto_continue_count(session: &Session) -> u32 {
    session
        .metadata
        .get(GOLD_AUTO_CONTINUE_COUNT_KEY)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
}

/// Record a fresh continuation: bump the budget counter and inject the hidden
/// runtime message that tells the agent to keep working toward the goal.
/// Returns the new continuation count.
pub(super) fn apply_gold_terminal_continue(
    session: &mut Session,
    config: &AgentLoopConfig,
    result: &GoldEvaluationResult,
) -> u32 {
    let max_auto_continuations = config
        .gold_config
        .as_ref()
        .map(|cfg| cfg.max_auto_continuations)
        .unwrap_or(0);
    let goal = config.active_goal();
    let next_count = gold_auto_continue_count(session).saturating_add(1);

    session.metadata.insert(
        GOLD_AUTO_CONTINUE_COUNT_KEY.to_string(),
        next_count.to_string(),
    );
    session.add_message(gold_continue_runtime_message(
        result,
        next_count,
        max_auto_continuations,
        goal,
    ));
    next_count
}

fn gold_continue_runtime_message(
    result: &GoldEvaluationResult,
    continuation_count: u32,
    max_auto_continuations: u32,
    goal: Option<&str>,
) -> Message {
    let goal_line = goal
        .map(|goal| format!("Goal: {goal}\n"))
        .unwrap_or_default();
    let next_action_line = result
        .next_action
        .as_deref()
        .map(|action| format!("Suggested next action: {action}\n"))
        .unwrap_or_default();
    let missing_line = if result.missing_information.is_empty() {
        String::new()
    } else {
        format!("Still missing: {}\n", result.missing_information.join("; "))
    };

    let body = format!(
        "Runtime notification: the goal is not yet achieved, so continue working autonomously toward it.\n\n{goal_line}Gold reasoning: {}\n{missing_line}{next_action_line}Continuation budget: {}/{}\n\nKeep making progress toward the goal. Prioritize the suggested next action when given. Do not ask the user to repeat already-available context unless truly necessary.",
        result.reasoning, continuation_count, max_auto_continuations,
    );

    let mut message = Message::user(body);
    message.metadata = Some(serde_json::json!({
        "hidden_from_ui": true,
        "runtime_kind": "gold_continue_resume",
        "gold_decision": result.decision.as_str(),
        "gold_confidence": result.confidence.as_str(),
        "gold_checkpoint": result.checkpoint.as_str(),
        "gold_auto_continue_count": continuation_count,
    }));
    message.never_compress = true;
    message
}

pub(super) async fn poll_completed_gold_evaluation(state: &mut LoopRunState) {
    let finished = state
        .gold_evaluation
        .in_flight
        .as_ref()
        .is_some_and(|in_flight| in_flight.join_handle.is_finished());
    if !finished {
        return;
    }

    let Some(in_flight) = state.gold_evaluation.in_flight.take() else {
        return;
    };

    match in_flight.join_handle.await {
        Ok(result) => {
            state.gold_evaluation.completed = Some(result);
        }
        Err(error) => {
            tracing::warn!(
                "[{}] Async Gold evaluation join failed for round {}: {}",
                state.session_id,
                in_flight.request.round_number,
                error
            );
        }
    }
}

pub(super) async fn drain_in_flight_gold_evaluation(state: &mut LoopRunState) {
    if state.gold_evaluation.completed.is_some() {
        return;
    }

    let Some(in_flight) = state.gold_evaluation.in_flight.take() else {
        return;
    };

    match in_flight.join_handle.await {
        Ok(result) => {
            state.gold_evaluation.completed = Some(result);
        }
        Err(error) => {
            tracing::warn!(
                "[{}] Async Gold evaluation join failed while draining round {}: {}",
                state.session_id,
                in_flight.request.round_number,
                error
            );
        }
    }
}

pub(super) async fn apply_completed_gold_evaluation(
    session: &mut Session,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
) {
    let Some(result) = state.gold_evaluation.completed.take() else {
        return;
    };

    let usage = apply_gold_evaluation_result(session, &result.evaluation_result);

    let synthetic_round_id = format!(
        "{}-gold-evaluation-round-{}",
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
        MetricsRoundStatus::Success,
        usage,
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

    if let Some(ref persistence) = config.persistence {
        if let Err(error) = persistence.save_runtime_session(session).await {
            tracing::warn!(
                "[{}] Failed to save session after Gold evaluation: {}",
                state.session_id,
                error
            );
        }
    }
}

fn spawn_gold_evaluation_request(
    state: &mut LoopRunState,
    event_tx: &mpsc::Sender<AgentEvent>,
    request: AsyncGoldEvaluationRequest,
    llm: Arc<dyn LLMProvider>,
) {
    let gold_round = request.round_number;
    let session_id = state.session_id.clone();
    let event_tx = event_tx.clone();
    let request_for_spawn = request.clone();
    let join_handle = tokio::spawn(async move {
        execute_async_gold_evaluation(request_for_spawn, llm, event_tx).await
    });

    tracing::debug!(
        "[{}] Spawned async Gold evaluation for round {}",
        session_id,
        gold_round
    );

    state.gold_evaluation.in_flight = Some(InFlightGoldEvaluation {
        request,
        join_handle,
    });
}

pub(super) fn start_queued_gold_evaluation_if_idle(
    state: &mut LoopRunState,
    event_tx: &mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
) {
    if state.gold_evaluation.in_flight.is_some() {
        return;
    }

    let Some(request) = state.gold_evaluation.queued_request.take() else {
        return;
    };

    spawn_gold_evaluation_request(state, event_tx, request, llm);
}

pub(super) fn spawn_gold_evaluation_if_needed(
    turn: usize,
    session: &Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
    llm: Arc<dyn LLMProvider>,
) -> Result<(), AgentError> {
    let Some(gold_config) = config.gold_config.as_ref() else {
        return Ok(());
    };
    if !gold_config.enabled {
        return Ok(());
    }

    let eval_model = state
        .auxiliary_models
        .fast_model_name
        .as_deref()
        .or(Some(state.model_name.as_str()));
    let request = build_async_gold_evaluation_request(
        &state.task_context,
        session,
        &state.session_id,
        turn + 1,
        eval_model,
        config.reasoning_effort,
        GoldCheckpoint::PostRound,
        gold_config,
    )?;
    let Some(request) = request else {
        return Ok(());
    };

    if state.gold_evaluation.in_flight.is_some() {
        state.gold_evaluation.queued_request = Some(request);
        tracing::debug!(
            "[{}] Queued latest async Gold evaluation snapshot for round {} while another evaluation is still in flight",
            state.session_id,
            turn + 1
        );
        return Ok(());
    }

    spawn_gold_evaluation_request(state, event_tx, request, llm);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::config::GoldConfig;
    use crate::runtime::gold_evaluation::GoldEvaluationResult;
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_agent_core::{GoldConfidence, Role, Session};
    use bamboo_infrastructure::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use futures::stream;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    /// Minimal provider; the terminal gate short-circuits before calling it in
    /// the disabled / budget-exhausted paths exercised below.
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

    /// Provider that returns a single `report_gold_evaluation` tool call with the
    /// configured decision/confidence, so the terminal gate's real decision path
    /// can be exercised without a live model.
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
                r#"{{"decision":"{}","confidence":"{}","reasoning":"gate test","next_action":"write the report","missing_information":["the report file"]}}"#,
                self.decision, self.confidence
            );
            let call = ToolCall {
                id: "gold-call-1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
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

    async fn run_terminal_gate(
        session: &Session,
        config: &AgentLoopConfig,
        provider: Arc<dyn LLMProvider>,
    ) -> GoldTerminalDecision {
        let (tx, _rx) = mpsc::channel(8);
        evaluate_gold_terminal(session, &None, config, "model", None, "s", 1, provider, &tx).await
    }

    fn continue_result() -> GoldEvaluationResult {
        GoldEvaluationResult {
            checkpoint: GoldCheckpoint::Terminal,
            iteration: 1,
            decision: GoldDecision::Continue,
            confidence: GoldConfidence::High,
            reasoning: "Report not written yet".to_string(),
            missing_information: vec!["the report file".to_string()],
            next_action: Some("write the report".to_string()),
            prompt_tokens: 0,
            completion_tokens: 0,
        }
    }

    fn gold_continue_config() -> AgentLoopConfig {
        AgentLoopConfig {
            gold_config: Some(GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("ship the feature".to_string()),
                max_auto_continuations: 3,
                ..GoldConfig::default()
            }),
            ..AgentLoopConfig::default()
        }
    }

    #[tokio::test]
    async fn terminal_gate_stops_when_gold_disabled() {
        let session = Session::new("s", "model");
        let config = AgentLoopConfig::default();
        let (tx, _rx) = mpsc::channel(4);
        let decision = evaluate_gold_terminal(
            &session,
            &None,
            &config,
            "model",
            None,
            "s",
            1,
            Arc::new(StubProvider),
            &tx,
        )
        .await;
        assert!(matches!(decision, GoldTerminalDecision::Stop));
    }

    #[tokio::test]
    async fn terminal_gate_stops_when_budget_exhausted() {
        let mut session = Session::new("s", "model");
        session
            .metadata
            .insert(GOLD_AUTO_CONTINUE_COUNT_KEY.to_string(), "3".to_string());
        let config = gold_continue_config();
        let (tx, _rx) = mpsc::channel(4);
        let decision = evaluate_gold_terminal(
            &session,
            &None,
            &config,
            "model",
            None,
            "s",
            1,
            Arc::new(StubProvider),
            &tx,
        )
        .await;
        assert!(matches!(decision, GoldTerminalDecision::Stop));
    }

    #[test]
    fn apply_terminal_continue_injects_hidden_message_and_counter() {
        let mut session = Session::new("s", "model");
        let config = gold_continue_config();
        let result = continue_result();

        let count = apply_gold_terminal_continue(&mut session, &config, &result);

        assert_eq!(count, 1);
        assert_eq!(
            session
                .metadata
                .get(GOLD_AUTO_CONTINUE_COUNT_KEY)
                .map(String::as_str),
            Some("1")
        );
        let last = session.messages.last().expect("continue message appended");
        assert!(matches!(last.role, Role::User));
        assert!(last.never_compress);
        let metadata = last.metadata.as_ref().expect("runtime metadata");
        assert_eq!(
            metadata.get("hidden_from_ui").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            metadata.get("runtime_kind").and_then(|v| v.as_str()),
            Some("gold_continue_resume")
        );
        assert!(last.content.contains("ship the feature"));
        assert!(last.content.contains("write the report"));
        assert!(last.content.contains("the report file"));
    }

    #[tokio::test]
    async fn terminal_gate_continues_on_continue_with_sufficient_confidence() {
        let session = Session::new("s", "model");
        let config = gold_continue_config();
        let decision = run_terminal_gate(
            &session,
            &config,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "high",
            }),
        )
        .await;
        match decision {
            GoldTerminalDecision::Continue(result) => {
                assert!(matches!(result.decision, GoldDecision::Continue));
                assert_eq!(result.next_action.as_deref(), Some("write the report"));
            }
            GoldTerminalDecision::Stop => panic!("expected Continue, got Stop"),
        }
    }

    #[tokio::test]
    async fn terminal_gate_continues_when_confidence_exactly_meets_floor() {
        let session = Session::new("s", "model");
        // Default floor is `medium`; a `medium` continue should pass.
        let config = gold_continue_config();
        let decision = run_terminal_gate(
            &session,
            &config,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "medium",
            }),
        )
        .await;
        assert!(matches!(decision, GoldTerminalDecision::Continue(_)));
    }

    #[tokio::test]
    async fn terminal_gate_stops_when_confidence_below_floor() {
        let session = Session::new("s", "model");
        // Default floor is `medium`; a `low` continue must NOT continue.
        let config = gold_continue_config();
        let decision = run_terminal_gate(
            &session,
            &config,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "low",
            }),
        )
        .await;
        assert!(matches!(decision, GoldTerminalDecision::Stop));
    }

    #[tokio::test]
    async fn terminal_gate_stops_on_achieved_even_at_high_confidence() {
        let session = Session::new("s", "model");
        let config = gold_continue_config();
        let decision = run_terminal_gate(
            &session,
            &config,
            Arc::new(ScriptedGoldProvider {
                decision: "achieved",
                confidence: "high",
            }),
        )
        .await;
        assert!(matches!(decision, GoldTerminalDecision::Stop));
    }

    #[tokio::test]
    async fn terminal_gate_stops_when_auto_continue_disabled() {
        let session = Session::new("s", "model");
        let mut config = gold_continue_config();
        if let Some(cfg) = config.gold_config.as_mut() {
            cfg.auto_continue_enabled = false;
        }
        // Even though the provider would say "continue/high", auto_continue is
        // off so the gate must short-circuit to Stop (no continuation).
        let decision = run_terminal_gate(
            &session,
            &config,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "high",
            }),
        )
        .await;
        assert!(matches!(decision, GoldTerminalDecision::Stop));
    }

    #[tokio::test]
    async fn terminal_gate_continues_until_budget_then_stops() {
        // Walks the loop budget: with max=2, the first two terminal evaluations
        // continue; once the counter reaches the cap the gate stops.
        let mut session = Session::new("s", "model");
        let mut config = gold_continue_config();
        if let Some(cfg) = config.gold_config.as_mut() {
            cfg.max_auto_continuations = 2;
        }

        for expected in 1..=2u32 {
            let decision = run_terminal_gate(
                &session,
                &config,
                Arc::new(ScriptedGoldProvider {
                    decision: "continue",
                    confidence: "high",
                }),
            )
            .await;
            let result = match decision {
                GoldTerminalDecision::Continue(result) => result,
                GoldTerminalDecision::Stop => panic!("expected Continue on iteration {expected}"),
            };
            let count = apply_gold_terminal_continue(&mut session, &config, &result);
            assert_eq!(count, expected);
        }

        // Budget (2/2) is now exhausted: the gate stops without continuing.
        let decision = run_terminal_gate(
            &session,
            &config,
            Arc::new(ScriptedGoldProvider {
                decision: "continue",
                confidence: "high",
            }),
        )
        .await;
        assert!(matches!(decision, GoldTerminalDecision::Stop));
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
    async fn apply_completed_gold_evaluation_updates_metadata_and_persists_session() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(TestPersistence(storage.clone()));
        let mut session = Session::new("session-gold-eval", "model");

        let mut state = crate::runtime::runner::loop_execution::startup::LoopRunState {
            session_id: "session-gold-eval".to_string(),
            model_name: "model".to_string(),
            metrics_collector: None,
            debug_logger: crate::runtime::runner::logging::DebugLogger::new(false),
            task_context: None,
            overflow_recovery:
                crate::runtime::runner::loop_execution::startup::OverflowRecoveryState::default(),
            task_evaluation:
                crate::runtime::runner::loop_execution::startup::TaskEvaluationState::default(),
            gold_evaluation: crate::runtime::runner::loop_execution::startup::GoldEvaluationState {
                in_flight: None,
                completed: Some(crate::runtime::gold_evaluation::AsyncGoldEvaluationResult {
                    round_number: 2,
                    model_name: "fast-model".to_string(),
                    evaluation_result: crate::runtime::gold_evaluation::GoldEvaluationResult {
                        checkpoint: GoldCheckpoint::PostRound,
                        iteration: 2,
                        decision: bamboo_agent_core::GoldDecision::Achieved,
                        confidence: bamboo_agent_core::GoldConfidence::High,
                        reasoning: "Goal satisfied".to_string(),
                        missing_information: Vec::new(),
                        next_action: None,
                        prompt_tokens: 9,
                        completion_tokens: 4,
                    },
                }),
                queued_request: None,
            },
            auxiliary_models: crate::runtime::config::AuxiliaryModelConfig::default(),
            runtime_state: bamboo_domain::AgentRuntimeState::new("session-gold-eval"),
        };
        let config = crate::runtime::config::AgentLoopConfig {
            persistence: Some(persistence),
            ..Default::default()
        };

        apply_completed_gold_evaluation(&mut session, &config, &mut state).await;

        assert_eq!(
            session
                .metadata
                .get("gold.last_decision")
                .map(String::as_str),
            Some("achieved")
        );
        assert_eq!(
            session
                .metadata
                .get("gold.last_confidence")
                .map(String::as_str),
            Some("high")
        );
        assert_eq!(
            session
                .metadata
                .get("gold.last_checkpoint")
                .map(String::as_str),
            Some("post_round")
        );
        assert_eq!(
            session
                .metadata
                .get("gold.evaluation_count")
                .map(String::as_str),
            Some("1")
        );
        assert!(state.gold_evaluation.completed.is_none());

        let saved = storage
            .load_session("session-gold-eval")
            .await
            .expect("load should succeed")
            .expect("saved session should exist");
        assert_eq!(
            saved
                .metadata
                .get("gold.last_reasoning")
                .map(String::as_str),
            Some("Goal satisfied")
        );
    }
}
