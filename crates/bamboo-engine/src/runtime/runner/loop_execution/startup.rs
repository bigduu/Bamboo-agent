use crate::metrics::MetricsCollector;
use crate::runtime::config::{AgentLoopConfig, AuxiliaryModelConfig};
use crate::runtime::runner::task_lifecycle::{
    AsyncTaskEvaluationRequest, AsyncTaskEvaluationResult,
};
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::Session;
use bamboo_domain::{AgentRuntimeState, AgentStatusState};

use super::super::logging::DebugLogger;
use crate::runtime::runner::state_bridge;

const MAX_CONSECUTIVE_OVERFLOW_RECOVERIES: usize = 3;

#[derive(Debug, Clone, Default)]
pub(super) struct OverflowRecoveryState {
    pub(super) total_recoveries: usize,
    pub(super) consecutive_recoveries: usize,
    pub(super) last_recovered_round: Option<usize>,
}

impl OverflowRecoveryState {
    pub(super) fn can_attempt_recovery(&self) -> bool {
        self.consecutive_recoveries < MAX_CONSECUTIVE_OVERFLOW_RECOVERIES
    }

    pub(super) fn record_recovery(&mut self, round: usize) {
        self.total_recoveries += 1;
        self.consecutive_recoveries += 1;
        self.last_recovered_round = Some(round);
    }

    pub(super) fn reset_after_stable_round(&mut self) {
        self.consecutive_recoveries = 0;
    }
}

pub(super) struct InFlightTaskEvaluation {
    pub(super) request: AsyncTaskEvaluationRequest,
    pub(super) join_handle: tokio::task::JoinHandle<AsyncTaskEvaluationResult>,
}

#[derive(Default)]
pub(super) struct TaskEvaluationState {
    pub(super) in_flight: Option<InFlightTaskEvaluation>,
    pub(super) completed: Option<AsyncTaskEvaluationResult>,
    pub(super) queued_request: Option<AsyncTaskEvaluationRequest>,
}

pub(super) struct LoopRunState {
    pub(super) session_id: String,
    pub(super) model_name: String,
    pub(super) metrics_collector: Option<MetricsCollector>,
    pub(super) debug_logger: DebugLogger,
    pub(super) task_context: Option<TaskLoopContext>,
    pub(super) overflow_recovery: OverflowRecoveryState,
    pub(super) task_evaluation: TaskEvaluationState,
    pub(super) auxiliary_models: AuxiliaryModelConfig,
    /// Structured runtime state persisted alongside the session.
    pub(super) runtime_state: AgentRuntimeState,
}

pub(super) fn resolve_auxiliary_models(config: &AgentLoopConfig) -> AuxiliaryModelConfig {
    config
        .auxiliary_model_resolver
        .as_ref()
        .map(|resolver| resolver())
        .unwrap_or_else(|| AuxiliaryModelConfig {
            fast_model_name: config.fast_model_name.clone(),
            fast_model_provider: config.fast_model_provider.clone(),
            background_model_name: config.background_model_name.clone(),
            planning_model_name: config.planning_model_name.clone(),
            search_model_name: config.search_model_name.clone(),
            summarization_model_name: config.summarization_model_name.clone(),
            background_model_provider: config.background_model_provider.clone(),
            summarization_model_provider: config.summarization_model_provider.clone(),
        })
}

pub(super) async fn initialize_loop_state(
    session: &mut Session,
    initial_message: &str,
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
) -> LoopRunState {
    let debug_logger = DebugLogger::new(tracing::enabled!(tracing::Level::DEBUG));
    let session_id = session.id.clone();
    let metrics_collector = config.metrics_collector.clone();
    let model_name = config
        .model_name
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    super::super::metrics_lifecycle::record_session_started(
        metrics_collector.as_ref(),
        &session_id,
        &model_name,
        session.created_at,
        session.messages.len() as u32,
    );

    tracing::debug!(
        "[{}] Starting agent loop with message: {}",
        session_id,
        initial_message
    );
    debug_logger.log_event(
        &session_id,
        "agent_loop_start",
        serde_json::json!({
            "message": initial_message,
            "max_rounds": config.max_rounds,
            "initial_message_count": session.messages.len(),
        }),
    );

    let auxiliary_models = resolve_auxiliary_models(config);

    let mut runtime_state = AgentRuntimeState::new(&session_id);
    runtime_state.llm.model_name = Some(model_name.clone());
    runtime_state.llm.provider_name = config.provider_name.clone();
    runtime_state.llm.fast_model_name = auxiliary_models.fast_model_name.clone();
    runtime_state.llm.background_model_name = auxiliary_models.background_model_name.clone();
    runtime_state.round.max_rounds = config.max_rounds as u32;
    state_bridge::sync_from_metadata(session, &mut runtime_state);
    runtime_state.status = AgentStatusState::Initializing;
    state_bridge::write_runtime_state(session, &runtime_state);
    runtime_state.status = AgentStatusState::Running;

    let task_context = super::super::session_setup::prepare_session_for_loop(
        session,
        initial_message,
        config,
        tools,
        metrics_collector.as_ref(),
        &session_id,
        &debug_logger,
    )
    .await;

    LoopRunState {
        session_id,
        model_name,
        metrics_collector,
        debug_logger,
        task_context,
        overflow_recovery: OverflowRecoveryState::default(),
        task_evaluation: TaskEvaluationState::default(),
        auxiliary_models,
        runtime_state,
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_auxiliary_models, OverflowRecoveryState};
    use crate::runtime::config::{AgentLoopConfig, AuxiliaryModelConfig};
    use std::sync::{Arc, Mutex};

    #[test]
    fn overflow_recovery_state_tracks_recoveries_and_resets() {
        let mut state = OverflowRecoveryState::default();
        assert!(state.can_attempt_recovery());

        state.record_recovery(0);
        assert_eq!(state.total_recoveries, 1);
        assert_eq!(state.consecutive_recoveries, 1);
        assert_eq!(state.last_recovered_round, Some(0));
        assert!(state.can_attempt_recovery());

        state.record_recovery(1);
        state.record_recovery(2);
        assert_eq!(state.total_recoveries, 3);
        assert_eq!(state.consecutive_recoveries, 3);
        assert!(!state.can_attempt_recovery());

        state.reset_after_stable_round();
        assert_eq!(state.consecutive_recoveries, 0);
        assert!(state.can_attempt_recovery());
        assert_eq!(state.total_recoveries, 3);
    }

    #[test]
    fn auxiliary_model_resolver_returns_latest_values() {
        let counter = Arc::new(Mutex::new(0usize));
        let counter_for_resolver = counter.clone();
        let config = AgentLoopConfig {
            auxiliary_model_resolver: Some(Arc::new(move || {
                let mut guard = counter_for_resolver.lock().expect("counter lock");
                *guard += 1;
                AuxiliaryModelConfig {
                    fast_model_name: Some(format!("fast-{}", *guard)),
                    background_model_name: Some(format!("bg-{}", *guard)),
                    summarization_model_name: Some(format!("sum-{}", *guard)),
                    ..Default::default()
                }
            })),
            ..Default::default()
        };

        let first = resolve_auxiliary_models(&config);
        let second = resolve_auxiliary_models(&config);

        assert_eq!(first.fast_model_name.as_deref(), Some("fast-1"));
        assert_eq!(first.background_model_name.as_deref(), Some("bg-1"));
        assert_eq!(first.summarization_model_name.as_deref(), Some("sum-1"));
        assert_eq!(second.fast_model_name.as_deref(), Some("fast-2"));
        assert_eq!(second.background_model_name.as_deref(), Some("bg-2"));
        assert_eq!(second.summarization_model_name.as_deref(), Some("sum-2"));
    }

    #[test]
    fn auxiliary_model_resolver_refreshes_summarization_model_between_calls() {
        let counter = Arc::new(Mutex::new(0usize));
        let counter_for_resolver = counter.clone();
        let config = AgentLoopConfig {
            auxiliary_model_resolver: Some(Arc::new(move || {
                let mut guard = counter_for_resolver.lock().expect("counter lock");
                *guard += 1;
                AuxiliaryModelConfig {
                    summarization_model_name: Some(format!("sum-{}", *guard)),
                    ..Default::default()
                }
            })),
            ..Default::default()
        };

        let first = resolve_auxiliary_models(&config);
        let second = resolve_auxiliary_models(&config);

        assert_eq!(first.summarization_model_name.as_deref(), Some("sum-1"));
        assert_eq!(second.summarization_model_name.as_deref(), Some("sum-2"));
    }
}
