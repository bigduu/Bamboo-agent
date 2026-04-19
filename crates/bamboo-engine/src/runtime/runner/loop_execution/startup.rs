use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::Session;
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use crate::metrics::MetricsCollector;

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

pub(super) struct LoopRunState {
    pub(super) session_id: String,
    pub(super) model_name: String,
    pub(super) metrics_collector: Option<MetricsCollector>,
    pub(super) debug_logger: DebugLogger,
    pub(super) task_context: Option<TaskLoopContext>,
    pub(super) overflow_recovery: OverflowRecoveryState,
    /// Structured runtime state persisted alongside the session.
    pub(super) runtime_state: AgentRuntimeState,
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

    let mut runtime_state = AgentRuntimeState::new(&session_id);
    runtime_state.llm.model_name = Some(model_name.clone());
    runtime_state.llm.provider_name = config.provider_name.clone();
    runtime_state.llm.fast_model_name = config.fast_model_name.clone();
    runtime_state.llm.background_model_name = config.background_model_name.clone();
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
        runtime_state,
    }
}

#[cfg(test)]
mod tests {
    use super::OverflowRecoveryState;

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
}
