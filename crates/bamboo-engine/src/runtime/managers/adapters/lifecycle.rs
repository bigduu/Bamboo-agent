use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use bamboo_infrastructure::LLMProvider;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::metrics::MetricsCollector;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::managers::lifecycle::LifecycleManager;
use crate::runtime::runner::state_bridge;
use crate::runtime::task_context::TaskLoopContext;

/// Default lifecycle manager that delegates to existing runner functions.
pub struct DefaultLifecycleManager {
    llm: Arc<dyn LLMProvider>,
}

impl DefaultLifecycleManager {
    pub fn new(llm: Arc<dyn LLMProvider>) -> Self {
        Self { llm }
    }
}

#[async_trait]
impl LifecycleManager for DefaultLifecycleManager {
    fn initialize_run(&self, session: &Session, config: &AgentLoopConfig) -> AgentRuntimeState {
        let mut state = AgentRuntimeState::new(&session.id);
        state.llm.model_name = config.model_name.clone();
        state.llm.provider_name = config.provider_name.clone();
        state.llm.fast_model_name = config.fast_model_name.clone();
        state.llm.background_model_name = config.background_model_name.clone();
        state.round.max_rounds = config.max_rounds as u32;
        state.status = AgentStatusState::Initializing;
        state
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_round(
        &self,
        session: &mut Session,
        task_context: &mut Option<TaskLoopContext>,
        _runtime_state: &mut AgentRuntimeState,
        round: usize,
        max_rounds: usize,
        config: &AgentLoopConfig,
        cancel_token: &CancellationToken,
        metrics_collector: Option<&MetricsCollector>,
        session_id: &str,
        model_name: &str,
        tools: &dyn ToolExecutor,
        _llm: &dyn LLMProvider,
    ) -> Result<String, AgentError> {
        crate::runtime::runner::round_prelude::prepare_round(
            session,
            task_context,
            round,
            max_rounds,
            cancel_token,
            metrics_collector,
            session_id,
            model_name,
            false, // debug logging handled at runner level, not via adapter
            config,
            self.llm.clone(),
            tools,
        )
        .await
    }

    async fn handle_round_outcome(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        _task_context: &mut Option<TaskLoopContext>,
        round: usize,
        should_break: bool,
    ) -> Result<bool, AgentError> {
        runtime_state.round.current_round = round as u32;

        if should_break {
            runtime_state.status = AgentStatusState::Finalizing;
        } else if round as u32 >= runtime_state.round.max_rounds {
            tracing::info!(
                "[{}] Reached max rounds ({})",
                session.id,
                runtime_state.round.max_rounds
            );
            return Ok(true);
        }

        state_bridge::write_runtime_state(session, runtime_state);
        Ok(should_break)
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_run(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: &mpsc::Sender<AgentEvent>,
        session_id: &str,
        config: &AgentLoopConfig,
        metrics_collector: Option<&MetricsCollector>,
        task_context: Option<TaskLoopContext>,
    ) {
        runtime_state.status = AgentStatusState::Completed;
        state_bridge::write_runtime_state(session, runtime_state);

        crate::runtime::runner::session_finalize::finalize_session(
            task_context,
            session,
            event_tx,
            session_id,
            config,
            metrics_collector,
            false,
            runtime_state,
        )
        .await;
    }
}
