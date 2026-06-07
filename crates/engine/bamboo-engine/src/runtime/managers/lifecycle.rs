use async_trait::async_trait;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::AgentRuntimeState;
use bamboo_llm::LLMProvider;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;

/// Manages agent run state machine and round transitions.
#[async_trait]
pub trait LifecycleManager: Send + Sync {
    /// Initialize runtime state for a new agent run.
    fn initialize_run(&self, session: &Session, config: &AgentLoopConfig) -> AgentRuntimeState;

    /// Prepare for a new round. Returns the round ID.
    #[allow(clippy::too_many_arguments)]
    async fn prepare_round(
        &self,
        session: &mut Session,
        task_context: &mut Option<TaskLoopContext>,
        runtime_state: &mut AgentRuntimeState,
        round: usize,
        max_rounds: usize,
        config: &AgentLoopConfig,
        cancel_token: &CancellationToken,
        metrics_collector: Option<&bamboo_metrics::MetricsCollector>,
        session_id: &str,
        model_name: &str,
        tools: &dyn ToolExecutor,
        llm: &dyn LLMProvider,
    ) -> Result<String, AgentError>;

    /// Handle post-round processing and determine next action.
    /// Returns `true` if the agent run should break out of the round loop.
    async fn handle_round_outcome(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        task_context: &mut Option<TaskLoopContext>,
        round: usize,
        should_break: bool,
    ) -> Result<bool, AgentError>;

    /// Finalize the agent run.
    #[allow(clippy::too_many_arguments)]
    async fn finalize_run(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: &mpsc::Sender<AgentEvent>,
        session_id: &str,
        config: &AgentLoopConfig,
        metrics_collector: Option<&bamboo_metrics::MetricsCollector>,
        task_context: Option<TaskLoopContext>,
    );
}
