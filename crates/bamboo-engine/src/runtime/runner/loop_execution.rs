use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_infrastructure::LLMProvider;

mod pipeline;
mod startup;

use pipeline::run_pipeline;
use startup::{initialize_loop_state, LoopRunState};

/// Runs the agent loop with a custom configuration.
///
/// This is the primary entry point for executing an agent conversation loop.
/// It manages LLM streaming, tool execution, task list tracking, metrics collection,
/// and event emission throughout the conversation lifecycle.
///
/// # Arguments
///
/// * `session` - The conversation session to operate on
/// * `initial_message` - The user's initial message to process
/// * `event_tx` - Channel sender for agent events
/// * `llm` - The LLM provider to use for generation
/// * `tools` - The tool executor for handling tool calls
/// * `cancel_token` - Token for cancelling the operation
/// * `config` - Configuration controlling loop behavior
///
/// # Returns
///
/// Returns `Ok(())` on successful completion, or an error if the loop fails.
pub async fn run_agent_loop_with_config(
    session: &mut Session,
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    cancel_token: CancellationToken,
    config: AgentLoopConfig,
) -> super::Result<()> {
    let session_span = tracing::info_span!("agent_loop", session_id = %session.id);
    async {
        let mut state: LoopRunState =
            initialize_loop_state(session, initial_message.as_str(), &config, tools.as_ref()).await;

        let sent_complete = run_pipeline(
            session,
            &event_tx,
            llm,
            tools,
            &cancel_token,
            &config,
            &mut state,
        )
        .await?;

        super::session_finalize::finalize_session(
            state.task_context,
            session,
            &event_tx,
            &state.session_id,
            &config,
            state.metrics_collector.as_ref(),
            sent_complete,
            &mut state.runtime_state,
        )
        .await;

        Ok(())
    }
    .instrument(session_span)
    .await
}
