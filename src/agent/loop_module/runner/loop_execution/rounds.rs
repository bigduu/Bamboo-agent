use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;

use super::round_error::record_round_failure;
use super::startup::LoopRunState;

pub(super) async fn run_rounds(
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    cancel_token: &CancellationToken,
    config: &AgentLoopConfig,
    state: &mut LoopRunState,
) -> super::super::Result<bool> {
    let mut sent_complete = false;

    for round in 0..config.max_rounds {
        let round_id = super::super::round_prelude::prepare_round(
            session,
            &mut state.todo_context,
            round,
            config.max_rounds,
            cancel_token,
            state.metrics_collector.as_ref(),
            &state.session_id,
            &state.model_name,
            state.debug_logger.enabled,
        )
        .await?;

        let tool_schemas =
            super::super::session_setup::resolve_available_tool_schemas(config, tools.as_ref());

        let round_llm_output = match super::super::round_lifecycle::execute_llm_round(
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
                record_round_failure(
                    state.metrics_collector.as_ref(),
                    &round_id,
                    &state.session_id,
                    session.messages.len() as u32,
                    &error,
                );
                return Err(error);
            }
        };

        let round_flow_outcome = super::super::round_flow::handle_round_post_llm(
            super::super::round_flow::RoundFlowContext {
                round,
                round_id: &round_id,
                session_id: &state.session_id,
                debug_enabled: state.debug_logger.enabled,
            },
            round_llm_output,
            session,
            event_tx,
            state.metrics_collector.as_ref(),
            &tools,
            config,
            &mut state.todo_context,
            llm.clone(),
        )
        .await?;

        sent_complete = sent_complete || round_flow_outcome.sent_complete;
        if round_flow_outcome.should_break {
            break;
        }
    }

    Ok(sent_complete)
}
