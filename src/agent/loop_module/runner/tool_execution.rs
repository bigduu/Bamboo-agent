//! Tool execution helpers for the agent loop runner.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::tools::{ToolCall, ToolExecutor};
use crate::agent::core::{AgentEvent, Session};
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::metrics::{MetricsCollector, RoundStatus as MetricsRoundStatus};

mod clarification;
mod events;
mod execution_paths;
mod loop_state;
mod per_call;
mod todo;

use loop_state::RoundExecutionState;

pub(super) struct RoundToolExecutionResult {
    pub awaiting_clarification: bool,
    pub round_status: MetricsRoundStatus,
    pub round_error: Option<String>,
}

pub(super) async fn execute_round_tool_calls(
    tool_calls: &[ToolCall],
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    round: usize,
    session: &mut Session,
    tools: &Arc<dyn ToolExecutor>,
    config: &AgentLoopConfig,
    todo_context: &mut Option<TodoLoopContext>,
) -> RoundToolExecutionResult {
    let mut state = RoundExecutionState::default();

    for tool_call in tool_calls {
        let should_break = per_call::execute_single_tool_call(per_call::PerToolExecutionContext {
            tool_call,
            event_tx,
            metrics_collector,
            session_id,
            round_id,
            round,
            session,
            tools,
            config,
            todo_context,
            state: &mut state,
        })
        .await;

        if should_break {
            break;
        }
    }

    state.into_result()
}
