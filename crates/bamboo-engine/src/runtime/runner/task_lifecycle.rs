//! Task lifecycle helpers for the agent loop runner.

use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::{AgentEvent, Session};

mod evaluation;
mod finalize;

pub(in crate::runtime::runner) use evaluation::{
    apply_task_evaluation_result, build_async_task_evaluation_request,
    execute_async_task_evaluation, AsyncTaskEvaluationRequest, AsyncTaskEvaluationResult,
};

pub(super) async fn finalize_task_context(
    task_context: Option<TaskLoopContext>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    config: &AgentLoopConfig,
) {
    finalize::finalize_task_context(task_context, session, event_tx, session_id, config).await;
}
