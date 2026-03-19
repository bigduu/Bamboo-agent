use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::Session;
use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::todo_context::TodoLoopContext;
use crate::agent::metrics::MetricsCollector;

use super::super::logging::DebugLogger;

pub(super) struct LoopRunState {
    pub(super) session_id: String,
    pub(super) model_name: String,
    pub(super) metrics_collector: Option<MetricsCollector>,
    pub(super) debug_logger: DebugLogger,
    pub(super) todo_context: Option<TodoLoopContext>,
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

    let todo_context = super::super::session_setup::prepare_session_for_loop(
        session,
        initial_message,
        config,
        tools,
        metrics_collector.as_ref(),
        &session_id,
    )
    .await;

    LoopRunState {
        session_id,
        model_name,
        metrics_collector,
        debug_logger,
        todo_context,
    }
}
