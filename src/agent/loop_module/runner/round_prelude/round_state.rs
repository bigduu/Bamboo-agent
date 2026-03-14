use crate::agent::loop_module::todo_context::TodoLoopContext;

pub(super) fn update_todo_round_state(
    todo_context: &mut Option<TodoLoopContext>,
    round: usize,
    max_rounds: usize,
) {
    if let Some(ctx) = todo_context.as_mut() {
        ctx.current_round = round as u32;
        ctx.max_rounds = max_rounds as u32;
    }
}

pub(super) fn build_round_id(session_id: &str, round: usize) -> String {
    format!("{}-round-{}", session_id, round + 1)
}

pub(super) fn log_round_start(
    debug_enabled: bool,
    session_id: &str,
    round: usize,
    max_rounds: usize,
    message_count: usize,
) {
    if debug_enabled {
        log::debug!(
            "[{}] round_start: {}",
            session_id,
            serde_json::json!({
                "round": round + 1,
                "total_rounds": max_rounds,
                "message_count": message_count,
            })
        );
    }
}
