use crate::task_context::TaskLoopContext;

pub(super) fn update_task_round_state(
    task_context: &mut Option<TaskLoopContext>,
    round: usize,
    max_rounds: usize,
) {
    if let Some(ctx) = task_context.as_mut() {
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
        tracing::debug!(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_task_round_state_none() {
        let mut task_context: Option<TaskLoopContext> = None;
        update_task_round_state(&mut task_context, 3, 10);
        assert!(task_context.is_none());
    }

    #[test]
    fn test_build_round_id() {
        let id = build_round_id("session-123", 0);
        assert_eq!(id, "session-123-round-1");

        let id = build_round_id("test", 4);
        assert_eq!(id, "test-round-5");
    }

    #[test]
    fn test_log_round_start_does_not_panic() {
        // Should not panic regardless of debug_enabled
        log_round_start(false, "test", 0, 10, 5);
        log_round_start(true, "test", 2, 10, 3);
    }
}
