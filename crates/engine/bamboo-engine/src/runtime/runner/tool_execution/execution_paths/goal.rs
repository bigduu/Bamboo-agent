//! Post-execution handler for the `update_goal` self-report tool.
//!
//! The tool itself is stateless; this handler is where the agent's declared
//! status (`complete` | `blocked`) is durably recorded into the session's
//! [`GoalState`](crate::runtime::goal_state::GoalState). The terminal decision
//! later reads that declaration to decide whether to run the side-channel
//! double-check and stop, or to keep the autonomous loop running.

use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::Session;
use bamboo_tools::tools::goal::{parse_update_goal_status, UPDATE_GOAL_TOOL_NAME};

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::goal_state::{ensure_goal_state, write_goal_state, GoalDeclaredStatus};

/// Record an `update_goal` call into the durable goal state, if this tool call
/// was a successful `update_goal` and a goal is actually active.
pub(super) fn maybe_apply_goal_update(
    session: &mut Session,
    tool_call: &ToolCall,
    result: &ToolResult,
    config: &AgentLoopConfig,
    round: usize,
) {
    if tool_call.function.name != UPDATE_GOAL_TOOL_NAME || !result.success {
        return;
    }
    let Ok(status) = parse_update_goal_status(&tool_call.function.arguments) else {
        return;
    };
    // Only meaningful while a goal is active; ignore stray calls otherwise.
    let Some(objective) = config.active_goal() else {
        return;
    };
    let declared = match status.as_str() {
        "complete" => GoalDeclaredStatus::Complete,
        "blocked" => GoalDeclaredStatus::Blocked,
        _ => return,
    };

    // Store the round 1-based to match how rounds are surfaced everywhere else
    // (logs/UI display `round + 1`), so the persisted `declared_at_round` and the
    // log line below agree.
    let declared_round = (round + 1) as u32;
    let mut state = ensure_goal_state(session, objective);
    state.declare(declared, declared_round);
    write_goal_state(session, state);

    tracing::info!(
        "[{}] update_goal recorded: agent declared status={} at round {}",
        session.id,
        declared.as_str(),
        declared_round,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::config::GoldConfig;
    use crate::runtime::goal_state::read_goal_state;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolResult};

    fn goal_config() -> AgentLoopConfig {
        AgentLoopConfig {
            gold_config: Some(GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("ship it".to_string()),
                ..GoldConfig::default()
            }),
            ..AgentLoopConfig::default()
        }
    }

    fn update_goal_call(status: &str) -> ToolCall {
        ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: UPDATE_GOAL_TOOL_NAME.to_string(),
                arguments: format!(r#"{{"status":"{status}"}}"#),
            },
        }
    }

    #[test]
    fn records_complete_declaration() {
        let mut session = Session::new("s", "m");
        let config = goal_config();
        maybe_apply_goal_update(
            &mut session,
            &update_goal_call("complete"),
            &ToolResult::text(true, "ok"),
            &config,
            3,
        );
        let state = read_goal_state(&session).expect("goal state recorded");
        assert_eq!(state.declared_status, Some(GoalDeclaredStatus::Complete));
        // Stored 1-based (round arg 3 -> displayed/persisted round 4).
        assert_eq!(state.declared_at_round, Some(4));
        assert_eq!(state.objective, "ship it");
    }

    #[test]
    fn records_blocked_declaration() {
        let mut session = Session::new("s", "m");
        let config = goal_config();
        maybe_apply_goal_update(
            &mut session,
            &update_goal_call("blocked"),
            &ToolResult::text(true, "ok"),
            &config,
            0,
        );
        let state = read_goal_state(&session).expect("goal state recorded");
        assert_eq!(state.declared_status, Some(GoalDeclaredStatus::Blocked));
    }

    #[test]
    fn ignores_other_tools() {
        let mut session = Session::new("s", "m");
        let config = goal_config();
        let other = ToolCall {
            id: "c".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Bash".to_string(),
                arguments: "{}".to_string(),
            },
        };
        maybe_apply_goal_update(
            &mut session,
            &other,
            &ToolResult::text(true, "ok"),
            &config,
            0,
        );
        assert!(read_goal_state(&session).is_none());
    }

    #[test]
    fn ignores_when_tool_failed() {
        let mut session = Session::new("s", "m");
        let config = goal_config();
        maybe_apply_goal_update(
            &mut session,
            &update_goal_call("complete"),
            &ToolResult::text(false, "error"),
            &config,
            0,
        );
        assert!(read_goal_state(&session).is_none());
    }

    #[test]
    fn ignores_when_no_active_goal() {
        let mut session = Session::new("s", "m");
        let config = AgentLoopConfig::default(); // Gold disabled, no goal.
        maybe_apply_goal_update(
            &mut session,
            &update_goal_call("complete"),
            &ToolResult::text(true, "ok"),
            &config,
            0,
        );
        assert!(read_goal_state(&session).is_none());
    }
}
