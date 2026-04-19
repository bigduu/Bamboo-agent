use bamboo_agent_core::AgentError;
use crate::metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
};

fn map_round_error_status(error: &AgentError) -> (MetricsRoundStatus, MetricsSessionStatus) {
    if matches!(error, AgentError::Cancelled) {
        (
            MetricsRoundStatus::Cancelled,
            MetricsSessionStatus::Cancelled,
        )
    } else {
        (MetricsRoundStatus::Error, MetricsSessionStatus::Error)
    }
}

pub(super) fn record_round_failure(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    message_count: u32,
    error: &AgentError,
) {
    let (round_status, session_status) = map_round_error_status(error);
    super::super::metrics_lifecycle::record_round_and_session_error(
        metrics_collector,
        round_id,
        session_id,
        message_count,
        round_status,
        Some(error.to_string()),
        session_status,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::AgentError;

    #[test]
    fn test_map_round_error_status_cancelled() {
        let error = AgentError::Cancelled;
        let (round_status, session_status) = map_round_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Cancelled);
        assert_eq!(session_status, MetricsSessionStatus::Cancelled);
    }

    #[test]
    fn test_map_round_error_status_tool_error() {
        let error = AgentError::Tool("Tool failed".to_string());
        let (round_status, session_status) = map_round_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_round_error_status_llm_error() {
        let error = AgentError::LLM("LLM provider error".to_string());
        let (round_status, session_status) = map_round_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_round_error_status_session_not_found() {
        let error = AgentError::SessionNotFound("session-123".to_string());
        let (round_status, session_status) = map_round_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_round_error_status_budget_error() {
        let error = AgentError::Budget("Budget exceeded".to_string());
        let (round_status, session_status) = map_round_error_status(&error);
        assert_eq!(round_status, MetricsRoundStatus::Error);
        assert_eq!(session_status, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_round_error_status_cancelled_is_distinct() {
        let cancelled_error = AgentError::Cancelled;
        let other_error = AgentError::Tool("Tool error".to_string());

        let (cancelled_round, cancelled_session) = map_round_error_status(&cancelled_error);
        let (other_round, other_session) = map_round_error_status(&other_error);

        assert_ne!(cancelled_round, other_round);
        assert_ne!(cancelled_session, other_session);
    }

    #[test]
    fn test_map_round_error_returns_tuple() {
        let error = AgentError::Cancelled;
        let result = map_round_error_status(&error);
        assert!(matches!(result.0, MetricsRoundStatus::Cancelled));
        assert!(matches!(result.1, MetricsSessionStatus::Cancelled));
    }

    #[test]
    fn test_map_round_error_different_tool_errors() {
        let tool_error1 = AgentError::Tool("Connection timeout".to_string());
        let tool_error2 = AgentError::Tool("Permission denied".to_string());

        let (round1, session1) = map_round_error_status(&tool_error1);
        let (round2, session2) = map_round_error_status(&tool_error2);

        assert_eq!(round1, MetricsRoundStatus::Error);
        assert_eq!(round2, MetricsRoundStatus::Error);
        assert_eq!(session1, MetricsSessionStatus::Error);
        assert_eq!(session2, MetricsSessionStatus::Error);
    }

    #[test]
    fn test_map_round_error_different_llm_errors() {
        let llm_error1 = AgentError::LLM("Rate limit".to_string());
        let llm_error2 = AgentError::LLM("Invalid API key".to_string());

        let (round1, _) = map_round_error_status(&llm_error1);
        let (round2, _) = map_round_error_status(&llm_error2);

        assert_eq!(round1, MetricsRoundStatus::Error);
        assert_eq!(round2, MetricsRoundStatus::Error);
    }

    #[test]
    fn test_map_round_error_only_cancelled_gets_cancelled_status() {
        // Verify that ONLY Cancelled gets Cancelled status
        let errors = vec![
            AgentError::LLM("error".to_string()),
            AgentError::Tool("error".to_string()),
            AgentError::SessionNotFound("id".to_string()),
            AgentError::Budget("error".to_string()),
        ];

        for error in errors {
            let (round_status, session_status) = map_round_error_status(&error);
            assert_eq!(round_status, MetricsRoundStatus::Error);
            assert_eq!(session_status, MetricsSessionStatus::Error);
        }

        // Cancelled should be different
        let (round_status, session_status) = map_round_error_status(&AgentError::Cancelled);
        assert_eq!(round_status, MetricsRoundStatus::Cancelled);
        assert_eq!(session_status, MetricsSessionStatus::Cancelled);
    }
}
