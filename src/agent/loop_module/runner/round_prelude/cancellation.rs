use tokio_util::sync::CancellationToken;

use crate::agent::core::AgentError;
use crate::agent::metrics::MetricsCollector;

pub(super) fn ensure_not_cancelled(
    cancel_token: &CancellationToken,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    message_count: usize,
) -> Result<(), AgentError> {
    if cancel_token.is_cancelled() {
        super::super::metrics_lifecycle::record_session_cancelled(
            metrics_collector,
            session_id,
            message_count as u32,
        );
        return Err(AgentError::Cancelled);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ensure_not_cancelled_returns_ok_when_not_cancelled() {
        let token = CancellationToken::new();
        let result = ensure_not_cancelled(&token, None, "session_1", 5);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_returns_error_when_cancelled() {
        let token = CancellationToken::new();
        token.cancel();

        let result = ensure_not_cancelled(&token, None, "session_1", 5);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AgentError::Cancelled));
    }

    #[tokio::test]
    async fn ensure_not_cancelled_handles_zero_message_count() {
        let token = CancellationToken::new();
        let result = ensure_not_cancelled(&token, None, "session_1", 0);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_handles_large_message_count() {
        let token = CancellationToken::new();
        let result = ensure_not_cancelled(&token, None, "session_1", 1000000);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_with_empty_session_id() {
        let token = CancellationToken::new();
        let result = ensure_not_cancelled(&token, None, "", 5);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_with_unicode_session_id() {
        let token = CancellationToken::new();
        let result = ensure_not_cancelled(&token, None, "会话-123🎯", 10);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_cancelled_with_zero_messages() {
        let token = CancellationToken::new();
        token.cancel();

        let result = ensure_not_cancelled(&token, None, "session_1", 0);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AgentError::Cancelled));
    }

    #[tokio::test]
    async fn ensure_not_cancelled_cancelled_with_large_message_count() {
        let token = CancellationToken::new();
        token.cancel();

        let result = ensure_not_cancelled(&token, None, "session_1", 1000000);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AgentError::Cancelled));
    }

    #[tokio::test]
    async fn ensure_not_cancelled_can_be_called_multiple_times() {
        let token = CancellationToken::new();

        // First call
        let result1 = ensure_not_cancelled(&token, None, "session_1", 5);
        assert!(result1.is_ok());

        // Second call
        let result2 = ensure_not_cancelled(&token, None, "session_1", 10);
        assert!(result2.is_ok());

        // Cancel and call again
        token.cancel();
        let result3 = ensure_not_cancelled(&token, None, "session_1", 15);
        assert!(result3.is_err());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_with_different_session_ids() {
        let token = CancellationToken::new();

        let result1 = ensure_not_cancelled(&token, None, "session_1", 5);
        let result2 = ensure_not_cancelled(&token, None, "session_2", 10);
        let result3 = ensure_not_cancelled(&token, None, "session_3", 15);

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
    }

    #[tokio::test]
    async fn ensure_not_cancelled_after_cancellation_always_returns_error() {
        let token = CancellationToken::new();
        token.cancel();

        // Multiple calls after cancellation should all fail
        let result1 = ensure_not_cancelled(&token, None, "session_1", 5);
        let result2 = ensure_not_cancelled(&token, None, "session_2", 10);
        let result3 = ensure_not_cancelled(&token, None, "session_3", 15);

        assert!(result1.is_err());
        assert!(result2.is_err());
        assert!(result3.is_err());
    }
}
