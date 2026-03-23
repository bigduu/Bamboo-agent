use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;

use super::round_error::record_round_failure;
use super::startup::LoopRunState;

const MAX_LLM_ROUND_ATTEMPTS: usize = 3;
const LLM_RETRY_BASE_DELAY_MS: u64 = 400;

fn should_retry_round_error(error: &crate::agent::core::AgentError) -> bool {
    use crate::agent::core::AgentError;

    let AgentError::LLM(message) = error else {
        return false;
    };

    let message = message.trim().to_ascii_lowercase();
    if message.is_empty() {
        return false;
    }

    // Hard failures that should fail fast — everything else is retried.
    let non_retryable_patterns = [
        "authentication error",
        "invalid api key",
        "invalid_request_error",
        "unsupported model",
        "model_name is required",
        "http 400",
        "http 401",
        "http 403",
        "http 404",
    ];

    !non_retryable_patterns
        .iter()
        .any(|pattern| message.contains(pattern))
}

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
            &mut state.task_context,
            round,
            config.max_rounds,
            cancel_token,
            state.metrics_collector.as_ref(),
            &state.session_id,
            &state.model_name,
            state.debug_logger.enabled,
        )
        .await?;

        let tool_schemas = super::super::session_setup::resolve_available_tool_schemas(
            config,
            tools.as_ref(),
        );

        let mut round_flow_outcome: Option<super::super::round_flow::RoundFlowOutcome> = None;
        let mut terminal_error: Option<crate::agent::core::AgentError> = None;

        for attempt in 1..=MAX_LLM_ROUND_ATTEMPTS {
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
                    if should_retry_round_error(&error) && attempt < MAX_LLM_ROUND_ATTEMPTS {
                        let delay_ms = LLM_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1));
                        tracing::warn!(
                            "[{}] Round {} LLM call failed (attempt {}/{}): {}. Retrying in {}ms",
                            state.session_id,
                            round + 1,
                            attempt,
                            MAX_LLM_ROUND_ATTEMPTS,
                            error,
                            delay_ms
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    }

                    tracing::error!(
                        "[{}] Round {} LLM call failed terminally (attempt {}/{}): {}",
                        state.session_id,
                        round + 1,
                        attempt,
                        MAX_LLM_ROUND_ATTEMPTS,
                        error,
                    );
                    terminal_error = Some(error);
                    break;
                }
            };

            match super::super::round_flow::handle_round_post_llm(
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
                &mut state.task_context,
                llm.clone(),
            )
            .await
            {
                Ok(outcome) => {
                    round_flow_outcome = Some(outcome);
                    break;
                }
                Err(error) => {
                    if should_retry_round_error(&error) && attempt < MAX_LLM_ROUND_ATTEMPTS {
                        let delay_ms = LLM_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1));
                        tracing::warn!(
                            "[{}] Round {} post-LLM handling failed (attempt {}/{}): {}. Retrying in {}ms",
                            state.session_id,
                            round + 1,
                            attempt,
                            MAX_LLM_ROUND_ATTEMPTS,
                            error,
                            delay_ms
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    }

                    tracing::error!(
                        "[{}] Round {} post-LLM handling failed terminally (attempt {}/{}): {}",
                        state.session_id,
                        round + 1,
                        attempt,
                        MAX_LLM_ROUND_ATTEMPTS,
                        error,
                    );
                    terminal_error = Some(error);
                    break;
                }
            }
        }

        if let Some(error) = terminal_error {
            record_round_failure(
                state.metrics_collector.as_ref(),
                &round_id,
                &state.session_id,
                session.messages.len() as u32,
                &error,
            );
            return Err(error);
        }

        let Some(round_flow_outcome) = round_flow_outcome else {
            let error =
                crate::agent::core::AgentError::LLM("round completed without outcome".to_string());
            record_round_failure(
                state.metrics_collector.as_ref(),
                &round_id,
                &state.session_id,
                session.messages.len() as u32,
                &error,
            );
            return Err(error);
        };

        sent_complete = sent_complete || round_flow_outcome.sent_complete;
        if round_flow_outcome.should_break {
            break;
        }
    }

    Ok(sent_complete)
}

#[cfg(test)]
mod tests {
    use super::should_retry_round_error;
    use crate::agent::core::AgentError;

    #[test]
    fn retries_transient_llm_errors() {
        assert!(should_retry_round_error(&AgentError::LLM(
            "HTTP error: timeout while connecting".to_string(),
        )));
        assert!(should_retry_round_error(&AgentError::LLM(
            "API error: HTTP 503: Service Unavailable".to_string(),
        )));
        assert!(should_retry_round_error(&AgentError::LLM(
            "empty assistant response".to_string(),
        )));
    }

    #[test]
    fn retries_reqwest_transport_errors() {
        // This was the original bug: "error sending request" was not retried.
        assert!(should_retry_round_error(&AgentError::LLM(
            "HTTP error: error sending request for url (https://api.githubcopilot.com/chat/completions)".to_string(),
        )));
    }

    #[test]
    fn retries_unknown_llm_errors_by_default() {
        // Any LLM error that doesn't match non-retryable patterns should be retried.
        assert!(should_retry_round_error(&AgentError::LLM(
            "some completely unknown error".to_string(),
        )));
    }

    #[test]
    fn does_not_retry_non_retryable_llm_errors() {
        assert!(!should_retry_round_error(&AgentError::LLM(
            "Authentication error: Invalid API key".to_string(),
        )));
        assert!(!should_retry_round_error(&AgentError::LLM(
            "API error: HTTP 400: invalid request".to_string(),
        )));
    }

    #[test]
    fn does_not_retry_non_llm_errors() {
        assert!(!should_retry_round_error(&AgentError::Cancelled));
        assert!(!should_retry_round_error(&AgentError::Tool(
            "tool failed".to_string(),
        )));
        assert!(!should_retry_round_error(&AgentError::Budget(
            "budget exceeded".to_string(),
        )));
    }

    #[test]
    fn does_not_retry_empty_llm_error() {
        assert!(!should_retry_round_error(&AgentError::LLM("".to_string(),)));
        assert!(!should_retry_round_error(&AgentError::LLM(
            "   ".to_string(),
        )));
    }
}
