use std::sync::Arc;
use std::error::Error as StdError;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::agent::events::TokenBudgetUsage;
use crate::agent::core::budget::PreparedContext;
use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{AgentError, AgentEvent, Message, Role, Session};
use crate::agent::llm::provider::ResponsesRequestOptions;
use crate::agent::llm::{LLMProvider, LLMRequestOptions};
use crate::core::ReasoningEffort;

const SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY: &str = "responses.previous_response_id";

fn session_previous_response_id(session: &Session) -> Option<&str> {
    session
        .metadata
        .get(SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn continuation_messages(messages: &[Message]) -> Option<&[Message]> {
    let last_assistant_index = messages
        .iter()
        .rposition(|message| matches!(message.role, Role::Assistant))?;
    let continuation = messages.get(last_assistant_index + 1..)?;
    (!continuation.is_empty()).then_some(continuation)
}

fn provider_supports_previous_response_id(provider_name: Option<&str>) -> bool {
    !matches!(provider_name.map(str::trim), Some("copilot"))
}

fn format_reqwest_transport_error(error: &reqwest::Error) -> String {
    let mut kinds = Vec::new();
    if error.is_timeout() {
        kinds.push("timeout");
    }
    if error.is_connect() {
        kinds.push("connect");
    }
    if error.is_request() {
        kinds.push("request");
    }
    if error.is_body() {
        kinds.push("body");
    }
    if error.is_decode() {
        kinds.push("decode");
    }
    if error.is_redirect() {
        kinds.push("redirect");
    }
    if error.is_builder() {
        kinds.push("builder");
    }
    if error.is_status() {
        kinds.push("status");
    }

    let kind = if kinds.is_empty() {
        "unknown".to_string()
    } else {
        kinds.join("+")
    };
    let url = error
        .url()
        .map(ToString::to_string)
        .unwrap_or_else(|| "<unknown>".to_string());

    let mut causes = Vec::new();
    let mut source = StdError::source(error);
    while let Some(cause) = source {
        causes.push(cause.to_string());
        source = cause.source();
        if causes.len() >= 4 {
            break;
        }
    }

    if causes.is_empty() {
        format!(
            "HTTP transport error [{}] for url ({}): {}",
            kind, url, error
        )
    } else {
        format!(
            "HTTP transport error [{}] for url ({}): {} | causes: {}",
            kind,
            url,
            error,
            causes.join(" | ")
        )
    }
}

fn format_provider_error(error: crate::agent::llm::provider::LLMError) -> String {
    match error {
        crate::agent::llm::provider::LLMError::Http(http) => format_reqwest_transport_error(&http),
        other => other.to_string(),
    }
}

pub(super) async fn execute_llm_stream(
    session: &mut Session,
    llm: &Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    prepared_context: &PreparedContext,
    tool_schemas: &[ToolSchema],
    max_output_tokens: u32,
    model: &str,
    provider_name: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
    session_id: &str,
) -> Result<
    (
        crate::agent::loop_module::stream::handler::StreamHandlingOutput,
        u128,
    ),
    AgentError,
> {
    let llm_started_at = std::time::Instant::now();
    let supports_previous_response_id = provider_supports_previous_response_id(provider_name);
    let previous_response_id = if supports_previous_response_id {
        session_previous_response_id(session)
    } else {
        None
    };
    let request_messages = if previous_response_id.is_some() {
        continuation_messages(&prepared_context.messages)
            .unwrap_or(prepared_context.messages.as_slice())
    } else {
        prepared_context.messages.as_slice()
    };
    let mut responses_options = ResponsesRequestOptions {
        store: Some(false),
        // Keep tool-planning narration visible but compact.
        text_verbosity: Some("low".to_string()),
        reasoning_summary: Some("auto".to_string()),
        include: Some(vec!["reasoning.encrypted_content".to_string()]),
        ..Default::default()
    };
    if let Some(response_id) = previous_response_id {
        responses_options.previous_response_id = Some(response_id.to_string());
    }
    let request_options = LLMRequestOptions {
        reasoning_effort,
        parallel_tool_calls: Some(true),
        responses: Some(responses_options),
    };

    if !supports_previous_response_id {
        tracing::debug!(
            "[{}] Responses API previous_response_id disabled for provider={}",
            session_id,
            provider_name.unwrap_or("unknown")
        );
    } else if let Some(response_id) = previous_response_id {
        tracing::debug!(
            "[{}] Continuing Responses API turn with previous_response_id={} using {} delta messages ({} total messages in context)",
            session_id,
            response_id,
            request_messages.len(),
            prepared_context.messages.len()
        );
    }

    tracing::info!(
        "[{}] LLM request: model={}, parallel_tool_calls={:?}, reasoning_effort={:?}, tools={}, messages={}",
        session_id,
        model,
        request_options.parallel_tool_calls,
        request_options.reasoning_effort,
        tool_schemas.len(),
        request_messages.len(),
    );
    let stream = llm
        .chat_stream_with_options(
            request_messages,
            tool_schemas,
            Some(max_output_tokens),
            model,
            Some(&request_options),
        )
        .await
        .map_err(|error| AgentError::LLM(format_provider_error(error)))?;

    // Send token budget update AFTER LLM call succeeds.
    // This timing gives frontend time to subscribe to /events endpoint.
    let usage = TokenBudgetUsage {
        system_tokens: prepared_context.token_usage.system_tokens,
        summary_tokens: prepared_context.token_usage.summary_tokens,
        window_tokens: prepared_context.token_usage.window_tokens,
        total_tokens: prepared_context.token_usage.total_tokens,
        budget_limit: prepared_context.token_usage.budget_limit,
        truncation_occurred: prepared_context.truncation_occurred,
        segments_removed: prepared_context.segments_removed,
    };

    session.token_usage = Some(usage.clone());

    let budget_event = AgentEvent::TokenBudgetUpdated { usage };
    if let Err(error) = event_tx.send(budget_event).await {
        tracing::warn!(
            "[{}] Failed to send token budget event: {}",
            session_id,
            error
        );
    }

    let stream_output = crate::agent::loop_module::stream::handler::consume_llm_stream(
        stream,
        event_tx,
        cancel_token,
        session_id,
    )
    .await?;

    if supports_previous_response_id {
        if let Some(response_id) = stream_output
            .response_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            session.metadata.insert(
                SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY.to_string(),
                response_id.to_string(),
            );
        } else {
            session
                .metadata
                .remove(SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
        }
    } else {
        session
            .metadata
            .remove(SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
    }

    let llm_duration = llm_started_at.elapsed().as_millis();

    Ok((stream_output, llm_duration))
}

#[cfg(test)]
mod tests;
