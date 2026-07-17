use crate::error::AppError;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::Message;
use bamboo_llm::api::models::{
    ChatCompletionRequest, ChatCompletionResponse, ChatCompletionStreamChunk, StreamChoice,
    StreamDelta,
};
use bamboo_llm::protocol::FromProvider;
use bamboo_llm::providers::anthropic::{
    api_types::{
        AnthropicCompleteRequest, AnthropicCompleteResponse, AnthropicMessagesRequest,
        AnthropicMessagesResponse,
    },
    conversion::{self as anthropic_conversion},
};

use super::errors::{map_conversion_error, AnthropicError};

pub(super) fn convert_messages_request(
    request: AnthropicMessagesRequest,
) -> Result<ChatCompletionRequest, AnthropicError> {
    anthropic_conversion::convert_messages_request(request).map_err(map_conversion_error)
}

pub(super) fn convert_messages_response(
    response: ChatCompletionResponse,
    response_model: &str,
) -> Result<AnthropicMessagesResponse, AnthropicError> {
    anthropic_conversion::convert_messages_response(response, response_model)
        .map_err(map_conversion_error)
}

pub(super) fn convert_complete_request(
    request: AnthropicCompleteRequest,
) -> Result<ChatCompletionRequest, AnthropicError> {
    anthropic_conversion::convert_complete_request(request).map_err(map_conversion_error)
}

pub(super) fn convert_complete_response(
    response: ChatCompletionResponse,
    response_model: &str,
) -> Result<AnthropicCompleteResponse, AnthropicError> {
    anthropic_conversion::convert_complete_response(response, response_model)
        .map_err(map_conversion_error)
}

pub(super) fn map_stop_reason(reason: Option<&str>) -> String {
    match reason {
        Some("stop") => "end_turn".to_string(),
        Some("length") => "max_tokens".to_string(),
        Some("tool_calls") => "tool_use".to_string(),
        Some(value) => value.to_string(),
        None => "end_turn".to_string(),
    }
}

pub(super) fn map_stop_reason_complete(reason: Option<&str>) -> String {
    match reason {
        Some("length") => "max_tokens".to_string(),
        Some("stop") => "stop_sequence".to_string(),
        Some(value) => value.to_string(),
        None => "stop_sequence".to_string(),
    }
}

/// Convert OpenAI chat messages to internal messages.
pub(super) fn convert_messages(
    chat_messages: Vec<bamboo_llm::api::models::ChatMessage>,
) -> Result<Vec<Message>, AppError> {
    chat_messages
        .into_iter()
        .map(|msg| {
            Message::from_provider(msg).map_err(|e| {
                AppError::InternalError(anyhow::anyhow!("Failed to convert message: {}", e))
            })
        })
        .collect()
}

/// Convert OpenAI tools to internal tool schemas.
pub(super) fn convert_tools(
    tools: Option<Vec<bamboo_llm::api::models::Tool>>,
) -> Result<Vec<ToolSchema>, AppError> {
    match tools {
        Some(tools) => tools
            .into_iter()
            .map(|tool| {
                ToolSchema::from_provider(tool).map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!("Failed to convert tool: {}", e))
                })
            })
            .collect(),
        None => Ok(vec![]),
    }
}

/// Convert LLMChunk to OpenAI stream format.
pub(super) fn convert_llm_chunk_to_openai(
    chunk: bamboo_llm::types::LLMChunk,
    model: &str,
) -> Option<ChatCompletionStreamChunk> {
    match chunk {
        bamboo_llm::types::LLMChunk::ResponseId(_) => None,
        bamboo_llm::types::LLMChunk::Token(text) => Some(ChatCompletionStreamChunk {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            object: Some("chat.completion.chunk".to_string()),
            created: chrono::Utc::now().timestamp() as u64,
            model: Some(model.to_string()),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some(text),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        }),
        // Preserve the provider's REAL tool-call index (see #379 / the shared
        // helper) so a downstream client routes interleaved fragments correctly;
        // `.enumerate()` over a single-fragment chunk always yields 0.
        bamboo_llm::types::LLMChunk::ToolCallsIndexed(tool_calls) => Some(
            crate::handlers::openai::helpers::stream_utils::tool_call_stream_chunk(
                tool_calls, model,
            ),
        ),
        bamboo_llm::types::LLMChunk::ToolCalls(tool_calls) => {
            // No per-fragment index carried → positional index is correct.
            let indexed = tool_calls
                .into_iter()
                .enumerate()
                .map(|(i, call)| (i as u32, call))
                .collect();
            Some(
                crate::handlers::openai::helpers::stream_utils::tool_call_stream_chunk(
                    indexed, model,
                ),
            )
        }
        bamboo_llm::types::LLMChunk::ReasoningToken(_) => None,
        bamboo_llm::types::LLMChunk::Done => Some(ChatCompletionStreamChunk {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            object: Some("chat.completion.chunk".to_string()),
            created: chrono::Utc::now().timestamp() as u64,
            model: Some(model.to_string()),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: None,
                    tool_calls: None,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: None,
        }),
        bamboo_llm::types::LLMChunk::CacheUsage { .. }
        | bamboo_llm::types::LLMChunk::UsageSummary { .. }
        | bamboo_llm::types::LLMChunk::ReasoningSignature(_) => None,
    }
}
