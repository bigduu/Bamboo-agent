mod completion;
mod conversion;
mod responses_input;
mod stream_utils;

use bytes::Bytes;

pub(super) fn convert_messages(
    messages: Vec<crate::agent::llm::api::models::ChatMessage>,
) -> Result<Vec<crate::agent::core::Message>, crate::server::error::AppError> {
    conversion::convert_messages(messages)
}

pub(super) fn convert_tools(
    tools: Option<Vec<crate::agent::llm::api::models::Tool>>,
) -> Result<Vec<crate::agent::core::tools::ToolSchema>, crate::server::error::AppError> {
    conversion::convert_tools(tools)
}

pub(super) fn responses_input_to_chat_messages(
    input: serde_json::Value,
) -> Result<Vec<crate::agent::llm::api::models::ChatMessage>, crate::server::error::AppError> {
    responses_input::responses_input_to_chat_messages(input)
}

pub(super) fn now_unix_ts() -> u64 {
    stream_utils::now_unix_ts()
}

pub(super) fn sse_data(json: &str) -> Bytes {
    stream_utils::sse_data(json)
}

pub(super) fn convert_chunk_to_openai(
    chunk: crate::agent::llm::types::LLMChunk,
    model: &str,
) -> Option<crate::agent::llm::api::models::ChatCompletionStreamChunk> {
    stream_utils::convert_chunk_to_openai(chunk, model)
}

pub(super) fn build_completion_response(
    content: String,
    tool_calls: Option<Vec<crate::agent::llm::api::models::ToolCall>>,
    model: &str,
) -> crate::agent::llm::api::models::ChatCompletionResponse {
    completion::build_completion_response(content, tool_calls, model)
}
