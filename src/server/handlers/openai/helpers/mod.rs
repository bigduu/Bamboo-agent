mod completion;
mod conversion;
mod reasoning;
mod responses_input;
mod responses_options;
mod stream_utils;

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

pub(crate) fn parse_reasoning_effort(
    parameters: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<crate::core::ReasoningEffort> {
    reasoning::parse_reasoning_effort(parameters)
}

pub(crate) fn parse_responses_request_options(
    parameters: &std::collections::HashMap<String, serde_json::Value>,
) -> crate::agent::llm::provider::ResponsesRequestOptions {
    responses_options::parse_responses_request_options(parameters)
}
