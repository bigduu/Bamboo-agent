mod completion;
mod conversion;
mod parallel_tool_calls;
mod reasoning;
mod responses_input;
mod responses_options;
pub(crate) mod stream_utils;

pub(super) fn convert_messages(
    messages: Vec<bamboo_llm::api::models::ChatMessage>,
) -> Result<Vec<bamboo_agent_core::Message>, crate::error::AppError> {
    conversion::convert_messages(messages)
}

pub(super) fn convert_tools(
    tools: Option<Vec<bamboo_llm::api::models::Tool>>,
) -> Result<Vec<bamboo_agent_core::tools::ToolSchema>, crate::error::AppError> {
    conversion::convert_tools(tools)
}

pub(super) fn responses_input_to_chat_messages(
    input: serde_json::Value,
) -> Result<Vec<bamboo_llm::api::models::ChatMessage>, crate::error::AppError> {
    responses_input::responses_input_to_chat_messages(input)
}

pub(super) fn now_unix_ts() -> u64 {
    stream_utils::now_unix_ts()
}

pub(super) fn convert_chunk_to_openai(
    chunk: bamboo_llm::types::LLMChunk,
    model: &str,
) -> Option<bamboo_llm::api::models::ChatCompletionStreamChunk> {
    stream_utils::convert_chunk_to_openai(chunk, model)
}

pub(super) fn build_completion_response(
    content: String,
    tool_calls: Option<Vec<bamboo_llm::api::models::ToolCall>>,
    model: &str,
) -> bamboo_llm::api::models::ChatCompletionResponse {
    completion::build_completion_response(content, tool_calls, model)
}

pub(crate) fn parse_reasoning_effort(
    parameters: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<bamboo_domain::reasoning::ReasoningEffort> {
    reasoning::parse_reasoning_effort(parameters)
}

pub(crate) fn parse_parallel_tool_calls(
    parameters: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<bool> {
    parallel_tool_calls::parse_parallel_tool_calls(parameters)
}

pub(crate) fn parse_responses_request_options(
    parameters: &std::collections::HashMap<String, serde_json::Value>,
) -> bamboo_llm::provider::ResponsesRequestOptions {
    responses_options::parse_responses_request_options(parameters)
}
