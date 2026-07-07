use bamboo_agent_core::tools::ToolCallAccumulator;

use super::StreamHandlingOutput;

pub(super) struct StreamAccumulationState {
    response_id: Option<String>,
    content: String,
    reasoning_content: String,
    token_count: usize,
    tool_calls: ToolCallAccumulator,
    output_tokens: u64,
    thinking_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    input_tokens: u64,
}

impl StreamAccumulationState {
    pub(super) fn new() -> Self {
        Self {
            response_id: None,
            content: String::new(),
            reasoning_content: String::new(),
            token_count: 0,
            tool_calls: ToolCallAccumulator::new(),
            output_tokens: 0,
            thinking_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            input_tokens: 0,
        }
    }

    pub(super) fn append_token(&mut self, token: &str) {
        self.token_count += token.len();
        self.content.push_str(token);
    }

    pub(super) fn append_reasoning_token(&mut self, token: &str) {
        self.reasoning_content.push_str(token);
    }

    pub(super) fn set_response_id(&mut self, response_id: String) {
        self.response_id = Some(response_id);
    }

    pub(super) fn extend_tool_calls(
        &mut self,
        partial_calls: Vec<bamboo_agent_core::tools::ToolCall>,
    ) {
        self.tool_calls.extend(partial_calls);
    }

    /// Accumulate index-tagged tool-call fragments, routing each to its call by
    /// provider index (the OpenAI-compatible chat-completions path). #236.
    pub(super) fn extend_tool_calls_indexed(
        &mut self,
        partial_calls: Vec<(u32, bamboo_agent_core::tools::ToolCall)>,
    ) {
        self.tool_calls.extend_indexed(partial_calls);
    }

    pub(super) fn record_usage(&mut self, output_tokens: u64, thinking_tokens: u64) {
        self.output_tokens = output_tokens;
        self.thinking_tokens = thinking_tokens;
    }

    pub(super) fn record_cache(&mut self, creation: u64, read: u64, input: u64) {
        self.cache_creation_input_tokens =
            self.cache_creation_input_tokens.saturating_add(creation);
        self.cache_read_input_tokens = self.cache_read_input_tokens.saturating_add(read);
        // input_tokens is the (fixed) fresh prompt size, reported once in
        // message_start. Take the max rather than accumulating so a delta that
        // echoes it does not double-count.
        self.input_tokens = self.input_tokens.max(input);
    }

    pub(super) fn into_output(self) -> StreamHandlingOutput {
        StreamHandlingOutput {
            response_id: self.response_id,
            content: self.content,
            reasoning_content: self.reasoning_content,
            token_count: self.token_count,
            tool_calls: self.tool_calls.finalize(),
            output_tokens: self.output_tokens,
            thinking_tokens: self.thinking_tokens,
            cache_creation_input_tokens: self.cache_creation_input_tokens,
            cache_read_input_tokens: self.cache_read_input_tokens,
            input_tokens: self.input_tokens,
        }
    }
}
