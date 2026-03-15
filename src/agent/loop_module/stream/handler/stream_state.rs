use crate::agent::core::tools::ToolCallAccumulator;

use super::StreamHandlingOutput;

pub(super) struct StreamAccumulationState {
    content: String,
    reasoning_content: String,
    token_count: usize,
    tool_calls: ToolCallAccumulator,
}

impl StreamAccumulationState {
    pub(super) fn new() -> Self {
        Self {
            content: String::new(),
            reasoning_content: String::new(),
            token_count: 0,
            tool_calls: ToolCallAccumulator::new(),
        }
    }

    pub(super) fn append_token(&mut self, token: &str) {
        self.token_count += token.len();
        self.content.push_str(token);
    }

    pub(super) fn append_reasoning_token(&mut self, token: &str) {
        self.reasoning_content.push_str(token);
    }

    pub(super) fn extend_tool_calls(
        &mut self,
        partial_calls: Vec<crate::agent::core::tools::ToolCall>,
    ) {
        self.tool_calls.extend(partial_calls);
    }

    pub(super) fn into_output(self) -> StreamHandlingOutput {
        StreamHandlingOutput {
            content: self.content,
            reasoning_content: self.reasoning_content,
            token_count: self.token_count,
            tool_calls: self.tool_calls.finalize(),
        }
    }
}
