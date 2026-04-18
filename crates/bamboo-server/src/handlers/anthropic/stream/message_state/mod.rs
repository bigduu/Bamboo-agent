use bamboo_infrastructure_llm::api::models::ChatCompletionStreamChunk;
use std::collections::HashMap;

mod finish;
mod start;
#[cfg(test)]
mod tests;
mod text;
mod tools;

struct ToolStreamState {
    block_index: usize,
    id: Option<String>,
    name: Option<String>,
    started: bool,
}

pub(crate) struct AnthropicStreamState {
    message_started: bool,
    pub(crate) sent_message_stop: bool,
    next_block_index: usize,
    text_block_index: Option<usize>,
    tool_blocks: HashMap<u32, ToolStreamState>,
    model: String,
    message_id: Option<String>,
}

impl AnthropicStreamState {
    pub(crate) fn new(model: String) -> Self {
        Self {
            message_started: false,
            sent_message_stop: false,
            next_block_index: 0,
            text_block_index: None,
            tool_blocks: HashMap::new(),
            model,
            message_id: None,
        }
    }

    pub(crate) fn handle_chunk(&mut self, chunk: &ChatCompletionStreamChunk) -> String {
        let mut output = String::new();
        self.ensure_message_started(chunk, &mut output);

        for choice in &chunk.choices {
            if let Some(content) = &choice.delta.content {
                output.push_str(&self.handle_text_delta(content));
            }

            if let Some(tool_calls) = &choice.delta.tool_calls {
                output.push_str(&self.handle_tool_calls(tool_calls));
            }

            if let Some(reason) = &choice.finish_reason {
                output.push_str(&self.finish(Some(reason.as_str())));
            }
        }

        output
    }
}
