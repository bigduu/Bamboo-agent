use bamboo_infrastructure_llm::api::models::StreamToolCall;
use serde_json::json;

use super::super::format::format_sse_event;
use super::{AnthropicStreamState, ToolStreamState};

impl AnthropicStreamState {
    pub(super) fn handle_tool_calls(&mut self, tool_calls: &[StreamToolCall]) -> String {
        let mut output = String::new();

        for tool_call in tool_calls {
            let tool_index = tool_call.index;
            let entry = self.tool_blocks.entry(tool_index).or_insert_with(|| {
                let block_index = self.next_block_index;
                self.next_block_index += 1;
                ToolStreamState {
                    block_index,
                    id: None,
                    name: None,
                    started: false,
                }
            });

            if let Some(id) = &tool_call.id {
                entry.id = Some(id.clone());
            }

            if let Some(function) = &tool_call.function {
                if let Some(name) = &function.name {
                    entry.name = Some(name.clone());
                }
            }

            if !entry.started && entry.id.is_some() && entry.name.is_some() {
                let start = json!({
                    "type": "content_block_start",
                    "index": entry.block_index,
                    "content_block": {
                        "type": "tool_use",
                        "id": entry.id.clone().unwrap_or_default(),
                        "name": entry.name.clone().unwrap_or_default(),
                        "input": {}
                    }
                });
                output.push_str(&format_sse_event("content_block_start", start));
                entry.started = true;
            }

            if let Some(function) = &tool_call.function {
                if let Some(arguments) = &function.arguments {
                    let delta = json!({
                        "type": "content_block_delta",
                        "index": entry.block_index,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": arguments
                        }
                    });
                    output.push_str(&format_sse_event("content_block_delta", delta));
                }
            }
        }

        output
    }
}
