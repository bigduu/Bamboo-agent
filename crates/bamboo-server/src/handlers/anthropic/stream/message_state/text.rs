use serde_json::json;

use super::super::format::format_sse_event;
use super::AnthropicStreamState;

impl AnthropicStreamState {
    pub(super) fn handle_text_delta(&mut self, content: &str) -> String {
        let mut output = String::new();

        let block_index = match self.text_block_index {
            Some(index) => index,
            None => {
                let index = self.next_block_index;
                self.next_block_index += 1;
                self.text_block_index = Some(index);
                let start = json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "text",
                        "text": ""
                    }
                });
                output.push_str(&format_sse_event("content_block_start", start));
                index
            }
        };

        let delta = json!({
            "type": "content_block_delta",
            "index": block_index,
            "delta": {
                "type": "text_delta",
                "text": content
            }
        });
        output.push_str(&format_sse_event("content_block_delta", delta));

        output
    }
}
