use serde_json::json;

use super::super::super::conversion::map_stop_reason;
use super::super::format::format_sse_event;
use super::AnthropicStreamState;

impl AnthropicStreamState {
    pub(crate) fn finish(&mut self, reason: Option<&str>) -> String {
        if self.sent_message_stop {
            return String::new();
        }

        let mut output = String::new();

        if let Some(index) = self.text_block_index.take() {
            let stop = json!({
                "type": "content_block_stop",
                "index": index
            });
            output.push_str(&format_sse_event("content_block_stop", stop));
        }

        for entry in self.tool_blocks.values() {
            let stop = json!({
                "type": "content_block_stop",
                "index": entry.block_index
            });
            output.push_str(&format_sse_event("content_block_stop", stop));
        }

        let delta = json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": map_stop_reason(reason),
                "stop_sequence": null
            },
            "usage": {
                "output_tokens": 0
            }
        });
        output.push_str(&format_sse_event("message_delta", delta));

        let stop = json!({ "type": "message_stop" });
        output.push_str(&format_sse_event("message_stop", stop));

        self.sent_message_stop = true;
        output
    }
}
