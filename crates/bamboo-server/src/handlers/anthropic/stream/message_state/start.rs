use bamboo_infrastructure_llm::api::models::ChatCompletionStreamChunk;
use serde_json::json;

use super::super::format::format_sse_event;
use super::AnthropicStreamState;

impl AnthropicStreamState {
    pub(super) fn ensure_message_started(
        &mut self,
        chunk: &ChatCompletionStreamChunk,
        output: &mut String,
    ) {
        if self.message_started {
            return;
        }

        let message_id = chunk.id.clone();
        self.message_id = Some(message_id.clone());
        let model = if self.model.is_empty() {
            chunk.model.clone().unwrap_or_else(|| self.model.clone())
        } else {
            self.model.clone()
        };
        let message_start = json!({
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": model,
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 0
                }
            }
        });
        output.push_str(&format_sse_event("message_start", message_start));
        self.message_started = true;
    }
}
