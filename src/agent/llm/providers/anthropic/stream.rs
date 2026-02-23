//! Streaming response handling for Anthropic API format.

use crate::api::models::{ChatCompletionStreamChunk, StreamToolCall};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Format an SSE event
pub fn format_sse_event(event: &str, data: Value) -> String {
    format!("event: {}\ndata: {}\n\n", event, data)
}

/// Format SSE data (without event name)
pub fn format_sse_data(data: Value) -> String {
    format!("data: {}\n\n", data)
}

/// State for tracking tool use blocks during streaming
struct ToolStreamState {
    block_index: usize,
    id: Option<String>,
    name: Option<String>,
    started: bool,
}

/// Stateful handler for Anthropic streaming responses (converts OpenAI format to Anthropic)
pub struct AnthropicStreamAdapter {
    message_started: bool,
    sent_message_stop: bool,
    next_block_index: usize,
    text_block_index: Option<usize>,
    tool_blocks: HashMap<u32, ToolStreamState>,
    model: String,
    message_id: Option<String>,
}

impl AnthropicStreamAdapter {
    /// Create a new stream state
    pub fn new(model: String) -> Self {
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

    /// Handle a streaming chunk and return Anthropic-formatted SSE output
    pub fn handle_chunk(&mut self, chunk: &ChatCompletionStreamChunk) -> String {
        let mut output = String::new();

        if !self.message_started {
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

    fn handle_text_delta(&mut self, content: &str) -> String {
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

    fn handle_tool_calls(&mut self, tool_calls: &[StreamToolCall]) -> String {
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

    /// Finish the stream and return final events
    pub fn finish(&mut self, reason: Option<&str>) -> String {
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

/// Map streaming chunk to Anthropic completion format (legacy)
pub fn map_completion_stream_chunk(chunk: &ChatCompletionStreamChunk, model: &str) -> String {
    let mut output = String::new();
    let model_name = if model.is_empty() {
        chunk.model.clone().unwrap_or_else(|| model.to_string())
    } else {
        model.to_string()
    };
    for choice in &chunk.choices {
        let mut completion = String::new();
        if let Some(content) = &choice.delta.content {
            completion.push_str(content);
        }

        if !completion.is_empty() {
            let data = json!({
                "type": "completion",
                "completion": completion,
                "model": model_name.clone(),
                "stop_reason": Value::Null
            });
            output.push_str(&format_sse_data(data));
        }

        if let Some(reason) = &choice.finish_reason {
            let data = json!({
                "type": "completion",
                "completion": "",
                "model": model_name.clone(),
                "stop_reason": map_stop_reason_complete(Some(reason.as_str()))
            });
            output.push_str(&format_sse_data(data));
        }
    }

    output
}

fn map_stop_reason(reason: Option<&str>) -> String {
    match reason {
        Some("stop") => "end_turn".to_string(),
        Some("length") => "max_tokens".to_string(),
        Some("tool_calls") => "tool_use".to_string(),
        Some(value) => value.to_string(),
        None => "end_turn".to_string(),
    }
}

fn map_stop_reason_complete(reason: Option<&str>) -> String {
    match reason {
        Some("length") => "max_tokens".to_string(),
        Some("stop") => "stop_sequence".to_string(),
        Some(value) => value.to_string(),
        None => "stop_sequence".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::{StreamChoice, StreamDelta, StreamFunctionCall};

    #[test]
    fn test_format_sse_event() {
        let data = json!({"type": "test"});
        let output = format_sse_event("test_event", data.clone());

        assert!(output.starts_with("event: test_event\n"));
        assert!(output.contains("data: {\"type\":\"test\"}"));
        assert!(output.ends_with("\n\n"));
    }

    #[test]
    fn test_format_sse_data() {
        let data = json!({"type": "test"});
        let output = format_sse_data(data);

        assert!(output.starts_with("data: "));
        assert!(output.ends_with("\n\n"));
    }

    #[test]
    fn test_adapter_new() {
        let adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        assert!(!adapter.message_started);
        assert!(!adapter.sent_message_stop);
        assert_eq!(adapter.next_block_index, 0);
        assert_eq!(adapter.model, "claude-3-opus");
    }

    #[test]
    fn test_adapter_first_chunk_sends_message_start() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        let chunk = ChatCompletionStreamChunk {
            id: "msg_123".to_string(),
            object: Some("chat.completion.chunk".to_string()),
            created: 1234567890,
            model: Some("claude-3-opus".to_string()),
            choices: vec![],
            usage: None,
        };

        let output = adapter.handle_chunk(&chunk);

        assert!(adapter.message_started);
        assert!(output.contains("event: message_start"));
        assert!(output.contains("\"id\":\"msg_123\""));
        assert!(output.contains("\"model\":\"claude-3-opus\""));
    }

    #[test]
    fn test_adapter_text_delta_creates_text_block() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        // First chunk to start the message
        let start_chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![],
            usage: None,
        };
        adapter.handle_chunk(&start_chunk);

        // Text delta chunk
        let chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some("Hello".to_string()),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        };

        let output = adapter.handle_chunk(&chunk);

        assert!(output.contains("event: content_block_start"));
        assert!(output.contains("\"type\":\"text\""));
        assert!(output.contains("event: content_block_delta"));
        assert!(output.contains("\"text\":\"Hello\""));
        assert_eq!(adapter.text_block_index, Some(0));
    }

    #[test]
    fn test_adapter_tool_call_creates_tool_block() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        // First chunk to start the message
        let start_chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![],
            usage: None,
        };
        adapter.handle_chunk(&start_chunk);

        // Tool call chunk
        let chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: None,
                    tool_calls: Some(vec![StreamToolCall {
                        index: 0,
                        id: Some("call_1".to_string()),
                        tool_type: Some("function".to_string()),
                        function: Some(StreamFunctionCall {
                            name: Some("search".to_string()),
                            arguments: None,
                        }),
                    }]),
                },
                finish_reason: None,
            }],
            usage: None,
        };

        let output = adapter.handle_chunk(&chunk);

        assert!(output.contains("event: content_block_start"));
        assert!(output.contains("\"type\":\"tool_use\""));
        assert!(output.contains("\"id\":\"call_1\""));
        assert!(output.contains("\"name\":\"search\""));
    }

    #[test]
    fn test_adapter_tool_call_arguments_streaming() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        // First chunk to start the message
        let start_chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![],
            usage: None,
        };
        adapter.handle_chunk(&start_chunk);

        // First tool call chunk (with id and name)
        let chunk1 = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: None,
                    tool_calls: Some(vec![StreamToolCall {
                        index: 0,
                        id: Some("call_1".to_string()),
                        tool_type: Some("function".to_string()),
                        function: Some(StreamFunctionCall {
                            name: Some("search".to_string()),
                            arguments: None,
                        }),
                    }]),
                },
                finish_reason: None,
            }],
            usage: None,
        };
        adapter.handle_chunk(&chunk1);

        // Second tool call chunk (with arguments)
        let chunk2 = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: None,
                    tool_calls: Some(vec![StreamToolCall {
                        index: 0,
                        id: None,
                        tool_type: None,
                        function: Some(StreamFunctionCall {
                            name: None,
                            arguments: Some("{\"q\":\"test\"}".to_string()),
                        }),
                    }]),
                },
                finish_reason: None,
            }],
            usage: None,
        };

        let output = adapter.handle_chunk(&chunk2);

        assert!(output.contains("event: content_block_delta"));
        assert!(output.contains("\"type\":\"input_json_delta\""));
        assert!(output.contains("\"partial_json\":\"{\\\"q\\\":\\\"test\\\"}\""));
    }

    #[test]
    fn test_adapter_finish() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        // Start message and add some content
        let chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some("Hello".to_string()),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        };
        adapter.handle_chunk(&chunk);

        let output = adapter.finish(Some("stop"));

        assert!(output.contains("event: content_block_stop"));
        assert!(output.contains("event: message_delta"));
        assert!(output.contains("\"stop_reason\":\"end_turn\""));
        assert!(output.contains("event: message_stop"));
        assert!(adapter.sent_message_stop);
    }

    #[test]
    fn test_adapter_finish_idempotent() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        adapter.finish(Some("stop"));
        let output = adapter.finish(Some("stop"));

        assert!(output.is_empty());
    }

    #[test]
    fn test_adapter_multiple_text_deltas_reuse_block() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        // First chunk to start the message
        let start_chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![],
            usage: None,
        };
        adapter.handle_chunk(&start_chunk);

        // First text delta
        let chunk1 = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some("Hello".to_string()),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        };
        let output1 = adapter.handle_chunk(&chunk1);

        // Second text delta
        let chunk2 = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: None,
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some(" world".to_string()),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        };
        let output2 = adapter.handle_chunk(&chunk2);

        // First output should have content_block_start
        assert!(output1.contains("event: content_block_start"));
        // Second output should not have content_block_start (reuses block)
        assert!(!output2.contains("event: content_block_start"));
        assert!(output2.contains("\"text\":\" world\""));
    }

    #[test]
    fn test_map_stop_reason() {
        assert_eq!(map_stop_reason(Some("stop")), "end_turn");
        assert_eq!(map_stop_reason(Some("length")), "max_tokens");
        assert_eq!(map_stop_reason(Some("tool_calls")), "tool_use");
        assert_eq!(map_stop_reason(Some("custom")), "custom");
        assert_eq!(map_stop_reason(None), "end_turn");
    }

    #[test]
    fn test_map_stop_reason_complete() {
        assert_eq!(map_stop_reason_complete(Some("length")), "max_tokens");
        assert_eq!(map_stop_reason_complete(Some("stop")), "stop_sequence");
        assert_eq!(map_stop_reason_complete(Some("custom")), "custom");
        assert_eq!(map_stop_reason_complete(None), "stop_sequence");
    }

    #[test]
    fn test_map_completion_stream_chunk_text() {
        let chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: Some("claude-3-opus".to_string()),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: Some("Hello".to_string()),
                    tool_calls: None,
                },
                finish_reason: None,
            }],
            usage: None,
        };

        let output = map_completion_stream_chunk(&chunk, "claude-3-opus");

        assert!(output.contains("data:"));
        assert!(output.contains("\"type\":\"completion\""));
        assert!(output.contains("\"completion\":\"Hello\""));
        assert!(output.contains("\"model\":\"claude-3-opus\""));
    }

    #[test]
    fn test_map_completion_stream_chunk_finish() {
        let chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: Some("claude-3-opus".to_string()),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    role: None,
                    content: None,
                    tool_calls: None,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: None,
        };

        let output = map_completion_stream_chunk(&chunk, "claude-3-opus");

        assert!(output.contains("\"stop_reason\":\"stop_sequence\""));
    }

    #[test]
    fn test_adapter_uses_chunk_model_when_model_empty() {
        let mut adapter = AnthropicStreamAdapter::new("".to_string());

        let chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: Some("claude-3-sonnet".to_string()),
            choices: vec![],
            usage: None,
        };

        let output = adapter.handle_chunk(&chunk);

        assert!(output.contains("\"model\":\"claude-3-sonnet\""));
    }

    #[test]
    fn test_adapter_prefers_instance_model() {
        let mut adapter = AnthropicStreamAdapter::new("claude-3-opus".to_string());

        let chunk = ChatCompletionStreamChunk {
            id: "msg_1".to_string(),
            object: None,
            created: 0,
            model: Some("claude-3-sonnet".to_string()),
            choices: vec![],
            usage: None,
        };

        let output = adapter.handle_chunk(&chunk);

        // Should use the instance model, not the chunk model
        assert!(output.contains("\"model\":\"claude-3-opus\""));
    }
}
