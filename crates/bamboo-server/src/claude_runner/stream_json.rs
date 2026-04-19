//! Parsing helpers for Claude Code CLI `--output-format stream-json`.
//!
//! Claude Code can stream machine-readable JSON lines (one JSON object per line).
//! We map those events into Bamboo's `AgentEvent` stream so the frontend can
//! display them consistently.

use bamboo_agent_core::{AgentEvent, TokenUsage, ToolResult};

#[derive(Debug, Default)]
pub struct ClaudeStreamJsonParser {
    last_message_id: Option<String>,
    last_message_text: String,
    terminal_emitted: bool,
}

impl ClaudeStreamJsonParser {
    pub fn parse_line(&mut self, line: &str) -> Vec<AgentEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }

        let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            // Best-effort fallback: treat as plain text output.
            return vec![AgentEvent::Token {
                content: format!("{trimmed}\n"),
            }];
        };

        self.parse_value(&value)
    }

    fn parse_value(&mut self, value: &serde_json::Value) -> Vec<AgentEvent> {
        let mut out = Vec::new();

        let event_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        // Terminal "result" event is the most reliable "done" indicator in stream-json mode.
        if event_type == "result" {
            self.terminal_emitted = true;
            out.push(AgentEvent::Complete {
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
            });
            return out;
        }

        // Most interesting information arrives as a message object.
        if let Some(message) = value.get("message") {
            out.extend(self.parse_message(message));
            return out;
        }

        out
    }

    fn parse_message(&mut self, message: &serde_json::Value) -> Vec<AgentEvent> {
        let mut out = Vec::new();

        let message_id = message
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Reset delta tracking when message id changes.
        if message_id.is_some() && message_id != self.last_message_id {
            self.last_message_id = message_id.clone();
            self.last_message_text.clear();
        }

        let Some(content) = message.get("content") else {
            return out;
        };

        // Parse blocks: tool_use/tool_result and assistant text.
        if let Some(blocks) = content.as_array() {
            // Tool events can appear in any role; we map them regardless.
            for block in blocks {
                let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match block_type {
                    "tool_use" => {
                        if let Some(event) = tool_use_block_to_event(block) {
                            out.push(event);
                        }
                    }
                    "tool_result" => {
                        if let Some(event) = tool_result_block_to_event(block) {
                            out.push(event);
                        }
                    }
                    _ => {}
                }
            }

            // Emit assistant text deltas (best-effort). We only stream text blocks.
            let role = message.get("role").and_then(|v| v.as_str()).unwrap_or("");
            if role == "assistant" {
                let full_text = blocks
                    .iter()
                    .filter_map(|b| {
                        if b.get("type").and_then(|v| v.as_str()) == Some("text") {
                            b.get("text")
                                .and_then(|t| t.as_str())
                                .map(|s| s.to_string())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("");

                let delta = if full_text.starts_with(&self.last_message_text) {
                    full_text[self.last_message_text.len()..].to_string()
                } else {
                    // If the stream sends non-prefix partials, fall back to emitting the full text.
                    full_text.clone()
                };

                self.last_message_text = full_text;

                if !delta.is_empty() {
                    out.push(AgentEvent::Token { content: delta });
                }
            }
        }

        out
    }
}

fn tool_use_block_to_event(block: &serde_json::Value) -> Option<AgentEvent> {
    let tool_call_id = block.get("id")?.as_str()?.to_string();
    let tool_name = block.get("name")?.as_str()?.to_string();
    let arguments = block
        .get("input")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    Some(AgentEvent::ToolStart {
        tool_call_id,
        tool_name,
        arguments,
    })
}

fn tool_result_block_to_event(block: &serde_json::Value) -> Option<AgentEvent> {
    // Claude's tool_result blocks typically reference the originating tool_use block.
    let tool_call_id = block
        .get("tool_use_id")
        .and_then(|v| v.as_str())
        .or_else(|| block.get("toolUseId").and_then(|v| v.as_str()))?
        .to_string();

    let is_error = block
        .get("is_error")
        .and_then(|v| v.as_bool())
        .or_else(|| block.get("isError").and_then(|v| v.as_bool()))
        .unwrap_or(false);

    let result_str = if let Some(s) = block.get("content").and_then(|v| v.as_str()) {
        s.to_string()
    } else if let Some(arr) = block.get("content").and_then(|v| v.as_array()) {
        // content could be an array of blocks
        arr.iter()
            .filter_map(|b| {
                if b.get("type").and_then(|v| v.as_str()) == Some("text") {
                    b.get("text").and_then(|v| v.as_str())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("")
    } else {
        // As a fallback, serialize the block for visibility.
        serde_json::to_string(block).unwrap_or_default()
    };

    if is_error {
        Some(AgentEvent::ToolError {
            tool_call_id,
            error: result_str,
        })
    } else {
        Some(AgentEvent::ToolComplete {
            tool_call_id,
            result: ToolResult {
                success: true,
                result: result_str,
                display_preference: None,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assistant_text_emits_delta() {
        let mut p = ClaudeStreamJsonParser::default();

        let a = r#"{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"Hello"}]}}"#;
        let events = p.parse_line(a);
        assert!(matches!(events.as_slice(), [AgentEvent::Token { content }] if content == "Hello"));

        let b = r#"{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"Hello world"}]}}"#;
        let events = p.parse_line(b);
        assert!(
            matches!(events.as_slice(), [AgentEvent::Token { content }] if content == " world")
        );
    }

    #[test]
    fn tool_use_maps_to_tool_start() {
        let mut p = ClaudeStreamJsonParser::default();

        let line = r#"{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"read_file","input":{"path":"Cargo.toml"}}]}}"#;
        let events = p.parse_line(line);
        assert!(matches!(
            events.as_slice(),
            [AgentEvent::ToolStart { tool_call_id, tool_name, arguments }]
            if tool_call_id == "call_1"
                && tool_name == "read_file"
                && arguments.get("path").and_then(|v| v.as_str()) == Some("Cargo.toml")
        ));
    }

    #[test]
    fn tool_result_maps_to_tool_complete() {
        let mut p = ClaudeStreamJsonParser::default();

        let line = r#"{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"tool_result","tool_use_id":"call_1","content":"ok"}]}}"#;
        let events = p.parse_line(line);
        assert!(matches!(
            events.as_slice(),
            [AgentEvent::ToolComplete { tool_call_id, result }]
            if tool_call_id == "call_1" && result.success && result.result == "ok"
        ));
    }
}
