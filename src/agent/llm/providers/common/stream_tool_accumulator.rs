use crate::models::{FunctionCall, StreamToolCall, ToolCall};
use std::collections::HashMap;

/// Accumulates streaming tool call fragments into complete tool calls.
///
/// Some OpenAI-compatible providers (e.g. GitHub Copilot) can send tool calls across
/// multiple streaming chunks:
/// - First chunk contains metadata (id, type, function name)
/// - Subsequent chunks contain only argument fragments
///
/// This accumulator collects all fragments by index and converts them into complete
/// [`ToolCall`] objects when the stream finishes.
#[derive(Debug, Default)]
pub struct StreamToolAccumulator {
    /// Maps tool call index to accumulated data
    tool_calls: HashMap<u32, AccumulatedToolCall>,
}

#[derive(Debug, Clone)]
struct AccumulatedToolCall {
    id: Option<String>,
    tool_type: Option<String>,
    name: Option<String>,
    arguments: String, // Accumulated incrementally
}

impl StreamToolAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Process a streaming chunk's tool calls.
    ///
    /// Merges the data from this chunk into the accumulated state. Fields that are already set
    /// won't be overwritten.
    pub fn process_chunk(&mut self, stream_calls: &[StreamToolCall]) {
        for call in stream_calls {
            let entry = self
                .tool_calls
                .entry(call.index)
                .or_insert_with(|| AccumulatedToolCall {
                    id: None,
                    tool_type: None,
                    name: None,
                    arguments: String::new(),
                });

            if let Some(id) = &call.id {
                entry.id = Some(id.clone());
            }
            if let Some(tool_type) = &call.tool_type {
                entry.tool_type = Some(tool_type.clone());
            }
            if let Some(function) = &call.function {
                if let Some(name) = &function.name {
                    entry.name = Some(name.clone());
                }
                if let Some(args) = &function.arguments {
                    entry.arguments.push_str(args);
                }
            }
        }
    }

    /// Convert accumulated data into complete [`ToolCall`] objects, sorted by index.
    ///
    /// Incomplete tool calls (missing required fields) are filtered out.
    pub fn into_tool_calls(self) -> Vec<ToolCall> {
        let mut calls: Vec<_> = self.tool_calls.into_iter().collect();
        calls.sort_by_key(|(index, _)| *index);

        calls
            .into_iter()
            .filter_map(|(_, acc)| {
                Some(ToolCall {
                    id: acc.id?,
                    tool_type: acc.tool_type.unwrap_or_else(|| "function".to_string()),
                    function: FunctionCall {
                        name: acc.name?,
                        arguments: acc.arguments,
                    },
                })
            })
            .collect()
    }

    /// Check if any tool calls are being accumulated.
    pub fn has_tool_calls(&self) -> bool {
        !self.tool_calls.is_empty()
    }

    /// Get the current number of tool calls being accumulated.
    pub fn len(&self) -> usize {
        self.tool_calls.len()
    }

    /// Check if the accumulator is empty.
    pub fn is_empty(&self) -> bool {
        self.tool_calls.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::StreamFunctionCall;

    #[test]
    fn test_basic_accumulation() {
        let mut accumulator = StreamToolAccumulator::new();

        // First chunk with metadata
        let chunk1 = StreamToolCall {
            index: 0,
            id: Some("call_123".to_string()),
            tool_type: Some("function".to_string()),
            function: Some(StreamFunctionCall {
                name: Some("search".to_string()),
                arguments: Some("{\"query".to_string()),
            }),
        };

        // Second chunk with only arguments
        let chunk2 = StreamToolCall {
            index: 0,
            id: None,
            tool_type: None,
            function: Some(StreamFunctionCall {
                name: None,
                arguments: Some("\":\"test\"}".to_string()),
            }),
        };

        accumulator.process_chunk(&[chunk1]);
        accumulator.process_chunk(&[chunk2]);

        let tool_calls = accumulator.into_tool_calls();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "call_123");
        assert_eq!(tool_calls[0].function.name, "search");
        assert_eq!(tool_calls[0].function.arguments, r#"{"query":"test"}"#);
    }

    #[test]
    fn test_multiple_tool_calls() {
        let mut accumulator = StreamToolAccumulator::new();

        // Two tool calls being streamed concurrently
        accumulator.process_chunk(&[
            StreamToolCall {
                index: 0,
                id: Some("call_1".to_string()),
                tool_type: Some("function".to_string()),
                function: Some(StreamFunctionCall {
                    name: Some("search".to_string()),
                    arguments: Some("{\"q\":".to_string()),
                }),
            },
            StreamToolCall {
                index: 1,
                id: Some("call_2".to_string()),
                tool_type: Some("function".to_string()),
                function: Some(StreamFunctionCall {
                    name: Some("create".to_string()),
                    arguments: Some("{\"name\":".to_string()),
                }),
            },
        ]);

        accumulator.process_chunk(&[
            StreamToolCall {
                index: 0,
                id: None,
                tool_type: None,
                function: Some(StreamFunctionCall {
                    name: None,
                    arguments: Some("\"test\"}".to_string()),
                }),
            },
            StreamToolCall {
                index: 1,
                id: None,
                tool_type: None,
                function: Some(StreamFunctionCall {
                    name: None,
                    arguments: Some("\"foo\"}".to_string()),
                }),
            },
        ]);

        let tool_calls = accumulator.into_tool_calls();
        assert_eq!(tool_calls.len(), 2);
        assert_eq!(tool_calls[0].id, "call_1");
        assert_eq!(tool_calls[0].function.name, "search");
        assert_eq!(tool_calls[0].function.arguments, r#"{"q":"test"}"#);
        assert_eq!(tool_calls[1].id, "call_2");
        assert_eq!(tool_calls[1].function.name, "create");
        assert_eq!(tool_calls[1].function.arguments, r#"{"name":"foo"}"#);
    }

    #[test]
    fn test_has_tool_calls() {
        let mut accumulator = StreamToolAccumulator::new();
        assert!(!accumulator.has_tool_calls());

        accumulator.process_chunk(&[StreamToolCall {
            index: 0,
            id: Some("call_123".to_string()),
            tool_type: None,
            function: None,
        }]);

        assert!(accumulator.has_tool_calls());
        assert_eq!(accumulator.len(), 1);
    }

    #[test]
    fn test_incomplete_tool_call_filtered_out() {
        let mut accumulator = StreamToolAccumulator::new();

        // Tool call with arguments but no name (should be filtered)
        accumulator.process_chunk(&[StreamToolCall {
            index: 0,
            id: Some("call_123".to_string()),
            tool_type: Some("function".to_string()),
            function: Some(StreamFunctionCall {
                name: None,
                arguments: Some("{\"test\": true}".to_string()),
            }),
        }]);

        let tool_calls = accumulator.into_tool_calls();
        assert_eq!(
            tool_calls.len(),
            0,
            "Incomplete tool call should be filtered"
        );
    }
}

