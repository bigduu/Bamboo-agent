//! Tool call accumulator for handling streaming LLM responses
//!
//! This module provides utilities for accumulating partial tool calls from streaming
//! LLM responses. When LLMs return tool calls in chunks, this module helps reconstruct
//! the complete tool call by merging partial updates.
//!
//! # Example
//!
//! ```
//! use bamboo::agent::core::tools::accumulator::ToolCallAccumulator;
//! use bamboo::agent::core::tools::ToolCall;
//!
//! let mut accumulator = ToolCallAccumulator::new();
//!
//! // Accumulate partial tool calls from streaming response
//! accumulator.update(ToolCall {
//!     id: "call_1".to_string(),
//!     tool_type: "function".to_string(),
//!     function: FunctionCall {
//!         name: "execute_command".to_string(),
//!         arguments: "{\"command\": \"".to_string(),
//!     },
//! });
//!
//! accumulator.update(ToolCall {
//!     id: "call_1".to_string(),
//!     tool_type: "function".to_string(),
//!     function: FunctionCall {
//!         name: String::new(),
//!         arguments: "echo hello".to_string(),
//!     },
//! });
//!
//! // Finalize to get complete tool calls
//! let complete_calls = accumulator.finalize();
//! ```

use uuid::Uuid;

use crate::agent::core::tools::{FunctionCall, ToolCall};

/// Represents a partially accumulated tool call during streaming
///
/// This struct holds the intermediate state of a tool call as it's being
/// streamed from the LLM. The arguments field grows as chunks are received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialToolCall {
    /// Unique identifier for the tool call (may be empty in early chunks)
    pub id: String,

    /// Type of tool (typically "function")
    pub tool_type: String,

    /// Name of the tool to invoke (may be empty in early chunks)
    pub name: String,

    /// Accumulated arguments string (grows with each update)
    pub arguments: String,
}

/// Accumulator for reconstructing complete tool calls from streaming chunks
///
/// This struct manages the accumulation of partial tool calls received from
/// streaming LLM responses. It handles merging chunks that belong to the same
/// tool call and can finalize them into complete `ToolCall` instances.
///
/// # Features
///
/// - Merges partial arguments for the same tool call
/// - Handles tool calls with or without IDs
/// - Generates UUIDs for tool calls missing IDs during finalization
/// - Filters out incomplete tool calls (missing name)
#[derive(Debug, Default, Clone)]
pub struct ToolCallAccumulator {
    /// List of partial tool calls being accumulated
    parts: Vec<PartialToolCall>,
}

impl ToolCallAccumulator {
    /// Creates a new empty accumulator
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates the accumulator with a single tool call chunk
    ///
    /// This method merges the incoming tool call with existing partial calls
    /// based on ID or name matching. If no match is found, a new partial call
    /// is created.
    ///
    /// # Arguments
    ///
    /// * `call` - Tool call chunk to accumulate
    pub fn update(&mut self, call: ToolCall) {
        update_partial_tool_call(&mut self.parts, call);
    }

    /// Updates the accumulator with multiple tool call chunks
    ///
    /// # Arguments
    ///
    /// * `calls` - Iterator of tool call chunks to accumulate
    pub fn extend<I>(&mut self, calls: I)
    where
        I: IntoIterator<Item = ToolCall>,
    {
        for call in calls {
            self.update(call);
        }
    }

    /// Finalizes accumulated partial calls into complete tool calls
    ///
    /// This method:
    /// - Filters out incomplete tool calls (those without a name)
    /// - Generates UUIDs for tool calls missing IDs
    /// - Sets default "function" type for calls missing tool_type
    ///
    /// # Returns
    ///
    /// Vector of complete, valid tool calls ready for execution
    pub fn finalize(self) -> Vec<ToolCall> {
        finalize_tool_calls(self.parts)
    }

    /// Returns a reference to the current partial tool calls
    pub fn parts(&self) -> &[PartialToolCall] {
        &self.parts
    }

    /// Returns true if no partial calls have been accumulated
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

/// Updates a list of partial tool calls with a new tool call chunk
///
/// This function implements the core merging logic for accumulating tool calls:
///
/// 1. Empty chunks (no id, name, or arguments) are ignored
/// 2. Chunks with only arguments extend the last partial call if it exists
/// 3. Chunks with ID or name are matched to existing partials and merged
/// 4. Unmatched chunks create new partial calls
///
/// # Arguments
///
/// * `parts` - Mutable reference to the list of partial tool calls
/// * `call` - New tool call chunk to merge
///
/// # Example
///
/// ```
/// use bamboo::agent::core::tools::accumulator::{update_partial_tool_call, PartialToolCall};
/// use bamboo::agent::core::tools::ToolCall;
///
/// let mut parts = Vec::new();
///
/// // First chunk with ID and name
/// update_partial_tool_call(&mut parts, ToolCall {
///     id: "call_1".to_string(),
///     tool_type: "function".to_string(),
///     function: FunctionCall {
///         name: "read_file".to_string(),
///         arguments: "{\"path\": \"/tmp".to_string(),
///     },
/// });
///
/// // Second chunk extends arguments
/// update_partial_tool_call(&mut parts, ToolCall {
///     id: "call_1".to_string(),
///     tool_type: "function".to_string(),
///     function: FunctionCall {
///         name: String::new(),
///         arguments: "/file.txt\"}".to_string(),
///     },
/// });
///
/// assert_eq!(parts[0].arguments, "{\"path\": \"/tmp/file.txt\"}");
/// ```
pub fn update_partial_tool_call(parts: &mut Vec<PartialToolCall>, call: ToolCall) {
    if call.id.is_empty() && call.function.name.is_empty() && call.function.arguments.is_empty() {
        return;
    }

    if call.id.is_empty() && call.function.name.is_empty() {
        if let Some(last) = parts.last_mut() {
            last.arguments.push_str(&call.function.arguments);
        } else {
            parts.push(PartialToolCall {
                id: String::new(),
                tool_type: call.tool_type.clone(),
                name: String::new(),
                arguments: call.function.arguments.clone(),
            });
        }
        return;
    }

    let existing = if !call.id.is_empty() {
        parts.iter_mut().find(|part| part.id == call.id)
    } else if !call.function.name.is_empty() {
        parts.iter_mut().find(|part| {
            (part.id.is_empty() && part.name == call.function.name)
                || (part.id.is_empty() && part.name.is_empty())
        })
    } else {
        None
    };

    if let Some(existing) = existing {
        existing.arguments.push_str(&call.function.arguments);

        if !call.function.name.is_empty() {
            existing.name = call.function.name.clone();
        }

        if !call.tool_type.is_empty() {
            existing.tool_type = call.tool_type.clone();
        }
    } else {
        parts.push(PartialToolCall {
            id: call.id.clone(),
            tool_type: call.tool_type.clone(),
            name: call.function.name.clone(),
            arguments: call.function.arguments.clone(),
        });
    }
}

/// Converts partial tool calls into complete, valid tool calls
///
/// This function finalizes the accumulation process by:
///
/// 1. Filtering out incomplete calls (those with empty or whitespace-only names)
/// 2. Generating UUIDs for calls missing IDs (format: "call_{uuid}")
/// 3. Setting default "function" type for calls missing tool_type
///
/// # Arguments
///
/// * `parts` - Vector of partial tool calls to finalize
///
/// # Returns
///
/// Vector of complete `ToolCall` instances ready for execution
///
/// # Example
///
/// ```
/// use bamboo::agent::core::tools::accumulator::{finalize_tool_calls, PartialToolCall};
///
/// let parts = vec![
///     PartialToolCall {
///         id: String::new(),
///         tool_type: String::new(),
///         name: "execute_command".to_string(),
///         arguments: "{\"cmd\": \"ls\"}".to_string(),
///     },
/// ];
///
/// let calls = finalize_tool_calls(parts);
/// assert_eq!(calls.len(), 1);
/// assert!(calls[0].id.starts_with("call_"));
/// assert_eq!(calls[0].tool_type, "function");
/// ```
pub fn finalize_tool_calls(parts: Vec<PartialToolCall>) -> Vec<ToolCall> {
    parts
        .into_iter()
        .filter(|part| !part.name.trim().is_empty())
        .map(|part| ToolCall {
            id: if part.id.is_empty() {
                format!("call_{}", Uuid::new_v4())
            } else {
                part.id
            },
            tool_type: if part.tool_type.is_empty() {
                "function".to_string()
            } else {
                part.tool_type
            },
            function: FunctionCall {
                name: part.name,
                arguments: part.arguments,
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tool_call(id: &str, name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn accumulator_merges_partial_arguments() {
        let mut accumulator = ToolCallAccumulator::new();

        accumulator.update(make_tool_call(
            "call_1",
            "execute_command",
            "{\"command\": \"",
        ));
        accumulator.update(make_tool_call("call_1", "", "echo hello"));
        accumulator.update(make_tool_call("call_1", "", "\"}"));

        let calls = accumulator.finalize();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "execute_command");
        assert_eq!(calls[0].function.arguments, "{\"command\": \"echo hello\"}");
    }

    #[test]
    fn finalize_skips_calls_without_tool_name() {
        let mut parts = Vec::new();
        update_partial_tool_call(
            &mut parts,
            ToolCall {
                id: "call_1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: String::new(),
                    arguments: "{}".to_string(),
                },
            },
        );

        let calls = finalize_tool_calls(parts);
        assert!(calls.is_empty());
    }

    #[test]
    fn argument_only_chunk_extends_last_partial() {
        let mut parts = Vec::new();
        update_partial_tool_call(
            &mut parts,
            make_tool_call("call_1", "execute_command", "{\"a\":"),
        );
        update_partial_tool_call(&mut parts, make_tool_call("", "", "1}"));

        let calls = finalize_tool_calls(parts);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.arguments, "{\"a\":1}");
    }
}
