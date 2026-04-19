//! Message segmentation for budget management.
//!
//! Groups messages into atomic segments, ensuring tool-call chains stay together.
//! This prevents protocol errors that occur when tool results are included without
//! their corresponding tool calls.

use bamboo_agent_core::{Message, Role};
use std::collections::HashSet;

/// A segment of conversation that should be treated as atomic during truncation.
///
/// Ensures tool-call relationships are preserved: an assistant's tool_call
/// and the corresponding tool results form a single segment.
#[derive(Debug, Clone)]
pub struct MessageSegment {
    /// Messages in this segment
    pub messages: Vec<Message>,
    /// Unique tool call IDs referenced in this segment
    pub tool_call_ids: HashSet<String>,
    /// Whether this segment contains a tool call chain (assistant call + results)
    pub is_tool_chain: bool,
    /// Approximate token count (for sorting/filtering)
    pub token_estimate: u32,
}

impl MessageSegment {
    /// Create a new segment containing a single message.
    pub fn from_message(message: Message) -> Self {
        let tool_call_ids = extract_tool_call_ids(&message);
        let is_tool_chain = !tool_call_ids.is_empty();
        Self {
            messages: vec![message],
            tool_call_ids,
            is_tool_chain,
            token_estimate: 0, // Will be set later by counter
        }
    }

    /// Merge another segment into this one.
    pub fn merge(&mut self, other: MessageSegment) {
        self.messages.extend(other.messages);
        self.tool_call_ids.extend(other.tool_call_ids);
        self.is_tool_chain = !self.tool_call_ids.is_empty();
        self.token_estimate += other.token_estimate;
    }

    /// Check if this segment contains a tool result for the given tool call ID.
    pub fn contains_tool_result(&self, tool_call_id: &str) -> bool {
        self.messages
            .iter()
            .any(|m| m.role == Role::Tool && m.tool_call_id.as_deref() == Some(tool_call_id))
    }

    /// Check if this segment contains the tool call (assistant message) for the given ID.
    pub fn contains_tool_call(&self, tool_call_id: &str) -> bool {
        self.messages.iter().any(|m| {
            m.role == Role::Assistant
                && m.tool_calls
                    .as_ref()
                    .is_some_and(|tc| tc.iter().any(|c| c.id == tool_call_id))
        })
    }

    /// Get the IDs of tool calls that are missing their results in this segment.
    pub fn get_missing_results(&self) -> Vec<&str> {
        self.tool_call_ids
            .iter()
            .filter(|id| !self.contains_tool_result(id))
            .map(|id| id.as_str())
            .collect()
    }
}

/// Extracts tool call IDs from a message.
fn extract_tool_call_ids(message: &Message) -> HashSet<String> {
    let mut ids = HashSet::new();

    // Tool results reference a tool call
    if let Some(ref id) = message.tool_call_id {
        ids.insert(id.clone());
    }

    // Assistant messages with tool calls
    if let Some(ref calls) = message.tool_calls {
        for call in calls {
            ids.insert(call.id.clone());
        }
    }

    ids
}

/// Segments messages into atomic units for budget management.
///
/// # Algorithm
///
/// 1. Iterate through messages
/// 2. When finding an assistant message with tool_calls, start a segment
/// 3. Continue adding messages to the segment until all tool results are collected
/// 4. Handle edge cases: orphan tool results, standalone messages
#[derive(Debug)]
pub struct MessageSegmenter;

impl MessageSegmenter {
    /// Create a new segmenter.
    pub fn new() -> Self {
        Self
    }

    /// Segment messages, ensuring tool-call chains stay together.
    ///
    /// Returns segments in chronological order (oldest first).
    pub fn segment(&self, messages: Vec<Message>) -> Vec<MessageSegment> {
        let mut segments: Vec<MessageSegment> = Vec::new();
        let mut current_segment: Option<MessageSegment> = None;
        let mut pending_tool_calls: HashSet<String> = HashSet::new();

        for message in messages {
            match message.role {
                // System messages are handled separately (always included)
                Role::System => {
                    // System messages don't go into segments - they're handled separately
                    continue;
                }

                // User and Tool messages
                Role::User | Role::Tool => {
                    if let Some(ref mut seg) = current_segment {
                        // Check if this is a tool result for a pending tool call
                        if message.role == Role::Tool {
                            if let Some(ref tool_call_id) = message.tool_call_id {
                                let tool_call_id = tool_call_id.clone();
                                if pending_tool_calls.contains(&tool_call_id) {
                                    seg.messages.push(message);
                                    pending_tool_calls.remove(&tool_call_id);

                                    // If all results collected, close the segment
                                    if pending_tool_calls.is_empty() {
                                        if let Some(seg) = current_segment.take() {
                                            segments.push(seg);
                                        }
                                    }
                                    continue;
                                }
                            }
                        }

                        // Not part of tool chain - close current segment and start new
                        if !pending_tool_calls.is_empty() {
                            // We have an incomplete tool chain - log warning and continue
                            // This can happen if tool execution was interrupted
                            tracing::warn!(
                                "Incomplete tool chain for tool calls: {:?}",
                                pending_tool_calls
                            );
                        }
                        if let Some(seg) = current_segment.take() {
                            segments.push(seg);
                        }
                    }

                    // Start new standalone segment for this message
                    if message.role == Role::Tool {
                        // Orphan tool result - this shouldn't happen but handle gracefully
                        tracing::warn!(
                            "Orphan tool result without preceding tool call: {:?}",
                            message.tool_call_id
                        );
                        // Still create a segment for it to avoid losing data
                    }
                    segments.push(MessageSegment::from_message(message));
                }

                // Assistant messages
                Role::Assistant => {
                    // Check if this assistant is responding to a user message
                    // (no tool calls = standalone message)
                    let has_tool_calls = message
                        .tool_calls
                        .as_ref()
                        .is_some_and(|calls| !calls.is_empty());

                    if !has_tool_calls {
                        // Close any pending segment
                        if let Some(seg) = current_segment.take() {
                            if !pending_tool_calls.is_empty() {
                                tracing::warn!(
                                    "Tool chain interrupted by assistant message: {:?}",
                                    pending_tool_calls
                                );
                            }
                            segments.push(seg);
                        }
                        // Create standalone segment
                        segments.push(MessageSegment::from_message(message));
                    } else {
                        // Close any pending segment
                        if let Some(seg) = current_segment.take() {
                            if !pending_tool_calls.is_empty() {
                                tracing::warn!(
                                    "Tool chain interrupted by new tool call: {:?}",
                                    pending_tool_calls
                                );
                            }
                            segments.push(seg);
                        }

                        // Start new tool-call segment
                        let mut new_seg = MessageSegment::from_message(message.clone());

                        // Collect pending tool calls
                        if let Some(ref calls) = message.tool_calls {
                            for call in calls {
                                pending_tool_calls.insert(call.id.clone());
                            }
                            new_seg.is_tool_chain = true;
                        }

                        current_segment = Some(new_seg);
                    }
                }
            }
        }

        // Close any remaining segment
        if let Some(seg) = current_segment.take() {
            if !pending_tool_calls.is_empty() {
                tracing::warn!(
                    "Session ended with incomplete tool chain: {:?}",
                    pending_tool_calls
                );
            }
            segments.push(seg);
        }

        segments
    }

    /// Segment messages including system messages in a separate collection.
    ///
    /// Returns (system_messages, segments) tuple.
    pub fn segment_with_system(
        &self,
        messages: Vec<Message>,
    ) -> (Vec<Message>, Vec<MessageSegment>) {
        let system_messages: Vec<Message> = messages
            .iter()
            .filter(|m| m.role == Role::System)
            .cloned()
            .collect();

        let non_system: Vec<Message> = messages
            .into_iter()
            .filter(|m| m.role != Role::System)
            .collect();

        let segments = self.segment(non_system);
        (system_messages, segments)
    }
}

impl Default for MessageSegmenter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Message;
    use bamboo_agent_core::{FunctionCall, ToolCall};

    fn create_tool_call(id: &str, name: &str, args: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    #[test]
    fn segments_simple_conversation() {
        let segmenter = MessageSegmenter::new();
        let messages = vec![
            Message::user("Hello"),
            Message::assistant("Hi there", None),
            Message::user("How are you?"),
        ];

        let segments = segmenter.segment(messages);

        assert_eq!(segments.len(), 3, "Expected 3 separate segments");
        assert!(!segments[0].is_tool_chain);
        assert!(!segments[1].is_tool_chain);
        assert!(!segments[2].is_tool_chain);
    }

    #[test]
    fn segments_tool_call_chain() {
        let segmenter = MessageSegmenter::new();
        let messages = vec![
            Message::user("Search for something"),
            Message::assistant(
                "Let me search",
                Some(vec![create_tool_call(
                    "call_1",
                    "search",
                    r#"{"q":"test"}"#,
                )]),
            ),
            Message::tool_result("call_1", "Here are the results..."),
        ];

        let segments = segmenter.segment(messages);

        assert_eq!(segments.len(), 2, "Expected 2 segments (user + tool chain)");
        assert!(!segments[0].is_tool_chain);
        assert!(segments[1].is_tool_chain);
        assert_eq!(segments[1].messages.len(), 2); // assistant + tool result
    }

    #[test]
    fn segments_multiple_tool_calls() {
        let segmenter = MessageSegmenter::new();
        let messages = vec![
            Message::user("Do multiple things"),
            Message::assistant(
                "I'll help",
                Some(vec![
                    create_tool_call("call_1", "search", r#"{"q":"a"}"#),
                    create_tool_call("call_2", "read", r#"{"file":"test.txt"}"#),
                ]),
            ),
            Message::tool_result("call_1", "Search results..."),
            Message::tool_result("call_2", "File contents..."),
        ];

        let segments = segmenter.segment(messages);

        assert_eq!(segments.len(), 2);
        assert!(segments[1].is_tool_chain);
        assert_eq!(segments[1].messages.len(), 3); // assistant + 2 results
        assert_eq!(segments[1].tool_call_ids.len(), 2);
    }

    #[test]
    fn handles_orphan_tool_result() {
        let segmenter = MessageSegmenter::new();
        let messages = vec![
            Message::user("Hello"),
            Message::tool_result("orphan_call", "Some result"),
        ];

        let segments = segmenter.segment(messages);

        assert_eq!(segments.len(), 2);
        // Orphan tool result gets its own segment
        assert_eq!(segments[1].messages.len(), 1);
    }

    #[test]
    fn handles_system_messages_separately() {
        let segmenter = MessageSegmenter::new();
        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hello"),
            Message::assistant("Hi", None),
        ];

        let (system, segments) = segmenter.segment_with_system(messages);

        assert_eq!(system.len(), 1);
        assert_eq!(segments.len(), 2);
    }

    #[test]
    fn segments_multiple_interleaved_tool_chains() {
        let segmenter = MessageSegmenter::new();
        let messages = vec![
            Message::user("First task"),
            Message::assistant(
                "Doing first",
                Some(vec![create_tool_call("call_1", "search", "{}")]),
            ),
            Message::tool_result("call_1", "Result 1"),
            Message::user("Second task"),
            Message::assistant(
                "Doing second",
                Some(vec![create_tool_call("call_2", "read", "{}")]),
            ),
            Message::tool_result("call_2", "Result 2"),
        ];

        let segments = segmenter.segment(messages);

        assert_eq!(segments.len(), 4);
        // segments[0] = user("First task")
        // segments[1] = tool chain 1
        // segments[2] = user("Second task")
        // segments[3] = tool chain 2
        assert!(segments[1].is_tool_chain);
        assert!(segments[3].is_tool_chain);
    }

    #[test]
    fn empty_messages_produces_empty_segments() {
        let segmenter = MessageSegmenter::new();
        let segments = segmenter.segment(vec![]);
        assert!(segments.is_empty());
    }
}
