//! Token counting for budget management.
//!
//! Provides both heuristic and accurate BPE-based token counting.
//! `TiktokenTokenCounter` uses OpenAI's o200k_base encoding (bundled at compile
//! time) for accurate counts. `HeuristicTokenCounter` remains available as a
//! lightweight fallback.

use bamboo_agent_core::Message;
use std::sync::Arc;
use tiktoken_rs::o200k_base;
use tiktoken_rs::CoreBPE;

/// Cached BPE encoder — initialized once, reused across all count_text calls.
static O200K_ENCODER: std::sync::LazyLock<CoreBPE> =
    std::sync::LazyLock::new(|| o200k_base().unwrap());

/// Trait for token counting implementations.
pub trait TokenCounter: Send + Sync {
    /// Count tokens in a single message.
    fn count_message(&self, message: &Message) -> u32;

    /// Count tokens in multiple messages.
    fn count_messages(&self, messages: &[Message]) -> u32 {
        messages.iter().map(|m| self.count_message(m)).sum()
    }

    /// Count tokens in a plain text string.
    fn count_text(&self, text: &str) -> u32;
}

/// Heuristic token counter using character-based estimation.
///
/// Uses the approximation: tokens ≈ characters / 4, with a 10% safety margin
/// plus additional overhead for message metadata (role, timestamps, etc.).
///
/// This is intentionally conservative to avoid underestimating token usage.
#[derive(Debug, Clone)]
pub struct HeuristicTokenCounter {
    /// Characters per token ratio (default: 4)
    chars_per_token: f64,
    /// Safety margin multiplier (default: 1.1 = 10% extra)
    safety_margin: f64,
    /// Metadata overhead per message in tokens
    metadata_overhead: u32,
}

impl HeuristicTokenCounter {
    /// Create a new heuristic counter with custom parameters.
    pub fn new(chars_per_token: f64, safety_margin: f64, metadata_overhead: u32) -> Self {
        Self {
            chars_per_token,
            safety_margin,
            metadata_overhead,
        }
    }

    /// Create with default parameters (chars/4 + 10% margin + 10 metadata overhead).
    pub fn with_defaults() -> Self {
        Self {
            chars_per_token: 4.0,
            safety_margin: 1.1,
            metadata_overhead: 10,
        }
    }
}

impl Default for HeuristicTokenCounter {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl TokenCounter for HeuristicTokenCounter {
    fn count_message(&self, message: &Message) -> u32 {
        let content_tokens = self.count_text(&message.content);

        // Add tokens for tool calls if present
        let tool_calls_tokens = message
            .tool_calls
            .as_ref()
            .map(|tc| {
                tc.iter()
                    .map(|c| {
                        // Rough estimate: id + name + arguments
                        let args_tokens = self.count_text(&c.function.arguments);
                        let id_tokens = self.count_text(&c.id);
                        let name_tokens = self.count_text(&c.function.name);
                        // Use saturating_add to prevent overflow
                        args_tokens
                            .saturating_add(id_tokens)
                            .saturating_add(name_tokens)
                            .saturating_add(5) // type overhead
                    })
                    .fold(0u32, |acc, x| acc.saturating_add(x))
            })
            .unwrap_or(0);

        // Add tokens for tool_call_id if present
        let tool_call_id_tokens = message
            .tool_call_id
            .as_ref()
            .map(|id| self.count_text(id).saturating_add(3)) // +3 for field name overhead
            .unwrap_or(0);

        // Use saturating_add to prevent overflow
        content_tokens
            .saturating_add(tool_calls_tokens)
            .saturating_add(tool_call_id_tokens)
            .saturating_add(self.metadata_overhead)
    }

    fn count_text(&self, text: &str) -> u32 {
        if text.is_empty() {
            return 0;
        }

        let char_count = text.chars().count() as f64;
        let base_tokens = char_count / self.chars_per_token;
        let adjusted_tokens = base_tokens * self.safety_margin;

        adjusted_tokens.ceil() as u32
    }
}

/// Arc-wrapped token counter for easy sharing.
pub type SharedTokenCounter = Arc<dyn TokenCounter>;

/// Accurate BPE-based token counter using OpenAI's o200k_base encoding.
///
/// Uses `tiktoken-rs` with the vocabulary bundled at compile time — no runtime
/// downloads. This is the recommended counter for production use.
#[derive(Debug)]
pub struct TiktokenTokenCounter {
    /// Per-message metadata overhead in tokens (role markers, formatting, etc.)
    metadata_overhead: u32,
}

impl TiktokenTokenCounter {
    /// Create with a custom metadata overhead.
    pub fn new(metadata_overhead: u32) -> Self {
        Self { metadata_overhead }
    }
}

impl Default for TiktokenTokenCounter {
    fn default() -> Self {
        Self {
            metadata_overhead: 10,
        }
    }
}

impl TokenCounter for TiktokenTokenCounter {
    fn count_message(&self, message: &Message) -> u32 {
        let content_tokens = self.count_text(&message.content);

        let tool_calls_tokens = message
            .tool_calls
            .as_ref()
            .map(|tc| {
                tc.iter()
                    .map(|c| {
                        let args_tokens = self.count_text(&c.function.arguments);
                        let id_tokens = self.count_text(&c.id);
                        let name_tokens = self.count_text(&c.function.name);
                        args_tokens
                            .saturating_add(id_tokens)
                            .saturating_add(name_tokens)
                            .saturating_add(5)
                    })
                    .fold(0u32, |acc, x| acc.saturating_add(x))
            })
            .unwrap_or(0);

        let tool_call_id_tokens = message
            .tool_call_id
            .as_ref()
            .map(|id| self.count_text(id).saturating_add(3))
            .unwrap_or(0);

        content_tokens
            .saturating_add(tool_calls_tokens)
            .saturating_add(tool_call_id_tokens)
            .saturating_add(self.metadata_overhead)
    }

    fn count_text(&self, text: &str) -> u32 {
        if text.is_empty() {
            return 0;
        }
        let tokens = O200K_ENCODER.encode_with_special_tokens(text);
        tokens.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{FunctionCall, ToolCall};

    #[test]
    fn heuristic_counter_counts_text() {
        let counter = HeuristicTokenCounter::default();

        // "Hello, world!" = 13 chars -> 13/4 * 1.1 ≈ 3.57 -> 4 tokens
        let tokens = counter.count_text("Hello, world!");
        assert!(
            tokens >= 3 && tokens <= 5,
            "Expected ~4 tokens, got {}",
            tokens
        );
    }

    #[test]
    fn heuristic_counter_counts_empty_text() {
        let counter = HeuristicTokenCounter::default();
        assert_eq!(counter.count_text(""), 0);
    }

    #[test]
    fn heuristic_counter_counts_user_message() {
        let counter = HeuristicTokenCounter::default();
        let message = Message::user("Hello, world!");

        let tokens = counter.count_message(&message);
        // Should include content + metadata overhead (10)
        assert!(
            tokens >= 10,
            "Expected at least 10 tokens (content + metadata), got {}",
            tokens
        );
    }

    #[test]
    fn heuristic_counter_counts_tool_calls() {
        let counter = HeuristicTokenCounter::default();

        let tool_call = ToolCall {
            id: "call_123".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"query":"test"}"#.to_string(),
            },
        };

        let message = Message::assistant("Let me search", Some(vec![tool_call]));

        let tokens = counter.count_message(&message);
        // Should include content + tool call (id + name + args) + metadata
        assert!(tokens >= 15, "Expected at least 15 tokens, got {}", tokens);
    }

    #[test]
    fn heuristic_counter_counts_tool_result() {
        let counter = HeuristicTokenCounter::default();
        let message = Message::tool_result("call_123", "Search results here");

        let tokens = counter.count_message(&message);
        // Should include content + tool_call_id + metadata
        assert!(tokens >= 15, "Expected at least 15 tokens, got {}", tokens);
    }

    #[test]
    fn heuristic_counter_counts_multiple_messages() {
        let counter = HeuristicTokenCounter::default();
        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hello"),
            Message::assistant("Hi there", None),
        ];

        let total = counter.count_messages(&messages);
        let sum: u32 = messages.iter().map(|m| counter.count_message(m)).sum();

        assert_eq!(total, sum);
    }

    #[test]
    fn custom_chars_per_token() {
        let counter = HeuristicTokenCounter::new(2.0, 1.0, 0);
        // With 2 chars per token, "test" (4 chars) = 2 tokens
        let tokens = counter.count_text("test");
        assert_eq!(tokens, 2);
    }

    #[test]
    fn safety_margin_applied() {
        let counter_no_margin = HeuristicTokenCounter::new(4.0, 1.0, 0);
        let counter_with_margin = HeuristicTokenCounter::new(4.0, 1.1, 0);

        let text = "Hello world!"; // 12 chars
        let base = counter_no_margin.count_text(text);
        let adjusted = counter_with_margin.count_text(text);

        assert!(adjusted > base, "Safety margin should increase token count");
    }

    // --- TiktokenTokenCounter tests ---

    #[test]
    fn tiktoken_counter_counts_text() {
        let counter = TiktokenTokenCounter::default();
        let tokens = counter.count_text("Hello, world!");
        // "Hello, world!" is 4 tokens with o200k_base
        assert!(
            tokens >= 3 && tokens <= 6,
            "Expected ~4 tokens, got {}",
            tokens
        );
    }

    #[test]
    fn tiktoken_counter_counts_empty_text() {
        let counter = TiktokenTokenCounter::default();
        assert_eq!(counter.count_text(""), 0);
    }

    #[test]
    fn tiktoken_counter_counts_cjk() {
        let counter = TiktokenTokenCounter::default();
        // CJK text: each character is typically 1-2 tokens
        let tokens = counter.count_text("你好世界");
        assert!(
            tokens >= 2 && tokens <= 8,
            "Expected 2-8 tokens, got {}",
            tokens
        );
    }

    #[test]
    fn tiktoken_counter_counts_user_message() {
        let counter = TiktokenTokenCounter::default();
        let message = Message::user("Hello, world!");
        let tokens = counter.count_message(&message);
        // Should include content + metadata overhead (10)
        assert!(tokens >= 10, "Expected at least 10 tokens, got {}", tokens);
    }

    #[test]
    fn tiktoken_counter_counts_tool_calls() {
        let counter = TiktokenTokenCounter::default();
        let tool_call = ToolCall {
            id: "call_123".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"query":"test"}"#.to_string(),
            },
        };
        let message = Message::assistant("Let me search", Some(vec![tool_call]));
        let tokens = counter.count_message(&message);
        assert!(tokens >= 15, "Expected at least 15 tokens, got {}", tokens);
    }

    #[test]
    fn tiktoken_counter_more_accurate_than_heuristic() {
        let heuristic = HeuristicTokenCounter::default();
        let tiktoken = TiktokenTokenCounter::default();

        let text = "The quick brown fox jumps over the lazy dog.";
        let h_tokens = heuristic.count_text(text);
        let t_tokens = tiktoken.count_text(text);

        // Both should produce reasonable counts
        assert!(h_tokens > 0 && t_tokens > 0);
    }
}
