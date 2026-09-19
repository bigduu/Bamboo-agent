//! Token counting for budget management.
//!
//! Provides both heuristic and accurate BPE-based token counting.
//! `TiktokenTokenCounter` uses OpenAI's o200k_base encoding (bundled at compile
//! time) for accurate counts. `HeuristicTokenCounter` remains available as a
//! lightweight fallback and is also used automatically if the bundled BPE
//! vocabulary ever fails to load, so a failed load degrades gracefully rather
//! than panicking.

use std::sync::OnceLock;

use bamboo_domain::Message;
use tiktoken_rs::o200k_base;
use tiktoken_rs::CoreBPE;

/// Cached BPE encoder — initialized once, reused across all count_text calls.
///
/// Holds `None` if the bundled o200k_base vocabulary failed to load (e.g. a
/// corrupt or unlinkable build). When `None`, `TiktokenTokenCounter` falls back
/// to `HeuristicTokenCounter` instead of panicking. The failure is logged once
/// at initialization time so the degradation is observable.
static O200K_ENCODER: OnceLock<Option<CoreBPE>> = OnceLock::new();

/// Returns the cached o200k_base encoder, or `None` if it failed to load.
///
/// The first call loads (or attempts to load) the bundled vocabulary exactly
/// once; a load failure is logged a single time and cached as `None`, so the
/// hot path never panics and never re-attempts the failing load.
fn o200k_encoder() -> Option<&'static CoreBPE> {
    O200K_ENCODER
        .get_or_init(|| match o200k_base() {
            Ok(encoder) => Some(encoder),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to load bundled o200k_base tokenizer; \
                     falling back to heuristic token counting"
                );
                None
            }
        })
        .as_ref()
}

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

/// Deterministic aggregate for ordered provider-visible tool positions.
///
/// This is a local context-window estimate, not provider-authoritative billing
/// usage. The provider-reported usage remains authoritative after dispatch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderVisibleToolEstimate {
    /// Saturating token sum across the ordered positions.
    pub input_tokens: u32,
    /// Saturating UTF-8 byte sum across the ordered positions.
    pub serialized_bytes: usize,
    /// Saturating Unicode scalar-value sum across the ordered positions.
    pub serialized_chars: usize,
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

    /// Estimate already-lowered provider-visible tool segments exactly once.
    ///
    /// `segments` must contain compact serialized JSON/text in the order the
    /// selected provider exposes it to the model. Each segment is one contiguous
    /// model position and is tokenized separately with the same counter used for
    /// prompt text; provider lowering must serialize adjacent content such as an
    /// initial tools array into one segment. This prevents BPE merges across
    /// positions separated by transcript content.
    ///
    /// Repeated fragments at different model-visible positions remain repeated
    /// by design; callers must omit definitions that are not model-visible at
    /// that position. An empty slice returns an all-zero estimate.
    pub fn estimate_provider_visible_tool_segments<'a>(
        &self,
        segments: impl IntoIterator<Item = &'a str>,
    ) -> ProviderVisibleToolEstimate {
        let mut estimate = ProviderVisibleToolEstimate::default();
        for segment in segments {
            estimate.input_tokens = estimate
                .input_tokens
                .saturating_add(self.count_text(segment));
            estimate.serialized_bytes = estimate.serialized_bytes.saturating_add(segment.len());
            estimate.serialized_chars = estimate
                .serialized_chars
                .saturating_add(segment.chars().count());
        }
        estimate
    }

    /// Truncate `text` to at most `max_tokens` tokens, keeping the START.
    ///
    /// Encodes the text **once** and decodes the first `max_tokens` tokens back
    /// to a string — O(N) (one encode + one decode), versus the O(N²)
    /// char-by-char re-tokenization the previous `find_prefix_within_tokens`
    /// performed (which called `count_text(&text[..i])` on every char index).
    ///
    /// # Semantics
    /// - `max_tokens == 0` → empty string (exactly 0 tokens; never exceeds budget).
    /// - Text already within `max_tokens` → returned unchanged (fast path).
    /// - Otherwise the result is an exact prefix of `text` (its START preserved),
    ///   is valid UTF-8, and re-counts to ≤ `max_tokens`.
    ///
    /// If the o200k encoder is unavailable (the issue #25 fallback path), this
    /// degrades to a conservative char-based cut instead of panicking.
    pub fn truncate_to_token_prefix(&self, text: &str, max_tokens: u32) -> String {
        if max_tokens == 0 {
            return String::new();
        }
        let Some(encoder) = o200k_encoder() else {
            return heuristic_char_prefix(text, max_tokens);
        };
        // One encode — same encoder `count_text` uses, so the fast-path length
        // check is consistent with `count_text(text)`.
        let tokens = encoder.encode_with_special_tokens(text);
        if (tokens.len() as u32) <= max_tokens {
            return text.to_string();
        }
        let end = max_tokens as usize;
        match encoder.decode_bytes(&tokens[..end]) {
            // `decode_bytes` yields exactly the bytes of `text` spanned by the
            // first `end` tokens — a byte-prefix of `text`. A token boundary can
            // fall inside a multi-byte UTF-8 char, so trim any partial trailing
            // char: the result stays a valid-UTF-8 exact prefix of `text`.
            Ok(bytes) => valid_utf8_prefix(bytes),
            Err(_) => heuristic_char_prefix(text, max_tokens),
        }
    }

    /// Truncate `text` to at most `max_tokens` tokens, keeping the END.
    ///
    /// Symmetric to [`truncate_to_token_prefix`](Self::truncate_to_token_prefix):
    /// encodes once and decodes the **last** `max_tokens` tokens. Same budget /
    /// fast-path / fallback semantics; the result is a valid-UTF-8 exact suffix
    /// of `text` (its END preserved) that re-counts to ≤ `max_tokens`.
    pub fn truncate_to_token_suffix(&self, text: &str, max_tokens: u32) -> String {
        if max_tokens == 0 {
            return String::new();
        }
        let Some(encoder) = o200k_encoder() else {
            return heuristic_char_suffix(text, max_tokens);
        };
        let tokens = encoder.encode_with_special_tokens(text);
        if (tokens.len() as u32) <= max_tokens {
            return text.to_string();
        }
        let start = tokens.len() - (max_tokens as usize);
        match encoder.decode_bytes(&tokens[start..]) {
            // The last `max_tokens` tokens span a byte-suffix of `text`; a
            // boundary may split a *leading* multi-byte char, so drop any partial
            // leading bytes to keep the result valid UTF-8 (still an exact suffix
            // of `text`).
            Ok(bytes) => valid_utf8_suffix(bytes),
            Err(_) => heuristic_char_suffix(text, max_tokens),
        }
    }
}

// ── Encode-once truncation helpers ───────────────────────────────────────────
//
// These are used only by `TiktokenTokenCounter::truncate_to_token_{prefix,suffix}`.

/// Conservative char-based prefix used solely when the BPE encoder is
/// unavailable (the issue #25 fallback). Sized so the `HeuristicTokenCounter`
/// estimate (chars/4 · 1.1) stays within budget.
fn heuristic_char_prefix(text: &str, max_tokens: u32) -> String {
    text.chars()
        .take(heuristic_char_budget(max_tokens))
        .collect()
}

/// Conservative char-based suffix — symmetric to [`heuristic_char_prefix`].
fn heuristic_char_suffix(text: &str, max_tokens: u32) -> String {
    let max_chars = heuristic_char_budget(max_tokens);
    let skip = text.chars().count().saturating_sub(max_chars);
    text.chars().skip(skip).collect()
}

/// Number of chars whose heuristic token estimate (ceil(chars/4 · 1.1)) is
/// ≤ `max_tokens`: solves ceil(c/4 · 1.1) ≤ max_tokens ⟺ c ≤ max_tokens·4/1.1.
fn heuristic_char_budget(max_tokens: u32) -> usize {
    ((max_tokens as f64) * 4.0 / 1.1).floor() as usize
}

/// Turn a byte-prefix of some valid UTF-8 text into a valid UTF-8 string,
/// trimming a partial trailing multi-byte char if the token boundary landed
/// mid-character. The result is still an exact prefix of the original text.
fn valid_utf8_prefix(bytes: Vec<u8>) -> String {
    let valid_up_to = match std::str::from_utf8(&bytes) {
        Ok(_) => bytes.len(),
        Err(e) => e.valid_up_to(),
    };
    // bytes[..valid_up_to] is valid UTF-8 → lossy is a zero-copy borrow.
    String::from_utf8_lossy(&bytes[..valid_up_to]).into_owned()
}

/// Turn a byte-suffix of some valid UTF-8 text into a valid UTF-8 string,
/// dropping leading bytes that belong to a partial multi-byte char. Because the
/// input is a contiguous suffix of valid UTF-8 text, the only possible
/// invalidity is a leading partial char (≤ 3 bytes), so this advances at most a
/// couple of times — O(N) overall.
fn valid_utf8_suffix(bytes: Vec<u8>) -> String {
    let mut start = 0;
    while start < bytes.len() {
        if std::str::from_utf8(&bytes[start..]).is_ok() {
            return String::from_utf8_lossy(&bytes[start..]).into_owned();
        }
        start += 1;
    }
    String::new()
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
        match o200k_encoder() {
            // Accurate BPE count.
            Some(encoder) => encoder.encode_with_special_tokens(text).len() as u32,
            // Encoder unavailable — degrade to the char-based heuristic instead
            // of panicking. Reuses the existing HeuristicTokenCounter.
            None => HeuristicTokenCounter::default().count_text(text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{FunctionCall, ToolCall};

    #[test]
    fn heuristic_counter_counts_text() {
        let counter = HeuristicTokenCounter::default();

        // "Hello, world!" = 13 chars -> 13/4 * 1.1 ≈ 3.57 -> 4 tokens
        let tokens = counter.count_text("Hello, world!");
        assert!(
            (3..=5).contains(&tokens),
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
            (3..=6).contains(&tokens),
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
    fn provider_visible_tool_estimate_is_zero_for_no_segments() {
        let counter = TiktokenTokenCounter::default();
        assert_eq!(
            counter.estimate_provider_visible_tool_segments(std::iter::empty()),
            ProviderVisibleToolEstimate::default()
        );
    }

    #[test]
    fn provider_visible_tool_estimate_counts_each_ordered_position_once() {
        let counter = TiktokenTokenCounter::default();
        let segments = [
            r#"{"type":"function","function":{"#.to_string(),
            r#""name":"lookup","description":"café 工具","#.to_string(),
            r#""parameters":{"type":"object"}}}"#.to_string(),
        ];
        let estimate =
            counter.estimate_provider_visible_tool_segments(segments.iter().map(String::as_str));
        let expected_tokens = segments.iter().fold(0u32, |total, segment| {
            total.saturating_add(counter.count_text(segment))
        });
        let expected_bytes = segments.iter().map(String::len).sum::<usize>();
        let expected_chars = segments
            .iter()
            .map(|segment| segment.chars().count())
            .sum::<usize>();

        assert_eq!(estimate.input_tokens, expected_tokens);
        assert_eq!(estimate.serialized_bytes, expected_bytes);
        assert_eq!(estimate.serialized_chars, expected_chars);
    }

    #[test]
    fn provider_visible_tool_estimate_keeps_bpe_boundaries_between_positions() {
        let counter = TiktokenTokenCounter::default();
        let segments = ["a".to_string(), "b".to_string()];

        let estimate =
            counter.estimate_provider_visible_tool_segments(segments.iter().map(String::as_str));
        let separate = counter
            .count_text(&segments[0])
            .saturating_add(counter.count_text(&segments[1]));

        assert_eq!(estimate.input_tokens, separate);
        assert_ne!(estimate.input_tokens, counter.count_text("ab"));
    }

    #[test]
    fn larger_provider_visible_tool_segment_increases_every_measure() {
        let counter = TiktokenTokenCounter::default();
        let small = [r#"{"parameters":{}}"#.to_string()];
        let large = [format!(
            r#"{{"parameters":{{"description":"{}"}}}}"#,
            "provider visible parameter ".repeat(128)
        )];

        let small =
            counter.estimate_provider_visible_tool_segments(small.iter().map(String::as_str));
        let large =
            counter.estimate_provider_visible_tool_segments(large.iter().map(String::as_str));

        assert!(large.input_tokens > small.input_tokens);
        assert!(large.serialized_bytes > small.serialized_bytes);
        assert!(large.serialized_chars > small.serialized_chars);
    }

    #[test]
    fn tiktoken_counter_counts_cjk() {
        let counter = TiktokenTokenCounter::default();
        // CJK text: each character is typically 1-2 tokens
        let tokens = counter.count_text("你好世界");
        assert!(
            (2..=8).contains(&tokens),
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

    #[test]
    fn bundled_o200k_encoder_loads_successfully() {
        // Regression guard: if the bundled o200k_base vocabulary ever fails to
        // load (a build/link regression in tiktoken-rs), TiktokenTokenCounter
        // would silently fall back to heuristic counting. Assert the bundled
        // encoder actually loads so such a regression is caught here.
        assert!(
            o200k_base().is_ok(),
            "bundled o200k_base tokenizer failed to load; \
             suspected tiktoken-rs build/link regression"
        );
    }

    // ── Encode-once truncation (issue #24: O(N²) → O(N)) ──

    #[test]
    fn truncate_prefix_keeps_start_and_stays_within_budget() {
        let counter = TiktokenTokenCounter::default();
        let text = "The quick brown fox jumps over the lazy dog. ".repeat(50);
        // Sanity: text is well over the budget.
        assert!(counter.count_text(&text) > 30);

        let max_tokens = 30u32;
        let prefix = counter.truncate_to_token_prefix(&text, max_tokens);

        // (c) keep the START: prefix must be an exact prefix of `text`.
        assert!(
            text.starts_with(&prefix),
            "prefix must be the START of text"
        );
        assert!(
            !prefix.is_empty(),
            "prefix should not be empty under budget"
        );
        // (a) never exceed max_tokens.
        let count = counter.count_text(&prefix);
        assert!(
            count <= max_tokens,
            "prefix token count {count} exceeds budget {max_tokens}"
        );
    }

    #[test]
    fn truncate_suffix_keeps_end_and_stays_within_budget() {
        let counter = TiktokenTokenCounter::default();
        let text = "The quick brown fox jumps over the lazy dog. ".repeat(50);
        assert!(counter.count_text(&text) > 30);

        let max_tokens = 30u32;
        let suffix = counter.truncate_to_token_suffix(&text, max_tokens);

        // (c) keep the END: suffix must be an exact suffix of `text`.
        assert!(text.ends_with(&suffix), "suffix must be the END of text");
        assert!(
            !suffix.is_empty(),
            "suffix should not be empty under budget"
        );
        // (a) never exceed max_tokens.
        let count = counter.count_text(&suffix);
        assert!(
            count <= max_tokens,
            "suffix token count {count} exceeds budget {max_tokens}"
        );
    }

    #[test]
    fn truncate_returns_text_unchanged_when_within_budget() {
        let counter = TiktokenTokenCounter::default();
        let text = "Hello, world!"; // a handful of tokens
        assert!(counter.count_text(text) <= 1000);

        assert_eq!(counter.truncate_to_token_prefix(text, 1000), text);
        assert_eq!(counter.truncate_to_token_suffix(text, 1000), text);
    }

    #[test]
    fn truncate_max_tokens_zero_returns_empty() {
        let counter = TiktokenTokenCounter::default();
        // (a) with budget 0 the only value that never exceeds it is empty.
        assert_eq!(counter.truncate_to_token_prefix("Hello, world!", 0), "");
        assert_eq!(counter.truncate_to_token_suffix("Hello, world!", 0), "");
    }

    #[test]
    fn truncate_prefix_suffix_large_input_is_valid_and_within_budget() {
        // Correctness + perf sanity on a ~100KB input mixing ASCII, CJK, digits
        // and newlines — exercises multi-byte token-boundary alignment.
        let counter = TiktokenTokenCounter::default();
        let unit = "The quick brown fox 你好世界 jumps 1234567890 over.\n";
        let text = unit.repeat(2_500);
        assert!(text.len() > 100_000, "precondition: large input");
        assert!(counter.count_text(&text) > 500);

        let max_tokens = 500u32;

        let prefix = counter.truncate_to_token_prefix(&text, max_tokens);
        assert!(
            text.starts_with(&prefix),
            "prefix must be the START of text"
        );
        let pcount = counter.count_text(&prefix);
        assert!(
            pcount <= max_tokens,
            "prefix token count {pcount} exceeds budget {max_tokens}"
        );

        let suffix = counter.truncate_to_token_suffix(&text, max_tokens);
        assert!(text.ends_with(&suffix), "suffix must be the END of text");
        let scount = counter.count_text(&suffix);
        assert!(
            scount <= max_tokens,
            "suffix token count {scount} exceeds budget {max_tokens}"
        );
    }
}
