//! Conversation summarization for rolling context management.
//!
//! When conversations are truncated due to token limits, a summary preserves
//! key information from earlier context.

use crate::agent::core::agent::types::{Message, Role};
use crate::agent::llm::provider::LLMProvider;
use crate::agent::llm::types::LLMChunk;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;

/// Trait for summarization implementations.
#[async_trait]
pub trait Summarizer: Send + Sync {
    /// Generate a summary of the given messages.
    ///
    /// Returns a string containing the summary.
    async fn summarize(
        &self,
        messages: &[Message],
    ) -> Result<String, crate::agent::core::budget::types::BudgetError>;

    /// Get the estimated token count of the summary.
    ///
    /// Used to ensure the summary fits within the budget.
    fn estimate_summary_tokens(&self, message_count: usize) -> u32 {
        // Rough estimate: each message contributes ~50 tokens to the summary
        (message_count * 50).min(1000) as u32
    }
}

/// Heuristic summarizer that extracts key points without using an LLM.
///
/// This is a lightweight summarization approach that:
/// 1. Lists user questions/requests
/// 2. Lists tools that were used
/// 3. Captures final conclusions
///
/// This provides continuity without expensive LLM calls.
#[derive(Debug, Default)]
pub struct HeuristicSummarizer;

impl HeuristicSummarizer {
    /// Create a new heuristic summarizer.
    pub fn new() -> Self {
        Self
    }

    /// Extract user questions from messages.
    fn extract_user_questions<'a>(&self, messages: &'a [Message]) -> Vec<&'a str> {
        messages
            .iter()
            .filter(|m| m.role == Role::User)
            .filter(|m| !m.content.is_empty())
            .take(10) // Limit to prevent huge summaries
            .map(|m| m.content.as_str())
            .collect()
    }

    /// Extract tool calls that were made.
    fn extract_tools_used(&self, messages: &[Message]) -> Vec<String> {
        let mut tools = HashSet::new();

        for message in messages {
            if let Some(ref tool_calls) = message.tool_calls {
                for call in tool_calls {
                    tools.insert(call.function.name.clone());
                }
            }
        }

        let mut result: Vec<String> = tools.into_iter().collect();
        result.sort();
        result
    }

    /// Extract key assistant responses.
    fn extract_key_responses<'a>(&self, messages: &'a [Message]) -> Vec<&'a str> {
        messages
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .filter(|m| !m.content.is_empty())
            .rev() // Take most recent first
            .take(3)
            .map(|m| m.content.as_str())
            .collect()
    }

    /// Safely truncate a string at a character boundary.
    /// Uses char_indices() to ensure we don't split UTF-8 multi-byte characters.
    fn safe_truncate(&self, s: &str, max_chars: usize) -> String {
        if s.chars().count() <= max_chars {
            return s.to_string();
        }

        // Take up to max_chars characters safely
        let truncated: String = s.chars().take(max_chars).collect();
        format!("{}...", truncated)
    }
}

#[async_trait]
impl Summarizer for HeuristicSummarizer {
    async fn summarize(
        &self,
        messages: &[Message],
    ) -> Result<String, crate::agent::core::budget::types::BudgetError> {
        if messages.is_empty() {
            return Ok("No conversation history.".to_string());
        }

        let questions = self.extract_user_questions(messages);
        let tools = self.extract_tools_used(messages);
        let responses = self.extract_key_responses(messages);

        let mut summary_parts = Vec::new();

        // User requests section
        if !questions.is_empty() {
            summary_parts.push("## User Requests".to_string());
            for (i, q) in questions.iter().enumerate() {
                // Truncate long questions for the summary (safe UTF-8)
                let truncated = self.safe_truncate(q, 200);
                summary_parts.push(format!("{}. {}", i + 1, truncated));
            }
        }

        // Tools used section
        if !tools.is_empty() {
            summary_parts.push("\n## Tools Used".to_string());
            for tool in tools {
                summary_parts.push(format!("- {}", tool));
            }
        }

        // Key responses section
        if !responses.is_empty() {
            summary_parts.push("\n## Key Outcomes".to_string());
            for (i, r) in responses.iter().enumerate() {
                // Truncate long responses (safe UTF-8)
                let truncated = self.safe_truncate(r, 300);
                summary_parts.push(format!("{}. {}", i + 1, truncated));
            }
        }

        if summary_parts.is_empty() {
            Ok("Previous conversation context available.".to_string())
        } else {
            Ok(summary_parts.join("\n"))
        }
    }
}

/// Trigger conditions for when to create a summary.
#[derive(Debug, Clone)]
pub enum SummaryTrigger {
    /// Always summarize when truncation occurs
    OnTruncation,
    /// Summarize after N rounds of conversation
    Periodic { interval: usize },
    /// Summarize when token count exceeds threshold
    TokenThreshold { threshold: u32 },
}

/// Manager for conversation summarization.
pub struct SummaryManager {
    summarizer: Box<dyn Summarizer>,
    trigger: SummaryTrigger,
}

impl std::fmt::Debug for SummaryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SummaryManager")
            .field("trigger", &self.trigger)
            .finish_non_exhaustive()
    }
}

impl SummaryManager {
    /// Create a new summary manager.
    pub fn new(summarizer: impl Summarizer + 'static, trigger: SummaryTrigger) -> Self {
        Self {
            summarizer: Box::new(summarizer),
            trigger,
        }
    }

    /// Check if summarization should be triggered based on conversation state.
    pub fn should_summarize(
        &self,
        messages: &[Message],
        _truncation_occurred: bool,
        current_token_count: u32,
    ) -> bool {
        match &self.trigger {
            SummaryTrigger::OnTruncation => _truncation_occurred,
            SummaryTrigger::Periodic { interval } => messages.len() >= *interval,
            SummaryTrigger::TokenThreshold { threshold } => current_token_count >= *threshold,
        }
    }

    /// Generate a summary of the messages.
    pub async fn summarize(
        &self,
        messages: &[Message],
    ) -> Result<String, crate::agent::core::budget::types::BudgetError> {
        self.summarizer.summarize(messages).await
    }

    /// Estimate the token count of a summary for N messages.
    pub fn estimate_summary_tokens(&self, message_count: usize) -> u32 {
        self.summarizer.estimate_summary_tokens(message_count)
    }
}

/// LLM-based summarizer that calls the current session's model to generate
/// a rich summary of compressed/removed messages.
///
/// Falls back to [`HeuristicSummarizer`] if the LLM call fails.
pub struct LlmSummarizer {
    llm: Arc<dyn LLMProvider>,
    model: String,
    /// Optional existing summary to build upon (incremental summarization).
    existing_summary: Option<String>,
}

impl LlmSummarizer {
    /// Create a new LLM-based summarizer.
    ///
    /// # Arguments
    /// * `llm` - The LLM provider to use (same as the current session's provider)
    /// * `model` - Model name to use for summarization
    /// * `existing_summary` - Optional previous summary to extend
    pub fn new(llm: Arc<dyn LLMProvider>, model: String, existing_summary: Option<String>) -> Self {
        Self {
            llm,
            model,
            existing_summary,
        }
    }

    /// Build the summarization prompt for the LLM.
    fn build_summarization_messages(&self, messages: &[Message]) -> Vec<Message> {
        let mut prompt_messages = Vec::new();

        let system_prompt = r#"You are a conversation summarizer. Your task is to create a concise but comprehensive summary of a conversation that was removed due to context window limits.

Guidelines:
- Preserve key decisions, facts, code changes, file paths, and important outcomes
- Maintain the user's original intent and any constraints they specified
- Note important tool results (files read, commands executed, errors encountered)
- Keep technical details that would be needed to continue the work
- Use a structured format with sections if the conversation covers multiple topics
- Be concise but don't lose critical information
- Write in the same language as the original conversation"#;

        prompt_messages.push(Message::system(system_prompt));

        // If there's an existing summary, include it for incremental updates
        let mut user_content = String::new();

        if let Some(ref existing) = self.existing_summary {
            user_content.push_str("## Previous Summary\n\n");
            user_content.push_str(existing);
            user_content.push_str("\n\n---\n\n");
            user_content.push_str(
                "The above is the previous summary. Below are additional messages that have now been compressed. \
                 Please produce an updated, merged summary that incorporates both the previous summary and the new messages.\n\n",
            );
        }

        user_content.push_str("## Messages to Summarize\n\n");

        for message in messages {
            let role_label = match message.role {
                Role::User => "User",
                Role::Assistant => "Assistant",
                Role::Tool => "Tool Result",
                Role::System => continue, // Skip system messages
            };

            // Include tool call info if present
            if let Some(ref tool_calls) = message.tool_calls {
                if !tool_calls.is_empty() {
                    let tool_names: Vec<&str> = tool_calls
                        .iter()
                        .map(|tc| tc.function.name.as_str())
                        .collect();
                    user_content.push_str(&format!(
                        "**{}** [called tools: {}]:\n",
                        role_label,
                        tool_names.join(", ")
                    ));
                } else {
                    user_content.push_str(&format!("**{}**:\n", role_label));
                }
            } else {
                user_content.push_str(&format!("**{}**:\n", role_label));
            }

            // Include tool_call_id for tool results
            if let Some(ref tool_call_id) = message.tool_call_id {
                user_content.push_str(&format!("(tool_call_id: {})\n", tool_call_id));
            }

            // Truncate very long messages to avoid blowing up the summary request
            let content = &message.content;
            const MAX_CONTENT_CHARS: usize = 2000;
            if content.chars().count() > MAX_CONTENT_CHARS {
                let truncated: String = content.chars().take(MAX_CONTENT_CHARS).collect();
                user_content.push_str(&truncated);
                user_content.push_str("... [truncated]\n\n");
            } else {
                user_content.push_str(content);
                user_content.push_str("\n\n");
            }
        }

        user_content.push_str(
            "\n---\n\nPlease summarize the above conversation. \
             Focus on preserving actionable information, decisions made, and current state of work.",
        );

        prompt_messages.push(Message::user(user_content));

        prompt_messages
    }

    /// Consume an LLM stream and collect the full text response.
    async fn collect_stream_response(
        &self,
        messages: &[Message],
    ) -> Result<String, crate::agent::core::budget::types::BudgetError> {
        let stream = self
            .llm
            .chat_stream(messages, &[], None, &self.model)
            .await
            .map_err(|e| {
                crate::agent::core::budget::types::BudgetError::TokenCountError(format!(
                    "LLM summarization call failed: {}",
                    e
                ))
            })?;

        let mut content = String::new();
        let mut stream = stream;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(LLMChunk::Token(text)) => content.push_str(&text),
                Ok(LLMChunk::Done) => break,
                Ok(_) => {} // Ignore reasoning tokens, tool calls, etc.
                Err(e) => {
                    tracing::warn!("LLM summarization stream error: {}", e);
                    if !content.is_empty() {
                        break;
                    }
                    return Err(
                        crate::agent::core::budget::types::BudgetError::TokenCountError(format!(
                            "LLM summarization stream failed: {}",
                            e
                        )),
                    );
                }
            }
        }

        Ok(content)
    }
}

impl std::fmt::Debug for LlmSummarizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmSummarizer")
            .field("model", &self.model)
            .field("has_existing_summary", &self.existing_summary.is_some())
            .finish()
    }
}

#[async_trait]
impl Summarizer for LlmSummarizer {
    async fn summarize(
        &self,
        messages: &[Message],
    ) -> Result<String, crate::agent::core::budget::types::BudgetError> {
        if messages.is_empty() {
            return Ok("No conversation history to summarize.".to_string());
        }

        let prompt_messages = self.build_summarization_messages(messages);

        tracing::info!(
            "LlmSummarizer: summarizing {} messages using model '{}' (existing_summary={})",
            messages.len(),
            self.model,
            self.existing_summary.is_some()
        );

        match self.collect_stream_response(&prompt_messages).await {
            Ok(summary) if !summary.trim().is_empty() => {
                tracing::info!("LlmSummarizer: generated summary ({} chars)", summary.len());
                Ok(summary)
            }
            Ok(_) => {
                tracing::warn!(
                    "LlmSummarizer: LLM returned empty summary, falling back to heuristic"
                );
                HeuristicSummarizer::new().summarize(messages).await
            }
            Err(e) => {
                tracing::warn!(
                    "LlmSummarizer: LLM call failed ({}), falling back to heuristic",
                    e
                );
                HeuristicSummarizer::new().summarize(messages).await
            }
        }
    }

    fn estimate_summary_tokens(&self, message_count: usize) -> u32 {
        // LLM summaries tend to be more detailed; estimate higher than heuristic
        (message_count * 80).min(2000) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heuristic_summarizer_extracts_user_questions() {
        let summarizer = HeuristicSummarizer::new();
        let messages = vec![
            Message::user("What is the weather?"),
            Message::assistant("It's sunny.", None),
            Message::user("What about tomorrow?"),
        ];

        let questions = summarizer.extract_user_questions(&messages);
        assert_eq!(questions.len(), 2);
        assert!(questions[0].contains("weather"));
    }

    #[test]
    fn heuristic_summarizer_extracts_tools_used() {
        use crate::agent::core::tools::{FunctionCall, ToolCall};

        let summarizer = HeuristicSummarizer::new();
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: "{}".to_string(),
            },
        };

        let messages = vec![
            Message::user("Search for something"),
            Message::assistant("I'll search", Some(vec![tool_call])),
        ];

        let tools = summarizer.extract_tools_used(&messages);
        assert_eq!(tools, vec!["search"]);
    }

    #[test]
    fn heuristic_summarizer_extracts_key_responses() {
        let summarizer = HeuristicSummarizer::new();
        let messages = vec![
            Message::user("Hello"),
            Message::assistant("First response", None),
            Message::user("How are you?"),
            Message::assistant("Most recent response", None),
        ];

        let responses = summarizer.extract_key_responses(&messages);
        // Should return most recent first
        assert_eq!(responses[0], "Most recent response");
    }

    #[tokio::test]
    async fn heuristic_summarizer_generates_summary() {
        let summarizer = HeuristicSummarizer::new();
        let messages = vec![
            Message::user("What is Rust?"),
            Message::assistant("Rust is a systems programming language.", None),
        ];

        let summary = summarizer.summarize(&messages).await.unwrap();
        assert!(summary.contains("User Requests"));
        assert!(summary.contains("What is Rust?"));
    }

    #[test]
    fn summary_trigger_on_truncation() {
        let trigger = SummaryTrigger::OnTruncation;

        assert!(matches!(trigger, SummaryTrigger::OnTruncation));
        // When truncation_occurred is true
        assert!(matches!(trigger, SummaryTrigger::OnTruncation));
        // When truncation_occurred is false - just verify the trigger type
    }

    #[test]
    fn summary_trigger_periodic() {
        let trigger = SummaryTrigger::Periodic { interval: 5 };
        let messages: Vec<Message> = (0..5).map(|_| Message::user("Test")).collect();

        // Verify the trigger is periodic with correct interval
        if let SummaryTrigger::Periodic { interval } = trigger {
            assert_eq!(interval, 5);
            assert!(messages.len() >= interval);
        } else {
            panic!("Expected Periodic trigger");
        }
    }

    #[test]
    fn summary_trigger_token_threshold() {
        let trigger = SummaryTrigger::TokenThreshold { threshold: 1000 };

        // Verify the trigger has the correct threshold
        if let SummaryTrigger::TokenThreshold { threshold } = trigger {
            assert_eq!(threshold, 1000);
        } else {
            panic!("Expected TokenThreshold trigger");
        }
    }

    #[test]
    fn safe_truncate_handles_ascii() {
        let summarizer = HeuristicSummarizer::new();
        let text = "Hello world this is a test";
        let truncated = summarizer.safe_truncate(text, 10);

        assert!(truncated.ends_with("..."));
        // Should have at most 10 characters + "..."
        assert!(truncated.chars().count() <= 13);
    }

    #[test]
    fn safe_truncate_handles_unicode() {
        let summarizer = HeuristicSummarizer::new();

        // Test with emoji (multi-byte UTF-8)
        let text = "Hello 😀🎉🚀 World with emoji";
        let truncated = summarizer.safe_truncate(text, 10);

        // Should not panic and should end with "..."
        assert!(truncated.ends_with("..."));
        assert!(truncated.chars().count() <= 13);
    }

    #[test]
    fn safe_truncate_handles_cjk() {
        let summarizer = HeuristicSummarizer::new();

        // Test with Chinese/Japanese/Korean characters (3-byte UTF-8)
        let text = "这是一个中文测试消息用于验证截断";
        let truncated = summarizer.safe_truncate(text, 10);

        // Should not panic
        assert!(truncated.ends_with("..."));
        assert!(truncated.chars().count() <= 13);
    }

    #[test]
    fn safe_truncate_handles_mixed_unicode() {
        let summarizer = HeuristicSummarizer::new();

        // Mixed ASCII, CJK, and emoji
        let text = "Hello 世界 🌍 test message";
        let truncated = summarizer.safe_truncate(text, 8);

        // Should not panic
        assert!(truncated.ends_with("..."));
        assert!(truncated.chars().count() <= 11);
    }

    #[tokio::test]
    async fn summarizer_handles_unicode_messages() {
        let summarizer = HeuristicSummarizer::new();

        // Create messages with unicode that needs truncation
        let long_unicode =
            "这是一段很长的中文消息需要被截断以测试我们的安全截断功能 😀🎉🚀".repeat(10);
        let messages = vec![
            Message::user(&long_unicode),
            Message::assistant("Response", None),
        ];

        // Should not panic on unicode truncation
        let summary = summarizer.summarize(&messages).await.unwrap();
        assert!(summary.contains("User Requests"));
    }

    #[test]
    fn safe_truncate_returns_short_text_unchanged() {
        let summarizer = HeuristicSummarizer::new();
        let text = "Short";
        let truncated = summarizer.safe_truncate(text, 100);

        // Should return unchanged
        assert_eq!(truncated, text);
    }
}
