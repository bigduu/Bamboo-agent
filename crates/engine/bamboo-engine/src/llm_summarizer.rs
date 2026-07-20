//! LLM-backed conversation summarizer.
//!
//! `LlmSummarizer` is the infrastructure-coupled implementation of
//! `bamboo_compression::Summarizer`: it calls the session model to produce a
//! rich summary of compressed/removed messages, falling back to the pure
//! `HeuristicSummarizer` on failure. It lives in the engine (not in
//! bamboo-compression) so that the compression crate stays free of any
//! LLM-provider dependency.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use bamboo_compression::{HeuristicSummarizer, Summarizer};
use bamboo_domain::ReasoningEffort;
use bamboo_domain::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, Message, Role,
};
use bamboo_llm::LLMChunk;
use bamboo_llm::{LLMProvider, LLMRequestOptions};

/// Mode controlling how the LLM summarizer handles existing summaries.
#[derive(Debug, Clone, Default)]
pub enum SummaryMode {
    /// Generate a complete summary from scratch (default).
    #[default]
    FullRewrite,
    /// Update an existing summary by incorporating new information incrementally.
    IncrementalMerge,
}

/// LLM-based summarizer that calls the current session's model to generate
/// a rich summary of compressed/removed messages.
///
/// Falls back to [`HeuristicSummarizer`] if the LLM call fails, UNLESS
/// [`with_heuristic_fallback_on_error(false)`](Self::with_heuristic_fallback_on_error)
/// is set — in which case a transient LLM *error* surfaces to the caller so a
/// best-effort caller (mid-turn context compression) can skip and degrade
/// gracefully instead of silently substituting a low-quality heuristic summary.
pub struct LlmSummarizer {
    llm: Arc<dyn LLMProvider>,
    model: String,
    /// Optional existing summary to build upon (incremental summarization).
    existing_summary: Option<String>,
    /// Structured runtime context blocks that should inform summarization.
    context_blocks: Vec<ContextBlock>,
    /// Optional user-provided instructions that override/extend the default summary focus.
    custom_instructions: Option<String>,
    /// Controls how the summarizer handles existing summaries.
    summary_mode: SummaryMode,
    /// When true (default), a transient LLM call/stream failure is recovered by
    /// falling back to the [`HeuristicSummarizer`]. When false, that error is
    /// returned to the caller instead — used by best-effort callers that would
    /// rather SKIP compression than emit a degraded heuristic summary. The
    /// empty-response fallback is unaffected either way. (issue #238)
    heuristic_fallback_on_error: bool,
}

impl LlmSummarizer {
    pub fn new(
        llm: Arc<dyn LLMProvider>,
        model: String,
        existing_summary: Option<String>,
        task_list_prompt: Option<String>,
    ) -> Self {
        let context_blocks = task_list_prompt
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|task_list| {
                vec![ContextBlock::new(
                    ContextBlockType::TaskSnapshot,
                    ContextBlockPriority::High,
                    ContextBlockStability::RoundDynamic,
                    "Current Task List",
                    task_list,
                )]
            })
            .unwrap_or_default();

        Self {
            llm,
            model,
            existing_summary,
            context_blocks,
            custom_instructions: None,
            summary_mode: SummaryMode::default(),
            heuristic_fallback_on_error: true,
        }
    }

    /// Control whether a transient LLM *error* recovers via the heuristic
    /// summarizer (default `true`) or surfaces to the caller (`false`). Set
    /// `false` for best-effort compression (mid-turn) that should skip rather
    /// than substitute a heuristic summary. (issue #238)
    pub fn with_heuristic_fallback_on_error(mut self, enabled: bool) -> Self {
        self.heuristic_fallback_on_error = enabled;
        self
    }

    pub fn with_context_blocks(mut self, context_blocks: Vec<ContextBlock>) -> Self {
        self.context_blocks = context_blocks;
        self
    }

    pub fn with_custom_instructions(mut self, instructions: Option<String>) -> Self {
        self.custom_instructions = instructions;
        self
    }

    pub fn with_summary_mode(mut self, mode: SummaryMode) -> Self {
        self.summary_mode = mode;
        self
    }

    /// Build the summarization prompt for the LLM.
    fn build_summarization_messages(&self, messages: &[Message]) -> Vec<Message> {
        let mut prompt_messages = Vec::new();

        let system_prompt = match self.summary_mode {
            SummaryMode::FullRewrite => {
                r#"You are a conversation summarizer. Your task is to create a concise but reliable working-memory summary for a conversation that was removed due to context window limits.

Guidelines:
- First capture the in-flight work right before compression (what was being done, where, and with which tool/file)
- Distinguish clearly between CURRENT ACTIVE work, COMPLETED work, and OBSOLETE or superseded work
- Do not restate old tasks as active unless they are still unresolved
- The provided current task list is the source of truth for active work
- Preserve key decisions, constraints, file paths, code changes, tool findings, blockers, and important outcomes
- Preserve error messages, test results (pass/fail counts), and function/variable names that are relevant to active work
- If earlier plans conflict with newer messages or the current task list, mark them as obsolete or completed
- Explicitly evaluate each clear user requirement (e.g. requirement 1, requirement 2) with a status and evidence
- Keep the next step specific and aligned with the active work only
- Use structured sections
- Write in the same language as the original conversation"#
            }
            SummaryMode::IncrementalMerge => {
                r#"You are updating an existing conversation summary with new information from recent messages.

Guidelines:
- Incorporate new information into the existing summary structure
- Mark previously active work as completed if the new messages confirm completion
- Remove or condense information that is no longer relevant
- Preserve all key decisions, file paths, and constraints that remain active
- If new messages conflict with the existing summary, the new messages take precedence
- Keep the summary focused on what is currently active and relevant
- The provided current task list is the source of truth for active work
- Maintain the same structured sections as the existing summary
- Write in the same language as the original conversation
- Be concise: avoid repeating information already well-captured in the existing summary"#
            }
        };

        prompt_messages.push(Message::system(system_prompt));

        let mut user_content = String::new();

        if let Some(ref existing) = self.existing_summary {
            user_content.push_str("## Previous Summary\n\n");
            user_content.push_str(existing);
            user_content.push_str("\n\n---\n\n");
        }

        if !self.context_blocks.is_empty() {
            user_content.push_str("## Compression Context Blocks\n\n");
            for block in &self.context_blocks {
                user_content.push_str(&format!(
                    "### {}\n- type: {}\n- priority: {}\n- stability: {}\n\n{}\n\n",
                    block.title.trim(),
                    block.block_type.as_str(),
                    block.priority.as_str(),
                    block.stability.as_str(),
                    block.content.trim(),
                ));
            }
            user_content.push_str("---\n\n");
        }

        if let Some(ref instructions) = self.custom_instructions {
            if !instructions.trim().is_empty() {
                user_content.push_str("## Custom Compression Instructions\n\n");
                user_content.push_str(instructions.trim());
                user_content.push_str("\n\n---\n\n");
            }
        }

        user_content.push_str(
            "## Required Output Sections\n1. Pre-compression in-flight work (what was being done immediately before compression)\n2. Current active objective\n3. Requirement checklist (Requirement | Status: completed/in_progress/pending/blocked/obsolete | Evidence)\n4. Active tasks\n5. Completed tasks\n6. Obsolete or superseded tasks\n7. Important context and constraints\n8. Files, code, and tool findings\n9. Open issues and next step\n\n",
        );

        user_content.push_str("## Messages to Summarize\n\n");

        for message in messages {
            let role_label = match message.role {
                Role::User => "User",
                Role::Assistant => "Assistant",
                Role::Tool => "Tool Result",
                Role::System => continue,
            };

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

            if let Some(ref tool_call_id) = message.tool_call_id {
                user_content.push_str(&format!("(tool_call_id: {})\n", tool_call_id));
            }

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
            "\n---\n\nReturn only the summary text. Be explicit about what is active now versus what is already completed or no longer relevant.",
        );

        prompt_messages.push(Message::user(user_content));

        prompt_messages
    }

    /// Consume an LLM stream and collect the full text response.
    async fn collect_stream_response(
        &self,
        messages: &[Message],
    ) -> Result<String, bamboo_compression::types::BudgetError> {
        // Summarization is a lightweight auxiliary request; cap reasoning effort at `high`
        // to stay compatible with fast models (e.g. gpt-5-mini).
        let options = LLMRequestOptions {
            session_id: None,
            reasoning_effort: Some(ReasoningEffort::High),
            parallel_tool_calls: None,
            required_tool: None,
            responses: None,
            request_purpose: Some("compression".to_string()),
            cache: None,
        };
        let stream = self
            .llm
            .chat_stream_with_options(messages, &[], Some(8192), &self.model, Some(&options))
            .await
            .map_err(|e| {
                bamboo_compression::types::BudgetError::TokenCountError(format!(
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
                    return Err(bamboo_compression::types::BudgetError::TokenCountError(
                        format!("LLM summarization stream failed: {}", e),
                    ));
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
            .field("context_block_count", &self.context_blocks.len())
            .finish()
    }
}

#[async_trait]
impl Summarizer for LlmSummarizer {
    async fn summarize(
        &self,
        messages: &[Message],
    ) -> Result<String, bamboo_compression::types::BudgetError> {
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
            Err(e) if self.heuristic_fallback_on_error => {
                tracing::warn!(
                    "LlmSummarizer: LLM call failed ({}), falling back to heuristic",
                    e
                );
                HeuristicSummarizer::new().summarize(messages).await
            }
            // Best-effort caller opted out of the heuristic fallback: surface the
            // transient error so the caller can skip compression entirely rather
            // than substitute a low-quality heuristic summary. (issue #238)
            Err(e) => {
                tracing::warn!(
                    "LlmSummarizer: LLM call failed ({}); surfacing error (heuristic fallback disabled)",
                    e
                );
                Err(e)
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
    use bamboo_domain::ReasoningEffort;
    use bamboo_llm::{LLMChunk, LLMError, LLMRequestOptions, LLMStream};
    use futures::stream;
    use std::sync::Mutex;

    struct DummyProvider;

    #[async_trait]
    impl LLMProvider for DummyProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_domain::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token("dummy summary".to_string())),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ])))
        }
    }
    fn llm_summarizer_prompt_includes_context_blocks_and_state_sections() {
        let summarizer = LlmSummarizer::new(
            Arc::new(DummyProvider),
            "gpt-4o-mini".to_string(),
            Some("Earlier summary".to_string()),
            Some(
                "## Current Task List\n[/] task_1: Fix compression bounce\n[x] task_0: Analyze bug"
                    .to_string(),
            ),
        )
        .with_context_blocks(vec![
            ContextBlock::new(
                ContextBlockType::TaskSnapshot,
                ContextBlockPriority::High,
                ContextBlockStability::RoundDynamic,
                "Current Task List",
                "[/] task_1: Fix compression bounce",
            ),
            ContextBlock::new(
                ContextBlockType::ExternalMemory,
                ContextBlockPriority::Medium,
                ContextBlockStability::RoundDynamic,
                "External Memory (Persistent)",
                "Session note body",
            ),
        ]);
        let messages = vec![
            Message::user("继续做压缩修复"),
            Message::assistant("我先检查 trigger 与 target", None),
        ];

        let prompt_messages = summarizer.build_summarization_messages(&messages);
        assert_eq!(prompt_messages.len(), 2);
        assert_eq!(prompt_messages[0].role, Role::System);
        assert!(prompt_messages[1]
            .content
            .contains("## Compression Context Blocks"));
        assert!(prompt_messages[1].content.contains("Current Task List"));
        assert!(prompt_messages[1]
            .content
            .contains("External Memory (Persistent)"));
        assert!(prompt_messages[1]
            .content
            .contains("Current active objective"));
        assert!(prompt_messages[1].content.contains("Requirement checklist"));
        assert!(prompt_messages[1].content.contains("Active tasks"));
        assert!(prompt_messages[1].content.contains("Completed tasks"));
        assert!(prompt_messages[1]
            .content
            .contains("Obsolete or superseded tasks"));
        assert!(prompt_messages[1].content.contains("Earlier summary"));
    }

    #[derive(Default)]
    struct ReasoningCaptureProvider {
        captured_reasoning: Mutex<Vec<Option<ReasoningEffort>>>,
    }

    #[async_trait]
    impl LLMProvider for ReasoningCaptureProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_domain::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token("captured summary".to_string())),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ])))
        }

        async fn chat_stream_with_options(
            &self,
            messages: &[Message],
            tools: &[bamboo_domain::ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            self.captured_reasoning
                .lock()
                .expect("captured reasoning lock should not be poisoned")
                .push(options.and_then(|o| o.reasoning_effort));
            self.chat_stream(messages, tools, max_output_tokens, model)
                .await
        }
    }

    #[tokio::test]
    async fn llm_summarizer_requests_high_reasoning_effort_for_summary_calls() {
        let provider = Arc::new(ReasoningCaptureProvider::default());
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "gpt-5-mini".to_string(),
            None,
            Some("task list".to_string()),
        );
        let messages = vec![
            Message::user("请总结最近三轮"),
            Message::assistant("已完成第一步并准备第二步", None),
        ];

        let summary = summarizer
            .summarize(&messages)
            .await
            .expect("summary generation should succeed");
        assert_eq!(summary, "captured summary");

        let captured = provider
            .captured_reasoning
            .lock()
            .expect("captured reasoning lock should not be poisoned");
        assert_eq!(captured.as_slice(), [Some(ReasoningEffort::High)]);
    }

    /// Provider that captures both `reasoning_effort` and `max_output_tokens`.
    #[derive(Default)]
    struct RequestOptionsCaptureProvider {
        captured_reasoning: Mutex<Vec<Option<ReasoningEffort>>>,
        captured_max_tokens: Mutex<Vec<Option<u32>>>,
    }

    #[async_trait]
    impl LLMProvider for RequestOptionsCaptureProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_domain::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token("captured summary".to_string())),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ])))
        }

        async fn chat_stream_with_options(
            &self,
            messages: &[Message],
            tools: &[bamboo_domain::ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            self.captured_reasoning
                .lock()
                .expect("lock should not be poisoned")
                .push(options.and_then(|o| o.reasoning_effort));
            self.captured_max_tokens
                .lock()
                .expect("lock should not be poisoned")
                .push(max_output_tokens);
            self.chat_stream(messages, tools, max_output_tokens, model)
                .await
        }
    }

    #[tokio::test]
    async fn llm_summarizer_sufficient_max_tokens_for_high_reasoning() {
        let provider = Arc::new(RequestOptionsCaptureProvider::default());
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "gpt-5-mini".to_string(),
            None,
            Some("task list".to_string()),
        );
        let messages = vec![
            Message::user("请总结最近三轮"),
            Message::assistant("已完成第一步并准备第二步", None),
        ];

        let summary = summarizer
            .summarize(&messages)
            .await
            .expect("summary generation should succeed");
        assert_eq!(summary, "captured summary");

        let captured_reasoning = provider
            .captured_reasoning
            .lock()
            .expect("lock should not be poisoned");
        let captured_max_tokens = provider
            .captured_max_tokens
            .lock()
            .expect("lock should not be poisoned");
        assert_eq!(captured_reasoning.as_slice(), [Some(ReasoningEffort::High)]);
        let max_tokens = captured_max_tokens[0].expect("max_output_tokens should be set");
        // ReasoningEffort::High targets 4096 thinking budget; max_tokens must leave room for output.
        assert!(
            max_tokens > 4096,
            "max_output_tokens ({}) must exceed thinking budget (4096) to avoid truncation",
            max_tokens
        );
    }

    #[test]
    fn full_rewrite_mode_uses_default_system_prompt() {
        let summarizer =
            LlmSummarizer::new(Arc::new(DummyProvider), "model".to_string(), None, None)
                .with_summary_mode(SummaryMode::FullRewrite);
        let messages = vec![Message::user("hello"), Message::assistant("hi", None)];
        let prompts = summarizer.build_summarization_messages(&messages);
        let system = &prompts[0].content;
        assert!(
            system.contains("conversation summarizer"),
            "FullRewrite prompt should contain 'conversation summarizer'"
        );
        assert!(
            !system.contains("updating an existing"),
            "FullRewrite prompt should not contain incremental language"
        );
    }

    #[test]
    fn incremental_merge_mode_uses_update_system_prompt() {
        let summarizer = LlmSummarizer::new(
            Arc::new(DummyProvider),
            "model".to_string(),
            Some("Previous summary content".to_string()),
            None,
        )
        .with_summary_mode(SummaryMode::IncrementalMerge);
        let messages = vec![Message::user("hello"), Message::assistant("hi", None)];
        let prompts = summarizer.build_summarization_messages(&messages);
        let system = &prompts[0].content;
        assert!(
            system.contains("updating an existing conversation summary"),
            "IncrementalMerge prompt should contain 'updating an existing conversation summary'"
        );
        assert!(
            system.contains("Incorporate new information"),
            "IncrementalMerge prompt should mention incorporating new information"
        );
    }

    #[test]
    fn default_summary_mode_is_full_rewrite() {
        assert!(matches!(SummaryMode::default(), SummaryMode::FullRewrite));
    }

    #[test]
    fn incremental_merge_includes_existing_summary_in_user_content() {
        let summarizer = LlmSummarizer::new(
            Arc::new(DummyProvider),
            "model".to_string(),
            Some("Previous summary content".to_string()),
            None,
        )
        .with_summary_mode(SummaryMode::IncrementalMerge);
        let messages = vec![
            Message::user("new work"),
            Message::assistant("doing it", None),
        ];
        let prompts = summarizer.build_summarization_messages(&messages);
        let user_content = &prompts[1].content;
        assert!(
            user_content.contains("Previous Summary"),
            "IncrementalMerge user prompt should include the existing summary"
        );
        assert!(
            user_content.contains("Previous summary content"),
            "IncrementalMerge user prompt should include the actual summary text"
        );
    }

    /// Provider whose summarization stream call fails transiently (500/429/timeout).
    struct FailingProvider;

    #[async_trait]
    impl LLMProvider for FailingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_domain::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Err(LLMError::Api("http 500 transient".to_string()))
        }
    }

    fn summary_messages() -> Vec<Message> {
        vec![
            Message::user("do the work"),
            Message::assistant("working on it", None),
            Message::user("keep going"),
        ]
    }

    #[tokio::test]
    async fn summarize_falls_back_to_heuristic_on_llm_error_by_default() {
        // Default: a transient LLM failure is recovered by the heuristic
        // summarizer, so summarize() succeeds. This is what makes pre-turn /
        // overflow compression resilient (and, historically, masked #238).
        let summarizer =
            LlmSummarizer::new(Arc::new(FailingProvider), "model".to_string(), None, None);
        let out = summarizer.summarize(&summary_messages()).await;
        assert!(
            out.is_ok(),
            "default heuristic fallback should recover from a transient LLM error, got {out:?}"
        );
    }

    #[tokio::test]
    async fn summarize_surfaces_llm_error_when_heuristic_fallback_disabled() {
        // Best-effort caller (mid-turn compression) opts out of the heuristic
        // fallback: the transient error must SURFACE so the caller can skip
        // compression rather than substitute a low-quality heuristic summary.
        // (issue #238)
        let summarizer =
            LlmSummarizer::new(Arc::new(FailingProvider), "model".to_string(), None, None)
                .with_heuristic_fallback_on_error(false);
        let out = summarizer.summarize(&summary_messages()).await;
        assert!(
            out.is_err(),
            "with the heuristic fallback disabled, a transient LLM error must surface"
        );
    }
}
