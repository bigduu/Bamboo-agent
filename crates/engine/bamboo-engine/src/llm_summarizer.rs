//! LLM-backed conversation summarizer.
//!
//! `LlmSummarizer` is the infrastructure-coupled implementation of
//! `bamboo_compression::Summarizer`: it calls the session model to produce a
//! rich summary of compressed/removed messages. Callers without an explicit
//! model budget use a conservative compatibility budget, so every LLM-backed
//! path still runs through bounded map/reduce. Compatibility callers may fall
//! back to the pure `HeuristicSummarizer`; explicitly budgeted production passes
//! surface every failed stage so callers can preserve session state atomically.
//! It lives in the engine (not in bamboo-compression) so that the compression
//! crate stays free of any LLM-provider dependency.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use bamboo_compression::{
    HeuristicSummarizer, MessageSegmenter, Summarizer, TiktokenTokenCounter, TokenBudget,
    TokenCounter,
};
use bamboo_domain::ReasoningEffort;
use bamboo_domain::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, Message, Role,
};
use bamboo_llm::LLMChunk;
use bamboo_llm::{LLMProvider, LLMRequestOptions};

const COMPATIBILITY_CONTEXT_WINDOW_TOKENS: u32 = 32_000;
const COMPATIBILITY_MAX_OUTPUT_TOKENS: u32 = 8_000;
const COMPATIBILITY_SAFETY_MARGIN_TOKENS: u32 = 1_000;
const COMPATIBILITY_SAFE_WINDOW_PERCENT: u8 = 80;

/// Mode controlling how the LLM summarizer handles existing summaries.
#[derive(Debug, Clone, Default)]
pub enum SummaryMode {
    /// Generate a complete summary from scratch (default).
    #[default]
    FullRewrite,
    /// Update an existing summary by incorporating new information incrementally.
    IncrementalMerge,
}

/// Hard request limits for a logical compression pass.
#[derive(Debug, Clone)]
pub struct SummaryRequestBudget {
    pub context_window_tokens: u32,
    pub max_output_tokens: u32,
    pub safety_margin_tokens: u32,
    pub safe_window_percent: u8,
    pub target_summary_tokens: u32,
    pub target_ratio: f64,
}

impl SummaryRequestBudget {
    pub fn from_token_budget(
        budget: &TokenBudget,
        safe_window_percent: u8,
        target_summary_tokens: u32,
        target_ratio: f64,
    ) -> Self {
        Self {
            context_window_tokens: budget.max_context_tokens.max(1),
            max_output_tokens: budget.max_output_tokens.max(1),
            safety_margin_tokens: budget.safety_margin,
            safe_window_percent: safe_window_percent.clamp(10, 95),
            target_summary_tokens: target_summary_tokens.max(1),
            target_ratio: if target_ratio.is_finite() && target_ratio > 0.0 {
                target_ratio.clamp(0.01, 0.50)
            } else {
                0.20
            },
        }
    }

    fn safe_request_tokens(&self) -> u32 {
        self.context_window_tokens
            .saturating_mul(self.safe_window_percent as u32)
            .saturating_div(100)
            .max(1)
    }
}

#[derive(Debug, Clone)]
pub struct SummarizationReport {
    pub content: String,
    pub represented_source_tokens: u32,
    pub target_summary_tokens: u32,
    pub actual_summary_tokens: u32,
    pub map_calls: u32,
    pub reduce_calls: u32,
    pub fallback_used: bool,
    pub budget_clamped: bool,
    pub budget_clamp_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SummarizationProgress {
    pub stage: String,
    pub stage_index: usize,
    pub stage_count: usize,
    pub estimated_input_tokens: u32,
    pub requested_output_tokens: u32,
    pub safe_request_tokens: u32,
    pub model_context_tokens: u32,
}

type SummarizationProgressCallback = dyn Fn(&SummarizationProgress) + Send + Sync;

#[derive(Debug, Clone)]
struct SourceUnit {
    text: String,
    represented_source_tokens: u32,
    first_message_id: String,
    last_message_id: String,
    continuation_part: usize,
}

#[derive(Debug, Clone)]
struct SummaryPart {
    content: String,
    represented_source_tokens: u32,
    first_message_id: String,
    last_message_id: String,
}

/// LLM-based summarizer that calls the current session's model to generate
/// a rich summary of compressed/removed messages.
///
/// Calls without an explicit request budget fall back to
/// [`HeuristicSummarizer`] if the bounded LLM pipeline fails, UNLESS
/// [`with_heuristic_fallback_on_error(false)`](Self::with_heuristic_fallback_on_error)
/// is set. Budgeted calls always surface failed map/reduce stages so the archive
/// transaction cannot commit a heuristic summary that represents a failed pass.
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
    /// When true (default), a transient compatibility-path LLM failure is
    /// recovered by falling back to the [`HeuristicSummarizer`]. Explicitly
    /// budgeted calls always return stage failures so a multi-request pass
    /// remains atomic. The empty-response fallback is unaffected either way.
    /// (issues #238, #763)
    heuristic_fallback_on_error: bool,
    /// Exact selected summarization-model limits and source-derived output goal.
    request_budget: Option<SummaryRequestBudget>,
    /// Correlates every map/reduce request with the eventual persisted
    /// compression event (or with a failed logical pass).
    logical_pass_id: Option<String>,
    logical_phase: Option<String>,
    progress_callback: Option<Arc<SummarizationProgressCallback>>,
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
            request_budget: None,
            logical_pass_id: None,
            logical_phase: None,
            progress_callback: None,
        }
    }

    /// Control whether a transient compatibility-path LLM *error* recovers via
    /// the heuristic summarizer (default `true`) or surfaces to the caller
    /// (`false`). Explicitly budgeted map/reduce failures always surface.
    /// (issues #238, #763)
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

    pub fn with_request_budget(mut self, budget: SummaryRequestBudget) -> Self {
        self.request_budget = Some(budget);
        self
    }

    pub fn with_logical_pass_context(
        mut self,
        logical_pass_id: impl Into<String>,
        logical_phase: impl Into<String>,
    ) -> Self {
        self.logical_pass_id = Some(logical_pass_id.into());
        self.logical_phase = Some(logical_phase.into());
        self
    }

    pub fn with_progress_callback(mut self, callback: Arc<SummarizationProgressCallback>) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    fn append_shared_context(&self, user_content: &mut String) {
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
    }

    fn render_shared_context(&self) -> String {
        let mut content = String::new();
        self.append_shared_context(&mut content);
        content
    }

    fn render_message_block(message: &Message) -> Option<String> {
        let role_label = match message.role {
            Role::User => "User",
            Role::Assistant => "Assistant",
            Role::Tool => "Tool Result",
            Role::System => return None,
        };
        let mut block = String::new();
        if let Some(ref tool_calls) = message.tool_calls {
            if !tool_calls.is_empty() {
                let tool_names = tool_calls
                    .iter()
                    .map(|call| call.function.name.as_str())
                    .collect::<Vec<_>>();
                block.push_str(&format!(
                    "**{}** [message_id: {}; called tools: {}]:\n",
                    role_label,
                    message.id,
                    tool_names.join(", ")
                ));
            } else {
                block.push_str(&format!(
                    "**{}** [message_id: {}]:\n",
                    role_label, message.id
                ));
            }
        } else {
            block.push_str(&format!(
                "**{}** [message_id: {}]:\n",
                role_label, message.id
            ));
        }
        if let Some(ref tool_call_id) = message.tool_call_id {
            block.push_str(&format!("(tool_call_id: {})\n", tool_call_id));
        }
        block.push_str(&message.content);
        block.push_str("\n\n");
        Some(block)
    }

    fn render_messages(messages: &[Message]) -> String {
        messages
            .iter()
            .filter_map(Self::render_message_block)
            .collect::<String>()
    }

    fn build_map_messages(&self, units: &[SourceUnit], target_tokens: u32) -> Vec<Message> {
        let represented_source_tokens = units.iter().fold(0u32, |total, unit| {
            total.saturating_add(unit.represented_source_tokens)
        });
        let mut user_content = format!(
            "Summarize this chronological source slice as loss-aware working memory.\n\
             It represents approximately {represented_source_tokens} raw source tokens.\n\
             Target approximately {target_tokens} output tokens. Preserve concrete requirements, \
             decisions, paths, commands, errors, test results, tool outcomes, active work, and next \
             steps. Do not turn it into a tiny high-level synopsis.\n\n## Source Slice\n\n"
        );
        for unit in units {
            user_content.push_str(&format!(
                "### Source range {}..{} (continuation part {})\n\n",
                unit.first_message_id, unit.last_message_id, unit.continuation_part
            ));
            user_content.push_str(&unit.text);
            user_content.push('\n');
        }
        user_content.push_str(
            "\nReturn only the chronological partial summary. Do not claim this is the final conversation summary.",
        );
        vec![
            Message::system(
                "You are the map stage of a bounded conversation-compression pipeline. \
                 Preserve detailed facts and ordering for a later reducer.",
            ),
            Message::user(user_content),
        ]
    }

    fn build_reduce_messages(
        &self,
        parts: &[SummaryPart],
        target_tokens: u32,
        include_shared_context: bool,
    ) -> Vec<Message> {
        let represented_source_tokens = parts.iter().fold(0u32, |total, part| {
            total.saturating_add(part.represented_source_tokens)
        });
        let mut user_content = String::new();
        if include_shared_context {
            self.append_shared_context(&mut user_content);
        }
        user_content.push_str(&format!(
            "## Summary Size Budget\nTarget approximately {target_tokens} tokens. The partials \
             below represent approximately {represented_source_tokens} raw source tokens. Derive \
             detail from represented source size, not from the already-compressed partial length; \
             do not apply the target ratio again.\n\n## Ordered Partial Summaries\n\n"
        ));
        for (index, part) in parts.iter().enumerate() {
            user_content.push_str(&format!(
                "### Partial {} — source {}..{} ({} represented raw tokens)\n\n{}\n\n",
                index + 1,
                part.first_message_id,
                part.last_message_id,
                part.represented_source_tokens,
                part.content.trim(),
            ));
        }
        if include_shared_context {
            user_content.push_str(
                "## Required Final Sections\n1. Pre-compression in-flight work\n2. Current active objective\n3. Requirement checklist with status and evidence\n4. Active tasks\n5. Completed tasks\n6. Obsolete or superseded tasks\n7. Important context and constraints\n8. Files, code, and tool findings\n9. Open issues and next step\n\n",
            );
        }
        user_content.push_str(
            "Return only the merged summary. Preserve source chronology and remove only genuine duplication.",
        );
        let system_prompt = if include_shared_context {
            match self.summary_mode {
                SummaryMode::FullRewrite => {
                    "You are the final reduce stage of a bounded conversation-compression \
                     pipeline. Merge all ordered partials and the supplied prior/runtime context \
                     into one reliable working-memory summary."
                }
                SummaryMode::IncrementalMerge => {
                    "You are the incremental final reduce stage of a bounded \
                     conversation-compression pipeline. Update the supplied prior summary with the \
                     ordered new partials and current runtime context. Newer facts supersede stale \
                     prior state."
                }
            }
        } else {
            "You are an intermediate reduce stage of a bounded conversation-compression pipeline. \
             Merge ordered partials without compounding the source-to-summary ratio."
        };
        vec![Message::system(system_prompt), Message::user(user_content)]
    }

    fn build_multipart_finalize_messages(
        &self,
        parts: &[SummaryPart],
        target_tokens: u32,
        shared_context_capsule: &str,
        retain_shared_context_capsule: bool,
    ) -> Vec<Message> {
        let represented_source_tokens = parts.iter().fold(0u32, |total, part| {
            total.saturating_add(part.represented_source_tokens)
        });
        let has_shared_context = !shared_context_capsule.trim().is_empty();
        let mut user_content = String::new();
        if has_shared_context {
            let retention_instruction = if retain_shared_context_capsule {
                "The shared-context capsule is retained once before the multipart updates, so do \
                 not repeat unchanged capsule text; emit corrections, superseding facts, and \
                 current runtime state when relevant."
            } else {
                "The shared-context capsule is reference material and is not stored separately. \
                 Incorporate its durable prior state and current runtime facts wherever needed, \
                 while applying its custom and hook-injected instructions as binding directives."
            };
            user_content.push_str(&format!(
                "## Shared Context Capsule\n\n{shared_context_capsule}\n\n\
                 Apply all custom and hook-injected instructions carried by the capsule. \
                 {retention_instruction}\n\n"
            ));
        }
        user_content.push_str(&format!(
            "## Multipart Section Budget\n\n\
             Target approximately {target_tokens} tokens for the source ranges in this section. \
             They represent approximately {represented_source_tokens} raw source tokens. Preserve \
             that source-derived allocation instead of applying the compression ratio again.\n\n\
             ## Ordered Partial Summaries\n\n"
        ));
        for (index, part) in parts.iter().enumerate() {
            user_content.push_str(&format!(
                "### Partial {} — source {}..{} ({} represented raw tokens)\n\n{}\n\n",
                index + 1,
                part.first_message_id,
                part.last_message_id,
                part.represented_source_tokens,
                part.content.trim(),
            ));
        }
        user_content.push_str(
            "Return only this finalized chronological multipart section. Preserve detailed evidence \
             up to the allocated budget and make any newer correction explicit.",
        );
        vec![
            Message::system(if has_shared_context {
                "You are the instruction-aware multipart final stage of a bounded \
                 conversation-compression pipeline. Finalize one ordered section under the supplied \
                 shared context without compounding the source-to-summary ratio."
            } else {
                "You are the multipart final stage of a bounded conversation-compression pipeline. \
                 Reduce one ordered section into its final form without compounding the \
                 source-to-summary ratio."
            }),
            Message::user(user_content),
        ]
    }

    fn target_for_source(&self, represented_source_tokens: u32) -> u32 {
        let ratio = self
            .request_budget
            .as_ref()
            .map(|budget| budget.target_ratio)
            .unwrap_or(0.20);
        ((represented_source_tokens as f64) * ratio).ceil().max(1.0) as u32
    }

    fn compatibility_request_budget(&self, messages: &[Message]) -> SummaryRequestBudget {
        let counter = TiktokenTokenCounter::default();
        let represented_source_tokens = counter.count_messages(messages);
        let previous_summary_tokens = self
            .existing_summary
            .as_deref()
            .map(|summary| counter.count_text(summary))
            .unwrap_or(0);
        SummaryRequestBudget {
            context_window_tokens: COMPATIBILITY_CONTEXT_WINDOW_TOKENS,
            max_output_tokens: COMPATIBILITY_MAX_OUTPUT_TOKENS,
            safety_margin_tokens: COMPATIBILITY_SAFETY_MARGIN_TOKENS,
            safe_window_percent: COMPATIBILITY_SAFE_WINDOW_PERCENT,
            target_summary_tokens: previous_summary_tokens
                .saturating_add(((represented_source_tokens as f64) * 0.20).ceil().max(1.0) as u32)
                .max(1),
            target_ratio: 0.20,
        }
    }

    fn request_fits(
        &self,
        messages: &[Message],
        requested_output_tokens: u32,
        budget: &SummaryRequestBudget,
    ) -> bool {
        if requested_output_tokens == 0 || requested_output_tokens > budget.max_output_tokens {
            return false;
        }
        let counter = TiktokenTokenCounter::default();
        let input_tokens = counter.count_messages(messages);
        input_tokens
            .saturating_add(requested_output_tokens)
            .saturating_add(budget.safety_margin_tokens)
            <= budget.safe_request_tokens()
    }

    fn source_units(&self, messages: &[Message]) -> Vec<SourceUnit> {
        let counter = TiktokenTokenCounter::default();
        MessageSegmenter::new()
            .segment(messages.to_vec())
            .into_iter()
            .filter(|segment| !segment.messages.is_empty())
            .map(|segment| {
                let first_message_id = segment
                    .messages
                    .first()
                    .map(|message| message.id.clone())
                    .unwrap_or_default();
                let last_message_id = segment
                    .messages
                    .last()
                    .map(|message| message.id.clone())
                    .unwrap_or_else(|| first_message_id.clone());
                SourceUnit {
                    text: Self::render_messages(&segment.messages),
                    represented_source_tokens: counter.count_messages(&segment.messages),
                    first_message_id,
                    last_message_id,
                    continuation_part: 1,
                }
            })
            .collect()
    }

    fn split_oversized_source_unit(
        &self,
        unit: SourceUnit,
        budget: &SummaryRequestBudget,
    ) -> Result<Vec<SourceUnit>, bamboo_compression::types::BudgetError> {
        let counter = TiktokenTokenCounter::default();
        let empty_prompt_tokens = counter.count_messages(&self.build_map_messages(&[], 1));
        let available = budget
            .safe_request_tokens()
            .saturating_sub(budget.safety_margin_tokens)
            .saturating_sub(empty_prompt_tokens);
        if available < 2 {
            return Err(bamboo_compression::types::BudgetError::TokenCountError(
                "summarization model context is too small for map prompt overhead".to_string(),
            ));
        }

        let ratio = budget.target_ratio.max(0.01);
        let max_by_window = ((available as f64) / (1.0 + ratio)).floor() as u32;
        let max_by_output = ((budget.max_output_tokens as f64) / ratio).floor() as u32;
        let initial_piece_tokens = max_by_window.min(max_by_output).max(1);

        let mut remaining = unit.text;
        let mut remaining_represented = unit.represented_source_tokens;
        let mut continuation_part = unit.continuation_part;
        let mut parts = Vec::new();
        while !remaining.is_empty() {
            let remaining_text_tokens = counter.count_text(&remaining).max(1);
            let mut piece_token_budget = initial_piece_tokens.min(remaining_text_tokens).max(1);
            let (piece_text, piece_represented) = loop {
                let mut prefix = counter.truncate_to_token_prefix(&remaining, piece_token_budget);
                if prefix.is_empty() {
                    prefix = remaining
                        .chars()
                        .next()
                        .map(|character| character.to_string())
                        .unwrap_or_default();
                }
                let is_last = prefix.len() == remaining.len();
                let prefix_tokens = counter.count_text(&prefix).max(1);
                let represented = if is_last {
                    remaining_represented
                } else if remaining_represented <= 1 {
                    0
                } else {
                    (((remaining_represented as u64) * (prefix_tokens as u64)
                        / (remaining_text_tokens as u64))
                        .max(1)
                        .min(remaining_represented.saturating_sub(1) as u64))
                        as u32
                };
                let candidate = SourceUnit {
                    text: prefix.clone(),
                    represented_source_tokens: represented,
                    first_message_id: unit.first_message_id.clone(),
                    last_message_id: unit.last_message_id.clone(),
                    continuation_part,
                };
                let requested_output = self.target_for_source(represented);
                if self.request_fits(
                    &self.build_map_messages(std::slice::from_ref(&candidate), requested_output),
                    requested_output,
                    budget,
                ) {
                    break (prefix, represented);
                }
                if piece_token_budget <= 1 {
                    return Err(bamboo_compression::types::BudgetError::TokenCountError(
                        format!(
                            "single source continuation cannot fit summarization model window (range {}..{})",
                            unit.first_message_id, unit.last_message_id
                        ),
                    ));
                }
                piece_token_budget = (piece_token_budget * 3 / 4).max(1);
            };

            let consumed_bytes = piece_text.len();
            parts.push(SourceUnit {
                text: piece_text,
                represented_source_tokens: piece_represented,
                first_message_id: unit.first_message_id.clone(),
                last_message_id: unit.last_message_id.clone(),
                continuation_part,
            });
            remaining = remaining[consumed_bytes..].to_string();
            remaining_represented = remaining_represented.saturating_sub(piece_represented);
            continuation_part += 1;
        }
        Ok(parts)
    }

    fn pack_source_chunks(
        &self,
        messages: &[Message],
        budget: &SummaryRequestBudget,
    ) -> Result<Vec<Vec<SourceUnit>>, bamboo_compression::types::BudgetError> {
        let mut bounded_units = Vec::new();
        for unit in self.source_units(messages) {
            let requested_output = self.target_for_source(unit.represented_source_tokens);
            let prompt = self.build_map_messages(std::slice::from_ref(&unit), requested_output);
            if self.request_fits(&prompt, requested_output, budget) {
                bounded_units.push(unit);
            } else {
                bounded_units.extend(self.split_oversized_source_unit(unit, budget)?);
            }
        }

        let mut chunks = Vec::new();
        let mut current = Vec::<SourceUnit>::new();
        for unit in bounded_units {
            let mut candidate = current.clone();
            candidate.push(unit.clone());
            let represented = candidate.iter().fold(0u32, |total, item| {
                total.saturating_add(item.represented_source_tokens)
            });
            let requested_output = self.target_for_source(represented);
            if self.request_fits(
                &self.build_map_messages(&candidate, requested_output),
                requested_output,
                budget,
            ) {
                current = candidate;
                continue;
            }
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
            }
            let requested_output = self.target_for_source(unit.represented_source_tokens);
            if !self.request_fits(
                &self.build_map_messages(std::slice::from_ref(&unit), requested_output),
                requested_output,
                budget,
            ) {
                return Err(bamboo_compression::types::BudgetError::TokenCountError(
                    "split source unit still exceeds summarization request ceiling".to_string(),
                ));
            }
            current.push(unit);
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        if chunks.is_empty() {
            return Err(bamboo_compression::types::BudgetError::TokenCountError(
                "no bounded summarization chunks were produced".to_string(),
            ));
        }
        Ok(chunks)
    }

    fn pack_reduction_groups(
        &self,
        parts: &[SummaryPart],
        budget: &SummaryRequestBudget,
    ) -> Vec<Vec<SummaryPart>> {
        let mut groups = Vec::new();
        let mut current = Vec::<SummaryPart>::new();
        for part in parts {
            let mut candidate = current.clone();
            candidate.push(part.clone());
            let represented = candidate.iter().fold(0u32, |total, item| {
                total.saturating_add(item.represented_source_tokens)
            });
            let requested_output = self.target_for_source(represented);
            if self.request_fits(
                &self.build_reduce_messages(&candidate, requested_output, false),
                requested_output,
                budget,
            ) {
                current = candidate;
            } else {
                if !current.is_empty() {
                    groups.push(std::mem::take(&mut current));
                }
                current.push(part.clone());
            }
        }
        if !current.is_empty() {
            groups.push(current);
        }
        groups
    }

    fn pack_multipart_final_groups(
        &self,
        parts: &[SummaryPart],
        shared_context_capsule: &str,
        retain_shared_context_capsule: bool,
        budget: &SummaryRequestBudget,
    ) -> Result<Vec<Vec<SummaryPart>>, bamboo_compression::types::BudgetError> {
        let mut bounded_parts = Vec::new();
        for part in parts {
            let requested_output = self.target_for_source(part.represented_source_tokens);
            if self.request_fits(
                &self.build_multipart_finalize_messages(
                    std::slice::from_ref(part),
                    requested_output,
                    shared_context_capsule,
                    retain_shared_context_capsule,
                ),
                requested_output,
                budget,
            ) {
                bounded_parts.push(part.clone());
            } else {
                bounded_parts.extend(self.split_oversized_multipart_part(
                    part.clone(),
                    shared_context_capsule,
                    retain_shared_context_capsule,
                    budget,
                )?);
            }
        }

        let mut groups = Vec::new();
        let mut current = Vec::<SummaryPart>::new();
        for part in &bounded_parts {
            let mut candidate = current.clone();
            candidate.push(part.clone());
            let represented = candidate.iter().fold(0u32, |total, item| {
                total.saturating_add(item.represented_source_tokens)
            });
            let requested_output = self.target_for_source(represented);
            if self.request_fits(
                &self.build_multipart_finalize_messages(
                    &candidate,
                    requested_output,
                    shared_context_capsule,
                    retain_shared_context_capsule,
                ),
                requested_output,
                budget,
            ) {
                current = candidate;
                continue;
            }

            if !current.is_empty() {
                groups.push(std::mem::take(&mut current));
            }
            current.push(part.clone());
        }
        if !current.is_empty() {
            groups.push(current);
        }
        Ok(groups)
    }

    fn split_oversized_multipart_part(
        &self,
        part: SummaryPart,
        shared_context_capsule: &str,
        retain_shared_context_capsule: bool,
        budget: &SummaryRequestBudget,
    ) -> Result<Vec<SummaryPart>, bamboo_compression::types::BudgetError> {
        let counter = TiktokenTokenCounter::default();
        let mut remaining = part.content;
        let mut remaining_represented = part.represented_source_tokens;
        let mut split_parts = Vec::new();
        while !remaining.is_empty() {
            let remaining_text_tokens = counter.count_text(&remaining).max(1);
            let mut piece_token_budget = remaining_text_tokens;
            let (piece_text, piece_represented) = loop {
                let mut prefix = counter.truncate_to_token_prefix(&remaining, piece_token_budget);
                if prefix.is_empty() {
                    prefix = remaining
                        .chars()
                        .next()
                        .map(|character| character.to_string())
                        .unwrap_or_default();
                }
                let is_last = prefix.len() == remaining.len();
                let prefix_tokens = counter.count_text(&prefix).max(1);
                let represented = if is_last {
                    remaining_represented
                } else if remaining_represented <= 1 {
                    0
                } else {
                    (((remaining_represented as u64) * (prefix_tokens as u64)
                        / (remaining_text_tokens as u64))
                        .max(1)
                        .min(remaining_represented.saturating_sub(1) as u64))
                        as u32
                };
                let candidate = SummaryPart {
                    content: prefix.clone(),
                    represented_source_tokens: represented,
                    first_message_id: part.first_message_id.clone(),
                    last_message_id: part.last_message_id.clone(),
                };
                let requested_output = self.target_for_source(represented);
                if self.request_fits(
                    &self.build_multipart_finalize_messages(
                        std::slice::from_ref(&candidate),
                        requested_output,
                        shared_context_capsule,
                        retain_shared_context_capsule,
                    ),
                    requested_output,
                    budget,
                ) {
                    break (prefix, represented);
                }
                if piece_token_budget <= 1 {
                    return Err(bamboo_compression::types::BudgetError::TokenCountError(
                        format!(
                            "shared-context capsule and multipart source range {}..{} cannot fit the summarization request ceiling",
                            part.first_message_id, part.last_message_id
                        ),
                    ));
                }
                piece_token_budget = (piece_token_budget * 3 / 4).max(1);
            };

            let consumed_bytes = piece_text.len();
            split_parts.push(SummaryPart {
                content: piece_text,
                represented_source_tokens: piece_represented,
                first_message_id: part.first_message_id.clone(),
                last_message_id: part.last_message_id.clone(),
            });
            remaining = remaining[consumed_bytes..].to_string();
            remaining_represented = remaining_represented.saturating_sub(piece_represented);
        }
        Ok(split_parts)
    }

    async fn execute_bounded_request(
        &self,
        stage: &str,
        stage_index: usize,
        stage_count: usize,
        messages: &[Message],
        requested_output_tokens: u32,
        budget: &SummaryRequestBudget,
    ) -> Result<String, bamboo_compression::types::BudgetError> {
        let counter = TiktokenTokenCounter::default();
        let input_tokens = counter.count_messages(messages);
        let safe_request_tokens = budget.safe_request_tokens();
        if !self.request_fits(messages, requested_output_tokens, budget) {
            return Err(bamboo_compression::types::BudgetError::TokenCountError(
                format!(
                    "bounded summarization invariant failed for {stage}: input={input_tokens}, output={requested_output_tokens}, safety={}, safe_limit={safe_request_tokens}",
                    budget.safety_margin_tokens,
                ),
            ));
        }
        let progress = SummarizationProgress {
            stage: stage.to_string(),
            stage_index,
            stage_count,
            estimated_input_tokens: input_tokens,
            requested_output_tokens,
            safe_request_tokens,
            model_context_tokens: budget.context_window_tokens,
        };
        if let Some(callback) = self.progress_callback.as_ref() {
            callback(&progress);
        }
        tracing::info!(
            logical_pass_id = self.logical_pass_id.as_deref().unwrap_or("untracked"),
            logical_phase = self.logical_phase.as_deref().unwrap_or("unspecified"),
            stage,
            stage_index,
            stage_count,
            input_tokens,
            requested_output_tokens,
            safe_request_tokens,
            model_context_tokens = budget.context_window_tokens,
            model = %self.model,
            "Executing bounded summarization request"
        );
        let content = self
            .collect_stream_response(messages, requested_output_tokens)
            .await
            .map_err(|error| {
                bamboo_compression::types::BudgetError::TokenCountError(format!(
                    "{stage} stage {stage_index}/{stage_count} failed: {error}"
                ))
            })?;
        if content.trim().is_empty() {
            return Err(bamboo_compression::types::BudgetError::TokenCountError(
                format!(
                    "{stage} stage {stage_index}/{stage_count} returned an empty completed response"
                ),
            ));
        }
        Ok(content)
    }

    async fn build_bounded_shared_context_capsule(
        &self,
        shared_context: &str,
        target_tokens: u32,
        budget: &SummaryRequestBudget,
    ) -> Result<(String, u32, u32), bamboo_compression::types::BudgetError> {
        let counter = TiktokenTokenCounter::default();
        let shared_tokens = counter.count_text(shared_context).max(1);
        let capsule_target = target_tokens.max(1).min(shared_tokens);
        // A shared-context capsule is still a compression pass, so it follows
        // the same map-then-reduce contract even when its fully rendered source
        // would fit one provider request. The child has no shared context of its
        // own, so multipart finalization cannot recurse back into this lane.
        let capsule_messages = [Message::user(format!(
            "Preserve this shared context capsule, including every binding compression \
             instruction and hook-injected directive:\n\n{shared_context}"
        ))];
        let capsule_source_tokens = counter.count_messages(&capsule_messages).max(1);
        let mut capsule_budget = budget.clone();
        capsule_budget.target_summary_tokens = capsule_target;
        // A prior durable summary has already paid the global compression
        // ratio. Allow a 1:1 capsule map when its retained allocation calls for
        // it; clamping this child to the normal 50% ceiling would silently apply
        // a second compression ratio before the outer multipart reduce.
        capsule_budget.target_ratio =
            (capsule_target as f64 / capsule_source_tokens as f64).min(1.0);
        let mut capsule_summarizer =
            LlmSummarizer::new(Arc::clone(&self.llm), self.model.clone(), None, None)
                .with_summary_mode(SummaryMode::FullRewrite)
                .with_request_budget(capsule_budget)
                .with_heuristic_fallback_on_error(false);
        if let (Some(pass_id), Some(phase)) =
            (self.logical_pass_id.as_ref(), self.logical_phase.as_ref())
        {
            capsule_summarizer =
                capsule_summarizer.with_logical_pass_context(pass_id.clone(), phase.clone());
        }
        if let Some(callback) = self.progress_callback.as_ref() {
            capsule_summarizer = capsule_summarizer.with_progress_callback(Arc::clone(callback));
        }
        let report = Box::pin(capsule_summarizer.summarize_with_report(&capsule_messages)).await?;
        let content = counter.truncate_to_token_prefix(report.content.trim(), capsule_target);
        if content.trim().is_empty() {
            return Err(bamboo_compression::types::BudgetError::TokenCountError(
                "bounded shared-context capsule is empty".to_string(),
            ));
        }
        Ok((content, report.map_calls, report.reduce_calls))
    }

    fn compose_multipart_summary(
        parts: &[SummaryPart],
        shared_context_capsule: Option<&str>,
    ) -> String {
        let mut sections = Vec::new();
        if let Some(capsule) = shared_context_capsule
            .map(str::trim)
            .filter(|capsule| !capsule.is_empty())
        {
            sections.push(capsule.to_string());
        }
        sections.extend(
            parts
                .iter()
                .map(|part| part.content.trim())
                .filter(|content| !content.is_empty())
                .map(String::from),
        );
        sections.join("\n\n")
    }

    async fn finalize_multipart_summary(
        &self,
        parts: &[SummaryPart],
        budget: &SummaryRequestBudget,
    ) -> Result<(String, u32, u32), bamboo_compression::types::BudgetError> {
        let shared_context = self.render_shared_context();
        let represented_new_source = parts.iter().fold(0u32, |total, part| {
            total.saturating_add(part.represented_source_tokens)
        });
        let new_source_target = self.target_for_source(represented_new_source);
        let retained_capsule_target = budget
            .target_summary_tokens
            .saturating_sub(new_source_target);
        let retain_shared_context_capsule = retained_capsule_target > 0;
        let (shared_context_capsule, capsule_map_calls, capsule_reduce_calls) =
            if shared_context.trim().is_empty() {
                (String::new(), 0, 0)
            } else {
                let counter = TiktokenTokenCounter::default();
                let reference_only_target =
                    self.target_for_source(counter.count_text(&shared_context));
                self.build_bounded_shared_context_capsule(
                    &shared_context,
                    if retain_shared_context_capsule {
                        retained_capsule_target
                    } else {
                        reference_only_target
                    },
                    budget,
                )
                .await?
            };
        let mut multipart_reduce_calls = 0u32;
        let groups = self.pack_multipart_final_groups(
            parts,
            &shared_context_capsule,
            retain_shared_context_capsule,
            budget,
        )?;
        let group_count = groups.len();
        let mut finalized = Vec::with_capacity(group_count);
        for (group_index, group) in groups.into_iter().enumerate() {
            let represented = group.iter().fold(0u32, |total, part| {
                total.saturating_add(part.represented_source_tokens)
            });
            let requested_output = self.target_for_source(represented);
            let prompt = self.build_multipart_finalize_messages(
                &group,
                requested_output,
                &shared_context_capsule,
                retain_shared_context_capsule,
            );
            let content = self
                .execute_bounded_request(
                    "multipart_final_reduce",
                    group_index + 1,
                    group_count,
                    &prompt,
                    requested_output,
                    budget,
                )
                .await?;
            multipart_reduce_calls = multipart_reduce_calls.saturating_add(1);
            finalized.push(SummaryPart {
                content,
                represented_source_tokens: represented,
                first_message_id: group
                    .first()
                    .map(|part| part.first_message_id.clone())
                    .unwrap_or_default(),
                last_message_id: group
                    .last()
                    .map(|part| part.last_message_id.clone())
                    .unwrap_or_default(),
            });
        }
        Ok((
            Self::compose_multipart_summary(
                &finalized,
                (retain_shared_context_capsule && !shared_context_capsule.trim().is_empty())
                    .then_some(shared_context_capsule.as_str()),
            ),
            capsule_map_calls,
            capsule_reduce_calls.saturating_add(multipart_reduce_calls),
        ))
    }

    async fn summarize_bounded(
        &self,
        messages: &[Message],
        budget: &SummaryRequestBudget,
    ) -> Result<SummarizationReport, bamboo_compression::types::BudgetError> {
        let counter = TiktokenTokenCounter::default();
        let represented_source_tokens = counter.count_messages(messages);
        let final_target = budget.target_summary_tokens.max(1);

        // Every bounded compression pass deliberately goes through map then
        // reduce, even when the source would fit a single provider request.
        // Keeping one pipeline for all source sizes avoids a size-dependent
        // semantic split and guarantees that no raw source is ever sent
        // directly to the terminal summarizer.
        let chunks = self.pack_source_chunks(messages, budget)?;
        let mut map_calls = 0u32;
        let mut parts = Vec::with_capacity(chunks.len());
        for (index, chunk) in chunks.iter().enumerate() {
            let represented = chunk.iter().fold(0u32, |total, unit| {
                total.saturating_add(unit.represented_source_tokens)
            });
            let requested_output = self.target_for_source(represented);
            let prompt = self.build_map_messages(chunk, requested_output);
            let content = self
                .execute_bounded_request(
                    "map",
                    index + 1,
                    chunks.len(),
                    &prompt,
                    requested_output,
                    budget,
                )
                .await?;
            map_calls = map_calls.saturating_add(1);
            let first_message_id = chunk
                .first()
                .map(|unit| unit.first_message_id.clone())
                .unwrap_or_else(|| format!("chunk-{index}"));
            let last_message_id = chunk
                .last()
                .map(|unit| unit.last_message_id.clone())
                .unwrap_or_else(|| first_message_id.clone());
            parts.push(SummaryPart {
                content,
                represented_source_tokens: represented,
                first_message_id,
                last_message_id,
            });
        }

        let mut reduce_calls = 0u32;
        let mut depth = 0usize;
        loop {
            let final_prompt = self.build_reduce_messages(&parts, final_target, true);
            if self.request_fits(&final_prompt, final_target, budget) {
                let content = self
                    .execute_bounded_request(
                        "final_reduce",
                        1,
                        1,
                        &final_prompt,
                        final_target,
                        budget,
                    )
                    .await?;
                reduce_calls = reduce_calls.saturating_add(1);
                let actual_summary_tokens = counter.count_text(&content);
                let underfilled =
                    actual_summary_tokens.saturating_mul(5) < final_target.saturating_mul(4);
                return Ok(SummarizationReport {
                    content,
                    represented_source_tokens,
                    target_summary_tokens: final_target,
                    actual_summary_tokens,
                    map_calls,
                    reduce_calls,
                    fallback_used: false,
                    budget_clamped: underfilled,
                    budget_clamp_reason: underfilled
                        .then(|| "model_returned_below_80_percent_of_target".to_string()),
                });
            }

            if parts.len() <= 1 || depth >= 8 {
                break;
            }
            let groups = self.pack_reduction_groups(&parts, budget);
            if groups.len() >= parts.len() {
                break;
            }
            let mut reduced = Vec::with_capacity(groups.len());
            let group_count = groups.len();
            for (group_index, group) in groups.into_iter().enumerate() {
                if group.len() == 1 {
                    reduced.push(group.into_iter().next().expect("single reduction part"));
                    continue;
                }
                let represented = group.iter().fold(0u32, |total, part| {
                    total.saturating_add(part.represented_source_tokens)
                });
                let requested_output = self.target_for_source(represented);
                let prompt = self.build_reduce_messages(&group, requested_output, false);
                let content = self
                    .execute_bounded_request(
                        "intermediate_reduce",
                        group_index + 1,
                        group_count,
                        &prompt,
                        requested_output,
                        budget,
                    )
                    .await?;
                reduce_calls = reduce_calls.saturating_add(1);
                reduced.push(SummaryPart {
                    content,
                    represented_source_tokens: represented,
                    first_message_id: group
                        .first()
                        .map(|part| part.first_message_id.clone())
                        .unwrap_or_default(),
                    last_message_id: group
                        .last()
                        .map(|part| part.last_message_id.clone())
                        .unwrap_or_default(),
                });
            }
            parts = reduced;
            depth += 1;
        }

        // A single final model response cannot represent an arbitrarily large
        // 20%-of-source target when the selected model has a smaller output
        // limit. Finalize bounded sections under one shared-context capsule,
        // then compose them instead of applying another global 20% pass (which
        // would collapse to ~4%).
        let (content, multipart_map_calls, multipart_reduce_calls) =
            self.finalize_multipart_summary(&parts, budget).await?;
        map_calls = map_calls.saturating_add(multipart_map_calls);
        reduce_calls = reduce_calls.saturating_add(multipart_reduce_calls);
        let actual_summary_tokens = counter.count_text(&content);
        let underfilled = actual_summary_tokens.saturating_mul(5) < final_target.saturating_mul(4);
        tracing::info!(
            logical_pass_id = self.logical_pass_id.as_deref().unwrap_or("untracked"),
            logical_phase = self.logical_phase.as_deref().unwrap_or("unspecified"),
            part_count = parts.len(),
            multipart_map_calls,
            multipart_reduce_calls,
            actual_summary_tokens,
            target_summary_tokens = final_target,
            "Final single-response reduce did not fit; composed instruction-aware bounded multipart summary"
        );
        Ok(SummarizationReport {
            content,
            represented_source_tokens,
            target_summary_tokens: final_target,
            actual_summary_tokens,
            map_calls,
            reduce_calls,
            fallback_used: false,
            budget_clamped: underfilled,
            budget_clamp_reason: underfilled
                .then(|| "multipart_summary_below_80_percent_of_target".to_string()),
        })
    }

    async fn heuristic_report(
        &self,
        messages: &[Message],
        target_summary_tokens: u32,
        reason: &str,
    ) -> Result<SummarizationReport, bamboo_compression::types::BudgetError> {
        let counter = TiktokenTokenCounter::default();
        let heuristic = HeuristicSummarizer::new().summarize(messages).await?;
        let content = counter.truncate_to_token_prefix(&heuristic, target_summary_tokens.max(1));
        Ok(SummarizationReport {
            represented_source_tokens: counter.count_messages(messages),
            actual_summary_tokens: counter.count_text(&content),
            target_summary_tokens,
            content,
            map_calls: 0,
            reduce_calls: 0,
            fallback_used: true,
            budget_clamped: true,
            budget_clamp_reason: Some(reason.to_string()),
        })
    }

    pub async fn summarize_with_report(
        &self,
        messages: &[Message],
    ) -> Result<SummarizationReport, bamboo_compression::types::BudgetError> {
        if messages.is_empty() {
            return Ok(SummarizationReport {
                content: "No conversation history to summarize.".to_string(),
                represented_source_tokens: 0,
                target_summary_tokens: 0,
                actual_summary_tokens: 0,
                map_calls: 0,
                reduce_calls: 0,
                fallback_used: false,
                budget_clamped: false,
                budget_clamp_reason: None,
            });
        }

        let compatibility_budget;
        let budget = if let Some(budget) = self.request_budget.as_ref() {
            budget
        } else {
            compatibility_budget = self.compatibility_request_budget(messages);
            &compatibility_budget
        };
        let target_summary_tokens = budget.target_summary_tokens;
        let result = self.summarize_bounded(messages, budget).await;

        match result {
            Ok(report) if !report.content.trim().is_empty() => Ok(report),
            Ok(_) => {
                tracing::warn!(
                    "LlmSummarizer: LLM returned empty summary, falling back to heuristic"
                );
                self.heuristic_report(messages, target_summary_tokens, "empty_llm_response")
                    .await
            }
            Err(error) if self.heuristic_fallback_on_error && self.request_budget.is_none() => {
                tracing::warn!(
                    "LlmSummarizer: compatibility map/reduce pipeline failed ({}), falling back to heuristic",
                    error
                );
                self.heuristic_report(
                    messages,
                    target_summary_tokens,
                    "llm_error_heuristic_fallback",
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    /// Consume an LLM stream and collect the full text response.
    async fn collect_stream_response(
        &self,
        messages: &[Message],
        max_output_tokens: u32,
    ) -> Result<String, bamboo_compression::types::BudgetError> {
        // Compression calls need most of their output allowance for the summary
        // itself. Low reasoning keeps dynamic, potentially small output budgets
        // useful across both reasoning and non-reasoning models.
        let options = LLMRequestOptions {
            session_id: None,
            reasoning_effort: Some(ReasoningEffort::Low),
            parallel_tool_calls: None,
            required_tool: None,
            responses: None,
            request_purpose: Some("compression".to_string()),
            cache: None,
        };
        let stream = self
            .llm
            .chat_stream_with_options(
                messages,
                &[],
                Some(max_output_tokens.max(1)),
                &self.model,
                Some(&options),
            )
            .await
            .map_err(|e| {
                bamboo_compression::types::BudgetError::TokenCountError(format!(
                    "LLM summarization call failed: {}",
                    e
                ))
            })?;

        let mut content = String::new();
        let mut stream = stream;
        let mut terminal_done = false;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(LLMChunk::Token(text)) => content.push_str(&text),
                Ok(LLMChunk::Done) => {
                    terminal_done = true;
                    break;
                }
                Ok(_) => {} // Ignore reasoning tokens, tool calls, etc.
                Err(e) => {
                    tracing::warn!("LLM summarization stream error: {}", e);
                    return Err(bamboo_compression::types::BudgetError::TokenCountError(
                        format!("LLM summarization stream failed: {}", e),
                    ));
                }
            }
        }

        if !terminal_done && !content.is_empty() {
            return Err(bamboo_compression::types::BudgetError::TokenCountError(
                "LLM summarization stream ended without terminal completion".to_string(),
            ));
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
            .field("logical_pass_id", &self.logical_pass_id)
            .field("logical_phase", &self.logical_phase)
            .field("has_progress_callback", &self.progress_callback.is_some())
            .finish()
    }
}

#[async_trait]
impl Summarizer for LlmSummarizer {
    async fn summarize(
        &self,
        messages: &[Message],
    ) -> Result<String, bamboo_compression::types::BudgetError> {
        tracing::info!(
            "LlmSummarizer: summarizing {} messages using model '{}' (existing_summary={})",
            messages.len(),
            self.model,
            self.existing_summary.is_some()
        );
        let report = self.summarize_with_report(messages).await?;
        tracing::info!(
            chars = report.content.len(),
            actual_summary_tokens = report.actual_summary_tokens,
            target_summary_tokens = report.target_summary_tokens,
            map_calls = report.map_calls,
            reduce_calls = report.reduce_calls,
            fallback_used = report.fallback_used,
            "LlmSummarizer: generated summary"
        );
        Ok(report.content)
    }

    fn estimate_summary_tokens(&self, message_count: usize) -> u32 {
        self.request_budget
            .as_ref()
            .map(|budget| budget.target_summary_tokens)
            .unwrap_or_else(|| (message_count * 80).min(2000) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{FunctionCall, ReasoningEffort, ToolCall};
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

    fn test_summary_part() -> SummaryPart {
        SummaryPart {
            content: "New chronological partial summary".to_string(),
            represented_source_tokens: 100,
            first_message_id: "first".to_string(),
            last_message_id: "last".to_string(),
        }
    }

    #[test]
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
        let prompt_messages = summarizer.build_reduce_messages(&[test_summary_part()], 20, true);
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
    async fn llm_summarizer_requests_low_reasoning_effort_for_summary_calls() {
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
        assert_eq!(
            captured.as_slice(),
            [Some(ReasoningEffort::Low), Some(ReasoningEffort::Low)],
            "compatibility callers must use one map request followed by one reduce request"
        );
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
    async fn compatibility_summarizer_uses_source_derived_output_budget_with_low_reasoning() {
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
        assert_eq!(
            captured_reasoning.as_slice(),
            [Some(ReasoningEffort::Low), Some(ReasoningEffort::Low)]
        );
        let expected_target = ((TiktokenTokenCounter::default().count_messages(&messages) as f64)
            * 0.20)
            .ceil()
            .max(1.0) as u32;
        assert_eq!(
            captured_max_tokens.as_slice(),
            [Some(expected_target), Some(expected_target)],
            "both compatibility map and reduce requests use the source-derived 20% target"
        );
    }

    #[test]
    fn compatibility_budget_retains_previous_summary_before_adding_twenty_percent() {
        let previous_summary = "durable prior summary evidence ".repeat(20);
        let summarizer = LlmSummarizer::new(
            Arc::new(DummyProvider),
            "summary-model".to_string(),
            Some(previous_summary.clone()),
            None,
        );
        let messages = summary_messages();
        let counter = TiktokenTokenCounter::default();

        let budget = summarizer.compatibility_request_budget(&messages);

        let expected = counter.count_text(&previous_summary).saturating_add(
            ((counter.count_messages(&messages) as f64) * 0.20)
                .ceil()
                .max(1.0) as u32,
        );
        assert_eq!(budget.target_summary_tokens, expected);
    }

    #[test]
    fn full_rewrite_mode_uses_default_final_reduce_prompt() {
        let summarizer =
            LlmSummarizer::new(Arc::new(DummyProvider), "model".to_string(), None, None)
                .with_summary_mode(SummaryMode::FullRewrite);
        let prompts = summarizer.build_reduce_messages(&[test_summary_part()], 20, true);
        let system = &prompts[0].content;
        assert!(
            system.contains("final reduce stage"),
            "FullRewrite prompt should identify the final reduce stage"
        );
        assert!(
            !system.contains("incremental final reduce"),
            "FullRewrite prompt should not contain incremental language"
        );
    }

    #[test]
    fn incremental_merge_mode_uses_update_final_reduce_prompt() {
        let summarizer = LlmSummarizer::new(
            Arc::new(DummyProvider),
            "model".to_string(),
            Some("Previous summary content".to_string()),
            None,
        )
        .with_summary_mode(SummaryMode::IncrementalMerge);
        let prompts = summarizer.build_reduce_messages(&[test_summary_part()], 20, true);
        let system = &prompts[0].content;
        assert!(
            system.contains("incremental final reduce stage"),
            "IncrementalMerge prompt should identify the incremental final reduce stage"
        );
        assert!(
            system.contains("Update the supplied prior summary"),
            "IncrementalMerge prompt should direct the reducer to update the prior summary"
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
        let prompts = summarizer.build_reduce_messages(&[test_summary_part()], 20, true);
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
        // Compatibility callers that do not provide exact model limits retain
        // the historical heuristic recovery behavior. Production compression
        // always supplies an explicit budget and surfaces stage failures.
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
        // Compatibility callers can still opt out of heuristic recovery.
        let summarizer =
            LlmSummarizer::new(Arc::new(FailingProvider), "model".to_string(), None, None)
                .with_heuristic_fallback_on_error(false);
        let out = summarizer.summarize(&summary_messages()).await;
        assert!(
            out.is_err(),
            "with the heuristic fallback disabled, a transient LLM error must surface"
        );
    }

    #[derive(Default)]
    struct BoundedRequestCaptureProvider {
        requests: Mutex<Vec<(Vec<Message>, u32)>>,
    }

    #[async_trait]
    impl LLMProvider for BoundedRequestCaptureProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[bamboo_domain::ToolSchema],
            max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.requests
                .lock()
                .expect("bounded request capture lock")
                .push((messages.to_vec(), max_output_tokens.unwrap_or_default()));
            Ok(Box::pin(stream::iter(vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token(
                    "Detailed bounded summary with requirements, decisions, files, tests, and next steps. "
                        .repeat(16),
                )),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ])))
        }
    }

    #[derive(Default)]
    struct MultipartSharedContextProvider {
        requests: Mutex<Vec<(Vec<Message>, u32)>>,
    }

    impl MultipartSharedContextProvider {
        fn echoed_shared_context(rendered: &str) -> String {
            let mut retained = vec!["SHARED_CAPSULE_763"];
            for sentinel in [
                "PREVIOUS_SENTINEL_763",
                "RUNTIME_CONTEXT_SENTINEL_763",
                "CUSTOM_INSTRUCTION_SENTINEL_763",
                "PRECOMPACT_SENTINEL_763",
            ] {
                if rendered.contains(sentinel) {
                    retained.push(sentinel);
                }
            }
            retained.join(" ")
        }
    }

    #[async_trait]
    impl LLMProvider for MultipartSharedContextProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[bamboo_domain::ToolSchema],
            max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.requests
                .lock()
                .expect("multipart request capture lock")
                .push((messages.to_vec(), max_output_tokens.unwrap_or_default()));
            let rendered = messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            let response = if rendered.contains("shared-context capsule stage") {
                "SHARED_CAPSULE_763 PREVIOUS_SENTINEL_763 RUNTIME_CONTEXT_SENTINEL_763 \
                 CUSTOM_INSTRUCTION_SENTINEL_763 PRECOMPACT_SENTINEL_763"
                    .to_string()
            } else if rendered.contains("multipart final stage") {
                format!(
                    "FINAL_MULTIPART_SECTION_763 {}",
                    Self::echoed_shared_context(&rendered)
                )
            } else if rendered.contains("Preserve this shared context capsule")
                || rendered.contains("SHARED_CAPSULE_763")
                || rendered.contains("PREVIOUS_SENTINEL_763")
                || rendered.contains("RUNTIME_CONTEXT_SENTINEL_763")
                || rendered.contains("CUSTOM_INSTRUCTION_SENTINEL_763")
                || rendered.contains("PRECOMPACT_SENTINEL_763")
            {
                Self::echoed_shared_context(&rendered)
            } else if rendered.contains("intermediate reduce stage") {
                "INTERMEDIATE_PART_763 with detailed chronological evidence. ".repeat(12)
            } else {
                "MAP_PART_763 with detailed chronological evidence. ".repeat(12)
            };
            Ok(Box::pin(stream::iter(vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token(response)),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ])))
        }
    }

    fn bounded_budget(
        context_window_tokens: u32,
        max_output_tokens: u32,
        safety_margin_tokens: u32,
        target_summary_tokens: u32,
    ) -> SummaryRequestBudget {
        let token_budget = TokenBudget::with_safety_margin(
            context_window_tokens,
            max_output_tokens,
            bamboo_compression::BudgetStrategy::default(),
            safety_margin_tokens,
        );
        SummaryRequestBudget::from_token_budget(&token_budget, 80, target_summary_tokens, 0.20)
    }

    #[tokio::test]
    async fn bounded_small_source_still_uses_map_then_reduce() {
        let provider = Arc::new(BoundedRequestCaptureProvider::default());
        let budget = bounded_budget(10_000, 2_000, 100, 400);
        let summarizer =
            LlmSummarizer::new(provider.clone(), "summary-model".to_string(), None, None)
                .with_request_budget(budget.clone())
                .with_heuristic_fallback_on_error(false);

        let report = summarizer
            .summarize_with_report(&summary_messages())
            .await
            .expect("bounded chunked summary");
        assert_eq!(report.map_calls, 1);
        assert_eq!(report.reduce_calls, 1);

        let requests = provider.requests.lock().expect("capture lock");
        assert_eq!(requests.len(), 2);
        let map_request = requests
            .iter()
            .find(|(messages, _)| {
                messages
                    .iter()
                    .any(|message| message.content.contains("map stage"))
            })
            .expect("small input must still use a map request");
        let reduce_request = requests
            .iter()
            .find(|(messages, _)| {
                messages
                    .iter()
                    .any(|message| message.content.contains("final reduce stage"))
            })
            .expect("small input must still use a final reduce request");
        assert_eq!(reduce_request.1, 400);
        let counter = TiktokenTokenCounter::default();
        for (request, output) in [map_request, reduce_request] {
            assert!(
                counter
                    .count_messages(request)
                    .saturating_add(*output)
                    .saturating_add(budget.safety_margin_tokens)
                    <= budget.safe_request_tokens()
            );
        }
    }

    #[tokio::test]
    async fn retained_shared_context_is_mapped_and_reduced_without_a_second_ratio() {
        let provider = Arc::new(BoundedRequestCaptureProvider::default());
        let budget = bounded_budget(10_000, 2_000, 100, 1_000);
        let summarizer =
            LlmSummarizer::new(provider.clone(), "summary-model".to_string(), None, None)
                .with_request_budget(budget.clone())
                .with_heuristic_fallback_on_error(false);
        let shared_context =
            "durable prior summary requirement decision exact path test evidence ".repeat(24);
        let shared_tokens = TiktokenTokenCounter::default().count_text(&shared_context);

        let (_capsule, map_calls, reduce_calls) = summarizer
            .build_bounded_shared_context_capsule(&shared_context, shared_tokens, &budget)
            .await
            .expect("retained shared context should use bounded map/reduce");

        assert_eq!(map_calls, 1);
        assert_eq!(reduce_calls, 1);
        let requests = provider.requests.lock().expect("capture lock");
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|(_, output)| *output >= shared_tokens.saturating_sub(1)),
            "the child map/reduce must preserve its allocated prior-summary size instead of applying 20% again"
        );
    }

    #[tokio::test]
    async fn bounded_pipeline_chunks_large_source_and_never_compounds_twenty_percent_target() {
        let provider = Arc::new(BoundedRequestCaptureProvider::default());
        let budget = bounded_budget(3_000, 800, 100, 600);
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "small-summary-model".to_string(),
            Some("Previous durable summary evidence. ".repeat(12)),
            None,
        )
        .with_context_blocks(vec![ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Current Task List",
            "Active compression task with exact acceptance evidence. ".repeat(8),
        )])
        .with_custom_instructions(Some(
            "Keep exact paths, failures, and remaining work.".to_string(),
        ))
        .with_request_budget(budget.clone())
        .with_heuristic_fallback_on_error(false);
        let messages = (0..80)
            .map(|index| {
                if index % 2 == 0 {
                    Message::user(format!(
                        "user-{index} {}",
                        "requirement decision path error evidence ".repeat(24)
                    ))
                } else {
                    Message::assistant(
                        format!(
                            "assistant-{index} {}",
                            "implementation command output test result next step ".repeat(24)
                        ),
                        None,
                    )
                }
            })
            .collect::<Vec<_>>();

        let report = summarizer
            .summarize_with_report(&messages)
            .await
            .expect("chunked summary");
        assert!(report.map_calls > 1, "large source must use multiple maps");
        assert!(
            report.reduce_calls >= 1,
            "bounded partials should receive a final reduce when it fits"
        );

        let requests = provider.requests.lock().expect("capture lock");
        let counter = TiktokenTokenCounter::default();
        for (request, output_tokens) in requests.iter() {
            assert!(
                counter
                    .count_messages(request)
                    .saturating_add(*output_tokens)
                    .saturating_add(budget.safety_margin_tokens)
                    <= budget.safe_request_tokens(),
                "every map/reduce request must satisfy the 80% invariant"
            );
            assert!(*output_tokens <= budget.max_output_tokens);
        }
        assert_eq!(
            requests.last().map(|(_, output)| *output),
            Some(600),
            "the final reduce keeps the global source-derived target instead of taking 20% of map summaries"
        );
        assert!(
            requests.iter().any(|(request, _)| request
                .iter()
                .any(|message| { message.content.contains("intermediate reduce stage") })),
            "large ordered partials should be recursively reduced before the final merge"
        );
        let final_request = requests.last().expect("final reduce request");
        let final_rendered = final_request
            .0
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(final_rendered.contains("Previous durable summary evidence"));
        assert!(final_rendered.contains("Current Task List"));
        assert!(final_rendered.contains("Keep exact paths"));
    }

    #[tokio::test]
    async fn multipart_terminal_path_preserves_shared_context_and_instructions_boundedly() {
        let provider = Arc::new(MultipartSharedContextProvider::default());
        let budget = bounded_budget(3_000, 240, 100, 700);
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "small-summary-model".to_string(),
            Some(format!(
                "PREVIOUS_SENTINEL_763 durable prior state. {}",
                "large previous summary evidence ".repeat(2_000)
            )),
            None,
        )
        .with_context_blocks(vec![ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Current Runtime State",
            "RUNTIME_CONTEXT_SENTINEL_763 active objective and exact evidence.",
        )])
        .with_custom_instructions(Some(
            "CUSTOM_INSTRUCTION_SENTINEL_763 keep exact paths and failures.\n\n\
             ## PreCompact Hook Instructions\n\n\
             PRECOMPACT_SENTINEL_763 preserve the hook-injected next step."
                .to_string(),
        ))
        .with_request_budget(budget.clone())
        .with_heuristic_fallback_on_error(false);
        let messages = (0..100)
            .map(|index| {
                if index % 2 == 0 {
                    Message::user(format!(
                        "user-{index} {}",
                        "requirement decision path error evidence ".repeat(24)
                    ))
                } else {
                    Message::assistant(
                        format!(
                            "assistant-{index} {}",
                            "implementation command output test result next step ".repeat(24)
                        ),
                        None,
                    )
                }
            })
            .collect::<Vec<_>>();

        let report = summarizer
            .summarize_with_report(&messages)
            .await
            .expect("multipart finalization should preserve shared context");
        assert!(report.map_calls > 1);
        assert!(
            report.reduce_calls >= 2,
            "the shared capsule and multipart final sections must be observable as reduce calls"
        );
        assert!(report.content.contains("PREVIOUS_SENTINEL_763"));
        assert!(report.content.contains("RUNTIME_CONTEXT_SENTINEL_763"));
        assert!(report.content.contains("CUSTOM_INSTRUCTION_SENTINEL_763"));
        assert!(report.content.contains("PRECOMPACT_SENTINEL_763"));

        let requests = provider.requests.lock().expect("capture lock");
        let counter = TiktokenTokenCounter::default();
        for (request, output_tokens) in requests.iter() {
            assert!(
                counter
                    .count_messages(request)
                    .saturating_add(*output_tokens)
                    .saturating_add(budget.safety_margin_tokens)
                    <= budget.safe_request_tokens(),
                "every shared-capsule and multipart request must remain under the safe ceiling"
            );
        }

        assert!(
            requests.iter().all(|(request, _)| !request
                .iter()
                .any(|message| message.content.contains("shared-context capsule stage"))),
            "large shared context must use the bounded hierarchical capsule path"
        );
        let capsule_inputs = requests
            .iter()
            .filter(|(request, _)| {
                request
                    .iter()
                    .any(|message| message.content.contains("You are the map stage"))
                    && request.iter().any(|message| {
                        message
                            .content
                            .contains("Preserve this shared context capsule")
                            || message.content.contains("PREVIOUS_SENTINEL_763")
                            || message.content.contains("RUNTIME_CONTEXT_SENTINEL_763")
                            || message.content.contains("CUSTOM_INSTRUCTION_SENTINEL_763")
                            || message.content.contains("PRECOMPACT_SENTINEL_763")
                    })
            })
            .flat_map(|(request, _)| request.iter())
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(capsule_inputs.contains("PREVIOUS_SENTINEL_763"));
        assert!(capsule_inputs.contains("RUNTIME_CONTEXT_SENTINEL_763"));
        assert!(capsule_inputs.contains("CUSTOM_INSTRUCTION_SENTINEL_763"));
        assert!(capsule_inputs.contains("PRECOMPACT_SENTINEL_763"));

        let multipart_requests = requests
            .iter()
            .filter(|(request, _)| {
                request.iter().any(|message| {
                    message
                        .content
                        .contains("instruction-aware multipart final stage")
                })
            })
            .collect::<Vec<_>>();
        assert!(
            !multipart_requests.is_empty(),
            "global target above model output capacity must use multipart final sections"
        );
        for (request, _) in multipart_requests {
            let rendered = request
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(rendered.contains("SHARED_CAPSULE_763"));
            assert!(rendered.contains("PREVIOUS_SENTINEL_763"));
            assert!(rendered.contains("RUNTIME_CONTEXT_SENTINEL_763"));
            assert!(rendered.contains("CUSTOM_INSTRUCTION_SENTINEL_763"));
            assert!(rendered.contains("PRECOMPACT_SENTINEL_763"));
        }
    }

    #[tokio::test]
    async fn multipart_terminal_path_without_shared_context_still_reduces_every_section() {
        let provider = Arc::new(BoundedRequestCaptureProvider::default());
        let budget = bounded_budget(3_000, 240, 100, 700);
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "small-summary-model".to_string(),
            None,
            None,
        )
        .with_request_budget(budget.clone())
        .with_heuristic_fallback_on_error(false);
        let messages = (0..100)
            .map(|index| {
                if index % 2 == 0 {
                    Message::user(format!(
                        "user-{index} {}",
                        "requirement decision path error evidence ".repeat(24)
                    ))
                } else {
                    Message::assistant(
                        format!(
                            "assistant-{index} {}",
                            "implementation command output test result next step ".repeat(24)
                        ),
                        None,
                    )
                }
            })
            .collect::<Vec<_>>();

        let report = summarizer
            .summarize_with_report(&messages)
            .await
            .expect("multipart finalization should reduce every section");
        assert!(report.map_calls > 1);
        assert!(
            report.reduce_calls >= 1,
            "the terminal path must never persist unreduced map/intermediate partials"
        );

        let requests = provider.requests.lock().expect("capture lock");
        assert!(requests.iter().any(|(request, _)| request
            .iter()
            .any(|message| message.content.contains("multipart final stage"))));
        let counter = TiktokenTokenCounter::default();
        for (request, output_tokens) in requests.iter() {
            assert!(
                counter
                    .count_messages(request)
                    .saturating_add(*output_tokens)
                    .saturating_add(budget.safety_margin_tokens)
                    <= budget.safe_request_tokens(),
                "every terminal reduce request must remain under the safe ceiling"
            );
        }
    }

    #[test]
    fn oversized_terminal_partial_is_split_into_bounded_reduce_sections() {
        let budget = bounded_budget(3_000, 800, 100, 1_600);
        let summarizer = LlmSummarizer::new(
            Arc::new(DummyProvider),
            "small-summary-model".to_string(),
            None,
            None,
        )
        .with_request_budget(budget.clone());
        let counter = TiktokenTokenCounter::default();
        let capsule =
            counter.truncate_to_token_prefix(&"shared capsule evidence ".repeat(1_000), 575);
        let part = SummaryPart {
            content: counter
                .truncate_to_token_prefix(&"ordered partial evidence ".repeat(1_000), 800),
            represented_source_tokens: 4_000,
            first_message_id: "first".to_string(),
            last_message_id: "last".to_string(),
        };
        let requested_output = summarizer.target_for_source(part.represented_source_tokens);
        assert!(!summarizer.request_fits(
            &summarizer.build_multipart_finalize_messages(
                std::slice::from_ref(&part),
                requested_output,
                &capsule,
                true,
            ),
            requested_output,
            &budget,
        ));

        let groups = summarizer
            .pack_multipart_final_groups(std::slice::from_ref(&part), &capsule, true, &budget)
            .expect("every finite terminal partial should split into bounded reduce sections");
        assert!(groups.len() > 1);
        assert_eq!(
            groups
                .iter()
                .flatten()
                .map(|part| part.represented_source_tokens)
                .sum::<u32>(),
            part.represented_source_tokens
        );
        for group in groups {
            let represented = group.iter().fold(0u32, |total, part| {
                total.saturating_add(part.represented_source_tokens)
            });
            let requested_output = summarizer.target_for_source(represented);
            assert!(summarizer.request_fits(
                &summarizer.build_multipart_finalize_messages(
                    &group,
                    requested_output,
                    &capsule,
                    true,
                ),
                requested_output,
                &budget,
            ));
        }
    }

    #[test]
    fn one_hundred_thousand_raw_tokens_receive_twenty_thousand_token_target() {
        let summarizer = LlmSummarizer::new(
            Arc::new(DummyProvider),
            "summary-model".to_string(),
            None,
            None,
        )
        .with_request_budget(bounded_budget(128_000, 32_000, 1_000, 20_000));

        assert_eq!(summarizer.target_for_source(100_000), 20_000);
    }

    #[tokio::test]
    async fn bounded_stage_errors_never_fall_back_to_heuristic_by_default() {
        let summarizer = LlmSummarizer::new(
            Arc::new(FailingProvider),
            "summary-model".to_string(),
            None,
            None,
        )
        .with_request_budget(bounded_budget(10_000, 2_000, 100, 400));

        let error = summarizer
            .summarize_with_report(&summary_messages())
            .await
            .expect_err("a failed bounded stage must surface atomically");
        assert!(error.to_string().contains("http 500 transient"));
    }

    #[tokio::test]
    async fn hundreds_of_individually_small_messages_never_use_one_unbounded_request() {
        let provider = Arc::new(BoundedRequestCaptureProvider::default());
        let messages = (0..400)
            .map(|index| {
                if index % 2 == 0 {
                    Message::user(format!(
                        "small-user-{index} {}",
                        "requirement evidence detail ".repeat(4)
                    ))
                } else {
                    Message::assistant(
                        format!(
                            "small-assistant-{index} {}",
                            "result test next-step ".repeat(4)
                        ),
                        None,
                    )
                }
            })
            .collect::<Vec<_>>();
        let counter = TiktokenTokenCounter::default();
        let represented = counter.count_messages(&messages);
        let target = ((represented as f64) * 0.20).ceil() as u32;
        let budget = bounded_budget(3_000, 800, 100, target);
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "small-summary-model".to_string(),
            None,
            None,
        )
        .with_request_budget(budget.clone())
        .with_heuristic_fallback_on_error(false);

        let report = summarizer
            .summarize_with_report(&messages)
            .await
            .expect("hundreds of small messages should chunk");
        assert!(report.map_calls > 1);
        assert_eq!(report.target_summary_tokens, target);
        let requests = provider.requests.lock().expect("capture lock");
        for (request, output) in requests.iter() {
            assert!(
                counter
                    .count_messages(request)
                    .saturating_add(*output)
                    .saturating_add(budget.safety_margin_tokens)
                    <= budget.safe_request_tokens()
            );
        }
        let raw_map_prompts = requests
            .iter()
            .filter(|(request, _)| {
                request
                    .iter()
                    .any(|message| message.content.contains("map stage"))
            })
            .flat_map(|(request, _)| request.iter())
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(raw_map_prompts.contains("small-user-0"));
        assert!(raw_map_prompts.contains("small-assistant-399"));
    }

    #[tokio::test]
    async fn map_chunk_boundaries_do_not_split_generic_multi_tool_chains() {
        let provider = Arc::new(BoundedRequestCaptureProvider::default());
        let budget = bounded_budget(2_000, 300, 100, 300);
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "tiny-summary-model".to_string(),
            None,
            None,
        )
        .with_request_budget(budget)
        .with_heuristic_fallback_on_error(false);
        let mut messages = (0..8)
            .map(|index| {
                Message::user(format!(
                    "prefix-{index} {}",
                    "filler requirement evidence ".repeat(14)
                ))
            })
            .collect::<Vec<_>>();
        let mut chain = Message::assistant("CHAIN_ASSISTANT_763", None);
        chain.tool_calls = Some(vec![
            ToolCall {
                id: "chain-call-a-763".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "search".to_string(),
                    arguments: r#"{"query":"a"}"#.to_string(),
                },
            },
            ToolCall {
                id: "chain-call-b-763".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "read".to_string(),
                    arguments: r#"{"path":"b"}"#.to_string(),
                },
            },
        ]);
        messages.push(chain);
        messages.push(Message::tool_result(
            "chain-call-a-763",
            "CHAIN_RESULT_A_763",
        ));
        messages.push(Message::tool_result(
            "chain-call-b-763",
            "CHAIN_RESULT_B_763",
        ));
        messages.extend((0..8).map(|index| {
            Message::assistant(
                format!(
                    "suffix-{index} {}",
                    "implementation result evidence ".repeat(14)
                ),
                None,
            )
        }));

        let report = summarizer
            .summarize_with_report(&messages)
            .await
            .expect("tool-chain source should chunk");
        assert!(report.map_calls > 1);
        let requests = provider.requests.lock().expect("capture lock");
        let chain_map = requests
            .iter()
            .flat_map(|(request, _)| request.iter())
            .find(|message| message.content.contains("CHAIN_RESULT_A_763"))
            .expect("map request containing tool chain");
        assert!(chain_map.content.contains("CHAIN_ASSISTANT_763"));
        assert!(chain_map.content.contains("CHAIN_RESULT_B_763"));
    }

    #[tokio::test]
    async fn oversized_single_message_is_split_without_dropping_its_tail() {
        let provider = Arc::new(BoundedRequestCaptureProvider::default());
        let budget = bounded_budget(2_000, 300, 100, 300);
        let summarizer = LlmSummarizer::new(
            provider.clone(),
            "tiny-summary-model".to_string(),
            None,
            None,
        )
        .with_request_budget(budget)
        .with_heuristic_fallback_on_error(false);
        let messages = vec![Message::user(format!(
            "{} TAIL_SENTINEL_763",
            "very large source message with concrete content ".repeat(2_000)
        ))];

        let report = summarizer
            .summarize_with_report(&messages)
            .await
            .expect("oversized source should split");
        assert!(report.map_calls > 1);
        let requests = provider.requests.lock().expect("capture lock");
        assert!(
            requests.iter().any(|(request, _)| request
                .iter()
                .any(|message| message.content.contains("TAIL_SENTINEL_763"))),
            "the deterministic continuation chunks must include the original tail"
        );
    }

    struct PartialWithoutDoneProvider;

    #[async_trait]
    impl LLMProvider for PartialWithoutDoneProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_domain::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok::<LLMChunk, LLMError>(
                LLMChunk::Token("partial but incomplete summary".to_string()),
            )])))
        }
    }

    #[tokio::test]
    async fn partial_stream_without_done_is_never_accepted_as_summary() {
        let summarizer = LlmSummarizer::new(
            Arc::new(PartialWithoutDoneProvider),
            "model".to_string(),
            None,
            None,
        )
        .with_request_budget(bounded_budget(10_000, 2_000, 100, 400))
        .with_heuristic_fallback_on_error(false);
        let error = summarizer
            .summarize_with_report(&summary_messages())
            .await
            .expect_err("partial stream must fail");
        assert!(error.to_string().contains("without terminal completion"));
    }
}
