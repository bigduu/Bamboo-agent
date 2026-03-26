use crate::agent::core::agent::types::{CompressionEvent, ConversationSummary};
use crate::agent::core::budget::{
    prepare_hybrid_context, HeuristicTokenCounter, LlmSummarizer, PreparedContext, Summarizer,
    TokenBudget, TokenCounter,
};
use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{AgentError, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;
use chrono::Utc;
use std::collections::HashSet;
use std::sync::Arc;

mod logging;
mod ocr_cache;
mod transforms;

pub(super) struct PreparedRoundContext {
    pub prepared_context: PreparedContext,
    pub budget: TokenBudget,
}

pub(super) async fn prepare_round_context(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
) -> Result<PreparedRoundContext, AgentError> {
    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;

    let mut budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;

    // Reserve budget space for tool schemas (they consume context tokens but
    // are not part of the message list). Without this, context compression
    // underestimates actual token usage, which can cause the LLM to receive
    // more tokens than its context window supports — resulting in empty
    // responses.
    let tool_tokens = super::token_estimation::estimate_tool_schemas_tokens(tool_schemas);
    if tool_tokens > 0 {
        budget.safety_margin = budget.safety_margin.saturating_add(tool_tokens);
        tracing::debug!(
            "[{}] Reserved {} tokens for {} tool schemas (effective safety_margin={})",
            session_id,
            tool_tokens,
            tool_schemas.len(),
            budget.safety_margin
        );
    }

    let counter = HeuristicTokenCounter::default();

    let mut prepared_context = prepare_hybrid_context(session, &budget, &counter)
        .map_err(|error| AgentError::Budget(error.to_string()))?;
    record_compression_event(session, &prepared_context, session_id);

    // Phase 2: After compression removes messages, generate an LLM-based summary
    // of the removed content to preserve context continuity.
    // Use fast_model for summarization when available (lightweight task).
    if !prepared_context.compressed_message_ids.is_empty() {
        let summary_model = config.fast_model_name.as_deref().unwrap_or(model_name);
        maybe_summarize_compressed_messages(session, llm, summary_model, session_id).await;
    }

    transforms::apply_message_transforms(config, &mut prepared_context, llm).await?;
    logging::log_context_truncation(session_id, &prepared_context);

    Ok(PreparedRoundContext {
        prepared_context,
        budget,
    })
}

/// Generate an LLM-based summary of messages that were just compressed.
///
/// This reads the compressed (but still stored) messages from the session,
/// sends them to the LLM for summarization, and stores the result in
/// `session.conversation_summary`. On failure, falls back to heuristic summary.
async fn maybe_summarize_compressed_messages(
    session: &mut Session,
    llm: &Arc<dyn LLMProvider>,
    model_name: &str,
    session_id: &str,
) {
    // Collect all compressed messages (they still exist in session.messages
    // with `compressed = true` but were excluded from the prepared context).
    let compressed_messages: Vec<_> = session
        .messages
        .iter()
        .filter(|m| m.compressed)
        .cloned()
        .collect();

    if compressed_messages.is_empty() {
        return;
    }

    let existing_summary = session
        .conversation_summary
        .as_ref()
        .map(|s| s.content.clone());

    let summarizer = LlmSummarizer::new(Arc::clone(llm), model_name.to_string(), existing_summary);

    tracing::info!(
        "[{}] Generating LLM summary for {} compressed messages",
        session_id,
        compressed_messages.len()
    );

    match summarizer.summarize(&compressed_messages).await {
        Ok(summary_content) => {
            let counter = HeuristicTokenCounter::default();
            let token_count = counter.count_text(&summary_content);

            let now = Utc::now();
            let summary =
                ConversationSummary::new(&summary_content, compressed_messages.len(), token_count);

            tracing::info!(
                "[{}] Conversation summary updated: {} chars, ~{} tokens, {} messages summarized",
                session_id,
                summary_content.len(),
                token_count,
                compressed_messages.len()
            );

            session.conversation_summary = Some(summary);
            session.updated_at = now;
        }
        Err(e) => {
            tracing::warn!(
                "[{}] Failed to generate conversation summary: {}",
                session_id,
                e
            );
        }
    }
}

fn record_compression_event(
    session: &mut Session,
    prepared_context: &PreparedContext,
    session_id: &str,
) {
    if prepared_context.compressed_message_ids.is_empty() {
        return;
    }

    let compressed_ids: HashSet<&str> = prepared_context
        .compressed_message_ids
        .iter()
        .map(String::as_str)
        .collect();

    let mut changed_indexes = Vec::new();
    for (index, message) in session.messages.iter_mut().enumerate() {
        if !compressed_ids.contains(message.id.as_str()) || message.compressed {
            continue;
        }
        message.compressed = true;
        changed_indexes.push(index);
    }

    if changed_indexes.is_empty() {
        return;
    }

    let event = CompressionEvent::new(changed_indexes.len(), prepared_context.segments_removed);
    let event_id = event.id.clone();
    for index in changed_indexes {
        session.messages[index].compressed_by_event_id = Some(event_id.clone());
    }
    session.compression_events.push(event.clone());
    session.updated_at = Utc::now();

    tracing::info!(
        "[{}] Context compression event {} recorded: messages={}, segments_removed={}",
        session_id,
        event.id,
        event.messages_compressed,
        event.segments_removed
    );
}

#[cfg(test)]
mod tests;
