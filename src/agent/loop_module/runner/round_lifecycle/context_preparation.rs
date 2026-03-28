use crate::agent::core::budget::{
    apply_compression_plan, build_forced_compression_plan_with_summary,
    estimate_context_compression_exposure, prepare_hybrid_context, summary_source_messages,
    HeuristicTokenCounter, LlmSummarizer, PreparedContext, Summarizer, TokenBudget,
};
use crate::agent::core::tools::ToolSchema;
use crate::agent::core::{AgentError, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::config::AgentLoopConfig;
use std::sync::Arc;

mod logging;
mod ocr_cache;
mod transforms;

const FORCE_CONTEXT_COMPRESSION_PERCENT: f64 = 98.0;
const CONTEXT_COMPRESSION_ENABLED_KEY: &str = "context_compression_tool_enabled";
const CONTEXT_COMPRESSION_TRIGGER_PCT_KEY: &str = "context_compression_tool_trigger_pct";
const CONTEXT_COMPRESSION_USAGE_PCT_KEY: &str = "context_compression_tool_usage_pct";

pub(super) struct PreparedRoundContext {
    pub prepared_context: PreparedContext,
    pub budget: TokenBudget,
}

async fn maybe_force_context_compression(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
) -> Result<bool, AgentError> {
    let persisted_usage = session
        .token_usage
        .as_ref()
        .and_then(|usage| {
            (usage.budget_limit > 0)
                .then_some((usage.total_tokens as f64 / usage.budget_limit as f64) * 100.0)
        })
        .unwrap_or(0.0);
    if persisted_usage < FORCE_CONTEXT_COMPRESSION_PERCENT {
        return Ok(false);
    }

    let exposure = estimate_context_compression_exposure(session, model_name, Some(budget));

    let messages = summary_source_messages(session);
    if messages.len() < 3 {
        tracing::warn!(
            "[{}] Context usage {:.1}% >= {}% but forced compression skipped: not enough active messages ({})",
            session_id,
            exposure.active_usage_percent,
            FORCE_CONTEXT_COMPRESSION_PERCENT,
            messages.len()
        );
        return Ok(false);
    }

    let summary_model = config
        .fast_model_name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(model_name);
    let existing_summary = session
        .conversation_summary
        .as_ref()
        .map(|summary| summary.content.clone());
    let task_list_prompt = session
        .task_list
        .as_ref()
        .map(|_| session.format_task_list_for_prompt())
        .filter(|value| !value.trim().is_empty());

    let summarizer = LlmSummarizer::new(
        Arc::clone(llm),
        summary_model.to_string(),
        existing_summary,
        task_list_prompt,
    );
    let summary = summarizer
        .summarize(&messages)
        .await
        .map_err(|error| AgentError::Budget(error.to_string()))?;

    let Some(plan) =
        build_forced_compression_plan_with_summary(session, model_name, Some(budget), summary)
    else {
        tracing::warn!(
            "[{}] Context usage {:.1}% >= {}% but forced compression produced no safe plan",
            session_id,
            exposure.active_usage_percent,
            FORCE_CONTEXT_COMPRESSION_PERCENT
        );
        return Ok(false);
    };

    let compressed_count = apply_compression_plan(session, plan.clone());
    if compressed_count == 0 {
        tracing::warn!(
            "[{}] Context usage {:.1}% >= {}% but forced compression did not archive messages",
            session_id,
            exposure.active_usage_percent,
            FORCE_CONTEXT_COMPRESSION_PERCENT
        );
        return Ok(false);
    }

    session.metadata.insert(
        CONTEXT_COMPRESSION_ENABLED_KEY.to_string(),
        "false".to_string(),
    );
    session.metadata.insert(
        CONTEXT_COMPRESSION_TRIGGER_PCT_KEY.to_string(),
        plan.trigger_percent.to_string(),
    );
    session.metadata.insert(
        CONTEXT_COMPRESSION_USAGE_PCT_KEY.to_string(),
        format!("{:.1}", plan.active_usage_after_percent.clamp(0.0, 100.0)),
    );

    if let Some(storage) = config.storage.as_ref() {
        if let Err(error) = storage.save_session(session).await {
            tracing::warn!(
                "[{}] Failed to persist forced context compression result: {}",
                session_id,
                error
            );
        }
    }

    tracing::info!(
        "[{}] Forced context compression applied at {:.1}% usage (threshold {}%): compressed_messages={}, usage_after={:.1}%",
        session_id,
        exposure.active_usage_percent,
        FORCE_CONTEXT_COMPRESSION_PERCENT,
        compressed_count,
        plan.active_usage_after_percent
    );
    Ok(true)
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

    if maybe_force_context_compression(session, config, model_name, session_id, llm, &budget)
        .await?
    {
        tracing::debug!(
            "[{}] Recomputing prepared context after forced compression fallback",
            session_id
        );
    }

    let mut prepared_context = prepare_hybrid_context(session, &budget, &counter)
        .map_err(|error| AgentError::Budget(error.to_string()))?;

    transforms::apply_message_transforms(config, &mut prepared_context, llm, session_id).await?;
    logging::log_context_truncation(session_id, &prepared_context);

    Ok(PreparedRoundContext {
        prepared_context,
        budget,
    })
}

#[cfg(test)]
mod tests;
