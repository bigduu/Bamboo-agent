use crate::agent::core::budget::{
    apply_compression_plan, build_forced_compression_plan_with_summary,
    estimate_context_compression_exposure, normalized_trigger_percent, prepare_hybrid_context,
    summary_source_messages, HeuristicTokenCounter, LlmSummarizer, PreparedContext, Summarizer,
    TokenBudget,
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

pub(super) struct PreparedRoundContext {
    pub prepared_context: PreparedContext,
    pub budget: TokenBudget,
}

async fn maybe_apply_host_context_compression_with_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
    phase_label: &str,
) -> Result<bool, AgentError> {
    let exposure = estimate_context_compression_exposure(session, model_name, Some(budget));
    let usage_percent = exposure.active_usage_percent;
    let auto_threshold = normalized_trigger_percent(exposure.budget.compression_trigger_percent);
    let host_auto_requested = usage_percent >= auto_threshold;
    let critical_fallback_requested = usage_percent >= FORCE_CONTEXT_COMPRESSION_PERCENT;
    if !host_auto_requested && !critical_fallback_requested {
        return Ok(false);
    }

    let messages = summary_source_messages(session);
    if messages.len() < 3 {
        tracing::warn!(
            "[{}] {} context compression skipped: usage={:.1}%, auto_threshold={:.1}%, critical_threshold={}%, not enough active messages ({})",
            session_id,
            phase_label,
            usage_percent,
            auto_threshold,
            FORCE_CONTEXT_COMPRESSION_PERCENT,
            messages.len()
        );
        return Ok(false);
    }

    let summary_model = config
        .fast_model_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
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

    let plan = match build_forced_compression_plan_with_summary(
        session,
        model_name,
        Some(budget),
        summary,
    ) {
        Ok(plan) => plan,
        Err(reason) => {
            tracing::warn!(
                "[{}] {} context compression attempted (usage={:.1}%) but plan build failed: {}",
                session_id,
                phase_label,
                usage_percent,
                reason
            );
            return Ok(false);
        }
    };

    let compressed_count = apply_compression_plan(session, plan.clone());
    if compressed_count == 0 {
        tracing::warn!(
            "[{}] {} context compression attempted (usage={:.1}%) but did not archive messages",
            session_id,
            phase_label,
            usage_percent
        );
        return Ok(false);
    }

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
        "[{}] {} context compression applied: usage={:.1}%, auto_threshold={:.1}%, critical_threshold={}%, compressed_messages={}, usage_after_context_window={:.1}%",
        session_id,
        phase_label,
        usage_percent,
        auto_threshold,
        FORCE_CONTEXT_COMPRESSION_PERCENT,
        compressed_count,
        plan.active_usage_after_percent
    );
    Ok(true)
}

pub(super) async fn maybe_apply_host_context_compression(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    _tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    phase_label: &str,
) -> Result<bool, AgentError> {
    let budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;
    maybe_apply_host_context_compression_with_budget(
        session,
        config,
        model_name,
        session_id,
        llm,
        &budget,
        phase_label,
    )
    .await
}

pub(super) async fn prepare_round_context(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    _tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
) -> Result<PreparedRoundContext, AgentError> {
    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;

    let budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;

    let counter = HeuristicTokenCounter::default();

    if maybe_apply_host_context_compression_with_budget(
        session, config, model_name, session_id, llm, &budget, "pre-turn",
    )
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
