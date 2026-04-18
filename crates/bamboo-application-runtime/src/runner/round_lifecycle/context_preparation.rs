use bamboo_application_memory::budget::{
    apply_compression_plan, build_forced_compression_plan_with_summary,
    estimate_context_compression_exposure, normalized_trigger_percent, prepare_hybrid_context,
    summary_source_messages, HeuristicTokenCounter, LlmSummarizer, PreparedContext, Summarizer,
    TokenBudget,
};
use bamboo_application_agent::tools::ToolSchema;
use bamboo_application_agent::{AgentError, AgentEvent, Role, Session};
use bamboo_infrastructure_llm::LLMProvider;
use crate::config::AgentLoopConfig;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::super::prompt_context::{
    strip_existing_skill_context, strip_existing_tool_guide_context,
};

mod logging;
mod ocr_cache;
mod transforms;

const FORCE_CONTEXT_COMPRESSION_PERCENT: f64 = 98.0;

pub(super) struct PreparedRoundContext {
    pub prepared_context: PreparedContext,
    pub budget: TokenBudget,
}

async fn emit_context_compression_status(
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    phase_label: &str,
    status: &str,
) {
    let Some(tx) = event_tx else {
        return;
    };
    let _ = tx
        .send(AgentEvent::ContextCompressionStatus {
            phase: phase_label.to_string(),
            status: status.to_string(),
        })
        .await;
}

fn degrade_prompt_context_sections_for_overflow(session: &mut Session) -> Option<&'static str> {
    let system_message = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))?;

    let without_tool_guide = strip_existing_tool_guide_context(&system_message.content);
    if without_tool_guide != system_message.content {
        system_message.content = without_tool_guide;
        return Some("tool_guide_context");
    }

    let without_skill = strip_existing_skill_context(&system_message.content);
    if without_skill != system_message.content {
        system_message.content = without_skill;
        return Some("skill_context");
    }

    None
}

async fn maybe_apply_host_context_compression_with_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
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

    let Some(summary_model) = config
        .background_model_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        tracing::warn!(
            "[{}] {} context compression skipped: no background/fast summarization model configured",
            session_id,
            phase_label,
        );
        emit_context_compression_status(event_tx, phase_label, "skipped_no_background_model").await;
        return Ok(false);
    };
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
    emit_context_compression_status(event_tx, phase_label, "started").await;
    let summary = match summarizer.summarize(&messages).await {
        Ok(summary) => summary,
        Err(error) => {
            emit_context_compression_status(event_tx, phase_label, "failed").await;
            return Err(AgentError::Budget(error.to_string()));
        }
    };

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
            emit_context_compression_status(event_tx, phase_label, "failed").await;
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
        emit_context_compression_status(event_tx, phase_label, "skipped").await;
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
    emit_context_compression_status(event_tx, phase_label, "completed").await;
    Ok(true)
}

pub(super) async fn maybe_apply_host_context_compression(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    _tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
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
        event_tx,
        phase_label,
    )
    .await
}

pub(crate) async fn force_overflow_context_recovery(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    llm: &Arc<dyn LLMProvider>,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
) -> Result<bool, AgentError> {
    if let Some(degraded_section) = degrade_prompt_context_sections_for_overflow(session) {
        tracing::info!(
            "[{}] Overflow recovery pre-pass degraded prompt section: {}",
            session_id,
            degraded_section,
        );
        emit_context_compression_status(event_tx, "overflow-recovery", "degraded_sections").await;
        return Ok(true);
    }

    let budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;
    maybe_apply_host_context_compression_with_budget(
        session,
        config,
        model_name,
        session_id,
        llm,
        &budget,
        event_tx,
        "overflow-recovery",
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
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
) -> Result<PreparedRoundContext, AgentError> {
    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;

    let budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;

    let counter = HeuristicTokenCounter::default();

    if maybe_apply_host_context_compression_with_budget(
        session, config, model_name, session_id, llm, &budget, event_tx, "pre-turn",
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
