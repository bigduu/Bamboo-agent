use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, AgentEvent, CompressionTriggerType, Role, Session};
use bamboo_compression::{
    apply_compression_plan, build_forced_compression_plan_with_summary,
    estimate_context_compression_exposure, prepare_hybrid_context, summary_source_messages,
    LlmSummarizer, PreparedContext, Summarizer, TiktokenTokenCounter, TokenBudget, TokenCounter,
};
use bamboo_infrastructure::LLMProvider;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use super::super::prompt_context::{
    strip_existing_env_context, strip_existing_external_memory, strip_existing_skill_context,
    strip_existing_task_list, strip_existing_tool_guide_context,
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

fn emit_context_pressure_notification(
    session: &Session,
    _budget: &TokenBudget,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    last_emitted_level: &mut Option<String>,
) {
    let Some(tx) = event_tx else { return };
    let Some(usage) = session.token_usage.as_ref() else {
        return;
    };
    let denominator = if usage.max_context_tokens > 0 {
        usage.max_context_tokens
    } else {
        usage.budget_limit
    };
    if denominator == 0 {
        return;
    }

    let pct = (usage.total_tokens as f64 / denominator as f64) * 100.0;

    let (level, message) = if pct >= 90.0 {
        (
            "critical",
            format!(
                "Context window is critically full (~{pct:.0}%). Auto-compression is imminent. \
                 Consider using compact_context to compress on your terms."
            ),
        )
    } else if pct >= 70.0 {
        (
            "warning",
            format!(
                "Context window filling up (~{pct:.0}%). Consider using compact_context \
                 to compress older conversation history before auto-compression triggers."
            ),
        )
    } else {
        return;
    };

    if last_emitted_level.as_deref() == Some(level) {
        return;
    }
    *last_emitted_level = Some(level.to_string());

    let _ = tx.try_send(AgentEvent::ContextPressureNotification {
        percent: pct,
        level: level.to_string(),
        message,
    });
}

const DEGRADATION_LEVELS: &[(&str, fn(&str) -> String)] = &[
    ("tool_guide", strip_existing_tool_guide_context),
    ("skill_context", strip_existing_skill_context),
    ("external_memory", strip_existing_external_memory),
    ("task_list", strip_existing_task_list),
    ("env_context", strip_existing_env_context),
];

fn degrade_prompt_context_sections_for_overflow(session: &mut Session) -> Option<&'static str> {
    let system_message = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))?;

    for &(label, strip_fn) in DEGRADATION_LEVELS {
        let stripped = strip_fn(&system_message.content);
        if stripped != system_message.content {
            system_message.content = stripped;
            return Some(label);
        }
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
    let trigger_context_tokens = budget.compression_trigger_context_tokens();
    let auto_threshold = if budget.max_context_tokens > 0 {
        (trigger_context_tokens as f64 / budget.max_context_tokens as f64) * 100.0
    } else {
        0.0
    };
    let host_auto_requested = usage_percent >= auto_threshold;
    let critical_fallback_requested = usage_percent >= FORCE_CONTEXT_COMPRESSION_PERCENT;
    let manual_requested = session.force_manual_compression.is_some();
    if !host_auto_requested && !critical_fallback_requested && !manual_requested {
        return Ok(false);
    }

    // Defer auto-triggered compression when active execution tasks are running
    // and context pressure is only moderately above the trigger (within a buffer
    // zone). Critical overflow and manual requests always proceed.
    let deferral_buffer_tokens: u32 = 10_000;
    if host_auto_requested
        && !critical_fallback_requested
        && !manual_requested
        && session
            .task_list
            .as_ref()
            .is_some_and(|tl| tl.has_active_execution_tasks())
    {
        let trigger_tokens = budget.compression_trigger_context_tokens();
        let buffered_trigger = trigger_tokens.saturating_add(deferral_buffer_tokens);
        let active_tokens = exposure.active_tokens;
        if active_tokens <= buffered_trigger {
            tracing::debug!(
                "[{}] {} auto-compression deferred: active execution tasks running, usage within buffer zone ({:.1}% < trigger+buffer)",
                session_id, phase_label, usage_percent
            );
            return Ok(false);
        }
    }

    // For auto-triggered (non-critical, non-manual) compression, try lightweight
    // prompt section degradation first. If a section can be stripped, skip the
    // expensive LLM summarization pass entirely.
    if host_auto_requested && !critical_fallback_requested && !manual_requested {
        if let Some(degraded) = degrade_prompt_context_sections_for_overflow(session) {
            tracing::info!(
                "[{}] {} pre-summarization degradation stripped: {}, skipping LLM summarization",
                session_id,
                phase_label,
                degraded,
            );
            emit_context_compression_status(event_tx, phase_label, "degraded_sections").await;
            return Ok(true);
        }
    }

    // Microcompact-first: estimate how many tokens prompt cache compaction would save.
    // If projected usage drops below the trigger threshold, skip LLM summarization —
    // the cheaper prompt-side compaction in prepare_hybrid_context will handle it.
    if host_auto_requested && !critical_fallback_requested && !manual_requested {
        let counter = TiktokenTokenCounter::default();
        let summary_tokens = session
            .conversation_summary
            .as_ref()
            .map(|s| counter.count_message(&bamboo_agent_core::Message::system(&s.content)))
            .unwrap_or(0);
        let savings = bamboo_compression::estimate_prompt_cache_savings(
            session,
            budget,
            &counter,
            summary_tokens,
        );
        if savings > 0 {
            let projected = exposure.active_tokens.saturating_sub(savings);
            let projected_pct = if budget.max_context_tokens > 0 {
                (projected as f64 / budget.max_context_tokens as f64) * 100.0
            } else {
                0.0
            };
            if projected_pct < auto_threshold {
                tracing::info!(
                    "[{}] {} microcompact-first: skipping LLM summarization, prompt cache saves {} tokens (projected {:.1}% < trigger {:.1}%)",
                    session_id, phase_label, savings, projected_pct, auto_threshold
                );
                return Ok(false);
            }
        }
    }

    let trigger_type = if manual_requested {
        CompressionTriggerType::Manual
    } else if critical_fallback_requested {
        CompressionTriggerType::CriticalOverflow
    } else {
        CompressionTriggerType::Auto
    };

    let start = Instant::now();

    let trigger_type_clone = trigger_type.clone();

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
        .summarization_model_name
        .as_deref()
        .or(config.background_model_name.as_deref())
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

    let base_instructions = session
        .compression_instructions
        .as_deref()
        .or(config.compression_instructions.as_deref())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(String::from);

    // Manual compression instructions from compact_context tool take priority.
    let compression_instructions = session
        .force_manual_compression
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .map(String::from)
        .or(base_instructions);

    let bg_provider = config.background_model_provider.as_ref().unwrap_or(llm);
    let summarizer = LlmSummarizer::new(
        Arc::clone(bg_provider),
        summary_model.to_string(),
        existing_summary.clone(),
        task_list_prompt,
    )
    .with_custom_instructions(compression_instructions)
    .with_summary_mode(if existing_summary.is_some() {
        bamboo_compression::SummaryMode::IncrementalMerge
    } else {
        bamboo_compression::SummaryMode::FullRewrite
    });
    emit_context_compression_status(event_tx, phase_label, "started").await;
    let summary = match summarizer.summarize(&messages).await {
        Ok(summary) => summary,
        Err(error) => {
            emit_context_compression_status(event_tx, phase_label, "failed").await;
            return Err(AgentError::Budget(error.to_string()));
        }
    };

    let mut plan = match build_forced_compression_plan_with_summary(
        session,
        model_name,
        Some(budget),
        summary,
        trigger_type_clone,
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

    let elapsed = start.elapsed();
    let latency_ms = elapsed.as_millis() as u64;
    let compression_ratio = if plan.active_usage_after_percent > 0.0 {
        plan.active_usage_before_percent / plan.active_usage_after_percent
    } else {
        0.0
    };
    plan.compression_ratio = compression_ratio;
    plan.model_used = config.background_model_name.clone();
    plan.latency_ms = latency_ms;

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

    let saved_counter = TiktokenTokenCounter::default();
    let original_tokens = saved_counter.count_messages(&plan.messages_to_summarize);
    let tokens_saved = original_tokens.saturating_sub(plan.summary_tokens);

    if let Some(tx) = event_tx {
        let trigger_label = match trigger_type {
            CompressionTriggerType::Auto => "auto",
            CompressionTriggerType::Manual => "manual",
            CompressionTriggerType::CriticalOverflow => "critical",
        };
        let _ = tx
            .send(AgentEvent::ContextSummarized {
                summary: session
                    .conversation_summary
                    .as_ref()
                    .map(|s| {
                        let end = s
                            .content
                            .char_indices()
                            .nth(200)
                            .map_or(s.content.len(), |(i, _)| i);
                        s.content[..end].to_string()
                    })
                    .unwrap_or_default(),
                messages_summarized: compressed_count,
                tokens_saved,
                usage_before_percent: usage_percent,
                usage_after_percent: plan.active_usage_after_percent,
                trigger_type: trigger_label.to_string(),
            })
            .await;
    }

    // Emit metrics event for observability.
    if let Some(collector) = config.metrics_collector.as_ref() {
        let trigger_label = match trigger_type {
            CompressionTriggerType::Auto => "auto",
            CompressionTriggerType::Manual => "manual",
            CompressionTriggerType::CriticalOverflow => "critical",
        };
        collector.context_compressed(
            session_id,
            compressed_count as u32,
            tokens_saved as u32,
            usage_percent,
            plan.active_usage_after_percent,
            trigger_label,
            latency_ms,
        );
    }

    // Clear manual compression flag after successful compression.
    session.force_manual_compression = None;
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

    let counter = TiktokenTokenCounter::default();

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

    let mut _pressure_level = None;
    emit_context_pressure_notification(session, &budget, event_tx, &mut _pressure_level);

    Ok(PreparedRoundContext {
        prepared_context,
        budget,
    })
}

#[cfg(test)]
mod tests;
