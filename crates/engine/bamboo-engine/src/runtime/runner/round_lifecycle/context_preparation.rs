use crate::llm_summarizer::LlmSummarizer;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::session_setup::prompt_envelope::{
    build_active_workflow_context_block, build_external_memory_context_block,
    build_plan_mode_context_block, build_plan_runtime_context_block, build_task_list_context_block,
};
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{
    AgentError, AgentEvent, CompressionTriggerType, ContextBlock, Role, Session,
};
use bamboo_compression::{
    apply_compression_plan, build_forced_compression_plan_with_summary,
    estimate_context_compression_exposure, prepare_hybrid_context, summary_source_messages,
    PreparedContext, Summarizer, TiktokenTokenCounter, TokenBudget, TokenCounter,
};
use bamboo_llm::LLMProvider;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use super::super::prompt_context::{
    strip_existing_env_context, strip_existing_skill_context, strip_existing_tool_guide_context,
};

mod logging;
mod ocr_cache;
mod transforms;

const FORCE_CONTEXT_COMPRESSION_PERCENT: f64 = 98.0;

/// Session-metadata key holding the last emitted context-pressure level, so
/// `ContextPressureNotification` is deduplicated across rounds on a per-level-
/// transition basis (mirrors the prefix-drift `session.metadata` key style).
const LAST_PRESSURE_LEVEL_KEY: &str = "context_pressure_last_level";

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
    session: &mut Session,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
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
    // `usage`'s immutable borrow ends here; the metadata mutations below need it.

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
        // Pressure dropped below the warning threshold: clear the stored level so
        // that re-entering pressure later re-notifies. Dedup is per level
        // transition, not once-forever.
        session.metadata.remove(LAST_PRESSURE_LEVEL_KEY);
        return;
    };

    // Dedup across rounds via session.metadata: skip if the current level matches
    // the last one we emitted for this session.
    if session
        .metadata
        .get(LAST_PRESSURE_LEVEL_KEY)
        .map(String::as_str)
        == Some(level)
    {
        return;
    }
    session
        .metadata
        .insert(LAST_PRESSURE_LEVEL_KEY.to_string(), level.to_string());

    let _ = tx.try_send(AgentEvent::ContextPressureNotification {
        percent: pct,
        level: level.to_string(),
        message,
    });
}

type DegradationStripFn = fn(&str) -> String;
type DegradationLevel = (&'static str, DegradationStripFn);

// External memory and task list no longer live in the system message (they ride
// volatile blocks built from session state/field), so they are not strippable
// here — overflow sheds them via conversation/tail compression instead.
const DEGRADATION_LEVELS: &[DegradationLevel] = &[
    ("tool_guide", strip_existing_tool_guide_context),
    ("skill_context", strip_existing_skill_context),
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

fn build_compression_context_blocks(
    session: &Session,
    app_data_dir: Option<&std::path::Path>,
) -> Vec<ContextBlock> {
    let mut blocks = Vec::new();
    if let Some(block) = build_active_workflow_context_block(session) {
        blocks.push(block);
    }
    if let Some(block) = build_task_list_context_block(session) {
        blocks.push(block);
    }
    if let Some(block) = build_external_memory_context_block(session) {
        blocks.push(block);
    }
    // Plan blocks come straight from session state, not reparsed markers.
    if let Some(block) = build_plan_runtime_context_block(session, app_data_dir) {
        blocks.push(block);
    }
    if let Some(block) = build_plan_mode_context_block(session) {
        blocks.push(block);
    }
    blocks
}

#[allow(clippy::too_many_arguments)]
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
            "[{}] {} context compression skipped: no summarization/background model configured",
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
    let compression_context_blocks =
        build_compression_context_blocks(session, config.app_data_dir.as_deref());

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

    let summary_provider = config
        .summarization_model_provider
        .as_ref()
        .or(config.background_model_provider.as_ref())
        .unwrap_or(llm);
    // Mid-turn compression is BEST-EFFORT (it runs between a turn's tool calls,
    // after the assistant message is already committed). A transient
    // summarization failure there must NOT be papered over with a low-quality
    // heuristic summary that mutates in-flight context; it should SURFACE so the
    // mid-turn call site can skip compression and continue with the uncompressed
    // context (see `maybe_apply_mid_turn_context_compression_after_tool`). Every
    // other phase (pre-turn, overflow-recovery) keeps the heuristic fallback so a
    // round that genuinely needs the context reduced stays resilient. (issue #238)
    let heuristic_fallback_on_error = phase_label != "mid-turn";
    let summarizer = LlmSummarizer::new(
        Arc::clone(summary_provider),
        summary_model.to_string(),
        existing_summary.clone(),
        None,
    )
    .with_heuristic_fallback_on_error(heuristic_fallback_on_error)
    .with_context_blocks(compression_context_blocks)
    .with_custom_instructions(compression_instructions)
    .with_summary_mode(if existing_summary.is_some() {
        crate::llm_summarizer::SummaryMode::IncrementalMerge
    } else {
        crate::llm_summarizer::SummaryMode::FullRewrite
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
    plan.model_used = Some(summary_model.to_string());
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

    if let Some(persistence) = config.persistence.as_ref() {
        if let Err(error) = persistence.save_runtime_session(session).await {
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
            session
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.round.last_round_id.clone())
                .unwrap_or_else(|| session_id.to_string()),
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

#[allow(clippy::too_many_arguments)]
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

    // Dedup state for pressure notifications lives in session.metadata so it
    // persists across rounds (see LAST_PRESSURE_LEVEL_KEY).
    emit_context_pressure_notification(session, event_tx);

    Ok(PreparedRoundContext {
        prepared_context,
        budget,
    })
}

#[cfg(test)]
mod tests;
