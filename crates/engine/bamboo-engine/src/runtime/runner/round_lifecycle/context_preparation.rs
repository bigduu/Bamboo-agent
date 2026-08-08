use crate::llm_summarizer::{LlmSummarizer, SummaryRequestBudget};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::session_setup::prompt_envelope::{
    build_active_workflow_context_block, build_external_memory_context_block,
    build_plan_mode_context_block, build_plan_runtime_context_block,
    build_project_resources_context_block, build_task_list_context_block,
};
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{
    AgentError, AgentEvent, CompressionTriggerType, ContextBlock, Role, Session,
};
use bamboo_compression::{
    apply_compression_plan, build_forced_compression_candidate_plan_with_fixed_tokens,
    estimate_context_compression_exposure_with_fixed_tokens,
    estimate_prompt_cache_savings_with_fixed_tokens, finalize_compression_candidate_plan,
    prepare_hybrid_context_with_fixed_tokens, PreparedContext, TiktokenTokenCounter, TokenBudget,
    TokenCounter,
};
use bamboo_domain::{
    AgentHookPoint, AgentRuntimeState, HookPayload, ModelContextResetReason,
    MAX_MODEL_CONTEXT_EVENTS, MAX_MODEL_CONTEXT_RENDERED_BYTES,
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
const MODEL_CONTEXT_RETENTION_PERCENT: u32 = 25;
const MAX_PROJECTED_REQUEST_REFIT_PASSES: usize = 3;

/// Session-metadata key holding the last emitted context-pressure level, so
/// `ContextPressureNotification` is deduplicated across rounds on a per-level-
/// transition basis (mirrors the prefix-drift `session.metadata` key style).
const LAST_PRESSURE_LEVEL_KEY: &str = "context_pressure_last_level";

pub(super) struct PreparedRoundContext {
    pub prepared_context: PreparedContext,
    pub budget: TokenBudget,
}

#[derive(Debug, Clone, Copy, Default)]
struct ModelContextLedgerUsage {
    tokens: u32,
    rendered_bytes: usize,
}

fn model_context_ledger_usage(
    session: &Session,
    counter: &dyn TokenCounter,
) -> ModelContextLedgerUsage {
    let Some(state) = session.model_context_state.as_ref() else {
        return ModelContextLedgerUsage::default();
    };
    let messages = state
        .events
        .iter()
        .map(bamboo_domain::ModelContextEvent::render_message)
        .collect::<Vec<_>>();
    ModelContextLedgerUsage {
        tokens: counter.count_messages(&messages),
        rendered_bytes: state.events.iter().fold(0usize, |total, event| {
            total.saturating_add(event.rendered_text.len())
        }),
    }
}

/// Bound superseded ledger history before ordinary message fitting. A single
/// current snapshot is treated as fixed authority and either fits (with the
/// conversation trimmed around it) or fails the final request guard; only
/// historical growth is coalesced automatically into a new prefix epoch.
fn enforce_model_context_ledger_retention(
    session: &mut Session,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
) -> ModelContextLedgerUsage {
    let usage = model_context_ledger_usage(session, counter);
    let Some(state) = session.model_context_state.as_ref() else {
        return usage;
    };
    let token_limit = budget
        .max_request_input_tokens()
        .saturating_mul(MODEL_CONTEXT_RETENTION_PERCENT)
        / 100;
    let has_superseded_history = state.events.len() > state.baselines.len();
    let retention_exceeded = state.events.len() > MAX_MODEL_CONTEXT_EVENTS
        || usage.rendered_bytes > MAX_MODEL_CONTEXT_RENDERED_BYTES
        || (has_superseded_history && usage.tokens > token_limit);
    if !retention_exceeded {
        return usage;
    }

    tracing::info!(
        session_id = %session.id,
        ledger_events = state.events.len(),
        ledger_tokens = usage.tokens,
        ledger_token_limit = token_limit,
        ledger_rendered_bytes = usage.rendered_bytes,
        ledger_byte_limit = MAX_MODEL_CONTEXT_RENDERED_BYTES,
        "model-context ledger retention limit reached; starting a coalesced prefix epoch"
    );
    session.reset_model_context_epoch(ModelContextResetReason::RetentionLimit);
    ModelContextLedgerUsage::default()
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
    if let Some(block) = build_project_resources_context_block(session) {
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

fn merge_compression_instructions(
    base: Option<String>,
    hook_contexts: Vec<String>,
) -> Option<String> {
    let hook_contexts = hook_contexts
        .into_iter()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>();
    if hook_contexts.is_empty() {
        return base;
    }

    let hook_section = format!(
        "## PreCompact Hook Instructions\n\n{}",
        hook_contexts.join("\n\n---\n\n")
    );
    Some(match base {
        Some(base) if !base.trim().is_empty() => format!("{}\n\n{}", base.trim(), hook_section),
        _ => hook_section,
    })
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
    let counter = TiktokenTokenCounter::default();
    let ledger_usage = enforce_model_context_ledger_retention(session, budget, &counter);
    let exposure = estimate_context_compression_exposure_with_fixed_tokens(
        session,
        model_name,
        Some(budget),
        ledger_usage.tokens,
    );
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
        let summary_tokens = session
            .conversation_summary
            .as_ref()
            .map(|s| counter.count_message(&bamboo_agent_core::Message::system(&s.content)))
            .unwrap_or(0);
        let savings = estimate_prompt_cache_savings_with_fixed_tokens(
            session,
            budget,
            &counter,
            summary_tokens,
            ledger_usage.tokens,
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

    let trigger_type_clone = trigger_type.clone();
    let logical_pass_id = uuid::Uuid::new_v4().to_string();

    let active_non_system_count = session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .filter(|message| !matches!(message.role, Role::System))
        .count();
    if active_non_system_count < 3 {
        tracing::warn!(
            "[{}] {} context compression skipped: usage={:.1}%, auto_threshold={:.1}%, critical_threshold={}%, not enough active messages ({})",
            session_id,
            phase_label,
            usage_percent,
            auto_threshold,
            FORCE_CONTEXT_COMPRESSION_PERCENT,
            active_non_system_count
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

    let compression_context_blocks =
        build_compression_context_blocks(session, config.app_data_dir.as_deref());
    let additional_fixed_tokens = compression_context_blocks
        .iter()
        .fold(0u32, |total, block| {
            total.saturating_add(counter.count_message(&block.render_runtime_context_message()))
        });
    let candidate = match build_forced_compression_candidate_plan_with_fixed_tokens(
        session,
        model_name,
        Some(budget),
        config.summary_target_ratio,
        trigger_type_clone,
        additional_fixed_tokens,
    ) {
        Ok(candidate) => candidate,
        Err(reason) => {
            tracing::warn!(
                "[{}] {} context compression pass {} candidate planning failed before summarization: {}",
                session_id,
                phase_label,
                logical_pass_id,
                reason
            );
            let status = format!("failed_candidate_plan:{reason}");
            emit_context_compression_status(event_tx, phase_label, &status).await;
            return Ok(false);
        }
    };
    let messages = candidate.messages_to_summarize.clone();

    let mut hook_compression_instructions = Vec::new();
    if config
        .hook_runner
        .has_hooks_for(AgentHookPoint::BeforeCompression)
    {
        let trigger = match &trigger_type {
            CompressionTriggerType::Manual => "manual",
            CompressionTriggerType::CriticalOverflow => "forced_overflow_recovery",
            CompressionTriggerType::Auto => "threshold",
        };
        let payload = HookPayload::Compression {
            estimated_tokens: exposure.active_tokens,
            usage_percent,
            max_context_tokens: budget.max_context_tokens,
            trigger_context_tokens,
            trigger: trigger.to_string(),
            phase: phase_label.to_string(),
        };
        let mut hook_runtime_state = session
            .agent_runtime_state
            .clone()
            .unwrap_or_else(|| AgentRuntimeState::new(session_id));
        let hook_outcome = config
            .hook_runner
            .run_observer_hooks(
                AgentHookPoint::BeforeCompression,
                &payload,
                session,
                &mut hook_runtime_state,
                event_tx,
            )
            .await;
        hook_compression_instructions = hook_outcome.injected_contexts;
        session.agent_runtime_state = Some(hook_runtime_state);
    }

    let start = Instant::now();

    let existing_summary = session
        .conversation_summary
        .as_ref()
        .map(|summary| summary.content.clone());
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
    let compression_instructions =
        merge_compression_instructions(compression_instructions, hook_compression_instructions);

    let summary_provider = config
        .summarization_model_provider
        .as_ref()
        .or(config.background_model_provider.as_ref())
        .unwrap_or(llm);
    let summary_model_budget = super::token_budget::resolve_auxiliary_token_budget(
        config,
        summary_model,
        summary_provider.as_ref(),
    )
    .await;
    let summary_request_budget = SummaryRequestBudget::from_token_budget(
        &summary_model_budget,
        config.summary_safe_window_percent,
        candidate.target_summary_tokens,
        candidate.summary_target_ratio,
    );
    // A bounded pass is a single archive transaction even when it uses several
    // provider requests. Any failed map/reduce stage must surface without
    // substituting a heuristic summary, otherwise the caller could archive a
    // candidate set after only part of the source was successfully processed.
    // The mid-turn caller still swallows this error as best-effort (#238);
    // pre-turn/overflow callers retain the unchanged session and can retry.
    let summarizer = LlmSummarizer::new(
        Arc::clone(summary_provider),
        summary_model.to_string(),
        existing_summary.clone(),
        None,
    )
    .with_heuristic_fallback_on_error(false)
    .with_context_blocks(compression_context_blocks)
    .with_custom_instructions(compression_instructions)
    .with_summary_mode(if existing_summary.is_some() {
        crate::llm_summarizer::SummaryMode::IncrementalMerge
    } else {
        crate::llm_summarizer::SummaryMode::FullRewrite
    })
    .with_request_budget(summary_request_budget);
    let mut summarizer =
        summarizer.with_logical_pass_context(logical_pass_id.clone(), phase_label.to_string());
    if let Some(tx) = event_tx {
        let progress_tx = tx.clone();
        let progress_phase = phase_label.to_string();
        summarizer = summarizer.with_progress_callback(Arc::new(
            move |progress: &crate::llm_summarizer::SummarizationProgress| {
                let status = format!(
                    "{}:{}/{} input={} output={} safe={} model_limit={}",
                    progress.stage,
                    progress.stage_index,
                    progress.stage_count,
                    progress.estimated_input_tokens,
                    progress.requested_output_tokens,
                    progress.safe_request_tokens,
                    progress.model_context_tokens,
                );
                let _ = progress_tx.try_send(AgentEvent::ContextCompressionStatus {
                    phase: progress_phase.clone(),
                    status,
                });
            },
        ));
    }
    emit_context_compression_status(event_tx, phase_label, "started").await;
    let summary_report = match summarizer.summarize_with_report(&messages).await {
        Ok(report) => report,
        Err(error) => {
            tracing::warn!(
                logical_pass_id = %logical_pass_id,
                phase = phase_label,
                error = %error,
                "Bounded context compression failed before commit"
            );
            let status = format!("failed:{error}");
            emit_context_compression_status(event_tx, phase_label, &status).await;
            return Err(AgentError::Budget(error.to_string()));
        }
    };

    let mut plan =
        match finalize_compression_candidate_plan(session, candidate, summary_report.content) {
            Ok(plan) => plan,
            Err(reason) => {
                tracing::warn!(
                "[{}] {} context compression attempted (usage={:.1}%) but plan build failed: {}",
                session_id,
                phase_label,
                usage_percent,
                reason
            );
                let status = format!("failed_postcondition:{reason}");
                emit_context_compression_status(event_tx, phase_label, &status).await;
                return Ok(false);
            }
        };
    plan.summary_budget_clamped = summary_report.budget_clamped;
    plan.summary_budget_clamp_reason = summary_report.budget_clamp_reason;
    plan.summarization_map_calls = summary_report.map_calls;
    plan.summarization_reduce_calls = summary_report.reduce_calls;
    plan.summarization_fallback_used = summary_report.fallback_used;
    plan.logical_pass_id = Some(logical_pass_id);

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
    tool_schemas: &[ToolSchema],
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

    // Compression may have reset/coalesced the ledger, so measure it again at
    // the exact fitting boundary. Historical context is a fixed part of the
    // provider-visible transcript and must reduce the ordinary message window.
    let ledger_usage = enforce_model_context_ledger_retention(session, &budget, &counter);
    let mut additional_fixed_tokens = ledger_usage.tokens;
    let mut refit_pass = 0usize;
    let prepared_context = loop {
        let mut candidate = prepare_hybrid_context_with_fixed_tokens(
            session,
            &budget,
            &counter,
            additional_fixed_tokens,
        )
        .map_err(|error| AgentError::Budget(error.to_string()))?;
        transforms::apply_message_transforms(config, &mut candidate, llm, session_id).await?;

        // Reconciliation may append snapshots that did not exist when the
        // durable ledger was measured above, especially after retention or
        // hard-truncation starts a fresh epoch. Project the exact final IR on a
        // shadow session and feed any deficit back into message fitting before
        // the provider-bound reconciliation mutates or checkpoints live state.
        let projected = super::stream_execution::project_request_usage(
            session,
            &candidate,
            config,
            tool_schemas,
            model_name,
        );
        if projected.ledger_rendered_bytes > MAX_MODEL_CONTEXT_RENDERED_BYTES {
            return Err(AgentError::Budget(format!(
                "projected model-context ledger exceeds byte limit: ledger_bytes={}, ledger_byte_limit={MAX_MODEL_CONTEXT_RENDERED_BYTES}",
                projected.ledger_rendered_bytes,
            )));
        }

        // The ordinary fitter already owns system/conversation budgeting. Feed
        // back only ledger tokens created by the shadow reconciliation; using
        // the full PromptIR delta here would double-count stable prompt material
        // that has its own existing budget path.
        if projected.ledger_tokens <= additional_fixed_tokens {
            break candidate;
        }
        if refit_pass >= MAX_PROJECTED_REQUEST_REFIT_PASSES {
            return Err(AgentError::Budget(format!(
                "projected model-context ledger remains under-reserved after {refit_pass} refit passes: ledger_tokens={}, reserved_tokens={additional_fixed_tokens}",
                projected.ledger_tokens,
            )));
        }

        let missing_ledger_tokens = projected
            .ledger_tokens
            .saturating_sub(additional_fixed_tokens);
        additional_fixed_tokens = additional_fixed_tokens.saturating_add(missing_ledger_tokens);
        refit_pass += 1;
        tracing::info!(
            session_id = %session.id,
            refit_pass,
            projected_ledger_tokens = projected.ledger_tokens,
            missing_ledger_tokens,
            additional_fixed_tokens,
            "projected model-context snapshots were not reserved during fitting; refitting transcript"
        );
    };

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
