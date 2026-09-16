use crate::llm_summarizer::{LlmSummarizer, SummaryRequestBudget};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::session_setup::prompt_envelope::{
    build_active_workflow_context_block, build_external_memory_context_block,
    build_history_boundary_reservation_context_block, build_plan_mode_context_block,
    build_plan_runtime_context_block, build_project_resources_context_block,
    build_task_list_context_block,
};
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{
    AgentError, AgentEvent, CompressionTriggerType, ContextBlock, Message, MessagePart, Role,
    Session,
};
use bamboo_compression::{
    active_messages_for_budget, apply_compression_plan, apply_retrieval_window_plan_with_trigger,
    build_forced_compression_candidate_plan_with_fixed_tokens,
    build_retrieval_window_candidate_plan_with_token_accounting,
    effective_retrieval_window_target_tokens,
    estimate_context_compression_exposure_with_fixed_tokens,
    estimate_prompt_cache_savings_with_fixed_tokens, finalize_compression_candidate_plan,
    prepare_hybrid_context_with_fixed_tokens, PreparedContext, RetrievalWindowPlanError,
    RetrievalWindowPolicy, RetrievalWindowTokenAccounting, TiktokenTokenCounter, TokenBudget,
    TokenCounter,
};
use bamboo_config::{ContextManagementFallbackStrategy, ContextManagementStrategy};
use bamboo_domain::{
    AgentHookPoint, AgentRuntimeState, HookPayload, ModelContextResetReason,
    RetrievalWindowCheckpointOutcome, TokenUsageBreakdown, MAX_MODEL_CONTEXT_EVENTS,
    MAX_MODEL_CONTEXT_RENDERED_BYTES,
};
use bamboo_llm::LLMProvider;
use std::collections::{BTreeMap, HashSet};
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
const MAX_RETRIEVAL_WINDOW_CHECKPOINT_REBASE_RETRIES: usize = 2;
const LAST_MANUAL_ARCHIVE_CALL_ID_KEY: &str = "context_management.last_manual_archive_call_id";

/// Session-metadata key holding the last emitted context-pressure level, so
/// `ContextPressureNotification` is deduplicated across rounds on a per-level-
/// transition basis (mirrors the prefix-drift `session.metadata` key style).
const LAST_PRESSURE_LEVEL_KEY: &str = "context_pressure_last_level";

fn pending_manual_archive_call_id(session: &Session) -> Option<String> {
    let last_consumed = session
        .metadata
        .get(LAST_MANUAL_ARCHIVE_CALL_ID_KEY)
        .map(String::as_str);
    let mut completed_tool_calls = HashSet::new();

    // A manual request belongs to the current user-anchored turn. Walking only
    // that tail keeps restart recovery bounded by the per-round tool-call cap,
    // even when the authoritative Session contains a very large history.
    for message in session.messages.iter().rev() {
        match message.role {
            Role::Tool => {
                if message.tool_success != Some(false) {
                    if let Some(call_id) = message.tool_call_id.as_deref() {
                        completed_tool_calls.insert(call_id);
                    }
                }
            }
            Role::Assistant => {
                let Some(call) = message.tool_calls.as_deref().and_then(|calls| {
                    calls.iter().rev().find(|call| {
                        bamboo_domain::canonical_tool_name(&call.function.name) == "archive_context"
                            && completed_tool_calls.contains(call.id.as_str())
                    })
                }) else {
                    continue;
                };
                // The newest completed request is the ordering fence. If it is
                // already consumed, every older request in this turn is older
                // than the durable fence and must not be replayed.
                return (last_consumed != Some(call.id.as_str())).then(|| call.id.clone());
            }
            Role::User => break,
            Role::System => {}
        }
    }

    None
}

fn mark_manual_archive_call_consumed(session: &mut Session, call_id: &str) {
    session.metadata.insert(
        LAST_MANUAL_ARCHIVE_CALL_ID_KEY.to_string(),
        call_id.to_string(),
    );
}

async fn checkpoint_manual_archive_noop(
    session: &mut Session,
    config: &AgentLoopConfig,
    call_id: &str,
) -> Result<(), AgentError> {
    let Some(persistence) = config.persistence.as_ref() else {
        return Err(AgentError::Budget(
            "archive_context requires RuntimeSessionPersistence to durably consume a retrieval-window request"
                .to_string(),
        ));
    };
    let mut staged = session.clone();
    mark_manual_archive_call_consumed(&mut staged, call_id);
    persistence
        .checkpoint_runtime_session(&mut staged)
        .await
        .map_err(|error| {
            AgentError::Budget(format!(
                "archive_context no-op checkpoint failed; request remains retryable: {error}"
            ))
        })?;
    *session = staged;
    Ok(())
}

#[derive(Debug)]
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

fn refit_transformed_context(
    session: &Session,
    previous: PreparedContext,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
    additional_fixed_tokens: u32,
) -> Result<PreparedContext, AgentError> {
    // Image fallback and attachment resolution may perform I/O or a paid
    // auxiliary model call. Refit the already-transformed candidate rather than
    // rerunning those transforms on every bounded projection pass.
    let mut refit_session = session.clone();
    // A prepared candidate already contains the synthetic summary carrier. Drop
    // that carrier from the bounded subset and let the fitter re-inject the same
    // durable summary exactly once, preserving its summary-token attribution.
    refit_session.messages = previous
        .messages
        .iter()
        .filter(|message| {
            !(matches!(message.role, Role::System)
                && message
                    .content
                    .contains("<!-- CONVERSATION_SUMMARY_START -->"))
        })
        .cloned()
        .collect();

    let mut refitted = prepare_hybrid_context_with_fixed_tokens(
        &refit_session,
        budget,
        counter,
        additional_fixed_tokens,
    )
    .map_err(|error| AgentError::Budget(error.to_string()))?;
    refitted.truncation_occurred |= previous.truncation_occurred;
    refitted.segments_removed = refitted
        .segments_removed
        .saturating_add(previous.segments_removed);
    refitted.prompt_cached_tool_outputs = refitted
        .prompt_cached_tool_outputs
        .saturating_add(previous.prompt_cached_tool_outputs);
    refitted.prompt_cached_tool_tokens_saved = refitted
        .prompt_cached_tool_tokens_saved
        .saturating_add(previous.prompt_cached_tool_tokens_saved);
    let mut compressed_message_ids = previous.compressed_message_ids;
    for message_id in refitted.compressed_message_ids.drain(..) {
        if !compressed_message_ids.contains(&message_id) {
            compressed_message_ids.push(message_id);
        }
    }
    refitted.compressed_message_ids = compressed_message_ids;
    Ok(refitted)
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

async fn checkpoint_retrieval_overflow_prompt_degradation(
    session: &mut Session,
    config: &AgentLoopConfig,
) -> Result<Vec<&'static str>, AgentError> {
    let mut staged = session.clone();
    let mut degraded_sections = Vec::new();
    while let Some(section) = degrade_prompt_context_sections_for_overflow(&mut staged) {
        degraded_sections.push(section);
    }
    if degraded_sections.is_empty() {
        return Ok(degraded_sections);
    }

    // Prompt degradation rewrites provider-visible history even when forced
    // archival later proves unnecessary. Fence every native replay lane at the
    // same durable checkpoint so a degradation-only recovery cannot reuse a
    // continuation anchored to the pre-degradation prompt.
    staged
        .metadata
        .remove(super::stream_execution::SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
    staged.reset_model_context_epoch(ModelContextResetReason::ExplicitHistoryRewrite);

    let Some(persistence) = config.persistence.as_ref() else {
        return Err(AgentError::Budget(
            "retrieval-window overflow recovery requires RuntimeSessionPersistence to durably checkpoint prompt degradation before archive planning"
                .to_string(),
        ));
    };
    persistence
        .checkpoint_runtime_session(&mut staged)
        .await
        .map_err(|error| {
            AgentError::Budget(format!(
                "retrieval-window prompt degradation checkpoint failed; overflow recovery remains retryable: {error}"
            ))
        })?;
    *session = staged;
    Ok(degraded_sections)
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

fn all_active_prepared_context(
    session: &Session,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
) -> PreparedContext {
    let messages = active_messages_for_budget(session);
    let system_tokens = messages
        .iter()
        .filter(|message| matches!(message.role, Role::System))
        .fold(0u32, |total, message| {
            total.saturating_add(counter.count_message(message))
        });
    let window_tokens = messages
        .iter()
        .filter(|message| !matches!(message.role, Role::System))
        .fold(0u32, |total, message| {
            total.saturating_add(counter.count_message(message))
        });
    PreparedContext {
        messages,
        token_usage: TokenUsageBreakdown {
            system_tokens,
            summary_tokens: 0,
            window_tokens,
            total_tokens: system_tokens.saturating_add(window_tokens),
            budget_limit: budget.max_request_input_tokens(),
        },
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    }
}

fn provider_prepared_message_tokens(
    message: &Message,
    counter: &dyn TokenCounter,
) -> Result<u32, AgentError> {
    let mut tokens = counter.count_message(message);
    if let Some(reasoning) = message.reasoning.as_deref() {
        tokens = tokens.saturating_add(counter.count_text(reasoning));
    }
    if let Some(signature) = message.reasoning_signature.as_deref() {
        tokens = tokens.saturating_add(counter.count_text(signature));
    }
    if let Some(parts) = message.content_parts.as_deref() {
        for part in parts {
            match part {
                MessagePart::Text { text } => {
                    // Some provider adapters prefer content parts over the
                    // legacy text field. Counting both is deliberately
                    // conservative when a caller populated both views.
                    tokens = tokens.saturating_add(counter.count_text(text));
                }
                MessagePart::ImageUrl { .. } => {
                    return Err(AgentError::Budget(format!(
                        "retrieval-window cannot determine the complete provider token cost for active image message {}",
                        message.id
                    )));
                }
            }
        }
    }
    Ok(tokens)
}

fn late_bound_tool_schema_reserve(
    session: &Session,
    tool_schemas: &[ToolSchema],
    projected: &super::stream_execution::ProjectedRequestUsage,
    counter: &dyn TokenCounter,
) -> Result<u32, AgentError> {
    if projected.tool_schema_late_bound_segment_count == 0 {
        return Ok(0);
    }
    let effective = super::stream_execution::effective_tool_schemas(session, tool_schemas);
    let serialized = serde_json::to_string(effective.as_ref()).map_err(|error| {
        AgentError::Budget(format!(
            "retrieval-window could not serialize late-bound effective tool schemas: {error}"
        ))
    })?;
    Ok(counter
        .count_text(&serialized)
        .saturating_add((effective.len() as u32).saturating_mul(4)))
}

struct RetrievalWindowAccountingFrame {
    accounting: RetrievalWindowTokenAccounting,
    active_tokens_without_boundary: u32,
    #[cfg(test)]
    existing_history_boundary_tokens: u32,
    #[cfg(test)]
    fixed_prompt_tokens_before_boundary_reserve: u32,
    #[cfg(test)]
    history_boundary_reserve_tokens: u32,
}

struct RetrievalWindowPreflight {
    active_tokens: u32,
    has_provider_native_image: bool,
}

enum RetrievalWindowPreparationOutcome {
    Archived(PreparedContext),
    NotNeeded,
    Deferred,
}

fn retrieval_trigger_label(trigger_type: &CompressionTriggerType) -> &'static str {
    match trigger_type {
        CompressionTriggerType::Auto => "auto",
        CompressionTriggerType::Manual => "manual",
        CompressionTriggerType::CriticalOverflow => "critical_overflow",
    }
}

#[allow(clippy::too_many_arguments)]
async fn retrieval_window_preflight(
    session: &Session,
    config: &AgentLoopConfig,
    model_name: &str,
    tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
) -> Result<RetrievalWindowPreflight, AgentError> {
    // This projection deliberately performs no image fallback: it is the
    // mutation-free/no-model-call gate used before capability validation. For
    // text-only messages the extra fields below make it conservative; native
    // images remain explicitly unknown and therefore cannot prove that usage is
    // below the trigger.
    let prepared = all_active_prepared_context(session, budget, counter);
    let projected = super::stream_execution::project_request_usage(
        session,
        &prepared,
        config,
        tool_schemas,
        model_name,
        llm,
    )
    .await?;
    let mut extra_tokens = 0u32;
    let mut has_provider_native_image = false;
    for message in &prepared.messages {
        if let Some(reasoning) = message.reasoning.as_deref() {
            extra_tokens = extra_tokens.saturating_add(counter.count_text(reasoning));
        }
        if let Some(signature) = message.reasoning_signature.as_deref() {
            extra_tokens = extra_tokens.saturating_add(counter.count_text(signature));
        }
        if let Some(parts) = message.content_parts.as_deref() {
            for part in parts {
                match part {
                    MessagePart::Text { text } => {
                        extra_tokens = extra_tokens.saturating_add(counter.count_text(text));
                    }
                    MessagePart::ImageUrl { .. } => has_provider_native_image = true,
                }
            }
        }
    }
    let late_bound_tool_tokens =
        late_bound_tool_schema_reserve(session, tool_schemas, &projected, counter)?;
    Ok(RetrievalWindowPreflight {
        active_tokens: projected
            .input_tokens
            .saturating_add(extra_tokens)
            .saturating_add(late_bound_tool_tokens),
        has_provider_native_image,
    })
}

#[allow(clippy::too_many_arguments)]
async fn build_retrieval_window_accounting_frame(
    session: &Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
) -> Result<RetrievalWindowAccountingFrame, AgentError> {
    let active_messages = active_messages_for_budget(session);
    let mut active_ids = HashSet::new();
    for message in &active_messages {
        if message.id.is_empty() || !active_ids.insert(message.id.clone()) {
            return Err(AgentError::Budget(format!(
                "retrieval-window requires unique non-empty active message IDs (invalid ID: {:?})",
                message.id
            )));
        }
    }

    let mut prepared = all_active_prepared_context(session, budget, counter);
    transforms::apply_message_transforms(config, &mut prepared, llm, session_id).await?;

    let mut transformed_by_id = BTreeMap::new();
    for message in &prepared.messages {
        if !active_ids.contains(&message.id) {
            continue;
        }
        if transformed_by_id
            .insert(message.id.clone(), message)
            .is_some()
        {
            return Err(AgentError::Budget(format!(
                "retrieval-window provider preparation duplicated active message ID {}",
                message.id
            )));
        }
    }

    let mut provider_message_tokens = BTreeMap::new();
    let mut provider_message_token_total = 0u32;
    for message in &active_messages {
        let tokens = match transformed_by_id.get(&message.id) {
            Some(prepared_message) => provider_prepared_message_tokens(prepared_message, counter)?,
            // Tool-chain normalization may remove an orphan result. It is not
            // provider-visible in this accounting frame, so its complete
            // provider cost is exactly zero.
            None => 0,
        };
        provider_message_token_total = provider_message_token_total.saturating_add(tokens);
        provider_message_tokens.insert(message.id.clone(), tokens);
    }

    let current_projected = super::stream_execution::project_request_usage(
        session,
        &prepared,
        config,
        tool_schemas,
        model_name,
        llm,
    )
    .await?;

    // A committed retrieval boundary resets both the model-context ledger and
    // provider-native replay lane. Plan against that exact post-reset request;
    // otherwise replayable reasoning/tool-search payloads are misclassified as
    // permanently fixed prompt cost even though the boundary removes them.
    // Keep the second shadow session off the async state machine's stack. A
    // `Session` is intentionally rich, and retaining an inline clone across
    // the projection await can overflow callers with otherwise ordinary test
    // thread stacks.
    let mut post_boundary_session = Box::new(session.clone());
    post_boundary_session.reset_model_context_epoch(ModelContextResetReason::Compression);
    post_boundary_session
        .metadata
        .remove(super::stream_execution::SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
    let post_boundary_projected = super::stream_execution::project_request_usage(
        &post_boundary_session,
        &prepared,
        config,
        tool_schemas,
        model_name,
        llm,
    )
    .await?;
    if post_boundary_projected.ledger_rendered_bytes > MAX_MODEL_CONTEXT_RENDERED_BYTES {
        return Err(AgentError::Budget(format!(
            "retrieval-window post-boundary accounting projection exceeds model-context ledger byte limit: ledger_bytes={}, ledger_byte_limit={MAX_MODEL_CONTEXT_RENDERED_BYTES}",
            post_boundary_projected.ledger_rendered_bytes
        )));
    }

    let locally_counted_prepared_tokens = counter.count_messages(&prepared.messages);
    let current_late_bound_tool_tokens =
        late_bound_tool_schema_reserve(session, tool_schemas, &current_projected, counter)?;
    let post_boundary_late_bound_tool_tokens = late_bound_tool_schema_reserve(
        &post_boundary_session,
        tool_schemas,
        &post_boundary_projected,
        counter,
    )?;
    let current_projected_tokens = current_projected
        .input_tokens
        .saturating_add(current_late_bound_tool_tokens);
    let post_boundary_projected_tokens = post_boundary_projected
        .input_tokens
        .saturating_add(post_boundary_late_bound_tool_tokens);
    let boundary_reclaimable_tokens =
        current_projected_tokens.saturating_sub(post_boundary_projected_tokens);
    let fixed_without_boundary = post_boundary_projected
        .input_tokens
        .saturating_sub(locally_counted_prepared_tokens)
        .saturating_add(post_boundary_late_bound_tool_tokens);
    // The block is persisted through the model-context ledger, whose snapshot
    // envelope is larger than the block's direct runtime-context rendering.
    // Reserve the canonical snapshot with maximum-width numeric fields and the
    // fixed-length deterministic event ID so the first archive cannot add an
    // unaccounted wrapper after planning.
    let boundary_reservation_block = build_history_boundary_reservation_context_block();
    let boundary_snapshot = bamboo_domain::render_model_context_snapshot(
        &format!("ctx_{}", "0".repeat(64)),
        u64::MAX,
        u64::MAX,
        &boundary_reservation_block,
        u64::MAX,
        Some(u64::MAX),
    );
    let boundary_reserve = counter.count_message(&Message::user(boundary_snapshot));
    // The post-reset projection already contains the current HistoryBoundary
    // on every archive after the first. Reserve only enough to grow that
    // replacement snapshot to the maximum-width form; adding the whole block
    // again would double-count it and could archive an extra complete turn.
    let boundary_reserve_increment =
        boundary_reserve.saturating_sub(post_boundary_projected.history_boundary_input_tokens);
    let active_tokens_without_boundary = fixed_without_boundary
        .saturating_add(provider_message_token_total)
        .saturating_add(boundary_reclaimable_tokens);
    let accounting = RetrievalWindowTokenAccounting {
        fixed_prompt_tokens: fixed_without_boundary.saturating_add(boundary_reserve_increment),
        boundary_reclaimable_tokens,
        provider_message_tokens,
    };

    Ok(RetrievalWindowAccountingFrame {
        accounting,
        active_tokens_without_boundary,
        #[cfg(test)]
        existing_history_boundary_tokens: post_boundary_projected.history_boundary_input_tokens,
        #[cfg(test)]
        fixed_prompt_tokens_before_boundary_reserve: fixed_without_boundary,
        #[cfg(test)]
        history_boundary_reserve_tokens: boundary_reserve,
    })
}

fn retrieval_window_trigger_tokens(config: &AgentLoopConfig, budget: &TokenBudget) -> u32 {
    let configured_trigger = (f64::from(budget.max_context_tokens)
        * config
            .context_management
            .retrieval_window
            .trigger_usage_ratio)
        .floor() as u32;
    let capped_trigger = configured_trigger
        .min(budget.max_request_input_tokens())
        .max(1);
    let target_tokens = effective_retrieval_window_target_tokens(
        budget,
        config.context_management.retrieval_target_usage_percent(),
    );

    // A provider request-input cap or integer rounding can collapse two valid
    // configured ratios onto the same token count. Route the first archive at
    // least one token above the planner target so an exactly-at-limit request
    // remains dispatchable instead of failing with TargetAlreadySatisfied.
    capped_trigger.max(target_tokens.saturating_add(1))
}

fn retrieval_window_fallback_is_summary(config: &AgentLoopConfig) -> bool {
    config.context_management.retrieval_window.fallback_strategy
        == ContextManagementFallbackStrategy::Summary
}

#[allow(clippy::too_many_arguments)]
async fn maybe_prepare_retrieval_window_context(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    trigger_type: CompressionTriggerType,
    manual_archive_call_id: Option<&str>,
) -> Result<RetrievalWindowPreparationOutcome, AgentError> {
    // A retrieval boundary cannot be layered on top of summary-owned history.
    // This is a permanent precondition failure for this specific manual call,
    // even when summary fallback is explicitly allowed for other routes. Consume
    // it durably so restart cannot trap the Session in the same rejected call.
    // Checkpoint failures still return before publication and remain retryable.
    if let Some(call_id) = manual_archive_call_id.filter(|_| session.conversation_summary.is_some())
    {
        checkpoint_manual_archive_noop(session, config, call_id).await?;
        return Err(AgentError::Budget(
            "archive_context cannot create a retrieval-window boundary for a Session that already has a conversation summary; the rejected request was durably consumed"
                .to_string(),
        ));
    }

    // An explicit workflow selection requires the model's first step to be a
    // lone `load_skill` call. During that setup round the effective callable
    // catalog intentionally excludes `session_history_current`; defer archival
    // until activation completes instead of misclassifying the temporary tool
    // restriction as a missing retrieval capability.
    if crate::runtime::runner::session_setup::skill_context::explicit_activation_pending(session) {
        tracing::debug!(
            session_id = %session_id,
            "retrieval-window archival deferred until explicit skill activation completes"
        );
        return Ok(RetrievalWindowPreparationOutcome::Deferred);
    }

    let counter = TiktokenTokenCounter::default();
    let trigger_tokens = retrieval_window_trigger_tokens(config, budget);
    let target_tokens = effective_retrieval_window_target_tokens(
        budget,
        config.context_management.retrieval_target_usage_percent(),
    );
    let forced = trigger_type != CompressionTriggerType::Auto;
    let preflight = retrieval_window_preflight(
        session,
        config,
        model_name,
        tool_schemas,
        llm,
        budget,
        &counter,
    )
    .await?;
    let below_boundary = if forced {
        preflight.active_tokens <= target_tokens
    } else {
        preflight.active_tokens < trigger_tokens
    };
    if !preflight.has_provider_native_image && below_boundary {
        return Ok(RetrievalWindowPreparationOutcome::NotNeeded);
    }
    let policy = RetrievalWindowPolicy {
        min_recent_user_turns: config
            .context_management
            .retrieval_window
            .min_recent_user_turns,
        target_usage_percent: config.context_management.retrieval_target_usage_percent(),
    };
    let mut candidate_base = session.clone();

    for attempt in 0..=MAX_RETRIEVAL_WINDOW_CHECKPOINT_REBASE_RETRIES {
        let effective_tool_schemas =
            super::stream_execution::effective_tool_schemas(&candidate_base, tool_schemas);
        let history_tool_available = effective_tool_schemas.iter().any(|schema| {
            bamboo_domain::canonical_tool_name(&schema.function.name) == "session_history_current"
        });
        if !history_tool_available {
            return Err(AgentError::Budget(
                "retrieval-window requires the effective callable tool session_history_current before archiving any messages"
                    .to_string(),
            ));
        }

        // OCR caching is an existing durable Session side effect. Populate it
        // only on the candidate base so a failure leaves live state intact and
        // a successful archive commits it in the same transaction.
        ocr_cache::cache_ocr_results_in_session(&mut candidate_base, config).await;
        let frame = build_retrieval_window_accounting_frame(
            &candidate_base,
            config,
            model_name,
            session_id,
            tool_schemas,
            llm,
            budget,
            &counter,
        )
        .await?;
        let below_boundary = if forced {
            frame.active_tokens_without_boundary <= target_tokens
        } else {
            frame.active_tokens_without_boundary < trigger_tokens
        };
        if below_boundary {
            // A rebase is authoritative durable state. Publish the fully
            // prepared candidate too so OCR caching performed on the retry is
            // not lost when the new suffix removes the need for an archive.
            if attempt > 0 {
                *session = candidate_base;
            }
            return Ok(RetrievalWindowPreparationOutcome::NotNeeded);
        }
        let Some(persistence) = config.persistence.as_ref() else {
            return Err(AgentError::Budget(
                "retrieval-window requires RuntimeSessionPersistence for a durable pre-dispatch checkpoint"
                    .to_string(),
            ));
        };

        // The exact post-OCR base is the optimistic compare value. All archive
        // mutations and provider preparation happen on a separate staged clone.
        let expected_base = candidate_base;
        let mut staged = expected_base.clone();
        let plan = match build_retrieval_window_candidate_plan_with_token_accounting(
            &staged,
            budget,
            policy,
            &frame.accounting,
        ) {
            Ok(plan) => plan,
            Err(error)
                if manual_archive_call_id.is_some()
                    && matches!(
                        &error,
                        RetrievalWindowPlanError::NothingToArchive { .. }
                            | RetrievalWindowPlanError::ProtectedContentExceedsTarget { .. }
                    ) =>
            {
                let call_id = manual_archive_call_id
                    .expect("guarded manual archive call ID must remain available");
                checkpoint_manual_archive_noop(session, config, call_id).await?;
                return Err(AgentError::Budget(format!(
                    "retrieval-window candidate planning permanently rejected archive_context: {error}; the rejected request was durably consumed"
                )));
            }
            Err(error) => {
                return Err(AgentError::Budget(format!(
                    "retrieval-window candidate planning failed: {error}"
                )));
            }
        };

        let applied = apply_retrieval_window_plan_with_trigger(
            &mut staged,
            &plan,
            policy,
            budget,
            &frame.accounting,
            trigger_type.clone(),
        )
        .map_err(|error| {
            AgentError::Budget(format!(
                "retrieval-window staged application failed: {error}"
            ))
        })?;
        if applied.idempotent_replay {
            return Err(AgentError::Budget(
                "retrieval-window automatic route unexpectedly produced an idempotent replay"
                    .to_string(),
            ));
        }
        // A retrieval boundary rewrites the provider-visible transcript. Bind
        // Responses continuation invalidation to the same staged checkpoint.
        staged
            .metadata
            .remove(super::stream_execution::SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
        if let Some(call_id) = manual_archive_call_id {
            mark_manual_archive_call_consumed(&mut staged, call_id);
        }

        let mut retained = prepare_hybrid_context_with_fixed_tokens(
            &staged,
            budget,
            &counter,
            plan.fixed_prompt_tokens,
        )
        .map_err(|error| {
            AgentError::Budget(format!(
                "retrieval-window retained context preparation failed: {error}"
            ))
        })?;
        if retained.truncation_occurred || !retained.compressed_message_ids.is_empty() {
            return Err(AgentError::Budget(format!(
                "retrieval-window retained context required an additional transient hard-limit fit (removed_segments={}, removed_messages={})",
                retained.segments_removed,
                retained.compressed_message_ids.len()
            )));
        }
        transforms::apply_message_transforms(config, &mut retained, llm, session_id).await?;
        let projected = super::stream_execution::project_request_usage(
            &staged,
            &retained,
            config,
            tool_schemas,
            model_name,
            llm,
        )
        .await?;
        let late_bound_tool_tokens =
            late_bound_tool_schema_reserve(&staged, tool_schemas, &projected, &counter)?;
        let retained_supplemental_tokens = retained.messages.iter().try_fold(
            0u32,
            |total, message| -> Result<u32, AgentError> {
                let complete = provider_prepared_message_tokens(message, &counter)?;
                Ok(total.saturating_add(complete.saturating_sub(counter.count_message(message))))
            },
        )?;
        let projected_input_tokens = projected
            .input_tokens
            .saturating_add(late_bound_tool_tokens)
            .saturating_add(retained_supplemental_tokens);
        if projected_input_tokens > plan.target_tokens
            || projected_input_tokens > budget.max_request_input_tokens()
            || projected.ledger_rendered_bytes > MAX_MODEL_CONTEXT_RENDERED_BYTES
        {
            return Err(AgentError::Budget(format!(
                "retrieval-window exact retained request exceeds its committed limits: input_tokens={projected_input_tokens}, target_tokens={}, input_limit={}, late_bound_tool_tokens={late_bound_tool_tokens}, supplemental_message_tokens={retained_supplemental_tokens}, ledger_bytes={}, ledger_byte_limit={MAX_MODEL_CONTEXT_RENDERED_BYTES}",
                plan.target_tokens,
                budget.max_request_input_tokens(),
                projected.ledger_rendered_bytes,
            )));
        }

        match persistence
            .checkpoint_retrieval_window(&expected_base, &mut staged)
            .await
            .map_err(|error| {
                AgentError::Budget(format!(
                    "retrieval-window durable checkpoint failed before provider dispatch: {error}"
                ))
            })? {
            RetrievalWindowCheckpointOutcome::Rebased => {
                // `staged` is no longer the speculative archive candidate here:
                // the persistence boundary replaced it with the latest durable
                // Session. Publish that authority immediately so any later
                // planning error or explicit summary fallback cannot operate on
                // the runner's stale pre-rebase snapshot.
                candidate_base = staged;
                *session = candidate_base.clone();
                if attempt == MAX_RETRIEVAL_WINDOW_CHECKPOINT_REBASE_RETRIES {
                    return Err(AgentError::Budget(format!(
                        "retrieval-window durable checkpoint could not stabilize after {} attempts with concurrent transcript changes",
                        MAX_RETRIEVAL_WINDOW_CHECKPOINT_REBASE_RETRIES + 1
                    )));
                }
                continue;
            }
            RetrievalWindowCheckpointOutcome::Committed => {}
        }

        let (model_context_epoch, reset_reason) = staged
            .model_context_state
            .as_ref()
            .map(|state| {
                (
                    state.prefix_epoch,
                    state
                        .last_reset_reason
                        .map(ModelContextResetReason::as_str)
                        .unwrap_or("unknown")
                        .to_string(),
                )
            })
            .unwrap_or_else(|| (0, "unknown".to_string()));

        // Publish only the exact candidate that persistence committed. Every
        // fallible step above operated on an immutable base or staged clone.
        *session = staged;

        if let Some(tx) = event_tx {
            let _ = tx
                .send(AgentEvent::ContextArchived {
                    archive_event_id: applied.event_id,
                    trigger_type: retrieval_trigger_label(&trigger_type).to_string(),
                    messages_archived: plan.archive_message_count,
                    groups_archived: plan.archive_group_count,
                    user_turns_archived: plan.archive_user_turn_count,
                    active_tokens_before: plan.active_tokens_before,
                    active_tokens_after: plan.projected_active_tokens_after,
                    target_tokens: plan.target_tokens,
                    retained_recent_user_turns: plan.retained_recent_user_turn_count,
                    oldest_retained_message_id: plan.oldest_retained_message_id,
                    oldest_retained_user_message_id: plan.oldest_retained_user_message_id,
                    model_context_epoch,
                    reset_reason,
                })
                .await;
        }

        tracing::info!(
            session_id = %session_id,
            archive_event_id = %session.compression_events.last().map(|event| event.id.as_str()).unwrap_or("unknown"),
            messages_archived = plan.archive_message_count,
            groups_archived = plan.archive_group_count,
            active_tokens_before = plan.active_tokens_before,
            active_tokens_after = plan.projected_active_tokens_after,
            target_tokens = plan.target_tokens,
            checkpoint_attempt = attempt + 1,
            "retrieval-window context boundary durably committed"
        );

        return Ok(RetrievalWindowPreparationOutcome::Archived(retained));
    }

    unreachable!("bounded retrieval-window checkpoint loop must return")
}

#[allow(clippy::too_many_arguments)]
async fn maybe_apply_summary_context_compression_with_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    phase_label: &str,
    forced_trigger_type: Option<CompressionTriggerType>,
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
    let forced_requested = forced_trigger_type.is_some();
    let host_auto_requested = usage_percent >= auto_threshold
        || matches!(
            forced_trigger_type.as_ref(),
            Some(CompressionTriggerType::Auto)
        );
    let critical_fallback_requested = usage_percent >= FORCE_CONTEXT_COMPRESSION_PERCENT
        || matches!(
            forced_trigger_type.as_ref(),
            Some(CompressionTriggerType::CriticalOverflow)
        );
    let manual_requested = session.force_manual_compression.is_some()
        || matches!(
            forced_trigger_type.as_ref(),
            Some(CompressionTriggerType::Manual)
        );
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
        && !forced_requested
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
    if host_auto_requested && !critical_fallback_requested && !manual_requested && !forced_requested
    {
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
    if host_auto_requested && !critical_fallback_requested && !manual_requested && !forced_requested
    {
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

    let trigger_type = if let Some(forced) = forced_trigger_type {
        forced
    } else if manual_requested {
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
    tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    phase_label: &str,
) -> Result<bool, AgentError> {
    let manual_archive_call_id = pending_manual_archive_call_id(session);
    if config.context_management.strategy != ContextManagementStrategy::RetrievalWindow {
        if let Some(call_id) = manual_archive_call_id.as_deref() {
            if config.persistence.is_some() {
                checkpoint_manual_archive_noop(session, config, call_id)
                    .await
                    .map_err(|error| {
                        AgentError::Budget(format!(
                            "archive_context requires context_management.strategy=retrieval_window; failed to persist the rejected request marker: {error}"
                        ))
                    })?;
            } else {
                mark_manual_archive_call_consumed(session, call_id);
            }
            return Err(AgentError::Budget(
                "archive_context requires context_management.strategy=retrieval_window; compact_context remains the manual control for summary strategy"
                    .to_string(),
            ));
        }
    } else {
        if let Some(call_id) = manual_archive_call_id.as_deref() {
            let budget = super::token_budget::resolve_token_budget(
                session,
                config,
                model_name,
                llm.as_ref(),
            )
            .await;
            return match Box::pin(maybe_prepare_retrieval_window_context(
                session,
                config,
                model_name,
                session_id,
                tool_schemas,
                llm,
                &budget,
                event_tx,
                CompressionTriggerType::Manual,
                Some(call_id),
            ))
            .await?
            {
                RetrievalWindowPreparationOutcome::Archived(_) => Ok(true),
                RetrievalWindowPreparationOutcome::NotNeeded => {
                    checkpoint_manual_archive_noop(session, config, call_id).await?;
                    Ok(false)
                }
                RetrievalWindowPreparationOutcome::Deferred => Ok(false),
            };
        }

        if session.force_manual_compression.is_none() {
            // Retrieval-window v1 commits only at the ordinary pre-turn
            // boundary. Explicit summary fallback is not an alternate
            // automatic policy: a mid-turn pressure check must defer until an
            // actual retrieval failure or overflow recovery requests it.
            return Ok(false);
        }
        if !retrieval_window_fallback_is_summary(config) {
            session.force_manual_compression = None;
            return Err(AgentError::Budget(
                "compact_context requests summary compression, which is unsupported while context_management.strategy=retrieval_window; configure fallback_strategy=summary to opt into summary fallback"
                    .to_string(),
            ));
        }

        let budget =
            super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref())
                .await;
        let applied = maybe_apply_summary_context_compression_with_budget(
            session,
            config,
            model_name,
            session_id,
            llm,
            &budget,
            event_tx,
            phase_label,
            None,
        )
        .await?;
        if !applied {
            return Err(AgentError::Budget(
                "explicit summary fallback could not satisfy the compact_context request"
                    .to_string(),
            ));
        }
        return Ok(true);
    }

    let budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;
    maybe_apply_summary_context_compression_with_budget(
        session,
        config,
        model_name,
        session_id,
        llm,
        &budget,
        event_tx,
        phase_label,
        None,
    )
    .await
}

pub(crate) async fn force_overflow_context_recovery(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    session_id: &str,
    tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
) -> Result<bool, AgentError> {
    let degraded_sections =
        if config.context_management.strategy == ContextManagementStrategy::RetrievalWindow {
            checkpoint_retrieval_overflow_prompt_degradation(session, config).await?
        } else {
            degrade_prompt_context_sections_for_overflow(session)
                .into_iter()
                .collect()
        };
    let degraded_prompt = !degraded_sections.is_empty();
    for degraded_section in degraded_sections {
        tracing::info!(
            "[{}] Overflow recovery pre-pass degraded prompt section: {}",
            session_id,
            degraded_section,
        );
        emit_context_compression_status(event_tx, "overflow-recovery", "degraded_sections").await;
    }

    // Summary mode preserves the existing one-section-per-recovery behavior.
    // Retrieval-window recovery checkpointed every bounded degradation above
    // and must now reach forced archive planning during this same invocation.
    if degraded_prompt
        && config.context_management.strategy != ContextManagementStrategy::RetrievalWindow
    {
        return Ok(true);
    }

    if config.context_management.strategy == ContextManagementStrategy::RetrievalWindow {
        let budget =
            super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref())
                .await;
        let retrieval_result = Box::pin(maybe_prepare_retrieval_window_context(
            session,
            config,
            model_name,
            session_id,
            tool_schemas,
            llm,
            &budget,
            event_tx,
            CompressionTriggerType::CriticalOverflow,
            None,
        ))
        .await;

        match retrieval_result {
            Ok(RetrievalWindowPreparationOutcome::Archived(_)) => return Ok(true),
            Ok(RetrievalWindowPreparationOutcome::Deferred) if degraded_prompt => {
                tracing::warn!(
                    session_id = %session_id,
                    "retrieval-window archival was deferred after durable prompt degradation; retrying the provider with the degraded prompt"
                );
                return Ok(true);
            }
            Ok(RetrievalWindowPreparationOutcome::Deferred) => {
                return Err(AgentError::Budget(
                    "retrieval-window critical overflow recovery was deferred by a required setup boundary"
                        .to_string(),
                ));
            }
            Ok(RetrievalWindowPreparationOutcome::NotNeeded) if degraded_prompt => {
                tracing::info!(
                    session_id = %session_id,
                    "retrieval-window critical recovery needed prompt degradation only"
                );
                return Ok(true);
            }
            Ok(RetrievalWindowPreparationOutcome::NotNeeded)
                if !retrieval_window_fallback_is_summary(config) =>
            {
                return Err(AgentError::Budget(
                    "retrieval-window critical overflow recovery could not reduce context because the provider-prepared request is already at or below the configured archive target"
                        .to_string(),
                ));
            }
            Err(error) if degraded_prompt && !retrieval_window_fallback_is_summary(config) => {
                tracing::warn!(
                    session_id = %session_id,
                    error = %error,
                    "retrieval-window archival failed after durable prompt degradation; retrying the provider with the degraded prompt"
                );
                return Ok(true);
            }
            Err(error) if !retrieval_window_fallback_is_summary(config) => return Err(error),
            Ok(RetrievalWindowPreparationOutcome::NotNeeded) => {
                tracing::warn!(
                    session_id = %session_id,
                    "retrieval-window critical recovery found no archive candidate; applying explicitly configured summary fallback"
                );
            }
            Err(error) => {
                tracing::warn!(
                    session_id = %session_id,
                    error = %error,
                    "retrieval-window critical recovery failed; applying explicitly configured summary fallback"
                );
            }
        }
    }

    let budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;
    maybe_apply_summary_context_compression_with_budget(
        session,
        config,
        model_name,
        session_id,
        llm,
        &budget,
        event_tx,
        "overflow-recovery",
        Some(CompressionTriggerType::CriticalOverflow),
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
    let retrieval_window_enabled =
        config.context_management.strategy == ContextManagementStrategy::RetrievalWindow;
    let manual_archive_call_id = pending_manual_archive_call_id(session);
    if !retrieval_window_enabled {
        if let Some(call_id) = manual_archive_call_id.as_deref() {
            if config.persistence.is_some() {
                checkpoint_manual_archive_noop(session, config, call_id).await?;
            } else {
                mark_manual_archive_call_consumed(session, call_id);
            }
            return Err(AgentError::Budget(
                "archive_context requires context_management.strategy=retrieval_window; compact_context remains the manual control for summary strategy"
                    .to_string(),
            ));
        }
    }
    // Let a pending manual call reach the dedicated rejection path so its
    // permanent precondition failure is durably consumed instead of replayed.
    if retrieval_window_enabled
        && session.conversation_summary.is_some()
        && manual_archive_call_id.is_none()
        && !retrieval_window_fallback_is_summary(config)
    {
        return Err(AgentError::Budget(
            "retrieval-window cannot start for a Session that already has a conversation summary; configure fallback_strategy=summary or start a new Session"
                .to_string(),
        ));
    }
    if !retrieval_window_enabled {
        // Preserve the legacy summary/default ordering exactly.
        ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;
    }

    let budget =
        super::token_budget::resolve_token_budget(session, config, model_name, llm.as_ref()).await;

    let counter = TiktokenTokenCounter::default();
    let mut retrieval_prepared = None;

    if retrieval_window_enabled {
        if session.force_manual_compression.is_some() {
            if !retrieval_window_fallback_is_summary(config) {
                session.force_manual_compression = None;
                return Err(AgentError::Budget(
                    "compact_context requests summary compression, which is unsupported while context_management.strategy=retrieval_window; configure fallback_strategy=summary to opt into summary fallback"
                        .to_string(),
                ));
            }
            ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;
            let fallback_applied = maybe_apply_summary_context_compression_with_budget(
                session, config, model_name, session_id, llm, &budget, event_tx, "pre-turn", None,
            )
            .await?;
            if !fallback_applied {
                return Err(AgentError::Budget(
                    "explicit summary fallback could not satisfy the compact_context request"
                        .to_string(),
                ));
            }
            tracing::debug!(
                "[{}] Recomputing prepared context after explicit summary fallback",
                session_id
            );
        } else if let Some(call_id) = manual_archive_call_id.as_deref() {
            match Box::pin(maybe_prepare_retrieval_window_context(
                session,
                config,
                model_name,
                session_id,
                tool_schemas,
                llm,
                &budget,
                event_tx,
                CompressionTriggerType::Manual,
                Some(call_id),
            ))
            .await?
            {
                RetrievalWindowPreparationOutcome::Archived(prepared) => {
                    retrieval_prepared = Some(prepared)
                }
                RetrievalWindowPreparationOutcome::NotNeeded => {
                    checkpoint_manual_archive_noop(session, config, call_id).await?;
                    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;
                }
                RetrievalWindowPreparationOutcome::Deferred => {
                    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;
                }
            }
        } else {
            // Retrieval preparation carries shadow sessions and provider
            // projections across awaits. Heap-box this opt-in branch so its
            // future does not inflate every ordinary agent run's stack frame.
            match Box::pin(maybe_prepare_retrieval_window_context(
                session,
                config,
                model_name,
                session_id,
                tool_schemas,
                llm,
                &budget,
                event_tx,
                CompressionTriggerType::Auto,
                None,
            ))
            .await
            {
                Ok(RetrievalWindowPreparationOutcome::Archived(prepared)) => {
                    retrieval_prepared = Some(prepared)
                }
                Ok(
                    RetrievalWindowPreparationOutcome::NotNeeded
                    | RetrievalWindowPreparationOutcome::Deferred,
                ) => {
                    // OCR caching is an existing durable side effect. Delay it
                    // until retrieval preflight has established there is no
                    // archive transaction, so retrieval failures leave the live
                    // Session byte-identical.
                    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;
                }
                Err(error) if retrieval_window_fallback_is_summary(config) => {
                    tracing::warn!(
                        session_id = %session_id,
                        error = %error,
                        "retrieval-window pre-turn route failed; applying explicitly configured summary fallback"
                    );
                    ocr_cache::maybe_cache_ocr_results(session, config, session_id).await;
                    let fallback_applied = maybe_apply_summary_context_compression_with_budget(
                        session,
                        config,
                        model_name,
                        session_id,
                        llm,
                        &budget,
                        event_tx,
                        "pre-turn-retrieval-fallback",
                        Some(CompressionTriggerType::Auto),
                    )
                    .await?;
                    if !fallback_applied {
                        return Err(AgentError::Budget(format!(
                            "retrieval-window pre-turn route failed ({error}); explicitly configured summary fallback did not apply"
                        )));
                    }
                    tracing::debug!(
                        "[{}] Recomputing prepared context after automatic summary fallback",
                        session_id
                    );
                }
                Err(error) => return Err(error),
            }
        }
    } else if maybe_apply_summary_context_compression_with_budget(
        session, config, model_name, session_id, llm, &budget, event_tx, "pre-turn", None,
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
    let request_input_limit = budget.max_request_input_tokens();
    let mut refit_pass = 0usize;
    let mut prepared_context = if let Some(prepared) = retrieval_prepared {
        prepared
    } else {
        let mut prepared = prepare_hybrid_context_with_fixed_tokens(
            session,
            &budget,
            &counter,
            ledger_usage.tokens,
        )
        .map_err(|error| AgentError::Budget(error.to_string()))?;
        transforms::apply_message_transforms(config, &mut prepared, llm, session_id).await?;
        prepared
    };

    loop {
        // Reconciliation may append snapshots that did not exist when the
        // durable ledger was measured above, especially after retention or
        // hard-truncation starts a fresh epoch. Project the exact final IR on a
        // shadow session and feed any deficit back into message fitting before
        // the provider-bound reconciliation mutates or checkpoints live state.
        let projected = super::stream_execution::project_request_usage(
            session,
            &prepared_context,
            config,
            tool_schemas,
            model_name,
            llm,
        )
        .await?;
        if projected.ledger_rendered_bytes > MAX_MODEL_CONTEXT_RENDERED_BYTES {
            return Err(AgentError::Budget(format!(
                "projected model-context ledger exceeds byte limit: ledger_bytes={}, ledger_byte_limit={MAX_MODEL_CONTEXT_RENDERED_BYTES}",
                projected.ledger_rendered_bytes,
            )));
        }

        // Use the exact final PromptIR deficit, not total ledger size:
        // session-stable workspace/env/instruction/skill content may already be
        // present in the fitted System message and then merely relocated into the
        // ledger by the final envelope. Always close any exact-wire gap here, even
        // when reconciliation itself is unchanged, so execute cannot discover an
        // avoidable terminal Budget error after preparation.
        if projected.input_tokens <= request_input_limit {
            break;
        }
        if refit_pass >= MAX_PROJECTED_REQUEST_REFIT_PASSES {
            return Err(AgentError::Budget(format!(
                "projected known provider-visible request remains over budget after {refit_pass} refit passes: message_input_tokens={}, tool_schema_input_tokens={}, input_tokens={}, input_limit={request_input_limit}, tool_schema_late_bound_segments={}",
                projected.message_input_tokens,
                projected.tool_schema_input_tokens,
                projected.input_tokens,
                projected.tool_schema_late_bound_segment_count,
            )));
        }

        let deficit_tokens = projected.input_tokens.saturating_sub(request_input_limit);
        let prepared_message_tokens = counter.count_messages(&prepared_context.messages);
        // The projection is the transformed messages plus provider-visible
        // material outside that vector. Subtract the messages directly to get
        // the exact fixed reservation. Deriving it via unused input-window space
        // would over-reserve when Vision expansion makes the transformed
        // candidate itself larger than the input limit.
        let refit_fixed_tokens = projected
            .input_tokens
            .saturating_sub(prepared_message_tokens);
        refit_pass += 1;
        tracing::info!(
            session_id = %session.id,
            refit_pass,
            projected_input_tokens = projected.input_tokens,
            message_input_tokens = projected.message_input_tokens,
            tool_schema_input_tokens = projected.tool_schema_input_tokens,
            tool_schema_late_bound_segments = projected.tool_schema_late_bound_segment_count,
            request_input_limit,
            deficit_tokens,
            prepared_message_tokens,
            refit_fixed_tokens,
            "projected model-context snapshots exceeded the exact PromptIR window; refitting transformed transcript"
        );
        prepared_context = refit_transformed_context(
            session,
            prepared_context,
            &budget,
            &counter,
            refit_fixed_tokens,
        )
        .map_err(|error| {
            AgentError::Budget(format!(
                "known provider-visible request cannot be refitted: message_input_tokens={}, tool_schema_input_tokens={}, input_tokens={}, input_limit={request_input_limit}, tool_schema_late_bound_segments={}, cause={error}",
                projected.message_input_tokens,
                projected.tool_schema_input_tokens,
                projected.input_tokens,
                projected.tool_schema_late_bound_segment_count,
            ))
        })?;
    }

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
