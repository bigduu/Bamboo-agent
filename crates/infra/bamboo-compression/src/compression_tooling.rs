use crate::counter::{TiktokenTokenCounter, TokenCounter};
use crate::limits::{create_budget_for_model, ModelLimitsRegistry};
use crate::{BudgetStrategy, TokenBudget};
use bamboo_domain::MessagePhase;
use bamboo_domain::{
    CompressionEvent, CompressionTriggerType, ConversationSummary, Message,
    ModelContextResetReason, Session,
};

/// Checks if a message is part of a skill tool chain (load_skill / read_skill_resource).
fn is_skill_tool_chain_message(message: &Message) -> bool {
    message.tool_calls.as_ref().is_some_and(|calls| {
        calls.iter().any(|call| {
            matches!(
                call.function.name.as_str(),
                "load_skill" | "read_skill_resource"
            )
        })
    })
}

/// The `tool_call_id`s a message participates in: the ids of the tool calls an
/// assistant message initiates, plus the id of the call a `tool` result answers.
/// Two messages belong to the same tool chain iff these overlap.
fn tool_chain_call_ids(message: &Message) -> impl Iterator<Item = String> + '_ {
    message
        .tool_calls
        .iter()
        .flatten()
        .map(|call| call.id.clone())
        .chain(message.tool_call_id.clone())
}

/// Close the compressed (`messages_to_summarize`) set over tool chains: repeatedly
/// move any message still in `messages_to_keep` that shares a `tool_call_id` with
/// an already-compressed message into the summarize set. This keeps an assistant
/// `tool_calls` message and its matching `tool` result(s) on the same side — a
/// split leaves an orphan `tool_result` (or a `tool_use` with no result) in the
/// active set, which providers reject with a 400 that then poisons every
/// subsequent request in the session (#340). Protected messages are left in place
/// (skill chains are already fully protected upstream, so are never partially
/// compressed; protected user messages carry no `tool_call_id`).
fn close_compressed_set_over_tool_chains(
    messages_to_keep: &mut Vec<Message>,
    messages_to_summarize: &mut Vec<Message>,
    protected_user_ids: &HashSet<String>,
    never_compress_ids: &[String],
) {
    loop {
        let compressed_call_ids: HashSet<String> = messages_to_summarize
            .iter()
            .flat_map(tool_chain_call_ids)
            .collect();
        let split_index = messages_to_keep.iter().position(|message| {
            !protected_user_ids.contains(message.id.as_str())
                && !never_compress_ids.contains(&message.id)
                && tool_chain_call_ids(message).any(|id| compressed_call_ids.contains(&id))
        });
        match split_index {
            Some(index) => messages_to_summarize.push(messages_to_keep.remove(index)),
            None => break,
        }
    }
}
use chrono::Utc;
use std::collections::HashSet;

/// Structured reason why a compression plan could not be built.
#[derive(Debug, Clone)]
pub enum CompressionPlanError {
    /// The exposure gate (threshold not reached) prevented building.
    ExposureGateNotMet {
        usage_percent: f64,
        trigger_percent: u8,
    },
    /// No active messages in the session.
    NoActiveMessages,
    /// Not enough non-system messages to compress (need >=3).
    NotEnoughMessages { non_system_count: usize },
    /// Nothing to compress after anchor/keep splitting.
    NothingToCompress {
        anchor_index: usize,
        non_system_count: usize,
    },
    /// Eligible history was exhausted while protected/recent content still kept
    /// the active prompt above the configured post-compression target.
    ProtectedContentExceedsTarget {
        projected_tokens: u32,
        target_tokens: u32,
    },
    /// The session changed after candidate selection and before finalization.
    CandidateSetChanged,
    /// The real generated summary was larger than the source-derived reserve,
    /// so applying the candidate set would miss the post-compression target.
    SummaryExceedsTarget {
        projected_tokens: u32,
        target_tokens: u32,
    },
}

impl std::fmt::Display for CompressionPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExposureGateNotMet {
                usage_percent,
                trigger_percent,
            } => write!(
                f,
                "compression threshold not reached (usage={:.1}%, trigger={}%)",
                usage_percent, trigger_percent
            ),
            Self::NoActiveMessages => write!(f, "no active messages to compress"),
            Self::NotEnoughMessages { non_system_count } => write!(
                f,
                "not enough non-system messages to compress ({}, need >=3)",
                non_system_count
            ),
            Self::NothingToCompress {
                anchor_index,
                non_system_count,
            } => write!(
                f,
                "nothing to compress after anchor/keep splitting (anchor_index={}, non_system={})",
                anchor_index, non_system_count
            ),
            Self::ProtectedContentExceedsTarget {
                projected_tokens,
                target_tokens,
            } => write!(
                f,
                "protected active content prevents compression target (projected={}, target={})",
                projected_tokens, target_tokens
            ),
            Self::CandidateSetChanged => {
                write!(f, "compression candidate set changed before finalization")
            }
            Self::SummaryExceedsTarget {
                projected_tokens,
                target_tokens,
            } => write!(
                f,
                "actual summary misses compression target (projected={}, target={})",
                projected_tokens, target_tokens
            ),
        }
    }
}

/// Metadata about current context pressure, used to decide when compression
/// should be requested by host-side control flow.
#[derive(Debug, Clone)]
pub struct ContextCompressionExposure {
    pub budget: TokenBudget,
    pub active_tokens: u32,
    pub active_usage_percent: f64,
    pub active_usage_percent_rounded: u8,
    pub should_expose_tool: bool,
}

/// A compression plan describing which active historical messages should be
/// archived and summarized.
#[derive(Debug, Clone)]
pub struct CompressionPlan {
    /// Stable identifier for all requests and the persisted event belonging to
    /// one logical compression pass.
    pub logical_pass_id: Option<String>,
    /// Tokens from active prompt blocks rendered outside `Session.messages`.
    pub fixed_prompt_tokens: u32,
    pub compressed_message_ids: Vec<String>,
    pub messages_to_summarize: Vec<Message>,
    pub summary_tokens: u32,
    pub summary_content: String,
    pub active_usage_before_percent: f64,
    pub active_usage_after_percent: f64,
    pub trigger_percent: u8,
    pub target_percent: u8,
    pub segments_removed: usize,
    pub trigger_type: CompressionTriggerType,
    pub compression_ratio: f64,
    pub model_used: Option<String>,
    pub latency_ms: u64,
    pub source_tokens: u32,
    pub represented_source_tokens: u32,
    pub target_summary_tokens: u32,
    pub actual_summary_content_tokens: u32,
    pub summary_target_ratio: f64,
    pub summary_budget_clamped: bool,
    pub summary_budget_clamp_reason: Option<String>,
    pub summarization_map_calls: u32,
    pub summarization_reduce_calls: u32,
    pub summarization_fallback_used: bool,
}

/// Immutable archive selection produced before any summarization request is
/// sent. The final summary must represent exactly this set.
#[derive(Debug, Clone)]
pub struct CompressionCandidatePlan {
    pub compressed_message_ids: Vec<String>,
    pub messages_to_summarize: Vec<Message>,
    pub source_tokens: u32,
    pub previous_represented_source_tokens: u32,
    pub represented_source_tokens: u32,
    pub target_summary_tokens: u32,
    pub summary_target_ratio: f64,
    pub active_usage_before_percent: f64,
    pub projected_usage_after_percent: f64,
    pub trigger_percent: u8,
    pub target_percent: u8,
    pub segments_removed: usize,
    pub trigger_type: CompressionTriggerType,
    additional_fixed_tokens: u32,
    context_window: u32,
    target_limit: u32,
}

pub const DEFAULT_SUMMARY_TARGET_RATIO: f64 = 0.20;

pub fn normalized_summary_target_ratio(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value.clamp(0.01, 0.50)
    } else {
        DEFAULT_SUMMARY_TARGET_RATIO
    }
}

fn target_summary_content_tokens(
    session: &Session,
    counter: &impl TokenCounter,
    newly_represented_tokens: u32,
    target_ratio: f64,
) -> (u32, u32) {
    let target_ratio = normalized_summary_target_ratio(target_ratio);
    match session.conversation_summary.as_ref() {
        Some(summary) if summary.represented_source_tokens > 0 => {
            let represented = summary
                .represented_source_tokens
                .saturating_add(newly_represented_tokens);
            (
                ((represented as f64) * target_ratio).ceil() as u32,
                summary.represented_source_tokens,
            )
        }
        Some(summary) => {
            let existing_tokens = counter.count_text(&summary.content);
            let inferred_previous_source = ((existing_tokens as f64) / target_ratio).ceil() as u32;
            (
                existing_tokens.saturating_add(
                    ((newly_represented_tokens as f64) * target_ratio).ceil() as u32,
                ),
                inferred_previous_source,
            )
        }
        None => (
            ((newly_represented_tokens as f64) * target_ratio).ceil() as u32,
            0,
        ),
    }
}

fn protected_compression_message_ids(non_system: &[Message]) -> HashSet<String> {
    let user_indexes = non_system
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            matches!(message.role, bamboo_domain::Role::User).then_some(index)
        })
        .collect::<Vec<_>>();
    let keep_user_count = user_indexes.len().min(3);
    let mut protected = user_indexes[user_indexes.len().saturating_sub(keep_user_count)..]
        .iter()
        .filter_map(|index| non_system.get(*index))
        .map(|message| message.id.clone())
        .collect::<HashSet<_>>();

    let skill_call_ids = non_system
        .iter()
        .filter(|message| is_skill_tool_chain_message(message))
        .flat_map(|message| {
            message
                .tool_calls
                .iter()
                .flatten()
                .map(|call| call.id.clone())
        })
        .collect::<HashSet<_>>();

    for message in non_system {
        if message.never_compress
            || is_skill_tool_chain_message(message)
            || message
                .tool_call_id
                .as_ref()
                .is_some_and(|id| skill_call_ids.contains(id))
        {
            protected.insert(message.id.clone());
        }
    }
    protected
}

fn post_compaction_recovery_tokens(
    compressed_messages: &[Message],
    session: &Session,
    counter: &impl TokenCounter,
) -> u32 {
    build_post_compaction_recovery_message(compressed_messages, session)
        .as_ref()
        .map(|message| counter.count_message(message))
        .unwrap_or(0)
}

/// Select the exact archive candidates before invoking the summarization model.
///
/// Candidate segments are considered oldest-first. Generic tool chains remain
/// atomic because selection operates on [`crate::MessageSegmenter`] output;
/// newest user turns, `never_compress`, and skill chains remain active.
pub fn build_forced_compression_candidate_plan(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
    summary_target_ratio: f64,
    trigger_type: CompressionTriggerType,
) -> Result<CompressionCandidatePlan, CompressionPlanError> {
    build_forced_compression_candidate_plan_with_fixed_tokens(
        session,
        model_name,
        configured_budget,
        summary_target_ratio,
        trigger_type,
        0,
    )
}

/// Variant of [`build_forced_compression_candidate_plan`] that accounts for
/// fixed prompt blocks rendered outside `Session.messages` (for example task,
/// plan, workflow, project-resource, and external-memory context blocks).
pub fn build_forced_compression_candidate_plan_with_fixed_tokens(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
    summary_target_ratio: f64,
    trigger_type: CompressionTriggerType,
    additional_fixed_tokens: u32,
) -> Result<CompressionCandidatePlan, CompressionPlanError> {
    let exposure = estimate_context_compression_exposure(session, model_name, configured_budget);
    let budget = &exposure.budget;
    let counter = TiktokenTokenCounter::default();
    let active_messages = active_messages_for_budget(session);
    if active_messages.is_empty() {
        return Err(CompressionPlanError::NoActiveMessages);
    }

    let system_messages = active_messages
        .iter()
        .filter(|message| matches!(message.role, bamboo_domain::Role::System))
        .cloned()
        .collect::<Vec<_>>();
    let non_system = active_messages
        .into_iter()
        .filter(|message| !matches!(message.role, bamboo_domain::Role::System))
        .collect::<Vec<_>>();
    if non_system.len() < 3 {
        return Err(CompressionPlanError::NotEnoughMessages {
            non_system_count: non_system.len(),
        });
    }

    let context_window = budget.max_context_tokens;
    let target_limit = budget.compression_target_context_tokens();
    let system_tokens = counter.count_messages(&system_messages);
    let protected_ids = protected_compression_message_ids(&non_system);
    let segments = crate::segmenter::MessageSegmenter::new().segment(non_system.clone());
    let mut remaining_tokens = counter.count_messages(&non_system);
    let mut source_tokens = 0u32;
    let mut selected_messages = Vec::new();
    let mut selected_segment_count = 0usize;
    let ratio = normalized_summary_target_ratio(summary_target_ratio);
    let summary_envelope_tokens = counter.count_messages(&[compression_summary_message("")]);
    let mut projected_tokens = system_tokens
        .saturating_add(remaining_tokens)
        .saturating_add(additional_fixed_tokens);
    let mut target_summary_tokens = 0u32;
    let mut previous_represented_source_tokens = session
        .conversation_summary
        .as_ref()
        .map(|summary| summary.represented_source_tokens)
        .unwrap_or(0);

    for segment in segments {
        if segment
            .messages
            .iter()
            .any(|message| protected_ids.contains(&message.id))
        {
            continue;
        }

        let segment_tokens = counter.count_messages(&segment.messages);
        source_tokens = source_tokens.saturating_add(segment_tokens);
        remaining_tokens = remaining_tokens.saturating_sub(segment_tokens);
        selected_messages.extend(segment.messages);
        selected_segment_count += 1;

        let (desired, previous_represented) =
            target_summary_content_tokens(session, &counter, source_tokens, ratio);
        target_summary_tokens = desired;
        previous_represented_source_tokens = previous_represented;
        let recovery_tokens =
            post_compaction_recovery_tokens(&selected_messages, session, &counter);
        projected_tokens = system_tokens
            .saturating_add(remaining_tokens)
            .saturating_add(additional_fixed_tokens)
            .saturating_add(summary_envelope_tokens)
            .saturating_add(target_summary_tokens)
            .saturating_add(recovery_tokens);
        if projected_tokens <= target_limit {
            break;
        }
    }

    if selected_messages.is_empty() {
        return Err(CompressionPlanError::NothingToCompress {
            anchor_index: 0,
            non_system_count: non_system.len(),
        });
    }
    if projected_tokens > target_limit {
        return Err(CompressionPlanError::ProtectedContentExceedsTarget {
            projected_tokens,
            target_tokens: target_limit,
        });
    }

    let represented_source_tokens =
        previous_represented_source_tokens.saturating_add(source_tokens);
    let projected_usage_after_percent =
        context_window_usage_percent(projected_tokens, context_window);
    let compressed_message_ids = selected_messages
        .iter()
        .map(|message| message.id.clone())
        .collect();

    Ok(CompressionCandidatePlan {
        compressed_message_ids,
        messages_to_summarize: selected_messages,
        source_tokens,
        previous_represented_source_tokens,
        represented_source_tokens,
        target_summary_tokens,
        summary_target_ratio: ratio,
        active_usage_before_percent: exposure.active_usage_percent,
        projected_usage_after_percent,
        trigger_percent: budget.compression_trigger_percent,
        target_percent: budget.compression_target_percent,
        segments_removed: selected_segment_count,
        trigger_type,
        additional_fixed_tokens,
        context_window,
        target_limit,
    })
}

/// Bind a completed summary to a previously selected candidate set and verify
/// the real post-compression prompt before any session mutation occurs.
pub fn finalize_compression_candidate_plan(
    session: &Session,
    candidate: CompressionCandidatePlan,
    summary_content: String,
) -> Result<CompressionPlan, CompressionPlanError> {
    let active_ids = session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .map(|message| message.id.as_str())
        .collect::<HashSet<_>>();
    if candidate
        .compressed_message_ids
        .iter()
        .any(|id| !active_ids.contains(id.as_str()))
    {
        return Err(CompressionPlanError::CandidateSetChanged);
    }

    let candidate_ids = candidate
        .compressed_message_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let counter = TiktokenTokenCounter::default();
    let summary_tokens = counter.count_messages(&[compression_summary_message(&summary_content)]);
    let actual_summary_content_tokens = counter.count_text(&summary_content);
    let recovery_tokens =
        post_compaction_recovery_tokens(&candidate.messages_to_summarize, session, &counter);
    let remaining_tokens = session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .filter(|message| !candidate_ids.contains(message.id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let projected_tokens = counter
        .count_messages(&remaining_tokens)
        .saturating_add(candidate.additional_fixed_tokens)
        .saturating_add(summary_tokens)
        .saturating_add(recovery_tokens);
    if projected_tokens > candidate.target_limit {
        return Err(CompressionPlanError::SummaryExceedsTarget {
            projected_tokens,
            target_tokens: candidate.target_limit,
        });
    }

    Ok(CompressionPlan {
        logical_pass_id: None,
        fixed_prompt_tokens: candidate.additional_fixed_tokens,
        compressed_message_ids: candidate.compressed_message_ids,
        messages_to_summarize: candidate.messages_to_summarize,
        summary_tokens,
        summary_content,
        active_usage_before_percent: candidate.active_usage_before_percent,
        active_usage_after_percent: context_window_usage_percent(
            projected_tokens,
            candidate.context_window,
        ),
        trigger_percent: candidate.trigger_percent,
        target_percent: candidate.target_percent,
        segments_removed: candidate.segments_removed,
        trigger_type: candidate.trigger_type,
        compression_ratio: 0.0,
        model_used: None,
        latency_ms: 0,
        source_tokens: candidate.source_tokens,
        represented_source_tokens: candidate.represented_source_tokens,
        target_summary_tokens: candidate.target_summary_tokens,
        actual_summary_content_tokens,
        summary_target_ratio: candidate.summary_target_ratio,
        summary_budget_clamped: false,
        summary_budget_clamp_reason: None,
        summarization_map_calls: 0,
        summarization_reduce_calls: 0,
        summarization_fallback_used: false,
    })
}

pub fn context_window_usage_percent(total_tokens: u32, context_window_tokens: u32) -> f64 {
    if context_window_tokens == 0 {
        return 0.0;
    }
    (total_tokens as f64 / context_window_tokens as f64) * 100.0
}

pub fn normalized_trigger_percent(trigger_percent: u8) -> f64 {
    match trigger_percent {
        0 => 100.0,
        1..=100 => trigger_percent as f64,
        _ => 100.0,
    }
}

/// Estimate whether context pressure has crossed the configured threshold for
/// compression eligibility.
pub fn estimate_context_compression_exposure(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
) -> ContextCompressionExposure {
    // When a budget was already resolved upstream (the production path — see
    // `resolve_token_budget`, which publishes the current-round snapshot in
    // `session.resolved_token_budget` (#180),
    // issue #20 bug 1), use it directly. Only when none is available do we fall
    // back to a model-derived budget. No `model_limits.json` registry is in
    // scope synchronously here, so this fallback resolves to the global default
    // rather than silently fabricating an empty override registry (#20 bug 2).
    let budget = configured_budget.cloned().unwrap_or_else(|| {
        create_budget_for_model(
            model_name,
            BudgetStrategy::default(),
            &ModelLimitsRegistry::new(),
        )
    });
    let counter = TiktokenTokenCounter::default();
    let active_messages = active_messages_for_budget(session);
    let active_message_tokens = counter.count_messages(&active_messages);
    let summary_tokens = session
        .conversation_summary
        .as_ref()
        .map(|summary| counter.count_messages(&[compression_summary_message(&summary.content)]))
        .unwrap_or(0);
    let active_tokens = active_message_tokens.saturating_add(summary_tokens);
    // Use context window as the denominator for a single, provider-aligned
    // pressure scale across backend and frontend.
    let context_window = budget.max_context_tokens;
    let estimated_usage = context_window_usage_percent(active_tokens, context_window);
    let usage = session
        .token_usage
        .as_ref()
        .and_then(|token_usage| {
            let denominator = if token_usage.max_context_tokens > 0 {
                token_usage.max_context_tokens
            } else if token_usage.budget_limit > 0 {
                // Legacy payload compatibility.
                token_usage.budget_limit
            } else {
                context_window
            };
            (denominator > 0).then_some(context_window_usage_percent(
                token_usage.total_tokens,
                denominator,
            ))
        })
        .map(|persisted_usage| persisted_usage.max(estimated_usage))
        .unwrap_or(estimated_usage);

    let rounded = usage.clamp(0.0, 100.0).round() as u8;
    let trigger_tokens = budget.compression_trigger_context_tokens();
    let trigger_percent = if budget.max_context_tokens > 0 {
        (trigger_tokens as f64 / budget.max_context_tokens as f64) * 100.0
    } else {
        0.0
    };
    let threshold_reached = usage >= trigger_percent;

    // Check non-system message count to stay consistent with the plan
    // building requirement of >=3 non-system messages.  Using
    // active_messages.len() would include system messages and expose the
    // tool even when plan building would immediately fail.
    let non_system_count = active_messages
        .iter()
        .filter(|m| !matches!(m.role, bamboo_domain::Role::System))
        .count();

    let should_expose_tool = threshold_reached && non_system_count >= 3;

    ContextCompressionExposure {
        budget,
        active_tokens,
        active_usage_percent: usage,
        active_usage_percent_rounded: rounded,
        should_expose_tool,
    }
}

/// Build a compression plan that archives older active messages and replaces
/// them with a caller-provided summary.
pub fn build_compression_plan_with_summary(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
    summary_content: String,
) -> Result<CompressionPlan, CompressionPlanError> {
    build_compression_plan_with_summary_internal(
        session,
        model_name,
        configured_budget,
        summary_content,
        true,
        CompressionTriggerType::Auto,
    )
}

/// Build a compression plan while bypassing "tool exposure" gating.
///
/// This is intended for host-enforced fallback paths when context pressure is
/// critically high and compression must be attempted regardless of the normal
/// trigger gate.
pub fn build_forced_compression_plan_with_summary(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
    summary_content: String,
    trigger_type: CompressionTriggerType,
) -> Result<CompressionPlan, CompressionPlanError> {
    build_compression_plan_with_summary_internal(
        session,
        model_name,
        configured_budget,
        summary_content,
        false,
        trigger_type,
    )
}

fn build_compression_plan_with_summary_internal(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
    summary_content: String,
    require_exposure_gate: bool,
    trigger_type: CompressionTriggerType,
) -> Result<CompressionPlan, CompressionPlanError> {
    let exposure = estimate_context_compression_exposure(session, model_name, configured_budget);
    if require_exposure_gate && !exposure.should_expose_tool {
        return Err(CompressionPlanError::ExposureGateNotMet {
            usage_percent: exposure.active_usage_percent,
            trigger_percent: exposure.budget.compression_trigger_percent,
        });
    }

    let budget = &exposure.budget;
    let counter = TiktokenTokenCounter::default();
    let summary_message = compression_summary_message(&summary_content);
    let summary_tokens = counter.count_messages(&[summary_message]);

    let context_window = budget.max_context_tokens;
    let target_limit = budget.compression_target_context_tokens();

    let mut active_messages = active_messages_for_budget(session);
    if active_messages.is_empty() {
        tracing::debug!("compression plan: no active messages, cannot build plan");
        return Err(CompressionPlanError::NoActiveMessages);
    }

    let system_messages: Vec<Message> = active_messages
        .iter()
        .filter(|m| matches!(m.role, bamboo_domain::Role::System))
        .cloned()
        .collect();
    let system_tokens = counter.count_messages(&system_messages);
    let reserved_non_window_tokens = system_tokens.saturating_add(summary_tokens);
    let window_limit = target_limit.saturating_sub(reserved_non_window_tokens);

    let non_system: Vec<Message> = active_messages
        .drain(..)
        .filter(|m| !matches!(m.role, bamboo_domain::Role::System))
        .collect();

    if non_system.len() < 3 {
        tracing::debug!(
            "compression plan: not enough non-system messages ({}), need at least 3",
            non_system.len()
        );
        return Err(CompressionPlanError::NotEnoughMessages {
            non_system_count: non_system.len(),
        });
    }

    let user_indexes = non_system
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            matches!(message.role, bamboo_domain::Role::User).then_some(index)
        })
        .collect::<Vec<_>>();
    let keep_user_count = user_indexes.len().min(3);
    let anchor_index = if keep_user_count > 0 {
        user_indexes[user_indexes.len() - keep_user_count]
    } else {
        non_system
            .iter()
            .rposition(|m| matches!(m.role, bamboo_domain::Role::User))
            .unwrap_or(non_system.len().saturating_sub(1))
    };
    let protected_user_ids: HashSet<String> = if keep_user_count > 0 {
        user_indexes[user_indexes.len() - keep_user_count..]
            .iter()
            .filter_map(|idx| non_system.get(*idx))
            .map(|message| message.id.clone())
            .collect()
    } else {
        HashSet::new()
    };

    tracing::debug!(
        "compression plan: context_window={}, target_limit={}, system_tokens={}, summary_tokens={}, window_limit={}, non_system_messages={}, keep_user_count={}, keep_from_index={}",
        context_window, target_limit, system_tokens, summary_tokens, window_limit, non_system.len(), keep_user_count, anchor_index
    );

    // Keep the newest 3 user turns (or fewer if there are not enough user
    // turns) as active context and summarize older history before that
    // boundary. If budget is still too high, continue moving the oldest
    // non-protected messages into the summarize set.
    let mut messages_to_summarize = non_system[..anchor_index].to_vec();

    // Protected messages must never be summarized — move them to the keep set.
    let mut never_compress_ids: Vec<String> = messages_to_summarize
        .iter()
        .filter(|m| m.never_compress || is_skill_tool_chain_message(m))
        .map(|m| m.id.clone())
        .collect();

    // Also protect tool result messages that correspond to skill tool calls.
    let skill_call_ids: Vec<String> = messages_to_summarize
        .iter()
        .filter(|m| is_skill_tool_chain_message(m))
        .flat_map(|m| m.tool_calls.iter().flatten().map(|c| c.id.clone()))
        .collect();
    if !skill_call_ids.is_empty() {
        for m in &*messages_to_summarize {
            if let Some(ref call_id) = m.tool_call_id {
                if skill_call_ids.contains(call_id) && !never_compress_ids.contains(&m.id) {
                    never_compress_ids.push(m.id.clone());
                }
            }
        }
    }

    if !never_compress_ids.is_empty() {
        messages_to_summarize.retain(|m| !never_compress_ids.contains(&m.id));
    }

    let non_system_count = non_system.len();
    let mut messages_to_keep = non_system[anchor_index..].to_vec();
    // Add never_compress / skill messages to the keep set.
    for id in &never_compress_ids {
        if let Some(msg) = non_system.iter().find(|m| &m.id == id) {
            if !messages_to_keep.iter().any(|m| m.id == *id) {
                messages_to_keep.push(msg.clone());
            }
        }
    }

    while !messages_to_keep.is_empty() {
        let keep_tokens = counter.count_messages(&messages_to_keep);
        if keep_tokens <= window_limit {
            break;
        }

        let Some(remove_index) = messages_to_keep.iter().position(|message| {
            !protected_user_ids.contains(message.id.as_str())
                && !never_compress_ids.contains(&message.id)
        }) else {
            // Remaining messages are all protected; stop shrinking.
            break;
        };
        let moved = messages_to_keep.remove(remove_index);
        messages_to_summarize.push(moved);
    }

    // Tool-chain atomicity. The one-at-a-time eviction above can split a generic
    // (non-skill) tool chain — compressing an assistant `tool_calls` message
    // while keeping its `tool` result active, or vice versa. That leaves an
    // orphan `tool_result` (or a `tool_use` with no result) in the active set,
    // which providers reject with a 400 that then poisons EVERY subsequent
    // request in the session. Close the compressed set over tool chains: keep
    // moving any kept message that shares a `tool_call_id` with an
    // already-compressed message into the summarize set until none remain.
    // (Skill chains are fully protected above, so are never partially
    // compressed; protected user messages carry no `tool_call_id`.)
    close_compressed_set_over_tool_chains(
        &mut messages_to_keep,
        &mut messages_to_summarize,
        &protected_user_ids,
        &never_compress_ids,
    );

    if messages_to_summarize.is_empty() {
        tracing::debug!(
            "compression plan: messages_to_summarize is empty after anchor/keep splitting"
        );
        return Err(CompressionPlanError::NothingToCompress {
            anchor_index,
            non_system_count,
        });
    }

    let compressed_message_ids = messages_to_summarize
        .iter()
        .map(|message| message.id.clone())
        .collect::<Vec<_>>();

    let keep_tokens = counter.count_messages(&messages_to_keep);
    let active_before = exposure.active_usage_percent;
    // Use context_window as denominator, consistent with
    // estimate_context_compression_exposure().
    let active_after = if context_window == 0 {
        0.0
    } else {
        let after_total = reserved_non_window_tokens.saturating_add(keep_tokens);
        (after_total as f64 / context_window as f64) * 100.0
    };

    // Count actual segments being compressed using the same segmenter that
    // prepare_hybrid_context uses, so the segment count is accurate.
    let segmenter = crate::segmenter::MessageSegmenter::new();
    let segments_removed = segmenter.segment(messages_to_summarize.clone()).len();
    let source_tokens = counter.count_messages(&messages_to_summarize);
    let actual_summary_content_tokens = counter.count_text(&summary_content);
    let previous_represented_source_tokens = session
        .conversation_summary
        .as_ref()
        .map(|summary| summary.represented_source_tokens)
        .unwrap_or(0);

    Ok(CompressionPlan {
        logical_pass_id: None,
        fixed_prompt_tokens: 0,
        compressed_message_ids,
        messages_to_summarize,
        summary_tokens,
        summary_content,
        active_usage_before_percent: active_before,
        active_usage_after_percent: active_after,
        trigger_percent: budget.compression_trigger_percent,
        target_percent: budget.compression_target_percent,
        segments_removed,
        trigger_type,
        compression_ratio: 0.0,
        model_used: None,
        latency_ms: 0,
        source_tokens,
        represented_source_tokens: previous_represented_source_tokens.saturating_add(source_tokens),
        target_summary_tokens: actual_summary_content_tokens,
        actual_summary_content_tokens,
        summary_target_ratio: 0.0,
        summary_budget_clamped: false,
        summary_budget_clamp_reason: None,
        summarization_map_calls: 0,
        summarization_reduce_calls: 0,
        summarization_fallback_used: false,
    })
}

/// Apply a previously computed compression plan to the session.
/// Extract recently modified files from tool calls in the given messages.
pub(super) fn extract_recently_modified_files(messages: &[Message]) -> Vec<(String, String)> {
    let mut files = Vec::new();
    for message in messages {
        if let Some(ref tool_calls) = message.tool_calls {
            for call in tool_calls {
                let tool_name = call.function.name.as_str();
                if !matches!(tool_name, "Write" | "Edit" | "Bash") {
                    continue;
                }
                let args = &call.function.arguments;
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(args) {
                    if let Some(path) = parsed.get("file_path").and_then(|v| v.as_str()) {
                        files.push((path.to_string(), tool_name.to_string()));
                    } else if let Some(cmd) = parsed.get("command").and_then(|v| v.as_str()) {
                        // Extract file paths from shell commands heuristically
                        for part in cmd.split_whitespace() {
                            if part.contains('/')
                                && (part.ends_with(".rs")
                                    || part.ends_with(".ts")
                                    || part.ends_with(".js")
                                    || part.ends_with(".toml")
                                    || part.ends_with(".json")
                                    || part.ends_with(".md"))
                            {
                                files.push((part.to_string(), "Bash".to_string()));
                            }
                        }
                    }
                }
            }
        }
    }
    files.truncate(10);
    files
}

/// Extract key decision snippets from assistant messages.
pub(super) fn extract_key_decisions(messages: &[Message], limit: usize) -> Vec<String> {
    let decision_keywords = [
        "decided to",
        "approach is",
        "use ",
        "using ",
        "we'll go with",
        "the plan is",
        "strategy:",
        "solution:",
        "chose to",
        "switched to",
        "refactored to",
        "migrated to",
        "replaced with",
    ];
    let mut decisions = Vec::new();
    for message in messages {
        if !matches!(message.role, bamboo_domain::Role::Assistant) {
            continue;
        }
        let content = &message.content;
        for line in content.lines() {
            let line_lower = line.to_lowercase();
            if decision_keywords.iter().any(|kw| line_lower.contains(kw)) {
                let truncated: String = line.chars().take(200).collect();
                decisions.push(truncated);
                if decisions.len() >= limit {
                    return decisions;
                }
            }
        }
    }
    decisions
}

/// Build a post-compaction recovery message that preserves critical context
/// from the compressed messages so the LLM can continue work without losing
/// track of active files, tasks, and decisions.
fn build_post_compaction_recovery_message(
    compressed_messages: &[Message],
    session: &Session,
) -> Option<Message> {
    if compressed_messages.is_empty() {
        return None;
    }

    let mut sections = Vec::new();

    // 1. Recently modified files
    let files = extract_recently_modified_files(compressed_messages);
    if !files.is_empty() {
        let mut section = String::from("## Recently Modified Files\n");
        for (path, tool) in &files {
            section.push_str(&format!("- {} ({})\n", path, tool));
        }
        sections.push(section);
    }

    // 2. Active tasks from task list
    if let Some(ref task_list) = session.task_list {
        let active_items: Vec<_> = task_list
            .items
            .iter()
            .filter(|item| !matches!(item.status, bamboo_domain::TaskItemStatus::Completed))
            .collect();
        if !active_items.is_empty() {
            let mut section = String::from("## Active Tasks\n");
            for item in active_items.iter().take(10) {
                section.push_str(&format!("- [{:?}] {}\n", item.status, item.description));
            }
            sections.push(section);
        }
    }

    // 3. Key decisions
    let decisions = extract_key_decisions(compressed_messages, 5);
    if !decisions.is_empty() {
        let mut section = String::from("## Key Decisions\n");
        for decision in &decisions {
            section.push_str(&format!("- {}\n", decision));
        }
        sections.push(section);
    }

    if sections.is_empty() {
        return None;
    }

    let mut content = String::from("[post-compaction-recovery]\nContext extracted from compressed messages for continued work.\n\n");
    content.push_str(&sections.join("\n"));

    let mut message = Message::assistant(content, None);
    message.never_compress = true;
    Some(message)
}

struct SummaryQualityMetrics {
    file_coverage: f64,
    decision_coverage: f64,
}

fn validate_summary_quality(summary: &str, messages: &[Message]) -> SummaryQualityMetrics {
    let files = extract_recently_modified_files(messages);
    let decisions = extract_key_decisions(messages, 10);

    let files_mentioned = files
        .iter()
        .filter(|(path, _)| summary.contains(path.as_str()))
        .count();
    let file_coverage = if files.is_empty() {
        1.0
    } else {
        files_mentioned as f64 / files.len() as f64
    };

    let decisions_mentioned = decisions
        .iter()
        .filter(|d| {
            let check_str: String = d.chars().take(50).collect();
            summary.contains(&check_str)
        })
        .count();
    let decision_coverage = if decisions.is_empty() {
        1.0
    } else {
        decisions_mentioned as f64 / decisions.len() as f64
    };

    SummaryQualityMetrics {
        file_coverage,
        decision_coverage,
    }
}

pub fn apply_compression_plan(session: &mut Session, plan: CompressionPlan) -> usize {
    let compressed_ids: HashSet<&str> = plan
        .compressed_message_ids
        .iter()
        .map(String::as_str)
        .collect();

    let mut changed_indexes = Vec::new();
    for (index, message) in session.messages.iter_mut().enumerate() {
        if message.compressed || !compressed_ids.contains(message.id.as_str()) {
            continue;
        }
        message.compressed = true;
        changed_indexes.push(index);
    }

    if changed_indexes.is_empty() {
        return 0;
    }

    let mut event = CompressionEvent::new(
        changed_indexes.len(),
        plan.segments_removed,
        plan.active_usage_before_percent,
        plan.active_usage_after_percent,
        plan.summary_tokens,
        plan.trigger_type,
        plan.compression_ratio,
        plan.model_used.clone(),
        plan.latency_ms,
    );
    if let Some(logical_pass_id) = plan.logical_pass_id.as_ref() {
        event.id.clone_from(logical_pass_id);
    }
    event.source_tokens = plan.source_tokens;
    event.fixed_prompt_tokens = plan.fixed_prompt_tokens;
    event.actual_summary_tokens = plan.actual_summary_content_tokens;
    event.target_summary_tokens = plan.target_summary_tokens;
    event.summary_target_ratio = plan.summary_target_ratio;
    event.actual_summary_ratio = if plan.represented_source_tokens == 0 {
        0.0
    } else {
        plan.actual_summary_content_tokens as f64 / plan.represented_source_tokens as f64
    };
    event.summary_budget_clamped = plan.summary_budget_clamped;
    event.summary_budget_clamp_reason = plan.summary_budget_clamp_reason.clone();
    event.summarization_map_calls = plan.summarization_map_calls;
    event.summarization_reduce_calls = plan.summarization_reduce_calls;
    event.summarization_fallback_used = plan.summarization_fallback_used;
    let event_id = event.id.clone();
    for index in changed_indexes {
        session.messages[index].compressed_by_event_id = Some(event_id.clone());
    }
    session.compression_events.push(event);
    session.conversation_summary = Some(
        ConversationSummary::new(
            &plan.summary_content,
            plan.compressed_message_ids.len(),
            plan.summary_tokens,
        )
        .with_compression_metrics(
            plan.represented_source_tokens,
            plan.target_summary_tokens,
            plan.summary_target_ratio,
            plan.summary_budget_clamped,
            plan.summary_budget_clamp_reason.clone(),
        ),
    );

    // Inject a post-compaction recovery message to preserve critical context
    // from the compressed messages (files, tasks, decisions).
    let compressed_messages: Vec<Message> = session
        .messages
        .iter()
        .filter(|m| compressed_ids.contains(m.id.as_str()))
        .cloned()
        .collect();
    if let Some(recovery) = build_post_compaction_recovery_message(&compressed_messages, session) {
        // Insert just before the last user message, or at the end
        let insert_pos = session
            .messages
            .iter()
            .rposition(|m| matches!(m.role, bamboo_domain::Role::User) && !m.compressed)
            .map(|pos| pos + 1)
            .unwrap_or(session.messages.len());
        session.messages.insert(insert_pos, recovery);
    }

    let quality = validate_summary_quality(&plan.summary_content, &compressed_messages);
    if quality.file_coverage < 0.5 || quality.decision_coverage < 0.3 {
        tracing::warn!(
            "[{}] Summary quality: file_coverage={:.0}%, decision_coverage={:.0}%",
            session.id,
            quality.file_coverage * 100.0,
            quality.decision_coverage * 100.0
        );
    }

    // Instead of clearing token_usage entirely (which forces the next round
    // to rely on heuristic estimates that don't account for tool schema
    // tokens), recompute an approximate post-compression snapshot.  We
    // preserve both the total context-window denominator and the request input
    // limit from the previous usage snapshot so their meanings stay distinct
    // across rounds.
    let counter = TiktokenTokenCounter::default();
    let remaining_active: Vec<_> = session
        .messages
        .iter()
        .filter(|m| !m.compressed)
        .cloned()
        .collect();
    let system_msgs: Vec<_> = remaining_active
        .iter()
        .filter(|m| matches!(m.role, bamboo_domain::Role::System))
        .cloned()
        .collect();
    let window_msgs: Vec<_> = remaining_active
        .iter()
        .filter(|m| !matches!(m.role, bamboo_domain::Role::System))
        .cloned()
        .collect();
    let system_tokens = counter
        .count_messages(&system_msgs)
        .saturating_add(plan.fixed_prompt_tokens);
    let new_summary_tokens = plan.summary_tokens;
    let window_tokens = counter.count_messages(&window_msgs);
    let total_tokens = system_tokens
        .saturating_add(new_summary_tokens)
        .saturating_add(window_tokens);
    let previous_usage = session.token_usage.take();
    let budget_limit = previous_usage
        .as_ref()
        .map(|u| {
            if u.budget_limit > 0 {
                u.budget_limit
            } else {
                // Legacy snapshots used the total context window as the only
                // denominator.
                u.max_context_tokens
            }
        })
        .unwrap_or(0);
    let max_context_tokens = previous_usage
        .as_ref()
        .map(|u| u.max_context_tokens)
        .unwrap_or(0);
    session.token_usage = Some(bamboo_domain::TokenBudgetUsage {
        system_tokens,
        summary_tokens: new_summary_tokens,
        window_tokens,
        total_tokens,
        max_context_tokens,
        budget_limit,
        truncation_occurred: false,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });

    session.reset_model_context_epoch(ModelContextResetReason::Compression);
    session.updated_at = Utc::now();
    plan.compressed_message_ids.len()
}

pub fn compression_summary_message(summary_content: &str) -> Message {
    Message::system(format!(
        "<!-- CONVERSATION_SUMMARY_START -->\n\
         ## Previous Conversation Summary\n\
         The following is compressed historical context for continuity only.\n\
         It is background memory, not a new user request. Follow the current task list and recent messages over this summary when they conflict.\n\n\
         {}\n\
         <!-- CONVERSATION_SUMMARY_END -->",
        summary_content
    ))
}

pub fn active_messages_for_budget(session: &Session) -> Vec<Message> {
    session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .cloned()
        .collect()
}

pub fn summary_source_messages(session: &Session) -> Vec<Message> {
    session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .filter(|message| !matches!(message.role, bamboo_domain::Role::System))
        .cloned()
        .collect()
}

pub fn build_summary_prompt(
    session: &Session,
    messages: &[Message],
    existing_summary: Option<&str>,
) -> String {
    let mut content = String::new();
    content.push_str(
        "You are compressing conversation history for continued work. Produce a compact but reliable working-memory summary.\n\n",
    );
    content.push_str(
        "Critical requirements:\n- First capture the in-flight work right before compression (what was being done, where, and with which tool/file)\n- Distinguish clearly between ACTIVE work, COMPLETED work, and OBSOLETE or superseded work\n- Do not restate old tasks as active unless they are still unresolved\n- The current task list is the source of truth for what is actively being worked on\n- Preserve constraints, decisions, file paths, code changes, errors, tool findings, blockers, and the next step\n- If earlier plans conflict with the current task list or newer messages, treat the earlier plans as obsolete or completed\n- Explicitly evaluate each clear user requirement (e.g. requirement 1, requirement 2) with a status and evidence\n- Return only summary text in the same language as the conversation\n\n",
    );

    if let Some(existing) = existing_summary.map(str::trim).filter(|s| !s.is_empty()) {
        content.push_str("## Existing Summary\n");
        content.push_str(existing);
        content.push_str("\n\n");
    }

    let task_list_prompt = session.format_task_list_for_prompt();
    if !task_list_prompt.trim().is_empty() {
        content.push_str("## Current Task List\n");
        content.push_str(task_list_prompt.trim());
        content.push_str("\n\n");
    }

    content.push_str(
        "## Required Output Sections\n1. Pre-compression in-flight work (what was being done immediately before compression)\n2. Current active objective\n3. Requirement checklist (Requirement | Status: completed/in_progress/pending/blocked/obsolete | Evidence)\n4. Active tasks\n5. Completed tasks\n6. Obsolete or superseded tasks\n7. Important context and constraints\n8. Files, code, and tool findings\n9. Open issues and next step\n\n",
    );

    content.push_str("## Messages To Compress\n\n");
    for message in messages {
        let role = match message.role {
            bamboo_domain::Role::System => continue,
            bamboo_domain::Role::User => "User",
            bamboo_domain::Role::Assistant => match message.phase {
                Some(MessagePhase::Commentary) => "Assistant Commentary",
                Some(MessagePhase::FinalAnswer) => "Assistant Final",
                None => "Assistant",
            },
            bamboo_domain::Role::Tool => "Tool Result",
        };

        content.push_str("### ");
        content.push_str(role);
        content.push('\n');
        if let Some(tool_calls) = &message.tool_calls {
            if !tool_calls.is_empty() {
                let names = tool_calls
                    .iter()
                    .map(|call| call.function.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                content.push_str("Called tools: ");
                content.push_str(&names);
                content.push('\n');
            }
        }
        if let Some(tool_call_id) = &message.tool_call_id {
            content.push_str("Tool call id: ");
            content.push_str(tool_call_id);
            content.push('\n');
        }
        // Keep this compatibility renderer lossless. Production callers bound
        // the fully rendered request through the hierarchical summarizer;
        // clipping each message would discard tails without bounding the total.
        content.push_str(&message.content);
        content.push_str("\n\n");
    }

    content.push_str(
        "Return only the summary text. Be explicit about what is active now versus what is already done or no longer relevant.",
    );
    content
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{FunctionCall, TaskItem, TaskItemStatus, TaskList, ToolCall};
    use bamboo_domain::{ModelContextResetReason, ModelContextState, TokenBudgetUsage};
    use chrono::Utc;

    fn make_budget() -> TokenBudget {
        TokenBudget {
            max_context_tokens: 1000,
            max_output_tokens: 100,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 50,
            compression_target_percent: 20,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        }
    }

    fn make_session_with_pressure() -> Session {
        let mut session = Session::new("compression-hysteresis", "gpt-4o-mini");
        session.token_budget = Some(make_budget());
        session.add_message(Message::system("system"));
        for i in 0..3 {
            session.add_message(Message::user(format!(
                "User message {i}: {}",
                "alpha beta gamma delta epsilon ".repeat(2)
            )));
            session.add_message(Message::assistant(
                format!(
                    "Assistant message {i}: {}",
                    "work log decisions next steps ".repeat(2)
                ),
                None,
            ));
        }
        session
    }

    #[test]
    fn context_window_usage_percent_uses_context_window_denominator() {
        assert_eq!(context_window_usage_percent(0, 0), 0.0);
        assert_eq!(context_window_usage_percent(500, 1000), 50.0);
    }

    #[test]
    fn estimate_context_compression_exposure_crosses_trigger_when_usage_is_high_enough() {
        let mut session = make_session_with_pressure();
        if let Some(budget) = session.token_budget.as_mut() {
            budget.compression_trigger_percent = 10;
        }
        let exposure = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );
        assert!(exposure.active_usage_percent >= 10.0);
        assert!(exposure.should_expose_tool);
    }

    #[test]
    fn estimate_context_compression_exposure_stays_below_trigger_when_usage_is_low() {
        let mut session = make_session_with_pressure();
        if let Some(budget) = session.token_budget.as_mut() {
            budget.compression_trigger_percent = 99;
        }

        let exposure = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );

        assert!(exposure.active_usage_percent < 99.0);
        assert!(!exposure.should_expose_tool);
    }

    #[test]
    fn build_summary_prompt_includes_task_list_and_state_sections() {
        let mut session = Session::new("summary-prompt", "gpt-4o-mini");
        session.set_task_list(TaskList {
            session_id: session.id.clone(),
            title: "Task List".to_string(),
            items: vec![
                TaskItem {
                    id: "task_1".to_string(),
                    description: "检查 51% 又回落到 50% 的触发逻辑".to_string(),
                    status: TaskItemStatus::InProgress,
                    depends_on: Vec::new(),
                    notes: "避免刚压缩完又立刻再次压缩".to_string(),
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "task_2".to_string(),
                    description: "重写 summarizer prompt 并纳入 task list".to_string(),
                    status: TaskItemStatus::Pending,
                    depends_on: Vec::new(),
                    notes: String::new(),
                    ..TaskItem::default()
                },
            ],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let prompt = build_summary_prompt(
            &session,
            &[
                Message::user("继续修复 context compression"),
                Message::assistant("先分析 trigger / target / summary", None),
            ],
            Some("old summary"),
        );

        assert!(prompt.contains("## Current Task List"));
        assert!(prompt.contains("Current active objective"));
        assert!(prompt.contains("Requirement checklist"));
        assert!(prompt.contains("Active tasks"));
        assert!(prompt.contains("Completed tasks"));
        assert!(prompt.contains("Obsolete or superseded tasks"));
        assert!(prompt.contains("检查 51% 又回落到 50% 的触发逻辑"));
        assert!(prompt.contains("old summary"));
    }

    #[test]
    fn forced_plan_keeps_last_three_user_messages_active() {
        let budget = TokenBudget {
            max_context_tokens: 1200,
            max_output_tokens: 100,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: 20,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        };
        let mut session = Session::new("keep-last-three-user-turns", "gpt-4o-mini");
        session.token_budget = Some(budget.clone());
        session.add_message(Message::system("system"));
        for i in 0..6 {
            session.add_message(Message::user(format!(
                "U{i}: {}",
                "alpha beta gamma ".repeat(8)
            )));
            session.add_message(Message::assistant(
                format!("A{i}: {}", "analysis plan steps ".repeat(8)),
                None,
            ));
        }

        let plan = build_forced_compression_plan_with_summary(
            &session,
            "gpt-4o-mini",
            Some(&budget),
            "summary".to_string(),
            CompressionTriggerType::CriticalOverflow,
        )
        .expect("forced plan should build");

        let compressed_ids = plan
            .compressed_message_ids
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let kept_user_contents = session
            .messages
            .iter()
            .filter(|message| !matches!(message.role, bamboo_domain::Role::System))
            .filter(|message| !compressed_ids.contains(message.id.as_str()))
            .filter(|message| matches!(message.role, bamboo_domain::Role::User))
            .map(|message| message.content.clone())
            .collect::<Vec<_>>();

        assert!(
            kept_user_contents.len() >= 3,
            "expected to keep at least 3 user messages, got {}",
            kept_user_contents.len()
        );
        assert!(kept_user_contents
            .iter()
            .any(|content| content.starts_with("U3:")));
        assert!(kept_user_contents
            .iter()
            .any(|content| content.starts_with("U4:")));
        assert!(kept_user_contents
            .iter()
            .any(|content| content.starts_with("U5:")));
    }

    #[test]
    fn estimate_exposure_prefers_persisted_budget_usage_when_higher() {
        let mut session = Session::new("persisted-usage", "gpt-4o-mini");
        session.token_budget = Some(TokenBudget {
            max_context_tokens: 100_000,
            max_output_tokens: 1_000,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: 50,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        });
        session.add_message(Message::system("system"));
        session.add_message(Message::user("short"));
        session.add_message(Message::assistant("short", None));
        session.add_message(Message::user("follow-up"));
        session.add_message(Message::assistant("reply", None));
        session.token_usage = Some(TokenBudgetUsage {
            system_tokens: 100,
            summary_tokens: 0,
            window_tokens: 95_900,
            total_tokens: 96_000,
            max_context_tokens: 100_000,
            budget_limit: 10_000,
            truncation_occurred: true,
            segments_removed: 12,
            prompt_cached_tool_outputs: 0,
            prompt_cached_tool_tokens_saved: 0,
            thinking_tokens: 0,
            cache_read_input_tokens: 0,
        });

        let exposure = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );

        assert!(
            exposure.active_usage_percent >= 96.0,
            "expected persisted context-window usage to drive exposure, got {}",
            exposure.active_usage_percent
        );
        assert!(exposure.should_expose_tool);
    }

    #[test]
    fn never_compress_messages_are_excluded_from_summarize_set() {
        let budget = TokenBudget {
            max_context_tokens: 1200,
            max_output_tokens: 100,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: 20,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        };
        let mut session = Session::new("never-compress-test", "gpt-4o-mini");
        session.token_budget = Some(budget.clone());
        session.add_message(Message::system("system"));

        // Old user message that should be summarized
        session.add_message(Message::user("Old question about X"));
        session.add_message(Message::assistant("Old answer about X", None));

        // Protected user message (never_compress = true)
        let mut protected = Message::user("Critical context that must survive");
        protected.never_compress = true;
        session.add_message(protected);
        session.add_message(Message::assistant("Response to critical", None));

        // Recent user messages that anchor the keep window
        for i in 0..4 {
            session.add_message(Message::user(format!(
                "Recent U{i}: {}",
                "padding text to fill budget ".repeat(6)
            )));
            session.add_message(Message::assistant(
                format!("Recent A{i}: {}", "reply padding text ".repeat(6)),
                None,
            ));
        }

        let plan = build_forced_compression_plan_with_summary(
            &session,
            "gpt-4o-mini",
            Some(&budget),
            "summary".to_string(),
            CompressionTriggerType::Auto,
        )
        .expect("plan should build");

        let compressed_ids: HashSet<&str> = plan
            .compressed_message_ids
            .iter()
            .map(String::as_str)
            .collect();

        // Find the never_compress message
        let protected_msg = session
            .messages
            .iter()
            .find(|m| m.never_compress)
            .expect("should find the protected message");

        assert!(
            !compressed_ids.contains(protected_msg.id.as_str()),
            "never_compress message should NOT be in the compressed set"
        );
    }

    #[test]
    fn skill_tool_chain_messages_are_protected_from_compression() {
        let budget = TokenBudget {
            max_context_tokens: 1200,
            max_output_tokens: 100,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: 20,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        };
        let mut session = Session::new("skill-chain-test", "gpt-4o-mini");
        session.token_budget = Some(budget.clone());
        session.add_message(Message::system("system"));

        // Skill tool chain (load_skill + read_skill_resource)
        let mut skill_call = Message::assistant(String::new(), None);
        skill_call.tool_calls = Some(vec![ToolCall {
            id: "tc-skill".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "load_skill".to_string(),
                arguments: r#"{"skill_id":"my-skill"}"#.to_string(),
            },
        }]);
        session.add_message(skill_call);

        let mut skill_result = Message::tool_result("tc-skill", "skill loaded");
        skill_result.tool_success = Some(true);
        session.add_message(skill_result);

        // Regular messages to fill budget
        for i in 0..6 {
            session.add_message(Message::user(format!(
                "U{i}: {}",
                "alpha beta gamma delta ".repeat(8)
            )));
            session.add_message(Message::assistant(
                format!("A{i}: {}", "analysis steps plan ".repeat(8)),
                None,
            ));
        }

        let plan = build_forced_compression_plan_with_summary(
            &session,
            "gpt-4o-mini",
            Some(&budget),
            "summary".to_string(),
            CompressionTriggerType::Auto,
        )
        .expect("plan should build");

        let compressed_ids: HashSet<&str> = plan
            .compressed_message_ids
            .iter()
            .map(String::as_str)
            .collect();

        // Skill tool chain messages should not be compressed
        let skill_messages: Vec<&Message> = session
            .messages
            .iter()
            .filter(|m| {
                m.tool_calls
                    .as_ref()
                    .is_some_and(|calls| calls.iter().any(|c| c.function.name == "load_skill"))
                    || m.tool_call_id.as_deref() == Some("tc-skill")
            })
            .collect();

        for msg in &skill_messages {
            assert!(
                !compressed_ids.contains(msg.id.as_str()),
                "skill tool chain message {} should NOT be compressed",
                msg.id
            );
        }
    }

    #[test]
    fn generic_tool_chain_is_never_split_by_forced_compression() {
        // A generic (non-skill) tool_use and its tool_result must never be split
        // across the compression boundary — a split orphans one of them in the
        // active set and the provider 400s, poisoning the session. #340.
        let budget = TokenBudget {
            max_context_tokens: 1200,
            max_output_tokens: 100,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: 20,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        };
        let mut session = Session::new("generic-chain-test", "gpt-4o-mini");
        session.token_budget = Some(budget.clone());
        session.add_message(Message::system("system"));

        // A generic tool chain (search + its result) placed early so it is an
        // eviction candidate; the large result makes it a prime compression target.
        let mut call = Message::assistant(String::new(), None);
        call.tool_calls = Some(vec![ToolCall {
            id: "tc-gen".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"rust"}"#.to_string(),
            },
        }]);
        session.add_message(call);
        let mut result = Message::tool_result("tc-gen", &"search result payload ".repeat(20));
        result.tool_success = Some(true);
        session.add_message(result);

        // Filler to push usage over budget and force eviction.
        for i in 0..8 {
            session.add_message(Message::user(format!(
                "U{i}: {}",
                "alpha beta gamma delta ".repeat(8)
            )));
            session.add_message(Message::assistant(
                format!("A{i}: {}", "analysis steps plan ".repeat(8)),
                None,
            ));
        }

        let plan = build_forced_compression_plan_with_summary(
            &session,
            "gpt-4o-mini",
            Some(&budget),
            "summary".to_string(),
            CompressionTriggerType::Auto,
        )
        .expect("plan should build");

        let compressed: HashSet<&str> = plan
            .compressed_message_ids
            .iter()
            .map(String::as_str)
            .collect();

        let assistant_id = session
            .messages
            .iter()
            .find(|m| {
                m.tool_calls
                    .as_ref()
                    .is_some_and(|c| c.iter().any(|tc| tc.id == "tc-gen"))
            })
            .map(|m| m.id.as_str())
            .expect("assistant tool_use message present");
        let result_id = session
            .messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some("tc-gen"))
            .map(|m| m.id.as_str())
            .expect("tool_result message present");

        // The tool_use and its tool_result must land on the SAME side.
        assert_eq!(
            compressed.contains(assistant_id),
            compressed.contains(result_id),
            "generic tool_use ({assistant_id}) and its tool_result ({result_id}) must not be split"
        );
    }

    fn tool_use_message(call_id: &str) -> Message {
        let mut assistant = Message::assistant(String::new(), None);
        assistant.tool_calls = Some(vec![ToolCall {
            id: call_id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: "{}".to_string(),
            },
        }]);
        assistant
    }

    #[test]
    fn close_compressed_set_over_tool_chains_reunites_a_split_chain() {
        // Pre-split state: the assistant tool_use is compressed while its
        // tool_result was left in the keep (active) set — an orphan. #340.
        let assistant = tool_use_message("tc-1");
        let result = Message::tool_result("tc-1", "result payload");
        let assistant_id = assistant.id.clone();
        let result_id = result.id.clone();

        let mut messages_to_keep = vec![result];
        let mut messages_to_summarize = vec![assistant];

        close_compressed_set_over_tool_chains(
            &mut messages_to_keep,
            &mut messages_to_summarize,
            &HashSet::new(),
            &[],
        );

        // The orphan tool_result must be pulled into the compressed set.
        assert!(
            messages_to_keep.is_empty(),
            "orphaned tool_result must be moved into the compressed set"
        );
        let summarized: HashSet<&str> = messages_to_summarize
            .iter()
            .map(|m| m.id.as_str())
            .collect();
        assert!(summarized.contains(assistant_id.as_str()));
        assert!(summarized.contains(result_id.as_str()));
    }

    #[test]
    fn close_compressed_set_over_tool_chains_respects_protected_messages() {
        // A protected (never-compress) chain member must NOT be force-compressed.
        let assistant = tool_use_message("tc-2");
        let result = Message::tool_result("tc-2", "result");
        let result_id = result.id.clone();

        let mut messages_to_keep = vec![result];
        let mut messages_to_summarize = vec![assistant];
        let never_compress_ids = vec![result_id.clone()];

        close_compressed_set_over_tool_chains(
            &mut messages_to_keep,
            &mut messages_to_summarize,
            &HashSet::new(),
            &never_compress_ids,
        );

        assert_eq!(
            messages_to_keep.len(),
            1,
            "protected result must stay in keep"
        );
        assert_eq!(messages_to_keep[0].id, result_id);
    }

    #[test]
    fn recovery_message_returns_none_for_empty_messages() {
        let session = Session::new("recovery-empty", "model");
        let result = build_post_compaction_recovery_message(&[], &session);
        assert!(result.is_none());
    }

    #[test]
    fn recovery_message_has_never_compress_flag() {
        let mut session = Session::new("recovery-flag", "model");
        let messages = vec![Message::assistant("no decisions here", None)];
        session.set_task_list(TaskList {
            session_id: session.id.clone(),
            title: "Tasks".to_string(),
            items: vec![TaskItem {
                id: "t1".to_string(),
                description: "Active task".to_string(),
                status: TaskItemStatus::InProgress,
                ..TaskItem::default()
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let recovery = build_post_compaction_recovery_message(&messages, &session)
            .expect("should return recovery message");
        assert!(recovery.never_compress);
        assert!(recovery.content.contains("[post-compaction-recovery]"));
    }

    #[test]
    fn recovery_message_extracts_file_paths_from_tool_calls() {
        let session = Session::new("recovery-files", "model");
        let mut write_call = Message::assistant("writing file", None);
        write_call.tool_calls = Some(vec![ToolCall {
            id: "tc1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: r#"{"file_path":"/src/main.rs","content":"fn main() {}"}"#.to_string(),
            },
        }]);
        let mut edit_call = Message::assistant("editing file", None);
        edit_call.tool_calls = Some(vec![ToolCall {
            id: "tc2".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Edit".to_string(),
                arguments: r#"{"file_path":"/lib/utils.rs","old":"x","new":"y"}"#.to_string(),
            },
        }]);
        let messages = vec![write_call, edit_call];

        let recovery = build_post_compaction_recovery_message(&messages, &session)
            .expect("should return recovery");
        assert!(recovery.content.contains("/src/main.rs"));
        assert!(recovery.content.contains("/lib/utils.rs"));
        assert!(recovery.content.contains("Recently Modified Files"));
    }

    #[test]
    fn recovery_message_includes_active_tasks() {
        let mut session = Session::new("recovery-tasks", "model");
        session.set_task_list(TaskList {
            session_id: session.id.clone(),
            title: "Tasks".to_string(),
            items: vec![
                TaskItem {
                    id: "t1".to_string(),
                    description: "Fix auth middleware".to_string(),
                    status: TaskItemStatus::InProgress,
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "t2".to_string(),
                    description: "Add tests".to_string(),
                    status: TaskItemStatus::Pending,
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "t3".to_string(),
                    description: "Done task".to_string(),
                    status: TaskItemStatus::Completed,
                    ..TaskItem::default()
                },
            ],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let messages = vec![Message::assistant("some work", None)];

        let recovery = build_post_compaction_recovery_message(&messages, &session)
            .expect("should return recovery");
        assert!(recovery.content.contains("Active Tasks"));
        assert!(recovery.content.contains("Fix auth middleware"));
        assert!(recovery.content.contains("Add tests"));
        // Completed tasks should NOT appear in active tasks
        assert!(!recovery.content.contains("Done task"));
    }

    #[test]
    fn apply_compression_plan_injects_recovery_message() {
        let budget = TokenBudget {
            max_context_tokens: 1200,
            max_output_tokens: 100,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: 20,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        };
        let mut session = Session::new("recovery-inject", "gpt-4o-mini");
        session.token_budget = Some(budget.clone());
        session.model_context_state = Some(ModelContextState {
            prefix_epoch: 7,
            cache_scope_sha256: Some("old-scope".to_string()),
            ..ModelContextState::default()
        });
        session.add_message(Message::system("system"));

        // Old messages with tool calls containing file paths
        let mut write_msg = Message::assistant("writing", None);
        write_msg.tool_calls = Some(vec![ToolCall {
            id: "tc-w".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: r#"{"file_path":"/src/lib.rs","content":"pub fn hello() {}"}"#
                    .to_string(),
            },
        }]);
        session.add_message(Message::user("Write the file"));
        session.add_message(write_msg);

        // Fill with enough messages to force compression
        for i in 0..6 {
            session.add_message(Message::user(format!(
                "U{i}: {}",
                "alpha beta gamma delta ".repeat(8)
            )));
            session.add_message(Message::assistant(
                format!("A{i}: {}", "analysis plan ".repeat(8)),
                None,
            ));
        }

        let plan = build_forced_compression_plan_with_summary(
            &session,
            "gpt-4o-mini",
            Some(&budget),
            "summary text".to_string(),
            CompressionTriggerType::Auto,
        )
        .expect("plan should build");

        assert!(!plan.compressed_message_ids.is_empty());

        let compressed_count = apply_compression_plan(&mut session, plan);
        assert!(compressed_count > 0);

        // Verify recovery message was injected
        let has_recovery = session.messages.iter().any(|m| {
            m.never_compress
                && m.content.contains("[post-compaction-recovery]")
                && m.content.contains("/src/lib.rs")
        });
        assert!(
            has_recovery,
            "session should contain a post-compaction recovery message with the file path"
        );
        let state = session
            .model_context_state
            .as_ref()
            .expect("compression retains an explicit empty epoch boundary");
        assert_eq!(state.prefix_epoch, 8);
        assert_eq!(
            state.last_reset_reason,
            Some(ModelContextResetReason::Compression)
        );
        assert!(state.events.is_empty());
        assert!(state.baselines.is_empty());
        assert!(state.cache_scope_sha256.is_none());
    }

    #[test]
    fn summary_quality_full_coverage_when_all_files_mentioned() {
        let messages = vec![{
            let mut m = Message::assistant("writing", None);
            m.tool_calls = Some(vec![ToolCall {
                id: "tc1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "Write".to_string(),
                    arguments: r#"{"file_path":"/src/main.rs","content":"fn main() {}"}"#
                        .to_string(),
                },
            }]);
            m
        }];
        let summary = "Modified /src/main.rs to add main function";
        let quality = validate_summary_quality(summary, &messages);
        assert!(
            quality.file_coverage >= 0.99,
            "file_coverage should be ~1.0, got {:.2}",
            quality.file_coverage
        );
    }

    #[test]
    fn summary_quality_zero_coverage_when_no_files_mentioned() {
        let messages = vec![{
            let mut m = Message::assistant("writing", None);
            m.tool_calls = Some(vec![ToolCall {
                id: "tc1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "Write".to_string(),
                    arguments: r#"{"file_path":"/src/main.rs","content":"fn main() {}"}"#
                        .to_string(),
                },
            }]);
            m
        }];
        let summary = "Summary that mentions nothing about files";
        let quality = validate_summary_quality(summary, &messages);
        assert!(
            quality.file_coverage < 0.01,
            "file_coverage should be ~0.0, got {:.2}",
            quality.file_coverage
        );
    }

    #[test]
    fn summary_quality_handles_empty_messages() {
        let quality = validate_summary_quality("some summary", &[]);
        assert_eq!(quality.file_coverage, 1.0);
        assert_eq!(quality.decision_coverage, 1.0);
    }

    fn candidate_budget(max_context_tokens: u32, target_percent: u8) -> TokenBudget {
        TokenBudget {
            max_context_tokens,
            max_output_tokens: max_context_tokens / 4,
            strategy: BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: target_percent,
            working_reserve_tokens: 0,
            fallback_trigger_percent: 75,
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
            max_tool_output_tokens: 0,
        }
    }

    #[test]
    fn candidate_plan_is_selected_before_summary_and_reserves_twenty_percent_of_source() {
        let budget = candidate_budget(6_000, 40);
        let mut session = Session::new("candidate-first", "main-model");
        session.add_message(Message::system("system"));

        let mut tool_call = Message::assistant("searching", None);
        tool_call.tool_calls = Some(vec![ToolCall {
            id: "candidate-chain".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"compression"}"#.to_string(),
            },
        }]);
        let tool_call_id = tool_call.id.clone();
        session.add_message(tool_call);
        let tool_result = Message::tool_result(
            "candidate-chain",
            "result payload with decisions and paths ".repeat(80),
        );
        let tool_result_id = tool_result.id.clone();
        session.add_message(tool_result);

        let mut never = Message::assistant("protected runtime state ".repeat(80), None);
        never.never_compress = true;
        let never_id = never.id.clone();
        session.add_message(never);

        for index in 0..12 {
            session.add_message(Message::user(format!(
                "U{index}: {}",
                "requirement context decision ".repeat(40)
            )));
            session.add_message(Message::assistant(
                format!("A{index}: {}", "implementation evidence result ".repeat(40)),
                None,
            ));
        }
        let newest_user_ids = session
            .messages
            .iter()
            .filter(|message| matches!(message.role, bamboo_domain::Role::User))
            .rev()
            .take(3)
            .map(|message| message.id.clone())
            .collect::<HashSet<_>>();

        let candidate = build_forced_compression_candidate_plan(
            &session,
            "main-model",
            Some(&budget),
            0.20,
            CompressionTriggerType::Auto,
        )
        .expect("candidate plan should reach target");
        let selected = candidate
            .compressed_message_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();

        assert_eq!(
            selected,
            candidate
                .messages_to_summarize
                .iter()
                .map(|message| message.id.clone())
                .collect()
        );
        assert_eq!(
            candidate.target_summary_tokens,
            ((candidate.source_tokens as f64) * 0.20).ceil() as u32
        );
        assert!(!selected.contains(&never_id));
        assert!(newest_user_ids.is_disjoint(&selected));
        assert_eq!(
            selected.contains(&tool_call_id),
            selected.contains(&tool_result_id),
            "generic tool chain must be selected atomically"
        );
        assert!(
            candidate.projected_usage_after_percent <= budget.compression_target_percent as f64
        );
    }

    #[test]
    fn cumulative_twenty_percent_uses_represented_raw_tokens_not_previous_summary_length() {
        let budget = candidate_budget(30_000, 50);
        let mut session = Session::new("cumulative-ratio", "main-model");
        session.add_message(Message::system("system"));
        session.conversation_summary = Some(
            ConversationSummary::new("existing detailed summary ".repeat(200), 40, 2_000)
                .with_compression_metrics(10_000, 2_000, 0.20, false, None),
        );
        for index in 0..80 {
            session.add_message(Message::user(format!(
                "U{index}: {}",
                "raw source requirement and evidence ".repeat(30)
            )));
            session.add_message(Message::assistant(
                format!(
                    "A{index}: {}",
                    "implementation result and next step ".repeat(30)
                ),
                None,
            ));
        }

        let candidate = build_forced_compression_candidate_plan(
            &session,
            "main-model",
            Some(&budget),
            0.20,
            CompressionTriggerType::Auto,
        )
        .expect("cumulative candidate plan");
        assert_eq!(
            candidate.target_summary_tokens,
            ((10_000u32.saturating_add(candidate.source_tokens) as f64) * 0.20).ceil() as u32
        );
        assert_eq!(
            candidate.represented_source_tokens,
            10_000u32.saturating_add(candidate.source_tokens)
        );
    }

    #[test]
    fn legacy_summary_uses_conservative_growth_and_migrates_represented_source_metadata() {
        let budget = candidate_budget(20_000, 50);
        let counter = TiktokenTokenCounter::default();
        let existing_content = "legacy summary fact and decision ".repeat(180);
        let existing_tokens = counter.count_text(&existing_content);
        let mut session = Session::new("legacy-summary-ratio", "main-model");
        session.add_message(Message::system("system"));
        session.conversation_summary = Some(ConversationSummary::new(
            existing_content,
            40,
            existing_tokens,
        ));
        for index in 0..40 {
            session.add_message(Message::user(format!(
                "U{index}: {}",
                "raw requirement evidence ".repeat(40)
            )));
            session.add_message(Message::assistant(
                format!("A{index}: {}", "result next step ".repeat(40)),
                None,
            ));
        }

        let candidate = build_forced_compression_candidate_plan(
            &session,
            "main-model",
            Some(&budget),
            0.20,
            CompressionTriggerType::Auto,
        )
        .expect("legacy candidate plan");
        assert_eq!(
            candidate.target_summary_tokens,
            existing_tokens.saturating_add(((candidate.source_tokens as f64) * 0.20).ceil() as u32)
        );
        assert_eq!(
            candidate.previous_represented_source_tokens,
            ((existing_tokens as f64) / 0.20).ceil() as u32
        );
        let expected_represented = candidate.represented_source_tokens;
        let expected_target = candidate.target_summary_tokens;
        let plan = finalize_compression_candidate_plan(
            &session,
            candidate,
            "migrated legacy summary with new evidence".to_string(),
        )
        .expect("short real summary should satisfy target");
        assert!(apply_compression_plan(&mut session, plan) > 0);
        let migrated = session
            .conversation_summary
            .as_ref()
            .expect("migrated summary");
        assert_eq!(migrated.represented_source_tokens, expected_represented);
        assert_eq!(migrated.target_token_count, expected_target);
        assert_eq!(migrated.target_ratio, 0.20);
    }

    #[test]
    fn final_postcondition_counts_the_recovery_message_inserted_during_apply() {
        let budget = candidate_budget(8_000, 40);
        let mut session = Session::new("recovery-postcondition", "main-model");
        session.add_message(Message::system("system"));
        for index in 0..20 {
            session.add_message(Message::user(format!(
                "U{index}: {}",
                "requirement source content ".repeat(45)
            )));
            let mut assistant = Message::assistant(
                format!("A{index}: {}", "implementation evidence ".repeat(45)),
                None,
            );
            if index == 0 {
                assistant.tool_calls = Some(vec![ToolCall {
                    id: "write-recovery-763".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "Write".to_string(),
                        arguments:
                            r#"{"file_path":"/workspace/src/recovery_763.rs","content":"fixed"}"#
                                .to_string(),
                    },
                }]);
            }
            session.add_message(assistant);
            if index == 0 {
                session.add_message(Message::tool_result(
                    "write-recovery-763",
                    "write completed",
                ));
            }
        }

        let candidate = build_forced_compression_candidate_plan(
            &session,
            "main-model",
            Some(&budget),
            0.20,
            CompressionTriggerType::CriticalOverflow,
        )
        .expect("candidate should reserve recovery-message tokens");
        let plan = finalize_compression_candidate_plan(
            &session,
            candidate,
            "summary with /workspace/src/recovery_763.rs".to_string(),
        )
        .expect("final plan");
        assert!(apply_compression_plan(&mut session, plan) > 0);
        assert!(session
            .messages
            .iter()
            .any(|message| message.content.contains("[post-compaction-recovery]")));
        assert!(
            session.token_usage.as_ref().is_some_and(
                |usage| usage.total_tokens <= budget.compression_target_context_tokens()
            ),
            "the real active context, including recovery, must remain at or below target"
        );
    }

    #[test]
    fn finalization_revalidates_actual_summary_without_mutating_session() {
        let budget = candidate_budget(6_000, 40);
        let mut session = Session::new("atomic-finalize", "main-model");
        session.add_message(Message::system("system"));
        for index in 0..20 {
            session.add_message(Message::user(format!(
                "U{index}: {}",
                "source content ".repeat(50)
            )));
            session.add_message(Message::assistant(
                format!("A{index}: {}", "response content ".repeat(50)),
                None,
            ));
        }
        let candidate = build_forced_compression_candidate_plan(
            &session,
            "main-model",
            Some(&budget),
            0.20,
            CompressionTriggerType::CriticalOverflow,
        )
        .expect("candidate plan");
        let before_flags = session
            .messages
            .iter()
            .map(|message| (message.id.clone(), message.compressed))
            .collect::<Vec<_>>();

        let result = finalize_compression_candidate_plan(
            &session,
            candidate,
            "oversized summary ".repeat(20_000),
        );
        assert!(matches!(
            result,
            Err(CompressionPlanError::SummaryExceedsTarget { .. })
        ));
        assert_eq!(
            before_flags,
            session
                .messages
                .iter()
                .map(|message| (message.id.clone(), message.compressed))
                .collect::<Vec<_>>()
        );
        assert!(session.conversation_summary.is_none());
        assert!(session.compression_events.is_empty());
    }

    #[test]
    fn candidate_plan_reports_when_protected_content_makes_target_impossible() {
        let budget = candidate_budget(2_000, 20);
        let mut session = Session::new("protected-capacity", "main-model");
        session.add_message(Message::system("system"));
        session.add_message(Message::assistant(
            "one eligible old message ".repeat(200),
            None,
        ));
        for index in 0..3 {
            session.add_message(Message::user(format!(
                "protected recent user {index} {}",
                "large active content ".repeat(300)
            )));
        }

        let result = build_forced_compression_candidate_plan(
            &session,
            "main-model",
            Some(&budget),
            0.20,
            CompressionTriggerType::CriticalOverflow,
        );
        assert!(matches!(
            result,
            Err(CompressionPlanError::ProtectedContentExceedsTarget { .. })
        ));
    }
}
