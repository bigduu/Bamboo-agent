//! Pure candidate planning for retrieval-window context management.
//!
//! The planner in this module deliberately stops before any session mutation.
//! It selects complete, provider-safe logical turns that a later lifecycle step
//! may archive after capability and persistence invariants have been verified.

use crate::{TiktokenTokenCounter, TokenBudget, TokenCounter};
use bamboo_domain::{
    canonical_tool_name, CompressionEvent, CompressionEventKind, CompressionTriggerType, Message,
    MessagePart, ModelContextResetReason, Role, Session, TokenBudgetUsage,
};
use chrono::Utc;
use std::collections::{BTreeMap, HashMap, HashSet};
use thiserror::Error;

/// Internal policy used while retrieval-window remains an opt-in engine seam.
///
/// Public configuration is intentionally deferred until the runtime can apply
/// a plan transactionally and guarantee current-session history retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrievalWindowPolicy {
    /// Minimum number of newest user-anchored logical turns to keep active.
    pub min_recent_user_turns: usize,
    /// Desired percentage of the provider context window after archiving.
    pub target_usage_percent: u8,
}

/// Provider-aware token accounting supplied to the pure planner.
///
/// Most persisted text messages can be estimated directly. Messages containing
/// images cannot: attachment references are resolved and image token costs are
/// provider-specific. Callers that have prepared the provider request can
/// replace the complete token cost of any message by its stable message ID.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetrievalWindowTokenAccounting {
    /// Provider-visible prompt/tool tokens outside `Session.messages`.
    pub fixed_prompt_tokens: u32,
    /// Complete provider-visible token cost overrides keyed by message ID.
    pub provider_message_tokens: BTreeMap<String, u32>,
}

/// Immutable evidence describing a safe retrieval-window archive candidate set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalWindowCandidatePlan {
    /// Message IDs to archive, in authoritative session order.
    pub message_ids_to_archive: Vec<String>,
    /// Number of messages selected for archival.
    pub archive_message_count: usize,
    /// Number of complete logical groups selected, including a preamble group.
    pub archive_group_count: usize,
    /// Number of selected groups anchored by a user message.
    pub archive_user_turn_count: usize,
    /// Tokens represented by `message_ids_to_archive`.
    pub archive_message_tokens: u32,
    /// Number of active messages observed before candidate selection.
    pub active_message_count: usize,
    /// Active message plus fixed prompt tokens before candidate selection.
    pub active_tokens_before: u32,
    /// Projected active tokens after the candidate messages are removed.
    pub projected_active_tokens_after: u32,
    /// Provider context-window size used for the plan.
    pub context_window_tokens: u32,
    /// Effective provider request-input limit after output and safety reserves.
    pub request_input_limit_tokens: u32,
    /// Effective token target after output/safety reserves are respected.
    pub target_tokens: u32,
    /// Requested target percentage retained for observability.
    pub target_usage_percent: u8,
    /// Provider-visible prompt/tool tokens outside `Session.messages`.
    pub fixed_prompt_tokens: u32,
    /// Tokens contributed by active system messages.
    pub system_message_tokens: u32,
    /// Active messages whose complete cost came from provider-aware accounting.
    pub provider_message_token_override_count: usize,
    /// Active message tokens that cannot be selected by this plan.
    pub protected_active_tokens: u32,
    /// Number of groups retained specifically by the recent-turn floor.
    pub retained_recent_user_turn_count: usize,
    /// Total user-anchored groups projected to remain active.
    pub retained_user_turn_count: usize,
    /// Oldest projected retained non-system message, if one remains.
    pub oldest_retained_message_id: Option<String>,
    /// User anchor of the oldest projected retained user turn, if one remains.
    pub oldest_retained_user_message_id: Option<String>,
    /// Unsafe protocol groups that constrained selection.
    pub incomplete_protocol_group_count: usize,
}

/// Structured reason why retrieval-window planning could not produce a plan.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RetrievalWindowPlanError {
    #[error("retrieval-window requires at least one recent user turn")]
    InvalidRecentUserTurnFloor,
    #[error("retrieval-window target usage percent must be between 1 and 100 (got {target_usage_percent})")]
    InvalidTargetUsagePercent { target_usage_percent: u8 },
    #[error(
        "invalid token budget for retrieval-window planning (context={context_window_tokens}, request_input={max_request_input_tokens})"
    )]
    InvalidTokenBudget {
        context_window_tokens: u32,
        max_request_input_tokens: u32,
    },
    #[error("token accounting overflow while planning retrieval-window archive candidates")]
    TokenAccountingOverflow,
    #[error("provider-aware token accounting is required for image message {message_id}")]
    MissingProviderMessageTokenEstimate { message_id: String },
    #[error("no active messages are available for retrieval-window planning")]
    NoActiveMessages,
    #[error(
        "retrieval-window target is already satisfied (active={active_tokens}, target={target_tokens})"
    )]
    TargetAlreadySatisfied {
        active_tokens: u32,
        target_tokens: u32,
    },
    #[error(
        "no eligible active logical group can be archived (active={active_tokens}, target={target_tokens}, protected={protected_active_tokens}, incomplete_protocol_groups={incomplete_protocol_group_count})"
    )]
    NothingToArchive {
        active_tokens: u32,
        target_tokens: u32,
        protected_active_tokens: u32,
        incomplete_protocol_group_count: usize,
    },
    #[error(
        "protected active content prevents the retrieval-window target (projected={projected_tokens}, target={target_tokens}, protected={protected_active_tokens}, fixed={fixed_prompt_tokens}, incomplete_protocol_groups={incomplete_protocol_group_count})"
    )]
    ProtectedContentExceedsTarget {
        projected_tokens: u32,
        target_tokens: u32,
        protected_active_tokens: u32,
        fixed_prompt_tokens: u32,
        incomplete_protocol_group_count: usize,
    },
}

/// Result of committing a retrieval-window boundary to an in-memory session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalWindowApplyResult {
    pub event_id: String,
    pub newly_archived_message_count: usize,
    pub idempotent_replay: bool,
}

/// Structured reason why a retrieval-window plan could not be committed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RetrievalWindowApplyError {
    #[error("retrieval-window plan has no candidate messages")]
    EmptyCandidateSet,
    #[error("retrieval-window plan candidate at index {index} has an empty message ID")]
    EmptyCandidateId { index: usize },
    #[error("retrieval-window plan repeats candidate message ID {message_id}")]
    DuplicateCandidateId { message_id: String },
    #[error("retrieval-window candidate message {message_id} is missing from the session")]
    MissingCandidateMessage { message_id: String },
    #[error("retrieval-window candidate message ID {message_id} is not unique in the session")]
    DuplicateSessionMessageId { message_id: String },
    #[error("retrieval-window candidate message {message_id} is out of session order")]
    CandidateOrderMismatch { message_id: String },
    #[error("retrieval-window plan evidence is inconsistent: {field}")]
    InconsistentPlan { field: &'static str },
    #[error("session already has a conversation summary")]
    PreExistingConversationSummary,
    #[error("retrieval-window plan was only partially applied")]
    PartialApplication,
    #[error("active candidate message {message_id} is already correlated to an archive event")]
    ActiveCandidateAlreadyCorrelated { message_id: String },
    #[error("retrieval-window candidate message {message_id} is a system message")]
    SystemMessageCandidate { message_id: String },
    #[error("retrieval-window candidate message {message_id} is marked never_compress")]
    NeverCompressCandidate { message_id: String },
    #[error("retrieval-window candidates do not cover a complete logical group")]
    IncompleteLogicalGroup,
    #[error("retrieval-window candidates contain an unsafe tool call/result chain")]
    UnsafeToolProtocol,
    #[error("retrieval-window candidates contain a protected Skill-loading chain")]
    ProtectedSkillChain,
    #[error("retrieval-window candidates include a retained recent user turn")]
    RecentUserTurnProtected,
    #[error("retrieval-window plan is stale: {invariant}")]
    StalePlan { invariant: &'static str },
    #[error("retrieval-window replay candidates refer to different archive events")]
    MixedArchiveEvents,
    #[error("retrieval-window replay event {event_id} is missing or duplicated")]
    MissingOrDuplicateArchiveEvent { event_id: String },
    #[error("archive event {event_id} was produced by a different compression strategy")]
    ArchiveEventKindConflict { event_id: String },
    #[error("retrieval-window replay evidence does not match event {event_id}: {field}")]
    ArchiveEventEvidenceMismatch {
        event_id: String,
        field: &'static str,
    },
    #[error("token accounting overflow while applying retrieval-window plan")]
    TokenAccountingOverflow,
}

#[derive(Debug, Clone, Copy)]
struct IndexedMessage<'a> {
    session_index: usize,
    message: &'a Message,
}

#[derive(Debug)]
struct LogicalGroup<'a> {
    messages: Vec<IndexedMessage<'a>>,
    user_message_id: Option<String>,
    protocol_safe: bool,
    protected: bool,
    token_count: u32,
}

impl<'a> LogicalGroup<'a> {
    fn preamble(message: IndexedMessage<'a>) -> Self {
        Self {
            messages: vec![message],
            user_message_id: None,
            protocol_safe: true,
            protected: false,
            token_count: 0,
        }
    }

    fn user_turn(message: IndexedMessage<'a>) -> Self {
        Self {
            user_message_id: Some(message.message.id.clone()),
            messages: vec![message],
            protocol_safe: true,
            protected: false,
            token_count: 0,
        }
    }
}

/// Build a pure retrieval-window plan with no fixed prompt cost.
pub fn build_retrieval_window_candidate_plan(
    session: &Session,
    budget: &TokenBudget,
    policy: RetrievalWindowPolicy,
) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
    build_retrieval_window_candidate_plan_with_token_accounting(
        session,
        budget,
        policy,
        &RetrievalWindowTokenAccounting::default(),
    )
}

/// Build a pure retrieval-window plan while accounting for prompt blocks and
/// tool schemas rendered outside `Session.messages`.
pub fn build_retrieval_window_candidate_plan_with_fixed_tokens(
    session: &Session,
    budget: &TokenBudget,
    policy: RetrievalWindowPolicy,
    fixed_prompt_tokens: u32,
) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
    build_retrieval_window_candidate_plan_with_token_accounting(
        session,
        budget,
        policy,
        &RetrievalWindowTokenAccounting {
            fixed_prompt_tokens,
            ..RetrievalWindowTokenAccounting::default()
        },
    )
}

/// Build a pure retrieval-window plan with provider-aware message costs.
///
/// An override replaces the complete locally estimated cost of its message. It
/// must therefore be computed after provider-visible transformations such as
/// attachment resolution and image fallback. Any active image message without
/// an override fails closed rather than underestimating the request.
pub fn build_retrieval_window_candidate_plan_with_token_accounting(
    session: &Session,
    budget: &TokenBudget,
    policy: RetrievalWindowPolicy,
    accounting: &RetrievalWindowTokenAccounting,
) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
    build_retrieval_window_candidate_plan_with_counter(
        session,
        budget,
        policy,
        accounting,
        &TiktokenTokenCounter::default(),
    )
}

fn build_retrieval_window_candidate_plan_with_counter(
    session: &Session,
    budget: &TokenBudget,
    policy: RetrievalWindowPolicy,
    accounting: &RetrievalWindowTokenAccounting,
    counter: &impl TokenCounter,
) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
    validate_inputs(budget, policy)?;

    let (system_messages, mut groups, active_message_count) = build_logical_groups(session);
    if active_message_count == 0 {
        return Err(RetrievalWindowPlanError::NoActiveMessages);
    }

    let system_tokens = count_indexed_messages(&system_messages, accounting, counter)?;
    for group in &mut groups {
        group.token_count = count_indexed_messages(&group.messages, accounting, counter)?;
    }
    let group_tokens = checked_sum(groups.iter().map(|group| group.token_count))?;
    let active_tokens_before = accounting
        .fixed_prompt_tokens
        .checked_add(system_tokens)
        .and_then(|tokens| tokens.checked_add(group_tokens))
        .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)?;
    let target_tokens = effective_target_tokens(budget, policy.target_usage_percent);

    if active_tokens_before <= target_tokens {
        return Err(RetrievalWindowPlanError::TargetAlreadySatisfied {
            active_tokens: active_tokens_before,
            target_tokens,
        });
    }

    mark_protocol_safety(&mut groups);
    mark_protected_groups(&mut groups, policy.min_recent_user_turns);

    let protected_group_tokens = checked_sum(
        groups
            .iter()
            .filter(|group| group.protected)
            .map(|group| group.token_count),
    )?;
    let protected_active_tokens = system_tokens
        .checked_add(protected_group_tokens)
        .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)?;
    let incomplete_protocol_group_count =
        groups.iter().filter(|group| !group.protocol_safe).count();

    let mut selected_group_indexes = HashSet::new();
    let mut message_ids_to_archive = Vec::new();
    let mut archive_message_tokens = 0u32;
    let mut projected_active_tokens_after = active_tokens_before;
    let mut archive_user_turn_count = 0usize;

    for (group_index, group) in groups.iter().enumerate() {
        if projected_active_tokens_after <= target_tokens {
            break;
        }
        if group.protected {
            continue;
        }

        selected_group_indexes.insert(group_index);
        archive_message_tokens = archive_message_tokens
            .checked_add(group.token_count)
            .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)?;
        projected_active_tokens_after = projected_active_tokens_after
            .checked_sub(group.token_count)
            .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)?;
        if group.user_message_id.is_some() {
            archive_user_turn_count += 1;
        }
        message_ids_to_archive.extend(
            group
                .messages
                .iter()
                .map(|indexed| indexed.message.id.clone()),
        );
    }

    if message_ids_to_archive.is_empty() {
        return Err(RetrievalWindowPlanError::NothingToArchive {
            active_tokens: active_tokens_before,
            target_tokens,
            protected_active_tokens,
            incomplete_protocol_group_count,
        });
    }
    if projected_active_tokens_after > target_tokens {
        return Err(RetrievalWindowPlanError::ProtectedContentExceedsTarget {
            projected_tokens: projected_active_tokens_after,
            target_tokens,
            protected_active_tokens,
            fixed_prompt_tokens: accounting.fixed_prompt_tokens,
            incomplete_protocol_group_count,
        });
    }

    let oldest_retained_message_id = groups
        .iter()
        .enumerate()
        .filter(|(index, _)| !selected_group_indexes.contains(index))
        .flat_map(|(_, group)| group.messages.iter())
        .min_by_key(|indexed| indexed.session_index)
        .map(|indexed| indexed.message.id.clone());
    let oldest_retained_user_message_id = groups
        .iter()
        .enumerate()
        .find(|(index, group)| {
            !selected_group_indexes.contains(index) && group.user_message_id.is_some()
        })
        .and_then(|(_, group)| group.user_message_id.clone());
    let retained_user_turn_count = groups
        .iter()
        .enumerate()
        .filter(|(index, group)| {
            !selected_group_indexes.contains(index) && group.user_message_id.is_some()
        })
        .count();
    let total_user_turn_count = groups
        .iter()
        .filter(|group| group.user_message_id.is_some())
        .count();

    Ok(RetrievalWindowCandidatePlan {
        archive_message_count: message_ids_to_archive.len(),
        archive_group_count: selected_group_indexes.len(),
        archive_user_turn_count,
        archive_message_tokens,
        active_message_count,
        active_tokens_before,
        projected_active_tokens_after,
        context_window_tokens: budget.max_context_tokens,
        request_input_limit_tokens: budget.max_request_input_tokens(),
        target_tokens,
        target_usage_percent: policy.target_usage_percent,
        fixed_prompt_tokens: accounting.fixed_prompt_tokens,
        system_message_tokens: system_tokens,
        provider_message_token_override_count: system_messages
            .iter()
            .chain(groups.iter().flat_map(|group| group.messages.iter()))
            .filter(|indexed| {
                accounting
                    .provider_message_tokens
                    .contains_key(&indexed.message.id)
            })
            .count(),
        protected_active_tokens,
        retained_recent_user_turn_count: total_user_turn_count.min(policy.min_recent_user_turns),
        retained_user_turn_count,
        oldest_retained_message_id,
        oldest_retained_user_message_id,
        incomplete_protocol_group_count,
        message_ids_to_archive,
    })
}

#[derive(Debug, Clone, Copy)]
struct ValidatedRetrievalWindowUsage {
    system_tokens: u32,
    window_tokens: u32,
}

/// Atomically commit one validated summary-free retrieval-window boundary.
///
/// This operation is intentionally separate from [`crate::apply_compression_plan`]:
/// it never creates a summary or recovery message and performs every fallible
/// validation before mutating the session.
pub fn apply_retrieval_window_plan(
    session: &mut Session,
    plan: &RetrievalWindowCandidatePlan,
) -> Result<RetrievalWindowApplyResult, RetrievalWindowApplyError> {
    let usage = validate_apply_plan_arithmetic(plan)?;
    if session.conversation_summary.is_some() {
        return Err(RetrievalWindowApplyError::PreExistingConversationSummary);
    }

    let candidate_indexes = resolve_candidate_indexes(session, plan)?;
    let active_count = candidate_indexes
        .iter()
        .filter(|index| !session.messages[**index].compressed)
        .count();
    let archived_count = candidate_indexes.len().saturating_sub(active_count);

    if active_count > 0 && archived_count > 0 {
        return Err(RetrievalWindowApplyError::PartialApplication);
    }
    if archived_count == candidate_indexes.len() {
        return validate_idempotent_replay(session, plan, &candidate_indexes);
    }

    for index in &candidate_indexes {
        let message = &session.messages[*index];
        if message.compressed_by_event_id.is_some() {
            return Err(
                RetrievalWindowApplyError::ActiveCandidateAlreadyCorrelated {
                    message_id: message.id.clone(),
                },
            );
        }
        if matches!(message.role, Role::System) {
            return Err(RetrievalWindowApplyError::SystemMessageCandidate {
                message_id: message.id.clone(),
            });
        }
        if message.never_compress {
            return Err(RetrievalWindowApplyError::NeverCompressCandidate {
                message_id: message.id.clone(),
            });
        }
    }

    validate_active_plan_structure(session, plan)?;

    let mut event = CompressionEvent::new(
        plan.archive_message_count,
        plan.archive_group_count,
        usage_percentage(plan.active_tokens_before, plan.context_window_tokens),
        usage_percentage(
            plan.projected_active_tokens_after,
            plan.context_window_tokens,
        ),
        0,
        CompressionTriggerType::Auto,
        0.0,
        None,
        0,
    );
    event.kind = CompressionEventKind::RetrievalWindow;
    event.source_tokens = plan.archive_message_tokens;
    event.fixed_prompt_tokens = plan.fixed_prompt_tokens;
    event.retrieval_active_tokens_before = plan.active_tokens_before;
    event.retrieval_active_message_count_before = plan.active_message_count;
    event.retrieval_active_tokens_after = plan.projected_active_tokens_after;
    event.retrieval_target_tokens = plan.target_tokens;
    event.retrieval_target_usage_percent = plan.target_usage_percent;
    event.retrieval_archived_group_count = plan.archive_group_count;
    event.retrieval_archived_user_turn_count = plan.archive_user_turn_count;
    event.retrieval_archived_message_tokens = plan.archive_message_tokens;
    event.retrieval_system_message_tokens = plan.system_message_tokens;
    event.retrieval_context_window_tokens = plan.context_window_tokens;
    event.retrieval_request_input_limit_tokens = plan.request_input_limit_tokens;
    event.retrieval_retained_recent_user_turn_count = plan.retained_recent_user_turn_count;
    event.retrieval_retained_user_turn_count = plan.retained_user_turn_count;
    event
        .retrieval_oldest_retained_message_id
        .clone_from(&plan.oldest_retained_message_id);
    event
        .retrieval_oldest_retained_user_message_id
        .clone_from(&plan.oldest_retained_user_message_id);
    event.retrieval_provider_message_token_override_count =
        plan.provider_message_token_override_count;
    event.retrieval_protected_active_tokens = plan.protected_active_tokens;
    event.retrieval_incomplete_protocol_group_count = plan.incomplete_protocol_group_count;
    let event_id = event.id.clone();

    for index in candidate_indexes {
        session.messages[index].compressed = true;
        session.messages[index].compressed_by_event_id = Some(event_id.clone());
    }
    session.compression_events.push(event);
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: usage.system_tokens,
        summary_tokens: 0,
        window_tokens: usage.window_tokens,
        total_tokens: plan.projected_active_tokens_after,
        max_context_tokens: plan.context_window_tokens,
        budget_limit: plan.request_input_limit_tokens,
        truncation_occurred: false,
        segments_removed: plan.archive_group_count,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    });
    session.reset_model_context_epoch(ModelContextResetReason::Compression);
    session.updated_at = Utc::now();

    Ok(RetrievalWindowApplyResult {
        event_id,
        newly_archived_message_count: plan.archive_message_count,
        idempotent_replay: false,
    })
}

fn validate_apply_plan_arithmetic(
    plan: &RetrievalWindowCandidatePlan,
) -> Result<ValidatedRetrievalWindowUsage, RetrievalWindowApplyError> {
    if plan.message_ids_to_archive.is_empty() {
        return Err(RetrievalWindowApplyError::EmptyCandidateSet);
    }
    if plan.archive_message_count != plan.message_ids_to_archive.len() {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "archive_message_count",
        });
    }
    if plan.archive_group_count == 0 || plan.archive_group_count > plan.archive_message_count {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "archive_group_count",
        });
    }
    if plan.archive_user_turn_count > plan.archive_group_count {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "archive_user_turn_count",
        });
    }
    if plan.active_message_count < plan.archive_message_count {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "active_message_count",
        });
    }
    if !(1..=100).contains(&plan.target_usage_percent) {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "target_usage_percent",
        });
    }
    if plan.context_window_tokens == 0
        || plan.request_input_limit_tokens == 0
        || plan.request_input_limit_tokens > plan.context_window_tokens
    {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "request_input_limit_tokens",
        });
    }
    let expected_target =
        ((u64::from(plan.context_window_tokens) * u64::from(plan.target_usage_percent)) / 100)
            .max(1) as u32;
    let expected_target = expected_target.min(plan.request_input_limit_tokens);
    if plan.target_tokens != expected_target {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "target_tokens",
        });
    }
    if plan.active_tokens_before <= plan.target_tokens
        || plan.projected_active_tokens_after > plan.target_tokens
        || plan.archive_message_tokens == 0
        || plan
            .active_tokens_before
            .checked_sub(plan.archive_message_tokens)
            != Some(plan.projected_active_tokens_after)
    {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "active_token_arithmetic",
        });
    }
    if plan.retained_recent_user_turn_count > plan.retained_user_turn_count {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "retained_recent_user_turn_count",
        });
    }
    if plan.protected_active_tokens < plan.system_message_tokens {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "protected_active_tokens",
        });
    }

    let system_tokens = plan
        .fixed_prompt_tokens
        .checked_add(plan.system_message_tokens)
        .ok_or(RetrievalWindowApplyError::TokenAccountingOverflow)?;
    let protected_with_fixed = plan
        .fixed_prompt_tokens
        .checked_add(plan.protected_active_tokens)
        .ok_or(RetrievalWindowApplyError::TokenAccountingOverflow)?;
    if system_tokens > plan.projected_active_tokens_after
        || protected_with_fixed > plan.projected_active_tokens_after
    {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "projected_active_tokens_after",
        });
    }
    let window_tokens = plan
        .projected_active_tokens_after
        .checked_sub(system_tokens)
        .ok_or(RetrievalWindowApplyError::TokenAccountingOverflow)?;

    Ok(ValidatedRetrievalWindowUsage {
        system_tokens,
        window_tokens,
    })
}

fn resolve_candidate_indexes(
    session: &Session,
    plan: &RetrievalWindowCandidatePlan,
) -> Result<Vec<usize>, RetrievalWindowApplyError> {
    let mut candidate_ids = HashSet::new();
    for (index, message_id) in plan.message_ids_to_archive.iter().enumerate() {
        if message_id.is_empty() {
            return Err(RetrievalWindowApplyError::EmptyCandidateId { index });
        }
        if !candidate_ids.insert(message_id.as_str()) {
            return Err(RetrievalWindowApplyError::DuplicateCandidateId {
                message_id: message_id.clone(),
            });
        }
    }

    let mut indexes_by_id: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, message) in session.messages.iter().enumerate() {
        if candidate_ids.contains(message.id.as_str()) {
            indexes_by_id
                .entry(message.id.as_str())
                .or_default()
                .push(index);
        }
    }

    let mut indexes = Vec::with_capacity(plan.archive_message_count);
    let mut previous_index = None;
    for message_id in &plan.message_ids_to_archive {
        let matches = indexes_by_id
            .get(message_id.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let index = match matches {
            [] => {
                return Err(RetrievalWindowApplyError::MissingCandidateMessage {
                    message_id: message_id.clone(),
                });
            }
            [index] => *index,
            _ => {
                return Err(RetrievalWindowApplyError::DuplicateSessionMessageId {
                    message_id: message_id.clone(),
                });
            }
        };
        if previous_index.is_some_and(|previous| previous >= index) {
            return Err(RetrievalWindowApplyError::CandidateOrderMismatch {
                message_id: message_id.clone(),
            });
        }
        previous_index = Some(index);
        indexes.push(index);
    }
    Ok(indexes)
}

fn usage_percentage(tokens: u32, limit: u32) -> f64 {
    if limit == 0 {
        0.0
    } else {
        (f64::from(tokens) / f64::from(limit)) * 100.0
    }
}

fn validate_active_plan_structure(
    session: &Session,
    plan: &RetrievalWindowCandidatePlan,
) -> Result<(), RetrievalWindowApplyError> {
    let candidate_ids = plan
        .message_ids_to_archive
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let (_, mut groups, active_message_count) = build_logical_groups(session);
    if active_message_count != plan.active_message_count {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "active_message_count",
        });
    }
    if plan.provider_message_token_override_count > active_message_count {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "provider_message_token_override_count",
        });
    }

    mark_protocol_safety(&mut groups);
    let incomplete_protocol_group_count =
        groups.iter().filter(|group| !group.protocol_safe).count();

    let total_user_turn_count = groups
        .iter()
        .filter(|group| group.user_message_id.is_some())
        .count();
    if (total_user_turn_count > 0 && plan.retained_recent_user_turn_count == 0)
        || plan.retained_recent_user_turn_count > total_user_turn_count
    {
        return Err(RetrievalWindowApplyError::InconsistentPlan {
            field: "retained_recent_user_turn_count",
        });
    }
    mark_protected_groups(&mut groups, plan.retained_recent_user_turn_count);

    let mut selected_group_indexes = Vec::new();
    for (group_index, group) in groups.iter().enumerate() {
        let selected_message_count = group
            .messages
            .iter()
            .filter(|indexed| candidate_ids.contains(indexed.message.id.as_str()))
            .count();
        if selected_message_count == 0 {
            continue;
        }
        if selected_message_count != group.messages.len() {
            return Err(RetrievalWindowApplyError::IncompleteLogicalGroup);
        }
        if !group.protocol_safe {
            return Err(RetrievalWindowApplyError::UnsafeToolProtocol);
        }
        if logical_group_has_protected_skill(group) {
            return Err(RetrievalWindowApplyError::ProtectedSkillChain);
        }
        if group.protected {
            return Err(RetrievalWindowApplyError::RecentUserTurnProtected);
        }
        selected_group_indexes.push(group_index);
    }

    if incomplete_protocol_group_count != plan.incomplete_protocol_group_count {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "incomplete_protocol_group_count",
        });
    }

    if selected_group_indexes.len() != plan.archive_group_count {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "archive_group_count",
        });
    }
    let selected_group_index_set = selected_group_indexes
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let selected_ids_in_session_order = groups
        .iter()
        .enumerate()
        .filter(|(index, _)| selected_group_index_set.contains(index))
        .flat_map(|(_, group)| group.messages.iter())
        .map(|indexed| indexed.message.id.as_str())
        .collect::<Vec<_>>();
    if selected_ids_in_session_order
        != plan
            .message_ids_to_archive
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "candidate_group_order",
        });
    }

    let Some(last_selected_group_index) = selected_group_indexes.last().copied() else {
        return Err(RetrievalWindowApplyError::IncompleteLogicalGroup);
    };
    if groups
        .iter()
        .enumerate()
        .take(last_selected_group_index + 1)
        .any(|(index, group)| !group.protected && !selected_group_index_set.contains(&index))
    {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "oldest_first_selection",
        });
    }

    let archive_user_turn_count = selected_group_indexes
        .iter()
        .filter(|index| groups[**index].user_message_id.is_some())
        .count();
    if archive_user_turn_count != plan.archive_user_turn_count {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "archive_user_turn_count",
        });
    }
    let retained_user_turn_count = groups
        .iter()
        .enumerate()
        .filter(|(index, group)| {
            !selected_group_index_set.contains(index) && group.user_message_id.is_some()
        })
        .count();
    if retained_user_turn_count != plan.retained_user_turn_count {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "retained_user_turn_count",
        });
    }

    let oldest_retained_message_id = groups
        .iter()
        .enumerate()
        .filter(|(index, _)| !selected_group_index_set.contains(index))
        .flat_map(|(_, group)| group.messages.iter())
        .min_by_key(|indexed| indexed.session_index)
        .map(|indexed| indexed.message.id.as_str());
    if oldest_retained_message_id != plan.oldest_retained_message_id.as_deref() {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "oldest_retained_message_id",
        });
    }
    let oldest_retained_user_message_id = groups
        .iter()
        .enumerate()
        .find(|(index, group)| {
            !selected_group_index_set.contains(index) && group.user_message_id.is_some()
        })
        .and_then(|(_, group)| group.user_message_id.as_deref());
    if oldest_retained_user_message_id != plan.oldest_retained_user_message_id.as_deref() {
        return Err(RetrievalWindowApplyError::StalePlan {
            invariant: "oldest_retained_user_message_id",
        });
    }
    Ok(())
}

fn validate_idempotent_replay(
    session: &Session,
    plan: &RetrievalWindowCandidatePlan,
    candidate_indexes: &[usize],
) -> Result<RetrievalWindowApplyResult, RetrievalWindowApplyError> {
    let Some(event_id) = candidate_indexes
        .first()
        .and_then(|index| session.messages[*index].compressed_by_event_id.as_deref())
    else {
        return Err(RetrievalWindowApplyError::MixedArchiveEvents);
    };
    if candidate_indexes
        .iter()
        .any(|index| session.messages[*index].compressed_by_event_id.as_deref() != Some(event_id))
    {
        return Err(RetrievalWindowApplyError::MixedArchiveEvents);
    }

    let matching_events = session
        .compression_events
        .iter()
        .filter(|event| event.id == event_id)
        .collect::<Vec<_>>();
    let [event] = matching_events.as_slice() else {
        return Err(RetrievalWindowApplyError::MissingOrDuplicateArchiveEvent {
            event_id: event_id.to_string(),
        });
    };
    if event.kind != CompressionEventKind::RetrievalWindow {
        return Err(RetrievalWindowApplyError::ArchiveEventKindConflict {
            event_id: event_id.to_string(),
        });
    }

    let correlated_message_ids = session
        .messages
        .iter()
        .filter(|message| message.compressed_by_event_id.as_deref() == Some(event_id))
        .map(|message| message.id.as_str())
        .collect::<Vec<_>>();
    if correlated_message_ids
        != plan
            .message_ids_to_archive
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        return Err(replay_evidence_mismatch(event_id, "message_correlation"));
    }

    macro_rules! require_event_evidence {
        ($actual:expr, $expected:expr, $field:literal) => {
            if $actual != $expected {
                return Err(replay_evidence_mismatch(event_id, $field));
            }
        };
    }

    require_event_evidence!(
        event.messages_compressed,
        plan.archive_message_count,
        "messages_compressed"
    );
    require_event_evidence!(
        event.segments_removed,
        plan.archive_group_count,
        "segments_removed"
    );
    require_event_evidence!(event.summary_tokens, 0, "summary_tokens");
    require_event_evidence!(event.actual_summary_tokens, 0, "actual_summary_tokens");
    require_event_evidence!(
        event.trigger_type,
        CompressionTriggerType::Auto,
        "trigger_type"
    );
    require_event_evidence!(
        event.compression_ratio.to_bits(),
        0.0f64.to_bits(),
        "compression_ratio"
    );
    require_event_evidence!(event.model_used.as_ref(), None, "model_used");
    require_event_evidence!(event.latency_ms, 0, "latency_ms");
    require_event_evidence!(
        event.source_tokens,
        plan.archive_message_tokens,
        "source_tokens"
    );
    require_event_evidence!(
        event.fixed_prompt_tokens,
        plan.fixed_prompt_tokens,
        "fixed_prompt_tokens"
    );
    require_event_evidence!(event.target_summary_tokens, 0, "target_summary_tokens");
    require_event_evidence!(
        event.summary_target_ratio.to_bits(),
        0.0f64.to_bits(),
        "summary_target_ratio"
    );
    require_event_evidence!(
        event.actual_summary_ratio.to_bits(),
        0.0f64.to_bits(),
        "actual_summary_ratio"
    );
    require_event_evidence!(
        event.summary_budget_clamped,
        false,
        "summary_budget_clamped"
    );
    require_event_evidence!(
        event.summary_budget_clamp_reason.as_ref(),
        None,
        "summary_budget_clamp_reason"
    );
    require_event_evidence!(event.summarization_map_calls, 0, "summarization_map_calls");
    require_event_evidence!(
        event.summarization_reduce_calls,
        0,
        "summarization_reduce_calls"
    );
    require_event_evidence!(
        event.summarization_fallback_used,
        false,
        "summarization_fallback_used"
    );
    require_event_evidence!(
        event.retrieval_active_tokens_before,
        plan.active_tokens_before,
        "retrieval_active_tokens_before"
    );
    require_event_evidence!(
        event.retrieval_active_message_count_before,
        plan.active_message_count,
        "retrieval_active_message_count_before"
    );
    require_event_evidence!(
        event.retrieval_active_tokens_after,
        plan.projected_active_tokens_after,
        "retrieval_active_tokens_after"
    );
    require_event_evidence!(
        event.retrieval_target_tokens,
        plan.target_tokens,
        "retrieval_target_tokens"
    );
    require_event_evidence!(
        event.retrieval_target_usage_percent,
        plan.target_usage_percent,
        "retrieval_target_usage_percent"
    );
    require_event_evidence!(
        event.retrieval_archived_group_count,
        plan.archive_group_count,
        "retrieval_archived_group_count"
    );
    require_event_evidence!(
        event.retrieval_archived_user_turn_count,
        plan.archive_user_turn_count,
        "retrieval_archived_user_turn_count"
    );
    require_event_evidence!(
        event.retrieval_archived_message_tokens,
        plan.archive_message_tokens,
        "retrieval_archived_message_tokens"
    );
    require_event_evidence!(
        event.retrieval_system_message_tokens,
        plan.system_message_tokens,
        "retrieval_system_message_tokens"
    );
    require_event_evidence!(
        event.retrieval_context_window_tokens,
        plan.context_window_tokens,
        "retrieval_context_window_tokens"
    );
    require_event_evidence!(
        event.retrieval_request_input_limit_tokens,
        plan.request_input_limit_tokens,
        "retrieval_request_input_limit_tokens"
    );
    require_event_evidence!(
        event.retrieval_retained_recent_user_turn_count,
        plan.retained_recent_user_turn_count,
        "retrieval_retained_recent_user_turn_count"
    );
    require_event_evidence!(
        event.retrieval_retained_user_turn_count,
        plan.retained_user_turn_count,
        "retrieval_retained_user_turn_count"
    );
    require_event_evidence!(
        event.retrieval_oldest_retained_message_id.as_ref(),
        plan.oldest_retained_message_id.as_ref(),
        "retrieval_oldest_retained_message_id"
    );
    require_event_evidence!(
        event.retrieval_oldest_retained_user_message_id.as_ref(),
        plan.oldest_retained_user_message_id.as_ref(),
        "retrieval_oldest_retained_user_message_id"
    );
    require_event_evidence!(
        event.retrieval_provider_message_token_override_count,
        plan.provider_message_token_override_count,
        "retrieval_provider_message_token_override_count"
    );
    require_event_evidence!(
        event.retrieval_protected_active_tokens,
        plan.protected_active_tokens,
        "retrieval_protected_active_tokens"
    );
    require_event_evidence!(
        event.retrieval_incomplete_protocol_group_count,
        plan.incomplete_protocol_group_count,
        "retrieval_incomplete_protocol_group_count"
    );
    require_event_evidence!(
        event.usage_before_percent.to_bits(),
        usage_percentage(plan.active_tokens_before, plan.context_window_tokens).to_bits(),
        "usage_before_percent"
    );
    require_event_evidence!(
        event.usage_after_percent.to_bits(),
        usage_percentage(
            plan.projected_active_tokens_after,
            plan.context_window_tokens
        )
        .to_bits(),
        "usage_after_percent"
    );

    Ok(RetrievalWindowApplyResult {
        event_id: event_id.to_string(),
        newly_archived_message_count: 0,
        idempotent_replay: true,
    })
}

fn replay_evidence_mismatch(event_id: &str, field: &'static str) -> RetrievalWindowApplyError {
    RetrievalWindowApplyError::ArchiveEventEvidenceMismatch {
        event_id: event_id.to_string(),
        field,
    }
}

fn validate_inputs(
    budget: &TokenBudget,
    policy: RetrievalWindowPolicy,
) -> Result<(), RetrievalWindowPlanError> {
    if policy.min_recent_user_turns == 0 {
        return Err(RetrievalWindowPlanError::InvalidRecentUserTurnFloor);
    }
    if !(1..=100).contains(&policy.target_usage_percent) {
        return Err(RetrievalWindowPlanError::InvalidTargetUsagePercent {
            target_usage_percent: policy.target_usage_percent,
        });
    }
    let max_request_input_tokens = budget.max_request_input_tokens();
    if budget.max_context_tokens == 0 || max_request_input_tokens == 0 {
        return Err(RetrievalWindowPlanError::InvalidTokenBudget {
            context_window_tokens: budget.max_context_tokens,
            max_request_input_tokens,
        });
    }
    Ok(())
}

fn effective_target_tokens(budget: &TokenBudget, target_usage_percent: u8) -> u32 {
    let percentage_target =
        ((u64::from(budget.max_context_tokens) * u64::from(target_usage_percent)) / 100).max(1)
            as u32;
    percentage_target.min(budget.max_request_input_tokens())
}

fn checked_sum(values: impl IntoIterator<Item = u32>) -> Result<u32, RetrievalWindowPlanError> {
    values.into_iter().try_fold(0u32, |total, value| {
        total
            .checked_add(value)
            .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)
    })
}

fn count_indexed_messages(
    messages: &[IndexedMessage<'_>],
    accounting: &RetrievalWindowTokenAccounting,
    counter: &impl TokenCounter,
) -> Result<u32, RetrievalWindowPlanError> {
    messages.iter().try_fold(0u32, |total, indexed| {
        total
            .checked_add(count_provider_visible_message_tokens(
                indexed.message,
                accounting,
                counter,
            )?)
            .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)
    })
}

fn count_provider_visible_message_tokens(
    message: &Message,
    accounting: &RetrievalWindowTokenAccounting,
    counter: &impl TokenCounter,
) -> Result<u32, RetrievalWindowPlanError> {
    if let Some(provider_tokens) = accounting.provider_message_tokens.get(&message.id) {
        return Ok(*provider_tokens);
    }

    let mut tokens = counter.count_message(message);

    // Reasoning replay is provider- and request-dependent. This pure planner
    // cannot safely assume stored assistant reasoning will be omitted, so
    // account for it as potentially provider-visible.
    if matches!(message.role, Role::Assistant) {
        if let Some(reasoning) = message
            .reasoning
            .as_deref()
            .filter(|reasoning| !reasoning.is_empty())
        {
            tokens = tokens
                .checked_add(counter.count_text(reasoning))
                .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)?;
        }
    }

    // Provider lowering differs by role: some adapters replace `content` with
    // these parts, while tool-result adapters can expose both. Text can be
    // counted conservatively here. Image cost depends on provider preparation
    // and attachment resolution, so it must come from a complete message-level
    // override rather than the persisted URL text.
    if let Some(parts) = message.content_parts.as_deref() {
        for part in parts {
            let part_tokens = match part {
                MessagePart::Text { text } => counter.count_text(text),
                MessagePart::ImageUrl { .. } => {
                    return Err(
                        RetrievalWindowPlanError::MissingProviderMessageTokenEstimate {
                            message_id: message.id.clone(),
                        },
                    );
                }
            };
            tokens = tokens
                .checked_add(part_tokens)
                .ok_or(RetrievalWindowPlanError::TokenAccountingOverflow)?;
        }
    }

    Ok(tokens)
}

fn build_logical_groups(
    session: &Session,
) -> (Vec<IndexedMessage<'_>>, Vec<LogicalGroup<'_>>, usize) {
    let mut system_messages = Vec::new();
    let mut groups = Vec::new();
    let mut active_message_count = 0usize;

    for (session_index, message) in session.messages.iter().enumerate() {
        if message.compressed {
            continue;
        }
        active_message_count += 1;
        let indexed = IndexedMessage {
            session_index,
            message,
        };

        match message.role {
            Role::System => system_messages.push(indexed),
            Role::User => groups.push(LogicalGroup::user_turn(indexed)),
            Role::Assistant | Role::Tool => match groups.last_mut() {
                Some(group) => group.messages.push(indexed),
                None => groups.push(LogicalGroup::preamble(indexed)),
            },
        }
    }

    (system_messages, groups, active_message_count)
}

fn mark_protocol_safety(groups: &mut [LogicalGroup<'_>]) {
    let mut calls_by_id: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
    let mut results_by_id: HashMap<String, Vec<(usize, usize)>> = HashMap::new();

    for (group_index, group) in groups.iter_mut().enumerate() {
        let mut pending_call_ids = HashSet::new();

        for indexed in &group.messages {
            let message = indexed.message;
            match message.role {
                Role::Assistant => {
                    if message.tool_call_id.is_some() {
                        group.protocol_safe = false;
                    }
                    let calls = message
                        .tool_calls
                        .as_ref()
                        .filter(|calls| !calls.is_empty());
                    match calls {
                        Some(calls) => {
                            if !pending_call_ids.is_empty() {
                                group.protocol_safe = false;
                                pending_call_ids.clear();
                            }
                            let mut local_call_ids = HashSet::new();
                            for call in calls {
                                if call.id.is_empty() || !local_call_ids.insert(call.id.clone()) {
                                    group.protocol_safe = false;
                                }
                                pending_call_ids.insert(call.id.clone());
                                calls_by_id
                                    .entry(call.id.clone())
                                    .or_default()
                                    .push((group_index, indexed.session_index));
                            }
                        }
                        None if !pending_call_ids.is_empty() => {
                            group.protocol_safe = false;
                            pending_call_ids.clear();
                        }
                        None => {}
                    }
                }
                Role::Tool => {
                    if message
                        .tool_calls
                        .as_ref()
                        .is_some_and(|calls| !calls.is_empty())
                    {
                        group.protocol_safe = false;
                    }
                    match message.tool_call_id.as_deref() {
                        Some(call_id) if !call_id.is_empty() => {
                            results_by_id
                                .entry(call_id.to_string())
                                .or_default()
                                .push((group_index, indexed.session_index));
                            if !pending_call_ids.remove(call_id) {
                                group.protocol_safe = false;
                            }
                        }
                        _ => group.protocol_safe = false,
                    }
                }
                Role::User => {
                    if !pending_call_ids.is_empty()
                        || message.tool_call_id.is_some()
                        || message
                            .tool_calls
                            .as_ref()
                            .is_some_and(|calls| !calls.is_empty())
                    {
                        group.protocol_safe = false;
                        pending_call_ids.clear();
                    }
                }
                Role::System => unreachable!("system messages are not placed in logical groups"),
            }
        }

        if !pending_call_ids.is_empty() {
            group.protocol_safe = false;
        }
    }

    let call_ids = calls_by_id
        .keys()
        .chain(results_by_id.keys())
        .cloned()
        .collect::<HashSet<_>>();
    for call_id in call_ids {
        let calls = calls_by_id.get(&call_id).map(Vec::as_slice).unwrap_or(&[]);
        let results = results_by_id
            .get(&call_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let valid = calls.len() == 1
            && results.len() == 1
            && calls[0].0 == results[0].0
            && calls[0].1 < results[0].1;
        if valid {
            continue;
        }
        for (group_index, _) in calls.iter().chain(results.iter()) {
            if let Some(group) = groups.get_mut(*group_index) {
                group.protocol_safe = false;
            }
        }
    }
}

fn logical_group_has_protected_skill(group: &LogicalGroup<'_>) -> bool {
    group.messages.iter().any(|indexed| {
        indexed.message.tool_calls.as_ref().is_some_and(|calls| {
            calls.iter().any(|call| {
                let tool_name = canonical_tool_name(&call.function.name);
                matches!(tool_name.as_str(), "load_skill" | "read_skill_resource")
            })
        })
    })
}

fn mark_protected_groups(groups: &mut [LogicalGroup<'_>], min_recent_user_turns: usize) {
    for group in groups.iter_mut() {
        group.protected = !group.protocol_safe
            || group
                .messages
                .iter()
                .any(|indexed| indexed.message.never_compress)
            || logical_group_has_protected_skill(group);
    }

    let user_group_indexes = groups
        .iter()
        .enumerate()
        .filter_map(|(index, group)| group.user_message_id.as_ref().map(|_| index))
        .collect::<Vec<_>>();
    let retained_start = user_group_indexes
        .len()
        .saturating_sub(min_recent_user_turns);
    for group_index in &user_group_indexes[retained_start..] {
        groups[*group_index].protected = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        provider_transcript_boundary_sha256, ConversationSummary, FunctionCall, ModelContextState,
        ProviderFamily, ProviderProtocol, ProviderTranscriptResetReason, ToolCall,
    };

    #[derive(Debug)]
    struct CharacterTokenCounter;

    impl TokenCounter for CharacterTokenCounter {
        fn count_message(&self, message: &Message) -> u32 {
            message.content.chars().count() as u32
        }

        fn count_text(&self, text: &str) -> u32 {
            text.chars().count() as u32
        }
    }

    fn budget(context_window: u32) -> TokenBudget {
        let mut budget = TokenBudget::for_model(context_window);
        budget.max_output_tokens = 0;
        budget.safety_margin = 0;
        budget
    }

    fn policy(min_recent_user_turns: usize, target_usage_percent: u8) -> RetrievalWindowPolicy {
        RetrievalWindowPolicy {
            min_recent_user_turns,
            target_usage_percent,
        }
    }

    fn identified(mut message: Message, id: &str, tokens: usize) -> Message {
        message.id = id.to_string();
        message.content = "x".repeat(tokens);
        message
    }

    fn system(id: &str, tokens: usize) -> Message {
        identified(Message::system(""), id, tokens)
    }

    fn user(id: &str, tokens: usize) -> Message {
        identified(Message::user(""), id, tokens)
    }

    fn assistant(id: &str, tokens: usize) -> Message {
        identified(Message::assistant("", None), id, tokens)
    }

    fn tool_call(message_id: &str, call_id: &str, tokens: usize, name: &str) -> Message {
        let mut message = assistant(message_id, tokens);
        message.tool_calls = Some(vec![ToolCall {
            id: call_id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }]);
        message
    }

    fn tool_result(message_id: &str, call_id: &str, tokens: usize) -> Message {
        identified(Message::tool_result(call_id, ""), message_id, tokens)
    }

    fn add_turn(session: &mut Session, prefix: &str, tokens_per_message: usize) {
        session.add_message(user(&format!("{prefix}-u"), tokens_per_message));
        session.add_message(assistant(&format!("{prefix}-a"), tokens_per_message));
    }

    fn basic_session_and_plan() -> (Session, RetrievalWindowCandidatePlan) {
        let mut session = Session::new("retrieval-window-apply", "test-model");
        session.add_message(system("system", 5));
        for turn in 1..=4 {
            add_turn(&mut session, &format!("t{turn}"), 10);
        }
        let plan = plan_with_counter(&session, 50, 2, 5).expect("plan should build");
        (session, plan)
    }

    fn safe_tool_session_and_plan() -> (Session, RetrievalWindowCandidatePlan) {
        let mut session = Session::new("retrieval-window-apply-tool", "test-model");
        session.add_message(system("system", 5));
        session.add_message(user("old-u", 5));
        session.add_message(tool_call("old-call-1", "call-1", 5, "Read"));
        session.add_message(tool_result("old-result-1", "call-1", 5));
        session.add_message(tool_call("old-call-2", "call-2", 5, "Grep"));
        session.add_message(tool_result("old-result-2", "call-2", 5));
        session.add_message(assistant("old-final", 5));
        add_turn(&mut session, "recent", 10);
        let plan = plan_with_counter(&session, 50, 1, 0).expect("tool plan should build");
        (session, plan)
    }

    fn assert_apply_error_without_mutation(
        session: &mut Session,
        plan: &RetrievalWindowCandidatePlan,
        expected: RetrievalWindowApplyError,
    ) {
        let before = serde_json::to_vec(session).expect("session should serialize");
        assert_eq!(apply_retrieval_window_plan(session, plan), Err(expected));
        assert_eq!(
            serde_json::to_vec(session).expect("session should serialize"),
            before,
            "failed application must not mutate durable session state"
        );
    }

    fn plan_with_counter(
        session: &Session,
        target_usage_percent: u8,
        min_recent_user_turns: usize,
        fixed_prompt_tokens: u32,
    ) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
        plan_with_counter_and_accounting(
            session,
            target_usage_percent,
            min_recent_user_turns,
            &RetrievalWindowTokenAccounting {
                fixed_prompt_tokens,
                ..RetrievalWindowTokenAccounting::default()
            },
        )
    }

    fn plan_with_counter_and_accounting(
        session: &Session,
        target_usage_percent: u8,
        min_recent_user_turns: usize,
        accounting: &RetrievalWindowTokenAccounting,
    ) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
        build_retrieval_window_candidate_plan_with_counter(
            session,
            &budget(100),
            policy(min_recent_user_turns, target_usage_percent),
            accounting,
            &CharacterTokenCounter,
        )
    }

    #[test]
    fn selects_oldest_complete_turns_deterministically_without_mutation() {
        let mut session = Session::new("retrieval-window-basic", "test-model");
        session.add_message(system("system", 5));
        for turn in 1..=4 {
            add_turn(&mut session, &format!("t{turn}"), 10);
        }
        let before = serde_json::to_value(&session).expect("session should serialize");

        let first = plan_with_counter(&session, 50, 2, 5).expect("plan should build");
        let second = plan_with_counter(&session, 50, 2, 5).expect("plan should repeat");

        assert_eq!(first, second);
        assert_eq!(
            first.message_ids_to_archive,
            vec!["t1-u", "t1-a", "t2-u", "t2-a"]
        );
        assert_eq!(first.archive_message_count, 4);
        assert_eq!(first.archive_group_count, 2);
        assert_eq!(first.archive_user_turn_count, 2);
        assert_eq!(first.archive_message_tokens, 40);
        assert_eq!(first.active_message_count, 9);
        assert_eq!(first.active_tokens_before, 90);
        assert_eq!(first.projected_active_tokens_after, 50);
        assert_eq!(first.context_window_tokens, 100);
        assert_eq!(first.request_input_limit_tokens, 100);
        assert_eq!(first.fixed_prompt_tokens, 5);
        assert_eq!(first.system_message_tokens, 5);
        assert_eq!(first.protected_active_tokens, 45);
        assert_eq!(first.retained_recent_user_turn_count, 2);
        assert_eq!(first.retained_user_turn_count, 2);
        assert_eq!(first.oldest_retained_message_id.as_deref(), Some("t3-u"));
        assert_eq!(
            first.oldest_retained_user_message_id.as_deref(),
            Some("t3-u")
        );
        assert_eq!(
            serde_json::to_value(&session).expect("session should serialize"),
            before,
            "planning must not mutate the authoritative session"
        );
    }

    #[test]
    fn keeps_multiple_tool_rounds_atomic_with_their_user_turn() {
        let mut session = Session::new("retrieval-window-tools", "test-model");
        session.add_message(system("system", 5));
        session.add_message(user("old-u", 5));
        session.add_message(tool_call("old-call-1", "call-1", 5, "Read"));
        session.add_message(tool_result("old-result-1", "call-1", 5));
        session.add_message(tool_call("old-call-2", "call-2", 5, "Grep"));
        session.add_message(tool_result("old-result-2", "call-2", 5));
        session.add_message(assistant("old-final", 5));
        add_turn(&mut session, "recent", 10);

        let plan = plan_with_counter(&session, 50, 1, 0).expect("tool turn is safe");

        assert_eq!(
            plan.message_ids_to_archive,
            vec![
                "old-u",
                "old-call-1",
                "old-result-1",
                "old-call-2",
                "old-result-2",
                "old-final"
            ]
        );
        assert_eq!(plan.archive_group_count, 1);
        assert_eq!(plan.archive_user_turn_count, 1);
        assert_eq!(plan.incomplete_protocol_group_count, 0);
    }

    #[test]
    fn incomplete_tool_chain_is_protected_and_reported() {
        let mut session = Session::new("retrieval-window-incomplete", "test-model");
        session.add_message(system("system", 5));
        session.add_message(user("old-u", 10));
        session.add_message(tool_call("old-call", "missing-result", 10, "Read"));
        add_turn(&mut session, "recent", 15);

        let error = plan_with_counter(&session, 50, 1, 0).expect_err("chain is incomplete");

        assert_eq!(
            error,
            RetrievalWindowPlanError::NothingToArchive {
                active_tokens: 55,
                target_tokens: 50,
                protected_active_tokens: 55,
                incomplete_protocol_group_count: 1,
            }
        );
    }

    #[test]
    fn duplicate_tool_results_make_the_whole_turn_ambiguous() {
        let mut session = Session::new("retrieval-window-ambiguous", "test-model");
        session.add_message(system("system", 5));
        session.add_message(user("old-u", 5));
        session.add_message(tool_call("old-call", "duplicate-result", 5, "Read"));
        session.add_message(tool_result("old-result-1", "duplicate-result", 5));
        session.add_message(tool_result("old-result-2", "duplicate-result", 5));
        add_turn(&mut session, "recent", 15);

        let error = plan_with_counter(&session, 50, 1, 0).expect_err("chain is ambiguous");

        assert_eq!(
            error,
            RetrievalWindowPlanError::NothingToArchive {
                active_tokens: 55,
                target_tokens: 50,
                protected_active_tokens: 55,
                incomplete_protocol_group_count: 1,
            }
        );
    }

    #[test]
    fn never_compress_turn_is_retained_while_another_old_turn_is_selected() {
        let mut session = Session::new("retrieval-window-protected", "test-model");
        session.add_message(system("system", 5));
        let mut protected = user("protected-u", 10);
        protected.never_compress = true;
        session.add_message(protected);
        session.add_message(assistant("protected-a", 10));
        add_turn(&mut session, "eligible", 10);
        add_turn(&mut session, "recent", 10);

        let plan = plan_with_counter(&session, 50, 1, 0).expect("eligible turn should fit");

        assert_eq!(
            plan.message_ids_to_archive,
            vec!["eligible-u", "eligible-a"]
        );
        assert_eq!(plan.protected_active_tokens, 45);
        assert_eq!(
            plan.oldest_retained_user_message_id.as_deref(),
            Some("protected-u")
        );
    }

    #[test]
    fn leading_non_system_preamble_is_an_explicit_candidate_group() {
        let mut session = Session::new("retrieval-window-preamble", "test-model");
        session.add_message(system("system", 5));
        session.add_message(assistant("preamble", 20));
        add_turn(&mut session, "old", 10);
        add_turn(&mut session, "recent", 10);

        let plan = plan_with_counter(&session, 50, 1, 0).expect("preamble should archive");

        assert_eq!(plan.message_ids_to_archive, vec!["preamble"]);
        assert_eq!(plan.archive_group_count, 1);
        assert_eq!(plan.archive_user_turn_count, 0);
        assert_eq!(plan.oldest_retained_message_id.as_deref(), Some("old-u"));
    }

    #[test]
    fn already_archived_messages_are_ignored_on_repeated_planning() {
        let mut session = Session::new("retrieval-window-repeat", "test-model");
        session.add_message(system("system", 5));
        for turn in 1..=4 {
            add_turn(&mut session, &format!("t{turn}"), 10);
        }

        let first = plan_with_counter(&session, 50, 2, 5).expect("first plan should build");
        let archived_ids = first
            .message_ids_to_archive
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        for message in &mut session.messages {
            if archived_ids.contains(message.id.as_str()) {
                message.compressed = true;
            }
        }

        assert_eq!(
            plan_with_counter(&session, 50, 2, 5),
            Err(RetrievalWindowPlanError::TargetAlreadySatisfied {
                active_tokens: 50,
                target_tokens: 50,
            })
        );
    }

    #[test]
    fn fixed_and_protected_content_over_target_returns_typed_error() {
        let mut session = Session::new("retrieval-window-fixed-overflow", "test-model");
        session.add_message(system("system", 5));
        add_turn(&mut session, "old", 10);
        add_turn(&mut session, "recent", 10);

        let error =
            plan_with_counter(&session, 50, 1, 60).expect_err("fixed prompt cost prevents target");

        assert_eq!(
            error,
            RetrievalWindowPlanError::ProtectedContentExceedsTarget {
                projected_tokens: 85,
                target_tokens: 50,
                protected_active_tokens: 25,
                fixed_prompt_tokens: 60,
                incomplete_protocol_group_count: 0,
            }
        );
    }

    #[test]
    fn stored_assistant_reasoning_counts_with_its_logical_group() {
        let mut session = Session::new("retrieval-window-reasoning", "test-model");
        session.add_message(system("system", 5));
        session.add_message(user("old-u", 5));
        let mut reasoned = assistant("old-a", 5);
        reasoned.reasoning = Some("r".repeat(40));
        reasoned.reasoning_signature = Some("provider-signature".to_string());
        session.add_message(reasoned);
        add_turn(&mut session, "recent", 10);

        let plan = plan_with_counter(&session, 50, 1, 0)
            .expect("provider-visible reasoning should put the session over target");

        assert_eq!(plan.active_tokens_before, 75);
        assert_eq!(plan.message_ids_to_archive, vec!["old-u", "old-a"]);
        assert_eq!(plan.archive_message_tokens, 50);
        assert_eq!(plan.projected_active_tokens_after, 25);
    }

    #[test]
    fn provider_prepared_multimodal_cost_counts_with_its_logical_group() {
        let mut session = Session::new("retrieval-window-multimodal", "test-model");
        session.add_message(system("system", 5));
        let mut multimodal = user("old-u", 0);
        multimodal.content_parts = Some(vec![
            MessagePart::Text {
                text: "t".repeat(20),
            },
            MessagePart::ImageUrl {
                image_url: bamboo_domain::ImageUrlRef {
                    url: "i".repeat(20),
                    detail: Some("high".to_string()),
                },
            },
        ]);
        session.add_message(multimodal);
        session.add_message(assistant("old-a", 5));
        add_turn(&mut session, "recent", 10);

        let mut accounting = RetrievalWindowTokenAccounting::default();
        accounting
            .provider_message_tokens
            .insert("old-u".to_string(), 44);

        let plan = plan_with_counter_and_accounting(&session, 50, 1, &accounting)
            .expect("provider-visible multimodal parts should exceed the target");

        assert_eq!(plan.active_tokens_before, 74);
        assert_eq!(plan.message_ids_to_archive, vec!["old-u", "old-a"]);
        assert_eq!(plan.archive_message_tokens, 49);
        assert_eq!(plan.projected_active_tokens_after, 25);
        assert_eq!(plan.provider_message_token_override_count, 1);
    }

    #[test]
    fn image_without_provider_prepared_cost_fails_closed() {
        let mut session = Session::new("retrieval-window-attachment", "test-model");
        session.add_message(system("system", 5));
        let mut image = user("old-u", 0);
        image.content_parts = Some(vec![MessagePart::ImageUrl {
            image_url: bamboo_domain::ImageUrlRef {
                url: "bamboo-attachment://retrieval-window-attachment/image-1".to_string(),
                detail: Some("high".to_string()),
            },
        }]);
        session.add_message(image);
        session.add_message(assistant("old-a", 5));
        add_turn(&mut session, "recent", 10);

        assert_eq!(
            plan_with_counter(&session, 50, 1, 0),
            Err(
                RetrievalWindowPlanError::MissingProviderMessageTokenEstimate {
                    message_id: "old-u".to_string(),
                }
            )
        );
    }

    #[test]
    fn oversized_latest_turn_returns_protected_content_error() {
        let mut session = Session::new("retrieval-window-latest-overflow", "test-model");
        session.add_message(system("system", 5));
        add_turn(&mut session, "old", 10);
        add_turn(&mut session, "recent", 30);

        let error = plan_with_counter(&session, 50, 1, 0)
            .expect_err("latest protected turn cannot meet target");

        assert_eq!(
            error,
            RetrievalWindowPlanError::ProtectedContentExceedsTarget {
                projected_tokens: 65,
                target_tokens: 50,
                protected_active_tokens: 65,
                fixed_prompt_tokens: 0,
                incomplete_protocol_group_count: 0,
            }
        );
    }

    #[test]
    fn invalid_policy_and_already_satisfied_target_are_explicit() {
        let mut session = Session::new("retrieval-window-inputs", "test-model");
        session.add_message(system("system", 5));
        add_turn(&mut session, "one", 10);
        add_turn(&mut session, "two", 10);

        assert_eq!(
            build_retrieval_window_candidate_plan_with_counter(
                &session,
                &budget(100),
                policy(0, 50),
                &RetrievalWindowTokenAccounting::default(),
                &CharacterTokenCounter,
            ),
            Err(RetrievalWindowPlanError::InvalidRecentUserTurnFloor)
        );
        assert_eq!(
            build_retrieval_window_candidate_plan_with_counter(
                &session,
                &budget(100),
                policy(1, 0),
                &RetrievalWindowTokenAccounting::default(),
                &CharacterTokenCounter,
            ),
            Err(RetrievalWindowPlanError::InvalidTargetUsagePercent {
                target_usage_percent: 0,
            })
        );
        assert_eq!(
            plan_with_counter(&session, 50, 1, 0),
            Err(RetrievalWindowPlanError::TargetAlreadySatisfied {
                active_tokens: 45,
                target_tokens: 50,
            })
        );
    }

    #[test]
    fn applies_summary_free_boundary_and_exact_replay_is_a_noop() {
        let (mut session, plan) = basic_session_and_plan();
        let transcript_before = session
            .messages
            .iter()
            .map(|message| {
                (
                    message.id.clone(),
                    message.role.clone(),
                    message.content.clone(),
                )
            })
            .collect::<Vec<_>>();

        session.model_context_state = Some(ModelContextState {
            prefix_epoch: 7,
            cache_scope_sha256: Some("prepared-scope".to_string()),
            ..ModelContextState::default()
        });
        let provider_boundary =
            provider_transcript_boundary_sha256(Some("provider-a"), Some("openai"))
                .expect("provider boundary");
        session
            .activate_provider_transcript_route(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                &provider_boundary,
            )
            .expect("route should activate");
        let model_epoch_before = session
            .model_context_state
            .as_ref()
            .expect("model context")
            .prefix_epoch;
        let provider_epoch_before = session.provider_transcript.epoch();

        let result = apply_retrieval_window_plan(&mut session, &plan).expect("plan should apply");

        assert!(!result.idempotent_replay);
        assert_eq!(result.newly_archived_message_count, 4);
        assert_eq!(session.messages.len(), transcript_before.len());
        assert_eq!(
            session
                .messages
                .iter()
                .map(|message| {
                    (
                        message.id.clone(),
                        message.role.clone(),
                        message.content.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            transcript_before,
            "archive flags must not rewrite authoritative transcript content or order"
        );
        assert!(session.conversation_summary.is_none());
        assert_eq!(session.compression_events.len(), 1);

        let event = &session.compression_events[0];
        assert_eq!(event.id, result.event_id);
        assert_eq!(event.kind, CompressionEventKind::RetrievalWindow);
        assert_eq!(event.messages_compressed, 4);
        assert_eq!(event.segments_removed, 2);
        assert_eq!(event.summary_tokens, 0);
        assert_eq!(event.actual_summary_tokens, 0);
        assert!(event.model_used.is_none());
        assert_eq!(event.retrieval_active_tokens_before, 90);
        assert_eq!(event.retrieval_active_message_count_before, 9);
        assert_eq!(event.retrieval_active_tokens_after, 50);
        assert_eq!(event.retrieval_target_tokens, 50);
        assert_eq!(event.retrieval_archived_message_tokens, 40);
        assert_eq!(event.retrieval_system_message_tokens, 5);
        assert_eq!(event.retrieval_context_window_tokens, 100);
        assert_eq!(event.retrieval_request_input_limit_tokens, 100);

        let archived_ids = plan
            .message_ids_to_archive
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        for message in &session.messages {
            if archived_ids.contains(message.id.as_str()) {
                assert!(message.compressed);
                assert_eq!(
                    message.compressed_by_event_id.as_deref(),
                    Some(result.event_id.as_str())
                );
            } else {
                assert!(!message.compressed);
                assert!(message.compressed_by_event_id.is_none());
            }
        }

        let usage = session.token_usage.as_ref().expect("token usage snapshot");
        assert_eq!(usage.system_tokens, 10);
        assert_eq!(usage.summary_tokens, 0);
        assert_eq!(usage.window_tokens, 40);
        assert_eq!(usage.total_tokens, 50);
        assert_eq!(usage.max_context_tokens, 100);
        assert_eq!(usage.budget_limit, 100);
        assert_eq!(usage.segments_removed, 2);

        assert_eq!(
            session
                .model_context_state
                .as_ref()
                .expect("model context")
                .prefix_epoch,
            model_epoch_before + 1
        );
        assert_eq!(
            session.provider_transcript.epoch(),
            provider_epoch_before + 1
        );
        assert_eq!(
            session.provider_transcript.last_reset_reason(),
            Some(ProviderTranscriptResetReason::Compression)
        );

        let round_trip: Session = serde_json::from_slice(
            &serde_json::to_vec(&session).expect("session should serialize"),
        )
        .expect("session should deserialize");
        assert_eq!(
            round_trip.compression_events[0].kind,
            CompressionEventKind::RetrievalWindow
        );
        assert_eq!(
            round_trip.compression_events[0].retrieval_active_tokens_after,
            50
        );

        let serialized_after_first =
            serde_json::to_vec(&session).expect("session should serialize");
        let model_epoch_after_first = session
            .model_context_state
            .as_ref()
            .expect("model context")
            .prefix_epoch;
        let provider_epoch_after_first = session.provider_transcript.epoch();
        let replay = apply_retrieval_window_plan(&mut session, &plan)
            .expect("exact replay should be idempotent");
        assert!(replay.idempotent_replay);
        assert_eq!(replay.event_id, result.event_id);
        assert_eq!(replay.newly_archived_message_count, 0);
        assert_eq!(
            serde_json::to_vec(&session).expect("session should serialize"),
            serialized_after_first
        );
        assert_eq!(
            session
                .model_context_state
                .as_ref()
                .expect("model context")
                .prefix_epoch,
            model_epoch_after_first
        );
        assert_eq!(
            session.provider_transcript.epoch(),
            provider_epoch_after_first
        );
    }

    #[test]
    fn later_plan_archives_additional_groups_without_reassigning_old_messages() {
        let (mut session, first_plan) = basic_session_and_plan();
        let first = apply_retrieval_window_plan(&mut session, &first_plan)
            .expect("first plan should apply");
        add_turn(&mut session, "t5", 10);
        add_turn(&mut session, "t6", 10);

        let second_plan = plan_with_counter(&session, 50, 2, 5).expect("second plan should build");
        assert_eq!(
            second_plan.message_ids_to_archive,
            vec!["t3-u", "t3-a", "t4-u", "t4-a"]
        );
        let second = apply_retrieval_window_plan(&mut session, &second_plan)
            .expect("second plan should apply");

        assert_ne!(second.event_id, first.event_id);
        assert_eq!(session.compression_events.len(), 2);
        for message_id in &first_plan.message_ids_to_archive {
            let message = session
                .messages
                .iter()
                .find(|message| &message.id == message_id)
                .expect("first message remains exact");
            assert_eq!(
                message.compressed_by_event_id.as_deref(),
                Some(first.event_id.as_str())
            );
        }
        for message_id in &second_plan.message_ids_to_archive {
            let message = session
                .messages
                .iter()
                .find(|message| &message.id == message_id)
                .expect("second message remains exact");
            assert_eq!(
                message.compressed_by_event_id.as_deref(),
                Some(second.event_id.as_str())
            );
        }
    }

    #[test]
    fn missing_duplicate_system_and_never_compress_candidates_fail_transactionally() {
        let (mut session, mut plan) = basic_session_and_plan();
        plan.message_ids_to_archive[0] = "missing".to_string();
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::MissingCandidateMessage {
                message_id: "missing".to_string(),
            },
        );

        let (mut session, mut plan) = basic_session_and_plan();
        plan.message_ids_to_archive[1] = plan.message_ids_to_archive[0].clone();
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::DuplicateCandidateId {
                message_id: "t1-u".to_string(),
            },
        );

        let (mut session, plan) = basic_session_and_plan();
        session
            .messages
            .iter_mut()
            .find(|message| message.id == "t4-a")
            .expect("retained message")
            .id = "t1-u".to_string();
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::DuplicateSessionMessageId {
                message_id: "t1-u".to_string(),
            },
        );

        let (mut session, mut plan) = basic_session_and_plan();
        plan.message_ids_to_archive[0] = "system".to_string();
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::SystemMessageCandidate {
                message_id: "system".to_string(),
            },
        );

        let (mut session, plan) = basic_session_and_plan();
        session
            .messages
            .iter_mut()
            .find(|message| message.id == "t1-u")
            .expect("candidate message")
            .never_compress = true;
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::NeverCompressCandidate {
                message_id: "t1-u".to_string(),
            },
        );
    }

    #[test]
    fn inconsistent_summary_and_stale_plans_fail_before_mutation() {
        let (mut session, mut plan) = basic_session_and_plan();
        plan.archive_message_tokens += 1;
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::InconsistentPlan {
                field: "active_token_arithmetic",
            },
        );

        let (mut session, plan) = basic_session_and_plan();
        session.conversation_summary = Some(ConversationSummary::new("existing", 1, 1));
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::PreExistingConversationSummary,
        );

        let (mut session, plan) = basic_session_and_plan();
        add_turn(&mut session, "newer", 10);
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::StalePlan {
                invariant: "active_message_count",
            },
        );
    }

    #[test]
    fn incomplete_tool_and_skill_groups_fail_before_mutation() {
        let (mut session, mut plan) = basic_session_and_plan();
        plan.message_ids_to_archive.remove(1);
        plan.archive_message_count -= 1;
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::IncompleteLogicalGroup,
        );

        let (mut session, plan) = safe_tool_session_and_plan();
        session
            .messages
            .iter_mut()
            .find(|message| message.id == "old-result-1")
            .expect("tool result")
            .tool_call_id = Some("wrong-call".to_string());
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::UnsafeToolProtocol,
        );

        let (mut session, plan) = safe_tool_session_and_plan();
        session
            .messages
            .iter_mut()
            .find(|message| message.id == "old-call-1")
            .and_then(|message| message.tool_calls.as_mut())
            .and_then(|calls| calls.first_mut())
            .expect("tool call")
            .function
            .name = "namespace::load_skill".to_string();
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::ProtectedSkillChain,
        );
    }

    #[test]
    fn partial_mixed_and_wrong_strategy_replays_are_typed_conflicts() {
        let (mut session, plan) = basic_session_and_plan();
        session
            .messages
            .iter_mut()
            .find(|message| message.id == "t1-u")
            .expect("candidate")
            .compressed = true;
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::PartialApplication,
        );

        let (mut session, plan) = basic_session_and_plan();
        apply_retrieval_window_plan(&mut session, &plan).expect("plan should apply");
        session
            .messages
            .iter_mut()
            .find(|message| message.id == "t1-a")
            .expect("candidate")
            .compressed_by_event_id = Some("different-event".to_string());
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::MixedArchiveEvents,
        );

        let (mut session, plan) = basic_session_and_plan();
        let result = apply_retrieval_window_plan(&mut session, &plan).expect("plan should apply");
        session.compression_events[0].kind = CompressionEventKind::Summary;
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::ArchiveEventKindConflict {
                event_id: result.event_id,
            },
        );

        let (mut session, plan) = basic_session_and_plan();
        let result = apply_retrieval_window_plan(&mut session, &plan).expect("plan should apply");
        session.compression_events.clear();
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::MissingOrDuplicateArchiveEvent {
                event_id: result.event_id,
            },
        );

        let (mut session, plan) = basic_session_and_plan();
        let result = apply_retrieval_window_plan(&mut session, &plan).expect("plan should apply");
        session.compression_events[0].retrieval_target_tokens += 1;
        assert_apply_error_without_mutation(
            &mut session,
            &plan,
            RetrievalWindowApplyError::ArchiveEventEvidenceMismatch {
                event_id: result.event_id,
                field: "retrieval_target_tokens",
            },
        );
    }

    #[test]
    fn skill_loading_turn_remains_protected() {
        let mut session = Session::new("retrieval-window-skill", "test-model");
        session.add_message(system("system", 5));
        session.add_message(user("skill-u", 5));
        session.add_message(tool_call("skill-call", "load", 5, "default::LoAd_SkIlL"));
        session.add_message(tool_result("skill-result", "load", 5));
        session.add_message(assistant("skill-final", 5));
        add_turn(&mut session, "eligible", 10);
        add_turn(&mut session, "recent", 10);

        let plan = plan_with_counter(&session, 50, 1, 0).expect("eligible turn should archive");

        assert_eq!(
            plan.message_ids_to_archive,
            vec!["eligible-u", "eligible-a"]
        );
        assert_eq!(
            plan.oldest_retained_user_message_id.as_deref(),
            Some("skill-u")
        );
    }
}
