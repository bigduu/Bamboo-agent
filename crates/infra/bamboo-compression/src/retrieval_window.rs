//! Pure candidate planning for retrieval-window context management.
//!
//! The planner in this module deliberately stops before any session mutation.
//! It selects complete, provider-safe logical turns that a later lifecycle step
//! may archive after capability and persistence invariants have been verified.

use crate::{TiktokenTokenCounter, TokenBudget, TokenCounter};
use bamboo_domain::{canonical_tool_name, Message, Role, Session};
use std::collections::{HashMap, HashSet};
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

/// Immutable evidence describing a safe retrieval-window archive candidate set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalWindowCandidatePlan {
    /// Message IDs to archive, in authoritative session order.
    pub message_ids_to_archive: Vec<String>,
    /// Number of complete logical groups selected, including a preamble group.
    pub archive_group_count: usize,
    /// Number of selected groups anchored by a user message.
    pub archive_user_turn_count: usize,
    /// Tokens represented by `message_ids_to_archive`.
    pub archive_message_tokens: u32,
    /// Active message plus fixed prompt tokens before candidate selection.
    pub active_tokens_before: u32,
    /// Projected active tokens after the candidate messages are removed.
    pub projected_active_tokens_after: u32,
    /// Provider context-window size used for the plan.
    pub context_window_tokens: u32,
    /// Effective token target after output/safety reserves are respected.
    pub target_tokens: u32,
    /// Requested target percentage retained for observability.
    pub target_usage_percent: u8,
    /// Provider-visible prompt/tool tokens outside `Session.messages`.
    pub fixed_prompt_tokens: u32,
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
    build_retrieval_window_candidate_plan_with_fixed_tokens(session, budget, policy, 0)
}

/// Build a pure retrieval-window plan while accounting for prompt blocks and
/// tool schemas rendered outside `Session.messages`.
pub fn build_retrieval_window_candidate_plan_with_fixed_tokens(
    session: &Session,
    budget: &TokenBudget,
    policy: RetrievalWindowPolicy,
    fixed_prompt_tokens: u32,
) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
    build_retrieval_window_candidate_plan_with_counter(
        session,
        budget,
        policy,
        fixed_prompt_tokens,
        &TiktokenTokenCounter::default(),
    )
}

fn build_retrieval_window_candidate_plan_with_counter(
    session: &Session,
    budget: &TokenBudget,
    policy: RetrievalWindowPolicy,
    fixed_prompt_tokens: u32,
    counter: &impl TokenCounter,
) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
    validate_inputs(budget, policy)?;

    let (system_messages, mut groups, active_message_count) = build_logical_groups(session);
    if active_message_count == 0 {
        return Err(RetrievalWindowPlanError::NoActiveMessages);
    }

    let system_tokens = count_indexed_messages(&system_messages, counter)?;
    for group in &mut groups {
        group.token_count = count_indexed_messages(&group.messages, counter)?;
    }
    let group_tokens = checked_sum(groups.iter().map(|group| group.token_count))?;
    let active_tokens_before = fixed_prompt_tokens
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
            fixed_prompt_tokens,
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
        archive_group_count: selected_group_indexes.len(),
        archive_user_turn_count,
        archive_message_tokens,
        active_tokens_before,
        projected_active_tokens_after,
        context_window_tokens: budget.max_context_tokens,
        target_tokens,
        target_usage_percent: policy.target_usage_percent,
        fixed_prompt_tokens,
        protected_active_tokens,
        retained_recent_user_turn_count: total_user_turn_count.min(policy.min_recent_user_turns),
        retained_user_turn_count,
        oldest_retained_message_id,
        oldest_retained_user_message_id,
        incomplete_protocol_group_count,
        message_ids_to_archive,
    })
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
    counter: &impl TokenCounter,
) -> Result<u32, RetrievalWindowPlanError> {
    checked_sum(
        messages
            .iter()
            .map(|indexed| counter.count_message(indexed.message)),
    )
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

fn mark_protected_groups(groups: &mut [LogicalGroup<'_>], min_recent_user_turns: usize) {
    for group in groups.iter_mut() {
        group.protected = !group.protocol_safe
            || group
                .messages
                .iter()
                .any(|indexed| indexed.message.never_compress)
            || group.messages.iter().any(|indexed| {
                indexed.message.tool_calls.as_ref().is_some_and(|calls| {
                    calls.iter().any(|call| {
                        let tool_name = canonical_tool_name(&call.function.name);
                        matches!(tool_name.as_str(), "load_skill" | "read_skill_resource")
                    })
                })
            });
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
    use bamboo_domain::{FunctionCall, ToolCall};

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

    fn plan_with_counter(
        session: &Session,
        target_usage_percent: u8,
        min_recent_user_turns: usize,
        fixed_prompt_tokens: u32,
    ) -> Result<RetrievalWindowCandidatePlan, RetrievalWindowPlanError> {
        build_retrieval_window_candidate_plan_with_counter(
            session,
            &budget(100),
            policy(min_recent_user_turns, target_usage_percent),
            fixed_prompt_tokens,
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
        assert_eq!(first.archive_group_count, 2);
        assert_eq!(first.archive_user_turn_count, 2);
        assert_eq!(first.archive_message_tokens, 40);
        assert_eq!(first.active_tokens_before, 90);
        assert_eq!(first.projected_active_tokens_after, 50);
        assert_eq!(first.fixed_prompt_tokens, 5);
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
                0,
                &CharacterTokenCounter,
            ),
            Err(RetrievalWindowPlanError::InvalidRecentUserTurnFloor)
        );
        assert_eq!(
            build_retrieval_window_candidate_plan_with_counter(
                &session,
                &budget(100),
                policy(1, 0),
                0,
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
