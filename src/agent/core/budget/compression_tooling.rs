use crate::agent::core::agent::types::{CompressionEvent, ConversationSummary, Message, Session};
use crate::agent::core::budget::counter::{HeuristicTokenCounter, TokenCounter};
use crate::agent::core::budget::limits::create_budget_for_model;
use crate::agent::core::budget::{BudgetStrategy, TokenBudget};
use crate::agent::core::MessagePhase;
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
    pub compressed_message_ids: Vec<String>,
    pub messages_to_summarize: Vec<Message>,
    pub summary_tokens: u32,
    pub summary_content: String,
    pub active_usage_before_percent: f64,
    pub active_usage_after_percent: f64,
    pub trigger_percent: u8,
    pub target_percent: u8,
    pub segments_removed: usize,
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
    let budget = configured_budget
        .cloned()
        .unwrap_or_else(|| create_budget_for_model(model_name, BudgetStrategy::default()));
    let counter = HeuristicTokenCounter::default();
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
    let trigger = normalized_trigger_percent(budget.compression_trigger_percent);
    let threshold_reached = usage >= trigger;

    // Check non-system message count to stay consistent with the plan
    // building requirement of >=3 non-system messages.  Using
    // active_messages.len() would include system messages and expose the
    // tool even when plan building would immediately fail.
    let non_system_count = active_messages
        .iter()
        .filter(|m| !matches!(m.role, crate::agent::core::Role::System))
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
) -> Result<CompressionPlan, CompressionPlanError> {
    build_compression_plan_with_summary_internal(
        session,
        model_name,
        configured_budget,
        summary_content,
        false,
    )
}

fn build_compression_plan_with_summary_internal(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
    summary_content: String,
    require_exposure_gate: bool,
) -> Result<CompressionPlan, CompressionPlanError> {
    let exposure = estimate_context_compression_exposure(session, model_name, configured_budget);
    if require_exposure_gate && !exposure.should_expose_tool {
        return Err(CompressionPlanError::ExposureGateNotMet {
            usage_percent: exposure.active_usage_percent,
            trigger_percent: exposure.budget.compression_trigger_percent,
        });
    }

    let budget = &exposure.budget;
    let counter = HeuristicTokenCounter::default();
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
        .filter(|m| matches!(m.role, crate::agent::core::Role::System))
        .cloned()
        .collect();
    let system_tokens = counter.count_messages(&system_messages);
    let reserved_non_window_tokens = system_tokens.saturating_add(summary_tokens);
    let window_limit = target_limit.saturating_sub(reserved_non_window_tokens);

    let non_system: Vec<Message> = active_messages
        .drain(..)
        .filter(|m| !matches!(m.role, crate::agent::core::Role::System))
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
            matches!(message.role, crate::agent::core::Role::User).then_some(index)
        })
        .collect::<Vec<_>>();
    let keep_user_count = user_indexes.len().min(3);
    let anchor_index = if keep_user_count > 0 {
        user_indexes[user_indexes.len() - keep_user_count]
    } else {
        non_system
            .iter()
            .rposition(|m| matches!(m.role, crate::agent::core::Role::User))
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
    let non_system_count = non_system.len();
    let mut messages_to_keep = non_system[anchor_index..].to_vec();

    while !messages_to_keep.is_empty() {
        let keep_tokens = counter.count_messages(&messages_to_keep);
        if keep_tokens <= window_limit {
            break;
        }

        let Some(remove_index) = messages_to_keep
            .iter()
            .position(|message| !protected_user_ids.contains(message.id.as_str()))
        else {
            // Remaining messages are all protected user turns; stop shrinking.
            break;
        };
        let moved = messages_to_keep.remove(remove_index);
        messages_to_summarize.push(moved);
    }

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
    let segmenter = crate::agent::core::budget::segmenter::MessageSegmenter::new();
    let segments_removed = segmenter.segment(messages_to_summarize.clone()).len();

    Ok(CompressionPlan {
        compressed_message_ids,
        messages_to_summarize,
        summary_tokens,
        summary_content,
        active_usage_before_percent: active_before,
        active_usage_after_percent: active_after,
        trigger_percent: budget.compression_trigger_percent,
        target_percent: budget.compression_target_percent,
        segments_removed,
    })
}

/// Apply a previously computed compression plan to the session.
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

    let event = CompressionEvent::new(
        changed_indexes.len(),
        plan.segments_removed,
        plan.active_usage_before_percent,
        plan.active_usage_after_percent,
        plan.summary_tokens,
    );
    let event_id = event.id.clone();
    for index in changed_indexes {
        session.messages[index].compressed_by_event_id = Some(event_id.clone());
    }
    session.compression_events.push(event);
    session.conversation_summary = Some(ConversationSummary::new(
        &plan.summary_content,
        plan.compressed_message_ids.len(),
        plan.summary_tokens,
    ));

    // Instead of clearing token_usage entirely (which forces the next round
    // to rely on heuristic estimates that don't account for tool schema
    // tokens), recompute an approximate post-compression snapshot.  We
    // preserve the context-window denominator from the previous usage snapshot
    // so percentages stay consistent across rounds.
    let counter = HeuristicTokenCounter::default();
    let remaining_active: Vec<_> = session
        .messages
        .iter()
        .filter(|m| !m.compressed)
        .cloned()
        .collect();
    let system_msgs: Vec<_> = remaining_active
        .iter()
        .filter(|m| matches!(m.role, crate::agent::core::Role::System))
        .cloned()
        .collect();
    let window_msgs: Vec<_> = remaining_active
        .iter()
        .filter(|m| !matches!(m.role, crate::agent::core::Role::System))
        .cloned()
        .collect();
    let system_tokens = counter.count_messages(&system_msgs);
    let new_summary_tokens = plan.summary_tokens;
    let window_tokens = counter.count_messages(&window_msgs);
    let total_tokens = system_tokens
        .saturating_add(new_summary_tokens)
        .saturating_add(window_tokens);
    let previous_usage = session.token_usage.take();
    let budget_limit = previous_usage
        .as_ref()
        .map(|u| {
            if u.max_context_tokens > 0 {
                u.max_context_tokens
            } else {
                u.budget_limit
            }
        })
        .unwrap_or(0);
    let max_context_tokens = previous_usage
        .as_ref()
        .map(|u| u.max_context_tokens)
        .unwrap_or(0);
    session.token_usage = Some(crate::agent::core::TokenBudgetUsage {
        system_tokens,
        summary_tokens: new_summary_tokens,
        window_tokens,
        total_tokens,
        max_context_tokens,
        budget_limit,
        truncation_occurred: false,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
    });

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
        .filter(|message| !matches!(message.role, crate::agent::core::Role::System))
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
            crate::agent::core::Role::System => continue,
            crate::agent::core::Role::User => "User",
            crate::agent::core::Role::Assistant => match message.phase {
                Some(MessagePhase::Commentary) => "Assistant Commentary",
                Some(MessagePhase::FinalAnswer) => "Assistant Final",
                None => "Assistant",
            },
            crate::agent::core::Role::Tool => "Tool Result",
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
        let snippet = truncate_chars(&message.content, 2000);
        content.push_str(&snippet);
        content.push_str("\n\n");
    }

    content.push_str(
        "Return only the summary text. Be explicit about what is active now versus what is already done or no longer relevant.",
    );
    content
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    value.chars().take(max_chars).collect::<String>() + "..."
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::TokenBudgetUsage;
    use crate::agent::core::{TaskItem, TaskItemStatus, TaskList};
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
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
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
                },
                TaskItem {
                    id: "task_2".to_string(),
                    description: "重写 summarizer prompt 并纳入 task list".to_string(),
                    status: TaskItemStatus::Pending,
                    depends_on: Vec::new(),
                    notes: String::new(),
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
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
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
            .filter(|message| !matches!(message.role, crate::agent::core::Role::System))
            .filter(|message| !compressed_ids.contains(message.id.as_str()))
            .filter(|message| matches!(message.role, crate::agent::core::Role::User))
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
            prompt_cache_min_tool_output_chars: 1_200,
            prompt_cache_head_chars: 280,
            prompt_cache_tail_chars: 180,
            prompt_cache_recent_user_turns: 2,
            prompt_cache_recent_tool_chains: 2,
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
}
