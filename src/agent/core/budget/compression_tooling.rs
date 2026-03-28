use crate::agent::core::agent::types::{CompressionEvent, ConversationSummary, Message, Session};
use crate::agent::core::budget::counter::{HeuristicTokenCounter, TokenCounter};
use crate::agent::core::budget::limits::create_budget_for_model;
use crate::agent::core::budget::{BudgetStrategy, TokenBudget};
use crate::agent::core::MessagePhase;
use chrono::Utc;
use std::collections::HashSet;

const CONTEXT_COMPRESSION_LAST_APPLIED_AT_KEY: &str = "context_compression_last_applied_at";
const CONTEXT_COMPRESSION_LAST_APPLIED_USAGE_PCT_KEY: &str =
    "context_compression_last_applied_usage_pct";
const CONTEXT_COMPRESSION_REEXPOSE_MIN_DELTA_PCT: f64 = 5.0;
const CONTEXT_COMPRESSION_CRITICAL_EXPOSE_PERCENT: f64 = 95.0;

/// Metadata about current context pressure, used to decide when to expose
/// the `compress_context` tool to the model.
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

/// Estimate whether context pressure has crossed the configured threshold for
/// manual tool exposure.
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
    let available = budget.available_input_tokens();
    let estimated_usage = if available == 0 {
        0.0
    } else {
        (active_tokens as f64 / available as f64) * 100.0
    };
    let usage = session
        .token_usage
        .as_ref()
        .and_then(|token_usage| {
            (token_usage.budget_limit > 0).then_some(
                (token_usage.total_tokens as f64 / token_usage.budget_limit as f64) * 100.0,
            )
        })
        .map(|persisted_usage| persisted_usage.max(estimated_usage))
        .unwrap_or(estimated_usage);

    let rounded = usage.clamp(0.0, 100.0).round() as u8;
    let trigger = f64::from(budget.compression_trigger_percent);
    let reexpose_delta = session
        .metadata
        .get(CONTEXT_COMPRESSION_LAST_APPLIED_USAGE_PCT_KEY)
        .and_then(|value| value.parse::<f64>().ok())
        .map(|previous| usage - previous)
        .unwrap_or(f64::INFINITY);
    let recently_compressed = session
        .metadata
        .contains_key(CONTEXT_COMPRESSION_LAST_APPLIED_AT_KEY);

    let critical_usage = usage >= CONTEXT_COMPRESSION_CRITICAL_EXPOSE_PERCENT;
    let threshold_reached = usage >= trigger || critical_usage;

    let should_expose_tool = threshold_reached
        && active_messages.len() > 2
        && (critical_usage
            || !recently_compressed
            || reexpose_delta >= CONTEXT_COMPRESSION_REEXPOSE_MIN_DELTA_PCT);

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
) -> Option<CompressionPlan> {
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
/// critically high and compression must be attempted even if hysteresis would
/// normally suppress re-exposure.
pub fn build_forced_compression_plan_with_summary(
    session: &Session,
    model_name: &str,
    configured_budget: Option<&TokenBudget>,
    summary_content: String,
) -> Option<CompressionPlan> {
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
) -> Option<CompressionPlan> {
    let exposure = estimate_context_compression_exposure(session, model_name, configured_budget);
    if require_exposure_gate && !exposure.should_expose_tool {
        return None;
    }

    let budget = &exposure.budget;
    let counter = HeuristicTokenCounter::default();
    let summary_message = compression_summary_message(&summary_content);
    let summary_tokens = counter.count_messages(&[summary_message]);

    let available = budget.available_input_tokens();
    let target_limit = budget.compression_target_input_tokens();

    let mut active_messages = active_messages_for_budget(session);
    if active_messages.is_empty() {
        return None;
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
        return None;
    }

    let anchor_index = non_system
        .iter()
        .rposition(|m| matches!(m.role, crate::agent::core::Role::User))
        .unwrap_or(non_system.len().saturating_sub(1));

    if anchor_index == 0 {
        return None;
    }

    let mut messages_to_summarize = non_system[..anchor_index].to_vec();
    let mut messages_to_keep = non_system[anchor_index..].to_vec();

    while messages_to_keep.len() > 1 {
        let keep_tokens = counter.count_messages(&messages_to_keep);
        if keep_tokens <= window_limit {
            break;
        }
        let moved = messages_to_keep.remove(0);
        messages_to_summarize.push(moved);
    }

    if messages_to_summarize.is_empty() {
        return None;
    }

    let compressed_message_ids = messages_to_summarize
        .iter()
        .map(|message| message.id.clone())
        .collect::<Vec<_>>();

    let keep_tokens = counter.count_messages(&messages_to_keep);
    let active_before = exposure.active_usage_percent;
    let active_after = if available == 0 {
        0.0
    } else {
        let after_total = reserved_non_window_tokens.saturating_add(keep_tokens);
        (after_total as f64 / available as f64) * 100.0
    };

    Some(CompressionPlan {
        compressed_message_ids,
        messages_to_summarize,
        summary_tokens,
        summary_content,
        active_usage_before_percent: active_before,
        active_usage_after_percent: active_after,
        trigger_percent: budget.compression_trigger_percent,
        target_percent: budget.compression_target_percent,
        segments_removed: 1,
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

    let event = CompressionEvent::new(changed_indexes.len(), plan.segments_removed);
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
    session.metadata.remove("responses.previous_response_id");
    session.metadata.insert(
        CONTEXT_COMPRESSION_LAST_APPLIED_AT_KEY.to_string(),
        Utc::now().to_rfc3339(),
    );
    session.metadata.insert(
        CONTEXT_COMPRESSION_LAST_APPLIED_USAGE_PCT_KEY.to_string(),
        format!("{:.2}", plan.active_usage_after_percent),
    );
    session.token_usage = None;
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
        "Critical requirements:\n- Distinguish clearly between ACTIVE work, COMPLETED work, and OBSOLETE or superseded work\n- Do not restate old tasks as active unless they are still unresolved\n- The current task list is the source of truth for what is actively being worked on\n- Preserve constraints, decisions, file paths, code changes, errors, tool findings, blockers, and the next step\n- If earlier plans conflict with the current task list or newer messages, treat the earlier plans as obsolete or completed\n- Return only summary text in the same language as the conversation\n\n",
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
        "## Required Output Sections\n1. Current active objective\n2. Active tasks\n3. Completed tasks\n4. Obsolete or superseded tasks\n5. Important context and constraints\n6. Files, code, and tool findings\n7. Open issues and next step\n\n",
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
    fn estimate_context_compression_exposure_has_reexpose_hysteresis_after_recent_compression() {
        let mut session = make_session_with_pressure();
        let baseline = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );
        let usage_now = baseline.active_usage_percent.max(1.0);
        let trigger = usage_now.floor().clamp(1.0, 100.0) as u8;
        if let Some(budget) = session.token_budget.as_mut() {
            budget.compression_trigger_percent = trigger;
        }
        session.metadata.insert(
            CONTEXT_COMPRESSION_LAST_APPLIED_AT_KEY.to_string(),
            Utc::now().to_rfc3339(),
        );
        session.metadata.insert(
            CONTEXT_COMPRESSION_LAST_APPLIED_USAGE_PCT_KEY.to_string(),
            format!("{:.2}", usage_now - 1.0),
        );

        let exposure = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );

        assert!(
            exposure.active_usage_percent >= f64::from(trigger),
            "setup should still be above trigger (usage={}, trigger={})",
            exposure.active_usage_percent,
            trigger
        );
        assert!(
            !exposure.should_expose_tool,
            "session should not re-expose compression immediately when usage only rose by about 1% after the last compression"
        );
    }

    #[test]
    fn estimate_context_compression_exposure_reexposes_tool_in_critical_usage_band() {
        let mut session = make_session_with_pressure();
        if let Some(budget) = session.token_budget.as_mut() {
            budget.compression_trigger_percent = 99;
        }

        let baseline = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );
        let usage_now = baseline.active_usage_percent.max(95.0);
        session.metadata.insert(
            CONTEXT_COMPRESSION_LAST_APPLIED_AT_KEY.to_string(),
            Utc::now().to_rfc3339(),
        );
        session.metadata.insert(
            CONTEXT_COMPRESSION_LAST_APPLIED_USAGE_PCT_KEY.to_string(),
            format!("{:.2}", usage_now - 1.0),
        );
        session.token_usage = Some(TokenBudgetUsage {
            system_tokens: 120,
            summary_tokens: 0,
            window_tokens: 9_540,
            total_tokens: 9_660,
            max_context_tokens: 10_000,
            budget_limit: 10_000,
            truncation_occurred: true,
            segments_removed: 8,
        });

        let exposure = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );

        assert!(
            exposure.active_usage_percent >= CONTEXT_COMPRESSION_CRITICAL_EXPOSE_PERCENT,
            "setup should be in critical usage band, got {}",
            exposure.active_usage_percent
        );
        assert!(
            exposure.should_expose_tool,
            "critical usage should bypass hysteresis and re-expose compress_context"
        );
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
        assert!(prompt.contains("Active tasks"));
        assert!(prompt.contains("Completed tasks"));
        assert!(prompt.contains("Obsolete or superseded tasks"));
        assert!(prompt.contains("检查 51% 又回落到 50% 的触发逻辑"));
        assert!(prompt.contains("old summary"));
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
        });
        session.add_message(Message::system("system"));
        session.add_message(Message::user("short"));
        session.add_message(Message::assistant("short", None));
        session.token_usage = Some(TokenBudgetUsage {
            system_tokens: 100,
            summary_tokens: 0,
            window_tokens: 9_500,
            total_tokens: 9_600,
            max_context_tokens: 100_000,
            budget_limit: 10_000,
            truncation_occurred: true,
            segments_removed: 12,
        });

        let exposure = estimate_context_compression_exposure(
            &session,
            "gpt-4o-mini",
            session.token_budget.as_ref(),
        );

        assert!(
            exposure.active_usage_percent >= 96.0,
            "expected persisted budget usage to drive exposure, got {}",
            exposure.active_usage_percent
        );
        assert!(exposure.should_expose_tool);
    }
}
