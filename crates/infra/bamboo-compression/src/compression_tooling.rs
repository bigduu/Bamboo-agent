use crate::counter::{TiktokenTokenCounter, TokenCounter};
use crate::limits::{create_budget_for_model, ModelLimitsRegistry};
use crate::{BudgetStrategy, TokenBudget};
use bamboo_domain::MessagePhase;
use bamboo_domain::{
    CompressionEvent, CompressionTriggerType, ConversationSummary, Message, Session,
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
    pub trigger_type: CompressionTriggerType,
    pub compression_ratio: f64,
    pub model_used: Option<String>,
    pub latency_ms: u64,
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
    // `resolve_token_budget`, which now also caches it on `session.token_budget`,
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
        trigger_type,
        compression_ratio: 0.0,
        model_used: None,
        latency_ms: 0,
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

    let event = CompressionEvent::new(
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
    // preserve the context-window denominator from the previous usage snapshot
    // so percentages stay consistent across rounds.
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
    use bamboo_domain::TokenBudgetUsage;
    use bamboo_domain::{FunctionCall, TaskItem, TaskItemStatus, TaskList, ToolCall};
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
}
