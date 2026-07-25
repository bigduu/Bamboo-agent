use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::session::runtime_state::PlanModeStatus;
use bamboo_domain::TaskItemStatus;
use bamboo_memory::plan_store::{
    PlanCursorArtifact, PlanSectionArtifact, PlanStateArtifact, PlanStore,
};
use bamboo_metrics::MetricsCollector;

use super::events::send_event_with_metrics;

mod payload;
mod session_effects;

use payload::{parse_user_question_payload, should_handle_user_question_tool, UserQuestionPayload};
use session_effects::{
    append_waiting_tool_result_message, emit_need_clarification_event,
    persist_session_after_question,
};

fn stable_plan_hash(plan: &str) -> String {
    let mut hasher = DefaultHasher::new();
    plan.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn resolve_section_id(
    sections: Option<&PlanSectionArtifact>,
    task_id: Option<&str>,
    step_id: Option<&str>,
) -> Option<String> {
    let sections = sections?;

    if let Some(step_id) = step_id {
        if let Some(section) = sections.sections.iter().find(|section| {
            section.id == step_id
                || section
                    .anchor_terms
                    .iter()
                    .any(|term| term.eq_ignore_ascii_case(step_id))
        }) {
            return Some(section.id.clone());
        }
    }

    if let Some(task_id) = task_id {
        if let Some(section) = sections.sections.iter().find(|section| {
            section.id == task_id
                || section
                    .anchor_terms
                    .iter()
                    .any(|term| term.eq_ignore_ascii_case(task_id))
        }) {
            return Some(section.id.clone());
        }
    }

    None
}

fn task_ordinal(session: &Session, task_id: &str) -> Option<u32> {
    let task_list = session.task_list.as_ref()?;
    task_list
        .items
        .iter()
        .position(|item| item.id == task_id)
        .map(|index| index as u32 + 1)
}

fn current_task_id(session: &Session) -> Option<String> {
    let task_list = session.task_list.as_ref()?;
    task_list
        .items
        .iter()
        .find(|item| matches!(item.status, TaskItemStatus::InProgress))
        .or_else(|| {
            task_list
                .items
                .iter()
                .find(|item| matches!(item.status, TaskItemStatus::Pending))
        })
        .map(|item| item.id.clone())
}

fn next_pending_task_id(session: &Session, current_task_id: Option<&str>) -> Option<String> {
    let task_list = session.task_list.as_ref()?;
    task_list
        .items
        .iter()
        .find(|item| {
            matches!(item.status, TaskItemStatus::Pending)
                && current_task_id.is_none_or(|current| item.id != current)
        })
        .map(|item| item.id.clone())
}

fn last_completed_task_id(session: &Session) -> Option<String> {
    let task_list = session.task_list.as_ref()?;
    task_list
        .items
        .iter()
        .rev()
        .find(|item| matches!(item.status, TaskItemStatus::Completed))
        .map(|item| item.id.clone())
}

fn maybe_persist_exit_plan_file(
    session: &mut Session,
    session_id: &str,
    result_payload: &str,
    config: &AgentLoopConfig,
    tool_name: &str,
    round_id: &str,
) -> Option<String> {
    let payload = serde_json::from_str::<serde_json::Value>(result_payload).ok()?;
    let plan = payload
        .get("plan")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let app_data_dir = config.app_data_dir.as_ref()?;
    let store = PlanStore::new(app_data_dir).ok()?;
    let path = store.write_plan(session_id, plan).ok()?;
    let path_string = path.display().to_string();
    let sections = store.read_sections(session_id).ok().flatten();

    let active_task_id = current_task_id(session);
    let next_task_id = next_pending_task_id(session, active_task_id.as_deref());
    let last_completed_task_id = last_completed_task_id(session);

    let active_section_id = resolve_section_id(sections.as_ref(), active_task_id.as_deref(), None);
    let next_section_id = resolve_section_id(sections.as_ref(), next_task_id.as_deref(), None);
    let last_completed_section_id =
        resolve_section_id(sections.as_ref(), last_completed_task_id.as_deref(), None);

    let round_hint = session
        .agent_runtime_state
        .as_ref()
        .map(|runtime_state| runtime_state.round.current_round)
        .filter(|round| *round > 0);
    let round_id_hint = session
        .agent_runtime_state
        .as_ref()
        .and_then(|runtime_state| runtime_state.round.last_round_id.clone())
        .or_else(|| Some(round_id.to_string()));

    let current_task_ordinal = active_task_id
        .as_deref()
        .and_then(|task_id| task_ordinal(session, task_id));
    let next_task_ordinal = next_task_id
        .as_deref()
        .and_then(|task_id| task_ordinal(session, task_id));

    let mut state = store
        .read_state(session_id)
        .ok()
        .flatten()
        .unwrap_or_else(|| PlanStateArtifact::new(session_id));
    state.updated_at = chrono::Utc::now();
    state.status = Some("awaiting_approval".to_string());
    state.active_task_id = active_task_id.clone();
    state.active_section_id = active_section_id.clone();
    state.next_section_id = next_section_id.clone();
    state.last_completed_task_id = last_completed_task_id.clone();
    state.last_completed_section_id = last_completed_section_id.clone();
    state.round_hint = round_hint;
    state.plan_hash = Some(stable_plan_hash(plan));
    let _ = store.write_state(session_id, &state);

    let mut cursor = store
        .read_cursor(session_id)
        .ok()
        .flatten()
        .unwrap_or_else(|| PlanCursorArtifact::new(session_id));
    cursor.updated_at = chrono::Utc::now();
    cursor.cursor_type = Some("task_item".to_string());
    cursor.current_task_id = state.active_task_id.clone();
    cursor.current_task_ordinal = current_task_ordinal;
    cursor.current_section_id = active_section_id;
    cursor.next_task_id = next_task_id;
    cursor.next_task_ordinal = next_task_ordinal;
    cursor.next_section_id = next_section_id;
    cursor.last_completed_task_id = last_completed_task_id;
    cursor.last_completed_section_id = last_completed_section_id;
    cursor.last_completed_checkpoint = Some("before_user_approval".to_string());
    cursor.round_hint = round_hint;
    cursor.round_id_hint = round_id_hint;
    cursor.suspension_hook_point = Some("AfterToolExecution".to_string());
    cursor.tool_call_boundary = Some(tool_name.to_string());
    cursor.resume_note = Some(
        "Resume from the current indexed task/section boundary; if already approved, continue with the next pending task".to_string(),
    );
    let _ = store.write_cursor(session_id, &cursor);

    if let Some(runtime_state) = session.agent_runtime_state.as_mut() {
        if let Some(plan_mode) = runtime_state.plan_mode.as_mut() {
            plan_mode.plan_file_path = Some(path_string.clone());
            plan_mode.status = PlanModeStatus::AwaitingApproval;
        }
    }

    Some(path_string)
}

fn plan_content_summary(result_payload: &str) -> Option<String> {
    let payload = serde_json::from_str::<serde_json::Value>(result_payload).ok()?;
    let plan = payload.get("plan")?.as_str()?.trim();
    if plan.is_empty() {
        return None;
    }
    let summary = plan.replace('\n', " ");
    if summary.chars().count() <= 160 {
        Some(summary)
    } else {
        Some(format!(
            "{}...",
            summary.chars().take(160).collect::<String>()
        ))
    }
}

#[allow(clippy::too_many_arguments)]
/// Suspend the loop for a human decision the tool signalled by returning
/// [`ToolOutcome::NeedsHuman`](bamboo_agent_core::ToolOutcome::NeedsHuman) (Phase
/// B). Unlike [`maybe_handle_user_question_tool`] (which sniffs a `ToolResult`),
/// this is driven by the `PendingQuestion` the tool built. It synthesizes the
/// paired placeholder `tool_result` (so the transcript stays paired now that the
/// tool returns no result), emits the clarification event, sets the pending
/// question, stamps `runtime.suspend_reason=awaiting_clarification`, and
/// persists. The caller marks awaiting-clarification and breaks the round.
///
/// Note: plan-file persistence (ExitPlanMode) and child→parent approval
/// delegation (permission tools) are NOT handled here — those tools stay on the
/// `maybe_handle_user_question_tool` sniff path until they too migrate.
pub(super) async fn suspend_for_pending_question(
    tool_call: &ToolCall,
    pq: bamboo_agent_core::PendingQuestion,
    result: ToolResult,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    config: &AgentLoopConfig,
) {
    // The tool's own result IS the paired tool_result (carries the rich display
    // payload: conclusion / plan / permission data), kept identical to the
    // pre-Phase-B transcript.
    append_waiting_tool_result_message(session, tool_call, &result.result, session_id);

    send_event_with_metrics(
        event_tx,
        metrics_collector,
        session_id,
        round_id,
        AgentEvent::ToolComplete {
            tool_call_id: tool_call.id.clone(),
            result: result.clone(),
        },
    )
    .await;

    let payload = UserQuestionPayload {
        question: pq.question.clone(),
        options: pq.options.clone(),
        allow_custom: pq.allow_custom,
    };
    emit_need_clarification_event(event_tx, &payload, &tool_call.id, &tool_call.function.name)
        .await;

    session.set_pending_question_with_source(
        pq.tool_call_id,
        pq.tool_name,
        pq.question,
        pq.options,
        pq.allow_custom,
        pq.source,
    );
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "awaiting_clarification".to_string(),
    );

    persist_session_after_question(config, session, session_id).await;
}

pub(super) struct UserQuestionToolContext<'a> {
    pub(super) tool_call: &'a ToolCall,
    pub(super) result: &'a ToolResult,
    pub(super) session: &'a mut Session,
    pub(super) event_tx: &'a mpsc::Sender<AgentEvent>,
    pub(super) metrics_collector: Option<&'a MetricsCollector>,
    pub(super) session_id: &'a str,
    pub(super) round_id: &'a str,
    pub(super) config: &'a AgentLoopConfig,
}

pub(super) async fn maybe_handle_user_question_tool(context: UserQuestionToolContext<'_>) -> bool {
    let UserQuestionToolContext {
        tool_call,
        result,
        session,
        event_tx,
        metrics_collector,
        session_id,
        round_id,
        config,
    } = context;

    if !should_handle_user_question_tool(tool_call, result) {
        return false;
    }

    let Some(question_payload) = parse_user_question_payload(&result.result) else {
        return false;
    };

    tracing::info!(
        "[{}] {} called, awaiting user response",
        session_id,
        tool_call.function.name
    );

    let plan_file_path = if bamboo_tools::normalize_tool_ref(&tool_call.function.name).as_deref()
        == Some("ExitPlanMode")
    {
        maybe_persist_exit_plan_file(
            session,
            session_id,
            &result.result,
            config,
            &tool_call.function.name,
            round_id,
        )
    } else {
        None
    };

    append_waiting_tool_result_message(session, tool_call, &result.result, session_id);

    send_event_with_metrics(
        event_tx,
        metrics_collector,
        session_id,
        round_id,
        AgentEvent::ToolComplete {
            tool_call_id: tool_call.id.clone(),
            result: result.clone(),
        },
    )
    .await;

    if let Some(file_path) = plan_file_path {
        let _ = event_tx
            .send(AgentEvent::PlanFileUpdated {
                session_id: session_id.to_string(),
                file_path,
                content_summary: plan_content_summary(&result.result)
                    .unwrap_or_else(|| "Plan file updated".to_string()),
            })
            .await;
    }

    // Child→parent approval delegation (Phase 2): a non-bypassed CHILD cannot
    // answer its own permission prompt (no human is attached to a child session).
    // When a delegate is wired and this is a permission approval, route the
    // request UP to the parent (which surfaces it to the human + persists the
    // mapping) and suspend the child distinctly — do NOT set a human-answerable
    // pending question on the child, which would strand it.
    if try_delegate_child_approval(session, tool_call, &result.result, config)
        .await
        .is_some()
    {
        // ANY delegated outcome routes the decision to the parent and suspends
        // the child here — never a child-side pending question (which a child has
        // no human to answer, so it would strand). Auto-approve/auto-deny
        // fast-paths are resolved on the resume side, not by falling through to
        // the legacy clarification pause.
        tracing::info!(
            "[{}] child gated tool `{}` delegated to parent for approval; suspending child",
            session_id,
            tool_call.function.name
        );
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "awaiting_parent_approval".to_string(),
        );
        persist_session_after_question(config, session, session_id).await;
        return true;
    }

    emit_need_clarification_event(
        event_tx,
        &question_payload,
        &tool_call.id,
        &tool_call.function.name,
    )
    .await;

    session.set_pending_question_with_source(
        tool_call.id.clone(),
        tool_call.function.name.clone(),
        question_payload.question,
        question_payload.options,
        question_payload.allow_custom,
        bamboo_agent_core::PendingQuestionSource::PauseTool,
    );
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "awaiting_clarification".to_string(),
    );

    persist_session_after_question(config, session, session_id).await;

    true
}

/// If a paused tool call is a permission approval AND the executing session is a
/// CHILD with an approval delegate wired, route the request up to the parent.
/// Returns the delegate outcome when delegation was attempted, or `None` to let
/// the caller fall back to the legacy on-session pending-question pause (the
/// session is not a child, no delegate is wired, this is not a permission
/// approval, or the delegate failed).
async fn try_delegate_child_approval(
    session: &Session,
    tool_call: &ToolCall,
    raw_result: &str,
    config: &AgentLoopConfig,
) -> Option<crate::runtime::config::ChildApprovalOutcome> {
    let delegate = config.approval_delegate.as_ref()?;
    let parent_session_id = session.parent_session_id.clone()?;

    let payload: serde_json::Value = serde_json::from_str(raw_result).ok()?;
    if payload.get("status").and_then(|v| v.as_str()) != Some("awaiting_permission_approval") {
        // A non-permission user question (e.g. ExitPlanMode / conclusion) — not
        // an approval to delegate.
        return None;
    }
    let permission_type = payload
        .get("permission_type")
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string())
        })
        .unwrap_or_default();
    let resource = payload
        .get("resource")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let question = payload
        .get("question")
        .and_then(|value| value.as_str())
        .unwrap_or("Permission required")
        .to_string();

    let request = crate::runtime::config::ChildApprovalRequest {
        child_session_id: session.id.clone(),
        parent_session_id,
        child_tool_call_id: tool_call.id.clone(),
        tool_name: tool_call.function.name.clone(),
        permission_type,
        resource,
        question,
        approval_payload: payload,
    };

    match delegate.delegate_child_approval(request).await {
        Ok(outcome) => Some(outcome),
        Err(error) => {
            tracing::warn!(
                "[{}] child approval delegation failed: {}; falling back to local pause",
                session.id,
                error
            );
            None
        }
    }
}

#[cfg(test)]
mod tests;
