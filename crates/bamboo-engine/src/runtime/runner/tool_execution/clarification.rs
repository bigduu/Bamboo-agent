use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::sync::mpsc;

use crate::metrics::MetricsCollector;
use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::session::runtime_state::PlanModeStatus;
use bamboo_domain::TaskItemStatus;
use bamboo_memory::plan_store::{
    PlanCursorArtifact, PlanSectionArtifact, PlanStateArtifact, PlanStore,
};

use super::events::send_event_with_metrics;

mod payload;
mod session_effects;

use payload::{parse_user_question_payload, should_handle_user_question_tool};
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

pub(super) async fn maybe_handle_user_question_tool(
    tool_call: &ToolCall,
    result: &ToolResult,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    config: &AgentLoopConfig,
) -> bool {
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

#[cfg(test)]
mod tests;
