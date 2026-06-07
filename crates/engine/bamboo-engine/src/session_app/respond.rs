//! Respond use case: submit a user response to a pending question.

use bamboo_agent_core::{PendingQuestion, Session};
use bamboo_domain::session::runtime_state::{AgentRuntimeState, PlanModeState, PlanModeStatus};
use chrono::Utc;

use super::errors::RespondError;
use super::provider_model::{derive_model_ref, persist_legacy_model_provider, persist_model_ref};
use super::repository::SessionAccess;
use super::types::RespondInput;

const CLARIFICATION_RESUME_PENDING_KEY: &str = "clarification_resume_pending";
const CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY: &str = "conclusion_with_options_resume_pending";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseSource {
    Human,
    Gold,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanModeTransition {
    Entered {
        reason: Option<String>,
        pre_permission_mode: String,
        entered_at: chrono::DateTime<chrono::Utc>,
        status: PlanModeStatus,
        plan_file_path: Option<String>,
    },
    Exited {
        approved: bool,
        restored_mode: String,
        plan: Option<String>,
    },
}

/// Submit a pending response: load session, validate, update messages,
/// apply plan mode transitions, persist, and return the updated session.
///
/// The caller (handler) is responsible for auto-resume triggering.
pub async fn submit_pending_response(
    repo: &dyn SessionAccess,
    input: RespondInput,
) -> Result<(Session, String, Option<PlanModeTransition>), RespondError> {
    submit_pending_response_with_source(repo, input, ResponseSource::Human).await
}

pub async fn submit_pending_response_with_source(
    repo: &dyn SessionAccess,
    input: RespondInput,
    response_source: ResponseSource,
) -> Result<(Session, String, Option<PlanModeTransition>), RespondError> {
    // ---- Load session (merged for respond to pick up in-memory pending question) ----
    let mut session = repo
        .load_merged(&input.session_id)
        .await?
        .ok_or_else(|| RespondError::NotFound(input.session_id.clone()))?;

    // ---- Take pending question ----
    let pending = session
        .pending_question
        .take()
        .ok_or(RespondError::NoPendingQuestion)?;

    // ---- Validate response ----
    if let Err(error_message) = validate_pending_response(&pending, &input.user_response) {
        // Put the pending question back when validation fails.
        session.pending_question = Some(pending);
        return Err(RespondError::InvalidResponse(error_message));
    }

    let tool_call_id = pending.tool_call_id.clone();
    tracing::debug!(
        "[{}] Looking for tool result message with tool_call_id: {}",
        input.session_id,
        tool_call_id
    );

    let reviewed_plan = extract_exit_plan_from_tool_result_message(&session, &tool_call_id);

    // ---- Update or append tool result message ----
    let found = update_or_append_tool_result_message(
        &mut session,
        &tool_call_id,
        &input.user_response,
        response_source,
    );
    if found {
        tracing::info!(
            "[{}] Updated existing tool result message",
            input.session_id
        );
    } else {
        tracing::warn!(
            "[{}] Tool result message not found for tool_call_id: {}, added fallback message",
            input.session_id,
            tool_call_id
        );
    }

    // ---- Plan mode state transitions ----
    let plan_mode_transition =
        apply_plan_mode_transition(&mut session, &pending, &input.user_response, reviewed_plan);

    // ---- Clear pending question and set resume marker ----
    session.clear_pending_question();
    session.metadata.remove("runtime.suspend_reason");
    session.metadata.insert(
        CLARIFICATION_RESUME_PENDING_KEY.to_string(),
        "true".to_string(),
    );
    session.metadata.insert(
        CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY.to_string(),
        "true".to_string(),
    );

    // ---- Merge model/reasoning from request ----
    let request_model_ref = derive_model_ref(
        input.model_ref.as_ref(),
        input.provider.as_deref(),
        input.model.as_deref(),
    );
    if let Some(model_ref) = request_model_ref.as_ref() {
        persist_model_ref(&mut session, model_ref);
    } else {
        persist_legacy_model_provider(
            &mut session,
            input.model.as_deref(),
            input.provider.as_deref(),
        );
    }
    if let Some(reasoning_effort) = input.reasoning_effort {
        session.reasoning_effort = Some(reasoning_effort);
    }

    // ---- Save ----
    repo.save_and_cache(&mut session).await?;

    tracing::info!(
        "[{}] Response processed successfully, agent loop can resume",
        input.session_id
    );

    Ok((session, input.user_response, plan_mode_transition))
}

/// Apply plan mode state transitions based on the pending question tool and user response.
fn apply_plan_mode_transition(
    session: &mut Session,
    pending: &PendingQuestion,
    user_response: &str,
    reviewed_plan: Option<String>,
) -> Option<PlanModeTransition> {
    match pending.tool_name.as_str() {
        "EnterPlanMode" if user_response.to_lowercase().contains("enter plan mode") => {
            let pre_mode = session
                .agent_runtime_state
                .as_ref()
                .and_then(|s| s.plan_mode.as_ref())
                .map(|p| p.pre_permission_mode.clone())
                .unwrap_or_else(|| "default".to_string());

            let entered_at = Utc::now();
            let status = PlanModeStatus::Exploring;
            let runtime_state = session
                .agent_runtime_state
                .get_or_insert_with(|| AgentRuntimeState::new(uuid::Uuid::new_v4().to_string()));
            runtime_state.plan_mode = Some(PlanModeState {
                entered_at,
                pre_permission_mode: pre_mode.clone(),
                plan_file_path: None,
                status,
            });
            tracing::info!(
                session_id = %session.id,
                "Entered plan mode"
            );
            Some(PlanModeTransition::Entered {
                reason: Some(pending.question.clone()),
                pre_permission_mode: pre_mode,
                entered_at,
                status,
                plan_file_path: None,
            })
        }
        "ExitPlanMode" if is_exit_plan_mode_approved(user_response) => {
            let restored_mode = session
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.plan_mode.as_ref())
                .map(|plan| plan.pre_permission_mode.clone())
                .unwrap_or_else(|| "default".to_string());
            if let Some(ref mut runtime_state) = session.agent_runtime_state {
                runtime_state.plan_mode = None;
            }
            tracing::info!(
                session_id = %session.id,
                "Exited plan mode"
            );
            Some(PlanModeTransition::Exited {
                approved: true,
                restored_mode,
                plan: reviewed_plan,
            })
        }
        _ => None,
    }
}

/// Check if the user response approves exiting plan mode.
fn is_exit_plan_mode_approved(user_response: &str) -> bool {
    let lower = user_response.to_lowercase();
    lower.contains("approve") && !lower.contains("stay in plan mode")
}

// ---- Internal helpers ----

pub fn validate_pending_response(
    pending: &PendingQuestion,
    user_response: &str,
) -> Result<(), String> {
    if pending.allow_custom {
        return Ok(());
    }

    let valid = pending.options.iter().any(|option| option == user_response);
    if valid {
        Ok(())
    } else {
        let options_str = pending.options.join(", ");
        Err(format!("Response must be one of: {options_str}"))
    }
}

pub fn update_or_append_tool_result_message(
    session: &mut Session,
    tool_call_id: &str,
    user_response: &str,
    response_source: ResponseSource,
) -> bool {
    for message in &mut session.messages {
        if message.tool_call_id.as_deref() == Some(tool_call_id) {
            message.content = selected_message_content(user_response, response_source);
            message.tool_success = Some(true);
            return true;
        }
    }

    session.add_message(bamboo_agent_core::Message::tool_result_with_status(
        tool_call_id,
        selected_message_content(user_response, response_source),
        true,
    ));
    false
}

fn selected_message_content(user_response: &str, response_source: ResponseSource) -> String {
    match response_source {
        ResponseSource::Human => format!("Selected response: {}", user_response),
        ResponseSource::Gold => format!("Auto-selected response (gold): {}", user_response),
    }
}

fn extract_exit_plan_from_tool_result_message(
    session: &Session,
    tool_call_id: &str,
) -> Option<String> {
    let message = session
        .messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some(tool_call_id))?;
    let payload = serde_json::from_str::<serde_json::Value>(&message.content).ok()?;
    payload
        .get("plan")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pending(tool_name: &str) -> PendingQuestion {
        PendingQuestion {
            tool_call_id: "call-1".to_string(),
            tool_name: tool_name.to_string(),
            question: "Question?".to_string(),
            options: vec!["A".to_string(), "B".to_string()],
            allow_custom: false,
            source: bamboo_agent_core::PendingQuestionSource::PauseTool,
        }
    }

    #[test]
    fn enter_plan_mode_activates_plan_mode_state() {
        let mut session = Session::new("sess-1", "test-model");
        let pending = make_pending("EnterPlanMode");

        apply_plan_mode_transition(&mut session, &pending, "Enter plan mode", None);

        assert!(session.agent_runtime_state.is_some());
        let state = session.agent_runtime_state.unwrap();
        assert!(state.plan_mode.is_some());
        let plan = state.plan_mode.unwrap();
        assert_eq!(plan.status, PlanModeStatus::Exploring);
        assert_eq!(plan.pre_permission_mode, "default");
    }

    #[test]
    fn enter_plan_mode_does_nothing_when_not_approved() {
        let mut session = Session::new("sess-1", "test-model");
        let pending = make_pending("EnterPlanMode");

        apply_plan_mode_transition(&mut session, &pending, "Stay in normal mode", None);

        assert!(session.agent_runtime_state.is_none());
    }

    #[test]
    fn exit_plan_mode_clears_plan_mode_state() {
        let mut session = Session::new("sess-1", "test-model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::AwaitingApproval,
        });
        let pending = make_pending("ExitPlanMode");

        apply_plan_mode_transition(
            &mut session,
            &pending,
            "Approve (Default mode)",
            Some("Reviewed plan".to_string()),
        );

        assert!(session.agent_runtime_state.unwrap().plan_mode.is_none());
    }

    #[test]
    fn exit_plan_mode_keeps_plan_mode_when_not_approved() {
        let mut session = Session::new("sess-1", "test-model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::AwaitingApproval,
        });
        let pending = make_pending("ExitPlanMode");

        apply_plan_mode_transition(&mut session, &pending, "Stay in plan mode", None);

        assert!(session.agent_runtime_state.unwrap().plan_mode.is_some());
    }

    #[test]
    fn exit_plan_mode_ignores_other_tools() {
        let mut session = Session::new("sess-1", "test-model");
        let pending = make_pending("ConclusionWithOptions");

        apply_plan_mode_transition(&mut session, &pending, "Approve", None);

        assert!(session.agent_runtime_state.is_none());
    }

    #[test]
    fn is_exit_plan_mode_approved_detects_approval() {
        assert!(is_exit_plan_mode_approved("Approve (Default mode)"));
        assert!(is_exit_plan_mode_approved("Approve (Accept edits mode)"));
        assert!(!is_exit_plan_mode_approved("Stay in plan mode"));
        assert!(!is_exit_plan_mode_approved("Edit plan first"));
    }

    #[test]
    fn extract_exit_plan_from_tool_result_message_reads_plan_payload() {
        let mut session = Session::new("sess-1", "test-model");
        let mut tool_message = bamboo_agent_core::Message::tool_result(
            "call-1",
            serde_json::json!({
                "plan": "# Plan\n\n1. Step"
            })
            .to_string(),
        );
        tool_message.tool_success = Some(true);
        session.add_message(tool_message);

        let plan = extract_exit_plan_from_tool_result_message(&session, "call-1");
        assert_eq!(plan.as_deref(), Some("# Plan\n\n1. Step"));
    }
}
