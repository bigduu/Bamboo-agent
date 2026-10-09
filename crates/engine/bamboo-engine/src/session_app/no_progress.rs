//! Human control for a Root run paused after repeated unchanged observations.
//!
//! Uses the existing pending-question and response transaction. The question is
//! an ordinary runtime-authored assistant message, never a fabricated tool call.

use bamboo_agent_core::{Message, PendingQuestion, PendingQuestionSource, Role, Session};
use bamboo_domain::{AgentRuntimeState, AgentStatusState, SessionKind};

pub const QUESTION_TOOL_NAME: &str = "observation_progress";
pub const CONTINUE_OPTION: &str = "Continue";
pub const STOP_OPTION: &str = "Stop";
const QUESTION_KIND: &str = "observation_progress_pause";
pub(crate) const QUESTION_TEXT: &str = "The same successful file observations stayed unchanged for six consecutive rounds, including three rounds after the progress reminder. The run is paused. Continue, stop, or provide a different approach.";

/// Match our own persisted runtime question, not another tool with a similar name.
pub fn is_no_progress_question(session: &Session, pending: &PendingQuestion) -> bool {
    session.kind == SessionKind::Root
        && pending.tool_name == QUESTION_TOOL_NAME
        && pending.source == PendingQuestionSource::AgenticClarification
        && session.messages.iter().any(|message| {
            message.id == pending.tool_call_id
                && message.role == Role::Assistant
                && message.tool_calls.is_none()
                && message
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata["runtime_kind"] == QUESTION_KIND)
        })
}

pub(crate) fn create_question(session: &mut Session, runtime_state: &mut AgentRuntimeState) {
    let mut message = Message::assistant(QUESTION_TEXT, None);
    message.metadata = Some(serde_json::json!({"runtime_kind": QUESTION_KIND}));
    let question_id = message.id.clone();
    session.add_message(message);
    session.set_pending_question_with_source(
        question_id,
        QUESTION_TOOL_NAME.into(),
        QUESTION_TEXT.into(),
        vec![CONTINUE_OPTION.into(), STOP_OPTION.into()],
        true,
        PendingQuestionSource::AgenticClarification,
    );
    session.metadata.insert(
        "runtime.suspend_reason".into(),
        "awaiting_clarification".into(),
    );
    runtime_state.status = AgentStatusState::Suspended;
    runtime_state.suspension = Some(bamboo_domain::SuspensionState {
        reason: "awaiting_clarification".into(),
        suspended_at: chrono::Utc::now(),
        resumable: true,
        hook_point: Some("AfterToolExecution".into()),
    });
    crate::runtime::runner::state_bridge::write_runtime_state(session, runtime_state);
}

/// A normal Human message can steer a paused run through the usual chat/Inbox
/// path. Runtime notifications and peer messages do not answer a Human question.
pub(crate) fn resume_after_user_message(
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
) -> bool {
    let Some(pending) = session.pending_question.as_ref() else {
        return false;
    };
    if !is_no_progress_question(session, pending) {
        return false;
    }
    let Some(question_index) = session
        .messages
        .iter()
        .position(|message| message.id == pending.tool_call_id)
    else {
        return false;
    };
    let has_user_message = session.messages[question_index + 1..]
        .iter()
        .any(|message| {
            if message.role != Role::User
                || bamboo_domain::session::is_system_resume_message(message)
            {
                return false;
            }
            let Some(marker) = message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("session_message"))
            else {
                return true;
            };
            serde_json::from_value::<bamboo_domain::SessionMessageEnvelope>(marker.clone())
                .is_ok_and(|envelope| {
                    let human_input = (envelope.source == bamboo_domain::SessionMessageSource::User
                        && envelope.kind == bamboo_domain::SessionMessageKind::UserInput)
                        || matches!((&envelope.source, &envelope.body),
                            (bamboo_domain::SessionMessageSource::Runtime { subsystem },
                             bamboo_domain::SessionMessageBody::RuntimeInstruction(instruction))
                             if subsystem == "chat" && instruction.instruction == "root_chat_turn_v1"
                                && envelope.kind == bamboo_domain::SessionMessageKind::RuntimeInstruction);
                    human_input && bamboo_domain::is_matching_session_message(message, &envelope)
                })
        });
    if !has_user_message {
        return false;
    }
    session.clear_pending_question();
    session.metadata.remove("runtime.suspend_reason");
    runtime_state.suspension = None;
    runtime_state.status = AgentStatusState::Running;
    crate::runtime::runner::state_bridge::write_runtime_state(session, runtime_state);
    true
}

pub(crate) fn stop_after_response(session: &mut Session) {
    super::execute::consume_pending_clarification_resume(session);
    if let Some(runtime_state) = session.agent_runtime_state.as_mut() {
        runtime_state.status = AgentStatusState::Cancelled;
        runtime_state.suspension = None;
    }
    session.set_last_run_status("cancelled");
    session.clear_last_run_error();
    let mut message =
        Message::assistant("Stopped this run. Send another message to continue.", None);
    message.metadata = Some(serde_json::json!({"runtime_kind": "observation_progress_stop"}));
    session.add_message(message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_user_message_resumes_only_our_own_root_question() {
        for canonical in [false, true] {
            let mut session = Session::new("progress-user", "model");
            let mut runtime = AgentRuntimeState::default();
            create_question(&mut session, &mut runtime);
            let message = if canonical {
                bamboo_domain::SessionMessageEnvelope::user_input(
                    &session.id,
                    "Inspect the other file",
                )
                .to_provider_message()
                .unwrap()
            } else {
                Message::user("Inspect the other file")
            };
            session.add_message(message);
            assert!(resume_after_user_message(&mut session, &mut runtime));
            assert!(session.pending_question.is_none());
            assert!(!session.metadata.contains_key("runtime.suspend_reason"));
            assert_eq!(runtime.status, AgentStatusState::Running);
            assert_eq!(
                session.messages.last().unwrap().content,
                "Inspect the other file"
            );
        }
    }

    #[test]
    fn runtime_notification_does_not_resume_a_progress_question() {
        let mut session = Session::new("progress-notification", "model");
        let mut runtime = AgentRuntimeState::default();
        create_question(&mut session, &mut runtime);
        let mut notification = Message::user("A child finished");
        notification.metadata = Some(
            serde_json::json!({"hidden_from_ui": true, "runtime_kind": "child_completion_resume"}),
        );
        session.add_message(notification);
        assert!(!resume_after_user_message(&mut session, &mut runtime));
        assert!(session.pending_question.is_some());
        assert_eq!(runtime.status, AgentStatusState::Suspended);
    }

    #[test]
    fn ordinary_user_message_does_not_answer_other_pending_questions() {
        let mut session = Session::new("other-question", "model");
        let mut runtime = AgentRuntimeState::default();
        session.set_pending_question(
            "permission".into(),
            "Bash".into(),
            "Approve?".into(),
            vec!["Approve".into()],
            false,
        );
        session.add_message(Message::user("Something else"));
        assert!(!resume_after_user_message(&mut session, &mut runtime));
        assert_eq!(session.pending_question.unwrap().tool_call_id, "permission");
    }
}
