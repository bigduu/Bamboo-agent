use super::*;
use crate::session_app::no_progress::{create_question, STOP_OPTION};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};

fn paused_session() -> Session {
    let mut session = Session::new("progress-response", "model");
    session.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "real-read".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: r#"{"path":"file"}"#.into(),
            },
        }]),
    ));
    session.add_message(Message::tool_result_with_status(
        "real-read",
        "unchanged evidence",
        true,
    ));
    create_question(&mut session, &mut AgentRuntimeState::default());
    session
}

fn input(response: &str) -> RespondInput {
    RespondInput {
        session_id: "progress-response".into(),
        user_response: response.into(),
        model: None,
        model_ref: None,
        provider: None,
        reasoning_effort: None,
    }
}

#[test]
fn no_progress_continue_and_custom_direction_preserve_tool_evidence() {
    for response in ["Continue", "Inspect a different file and finish"] {
        let mut session = paused_session();
        let evidence = serde_json::to_value(&session.messages[..2]).unwrap();
        let id = session
            .pending_question
            .as_ref()
            .unwrap()
            .tool_call_id
            .clone();
        let (_, transition, grants) = apply_pending_response(
            &mut session,
            &input(response),
            Some(&id),
            ResponseSource::Human,
            None,
        )
        .unwrap();
        assert!(transition.is_none() && grants.is_empty());
        assert_eq!(
            serde_json::to_value(&session.messages[..2]).unwrap(),
            evidence
        );
        assert!(session.pending_question.is_none());
        assert!(super::super::execute::has_pending_clarification_resume(
            &session
        ));
        assert_eq!(
            session.messages.last().unwrap().role,
            bamboo_agent_core::Role::User
        );
        assert_eq!(session.messages.last().unwrap().content, response);
        let runtime_kind = session
            .messages
            .last()
            .unwrap()
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("runtime_kind"))
            .and_then(|kind| kind.as_str());
        assert_eq!(
            runtime_kind,
            (response == "Continue").then_some("no_progress_continue")
        );
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.tool_call_id.is_some())
                .count(),
            1
        );
    }
}

#[test]
fn no_progress_stop_consumes_question_without_resumable_work() {
    let mut session = paused_session();
    let id = session
        .pending_question
        .as_ref()
        .unwrap()
        .tool_call_id
        .clone();
    apply_pending_response(
        &mut session,
        &input(STOP_OPTION),
        Some(&id),
        ResponseSource::Human,
        None,
    )
    .unwrap();
    assert!(session.pending_question.is_none());
    assert!(!super::super::execute::has_pending_user_message(&session));
    assert_eq!(session.last_run_status().as_deref(), Some("cancelled"));
    assert_eq!(
        session.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Cancelled
    );
    assert!(!session.metadata.contains_key("runtime.suspend_reason"));
    assert_eq!(session.messages[1].content, "unchanged evidence");
    assert_eq!(session.messages.len(), 5);
}

#[test]
fn no_progress_response_rejects_stale_identity_and_gold() {
    for (expected, source) in [
        ("stale", ResponseSource::Human),
        ("current", ResponseSource::Gold),
    ] {
        let mut session = paused_session();
        let id = session
            .pending_question
            .as_ref()
            .unwrap()
            .tool_call_id
            .clone();
        let expected = if expected == "current" {
            id.as_str()
        } else {
            expected
        };
        let before = serde_json::to_value(&session).unwrap();
        assert!(apply_pending_response(
            &mut session,
            &input("Continue"),
            Some(expected),
            source,
            None
        )
        .is_err());
        assert_eq!(serde_json::to_value(session).unwrap(), before);
    }
}

#[test]
fn no_progress_preflight_id_cannot_stop_a_replacement_question() {
    for another_progress_question in [false, true] {
        let mut session = paused_session();
        let bound_preflight_id = session
            .pending_question
            .as_ref()
            .unwrap()
            .tool_call_id
            .clone();
        if another_progress_question {
            create_question(&mut session, &mut AgentRuntimeState::default());
        } else {
            session.set_pending_question(
                "replacement-tool".into(),
                "conclusion_with_options".into(),
                "Choose next action".into(),
                vec!["Stop".into()],
                true,
            );
        }
        let before = serde_json::to_value(&session).unwrap();
        assert!(matches!(
            apply_pending_response(
                &mut session,
                &input("Stop"),
                Some(&bound_preflight_id),
                ResponseSource::Human,
                None
            ),
            Err(RespondError::PendingQuestionMismatch { .. })
        ));
        assert_eq!(
            serde_json::to_value(session).unwrap(),
            before,
            "the replacement question and its evidence remain unconsumed"
        );
    }
}
