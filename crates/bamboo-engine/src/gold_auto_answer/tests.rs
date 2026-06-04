//! Pure unit tests for Gold auto-answer decision helpers.
//!
//! The full-loop integration tests (which construct a server `AppState` and the
//! `AppStateResumeRef` adapter) live in the `bamboo-server` crate.

use super::decision::{
    canonicalize_pending_answer, parse_gold_auto_answer_decision,
    session_is_awaiting_clarification, should_attempt_gold_auto_answer,
};
use super::GOLD_AUTO_ANSWER_TOOL_NAME;
use bamboo_agent_core::{
    FunctionCall, GoldConfidence, PendingQuestion, PendingQuestionSource, Session, ToolCall,
};
use serde_json::json;

fn pending_question(tool_name: &str) -> PendingQuestion {
    PendingQuestion {
        tool_call_id: format!("call::{tool_name}"),
        tool_name: tool_name.to_string(),
        question: "Question?".to_string(),
        options: vec!["OK".to_string(), "Need changes".to_string()],
        allow_custom: true,
        source: PendingQuestionSource::PauseTool,
    }
}

fn auto_answer_call(arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: "call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: GOLD_AUTO_ANSWER_TOOL_NAME.to_string(),
            arguments: arguments.to_string(),
        },
    }
}

#[test]
fn should_attempt_gold_auto_answer_allows_exit_plan_mode() {
    assert!(should_attempt_gold_auto_answer(&pending_question(
        "ExitPlanMode"
    )));
}

#[test]
fn should_attempt_gold_auto_answer_allows_conclusion_with_options() {
    assert!(should_attempt_gold_auto_answer(&pending_question(
        "conclusion_with_options"
    )));
}

#[test]
fn should_attempt_gold_auto_answer_rejects_request_permissions() {
    assert!(!should_attempt_gold_auto_answer(&pending_question(
        "request_permissions"
    )));
}

#[test]
fn should_attempt_gold_auto_answer_rejects_gold_origin_questions() {
    let mut pending = pending_question("conclusion_with_options");
    pending.source = PendingQuestionSource::Gold;
    assert!(!should_attempt_gold_auto_answer(&pending));
}

#[test]
fn parse_gold_auto_answer_decision_reads_structured_fields() {
    let parsed = parse_gold_auto_answer_decision(&[auto_answer_call(json!({
        "apply": true,
        "answer": "OK",
        "confidence": "high",
        "reasoning": "The option is explicitly supported"
    }))])
    .expect("decision should parse");

    assert!(parsed.apply);
    assert_eq!(parsed.answer.as_deref(), Some("OK"));
    assert_eq!(parsed.confidence, GoldConfidence::High);
    assert_eq!(parsed.reasoning, "The option is explicitly supported");
}

#[test]
fn canonicalize_pending_answer_uses_existing_option_casing() {
    let pending = pending_question("conclusion_with_options");
    let answer = canonicalize_pending_answer(&pending, "ok.");
    assert_eq!(answer.as_deref(), Some("OK"));
}

#[test]
fn canonicalize_pending_answer_rejects_free_text() {
    let pending = pending_question("conclusion_with_options");
    let answer = canonicalize_pending_answer(&pending, "Ship it");
    assert!(answer.is_none());
}

#[test]
fn session_is_awaiting_clarification_checks_metadata_reason() {
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "awaiting_clarification".to_string(),
    );
    assert!(session_is_awaiting_clarification(&session));
}
