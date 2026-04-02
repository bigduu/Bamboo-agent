use super::response::execute_response_payload;
use super::validation::validate_and_normalize_model;
use super::{evaluate_client_sync, ServerExecuteSnapshot};
use crate::server::handlers::agent::execute::{ExecuteClientSync, ExecuteSyncReason};

#[test]
fn validate_and_normalize_model_treats_empty_value_as_absent() {
    assert_eq!(
        validate_and_normalize_model(Some("   ")).expect("empty model should normalize"),
        None
    );
}

#[test]
fn validate_and_normalize_model_trims_whitespace() {
    let model = validate_and_normalize_model(Some(" gpt-4o-mini ")).expect("model should be valid");
    assert_eq!(model.as_deref(), Some("gpt-4o-mini"));
}

#[test]
fn execute_response_payload_formats_status_and_events_url() {
    let payload = execute_response_payload(
        "session-123",
        "started",
        Some(
            ServerExecuteSnapshot {
                message_count: 2,
                last_message_id: Some("msg-2".to_string()),
                has_pending_question: false,
                pending_question_tool_call_id: None,
                has_pending_user_message: true,
            }
            .to_sync_info(None),
        ),
    );
    assert_eq!(payload.session_id, "session-123");
    assert_eq!(payload.status, "started");
    assert_eq!(payload.events_url, "/api/v1/events/session-123");
    assert!(payload.sync.is_some());
}

#[test]
fn evaluate_client_sync_accepts_matching_snapshot() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 3,
        last_message_id: Some("msg-3".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tool-1".to_string()),
        has_pending_user_message: false,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 3,
        client_last_message_id: Some("msg-3".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: Some("tool-1".to_string()),
    };

    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &server_snapshot),
        None
    );
}

#[test]
fn evaluate_client_sync_detects_message_count_mismatch() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 3,
        client_last_message_id: Some("msg-4".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };

    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &server_snapshot),
        Some(ExecuteSyncReason::MessageCountMismatch)
    );
}

#[test]
fn evaluate_client_sync_detects_last_message_id_mismatch() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 4,
        client_last_message_id: Some("msg-3".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };

    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &server_snapshot),
        Some(ExecuteSyncReason::LastMessageIdMismatch)
    );
}

#[test]
fn evaluate_client_sync_detects_pending_question_mismatch() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tool-2".to_string()),
        has_pending_user_message: false,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 4,
        client_last_message_id: Some("msg-4".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: Some("tool-1".to_string()),
    };

    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &server_snapshot),
        Some(ExecuteSyncReason::PendingQuestionMismatch)
    );
}

#[test]
fn evaluate_client_sync_allows_missing_pending_question_tool_call_id() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tool-2".to_string()),
        has_pending_user_message: false,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 4,
        client_last_message_id: Some("msg-4".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: None,
    };

    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &server_snapshot),
        None
    );
}
