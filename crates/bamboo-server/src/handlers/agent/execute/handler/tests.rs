use super::response::execute_response_payload;
use super::validation::validate_and_normalize_model;
use crate::handlers::agent::execute::{ExecuteClientSync, ExecuteSyncInfo, ExecuteSyncReason};

use crate::session_app::types::ServerExecuteSnapshot;

fn evaluate_client_sync_adapter(
    client_sync: Option<&ExecuteClientSync>,
    server_snapshot: &ServerExecuteSnapshot,
) -> Option<ExecuteSyncReason> {
    let crate_sync = client_sync.map(|cs| crate::session_app::types::ExecuteClientSync {
        client_message_count: cs.client_message_count,
        client_last_message_id: cs.client_last_message_id.clone(),
        client_has_pending_question: cs.client_has_pending_question,
        client_pending_question_tool_call_id: cs.client_pending_question_tool_call_id.clone(),
    });

    crate::session_app::execute::evaluate_client_sync(crate_sync.as_ref(), server_snapshot)
        .map(|reason| match reason {
            crate::session_app::types::ExecuteSyncReason::PendingQuestionMismatch => {
                ExecuteSyncReason::PendingQuestionMismatch
            }
            crate::session_app::types::ExecuteSyncReason::MessageCountMismatch => {
                ExecuteSyncReason::MessageCountMismatch
            }
            crate::session_app::types::ExecuteSyncReason::LastMessageIdMismatch => {
                ExecuteSyncReason::LastMessageIdMismatch
            }
        })
}

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
        Some(ExecuteSyncInfo {
            need_sync: false,
            reason: None,
            server_message_count: 2,
            server_last_message_id: Some("msg-2".to_string()),
            has_pending_question: false,
            pending_question_tool_call_id: None,
            has_pending_user_message: true,
        }),
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
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
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
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
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
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
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
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
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
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
        None
    );
}
