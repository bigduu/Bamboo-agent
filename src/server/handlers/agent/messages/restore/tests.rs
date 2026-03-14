use crate::agent::core::agent::Message;

use super::{
    models::FileRestoreAction,
    planning::{build_restore_plan, find_target_message_index},
};

#[test]
fn find_target_message_index_returns_matching_position() {
    let first = Message::system("system");
    let mut target = Message::user("user");
    target.id = "target-message-id".to_string();
    let third = Message::assistant("assistant", None);

    let messages = vec![first, target, third];
    let target_idx = find_target_message_index(&messages, "target-message-id");

    assert_eq!(target_idx, Some(1));
}

#[test]
fn build_restore_plan_skips_non_tool_and_invalid_messages() {
    let messages_after_target = vec![
        Message::assistant("assistant-tail", None),
        Message::tool_result("tool-1", "not-json"),
        Message::tool_result("tool-2", r#"{"file_path":"   "}"#),
    ];

    let plan = build_restore_plan(&messages_after_target);

    assert!(plan.actions.is_empty());
    assert!(plan.initial_errors.is_empty());
}

#[test]
fn build_restore_plan_collects_actions_in_reverse_order() {
    let messages_after_target = vec![
        Message::tool_result(
            "tool-1",
            r#"{"file_path":"/tmp/older.txt","checkpoint":{"created":true,"path":"/tmp/older.ckpt"}}"#,
        ),
        Message::tool_result("tool-2", r#"{"file_path":"/tmp/newer.txt"}"#),
    ];

    let plan = build_restore_plan(&messages_after_target);

    assert_eq!(plan.actions.len(), 2);
    assert!(plan.initial_errors.is_empty());

    assert_eq!(
        plan.actions[0],
        FileRestoreAction::DeleteFile {
            file_path: "/tmp/newer.txt".to_string()
        }
    );
    assert_eq!(
        plan.actions[1],
        FileRestoreAction::RestoreFromCheckpoint {
            file_path: "/tmp/older.txt".to_string(),
            checkpoint_path: "/tmp/older.ckpt".to_string(),
        }
    );
}

#[test]
fn build_restore_plan_records_missing_checkpoint_path_error() {
    let messages_after_target = vec![Message::tool_result(
        "tool-1",
        r#"{"file_path":"/tmp/file.txt","checkpoint":{"created":true}}"#,
    )];

    let plan = build_restore_plan(&messages_after_target);

    assert!(plan.actions.is_empty());
    assert_eq!(plan.initial_errors.len(), 1);
    assert_eq!(plan.initial_errors[0].file_path, "/tmp/file.txt");
    assert_eq!(plan.initial_errors[0].checkpoint_path, None);
    assert_eq!(plan.initial_errors[0].error, "Checkpoint path missing");
}
