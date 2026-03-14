use crate::agent::core::agent::{Message, Role};

use super::models::{FileChangePayload, FileRestoreAction, FileRestoreError};

#[derive(Debug, Default)]
pub(super) struct FileRestorePlan {
    pub(super) actions: Vec<FileRestoreAction>,
    pub(super) initial_errors: Vec<FileRestoreError>,
}

pub(super) fn find_target_message_index(
    messages: &[Message],
    target_message_id: &str,
) -> Option<usize> {
    messages
        .iter()
        .position(|message| message.id == target_message_id)
}

pub(super) fn build_restore_plan(messages_after_target: &[Message]) -> FileRestorePlan {
    let mut plan = FileRestorePlan::default();

    for message in messages_after_target.iter().rev() {
        if !matches!(message.role, Role::Tool) {
            continue;
        }

        let Ok(payload) = serde_json::from_str::<FileChangePayload>(&message.content) else {
            continue;
        };

        let file_path = payload.file_path.trim();
        if file_path.is_empty() {
            continue;
        }

        if let Some(checkpoint) = payload.checkpoint {
            if checkpoint.created {
                let Some(checkpoint_path) = checkpoint.path else {
                    plan.initial_errors.push(FileRestoreError::new(
                        file_path,
                        None,
                        "Checkpoint path missing",
                    ));
                    continue;
                };

                plan.actions.push(FileRestoreAction::RestoreFromCheckpoint {
                    file_path: file_path.to_string(),
                    checkpoint_path,
                });
                continue;
            }
        }

        plan.actions.push(FileRestoreAction::DeleteFile {
            file_path: file_path.to_string(),
        });
    }

    plan
}
