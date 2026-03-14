use std::{io::ErrorKind, path::Path};

use super::{
    models::{FileRestoreAction, FileRestoreError, FileRestoreOutcome},
    planning::FileRestorePlan,
};

pub(super) async fn apply_restore_plan(plan: FileRestorePlan) -> FileRestoreOutcome {
    let mut outcome = FileRestoreOutcome {
        file_errors: plan.initial_errors,
        ..FileRestoreOutcome::default()
    };

    for action in plan.actions {
        match action {
            FileRestoreAction::RestoreFromCheckpoint {
                file_path,
                checkpoint_path,
            } => {
                restore_from_checkpoint(&file_path, &checkpoint_path, &mut outcome).await;
            }
            FileRestoreAction::DeleteFile { file_path } => {
                delete_file_if_needed(&file_path, &mut outcome).await;
            }
        }
    }

    outcome
}

async fn restore_from_checkpoint(
    file_path: &str,
    checkpoint_path: &str,
    outcome: &mut FileRestoreOutcome,
) {
    let checkpoint_path_buf = Path::new(checkpoint_path);
    let file_path_buf = Path::new(file_path);

    let bytes = match tokio::fs::read(checkpoint_path_buf).await {
        Ok(data) => data,
        Err(error) => {
            outcome.file_errors.push(FileRestoreError::new(
                file_path,
                Some(checkpoint_path.to_string()),
                format!("Failed to read checkpoint: {error}"),
            ));
            return;
        }
    };

    if let Some(parent) = file_path_buf.parent() {
        if let Err(error) = tokio::fs::create_dir_all(parent).await {
            outcome.file_errors.push(FileRestoreError::new(
                file_path,
                Some(checkpoint_path.to_string()),
                format!("Failed to create parent directory: {error}"),
            ));
            return;
        }
    }

    if let Err(error) = tokio::fs::write(file_path_buf, bytes).await {
        outcome.file_errors.push(FileRestoreError::new(
            file_path,
            Some(checkpoint_path.to_string()),
            format!("Failed to restore file content: {error}"),
        ));
        return;
    }

    outcome.restored_files += 1;
}

async fn delete_file_if_needed(file_path: &str, outcome: &mut FileRestoreOutcome) {
    match tokio::fs::metadata(file_path).await {
        Ok(metadata) => {
            if metadata.is_file() {
                if let Err(error) = tokio::fs::remove_file(file_path).await {
                    outcome.file_errors.push(FileRestoreError::new(
                        file_path,
                        None,
                        format!("Failed to delete file: {error}"),
                    ));
                    return;
                }
                outcome.deleted_files += 1;
            } else {
                outcome.file_errors.push(FileRestoreError::new(
                    file_path,
                    None,
                    "Path is not a file",
                ));
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            outcome.file_errors.push(FileRestoreError::new(
                file_path,
                None,
                format!("Failed to inspect file: {error}"),
            ));
        }
    }
}
