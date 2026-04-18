use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub(super) struct FileChangeCheckpoint {
    #[serde(default)]
    pub(super) created: bool,
    pub(super) path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct FileChangePayload {
    pub(super) file_path: String,
    pub(super) checkpoint: Option<FileChangeCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FileRestoreAction {
    RestoreFromCheckpoint {
        file_path: String,
        checkpoint_path: String,
    },
    DeleteFile {
        file_path: String,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(super) struct FileRestoreError {
    pub(super) file_path: String,
    pub(super) checkpoint_path: Option<String>,
    pub(super) error: String,
}

impl FileRestoreError {
    pub(super) fn new(
        file_path: impl Into<String>,
        checkpoint_path: Option<String>,
        error: impl Into<String>,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            checkpoint_path,
            error: error.into(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct FileRestoreOutcome {
    pub(super) restored_files: usize,
    pub(super) deleted_files: usize,
    pub(super) file_errors: Vec<FileRestoreError>,
}
