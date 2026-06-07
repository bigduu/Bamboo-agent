use std::path::PathBuf;

use crate::handlers::workspace::types::WorkspaceInfo;

pub(super) async fn build_workspace_info(path: &str) -> WorkspaceInfo {
    let workspace_name = workspace_name_from_path(path);

    let metadata = tokio::fs::metadata(path).await;
    match metadata {
        Ok(meta) => {
            if !meta.is_dir() {
                return WorkspaceInfo {
                    path: path.to_string(),
                    is_valid: false,
                    error_message: Some("Not a directory".to_string()),
                    file_count: None,
                    last_modified: None,
                    size_bytes: None,
                    workspace_name,
                };
            }

            let mut count = 0_u64;
            if let Ok(mut entries) = tokio::fs::read_dir(path).await {
                while let Ok(Some(_)) = entries.next_entry().await {
                    count += 1;
                }
            }

            WorkspaceInfo {
                path: path.to_string(),
                is_valid: true,
                error_message: None,
                file_count: Some(count),
                last_modified: None,
                size_bytes: None,
                workspace_name,
            }
        }
        Err(error) => WorkspaceInfo {
            path: path.to_string(),
            is_valid: false,
            error_message: Some(error.to_string()),
            file_count: None,
            last_modified: None,
            size_bytes: None,
            workspace_name,
        },
    }
}

pub(super) fn workspace_name_from_path(path: &str) -> Option<String> {
    PathBuf::from(path)
        .file_name()
        .and_then(|segment| segment.to_str())
        .map(ToString::to_string)
}
