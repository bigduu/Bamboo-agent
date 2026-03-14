use std::path::{Path, PathBuf};

use crate::server::error::AppError;

use super::types::RecentWorkspaceStore;

fn workspace_store_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("workspaces").join("recent.json")
}

pub(super) async fn load_recent_store(
    app_data_dir: &Path,
) -> Result<RecentWorkspaceStore, AppError> {
    let path = workspace_store_path(app_data_dir);
    let content = match tokio::fs::read_to_string(&path).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RecentWorkspaceStore::default());
        }
        Err(error) => return Err(AppError::StorageError(error)),
    };
    let store = serde_json::from_str::<RecentWorkspaceStore>(&content)
        .map_err(AppError::SerializationError)?;
    Ok(store)
}

pub(super) async fn save_recent_store(
    app_data_dir: &Path,
    store: &RecentWorkspaceStore,
) -> Result<(), AppError> {
    let path = workspace_store_path(app_data_dir);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let content = serde_json::to_string_pretty(store).map_err(AppError::SerializationError)?;
    tokio::fs::write(&path, content).await?;
    Ok(())
}
