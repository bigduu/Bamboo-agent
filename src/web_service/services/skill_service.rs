use crate::web_service::error::AppError;
use std::path::Path;
use tokio::fs;

/// List available workflows from the workflows directory
pub async fn list_workflows(data_dir: &Path) -> Result<Vec<String>, AppError> {
    let workflows_dir = data_dir.join("workflows");

    if !workflows_dir.exists() {
        return Ok(Vec::new());
    }

    let mut workflows = Vec::new();
    let mut entries = fs::read_dir(&workflows_dir).await.map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to read workflows dir: {}", e))
    })?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to read entry: {}", e)))?
    {
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_stem() {
                workflows.push(name.to_string_lossy().to_string());
            }
        }
    }

    workflows.sort();
    Ok(workflows)
}
