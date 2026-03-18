use crate::server::error::AppError;
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

#[cfg(test)]
mod tests {
    use super::list_workflows;
    use crate::server::error::AppError;
    use tempfile::tempdir;
    use tokio::fs;

    #[tokio::test]
    async fn list_workflows_returns_empty_when_workflows_dir_missing() {
        let tmp = tempdir().expect("create temp dir");

        let workflows = list_workflows(tmp.path())
            .await
            .expect("list workflows should succeed");

        assert!(workflows.is_empty());
    }

    #[tokio::test]
    async fn list_workflows_returns_sorted_file_stems_and_ignores_directories() {
        let tmp = tempdir().expect("create temp dir");
        let workflows_dir = tmp.path().join("workflows");
        fs::create_dir_all(&workflows_dir)
            .await
            .expect("create workflows dir");

        fs::write(workflows_dir.join("zeta.yaml"), "# zeta")
            .await
            .expect("write zeta");
        fs::write(workflows_dir.join("alpha.md"), "# alpha")
            .await
            .expect("write alpha");
        fs::write(workflows_dir.join("bravo"), "# bravo")
            .await
            .expect("write bravo");
        fs::create_dir_all(workflows_dir.join("nested"))
            .await
            .expect("create nested dir");

        let workflows = list_workflows(tmp.path())
            .await
            .expect("list workflows should succeed");

        assert_eq!(workflows, vec!["alpha", "bravo", "zeta"]);
    }

    #[tokio::test]
    async fn list_workflows_returns_internal_error_when_workflows_path_is_file() {
        let tmp = tempdir().expect("create temp dir");
        let workflows_path = tmp.path().join("workflows");
        fs::write(&workflows_path, "not a directory")
            .await
            .expect("write file at workflows path");

        let error = list_workflows(tmp.path())
            .await
            .expect_err("listing should fail when workflows path is not a directory");

        match error {
            AppError::InternalError(inner) => {
                assert!(
                    inner.to_string().contains("Failed to read workflows dir"),
                    "unexpected error message: {inner}"
                );
            }
            other => panic!("expected InternalError, got {other:?}"),
        }
    }
}
