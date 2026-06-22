use bamboo_config::paths::{is_safe_workflow_name, workflows_dir};
use std::fs;

/// Save a workflow to disk
pub async fn save_workflow(name: String, content: String) -> Result<String, String> {
    if !is_safe_workflow_name(&name) {
        return Err("Invalid workflow name".to_string());
    }

    let dir = workflows_dir();
    fs::create_dir_all(&dir).map_err(|e| format!("Failed to create workflows dir: {e}"))?;

    let file_path = dir.join(format!("{name}.md"));
    fs::write(&file_path, content).map_err(|e| format!("Failed to save workflow: {e}"))?;

    Ok(file_path.to_string_lossy().to_string())
}

/// Delete a workflow from disk
pub async fn delete_workflow(name: String) -> Result<(), String> {
    if !is_safe_workflow_name(&name) {
        return Err("Invalid workflow name".to_string());
    }

    let dir = workflows_dir();
    let file_path = dir.join(format!("{name}.md"));

    if !file_path.exists() {
        return Err("Workflow not found".to_string());
    }

    fs::remove_file(&file_path).map_err(|e| format!("Failed to delete workflow: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    // The name validator itself (`is_safe_workflow_name`) now lives in
    // `bamboo_config::paths` and is unit-tested there (#97). These tests cover the
    // command layer: that save/delete reject unsafe names before touching disk.
    use super::*;

    #[tokio::test]
    async fn save_workflow_rejects_unsafe_names() {
        for bad in ["test/workflow", "test\\workflow", "..workflow", "", "wf@#$"] {
            let result = save_workflow(bad.to_string(), "content".to_string()).await;
            assert!(
                result
                    .as_ref()
                    .is_err_and(|e| e.contains("Invalid workflow name")),
                "save_workflow must reject {bad:?}, got {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn delete_workflow_rejects_unsafe_names() {
        for bad in ["test/workflow", "test\\workflow", "..workflow", "", "wf@#$"] {
            let result = delete_workflow(bad.to_string()).await;
            assert!(
                result
                    .as_ref()
                    .is_err_and(|e| e.contains("Invalid workflow name")),
                "delete_workflow must reject {bad:?}, got {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn delete_workflow_fails_for_nonexistent_workflow() {
        let result = delete_workflow("nonexistent-workflow-12345".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Workflow not found"));
    }
}
