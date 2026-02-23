use crate::core::paths::workflows_dir;
use std::fs;

fn is_safe_workflow_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return false;
    }
    true
}

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
