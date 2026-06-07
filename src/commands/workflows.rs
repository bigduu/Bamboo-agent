use bamboo_config::paths::workflows_dir;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_workflow_name_accepts_valid_names() {
        assert!(is_safe_workflow_name("my-workflow"));
        assert!(is_safe_workflow_name("workflow_123"));
        assert!(is_safe_workflow_name("My Workflow"));
        assert!(is_safe_workflow_name("test.workflow"));
        assert!(is_safe_workflow_name("workflow (v1)"));
        assert!(is_safe_workflow_name("工作流"));
        assert!(is_safe_workflow_name("a")); // Single character
    }

    #[test]
    fn safe_workflow_name_rejects_empty_string() {
        assert!(!is_safe_workflow_name(""));
    }

    #[test]
    fn safe_workflow_name_rejects_forward_slash() {
        assert!(!is_safe_workflow_name("workflow/name"));
        assert!(!is_safe_workflow_name("/workflow"));
        assert!(!is_safe_workflow_name("workflow/"));
        assert!(!is_safe_workflow_name("/"));
    }

    #[test]
    fn safe_workflow_name_rejects_backslash() {
        assert!(!is_safe_workflow_name("workflow\\name"));
        assert!(!is_safe_workflow_name("\\workflow"));
        assert!(!is_safe_workflow_name("workflow\\"));
        assert!(!is_safe_workflow_name("\\"));
    }

    #[test]
    fn safe_workflow_name_rejects_double_dot() {
        assert!(!is_safe_workflow_name(".."));
        assert!(!is_safe_workflow_name("../workflow"));
        assert!(!is_safe_workflow_name("workflow/.."));
        assert!(!is_safe_workflow_name("workflow..test"));
        assert!(!is_safe_workflow_name("..workflow"));
    }

    #[test]
    fn safe_workflow_name_accepts_single_dot() {
        // Single dots are allowed (e.g., "workflow.md" or "v1.0")
        assert!(is_safe_workflow_name("workflow.md"));
        assert!(is_safe_workflow_name("v1.0"));
        assert!(is_safe_workflow_name(".workflow")); // Leading dot is allowed
        assert!(is_safe_workflow_name("workflow.")); // Trailing dot is allowed
    }

    #[test]
    fn safe_workflow_name_rejects_mixed_dangerous_chars() {
        assert!(!is_safe_workflow_name("../workflow\\test"));
        assert!(!is_safe_workflow_name("..\\workflow/test"));
        assert!(!is_safe_workflow_name("a/b\\c..d"));
    }

    #[test]
    fn safe_workflow_name_accepts_special_characters() {
        // Various safe special characters
        assert!(is_safe_workflow_name("workflow-v1"));
        assert!(is_safe_workflow_name("workflow_v2"));
        assert!(is_safe_workflow_name("workflow (copy)"));
        assert!(is_safe_workflow_name("workflow [test]"));
        assert!(is_safe_workflow_name("workflow {v1}"));
        assert!(is_safe_workflow_name("workflow@2.0"));
        assert!(is_safe_workflow_name("workflow#1"));
        assert!(is_safe_workflow_name("workflow$var"));
        assert!(is_safe_workflow_name("workflow!urgent"));
        assert!(is_safe_workflow_name("workflow?optional"));
        assert!(is_safe_workflow_name("workflow*"));
        assert!(is_safe_workflow_name("workflow+test"));
        assert!(is_safe_workflow_name("workflow=value"));
    }

    #[test]
    fn safe_workflow_name_accepts_unicode() {
        assert!(is_safe_workflow_name("工作流"));
        assert!(is_safe_workflow_name("ワークフロー"));
        assert!(is_safe_workflow_name("العربية"));
        assert!(is_safe_workflow_name("🚀-workflow"));
        assert!(is_safe_workflow_name("workflow-🎯"));
    }

    #[test]
    fn safe_workflow_name_accepts_numbers() {
        assert!(is_safe_workflow_name("123"));
        assert!(is_safe_workflow_name("workflow-123"));
        assert!(is_safe_workflow_name("2024-01-15"));
    }

    #[test]
    fn safe_workflow_name_handles_whitespace() {
        // Whitespace is technically allowed (might be trimmed elsewhere)
        assert!(is_safe_workflow_name("workflow test"));
        assert!(is_safe_workflow_name(" workflow"));
        assert!(is_safe_workflow_name("workflow "));
        assert!(is_safe_workflow_name("\tworkflow"));
    }

    #[test]
    fn safe_workflow_name_long_names() {
        let long_name = "a".repeat(1000);
        assert!(is_safe_workflow_name(&long_name));
    }

    #[tokio::test]
    async fn save_workflow_rejects_unsafe_name_with_slash() {
        let result = save_workflow("test/workflow".to_string(), "content".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn save_workflow_rejects_unsafe_name_with_backslash() {
        let result = save_workflow("test\\workflow".to_string(), "content".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn save_workflow_rejects_unsafe_name_with_double_dot() {
        let result = save_workflow("..workflow".to_string(), "content".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn save_workflow_rejects_empty_name() {
        let result = save_workflow("".to_string(), "content".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn delete_workflow_rejects_unsafe_name_with_slash() {
        let result = delete_workflow("test/workflow".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn delete_workflow_rejects_unsafe_name_with_backslash() {
        let result = delete_workflow("test\\workflow".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn delete_workflow_rejects_unsafe_name_with_double_dot() {
        let result = delete_workflow("..workflow".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn delete_workflow_rejects_empty_name() {
        let result = delete_workflow("".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workflow name"));
    }

    #[tokio::test]
    async fn delete_workflow_fails_for_nonexistent_workflow() {
        let result = delete_workflow("nonexistent-workflow-12345".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Workflow not found"));
    }
}
