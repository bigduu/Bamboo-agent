//! Workflow integration tests
//!
//! Tests for workflow loading, saving, and management.

#[cfg(test)]
mod tests {
    use bamboo_infrastructure::paths::workflows_dir;
    use std::sync::Mutex;
    use tempfile::TempDir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn test_save_workflow() {
        let temp_dir = TempDir::new().unwrap();

        // Save a test workflow
        let name = "test-workflow".to_string();
        let content = "# Test Workflow\n\nThis is a test workflow.".to_string();

        // Use the async API while the env var is set (guarded by the lock).
        let saved_path = {
            let _guard = ENV_LOCK.lock().expect("env lock");
            let original = std::env::var_os("BAMBOO_DATA_DIR");
            std::env::set_var("BAMBOO_DATA_DIR", temp_dir.path());

            let saved_path = bamboo_agent::commands::save_workflow(name.clone(), content.clone())
                .await
                .unwrap();

            if let Some(val) = original {
                std::env::set_var("BAMBOO_DATA_DIR", val);
            } else {
                std::env::remove_var("BAMBOO_DATA_DIR");
            }

            saved_path
        };

        let workflows = temp_dir.path().join("workflows");
        assert!(saved_path.starts_with(workflows.to_string_lossy().as_ref()));
        assert!(std::path::Path::new(&saved_path).exists());
        let read_back = std::fs::read_to_string(&saved_path).unwrap();
        assert_eq!(read_back, content);
    }

    #[tokio::test]
    async fn test_delete_workflow() {
        let temp_dir = TempDir::new().unwrap();
        let name = "test-workflow-to-delete".to_string();
        let content = "# Workflow To Delete".to_string();

        // Save then delete under a temporary bamboo dir.
        let (saved_path, delete_result) = {
            let _guard = ENV_LOCK.lock().expect("env lock");
            let original = std::env::var_os("BAMBOO_DATA_DIR");
            std::env::set_var("BAMBOO_DATA_DIR", temp_dir.path());

            let saved_path = bamboo_agent::commands::save_workflow(name.clone(), content)
                .await
                .unwrap();
            let delete_result = bamboo_agent::commands::delete_workflow(name.clone()).await;

            if let Some(val) = original {
                std::env::set_var("BAMBOO_DATA_DIR", val);
            } else {
                std::env::remove_var("BAMBOO_DATA_DIR");
            }

            (saved_path, delete_result)
        };

        delete_result.unwrap();
        assert!(!std::path::Path::new(&saved_path).exists());
    }

    #[test]
    fn test_workflow_name_validation() {
        // Valid names
        assert!(is_safe_name("my-workflow"));
        assert!(is_safe_name("test_workflow"));
        assert!(is_safe_name("workflow123"));

        // Invalid names
        assert!(!is_safe_name(""));
        assert!(!is_safe_name("workflow/name"));
        assert!(!is_safe_name("workflow\\name"));
        assert!(!is_safe_name("workflow..name"));
    }

    fn is_safe_name(name: &str) -> bool {
        if name.is_empty() {
            return false;
        }
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            return false;
        }
        true
    }

    #[test]
    fn test_workflows_dir() {
        let dir = workflows_dir();
        assert!(dir.to_string_lossy().ends_with("workflows"));
    }

    #[test]
    fn test_workflow_file_format() {
        let workflow_content = r#"# My Workflow

This is a sample workflow for testing.

## Steps

1. First step
2. Second step
3. Third step
"#;

        assert!(workflow_content.contains("# My Workflow"));
        assert!(workflow_content.contains("## Steps"));
    }
}
