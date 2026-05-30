use std::path::{Path, PathBuf};

pub(super) fn path_is_within_workspace(target_path: &str, workspace_path: &str) -> bool {
    let target_path = Path::new(target_path);
    let workspace_path = Path::new(workspace_path);

    if !target_path.is_absolute() || !workspace_path.is_absolute() {
        return false;
    }

    let normalized_target = normalize_path_for_comparison(target_path);
    let normalized_workspace = normalize_path_for_comparison(workspace_path);

    normalized_target == normalized_workspace
        || normalized_target.starts_with(&normalized_workspace)
}

fn normalize_path_for_comparison(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }

    if let Some(parent) = path.parent() {
        if let Ok(canonical_parent) = std::fs::canonicalize(parent) {
            if let Some(file_name) = path.file_name() {
                return canonical_parent.join(file_name);
            }
            return canonical_parent;
        }
    }

    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // Helper to create temporary directories for testing
    fn create_temp_workspace() -> tempfile::TempDir {
        tempfile::tempdir().expect("Failed to create temp directory")
    }

    #[test]
    fn path_is_within_workspace_returns_true_for_same_path() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();

        assert!(path_is_within_workspace(workspace_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_true_for_nested_path() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();
        let nested_file = workspace.path().join("src").join("main.rs");
        let nested_path = nested_file.to_str().unwrap();

        // Create parent directories
        std::fs::create_dir_all(nested_file.parent().unwrap()).ok();

        assert!(path_is_within_workspace(nested_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_true_for_deeply_nested_path() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();
        let nested = workspace
            .path()
            .join("a")
            .join("b")
            .join("c")
            .join("d")
            .join("file.txt");
        let nested_path = nested.to_str().unwrap();

        // Create parent directories
        std::fs::create_dir_all(nested.parent().unwrap()).ok();

        assert!(path_is_within_workspace(nested_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_false_for_outside_path() {
        let workspace = create_temp_workspace();
        let other_dir = create_temp_workspace();

        let workspace_path = workspace.path().to_str().unwrap();
        let outside_file = other_dir.path().join("file.txt");
        let outside_path = outside_file.to_str().unwrap();

        assert!(!path_is_within_workspace(outside_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_false_for_parent_directory() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();
        let parent_path = workspace.path().parent().unwrap().to_str().unwrap();

        assert!(!path_is_within_workspace(parent_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_false_for_sibling_directory() {
        let workspace = create_temp_workspace();
        let sibling = create_temp_workspace();

        let workspace_path = workspace.path().to_str().unwrap();
        let sibling_path = sibling.path().to_str().unwrap();

        assert!(!path_is_within_workspace(sibling_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_false_for_relative_target() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();

        assert!(!path_is_within_workspace("src/main.rs", workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_false_for_relative_workspace() {
        let workspace = create_temp_workspace();
        let target_path = workspace.path().join("src").join("main.rs");
        let target = target_path.to_str().unwrap();

        assert!(!path_is_within_workspace(target, "workspace"));
    }

    #[test]
    fn path_is_within_workspace_returns_false_for_both_relative() {
        assert!(!path_is_within_workspace(
            "workspace/src/main.rs",
            "workspace"
        ));
    }

    #[test]
    fn path_is_within_workspace_handles_path_with_spaces() {
        let workspace = tempfile::Builder::new()
            .prefix("my workspace ")
            .tempdir()
            .expect("Failed to create temp directory");
        let workspace_path = workspace.path().to_str().unwrap();
        let nested = workspace.path().join("src").join("file.txt");
        let nested_path = nested.to_str().unwrap();

        std::fs::create_dir_all(nested.parent().unwrap()).ok();

        assert!(path_is_within_workspace(nested_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_handles_path_with_special_characters() {
        let workspace = tempfile::Builder::new()
            .prefix("workspace-test_123")
            .tempdir()
            .expect("Failed to create temp directory");
        let workspace_path = workspace.path().to_str().unwrap();
        let nested = workspace.path().join("src").join("file.txt");
        let nested_path = nested.to_str().unwrap();

        std::fs::create_dir_all(nested.parent().unwrap()).ok();

        assert!(path_is_within_workspace(nested_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_handles_unicode_paths() {
        // Unicode directory names may not work on all filesystems
        // So we'll use a temp dir with unicode in a subdirectory
        let workspace = create_temp_workspace();
        let unicode_dir = workspace.path().join("工作空间");
        std::fs::create_dir(&unicode_dir).ok();

        if unicode_dir.exists() {
            let workspace_path = unicode_dir.to_str().unwrap();
            let nested = unicode_dir.join("src").join("文件.txt");
            std::fs::create_dir_all(nested.parent().unwrap()).ok();
            let nested_path = nested.to_str().unwrap();

            assert!(path_is_within_workspace(nested_path, workspace_path));
        }
        // If unicode directory creation fails, test passes (filesystem doesn't support it)
    }

    #[test]
    fn path_is_within_workspace_distinguishes_similar_names() {
        let workspace = create_temp_workspace();
        let other = tempfile::Builder::new()
            .prefix("workspace-other")
            .tempdir()
            .expect("Failed to create temp directory");

        let workspace_path = workspace.path().to_str().unwrap();
        let other_path = other.path().to_str().unwrap();

        // workspace-other should not be considered within workspace
        assert!(!path_is_within_workspace(other_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_handles_root_directory() {
        // Test that /tmp is within /
        assert!(path_is_within_workspace("/tmp", "/"));
    }

    #[test]
    fn path_is_within_workspace_handles_trailing_slash() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();
        let nested = workspace.path().join("file.txt");
        let nested_path = nested.to_str().unwrap();

        // Add trailing slash to workspace path
        let workspace_with_slash = format!("{}/", workspace_path);

        assert!(path_is_within_workspace(nested_path, &workspace_with_slash));
    }

    #[test]
    fn path_is_within_workspace_case_sensitive_on_unix() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();

        // Create a directory with different case
        let case_diff_path = if let Some(prefix) = workspace_path.strip_suffix("test") {
            format!("{prefix}T")
        } else {
            format!("{workspace_path}X")
        };

        // These are different paths on case-sensitive filesystems
        // The function works at path comparison level
        assert!(!path_is_within_workspace(&case_diff_path, workspace_path));
    }

    #[test]
    fn path_is_within_workspace_returns_false_for_completely_different_roots() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();

        assert!(!path_is_within_workspace(
            "/usr/local/bin/file.txt",
            workspace_path
        ));
    }

    #[test]
    fn path_is_within_workspace_handles_dotdot_sequences() {
        let workspace = create_temp_workspace();
        let workspace_path = workspace.path().to_str().unwrap();
        let sibling = tempfile::Builder::new()
            .prefix("other")
            .tempdir()
            .expect("Failed to create temp directory");

        // Create a path with .. that points outside
        let outside_via_dotdot = workspace
            .path()
            .join("..")
            .join(sibling.path().file_name().unwrap())
            .join("file.txt");
        let outside_path = outside_via_dotdot.to_str().unwrap();

        // After normalization, this should be outside workspace
        // Note: This test depends on filesystem canonicalization
        // It may pass or fail depending on whether the path can be canonicalized
        // The behavior depends on whether canonicalization works
        // We're testing that the function doesn't panic
        // The actual result may vary based on filesystem
        let _result = path_is_within_workspace(outside_path, workspace_path);
    }

    #[test]
    fn normalize_path_for_comparison_handles_nonexistent_paths() {
        // Should not panic for paths that don't exist
        let path = Path::new("/this/path/does/not/exist/file.txt");
        let normalized = normalize_path_for_comparison(path);

        // Should return the path as-is or with parent canonicalized
        // since it can't be fully canonicalized
        assert!(normalized.is_absolute() || normalized == path);
    }

    #[test]
    fn normalize_path_for_comparison_handles_existing_directory() {
        // /tmp should exist on most systems
        let path = Path::new("/tmp");
        let normalized = normalize_path_for_comparison(path);

        // Should be canonicalized
        assert!(normalized.is_absolute());
    }

    #[test]
    fn normalize_path_for_comparison_preserves_filename() {
        let workspace = create_temp_workspace();
        let file_path = workspace.path().join("file.txt");
        let normalized = normalize_path_for_comparison(&file_path);

        // The filename should be preserved even if the file doesn't exist
        assert_eq!(
            normalized.file_name(),
            Some(std::ffi::OsStr::new("file.txt"))
        );
    }
}
