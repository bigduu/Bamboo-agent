//! Glob search tool for finding files by pattern.
//!
//! This tool provides advanced file searching using glob patterns like `**/*.rs`
//! or `src/**/*.{ts,tsx}`. It's more powerful than simple directory listing
//! and supports exclusion patterns.

use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use glob::Pattern;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde_json::json;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Tool for searching files using glob patterns
pub struct GlobSearchTool;

/// Arguments for glob search
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlobArgs {
    /// Glob pattern to match files (e.g., "**/*.rs", "src/**/*.{ts,tsx}")
    pub pattern: String,
    /// Optional base directory to search from (defaults to current directory)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_dir: Option<String>,
    /// Patterns to exclude (e.g., ["**/node_modules/**", "**/.git/**"])
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Maximum number of results to return (default: 1000)
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Whether to return only files (default: true)
    #[serde(default = "default_true")]
    pub files_only: bool,
    /// Whether to return only directories (default: false)
    #[serde(default)]
    pub dirs_only: bool,
}

fn default_limit() -> usize {
    1000
}

fn default_true() -> bool {
    true
}

impl GlobSearchTool {
    /// Create a new glob search tool
    pub fn new() -> Self {
        Self
    }

    /// Validate and canonicalize the base directory for safe path traversal.
    ///
    /// This function:
    /// 1. Requires absolute paths (rejects relative paths)
    /// 2. Canonicalizes the path to resolve symlinks
    /// 3. Verifies the path exists and is a directory
    fn validate_base_dir(base_dir: &str) -> Result<PathBuf, String> {
        let path = Path::new(base_dir);

        // Require absolute path
        if !path.is_absolute() {
            return Err(format!(
                "Path traversal protection: base_dir '{}' must be an absolute path",
                base_dir
            ));
        }

        // Canonicalize to resolve symlinks
        let canonical = std::fs::canonicalize(path)
            .map_err(|e| format!("Cannot access base_dir '{}': {}", base_dir, e))?;

        // Verify it's a directory
        if !canonical.is_dir() {
            return Err(format!(
                "Path traversal protection: base_dir '{}' is not a directory",
                base_dir
            ));
        }

        Ok(canonical)
    }

    /// Check if a glob pattern contains path traversal components.
    ///
    /// Rejects patterns that start with '/' (absolute paths) or contain '..' components.
    fn validate_pattern(pattern: &str) -> Result<(), String> {
        // Reject absolute path patterns
        if pattern.starts_with('/') {
            return Err(format!(
                "Path traversal protection: Pattern '{}' cannot be an absolute path",
                pattern
            ));
        }

        // Reject patterns with parent directory traversal
        if pattern.contains("..") {
            return Err(format!(
                "Path traversal protection: Pattern '{}' cannot contain '..'",
                pattern
            ));
        }

        Ok(())
    }

    /// Check if a path is safely within the base directory.
    ///
    /// This prevents path traversal where a pattern like `**/../../etc/passwd`
    /// would escape the intended base directory.
    fn is_within_base_dir(path: &Path, base_dir: &Path) -> bool {
        match path.canonicalize() {
            Ok(canonical) => canonical.starts_with(base_dir),
            Err(_) => {
                // If we can't canonicalize, be conservative and check if the path
                // contains any parent directory components that would escape
                let path_str = path.to_string_lossy();
                !path_str.contains("..")
            }
        }
    }

    /// Search for files matching the given pattern
    pub async fn search(args: GlobArgs) -> Result<Vec<String>, String> {
        // Validate and canonicalize base_dir
        let base_dir = args
            .base_dir
            .as_deref()
            .map(Self::validate_base_dir)
            .transpose()?
            .unwrap_or_else(|| {
                // Default to current directory, but still validate it
                std::env::current_dir()
                    .and_then(std::fs::canonicalize)
                    .expect("Failed to get current directory")
            });

        // Validate the pattern doesn't contain traversal
        Self::validate_pattern(&args.pattern)?;

        // Build exclusion set
        let exclude_set = Self::build_exclude_set(&args.exclude)?;

        // Determine if this is a recursive pattern
        // Patterns with ** or explicit / characters (like subdir/*.txt) are recursive
        let is_recursive = args.pattern.contains("**") || args.pattern.contains('/');

        // Compile the pattern for matching
        let pattern = Pattern::new(&args.pattern)
            .map_err(|e| format!("Invalid glob pattern '{}': {}", args.pattern, e))?;

        // Use walkdir with follow_links=false to prevent symlink traversal
        let mut results = Vec::new();

        for entry in WalkDir::new(&base_dir)
            .follow_links(false) // Security: Don't follow symlinks
            .max_depth(if is_recursive { usize::MAX } else { 1 }) // Limit depth for non-recursive
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            // Get path relative to base_dir for pattern matching
            let relative_path = match path.strip_prefix(&base_dir) {
                Ok(rel) => rel,
                Err(_) => {
                    // Path is not under base_dir, skip
                    continue;
                }
            };

            // For non-recursive patterns, we need to check differently
            // *.txt should match "test.txt" but not "subdir/test.txt"
            let matches_pattern = if !is_recursive {
                // Only match files directly in base_dir (relative path should have no /)
                let relative_str = relative_path.to_string_lossy();
                !relative_str.contains('/') && pattern.matches(&relative_str)
            } else {
                // Recursive patterns match anywhere
                let relative_str = relative_path.to_string_lossy();
                pattern.matches(&relative_str)
            };

            if !matches_pattern {
                continue;
            }

            // Security: Double-check the path is still within base_dir
            if !Self::is_within_base_dir(path, &base_dir) {
                log::warn!(
                    "Path traversal blocked: '{}' is outside base directory",
                    path.display()
                );
                continue;
            }

            // Check exclusion patterns
            if Self::is_excluded(path, &exclude_set) {
                continue;
            }

            // Check file type filters
            let is_file = entry.file_type().is_file();
            let is_dir = entry.file_type().is_dir();

            if args.dirs_only && !is_dir {
                continue;
            }

            if args.files_only && !is_file {
                continue;
            }

            // Convert to string
            if let Some(path_str) = path.to_str() {
                results.push(path_str.to_string());

                // Check limit
                if results.len() >= args.limit {
                    break;
                }
            }
        }

        // Sort results for consistent output
        results.sort();

        Ok(results)
    }

    /// Build a GlobSet from exclusion patterns
    fn build_exclude_set(patterns: &[String]) -> Result<Option<GlobSet>, String> {
        if patterns.is_empty() {
            return Ok(None);
        }

        let mut builder = GlobSetBuilder::new();

        for pattern in patterns {
            let glob = Glob::new(pattern)
                .map_err(|e| format!("Invalid exclude pattern '{}': {}", pattern, e))?;
            builder.add(glob);
        }

        builder
            .build()
            .map(Some)
            .map_err(|e| format!("Failed to build exclude set: {}", e))
    }

    /// Check if a path matches any exclusion pattern
    fn is_excluded(path: &Path, exclude_set: &Option<GlobSet>) -> bool {
        if let Some(set) = exclude_set {
            set.is_match(path)
        } else {
            false
        }
    }
}

impl Default for GlobSearchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GlobSearchTool {
    fn name(&self) -> &str {
        "glob_search"
    }

    fn description(&self) -> &str {
        "Search for files using glob patterns. Supports wildcards like **/*.rs for recursive search, and exclusion patterns. Returns a list of matching file paths."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern to match files (e.g., '**/*.rs', 'src/**/*.{ts,tsx}', '*.md')"
                },
                "base_dir": {
                    "type": "string",
                    "description": "Base directory to search from (optional, defaults to current directory)"
                },
                "exclude": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "Patterns to exclude from results (e.g., ['**/node_modules/**', '**/.git/**', '**/target/**'])"
                },
                "limit": {
                    "type": "integer",
                    "default": 1000,
                    "description": "Maximum number of results to return"
                },
                "files_only": {
                    "type": "boolean",
                    "default": true,
                    "description": "Return only files (not directories)"
                },
                "dirs_only": {
                    "type": "boolean",
                    "default": false,
                    "description": "Return only directories (not files). Overrides files_only if true."
                }
            },
            "required": ["pattern"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let glob_args: GlobArgs =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        match Self::search(glob_args).await {
            Ok(results) => {
                let count = results.len();
                let output = if results.is_empty() {
                    "No files found matching the pattern.".to_string()
                } else {
                    format!("Found {} file(s):\n\n{}", count, results.join("\n"))
                };

                Ok(ToolResult {
                    success: true,
                    result: output,
                    display_preference: Some("markdown".to_string()),
                })
            }
            Err(e) => Ok(ToolResult {
                success: false,
                result: e,
                display_preference: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::fs;

    #[tokio::test]
    async fn test_glob_search_simple_pattern() {
        // Create test files
        let test_dir = "/tmp/glob_test_simple";
        let _ = fs::remove_dir_all(test_dir).await;
        fs::create_dir_all(test_dir).await.unwrap();
        fs::write(format!("{}/test1.txt", test_dir), "content1")
            .await
            .unwrap();
        fs::write(format!("{}/test2.txt", test_dir), "content2")
            .await
            .unwrap();
        fs::write(format!("{}/test.rs", test_dir), "content3")
            .await
            .unwrap();

        let args = GlobArgs {
            pattern: "*.txt".to_string(),
            base_dir: Some(test_dir.to_string()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let results = GlobSearchTool::search(args).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|p| p.ends_with(".txt")));

        // Cleanup
        let _ = fs::remove_dir_all(test_dir).await;
    }

    #[tokio::test]
    async fn test_glob_search_with_exclude() {
        let test_dir = "/tmp/glob_test_exclude";
        let _ = fs::remove_dir_all(test_dir).await;
        fs::create_dir_all(format!("{}/node_modules", test_dir))
            .await
            .unwrap();
        fs::write(format!("{}/file1.txt", test_dir), "content1")
            .await
            .unwrap();
        fs::create_dir_all(format!("{}/node_modules/pkg", test_dir))
            .await
            .unwrap();
        fs::write(format!("{}/node_modules/pkg/index.js", test_dir), "content2")
            .await
            .unwrap();

        let args = GlobArgs {
            pattern: "**/*".to_string(),
            base_dir: Some(test_dir.to_string()),
            // Match node_modules directory and all its contents
            // **/node_modules matches the directory itself
            // **/node_modules/** matches all contents
            exclude: vec!["**/node_modules".to_string(), "**/node_modules/**".to_string()],
            limit: 1000,
            files_only: false,
            dirs_only: false,
        };

        let results = GlobSearchTool::search(args).await.unwrap();
        assert!(!results.iter().any(|p| p.contains("node_modules")), "Found node_modules in results: {:?}", results);

        // Cleanup
        let _ = fs::remove_dir_all(test_dir).await;
    }

    #[tokio::test]
    async fn test_glob_search_limit() {
        let test_dir = "/tmp/glob_test_limit";
        let _ = fs::remove_dir_all(test_dir).await;
        fs::create_dir_all(test_dir).await.unwrap();

        for i in 0..10 {
            fs::write(format!("{}/file{}.txt", test_dir, i), "content")
                .await
                .unwrap();
        }

        let args = GlobArgs {
            pattern: "*.txt".to_string(),
            base_dir: Some(test_dir.to_string()),
            exclude: vec![],
            limit: 5,
            files_only: true,
            dirs_only: false,
        };

        let results = GlobSearchTool::search(args).await.unwrap();
        assert_eq!(results.len(), 5);

        // Cleanup
        let _ = fs::remove_dir_all(test_dir).await;
    }

    #[tokio::test]
    async fn test_glob_search_recursive() {
        let test_dir = "/tmp/glob_test_recursive";
        let _ = fs::remove_dir_all(test_dir).await;
        fs::create_dir_all(format!("{}/subdir1/nested", test_dir))
            .await
            .unwrap();
        fs::write(format!("{}/root.txt", test_dir), "content1")
            .await
            .unwrap();
        fs::write(format!("{}/subdir1/sub.txt", test_dir), "content2")
            .await
            .unwrap();
        fs::write(format!("{}/subdir1/nested/deep.txt", test_dir), "content3")
            .await
            .unwrap();

        let args = GlobArgs {
            pattern: "**/*.txt".to_string(),
            base_dir: Some(test_dir.to_string()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let results = GlobSearchTool::search(args).await.unwrap();
        assert_eq!(results.len(), 3);

        // Cleanup
        let _ = fs::remove_dir_all(test_dir).await;
    }

    #[test]
    fn test_tool_name_and_description() {
        let tool = GlobSearchTool::new();
        assert_eq!(tool.name(), "glob_search");
        assert!(tool.description().contains("glob"));
    }

    #[tokio::test]
    async fn test_tool_execute() {
        let test_dir = "/tmp/glob_test_execute";
        let _ = tokio::fs::remove_dir_all(test_dir).await;
        tokio::fs::create_dir_all(test_dir).await.unwrap();
        tokio::fs::write(format!("{}/test.txt", test_dir), "content")
            .await
            .unwrap();

        let tool = GlobSearchTool::new();
        let result = tool
            .execute(json!({
                "pattern": "*.txt",
                "base_dir": test_dir
            }))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("test.txt"));

        // Cleanup
        let _ = tokio::fs::remove_dir_all(test_dir).await;
    }

    // Path Traversal Protection Tests

    #[tokio::test]
    async fn test_glob_absolute_pattern_rejected() {
        let args = GlobArgs {
            pattern: "/etc/passwd".to_string(),
            base_dir: Some("/tmp".to_string()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let result = GlobSearchTool::search(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Path traversal protection"));
    }

    #[tokio::test]
    async fn test_glob_path_traversal_blocked() {
        // Create test directories
        let safe_dir = "/tmp/glob_safe_test";
        let outside_dir = "/tmp/glob_outside_test";

        let _ = tokio::fs::remove_dir_all(safe_dir).await;
        let _ = tokio::fs::remove_dir_all(outside_dir).await;

        tokio::fs::create_dir_all(safe_dir).await.unwrap();
        tokio::fs::create_dir_all(outside_dir).await.unwrap();
        tokio::fs::write(format!("{}/safe.txt", safe_dir), "safe content")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/outside.txt", outside_dir), "outside content")
            .await
            .unwrap();

        // Pattern with traversal should be rejected
        let args = GlobArgs {
            pattern: "../glob_outside_test/*".to_string(),
            base_dir: Some(safe_dir.to_string()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let result = GlobSearchTool::search(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Path traversal protection"));

        // Cleanup
        let _ = tokio::fs::remove_dir_all(safe_dir).await;
        let _ = tokio::fs::remove_dir_all(outside_dir).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_glob_symlink_not_followed() {
        use std::os::unix::fs::symlink;

        // Create test directories
        let base_dir = "/tmp/glob_symlink_test_base";
        let outside_dir = "/tmp/glob_symlink_outside";

        let _ = tokio::fs::remove_dir_all(base_dir).await;
        let _ = tokio::fs::remove_dir_all(outside_dir).await;

        tokio::fs::create_dir_all(base_dir).await.unwrap();
        tokio::fs::create_dir_all(outside_dir).await.unwrap();

        tokio::fs::write(format!("{}/real.txt", base_dir), "real content")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/secret.txt", outside_dir), "secret content")
            .await
            .unwrap();

        // Create a symlink pointing outside the base directory
        let symlink_path = format!("{}/link", base_dir);
        symlink(outside_dir, &symlink_path).unwrap();

        // Search should NOT follow the symlink
        let args = GlobArgs {
            pattern: "**/*.txt".to_string(),
            base_dir: Some(base_dir.to_string()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let result = GlobSearchTool::search(args).await.unwrap();

        // Should only find real.txt, not secret.txt
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("real.txt"));
        assert!(!result.iter().any(|p| p.contains("secret.txt")));

        // Cleanup
        let _ = tokio::fs::remove_dir_all(base_dir).await;
        let _ = tokio::fs::remove_dir_all(outside_dir).await;
    }

    #[tokio::test]
    async fn test_glob_valid_pattern_allowed() {
        // Use a unique test directory to avoid interference from previous runs
        let test_dir = format!("/tmp/glob_valid_test_{}", std::process::id());
        // Ensure clean state
        let _ = tokio::fs::remove_dir_all(&test_dir).await;

        tokio::fs::create_dir_all(format!("{}/subdir", test_dir))
            .await
            .unwrap();
        tokio::fs::write(format!("{}/test.txt", test_dir), "content1")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/subdir/nested.txt", test_dir), "content2")
            .await
            .unwrap();

        // Non-recursive pattern - should only match test.txt
        let args = GlobArgs {
            pattern: "*.txt".to_string(),
            base_dir: Some(test_dir.clone()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let result = GlobSearchTool::search(args).await.unwrap();
        // Should only find test.txt in the root, not subdir/nested.txt
        assert_eq!(result.len(), 1, "Expected 1 file for non-recursive pattern, got {:?}", result);
        // Check that it ends with test.txt (accounting for macOS /private/tmp path)
        assert!(
            result[0].ends_with("test.txt"),
            "Expected test.txt, got {}",
            result[0]
        );
        // Verify it doesn't contain nested.txt
        assert!(
            !result[0].contains("nested"),
            "Non-recursive pattern should not match nested.txt"
        );

        // Recursive pattern - should match both
        let args = GlobArgs {
            pattern: "**/*.txt".to_string(),
            base_dir: Some(test_dir.clone()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let result = GlobSearchTool::search(args).await.unwrap();
        // Should find both test.txt and subdir/nested.txt
        assert_eq!(result.len(), 2, "Expected 2 files for recursive pattern, got {:?}", result);
        assert!(result.iter().any(|p| p.contains("test.txt")), "Missing test.txt");
        assert!(result.iter().any(|p| p.contains("nested.txt")), "Missing nested.txt");

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_glob_outside_base_dir_blocked() {
        // Create directories where one is outside the other
        let base_dir = "/tmp/glob_boundary_base";
        let outside_dir = "/tmp/glob_boundary_outside";

        let _ = tokio::fs::remove_dir_all(base_dir).await;
        let _ = tokio::fs::remove_dir_all(outside_dir).await;

        tokio::fs::create_dir_all(format!("{}/inside", base_dir))
            .await
            .unwrap();
        tokio::fs::create_dir_all(outside_dir).await.unwrap();

        tokio::fs::write(format!("{}/inside/file.txt", base_dir), "inside")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/outside_file.txt", outside_dir), "outside")
            .await
            .unwrap();

        // Create a pattern that might escape if not careful
        // The glob library should not allow this, but we add extra protection
        let args = GlobArgs {
            pattern: "../../../*".to_string(),
            base_dir: Some(format!("{}/inside", base_dir)),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        // This should either be rejected or return no results
        let result = GlobSearchTool::search(args).await;
        if let Ok(files) = result {
            // If the pattern was accepted, ensure we didn't get outside files
            assert!(!files.iter().any(|f| f.contains("outside_file")),
                "Should not find files outside base_dir");
        }
        // If it was rejected, that's also acceptable

        // Cleanup
        let _ = tokio::fs::remove_dir_all(base_dir).await;
        let _ = tokio::fs::remove_dir_all(outside_dir).await;
    }

    #[tokio::test]
    async fn test_base_dir_must_be_absolute() {
        let args = GlobArgs {
            pattern: "*.txt".to_string(),
            base_dir: Some("relative/path".to_string()),
            exclude: vec![],
            limit: 1000,
            files_only: true,
            dirs_only: false,
        };

        let result = GlobSearchTool::search(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be an absolute path"));
    }
}
