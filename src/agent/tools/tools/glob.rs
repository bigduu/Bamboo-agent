use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use globset::{GlobBuilder, GlobSetBuilder};
use serde::Deserialize;
use serde_json::json;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use super::workspace_state;

const DEFAULT_GLOB_MATCHES: usize = 100;
const MAX_GLOB_MATCHES: usize = 200;
const MAX_GLOB_SCANNED_FILES: usize = 50_000;
const SKIP_DIRS: [&str; 8] = [
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    ".next",
    ".cache",
    "coverage",
];
const SEARCH_SCOPE_TOO_BROAD_ERROR: &str =
    "Search scope too broad. Add path/glob/type or reduce pattern.";

#[derive(Debug, Deserialize)]
struct GlobArgs {
    pattern: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

pub struct GlobTool;

impl GlobTool {
    pub fn new() -> Self {
        Self
    }

    fn is_unbounded_pattern(pattern: &str) -> bool {
        let normalized = pattern.trim().replace('\\', "/");
        matches!(
            normalized.as_str(),
            "*" | "**" | "**/*" | "**/**" | "./**/*" | ".//**/*"
        )
    }

    fn should_skip_dir(path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|name| SKIP_DIRS.contains(&name))
            .unwrap_or(false)
    }
}

impl Default for GlobTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GlobTool {
    fn name(&self) -> &str {
        "Glob"
    }

    fn description(&self) -> &str {
        "Fast file pattern matching tool. Use it to find candidate files before deeper Read or Grep steps. Avoid unbounded root patterns without narrowing path or pattern."
    }

    fn mutability(&self) -> crate::agent::tools::ToolMutability {
        crate::agent::tools::ToolMutability::ReadOnly
    }

    fn concurrency_safe(&self) -> bool {
        true
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "The glob pattern to match files against (for example **/*.rs or src/**/*.ts)"
                },
                "path": {
                    "type": "string",
                    "description": "The directory to search in. Omit to use the current workspace root."
                },
                "limit": {
                    "type": "number",
                    "description": "Maximum number of returned matches (default 100, hard cap 200). Use a smaller limit for broad searches."
                }
            },
            "required": ["pattern"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Glob"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: GlobArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Glob args: {}", e)))?;

        if parsed.path.is_none() && Self::is_unbounded_pattern(&parsed.pattern) {
            return Err(ToolError::InvalidArguments(
                SEARCH_SCOPE_TOO_BROAD_ERROR.to_string(),
            ));
        }

        let default_root = workspace_state::workspace_or_process_cwd(ctx.session_id);
        let root = parsed
            .path
            .as_ref()
            .map(|value| {
                let path = PathBuf::from(value);
                if path.is_absolute() {
                    path
                } else {
                    default_root.join(path)
                }
            })
            .unwrap_or(default_root);

        if !root.exists() || !root.is_dir() {
            return Err(ToolError::Execution(format!(
                "Search path is not a directory: {}",
                root.display()
            )));
        }

        let limit = parsed
            .limit
            .unwrap_or(DEFAULT_GLOB_MATCHES)
            .clamp(1, MAX_GLOB_MATCHES);

        let mut glob_builder = GlobSetBuilder::new();
        let glob = GlobBuilder::new(parsed.pattern.trim())
            .literal_separator(false)
            .build()
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid glob pattern: {}", e)))?;
        glob_builder.add(glob);
        let glob_set = glob_builder
            .build()
            .map_err(|e| ToolError::Execution(format!("Failed to compile glob: {}", e)))?;

        let mut matches: Vec<(String, u64)> = Vec::new();
        let mut total_matches = 0usize;
        let mut scanned_files = 0usize;
        let mut scan_truncated = false;

        for entry in WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                !entry.file_type().is_dir() || !Self::should_skip_dir(entry.path())
            })
            .filter_map(|entry| entry.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }

            scanned_files += 1;
            if scanned_files > MAX_GLOB_SCANNED_FILES {
                scan_truncated = true;
                break;
            }

            let path = entry.path();
            let relative = path.strip_prefix(&root).unwrap_or(path);
            if !glob_set.is_match(relative) && !glob_set.is_match(path) {
                continue;
            }

            total_matches += 1;
            let modified = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            matches.push((
                crate::core::paths::path_to_display_string(Path::new(path)),
                modified,
            ));
        }

        matches.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let mut result_lines: Vec<String> = matches
            .into_iter()
            .take(limit)
            .map(|(path, _)| path)
            .collect();

        if total_matches > limit {
            result_lines.push(format!(
                "[TRUNCATED] Showing first {limit} matches (matched {total_matches}). Refine pattern/path and retry."
            ));
        }

        if scan_truncated {
            result_lines.push(format!(
                "[PARTIAL] Stopped after scanning {} files. Narrow path/pattern to improve results.",
                MAX_GLOB_SCANNED_FILES
            ));
        }

        Ok(ToolResult {
            success: true,
            result: result_lines.join("\n"),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::GlobTool;
    use crate::agent::core::tools::Tool;
    use serde_json::json;

    fn result_lines(result: &crate::agent::core::tools::ToolResult) -> Vec<&str> {
        result
            .result
            .lines()
            .filter(|line| !line.is_empty())
            .collect()
    }

    #[tokio::test]
    async fn glob_rejects_unbounded_default_root_pattern() {
        let tool = GlobTool::new();
        let error = tool
            .execute(json!({
                "pattern": "**/*"
            }))
            .await
            .expect_err("unbounded root glob should fail");
        assert!(error
            .to_string()
            .contains(super::SEARCH_SCOPE_TOO_BROAD_ERROR));
    }

    #[tokio::test]
    async fn glob_truncates_to_max_matches_with_notice() {
        let dir = tempfile::tempdir().unwrap();
        for idx in 0..520 {
            let file = dir.path().join(format!("f-{idx}.txt"));
            tokio::fs::write(file, "x").await.unwrap();
        }

        let tool = GlobTool::new();
        let result = tool
            .execute(json!({
                "pattern": "**/*.txt",
                "path": dir.path(),
                "limit": 120
            }))
            .await
            .unwrap();

        let lines = result_lines(&result);
        assert_eq!(lines.len(), 121);
        assert!(lines
            .last()
            .copied()
            .unwrap_or_default()
            .contains("[TRUNCATED]"));
    }

    #[tokio::test]
    async fn glob_skips_heavy_default_directories() {
        let dir = tempfile::tempdir().unwrap();
        let kept = dir.path().join("src").join("keep.txt");
        let skipped = dir.path().join("node_modules").join("skip.txt");
        tokio::fs::create_dir_all(kept.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::create_dir_all(skipped.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&kept, "ok").await.unwrap();
        tokio::fs::write(&skipped, "skip").await.unwrap();

        let tool = GlobTool::new();
        let result = tool
            .execute(json!({
                "pattern": "**/*.txt",
                "path": dir.path(),
                "limit": 50
            }))
            .await
            .unwrap();

        assert!(result.result.contains("keep.txt"));
        assert!(!result.result.contains("node_modules"));
    }
}
