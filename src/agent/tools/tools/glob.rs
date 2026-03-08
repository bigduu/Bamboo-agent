use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use globset::{GlobBuilder, GlobSetBuilder};
use serde::Deserialize;
use serde_json::json;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

const MAX_GLOB_MATCHES: usize = 500;
const SEARCH_SCOPE_TOO_BROAD_ERROR: &str =
    "Search scope too broad. Add path/glob/type or reduce pattern.";
const GLOB_TRUNCATED_NOTICE: &str =
    "[TRUNCATED] Showing first 500 matches. Refine pattern/path and retry.";

#[derive(Debug, Deserialize)]
struct GlobArgs {
    pattern: String,
    #[serde(default)]
    path: Option<String>,
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
        "Fast file pattern matching tool"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "The glob pattern to match files against"
                },
                "path": {
                    "type": "string",
                    "description": "The directory to search in"
                }
            },
            "required": ["pattern"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: GlobArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Glob args: {}", e)))?;

        if parsed.path.is_none() && Self::is_unbounded_pattern(&parsed.pattern) {
            return Err(ToolError::InvalidArguments(
                SEARCH_SCOPE_TOO_BROAD_ERROR.to_string(),
            ));
        }

        let root = parsed
            .path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

        if !root.exists() || !root.is_dir() {
            return Err(ToolError::Execution(format!(
                "Search path is not a directory: {}",
                root.display()
            )));
        }

        let mut glob_builder = GlobSetBuilder::new();
        let glob = GlobBuilder::new(parsed.pattern.trim())
            .literal_separator(false)
            .build()
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid glob pattern: {}", e)))?;
        glob_builder.add(glob);
        let glob_set = glob_builder
            .build()
            .map_err(|e| ToolError::Execution(format!("Failed to compile glob: {}", e)))?;

        let mut matches: Vec<(String, std::time::SystemTime)> = Vec::new();
        for entry in WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_map(|entry| entry.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.path();
            let relative = path.strip_prefix(&root).unwrap_or(path);
            if !glob_set.is_match(relative) && !glob_set.is_match(path) {
                continue;
            }

            let modified = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            matches.push((
                crate::core::paths::path_to_display_string(Path::new(path)),
                modified,
            ));
        }

        matches.sort_by(|a, b| b.1.cmp(&a.1));

        let mut result_lines: Vec<String> = matches.into_iter().map(|(path, _)| path).collect();
        let mut truncated = false;
        if result_lines.len() > MAX_GLOB_MATCHES {
            result_lines.truncate(MAX_GLOB_MATCHES);
            truncated = true;
        }
        if truncated {
            result_lines.push(GLOB_TRUNCATED_NOTICE.to_string());
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
                "path": dir.path()
            }))
            .await
            .unwrap();

        let lines = result_lines(&result);
        assert_eq!(lines.len(), super::MAX_GLOB_MATCHES + 1);
        assert_eq!(lines.last().copied(), Some(super::GLOB_TRUNCATED_NOTICE));
    }
}
