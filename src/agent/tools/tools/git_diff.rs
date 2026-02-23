use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::process::Command;

/// Tool for showing git diff
pub struct GitDiffTool;

impl GitDiffTool {
    pub fn new() -> Self {
        Self
    }

    /// Get git diff
    pub async fn git_diff(
        cwd: Option<&str>,
        staged: bool,
        file: Option<&str>,
    ) -> Result<String, String> {
        let mut cmd = Command::new("git");
        cmd.arg("diff");

        if staged {
            cmd.arg("--staged");
        }

        if let Some(f) = file {
            cmd.arg(f);
        }

        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }

        let output = cmd
            .output()
            .await
            .map_err(|e| format!("Failed to execute git diff: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Git diff failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            Ok("No changes".to_string())
        } else {
            Ok(stdout.to_string())
        }
    }
}

impl Default for GitDiffTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GitDiffTool {
    fn name(&self) -> &str {
        "git_diff"
    }

    fn description(&self) -> &str {
        "Show Git code changes (diff). Can view working directory changes or staged changes"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "cwd": {
                    "type": "string",
                    "description": "Working directory path; if not provided, uses current directory"
                },
                "staged": {
                    "type": "boolean",
                    "description": "Whether to view staged changes (git diff --staged), default false",
                    "default": false
                },
                "file": {
                    "type": "string",
                    "description": "Specify file path to view diff for a specific file; if not provided, views all changes"
                }
            }
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let cwd = args["cwd"].as_str();
        let staged = args["staged"].as_bool().unwrap_or(false);
        let file = args["file"].as_str();

        match Self::git_diff(cwd, staged, file).await {
            Ok(diff) => Ok(ToolResult {
                success: true,
                result: diff,
                display_preference: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                result: e,
                display_preference: Some("error".to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_git_diff_tool_name() {
        let tool = GitDiffTool::new();
        assert_eq!(tool.name(), "git_diff");
    }
}
