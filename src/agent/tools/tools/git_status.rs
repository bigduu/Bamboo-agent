use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::process::Command;

/// Tool for checking git repository status
pub struct GitStatusTool;

impl GitStatusTool {
    pub fn new() -> Self {
        Self
    }

    /// Get git status for the current directory
    pub async fn git_status(cwd: Option<&str>) -> Result<String, String> {
        let mut cmd = Command::new("git");
        cmd.args(["status", "--porcelain", "-b"]);

        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }

        let output = cmd
            .output()
            .await
            .map_err(|e| format!("Failed to execute git status: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Git status failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            Ok("Working tree clean".to_string())
        } else {
            Ok(stdout.to_string())
        }
    }

    /// Get a summary of changes
    pub async fn git_status_summary(cwd: Option<&str>) -> Result<GitStatusSummary, String> {
        let mut cmd = Command::new("git");
        cmd.args(["status", "--porcelain"]);

        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }

        let output = cmd
            .output()
            .await
            .map_err(|e| format!("Failed to execute git status: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Git status failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut summary = GitStatusSummary {
            staged: Vec::new(),
            modified: Vec::new(),
            untracked: Vec::new(),
            deleted: Vec::new(),
        };

        for line in stdout.lines() {
            if line.len() < 3 {
                continue;
            }
            let status = &line[..2];
            let file = &line[3..];

            if status.starts_with('A') || status.starts_with('M') || status.starts_with('D') {
                summary.staged.push(file.to_string());
            } else if status.ends_with('M') {
                summary.modified.push(file.to_string());
            } else if status.ends_with('?') {
                summary.untracked.push(file.to_string());
            } else if status.ends_with('D') {
                summary.deleted.push(file.to_string());
            }
        }

        Ok(summary)
    }
}

#[derive(Debug)]
pub struct GitStatusSummary {
    pub staged: Vec<String>,
    pub modified: Vec<String>,
    pub untracked: Vec<String>,
    pub deleted: Vec<String>,
}

impl Default for GitStatusTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GitStatusTool {
    fn name(&self) -> &str {
        "git_status"
    }

    fn description(&self) -> &str {
        "Check Git repository status, returning current branch, modified files, staged files, and other information"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "cwd": {
                    "type": "string",
                    "description": "Working directory path; if not provided, uses current directory"
                }
            }
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let cwd = args["cwd"].as_str();

        match Self::git_status(cwd).await {
            Ok(status) => Ok(ToolResult {
                success: true,
                result: status,
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
    fn test_git_status_tool_name() {
        let tool = GitStatusTool::new();
        assert_eq!(tool.name(), "git_status");
    }
}
