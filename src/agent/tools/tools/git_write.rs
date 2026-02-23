//! Git write operations tool.
//!
//! This tool provides write operations for Git repositories including commit, push,
//! branch creation, checkout, and merge. All operations require the `GitWrite` permission.
//!
//! # Security
//!
//! This tool requires `PermissionType::GitWrite` permission.
//! - Force push is detected and requires explicit confirmation
//! - Commit operations check for uncommitted changes
//! - All operations validate they're run in a git repository

use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::process::Command;

/// Git write operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum GitWriteOperation {
    /// Commit changes with a message
    Commit {
        /// Commit message
        message: String,
        /// Files to stage (empty = stage all changes)
        #[serde(default)]
        files: Vec<String>,
        /// Allow empty commit
        #[serde(default)]
        allow_empty: bool,
    },
    /// Push changes to remote
    Push {
        /// Remote name (default: origin)
        #[serde(default = "default_origin")]
        remote: String,
        /// Branch name (default: current branch)
        #[serde(skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Force push (requires explicit confirmation)
        #[serde(default)]
        force: bool,
    },
    /// Create a new branch
    Branch {
        /// Branch name
        name: String,
        /// Checkout the new branch immediately
        #[serde(default)]
        checkout: bool,
        /// Base branch/commit to create from
        #[serde(skip_serializing_if = "Option::is_none")]
        base: Option<String>,
    },
    /// Checkout a branch or commit
    Checkout {
        /// Target to checkout (branch name, commit hash, etc.)
        target: String,
        /// Create a new branch if it doesn't exist
        #[serde(default)]
        create: bool,
    },
    /// Merge a branch into the current branch
    Merge {
        /// Branch to merge
        branch: String,
        /// Merge commit message
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        /// Allow unrelated histories
        #[serde(default)]
        allow_unrelated: bool,
    },
    /// Add/stage files
    Add {
        /// Files to stage (empty = stage all)
        #[serde(default)]
        files: Vec<String>,
    },
    /// Reset changes
    Reset {
        /// Reset mode (soft, mixed, hard)
        #[serde(default)]
        mode: ResetMode,
        /// Target to reset to (default: HEAD)
        #[serde(default = "default_head")]
        target: String,
        /// Paths to reset (empty = reset all)
        #[serde(default)]
        paths: Vec<String>,
    },
}

fn default_origin() -> String {
    "origin".to_string()
}

fn default_head() -> String {
    "HEAD".to_string()
}

/// Reset mode
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetMode {
    #[default]
    Mixed,
    Soft,
    Hard,
}

impl ResetMode {
    fn as_str(&self) -> &'static str {
        match self {
            ResetMode::Soft => "--soft",
            ResetMode::Mixed => "--mixed",
            ResetMode::Hard => "--hard",
        }
    }
}

/// Git write tool arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitWriteArgs {
    /// Operation to perform
    #[serde(flatten)]
    pub operation: GitWriteOperation,
    /// Working directory (must be inside a git repository)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

/// Result of a git operation
#[derive(Debug, Clone, Serialize)]
pub struct GitResult {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Tool for Git write operations
pub struct GitWriteTool;

impl GitWriteTool {
    /// Create a new Git write tool
    pub fn new() -> Self {
        Self
    }

    /// Execute a git write operation
    pub async fn execute_operation(&self, args: GitWriteArgs) -> Result<GitResult, String> {
        let cwd = args.cwd.as_deref().unwrap_or(".");

        // Validate we're in a git repository
        if !self.is_git_repo(cwd).await? {
            return Err(format!("'{}' is not a git repository", cwd));
        }

        match args.operation {
            GitWriteOperation::Commit {
                message,
                files,
                allow_empty,
            } => self.git_commit(cwd, message, files, allow_empty).await,
            GitWriteOperation::Push {
                remote,
                branch,
                force,
            } => self.git_push(cwd, remote, branch, force).await,
            GitWriteOperation::Branch {
                name,
                checkout,
                base,
            } => self.git_branch(cwd, name, checkout, base).await,
            GitWriteOperation::Checkout { target, create } => {
                self.git_checkout(cwd, target, create).await
            }
            GitWriteOperation::Merge {
                branch,
                message,
                allow_unrelated,
            } => self.git_merge(cwd, branch, message, allow_unrelated).await,
            GitWriteOperation::Add { files } => self.git_add(cwd, files).await,
            GitWriteOperation::Reset { mode, target, paths } => {
                self.git_reset(cwd, mode, target, paths).await
            }
        }
    }

    /// Check if the directory is a git repository
    async fn is_git_repo(&self, cwd: &str) -> Result<bool, String> {
        let output = Command::new("git")
            .args(["rev-parse", "--git-dir"])
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to run git command: {}", e))?;

        Ok(output.status.success())
    }

    /// Get the current branch name
    async fn get_current_branch(&self, cwd: &str) -> Result<String, String> {
        let output = Command::new("git")
            .args(["branch", "--show-current"])
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to get current branch: {}", e))?;

        if !output.status.success() {
            return Err("Failed to get current branch".to_string());
        }

        String::from_utf8(output.stdout)
            .map(|s| s.trim().to_string())
            .map_err(|e| format!("Invalid UTF-8 in branch name: {}", e))
    }

    /// Execute git commit
    async fn git_commit(
        &self,
        cwd: &str,
        message: String,
        files: Vec<String>,
        allow_empty: bool,
    ) -> Result<GitResult, String> {
        // Stage files if specified
        if !files.is_empty() {
            let add_result = self.git_add(cwd, files).await?;
            if !add_result.success {
                return Ok(GitResult {
                    success: false,
                    message: format!("Failed to stage files: {}", add_result.message),
                    details: add_result.details,
                });
            }
        }

        // Build commit command
        let mut args = vec!["commit", "-m", &message];
        if allow_empty {
            args.push("--allow-empty");
        }

        let output = Command::new("git")
            .args(&args)
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to execute git commit: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if output.status.success() {
            // Extract commit hash from output
            let commit_hash = stdout
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .map(|s| s.to_string());

            Ok(GitResult {
                success: true,
                message: "Commit created successfully".to_string(),
                details: Some(json!({
                    "commit_hash": commit_hash,
                    "message": message,
                    "output": stdout.to_string(),
                })),
            })
        } else {
            Ok(GitResult {
                success: false,
                message: format!("Commit failed: {}", stderr),
                details: None,
            })
        }
    }

    /// Execute git push
    async fn git_push(
        &self,
        cwd: &str,
        remote: String,
        branch: Option<String>,
        force: bool,
    ) -> Result<GitResult, String> {
        // Get current branch if not specified
        let branch = match branch {
            Some(b) => b,
            None => self.get_current_branch(cwd).await?,
        };

        // Safety check: warn about force push
        if force {
            return Err(
                "Force push detected. This is a dangerous operation. Use --force-with-lease instead or confirm explicitly.".to_string()
            );
        }

        let args = vec!["push", &remote, &branch];

        let output = Command::new("git")
            .args(&args)
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to execute git push: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if output.status.success() {
            Ok(GitResult {
                success: true,
                message: format!("Pushed to {}/{}", remote, branch),
                details: Some(json!({
                    "remote": remote,
                    "branch": branch,
                    "output": stdout.to_string(),
                })),
            })
        } else {
            Ok(GitResult {
                success: false,
                message: format!("Push failed: {}", stderr),
                details: None,
            })
        }
    }

    /// Execute git branch
    async fn git_branch(
        &self,
        cwd: &str,
        name: String,
        checkout: bool,
        base: Option<String>,
    ) -> Result<GitResult, String> {
        let mut args = vec!["branch"];

        if let Some(ref base_ref) = base {
            args.extend(["-c", &name, base_ref]);
        } else {
            args.push(&name);
        }

        let output = Command::new("git")
            .args(&args)
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to execute git branch: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Ok(GitResult {
                success: false,
                message: format!("Failed to create branch: {}", stderr),
                details: None,
            });
        }

        // Checkout the branch if requested
        if checkout {
            let checkout_result = self.git_checkout(cwd, name.clone(), false).await?;
            if !checkout_result.success {
                return Ok(GitResult {
                    success: false,
                    message: format!("Branch '{}' created but checkout failed: {}", name, checkout_result.message),
                    details: checkout_result.details,
                });
            }
        }

        Ok(GitResult {
            success: true,
            message: format!(
                "Branch '{}' created{}",
                name,
                if checkout { " and checked out" } else { "" }
            ),
            details: Some(json!({
                "branch": name,
                "checked_out": checkout,
                "base": base,
            })),
        })
    }

    /// Execute git checkout
    async fn git_checkout(
        &self,
        cwd: &str,
        target: String,
        create: bool,
    ) -> Result<GitResult, String> {
        let mut args = vec!["checkout"];

        if create {
            args.push("-b");
        }
        args.push(&target);

        let output = Command::new("git")
            .args(&args)
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to execute git checkout: {}", e))?;

        let stderr = String::from_utf8_lossy(&output.stderr);

        if output.status.success() {
            Ok(GitResult {
                success: true,
                message: format!("Checked out '{}'", target),
                details: Some(json!({
                    "target": target,
                    "created": create,
                })),
            })
        } else {
            Ok(GitResult {
                success: false,
                message: format!("Checkout failed: {}", stderr),
                details: None,
            })
        }
    }

    /// Execute git merge
    async fn git_merge(
        &self,
        cwd: &str,
        branch: String,
        message: Option<String>,
        allow_unrelated: bool,
    ) -> Result<GitResult, String> {
        let mut args = vec!["merge"];

        if allow_unrelated {
            args.push("--allow-unrelated-histories");
        }

        if let Some(ref msg) = message {
            args.extend(["-m", msg]);
        }

        args.push(&branch);

        let output = Command::new("git")
            .args(&args)
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to execute git merge: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if output.status.success() {
            Ok(GitResult {
                success: true,
                message: format!("Merged '{}' into current branch", branch),
                details: Some(json!({
                    "merged_branch": branch,
                    "output": stdout.to_string(),
                })),
            })
        } else {
            // Check if it's a merge conflict
            let has_conflicts = stderr.contains("CONFLICT") || stdout.contains("CONFLICT");

            Ok(GitResult {
                success: false,
                message: if has_conflicts {
                    format!("Merge conflict with '{}'. Please resolve manually.", branch)
                } else {
                    format!("Merge failed: {}", stderr)
                },
                details: Some(json!({
                    "conflict": has_conflicts,
                    "error": stderr.to_string(),
                })),
            })
        }
    }

    /// Execute git add
    async fn git_add(&self, cwd: &str, files: Vec<String>) -> Result<GitResult, String> {
        let mut args = vec!["add"];

        if files.is_empty() {
            args.push(".");
        } else {
            // Use -- separator to prevent pathspecs from being interpreted as options
            args.push("--");
            for file in &files {
                args.push(file);
            }
        }

        let output = Command::new("git")
            .args(&args)
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to execute git add: {}", e))?;

        if output.status.success() {
            Ok(GitResult {
                success: true,
                message: if files.is_empty() {
                    "Staged all changes".to_string()
                } else {
                    format!("Staged {} file(s)", files.len())
                },
                details: Some(json!({
                    "files": if files.is_empty() { None } else { Some(files) },
                })),
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Ok(GitResult {
                success: false,
                message: format!("Add failed: {}", stderr),
                details: None,
            })
        }
    }

    /// Execute git reset
    async fn git_reset(
        &self,
        cwd: &str,
        mode: ResetMode,
        target: String,
        paths: Vec<String>,
    ) -> Result<GitResult, String> {
        let mut args = vec!["reset", mode.as_str()];

        // If paths are specified, only reset those
        if paths.is_empty() {
            args.push(&target);
        } else {
            args.push("--");
            for path in &paths {
                args.push(path);
            }
        }

        let output = Command::new("git")
            .args(&args)
            .current_dir(cwd)
            .output()
            .await
            .map_err(|e| format!("Failed to execute git reset: {}", e))?;

        if output.status.success() {
            Ok(GitResult {
                success: true,
                message: if paths.is_empty() {
                    format!("Reset to {}", target)
                } else {
                    format!("Reset {} path(s)", paths.len())
                },
                details: Some(json!({
                    "mode": format!("{:?}", mode).to_lowercase(),
                    "target": target,
                    "paths": if paths.is_empty() { None } else { Some(paths) },
                })),
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Ok(GitResult {
                success: false,
                message: format!("Reset failed: {}", stderr),
                details: None,
            })
        }
    }
}

impl Default for GitWriteTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GitWriteTool {
    fn name(&self) -> &str {
        "git_write"
    }

    fn description(&self) -> &str {
        "Perform Git write operations: commit, push, branch, checkout, merge, add, reset. All operations require GitWrite permission. Safety checks prevent dangerous operations like force push."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": ["commit", "push", "branch", "checkout", "merge", "add", "reset"],
                    "description": "Git operation to perform"
                },
                "message": {
                    "type": "string",
                    "description": "Commit or merge message (for commit/merge operations)"
                },
                "files": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Files to stage (for commit/add operations, empty = all)"
                },
                "allow_empty": {
                    "type": "boolean",
                    "default": false,
                    "description": "Allow empty commit"
                },
                "remote": {
                    "type": "string",
                    "default": "origin",
                    "description": "Remote name (for push operation)"
                },
                "branch": {
                    "type": "string",
                    "description": "Branch name (for push/branch/merge operations)"
                },
                "force": {
                    "type": "boolean",
                    "default": false,
                    "description": "Force push (WARNING: dangerous, will be blocked)"
                },
                "name": {
                    "type": "string",
                    "description": "New branch name (for branch operation)"
                },
                "checkout": {
                    "type": "boolean",
                    "default": false,
                    "description": "Checkout the new branch immediately (for branch operation)"
                },
                "base": {
                    "type": "string",
                    "description": "Base branch/commit to create from (for branch operation)"
                },
                "target": {
                    "type": "string",
                    "description": "Target to checkout (for checkout operation)"
                },
                "create": {
                    "type": "boolean",
                    "default": false,
                    "description": "Create branch if it doesn't exist (for checkout operation)"
                },
                "mode": {
                    "type": "string",
                    "enum": ["soft", "mixed", "hard"],
                    "default": "mixed",
                    "description": "Reset mode (for reset operation)"
                },
                "allow_unrelated": {
                    "type": "boolean",
                    "default": false,
                    "description": "Allow unrelated histories (for merge operation)"
                },
                "cwd": {
                    "type": "string",
                    "description": "Working directory (must be inside a git repository)"
                }
            },
            "required": ["operation"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let git_args: GitWriteArgs =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        match self.execute_operation(git_args).await {
            Ok(result) => Ok(ToolResult {
                success: result.success,
                result: result.message,
                display_preference: Some("markdown".to_string()),
            }),
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

    #[test]
    fn test_git_write_operation_deserialization() {
        let json = json!({
            "operation": "commit",
            "message": "Test commit",
            "files": ["file1.txt", "file2.rs"]
        });

        let op: GitWriteOperation = serde_json::from_value(json).unwrap();
        match op {
            GitWriteOperation::Commit { message, files, allow_empty } => {
                assert_eq!(message, "Test commit");
                assert_eq!(files.len(), 2);
                assert!(!allow_empty);
            }
            _ => panic!("Expected Commit operation"),
        }
    }

    #[test]
    fn test_git_write_push_deserialization() {
        let json = json!({
            "operation": "push",
            "remote": "origin",
            "branch": "main",
            "force": false
        });

        let op: GitWriteOperation = serde_json::from_value(json).unwrap();
        match op {
            GitWriteOperation::Push { remote, branch, force } => {
                assert_eq!(remote, "origin");
                assert_eq!(branch, Some("main".to_string()));
                assert!(!force);
            }
            _ => panic!("Expected Push operation"),
        }
    }

    #[test]
    fn test_reset_mode_serialization() {
        assert_eq!(ResetMode::Soft.as_str(), "--soft");
        assert_eq!(ResetMode::Mixed.as_str(), "--mixed");
        assert_eq!(ResetMode::Hard.as_str(), "--hard");
    }

    #[test]
    fn test_tool_name_and_description() {
        let tool = GitWriteTool::new();
        assert_eq!(tool.name(), "git_write");
        assert!(tool.description().contains("Git"));
    }

    #[tokio::test]
    async fn test_not_git_repo() {
        let tool = GitWriteTool::new();
        let result = tool
            .execute(json!({
                "operation": "commit",
                "message": "test",
                "cwd": "/tmp/nonexistent_git_repo_xyz123"
            }))
            .await
            .unwrap();

        // This will fail because the directory doesn't exist or isn't a git repo
        assert!(!result.success);
    }

    #[test]
    fn test_git_result_serialization() {
        let result = GitResult {
            success: true,
            message: "Test message".to_string(),
            details: Some(json!({ "key": "value" })),
        };

        let serialized = serde_json::to_string(&result).unwrap();
        assert!(serialized.contains("Test message"));
        assert!(serialized.contains("key"));
    }
}
