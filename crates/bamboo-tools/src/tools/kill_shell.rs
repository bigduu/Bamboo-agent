use bamboo_agent_core::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use super::bash_runtime;

#[derive(Debug, Deserialize)]
struct KillShellArgs {
    #[serde(default)]
    shell_id: Option<String>,
    #[serde(default)]
    bash_id: Option<String>,
}

impl KillShellArgs {
    fn resolved_shell_id(&self) -> Option<&str> {
        self.shell_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                self.bash_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
            })
    }
}

pub struct KillShellTool;

impl KillShellTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KillShellTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for KillShellTool {
    fn name(&self) -> &str {
        "KillShell"
    }

    fn description(&self) -> &str {
        "Kill a running background Bash shell by ID (use the bash_id returned by Bash run_in_background)"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "shell_id": {
                    "type": "string",
                    "description": "The ID of the background shell to kill (recommended: pass Bash's bash_id here)"
                },
                "bash_id": {
                    "type": "string",
                    "description": "Legacy alias for shell_id; use the id returned by Bash"
                }
            },
            "required": ["shell_id"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: KillShellArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid KillShell args: {}", e)))?;

        let shell_id = parsed.resolved_shell_id().ok_or_else(|| {
            ToolError::InvalidArguments(
                "KillShell requires 'shell_id' (or legacy alias 'bash_id') from Bash run_in_background".to_string(),
            )
        })?;
        let shell = bash_runtime::get_shell(shell_id).ok_or_else(|| {
            ToolError::Execution(format!(
                "Background shell '{}' not found. Use the bash_id returned by Bash(run_in_background=true), not chat session_id.",
                shell_id
            ))
        })?;

        if shell.status() == "running" {
            shell.kill().await.map_err(ToolError::Execution)?;
        }
        let _ = bash_runtime::remove_shell(shell_id);

        Ok(ToolResult {
            success: true,
            result: json!({
                "shell_id": shell_id,
                "bash_id": shell_id,
                "status": "killed"
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::bash::BashTool;
    use serde_json::Value;

    #[cfg(target_os = "windows")]
    fn long_running_command() -> &'static str {
        "powershell -NoProfile -Command \"Start-Sleep -Seconds 2\""
    }

    #[cfg(not(target_os = "windows"))]
    fn long_running_command() -> &'static str {
        "sleep 2"
    }

    #[tokio::test]
    async fn kill_shell_terminates_and_removes_session() {
        let bash = BashTool::new();
        let spawned = bash
            .execute(json!({
                "command": long_running_command(),
                "run_in_background": true
            }))
            .await
            .unwrap();
        let spawned_payload: Value = serde_json::from_str(&spawned.result).unwrap();
        let shell_id = spawned_payload["bash_id"].as_str().unwrap().to_string();
        assert!(super::bash_runtime::get_shell(&shell_id).is_some());

        let kill = KillShellTool::new();
        let result = kill
            .execute(json!({
                "shell_id": shell_id
            }))
            .await
            .unwrap();
        assert!(result.success);

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        let killed_id = payload["shell_id"].as_str().unwrap();
        assert!(super::bash_runtime::get_shell(killed_id).is_none());
    }

    #[tokio::test]
    async fn kill_shell_accepts_bash_id_alias() {
        let bash = BashTool::new();
        let spawned = bash
            .execute(json!({
                "command": long_running_command(),
                "run_in_background": true
            }))
            .await
            .unwrap();
        let spawned_payload: Value = serde_json::from_str(&spawned.result).unwrap();
        let shell_id = spawned_payload["bash_id"].as_str().unwrap().to_string();

        let kill = KillShellTool::new();
        let result = kill
            .execute(json!({
                "bash_id": shell_id
            }))
            .await
            .unwrap();
        assert!(result.success);
    }
}
