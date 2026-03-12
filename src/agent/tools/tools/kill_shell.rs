use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use super::bash_runtime;

#[derive(Debug, Deserialize)]
struct KillShellArgs {
    shell_id: String,
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
        "Kill a running background Bash shell by its shell_id"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "shell_id": {
                    "type": "string",
                    "description": "The ID of the background shell to kill"
                }
            },
            "required": ["shell_id"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: KillShellArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid KillShell args: {}", e)))?;

        let shell_id = parsed.shell_id.trim();
        let shell = bash_runtime::get_shell(shell_id).ok_or_else(|| {
            ToolError::Execution(format!("Background shell '{}' not found", parsed.shell_id))
        })?;

        if shell.status() == "running" {
            shell.kill().await.map_err(ToolError::Execution)?;
        }
        let _ = bash_runtime::remove_shell(shell_id);

        Ok(ToolResult {
            success: true,
            result: json!({
                "shell_id": parsed.shell_id,
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
    use crate::agent::tools::tools::bash::BashTool;
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
}
