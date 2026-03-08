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

        let shell = bash_runtime::remove_shell(parsed.shell_id.trim()).ok_or_else(|| {
            ToolError::Execution(format!("Background shell '{}' not found", parsed.shell_id))
        })?;

        shell.kill().await.map_err(ToolError::Execution)?;

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
