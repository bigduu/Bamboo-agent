use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;

use super::bash_runtime;

#[derive(Debug, Deserialize)]
struct BashOutputArgs {
    bash_id: String,
    #[serde(default)]
    filter: Option<String>,
}

pub struct BashOutputTool;

impl BashOutputTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BashOutputTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for BashOutputTool {
    fn name(&self) -> &str {
        "BashOutput"
    }

    fn description(&self) -> &str {
        "Retrieve incremental output from a running or completed background Bash shell"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "bash_id": {
                    "type": "string",
                    "description": "The ID of the background shell to retrieve output from"
                },
                "filter": {
                    "type": "string",
                    "description": "Optional regular expression to filter output lines"
                }
            },
            "required": ["bash_id"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: BashOutputArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid BashOutput args: {}", e)))?;

        let shell = bash_runtime::get_shell(parsed.bash_id.trim()).ok_or_else(|| {
            ToolError::Execution(format!("Background shell '{}' not found", parsed.bash_id))
        })?;

        let regex = parsed
            .filter
            .as_ref()
            .map(|value| {
                Regex::new(value).map_err(|e| {
                    ToolError::InvalidArguments(format!("Invalid filter regex: {}", e))
                })
            })
            .transpose()?;

        let (lines, next_cursor) = shell.read_new_output(regex.as_ref()).await;
        let status = shell.status();
        let exit_code = shell.exit_code().await;

        Ok(ToolResult {
            success: true,
            result: json!({
                "bash_id": parsed.bash_id,
                "status": status,
                "exit_code": exit_code,
                "next_cursor": next_cursor,
                "output": lines.join("\n"),
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
    use tokio::time::{sleep, Duration};

    #[cfg(target_os = "windows")]
    fn background_command() -> &'static str {
        "echo alpha && echo beta"
    }

    #[cfg(not(target_os = "windows"))]
    fn background_command() -> &'static str {
        "printf 'alpha\\n'; printf 'beta\\n'"
    }

    async fn spawn_background_shell_id() -> String {
        let bash = BashTool::new();
        let result = bash
            .execute(json!({
                "command": background_command(),
                "run_in_background": true
            }))
            .await
            .unwrap();

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        payload["bash_id"].as_str().unwrap().to_string()
    }

    async fn wait_until_completed(shell_id: &str) {
        let shell = super::bash_runtime::get_shell(shell_id).unwrap();
        for _ in 0..100 {
            if shell.status() == "completed" {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("background shell did not complete in time");
    }

    #[tokio::test]
    async fn bash_output_reads_incrementally() {
        let shell_id = spawn_background_shell_id().await;
        wait_until_completed(&shell_id).await;

        let output_tool = BashOutputTool::new();
        let first = output_tool
            .execute(json!({ "bash_id": shell_id }))
            .await
            .unwrap();
        let first_payload: Value = serde_json::from_str(&first.result).unwrap();
        let first_output = first_payload["output"].as_str().unwrap_or_default();
        assert!(first_output.contains("alpha"));
        assert!(first_output.contains("beta"));

        let second = output_tool
            .execute(json!({ "bash_id": shell_id }))
            .await
            .unwrap();
        let second_payload: Value = serde_json::from_str(&second.result).unwrap();
        assert_eq!(second_payload["output"], "");
    }

    #[tokio::test]
    async fn bash_output_filter_consumes_unmatched_lines() {
        let shell_id = spawn_background_shell_id().await;
        wait_until_completed(&shell_id).await;

        let output_tool = BashOutputTool::new();
        let filtered = output_tool
            .execute(json!({
                "bash_id": shell_id,
                "filter": "alpha"
            }))
            .await
            .unwrap();
        let filtered_payload: Value = serde_json::from_str(&filtered.result).unwrap();
        assert!(filtered_payload["output"]
            .as_str()
            .unwrap_or_default()
            .contains("alpha"));
        assert!(!filtered_payload["output"]
            .as_str()
            .unwrap_or_default()
            .contains("beta"));

        let second = output_tool
            .execute(json!({ "bash_id": shell_id }))
            .await
            .unwrap();
        let second_payload: Value = serde_json::from_str(&second.result).unwrap();
        assert_eq!(second_payload["output"], "");
    }
}
