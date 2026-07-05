use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use regex::Regex;
use serde::Deserialize;
use serde_json::json;

use super::bash_runtime;

#[derive(Debug, Deserialize)]
struct BashOutputArgs {
    bash_id: String,
    #[serde(default)]
    cursor: Option<usize>,
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
        "Retrieve incremental output from a running or completed background Bash shell. Use the returned cursor to continue reading without replaying earlier lines."
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
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
                },
                "cursor": {
                    "type": "number",
                    "description": "Read output starting from this cursor (0 for beginning)"
                }
            },
            "required": ["bash_id"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
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

        let cursor = parsed.cursor.unwrap_or(0);
        let (lines, next_cursor, dropped_lines) =
            shell.read_output_since(cursor, regex.as_ref()).await;
        let status = shell.status();
        let exit_code = shell.exit_code().await;

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({
                "bash_id": parsed.bash_id,
                "status": status,
                "exit_code": exit_code,
                "next_cursor": next_cursor,
                "dropped_lines": dropped_lines,
                "output": lines.join("\n"),
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::bash::BashTool;
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

    #[cfg(target_os = "windows")]
    fn invalid_utf8_background_command() -> String {
        let shell = bamboo_infrastructure::process::preferred_bash_shell();
        if shell.arg == "-lc" {
            "printf '\\377\\n'".to_string()
        } else {
            "powershell -NoProfile -Command \"$bytes = [byte[]](0xFF,0x0A); [Console]::OpenStandardOutput().Write($bytes,0,$bytes.Length)\"".to_string()
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn invalid_utf8_background_command() -> String {
        "printf '\\377\\n'".to_string()
    }

    async fn spawn_background_shell_id() -> String {
        let bash = BashTool::new();
        let out = bash
            .invoke(
                json!({
                    "command": background_command(),
                    "run_in_background": true
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        payload["bash_id"].as_str().unwrap().to_string()
    }

    async fn spawn_background_shell_id_for_command(command: String) -> String {
        let bash = BashTool::new();
        let out = bash
            .invoke(
                json!({
                    "command": command,
                    "run_in_background": true
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

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
        let first_out = output_tool
            .invoke(json!({ "bash_id": shell_id }), ToolCtx::none("t"))
            .await
            .unwrap();
        let ToolOutcome::Completed(first) = first_out else {
            panic!("expected Completed")
        };
        let first_payload: Value = serde_json::from_str(&first.result).unwrap();
        let first_output = first_payload["output"].as_str().unwrap_or_default();
        let next_cursor = first_payload["next_cursor"].as_u64().unwrap_or(0);
        assert!(first_output.contains("alpha"));
        assert!(first_output.contains("beta"));

        let second_out = output_tool
            .invoke(
                json!({ "bash_id": shell_id, "cursor": next_cursor }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(second) = second_out else {
            panic!("expected Completed")
        };
        let second_payload: Value = serde_json::from_str(&second.result).unwrap();
        assert_eq!(second_payload["output"], "");
    }

    #[tokio::test]
    async fn bash_output_filter_consumes_unmatched_lines() {
        let shell_id = spawn_background_shell_id().await;
        wait_until_completed(&shell_id).await;

        let output_tool = BashOutputTool::new();
        let filtered_out = output_tool
            .invoke(
                json!({
                    "bash_id": shell_id,
                    "filter": "alpha"
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(filtered) = filtered_out else {
            panic!("expected Completed")
        };
        let filtered_payload: Value = serde_json::from_str(&filtered.result).unwrap();
        let next_cursor = filtered_payload["next_cursor"].as_u64().unwrap_or(0);
        assert!(filtered_payload["output"]
            .as_str()
            .unwrap_or_default()
            .contains("alpha"));
        assert!(!filtered_payload["output"]
            .as_str()
            .unwrap_or_default()
            .contains("beta"));

        let second_out = output_tool
            .invoke(
                json!({ "bash_id": shell_id, "cursor": next_cursor }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(second) = second_out else {
            panic!("expected Completed")
        };
        let second_payload: Value = serde_json::from_str(&second.result).unwrap();
        assert_eq!(second_payload["output"], "");
    }

    #[tokio::test]
    async fn bash_output_tolerates_invalid_utf8_streams() {
        let shell_id =
            spawn_background_shell_id_for_command(invalid_utf8_background_command()).await;
        wait_until_completed(&shell_id).await;

        let output_tool = BashOutputTool::new();
        let out = output_tool
            .invoke(json!({ "bash_id": shell_id }), ToolCtx::none("t"))
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        let payload: Value = serde_json::from_str(&result.result).unwrap();
        let output = payload["output"].as_str().unwrap_or_default();
        assert!(!output.is_empty());
    }
}
