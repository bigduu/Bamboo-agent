use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::time::{sleep, Duration};

const MAX_SLEEP_SECONDS: f64 = 300.0;

#[derive(Debug, Deserialize)]
struct SleepArgs {
    seconds: f64,
    #[serde(default)]
    reason: Option<String>,
}

/// Pause execution for a short duration.
pub struct SleepTool;

impl SleepTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SleepTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for SleepTool {
    fn name(&self) -> &str {
        "Sleep"
    }

    fn description(&self) -> &str {
        "Pause execution for a specified number of seconds (max 300s)"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "seconds": {
                    "type": "number",
                    "description": "Seconds to sleep, can be fractional"
                },
                "reason": {
                    "type": "string",
                    "description": "Optional reason for logging"
                }
            },
            "required": ["seconds"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: SleepArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid sleep args: {e}")))?;

        if parsed.seconds < 0.0 {
            return Err(ToolError::InvalidArguments(
                "seconds cannot be negative".to_string(),
            ));
        }
        if parsed.seconds > MAX_SLEEP_SECONDS {
            return Err(ToolError::InvalidArguments(format!(
                "seconds cannot exceed {MAX_SLEEP_SECONDS}"
            )));
        }

        if let Some(reason) = parsed.reason.as_deref() {
            log::info!("Sleeping for {} seconds: {}", parsed.seconds, reason);
        } else {
            log::info!("Sleeping for {} seconds", parsed.seconds);
        }

        sleep(Duration::from_secs_f64(parsed.seconds)).await;

        Ok(ToolResult {
            success: true,
            result: format!(
                "Slept for {} seconds{}",
                parsed.seconds,
                parsed
                    .reason
                    .as_deref()
                    .map(|r| format!(" ({r})"))
                    .unwrap_or_default()
            ),
            display_preference: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn sleep_tool_waits_and_returns_success() {
        let tool = SleepTool::new();
        let start = Instant::now();
        let result = tool.execute(json!({"seconds": 0.01})).await.unwrap();
        assert!(result.success);
        assert!(start.elapsed().as_millis() >= 10);
    }
}
