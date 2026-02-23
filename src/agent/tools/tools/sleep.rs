//! Sleep tool for adding delays.
//!
//! This tool provides a simple way to add delays, which is useful for:
//! - Rate limiting when making API calls
//! - Waiting for asynchronous operations to complete
//! - Adding pauses in automation workflows

use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::time::{sleep, Duration};

/// Tool for adding delays/sleep
pub struct SleepTool;

/// Arguments for sleep
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SleepArgs {
    /// Number of seconds to sleep (can be fractional, e.g., 0.5 for 500ms)
    pub seconds: f64,
    /// Optional reason/description for the delay
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl SleepTool {
    /// Create a new sleep tool
    pub fn new() -> Self {
        Self
    }

    /// Sleep for the specified duration
    pub async fn sleep(seconds: f64, reason: Option<&str>) -> Result<String, String> {
        // Validate input
        if seconds < 0.0 {
            return Err("Sleep duration cannot be negative".to_string());
        }

        if seconds > 300.0 {
            return Err("Sleep duration cannot exceed 300 seconds (5 minutes)".to_string());
        }

        let duration = Duration::from_secs_f64(seconds);

        // Log the reason if provided
        if let Some(r) = reason {
            log::info!("Sleeping for {} seconds: {}", seconds, r);
        } else {
            log::info!("Sleeping for {} seconds", seconds);
        }

        sleep(duration).await;

        let reason_str = reason
            .map(|r| format!(" ({})", r))
            .unwrap_or_default();
        Ok(format!("Slept for {} seconds{}", seconds, reason_str))
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
        "sleep"
    }

    fn description(&self) -> &str {
        "Pause execution for a specified number of seconds. Useful for rate limiting, waiting for async operations, or adding delays in workflows. Maximum sleep time is 300 seconds (5 minutes)."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "seconds": {
                    "type": "number",
                    "description": "Number of seconds to sleep. Can be fractional (e.g., 0.5 for 500ms). Maximum is 300 seconds (5 minutes)."
                },
                "reason": {
                    "type": "string",
                    "description": "Optional description of why the sleep is needed (for logging purposes)"
                }
            },
            "required": ["seconds"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let sleep_args: SleepArgs =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        match Self::sleep(sleep_args.seconds, sleep_args.reason.as_deref()).await {
            Ok(result) => Ok(ToolResult {
                success: true,
                result,
                display_preference: None,
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
    use std::time::Instant;

    #[tokio::test]
    async fn test_sleep_basic() {
        let start = Instant::now();
        let result = SleepTool::sleep(0.1, None).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert!(elapsed.as_millis() >= 100);
        assert!(result.unwrap().contains("Slept for 0.1 seconds"));
    }

    #[tokio::test]
    async fn test_sleep_with_reason() {
        let result = SleepTool::sleep(0.01, Some("waiting for API")).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("waiting for API"));
    }

    #[tokio::test]
    async fn test_sleep_negative() {
        let result = SleepTool::sleep(-1.0, None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be negative"));
    }

    #[tokio::test]
    async fn test_sleep_too_long() {
        let result = SleepTool::sleep(301.0, None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot exceed"));
    }

    #[tokio::test]
    async fn test_sleep_zero() {
        let start = Instant::now();
        let result = SleepTool::sleep(0.0, None).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert!(elapsed.as_millis() < 100); // Should return almost immediately
    }

    #[test]
    fn test_tool_name_and_description() {
        let tool = SleepTool::new();
        assert_eq!(tool.name(), "sleep");
        assert!(tool.description().contains("Pause execution"));
        assert!(tool.description().contains("rate limiting"));
    }

    #[tokio::test]
    async fn test_tool_execute() {
        let tool = SleepTool::new();
        let start = Instant::now();

        let result = tool
            .execute(json!({
                "seconds": 0.05,
                "reason": "test sleep"
            }))
            .await
            .unwrap();

        let elapsed = start.elapsed();

        assert!(result.success);
        assert!(elapsed.as_millis() >= 50);
        assert!(result.result.contains("test sleep"));
    }

    #[tokio::test]
    async fn test_tool_execute_invalid_args() {
        let tool = SleepTool::new();

        let result = tool.execute(json!({})).await;
        assert!(result.is_err()); // Missing required field
    }
}
