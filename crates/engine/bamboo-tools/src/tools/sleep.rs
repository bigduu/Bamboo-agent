use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
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

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL.promotable()
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

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
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
            tracing::info!("Sleeping for {} seconds: {}", parsed.seconds, reason);
        } else {
            tracing::info!("Sleeping for {} seconds", parsed.seconds);
        }

        sleep(Duration::from_secs_f64(parsed.seconds)).await;

        Ok(ToolOutcome::Completed(ToolResult {
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
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    async fn run(tool: &SleepTool, args: serde_json::Value) -> ToolResult {
        let out = tool.invoke(args, ToolCtx::none("t")).await.unwrap();
        let ToolOutcome::Completed(r) = out else {
            panic!("expected Completed")
        };
        r
    }

    #[tokio::test]
    async fn sleep_tool_waits_and_returns_success() {
        let tool = SleepTool::new();
        let start = Instant::now();
        let result = run(&tool, json!({"seconds": 0.01})).await;
        assert!(result.success);
        assert!(start.elapsed().as_millis() >= 10);
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_tool_accepts_valid_seconds() {
        let tool = SleepTool::new();

        // Minimum value
        let result = run(&tool, json!({"seconds": 0.0})).await;
        assert!(result.success);

        // Small positive value
        let result = run(&tool, json!({"seconds": 0.001})).await;
        assert!(result.success);

        // Maximum allowed value (300.0)
        let result = run(&tool, json!({"seconds": 300.0})).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn sleep_tool_rejects_negative_seconds() {
        let tool = SleepTool::new();
        let result = tool
            .invoke(json!({"seconds": -1.0}), ToolCtx::none("t"))
            .await;
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn sleep_tool_rejects_seconds_exceeding_max() {
        let tool = SleepTool::new();
        let result = tool
            .invoke(json!({"seconds": 300.1}), ToolCtx::none("t"))
            .await;
        assert!(result.is_err());
        let error = result.unwrap_err();
        if let ToolError::InvalidArguments(msg) = error {
            assert!(msg.contains("cannot exceed"));
            assert!(msg.contains("300"));
        } else {
            panic!("Expected InvalidArguments error");
        }
    }

    #[tokio::test]
    async fn sleep_tool_includes_reason_in_result() {
        let tool = SleepTool::new();
        let result = run(
            &tool,
            json!({
                "seconds": 0.001,
                "reason": "testing sleep"
            }),
        )
        .await;

        assert!(result.success);
        assert!(result.result.contains("testing sleep"));
        assert!(result.result.contains("(testing sleep)"));
    }

    #[tokio::test]
    async fn sleep_tool_works_without_reason() {
        let tool = SleepTool::new();
        let result = run(&tool, json!({"seconds": 0.001})).await;

        assert!(result.success);
        assert!(result.result.contains("Slept for 0.001 seconds"));
        assert!(!result.result.contains("("));
    }

    #[tokio::test]
    async fn sleep_tool_rejects_missing_seconds() {
        let tool = SleepTool::new();
        let result = tool.invoke(json!({}), ToolCtx::none("t")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sleep_tool_rejects_invalid_seconds_type() {
        let tool = SleepTool::new();
        let result = tool
            .invoke(json!({"seconds": "not a number"}), ToolCtx::none("t"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sleep_tool_accepts_fractional_seconds() {
        let tool = SleepTool::new();
        let start = Instant::now();
        let result = run(&tool, json!({"seconds": 0.05})).await;

        assert!(result.success);
        assert!(result.result.contains("0.05"));
        assert!(start.elapsed().as_millis() >= 50);
    }

    #[test]
    fn sleep_tool_has_correct_name() {
        let tool = SleepTool::new();
        assert_eq!(tool.name(), "Sleep");
    }

    #[test]
    fn sleep_tool_has_description() {
        let tool = SleepTool::new();
        assert!(!tool.description().is_empty());
        assert!(tool.description().contains("300"));
    }

    #[test]
    fn sleep_tool_parameters_schema_has_required_fields() {
        let tool = SleepTool::new();
        let schema = tool.parameters_schema();

        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["seconds"].is_object());
        assert!(schema["properties"]["reason"].is_object());
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("seconds")));
    }

    #[tokio::test]
    async fn sleep_tool_default_impl() {
        let tool = SleepTool;
        let result = run(&tool, json!({"seconds": 0.001})).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn sleep_tool_handles_zero_seconds() {
        let tool = SleepTool::new();
        let result = run(&tool, json!({"seconds": 0.0})).await;

        assert!(result.success);
        assert!(result.result.contains("0 seconds"));
    }

    #[tokio::test]
    async fn sleep_tool_reason_with_special_characters() {
        let tool = SleepTool::new();
        let result = run(
            &tool,
            json!({
                "seconds": 0.001,
                "reason": "等待数据 🎯 (waiting for data)"
            }),
        )
        .await;

        assert!(result.success);
        assert!(result.result.contains("等待数据 🎯"));
    }

    #[tokio::test]
    async fn sleep_tool_reason_empty_string() {
        let tool = SleepTool::new();
        let result = run(
            &tool,
            json!({
                "seconds": 0.001,
                "reason": ""
            }),
        )
        .await;

        assert!(result.success);
        // Empty string is still treated as a reason, so it will show " ()"
        assert!(result.result.contains(" ()"));
    }
}
