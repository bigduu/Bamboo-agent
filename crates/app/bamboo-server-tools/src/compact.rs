use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use serde::Deserialize;
use serde_json::json;

/// Server-side tool for manually triggering conversation context compression.
///
/// Unlike automatic compression (which triggers based on token thresholds), this
/// tool lets the model or user proactively compress history at natural task boundaries.
/// It bypasses the exposure gate and always forces a compression cycle.
pub struct CompactContextTool;

#[derive(Debug, Deserialize)]
struct CompactContextArgs {
    #[serde(default)]
    instructions: Option<String>,
}

#[async_trait]
impl Tool for CompactContextTool {
    fn name(&self) -> &str {
        "compact_context"
    }

    fn description(&self) -> &str {
        "Manually compress conversation history to free up context window space. \
         Use at natural task boundaries (after finishing a feature, before starting a new topic). \
         Optionally provide custom instructions to control what the summary focuses on."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "instructions": {
                    "type": "string",
                    "description": "Optional custom instructions for what to focus on in the summary. \
                     Examples: 'Preserve variable names and function signatures', \
                     'Focus on open issues and blockers', 'Keep only active task status'"
                }
            }
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: CompactContextArgs = serde_json::from_value(args.clone()).map_err(|e| {
            ToolError::Execution(format!("invalid arguments for compact_context: {e}"))
        })?;

        let instructions_note = parsed
            .instructions
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|i| format!(" with instructions: {i}"))
            .unwrap_or_default();

        Ok(ToolResult {
            success: true,
            result: format!(
                "Context compression requested{}. Compression will be applied before the next turn.",
                instructions_note
            ),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        })
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.execute(args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_without_instructions() {
        let tool = CompactContextTool;
        let result = tool
            .execute(serde_json::json!({}))
            .await
            .expect("execute should succeed");

        assert!(result.success);
        assert_eq!(
            result.result,
            "Context compression requested. Compression will be applied before the next turn."
        );
        assert_eq!(result.display_preference.as_deref(), Some("Collapsible"));
    }

    #[tokio::test]
    async fn execute_with_instructions() {
        let tool = CompactContextTool;
        let result = tool
            .execute(serde_json::json!({
                "instructions": "Preserve all function signatures"
            }))
            .await
            .expect("execute should succeed");

        assert!(result.success);
        assert!(result
            .result
            .contains("with instructions: Preserve all function signatures"));
    }

    #[tokio::test]
    async fn execute_with_empty_instructions() {
        let tool = CompactContextTool;
        let result = tool
            .execute(serde_json::json!({
                "instructions": ""
            }))
            .await
            .expect("execute should succeed");

        assert!(result.success);
        assert!(!result.result.contains("with instructions"));
    }

    #[tokio::test]
    async fn execute_with_null_instructions() {
        let tool = CompactContextTool;
        let result = tool
            .execute(serde_json::json!({
                "instructions": null
            }))
            .await
            .expect("execute should succeed");

        assert!(result.success);
        assert!(!result.result.contains("with instructions"));
    }

    #[tokio::test]
    async fn execute_with_invalid_args_returns_error() {
        let tool = CompactContextTool;
        let result = tool.execute(serde_json::json!("not an object")).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ToolError::Execution(msg) => assert!(msg.contains("invalid arguments")),
            other => panic!("expected Execution error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_with_context_delegates_to_execute() {
        let tool = CompactContextTool;
        let result = tool
            .execute_with_context(
                serde_json::json!({"instructions": "keep task status"}),
                ToolExecutionContext::none("call_123"),
            )
            .await
            .expect("execute_with_context should succeed");

        assert!(result.success);
        assert!(result.result.contains("keep task status"));
    }

    #[test]
    fn tool_name_is_compact_context() {
        let tool = CompactContextTool;
        assert_eq!(tool.name(), "compact_context");
    }

    #[test]
    fn parameters_schema_has_instructions_field() {
        let tool = CompactContextTool;
        let schema = tool.parameters_schema();
        let props = schema
            .get("properties")
            .expect("schema should have properties");
        assert!(
            props.get("instructions").is_some(),
            "should have instructions property"
        );
        // No required fields — instructions is optional
        assert!(
            schema.get("required").is_none(),
            "instructions should be optional"
        );
    }
}
