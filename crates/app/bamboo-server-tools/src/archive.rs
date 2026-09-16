use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde_json::json;

/// Server-side control for explicitly requesting a retrieval-window archive.
///
/// The tool deliberately carries no summarization instructions. The engine
/// validates the active context-management strategy and applies the request at
/// the next safe context boundary after this tool result is committed.
pub struct ArchiveContextTool;

#[async_trait]
impl Tool for ArchiveContextTool {
    fn name(&self) -> &str {
        "archive_context"
    }

    fn description(&self) -> &str {
        "Request a summary-free retrieval-window archive of older exact conversation history. \
         Use only when context_management.strategy is retrieval_window. The engine validates \
         the strategy and keeps archived messages recoverable through session_history_current."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL.promotable()
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        if !args.as_object().is_some_and(serde_json::Map::is_empty) {
            return Err(ToolError::Execution(
                "archive_context accepts no arguments".to_string(),
            ));
        }

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: "Retrieval-window archive requested. The engine will validate the active strategy and apply it at the next safe context boundary.".to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn accepts_only_an_empty_object() {
        let tool = ArchiveContextTool;
        let outcome = tool
            .invoke(serde_json::json!({}), ToolCtx::none("archive-call"))
            .await
            .expect("empty request should be accepted");
        let ToolOutcome::Completed(result) = outcome else {
            panic!("expected completed result")
        };
        assert!(result.success);
        assert!(result.result.contains("Retrieval-window archive requested"));

        let error = tool
            .invoke(
                serde_json::json!({"instructions": "summarize this"}),
                ToolCtx::none("invalid-archive-call"),
            )
            .await
            .expect_err("summary instructions must not be accepted");
        assert!(matches!(error, ToolError::Execution(message) if message.contains("no arguments")));
    }

    #[test]
    fn schema_has_no_summary_instruction_surface() {
        let tool = ArchiveContextTool;
        assert_eq!(tool.name(), "archive_context");
        let schema = tool.parameters_schema();
        assert_eq!(schema["properties"], serde_json::json!({}));
        assert_eq!(schema["additionalProperties"], false);
        assert!(schema.get("required").is_none());
    }
}
